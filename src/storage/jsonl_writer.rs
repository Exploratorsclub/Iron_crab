//! Append-only JSONL Writer with Daily Rotation
//!
//! Per docs/STORAGE_CONVENTIONS.md:
//! - Root: IRONCRAB_LOG_DIR or trade_logs/
//! - Rotation: daily (UTC)
//! - Naming: {prefix}-YYYYMMDD.jsonl

use chrono::Utc;
use serde::Serialize;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use tracing::{debug, error, warn};

/// Configuration for JSONL writer
///
/// All defaults documented (DoD K) P0: No hidden defaults).
#[derive(Debug, Clone)]
pub struct JsonlWriterConfig {
    /// Base directory for logs. Default: $IRONCRAB_LOG_DIR or "trade_logs"
    pub log_dir: PathBuf,
    /// Prefix for filenames (e.g., "market_events", "trade_intents"). Default: "records"
    pub prefix: String,
    /// Buffer size for writes. Default: 8KB
    pub buffer_size: usize,
    /// Flush after each write (safer but slower). Default: false
    pub flush_each_write: bool,
}

impl Default for JsonlWriterConfig {
    fn default() -> Self {
        Self {
            log_dir: PathBuf::from(
                std::env::var("IRONCRAB_LOG_DIR").unwrap_or_else(|_| "trade_logs".to_string()),
            ),
            prefix: "records".to_string(),
            buffer_size: 8192,       // 8KB buffer
            flush_each_write: false, // batch writes for performance
        }
    }
}

impl JsonlWriterConfig {
    pub fn new(prefix: &str) -> Self {
        Self {
            prefix: prefix.to_string(),
            ..Default::default()
        }
    }

    pub fn with_log_dir(mut self, dir: impl AsRef<Path>) -> Self {
        self.log_dir = dir.as_ref().to_path_buf();
        self
    }

    pub fn with_flush_each_write(mut self, flush: bool) -> Self {
        self.flush_each_write = flush;
        self
    }
}

/// Thread-safe append-only JSONL writer with daily rotation
pub struct JsonlWriter {
    config: JsonlWriterConfig,
    state: Mutex<WriterState>,
}

struct WriterState {
    writer: Option<BufWriter<File>>,
    current_date: Option<String>,
    current_path: Option<PathBuf>,
    records_written: u64,
    bytes_written: u64,
}

impl JsonlWriter {
    /// Create a new JSONL writer
    pub fn new(config: JsonlWriterConfig) -> std::io::Result<Self> {
        // Ensure log directory exists
        fs::create_dir_all(&config.log_dir)?;

        Ok(Self {
            config,
            state: Mutex::new(WriterState {
                writer: None,
                current_date: None,
                current_path: None,
                records_written: 0,
                bytes_written: 0,
            }),
        })
    }

    /// Create with default config for a given prefix
    pub fn for_prefix(prefix: &str) -> std::io::Result<Self> {
        Self::new(JsonlWriterConfig::new(prefix))
    }

    /// Write a serializable record as JSONL
    pub fn write<T: Serialize>(&self, record: &T) -> std::io::Result<()> {
        let json = serde_json::to_string(record)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        self.write_json_line(&json)
    }

    /// Write a pre-serialized JSON object as one JSONL line (no extra allocation on hot path).
    pub fn write_json_line(&self, json: &str) -> std::io::Result<()> {
        let mut state = self.state.lock().unwrap();

        let today = Utc::now().format("%Y%m%d").to_string();
        if state.current_date.as_ref() != Some(&today) {
            self.rotate_locked(&mut state, &today)?;
        }

        if let Some(writer) = state.writer.as_mut() {
            writeln!(writer, "{json}")?;
            if self.config.flush_each_write {
                writer.flush()?;
            }
        }
        state.records_written += 1;
        state.bytes_written += json.len() as u64 + 1;

        Ok(())
    }

    /// Write multiple records efficiently
    pub fn write_batch<T: Serialize>(&self, records: &[T]) -> std::io::Result<usize> {
        let mut count = 0;
        for record in records {
            self.write(record)?;
            count += 1;
        }
        Ok(count)
    }

    /// Force flush to disk
    pub fn flush(&self) -> std::io::Result<()> {
        let mut state = self.state.lock().unwrap();
        if let Some(writer) = state.writer.as_mut() {
            writer.flush()?;
        }
        Ok(())
    }

    /// Get current file path
    pub fn current_path(&self) -> Option<PathBuf> {
        self.state.lock().unwrap().current_path.clone()
    }

    /// Get statistics
    pub fn stats(&self) -> (u64, u64) {
        let state = self.state.lock().unwrap();
        (state.records_written, state.bytes_written)
    }

    fn rotate_locked(&self, state: &mut WriterState, date: &str) -> std::io::Result<()> {
        if let Some(mut writer) = state.writer.take() {
            writer.flush()?;
        }

        let filename = format!("{}-{}.jsonl", self.config.prefix, date);
        let path = self.config.log_dir.join(&filename);

        debug!(
            prefix = %self.config.prefix,
            path = %path.display(),
            "Rotating JSONL writer to new file"
        );

        let file = OpenOptions::new().create(true).append(true).open(&path)?;

        state.writer = Some(BufWriter::with_capacity(self.config.buffer_size, file));
        state.current_date = Some(date.to_string());
        state.current_path = Some(path);

        Ok(())
    }
}

impl Drop for JsonlWriter {
    fn drop(&mut self) {
        if let Err(e) = self.flush() {
            error!(error = %e, "Failed to flush JSONL writer on drop");
        }
    }
}

// ============================================================================
// Off-hot-path queued writer (dedicated OS thread)
// ============================================================================

enum QueuedJsonlMsg {
    Line(String),
    Flush,
    Shutdown,
}

/// Non-blocking JSONL enqueue from Geyser paths; actual I/O on `jsonl-writer` thread.
pub struct QueuedJsonlWriter {
    sender: std::sync::mpsc::SyncSender<QueuedJsonlMsg>,
    records_written: Arc<AtomicU64>,
    bytes_written: Arc<AtomicU64>,
    join: Mutex<Option<JoinHandle<()>>>,
}

impl QueuedJsonlWriter {
    /// Spawn background writer with bounded queue (`try_enqueue` drops when full).
    pub fn spawn(config: JsonlWriterConfig, queue_capacity: usize) -> std::io::Result<Self> {
        let queue_capacity = queue_capacity.max(64);
        let (tx, rx) = std::sync::mpsc::sync_channel::<QueuedJsonlMsg>(queue_capacity);
        let records_written = Arc::new(AtomicU64::new(0));
        let bytes_written = Arc::new(AtomicU64::new(0));
        let records_for_thread = Arc::clone(&records_written);
        let bytes_for_thread = Arc::clone(&bytes_written);
        let flush_each_write = config.flush_each_write;
        let join = std::thread::Builder::new()
            .name("jsonl-writer".into())
            .spawn(move || {
                let writer = match JsonlWriter::new(config) {
                    Ok(w) => w,
                    Err(e) => {
                        error!(error = %e, "QueuedJsonlWriter: failed to open log file");
                        return;
                    }
                };
                let periodic_flush = Duration::from_secs(1);
                let mut last_periodic_flush = Instant::now();
                let write_line = |json: String| {
                    let len = json.len() as u64 + 1;
                    if let Err(e) = writer.write_json_line(&json) {
                        warn!(error = %e, "QueuedJsonlWriter: write failed");
                    } else {
                        records_for_thread.fetch_add(1, Ordering::Relaxed);
                        bytes_for_thread.fetch_add(len, Ordering::Relaxed);
                    }
                };
                loop {
                    match rx.recv_timeout(Duration::from_millis(200)) {
                        Ok(QueuedJsonlMsg::Line(json)) => write_line(json),
                        Ok(QueuedJsonlMsg::Flush) => {
                            let _ = writer.flush();
                        }
                        Ok(QueuedJsonlMsg::Shutdown) => {
                            let _ = writer.flush();
                            break;
                        }
                        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                            if !flush_each_write && last_periodic_flush.elapsed() >= periodic_flush
                            {
                                let _ = writer.flush();
                                last_periodic_flush = Instant::now();
                            }
                        }
                        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                    }
                }
                let _ = writer.flush();
            })?;

        Ok(Self {
            sender: tx,
            records_written,
            bytes_written,
            join: Mutex::new(Some(join)),
        })
    }

    /// Queue serialized JSON (non-blocking). Returns `false` when queue is full.
    pub fn try_enqueue_json(&self, json: String) -> bool {
        self.sender.try_send(QueuedJsonlMsg::Line(json)).is_ok()
    }

    /// Queue a serializable record (serializes on caller thread — prefer `try_enqueue_json` from hot path).
    pub fn try_write<T: Serialize>(&self, record: &T) -> bool {
        let json = serde_json::to_string(record).unwrap_or_else(|_| "{}".to_string());
        self.try_enqueue_json(json)
    }

    pub fn flush(&self) -> std::io::Result<()> {
        let _ = self.sender.send(QueuedJsonlMsg::Flush);
        Ok(())
    }

    pub fn stats(&self) -> (u64, u64) {
        (
            self.records_written.load(Ordering::Relaxed),
            self.bytes_written.load(Ordering::Relaxed),
        )
    }
}

impl Drop for QueuedJsonlWriter {
    fn drop(&mut self) {
        let _ = self.sender.send(QueuedJsonlMsg::Shutdown);
        if let Some(handle) = self.join.lock().unwrap().take() {
            let _ = handle.join();
        }
    }
}

// ============================================================================
// Async wrapper for non-blocking writes (legacy; prefer QueuedJsonlWriter)
// ============================================================================

use tokio::sync::mpsc;

/// Async JSONL writer that buffers writes through a channel
pub struct AsyncJsonlWriter {
    sender: mpsc::Sender<String>,
}

impl AsyncJsonlWriter {
    /// Spawn an async writer with background flushing
    pub fn spawn(config: JsonlWriterConfig, buffer_size: usize) -> std::io::Result<Self> {
        let (sender, mut receiver) = mpsc::channel::<String>(buffer_size);

        let writer = JsonlWriter::new(config)?;

        tokio::spawn(async move {
            while let Some(json) = receiver.recv().await {
                if let Err(e) = writer.write_json_line(&json) {
                    warn!(error = %e, "Failed to write record to JSONL");
                }
            }
            let _ = writer.flush();
        });

        Ok(Self { sender })
    }

    /// Queue a record for async writing (non-blocking)
    pub fn write<T: Serialize>(&self, record: &T) -> Result<(), mpsc::error::TrySendError<String>> {
        let json = serde_json::to_string(record).unwrap_or_else(|_| "{}".to_string());
        self.sender.try_send(json)
    }

    /// Queue with backpressure (async)
    pub async fn write_async<T: Serialize>(
        &self,
        record: &T,
    ) -> Result<(), mpsc::error::SendError<String>> {
        let json = serde_json::to_string(record).unwrap_or_else(|_| "{}".to_string());
        self.sender.send(json).await
    }
}

// ============================================================================
// Convenience functions for common record types
// ============================================================================

/// Get the standard subdirectory for a record type
pub fn get_log_subdir(record_type: &str) -> PathBuf {
    let base = std::env::var("IRONCRAB_LOG_DIR").unwrap_or_else(|_| "trade_logs".to_string());
    PathBuf::from(base).join(record_type)
}

/// Standard prefixes per STORAGE_CONVENTIONS.md
pub mod prefixes {
    pub const MARKET_EVENTS: &str = "market_events";
    pub const TRADE_INTENTS: &str = "trade_intents";
    pub const DECISION_RECORDS: &str = "decision_records";
    pub const EXECUTION_RESULTS: &str = "execution_results";
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[derive(Serialize)]
    struct TestRecord {
        id: String,
        value: i32,
    }

    #[test]
    fn test_jsonl_writer_creates_file() {
        let dir = tempdir().unwrap();
        let config = JsonlWriterConfig::new("test")
            .with_log_dir(dir.path())
            .with_flush_each_write(true);

        let writer = JsonlWriter::new(config).unwrap();

        writer
            .write(&TestRecord {
                id: "1".to_string(),
                value: 42,
            })
            .unwrap();
        writer
            .write(&TestRecord {
                id: "2".to_string(),
                value: 99,
            })
            .unwrap();

        let (records, bytes) = writer.stats();
        assert_eq!(records, 2);
        assert!(bytes > 0);

        let path = writer.current_path().unwrap();
        assert!(path.exists());

        let content = fs::read_to_string(&path).unwrap();
        let lines: Vec<_> = content.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("\"id\":\"1\""));
        assert!(lines[1].contains("\"id\":\"2\""));
    }

    #[test]
    fn test_jsonl_writer_no_flush_per_write_when_disabled() {
        let dir = tempdir().unwrap();
        let config = JsonlWriterConfig::new("noflush")
            .with_log_dir(dir.path())
            .with_flush_each_write(false);

        let writer = JsonlWriter::new(config).unwrap();
        writer
            .write(&TestRecord {
                id: "x".to_string(),
                value: 1,
            })
            .unwrap();

        let path = writer.current_path().unwrap();
        // Buffered write: file may exist but stay empty until flush/rotation/drop.
        let size_before_flush = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        writer.flush().unwrap();
        let size_after_flush = fs::metadata(&path).unwrap().len();
        assert_eq!(
            size_before_flush, 0,
            "write() must not flush when flush_each_write is false"
        );
        assert!(size_after_flush > 0);
    }

    #[test]
    fn test_jsonl_writer_rotation() {
        let dir = tempdir().unwrap();
        let config = JsonlWriterConfig::new("rotate_test").with_log_dir(dir.path());

        let writer = JsonlWriter::new(config).unwrap();
        writer
            .write(&TestRecord {
                id: "test".to_string(),
                value: 1,
            })
            .unwrap();

        let path = writer.current_path().unwrap();
        let filename = path.file_name().unwrap().to_str().unwrap();

        let today = Utc::now().format("%Y%m%d").to_string();
        assert!(filename.contains(&today));
        assert!(filename.starts_with("rotate_test-"));
        assert!(filename.ends_with(".jsonl"));
    }

    #[test]
    fn test_queued_jsonl_writer_delivers_lines() {
        let dir = tempdir().unwrap();
        let config = JsonlWriterConfig::new("queued")
            .with_log_dir(dir.path())
            .with_flush_each_write(true);
        let q = QueuedJsonlWriter::spawn(config, 64).unwrap();
        assert!(q.try_write(&TestRecord {
            id: "q1".to_string(),
            value: 7,
        }));
        q.flush().unwrap();
        drop(q);
        let today = Utc::now().format("%Y%m%d").to_string();
        let path = dir.path().join(format!("queued-{today}.jsonl"));
        let content = fs::read_to_string(path).unwrap();
        assert!(content.contains("q1"));
    }
}
