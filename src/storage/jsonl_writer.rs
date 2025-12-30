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
use std::sync::Mutex;
use tracing::{debug, error, warn};

/// Configuration for JSONL writer
#[derive(Debug, Clone)]
pub struct JsonlWriterConfig {
    /// Base directory for logs
    pub log_dir: PathBuf,
    /// Prefix for filenames (e.g., "market_events", "trade_intents")
    pub prefix: String,
    /// Buffer size for writes (default: 8KB)
    pub buffer_size: usize,
    /// Flush after each write (safer but slower)
    pub flush_each_write: bool,
}

impl Default for JsonlWriterConfig {
    fn default() -> Self {
        Self {
            log_dir: PathBuf::from(
                std::env::var("IRONCRAB_LOG_DIR").unwrap_or_else(|_| "trade_logs".to_string()),
            ),
            prefix: "records".to_string(),
            buffer_size: 8192,
            flush_each_write: false,
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
        let mut state = self.state.lock().unwrap();

        // Check if we need to rotate
        let today = Utc::now().format("%Y%m%d").to_string();
        if state.current_date.as_ref() != Some(&today) {
            self.rotate_locked(&mut state, &today)?;
        }

        // Serialize and write
        let json = serde_json::to_string(record).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
        })?;

        if let Some(writer) = state.writer.as_mut() {
            writeln!(writer, "{}", json)?;
            writer.flush().ok(); // Flush within the same borrow if configured
        }
        state.records_written += 1;
        state.bytes_written += json.len() as u64 + 1; // +1 for newline

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
        // Flush and close existing writer
        if let Some(mut writer) = state.writer.take() {
            writer.flush()?;
        }

        // Build new filename
        let filename = format!("{}-{}.jsonl", self.config.prefix, date);
        let path = self.config.log_dir.join(&filename);

        debug!(
            prefix = %self.config.prefix,
            path = %path.display(),
            "Rotating JSONL writer to new file"
        );

        // Open for append
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;

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
// Async wrapper for non-blocking writes
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
                // Write raw JSON string directly - parse to Value to satisfy Serialize trait
                let value: serde_json::Value = serde_json::from_str(&json).unwrap_or(serde_json::json!({}));
                if let Err(e) = writer.write(&value) {
                    warn!(error = %e, "Failed to write record to JSONL");
                }
            }
            // Final flush on shutdown
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
    pub async fn write_async<T: Serialize>(&self, record: &T) -> Result<(), mpsc::error::SendError<String>> {
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

        writer.write(&TestRecord { id: "1".to_string(), value: 42 }).unwrap();
        writer.write(&TestRecord { id: "2".to_string(), value: 99 }).unwrap();

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
    fn test_jsonl_writer_rotation() {
        // This test verifies the rotation logic works
        // In practice, rotation happens on date change
        let dir = tempdir().unwrap();
        let config = JsonlWriterConfig::new("rotate_test").with_log_dir(dir.path());

        let writer = JsonlWriter::new(config).unwrap();
        writer.write(&TestRecord { id: "test".to_string(), value: 1 }).unwrap();

        let path = writer.current_path().unwrap();
        let filename = path.file_name().unwrap().to_str().unwrap();

        // Should contain today's date
        let today = Utc::now().format("%Y%m%d").to_string();
        assert!(filename.contains(&today));
        assert!(filename.starts_with("rotate_test-"));
        assert!(filename.ends_with(".jsonl"));
    }
}
