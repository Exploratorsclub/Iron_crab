//! Append-only JSONL Writer with Daily Rotation
//!
//! Per docs/STORAGE_CONVENTIONS.md:
//! - Root: IRONCRAB_LOG_DIR or trade_logs/
//! - Rotation: daily (UTC)
//! - Naming: {prefix}-YYYYMMDD.jsonl

use crate::ipc::schema::MarketEvent;
use crate::metrics::{
    inc_jsonl_retention_deleted_bytes_total, inc_jsonl_retention_deleted_files_total,
    inc_market_data_jsonl_records_written_total, set_market_data_jsonl_queue_depth,
};
use chrono::Utc;
use serde::Serialize;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime};
use tracing::{debug, error, warn};

/// Size/record caps for same-day JSONL segments (P172: execution_results only).
#[derive(Debug, Clone, Copy)]
pub struct SegmentRotationLimits {
    /// Rotate when current segment file size reaches this (bytes). 0 = disabled.
    pub max_bytes: u64,
    /// Rotate when this many records were written to the current segment. 0 = disabled.
    pub max_records: u64,
}

impl SegmentRotationLimits {
    /// Defaults: 32 MiB and 50_000 records (`IRONCRAB_EXEC_JSONL_SEGMENT_MAX_*` env).
    pub fn execution_results_from_env() -> Self {
        let max_mb: u64 = std::env::var("IRONCRAB_EXEC_JSONL_SEGMENT_MAX_MB")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(32);
        let max_records: u64 = std::env::var("IRONCRAB_EXEC_JSONL_SEGMENT_MAX_RECORDS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(50_000);
        Self {
            max_bytes: max_mb.saturating_mul(1024 * 1024),
            max_records,
        }
    }

    fn should_rotate(&self, segment_bytes: u64, segment_records: u64) -> bool {
        (self.max_bytes > 0 && segment_bytes >= self.max_bytes)
            || (self.max_records > 0 && segment_records >= self.max_records)
    }

    fn file_exceeds_cap(&self, file_len: u64) -> bool {
        self.max_bytes > 0 && file_len >= self.max_bytes
    }
}

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
    /// Same-day segment rotation when file grows too large (optional; P172 execution_results).
    pub segment_rotation: Option<SegmentRotationLimits>,
    /// Delete closed JSONL files older than this many hours. Default: 24. 0 = disabled.
    pub jsonl_retention_hours: u64,
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
            segment_rotation: None,
            jsonl_retention_hours: 24,
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

    /// Enable same-day segment files `{prefix}-YYYYMMDD.jsonl`, `.2.jsonl`, … when caps are hit.
    pub fn with_segment_rotation(mut self, limits: SegmentRotationLimits) -> Self {
        self.segment_rotation = Some(limits);
        self
    }

    pub fn with_retention_hours(mut self, hours: u64) -> Self {
        self.jsonl_retention_hours = hours;
        self
    }
}

fn segment_filename(prefix: &str, date: &str, segment_index: u32) -> String {
    if segment_index <= 1 {
        format!("{prefix}-{date}.jsonl")
    } else {
        format!("{prefix}-{date}.{segment_index}.jsonl")
    }
}

/// Remove closed JSONL segment files older than `retention_hours` under `log_dir`.
/// `open_path` is never deleted (current writer file).
pub fn prune_expired_jsonl_files(
    log_dir: &Path,
    prefix: &str,
    retention_hours: u64,
    open_path: Option<&Path>,
) -> std::io::Result<(u64, u64)> {
    if retention_hours == 0 {
        return Ok((0, 0));
    }

    let cutoff = SystemTime::now()
        .checked_sub(Duration::from_secs(retention_hours.saturating_mul(3600)))
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "jsonl retention cutoff underflow",
            )
        })?;

    let prefix_with_dash = format!("{prefix}-");
    let mut deleted_files = 0u64;
    let mut deleted_bytes = 0u64;

    for entry in fs::read_dir(log_dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if !name.starts_with(&prefix_with_dash) || !name.ends_with(".jsonl") {
            continue;
        }
        if open_path.is_some_and(|open| open == path) {
            continue;
        }
        let metadata = match fs::metadata(&path) {
            Ok(m) => m,
            Err(e) => {
                debug!(path = %path.display(), error = %e, "JSONL retention: metadata failed");
                continue;
            }
        };
        let modified = match metadata.modified() {
            Ok(t) => t,
            Err(e) => {
                debug!(path = %path.display(), error = %e, "JSONL retention: mtime failed");
                continue;
            }
        };
        if modified >= cutoff {
            continue;
        }
        let file_bytes = metadata.len();
        match fs::remove_file(&path) {
            Ok(()) => {
                deleted_files += 1;
                deleted_bytes += file_bytes;
                debug!(path = %path.display(), bytes = file_bytes, "JSONL retention: deleted expired file");
            }
            Err(e) => {
                warn!(path = %path.display(), error = %e, "JSONL retention: delete failed");
            }
        }
    }

    if deleted_files > 0 {
        inc_jsonl_retention_deleted_files_total(deleted_files);
        inc_jsonl_retention_deleted_bytes_total(deleted_bytes);
    }

    Ok((deleted_files, deleted_bytes))
}

fn resolve_append_segment(
    log_dir: &Path,
    prefix: &str,
    date: &str,
    limits: &SegmentRotationLimits,
) -> u32 {
    let mut idx = 1u32;
    loop {
        let path = log_dir.join(segment_filename(prefix, date, idx));
        if !path.exists() {
            return idx;
        }
        let len = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        if limits.file_exceeds_cap(len) {
            idx += 1;
            continue;
        }
        return idx;
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
    segment_index: u32,
    segment_records: u64,
    segment_bytes: u64,
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
                segment_index: 1,
                segment_records: 0,
                segment_bytes: 0,
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
            self.open_day_locked(&mut state, &today)?;
        }

        if let Some(writer) = state.writer.as_mut() {
            writeln!(writer, "{json}")?;
            // Production: honor `flush_each_write` only. Dev/test builds (`cfg(test)` on this
            // crate, `debug_assertions` when linked from integration/binary tests) flush so
            // sync `JsonlWriter` callers can read JSONL immediately (PR165 CI).
            if self.config.flush_each_write || cfg!(test) || cfg!(debug_assertions) {
                writer.flush()?;
            }
        }
        let line_len = json.len() as u64 + 1;
        state.records_written += 1;
        state.bytes_written += line_len;
        state.segment_records += 1;
        state.segment_bytes += line_len;

        if let Some(limits) = self.config.segment_rotation {
            if limits.should_rotate(state.segment_bytes, state.segment_records) {
                self.rotate_segment_locked(&mut state, &today)?;
            }
        }

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

    fn open_day_locked(&self, state: &mut WriterState, date: &str) -> std::io::Result<()> {
        if let Some(mut writer) = state.writer.take() {
            writer.flush()?;
        }

        state.segment_index = if let Some(limits) = self.config.segment_rotation {
            resolve_append_segment(&self.config.log_dir, &self.config.prefix, date, &limits)
        } else {
            1
        };
        state.segment_records = 0;
        state.segment_bytes = 0;
        self.open_segment_locked(state, date, state.segment_index)
    }

    fn rotate_segment_locked(&self, state: &mut WriterState, date: &str) -> std::io::Result<()> {
        if let Some(mut writer) = state.writer.take() {
            writer.flush()?;
        }
        state.segment_index = state.segment_index.saturating_add(1);
        state.segment_records = 0;
        state.segment_bytes = 0;
        self.open_segment_locked(state, date, state.segment_index)
    }

    fn open_segment_locked(
        &self,
        state: &mut WriterState,
        date: &str,
        segment_index: u32,
    ) -> std::io::Result<()> {
        let filename = segment_filename(&self.config.prefix, date, segment_index);
        let path = self.config.log_dir.join(&filename);

        debug!(
            prefix = %self.config.prefix,
            path = %path.display(),
            segment_index,
            "Opening JSONL segment"
        );

        let existing_len = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        let file = OpenOptions::new().create(true).append(true).open(&path)?;

        state.writer = Some(BufWriter::with_capacity(self.config.buffer_size, file));
        state.current_date = Some(date.to_string());
        state.current_path = Some(path);
        state.segment_index = segment_index;
        state.segment_bytes = existing_len;
        // segment_records stays 0 on reopen; byte cap still protects across restarts.

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
    /// Pre-serialized line (cold path / tests).
    Line(String),
    /// Market ingest: serialize only on `jsonl-writer` thread (Phase R1).
    MarketEvent(Box<MarketEvent>),
    /// Generic record: closure runs on writer thread (tests / non–market-data).
    Serialize(Box<dyn FnOnce() -> String + Send>),
    Flush,
    Shutdown,
}

/// Non-blocking JSONL enqueue from Geyser paths; actual I/O on `jsonl-writer` thread.
pub struct QueuedJsonlWriter {
    config: JsonlWriterConfig,
    sender: std::sync::mpsc::SyncSender<QueuedJsonlMsg>,
    /// Max in-flight + channel records (matches `sync_channel` capacity).
    queue_capacity: usize,
    queue_depth: Arc<AtomicUsize>,
    records_written: Arc<AtomicU64>,
    bytes_written: Arc<AtomicU64>,
    join: Mutex<Option<JoinHandle<()>>>,
}

impl QueuedJsonlWriter {
    /// Spawn background writer with bounded queue (`try_enqueue` drops when full).
    pub fn spawn(config: JsonlWriterConfig, queue_capacity: usize) -> std::io::Result<Self> {
        let queue_capacity = queue_capacity.max(1);
        let (tx, rx) = std::sync::mpsc::sync_channel::<QueuedJsonlMsg>(queue_capacity);
        let records_written = Arc::new(AtomicU64::new(0));
        let bytes_written = Arc::new(AtomicU64::new(0));
        let queue_depth = Arc::new(AtomicUsize::new(0));
        let records_for_thread = Arc::clone(&records_written);
        let bytes_for_thread = Arc::clone(&bytes_written);
        let depth_for_thread = Arc::clone(&queue_depth);
        let flush_each_write = config.flush_each_write;
        let config_for_thread = config.clone();
        let retention_hours = config.jsonl_retention_hours;
        let retention_log_dir = config.log_dir.clone();
        let retention_prefix = config.prefix.clone();
        let join = std::thread::Builder::new()
            .name("jsonl-writer".into())
            .spawn(move || {
                let writer = match JsonlWriter::new(config_for_thread) {
                    Ok(w) => w,
                    Err(e) => {
                        error!(error = %e, "QueuedJsonlWriter: failed to open log file");
                        return;
                    }
                };
                let periodic_flush = Duration::from_secs(1);
                let retention_interval = Duration::from_secs(60);
                let mut last_periodic_flush = Instant::now();
                let mut last_retention_run = Instant::now();
                let run_retention = || {
                    if retention_hours == 0 {
                        return;
                    }
                    let open_path = writer.current_path();
                    if let Err(e) = prune_expired_jsonl_files(
                        &retention_log_dir,
                        &retention_prefix,
                        retention_hours,
                        open_path.as_deref(),
                    ) {
                        warn!(error = %e, "QueuedJsonlWriter: JSONL retention janitor failed");
                    }
                };
                let dec_queue_depth = || {
                    let mut cur = depth_for_thread.load(Ordering::Relaxed);
                    while cur > 0 {
                        match depth_for_thread.compare_exchange_weak(
                            cur,
                            cur - 1,
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        ) {
                            Ok(_) => {
                                set_market_data_jsonl_queue_depth(cur - 1);
                                return;
                            }
                            Err(actual) => cur = actual,
                        }
                    }
                    set_market_data_jsonl_queue_depth(0);
                };
                let write_line = |json: String| {
                    let len = json.len() as u64 + 1;
                    if let Err(e) = writer.write_json_line(&json) {
                        warn!(error = %e, "QueuedJsonlWriter: write failed");
                    } else {
                        records_for_thread.fetch_add(1, Ordering::Relaxed);
                        bytes_for_thread.fetch_add(len, Ordering::Relaxed);
                        inc_market_data_jsonl_records_written_total();
                    }
                };
                loop {
                    match rx.recv_timeout(Duration::from_millis(200)) {
                        Ok(QueuedJsonlMsg::Line(json)) => {
                            write_line(json);
                            dec_queue_depth();
                        }
                        Ok(QueuedJsonlMsg::MarketEvent(event)) => {
                            let json =
                                serde_json::to_string(&*event).unwrap_or_else(|_| "{}".to_string());
                            write_line(json);
                            dec_queue_depth();
                        }
                        Ok(QueuedJsonlMsg::Serialize(serialize)) => {
                            write_line(serialize());
                            dec_queue_depth();
                        }
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
                            if last_retention_run.elapsed() >= retention_interval {
                                run_retention();
                                last_retention_run = Instant::now();
                            }
                        }
                        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                    }
                }
                let _ = writer.flush();
            })?;

        Ok(Self {
            config,
            sender: tx,
            queue_capacity,
            queue_depth,
            records_written,
            bytes_written,
            join: Mutex::new(Some(join)),
        })
    }

    /// Expected active JSONL path (same naming as [`JsonlWriter`]; writer thread may rotate segments).
    pub fn current_path(&self) -> Option<PathBuf> {
        let today = Utc::now().format("%Y%m%d").to_string();
        Some(
            self.config
                .log_dir
                .join(format!("{}-{}.jsonl", self.config.prefix, today)),
        )
    }

    fn try_enqueue_msg(&self, msg: QueuedJsonlMsg) -> bool {
        if self.queue_depth.load(Ordering::Relaxed) >= self.queue_capacity {
            return false;
        }
        if self.sender.try_send(msg).is_ok() {
            let d = self.queue_depth.fetch_add(1, Ordering::Relaxed) + 1;
            set_market_data_jsonl_queue_depth(d);
            true
        } else {
            false
        }
    }

    /// Queue pre-serialized JSON (non-blocking). Returns `false` when queue is full.
    pub fn try_enqueue_json(&self, json: String) -> bool {
        self.try_enqueue_msg(QueuedJsonlMsg::Line(json))
    }

    /// Queue a market event for JSONL (no serde on caller; clone + bounded `try_send` only).
    pub fn try_enqueue_market_event(&self, event: &MarketEvent) -> bool {
        self.try_enqueue_msg(QueuedJsonlMsg::MarketEvent(Box::new(event.clone())))
    }

    /// Queue a serializable record (serialization on `jsonl-writer` thread only).
    pub fn try_write<T: Serialize + Send + 'static>(&self, record: T) -> bool {
        let record = Box::new(record);
        self.try_enqueue_msg(QueuedJsonlMsg::Serialize(Box::new(move || {
            serde_json::to_string(&*record).unwrap_or_else(|_| "{}".to_string())
        })))
    }

    pub fn flush(&self) -> std::io::Result<()> {
        for _ in 0..256 {
            if self.sender.try_send(QueuedJsonlMsg::Flush).is_ok() {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::WouldBlock,
            "QueuedJsonlWriter flush: queue full",
        ))
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
        // Non-blocking: avoid deadlock if the bounded channel still holds pending records.
        for _ in 0..4 {
            if self.sender.try_send(QueuedJsonlMsg::Shutdown).is_ok() {
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
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

    /// PR165: production defers flush when `flush_each_write=false`; dev/test builds flush per
    /// write so binaries using sync `JsonlWriter` in tests can read JSONL immediately.
    #[test]
    fn test_jsonl_writer_flushes_per_write_in_dev_when_flush_disabled() {
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
        let size_after_write = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        assert!(
            size_after_write > 0,
            "dev/test build: write() must flush for test harness disk visibility"
        );
    }

    #[test]
    fn test_execution_results_segment_rotation_by_record_cap() {
        let dir = tempdir().unwrap();
        let limits = SegmentRotationLimits {
            max_bytes: 0,
            max_records: 3,
        };
        let config = JsonlWriterConfig::new("execution_results")
            .with_log_dir(dir.path())
            .with_flush_each_write(true)
            .with_segment_rotation(limits);

        let writer = JsonlWriter::new(config).unwrap();
        for i in 0..5 {
            writer
                .write(&TestRecord {
                    id: format!("r{i}"),
                    value: i,
                })
                .unwrap();
        }
        writer.flush().unwrap();

        let today = Utc::now().format("%Y%m%d").to_string();
        let seg1 = dir.path().join(format!("execution_results-{today}.jsonl"));
        let seg2 = dir
            .path()
            .join(format!("execution_results-{today}.2.jsonl"));
        assert!(seg1.exists());
        assert!(seg2.exists());
        let content1 = fs::read_to_string(&seg1).unwrap();
        let content2 = fs::read_to_string(&seg2).unwrap();
        let lines1: Vec<_> = content1.lines().collect();
        let lines2: Vec<_> = content2.lines().collect();
        assert_eq!(lines1.len(), 3);
        assert_eq!(lines2.len(), 2);
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
        assert!(q.try_write(TestRecord {
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

    /// Phase R1: full queue returns immediately (no blocking on ingest).
    #[test]
    fn phase_r1_queued_jsonl_full_queue_returns_false_immediately() {
        use std::sync::atomic::{AtomicBool, Ordering as AOrdering};
        use std::sync::Arc;
        use std::time::Instant;

        let dir = tempdir().unwrap();
        let config = JsonlWriterConfig::new("fullq")
            .with_log_dir(dir.path())
            .with_flush_each_write(true);
        let hold = Arc::new(AtomicBool::new(true));
        let hold_w = Arc::clone(&hold);
        let q = QueuedJsonlWriter::spawn(config, 1).unwrap();
        assert!(
            q.try_enqueue_msg(QueuedJsonlMsg::Serialize(Box::new(move || {
                while hold_w.load(AOrdering::Relaxed) {
                    std::thread::sleep(Duration::from_millis(1));
                }
                "{}".to_string()
            })))
        );
        std::thread::sleep(Duration::from_millis(20));
        let t0 = Instant::now();
        assert!(!q.try_enqueue_json("second".to_string()));
        assert!(
            t0.elapsed() < Duration::from_millis(50),
            "try_enqueue must not block when in-flight depth is at capacity"
        );
        hold.store(false, AOrdering::Relaxed);
        std::thread::sleep(Duration::from_millis(100));
        drop(q);
    }

    /// Phase R1: writer thread produces valid JSONL for `MarketEvent`.
    #[test]
    fn phase_r1_queued_jsonl_serializes_market_event_on_writer_thread() {
        use crate::ipc::schema::{MarketEvent, MarketEventKind, RecordHeader};

        let dir = tempdir().unwrap();
        let config = JsonlWriterConfig::new("mkt")
            .with_log_dir(dir.path())
            .with_flush_each_write(true);
        let q = QueuedJsonlWriter::spawn(config, 64).unwrap();
        let event = MarketEvent {
            header: RecordHeader::new("market_data", "test", "run-test"),
            event_id: "evt-r1-001".to_string(),
            source: "geyser".to_string(),
            slot: Some(42),
            kind: MarketEventKind::TransactionDetected {
                signature: "sig".into(),
                program: "prog".into(),
            },
        };
        assert!(q.try_enqueue_market_event(&event));
        q.flush().unwrap();
        drop(q);
        let today = Utc::now().format("%Y%m%d").to_string();
        let path = dir.path().join(format!("mkt-{today}.jsonl"));
        let content = fs::read_to_string(path).unwrap();
        assert!(content.contains("evt-r1-001"));
        assert!(content.contains("TransactionDetected"));
        let _: serde_json::Value =
            serde_json::from_str(content.lines().next().expect("one line")).unwrap();
    }

    /// Phase R1: parallel enqueues from many threads do not block (bounded try_send).
    #[test]
    fn phase_r1_parallel_try_enqueue_does_not_block_callers() {
        use std::sync::atomic::{AtomicBool, Ordering as AOrdering};
        use std::sync::Arc;
        use std::time::Instant;

        let dir = tempdir().unwrap();
        let config = JsonlWriterConfig::new("parallel")
            .with_log_dir(dir.path())
            .with_flush_each_write(true);
        let hold = Arc::new(AtomicBool::new(true));
        let hold_w = Arc::clone(&hold);
        let q = Arc::new(QueuedJsonlWriter::spawn(config, 2).unwrap());
        assert!(
            q.try_enqueue_msg(QueuedJsonlMsg::Serialize(Box::new(move || {
                while hold_w.load(AOrdering::Relaxed) {
                    std::thread::sleep(Duration::from_millis(1));
                }
                "{}".to_string()
            })))
        );
        assert!(q.try_enqueue_json("fill-2".to_string()));
        std::thread::sleep(Duration::from_millis(20));
        let threads: Vec<_> = (0..16)
            .map(|i| {
                let q = Arc::clone(&q);
                std::thread::spawn(move || {
                    let t0 = Instant::now();
                    let _ = q.try_enqueue_json(format!("line-{i}"));
                    assert!(
                        t0.elapsed() < Duration::from_millis(100),
                        "caller thread must not block on JsonlWriter mutex"
                    );
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        hold.store(false, AOrdering::Relaxed);
        std::thread::sleep(Duration::from_millis(100));
        drop(q);
    }

    #[test]
    fn jsonl_retention_deletes_only_expired_closed_files() {
        use std::time::UNIX_EPOCH;

        let dir = tempdir().unwrap();
        let open_path = dir.path().join("retention-open.jsonl");
        let old_path = dir.path().join("retention-20250101.jsonl");
        let recent_path = dir.path().join("retention-20250822.jsonl");

        fs::write(&open_path, b"open\n").unwrap();
        fs::write(&old_path, b"old\n").unwrap();
        fs::write(&recent_path, b"recent\n").unwrap();

        let old_time = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let recent_time = SystemTime::now() - Duration::from_secs(3600);
        fs::File::open(&old_path)
            .unwrap()
            .set_modified(old_time)
            .unwrap();
        fs::File::open(&recent_path)
            .unwrap()
            .set_modified(recent_time)
            .unwrap();

        let (deleted_files, deleted_bytes) =
            prune_expired_jsonl_files(dir.path(), "retention", 24, Some(&open_path)).unwrap();

        assert_eq!(deleted_files, 1);
        assert!(deleted_bytes > 0);
        assert!(!old_path.exists());
        assert!(recent_path.exists());
        assert!(open_path.exists());
    }
}
