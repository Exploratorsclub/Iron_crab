//! Spawn helpers for the off-thread `QueuedJsonlWriter` pipeline.

use crate::storage::{JsonlWriterConfig, QueuedJsonlWriter};
use anyhow::Result;
use std::path::Path;

/// Spawn the dedicated `jsonl-writer` OS thread wrapping [`QueuedJsonlWriter`].
pub fn spawn_market_data_jsonl_writer(
    config: JsonlWriterConfig,
    queue_cap: usize,
) -> Result<QueuedJsonlWriter> {
    Ok(QueuedJsonlWriter::spawn(config, queue_cap)?)
}

/// Convenience: build config + spawn with default market-data queue cap.
pub fn spawn_market_data_jsonl_writer_in_dir(
    log_dir: &Path,
    queue_cap: usize,
) -> Result<QueuedJsonlWriter> {
    let config = JsonlWriterConfig::new("market_events").with_log_dir(log_dir);
    spawn_market_data_jsonl_writer(config, queue_cap)
}
