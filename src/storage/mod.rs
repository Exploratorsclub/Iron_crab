//! Storage Module – append-only JSONL writers for replay/forensics
//!
//! Source of Truth: docs/STORAGE_CONVENTIONS.md
//!
//! Key requirements:
//! - Hot-path safe: no blocking on writes in trading path
//! - Append-only: no in-place updates
//! - Daily rotation (UTC)
//! - Schema-versioned records

pub mod jsonl_writer;
pub mod locks;

pub use jsonl_writer::*;
pub use locks::*;
