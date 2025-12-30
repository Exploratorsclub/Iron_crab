//! IPC Schema Module – shared types for inter-process communication
//!
//! Source of Truth: docs/STORAGE_CONVENTIONS.md + docs/TARGET_ARCHITECTURE.md
//!
//! All types here are versioned (`schema_version`) and serializable for:
//! - NATS pub/sub (JSON or bincode)
//! - Append-only JSONL files for replay/forensics

pub mod schema;
pub mod reason_codes;

pub use schema::*;
pub use reason_codes::*;
