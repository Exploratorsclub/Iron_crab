//! Read-only **PositionAuthority** (PA-1): pure event reducer for durable position SOT migration.
//!
//! Not connected to NATS, execution, or Momentum. See [`state::PositionAuthority`].

mod state;

pub use state::{PositionAuthority, PositionEvent, PositionState, PositionStatus, UpdateSource};
