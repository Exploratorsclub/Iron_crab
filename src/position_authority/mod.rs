//! Read-only **PositionAuthority** (PA-1): pure event reducer for durable position SOT migration.
//!
//! Not connected to NATS, execution, or Momentum. See [`state::PositionAuthority`].

mod state;

pub use state::{
    is_sol_or_wsol_mint, position_authority_drift_lockmanager, PositionAuthority, PositionEvent,
    PositionState, PositionStatus, UpdateSource,
};
