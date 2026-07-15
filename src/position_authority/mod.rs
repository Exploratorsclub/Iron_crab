//! Read-only **PositionAuthority** reducer + JetStream snapshot types (PA-1 / PA-5.1).
//! EE publishes KV snapshots; Momentum consumes readonly.

mod state;

pub use state::{
    is_sol_or_wsol_mint, position_authority_drift_lockmanager, position_authority_drift_momentum,
    PositionAuthority, PositionAuthorityChange, PositionEvent, PositionState, PositionStatus,
    UpdateSource,
};
