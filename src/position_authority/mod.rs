//! **PositionAuthority** reducer + JetStream snapshot types (PA-1 / PA-5.1 / PA-6b).
//! `position-manager` is the sole JetStream KV writer; Momentum consumes readonly.

mod kv_publish;
mod state;

pub use kv_publish::{
    publish_position_authority_changes_to_kv, reconcile_position_authority_kv_after_restart,
    wallet_bootstrap_allows_pa_kv_tombstone_sweep, PositionAuthorityKvMetricsSink,
    PositionAuthorityKvPublisher,
};
pub use state::{
    is_sol_or_wsol_mint, open_positions_count_from_kv_snapshots, position_authority_drift_ee_vs_kv,
    position_authority_drift_lockmanager, position_authority_drift_momentum,
    snapshot_counts_as_open_position, PositionAuthority, PositionAuthorityChange, PositionEvent,
    PositionState, PositionStatus, UpdateSource,
};
