//! md-state command enum — single-writer tracking mutations (Phase-R-R2).

use crate::market_data::track::TrackPinReason;
use solana_sdk::pubkey::Pubkey;

/// Phase-R-R2: commands for the single-writer `md-state` thread (was Tokio Geyser tracking actor).
pub enum MdStateCommand {
    TrackMint {
        mint: Pubkey,
        pin: Option<TrackPinReason>,
    },
    /// PR169b: wallet bootstrap / execution-results mint pin (no immediate sync).
    TrackWalletMint { mint: Pubkey },
    /// PR169b: `max_tracked_accounts` cap change — debounced flush/eviction only.
    ScheduleGeyserSyncAfterConfigChange,
    /// PR233: debounced explicit Geyser sync flush — executed only on `md-state` thread.
    FlushGeyserSyncDebounced,
    /// PR234: resume budgeted LRU eviction + partial sync after prior flush exhausted budget.
    ContinueGeyserEvict,
    /// Phase-R-R2: LRU touch only (no subscription change). Legacy singles coalesced into batch.
    #[allow(dead_code)]
    TouchVault(Pubkey),
    #[allow(dead_code)]
    TouchBinArray(Pubkey),
    /// Batched LRU touches flushed once per md-sidefx burst (deduped vault/bin pubkeys).
    TouchTrackedLruBatch {
        vaults: Vec<Pubkey>,
        bin_arrays: Vec<Pubkey>,
    },
    /// PR237: superseded by md-sidefx `TradePoolLruTouch` on trade path; kept for queued jobs.
    #[allow(dead_code)]
    TouchPool(Pubkey),
}
