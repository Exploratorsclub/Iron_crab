//! Track-worker command definitions (shared between worker loop and producers).

use solana_sdk::pubkey::Pubkey;
use std::collections::HashSet;

use super::snapshot::ExplicitSetSnapshot;
use crate::nats::{ArbTrackRequestsUpdate, MomentumActivePoolsUpdate};

/// Geyser pin reason used by track-worker pool registration commands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeyserPinReason {
    Wallet,
    MomentumActive,
    ArbMultiDex,
}

/// Phase 2a: commands for the `md-track-worker` OS thread (DesiredExplicitSet + coalesced Geyser push).
pub enum TrackWorkerCommand {
    ApplyMomentumActivePools(MomentumActivePoolsUpdate),
    ApplyArbTrackRequests(ArbTrackRequestsUpdate),
    ApplyWalletPin {
        mint: Pubkey,
    },
    TrackMint {
        mint: Pubkey,
        pin: Option<GeyserPinReason>,
    },
    SyncWalletExplicitDemand {
        demand: HashSet<Pubkey>,
        token_accounts: HashSet<Pubkey>,
    },
    RegisterPoolGeyserReserves {
        pool: Pubkey,
        pin: GeyserPinReason,
    },
    RegisterPoolVaultsFromAccount {
        pool: Pubkey,
    },
    RegisterGeyserReservesAfterTrade {
        pool: Pubkey,
    },
    RefreshDlmmBinWindow {
        pool: Pubkey,
        new_active_id: i32,
    },
    ScheduleGeyserSyncAfterConfigChange,
    /// Coalesced explicit Geyser push (md-state burst / trade path).
    ScheduleGeyserPush,
    /// Debounced push after `try_acquire_geyser_sync_flush_slot` (rate-limited TX debounce thread).
    ScheduleGeyserPushDebounced,
    /// Phase 3 P3: restore explicit set from on-disk snapshot (I-MD-6).
    RestoreExplicitSnapshot(ExplicitSetSnapshot),
    ContinueGeyserEvict,
}
