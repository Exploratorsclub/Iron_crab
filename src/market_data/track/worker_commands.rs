//! Track-worker command definitions (shared between worker loop and producers).

use solana_sdk::pubkey::Pubkey;
use std::collections::HashSet;

use super::desired_set::{ConsumerId, OwnerKey};
use super::snapshot::ExplicitSetSnapshot;
use crate::nats::{ArbTrackRequestsUpdate, MomentumActivePoolsUpdate};

/// Geyser pin reason used by track-worker pool registration commands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeyserPinReason {
    Wallet,
    MomentumActive,
    ArbMultiDex,
}

/// Immutable pool/account snapshot captured at enqueue time (admission + worker commit SSOT).
#[derive(Debug, Clone)]
pub struct PoolExplicitSnapshot {
    pub pool: Pubkey,
    pub pubkeys: HashSet<Pubkey>,
    pub consumer: ConsumerId,
    pub owner: OwnerKey,
    pub pin: GeyserPinReason,
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
    /// Worker reads authoritative [`super::pending::WalletExplicitPending`] by revision.
    SyncWalletExplicitDemand {
        revision: u64,
    },
    RegisterPoolGeyserReserves {
        snapshot: PoolExplicitSnapshot,
    },
    RegisterPoolVaultsFromAccount {
        snapshot: PoolExplicitSnapshot,
    },
    RegisterGeyserReservesAfterTrade {
        snapshot: PoolExplicitSnapshot,
    },
    RefreshDlmmBinWindow {
        snapshot: PoolExplicitSnapshot,
        new_active_id: i32,
    },
    ScheduleGeyserSyncAfterConfigChange,
    /// Coalesced explicit Geyser push (md-state burst / trade path).
    ScheduleGeyserPush,
    /// Debounced push after `try_acquire_geyser_sync_flush_slot` (rate-limited TX debounce thread).
    ScheduleGeyserPushDebounced,
    /// Phase 3 P3: restore explicit set from on-disk snapshot (I-MD-6).
    RestoreExplicitSnapshot(ExplicitSetSnapshot),
    /// Worker signals restore barrier completion (startup).
    RestoreBarrierComplete {
        ok: bool,
    },
    ContinueGeyserEvict,
}
