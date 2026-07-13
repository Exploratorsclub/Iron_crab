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

/// Immutable vault row captured at enqueue time.
#[derive(Debug, Clone)]
pub struct VaultExplicitRow {
    pub pubkey: Pubkey,
    pub dex: String,
    pub base_mint: Pubkey,
    pub quote_mint: Pubkey,
    pub is_base_vault: bool,
    pub sibling_vault: Option<Pubkey>,
    pub active_id: Option<i32>,
    pub bin_step: Option<u16>,
}

/// Immutable DLMM bin-array row captured at enqueue time.
#[derive(Debug, Clone)]
pub struct BinArrayExplicitRow {
    pub pubkey: Pubkey,
    pub bin_array_index: i64,
    pub bin_step: u16,
}

/// Immutable mint row captured at enqueue time (tracked_mints, not vaults).
#[derive(Debug, Clone)]
pub struct MintExplicitRow {
    pub pubkey: Pubkey,
}

/// Immutable pool/account snapshot captured at enqueue time (admission + worker commit SSOT).
#[derive(Debug, Clone)]
pub struct PoolExplicitSnapshot {
    pub pool: Pubkey,
    pub vaults: Vec<VaultExplicitRow>,
    pub bin_arrays: Vec<BinArrayExplicitRow>,
    pub mints: Vec<MintExplicitRow>,
    pub consumer: ConsumerId,
    pub owner: OwnerKey,
    pub pin: GeyserPinReason,
    /// Monotonic sequence assigned at enqueue/pending-stash time (stale replay guard).
    pub revision: u64,
    /// When set, enqueue success may resolve exactly this ledger rejection (token + payload).
    pub rejection_ledger_token: Option<u64>,
}

impl PoolExplicitSnapshot {
    pub fn all_pubkeys(&self) -> HashSet<Pubkey> {
        let mut out = HashSet::new();
        for v in &self.vaults {
            out.insert(v.pubkey);
        }
        for b in &self.bin_arrays {
            out.insert(b.pubkey);
        }
        for m in &self.mints {
            out.insert(m.pubkey);
        }
        out
    }
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
    /// Startup barrier without persisted snapshot (wallet demand + convergence + physical publish).
    CompleteStartupBarrier,
    /// Worker signals restore barrier completion (startup).
    RestoreBarrierComplete {
        ok: bool,
    },
    ContinueGeyserEvict,
}
