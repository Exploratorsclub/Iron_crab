//! Bounded track-worker protocol: immutable typed commands with monotone per-stream revisions.

use super::snapshot::ExplicitSetSnapshot;
use crate::nats::{ArbTrackRequestsUpdate, MomentumActivePoolsUpdate};
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;

/// Pin reason for explicit Geyser tracking (maps to [`super::ConsumerId`] in rebuild).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrackPinReason {
    Wallet,
    MomentumActive,
    ArbMultiDex,
}

/// Commands for the `md-track-worker` OS thread (`FixedCapAdmission` SSOT + coalesced Geyser push).
#[derive(Debug, Clone)]
pub enum TrackWorkerCommand {
    ApplyMomentumActivePools(MomentumActivePoolsUpdate),
    ApplyArbTrackRequests(ArbTrackRequestsUpdate),
    ApplyWalletPin {
        mint: Pubkey,
    },
    /// PR4b: explicit wallet-pin withdrawal before tracked-map demotion (I-MD-8).
    WithdrawWalletPin {
        mint: Pubkey,
    },
    TrackMint {
        mint: Pubkey,
        pin: Option<TrackPinReason>,
    },
    /// Scope H: burst-coalesced tracker/wallet TrackMint demand (deduped by mint, single Geyser schedule).
    TrackMints {
        entries: Vec<(Pubkey, Option<TrackPinReason>)>,
    },
    ScheduleGeyserSyncAfterConfigChange,
    /// Coalesced explicit Geyser push (md-state burst / trade path).
    ScheduleGeyserPush,
    /// Debounced push after `try_acquire_geyser_sync_flush_slot` (rate-limited TX debounce thread).
    ScheduleGeyserPushDebounced,
    /// Phase 3 P3: restore explicit set from on-disk snapshot (I-MD-6).
    RestoreExplicitSnapshot(ExplicitSetSnapshot),
    ContinueGeyserEvict,
    /// Shutdown / external caller: write explicit-set snapshot using worker admission.
    FlushExplicitSetSnapshot {
        done: std::sync::mpsc::Sender<()>,
    },
    /// Scope L2: evict tracker-only owner groups under EXEC_HOT broadcast pressure (LRU).
    ShedTrackerUnderExecHotPressure {
        max_groups: usize,
    },
    /// Scope L2b: evict momentum-active (non-position) owner groups under EXEC_HOT pressure.
    ShedMomentumUnderExecHotPressure {
        max_groups: usize,
    },
    /// Scope L2b: evict arb owner groups under EXEC_HOT pressure.
    ShedArbUnderExecHotPressure {
        max_groups: usize,
    },
    /// Scope C: retry deferred hot-pool vault/bin registration after LivePoolCache fill.
    RetryDeferredHotPoolReserves,
}

/// Consumer/stream identity for monotone revision assignment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TrackCommandStream {
    Momentum = 0,
    Arb = 1,
    Wallet = 2,
    Tracker = 3,
    Control = 4,
}

impl TrackCommandStream {
    pub const COUNT: usize = 5;

    #[inline]
    pub fn index(self) -> usize {
        self as usize
    }
}

/// Immutable protocol envelope: monotone `revision` per [`TrackCommandStream`].
#[derive(Debug, Clone)]
pub struct ImmutableTrackCommand {
    pub stream: TrackCommandStream,
    pub revision: u64,
    pub payload: TrackWorkerCommand,
}

impl ImmutableTrackCommand {
    pub fn new(stream: TrackCommandStream, revision: u64, payload: TrackWorkerCommand) -> Self {
        Self {
            stream,
            revision,
            payload,
        }
    }
}

/// Protocol stream for a coalesced TrackMints batch (after wallet pins are split out).
pub fn track_mints_batch_stream(
    entries: &[(Pubkey, Option<TrackPinReason>)],
) -> TrackCommandStream {
    match entries.len() {
        0 => TrackCommandStream::Control,
        1 => stream_for_command(&TrackWorkerCommand::TrackMint {
            mint: entries[0].0,
            pin: entries[0].1,
        }),
        // Multi-mint unpinned tracker batches use Control revision (per-mint keys need demand_mint).
        _ if entries.iter().all(|(_, pin)| pin.is_none()) => TrackCommandStream::Control,
        _ => TrackCommandStream::Control,
    }
}

/// Map a legacy command to its protocol stream.
pub fn stream_for_command(cmd: &TrackWorkerCommand) -> TrackCommandStream {
    match cmd {
        TrackWorkerCommand::ApplyMomentumActivePools(_) => TrackCommandStream::Momentum,
        TrackWorkerCommand::ApplyArbTrackRequests(_) => TrackCommandStream::Arb,
        TrackWorkerCommand::ApplyWalletPin { .. }
        | TrackWorkerCommand::WithdrawWalletPin { .. } => TrackCommandStream::Wallet,
        TrackWorkerCommand::TrackMint {
            pin: Some(TrackPinReason::Wallet),
            ..
        } => TrackCommandStream::Wallet,
        TrackWorkerCommand::TrackMint { pin: None, .. } => TrackCommandStream::Tracker,
        TrackWorkerCommand::TrackMints { entries } => track_mints_batch_stream(entries),
        TrackWorkerCommand::TrackMint { .. }
        | TrackWorkerCommand::ScheduleGeyserSyncAfterConfigChange
        | TrackWorkerCommand::ScheduleGeyserPush
        | TrackWorkerCommand::ScheduleGeyserPushDebounced
        | TrackWorkerCommand::RestoreExplicitSnapshot(_)
        | TrackWorkerCommand::ContinueGeyserEvict
        | TrackWorkerCommand::ShedTrackerUnderExecHotPressure { .. }
        | TrackWorkerCommand::ShedMomentumUnderExecHotPressure { .. }
        | TrackWorkerCommand::ShedArbUnderExecHotPressure { .. }
        | TrackWorkerCommand::RetryDeferredHotPoolReserves
        | TrackWorkerCommand::FlushExplicitSetSnapshot { .. } => TrackCommandStream::Control,
    }
}

/// Coarse command kind for low-cardinality enqueue/stage metrics (no pubkey labels).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrackCommandKind {
    Momentum,
    Arb,
    Wallet,
    Tracker,
    Sync,
    Continue,
    Other,
}

impl TrackCommandKind {
    pub const COUNT: usize = 7;

    #[inline]
    pub fn index(self) -> usize {
        match self {
            Self::Momentum => 0,
            Self::Arb => 1,
            Self::Wallet => 2,
            Self::Tracker => 3,
            Self::Sync => 4,
            Self::Continue => 5,
            Self::Other => 6,
        }
    }
}

/// Relative strength for TrackMint pin merge (higher wins on coalesce).
#[inline]
pub fn track_mint_pin_strength(pin: Option<TrackPinReason>) -> u8 {
    match pin {
        Some(TrackPinReason::Wallet) => 3,
        Some(TrackPinReason::MomentumActive) => 2,
        Some(TrackPinReason::ArbMultiDex) => 1,
        None => 0,
    }
}

/// Merge two TrackMint pins; stronger pin wins (Wallet > strategy pins > unpinned tracker).
#[inline]
pub fn merge_track_mint_pin(
    existing: Option<TrackPinReason>,
    incoming: Option<TrackPinReason>,
) -> Option<TrackPinReason> {
    if track_mint_pin_strength(incoming) >= track_mint_pin_strength(existing) {
        incoming
    } else {
        existing
    }
}

/// Relative strength for idempotent Geyser sync intents (higher wins on merge).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SyncIntentStrength {
    Debounced = 0,
    Push = 1,
    ConfigChange = 2,
}

#[inline]
pub fn track_command_kind(cmd: &TrackWorkerCommand) -> TrackCommandKind {
    match cmd {
        TrackWorkerCommand::ApplyMomentumActivePools(_) => TrackCommandKind::Momentum,
        TrackWorkerCommand::ApplyArbTrackRequests(_) => TrackCommandKind::Arb,
        TrackWorkerCommand::ApplyWalletPin { .. }
        | TrackWorkerCommand::WithdrawWalletPin { .. } => TrackCommandKind::Wallet,
        TrackWorkerCommand::TrackMint {
            pin: Some(TrackPinReason::Wallet),
            ..
        } => TrackCommandKind::Wallet,
        TrackWorkerCommand::TrackMint { pin: None, .. } => TrackCommandKind::Tracker,
        TrackWorkerCommand::TrackMints { .. } => TrackCommandKind::Tracker,
        TrackWorkerCommand::ScheduleGeyserSyncAfterConfigChange
        | TrackWorkerCommand::ScheduleGeyserPush
        | TrackWorkerCommand::ScheduleGeyserPushDebounced => TrackCommandKind::Sync,
        TrackWorkerCommand::ContinueGeyserEvict => TrackCommandKind::Continue,
        _ => TrackCommandKind::Other,
    }
}

#[inline]
pub fn sync_intent_strength(cmd: &TrackWorkerCommand) -> Option<SyncIntentStrength> {
    match cmd {
        TrackWorkerCommand::ScheduleGeyserPushDebounced => Some(SyncIntentStrength::Debounced),
        TrackWorkerCommand::ScheduleGeyserPush => Some(SyncIntentStrength::Push),
        TrackWorkerCommand::ScheduleGeyserSyncAfterConfigChange => {
            Some(SyncIntentStrength::ConfigChange)
        }
        _ => None,
    }
}

#[inline]
pub fn is_continue_evict(cmd: &TrackWorkerCommand) -> bool {
    matches!(cmd, TrackWorkerCommand::ContinueGeyserEvict)
}

/// Merge multiple sync intents into a single strongest payload (push-needed semantics).
pub fn merge_sync_intent_payloads(commands: &[TrackWorkerCommand]) -> TrackWorkerCommand {
    let mut strength = SyncIntentStrength::Debounced;
    for cmd in commands {
        if let Some(s) = sync_intent_strength(cmd) {
            strength = strength.max(s);
        }
    }
    match strength {
        SyncIntentStrength::ConfigChange => TrackWorkerCommand::ScheduleGeyserSyncAfterConfigChange,
        SyncIntentStrength::Push => TrackWorkerCommand::ScheduleGeyserPush,
        SyncIntentStrength::Debounced => TrackWorkerCommand::ScheduleGeyserPushDebounced,
    }
}

/// Per-mint demand key for wallet/tracker streams (supersession scoped per mint).
pub fn demand_mint_for_command(cmd: &TrackWorkerCommand) -> Option<Pubkey> {
    match cmd {
        TrackWorkerCommand::ApplyWalletPin { mint }
        | TrackWorkerCommand::WithdrawWalletPin { mint } => Some(*mint),
        TrackWorkerCommand::TrackMint {
            mint,
            pin: None | Some(TrackPinReason::Wallet),
        } => Some(*mint),
        _ => None,
    }
}

#[inline]
pub fn stream_uses_per_mint_revision(stream: TrackCommandStream) -> bool {
    matches!(
        stream,
        TrackCommandStream::Wallet | TrackCommandStream::Tracker
    )
}

/// Monotone revision counter per stream (starts at 1); wallet/tracker use per-mint counters.
#[derive(Debug, Clone, Default)]
pub struct RevisionAssigner {
    next: [u64; TrackCommandStream::COUNT],
    wallet_mint_next: HashMap<Pubkey, u64>,
    tracker_mint_next: HashMap<Pubkey, u64>,
}

impl RevisionAssigner {
    pub fn next_revision(
        &mut self,
        stream: TrackCommandStream,
        demand_mint: Option<Pubkey>,
    ) -> u64 {
        if stream_uses_per_mint_revision(stream) {
            let mint = demand_mint.expect("wallet/tracker revision requires demand mint");
            let map = match stream {
                TrackCommandStream::Wallet => &mut self.wallet_mint_next,
                TrackCommandStream::Tracker => &mut self.tracker_mint_next,
                _ => unreachable!(),
            };
            let rev = map
                .entry(mint)
                .and_modify(|r| *r = r.saturating_add(1))
                .or_insert(1);
            return *rev;
        }
        let idx = stream.index();
        let rev = self.next[idx].saturating_add(1);
        self.next[idx] = rev;
        rev
    }

    /// Highest revision already assigned for a per-mint wallet/tracker stream (0 if none).
    pub fn last_issued_revision_for_mint(&self, stream: TrackCommandStream, mint: Pubkey) -> u64 {
        let map = match stream {
            TrackCommandStream::Wallet => &self.wallet_mint_next,
            TrackCommandStream::Tracker => &self.tracker_mint_next,
            _ => return 0,
        };
        *map.get(&mint).unwrap_or(&0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_mapping_covers_all_variants() {
        let mint = Pubkey::new_unique();
        let streams = [
            (
                TrackWorkerCommand::ApplyMomentumActivePools(MomentumActivePoolsUpdate {
                    version: 1,
                    ts_unix_ms: 0,
                    active: vec![],
                    removed: vec![],
                    full_active_snapshot: false,
                }),
                TrackCommandStream::Momentum,
            ),
            (
                TrackWorkerCommand::ApplyArbTrackRequests(ArbTrackRequestsUpdate {
                    version: 1,
                    ts_unix_ms: 0,
                    active: vec![],
                    removed: vec![],
                    reconcile: false,
                }),
                TrackCommandStream::Arb,
            ),
            (
                TrackWorkerCommand::TrackMint { mint, pin: None },
                TrackCommandStream::Tracker,
            ),
            (
                TrackWorkerCommand::ApplyWalletPin { mint },
                TrackCommandStream::Wallet,
            ),
            (
                TrackWorkerCommand::TrackMint {
                    mint,
                    pin: Some(TrackPinReason::Wallet),
                },
                TrackCommandStream::Wallet,
            ),
            (
                TrackWorkerCommand::TrackMint {
                    mint,
                    pin: Some(TrackPinReason::MomentumActive),
                },
                TrackCommandStream::Control,
            ),
        ];
        for (cmd, expected) in streams {
            assert_eq!(stream_for_command(&cmd), expected);
        }
    }

    #[test]
    fn merge_track_mint_pin_prefers_wallet_over_unpinned() {
        use super::merge_track_mint_pin;
        assert_eq!(
            merge_track_mint_pin(None, Some(TrackPinReason::Wallet)),
            Some(TrackPinReason::Wallet)
        );
        assert_eq!(
            merge_track_mint_pin(Some(TrackPinReason::Wallet), None),
            Some(TrackPinReason::Wallet)
        );
    }

    #[test]
    fn stream_mapping_track_mints_single_unpinned_is_tracker() {
        let mint = Pubkey::new_unique();
        assert_eq!(
            stream_for_command(&TrackWorkerCommand::TrackMints {
                entries: vec![(mint, None)],
            }),
            TrackCommandStream::Tracker
        );
    }

    #[test]
    fn stream_mapping_track_mints_multi_unpinned_is_control() {
        let m1 = Pubkey::new_unique();
        let m2 = Pubkey::new_unique();
        assert_eq!(
            stream_for_command(&TrackWorkerCommand::TrackMints {
                entries: vec![(m1, None), (m2, None)],
            }),
            TrackCommandStream::Control
        );
    }

    #[test]
    fn revision_assigner_is_monotone_per_stream() {
        let mut assigner = RevisionAssigner::default();
        let r1 = assigner.next_revision(TrackCommandStream::Momentum, None);
        let r2 = assigner.next_revision(TrackCommandStream::Momentum, None);
        let a1 = assigner.next_revision(TrackCommandStream::Arb, None);
        assert!(r2 > r1);
        assert_eq!(a1, 1);
        assert_eq!(r1, 1);
        assert_eq!(r2, 2);
    }

    #[test]
    fn revision_assigner_wallet_tracker_is_per_mint() {
        let mut assigner = RevisionAssigner::default();
        let m1 = Pubkey::new_unique();
        let m2 = Pubkey::new_unique();
        let w1a = assigner.next_revision(TrackCommandStream::Wallet, Some(m1));
        let w2a = assigner.next_revision(TrackCommandStream::Wallet, Some(m2));
        let w1b = assigner.next_revision(TrackCommandStream::Wallet, Some(m1));
        let t1a = assigner.next_revision(TrackCommandStream::Tracker, Some(m1));
        let t2a = assigner.next_revision(TrackCommandStream::Tracker, Some(m2));
        assert_eq!(w1a, 1);
        assert_eq!(w2a, 1);
        assert_eq!(w1b, 2);
        assert_eq!(t1a, 1);
        assert_eq!(t2a, 1);
    }

    #[test]
    fn merge_sync_intent_prefers_config_change_then_push() {
        use super::merge_sync_intent_payloads;
        let merged = merge_sync_intent_payloads(&[
            TrackWorkerCommand::ScheduleGeyserPushDebounced,
            TrackWorkerCommand::ScheduleGeyserPush,
            TrackWorkerCommand::ScheduleGeyserSyncAfterConfigChange,
        ]);
        assert!(matches!(
            merged,
            TrackWorkerCommand::ScheduleGeyserSyncAfterConfigChange
        ));
        let merged2 = merge_sync_intent_payloads(&[
            TrackWorkerCommand::ScheduleGeyserPushDebounced,
            TrackWorkerCommand::ScheduleGeyserPush,
        ]);
        assert!(matches!(merged2, TrackWorkerCommand::ScheduleGeyserPush));
    }
}
