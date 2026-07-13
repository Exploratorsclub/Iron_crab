//! Bounded track-worker protocol: immutable typed commands with monotone per-stream revisions.

use super::snapshot::ExplicitSetSnapshot;
use crate::nats::{ArbTrackRequestsUpdate, MomentumActivePoolsUpdate};
use solana_sdk::pubkey::Pubkey;

/// Pin reason for explicit Geyser tracking (maps to [`super::ConsumerId`] in rebuild).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrackPinReason {
    Wallet,
    MomentumActive,
    ArbMultiDex,
}

/// Legacy commands for the `md-track-worker` OS thread (DesiredExplicitSet + coalesced Geyser push).
#[derive(Debug, Clone)]
pub enum TrackWorkerCommand {
    ApplyMomentumActivePools(MomentumActivePoolsUpdate),
    ApplyArbTrackRequests(ArbTrackRequestsUpdate),
    ApplyWalletPin {
        mint: Pubkey,
    },
    TrackMint {
        mint: Pubkey,
        pin: Option<TrackPinReason>,
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

/// Consumer/stream identity for monotone revision assignment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TrackCommandStream {
    Momentum = 0,
    Arb = 1,
    Control = 2,
}

impl TrackCommandStream {
    pub const COUNT: usize = 3;

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

/// Map a legacy command to its protocol stream.
pub fn stream_for_command(cmd: &TrackWorkerCommand) -> TrackCommandStream {
    match cmd {
        TrackWorkerCommand::ApplyMomentumActivePools(_) => TrackCommandStream::Momentum,
        TrackWorkerCommand::ApplyArbTrackRequests(_) => TrackCommandStream::Arb,
        TrackWorkerCommand::ApplyWalletPin { .. }
        | TrackWorkerCommand::TrackMint { .. }
        | TrackWorkerCommand::ScheduleGeyserSyncAfterConfigChange
        | TrackWorkerCommand::ScheduleGeyserPush
        | TrackWorkerCommand::ScheduleGeyserPushDebounced
        | TrackWorkerCommand::RestoreExplicitSnapshot(_)
        | TrackWorkerCommand::ContinueGeyserEvict => TrackCommandStream::Control,
    }
}

/// Monotone revision counter per stream (starts at 1).
#[derive(Debug, Clone, Default)]
pub struct RevisionAssigner {
    next: [u64; TrackCommandStream::COUNT],
}

impl RevisionAssigner {
    pub fn next_revision(&mut self, stream: TrackCommandStream) -> u64 {
        let idx = stream.index();
        let rev = self.next[idx].saturating_add(1);
        self.next[idx] = rev;
        rev
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
                TrackCommandStream::Control,
            ),
        ];
        for (cmd, expected) in streams {
            assert_eq!(stream_for_command(&cmd), expected);
        }
    }

    #[test]
    fn revision_assigner_is_monotone_per_stream() {
        let mut assigner = RevisionAssigner::default();
        let r1 = assigner.next_revision(TrackCommandStream::Momentum);
        let r2 = assigner.next_revision(TrackCommandStream::Momentum);
        let a1 = assigner.next_revision(TrackCommandStream::Arb);
        assert!(r2 > r1);
        assert_eq!(a1, 1);
        assert_eq!(r1, 1);
        assert_eq!(r2, 2);
    }
}
