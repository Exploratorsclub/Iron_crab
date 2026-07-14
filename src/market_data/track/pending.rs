//! Bounded pending / inflight / revision store for track-worker queue-full replay.

use super::worker_commands::{
    stream_for_command, ImmutableTrackCommand, RevisionAssigner, TrackCommandStream,
    TrackWorkerCommand,
};
use crate::metrics::{
    inc_market_data_track_protocol_pending_evicted_total,
    inc_market_data_track_protocol_replay_triggers_total,
    inc_market_data_track_protocol_superseded_revisions_total,
    set_market_data_track_protocol_inflight_depth, set_market_data_track_protocol_pending_depth,
};

/// Bounded cap for pending slots when the worker queue is full (I-MD-5).
pub const MARKET_DATA_TRACK_PENDING_CAP: usize = 512;
/// Bounded cap for in-flight protocol commands (queued + pending).
pub const MARKET_DATA_TRACK_INFLIGHT_CAP: usize = 1024;

/// Result of staging a command when the worker queue is full.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StageResult {
    /// Command preserved in pending store; replay will drain it.
    Staged,
    /// Could not preserve (should not occur with eviction policy).
    Lost,
}

/// Bounded store: pending slots, inflight accounting, last-applied revision per stream.
#[derive(Debug)]
pub struct BoundedProtocolStore {
    pending: Vec<ImmutableTrackCommand>,
    pending_cap: usize,
    inflight: usize,
    #[allow(dead_code)]
    inflight_cap: usize,
    revision: RevisionAssigner,
    last_applied: [u64; TrackCommandStream::COUNT],
}

impl BoundedProtocolStore {
    pub fn new(pending_cap: usize, inflight_cap: usize) -> Self {
        Self {
            pending: Vec::with_capacity(pending_cap.min(64)),
            pending_cap,
            inflight: 0,
            inflight_cap,
            revision: RevisionAssigner::default(),
            last_applied: [0; TrackCommandStream::COUNT],
        }
    }

    pub fn default_caps() -> Self {
        Self::new(
            MARKET_DATA_TRACK_PENDING_CAP,
            MARKET_DATA_TRACK_INFLIGHT_CAP,
        )
    }

    #[inline]
    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    #[inline]
    pub fn inflight_len(&self) -> usize {
        self.inflight
    }

    /// Wrap a legacy command with the next monotone revision for its stream.
    pub fn wrap_command(&mut self, payload: TrackWorkerCommand) -> ImmutableTrackCommand {
        let stream = stream_for_command(&payload);
        let revision = self.revision.next_revision(stream);
        ImmutableTrackCommand::new(stream, revision, payload)
    }

    /// True when `revision` is strictly newer than the last applied revision for `stream`.
    pub fn is_applicable(&self, stream: TrackCommandStream, revision: u64) -> bool {
        revision > self.last_applied[stream.index()]
    }

    pub fn mark_applied(&mut self, stream: TrackCommandStream, revision: u64) {
        let idx = stream.index();
        if revision > self.last_applied[idx] {
            self.last_applied[idx] = revision;
        }
    }

    pub fn begin_inflight(&mut self) {
        self.inflight = self.inflight.saturating_add(1);
        set_market_data_track_protocol_inflight_depth(self.inflight);
    }

    pub fn end_inflight(&mut self) {
        self.inflight = self.inflight.saturating_sub(1);
        set_market_data_track_protocol_inflight_depth(self.inflight);
    }

    fn refresh_pending_depth_metric(&self) {
        set_market_data_track_protocol_pending_depth(self.pending.len());
    }

    /// Stage command in bounded pending when the worker queue is full (I-MD-5).
    pub fn stage_on_queue_full(&mut self, cmd: ImmutableTrackCommand) -> StageResult {
        inc_market_data_track_protocol_replay_triggers_total();
        if self.pending.len() < self.pending_cap {
            self.pending.push(cmd);
            self.refresh_pending_depth_metric();
            return StageResult::Staged;
        }
        // Pending full: deterministically evict oldest slot to make room (bounded, no silent drop).
        if !self.pending.is_empty() {
            self.pending.remove(0);
            inc_market_data_track_protocol_pending_evicted_total();
        }
        self.pending.push(cmd);
        self.refresh_pending_depth_metric();
        StageResult::Staged
    }

    /// Extract applicable pending commands in staging order (per-stream revision order).
    pub fn take_applicable_pending_sorted(&mut self) -> Vec<ImmutableTrackCommand> {
        if self.pending.is_empty() {
            return Vec::new();
        }
        let drained: Vec<ImmutableTrackCommand> = self.pending.drain(..).collect();
        let mut stream_order = Vec::new();
        for cmd in &drained {
            if !stream_order.contains(&cmd.stream) {
                stream_order.push(cmd.stream);
            }
        }
        let mut applicable = Vec::new();
        for stream in stream_order {
            let mut stream_cmds: Vec<ImmutableTrackCommand> = drained
                .iter()
                .filter(|cmd| cmd.stream == stream)
                .cloned()
                .collect();
            stream_cmds.sort_by_key(|cmd| cmd.revision);
            for cmd in stream_cmds {
                if self.is_applicable(cmd.stream, cmd.revision) {
                    applicable.push(cmd);
                } else {
                    inc_market_data_track_protocol_superseded_revisions_total();
                }
            }
        }
        self.refresh_pending_depth_metric();
        applicable
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nats::MomentumActivePoolsUpdate;
    use solana_sdk::pubkey::Pubkey;

    fn momentum_cmd(version: u32) -> TrackWorkerCommand {
        TrackWorkerCommand::ApplyMomentumActivePools(MomentumActivePoolsUpdate {
            version,
            ts_unix_ms: 0,
            active: vec![],
            removed: vec![],
            full_active_snapshot: false,
        })
    }

    #[test]
    fn queue_full_stages_in_pending_no_loss() {
        let mut store = BoundedProtocolStore::new(8, 64);
        let cmd = store.wrap_command(momentum_cmd(1));
        assert_eq!(store.stage_on_queue_full(cmd.clone()), StageResult::Staged);
        assert_eq!(store.pending_len(), 1);
        assert!(store.pending.iter().any(|c| c.revision == cmd.revision));
    }

    #[test]
    fn replay_drain_applies_staged_commands() {
        let mut store = BoundedProtocolStore::new(8, 64);
        let c1 = store.wrap_command(momentum_cmd(1));
        let c2 = store.wrap_command(momentum_cmd(2));
        store.stage_on_queue_full(c1.clone());
        store.stage_on_queue_full(c2.clone());
        let mut applied = Vec::new();
        for cmd in store.take_applicable_pending_sorted() {
            store.mark_applied(cmd.stream, cmd.revision);
            applied.push(cmd.revision);
        }
        assert_eq!(applied, vec![1, 2]);
        assert_eq!(store.pending_len(), 0);
    }

    #[test]
    fn stale_revision_ignored_idempotently() {
        let mut store = BoundedProtocolStore::new(8, 64);
        let c1 = store.wrap_command(momentum_cmd(1));
        let c2 = store.wrap_command(momentum_cmd(2));
        store.mark_applied(TrackCommandStream::Momentum, 2);
        store.stage_on_queue_full(c1);
        store.stage_on_queue_full(c2);
        let mut applied = Vec::new();
        for cmd in store.take_applicable_pending_sorted() {
            if store.is_applicable(cmd.stream, cmd.revision) {
                store.mark_applied(cmd.stream, cmd.revision);
                applied.push(cmd.revision);
            }
        }
        assert!(applied.is_empty());
        assert_eq!(store.pending_len(), 0);
    }

    #[test]
    fn out_of_order_pending_sorted_by_revision() {
        let mut store = BoundedProtocolStore::new(8, 64);
        let c1 = ImmutableTrackCommand::new(TrackCommandStream::Momentum, 1, momentum_cmd(1));
        let c2 = ImmutableTrackCommand::new(TrackCommandStream::Momentum, 2, momentum_cmd(2));
        let c3 = ImmutableTrackCommand::new(TrackCommandStream::Momentum, 3, momentum_cmd(3));
        store.stage_on_queue_full(c3);
        store.stage_on_queue_full(c1);
        store.stage_on_queue_full(c2);
        let mut applied = Vec::new();
        for cmd in store.take_applicable_pending_sorted() {
            store.mark_applied(cmd.stream, cmd.revision);
            applied.push(cmd.revision);
        }
        assert_eq!(applied, vec![1, 2, 3]);
    }

    #[test]
    fn pending_cap_bounded_by_eviction() {
        let cap = 4;
        let mut store = BoundedProtocolStore::new(cap, 64);
        for v in 0..cap + 2 {
            let cmd = store.wrap_command(momentum_cmd(v as u32));
            assert_eq!(store.stage_on_queue_full(cmd), StageResult::Staged);
        }
        assert!(store.pending_len() <= cap);
    }

    #[test]
    fn all_indices_bounded() {
        let mut store = BoundedProtocolStore::new(
            MARKET_DATA_TRACK_PENDING_CAP,
            MARKET_DATA_TRACK_INFLIGHT_CAP,
        );
        for v in 0..MARKET_DATA_TRACK_PENDING_CAP + 4 {
            let cmd = store.wrap_command(momentum_cmd(v as u32));
            store.stage_on_queue_full(cmd);
        }
        assert!(store.pending_len() <= MARKET_DATA_TRACK_PENDING_CAP);
        assert_eq!(TrackCommandStream::COUNT, 5);
    }

    #[test]
    fn wallet_withdraw_supersedes_older_pending_pin() {
        let mut store = BoundedProtocolStore::new(8, 64);
        let mint = Pubkey::new_unique();
        let pin_old = store.wrap_command(TrackWorkerCommand::ApplyWalletPin { mint });
        store.stage_on_queue_full(pin_old.clone());
        let withdraw = store.wrap_command(TrackWorkerCommand::WithdrawWalletPin { mint });
        store.mark_applied(withdraw.stream, withdraw.revision);
        assert!(!store.is_applicable(pin_old.stream, pin_old.revision));
        let applicable = store.take_applicable_pending_sorted();
        assert!(
            applicable.is_empty(),
            "stale wallet pin must not replay after newer withdraw advances watermark"
        );
    }

    #[test]
    fn tracker_stream_separate_from_control() {
        let mut store = BoundedProtocolStore::new(8, 64);
        let mint = Pubkey::new_unique();
        let tracker = store.wrap_command(TrackWorkerCommand::TrackMint { mint, pin: None });
        let control = store.wrap_command(TrackWorkerCommand::ScheduleGeyserPush);
        assert_eq!(tracker.stream, TrackCommandStream::Tracker);
        assert_eq!(control.stream, TrackCommandStream::Control);
        assert_eq!(tracker.revision, 1);
        assert_eq!(control.revision, 1);
    }
}
