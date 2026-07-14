//! Bounded pending / inflight / revision store for track-worker queue-full replay.

use super::worker_commands::{
    demand_mint_for_command, stream_for_command, stream_uses_per_mint_revision,
    ImmutableTrackCommand, RevisionAssigner, TrackCommandStream, TrackWorkerCommand,
};
use crate::metrics::{
    inc_market_data_track_protocol_pending_evicted_total,
    inc_market_data_track_protocol_replay_triggers_total,
    inc_market_data_track_protocol_superseded_revisions_total,
    set_market_data_track_protocol_inflight_depth, set_market_data_track_protocol_pending_depth,
};
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;

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

/// Bounded store: pending slots, inflight accounting, last-applied revision per stream/mint.
#[derive(Debug)]
pub struct BoundedProtocolStore {
    pending: Vec<ImmutableTrackCommand>,
    pending_cap: usize,
    inflight: usize,
    #[allow(dead_code)]
    inflight_cap: usize,
    revision: RevisionAssigner,
    last_applied: [u64; TrackCommandStream::COUNT],
    last_applied_wallet_mint: HashMap<Pubkey, u64>,
    last_applied_tracker_mint: HashMap<Pubkey, u64>,
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
            last_applied_wallet_mint: HashMap::new(),
            last_applied_tracker_mint: HashMap::new(),
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

    /// Wrap a legacy command with the next monotone revision for its stream (per-mint for wallet/tracker).
    pub fn wrap_command(&mut self, payload: TrackWorkerCommand) -> ImmutableTrackCommand {
        let stream = stream_for_command(&payload);
        let demand_mint = demand_mint_for_command(&payload);
        let revision = self.revision.next_revision(stream, demand_mint);
        ImmutableTrackCommand::new(stream, revision, payload)
    }

    fn last_applied_for_cmd(&self, cmd: &ImmutableTrackCommand) -> u64 {
        if stream_uses_per_mint_revision(cmd.stream) {
            let mint = demand_mint_for_command(&cmd.payload)
                .expect("wallet/tracker command requires demand mint");
            let map = match cmd.stream {
                TrackCommandStream::Wallet => &self.last_applied_wallet_mint,
                TrackCommandStream::Tracker => &self.last_applied_tracker_mint,
                _ => unreachable!(),
            };
            *map.get(&mint).unwrap_or(&0)
        } else {
            self.last_applied[cmd.stream.index()]
        }
    }

    /// True when `cmd.revision` is strictly newer than the last applied revision for its demand key.
    pub fn is_applicable(&self, cmd: &ImmutableTrackCommand) -> bool {
        cmd.revision > self.last_applied_for_cmd(cmd)
    }

    pub fn mark_applied(&mut self, cmd: &ImmutableTrackCommand) {
        if stream_uses_per_mint_revision(cmd.stream) {
            let mint = demand_mint_for_command(&cmd.payload)
                .expect("wallet/tracker command requires demand mint");
            let map = match cmd.stream {
                TrackCommandStream::Wallet => &mut self.last_applied_wallet_mint,
                TrackCommandStream::Tracker => &mut self.last_applied_tracker_mint,
                _ => unreachable!(),
            };
            let entry = map.entry(mint).or_insert(0);
            if cmd.revision > *entry {
                *entry = cmd.revision;
            }
            return;
        }
        let idx = cmd.stream.index();
        if cmd.revision > self.last_applied[idx] {
            self.last_applied[idx] = cmd.revision;
        }
    }

    /// Backward-compatible helper for global-stream tests.
    pub fn is_applicable_stream(&self, stream: TrackCommandStream, revision: u64) -> bool {
        revision > self.last_applied[stream.index()]
    }

    pub fn mark_applied_stream(&mut self, stream: TrackCommandStream, revision: u64) {
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

    /// Stage command for later replay (queue full or transient wallet/tracker admission reject).
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
                if self.is_applicable(&cmd) {
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
            store.mark_applied(&cmd);
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
        store.mark_applied_stream(TrackCommandStream::Momentum, 2);
        store.stage_on_queue_full(c1);
        store.stage_on_queue_full(c2);
        let mut applied = Vec::new();
        for cmd in store.take_applicable_pending_sorted() {
            if store.is_applicable(&cmd) {
                store.mark_applied(&cmd);
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
            store.mark_applied(&cmd);
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
        store.mark_applied(&withdraw);
        assert!(!store.is_applicable(&pin_old));
        let applicable = store.take_applicable_pending_sorted();
        assert!(
            applicable.is_empty(),
            "stale wallet pin must not replay after newer withdraw advances watermark"
        );
    }

    #[test]
    fn wallet_later_mint_does_not_supersede_pending_pin_for_other_mint() {
        let mut store = BoundedProtocolStore::new(8, 64);
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        let pin_a = store.wrap_command(TrackWorkerCommand::ApplyWalletPin { mint: mint_a });
        store.stage_on_queue_full(pin_a.clone());
        let pin_b = store.wrap_command(TrackWorkerCommand::ApplyWalletPin { mint: mint_b });
        store.mark_applied(&pin_b);
        assert!(
            store.is_applicable(&pin_a),
            "pending wallet pin for mint A must remain applicable after mint B advances"
        );
        let applicable = store.take_applicable_pending_sorted();
        assert_eq!(applicable.len(), 1);
        assert_eq!(
            demand_mint_for_command(&applicable[0].payload),
            Some(mint_a)
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
