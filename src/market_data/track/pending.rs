//! Bounded pending / inflight / revision store for track-worker queue-full replay.

use super::worker_commands::{
    demand_mint_for_command, is_continue_evict, merge_sync_intent_payloads, stream_for_command,
    stream_uses_per_mint_revision, sync_intent_strength, track_command_kind, ImmutableTrackCommand,
    RevisionAssigner, TrackCommandStream, TrackWorkerCommand,
};
use crate::metrics::{
    inc_market_data_track_protocol_pending_coalesced_total,
    inc_market_data_track_protocol_pending_evicted_total,
    inc_market_data_track_protocol_replay_triggers_total,
    inc_market_data_track_protocol_stage_by_kind,
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
    /// In-flight (queued, not yet dequeued) sync intents for enqueue dedupe.
    inflight_sync_intent: u32,
    /// In-flight continue-evict commands for enqueue dedupe.
    inflight_continue_evict: u32,
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
            inflight_sync_intent: 0,
            inflight_continue_evict: 0,
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

    /// Wallet pin/withdraw supersedes unpinned tracker demand for the same mint (cross-stream).
    pub fn supersede_tracker_demand_for_mint(&mut self, mint: Pubkey) {
        let mut watermark = *self.last_applied_tracker_mint.get(&mint).unwrap_or(&0);
        watermark = watermark.max(
            self.revision
                .last_issued_revision_for_mint(TrackCommandStream::Tracker, mint),
        );
        for cmd in &self.pending {
            if cmd.stream == TrackCommandStream::Tracker
                && demand_mint_for_command(&cmd.payload) == Some(mint)
            {
                watermark = watermark.max(cmd.revision);
            }
        }
        if watermark > 0 {
            let entry = self.last_applied_tracker_mint.entry(mint).or_insert(0);
            if watermark > *entry {
                *entry = watermark;
            }
        }
    }

    /// Mark wallet demand applied and supersede stale tracker protocol demand for the mint.
    pub fn mark_applied_wallet_demand(&mut self, cmd: &ImmutableTrackCommand) {
        debug_assert_eq!(cmd.stream, TrackCommandStream::Wallet);
        self.mark_applied(cmd);
        if let Some(mint) = demand_mint_for_command(&cmd.payload) {
            self.supersede_tracker_demand_for_mint(mint);
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

    #[inline]
    fn has_pending_sync_intent(&self) -> bool {
        self.pending
            .iter()
            .any(|c| sync_intent_strength(&c.payload).is_some())
    }

    #[inline]
    fn has_pending_continue_evict(&self) -> bool {
        self.pending.iter().any(|c| is_continue_evict(&c.payload))
    }

    /// Record inflight dedupe flags when a command is enqueued to the worker queue.
    pub fn note_inflight_enqueued(&mut self, cmd: &ImmutableTrackCommand) {
        if sync_intent_strength(&cmd.payload).is_some() {
            self.inflight_sync_intent = self.inflight_sync_intent.saturating_add(1);
        }
        if is_continue_evict(&cmd.payload) {
            self.inflight_continue_evict = self.inflight_continue_evict.saturating_add(1);
        }
    }

    /// Clear inflight dedupe flags when a command is dequeued by the worker.
    pub fn note_inflight_dequeued(&mut self, cmd: &ImmutableTrackCommand) {
        if sync_intent_strength(&cmd.payload).is_some() {
            self.inflight_sync_intent = self.inflight_sync_intent.saturating_sub(1);
        }
        if is_continue_evict(&cmd.payload) {
            self.inflight_continue_evict = self.inflight_continue_evict.saturating_sub(1);
        }
    }

    /// Enqueue-side dedupe: merge into pending or skip when equivalent intent already queued.
    /// Returns `true` when demand is preserved without a new queue slot.
    pub fn try_dedupe_enqueue(&mut self, job: &TrackWorkerCommand) -> bool {
        if sync_intent_strength(job).is_some() {
            if self.inflight_sync_intent > 0 {
                return true;
            }
            if self.has_pending_sync_intent() {
                let payloads: Vec<TrackWorkerCommand> = self
                    .pending
                    .iter()
                    .filter(|c| sync_intent_strength(&c.payload).is_some())
                    .map(|c| c.payload.clone())
                    .chain(std::iter::once(job.clone()))
                    .collect();
                let max_rev = self
                    .pending
                    .iter()
                    .filter(|c| sync_intent_strength(&c.payload).is_some())
                    .map(|c| c.revision)
                    .max()
                    .unwrap_or(0)
                    .max(
                        self.revision
                            .next_revision(TrackCommandStream::Control, None),
                    );
                let merged = merge_sync_intent_payloads(&payloads);
                self.pending
                    .retain(|c| sync_intent_strength(&c.payload).is_none());
                self.pending.push(ImmutableTrackCommand::new(
                    TrackCommandStream::Control,
                    max_rev,
                    merged,
                ));
                inc_market_data_track_protocol_pending_coalesced_total();
                self.refresh_pending_depth_metric();
                return true;
            }
            return false;
        }
        if is_continue_evict(job) {
            if self.inflight_continue_evict > 0 {
                return true;
            }
            if let Some(idx) = self
                .pending
                .iter()
                .position(|c| is_continue_evict(&c.payload))
            {
                let new_rev = self
                    .revision
                    .next_revision(TrackCommandStream::Control, None)
                    .max(self.pending[idx].revision);
                self.pending[idx] = ImmutableTrackCommand::new(
                    TrackCommandStream::Control,
                    new_rev,
                    TrackWorkerCommand::ContinueGeyserEvict,
                );
                inc_market_data_track_protocol_pending_coalesced_total();
                return true;
            }
            return false;
        }
        false
    }

    /// Coalesce `cmd` with existing pending entries before push (latest-wins per demand key).
    fn coalesce_with_pending(&mut self, cmd: ImmutableTrackCommand) -> ImmutableTrackCommand {
        if sync_intent_strength(&cmd.payload).is_some() {
            let existing_sync: Vec<ImmutableTrackCommand> = self
                .pending
                .iter()
                .filter(|c| sync_intent_strength(&c.payload).is_some())
                .cloned()
                .collect();
            if !existing_sync.is_empty() {
                let payloads: Vec<TrackWorkerCommand> = existing_sync
                    .iter()
                    .map(|c| c.payload.clone())
                    .chain(std::iter::once(cmd.payload.clone()))
                    .collect();
                let max_rev = existing_sync
                    .iter()
                    .map(|c| c.revision)
                    .chain(std::iter::once(cmd.revision))
                    .max()
                    .unwrap_or(cmd.revision);
                for _ in &existing_sync {
                    inc_market_data_track_protocol_superseded_revisions_total();
                }
                self.pending
                    .retain(|c| sync_intent_strength(&c.payload).is_none());
                inc_market_data_track_protocol_pending_coalesced_total();
                return ImmutableTrackCommand::new(
                    TrackCommandStream::Control,
                    max_rev,
                    merge_sync_intent_payloads(&payloads),
                );
            }
            return cmd;
        }
        if is_continue_evict(&cmd.payload) {
            if let Some(idx) = self
                .pending
                .iter()
                .position(|c| is_continue_evict(&c.payload))
            {
                let old_rev = self.pending[idx].revision;
                if cmd.revision > old_rev {
                    inc_market_data_track_protocol_superseded_revisions_total();
                    self.pending.remove(idx);
                    inc_market_data_track_protocol_pending_coalesced_total();
                } else {
                    inc_market_data_track_protocol_superseded_revisions_total();
                    inc_market_data_track_protocol_pending_coalesced_total();
                    return cmd;
                }
            }
            return cmd;
        }
        if stream_uses_per_mint_revision(cmd.stream) {
            if let Some(mint) = demand_mint_for_command(&cmd.payload) {
                let superseded: Vec<u64> = self
                    .pending
                    .iter()
                    .filter(|c| {
                        c.stream == cmd.stream
                            && demand_mint_for_command(&c.payload) == Some(mint)
                            && c.revision < cmd.revision
                    })
                    .map(|c| c.revision)
                    .collect();
                if !superseded.is_empty() {
                    for _ in &superseded {
                        inc_market_data_track_protocol_superseded_revisions_total();
                    }
                    self.pending.retain(|c| {
                        !(c.stream == cmd.stream
                            && demand_mint_for_command(&c.payload) == Some(mint)
                            && c.revision < cmd.revision)
                    });
                    inc_market_data_track_protocol_pending_coalesced_total();
                }
            }
            return cmd;
        }
        if matches!(
            cmd.stream,
            TrackCommandStream::Momentum | TrackCommandStream::Arb
        ) {
            let superseded: Vec<u64> = self
                .pending
                .iter()
                .filter(|c| c.stream == cmd.stream && c.revision < cmd.revision)
                .map(|c| c.revision)
                .collect();
            if !superseded.is_empty() {
                for _ in &superseded {
                    inc_market_data_track_protocol_superseded_revisions_total();
                }
                self.pending
                    .retain(|c| !(c.stream == cmd.stream && c.revision < cmd.revision));
                inc_market_data_track_protocol_pending_coalesced_total();
            }
        }
        cmd
    }

    /// Stage command for later replay (queue full or transient wallet/tracker admission reject).
    pub fn stage_on_queue_full(&mut self, cmd: ImmutableTrackCommand) -> StageResult {
        inc_market_data_track_protocol_replay_triggers_total();
        inc_market_data_track_protocol_stage_by_kind(track_command_kind(&cmd.payload).index());
        let cmd = self.coalesce_with_pending(cmd);
        if is_continue_evict(&cmd.payload) && self.has_pending_continue_evict() {
            return StageResult::Staged;
        }
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
    use super::super::worker_commands::TrackPinReason;
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
        store.stage_on_queue_full(c1);
        store.stage_on_queue_full(c2.clone());
        let mut applied = Vec::new();
        for cmd in store.take_applicable_pending_sorted() {
            store.mark_applied(&cmd);
            applied.push(cmd.revision);
        }
        assert_eq!(
            applied,
            vec![2],
            "newer momentum revision supersedes older pending"
        );
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
        assert_eq!(
            applied,
            vec![2, 3],
            "superseded rev 1 dropped; rev 2 and 3 remain applicable"
        );
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
    fn wallet_withdraw_supersedes_pending_tracker_demand_for_same_mint() {
        let mut store = BoundedProtocolStore::new(8, 64);
        let mint = Pubkey::new_unique();
        let tracker = store.wrap_command(TrackWorkerCommand::TrackMint { mint, pin: None });
        store.stage_on_queue_full(tracker.clone());
        let withdraw = store.wrap_command(TrackWorkerCommand::WithdrawWalletPin { mint });
        store.mark_applied_wallet_demand(&withdraw);
        assert!(
            !store.is_applicable(&tracker),
            "wallet withdraw must supersede pending tracker demand for same mint"
        );
        let applicable = store.take_applicable_pending_sorted();
        assert!(
            applicable.is_empty(),
            "superseded tracker demand must not replay after wallet withdraw"
        );
    }

    #[test]
    fn wallet_pin_supersedes_pending_tracker_demand_for_same_mint() {
        let mut store = BoundedProtocolStore::new(8, 64);
        let mint = Pubkey::new_unique();
        let tracker = store.wrap_command(TrackWorkerCommand::TrackMint { mint, pin: None });
        store.stage_on_queue_full(tracker.clone());
        let pin = store.wrap_command(TrackWorkerCommand::ApplyWalletPin { mint });
        store.mark_applied_wallet_demand(&pin);
        assert!(!store.is_applicable(&tracker));
        assert!(store.take_applicable_pending_sorted().is_empty());
    }

    #[test]
    fn wallet_track_mint_uses_wallet_stream_per_mint_revision() {
        let mut store = BoundedProtocolStore::new(8, 64);
        let mint = Pubkey::new_unique();
        let cmd = store.wrap_command(TrackWorkerCommand::TrackMint {
            mint,
            pin: Some(TrackPinReason::Wallet),
        });
        assert_eq!(cmd.stream, TrackCommandStream::Wallet);
        assert_eq!(cmd.revision, 1);
        let cmd2 = store.wrap_command(TrackWorkerCommand::TrackMint {
            mint,
            pin: Some(TrackPinReason::Wallet),
        });
        assert_eq!(cmd2.revision, 2);
    }

    #[test]
    fn wallet_track_mint_reject_re_stages_pending() {
        let mut store = BoundedProtocolStore::new(8, 64);
        let mint = Pubkey::new_unique();
        let cmd = store.wrap_command(TrackWorkerCommand::TrackMint {
            mint,
            pin: Some(TrackPinReason::Wallet),
        });
        assert_eq!(store.stage_on_queue_full(cmd.clone()), StageResult::Staged);
        assert_eq!(store.pending_len(), 1);
        assert!(store.is_applicable(&cmd));
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

    #[test]
    fn sync_duplicates_under_queue_full_coalesce_to_single_pending_intent() {
        let mut store = BoundedProtocolStore::new(8, 64);
        for _ in 0..32 {
            let cmd = store.wrap_command(TrackWorkerCommand::ScheduleGeyserPush);
            assert_eq!(store.stage_on_queue_full(cmd), StageResult::Staged);
        }
        let sync_count = store
            .pending
            .iter()
            .filter(|c| {
                matches!(
                    c.payload,
                    TrackWorkerCommand::ScheduleGeyserPush
                        | TrackWorkerCommand::ScheduleGeyserPushDebounced
                        | TrackWorkerCommand::ScheduleGeyserSyncAfterConfigChange
                )
            })
            .count();
        assert_eq!(
            sync_count, 1,
            "many sync pushes must coalesce to one pending slot"
        );
    }

    #[test]
    fn sync_flood_at_pending_cap_does_not_evict() {
        let cap = 4;
        let mut store = BoundedProtocolStore::new(cap, 64);
        let mints: Vec<Pubkey> = (0..cap).map(|_| Pubkey::new_unique()).collect();
        for mint in &mints {
            let cmd = store.wrap_command(TrackWorkerCommand::TrackMint {
                mint: *mint,
                pin: None,
            });
            store.stage_on_queue_full(cmd);
        }
        assert_eq!(store.pending_len(), cap);
        for _ in 0..64 {
            let cmd = store.wrap_command(TrackWorkerCommand::ScheduleGeyserPush);
            store.stage_on_queue_full(cmd);
        }
        assert_eq!(
            store.pending_len(),
            cap,
            "sync duplicate flood must not grow pending or evict unique mint demand"
        );
        let sync_count = store
            .pending
            .iter()
            .filter(|c| matches!(c.payload, TrackWorkerCommand::ScheduleGeyserPush))
            .count();
        assert_eq!(sync_count, 1);
    }

    #[test]
    fn same_mint_trackmint_latest_wins_in_pending() {
        let mut store = BoundedProtocolStore::new(8, 64);
        let mint = Pubkey::new_unique();
        let old = store.wrap_command(TrackWorkerCommand::TrackMint { mint, pin: None });
        store.stage_on_queue_full(old.clone());
        let new = store.wrap_command(TrackWorkerCommand::TrackMint { mint, pin: None });
        store.stage_on_queue_full(new.clone());
        let tracker_pending: Vec<_> = store
            .pending
            .iter()
            .filter(|c| c.stream == TrackCommandStream::Tracker)
            .collect();
        assert_eq!(tracker_pending.len(), 1);
        assert_eq!(tracker_pending[0].revision, new.revision);
        assert!(
            !store.pending.iter().any(|c| c.revision == old.revision),
            "older same-mint tracker demand must be removed from pending"
        );
        assert!(store.is_applicable(&new));
    }

    #[test]
    fn momentum_newer_revision_supersedes_older_pending() {
        let mut store = BoundedProtocolStore::new(8, 64);
        let old = store.wrap_command(momentum_cmd(1));
        store.stage_on_queue_full(old.clone());
        let new = store.wrap_command(momentum_cmd(2));
        store.stage_on_queue_full(new.clone());
        let momentum_pending: Vec<_> = store
            .pending
            .iter()
            .filter(|c| c.stream == TrackCommandStream::Momentum)
            .collect();
        assert_eq!(momentum_pending.len(), 1);
        assert_eq!(momentum_pending[0].revision, new.revision);
        let applicable = store.take_applicable_pending_sorted();
        assert_eq!(applicable.len(), 1);
        assert_eq!(applicable[0].revision, new.revision);
    }

    #[test]
    fn continue_evict_dedupes_pending_to_single_slot() {
        let mut store = BoundedProtocolStore::new(8, 64);
        for _ in 0..16 {
            let cmd = store.wrap_command(TrackWorkerCommand::ContinueGeyserEvict);
            store.stage_on_queue_full(cmd);
        }
        let continue_count = store
            .pending
            .iter()
            .filter(|c| matches!(c.payload, TrackWorkerCommand::ContinueGeyserEvict))
            .count();
        assert_eq!(continue_count, 1);
    }

    #[test]
    fn enqueue_dedupe_skips_when_sync_already_inflight() {
        let mut store = BoundedProtocolStore::new(8, 64);
        let cmd = store.wrap_command(TrackWorkerCommand::ScheduleGeyserPush);
        store.note_inflight_enqueued(&cmd);
        assert!(store.try_dedupe_enqueue(&TrackWorkerCommand::ScheduleGeyserPush));
        assert_eq!(store.pending_len(), 0);
    }
}
