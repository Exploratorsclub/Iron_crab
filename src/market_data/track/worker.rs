//! Phase 5a: `md-track-worker` OS thread — bounded enqueue, command processing, coalesced Geyser push.

use super::admission_wiring::{AdmissionConvergeResult, AdmissionRestoreResult};
use super::explicit_admission::{CapShrinkResult, FixedCapAdmission};
use super::geyser_sync::track_worker_execute_coalesced_push;
use super::pending::{BoundedProtocolStore, StageResult};
use super::snapshot::{
    explicit_set_snapshot_path, write_explicit_set_snapshot, ExplicitSetSnapshot,
    MARKET_DATA_EXPLICIT_SET_SNAPSHOT_INTERVAL_SECS,
};
use super::worker_commands::track_command_kind;
use super::worker_commands::ImmutableTrackCommand;
pub use super::worker_commands::{TrackCommandStream, TrackPinReason, TrackWorkerCommand};
use crate::metrics::{
    add_market_data_arb_shed_skipped_must_hot_total,
    add_market_data_exec_hot_hard_shed_groups_for_tier,
    inc_market_data_explicit_set_snapshot_write_errors_total,
    inc_market_data_explicit_set_snapshot_write_total,
    inc_market_data_geyser_tracking_enqueue_dropped_total,
    inc_market_data_track_protocol_superseded_revisions_total,
    inc_market_data_track_request_coalesce_batches_total,
    inc_market_data_track_worker_enqueue_by_kind,
    inc_market_data_track_worker_enqueue_deduped_total,
    record_market_data_arb_track_requests_messages_total,
    record_market_data_momentum_active_pool_messages_total, set_market_data_arb_pinned_pools_gauge,
    set_market_data_exec_hot_last_shed_groups, set_market_data_momentum_active_pool_pins_gauge,
    set_market_data_track_worker_queue_depth, ExecHotShedTier,
};
use crate::nats::{
    ArbTrackActiveEntry, ArbTrackRemovedEntry, ArbTrackRequestsUpdate, MomentumActivePoolEntry,
    MomentumActivePoolsUpdate, MomentumRemovedPoolEntry,
};
use solana_sdk::pubkey::Pubkey;
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc as std_mpsc;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// Phase 2a: track-worker Geyser push coalesce window (I-4d prep).
pub const MARKET_DATA_TRACK_WORKER_COALESCE_MS: u64 = 500;

/// Momentum-hot JetStream balance refresh heartbeat interval (must stay below 4s entry-age gate).
pub const MARKET_DATA_MOMENTUM_HOT_BALANCE_REFRESH_HEARTBEAT_MS: u64 = 2000;
/// Phase 2a: bounded queue for md-track-worker commands.
pub const MARKET_DATA_TRACK_WORKER_QUEUE_CAP: usize = 8192;
/// PR169c: chunk large momentum applies so the tracking actor yields to ingest tasks.
pub const MARKET_DATA_MOMENTUM_APPLY_CHUNK_THRESHOLD: usize = 32;
pub const MARKET_DATA_MOMENTUM_APPLY_CHUNK_SIZE: usize = 16;
/// Phase 3: chunk large arb track applies on md-track-worker.
pub const MARKET_DATA_ARB_APPLY_CHUNK_THRESHOLD: usize = 32;
pub const MARKET_DATA_ARB_APPLY_CHUNK_SIZE: usize = 16;

/// Context surface required by the track-worker loop and apply helpers.
pub trait TrackWorkerContext: Send + Sync {
    fn geyser_sync_batch_debounce_ms(&self) -> u64;
    fn max_tracked_accounts(&self) -> usize;
    fn apply_momentum_active_pools_update(
        &self,
        admission: &mut FixedCapAdmission,
        update: &MomentumActivePoolsUpdate,
    ) -> bool;
    fn apply_momentum_snapshot_reconcile(
        &self,
        admission: &mut FixedCapAdmission,
        active: &[MomentumActivePoolEntry],
    ) -> bool;
    fn apply_momentum_removed_entries(
        &self,
        admission: &mut FixedCapAdmission,
        chunk: &[MomentumRemovedPoolEntry],
    ) -> bool;
    fn apply_momentum_active_entries(
        &self,
        admission: &mut FixedCapAdmission,
        chunk: &[MomentumActivePoolEntry],
    ) -> bool;
    fn apply_arb_track_requests_update(
        &self,
        admission: &mut FixedCapAdmission,
        update: &ArbTrackRequestsUpdate,
    ) -> bool;
    fn apply_arb_snapshot_reconcile(
        &self,
        admission: &mut FixedCapAdmission,
        active: &[ArbTrackActiveEntry],
    ) -> bool;
    fn apply_arb_removed_entries(
        &self,
        admission: &mut FixedCapAdmission,
        chunk: &[ArbTrackRemovedEntry],
    ) -> bool;
    fn apply_arb_active_entries(
        &self,
        admission: &mut FixedCapAdmission,
        chunk: &[ArbTrackActiveEntry],
    ) -> bool;
    fn apply_wallet_pin(&self, admission: &mut FixedCapAdmission, mint: Pubkey) -> bool;
    fn withdraw_wallet_pin(&self, admission: &mut FixedCapAdmission, mint: Pubkey) -> bool;
    fn apply_track_mint(
        &self,
        admission: &mut FixedCapAdmission,
        mint: Pubkey,
        pin: Option<TrackPinReason>,
    ) -> bool;
    fn refresh_geyser_pins_gauge(&self);
    fn hot_pool_registry_pair_count(&self) -> usize;
    fn hot_pool_registry_arb_pool_count(&self) -> usize;
    fn arb_pool_is_must_hot(&self, pool: Pubkey) -> bool;
    fn refresh_hot_pool_registry_gauges(&self);
    /// Periodic JetStream refresh for momentum-hot pins (WaitHotSet sustained freshness).
    fn tick_momentum_hot_balance_refresh_heartbeat(&self);
    fn snapshot_explicit_subscription_pubkeys(&self) -> HashSet<Pubkey>;
    fn sync_geyser_tracked_accounts_batched_flush_with_deadline(
        &self,
        deadline: Instant,
        admission: &FixedCapAdmission,
    ) -> bool;
    fn continue_geyser_evict_with_deadline(
        &self,
        deadline: Instant,
        admission: &FixedCapAdmission,
    ) -> bool;
    fn release_geyser_sync_flush_slot(&self);
    fn refresh_tracked_membership_snapshot(&self);
    fn explicit_pubkey_rows_for_desired_set(
        &self,
    ) -> Vec<(Pubkey, super::ConsumerId, Option<Pubkey>)>;
    fn build_explicit_set_snapshot(&self, admission: &FixedCapAdmission) -> ExplicitSetSnapshot;
    fn apply_explicit_set_snapshot(
        &self,
        admission: &mut FixedCapAdmission,
        snapshot: &ExplicitSetSnapshot,
    ) -> AdmissionRestoreResult;
    fn on_admission_converge_result(
        &self,
        admission: &FixedCapAdmission,
        result: AdmissionConvergeResult,
    );
    fn prune_tracked_maps_to_admitted(&self, admission: &FixedCapAdmission);
    fn publish_admitted_explicit_physical(&self, admission: &FixedCapAdmission);
    /// True when `tracked_*` maps contain pubkeys not present in admitted set.
    fn tracked_maps_need_prune(&self, admission: &FixedCapAdmission) -> bool;
    /// Publish physical explicit set even when admission matches `last_synced` (restore barrier).
    fn publish_admitted_explicit_physical_force(&self, admission: &FixedCapAdmission);
    fn last_synced_explicit_pubkeys_write(
        &self,
    ) -> parking_lot::RwLockWriteGuard<'_, HashSet<Pubkey>>;
    fn geyser_explicit_readiness_ok(&self) -> bool;
    fn geyser_connect_barrier_pending(&self) -> bool;
    fn signal_restore_barrier(&self, ok: bool);
    fn apply_explicit_cap_shrink(
        &self,
        admission: &mut FixedCapAdmission,
        new_cap: usize,
    ) -> CapShrinkResult;
    /// Scope C: retry vault/bin registration for hot pools deferred on LivePoolCache miss.
    fn retry_deferred_hot_pool_reserve_registrations(
        &self,
        admission: &mut FixedCapAdmission,
    ) -> bool;
}

#[derive(Clone)]
pub struct TrackWorkerSender {
    tx: std_mpsc::SyncSender<ImmutableTrackCommand>,
    queue_depth: Arc<AtomicUsize>,
    queue_capacity: usize,
    protocol: Arc<Mutex<BoundedProtocolStore>>,
}

fn track_worker_stage_on_queue_full(
    protocol: &Arc<Mutex<BoundedProtocolStore>>,
    cmd: ImmutableTrackCommand,
) -> bool {
    let mut store = protocol.lock().expect("track protocol store lock");
    match store.stage_on_queue_full(cmd) {
        StageResult::Staged => true,
        StageResult::Lost => {
            inc_market_data_geyser_tracking_enqueue_dropped_total();
            false
        }
    }
}

pub fn track_worker_try_enqueue(sender: &TrackWorkerSender, job: TrackWorkerCommand) -> bool {
    let kind_idx = track_command_kind(&job).index();
    inc_market_data_track_worker_enqueue_by_kind(kind_idx);
    let immutable = {
        let mut store = sender.protocol.lock().expect("track protocol store lock");
        if store.try_dedupe_enqueue(&job) {
            inc_market_data_track_worker_enqueue_deduped_total();
            return true;
        }
        store.wrap_command(job)
    };
    if sender.queue_depth.load(Ordering::Relaxed) >= sender.queue_capacity {
        return track_worker_stage_on_queue_full(&sender.protocol, immutable);
    }
    if sender.tx.try_send(immutable.clone()).is_ok() {
        let depth = sender.queue_depth.fetch_add(1, Ordering::Relaxed) + 1;
        set_market_data_track_worker_queue_depth(depth);
        let mut store = sender.protocol.lock().expect("track protocol store lock");
        store.note_inflight_enqueued(&immutable);
        store.begin_inflight();
        true
    } else {
        track_worker_stage_on_queue_full(&sender.protocol, immutable)
    }
}

fn track_worker_on_dequeue(
    queue_depth: &AtomicUsize,
    protocol: &Arc<Mutex<BoundedProtocolStore>>,
    cmd: &ImmutableTrackCommand,
) {
    {
        let mut store = protocol.lock().expect("track protocol store lock");
        store.note_inflight_dequeued(cmd);
    }
    track_worker_dec_queue_depth(queue_depth, protocol);
}

fn track_worker_dec_queue_depth(
    queue_depth: &AtomicUsize,
    protocol: &Arc<Mutex<BoundedProtocolStore>>,
) {
    let new_depth = queue_depth
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
            cur.checked_sub(1)
        })
        .unwrap_or(0);
    set_market_data_track_worker_queue_depth(new_depth);
    let mut store = protocol.lock().expect("track protocol store lock");
    store.end_inflight();
}

/// Phase-2b: sync momentum apply on `md-track-worker` thread (no Tokio yield).
pub fn apply_momentum_active_pools_on_track_worker<C: TrackWorkerContext>(
    ctx: &Arc<C>,
    admission: &mut FixedCapAdmission,
    update: MomentumActivePoolsUpdate,
) -> bool {
    let item_count = update.active.len() + update.removed.len();
    if item_count <= MARKET_DATA_MOMENTUM_APPLY_CHUNK_THRESHOLD {
        return ctx.apply_momentum_active_pools_update(admission, &update);
    }
    record_market_data_momentum_active_pool_messages_total();
    let mut batch_dirty = false;
    if update.full_active_snapshot {
        batch_dirty |= ctx.apply_momentum_snapshot_reconcile(admission, &update.active);
        std::thread::yield_now();
    }
    for chunk in update.removed.chunks(MARKET_DATA_MOMENTUM_APPLY_CHUNK_SIZE) {
        batch_dirty |= ctx.apply_momentum_removed_entries(admission, chunk);
        std::thread::yield_now();
    }
    for chunk in update.active.chunks(MARKET_DATA_MOMENTUM_APPLY_CHUNK_SIZE) {
        batch_dirty |= ctx.apply_momentum_active_entries(admission, chunk);
        std::thread::yield_now();
    }
    batch_dirty |= ctx.retry_deferred_hot_pool_reserve_registrations(admission);
    if !batch_dirty {
        ctx.refresh_geyser_pins_gauge();
    }
    set_market_data_momentum_active_pool_pins_gauge(ctx.hot_pool_registry_pair_count());
    batch_dirty
}

/// Phase 3: sync arb track apply on `md-track-worker` thread (no Tokio yield).
pub fn apply_arb_track_requests_on_track_worker<C: TrackWorkerContext>(
    ctx: &Arc<C>,
    admission: &mut FixedCapAdmission,
    update: ArbTrackRequestsUpdate,
) -> bool {
    let item_count = update.active.len() + update.removed.len();
    if item_count <= MARKET_DATA_ARB_APPLY_CHUNK_THRESHOLD {
        return ctx.apply_arb_track_requests_update(admission, &update);
    }
    record_market_data_arb_track_requests_messages_total();
    let mut batch_dirty = false;
    if update.reconcile {
        batch_dirty |= ctx.apply_arb_snapshot_reconcile(admission, &update.active);
        std::thread::yield_now();
    }
    for chunk in update.removed.chunks(MARKET_DATA_ARB_APPLY_CHUNK_SIZE) {
        batch_dirty |= ctx.apply_arb_removed_entries(admission, chunk);
        std::thread::yield_now();
    }
    for chunk in update.active.chunks(MARKET_DATA_ARB_APPLY_CHUNK_SIZE) {
        batch_dirty |= ctx.apply_arb_active_entries(admission, chunk);
        std::thread::yield_now();
    }
    if !batch_dirty {
        ctx.refresh_geyser_pins_gauge();
    }
    set_market_data_arb_pinned_pools_gauge(ctx.hot_pool_registry_arb_pool_count());
    batch_dirty
}

fn apply_exec_hot_pressure_shed<C: TrackWorkerContext>(
    ctx: &Arc<C>,
    admission: &mut FixedCapAdmission,
    tier: ExecHotShedTier,
    max_groups: usize,
) -> bool {
    let result = match tier {
        ExecHotShedTier::Tracker => admission.shed_tracker_owner_groups(max_groups),
        ExecHotShedTier::Momentum => admission.shed_momentum_owner_groups(max_groups),
        ExecHotShedTier::Arb => {
            admission.shed_arb_owner_groups(max_groups, |pool| ctx.arb_pool_is_must_hot(pool))
        }
        ExecHotShedTier::None => {
            return false;
        }
    };
    if result.groups_skipped_must_hot > 0 {
        add_market_data_arb_shed_skipped_must_hot_total(result.groups_skipped_must_hot as u64);
    }
    set_market_data_exec_hot_last_shed_groups(tier, result.groups_evicted as u64);
    if result.groups_evicted > 0 {
        add_market_data_exec_hot_hard_shed_groups_for_tier(tier, result.groups_evicted as u64);
        ctx.prune_tracked_maps_to_admitted(admission);
        ctx.refresh_tracked_membership_snapshot();
    }
    result.groups_evicted > 0
}

pub fn track_worker_process_command<C: TrackWorkerContext>(
    ctx: &Arc<C>,
    admission: &mut FixedCapAdmission,
    restore_barrier_pending: &mut bool,
    job: TrackWorkerCommand,
) -> bool {
    match job {
        TrackWorkerCommand::ApplyMomentumActivePools(update) => {
            apply_momentum_active_pools_on_track_worker(ctx, admission, update)
        }
        TrackWorkerCommand::ApplyArbTrackRequests(update) => {
            apply_arb_track_requests_on_track_worker(ctx, admission, update)
        }
        TrackWorkerCommand::ApplyWalletPin { mint } => ctx.apply_wallet_pin(admission, mint),
        TrackWorkerCommand::WithdrawWalletPin { mint } => ctx.withdraw_wallet_pin(admission, mint),
        TrackWorkerCommand::TrackMint { mint, pin } => ctx.apply_track_mint(admission, mint, pin),
        TrackWorkerCommand::TrackMints { entries } => {
            let mut all_ok = true;
            for (mint, pin) in entries {
                all_ok &= ctx.apply_track_mint(admission, mint, pin);
            }
            all_ok
        }
        TrackWorkerCommand::ScheduleGeyserSyncAfterConfigChange => {
            let new_cap = ctx.max_tracked_accounts();
            let _ = ctx.apply_explicit_cap_shrink(admission, new_cap);
            true
        }
        TrackWorkerCommand::RestoreExplicitSnapshot(snapshot) => {
            let restore = ctx.apply_explicit_set_snapshot(admission, &snapshot);
            *restore_barrier_pending = true;
            matches!(restore, AdmissionRestoreResult::Restored)
        }
        TrackWorkerCommand::ScheduleGeyserPush
        | TrackWorkerCommand::ScheduleGeyserPushDebounced => {
            if ctx.geyser_connect_barrier_pending() {
                *restore_barrier_pending = true;
            }
            false
        }
        TrackWorkerCommand::ContinueGeyserEvict => {
            if ctx.geyser_connect_barrier_pending() {
                *restore_barrier_pending = true;
            }
            false
        }
        TrackWorkerCommand::ShedTrackerUnderExecHotPressure { max_groups } => {
            apply_exec_hot_pressure_shed(ctx, admission, ExecHotShedTier::Tracker, max_groups)
        }
        TrackWorkerCommand::ShedMomentumUnderExecHotPressure { max_groups } => {
            apply_exec_hot_pressure_shed(ctx, admission, ExecHotShedTier::Momentum, max_groups)
        }
        TrackWorkerCommand::ShedArbUnderExecHotPressure { max_groups } => {
            apply_exec_hot_pressure_shed(ctx, admission, ExecHotShedTier::Arb, max_groups)
        }
        TrackWorkerCommand::FlushExplicitSetSnapshot { done } => {
            try_write_explicit_set_snapshot_from_ctx(ctx.as_ref(), admission);
            let _ = done.send(());
            false
        }
        TrackWorkerCommand::RetryDeferredHotPoolReserves => {
            ctx.retry_deferred_hot_pool_reserve_registrations(admission)
        }
    }
}

/// Wallet/Tracker demand commands only advance the monotone revision when the handler
/// succeeds so transient admission rejects remain replayable (I-MD-5).
fn track_protocol_should_advance_revision(
    stream: TrackCommandStream,
    handler_succeeded: bool,
) -> bool {
    match stream {
        TrackCommandStream::Wallet | TrackCommandStream::Tracker => handler_succeeded,
        TrackCommandStream::Momentum | TrackCommandStream::Arb | TrackCommandStream::Control => {
            true
        }
    }
}

/// Coalesced TrackMints batches use Control revision but must replay like tracker demand.
#[inline]
fn track_mints_batch_requires_replay_on_failure(payload: &TrackWorkerCommand) -> bool {
    matches!(payload, TrackWorkerCommand::TrackMints { .. })
}

fn track_protocol_stage_for_replay(
    protocol: &Arc<Mutex<BoundedProtocolStore>>,
    cmd: ImmutableTrackCommand,
) {
    let mut store = protocol.lock().expect("track protocol store lock");
    let _ = store.stage_on_queue_full(cmd);
}

fn track_worker_apply_protocol_command<C: TrackWorkerContext>(
    ctx: &Arc<C>,
    protocol: &Arc<Mutex<BoundedProtocolStore>>,
    admission: &mut FixedCapAdmission,
    restore_barrier_pending: &mut bool,
    cmd: ImmutableTrackCommand,
) {
    let applicable = {
        let store = protocol.lock().expect("track protocol store lock");
        store.is_applicable(&cmd)
    };
    if !applicable {
        inc_market_data_track_protocol_superseded_revisions_total();
        return;
    }
    let stream = cmd.stream;
    let revision = cmd.revision;
    let payload_backup = cmd.payload.clone();
    let restage_on_failure = matches!(
        stream,
        TrackCommandStream::Wallet | TrackCommandStream::Tracker
    ) || track_mints_batch_requires_replay_on_failure(&payload_backup);
    let handler_succeeded =
        track_worker_process_command(ctx, admission, restore_barrier_pending, cmd.payload);
    let protocol_cmd = ImmutableTrackCommand::new(stream, revision, payload_backup);
    let should_advance = if track_mints_batch_requires_replay_on_failure(&protocol_cmd.payload) {
        handler_succeeded
    } else {
        track_protocol_should_advance_revision(stream, handler_succeeded)
    };
    if should_advance {
        let mut store = protocol.lock().expect("track protocol store lock");
        if stream == TrackCommandStream::Wallet {
            store.mark_applied_wallet_demand(&protocol_cmd);
        } else {
            store.mark_applied(&protocol_cmd);
        }
    } else if restage_on_failure {
        track_protocol_stage_for_replay(protocol, protocol_cmd);
    }
}

fn track_worker_prepare_command_delivery<C: TrackWorkerContext>(
    ctx: &Arc<C>,
    payload: &TrackWorkerCommand,
    coalesce_deadline: &mut Option<Instant>,
    push_before_keys: &mut Option<HashSet<Pubkey>>,
    pending_continue_evict: &mut bool,
    pending_release_flush_slot: &mut bool,
) {
    if coalesce_deadline.is_none() {
        *push_before_keys = Some(ctx.snapshot_explicit_subscription_pubkeys());
        *coalesce_deadline =
            Some(Instant::now() + Duration::from_millis(MARKET_DATA_TRACK_WORKER_COALESCE_MS));
    }
    if matches!(payload, TrackWorkerCommand::ContinueGeyserEvict) {
        *pending_continue_evict = true;
    }
    if matches!(payload, TrackWorkerCommand::ScheduleGeyserPushDebounced) {
        *pending_release_flush_slot = true;
    }
}

#[allow(clippy::too_many_arguments)]
fn track_worker_receive_protocol_command<C: TrackWorkerContext>(
    ctx: &Arc<C>,
    protocol: &Arc<Mutex<BoundedProtocolStore>>,
    admission: &mut FixedCapAdmission,
    restore_barrier_pending: &mut bool,
    cmd: ImmutableTrackCommand,
    coalesce_deadline: &mut Option<Instant>,
    push_before_keys: &mut Option<HashSet<Pubkey>>,
    pending_continue_evict: &mut bool,
    pending_release_flush_slot: &mut bool,
) {
    let applicable = {
        let store = protocol.lock().expect("track protocol store lock");
        store.is_applicable(&cmd)
    };
    if applicable {
        track_worker_prepare_command_delivery(
            ctx,
            &cmd.payload,
            coalesce_deadline,
            push_before_keys,
            pending_continue_evict,
            pending_release_flush_slot,
        );
    }
    track_worker_apply_protocol_command(ctx, protocol, admission, restore_barrier_pending, cmd);
}

#[allow(clippy::too_many_arguments)]
fn track_worker_drain_pending_replay<C: TrackWorkerContext>(
    ctx: &Arc<C>,
    protocol: &Arc<Mutex<BoundedProtocolStore>>,
    admission: &mut FixedCapAdmission,
    restore_barrier_pending: &mut bool,
    coalesce_deadline: &mut Option<Instant>,
    push_before_keys: &mut Option<HashSet<Pubkey>>,
    pending_continue_evict: &mut bool,
    pending_release_flush_slot: &mut bool,
) {
    let pending_cmds = {
        let mut store = protocol.lock().expect("track protocol store lock");
        store.take_applicable_pending_sorted()
    };
    for cmd in pending_cmds {
        let applicable = {
            let store = protocol.lock().expect("track protocol store lock");
            store.is_applicable(&cmd)
        };
        if applicable {
            track_worker_prepare_command_delivery(
                ctx,
                &cmd.payload,
                coalesce_deadline,
                push_before_keys,
                pending_continue_evict,
                pending_release_flush_slot,
            );
        }
        track_worker_apply_protocol_command(ctx, protocol, admission, restore_barrier_pending, cmd);
    }
}

fn track_worker_loop<C: TrackWorkerContext + 'static>(
    ctx: Arc<C>,
    rx: std_mpsc::Receiver<ImmutableTrackCommand>,
    queue_depth: Arc<AtomicUsize>,
    track_worker: TrackWorkerSender,
    protocol: Arc<Mutex<BoundedProtocolStore>>,
) {
    let mut admission = FixedCapAdmission::new(ctx.max_tracked_accounts());
    let mut coalesce_deadline: Option<Instant> = None;
    let mut push_before_keys: Option<HashSet<Pubkey>> = None;
    let mut pending_continue_evict = false;
    let mut pending_release_flush_slot = false;
    let mut restore_barrier_pending = false;
    let mut last_snapshot_write = Instant::now()
        .checked_sub(Duration::from_secs(
            MARKET_DATA_EXPLICIT_SET_SNAPSHOT_INTERVAL_SECS,
        ))
        .unwrap_or_else(Instant::now);
    let mut last_momentum_hot_balance_heartbeat = Instant::now()
        .checked_sub(Duration::from_millis(
            MARKET_DATA_MOMENTUM_HOT_BALANCE_REFRESH_HEARTBEAT_MS,
        ))
        .unwrap_or_else(Instant::now);

    loop {
        let timeout = match coalesce_deadline {
            Some(deadline) => deadline.saturating_duration_since(Instant::now()),
            None => Duration::from_millis(MARKET_DATA_TRACK_WORKER_COALESCE_MS),
        };
        let recv_result = if timeout.is_zero() {
            rx.try_recv().map_err(|e| match e {
                std_mpsc::TryRecvError::Empty => std_mpsc::RecvTimeoutError::Timeout,
                std_mpsc::TryRecvError::Disconnected => std_mpsc::RecvTimeoutError::Disconnected,
            })
        } else {
            rx.recv_timeout(timeout)
        };

        match recv_result {
            Ok(job) => {
                track_worker_on_dequeue(&queue_depth, &protocol, &job);
                track_worker_receive_protocol_command(
                    &ctx,
                    &protocol,
                    &mut admission,
                    &mut restore_barrier_pending,
                    job,
                    &mut coalesce_deadline,
                    &mut push_before_keys,
                    &mut pending_continue_evict,
                    &mut pending_release_flush_slot,
                );
                while let Ok(more) = rx.try_recv() {
                    track_worker_on_dequeue(&queue_depth, &protocol, &more);
                    track_worker_receive_protocol_command(
                        &ctx,
                        &protocol,
                        &mut admission,
                        &mut restore_barrier_pending,
                        more,
                        &mut coalesce_deadline,
                        &mut push_before_keys,
                        &mut pending_continue_evict,
                        &mut pending_release_flush_slot,
                    );
                }
            }
            Err(std_mpsc::RecvTimeoutError::Timeout) => {}
            Err(std_mpsc::RecvTimeoutError::Disconnected) => break,
        }

        track_worker_drain_pending_replay(
            &ctx,
            &protocol,
            &mut admission,
            &mut restore_barrier_pending,
            &mut coalesce_deadline,
            &mut push_before_keys,
            &mut pending_continue_evict,
            &mut pending_release_flush_slot,
        );

        let should_push =
            push_before_keys.is_some() && coalesce_deadline.is_some_and(|d| Instant::now() >= d);
        if should_push {
            let before = push_before_keys.take().unwrap_or_default();
            let continue_evict = pending_continue_evict;
            let release_flush_slot = pending_release_flush_slot;
            let barrier_pending = restore_barrier_pending;
            pending_continue_evict = false;
            pending_release_flush_slot = false;
            restore_barrier_pending = false;
            coalesce_deadline = None;
            let _ = ctx.retry_deferred_hot_pool_reserve_registrations(&mut admission);
            let push_ok = track_worker_execute_coalesced_push(
                &ctx,
                &mut admission,
                before,
                continue_evict,
                release_flush_slot,
                barrier_pending,
            );
            if !push_ok {
                track_worker_try_enqueue(&track_worker, TrackWorkerCommand::ContinueGeyserEvict);
            }
            inc_market_data_track_request_coalesce_batches_total();
            ctx.refresh_hot_pool_registry_gauges();
            if push_ok
                && last_snapshot_write.elapsed()
                    >= Duration::from_secs(MARKET_DATA_EXPLICIT_SET_SNAPSHOT_INTERVAL_SECS)
            {
                last_snapshot_write = Instant::now();
                try_write_explicit_set_snapshot_from_ctx(ctx.as_ref(), &admission);
            }
        }

        if last_momentum_hot_balance_heartbeat.elapsed()
            >= Duration::from_millis(MARKET_DATA_MOMENTUM_HOT_BALANCE_REFRESH_HEARTBEAT_MS)
        {
            last_momentum_hot_balance_heartbeat = Instant::now();
            ctx.tick_momentum_hot_balance_refresh_heartbeat();
        }
    }
}

fn try_write_explicit_set_snapshot_from_ctx<C: TrackWorkerContext>(
    ctx: &C,
    admission: &FixedCapAdmission,
) {
    let path = explicit_set_snapshot_path();
    let snapshot = ctx.build_explicit_set_snapshot(admission);
    match write_explicit_set_snapshot(&path, &snapshot) {
        Ok(()) => inc_market_data_explicit_set_snapshot_write_total(),
        Err(e) => {
            inc_market_data_explicit_set_snapshot_write_errors_total();
            tracing::warn!(
                error = %e,
                path = %path.display(),
                "explicit-set snapshot write failed (graceful degrade)"
            );
        }
    }
}

fn track_worker_enqueue_blocking(sender: &TrackWorkerSender, job: TrackWorkerCommand) -> bool {
    inc_market_data_track_worker_enqueue_by_kind(track_command_kind(&job).index());
    let immutable = {
        let mut store = sender.protocol.lock().expect("track protocol store lock");
        if store.try_dedupe_enqueue(&job) {
            inc_market_data_track_worker_enqueue_deduped_total();
            return true;
        }
        store.wrap_command(job)
    };
    match sender.tx.send(immutable.clone()) {
        Ok(()) => {
            let depth = sender.queue_depth.fetch_add(1, Ordering::Relaxed) + 1;
            set_market_data_track_worker_queue_depth(depth);
            let mut store = sender.protocol.lock().expect("track protocol store lock");
            store.note_inflight_enqueued(&immutable);
            store.begin_inflight();
            true
        }
        Err(_) => false,
    }
}

/// Best-effort explicit-set snapshot flush (shutdown / external caller).
pub fn flush_explicit_set_snapshot(track_worker: &TrackWorkerSender) {
    let (done_tx, done_rx) = std_mpsc::channel();
    let job = TrackWorkerCommand::FlushExplicitSetSnapshot { done: done_tx };
    if !track_worker_enqueue_blocking(track_worker, job) {
        tracing::warn!("explicit-set snapshot flush enqueue failed (worker disconnected)");
        return;
    }
    if done_rx.recv_timeout(Duration::from_secs(10)).is_err() {
        tracing::warn!("explicit-set snapshot flush timed out waiting for worker");
    }
}

fn new_track_worker_sender(
    tx: std_mpsc::SyncSender<ImmutableTrackCommand>,
    queue_capacity: usize,
) -> TrackWorkerSender {
    TrackWorkerSender {
        tx,
        queue_depth: Arc::new(AtomicUsize::new(0)),
        queue_capacity,
        protocol: Arc::new(Mutex::new(BoundedProtocolStore::default_caps())),
    }
}

pub fn spawn_track_worker<C: TrackWorkerContext + 'static>(ctx: Arc<C>) -> TrackWorkerSender {
    let queue_capacity = MARKET_DATA_TRACK_WORKER_QUEUE_CAP;
    let (tx, rx) = std_mpsc::sync_channel::<ImmutableTrackCommand>(queue_capacity);
    let queue_depth = Arc::new(AtomicUsize::new(0));
    let depth_worker = Arc::clone(&queue_depth);
    let ctx_worker = Arc::clone(&ctx);
    let sender = TrackWorkerSender {
        tx: tx.clone(),
        queue_depth: Arc::clone(&queue_depth),
        queue_capacity,
        protocol: Arc::new(Mutex::new(BoundedProtocolStore::default_caps())),
    };
    let track_worker = sender.clone();
    let protocol = Arc::clone(&sender.protocol);
    let _join: JoinHandle<()> = std::thread::Builder::new()
        .name("md-track-worker".into())
        .spawn(move || track_worker_loop(ctx_worker, rx, depth_worker, track_worker, protocol))
        .expect("spawn md-track-worker thread");
    sender
}

/// Test helper: enqueue handle that drains jobs without processing (md-state unit tests).
pub fn spawn_noop_track_worker_sender(capacity: usize) -> TrackWorkerSender {
    let (tx, rx) = std_mpsc::sync_channel::<ImmutableTrackCommand>(capacity);
    std::thread::spawn(move || while let Ok(_job) = rx.recv() {});
    new_track_worker_sender(tx, capacity)
}

/// Test helper: inline command processor without Geyser push coalesce loop.
pub fn spawn_inline_track_worker_sender<C: TrackWorkerContext + 'static>(
    ctx: Arc<C>,
    capacity: usize,
) -> TrackWorkerSender {
    let (tx, rx) = std_mpsc::sync_channel::<ImmutableTrackCommand>(capacity);
    let ctx_worker = Arc::clone(&ctx);
    let protocol = Arc::new(Mutex::new(BoundedProtocolStore::default_caps()));
    let protocol_worker = Arc::clone(&protocol);
    std::thread::spawn(move || {
        while let Ok(job) = rx.recv() {
            let mut admission = FixedCapAdmission::new(ctx_worker.max_tracked_accounts());
            let mut restore_barrier_pending = false;
            track_worker_apply_protocol_command(
                &ctx_worker,
                &protocol_worker,
                &mut admission,
                &mut restore_barrier_pending,
                job,
            );
        }
    });
    TrackWorkerSender {
        tx,
        queue_depth: Arc::new(AtomicUsize::new(0)),
        queue_capacity: capacity,
        protocol,
    }
}

#[cfg(test)]
mod tests {
    use super::super::pending::BoundedProtocolStore;
    use super::super::worker_commands::TrackCommandStream;
    use super::*;
    use crate::nats::MomentumActivePoolsUpdate;

    #[test]
    fn queue_full_preserves_demand_in_pending() {
        let sender = spawn_noop_track_worker_sender(0);
        let ok = track_worker_try_enqueue(
            &sender,
            TrackWorkerCommand::ApplyMomentumActivePools(MomentumActivePoolsUpdate {
                version: 1,
                ts_unix_ms: 0,
                active: vec![],
                removed: vec![],
                full_active_snapshot: false,
            }),
        );
        assert!(ok, "queue-full must stage to pending, not drop");
        let store = sender.protocol.lock().expect("lock");
        assert_eq!(store.pending_len(), 1);
    }

    #[test]
    fn superseded_revision_skipped_on_apply() {
        let sender = spawn_noop_track_worker_sender(1);
        let cmd = {
            let mut store = sender.protocol.lock().expect("lock");
            let c = store.wrap_command(TrackWorkerCommand::ApplyMomentumActivePools(
                MomentumActivePoolsUpdate {
                    version: 1,
                    ts_unix_ms: 0,
                    active: vec![],
                    removed: vec![],
                    full_active_snapshot: false,
                },
            ));
            store.mark_applied(&c);
            c
        };
        assert!(sender.tx.try_send(cmd.clone()).is_ok());
        // inline apply path not running; verify applicability directly
        let store = sender.protocol.lock().expect("lock");
        assert!(!store.is_applicable(&cmd));
    }

    #[test]
    fn control_push_advances_revision_superseding_older_pending_push() {
        let mut store = BoundedProtocolStore::default_caps();
        let push_old = store.wrap_command(TrackWorkerCommand::ScheduleGeyserPush);
        store.stage_on_queue_full(push_old.clone());
        let push_new = store.wrap_command(TrackWorkerCommand::ScheduleGeyserPush);
        store.mark_applied(&push_new);
        assert!(!store.is_applicable(&push_old));
        let applicable = store.take_applicable_pending_sorted();
        assert!(
            applicable.is_empty(),
            "older pending push must be superseded after newer push advances watermark"
        );
    }

    #[test]
    fn wallet_stream_revision_held_on_handler_failure() {
        let mut store = BoundedProtocolStore::default_caps();
        let mint = Pubkey::new_unique();
        let cmd = store.wrap_command(TrackWorkerCommand::ApplyWalletPin { mint });
        assert_eq!(cmd.stream, TrackCommandStream::Wallet);
        assert!(!track_protocol_should_advance_revision(cmd.stream, false));
        assert!(store.is_applicable(&cmd));
        assert!(track_protocol_should_advance_revision(cmd.stream, true));
        store.mark_applied(&cmd);
        assert!(!store.is_applicable(&cmd));
    }

    #[test]
    fn tracker_stream_revision_held_on_handler_failure() {
        let mut store = BoundedProtocolStore::default_caps();
        let mint = Pubkey::new_unique();
        let cmd = store.wrap_command(TrackWorkerCommand::TrackMint { mint, pin: None });
        assert_eq!(cmd.stream, TrackCommandStream::Tracker);
        assert!(!track_protocol_should_advance_revision(cmd.stream, false));
        assert!(store.is_applicable(&cmd));
    }

    struct WalletReplayTestCtx {
        fail_wallet_pin_once: std::sync::atomic::AtomicBool,
        pinned: parking_lot::Mutex<Vec<Pubkey>>,
    }

    impl WalletReplayTestCtx {
        fn fail_once() -> Self {
            Self {
                fail_wallet_pin_once: std::sync::atomic::AtomicBool::new(true),
                pinned: parking_lot::Mutex::new(Vec::new()),
            }
        }
    }

    impl TrackWorkerContext for WalletReplayTestCtx {
        fn geyser_sync_batch_debounce_ms(&self) -> u64 {
            0
        }

        fn max_tracked_accounts(&self) -> usize {
            500_000
        }

        fn apply_momentum_active_pools_update(
            &self,
            _admission: &mut FixedCapAdmission,
            _update: &MomentumActivePoolsUpdate,
        ) -> bool {
            false
        }

        fn apply_momentum_snapshot_reconcile(
            &self,
            _admission: &mut FixedCapAdmission,
            _active: &[MomentumActivePoolEntry],
        ) -> bool {
            false
        }

        fn apply_momentum_removed_entries(
            &self,
            _admission: &mut FixedCapAdmission,
            _chunk: &[MomentumRemovedPoolEntry],
        ) -> bool {
            false
        }

        fn apply_momentum_active_entries(
            &self,
            _admission: &mut FixedCapAdmission,
            _chunk: &[MomentumActivePoolEntry],
        ) -> bool {
            false
        }

        fn apply_arb_track_requests_update(
            &self,
            _admission: &mut FixedCapAdmission,
            _update: &ArbTrackRequestsUpdate,
        ) -> bool {
            false
        }

        fn apply_arb_snapshot_reconcile(
            &self,
            _admission: &mut FixedCapAdmission,
            _active: &[ArbTrackActiveEntry],
        ) -> bool {
            false
        }

        fn apply_arb_removed_entries(
            &self,
            _admission: &mut FixedCapAdmission,
            _chunk: &[ArbTrackRemovedEntry],
        ) -> bool {
            false
        }

        fn apply_arb_active_entries(
            &self,
            _admission: &mut FixedCapAdmission,
            _chunk: &[ArbTrackActiveEntry],
        ) -> bool {
            false
        }

        fn apply_wallet_pin(&self, _admission: &mut FixedCapAdmission, mint: Pubkey) -> bool {
            if self
                .fail_wallet_pin_once
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                return false;
            }
            self.pinned.lock().push(mint);
            true
        }

        fn withdraw_wallet_pin(&self, _admission: &mut FixedCapAdmission, _mint: Pubkey) -> bool {
            true
        }

        fn apply_track_mint(
            &self,
            admission: &mut FixedCapAdmission,
            mint: Pubkey,
            pin: Option<TrackPinReason>,
        ) -> bool {
            if pin == Some(TrackPinReason::Wallet) {
                return self.apply_wallet_pin(admission, mint);
            }
            true
        }

        fn refresh_geyser_pins_gauge(&self) {}

        fn hot_pool_registry_pair_count(&self) -> usize {
            0
        }

        fn hot_pool_registry_arb_pool_count(&self) -> usize {
            0
        }

        fn arb_pool_is_must_hot(&self, _pool: Pubkey) -> bool {
            false
        }

        fn refresh_hot_pool_registry_gauges(&self) {}

        fn tick_momentum_hot_balance_refresh_heartbeat(&self) {}

        fn snapshot_explicit_subscription_pubkeys(&self) -> HashSet<Pubkey> {
            HashSet::new()
        }

        fn sync_geyser_tracked_accounts_batched_flush_with_deadline(
            &self,
            _deadline: Instant,
            _admission: &FixedCapAdmission,
        ) -> bool {
            true
        }

        fn continue_geyser_evict_with_deadline(
            &self,
            _deadline: Instant,
            _admission: &FixedCapAdmission,
        ) -> bool {
            true
        }

        fn release_geyser_sync_flush_slot(&self) {}

        fn refresh_tracked_membership_snapshot(&self) {}

        fn explicit_pubkey_rows_for_desired_set(
            &self,
        ) -> Vec<(
            Pubkey,
            crate::market_data::track::ConsumerId,
            Option<Pubkey>,
        )> {
            Vec::new()
        }

        fn build_explicit_set_snapshot(
            &self,
            _admission: &FixedCapAdmission,
        ) -> super::ExplicitSetSnapshot {
            super::ExplicitSetSnapshot::new(None)
        }

        fn apply_explicit_set_snapshot(
            &self,
            _admission: &mut FixedCapAdmission,
            _snapshot: &super::ExplicitSetSnapshot,
        ) -> AdmissionRestoreResult {
            AdmissionRestoreResult::Restored
        }

        fn on_admission_converge_result(
            &self,
            _admission: &FixedCapAdmission,
            _result: AdmissionConvergeResult,
        ) {
        }

        fn prune_tracked_maps_to_admitted(&self, _admission: &FixedCapAdmission) {}

        fn publish_admitted_explicit_physical(&self, _admission: &FixedCapAdmission) {}

        fn tracked_maps_need_prune(&self, _admission: &FixedCapAdmission) -> bool {
            false
        }

        fn publish_admitted_explicit_physical_force(&self, _admission: &FixedCapAdmission) {}

        fn last_synced_explicit_pubkeys_write(
            &self,
        ) -> parking_lot::RwLockWriteGuard<'_, HashSet<Pubkey>> {
            static KEYS: std::sync::LazyLock<parking_lot::RwLock<HashSet<Pubkey>>> =
                std::sync::LazyLock::new(|| parking_lot::RwLock::new(HashSet::new()));
            KEYS.write()
        }

        fn geyser_explicit_readiness_ok(&self) -> bool {
            true
        }

        fn geyser_connect_barrier_pending(&self) -> bool {
            false
        }

        fn signal_restore_barrier(&self, _ok: bool) {}

        fn apply_explicit_cap_shrink(
            &self,
            _admission: &mut FixedCapAdmission,
            _new_cap: usize,
        ) -> CapShrinkResult {
            CapShrinkResult::NoOpAlreadyWithinCap {
                old_cap: 500_000,
                new_cap: 500_000,
            }
        }

        fn retry_deferred_hot_pool_reserve_registrations(
            &self,
            _admission: &mut FixedCapAdmission,
        ) -> bool {
            false
        }
    }

    #[test]
    fn wallet_pin_replays_via_protocol_after_transient_reject() {
        let ctx = Arc::new(WalletReplayTestCtx::fail_once());
        let protocol = Arc::new(Mutex::new(BoundedProtocolStore::default_caps()));
        let mint = Pubkey::new_unique();
        let cmd = {
            let mut store = protocol.lock().expect("lock");
            store.wrap_command(TrackWorkerCommand::ApplyWalletPin { mint })
        };
        let mut admission = FixedCapAdmission::new(500_000);
        let mut restore_barrier_pending = false;

        track_worker_apply_protocol_command(
            &ctx,
            &protocol,
            &mut admission,
            &mut restore_barrier_pending,
            cmd.clone(),
        );
        {
            let store = protocol.lock().expect("lock");
            assert!(
                store.is_applicable(&cmd),
                "failed wallet pin must not advance revision"
            );
            assert_eq!(
                store.pending_len(),
                1,
                "failed wallet pin must be re-staged into bounded pending"
            );
        }
        assert!(ctx.pinned.lock().is_empty());

        track_worker_drain_pending_replay(
            &ctx,
            &protocol,
            &mut admission,
            &mut restore_barrier_pending,
            &mut None,
            &mut None,
            &mut false,
            &mut false,
        );
        {
            let store = protocol.lock().expect("lock");
            assert!(
                !store.is_applicable(&cmd),
                "successful wallet pin must advance revision"
            );
        }
        assert_eq!(ctx.pinned.lock().as_slice(), &[mint]);
    }

    #[test]
    fn wallet_track_mint_replays_after_transient_reject() {
        let ctx = Arc::new(WalletReplayTestCtx::fail_once());
        let protocol = Arc::new(Mutex::new(BoundedProtocolStore::default_caps()));
        let mint = Pubkey::new_unique();
        let cmd = {
            let mut store = protocol.lock().expect("lock");
            store.wrap_command(TrackWorkerCommand::TrackMint {
                mint,
                pin: Some(TrackPinReason::Wallet),
            })
        };
        assert_eq!(cmd.stream, TrackCommandStream::Wallet);
        let mut admission = FixedCapAdmission::new(500_000);
        let mut restore_barrier_pending = false;

        track_worker_apply_protocol_command(
            &ctx,
            &protocol,
            &mut admission,
            &mut restore_barrier_pending,
            cmd.clone(),
        );
        {
            let store = protocol.lock().expect("lock");
            assert!(store.is_applicable(&cmd));
            assert_eq!(store.pending_len(), 1);
        }

        track_worker_drain_pending_replay(
            &ctx,
            &protocol,
            &mut admission,
            &mut restore_barrier_pending,
            &mut None,
            &mut None,
            &mut false,
            &mut false,
        );
        assert_eq!(ctx.pinned.lock().as_slice(), &[mint]);
    }

    #[test]
    fn repeated_tracker_mint_advances_revision_without_pending_growth() {
        struct TrackerIdempotentCtx;

        impl TrackWorkerContext for TrackerIdempotentCtx {
            fn geyser_sync_batch_debounce_ms(&self) -> u64 {
                0
            }

            fn max_tracked_accounts(&self) -> usize {
                500_000
            }

            fn apply_momentum_active_pools_update(
                &self,
                _admission: &mut FixedCapAdmission,
                _update: &MomentumActivePoolsUpdate,
            ) -> bool {
                false
            }

            fn apply_momentum_snapshot_reconcile(
                &self,
                _admission: &mut FixedCapAdmission,
                _active: &[MomentumActivePoolEntry],
            ) -> bool {
                false
            }

            fn apply_momentum_removed_entries(
                &self,
                _admission: &mut FixedCapAdmission,
                _chunk: &[MomentumRemovedPoolEntry],
            ) -> bool {
                false
            }

            fn apply_momentum_active_entries(
                &self,
                _admission: &mut FixedCapAdmission,
                _chunk: &[MomentumActivePoolEntry],
            ) -> bool {
                false
            }

            fn apply_arb_track_requests_update(
                &self,
                _admission: &mut FixedCapAdmission,
                _update: &ArbTrackRequestsUpdate,
            ) -> bool {
                false
            }

            fn apply_arb_snapshot_reconcile(
                &self,
                _admission: &mut FixedCapAdmission,
                _active: &[ArbTrackActiveEntry],
            ) -> bool {
                false
            }

            fn apply_arb_removed_entries(
                &self,
                _admission: &mut FixedCapAdmission,
                _chunk: &[ArbTrackRemovedEntry],
            ) -> bool {
                false
            }

            fn apply_arb_active_entries(
                &self,
                _admission: &mut FixedCapAdmission,
                _chunk: &[ArbTrackActiveEntry],
            ) -> bool {
                false
            }

            fn apply_wallet_pin(&self, _admission: &mut FixedCapAdmission, _mint: Pubkey) -> bool {
                true
            }

            fn withdraw_wallet_pin(
                &self,
                _admission: &mut FixedCapAdmission,
                _mint: Pubkey,
            ) -> bool {
                true
            }

            fn apply_track_mint(
                &self,
                _admission: &mut FixedCapAdmission,
                _mint: Pubkey,
                _pin: Option<TrackPinReason>,
            ) -> bool {
                true
            }

            fn refresh_geyser_pins_gauge(&self) {}

            fn hot_pool_registry_pair_count(&self) -> usize {
                0
            }

            fn hot_pool_registry_arb_pool_count(&self) -> usize {
                0
            }

            fn arb_pool_is_must_hot(&self, _pool: Pubkey) -> bool {
                false
            }

            fn refresh_hot_pool_registry_gauges(&self) {}

            fn tick_momentum_hot_balance_refresh_heartbeat(&self) {}

            fn snapshot_explicit_subscription_pubkeys(&self) -> HashSet<Pubkey> {
                HashSet::new()
            }

            fn sync_geyser_tracked_accounts_batched_flush_with_deadline(
                &self,
                _deadline: Instant,
                _admission: &FixedCapAdmission,
            ) -> bool {
                true
            }

            fn continue_geyser_evict_with_deadline(
                &self,
                _deadline: Instant,
                _admission: &FixedCapAdmission,
            ) -> bool {
                true
            }

            fn release_geyser_sync_flush_slot(&self) {}

            fn refresh_tracked_membership_snapshot(&self) {}

            fn explicit_pubkey_rows_for_desired_set(
                &self,
            ) -> Vec<(
                Pubkey,
                crate::market_data::track::ConsumerId,
                Option<Pubkey>,
            )> {
                Vec::new()
            }

            fn build_explicit_set_snapshot(
                &self,
                _admission: &FixedCapAdmission,
            ) -> super::ExplicitSetSnapshot {
                super::ExplicitSetSnapshot::new(None)
            }

            fn apply_explicit_set_snapshot(
                &self,
                _admission: &mut FixedCapAdmission,
                _snapshot: &super::ExplicitSetSnapshot,
            ) -> AdmissionRestoreResult {
                AdmissionRestoreResult::Restored
            }

            fn on_admission_converge_result(
                &self,
                _admission: &FixedCapAdmission,
                _result: AdmissionConvergeResult,
            ) {
            }

            fn prune_tracked_maps_to_admitted(&self, _admission: &FixedCapAdmission) {}

            fn publish_admitted_explicit_physical(&self, _admission: &FixedCapAdmission) {}

            fn tracked_maps_need_prune(&self, _admission: &FixedCapAdmission) -> bool {
                false
            }

            fn publish_admitted_explicit_physical_force(&self, _admission: &FixedCapAdmission) {}

            fn last_synced_explicit_pubkeys_write(
                &self,
            ) -> parking_lot::RwLockWriteGuard<'_, HashSet<Pubkey>> {
                static KEYS: std::sync::LazyLock<parking_lot::RwLock<HashSet<Pubkey>>> =
                    std::sync::LazyLock::new(|| parking_lot::RwLock::new(HashSet::new()));
                KEYS.write()
            }

            fn geyser_explicit_readiness_ok(&self) -> bool {
                true
            }

            fn geyser_connect_barrier_pending(&self) -> bool {
                false
            }

            fn signal_restore_barrier(&self, _ok: bool) {}

            fn apply_explicit_cap_shrink(
                &self,
                _admission: &mut FixedCapAdmission,
                _new_cap: usize,
            ) -> CapShrinkResult {
                CapShrinkResult::NoOpAlreadyWithinCap {
                    old_cap: 500_000,
                    new_cap: 500_000,
                }
            }

            fn retry_deferred_hot_pool_reserve_registrations(
                &self,
                _admission: &mut FixedCapAdmission,
            ) -> bool {
                false
            }
        }

        let ctx = Arc::new(TrackerIdempotentCtx);
        let protocol = Arc::new(Mutex::new(BoundedProtocolStore::default_caps()));
        let mint = Pubkey::new_unique();
        let mut admission = FixedCapAdmission::new(500_000);
        let mut restore_barrier_pending = false;

        for _ in 0..2 {
            let cmd = {
                let mut store = protocol.lock().expect("lock");
                store.wrap_command(TrackWorkerCommand::TrackMint { mint, pin: None })
            };
            track_worker_apply_protocol_command(
                &ctx,
                &protocol,
                &mut admission,
                &mut restore_barrier_pending,
                cmd.clone(),
            );
            let store = protocol.lock().expect("lock");
            assert_eq!(
                store.pending_len(),
                0,
                "idempotent tracker mint must not re-stage pending"
            );
            assert!(
                !store.is_applicable(&cmd),
                "each tracker mint command must advance revision"
            );
        }
    }

    struct WithdrawSupersedesTrackerTestCtx {
        tracked: parking_lot::Mutex<std::collections::HashSet<Pubkey>>,
    }

    impl WithdrawSupersedesTrackerTestCtx {
        fn new() -> Self {
            Self {
                tracked: parking_lot::Mutex::new(std::collections::HashSet::new()),
            }
        }
    }

    impl TrackWorkerContext for WithdrawSupersedesTrackerTestCtx {
        fn geyser_sync_batch_debounce_ms(&self) -> u64 {
            0
        }

        fn max_tracked_accounts(&self) -> usize {
            500_000
        }

        fn apply_momentum_active_pools_update(
            &self,
            _admission: &mut FixedCapAdmission,
            _update: &MomentumActivePoolsUpdate,
        ) -> bool {
            false
        }

        fn apply_momentum_snapshot_reconcile(
            &self,
            _admission: &mut FixedCapAdmission,
            _active: &[MomentumActivePoolEntry],
        ) -> bool {
            false
        }

        fn apply_momentum_removed_entries(
            &self,
            _admission: &mut FixedCapAdmission,
            _chunk: &[MomentumRemovedPoolEntry],
        ) -> bool {
            false
        }

        fn apply_momentum_active_entries(
            &self,
            _admission: &mut FixedCapAdmission,
            _chunk: &[MomentumActivePoolEntry],
        ) -> bool {
            false
        }

        fn apply_arb_track_requests_update(
            &self,
            _admission: &mut FixedCapAdmission,
            _update: &ArbTrackRequestsUpdate,
        ) -> bool {
            false
        }

        fn apply_arb_snapshot_reconcile(
            &self,
            _admission: &mut FixedCapAdmission,
            _active: &[ArbTrackActiveEntry],
        ) -> bool {
            false
        }

        fn apply_arb_removed_entries(
            &self,
            _admission: &mut FixedCapAdmission,
            _chunk: &[ArbTrackRemovedEntry],
        ) -> bool {
            false
        }

        fn apply_arb_active_entries(
            &self,
            _admission: &mut FixedCapAdmission,
            _chunk: &[ArbTrackActiveEntry],
        ) -> bool {
            false
        }

        fn apply_wallet_pin(&self, _admission: &mut FixedCapAdmission, mint: Pubkey) -> bool {
            self.tracked.lock().insert(mint);
            true
        }

        fn withdraw_wallet_pin(&self, _admission: &mut FixedCapAdmission, mint: Pubkey) -> bool {
            self.tracked.lock().remove(&mint);
            true
        }

        fn apply_track_mint(
            &self,
            _admission: &mut FixedCapAdmission,
            mint: Pubkey,
            pin: Option<TrackPinReason>,
        ) -> bool {
            if pin.is_none() {
                self.tracked.lock().insert(mint);
            }
            true
        }

        fn refresh_geyser_pins_gauge(&self) {}

        fn hot_pool_registry_pair_count(&self) -> usize {
            0
        }

        fn hot_pool_registry_arb_pool_count(&self) -> usize {
            0
        }

        fn arb_pool_is_must_hot(&self, _pool: Pubkey) -> bool {
            false
        }

        fn refresh_hot_pool_registry_gauges(&self) {}

        fn tick_momentum_hot_balance_refresh_heartbeat(&self) {}

        fn snapshot_explicit_subscription_pubkeys(&self) -> HashSet<Pubkey> {
            HashSet::new()
        }

        fn sync_geyser_tracked_accounts_batched_flush_with_deadline(
            &self,
            _deadline: Instant,
            _admission: &FixedCapAdmission,
        ) -> bool {
            true
        }

        fn continue_geyser_evict_with_deadline(
            &self,
            _deadline: Instant,
            _admission: &FixedCapAdmission,
        ) -> bool {
            true
        }

        fn release_geyser_sync_flush_slot(&self) {}

        fn refresh_tracked_membership_snapshot(&self) {}

        fn explicit_pubkey_rows_for_desired_set(
            &self,
        ) -> Vec<(
            Pubkey,
            crate::market_data::track::ConsumerId,
            Option<Pubkey>,
        )> {
            Vec::new()
        }

        fn build_explicit_set_snapshot(
            &self,
            _admission: &FixedCapAdmission,
        ) -> super::ExplicitSetSnapshot {
            super::ExplicitSetSnapshot::new(None)
        }

        fn apply_explicit_set_snapshot(
            &self,
            _admission: &mut FixedCapAdmission,
            _snapshot: &super::ExplicitSetSnapshot,
        ) -> AdmissionRestoreResult {
            AdmissionRestoreResult::Restored
        }

        fn on_admission_converge_result(
            &self,
            _admission: &FixedCapAdmission,
            _result: AdmissionConvergeResult,
        ) {
        }

        fn prune_tracked_maps_to_admitted(&self, _admission: &FixedCapAdmission) {}

        fn publish_admitted_explicit_physical(&self, _admission: &FixedCapAdmission) {}

        fn tracked_maps_need_prune(&self, _admission: &FixedCapAdmission) -> bool {
            false
        }

        fn publish_admitted_explicit_physical_force(&self, _admission: &FixedCapAdmission) {}

        fn last_synced_explicit_pubkeys_write(
            &self,
        ) -> parking_lot::RwLockWriteGuard<'_, HashSet<Pubkey>> {
            static KEYS: std::sync::LazyLock<parking_lot::RwLock<HashSet<Pubkey>>> =
                std::sync::LazyLock::new(|| parking_lot::RwLock::new(HashSet::new()));
            KEYS.write()
        }

        fn geyser_explicit_readiness_ok(&self) -> bool {
            true
        }

        fn geyser_connect_barrier_pending(&self) -> bool {
            false
        }

        fn signal_restore_barrier(&self, _ok: bool) {}

        fn apply_explicit_cap_shrink(
            &self,
            _admission: &mut FixedCapAdmission,
            _new_cap: usize,
        ) -> CapShrinkResult {
            CapShrinkResult::NoOpAlreadyWithinCap {
                old_cap: 500_000,
                new_cap: 500_000,
            }
        }

        fn retry_deferred_hot_pool_reserve_registrations(
            &self,
            _admission: &mut FixedCapAdmission,
        ) -> bool {
            false
        }
    }

    #[test]
    fn wallet_withdraw_supersedes_staged_tracker_replay() {
        let ctx = Arc::new(WithdrawSupersedesTrackerTestCtx::new());
        let protocol = Arc::new(Mutex::new(BoundedProtocolStore::default_caps()));
        let mint = Pubkey::new_unique();
        let mut admission = FixedCapAdmission::new(500_000);
        let mut restore_barrier_pending = false;

        let tracker_cmd = {
            let mut store = protocol.lock().expect("lock");
            let cmd = store.wrap_command(TrackWorkerCommand::TrackMint { mint, pin: None });
            store.stage_on_queue_full(cmd.clone());
            cmd
        };
        ctx.tracked.lock().insert(mint);

        let withdraw_cmd = {
            let mut store = protocol.lock().expect("lock");
            store.wrap_command(TrackWorkerCommand::WithdrawWalletPin { mint })
        };
        track_worker_apply_protocol_command(
            &ctx,
            &protocol,
            &mut admission,
            &mut restore_barrier_pending,
            withdraw_cmd,
        );
        assert!(!ctx.tracked.lock().contains(&mint));

        track_worker_drain_pending_replay(
            &ctx,
            &protocol,
            &mut admission,
            &mut restore_barrier_pending,
            &mut None,
            &mut None,
            &mut false,
            &mut false,
        );
        assert!(
            !ctx.tracked.lock().contains(&mint),
            "superseded tracker replay must not re-admit mint after wallet withdraw"
        );
        {
            let store = protocol.lock().expect("lock");
            assert!(!store.is_applicable(&tracker_cmd));
        }
    }

    #[test]
    fn chunked_momentum_apply_retries_deferred_hot_pool_reserves() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct ChunkedRetryCtx {
            retry_calls: AtomicUsize,
            last_synced: parking_lot::RwLock<HashSet<Pubkey>>,
        }

        impl TrackWorkerContext for ChunkedRetryCtx {
            fn geyser_sync_batch_debounce_ms(&self) -> u64 {
                0
            }
            fn max_tracked_accounts(&self) -> usize {
                500_000
            }
            fn apply_momentum_active_pools_update(
                &self,
                _admission: &mut FixedCapAdmission,
                _update: &MomentumActivePoolsUpdate,
            ) -> bool {
                false
            }
            fn apply_momentum_snapshot_reconcile(
                &self,
                _admission: &mut FixedCapAdmission,
                _active: &[MomentumActivePoolEntry],
            ) -> bool {
                false
            }
            fn apply_momentum_removed_entries(
                &self,
                _admission: &mut FixedCapAdmission,
                _removed: &[MomentumRemovedPoolEntry],
            ) -> bool {
                false
            }
            fn apply_momentum_active_entries(
                &self,
                _admission: &mut FixedCapAdmission,
                _active: &[MomentumActivePoolEntry],
            ) -> bool {
                false
            }
            fn apply_arb_track_requests_update(
                &self,
                _admission: &mut FixedCapAdmission,
                _update: &ArbTrackRequestsUpdate,
            ) -> bool {
                false
            }
            fn apply_arb_snapshot_reconcile(
                &self,
                _admission: &mut FixedCapAdmission,
                _active: &[ArbTrackActiveEntry],
            ) -> bool {
                false
            }
            fn apply_arb_removed_entries(
                &self,
                _admission: &mut FixedCapAdmission,
                _removed: &[ArbTrackRemovedEntry],
            ) -> bool {
                false
            }
            fn apply_arb_active_entries(
                &self,
                _admission: &mut FixedCapAdmission,
                _active: &[ArbTrackActiveEntry],
            ) -> bool {
                false
            }
            fn apply_wallet_pin(&self, _admission: &mut FixedCapAdmission, _mint: Pubkey) -> bool {
                false
            }
            fn withdraw_wallet_pin(
                &self,
                _admission: &mut FixedCapAdmission,
                _mint: Pubkey,
            ) -> bool {
                false
            }
            fn apply_track_mint(
                &self,
                _admission: &mut FixedCapAdmission,
                _mint: Pubkey,
                _pin: Option<TrackPinReason>,
            ) -> bool {
                false
            }
            fn refresh_geyser_pins_gauge(&self) {}
            fn hot_pool_registry_pair_count(&self) -> usize {
                0
            }
            fn hot_pool_registry_arb_pool_count(&self) -> usize {
                0
            }
            fn arb_pool_is_must_hot(&self, _pool: Pubkey) -> bool {
                false
            }
            fn refresh_hot_pool_registry_gauges(&self) {}
            fn tick_momentum_hot_balance_refresh_heartbeat(&self) {}
            fn snapshot_explicit_subscription_pubkeys(&self) -> HashSet<Pubkey> {
                HashSet::new()
            }
            fn sync_geyser_tracked_accounts_batched_flush_with_deadline(
                &self,
                _deadline: Instant,
                _admission: &FixedCapAdmission,
            ) -> bool {
                true
            }
            fn continue_geyser_evict_with_deadline(
                &self,
                _deadline: Instant,
                _admission: &FixedCapAdmission,
            ) -> bool {
                true
            }
            fn release_geyser_sync_flush_slot(&self) {}
            fn refresh_tracked_membership_snapshot(&self) {}
            fn explicit_pubkey_rows_for_desired_set(
                &self,
            ) -> Vec<(Pubkey, super::super::ConsumerId, Option<Pubkey>)> {
                Vec::new()
            }
            fn build_explicit_set_snapshot(
                &self,
                _admission: &FixedCapAdmission,
            ) -> super::super::snapshot::ExplicitSetSnapshot {
                super::super::snapshot::ExplicitSetSnapshot::new(None)
            }
            fn apply_explicit_set_snapshot(
                &self,
                _admission: &mut FixedCapAdmission,
                _snapshot: &super::super::snapshot::ExplicitSetSnapshot,
            ) -> super::super::admission_wiring::AdmissionRestoreResult {
                super::super::admission_wiring::AdmissionRestoreResult::Restored
            }
            fn on_admission_converge_result(
                &self,
                _admission: &FixedCapAdmission,
                _result: super::super::admission_wiring::AdmissionConvergeResult,
            ) {
            }
            fn prune_tracked_maps_to_admitted(&self, _admission: &FixedCapAdmission) {}
            fn publish_admitted_explicit_physical(&self, _admission: &FixedCapAdmission) {}
            fn tracked_maps_need_prune(&self, _admission: &FixedCapAdmission) -> bool {
                false
            }
            fn publish_admitted_explicit_physical_force(&self, _admission: &FixedCapAdmission) {}
            fn last_synced_explicit_pubkeys_write(
                &self,
            ) -> parking_lot::RwLockWriteGuard<'_, HashSet<Pubkey>> {
                self.last_synced.write()
            }
            fn geyser_explicit_readiness_ok(&self) -> bool {
                true
            }
            fn geyser_connect_barrier_pending(&self) -> bool {
                false
            }
            fn signal_restore_barrier(&self, _ok: bool) {}
            fn apply_explicit_cap_shrink(
                &self,
                _admission: &mut FixedCapAdmission,
                _new_cap: usize,
            ) -> super::super::explicit_admission::CapShrinkResult {
                super::super::explicit_admission::CapShrinkResult::NoOpAlreadyWithinCap {
                    old_cap: 500_000,
                    new_cap: 500_000,
                }
            }
            fn retry_deferred_hot_pool_reserve_registrations(
                &self,
                _admission: &mut FixedCapAdmission,
            ) -> bool {
                self.retry_calls.fetch_add(1, Ordering::Relaxed);
                false
            }
        }

        let ctx = Arc::new(ChunkedRetryCtx {
            retry_calls: AtomicUsize::new(0),
            last_synced: parking_lot::RwLock::new(HashSet::new()),
        });
        let mut admission = FixedCapAdmission::new(500_000);
        let mut active = Vec::new();
        for i in 0..48 {
            active.push(MomentumActivePoolEntry {
                mint: Pubkey::new_unique().to_string(),
                pool: Pubkey::new_unique().to_string(),
                pin_reason: crate::nats::MomentumActivePinReason::Tracker,
            });
            let _ = i;
        }
        apply_momentum_active_pools_on_track_worker(
            &ctx,
            &mut admission,
            MomentumActivePoolsUpdate {
                version: 1,
                ts_unix_ms: 1,
                active,
                removed: vec![],
                full_active_snapshot: false,
            },
        );
        assert_eq!(ctx.retry_calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn control_push_evict_variants_advance_revision() {
        let mut store = BoundedProtocolStore::default_caps();
        for payload in [
            TrackWorkerCommand::ScheduleGeyserPush,
            TrackWorkerCommand::ScheduleGeyserPushDebounced,
            TrackWorkerCommand::ContinueGeyserEvict,
            TrackWorkerCommand::ShedTrackerUnderExecHotPressure { max_groups: 8 },
            TrackWorkerCommand::ShedMomentumUnderExecHotPressure { max_groups: 8 },
            TrackWorkerCommand::ShedArbUnderExecHotPressure { max_groups: 8 },
        ] {
            let cmd = store.wrap_command(payload);
            store.mark_applied(&cmd);
            assert!(
                !store.is_applicable(&cmd),
                "applied revision must not remain applicable"
            );
        }
    }

    #[test]
    fn enqueue_dedupe_skips_duplicate_sync_when_queued() {
        let sender = spawn_noop_track_worker_sender(1);
        assert!(track_worker_try_enqueue(
            &sender,
            TrackWorkerCommand::ScheduleGeyserPush
        ));
        assert!(track_worker_try_enqueue(
            &sender,
            TrackWorkerCommand::ScheduleGeyserPushDebounced
        ));
        let store = sender.protocol.lock().expect("lock");
        assert_eq!(
            store.pending_len(),
            0,
            "deduped sync must not stage when inflight"
        );
    }
}
