//! Phase 5a: `md-track-worker` OS thread — bounded enqueue, command processing, coalesced Geyser push.

use super::admission_wiring::{AdmissionConvergeResult, AdmissionRestoreResult};
use super::explicit_admission::{CapShrinkResult, FixedCapAdmission};
use super::geyser_sync::{converge_admission_from_ctx, track_worker_execute_coalesced_push};
use super::pending::{BoundedProtocolStore, StageResult};
use super::snapshot::{
    explicit_set_snapshot_path, write_explicit_set_snapshot, ExplicitSetSnapshot,
    MARKET_DATA_EXPLICIT_SET_SNAPSHOT_INTERVAL_SECS,
};
use super::worker_commands::ImmutableTrackCommand;
pub use super::worker_commands::{TrackPinReason, TrackWorkerCommand};
use crate::metrics::{
    inc_market_data_explicit_set_snapshot_write_errors_total,
    inc_market_data_explicit_set_snapshot_write_total,
    inc_market_data_geyser_tracking_enqueue_dropped_total,
    inc_market_data_track_protocol_superseded_revisions_total,
    inc_market_data_track_request_coalesce_batches_total,
    record_market_data_arb_track_requests_messages_total,
    record_market_data_momentum_active_pool_messages_total, set_market_data_arb_pinned_pools_gauge,
    set_market_data_momentum_active_pool_pins_gauge, set_market_data_track_worker_queue_depth,
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
    fn apply_momentum_active_pools_update(&self, update: &MomentumActivePoolsUpdate) -> bool;
    fn apply_momentum_snapshot_reconcile(&self, active: &[MomentumActivePoolEntry]) -> bool;
    fn apply_momentum_removed_entries(&self, chunk: &[MomentumRemovedPoolEntry]) -> bool;
    fn apply_momentum_active_entries(&self, chunk: &[MomentumActivePoolEntry]) -> bool;
    fn apply_arb_track_requests_update(&self, update: &ArbTrackRequestsUpdate) -> bool;
    fn apply_arb_snapshot_reconcile(&self, active: &[ArbTrackActiveEntry]) -> bool;
    fn apply_arb_removed_entries(&self, chunk: &[ArbTrackRemovedEntry]) -> bool;
    fn apply_arb_active_entries(&self, chunk: &[ArbTrackActiveEntry]) -> bool;
    fn track_mint_for_geyser_metadata(&self, mint: Pubkey, pin: Option<TrackPinReason>) -> bool;
    fn refresh_geyser_pins_gauge(&self);
    fn hot_pool_registry_pair_count(&self) -> usize;
    fn hot_pool_registry_arb_pool_count(&self) -> usize;
    fn refresh_hot_pool_registry_gauges(&self);
    fn snapshot_explicit_subscription_pubkeys(&self) -> HashSet<Pubkey>;
    fn pending_geyser_evict(&self) -> bool;
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
    fn apply_explicit_set_snapshot_legacy(&self, snapshot: &ExplicitSetSnapshot) -> usize;
    fn on_admission_converge_result(
        &self,
        admission: &FixedCapAdmission,
        result: AdmissionConvergeResult,
    );
    fn prune_tracked_maps_to_admitted(&self, admission: &FixedCapAdmission);
    fn publish_admitted_explicit_physical(&self, admission: &FixedCapAdmission);
    fn last_synced_explicit_pubkeys_write(
        &self,
    ) -> parking_lot::RwLockWriteGuard<'_, HashSet<Pubkey>>;
    fn clear_pending_geyser_evict(&self);
    fn geyser_explicit_readiness_ok(&self) -> bool;
    fn signal_restore_barrier(&self, ok: bool);
    fn apply_explicit_cap_shrink(
        &self,
        admission: &mut FixedCapAdmission,
        new_cap: usize,
    ) -> CapShrinkResult;
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
    let immutable = {
        let mut store = sender.protocol.lock().expect("track protocol store lock");
        store.wrap_command(job)
    };
    if sender.queue_depth.load(Ordering::Relaxed) >= sender.queue_capacity {
        return track_worker_stage_on_queue_full(&sender.protocol, immutable);
    }
    if sender.tx.try_send(immutable.clone()).is_ok() {
        let depth = sender.queue_depth.fetch_add(1, Ordering::Relaxed) + 1;
        set_market_data_track_worker_queue_depth(depth);
        let mut store = sender.protocol.lock().expect("track protocol store lock");
        store.begin_inflight();
        true
    } else {
        track_worker_stage_on_queue_full(&sender.protocol, immutable)
    }
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
    update: MomentumActivePoolsUpdate,
) -> bool {
    let item_count = update.active.len() + update.removed.len();
    if item_count <= MARKET_DATA_MOMENTUM_APPLY_CHUNK_THRESHOLD {
        return ctx.apply_momentum_active_pools_update(&update);
    }
    record_market_data_momentum_active_pool_messages_total();
    let mut batch_dirty = false;
    if update.full_active_snapshot {
        batch_dirty |= ctx.apply_momentum_snapshot_reconcile(&update.active);
        std::thread::yield_now();
    }
    for chunk in update.removed.chunks(MARKET_DATA_MOMENTUM_APPLY_CHUNK_SIZE) {
        batch_dirty |= ctx.apply_momentum_removed_entries(chunk);
        std::thread::yield_now();
    }
    for chunk in update.active.chunks(MARKET_DATA_MOMENTUM_APPLY_CHUNK_SIZE) {
        batch_dirty |= ctx.apply_momentum_active_entries(chunk);
        std::thread::yield_now();
    }
    if !batch_dirty {
        ctx.refresh_geyser_pins_gauge();
    }
    set_market_data_momentum_active_pool_pins_gauge(ctx.hot_pool_registry_pair_count());
    batch_dirty
}

/// Phase 3: sync arb track apply on `md-track-worker` thread (no Tokio yield).
pub fn apply_arb_track_requests_on_track_worker<C: TrackWorkerContext>(
    ctx: &Arc<C>,
    update: ArbTrackRequestsUpdate,
) -> bool {
    let item_count = update.active.len() + update.removed.len();
    if item_count <= MARKET_DATA_ARB_APPLY_CHUNK_THRESHOLD {
        return ctx.apply_arb_track_requests_update(&update);
    }
    record_market_data_arb_track_requests_messages_total();
    let mut batch_dirty = false;
    if update.reconcile {
        batch_dirty |= ctx.apply_arb_snapshot_reconcile(&update.active);
        std::thread::yield_now();
    }
    for chunk in update.removed.chunks(MARKET_DATA_ARB_APPLY_CHUNK_SIZE) {
        batch_dirty |= ctx.apply_arb_removed_entries(chunk);
        std::thread::yield_now();
    }
    for chunk in update.active.chunks(MARKET_DATA_ARB_APPLY_CHUNK_SIZE) {
        batch_dirty |= ctx.apply_arb_active_entries(chunk);
        std::thread::yield_now();
    }
    if !batch_dirty {
        ctx.refresh_geyser_pins_gauge();
    }
    set_market_data_arb_pinned_pools_gauge(ctx.hot_pool_registry_arb_pool_count());
    batch_dirty
}

pub fn track_worker_process_command<C: TrackWorkerContext>(
    ctx: &Arc<C>,
    admission: &mut FixedCapAdmission,
    restore_barrier_pending: &mut bool,
    job: TrackWorkerCommand,
) -> bool {
    match job {
        TrackWorkerCommand::ApplyMomentumActivePools(update) => {
            apply_momentum_active_pools_on_track_worker(ctx, update)
        }
        TrackWorkerCommand::ApplyArbTrackRequests(update) => {
            apply_arb_track_requests_on_track_worker(ctx, update)
        }
        TrackWorkerCommand::ApplyWalletPin { mint } => {
            ctx.track_mint_for_geyser_metadata(mint, Some(TrackPinReason::Wallet))
        }
        TrackWorkerCommand::TrackMint { mint, pin } => {
            ctx.track_mint_for_geyser_metadata(mint, pin)
        }
        TrackWorkerCommand::ScheduleGeyserSyncAfterConfigChange => {
            let new_cap = ctx.max_tracked_accounts();
            let _ = ctx.apply_explicit_cap_shrink(admission, new_cap);
            true
        }
        TrackWorkerCommand::RestoreExplicitSnapshot(snapshot) => {
            let legacy = ctx.apply_explicit_set_snapshot_legacy(&snapshot);
            let restore = ctx.apply_explicit_set_snapshot(admission, &snapshot);
            *restore_barrier_pending = true;
            legacy > 0 || matches!(restore, AdmissionRestoreResult::Restored)
        }
        TrackWorkerCommand::ScheduleGeyserPush
        | TrackWorkerCommand::ScheduleGeyserPushDebounced
        | TrackWorkerCommand::ContinueGeyserEvict => false,
    }
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
        store.is_applicable(cmd.stream, cmd.revision)
    };
    if !applicable {
        inc_market_data_track_protocol_superseded_revisions_total();
        return;
    }
    let _ = track_worker_process_command(ctx, admission, restore_barrier_pending, cmd.payload);
    let mut store = protocol.lock().expect("track protocol store lock");
    store.mark_applied(cmd.stream, cmd.revision);
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
        store.is_applicable(cmd.stream, cmd.revision)
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
            store.is_applicable(cmd.stream, cmd.revision)
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
                track_worker_dec_queue_depth(&queue_depth, &protocol);
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
                    track_worker_dec_queue_depth(&queue_depth, &protocol);
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
            if !track_worker_execute_coalesced_push(
                &ctx,
                &mut admission,
                before,
                continue_evict,
                release_flush_slot,
                barrier_pending,
            ) {
                track_worker_try_enqueue(&track_worker, TrackWorkerCommand::ContinueGeyserEvict);
            }
            inc_market_data_track_request_coalesce_batches_total();
            ctx.refresh_hot_pool_registry_gauges();
            if last_snapshot_write.elapsed()
                >= Duration::from_secs(MARKET_DATA_EXPLICIT_SET_SNAPSHOT_INTERVAL_SECS)
            {
                last_snapshot_write = Instant::now();
                try_write_explicit_set_snapshot_from_ctx(ctx.as_ref(), &admission);
            }
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

/// Best-effort explicit-set snapshot flush (shutdown / external caller).
pub fn flush_explicit_set_snapshot<C: TrackWorkerContext>(ctx: &C) {
    let mut admission = FixedCapAdmission::new(ctx.max_tracked_accounts());
    let _ = converge_admission_from_ctx(ctx, &mut admission);
    try_write_explicit_set_snapshot_from_ctx(ctx, &admission);
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
            store.mark_applied(TrackCommandStream::Momentum, c.revision);
            c
        };
        assert!(sender.tx.try_send(cmd.clone()).is_ok());
        // inline apply path not running; verify applicability directly
        let store = sender.protocol.lock().expect("lock");
        assert!(!store.is_applicable(cmd.stream, cmd.revision));
    }

    #[test]
    fn control_push_advances_revision_superseding_older_pending_push() {
        let mut store = BoundedProtocolStore::default_caps();
        let push_old = store.wrap_command(TrackWorkerCommand::ScheduleGeyserPush);
        store.stage_on_queue_full(push_old.clone());
        let push_new = store.wrap_command(TrackWorkerCommand::ScheduleGeyserPush);
        store.mark_applied(push_new.stream, push_new.revision);
        assert!(!store.is_applicable(push_old.stream, push_old.revision));
        let applicable = store.take_applicable_pending_sorted();
        assert!(
            applicable.is_empty(),
            "older pending push must be superseded after newer push advances watermark"
        );
    }

    #[test]
    fn control_push_evict_variants_advance_revision() {
        let mut store = BoundedProtocolStore::default_caps();
        for payload in [
            TrackWorkerCommand::ScheduleGeyserPush,
            TrackWorkerCommand::ScheduleGeyserPushDebounced,
            TrackWorkerCommand::ContinueGeyserEvict,
        ] {
            let cmd = store.wrap_command(payload);
            store.mark_applied(cmd.stream, cmd.revision);
            assert!(
                !store.is_applicable(cmd.stream, cmd.revision),
                "applied revision must not remain applicable"
            );
        }
    }
}
