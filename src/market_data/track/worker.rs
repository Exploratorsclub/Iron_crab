//! Phase 5a: `md-track-worker` OS thread — bounded enqueue, command processing, coalesced Geyser push.

use super::desired_set::DesiredExplicitSet;
use super::geyser_sync::track_worker_execute_coalesced_push;
use super::snapshot::{
    explicit_set_snapshot_path, write_explicit_set_snapshot, ExplicitSetSnapshot,
    MARKET_DATA_EXPLICIT_SET_SNAPSHOT_INTERVAL_SECS,
};
use super::worker_commands::{GeyserPinReason, TrackWorkerCommand};
use crate::metrics::{
    inc_market_data_explicit_set_snapshot_write_errors_total,
    inc_market_data_explicit_set_snapshot_write_total,
    inc_market_data_geyser_tracking_enqueue_dropped_total,
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
use std::sync::Arc;
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

/// Pin reason for explicit Geyser tracking (maps to [`super::ConsumerId`] in rebuild).
pub type TrackPinReason = GeyserPinReason;

/// Context surface required by the track-worker loop and apply helpers.
pub trait TrackWorkerContext: Send + Sync {
    fn geyser_sync_batch_debounce_ms(&self) -> u64;
    fn max_tracked_accounts(&self) -> usize;
    fn apply_momentum_active_pools_update(
        &self,
        desired: &mut DesiredExplicitSet,
        update: &MomentumActivePoolsUpdate,
    ) -> bool;
    fn apply_momentum_snapshot_reconcile(
        &self,
        desired: &mut DesiredExplicitSet,
        active: &[MomentumActivePoolEntry],
    ) -> bool;
    fn apply_momentum_removed_entries(
        &self,
        desired: &mut DesiredExplicitSet,
        chunk: &[MomentumRemovedPoolEntry],
    ) -> bool;
    fn apply_momentum_active_entries(
        &self,
        desired: &mut DesiredExplicitSet,
        chunk: &[MomentumActivePoolEntry],
    ) -> bool;
    fn apply_arb_track_requests_update(
        &self,
        desired: &mut DesiredExplicitSet,
        update: &ArbTrackRequestsUpdate,
    ) -> bool;
    fn apply_arb_snapshot_reconcile(
        &self,
        desired: &mut DesiredExplicitSet,
        active: &[ArbTrackActiveEntry],
    ) -> bool;
    fn apply_arb_removed_entries(
        &self,
        desired: &mut DesiredExplicitSet,
        chunk: &[ArbTrackRemovedEntry],
    ) -> bool;
    fn apply_arb_active_entries(
        &self,
        desired: &mut DesiredExplicitSet,
        chunk: &[ArbTrackActiveEntry],
    ) -> bool;
    fn track_mint_for_geyser_metadata(
        &self,
        desired: &mut DesiredExplicitSet,
        mint: Pubkey,
        pin: Option<TrackPinReason>,
    ) -> bool;
    fn sync_wallet_explicit_demand(
        &self,
        desired: &mut DesiredExplicitSet,
        demand: HashSet<Pubkey>,
    ) -> bool;
    fn commit_register_pool_geyser_reserves(
        &self,
        desired: &mut DesiredExplicitSet,
        pool: Pubkey,
        pin: TrackPinReason,
    ) -> bool;
    fn commit_register_pool_vaults_from_account(
        &self,
        desired: &mut DesiredExplicitSet,
        pool: Pubkey,
    ) -> bool;
    fn commit_register_geyser_reserves_after_trade(
        &self,
        desired: &mut DesiredExplicitSet,
        pool: Pubkey,
    ) -> bool;
    fn commit_refresh_dlmm_bin_window(
        &self,
        desired: &mut DesiredExplicitSet,
        pool: Pubkey,
        new_active_id: i32,
    ) -> bool;
    fn refresh_geyser_pins_gauge(&self);
    fn hot_pool_registry_pair_count(&self) -> usize;
    fn hot_pool_registry_arb_pool_count(&self) -> usize;
    fn refresh_hot_pool_registry_gauges(&self);
    fn snapshot_explicit_subscription_pubkeys(&self) -> HashSet<Pubkey>;
    fn pending_geyser_evict(&self) -> bool;
    fn clear_pending_geyser_evict(&self);
    fn continue_geyser_evict_with_deadline(
        &self,
        deadline: Instant,
        desired: &DesiredExplicitSet,
    ) -> bool;
    fn sync_geyser_tracked_accounts_batched_flush_with_deadline(
        &self,
        deadline: Instant,
        desired: &DesiredExplicitSet,
    ) -> bool;
    fn release_geyser_sync_flush_slot(&self);
    fn refresh_tracked_membership_snapshot(&self);
    fn explicit_owner_groups_for_convergence(
        &self,
    ) -> Vec<(super::ConsumerId, super::OwnerKey, HashSet<Pubkey>)>;
    fn build_explicit_set_snapshot(&self, desired: &DesiredExplicitSet) -> ExplicitSetSnapshot;
    fn apply_explicit_set_snapshot(
        &self,
        desired: &mut DesiredExplicitSet,
        snapshot: &ExplicitSetSnapshot,
    ) -> usize;
    fn converge_explicit_admission(&self, desired: &mut DesiredExplicitSet);
    fn prune_tracked_maps_to_desired(&self, desired: &DesiredExplicitSet);
    fn refresh_explicit_admission_metrics(&self, desired: &DesiredExplicitSet);
    fn publish_admitted_explicit_physical(&self, desired: &DesiredExplicitSet);
    fn last_synced_explicit_pubkeys_write(
        &self,
    ) -> parking_lot::RwLockWriteGuard<'_, HashSet<Pubkey>>;
    fn invalidate_explicit_admission_convergence(&self);
    fn take_explicit_admission_invalidate(&self) -> bool;
    fn geyser_explicit_readiness_ok(&self) -> bool;
}

#[derive(Clone)]
pub struct TrackWorkerSender {
    tx: std_mpsc::SyncSender<TrackWorkerCommand>,
    queue_depth: Arc<AtomicUsize>,
    queue_capacity: usize,
}

pub fn track_worker_try_enqueue(sender: &TrackWorkerSender, job: TrackWorkerCommand) -> bool {
    if sender.queue_depth.load(Ordering::Relaxed) >= sender.queue_capacity {
        inc_market_data_geyser_tracking_enqueue_dropped_total();
        return false;
    }
    if sender.tx.try_send(job).is_ok() {
        let depth = sender.queue_depth.fetch_add(1, Ordering::Relaxed) + 1;
        set_market_data_track_worker_queue_depth(depth);
        true
    } else {
        inc_market_data_geyser_tracking_enqueue_dropped_total();
        false
    }
}

fn track_worker_dec_queue_depth(queue_depth: &AtomicUsize) {
    let new_depth = queue_depth
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
            cur.checked_sub(1)
        })
        .unwrap_or(0);
    set_market_data_track_worker_queue_depth(new_depth);
}

/// Phase-2b: sync momentum apply on `md-track-worker` thread (no Tokio yield).
pub fn apply_momentum_active_pools_on_track_worker<C: TrackWorkerContext>(
    ctx: &Arc<C>,
    desired: &mut DesiredExplicitSet,
    update: MomentumActivePoolsUpdate,
) -> bool {
    let item_count = update.active.len() + update.removed.len();
    if item_count <= MARKET_DATA_MOMENTUM_APPLY_CHUNK_THRESHOLD {
        return ctx.apply_momentum_active_pools_update(desired, &update);
    }
    record_market_data_momentum_active_pool_messages_total();
    let mut batch_dirty = false;
    if update.full_active_snapshot {
        batch_dirty |= ctx.apply_momentum_snapshot_reconcile(desired, &update.active);
        std::thread::yield_now();
    }
    for chunk in update.removed.chunks(MARKET_DATA_MOMENTUM_APPLY_CHUNK_SIZE) {
        batch_dirty |= ctx.apply_momentum_removed_entries(desired, chunk);
        std::thread::yield_now();
    }
    for chunk in update.active.chunks(MARKET_DATA_MOMENTUM_APPLY_CHUNK_SIZE) {
        batch_dirty |= ctx.apply_momentum_active_entries(desired, chunk);
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
    desired: &mut DesiredExplicitSet,
    update: ArbTrackRequestsUpdate,
) -> bool {
    let item_count = update.active.len() + update.removed.len();
    if item_count <= MARKET_DATA_ARB_APPLY_CHUNK_THRESHOLD {
        return ctx.apply_arb_track_requests_update(desired, &update);
    }
    record_market_data_arb_track_requests_messages_total();
    let mut batch_dirty = false;
    if update.reconcile {
        batch_dirty |= ctx.apply_arb_snapshot_reconcile(desired, &update.active);
        std::thread::yield_now();
    }
    for chunk in update.removed.chunks(MARKET_DATA_ARB_APPLY_CHUNK_SIZE) {
        batch_dirty |= ctx.apply_arb_removed_entries(desired, chunk);
        std::thread::yield_now();
    }
    for chunk in update.active.chunks(MARKET_DATA_ARB_APPLY_CHUNK_SIZE) {
        batch_dirty |= ctx.apply_arb_active_entries(desired, chunk);
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
    desired: &mut DesiredExplicitSet,
    job: TrackWorkerCommand,
) -> bool {
    match job {
        TrackWorkerCommand::ApplyMomentumActivePools(update) => {
            apply_momentum_active_pools_on_track_worker(ctx, desired, update)
        }
        TrackWorkerCommand::ApplyArbTrackRequests(update) => {
            apply_arb_track_requests_on_track_worker(ctx, desired, update)
        }
        TrackWorkerCommand::ApplyWalletPin { mint } => {
            ctx.track_mint_for_geyser_metadata(desired, mint, Some(TrackPinReason::Wallet))
        }
        TrackWorkerCommand::TrackMint { mint, pin } => {
            ctx.track_mint_for_geyser_metadata(desired, mint, pin)
        }
        TrackWorkerCommand::SyncWalletExplicitDemand { demand } => {
            ctx.sync_wallet_explicit_demand(desired, demand)
        }
        TrackWorkerCommand::RegisterPoolGeyserReserves { pool, pin } => {
            ctx.commit_register_pool_geyser_reserves(desired, pool, pin)
        }
        TrackWorkerCommand::RegisterPoolVaultsFromAccount { pool } => {
            ctx.commit_register_pool_vaults_from_account(desired, pool)
        }
        TrackWorkerCommand::RegisterGeyserReservesAfterTrade { pool } => {
            ctx.commit_register_geyser_reserves_after_trade(desired, pool)
        }
        TrackWorkerCommand::RefreshDlmmBinWindow {
            pool,
            new_active_id,
        } => ctx.commit_refresh_dlmm_bin_window(desired, pool, new_active_id),
        TrackWorkerCommand::ScheduleGeyserSyncAfterConfigChange => {
            ctx.invalidate_explicit_admission_convergence();
            true
        }
        TrackWorkerCommand::RestoreExplicitSnapshot(snapshot) => {
            ctx.apply_explicit_set_snapshot(desired, &snapshot) > 0
        }
        TrackWorkerCommand::ScheduleGeyserPush
        | TrackWorkerCommand::ScheduleGeyserPushDebounced
        | TrackWorkerCommand::ContinueGeyserEvict => false,
    }
}

fn track_worker_apply_invalidation<C: TrackWorkerContext>(
    ctx: &Arc<C>,
    desired: &mut DesiredExplicitSet,
    admission_converged: &mut bool,
) {
    if ctx.take_explicit_admission_invalidate() {
        *admission_converged = false;
        desired.set_max_explicit_pubkeys(ctx.max_tracked_accounts());
    }
}

fn track_worker_loop<C: TrackWorkerContext + 'static>(
    ctx: Arc<C>,
    rx: std_mpsc::Receiver<TrackWorkerCommand>,
    queue_depth: Arc<AtomicUsize>,
    track_worker: TrackWorkerSender,
) {
    let mut desired = DesiredExplicitSet::new(ctx.max_tracked_accounts());
    let mut coalesce_deadline: Option<Instant> = None;
    let mut push_before_keys: Option<HashSet<Pubkey>> = None;
    let mut pending_continue_evict = false;
    let mut pending_release_flush_slot = false;
    let mut admission_converged = false;
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
                track_worker_dec_queue_depth(&queue_depth);
                if coalesce_deadline.is_none() {
                    push_before_keys = Some(ctx.snapshot_explicit_subscription_pubkeys());
                    coalesce_deadline = Some(
                        Instant::now()
                            + Duration::from_millis(MARKET_DATA_TRACK_WORKER_COALESCE_MS),
                    );
                }
                if matches!(job, TrackWorkerCommand::ContinueGeyserEvict) {
                    pending_continue_evict = true;
                }
                if matches!(job, TrackWorkerCommand::ScheduleGeyserPushDebounced) {
                    pending_release_flush_slot = true;
                }
                let cmd = match job {
                    TrackWorkerCommand::ScheduleGeyserPushDebounced => {
                        TrackWorkerCommand::ScheduleGeyserPush
                    }
                    other => other,
                };
                let _ = track_worker_process_command(&ctx, &mut desired, cmd);
                track_worker_apply_invalidation(&ctx, &mut desired, &mut admission_converged);
                while let Ok(more) = rx.try_recv() {
                    track_worker_dec_queue_depth(&queue_depth);
                    if matches!(more, TrackWorkerCommand::ContinueGeyserEvict) {
                        pending_continue_evict = true;
                    }
                    if matches!(more, TrackWorkerCommand::ScheduleGeyserPushDebounced) {
                        pending_release_flush_slot = true;
                    }
                    let cmd = match more {
                        TrackWorkerCommand::ScheduleGeyserPushDebounced => {
                            TrackWorkerCommand::ScheduleGeyserPush
                        }
                        other => other,
                    };
                    let _ = track_worker_process_command(&ctx, &mut desired, cmd);
                }
                track_worker_apply_invalidation(&ctx, &mut desired, &mut admission_converged);
            }
            Err(std_mpsc::RecvTimeoutError::Timeout) => {}
            Err(std_mpsc::RecvTimeoutError::Disconnected) => break,
        }

        let should_push =
            push_before_keys.is_some() && coalesce_deadline.is_some_and(|d| Instant::now() >= d);
        if should_push {
            if !ctx.geyser_explicit_readiness_ok() {
                push_before_keys = None;
                coalesce_deadline = None;
                pending_continue_evict = false;
                pending_release_flush_slot = false;
                continue;
            }
            let before = push_before_keys.take().unwrap_or_default();
            let continue_evict = pending_continue_evict;
            let release_flush_slot = pending_release_flush_slot;
            pending_continue_evict = false;
            pending_release_flush_slot = false;
            coalesce_deadline = None;
            if !track_worker_execute_coalesced_push(
                &ctx,
                &mut desired,
                before,
                continue_evict,
                release_flush_slot,
                &mut admission_converged,
            ) {
                track_worker_try_enqueue(&track_worker, TrackWorkerCommand::ContinueGeyserEvict);
            }
            inc_market_data_track_request_coalesce_batches_total();
            ctx.refresh_hot_pool_registry_gauges();
            if last_snapshot_write.elapsed()
                >= Duration::from_secs(MARKET_DATA_EXPLICIT_SET_SNAPSHOT_INTERVAL_SECS)
            {
                last_snapshot_write = Instant::now();
                try_write_explicit_set_snapshot_from_ctx(ctx.as_ref(), &desired);
            }
        }
    }
}

fn try_write_explicit_set_snapshot_from_ctx<C: TrackWorkerContext>(
    ctx: &C,
    desired: &DesiredExplicitSet,
) {
    let path = explicit_set_snapshot_path();
    let snapshot = ctx.build_explicit_set_snapshot(desired);
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
pub fn flush_explicit_set_snapshot<C: TrackWorkerContext>(ctx: &C, desired: &DesiredExplicitSet) {
    try_write_explicit_set_snapshot_from_ctx(ctx, desired);
}

pub fn spawn_track_worker<C: TrackWorkerContext + 'static>(ctx: Arc<C>) -> TrackWorkerSender {
    let queue_capacity = MARKET_DATA_TRACK_WORKER_QUEUE_CAP;
    let (tx, rx) = std_mpsc::sync_channel::<TrackWorkerCommand>(queue_capacity);
    let queue_depth = Arc::new(AtomicUsize::new(0));
    let depth_worker = Arc::clone(&queue_depth);
    let ctx_worker = Arc::clone(&ctx);
    let sender = TrackWorkerSender {
        tx: tx.clone(),
        queue_depth: Arc::clone(&queue_depth),
        queue_capacity,
    };
    let track_worker = sender.clone();
    let _join: JoinHandle<()> = std::thread::Builder::new()
        .name("md-track-worker".into())
        .spawn(move || track_worker_loop(ctx_worker, rx, depth_worker, track_worker))
        .expect("spawn md-track-worker thread");
    sender
}

/// Test helper: enqueue handle that drains jobs without processing (md-state unit tests).
pub fn spawn_noop_track_worker_sender(capacity: usize) -> TrackWorkerSender {
    let (tx, rx) = std_mpsc::sync_channel::<TrackWorkerCommand>(capacity);
    std::thread::spawn(move || while let Ok(_job) = rx.recv() {});
    TrackWorkerSender {
        tx,
        queue_depth: Arc::new(AtomicUsize::new(0)),
        queue_capacity: capacity,
    }
}

/// Test helper: inline command processor without Geyser push coalesce loop.
pub fn spawn_inline_track_worker_sender<C: TrackWorkerContext + 'static>(
    ctx: Arc<C>,
    capacity: usize,
) -> TrackWorkerSender {
    let (tx, rx) = std_mpsc::sync_channel::<TrackWorkerCommand>(capacity);
    let ctx_worker = Arc::clone(&ctx);
    std::thread::spawn(move || {
        let mut desired = DesiredExplicitSet::new(ctx_worker.max_tracked_accounts());
        while let Ok(job) = rx.recv() {
            let _ = track_worker_process_command(&ctx_worker, &mut desired, job);
        }
    });
    TrackWorkerSender {
        tx,
        queue_depth: Arc::new(AtomicUsize::new(0)),
        queue_capacity: capacity,
    }
}
