//! Phase 5a: `md-track-worker` OS thread — bounded enqueue, command processing, coalesced Geyser push.

use super::desired_set::DesiredExplicitSet;
use super::geyser_sync::track_worker_execute_coalesced_push;
use crate::metrics::{
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrackPinReason {
    Wallet,
    MomentumActive,
    ArbMultiDex,
}

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
    fn continue_geyser_evict_with_deadline(&self, deadline: Instant) -> bool;
    fn sync_geyser_tracked_accounts_batched_flush_with_deadline(&self, deadline: Instant) -> bool;
    fn release_geyser_sync_flush_slot(&self);
    fn refresh_tracked_membership_snapshot(&self);
    fn explicit_pubkey_rows_for_desired_set(
        &self,
    ) -> Vec<(Pubkey, super::ConsumerId, Option<Pubkey>)>;
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
        pin: Option<TrackPinReason>,
    },
    ScheduleGeyserSyncAfterConfigChange,
    /// Coalesced explicit Geyser push (md-state burst / trade path).
    ScheduleGeyserPush,
    /// Debounced push after `try_acquire_geyser_sync_flush_slot` (rate-limited TX debounce thread).
    ScheduleGeyserPushDebounced,
    ContinueGeyserEvict,
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
        TrackWorkerCommand::ScheduleGeyserSyncAfterConfigChange => true,
        TrackWorkerCommand::ScheduleGeyserPush
        | TrackWorkerCommand::ScheduleGeyserPushDebounced
        | TrackWorkerCommand::ContinueGeyserEvict => false,
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
                let _ = track_worker_process_command(&ctx, cmd);
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
                    let _ = track_worker_process_command(&ctx, cmd);
                }
            }
            Err(std_mpsc::RecvTimeoutError::Timeout) => {}
            Err(std_mpsc::RecvTimeoutError::Disconnected) => break,
        }

        let should_push =
            push_before_keys.is_some() && coalesce_deadline.is_some_and(|d| Instant::now() >= d);
        if should_push {
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
            ) {
                track_worker_try_enqueue(&track_worker, TrackWorkerCommand::ContinueGeyserEvict);
            }
            inc_market_data_track_request_coalesce_batches_total();
            ctx.refresh_hot_pool_registry_gauges();
        }
    }
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
        while let Ok(job) = rx.recv() {
            let _ = track_worker_process_command(&ctx_worker, job);
        }
    });
    TrackWorkerSender {
        tx,
        queue_depth: Arc::new(AtomicUsize::new(0)),
        queue_capacity: capacity,
    }
}
