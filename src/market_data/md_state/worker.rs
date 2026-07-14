//! Phase 5e: `md-state` OS thread — bounded enqueue, burst coalesce, deferred Geyser sync.

use super::command::MdStateCommand;
use super::host::MdStateContext;
use crate::market_data::track::{
    explicit_subscription_has_new_keys, track_worker_try_enqueue, TrackWorkerCommand,
    TrackWorkerSender,
};
use crate::metrics::{
    inc_market_data_geyser_sync_skipped_no_delta_total,
    inc_market_data_geyser_tracking_enqueue_dropped_total,
    inc_market_data_geyser_tracking_jobs_processed_total,
    inc_market_data_md_state_bursts_completed_total, set_market_data_geyser_tracking_queue_depth,
    set_market_data_md_state_burst_in_progress, set_market_data_md_state_deferred_jobs_len,
    set_market_data_md_state_queue_depth,
};
use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc as std_mpsc;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// PR169a / Phase-R-R2: bounded queue for single-writer Geyser tracking mutations.
pub const MARKET_DATA_GEYSER_TRACKING_QUEUE_CAP: usize = 8192;
/// Phase-R-R2: max jobs drained per `md-state` burst before one debounced Geyser sync.
pub const MARKET_DATA_MD_STATE_BURST_MAX: usize = 256;
/// PR235: wall time budget for job processing only (flush/evict uses separate budget).
pub const MARKET_DATA_MD_STATE_JOB_BUDGET_MS: u64 = 16;
/// PR235: minimum jobs completed per burst before time-budget defer (avoids re-defer loops).
pub const MARKET_DATA_MD_STATE_MIN_JOBS_PER_BURST: usize = 32;

/// Bounded enqueue handle for the `md-state` OS thread (non-Tokio).
#[derive(Clone)]
pub struct MdStateSender {
    pub tx: std_mpsc::SyncSender<MdStateCommand>,
    pub queue_depth: Arc<AtomicUsize>,
    pub queue_capacity: usize,
}

fn md_state_dec_queue_depth(queue_depth: &AtomicUsize) {
    let mut cur = queue_depth.load(Ordering::Relaxed);
    while cur > 0 {
        match queue_depth.compare_exchange_weak(cur, cur - 1, Ordering::Relaxed, Ordering::Relaxed)
        {
            Ok(_) => {
                set_market_data_geyser_tracking_queue_depth(cur - 1);
                set_market_data_md_state_queue_depth(cur - 1);
                return;
            }
            Err(actual) => cur = actual,
        }
    }
    set_market_data_geyser_tracking_queue_depth(0);
    set_market_data_md_state_queue_depth(0);
}

pub fn md_state_coalesce_jobs(jobs: Vec<MdStateCommand>) -> Vec<MdStateCommand> {
    let mut out = Vec::with_capacity(jobs.len());
    let mut lru_vaults = HashSet::new();
    let mut lru_bin_arrays = HashSet::new();
    let mut pending_flush = false;
    let mut pending_evict_continue = false;
    for job in jobs {
        match job {
            MdStateCommand::FlushGeyserSyncDebounced => {
                pending_flush = true;
            }
            MdStateCommand::ContinueGeyserEvict => {
                pending_evict_continue = true;
            }
            MdStateCommand::TouchVault(vault) => {
                lru_vaults.insert(vault);
            }
            MdStateCommand::TouchBinArray(pda) => {
                lru_bin_arrays.insert(pda);
            }
            MdStateCommand::TouchTrackedLruBatch { vaults, bin_arrays } => {
                lru_vaults.extend(vaults);
                lru_bin_arrays.extend(bin_arrays);
            }
            other => out.push(other),
        }
    }
    if !lru_vaults.is_empty() || !lru_bin_arrays.is_empty() {
        out.push(MdStateCommand::TouchTrackedLruBatch {
            vaults: lru_vaults.into_iter().collect(),
            bin_arrays: lru_bin_arrays.into_iter().collect(),
        });
    }
    if pending_flush {
        out.push(MdStateCommand::FlushGeyserSyncDebounced);
    }
    if pending_evict_continue {
        out.push(MdStateCommand::ContinueGeyserEvict);
    }
    out
}

pub fn md_state_process_job<C: MdStateContext>(
    ctx: &Arc<C>,
    job: MdStateCommand,
    track_worker: &TrackWorkerSender,
) -> bool {
    match job {
        MdStateCommand::TrackMint { mint, pin } => {
            track_worker_try_enqueue(track_worker, TrackWorkerCommand::TrackMint { mint, pin });
            false
        }
        MdStateCommand::TrackWalletMint { mint } => {
            track_worker_try_enqueue(track_worker, TrackWorkerCommand::ApplyWalletPin { mint });
            false
        }
        MdStateCommand::WithdrawWalletMint { mint } => {
            track_worker_try_enqueue(track_worker, TrackWorkerCommand::WithdrawWalletPin { mint });
            false
        }
        MdStateCommand::ScheduleGeyserSyncAfterConfigChange => {
            track_worker_try_enqueue(
                track_worker,
                TrackWorkerCommand::ScheduleGeyserSyncAfterConfigChange,
            );
            false
        }
        MdStateCommand::FlushGeyserSyncDebounced => {
            track_worker_try_enqueue(
                track_worker,
                TrackWorkerCommand::ScheduleGeyserPushDebounced,
            );
            false
        }
        MdStateCommand::ContinueGeyserEvict => {
            track_worker_try_enqueue(track_worker, TrackWorkerCommand::ContinueGeyserEvict);
            false
        }
        MdStateCommand::TouchVault(vault) => {
            ctx.touch_tracked_vault_pubkey(&vault);
            false
        }
        MdStateCommand::TouchBinArray(pda) => {
            ctx.touch_tracked_bin_array_pubkey(&pda);
            false
        }
        MdStateCommand::TouchTrackedLruBatch { vaults, bin_arrays } => {
            for vault in vaults {
                ctx.touch_tracked_vault_pubkey(&vault);
            }
            for pda in bin_arrays {
                ctx.touch_tracked_bin_array_pubkey(&pda);
            }
            false
        }
        MdStateCommand::TouchPool(pool) => {
            ctx.touch_tracked_pool_vaults_and_bins_if_tracked(pool);
            false
        }
    }
}

pub fn md_state_try_enqueue(sender: &MdStateSender, job: MdStateCommand) -> bool {
    if sender.queue_depth.load(Ordering::Relaxed) >= sender.queue_capacity {
        inc_market_data_geyser_tracking_enqueue_dropped_total();
        return false;
    }
    if sender.tx.try_send(job).is_ok() {
        let depth = sender.queue_depth.fetch_add(1, Ordering::Relaxed) + 1;
        set_market_data_geyser_tracking_queue_depth(depth);
        set_market_data_md_state_queue_depth(depth);
        true
    } else {
        inc_market_data_geyser_tracking_enqueue_dropped_total();
        false
    }
}

fn md_state_worker_loop<C: MdStateContext + 'static>(
    ctx: Arc<C>,
    rx: std_mpsc::Receiver<MdStateCommand>,
    queue_depth: Arc<AtomicUsize>,
    md_state: MdStateSender,
    track_worker: TrackWorkerSender,
) {
    let mut deferred_jobs: VecDeque<MdStateCommand> = VecDeque::new();
    loop {
        set_market_data_md_state_burst_in_progress(true);
        set_market_data_md_state_deferred_jobs_len(deferred_jobs.len());
        let mut jobs = Vec::new();
        while jobs.len() < MARKET_DATA_MD_STATE_BURST_MAX {
            if let Some(job) = deferred_jobs.pop_front() {
                jobs.push(job);
            } else if jobs.is_empty() {
                let Ok(first) = rx.recv() else {
                    set_market_data_md_state_burst_in_progress(false);
                    break;
                };
                md_state_dec_queue_depth(&queue_depth);
                inc_market_data_geyser_tracking_jobs_processed_total();
                jobs.push(first);
            } else {
                match rx.try_recv() {
                    Ok(job) => {
                        md_state_dec_queue_depth(&queue_depth);
                        inc_market_data_geyser_tracking_jobs_processed_total();
                        jobs.push(job);
                    }
                    Err(std_mpsc::TryRecvError::Empty) => break,
                    Err(std_mpsc::TryRecvError::Disconnected) => {
                        set_market_data_md_state_burst_in_progress(false);
                        break;
                    }
                }
            }
        }
        if jobs.is_empty() {
            set_market_data_md_state_burst_in_progress(false);
            set_market_data_md_state_deferred_jobs_len(deferred_jobs.len());
            continue;
        }
        let jobs = md_state_coalesce_jobs(jobs);
        let schedule_sync_after_config = jobs
            .iter()
            .any(|job| matches!(job, MdStateCommand::ScheduleGeyserSyncAfterConfigChange));
        let before_keys = ctx.snapshot_explicit_subscription_pubkeys();
        let job_deadline =
            Instant::now() + Duration::from_millis(MARKET_DATA_MD_STATE_JOB_BUDGET_MS);
        let mut deferred_this_burst = VecDeque::new();
        let mut jobs_completed = 0usize;
        for job in jobs {
            let at_budget = Instant::now() >= job_deadline;
            let below_min = jobs_completed < MARKET_DATA_MD_STATE_MIN_JOBS_PER_BURST;
            if at_budget && !below_min {
                deferred_this_burst.push_back(job);
                continue;
            }
            let _ = md_state_process_job(&ctx, job, &track_worker);
            jobs_completed += 1;
        }
        while let Some(job) = deferred_this_burst.pop_front() {
            deferred_jobs.push_back(job);
        }
        let after_keys = ctx.snapshot_explicit_subscription_pubkeys();
        if explicit_subscription_has_new_keys(&before_keys, &after_keys)
            || schedule_sync_after_config
        {
            C::schedule_geyser_sync_batch_debounced(&ctx, &md_state);
        } else if before_keys != after_keys {
            inc_market_data_geyser_sync_skipped_no_delta_total();
        }
        ctx.refresh_hot_pool_registry_gauges();
        ctx.refresh_tracked_membership_snapshot();
        if jobs_completed > 0 {
            inc_market_data_md_state_bursts_completed_total();
        }
        set_market_data_md_state_burst_in_progress(false);
        set_market_data_md_state_deferred_jobs_len(deferred_jobs.len());
    }
}

pub fn spawn_md_state_worker<C: MdStateContext + 'static>(
    ctx: Arc<C>,
    tokio_handle: tokio::runtime::Handle,
    track_worker: TrackWorkerSender,
) -> MdStateSender {
    ctx.set_ingest_tokio_handle(tokio_handle);
    let queue_capacity = MARKET_DATA_GEYSER_TRACKING_QUEUE_CAP;
    let (tx, rx) = std_mpsc::sync_channel::<MdStateCommand>(queue_capacity);
    let queue_depth = Arc::new(AtomicUsize::new(0));
    let depth_worker = Arc::clone(&queue_depth);
    let ctx_worker = Arc::clone(&ctx);
    let md_state_sender = MdStateSender {
        tx: tx.clone(),
        queue_depth: Arc::clone(&queue_depth),
        queue_capacity,
    };
    let md_state_worker = md_state_sender.clone();
    let track_worker_worker = track_worker.clone();
    let _join: JoinHandle<()> = std::thread::Builder::new()
        .name("md-state".into())
        .spawn(move || {
            md_state_worker_loop(
                ctx_worker,
                rx,
                depth_worker,
                md_state_worker,
                track_worker_worker,
            )
        })
        .expect("spawn md-state thread");
    md_state_sender
}
