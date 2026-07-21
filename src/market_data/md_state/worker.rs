//! Phase 5e: `md-state` OS thread — bounded enqueue, burst coalesce, deferred Geyser sync.

use super::command::MdStateCommand;
use super::host::MdStateContext;
use crate::market_data::track::{
    explicit_subscription_has_new_keys, merge_track_mint_pin, track_worker_try_enqueue,
    TrackPinReason, TrackWorkerCommand, TrackWorkerSender,
};
use crate::metrics::{
    inc_market_data_geyser_sync_skipped_no_delta_total,
    inc_market_data_geyser_tracking_enqueue_dropped_total,
    inc_market_data_geyser_tracking_jobs_processed_total,
    inc_market_data_md_state_bursts_completed_total,
    inc_market_data_md_state_track_mint_coalesce_batches_out,
    inc_market_data_md_state_track_mint_coalesce_messages_in,
    set_market_data_geyser_tracking_queue_depth, set_market_data_md_state_burst_in_progress,
    set_market_data_md_state_deferred_jobs_len, set_market_data_md_state_queue_depth,
};
use solana_sdk::pubkey::Pubkey;
use std::collections::{HashMap, HashSet, VecDeque};
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
/// Bounded retries for critical md-state enqueues that must not be silently dropped (PR4b).
pub const MARKET_DATA_MD_STATE_CRITICAL_ENQUEUE_ATTEMPTS: usize = 32;

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
    let mut track_mints: HashMap<Pubkey, Option<TrackPinReason>> = HashMap::new();
    let mut track_mint_messages_in = 0u64;
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
            MdStateCommand::TrackMint { mint, pin } => {
                track_mint_messages_in += 1;
                track_mints
                    .entry(mint)
                    .and_modify(|existing| *existing = merge_track_mint_pin(*existing, pin))
                    .or_insert(pin);
            }
            MdStateCommand::TrackMints { entries } => {
                track_mint_messages_in += entries.len() as u64;
                for (mint, pin) in entries {
                    track_mints
                        .entry(mint)
                        .and_modify(|existing| *existing = merge_track_mint_pin(*existing, pin))
                        .or_insert(pin);
                }
            }
            other => out.push(other),
        }
    }
    if track_mint_messages_in > 0 {
        inc_market_data_md_state_track_mint_coalesce_messages_in(track_mint_messages_in);
        if !track_mints.is_empty() {
            inc_market_data_md_state_track_mint_coalesce_batches_out();
            out.push(MdStateCommand::TrackMints {
                entries: track_mints.into_iter().collect(),
            });
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

/// Split wallet/strategy pins out before tracker batch enqueue (I-MD-5 wallet demand preserved).
fn enqueue_track_mints_to_worker(
    track_worker: &TrackWorkerSender,
    entries: Vec<(Pubkey, Option<TrackPinReason>)>,
) {
    if entries.is_empty() {
        return;
    }
    let mut tracker_entries = Vec::with_capacity(entries.len());
    for (mint, pin) in entries {
        match pin {
            Some(TrackPinReason::Wallet) => {
                track_worker_try_enqueue(track_worker, TrackWorkerCommand::ApplyWalletPin { mint });
            }
            None => tracker_entries.push((mint, pin)),
            Some(other) => {
                track_worker_try_enqueue(
                    track_worker,
                    TrackWorkerCommand::TrackMint {
                        mint,
                        pin: Some(other),
                    },
                );
            }
        }
    }
    match tracker_entries.len() {
        0 => {}
        1 => {
            let (mint, pin) = tracker_entries[0];
            track_worker_try_enqueue(track_worker, TrackWorkerCommand::TrackMint { mint, pin });
        }
        _ => {
            track_worker_try_enqueue(
                track_worker,
                TrackWorkerCommand::TrackMints {
                    entries: tracker_entries,
                },
            );
        }
    }
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
        MdStateCommand::TrackMints { entries } => {
            enqueue_track_mints_to_worker(track_worker, entries);
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

/// PR4b: enqueue wallet-pin withdraw with bounded yield-retry (I-MD-8 demand must not be lost).
pub fn md_state_try_enqueue_withdraw_wallet_mint(sender: &MdStateSender, mint: Pubkey) -> bool {
    for attempt in 0..MARKET_DATA_MD_STATE_CRITICAL_ENQUEUE_ATTEMPTS {
        if md_state_try_enqueue(sender, MdStateCommand::WithdrawWalletMint { mint }) {
            return true;
        }
        if attempt + 1 < MARKET_DATA_MD_STATE_CRITICAL_ENQUEUE_ATTEMPTS {
            std::thread::yield_now();
        }
    }
    false
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
        let before_keys = ctx.snapshot_explicit_demand_pubkeys();
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
        let after_keys = ctx.snapshot_explicit_demand_pubkeys();
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

#[cfg(test)]
mod tests {
    use super::md_state_coalesce_jobs;
    use crate::market_data::md_state::MdStateCommand;
    use crate::market_data::track::TrackPinReason;
    use solana_sdk::pubkey::Pubkey;

    #[test]
    fn scope_h_coalesce_dedupes_repeated_track_mint() {
        let mint = Pubkey::new_unique();
        let jobs: Vec<_> = (0..100)
            .map(|_| MdStateCommand::TrackMint { mint, pin: None })
            .collect();
        let out = md_state_coalesce_jobs(jobs);
        assert_eq!(out.len(), 1);
        let MdStateCommand::TrackMints { entries } = &out[0] else {
            panic!("expected TrackMints batch");
        };
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0], (mint, None));
    }

    #[test]
    fn scope_h_coalesce_track_mint_wallet_pin_wins() {
        let mint = Pubkey::new_unique();
        let jobs = vec![
            MdStateCommand::TrackMint { mint, pin: None },
            MdStateCommand::TrackMint {
                mint,
                pin: Some(TrackPinReason::Wallet),
            },
            MdStateCommand::TrackMint { mint, pin: None },
        ];
        let out = md_state_coalesce_jobs(jobs);
        assert_eq!(out.len(), 1);
        let MdStateCommand::TrackMints { entries } = &out[0] else {
            panic!("expected TrackMints batch");
        };
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0], (mint, Some(TrackPinReason::Wallet)));
    }
}
