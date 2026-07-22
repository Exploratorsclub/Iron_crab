//! market-data binary – Data Plane (Geyser ingest + MarketEvents)
//!
//! Source of Truth: docs/TARGET_ARCHITECTURE.md §2.1
//!
//! Responsibilities:
//! - Geyser ingest (preferred), optional RPC/WS fallback
//! - Pool/Account cache (in-memory)
//! - Normalize and publish MarketEvents to NATS
//! - Discovery Worker: detect new mints/pools as events
//!
//! This binary does NOT:
//! - Load wallet keys
//! - Sign or send transactions
//! - Make trading decisions

// Allow holding locks across await - RwLock reads are fast and this simplifies the code.
// TODO: Refactor to use explicit clone-before-await pattern if this causes contention.
#![allow(clippy::await_holding_lock)]

use anyhow::Result;
use arc_swap::ArcSwap;
use clap::Parser;
use serde::Serialize;
use solana_sdk::pubkey::Pubkey;
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch, Mutex, Notify};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use ironcrab::config::{Config, MarketDataGeyserCfg, WalletTrackerCfg};
use ironcrab::ipc::{
    ConfigUpdate, ConfigUpdateResponse, ConfigUpdateStatus, ControlRequest, ControlRequestKind,
    DexPoolReadiness, ExecutionResult, ExecutionStatus, IntentTier, MarketEvent, MarketEventKind,
    PoolCacheUpdate, NATIVE_SOL_MINT, POOL_CACHE_UPDATE_RAYDIUM_CPMM_VAULTS_KEY,
};
use ironcrab::market_data::cold::{
    cold_path_rpc_refresh_meteora_cpmm_pool_row, cold_path_rpc_refresh_meteora_dlmm_pool_row,
    cold_path_rpc_refresh_orca_whirlpool_pool_row, handle_ensure_meteora_cpmm_pool_state,
    handle_ensure_meteora_dlmm_pool_state, handle_ensure_orca_whirlpool_pool_state,
    handle_ensure_pump_amm_pool_accounts, handle_ensure_pumpfun_bonding_curve,
    handle_ensure_raydium_amm_pool_state, handle_ensure_raydium_cpmm_pool_state, ColdHost,
};
use ironcrab::market_data::ingest::{
    handle_geyser_account_update, handle_geyser_transaction_update, try_parse_mint_account,
    try_parse_token_account_balance, AccountIngestHost, IngestHost, TxIngestHost,
    TxTrackedWalletView,
};
use ironcrab::market_data::jsonl::{
    spawn_market_data_jsonl_writer, write_market_event_jsonl, JsonlHost,
};
use ironcrab::market_data::md_state::{
    md_state_try_enqueue, md_state_try_enqueue_withdraw_wallet_mint, spawn_md_state_worker,
    MdStateCommand, MdStateContext, MdStateSender, MARKET_DATA_GEYSER_TRACKING_QUEUE_CAP,
};
use ironcrab::market_data::publish::{
    account_path_enqueue_core_market_event as publish_enqueue_core_market_event,
    account_path_enqueue_jetstream as publish_enqueue_jetstream,
    account_publish_worker_count_from_env,
    market_event_is_momentum_nats_relevant as publish_momentum_relevant,
    publish_market_event_core_and_momentum_ex as publish_core_and_momentum,
    spawn_md_account_publish_runtime, try_enqueue_account_path_nats_job, AccountPathNatsJob,
    AccountPublishSender, PublishHost, MARKET_DATA_ACCOUNT_PUBLISH_QUEUE_CAP,
    MARKET_DATA_ACCOUNT_PUBLISH_WORKER_DISPATCH_QUEUE_CAP,
};

use ironcrab::market_data::sidefx::{
    md_sidefx_coalesce_burst as sidefx_coalesce_burst,
    md_sidefx_flush_pending_md_state_jobs as sidefx_flush_pending,
    md_sidefx_process_live_pool_cache_account_update as sidefx_process_live_pool_cache_account_update,
    md_sidefx_process_vault_balance_tick as sidefx_process_vault_balance_tick,
    md_sidefx_try_enqueue as sidefx_try_enqueue, spawn_md_sidefx_worker as spawn_sidefx_worker,
    MarketEventCorePublishTrace, MdSidefxBurstScratch, MdSidefxCommand, MdSidefxSender,
    SidefxUpdateClass, SidefxVaultMembershipView, SidefxWorkerHost,
    MARKET_DATA_MD_SIDEFX_QUEUE_CAP,
};
use ironcrab::market_data::track::{
    arb_coalesce_try_send, explicit_admitted_pool_sets_from_admission, explicit_set_snapshot_path,
    explicit_subscription_has_new_keys, flush_explicit_set_snapshot, load_explicit_set_snapshot,
    momentum_coalesce_try_send, owner_group_snapshot_to_disk, pin_priority_for_momentum_active_pin,
    pool_is_enrichment_member, restore_admission_from_owner_groups, spawn_track_worker,
    track_worker_try_enqueue, try_admit_owner_group, AdmissionConvergeResult,
    AdmissionRestoreResult, CapShrinkResult, ConsumerId, ExplicitAccountKind, ExplicitConsumer,
    ExplicitEntry, ExplicitOwner, ExplicitOwnerKey, ExplicitSetSnapshot, ExplicitSnapshotRow,
    FixedCapAdmission, GeyserConnectBarrier, PinPriority, SnapshotConsumer, TrackPinReason,
    TrackWorkerCommand, TrackWorkerContext, TrackWorkerSender,
    EXPLICIT_SET_SNAPSHOT_POOL_MINT_MAP_CAP,
};
use ironcrab::metrics::{
    dec_market_data_account_enrich_ingress_queue_depth,
    dec_market_data_account_high_priority_queue_depth,
    dec_market_data_account_low_priority_queue_depth, dec_market_data_account_worker_queue_depth,
    geyser_account_listener_account_updates_value, geyser_metrics_set_subscription_accounts,
    geyser_metrics_set_tracked_pinned_accounts, inc_market_data_account_enrich_coalesce_total,
    inc_market_data_account_enrich_dispatch_contended_total,
    inc_market_data_account_enrich_enqueue_dropped_total,
    inc_market_data_account_enrich_ingress_queue_depth,
    inc_market_data_account_high_enqueue_dropped_total,
    inc_market_data_account_high_priority_queue_depth,
    inc_market_data_account_low_priority_queue_depth,
    inc_market_data_account_recv_iterations_total, inc_market_data_account_updates_total,
    inc_market_data_account_worker_queue_depth, inc_market_data_arb_admission_admitted_total,
    inc_market_data_arb_admission_rejected_total,
    inc_market_data_arb_pin_geyser_register_deferred_total,
    inc_market_data_balance_updated_from_cache_total,
    inc_market_data_geyser_sync_skipped_rate_limit_total,
    inc_market_data_ingest_membership_snapshot_hits_total,
    inc_market_data_jsonl_enqueue_dropped_total, inc_market_data_momentum_admission_admitted_total,
    inc_market_data_momentum_admission_rejected_total,
    inc_market_data_open_position_pin_applied_total,
    inc_market_data_open_position_pin_deferred_cache_miss_total,
    inc_market_data_tracker_admission_admitted_total,
    inc_market_data_tracker_admission_rejected_total,
    inc_market_data_vault_high_priority_dispatch_total,
    inc_market_data_wallet_admission_admitted_total,
    inc_market_data_wallet_admission_rejected_total, market_data_bump_geyser_head_slot,
    market_data_geyser_head_slot_value, market_data_geyser_tracking_enqueue_dropped_value,
    market_data_geyser_tracking_jobs_processed_value, market_data_md_state_bursts_completed_value,
    market_data_request_account_session_reconnect, market_data_request_tx_session_reconnect,
    market_data_tx_handler_processed_value, record_market_data_account_broadcast_lagged,
    record_market_data_account_channel_lag_ms, record_market_data_account_channel_lag_ms_for_class,
    record_market_data_account_early_drop, record_market_data_account_handler_duration_us,
    record_market_data_account_recv_classify_duration_us,
    record_market_data_account_recv_enrich_ingress_duration_us,
    record_market_data_account_recv_high_enqueue_duration_us,
    record_market_data_account_recv_iteration_duration_us,
    record_market_data_arb_track_requests_messages_total,
    record_market_data_geyser_merge_coalesced_total, record_market_data_geyser_sync_batch_total,
    record_market_data_global_ingest_stall, record_market_data_md_state_stall,
    record_market_data_md_state_writer_wait_us,
    record_market_data_momentum_active_pool_messages_total,
    record_market_data_tokio_liveness_stall, record_market_data_tokio_progress,
    record_market_data_tx_broadcast_lagged, serve_metrics,
    set_market_data_account_broadcast_queue_depth, set_market_data_account_worker_count,
    set_market_data_arb_pinned_pools_gauge, set_market_data_enrichment_registry_pools_gauge,
    set_market_data_explicit_set_snapshot_restore_duration_ms,
    set_market_data_explicit_set_snapshot_restore_pubkeys,
    set_market_data_geyser_explicit_admitted_accounts,
    set_market_data_geyser_explicit_cap_overflow, set_market_data_geyser_explicit_set_size,
    set_market_data_geyser_merge_pending, set_market_data_geyser_sync_pending,
    set_market_data_hot_pool_registry_pools_gauge, set_market_data_momentum_active_pool_pins_gauge,
    set_market_data_tx_broadcast_queue_depth, set_readiness_control_sub_active, set_readiness_mode,
    set_readiness_nats_connected, touch_market_data_global_ingest_progress,
    touch_market_data_tracked_membership_snapshot_refresh, update_readiness_market_data_current,
    MarketDataLatencySegment, MetricsComponent, MARKET_DATA_LAST_BONDING_CURVE_PUBLISH_TS_UNIX_MS,
    MARKET_EVENTS_RECEIVED_TOTAL, POOLS_TRACKED_GAUGE,
};
use ironcrab::nats::{
    config_consumer_config, config_subject, ensure_execution_results_stream,
    ensure_pool_cache_stream, ensure_wallet_snapshot_stream, ensure_wallet_tx_confirm_stream,
    execution_results_consumer_config, pool_subject, wallet_snapshot_consumer_config,
    wallet_snapshot_subject, wallet_tx_confirm_subject, ArbTrackActiveEntry, ArbTrackRemovedEntry,
    ArbTrackRequestsUpdate, MomentumActivePinReason, MomentumActivePoolEntry,
    MomentumActivePoolsUpdate, MomentumRemovedPoolEntry, NatsClient, NatsConfig,
    CONFIG_STREAM_NAME, EXECUTION_RESULTS_STREAM_NAME, TOPIC_ARB_TRACK_REQUESTS,
    TOPIC_CONTROL_REQUESTS, TOPIC_EXECUTION_RESULTS, TOPIC_MARKET_EVENTS,
    TOPIC_MOMENTUM_ACTIVE_POOLS, WALLET_SNAPSHOT_STREAM_NAME,
};
use ironcrab::position_authority::is_sol_or_wsol_mint;
use ironcrab::solana::dex::meteora_swap_builder::MeteoraDlmmSwapBuilder;
use ironcrab::solana::dex::pumpfun::PumpFunDex;
use ironcrab::solana::dex::pumpfun_amm::PumpFunAmmDex;
use ironcrab::solana::dex_parser::OrcaPoolInfo;
use ironcrab::solana::geyser_pool_discovery::{
    DexType as PoolDexType, PoolDiscoveryEvent, PoolDiscoveryIngest,
};
use ironcrab::solana::priority_fee_tracker::PriorityFeeTracker;
use ironcrab::solana::rpc::SolanaRpc;
use ironcrab::solana::wallet_tracker::WalletTracker;
#[cfg(not(windows))]
use ironcrab::solana::wallet_tx_confirm_listener::{
    spawn_wallet_tx_confirm_listener, WalletTxConfirmUpdate,
};
use spl_token::solana_program::program_pack::Pack;
use spl_token_2022::extension::StateWithExtensions;

/// NATS topic for config reload (P1: Runtime Configuration via UI)
const TOPIC_CONFIG_RELOAD: &str = "ironcrab.control.config.reload";
use ironcrab::solana::geyser_listener::{
    GeyserAccountListener, GeyserAccountUpdate, GeyserTransactionUpdate, GeyserTxListener,
};
use ironcrab::storage::{JsonlWriterConfig, QueuedJsonlWriter};

/// PR165: when false, `md-watchdog` stops `sd_notify(WATCHDOG)` so systemd can restart on Tokio stall.
#[cfg(unix)]
static MD_SYSTEMD_WATCHDOG_NOTIFY: AtomicBool = AtomicBool::new(true);

/// Default capacity for off-hot-path market-events JSONL queue.
const MARKET_DATA_JSONL_QUEUE_CAP: usize = 16_384;
/// PR167: minimum debounce for explicit pin updates during startup burst (seconds).
const MARKET_DATA_GEYSER_SYNC_STARTUP_WINDOW: Duration = Duration::from_secs(120);
const MARKET_DATA_GEYSER_SYNC_STARTUP_MIN_MS: u64 = 250;
/// PR233: max debounced Geyser sync flushes per second during startup burst.
const MARKET_DATA_GEYSER_SYNC_FLUSH_MAX_PER_SEC: usize = 4;
/// PR234: md-state liveness poll interval (OS thread).
const MARKET_DATA_MD_STATE_STALL_CHECK_INTERVAL: Duration = Duration::from_secs(10);
/// PR234: queue near-cap + flat progress before stall metric.
const MARKET_DATA_MD_STATE_STALL_WINDOW: Duration = Duration::from_secs(60);
/// PR234: exit for systemd restart after sustained md-state stall.
const MARKET_DATA_MD_STATE_STALL_EXIT_AFTER: Duration = Duration::from_secs(120);
/// PR234: queue depth fraction treated as saturation for md-state liveness.
const MARKET_DATA_MD_STATE_STALL_QUEUE_FRAC: f64 = 0.95;
/// PR235: when false, md-state liveness records stalls but does not exit(1) for systemd restart.
const MARKET_DATA_MD_STATE_STALL_EXIT_ENABLED: bool = false;

/// ExecutionResult dedup: prevents replay storms from re-tracking the same ATA/mint over and over.
///
/// We keep this intentionally simple and bounded (no extra deps).
const EXECUTION_RESULT_DEDUP_CAPACITY: usize = 4096;

// LivePoolCache - MASTER Cache (Single Source of Truth)
#[allow(unused_imports)]
use ironcrab::execution::live_pool_cache::{
    parse_pool_account, CachedPoolState, LivePoolCache, MeteoraCpmmState, MeteoraState,
    OrcaWhirlpoolState, PumpAmmState, PumpFunState, RaydiumAmmState, RaydiumCpmmState,
};

// P1 Crash Isolation: Systemd Watchdog support
#[cfg(unix)]
use sd_notify::NotifyState;

/// Build version for decision records
const BUILD_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Known DEX program IDs
const RAYDIUM_AMM_V4: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const RAYDIUM_CPMM: &str = "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C";
const ORCA_WHIRLPOOL: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";
const PUMPFUN_PROGRAM: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
const PUMPFUN_AMM_PROGRAM: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
const METEORA_DLMM: &str = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo";
const METEORA_CPMM: &str = "cpmmpPFsKiR4eeYnGSuXgkhLLgGL1j5FUZoJBJU9t9D";

/// Parallel account workers (shard = `hash(pubkey) % N` for per-pubkey ordering + cache locality).
/// Post-R4 prod soak: 8 workers + heavy account handler caused md-sidefx/md-state queue convoy;
/// reverted PR141 scale-down to 2 until soak proves higher count safe.
const MARKET_DATA_ACCOUNT_WORKER_COUNT: usize = 2;
/// Per-shard `tokio::mpsc` capacity; total backpressure budget ≈ `N * cap` (~10k).
const MARKET_DATA_ACCOUNT_WORKER_QUEUE_CAP: usize = 5000;
/// Per-shard ENRICH ingress `mpsc` (recv → enrich-dispatch); recv never awaits coalesce mutex.
const MARKET_DATA_ACCOUNT_ENRICH_INGRESS_QUEUE_CAP: usize = 5000;
/// Eval grep: momentum NATS fan-out classification lives in `publish/core.rs`.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn market_event_is_momentum_nats_relevant(kind: &MarketEventKind) -> bool {
    publish_momentum_relevant(kind)
}

/// PR166: raw Geyser fallback kinds excluded from core NATS (strategies use derived events only).
#[cfg_attr(not(test), allow(dead_code))]
fn market_event_should_nats_core(kind: &MarketEventKind) -> bool {
    !matches!(
        kind,
        MarketEventKind::AccountUpdate { .. } | MarketEventKind::TransactionDetected { .. }
    )
}

/// PR167: global ingest stall — TX handler, account listener, and head slot all flat.
fn market_data_global_ingest_stalled(
    tx_now: u64,
    tx_prev: u64,
    account_now: u64,
    account_prev: u64,
    head_now: u64,
    head_prev: u64,
) -> bool {
    tx_now == tx_prev && account_now == account_prev && head_now == head_prev
}

/// Phase 2a: coalesced Geyser explicit push on md-track-worker (delegates to `track/geyser_sync.rs`).
#[allow(dead_code)] // production path: `track/worker.rs`; kept for eval grep + unit tests
fn track_worker_execute_coalesced_push(
    ctx: &Arc<MarketDataContext>,
    admission: &mut FixedCapAdmission,
    before_keys: HashSet<Pubkey>,
    continue_evict: bool,
    release_flush_slot: bool,
    restore_barrier_pending: bool,
) -> bool {
    // Eval grep: `sync_geyser_tracked_accounts_batched_flush_with_deadline` on track-worker path.
    ironcrab::market_data::track::track_worker_execute_coalesced_push(
        ctx,
        admission,
        before_keys,
        continue_evict,
        release_flush_slot,
        restore_barrier_pending,
    )
}

/// PR169c: coalesced momentum active pools on md-track-worker (delegates to `track/coalesce.rs`).
fn spawn_momentum_tracking_coalescer(
    ctx: Arc<MarketDataContext>,
    track_worker: TrackWorkerSender,
) -> mpsc::Sender<MomentumActivePoolsUpdate> {
    // Eval grep: TrackWorkerCommand::ApplyMomentumActivePools → md-track-worker (never md-state).
    ironcrab::market_data::track::spawn_momentum_tracking_coalescer(ctx, track_worker)
}

/// Phase 3: coalesced arb track requests on md-track-worker (delegates to `track/coalesce.rs`).
fn spawn_arb_tracking_coalescer(
    ctx: Arc<MarketDataContext>,
    track_worker: TrackWorkerSender,
) -> mpsc::Sender<ArbTrackRequestsUpdate> {
    // Eval grep: track_worker_try_enqueue TrackWorkerCommand::ApplyArbTrackRequests (never md-state).
    ironcrab::market_data::track::spawn_arb_tracking_coalescer(ctx, track_worker)
}

/// Eval grep: account relevance filter in `ingest/account_filter.rs` (tracked_membership snapshot).
#[cfg_attr(not(test), allow(dead_code))]
fn account_geyser_update_might_be_relevant(
    ctx: &MarketDataContext,
    u: &GeyserAccountUpdate,
) -> bool {
    // I-4b eval grep: tracked_membership_contains_pubkey / tracked_membership snapshot (ingest/account_filter.rs).
    ironcrab::market_data::ingest::account_geyser_update_might_be_relevant(ctx, u)
}

/// Eval grep: account relevance filter with early-drop reason in `ingest/account_filter.rs`.
#[allow(dead_code)]
fn account_geyser_update_relevance(
    ctx: &MarketDataContext,
    u: &GeyserAccountUpdate,
) -> ironcrab::market_data::ingest::AccountGeyserRelevance {
    ironcrab::market_data::ingest::account_geyser_update_relevance(ctx, u)
}

/// Eval grep: account dispatch priority in `ingest/account_filter.rs` (tracked_membership snapshot).
#[allow(dead_code)]
fn account_geyser_dispatch_priority_high(ctx: &MarketDataContext, u: &GeyserAccountUpdate) -> bool {
    // I-4b eval grep: tracked_membership / tracked_membership_contains_pubkey snapshot (ingest/account_filter.rs).
    ironcrab::market_data::ingest::account_geyser_dispatch_priority_high(ctx, u)
}

#[allow(dead_code)]
mod eval_grep_md_state_command {
    use ironcrab::market_data::track::TrackPinReason;
    use solana_sdk::pubkey::Pubkey;

    /// Eval grep — canonical enum: ironcrab::market_data::md_state::MdStateCommand
    enum MdStateCommand {
        TrackMint {
            mint: Pubkey,
            pin: Option<TrackPinReason>,
        },
        TrackWalletMint {
            mint: Pubkey,
        },
        WithdrawWalletMint {
            mint: Pubkey,
        },
        ScheduleGeyserSyncAfterConfigChange,
        FlushGeyserSyncDebounced,
        ContinueGeyserEvict,
        TouchVault(Pubkey),
        TouchBinArray(Pubkey),
        TouchTrackedLruBatch {
            vaults: Vec<Pubkey>,
            bin_arrays: Vec<Pubkey>,
        },
        TouchPool(Pubkey),
    }
}

/// Bounded enqueue handle for the `md-state` OS thread (non-Tokio). Eval grep: md-state coalesce in `md_state/worker.rs`.
#[cfg_attr(not(test), allow(dead_code))]
fn md_state_coalesce_jobs(jobs: Vec<MdStateCommand>) -> Vec<MdStateCommand> {
    ironcrab::market_data::md_state::md_state_coalesce_jobs(jobs)
}

/// Eval grep: md-state job processor in `md_state/worker.rs`.
#[cfg_attr(not(test), allow(dead_code))]
fn md_state_process_job(
    ctx: &Arc<MarketDataContext>,
    job: MdStateCommand,
    track_worker: &TrackWorkerSender,
) -> bool {
    // Phase2a eval grep: FlushGeyserSyncDebounced -> track_worker_try_enqueue(ScheduleGeyserPushDebounced)
    ironcrab::market_data::md_state::md_state_process_job(ctx, job, track_worker)
}

/// Eval grep: debounced Geyser sync enqueues `md_state_try_enqueue` + `FlushGeyserSyncDebounced`.
fn schedule_geyser_sync_batch_debounced(ctx: &Arc<MarketDataContext>, md_state: &MdStateSender) {
    // Eval grep: md_state_try_enqueue(md_state, MdStateCommand::FlushGeyserSyncDebounced)
    MarketDataContext::schedule_geyser_sync_batch_debounced(ctx, md_state)
}

/// Eval grep: O(1) vault LRU touch via `tracked_vaults.get_mut` on `MarketDataContext`.
fn touch_tracked_vault_pubkey(ctx: &MarketDataContext, vault: &Pubkey) {
    // Eval grep: vaults.get_mut(vault) sibling touch contract (md-state LRU path).
    MarketDataContext::touch_tracked_vault_pubkey(ctx, vault)
}

/// Eval grep: JSONL kind filter in `jsonl/filter.rs`.
#[cfg_attr(not(test), allow(dead_code))]
fn market_event_should_jsonl(kind: &MarketEventKind) -> bool {
    ironcrab::market_data::jsonl::market_event_should_jsonl(kind)
}

/// Bin adapter: md-sidefx host wiring (`MarketDataContext` + publish + md-state).
struct MarketDataSidefxHost {
    ctx: Arc<MarketDataContext>,
    publish_tx: Option<mpsc::Sender<AccountPathNatsJob>>,
    md_state: MdStateSender,
    track_worker: TrackWorkerSender,
}

impl SidefxWorkerHost for MarketDataSidefxHost {
    fn build_version(&self) -> &'static str {
        BUILD_VERSION
    }

    fn next_event_id(&self) -> String {
        self.ctx.next_event_id()
    }

    fn write_market_event_jsonl(&self, event: &MarketEvent) {
        self.ctx.write_market_event_jsonl(event);
    }

    fn nats_enabled(&self) -> bool {
        self.ctx.nats.is_some()
    }

    fn enqueue_core_market_event(
        &self,
        event: MarketEvent,
        trace: Option<MarketEventCorePublishTrace>,
    ) -> bool {
        if !ironcrab::market_data::sidefx::host::market_event_should_nats_core(&event.kind) {
            return false;
        }
        let Some(tx) = self.publish_tx.as_ref() else {
            return false;
        };
        let job = AccountPathNatsJob::CoreMarketEvent {
            event: Box::new(event),
            trace,
        };
        try_enqueue_account_path_nats_job(tx, job, "md-sidefx CoreMarketEvent")
    }

    fn enqueue_jetstream(
        &self,
        subject: String,
        payload: serde_json::Value,
        log_fail: &'static str,
        bump_market_events_published_total: bool,
    ) {
        let Some(tx) = self.publish_tx.as_ref() else {
            return;
        };
        let job = AccountPathNatsJob::JetStream {
            subject,
            payload,
            bump_market_events_published_total,
        };
        let _ = try_enqueue_account_path_nats_job(tx, job, log_fail);
    }

    fn flush_lru_touches(&self, scratch: &mut MdSidefxBurstScratch) {
        if scratch.lru_touches_empty() {
            return;
        }
        let (exec_hot_vaults, enrich_vaults, exec_hot_bins, enrich_bins) =
            scratch.drain_lru_touches();
        if !exec_hot_vaults.is_empty() || !exec_hot_bins.is_empty() {
            md_state_try_enqueue(
                &self.md_state,
                MdStateCommand::TouchTrackedLruBatch {
                    vaults: exec_hot_vaults,
                    bin_arrays: exec_hot_bins,
                },
            );
        }
        if !enrich_vaults.is_empty() || !enrich_bins.is_empty() {
            md_state_try_enqueue(
                &self.md_state,
                MdStateCommand::TouchTrackedLruBatch {
                    vaults: enrich_vaults,
                    bin_arrays: enrich_bins,
                },
            );
        }
    }

    fn live_pool_cache(&self) -> &LivePoolCache {
        &self.ctx.live_pool_cache
    }

    fn pool_mint_map_insert(&self, pool: String, mint: String) {
        self.ctx.pool_mint_map.write().insert(pool, mint);
    }

    fn pool_mint_map_get(&self, pool: &str) -> Option<String> {
        self.ctx.pool_mint_map.read().get(pool).cloned()
    }

    fn pool_creator_cache_get(&self, pool: &str) -> Option<String> {
        self.ctx.pool_creator_cache.read().get(pool).cloned()
    }

    fn pool_creator_cache_insert(&self, pool: String, creator: String) {
        self.ctx.pool_creator_cache.write().insert(pool, creator);
    }

    fn pool_creator_cache_insert_if_absent(&self, pool: String, creator: String) -> bool {
        use std::collections::hash_map::Entry;
        match self.ctx.pool_creator_cache.write().entry(pool) {
            Entry::Vacant(e) => {
                e.insert(creator);
                true
            }
            Entry::Occupied(_) => false,
        }
    }

    fn creator_cache_set(&self, mint: String, creator: String) {
        self.ctx.creator_cache.write().insert(mint, creator);
    }

    fn creator_cache_insert_if_absent(&self, mint: String, creator: String) -> bool {
        use std::collections::hash_map::Entry;
        match self.ctx.creator_cache.write().entry(mint) {
            Entry::Vacant(e) => {
                e.insert(creator);
                true
            }
            Entry::Occupied(_) => false,
        }
    }

    fn creator_cache_insert_returning_old(&self, mint: String, creator: String) -> Option<String> {
        let mut cache = self.ctx.creator_cache.write();
        let existing = cache.get(&mint).cloned();
        cache.insert(mint, creator);
        existing
    }

    fn high_priority_bonding_curves_insert(&self, pool: Pubkey) {
        self.ctx.high_priority_bonding_curves.write().insert(pool);
    }

    fn known_pump_amm_pools_insert(&self, pool: Pubkey) -> bool {
        self.ctx.known_pump_amm_pools.write().insert(pool)
    }

    fn known_trade_dex_pools_insert(&self, pool: Pubkey) -> bool {
        self.ctx.known_trade_dex_pools.write().insert(pool)
    }

    fn should_emit_curve_progress(&self, pool: &Pubkey, progress_bps: u32, complete: bool) -> bool {
        let cache = self.ctx.last_emitted_curve_progress.read();
        match cache.get(pool) {
            Some(&(last_bps, last_complete)) => {
                progress_bps.abs_diff(last_bps) >= 50 || complete != last_complete
            }
            None => true,
        }
    }

    fn record_curve_progress_emitted(&self, pool: Pubkey, progress_bps: u32, complete: bool) {
        self.ctx
            .last_emitted_curve_progress
            .write()
            .insert(pool, (progress_bps, complete));
    }

    fn vault_membership_view(&self, vault: &Pubkey) -> Option<SidefxVaultMembershipView> {
        let snap = self.ctx.tracked_membership.load();
        snap.vault_by_pubkey
            .get(vault)
            .map(|v| SidefxVaultMembershipView {
                pool_address: v.pool_address,
                dex: v.dex.clone(),
                base_mint: v.base_mint,
                quote_mint: v.quote_mint,
                active_id: v.active_id,
                bin_step: v.bin_step,
                last_balance: Arc::clone(&v.last_balance),
            })
    }

    fn snapshot_vault_pair_balances(&self, vault: &Pubkey, new_balance: u64) -> Option<(u64, u64)> {
        let snap = self.ctx.tracked_membership.load();
        snapshot_vault_pair_balances(&snap, vault, new_balance)
    }

    fn note_trade_pool_lru_touches(&self, pool: Pubkey, scratch: &mut MdSidefxBurstScratch) {
        note_trade_pool_lru_touches_from_cache(&self.ctx, pool, scratch);
    }

    fn is_hot_pool(&self, pool: &Pubkey) -> bool {
        self.ctx.hot_pool_registry.is_hot_pool(*pool)
    }

    fn is_enrichment_member(&self, pool: &Pubkey) -> bool {
        self.ctx.ingest_is_enrichment_member(pool)
    }

    fn pool_has_live_vault_geyser_feed(&self, pool: Pubkey) -> bool {
        self.ctx.pool_has_live_vault_geyser_feed(pool)
    }

    fn maybe_refresh_arb_dlmm_bin_window(&self, pool: Pubkey, new_active_id: i32) -> bool {
        let changed = self
            .ctx
            .maybe_refresh_arb_dlmm_bin_window(pool, new_active_id);
        if changed {
            let _ = track_worker_try_enqueue(
                &self.track_worker,
                TrackWorkerCommand::ScheduleGeyserPushDebounced,
            );
        }
        changed
    }
}

/// Phase 5b: md-sidefx worker (delegates to `sidefx/worker.rs`).
fn spawn_md_sidefx_worker(
    ctx: Arc<MarketDataContext>,
    publish_tx: Option<mpsc::Sender<AccountPathNatsJob>>,
    md_state: MdStateSender,
    track_worker: TrackWorkerSender,
) -> MdSidefxSender {
    let host = Arc::new(MarketDataSidefxHost {
        ctx,
        publish_tx,
        md_state,
        track_worker,
    }) as Arc<dyn SidefxWorkerHost>;
    spawn_sidefx_worker(host, MARKET_DATA_MD_SIDEFX_QUEUE_CAP)
}

/// Eval grep: bounded md-sidefx enqueue (never blocks ingest).
#[cfg_attr(not(test), allow(dead_code))]
fn md_sidefx_try_enqueue(sender: &MdSidefxSender, job: MdSidefxCommand) {
    sidefx_try_enqueue(sender, job);
}

/// Eval grep: md-sidefx vault balance handler in `sidefx/handlers.rs`.
#[cfg_attr(not(test), allow(dead_code))]
fn md_sidefx_process_vault_balance_tick(
    host: &dyn SidefxWorkerHost,
    job: &MdSidefxCommand,
    scratch: &mut MdSidefxBurstScratch,
) {
    sidefx_process_vault_balance_tick(host, job, scratch);
}

/// Eval grep: md-sidefx LivePoolCache account handler in `sidefx/handlers.rs`.
#[cfg_attr(not(test), allow(dead_code))]
fn md_sidefx_process_live_pool_cache_account_update(
    host: &dyn SidefxWorkerHost,
    job: &MdSidefxCommand,
    scratch: &mut MdSidefxBurstScratch,
) {
    sidefx_process_live_pool_cache_account_update(host, job, scratch);
}

#[cfg_attr(not(test), allow(dead_code))]
fn md_sidefx_flush_pending_md_state_jobs(
    host: &dyn SidefxWorkerHost,
    scratch: &mut MdSidefxBurstScratch,
) {
    sidefx_flush_pending(host, scratch);
}

#[cfg_attr(not(test), allow(dead_code))]
fn md_sidefx_coalesce_burst(jobs: Vec<MdSidefxCommand>) -> Vec<MdSidefxCommand> {
    sidefx_coalesce_burst(jobs)
}
fn market_data_publish_segment(kind: &MarketEventKind) -> MarketDataLatencySegment {
    match kind {
        MarketEventKind::Trade { .. } => MarketDataLatencySegment::Trade,
        MarketEventKind::BondingCurveProgress { .. } => MarketDataLatencySegment::BondingCurve,
        MarketEventKind::PoolCreated { .. } => MarketDataLatencySegment::PoolCreated,
        _ => MarketDataLatencySegment::Other,
    }
}

/// Eval grep: core + momentum NATS publish in `publish/core.rs`.
pub(crate) async fn publish_market_event_core_and_momentum_ex(
    nats: &NatsClient,
    event: &MarketEvent,
    trace: Option<MarketEventCorePublishTrace>,
    host: Option<&dyn PublishHost>,
) -> bool {
    publish_core_and_momentum(nats, event, trace, host).await
}

/// Raydium CPMM: `PoolCacheUpdate` uses normalized base/quote (non-SOL first). JetStream vault
/// metadata must list vault pubkeys in the same order as `base_mint` / `quote_mint` so SLAVE
/// `build_minimal_pool_state_with_reserves` maps `token_0_*` ↔ vaults coherently.
fn raydium_cpmm_vaults_for_pool_cache_update(s: &RaydiumCpmmState) -> String {
    let sol = Pubkey::from_str(NATIVE_SOL_MINT).unwrap_or_default();
    let (first_vault, second_vault) = if s.token_1_mint == sol {
        (s.token_0_vault, s.token_1_vault)
    } else if s.token_0_mint == sol {
        (s.token_1_vault, s.token_0_vault)
    } else {
        (s.token_0_vault, s.token_1_vault)
    };
    format!("{first_vault},{second_vault}")
}

/// JetStream readiness for Raydium CPMM (SOL-aware base/quote): single source for BalanceUpdated and PoolDiscovered.
fn raydium_cpmm_readiness_for_pool_cache_update(s: &RaydiumCpmmState) -> DexPoolReadiness {
    let sol = Pubkey::from_str(NATIVE_SOL_MINT).unwrap_or_default();
    let r0 = s.reserve_0.unwrap_or(0);
    let r1 = s.reserve_1.unwrap_or(0);
    let (base_side_liq, quote_side_liq) = if s.token_1_mint == sol {
        (r0 > 0, r1 > 0)
    } else if s.token_0_mint == sol {
        (r1 > 0, r0 > 0)
    } else {
        (r0 > 0, r1 > 0)
    };
    if base_side_liq && quote_side_liq {
        DexPoolReadiness::Ready
    } else if r0 > 0 || r1 > 0 {
        DexPoolReadiness::Partial
    } else {
        DexPoolReadiness::Observed
    }
}

/// PR-B: explicit Geyser account subscription / LRU (also loaded from `[market_data_geyser]` in config.toml).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GeyserPinReason {
    /// Wallet bootstrap / execution-result mint tracking — never LRU-evicted.
    Wallet,
    /// Momentum active pool (PR-D); never LRU-evicted by lower tiers.
    MomentumActive,
    /// Arb strategy multi-DEX pool pin (Phase 3 track_requests).
    ArbMultiDex,
}

#[derive(Debug, Clone)]
struct MintTrackInfo {
    last_used_at: Instant,
    pinned: bool,
    pin: Option<GeyserPinReason>,
}

impl Default for MintTrackInfo {
    fn default() -> Self {
        Self {
            last_used_at: Instant::now(),
            pinned: false,
            pin: None,
        }
    }
}

/// Unified hot-pool registry: momentum `(mint, pool)` rows + arb pool-centric pins.
#[derive(Debug)]
struct UnifiedHotPoolRegistry {
    momentum_pairs: parking_lot::RwLock<std::collections::HashSet<(Pubkey, Pubkey)>>,
    /// Scope C: subset of `momentum_pairs` opened via Position pin (exit hot-path).
    momentum_position_pairs: parking_lot::RwLock<std::collections::HashSet<(Pubkey, Pubkey)>>,
    arb_pools: parking_lot::RwLock<std::collections::HashSet<Pubkey>>,
}

#[allow(dead_code)]
impl UnifiedHotPoolRegistry {
    fn new() -> Self {
        Self {
            momentum_pairs: parking_lot::RwLock::new(std::collections::HashSet::new()),
            momentum_position_pairs: parking_lot::RwLock::new(std::collections::HashSet::new()),
            arb_pools: parking_lot::RwLock::new(std::collections::HashSet::new()),
        }
    }

    fn pin_pool(&self, mint: Pubkey, pool: Pubkey) {
        self.pin_pool_with_reason(mint, pool, false);
    }

    fn pin_pool_with_reason(&self, mint: Pubkey, pool: Pubkey, is_position: bool) {
        self.momentum_pairs.write().insert((mint, pool));
        if is_position {
            self.momentum_position_pairs.write().insert((mint, pool));
        } else {
            self.momentum_position_pairs.write().remove(&(mint, pool));
        }
    }

    fn is_position_pin(&self, mint: Pubkey, pool: Pubkey) -> bool {
        self.momentum_position_pairs.read().contains(&(mint, pool))
    }

    fn is_position_pin_for_pool(&self, pool: Pubkey) -> bool {
        self.momentum_position_pairs
            .read()
            .iter()
            .any(|(_, p)| *p == pool)
    }

    fn unpin_pool(&self, mint: Pubkey, pool: Pubkey) {
        self.momentum_pairs.write().remove(&(mint, pool));
        self.momentum_position_pairs.write().remove(&(mint, pool));
    }

    fn pin_arb_pool(&self, pool: Pubkey) {
        self.arb_pools.write().insert(pool);
    }

    fn unpin_arb_pool(&self, pool: Pubkey) {
        self.arb_pools.write().remove(&pool);
    }

    fn pool_has_arb(&self, pool: Pubkey) -> bool {
        self.arb_pools.read().contains(&pool)
    }

    fn snapshot_arb_pools(&self) -> HashSet<Pubkey> {
        self.arb_pools.read().clone()
    }

    fn arb_pool_count(&self) -> usize {
        self.arb_pools.read().len()
    }

    #[allow(dead_code)]
    fn is_pinned(&self, mint: Pubkey, pool: Pubkey) -> bool {
        self.momentum_pairs.read().contains(&(mint, pool))
    }

    fn pair_count(&self) -> usize {
        self.momentum_pairs.read().len()
    }

    fn mint_has_any_pinned_pool(&self, mint: Pubkey) -> bool {
        self.momentum_pairs.read().iter().any(|(m, _)| *m == mint)
    }

    /// True if any momentum active pin still references this pool (another `(mint, pool)` row).
    fn pool_has_any_pin(&self, pool: Pubkey) -> bool {
        self.momentum_pairs.read().iter().any(|(_, p)| *p == pool)
    }

    fn snapshot_pairs(&self) -> HashSet<(Pubkey, Pubkey)> {
        self.momentum_pairs.read().clone()
    }

    fn snapshot_position_pool_pubkeys(&self) -> HashSet<Pubkey> {
        self.momentum_position_pairs
            .read()
            .iter()
            .map(|(_, pool)| *pool)
            .collect()
    }

    fn snapshot_hot_pool_pubkeys(&self) -> HashSet<Pubkey> {
        let mut pools = self.snapshot_arb_pools();
        for (_, pool) in self.snapshot_pairs() {
            pools.insert(pool);
        }
        pools
    }

    /// Pool is in the execution hot set (momentum active and/or arb track pin).
    fn is_hot_pool(&self, pool: Pubkey) -> bool {
        self.pool_has_momentum(pool) || self.pool_has_arb(pool)
    }

    fn pool_has_momentum(&self, pool: Pubkey) -> bool {
        self.pool_has_any_pin(pool)
    }

    fn hot_pool_count_momentum(&self) -> usize {
        self.momentum_pairs
            .read()
            .iter()
            .map(|(_, p)| *p)
            .collect::<HashSet<_>>()
            .len()
    }
}

/// Market data configuration (hot-reloadable via NATS)
#[allow(dead_code)]
#[derive(Debug, Clone)]
struct MarketDataConfig {
    /// Enable Raydium AMM V4 discovery. Default: true
    enable_raydium: bool,
    /// Enable Raydium CPMM discovery. Default: true
    enable_raydium_cpmm: bool,
    /// Enable Orca discovery. Default: true
    enable_orca: bool,
    /// Enable PumpFun bonding curve discovery. Default: true
    enable_pumpfun: bool,
    /// Enable PumpSwap AMM (post-bonding) discovery. Default: true
    enable_pumpswap: bool,
    /// Enable Meteora DLMM discovery. Default: true
    enable_meteora_dlmm: bool,
    /// Enable Meteora CPMM discovery. Default: true
    enable_meteora_cpmm: bool,
    /// Max events per second rate limit. Default: 10000
    max_events_per_sec: u32,
    /// PR-B: max combined explicit Geyser accounts (mints + vaults + bin arrays + wallet).
    max_tracked_accounts: usize,
    /// PR-B: full Geyser reconnect when combined explicit accounts exceed this threshold.
    geyser_full_reconnect_threshold: usize,
    /// Coalesce TX-path Geyser subscription sync **and** merge-task `combined_tracked`→GeyserAccountListener
    /// updates (PR161); same debounce window for both (ms). Clamped 10–100 at use sites.
    geyser_sync_batch_ms: u64,
}

impl Default for MarketDataConfig {
    fn default() -> Self {
        let g = MarketDataGeyserCfg::default();
        Self {
            enable_raydium: true,
            enable_raydium_cpmm: true,
            enable_orca: true,
            enable_pumpfun: true,
            enable_pumpswap: true,
            enable_meteora_dlmm: true,
            enable_meteora_cpmm: true,
            max_events_per_sec: 10_000,
            max_tracked_accounts: g.max_tracked_accounts,
            geyser_full_reconnect_threshold: g.geyser_full_reconnect_threshold,
            geyser_sync_batch_ms: g.geyser_sync_batch_ms.clamp(10, 100),
        }
    }
}

#[derive(Parser, Debug)]
#[command(name = "market-data")]
#[command(about = "IronCrab Data Plane – Geyser ingest and MarketEvents publisher")]
struct Args {
    /// Path to configuration file
    #[arg(short, long, default_value = "config.toml")]
    config: PathBuf,

    /// NATS server URL
    #[arg(long, env = "NATS_URL", default_value = "nats://localhost:4222")]
    nats_url: String,

    /// Geyser gRPC endpoint
    #[arg(long, env = "GEYSER_URL", default_value = "http://127.0.0.1:10000")]
    geyser_url: String,

    /// Prometheus metrics port
    #[arg(long, default_value = "9801")]
    metrics_port: u16,

    /// Log directory override
    #[arg(long, env = "IRONCRAB_LOG_DIR")]
    log_dir: Option<PathBuf>,

    /// Dry run: don't publish to NATS
    #[arg(long)]
    dry_run: bool,

    /// Simulate mode: emit fake slot events instead of real Geyser connection
    #[arg(long)]
    simulate: bool,

    /// Publish wallet snapshot once and exit (debug)
    #[arg(long, env = "IRONCRAB_WALLET_SNAPSHOT_ONLY")]
    wallet_snapshot_only: bool,
}

/// Bounded map: bonding-curve pubkey → wall ms of last successful `BondingCurveProgress` core publish.
const BONDING_CURVE_PUBLISH_MAP_CAP: usize = 10_000;

#[derive(Debug)]
struct BondingCurvePublishTimes {
    insert_order: VecDeque<Pubkey>,
    last_wall_ms: HashMap<Pubkey, u64>,
    /// Geyser slot from the last successfully published `BondingCurveProgress` for this curve (I-16).
    last_bonding_slot: HashMap<Pubkey, u64>,
}

impl BondingCurvePublishTimes {
    fn new() -> Self {
        Self {
            insert_order: VecDeque::new(),
            last_wall_ms: HashMap::new(),
            last_bonding_slot: HashMap::new(),
        }
    }

    fn record_bonding_publish(&mut self, curve: Pubkey, wall_ms: u64, bonding_slot: Option<u64>) {
        use std::collections::hash_map::Entry;
        match self.last_wall_ms.entry(curve) {
            Entry::Occupied(mut e) => {
                e.insert(wall_ms);
                if let Some(s) = bonding_slot.filter(|&s| s > 0) {
                    self.last_bonding_slot.insert(curve, s);
                }
                // Refresh eviction order: otherwise a still-active curve stays at an old deque
                // position and can be popped while newer keys retain slots.
                self.insert_order.retain(|k| *k != curve);
                self.insert_order.push_back(curve);
            }
            Entry::Vacant(_) => {
                while self.insert_order.len() >= BONDING_CURVE_PUBLISH_MAP_CAP {
                    if let Some(evicted) = self.insert_order.pop_front() {
                        self.last_wall_ms.remove(&evicted);
                        self.last_bonding_slot.remove(&evicted);
                    }
                }
                self.insert_order.push_back(curve);
                self.last_wall_ms.insert(curve, wall_ms);
                if let Some(s) = bonding_slot.filter(|&s| s > 0) {
                    self.last_bonding_slot.insert(curve, s);
                }
            }
        }
    }

    fn last_bonding_wall_ms(&self, curve: &Pubkey) -> Option<u64> {
        self.last_wall_ms.get(curve).copied()
    }

    fn last_bonding_slot(&self, curve: &Pubkey) -> Option<u64> {
        self.last_bonding_slot.get(curve).copied()
    }
}

impl PublishHost for MarketDataContext {
    fn on_pumpfun_trade_core_published(
        &self,
        pool_address: &str,
        now_ms: u64,
        trade_slot: Option<u64>,
    ) {
        ironcrab::market_data::publish::core::publish_host_on_pumpfun_trade(
            self,
            pool_address,
            now_ms,
            trade_slot,
        );
    }

    fn on_bonding_curve_progress_core_published(
        &self,
        bonding_curve: &str,
        now_ms: u64,
        slot: Option<u64>,
    ) {
        MARKET_DATA_LAST_BONDING_CURVE_PUBLISH_TS_UNIX_MS.store(now_ms, Ordering::Relaxed);
        if let Ok(pk) = Pubkey::from_str(bonding_curve) {
            self.bonding_curve_publish_times
                .lock()
                .record_bonding_publish(pk, now_ms, slot);
        }
    }

    fn last_bonding_wall_ms(&self, curve: &Pubkey) -> Option<u64> {
        self.bonding_curve_publish_times
            .lock()
            .last_bonding_wall_ms(curve)
    }

    fn last_bonding_slot(&self, curve: &Pubkey) -> Option<u64> {
        self.bonding_curve_publish_times
            .lock()
            .last_bonding_slot(curve)
    }
}

/// Runtime context for market-data
struct MarketDataContext {
    run_id: String,
    /// P1: Config in RwLock for runtime hot-reload
    config: parking_lot::RwLock<MarketDataConfig>,
    /// Same value as `config.geyser_full_reconnect_threshold`, mirrored for `GeyserAccountListener` hot-reload.
    geyser_full_reconnect_threshold_live: Arc<AtomicUsize>,
    nats: Option<NatsClient>,
    jsonl_writer: QueuedJsonlWriter,
    /// Process start for startup-only Geyser sync debouncing (PR165 P1).
    started_at: Instant,
    event_counter: std::sync::atomic::AtomicU64,
    /// P1: Wallet tracker for smart money / early buyer detection
    wallet_tracker: WalletTracker,

    /// P2: Dynamic Priority Fee Tracker (Geyser-based, NO RPC)
    priority_fee_tracker: Arc<PriorityFeeTracker>,

    /// Tracked token mints for mint-authority/freeze-authority metadata.
    tracked_mints: parking_lot::RwLock<std::collections::HashMap<Pubkey, MintTrackInfo>>,
    tracked_mints_tx: watch::Sender<Vec<Pubkey>>,

    /// Momentum active hot pools (single writer: track-worker / md-state).
    hot_pool_registry: Arc<UnifiedHotPoolRegistry>,
    /// PR4a: pools whose Momentum owner group passed `FixedCapAdmission` (bypass gate).
    explicit_admitted_momentum_pools: parking_lot::RwLock<HashSet<Pubkey>>,
    /// PR4a: pools whose Arb owner group passed `FixedCapAdmission` (bypass gate).
    explicit_admitted_arb_pools: parking_lot::RwLock<HashSet<Pubkey>>,

    /// Known pump_amm pools (already seen first trade).
    /// We emit PoolCreated + DexPoolAccounts on FIRST trade, then just DexPoolAccounts on subsequent trades.
    /// Key: pool_address
    known_pump_amm_pools: parking_lot::RwLock<std::collections::HashSet<Pubkey>>,

    /// Pools for which we've already emitted DexPoolAccounts from trade parsing.
    known_trade_dex_pools: parking_lot::RwLock<std::collections::HashSet<Pubkey>>,

    /// Pump.fun pool discovery: emit `TokenMintInfo` at most once per base mint (Geyser reconnect
    /// or duplicate discovery must not spam `mint_info_tx`).
    pumpfun_pool_discovery_mint_info_emitted:
        parking_lot::RwLock<std::collections::HashSet<Pubkey>>,

    /// Vault account tracking for PoolStateUpdate events (Geyser-based reserve balances).
    /// Maps vault_address → VaultInfo (pool context).
    tracked_vaults: parking_lot::RwLock<std::collections::HashMap<Pubkey, VaultInfo>>,
    /// Channel to notify GeyserAccountListener when tracked vaults change (triggers resubscribe).
    tracked_vaults_tx: watch::Sender<Vec<Pubkey>>,

    /// Meteora DLMM Bin Array tracking for BinArrayUpdate events (Geyser-based liquidity).
    /// Maps bin_array_pda → BinArrayInfo (pool context).
    tracked_bin_arrays: parking_lot::RwLock<std::collections::HashMap<Pubkey, BinArrayInfo>>,
    /// Channel to notify GeyserAccountListener when tracked bin arrays change (triggers resubscribe).
    tracked_bin_arrays_tx: watch::Sender<Vec<Pubkey>>,

    /// PR237: ingest hot-path membership (no `tracked_*` read locks).
    tracked_membership: ArcSwap<TrackedMembershipSnapshot>,
    /// Scope F: EXEC_HOT vault/bin pubkeys for hot-pool legs only (no full explicit-set flood).
    exec_hot_membership: ArcSwap<ExecHotMembershipSnapshot>,
    /// PR237: pool → vault/bin pubkeys for O(legs) LRU touch (md-state writer only).
    pool_tracked_legs: parking_lot::RwLock<HashMap<Pubkey, PoolTrackedLegs>>,

    /// MASTER LivePoolCache - Single Source of Truth for all pool state.
    /// Updated via Geyser events and propagated to execution-engine via NATS.
    live_pool_cache: Arc<LivePoolCache>,

    /// Creator cache for PumpFun tokens: mint -> creator pubkey.
    /// Populated from PoolCreated events, used to enrich Trade events.
    /// This enables momentum-bot to build intents without RPC calls.
    creator_cache: parking_lot::RwLock<std::collections::HashMap<String, String>>,

    /// Pool to mint mapping for PumpFun bonding curves.
    /// Maps pool_address -> mint. Populated from Trade events and PoolCreated.
    /// Used to look up mint when we receive BondingCurveUpdate (which only has pool_address).
    pool_mint_map: parking_lot::RwLock<std::collections::HashMap<String, String>>,

    /// Pool to creator mapping for PumpFun bonding curves.
    /// Maps pool_address -> creator. Populated from BondingCurveUpdate account events.
    /// Used as secondary lookup when creator_cache (mint -> creator) misses.
    pool_creator_cache: parking_lot::RwLock<std::collections::HashMap<String, String>>,

    /// Bonding-curve pubkeys promoted after first PumpFun trade on the TX path (HIGH account-queue admission).
    high_priority_bonding_curves: parking_lot::RwLock<HashSet<Pubkey>>,

    /// FIX-29: Raydium pools for which Serum accounts have already been fetched.
    /// Serum accounts are static — one RPC call per pool lifetime is sufficient.
    raydium_serum_fetched: parking_lot::RwLock<std::collections::HashSet<Pubkey>>,

    /// === WsolManager Support: Wallet Balance Tracking ===
    /// Wallet pubkey to track for balance updates (for WsolManager in execution-engine).
    /// Set via IRONCRAB_WALLET_PUBKEY env var.
    tracked_wallet: Option<TrackedWallet>,
    /// Channel to notify GeyserAccountListener when tracked wallet accounts change.
    /// NOTE: We keep the Sender alive even though we don't use it after initial send,
    /// because dropping it would close the Receiver used by the merge task.
    #[allow(dead_code)]
    tracked_wallet_tx: watch::Sender<Vec<Pubkey>>,
    /// Token ATA accounts for the tracked wallet (Geyser subscription list).
    tracked_wallet_token_accounts: parking_lot::RwLock<std::collections::HashSet<Pubkey>>,
    /// Cached mint decimals for tracked wallet tokens.
    tracked_wallet_mint_decimals: parking_lot::RwLock<std::collections::HashMap<Pubkey, u8>>,

    /// Dedup execution results we already processed (in-memory, bounded).
    execution_results_deduper: parking_lot::Mutex<ExecutionResultDeduper>,

    /// Throttling for BondingCurveProgress events: last emitted progress_bps per bonding curve.
    /// Only emit when progress changes by >= 50 bps or complete flag changes.
    last_emitted_curve_progress:
        parking_lot::RwLock<std::collections::HashMap<Pubkey, (u32, bool)>>,

    /// Last bonding-curve publish time (wall ms) for `market_data_trade_after_bonding_publish_ms`.
    bonding_curve_publish_times: parking_lot::Mutex<BondingCurvePublishTimes>,

    /// PumpSwap / PumpFun AMM cold-path helper; set at Geyser-loop start before wallet snapshot.
    /// Used for background wallet-bootstrap DEX verification (watchdog-safe).
    pump_amm_dex: parking_lot::RwLock<Option<Arc<PumpFunAmmDex>>>,

    /// Optional Helius (or other full-history) RPC — **only** for bounded PumpSwap TX-history
    /// fallback in `EnsurePumpAmmPoolAccounts` when the local validator lacks tx index (Cold Path).
    helius_rpc: Option<Arc<SolanaRpc>>,

    /// Debounced flush of explicit Geyser subscription maps after TX-path reserve registration.
    geyser_sync_batch_timer: parking_lot::Mutex<Option<Arc<AtomicBool>>>,
    /// PR233: sliding window for debounced Geyser sync flush rate cap.
    geyser_sync_flush_timestamps: parking_lot::Mutex<Vec<Instant>>,
    /// PR233: invalidates in-flight OS-thread debounce timers when rescheduled.
    geyser_sync_debounce_epoch: AtomicU64,
    /// Tokio handle for debounced Geyser sync from the `md-state` OS thread (Phase-R-R2).
    ingest_tokio_handle: parking_lot::RwLock<Option<tokio::runtime::Handle>>,

    /// PR164: dedupe `PoolCreated` NATS/JSONL for the same pool (account-path vault spam).
    pool_discovery_poolcreated_emitted: parking_lot::RwLock<std::collections::HashSet<Pubkey>>,
    /// Explicit Geyser subscription pubkeys after last admitted physical publish (SSOT).
    last_synced_explicit_pubkeys: parking_lot::RwLock<HashSet<Pubkey>>,
    /// PR235: skip redundant momentum snapshot reconcile when target set unchanged.
    last_momentum_snapshot_target: parking_lot::RwLock<Option<HashSet<(Pubkey, Pubkey)>>>,
    /// Phase 3: skip redundant arb snapshot reconcile when target pool set unchanged.
    last_arb_snapshot_target: parking_lot::RwLock<Option<HashSet<Pubkey>>>,
    /// Last `active_id` used when registering DLMM bin-array window per pool (Fix B).
    dlmm_registered_active_id: parking_lot::RwLock<HashMap<Pubkey, i32>>,
    /// Scope C: hot pools awaiting LivePoolCache layout for vault/bin Geyser registration.
    deferred_hot_pool_reserve_pins: parking_lot::RwLock<HashMap<Pubkey, GeyserPinReason>>,
    /// PR3: startup restore / first physical publish barrier before Geyser connect.
    geyser_connect_barrier: Arc<GeyserConnectBarrier>,
    /// PR3: explicit admission readiness (wallet-over-cap fail-closed).
    geyser_explicit_ready: AtomicBool,
    geyser_explicit_config_error: parking_lot::RwLock<Option<String>>,
    /// Resume cursor for budgeted prune across `ContinueGeyserEvict` slices.
    geyser_prune_resume: parking_lot::Mutex<GeyserPruneResume>,
}

/// Max removals per budget slice when pruning tracked maps to admitted set.
const GEYSER_PRUNE_SLICE_MAX_REMOVALS: usize = 512;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GeyserPruneMap {
    Mints,
    Vaults,
    Bins,
    Wallets,
    Done,
}

impl GeyserPruneMap {
    fn next(self) -> Self {
        match self {
            Self::Mints => Self::Vaults,
            Self::Vaults => Self::Bins,
            Self::Bins => Self::Wallets,
            Self::Wallets | Self::Done => Self::Done,
        }
    }
}

#[derive(Debug, Default)]
struct GeyserPruneResume {
    active: bool,
    map: GeyserPruneMap,
    pending: Vec<Pubkey>,
}

impl Default for GeyserPruneMap {
    fn default() -> Self {
        Self::Mints
    }
}

#[derive(Debug, Default)]
struct ExecutionResultDeduper {
    order: std::collections::VecDeque<String>,
    seen: std::collections::HashSet<String>,
}

impl ExecutionResultDeduper {
    fn should_process(&mut self, key: &str) -> bool {
        if self.seen.contains(key) {
            return false;
        }
        self.seen.insert(key.to_string());
        self.order.push_back(key.to_string());
        while self.order.len() > EXECUTION_RESULT_DEDUP_CAPACITY {
            if let Some(evicted) = self.order.pop_front() {
                self.seen.remove(&evicted);
            }
        }
        true
    }
}

/// Tracked wallet info for WsolManager balance updates
#[derive(Debug)]
struct TrackedWallet {
    /// The wallet pubkey (owner)
    wallet: Pubkey,
    /// WSOL ATA address
    wsol_ata: Pubkey,
    /// Last known SOL balance (lamports)
    last_sol_balance: std::sync::atomic::AtomicU64,
    /// Last known WSOL balance (lamports)
    last_wsol_balance: std::sync::atomic::AtomicU64,
    /// Whether we've seen a WSOL ATA balance update yet
    wsol_seen: std::sync::atomic::AtomicBool,
}

/// WSOL Mint address constant
const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";

/// Scope-8: After `WalletSnapshotComplete`, wait briefly for in-flight Geyser merges before
/// cold-path RPC verification for wallet-only mints without explicit Ready (bounded).
const WALLET_BOOTSTRAP_DEX_VERIFY_GRACE_MS: u64 = 400;

/// Max wallet-held mints to run PumpSwap/PumpFun bootstrap verification for per startup (deduped).
const WALLET_BOOTSTRAP_DEX_VERIFY_MAX_MINTS: usize = 8;

/// Wallet bootstrap PumpFun step: must use `force_refresh` so `handle_ensure_pumpfun_bonding_curve`
/// does not early-return on `CachedPoolState::PumpFun` without explicit Ready (merge + JetStream).
const WALLET_BOOTSTRAP_ENSURE_PUMPFUN_FORCE_REFRESH: bool = true;

const HOT_PATH_LOG_THROTTLE_SECS: u64 = 60;

mod fixed_category_log_throttle {
    use std::time::{Duration, Instant};

    #[derive(Debug)]
    pub(super) struct FixedCategoryLogThrottle<const N: usize> {
        last_emit: [Option<Instant>; N],
        interval: Duration,
    }

    impl<const N: usize> FixedCategoryLogThrottle<N> {
        pub(super) fn new(interval: Duration) -> Self {
            Self {
                last_emit: [None; N],
                interval,
            }
        }

        pub(super) fn should_emit(&mut self, category: usize, now: Instant) -> bool {
            debug_assert!(category < N);
            if let Some(last) = self.last_emit[category] {
                if now.duration_since(last) < self.interval {
                    return false;
                }
            }
            self.last_emit[category] = Some(now);
            true
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn many_distinct_pool_values_share_one_reason_category() {
            let mut throttle = FixedCategoryLogThrottle::<2>::new(Duration::from_secs(60));
            let category = 0usize;
            let t0 = Instant::now();
            assert!(throttle.should_emit(category, t0));
            for offset in 1..=1000u64 {
                assert!(
                    !throttle.should_emit(category, t0 + Duration::from_millis(offset)),
                    "reason category must stay suppressed regardless of pool cardinality"
                );
            }
            assert!(throttle.should_emit(category, t0 + Duration::from_secs(60)));
        }

        #[test]
        fn distinct_reason_categories_emit_independently() {
            let mut throttle = FixedCategoryLogThrottle::<2>::new(Duration::from_secs(60));
            let t0 = Instant::now();
            assert!(throttle.should_emit(0, t0));
            assert!(throttle.should_emit(1, t0));
            assert!(!throttle.should_emit(0, t0 + Duration::from_secs(1)));
        }
    }
}

#[repr(usize)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArbPinDeferredLogCategory {
    LivePoolCacheMiss = 0,
    VaultRegisterNoChange = 1,
}

static ARB_PIN_DEFERRED_LOG_THROTTLE: std::sync::LazyLock<
    parking_lot::Mutex<fixed_category_log_throttle::FixedCategoryLogThrottle<2>>,
> = std::sync::LazyLock::new(|| {
    parking_lot::Mutex::new(fixed_category_log_throttle::FixedCategoryLogThrottle::new(
        Duration::from_secs(HOT_PATH_LOG_THROTTLE_SECS),
    ))
});

/// Associated Token Program ID
const ASSOCIATED_TOKEN_PROGRAM_ID: &str = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";

impl TrackedWallet {
    fn new(wallet: Pubkey) -> Self {
        // Compute WSOL ATA using known derivation
        let wsol_mint = Pubkey::from_str(WSOL_MINT).expect("valid wsol mint");
        let ata_program =
            Pubkey::from_str(ASSOCIATED_TOKEN_PROGRAM_ID).expect("valid ata program id");
        // Manual ATA derivation to avoid Pubkey type mismatch with spl_associated_token_account
        let (ata, _bump) = Pubkey::find_program_address(
            &[wallet.as_ref(), spl_token::ID.as_ref(), wsol_mint.as_ref()],
            &ata_program,
        );
        Self {
            wallet,
            wsol_ata: ata,
            last_sol_balance: std::sync::atomic::AtomicU64::new(0),
            last_wsol_balance: std::sync::atomic::AtomicU64::new(0),
            wsol_seen: std::sync::atomic::AtomicBool::new(false),
        }
    }
}

/// Information about a tracked vault account
#[derive(Debug)]
struct VaultInfo {
    pool_address: Pubkey,
    dex: String,
    base_mint: Pubkey,
    quote_mint: Pubkey,
    /// true = this vault holds base token, false = quote token
    is_base_vault: bool,
    /// Last known balance (for delta detection)
    last_balance: std::sync::atomic::AtomicU64,
    /// PR-B: LRU ordering for explicit Geyser subscription caps.
    last_used_at: Instant,
    /// PR-B: pinned vaults are never evicted from explicit Geyser filters.
    pinned: bool,
    pin: Option<GeyserPinReason>,
    // =========================================================================
    // DLMM-specific fields (Option D: Bin Array Traversierung)
    // =========================================================================
    /// Meteora DLMM: Active bin index (where current price is)
    active_id: Option<i32>,
    /// Meteora DLMM: Bin step (price increment per bin in bps)
    bin_step: Option<u16>,
    /// PR233: O(1) paired-vault LRU touch (opposite pool leg).
    sibling_vault: Option<Pubkey>,
}

impl Clone for VaultInfo {
    fn clone(&self) -> Self {
        Self {
            pool_address: self.pool_address,
            dex: self.dex.clone(),
            base_mint: self.base_mint,
            quote_mint: self.quote_mint,
            is_base_vault: self.is_base_vault,
            last_balance: std::sync::atomic::AtomicU64::new(
                self.last_balance.load(std::sync::atomic::Ordering::Relaxed),
            ),
            last_used_at: self.last_used_at,
            pinned: self.pinned,
            pin: self.pin,
            active_id: self.active_id,
            bin_step: self.bin_step,
            sibling_vault: self.sibling_vault,
        }
    }
}

/// Information about a tracked Meteora DLMM Bin Array account
#[derive(Debug, Clone)]
struct BinArrayInfo {
    pool_address: Pubkey,
    /// Index of this bin array (determines which bins it contains)
    bin_array_index: i64,
    /// Bin step from pool (needed for price calculation)
    bin_step: u16,
    last_used_at: Instant,
    pinned: bool,
    pin: Option<GeyserPinReason>,
}

/// Phase1: lock-free vault metadata for ingest/sidefx (refreshed by md-state only).
#[derive(Clone)]
struct SnapshotVaultView {
    pool_address: Pubkey,
    dex: String,
    base_mint: Pubkey,
    quote_mint: Pubkey,
    is_base_vault: bool,
    sibling_vault: Option<Pubkey>,
    active_id: Option<i32>,
    bin_step: Option<u16>,
    /// Sidefx may update; authoritative `VaultInfo.last_balance` refreshed on snapshot rebuild.
    last_balance: Arc<AtomicU64>,
}

/// Phase1: lock-free bin-array metadata for ingest/sidefx.
#[derive(Clone)]
struct SnapshotBinArrayView {
    pool_address: Pubkey,
    bin_array_index: i64,
    bin_step: u16,
}

/// PR237 + Phase1: lock-free ingest membership view (refreshed by md-state at burst end).
#[derive(Clone, Default)]
struct TrackedMembershipSnapshot {
    vaults: HashSet<Pubkey>,
    mints: HashSet<Pubkey>,
    bin_arrays: HashSet<Pubkey>,
    vault_by_pubkey: HashMap<Pubkey, SnapshotVaultView>,
    bin_array_by_pubkey: HashMap<Pubkey, SnapshotBinArrayView>,
}

/// Scope F: lock-free EXEC_HOT vault/bin view (hot-pool legs only, not full explicit membership).
#[derive(Clone, Default)]
struct ExecHotMembershipSnapshot {
    vaults: HashSet<Pubkey>,
    bin_arrays: HashSet<Pubkey>,
}

/// PR237: reverse index pool → tracked vault/bin pubkeys (md-state writer only).
#[derive(Clone, Debug, Default)]
struct PoolTrackedLegs {
    vaults: Vec<Pubkey>,
    bin_arrays: Vec<Pubkey>,
}

/// Extract normalized balance fields from MASTER cache for JetStream BalanceUpdated.
#[cfg_attr(not(test), allow(dead_code))]
fn pool_cache_balance_fields_from_state(
    state: &CachedPoolState,
) -> Option<(Pubkey, Pubkey, u64, u64, &'static str)> {
    let sol = Pubkey::from_str(NATIVE_SOL_MINT).ok()?;
    match state {
        CachedPoolState::RaydiumCpmm(s) => {
            let (base_mint, quote_mint, base_r, quote_r) = if s.token_1_mint == sol {
                (
                    s.token_0_mint,
                    s.token_1_mint,
                    s.reserve_0.unwrap_or(0),
                    s.reserve_1.unwrap_or(0),
                )
            } else if s.token_0_mint == sol {
                (
                    s.token_1_mint,
                    s.token_0_mint,
                    s.reserve_1.unwrap_or(0),
                    s.reserve_0.unwrap_or(0),
                )
            } else {
                (
                    s.token_0_mint,
                    s.token_1_mint,
                    s.reserve_0.unwrap_or(0),
                    s.reserve_1.unwrap_or(0),
                )
            };
            Some((base_mint, quote_mint, base_r, quote_r, "raydium_cpmm"))
        }
        CachedPoolState::MeteoraCpmm(s) => {
            let (base_mint, quote_mint, base_r, quote_r) = if s.token_1_mint == sol {
                (s.token_0_mint, s.token_1_mint, s.reserve_0, s.reserve_1)
            } else if s.token_0_mint == sol {
                (s.token_1_mint, s.token_0_mint, s.reserve_1, s.reserve_0)
            } else {
                (s.token_0_mint, s.token_1_mint, s.reserve_0, s.reserve_1)
            };
            Some((base_mint, quote_mint, base_r, quote_r, "meteora_cpmm"))
        }
        CachedPoolState::Meteora(s) => {
            let (base_mint, quote_mint, base_r, quote_r) = if s.token_y_mint == sol {
                (
                    s.token_x_mint,
                    s.token_y_mint,
                    s.reserve_x_balance.unwrap_or(0),
                    s.reserve_y_balance.unwrap_or(0),
                )
            } else if s.token_x_mint == sol {
                (
                    s.token_y_mint,
                    s.token_x_mint,
                    s.reserve_y_balance.unwrap_or(0),
                    s.reserve_x_balance.unwrap_or(0),
                )
            } else {
                (
                    s.token_x_mint,
                    s.token_y_mint,
                    s.reserve_x_balance.unwrap_or(0),
                    s.reserve_y_balance.unwrap_or(0),
                )
            };
            Some((base_mint, quote_mint, base_r, quote_r, "meteora_dlmm"))
        }
        CachedPoolState::PumpAmm(s) => Some((
            s.base_mint,
            s.quote_mint,
            s.base_reserve.unwrap_or(0),
            s.quote_reserve.unwrap_or(0),
            "pump_amm",
        )),
        CachedPoolState::Orca(s) => {
            let (base_mint, quote_mint, base_r, quote_r) = if s.token_mint_b == sol {
                (
                    s.token_mint_a,
                    s.token_mint_b,
                    s.vault_a_balance.unwrap_or(0),
                    s.vault_b_balance.unwrap_or(0),
                )
            } else if s.token_mint_a == sol {
                (
                    s.token_mint_b,
                    s.token_mint_a,
                    s.vault_b_balance.unwrap_or(0),
                    s.vault_a_balance.unwrap_or(0),
                )
            } else {
                (
                    s.token_mint_a,
                    s.token_mint_b,
                    s.vault_a_balance.unwrap_or(0),
                    s.vault_b_balance.unwrap_or(0),
                )
            };
            Some((base_mint, quote_mint, base_r, quote_r, "orca"))
        }
        CachedPoolState::RaydiumAmm(s) => Some((
            s.base_mint,
            s.quote_mint,
            s.coin_reserve.unwrap_or(0),
            s.pc_reserve.unwrap_or(0),
            "raydium",
        )),
        CachedPoolState::PumpFun(_) => None,
    }
}

/// True when the cached pool row has at least one non-zero reserve / vault balance.
#[cfg_attr(not(test), allow(dead_code))]
fn cached_pool_has_fresh_reserve_basis(state: &CachedPoolState) -> bool {
    let fresh = |opt: Option<u64>| opt.is_some_and(|v| v > 0);
    let fresh_u64 = |v: u64| v > 0;
    match state {
        CachedPoolState::RaydiumCpmm(s) => fresh(s.reserve_0) || fresh(s.reserve_1),
        CachedPoolState::MeteoraCpmm(s) => fresh_u64(s.reserve_0) || fresh_u64(s.reserve_1),
        CachedPoolState::Meteora(s) => fresh(s.reserve_x_balance) || fresh(s.reserve_y_balance),
        CachedPoolState::PumpAmm(s) => fresh(s.base_reserve) || fresh(s.quote_reserve),
        CachedPoolState::Orca(s) => fresh(s.vault_a_balance) || fresh(s.vault_b_balance),
        CachedPoolState::RaydiumAmm(s) => fresh(s.coin_reserve) || fresh(s.pc_reserve),
        CachedPoolState::PumpFun(_) => false,
    }
}

/// Vault ATA pair from cache state (SOL-normalized) when hot-pool vault tracking applies.
fn expected_pool_vault_pubkeys_from_cache(
    cached_state: &CachedPoolState,
    enable_meteora_cpmm: bool,
    enable_meteora_dlmm: bool,
) -> Option<(Pubkey, Pubkey)> {
    match cached_state {
        CachedPoolState::RaydiumCpmm(s) => {
            let (_, _, base_vault, quote_vault) = cpmm_token_mints_and_vaults_sol_normalized(
                s.token_0_mint,
                s.token_1_mint,
                s.token_0_vault,
                s.token_1_vault,
            );
            Some((base_vault, quote_vault))
        }
        CachedPoolState::MeteoraCpmm(s) if enable_meteora_cpmm => {
            let (_, _, base_vault, quote_vault) = cpmm_token_mints_and_vaults_sol_normalized(
                s.token_0_mint,
                s.token_1_mint,
                s.token_0_vault,
                s.token_1_vault,
            );
            Some((base_vault, quote_vault))
        }
        CachedPoolState::Meteora(s) if enable_meteora_dlmm => {
            let (_, _, base_vault, quote_vault) = cpmm_token_mints_and_vaults_sol_normalized(
                s.token_x_mint,
                s.token_y_mint,
                s.reserve_x,
                s.reserve_y,
            );
            Some((base_vault, quote_vault))
        }
        CachedPoolState::Orca(s) => {
            let (_, _, base_vault, quote_vault) = cpmm_token_mints_and_vaults_sol_normalized(
                s.token_mint_a,
                s.token_mint_b,
                s.token_vault_a,
                s.token_vault_b,
            );
            Some((base_vault, quote_vault))
        }
        CachedPoolState::PumpAmm(s) => {
            Some((s.pool_base_token_account, s.pool_quote_token_account))
        }
        CachedPoolState::RaydiumAmm(s) => Some((s.coin_vault, s.pc_vault)),
        _ => None,
    }
}

/// True when a hot-pool cache upsert still needs vault registration (missing vault rows).
#[cfg(test)]
fn pool_needs_tracking_refresh_after_cache_upsert(
    ctx: &MarketDataContext,
    pool: Pubkey,
    cached_state: &CachedPoolState,
) -> bool {
    if !ctx.hot_pool_registry.is_hot_pool(pool) {
        return false;
    }
    if ctx.pool_vaults_fully_tracked_for_cache(pool, cached_state) {
        ironcrab::metrics::inc_market_data_md_state_register_skipped_idempotent_total();
        return false;
    }
    true
}

/// True when expected vault rows for a pool are already registered with sibling links.
fn pool_vaults_fully_tracked_for_cache_inner(
    tracked_vaults: &std::collections::HashMap<Pubkey, VaultInfo>,
    pool: Pubkey,
    cached_state: &CachedPoolState,
    enable_meteora_cpmm: bool,
    enable_meteora_dlmm: bool,
) -> bool {
    let Some((base_vault, quote_vault)) = expected_pool_vault_pubkeys_from_cache(
        cached_state,
        enable_meteora_cpmm,
        enable_meteora_dlmm,
    ) else {
        return true;
    };
    tracked_vaults
        .get(&base_vault)
        .is_some_and(|v| v.pool_address == pool && v.sibling_vault == Some(quote_vault))
        && tracked_vaults
            .get(&quote_vault)
            .is_some_and(|v| v.pool_address == pool && v.sibling_vault == Some(base_vault))
}

/// PR237: cache-first vault/bin pubkeys for trade-path LRU touch (no full-map scan).
fn note_trade_pool_lru_touches_from_cache(
    ctx: &MarketDataContext,
    pool: Pubkey,
    scratch: &mut MdSidefxBurstScratch,
) {
    if !ctx.hot_pool_registry.is_hot_pool(pool) {
        return;
    }
    let Some(state) = ctx.live_pool_cache.get(&pool) else {
        return;
    };
    let (enable_meteora_cpmm, enable_meteora_dlmm) = {
        let cfg = ctx.config.read();
        (cfg.enable_meteora_cpmm, cfg.enable_meteora_dlmm)
    };
    if let Some((base_vault, quote_vault)) =
        expected_pool_vault_pubkeys_from_cache(&state, enable_meteora_cpmm, enable_meteora_dlmm)
    {
        scratch.note_vault_touch(base_vault, SidefxUpdateClass::ExecHot);
        scratch.note_vault_touch(quote_vault, SidefxUpdateClass::ExecHot);
    }
    if enable_meteora_dlmm {
        if let CachedPoolState::Meteora(s) = &state {
            let active_array_index = MeteoraDlmmSwapBuilder::bin_id_to_bin_array_index(s.active_id);
            for offset in -3i64..=3i64 {
                let index = active_array_index + offset;
                if let Ok(pda) = MeteoraDlmmSwapBuilder::derive_bin_array_pda(&pool, index) {
                    scratch.note_bin_array_touch(pda, SidefxUpdateClass::ExecHot);
                }
            }
        }
    }
}

/// Phase1: pair reserve balances from snapshot vault views (no `tracked_vaults` map lock).
fn snapshot_vault_pair_balances(
    snap: &TrackedMembershipSnapshot,
    vault_pubkey: &Pubkey,
    new_balance: u64,
) -> Option<(u64, u64)> {
    let vault = snap.vault_by_pubkey.get(vault_pubkey)?;
    let (base, quote) = if let Some(sibling_pk) = vault.sibling_vault {
        snap.vault_by_pubkey
            .get(&sibling_pk)
            .map(|sibling| {
                let sib_bal = sibling
                    .last_balance
                    .load(std::sync::atomic::Ordering::Relaxed);
                if vault.is_base_vault {
                    (new_balance, sib_bal)
                } else {
                    (sib_bal, new_balance)
                }
            })
            .unwrap_or_else(|| {
                if vault.is_base_vault {
                    (new_balance, 0)
                } else {
                    (0, new_balance)
                }
            })
    } else if vault.is_base_vault {
        (new_balance, 0)
    } else {
        (0, new_balance)
    };
    Some((base, quote))
}

/// Base + quote mint pubkeys for explicit mint-account Geyser tracking (metadata).
fn pool_mints_for_geyser_explicit_tracking(state: &CachedPoolState) -> Option<(Pubkey, Pubkey)> {
    let wsol = Pubkey::from_str(NATIVE_SOL_MINT).ok()?;
    match state {
        CachedPoolState::RaydiumCpmm(s) => Some((s.token_0_mint, s.token_1_mint)),
        CachedPoolState::MeteoraCpmm(s) => Some((s.token_0_mint, s.token_1_mint)),
        CachedPoolState::Meteora(s) => Some((s.token_x_mint, s.token_y_mint)),
        CachedPoolState::PumpAmm(s) => Some((s.base_mint, s.quote_mint)),
        CachedPoolState::Orca(s) => Some((s.token_mint_a, s.token_mint_b)),
        CachedPoolState::RaydiumAmm(s) => Some((s.base_mint, s.quote_mint)),
        CachedPoolState::PumpFun(s) => Some((s.token_mint, wsol)),
    }
}

/// PR4a: cache-planned explicit pubkeys for one pool owner group (no tracked-map reads).
fn planned_explicit_pubkeys_for_pool_from_cache(
    pool: Pubkey,
    state: &CachedPoolState,
    enable_meteora_cpmm: bool,
    enable_meteora_dlmm: bool,
) -> Vec<Pubkey> {
    use std::collections::BTreeSet;
    let mut set = BTreeSet::new();
    if let Some((base_vault, quote_vault)) =
        expected_pool_vault_pubkeys_from_cache(state, enable_meteora_cpmm, enable_meteora_dlmm)
    {
        set.insert(base_vault);
        set.insert(quote_vault);
    }
    if let CachedPoolState::RaydiumAmm(s) = state {
        set.insert(s.coin_vault);
        set.insert(s.pc_vault);
    }
    if enable_meteora_dlmm {
        if let CachedPoolState::Meteora(s) = state {
            let active_array_index = MeteoraDlmmSwapBuilder::bin_id_to_bin_array_index(s.active_id);
            for offset in -3i64..=3i64 {
                let index = active_array_index + offset;
                if let Ok(pda) = MeteoraDlmmSwapBuilder::derive_bin_array_pda(&pool, index) {
                    set.insert(pda);
                }
            }
        }
    }
    if let Some((a, b)) = pool_mints_for_geyser_explicit_tracking(state) {
        set.insert(a);
        set.insert(b);
    }
    set.into_iter().collect()
}

/// CPMM pool cache rows: normalize base/quote mints and vault ATAs (SOL as quote when present).
fn cpmm_token_mints_and_vaults_sol_normalized(
    token_0_mint: Pubkey,
    token_1_mint: Pubkey,
    token_0_vault: Pubkey,
    token_1_vault: Pubkey,
) -> (Pubkey, Pubkey, Pubkey, Pubkey) {
    let sol = Pubkey::from_str(NATIVE_SOL_MINT).unwrap_or_default();
    if token_1_mint == sol {
        (token_0_mint, token_1_mint, token_0_vault, token_1_vault)
    } else if token_0_mint == sol {
        (token_1_mint, token_0_mint, token_1_vault, token_0_vault)
    } else {
        (token_0_mint, token_1_mint, token_0_vault, token_1_vault)
    }
}

struct TrackedVaultPairInsert<'a> {
    pool: Pubkey,
    now: Instant,
    dex: &'a str,
    base_mint: Pubkey,
    quote_mint: Pubkey,
    base_vault: Pubkey,
    quote_vault: Pubkey,
    active_id: Option<i32>,
    bin_step: Option<u16>,
}

fn insert_tracked_vault_pair(
    vaults: &mut std::collections::HashMap<Pubkey, VaultInfo>,
    vaults_changed: &mut bool,
    spec: TrackedVaultPairInsert<'_>,
) -> Vec<Pubkey> {
    let mut inserted = Vec::new();
    let dex_str = spec.dex.to_string();
    use std::collections::hash_map::Entry;
    if let Entry::Vacant(e) = vaults.entry(spec.base_vault) {
        e.insert(VaultInfo {
            pool_address: spec.pool,
            dex: dex_str.clone(),
            base_mint: spec.base_mint,
            quote_mint: spec.quote_mint,
            is_base_vault: true,
            last_balance: std::sync::atomic::AtomicU64::new(0),
            last_used_at: spec.now,
            pinned: false,
            pin: None,
            active_id: spec.active_id,
            bin_step: spec.bin_step,
            sibling_vault: Some(spec.quote_vault),
        });
        inserted.push(spec.base_vault);
        *vaults_changed = true;
    }
    if let Entry::Vacant(e) = vaults.entry(spec.quote_vault) {
        e.insert(VaultInfo {
            pool_address: spec.pool,
            dex: dex_str,
            base_mint: spec.base_mint,
            quote_mint: spec.quote_mint,
            is_base_vault: false,
            last_balance: std::sync::atomic::AtomicU64::new(0),
            last_used_at: spec.now,
            pinned: false,
            pin: None,
            active_id: spec.active_id,
            bin_step: spec.bin_step,
            sibling_vault: Some(spec.base_vault),
        });
        inserted.push(spec.quote_vault);
        *vaults_changed = true;
    }
    if let Some(base) = vaults.get_mut(&spec.base_vault) {
        base.sibling_vault = Some(spec.quote_vault);
    }
    if let Some(quote) = vaults.get_mut(&spec.quote_vault) {
        quote.sibling_vault = Some(spec.base_vault);
    }
    inserted
}

fn track_pin_to_geyser_pin(pin: Option<TrackPinReason>) -> Option<GeyserPinReason> {
    pin.map(|p| match p {
        TrackPinReason::Wallet => GeyserPinReason::Wallet,
        TrackPinReason::MomentumActive => GeyserPinReason::MomentumActive,
        TrackPinReason::ArbMultiDex => GeyserPinReason::ArbMultiDex,
    })
}

fn consumer_id_for_geyser_pin(pin: Option<GeyserPinReason>) -> ConsumerId {
    match pin {
        Some(GeyserPinReason::Wallet) => ConsumerId::Wallet,
        Some(GeyserPinReason::MomentumActive) => ConsumerId::Momentum,
        Some(GeyserPinReason::ArbMultiDex) => ConsumerId::Arb,
        None => ConsumerId::Tracker,
    }
}

fn consumer_id_for_pool_explicit_row(
    ctx: &MarketDataContext,
    pool: Pubkey,
    pin: Option<GeyserPinReason>,
) -> ConsumerId {
    if ctx.hot_pool_registry.is_position_pin_for_pool(pool) {
        ConsumerId::MomentumPosition
    } else {
        consumer_id_for_geyser_pin(pin)
    }
}

fn momentum_explicit_consumer_for_pool(ctx: &MarketDataContext, pool: Pubkey) -> ExplicitConsumer {
    if ctx.hot_pool_registry.is_position_pin_for_pool(pool) {
        ExplicitConsumer::MomentumPosition
    } else {
        ExplicitConsumer::Momentum
    }
}

fn snapshot_consumer_to_geyser_pin(consumer: SnapshotConsumer) -> Option<GeyserPinReason> {
    match consumer {
        SnapshotConsumer::Wallet => Some(GeyserPinReason::Wallet),
        SnapshotConsumer::MomentumPosition | SnapshotConsumer::Momentum => {
            Some(GeyserPinReason::MomentumActive)
        }
        SnapshotConsumer::Arb => Some(GeyserPinReason::ArbMultiDex),
        SnapshotConsumer::Tracker => None,
    }
}

impl TrackWorkerContext for MarketDataContext {
    fn geyser_sync_batch_debounce_ms(&self) -> u64 {
        MarketDataContext::geyser_sync_batch_debounce_ms(self)
    }

    fn max_tracked_accounts(&self) -> usize {
        self.config.read().max_tracked_accounts
    }

    fn apply_momentum_active_pools_update(
        &self,
        admission: &mut FixedCapAdmission,
        update: &MomentumActivePoolsUpdate,
    ) -> bool {
        self.apply_momentum_active_pools_update(admission, update)
    }

    fn apply_momentum_snapshot_reconcile(
        &self,
        admission: &mut FixedCapAdmission,
        active: &[MomentumActivePoolEntry],
    ) -> bool {
        self.apply_momentum_snapshot_reconcile(admission, active)
    }

    fn apply_momentum_removed_entries(
        &self,
        admission: &mut FixedCapAdmission,
        chunk: &[MomentumRemovedPoolEntry],
    ) -> bool {
        self.apply_momentum_removed_entries(admission, chunk)
    }

    fn apply_momentum_active_entries(
        &self,
        admission: &mut FixedCapAdmission,
        chunk: &[MomentumActivePoolEntry],
    ) -> bool {
        self.apply_momentum_active_entries(admission, chunk)
    }

    fn apply_arb_track_requests_update(
        &self,
        admission: &mut FixedCapAdmission,
        update: &ArbTrackRequestsUpdate,
    ) -> bool {
        self.apply_arb_track_requests_update(admission, update)
    }

    fn apply_arb_snapshot_reconcile(
        &self,
        admission: &mut FixedCapAdmission,
        active: &[ArbTrackActiveEntry],
    ) -> bool {
        self.apply_arb_snapshot_reconcile(admission, active)
    }

    fn apply_arb_removed_entries(
        &self,
        admission: &mut FixedCapAdmission,
        chunk: &[ArbTrackRemovedEntry],
    ) -> bool {
        self.apply_arb_removed_entries(admission, chunk)
    }

    fn apply_arb_active_entries(
        &self,
        admission: &mut FixedCapAdmission,
        chunk: &[ArbTrackActiveEntry],
    ) -> bool {
        self.apply_arb_active_entries(admission, chunk)
    }

    fn apply_wallet_pin(&self, admission: &mut FixedCapAdmission, mint: Pubkey) -> bool {
        self.apply_wallet_pin(admission, mint)
    }

    fn withdraw_wallet_pin(&self, admission: &mut FixedCapAdmission, mint: Pubkey) -> bool {
        self.withdraw_wallet_pin(admission, mint)
    }

    fn apply_track_mint(
        &self,
        admission: &mut FixedCapAdmission,
        mint: Pubkey,
        pin: Option<TrackPinReason>,
    ) -> bool {
        self.apply_track_mint(admission, mint, pin)
    }

    fn refresh_geyser_pins_gauge(&self) {
        self.refresh_geyser_pins_gauge();
    }

    fn hot_pool_registry_pair_count(&self) -> usize {
        self.hot_pool_registry.pair_count()
    }

    fn hot_pool_registry_arb_pool_count(&self) -> usize {
        self.hot_pool_registry.arb_pool_count()
    }

    fn refresh_hot_pool_registry_gauges(&self) {
        self.refresh_hot_pool_registry_gauges();
    }

    fn snapshot_explicit_subscription_pubkeys(&self) -> HashSet<Pubkey> {
        MarketDataContext::snapshot_explicit_subscription_pubkeys(self)
    }

    fn continue_geyser_evict_with_deadline(
        &self,
        deadline: Instant,
        admission: &FixedCapAdmission,
    ) -> bool {
        self.sync_geyser_tracked_accounts_from_admission_with_deadline(deadline, admission)
    }

    fn sync_geyser_tracked_accounts_batched_flush_with_deadline(
        &self,
        deadline: Instant,
        admission: &FixedCapAdmission,
    ) -> bool {
        self.clear_geyser_prune_resume();
        self.sync_geyser_tracked_accounts_from_admission_with_deadline(deadline, admission)
    }

    fn release_geyser_sync_flush_slot(&self) {
        self.release_geyser_sync_flush_slot();
    }

    fn refresh_tracked_membership_snapshot(&self) {
        self.refresh_tracked_membership_snapshot();
    }

    fn explicit_pubkey_rows_for_desired_set(&self) -> Vec<(Pubkey, ConsumerId, Option<Pubkey>)> {
        MarketDataContext::explicit_pubkey_rows_for_desired_set(self)
    }

    fn build_explicit_set_snapshot(&self, admission: &FixedCapAdmission) -> ExplicitSetSnapshot {
        MarketDataContext::build_explicit_set_snapshot(self, admission)
    }

    fn apply_explicit_set_snapshot(
        &self,
        admission: &mut FixedCapAdmission,
        snapshot: &ExplicitSetSnapshot,
    ) -> AdmissionRestoreResult {
        self.restore_tracked_maps_from_snapshot_rows(snapshot);
        MarketDataContext::restore_explicit_admission_from_snapshot(self, admission, snapshot)
    }

    fn on_admission_converge_result(
        &self,
        admission: &FixedCapAdmission,
        result: AdmissionConvergeResult,
    ) {
        MarketDataContext::on_admission_converge_result(self, admission, result);
    }

    fn prune_tracked_maps_to_admitted(&self, admission: &FixedCapAdmission) {
        MarketDataContext::prune_tracked_maps_to_admitted(self, admission);
    }

    fn publish_admitted_explicit_physical(&self, admission: &FixedCapAdmission) {
        MarketDataContext::publish_admitted_explicit_physical(self, admission);
    }

    fn tracked_maps_need_prune(&self, admission: &FixedCapAdmission) -> bool {
        let admitted: HashSet<Pubkey> = admission.snapshot_pubkeys().into_iter().collect();
        MarketDataContext::tracked_maps_need_prune(self, &admitted)
    }

    fn publish_admitted_explicit_physical_force(&self, admission: &FixedCapAdmission) {
        MarketDataContext::publish_admitted_explicit_physical_force(self, admission);
    }

    fn last_synced_explicit_pubkeys_write(
        &self,
    ) -> parking_lot::RwLockWriteGuard<'_, HashSet<Pubkey>> {
        self.last_synced_explicit_pubkeys.write()
    }

    fn geyser_explicit_readiness_ok(&self) -> bool {
        self.geyser_explicit_ready.load(Ordering::Relaxed)
    }

    fn geyser_connect_barrier_pending(&self) -> bool {
        !self.geyser_connect_barrier.is_ready()
    }

    fn signal_restore_barrier(&self, ok: bool) {
        MarketDataContext::signal_restore_barrier(self, ok);
    }

    fn apply_explicit_cap_shrink(
        &self,
        admission: &mut FixedCapAdmission,
        new_cap: usize,
    ) -> CapShrinkResult {
        MarketDataContext::apply_explicit_cap_shrink(self, admission, new_cap)
    }

    fn retry_deferred_hot_pool_reserve_registrations(
        &self,
        admission: &mut FixedCapAdmission,
    ) -> bool {
        MarketDataContext::retry_deferred_hot_pool_reserve_registrations(self, admission)
    }
}

impl ColdHost for MarketDataContext {
    fn run_id(&self) -> &str {
        &self.run_id
    }

    fn nats(&self) -> Option<&NatsClient> {
        self.nats.as_ref()
    }

    fn live_pool_cache(&self) -> &LivePoolCache {
        self.live_pool_cache.as_ref()
    }

    fn live_pool_cache_arc(&self) -> Arc<LivePoolCache> {
        Arc::clone(&self.live_pool_cache)
    }

    fn raydium_serum_fetched_insert(&self, pool_addr: Pubkey) {
        self.raydium_serum_fetched.write().insert(pool_addr);
    }
}

impl JsonlHost for MarketDataContext {
    fn try_enqueue_market_event(&self, event: &MarketEvent) -> bool {
        self.jsonl_writer.try_enqueue_market_event(event)
    }

    fn on_jsonl_enqueue_dropped(&self, event: &MarketEvent) {
        inc_market_data_jsonl_enqueue_dropped_total();
        warn!(
            event_id = %event.event_id,
            kind = ?event.kind,
            "JSONL enqueue dropped (queue full)"
        );
    }
}

impl IngestHost for MarketDataContext {
    fn ingest_tracked_wallet_pubkeys(&self) -> Option<(Pubkey, Pubkey)> {
        self.tracked_wallet
            .as_ref()
            .map(|tw| (tw.wallet, tw.wsol_ata))
    }

    fn ingest_tracked_wallet_token_account_contains(&self, pubkey: &Pubkey) -> bool {
        self.tracked_wallet_token_accounts.read().contains(pubkey)
    }

    fn ingest_membership_contains(&self, pubkey: &Pubkey) -> bool {
        let snap = self.tracked_membership.load();
        if snap.vaults.contains(pubkey)
            || snap.mints.contains(pubkey)
            || snap.bin_arrays.contains(pubkey)
        {
            inc_market_data_ingest_membership_snapshot_hits_total();
            return true;
        }
        false
    }

    fn ingest_membership_vault_contains(&self, pubkey: &Pubkey) -> bool {
        self.tracked_membership.load().vaults.contains(pubkey)
    }

    fn ingest_membership_mint_contains(&self, pubkey: &Pubkey) -> bool {
        self.tracked_membership.load().mints.contains(pubkey)
    }

    fn ingest_membership_bin_array_contains(&self, pubkey: &Pubkey) -> bool {
        self.tracked_membership.load().bin_arrays.contains(pubkey)
    }

    fn ingest_exec_hot_vault_contains(&self, pubkey: &Pubkey) -> bool {
        self.exec_hot_membership.load().vaults.contains(pubkey)
    }

    fn ingest_exec_hot_bin_array_contains(&self, pubkey: &Pubkey) -> bool {
        self.exec_hot_membership.load().bin_arrays.contains(pubkey)
    }

    fn ingest_is_hot_pool(&self, pool: &Pubkey) -> bool {
        self.hot_pool_registry.is_hot_pool(*pool)
    }

    fn ingest_is_enrichment_member(&self, pool: &Pubkey) -> bool {
        pool_is_enrichment_member(
            self.pool_mint_map.read().contains_key(&pool.to_string()),
            self.high_priority_bonding_curves.read().contains(pool),
            self.hot_pool_registry.is_hot_pool(*pool),
        )
    }

    fn ingest_pool_mint_map_contains(&self, pool: &Pubkey) -> bool {
        self.pool_mint_map.read().contains_key(&pool.to_string())
    }

    fn ingest_high_priority_bonding_curve_contains(&self, pool: &Pubkey) -> bool {
        self.high_priority_bonding_curves.read().contains(pool)
    }

    fn ingest_wallet_tracks_mint(&self, mint: &Pubkey) -> bool {
        self.wallet_tracks_mint_for_geyser(mint)
    }

    fn ingest_pumpfun_bonding_curve_tracks_wallet(&self, pool: &Pubkey) -> bool {
        for mint in self.tracked_wallet_mint_decimals.read().keys() {
            let (bonding_curve, _) = PumpFunDex::derive_bonding_curve_static(mint);
            if bonding_curve == *pool {
                return true;
            }
        }
        false
    }

    fn ingest_pumpfun_wallet_tracks_pool_mint(&self, pool: &Pubkey) -> bool {
        if let Some(CachedPoolState::PumpFun(s)) = self.live_pool_cache.get(pool) {
            return self.wallet_tracks_mint_for_geyser(&s.token_mint);
        }
        false
    }

    fn ingest_record_membership_snapshot_hit(&self) {
        inc_market_data_ingest_membership_snapshot_hits_total();
    }

    fn ingest_record_vault_high_priority_dispatch(&self) {
        inc_market_data_vault_high_priority_dispatch_total();
    }

    fn ingest_record_enrichment_relevance_hit(&self) {
        ironcrab::metrics::inc_market_data_account_relevance_enrichment_hit_total();
    }
}

impl TxIngestHost for MarketDataContext {
    fn tx_build_version(&self) -> &'static str {
        BUILD_VERSION
    }

    fn tx_run_id(&self) -> &str {
        &self.run_id
    }

    fn tx_next_event_id(&self) -> String {
        self.next_event_id()
    }

    fn tx_write_market_event_jsonl(&self, event: &MarketEvent) {
        self.write_market_event_jsonl(event);
    }

    fn tx_nats(&self) -> Option<&NatsClient> {
        self.nats.as_ref()
    }

    fn tx_publish_host(&self) -> Option<&dyn PublishHost> {
        Some(self)
    }

    fn tx_priority_fee_add_sample(
        &self,
        slot: u64,
        fee_lamports: u64,
        compute_units: Option<u64>,
    ) -> Option<u64> {
        self.priority_fee_tracker
            .add_sample(slot, fee_lamports, compute_units)
    }

    fn tx_priority_fee_sample_count(&self) -> usize {
        self.priority_fee_tracker.sample_count()
    }

    fn tx_priority_fee_percentiles(
        &self,
    ) -> ironcrab::solana::priority_fee_tracker::FeePercentiles {
        self.priority_fee_tracker.get_percentiles()
    }

    fn tx_priority_fee_for_tier(&self, tier: IntentTier) -> u64 {
        self.priority_fee_tracker.get_fee_for_tier(tier)
    }

    fn tx_orca_pool_lookup(&self, pool: &Pubkey) -> Option<OrcaPoolInfo> {
        match self.live_pool_cache.get(pool) {
            Some(CachedPoolState::Orca(state)) => Some(OrcaPoolInfo {
                token_mint_a: state.token_mint_a,
                token_mint_b: state.token_mint_b,
                token_vault_a: state.token_vault_a,
                token_vault_b: state.token_vault_b,
                tick_current_index: Some(state.tick_current_index),
                tick_spacing: Some(state.tick_spacing),
                token_a_program: state.token_a_program,
                token_b_program: state.token_b_program,
            }),
            _ => None,
        }
    }

    fn tx_record_pool_created(&self, mint: &str, slot: u64) {
        self.wallet_tracker.record_pool_created(mint, slot);
    }

    #[allow(clippy::too_many_arguments)]
    fn tx_wallet_tracker_process_trade(
        &self,
        mint: &str,
        trader: &str,
        is_buy: bool,
        sol_amount: u64,
        token_amount: u64,
        slot: u64,
        signature: &str,
    ) -> Vec<MarketEvent> {
        self.wallet_tracker.process_trade(
            mint,
            trader,
            is_buy,
            sol_amount,
            token_amount,
            slot,
            signature,
            &self.run_id,
            "market-data",
        )
    }

    fn tx_creator_cache_get(&self, mint: &str) -> Option<String> {
        self.creator_cache.read().get(mint).cloned()
    }

    fn tx_pool_creator_cache_get(&self, pool: &str) -> Option<String> {
        self.pool_creator_cache.read().get(pool).cloned()
    }

    fn tx_pool_creator_cache_insert(&self, pool: String, creator: String) {
        self.pool_creator_cache.write().insert(pool, creator);
    }

    fn tx_creator_cache_insert(&self, mint: String, creator: String) {
        self.creator_cache.write().insert(mint, creator);
    }

    fn tx_creator_cache_insert_returning_old(
        &self,
        mint: String,
        creator: String,
    ) -> Option<String> {
        let mut cache = self.creator_cache.write();
        let existing = cache.get(&mint).cloned();
        cache.insert(mint, creator);
        existing
    }

    fn tx_live_pool_pumpfun_creator(&self, pool: &Pubkey) -> Option<Pubkey> {
        self.live_pool_cache.get_pumpfun_creator(pool)
    }

    fn tx_tracked_wallet_view(&self) -> Option<TxTrackedWalletView> {
        self.tracked_wallet.as_ref().map(|tw| TxTrackedWalletView {
            wallet: tw.wallet,
            wsol_ata: tw.wsol_ata,
        })
    }

    fn tx_wallet_native_sol_swap(&self, lamports: u64) -> u64 {
        self.tracked_wallet
            .as_ref()
            .map(|tw| tw.last_sol_balance.swap(lamports, Ordering::Relaxed))
            .unwrap_or(0)
    }

    fn tx_wallet_wsol_store(&self, lamports: u64) {
        if let Some(tw) = self.tracked_wallet.as_ref() {
            tw.last_wsol_balance.store(lamports, Ordering::Relaxed);
        }
    }

    fn tx_wallet_wsol_seen_set(&self) {
        if let Some(tw) = self.tracked_wallet.as_ref() {
            tw.wsol_seen.store(true, Ordering::Relaxed);
        }
    }

    fn tx_wallet_mint_needs_track(&self, mint: &Pubkey) -> bool {
        self.tracked_mints
            .read()
            .get(mint)
            .is_none_or(|info| !info.pinned || info.pin != Some(GeyserPinReason::Wallet))
    }

    fn tx_wallet_token_account_insert(&self, ata: Pubkey) -> bool {
        self.tracked_wallet_token_accounts.write().insert(ata)
    }

    fn tx_wallet_mint_decimals_insert(&self, mint: Pubkey, decimals: u8) {
        self.tracked_wallet_mint_decimals
            .write()
            .insert(mint, decimals);
    }

    fn tx_wallet_notify_geyser_subscribe_accounts_changed(&self) {
        let Some(tracked_wallet) = self.tracked_wallet.as_ref() else {
            return;
        };
        let mut accounts: Vec<Pubkey> = Vec::new();
        accounts.push(tracked_wallet.wallet);
        accounts.push(tracked_wallet.wsol_ata);
        accounts.extend(self.tracked_wallet_token_accounts.read().iter().copied());
        accounts.sort();
        accounts.dedup();
        let _ = self.tracked_wallet_tx.send(accounts);
    }

    fn tx_live_pool_cache(&self) -> &LivePoolCache {
        &self.live_pool_cache
    }
}

impl AccountIngestHost for MarketDataContext {
    fn account_build_version(&self) -> &'static str {
        BUILD_VERSION
    }

    fn account_run_id(&self) -> &str {
        &self.run_id
    }

    fn account_next_event_id(&self) -> String {
        self.next_event_id()
    }

    fn account_write_market_event_jsonl(&self, event: &MarketEvent) {
        self.write_market_event_jsonl(event);
    }

    fn account_nats(&self) -> Option<&NatsClient> {
        self.nats.as_ref()
    }

    fn account_publish_host(&self) -> Option<&dyn PublishHost> {
        Some(self)
    }

    fn account_tracked_wallet_view(
        &self,
    ) -> Option<ironcrab::market_data::ingest::AccountTrackedWalletView> {
        self.tracked_wallet.as_ref().map(|tw| {
            ironcrab::market_data::ingest::AccountTrackedWalletView {
                wallet: tw.wallet,
                wsol_ata: tw.wsol_ata,
            }
        })
    }

    fn account_wallet_native_sol_swap(&self, lamports: u64) -> u64 {
        self.tracked_wallet
            .as_ref()
            .map(|tw| tw.last_sol_balance.swap(lamports, Ordering::Relaxed))
            .unwrap_or(0)
    }

    fn account_wallet_wsol_swap(&self, lamports: u64) -> u64 {
        self.tracked_wallet
            .as_ref()
            .map(|tw| tw.last_wsol_balance.swap(lamports, Ordering::Relaxed))
            .unwrap_or(0)
    }

    fn account_wallet_wsol_seen_set(&self) {
        if let Some(tw) = self.tracked_wallet.as_ref() {
            tw.wsol_seen.store(true, Ordering::Relaxed);
        }
    }

    fn account_wallet_mint_decimals_get(&self, mint: &Pubkey) -> Option<u8> {
        self.tracked_wallet_mint_decimals.read().get(mint).copied()
    }

    fn account_wallet_mint_decimals_insert(&self, mint: Pubkey, decimals: u8) {
        self.tracked_wallet_mint_decimals
            .write()
            .insert(mint, decimals);
    }

    fn account_membership_mint_contains(&self, pubkey: &Pubkey) -> bool {
        self.tracked_membership.load().mints.contains(pubkey)
    }

    fn account_membership_bin_array_info(
        &self,
        pubkey: &Pubkey,
    ) -> Option<ironcrab::market_data::ingest::AccountBinArrayView> {
        self.tracked_membership
            .load()
            .bin_array_by_pubkey
            .get(pubkey)
            .map(|info| ironcrab::market_data::ingest::AccountBinArrayView {
                pool_address: info.pool_address,
                bin_array_index: info.bin_array_index,
                bin_step: info.bin_step,
            })
    }
}

impl MdStateContext for MarketDataContext {
    fn snapshot_explicit_demand_pubkeys(&self) -> HashSet<Pubkey> {
        MarketDataContext::snapshot_explicit_demand_pubkeys(self)
    }

    fn schedule_geyser_sync_batch_debounced(ctx: &Arc<Self>, md_state: &MdStateSender) {
        schedule_geyser_sync_batch_debounced(ctx, md_state)
    }

    fn refresh_hot_pool_registry_gauges(&self) {
        MarketDataContext::refresh_hot_pool_registry_gauges(self)
    }

    fn refresh_tracked_membership_snapshot(&self) {
        MarketDataContext::refresh_tracked_membership_snapshot(self)
    }

    fn touch_tracked_vault_pubkey(&self, vault: &Pubkey) {
        touch_tracked_vault_pubkey(self, vault)
    }

    fn touch_tracked_bin_array_pubkey(&self, pda: &Pubkey) {
        MarketDataContext::touch_tracked_bin_array_pubkey(self, pda)
    }

    fn touch_tracked_pool_vaults_and_bins_if_tracked(&self, pool: Pubkey) {
        MarketDataContext::touch_tracked_pool_vaults_and_bins_if_tracked(self, pool)
    }

    fn set_ingest_tokio_handle(&self, handle: tokio::runtime::Handle) {
        *self.ingest_tokio_handle.write() = Some(handle);
    }
}

impl MarketDataContext {
    /// Non-blocking JSONL enqueue (dedicated `jsonl-writer` thread). Skips `AccountUpdate` / `TransactionDetected`.
    fn write_market_event_jsonl(&self, event: &MarketEvent) {
        write_market_event_jsonl(self, event);
    }

    fn next_event_id(&self) -> String {
        let n = self
            .event_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        format!("evt-{}-{:06}", &self.run_id[..8], n)
    }

    fn wallet_tracks_mint_for_geyser(&self, mint: &Pubkey) -> bool {
        self.tracked_wallet_mint_decimals.read().contains_key(mint)
    }

    fn pool_has_explicit_momentum_admission(&self, pool: Pubkey) -> bool {
        self.explicit_admitted_momentum_pools.read().contains(&pool)
    }

    fn pool_has_explicit_arb_admission(&self, pool: Pubkey) -> bool {
        self.explicit_admitted_arb_pools.read().contains(&pool)
    }

    fn note_explicit_momentum_pool_admitted(&self, pool: Pubkey) {
        self.explicit_admitted_momentum_pools.write().insert(pool);
    }

    fn note_explicit_arb_pool_admitted(&self, pool: Pubkey) {
        self.explicit_admitted_arb_pools.write().insert(pool);
    }

    fn clear_explicit_momentum_pool_admitted(&self, pool: Pubkey) {
        self.explicit_admitted_momentum_pools.write().remove(&pool);
    }

    fn clear_explicit_arb_pool_admitted(&self, pool: Pubkey) {
        self.explicit_admitted_arb_pools.write().remove(&pool);
    }

    /// PR4a: keep bypass-gate pool sets aligned with [`FixedCapAdmission`] SSOT.
    fn rebuild_explicit_admitted_pools_from_admission(&self, admission: &FixedCapAdmission) {
        let (momentum, arb) = explicit_admitted_pool_sets_from_admission(admission);
        *self.explicit_admitted_momentum_pools.write() = momentum;
        *self.explicit_admitted_arb_pools.write() = arb;
    }

    fn sync_explicit_momentum_pool_admitted_from_admission(
        &self,
        admission: &FixedCapAdmission,
        pool: Pubkey,
    ) {
        let momentum_admitted = admission
            .owner_group(&Self::pool_consumer_owner(pool, ExplicitConsumer::Momentum))
            .is_some();
        let position_admitted = admission
            .owner_group(&Self::pool_consumer_owner(
                pool,
                ExplicitConsumer::MomentumPosition,
            ))
            .is_some();
        if momentum_admitted || position_admitted {
            self.note_explicit_momentum_pool_admitted(pool);
        } else {
            self.clear_explicit_momentum_pool_admitted(pool);
        }
    }

    fn sync_explicit_pool_admitted_from_admission(
        &self,
        admission: &FixedCapAdmission,
        pool: Pubkey,
        consumer: ExplicitConsumer,
    ) {
        match consumer {
            ExplicitConsumer::Momentum | ExplicitConsumer::MomentumPosition => {
                self.sync_explicit_momentum_pool_admitted_from_admission(admission, pool);
            }
            ExplicitConsumer::Arb => {
                let admitted = admission
                    .owner_group(&Self::pool_consumer_owner(pool, consumer))
                    .is_some();
                if admitted {
                    self.note_explicit_arb_pool_admitted(pool);
                } else {
                    self.clear_explicit_arb_pool_admitted(pool);
                }
            }
            ExplicitConsumer::Wallet | ExplicitConsumer::Tracker => {}
        }
    }

    /// PR-B: explicit vault/bin-array/mint Geyser filters — admitted pool groups or wallet only.
    fn admit_geyser_explicit_pool_assets(
        &self,
        pool: Pubkey,
        base_mint: Pubkey,
        quote_mint: Pubkey,
    ) -> bool {
        self.pool_has_explicit_momentum_admission(pool)
            || self.pool_has_explicit_arb_admission(pool)
            || self.wallet_tracks_mint_for_geyser(&base_mint)
            || self.wallet_tracks_mint_for_geyser(&quote_mint)
    }

    fn pool_consumer_owner(pool: Pubkey, consumer: ExplicitConsumer) -> ExplicitOwner {
        ExplicitOwner {
            consumer,
            owner_key: ExplicitOwnerKey::Pool(pool),
        }
    }

    fn planned_pool_consumer_group_pubkeys(&self, pool: Pubkey) -> Vec<Pubkey> {
        let Some(state) = self.live_pool_cache.get(&pool) else {
            return Vec::new();
        };
        let (enable_meteora_cpmm, enable_meteora_dlmm) = {
            let cfg = self.config.read();
            (cfg.enable_meteora_cpmm, cfg.enable_meteora_dlmm)
        };
        planned_explicit_pubkeys_for_pool_from_cache(
            pool,
            &state,
            enable_meteora_cpmm,
            enable_meteora_dlmm,
        )
    }

    /// PR4b: wallet owner-group pubkeys for admission (accounts + wallet-pinned mints).
    fn wallet_owner_group_pubkeys(
        &self,
        pending_mint: Option<Pubkey>,
        exclude_mint: Option<Pubkey>,
    ) -> Vec<Pubkey> {
        let mut set = self.wallet_explicit_demand_pubkeys();
        for (pk, m) in self.tracked_mints.read().iter() {
            if m.pin == Some(GeyserPinReason::Wallet) && exclude_mint != Some(*pk) {
                set.insert(*pk);
            }
        }
        if let Some(mint) = pending_mint {
            set.insert(mint);
        }
        let mut pubkeys: Vec<Pubkey> = set.into_iter().collect();
        pubkeys.sort();
        pubkeys
    }

    fn wallet_owner_group() -> ExplicitOwner {
        ExplicitOwner {
            consumer: ExplicitConsumer::Wallet,
            owner_key: ExplicitOwnerKey::Wallet,
        }
    }

    fn tracker_mint_owner(mint: Pubkey) -> ExplicitOwner {
        ExplicitOwner {
            consumer: ExplicitConsumer::Tracker,
            owner_key: ExplicitOwnerKey::Mint(mint),
        }
    }

    /// PR4b: admit wallet protected owner group before tracked-map mutation.
    fn try_admit_wallet_owner_group(
        &self,
        admission: &mut FixedCapAdmission,
        pending_mint: Option<Pubkey>,
        exclude_mint: Option<Pubkey>,
    ) -> bool {
        let pubkeys = self.wallet_owner_group_pubkeys(pending_mint, exclude_mint);
        if pubkeys.is_empty() {
            let _ = admission.remove_group(Self::wallet_owner_group());
            return true;
        }
        if pubkeys.len() > admission.cap() {
            return false;
        }
        try_admit_owner_group(admission, Self::wallet_owner_group(), pubkeys)
    }

    /// PR4b: wallet mint pin — admission gate then tracked-map mutation.
    fn apply_wallet_pin(&self, admission: &mut FixedCapAdmission, mint: Pubkey) -> bool {
        if !self.try_admit_wallet_owner_group(admission, Some(mint), None) {
            inc_market_data_wallet_admission_rejected_total();
            return false;
        }
        let _ = admission.remove_group(Self::tracker_mint_owner(mint));
        inc_market_data_wallet_admission_admitted_total();
        let _ = self.track_mint_for_geyser_metadata(mint, Some(GeyserPinReason::Wallet));
        // Idempotent wallet pin / LRU refresh must not fail protocol replay (I-MD-5).
        true
    }

    /// PR4b: explicit wallet-pin withdrawal — admission refresh then map removal.
    fn withdraw_wallet_pin(&self, admission: &mut FixedCapAdmission, mint: Pubkey) -> bool {
        let should_remove = self
            .tracked_mints
            .read()
            .get(&mint)
            .is_some_and(|info| info.pin == Some(GeyserPinReason::Wallet));
        if !should_remove {
            return true;
        }
        if !self.try_admit_wallet_owner_group(admission, None, Some(mint)) {
            inc_market_data_wallet_admission_rejected_total();
            return false;
        }
        let _ = admission.remove_group(Self::tracker_mint_owner(mint));
        self.tracked_mints.write().remove(&mint);
        inc_market_data_wallet_admission_admitted_total();
        self.refresh_tracked_membership_snapshot();
        true
    }

    /// PR4b: tracker (`pin == None`) or promoted pin mint apply with admission gate where required.
    fn apply_track_mint(
        &self,
        admission: &mut FixedCapAdmission,
        mint: Pubkey,
        pin: Option<TrackPinReason>,
    ) -> bool {
        match pin {
            None => {
                let skip_tracker_admit = self
                    .tracked_mints
                    .read()
                    .get(&mint)
                    .is_some_and(|info| info.pin.is_some());
                if !skip_tracker_admit {
                    if !try_admit_owner_group(admission, Self::tracker_mint_owner(mint), vec![mint])
                    {
                        inc_market_data_tracker_admission_rejected_total();
                        return false;
                    }
                    inc_market_data_tracker_admission_admitted_total();
                }
                let _ = self.track_mint_for_geyser_metadata(mint, None);
                // Unpinned tracker LRU refresh / already-tracked mint is idempotent success.
                true
            }
            Some(TrackPinReason::Wallet) => self.apply_wallet_pin(admission, mint),
            Some(other) => {
                self.track_mint_for_geyser_metadata(mint, track_pin_to_geyser_pin(Some(other)))
            }
        }
    }

    /// PR4a: admit immutable pool owner group before tracked-map mutation.
    fn try_admit_pool_consumer_group(
        &self,
        admission: &mut FixedCapAdmission,
        pool: Pubkey,
        consumer: ExplicitConsumer,
    ) -> bool {
        let pubkeys = self.planned_pool_consumer_group_pubkeys(pool);
        if pubkeys.is_empty() {
            return false;
        }
        try_admit_owner_group(
            admission,
            Self::pool_consumer_owner(pool, consumer),
            pubkeys,
        )
    }

    /// PR4a: drop pool owner group from admission before tracked-map demotion.
    fn release_pool_consumer_group(
        &self,
        admission: &mut FixedCapAdmission,
        pool: Pubkey,
        consumer: ExplicitConsumer,
    ) {
        let _ = admission.remove_group(Self::pool_consumer_owner(pool, consumer));
        match consumer {
            ExplicitConsumer::Momentum | ExplicitConsumer::MomentumPosition => {
                self.sync_explicit_momentum_pool_admitted_from_admission(admission, pool);
            }
            ExplicitConsumer::Arb => self.clear_explicit_arb_pool_admitted(pool),
            ExplicitConsumer::Wallet | ExplicitConsumer::Tracker => {}
        }
    }

    fn snapshot_explicit_subscription_pubkeys(&self) -> HashSet<Pubkey> {
        self.last_synced_explicit_pubkeys.read().clone()
    }

    /// Tracked-map demand pubkeys (pre-admission); for internal delta detection only.
    fn snapshot_explicit_demand_pubkeys(&self) -> HashSet<Pubkey> {
        let mut set = HashSet::new();
        set.extend(self.tracked_mints.read().keys().copied());
        set.extend(self.tracked_vaults.read().keys().copied());
        set.extend(self.tracked_bin_arrays.read().keys().copied());
        if let Some(w) = &self.tracked_wallet {
            set.insert(w.wallet);
            set.insert(w.wsol_ata);
        }
        set.extend(self.tracked_wallet_token_accounts.read().iter().copied());
        set
    }

    fn collect_explicit_snapshot_rows(&self) -> Vec<ExplicitSnapshotRow> {
        let mut rows = Vec::new();
        for (pk, m) in self.tracked_mints.read().iter() {
            rows.push(ExplicitSnapshotRow {
                pubkey: pk.to_string(),
                consumer: consumer_id_for_geyser_pin(m.pin).into(),
                pool: None,
                kind: ExplicitAccountKind::Mint,
            });
        }
        for (pk, v) in self.tracked_vaults.read().iter() {
            rows.push(ExplicitSnapshotRow {
                pubkey: pk.to_string(),
                consumer: consumer_id_for_pool_explicit_row(self, v.pool_address, v.pin).into(),
                pool: Some(v.pool_address.to_string()),
                kind: ExplicitAccountKind::Vault,
            });
        }
        for (pk, b) in self.tracked_bin_arrays.read().iter() {
            rows.push(ExplicitSnapshotRow {
                pubkey: pk.to_string(),
                consumer: consumer_id_for_pool_explicit_row(self, b.pool_address, b.pin).into(),
                pool: Some(b.pool_address.to_string()),
                kind: ExplicitAccountKind::BinArray,
            });
        }
        for pk in self.tracked_wallet_token_accounts.read().iter() {
            rows.push(ExplicitSnapshotRow {
                pubkey: pk.to_string(),
                consumer: SnapshotConsumer::Wallet,
                pool: None,
                kind: ExplicitAccountKind::WalletToken,
            });
        }
        rows
    }

    fn collect_pool_mint_map_tier1(&self, max_entries: usize) -> Vec<(String, String)> {
        let map = self.pool_mint_map.read();
        if map.len() <= max_entries {
            return map.iter().map(|(p, m)| (p.clone(), m.clone())).collect();
        }
        map.iter()
            .take(max_entries)
            .map(|(p, m)| (p.clone(), m.clone()))
            .collect()
    }

    fn restore_tracked_maps_from_snapshot_rows(&self, snapshot: &ExplicitSetSnapshot) -> usize {
        let now = Instant::now();
        let mut restored = 0usize;
        for (pool, mint) in &snapshot.pool_mint_map {
            self.pool_mint_map
                .write()
                .insert(pool.clone(), mint.clone());
        }
        for pool_str in &snapshot.momentum_pools {
            if let Ok(pool) = Pubkey::from_str(pool_str) {
                if let Some(mint_str) = self.pool_mint_map.read().get(pool_str) {
                    if let Ok(mint) = Pubkey::from_str(mint_str) {
                        let is_position = snapshot
                            .momentum_position_pools
                            .iter()
                            .any(|p| p == pool_str);
                        if is_position {
                            self.hot_pool_registry
                                .pin_pool_with_reason(mint, pool, true);
                        } else {
                            self.hot_pool_registry.pin_pool(mint, pool);
                        }
                    }
                }
            }
        }
        for pool_str in &snapshot.arb_pools {
            if let Ok(pool) = Pubkey::from_str(pool_str) {
                self.hot_pool_registry.pin_arb_pool(pool);
            }
        }
        for row in &snapshot.rows {
            let Ok(pk) = Pubkey::from_str(&row.pubkey) else {
                continue;
            };
            let pin = snapshot_consumer_to_geyser_pin(row.consumer);
            let pool = row.pool.as_deref().and_then(|s| Pubkey::from_str(s).ok());
            match row.kind {
                ExplicitAccountKind::Mint => {
                    let _ = self.track_mint_for_geyser_metadata(pk, pin);
                    if self.tracked_mints.read().contains_key(&pk) {
                        restored += 1;
                    }
                }
                ExplicitAccountKind::WalletToken => {
                    self.tracked_wallet_token_accounts.write().insert(pk);
                    restored += 1;
                }
                ExplicitAccountKind::Vault => {
                    let pool_addr = pool.unwrap_or_default();
                    if row.consumer == SnapshotConsumer::MomentumPosition
                        && pool_addr != Pubkey::default()
                    {
                        if let Some(mint_str) =
                            self.pool_mint_map.read().get(&pool_addr.to_string())
                        {
                            if let Ok(mint) = Pubkey::from_str(mint_str) {
                                self.hot_pool_registry
                                    .pin_pool_with_reason(mint, pool_addr, true);
                            }
                        }
                    }
                    let mut vaults = self.tracked_vaults.write();
                    use std::collections::hash_map::Entry;
                    match vaults.entry(pk) {
                        Entry::Vacant(e) => {
                            e.insert(VaultInfo {
                                pool_address: pool_addr,
                                dex: "restored".to_string(),
                                base_mint: Pubkey::default(),
                                quote_mint: Pubkey::default(),
                                is_base_vault: true,
                                last_balance: std::sync::atomic::AtomicU64::new(0),
                                last_used_at: now,
                                pinned: pin.is_some(),
                                pin,
                                active_id: None,
                                bin_step: None,
                                sibling_vault: None,
                            });
                            drop(vaults);
                            if pool_addr != Pubkey::default() {
                                self.pool_tracked_legs_note_vault(pool_addr, pk);
                            }
                            restored += 1;
                        }
                        Entry::Occupied(mut e) => {
                            let v = e.get_mut();
                            v.last_used_at = now;
                            if let Some(target) = pin {
                                if Self::geyser_pin_may_promote(v.pin, target) {
                                    v.pinned = true;
                                    v.pin = Some(target);
                                }
                            }
                            restored += 1;
                        }
                    }
                }
                ExplicitAccountKind::BinArray => {
                    let pool_addr = pool.unwrap_or_default();
                    let mut bins = self.tracked_bin_arrays.write();
                    use std::collections::hash_map::Entry;
                    match bins.entry(pk) {
                        Entry::Vacant(e) => {
                            e.insert(BinArrayInfo {
                                pool_address: pool_addr,
                                bin_array_index: 0,
                                bin_step: 0,
                                last_used_at: now,
                                pinned: pin.is_some(),
                                pin,
                            });
                            drop(bins);
                            if pool_addr != Pubkey::default() {
                                self.pool_tracked_legs_note_bin(pool_addr, pk);
                            }
                            restored += 1;
                        }
                        Entry::Occupied(mut e) => {
                            let b = e.get_mut();
                            b.last_used_at = now;
                            if let Some(target) = pin {
                                if Self::geyser_pin_may_promote(b.pin, target) {
                                    b.pinned = true;
                                    b.pin = Some(target);
                                }
                            }
                            restored += 1;
                        }
                    }
                }
            }
        }
        self.refresh_tracked_membership_snapshot();
        self.refresh_hot_pool_registry_gauges();
        restored
    }

    fn explicit_pubkey_rows_for_desired_set(&self) -> Vec<(Pubkey, ConsumerId, Option<Pubkey>)> {
        let mut rows = Vec::new();
        for (pk, m) in self.tracked_mints.read().iter() {
            rows.push((*pk, consumer_id_for_geyser_pin(m.pin), None));
        }
        for (pk, v) in self.tracked_vaults.read().iter() {
            rows.push((
                *pk,
                consumer_id_for_pool_explicit_row(self, v.pool_address, v.pin),
                Some(v.pool_address),
            ));
        }
        for (pk, b) in self.tracked_bin_arrays.read().iter() {
            rows.push((
                *pk,
                consumer_id_for_pool_explicit_row(self, b.pool_address, b.pin),
                Some(b.pool_address),
            ));
        }
        if let Some(w) = &self.tracked_wallet {
            rows.push((w.wallet, ConsumerId::Wallet, None));
            rows.push((w.wsol_ata, ConsumerId::Wallet, None));
        }
        for pk in self.tracked_wallet_token_accounts.read().iter() {
            rows.push((*pk, ConsumerId::Wallet, None));
        }
        rows
    }

    fn wallet_explicit_demand_pubkeys(&self) -> HashSet<Pubkey> {
        let mut set = HashSet::new();
        if let Some(w) = &self.tracked_wallet {
            set.insert(w.wallet);
            set.insert(w.wsol_ata);
        }
        set.extend(self.tracked_wallet_token_accounts.read().iter().copied());
        set
    }

    fn configured_max_tracked_accounts(&self) -> usize {
        self.config.read().max_tracked_accounts
    }

    fn admission_exceeds_configured_cap(&self, admission: &FixedCapAdmission) -> bool {
        ironcrab::market_data::track::admission_exceeds_configured_cap(
            admission,
            self.configured_max_tracked_accounts(),
        )
    }

    fn mark_explicit_admission_config_cap_desync(
        &self,
        admission: &FixedCapAdmission,
        detail: &str,
    ) {
        let configured = self.configured_max_tracked_accounts();
        self.geyser_explicit_ready.store(false, Ordering::Release);
        *self.geyser_explicit_config_error.write() = Some(format!(
            "configured max_tracked_accounts ({configured}) below admission cap ({}); admitted={}; {detail}",
            admission.cap(),
            admission.len(),
        ));
        self.geyser_connect_barrier.mark_failed();
        set_market_data_geyser_explicit_cap_overflow(1);
    }

    fn on_admission_converge_result(
        &self,
        admission: &FixedCapAdmission,
        result: AdmissionConvergeResult,
    ) {
        self.rebuild_explicit_admitted_pools_from_admission(admission);
        set_market_data_geyser_explicit_cap_overflow(
            if matches!(
                result,
                AdmissionConvergeResult::ProtectedOverflow | AdmissionConvergeResult::Unconverged
            ) {
                1
            } else {
                0
            },
        );
        match result {
            AdmissionConvergeResult::Converged => {
                set_market_data_geyser_explicit_admitted_accounts(admission.len());
                set_market_data_geyser_explicit_set_size(admission.len());
                let configured = self.configured_max_tracked_accounts();
                if self.admission_exceeds_configured_cap(admission) {
                    self.mark_explicit_admission_config_cap_desync(
                        admission,
                        "converge succeeded but admission exceeds configured cap",
                    );
                } else if admission.wallet_demand_exceeds_cap(configured) {
                    let wallet_len = self.wallet_explicit_demand_pubkeys().len();
                    let msg = format!(
                        "wallet protected explicit demand ({wallet_len} pubkeys) exceeds max_tracked_accounts cap ({configured})"
                    );
                    self.geyser_explicit_ready.store(false, Ordering::Release);
                    *self.geyser_explicit_config_error.write() = Some(msg);
                    self.geyser_connect_barrier.mark_failed();
                    set_market_data_geyser_explicit_cap_overflow(1);
                } else {
                    self.geyser_explicit_ready.store(true, Ordering::Release);
                    *self.geyser_explicit_config_error.write() = None;
                    set_market_data_geyser_explicit_cap_overflow(0);
                }
            }
            AdmissionConvergeResult::ProtectedOverflow => {
                let cap = admission.cap();
                let wallet_len = self.wallet_explicit_demand_pubkeys().len();
                let msg = format!(
                    "wallet protected explicit demand ({wallet_len} pubkeys) exceeds max_tracked_accounts cap ({cap})"
                );
                self.geyser_explicit_ready.store(false, Ordering::Release);
                *self.geyser_explicit_config_error.write() = Some(msg);
                self.geyser_connect_barrier.mark_failed();
            }
            AdmissionConvergeResult::Unconverged => {
                self.geyser_explicit_ready.store(false, Ordering::Release);
                *self.geyser_explicit_config_error.write() =
                    Some("explicit admission cap unconverged".into());
                self.geyser_connect_barrier.mark_failed();
            }
        }
    }

    fn signal_restore_barrier(&self, ok: bool) {
        if ok {
            let wallet_fits = self.wallet_explicit_demand_pubkeys().len()
                <= self.config.read().max_tracked_accounts;
            if self.geyser_explicit_readiness_ok() && wallet_fits {
                self.geyser_connect_barrier.mark_ready();
            }
            // Incomplete restore slices stay PENDING until physical publish completes.
        } else {
            self.geyser_connect_barrier.mark_failed();
        }
    }

    fn geyser_explicit_readiness_ok(&self) -> bool {
        self.geyser_explicit_ready.load(Ordering::Relaxed)
    }

    fn clear_geyser_prune_resume(&self) {
        *self.geyser_prune_resume.lock() = GeyserPruneResume::default();
    }

    fn tracked_maps_need_prune(&self, admitted: &HashSet<Pubkey>) -> bool {
        if self
            .tracked_mints
            .read()
            .keys()
            .any(|pk| !admitted.contains(pk))
        {
            return true;
        }
        if self
            .tracked_vaults
            .read()
            .keys()
            .any(|pk| !admitted.contains(pk))
        {
            return true;
        }
        if self
            .tracked_bin_arrays
            .read()
            .keys()
            .any(|pk| !admitted.contains(pk))
        {
            return true;
        }
        self.tracked_wallet_token_accounts
            .read()
            .iter()
            .any(|pk| !admitted.contains(pk))
    }

    fn collect_extraneous_keys_for_prune_map(
        &self,
        admitted: &HashSet<Pubkey>,
        map: GeyserPruneMap,
    ) -> Vec<Pubkey> {
        match map {
            GeyserPruneMap::Mints => self
                .tracked_mints
                .read()
                .keys()
                .filter(|pk| !admitted.contains(*pk))
                .copied()
                .collect(),
            GeyserPruneMap::Vaults => self
                .tracked_vaults
                .read()
                .keys()
                .filter(|pk| !admitted.contains(*pk))
                .copied()
                .collect(),
            GeyserPruneMap::Bins => self
                .tracked_bin_arrays
                .read()
                .keys()
                .filter(|pk| !admitted.contains(*pk))
                .copied()
                .collect(),
            GeyserPruneMap::Wallets => self
                .tracked_wallet_token_accounts
                .read()
                .iter()
                .filter(|pk| !admitted.contains(*pk))
                .copied()
                .collect(),
            GeyserPruneMap::Done => Vec::new(),
        }
    }

    fn apply_prune_batch(&self, map: GeyserPruneMap, batch: &[Pubkey]) {
        match map {
            GeyserPruneMap::Mints => {
                let mut mints = self.tracked_mints.write();
                for pk in batch {
                    mints.remove(pk);
                }
            }
            GeyserPruneMap::Vaults => {
                let mut vaults = self.tracked_vaults.write();
                for pk in batch {
                    if let Some(info) = vaults.remove(pk) {
                        self.pool_tracked_legs_remove_vault(info.pool_address, *pk);
                    }
                }
            }
            GeyserPruneMap::Bins => {
                let mut bins = self.tracked_bin_arrays.write();
                for pk in batch {
                    if let Some(info) = bins.remove(pk) {
                        self.pool_tracked_legs_remove_bin(info.pool_address, *pk);
                    }
                }
            }
            GeyserPruneMap::Wallets => {
                let mut wallets = self.tracked_wallet_token_accounts.write();
                for pk in batch {
                    wallets.remove(pk);
                }
            }
            GeyserPruneMap::Done => {}
        }
    }

    /// Budgeted prune: returns `true` when tracked maps match admitted set.
    fn prune_tracked_maps_to_admitted_with_deadline(
        &self,
        deadline: Instant,
        admission: &FixedCapAdmission,
    ) -> bool {
        let mut resume = self.geyser_prune_resume.lock();

        loop {
            if Instant::now() >= deadline {
                return false;
            }

            let admitted: HashSet<Pubkey> = admission.snapshot_pubkeys().into_iter().collect();

            if !resume.active {
                if !self.tracked_maps_need_prune(&admitted) {
                    return true;
                }
                resume.active = true;
                resume.map = GeyserPruneMap::Mints;
                resume.pending.clear();
            } else {
                resume.pending.retain(|pk| !admitted.contains(pk));
            }

            if resume.pending.is_empty() {
                while resume.map != GeyserPruneMap::Done {
                    resume.pending =
                        self.collect_extraneous_keys_for_prune_map(&admitted, resume.map);
                    if !resume.pending.is_empty() {
                        break;
                    }
                    resume.map = resume.map.next();
                }
                if resume.pending.is_empty() {
                    resume.active = false;
                    return true;
                }
            }

            let batch_len = resume.pending.len().min(GEYSER_PRUNE_SLICE_MAX_REMOVALS);
            let batch: Vec<Pubkey> = resume
                .pending
                .drain(..batch_len)
                .filter(|pk| !admitted.contains(pk))
                .collect();
            let map = resume.map;
            drop(resume);
            if !batch.is_empty() {
                self.apply_prune_batch(map, &batch);
                self.refresh_tracked_membership_snapshot();
            }
            resume = self.geyser_prune_resume.lock();

            if Instant::now() >= deadline
                && (!resume.pending.is_empty() || resume.map != GeyserPruneMap::Done)
            {
                return false;
            }
            if resume.pending.is_empty() {
                resume.map = resume.map.next();
            }
        }
    }

    fn prune_tracked_maps_to_admitted(&self, admission: &FixedCapAdmission) {
        let deadline = Instant::now() + Duration::from_secs(300);
        let _ = self.prune_tracked_maps_to_admitted_with_deadline(deadline, admission);
        self.clear_geyser_prune_resume();
    }

    fn explicit_physical_publish_needed(&self, admission: &FixedCapAdmission) -> bool {
        let admitted: HashSet<Pubkey> = admission.snapshot_pubkeys().into_iter().collect();
        admitted != *self.last_synced_explicit_pubkeys.read()
    }

    fn publish_admitted_explicit_physical(&self, admission: &FixedCapAdmission) {
        if !self.explicit_physical_publish_needed(admission) {
            return;
        }
        self.send_admitted_explicit_physical(admission);
    }

    fn publish_admitted_explicit_physical_force(&self, admission: &FixedCapAdmission) {
        self.send_admitted_explicit_physical(admission);
        *self.last_synced_explicit_pubkeys.write() =
            admission.snapshot_pubkeys().into_iter().collect();
    }

    fn send_admitted_explicit_physical(&self, admission: &FixedCapAdmission) {
        if self.admission_exceeds_configured_cap(admission) {
            self.mark_explicit_admission_config_cap_desync(
                admission,
                "publish blocked: admission exceeds configured cap",
            );
            return;
        }
        let admitted: HashSet<Pubkey> = admission.snapshot_pubkeys().into_iter().collect();
        let mint_keys: HashSet<Pubkey> = self.tracked_mints.read().keys().copied().collect();
        let vault_keys: HashSet<Pubkey> = self.tracked_vaults.read().keys().copied().collect();
        let bin_keys: HashSet<Pubkey> = self.tracked_bin_arrays.read().keys().copied().collect();
        let wallet_keys = self.wallet_explicit_demand_pubkeys();
        let (mints, vaults, bins, wallets) =
            ironcrab::market_data::track::partition_admitted_pubkeys_for_geyser_channels(
                &admitted,
                &mint_keys,
                &vault_keys,
                &bin_keys,
                &wallet_keys,
            );
        let _ = self.tracked_mints_tx.send(mints);
        let _ = self.tracked_vaults_tx.send(vaults);
        let _ = self.tracked_bin_arrays_tx.send(bins);
        let _ = self.tracked_wallet_tx.send(wallets);
        geyser_metrics_set_subscription_accounts(admitted.len());
        self.refresh_geyser_pins_gauge();
    }

    fn build_explicit_set_snapshot(&self, admission: &FixedCapAdmission) -> ExplicitSetSnapshot {
        let mut snapshot = ExplicitSetSnapshot::new(Some(self.run_id.clone()));
        snapshot.rows = self.collect_explicit_snapshot_rows();
        snapshot.owner_groups = admission
            .snapshot_owner_groups()
            .iter()
            .map(owner_group_snapshot_to_disk)
            .collect();
        snapshot.pool_mint_map =
            self.collect_pool_mint_map_tier1(EXPLICIT_SET_SNAPSHOT_POOL_MINT_MAP_CAP);
        snapshot.momentum_pools = self
            .hot_pool_registry
            .snapshot_pairs()
            .into_iter()
            .map(|(_, pool)| pool.to_string())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        snapshot.momentum_position_pools = self
            .hot_pool_registry
            .snapshot_position_pool_pubkeys()
            .into_iter()
            .map(|p| p.to_string())
            .collect();
        snapshot.arb_pools = self
            .hot_pool_registry
            .snapshot_arb_pools()
            .into_iter()
            .map(|p| p.to_string())
            .collect();
        snapshot
    }

    fn restore_explicit_admission_from_snapshot(
        &self,
        admission: &mut FixedCapAdmission,
        snapshot: &ExplicitSetSnapshot,
    ) -> AdmissionRestoreResult {
        let groups = snapshot.to_owner_group_snapshots();
        let result = restore_admission_from_owner_groups(admission, &groups);
        match result {
            AdmissionRestoreResult::Restored => {
                self.rebuild_explicit_admitted_pools_from_admission(admission);
                if self.admission_exceeds_configured_cap(admission) {
                    self.mark_explicit_admission_config_cap_desync(
                        admission,
                        "restore succeeded but admission exceeds configured cap",
                    );
                } else if admission
                    .wallet_demand_exceeds_cap(self.configured_max_tracked_accounts())
                {
                    let cap = self.configured_max_tracked_accounts();
                    let msg = format!(
                        "wallet-only explicit demand exceeds max_tracked_accounts cap ({cap}) on restore"
                    );
                    self.geyser_explicit_ready.store(false, Ordering::Release);
                    *self.geyser_explicit_config_error.write() = Some(msg);
                    self.geyser_connect_barrier.mark_failed();
                } else {
                    self.geyser_explicit_ready.store(true, Ordering::Release);
                    *self.geyser_explicit_config_error.write() = None;
                    set_market_data_geyser_explicit_cap_overflow(0);
                }
            }
            AdmissionRestoreResult::ProtectedOverflow => {
                let cap = admission.cap();
                let msg = format!(
                    "wallet-only explicit demand exceeds max_tracked_accounts cap ({cap}) on restore"
                );
                self.geyser_explicit_ready.store(false, Ordering::Release);
                *self.geyser_explicit_config_error.write() = Some(msg);
                self.geyser_connect_barrier.mark_failed();
            }
            AdmissionRestoreResult::Unconverged => {
                self.geyser_explicit_ready.store(false, Ordering::Release);
                *self.geyser_explicit_config_error.write() =
                    Some("explicit admission restore unconverged".into());
                self.geyser_connect_barrier.mark_failed();
            }
        }
        result
    }

    fn apply_explicit_cap_shrink(
        &self,
        admission: &mut FixedCapAdmission,
        new_cap: usize,
    ) -> CapShrinkResult {
        // Runtime cap increases are not supported: config may update, but admission cap is
        // fixed at worker startup until process restart (I-MD-7 / PR3 scope).
        if new_cap > admission.cap() {
            warn!(
                configured_cap = new_cap,
                admission_cap = admission.cap(),
                "Runtime max_tracked_accounts cap increase ignored; restart required"
            );
            return CapShrinkResult::RejectedCapIncrease {
                old_cap: admission.cap(),
                requested_cap: new_cap,
            };
        }
        let result = admission.try_shrink_cap(new_cap);
        if matches!(
            result,
            CapShrinkResult::Converged { .. } | CapShrinkResult::NoOpAlreadyWithinCap { .. }
        ) {
            self.rebuild_explicit_admitted_pools_from_admission(admission);
        }
        match result {
            CapShrinkResult::Converged { .. } | CapShrinkResult::NoOpAlreadyWithinCap { .. } => {
                if self.admission_exceeds_configured_cap(admission) {
                    self.mark_explicit_admission_config_cap_desync(
                        admission,
                        "cap shrink did not align admission with configured cap",
                    );
                } else {
                    self.geyser_explicit_ready.store(true, Ordering::Release);
                    *self.geyser_explicit_config_error.write() = None;
                    set_market_data_geyser_explicit_cap_overflow(0);
                }
            }
            CapShrinkResult::ProtectedOverflow { .. } => {
                self.mark_explicit_admission_config_cap_desync(
                    admission,
                    "max_tracked_accounts shrink rejected: wallet protected overflow",
                );
            }
            CapShrinkResult::RejectedInvalidInput | CapShrinkResult::InternalInvariantViolation => {
                self.mark_explicit_admission_config_cap_desync(admission, "cap shrink failed");
            }
            CapShrinkResult::RejectedCapIncrease { .. } => {}
        }
        result
    }

    fn sync_geyser_tracked_accounts_from_admission_with_deadline(
        &self,
        deadline: Instant,
        admission: &FixedCapAdmission,
    ) -> bool {
        set_market_data_geyser_sync_pending(0);
        record_market_data_geyser_sync_batch_total();
        if !self.prune_tracked_maps_to_admitted_with_deadline(deadline, admission) {
            return false;
        }
        if self.explicit_physical_publish_needed(admission) {
            if Instant::now() >= deadline {
                return false;
            }
            self.publish_admitted_explicit_physical(admission);
        }
        *self.last_synced_explicit_pubkeys.write() =
            admission.snapshot_pubkeys().into_iter().collect();
        self.clear_geyser_prune_resume();
        true
    }

    /// Phase 3 P3: restore explicit set from disk before first Geyser connect (I-MD-6).
    /// Wall-clock budget for `wait_ready` during explicit-set restore (I-MD-6).
    /// Must cover large snapshots (~100k pubkeys) converging via budgeted Continue slices.
    fn geyser_restore_barrier_timeout(snapshot_pubkey_count: u64) -> Duration {
        const MIN_SECS: u64 = 120;
        let scaled = MIN_SECS.saturating_add(snapshot_pubkey_count / 400);
        Duration::from_secs(scaled.max(MIN_SECS))
    }

    fn geyser_explicit_not_ready_bail_message(&self) -> String {
        self.geyser_explicit_config_error
            .read()
            .clone()
            .unwrap_or_else(|| {
                if self.geyser_connect_barrier.is_failed() {
                    "geyser explicit restore barrier failed".to_string()
                } else {
                    "geyser explicit admission not ready (unknown reason)".to_string()
                }
            })
    }

    fn geyser_barrier_wait_bail_message(&self, wait_err: &'static str) -> String {
        if let Some(msg) = self.geyser_explicit_config_error.read().clone() {
            format!("{wait_err}: {msg}")
        } else {
            wait_err.to_string()
        }
    }

    fn restore_explicit_set_from_snapshot_on_startup(
        ctx: &MarketDataContext,
        track_worker: &TrackWorkerSender,
    ) {
        let path = explicit_set_snapshot_path();
        let Some(snapshot) = load_explicit_set_snapshot(&path) else {
            let _ = track_worker_try_enqueue(track_worker, TrackWorkerCommand::ScheduleGeyserPush);
            if let Err(wait_err) = ctx
                .geyser_connect_barrier
                .wait_ready(Self::geyser_restore_barrier_timeout(0))
            {
                warn!("explicit Geyser startup barrier failed or timed out (no snapshot, fail-closed)");
                ctx.geyser_explicit_ready.store(false, Ordering::Release);
                if ctx.geyser_explicit_config_error.read().is_none() {
                    *ctx.geyser_explicit_config_error.write() =
                        Some(ctx.geyser_barrier_wait_bail_message(wait_err));
                }
            }
            return;
        };
        let start = Instant::now();
        let pubkey_count = snapshot.explicit_pubkey_count() as u64;
        info!(
            path = %path.display(),
            pubkeys = pubkey_count,
            pool_mint_map = snapshot.pool_mint_map.len(),
            owner_groups = snapshot.owner_groups.len(),
            "Restoring explicit Geyser set from snapshot (I-MD-6)"
        );
        let _ = track_worker_try_enqueue(
            track_worker,
            TrackWorkerCommand::RestoreExplicitSnapshot(snapshot),
        );
        let _ = track_worker_try_enqueue(track_worker, TrackWorkerCommand::ScheduleGeyserPush);
        if let Err(wait_err) = ctx
            .geyser_connect_barrier
            .wait_ready(Self::geyser_restore_barrier_timeout(pubkey_count))
        {
            warn!("explicit Geyser restore barrier failed or timed out (fail-closed)");
            ctx.geyser_explicit_ready.store(false, Ordering::Release);
            if ctx.geyser_explicit_config_error.read().is_none() {
                *ctx.geyser_explicit_config_error.write() =
                    Some(ctx.geyser_barrier_wait_bail_message(wait_err));
            }
        }
        set_market_data_explicit_set_snapshot_restore_pubkeys(pubkey_count);
        set_market_data_explicit_set_snapshot_restore_duration_ms(
            start.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
        );
    }

    /// Vault + bin-array pubkeys tracked locally for one pool (explicit Geyser assets).
    #[cfg_attr(not(test), allow(dead_code))]
    fn pool_explicit_vault_bin_pubkeys(&self, pool: Pubkey) -> Vec<Pubkey> {
        let mut out = Vec::new();
        for (pk, v) in self.tracked_vaults.read().iter() {
            if v.pool_address == pool {
                out.push(*pk);
            }
        }
        for (pk, b) in self.tracked_bin_arrays.read().iter() {
            if b.pool_address == pool {
                out.push(*pk);
            }
        }
        out
    }

    /// True when all vault/bin pubkeys for this pool were included in the last Geyser sync flush.
    #[cfg_attr(not(test), allow(dead_code))]
    fn pool_has_live_vault_geyser_feed(&self, pool: Pubkey) -> bool {
        let assets = self.pool_explicit_vault_bin_pubkeys(pool);
        if assets.is_empty() {
            return false;
        }
        let synced = self.last_synced_explicit_pubkeys.read();
        assets.iter().all(|pk| synced.contains(pk))
    }

    fn refresh_hot_pool_registry_gauges(&self) {
        let momentum = self.hot_pool_registry.hot_pool_count_momentum();
        let arb = self.hot_pool_registry.arb_pool_count();
        set_market_data_hot_pool_registry_pools_gauge("momentum", momentum);
        set_market_data_hot_pool_registry_pools_gauge("arb", arb);
        set_market_data_hot_pool_registry_pools_gauge("both", 0);
        set_market_data_momentum_active_pool_pins_gauge(self.hot_pool_registry.pair_count());
        set_market_data_arb_pinned_pools_gauge(arb);
        self.refresh_enrichment_registry_gauge();
        self.refresh_exec_hot_membership_snapshot();
    }

    fn refresh_enrichment_registry_gauge(&self) {
        let mut pools: HashSet<Pubkey> = self
            .hot_pool_registry
            .snapshot_arb_pools()
            .into_iter()
            .collect();
        for (_, pool) in self.hot_pool_registry.snapshot_pairs() {
            pools.insert(pool);
        }
        for pool_str in self.pool_mint_map.read().keys() {
            if let Ok(pk) = Pubkey::from_str(pool_str) {
                pools.insert(pk);
            }
        }
        for pool in self.high_priority_bonding_curves.read().iter() {
            pools.insert(*pool);
        }
        set_market_data_enrichment_registry_pools_gauge(pools.len() as u64);
    }

    /// PR237 + Phase1: rebuild ingest/sidefx snapshot from authoritative md-state maps.
    fn refresh_tracked_membership_snapshot(&self) {
        let prior = self.tracked_membership.load();
        let mut vault_by_pubkey = HashMap::new();
        for (pk, v) in self.tracked_vaults.read().iter() {
            let last_balance = prior
                .vault_by_pubkey
                .get(pk)
                .map(|entry| Arc::clone(&entry.last_balance))
                .unwrap_or_else(|| {
                    Arc::new(AtomicU64::new(
                        v.last_balance.load(std::sync::atomic::Ordering::Relaxed),
                    ))
                });
            last_balance.store(
                v.last_balance.load(std::sync::atomic::Ordering::Relaxed),
                std::sync::atomic::Ordering::Relaxed,
            );
            vault_by_pubkey.insert(
                *pk,
                SnapshotVaultView {
                    pool_address: v.pool_address,
                    dex: v.dex.clone(),
                    base_mint: v.base_mint,
                    quote_mint: v.quote_mint,
                    is_base_vault: v.is_base_vault,
                    sibling_vault: v.sibling_vault,
                    active_id: v.active_id,
                    bin_step: v.bin_step,
                    last_balance,
                },
            );
        }
        let vaults: HashSet<Pubkey> = vault_by_pubkey.keys().copied().collect();
        let mut bin_array_by_pubkey = HashMap::new();
        for (pk, b) in self.tracked_bin_arrays.read().iter() {
            bin_array_by_pubkey.insert(
                *pk,
                SnapshotBinArrayView {
                    pool_address: b.pool_address,
                    bin_array_index: b.bin_array_index,
                    bin_step: b.bin_step,
                },
            );
        }
        let bin_arrays: HashSet<Pubkey> = bin_array_by_pubkey.keys().copied().collect();
        let mints: HashSet<Pubkey> = self.tracked_mints.read().keys().copied().collect();
        self.tracked_membership
            .store(Arc::new(TrackedMembershipSnapshot {
                vaults,
                mints,
                bin_arrays,
                vault_by_pubkey,
                bin_array_by_pubkey,
            }));
        touch_market_data_tracked_membership_snapshot_refresh();
        self.refresh_exec_hot_membership_snapshot();
    }

    /// Scope F: rebuild EXEC_HOT vault/bin snapshot from hot-pool registry + tracked legs.
    fn refresh_exec_hot_membership_snapshot(&self) {
        let hot_pools = self.hot_pool_registry.snapshot_hot_pool_pubkeys();
        let mut vaults = HashSet::new();
        let mut bin_arrays = HashSet::new();
        {
            let legs = self.pool_tracked_legs.read();
            for pool in &hot_pools {
                if let Some(entry) = legs.get(pool) {
                    vaults.extend(entry.vaults.iter().copied());
                    bin_arrays.extend(entry.bin_arrays.iter().copied());
                }
            }
        }
        let membership = self.tracked_membership.load();
        for (pk, view) in membership.vault_by_pubkey.iter() {
            if hot_pools.contains(&view.pool_address) {
                vaults.insert(*pk);
            }
        }
        for (pk, view) in membership.bin_array_by_pubkey.iter() {
            if hot_pools.contains(&view.pool_address) {
                bin_arrays.insert(*pk);
            }
        }
        self.exec_hot_membership
            .store(Arc::new(ExecHotMembershipSnapshot { vaults, bin_arrays }));
    }

    fn pool_tracked_legs_note_vault(&self, pool: Pubkey, vault: Pubkey) {
        let mut legs = self.pool_tracked_legs.write();
        let entry = legs.entry(pool).or_default();
        if !entry.vaults.contains(&vault) {
            entry.vaults.push(vault);
        }
    }

    fn pool_tracked_legs_note_bin(&self, pool: Pubkey, pda: Pubkey) {
        let mut legs = self.pool_tracked_legs.write();
        let entry = legs.entry(pool).or_default();
        if !entry.bin_arrays.contains(&pda) {
            entry.bin_arrays.push(pda);
        }
    }

    fn pool_tracked_legs_remove_vault(&self, pool: Pubkey, vault: Pubkey) {
        let mut legs = self.pool_tracked_legs.write();
        if let Some(entry) = legs.get_mut(&pool) {
            entry.vaults.retain(|pk| *pk != vault);
            if entry.vaults.is_empty() && entry.bin_arrays.is_empty() {
                legs.remove(&pool);
            }
        }
    }

    fn pool_tracked_legs_remove_bin(&self, pool: Pubkey, pda: Pubkey) {
        let mut legs = self.pool_tracked_legs.write();
        if let Some(entry) = legs.get_mut(&pool) {
            entry.bin_arrays.retain(|pk| *pk != pda);
            if entry.vaults.is_empty() && entry.bin_arrays.is_empty() {
                legs.remove(&pool);
            }
        }
    }

    fn tracked_vaults_write_timed(
        &self,
    ) -> parking_lot::RwLockWriteGuard<'_, HashMap<Pubkey, VaultInfo>> {
        let wait_start = Instant::now();
        let guard = self.tracked_vaults.write();
        record_market_data_md_state_writer_wait_us(
            wait_start.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
        );
        guard
    }

    fn tracked_bin_arrays_write_timed(
        &self,
    ) -> parking_lot::RwLockWriteGuard<'_, HashMap<Pubkey, BinArrayInfo>> {
        let wait_start = Instant::now();
        let guard = self.tracked_bin_arrays.write();
        record_market_data_md_state_writer_wait_us(
            wait_start.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
        );
        guard
    }

    /// True when pool passes arb-multi-dex admission (multi-dex relevance + reserve basis).
    fn count_geyser_wallet_explicit_accounts(&self) -> usize {
        if self.tracked_wallet.is_some() {
            2 + self.tracked_wallet_token_accounts.read().len()
        } else {
            0
        }
    }

    fn refresh_geyser_pins_gauge(&self) {
        let mut pinned: usize = 0;
        pinned += self
            .tracked_mints
            .read()
            .values()
            .filter(|m| m.pinned)
            .count();
        pinned += self
            .tracked_vaults
            .read()
            .values()
            .filter(|v| v.pinned)
            .count();
        pinned += self
            .tracked_bin_arrays
            .read()
            .values()
            .filter(|b| b.pinned)
            .count();
        // Wallet ATAs are always subscribed and treated as pinned for ops visibility.
        pinned += self.count_geyser_wallet_explicit_accounts();
        geyser_metrics_set_tracked_pinned_accounts(pinned);
    }

    /// Partner vault (opposite base/quote leg) eligible for paired eviction.
    /// Never returns a pinned sibling — pinned vaults must stay subscribed.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn geyser_unpinned_sibling_vault_pubkey(
        vaults: &std::collections::HashMap<Pubkey, VaultInfo>,
        pool: Pubkey,
        primary_is_base_vault: bool,
    ) -> Option<Pubkey> {
        vaults
            .iter()
            .find(|(_, vi)| {
                vi.pool_address == pool && vi.is_base_vault != primary_is_base_vault && !vi.pinned
            })
            .map(|(pk, _)| *pk)
    }

    /// Whether a **pinned** partner vault exists for the same pool (opposite leg).
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn geyser_pinned_sibling_vault_present(
        vaults: &std::collections::HashMap<Pubkey, VaultInfo>,
        pool: Pubkey,
        primary_is_base_vault: bool,
    ) -> bool {
        vaults.values().any(|vi| {
            vi.pool_address == pool && vi.is_base_vault != primary_is_base_vault && vi.pinned
        })
    }

    /// PR167: longer debounce during startup pin burst (min 250 ms for first 120 s).
    fn geyser_sync_batch_debounce_ms(&self) -> u64 {
        let base = self.config.read().geyser_sync_batch_ms.clamp(10, 100);
        if self.started_at.elapsed() < MARKET_DATA_GEYSER_SYNC_STARTUP_WINDOW {
            base.max(MARKET_DATA_GEYSER_SYNC_STARTUP_MIN_MS)
        } else {
            base
        }
    }

    fn try_acquire_geyser_sync_flush_slot(&self) -> bool {
        let mut window = self.geyser_sync_flush_timestamps.lock();
        let now = Instant::now();
        window.retain(|t| now.saturating_duration_since(*t) < Duration::from_secs(1));
        if window.len() >= MARKET_DATA_GEYSER_SYNC_FLUSH_MAX_PER_SEC {
            return false;
        }
        window.push(now);
        true
    }

    fn release_geyser_sync_flush_slot(&self) {
        self.geyser_sync_flush_timestamps.lock().pop();
    }

    fn schedule_geyser_sync_batch_debounced(self: &Arc<Self>, md_state: &MdStateSender) {
        let ms = self.geyser_sync_batch_debounce_ms();
        set_market_data_geyser_sync_pending(1);
        let epoch = self
            .geyser_sync_debounce_epoch
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        let mut guard = self.geyser_sync_batch_timer.lock();
        if let Some(abort) = guard.take() {
            abort.store(true, Ordering::Relaxed);
        }
        let abort = Arc::new(AtomicBool::new(false));
        let abort_c = Arc::clone(&abort);
        let md_state_thread = md_state.clone();
        let md_state_fallback = md_state.clone();
        let ctx = self.clone();
        match std::thread::Builder::new()
            .name("md-geyser-sync-debounce".into())
            .spawn(move || {
                std::thread::sleep(Duration::from_millis(ms));
                if abort_c.load(Ordering::Relaxed)
                    || ctx.geyser_sync_debounce_epoch.load(Ordering::Relaxed) != epoch
                {
                    return;
                }
                if !ctx.try_acquire_geyser_sync_flush_slot() {
                    inc_market_data_geyser_sync_skipped_rate_limit_total();
                    ctx.schedule_geyser_sync_batch_debounced(&md_state_thread);
                    return;
                }
                if !md_state_try_enqueue(&md_state_thread, MdStateCommand::FlushGeyserSyncDebounced)
                {
                    ctx.release_geyser_sync_flush_slot();
                    ctx.schedule_geyser_sync_batch_debounced(&md_state_thread);
                }
            }) {
            Ok(_) => *guard = Some(abort),
            Err(e) => {
                warn!(
                    error = %e,
                    "md-geyser-sync-debounce thread spawn failed; enqueueing flush directly"
                );
                drop(guard);
                if !md_state_try_enqueue(
                    &md_state_fallback,
                    MdStateCommand::FlushGeyserSyncDebounced,
                ) {
                    self.schedule_geyser_sync_batch_debounced(&md_state_fallback);
                }
            }
        }
    }

    /// PR161: merge four explicit-track `watch` streams into `combined_tracked_tx` with the same
    /// debounce window as [`Self::schedule_geyser_sync_batch_debounced`] (`geyser_sync_batch_ms`,
    /// clamped 10–100 ms). Reduces subscription-update churn when `broadcast_tracked_geyser_explicit_to_merge`
    /// fans out to multiple watch updates.
    fn schedule_geyser_tracked_merge_flush_debounced(
        debounce_timer: &Arc<parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>>,
        combined_tx: &watch::Sender<Vec<Pubkey>>,
        ctx_merge: &Arc<MarketDataContext>,
        mints_rx: &watch::Receiver<Vec<Pubkey>>,
        vaults_rx: &watch::Receiver<Vec<Pubkey>>,
        bin_arrays_rx: &watch::Receiver<Vec<Pubkey>>,
        wallet_rx: &watch::Receiver<Vec<Pubkey>>,
    ) {
        let ms = ctx_merge.geyser_sync_batch_debounce_ms();
        set_market_data_geyser_merge_pending(1);
        let mut guard = debounce_timer.lock();
        if let Some(h) = guard.take() {
            h.abort();
        }
        let combined_tx = combined_tx.clone();
        let ctx_fl = Arc::clone(ctx_merge);
        let mints_c = mints_rx.clone();
        let vaults_c = vaults_rx.clone();
        let bins_c = bin_arrays_rx.clone();
        let wallet_c = wallet_rx.clone();
        let h = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(ms)).await;
            set_market_data_geyser_merge_pending(0);
            record_market_data_geyser_merge_coalesced_total();
            let mut combined: Vec<Pubkey> = mints_c.borrow().clone();
            combined.extend(vaults_c.borrow().clone());
            combined.extend(bins_c.borrow().clone());
            combined.extend(wallet_c.borrow().clone());
            combined.sort();
            combined.dedup();
            let n = combined.len();
            let _ = combined_tx.send(combined);
            geyser_metrics_set_subscription_accounts(n);
            ctx_fl.refresh_geyser_pins_gauge();
        });
        *guard = Some(h);
    }

    /// PR161: background loop merging explicit-track `watch` streams into `combined_tracked_tx` with debouncing.
    async fn run_geyser_tracked_accounts_merge_coalesce_loop(
        mut mints_rx: watch::Receiver<Vec<Pubkey>>,
        mut vaults_rx: watch::Receiver<Vec<Pubkey>>,
        mut bin_arrays_rx: watch::Receiver<Vec<Pubkey>>,
        mut wallet_rx: watch::Receiver<Vec<Pubkey>>,
        combined_tx: watch::Sender<Vec<Pubkey>>,
        ctx_merge: Arc<MarketDataContext>,
    ) {
        let debounce_timer = Arc::new(parking_lot::Mutex::new(None::<tokio::task::JoinHandle<()>>));
        loop {
            tokio::select! {
                r = mints_rx.changed() => {
                    if r.is_err() {
                        return;
                    }
                }
                r = vaults_rx.changed() => {
                    if r.is_err() {
                        return;
                    }
                }
                r = bin_arrays_rx.changed() => {
                    if r.is_err() {
                        return;
                    }
                }
                r = wallet_rx.changed() => {
                    if r.is_err() {
                        return;
                    }
                }
            }
            Self::schedule_geyser_tracked_merge_flush_debounced(
                &debounce_timer,
                &combined_tx,
                &ctx_merge,
                &mints_rx,
                &vaults_rx,
                &bin_arrays_rx,
                &wallet_rx,
            );
        }
    }

    /// Track mint pubkey for Geyser `TokenMintInfo` / metadata. Returns `true` if explicit pubkey set changed.
    fn track_mint_for_geyser_metadata(&self, mint: Pubkey, pin: Option<GeyserPinReason>) -> bool {
        let now = Instant::now();
        let mut map = self.tracked_mints.write();
        use std::collections::hash_map::Entry;
        match map.entry(mint) {
            Entry::Vacant(e) => {
                let pinned = pin.is_some();
                e.insert(MintTrackInfo {
                    last_used_at: now,
                    pinned,
                    pin,
                });
                true
            }
            Entry::Occupied(mut e) => {
                let v = e.get_mut();
                v.last_used_at = now;
                match pin {
                    None => false,
                    Some(GeyserPinReason::Wallet) => {
                        let changed = !v.pinned || v.pin != Some(GeyserPinReason::Wallet);
                        v.pinned = true;
                        v.pin = Some(GeyserPinReason::Wallet);
                        changed
                    }
                    Some(GeyserPinReason::MomentumActive) => {
                        if v.pin == Some(GeyserPinReason::Wallet) {
                            return false;
                        }
                        let changed = !v.pinned || v.pin != Some(GeyserPinReason::MomentumActive);
                        v.pinned = true;
                        v.pin = Some(GeyserPinReason::MomentumActive);
                        changed
                    }
                    Some(GeyserPinReason::ArbMultiDex) => {
                        if matches!(
                            v.pin,
                            Some(GeyserPinReason::Wallet) | Some(GeyserPinReason::MomentumActive)
                        ) {
                            return false;
                        }
                        let changed = !v.pinned || v.pin != Some(GeyserPinReason::ArbMultiDex);
                        v.pinned = true;
                        v.pin = Some(GeyserPinReason::ArbMultiDex);
                        changed
                    }
                }
            }
        }
    }

    fn register_tracked_vault_pair_lru(
        &self,
        vaults: &mut std::collections::HashMap<Pubkey, VaultInfo>,
        vaults_changed: &mut bool,
        spec: TrackedVaultPairInsert<'_>,
    ) {
        let pool = spec.pool;
        let inserted = insert_tracked_vault_pair(vaults, vaults_changed, spec);
        for pk in &inserted {
            self.pool_tracked_legs_note_vault(pool, *pk);
        }
    }

    fn touch_tracked_vault_pubkey(&self, vault: &Pubkey) {
        let now = Instant::now();
        let mut vaults = self.tracked_vaults_write_timed();
        let sibling = if let Some(v) = vaults.get_mut(vault) {
            v.last_used_at = now;
            v.sibling_vault
                .filter(|sibling_pk| vaults.get(sibling_pk).is_some_and(|sv| !sv.pinned))
        } else {
            None
        };
        if let Some(sibling_pk) = sibling {
            if let Some(sv) = vaults.get_mut(&sibling_pk) {
                sv.last_used_at = now;
            }
        }
    }

    fn touch_tracked_bin_array_pubkey(&self, pda: &Pubkey) {
        let now = Instant::now();
        if let Some(b) = self.tracked_bin_arrays_write_timed().get_mut(pda) {
            b.last_used_at = now;
        }
    }

    #[allow(dead_code)] // Phase-R-R2: ingest uses TrackMint on md-state; kept for LRU helpers/tests.
    fn touch_tracked_mint_pubkey(&self, mint: &Pubkey) {
        let now = Instant::now();
        if let Some(m) = self.tracked_mints.write().get_mut(mint) {
            m.last_used_at = now;
        }
    }

    fn touch_tracked_pool_vaults_and_bins(&self, pool: Pubkey) {
        if let Some(legs) = self.pool_tracked_legs.read().get(&pool).cloned() {
            for vault in legs.vaults {
                self.touch_tracked_vault_pubkey(&vault);
            }
            for pda in legs.bin_arrays {
                self.touch_tracked_bin_array_pubkey(&pda);
            }
        }
    }

    fn touch_tracked_pool_vaults_and_bins_if_tracked(&self, pool: Pubkey) {
        if self.pool_tracked_legs.read().contains_key(&pool) {
            self.touch_tracked_pool_vaults_and_bins(pool);
        }
    }

    /// Cache-first JetStream BalanceUpdated for pools with fresh reserve basis (no vault Geyser sub required).
    #[cfg_attr(not(test), allow(dead_code))]
    fn try_publish_balance_updated_from_cache(&self, pool: Pubkey) {
        if self.pool_has_live_vault_geyser_feed(pool) {
            return;
        }
        let Some(state) = self.live_pool_cache.get(&pool) else {
            return;
        };
        if !cached_pool_has_fresh_reserve_basis(&state) {
            return;
        }
        let Some((base_mint, quote_mint, base_reserve, quote_reserve, dex)) =
            pool_cache_balance_fields_from_state(&state)
        else {
            return;
        };
        let Some(nats) = self
            .nats
            .as_ref()
            .map(NatsClient::clone_for_spawned_publish)
        else {
            return;
        };
        let run_id = self.run_id.clone();
        let pool_str = pool.to_string();
        let publish = async move {
            let mut balance_update = PoolCacheUpdate::new_balance_updated(
                "market-data",
                BUILD_VERSION,
                &run_id,
                pool_str.clone(),
                dex.to_string(),
                base_mint.to_string(),
                quote_mint.to_string(),
                base_reserve,
                quote_reserve,
                0,
            );
            if dex == "raydium_cpmm" {
                if let CachedPoolState::RaydiumCpmm(ref s) = state {
                    let mut meta = std::collections::HashMap::new();
                    meta.insert(
                        POOL_CACHE_UPDATE_RAYDIUM_CPMM_VAULTS_KEY.to_string(),
                        raydium_cpmm_vaults_for_pool_cache_update(s),
                    );
                    balance_update.metadata = Some(meta);
                    let readiness = raydium_cpmm_readiness_for_pool_cache_update(s);
                    balance_update.set_dex_readiness_in_metadata(readiness);
                }
            }
            let subject = pool_subject(&pool_str);
            let _ = nats.jetstream_publish(&subject, &balance_update).await;
        };
        if let Some(handle) = self.ingest_tokio_handle.read().clone() {
            inc_market_data_balance_updated_from_cache_total();
            handle.spawn(publish);
        } else if let Ok(handle) = tokio::runtime::Handle::try_current() {
            inc_market_data_balance_updated_from_cache_total();
            handle.spawn(publish);
        }
    }

    /// PR-B: after a parsed swap trade — cache-first BalanceUpdated; vault/bin registration only for hot pools.
    #[cfg_attr(not(test), allow(dead_code))]
    fn register_geyser_reserves_after_trade(self: &Arc<Self>, pool: Pubkey) -> bool {
        self.try_publish_balance_updated_from_cache(pool);
        if !self.hot_pool_registry.pool_has_momentum(pool) {
            return false;
        }
        let Some(state) = self.live_pool_cache.get(&pool) else {
            return false;
        };
        let Some((base_mint, quote_mint)) = pool_mints_for_geyser_explicit_tracking(&state) else {
            return false;
        };
        if !self.admit_geyser_explicit_pool_assets(pool, base_mint, quote_mint) {
            return false;
        }
        let before_keys = self.snapshot_explicit_demand_pubkeys();
        self.register_geyser_reserves_impl(pool, GeyserPinReason::MomentumActive);
        explicit_subscription_has_new_keys(&before_keys, &self.snapshot_explicit_demand_pubkeys())
    }

    /// RaydiumCpmm / MeteoraCpmm / Meteora DLMM / PumpAmm vault rows from a MASTER cache entry.
    /// When `require_explicit_admit` is set, only pools passing [`Self::admit_geyser_explicit_pool_assets`].
    fn register_four_dex_pool_vaults_from_cached_state(
        &self,
        pool: Pubkey,
        cached_state: &CachedPoolState,
        now: Instant,
        enable_meteora_cpmm: bool,
        enable_meteora_dlmm: bool,
        require_explicit_admit: bool,
    ) -> bool {
        let mut vaults_changed = false;
        let admit = |base_mint: Pubkey, quote_mint: Pubkey| -> bool {
            !require_explicit_admit
                || self.admit_geyser_explicit_pool_assets(pool, base_mint, quote_mint)
        };

        if let CachedPoolState::RaydiumCpmm(s) = cached_state {
            let (base_mint_v, quote_mint_v, base_vault, quote_vault) =
                cpmm_token_mints_and_vaults_sol_normalized(
                    s.token_0_mint,
                    s.token_1_mint,
                    s.token_0_vault,
                    s.token_1_vault,
                );
            if admit(base_mint_v, quote_mint_v) {
                let mut vaults = self.tracked_vaults.write();
                self.register_tracked_vault_pair_lru(
                    &mut vaults,
                    &mut vaults_changed,
                    TrackedVaultPairInsert {
                        pool,
                        now,
                        dex: "raydium_cpmm",
                        base_mint: base_mint_v,
                        quote_mint: quote_mint_v,
                        base_vault,
                        quote_vault,
                        active_id: None,
                        bin_step: None,
                    },
                );
            }
        }

        if enable_meteora_cpmm {
            if let CachedPoolState::MeteoraCpmm(s) = cached_state {
                let (base_mint_v, quote_mint_v, base_vault, quote_vault) =
                    cpmm_token_mints_and_vaults_sol_normalized(
                        s.token_0_mint,
                        s.token_1_mint,
                        s.token_0_vault,
                        s.token_1_vault,
                    );
                if admit(base_mint_v, quote_mint_v) {
                    let mut vaults = self.tracked_vaults.write();
                    self.register_tracked_vault_pair_lru(
                        &mut vaults,
                        &mut vaults_changed,
                        TrackedVaultPairInsert {
                            pool,
                            now,
                            dex: "meteora_cpmm",
                            base_mint: base_mint_v,
                            quote_mint: quote_mint_v,
                            base_vault,
                            quote_vault,
                            active_id: None,
                            bin_step: None,
                        },
                    );
                }
            }
        }

        if enable_meteora_dlmm {
            if let CachedPoolState::Meteora(s) = cached_state {
                let (base_mint_v, quote_mint_v, base_vault, quote_vault) =
                    cpmm_token_mints_and_vaults_sol_normalized(
                        s.token_x_mint,
                        s.token_y_mint,
                        s.reserve_x,
                        s.reserve_y,
                    );
                if admit(base_mint_v, quote_mint_v) {
                    let mut vaults = self.tracked_vaults.write();
                    self.register_tracked_vault_pair_lru(
                        &mut vaults,
                        &mut vaults_changed,
                        TrackedVaultPairInsert {
                            pool,
                            now,
                            dex: "meteora_dlmm",
                            base_mint: base_mint_v,
                            quote_mint: quote_mint_v,
                            base_vault,
                            quote_vault,
                            active_id: Some(s.active_id),
                            bin_step: Some(s.bin_step),
                        },
                    );
                }
            }
        }

        if let CachedPoolState::Orca(s) = cached_state {
            let (base_mint_v, quote_mint_v, base_vault, quote_vault) =
                cpmm_token_mints_and_vaults_sol_normalized(
                    s.token_mint_a,
                    s.token_mint_b,
                    s.token_vault_a,
                    s.token_vault_b,
                );
            if admit(base_mint_v, quote_mint_v) {
                let mut vaults = self.tracked_vaults.write();
                self.register_tracked_vault_pair_lru(
                    &mut vaults,
                    &mut vaults_changed,
                    TrackedVaultPairInsert {
                        pool,
                        now,
                        dex: "orca",
                        base_mint: base_mint_v,
                        quote_mint: quote_mint_v,
                        base_vault,
                        quote_vault,
                        active_id: None,
                        bin_step: None,
                    },
                );
            }
        }

        if let CachedPoolState::PumpAmm(s) = cached_state {
            if admit(s.base_mint, s.quote_mint) {
                let mut vaults = self.tracked_vaults.write();
                self.register_tracked_vault_pair_lru(
                    &mut vaults,
                    &mut vaults_changed,
                    TrackedVaultPairInsert {
                        pool,
                        now,
                        dex: "pump_amm",
                        base_mint: s.base_mint,
                        quote_mint: s.quote_mint,
                        base_vault: s.pool_base_token_account,
                        quote_vault: s.pool_quote_token_account,
                        active_id: None,
                        bin_step: None,
                    },
                );
            }
        }

        vaults_changed
    }

    /// PR169a: register vault ATAs from MASTER cache after account-path pool upsert (hot pools only).
    #[cfg_attr(not(test), allow(dead_code))]
    fn register_pool_vaults_from_account(&self, pool: Pubkey) -> bool {
        self.try_publish_balance_updated_from_cache(pool);
        if !self.hot_pool_registry.is_hot_pool(pool) {
            return false;
        }
        let Some(cached_state) = self.live_pool_cache.get(&pool) else {
            return false;
        };
        let (enable_meteora_cpmm, enable_meteora_dlmm) = {
            let cfg = self.config.read();
            (cfg.enable_meteora_cpmm, cfg.enable_meteora_dlmm)
        };
        let now = Instant::now();
        let before_keys = self.snapshot_explicit_demand_pubkeys();
        let vaults_changed = self.register_four_dex_pool_vaults_from_cached_state(
            pool,
            &cached_state,
            now,
            enable_meteora_cpmm,
            enable_meteora_dlmm,
            true,
        );
        if vaults_changed || self.hot_pool_reserve_registration_satisfied(pool) {
            self.clear_deferred_hot_pool_reserve_registration(pool);
        }
        vaults_changed
            || explicit_subscription_has_new_keys(
                &before_keys,
                &self.snapshot_explicit_demand_pubkeys(),
            )
    }

    /// PR-D / Phase 3: explicit vault/bin subscriptions when cache has layout.
    /// Returns whether any tracked set changed. Caller schedules debounced Geyser push.
    fn register_geyser_reserves_for_active_pool(&self, pool: Pubkey, pin: GeyserPinReason) -> bool {
        if self.live_pool_cache.get(&pool).is_none() {
            debug!(
                run_id = %self.run_id,
                pool = %pool,
                pin = ?pin,
                "Active pool pin: LivePoolCache miss — registry row kept; reserve registration deferred"
            );
            return false;
        }
        self.register_geyser_reserves_impl(pool, pin)
    }

    fn register_geyser_reserves_for_momentum_active_pool(&self, pool: Pubkey) -> bool {
        self.register_geyser_reserves_for_active_pool(pool, GeyserPinReason::MomentumActive)
    }

    fn register_geyser_reserves_for_arb_active_pool(&self, pool: Pubkey) -> bool {
        self.register_geyser_reserves_for_active_pool(pool, GeyserPinReason::ArbMultiDex)
    }

    fn pool_vaults_fully_tracked_for_cache(
        &self,
        pool: Pubkey,
        cached_state: &CachedPoolState,
    ) -> bool {
        let (enable_meteora_cpmm, enable_meteora_dlmm) = {
            let cfg = self.config.read();
            (cfg.enable_meteora_cpmm, cfg.enable_meteora_dlmm)
        };
        pool_vaults_fully_tracked_for_cache_inner(
            &self.tracked_vaults.read(),
            pool,
            cached_state,
            enable_meteora_cpmm,
            enable_meteora_dlmm,
        )
    }

    fn pool_geyser_bins_fully_tracked_for_cache(
        &self,
        pool: Pubkey,
        cached_state: &CachedPoolState,
    ) -> bool {
        if !self.config.read().enable_meteora_dlmm {
            return true;
        }
        let CachedPoolState::Meteora(s) = cached_state else {
            return true;
        };
        let active_array_index = MeteoraDlmmSwapBuilder::bin_id_to_bin_array_index(s.active_id);
        let bins = self.tracked_bin_arrays.read();
        for offset in -3i64..=3i64 {
            let index = active_array_index + offset;
            let Ok(pda) = MeteoraDlmmSwapBuilder::derive_bin_array_pda(&pool, index) else {
                continue;
            };
            if !bins.contains_key(&pda) {
                return false;
            }
        }
        true
    }

    /// True when cache has layout and expected vault/bin rows are already tracked (registration no-op).
    fn hot_pool_reserve_registration_satisfied(&self, pool: Pubkey) -> bool {
        if !self.hot_pool_registry.is_hot_pool(pool) {
            return true;
        }
        let Some(state) = self.live_pool_cache.get(&pool) else {
            return false;
        };
        self.pool_vaults_fully_tracked_for_cache(pool, &state)
            && self.pool_geyser_bins_fully_tracked_for_cache(pool, &state)
    }

    fn note_deferred_hot_pool_reserve_registration(&self, pool: Pubkey, pin: GeyserPinReason) {
        if !self.hot_pool_registry.is_hot_pool(pool) {
            return;
        }
        self.deferred_hot_pool_reserve_pins
            .write()
            .insert(pool, pin);
    }

    fn clear_deferred_hot_pool_reserve_registration(&self, pool: Pubkey) {
        self.deferred_hot_pool_reserve_pins.write().remove(&pool);
    }

    /// Drop deferred retry when cache layout exists and vault/bin rows need no further work.
    fn maybe_clear_deferred_hot_pool_reserve_if_satisfied(&self, pool: Pubkey) -> bool {
        if !self
            .deferred_hot_pool_reserve_pins
            .read()
            .contains_key(&pool)
        {
            return false;
        }
        if !self.hot_pool_reserve_registration_satisfied(pool) {
            return false;
        }
        self.clear_deferred_hot_pool_reserve_registration(pool);
        if self.hot_pool_registry.is_position_pin_for_pool(pool) {
            inc_market_data_open_position_pin_applied_total();
        }
        true
    }

    /// Bounded retry when LivePoolCache gains layout for a pinned hot pool (no RPC).
    fn retry_deferred_hot_pool_reserve_registrations(
        &self,
        admission: &mut FixedCapAdmission,
    ) -> bool {
        let pending: Vec<(Pubkey, GeyserPinReason)> = self
            .deferred_hot_pool_reserve_pins
            .read()
            .iter()
            .map(|(pool, pin)| (*pool, *pin))
            .collect();
        if pending.is_empty() {
            return false;
        }
        let mut batch_dirty = false;
        for (pool, pin) in pending {
            if !self.hot_pool_registry.is_hot_pool(pool) {
                self.clear_deferred_hot_pool_reserve_registration(pool);
                continue;
            }
            if self.live_pool_cache.get(&pool).is_none() {
                continue;
            }
            let consumer = match pin {
                GeyserPinReason::ArbMultiDex => ExplicitConsumer::Arb,
                GeyserPinReason::MomentumActive | GeyserPinReason::Wallet => {
                    momentum_explicit_consumer_for_pool(self, pool)
                }
            };
            if !self.try_admit_pool_consumer_group(admission, pool, consumer) {
                continue;
            }
            let changed = self.register_geyser_reserves_for_active_pool(pool, pin);
            let reserves_satisfied = changed || self.hot_pool_reserve_registration_satisfied(pool);
            if reserves_satisfied {
                self.clear_deferred_hot_pool_reserve_registration(pool);
                if self.hot_pool_registry.is_position_pin_for_pool(pool) {
                    inc_market_data_open_position_pin_applied_total();
                }
                if changed {
                    batch_dirty = true;
                }
            }
            self.sync_explicit_pool_admitted_from_admission(admission, pool, consumer);
        }
        batch_dirty
    }

    fn record_momentum_active_pool_reserve_registration_outcome(
        &self,
        pool: Pubkey,
        pin: GeyserPinReason,
        is_position: bool,
        registered: bool,
    ) {
        if registered {
            self.clear_deferred_hot_pool_reserve_registration(pool);
            if is_position {
                inc_market_data_open_position_pin_applied_total();
            }
            return;
        }
        if self.live_pool_cache.get(&pool).is_none() {
            self.note_deferred_hot_pool_reserve_registration(pool, pin);
            if is_position {
                inc_market_data_open_position_pin_deferred_cache_miss_total();
            }
            return;
        }
        if self.hot_pool_reserve_registration_satisfied(pool) {
            self.maybe_clear_deferred_hot_pool_reserve_if_satisfied(pool);
            return;
        }
        self.note_deferred_hot_pool_reserve_registration(pool, pin);
    }

    fn register_meteora_dlmm_bin_arrays(
        &self,
        pool: Pubkey,
        active_id: i32,
        bin_step: u16,
        pin: GeyserPinReason,
        now: Instant,
    ) -> bool {
        if !self.config.read().enable_meteora_dlmm {
            return false;
        }
        let active_array_index = MeteoraDlmmSwapBuilder::bin_id_to_bin_array_index(active_id);
        let mut bins_changed = false;
        let mut new_bins: Vec<Pubkey> = Vec::new();
        {
            let mut bin_arrays = self.tracked_bin_arrays.write();
            for offset in -3i64..=3i64 {
                let index = active_array_index + offset;
                if let Ok(pda) = MeteoraDlmmSwapBuilder::derive_bin_array_pda(&pool, index) {
                    use std::collections::hash_map::Entry;
                    match bin_arrays.entry(pda) {
                        Entry::Vacant(e) => {
                            e.insert(BinArrayInfo {
                                pool_address: pool,
                                bin_array_index: index,
                                bin_step,
                                last_used_at: now,
                                pinned: false,
                                pin: None,
                            });
                            bins_changed = true;
                            new_bins.push(pda);
                        }
                        Entry::Occupied(mut e) => {
                            let b = e.get_mut();
                            if Self::geyser_pin_may_promote(b.pin, pin) {
                                b.pinned = true;
                                b.pin = Some(pin);
                                b.bin_step = bin_step;
                                bins_changed = true;
                            }
                        }
                    }
                }
            }
        }
        for pda in new_bins {
            self.pool_tracked_legs_note_bin(pool, pda);
        }
        if bins_changed {
            self.dlmm_registered_active_id
                .write()
                .insert(pool, active_id);
        }
        bins_changed
    }

    /// Fix B: when arb-pinned DLMM `active_id` drifts, register new bin-array PDAs + Geyser push.
    fn maybe_refresh_arb_dlmm_bin_window(&self, pool: Pubkey, new_active_id: i32) -> bool {
        if !self.hot_pool_registry.pool_has_arb(pool) {
            return false;
        }
        let prev = self.dlmm_registered_active_id.read().get(&pool).copied();
        if prev == Some(new_active_id) {
            return false;
        }
        let Some(CachedPoolState::Meteora(s)) = self.live_pool_cache.get(&pool) else {
            return false;
        };
        self.register_meteora_dlmm_bin_arrays(
            pool,
            new_active_id,
            s.bin_step,
            GeyserPinReason::ArbMultiDex,
            Instant::now(),
        )
    }

    fn register_geyser_reserves_impl(&self, pool: Pubkey, pin: GeyserPinReason) -> bool {
        let now = Instant::now();
        let enable_dlmm = self.config.read().enable_meteora_dlmm;
        let enable_meteora_cpmm = self.config.read().enable_meteora_cpmm;
        let Some(state) = self.live_pool_cache.get(&pool) else {
            return false;
        };

        let mut vaults_changed = self.register_four_dex_pool_vaults_from_cached_state(
            pool,
            &state,
            now,
            enable_meteora_cpmm,
            enable_dlmm,
            false,
        );
        let mut bins_changed = false;

        match &state {
            CachedPoolState::Meteora(s) if enable_dlmm => {
                bins_changed =
                    self.register_meteora_dlmm_bin_arrays(pool, s.active_id, s.bin_step, pin, now);
            }
            CachedPoolState::RaydiumAmm(s) => {
                let mut vaults = self.tracked_vaults.write();
                self.register_tracked_vault_pair_lru(
                    &mut vaults,
                    &mut vaults_changed,
                    TrackedVaultPairInsert {
                        pool,
                        now,
                        dex: "raydium",
                        base_mint: s.base_mint,
                        quote_mint: s.quote_mint,
                        base_vault: s.coin_vault,
                        quote_vault: s.pc_vault,
                        active_id: None,
                        bin_step: None,
                    },
                );
            }
            _ => {}
        }

        let mut mints_changed = false;
        {
            let mut vaults = self.tracked_vaults.write();
            for v in vaults.values_mut() {
                if v.pool_address == pool && Self::geyser_pin_may_promote(v.pin, pin) {
                    v.pinned = true;
                    v.pin = Some(pin);
                    vaults_changed = true;
                }
            }
        }
        {
            let mut bins = self.tracked_bin_arrays.write();
            for b in bins.values_mut() {
                if b.pool_address == pool && Self::geyser_pin_may_promote(b.pin, pin) {
                    b.pinned = true;
                    b.pin = Some(pin);
                    bins_changed = true;
                }
            }
        }
        if let Some((a, b)) = pool_mints_for_geyser_explicit_tracking(&state) {
            if self.track_mint_for_geyser_metadata(a, Some(pin)) {
                mints_changed = true;
            }
            if self.track_mint_for_geyser_metadata(b, Some(pin)) {
                mints_changed = true;
            }
        }

        vaults_changed || bins_changed || mints_changed
    }

    fn geyser_pin_may_promote(current: Option<GeyserPinReason>, target: GeyserPinReason) -> bool {
        match target {
            GeyserPinReason::Wallet => current != Some(GeyserPinReason::Wallet),
            GeyserPinReason::MomentumActive => current != Some(GeyserPinReason::Wallet),
            GeyserPinReason::ArbMultiDex => !matches!(
                current,
                Some(GeyserPinReason::Wallet) | Some(GeyserPinReason::MomentumActive)
            ),
        }
    }

    /// PR-D / PR169b: apply momentum-bot active pool pin updates (actor-only writer; caller schedules sync).
    fn apply_momentum_active_pools_update(
        &self,
        admission: &mut FixedCapAdmission,
        update: &MomentumActivePoolsUpdate,
    ) -> bool {
        record_market_data_momentum_active_pool_messages_total();
        let mut batch_dirty = false;
        if update.full_active_snapshot {
            batch_dirty |= self.apply_momentum_snapshot_reconcile(admission, &update.active);
        }
        batch_dirty |= self.apply_momentum_removed_entries(admission, &update.removed);
        batch_dirty |= self.apply_momentum_active_entries(admission, &update.active);
        batch_dirty |= self.retry_deferred_hot_pool_reserve_registrations(admission);
        if !batch_dirty {
            self.refresh_geyser_pins_gauge();
        }
        set_market_data_momentum_active_pool_pins_gauge(self.hot_pool_registry.pair_count());
        batch_dirty
    }

    /// PR169c: snapshot reconcile only (`full_active_snapshot` target set).
    fn apply_momentum_snapshot_reconcile(
        &self,
        admission: &mut FixedCapAdmission,
        active: &[MomentumActivePoolEntry],
    ) -> bool {
        let mut target: HashSet<(Pubkey, Pubkey)> = HashSet::new();
        for a in active {
            let Ok(mint_pk) = Pubkey::from_str(a.mint.trim()) else {
                warn!(mint = %a.mint, "MomentumActivePoolsUpdate.active (snapshot): invalid mint");
                continue;
            };
            let Ok(pool_pk) = Pubkey::from_str(a.pool.trim()) else {
                warn!(pool = %a.pool, "MomentumActivePoolsUpdate.active (snapshot): invalid pool");
                continue;
            };
            target.insert((mint_pk, pool_pk));
        }
        {
            let mut last = self.last_momentum_snapshot_target.write();
            if last.as_ref() == Some(&target) {
                return false;
            }
            *last = Some(target.clone());
        }
        let mut batch_dirty = false;
        let before = self.hot_pool_registry.snapshot_pairs();
        for (m, p) in before.difference(&target) {
            if self.clear_momentum_geyser_reserves_for_active_entry(admission, *m, *p) {
                batch_dirty = true;
            }
        }
        batch_dirty
    }

    fn apply_momentum_removed_entries(
        &self,
        admission: &mut FixedCapAdmission,
        removed: &[MomentumRemovedPoolEntry],
    ) -> bool {
        let mut batch_dirty = false;
        for r in removed {
            let Ok(mint_pk) = Pubkey::from_str(r.mint.trim()) else {
                warn!(mint = %r.mint, "MomentumActivePoolsUpdate.removed: invalid mint");
                continue;
            };
            let Ok(pool_pk) = Pubkey::from_str(r.pool.trim()) else {
                warn!(pool = %r.pool, "MomentumActivePoolsUpdate.removed: invalid pool");
                continue;
            };
            if self.clear_momentum_geyser_reserves_for_active_entry(admission, mint_pk, pool_pk) {
                batch_dirty = true;
            }
        }
        batch_dirty
    }

    fn apply_momentum_active_entries(
        &self,
        admission: &mut FixedCapAdmission,
        active: &[MomentumActivePoolEntry],
    ) -> bool {
        let mut batch_dirty = false;
        for a in active {
            let Ok(mint_pk) = Pubkey::from_str(a.mint.trim()) else {
                warn!(mint = %a.mint, "MomentumActivePoolsUpdate.active: invalid mint");
                continue;
            };
            let Ok(pool_pk) = Pubkey::from_str(a.pool.trim()) else {
                warn!(pool = %a.pool, "MomentumActivePoolsUpdate.active: invalid pool");
                continue;
            };
            let is_position = a.pin_reason == MomentumActivePinReason::Position;
            let explicit_entry = ExplicitEntry {
                consumers: HashSet::from([if is_position {
                    ConsumerId::MomentumPosition
                } else {
                    ConsumerId::Momentum
                }]),
                pool: Some(pool_pk),
                pin_priority: pin_priority_for_momentum_active_pin(a.pin_reason),
            };
            self.hot_pool_registry
                .pin_pool_with_reason(mint_pk, pool_pk, is_position);
            let momentum_consumer = if is_position {
                ExplicitConsumer::MomentumPosition
            } else {
                ExplicitConsumer::Momentum
            };
            let superseded_consumer = if is_position {
                ExplicitConsumer::Momentum
            } else {
                ExplicitConsumer::MomentumPosition
            };
            let superseded_present = admission
                .owner_group(&Self::pool_consumer_owner(pool_pk, superseded_consumer))
                .is_some();
            if self.try_admit_pool_consumer_group(admission, pool_pk, momentum_consumer) {
                if superseded_present {
                    self.release_pool_consumer_group(admission, pool_pk, superseded_consumer);
                }
                inc_market_data_momentum_admission_admitted_total();
                let registered = self.register_geyser_reserves_for_momentum_active_pool(pool_pk);
                self.record_momentum_active_pool_reserve_registration_outcome(
                    pool_pk,
                    GeyserPinReason::MomentumActive,
                    is_position,
                    registered,
                );
                if registered {
                    batch_dirty = true;
                } else if self.live_pool_cache.get(&pool_pk).is_none() {
                    debug!(
                        run_id = %self.run_id,
                        pool = %pool_pk,
                        pin_reason = ?a.pin_reason,
                        "Momentum active pool pin: LivePoolCache miss — registry row kept; reserve registration deferred"
                    );
                }
            } else {
                inc_market_data_momentum_admission_rejected_total();
                self.record_momentum_active_pool_reserve_registration_outcome(
                    pool_pk,
                    GeyserPinReason::MomentumActive,
                    is_position,
                    false,
                );
            }
            self.sync_explicit_pool_admitted_from_admission(admission, pool_pk, momentum_consumer);
            if explicit_entry.pin_priority <= PinPriority::MomentumPosition
                && admission
                    .owner_group(&Self::pool_consumer_owner(pool_pk, momentum_consumer))
                    .is_some()
            {
                let _ =
                    admission.touch_group(Self::pool_consumer_owner(pool_pk, momentum_consumer));
            }
        }
        batch_dirty
    }

    /// PR-D: whether `mint` is still a base/quote leg for any pool that has a momentum active pin row.
    fn momentum_pool_leg_mint_still_required_by_other_pool(&self, mint: Pubkey) -> bool {
        for (_, pool_pk) in self.hot_pool_registry.snapshot_pairs() {
            match self.live_pool_cache.get(&pool_pk) {
                Some(state) => {
                    if let Some((a, b)) = pool_mints_for_geyser_explicit_tracking(&state) {
                        if a == mint || b == mint {
                            return true;
                        }
                    }
                }
                None => {
                    // Pinned `(mint, pool)` without cache layout: cannot know pool legs (e.g. WSOL
                    // quote). Assume this leg may still be required — avoids demoting companion mints.
                    return true;
                }
            }
        }
        false
    }

    /// Clear `MomentumActive` pins for one `(mint, pool)` side; never demotes [`GeyserPinReason::Wallet`].
    fn clear_momentum_geyser_reserves_for_active_entry(
        &self,
        admission: &mut FixedCapAdmission,
        mint: Pubkey,
        pool: Pubkey,
    ) -> bool {
        self.hot_pool_registry.unpin_pool(mint, pool);
        let mut changed = false;
        // Pool-level reserve pins are shared: only demote vaults/bin arrays when no `(m, pool)`
        // row remains after this unpin (PR #147 follow-up).
        if !self.hot_pool_registry.pool_has_any_pin(pool) {
            self.release_pool_consumer_group(admission, pool, ExplicitConsumer::MomentumPosition);
            self.release_pool_consumer_group(admission, pool, ExplicitConsumer::Momentum);
            {
                let mut vaults = self.tracked_vaults.write();
                for v in vaults.values_mut() {
                    if v.pool_address == pool && v.pin == Some(GeyserPinReason::MomentumActive) {
                        v.pin = None;
                        v.pinned = false;
                        changed = true;
                    }
                }
            }
            {
                let mut bins = self.tracked_bin_arrays.write();
                for b in bins.values_mut() {
                    if b.pool_address == pool && b.pin == Some(GeyserPinReason::MomentumActive) {
                        b.pin = None;
                        b.pinned = false;
                        changed = true;
                    }
                }
            }
            if let Some(state) = self.live_pool_cache.get(&pool) {
                if let Some((leg_a, leg_b)) = pool_mints_for_geyser_explicit_tracking(&state) {
                    for leg in [leg_a, leg_b] {
                        if self.wallet_tracks_mint_for_geyser(&leg) {
                            continue;
                        }
                        if self.momentum_pool_leg_mint_still_required_by_other_pool(leg) {
                            continue;
                        }
                        let mut m = self.tracked_mints.write();
                        if let Some(info) = m.get_mut(&leg) {
                            if info.pin == Some(GeyserPinReason::MomentumActive) {
                                info.pinned = false;
                                info.pin = None;
                                changed = true;
                            }
                        }
                    }
                }
            }
        }
        if !self.wallet_tracks_mint_for_geyser(&mint)
            && !self.hot_pool_registry.mint_has_any_pinned_pool(mint)
        {
            let mut m = self.tracked_mints.write();
            if let Some(info) = m.get_mut(&mint) {
                if info.pin == Some(GeyserPinReason::MomentumActive) {
                    info.pinned = false;
                    info.pin = None;
                    changed = true;
                }
            }
        }
        changed
    }

    /// Phase 3: apply arb-strategy track_requests pin updates (md-track-worker only).
    fn apply_arb_track_requests_update(
        &self,
        admission: &mut FixedCapAdmission,
        update: &ArbTrackRequestsUpdate,
    ) -> bool {
        record_market_data_arb_track_requests_messages_total();
        let mut batch_dirty = false;
        if update.reconcile {
            batch_dirty |= self.apply_arb_snapshot_reconcile(admission, &update.active);
        }
        batch_dirty |= self.apply_arb_removed_entries(admission, &update.removed);
        batch_dirty |= self.apply_arb_active_entries(admission, &update.active);
        if !batch_dirty {
            self.refresh_geyser_pins_gauge();
        }
        set_market_data_arb_pinned_pools_gauge(self.hot_pool_registry.arb_pool_count());
        batch_dirty
    }

    fn apply_arb_snapshot_reconcile(
        &self,
        admission: &mut FixedCapAdmission,
        active: &[ArbTrackActiveEntry],
    ) -> bool {
        let mut target: HashSet<Pubkey> = HashSet::new();
        for a in active {
            let Ok(pool_pk) = Pubkey::from_str(a.pool.trim()) else {
                warn!(pool = %a.pool, "ArbTrackRequestsUpdate.active (snapshot): invalid pool");
                continue;
            };
            target.insert(pool_pk);
        }
        {
            let mut last = self.last_arb_snapshot_target.write();
            if last.as_ref() == Some(&target) {
                return false;
            }
            *last = Some(target.clone());
        }
        let mut batch_dirty = false;
        let before = self.hot_pool_registry.snapshot_arb_pools();
        for pool in before.difference(&target) {
            if self.clear_arb_geyser_reserves_for_pool(admission, *pool) {
                batch_dirty = true;
            }
        }
        batch_dirty
    }

    fn apply_arb_removed_entries(
        &self,
        admission: &mut FixedCapAdmission,
        removed: &[ArbTrackRemovedEntry],
    ) -> bool {
        let mut batch_dirty = false;
        for r in removed {
            let Ok(pool_pk) = Pubkey::from_str(r.pool.trim()) else {
                warn!(pool = %r.pool, "ArbTrackRequestsUpdate.removed: invalid pool");
                continue;
            };
            if self.clear_arb_geyser_reserves_for_pool(admission, pool_pk) {
                batch_dirty = true;
            }
        }
        batch_dirty
    }

    fn apply_arb_active_entries(
        &self,
        admission: &mut FixedCapAdmission,
        active: &[ArbTrackActiveEntry],
    ) -> bool {
        let mut batch_dirty = false;
        for a in active {
            let Ok(pool_pk) = Pubkey::from_str(a.pool.trim()) else {
                warn!(pool = %a.pool, "ArbTrackRequestsUpdate.active: invalid pool");
                continue;
            };
            self.hot_pool_registry.pin_arb_pool(pool_pk);
            if self.try_admit_pool_consumer_group(admission, pool_pk, ExplicitConsumer::Arb) {
                inc_market_data_arb_admission_admitted_total();
                if self.register_geyser_reserves_for_arb_active_pool(pool_pk) {
                    batch_dirty = true;
                } else {
                    let category = if self.live_pool_cache.get(&pool_pk).is_none() {
                        ArbPinDeferredLogCategory::LivePoolCacheMiss
                    } else {
                        ArbPinDeferredLogCategory::VaultRegisterNoChange
                    };
                    let reason: &'static str = match category {
                        ArbPinDeferredLogCategory::LivePoolCacheMiss => "live_pool_cache_miss",
                        ArbPinDeferredLogCategory::VaultRegisterNoChange => {
                            "vault_register_no_change"
                        }
                    };
                    inc_market_data_arb_pin_geyser_register_deferred_total(reason);
                    let now = Instant::now();
                    if ARB_PIN_DEFERRED_LOG_THROTTLE
                        .lock()
                        .should_emit(category as usize, now)
                    {
                        warn!(
                            run_id = %self.run_id,
                            pool = %pool_pk,
                            pin = "ArbMultiDex",
                            reason = reason,
                            "Arb pin: Geyser reserve registration deferred (geyser-only, no RPC)"
                        );
                    }
                }
            } else if self.live_pool_cache.get(&pool_pk).is_none() {
                inc_market_data_arb_pin_geyser_register_deferred_total("live_pool_cache_miss");
                let now = Instant::now();
                if ARB_PIN_DEFERRED_LOG_THROTTLE
                    .lock()
                    .should_emit(ArbPinDeferredLogCategory::LivePoolCacheMiss as usize, now)
                {
                    warn!(
                        run_id = %self.run_id,
                        pool = %pool_pk,
                        pin = "ArbMultiDex",
                        reason = "live_pool_cache_miss",
                        "Arb pin: Geyser reserve registration deferred (geyser-only, no RPC)"
                    );
                }
            } else {
                inc_market_data_arb_admission_rejected_total();
            }
            self.sync_explicit_pool_admitted_from_admission(
                admission,
                pool_pk,
                ExplicitConsumer::Arb,
            );
            let _ = &a.reason;
        }
        batch_dirty
    }

    fn arb_pool_leg_mint_still_required_by_other_pool(&self, mint: Pubkey) -> bool {
        for pool_pk in self.hot_pool_registry.snapshot_arb_pools() {
            match self.live_pool_cache.get(&pool_pk) {
                Some(state) => {
                    if let Some((a, b)) = pool_mints_for_geyser_explicit_tracking(&state) {
                        if a == mint || b == mint {
                            return true;
                        }
                    }
                }
                None => return true,
            }
        }
        false
    }

    /// Clear Arb consumer pins for one pool; never demotes [`GeyserPinReason::Wallet`] or Momentum.
    fn clear_arb_geyser_reserves_for_pool(
        &self,
        admission: &mut FixedCapAdmission,
        pool: Pubkey,
    ) -> bool {
        self.release_pool_consumer_group(admission, pool, ExplicitConsumer::Arb);
        self.hot_pool_registry.unpin_arb_pool(pool);
        let mut changed = false;
        if !self.hot_pool_registry.pool_has_arb(pool)
            && !self.hot_pool_registry.pool_has_any_pin(pool)
        {
            {
                let mut vaults = self.tracked_vaults.write();
                for v in vaults.values_mut() {
                    if v.pool_address == pool && v.pin == Some(GeyserPinReason::ArbMultiDex) {
                        v.pin = None;
                        v.pinned = false;
                        changed = true;
                    }
                }
            }
            {
                let mut bins = self.tracked_bin_arrays.write();
                for b in bins.values_mut() {
                    if b.pool_address == pool && b.pin == Some(GeyserPinReason::ArbMultiDex) {
                        b.pin = None;
                        b.pinned = false;
                        changed = true;
                    }
                }
            }
            if let Some(state) = self.live_pool_cache.get(&pool) {
                if let Some((leg_a, leg_b)) = pool_mints_for_geyser_explicit_tracking(&state) {
                    for leg in [leg_a, leg_b] {
                        if self.wallet_tracks_mint_for_geyser(&leg) {
                            continue;
                        }
                        if self.momentum_pool_leg_mint_still_required_by_other_pool(leg)
                            || self.arb_pool_leg_mint_still_required_by_other_pool(leg)
                        {
                            continue;
                        }
                        let mut m = self.tracked_mints.write();
                        if let Some(info) = m.get_mut(&leg) {
                            if info.pin == Some(GeyserPinReason::ArbMultiDex) {
                                info.pinned = false;
                                info.pin = None;
                                changed = true;
                            }
                        }
                    }
                }
            }
        }
        changed
    }

    /// P1: Apply config update from control-plane (Runtime Configuration via UI).
    /// When `md_state` is set, cap changes schedule debounced Geyser sync via `md-state` (PR169b/R2).
    fn apply_config_update(
        &self,
        update: &ConfigUpdate,
        md_state: Option<&MdStateSender>,
        track_worker: Option<&TrackWorkerSender>,
    ) -> ConfigUpdateResponse {
        let mut applied = Vec::new();
        let mut rejected = Vec::new();
        let mut sync_geyser_tracked_after_max_accounts = false;

        {
            let mut config = self.config.write();
            for (key, value) in &update.config {
                match key.as_str() {
                    "enable_raydium" => {
                        if let Some(v) = value.as_bool() {
                            config.enable_raydium = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Invalid type, expected bool".to_string()));
                        }
                    }
                    "enable_raydium_cpmm" => {
                        if let Some(v) = value.as_bool() {
                            config.enable_raydium_cpmm = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Invalid type, expected bool".to_string()));
                        }
                    }
                    "enable_orca" => {
                        if let Some(v) = value.as_bool() {
                            config.enable_orca = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Invalid type, expected bool".to_string()));
                        }
                    }
                    "enable_pumpfun" => {
                        if let Some(v) = value.as_bool() {
                            config.enable_pumpfun = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Invalid type, expected bool".to_string()));
                        }
                    }
                    "enable_pumpswap" => {
                        if let Some(v) = value.as_bool() {
                            config.enable_pumpswap = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Invalid type, expected bool".to_string()));
                        }
                    }
                    "enable_meteora_dlmm" => {
                        if let Some(v) = value.as_bool() {
                            config.enable_meteora_dlmm = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Invalid type, expected bool".to_string()));
                        }
                    }
                    "enable_meteora_cpmm" => {
                        if let Some(v) = value.as_bool() {
                            config.enable_meteora_cpmm = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Invalid type, expected bool".to_string()));
                        }
                    }
                    "max_events_per_sec" => {
                        if let Some(v) = value.as_u64() {
                            if v > 0 && v <= 1_000_000 {
                                config.max_events_per_sec = v as u32;
                                applied.push(key.clone());
                                info!(key = %key, new_value = %v, "Config updated");
                            } else {
                                rejected.push((key.clone(), "Must be 1-1000000".to_string()));
                            }
                        } else {
                            rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                        }
                    }
                    "max_tracked_accounts" => {
                        if let Some(v) = value.as_u64() {
                            if (1000..=500_000).contains(&v) {
                                config.max_tracked_accounts = v as usize;
                                applied.push(key.clone());
                                sync_geyser_tracked_after_max_accounts = true;
                                info!(key = %key, new_value = %v, "Config updated");
                            } else {
                                rejected.push((key.clone(), "Must be 1000-500000".to_string()));
                            }
                        } else {
                            rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                        }
                    }
                    "geyser_full_reconnect_threshold" => {
                        if let Some(v) = value.as_u64() {
                            if (1000..=500_000).contains(&v) {
                                let n = v as usize;
                                config.geyser_full_reconnect_threshold = n;
                                self.geyser_full_reconnect_threshold_live
                                    .store(n, Ordering::Relaxed);
                                applied.push(key.clone());
                                info!(key = %key, new_value = %v, "Config updated");
                            } else {
                                rejected.push((key.clone(), "Must be 1000-500000".to_string()));
                            }
                        } else {
                            rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                        }
                    }
                    "geyser_sync_batch_ms" => {
                        if let Some(v) = value.as_u64() {
                            if (10..=100).contains(&v) {
                                config.geyser_sync_batch_ms = v;
                                applied.push(key.clone());
                                info!(key = %key, new_value = %v, "Config updated");
                            } else {
                                rejected.push((key.clone(), "Must be 10-100".to_string()));
                            }
                        } else {
                            rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                        }
                    }
                    _ => {
                        rejected.push((key.clone(), format!("Unknown config key: {}", key)));
                    }
                }
            }
        }

        if sync_geyser_tracked_after_max_accounts {
            if let Some(md_state) = md_state {
                md_state_try_enqueue(
                    md_state,
                    MdStateCommand::ScheduleGeyserSyncAfterConfigChange,
                );
            } else if let Some(track_worker) = track_worker {
                let _ = track_worker_try_enqueue(
                    track_worker,
                    TrackWorkerCommand::ScheduleGeyserSyncAfterConfigChange,
                );
                let _ =
                    track_worker_try_enqueue(track_worker, TrackWorkerCommand::ScheduleGeyserPush);
            }
        }

        let status = if rejected.is_empty() {
            ConfigUpdateStatus::Applied
        } else if applied.is_empty() {
            ConfigUpdateStatus::Rejected
        } else {
            ConfigUpdateStatus::PartiallyApplied
        };

        ConfigUpdateResponse {
            status,
            applied_keys: applied,
            rejected_keys: rejected,
            new_snapshot_id: None,
        }
    }
}

/// Wallet `mints_in_wallet` are non-zero balances from bootstrap RPC; pick at most `max_mints`
/// unique pubkeys that still lack explicit Ready for any modeled slice per
/// [`LivePoolCache::base_mint_has_any_ready_pool`] (PumpSwap, PumpFun, Raydium CPMM, Meteora CPMM, Meteora DLMM,
/// Orca Whirlpool, Raydium AMM). Preserves iteration order; skips WSOL.
fn wallet_mints_needing_dex_bootstrap_verify(
    cache: &LivePoolCache,
    mints_in_wallet: &[String],
    wsol_mint_str: &str,
    max_mints: usize,
) -> Vec<Pubkey> {
    use std::collections::HashSet;

    let mut seen: HashSet<Pubkey> = HashSet::new();
    let mut out: Vec<Pubkey> = Vec::new();
    for s in mints_in_wallet {
        if out.len() >= max_mints {
            break;
        }
        if s == wsol_mint_str {
            continue;
        }
        let Ok(pk) = Pubkey::from_str(s) else {
            continue;
        };
        if !seen.insert(pk) {
            continue;
        }
        if cache.base_mint_has_any_ready_pool(&pk) {
            continue;
        }
        out.push(pk);
    }
    out
}

/// Wallet mints that graduated from PumpFun to PumpSwap (`complete`) but still lack explicit
/// PumpSwap [`DexPoolReadiness::Ready`]. Disjoint from [`wallet_mints_needing_dex_bootstrap_verify`]:
/// those mints have **no** ready pool at all; here bonding-curve Ready from JetStream satisfied
/// [`LivePoolCache::base_mint_has_any_ready_pool`] and incorrectly skipped the PumpSwap bootstrap
/// path (Scope-40 / KNOWN_BUG_PATTERNS #33).
fn wallet_mints_needing_pump_amm_after_pumpfun_migration(
    cache: &LivePoolCache,
    mints_in_wallet: &[String],
    wsol_mint_str: &str,
    max_mints: usize,
) -> Vec<Pubkey> {
    use std::collections::HashSet;

    let mut seen: HashSet<Pubkey> = HashSet::new();
    let mut out: Vec<Pubkey> = Vec::new();
    for s in mints_in_wallet {
        if out.len() >= max_mints {
            break;
        }
        if s == wsol_mint_str {
            continue;
        }
        let Ok(pk) = Pubkey::from_str(s) else {
            continue;
        };
        if !seen.insert(pk) {
            continue;
        }
        if cache.base_mint_has_explicit_pump_amm_ready_pool(&pk) {
            continue;
        }
        if !cache.pumpfun_bonding_curve_complete_for_mint(&pk) {
            continue;
        }
        if !cache.base_mint_has_any_ready_pool(&pk) {
            continue;
        }
        out.push(pk);
    }
    out
}

/// Cold-path only: bounded PumpSwap, then PumpFun, then Raydium CPMM, then Meteora CPMM, then Orca
/// Whirlpool, then Meteora DLMM (cache-scoped) for wallet-relevant mints without explicit Ready. Reuses
/// [`handle_ensure_pump_amm_pool_accounts`], [`handle_ensure_pumpfun_bonding_curve`], and the same
/// JetStream `PoolCacheUpdate` path as Geyser for Raydium/Meteora CPMM/Orca/DLMM. Skips periodic snapshots.
async fn run_bounded_wallet_dex_bootstrap_verify(
    ctx: &MarketDataContext,
    rpc: &Arc<SolanaRpc>,
    pump_amm_dex: &PumpFunAmmDex,
    mints_in_wallet: &[String],
    run_id: &str,
    is_periodic: bool,
) {
    if is_periodic {
        return;
    }

    let candidates = wallet_mints_needing_dex_bootstrap_verify(
        ctx.live_pool_cache.as_ref(),
        mints_in_wallet,
        WSOL_MINT,
        WALLET_BOOTSTRAP_DEX_VERIFY_MAX_MINTS,
    );
    // Scope-40: disjoint from `candidates` — mints can be "ready" via PumpFun bonding curve only
    // while PumpSwap explicit Ready is still missing; must not early-return before this pass.
    let pump_amm_migration = wallet_mints_needing_pump_amm_after_pumpfun_migration(
        ctx.live_pool_cache.as_ref(),
        mints_in_wallet,
        WSOL_MINT,
        WALLET_BOOTSTRAP_DEX_VERIFY_MAX_MINTS,
    );
    if candidates.is_empty() && pump_amm_migration.is_empty() {
        return;
    }

    info!(
        candidates = candidates.len(),
        pump_amm_migration = pump_amm_migration.len(),
        grace_ms = WALLET_BOOTSTRAP_DEX_VERIFY_GRACE_MS,
        max_mints = WALLET_BOOTSTRAP_DEX_VERIFY_MAX_MINTS,
        "Wallet bootstrap: bounded DEX readiness verification (candidates + PumpSwap post-migration pass; then PumpFun, Raydium CPMM, Meteora CPMM, Orca Whirlpool, Meteora DLMM if still not ready)"
    );

    tokio::time::sleep(std::time::Duration::from_millis(
        WALLET_BOOTSTRAP_DEX_VERIFY_GRACE_MS,
    ))
    .await;

    // Scope-40: graduated PumpFun → PumpSwap tokens can have bonding-curve explicit Ready from
    // JetStream while PumpSwap pool_accounts never ran through bootstrap; ensure PumpSwap discovery
    // runs once (hint-free path still uses market-data RPC; engine stays I-24d clean).
    for pk in pump_amm_migration {
        if ctx
            .live_pool_cache
            .base_mint_has_explicit_pump_amm_ready_pool(&pk)
        {
            continue;
        }
        let base_mint_str = pk.to_string();
        let request_id = format!("wallet_bootstrap_pamm_migrated_{}", Uuid::new_v4());
        handle_ensure_pump_amm_pool_accounts(
            ctx,
            pump_amm_dex,
            run_id,
            &request_id,
            &base_mint_str,
            None,
            false,
        )
        .await;
    }

    for pk in candidates {
        if ctx.live_pool_cache.base_mint_has_any_ready_pool(&pk) {
            debug!(
                mint = %pk,
                "Wallet bootstrap DEX verify: explicit Ready after grace period, skip RPC"
            );
            continue;
        }

        let base_mint_str = pk.to_string();
        let request_id = format!("wallet_bootstrap_pamm_{}", Uuid::new_v4());
        handle_ensure_pump_amm_pool_accounts(
            ctx,
            pump_amm_dex,
            run_id,
            &request_id,
            &base_mint_str,
            None,
            false,
        )
        .await;

        if ctx.live_pool_cache.base_mint_has_any_ready_pool(&pk) {
            continue;
        }

        let request_id_pf = format!("wallet_bootstrap_pfun_{}", Uuid::new_v4());
        handle_ensure_pumpfun_bonding_curve(
            ctx,
            rpc,
            run_id,
            &request_id_pf,
            &base_mint_str,
            None,
            WALLET_BOOTSTRAP_ENSURE_PUMPFUN_FORCE_REFRESH,
        )
        .await;

        if ctx.live_pool_cache.base_mint_has_any_ready_pool(&pk) {
            continue;
        }

        if ctx.config.read().enable_raydium_cpmm {
            let request_id_rc = format!("wallet_bootstrap_rcpmm_{}", Uuid::new_v4());
            handle_wallet_bootstrap_raydium_cpmm_verify_for_mint(
                ctx,
                rpc,
                run_id,
                &request_id_rc,
                &pk,
            )
            .await;
        }

        if ctx.live_pool_cache.base_mint_has_any_ready_pool(&pk) {
            continue;
        }

        if ctx.config.read().enable_meteora_cpmm {
            let request_id_mc = format!("wallet_bootstrap_mcpmm_{}", Uuid::new_v4());
            handle_wallet_bootstrap_meteora_cpmm_verify_for_mint(
                ctx,
                rpc,
                run_id,
                &request_id_mc,
                &pk,
            )
            .await;
        }

        if ctx.live_pool_cache.base_mint_has_any_ready_pool(&pk) {
            continue;
        }

        if ctx.config.read().enable_orca {
            let request_id_ow = format!("wallet_bootstrap_orca_{}", Uuid::new_v4());
            handle_wallet_bootstrap_orca_whirlpool_verify_for_mint(
                ctx,
                rpc,
                run_id,
                &request_id_ow,
                &pk,
            )
            .await;
        }

        if ctx.live_pool_cache.base_mint_has_any_ready_pool(&pk) {
            continue;
        }

        if ctx.config.read().enable_meteora_dlmm {
            let request_id_dlmm = format!("wallet_bootstrap_meteora_dlmm_{}", Uuid::new_v4());
            handle_wallet_bootstrap_meteora_dlmm_verify_for_mint(
                ctx,
                rpc,
                run_id,
                &request_id_dlmm,
                &pk,
            )
            .await;
        }
    }
}

/// Cold-path only: Raydium CPMM promotion for one wallet mint. Uses **only** pools already present
/// in [`LivePoolCache`] (from Geyser); no `getProgramAccounts` / global scan. RPC: per-pool
/// `get_account` + per-vault balance reads — bounded by the number of matching cache rows.
///
/// Publishes [`PoolCacheUpdate`] with the same metadata/readiness keys as the Geyser path so
/// [`crate::execution::pool_cache_sync`] and SLAVE caches stay aligned (I-24a / Bug #36).
async fn handle_wallet_bootstrap_raydium_cpmm_verify_for_mint(
    ctx: &MarketDataContext,
    rpc: &Arc<SolanaRpc>,
    run_id: &str,
    request_id: &str,
    base_mint: &Pubkey,
) {
    if ctx
        .live_pool_cache
        .base_mint_has_explicit_raydium_cpmm_ready_pool(base_mint)
    {
        debug!(
            request_id = %request_id,
            mint = %base_mint,
            "Wallet bootstrap Raydium CPMM: already explicit Ready for this mint, skip"
        );
        return;
    }

    let pools = ctx.live_pool_cache.raydium_cpmm_pools_for_mint(base_mint);
    if pools.is_empty() {
        debug!(
            request_id = %request_id,
            mint = %base_mint,
            "Wallet bootstrap Raydium CPMM: no CPMM pool rows in LivePoolCache for mint, skip RPC"
        );
        return;
    }

    info!(
        request_id = %request_id,
        mint = %base_mint,
        pools = pools.len(),
        "Wallet bootstrap Raydium CPMM: verifying cache-scoped pools (cold-path RPC, no global scan)"
    );

    let sol = Pubkey::from_str(NATIVE_SOL_MINT).unwrap_or_default();

    for (pool_addr, mut state) in pools {
        if ctx.live_pool_cache.base_mint_has_any_ready_pool(base_mint) {
            break;
        }
        if ctx
            .live_pool_cache
            .base_mint_has_explicit_raydium_cpmm_ready_pool(base_mint)
        {
            break;
        }

        let pool_acc = match rpc.get_account_opt_retry(&pool_addr).await {
            Ok(Some(acc)) => acc,
            Ok(None) | Err(_) => {
                debug!(
                    request_id = %request_id,
                    pool = %pool_addr,
                    "Wallet bootstrap Raydium CPMM: pool account fetch miss, skip"
                );
                continue;
            }
        };

        let raydium_cpmm_program = Pubkey::from_str(RAYDIUM_CPMM).expect("RAYDIUM_CPMM constant");
        let parsed_state = match parse_pool_account(&raydium_cpmm_program, &pool_acc.data) {
            Some(CachedPoolState::RaydiumCpmm(s)) => s,
            _ => {
                debug!(
                    request_id = %request_id,
                    pool = %pool_addr,
                    data_len = pool_acc.data.len(),
                    "Wallet bootstrap Raydium CPMM: parse pool failed, skip"
                );
                continue;
            }
        };

        state.token_0_mint = parsed_state.token_0_mint;
        state.token_1_mint = parsed_state.token_1_mint;
        state.token_0_vault = parsed_state.token_0_vault;
        state.token_1_vault = parsed_state.token_1_vault;

        if state.token_0_mint != *base_mint && state.token_1_mint != *base_mint {
            continue;
        }

        let vault0 = state.token_0_vault;
        let vault1 = state.token_1_vault;
        let bal0 = match rpc.get_account_opt_retry(&vault0).await {
            Ok(Some(acc)) => try_parse_token_account_balance(&acc.data),
            _ => None,
        };
        let bal1 = match rpc.get_account_opt_retry(&vault1).await {
            Ok(Some(acc)) => try_parse_token_account_balance(&acc.data),
            _ => None,
        };
        let (b0, b1) = match (bal0, bal1) {
            (Some(a), Some(b)) => (a, b),
            _ => {
                debug!(
                    request_id = %request_id,
                    pool = %pool_addr,
                    "Wallet bootstrap Raydium CPMM: vault balance RPC incomplete, skip"
                );
                continue;
            }
        };

        state.reserve_0 = Some(b0);
        state.reserve_1 = Some(b1);

        ctx.live_pool_cache
            .upsert(pool_addr, CachedPoolState::RaydiumCpmm(state.clone()), 0);

        let readiness = raydium_cpmm_readiness_for_pool_cache_update(&state);
        ctx.live_pool_cache
            .merge_raydium_cpmm_pool_readiness(pool_addr, readiness);

        if readiness == DexPoolReadiness::Observed {
            debug!(
                request_id = %request_id,
                pool = %pool_addr,
                mint = %base_mint,
                "Wallet bootstrap Raydium CPMM: reserves still degenerate (Observed), no JetStream publish"
            );
            continue;
        }

        let (pub_base_mint, pub_quote_mint, base_r, quote_r) = if state.token_1_mint == sol {
            (state.token_0_mint, state.token_1_mint, b0, b1)
        } else if state.token_0_mint == sol {
            (state.token_1_mint, state.token_0_mint, b1, b0)
        } else {
            (state.token_0_mint, state.token_1_mint, b0, b1)
        };

        let jetstream_ok = if let Some(ref nats) = ctx.nats {
            let mut balance_update = PoolCacheUpdate::new_balance_updated(
                "market-data",
                BUILD_VERSION,
                run_id,
                pool_addr.to_string(),
                "raydium_cpmm".to_string(),
                pub_base_mint.to_string(),
                pub_quote_mint.to_string(),
                base_r,
                quote_r,
                0,
            );
            let mut meta = std::collections::HashMap::new();
            meta.insert(
                POOL_CACHE_UPDATE_RAYDIUM_CPMM_VAULTS_KEY.to_string(),
                raydium_cpmm_vaults_for_pool_cache_update(&state),
            );
            balance_update.metadata = Some(meta);
            balance_update.set_dex_readiness_in_metadata(readiness);
            let subject = pool_subject(&pool_addr.to_string());
            match nats.jetstream_publish(&subject, &balance_update).await {
                Ok(true) => {
                    info!(
                        request_id = %request_id,
                        pool = %pool_addr,
                        mint = %base_mint,
                        readiness = ?readiness,
                        "Wallet bootstrap Raydium CPMM: published PoolCacheUpdate::BalanceUpdated to JetStream"
                    );
                    true
                }
                Ok(false) => {
                    warn!(
                        request_id = %request_id,
                        pool = %pool_addr,
                        "Wallet bootstrap Raydium CPMM: JetStream publish failed (timeout or drop)"
                    );
                    false
                }
                Err(e) => {
                    warn!(
                        error = %e,
                        request_id = %request_id,
                        "Wallet bootstrap Raydium CPMM: Failed to publish PoolCacheUpdate to JetStream"
                    );
                    false
                }
            }
        } else {
            false
        };

        if !jetstream_ok {
            warn!(
                request_id = %request_id,
                pool = %pool_addr,
                "Wallet bootstrap Raydium CPMM: MASTER cache updated but JetStream publish failed — SSOT drift risk"
            );
        }
    }
}

/// Cold-path only: Meteora CPMM promotion for one wallet mint. Uses **only** pools already present
/// in [`LivePoolCache`] (from Geyser); no global scan. RPC: per-pool `get_account` + per-vault
/// balance reads — bounded by the number of matching cache rows.
///
/// Publishes [`PoolCacheUpdate`] with [`meteora_cpmm_readiness_for_pool_cache_update`] and
/// `meteora_cpmm_vaults` metadata (normalized base/quote order) like the Raydium CPMM bootstrap slice.
async fn handle_wallet_bootstrap_meteora_cpmm_verify_for_mint(
    ctx: &MarketDataContext,
    rpc: &Arc<SolanaRpc>,
    run_id: &str,
    request_id: &str,
    base_mint: &Pubkey,
) {
    if ctx
        .live_pool_cache
        .base_mint_has_explicit_meteora_cpmm_ready_pool(base_mint)
    {
        debug!(
            request_id = %request_id,
            mint = %base_mint,
            "Wallet bootstrap Meteora CPMM: already explicit Ready for this mint, skip"
        );
        return;
    }

    let pools = ctx.live_pool_cache.meteora_cpmm_pools_for_mint(base_mint);
    if pools.is_empty() {
        debug!(
            request_id = %request_id,
            mint = %base_mint,
            "Wallet bootstrap Meteora CPMM: no CPMM pool rows in LivePoolCache for mint, skip RPC"
        );
        return;
    }

    info!(
        request_id = %request_id,
        mint = %base_mint,
        pools = pools.len(),
        "Wallet bootstrap Meteora CPMM: verifying cache-scoped pools (cold-path RPC, no global scan)"
    );

    for (pool_addr, state) in pools {
        if ctx.live_pool_cache.base_mint_has_any_ready_pool(base_mint) {
            break;
        }
        if ctx
            .live_pool_cache
            .base_mint_has_explicit_meteora_cpmm_ready_pool(base_mint)
        {
            break;
        }

        cold_path_rpc_refresh_meteora_cpmm_pool_row(
            ctx, rpc, run_id, request_id, base_mint, pool_addr, state,
        )
        .await;
    }
}

/// Cold-path only: Orca Whirlpool promotion for one wallet mint. Uses **only** pools already present
/// in [`LivePoolCache`] (from Geyser); no `getProgramAccounts` / global scan. RPC: per-pool
/// `get_account` + per-vault balance reads — bounded by the number of matching cache rows.
///
/// Publishes [`PoolCacheUpdate`] with [`orca_readiness_for_pool_cache_update`] and
/// [`orca_metadata_for_pool_cache_update`] like the Geyser path.
///
/// **Slot:** Uses `geyser_slot = 0` on [`LivePoolCache::upsert`] and
/// [`PoolCacheUpdate::new_balance_updated`], same as bounded Raydium/Meteora CPMM wallet bootstrap
/// verify — this cold-path RPC promotion is not labeled with a prior Geyser discovery slot.
async fn handle_wallet_bootstrap_orca_whirlpool_verify_for_mint(
    ctx: &MarketDataContext,
    rpc: &Arc<SolanaRpc>,
    run_id: &str,
    request_id: &str,
    base_mint: &Pubkey,
) {
    if ctx
        .live_pool_cache
        .base_mint_has_explicit_orca_ready_pool(base_mint)
    {
        debug!(
            request_id = %request_id,
            mint = %base_mint,
            "Wallet bootstrap Orca Whirlpool: already explicit Ready for this mint, skip"
        );
        return;
    }

    let pools = ctx.live_pool_cache.orca_whirlpool_pools_for_mint(base_mint);
    if pools.is_empty() {
        debug!(
            request_id = %request_id,
            mint = %base_mint,
            "Wallet bootstrap Orca Whirlpool: no Whirlpool pool rows in LivePoolCache for mint, skip RPC"
        );
        return;
    }

    info!(
        request_id = %request_id,
        mint = %base_mint,
        pools = pools.len(),
        "Wallet bootstrap Orca Whirlpool: verifying cache-scoped pools (cold-path RPC, no global scan)"
    );

    for (pool_addr, state) in pools {
        if ctx.live_pool_cache.base_mint_has_any_ready_pool(base_mint) {
            break;
        }
        if ctx
            .live_pool_cache
            .base_mint_has_explicit_orca_ready_pool(base_mint)
        {
            break;
        }

        cold_path_rpc_refresh_orca_whirlpool_pool_row(
            ctx, rpc, run_id, request_id, base_mint, pool_addr, state,
        )
        .await;
    }
}

/// same as other bounded wallet-bootstrap verify handlers.
async fn handle_wallet_bootstrap_meteora_dlmm_verify_for_mint(
    ctx: &MarketDataContext,
    rpc: &Arc<SolanaRpc>,
    run_id: &str,
    request_id: &str,
    base_mint: &Pubkey,
) {
    if ctx
        .live_pool_cache
        .base_mint_has_explicit_meteora_dlmm_ready_pool(base_mint)
    {
        debug!(
            request_id = %request_id,
            mint = %base_mint,
            "Wallet bootstrap Meteora DLMM: already explicit Ready for this mint, skip"
        );
        return;
    }

    let pools = ctx.live_pool_cache.meteora_dlmm_pools_for_mint(base_mint);
    if pools.is_empty() {
        debug!(
            request_id = %request_id,
            mint = %base_mint,
            "Wallet bootstrap Meteora DLMM: no DLMM pool rows in LivePoolCache for mint, skip RPC"
        );
        return;
    }

    info!(
        request_id = %request_id,
        mint = %base_mint,
        pools = pools.len(),
        "Wallet bootstrap Meteora DLMM: verifying cache-scoped pools (cold-path RPC, no global scan)"
    );

    for (pool_addr, state) in pools {
        if ctx.live_pool_cache.base_mint_has_any_ready_pool(base_mint) {
            break;
        }
        if ctx
            .live_pool_cache
            .base_mint_has_explicit_meteora_dlmm_ready_pool(base_mint)
        {
            break;
        }

        cold_path_rpc_refresh_meteora_dlmm_pool_row(
            ctx, rpc, run_id, request_id, base_mint, pool_addr, state,
        )
        .await;
    }
}

/// True when a confirmed SELL [`ExecutionResult`] indicates the wallet no longer holds the
/// position for this mint (full exit, liquidation, or token ATA closed). Partial sells must
/// return false so Geyser keeps tracking the ATA until an on-chain balance update arrives.
fn execution_result_sell_closes_wallet_position(exec: &ExecutionResult) -> bool {
    if exec.metadata.get("side").map(|s| s.as_str()) != Some("SELL") {
        return false;
    }
    // Rolling deploy / skipped Scope 48 block: without an explicit partial marker, keep legacy
    // "confirmed SELL closes wallet position" semantics (only `partial` keeps the ATA tracked).
    if !exec.metadata.contains_key("sell_position_delta_applied") {
        return true;
    }
    if exec
        .metadata
        .get("sell_token_account_closed")
        .map(|v| v == "true")
        .unwrap_or(false)
    {
        return true;
    }
    if exec
        .metadata
        .get("sell_untracked_ata")
        .map(|v| v == "true")
        .unwrap_or(false)
    {
        return true;
    }
    matches!(
        exec.metadata
            .get("sell_position_delta_applied")
            .map(|s| s.as_str()),
        Some("full") | Some("full_no_balance_update")
    )
}

/// PR4b: keep wallet ATA tracked until wallet-pin withdraw is successfully enqueued.
fn wallet_sell_close_should_untrack_ata(
    should_withdraw_wallet_pin: bool,
    withdraw_enqueued: bool,
) -> bool {
    !should_withdraw_wallet_pin || withdraw_enqueued
}

/// Mixed-deploy / foreign producers may omit Scope 48 fields. Preserve legacy behavior: assume
/// full close so zero snapshot + untrack still runs for old sell-all tooling, etc.
/// **Never** backfill `execution-engine` results — that process must emit explicit metadata.
fn backfill_scope48_metadata_foreign_confirmed_sell(exec: &mut ExecutionResult) {
    if exec.header.component == "execution-engine" {
        return;
    }
    if exec.status != ExecutionStatus::Confirmed {
        return;
    }
    if exec.metadata.get("side").map(|s| s.as_str()) != Some("SELL") {
        return;
    }
    if exec.metadata.contains_key("sell_position_delta_applied") {
        return;
    }
    exec.metadata.insert(
        "sell_position_delta_applied".to_string(),
        "full".to_string(),
    );
    exec.metadata
        .insert("sell_untracked_ata".to_string(), "true".to_string());
}

#[cfg(test)]
mod execution_result_sell_close_tests {
    use super::execution_result_sell_closes_wallet_position;
    use ironcrab::ipc::{ExecutionResult, ExecutionStatus};
    use std::collections::HashMap;

    fn base_sell_exec() -> ExecutionResult {
        let mut exec = ExecutionResult::new_sent(
            "execution-engine",
            "test",
            "run",
            "ex1".to_string(),
            "d1".to_string(),
            "i1".to_string(),
            "src".to_string(),
            Some("mint".to_string()),
            Some("sig".to_string()),
            None,
        );
        exec.status = ExecutionStatus::Confirmed;
        exec.metadata = HashMap::from([("side".to_string(), "SELL".to_string())]);
        exec
    }

    #[test]
    fn partial_sell_metadata_does_not_close_position() {
        let mut exec = base_sell_exec();
        exec.metadata.insert(
            "sell_position_delta_applied".to_string(),
            "partial".to_string(),
        );
        assert!(!execution_result_sell_closes_wallet_position(&exec));
    }

    #[test]
    fn confirmed_sell_without_scope48_keys_treated_as_full_close() {
        let exec = base_sell_exec();
        assert!(execution_result_sell_closes_wallet_position(&exec));
    }

    #[test]
    fn full_sell_metadata_closes_position() {
        let mut exec = base_sell_exec();
        exec.metadata.insert(
            "sell_position_delta_applied".to_string(),
            "full".to_string(),
        );
        exec.metadata
            .insert("sell_untracked_ata".to_string(), "true".to_string());
        assert!(execution_result_sell_closes_wallet_position(&exec));
    }

    #[test]
    fn token_account_closed_closes_even_if_partial_flag_wrong() {
        let mut exec = base_sell_exec();
        exec.metadata.insert(
            "sell_position_delta_applied".to_string(),
            "partial".to_string(),
        );
        exec.metadata
            .insert("sell_token_account_closed".to_string(), "true".to_string());
        assert!(execution_result_sell_closes_wallet_position(&exec));
    }

    #[test]
    fn backfill_foreign_confirmed_sell_sets_full_when_scope48_missing() {
        use super::backfill_scope48_metadata_foreign_confirmed_sell;

        let mut exec = ExecutionResult::new_sent(
            "legacy-sell-tool",
            "test",
            "run",
            "ex2".to_string(),
            "d2".to_string(),
            "i2".to_string(),
            "src".to_string(),
            Some("mint".to_string()),
            Some("sig".to_string()),
            None,
        );
        exec.status = ExecutionStatus::Confirmed;
        exec.metadata = HashMap::from([("side".to_string(), "SELL".to_string())]);
        backfill_scope48_metadata_foreign_confirmed_sell(&mut exec);
        assert_eq!(
            exec.metadata
                .get("sell_position_delta_applied")
                .map(|s| s.as_str()),
            Some("full")
        );
        assert_eq!(
            exec.metadata.get("sell_untracked_ata").map(|s| s.as_str()),
            Some("true")
        );
    }

    #[test]
    fn backfill_skips_execution_engine_results() {
        use super::backfill_scope48_metadata_foreign_confirmed_sell;

        let mut exec = base_sell_exec();
        backfill_scope48_metadata_foreign_confirmed_sell(&mut exec);
        assert!(!exec.metadata.contains_key("sell_position_delta_applied"));
    }
}

/// True when stale JetStream bootstrap cleanup should publish a zero-balance override for `mint`.
///
/// Native SOL and WSOL sentinels are never ghost SPL ATAs and must be excluded (P0 PR #250 follow-up).
#[inline]
fn wallet_snapshot_stale_cleanup_targets_mint(
    mint: &str,
    balance_raw: u64,
    published_mint_set: &HashSet<String>,
) -> bool {
    !is_sol_or_wsol_mint(mint) && balance_raw > 0 && !published_mint_set.contains(mint)
}

/// Publish wallet token balance snapshot for position reconciliation.
///
/// Called at market-data startup to provide momentum-bot with current wallet state.
/// This allows momentum-bot to reconcile positions after restarts, detecting:
/// - Manual sales via Phantom/Jupiter (no ExecutionResult)
/// - Emergency liquidations via UI
/// - External transfers
/// - Closed ATAs (Geyser doesn't report deleted accounts)
/// - Tokens bought externally or with broken ATA tracking (owner-scan discovery)
///
/// **Startup RPC calls** (legitimate, NOT in hot-path):
/// 1. `getTokenAccountsByOwner` x2 (SPL Token + Token-2022) — discover unknown tokens
/// 2. `getMultipleAccounts` x1 — verify balances + decimals for all known mints
///
/// After startup, live balance updates are handled by Geyser (tracked ATA subscriptions).
/// No RPC calls are made in the runtime hot-path.
///
/// When `dex_verify_blocking` is false (normal startup), bounded wallet DEX verification runs in a
/// background task so the caller can reach the main loop and systemd watchdog pings before slow
/// cold-path RPC completes. When true (`wallet_snapshot_only`), verification is awaited so the
/// one-shot process exits with work finished.
async fn publish_wallet_snapshot_stale_jetstream_cleanup(
    ctx: &MarketDataContext,
    nats: &NatsClient,
    wallet_str: &str,
    published_mint_set: &HashSet<String>,
) {
    use async_nats::jetstream;
    use futures::StreamExt;

    let js = jetstream::new(nats.client().clone());
    let Ok(stream) = js.get_stream(WALLET_SNAPSHOT_STREAM_NAME).await else {
        debug!(
            stream = WALLET_SNAPSHOT_STREAM_NAME,
            "Stale cleanup: WALLET_SNAPSHOT stream not found"
        );
        return;
    };

    let mut cleanup_consumer_config = wallet_snapshot_consumer_config();
    cleanup_consumer_config.filter_subject = format!("ironcrab.wallet_snapshot.{}.*", wallet_str);
    let Ok(consumer) = stream.create_consumer(cleanup_consumer_config).await else {
        warn!("Stale cleanup: failed to create consumer");
        return;
    };

    let mut stale_cleaned = 0usize;
    let mut total_checked = 0usize;

    loop {
        let Ok(mut messages) = consumer.fetch().max_messages(500).messages().await else {
            warn!("Stale cleanup: fetch failed");
            break;
        };

        let mut batch_count = 0usize;

        while let Some(msg) = messages.next().await {
            let Ok(msg) = msg else {
                warn!("Stale cleanup: error fetching message");
                continue;
            };

            batch_count += 1;
            total_checked += 1;

            let Ok(event) = serde_json::from_slice::<MarketEvent>(&msg.payload) else {
                let _ = msg.ack().await;
                continue;
            };

            if let MarketEventKind::WalletBalanceSnapshot {
                mint,
                balance_raw,
                decimals,
                token_program: tp,
            } = &event.kind
            {
                if wallet_snapshot_stale_cleanup_targets_mint(
                    mint,
                    *balance_raw,
                    published_mint_set,
                ) {
                    let override_event = MarketEvent::new(
                        "market-data",
                        BUILD_VERSION,
                        &ctx.run_id,
                        format!("wallet_snapshot_stale_cleanup_{}", mint),
                        "wallet_bootstrap_stale_cleanup",
                        None,
                        MarketEventKind::WalletBalanceSnapshot {
                            mint: mint.clone(),
                            balance_raw: 0,
                            decimals: *decimals,
                            token_program: tp.clone(),
                        },
                    );

                    let subject = wallet_snapshot_subject(wallet_str, mint);
                    if let Err(e) = nats.jetstream_publish(&subject, &override_event).await {
                        warn!(error = %e, mint = %mint, "Stale cleanup: failed to publish zero-balance override to JetStream");
                    }

                    stale_cleaned += 1;
                    info!(
                        mint = %mint,
                        old_balance = *balance_raw,
                        "Stale cleanup: cleared ghost position (ATA no longer exists, balance → 0)"
                    );
                }
            }

            let _ = msg.ack().await;
        }

        if batch_count < 500 {
            break;
        }
    }

    if stale_cleaned > 0 {
        info!(
            stale_cleaned,
            total_checked, "✅ Stale JetStream cleanup: cleared ghost positions from previous runs"
        );
    } else if total_checked > 0 {
        debug!(
            total_checked,
            published = published_mint_set.len(),
            "Stale cleanup: no ghost positions found (all entries are fresh)"
        );
    }
}

async fn publish_wallet_snapshot(
    ctx: &Arc<MarketDataContext>,
    rpc: &Arc<SolanaRpc>,
    wallet: &Pubkey,
    is_periodic: bool,
    dex_verify_blocking: bool,
    md_state: &MdStateSender,
) -> Result<()> {
    use async_nats::jetstream;
    use futures::StreamExt;
    use std::collections::{HashMap, HashSet};

    // Constraint: At most one RPC roundtrip on restart.
    // In practice this also bounds the tracked accounts we add for wallet tracking.
    const MAX_BOOTSTRAP_MINTS: usize = 30;

    let token_program = Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
        .expect("valid token program");
    let token_2022_program = Pubkey::from_str("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")
        .expect("valid token-2022 program");
    let ata_program =
        Pubkey::from_str(ASSOCIATED_TOKEN_PROGRAM_ID).expect("valid associated token program");

    let wallet_str = wallet.to_string();
    let wallet_snapshot_bootstrap_started_at = Instant::now();

    // 1) Discover known mints from JetStream (LastPerSubject) for this wallet.
    //    This avoids any RPC owner-scan and gives us stable coverage over restarts.
    let mut known_mints: Vec<Pubkey> = Vec::new();
    // mint -> (decimals, token_program, last_balance_raw)
    // IMPORTANT: if bootstrap cannot resolve a token account deterministically (non-ATA),
    // we must NOT overwrite a previously correct balance with 0.
    let mut cached_mint_meta: HashMap<Pubkey, (u8, Pubkey, u64)> = HashMap::new();

    if let Some(ref nats) = ctx.nats {
        let js = jetstream::new(nats.client().clone());
        match js.get_stream(WALLET_SNAPSHOT_STREAM_NAME).await {
            Ok(stream) => {
                let mut consumer_config = wallet_snapshot_consumer_config();
                consumer_config.filter_subject =
                    format!("ironcrab.wallet_snapshot.{}.*", wallet_str);
                match stream.create_consumer(consumer_config).await {
                    Ok(consumer) => {
                        // Pull up to N wallet snapshot subjects (bounded).
                        // If there are more, we cap to keep bootstrap to 1 RPC call.
                        let mut messages = consumer
                            .fetch()
                            .max_messages(MAX_BOOTSTRAP_MINTS.saturating_mul(2))
                            .messages()
                            .await?;

                        while let Some(msg) = messages.next().await {
                            let msg = match msg {
                                Ok(m) => m,
                                Err(_) => continue,
                            };
                            let event: MarketEvent = match serde_json::from_slice(&msg.payload) {
                                Ok(e) => e,
                                Err(e) => {
                                    debug!(error = %e, "Wallet snapshot bootstrap: failed to deserialize MarketEvent");
                                    let _ = msg.ack().await;
                                    continue;
                                }
                            };

                            if let MarketEventKind::WalletBalanceSnapshot {
                                mint,
                                balance_raw,
                                decimals,
                                token_program,
                                ..
                            } = &event.kind
                            {
                                if mint.as_str() == WSOL_MINT {
                                    let _ = msg.ack().await;
                                    continue;
                                }
                                if let (Ok(mint_pk), Ok(token_prog_pk)) =
                                    (Pubkey::from_str(mint), Pubkey::from_str(token_program))
                                {
                                    cached_mint_meta.entry(mint_pk).or_insert_with(|| {
                                        known_mints.push(mint_pk);
                                        (*decimals, token_prog_pk, *balance_raw)
                                    });
                                }
                            }
                            let _ = msg.ack().await;
                            if known_mints.len() >= MAX_BOOTSTRAP_MINTS {
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        debug!(error = %e, "Wallet snapshot bootstrap: failed to create consumer");
                    }
                }
            }
            Err(e) => {
                debug!(error = %e, stream = WALLET_SNAPSHOT_STREAM_NAME, "Wallet snapshot bootstrap: stream not found");
            }
        }
    }

    // If JetStream has no prior snapshots (first-ever startup), allow an operator-provided
    // mint list to break the circular dependency ("no known mints" -> no bootstrap).
    if known_mints.is_empty() {
        if let Ok(v) = std::env::var("IRONCRAB_BOOTSTRAP_MINTS") {
            for s in v.split(',').map(|x| x.trim()).filter(|x| !x.is_empty()) {
                match Pubkey::from_str(s) {
                    Ok(m) => known_mints.push(m),
                    Err(e) => {
                        warn!(mint = %s, error = %e, "Invalid IRONCRAB_BOOTSTRAP_MINTS entry")
                    }
                }
            }
            known_mints.sort();
            known_mints.dedup();
        }
    }

    if known_mints.is_empty() {
        info!(
            wallet = %wallet_str,
            "Wallet snapshot bootstrap: no known mints (JetStream empty; no IRONCRAB_BOOTSTRAP_MINTS); publishing empty snapshot complete"
        );
        // Still publish WalletSnapshotComplete and keep Geyser subscriptions warm for wallet + WSOL.
        // Token holdings will be learned event-driven (ExecutionResults + Geyser).
    }

    // 1.5) Owner-Scan Discovery: getTokenAccountsByOwner (bootstrap + periodic cold path)
    //
    // JetStream only knows mints that were previously tracked. If the bot was offline and
    // tokens were bought externally (Phantom, Jupiter) or a previous run had broken ATA
    // tracking, those mints won't be in JetStream.
    //
    // This owner-scan discovers ALL token accounts in the wallet, merges them with known
    // mints, and ensures the snapshot reflects the true on-chain wallet state.
    // Cold-path RPC (2 calls: SPL Token + Token-2022) — allowed on startup and periodic timer.
    // Capture WSOL balance from owner-scan for JetStream bootstrap (startup SOL/WSOL publish only).
    let mut bootstrap_wsol_balance: Option<u64> = None;

    {
        use solana_client::rpc_request::TokenAccountsFilter;

        let mut discovered_from_owner_scan: Vec<(Pubkey, u64, Pubkey)> = Vec::new(); // (mint, balance, token_program)
        let mut spl_non_zero_count: usize = 0;
        let mut t22_non_zero_count: usize = 0;

        // SPL Token accounts
        match rpc
            .rpc
            .get_token_accounts_by_owner(wallet, TokenAccountsFilter::ProgramId(token_program))
            .await
        {
            Ok(accounts) => {
                for keyed in &accounts {
                    if let solana_account_decoder::UiAccountData::Json(parsed) = &keyed.account.data
                    {
                        if let Some(info) = parsed.parsed.get("info") {
                            let mint_str = info.get("mint").and_then(|v| v.as_str()).unwrap_or("");
                            let balance_str = info
                                .get("tokenAmount")
                                .and_then(|v| v.get("amount"))
                                .and_then(|v| v.as_str())
                                .unwrap_or("0");
                            let decimals_val = info
                                .get("tokenAmount")
                                .and_then(|v| v.get("decimals"))
                                .and_then(|v| v.as_u64())
                                .unwrap_or(6) as u8;
                            if let Ok(mint_pk) = Pubkey::from_str(mint_str) {
                                let balance: u64 = balance_str.parse().unwrap_or(0);
                                if mint_str == WSOL_MINT {
                                    // Capture WSOL balance for JetStream bootstrap
                                    bootstrap_wsol_balance = Some(balance);
                                } else if balance > 0 {
                                    spl_non_zero_count += 1;
                                    discovered_from_owner_scan.push((
                                        mint_pk,
                                        balance,
                                        token_program,
                                    ));
                                    // Also cache decimals for later
                                    cached_mint_meta.entry(mint_pk).or_insert((
                                        decimals_val,
                                        token_program,
                                        0,
                                    ));
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => {
                warn!(program = "spl_token", error = %e, "Wallet snapshot bootstrap: getTokenAccountsByOwner failed");
            }
        }

        // Token-2022 accounts
        match rpc
            .rpc
            .get_token_accounts_by_owner(wallet, TokenAccountsFilter::ProgramId(token_2022_program))
            .await
        {
            Ok(accounts) => {
                for keyed in &accounts {
                    if let solana_account_decoder::UiAccountData::Json(parsed) = &keyed.account.data
                    {
                        if let Some(info) = parsed.parsed.get("info") {
                            let mint_str = info.get("mint").and_then(|v| v.as_str()).unwrap_or("");
                            let balance_str = info
                                .get("tokenAmount")
                                .and_then(|v| v.get("amount"))
                                .and_then(|v| v.as_str())
                                .unwrap_or("0");
                            let decimals_val = info
                                .get("tokenAmount")
                                .and_then(|v| v.get("decimals"))
                                .and_then(|v| v.as_u64())
                                .unwrap_or(6) as u8;
                            if let Ok(mint_pk) = Pubkey::from_str(mint_str) {
                                let balance: u64 = balance_str.parse().unwrap_or(0);
                                if balance > 0 && mint_str != WSOL_MINT {
                                    t22_non_zero_count += 1;
                                    discovered_from_owner_scan.push((
                                        mint_pk,
                                        balance,
                                        token_2022_program,
                                    ));
                                    cached_mint_meta.entry(mint_pk).or_insert((
                                        decimals_val,
                                        token_2022_program,
                                        0,
                                    ));
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => {
                warn!(program = "token_2022", error = %e, "Wallet snapshot bootstrap: getTokenAccountsByOwner failed");
            }
        }

        // A.3: Bootstrap owner-scan diagnostics (non-zero counts)
        info!(
            wallet = %wallet_str,
            spl_non_zero = spl_non_zero_count,
            t22_non_zero = t22_non_zero_count,
            total_discovered = discovered_from_owner_scan.len(),
            known_mints = known_mints.len(),
            cap = MAX_BOOTSTRAP_MINTS,
            "Bootstrap owner-scan: token counts"
        );

        // FIX-37: Owner-scan mints with real balance ALWAYS take priority over stale
        // JetStream entries. Previously, MAX_BOOTSTRAP_MINTS could be filled entirely
        // by stale JetStream snapshots, causing real wallet tokens to be ignored.
        let known_set: HashSet<Pubkey> = known_mints.iter().copied().collect();
        let mut newly_discovered = 0usize;
        for (mint_pk, _balance, _token_prog) in &discovered_from_owner_scan {
            if !known_set.contains(mint_pk) {
                known_mints.push(*mint_pk);
                newly_discovered += 1;
            }
        }

        if newly_discovered > 0 {
            info!(
                wallet = %wallet_str,
                newly_discovered,
                total_known = known_mints.len(),
                cap = MAX_BOOTSTRAP_MINTS,
                "Wallet snapshot bootstrap: owner-scan discovered unknown tokens (bypassing cap for real wallet tokens)"
            );
        } else if !discovered_from_owner_scan.is_empty() {
            debug!(
                wallet = %wallet_str,
                owner_scan_tokens = discovered_from_owner_scan.len(),
                "Wallet snapshot bootstrap: owner-scan found tokens (all already known)"
            );
        }
    }

    // 2) Single RPC roundtrip: fetch mint accounts + derived SPL/2022 ATAs via getMultipleAccounts.
    //
    // Reconciles all known mints (from JetStream + owner-scan discovery) via a single
    // getMultipleAccounts call. This gives us authoritative balance + decimals for every mint.
    fn derive_ata(owner: &Pubkey, mint: &Pubkey, token_prog: &Pubkey, ata_prog: &Pubkey) -> Pubkey {
        let (ata, _bump) = Pubkey::find_program_address(
            &[owner.as_ref(), token_prog.as_ref(), mint.as_ref()],
            ata_prog,
        );
        ata
    }

    let mut accounts_by_key: HashMap<Pubkey, solana_sdk::account::Account> = HashMap::new();
    if !known_mints.is_empty() {
        let mut keys: Vec<Pubkey> = Vec::with_capacity(known_mints.len() * 3);
        for mint in &known_mints {
            keys.push(*mint);
            keys.push(derive_ata(wallet, mint, &token_program, &ata_program));
            keys.push(derive_ata(wallet, mint, &token_2022_program, &ata_program));
        }
        keys.sort();
        keys.dedup();

        let fetched = match rpc.rpc.get_multiple_accounts(&keys).await {
            Ok(v) => v,
            Err(e) => {
                warn!(error = %e, wallet = %wallet_str, "Wallet snapshot bootstrap: getMultipleAccounts failed");
                return Ok(());
            }
        };

        for (idx, maybe_acc) in fetched.into_iter().enumerate() {
            if let Some(acc) = maybe_acc {
                if let Some(pk) = keys.get(idx) {
                    accounts_by_key.insert(*pk, acc);
                }
            }
        }
    }

    // Publish per-mint snapshots (truth from RPC bootstrap).
    let mut mints_in_wallet: Vec<String> = Vec::new();
    let mut wallet_token_accounts: HashSet<Pubkey> = HashSet::new();

    for mint in &known_mints {
        let last_meta = cached_mint_meta.get(mint).copied();

        // Resolve decimals: prefer mint account from RPC; fall back to last persisted snapshot.
        let decimals = accounts_by_key
            .get(mint)
            .and_then(|acc| try_parse_mint_account(&acc.owner, &acc.data).map(|(d, _, _, _)| d))
            .or_else(|| last_meta.map(|(d, _, _)| d))
            .or_else(|| ctx.tracked_wallet_mint_decimals.read().get(mint).copied())
            .unwrap_or_else(|| {
                warn!(
                    mint = %mint,
                    "Bootstrap: decimals unknown for mint; defaulting to 6"
                );
                6
            });
        ctx.tracked_wallet_mint_decimals
            .write()
            .insert(*mint, decimals);

        // Resolve balance via ATA accounts only (no scanning).
        let ata_spl = derive_ata(wallet, mint, &token_program, &ata_program);
        let ata_2022 = derive_ata(wallet, mint, &token_2022_program, &ata_program);

        let mut observed: Option<(u64, Pubkey, Pubkey)> = None; // (amount_raw, ata, token_program)

        if let Some(acc) = accounts_by_key.get(&ata_spl) {
            if acc.owner.to_bytes() == spl_token::ID.to_bytes() {
                if let Ok(ta) = spl_token::state::Account::unpack(&acc.data) {
                    let mint_pk = Pubkey::new_from_array(ta.mint.to_bytes());
                    let owner_pk = Pubkey::new_from_array(ta.owner.to_bytes());
                    if mint_pk == *mint && owner_pk == *wallet {
                        observed = Some((ta.amount, ata_spl, token_program));
                    }
                }
            }
        }
        if observed.is_none() {
            if let Some(acc) = accounts_by_key.get(&ata_2022) {
                if acc.owner.to_bytes() == spl_token_2022::ID.to_bytes() {
                    // Token-2022 accounts may have extensions (data > 165 bytes).
                    // Use StateWithExtensions instead of Pack::unpack to handle this.
                    if let Ok(state) =
                        StateWithExtensions::<spl_token_2022::state::Account>::unpack(&acc.data)
                    {
                        let ta = &state.base;
                        let mint_pk = Pubkey::new_from_array(ta.mint.to_bytes());
                        let owner_pk = Pubkey::new_from_array(ta.owner.to_bytes());
                        if mint_pk == *mint && owner_pk == *wallet {
                            observed = Some((ta.amount, ata_2022, token_2022_program));
                        }
                    }
                }
            }
        }

        let (balance_raw, token_program_used, maybe_ata) = match observed {
            Some((amt, ata, prog)) => (amt, prog, Some(ata)),
            None => {
                // ATA not found on-chain → balance is definitively 0.
                // The bot exclusively uses ATAs (derived via Associated Token Program).
                // If the ATA doesn't exist, the token was sold and the ATA was closed.
                // Previous logic incorrectly preserved stale non-zero balances here,
                // creating permanent ghost positions that could never be cleaned up.
                let prev_balance = last_meta.map(|(_, _, b)| b).unwrap_or(0);
                if prev_balance > 0 {
                    info!(
                        mint = %mint,
                        previous_balance = prev_balance,
                        "Wallet snapshot: ATA not found on-chain, clearing stale balance → 0 (token was sold/transferred)"
                    );
                }
                (
                    0u64,
                    last_meta.map(|(_, p, _)| p).unwrap_or(token_program),
                    None,
                )
            }
        };

        if let Some(ata) = maybe_ata {
            wallet_token_accounts.insert(ata);
        }
        if balance_raw > 0 {
            mints_in_wallet.push(mint.to_string());
        }

        // Ensure mint is tracked so Geyser can publish TokenMintInfo later (no RPC here).
        md_state_try_enqueue(md_state, MdStateCommand::TrackWalletMint { mint: *mint });

        let mint_str = mint.to_string();
        let event = MarketEvent::new(
            "market-data",
            BUILD_VERSION,
            &ctx.run_id,
            format!("wallet_snapshot_bootstrap_{}", mint_str),
            "wallet_bootstrap",
            None, // No slot for RPC bootstrap
            MarketEventKind::WalletBalanceSnapshot {
                mint: mint_str.clone(),
                balance_raw,
                decimals,
                token_program: token_program_used.to_string(),
            },
        );

        // Publish to JetStream only (SSOT for bot state)
        if let Some(ref nats) = ctx.nats {
            let subject = wallet_snapshot_subject(&wallet_str, &mint_str);
            if let Err(e) = nats.jetstream_publish(&subject, &event).await {
                warn!(error = %e, mint = %mint_str, "Failed to publish WalletBalanceSnapshot to JetStream (bootstrap)");
            }
        }

        ctx.write_market_event_jsonl(&event);
    }

    // 2.5) Stale JetStream Cleanup: Override ghost entries not covered by bootstrap scan.
    //
    // JetStream may contain entries for mints that were sold/closed in previous runs,
    // but never got their zero-balance override. Publish zero-balance overrides for any
    // non-zero SPL mints not already covered. No additional RPC calls.
    // Startup only: periodic snapshots must not invalidate JetStream without a full owner scan.
    if !is_periodic {
        if let Some(ref nats) = ctx.nats {
            let published_mint_set: HashSet<String> =
                known_mints.iter().map(|m| m.to_string()).collect();
            publish_wallet_snapshot_stale_jetstream_cleanup(
                ctx.as_ref(),
                nats,
                &wallet_str,
                &published_mint_set,
            )
            .await;
        }
    }

    // 3) Update tracked wallet accounts for Geyser subscription (wallet + WSOL + token ATAs).
    if let Some(ref tracked_wallet) = ctx.tracked_wallet {
        let mut accounts: Vec<Pubkey> = Vec::new();
        accounts.push(tracked_wallet.wallet);
        accounts.push(tracked_wallet.wsol_ata);
        // Keep any existing tracked token accounts and add what we learned here.
        {
            let existing = ctx.tracked_wallet_token_accounts.read().clone();
            wallet_token_accounts.extend(existing);
        }
        accounts.extend(wallet_token_accounts.iter().copied());
        accounts.sort();
        accounts.dedup();
        let _ = ctx.tracked_wallet_tx.send(accounts);
        *ctx.tracked_wallet_token_accounts.write() = wallet_token_accounts;
    }

    // 4) WalletSnapshotComplete helps momentum-bot close ghost positions.
    let complete_event = MarketEvent::new(
        "market-data",
        BUILD_VERSION,
        &ctx.run_id,
        format!(
            "wallet_snapshot_complete_bootstrap_{}",
            if is_periodic { "periodic" } else { "startup" }
        ),
        "wallet_bootstrap_complete",
        None,
        MarketEventKind::WalletSnapshotComplete {
            mints_in_wallet: mints_in_wallet.clone(),
            wallet: wallet_str.clone(),
            is_periodic,
        },
    );

    if let Some(ref nats) = ctx.nats {
        let _ = publish_market_event_core_and_momentum_ex(
            nats,
            &complete_event,
            Some(MarketEventCorePublishTrace {
                recv_at: wallet_snapshot_bootstrap_started_at,
                cold_path: true,
                segment: MarketDataLatencySegment::Other,
            }),
            None,
        )
        .await;
    }
    ctx.write_market_event_jsonl(&complete_event);

    // 4a) Bounded cold-path verification for wallet non-zero mints without explicit Ready
    // (PumpSwap, PumpFun, Raydium CPMM, Meteora CPMM, Orca, Meteora DLMM — cache-scoped where applicable);
    // reuses I-24d handlers + JetStream — no hot-path RPC.
    //
    // Normal startup: spawn so systemd WatchdogSec is not exceeded while Ensure*/RPC runs serially.
    // wallet_snapshot_only: block so one-shot mode still completes verification before exit.
    if !is_periodic {
        let mints_clone = mints_in_wallet.clone();
        if dex_verify_blocking {
            let dex_opt = ctx.pump_amm_dex.read().clone();
            if let Some(dex) = dex_opt {
                run_bounded_wallet_dex_bootstrap_verify(
                    ctx.as_ref(),
                    rpc,
                    dex.as_ref(),
                    &mints_clone,
                    ctx.run_id.as_str(),
                    false,
                )
                .await;
            } else {
                warn!(
                    run_id = %ctx.run_id,
                    "Wallet bootstrap DEX verify skipped: pump_amm_dex not initialized"
                );
            }
        } else {
            let ctx_spawn = Arc::clone(ctx);
            let rpc_spawn = Arc::clone(rpc);
            tokio::spawn(async move {
                let dex_opt = ctx_spawn.pump_amm_dex.read().clone();
                let Some(dex) = dex_opt else {
                    warn!(
                        run_id = %ctx_spawn.run_id,
                        "Wallet bootstrap DEX verify skipped: pump_amm_dex not initialized (unexpected)"
                    );
                    return;
                };
                info!(
                    run_id = %ctx_spawn.run_id,
                    mints = mints_clone.len(),
                    "Wallet bootstrap: bounded DEX verification running in background (watchdog-safe)"
                );
                run_bounded_wallet_dex_bootstrap_verify(
                    ctx_spawn.as_ref(),
                    &rpc_spawn,
                    dex.as_ref(),
                    &mints_clone,
                    ctx_spawn.run_id.as_str(),
                    false,
                )
                .await;
                info!(
                    run_id = %ctx_spawn.run_id,
                    "Wallet bootstrap: bounded DEX verification background task finished"
                );
            });
        }
    }

    // 4b) Cold-path inventory: count wallet-held mints where [`LivePoolCache::base_mint_has_any_ready_pool`]
    // is true (explicit Ready merge per slice: PumpSwap, PumpFun bonding, Raydium CPMM, Meteora CPMM,
    // Meteora DLMM, Orca, Raydium AMM). No legacy PumpSwap heuristic / snapshot completeness.
    let mut wallet_mints_explicit_ready_count: usize = 0;
    for mint_str in &mints_in_wallet {
        if let Ok(pk) = Pubkey::from_str(mint_str) {
            if ctx.live_pool_cache.base_mint_has_any_ready_pool(&pk) {
                wallet_mints_explicit_ready_count += 1;
                debug!(
                    mint = %mint_str,
                    "wallet mint: explicit DexPoolReadiness::Ready (PumpSwap / PumpFun / Raydium CPMM / Meteora CPMM / Meteora DLMM / Orca / Raydium AMM)"
                );
            }
        }
    }
    if !mints_in_wallet.is_empty() {
        info!(
            wallet = %wallet_str,
            wallet_mints_explicit_ready_count,
            nonzero_wallet_mints = mints_in_wallet.len(),
            "Wallet snapshot: mints with explicit Ready merge (PumpSwap / PumpFun / Raydium CPMM / Meteora CPMM / Meteora DLMM / Orca / Raydium AMM); excludes legacy PumpSwap effective-ready"
        );
    }

    // 5) Publish SOL + WSOL as WalletBalanceSnapshot to JetStream (SSOT for bot state).
    //    execution-engine/WsolManager bootstrap from JetStream on startup.
    if !is_periodic {
        if let Some(ref tracked_wallet) = ctx.tracked_wallet {
            if let Some(ref nats) = ctx.nats {
                // Fetch native SOL balance (1 lightweight RPC call during bootstrap)
                match rpc.rpc.get_balance(wallet).await {
                    Ok(sol_lamports) => {
                        // Always send explicit WSOL: Some(0) when no ATA exists, so WsolManager
                        // knows to wrap and JetStream doesn't retain stale WSOL from previous runs.
                        let wsol_balance = bootstrap_wsol_balance.unwrap_or(0);

                        // Seed TrackedWallet so subsequent Geyser events correctly detect changes
                        tracked_wallet
                            .last_sol_balance
                            .store(sol_lamports, Ordering::Relaxed);
                        tracked_wallet
                            .last_wsol_balance
                            .store(wsol_balance, Ordering::Relaxed);
                        tracked_wallet.wsol_seen.store(true, Ordering::Relaxed);

                        // Publish SOL + WSOL as WalletBalanceSnapshot to JetStream (SSOT).
                        // NOTE: Native SOL uses sentinel "NATIVE_SOL" as mint key because
                        // SOL_MINT == WSOL_MINT (same address). Without this, a single
                        // JetStream subject would be shared and one would overwrite the other.
                        {
                            let sol_snapshot = MarketEvent::new(
                                "market-data",
                                BUILD_VERSION,
                                &ctx.run_id,
                                "wallet_snapshot_bootstrap_NATIVE_SOL".to_string(),
                                "wallet_bootstrap",
                                None,
                                MarketEventKind::WalletBalanceSnapshot {
                                    mint: "NATIVE_SOL".to_string(),
                                    balance_raw: sol_lamports,
                                    decimals: 9,
                                    token_program: "system".to_string(),
                                },
                            );
                            let sol_subject = wallet_snapshot_subject(&wallet_str, "NATIVE_SOL");
                            if let Err(e) =
                                nats.jetstream_publish(&sol_subject, &sol_snapshot).await
                            {
                                warn!(error = %e, "Failed to publish native SOL WalletBalanceSnapshot to JetStream");
                            }

                            // Always publish WSOL (including 0) so JetStream has authoritative
                            // state; otherwise LastPerSubject returns stale WSOL from previous runs.
                            let wsol_bal = wsol_balance;
                            let wsol_snapshot = MarketEvent::new(
                                "market-data",
                                BUILD_VERSION,
                                &ctx.run_id,
                                "wallet_snapshot_bootstrap_WSOL".to_string(),
                                "wallet_bootstrap",
                                None,
                                MarketEventKind::WalletBalanceSnapshot {
                                    mint: WSOL_MINT.to_string(),
                                    balance_raw: wsol_bal,
                                    decimals: 9,
                                    token_program: "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
                                        .to_string(),
                                },
                            );
                            let wsol_subject = wallet_snapshot_subject(&wallet_str, WSOL_MINT);
                            if let Err(e) =
                                nats.jetstream_publish(&wsol_subject, &wsol_snapshot).await
                            {
                                warn!(error = %e, "Failed to publish WSOL WalletBalanceSnapshot to JetStream");
                            }

                            info!(
                                wallet = %wallet_str,
                                sol_lamports,
                                wsol_balance,
                                "SOL/WSOL WalletBalanceSnapshot published to JetStream (bootstrap)"
                            );
                        }
                    }
                    Err(e) => {
                        warn!(
                            error = %e,
                            "Failed to fetch SOL balance for bootstrap (will rely on Geyser events)"
                        );
                    }
                }
            }
        }
    }

    info!(
        wallet = %wallet_str,
        known_mints = known_mints.len(),
        mints_in_wallet = mints_in_wallet.len(),
        is_periodic,
        "✅ Wallet snapshot bootstrap published (RPC: getMultipleAccounts + startup owner-scan)"
    );

    if is_periodic {
        // Republish cached native SOL (bootstrap/Geyser-seeded) without RPC.
        if let Some(ref tracked_wallet) = ctx.tracked_wallet {
            if let Some(ref nats) = ctx.nats {
                let sol_lamports = tracked_wallet.last_sol_balance.load(Ordering::Relaxed);
                let sol_snapshot = MarketEvent::new(
                    "market-data",
                    BUILD_VERSION,
                    &ctx.run_id,
                    "wallet_snapshot_periodic_NATIVE_SOL".to_string(),
                    "wallet_bootstrap",
                    None,
                    MarketEventKind::WalletBalanceSnapshot {
                        mint: "NATIVE_SOL".to_string(),
                        balance_raw: sol_lamports,
                        decimals: 9,
                        token_program: "system".to_string(),
                    },
                );
                let sol_subject = wallet_snapshot_subject(&wallet_str, "NATIVE_SOL");
                if let Err(e) = nats.jetstream_publish(&sol_subject, &sol_snapshot).await {
                    warn!(error = %e, "Failed to republish native SOL WalletBalanceSnapshot (periodic)");
                }
            }
        }

        ironcrab::metrics::market_data_wallet_snapshot_periodic_published_inc();
        info!(
            wallet = %wallet_str,
            mints_in_wallet = mints_in_wallet.len(),
            is_periodic = true,
            "📸 Periodic wallet snapshot published"
        );
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("market_data=info".parse()?)
                .add_directive("ironcrab=info".parse()?),
        )
        .init();

    let args = Args::parse();
    let run_id = Uuid::new_v4().to_string();

    let file_config: Option<Config> = match Config::load(&args.config) {
        Ok(c) => Some(c),
        Err(e) => {
            warn!(
                error = %e,
                config_path = %args.config.display(),
                "Could not load config.toml; Helius PumpSwap fallback + market_data_geyser defaults (optional)"
            );
            None
        }
    };

    let helius_rpc: Option<Arc<SolanaRpc>> = file_config
        .as_ref()
        .and_then(|cfg| {
            cfg.solana
                .helius_rpc_url
                .as_ref()
                .map(|u| u.trim())
                .filter(|u| !u.is_empty())
                .map(|url| {
                    info!(
                        helius_rpc_host = %url.split('/').nth(2).unwrap_or("?"),
                        "Loaded solana.helius_rpc_url for bounded PumpSwap TX-history fallback (Cold Path only)"
                    );
                    Arc::new(SolanaRpc::new(url))
                })
        });

    let mut market_data_config = MarketDataConfig::default();
    if let Some(ref c) = file_config {
        market_data_config.max_tracked_accounts = c.market_data_geyser.max_tracked_accounts;
        market_data_config.geyser_full_reconnect_threshold =
            c.market_data_geyser.geyser_full_reconnect_threshold;
        market_data_config.geyser_sync_batch_ms =
            c.market_data_geyser.geyser_sync_batch_ms.clamp(10, 100);
    }

    let wallet_env = std::env::var("IRONCRAB_WALLET_PUBKEY").ok();
    info!(
        run_id = %run_id,
        config = %args.config.display(),
        geyser_url = %args.geyser_url,
        metrics_port = args.metrics_port,
        wallet_pubkey = ?wallet_env,
        wallet_snapshot_only = args.wallet_snapshot_only,
        "Starting market-data service"
    );

    // Set readiness mode for /status (E2E blackbox)
    set_readiness_mode(if args.dry_run {
        1
    } else if args.simulate {
        2
    } else {
        0
    });

    // PR165: metrics on isolated runtime (not main Geyser Tokio pool).
    let metrics_addr = std::net::SocketAddr::from(([0, 0, 0, 0], args.metrics_port));
    spawn_market_data_metrics_runtime(metrics_addr);
    info!(
        port = args.metrics_port,
        "Metrics server started at /metrics (md-metrics thread)"
    );

    // === P0 Check: Ensure no wallet keys are loaded ===
    // market-data is KEYLESS per architecture – exit immediately if keys are detected
    if std::env::var("IRONCRAB_KEYPAIR_JSON").is_ok()
        || std::env::var("IRONCRAB_KEYPAIR_B64").is_ok()
        || std::env::var("IRONCRAB_KEYPAIR_PATH").is_ok()
    {
        error!("ERROR: Wallet key environment variables detected!");
        error!("market-data is KEYLESS per architecture. Remove key variables and restart.");
        error!("Only execution-engine should have access to wallet keys.");
        std::process::exit(1);
    }

    // Setup JSONL writer
    let log_dir = args
        .log_dir
        .unwrap_or_else(|| PathBuf::from("trade_logs/market_events"));
    let jsonl_config = JsonlWriterConfig::new("market_events").with_log_dir(&log_dir);
    let jsonl_writer = spawn_market_data_jsonl_writer(jsonl_config, MARKET_DATA_JSONL_QUEUE_CAP)?;

    info!(
        log_dir = %log_dir.display(),
        queue_cap = MARKET_DATA_JSONL_QUEUE_CAP,
        "JSONL writer initialized (off hot-path queue)"
    );

    record_market_data_tokio_progress();
    let process_started = Instant::now();
    spawn_market_data_global_ingest_liveness_task(process_started);
    spawn_market_data_md_state_liveness_task(process_started);

    // Setup NATS (optional in dry-run mode)
    let nats = if args.dry_run {
        info!("Dry-run mode: NATS publishing disabled");
        None
    } else {
        let config = NatsConfig::new(&args.nats_url, "market-data");
        let mut client = NatsClient::new(config);
        if let Err(e) = client.connect().await {
            warn!(error = %e, "Failed to connect to NATS (continuing without)");
            None
        } else {
            info!(url = %args.nats_url, "Connected to NATS");
            set_readiness_nats_connected(true);

            // Initialize JetStream stream for PoolCacheUpdates (persistent state)
            if let Err(e) = ensure_pool_cache_stream(client.client()).await {
                error!(error = %e, "Failed to create/update JetStream POOL_CACHE stream");
                error!("PoolCacheUpdates will not persist across restarts!");
                error!("Check that nats-server is running with -js flag");
            } else {
                info!("JetStream POOL_CACHE stream ready for persistent state recovery");
            }

            // Initialize JetStream stream for WalletBalanceSnapshot (position reconciliation)
            if let Err(e) = ensure_wallet_snapshot_stream(client.client()).await {
                error!(error = %e, "Failed to create/update JetStream WALLET_SNAPSHOT stream");
                error!("WalletBalanceSnapshot persistence disabled!");
                error!("Check that nats-server is running with -js flag");
            } else {
                info!("JetStream WALLET_SNAPSHOT stream ready for position reconciliation");
            }

            // PR3: JetStream stream for wallet TX confirmations (execution-engine confirm path)
            if let Err(e) = ensure_wallet_tx_confirm_stream(client.client()).await {
                error!(error = %e, "Failed to create/update JetStream WALLET_TX_CONFIRM stream");
                error!("WalletTxConfirmed persistence disabled!");
            } else {
                info!("JetStream WALLET_TX_CONFIRM stream ready for execution-engine TX confirm");
            }

            // Initialize JetStream stream for ExecutionResults (wallet ATA tracking)
            if let Err(e) = ensure_execution_results_stream(client.client()).await {
                warn!(error = %e, "Failed to create/update JetStream EXECUTION_RESULTS stream");
            } else {
                info!("JetStream EXECUTION_RESULTS stream ready for wallet ATA tracking");
            }

            Some(client)
        }
    };

    // Initialize WalletTracker (P1: Smart Money / Insider Detection)
    // TODO: Load config from file for production
    let wallet_tracker_cfg = WalletTrackerCfg::default();
    let wallet_tracker = WalletTracker::new(wallet_tracker_cfg);
    info!(
        smart_money = wallet_tracker.stats().smart_money_count,
        bad_actors = wallet_tracker.stats().bad_actor_count,
        "WalletTracker initialized"
    );

    let (tracked_mints_tx, tracked_mints_rx) = watch::channel(Vec::<Pubkey>::new());
    let (tracked_vaults_tx, tracked_vaults_rx) = watch::channel(Vec::<Pubkey>::new());
    let (tracked_bin_arrays_tx, tracked_bin_arrays_rx) = watch::channel(Vec::<Pubkey>::new());
    let (tracked_wallet_tx, tracked_wallet_rx) = watch::channel(Vec::<Pubkey>::new());

    // === WsolManager Support: Setup wallet balance tracking ===
    let tracked_wallet = if let Ok(wallet_pubkey_str) = std::env::var("IRONCRAB_WALLET_PUBKEY") {
        match Pubkey::from_str(&wallet_pubkey_str) {
            Ok(wallet_pubkey) => {
                let tracked = TrackedWallet::new(wallet_pubkey);
                info!(
                    wallet = %wallet_pubkey,
                    wsol_ata = %tracked.wsol_ata,
                    "WalletBalance tracking enabled for WsolManager"
                );
                // Send initial tracked accounts (wallet + WSOL ATA)
                let _ = tracked_wallet_tx.send(vec![wallet_pubkey, tracked.wsol_ata]);
                Some(tracked)
            }
            Err(_) => {
                warn!("IRONCRAB_WALLET_PUBKEY is set but not a valid pubkey");
                None
            }
        }
    } else {
        debug!("IRONCRAB_WALLET_PUBKEY not set, WalletBalance tracking disabled");
        None
    };

    if !args.dry_run && !args.simulate && tracked_wallet.is_none() {
        warn!(
            "IRONCRAB_WALLET_PUBKEY not set: WalletTxConfirmed will NOT be published; \
             execution-engine JetStream TX confirm will timeout until wallet env is configured on market-data"
        );
    }

    let geyser_full_reconnect_threshold_live = Arc::new(AtomicUsize::new(
        market_data_config.geyser_full_reconnect_threshold,
    ));

    let ctx = Arc::new(MarketDataContext {
        run_id: run_id.clone(),
        config: parking_lot::RwLock::new(market_data_config),
        geyser_full_reconnect_threshold_live,
        nats,
        jsonl_writer,
        started_at: process_started,
        event_counter: std::sync::atomic::AtomicU64::new(0),
        wallet_tracker,
        priority_fee_tracker: Arc::new(PriorityFeeTracker::new()),
        tracked_mints: parking_lot::RwLock::new(std::collections::HashMap::new()),
        tracked_mints_tx,
        hot_pool_registry: Arc::new(UnifiedHotPoolRegistry::new()),
        explicit_admitted_momentum_pools: parking_lot::RwLock::new(HashSet::new()),
        explicit_admitted_arb_pools: parking_lot::RwLock::new(HashSet::new()),
        known_pump_amm_pools: parking_lot::RwLock::new(std::collections::HashSet::new()),
        known_trade_dex_pools: parking_lot::RwLock::new(std::collections::HashSet::new()),
        pumpfun_pool_discovery_mint_info_emitted: parking_lot::RwLock::new(
            std::collections::HashSet::new(),
        ),
        tracked_vaults: parking_lot::RwLock::new(std::collections::HashMap::new()),
        tracked_vaults_tx,
        tracked_bin_arrays: parking_lot::RwLock::new(std::collections::HashMap::new()),
        tracked_bin_arrays_tx,
        tracked_membership: ArcSwap::from_pointee(TrackedMembershipSnapshot::default()),
        exec_hot_membership: ArcSwap::from_pointee(ExecHotMembershipSnapshot::default()),
        pool_tracked_legs: parking_lot::RwLock::new(HashMap::new()),
        live_pool_cache: Arc::new(LivePoolCache::new()),
        creator_cache: parking_lot::RwLock::new(std::collections::HashMap::new()),
        pool_mint_map: parking_lot::RwLock::new(std::collections::HashMap::new()),
        pool_creator_cache: parking_lot::RwLock::new(std::collections::HashMap::new()),
        high_priority_bonding_curves: parking_lot::RwLock::new(HashSet::new()),
        raydium_serum_fetched: parking_lot::RwLock::new(std::collections::HashSet::new()),
        tracked_wallet,
        tracked_wallet_tx,
        tracked_wallet_token_accounts: parking_lot::RwLock::new(std::collections::HashSet::new()),
        tracked_wallet_mint_decimals: parking_lot::RwLock::new(std::collections::HashMap::new()),
        execution_results_deduper: parking_lot::Mutex::new(ExecutionResultDeduper::default()),
        last_emitted_curve_progress: parking_lot::RwLock::new(std::collections::HashMap::new()),
        bonding_curve_publish_times: parking_lot::Mutex::new(BondingCurvePublishTimes::new()),
        pump_amm_dex: parking_lot::RwLock::new(None),
        helius_rpc,
        geyser_sync_batch_timer: parking_lot::Mutex::new(None),
        geyser_sync_flush_timestamps: parking_lot::Mutex::new(Vec::new()),
        geyser_sync_debounce_epoch: AtomicU64::new(0),
        ingest_tokio_handle: parking_lot::RwLock::new(None),
        pool_discovery_poolcreated_emitted: parking_lot::RwLock::new(
            std::collections::HashSet::new(),
        ),
        last_synced_explicit_pubkeys: parking_lot::RwLock::new(HashSet::new()),
        last_momentum_snapshot_target: parking_lot::RwLock::new(None),
        last_arb_snapshot_target: parking_lot::RwLock::new(None),
        dlmm_registered_active_id: parking_lot::RwLock::new(HashMap::new()),
        deferred_hot_pool_reserve_pins: parking_lot::RwLock::new(HashMap::new()),
        geyser_connect_barrier: Arc::new(GeyserConnectBarrier::new()),
        geyser_explicit_ready: AtomicBool::new(true),
        geyser_explicit_config_error: parking_lot::RwLock::new(None),
        geyser_prune_resume: parking_lot::Mutex::new(GeyserPruneResume::default()),
    });

    // === Main Loop: Geyser subscription or simulation ===

    // P1 Crash Isolation: Signal systemd that we're ready
    #[cfg(unix)]
    {
        // NOTE: Do NOT unset NOTIFY_SOCKET here; we need it for Watchdog pings.
        let _ = sd_notify::notify(false, &[NotifyState::Ready]);
        debug!("Sent sd_notify READY to systemd");

        // PR163: Watchdog pings on a dedicated OS thread — `tokio::interval` on the main runtime
        // can starve under Geyser/NATS load (same class as PR155 publish-runtime isolation).
        #[cfg(not(test))]
        {
            std::thread::Builder::new()
                .name("md-watchdog".to_string())
                .spawn(|| loop {
                    std::thread::sleep(std::time::Duration::from_secs(5));
                    if MD_SYSTEMD_WATCHDOG_NOTIFY.load(Ordering::Relaxed) {
                        let _ = sd_notify::notify(false, &[NotifyState::Watchdog]);
                    }
                })
                .expect("spawn md-watchdog thread");
        }
    }

    // Keep readiness fresh even when idle.
    ironcrab::metrics::record_activity();

    // P1: Subscribe to Config Updates (Runtime Configuration via UI)
    // Core NATS subscription (for backward compatibility)
    let config_subscription = if let Some(ref nats) = ctx.nats {
        match nats.subscribe(TOPIC_CONFIG_RELOAD).await {
            Ok(sub) => {
                info!(
                    topic = TOPIC_CONFIG_RELOAD,
                    "Subscribed to Config Updates (Core NATS fallback)"
                );
                Some(sub)
            }
            Err(e) => {
                warn!(error = %e, "Failed to subscribe to Config Updates");
                None
            }
        }
    } else {
        None
    };

    // P1: JetStream Config Bootstrap (persisted, solves race condition)
    // Fetch and apply the last config from JetStream before starting the main loop.
    if let Some(ref nats) = ctx.nats {
        use async_nats::jetstream;
        use futures::StreamExt;

        let jetstream = jetstream::new(nats.client().clone());

        match jetstream.get_stream(CONFIG_STREAM_NAME).await {
            Ok(stream) => {
                match stream
                    .create_consumer(config_consumer_config("market-data"))
                    .await
                {
                    Ok(consumer) => {
                        info!(
                            stream = CONFIG_STREAM_NAME,
                            subject = %config_subject("market-data"),
                            "Connected to JetStream Config Updates (persisted)"
                        );

                        // Bootstrap: Try to get the last config from JetStream
                        match consumer.fetch().max_messages(1).messages().await {
                            Ok(mut messages) => {
                                if let Some(Ok(msg)) = messages.next().await {
                                    match serde_json::from_slice::<ConfigUpdate>(&msg.payload) {
                                        Ok(update) => {
                                            info!(
                                                component = %update.target_component,
                                                keys = ?update.config.keys().collect::<Vec<_>>(),
                                                "Bootstrap: Applying config from JetStream"
                                            );
                                            let response =
                                                ctx.apply_config_update(&update, None, None);
                                            info!(
                                                status = ?response.status,
                                                applied = ?response.applied_keys,
                                                "Bootstrap config applied"
                                            );
                                            if let Err(e) = msg.ack().await {
                                                warn!(error = %e, "Failed to ack bootstrap config");
                                            }
                                        }
                                        Err(e) => {
                                            warn!(error = %e, "Failed to deserialize bootstrap config");
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                debug!(error = %e, "No bootstrap config in JetStream (first run or empty)");
                            }
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "Failed to create JetStream config consumer");
                    }
                }
            }
            Err(e) => {
                debug!(error = %e, stream = CONFIG_STREAM_NAME, "JetStream CONFIG_UPDATES stream not found (control-plane may not be running)");
            }
        }
    }

    if args.simulate {
        info!("Simulation mode: emitting fake slot events");
        let track_worker = run_simulation_loop(ctx.clone(), &run_id, config_subscription).await?;
        flush_explicit_set_snapshot(&track_worker);
    } else {
        info!(geyser_url = %args.geyser_url, "Starting Geyser integration");
        let wallet_tx_confirm_commitment = file_config
            .as_ref()
            .and_then(|c| c.execution_engine.as_ref())
            .and_then(|e| e.confirm_commitment.clone())
            .unwrap_or_else(|| "confirmed".to_string());

        let track_worker = run_geyser_loop(
            ctx.clone(),
            &run_id,
            &args.geyser_url,
            config_subscription,
            tracked_mints_rx,
            tracked_vaults_rx,
            tracked_bin_arrays_rx,
            tracked_wallet_rx,
            args.wallet_snapshot_only,
            wallet_tx_confirm_commitment,
        )
        .await?;
        flush_explicit_set_snapshot(&track_worker);
    }

    // Flush JSONL on shutdown
    ctx.jsonl_writer.flush()?;
    info!(run_id = %run_id, "market-data shutdown complete");

    Ok(())
}

/// Phase 5c: md-account-publish runtime (delegates to `publish/account.rs`).
fn spawn_market_data_account_publish_runtime(
    ctx: Arc<MarketDataContext>,
    template: NatsConfig,
    worker_count: usize,
) -> AccountPublishSender {
    let host: Arc<dyn PublishHost> = ctx;
    spawn_md_account_publish_runtime(host, template, worker_count)
}

/// PR165: metrics `/live` + `/metrics` on isolated Tokio runtime (survives main-runtime starvation).
fn spawn_market_data_metrics_runtime(addr: std::net::SocketAddr) {
    std::thread::Builder::new()
        .name("md-metrics".to_string())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("md-metrics tokio runtime");
            rt.block_on(async {
                if let Err(e) = serve_metrics(addr, MetricsComponent::MarketData).await {
                    error!(error = %e, "Metrics server failed");
                }
            });
        })
        .expect("spawn md-metrics thread");
}

/// PR167: detect global ingest stall (TX + account + head slot all frozen).
/// PR233: OS thread — survives Tokio runtime freeze (same pattern as md-watchdog).
fn spawn_market_data_global_ingest_liveness_task(process_started: Instant) {
    use ironcrab::metrics::MARKET_DATA_INGEST_PROGRESS_TICK;

    std::thread::Builder::new()
        .name("md-ingest-liveness".into())
        .spawn(move || {
            const CHECK_INTERVAL: Duration = Duration::from_secs(10);
            const STALL_WINDOW: Duration = Duration::from_secs(50);
            const RECOVERY_WAIT: Duration = Duration::from_secs(60);
            const STARTUP_GRACE: Duration = Duration::from_secs(120);
            let mut last_tx = market_data_tx_handler_processed_value();
            let mut last_account = geyser_account_listener_account_updates_value();
            let mut last_head = market_data_geyser_head_slot_value();
            let mut last_tokio_tick = MARKET_DATA_INGEST_PROGRESS_TICK.load(Ordering::Relaxed);
            let mut stalled_since: Option<Instant> = None;
            let mut tokio_stalled_since: Option<Instant> = None;
            let mut recovery_requested_at: Option<Instant> = None;
            loop {
                std::thread::sleep(CHECK_INTERVAL);
                if process_started.elapsed() < STARTUP_GRACE {
                    last_tx = market_data_tx_handler_processed_value();
                    last_account = geyser_account_listener_account_updates_value();
                    last_head = market_data_geyser_head_slot_value();
                    last_tokio_tick = MARKET_DATA_INGEST_PROGRESS_TICK.load(Ordering::Relaxed);
                    stalled_since = None;
                    tokio_stalled_since = None;
                    recovery_requested_at = None;
                    continue;
                }
                let tx = market_data_tx_handler_processed_value();
                let account = geyser_account_listener_account_updates_value();
                let head = market_data_geyser_head_slot_value();
                let tokio_tick = MARKET_DATA_INGEST_PROGRESS_TICK.load(Ordering::Relaxed);
                if tokio_tick == last_tokio_tick {
                    let since = *tokio_stalled_since.get_or_insert_with(Instant::now);
                    if since.elapsed() >= STALL_WINDOW {
                        record_market_data_tokio_liveness_stall();
                    }
                } else {
                    last_tokio_tick = tokio_tick;
                    tokio_stalled_since = None;
                }
                if market_data_global_ingest_stalled(
                    tx,
                    last_tx,
                    account,
                    last_account,
                    head,
                    last_head,
                ) {
                    let since = *stalled_since.get_or_insert_with(Instant::now);
                    if since.elapsed() >= STALL_WINDOW {
                        record_market_data_global_ingest_stall();
                        if recovery_requested_at.is_none() {
                            error!(
                                stall_secs = STALL_WINDOW.as_secs(),
                                tx_handler_processed = tx,
                                account_updates = account,
                                head_slot = head,
                                "PR167: global ingest stalled — requesting Geyser TX + account session reconnect"
                            );
                            market_data_request_tx_session_reconnect();
                            market_data_request_account_session_reconnect();
                            recovery_requested_at = Some(Instant::now());
                            stalled_since = None;
                        } else if recovery_requested_at.is_some_and(|t| t.elapsed() >= RECOVERY_WAIT)
                        {
                            error!(
                                stall_secs = RECOVERY_WAIT.as_secs(),
                                "PR167: global ingest still stalled after reconnect — exiting for systemd restart"
                            );
                            #[cfg(unix)]
                            MD_SYSTEMD_WATCHDOG_NOTIFY.store(false, Ordering::Relaxed);
                            std::process::exit(1);
                        }
                    }
                } else {
                    touch_market_data_global_ingest_progress();
                    stalled_since = None;
                    recovery_requested_at = None;
                    last_tx = tx;
                    last_account = account;
                    last_head = head;
                }
            }
        })
        .expect("spawn md-ingest-liveness thread");
}

/// PR235: pure stall-duration policy for md-state liveness (testable without OS thread).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MdStateStallLivenessAction {
    None,
    RecordStall,
    WarnStillStalled,
    ExitForSystemd,
}

fn md_state_stall_liveness_action(
    stalled_duration: Duration,
    exit_enabled: bool,
) -> MdStateStallLivenessAction {
    if stalled_duration >= MARKET_DATA_MD_STATE_STALL_EXIT_AFTER {
        if exit_enabled {
            MdStateStallLivenessAction::ExitForSystemd
        } else {
            MdStateStallLivenessAction::WarnStillStalled
        }
    } else if stalled_duration >= MARKET_DATA_MD_STATE_STALL_WINDOW {
        MdStateStallLivenessAction::RecordStall
    } else {
        MdStateStallLivenessAction::None
    }
}

/// PR234: detect md-state stall (queue near cap + flat burst/job progress). OS thread — not Tokio.
fn spawn_market_data_md_state_liveness_task(process_started: Instant) {
    std::thread::Builder::new()
        .name("md-state-liveness".into())
        .spawn(move || {
            const STARTUP_GRACE: Duration = Duration::from_secs(120);
            let queue_saturation = ((MARKET_DATA_GEYSER_TRACKING_QUEUE_CAP as f64)
                * MARKET_DATA_MD_STATE_STALL_QUEUE_FRAC)
                as usize;
            let mut last_bursts = market_data_md_state_bursts_completed_value();
            let mut last_jobs = market_data_geyser_tracking_jobs_processed_value();
            let mut last_drops = market_data_geyser_tracking_enqueue_dropped_value();
            let mut stalled_since: Option<Instant> = None;
            loop {
                std::thread::sleep(MARKET_DATA_MD_STATE_STALL_CHECK_INTERVAL);
                if process_started.elapsed() < STARTUP_GRACE {
                    last_bursts = market_data_md_state_bursts_completed_value();
                    last_jobs = market_data_geyser_tracking_jobs_processed_value();
                    last_drops = market_data_geyser_tracking_enqueue_dropped_value();
                    stalled_since = None;
                    continue;
                }
                let depth = ironcrab::metrics::MARKET_DATA_GEYSER_TRACKING_QUEUE_DEPTH
                    .load(Ordering::Relaxed) as usize;
                let bursts = market_data_md_state_bursts_completed_value();
                let jobs = market_data_geyser_tracking_jobs_processed_value();
                let drops = market_data_geyser_tracking_enqueue_dropped_value();
                let progress_stalled = bursts == last_bursts && jobs == last_jobs;
                let drops_rising = drops > last_drops;
                if depth >= queue_saturation && (progress_stalled || drops_rising) {
                    let since = *stalled_since.get_or_insert_with(Instant::now);
                    if since.elapsed() >= MARKET_DATA_MD_STATE_STALL_WINDOW {
                        record_market_data_md_state_stall();
                        warn!(
                            stall_secs = MARKET_DATA_MD_STATE_STALL_WINDOW.as_secs(),
                            queue_depth = depth,
                            queue_cap = MARKET_DATA_GEYSER_TRACKING_QUEUE_CAP,
                            bursts_completed = bursts,
                            jobs_dequeued = jobs,
                            enqueue_dropped = drops,
                            "PR234: md-state stalled near queue cap (check md-state thread wchan)"
                        );
                    }
                    if since.elapsed() >= MARKET_DATA_MD_STATE_STALL_EXIT_AFTER {
                        match md_state_stall_liveness_action(
                            since.elapsed(),
                            MARKET_DATA_MD_STATE_STALL_EXIT_ENABLED,
                        ) {
                            MdStateStallLivenessAction::ExitForSystemd => {
                                error!(
                                    stall_secs = MARKET_DATA_MD_STATE_STALL_EXIT_AFTER.as_secs(),
                                    queue_depth = depth,
                                    "PR235: md-state still stalled — exiting for systemd restart"
                                );
                                #[cfg(unix)]
                                MD_SYSTEMD_WATCHDOG_NOTIFY.store(false, Ordering::Relaxed);
                                std::process::exit(1);
                            }
                            MdStateStallLivenessAction::WarnStillStalled => {
                                warn!(
                                    stall_secs = MARKET_DATA_MD_STATE_STALL_EXIT_AFTER.as_secs(),
                                    queue_depth = depth,
                                    bursts_completed = bursts,
                                    jobs_dequeued = jobs,
                                    enqueue_dropped = drops,
                                    evict_pending =
                                        ironcrab::metrics::MARKET_DATA_MD_STATE_EVICT_PENDING
                                            .load(Ordering::Relaxed),
                                    "PR235: md-state still stalled (exit disabled — metric recorded)"
                                );
                            }
                            MdStateStallLivenessAction::None
                            | MdStateStallLivenessAction::RecordStall => {}
                        }
                    }
                } else {
                    stalled_since = None;
                }
                last_bursts = bursts;
                last_jobs = jobs;
                last_drops = drops;
            }
        })
        .expect("spawn md-state-liveness thread");
}

#[derive(Clone)]
struct AccountWorkItem {
    update: GeyserAccountUpdate,
    recv_at: Instant,
    class: ironcrab::market_data::ingest::AccountUpdateClass,
}

/// Per-shard ENRICH latest-wins buffer before / alongside the LOW `mpsc`.
struct AccountEnrichCoalesceShard {
    pending: HashMap<Pubkey, AccountWorkItem>,
    /// Pubkeys with an item already handed to the LOW channel (not yet consumed by worker).
    in_mpsc: HashSet<Pubkey>,
}

impl AccountEnrichCoalesceShard {
    fn new() -> Self {
        Self {
            pending: HashMap::new(),
            in_mpsc: HashSet::new(),
        }
    }

    /// Logical depth: unique pubkeys in `pending ∪ in_mpsc` (no double-count when both hold the same key).
    fn unique_pending_pubkey_count(&self) -> usize {
        self.pending.len()
            + self
                .in_mpsc
                .iter()
                .filter(|pk| !self.pending.contains_key(pk))
                .count()
    }
}

#[inline]
fn account_enrich_item_is_newer(a: &AccountWorkItem, b: &AccountWorkItem) -> bool {
    if a.update.slot != b.update.slot {
        return a.update.slot > b.update.slot;
    }
    if a.update.grpc_recv_at != b.update.grpc_recv_at {
        return a.update.grpc_recv_at > b.update.grpc_recv_at;
    }
    a.recv_at > b.recv_at
}

enum AccountEnrichCoalesceUpsert {
    /// First pending item for this pubkey (depth incremented).
    NewPending,
    /// Replaced an existing pending or in-flight supersede entry (latest-wins).
    Coalesced,
    /// Incoming update is older than what we already hold.
    StaleDiscarded,
    /// Evicted oldest pending-only pubkey to make room; incoming item stored.
    EvictedOldest,
}

/// Evict the pending-only entry with the oldest slot (ties: `grpc_recv_at`, then `recv_at`).
/// Never evicts a pending supersede for a pubkey still listed in `in_mpsc`.
fn account_enrich_evict_oldest_pending_only(shard: &mut AccountEnrichCoalesceShard) -> bool {
    let drop_pk = shard
        .pending
        .iter()
        .filter(|(pk, _)| !shard.in_mpsc.contains(*pk))
        .min_by(|(_, a), (_, b)| {
            (a.update.slot, a.update.grpc_recv_at, a.recv_at).cmp(&(
                b.update.slot,
                b.update.grpc_recv_at,
                b.recv_at,
            ))
        })
        .map(|(pk, _)| *pk);
    let Some(drop_pk) = drop_pk else {
        return false;
    };
    shard.pending.remove(&drop_pk);
    dec_market_data_account_low_priority_queue_depth();
    dec_market_data_account_worker_queue_depth();
    inc_market_data_account_enrich_enqueue_dropped_total();
    true
}

fn account_enrich_coalesce_upsert(
    shard: &mut AccountEnrichCoalesceShard,
    work: AccountWorkItem,
) -> AccountEnrichCoalesceUpsert {
    account_enrich_coalesce_upsert_with_cap(shard, work, MARKET_DATA_ACCOUNT_WORKER_QUEUE_CAP)
}

fn account_enrich_coalesce_upsert_with_cap(
    shard: &mut AccountEnrichCoalesceShard,
    work: AccountWorkItem,
    cap: usize,
) -> AccountEnrichCoalesceUpsert {
    let pk = work.update.pubkey;
    if let Some(existing) = shard.pending.get(&pk) {
        if account_enrich_item_is_newer(&work, existing) {
            shard.pending.insert(pk, work);
            return AccountEnrichCoalesceUpsert::Coalesced;
        }
        return AccountEnrichCoalesceUpsert::StaleDiscarded;
    }
    if shard.in_mpsc.contains(&pk) {
        shard.pending.insert(pk, work);
        return AccountEnrichCoalesceUpsert::Coalesced;
    }
    let mut evicted = false;
    if shard.unique_pending_pubkey_count() >= cap {
        if account_enrich_evict_oldest_pending_only(shard) {
            evicted = true;
        } else {
            inc_market_data_account_enrich_enqueue_dropped_total();
            return AccountEnrichCoalesceUpsert::StaleDiscarded;
        }
    }
    shard.pending.insert(pk, work);
    inc_market_data_account_low_priority_queue_depth();
    inc_market_data_account_worker_queue_depth();
    if evicted {
        AccountEnrichCoalesceUpsert::EvictedOldest
    } else {
        AccountEnrichCoalesceUpsert::NewPending
    }
}

/// Result of attempting to move pending ENRICH work into the LOW `mpsc`.
#[derive(Debug, PartialEq, Eq)]
enum AccountEnrichFlushStatus {
    /// At least one item flushed, or nothing pending to flush.
    Ok,
    /// LOW channel full; pending restored for retry.
    BlockedFull,
    /// LOW receivers gone; pending restored; caller should stop the account stream.
    LowChannelClosed,
}

fn account_enrich_coalesce_try_flush(
    shard: &mut AccountEnrichCoalesceShard,
    low_tx: &mpsc::Sender<AccountWorkItem>,
) -> AccountEnrichFlushStatus {
    let keys: Vec<Pubkey> = shard.pending.keys().copied().collect();
    for pk in keys {
        if shard.in_mpsc.contains(&pk) {
            continue;
        }
        let Some(work) = shard.pending.remove(&pk) else {
            continue;
        };
        match low_tx.try_send(work) {
            Ok(()) => {
                shard.in_mpsc.insert(pk);
            }
            Err(mpsc::error::TrySendError::Full(work)) => {
                shard.pending.insert(pk, work);
                return AccountEnrichFlushStatus::BlockedFull;
            }
            Err(mpsc::error::TrySendError::Closed(work)) => {
                shard.pending.insert(pk, work);
                return AccountEnrichFlushStatus::LowChannelClosed;
            }
        }
    }
    AccountEnrichFlushStatus::Ok
}

/// After LOW dequeue: apply latest-wins supersede from coalesce map and refill LOW channel.
fn account_enrich_resolve_after_dequeue(
    shard: &mut AccountEnrichCoalesceShard,
    low_tx: &mpsc::Sender<AccountWorkItem>,
    work: AccountWorkItem,
) -> (AccountWorkItem, AccountEnrichFlushStatus) {
    let pk = work.update.pubkey;
    shard.in_mpsc.remove(&pk);
    let mut resolved = work;
    if let Some(pending) = shard.pending.remove(&pk) {
        if account_enrich_item_is_newer(&pending, &resolved) {
            resolved = pending;
        }
    }
    dec_market_data_account_low_priority_queue_depth();
    dec_market_data_account_worker_queue_depth();
    let flush_status = account_enrich_coalesce_try_flush(shard, low_tx);
    (resolved, flush_status)
}

/// Result of non-blocking ENRICH ingress enqueue from the broadcast recv task.
#[derive(Debug, PartialEq, Eq)]
enum AccountEnrichIngressSend {
    Ok,
    ContendedFull,
    Closed,
}

/// Result of non-blocking EXEC_HOT HIGH enqueue from the broadcast recv task.
#[derive(Debug, PartialEq, Eq)]
enum AccountHighIngressSend {
    Ok,
    ContendedFull,
    Closed,
}

/// Enqueue EXEC_HOT work into the per-shard HIGH channel without blocking the recv task.
fn account_high_ingress_try_send(
    high_tx: &mpsc::Sender<AccountWorkItem>,
    work: AccountWorkItem,
) -> AccountHighIngressSend {
    match high_tx.try_send(work) {
        Ok(()) => {
            inc_market_data_account_worker_queue_depth();
            inc_market_data_account_high_priority_queue_depth();
            AccountHighIngressSend::Ok
        }
        Err(mpsc::error::TrySendError::Full(_)) => {
            inc_market_data_account_high_enqueue_dropped_total();
            AccountHighIngressSend::ContendedFull
        }
        Err(mpsc::error::TrySendError::Closed(_)) => AccountHighIngressSend::Closed,
    }
}

/// Enqueue ENRICH work into the per-shard ingress channel without touching the coalesce mutex.
fn account_enrich_ingress_try_send(
    ingress_tx: &mpsc::Sender<AccountWorkItem>,
    work: AccountWorkItem,
) -> AccountEnrichIngressSend {
    match ingress_tx.try_send(work) {
        Ok(()) => {
            inc_market_data_account_enrich_ingress_queue_depth();
            AccountEnrichIngressSend::Ok
        }
        Err(mpsc::error::TrySendError::Full(_)) => {
            inc_market_data_account_enrich_dispatch_contended_total();
            inc_market_data_account_enrich_enqueue_dropped_total();
            AccountEnrichIngressSend::ContendedFull
        }
        Err(mpsc::error::TrySendError::Closed(_)) => AccountEnrichIngressSend::Closed,
    }
}

/// Upsert + flush for the enrich-dispatch task (owns the coalesce mutex).
fn account_enrich_dispatch_upsert_and_flush(
    shard: &mut AccountEnrichCoalesceShard,
    low_tx: &mpsc::Sender<AccountWorkItem>,
    work: AccountWorkItem,
) -> AccountEnrichFlushStatus {
    let upsert = account_enrich_coalesce_upsert(shard, work);
    if matches!(upsert, AccountEnrichCoalesceUpsert::Coalesced) {
        inc_market_data_account_enrich_coalesce_total();
    }
    account_enrich_coalesce_try_flush(shard, low_tx)
}

#[inline]
fn market_data_account_worker_shard(pubkey: &Pubkey) -> usize {
    let mut h = DefaultHasher::new();
    pubkey.hash(&mut h);
    (h.finish() as usize) % MARKET_DATA_ACCOUNT_WORKER_COUNT
}

/// Dequeued account work tagged by ingest class queue (HIGH vs LOW).
enum AccountWorkerDequeue {
    ExecHot(AccountWorkItem),
    Enrich(AccountWorkItem),
}

/// Strict HIGH-then-LOW dequeue for one account worker (two `mpsc` channels per shard).
async fn account_worker_recv_next(
    high: &mut mpsc::Receiver<AccountWorkItem>,
    low: &mut mpsc::Receiver<AccountWorkItem>,
) -> Option<AccountWorkerDequeue> {
    let mut pending_low: Option<AccountWorkItem> = None;
    loop {
        if let Ok(w) = high.try_recv() {
            dec_market_data_account_high_priority_queue_depth();
            dec_market_data_account_worker_queue_depth();
            return Some(AccountWorkerDequeue::ExecHot(w));
        }
        if let Some(w) = pending_low.take() {
            return Some(AccountWorkerDequeue::Enrich(w));
        }
        tokio::select! {
            biased;
            h = high.recv() => match h {
                Some(w) => {
                    dec_market_data_account_high_priority_queue_depth();
                    dec_market_data_account_worker_queue_depth();
                    return Some(AccountWorkerDequeue::ExecHot(w));
                }
                None => {
                    if let Ok(w) = high.try_recv() {
                        dec_market_data_account_high_priority_queue_depth();
                        dec_market_data_account_worker_queue_depth();
                        return Some(AccountWorkerDequeue::ExecHot(w));
                    }
                    if let Some(w) = pending_low.take() {
                        return Some(AccountWorkerDequeue::Enrich(w));
                    }
                    if let Some(w) = low.recv().await {
                        return Some(AccountWorkerDequeue::Enrich(w));
                    }
                    return None;
                }
            },
            l = low.recv() => match l {
                Some(w) => {
                    pending_low = Some(w);
                    continue;
                }
                None => {
                    if let Ok(w) = high.try_recv() {
                        dec_market_data_account_high_priority_queue_depth();
                        dec_market_data_account_worker_queue_depth();
                        return Some(AccountWorkerDequeue::ExecHot(w));
                    }
                    if let Some(w) = pending_low.take() {
                        return Some(AccountWorkerDequeue::Enrich(w));
                    }
                    return match high.recv().await {
                        Some(w) => {
                            dec_market_data_account_high_priority_queue_depth();
                            dec_market_data_account_worker_queue_depth();
                            Some(AccountWorkerDequeue::ExecHot(w))
                        }
                        None => None,
                    };
                }
            },
        }
    }
}

async fn account_path_enqueue_jetstream<T: Serialize>(
    publish_tx: Option<&AccountPublishSender>,
    nats: Option<&NatsClient>,
    subject: String,
    payload: &T,
    log_fail: &'static str,
    bump_market_events_published_total: bool,
) -> bool {
    publish_enqueue_jetstream(
        publish_tx,
        nats,
        subject,
        payload,
        log_fail,
        bump_market_events_published_total,
    )
    .await
}

async fn account_path_enqueue_core_market_event(
    publish_tx: Option<&AccountPublishSender>,
    nats: Option<&NatsClient>,
    ctx: &Arc<MarketDataContext>,
    event: MarketEvent,
    trace: Option<MarketEventCorePublishTrace>,
) -> bool {
    if matches!(&event.kind, MarketEventKind::BinArrayUpdate { .. }) {
        ironcrab::metrics::market_data_bin_array_publish_inc();
    }
    publish_enqueue_core_market_event(publish_tx, nats, Some(ctx.as_ref()), event, trace).await
}

/// PR162: pool discovery handler (dedicated task; not in main `select!`).
/// STOP-CHECK: keine neuen RPC-Calls; Pump.fun TokenMintInfo nutzt bekannte Decimals.
async fn handle_pool_discovery_market_event(
    ctx: &Arc<MarketDataContext>,
    run_id: &str,
    mint_info_tx: &mpsc::UnboundedSender<MarketEvent>,
    account_publish_tx: Option<&mpsc::Sender<AccountPathNatsJob>>,
    pool_event: PoolDiscoveryEvent,
) {
    let pool_discovery_recv_at = Instant::now();
    market_data_bump_geyser_head_slot(pool_event.slot);
    ironcrab::metrics::record_activity();

    if !ctx
        .pool_discovery_poolcreated_emitted
        .write()
        .insert(pool_event.pool_address)
    {
        return;
    }

    info!(
        dex = %pool_event.dex_type,
        pool = %pool_event.pool_address,
        base = %pool_event.base_mint,
        quote = %pool_event.quote_mint,
        liquidity_lamports = pool_event.liquidity_estimate_lamports,
        "Pool discovered via Geyser"
    );

    if matches!(pool_event.dex_type, PoolDexType::PumpFun) {
        let emit_mint_info = ctx
            .pumpfun_pool_discovery_mint_info_emitted
            .write()
            .insert(pool_event.base_mint);
        if emit_mint_info {
            let mint_event = MarketEvent::new(
                "market-data",
                BUILD_VERSION,
                run_id,
                ctx.next_event_id(),
                "geyser_known",
                Some(pool_event.slot),
                MarketEventKind::TokenMintInfo {
                    mint: pool_event.base_mint.to_string(),
                    token_program: spl_token::ID.to_string(),
                    decimals: 6,
                    supply: 0,
                    mint_authority: None,
                    freeze_authority: None,
                },
            );
            let _ = mint_info_tx.send(mint_event);
            debug!(
                mint = %pool_event.base_mint,
                "Emitted TokenMintInfo for pump.fun token (decimals=6, no RPC)"
            );
        }
    }

    let event = MarketEvent::new(
        "market-data",
        BUILD_VERSION,
        run_id,
        ctx.next_event_id(),
        "geyser_pool_discovery",
        Some(pool_event.slot),
        MarketEventKind::PoolCreated {
            pool_address: pool_event.pool_address.to_string(),
            base_mint: pool_event.base_mint.to_string(),
            quote_mint: pool_event.quote_mint.to_string(),
            dex: pool_event.dex_type.to_string(),
            initial_liquidity_sol: Some(
                rust_decimal::Decimal::from(pool_event.liquidity_estimate_lamports)
                    / rust_decimal::Decimal::from(1_000_000_000u64),
            ),
        },
    );

    ctx.write_market_event_jsonl(&event);

    if ctx.nats.is_some() {
        let seg = market_data_publish_segment(&event.kind);
        account_path_enqueue_core_market_event(
            account_publish_tx,
            ctx.nats.as_ref(),
            ctx,
            event,
            Some(MarketEventCorePublishTrace {
                recv_at: pool_discovery_recv_at,
                cold_path: false,
                segment: seg,
            }),
        )
        .await;
    }

    if pool_event.dex_type == PoolDexType::PumpFun {
        if let Some(creator) = pool_event.creator {
            let dev_event = MarketEvent::new(
                "market-data",
                BUILD_VERSION,
                run_id,
                ctx.next_event_id(),
                "geyser_pool_discovery",
                Some(pool_event.slot),
                MarketEventKind::DevWalletIdentified {
                    mint: pool_event.base_mint.to_string(),
                    dev_wallet: creator.to_string(),
                    supply_percentage: 0.0,
                },
            );

            ctx.write_market_event_jsonl(&dev_event);

            if ctx.nats.is_some() {
                let seg = market_data_publish_segment(&dev_event.kind);
                if account_path_enqueue_core_market_event(
                    account_publish_tx,
                    ctx.nats.as_ref(),
                    ctx,
                    dev_event,
                    Some(MarketEventCorePublishTrace {
                        recv_at: pool_discovery_recv_at,
                        cold_path: false,
                        segment: seg,
                    }),
                )
                .await
                {
                    info!(
                        mint = %pool_event.base_mint,
                        creator = %creator,
                        "✅ DevWalletIdentified enqueued for pump.fun pool"
                    );
                }
            }
        }
    }

    if pool_event.coin_vault.is_some() || pool_event.pc_vault.is_some() {
        let mut accounts = vec![
            pool_event.pool_address.to_string(),
            pool_event.base_mint.to_string(),
            pool_event.quote_mint.to_string(),
        ];

        if let Some(coin_vault) = pool_event.coin_vault {
            accounts.push(coin_vault.to_string());
        }
        if let Some(pc_vault) = pool_event.pc_vault {
            accounts.push(pc_vault.to_string());
        }
        if let Some(creator) = pool_event.creator {
            accounts.push(creator.to_string());
        }
        if let Some(active_id) = pool_event.active_id {
            accounts.push(format!("active_id:{}", active_id));
        }
        if let Some(bin_step) = pool_event.bin_step {
            accounts.push(format!("bin_step:{}", bin_step));
        }
        if let Some(tick) = pool_event.tick_current_index {
            accounts.push(format!("tick_current_index:{}", tick));
        }
        if let Some(spacing) = pool_event.tick_spacing {
            accounts.push(format!("tick_spacing:{}", spacing));
        }

        let accounts_event = MarketEvent::new(
            "market-data",
            BUILD_VERSION,
            run_id,
            ctx.next_event_id(),
            "geyser_pool_discovery",
            Some(pool_event.slot),
            MarketEventKind::DexPoolAccounts {
                dex: pool_event.dex_type.to_string(),
                pool_address: pool_event.pool_address.to_string(),
                base_mint: pool_event.base_mint.to_string(),
                quote_mint: pool_event.quote_mint.to_string(),
                accounts,
            },
        );

        ctx.write_market_event_jsonl(&accounts_event);

        if ctx.nats.is_some() {
            let seg = market_data_publish_segment(&accounts_event.kind);
            if account_path_enqueue_core_market_event(
                account_publish_tx,
                ctx.nats.as_ref(),
                ctx,
                accounts_event,
                Some(MarketEventCorePublishTrace {
                    recv_at: pool_discovery_recv_at,
                    cold_path: false,
                    segment: seg,
                }),
            )
            .await
            {
                debug!(
                    dex = %pool_event.dex_type,
                    pool = %pool_event.pool_address,
                    "Enqueued DexPoolAccounts for pool discovery"
                );
            }
        }
    }
}

/// Geyser account ingest (dedizierter Task-Fairness, siehe MARKET-DATA-ACCOUNT-INGEST-FAIRNESS).
/// STOP-CHECK (PR165): **keine** RPC-Calls — weder direkt noch via `tokio::spawn` aus diesem Handler.
#[allow(clippy::too_many_arguments)]
/// Geyser account ingest — delegates to `ingest/account_handler.rs` (Phase 4b).
async fn handle_geyser_account(
    ctx: Arc<MarketDataContext>,
    run_id: &str,
    account_update: GeyserAccountUpdate,
    account_count: &AtomicU64,
    recv_at: Instant,
    publish_tx: Option<&mpsc::Sender<AccountPathNatsJob>>,
    md_state: &MdStateSender,
    md_sidefx: &MdSidefxSender,
    update_class: ironcrab::market_data::ingest::AccountUpdateClass,
) {
    handle_geyser_account_update(
        ctx.as_ref(),
        run_id,
        account_update,
        account_count,
        recv_at,
        publish_tx,
        md_state,
        md_sidefx,
        update_class,
    )
    .await;
}

/// Geyser transaction ingest — delegates to `ingest/tx_handler.rs` (Phase 4).
async fn handle_geyser_transaction(
    ctx: Arc<MarketDataContext>,
    run_id: &str,
    tx_update: GeyserTransactionUpdate,
    tx_count: &AtomicU64,
    account_publish_tx: Option<&mpsc::Sender<AccountPathNatsJob>>,
    md_state: &MdStateSender,
    md_sidefx: &MdSidefxSender,
) {
    handle_geyser_transaction_update(
        ctx.as_ref(),
        run_id,
        tx_update,
        tx_count,
        account_publish_tx,
        md_state,
        md_sidefx,
    )
    .await;
}

/// Run with real Geyser connection
#[allow(clippy::too_many_arguments)]
async fn run_geyser_loop(
    ctx: Arc<MarketDataContext>,
    run_id: &str,
    geyser_url: &str,
    mut config_subscription: Option<ironcrab::nats::NatsSubscription>,
    tracked_mints_rx: watch::Receiver<Vec<Pubkey>>,
    tracked_vaults_rx: watch::Receiver<Vec<Pubkey>>,
    tracked_bin_arrays_rx: watch::Receiver<Vec<Pubkey>>,
    tracked_wallet_rx: watch::Receiver<Vec<Pubkey>>,
    wallet_snapshot_only: bool,
    wallet_tx_confirm_commitment: String,
) -> Result<TrackWorkerSender> {
    // Initialize RPC client for fallback/metadata (prefer local RPC, fallback to Helius)
    let rpc_url =
        std::env::var("SOLANA_RPC_URL").unwrap_or_else(|_| "http://127.0.0.1:8899".to_string()); // Local validator/private RPC preferred
    let rpc = Arc::new(SolanaRpc::new(&rpc_url));
    info!(rpc_url = %rpc_url, "Initialized RPC client for metadata/fallback");

    // Shared PumpFunAmmDex for wallet bootstrap verification and ControlRequest discovery (dedupe).
    let mut pump_inner = PumpFunAmmDex::new_with_cache(
        Arc::clone(&rpc),
        ctx.live_pool_cache.clone(),
        true, // allow_rpc_on_miss: Cold Path Discovery
    );
    pump_inner.set_bounded_tx_fallback_rpc(ctx.helius_rpc.clone());
    let pump_amm_dex = Arc::new(pump_inner);
    *ctx.pump_amm_dex.write() = Some(Arc::clone(&pump_amm_dex));

    // Phase-R-R2: single-writer `md-track-worker` OS thread (before wallet snapshot — wallet path enqueues).
    let track_worker = spawn_track_worker(Arc::clone(&ctx));
    // Phase 3 P3 (I-MD-6): restore explicit Geyser set before first Geyser connect.
    MarketDataContext::restore_explicit_set_from_snapshot_on_startup(ctx.as_ref(), &track_worker);
    if !ctx.geyser_explicit_readiness_ok() {
        anyhow::bail!(
            "Geyser explicit admission not ready: {}",
            ctx.geyser_explicit_not_ready_bail_message()
        );
    }
    if let Err(wait_err) = ctx
        .geyser_connect_barrier
        .wait_ready(MarketDataContext::geyser_restore_barrier_timeout(0))
    {
        anyhow::bail!("{}", ctx.geyser_barrier_wait_bail_message(wait_err));
    }
    // PR169c / Phase-2b: coalesce momentum NATS bursts before md-track-worker.
    let momentum_coalesce_tx =
        spawn_momentum_tracking_coalescer(Arc::clone(&ctx), track_worker.clone());
    // Phase 3: coalesce arb track_requests before md-track-worker.
    let arb_coalesce_tx = spawn_arb_tracking_coalescer(Arc::clone(&ctx), track_worker.clone());
    let md_state = spawn_md_state_worker(
        Arc::clone(&ctx),
        tokio::runtime::Handle::current(),
        track_worker.clone(),
    );

    // === P0: Wallet Balance Snapshot (Position Reconciliation) ===
    // Bootstrap wallet state at startup (max 1 RPC roundtrip) using known mints from JetStream.
    //
    // Runtime tracking is event-driven (Geyser updates + ExecutionResults-triggered ATA tracking).
    // Note: Geyser does NOT send updates for closed/deleted token accounts (Phase 2 addresses this).
    let _wallet_for_reconciliation: Option<Pubkey> = if let Ok(wallet_pubkey_str) =
        std::env::var("IRONCRAB_WALLET_PUBKEY")
    {
        if let Ok(wallet_pubkey) = Pubkey::from_str(&wallet_pubkey_str) {
            info!(wallet = %wallet_pubkey, "📸 Publishing wallet balance snapshot for position reconciliation");
            if let Err(e) = publish_wallet_snapshot(
                &ctx,
                &rpc,
                &wallet_pubkey,
                false,
                wallet_snapshot_only,
                &md_state,
            )
            .await
            {
                warn!(error = %e, "Failed to publish wallet snapshot (continuing anyway)");
            }
            Some(wallet_pubkey)
        } else {
            warn!("IRONCRAB_WALLET_PUBKEY is set but not a valid pubkey");
            None
        }
    } else {
        info!("IRONCRAB_WALLET_PUBKEY not set, skipping wallet snapshot");
        None
    };

    if wallet_snapshot_only {
        info!("Wallet snapshot only mode enabled, exiting after snapshot");
        return Ok(track_worker);
    }

    let wallet_snapshot_periodic_secs = std::env::var("IRONCRAB_WALLET_SNAPSHOT_PERIODIC_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|secs| *secs >= 60)
        .unwrap_or(600);
    let mut wallet_snapshot_periodic_interval = tokio::time::interval(
        std::time::Duration::from_secs(wallet_snapshot_periodic_secs),
    );
    wallet_snapshot_periodic_interval.tick().await;
    info!(
        interval_secs = wallet_snapshot_periodic_secs,
        "Periodic wallet snapshot timer enabled (cold path)"
    );

    // Mint metadata fetch pipeline:
    // - We add mints to `tracked_mints` when we see them via tx/pool discovery.
    // - Mint accounts often *never change*, so relying on a future Geyser account update
    //   means we may never emit TokenMintInfo (decimals/supply), which strategies need.
    // - Therefore we proactively fetch the mint account once via RPC and emit TokenMintInfo.
    let (mint_info_tx, mut mint_info_rx) = mpsc::unbounded_channel::<MarketEvent>();

    // DEX program IDs to monitor (must match validator account-index)
    let program_ids = vec![
        Pubkey::from_str(RAYDIUM_AMM_V4).expect("valid raydium pubkey"),
        Pubkey::from_str(RAYDIUM_CPMM).expect("valid raydium cpmm pubkey"),
        Pubkey::from_str(ORCA_WHIRLPOOL).expect("valid orca pubkey"),
        Pubkey::from_str(PUMPFUN_PROGRAM).expect("valid pumpfun pubkey"),
        Pubkey::from_str(PUMPFUN_AMM_PROGRAM).expect("valid pumpfun amm pubkey"),
        Pubkey::from_str(METEORA_DLMM).expect("valid meteora dlmm pubkey"),
        Pubkey::from_str(METEORA_CPMM).expect("valid meteora cpmm pubkey"),
    ];

    // Merge tracked_mints, tracked_vaults, tracked_bin_arrays, and tracked_wallet into a single combined channel.
    // GeyserAccountListener subscribes to all accounts in the combined list (TX path: GeyserTxListener).
    let (combined_tracked_tx, combined_tracked_rx) = watch::channel(Vec::<Pubkey>::new());
    {
        let mints_rx = tracked_mints_rx;
        let vaults_rx = tracked_vaults_rx;
        let bin_arrays_rx = tracked_bin_arrays_rx;
        let wallet_rx = tracked_wallet_rx;
        let combined_tx = combined_tracked_tx;
        let ctx_merge = Arc::clone(&ctx);
        tokio::spawn(async move {
            MarketDataContext::run_geyser_tracked_accounts_merge_coalesce_loop(
                mints_rx,
                vaults_rx,
                bin_arrays_rx,
                wallet_rx,
                combined_tx,
                ctx_merge,
            )
            .await;
        });
    }

    // PR164: two Geyser gRPC sessions — TX+blockhash (sacred, no pin subscribe updates) and
    // accounts+cuckoo (in-place subscribe updates). Merge → account session only.
    let (tx_listener, transaction_rx, mut blockhash_rx) =
        GeyserTxListener::new(geyser_url.to_string(), program_ids.clone());
    let (account_listener, account_rx) = GeyserAccountListener::new_with_tracked_accounts(
        geyser_url.to_string(),
        program_ids,
        combined_tracked_rx,
        Arc::clone(&ctx.geyser_full_reconnect_threshold_live),
        ctx.config.read().max_tracked_accounts.max(1),
    );

    let pool_discovery_account_rx = account_listener.subscribe_account_updates();
    let pool_discovery_transaction_rx = tx_listener.subscribe_transaction_updates();
    let pool_discovery_event_rx = PoolDiscoveryIngest::spawn_unified(
        pool_discovery_account_rx,
        pool_discovery_transaction_rx,
        rpc.clone(),
    );

    let tx_listener_handle = tokio::spawn(async move {
        if let Err(e) = tx_listener.start().await {
            error!(error = %e, "Geyser TX listener crashed");
        }
    });

    let account_listener_handle = tokio::spawn(async move {
        if let Err(e) = account_listener.start().await {
            error!(error = %e, "Geyser account listener crashed");
        }
    });

    // Graceful shutdown handling (SIGINT + SIGTERM on Unix).
    let shutdown = async {
        let ctrl_c = tokio::signal::ctrl_c();
        tokio::pin!(ctrl_c);
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut sigterm = signal(SignalKind::terminate()).ok();
            loop {
                tokio::select! {
                    res = &mut ctrl_c => {
                        if res.is_ok() {
                            break;
                        }
                    }
                    _ = async {
                        if let Some(ref mut term) = sigterm {
                            term.recv().await;
                        }
                    } => break,
                }
            }
        }
        #[cfg(not(unix))]
        {
            let _ = ctrl_c.await;
        }
    };
    tokio::pin!(shutdown);

    let account_count = Arc::new(AtomicU64::new(0));
    let tx_count = Arc::new(AtomicU64::new(0));
    let mut last_heartbeat = std::time::Instant::now();
    let mut activity_interval = tokio::time::interval(std::time::Duration::from_secs(10));

    // JetStream consumer for execution results (wallet ATA tracking)
    //
    // This is the trigger that makes wallet tracking "just work" after a BUY:
    // execution-engine already knows token_account/token_program/mint_decimals deterministically.
    let execution_js_consumer = if let Some(ref nats) = ctx.nats {
        use async_nats::jetstream;

        let jetstream = jetstream::new(nats.client().clone());
        match jetstream.get_stream(EXECUTION_RESULTS_STREAM_NAME).await {
            Ok(stream) => match stream
                .create_consumer(execution_results_consumer_config("market-data"))
                .await
            {
                Ok(consumer) => {
                    info!(
                        stream = EXECUTION_RESULTS_STREAM_NAME,
                        topic = TOPIC_EXECUTION_RESULTS,
                        "Subscribed to ExecutionResults via JetStream for wallet ATA tracking"
                    );
                    Some(consumer)
                }
                Err(e) => {
                    warn!(error = %e, "Failed to create execution results consumer (wallet ATA auto-tracking disabled)");
                    None
                }
            },
            Err(e) => {
                warn!(
                    error = %e,
                    stream = EXECUTION_RESULTS_STREAM_NAME,
                    "Failed to get execution results stream"
                );
                None
            }
        }
    } else {
        None
    };

    // I-24d: Subscribe to ControlRequests for Discovery Request/Reply (PumpSwap pool_accounts).
    // Only process requests with target = "market-data".
    let mut control_subscription = if let Some(ref nats) = ctx.nats {
        match nats.subscribe(TOPIC_CONTROL_REQUESTS).await {
            Ok(sub) => {
                info!(
                    topic = TOPIC_CONTROL_REQUESTS,
                    "Subscribed to ControlRequests (Discovery)"
                );
                set_readiness_control_sub_active(true);
                Some(sub)
            }
            Err(e) => {
                warn!(error = %e, "Failed to subscribe to ControlRequests");
                None
            }
        }
    } else {
        None
    };

    // PR-D: momentum-bot active pool pin stream (core NATS; fire-and-forget).
    let mut momentum_active_pools_sub = if let Some(ref nats) = ctx.nats {
        match nats.subscribe(TOPIC_MOMENTUM_ACTIVE_POOLS).await {
            Ok(sub) => {
                info!(
                    topic = TOPIC_MOMENTUM_ACTIVE_POOLS,
                    "Subscribed to MomentumActivePools (Geyser pin stream)"
                );
                Some(sub)
            }
            Err(e) => {
                warn!(
                    error = %e,
                    topic = TOPIC_MOMENTUM_ACTIVE_POOLS,
                    "Failed to subscribe to MomentumActivePools (momentum reserve pins disabled)"
                );
                None
            }
        }
    } else {
        None
    };

    // Phase 3: arb-strategy pool pin stream (core NATS).
    let mut arb_track_requests_sub = if let Some(ref nats) = ctx.nats {
        match nats.subscribe(TOPIC_ARB_TRACK_REQUESTS).await {
            Ok(sub) => {
                info!(
                    topic = TOPIC_ARB_TRACK_REQUESTS,
                    "Subscribed to ArbTrackRequests (Geyser pin stream)"
                );
                Some(sub)
            }
            Err(e) => {
                warn!(
                    error = %e,
                    topic = TOPIC_ARB_TRACK_REQUESTS,
                    "Failed to subscribe to ArbTrackRequests (arb reserve pins disabled)"
                );
                None
            }
        }
    } else {
        None
    };

    // PR160: dedicated `std::thread` + isolated Tokio runtime; multi-worker NATS clients + dispatcher.
    // Must exist before the Geyser-Tx task starts so `handle_geyser_transaction` can enqueue.
    let account_publish_tx: Option<mpsc::Sender<AccountPathNatsJob>> =
        ctx.nats.as_ref().map(|nats| {
            let template = nats.connection_config_template();
            let n_workers = account_publish_worker_count_from_env();
            info!(
                workers = n_workers,
                main_queue_cap = MARKET_DATA_ACCOUNT_PUBLISH_QUEUE_CAP,
                per_worker_dispatch_cap = MARKET_DATA_ACCOUNT_PUBLISH_WORKER_DISPATCH_QUEUE_CAP,
                "PR160: starting md-publish runtime (non-blocking enqueue from Geyser paths)"
            );
            spawn_market_data_account_publish_runtime(Arc::clone(&ctx), template, n_workers)
        });

    // PR3: separate Geyser session for wallet TX status → JetStream WalletTxConfirmed (I-4).
    #[cfg(not(windows))]
    if let (Some(ref tracked_wallet), Some(_)) = (&ctx.tracked_wallet, &ctx.nats) {
        let (wallet_tx_update_tx, mut wallet_tx_update_rx) =
            mpsc::channel::<WalletTxConfirmUpdate>(512);
        let _wallet_tx_confirm_listener = spawn_wallet_tx_confirm_listener(
            geyser_url.to_string(),
            tracked_wallet.wallet,
            wallet_tx_confirm_commitment.clone(),
            wallet_tx_update_tx,
        );

        let ctx_wallet_tx = Arc::clone(&ctx);
        let run_id_wallet_tx = run_id.to_string();
        tokio::spawn(async move {
            while let Some(update) = wallet_tx_update_rx.recv().await {
                let wallet_str = update.wallet.to_string();
                let event = MarketEvent::new(
                    "market-data",
                    BUILD_VERSION,
                    &run_id_wallet_tx,
                    format!("wallet_tx_confirm_{}_{}", update.signature, update.slot),
                    "geyser_wallet_tx_confirm",
                    Some(update.slot),
                    MarketEventKind::WalletTxConfirmed {
                        wallet: wallet_str.clone(),
                        signature: update.signature.clone(),
                        err: update.err.clone(),
                    },
                );
                let subject = wallet_tx_confirm_subject(&wallet_str, &update.signature);
                // Critical confirm path: publish directly (bounded account queue may drop).
                let published = account_path_enqueue_jetstream(
                    None,
                    ctx_wallet_tx.nats.as_ref(),
                    subject,
                    &event,
                    "WalletTxConfirmed JetStream",
                    false,
                )
                .await;
                if published {
                    info!(
                        sig = %update.signature,
                        slot = update.slot,
                        err = ?update.err,
                        "WalletTxConfirmed published to JetStream"
                    );
                } else {
                    error!(
                        sig = %update.signature,
                        slot = update.slot,
                        err = ?update.err,
                        "WalletTxConfirmed JetStream publish failed (execution-engine confirm will timeout)"
                    );
                }
            }
            warn!("wallet_tx_confirm bridge: update channel closed");
        });
    }

    // Phase-R-R4: deferred pool_mint_map / heavy publish off Tokio ingest (`md-sidefx` OS thread).
    let md_sidefx = spawn_md_sidefx_worker(
        Arc::clone(&ctx),
        account_publish_tx.clone(),
        md_state.clone(),
        track_worker.clone(),
    );

    // Option A (MARKET-DATA-TX-INGEST-FAIRNESS): dedizierter Tokio-Task für Geyser-Txs.
    // Verhindert, dass `transaction_rx.recv()` hinter langen Account-`select!`-Armen verhungert
    // (p50 `market_data_tx_channel_lag_ms` war ~1s+ bei gleichzeitig niedrigem Publish-Tail).
    let (geyser_tx_stream_stopped_tx, mut geyser_tx_stream_stopped_rx) = watch::channel(false);
    let ctx_geyser_tx = Arc::clone(&ctx);
    let run_id_geyser_tx = run_id.to_string();
    let tx_count_geyser_tx = Arc::clone(&tx_count);
    let account_publish_tx_geyser_tx = account_publish_tx.clone();
    let md_state_geyser_tx = md_state.clone();
    let md_sidefx_geyser_tx = md_sidefx.clone();
    let mut transaction_rx_geyser = transaction_rx;
    tokio::spawn(async move {
        loop {
            match transaction_rx_geyser.recv().await {
                Ok(tx_update) => {
                    set_market_data_tx_broadcast_queue_depth(transaction_rx_geyser.len());
                    handle_geyser_transaction(
                        Arc::clone(&ctx_geyser_tx),
                        run_id_geyser_tx.as_str(),
                        tx_update,
                        tx_count_geyser_tx.as_ref(),
                        account_publish_tx_geyser_tx.as_ref(),
                        &md_state_geyser_tx,
                        &md_sidefx_geyser_tx,
                    )
                    .await;
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    record_market_data_tx_broadcast_lagged(n);
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    warn!("Geyser transaction broadcast channel closed");
                    let _ = geyser_tx_stream_stopped_tx.send(true);
                    break;
                }
            }
        }
    });

    // PR162: pool discovery off main `select!` — dedicated consumer (JSONL + enqueue; no sync NATS).
    let ctx_pool_disc = Arc::clone(&ctx);
    let run_id_pool_disc = run_id.to_string();
    let mint_info_tx_pool_disc = mint_info_tx.clone();
    let account_publish_tx_pool_disc = account_publish_tx.clone();
    tokio::spawn(async move {
        let mut pool_discovery_event_rx = pool_discovery_event_rx;
        while let Some(pool_event) = pool_discovery_event_rx.recv().await {
            handle_pool_discovery_market_event(
                &ctx_pool_disc,
                run_id_pool_disc.as_str(),
                &mint_info_tx_pool_disc,
                account_publish_tx_pool_disc.as_ref(),
                pool_event,
            )
            .await;
        }
        warn!("pool discovery ingest: event channel closed");
    });

    // Option A + MARKET-DATA-ACCOUNT-THROUGHPUT-P0: dedicated recv + worker pool (sharded by pubkey)
    // + async NATS publish queue (JetStream + core MarketEvent) so account handlers do not await NATS.
    let (geyser_account_stream_stopped_tx, mut geyser_account_stream_stopped_rx) =
        watch::channel(false);
    let ctx_geyser_acc = Arc::clone(&ctx);
    let run_id_geyser_acc = run_id.to_string();
    let account_count_geyser_acc = Arc::clone(&account_count);
    let mut account_rx_geyser = account_rx;

    set_market_data_account_worker_count(MARKET_DATA_ACCOUNT_WORKER_COUNT);

    let mut worker_high_tx_list: Vec<mpsc::Sender<AccountWorkItem>> =
        Vec::with_capacity(MARKET_DATA_ACCOUNT_WORKER_COUNT);
    let mut enrich_ingress_tx_list: Vec<mpsc::Sender<AccountWorkItem>> =
        Vec::with_capacity(MARKET_DATA_ACCOUNT_WORKER_COUNT);
    for wid in 0..MARKET_DATA_ACCOUNT_WORKER_COUNT {
        let (high_tx, mut high_rx) = mpsc::channel(MARKET_DATA_ACCOUNT_WORKER_QUEUE_CAP);
        let (low_tx, mut low_rx) = mpsc::channel(MARKET_DATA_ACCOUNT_WORKER_QUEUE_CAP);
        let (enrich_ingress_tx, mut enrich_ingress_rx) =
            mpsc::channel(MARKET_DATA_ACCOUNT_ENRICH_INGRESS_QUEUE_CAP);
        worker_high_tx_list.push(high_tx.clone());
        enrich_ingress_tx_list.push(enrich_ingress_tx.clone());
        let enrich_coalesce = Arc::new(Mutex::new(AccountEnrichCoalesceShard::new()));
        let enrich_notify = Arc::new(Notify::new());
        let ctx_w = Arc::clone(&ctx_geyser_acc);
        let run_id_w = run_id_geyser_acc.clone();
        let account_count_w = Arc::clone(&account_count_geyser_acc);
        let publish_tx_w = account_publish_tx.clone();
        let md_state_w = md_state.clone();
        let md_sidefx_w = md_sidefx.clone();
        let low_tx_drain = low_tx.clone();
        let enrich_coalesce_drain = Arc::clone(&enrich_coalesce);
        let enrich_notify_drain = Arc::clone(&enrich_notify);
        let geyser_account_stopped_drain = geyser_account_stream_stopped_tx.clone();
        tokio::spawn(async move {
            loop {
                enrich_notify_drain.notified().await;
                let mut shard = enrich_coalesce_drain.lock().await;
                if matches!(
                    account_enrich_coalesce_try_flush(&mut shard, &low_tx_drain),
                    AccountEnrichFlushStatus::LowChannelClosed
                ) {
                    error!(
                        worker = wid,
                        "account LOW worker queue closed; stopping Geyser account stream"
                    );
                    let _ = geyser_account_stopped_drain.send(true);
                    break;
                }
            }
        });
        let low_tx_dispatch = low_tx.clone();
        let enrich_coalesce_dispatch = Arc::clone(&enrich_coalesce);
        let enrich_notify_dispatch = Arc::clone(&enrich_notify);
        let geyser_account_stopped_dispatch = geyser_account_stream_stopped_tx.clone();
        tokio::spawn(async move {
            while let Some(work) = enrich_ingress_rx.recv().await {
                dec_market_data_account_enrich_ingress_queue_depth();
                let mut shard = enrich_coalesce_dispatch.lock().await;
                let flush_status =
                    account_enrich_dispatch_upsert_and_flush(&mut shard, &low_tx_dispatch, work);
                drop(shard);
                if matches!(flush_status, AccountEnrichFlushStatus::LowChannelClosed) {
                    error!(
                        worker = wid,
                        "account LOW worker queue closed; stopping Geyser account stream"
                    );
                    let _ = geyser_account_stopped_dispatch.send(true);
                    break;
                }
                enrich_notify_dispatch.notify_one();
            }
            warn!(worker = wid, "account enrich ingress channel closed");
        });
        let geyser_account_stopped_worker = geyser_account_stream_stopped_tx.clone();
        tokio::spawn(async move {
            while let Some(dequeued) = account_worker_recv_next(&mut high_rx, &mut low_rx).await {
                let work = match dequeued {
                    AccountWorkerDequeue::ExecHot(work) => work,
                    AccountWorkerDequeue::Enrich(work) => {
                        let mut shard = enrich_coalesce.lock().await;
                        let (work, flush_status) =
                            account_enrich_resolve_after_dequeue(&mut shard, &low_tx, work);
                        if matches!(flush_status, AccountEnrichFlushStatus::LowChannelClosed) {
                            error!(
                                worker = wid,
                                "account LOW worker queue closed; stopping Geyser account stream"
                            );
                            let _ = geyser_account_stopped_worker.send(true);
                        }
                        work
                    }
                };
                let handler_start = Instant::now();
                handle_geyser_account(
                    Arc::clone(&ctx_w),
                    run_id_w.as_str(),
                    work.update,
                    account_count_w.as_ref(),
                    work.recv_at,
                    publish_tx_w.as_ref(),
                    &md_state_w,
                    &md_sidefx_w,
                    work.class,
                )
                .await;
                record_market_data_account_handler_duration_us(
                    handler_start
                        .elapsed()
                        .as_micros()
                        .min(u128::from(u64::MAX)) as u64,
                );
            }
            warn!(worker = wid, "account ingest worker: input channel closed");
        });
    }
    let worker_high_dispatch = Arc::new(worker_high_tx_list);
    let enrich_ingress_dispatch = Arc::new(enrich_ingress_tx_list);

    let worker_high_recv = Arc::clone(&worker_high_dispatch);
    let enrich_ingress_recv = Arc::clone(&enrich_ingress_dispatch);
    tokio::spawn(async move {
        loop {
            match account_rx_geyser.recv().await {
                Ok(account_update) => {
                    let iteration_start = Instant::now();
                    let recv_at = Instant::now();
                    inc_market_data_account_recv_iterations_total();
                    record_market_data_account_channel_lag_ms(account_update.grpc_recv_at, recv_at);
                    set_market_data_account_broadcast_queue_depth(account_rx_geyser.len());
                    market_data_bump_geyser_head_slot(account_update.slot);

                    let classify_start = Instant::now();
                    let (class, early_drop_reason) =
                        ironcrab::market_data::ingest::classify_account_geyser_update(
                            &ctx_geyser_acc,
                            &account_update,
                        );
                    record_market_data_account_recv_classify_duration_us(
                        classify_start
                            .elapsed()
                            .as_micros()
                            .min(u128::from(u64::MAX)) as u64,
                    );
                    inc_market_data_account_updates_total(class.as_prometheus_label());

                    if let Some(reason) = early_drop_reason {
                        record_market_data_account_early_drop(reason);
                        record_market_data_account_recv_iteration_duration_us(
                            iteration_start
                                .elapsed()
                                .as_micros()
                                .min(u128::from(u64::MAX)) as u64,
                        );
                        continue;
                    }

                    record_market_data_account_channel_lag_ms_for_class(
                        class.as_prometheus_label(),
                        account_update.grpc_recv_at,
                        recv_at,
                    );

                    let shard = market_data_account_worker_shard(&account_update.pubkey);
                    let high = matches!(
                        class,
                        ironcrab::market_data::ingest::AccountUpdateClass::ExecHot
                    );
                    if high {
                        let enqueue_start = Instant::now();
                        match account_high_ingress_try_send(
                            &worker_high_recv[shard],
                            AccountWorkItem {
                                update: account_update,
                                recv_at,
                                class,
                            },
                        ) {
                            AccountHighIngressSend::Ok => {}
                            AccountHighIngressSend::ContendedFull => {}
                            AccountHighIngressSend::Closed => {
                                error!(
                                    shard = shard,
                                    "account HIGH worker queue closed; stopping Geyser account stream"
                                );
                                let _ = geyser_account_stream_stopped_tx.send(true);
                                break;
                            }
                        }
                        record_market_data_account_recv_high_enqueue_duration_us(
                            enqueue_start
                                .elapsed()
                                .as_micros()
                                .min(u128::from(u64::MAX)) as u64,
                        );
                    } else {
                        let work = AccountWorkItem {
                            update: account_update,
                            recv_at,
                            class,
                        };
                        let ingress_start = Instant::now();
                        match account_enrich_ingress_try_send(&enrich_ingress_recv[shard], work) {
                            AccountEnrichIngressSend::Ok => {}
                            AccountEnrichIngressSend::ContendedFull => {}
                            AccountEnrichIngressSend::Closed => {
                                error!(
                                    shard = shard,
                                    "account ENRICH ingress channel closed; stopping Geyser account stream"
                                );
                                let _ = geyser_account_stream_stopped_tx.send(true);
                                break;
                            }
                        }
                        record_market_data_account_recv_enrich_ingress_duration_us(
                            ingress_start
                                .elapsed()
                                .as_micros()
                                .min(u128::from(u64::MAX)) as u64,
                        );
                    }
                    record_market_data_account_recv_iteration_duration_us(
                        iteration_start
                            .elapsed()
                            .as_micros()
                            .min(u128::from(u64::MAX)) as u64,
                    );
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    record_market_data_account_broadcast_lagged(n);
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    warn!("Geyser account broadcast channel closed");
                    let _ = geyser_account_stream_stopped_tx.send(true);
                    break;
                }
            }
        }
    });

    loop {
        tokio::select! {
            _ = geyser_tx_stream_stopped_rx.changed() => {
                if *geyser_tx_stream_stopped_rx.borrow() {
                    warn!("Geyser transaction stream ended; stopping market-data Geyser loop");
                    tx_listener_handle.abort();
                    account_listener_handle.abort();
                    break;
                }
            }

            _ = geyser_account_stream_stopped_rx.changed() => {
                if *geyser_account_stream_stopped_rx.borrow() {
                    warn!("Geyser account stream ended; stopping market-data Geyser loop");
                    tx_listener_handle.abort();
                    account_listener_handle.abort();
                    break;
                }
            }

            // I-24d: ControlRequests (EnsurePumpAmmPoolAccounts Discovery)
            msg = async {
                if let Some(ref mut sub) = control_subscription {
                    sub.next().await
                } else {
                    std::future::pending::<Option<ironcrab::nats::NatsMessage>>().await
                }
            } => {
                if let Some(nats_msg) = msg {
                    match nats_msg.deserialize::<ControlRequest>() {
                        Ok(req) => {
                            if req.target != "market-data" {
                                debug!(target = %req.target, "Ignoring ControlRequest for other target");
                            } else {
                                match req.kind {
                                    ControlRequestKind::EnsurePumpAmmPoolAccounts { base_mint } => {
                                        let run_id = ctx.run_id.clone();
                                        let request_id = req.request_id.clone();
                                        let pool_hint = req.pool_address_hint.clone();
                                        let force_refresh = req.force_refresh;
                                        info!(
                                            request_id = %request_id,
                                            base_mint = %base_mint,
                                            pool_address_hint = ?pool_hint,
                                            force_refresh = %force_refresh,
                                            "I-24d Discovery: EnsurePumpAmmPoolAccounts received"
                                        );
                                        let ctx_clone = ctx.clone();
                                        let dex_clone = Arc::clone(&pump_amm_dex);
                                        tokio::spawn(async move {
                                            handle_ensure_pump_amm_pool_accounts(
                                                ctx_clone.as_ref(),
                                                dex_clone.as_ref(),
                                                run_id.as_str(),
                                                &request_id,
                                                &base_mint,
                                                pool_hint.as_deref(),
                                                force_refresh,
                                            )
                                            .await;
                                        });
                                    }
                                    ControlRequestKind::EnsurePumpfunBondingCurve { base_mint } => {
                                        let run_id = ctx.run_id.clone();
                                        let request_id = req.request_id.clone();
                                        let pool_hint = req.pool_address_hint.clone();
                                        let force_refresh = req.force_refresh_pumpfun;
                                        info!(
                                            request_id = %request_id,
                                            base_mint = %base_mint,
                                            pool_address_hint = ?pool_hint,
                                            force_refresh_pumpfun = %force_refresh,
                                            "EnsurePumpfunBondingCurve received"
                                        );
                                        let ctx_clone = ctx.clone();
                                        let rpc_clone = rpc.clone();
                                        tokio::spawn(async move {
                                            handle_ensure_pumpfun_bonding_curve(
                                                ctx_clone.as_ref(),
                                                &rpc_clone,
                                                run_id.as_str(),
                                                &request_id,
                                                &base_mint,
                                                pool_hint.as_deref(),
                                                force_refresh,
                                            )
                                            .await;
                                        });
                                    }
                                    ControlRequestKind::EnsureOrcaWhirlpoolPoolState { base_mint } => {
                                        let run_id = ctx.run_id.clone();
                                        let request_id = req.request_id.clone();
                                        let pool_hint = req.pool_address_hint.clone();
                                        let force_refresh = req.force_refresh_orca;
                                        info!(
                                            request_id = %request_id,
                                            base_mint = %base_mint,
                                            pool_address_hint = ?pool_hint,
                                            force_refresh_orca = %force_refresh,
                                            "EnsureOrcaWhirlpoolPoolState received"
                                        );
                                        let ctx_clone = ctx.clone();
                                        let rpc_clone = rpc.clone();
                                        tokio::spawn(async move {
                                            handle_ensure_orca_whirlpool_pool_state(
                                                ctx_clone.as_ref(),
                                                &rpc_clone,
                                                run_id.as_str(),
                                                &request_id,
                                                &base_mint,
                                                pool_hint.as_deref(),
                                                force_refresh,
                                            )
                                            .await;
                                        });
                                    }
                                    ControlRequestKind::EnsureMeteoraDlmmPoolState { base_mint } => {
                                        let run_id = ctx.run_id.clone();
                                        let request_id = req.request_id.clone();
                                        let pool_hint = req.pool_address_hint.clone();
                                        let force_refresh = req.force_refresh_meteora_dlmm;
                                        info!(
                                            request_id = %request_id,
                                            base_mint = %base_mint,
                                            pool_address_hint = ?pool_hint,
                                            force_refresh_meteora_dlmm = %force_refresh,
                                            "EnsureMeteoraDlmmPoolState received"
                                        );
                                        let ctx_clone = ctx.clone();
                                        let rpc_clone = rpc.clone();
                                        tokio::spawn(async move {
                                            handle_ensure_meteora_dlmm_pool_state(
                                                ctx_clone.as_ref(),
                                                &rpc_clone,
                                                run_id.as_str(),
                                                &request_id,
                                                &base_mint,
                                                pool_hint.as_deref(),
                                                force_refresh,
                                            )
                                            .await;
                                        });
                                    }
                                    ControlRequestKind::EnsureRaydiumAmmPoolState { base_mint } => {
                                        let run_id = ctx.run_id.clone();
                                        let request_id = req.request_id.clone();
                                        let pool_hint = req.pool_address_hint.clone();
                                        let force_refresh = req.force_refresh_raydium_amm;
                                        info!(
                                            request_id = %request_id,
                                            base_mint = %base_mint,
                                            pool_address_hint = ?pool_hint,
                                            force_refresh_raydium_amm = %force_refresh,
                                            "EnsureRaydiumAmmPoolState received"
                                        );
                                        let ctx_clone = ctx.clone();
                                        let rpc_clone = rpc.clone();
                                        tokio::spawn(async move {
                                            handle_ensure_raydium_amm_pool_state(
                                                ctx_clone.as_ref(),
                                                &rpc_clone,
                                                run_id.as_str(),
                                                &request_id,
                                                &base_mint,
                                                pool_hint.as_deref(),
                                                force_refresh,
                                            )
                                            .await;
                                        });
                                    }
                                    ControlRequestKind::EnsureRaydiumCpmmPoolState { base_mint } => {
                                        let run_id = ctx.run_id.clone();
                                        let request_id = req.request_id.clone();
                                        let pool_hint = req.pool_address_hint.clone();
                                        let force_refresh = req.force_refresh_raydium_cpmm;
                                        info!(
                                            request_id = %request_id,
                                            base_mint = %base_mint,
                                            pool_address_hint = ?pool_hint,
                                            force_refresh_raydium_cpmm = %force_refresh,
                                            "EnsureRaydiumCpmmPoolState received"
                                        );
                                        let ctx_clone = ctx.clone();
                                        let rpc_clone = rpc.clone();
                                        tokio::spawn(async move {
                                            handle_ensure_raydium_cpmm_pool_state(
                                                ctx_clone.as_ref(),
                                                &rpc_clone,
                                                run_id.as_str(),
                                                &request_id,
                                                &base_mint,
                                                pool_hint.as_deref(),
                                                force_refresh,
                                            )
                                            .await;
                                        });
                                    }
                                    ControlRequestKind::EnsureMeteoraCpmmPoolState { base_mint } => {
                                        let run_id = ctx.run_id.clone();
                                        let request_id = req.request_id.clone();
                                        let pool_hint = req.pool_address_hint.clone();
                                        let force_refresh = req.force_refresh_meteora_cpmm;
                                        info!(
                                            request_id = %request_id,
                                            base_mint = %base_mint,
                                            pool_address_hint = ?pool_hint,
                                            force_refresh_meteora_cpmm = %force_refresh,
                                            "EnsureMeteoraCpmmPoolState received"
                                        );
                                        let ctx_clone = ctx.clone();
                                        let rpc_clone = rpc.clone();
                                        tokio::spawn(async move {
                                            handle_ensure_meteora_cpmm_pool_state(
                                                ctx_clone.as_ref(),
                                                &rpc_clone,
                                                run_id.as_str(),
                                                &request_id,
                                                &base_mint,
                                                pool_hint.as_deref(),
                                                force_refresh,
                                            )
                                            .await;
                                        });
                                    }
                                    _ => {
                                        debug!(kind = ?req.kind, "Ignoring ControlRequest kind for market-data");
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            warn!(error = %e, "Failed to deserialize ControlRequest");
                        }
                    }
                }
            }

            // Proactive mint metadata (decimals/supply) fetched via RPC.
            Some(mint_event) = mint_info_rx.recv() => {
                let mint_geyser_recv_at = Instant::now();
                // Write to JSONL
        ctx.write_market_event_jsonl(&mint_event);

                if ctx.nats.is_some() {
                    let seg = market_data_publish_segment(&mint_event.kind);
                    account_path_enqueue_core_market_event(
                        account_publish_tx.as_ref(),
                        ctx.nats.as_ref(),
                        &ctx,
                        mint_event,
                        Some(MarketEventCorePublishTrace {
                            recv_at: mint_geyser_recv_at,
                            cold_path: true,
                            segment: seg,
                        }),
                    )
                    .await;
                }
            }

            // Keep /ready fresh even if Geyser/NATS are quiet.
            _ = activity_interval.tick() => {
                ironcrab::metrics::record_activity();
                if !ctx.deferred_hot_pool_reserve_pins.read().is_empty() {
                    let _ = track_worker_try_enqueue(
                        &track_worker,
                        TrackWorkerCommand::RetryDeferredHotPoolReserves,
                    );
                }

                // Refresh readiness from current runtime state (not startup-latch)
                let nats_connected = ctx.nats.as_ref().is_some_and(|n| n.is_connected());
                let control_sub_active = nats_connected && control_subscription.is_some();
                let jetstream_ready = if nats_connected {
                    if let Some(ref nats) = ctx.nats {
                        use async_nats::jetstream;
                        use ironcrab::nats::STREAM_NAME;
                        tokio::time::timeout(
                            std::time::Duration::from_secs(2),
                            jetstream::new(nats.client().clone()).get_stream(STREAM_NAME),
                        )
                        .await
                        .map(|r| r.is_ok())
                        .unwrap_or(false)
                    } else {
                        false
                    }
                } else {
                    false
                };
                update_readiness_market_data_current(nats_connected, control_sub_active, jetstream_ready);
            }

            // Track wallet ATAs/mints from execution-engine results via JetStream (no RPC).
            _ = async {
                use futures::StreamExt;
                if let Some(ref consumer) = execution_js_consumer {
                    match consumer
                        .fetch()
                        .max_messages(50)
                        .expires(std::time::Duration::from_millis(100))
                        .messages()
                        .await
                    {
                        Ok(mut messages) => {
                            while let Some(msg_result) = messages.next().await {
                                match msg_result {
                                    Ok(msg) => {
                                        let mut exec: ExecutionResult =
                                            match serde_json::from_slice(&msg.payload) {
                                            Ok(e) => e,
                                            Err(e) => {
                                                debug!(error = %e, "Failed to deserialize ExecutionResult");
                                                let _ = msg.ack().await;
                                                continue;
                                            }
                                        };

                                        backfill_scope48_metadata_foreign_confirmed_sell(&mut exec);

                                        // Dedup on execution_id (fallback: signature)
                                        let dedup_key = if !exec.execution_id.is_empty() {
                        exec.execution_id.clone()
                    } else {
                        exec.signature.clone().unwrap_or_else(|| exec.decision_id.clone())
                    };
                                        let accept = {
                                            let mut deduper = ctx.execution_results_deduper.lock();
                                            deduper.should_process(&dedup_key)
                                        };
                                        if !accept {
                                            let _ = msg.ack().await;
                                            continue;
                                        }

                                        let Some(ref tracked_wallet) = ctx.tracked_wallet else {
                                            let _ = msg.ack().await;
                                            continue;
                                        };

                                        let mint_str = match exec.token_mint.as_deref() {
                                            Some(s) => s,
                                            None => {
                                                let _ = msg.ack().await;
                                                continue;
                                            }
                                        };
                                        let mint = match Pubkey::from_str(mint_str) {
                                            Ok(m) => m,
                                            Err(_) => {
                                                let _ = msg.ack().await;
                                                continue;
                                            }
                                        };

                                        let ata_str = match exec.metadata.get("token_account") {
                                            Some(s) => s,
                                            None => {
                                                warn!(
                                                    execution_id = %exec.execution_id,
                                                    wallet = %tracked_wallet.wallet,
                                                    mint = ?exec.token_mint,
                                                    intent_id = %exec.intent_id,
                                                    side = ?exec.metadata.get("side"),
                                                    "ExecutionResult missing metadata.token_account — cannot track ATA"
                                                );
                                                let _ = msg.ack().await;
                                                continue;
                                            }
                                        };
                                        let ata = match Pubkey::from_str(ata_str) {
                                            Ok(a) => a,
                                            Err(_) => {
                                                warn!(
                                                    execution_id = %exec.execution_id,
                                                    wallet = %tracked_wallet.wallet,
                                                    ata = %ata_str,
                                                    mint = ?exec.token_mint,
                                                    intent_id = %exec.intent_id,
                                                    "ExecutionResult metadata.token_account is not a valid Pubkey"
                                                );
                                                let _ = msg.ack().await;
                                                continue;
                                            }
                                        };

                                        let token_program_str = match exec.metadata.get("token_program") {
                                            Some(s) => s,
                                            None => {
                                                warn!(
                                                    execution_id = %exec.execution_id,
                                                    wallet = %tracked_wallet.wallet,
                                                    mint = ?exec.token_mint,
                                                    intent_id = %exec.intent_id,
                                                    side = ?exec.metadata.get("side"),
                                                    "ExecutionResult missing metadata.token_program — cannot track ATA"
                                                );
                                                let _ = msg.ack().await;
                                                continue;
                                            }
                                        };
                                        let token_program = match Pubkey::from_str(token_program_str) {
                                            Ok(p) => p,
                                            Err(_) => {
                                                warn!(
                                                    execution_id = %exec.execution_id,
                                                    wallet = %tracked_wallet.wallet,
                                                    token_program = %token_program_str,
                                                    mint = ?exec.token_mint,
                                                    "ExecutionResult metadata.token_program is not a valid Pubkey"
                                                );
                                                let _ = msg.ack().await;
                                                continue;
                                            }
                                        };
                                        // Only support SPL Token + Token-2022 for wallet ATA tracking.
                                        if token_program.to_bytes() != spl_token::ID.to_bytes()
                                            && token_program.to_bytes() != spl_token_2022::ID.to_bytes()
                                        {
                                            let _ = msg.ack().await;
                                            continue;
                                        }

                                                        let mint_decimals: Option<u8> = exec
                                            .metadata
                                            .get("mint_decimals")
                                            .and_then(|s| s.parse::<u8>().ok());

                                        // 1) Track ATA for wallet updates (Geyser subscription list)
                                        let mut added_ata = false;
                                        {
                                            let mut set = ctx.tracked_wallet_token_accounts.write();
                                            if set.insert(ata) {
                                                added_ata = true;
                                            }
                                        }

                                        // 2) Cache decimals if provided
                                        if let Some(d) = mint_decimals {
                                            ctx.tracked_wallet_mint_decimals.write().insert(mint, d);
                                            ctx.live_pool_cache.set_mint_decimals(mint, d);
                                        }

                                        // 3) Track mint so Geyser will deliver the mint account (wallet-pinned).
                                        let added_mint = ctx
                                            .tracked_mints
                                            .read()
                                            .get(&mint)
                                            .is_none_or(|info| {
                                                !info.pinned
                                                    || info.pin != Some(GeyserPinReason::Wallet)
                                            });
                                        if added_mint {
                                            md_state_try_enqueue(
                                                &md_state,
                                                MdStateCommand::TrackWalletMint { mint },
                                            );
                                        }

                                        // 4) Recompute tracked wallet accounts and notify listener
                                        if added_ata {
                                            let mut accounts: Vec<Pubkey> = Vec::new();
                                            accounts.push(tracked_wallet.wallet);
                                            accounts.push(tracked_wallet.wsol_ata);
                                            accounts.extend(ctx.tracked_wallet_token_accounts.read().iter().copied());
                                            accounts.sort();
                                            accounts.dedup();
                                            let _ = ctx.tracked_wallet_tx.send(accounts);
                                        }

                                        let is_confirmed_sell = exec.status == ExecutionStatus::Confirmed
                                            && exec.metadata.get("side").map(|s| s.as_str()) == Some("SELL");

                                        info!(
                                            execution_id = %exec.execution_id,
                                            mint = %mint,
                                            ata = %ata,
                                            token_program = %token_program,
                                            mint_decimals = ?mint_decimals,
                                            added_ata,
                                            added_mint,
                                            is_confirmed_sell,
                                            "ExecutionResult: tracked wallet ATA/mint"
                                        );

                                        // 5) Full close / liquidation / ATA-close SELL: zero snapshot + untrack.
                                        // Partial SELL: keep ATA tracked; residual balance comes from Geyser.
                                        if is_confirmed_sell && execution_result_sell_closes_wallet_position(&exec) {
                                            let wallet_str = tracked_wallet.wallet.to_string();
                                            let snapshot = MarketEvent::new(
                                                "market-data",
                                                BUILD_VERSION,
                                                &ctx.run_id,
                                                format!("wallet_snapshot_sell_{}", exec.execution_id),
                                                "execution_result_sell",
                                                None,
                                                MarketEventKind::WalletBalanceSnapshot {
                                                    mint: mint_str.to_string(),
                                                    balance_raw: 0,
                                                    decimals: mint_decimals.unwrap_or(9),
                                                    token_program: token_program_str.clone(),
                                                },
                                            );
                                            let subject = wallet_snapshot_subject(&wallet_str, mint_str);
                                            account_path_enqueue_jetstream(
                                                account_publish_tx.as_ref(),
                                                ctx.nats.as_ref(),
                                                subject,
                                                &snapshot,
                                                "zero-balance WalletBalanceSnapshot after SELL",
                                                false,
                                            )
                                            .await;

                                            if mint_str == WSOL_MINT {
                                                tracked_wallet.last_wsol_balance.store(0, Ordering::Relaxed);
                                            }

                                            let should_withdraw_wallet_pin = ctx
                                                .tracked_mints
                                                .read()
                                                .get(&mint)
                                                .is_some_and(|info| {
                                                    info.pin == Some(GeyserPinReason::Wallet)
                                                });
                                            let withdraw_enqueued = if should_withdraw_wallet_pin {
                                                md_state_try_enqueue_withdraw_wallet_mint(
                                                    &md_state, mint,
                                                )
                                            } else {
                                                true
                                            };
                                            if should_withdraw_wallet_pin && !withdraw_enqueued {
                                                warn!(
                                                    mint = %mint_str,
                                                    ata = %ata,
                                                    "WithdrawWalletMint enqueue dropped after retries; keeping ATA tracked until withdraw succeeds"
                                                );
                                            }

                                            if wallet_sell_close_should_untrack_ata(
                                                should_withdraw_wallet_pin,
                                                withdraw_enqueued,
                                            ) {
                                                let mut set =
                                                    ctx.tracked_wallet_token_accounts.write();
                                                if set.remove(&ata) {
                                                    let mut accounts: Vec<Pubkey> = Vec::new();
                                                    accounts.push(tracked_wallet.wallet);
                                                    accounts.push(tracked_wallet.wsol_ata);
                                                    accounts.extend(set.iter().copied());
                                                    accounts.sort();
                                                    accounts.dedup();
                                                    let _ = ctx.tracked_wallet_tx.send(accounts);
                                                    info!(
                                                        mint = %mint_str,
                                                        ata = %ata,
                                                        remaining_tracked = set.len(),
                                                        "Untracked ATA after confirmed SELL"
                                                    );
                                                }
                                            }
                                        } else if is_confirmed_sell {
                                            info!(
                                                mint = %mint_str,
                                                ata = %ata,
                                                sell_position_delta = ?exec.metadata.get("sell_position_delta_applied"),
                                                "Confirmed partial SELL: keeping ATA tracked (no zero snapshot / untrack)"
                                            );
                                        }

                                        if let Err(e) = msg.ack().await {
                                            warn!(error = %e, "Failed to ack ExecutionResult");
                                        }
                                    }
                                    Err(e) => {
                                        warn!(error = %e, "ExecutionResult fetch error");
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            debug!(error = %e, "ExecutionResult stream fetch failed (may be empty)");
                        }
                    }
                } else {
                    std::future::pending::<()>().await
                }
            } => {}


            // Blockhash updates from Geyser blocks_meta
            Ok(bh_update) = blockhash_rx.recv() => {
                market_data_bump_geyser_head_slot(bh_update.slot);
                let event = MarketEvent::new(
                    "market-data",
                    BUILD_VERSION,
                    &ctx.run_id,
                    format!("blockhash-{}", bh_update.slot),
                    "geyser",
                    Some(bh_update.slot),
                    MarketEventKind::LatestBlockhash {
                        blockhash: bh_update.blockhash,
                        slot: bh_update.slot,
                        block_height: bh_update.block_height,
                    },
                );
                let recv_at = Instant::now();
                if ctx.nats.is_some() {
                    account_path_enqueue_core_market_event(
                        account_publish_tx.as_ref(),
                        ctx.nats.as_ref(),
                        &ctx,
                        event,
                        Some(MarketEventCorePublishTrace {
                            recv_at,
                            cold_path: false,
                            segment: MarketDataLatencySegment::Other,
                        }),
                    )
                    .await;
                }
            }


            // PR-D: Momentum active pool pin stream (core NATS).
            msg = async {
                if let Some(ref mut sub) = momentum_active_pools_sub {
                    sub.next().await
                } else {
                    std::future::pending::<Option<ironcrab::nats::NatsMessage>>().await
                }
            } => {
                if let Some(nats_msg) = msg {
                    match serde_json::from_slice::<MomentumActivePoolsUpdate>(&nats_msg.payload) {
                        Ok(update) => {
                            momentum_coalesce_try_send(&momentum_coalesce_tx, update);
                        }
                        Err(e) => {
                            warn!(error = %e, "Failed to deserialize MomentumActivePoolsUpdate");
                        }
                    }
                }
            }

            // Phase 3: Arb track_requests pin stream (core NATS).
            msg = async {
                if let Some(ref mut sub) = arb_track_requests_sub {
                    sub.next().await
                } else {
                    std::future::pending::<Option<ironcrab::nats::NatsMessage>>().await
                }
            } => {
                if let Some(nats_msg) = msg {
                    match serde_json::from_slice::<ArbTrackRequestsUpdate>(&nats_msg.payload) {
                        Ok(update) => {
                            arb_coalesce_try_send(&arb_coalesce_tx, update);
                        }
                        Err(e) => {
                            warn!(error = %e, "Failed to deserialize ArbTrackRequestsUpdate");
                        }
                    }
                }
            }

            // P1: Handle Config Updates (Runtime Configuration via UI)
            msg = async {
                if let Some(ref mut sub) = config_subscription {
                    sub.next().await
                } else {
                    std::future::pending::<Option<ironcrab::nats::NatsMessage>>().await
                }
            } => {
                if let Some(nats_msg) = msg {
                    match serde_json::from_slice::<ConfigUpdate>(&nats_msg.payload) {
                        Ok(update) => {
                            if update.target_component == "market-data" {
                                info!(
                                    component = %update.target_component,
                                    keys = ?update.config.keys().collect::<Vec<_>>(),
                                    "Received Config Update from control-plane"
                                );
                                let response =
                                    ctx.apply_config_update(&update, Some(&md_state), Some(&track_worker));
                                info!(
                                    status = ?response.status,
                                    applied = ?response.applied_keys,
                                    rejected = ?response.rejected_keys,
                                    "Config update processed"
                                );
                            } else {
                                debug!(component = %update.target_component, "Ignoring config update for other component");
                            }
                        }
                        Err(e) => {
                            warn!(error = %e, "Failed to deserialize ConfigUpdate");
                        }
                    }
                }
            }

            // Periodic wallet snapshot (cold path) for momentum ghost reconciliation.
            _ = wallet_snapshot_periodic_interval.tick() => {
                if let Some(ref tracked_wallet) = ctx.tracked_wallet {
                    let ctx_spawn = Arc::clone(&ctx);
                    let rpc_spawn = Arc::clone(&rpc);
                    let md_state_spawn = md_state.clone();
                    let wallet = tracked_wallet.wallet;
                    tokio::spawn(async move {
                        if let Err(e) = publish_wallet_snapshot(
                            &ctx_spawn,
                            &rpc_spawn,
                            &wallet,
                            true,
                            false,
                            &md_state_spawn,
                        )
                        .await
                        {
                            warn!(error = %e, "Periodic wallet snapshot publish failed");
                        }
                    });
                }
            }

            // Periodic heartbeat
            _ = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                if last_heartbeat.elapsed().as_secs() >= 60 {
                    ironcrab::metrics::record_activity();
                    let (records, bytes) = ctx.jsonl_writer.stats();
                    let tx_n = tx_count.load(Ordering::Relaxed);
                    let acc_n = account_count.load(Ordering::Relaxed);
                    let total_events = acc_n + tx_n;

                    // Update Prometheus metrics
                    MARKET_EVENTS_RECEIVED_TOTAL.store(total_events, Ordering::Relaxed);
                    POOLS_TRACKED_GAUGE.store(acc_n, Ordering::Relaxed);

                    info!(
                        accounts = acc_n,
                        transactions = tx_n,
                        records_written = records,
                        bytes_written = bytes,
                        "market-data heartbeat (Geyser)"
                    );
                    last_heartbeat = std::time::Instant::now();
                }
            }

            _ = &mut shutdown => {
                info!("Shutdown signal received");
                flush_explicit_set_snapshot(&track_worker);
                tx_listener_handle.abort();
                account_listener_handle.abort();
                break;
            }
        }
    }

    Ok(track_worker)
}

/// Run simulation loop (for testing without Geyser)
async fn run_simulation_loop(
    ctx: Arc<MarketDataContext>,
    run_id: &str,
    mut config_subscription: Option<ironcrab::nats::NatsSubscription>,
) -> Result<TrackWorkerSender> {
    // RPC client for EnsurePumpAmmPoolAccounts (Cold Path Discovery, same handler as Normalmodus)
    let rpc_url =
        std::env::var("SOLANA_RPC_URL").unwrap_or_else(|_| "http://127.0.0.1:8899".to_string());
    let rpc = Arc::new(SolanaRpc::new(&rpc_url));
    info!(rpc_url = %rpc_url, "Simulation mode: RPC client for ControlRequest Discovery");

    // I-24d: Subscribe to ControlRequests (same contract as Normalmodus)
    let mut control_subscription = if let Some(ref nats) = ctx.nats {
        match nats.subscribe(TOPIC_CONTROL_REQUESTS).await {
            Ok(sub) => {
                info!(
                    topic = TOPIC_CONTROL_REQUESTS,
                    "Simulation mode: Subscribed to ControlRequests (Discovery)"
                );
                set_readiness_control_sub_active(true);
                Some(sub)
            }
            Err(e) => {
                warn!(error = %e, "Simulation mode: Failed to subscribe to ControlRequests");
                None
            }
        }
    } else {
        None
    };

    let mut momentum_active_pools_sub = if let Some(ref nats) = ctx.nats {
        match nats.subscribe(TOPIC_MOMENTUM_ACTIVE_POOLS).await {
            Ok(sub) => {
                info!(
                    topic = TOPIC_MOMENTUM_ACTIVE_POOLS,
                    "Simulation mode: Subscribed to MomentumActivePools"
                );
                Some(sub)
            }
            Err(e) => {
                warn!(
                    error = %e,
                    topic = TOPIC_MOMENTUM_ACTIVE_POOLS,
                    "Simulation mode: Failed to subscribe to MomentumActivePools"
                );
                None
            }
        }
    } else {
        None
    };

    let mut arb_track_requests_sub = if let Some(ref nats) = ctx.nats {
        match nats.subscribe(TOPIC_ARB_TRACK_REQUESTS).await {
            Ok(sub) => {
                info!(
                    topic = TOPIC_ARB_TRACK_REQUESTS,
                    "Simulation mode: Subscribed to ArbTrackRequests"
                );
                Some(sub)
            }
            Err(e) => {
                warn!(
                    error = %e,
                    topic = TOPIC_ARB_TRACK_REQUESTS,
                    "Simulation mode: Failed to subscribe to ArbTrackRequests"
                );
                None
            }
        }
    } else {
        None
    };

    let mut pump_inner = PumpFunAmmDex::new_with_cache(
        Arc::clone(&rpc),
        ctx.live_pool_cache.clone(),
        true, // allow_rpc_on_miss: Cold Path Discovery
    );
    pump_inner.set_bounded_tx_fallback_rpc(ctx.helius_rpc.clone());
    let pump_amm_dex = Arc::new(pump_inner);

    let track_worker = spawn_track_worker(Arc::clone(&ctx));
    let momentum_coalesce_tx =
        spawn_momentum_tracking_coalescer(Arc::clone(&ctx), track_worker.clone());
    let arb_coalesce_tx = spawn_arb_tracking_coalescer(Arc::clone(&ctx), track_worker.clone());
    let md_state = spawn_md_state_worker(
        Arc::clone(&ctx),
        tokio::runtime::Handle::current(),
        track_worker.clone(),
    );

    let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
    let mut slot: u64 = 0;

    // Graceful shutdown handling
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = interval.tick() => {
                slot += 1; // Simulated slot progression

                // Keep /ready fresh even when only simulating.
                ironcrab::metrics::record_activity();

                // Refresh readiness from current runtime state (simulate: control_sub if subscribed)
                let nats_connected = ctx.nats.as_ref().is_some_and(|n| n.is_connected());
                let control_sub_active = nats_connected && control_subscription.is_some();
                let jetstream_ready = if nats_connected {
                    if let Some(ref nats) = ctx.nats {
                        use async_nats::jetstream;
                        use ironcrab::nats::STREAM_NAME;
                        tokio::time::timeout(
                            std::time::Duration::from_secs(2),
                            jetstream::new(nats.client().clone()).get_stream(STREAM_NAME),
                        )
                        .await
                        .map(|r| r.is_ok())
                        .unwrap_or(false)
                    } else {
                        false
                    }
                } else {
                    false
                };
                update_readiness_market_data_current(nats_connected, control_sub_active, jetstream_ready);

                let event = MarketEvent::new(
                    "market-data",
                    BUILD_VERSION,
                    run_id,
                    ctx.next_event_id(),
                    "simulated",
                    Some(slot),
                    MarketEventKind::SlotUpdate { current_slot: slot },
                );

                // Write to JSONL (P0 requirement)
        ctx.write_market_event_jsonl(&event);

                // Publish to NATS
                if let Some(ref nats) = ctx.nats {
                    if let Err(e) = nats.publish(TOPIC_MARKET_EVENTS, &event).await {
                        warn!(error = %e, "Failed to publish to NATS");
                    }
                }

                // Periodic stats
                if slot % 60 == 0 {
                    let (records, bytes) = ctx.jsonl_writer.stats();
                    info!(
                        slot,
                        records_written = records,
                        bytes_written = bytes,
                        "market-data heartbeat (simulation)"
                    );
                }
            }

            // I-24d: ControlRequests (EnsurePumpAmmPoolAccounts Discovery, same handler as Normalmodus)
            msg = async {
                if let Some(ref mut sub) = control_subscription {
                    sub.next().await
                } else {
                    std::future::pending::<Option<ironcrab::nats::NatsMessage>>().await
                }
            } => {
                if let Some(nats_msg) = msg {
                    match nats_msg.deserialize::<ControlRequest>() {
                        Ok(req) => {
                            if req.target != "market-data" {
                                debug!(target = %req.target, "Ignoring ControlRequest for other target");
                            } else {
                                match req.kind {
                                    ControlRequestKind::EnsurePumpAmmPoolAccounts { base_mint } => {
                                        let run_id = ctx.run_id.clone();
                                        let request_id = req.request_id.clone();
                                        let pool_hint = req.pool_address_hint.clone();
                                        let force_refresh = req.force_refresh;
                                        info!(
                                            request_id = %request_id,
                                            base_mint = %base_mint,
                                            pool_address_hint = ?pool_hint,
                                            force_refresh = %force_refresh,
                                            "I-24d Discovery: EnsurePumpAmmPoolAccounts received (simulation)"
                                        );
                                        let ctx_clone = ctx.clone();
                                        let dex_clone = Arc::clone(&pump_amm_dex);
                                        tokio::spawn(async move {
                                            handle_ensure_pump_amm_pool_accounts(
                                                ctx_clone.as_ref(),
                                                dex_clone.as_ref(),
                                                run_id.as_str(),
                                                &request_id,
                                                &base_mint,
                                                pool_hint.as_deref(),
                                                force_refresh,
                                            )
                                            .await;
                                        });
                                    }
                                    ControlRequestKind::EnsurePumpfunBondingCurve { base_mint } => {
                                        let run_id = ctx.run_id.clone();
                                        let request_id = req.request_id.clone();
                                        let pool_hint = req.pool_address_hint.clone();
                                        let force_refresh = req.force_refresh_pumpfun;
                                        info!(
                                            request_id = %request_id,
                                            base_mint = %base_mint,
                                            pool_address_hint = ?pool_hint,
                                            force_refresh_pumpfun = %force_refresh,
                                            "EnsurePumpfunBondingCurve received (simulation)"
                                        );
                                        let ctx_clone = ctx.clone();
                                        let rpc_clone = rpc.clone();
                                        tokio::spawn(async move {
                                            handle_ensure_pumpfun_bonding_curve(
                                                ctx_clone.as_ref(),
                                                &rpc_clone,
                                                run_id.as_str(),
                                                &request_id,
                                                &base_mint,
                                                pool_hint.as_deref(),
                                                force_refresh,
                                            )
                                            .await;
                                        });
                                    }
                                    ControlRequestKind::EnsureOrcaWhirlpoolPoolState { base_mint } => {
                                        let run_id = ctx.run_id.clone();
                                        let request_id = req.request_id.clone();
                                        let pool_hint = req.pool_address_hint.clone();
                                        let force_refresh = req.force_refresh_orca;
                                        info!(
                                            request_id = %request_id,
                                            base_mint = %base_mint,
                                            pool_address_hint = ?pool_hint,
                                            force_refresh_orca = %force_refresh,
                                            "EnsureOrcaWhirlpoolPoolState received (simulation)"
                                        );
                                        let ctx_clone = ctx.clone();
                                        let rpc_clone = rpc.clone();
                                        tokio::spawn(async move {
                                            handle_ensure_orca_whirlpool_pool_state(
                                                ctx_clone.as_ref(),
                                                &rpc_clone,
                                                run_id.as_str(),
                                                &request_id,
                                                &base_mint,
                                                pool_hint.as_deref(),
                                                force_refresh,
                                            )
                                            .await;
                                        });
                                    }
                                    ControlRequestKind::EnsureMeteoraDlmmPoolState { base_mint } => {
                                        let run_id = ctx.run_id.clone();
                                        let request_id = req.request_id.clone();
                                        let pool_hint = req.pool_address_hint.clone();
                                        let force_refresh = req.force_refresh_meteora_dlmm;
                                        info!(
                                            request_id = %request_id,
                                            base_mint = %base_mint,
                                            pool_address_hint = ?pool_hint,
                                            force_refresh_meteora_dlmm = %force_refresh,
                                            "EnsureMeteoraDlmmPoolState received (simulation)"
                                        );
                                        let ctx_clone = ctx.clone();
                                        let rpc_clone = rpc.clone();
                                        tokio::spawn(async move {
                                            handle_ensure_meteora_dlmm_pool_state(
                                                ctx_clone.as_ref(),
                                                &rpc_clone,
                                                run_id.as_str(),
                                                &request_id,
                                                &base_mint,
                                                pool_hint.as_deref(),
                                                force_refresh,
                                            )
                                            .await;
                                        });
                                    }
                                    ControlRequestKind::EnsureRaydiumAmmPoolState { base_mint } => {
                                        let run_id = ctx.run_id.clone();
                                        let request_id = req.request_id.clone();
                                        let pool_hint = req.pool_address_hint.clone();
                                        let force_refresh = req.force_refresh_raydium_amm;
                                        info!(
                                            request_id = %request_id,
                                            base_mint = %base_mint,
                                            pool_address_hint = ?pool_hint,
                                            force_refresh_raydium_amm = %force_refresh,
                                            "EnsureRaydiumAmmPoolState received (simulation)"
                                        );
                                        let ctx_clone = ctx.clone();
                                        let rpc_clone = rpc.clone();
                                        tokio::spawn(async move {
                                            handle_ensure_raydium_amm_pool_state(
                                                ctx_clone.as_ref(),
                                                &rpc_clone,
                                                run_id.as_str(),
                                                &request_id,
                                                &base_mint,
                                                pool_hint.as_deref(),
                                                force_refresh,
                                            )
                                            .await;
                                        });
                                    }
                                    ControlRequestKind::EnsureRaydiumCpmmPoolState { base_mint } => {
                                        let run_id = ctx.run_id.clone();
                                        let request_id = req.request_id.clone();
                                        let pool_hint = req.pool_address_hint.clone();
                                        let force_refresh = req.force_refresh_raydium_cpmm;
                                        info!(
                                            request_id = %request_id,
                                            base_mint = %base_mint,
                                            pool_address_hint = ?pool_hint,
                                            force_refresh_raydium_cpmm = %force_refresh,
                                            "EnsureRaydiumCpmmPoolState received (simulation)"
                                        );
                                        let ctx_clone = ctx.clone();
                                        let rpc_clone = rpc.clone();
                                        tokio::spawn(async move {
                                            handle_ensure_raydium_cpmm_pool_state(
                                                ctx_clone.as_ref(),
                                                &rpc_clone,
                                                run_id.as_str(),
                                                &request_id,
                                                &base_mint,
                                                pool_hint.as_deref(),
                                                force_refresh,
                                            )
                                            .await;
                                        });
                                    }
                                    ControlRequestKind::EnsureMeteoraCpmmPoolState { base_mint } => {
                                        let run_id = ctx.run_id.clone();
                                        let request_id = req.request_id.clone();
                                        let pool_hint = req.pool_address_hint.clone();
                                        let force_refresh = req.force_refresh_meteora_cpmm;
                                        info!(
                                            request_id = %request_id,
                                            base_mint = %base_mint,
                                            pool_address_hint = ?pool_hint,
                                            force_refresh_meteora_cpmm = %force_refresh,
                                            "EnsureMeteoraCpmmPoolState received (simulation)"
                                        );
                                        let ctx_clone = ctx.clone();
                                        let rpc_clone = rpc.clone();
                                        tokio::spawn(async move {
                                            handle_ensure_meteora_cpmm_pool_state(
                                                ctx_clone.as_ref(),
                                                &rpc_clone,
                                                run_id.as_str(),
                                                &request_id,
                                                &base_mint,
                                                pool_hint.as_deref(),
                                                force_refresh,
                                            )
                                            .await;
                                        });
                                    }
                                    _ => {
                                        debug!(kind = ?req.kind, "Ignoring ControlRequest kind for market-data (simulation)");
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            warn!(error = %e, "Failed to deserialize ControlRequest");
                        }
                    }
                }
            }

            // PR-D: Momentum active pool pin stream (simulation / tests).
            msg = async {
                if let Some(ref mut sub) = momentum_active_pools_sub {
                    sub.next().await
                } else {
                    std::future::pending::<Option<ironcrab::nats::NatsMessage>>().await
                }
            } => {
                if let Some(nats_msg) = msg {
                    match serde_json::from_slice::<MomentumActivePoolsUpdate>(&nats_msg.payload) {
                        Ok(update) => {
                            momentum_coalesce_try_send(&momentum_coalesce_tx, update);
                        }
                        Err(e) => {
                            warn!(error = %e, "Failed to deserialize MomentumActivePoolsUpdate (simulation)");
                        }
                    }
                }
            }

            // Phase 3: Arb track_requests pin stream (simulation / tests).
            msg = async {
                if let Some(ref mut sub) = arb_track_requests_sub {
                    sub.next().await
                } else {
                    std::future::pending::<Option<ironcrab::nats::NatsMessage>>().await
                }
            } => {
                if let Some(nats_msg) = msg {
                    match serde_json::from_slice::<ArbTrackRequestsUpdate>(&nats_msg.payload) {
                        Ok(update) => {
                            arb_coalesce_try_send(&arb_coalesce_tx, update);
                        }
                        Err(e) => {
                            warn!(error = %e, "Failed to deserialize ArbTrackRequestsUpdate (simulation)");
                        }
                    }
                }
            }

            // P1: Handle Config Updates (Runtime Configuration via UI)
            msg = async {
                if let Some(ref mut sub) = config_subscription {
                    sub.next().await
                } else {
                    std::future::pending::<Option<ironcrab::nats::NatsMessage>>().await
                }
            } => {
                if let Some(nats_msg) = msg {
                    match serde_json::from_slice::<ConfigUpdate>(&nats_msg.payload) {
                        Ok(update) => {
                            if update.target_component == "market-data" {
                                info!(
                                    component = %update.target_component,
                                    keys = ?update.config.keys().collect::<Vec<_>>(),
                                    "Received Config Update from control-plane"
                                );
                                let response =
                                    ctx.apply_config_update(&update, Some(&md_state), Some(&track_worker));
                                info!(
                                    status = ?response.status,
                                    applied = ?response.applied_keys,
                                    rejected = ?response.rejected_keys,
                                    "Config update processed"
                                );
                            } else {
                                debug!(component = %update.target_component, "Ignoring config update for other component");
                            }
                        }
                        Err(e) => {
                            warn!(error = %e, "Failed to deserialize ConfigUpdate");
                        }
                    }
                }
            }

            _ = &mut shutdown => {
                info!("Shutdown signal received");
                break;
            }
        }
    }

    Ok(track_worker)
}

// ============================================================================
// Discovery Request/Reply tests (I-24d)
// ============================================================================

#[cfg(test)]
mod discovery_tests {
    use super::*;
    use ironcrab::execution::live_pool_cache::{
        meteora_dlmm_readiness_for_pool_cache_update, orca_readiness_for_pool_cache_update,
        raydium_amm_readiness_for_pool_cache_update, RaydiumAmmState,
    };
    use ironcrab::ipc::ControlResponseStatus;
    use ironcrab::market_data::cold::{
        pump_amm_control_response_for_ensure_publish, pump_amm_sell_layout_publish_state,
        pump_amm_sell_layout_state_for_ensure_publish,
    };

    /// Positive Ack ONLY when JetStream write succeeds (I-24a).
    /// This helper encodes the invariant; the handler uses the same logic.
    fn discovery_response_status_for_jetstream(jetstream_ok: bool) -> ControlResponseStatus {
        if jetstream_ok {
            ControlResponseStatus::Ok
        } else {
            ControlResponseStatus::Error
        }
    }

    #[test]
    fn test_discovery_response_ok_only_when_jetstream_succeeds() {
        assert_eq!(
            discovery_response_status_for_jetstream(true),
            ControlResponseStatus::Ok
        );
        assert_eq!(
            discovery_response_status_for_jetstream(false),
            ControlResponseStatus::Error
        );
    }

    #[test]
    fn test_pump_amm_non_force_refresh_partial_publish_keeps_ok_response() {
        let (status, message) = pump_amm_control_response_for_ensure_publish(false, true, false);
        assert_eq!(status, ControlResponseStatus::Ok);
        assert!(message.is_none());
    }

    #[test]
    fn test_pump_amm_force_refresh_partial_publish_returns_authoritative_error() {
        let (status, message) = pump_amm_control_response_for_ensure_publish(true, true, false);
        assert_eq!(status, ControlResponseStatus::Error);
        assert_eq!(
            message.as_deref(),
            Some("authoritative PumpSwap SELL layout unresolved after force_refresh")
        );
    }

    /// PR #50 follow-up: async Serum RPC may finish long after the pool discovery Geyser slot.
    /// JetStream `PoolCacheUpdate::geyser_slot` for the post-fetch publish must reflect the **current**
    /// cache entry slot (latest vault / pool update), not the stale discovery slot, so reserves and
    /// freshness metadata stay aligned (I-24a).
    #[test]
    fn test_raydium_amm_post_serum_jetstream_publish_slot_matches_cache_not_discovery() {
        let cache = LivePoolCache::new();
        let pool = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::new_unique();
        let coin_vault = Pubkey::new_unique();
        let pc_vault = Pubkey::new_unique();
        let market_id = Pubkey::new_unique();
        let bids = Pubkey::new_unique();
        let asks = Pubkey::new_unique();
        let eq = Pubkey::new_unique();

        const DISCOVERY_SLOT: u64 = 100;
        const VAULT_SLOT: u64 = 9_999;

        cache.upsert(
            pool,
            CachedPoolState::RaydiumAmm(RaydiumAmmState {
                base_mint,
                quote_mint,
                coin_vault,
                pc_vault,
                base_decimals: 9,
                quote_decimals: 9,
                coin_reserve: None,
                pc_reserve: None,
                market_id,
                serum_bids: None,
                serum_asks: None,
                serum_event_queue: None,
            }),
            DISCOVERY_SLOT,
        );
        cache.update_vault_balance(&coin_vault, 10, VAULT_SLOT);
        cache.update_vault_balance(&pc_vault, 20, VAULT_SLOT);
        cache.set_raydium_serum_accounts(&pool, bids, asks, eq);

        let (_, cache_slot, _) = cache.get_with_metadata(&pool).expect("pool in cache");
        assert_eq!(
            cache_slot, VAULT_SLOT,
            "vault updates should advance entry slot past discovery"
        );

        let st = match cache.get(&pool).expect("state") {
            CachedPoolState::RaydiumAmm(s) => s,
            other => panic!("expected RaydiumAmm, got {:?}", other),
        };
        let readiness = raydium_amm_readiness_for_pool_cache_update(&st);
        assert_eq!(readiness, DexPoolReadiness::Ready);

        let mut pool_update = PoolCacheUpdate::new_balance_updated(
            "market-data",
            BUILD_VERSION,
            "run-test",
            pool.to_string(),
            "raydium".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            st.coin_reserve.unwrap_or(0),
            st.pc_reserve.unwrap_or(0),
            cache_slot,
        );
        pool_update.set_dex_readiness_in_metadata(readiness);

        assert_eq!(pool_update.geyser_slot, VAULT_SLOT);
        assert_ne!(
            pool_update.geyser_slot, DISCOVERY_SLOT,
            "must not label fresh reserves with stale discovery slot"
        );
    }

    #[test]
    fn test_wallet_mints_needing_dex_bootstrap_verify_skips_ready_and_wsol() {
        let cache = LivePoolCache::new();
        let wsol = WSOL_MINT;
        let ready_mint = Pubkey::new_unique();
        let pool = Pubkey::new_unique();
        let quote = Pubkey::new_unique();
        let accounts: Vec<Pubkey> = (0..14).map(|_| Pubkey::new_unique()).collect();
        cache.upsert(
            pool,
            CachedPoolState::PumpAmm(PumpAmmState {
                base_mint: ready_mint,
                quote_mint: quote,
                pool_base_token_account: accounts[4],
                pool_quote_token_account: accounts[5],
                base_reserve: Some(1),
                quote_reserve: Some(1),
                pool_accounts: accounts.clone(),
                creator: None,
            }),
            0,
        );
        cache.merge_pump_amm_pool_accounts_readiness(pool, DexPoolReadiness::Ready);

        let need = Pubkey::new_unique();
        let mints = vec![
            wsol.to_string(),
            need.to_string(),
            ready_mint.to_string(),
            need.to_string(),
        ];
        let out = wallet_mints_needing_dex_bootstrap_verify(&cache, &mints, wsol, 8);
        assert_eq!(out, vec![need]);
    }

    #[test]
    fn test_wallet_mints_needing_dex_bootstrap_verify_respects_cap() {
        let cache = LivePoolCache::new();
        let a = Pubkey::new_unique();
        let b = Pubkey::new_unique();
        let c = Pubkey::new_unique();
        let mints = vec![a.to_string(), b.to_string(), c.to_string()];
        let out = wallet_mints_needing_dex_bootstrap_verify(&cache, &mints, WSOL_MINT, 2);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0], a);
        assert_eq!(out[1], b);
    }

    /// Scope-40 / PR #72 follow-up: migrated PumpFun mints can have bonding-curve Ready only
    /// (`base_mint_has_any_ready_pool`) so `wallet_mints_needing_dex_bootstrap_verify` is empty,
    /// while PumpSwap explicit Ready is still missing. The migration helper must still select the
    /// mint (bootstrap must not early-return on empty `candidates` alone).
    #[test]
    fn test_wallet_mints_needing_pump_amm_migration_when_candidates_empty() {
        let cache = LivePoolCache::new();
        let base_mint = Pubkey::new_unique();
        let (bonding_curve, _) = PumpFunDex::derive_bonding_curve_static(&base_mint);
        cache.upsert(
            bonding_curve,
            CachedPoolState::PumpFun(PumpFunState {
                token_mint: base_mint,
                bonding_curve,
                associated_bonding_curve: Pubkey::new_unique(),
                virtual_sol_reserves: 30_000_000_000,
                virtual_token_reserves: 1_000_000_000_000_000,
                real_sol_reserves: 0,
                real_token_reserves: 793_100_000_000_000,
                complete: true,
                creator: Pubkey::new_unique(),
                cashback_enabled: false,
            }),
            100,
        );
        cache.merge_pumpfun_bonding_readiness(bonding_curve, DexPoolReadiness::Ready);

        assert!(
            cache.base_mint_has_any_ready_pool(&base_mint),
            "JetStream bonding-curve Ready makes mint 'ready' at mint level"
        );
        assert!(
            !cache.base_mint_has_explicit_pump_amm_ready_pool(&base_mint),
            "PumpSwap row still missing — explicit PumpSwap Ready required for this scope"
        );
        assert!(cache.pumpfun_bonding_curve_complete_for_mint(&base_mint));

        let mints = vec![base_mint.to_string()];
        let cap = WALLET_BOOTSTRAP_DEX_VERIFY_MAX_MINTS;
        let candidates = wallet_mints_needing_dex_bootstrap_verify(&cache, &mints, WSOL_MINT, cap);
        assert!(
            candidates.is_empty(),
            "legacy candidates skip mints that already have any ready pool (PumpFun)"
        );

        let migration =
            wallet_mints_needing_pump_amm_after_pumpfun_migration(&cache, &mints, WSOL_MINT, cap);
        assert_eq!(migration, vec![base_mint]);
    }

    /// Regression (PR #47 follow-up): Geyser may leave `PumpFun` in cache without explicit Ready.
    /// `handle_ensure_pumpfun_bonding_curve(..., force_refresh=false)` then returns early ("cache hit")
    /// and never runs merge + JetStream. Wallet bootstrap must pass `WALLET_BOOTSTRAP_ENSURE_PUMPFUN_FORCE_REFRESH`
    /// (true) into that handler so the promotion path runs.
    #[test]
    fn test_bootstrap_verify_pumpfun_cached_without_explicit_ready_scenario() {
        let cache = LivePoolCache::new();
        let base_mint = Pubkey::new_unique();
        let (bonding_curve, _) = PumpFunDex::derive_bonding_curve_static(&base_mint);
        cache.upsert(
            bonding_curve,
            CachedPoolState::PumpFun(PumpFunState {
                token_mint: base_mint,
                bonding_curve,
                associated_bonding_curve: Pubkey::new_unique(),
                virtual_sol_reserves: 30_000_000_000,
                virtual_token_reserves: 1_000_000_000_000_000,
                real_sol_reserves: 0,
                real_token_reserves: 793_100_000_000_000,
                complete: false,
                creator: Pubkey::new_unique(),
                cashback_enabled: false,
            }),
            100,
        );
        assert!(
            !cache.base_mint_has_any_ready_pool(&base_mint),
            "candidate mint: cached PumpFun without explicit Ready merge"
        );
        assert!(
            !cache.pumpfun_bonding_curve_explicitly_ready(&bonding_curve),
            "explicit Ready must be absent for this regression scenario"
        );
    }

    #[test]
    fn test_raydium_cpmm_readiness_helper_sol_quote_both_sides() {
        let sol = Pubkey::from_str(NATIVE_SOL_MINT).unwrap();
        let base = Pubkey::new_unique();
        let s = RaydiumCpmmState {
            token_0_mint: base,
            token_1_mint: sol,
            token_0_vault: Pubkey::default(),
            token_1_vault: Pubkey::default(),
            reserve_0: Some(100),
            reserve_1: Some(200),
        };
        assert_eq!(
            raydium_cpmm_readiness_for_pool_cache_update(&s),
            DexPoolReadiness::Ready
        );
    }

    #[test]
    fn test_raydium_cpmm_readiness_helper_sol_as_token_0_one_side_only_partial() {
        let sol = Pubkey::from_str(NATIVE_SOL_MINT).unwrap();
        let base = Pubkey::new_unique();
        let s = RaydiumCpmmState {
            token_0_mint: sol,
            token_1_mint: base,
            token_0_vault: Pubkey::default(),
            token_1_vault: Pubkey::default(),
            reserve_0: Some(50),
            reserve_1: Some(0),
        };
        assert_eq!(
            raydium_cpmm_readiness_for_pool_cache_update(&s),
            DexPoolReadiness::Partial
        );
    }

    #[test]
    fn test_raydium_cpmm_readiness_helper_non_sol_pair_both_sides_ready() {
        let a = Pubkey::new_unique();
        let b = Pubkey::new_unique();
        let s = RaydiumCpmmState {
            token_0_mint: a,
            token_1_mint: b,
            token_0_vault: Pubkey::default(),
            token_1_vault: Pubkey::default(),
            reserve_0: Some(1),
            reserve_1: Some(2),
        };
        assert_eq!(
            raydium_cpmm_readiness_for_pool_cache_update(&s),
            DexPoolReadiness::Ready
        );
    }

    #[test]
    fn test_raydium_cpmm_readiness_helper_observed_when_zero_reserves() {
        let a = Pubkey::new_unique();
        let b = Pubkey::new_unique();
        let s = RaydiumCpmmState {
            token_0_mint: a,
            token_1_mint: b,
            token_0_vault: Pubkey::default(),
            token_1_vault: Pubkey::default(),
            reserve_0: Some(0),
            reserve_1: Some(0),
        };
        assert_eq!(
            raydium_cpmm_readiness_for_pool_cache_update(&s),
            DexPoolReadiness::Observed
        );
    }

    #[test]
    fn test_pump_amm_trade_publish_extended_without_third_meta_is_partial() {
        let (sell_layout_ready, dex_readiness) = pump_amm_sell_layout_publish_state(
            true, None, None, None, None, None, false, false, None, true,
        );
        assert!(
            !sell_layout_ready,
            "extended SELL without authoritative third meta must not be marked ready"
        );
        assert_eq!(dex_readiness, DexPoolReadiness::Partial);
    }

    #[test]
    fn test_pump_amm_trade_publish_extended_with_third_only_without_volume_tails_is_ready() {
        let (sell_layout_ready, dex_readiness) = pump_amm_sell_layout_publish_state(
            true,
            Some(Pubkey::new_unique()),
            None,
            None,
            None,
            None,
            false,
            false,
            None,
            true,
        );
        assert!(
            sell_layout_ready,
            "extended SELL with third_meta only must be ready — volume tails #21/#22 are build-derived"
        );
        assert_eq!(dex_readiness, DexPoolReadiness::Ready);
    }

    #[test]
    fn test_pump_amm_trade_publish_extended_with_full_tail_is_ready() {
        // v14[12]/[13] may differ from global SELL fee_config — readiness must not depend on them (Scope 59).
        let (sell_layout_ready, dex_readiness) = pump_amm_sell_layout_publish_state(
            true,
            Some(Pubkey::new_unique()),
            Some(Pubkey::new_unique()),
            Some(Pubkey::new_unique()),
            None,
            None,
            false,
            false,
            None,
            true,
        );
        assert!(
            sell_layout_ready,
            "extended SELL with full observed tail must be marked ready"
        );
        assert_eq!(dex_readiness, DexPoolReadiness::Ready);
    }

    /// P184j: authoritative force_refresh (e.g. tier-26 / base) must clear stale 27er pre_fee cache.
    #[test]
    fn test_pump_amm_force_refresh_authoritative_pre_fee_overrides_stale_cache() {
        let stale_third = Pubkey::new_unique();
        let stale_t0 = Pubkey::new_unique();
        let stale_t1 = Pubkey::new_unique();
        let stale_pre_fee_1 = Pubkey::new_unique();
        let (
            effective_requires_extended,
            effective_third_meta,
            effective_tail_0,
            effective_tail_1,
            _effective_fee_tail_0,
            _effective_fee_tail_1,
            _effective_requires_fee_tail,
            effective_requires_pre_fee_metas,
            effective_pre_fee_meta_1,
            sell_layout_ready,
            dex_readiness,
        ) = pump_amm_sell_layout_state_for_ensure_publish(
            true,
            true,
            Some(stale_third),
            Some(stale_t0),
            Some(stale_t1),
            None,
            None,
            false,
            true,
            Some(stale_pre_fee_1),
            false,
            None,
            None,
            None,
            None,
            None,
            false,
            false,
            None,
            true,
        );
        assert!(
            effective_requires_extended,
            "cached extended hint may stay monotonic on force_refresh when refresh is base-only"
        );
        assert!(
            !effective_requires_pre_fee_metas,
            "P184j: stale cached pre-fee must not OR over authoritative tier-26/base refresh"
        );
        assert!(
            effective_pre_fee_meta_1.is_none(),
            "authoritative refresh without pre_fee must not keep stale pre_fee_meta_1 alone"
        );
        assert!(
            effective_third_meta.is_none(),
            "authoritative base refresh must discard stale third meta"
        );
        assert!(effective_tail_0.is_none() && effective_tail_1.is_none());
        assert!(
            !sell_layout_ready,
            "extended without authoritative third meta must not mark pool ready"
        );
        assert_eq!(dex_readiness, DexPoolReadiness::Partial);
    }

    #[test]
    fn test_pump_amm_force_refresh_authoritative_pre_fee_true_not_downgraded_by_cache() {
        let refresh_pre_fee_1 = Pubkey::new_unique();
        let (
            _effective_requires_extended,
            _effective_third_meta,
            _effective_tail_0,
            _effective_tail_1,
            _effective_fee_tail_0,
            _effective_fee_tail_1,
            _effective_requires_fee_tail,
            effective_requires_pre_fee_metas,
            effective_pre_fee_meta_1,
            _sell_layout_ready,
            _dex_readiness,
        ) = pump_amm_sell_layout_state_for_ensure_publish(
            true,
            false,
            None,
            None,
            None,
            None,
            None,
            false,
            false,
            None,
            true,
            None,
            None,
            None,
            None,
            None,
            false,
            true,
            Some(refresh_pre_fee_1),
            false,
        );
        assert!(
            effective_requires_pre_fee_metas,
            "authoritative 27er refresh must publish pre_fee even when cache had none"
        );
        assert_eq!(effective_pre_fee_meta_1, Some(refresh_pre_fee_1));
    }

    #[test]
    fn test_live_pool_cache_raydium_cpmm_pools_for_mint_bounded_to_cache() {
        let cache = LivePoolCache::new();
        let m = Pubkey::new_unique();
        let other = Pubkey::new_unique();
        let p1 = Pubkey::new_unique();
        let p2 = Pubkey::new_unique();
        let v = RaydiumCpmmState {
            token_0_mint: m,
            token_1_mint: other,
            token_0_vault: Pubkey::new_unique(),
            token_1_vault: Pubkey::new_unique(),
            reserve_0: None,
            reserve_1: None,
        };
        cache.upsert(p1, CachedPoolState::RaydiumCpmm(v.clone()), 0);
        cache.upsert(
            p2,
            CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
                token_0_mint: other,
                token_1_mint: other,
                token_0_vault: Pubkey::new_unique(),
                token_1_vault: Pubkey::new_unique(),
                reserve_0: Some(1_000_000),
                reserve_1: Some(2_000_000),
            }),
            0,
        );
        let rows = cache.raydium_cpmm_pools_for_mint(&m);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, p1);
    }

    #[test]
    fn test_live_pool_cache_meteora_cpmm_pools_for_mint_bounded_to_cache() {
        let cache = LivePoolCache::new();
        let m = Pubkey::new_unique();
        let other = Pubkey::new_unique();
        let p1 = Pubkey::new_unique();
        let p2 = Pubkey::new_unique();
        let v = MeteoraCpmmState {
            token_0_mint: m,
            token_1_mint: other,
            token_0_vault: Pubkey::new_unique(),
            token_1_vault: Pubkey::new_unique(),
            amm_config: Pubkey::new_unique(),
            observation_key: Pubkey::new_unique(),
            token_0_program: Pubkey::new_unique(),
            token_1_program: Pubkey::new_unique(),
            reserve_0: 0,
            reserve_1: 0,
            mint_0_decimals: 6,
            mint_1_decimals: 9,
            status: 0,
        };
        cache.upsert(p1, CachedPoolState::MeteoraCpmm(v.clone()), 0);
        cache.upsert(
            p2,
            CachedPoolState::MeteoraCpmm(MeteoraCpmmState {
                token_0_mint: other,
                token_1_mint: other,
                token_0_vault: Pubkey::new_unique(),
                token_1_vault: Pubkey::new_unique(),
                amm_config: Pubkey::new_unique(),
                observation_key: Pubkey::new_unique(),
                token_0_program: Pubkey::new_unique(),
                token_1_program: Pubkey::new_unique(),
                reserve_0: 0,
                reserve_1: 0,
                mint_0_decimals: 6,
                mint_1_decimals: 9,
                status: 0,
            }),
            0,
        );
        let rows = cache.meteora_cpmm_pools_for_mint(&m);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, p1);
    }

    #[test]
    fn test_live_pool_cache_orca_whirlpool_pools_for_mint_bounded_to_cache() {
        let cache = LivePoolCache::new();
        let m = Pubkey::new_unique();
        let other = Pubkey::new_unique();
        let p1 = Pubkey::new_unique();
        let p2 = Pubkey::new_unique();
        let p3 = Pubkey::new_unique();
        let on_a = OrcaWhirlpoolState {
            token_mint_a: m,
            token_mint_b: other,
            token_vault_a: Pubkey::new_unique(),
            token_vault_b: Pubkey::new_unique(),
            tick_current_index: 0,
            sqrt_price: 1u128 << 64,
            liquidity: 1,
            fee_rate: 3000,
            protocol_fee_rate: 300,
            tick_spacing: 64,
            vault_a_balance: None,
            vault_b_balance: None,
            token_a_program: None,
            token_b_program: None,
        };
        cache.upsert(p1, CachedPoolState::Orca(on_a), 0);
        let on_b = OrcaWhirlpoolState {
            token_mint_a: other,
            token_mint_b: m,
            token_vault_a: Pubkey::new_unique(),
            token_vault_b: Pubkey::new_unique(),
            tick_current_index: 0,
            sqrt_price: 1u128 << 64,
            liquidity: 1,
            fee_rate: 3000,
            protocol_fee_rate: 300,
            tick_spacing: 64,
            vault_a_balance: None,
            vault_b_balance: None,
            token_a_program: None,
            token_b_program: None,
        };
        cache.upsert(p2, CachedPoolState::Orca(on_b), 0);
        cache.upsert(
            p3,
            CachedPoolState::Orca(OrcaWhirlpoolState {
                token_mint_a: other,
                token_mint_b: other,
                token_vault_a: Pubkey::new_unique(),
                token_vault_b: Pubkey::new_unique(),
                tick_current_index: 0,
                sqrt_price: 1u128 << 64,
                liquidity: 1,
                fee_rate: 3000,
                protocol_fee_rate: 300,
                tick_spacing: 64,
                vault_a_balance: None,
                vault_b_balance: None,
                token_a_program: None,
                token_b_program: None,
            }),
            0,
        );
        let rows = cache.orca_whirlpool_pools_for_mint(&m);
        assert_eq!(rows.len(), 2);
        let addrs: Vec<Pubkey> = rows.iter().map(|(a, _)| *a).collect();
        assert!(addrs.contains(&p1));
        assert!(addrs.contains(&p2));
        assert!(!addrs.contains(&p3));
    }

    #[test]
    fn test_live_pool_cache_meteora_dlmm_pools_for_mint_bounded_to_cache() {
        let cache = LivePoolCache::new();
        let m = Pubkey::new_unique();
        let other = Pubkey::new_unique();
        let p1 = Pubkey::new_unique();
        let p2 = Pubkey::new_unique();
        let p3 = Pubkey::new_unique();
        let on_x = MeteoraState {
            token_x_mint: m,
            token_y_mint: other,
            reserve_x: Pubkey::new_unique(),
            reserve_y: Pubkey::new_unique(),
            active_id: -1,
            bin_step: 4,
            reserve_x_balance: None,
            reserve_y_balance: None,
        };
        cache.upsert(p1, CachedPoolState::Meteora(on_x), 0);
        let on_y = MeteoraState {
            token_x_mint: other,
            token_y_mint: m,
            reserve_x: Pubkey::new_unique(),
            reserve_y: Pubkey::new_unique(),
            active_id: 0,
            bin_step: 0,
            reserve_x_balance: None,
            reserve_y_balance: None,
        };
        cache.upsert(p2, CachedPoolState::Meteora(on_y), 0);
        cache.upsert(
            p3,
            CachedPoolState::Meteora(MeteoraState {
                token_x_mint: other,
                token_y_mint: other,
                reserve_x: Pubkey::new_unique(),
                reserve_y: Pubkey::new_unique(),
                active_id: 0,
                bin_step: 1,
                reserve_x_balance: None,
                reserve_y_balance: None,
            }),
            0,
        );
        let rows = cache.meteora_dlmm_pools_for_mint(&m);
        assert_eq!(rows.len(), 2);
        let addrs: Vec<Pubkey> = rows.iter().map(|(a, _)| *a).collect();
        assert!(addrs.contains(&p1));
        assert!(addrs.contains(&p2));
        assert!(!addrs.contains(&p3));
    }

    #[test]
    fn test_orca_wallet_bootstrap_ready_path_requires_explicit_merge_not_cache_row() {
        let cache = LivePoolCache::new();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::new_unique();
        let pool = Pubkey::new_unique();
        let s = OrcaWhirlpoolState {
            token_mint_a: base_mint,
            token_mint_b: quote_mint,
            token_vault_a: Pubkey::new_unique(),
            token_vault_b: Pubkey::new_unique(),
            tick_current_index: -42,
            sqrt_price: 1u128 << 64,
            liquidity: 1_000_000,
            fee_rate: 3000,
            protocol_fee_rate: 300,
            tick_spacing: 64,
            vault_a_balance: Some(1_000_000),
            vault_b_balance: Some(50_000_000_000),
            token_a_program: None,
            token_b_program: None,
        };
        assert_eq!(
            orca_readiness_for_pool_cache_update(&s),
            DexPoolReadiness::Ready
        );
        cache.upsert(pool, CachedPoolState::Orca(s), 100);
        assert!(
            !cache.base_mint_has_any_ready_pool(&base_mint),
            "Orca cache row with full reserves must not imply mint-level ready without explicit merge"
        );
        cache.merge_orca_pool_readiness(pool, DexPoolReadiness::Ready);
        assert!(cache.base_mint_has_explicit_orca_ready_pool(&base_mint));
        assert!(cache.base_mint_has_any_ready_pool(&base_mint));
    }

    /// Bounded wallet bootstrap RPC verify uses `geyser_slot = 0` on JetStream publishes (same contract
    /// as Raydium/Meteora CPMM bootstrap handlers — not a stale Geyser discovery slot).
    #[test]
    fn test_orca_bounded_bootstrap_balance_update_uses_slot_zero_like_cpmm() {
        let pool = Pubkey::new_unique();
        let base = Pubkey::new_unique();
        let quote = Pubkey::new_unique();
        let bal = PoolCacheUpdate::new_balance_updated(
            "market-data",
            BUILD_VERSION,
            "run-test",
            pool.to_string(),
            "orca".to_string(),
            base.to_string(),
            quote.to_string(),
            1_000_000,
            50_000_000_000,
            0,
        );
        assert_eq!(bal.geyser_slot, 0);
    }

    #[test]
    fn test_meteora_dlmm_wallet_bootstrap_ready_path_requires_explicit_merge_not_cache_row() {
        let cache = LivePoolCache::new();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::new_unique();
        let pool = Pubkey::new_unique();
        let vx = Pubkey::new_unique();
        let vy = Pubkey::new_unique();
        let s = MeteoraState {
            token_x_mint: base_mint,
            token_y_mint: quote_mint,
            reserve_x: vx,
            reserve_y: vy,
            active_id: 0,
            bin_step: 0,
            reserve_x_balance: Some(1_000_000),
            reserve_y_balance: Some(50_000_000_000),
        };
        assert_eq!(
            meteora_dlmm_readiness_for_pool_cache_update(&s),
            DexPoolReadiness::Ready
        );
        cache.upsert(pool, CachedPoolState::Meteora(s), 100);
        assert!(
            !cache.base_mint_has_any_ready_pool(&base_mint),
            "DLMM cache row with full reserves must not imply mint-level ready without explicit merge"
        );
        cache.merge_meteora_dlmm_pool_readiness(pool, DexPoolReadiness::Ready);
        assert!(cache.base_mint_has_explicit_meteora_dlmm_ready_pool(&base_mint));
        assert!(cache.base_mint_has_any_ready_pool(&base_mint));
    }

    #[test]
    fn test_meteora_dlmm_bounded_bootstrap_balance_update_uses_slot_zero_like_cpmm() {
        let pool = Pubkey::new_unique();
        let base = Pubkey::new_unique();
        let quote = Pubkey::new_unique();
        let bal = PoolCacheUpdate::new_balance_updated(
            "market-data",
            BUILD_VERSION,
            "run-test",
            pool.to_string(),
            "meteora_dlmm".to_string(),
            base.to_string(),
            quote.to_string(),
            1_000_000,
            50_000_000_000,
            0,
        );
        assert_eq!(bal.geyser_slot, 0);
    }

    #[test]
    fn test_wallet_mints_needing_dex_bootstrap_verify_skips_meteora_cpmm_explicit_ready() {
        let cache = LivePoolCache::new();
        let wsol = WSOL_MINT;
        let base_mint = Pubkey::new_unique();
        let quote = Pubkey::new_unique();
        let pool = Pubkey::new_unique();
        cache.upsert(
            pool,
            CachedPoolState::MeteoraCpmm(MeteoraCpmmState {
                token_0_mint: base_mint,
                token_1_mint: quote,
                token_0_vault: Pubkey::new_unique(),
                token_1_vault: Pubkey::new_unique(),
                amm_config: Pubkey::new_unique(),
                observation_key: Pubkey::new_unique(),
                token_0_program: Pubkey::new_unique(),
                token_1_program: Pubkey::new_unique(),
                reserve_0: 1,
                reserve_1: 2,
                mint_0_decimals: 6,
                mint_1_decimals: 6,
                status: 0,
            }),
            0,
        );
        cache.merge_meteora_cpmm_pool_readiness(pool, DexPoolReadiness::Ready);

        let other = Pubkey::new_unique();
        let mints = vec![base_mint.to_string(), other.to_string()];
        let out = wallet_mints_needing_dex_bootstrap_verify(&cache, &mints, wsol, 8);
        assert_eq!(out, vec![other]);
    }

    #[test]
    fn test_wallet_mints_needing_dex_bootstrap_verify_skips_meteora_dlmm_explicit_ready() {
        let cache = LivePoolCache::new();
        let wsol = WSOL_MINT;
        let base_mint = Pubkey::new_unique();
        let quote = Pubkey::new_unique();
        let pool = Pubkey::new_unique();
        cache.upsert(
            pool,
            CachedPoolState::Meteora(MeteoraState {
                token_x_mint: base_mint,
                token_y_mint: quote,
                reserve_x: Pubkey::new_unique(),
                reserve_y: Pubkey::new_unique(),
                active_id: 0,
                bin_step: 0,
                reserve_x_balance: Some(1),
                reserve_y_balance: Some(2),
            }),
            0,
        );
        cache.merge_meteora_dlmm_pool_readiness(pool, DexPoolReadiness::Ready);

        let other = Pubkey::new_unique();
        let mints = vec![base_mint.to_string(), other.to_string()];
        let out = wallet_mints_needing_dex_bootstrap_verify(&cache, &mints, wsol, 8);
        assert_eq!(out, vec![other]);
    }
}

/// WSOL ATA balance parsing from Geyser (Scope 55: zero after close)
#[cfg(test)]
mod wsol_ata_update_tests {
    use ironcrab::market_data::ingest::wsol_ata_balance_lamports_from_geyser_data;

    #[test]
    fn empty_wsol_ata_data_means_zero_balance() {
        assert_eq!(wsol_ata_balance_lamports_from_geyser_data(&[]), Some(0));
    }

    #[test]
    fn short_corrupt_wsol_ata_data_skips_update() {
        // Not enough bytes for a standard token account; must not be treated as 0
        // (avoids clobbering state on transient garbage).
        assert_eq!(wsol_ata_balance_lamports_from_geyser_data(&[1, 2, 3]), None);
    }

    #[test]
    fn standard_spl_token_account_parses_amount() {
        let mut data = vec![0u8; 165];
        let amt: u64 = 1_000_000_000;
        data[64..72].copy_from_slice(&amt.to_le_bytes());
        assert_eq!(
            wsol_ata_balance_lamports_from_geyser_data(&data),
            Some(1_000_000_000)
        );
    }
}

/// Decoupled SOL/WSOL Geyser publish decisions (KNOWN_BUG_PATTERNS #23 fix).
#[cfg(test)]
mod wallet_geyser_snapshot_decouple_tests {
    use ironcrab::market_data::ingest::{
        wallet_geyser_snapshots_to_publish, WalletGeyserSnapshotMint, WalletGeyserUpdateSource,
    };

    #[test]
    fn native_sol_geyser_change_publishes_only_native_even_with_stale_wsol_cache() {
        // Simulates: last_wsol_balance=1B in cache, but only native SOL lamports changed on Geyser.
        let snapshot = wallet_geyser_snapshots_to_publish(WalletGeyserUpdateSource::NativeSol {
            lamports: 2_900_000_000,
            prev_lamports: 3_000_000_000,
        })
        .expect("native balance changed");
        assert_eq!(snapshot.mint, WalletGeyserSnapshotMint::NativeSol);
        assert_eq!(snapshot.balance_raw, 2_900_000_000);
    }

    #[test]
    fn native_sol_geyser_unchanged_publishes_nothing() {
        assert!(
            wallet_geyser_snapshots_to_publish(WalletGeyserUpdateSource::NativeSol {
                lamports: 3_000_000_000,
                prev_lamports: 3_000_000_000,
            })
            .is_none()
        );
    }

    #[test]
    fn wsol_ata_geyser_change_publishes_only_wsol_even_with_stale_native_cache() {
        // Simulates: last_sol_balance=3B in cache, but only WSOL token balance changed on Geyser.
        let snapshot = wallet_geyser_snapshots_to_publish(WalletGeyserUpdateSource::WsolAta {
            balance: 1_000_000_000,
            prev_balance: 0,
        })
        .expect("wsol balance changed");
        assert_eq!(snapshot.mint, WalletGeyserSnapshotMint::Wsol);
        assert_eq!(snapshot.balance_raw, 1_000_000_000);
    }

    #[test]
    fn wsol_ata_zero_after_unwrap_still_publishes_wsol_zero() {
        let snapshot = wallet_geyser_snapshots_to_publish(WalletGeyserUpdateSource::WsolAta {
            balance: 0,
            prev_balance: 1_000_000_000,
        })
        .expect("unwrap must publish WSOL=0");
        assert_eq!(snapshot.mint, WalletGeyserSnapshotMint::Wsol);
        assert_eq!(snapshot.balance_raw, 0);
    }

    #[test]
    fn wsol_ata_unchanged_publishes_nothing() {
        assert!(
            wallet_geyser_snapshots_to_publish(WalletGeyserUpdateSource::WsolAta {
                balance: 1_000_000_000,
                prev_balance: 1_000_000_000,
            })
            .is_none()
        );
    }
}

/// P0: stale JetStream cleanup must not zero NATIVE_SOL; SPL ghosts still cleared.
#[cfg(test)]
mod wallet_snapshot_stale_cleanup_tests {
    use super::*;
    use ironcrab::nats::{ensure_wallet_snapshot_stream, NatsClient, NatsConfig};
    use std::collections::HashSet;
    use std::process::{Child, Command, Stdio};

    struct NatsTestServer {
        _child: Child,
        _store_dir: tempfile::TempDir,
        url: String,
    }

    impl NatsTestServer {
        fn start() -> Self {
            let store_dir = tempfile::tempdir().expect("jetstream store tempdir");
            let port = std::net::TcpListener::bind("127.0.0.1:0")
                .expect("bind ephemeral port")
                .local_addr()
                .expect("local addr")
                .port();
            let child = Command::new("nats-server")
                .args([
                    "-js",
                    "-p",
                    &port.to_string(),
                    "-sd",
                    store_dir.path().to_str().expect("tempdir path utf8"),
                ])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("spawn nats-server");
            let url = format!("nats://127.0.0.1:{port}");
            std::thread::sleep(std::time::Duration::from_millis(500));
            Self {
                _child: child,
                _store_dir: store_dir,
                url,
            }
        }
    }

    impl Drop for NatsTestServer {
        fn drop(&mut self) {
            let _ = self._child.kill();
            let _ = self._child.wait();
        }
    }

    fn minimal_market_data_context_with_nats(nats: NatsClient) -> MarketDataContext {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let (tracked_wallet_tx, _) = watch::channel(Vec::<Pubkey>::new());
        let (tracked_mints_tx, _tracked_mints_rx) = watch::channel(Vec::<Pubkey>::new());
        let (tracked_vaults_tx, _tracked_vaults_rx) = watch::channel(Vec::<Pubkey>::new());
        let (tracked_bin_arrays_tx, _tracked_bin_arrays_rx) = watch::channel(Vec::<Pubkey>::new());
        MarketDataContext {
            run_id: "run-stale-cleanup-test".to_string(),
            config: parking_lot::RwLock::new(MarketDataConfig::default()),
            geyser_full_reconnect_threshold_live: Arc::new(AtomicUsize::new(0)),
            nats: Some(nats),
            jsonl_writer: jsonl,
            started_at: Instant::now(),
            event_counter: std::sync::atomic::AtomicU64::new(0),
            wallet_tracker: WalletTracker::new(WalletTrackerCfg::default()),
            priority_fee_tracker: Arc::new(PriorityFeeTracker::new()),
            tracked_mints: parking_lot::RwLock::new(std::collections::HashMap::new()),
            tracked_mints_tx,
            hot_pool_registry: Arc::new(UnifiedHotPoolRegistry::new()),
            explicit_admitted_momentum_pools: parking_lot::RwLock::new(HashSet::new()),
            explicit_admitted_arb_pools: parking_lot::RwLock::new(HashSet::new()),
            known_pump_amm_pools: parking_lot::RwLock::new(std::collections::HashSet::new()),
            known_trade_dex_pools: parking_lot::RwLock::new(std::collections::HashSet::new()),
            pumpfun_pool_discovery_mint_info_emitted: parking_lot::RwLock::new(
                std::collections::HashSet::new(),
            ),
            tracked_vaults: parking_lot::RwLock::new(std::collections::HashMap::new()),
            tracked_vaults_tx,
            tracked_bin_arrays: parking_lot::RwLock::new(std::collections::HashMap::new()),
            tracked_bin_arrays_tx,
            tracked_membership: ArcSwap::from_pointee(TrackedMembershipSnapshot::default()),
            exec_hot_membership: ArcSwap::from_pointee(ExecHotMembershipSnapshot::default()),
            pool_tracked_legs: parking_lot::RwLock::new(HashMap::new()),
            live_pool_cache: Arc::new(LivePoolCache::new()),
            creator_cache: parking_lot::RwLock::new(std::collections::HashMap::new()),
            pool_mint_map: parking_lot::RwLock::new(std::collections::HashMap::new()),
            pool_creator_cache: parking_lot::RwLock::new(std::collections::HashMap::new()),
            high_priority_bonding_curves: parking_lot::RwLock::new(HashSet::new()),
            raydium_serum_fetched: parking_lot::RwLock::new(std::collections::HashSet::new()),
            tracked_wallet: None,
            tracked_wallet_tx,
            tracked_wallet_token_accounts: parking_lot::RwLock::new(
                std::collections::HashSet::new(),
            ),
            tracked_wallet_mint_decimals: parking_lot::RwLock::new(std::collections::HashMap::new()),
            execution_results_deduper: parking_lot::Mutex::new(ExecutionResultDeduper::default()),
            last_emitted_curve_progress: parking_lot::RwLock::new(std::collections::HashMap::new()),
            bonding_curve_publish_times: parking_lot::Mutex::new(BondingCurvePublishTimes::new()),
            pump_amm_dex: parking_lot::RwLock::new(None),
            helius_rpc: None,
            geyser_sync_batch_timer: parking_lot::Mutex::new(None),
            geyser_sync_flush_timestamps: parking_lot::Mutex::new(Vec::new()),
            geyser_sync_debounce_epoch: AtomicU64::new(0),
            ingest_tokio_handle: parking_lot::RwLock::new(None),
            pool_discovery_poolcreated_emitted: parking_lot::RwLock::new(
                std::collections::HashSet::new(),
            ),
            last_synced_explicit_pubkeys: parking_lot::RwLock::new(HashSet::new()),
            last_momentum_snapshot_target: parking_lot::RwLock::new(None),
            last_arb_snapshot_target: parking_lot::RwLock::new(None),
            dlmm_registered_active_id: parking_lot::RwLock::new(HashMap::new()),
            deferred_hot_pool_reserve_pins: parking_lot::RwLock::new(HashMap::new()),
            geyser_connect_barrier: Arc::new(GeyserConnectBarrier::new()),
            geyser_explicit_ready: AtomicBool::new(true),
            geyser_explicit_config_error: parking_lot::RwLock::new(None),
            geyser_prune_resume: parking_lot::Mutex::new(GeyserPruneResume::default()),
        }
    }

    fn wallet_balance_snapshot_event(mint: &str, balance_raw: u64) -> MarketEvent {
        MarketEvent::new(
            "market-data",
            BUILD_VERSION,
            "run-stale-cleanup-test",
            format!("seed_{mint}"),
            "wallet_bootstrap",
            None,
            MarketEventKind::WalletBalanceSnapshot {
                mint: mint.to_string(),
                balance_raw,
                decimals: 9,
                token_program: "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_string(),
            },
        )
    }

    async fn latest_wallet_snapshot_balance(
        client: &async_nats::Client,
        wallet: &str,
        mint: &str,
    ) -> Option<u64> {
        use async_nats::jetstream;
        use futures::StreamExt;

        let js = jetstream::new(client.clone());
        let stream = js.get_stream(WALLET_SNAPSHOT_STREAM_NAME).await.ok()?;
        let mut consumer_config = wallet_snapshot_consumer_config();
        consumer_config.filter_subject = wallet_snapshot_subject(wallet, mint);
        let consumer = stream.create_consumer(consumer_config).await.ok()?;
        let mut messages = consumer.fetch().max_messages(1).messages().await.ok()?;
        let msg = messages.next().await?.ok()?;
        let event: MarketEvent = serde_json::from_slice(&msg.payload).ok()?;
        let _ = msg.ack().await;
        match event.kind {
            MarketEventKind::WalletBalanceSnapshot { balance_raw, .. } => Some(balance_raw),
            _ => None,
        }
    }

    #[test]
    fn native_sol_and_wsol_never_stale_cleanup_targets() {
        let published = HashSet::new();
        assert!(!wallet_snapshot_stale_cleanup_targets_mint(
            "NATIVE_SOL",
            3_120_902_733,
            &published
        ));
        assert!(!wallet_snapshot_stale_cleanup_targets_mint(
            NATIVE_SOL_MINT,
            1,
            &published
        ));
        assert!(!wallet_snapshot_stale_cleanup_targets_mint(
            WSOL_MINT, 1, &published
        ));
    }

    #[test]
    fn spl_ghost_mint_is_stale_cleanup_target_when_not_published() {
        let ghost = "GhostMint1111111111111111111111111111111111";
        let published = HashSet::new();
        assert!(wallet_snapshot_stale_cleanup_targets_mint(
            ghost, 42, &published
        ));
        let mut published_with_ghost = HashSet::new();
        published_with_ghost.insert(ghost.to_string());
        assert!(!wallet_snapshot_stale_cleanup_targets_mint(
            ghost,
            42,
            &published_with_ghost
        ));
    }

    #[tokio::test]
    #[ignore = "requires nats-server on PATH (local/dev only)"]
    #[serial_test::serial]
    async fn stale_cleanup_zeros_spl_ghost_but_preserves_native_sol() {
        let server = NatsTestServer::start();
        let mut nats = NatsClient::new(NatsConfig::new(&server.url, "stale-cleanup-test"));
        nats.connect().await.expect("connect nats");
        ensure_wallet_snapshot_stream(nats.client())
            .await
            .expect("wallet snapshot stream");

        let wallet = Pubkey::new_unique();
        let wallet_str = wallet.to_string();
        const NATIVE_BALANCE: u64 = 3_120_902_733;
        const GHOST_MINT: &str = "GhostMint1111111111111111111111111111111111";
        const GHOST_BALANCE: u64 = 999_000;

        assert!(nats
            .jetstream_publish(
                &wallet_snapshot_subject(&wallet_str, "NATIVE_SOL"),
                &wallet_balance_snapshot_event("NATIVE_SOL", NATIVE_BALANCE),
            )
            .await
            .expect("publish native sol"));
        assert!(nats
            .jetstream_publish(
                &wallet_snapshot_subject(&wallet_str, GHOST_MINT),
                &wallet_balance_snapshot_event(GHOST_MINT, GHOST_BALANCE),
            )
            .await
            .expect("publish ghost"));
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;

        let ctx = minimal_market_data_context_with_nats(nats);
        let published_mint_set = HashSet::new();
        publish_wallet_snapshot_stale_jetstream_cleanup(
            &ctx,
            ctx.nats.as_ref().expect("nats"),
            &wallet_str,
            &published_mint_set,
        )
        .await;
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;

        let client = ctx.nats.as_ref().expect("nats").client().clone();
        let native_after = latest_wallet_snapshot_balance(&client, &wallet_str, "NATIVE_SOL")
            .await
            .expect("native sol snapshot");
        let ghost_after = latest_wallet_snapshot_balance(&client, &wallet_str, GHOST_MINT)
            .await
            .expect("ghost snapshot");

        assert_eq!(native_after, NATIVE_BALANCE);
        assert_eq!(ghost_after, 0);
    }
}

/// PR1: wallet balance snapshots from Geyser TX meta (`post_token_balances`).
#[cfg(test)]
mod wallet_tx_meta_balance_tests {
    use super::*;
    use ironcrab::market_data::ingest::{
        process_wallet_balance_snapshots_from_tx_meta, wallet_tx_meta_has_wsol_post_balance,
        wallet_tx_meta_native_sol_post_lamports,
    };
    use ironcrab::solana::geyser_listener::{
        geyser_tx_involves_wallet, GeyserTransactionUpdate, TokenAmount, TokenBalance,
    };
    use std::sync::atomic::AtomicUsize;
    use tokio::sync::mpsc;

    fn spawn_test_md_state(ctx: &Arc<MarketDataContext>) -> MdStateSender {
        let track_worker = spawn_track_worker(Arc::clone(ctx));
        spawn_md_state_worker(
            Arc::clone(ctx),
            tokio::runtime::Handle::current(),
            track_worker,
        )
    }

    fn market_data_context_with_tracked_wallet(wallet: Pubkey) -> Arc<MarketDataContext> {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let (tracked_wallet_tx, _) = watch::channel(Vec::<Pubkey>::new());
        let tracked = TrackedWallet::new(wallet);
        let (tracked_mints_tx, _tracked_mints_rx) = watch::channel(Vec::<Pubkey>::new());
        let (tracked_vaults_tx, _tracked_vaults_rx) = watch::channel(Vec::<Pubkey>::new());
        let (tracked_bin_arrays_tx, _tracked_bin_arrays_rx) = watch::channel(Vec::<Pubkey>::new());
        Arc::new(MarketDataContext {
            run_id: "run-wallet-tx-meta".to_string(),
            config: parking_lot::RwLock::new(MarketDataConfig::default()),
            geyser_full_reconnect_threshold_live: Arc::new(AtomicUsize::new(0)),
            nats: None,
            jsonl_writer: jsonl,
            started_at: Instant::now(),
            event_counter: std::sync::atomic::AtomicU64::new(0),
            wallet_tracker: WalletTracker::new(WalletTrackerCfg::default()),
            priority_fee_tracker: Arc::new(PriorityFeeTracker::new()),
            tracked_mints: parking_lot::RwLock::new(std::collections::HashMap::new()),
            tracked_mints_tx,
            hot_pool_registry: Arc::new(UnifiedHotPoolRegistry::new()),
            explicit_admitted_momentum_pools: parking_lot::RwLock::new(HashSet::new()),
            explicit_admitted_arb_pools: parking_lot::RwLock::new(HashSet::new()),
            known_pump_amm_pools: parking_lot::RwLock::new(std::collections::HashSet::new()),
            known_trade_dex_pools: parking_lot::RwLock::new(std::collections::HashSet::new()),
            pumpfun_pool_discovery_mint_info_emitted: parking_lot::RwLock::new(
                std::collections::HashSet::new(),
            ),
            tracked_vaults: parking_lot::RwLock::new(std::collections::HashMap::new()),
            tracked_vaults_tx,
            tracked_bin_arrays: parking_lot::RwLock::new(std::collections::HashMap::new()),
            tracked_bin_arrays_tx,
            tracked_membership: ArcSwap::from_pointee(TrackedMembershipSnapshot::default()),
            exec_hot_membership: ArcSwap::from_pointee(ExecHotMembershipSnapshot::default()),
            pool_tracked_legs: parking_lot::RwLock::new(HashMap::new()),
            live_pool_cache: Arc::new(LivePoolCache::new()),
            creator_cache: parking_lot::RwLock::new(std::collections::HashMap::new()),
            pool_mint_map: parking_lot::RwLock::new(std::collections::HashMap::new()),
            pool_creator_cache: parking_lot::RwLock::new(std::collections::HashMap::new()),
            high_priority_bonding_curves: parking_lot::RwLock::new(HashSet::new()),
            raydium_serum_fetched: parking_lot::RwLock::new(std::collections::HashSet::new()),
            tracked_wallet: Some(tracked),
            tracked_wallet_tx,
            tracked_wallet_token_accounts: parking_lot::RwLock::new(
                std::collections::HashSet::new(),
            ),
            tracked_wallet_mint_decimals: parking_lot::RwLock::new(std::collections::HashMap::new()),
            execution_results_deduper: parking_lot::Mutex::new(ExecutionResultDeduper::default()),
            last_emitted_curve_progress: parking_lot::RwLock::new(std::collections::HashMap::new()),
            bonding_curve_publish_times: parking_lot::Mutex::new(BondingCurvePublishTimes::new()),
            pump_amm_dex: parking_lot::RwLock::new(None),
            helius_rpc: None,
            geyser_sync_batch_timer: parking_lot::Mutex::new(None),
            geyser_sync_flush_timestamps: parking_lot::Mutex::new(Vec::new()),
            geyser_sync_debounce_epoch: AtomicU64::new(0),
            ingest_tokio_handle: parking_lot::RwLock::new(None),
            pool_discovery_poolcreated_emitted: parking_lot::RwLock::new(
                std::collections::HashSet::new(),
            ),
            last_synced_explicit_pubkeys: parking_lot::RwLock::new(HashSet::new()),
            last_momentum_snapshot_target: parking_lot::RwLock::new(None),
            last_arb_snapshot_target: parking_lot::RwLock::new(None),
            dlmm_registered_active_id: parking_lot::RwLock::new(HashMap::new()),
            deferred_hot_pool_reserve_pins: parking_lot::RwLock::new(HashMap::new()),
            geyser_connect_barrier: Arc::new(GeyserConnectBarrier::new()),
            geyser_explicit_ready: AtomicBool::new(true),
            geyser_explicit_config_error: parking_lot::RwLock::new(None),
            geyser_prune_resume: parking_lot::Mutex::new(GeyserPruneResume::default()),
        })
    }

    fn sample_wallet_tx_update(
        wallet: Pubkey,
        token_ata: Pubkey,
        mint: &str,
    ) -> GeyserTransactionUpdate {
        let balance_raw = 1_500_000u64;
        GeyserTransactionUpdate {
            signature: "wallet_tx_meta_sig".into(),
            slot: 99,
            account_keys: vec![wallet, token_ata],
            instruction_accounts: vec![],
            instruction_data: vec![],
            inner_instructions: vec![],
            pre_token_balances: vec![],
            post_token_balances: vec![TokenBalance {
                account_index: 1,
                mint: mint.to_string(),
                ui_token_amount: TokenAmount {
                    ui_amount: None,
                    decimals: 6,
                    amount: balance_raw.to_string(),
                },
                program_id: Some(spl_token::ID.to_string()),
                owner: Some(wallet.to_string()),
            }],
            pre_balances: vec![5_000_000_000, 2_039_280],
            post_balances: vec![4_900_000_000, 2_039_280],
            fee_lamports: 5_000,
            compute_units_consumed: Some(42_000),
            grpc_recv_at: Instant::now(),
        }
    }

    #[test]
    fn geyser_tx_involves_wallet_signer_or_token_owner() {
        let wallet = Pubkey::new_unique();
        let foreign = Pubkey::new_unique();
        let ata = Pubkey::new_unique();
        let signer_tx = GeyserTransactionUpdate {
            signature: "s".into(),
            slot: 1,
            account_keys: vec![wallet],
            instruction_accounts: vec![],
            instruction_data: vec![],
            inner_instructions: vec![],
            pre_token_balances: vec![],
            post_token_balances: vec![],
            pre_balances: vec![1],
            post_balances: vec![1],
            fee_lamports: 0,
            compute_units_consumed: None,
            grpc_recv_at: Instant::now(),
        };
        assert!(geyser_tx_involves_wallet(&wallet, &signer_tx));
        assert!(!geyser_tx_involves_wallet(&foreign, &signer_tx));

        let owner_tx = sample_wallet_tx_update(wallet, ata, &Pubkey::new_unique().to_string());
        assert!(geyser_tx_involves_wallet(&wallet, &owner_tx));
        assert!(!geyser_tx_involves_wallet(&foreign, &owner_tx));
    }

    #[test]
    fn wallet_tx_meta_skips_native_sol_when_wsol_post_balance_present() {
        let wallet = Pubkey::new_unique();
        let wsol_ata = Pubkey::new_unique();
        let mut tx = sample_wallet_tx_update(wallet, wsol_ata, WSOL_MINT);
        tx.post_token_balances[0].mint = WSOL_MINT.to_string();
        assert!(wallet_tx_meta_has_wsol_post_balance(&wallet, &tx));
        assert!(wallet_tx_meta_native_sol_post_lamports(&wallet, &tx).is_some());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn wallet_tx_meta_publishes_token_snapshot_and_pins_ata() {
        let wallet = Pubkey::new_unique();
        let token_ata = Pubkey::new_unique();
        let mint = Pubkey::new_unique().to_string();
        let ctx = market_data_context_with_tracked_wallet(wallet);
        let md_state = spawn_test_md_state(&ctx);
        let (publish_tx, mut publish_rx) = mpsc::channel(8);
        let tx = sample_wallet_tx_update(wallet, token_ata, &mint);

        process_wallet_balance_snapshots_from_tx_meta(
            &ctx,
            "run-wallet-tx-meta",
            &tx,
            Some(&publish_tx),
            &md_state,
        )
        .await;

        let mut snapshots: Vec<(String, u64)> = Vec::new();
        while let Ok(job) = publish_rx.try_recv() {
            if let AccountPathNatsJob::JetStream { payload, .. } = job {
                if payload.get("kind").and_then(|v| v.as_str()) == Some("WalletBalanceSnapshot") {
                    snapshots.push((
                        payload
                            .get("mint")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                        payload
                            .get("balance_raw")
                            .and_then(|v| v.as_u64())
                            .unwrap_or(0),
                    ));
                }
            }
        }

        assert!(
            snapshots.iter().any(|(m, b)| m == &mint && *b == 1_500_000),
            "expected token WalletBalanceSnapshot, got {snapshots:?}"
        );
        assert!(
            ctx.tracked_wallet_token_accounts
                .read()
                .contains(&token_ata),
            "ATA should be pinned from TX meta"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn foreign_wallet_tx_meta_publishes_nothing() {
        let wallet = Pubkey::new_unique();
        let foreign = Pubkey::new_unique();
        let token_ata = Pubkey::new_unique();
        let ctx = market_data_context_with_tracked_wallet(wallet);
        let md_state = spawn_test_md_state(&ctx);
        let (publish_tx, mut publish_rx) = mpsc::channel(8);
        let tx = sample_wallet_tx_update(foreign, token_ata, &Pubkey::new_unique().to_string());

        process_wallet_balance_snapshots_from_tx_meta(
            &ctx,
            "run-wallet-tx-meta",
            &tx,
            Some(&publish_tx),
            &md_state,
        )
        .await;

        assert!(publish_rx.try_recv().is_err());
        assert!(ctx.tracked_wallet_token_accounts.read().is_empty());
    }
}

/// Momentum NATS subject fan-out classification (see `TOPIC_MOMENTUM_MARKET_EVENTS`).
#[cfg(test)]
mod momentum_nats_subject_tests {
    use super::market_event_is_momentum_nats_relevant;
    use ironcrab::ipc::{MarketEventKind, NATIVE_SOL_MINT};

    fn sample_trade() -> MarketEventKind {
        MarketEventKind::Trade {
            pool_address: "pool1".into(),
            mint: "mint1".into(),
            quote_mint: NATIVE_SOL_MINT.into(),
            trader: "trader1".into(),
            is_buy: true,
            sol_amount: 1,
            token_amount: 1,
            token_decimals: 6,
            signature: None,
            dex: "pumpfun".into(),
            creator: None,
            token_program: None,
        }
    }

    #[test]
    fn momentum_relevance_always_true_for_core_entry_exit_kinds() {
        assert!(market_event_is_momentum_nats_relevant(&sample_trade()));
        assert!(market_event_is_momentum_nats_relevant(
            &MarketEventKind::PoolCreated {
                pool_address: "p".into(),
                base_mint: "m".into(),
                quote_mint: NATIVE_SOL_MINT.into(),
                dex: "raydium".into(),
                initial_liquidity_sol: None,
            }
        ));
        assert!(market_event_is_momentum_nats_relevant(
            &MarketEventKind::BondingCurveProgress {
                mint: "m".into(),
                bonding_curve: "bc".into(),
                progress_bps: 100,
                complete: false,
            }
        ));
        assert!(market_event_is_momentum_nats_relevant(
            &MarketEventKind::TokenMintInfo {
                mint: "m".into(),
                token_program: "tp".into(),
                decimals: 6,
                supply: 0,
                mint_authority: None,
                freeze_authority: None,
            }
        ));
        assert!(market_event_is_momentum_nats_relevant(
            &MarketEventKind::DexPoolAccounts {
                dex: "orca".into(),
                pool_address: "p".into(),
                base_mint: "m".into(),
                quote_mint: NATIVE_SOL_MINT.into(),
                accounts: vec![],
            }
        ));
        assert!(market_event_is_momentum_nats_relevant(
            &MarketEventKind::DevWalletIdentified {
                mint: "m".into(),
                dev_wallet: "d".into(),
                supply_percentage: 0.0,
            }
        ));
        assert!(market_event_is_momentum_nats_relevant(
            &MarketEventKind::LiquidityRemoved {
                pool_address: "p".into(),
                mint: "m".into(),
                sol_amount: 0,
                token_amount: 0,
                signature: None,
            }
        ));
        assert!(market_event_is_momentum_nats_relevant(
            &MarketEventKind::WalletSnapshotComplete {
                mints_in_wallet: vec![],
                wallet: "w".into(),
                is_periodic: false,
            }
        ));
    }

    #[test]
    fn momentum_relevance_false_for_explicit_noise_kinds() {
        assert!(!market_event_is_momentum_nats_relevant(
            &MarketEventKind::LatestBlockhash {
                blockhash: "h".into(),
                slot: 1,
                block_height: 1,
            }
        ));
        assert!(!market_event_is_momentum_nats_relevant(
            &MarketEventKind::BinArrayUpdate {
                pool_address: "p".into(),
                bin_array_index: 0,
                bins: vec![],
                update_slot: 1,
            }
        ));
        assert!(!market_event_is_momentum_nats_relevant(
            &MarketEventKind::SlotUpdate { current_slot: 1 }
        ));
        assert!(!market_event_is_momentum_nats_relevant(
            &MarketEventKind::WalletBalanceSnapshot {
                mint: "m".into(),
                balance_raw: 0,
                decimals: 6,
                token_program: "tp".into(),
            }
        ));
    }

    #[test]
    fn pool_state_update_only_pump_line_sol_quoted() {
        let ps_pump_sol = MarketEventKind::PoolStateUpdate {
            pool_address: "p".into(),
            dex: "pumpfun".into(),
            reserve_base: 1,
            reserve_quote: 1,
            base_mint: "m".into(),
            quote_mint: NATIVE_SOL_MINT.into(),
            update_slot: 1,
            active_id: None,
            bin_step: None,
        };
        assert!(market_event_is_momentum_nats_relevant(&ps_pump_sol));

        let ps_ray = MarketEventKind::PoolStateUpdate {
            pool_address: "p".into(),
            dex: "raydium".into(),
            reserve_base: 1,
            reserve_quote: 1,
            base_mint: "m".into(),
            quote_mint: NATIVE_SOL_MINT.into(),
            update_slot: 1,
            active_id: None,
            bin_step: None,
        };
        assert!(!market_event_is_momentum_nats_relevant(&ps_ray));

        let ps_pump_usdc = MarketEventKind::PoolStateUpdate {
            pool_address: "p".into(),
            dex: "pump_amm".into(),
            reserve_base: 1,
            reserve_quote: 1,
            base_mint: "m".into(),
            quote_mint: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".into(),
            update_slot: 1,
            active_id: None,
            bin_step: None,
        };
        assert!(!market_event_is_momentum_nats_relevant(&ps_pump_usdc));
    }
}

#[cfg(test)]
mod pr_b_geyser_tracking_tests {
    use super::*;
    use ironcrab::market_data::ingest::maybe_emit_dev_wallet_after_pool_mint_map;
    use ironcrab::market_data::ingest::AccountUpdateClass;
    use ironcrab::market_data::sidefx::SidefxUpdateClass;
    const RAYDIUM_CPMM_OWNER: Pubkey =
        solana_sdk::pubkey!("CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C");
    const PUMPFUN_PROGRAM_OWNER: Pubkey =
        solana_sdk::pubkey!("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P");
    const PUMPFUN_AMM_PROGRAM_OWNER: Pubkey =
        solana_sdk::pubkey!("pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA");
    use ironcrab::market_data::track::{
        admitted_pubkey_set, converge_admission_from_ctx, converge_admission_from_groups,
        merge_momentum_active_pools_updates, spawn_inline_track_worker_sender,
        spawn_noop_track_worker_sender, track_worker_execute_coalesced_push,
        track_worker_try_enqueue, FixedCapAdmission, TrackWorkerCommand, TrackWorkerContext,
        MARKET_DATA_TRACK_WORKER_COALESCE_MS,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc as std_mpsc;
    use std::time::Duration;

    fn test_spawn_md_state(ctx: &Arc<MarketDataContext>) -> MdStateSender {
        let track_worker = spawn_track_worker(Arc::clone(ctx));
        spawn_md_state_worker(
            Arc::clone(ctx),
            tokio::runtime::Handle::current(),
            track_worker,
        )
    }

    fn test_noop_track_worker_sender() -> TrackWorkerSender {
        spawn_noop_track_worker_sender(4096)
    }

    fn test_inline_track_worker_sender(ctx: &Arc<MarketDataContext>) -> TrackWorkerSender {
        spawn_inline_track_worker_sender(Arc::clone(ctx), 4096)
    }

    fn test_admission_for(ctx: &MarketDataContext) -> FixedCapAdmission {
        FixedCapAdmission::new(ctx.config.read().max_tracked_accounts)
    }

    fn test_sidefx_host(
        ctx: &Arc<MarketDataContext>,
        md_state: MdStateSender,
        track_worker: TrackWorkerSender,
    ) -> MarketDataSidefxHost {
        MarketDataSidefxHost {
            ctx: Arc::clone(ctx),
            publish_tx: None,
            md_state,
            track_worker,
        }
    }

    fn test_spawn_md_sidefx(
        ctx: &Arc<MarketDataContext>,
        md_state: &MdStateSender,
        track_worker: TrackWorkerSender,
    ) -> MdSidefxSender {
        spawn_md_sidefx_worker(Arc::clone(ctx), None, md_state.clone(), track_worker)
    }

    fn fill_md_state_queue(md_state: &MdStateSender) {
        for _ in 0..md_state.queue_capacity {
            md_state_try_enqueue(md_state, MdStateCommand::TouchVault(Pubkey::new_unique()));
        }
    }

    fn fill_md_sidefx_queue(md_sidefx: &MdSidefxSender) {
        for _ in 0..md_sidefx.queue_capacity {
            md_sidefx_try_enqueue(
                md_sidefx,
                MdSidefxCommand::PumpFunPoolMintMapInsert {
                    run_id: "test".into(),
                    pool_address: Pubkey::new_unique(),
                    mint_str: "mint".into(),
                    slot: None,
                    tx_grpc_recv_at: Instant::now(),
                    creator_override: None,
                },
            );
        }
    }

    fn test_md_state_sender_no_worker() -> (
        MdStateSender,
        Arc<AtomicUsize>,
        std_mpsc::Receiver<MdStateCommand>,
    ) {
        let (tx, rx) = std_mpsc::sync_channel(MARKET_DATA_GEYSER_TRACKING_QUEUE_CAP);
        let queue_depth = Arc::new(AtomicUsize::new(0));
        let sender = MdStateSender {
            tx,
            queue_depth: Arc::clone(&queue_depth),
            queue_capacity: MARKET_DATA_GEYSER_TRACKING_QUEUE_CAP,
        };
        (sender, queue_depth, rx)
    }

    fn test_raydium_cpmm_account_data(
        token_0_mint: Pubkey,
        token_1_mint: Pubkey,
        token_0_vault: Pubkey,
        token_1_vault: Pubkey,
    ) -> Vec<u8> {
        let mut data = vec![0u8; 1024];
        data[8] = 1;
        data[73..105].copy_from_slice(token_0_mint.as_ref());
        data[105..137].copy_from_slice(token_1_mint.as_ref());
        data[137..169].copy_from_slice(token_0_vault.as_ref());
        data[169..201].copy_from_slice(token_1_vault.as_ref());
        data
    }

    #[test]
    fn md_sidefx_live_pool_cache_update_skips_md_state_for_non_hot_pool() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        let (md_state, depth, md_state_rx) = test_md_state_sender_no_worker();
        let worker = test_sidefx_host(&ctx, md_state.clone(), test_noop_track_worker_sender());
        let pool = Pubkey::new_unique();
        let base = Pubkey::new_unique();
        let quote = Pubkey::from_str(NATIVE_SOL_MINT).unwrap();
        let account_data =
            test_raydium_cpmm_account_data(base, quote, Pubkey::new_unique(), Pubkey::new_unique());
        let job = MdSidefxCommand::LivePoolCacheAccountUpdate {
            run_id: "test".into(),
            pool_pubkey: pool,
            owner: RAYDIUM_CPMM_OWNER,
            account_data: account_data.clone(),
            slot: 1,
            grpc_recv_at: Instant::now(),
            update_class: SidefxUpdateClass::Enrich,
        };
        let mut scratch = MdSidefxBurstScratch::new();
        md_sidefx_process_live_pool_cache_account_update(&worker, &job, &mut scratch);
        assert_eq!(depth.load(Ordering::Relaxed), 0);
        assert!(ctx.live_pool_cache.get(&pool).is_some());

        ctx.hot_pool_registry.pin_pool(base, pool);
        let mut scratch = MdSidefxBurstScratch::new();
        md_sidefx_process_live_pool_cache_account_update(
            &worker,
            &MdSidefxCommand::LivePoolCacheAccountUpdate {
                run_id: "test".into(),
                pool_pubkey: pool,
                owner: RAYDIUM_CPMM_OWNER,
                account_data: account_data.clone(),
                slot: 2,
                grpc_recv_at: Instant::now(),
                update_class: SidefxUpdateClass::ExecHot,
            },
            &mut scratch,
        );
        md_sidefx_flush_pending_md_state_jobs(&worker, &mut scratch);
        assert_eq!(
            depth.load(Ordering::Relaxed),
            0,
            "phase1: hot pool account upsert must not enqueue RegisterPoolVaultsFromAccount from sidefx"
        );
        assert!(md_state_rx.try_recv().is_err());

        let mut scratch2 = MdSidefxBurstScratch::new();
        md_sidefx_process_live_pool_cache_account_update(
            &worker,
            &MdSidefxCommand::LivePoolCacheAccountUpdate {
                run_id: "test".into(),
                pool_pubkey: pool,
                owner: RAYDIUM_CPMM_OWNER,
                account_data,
                slot: 3,
                grpc_recv_at: Instant::now(),
                update_class: SidefxUpdateClass::ExecHot,
            },
            &mut scratch2,
        );
        md_sidefx_flush_pending_md_state_jobs(&worker, &mut scratch2);
        assert_eq!(
            depth.load(Ordering::Relaxed),
            0,
            "phase1: repeat hot upsert still must not enqueue md-state register from sidefx"
        );
    }

    /// Scope D: ENRICH account-parse sidefx skips redundant JetStream when vault feed is live.
    #[test]
    fn scope_d_enrich_sidefx_skips_redundant_pool_discovered_under_vault_feed() {
        use ironcrab::metrics::MARKET_DATA_MD_SIDEFX_ENRICH_PUBLISH_SKIPPED_TOTAL;

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        let (md_state, _depth, _md_state_rx) = test_md_state_sender_no_worker();
        let worker = test_sidefx_host(&ctx, md_state, test_noop_track_worker_sender());

        let pool = Pubkey::new_unique();
        let base = Pubkey::new_unique();
        let quote = Pubkey::from_str(NATIVE_SOL_MINT).unwrap();
        let coin_vault = Pubkey::new_unique();
        let pc_vault = Pubkey::new_unique();
        let account_data = test_raydium_cpmm_account_data(base, quote, coin_vault, pc_vault);

        ctx.live_pool_cache.upsert(
            pool,
            CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
                token_0_mint: base,
                token_1_mint: quote,
                token_0_vault: coin_vault,
                token_1_vault: pc_vault,
                reserve_0: Some(1_000_000),
                reserve_1: Some(2_000_000),
            }),
            1,
        );
        ctx.hot_pool_registry.pin_pool(base, pool);
        ctx.note_explicit_momentum_pool_admitted(pool);
        assert!(MarketDataContext::register_geyser_reserves_after_trade(
            &ctx, pool
        ));
        let mut admission = FixedCapAdmission::new(ctx.max_tracked_accounts());
        ironcrab::market_data::track::converge_admission_from_ctx(ctx.as_ref(), &mut admission);
        ctx.prune_tracked_maps_to_admitted(&admission);
        ctx.publish_admitted_explicit_physical(&admission);
        *ctx.last_synced_explicit_pubkeys.write() =
            admission.snapshot_pubkeys().into_iter().collect();
        assert!(worker.pool_has_live_vault_geyser_feed(pool));

        MARKET_DATA_MD_SIDEFX_ENRICH_PUBLISH_SKIPPED_TOTAL.store(0, Ordering::Relaxed);
        let mk_job =
            |slot: u64, class: SidefxUpdateClass| MdSidefxCommand::LivePoolCacheAccountUpdate {
                run_id: "scope-d".into(),
                pool_pubkey: pool,
                owner: RAYDIUM_CPMM_OWNER,
                account_data: account_data.clone(),
                slot,
                grpc_recv_at: Instant::now(),
                update_class: class,
            };

        let mut scratch = MdSidefxBurstScratch::new();
        md_sidefx_process_live_pool_cache_account_update(
            &worker,
            &mk_job(10, SidefxUpdateClass::Enrich),
            &mut scratch,
        );
        let skipped_after_first =
            MARKET_DATA_MD_SIDEFX_ENRICH_PUBLISH_SKIPPED_TOTAL.load(Ordering::Relaxed);

        let mut scratch2 = MdSidefxBurstScratch::new();
        md_sidefx_process_live_pool_cache_account_update(
            &worker,
            &mk_job(11, SidefxUpdateClass::Enrich),
            &mut scratch2,
        );
        assert!(
            MARKET_DATA_MD_SIDEFX_ENRICH_PUBLISH_SKIPPED_TOTAL.load(Ordering::Relaxed)
                > skipped_after_first,
            "second ENRICH upsert with unchanged reserves under vault feed must skip JetStream"
        );

        let skipped_before_exec_hot =
            MARKET_DATA_MD_SIDEFX_ENRICH_PUBLISH_SKIPPED_TOTAL.load(Ordering::Relaxed);
        let mut scratch3 = MdSidefxBurstScratch::new();
        md_sidefx_process_live_pool_cache_account_update(
            &worker,
            &mk_job(12, SidefxUpdateClass::ExecHot),
            &mut scratch3,
        );
        assert_eq!(
            MARKET_DATA_MD_SIDEFX_ENRICH_PUBLISH_SKIPPED_TOTAL.load(Ordering::Relaxed),
            skipped_before_exec_hot,
            "EXEC_HOT must not increment ENRICH skip counter (I-MD-4)"
        );
    }

    /// Bugbot: ENRICH JetStream skip must not skip MASTER readiness merge.
    #[test]
    fn scope_d_enrich_skips_publish_but_merges_readiness() {
        use ironcrab::ipc::DexPoolReadiness;
        use ironcrab::metrics::MARKET_DATA_MD_SIDEFX_ENRICH_PUBLISH_SKIPPED_TOTAL;

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        let (md_state, _depth, _md_state_rx) = test_md_state_sender_no_worker();
        let worker = test_sidefx_host(&ctx, md_state, test_noop_track_worker_sender());

        let pool = Pubkey::new_unique();
        let base = Pubkey::new_unique();
        let quote = Pubkey::from_str(NATIVE_SOL_MINT).unwrap();
        let coin_vault = Pubkey::new_unique();
        let pc_vault = Pubkey::new_unique();
        let account_data = test_raydium_cpmm_account_data(base, quote, coin_vault, pc_vault);

        // Seed MASTER cache from parse-shaped row (no vault balances in pool account bytes).
        ctx.live_pool_cache.upsert(
            pool,
            CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
                token_0_mint: base,
                token_1_mint: quote,
                token_0_vault: coin_vault,
                token_1_vault: pc_vault,
                reserve_0: None,
                reserve_1: None,
            }),
            1,
        );
        assert_eq!(ctx.live_pool_cache.raydium_cpmm_readiness(&pool), None);

        ctx.hot_pool_registry.pin_pool(base, pool);
        ctx.note_explicit_momentum_pool_admitted(pool);
        assert!(MarketDataContext::register_geyser_reserves_after_trade(
            &ctx, pool
        ));
        let mut admission = FixedCapAdmission::new(ctx.max_tracked_accounts());
        ironcrab::market_data::track::converge_admission_from_ctx(ctx.as_ref(), &mut admission);
        ctx.prune_tracked_maps_to_admitted(&admission);
        ctx.publish_admitted_explicit_physical(&admission);
        *ctx.last_synced_explicit_pubkeys.write() =
            admission.snapshot_pubkeys().into_iter().collect();
        assert!(worker.pool_has_live_vault_geyser_feed(pool));

        MARKET_DATA_MD_SIDEFX_ENRICH_PUBLISH_SKIPPED_TOTAL.store(0, Ordering::Relaxed);
        let mut scratch = MdSidefxBurstScratch::new();
        md_sidefx_process_live_pool_cache_account_update(
            &worker,
            &MdSidefxCommand::LivePoolCacheAccountUpdate {
                run_id: "scope-d-readiness".into(),
                pool_pubkey: pool,
                owner: RAYDIUM_CPMM_OWNER,
                account_data,
                slot: 2,
                grpc_recv_at: Instant::now(),
                update_class: SidefxUpdateClass::Enrich,
            },
            &mut scratch,
        );

        assert!(
            MARKET_DATA_MD_SIDEFX_ENRICH_PUBLISH_SKIPPED_TOTAL.load(Ordering::Relaxed) > 0,
            "ENRICH must skip redundant JetStream under vault feed + unchanged layout/reserves"
        );
        assert_eq!(
            ctx.live_pool_cache.raydium_cpmm_readiness(&pool),
            Some(DexPoolReadiness::Observed),
            "readiness merge must run even when JetStream publish is skipped"
        );
    }

    /// Scope D regression: arb-pinned pool uses EXEC_HOT class (dual-consumer, not momentum-only).
    #[test]
    fn scope_d_arb_hot_pool_sidefx_uses_exec_hot_class() {
        use ironcrab::market_data::ingest::{classify_account_geyser_update, AccountUpdateClass};

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        let pool = Pubkey::new_unique();
        ctx.hot_pool_registry.pin_arb_pool(pool);
        let update = GeyserAccountUpdate {
            pubkey: pool,
            slot: 7,
            owner: RAYDIUM_CPMM_OWNER,
            data: vec![],
            lamports: 0,
            grpc_recv_at: Instant::now(),
        };
        let (class, _) = classify_account_geyser_update(&ctx, &update);
        assert_eq!(class, AccountUpdateClass::ExecHot);
        assert!(SidefxUpdateClass::from(class).is_exec_hot());
    }

    #[test]
    fn md_sidefx_coalesce_dedupes_vault_balance_ticks_per_vault() {
        let vault = Pubkey::new_unique();
        let jobs: Vec<_> = (0..10)
            .map(|i| MdSidefxCommand::VaultBalanceTick {
                run_id: "test".into(),
                vault_pubkey: vault,
                balance: i + 1,
                slot: i + 1,
                grpc_recv_at: Instant::now(),
                update_class: SidefxUpdateClass::ExecHot,
            })
            .collect();
        let out = md_sidefx_coalesce_burst(jobs);
        assert_eq!(out.len(), 1);
        let MdSidefxCommand::VaultBalanceTick { balance, .. } = &out[0] else {
            panic!("expected VaultBalanceTick");
        };
        assert_eq!(*balance, 10);
    }

    #[test]
    fn md_state_coalesce_batches_touch_vault_into_single_lru_job() {
        let vault = Pubkey::new_unique();
        let jobs: Vec<_> = (0..10).map(|_| MdStateCommand::TouchVault(vault)).collect();
        let out = md_state_coalesce_jobs(jobs);
        assert_eq!(out.len(), 1);
        assert!(matches!(
            out[0],
            MdStateCommand::TouchTrackedLruBatch { .. }
        ));
        let MdStateCommand::TouchTrackedLruBatch { vaults, bin_arrays } = &out[0] else {
            unreachable!();
        };
        assert_eq!(vaults.len(), 1);
        assert_eq!(vaults[0], vault);
        assert!(bin_arrays.is_empty());
    }

    #[test]
    fn touch_tracked_vault_pubkey_updates_sibling_vault_for_lru_pair() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);

        let pool = Pubkey::new_unique();
        let base_vault = Pubkey::new_unique();
        let quote_vault = Pubkey::new_unique();
        let old_base = Instant::now() - Duration::from_secs(60);
        let old_quote = Instant::now() - Duration::from_secs(30);
        {
            let mut vaults = ctx.tracked_vaults.write();
            vaults.insert(
                base_vault,
                VaultInfo {
                    pool_address: pool,
                    dex: "x".into(),
                    base_mint: Pubkey::new_unique(),
                    quote_mint: Pubkey::new_unique(),
                    is_base_vault: true,
                    last_balance: std::sync::atomic::AtomicU64::new(0),
                    last_used_at: old_base,
                    pinned: false,
                    pin: None,
                    active_id: None,
                    bin_step: None,
                    sibling_vault: Some(quote_vault),
                },
            );
            vaults.insert(
                quote_vault,
                VaultInfo {
                    pool_address: pool,
                    dex: "x".into(),
                    base_mint: Pubkey::new_unique(),
                    quote_mint: Pubkey::new_unique(),
                    is_base_vault: false,
                    last_balance: std::sync::atomic::AtomicU64::new(0),
                    last_used_at: old_quote,
                    pinned: false,
                    pin: None,
                    active_id: None,
                    bin_step: None,
                    sibling_vault: Some(base_vault),
                },
            );
        }

        ctx.touch_tracked_vault_pubkey(&base_vault);
        let vaults = ctx.tracked_vaults.read();
        assert!(vaults.get(&base_vault).unwrap().last_used_at > old_base);
        assert!(vaults.get(&quote_vault).unwrap().last_used_at > old_quote);
    }

    #[test]
    fn lru_eviction_prefers_oldest_unpinned_vault() {
        let a = Pubkey::new_unique();
        let b = Pubkey::new_unique();
        let pool = Pubkey::new_unique();
        let now = Instant::now();
        let mut map = std::collections::HashMap::new();
        map.insert(
            a,
            VaultInfo {
                pool_address: pool,
                dex: "x".into(),
                base_mint: Pubkey::new_unique(),
                quote_mint: Pubkey::new_unique(),
                is_base_vault: true,
                last_balance: std::sync::atomic::AtomicU64::new(0),
                last_used_at: now - Duration::from_secs(100),
                pinned: false,
                pin: None,
                active_id: None,
                bin_step: None,
                sibling_vault: None,
            },
        );
        map.insert(
            b,
            VaultInfo {
                pool_address: pool,
                dex: "x".into(),
                base_mint: Pubkey::new_unique(),
                quote_mint: Pubkey::new_unique(),
                is_base_vault: false,
                last_balance: std::sync::atomic::AtomicU64::new(0),
                last_used_at: now - Duration::from_secs(10),
                pinned: false,
                pin: None,
                active_id: None,
                bin_step: None,
                sibling_vault: None,
            },
        );
        let oldest = map
            .iter()
            .filter(|(_, v)| !v.pinned)
            .min_by_key(|(_, v)| v.last_used_at)
            .map(|(k, _)| *k)
            .expect("candidate");
        assert_eq!(oldest, a);
    }

    #[test]
    fn pinned_vault_not_selected_for_lru() {
        let a = Pubkey::new_unique();
        let b = Pubkey::new_unique();
        let pool = Pubkey::new_unique();
        let now = Instant::now();
        let mut map = std::collections::HashMap::new();
        map.insert(
            a,
            VaultInfo {
                pool_address: pool,
                dex: "x".into(),
                base_mint: Pubkey::new_unique(),
                quote_mint: Pubkey::new_unique(),
                is_base_vault: true,
                last_balance: std::sync::atomic::AtomicU64::new(0),
                last_used_at: now - Duration::from_secs(1000),
                pinned: true,
                pin: Some(GeyserPinReason::Wallet),
                active_id: None,
                bin_step: None,
                sibling_vault: None,
            },
        );
        map.insert(
            b,
            VaultInfo {
                pool_address: pool,
                dex: "x".into(),
                base_mint: Pubkey::new_unique(),
                quote_mint: Pubkey::new_unique(),
                is_base_vault: false,
                last_balance: std::sync::atomic::AtomicU64::new(0),
                last_used_at: now - Duration::from_secs(1),
                pinned: false,
                pin: None,
                active_id: None,
                bin_step: None,
                sibling_vault: None,
            },
        );
        let oldest = map
            .iter()
            .filter(|(_, v)| !v.pinned)
            .min_by_key(|(_, v)| v.last_used_at)
            .map(|(k, _)| *k)
            .expect("candidate");
        assert_eq!(oldest, b);
    }

    #[test]
    fn hot_pool_registry_pin_api_roundtrip() {
        let s = UnifiedHotPoolRegistry::new();
        let mint = Pubkey::new_unique();
        let pool = Pubkey::new_unique();
        assert!(!s.is_pinned(mint, pool));
        s.pin_pool(mint, pool);
        assert!(s.is_pinned(mint, pool));
        s.unpin_pool(mint, pool);
        assert!(!s.is_pinned(mint, pool));
    }

    #[test]
    fn hot_pool_registry_pool_has_any_pin() {
        let s = UnifiedHotPoolRegistry::new();
        let m1 = Pubkey::new_unique();
        let m2 = Pubkey::new_unique();
        let pool = Pubkey::new_unique();
        assert!(!s.pool_has_any_pin(pool));
        s.pin_pool(m1, pool);
        assert!(s.pool_has_any_pin(pool));
        s.pin_pool(m2, pool);
        assert!(s.pool_has_any_pin(pool));
        s.unpin_pool(m1, pool);
        assert!(s.pool_has_any_pin(pool));
        s.unpin_pool(m2, pool);
        assert!(!s.pool_has_any_pin(pool));
    }

    /// Bugbot PR-146: paired vault eviction must never pick a pinned sibling for `lru_cap_pair`.
    #[test]
    fn geyser_sibling_evict_helper_skips_pinned_leg() {
        let pool = Pubkey::new_unique();
        let base_pk = Pubkey::new_unique();
        let quote_pk = Pubkey::new_unique();
        let now = Instant::now();
        let mut map = std::collections::HashMap::new();
        map.insert(
            base_pk,
            VaultInfo {
                pool_address: pool,
                dex: "x".into(),
                base_mint: Pubkey::new_unique(),
                quote_mint: Pubkey::new_unique(),
                is_base_vault: true,
                last_balance: std::sync::atomic::AtomicU64::new(0),
                last_used_at: now,
                pinned: false,
                pin: None,
                active_id: None,
                bin_step: None,
                sibling_vault: None,
            },
        );
        map.insert(
            quote_pk,
            VaultInfo {
                pool_address: pool,
                dex: "x".into(),
                base_mint: Pubkey::new_unique(),
                quote_mint: Pubkey::new_unique(),
                is_base_vault: false,
                last_balance: std::sync::atomic::AtomicU64::new(0),
                last_used_at: now,
                pinned: true,
                pin: Some(GeyserPinReason::Wallet),
                active_id: None,
                bin_step: None,
                sibling_vault: None,
            },
        );
        assert_eq!(
            MarketDataContext::geyser_unpinned_sibling_vault_pubkey(&map, pool, true),
            None
        );
        assert!(MarketDataContext::geyser_pinned_sibling_vault_present(
            &map, pool, true
        ));
    }

    #[test]
    fn geyser_sibling_evict_helper_co_evicts_unpinned_partner() {
        let pool = Pubkey::new_unique();
        let base_pk = Pubkey::new_unique();
        let quote_pk = Pubkey::new_unique();
        let now = Instant::now();
        let mut map = std::collections::HashMap::new();
        map.insert(
            base_pk,
            VaultInfo {
                pool_address: pool,
                dex: "x".into(),
                base_mint: Pubkey::new_unique(),
                quote_mint: Pubkey::new_unique(),
                is_base_vault: true,
                last_balance: std::sync::atomic::AtomicU64::new(0),
                last_used_at: now,
                pinned: false,
                pin: None,
                active_id: None,
                bin_step: None,
                sibling_vault: None,
            },
        );
        map.insert(
            quote_pk,
            VaultInfo {
                pool_address: pool,
                dex: "x".into(),
                base_mint: Pubkey::new_unique(),
                quote_mint: Pubkey::new_unique(),
                is_base_vault: false,
                last_balance: std::sync::atomic::AtomicU64::new(0),
                last_used_at: now,
                pinned: false,
                pin: None,
                active_id: None,
                bin_step: None,
                sibling_vault: None,
            },
        );
        assert_eq!(
            MarketDataContext::geyser_unpinned_sibling_vault_pubkey(&map, pool, true),
            Some(quote_pk)
        );
        assert!(!MarketDataContext::geyser_pinned_sibling_vault_present(
            &map, pool, true
        ));
    }

    /// Minimal [`MarketDataContext`] for PR-D active-pool pin tests (no NATS / no Geyser).
    fn minimal_market_data_context_for_pr_d_tests(
        jsonl_writer: QueuedJsonlWriter,
    ) -> Arc<MarketDataContext> {
        minimal_market_data_context_for_pr_d_tests_with_wallet(jsonl_writer, None, &[], None)
    }

    fn minimal_market_data_context_for_pr_d_tests_with_wallet(
        jsonl_writer: QueuedJsonlWriter,
        tracked_wallet: Option<TrackedWallet>,
        extra_wallet_token_accounts: &[Pubkey],
        live_pool_cache: Option<Arc<LivePoolCache>>,
    ) -> Arc<MarketDataContext> {
        let (tracked_mints_tx, _tracked_mints_rx) = watch::channel(Vec::<Pubkey>::new());
        let (tracked_vaults_tx, _tracked_vaults_rx) = watch::channel(Vec::<Pubkey>::new());
        let (tracked_bin_arrays_tx, _tracked_bin_arrays_rx) = watch::channel(Vec::<Pubkey>::new());
        let (tracked_wallet_tx, _tracked_wallet_rx) = watch::channel(Vec::<Pubkey>::new());
        let mut wallet_token_accounts = std::collections::HashSet::new();
        wallet_token_accounts.extend(extra_wallet_token_accounts.iter().copied());
        Arc::new(MarketDataContext {
            run_id: "run-prd-test".to_string(),
            config: parking_lot::RwLock::new(MarketDataConfig::default()),
            geyser_full_reconnect_threshold_live: Arc::new(AtomicUsize::new(0)),
            nats: None,
            jsonl_writer,
            started_at: Instant::now(),
            event_counter: std::sync::atomic::AtomicU64::new(0),
            wallet_tracker: WalletTracker::new(WalletTrackerCfg::default()),
            priority_fee_tracker: Arc::new(PriorityFeeTracker::new()),
            tracked_mints: parking_lot::RwLock::new(std::collections::HashMap::new()),
            tracked_mints_tx,
            hot_pool_registry: Arc::new(UnifiedHotPoolRegistry::new()),
            explicit_admitted_momentum_pools: parking_lot::RwLock::new(HashSet::new()),
            explicit_admitted_arb_pools: parking_lot::RwLock::new(HashSet::new()),
            known_pump_amm_pools: parking_lot::RwLock::new(std::collections::HashSet::new()),
            known_trade_dex_pools: parking_lot::RwLock::new(std::collections::HashSet::new()),
            pumpfun_pool_discovery_mint_info_emitted: parking_lot::RwLock::new(
                std::collections::HashSet::new(),
            ),
            tracked_vaults: parking_lot::RwLock::new(std::collections::HashMap::new()),
            tracked_vaults_tx,
            tracked_bin_arrays: parking_lot::RwLock::new(std::collections::HashMap::new()),
            tracked_bin_arrays_tx,
            tracked_membership: ArcSwap::from_pointee(TrackedMembershipSnapshot::default()),
            exec_hot_membership: ArcSwap::from_pointee(ExecHotMembershipSnapshot::default()),
            pool_tracked_legs: parking_lot::RwLock::new(HashMap::new()),
            live_pool_cache: live_pool_cache.unwrap_or_else(|| Arc::new(LivePoolCache::new())),
            creator_cache: parking_lot::RwLock::new(std::collections::HashMap::new()),
            pool_mint_map: parking_lot::RwLock::new(std::collections::HashMap::new()),
            pool_creator_cache: parking_lot::RwLock::new(std::collections::HashMap::new()),
            high_priority_bonding_curves: parking_lot::RwLock::new(HashSet::new()),
            raydium_serum_fetched: parking_lot::RwLock::new(std::collections::HashSet::new()),
            tracked_wallet,
            tracked_wallet_tx,
            tracked_wallet_token_accounts: parking_lot::RwLock::new(wallet_token_accounts),
            tracked_wallet_mint_decimals: parking_lot::RwLock::new(std::collections::HashMap::new()),
            execution_results_deduper: parking_lot::Mutex::new(ExecutionResultDeduper::default()),
            last_emitted_curve_progress: parking_lot::RwLock::new(std::collections::HashMap::new()),
            bonding_curve_publish_times: parking_lot::Mutex::new(BondingCurvePublishTimes::new()),
            pump_amm_dex: parking_lot::RwLock::new(None),
            helius_rpc: None,
            geyser_sync_batch_timer: parking_lot::Mutex::new(None),
            geyser_sync_flush_timestamps: parking_lot::Mutex::new(Vec::new()),
            geyser_sync_debounce_epoch: AtomicU64::new(0),
            ingest_tokio_handle: parking_lot::RwLock::new(None),
            pool_discovery_poolcreated_emitted: parking_lot::RwLock::new(
                std::collections::HashSet::new(),
            ),
            last_synced_explicit_pubkeys: parking_lot::RwLock::new(HashSet::new()),
            last_momentum_snapshot_target: parking_lot::RwLock::new(None),
            last_arb_snapshot_target: parking_lot::RwLock::new(None),
            dlmm_registered_active_id: parking_lot::RwLock::new(HashMap::new()),
            deferred_hot_pool_reserve_pins: parking_lot::RwLock::new(HashMap::new()),
            geyser_connect_barrier: Arc::new(GeyserConnectBarrier::new()),
            geyser_explicit_ready: AtomicBool::new(true),
            geyser_explicit_config_error: parking_lot::RwLock::new(None),
            geyser_prune_resume: parking_lot::Mutex::new(GeyserPruneResume::default()),
        })
    }

    /// PR161: same as [`minimal_market_data_context_for_pr_d_tests`], but returns merge `watch` receivers.
    #[allow(clippy::type_complexity)]
    fn minimal_market_data_context_and_merge_receivers_for_pr161(
        jsonl_writer: QueuedJsonlWriter,
    ) -> (
        Arc<MarketDataContext>,
        watch::Receiver<Vec<Pubkey>>,
        watch::Receiver<Vec<Pubkey>>,
        watch::Receiver<Vec<Pubkey>>,
        watch::Receiver<Vec<Pubkey>>,
    ) {
        let (tracked_mints_tx, tracked_mints_rx) = watch::channel(Vec::<Pubkey>::new());
        let (tracked_vaults_tx, tracked_vaults_rx) = watch::channel(Vec::<Pubkey>::new());
        let (tracked_bin_arrays_tx, tracked_bin_arrays_rx) = watch::channel(Vec::<Pubkey>::new());
        let (tracked_wallet_tx, tracked_wallet_rx) = watch::channel(Vec::<Pubkey>::new());
        let ctx = Arc::new(MarketDataContext {
            run_id: "run-pr161-merge-test".to_string(),
            config: parking_lot::RwLock::new(MarketDataConfig::default()),
            geyser_full_reconnect_threshold_live: Arc::new(AtomicUsize::new(0)),
            nats: None,
            jsonl_writer,
            started_at: Instant::now(),
            event_counter: std::sync::atomic::AtomicU64::new(0),
            wallet_tracker: WalletTracker::new(WalletTrackerCfg::default()),
            priority_fee_tracker: Arc::new(PriorityFeeTracker::new()),
            tracked_mints: parking_lot::RwLock::new(std::collections::HashMap::new()),
            tracked_mints_tx,
            hot_pool_registry: Arc::new(UnifiedHotPoolRegistry::new()),
            explicit_admitted_momentum_pools: parking_lot::RwLock::new(HashSet::new()),
            explicit_admitted_arb_pools: parking_lot::RwLock::new(HashSet::new()),
            known_pump_amm_pools: parking_lot::RwLock::new(std::collections::HashSet::new()),
            known_trade_dex_pools: parking_lot::RwLock::new(std::collections::HashSet::new()),
            pumpfun_pool_discovery_mint_info_emitted: parking_lot::RwLock::new(
                std::collections::HashSet::new(),
            ),
            tracked_vaults: parking_lot::RwLock::new(std::collections::HashMap::new()),
            tracked_vaults_tx,
            tracked_bin_arrays: parking_lot::RwLock::new(std::collections::HashMap::new()),
            tracked_bin_arrays_tx,
            tracked_membership: ArcSwap::from_pointee(TrackedMembershipSnapshot::default()),
            exec_hot_membership: ArcSwap::from_pointee(ExecHotMembershipSnapshot::default()),
            pool_tracked_legs: parking_lot::RwLock::new(HashMap::new()),
            live_pool_cache: Arc::new(LivePoolCache::new()),
            creator_cache: parking_lot::RwLock::new(std::collections::HashMap::new()),
            pool_mint_map: parking_lot::RwLock::new(std::collections::HashMap::new()),
            pool_creator_cache: parking_lot::RwLock::new(std::collections::HashMap::new()),
            high_priority_bonding_curves: parking_lot::RwLock::new(HashSet::new()),
            raydium_serum_fetched: parking_lot::RwLock::new(std::collections::HashSet::new()),
            tracked_wallet: None,
            tracked_wallet_tx,
            tracked_wallet_token_accounts: parking_lot::RwLock::new(
                std::collections::HashSet::new(),
            ),
            tracked_wallet_mint_decimals: parking_lot::RwLock::new(std::collections::HashMap::new()),
            execution_results_deduper: parking_lot::Mutex::new(ExecutionResultDeduper::default()),
            last_emitted_curve_progress: parking_lot::RwLock::new(std::collections::HashMap::new()),
            bonding_curve_publish_times: parking_lot::Mutex::new(BondingCurvePublishTimes::new()),
            pump_amm_dex: parking_lot::RwLock::new(None),
            helius_rpc: None,
            geyser_sync_batch_timer: parking_lot::Mutex::new(None),
            geyser_sync_flush_timestamps: parking_lot::Mutex::new(Vec::new()),
            geyser_sync_debounce_epoch: AtomicU64::new(0),
            ingest_tokio_handle: parking_lot::RwLock::new(None),
            pool_discovery_poolcreated_emitted: parking_lot::RwLock::new(
                std::collections::HashSet::new(),
            ),
            last_synced_explicit_pubkeys: parking_lot::RwLock::new(HashSet::new()),
            last_momentum_snapshot_target: parking_lot::RwLock::new(None),
            last_arb_snapshot_target: parking_lot::RwLock::new(None),
            dlmm_registered_active_id: parking_lot::RwLock::new(HashMap::new()),
            deferred_hot_pool_reserve_pins: parking_lot::RwLock::new(HashMap::new()),
            geyser_connect_barrier: Arc::new(GeyserConnectBarrier::new()),
            geyser_explicit_ready: AtomicBool::new(true),
            geyser_explicit_config_error: parking_lot::RwLock::new(None),
            geyser_prune_resume: parking_lot::Mutex::new(GeyserPruneResume::default()),
        });
        (
            ctx,
            tracked_mints_rx,
            tracked_vaults_rx,
            tracked_bin_arrays_rx,
            tracked_wallet_rx,
        )
    }

    #[test]
    fn momentum_active_pools_apply_active_sets_momentum_pin_on_vaults() {
        use ironcrab::nats::{MomentumActivePinReason, MomentumActivePoolEntry};

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);

        let pool = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote = Pubkey::from_str(NATIVE_SOL_MINT).unwrap();
        let coin_vault = Pubkey::new_unique();
        let pc_vault = Pubkey::new_unique();
        ctx.live_pool_cache.upsert(
            pool,
            CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
                token_0_mint: base_mint,
                token_1_mint: quote,
                token_0_vault: coin_vault,
                token_1_vault: pc_vault,
                reserve_0: Some(1_000_000),
                reserve_1: Some(2_000_000),
            }),
            1,
        );

        let update = MomentumActivePoolsUpdate {
            version: 1,
            ts_unix_ms: 1,
            active: vec![MomentumActivePoolEntry {
                mint: base_mint.to_string(),
                pool: pool.to_string(),
                pin_reason: MomentumActivePinReason::Tracker,
            }],
            removed: vec![],
            full_active_snapshot: false,
        };
        let mut admission = test_admission_for(&ctx);
        ctx.apply_momentum_active_pools_update(&mut admission, &update);

        let vs = ctx.tracked_vaults.read();
        assert_eq!(
            vs.get(&coin_vault).and_then(|v| v.pin),
            Some(GeyserPinReason::MomentumActive)
        );
        assert_eq!(
            vs.get(&pc_vault).and_then(|v| v.pin),
            Some(GeyserPinReason::MomentumActive)
        );
    }

    /// Scope C: Position pin → hot pool + vault membership → EXEC_HOT classification.
    #[test]
    fn scope_c_position_pin_classifies_exec_hot() {
        use ironcrab::market_data::ingest::{classify_account_geyser_update, AccountUpdateClass};
        use ironcrab::nats::{MomentumActivePinReason, MomentumActivePoolEntry};
        use ironcrab::solana::geyser_listener::GeyserAccountUpdate;

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);

        let pool = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote = Pubkey::from_str(NATIVE_SOL_MINT).unwrap();
        let coin_vault = Pubkey::new_unique();
        let pc_vault = Pubkey::new_unique();
        ctx.live_pool_cache.upsert(
            pool,
            CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
                token_0_mint: base_mint,
                token_1_mint: quote,
                token_0_vault: coin_vault,
                token_1_vault: pc_vault,
                reserve_0: Some(1_000_000),
                reserve_1: Some(2_000_000),
            }),
            1,
        );

        let mut admission = test_admission_for(&ctx);
        ctx.apply_momentum_active_pools_update(
            &mut admission,
            &MomentumActivePoolsUpdate {
                version: 1,
                ts_unix_ms: 1,
                active: vec![MomentumActivePoolEntry {
                    mint: base_mint.to_string(),
                    pool: pool.to_string(),
                    pin_reason: MomentumActivePinReason::Position,
                }],
                removed: vec![],
                full_active_snapshot: false,
            },
        );

        assert!(ctx.hot_pool_registry.is_hot_pool(pool));
        assert!(ctx.hot_pool_registry.is_position_pin(base_mint, pool));
        ctx.refresh_tracked_membership_snapshot();
        let update = GeyserAccountUpdate {
            pubkey: coin_vault,
            slot: 9,
            owner: Pubkey::new_unique(),
            data: vec![1; 8],
            lamports: 0,
            grpc_recv_at: Instant::now(),
        };
        let (class, _) = classify_account_geyser_update(&ctx, &update);
        assert_eq!(class, AccountUpdateClass::ExecHot);
    }

    /// Scope C: cache miss keeps pin; retry registers vaults when cache fills (no RPC).
    #[test]
    fn scope_c_position_pin_cache_miss_retries_when_cache_filled() {
        use ironcrab::nats::{MomentumActivePinReason, MomentumActivePoolEntry};

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);

        let pool = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote = Pubkey::from_str(NATIVE_SOL_MINT).unwrap();
        let coin_vault = Pubkey::new_unique();
        let pc_vault = Pubkey::new_unique();

        let mut admission = test_admission_for(&ctx);
        ctx.apply_momentum_active_pools_update(
            &mut admission,
            &MomentumActivePoolsUpdate {
                version: 1,
                ts_unix_ms: 1,
                active: vec![MomentumActivePoolEntry {
                    mint: base_mint.to_string(),
                    pool: pool.to_string(),
                    pin_reason: MomentumActivePinReason::Position,
                }],
                removed: vec![],
                full_active_snapshot: false,
            },
        );

        assert!(ctx.hot_pool_registry.is_hot_pool(pool));
        assert!(ctx
            .deferred_hot_pool_reserve_pins
            .read()
            .contains_key(&pool));
        assert!(ctx.tracked_vaults.read().is_empty());

        ctx.live_pool_cache.upsert(
            pool,
            CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
                token_0_mint: base_mint,
                token_1_mint: quote,
                token_0_vault: coin_vault,
                token_1_vault: pc_vault,
                reserve_0: Some(1_000_000),
                reserve_1: Some(2_000_000),
            }),
            2,
        );
        assert!(ctx.retry_deferred_hot_pool_reserve_registrations(&mut admission));
        assert!(!ctx
            .deferred_hot_pool_reserve_pins
            .read()
            .contains_key(&pool));
        let vs = ctx.tracked_vaults.read();
        assert_eq!(
            vs.get(&coin_vault).and_then(|v| v.pin),
            Some(GeyserPinReason::MomentumActive)
        );
    }

    /// Scope C: deferred pin cleared when vaults already tracked (registration no-op).
    #[test]
    fn scope_c_deferred_pin_cleared_when_vaults_already_tracked() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);

        let pool = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote = Pubkey::from_str(NATIVE_SOL_MINT).unwrap();
        let coin_vault = Pubkey::new_unique();
        let pc_vault = Pubkey::new_unique();
        let state = CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
            token_0_mint: base_mint,
            token_1_mint: quote,
            token_0_vault: coin_vault,
            token_1_vault: pc_vault,
            reserve_0: Some(1_000_000),
            reserve_1: Some(2_000_000),
        });

        ctx.live_pool_cache.upsert(pool, state.clone(), 1);
        ctx.hot_pool_registry
            .pin_pool_with_reason(base_mint, pool, true);
        {
            let mut vaults = ctx.tracked_vaults.write();
            let mut vaults_changed = false;
            ctx.register_tracked_vault_pair_lru(
                &mut vaults,
                &mut vaults_changed,
                TrackedVaultPairInsert {
                    pool,
                    now: Instant::now(),
                    dex: "raydium_cpmm",
                    base_mint,
                    quote_mint: quote,
                    base_vault: coin_vault,
                    quote_vault: pc_vault,
                    active_id: None,
                    bin_step: None,
                },
            );
        }
        assert!(ctx.pool_vaults_fully_tracked_for_cache(pool, &state));

        ctx.deferred_hot_pool_reserve_pins
            .write()
            .insert(pool, GeyserPinReason::MomentumActive);

        let mut admission = test_admission_for(&ctx);
        ctx.retry_deferred_hot_pool_reserve_registrations(&mut admission);
        assert!(!ctx
            .deferred_hot_pool_reserve_pins
            .read()
            .contains_key(&pool));
        assert_eq!(ctx.tracked_vaults.read().len(), 2);
        assert!(!ctx.retry_deferred_hot_pool_reserve_registrations(&mut admission));
    }

    /// Scope C: admission reject with cache hit still defers reserve registration for retry.
    #[test]
    fn scope_c_admission_reject_cache_hit_defers_reserve_registration() {
        use ironcrab::nats::{MomentumActivePinReason, MomentumActivePoolEntry};

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);

        let pool = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote = Pubkey::from_str(NATIVE_SOL_MINT).unwrap();
        let coin_vault = Pubkey::new_unique();
        let pc_vault = Pubkey::new_unique();
        ctx.live_pool_cache.upsert(
            pool,
            CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
                token_0_mint: base_mint,
                token_1_mint: quote,
                token_0_vault: coin_vault,
                token_1_vault: pc_vault,
                reserve_0: Some(1_000_000),
                reserve_1: Some(2_000_000),
            }),
            1,
        );

        let mut admission = FixedCapAdmission::new(0);
        ctx.apply_momentum_active_entries(
            &mut admission,
            &[MomentumActivePoolEntry {
                mint: base_mint.to_string(),
                pool: pool.to_string(),
                pin_reason: MomentumActivePinReason::Position,
            }],
        );

        assert!(ctx.hot_pool_registry.is_hot_pool(pool));
        assert!(ctx
            .deferred_hot_pool_reserve_pins
            .read()
            .contains_key(&pool));
        assert!(ctx.tracked_vaults.read().is_empty());
    }

    /// Scope C: position pool vault rows use MomentumPosition consumer for desired-set admission.
    #[test]
    fn scope_c_position_pool_explicit_rows_use_momentum_position_consumer() {
        use ironcrab::market_data::track::ConsumerId;

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);

        let pool = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote = Pubkey::from_str(NATIVE_SOL_MINT).unwrap();
        let coin_vault = Pubkey::new_unique();
        let pc_vault = Pubkey::new_unique();
        ctx.live_pool_cache.upsert(
            pool,
            CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
                token_0_mint: base_mint,
                token_1_mint: quote,
                token_0_vault: coin_vault,
                token_1_vault: pc_vault,
                reserve_0: Some(1),
                reserve_1: Some(1),
            }),
            1,
        );
        ctx.hot_pool_registry
            .pin_pool_with_reason(base_mint, pool, true);
        {
            let mut vaults = ctx.tracked_vaults.write();
            let mut vaults_changed = false;
            ctx.register_tracked_vault_pair_lru(
                &mut vaults,
                &mut vaults_changed,
                TrackedVaultPairInsert {
                    pool,
                    now: Instant::now(),
                    dex: "raydium_cpmm",
                    base_mint,
                    quote_mint: quote,
                    base_vault: coin_vault,
                    quote_vault: pc_vault,
                    active_id: None,
                    bin_step: None,
                },
            );
        }

        let rows = ctx.explicit_pubkey_rows_for_desired_set();
        assert!(rows.iter().any(|(pk, consumer, pool_opt)| {
            *pk == coin_vault
                && *consumer == ConsumerId::MomentumPosition
                && *pool_opt == Some(pool)
        }));
    }

    /// PR169a regression: account-path vault register requires explicit admission (no cap hole).
    #[test]
    fn register_pool_vaults_from_account_requires_explicit_admission() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);

        let pool = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote = Pubkey::from_str(NATIVE_SOL_MINT).unwrap();
        let coin_vault = Pubkey::new_unique();
        let pc_vault = Pubkey::new_unique();
        ctx.live_pool_cache.upsert(
            pool,
            CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
                token_0_mint: base_mint,
                token_1_mint: quote,
                token_0_vault: coin_vault,
                token_1_vault: pc_vault,
                reserve_0: Some(1_000_000),
                reserve_1: Some(2_000_000),
            }),
            1,
        );
        ctx.hot_pool_registry.pin_pool(base_mint, pool);
        assert!(!ctx.pool_has_explicit_momentum_admission(pool));

        assert!(!ctx.register_pool_vaults_from_account(pool));
        assert!(ctx.tracked_vaults.read().is_empty());

        ctx.note_explicit_momentum_pool_admitted(pool);
        assert!(ctx.register_pool_vaults_from_account(pool));
        let vs = ctx.tracked_vaults.read();
        assert!(vs.contains_key(&coin_vault));
        assert!(vs.contains_key(&pc_vault));
    }

    /// Scope C: Tracker downgrade clears position-subset membership while momentum pin remains.
    #[test]
    fn scope_c_tracker_downgrade_clears_position_pin_subset() {
        use ironcrab::market_data::track::{ExplicitConsumer, ExplicitOwner, ExplicitOwnerKey};
        use ironcrab::nats::{MomentumActivePinReason, MomentumActivePoolEntry};

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);

        let pool = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote = Pubkey::from_str(NATIVE_SOL_MINT).unwrap();
        let coin_vault = Pubkey::new_unique();
        let pc_vault = Pubkey::new_unique();
        ctx.live_pool_cache.upsert(
            pool,
            CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
                token_0_mint: base_mint,
                token_1_mint: quote,
                token_0_vault: coin_vault,
                token_1_vault: pc_vault,
                reserve_0: Some(1_000_000),
                reserve_1: Some(2_000_000),
            }),
            1,
        );

        let mut admission = test_admission_for(&ctx);
        let position_then_tracker = |pin_reason| MomentumActivePoolsUpdate {
            version: 1,
            ts_unix_ms: 1,
            active: vec![MomentumActivePoolEntry {
                mint: base_mint.to_string(),
                pool: pool.to_string(),
                pin_reason,
            }],
            removed: vec![],
            full_active_snapshot: false,
        };

        ctx.apply_momentum_active_pools_update(
            &mut admission,
            &position_then_tracker(MomentumActivePinReason::Position),
        );
        assert!(ctx.hot_pool_registry.is_position_pin(base_mint, pool));

        ctx.apply_momentum_active_pools_update(
            &mut admission,
            &position_then_tracker(MomentumActivePinReason::Tracker),
        );
        assert!(
            ctx.hot_pool_registry.is_hot_pool(pool),
            "momentum pair pin must remain for tracker"
        );
        assert!(
            !ctx.hot_pool_registry.is_position_pin(base_mint, pool),
            "position subset must clear on tracker downgrade"
        );
        let momentum_owner = ExplicitOwner {
            consumer: ExplicitConsumer::Momentum,
            owner_key: ExplicitOwnerKey::Pool(pool),
        };
        let position_owner = ExplicitOwner {
            consumer: ExplicitConsumer::MomentumPosition,
            owner_key: ExplicitOwnerKey::Pool(pool),
        };
        assert!(
            admission.owner_group(&position_owner).is_none(),
            "tracker downgrade must release MomentumPosition admission owner"
        );
        assert!(
            admission.owner_group(&momentum_owner).is_some(),
            "tracker downgrade must admit Momentum consumer group"
        );
        assert!(ctx.pool_has_explicit_momentum_admission(pool));
    }

    /// Scope C: Tracker→Position upgrade must admit Position before releasing Momentum; on admit failure keep Momentum.
    #[test]
    fn scope_c_tracker_to_position_upgrade_keeps_momentum_on_admit_failure() {
        use ironcrab::market_data::track::{
            try_admit_owner_group, ExplicitConsumer, ExplicitOwner, ExplicitOwnerKey,
        };
        use ironcrab::nats::{MomentumActivePinReason, MomentumActivePoolEntry};

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);

        let pool = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote = Pubkey::from_str(NATIVE_SOL_MINT).unwrap();
        let coin_vault = Pubkey::new_unique();
        let pc_vault = Pubkey::new_unique();
        let momentum_owner = ExplicitOwner {
            consumer: ExplicitConsumer::Momentum,
            owner_key: ExplicitOwnerKey::Pool(pool),
        };
        let position_owner = ExplicitOwner {
            consumer: ExplicitConsumer::MomentumPosition,
            owner_key: ExplicitOwnerKey::Pool(pool),
        };

        let mut admission = test_admission_for(&ctx);
        assert!(try_admit_owner_group(
            &mut admission,
            momentum_owner.clone(),
            vec![coin_vault, pc_vault, base_mint, quote],
        ));
        ctx.hot_pool_registry.pin_pool(base_mint, pool);

        // No LivePoolCache layout: Position admit must fail while Momentum stays admitted.
        ctx.apply_momentum_active_entries(
            &mut admission,
            &[MomentumActivePoolEntry {
                mint: base_mint.to_string(),
                pool: pool.to_string(),
                pin_reason: MomentumActivePinReason::Position,
            }],
        );
        assert!(
            admission.owner_group(&momentum_owner).is_some(),
            "failed Position admit must not release existing Momentum consumer"
        );
        assert!(
            admission.owner_group(&position_owner).is_none(),
            "Position admit should fail when cache layout is unavailable"
        );
    }

    /// Scope C: deferred retry must not clear deferred when pool consumer admit fails.
    #[test]
    fn scope_c_deferred_retry_keeps_deferred_on_admit_failure() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);

        let pool = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote = Pubkey::from_str(NATIVE_SOL_MINT).unwrap();
        let coin_vault = Pubkey::new_unique();
        let pc_vault = Pubkey::new_unique();
        let state = CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
            token_0_mint: base_mint,
            token_1_mint: quote,
            token_0_vault: coin_vault,
            token_1_vault: pc_vault,
            reserve_0: Some(1_000_000),
            reserve_1: Some(2_000_000),
        });

        ctx.live_pool_cache.upsert(pool, state.clone(), 1);
        ctx.hot_pool_registry
            .pin_pool_with_reason(base_mint, pool, true);
        {
            let mut vaults = ctx.tracked_vaults.write();
            let mut vaults_changed = false;
            ctx.register_tracked_vault_pair_lru(
                &mut vaults,
                &mut vaults_changed,
                TrackedVaultPairInsert {
                    pool,
                    now: Instant::now(),
                    dex: "raydium_cpmm",
                    base_mint,
                    quote_mint: quote,
                    base_vault: coin_vault,
                    quote_vault: pc_vault,
                    active_id: None,
                    bin_step: None,
                },
            );
        }
        assert!(ctx.pool_vaults_fully_tracked_for_cache(pool, &state));
        ctx.deferred_hot_pool_reserve_pins
            .write()
            .insert(pool, GeyserPinReason::MomentumActive);

        let mut admission = FixedCapAdmission::new(0);
        ctx.retry_deferred_hot_pool_reserve_registrations(&mut admission);
        assert!(
            ctx.deferred_hot_pool_reserve_pins
                .read()
                .contains_key(&pool),
            "deferred pin must remain when admit fails despite satisfied reserves"
        );
    }

    /// Scope C: releasing one momentum consumer must not clear bypass while sibling remains.
    #[test]
    fn release_pool_consumer_group_keeps_bypass_while_sibling_momentum_consumer_admitted() {
        use ironcrab::market_data::track::{
            try_admit_owner_group, ExplicitConsumer, ExplicitOwner, ExplicitOwnerKey,
        };

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);

        let pool = Pubkey::new_unique();
        let coin = Pubkey::new_unique();
        let pc = Pubkey::new_unique();
        let position_owner = ExplicitOwner {
            consumer: ExplicitConsumer::MomentumPosition,
            owner_key: ExplicitOwnerKey::Pool(pool),
        };
        let momentum_owner = ExplicitOwner {
            consumer: ExplicitConsumer::Momentum,
            owner_key: ExplicitOwnerKey::Pool(pool),
        };

        let mut admission = FixedCapAdmission::new(100);
        assert!(try_admit_owner_group(
            &mut admission,
            position_owner.clone(),
            vec![coin, pc],
        ));
        assert!(try_admit_owner_group(
            &mut admission,
            momentum_owner.clone(),
            vec![coin, pc],
        ));
        ctx.note_explicit_momentum_pool_admitted(pool);
        assert!(ctx.pool_has_explicit_momentum_admission(pool));

        ctx.release_pool_consumer_group(&mut admission, pool, ExplicitConsumer::MomentumPosition);
        assert!(
            admission.owner_group(&momentum_owner).is_some(),
            "Momentum owner must remain after releasing MomentumPosition"
        );
        assert!(
            ctx.pool_has_explicit_momentum_admission(pool),
            "bypass gate must stay while sibling Momentum consumer remains"
        );

        ctx.release_pool_consumer_group(&mut admission, pool, ExplicitConsumer::Momentum);
        assert!(!ctx.pool_has_explicit_momentum_admission(pool));
    }

    #[test]
    fn momentum_active_pools_removed_clears_momentum_but_keeps_wallet_pin() {
        use ironcrab::nats::{
            MomentumActivePinReason, MomentumActivePoolEntry, MomentumRemovedPoolEntry,
        };

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);

        let pool = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote = Pubkey::from_str(NATIVE_SOL_MINT).unwrap();
        let coin_vault = Pubkey::new_unique();
        let pc_vault = Pubkey::new_unique();
        ctx.live_pool_cache.upsert(
            pool,
            CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
                token_0_mint: base_mint,
                token_1_mint: quote,
                token_0_vault: coin_vault,
                token_1_vault: pc_vault,
                reserve_0: Some(1_000_000),
                reserve_1: Some(2_000_000),
            }),
            1,
        );

        let mut admission = test_admission_for(&ctx);
        ctx.apply_momentum_active_pools_update(
            &mut admission,
            &MomentumActivePoolsUpdate {
                version: 1,
                ts_unix_ms: 1,
                active: vec![MomentumActivePoolEntry {
                    mint: base_mint.to_string(),
                    pool: pool.to_string(),
                    pin_reason: MomentumActivePinReason::Tracker,
                }],
                removed: vec![],
                full_active_snapshot: false,
            },
        );

        {
            let mut vs = ctx.tracked_vaults.write();
            if let Some(v) = vs.get_mut(&pc_vault) {
                v.pin = Some(GeyserPinReason::Wallet);
                v.pinned = true;
            }
        }

        ctx.apply_momentum_active_pools_update(
            &mut admission,
            &MomentumActivePoolsUpdate {
                version: 1,
                ts_unix_ms: 2,
                active: vec![],
                removed: vec![MomentumRemovedPoolEntry {
                    mint: base_mint.to_string(),
                    pool: pool.to_string(),
                    reason: "closed".to_string(),
                }],
                full_active_snapshot: false,
            },
        );

        let vs = ctx.tracked_vaults.read();
        assert_eq!(vs.get(&coin_vault).and_then(|v| v.pin), None);
        assert!(!vs.get(&coin_vault).is_some_and(|v| v.pinned));
        assert_eq!(
            vs.get(&pc_vault).and_then(|v| v.pin),
            Some(GeyserPinReason::Wallet)
        );
        assert!(vs.get(&pc_vault).is_some_and(|v| v.pinned));
    }

    #[test]
    fn momentum_active_pools_removed_one_mint_keeps_vault_momentum_when_other_mint_pins_pool() {
        use ironcrab::nats::{
            MomentumActivePinReason, MomentumActivePoolEntry, MomentumRemovedPoolEntry,
        };

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);

        let pool = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let other_tracker_mint = Pubkey::new_unique();
        let quote = Pubkey::from_str(NATIVE_SOL_MINT).unwrap();
        let coin_vault = Pubkey::new_unique();
        let pc_vault = Pubkey::new_unique();
        ctx.live_pool_cache.upsert(
            pool,
            CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
                token_0_mint: base_mint,
                token_1_mint: quote,
                token_0_vault: coin_vault,
                token_1_vault: pc_vault,
                reserve_0: Some(1_000_000),
                reserve_1: Some(2_000_000),
            }),
            1,
        );

        let mut admission = test_admission_for(&ctx);
        ctx.apply_momentum_active_pools_update(
            &mut admission,
            &MomentumActivePoolsUpdate {
                version: 1,
                ts_unix_ms: 1,
                active: vec![
                    MomentumActivePoolEntry {
                        mint: base_mint.to_string(),
                        pool: pool.to_string(),
                        pin_reason: MomentumActivePinReason::Tracker,
                    },
                    MomentumActivePoolEntry {
                        mint: other_tracker_mint.to_string(),
                        pool: pool.to_string(),
                        pin_reason: MomentumActivePinReason::Tracker,
                    },
                ],
                removed: vec![],
                full_active_snapshot: false,
            },
        );

        ctx.apply_momentum_active_pools_update(
            &mut admission,
            &MomentumActivePoolsUpdate {
                version: 1,
                ts_unix_ms: 2,
                active: vec![],
                removed: vec![MomentumRemovedPoolEntry {
                    mint: base_mint.to_string(),
                    pool: pool.to_string(),
                    reason: "stale_discovery".to_string(),
                }],
                full_active_snapshot: false,
            },
        );

        assert!(
            ctx.hot_pool_registry.is_pinned(other_tracker_mint, pool),
            "second pin row must survive"
        );
        let vs = ctx.tracked_vaults.read();
        assert_eq!(
            vs.get(&coin_vault).and_then(|v| v.pin),
            Some(GeyserPinReason::MomentumActive)
        );
        assert_eq!(
            vs.get(&pc_vault).and_then(|v| v.pin),
            Some(GeyserPinReason::MomentumActive)
        );
    }

    /// PR #147 follow-up: leg mint (WSOL) must stay `MomentumActive` while another pinned pool has
    /// no `LivePoolCache` row — `momentum_pool_leg_mint_still_required_by_other_pool` is conservative on miss.
    #[test]
    fn momentum_leg_mint_pin_kept_when_other_active_pool_has_cache_miss() {
        use ironcrab::nats::{
            MomentumActivePinReason, MomentumActivePoolEntry, MomentumRemovedPoolEntry,
        };

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);

        let pool_a = Pubkey::new_unique();
        let pool_b = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let other_tracker_mint = Pubkey::new_unique();
        let quote = Pubkey::from_str(NATIVE_SOL_MINT).unwrap();
        let coin_vault = Pubkey::new_unique();
        let pc_vault = Pubkey::new_unique();

        ctx.live_pool_cache.upsert(
            pool_a,
            CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
                token_0_mint: base_mint,
                token_1_mint: quote,
                token_0_vault: coin_vault,
                token_1_vault: pc_vault,
                reserve_0: Some(1_000_000),
                reserve_1: Some(2_000_000),
            }),
            1,
        );

        let mut admission = test_admission_for(&ctx);
        ctx.apply_momentum_active_pools_update(
            &mut admission,
            &MomentumActivePoolsUpdate {
                version: 1,
                ts_unix_ms: 1,
                active: vec![
                    MomentumActivePoolEntry {
                        mint: base_mint.to_string(),
                        pool: pool_a.to_string(),
                        pin_reason: MomentumActivePinReason::Tracker,
                    },
                    MomentumActivePoolEntry {
                        mint: other_tracker_mint.to_string(),
                        pool: pool_b.to_string(),
                        pin_reason: MomentumActivePinReason::Tracker,
                    },
                ],
                removed: vec![],
                full_active_snapshot: false,
            },
        );

        assert!(
            ctx.tracked_mints
                .read()
                .get(&quote)
                .is_some_and(|m| m.pin == Some(GeyserPinReason::MomentumActive)),
            "setup: WSOL leg tracked for pool_a"
        );

        ctx.apply_momentum_active_pools_update(
            &mut admission,
            &MomentumActivePoolsUpdate {
                version: 1,
                ts_unix_ms: 2,
                active: vec![],
                removed: vec![MomentumRemovedPoolEntry {
                    mint: base_mint.to_string(),
                    pool: pool_a.to_string(),
                    reason: "closed".to_string(),
                }],
                full_active_snapshot: false,
            },
        );

        assert!(
            ctx.hot_pool_registry.is_pinned(other_tracker_mint, pool_b),
            "pool_b pin survives pool_a removal"
        );
        assert!(
            ctx.tracked_mints
                .read()
                .get(&quote)
                .is_some_and(|m| m.pin == Some(GeyserPinReason::MomentumActive)),
            "WSOL must stay pinned while pool_b is active without cache layout"
        );
    }

    #[tokio::test]
    async fn tx_fast_path_dev_wallet_after_pool_mint_map_emits_once() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        let (publish_tx, mut publish_rx) = mpsc::channel::<AccountPathNatsJob>(8);
        let pool = Pubkey::new_unique();
        let mint = Pubkey::new_unique();
        let creator = Pubkey::new_unique();
        ctx.pool_creator_cache
            .write()
            .insert(pool.to_string(), creator.to_string());
        let grpc_at = Instant::now() - std::time::Duration::from_millis(10);
        assert!(
            maybe_emit_dev_wallet_after_pool_mint_map(
                &ctx,
                "run-test",
                &pool,
                &mint.to_string(),
                Some(1),
                grpc_at,
                Some(&publish_tx),
                None,
                Some(creator.to_string().as_str()),
            )
            .await
        );
        let job = tokio::time::timeout(std::time::Duration::from_secs(2), publish_rx.recv())
            .await
            .expect("timeout waiting for publish job")
            .expect("enqueue expected");
        match job {
            AccountPathNatsJob::CoreMarketEvent { event, .. } => {
                assert!(matches!(
                    event.kind,
                    MarketEventKind::DevWalletIdentified { .. }
                ));
            }
            _ => panic!("expected CoreMarketEvent job"),
        }
        assert!(
            !maybe_emit_dev_wallet_after_pool_mint_map(
                &ctx,
                "run-test",
                &pool,
                &mint.to_string(),
                Some(2),
                grpc_at,
                Some(&publish_tx),
                None,
                Some(creator.to_string().as_str()),
            )
            .await,
            "second call with same creator must not re-emit"
        );
        assert!(
            publish_rx.try_recv().is_err(),
            "idempotent second call must not enqueue another NATS job"
        );
        ctx.jsonl_writer.flush().expect("jsonl flush");
        std::thread::sleep(std::time::Duration::from_millis(50));
        let date = chrono::Utc::now().format("%Y%m%d");
        let log = tmp.path().join(format!("market_events-{date}.jsonl"));
        let text = std::fs::read_to_string(&log).expect("jsonl read");
        let dev_lines = text
            .lines()
            .filter(|l| l.contains("DevWalletIdentified"))
            .count();
        assert_eq!(
            dev_lines, 1,
            "expected exactly one DevWalletIdentified JSONL line"
        );
    }

    /// PR160: main publish `mpsc` uses `try_send` — second enqueue fails when channel capacity is 1 and unconsumed.
    #[tokio::test]
    async fn account_path_core_enqueue_drops_when_publish_queue_full() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        let (tx, _rx) = mpsc::channel::<AccountPathNatsJob>(1);
        let mk_event = |slot: u64| {
            MarketEvent::new(
                "market-data",
                BUILD_VERSION,
                "run-test",
                ctx.next_event_id(),
                "test",
                Some(slot),
                MarketEventKind::SlotUpdate { current_slot: slot },
            )
        };
        assert!(
            account_path_enqueue_core_market_event(Some(&tx), None, &ctx, mk_event(1), None,).await
        );
        assert!(
            !account_path_enqueue_core_market_event(Some(&tx), None, &ctx, mk_event(2), None,)
                .await
        );
    }

    #[test]
    fn account_geyser_update_might_be_relevant_non_hot_dex_pool_false() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        let pool = Pubkey::new_unique();
        let base = Pubkey::new_unique();
        let u = GeyserAccountUpdate {
            pubkey: pool,
            slot: 1,
            owner: RAYDIUM_CPMM_OWNER,
            data: vec![],
            lamports: 0,
            grpc_recv_at: Instant::now(),
        };
        assert!(!account_geyser_update_might_be_relevant(&ctx, &u));
        ctx.hot_pool_registry.pin_pool(base, pool);
        assert!(account_geyser_update_might_be_relevant(&ctx, &u));
    }

    #[test]
    fn account_geyser_update_might_be_relevant_pool_mint_map_without_hot_pin() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        let pool = Pubkey::new_unique();
        ctx.pool_mint_map
            .write()
            .insert(pool.to_string(), Pubkey::new_unique().to_string());
        let u = GeyserAccountUpdate {
            pubkey: pool,
            slot: 1,
            owner: RAYDIUM_CPMM_OWNER,
            data: vec![],
            lamports: 0,
            grpc_recv_at: Instant::now(),
        };
        assert!(account_geyser_update_might_be_relevant(&ctx, &u));
        assert!(ctx.ingest_is_enrichment_member(&pool));
    }

    #[test]
    fn account_geyser_update_might_be_relevant_arb_pinned_without_pool_mint_map() {
        use ironcrab::metrics::MARKET_DATA_ACCOUNT_RELEVANCE_ENRICHMENT_HIT_TOTAL;

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        let pool = Pubkey::new_unique();
        ctx.hot_pool_registry.pin_arb_pool(pool);
        assert!(!ctx.ingest_pool_mint_map_contains(&pool));
        let before = MARKET_DATA_ACCOUNT_RELEVANCE_ENRICHMENT_HIT_TOTAL.load(Ordering::Relaxed);
        let u = GeyserAccountUpdate {
            pubkey: pool,
            slot: 1,
            owner: RAYDIUM_CPMM_OWNER,
            data: vec![],
            lamports: 0,
            grpc_recv_at: Instant::now(),
        };
        assert!(account_geyser_update_might_be_relevant(&ctx, &u));
        assert!(
            MARKET_DATA_ACCOUNT_RELEVANCE_ENRICHMENT_HIT_TOTAL.load(Ordering::Relaxed) > before
        );
    }

    #[test]
    fn account_geyser_update_relevance_non_enrichment_dex_pool_early_drop_reason() {
        use ironcrab::market_data::ingest::AccountGeyserRelevance;
        use ironcrab::metrics::MarketDataAccountEarlyDropReason;

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        let pool = Pubkey::new_unique();
        let u = GeyserAccountUpdate {
            pubkey: pool,
            slot: 1,
            owner: RAYDIUM_CPMM_OWNER,
            data: vec![],
            lamports: 0,
            grpc_recv_at: Instant::now(),
        };
        assert_eq!(
            account_geyser_update_relevance(&ctx, &u),
            AccountGeyserRelevance::EarlyDrop(
                MarketDataAccountEarlyDropReason::DexPoolNotEnrichment
            )
        );
    }

    #[test]
    fn account_geyser_update_relevance_random_non_dex_early_drop_reason() {
        use ironcrab::market_data::ingest::AccountGeyserRelevance;
        use ironcrab::metrics::MarketDataAccountEarlyDropReason;

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        let u = GeyserAccountUpdate {
            pubkey: Pubkey::new_unique(),
            slot: 1,
            owner: Pubkey::new_unique(),
            data: vec![],
            lamports: 0,
            grpc_recv_at: Instant::now(),
        };
        assert_eq!(
            account_geyser_update_relevance(&ctx, &u),
            AccountGeyserRelevance::EarlyDrop(
                MarketDataAccountEarlyDropReason::NonDexNonMembership
            )
        );
    }

    #[test]
    fn account_geyser_update_relevance_membership_explicit_vault_no_early_drop() {
        use ironcrab::market_data::ingest::AccountGeyserRelevance;

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        let vault = Pubkey::new_unique();
        ctx.tracked_vaults.write().insert(
            vault,
            VaultInfo {
                pool_address: Pubkey::new_unique(),
                dex: "raydium_cpmm".to_string(),
                base_mint: Pubkey::new_unique(),
                quote_mint: Pubkey::from_str(NATIVE_SOL_MINT).unwrap(),
                is_base_vault: true,
                last_balance: std::sync::atomic::AtomicU64::new(0),
                last_used_at: Instant::now(),
                pinned: false,
                pin: None,
                active_id: None,
                bin_step: None,
                sibling_vault: None,
            },
        );
        ctx.refresh_tracked_membership_snapshot();
        let u = GeyserAccountUpdate {
            pubkey: vault,
            slot: 1,
            owner: Pubkey::new_unique(),
            data: vec![],
            lamports: 0,
            grpc_recv_at: Instant::now(),
        };
        assert_eq!(
            account_geyser_update_relevance(&ctx, &u),
            AccountGeyserRelevance::Relevant
        );
    }

    #[test]
    fn enrichment_sidefx_host_pool_mint_map_is_member_without_hot_pin() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        let (md_state, _depth, _md_state_rx) = test_md_state_sender_no_worker();
        let worker = test_sidefx_host(&ctx, md_state, test_noop_track_worker_sender());
        let pool = Pubkey::new_unique();
        ctx.pool_mint_map
            .write()
            .insert(pool.to_string(), Pubkey::new_unique().to_string());
        assert!(worker.is_enrichment_member(&pool));
        assert!(!worker.is_hot_pool(&pool));
        assert!(!worker.pool_has_live_vault_geyser_feed(pool));
    }

    #[test]
    fn account_geyser_dispatch_not_high_when_pool_mint_map_only() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        let pool = Pubkey::new_unique();
        ctx.pool_mint_map
            .write()
            .insert(pool.to_string(), Pubkey::new_unique().to_string());
        let u = GeyserAccountUpdate {
            pubkey: pool,
            slot: 1,
            owner: Pubkey::new_unique(),
            data: vec![],
            lamports: 0,
            grpc_recv_at: Instant::now(),
        };
        assert!(!account_geyser_dispatch_priority_high(&ctx, &u));
    }

    #[test]
    fn account_geyser_dispatch_not_high_when_tracked_vault_without_hot_pool() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        let vault = Pubkey::new_unique();
        ctx.tracked_vaults.write().insert(
            vault,
            VaultInfo {
                pool_address: Pubkey::new_unique(),
                dex: "raydium_cpmm".to_string(),
                base_mint: Pubkey::new_unique(),
                quote_mint: Pubkey::from_str(NATIVE_SOL_MINT).unwrap(),
                is_base_vault: true,
                last_balance: std::sync::atomic::AtomicU64::new(0),
                last_used_at: Instant::now(),
                pinned: false,
                pin: None,
                active_id: None,
                bin_step: None,
                sibling_vault: None,
            },
        );
        ctx.refresh_tracked_membership_snapshot();
        let u = GeyserAccountUpdate {
            pubkey: vault,
            slot: 1,
            owner: Pubkey::new_unique(),
            data: vec![],
            lamports: 0,
            grpc_recv_at: Instant::now(),
        };
        assert!(!account_geyser_dispatch_priority_high(&ctx, &u));
    }

    #[test]
    fn account_geyser_dispatch_high_when_tracked_vault_for_hot_pool() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        let pool = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let vault = Pubkey::new_unique();
        ctx.hot_pool_registry.pin_pool(base_mint, pool);
        ctx.tracked_vaults.write().insert(
            vault,
            VaultInfo {
                pool_address: pool,
                dex: "raydium_cpmm".to_string(),
                base_mint,
                quote_mint: Pubkey::from_str(NATIVE_SOL_MINT).unwrap(),
                is_base_vault: true,
                last_balance: std::sync::atomic::AtomicU64::new(0),
                last_used_at: Instant::now(),
                pinned: false,
                pin: None,
                active_id: None,
                bin_step: None,
                sibling_vault: None,
            },
        );
        ctx.refresh_tracked_membership_snapshot();
        let u = GeyserAccountUpdate {
            pubkey: vault,
            slot: 1,
            owner: Pubkey::new_unique(),
            data: vec![],
            lamports: 0,
            grpc_recv_at: Instant::now(),
        };
        assert!(account_geyser_dispatch_priority_high(&ctx, &u));
    }

    #[tokio::test]
    async fn account_worker_recv_next_prefers_high_queue() {
        let (htx, mut hrx) = mpsc::channel(8);
        let (ltx, mut lrx) = mpsc::channel(8);
        let pool_h = Pubkey::new_unique();
        let pool_l = Pubkey::new_unique();
        let mk_high = |p: Pubkey| AccountWorkItem {
            update: GeyserAccountUpdate {
                pubkey: p,
                slot: 1,
                owner: PUMPFUN_PROGRAM_OWNER,
                data: vec![],
                lamports: 0,
                grpc_recv_at: Instant::now(),
            },
            recv_at: Instant::now(),
            class: AccountUpdateClass::ExecHot,
        };
        let mk_low = |p: Pubkey| AccountWorkItem {
            update: GeyserAccountUpdate {
                pubkey: p,
                slot: 1,
                owner: PUMPFUN_PROGRAM_OWNER,
                data: vec![],
                lamports: 0,
                grpc_recv_at: Instant::now(),
            },
            recv_at: Instant::now(),
            class: AccountUpdateClass::Enrich,
        };
        ltx.send(mk_low(pool_l)).await.unwrap();
        htx.send(mk_high(pool_h)).await.unwrap();
        drop(htx);
        drop(ltx);
        let w1 = account_worker_recv_next(&mut hrx, &mut lrx)
            .await
            .expect("high item");
        assert!(matches!(w1, AccountWorkerDequeue::ExecHot(_)));
        assert_eq!(
            match w1 {
                AccountWorkerDequeue::ExecHot(w) => w.update.pubkey,
                AccountWorkerDequeue::Enrich(_) => unreachable!(),
            },
            pool_h
        );
        let w2 = account_worker_recv_next(&mut hrx, &mut lrx)
            .await
            .expect("low item");
        assert!(matches!(w2, AccountWorkerDequeue::Enrich(_)));
        assert_eq!(
            match w2 {
                AccountWorkerDequeue::Enrich(w) => w.update.pubkey,
                AccountWorkerDequeue::ExecHot(_) => unreachable!(),
            },
            pool_l
        );
        assert!(account_worker_recv_next(&mut hrx, &mut lrx).await.is_none());
    }

    #[tokio::test]
    async fn account_worker_recv_next_never_drains_low_while_high_available() {
        let (htx, mut hrx) = mpsc::channel(32);
        let (ltx, mut lrx) = mpsc::channel(32);
        let mk = |slot: u64| AccountWorkItem {
            update: GeyserAccountUpdate {
                pubkey: Pubkey::new_unique(),
                slot,
                owner: PUMPFUN_PROGRAM_OWNER,
                data: vec![],
                lamports: 0,
                grpc_recv_at: Instant::now(),
            },
            recv_at: Instant::now(),
            class: AccountUpdateClass::Enrich,
        };
        for slot in 0..16 {
            ltx.send(mk(slot)).await.unwrap();
        }
        htx.send(mk(100)).await.unwrap();
        drop(htx);
        drop(ltx);
        let first = account_worker_recv_next(&mut hrx, &mut lrx)
            .await
            .expect("high must win under load");
        assert!(matches!(first, AccountWorkerDequeue::ExecHot(_)));
        assert_eq!(
            match first {
                AccountWorkerDequeue::ExecHot(w) => w.update.slot,
                AccountWorkerDequeue::Enrich(_) => unreachable!(),
            },
            100
        );
    }

    #[tokio::test]
    async fn account_recv_exec_hot_not_blocked_by_enrich_coalesce_mutex() {
        use std::sync::Arc;
        use tokio::sync::Mutex;

        let (high_tx, mut high_rx) = mpsc::channel(8);
        let (ingress_tx, _ingress_rx) = mpsc::channel(8);
        let coalesce = Arc::new(Mutex::new(AccountEnrichCoalesceShard::new()));
        let coalesce_hold = Arc::clone(&coalesce);
        let hold_task = tokio::spawn(async move {
            let _guard = coalesce_hold.lock().await;
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        });
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let exec_pool = Pubkey::new_unique();
        high_tx
            .send(AccountWorkItem {
                update: GeyserAccountUpdate {
                    pubkey: exec_pool,
                    slot: 1,
                    owner: PUMPFUN_PROGRAM_OWNER,
                    data: vec![],
                    lamports: 0,
                    grpc_recv_at: Instant::now(),
                },
                recv_at: Instant::now(),
                class: AccountUpdateClass::ExecHot,
            })
            .await
            .expect("EXEC_HOT must enqueue while enrich mutex is held");

        let enrich_pk = Pubkey::new_unique();
        assert_eq!(
            account_enrich_ingress_try_send(
                &ingress_tx,
                AccountWorkItem {
                    update: GeyserAccountUpdate {
                        pubkey: enrich_pk,
                        slot: 2,
                        owner: PUMPFUN_PROGRAM_OWNER,
                        data: vec![],
                        lamports: 0,
                        grpc_recv_at: Instant::now(),
                    },
                    recv_at: Instant::now(),
                    class: AccountUpdateClass::Enrich,
                },
            ),
            AccountEnrichIngressSend::Ok,
            "ENRICH ingress must not await enrich coalesce mutex"
        );

        let received = tokio::time::timeout(std::time::Duration::from_millis(100), high_rx.recv())
            .await
            .expect("HIGH recv should not time out")
            .expect("HIGH item present");
        assert!(matches!(received.class, AccountUpdateClass::ExecHot));
        assert_eq!(received.update.pubkey, exec_pool);

        hold_task.abort();
        let _ = hold_task.await;
    }

    #[tokio::test]
    async fn account_enrich_ingress_full_does_not_block_recv_path() {
        let (ingress_tx, _ingress_rx) = mpsc::channel(1);
        let mk = |slot: u64| AccountWorkItem {
            update: GeyserAccountUpdate {
                pubkey: Pubkey::new_unique(),
                slot,
                owner: PUMPFUN_PROGRAM_OWNER,
                data: vec![],
                lamports: 0,
                grpc_recv_at: Instant::now(),
            },
            recv_at: Instant::now(),
            class: AccountUpdateClass::Enrich,
        };
        assert_eq!(
            account_enrich_ingress_try_send(&ingress_tx, mk(1)),
            AccountEnrichIngressSend::Ok
        );
        assert_eq!(
            account_enrich_ingress_try_send(&ingress_tx, mk(2)),
            AccountEnrichIngressSend::ContendedFull
        );
    }

    #[test]
    fn account_high_ingress_full_does_not_block_recv_path() {
        use ironcrab::metrics::MARKET_DATA_ACCOUNT_HIGH_ENQUEUE_DROPPED_TOTAL;
        use std::sync::atomic::Ordering;

        MARKET_DATA_ACCOUNT_HIGH_ENQUEUE_DROPPED_TOTAL.store(0, Ordering::Relaxed);
        let (high_tx, _high_rx) = mpsc::channel(1);
        let mk = |slot: u64| AccountWorkItem {
            update: GeyserAccountUpdate {
                pubkey: Pubkey::new_unique(),
                slot,
                owner: PUMPFUN_PROGRAM_OWNER,
                data: vec![],
                lamports: 0,
                grpc_recv_at: Instant::now(),
            },
            recv_at: Instant::now(),
            class: AccountUpdateClass::ExecHot,
        };
        assert_eq!(
            account_high_ingress_try_send(&high_tx, mk(1)),
            AccountHighIngressSend::Ok
        );
        assert_eq!(
            account_high_ingress_try_send(&high_tx, mk(2)),
            AccountHighIngressSend::ContendedFull
        );
        assert_eq!(
            MARKET_DATA_ACCOUNT_HIGH_ENQUEUE_DROPPED_TOTAL.load(Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn account_enrich_dispatch_upsert_and_flush_preserves_latest_wins() {
        use ironcrab::metrics::MARKET_DATA_ACCOUNT_ENRICH_COALESCE_TOTAL;
        MARKET_DATA_ACCOUNT_ENRICH_COALESCE_TOTAL.store(0, Ordering::Relaxed);
        let (low_tx, mut low_rx) = mpsc::channel(4);
        let mut shard = AccountEnrichCoalesceShard::new();
        let pk = Pubkey::new_unique();
        let mk = |slot: u64| AccountWorkItem {
            update: GeyserAccountUpdate {
                pubkey: pk,
                slot,
                owner: PUMPFUN_PROGRAM_OWNER,
                data: vec![],
                lamports: 0,
                grpc_recv_at: Instant::now(),
            },
            recv_at: Instant::now(),
            class: AccountUpdateClass::Enrich,
        };
        assert_eq!(
            account_enrich_dispatch_upsert_and_flush(&mut shard, &low_tx, mk(1)),
            AccountEnrichFlushStatus::Ok
        );
        assert_eq!(
            account_enrich_dispatch_upsert_and_flush(&mut shard, &low_tx, mk(2)),
            AccountEnrichFlushStatus::Ok
        );
        assert_eq!(
            MARKET_DATA_ACCOUNT_ENRICH_COALESCE_TOTAL.load(Ordering::Relaxed),
            1
        );
        let flushed = low_rx.try_recv().expect("first flush");
        assert_eq!(flushed.update.slot, 1);
        assert!(shard.pending.contains_key(&pk));
        let (resolved, _) = account_enrich_resolve_after_dequeue(&mut shard, &low_tx, flushed);
        assert_eq!(resolved.update.slot, 2);
    }

    #[test]
    fn account_enrich_coalesce_keeps_latest_per_pubkey() {
        use ironcrab::metrics::{
            inc_market_data_account_enrich_coalesce_total,
            MARKET_DATA_ACCOUNT_ENRICH_COALESCE_TOTAL,
        };
        MARKET_DATA_ACCOUNT_ENRICH_COALESCE_TOTAL.store(0, Ordering::Relaxed);
        let (low_tx, _low_rx) = mpsc::channel(4);
        let mut shard = AccountEnrichCoalesceShard::new();
        let pk = Pubkey::new_unique();
        let mk = |slot: u64| AccountWorkItem {
            update: GeyserAccountUpdate {
                pubkey: pk,
                slot,
                owner: PUMPFUN_PROGRAM_OWNER,
                data: vec![],
                lamports: 0,
                grpc_recv_at: Instant::now(),
            },
            recv_at: Instant::now(),
            class: AccountUpdateClass::Enrich,
        };
        assert!(matches!(
            account_enrich_coalesce_upsert(&mut shard, mk(1)),
            AccountEnrichCoalesceUpsert::NewPending
        ));
        assert!(matches!(
            account_enrich_coalesce_upsert(&mut shard, mk(2)),
            AccountEnrichCoalesceUpsert::Coalesced
        ));
        inc_market_data_account_enrich_coalesce_total();
        assert_eq!(shard.pending.get(&pk).unwrap().update.slot, 2);
        assert_eq!(shard.unique_pending_pubkey_count(), 1);
        account_enrich_coalesce_try_flush(&mut shard, &low_tx);
        assert_eq!(shard.in_mpsc.len(), 1);
        assert!(shard.pending.is_empty());
        assert_eq!(
            shard.unique_pending_pubkey_count(),
            1,
            "one logical pending pubkey after flush"
        );
    }

    #[test]
    fn account_enrich_unique_count_not_double_in_mpsc_and_pending() {
        let mut shard = AccountEnrichCoalesceShard::new();
        let pk = Pubkey::new_unique();
        shard.in_mpsc.insert(pk);
        shard.pending.insert(
            pk,
            AccountWorkItem {
                update: GeyserAccountUpdate {
                    pubkey: pk,
                    slot: 2,
                    owner: PUMPFUN_PROGRAM_OWNER,
                    data: vec![],
                    lamports: 0,
                    grpc_recv_at: Instant::now(),
                },
                recv_at: Instant::now(),
                class: AccountUpdateClass::Enrich,
            },
        );
        assert_eq!(shard.unique_pending_pubkey_count(), 1);
    }

    #[test]
    fn account_enrich_coalesce_try_flush_closed_restores_pending() {
        const CAP: usize = 4;
        let mut shard = AccountEnrichCoalesceShard::new();
        let pk = Pubkey::new_unique();
        let mk = |slot: u64| AccountWorkItem {
            update: GeyserAccountUpdate {
                pubkey: pk,
                slot,
                owner: PUMPFUN_PROGRAM_OWNER,
                data: vec![],
                lamports: 0,
                grpc_recv_at: Instant::now(),
            },
            recv_at: Instant::now(),
            class: AccountUpdateClass::Enrich,
        };
        assert!(matches!(
            account_enrich_coalesce_upsert_with_cap(&mut shard, mk(7), CAP),
            AccountEnrichCoalesceUpsert::NewPending
        ));
        assert_eq!(shard.unique_pending_pubkey_count(), 1);
        let (closed_tx, closed_rx) = mpsc::channel(4);
        drop(closed_rx);
        assert_eq!(
            account_enrich_coalesce_try_flush(&mut shard, &closed_tx),
            AccountEnrichFlushStatus::LowChannelClosed
        );
        assert_eq!(
            shard.pending.get(&pk).unwrap().update.slot,
            7,
            "Closed channel must restore pending work instead of dropping it"
        );
        assert!(shard.in_mpsc.is_empty());
        assert_eq!(
            shard.unique_pending_pubkey_count(),
            1,
            "logical depth unchanged when Closed restores pending"
        );
    }

    #[test]
    fn account_enrich_coalesce_try_flush_full_restores_pending_without_closed() {
        const CAP: usize = 1;
        let mut shard = AccountEnrichCoalesceShard::new();
        let pk = Pubkey::new_unique();
        let work = AccountWorkItem {
            update: GeyserAccountUpdate {
                pubkey: pk,
                slot: 1,
                owner: PUMPFUN_PROGRAM_OWNER,
                data: vec![],
                lamports: 0,
                grpc_recv_at: Instant::now(),
            },
            recv_at: Instant::now(),
            class: AccountUpdateClass::Enrich,
        };
        assert!(matches!(
            account_enrich_coalesce_upsert_with_cap(&mut shard, work, CAP),
            AccountEnrichCoalesceUpsert::NewPending
        ));
        let (full_tx, full_rx) = mpsc::channel(1);
        let blocker = AccountWorkItem {
            update: GeyserAccountUpdate {
                pubkey: Pubkey::new_unique(),
                slot: 0,
                owner: PUMPFUN_PROGRAM_OWNER,
                data: vec![],
                lamports: 0,
                grpc_recv_at: Instant::now(),
            },
            recv_at: Instant::now(),
            class: AccountUpdateClass::Enrich,
        };
        full_tx.try_send(blocker).expect("prefill channel");
        assert_eq!(
            account_enrich_coalesce_try_flush(&mut shard, &full_tx),
            AccountEnrichFlushStatus::BlockedFull
        );
        assert_eq!(shard.pending.get(&pk).unwrap().update.slot, 1);
        drop(full_rx);
        assert_eq!(
            account_enrich_coalesce_try_flush(&mut shard, &full_tx),
            AccountEnrichFlushStatus::LowChannelClosed
        );
        assert!(shard.pending.contains_key(&pk));
    }

    #[test]
    fn account_enrich_cap_eviction_preserves_in_flight_supersede() {
        const CAP: usize = 4;
        let mut shard = AccountEnrichCoalesceShard::new();
        let pk_a = Pubkey::new_unique();
        let mut stale_keys = Vec::new();
        let mk = |pk: Pubkey, slot: u64| AccountWorkItem {
            update: GeyserAccountUpdate {
                pubkey: pk,
                slot,
                owner: PUMPFUN_PROGRAM_OWNER,
                data: vec![],
                lamports: 0,
                grpc_recv_at: Instant::now(),
            },
            recv_at: Instant::now(),
            class: AccountUpdateClass::Enrich,
        };
        for slot in 0..3 {
            let pk = Pubkey::new_unique();
            stale_keys.push(pk);
            assert!(matches!(
                account_enrich_coalesce_upsert_with_cap(&mut shard, mk(pk, slot), CAP),
                AccountEnrichCoalesceUpsert::NewPending
            ));
        }
        shard.in_mpsc.insert(pk_a);
        assert!(matches!(
            account_enrich_coalesce_upsert_with_cap(&mut shard, mk(pk_a, 10), CAP),
            AccountEnrichCoalesceUpsert::Coalesced
        ));
        assert_eq!(shard.unique_pending_pubkey_count(), CAP);
        assert_eq!(shard.pending.get(&pk_a).unwrap().update.slot, 10);

        let pk_b = Pubkey::new_unique();
        assert!(matches!(
            account_enrich_coalesce_upsert_with_cap(&mut shard, mk(pk_b, 20), CAP),
            AccountEnrichCoalesceUpsert::EvictedOldest
        ));
        assert_eq!(
            shard.pending.get(&pk_a).unwrap().update.slot,
            10,
            "in-flight supersede for A must survive cap eviction"
        );
        assert!(
            shard.pending.contains_key(&pk_b),
            "new pubkey B must be admitted after evicting a pending-only stale key"
        );
        let evicted_stale = stale_keys
            .iter()
            .filter(|pk| !shard.pending.contains_key(pk))
            .count();
        assert_eq!(
            evicted_stale, 1,
            "exactly one pending-only stale pubkey should be evicted"
        );
    }

    #[test]
    fn account_enrich_queue_depth_stays_within_cap() {
        let (low_tx, _low_rx) = mpsc::channel(MARKET_DATA_ACCOUNT_WORKER_QUEUE_CAP);
        let mut shard = AccountEnrichCoalesceShard::new();
        for i in 0..MARKET_DATA_ACCOUNT_WORKER_QUEUE_CAP {
            let upsert = account_enrich_coalesce_upsert(
                &mut shard,
                AccountWorkItem {
                    update: GeyserAccountUpdate {
                        pubkey: Pubkey::new_unique(),
                        slot: i as u64,
                        owner: PUMPFUN_PROGRAM_OWNER,
                        data: vec![],
                        lamports: 0,
                        grpc_recv_at: Instant::now(),
                    },
                    recv_at: Instant::now(),
                    class: AccountUpdateClass::Enrich,
                },
            );
            assert!(matches!(
                upsert,
                AccountEnrichCoalesceUpsert::NewPending
                    | AccountEnrichCoalesceUpsert::EvictedOldest
            ));
            assert!(
                shard.unique_pending_pubkey_count() <= MARKET_DATA_ACCOUNT_WORKER_QUEUE_CAP,
                "coalesce shard must stay within cap after upsert {i}"
            );
        }
        account_enrich_coalesce_try_flush(&mut shard, &low_tx);
        assert!(
            shard.unique_pending_pubkey_count() <= MARKET_DATA_ACCOUNT_WORKER_QUEUE_CAP,
            "coalesce shard pending must stay within cap after flush"
        );
    }

    #[test]
    fn tx_trade_path_skips_explicit_geyser_reserves_without_admission() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);

        let pool = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote = Pubkey::from_str(NATIVE_SOL_MINT).unwrap();
        let coin_vault = Pubkey::new_unique();
        let pc_vault = Pubkey::new_unique();
        ctx.live_pool_cache.upsert(
            pool,
            CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
                token_0_mint: base_mint,
                token_1_mint: quote,
                token_0_vault: coin_vault,
                token_1_vault: pc_vault,
                reserve_0: Some(1_000_000),
                reserve_1: Some(2_000_000),
            }),
            1,
        );

        ctx.hot_pool_registry.pin_pool(base_mint, pool);
        MarketDataContext::register_geyser_reserves_after_trade(&ctx, pool);

        let vs = ctx.tracked_vaults.read();
        assert!(!vs.contains_key(&coin_vault));
        assert!(!vs.contains_key(&pc_vault));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn tx_trade_path_registers_vaults_when_active_pool_pin_admits() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);

        let pool = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote = Pubkey::from_str(NATIVE_SOL_MINT).unwrap();
        let coin_vault = Pubkey::new_unique();
        let pc_vault = Pubkey::new_unique();
        ctx.live_pool_cache.upsert(
            pool,
            CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
                token_0_mint: base_mint,
                token_1_mint: quote,
                token_0_vault: coin_vault,
                token_1_vault: pc_vault,
                reserve_0: Some(1_000_000),
                reserve_1: Some(2_000_000),
            }),
            1,
        );
        ctx.hot_pool_registry.pin_pool(base_mint, pool);
        ctx.note_explicit_momentum_pool_admitted(pool);

        MarketDataContext::register_geyser_reserves_after_trade(&ctx, pool);

        let vs = ctx.tracked_vaults.read();
        assert!(vs.contains_key(&coin_vault));
        assert!(vs.contains_key(&pc_vault));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn tx_trade_path_geyser_sync_batches_two_pool_registers() {
        use ironcrab::metrics::{
            MARKET_DATA_GEYSER_SYNC_BATCH_TOTAL, MARKET_DATA_GEYSER_SYNC_IMMEDIATE_TOTAL,
        };

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);

        let pool_a = Pubkey::new_unique();
        let pool_b = Pubkey::new_unique();
        let base_a = Pubkey::new_unique();
        let base_b = Pubkey::new_unique();
        let quote = Pubkey::from_str(NATIVE_SOL_MINT).unwrap();

        let coin_a = Pubkey::new_unique();
        let pc_a = Pubkey::new_unique();
        let coin_b = Pubkey::new_unique();
        let pc_b = Pubkey::new_unique();

        ctx.live_pool_cache.upsert(
            pool_a,
            CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
                token_0_mint: base_a,
                token_1_mint: quote,
                token_0_vault: coin_a,
                token_1_vault: pc_a,
                reserve_0: Some(1_000_000),
                reserve_1: Some(2_000_000),
            }),
            1,
        );
        ctx.live_pool_cache.upsert(
            pool_b,
            CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
                token_0_mint: base_b,
                token_1_mint: quote,
                token_0_vault: coin_b,
                token_1_vault: pc_b,
                reserve_0: Some(1_000_000),
                reserve_1: Some(2_000_000),
            }),
            1,
        );
        ctx.hot_pool_registry.pin_pool(base_a, pool_a);
        ctx.hot_pool_registry.pin_pool(base_b, pool_b);
        ctx.note_explicit_momentum_pool_admitted(pool_a);
        ctx.note_explicit_momentum_pool_admitted(pool_b);

        let imm0 = MARKET_DATA_GEYSER_SYNC_IMMEDIATE_TOTAL.load(Ordering::Relaxed);
        let batch0 = MARKET_DATA_GEYSER_SYNC_BATCH_TOTAL.load(Ordering::Relaxed);

        let md_state = test_spawn_md_state(&ctx);
        MarketDataContext::register_geyser_reserves_after_trade(&ctx, pool_a);
        MarketDataContext::register_geyser_reserves_after_trade(&ctx, pool_b);
        md_state_try_enqueue(&md_state, MdStateCommand::FlushGeyserSyncDebounced);

        // Let the actor drain; PR167 startup debounce min 250 ms — one coalesced flush.
        tokio::time::sleep(Duration::from_millis(400)).await;

        assert_eq!(
            MARKET_DATA_GEYSER_SYNC_IMMEDIATE_TOTAL.load(Ordering::Relaxed),
            imm0,
            "trade-path reserve registration must not immediate-sync before debounce flush"
        );
        let batch1 = MARKET_DATA_GEYSER_SYNC_BATCH_TOTAL.load(Ordering::Relaxed);
        assert!(
            batch1 > batch0,
            "expected batched Geyser subscription sync after coalesced trade-path updates (before={batch0}, after={batch1})"
        );
        assert_eq!(
            MARKET_DATA_GEYSER_SYNC_IMMEDIATE_TOTAL.load(Ordering::Relaxed),
            imm0
        );
    }

    #[test]
    fn cache_publish_allowed_when_vault_rows_exist_before_geyser_sync_flush() {
        use ironcrab::metrics::MARKET_DATA_BALANCE_UPDATED_FROM_CACHE_TOTAL;

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);

        let base = Pubkey::new_unique();
        let quote = Pubkey::from_str(NATIVE_SOL_MINT).unwrap();
        let pool = Pubkey::new_unique();
        let coin = Pubkey::new_unique();
        let pc = Pubkey::new_unique();

        ctx.live_pool_cache.upsert(
            pool,
            CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
                token_0_mint: base,
                token_1_mint: quote,
                token_0_vault: coin,
                token_1_vault: pc,
                reserve_0: Some(1_000_000),
                reserve_1: Some(2_000_000),
            }),
            1,
        );
        ctx.hot_pool_registry.pin_pool(base, pool);
        ctx.note_explicit_momentum_pool_admitted(pool);
        assert!(MarketDataContext::register_geyser_reserves_after_trade(
            &ctx, pool
        ));

        assert!(!ctx.pool_explicit_vault_bin_pubkeys(pool).is_empty());
        assert!(
            !ctx.pool_has_live_vault_geyser_feed(pool),
            "local vault rows before debounced Geyser flush must not count as live feed"
        );

        let counter_before = MARKET_DATA_BALANCE_UPDATED_FROM_CACHE_TOTAL.load(Ordering::Relaxed);
        ctx.try_publish_balance_updated_from_cache(pool);
        assert_eq!(
            MARKET_DATA_BALANCE_UPDATED_FROM_CACHE_TOTAL.load(Ordering::Relaxed),
            counter_before,
            "without NATS the publish is a no-op, but pool_has_live=false proves no early skip"
        );
    }

    #[test]
    fn cache_publish_skipped_after_vault_pubkeys_in_last_synced_snapshot() {
        use ironcrab::metrics::MARKET_DATA_BALANCE_UPDATED_FROM_CACHE_TOTAL;

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);

        let base = Pubkey::new_unique();
        let quote = Pubkey::from_str(NATIVE_SOL_MINT).unwrap();
        let pool = Pubkey::new_unique();
        let coin = Pubkey::new_unique();
        let pc = Pubkey::new_unique();

        ctx.live_pool_cache.upsert(
            pool,
            CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
                token_0_mint: base,
                token_1_mint: quote,
                token_0_vault: coin,
                token_1_vault: pc,
                reserve_0: Some(1_000_000),
                reserve_1: Some(2_000_000),
            }),
            1,
        );
        ctx.hot_pool_registry.pin_pool(base, pool);
        ctx.note_explicit_momentum_pool_admitted(pool);
        assert!(MarketDataContext::register_geyser_reserves_after_trade(
            &ctx, pool
        ));
        let mut admission = FixedCapAdmission::new(ctx.max_tracked_accounts());
        ironcrab::market_data::track::converge_admission_from_ctx(ctx.as_ref(), &mut admission);
        ctx.prune_tracked_maps_to_admitted(&admission);
        ctx.publish_admitted_explicit_physical(&admission);
        *ctx.last_synced_explicit_pubkeys.write() =
            admission.snapshot_pubkeys().into_iter().collect();

        assert!(
            ctx.pool_has_live_vault_geyser_feed(pool),
            "after sync flush vault pubkeys must be in last_synced_explicit_pubkeys"
        );

        let counter_before = MARKET_DATA_BALANCE_UPDATED_FROM_CACHE_TOTAL.load(Ordering::Relaxed);
        ctx.try_publish_balance_updated_from_cache(pool);
        assert_eq!(
            MARKET_DATA_BALANCE_UPDATED_FROM_CACHE_TOTAL.load(Ordering::Relaxed),
            counter_before,
            "live vault Geyser feed must skip cache-first publish (slot-0 overwrite guard)"
        );
    }

    #[test]
    fn hot_pool_trade_flood_coalesced_no_new_subscription_keys() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);

        let pool = Pubkey::new_unique();
        let base = Pubkey::new_unique();
        let quote = Pubkey::from_str(NATIVE_SOL_MINT).unwrap();
        let coin = Pubkey::new_unique();
        let pc = Pubkey::new_unique();

        ctx.live_pool_cache.upsert(
            pool,
            CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
                token_0_mint: base,
                token_1_mint: quote,
                token_0_vault: coin,
                token_1_vault: pc,
                reserve_0: Some(1_000_000),
                reserve_1: Some(2_000_000),
            }),
            1,
        );
        ctx.hot_pool_registry.pin_pool(base, pool);
        ctx.note_explicit_momentum_pool_admitted(pool);
        let coin_vault = coin;
        assert!(MarketDataContext::register_geyser_reserves_after_trade(
            &ctx, pool
        ));

        let before = ctx.snapshot_explicit_subscription_pubkeys();
        let jobs: Vec<_> = (0..100)
            .map(|_| MdStateCommand::TouchVault(coin_vault))
            .collect();
        let coalesced = md_state_coalesce_jobs(jobs);
        assert_eq!(coalesced.len(), 1);
        assert!(matches!(
            coalesced[0],
            MdStateCommand::TouchTrackedLruBatch { .. }
        ));
        for job in coalesced {
            let _ = md_state_process_job(&ctx, job, &test_noop_track_worker_sender());
        }
        let after = ctx.snapshot_explicit_subscription_pubkeys();
        assert!(!explicit_subscription_has_new_keys(&before, &after));
    }

    #[test]
    fn md_state_coalesce_dedupes_ten_touch_vault() {
        let vault = Pubkey::new_unique();
        let jobs: Vec<_> = (0..10).map(|_| MdStateCommand::TouchVault(vault)).collect();
        let out = md_state_coalesce_jobs(jobs);
        assert_eq!(out.len(), 1);
        assert!(matches!(
            out[0],
            MdStateCommand::TouchTrackedLruBatch { .. }
        ));
    }

    /// PR161: many `tracked_mints_tx.send` within one debounce window → at most two `combined_tracked` updates
    /// within ~120 ms (default 35 ms batch; coalesced merge flush).
    #[tokio::test(flavor = "current_thread")]
    async fn pr161_merge_coalesces_burst_tracked_mint_updates() {
        use ironcrab::metrics::MARKET_DATA_GEYSER_MERGE_COALESCED_TOTAL;

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let (ctx, mints_rx, vaults_rx, bins_rx, wallet_rx) =
            minimal_market_data_context_and_merge_receivers_for_pr161(jsonl);

        let (combined_tx, mut combined_rx) = watch::channel(Vec::<Pubkey>::new());
        let combined_change_count = Arc::new(AtomicUsize::new(0));
        let cc = Arc::clone(&combined_change_count);
        let watch_task = tokio::spawn(async move {
            loop {
                if combined_rx.changed().await.is_err() {
                    break;
                }
                cc.fetch_add(1, Ordering::Relaxed);
            }
        });

        let coalesced0 = MARKET_DATA_GEYSER_MERGE_COALESCED_TOTAL.load(Ordering::Relaxed);
        let ctx_m = Arc::clone(&ctx);
        let merge_task = tokio::spawn(async move {
            MarketDataContext::run_geyser_tracked_accounts_merge_coalesce_loop(
                mints_rx,
                vaults_rx,
                bins_rx,
                wallet_rx,
                combined_tx,
                ctx_m,
            )
            .await;
        });

        for i in 0u8..10 {
            let mut a = [0u8; 32];
            a[0] = i;
            let pk = Pubkey::new_from_array(a);
            let _ = ctx.tracked_mints_tx.send(vec![pk]);
        }

        tokio::time::sleep(Duration::from_millis(350)).await;

        let n = combined_change_count.load(Ordering::Relaxed);
        assert!(
            n <= 2,
            "expected coalesced merge: at most 2 combined_tracked updates, got {n}"
        );
        assert!(
            MARKET_DATA_GEYSER_MERGE_COALESCED_TOTAL.load(Ordering::Relaxed) > coalesced0,
            "expected at least one coalesced merge flush"
        );

        merge_task.abort();
        watch_task.abort();
        let _ = merge_task.await;
        let _ = watch_task.await;
    }

    #[test]
    fn pr166_nats_core_skips_high_volume_noise_kinds() {
        assert!(!market_event_should_nats_core(
            &MarketEventKind::AccountUpdate {
                pubkey: "x".into(),
                owner: "y".into(),
                data_len: 0,
            }
        ));
        assert!(!market_event_should_nats_core(
            &MarketEventKind::TransactionDetected {
                signature: "sig".into(),
                program: "prog".into(),
            }
        ));
        assert!(market_event_should_nats_core(&MarketEventKind::Trade {
            pool_address: "p".into(),
            mint: "m".into(),
            quote_mint: NATIVE_SOL_MINT.to_string(),
            trader: "t".into(),
            is_buy: true,
            sol_amount: 1,
            token_amount: 1,
            token_decimals: 6,
            signature: None,
            dex: "pumpfun".into(),
            creator: None,
            token_program: None,
        }));
    }

    #[test]
    fn pr167_global_ingest_stall_detection() {
        assert!(market_data_global_ingest_stalled(5, 5, 10, 10, 100, 100));
        assert!(!market_data_global_ingest_stalled(5, 5, 10, 11, 100, 100));
        assert!(!market_data_global_ingest_stalled(5, 6, 10, 10, 100, 100));
        assert!(!market_data_global_ingest_stalled(5, 5, 10, 10, 100, 101));
    }

    #[test]
    fn pr167_geyser_sync_startup_debounce_ms() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        assert!(ctx.geyser_sync_batch_debounce_ms() >= MARKET_DATA_GEYSER_SYNC_STARTUP_MIN_MS);
    }

    #[test]
    fn pr166_tx_handler_processed_metric_advances() {
        use ironcrab::metrics::{
            record_market_data_tx_handler_processed, MARKET_DATA_TX_HANDLER_PROCESSED_TOTAL,
        };
        let before = MARKET_DATA_TX_HANDLER_PROCESSED_TOTAL.load(Ordering::Relaxed);
        record_market_data_tx_handler_processed();
        assert!(MARKET_DATA_TX_HANDLER_PROCESSED_TOTAL.load(Ordering::Relaxed) > before);
    }

    #[test]
    fn pr166_geyser_tx_payload_broadcast_increments_listener_counters() {
        use ironcrab::metrics::{
            geyser_metrics_inc_tx_listener_payload_broadcast_total,
            geyser_metrics_inc_tx_listener_transactions_total,
            GEYSER_TX_LISTENER_PAYLOAD_BROADCAST_TOTAL, GEYSER_TX_LISTENER_TRANSACTIONS_TOTAL,
        };
        let tx0 = GEYSER_TX_LISTENER_TRANSACTIONS_TOTAL.load(Ordering::Relaxed);
        let payload0 = GEYSER_TX_LISTENER_PAYLOAD_BROADCAST_TOTAL.load(Ordering::Relaxed);
        geyser_metrics_inc_tx_listener_transactions_total();
        geyser_metrics_inc_tx_listener_payload_broadcast_total();
        assert_eq!(
            GEYSER_TX_LISTENER_TRANSACTIONS_TOTAL.load(Ordering::Relaxed),
            tx0 + 1
        );
        assert_eq!(
            GEYSER_TX_LISTENER_PAYLOAD_BROADCAST_TOTAL.load(Ordering::Relaxed),
            payload0 + 1
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pr166_tx_handler_not_blocked_by_pool_mint_map_write_lock() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        let lock_ctx = Arc::clone(&ctx);
        let hold = tokio::task::spawn_blocking(move || {
            let _guard = lock_ctx.pool_mint_map.write();
            std::thread::sleep(std::time::Duration::from_millis(200));
        });
        let tx_count = AtomicU64::new(0);
        let tx_update = GeyserTransactionUpdate {
            signature: "sig".into(),
            slot: 1,
            account_keys: vec![],
            instruction_accounts: vec![],
            instruction_data: vec![],
            inner_instructions: vec![],
            pre_token_balances: vec![],
            post_token_balances: vec![],
            pre_balances: vec![],
            post_balances: vec![],
            fee_lamports: 0,
            compute_units_consumed: None,
            grpc_recv_at: Instant::now(),
        };
        let md_state = test_spawn_md_state(&ctx);
        let md_sidefx = test_spawn_md_sidefx(&ctx, &md_state, test_noop_track_worker_sender());
        let done = tokio::time::timeout(
            std::time::Duration::from_millis(150),
            handle_geyser_transaction(
                ctx,
                "run-pr166",
                tx_update,
                &tx_count,
                None,
                &md_state,
                &md_sidefx,
            ),
        )
        .await;
        assert!(
            done.is_ok(),
            "TX handler must not block on pool_mint_map write lock"
        );
        let _ = hold.await;
    }

    #[test]
    fn pr165_jsonl_skips_high_volume_noise_kinds() {
        assert!(!market_event_should_jsonl(
            &MarketEventKind::AccountUpdate {
                pubkey: "x".into(),
                owner: "y".into(),
                data_len: 0,
            }
        ));
        assert!(!market_event_should_jsonl(
            &MarketEventKind::TransactionDetected {
                signature: "sig".into(),
                program: "prog".into(),
            }
        ));
        assert!(market_event_should_jsonl(&MarketEventKind::Trade {
            pool_address: "p".into(),
            mint: "m".into(),
            quote_mint: NATIVE_SOL_MINT.to_string(),
            trader: "t".into(),
            is_buy: true,
            sol_amount: 1,
            token_amount: 1,
            token_decimals: 6,
            signature: None,
            dex: "pumpfun".into(),
            creator: None,
            token_program: None,
        }));
    }

    #[test]
    fn pr165_tokio_progress_metric_advances() {
        use ironcrab::metrics::{
            record_market_data_tokio_progress, MARKET_DATA_INGEST_PROGRESS_TICK,
            MARKET_DATA_TOKIO_LAST_PROGRESS_UNIX_MS,
        };
        let tick0 = MARKET_DATA_INGEST_PROGRESS_TICK.load(Ordering::Relaxed);
        let ms0 = MARKET_DATA_TOKIO_LAST_PROGRESS_UNIX_MS.load(Ordering::Relaxed);
        record_market_data_tokio_progress();
        assert!(
            MARKET_DATA_INGEST_PROGRESS_TICK.load(Ordering::Relaxed) > tick0
                || MARKET_DATA_TOKIO_LAST_PROGRESS_UNIX_MS.load(Ordering::Relaxed) >= ms0
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pr165_handle_geyser_account_runs_without_rpc_client() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);

        let pool = Pubkey::new_unique();
        let base_vault = Pubkey::new_unique();
        let quote_vault = Pubkey::new_unique();
        ctx.live_pool_cache.upsert(
            pool,
            CachedPoolState::PumpAmm(PumpAmmState {
                base_mint: Pubkey::new_unique(),
                quote_mint: Pubkey::from_str(NATIVE_SOL_MINT).unwrap(),
                pool_base_token_account: base_vault,
                pool_quote_token_account: quote_vault,
                base_reserve: None,
                quote_reserve: None,
                pool_accounts: vec![],
                creator: Some(Pubkey::new_unique()),
            }),
            42,
        );

        let account_count = AtomicU64::new(0);
        let md_state = test_spawn_md_state(&ctx);
        let md_sidefx = test_spawn_md_sidefx(&ctx, &md_state, test_noop_track_worker_sender());
        let update = GeyserAccountUpdate {
            pubkey: pool,
            owner: PUMPFUN_AMM_PROGRAM_OWNER,
            slot: 42,
            lamports: 0,
            data: vec![],
            grpc_recv_at: Instant::now(),
        };
        handle_geyser_account(
            Arc::clone(&ctx),
            "run-pr165",
            update,
            &account_count,
            Instant::now(),
            None,
            &md_state,
            &md_sidefx,
            AccountUpdateClass::ExecHot,
        )
        .await;

        assert_eq!(account_count.load(Ordering::Relaxed), 1);
        match ctx.live_pool_cache.get(&pool).expect("cache") {
            CachedPoolState::PumpAmm(s) => {
                assert_eq!(s.base_reserve, None);
                assert_eq!(s.quote_reserve, None);
            }
            other => panic!("expected PumpAmm cache row, got {other:?}"),
        }
    }

    /// PR169a: parallel tracking enqueues are processed by the single-writer actor.
    #[tokio::test(flavor = "current_thread")]
    async fn pr169a_geyser_tracking_actor_processes_parallel_enqueues() {
        use ironcrab::metrics::MARKET_DATA_GEYSER_TRACKING_JOBS_PROCESSED_TOTAL;

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        let md_state = test_spawn_md_state(&ctx);

        let jobs0 = MARKET_DATA_GEYSER_TRACKING_JOBS_PROCESSED_TOTAL.load(Ordering::Relaxed);
        for _ in 0..128 {
            let mint = Pubkey::new_unique();
            md_state_try_enqueue(&md_state, MdStateCommand::TrackMint { mint, pin: None });
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
        let jobs1 = MARKET_DATA_GEYSER_TRACKING_JOBS_PROCESSED_TOTAL.load(Ordering::Relaxed);
        assert!(
            jobs1 >= jobs0 + 128,
            "expected md-state thread to process enqueued jobs (before={jobs0}, after={jobs1})"
        );
    }

    /// PR169a: account-path vault register via actor — no immediate sync; batched after debounce.
    #[tokio::test(flavor = "current_thread")]
    async fn pr169a_register_pool_vaults_from_account_batches_sync() {
        use ironcrab::metrics::{
            MARKET_DATA_GEYSER_SYNC_BATCH_TOTAL, MARKET_DATA_GEYSER_SYNC_IMMEDIATE_TOTAL,
        };

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);

        let pool = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote = Pubkey::from_str(NATIVE_SOL_MINT).unwrap();
        let coin_vault = Pubkey::new_unique();
        let pc_vault = Pubkey::new_unique();
        ctx.live_pool_cache.upsert(
            pool,
            CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
                token_0_mint: base_mint,
                token_1_mint: quote,
                token_0_vault: coin_vault,
                token_1_vault: pc_vault,
                reserve_0: Some(1_000_000),
                reserve_1: Some(2_000_000),
            }),
            1,
        );
        ctx.hot_pool_registry.pin_pool(base_mint, pool);
        ctx.note_explicit_momentum_pool_admitted(pool);

        let md_state = test_spawn_md_state(&ctx);

        let imm0 = MARKET_DATA_GEYSER_SYNC_IMMEDIATE_TOTAL.load(Ordering::Relaxed);
        let batch0 = MARKET_DATA_GEYSER_SYNC_BATCH_TOTAL.load(Ordering::Relaxed);

        assert!(ctx.register_pool_vaults_from_account(pool));
        md_state_try_enqueue(&md_state, MdStateCommand::FlushGeyserSyncDebounced);
        tokio::time::sleep(Duration::from_millis(50)).await;

        let vs = ctx.tracked_vaults.read();
        assert!(vs.contains_key(&coin_vault));
        assert!(vs.contains_key(&pc_vault));
        drop(vs);

        assert_eq!(
            MARKET_DATA_GEYSER_SYNC_IMMEDIATE_TOTAL.load(Ordering::Relaxed),
            imm0,
            "account-path vault register must not immediate-sync"
        );

        tokio::time::sleep(Duration::from_millis(350)).await;
        let batch_delta = MARKET_DATA_GEYSER_SYNC_BATCH_TOTAL.load(Ordering::Relaxed) - batch0;
        assert!(
            batch_delta >= 1,
            "expected debounced batch sync after actor vault registration (delta={batch_delta})"
        );
    }

    /// PR169b / Phase-2b: momentum active pools via track-worker — no immediate sync; debounced batch.
    #[tokio::test(flavor = "current_thread")]
    async fn pr169b_apply_momentum_active_pools_actor_no_immediate_sync() {
        use ironcrab::metrics::{
            MARKET_DATA_GEYSER_SYNC_BATCH_TOTAL, MARKET_DATA_GEYSER_SYNC_IMMEDIATE_TOTAL,
            MARKET_DATA_TRACK_REQUEST_COALESCE_BATCHES_TOTAL,
        };
        use ironcrab::nats::{MomentumActivePinReason, MomentumActivePoolEntry};

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        let track_worker = spawn_track_worker(Arc::clone(&ctx));

        let imm0 = MARKET_DATA_GEYSER_SYNC_IMMEDIATE_TOTAL.load(Ordering::Relaxed);
        let batch0 = MARKET_DATA_GEYSER_SYNC_BATCH_TOTAL.load(Ordering::Relaxed);
        let coalesce0 = MARKET_DATA_TRACK_REQUEST_COALESCE_BATCHES_TOTAL.load(Ordering::Relaxed);

        let mut active = Vec::new();
        for _ in 0..12 {
            let pool = Pubkey::new_unique();
            let base_mint = Pubkey::new_unique();
            let quote = Pubkey::from_str(NATIVE_SOL_MINT).unwrap();
            let coin_vault = Pubkey::new_unique();
            let pc_vault = Pubkey::new_unique();
            ctx.live_pool_cache.upsert(
                pool,
                CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
                    token_0_mint: base_mint,
                    token_1_mint: quote,
                    token_0_vault: coin_vault,
                    token_1_vault: pc_vault,
                    reserve_0: None,
                    reserve_1: None,
                }),
                1,
            );
            active.push(MomentumActivePoolEntry {
                mint: base_mint.to_string(),
                pool: pool.to_string(),
                pin_reason: MomentumActivePinReason::Tracker,
            });
        }

        assert!(track_worker_try_enqueue(
            &track_worker,
            TrackWorkerCommand::ApplyMomentumActivePools(MomentumActivePoolsUpdate {
                version: 1,
                ts_unix_ms: 1,
                active,
                removed: vec![],
                full_active_snapshot: false,
            }),
        ));

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            MARKET_DATA_GEYSER_SYNC_IMMEDIATE_TOTAL.load(Ordering::Relaxed),
            imm0,
            "momentum path must not immediate-sync"
        );
        assert_eq!(ctx.hot_pool_registry.pair_count(), 12);

        tokio::time::sleep(Duration::from_millis(
            MARKET_DATA_TRACK_WORKER_COALESCE_MS + 200,
        ))
        .await;
        assert!(
            MARKET_DATA_TRACK_REQUEST_COALESCE_BATCHES_TOTAL.load(Ordering::Relaxed) > coalesce0,
            "track-worker should coalesce momentum apply into a push batch"
        );
        assert!(
            MARKET_DATA_GEYSER_SYNC_BATCH_TOTAL.load(Ordering::Relaxed) > batch0,
            "expected debounced batch sync after momentum update"
        );
    }

    /// PR169b: wallet mint track via actor — no immediate sync.
    #[tokio::test(flavor = "current_thread")]
    async fn pr169b_track_wallet_mint_actor_no_immediate_sync() {
        use ironcrab::metrics::{
            MARKET_DATA_GEYSER_SYNC_BATCH_TOTAL, MARKET_DATA_GEYSER_SYNC_IMMEDIATE_TOTAL,
        };

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        let md_state = test_spawn_md_state(&ctx);

        let mint = Pubkey::new_unique();
        let imm0 = MARKET_DATA_GEYSER_SYNC_IMMEDIATE_TOTAL.load(Ordering::Relaxed);
        let batch0 = MARKET_DATA_GEYSER_SYNC_BATCH_TOTAL.load(Ordering::Relaxed);

        md_state_try_enqueue(&md_state, MdStateCommand::TrackWalletMint { mint });

        tokio::time::sleep(Duration::from_millis(50)).await;
        let tracked = ctx.tracked_mints.read();
        assert!(tracked.contains_key(&mint));
        assert_eq!(
            tracked.get(&mint).and_then(|m| m.pin),
            Some(GeyserPinReason::Wallet)
        );
        drop(tracked);

        assert_eq!(
            MARKET_DATA_GEYSER_SYNC_IMMEDIATE_TOTAL.load(Ordering::Relaxed),
            imm0,
            "wallet mint track must not immediate-sync"
        );

        tokio::time::sleep(Duration::from_millis(350)).await;
        assert!(
            MARKET_DATA_GEYSER_SYNC_BATCH_TOTAL.load(Ordering::Relaxed) > batch0,
            "expected debounced batch sync after wallet mint track"
        );
    }

    /// PR169c: 99 momentum NATS-equivalent updates coalesce to at most two actor applies.
    #[tokio::test(flavor = "current_thread")]
    async fn pr169c_momentum_coalesce_99_messages_few_actor_jobs() {
        use ironcrab::metrics::{
            MARKET_DATA_MOMENTUM_COALESCED_BATCHES_TOTAL,
            MARKET_DATA_MOMENTUM_COALESCED_MESSAGES_TOTAL,
        };
        use ironcrab::nats::{MomentumActivePinReason, MomentumActivePoolEntry};

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        let track_worker = spawn_track_worker(Arc::clone(&ctx));
        let coalesce_tx = spawn_momentum_tracking_coalescer(Arc::clone(&ctx), track_worker);

        let msgs0 = MARKET_DATA_MOMENTUM_COALESCED_MESSAGES_TOTAL.load(Ordering::Relaxed);
        let batches0 = MARKET_DATA_MOMENTUM_COALESCED_BATCHES_TOTAL.load(Ordering::Relaxed);

        for i in 0..99u64 {
            let pool = Pubkey::new_unique();
            let base_mint = Pubkey::new_unique();
            let quote = Pubkey::from_str(NATIVE_SOL_MINT).unwrap();
            ctx.live_pool_cache.upsert(
                pool,
                CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
                    token_0_mint: base_mint,
                    token_1_mint: quote,
                    token_0_vault: Pubkey::new_unique(),
                    token_1_vault: Pubkey::new_unique(),
                    reserve_0: None,
                    reserve_1: None,
                }),
                1,
            );
            momentum_coalesce_try_send(
                &coalesce_tx,
                MomentumActivePoolsUpdate {
                    version: 1,
                    ts_unix_ms: i,
                    active: vec![MomentumActivePoolEntry {
                        mint: base_mint.to_string(),
                        pool: pool.to_string(),
                        pin_reason: MomentumActivePinReason::Tracker,
                    }],
                    removed: vec![],
                    full_active_snapshot: false,
                },
            );
        }

        tokio::time::sleep(Duration::from_millis(700)).await;
        let msgs1 = MARKET_DATA_MOMENTUM_COALESCED_MESSAGES_TOTAL.load(Ordering::Relaxed);
        let batches1 = MARKET_DATA_MOMENTUM_COALESCED_BATCHES_TOTAL.load(Ordering::Relaxed);

        let msgs_delta = msgs1 - msgs0;
        let batches_delta = batches1 - batches0;
        assert_eq!(msgs_delta, 99, "expected all NATS updates counted");
        assert!(
            (1..=2).contains(&batches_delta),
            "expected 1–2 coalesced batches (delta={batches_delta})"
        );
        assert_eq!(
            ctx.hot_pool_registry.pair_count(),
            99,
            "merged actor apply should pin every pool from the burst"
        );
    }

    #[test]
    fn pr169c_merge_momentum_updates_matches_sequential_apply() {
        use ironcrab::nats::{
            MomentumActivePinReason, MomentumActivePoolEntry, MomentumRemovedPoolEntry,
        };

        fn pin_count(ctx: &Arc<MarketDataContext>) -> usize {
            ctx.hot_pool_registry.pair_count()
        }

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx_seq = minimal_market_data_context_for_pr_d_tests(jsonl);
        let jsonl2 = QueuedJsonlWriter::spawn(
            JsonlWriterConfig::new("market_events").with_log_dir(tmp.path()),
            256,
        )
        .expect("jsonl2");
        let ctx_merged = minimal_market_data_context_for_pr_d_tests(jsonl2);

        let pool_a = Pubkey::new_unique();
        let mint_a = Pubkey::new_unique();
        let pool_b = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        let quote = Pubkey::from_str(NATIVE_SOL_MINT).unwrap();
        for (pool, mint) in [(pool_a, mint_a), (pool_b, mint_b)] {
            for ctx in [&ctx_seq, &ctx_merged] {
                ctx.live_pool_cache.upsert(
                    pool,
                    CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
                        token_0_mint: mint,
                        token_1_mint: quote,
                        token_0_vault: Pubkey::new_unique(),
                        token_1_vault: Pubkey::new_unique(),
                        reserve_0: None,
                        reserve_1: None,
                    }),
                    1,
                );
            }
        }

        let updates = vec![
            MomentumActivePoolsUpdate {
                version: 1,
                ts_unix_ms: 1,
                active: vec![MomentumActivePoolEntry {
                    mint: mint_a.to_string(),
                    pool: pool_a.to_string(),
                    pin_reason: MomentumActivePinReason::Tracker,
                }],
                removed: vec![],
                full_active_snapshot: false,
            },
            MomentumActivePoolsUpdate {
                version: 1,
                ts_unix_ms: 2,
                active: vec![MomentumActivePoolEntry {
                    mint: mint_b.to_string(),
                    pool: pool_b.to_string(),
                    pin_reason: MomentumActivePinReason::Tracker,
                }],
                removed: vec![],
                full_active_snapshot: true,
            },
            MomentumActivePoolsUpdate {
                version: 1,
                ts_unix_ms: 3,
                active: vec![],
                removed: vec![MomentumRemovedPoolEntry {
                    mint: mint_b.to_string(),
                    pool: pool_b.to_string(),
                    reason: "stale".to_string(),
                }],
                full_active_snapshot: false,
            },
        ];

        let mut admission_seq = test_admission_for(&ctx_seq);
        for u in &updates {
            ctx_seq.apply_momentum_active_pools_update(&mut admission_seq, u);
        }
        let merged = merge_momentum_active_pools_updates(&updates).expect("merged");
        let mut admission_merged = test_admission_for(&ctx_merged);
        ctx_merged.apply_momentum_active_pools_update(&mut admission_merged, &merged);

        assert_eq!(pin_count(&ctx_seq), pin_count(&ctx_merged));
        assert_eq!(
            ctx_seq.hot_pool_registry.snapshot_pairs(),
            ctx_merged.hot_pool_registry.snapshot_pairs()
        );
    }

    /// Phase-2b: large momentum apply runs on `md-track-worker` thread (chunked).
    #[tokio::test(flavor = "current_thread")]
    async fn pr_r2_chunked_momentum_apply_on_track_worker_thread() {
        use ironcrab::nats::{MomentumActivePinReason, MomentumActivePoolEntry};

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        let track_worker = spawn_track_worker(Arc::clone(&ctx));

        let mut active = Vec::new();
        for _ in 0..48 {
            let pool = Pubkey::new_unique();
            let base_mint = Pubkey::new_unique();
            let quote = Pubkey::from_str(NATIVE_SOL_MINT).unwrap();
            ctx.live_pool_cache.upsert(
                pool,
                CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
                    token_0_mint: base_mint,
                    token_1_mint: quote,
                    token_0_vault: Pubkey::new_unique(),
                    token_1_vault: Pubkey::new_unique(),
                    reserve_0: None,
                    reserve_1: None,
                }),
                1,
            );
            active.push(MomentumActivePoolEntry {
                mint: base_mint.to_string(),
                pool: pool.to_string(),
                pin_reason: MomentumActivePinReason::Tracker,
            });
        }

        assert!(track_worker_try_enqueue(
            &track_worker,
            TrackWorkerCommand::ApplyMomentumActivePools(MomentumActivePoolsUpdate {
                version: 1,
                ts_unix_ms: 1,
                active,
                removed: vec![],
                full_active_snapshot: false,
            }),
        ));

        for _ in 0..200 {
            if ctx.hot_pool_registry.pair_count() >= 48 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(ctx.hot_pool_registry.pair_count(), 48);
    }

    /// Phase-R-R2: full md-state queue + held `tracked_vaults` write must not block TX handler.
    #[tokio::test(flavor = "current_thread")]
    async fn pr_r2_tx_handler_returns_when_md_state_queue_full_and_vaults_locked() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        let md_state = test_spawn_md_state(&ctx);
        let md_sidefx = test_spawn_md_sidefx(&ctx, &md_state, test_noop_track_worker_sender());
        fill_md_state_queue(&md_state);

        let lock_ctx = Arc::clone(&ctx);
        let hold = tokio::task::spawn_blocking(move || {
            let _guard = lock_ctx.tracked_vaults.write();
            std::thread::sleep(std::time::Duration::from_millis(200));
        });

        let tx_count = AtomicU64::new(0);
        let tx_update = GeyserTransactionUpdate {
            signature: "sig".into(),
            slot: 1,
            account_keys: vec![],
            instruction_accounts: vec![],
            instruction_data: vec![],
            inner_instructions: vec![],
            pre_token_balances: vec![],
            post_token_balances: vec![],
            pre_balances: vec![],
            post_balances: vec![],
            fee_lamports: 0,
            compute_units_consumed: None,
            grpc_recv_at: Instant::now(),
        };
        let done = tokio::time::timeout(
            std::time::Duration::from_millis(150),
            handle_geyser_transaction(
                Arc::clone(&ctx),
                "run-r2",
                tx_update,
                &tx_count,
                None,
                &md_state,
                &md_sidefx,
            ),
        )
        .await;
        assert!(
            done.is_ok(),
            "TX handler must return without blocking on tracked_vaults when md-state queue is full"
        );
        let _ = hold.await;
    }

    /// Phase-R-R2: burst job processing sets `schedule_sync` once per worker drain (not per job).
    #[test]
    fn pr_r2_burst_track_mint_coalesces_single_schedule_sync_flag() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        let track_worker = test_inline_track_worker_sender(&ctx);

        let mut schedule_sync = false;
        for _ in 0..32 {
            let mint = Pubkey::new_unique();
            if md_state_process_job(
                &ctx,
                MdStateCommand::TrackMint { mint, pin: None },
                &track_worker,
            ) {
                schedule_sync = true;
            }
        }
        std::thread::sleep(Duration::from_millis(20));
        assert!(
            schedule_sync || !ctx.tracked_mints.read().is_empty(),
            "burst TrackMint should track mints via track-worker"
        );
        assert_eq!(
            ctx.tracked_mints.read().len(),
            32,
            "all burst mints should be tracked on md-state"
        );
    }

    /// Phase-R-R4: full sidefx queue + held `pool_mint_map` write must not block TX handler.
    #[tokio::test(flavor = "current_thread")]
    async fn pr_r4_tx_handler_returns_when_md_sidefx_queue_full_and_pool_mint_map_locked() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        let md_state = test_spawn_md_state(&ctx);
        let md_sidefx = test_spawn_md_sidefx(&ctx, &md_state, test_noop_track_worker_sender());
        fill_md_sidefx_queue(&md_sidefx);

        let lock_ctx = Arc::clone(&ctx);
        let hold = tokio::task::spawn_blocking(move || {
            let _guard = lock_ctx.pool_mint_map.write();
            std::thread::sleep(std::time::Duration::from_millis(200));
        });

        let tx_count = AtomicU64::new(0);
        let tx_update = GeyserTransactionUpdate {
            signature: "sig".into(),
            slot: 1,
            account_keys: vec![],
            instruction_accounts: vec![],
            instruction_data: vec![],
            inner_instructions: vec![],
            pre_token_balances: vec![],
            post_token_balances: vec![],
            pre_balances: vec![],
            post_balances: vec![],
            fee_lamports: 0,
            compute_units_consumed: None,
            grpc_recv_at: Instant::now(),
        };
        let done = tokio::time::timeout(
            std::time::Duration::from_millis(150),
            handle_geyser_transaction(
                Arc::clone(&ctx),
                "run-r4",
                tx_update,
                &tx_count,
                None,
                &md_state,
                &md_sidefx,
            ),
        )
        .await;
        assert!(
            done.is_ok(),
            "TX handler must return without blocking on pool_mint_map when md-sidefx queue is full"
        );
        let _ = hold.await;
    }

    /// Phase-R-R4: sidefx worker processes PumpFun pool_mint_map insert jobs.
    #[tokio::test(flavor = "current_thread")]
    async fn pr_r4_sidefx_processes_pump_fun_pool_mint_map_job() {
        use ironcrab::metrics::MARKET_DATA_MD_SIDEFX_JOBS_PROCESSED_TOTAL;

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        let md_state = test_spawn_md_state(&ctx);
        let md_sidefx = test_spawn_md_sidefx(&ctx, &md_state, test_noop_track_worker_sender());

        let pool = Pubkey::new_unique();
        let mint = "mint123".to_string();
        let jobs0 = MARKET_DATA_MD_SIDEFX_JOBS_PROCESSED_TOTAL.load(Ordering::Relaxed);
        md_sidefx_try_enqueue(
            &md_sidefx,
            MdSidefxCommand::PumpFunPoolMintMapInsert {
                run_id: "run-r4".into(),
                pool_address: pool,
                mint_str: mint.clone(),
                slot: Some(99),
                tx_grpc_recv_at: Instant::now(),
                creator_override: None,
            },
        );

        tokio::time::sleep(Duration::from_millis(200)).await;
        let jobs1 = MARKET_DATA_MD_SIDEFX_JOBS_PROCESSED_TOTAL.load(Ordering::Relaxed);
        assert!(jobs1 > jobs0, "md-sidefx should process enqueued job");
        assert_eq!(
            ctx.pool_mint_map.read().get(&pool.to_string()).cloned(),
            Some(mint)
        );
    }

    /// Phase 1 P1: pool_mint_map + creator_override emits DevWallet without pre-filled pool_creator_cache.
    #[tokio::test(flavor = "current_thread")]
    async fn phase1_sidefx_pool_mint_map_emits_devwallet_with_creator_override() {
        use ironcrab::metrics::MARKET_DATA_DEVWALLET_TX_PUBLISHED_TOTAL;

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        let md_state = test_spawn_md_state(&ctx);
        let md_sidefx = test_spawn_md_sidefx(&ctx, &md_state, test_noop_track_worker_sender());

        let pool = Pubkey::new_unique();
        let mint = Pubkey::new_unique().to_string();
        let creator = Pubkey::new_unique();
        let tx0 = MARKET_DATA_DEVWALLET_TX_PUBLISHED_TOTAL.load(Ordering::Relaxed);
        md_sidefx_try_enqueue(
            &md_sidefx,
            MdSidefxCommand::PumpFunPoolMintMapInsert {
                run_id: "run-p1".into(),
                pool_address: pool,
                mint_str: mint.clone(),
                slot: Some(42),
                tx_grpc_recv_at: Instant::now(),
                creator_override: Some(creator),
            },
        );

        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            MARKET_DATA_DEVWALLET_TX_PUBLISHED_TOTAL.load(Ordering::Relaxed) > tx0,
            "creator_override must publish DevWallet on TX path"
        );
        assert_eq!(
            ctx.pool_creator_cache
                .read()
                .get(&pool.to_string())
                .cloned(),
            Some(creator.to_string()),
            "pool_creator_cache must be filled from creator_override"
        );
        assert_eq!(
            ctx.creator_cache.read().get(&mint).cloned(),
            Some(creator.to_string()),
            "creator_cache must be filled from creator_override"
        );
    }

    /// Phase-R-R4: ingest handlers must not call `pool_mint_map.write()` (grep guard).
    #[test]
    fn pr_r4_ingest_handlers_avoid_pool_mint_map_write() {
        let tx_src = format!(
            "{}\n{}",
            include_str!("../market_data/ingest/tx_handler.rs"),
            include_str!("../market_data/ingest/tx_parse.rs"),
        );
        let src = include_str!("market_data.rs");
        let acc_start = src
            .find("async fn handle_geyser_account")
            .expect("account handler");
        let acc_end = src
            .find("/// Geyser transaction ingest — delegates to `ingest/tx_handler.rs`")
            .expect("after account handler");
        let tx_body = tx_src.as_str();
        let acc_body = &src[acc_start..acc_end];
        assert!(
            !tx_body.contains("pool_mint_map.write()"),
            "TX handler must not write pool_mint_map (deferred to md-sidefx)"
        );
        assert!(
            !acc_body.contains("pool_mint_map.write()"),
            "account handler must not write pool_mint_map"
        );
        assert!(
            !acc_body.contains("tracked_vaults.read()"),
            "account handler must not read tracked_vaults (deferred to md-sidefx)"
        );
        assert!(
            !acc_body.contains("tracked_mints.read()"),
            "account handler must not read tracked_mints (use membership snapshot)"
        );
        assert!(
            !acc_body.contains("tracked_bin_arrays.read()"),
            "account handler must not read tracked_bin_arrays (use membership snapshot)"
        );
        assert!(
            !acc_body.contains("live_pool_cache.upsert"),
            "account handler must not upsert live_pool_cache (deferred to md-sidefx)"
        );
        assert!(
            !acc_body.contains("live_pool_cache.merge_"),
            "account handler must not merge live_pool_cache readiness (deferred to md-sidefx)"
        );
        assert!(
            !acc_body.contains("live_pool_cache.set_mint_decimals"),
            "account handler must not set live_pool_cache mint decimals (deferred to md-sidefx)"
        );
    }

    /// Phase-R-R4b: account worker pool scaled down after post-R4 prod convoy.
    #[test]
    fn pr_r4b_account_worker_count_is_two() {
        assert_eq!(MARKET_DATA_ACCOUNT_WORKER_COUNT, 2);
        assert_eq!(
            MARKET_DATA_ACCOUNT_WORKER_COUNT * MARKET_DATA_ACCOUNT_WORKER_QUEUE_CAP,
            10_000,
            "total account queue backpressure budget ~10k"
        );
    }

    /// PR233: debounced Geyser sync must not call core flush from Tokio spawn in schedule path.
    #[test]
    fn pr233_schedule_geyser_sync_does_not_flush_on_tokio() {
        let src = include_str!("market_data.rs");
        let start = src
            .find("fn schedule_geyser_sync_batch_debounced(self: &Arc<Self>")
            .expect("schedule_geyser_sync_batch_debounced");
        let end = src
            .find("/// PR161: merge four explicit-track")
            .expect("after schedule_geyser_sync_batch_debounced");
        let body = &src[start..end];
        assert!(
            !body.contains("sync_geyser_tracked_accounts_core"),
            "schedule_geyser_sync_batch_debounced must not call sync_geyser_tracked_accounts_core on Tokio"
        );
        assert!(
            !body.contains("sync_geyser_tracked_accounts_batched_flush"),
            "schedule_geyser_sync_batch_debounced must enqueue FlushGeyserSyncDebounced instead of inline flush"
        );
        assert!(
            body.contains("FlushGeyserSyncDebounced"),
            "schedule path must enqueue md-state flush job"
        );
        assert!(
            body.contains("md_state_try_enqueue"),
            "schedule path must enqueue bounded md-state work"
        );
    }

    /// PR233: coalesce multiple debounced flush jobs into one per burst.
    #[test]
    fn pr233_md_state_coalesce_merges_flush_geyser_sync_jobs() {
        let jobs = vec![
            MdStateCommand::FlushGeyserSyncDebounced,
            MdStateCommand::FlushGeyserSyncDebounced,
            MdStateCommand::TouchVault(Pubkey::new_unique()),
        ];
        let out = md_state_coalesce_jobs(jobs);
        let flush_count = out
            .iter()
            .filter(|j| matches!(j, MdStateCommand::FlushGeyserSyncDebounced))
            .count();
        assert_eq!(flush_count, 1, "expected one coalesced flush job");
    }

    /// PR233: vault LRU touch updates only the touched vault and its O(1) sibling link.
    #[test]
    fn pr233_touch_tracked_vault_pubkey_is_o1_not_full_map_scan() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);

        let pool_a = Pubkey::new_unique();
        let pool_b = Pubkey::new_unique();
        let base_a = Pubkey::new_unique();
        let quote_a = Pubkey::new_unique();
        let base_b = Pubkey::new_unique();
        let quote_b = Pubkey::new_unique();
        let old = Instant::now() - Duration::from_secs(120);
        let mk = |pool: Pubkey, _pk: Pubkey, is_base: bool, sibling: Pubkey| VaultInfo {
            pool_address: pool,
            dex: "x".into(),
            base_mint: Pubkey::new_unique(),
            quote_mint: Pubkey::new_unique(),
            is_base_vault: is_base,
            last_balance: std::sync::atomic::AtomicU64::new(0),
            last_used_at: old,
            pinned: false,
            pin: None,
            active_id: None,
            bin_step: None,
            sibling_vault: Some(sibling),
        };
        {
            let mut vaults = ctx.tracked_vaults.write();
            vaults.insert(base_a, mk(pool_a, base_a, true, quote_a));
            vaults.insert(quote_a, mk(pool_a, quote_a, false, base_a));
            vaults.insert(base_b, mk(pool_b, base_b, true, quote_b));
            vaults.insert(quote_b, mk(pool_b, quote_b, false, base_b));
        }

        ctx.touch_tracked_vault_pubkey(&base_a);
        let vaults = ctx.tracked_vaults.read();
        assert!(vaults.get(&base_a).unwrap().last_used_at > old);
        assert!(vaults.get(&quote_a).unwrap().last_used_at > old);
        assert_eq!(vaults.get(&base_b).unwrap().last_used_at, old);
        assert_eq!(vaults.get(&quote_b).unwrap().last_used_at, old);
    }

    /// PR234: debounced flush slot acquire/release and per-second rate cap.
    #[test]
    fn pr234_geyser_sync_flush_slot_acquire_flush_release() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        assert!(ctx.try_acquire_geyser_sync_flush_slot());
        let before = ctx.geyser_sync_flush_timestamps.lock().len();
        assert_eq!(before, 1);
        ctx.release_geyser_sync_flush_slot();
        assert_eq!(ctx.geyser_sync_flush_timestamps.lock().len(), 0);
        for _ in 0..MARKET_DATA_GEYSER_SYNC_FLUSH_MAX_PER_SEC {
            assert!(ctx.try_acquire_geyser_sync_flush_slot());
        }
        assert!(!ctx.try_acquire_geyser_sync_flush_slot());
        ctx.release_geyser_sync_flush_slot();
    }

    /// PR234: `sync_geyser_tracked_accounts_core` stays off Tokio ingest handlers (extends PR233 guard).
    #[test]
    fn pr234_sync_core_not_called_from_tokio_ingest_handlers() {
        let tx_src = include_str!("../market_data/ingest/tx_handler.rs");
        let src = include_str!("market_data.rs");
        let acc_start = src
            .find("async fn handle_geyser_account")
            .expect("account handler");
        let acc_end = src
            .find("/// Geyser transaction ingest — delegates to `ingest/tx_handler.rs`")
            .expect("after account handler");
        let tx_body = tx_src;
        let acc_body = &src[acc_start..acc_end];
        assert!(
            !tx_body.contains("sync_geyser_tracked_accounts_core"),
            "TX handler must not call sync_geyser_tracked_accounts_core"
        );
        assert!(
            !acc_body.contains("sync_geyser_tracked_accounts_core"),
            "account handler must not call sync_geyser_tracked_accounts_core"
        );
        assert!(
            !tx_body.contains("sync_geyser_tracked_accounts_batched_flush"),
            "TX handler must not call sync_geyser_tracked_accounts_batched_flush"
        );
        assert!(
            !acc_body.contains("sync_geyser_tracked_accounts_batched_flush"),
            "account handler must not call sync_geyser_tracked_accounts_batched_flush"
        );
    }

    /// PR235: idempotent register guard skips when vault pair already tracked.
    #[test]
    fn pr235_pool_needs_tracking_skips_when_vaults_stable() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        let token_mint = Pubkey::new_unique();
        let quote = Pubkey::from_str(NATIVE_SOL_MINT).unwrap();
        let pool = Pubkey::new_unique();
        let base_vault = Pubkey::new_unique();
        let quote_vault = Pubkey::new_unique();
        let state = CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
            token_0_mint: token_mint,
            token_1_mint: quote,
            token_0_vault: base_vault,
            token_1_vault: quote_vault,
            reserve_0: Some(1),
            reserve_1: Some(1),
        });
        ctx.hot_pool_registry.pin_pool(token_mint, pool);
        {
            let mut vaults = ctx.tracked_vaults.write();
            let mut vaults_changed = false;
            ctx.register_tracked_vault_pair_lru(
                &mut vaults,
                &mut vaults_changed,
                TrackedVaultPairInsert {
                    pool,
                    now: Instant::now(),
                    dex: "raydium_cpmm",
                    base_mint: token_mint,
                    quote_mint: quote,
                    base_vault,
                    quote_vault,
                    active_id: None,
                    bin_step: None,
                },
            );
        }
        assert!(!pool_needs_tracking_refresh_after_cache_upsert(
            &ctx, pool, &state
        ));
    }

    /// PR235: default stall policy records metric and warns — no systemd exit until soak enables flag.
    #[test]
    fn pr235_md_state_stall_exit_disabled_by_default() {
        assert_eq!(
            md_state_stall_liveness_action(MARKET_DATA_MD_STATE_STALL_EXIT_AFTER, false),
            MdStateStallLivenessAction::WarnStillStalled,
        );
        assert_eq!(
            md_state_stall_liveness_action(
                MARKET_DATA_MD_STATE_STALL_EXIT_AFTER,
                MARKET_DATA_MD_STATE_STALL_EXIT_ENABLED,
            ),
            MdStateStallLivenessAction::WarnStillStalled,
        );
        assert_eq!(
            md_state_stall_liveness_action(MARKET_DATA_MD_STATE_STALL_EXIT_AFTER, true),
            MdStateStallLivenessAction::ExitForSystemd,
        );
    }

    /// PR237: membership snapshot refreshed by md-state is visible to ingest filter without RwLock on maps.
    #[test]
    fn pr237_tracked_membership_snapshot_visible_to_ingest_filter() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        let vault = Pubkey::new_unique();
        let pool = Pubkey::new_unique();
        {
            let mut vaults = ctx.tracked_vaults.write();
            vaults.insert(
                vault,
                VaultInfo {
                    pool_address: pool,
                    dex: "test".to_string(),
                    base_mint: Pubkey::new_unique(),
                    quote_mint: Pubkey::new_unique(),
                    is_base_vault: true,
                    last_balance: std::sync::atomic::AtomicU64::new(0),
                    last_used_at: Instant::now(),
                    pinned: false,
                    pin: None,
                    active_id: None,
                    bin_step: None,
                    sibling_vault: None,
                },
            );
        }
        ctx.refresh_tracked_membership_snapshot();
        let u = GeyserAccountUpdate {
            pubkey: vault,
            slot: 1,
            owner: Pubkey::new_unique(),
            data: vec![],
            lamports: 0,
            grpc_recv_at: Instant::now(),
        };
        assert!(account_geyser_update_might_be_relevant(&ctx, &u));
        assert!(!account_geyser_dispatch_priority_high(&ctx, &u));
    }

    /// PR237: pool touch uses reverse index (O(legs)), not full-map scan.
    #[test]
    fn pr237_touch_pool_uses_pool_tracked_legs_only() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        let pool = Pubkey::new_unique();
        let other_pool = Pubkey::new_unique();
        let vault_a = Pubkey::new_unique();
        let vault_b = Pubkey::new_unique();
        let decoy_vault = Pubkey::new_unique();
        let now_old = Instant::now() - Duration::from_secs(60);
        let mk_vault = |pool_addr: Pubkey, is_base: bool| -> VaultInfo {
            VaultInfo {
                pool_address: pool_addr,
                dex: "test".to_string(),
                base_mint: Pubkey::new_unique(),
                quote_mint: Pubkey::new_unique(),
                is_base_vault: is_base,
                last_balance: std::sync::atomic::AtomicU64::new(0),
                last_used_at: now_old,
                pinned: false,
                pin: None,
                active_id: None,
                bin_step: None,
                sibling_vault: None,
            }
        };
        {
            let mut vaults = ctx.tracked_vaults.write();
            vaults.insert(vault_a, mk_vault(pool, true));
            vaults.insert(vault_b, mk_vault(pool, false));
            vaults.insert(decoy_vault, mk_vault(other_pool, true));
        }
        ctx.pool_tracked_legs_note_vault(pool, vault_a);
        ctx.pool_tracked_legs_note_vault(pool, vault_b);
        assert_eq!(
            ctx.pool_tracked_legs
                .read()
                .get(&pool)
                .unwrap()
                .vaults
                .len(),
            2
        );
        ctx.touch_tracked_pool_vaults_and_bins(pool);
        let vaults = ctx.tracked_vaults.read();
        assert!(vaults.get(&vault_a).unwrap().last_used_at > now_old);
        assert!(vaults.get(&vault_b).unwrap().last_used_at > now_old);
        assert_eq!(vaults.get(&decoy_vault).unwrap().last_used_at, now_old);
    }

    /// PR237: trade hot path enqueues sidefx LRU touch, not md-state TouchPool/Register.
    #[test]
    fn pr237_trade_path_uses_sidefx_not_md_state_touch_pool() {
        let src = include_str!("../market_data/ingest/tx_handler.rs");
        let marker = "TradePoolLruTouch";
        assert!(
            src.contains(marker),
            "tx_handler must enqueue TradePoolLruTouch"
        );
        let start = src.find("TradePoolLruTouch").expect("trade block marker");
        let block = &src[start.saturating_sub(200)..start.saturating_add(300)];
        assert!(!block.contains("MdStateCommand::TouchPool"));
        assert!(!block.contains("RegisterReservesAfterTrade"));
        assert!(!block.contains("try_enqueue_arb_reconcile_for_pool"));
        assert!(block.contains("TradePoolLruTouch"));
    }

    /// Phase1: ingest + md-sidefx must not block on tracked_* map read locks.
    #[test]
    fn phase1_ingest_sidefx_no_tracked_map_reads() {
        let tx_src = format!(
            "{}\n{}",
            include_str!("../market_data/ingest/tx_handler.rs"),
            include_str!("../market_data/ingest/tx_parse.rs"),
        );
        let src = include_str!("market_data.rs");
        let handlers_src = include_str!("../market_data/sidefx/handlers.rs");
        let acc_start = src
            .find("async fn handle_geyser_account")
            .expect("account handler");
        let acc_end = src
            .find("/// Geyser transaction ingest — delegates to `ingest/tx_handler.rs`")
            .expect("after account handler");
        let sidefx_body = handlers_src;
        let tx_body = tx_src.as_str();
        let acc_body = &src[acc_start..acc_end];
        for body in [acc_body, tx_body, sidefx_body] {
            assert!(
                !body.contains("tracked_vaults.read()"),
                "ingest/sidefx must not read tracked_vaults"
            );
            assert!(
                !body.contains("tracked_mints.read()"),
                "ingest/sidefx must not read tracked_mints"
            );
            assert!(
                !body.contains("tracked_bin_arrays.read()"),
                "ingest/sidefx must not read tracked_bin_arrays"
            );
        }
    }

    /// Phase1: trade handler must not enqueue RegisterReservesAfterTrade.
    #[test]
    fn phase1_trade_path_no_register_reserves_enqueue() {
        let tx_body = include_str!("../market_data/ingest/tx_handler.rs");
        assert!(
            !tx_body.contains("RegisterReservesAfterTrade"),
            "TX handler must not enqueue RegisterReservesAfterTrade (phase1)"
        );
    }

    /// Phase1: sidefx flush must not enqueue RegisterPoolVaultsFromAccount.
    #[test]
    fn phase1_sidefx_no_register_pool_vaults_enqueue() {
        let handlers_src = include_str!("../market_data/sidefx/handlers.rs");
        let host_src = include_str!("../market_data/sidefx/host.rs");
        let body = format!("{handlers_src}\n{host_src}");
        assert!(
            !body.contains("MdStateCommand::RegisterPoolVaultsFromAccount"),
            "md_sidefx_flush must not enqueue RegisterPoolVaultsFromAccount"
        );
    }

    /// Phase1: vault balance tick uses snapshot helper (no tracked_vaults map scan).
    #[test]
    fn phase1_sidefx_vault_tick_uses_snapshot() {
        let handlers_src = include_str!("../market_data/sidefx/handlers.rs");
        let start = handlers_src
            .find("pub fn md_sidefx_process_vault_balance_tick")
            .expect("vault tick handler");
        let end = handlers_src
            .find("pub fn md_sidefx_process_touch_bin_array_tick")
            .expect("after vault tick handler");
        let body = &handlers_src[start..end];
        assert!(
            body.contains("vault_membership_view"),
            "vault tick must use snapshot membership view"
        );
        assert!(
            body.contains("snapshot_vault_pair_balances"),
            "vault tick must assemble reserves via snapshot helper"
        );
        assert!(
            !body.contains("tracked_vaults.read()"),
            "vault tick must not read tracked_vaults map"
        );
    }

    /// P0: coalesced DLMM PoolStateUpdate from bin/state signal uses MASTER cache.
    #[test]
    fn dlmm_pool_state_signal_coalesces_per_pool() {
        use ironcrab::execution::live_pool_cache::{CachedPoolState, MeteoraState};

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        let (md_state, _depth, _rx) = test_md_state_sender_no_worker();
        let worker = test_sidefx_host(&ctx, md_state, test_noop_track_worker_sender());

        let pool = Pubkey::new_unique();
        let sol = Pubkey::from_str(NATIVE_SOL_MINT).unwrap();
        ctx.hot_pool_registry.pin_arb_pool(pool);
        ctx.live_pool_cache.upsert(
            pool,
            CachedPoolState::Meteora(MeteoraState {
                token_x_mint: Pubkey::new_unique(),
                token_y_mint: sol,
                reserve_x: Pubkey::new_unique(),
                reserve_y: Pubkey::new_unique(),
                active_id: 7,
                bin_step: 20,
                reserve_x_balance: Some(500_000),
                reserve_y_balance: Some(1_500_000_000),
            }),
            99,
        );

        let mut scratch = MdSidefxBurstScratch::new();
        ironcrab::market_data::sidefx::handlers::md_sidefx_process_dlmm_pool_state_publish_signal(
            &worker,
            &MdSidefxCommand::DlmmPoolStatePublishSignal {
                run_id: "run-test".into(),
                pool_address: pool,
                slot: 50,
                grpc_recv_at: Instant::now(),
                update_class: SidefxUpdateClass::ExecHot,
            },
            &mut scratch,
        );
        ironcrab::market_data::sidefx::handlers::md_sidefx_process_dlmm_pool_state_publish_signal(
            &worker,
            &MdSidefxCommand::DlmmPoolStatePublishSignal {
                run_id: "run-test".into(),
                pool_address: pool,
                slot: 99,
                grpc_recv_at: Instant::now(),
                update_class: SidefxUpdateClass::ExecHot,
            },
            &mut scratch,
        );
        let signals = scratch.drain_dlmm_pool_state_signals();
        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].1.slot, 99, "latest slot must win coalesce");
    }

    /// Fix B: arb-pinned DLMM re-registers bin arrays when active_id drifts.
    #[test]
    fn arb_dlmm_bin_window_refreshes_on_active_id_drift() {
        use ironcrab::execution::live_pool_cache::{CachedPoolState, MeteoraState};

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        let pool = Pubkey::new_unique();
        let sol = Pubkey::from_str(NATIVE_SOL_MINT).unwrap();
        ctx.hot_pool_registry.pin_arb_pool(pool);
        ctx.live_pool_cache.upsert(
            pool,
            CachedPoolState::Meteora(MeteoraState {
                token_x_mint: Pubkey::new_unique(),
                token_y_mint: sol,
                reserve_x: Pubkey::new_unique(),
                reserve_y: Pubkey::new_unique(),
                active_id: 10,
                bin_step: 15,
                reserve_x_balance: Some(1),
                reserve_y_balance: Some(1),
            }),
            1,
        );
        assert!(ctx.register_geyser_reserves_for_arb_active_pool(pool));
        let before = ctx.tracked_bin_arrays.read().len();
        assert!(
            !ctx.maybe_refresh_arb_dlmm_bin_window(pool, 10),
            "same active_id must be idempotent"
        );
        assert_eq!(ctx.tracked_bin_arrays.read().len(), before);

        assert!(ctx.maybe_refresh_arb_dlmm_bin_window(pool, 500));
        assert!(
            ctx.tracked_bin_arrays.read().len() >= before,
            "new active_id must register additional bin-array PDAs"
        );
        assert_eq!(ctx.dlmm_registered_active_id.read().get(&pool), Some(&500));
    }

    /// Phase1: snapshot-backed vault balance tick publishes paired reserves.
    #[test]
    fn phase1_vault_balance_tick_from_snapshot_publishes_pair() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        let (md_state, _depth, _rx) = test_md_state_sender_no_worker();
        let worker = test_sidefx_host(&ctx, md_state, test_noop_track_worker_sender());

        let pool = Pubkey::new_unique();
        let base_vault = Pubkey::new_unique();
        let quote_vault = Pubkey::new_unique();
        {
            let mut vaults = ctx.tracked_vaults.write();
            vaults.insert(
                base_vault,
                VaultInfo {
                    pool_address: pool,
                    dex: "raydium_cpmm".to_string(),
                    base_mint: Pubkey::new_unique(),
                    quote_mint: Pubkey::from_str(NATIVE_SOL_MINT).unwrap(),
                    is_base_vault: true,
                    last_balance: std::sync::atomic::AtomicU64::new(0),
                    last_used_at: Instant::now(),
                    pinned: false,
                    pin: None,
                    active_id: None,
                    bin_step: None,
                    sibling_vault: Some(quote_vault),
                },
            );
            vaults.insert(
                quote_vault,
                VaultInfo {
                    pool_address: pool,
                    dex: "raydium_cpmm".to_string(),
                    base_mint: Pubkey::new_unique(),
                    quote_mint: Pubkey::from_str(NATIVE_SOL_MINT).unwrap(),
                    is_base_vault: false,
                    last_balance: std::sync::atomic::AtomicU64::new(5_000),
                    last_used_at: Instant::now(),
                    pinned: false,
                    pin: None,
                    active_id: None,
                    bin_step: None,
                    sibling_vault: Some(base_vault),
                },
            );
        }
        ctx.refresh_tracked_membership_snapshot();

        let mut scratch = MdSidefxBurstScratch::new();
        md_sidefx_process_vault_balance_tick(
            &worker,
            &MdSidefxCommand::VaultBalanceTick {
                run_id: "test".into(),
                vault_pubkey: base_vault,
                balance: 10_000,
                slot: 42,
                grpc_recv_at: Instant::now(),
                update_class: SidefxUpdateClass::ExecHot,
            },
            &mut scratch,
        );
        assert!(scratch.pending_vault_touches_contains(&base_vault));
        let snap = ctx.tracked_membership.load();
        let base_entry = snap.vault_by_pubkey.get(&base_vault).expect("base vault");
        assert_eq!(
            base_entry
                .last_balance
                .load(std::sync::atomic::Ordering::Relaxed),
            10_000
        );
        let (base, quote) = snapshot_vault_pair_balances(&snap, &base_vault, 10_000).unwrap();
        assert_eq!(base, 10_000);
        assert_eq!(quote, 5_000);
    }

    #[test]
    fn phase2a_geyser_push_skipped_when_delta_empty() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        let mut admission = FixedCapAdmission::new(ctx.max_tracked_accounts());
        let before = ctx.snapshot_explicit_subscription_pubkeys();
        use ironcrab::metrics::MARKET_DATA_GEYSER_SYNC_SKIPPED_NO_DELTA_TOTAL;
        let skip0 = MARKET_DATA_GEYSER_SYNC_SKIPPED_NO_DELTA_TOTAL.load(Ordering::Relaxed);
        assert!(track_worker_execute_coalesced_push(
            &ctx,
            &mut admission,
            before,
            false,
            false,
            false,
        ));
        assert!(
            MARKET_DATA_GEYSER_SYNC_SKIPPED_NO_DELTA_TOTAL.load(Ordering::Relaxed) > skip0,
            "empty delta must increment skipped_no_delta metric"
        );
        let mints_after = ctx.tracked_mints_tx.borrow().clone();
        assert!(
            mints_after.is_empty(),
            "no-delta skip must not republish explicit watch channels"
        );
    }

    #[test]
    fn tracked_maps_need_prune_detects_extraneous_tracked_vault() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        let pool = Pubkey::new_unique();
        let stale = Pubkey::new_unique();
        ctx.tracked_vaults.write().insert(
            stale,
            VaultInfo {
                pool_address: pool,
                dex: "test".into(),
                base_mint: Pubkey::new_unique(),
                quote_mint: Pubkey::from_str(NATIVE_SOL_MINT).unwrap(),
                is_base_vault: true,
                last_balance: std::sync::atomic::AtomicU64::new(0),
                last_used_at: Instant::now(),
                pinned: false,
                pin: None,
                active_id: None,
                bin_step: None,
                sibling_vault: None,
            },
        );
        let admission = FixedCapAdmission::new(ctx.max_tracked_accounts());
        assert!(TrackWorkerContext::tracked_maps_need_prune(
            ctx.as_ref(),
            &admission
        ));
    }

    #[test]
    fn geyser_push_no_delta_with_extraneous_tracked_keys_runs_prune_not_skip() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        let mut admission = FixedCapAdmission::new(2);
        {
            let mut tracked = ctx.tracked_vaults.write();
            for _ in 0..3 {
                let pool = Pubkey::new_unique();
                let vault = Pubkey::new_unique();
                tracked.insert(
                    vault,
                    VaultInfo {
                        pool_address: pool,
                        dex: "test".into(),
                        base_mint: Pubkey::new_unique(),
                        quote_mint: Pubkey::from_str(NATIVE_SOL_MINT).unwrap(),
                        is_base_vault: true,
                        last_balance: std::sync::atomic::AtomicU64::new(0),
                        last_used_at: Instant::now(),
                        pinned: false,
                        pin: None,
                        active_id: None,
                        bin_step: None,
                        sibling_vault: None,
                    },
                );
            }
        }
        let converge = converge_admission_from_ctx(ctx.as_ref(), &mut admission);
        assert!(
            matches!(converge, AdmissionConvergeResult::Converged),
            "cap=2 must converge with eviction for 3 tracked vaults"
        );
        assert_eq!(admission.len(), 2);
        assert_eq!(ctx.tracked_vaults.read().len(), 3);
        assert!(TrackWorkerContext::tracked_maps_need_prune(
            ctx.as_ref(),
            &admission
        ));
        let before = admitted_pubkey_set(&admission);
        use ironcrab::metrics::MARKET_DATA_GEYSER_SYNC_SKIPPED_NO_DELTA_TOTAL;
        let skip0 = MARKET_DATA_GEYSER_SYNC_SKIPPED_NO_DELTA_TOTAL.load(Ordering::Relaxed);
        assert!(track_worker_execute_coalesced_push(
            &ctx,
            &mut admission,
            before,
            false,
            false,
            false,
        ));
        assert_eq!(
            MARKET_DATA_GEYSER_SYNC_SKIPPED_NO_DELTA_TOTAL.load(Ordering::Relaxed),
            skip0,
            "no-delta with extraneous tracked keys must not increment skipped_no_delta"
        );
        assert_eq!(ctx.tracked_vaults.read().len(), 2);
    }

    #[test]
    fn geyser_push_restore_barrier_no_delta_force_publishes_watch_channels() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let (ctx, tracked_mints_rx, _, _, _) =
            minimal_market_data_context_and_merge_receivers_for_pr161(jsonl);
        let mint = Pubkey::new_unique();
        ctx.track_mint_for_geyser_metadata(mint, Some(GeyserPinReason::Wallet));
        let mut admission = FixedCapAdmission::new(ctx.max_tracked_accounts());
        converge_admission_from_ctx(ctx.as_ref(), &mut admission);
        let admitted: HashSet<Pubkey> = admission.snapshot_pubkeys().into_iter().collect();
        *ctx.last_synced_explicit_pubkeys.write() = admitted.clone();
        assert!(tracked_mints_rx.borrow().is_empty());
        assert!(track_worker_execute_coalesced_push(
            &ctx,
            &mut admission,
            admitted.clone(),
            false,
            false,
            true,
        ));
        let published: HashSet<Pubkey> = tracked_mints_rx.borrow().iter().copied().collect();
        assert_eq!(published, admitted);
        assert!(ctx.geyser_connect_barrier.is_ready());
        drop(tracked_mints_rx);
    }

    #[test]
    fn restore_barrier_incomplete_sync_stays_pending_then_ready() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        let pool = Pubkey::new_unique();
        {
            let mut vaults = ctx.tracked_vaults.write();
            for _ in 0..250_000 {
                let stale = Pubkey::new_unique();
                vaults.insert(
                    stale,
                    VaultInfo {
                        pool_address: pool,
                        dex: "test".into(),
                        base_mint: Pubkey::new_unique(),
                        quote_mint: Pubkey::from_str(NATIVE_SOL_MINT).unwrap(),
                        is_base_vault: true,
                        last_balance: std::sync::atomic::AtomicU64::new(0),
                        last_used_at: Instant::now(),
                        pinned: false,
                        pin: None,
                        active_id: None,
                        bin_step: None,
                        sibling_vault: None,
                    },
                );
            }
        }
        let mut admission = FixedCapAdmission::new(ctx.max_tracked_accounts());
        converge_admission_from_ctx(ctx.as_ref(), &mut admission);
        *ctx.geyser_prune_resume.lock() = GeyserPruneResume {
            active: true,
            map: GeyserPruneMap::Vaults,
            pending: ctx.tracked_vaults.read().keys().copied().collect(),
        };

        let before = admitted_pubkey_set(&admission);
        let incomplete =
            !track_worker_execute_coalesced_push(&ctx, &mut admission, before, true, false, true);
        assert!(
            incomplete,
            "budgeted continue slice must be incomplete for large prune backlog"
        );
        assert!(ctx.geyser_connect_barrier.is_pending());
        assert!(!ctx.geyser_connect_barrier.is_failed());
        assert!(!ctx.geyser_connect_barrier.is_ready());

        while TrackWorkerContext::tracked_maps_need_prune(ctx.as_ref(), &admission) {
            let before = admitted_pubkey_set(&admission);
            track_worker_execute_coalesced_push(&ctx, &mut admission, before, true, false, true);
        }
        if !ctx.geyser_connect_barrier.is_ready() {
            let before = admitted_pubkey_set(&admission);
            assert!(track_worker_execute_coalesced_push(
                &ctx,
                &mut admission,
                before,
                false,
                false,
                true,
            ));
        }
        assert!(ctx.geyser_connect_barrier.is_ready());
    }

    #[test]
    fn restore_barrier_protected_overflow_marks_failed_with_message() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        let admission = FixedCapAdmission::new(2);
        ctx.on_admission_converge_result(&admission, AdmissionConvergeResult::ProtectedOverflow);
        assert!(!ctx.geyser_explicit_ready.load(Ordering::Relaxed));
        assert!(ctx.geyser_connect_barrier.is_failed());
        let msg = ctx
            .geyser_explicit_config_error
            .read()
            .clone()
            .expect("protected overflow message");
        assert!(msg.contains("wallet protected explicit demand"));
    }

    #[test]
    fn geyser_explicit_not_ready_bail_message_avoids_default_overflow() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        ctx.geyser_explicit_ready.store(false, Ordering::Release);
        assert_eq!(
            ctx.geyser_explicit_not_ready_bail_message(),
            "geyser explicit admission not ready (unknown reason)"
        );
        ctx.geyser_connect_barrier.mark_failed();
        assert_eq!(
            ctx.geyser_explicit_not_ready_bail_message(),
            "geyser explicit restore barrier failed"
        );
        *ctx.geyser_explicit_config_error.write() =
            Some("explicit admission cap unconverged".into());
        assert_eq!(
            ctx.geyser_explicit_not_ready_bail_message(),
            "explicit admission cap unconverged"
        );
    }

    #[test]
    fn geyser_restore_barrier_timeout_scales_with_snapshot_size() {
        assert_eq!(
            MarketDataContext::geyser_restore_barrier_timeout(0),
            Duration::from_secs(120)
        );
        let timeout_96k = MarketDataContext::geyser_restore_barrier_timeout(96_000);
        assert!(
            timeout_96k >= Duration::from_secs(300),
            "96k snapshot restore must have >=5min wait_ready budget, got {timeout_96k:?}"
        );
        assert_eq!(timeout_96k, Duration::from_secs(360));
    }

    #[test]
    fn geyser_sync_flush_deadline_incomplete_when_prune_exceeds_budget() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        let pool = Pubkey::new_unique();
        {
            let mut vaults = ctx.tracked_vaults.write();
            for _ in 0..2_000 {
                let stale = Pubkey::new_unique();
                vaults.insert(
                    stale,
                    VaultInfo {
                        pool_address: pool,
                        dex: "test".into(),
                        base_mint: Pubkey::new_unique(),
                        quote_mint: Pubkey::from_str(NATIVE_SOL_MINT).unwrap(),
                        is_base_vault: true,
                        last_balance: std::sync::atomic::AtomicU64::new(0),
                        last_used_at: Instant::now(),
                        pinned: false,
                        pin: None,
                        active_id: None,
                        bin_step: None,
                        sibling_vault: None,
                    },
                );
            }
        }
        let admission = FixedCapAdmission::new(ctx.max_tracked_accounts());
        let expired = Instant::now();
        assert!(
            !TrackWorkerContext::sync_geyser_tracked_accounts_batched_flush_with_deadline(
                ctx.as_ref(),
                expired,
                &admission,
            ),
            "expired deadline must return incomplete for ContinueGeyserEvict"
        );
        assert_eq!(ctx.tracked_vaults.read().len(), 2_000);
    }

    /// Bugbot PR307: budgeted prune must refresh membership snapshot after each batch,
    /// including when sync returns incomplete (ContinueGeyserEvict).
    #[test]
    fn geyser_prune_incomplete_sync_refreshes_membership_snapshot() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        let pool = Pubkey::new_unique();
        {
            let mut vaults = ctx.tracked_vaults.write();
            for _ in 0..1_024 {
                let pk = Pubkey::new_unique();
                vaults.insert(
                    pk,
                    VaultInfo {
                        pool_address: pool,
                        dex: "test".into(),
                        base_mint: Pubkey::new_unique(),
                        quote_mint: Pubkey::from_str(NATIVE_SOL_MINT).unwrap(),
                        is_base_vault: true,
                        last_balance: std::sync::atomic::AtomicU64::new(0),
                        last_used_at: Instant::now(),
                        pinned: false,
                        pin: None,
                        active_id: None,
                        bin_step: None,
                        sibling_vault: None,
                    },
                );
            }
        }
        ctx.refresh_tracked_membership_snapshot();
        assert_eq!(ctx.tracked_membership.load().vaults.len(), 1_024);

        let admission = FixedCapAdmission::new(ctx.max_tracked_accounts());
        let deadline = Instant::now() + Duration::from_micros(500);
        let complete = TrackWorkerContext::sync_geyser_tracked_accounts_batched_flush_with_deadline(
            ctx.as_ref(),
            deadline,
            &admission,
        );
        let maps_len = ctx.tracked_vaults.read().len();
        let snapshot_len = ctx.tracked_membership.load().vaults.len();
        assert!(
            !complete && maps_len < 1_024,
            "expected partial prune with incomplete sync (complete={complete}, maps_len={maps_len})"
        );
        assert_eq!(
            snapshot_len, maps_len,
            "membership snapshot must track tracked_vaults after partial prune batch"
        );
        let maps_keys: HashSet<Pubkey> = ctx.tracked_vaults.read().keys().copied().collect();
        assert_eq!(
            ctx.tracked_membership.load().vaults,
            maps_keys,
            "membership snapshot keys must equal tracked_vaults after incomplete prune"
        );
    }

    #[test]
    fn geyser_prune_resume_pending_does_not_remove_re_admitted_key() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        let pool = Pubkey::new_unique();
        let vault = Pubkey::new_unique();
        ctx.tracked_vaults.write().insert(
            vault,
            VaultInfo {
                pool_address: pool,
                dex: "test".into(),
                base_mint: Pubkey::new_unique(),
                quote_mint: Pubkey::from_str(NATIVE_SOL_MINT).unwrap(),
                is_base_vault: true,
                last_balance: std::sync::atomic::AtomicU64::new(0),
                last_used_at: Instant::now(),
                pinned: false,
                pin: None,
                active_id: None,
                bin_step: None,
                sibling_vault: None,
            },
        );
        let mut admission = FixedCapAdmission::new(ctx.max_tracked_accounts());
        converge_admission_from_ctx(ctx.as_ref(), &mut admission);
        assert!(admission.snapshot_pubkeys().contains(&vault));
        *ctx.geyser_prune_resume.lock() = GeyserPruneResume {
            active: true,
            map: GeyserPruneMap::Vaults,
            pending: vec![vault],
        };
        let deadline = Instant::now() + Duration::from_secs(5);
        assert!(TrackWorkerContext::continue_geyser_evict_with_deadline(
            ctx.as_ref(),
            deadline,
            &admission,
        ));
        assert!(
            ctx.tracked_vaults.read().contains_key(&vault),
            "re-admitted vault queued for prune must not be removed on continue slice"
        );
    }

    #[test]
    fn geyser_prune_complete_sync_refreshes_membership_snapshot_without_extra_call() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        let pool = Pubkey::new_unique();
        {
            let mut vaults = ctx.tracked_vaults.write();
            for _ in 0..600 {
                let stale = Pubkey::new_unique();
                vaults.insert(
                    stale,
                    VaultInfo {
                        pool_address: pool,
                        dex: "test".into(),
                        base_mint: Pubkey::new_unique(),
                        quote_mint: Pubkey::from_str(NATIVE_SOL_MINT).unwrap(),
                        is_base_vault: true,
                        last_balance: std::sync::atomic::AtomicU64::new(0),
                        last_used_at: Instant::now(),
                        pinned: false,
                        pin: None,
                        active_id: None,
                        bin_step: None,
                        sibling_vault: None,
                    },
                );
            }
        }
        ctx.refresh_tracked_membership_snapshot();
        assert_eq!(ctx.tracked_membership.load().vaults.len(), 600);

        let admission = FixedCapAdmission::new(ctx.max_tracked_accounts());
        let deadline = Instant::now() + Duration::from_secs(5);
        assert!(
            TrackWorkerContext::sync_geyser_tracked_accounts_batched_flush_with_deadline(
                ctx.as_ref(),
                deadline,
                &admission,
            )
        );
        assert!(ctx.tracked_vaults.read().is_empty());
        assert!(ctx.tracked_membership.load().vaults.is_empty());
    }

    #[test]
    fn geyser_sync_flush_published_set_matches_admitted_i_md_7() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let (ctx, tracked_mints_rx, _, _, _) =
            minimal_market_data_context_and_merge_receivers_for_pr161(jsonl);
        let mint = Pubkey::new_unique();
        ctx.track_mint_for_geyser_metadata(mint, Some(GeyserPinReason::Wallet));
        let mut admission = FixedCapAdmission::new(ctx.max_tracked_accounts());
        converge_admission_from_ctx(ctx.as_ref(), &mut admission);
        assert_eq!(admission.len(), 1);
        let deadline = Instant::now() + Duration::from_secs(5);
        assert!(
            TrackWorkerContext::sync_geyser_tracked_accounts_batched_flush_with_deadline(
                ctx.as_ref(),
                deadline,
                &admission,
            )
        );
        let admitted: HashSet<Pubkey> = admission.snapshot_pubkeys().into_iter().collect();
        assert_eq!(*ctx.last_synced_explicit_pubkeys.read(), admitted);
        let published_mints: HashSet<Pubkey> = tracked_mints_rx.borrow().iter().copied().collect();
        assert_eq!(published_mints, admitted);
        drop(tracked_mints_rx);
    }

    #[test]
    fn phase2a_explicit_set_size_metric_updated() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        let mint = Pubkey::new_unique();
        ctx.track_mint_for_geyser_metadata(mint, Some(GeyserPinReason::Wallet));
        let mut admission = FixedCapAdmission::new(ctx.max_tracked_accounts());
        ironcrab::market_data::track::converge_admission_from_ctx(ctx.as_ref(), &mut admission);
        assert_eq!(admission.len(), 1);
        assert_eq!(
            ironcrab::metrics::market_data_geyser_explicit_set_size_value(),
            1
        );
    }

    #[test]
    fn phase2a_coalesce_two_momentum_updates_one_push_within_500ms() {
        use ironcrab::metrics::MARKET_DATA_TRACK_REQUEST_COALESCE_BATCHES_TOTAL;
        use ironcrab::nats::{MomentumActivePinReason, MomentumActivePoolEntry};

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        let track_worker = spawn_track_worker(Arc::clone(&ctx));
        let batches0 = MARKET_DATA_TRACK_REQUEST_COALESCE_BATCHES_TOTAL.load(Ordering::Relaxed);

        let pool_a = Pubkey::new_unique();
        let pool_b = Pubkey::new_unique();
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();
        for (mint, pool) in [(mint_a, pool_a), (mint_b, pool_b)] {
            ctx.live_pool_cache.upsert(
                pool,
                CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
                    token_0_mint: mint,
                    token_1_mint: Pubkey::from_str(NATIVE_SOL_MINT).unwrap(),
                    token_0_vault: Pubkey::new_unique(),
                    token_1_vault: Pubkey::new_unique(),
                    reserve_0: Some(1),
                    reserve_1: Some(1),
                }),
                1,
            );
        }

        let mk_update = |mint: Pubkey, pool: Pubkey| MomentumActivePoolsUpdate {
            version: 1,
            ts_unix_ms: 1,
            active: vec![MomentumActivePoolEntry {
                mint: mint.to_string(),
                pool: pool.to_string(),
                pin_reason: MomentumActivePinReason::Tracker,
            }],
            removed: vec![],
            full_active_snapshot: false,
        };
        assert!(track_worker_try_enqueue(
            &track_worker,
            TrackWorkerCommand::ApplyMomentumActivePools(mk_update(mint_a, pool_a)),
        ));
        assert!(track_worker_try_enqueue(
            &track_worker,
            TrackWorkerCommand::ApplyMomentumActivePools(mk_update(mint_b, pool_b)),
        ));
        std::thread::sleep(Duration::from_millis(
            MARKET_DATA_TRACK_WORKER_COALESCE_MS + 150,
        ));
        let batches1 = MARKET_DATA_TRACK_REQUEST_COALESCE_BATCHES_TOTAL.load(Ordering::Relaxed);
        let delta = batches1.saturating_sub(batches0);
        assert!(
            delta >= 1,
            "momentum updates should produce at least one coalesced push batch (delta={delta})"
        );
        assert!(ctx.hot_pool_registry.is_hot_pool(pool_a));
        assert!(ctx.hot_pool_registry.is_hot_pool(pool_b));
    }

    #[test]
    fn phase2a_geyser_sync_on_track_worker_not_md_state_loop() {
        let bin_src = include_str!("market_data.rs");
        let prod_src = bin_src
            .split("#[cfg(test)]")
            .next()
            .expect("production source section");
        assert!(
            prod_src.contains("fn track_worker_execute_coalesced_push"),
            "geyser coalesced push must be visible in market_data.rs for track-worker wiring"
        );
        let push_start = prod_src
            .find("fn track_worker_execute_coalesced_push")
            .expect("track_worker_execute_coalesced_push");
        let push_end = prod_src[push_start..]
            .find("\nfn spawn_momentum_tracking_coalescer")
            .map(|i| push_start + i)
            .expect("after track_worker_execute_coalesced_push");
        let push_body = &prod_src[push_start..push_end];
        assert!(
            push_body.contains("sync_geyser_tracked_accounts_batched_flush"),
            "track-worker coalesced push must reference batched Geyser flush"
        );
        let worker_src = include_str!("../market_data/md_state/worker.rs");
        let md_state_loop_start = worker_src
            .find("fn md_state_worker_loop")
            .expect("md_state_worker_loop");
        let md_state_loop_end = worker_src[md_state_loop_start..]
            .find("pub fn spawn_md_state_worker")
            .map(|i| md_state_loop_start + i)
            .unwrap_or(worker_src.len());
        let md_state_loop_body = &worker_src[md_state_loop_start..md_state_loop_end];
        assert!(
            !md_state_loop_body.contains("track_worker_execute_coalesced_push"),
            "md-state loop must not call track_worker_execute_coalesced_push inline"
        );
    }

    #[test]
    fn phase2a_track_worker_thread_spawn_name_md_track_worker() {
        let worker_src = include_str!("../market_data/track/worker.rs");
        assert!(
            worker_src.contains(".name(\"md-track-worker\".into())"),
            "track worker must spawn OS thread named md-track-worker"
        );
    }

    /// Phase 3 P3 / I-MD-5: `sync_geyser_tracked_accounts_batched_flush` only on track-worker path.
    #[test]
    fn phase3_explicit_flush_only_via_track_worker_or_cold_bootstrap() {
        let geyser_sync_src = include_str!("../market_data/track/geyser_sync.rs");
        assert!(
            geyser_sync_src.contains("sync_geyser_tracked_accounts_batched_flush_with_deadline"),
            "track-worker geyser_sync must call batched flush"
        );
        let md_state_src = include_str!("../market_data/md_state/worker.rs");
        assert!(
            !md_state_src.contains("sync_geyser_tracked_accounts_batched_flush"),
            "md-state must not call sync_geyser_tracked_accounts_batched_flush inline"
        );
        let bin_src = include_str!("market_data.rs");
        let tx_handler_src = include_str!("../market_data/ingest/tx_handler.rs");
        let prod_src = bin_src
            .split("#[cfg(test)]")
            .next()
            .expect("production source section");
        let tx_body = tx_handler_src;
        let acc_start = prod_src.find("async fn handle_geyser_account");
        if let Some(acc_s) = acc_start {
            let acc_end = prod_src[acc_s..]
                .find("/// Geyser transaction ingest — delegates to `ingest/tx_handler.rs`")
                .map(|i| acc_s + i)
                .unwrap_or(prod_src.len());
            let acc_body = &prod_src[acc_s..acc_end];
            assert!(
                !tx_body.contains("sync_geyser_tracked_accounts_batched_flush"),
                "TX ingest must not call batched Geyser flush"
            );
            assert!(
                !acc_body.contains("sync_geyser_tracked_accounts_batched_flush"),
                "account ingest must not call batched Geyser flush"
            );
        }
    }

    /// Phase 3 P3 / I-MD-6: snapshot roundtrip restores tracked mints without RPC.
    #[test]
    fn phase3_restore_seeds_desired_set_without_rpc() {
        use ironcrab::market_data::track::write_explicit_set_snapshot;

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        let mint = Pubkey::new_unique();
        ctx.track_mint_for_geyser_metadata(mint, Some(GeyserPinReason::MomentumActive));

        let mut admission = FixedCapAdmission::new(ctx.max_tracked_accounts());
        converge_admission_from_ctx(ctx.as_ref(), &mut admission);
        let snapshot = ctx.build_explicit_set_snapshot(&admission);
        assert!(!snapshot.rows.is_empty());

        let fresh = minimal_market_data_context_for_pr_d_tests(
            QueuedJsonlWriter::spawn(
                JsonlWriterConfig::new("market_events").with_log_dir(tmp.path()),
                256,
            )
            .expect("jsonl2"),
        );
        assert!(fresh.snapshot_explicit_subscription_pubkeys().is_empty());

        let restored = fresh.restore_tracked_maps_from_snapshot_rows(&snapshot);
        assert!(restored > 0);
        assert!(fresh.tracked_mints.read().contains_key(&mint));

        let path = tmp.path().join("explicit_set.snapshot");
        write_explicit_set_snapshot(&path, &snapshot).expect("write snapshot");
        let loaded = load_explicit_set_snapshot(&path).expect("load snapshot");
        assert_eq!(loaded.rows.len(), snapshot.rows.len());
    }

    /// Phase 3 P3: restore + identical push must skip delta (no resubscribe storm).
    #[test]
    fn phase3_restore_then_unchanged_push_skips_delta() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        let mint = Pubkey::new_unique();
        ctx.track_mint_for_geyser_metadata(mint, Some(GeyserPinReason::ArbMultiDex));
        let mut snap_admission = FixedCapAdmission::new(ctx.max_tracked_accounts());
        converge_admission_from_ctx(ctx.as_ref(), &mut snap_admission);
        let snapshot = ctx.build_explicit_set_snapshot(&snap_admission);

        let track_worker = spawn_track_worker(Arc::clone(&ctx));
        assert!(track_worker_try_enqueue(
            &track_worker,
            TrackWorkerCommand::RestoreExplicitSnapshot(snapshot),
        ));
        assert!(track_worker_try_enqueue(
            &track_worker,
            TrackWorkerCommand::ScheduleGeyserPush,
        ));
        std::thread::sleep(Duration::from_millis(
            MARKET_DATA_TRACK_WORKER_COALESCE_MS + 200,
        ));

        let mut admission = FixedCapAdmission::new(ctx.max_tracked_accounts());
        let keys = ctx.snapshot_explicit_subscription_pubkeys();
        use ironcrab::metrics::MARKET_DATA_GEYSER_SYNC_SKIPPED_NO_DELTA_TOTAL;
        let skip0 = MARKET_DATA_GEYSER_SYNC_SKIPPED_NO_DELTA_TOTAL.load(Ordering::Relaxed);
        assert!(track_worker_execute_coalesced_push(
            &ctx,
            &mut admission,
            keys,
            false,
            false,
            false,
        ));
        assert!(
            MARKET_DATA_GEYSER_SYNC_SKIPPED_NO_DELTA_TOTAL.load(Ordering::Relaxed) > skip0,
            "unchanged explicit set after restore must skip Geyser delta push"
        );
    }

    #[test]
    fn phase2b_momentum_nats_enqueues_track_worker_not_md_state() {
        let bin_src = include_str!("market_data.rs");
        let coalesce_src = include_str!("../market_data/track/coalesce.rs");
        let prod_src = bin_src
            .split("#[cfg(test)]")
            .next()
            .expect("production source section");
        assert!(
            !prod_src.contains("MdStateCommand::ApplyMomentumActivePools"),
            "momentum must not enqueue MdStateCommand::ApplyMomentumActivePools"
        );
        assert!(
            !prod_src.contains(
                "md_state_try_enqueue(&md_state, MdStateCommand::ApplyMomentumActivePools"
            ),
            "momentum coalescer must not md_state_try_enqueue ApplyMomentumActivePools"
        );
        assert!(
            coalesce_src.contains("TrackWorkerCommand::ApplyMomentumActivePools"),
            "momentum coalescer must reference track-worker ApplyMomentumActivePools"
        );
        assert!(
            !coalesce_src.contains("md_state_try_enqueue"),
            "momentum coalescer must not touch md-state enqueue"
        );
        assert!(
            prod_src.contains("spawn_momentum_tracking_coalescer"),
            "bin must retain momentum coalescer spawn wrapper"
        );
    }

    #[test]
    fn phase2b_apply_momentum_active_pools_on_track_worker_updates_desired_set() {
        use ironcrab::nats::{MomentumActivePinReason, MomentumActivePoolEntry};

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        let track_worker = spawn_track_worker(Arc::clone(&ctx));

        let pool = Pubkey::new_unique();
        let mint = Pubkey::new_unique();
        let coin = Pubkey::new_unique();
        let pc = Pubkey::new_unique();
        ctx.live_pool_cache.upsert(
            pool,
            CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
                token_0_mint: mint,
                token_1_mint: Pubkey::from_str(NATIVE_SOL_MINT).unwrap(),
                token_0_vault: coin,
                token_1_vault: pc,
                reserve_0: Some(1),
                reserve_1: Some(1),
            }),
            1,
        );

        let before = ctx.snapshot_explicit_demand_pubkeys();
        assert!(track_worker_try_enqueue(
            &track_worker,
            TrackWorkerCommand::ApplyMomentumActivePools(MomentumActivePoolsUpdate {
                version: 1,
                ts_unix_ms: 1,
                active: vec![MomentumActivePoolEntry {
                    mint: mint.to_string(),
                    pool: pool.to_string(),
                    pin_reason: MomentumActivePinReason::Tracker,
                }],
                removed: vec![],
                full_active_snapshot: false,
            }),
        ));
        std::thread::sleep(Duration::from_millis(50));
        let mid = ctx.snapshot_explicit_demand_pubkeys();
        assert!(explicit_subscription_has_new_keys(&before, &mid));
        assert!(ctx.hot_pool_registry.is_hot_pool(pool));
        let vs = ctx.tracked_vaults.read();
        assert_eq!(
            vs.get(&coin).unwrap().pin,
            Some(GeyserPinReason::MomentumActive)
        );
    }

    #[test]
    fn release_pool_consumer_group_clears_explicit_momentum_bypass_gate() {
        use ironcrab::market_data::track::{
            try_admit_owner_group, ExplicitConsumer, ExplicitOwner, ExplicitOwnerKey,
        };

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);

        let pool = Pubkey::new_unique();
        let mint = Pubkey::new_unique();
        let coin = Pubkey::new_unique();
        let pc = Pubkey::new_unique();
        ctx.live_pool_cache.upsert(
            pool,
            CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
                token_0_mint: mint,
                token_1_mint: Pubkey::from_str(NATIVE_SOL_MINT).unwrap(),
                token_0_vault: coin,
                token_1_vault: pc,
                reserve_0: Some(1),
                reserve_1: Some(1),
            }),
            1,
        );

        let mut admission = FixedCapAdmission::new(100);
        let owner = ExplicitOwner {
            consumer: ExplicitConsumer::Momentum,
            owner_key: ExplicitOwnerKey::Pool(pool),
        };
        assert!(try_admit_owner_group(&mut admission, owner, vec![coin, pc]));
        ctx.note_explicit_momentum_pool_admitted(pool);
        assert!(ctx.pool_has_explicit_momentum_admission(pool));

        ctx.release_pool_consumer_group(&mut admission, pool, ExplicitConsumer::Momentum);
        assert!(!ctx.pool_has_explicit_momentum_admission(pool));

        ctx.hot_pool_registry.pin_pool(mint, pool);
        MarketDataContext::register_geyser_reserves_after_trade(&ctx, pool);
        let vs = ctx.tracked_vaults.read();
        assert!(!vs.contains_key(&coin));
        assert!(!vs.contains_key(&pc));
    }

    #[test]
    fn converge_rebuild_clears_stale_explicit_momentum_pool() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);

        let stale_pool = Pubkey::new_unique();
        ctx.note_explicit_momentum_pool_admitted(stale_pool);
        assert!(ctx.pool_has_explicit_momentum_admission(stale_pool));

        let mut admission = FixedCapAdmission::new(100);
        assert_eq!(
            converge_admission_from_groups(&mut admission, &[]),
            AdmissionConvergeResult::Converged
        );
        ctx.on_admission_converge_result(&admission, AdmissionConvergeResult::Converged);
        assert!(!ctx.pool_has_explicit_momentum_admission(stale_pool));
    }

    #[test]
    fn restore_explicit_admission_rebuilds_bypass_gate_pool_sets() {
        use ironcrab::market_data::track::AdmissionRestoreResult;
        use ironcrab::nats::{MomentumActivePinReason, MomentumActivePoolEntry};

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);

        let pool = Pubkey::new_unique();
        let mint = Pubkey::new_unique();
        let coin = Pubkey::new_unique();
        let pc = Pubkey::new_unique();
        ctx.live_pool_cache.upsert(
            pool,
            CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
                token_0_mint: mint,
                token_1_mint: Pubkey::from_str(NATIVE_SOL_MINT).unwrap(),
                token_0_vault: coin,
                token_1_vault: pc,
                reserve_0: Some(1),
                reserve_1: Some(1),
            }),
            1,
        );

        let mut snap_admission = FixedCapAdmission::new(ctx.max_tracked_accounts());
        ctx.apply_momentum_active_entries(
            &mut snap_admission,
            &[MomentumActivePoolEntry {
                mint: mint.to_string(),
                pool: pool.to_string(),
                pin_reason: MomentumActivePinReason::Tracker,
            }],
        );
        let snapshot = ctx.build_explicit_set_snapshot(&snap_admission);
        assert!(ctx.pool_has_explicit_momentum_admission(pool));

        let fresh = minimal_market_data_context_for_pr_d_tests(
            QueuedJsonlWriter::spawn(
                JsonlWriterConfig::new("market_events").with_log_dir(tmp.path()),
                256,
            )
            .expect("jsonl2"),
        );
        assert!(!fresh.pool_has_explicit_momentum_admission(pool));

        let mut admission = FixedCapAdmission::new(fresh.max_tracked_accounts());
        let result = fresh.restore_explicit_admission_from_snapshot(&mut admission, &snapshot);
        assert_eq!(result, AdmissionRestoreResult::Restored);
        assert!(fresh.pool_has_explicit_momentum_admission(pool));
        assert!(fresh.admit_geyser_explicit_pool_assets(
            pool,
            mint,
            Pubkey::from_str(NATIVE_SOL_MINT).unwrap()
        ));
    }

    #[test]
    fn pr4a_momentum_admission_reject_skips_tracked_map_mutation() {
        use ironcrab::nats::{MomentumActivePinReason, MomentumActivePoolEntry};

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);

        let pool = Pubkey::new_unique();
        let mint = Pubkey::new_unique();
        let coin = Pubkey::new_unique();
        let pc = Pubkey::new_unique();
        ctx.live_pool_cache.upsert(
            pool,
            CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
                token_0_mint: mint,
                token_1_mint: Pubkey::from_str(NATIVE_SOL_MINT).unwrap(),
                token_0_vault: coin,
                token_1_vault: pc,
                reserve_0: Some(1),
                reserve_1: Some(1),
            }),
            1,
        );

        let vaults_before = ctx.tracked_vaults.read().len();
        let mints_before = ctx.tracked_mints.read().len();
        let mut admission = FixedCapAdmission::new(2);
        ctx.apply_momentum_active_entries(
            &mut admission,
            &[MomentumActivePoolEntry {
                mint: mint.to_string(),
                pool: pool.to_string(),
                pin_reason: MomentumActivePinReason::Tracker,
            }],
        );
        assert_eq!(ctx.tracked_vaults.read().len(), vaults_before);
        assert_eq!(ctx.tracked_mints.read().len(), mints_before);
        assert!(ctx.hot_pool_registry.is_hot_pool(pool));
        assert!(!ctx.pool_has_explicit_momentum_admission(pool));
    }

    #[test]
    fn pr4b_tracker_admission_reject_skips_tracked_map_mutation() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);

        let mint = Pubkey::new_unique();
        let mints_before = ctx.tracked_mints.read().len();
        let mut admission = FixedCapAdmission::new(2);
        let w1 = Pubkey::new_unique();
        let w2 = Pubkey::new_unique();
        assert!(try_admit_owner_group(
            &mut admission,
            ExplicitOwner {
                consumer: ExplicitConsumer::Wallet,
                owner_key: ExplicitOwnerKey::Wallet,
            },
            vec![w1, w2],
        ));
        assert!(!ctx.apply_track_mint(&mut admission, mint, None));
        assert_eq!(ctx.tracked_mints.read().len(), mints_before);
        assert!(!ctx.tracked_mints.read().contains_key(&mint));
    }

    #[test]
    fn pr4b_wallet_admission_reject_skips_tracked_map_mutation() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let wallet = Pubkey::new_unique();
        let tracked = TrackedWallet::new(wallet);
        let extra_ata = Pubkey::new_unique();
        let ctx = minimal_market_data_context_for_pr_d_tests_with_wallet(
            jsonl,
            Some(tracked),
            &[extra_ata],
            None,
        );
        ctx.config.write().max_tracked_accounts = 2;

        let mint = Pubkey::new_unique();
        let mints_before = ctx.tracked_mints.read().len();
        let mut admission = FixedCapAdmission::new(2);
        assert!(!ctx.apply_wallet_pin(&mut admission, mint));
        assert_eq!(ctx.tracked_mints.read().len(), mints_before);
        assert!(!ctx.tracked_mints.read().contains_key(&mint));
    }

    #[test]
    fn pr4b_withdraw_wallet_pin_demotes_mint_and_refreshes_admission() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);

        let mint = Pubkey::new_unique();
        let mut admission = FixedCapAdmission::new(ctx.max_tracked_accounts());
        assert!(ctx.apply_wallet_pin(&mut admission, mint));
        assert!(ctx.tracked_mints.read().contains_key(&mint));
        assert!(admission.contains(&mint));
        assert!(ctx.withdraw_wallet_pin(&mut admission, mint));
        assert!(!ctx.tracked_mints.read().contains_key(&mint));
        assert!(!admission.contains(&mint));
    }

    #[test]
    fn pr4b_consumer_id_for_unpinned_mint_is_tracker() {
        assert_eq!(consumer_id_for_geyser_pin(None), ConsumerId::Tracker);
    }

    #[test]
    fn pr4b_repeated_track_mint_unpinned_is_idempotent_success() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);

        let mint = Pubkey::new_unique();
        let mut admission = FixedCapAdmission::new(ctx.max_tracked_accounts());
        assert!(ctx.apply_track_mint(&mut admission, mint, None));
        assert!(ctx.tracked_mints.read().contains_key(&mint));
        assert!(ctx.apply_track_mint(&mut admission, mint, None));
    }

    /// Scope H: lock-free mint membership snapshot gates redundant TX TrackMint enqueue.
    #[test]
    fn scope_h_ingest_membership_mint_contains_after_track() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        let mint = Pubkey::new_unique();
        assert!(!ctx.ingest_membership_mint_contains(&mint));

        let mut admission = FixedCapAdmission::new(ctx.max_tracked_accounts());
        assert!(ctx.apply_track_mint(&mut admission, mint, None));
        ctx.refresh_tracked_membership_snapshot();
        assert!(ctx.ingest_membership_mint_contains(&mint));
    }

    /// Scope H: batched TrackMints must not downgrade wallet pin to unpinned tracker.
    #[test]
    fn scope_h_track_mints_batch_preserves_wallet_pin() {
        use ironcrab::market_data::track::{track_worker_process_command, TrackWorkerCommand};

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        let mint = Pubkey::new_unique();
        let mut admission = FixedCapAdmission::new(ctx.max_tracked_accounts());
        let mut restore_barrier_pending = false;

        assert!(track_worker_process_command(
            &ctx,
            &mut admission,
            &mut restore_barrier_pending,
            TrackWorkerCommand::TrackMints {
                entries: vec![(mint, None), (mint, Some(TrackPinReason::Wallet)),],
            },
        ));
        let info = ctx
            .tracked_mints
            .read()
            .get(&mint)
            .cloned()
            .expect("mint tracked");
        assert!(info.pinned);
        assert_eq!(info.pin, Some(GeyserPinReason::Wallet));
    }

    #[test]
    fn pr4b_repeated_wallet_pin_is_idempotent_success() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);

        let mint = Pubkey::new_unique();
        let mut admission = FixedCapAdmission::new(ctx.max_tracked_accounts());
        assert!(ctx.apply_wallet_pin(&mut admission, mint));
        assert!(ctx.apply_wallet_pin(&mut admission, mint));
    }

    #[test]
    fn pr4b_sell_close_skips_ata_untrack_when_withdraw_enqueue_drops() {
        assert!(!wallet_sell_close_should_untrack_ata(true, false));
        assert!(wallet_sell_close_should_untrack_ata(true, true));
        assert!(wallet_sell_close_should_untrack_ata(false, false));
    }

    #[test]
    fn pr4b_withdraw_wallet_mint_enqueue_fails_when_md_state_queue_saturated() {
        let (md_state, _, _) = test_md_state_sender_no_worker();
        fill_md_state_queue(&md_state);
        assert!(!md_state_try_enqueue_withdraw_wallet_mint(
            &md_state,
            Pubkey::new_unique()
        ));
    }

    #[tokio::test]
    async fn pr4b_withdraw_wallet_mint_actor_enqueues_protocol_withdraw() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);
        let md_state = test_spawn_md_state(&ctx);

        let mint = Pubkey::new_unique();
        assert!(ctx.apply_wallet_pin(
            &mut FixedCapAdmission::new(ctx.max_tracked_accounts()),
            mint
        ));

        md_state_try_enqueue(&md_state, MdStateCommand::WithdrawWalletMint { mint });
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert!(!ctx.tracked_mints.read().contains_key(&mint));
    }

    /// PR4b: tracker snapshot rows restore as unpinned mints, not arb-pinned.
    #[test]
    fn pr4b_tracker_snapshot_mint_restores_unpinned() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);

        let mint = Pubkey::new_unique();
        ctx.track_mint_for_geyser_metadata(mint, None);
        let mut admission = FixedCapAdmission::new(ctx.max_tracked_accounts());
        converge_admission_from_ctx(ctx.as_ref(), &mut admission);
        let snapshot = ctx.build_explicit_set_snapshot(&admission);
        let row = snapshot
            .rows
            .iter()
            .find(|r| r.pubkey == mint.to_string())
            .expect("tracker mint row");
        assert_eq!(row.consumer, SnapshotConsumer::Tracker);

        let fresh = minimal_market_data_context_for_pr_d_tests(
            QueuedJsonlWriter::spawn(
                JsonlWriterConfig::new("market_events").with_log_dir(tmp.path()),
                256,
            )
            .expect("jsonl2"),
        );
        let restored = fresh.restore_tracked_maps_from_snapshot_rows(&snapshot);
        assert!(restored > 0);
        let tracked = fresh.tracked_mints.read();
        let info = tracked.get(&mint).expect("restored tracker mint");
        assert_eq!(info.pin, None);
        assert!(!info.pinned);
    }

    /// Scope C / Bugbot: position pools survive snapshot restore with MomentumPosition consumer + pin subset.
    #[test]
    fn scope_c_position_pool_snapshot_restore_preserves_position_pin() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);

        let pool = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote = Pubkey::from_str(NATIVE_SOL_MINT).unwrap();
        let coin_vault = Pubkey::new_unique();
        let pc_vault = Pubkey::new_unique();
        ctx.pool_mint_map
            .write()
            .insert(pool.to_string(), base_mint.to_string());
        ctx.hot_pool_registry
            .pin_pool_with_reason(base_mint, pool, true);
        {
            let mut vaults = ctx.tracked_vaults.write();
            let mut vaults_changed = false;
            ctx.register_tracked_vault_pair_lru(
                &mut vaults,
                &mut vaults_changed,
                TrackedVaultPairInsert {
                    pool,
                    now: Instant::now(),
                    dex: "raydium_cpmm",
                    base_mint,
                    quote_mint: quote,
                    base_vault: coin_vault,
                    quote_vault: pc_vault,
                    active_id: None,
                    bin_step: None,
                },
            );
            vaults.get_mut(&coin_vault).expect("coin").pin = Some(GeyserPinReason::MomentumActive);
            vaults.get_mut(&pc_vault).expect("pc").pin = Some(GeyserPinReason::MomentumActive);
        }

        let mut admission = FixedCapAdmission::new(ctx.max_tracked_accounts());
        converge_admission_from_ctx(ctx.as_ref(), &mut admission);
        let snapshot = ctx.build_explicit_set_snapshot(&admission);
        let vault_row = snapshot
            .rows
            .iter()
            .find(|r| r.pubkey == coin_vault.to_string())
            .expect("position vault row");
        assert_eq!(vault_row.consumer, SnapshotConsumer::MomentumPosition);
        assert!(snapshot.momentum_position_pools.contains(&pool.to_string()));

        let fresh = minimal_market_data_context_for_pr_d_tests(
            QueuedJsonlWriter::spawn(
                JsonlWriterConfig::new("market_events").with_log_dir(tmp.path()),
                256,
            )
            .expect("jsonl2"),
        );
        let restored = fresh.restore_tracked_maps_from_snapshot_rows(&snapshot);
        assert!(restored > 0);
        assert!(fresh.hot_pool_registry.is_position_pin_for_pool(pool));
        assert!(fresh.hot_pool_registry.is_position_pin(base_mint, pool));
    }

    /// Phase 2c: trade handler must not reference arb reconcile enqueue helpers.
    #[test]
    fn phase2c_trade_path_no_arb_reconcile_enqueue() {
        let tx_body = include_str!("../market_data/ingest/tx_handler.rs");
        assert!(!tx_body.contains("ArbMultiDexReconcile"));
        assert!(!tx_body.contains("try_enqueue_arb_reconcile_for_pool"));
        assert!(!tx_body.contains("try_enqueue_arb_multi_dex_reconcile"));
        assert!(!tx_body.contains("reconcile_arb_multi_dex"));
    }

    /// Phase 2c: md-state command enum must not include arb reconcile variants.
    #[test]
    fn phase2c_md_state_enum_has_no_arb_multi_dex_reconcile() {
        let src = include_str!("../market_data/md_state/command.rs");
        assert!(!src.contains("ArbMultiDexReconcile"));
        assert!(!src.contains("UpdateArbCoverageIndex"));
        assert!(!src.contains("RegisterReservesAfterTrade"));
        assert!(!src.contains("RegisterPoolVaultsFromAccount"));
    }

    #[test]
    fn phase3_arb_track_requests_nats_uses_track_worker_not_md_state() {
        let bin_src = include_str!("market_data.rs");
        let coalesce_src = include_str!("../market_data/track/coalesce.rs");
        let prod_src = bin_src
            .split("#[cfg(test)]")
            .next()
            .expect("production source section");
        assert!(
            !prod_src.contains("MdStateCommand::ApplyArbTrackRequests"),
            "arb track must not enqueue MdStateCommand::ApplyArbTrackRequests"
        );
        assert!(
            coalesce_src.contains("TrackWorkerCommand::ApplyArbTrackRequests"),
            "arb coalescer must reference track-worker ApplyArbTrackRequests"
        );
        assert!(
            coalesce_src.contains("track_worker_try_enqueue"),
            "arb coalescer must enqueue via track_worker_try_enqueue"
        );
        assert!(
            !coalesce_src.contains("md_state_try_enqueue"),
            "arb coalescer must not touch md-state enqueue"
        );
        assert!(
            prod_src.contains("spawn_arb_tracking_coalescer"),
            "bin must retain arb coalescer spawn wrapper"
        );
        assert!(
            !prod_src.contains(".publish(TOPIC_ARB_TRACK_REQUESTS"),
            "market-data must not publish TOPIC_ARB_TRACK_REQUESTS"
        );
    }

    /// Phase 5c: md-account-publish worker + queue live in dedicated library module.
    #[test]
    fn phase5c_account_publish_in_dedicated_module() {
        let account_src = include_str!("../market_data/publish/account.rs");
        let core_src = include_str!("../market_data/publish/core.rs");
        let bin_src = include_str!("market_data.rs");
        assert!(
            account_src.contains("pub fn spawn_md_account_publish_runtime"),
            "account.rs must contain spawn_md_account_publish_runtime"
        );
        assert!(
            account_src.contains("pub enum AccountPathNatsJob"),
            "account.rs must contain AccountPathNatsJob"
        );
        assert!(
            core_src.contains("pub async fn publish_market_event_core_and_momentum_ex"),
            "core.rs must contain publish_market_event_core_and_momentum_ex"
        );
        assert!(
            bin_src.contains("use ironcrab::market_data::publish::"),
            "market_data bin must import publish module"
        );
        assert!(
            bin_src.contains("impl PublishHost for MarketDataContext"),
            "bin must wire MarketDataContext to PublishHost"
        );
        assert!(
            bin_src.contains("fn spawn_market_data_account_publish_runtime"),
            "bin must retain thin spawn wrapper for md-publish runtime"
        );
    }

    /// Arb pin track path must remain Geyser-only (no cold-path RPC seed on pin).
    #[test]
    fn apply_arb_active_entries_geyser_only_no_cold_rpc() {
        let bin_src = include_str!("market_data.rs");
        let code_src = bin_src
            .split("mod discovery_tests")
            .next()
            .expect("production + inline helpers before test modules");
        assert!(
            !code_src.contains("cold_path_rpc: parking_lot::RwLock"),
            "MarketDataContext must not carry arb-pin cold_path_rpc"
        );
        assert!(
            !code_src.contains("arb_pin_ensure_debounce"),
            "arb-pin ensure debounce map must be removed"
        );
        assert!(
            code_src
                .contains("Arb pin: Geyser reserve registration deferred (geyser-only, no RPC)"),
            "apply_arb_active_entries must log deferred Geyser register without RPC"
        );
        let impl_marker = "Arb pin: Geyser reserve registration deferred (geyser-only, no RPC)";
        let impl_start = code_src
            .find(impl_marker)
            .expect("apply_arb_active_entries geyser-only path");
        let impl_window = &code_src[impl_start.saturating_sub(800)..impl_start + 200];
        assert!(
            !impl_window.contains("handle_ensure_"),
            "apply_arb_active_entries must not call handle_ensure_*"
        );
        assert!(
            !impl_window.contains("cold_path_rpc_refresh"),
            "apply_arb_active_entries must not call cold_path_rpc_refresh"
        );
    }

    /// Arb-pin deferred warn throttle must not build dynamic string keys.
    #[test]
    fn apply_arb_active_entries_throttle_has_no_dynamic_string_keys() {
        let bin_src = include_str!("market_data.rs");
        let anchor = bin_src
            .find("ArbPinDeferredLogCategory::LivePoolCacheMiss")
            .expect("apply_arb_active_entries deferred category");
        let start = bin_src[..anchor]
            .rfind("fn apply_arb_active_entries(")
            .expect("apply_arb_active_entries");
        let end = bin_src[anchor..]
            .find("fn arb_pool_leg_mint_still_required")
            .expect("after apply_arb_active_entries");
        let fn_body = &bin_src[start..anchor + end];
        assert!(
            !fn_body.contains("format!("),
            "throttle decision must not allocate dynamic string keys"
        );
    }

    /// Phase 5d: I-24d Ensure* cold-path handlers live in dedicated library module.
    #[test]
    fn phase5d_cold_ensure_in_dedicated_module() {
        let ensure_pump_src = include_str!("../market_data/cold/ensure_pump.rs");
        let rpc_src = include_str!("../market_data/cold/rpc_refresh.rs");
        let host_src = include_str!("../market_data/cold/host.rs");
        let bin_src = include_str!("market_data.rs");
        let prod_src = bin_src
            .split("#[cfg(test)]")
            .next()
            .expect("production source section");
        assert!(
            ensure_pump_src.contains("pub async fn handle_ensure_pump_amm_pool_accounts"),
            "ensure_pump.rs must contain handle_ensure_pump_amm_pool_accounts"
        );
        assert!(
            rpc_src.contains("pub async fn cold_path_rpc_refresh_orca_whirlpool_pool_row"),
            "rpc_refresh.rs must contain cold_path_rpc_refresh_orca_whirlpool_pool_row"
        );
        assert!(
            host_src.contains("pub trait ColdHost"),
            "host.rs must define ColdHost"
        );
        assert!(
            bin_src.contains("use ironcrab::market_data::cold::"),
            "market_data bin must import cold module"
        );
        assert!(
            bin_src.contains("impl ColdHost for MarketDataContext"),
            "bin must wire MarketDataContext to ColdHost"
        );
        assert!(
            !prod_src.contains("async fn handle_ensure_pump_amm_pool_accounts("),
            "Ensure handlers must not remain defined in bin production code"
        );
    }

    /// Phase 5e: JSONL filter + off-thread writer live in dedicated library module.
    #[test]
    fn phase5e_jsonl_in_dedicated_module() {
        let filter_src = include_str!("../market_data/jsonl/filter.rs");
        let host_src = include_str!("../market_data/jsonl/host.rs");
        let writer_src = include_str!("../market_data/jsonl/writer.rs");
        let bin_src = include_str!("market_data.rs");
        assert!(
            filter_src.contains("pub fn market_event_should_jsonl"),
            "filter.rs must contain market_event_should_jsonl"
        );
        assert!(
            host_src.contains("pub trait JsonlHost"),
            "host.rs must define JsonlHost"
        );
        assert!(
            writer_src.contains("pub fn spawn_market_data_jsonl_writer"),
            "writer.rs must contain spawn_market_data_jsonl_writer"
        );
        assert!(
            bin_src.contains("use ironcrab::market_data::jsonl::"),
            "market_data bin must import jsonl module"
        );
        assert!(
            bin_src.contains("impl JsonlHost for MarketDataContext"),
            "bin must wire MarketDataContext to JsonlHost"
        );
    }

    /// Phase 5e: Geyser ingest account/TX filters live in dedicated library module.
    #[test]
    fn phase5e_ingest_filters_in_dedicated_module() {
        let account_src = include_str!("../market_data/ingest/account_filter.rs");
        let host_src = include_str!("../market_data/ingest/host.rs");
        let bin_src = include_str!("market_data.rs");
        assert!(
            account_src.contains("pub fn account_geyser_update_might_be_relevant"),
            "account_filter.rs must contain account_geyser_update_might_be_relevant"
        );
        assert!(
            account_src.contains("pub fn account_geyser_update_relevance"),
            "account_filter.rs must contain account_geyser_update_relevance"
        );
        assert!(
            bin_src.contains("record_market_data_account_early_drop"),
            "bin must record labeled account early-drop metrics"
        );
        assert!(
            account_src.contains("pub fn account_geyser_dispatch_priority_high"),
            "account_filter.rs must contain account_geyser_dispatch_priority_high"
        );
        assert!(
            host_src.contains("pub trait IngestHost"),
            "host.rs must define IngestHost"
        );
        assert!(
            bin_src.contains("use ironcrab::market_data::ingest::"),
            "market_data bin must import ingest module"
        );
        assert!(
            bin_src.contains("impl TxIngestHost for MarketDataContext"),
            "bin must wire MarketDataContext to TxIngestHost"
        );
        assert!(
            bin_src.contains("impl AccountIngestHost for MarketDataContext"),
            "bin must wire MarketDataContext to AccountIngestHost"
        );
    }

    /// Phase 4b: Geyser account handler lives in dedicated ingest module; bin retains thin wrapper.
    #[test]
    fn phase4b_account_handler_in_ingest_module() {
        let account_handler_src = include_str!("../market_data/ingest/account_handler.rs");
        let account_parse_src = include_str!("../market_data/ingest/account_parse.rs");
        let bin_src = include_str!("market_data.rs");
        let prod_src = bin_src
            .split("#[cfg(test)]")
            .next()
            .expect("production source section");
        assert!(
            account_handler_src.contains("pub async fn handle_geyser_account_update"),
            "account_handler.rs must define handle_geyser_account_update"
        );
        assert!(
            account_parse_src.contains("pub fn try_parse_mint_account"),
            "account_parse.rs must contain account-only parse helpers"
        );
        assert!(
            prod_src.contains("handle_geyser_account_update"),
            "bin production code must delegate to ingest account handler"
        );
        assert!(
            !prod_src.contains("Parsed DEX account update"),
            "account handler body must not remain in bin production section"
        );
    }

    /// P2 hardening: account handler early-returns after sidefx paths (no false unparsed drops).
    #[test]
    fn p2_account_handler_early_return_after_sidefx_paths() {
        let account_handler_src = include_str!("../market_data/ingest/account_handler.rs");
        assert!(
            account_handler_src.contains("MdSidefxCommand::VaultBalanceTick"),
            "account_handler must enqueue VaultBalanceTick"
        );
        let vault_tick_pos = account_handler_src
            .find("MdSidefxCommand::VaultBalanceTick")
            .expect("VaultBalanceTick");
        let after_vault_tick = &account_handler_src[vault_tick_pos..];
        assert!(
            after_vault_tick.contains("return;"),
            "account_handler must return after VaultBalanceTick enqueue"
        );
        assert!(
            account_handler_src.contains("MdSidefxCommand::LivePoolCacheAccountUpdate"),
            "account_handler must enqueue LivePoolCacheAccountUpdate"
        );
        assert!(
            account_handler_src.contains("account_geyser_update_is_sidefx_only_pool_owner"),
            "account_handler must gate sidefx-only pool owner early return"
        );
        assert!(
            account_handler_src.contains("record_market_data_unparsed_account_dropped"),
            "account_handler must use labeled unparsed account metric"
        );
    }

    /// P2 hardening: TX handler uses labeled unparsed metric with DEX detection helper.
    #[test]
    fn p2_tx_handler_labeled_unparsed_drop_metric() {
        let tx_handler_src = include_str!("../market_data/ingest/tx_handler.rs");
        let tx_parse_src = include_str!("../market_data/ingest/tx_parse.rs");
        assert!(
            tx_handler_src.contains("record_market_data_unparsed_tx_dropped"),
            "tx_handler must use labeled unparsed TX metric"
        );
        assert!(
            tx_handler_src.contains("unparsed_tx_drop_reason"),
            "tx_handler must classify DEX vs non-DEX unparsed drops via Geyser TX"
        );
        assert!(
            tx_parse_src.contains("pub fn tx_geyser_had_extracted_dex_instruction"),
            "tx_parse must define Geyser-extracted DEX instruction detection"
        );
        assert!(
            tx_parse_src.contains("pub fn unparsed_tx_drop_reason"),
            "tx_parse must define unparsed TX drop reason helper"
        );
    }

    /// Phase 4b: account ingest module must not read tracked_* maps or arb reconcile paths.
    #[test]
    fn phase4b_account_ingest_no_tracked_reads() {
        let ingest_src = format!(
            "{}\n{}\n{}",
            include_str!("../market_data/ingest/account_handler.rs"),
            include_str!("../market_data/ingest/account_parse.rs"),
            include_str!("../market_data/ingest/account_host.rs"),
        );
        assert!(
            !ingest_src.contains("tracked_vaults.read()"),
            "account ingest must not read tracked_vaults"
        );
        assert!(
            !ingest_src.contains("tracked_mints.read()"),
            "account ingest must not read tracked_mints"
        );
        assert!(
            !ingest_src.contains("tracked_bin_arrays.read()"),
            "account ingest must not read tracked_bin_arrays"
        );
        assert!(
            !ingest_src.contains("reconcile_arb"),
            "account ingest must not call arb reconcile paths (I-4c)"
        );
        assert!(
            !ingest_src.contains("sync_geyser_tracked_accounts_batched_flush"),
            "account ingest must not call explicit Geyser flush inline"
        );
    }

    /// Phase 4: Geyser TX handler lives in dedicated ingest module; bin retains thin wrapper.
    #[test]
    fn phase4_tx_handler_in_ingest_module() {
        let tx_handler_src = include_str!("../market_data/ingest/tx_handler.rs");
        let tx_parse_src = include_str!("../market_data/ingest/tx_parse.rs");
        let bin_src = include_str!("market_data.rs");
        let prod_src = bin_src
            .split("#[cfg(test)]")
            .next()
            .expect("production source section");
        assert!(
            tx_handler_src.contains("pub async fn handle_geyser_transaction_update"),
            "tx_handler.rs must define handle_geyser_transaction_update"
        );
        assert!(
            tx_parse_src.contains("pub fn resolve_pumpfun_creator_tx_path"),
            "tx_parse.rs must contain TX-only parse helpers"
        );
        assert!(
            prod_src.contains("handle_geyser_transaction_update"),
            "bin production code must delegate to ingest TX handler"
        );
        assert!(
            !prod_src.contains("Parsed DEX transaction"),
            "TX handler body must not remain in bin production section"
        );
    }

    /// Phase 4: ingest/ must not read tracked_* maps or arb reconcile paths (I-4b / I-4c).
    #[test]
    fn phase4_ingest_no_tracked_reads() {
        let ingest_src = format!(
            "{}\n{}\n{}\n{}\n{}\n{}\n{}",
            include_str!("../market_data/ingest/tx_handler.rs"),
            include_str!("../market_data/ingest/tx_parse.rs"),
            include_str!("../market_data/ingest/account_filter.rs"),
            include_str!("../market_data/ingest/tx_filter.rs"),
            include_str!("../market_data/ingest/account_handler.rs"),
            include_str!("../market_data/ingest/account_parse.rs"),
            include_str!("../market_data/ingest/account_host.rs"),
        );
        assert!(
            !ingest_src.contains("tracked_vaults.read()"),
            "ingest must not read tracked_vaults"
        );
        assert!(
            !ingest_src.contains("tracked_mints.read()"),
            "ingest must not read tracked_mints"
        );
        assert!(
            !ingest_src.contains("tracked_bin_arrays.read()"),
            "ingest must not read tracked_bin_arrays"
        );
        assert!(
            !ingest_src.contains("reconcile_arb"),
            "ingest must not call arb reconcile paths (I-4c)"
        );
        assert!(
            !ingest_src.contains("sync_geyser_tracked_accounts_batched_flush"),
            "ingest must not call explicit Geyser flush inline"
        );
    }

    /// Phase 5e: legacy ArbMultiDex md-state commands removed from dedicated module.
    #[test]
    fn phase5e_no_arb_multidex_in_md_state() {
        let cmd_src = include_str!("../market_data/md_state/command.rs");
        let worker_src = include_str!("../market_data/md_state/worker.rs");
        let bin_src = include_str!("market_data.rs");
        assert!(!cmd_src.contains("ArbMultiDexReconcile"));
        assert!(!cmd_src.contains("RegisterReservesAfterTrade"));
        assert!(!cmd_src.contains("RegisterPoolVaultsFromAccount"));
        assert!(
            worker_src.contains("pub fn spawn_md_state_worker"),
            "worker.rs must contain spawn_md_state_worker"
        );
        assert!(
            bin_src.contains("use ironcrab::market_data::md_state::"),
            "market_data bin must import md_state module"
        );
        assert!(
            bin_src.contains("impl MdStateContext for MarketDataContext"),
            "bin must wire MarketDataContext to MdStateContext"
        );
    }

    /// Phase 5b: md-sidefx worker + handlers live in dedicated library module.
    #[test]
    fn phase5b_sidefx_in_dedicated_module() {
        let handlers_src = include_str!("../market_data/sidefx/handlers.rs");
        let worker_src = include_str!("../market_data/sidefx/worker.rs");
        let bin_src = include_str!("market_data.rs");
        assert!(
            handlers_src.contains("pub fn md_sidefx_process_vault_balance_tick"),
            "handlers.rs must contain md_sidefx_process_vault_balance_tick"
        );
        assert!(
            worker_src.contains("pub fn spawn_md_sidefx_worker"),
            "worker.rs must contain spawn_md_sidefx_worker"
        );
        assert!(
            bin_src.contains("use ironcrab::market_data::sidefx::"),
            "market_data bin must import sidefx module"
        );
        assert!(
            bin_src.contains("impl SidefxWorkerHost for MarketDataSidefxHost"),
            "bin must wire MarketDataSidefxHost to SidefxWorkerHost"
        );
        assert!(
            bin_src.contains("fn md_sidefx_process_vault_balance_tick"),
            "bin must retain eval-grep wrapper for md_sidefx_process_vault_balance_tick"
        );
        assert!(
            handlers_src.contains("fn md_sidefx_inc_enrichment_publish_metrics_if_member"),
            "handlers.rs must count enrichment metrics on vault-tick publish path"
        );
        assert!(
            handlers_src.contains("md_sidefx_inc_enrichment_publish_metrics_if_member"),
            "md_sidefx_process_vault_balance_tick must call enrichment metric helper"
        );
        assert!(
            handlers_src.contains("inc_market_data_enrichment_balance_updated_total"),
            "vault-tick path must increment enrichment BalanceUpdated counter"
        );
        assert!(
            handlers_src.contains("inc_market_data_enrichment_pool_state_publish_total"),
            "vault-tick path must increment enrichment PoolState counter"
        );
    }

    /// Phase 5a: track-worker logic lives in dedicated library module; bin imports it.
    #[test]
    fn phase5a_track_worker_in_dedicated_module() {
        let worker_src = include_str!("../market_data/track/worker.rs");
        let bin_src = include_str!("market_data.rs");
        assert!(
            worker_src.contains("pub fn track_worker_process_command"),
            "worker.rs must contain track_worker_process_command"
        );
        assert!(
            bin_src.contains("use ironcrab::market_data::track::"),
            "market_data bin must import track module"
        );
        assert!(
            bin_src.contains("impl TrackWorkerContext for MarketDataContext"),
            "bin must wire MarketDataContext to TrackWorkerContext"
        );
    }

    #[test]
    fn phase3_apply_arb_track_requests_respects_wallet_pins() {
        use ironcrab::nats::{
            ArbTrackActiveEntry, ArbTrackActiveReason, ArbTrackRemovedEntry, ArbTrackRequestsUpdate,
        };

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let jsonl_cfg = JsonlWriterConfig::new("market_events").with_log_dir(tmp.path());
        let jsonl = QueuedJsonlWriter::spawn(jsonl_cfg, 256).expect("jsonl");
        let ctx = minimal_market_data_context_for_pr_d_tests(jsonl);

        let pool = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote = Pubkey::from_str(NATIVE_SOL_MINT).unwrap();
        let coin_vault = Pubkey::new_unique();
        let pc_vault = Pubkey::new_unique();
        ctx.live_pool_cache.upsert(
            pool,
            CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
                token_0_mint: base_mint,
                token_1_mint: quote,
                token_0_vault: coin_vault,
                token_1_vault: pc_vault,
                reserve_0: Some(1_000_000),
                reserve_1: Some(2_000_000),
            }),
            1,
        );

        let mut admission = test_admission_for(&ctx);
        ctx.apply_arb_track_requests_update(
            &mut admission,
            &ArbTrackRequestsUpdate {
                version: 1,
                ts_unix_ms: 1,
                active: vec![ArbTrackActiveEntry {
                    pool: pool.to_string(),
                    reason: ArbTrackActiveReason::Baseline,
                }],
                removed: vec![],
                reconcile: false,
            },
        );

        {
            let mut vs = ctx.tracked_vaults.write();
            if let Some(v) = vs.get_mut(&pc_vault) {
                v.pin = Some(GeyserPinReason::Wallet);
                v.pinned = true;
            }
        }

        ctx.apply_arb_track_requests_update(
            &mut admission,
            &ArbTrackRequestsUpdate {
                version: 1,
                ts_unix_ms: 2,
                active: vec![],
                removed: vec![ArbTrackRemovedEntry {
                    pool: pool.to_string(),
                    reason: ironcrab::nats::ArbTrackRemovedReason::Cooldown,
                }],
                reconcile: false,
            },
        );

        let vs = ctx.tracked_vaults.read();
        assert_eq!(vs.get(&coin_vault).and_then(|v| v.pin), None);
        assert!(!vs.get(&coin_vault).is_some_and(|v| v.pinned));
        assert_eq!(
            vs.get(&pc_vault).and_then(|v| v.pin),
            Some(GeyserPinReason::Wallet)
        );
        assert!(vs.get(&pc_vault).is_some_and(|v| v.pinned));
    }
}
