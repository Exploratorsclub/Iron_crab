//! arb-strategy binary – Typ A Market-Driven Arbitrage Strategy
//!
//! Source of Truth: docs/TARGET_ARCHITECTURE.md §2.2.1
//!
//! Responsibilities:
//! - Consume MarketEvents from market-data
//! - Track pools across DEXes (same token pairs on different DEXes)
//! - Detect price spreads and calculate arbitrage opportunities
//! - Generate TradeIntents with origin_type: StrategyA
//!
//! This binary does NOT:
//! - Load wallet keys (keyless)
//! - Sign or send transactions
//! - React to specific parent transactions (that's Typ B MEV)

use anyhow::Result;
use clap::Parser;
use parking_lot::RwLock;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;

use ironcrab::arbitrage::{
    arb_track_removal_reason, classify_cross_dex_sell_failure, dlmm_marginal_price_plausible,
    dlmm_sol_output_from_bins, dlmm_token_output_from_bins, freshness_age_bucket,
    is_arb_route_executable, is_expected_token_output_plausible, is_quote_fresh,
    populate_arb_slave_from_live_pool_cache, price_based_token_output_raw, quote_exact_in,
    quote_exact_in_with_freshness, quote_sell_round_trip, quotes_pairable,
    round_trip_profit_lamports, select_arb_track_pools, select_round_trip_pools,
    sync_arb_slave_from_pool_cache_update, MultiHopArbitrage, MultiHopConfig, MultiHopIntentBatch,
    NoCrossDexSellDetailReason, PoolQuote, QuoteFreshnessConfig, QuoteKind, QuotePoolInput,
    QuoteVaultInput, RoundTripInsufficient, RoundTripInsufficientSubreason, RoundTripLeg,
    RoundTripPoolCandidate, RoundTripSelectFailure, SellQuoteNoneDetailReason,
    TrackCandidateCounts, TrackMintInput, TrackPoolInput, TrackPoolReadiness, TrackSelectionConfig,
    DLMM_PROBE_SOL_LAMPORTS,
};
use ironcrab::config::Config as AppConfig;
use ironcrab::execution::live_pool_cache::{
    create_shared_cache, CachedPoolState, LivePoolCache, SharedLivePoolCache,
};
use ironcrab::execution::pool_cache_sync::bootstrap_pool_cache_from_jetstream;
use ironcrab::ipc::{
    BinData, ConfigUpdate, ConfigUpdateResponse, ConfigUpdateStatus, ExplicitAmount, IntentOrigin,
    IntentTier, MarketEvent, MarketEventKind, PoolCacheUpdate, PoolCacheUpdateType, TradeIntent,
    TradeResources, TradeSide, TradingRegime,
};
use ironcrab::metrics::{
    arb_heartbeat_finished, arb_pool_cache_apply_batches_inc, arb_pool_cache_sync_fetch_empty_inc,
    arb_pool_cache_sync_messages_add, arb_pool_cache_updates_applied_add,
    arb_strategy_bootstrap_skip_inc, arb_strategy_bootstrap_warmup_set,
    arb_strategy_pool_cache_update_seeded_inc, arb_strategy_pool_cache_update_seen_inc,
    arb_strategy_pool_cache_update_skip_no_seed_inc,
    arb_strategy_pool_cache_update_skip_non_arb_quote_inc, arb_subscriber_high_dropped_inc,
    arb_subscriber_high_processed_inc, arb_subscriber_high_queue_depth_set,
    arb_subscriber_low_coalesced_inc, arb_subscriber_low_dropped_inc,
    arb_subscriber_low_processed_inc, arb_subscriber_low_queue_depth_set,
    arb_subscriber_pool_created_skipped_inc, arb_tracker_write_coalesced_flushed_inc,
    arb_tracker_write_coalesced_inc, arb_tracker_write_coalescer_flush_lost_inc,
    arb_tracker_write_coalescer_flush_lost_total, arb_tracker_write_enqueue_dropped_inc,
    arb_tracker_write_init_worker_state, arb_tracker_write_job_finished,
    arb_tracker_write_job_processed_inc, arb_tracker_write_job_started,
    arb_tracker_write_stall_watchdog_inc, arb_two_hop_eligible_dexes_add,
    arb_two_hop_eligible_pools_by_dex_add, arb_two_hop_insufficient_subreason_inc,
    arb_two_hop_opportunity_inc, arb_two_hop_pool_gate_add, arb_two_hop_reject_subreason_inc,
    arb_two_hop_rejected_inc, arb_two_hop_tracker_seeded_pools_add,
    arb_two_hop_v2_incompatible_kind_inc, arb_two_hop_v2_insufficient_subreason_inc,
    arb_two_hop_v2_no_cross_dex_sell_detail_inc, arb_two_hop_v2_no_fresh_buy_quote_detail_inc,
    arb_two_hop_v2_rejected_inc, arb_two_hop_v2_round_trip_formable_inc, arb_two_hop_v2_screen_inc,
    arb_two_hop_v2_screen_multi_dex_inc, arb_two_hop_v2_screen_skipped_inc,
    arb_two_hop_v2_sell_not_fresh_detail_inc, arb_two_hop_v2_sell_quote_none_detail_inc,
    arb_two_hop_v2_state_stale_age_bucket_inc, arb_vault_live_snapshot_cache_age_bucket_inc,
    arb_vault_live_snapshot_cache_age_pin_bucket_inc, inc_arb_dlmm_bin_array_update_applied_total,
    inc_arb_dlmm_bin_array_update_received_total, inc_arb_dlmm_bin_rescreen_scheduled_total,
    inc_arb_pinned_meteora_pool_bin_cache_miss_total, inc_arb_pool_accounts_backfill,
    inc_arb_v2_screen_meteora_sell_bin_hit_total, inc_arb_v2_screen_meteora_sell_bin_miss_total,
    inc_arb_v2_screen_sell_stale_recovery_scheduled_total,
    inc_arb_v2_screen_sell_stale_then_fresh_after_pin_total,
    inc_arb_v2_sell_stale_recovery_outcome_total, inc_arb_vault_balance_applied_total,
    inc_arb_vault_live_snapshot_refreshed_total, inc_arb_vault_live_snapshot_seeded_total,
    inc_arb_vault_rescreen_scheduled_total, inc_arb_vault_seed_from_cache_miss_total,
    inc_arb_vault_seed_from_cache_ok_total, record_arb_heartbeat_phase,
    record_arb_intent_suppressed_implausible_token_out,
    record_arb_intent_suppressed_unsupported_route, record_arb_price_freshness_stale_age_ms,
    record_arb_proactive_pin_first_publish, record_arb_proactive_track_publish_total,
    record_arb_quote_pair_slot_delta, record_arb_quote_shadow_round_trip,
    record_arb_track_publish_skipped_unchanged_total, record_arb_track_removed_total,
    record_arb_track_requests_messages_total, record_arb_track_requests_publish_chunks_total,
    record_arb_track_requests_publish_failed_total,
    record_arb_track_selection_blocking_join_failed_total,
    record_arb_track_selection_queue_overflow_total, record_arb_track_selection_recompute_total,
    record_arb_two_hop_v2_formable_gates, record_arb_writer_lock_wait, serve_metrics,
    set_arb_pool_cache_apply_batch_size_gauge, set_arb_quote_shadow_legacy_spread_bps,
    set_arb_track_selected_pool_readiness_metrics, set_arb_track_selection_metrics,
    set_arb_tracker_write_coalescer_pending, set_arb_tracker_write_queue_depth,
    set_arb_two_hop_blocked_on_apply_trade, set_readiness_nats_connected,
    tick_arb_heartbeat_seconds_since_last_finish, tick_arb_tracker_write_seconds_since_last_finish,
    try_record_arb_track_pin_before_first_screen_ms, wall_clock_unix_ms_now, ArbHeartbeatPhase,
    ArbPoolAccountsBackfillSource, ArbStrategyWarmupSkipReason, ArbTrackerWriteJobType,
    ArbTwoHopInsufficientSubreason, ArbTwoHopPoolGate, ArbTwoHopRejectReason,
    ArbTwoHopRejectSubreason, ArbTwoHopV2FormableGateOutcome, ArbTwoHopV2InsufficientSubreason,
    ArbTwoHopV2NoCrossDexSellDetail, ArbTwoHopV2RejectReason, ArbTwoHopV2ScreenSkipReason,
    ArbTwoHopV2SellQuoteNoneDetail, ArbWriterLockKind, MetricsComponent,
    ARB_HEARTBEAT_SECONDS_SINCE_LAST_FINISH, ARB_REJECTED_MISSING_ACCOUNTS,
    ARB_SUBSCRIBER_HIGH_PROCESSED_TOTAL, ARB_SUBSCRIBER_HIGH_QUEUE_DEPTH,
    ARB_TRACKER_WRITE_COALESCER_PENDING, ARB_TRACKER_WRITE_CURRENT_JOB_STARTED_UNIX_MS,
    ARB_TRACKER_WRITE_CURRENT_JOB_TYPE, ARB_TRACKER_WRITE_QUEUE_DEPTH,
    ARB_TRACKER_WRITE_SECONDS_SINCE_LAST_FINISH, ARB_TRIANGLE_OPPORTUNITIES,
    INTENTS_GENERATED_TOTAL, MARKET_EVENTS_CONSUMED_TOTAL, NATS_MESSAGES_PUBLISHED_TOTAL,
    NATS_MESSAGES_RECEIVED_TOTAL, POOLS_TRACKED_GAUGE, TOKENS_TRACKED_GAUGE,
};
use ironcrab::nats::{
    arb_strategy_pool_cache_live_consumer_config, arb_track_payload_bytes, config_consumer_config,
    config_subject, split_arb_track_requests_update, trim_reconcile_update_to_budget,
    ArbTrackActiveEntry, ArbTrackActiveReason, ArbTrackReadiness, ArbTrackRemovedEntry,
    ArbTrackRequestsUpdate, ARB_TRACK_PUBLISH_MAX_PAYLOAD_BYTES, CONFIG_STREAM_NAME, STREAM_NAME,
};
use ironcrab::nats::{NatsClient, NatsConfig};
use ironcrab::nats::{TOPIC_ARB_TRACK_REQUESTS, TOPIC_MARKET_EVENTS, TOPIC_TRADE_INTENTS};
use ironcrab::storage::{JsonlWriter, JsonlWriterConfig};
use solana_sdk::pubkey::Pubkey;

type JetStreamPullConsumer =
    async_nats::jetstream::consumer::Consumer<async_nats::jetstream::consumer::pull::Config>;

/// Build version for decision records
const BUILD_VERSION: &str = env!("CARGO_PKG_VERSION");

/// NATS topic for config reload commands from control-plane (Core NATS fallback)
const TOPIC_CONFIG_RELOAD: &str = "ironcrab.control.config.reload";

/// Map selection readiness to arb track wire format.
fn track_pool_readiness_to_wire(r: TrackPoolReadiness) -> ArbTrackReadiness {
    match r {
        TrackPoolReadiness::Rejected => ArbTrackReadiness::Rejected,
        TrackPoolReadiness::Warmable => ArbTrackReadiness::Warmable,
        TrackPoolReadiness::QuoteReady => ArbTrackReadiness::QuoteReady,
        TrackPoolReadiness::Executable => ArbTrackReadiness::Executable,
    }
}

/// Wire format version for `TOPIC_ARB_TRACK_REQUESTS`.
const ARB_TRACK_REQUESTS_WIRE_VERSION: u32 = 1;
/// Default cap for baseline reconcile `active[]` (configurable via `arb_track_baseline_max_pools`).
/// Raised after pair-complete selection (S1): budget applies to complete pairs only; orphan gauge stays 0.
const ARB_TRACK_BASELINE_MAX_POOLS_DEFAULT: usize = 2000;
/// Default baseline reconcile interval (configurable via `arb_track_reconcile_interval_secs`).
const ARB_TRACK_RECONCILE_INTERVAL_SECS_DEFAULT: u64 = 60;
/// Max trade-signal pairs remembered per mint (bounded LRU by recency).
const ARB_TRADE_SIGNAL_PAIRS_CAP: usize = 64;
/// Coalesce dirty-mint marks before an incremental global select.
const ARB_TRACK_SELECTION_COALESCE_MS: u64 = 50;
/// Minimum interval between incremental global selects (rate limit).
const ARB_TRACK_INCREMENTAL_MIN_INTERVAL_MS: u64 = 1_000;
/// Max dirty mints queued between incremental selects.
const ARB_TRACK_SELECTION_DIRTY_MINTS_CAP: usize = 4_096;
/// Hot-path ingress dedup set cap (one slot per unique mint, not per Geyser update).
const ARB_TRACK_SELECTION_INGRESS_DIRTY_CAP: usize = ARB_TRACK_SELECTION_DIRTY_MINTS_CAP;
/// Single wake token queue: coalesces unlimited hot-path marks into one worker wakeup.
const ARB_TRACK_SELECTION_WAKE_QUEUE_CAP: usize = 1;
/// Minimum interval between global full-reconcile scans (prevents 50ms scan storms).
const ARB_TRACK_FULL_RECONCILE_MIN_INTERVAL_MS: u64 = 5_000;
/// Max mint snapshots retained for selection (bounded publish/readiness state).
const ARB_TRACK_MINT_SNAPSHOTS_CAP: usize = 2_048;
/// Max dirty mints processed per incremental batch; overflow schedules full reconcile.
const ARB_TRACK_INCREMENTAL_MINTS_MAX: usize = 64;

/// Bounded queue for off-hot-loop 2-hop trade detection (Scope D).
const ARB_TWO_HOP_WORKER_QUEUE_CAP: usize = 4096;
/// C1h5: min interval between sell-leg stale recovery rescreens per mint.
const V2_SELL_STALE_RECOVERY_MIN_INTERVAL_MS: u64 = 30_000;
/// C1h5: cap buy candidates scanned when detecting pinned sell-leg staleness.
const V2_SELL_STALE_RECOVERY_MAX_BUY_CANDIDATES: usize = 4;

/// C1h5 v2: pending sell-leg recovery state per mint (rate limit + target pair).
#[derive(Debug, Clone)]
struct V2SellStaleRecoveryPending {
    scheduled_at: Instant,
    buy_pool: String,
    sell_pool: String,
}
/// Max PoolCache updates applied per burst before yielding (main-loop decoupling).
const ARB_POOL_CACHE_APPLY_BATCH_MAX: usize = 20;
/// Single-writer queue for `trackers` / `vault_balances` mutations.
const ARB_TRACKER_WRITE_QUEUE_CAP: usize = 8192;
/// Max distinct pool keys coalesced before latest-wins eviction (tracker-write ingress).
const ARB_TRACKER_WRITE_COALESCER_CAP: usize = 65536;
/// Bounded side-map for DexPoolAccounts that arrive before a mint tracker exists.
const PENDING_POOL_ACCOUNTS_CAP: usize = 4_096;
/// Global pool_address → DexPoolAccounts index (O(1) lookup across mint trackers).
const POOL_ACCOUNTS_INDEX_CAP: usize = 8_192;

// ============================================================================
// Configuration
// ============================================================================

#[derive(Debug, Clone)]
struct ArbConfig {
    /// Minimum spread in bps to consider arbitrage. Default: 50 (0.5%)
    min_spread_bps: u32,
    /// Minimum profit in lamports after estimated fees. Default: 10_000_000 (0.01 SOL)
    min_profit_lamports: u64,
    /// Maximum position size in lamports. Default: 1_000_000_000 (1 SOL)
    max_position_lamports: u64,
    /// Estimated transaction cost in lamports. Default: 50_000 (0.00005 SOL)
    est_tx_cost_lamports: u64,
    /// Maximum slippage tolerance in bps. Default: 100 (1%)
    max_slippage_bps: u32,
    /// Cooldown between intents for same pair in ms. Default: 5000ms
    intent_cooldown_ms: u64,
    /// TTL for intents in ms. Default: 1000ms (reduced from 3000ms for Option C)
    /// Since execution-engine calculates fresh min_out from Geyser cache,
    /// we can use shorter TTL without quote staleness issues.
    intent_ttl_ms: u64,
    /// Enable 2-hop arbitrage (A→B on DEX1, B→A on DEX2). Default: true
    two_hop_enabled: bool,
    /// Max pools in baseline reconcile snapshot. Default: 2000.
    arb_track_baseline_max_pools: usize,
    /// Baseline reconcile publish interval in seconds. Default: 60.
    arb_track_reconcile_interval_secs: u64,
    /// Shadow pool_quote round-trip metrics (legacy path stays authoritative). Default: false.
    arb_quote_shadow_mode: bool,
    /// Profit-first 2-hop v2 (round-trip quotes). Default: false.
    arb_two_hop_v2_enabled: bool,
    /// SOL probe for v2 round-trip screening. Default: follows max_position_lamports.
    arb_probe_lamports: u64,
    /// When true, arb_probe_lamports tracks max_position_lamports on load/update.
    arb_probe_follows_max_position: bool,
    /// LastTradeMid quote TTL for v2 freshness. Default: 30_000 ms.
    arb_quote_trade_ttl_ms: u64,
    /// ExecutableMarginal state TTL for v2 freshness. Default: 120_000 ms.
    arb_quote_state_ttl_ms: u64,
    /// Max allowed |buy.as_of_slot - sell.as_of_slot| for v2 round-trip (0 = gate off). Default: 2.
    arb_max_leg_slot_delta: u64,
}

impl Default for ArbConfig {
    fn default() -> Self {
        let mut cfg = Self {
            min_spread_bps: 50,                   // 0.5% minimum spread
            min_profit_lamports: 10_000_000,      // 0.01 SOL min profit
            max_position_lamports: 1_000_000_000, // 1 SOL max position
            est_tx_cost_lamports: 50_000,         // 0.00005 SOL tx cost
            max_slippage_bps: 100,                // 1% max slippage
            intent_cooldown_ms: 5000,             // 5s cooldown per pair
            intent_ttl_ms: 1000,                  // 1s TTL (Option C: fresh quotes in exec-engine)
            two_hop_enabled: true,                // 2-hop arb enabled by default
            arb_track_baseline_max_pools: ARB_TRACK_BASELINE_MAX_POOLS_DEFAULT,
            arb_track_reconcile_interval_secs: ARB_TRACK_RECONCILE_INTERVAL_SECS_DEFAULT,
            arb_quote_shadow_mode: false,
            arb_two_hop_v2_enabled: false,
            arb_probe_lamports: DLMM_PROBE_SOL_LAMPORTS,
            arb_probe_follows_max_position: true,
            arb_quote_trade_ttl_ms: 30_000,
            arb_quote_state_ttl_ms: 120_000,
            arb_max_leg_slot_delta: 2,
        };
        sync_arb_probe_to_max_position(&mut cfg);
        cfg
    }
}

/// Keep v2 screening probe aligned with max position unless explicitly overridden.
fn sync_arb_probe_to_max_position(config: &mut ArbConfig) {
    if config.arb_probe_follows_max_position && config.max_position_lamports > 0 {
        config.arb_probe_lamports = config.max_position_lamports;
    }
}

fn load_initial_arb_config(config_path: &Path) -> ArbConfig {
    let mut cfg = ArbConfig::default();

    let app_cfg = match AppConfig::load(config_path) {
        Ok(c) => c,
        Err(e) => {
            warn!(
                error = %e,
                config = %config_path.display(),
                "Failed to load config TOML; using arb-strategy defaults"
            );
            return cfg;
        }
    };

    let Some(arb) = app_cfg.arbitrage else {
        info!(
            config = %config_path.display(),
            "No [arbitrage] section in config; using arb-strategy defaults"
        );
        return cfg;
    };

    if let Some(v) = arb.est_tx_cost_lamports {
        cfg.est_tx_cost_lamports = v;
    }
    if let Some(exec) = arb.execution {
        cfg.max_slippage_bps = exec.max_slippage_bps;
        cfg.max_position_lamports = exec.max_position_lamports;
    }

    // Map min_profit_bps -> min_profit_lamports by interpreting it as net profit bps
    // relative to max_position_lamports.
    if let Some(min_profit_bps) = arb.min_profit_bps {
        let implied_min_profit = (cfg
            .max_position_lamports
            .saturating_mul(min_profit_bps as u64))
            / 10_000;
        if implied_min_profit > 0 {
            cfg.min_profit_lamports = implied_min_profit;
        }
    }

    sync_arb_probe_to_max_position(&mut cfg);
    cfg
}

// Known token mints for sanity checks
const NATIVE_SOL_MINT: &str = "So11111111111111111111111111111111111111112";
const USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const USDT_MINT: &str = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB";

// Maximum reasonable spread before considering it a data error
const MAX_REASONABLE_SPREAD_BPS: i64 = 1000; // 10%
const STABLECOIN_MAX_SPREAD_BPS: i64 = 200; // 2% for stablecoins
/// Broad stablecoin comparable-price guard (SOL per 1 whole token); not a hardcoded SOL/USD peg.
const STABLECOIN_MIN_SOL_PER_TOKEN: &str = "0.0001";
const STABLECOIN_MAX_SOL_PER_TOKEN: &str = "1";
/// Geyser connection is considered broken if no MarketEvent received for this duration.
/// This is NOT about individual pool staleness - it's about connection health.
/// If Geyser is connected but a pool has no updates, the data IS current (pool is inactive).
const GEYSER_CONNECTION_TIMEOUT_SECS: u64 = 30;
const MIN_TRADE_VOLUME_LAMPORTS: u64 = 100_000; // 0.0001 SOL minimum (filter dust)
/// Max age for pool comparable prices used in 2-hop spread (aligns with Geyser health window).
const MAX_PRICE_AGE_MS: u64 = 30_000;
const SPREAD_TOO_LARGE_WARN_COOLDOWN: Duration = Duration::from_secs(30);
/// Rate limit for 2-hop eligibility diagnostic snapshots.
const ELIGIBILITY_SNAPSHOT_COOLDOWN: Duration = Duration::from_secs(60);
const ELIGIBILITY_SNAPSHOT_TOP_N: usize = 10;
const ELIGIBILITY_SNAPSHOT_POOL_ROWS: usize = 5;
const ELIGIBILITY_PENDING_CAP: usize = 256;

/// Bounded HIGH-priority MarketEvent queue (Trade + active-pool state updates).
const ARB_HIGH_EVENT_QUEUE_CAP: usize = 8192;
/// Max distinct LOW-priority pool keys coalesced before latest-wins eviction.
const ARB_LOW_COALESCER_CAP: usize = 2048;
/// Heartbeat warns when HIGH queue depth exceeds this fraction of capacity.
const ARB_HIGH_QUEUE_WARN_PCT: u64 = 80;

static DLMM_MARGINAL_PRICE_REJECTED_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Which marginal quote to use when ranking pools for 2-hop spread.
#[derive(Copy, Clone, Eq, PartialEq)]
enum ComparablePriceSide {
    /// SOL → token (buy leg / cheapest pool).
    Buy,
    /// Token → SOL (sell leg / highest bid).
    Sell,
}

fn is_known_dex_label(dex: &str) -> bool {
    matches!(
        dex,
        "raydium" | "raydium_cpmm" | "orca" | "meteora_dlmm" | "pumpfun" | "pump_amm"
    )
}

#[derive(Parser, Debug)]
#[command(name = "arb-strategy")]
#[command(about = "IronCrab Typ A Arbitrage Strategy – Market-driven cross-DEX arbitrage")]
struct Args {
    /// Path to configuration file
    #[arg(short, long, default_value = "config.toml")]
    config: PathBuf,

    /// NATS server URL
    #[arg(long, env = "NATS_URL", default_value = "nats://localhost:4222")]
    nats_url: String,

    /// Prometheus metrics port
    #[arg(long, default_value = "9803")]
    metrics_port: u16,

    /// Log directory override
    #[arg(long, env = "IRONCRAB_LOG_DIR")]
    log_dir: Option<PathBuf>,

    /// Dry run: don't publish intents to NATS
    #[arg(long)]
    dry_run: bool,
}

// ============================================================================
// Pool Tracking for Cross-DEX Arbitrage
// ============================================================================

/// Comparable price semantics for 2-hop: **SOL per 1 whole token** (not lamports, not tokens/SOL).
/// Reserve-mid from Geyser vault balances is preferred; trade-implied prices use buy/sell mid.
fn reserve_mid_sol_per_token(
    reserve_base: u64,
    reserve_quote: u64,
    token_decimals: u8,
) -> Option<Decimal> {
    if reserve_base == 0 || reserve_quote == 0 {
        return None;
    }
    let sol = Decimal::from(reserve_quote) / Decimal::from(1_000_000_000u64);
    let token_divisor = 10u64.pow(token_decimals as u32);
    let tokens = Decimal::from(reserve_base) / Decimal::from(token_divisor);
    if tokens <= Decimal::ZERO {
        return None;
    }
    Some(sol / tokens)
}

/// Trade-implied SOL per token from a single fill (same units as reserve mid).
fn trade_implied_sol_per_token(sol_amount: u64, token_amount: u64, token_decimals: u8) -> Decimal {
    let sol_dec = Decimal::from(sol_amount) / Decimal::from(1_000_000_000u64);
    let token_divisor = 10u64.pow(token_decimals as u32);
    let token_dec = Decimal::from(token_amount) / Decimal::from(token_divisor);
    if token_dec <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    sol_dec / token_dec
}

fn vault_dlmm_sol_is_x(vault: &VaultBalanceCache) -> bool {
    vault
        .dlmm_token_x_mint
        .as_deref()
        .map(|m| m == NATIVE_SOL_MINT)
        .unwrap_or(vault.dlmm_sol_is_x)
}

/// Build vault row from SLAVE LivePoolCache entry (Geyser-only, SOL-quoted pools only).
fn vault_balance_from_live_cache_state(
    state: &CachedPoolState,
    slot: u64,
    age_ms: u64,
) -> Option<VaultBalanceCache> {
    let warmup = arb_warmup_pool_seed(state)?;
    if warmup.quote_kind != ArbWarmupQuoteKind::Sol {
        return None;
    }
    let cache_updated_at = Instant::now()
        .checked_sub(Duration::from_millis(age_ms))
        .unwrap_or_else(Instant::now);
    let dlmm_token_x_mint = match state {
        CachedPoolState::Meteora(s) => Some(s.token_x_mint.to_string()),
        _ => None,
    };
    let dlmm_sol_is_x = dlmm_token_x_mint.as_deref() == Some(NATIVE_SOL_MINT);
    Some(VaultBalanceCache {
        reserve_base: warmup.reserve_base,
        reserve_quote: warmup.reserve_quote,
        update_slot: slot,
        active_id: warmup.active_id,
        bin_step: warmup.bin_step,
        updated_at: cache_updated_at,
        dlmm_sol_is_x,
        dlmm_token_x_mint,
    })
}

fn live_pool_cache_fresher_than_vault(
    cache_slot: u64,
    cache_updated_at: Instant,
    existing: &VaultBalanceCache,
) -> bool {
    cache_slot > existing.update_slot || cache_updated_at > existing.updated_at
}

/// H3 / C1vault: seed missing vault row from SLAVE LivePoolCache (Geyser-only, all SOL-quoted DEXes).
fn try_seed_vault_from_live_cache(
    pool_address: &str,
    live_pool_cache: &SharedLivePoolCache,
    vault_cache: &mut HashMap<String, VaultBalanceCache>,
    pin_class: &str,
) -> bool {
    if vault_cache.contains_key(pool_address) {
        return false;
    }
    let Ok(pool_pk) = Pubkey::from_str(pool_address) else {
        return false;
    };
    let Some((state, slot, age_ms)) = live_pool_cache.get_with_metadata(&pool_pk) else {
        return false;
    };
    let Some(vault) = vault_balance_from_live_cache_state(&state, slot, age_ms) else {
        return false;
    };
    record_live_cache_age_at_snapshot("seed", age_ms, pin_class);
    vault_cache.insert(pool_address.to_string(), vault);
    true
}

/// C1h H1: refresh stale vault_balances row from fresher SLAVE LivePoolCache at screen snapshot.
fn try_refresh_vault_from_live_cache(
    pool_address: &str,
    live_pool_cache: &SharedLivePoolCache,
    vault_cache: &mut HashMap<String, VaultBalanceCache>,
    pin_class: &str,
) -> bool {
    let Ok(pool_pk) = Pubkey::from_str(pool_address) else {
        return false;
    };
    let existing = match vault_cache.get(pool_address) {
        Some(v) => v,
        None => return false,
    };
    let Some((state, slot, age_ms)) = live_pool_cache.get_with_metadata(&pool_pk) else {
        return false;
    };
    let Some(new_vault) = vault_balance_from_live_cache_state(&state, slot, age_ms) else {
        return false;
    };
    if !live_pool_cache_fresher_than_vault(slot, new_vault.updated_at, existing) {
        return false;
    }
    record_live_cache_age_at_snapshot("refresh", age_ms, pin_class);
    vault_cache.insert(pool_address.to_string(), new_vault);
    true
}

/// C1h5: for pinned pools, re-seed from SLAVE when local vault row exceeds state TTL (cold-start).
fn try_overwrite_stale_pinned_vault_from_live_cache(
    pool_address: &str,
    live_pool_cache: &SharedLivePoolCache,
    vault_cache: &mut HashMap<String, VaultBalanceCache>,
    pin_class: &str,
    state_ttl_ms: u64,
) -> bool {
    if pin_class != "pin" {
        return false;
    }
    let Some(existing) = vault_cache.get(pool_address) else {
        return false;
    };
    if existing.updated_at.elapsed().as_millis() as u64 <= state_ttl_ms {
        return false;
    }
    if try_refresh_vault_from_live_cache(pool_address, live_pool_cache, vault_cache, pin_class) {
        return true;
    }
    vault_cache.remove(pool_address);
    try_seed_vault_from_live_cache(pool_address, live_pool_cache, vault_cache, pin_class)
}

/// H3: seed missing DLMM vault row from SLAVE cache on BinArrayUpdate (Geyser-only).
fn try_seed_dlmm_vault_on_bin_update(
    pool_address: &str,
    update_slot: u64,
    live_pool_cache: &SharedLivePoolCache,
    vault_cache: &mut HashMap<String, VaultBalanceCache>,
) {
    if vault_cache.contains_key(pool_address) {
        return;
    }
    let Ok(pool_pk) = Pubkey::from_str(pool_address) else {
        return;
    };
    let Some(state) = live_pool_cache.get(&pool_pk) else {
        return;
    };
    let CachedPoolState::Meteora(s) = state else {
        return;
    };
    let x = s.token_x_mint.to_string();
    let y = s.token_y_mint.to_string();
    let (reserve_base, reserve_quote, dlmm_sol_is_x, dlmm_token_x_mint) = if y == NATIVE_SOL_MINT {
        (
            s.reserve_x_balance.unwrap_or(0),
            s.reserve_y_balance.unwrap_or(0),
            false,
            Some(x),
        )
    } else if x == NATIVE_SOL_MINT {
        (
            s.reserve_y_balance.unwrap_or(0),
            s.reserve_x_balance.unwrap_or(0),
            true,
            Some(x),
        )
    } else {
        return;
    };
    vault_cache.insert(
        pool_address.to_string(),
        VaultBalanceCache {
            reserve_base,
            reserve_quote,
            update_slot,
            active_id: Some(s.active_id),
            bin_step: Some(s.bin_step),
            updated_at: Instant::now(),
            dlmm_sol_is_x,
            dlmm_token_x_mint,
        },
    );
}

/// Resolve on-chain `token_x_mint` for DLMM PoolStateUpdate (normalized base/quote ≠ token_x/y).
fn resolve_dlmm_token_x_mint_for_pool_update(
    pool_address: &str,
    vault_cache: &HashMap<String, VaultBalanceCache>,
    live_pool_cache: &SharedLivePoolCache,
) -> Option<String> {
    if let Some(existing) = vault_cache
        .get(pool_address)
        .and_then(|v| v.dlmm_token_x_mint.clone())
    {
        return Some(existing);
    }
    let pool_pk = Pubkey::from_str(pool_address).ok()?;
    live_pool_cache.get(&pool_pk).and_then(|state| {
        if let CachedPoolState::Meteora(s) = state {
            Some(s.token_x_mint.to_string())
        } else {
            None
        }
    })
}

fn trade_mid_sol_per_token(pool: &PoolState) -> Option<Decimal> {
    match (pool.trade_price_buy, pool.trade_price_sell) {
        (Some(buy), Some(sell)) if buy > Decimal::ZERO && sell > Decimal::ZERO => {
            Some((buy + sell) / Decimal::from(2))
        }
        (Some(one), None) | (None, Some(one)) if one > Decimal::ZERO => Some(one),
        _ => None,
    }
}

fn is_stablecoin_mint(mint: &str) -> bool {
    mint == USDC_MINT || mint == USDT_MINT
}

fn is_common_quote_mint(mint: &str) -> bool {
    mint == NATIVE_SOL_MINT || mint == USDC_MINT || mint == USDT_MINT
}

/// Token mint tracked by `TokenArbTracker` for a pool pair with SOL/USDC/USDT on one side.
fn arb_tracked_token_mint<'a>(base_mint: &'a str, quote_mint: &'a str) -> Option<&'a str> {
    if is_common_quote_mint(base_mint) && is_common_quote_mint(quote_mint) {
        return None;
    }
    if base_mint == NATIVE_SOL_MINT {
        return Some(quote_mint);
    }
    if quote_mint == NATIVE_SOL_MINT {
        return Some(base_mint);
    }
    if is_stablecoin_mint(quote_mint) {
        return Some(base_mint);
    }
    if is_stablecoin_mint(base_mint) {
        return Some(quote_mint);
    }
    None
}

/// Bounded per-mint trade-signal buy/sell pair with recency.
#[derive(Debug, Clone)]
struct ArbTradeSignalPair {
    buy_pool: String,
    sell_pool: String,
    #[allow(dead_code)]
    seen_at_unix_ms: u64,
}

#[derive(Debug, Default)]
struct ArbTrackSelectionIngress {
    dirty_mints: parking_lot::Mutex<HashSet<String>>,
    dirty_overflow: AtomicBool,
    wake_pending: AtomicBool,
}

struct ArbTrackSelectionHandle {
    ingress: ArbTrackSelectionIngress,
    wake_tx: mpsc::Sender<()>,
    pending_full_reconcile: Arc<AtomicBool>,
}

impl ArbTrackSelectionHandle {
    /// Hot path: bounded dedup ingress. Never allocates a queue slot per Geyser update.
    fn mark_dirty(&self, mint: &str) {
        let schedule_wake = {
            let mut dirty = self.ingress.dirty_mints.lock();
            if dirty.contains(mint) {
                return;
            }
            if dirty.len() >= ARB_TRACK_SELECTION_INGRESS_DIRTY_CAP {
                self.ingress.dirty_overflow.store(true, Ordering::Release);
                self.pending_full_reconcile.store(true, Ordering::Release);
                true
            } else {
                dirty.insert(mint.to_string());
                true
            }
        };
        if schedule_wake {
            self.schedule_worker_wake();
        }
    }

    fn request_full_reconcile(&self) {
        self.pending_full_reconcile.store(true, Ordering::Release);
        self.schedule_worker_wake();
    }

    fn record_blocking_join_failed(&self) {
        record_arb_track_selection_blocking_join_failed_total();
        self.pending_full_reconcile.store(true, Ordering::Release);
        self.schedule_worker_wake();
    }

    fn take_pending_full(&self) -> bool {
        self.pending_full_reconcile.swap(false, Ordering::AcqRel)
    }

    /// At most one wake token in flight; additional marks coalesce in the ingress set.
    fn schedule_worker_wake(&self) {
        if self
            .ingress
            .wake_pending
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        if self.wake_tx.try_send(()).is_err() {
            // Channel full: a wake token is already queued (capacity 1). Arm full
            // reconcile so the in-flight worker pass performs authoritative recovery.
            record_arb_track_selection_queue_overflow_total();
            self.pending_full_reconcile.store(true, Ordering::Release);
        }
    }

    fn clear_wake_pending(&self) {
        self.ingress.wake_pending.store(false, Ordering::Release);
    }

    fn drain_ingress_dirty(&self) -> (Vec<String>, bool) {
        let mut dirty = self.ingress.dirty_mints.lock();
        let overflow = self.ingress.dirty_overflow.swap(false, Ordering::AcqRel);
        let mints: Vec<String> = dirty.drain().collect();
        (mints, overflow)
    }

    fn drain_ingress_to_coalescer(&self, coalescer: &mut ArbTrackSelectionCoalescer) {
        let (mints, overflow) = self.drain_ingress_dirty();
        for mint in mints {
            coalescer.ingest_dirty(mint);
        }
        if overflow {
            coalescer.note_dirty_overflow();
        }
    }

    fn ingress_has_work(&self) -> bool {
        !self.ingress.dirty_mints.lock().is_empty()
            || self.ingress.dirty_overflow.load(Ordering::Acquire)
            || self.pending_full_reconcile.load(Ordering::Acquire)
    }

    #[cfg(test)]
    fn ingress_dirty_len(&self) -> usize {
        self.ingress.dirty_mints.lock().len()
    }
}

#[cfg(test)]
fn dummy_arb_track_selection_handle() -> ArbTrackSelectionHandle {
    let (wake_tx, _wake_rx) = mpsc::channel::<()>(ARB_TRACK_SELECTION_WAKE_QUEUE_CAP);
    ArbTrackSelectionHandle {
        ingress: ArbTrackSelectionIngress::default(),
        wake_tx,
        pending_full_reconcile: Arc::new(AtomicBool::new(false)),
    }
}

#[cfg(test)]
fn test_arb_track_selection_handle() -> (ArbTrackSelectionHandle, mpsc::Receiver<()>) {
    let (wake_tx, wake_rx) = mpsc::channel::<()>(ARB_TRACK_SELECTION_WAKE_QUEUE_CAP);
    let handle = ArbTrackSelectionHandle {
        ingress: ArbTrackSelectionIngress::default(),
        wake_tx,
        pending_full_reconcile: Arc::new(AtomicBool::new(false)),
    };
    (handle, wake_rx)
}

/// Deterministic top-K mint admission for bounded snapshot cache (unit-tested).
fn compute_snapshot_admit_set(
    ranked_mints: &[String],
    protected: &HashSet<String>,
    cap: usize,
) -> HashSet<String> {
    let mut admit = HashSet::new();
    if cap == 0 {
        return admit;
    }
    for mint in ranked_mints {
        if protected.contains(mint) {
            admit.insert(mint.clone());
            if admit.len() >= cap {
                return admit;
            }
        }
    }
    let ranked_set: HashSet<&str> = ranked_mints.iter().map(String::as_str).collect();
    let mut extra_protected: Vec<String> = protected
        .iter()
        .filter(|mint| !ranked_set.contains(mint.as_str()))
        .cloned()
        .collect();
    extra_protected.sort();
    for mint in extra_protected {
        admit.insert(mint);
        if admit.len() >= cap {
            return admit;
        }
    }
    for mint in ranked_mints {
        if admit.len() >= cap {
            break;
        }
        admit.insert(mint.clone());
    }
    admit
}

#[derive(Debug, Clone)]
struct MintSnapshotEntry {
    input: TrackMintInput,
    access_gen: u64,
}

/// Bounded mint snapshot cache with lazy min-heap eviction (O(log cap) touch).
#[derive(Debug, Default)]
struct ArbTrackMintSnapshotCache {
    entries: HashMap<String, MintSnapshotEntry>,
    next_gen: u64,
    eviction_heap: BinaryHeap<Reverse<(u64, String)>>,
}

impl ArbTrackMintSnapshotCache {
    const HEAP_COMPACT_FACTOR: usize = 4;
    const HEAP_COMPACT_MIN_EXTRA: usize = 64;

    #[allow(dead_code)] // used by unit tests (clippy does not count cfg(test) callers)
    fn len(&self) -> usize {
        self.entries.len()
    }

    #[cfg(test)]
    fn heap_len(&self) -> usize {
        self.eviction_heap.len()
    }

    fn values(&self) -> impl Iterator<Item = &TrackMintInput> {
        self.entries.values().map(|entry| &entry.input)
    }

    fn remove(&mut self, mint: &str) {
        self.entries.remove(mint);
    }

    fn retain(&mut self, mut keep: impl FnMut(&String) -> bool) {
        self.entries.retain(|mint, _| keep(mint));
        self.compact_heap_if_needed(true);
    }

    fn compact_heap_if_needed(&mut self, force: bool) {
        let threshold = self
            .entries
            .len()
            .saturating_mul(Self::HEAP_COMPACT_FACTOR)
            .max(Self::HEAP_COMPACT_MIN_EXTRA);
        if force || self.eviction_heap.len() > threshold {
            self.eviction_heap = self
                .entries
                .iter()
                .map(|(mint, entry)| Reverse((entry.access_gen, mint.clone())))
                .collect();
        }
    }

    fn touch_entry(&mut self, mint: &str) {
        self.next_gen = self.next_gen.saturating_add(1);
        let gen = self.next_gen;
        if let Some(entry) = self.entries.get_mut(mint) {
            entry.access_gen = gen;
            self.eviction_heap.push(Reverse((gen, mint.to_string())));
        }
    }

    fn pop_eviction_victim(
        &mut self,
        protected: &HashSet<String>,
        unprotected_only: bool,
    ) -> Option<String> {
        let mut deferred = Vec::new();
        let victim = loop {
            let Some(Reverse((heap_gen, mint))) = self.eviction_heap.pop() else {
                break None;
            };
            let Some(entry) = self.entries.get(&mint) else {
                continue;
            };
            if entry.access_gen != heap_gen {
                continue;
            }
            if unprotected_only && protected.contains(&mint) {
                deferred.push(Reverse((heap_gen, mint)));
                continue;
            }
            break Some(mint);
        };
        for item in deferred {
            self.eviction_heap.push(item);
        }
        victim
    }

    fn evict_one(&mut self, protected: &HashSet<String>) -> bool {
        if let Some(victim) = self.pop_eviction_victim(protected, true) {
            self.entries.remove(&victim);
            return true;
        }
        if let Some(victim) = self.pop_eviction_victim(protected, false) {
            self.entries.remove(&victim);
            return true;
        }
        false
    }

    /// Hard cap: protection only affects eviction preference, never bypasses capacity.
    fn insert_bounded(
        &mut self,
        mint: String,
        input: TrackMintInput,
        protected: &HashSet<String>,
    ) -> bool {
        if self.entries.contains_key(&mint) {
            self.entries.get_mut(&mint).unwrap().input = input;
            self.touch_entry(&mint);
            self.compact_heap_if_needed(false);
            return true;
        }

        while self.entries.len() >= ARB_TRACK_MINT_SNAPSHOTS_CAP {
            if !self.evict_one(protected) {
                return false;
            }
        }

        self.next_gen = self.next_gen.saturating_add(1);
        let gen = self.next_gen;
        self.entries.insert(
            mint.clone(),
            MintSnapshotEntry {
                input,
                access_gen: gen,
            },
        );
        self.eviction_heap.push(Reverse((gen, mint)));
        self.compact_heap_if_needed(false);
        true
    }

    #[cfg(test)]
    fn test_access_generations_in_order(&self, mints: &[String]) -> Vec<u64> {
        mints
            .iter()
            .filter_map(|mint| self.entries.get(mint).map(|entry| entry.access_gen))
            .collect()
    }

    #[cfg(test)]
    fn test_eviction_victim(&mut self, protected: &HashSet<String>) -> Option<String> {
        self.pop_eviction_victim(protected, true)
            .or_else(|| self.pop_eviction_victim(protected, false))
    }
}

/// Coalesces selection jobs for bounded worker scheduling (unit-tested).
#[derive(Debug, Default)]
struct ArbTrackSelectionCoalescer {
    dirty_mints: HashSet<String>,
    dirty_overflow: bool,
}

impl ArbTrackSelectionCoalescer {
    /// Returns `true` when dirty overflow requires a full reconcile recovery.
    ///
    /// Lock-free bounded state: `dirty_mints` never exceeds
    /// `ARB_TRACK_SELECTION_DIRTY_MINTS_CAP`. Overflow mints are not retained; full
    /// reconcile scans current tracker truth and will include eligible mints.
    fn ingest_dirty(&mut self, mint: String) -> bool {
        if self.dirty_mints.contains(&mint) {
            return false;
        }
        if self.dirty_mints.len() >= ARB_TRACK_SELECTION_DIRTY_MINTS_CAP {
            self.dirty_overflow = true;
            return true;
        }
        self.dirty_mints.insert(mint);
        false
    }

    fn note_dirty_overflow(&mut self) {
        self.dirty_overflow = true;
    }

    fn take_batch(&mut self) -> (Vec<String>, bool) {
        let overflow = self.dirty_overflow;
        self.dirty_overflow = false;
        let dirty = self.dirty_mints.drain().collect();
        (dirty, overflow)
    }
}

fn pool_activity_unix_ms(pool: &PoolState, vault: Option<&VaultBalanceCache>) -> u64 {
    let pool_ms = pool_last_update_activity_unix_ms(pool);
    vault
        .map(|v| wall_clock_unix_ms_now().saturating_sub(v.updated_at.elapsed().as_millis() as u64))
        .unwrap_or(pool_ms)
        .max(pool_ms)
}

/// Tracker-only coarse recency for top-K admission ranking (no vault lock).
fn pool_last_update_activity_unix_ms(pool: &PoolState) -> u64 {
    wall_clock_unix_ms_now().saturating_sub(pool.last_update.elapsed().as_millis() as u64)
}

fn tracker_coarse_activity_unix_ms(tracker: &TokenArbTracker) -> u64 {
    tracker
        .pools
        .values()
        .map(pool_last_update_activity_unix_ms)
        .max()
        .unwrap_or_else(wall_clock_unix_ms_now)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MintCoarseRankSnapshot {
    mint: String,
    activity_unix_ms: u64,
}

/// Clone minimal per-mint rank data while `trackers` read guard is held.
fn build_multi_dex_coarse_rank_snapshot(
    trackers: &HashMap<String, TokenArbTracker>,
    mints: &[String],
) -> Vec<MintCoarseRankSnapshot> {
    mints
        .iter()
        .filter_map(|mint| {
            let tracker = trackers.get(mint)?;
            if tracker.pool_count_on_distinct_dexes() < 2 {
                return None;
            }
            Some(MintCoarseRankSnapshot {
                mint: mint.clone(),
                activity_unix_ms: tracker_coarse_activity_unix_ms(tracker),
            })
        })
        .collect()
}

fn rank_coarse_rank_snapshot(snapshot: &mut [MintCoarseRankSnapshot]) -> Vec<String> {
    snapshot.sort_by(|a, b| {
        b.activity_unix_ms
            .cmp(&a.activity_unix_ms)
            .then_with(|| a.mint.cmp(&b.mint))
    });
    snapshot.iter().map(|row| row.mint.clone()).collect()
}

/// Deterministic full-admit refresh order: ranked admitted mints first, then
/// admitted-but-unranked mints in stable sorted order (HashSet membership only).
fn admit_refresh_order(ranked: &[String], admit: &HashSet<String>) -> Vec<String> {
    let ranked_set: HashSet<&str> = ranked.iter().map(String::as_str).collect();
    let mut order: Vec<String> = ranked
        .iter()
        .filter(|mint| admit.contains(*mint))
        .cloned()
        .collect();
    let mut unranked_admitted: Vec<String> = admit
        .iter()
        .filter(|mint| !ranked_set.contains(mint.as_str()))
        .cloned()
        .collect();
    unranked_admitted.sort();
    order.extend(unranked_admitted);
    order
}

fn tracker_mint_activity_unix_ms(
    tracker: &TokenArbTracker,
    vault_balances: &HashMap<String, VaultBalanceCache>,
) -> u64 {
    tracker
        .pools
        .values()
        .map(|pool| pool_activity_unix_ms(pool, vault_balances.get(&pool.pool_address)))
        .max()
        .unwrap_or_else(wall_clock_unix_ms_now)
}

/// Reject obviously wrong side/decimal comparable prices (stablecoins only).
fn is_plausible_sol_per_token_price(mint: &str, price: Decimal) -> bool {
    if price <= Decimal::ZERO {
        return false;
    }
    if is_stablecoin_mint(mint) {
        let min = Decimal::from_str(STABLECOIN_MIN_SOL_PER_TOKEN).unwrap_or(Decimal::ZERO);
        let max = Decimal::from_str(STABLECOIN_MAX_SOL_PER_TOKEN).unwrap_or(Decimal::ONE);
        price >= min && price <= max
    } else {
        true
    }
}

fn reserves_plausible_for_comparable_price(
    reserve_base: u64,
    reserve_quote: u64,
    token_decimals: u8,
    token_mint: &str,
) -> bool {
    reserve_base > 0
        && reserve_quote > 0
        && reserve_mid_sol_per_token(reserve_base, reserve_quote, token_decimals)
            .is_some_and(|mid| is_plausible_sol_per_token_price(token_mint, mid))
}

/// Map on-chain base/quote reserves to token-base + SOL-quote for comparable pricing.
fn sol_quoted_vault_reserves(
    base_mint: &str,
    quote_mint: &str,
    reserve_base: u64,
    reserve_quote: u64,
) -> (u64, u64) {
    if quote_mint == NATIVE_SOL_MINT {
        (reserve_base, reserve_quote)
    } else if base_mint == NATIVE_SOL_MINT {
        (reserve_quote, reserve_base)
    } else {
        (reserve_base, reserve_quote)
    }
}

/// Explicit Orca WSOL-side mapping: vault_a/vault_b → (token_reserve, sol_reserve).
fn orca_sol_quoted_vault_reserves(
    token_mint_a: &str,
    token_mint_b: &str,
    vault_a_balance: u64,
    vault_b_balance: u64,
) -> Option<(u64, u64)> {
    if token_mint_a == NATIVE_SOL_MINT {
        Some((vault_b_balance, vault_a_balance))
    } else if token_mint_b == NATIVE_SOL_MINT {
        Some((vault_a_balance, vault_b_balance))
    } else {
        None
    }
}

fn flatten_bin_array_cache(arrays: &HashMap<i64, BinArrayCache>) -> HashMap<i64, Vec<BinData>> {
    arrays
        .iter()
        .map(|(idx, cache)| (*idx, cache.bins.clone()))
        .collect()
}

/// True when trade-implied price or Geyser reserve data is within max_age.
fn is_pool_price_fresh(
    pool: &PoolState,
    vault: Option<&VaultBalanceCache>,
    max_age: Duration,
) -> bool {
    if pool.last_update.elapsed() <= max_age {
        return true;
    }
    if let Some(v) = vault {
        if pool.dex == "meteora_dlmm"
            && v.active_id.is_some()
            && v.bin_step.is_some()
            && v.updated_at.elapsed() <= max_age
        {
            return true;
        }
        if pool.has_reserve_data
            && v.reserve_base > 0
            && v.reserve_quote > 0
            && v.updated_at.elapsed() <= max_age
        {
            return true;
        }
    }
    false
}

/// Age and dominant freshness source for stale-price metrics at 2-hop reject.
fn stale_price_age_for_metrics(
    pool: &PoolState,
    vault: Option<&VaultBalanceCache>,
) -> (u64, &'static str) {
    let trade_age = pool.last_update.elapsed().as_millis() as u64;
    if let Some(v) = vault {
        if pool.dex == "meteora_dlmm" && v.active_id.is_some() && v.bin_step.is_some() {
            let meta_age = v.updated_at.elapsed().as_millis() as u64;
            if meta_age >= trade_age {
                return (meta_age, "dlmm_meta");
            }
        }
        if pool.has_reserve_data && v.reserve_base > 0 && v.reserve_quote > 0 {
            let vault_age = v.updated_at.elapsed().as_millis() as u64;
            if vault_age >= trade_age {
                return (vault_age, "vault");
            }
        }
    }
    (trade_age, "trade")
}

fn record_stale_price_freshness_metrics(pool: &PoolState, vault: Option<&VaultBalanceCache>) {
    let (age_ms, source) = stale_price_age_for_metrics(pool, vault);
    record_arb_price_freshness_stale_age_ms(&pool.dex, source, age_ms);
}

/// Comparable SOL/token for spread: DLMM marginal (probe) > reserve mid > trade mid.
fn comparable_price_sol_per_token(
    pool: &PoolState,
    vault_reserves: Option<(u64, u64)>,
    token_decimals: Option<u8>,
    token_mint: &str,
    vault_cache: Option<&VaultBalanceCache>,
    dlmm_bin_arrays: Option<&HashMap<i64, BinArrayCache>>,
    side: ComparablePriceSide,
) -> Option<Decimal> {
    let token_decimals = token_decimals?;

    if pool.dex == "meteora_dlmm" {
        if let (Some(vault), Some(arrays)) = (vault_cache, dlmm_bin_arrays) {
            if let (Some(active_id), Some(bin_step)) = (vault.active_id, vault.bin_step) {
                let flat = flatten_bin_array_cache(arrays);
                let sol_is_x = vault_dlmm_sol_is_x(vault);
                let reserve_mid = vault_reserves.and_then(|(base, quote)| {
                    if reserves_plausible_for_comparable_price(
                        base,
                        quote,
                        token_decimals,
                        token_mint,
                    ) {
                        reserve_mid_sol_per_token(base, quote, token_decimals)
                    } else {
                        None
                    }
                });
                let trade_mid = trade_mid_sol_per_token(pool)
                    .filter(|p| is_plausible_sol_per_token_price(token_mint, *p));
                let marginal = match side {
                    ComparablePriceSide::Buy => dlmm_token_output_from_bins(
                        active_id,
                        bin_step,
                        DLMM_PROBE_SOL_LAMPORTS,
                        &flat,
                        sol_is_x,
                    )
                    .filter(|tokens_out| *tokens_out > 0)
                    .map(|tokens_out| {
                        trade_implied_sol_per_token(
                            DLMM_PROBE_SOL_LAMPORTS,
                            tokens_out,
                            token_decimals,
                        )
                    }),
                    ComparablePriceSide::Sell => dlmm_token_output_from_bins(
                        active_id,
                        bin_step,
                        DLMM_PROBE_SOL_LAMPORTS,
                        &flat,
                        sol_is_x,
                    )
                    .filter(|token_probe| *token_probe > 0)
                    .and_then(|token_probe| {
                        dlmm_sol_output_from_bins(active_id, bin_step, token_probe, &flat, sol_is_x)
                            .filter(|sol_out| *sol_out > 0)
                            .map(|sol_out| {
                                trade_implied_sol_per_token(sol_out, token_probe, token_decimals)
                            })
                    }),
                };
                if let Some(price) = marginal.filter(|p| *p > Decimal::ZERO) {
                    if dlmm_marginal_price_plausible(price, reserve_mid, trade_mid)
                        && is_plausible_sol_per_token_price(token_mint, price)
                    {
                        return Some(price);
                    }
                    DLMM_MARGINAL_PRICE_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    if let Some((reserve_base, reserve_quote)) = vault_reserves {
        if reserves_plausible_for_comparable_price(
            reserve_base,
            reserve_quote,
            token_decimals,
            token_mint,
        ) {
            if let Some(mid) =
                reserve_mid_sol_per_token(reserve_base, reserve_quote, token_decimals)
            {
                return Some(mid);
            }
        }
    }
    match (pool.trade_price_buy, pool.trade_price_sell) {
        (Some(buy), Some(sell)) if buy > Decimal::ZERO && sell > Decimal::ZERO => {
            let mid = (buy + sell) / Decimal::from(2);
            if is_plausible_sol_per_token_price(token_mint, mid) {
                Some(mid)
            } else {
                None
            }
        }
        (Some(one), None) | (None, Some(one)) if one > Decimal::ZERO => {
            if is_plausible_sol_per_token_price(token_mint, one) {
                Some(one)
            } else {
                None
            }
        }
        _ => None,
    }
}

#[cfg(test)]
thread_local! {
    static COMPARABLE_PRICE_CALLS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn reset_comparable_price_call_count() {
    COMPARABLE_PRICE_CALLS.with(|c| c.set(0));
}

#[cfg(test)]
fn comparable_price_call_count() -> u64 {
    COMPARABLE_PRICE_CALLS.with(|c| c.get())
}

/// Single eligibility-path entry for comparable price (counted in tests).
fn comparable_price_for_eligibility(
    pool: &PoolState,
    vault_reserves: Option<(u64, u64)>,
    token_decimals: Option<u8>,
    token_mint: &str,
    vault_cache: Option<&VaultBalanceCache>,
    dlmm_bin_arrays: Option<&HashMap<i64, BinArrayCache>>,
    side: ComparablePriceSide,
) -> Option<Decimal> {
    #[cfg(test)]
    COMPARABLE_PRICE_CALLS.with(|c| c.set(c.get().saturating_add(1)));
    comparable_price_sol_per_token(
        pool,
        vault_reserves,
        token_decimals,
        token_mint,
        vault_cache,
        dlmm_bin_arrays,
        side,
    )
}

/// SOL-quoted pool seed: (token_mint, reserve_base, reserve_quote_sol, active_id, bin_step).
type SolQuotedPoolSeed = (String, u64, u64, Option<i32>, Option<u16>);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArbWarmupQuoteKind {
    Sol,
    Stablecoin,
}

#[derive(Debug, Clone)]
struct ArbWarmupSeed {
    token_mint: String,
    reserve_base: u64,
    reserve_quote: u64,
    active_id: Option<i32>,
    bin_step: Option<u16>,
    quote_kind: ArbWarmupQuoteKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SeedPoolOutcome {
    SeededNew,
    UpdatedExisting,
    Skipped(ArbStrategyWarmupSkipReason),
}

#[derive(Debug, Default, Clone)]
struct ArbWarmupBootstrapStats {
    tracker_seeded_pools: usize,
    tracker_seed_candidates: usize,
}

fn pool_state_mints(state: &CachedPoolState) -> (String, String) {
    match state {
        CachedPoolState::Orca(s) => (s.token_mint_a.to_string(), s.token_mint_b.to_string()),
        CachedPoolState::RaydiumAmm(s) => (s.base_mint.to_string(), s.quote_mint.to_string()),
        CachedPoolState::RaydiumCpmm(s) => (s.token_0_mint.to_string(), s.token_1_mint.to_string()),
        CachedPoolState::Meteora(s) => (s.token_x_mint.to_string(), s.token_y_mint.to_string()),
        CachedPoolState::MeteoraCpmm(s) => (s.token_0_mint.to_string(), s.token_1_mint.to_string()),
        CachedPoolState::PumpFun(s) => (s.token_mint.to_string(), NATIVE_SOL_MINT.to_string()),
        CachedPoolState::PumpAmm(s) => (s.base_mint.to_string(), s.quote_mint.to_string()),
    }
}

fn pool_state_has_arb_relevant_quote(state: &CachedPoolState) -> bool {
    let (mint_a, mint_b) = pool_state_mints(state);
    is_arb_relevant_pool_pair(&mint_a, &mint_b)
}

fn pool_state_has_any_reserves(state: &CachedPoolState) -> bool {
    match state {
        CachedPoolState::Orca(s) => s.vault_a_balance.is_some() || s.vault_b_balance.is_some(),
        CachedPoolState::RaydiumAmm(s) => s.coin_reserve.is_some() || s.pc_reserve.is_some(),
        CachedPoolState::RaydiumCpmm(s) => s.reserve_0.is_some() || s.reserve_1.is_some(),
        CachedPoolState::Meteora(s) => {
            s.reserve_x_balance.is_some() || s.reserve_y_balance.is_some()
        }
        CachedPoolState::MeteoraCpmm(_) => true,
        CachedPoolState::PumpAmm(s) => s.base_reserve.is_some() || s.quote_reserve.is_some(),
        CachedPoolState::PumpFun(s) => s.virtual_token_reserves > 0 || s.virtual_sol_reserves > 0,
    }
}

fn classify_warmup_skip(state: &CachedPoolState) -> ArbStrategyWarmupSkipReason {
    if !is_known_dex_label(state.dex_name()) {
        return ArbStrategyWarmupSkipReason::UnknownDex;
    }
    if !pool_state_has_arb_relevant_quote(state) {
        return ArbStrategyWarmupSkipReason::NonArbQuote;
    }
    if !pool_state_has_any_reserves(state) {
        return ArbStrategyWarmupSkipReason::MissingReserves;
    }
    ArbStrategyWarmupSkipReason::ZeroReserves
}

fn orca_quote_vault_reserves(
    token_mint_a: &str,
    token_mint_b: &str,
    vault_a_balance: u64,
    vault_b_balance: u64,
    quote_mint: &str,
) -> Option<(String, u64, u64)> {
    if token_mint_a == quote_mint {
        Some((token_mint_b.to_string(), vault_b_balance, vault_a_balance))
    } else if token_mint_b == quote_mint {
        Some((token_mint_a.to_string(), vault_a_balance, vault_b_balance))
    } else {
        None
    }
}

fn stablecoin_quoted_pool_seed(state: &CachedPoolState) -> Option<SolQuotedPoolSeed> {
    for quote_mint in [USDC_MINT, USDT_MINT] {
        if let Some(seed) = common_quote_pool_seed(state, quote_mint) {
            return Some(seed);
        }
    }
    None
}

fn common_quote_pool_seed(state: &CachedPoolState, quote_mint: &str) -> Option<SolQuotedPoolSeed> {
    match state {
        CachedPoolState::Orca(s) => {
            let mint_a = s.token_mint_a.to_string();
            let mint_b = s.token_mint_b.to_string();
            let va = s.vault_a_balance?;
            let vb = s.vault_b_balance?;
            let (token_mint, reserve_base, reserve_quote) =
                orca_quote_vault_reserves(&mint_a, &mint_b, va, vb, quote_mint)?;
            Some((token_mint, reserve_base, reserve_quote, None, None))
        }
        CachedPoolState::RaydiumAmm(s) => {
            let base = s.base_mint.to_string();
            let quote = s.quote_mint.to_string();
            let cr = s.coin_reserve?;
            let pr = s.pc_reserve?;
            if quote == quote_mint {
                Some((base, cr, pr, None, None))
            } else if base == quote_mint {
                Some((quote, pr, cr, None, None))
            } else {
                None
            }
        }
        CachedPoolState::RaydiumCpmm(s) => {
            let t0 = s.token_0_mint.to_string();
            let t1 = s.token_1_mint.to_string();
            let r0 = s.reserve_0?;
            let r1 = s.reserve_1?;
            if t1 == quote_mint {
                Some((t0, r0, r1, None, None))
            } else if t0 == quote_mint {
                Some((t1, r1, r0, None, None))
            } else {
                None
            }
        }
        CachedPoolState::Meteora(s) => {
            let x = s.token_x_mint.to_string();
            let y = s.token_y_mint.to_string();
            let rx = s.reserve_x_balance?;
            let ry = s.reserve_y_balance?;
            if y == quote_mint {
                Some((x, rx, ry, Some(s.active_id), Some(s.bin_step)))
            } else if x == quote_mint {
                Some((y, ry, rx, Some(s.active_id), Some(s.bin_step)))
            } else {
                None
            }
        }
        CachedPoolState::MeteoraCpmm(s) => {
            let t0 = s.token_0_mint.to_string();
            let t1 = s.token_1_mint.to_string();
            if t1 == quote_mint {
                Some((t0, s.reserve_0, s.reserve_1, None, None))
            } else if t0 == quote_mint {
                Some((t1, s.reserve_1, s.reserve_0, None, None))
            } else {
                None
            }
        }
        CachedPoolState::PumpAmm(s) => {
            let base = s.base_mint.to_string();
            let quote = s.quote_mint.to_string();
            let br = s.base_reserve?;
            let qr = s.quote_reserve?;
            if quote == quote_mint {
                Some((base, br, qr, None, None))
            } else if base == quote_mint {
                Some((quote, qr, br, None, None))
            } else {
                None
            }
        }
        CachedPoolState::PumpFun(_) => None,
    }
}

fn arb_warmup_pool_seed(state: &CachedPoolState) -> Option<ArbWarmupSeed> {
    if let Some((token_mint, reserve_base, reserve_quote, active_id, bin_step)) =
        sol_quoted_pool_seed(state)
    {
        return Some(ArbWarmupSeed {
            token_mint,
            reserve_base,
            reserve_quote,
            active_id,
            bin_step,
            quote_kind: ArbWarmupQuoteKind::Sol,
        });
    }
    let (token_mint, reserve_base, reserve_quote, active_id, bin_step) =
        stablecoin_quoted_pool_seed(state)?;
    Some(ArbWarmupSeed {
        token_mint,
        reserve_base,
        reserve_quote,
        active_id,
        bin_step,
        quote_kind: ArbWarmupQuoteKind::Stablecoin,
    })
}

fn token_decimals_from_pool_state(state: &CachedPoolState, token_mint: &str) -> Option<u8> {
    match state {
        CachedPoolState::RaydiumAmm(s) => {
            let base = s.base_mint.to_string();
            let quote = s.quote_mint.to_string();
            if token_mint == base && s.base_decimals > 0 {
                Some(s.base_decimals)
            } else if token_mint == quote && s.quote_decimals > 0 {
                Some(s.quote_decimals)
            } else {
                None
            }
        }
        CachedPoolState::MeteoraCpmm(s) => {
            let t0 = s.token_0_mint.to_string();
            let t1 = s.token_1_mint.to_string();
            if token_mint == t0 && s.mint_0_decimals > 0 {
                Some(s.mint_0_decimals)
            } else if token_mint == t1 && s.mint_1_decimals > 0 {
                Some(s.mint_1_decimals)
            } else {
                None
            }
        }
        _ => None,
    }
}

fn apply_warmup_token_decimals(
    tracker: &mut TokenArbTracker,
    live_pool_cache: &LivePoolCache,
    state: &CachedPoolState,
    token_mint: &str,
) {
    if tracker.token_decimals.is_some() {
        return;
    }
    if let Ok(pk) = Pubkey::from_str(token_mint) {
        if let Some(d) = live_pool_cache.get_mint_decimals(&pk) {
            tracker.token_decimals = Some(d);
            return;
        }
    }
    if let Some(d) = token_decimals_from_pool_state(state, token_mint) {
        tracker.token_decimals = Some(d);
    }
}

/// Extract SOL-quoted token reserves from SLAVE CachedPoolState (base=token, quote=SOL).
fn sol_quoted_pool_seed(state: &CachedPoolState) -> Option<SolQuotedPoolSeed> {
    match state {
        CachedPoolState::Orca(s) => {
            let mint_a = s.token_mint_a.to_string();
            let mint_b = s.token_mint_b.to_string();
            let va = s.vault_a_balance?;
            let vb = s.vault_b_balance?;
            let (reserve_base, reserve_quote) =
                orca_sol_quoted_vault_reserves(&mint_a, &mint_b, va, vb)?;
            let token_mint = if mint_a == NATIVE_SOL_MINT {
                mint_b
            } else {
                mint_a
            };
            Some((token_mint, reserve_base, reserve_quote, None, None))
        }
        CachedPoolState::RaydiumAmm(s) => {
            let base = s.base_mint.to_string();
            let quote = s.quote_mint.to_string();
            let cr = s.coin_reserve?;
            let pr = s.pc_reserve?;
            if quote == NATIVE_SOL_MINT {
                Some((base, cr, pr, None, None))
            } else if base == NATIVE_SOL_MINT {
                Some((quote, pr, cr, None, None))
            } else {
                None
            }
        }
        CachedPoolState::RaydiumCpmm(s) => {
            let t0 = s.token_0_mint.to_string();
            let t1 = s.token_1_mint.to_string();
            let r0 = s.reserve_0?;
            let r1 = s.reserve_1?;
            if t1 == NATIVE_SOL_MINT {
                Some((t0, r0, r1, None, None))
            } else if t0 == NATIVE_SOL_MINT {
                Some((t1, r1, r0, None, None))
            } else {
                None
            }
        }
        CachedPoolState::Meteora(s) => {
            let x = s.token_x_mint.to_string();
            let y = s.token_y_mint.to_string();
            let rx = s.reserve_x_balance?;
            let ry = s.reserve_y_balance?;
            if y == NATIVE_SOL_MINT {
                Some((x, rx, ry, Some(s.active_id), Some(s.bin_step)))
            } else if x == NATIVE_SOL_MINT {
                Some((y, ry, rx, Some(s.active_id), Some(s.bin_step)))
            } else {
                None
            }
        }
        CachedPoolState::MeteoraCpmm(s) => {
            let t0 = s.token_0_mint.to_string();
            let t1 = s.token_1_mint.to_string();
            if t1 == NATIVE_SOL_MINT {
                Some((t0, s.reserve_0, s.reserve_1, None, None))
            } else if t0 == NATIVE_SOL_MINT {
                Some((t1, s.reserve_1, s.reserve_0, None, None))
            } else {
                None
            }
        }
        CachedPoolState::PumpAmm(s) => {
            let base = s.base_mint.to_string();
            let quote = s.quote_mint.to_string();
            let br = s.base_reserve?;
            let qr = s.quote_reserve?;
            if quote == NATIVE_SOL_MINT {
                Some((base, br, qr, None, None))
            } else if base == NATIVE_SOL_MINT {
                Some((quote, qr, br, None, None))
            } else {
                None
            }
        }
        CachedPoolState::PumpFun(s) => {
            let mint = s.token_mint.to_string();
            if mint == NATIVE_SOL_MINT {
                return None;
            }
            let token_r = s.virtual_token_reserves;
            let sol_r = s.virtual_sol_reserves;
            if token_r > 0 && sol_r > 0 {
                Some((mint, token_r, sol_r, None, None))
            } else {
                None
            }
        }
    }
}

/// Pair mints for DexPoolAccounts dual-tracker storage from SLAVE cache state.
fn pool_pair_mints_from_cached_state(state: &CachedPoolState) -> Option<(String, String)> {
    match state {
        CachedPoolState::Meteora(s) => {
            Some((s.token_x_mint.to_string(), s.token_y_mint.to_string()))
        }
        CachedPoolState::Orca(s) => Some((s.token_mint_a.to_string(), s.token_mint_b.to_string())),
        CachedPoolState::PumpAmm(s) => Some((s.base_mint.to_string(), s.quote_mint.to_string())),
        CachedPoolState::RaydiumAmm(s) => Some((s.base_mint.to_string(), s.quote_mint.to_string())),
        CachedPoolState::RaydiumCpmm(s) => {
            Some((s.token_0_mint.to_string(), s.token_1_mint.to_string()))
        }
        CachedPoolState::MeteoraCpmm(s) => {
            Some((s.token_0_mint.to_string(), s.token_1_mint.to_string()))
        }
        CachedPoolState::PumpFun(_) => None,
    }
}

/// Build DexPoolAccounts vector from Geyser-sourced SLAVE cache (no RPC).
fn dex_pool_accounts_from_cached_state(
    pool_pk: &Pubkey,
    state: &CachedPoolState,
) -> Option<Vec<String>> {
    let pool_str = pool_pk.to_string();
    match state {
        CachedPoolState::Meteora(s) => {
            if s.reserve_x == Pubkey::default() || s.reserve_y == Pubkey::default() {
                return None;
            }
            Some(vec![
                pool_str,
                s.token_x_mint.to_string(),
                s.token_y_mint.to_string(),
                s.reserve_x.to_string(),
                s.reserve_y.to_string(),
                format!("active_id:{}", s.active_id),
                format!("bin_step:{}", s.bin_step),
            ])
        }
        CachedPoolState::Orca(s) => {
            if s.token_vault_a == Pubkey::default() || s.token_vault_b == Pubkey::default() {
                return None;
            }
            Some(vec![
                pool_str,
                s.token_mint_a.to_string(),
                s.token_mint_b.to_string(),
                s.token_vault_a.to_string(),
                s.token_vault_b.to_string(),
                format!("tick_current_index:{}", s.tick_current_index),
                format!("tick_spacing:{}", s.tick_spacing),
            ])
        }
        CachedPoolState::PumpAmm(s) => {
            if s.pool_accounts.len() < 14 {
                return None;
            }
            let accounts: Vec<String> = s.pool_accounts.iter().map(|p| p.to_string()).collect();
            if pump_amm_pool_accounts_valid_for_swap(&pool_str, &accounts) {
                Some(accounts)
            } else {
                None
            }
        }
        _ => None,
    }
}

fn maybe_backfill_tracker_pool_accounts_from_cache(
    pool_pk: Pubkey,
    pool_addr: &str,
    state: &CachedPoolState,
    tracker: &mut TokenArbTracker,
) {
    if tracker.get_pool_accounts(pool_addr).is_some() {
        return;
    }
    if let Some(accounts) = dex_pool_accounts_from_cached_state(&pool_pk, state) {
        tracker.set_pool_accounts(pool_addr, accounts);
        inc_arb_pool_accounts_backfill(ArbPoolAccountsBackfillSource::LiveCache);
    }
}

/// Upsert one pool into tracker + vault_balances from SLAVE cache.
fn seed_one_pool_from_live_cache(
    mint: &str,
    live_pool_cache: &LivePoolCache,
    pool_pk: Pubkey,
    state: &CachedPoolState,
    trackers: &mut HashMap<String, TokenArbTracker>,
    vault_balances: &mut HashMap<String, VaultBalanceCache>,
) -> SeedPoolOutcome {
    let dex = state.dex_name();
    if !is_known_dex_label(dex) {
        return SeedPoolOutcome::Skipped(ArbStrategyWarmupSkipReason::UnknownDex);
    }
    let Some(warmup) = arb_warmup_pool_seed(state) else {
        return SeedPoolOutcome::Skipped(classify_warmup_skip(state));
    };
    if warmup.token_mint != mint {
        return SeedPoolOutcome::Skipped(ArbStrategyWarmupSkipReason::NonArbQuote);
    }
    if warmup.token_mint == NATIVE_SOL_MINT || is_stablecoin_mint(&warmup.token_mint) {
        return SeedPoolOutcome::Skipped(ArbStrategyWarmupSkipReason::NativeTokenMint);
    }
    if warmup.reserve_base == 0 || warmup.reserve_quote == 0 {
        return SeedPoolOutcome::Skipped(ArbStrategyWarmupSkipReason::ZeroReserves);
    }

    let pool_addr = pool_pk.to_string();
    let (_, slot, age_ms) =
        live_pool_cache
            .get_with_metadata(&pool_pk)
            .unwrap_or((state.clone(), 0, 0));
    let cache_updated_at = Instant::now()
        .checked_sub(Duration::from_millis(age_ms))
        .unwrap_or_else(Instant::now);
    let dlmm_token_x_mint = match state {
        CachedPoolState::Meteora(s) => Some(s.token_x_mint.to_string()),
        _ => None,
    };
    let dlmm_sol_is_x = dlmm_token_x_mint.as_deref() == Some(NATIVE_SOL_MINT);

    let (should_replace_vault, should_touch_vault_updated_at) = match vault_balances.get(&pool_addr)
    {
        Some(existing) => (
            slot >= existing.update_slot,
            cache_updated_at > existing.updated_at,
        ),
        None => (true, false),
    };

    let tracker = trackers
        .entry(mint.to_string())
        .or_insert_with(|| TokenArbTracker::new(mint));
    apply_warmup_token_decimals(tracker, live_pool_cache, state, mint);

    let (trade_price_buy, trade_price_sell, trade_count, dex_accounts) = tracker
        .pools
        .get(&pool_addr)
        .map(|p| {
            (
                p.trade_price_buy,
                p.trade_price_sell,
                p.trade_count,
                p.dex_accounts.clone(),
            )
        })
        .unwrap_or((None, None, 0, None));

    let (
        _eff_reserve_base,
        _eff_reserve_quote,
        eff_updated_at,
        has_reserve_data,
        liquidity_sol,
        reserve_price,
        vault_for_comparable,
        vault_reserves_for_comparable,
    ) = match warmup.quote_kind {
        ArbWarmupQuoteKind::Sol => {
            if should_replace_vault {
                vault_balances.insert(
                    pool_addr.clone(),
                    VaultBalanceCache {
                        reserve_base: warmup.reserve_base,
                        reserve_quote: warmup.reserve_quote,
                        update_slot: slot,
                        active_id: warmup.active_id,
                        bin_step: warmup.bin_step,
                        updated_at: cache_updated_at,
                        dlmm_sol_is_x,
                        dlmm_token_x_mint,
                    },
                );
            } else if should_touch_vault_updated_at {
                if let Some(vault) = vault_balances.get_mut(&pool_addr) {
                    vault.updated_at = cache_updated_at;
                }
            }
            let vault_ref = vault_balances
                .get(&pool_addr)
                .expect("vault_balances entry must exist after SOL-quoted seed merge");
            let reserve_base = vault_ref.reserve_base;
            let reserve_quote = vault_ref.reserve_quote;
            let has_reserves = reserve_base > 0 && reserve_quote > 0;
            let reserve_price = tracker.token_decimals.and_then(|token_decimals| {
                if reserves_plausible_for_comparable_price(
                    reserve_base,
                    reserve_quote,
                    token_decimals,
                    mint,
                ) {
                    reserve_mid_sol_per_token(reserve_base, reserve_quote, token_decimals)
                } else {
                    None
                }
            });
            (
                reserve_base,
                reserve_quote,
                vault_ref.updated_at,
                has_reserves,
                Decimal::from(reserve_quote) / Decimal::from(1_000_000_000u64),
                reserve_price,
                vault_balances.get(&pool_addr),
                Some((reserve_base, reserve_quote)),
            )
        }
        ArbWarmupQuoteKind::Stablecoin => {
            // USDC/USDT quote reserves must not land in vault_balances: eligibility treats
            // reserve_quote as SOL lamports in reserve_mid_sol_per_token (I-15).
            (
                0,
                0,
                cache_updated_at,
                false,
                Decimal::ZERO,
                None,
                None,
                None,
            )
        }
    };

    let pool_last_update = match tracker.pools.get(&pool_addr) {
        Some(p) => p.last_update.max(eff_updated_at),
        None => eff_updated_at,
    };
    let seed_pool = PoolState {
        pool_address: pool_addr.clone(),
        dex: dex.to_string(),
        last_price: reserve_price,
        trade_price_buy,
        trade_price_sell,
        liquidity_sol,
        has_reserve_data,
        last_update: pool_last_update,
        trade_count,
        dex_accounts,
    };
    let dlmm_bins = None::<&HashMap<i64, BinArrayCache>>;
    let last_price = comparable_price_sol_per_token(
        &seed_pool,
        vault_reserves_for_comparable,
        tracker.token_decimals,
        mint,
        vault_for_comparable,
        dlmm_bins,
        ComparablePriceSide::Buy,
    )
    .or(reserve_price);

    let is_new_pool = !tracker.pools.contains_key(&pool_addr);
    tracker.upsert_pool(PoolState {
        pool_address: pool_addr.clone(),
        dex: dex.to_string(),
        last_price,
        trade_price_buy: seed_pool.trade_price_buy,
        trade_price_sell: seed_pool.trade_price_sell,
        liquidity_sol,
        has_reserve_data,
        last_update: pool_last_update,
        trade_count: seed_pool.trade_count,
        dex_accounts: seed_pool.dex_accounts,
    });
    maybe_backfill_tracker_pool_accounts_from_cache(pool_pk, &pool_addr, state, tracker);
    if is_new_pool {
        SeedPoolOutcome::SeededNew
    } else {
        SeedPoolOutcome::UpdatedExisting
    }
}

/// Seed TokenArbTracker pools for one mint from SLAVE LivePoolCache (Geyser-only, no RPC).
/// When `only_pool` is set, uses O(1) `get` (incremental JetStream); bootstrap uses full `iter`.
fn seed_token_tracker_from_live_pool_cache(
    mint: &str,
    live_pool_cache: &LivePoolCache,
    trackers: &mut HashMap<String, TokenArbTracker>,
    vault_balances: &mut HashMap<String, VaultBalanceCache>,
    only_pool: Option<&str>,
) -> usize {
    let mut seeded = 0usize;

    if let Some(pool_filter) = only_pool {
        let Ok(pool_pk) = Pubkey::from_str(pool_filter) else {
            return 0;
        };
        let Some((state, _, _)) = live_pool_cache.get_with_metadata(&pool_pk) else {
            return 0;
        };
        if matches!(
            seed_one_pool_from_live_cache(
                mint,
                live_pool_cache,
                pool_pk,
                &state,
                trackers,
                vault_balances,
            ),
            SeedPoolOutcome::SeededNew | SeedPoolOutcome::UpdatedExisting
        ) {
            seeded = 1;
        }
    } else {
        for (pool_pk, state) in live_pool_cache.iter() {
            if matches!(
                seed_one_pool_from_live_cache(
                    mint,
                    live_pool_cache,
                    pool_pk,
                    &state,
                    trackers,
                    vault_balances,
                ),
                SeedPoolOutcome::SeededNew | SeedPoolOutcome::UpdatedExisting
            ) {
                seeded += 1;
            }
        }
    }

    if seeded > 0 {
        arb_two_hop_tracker_seeded_pools_add(seeded as u64);
    }
    seeded
}

/// Seed all arb-relevant pools from SLAVE LivePoolCache (cold-start full scan).
fn seed_all_trackers_from_live_pool_cache(
    live_pool_cache: &LivePoolCache,
    trackers: &mut HashMap<String, TokenArbTracker>,
    vault_balances: &mut HashMap<String, VaultBalanceCache>,
) -> ArbWarmupBootstrapStats {
    let mut stats = ArbWarmupBootstrapStats::default();
    for (pool_pk, state) in live_pool_cache.iter() {
        if !is_known_dex_label(state.dex_name()) {
            arb_strategy_bootstrap_skip_inc(ArbStrategyWarmupSkipReason::UnknownDex);
            continue;
        }
        stats.tracker_seed_candidates += 1;
        let Some(warmup) = arb_warmup_pool_seed(&state) else {
            arb_strategy_bootstrap_skip_inc(classify_warmup_skip(&state));
            continue;
        };
        if warmup.token_mint == NATIVE_SOL_MINT || is_stablecoin_mint(&warmup.token_mint) {
            arb_strategy_bootstrap_skip_inc(ArbStrategyWarmupSkipReason::NativeTokenMint);
            continue;
        }
        if warmup.reserve_base == 0 || warmup.reserve_quote == 0 {
            arb_strategy_bootstrap_skip_inc(ArbStrategyWarmupSkipReason::ZeroReserves);
            continue;
        }
        match seed_one_pool_from_live_cache(
            &warmup.token_mint,
            live_pool_cache,
            pool_pk,
            &state,
            trackers,
            vault_balances,
        ) {
            SeedPoolOutcome::SeededNew | SeedPoolOutcome::UpdatedExisting => {
                stats.tracker_seeded_pools += 1;
            }
            SeedPoolOutcome::Skipped(reason) => arb_strategy_bootstrap_skip_inc(reason),
        }
    }
    if stats.tracker_seeded_pools > 0 {
        arb_two_hop_tracker_seeded_pools_add(stats.tracker_seeded_pools as u64);
    }
    stats
}

/// Tracks a pool's price/liquidity state
#[allow(dead_code)]
#[derive(Debug, Clone)]
struct PoolState {
    pool_address: String,
    dex: String,
    /// Last comparable SOL per token (from reserve mid or trade mid)
    last_price: Option<Decimal>,
    /// Last buy-side trade implied price (SOL per token)
    trade_price_buy: Option<Decimal>,
    /// Last sell-side trade implied price (SOL per token)
    trade_price_sell: Option<Decimal>,
    /// Liquidity in SOL (from PoolCreated or Geyser reserves)
    liquidity_sol: Decimal,
    /// True when `PoolStateUpdate` reserves were applied for this pool
    has_reserve_data: bool,
    /// Last update time
    last_update: Instant,
    /// Trade count for activity tracking
    trade_count: u64,
    /// DEX-specific accounts from DexPoolAccounts event (for deterministic IX building)
    /// These are passed through to execution-engine so it needs ZERO RPC calls.
    dex_accounts: Option<Vec<String>>,
}

/// Owned pool inputs for v2 round-trip selection (avoids dangling refs to temporaries).
type OwnedRoundTripCandidate = (
    QuotePoolInput,
    Option<QuoteVaultInput>,
    Option<HashMap<i64, Vec<BinData>>>,
    String,
);

/// Tracks same token across multiple DEXes
#[derive(Debug, Clone)]
struct TokenArbTracker {
    base_mint: String,
    /// Pool states keyed by pool_address (multiple pools per DEX allowed)
    pools: HashMap<String, PoolState>,
    /// Pool accounts by pool_address (from DexPoolAccounts events)
    /// Key: pool_address, Value: accounts vec
    pool_accounts: HashMap<String, Vec<String>>,
    /// Token program for base_mint (SPL Token or Token-2022), from TokenMintInfo event
    token_program: Option<String>,
    /// Token decimals from Trade events (for reserve mid normalization)
    token_decimals: Option<u8>,
    /// Last intent generated time
    last_intent_time: Option<Instant>,
}

/// Per-pool row for 2-hop eligibility forensics (bounded, no dynamic Prometheus labels).
#[derive(Debug, Clone)]
struct PoolEligibilityRow {
    pool_address: String,
    dex: String,
    known: bool,
    has_reserve_data: bool,
    has_trade_mid: bool,
    has_decimals: bool,
    fresh: bool,
    buy_price: Option<Decimal>,
    sell_price: Option<Decimal>,
    buy_plausible: bool,
    sell_plausible: bool,
    comparable_price_present: bool,
    comparable_price_plausible: bool,
    eligible: bool,
}

/// Aggregated mint-level eligibility breakdown for metrics + snapshots.
#[derive(Debug, Clone)]
struct MintEligibilityBreakdown {
    mint: String,
    candidate_pools_total: usize,
    known_pools: usize,
    fresh_price: usize,
    has_reserve_data: usize,
    has_trade_mid: usize,
    has_decimals: usize,
    comparable_price_present: usize,
    comparable_price_plausible: usize,
    eligible_pools: usize,
    eligible_dexes: usize,
    eligible_by_dex: HashMap<String, usize>,
    reject_subreason: Option<ArbTwoHopRejectSubreason>,
    pool_rows: Vec<PoolEligibilityRow>,
}

/// Rate-limited collector for top offending mints (insufficient_pools / stale_price).
struct ArbEligibilityForensics {
    last_snapshot: RwLock<Instant>,
    pending: RwLock<HashMap<String, MintEligibilityBreakdown>>,
    snapshots_emitted: AtomicU64,
}

impl ArbEligibilityForensics {
    fn new() -> Self {
        Self {
            last_snapshot: RwLock::new(Instant::now()),
            pending: RwLock::new(HashMap::new()),
            snapshots_emitted: AtomicU64::new(0),
        }
    }

    fn record(&self, breakdown: MintEligibilityBreakdown) {
        let Some(subreason) = breakdown.reject_subreason else {
            return;
        };
        if !matches!(
            subreason,
            ArbTwoHopRejectSubreason::StalePrice
                | ArbTwoHopRejectSubreason::NotKnownPool
                | ArbTwoHopRejectSubreason::MissingDecimals
                | ArbTwoHopRejectSubreason::MissingReserves
                | ArbTwoHopRejectSubreason::MissingTradePrice
                | ArbTwoHopRejectSubreason::NoComparablePrice
                | ArbTwoHopRejectSubreason::SameDexOnly
                | ArbTwoHopRejectSubreason::ImplausiblePrice
                | ArbTwoHopRejectSubreason::OnlyOneEligiblePool
                | ArbTwoHopRejectSubreason::OnlyOneEligibleDex
        ) {
            return;
        }

        let mut pending = self.pending.write();
        pending.insert(breakdown.mint.clone(), breakdown);
        if pending.len() > ELIGIBILITY_PENDING_CAP {
            let drop_key = pending
                .keys()
                .next()
                .cloned()
                .expect("pending non-empty after cap exceeded");
            pending.remove(&drop_key);
        }
    }

    fn maybe_emit_snapshot(&self) -> bool {
        {
            let last = self.last_snapshot.read();
            if last.elapsed() < ELIGIBILITY_SNAPSHOT_COOLDOWN {
                return false;
            }
        }

        let mut pending = self.pending.write();
        if pending.is_empty() {
            return false;
        }

        let mut ranked: Vec<MintEligibilityBreakdown> = pending.values().cloned().collect();
        ranked.sort_by(|a, b| {
            b.eligible_pools
                .cmp(&a.eligible_pools)
                .then_with(|| a.candidate_pools_total.cmp(&b.candidate_pools_total))
        });
        let logged: Vec<MintEligibilityBreakdown> = ranked
            .into_iter()
            .take(ELIGIBILITY_SNAPSHOT_TOP_N)
            .collect();

        for entry in &logged {
            let top_pools: Vec<_> = entry
                .pool_rows
                .iter()
                .take(ELIGIBILITY_SNAPSHOT_POOL_ROWS)
                .map(|row| {
                    serde_json::json!({
                        "pool": row.pool_address,
                        "dex": row.dex,
                        "known": row.known,
                        "has_reserve_data": row.has_reserve_data,
                        "has_trade_mid": row.has_trade_mid,
                        "has_decimals": row.has_decimals,
                        "fresh": row.fresh,
                        "comparable_price_present": row.comparable_price_present,
                        "comparable_price_plausible": row.comparable_price_plausible,
                    })
                })
                .collect();

            info!(
                kind = "arb_two_hop_eligibility_snapshot",
                mint = %entry.mint,
                total_pools = entry.candidate_pools_total,
                eligible_pools = entry.eligible_pools,
                eligible_dexes = entry.eligible_dexes,
                reject_subreason = ?entry.reject_subreason,
                top_pools = %serde_json::to_string(&top_pools).unwrap_or_else(|_| "[]".to_string()),
                "2-hop eligibility forensics snapshot"
            );
        }

        for entry in &logged {
            pending.remove(&entry.mint);
        }

        *self.last_snapshot.write() = Instant::now();
        self.snapshots_emitted.fetch_add(1, Ordering::Relaxed);
        true
    }

    #[cfg(test)]
    fn pending_mint_count(&self) -> usize {
        self.pending.read().len()
    }

    #[cfg(test)]
    fn snapshots_emitted_count(&self) -> u64 {
        self.snapshots_emitted.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    fn force_snapshot_ready(&self) {
        *self.last_snapshot.write() =
            Instant::now() - ELIGIBILITY_SNAPSHOT_COOLDOWN - Duration::from_secs(1);
    }
}

/// Per-pool row for v2 round-trip eligibility forensics (bounded, journal only).
#[derive(Debug, Clone)]
struct PoolV2EligibilityRow {
    pool_address: String,
    dex: String,
    known: bool,
    has_vault: bool,
    has_dlmm_bins: bool,
    buy_quote_ok: bool,
    buy_quote_fresh: bool,
    sell_quote_ok: bool,
    sell_quote_fresh: bool,
    sell_fail_reason: Option<String>,
    sell_quote_none_reason: Option<String>,
    token_amount_in: Option<u64>,
    sell_quote_kind: Option<String>,
}

/// Aggregated mint-level v2 eligibility breakdown for rate-limited snapshots.
#[derive(Debug, Clone)]
struct MintV2EligibilityBreakdown {
    mint: String,
    candidate_pools_total: usize,
    v2_candidate_pools: usize,
    distinct_dexes: usize,
    reject_subreason: RoundTripInsufficientSubreason,
    pool_rows: Vec<PoolV2EligibilityRow>,
}

/// Rate-limited collector for v2 insufficient-pools forensics.
struct ArbV2EligibilityForensics {
    last_snapshot: RwLock<Instant>,
    pending: RwLock<HashMap<String, MintV2EligibilityBreakdown>>,
    snapshots_emitted: AtomicU64,
}

impl ArbV2EligibilityForensics {
    fn new() -> Self {
        Self {
            last_snapshot: RwLock::new(Instant::now()),
            pending: RwLock::new(HashMap::new()),
            snapshots_emitted: AtomicU64::new(0),
        }
    }

    fn record(&self, breakdown: MintV2EligibilityBreakdown) {
        if !matches!(
            breakdown.reject_subreason,
            RoundTripInsufficientSubreason::CandidatesLt2
                | RoundTripInsufficientSubreason::NoFreshBuyQuote
                | RoundTripInsufficientSubreason::NoCrossDexSell
                | RoundTripInsufficientSubreason::SingleDexCandidates
        ) {
            return;
        }

        let mut pending = self.pending.write();
        pending.insert(breakdown.mint.clone(), breakdown);
        if pending.len() > ELIGIBILITY_PENDING_CAP {
            let drop_key = pending
                .keys()
                .next()
                .cloned()
                .expect("pending non-empty after cap exceeded");
            pending.remove(&drop_key);
        }
    }

    fn maybe_emit_snapshot(&self) -> bool {
        {
            let last = self.last_snapshot.read();
            if last.elapsed() < ELIGIBILITY_SNAPSHOT_COOLDOWN {
                return false;
            }
        }

        let mut pending = self.pending.write();
        if pending.is_empty() {
            return false;
        }

        let mut ranked: Vec<MintV2EligibilityBreakdown> = pending.values().cloned().collect();
        ranked.sort_by(|a, b| {
            b.v2_candidate_pools
                .cmp(&a.v2_candidate_pools)
                .then_with(|| a.candidate_pools_total.cmp(&b.candidate_pools_total))
        });
        let logged: Vec<MintV2EligibilityBreakdown> = ranked
            .into_iter()
            .take(ELIGIBILITY_SNAPSHOT_TOP_N)
            .collect();

        for entry in &logged {
            let top_pools: Vec<_> = entry
                .pool_rows
                .iter()
                .take(ELIGIBILITY_SNAPSHOT_POOL_ROWS)
                .map(|row| {
                    serde_json::json!({
                        "pool": row.pool_address,
                        "dex": row.dex,
                        "known": row.known,
                        "has_vault": row.has_vault,
                        "has_dlmm_bins": row.has_dlmm_bins,
                        "buy_quote_ok": row.buy_quote_ok,
                        "buy_quote_fresh": row.buy_quote_fresh,
                        "sell_quote_ok": row.sell_quote_ok,
                        "sell_quote_fresh": row.sell_quote_fresh,
                        "sell_fail_reason": row.sell_fail_reason,
                        "sell_quote_none_reason": row.sell_quote_none_reason,
                        "token_amount_in": row.token_amount_in,
                        "sell_quote_kind": row.sell_quote_kind,
                    })
                })
                .collect();

            info!(
                kind = "arb_two_hop_v2_eligibility_snapshot",
                mint = %entry.mint,
                candidate_pools_total = entry.candidate_pools_total,
                v2_candidate_pools = entry.v2_candidate_pools,
                distinct_dexes = entry.distinct_dexes,
                reject_subreason = ?entry.reject_subreason,
                top_pools = %serde_json::to_string(&top_pools).unwrap_or_else(|_| "[]".to_string()),
                "2-hop v2 eligibility forensics snapshot"
            );
        }

        for entry in &logged {
            pending.remove(&entry.mint);
        }

        *self.last_snapshot.write() = Instant::now();
        self.snapshots_emitted.fetch_add(1, Ordering::Relaxed);
        true
    }

    #[cfg(test)]
    fn pending_mint_count(&self) -> usize {
        self.pending.read().len()
    }

    #[cfg(test)]
    fn snapshots_emitted_count(&self) -> u64 {
        self.snapshots_emitted.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    fn force_snapshot_ready(&self) {
        *self.last_snapshot.write() =
            Instant::now() - ELIGIBILITY_SNAPSHOT_COOLDOWN - Duration::from_secs(1);
    }
}

fn v2_insufficient_subreason_to_metric(
    subreason: RoundTripInsufficientSubreason,
) -> ArbTwoHopV2InsufficientSubreason {
    match subreason {
        RoundTripInsufficientSubreason::CandidatesLt2 => {
            ArbTwoHopV2InsufficientSubreason::CandidatesLt2
        }
        RoundTripInsufficientSubreason::NoFreshBuyQuote => {
            ArbTwoHopV2InsufficientSubreason::NoFreshBuyQuote
        }
        RoundTripInsufficientSubreason::NoCrossDexSell => {
            ArbTwoHopV2InsufficientSubreason::NoCrossDexSell
        }
        RoundTripInsufficientSubreason::SingleDexCandidates => {
            ArbTwoHopV2InsufficientSubreason::SingleDexCandidates
        }
    }
}

fn v2_no_cross_dex_sell_detail_to_metric(
    detail: NoCrossDexSellDetailReason,
) -> ArbTwoHopV2NoCrossDexSellDetail {
    match detail {
        NoCrossDexSellDetailReason::SellMissingVault => {
            ArbTwoHopV2NoCrossDexSellDetail::SellMissingVault
        }
        NoCrossDexSellDetailReason::SellMissingDlmmBins => {
            ArbTwoHopV2NoCrossDexSellDetail::SellMissingDlmmBins
        }
        NoCrossDexSellDetailReason::SellQuoteNone => ArbTwoHopV2NoCrossDexSellDetail::SellQuoteNone,
        NoCrossDexSellDetailReason::SellNotFresh => ArbTwoHopV2NoCrossDexSellDetail::SellNotFresh,
        NoCrossDexSellDetailReason::SellZeroOut => ArbTwoHopV2NoCrossDexSellDetail::SellZeroOut,
    }
}

fn v2_sell_quote_none_detail_to_metric(
    detail: SellQuoteNoneDetailReason,
) -> ArbTwoHopV2SellQuoteNoneDetail {
    match detail {
        SellQuoteNoneDetailReason::StateStale => ArbTwoHopV2SellQuoteNoneDetail::StateStale,
        SellQuoteNoneDetailReason::ReservesImplausible => {
            ArbTwoHopV2SellQuoteNoneDetail::ReservesImplausible
        }
        SellQuoteNoneDetailReason::DlmmActiveBinMissing => {
            ArbTwoHopV2SellQuoteNoneDetail::DlmmActiveBinMissing
        }
        SellQuoteNoneDetailReason::DlmmWalkerZero => ArbTwoHopV2SellQuoteNoneDetail::DlmmWalkerZero,
        SellQuoteNoneDetailReason::DlmmMarginalReject => {
            ArbTwoHopV2SellQuoteNoneDetail::DlmmMarginalReject
        }
        SellQuoteNoneDetailReason::CpmmMathNone => ArbTwoHopV2SellQuoteNoneDetail::CpmmMathNone,
        SellQuoteNoneDetailReason::UnsupportedDex => ArbTwoHopV2SellQuoteNoneDetail::UnsupportedDex,
        SellQuoteNoneDetailReason::TradeFallbackNone => {
            ArbTwoHopV2SellQuoteNoneDetail::TradeFallbackNone
        }
        SellQuoteNoneDetailReason::MintDirectionInvalid => {
            ArbTwoHopV2SellQuoteNoneDetail::MintDirectionInvalid
        }
    }
}

fn record_live_cache_age_at_snapshot(op: &str, age_ms: u64, pin_class: &str) {
    let bucket = freshness_age_bucket(age_ms).as_metric_label();
    arb_vault_live_snapshot_cache_age_bucket_inc(op, bucket);
    arb_vault_live_snapshot_cache_age_pin_bucket_inc(op, bucket, pin_class);
}

fn record_v2_insufficient_subreason(insufficient: &RoundTripInsufficient) {
    arb_two_hop_v2_insufficient_subreason_inc(v2_insufficient_subreason_to_metric(
        insufficient.subreason,
    ));
    if let Some(detail) = insufficient.no_fresh_buy_quote_detail {
        arb_two_hop_v2_no_fresh_buy_quote_detail_inc(detail.as_metric_label());
    }
    if let Some(detail) = insufficient.no_cross_dex_sell_detail {
        arb_two_hop_v2_no_cross_dex_sell_detail_inc(v2_no_cross_dex_sell_detail_to_metric(detail));
    }
    if let Some(counts) = &insufficient.sell_quote_none_detail_counts {
        for (reason, count) in counts {
            let metric = v2_sell_quote_none_detail_to_metric(*reason);
            for _ in 0..*count {
                arb_two_hop_v2_sell_quote_none_detail_inc(metric);
            }
        }
    }
    if let Some(counts) = &insufficient.sell_not_fresh_detail_counts {
        for (diagnosis, count) in counts {
            for _ in 0..*count {
                arb_two_hop_v2_sell_not_fresh_detail_inc(
                    diagnosis.kind.as_metric_label(),
                    diagnosis.cause.as_metric_label(),
                );
            }
        }
    }
    if let Some(counts) = &insufficient.state_stale_age_bucket_counts {
        for (bucket, count) in counts {
            for _ in 0..*count {
                arb_two_hop_v2_state_stale_age_bucket_inc(bucket.as_metric_label());
            }
        }
    }
}

const CROSS_DEX_PAIR_DEBUG_SAMPLE_MAX: usize = 3;
const HOT_PATH_LOG_THROTTLE_SECS: u64 = 60;
const V2_INSUFFICIENT_LOG_CATEGORY_COUNT: usize = 9;

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
        fn many_distinct_mint_values_share_one_category_slot() {
            let mut throttle = FixedCategoryLogThrottle::<2>::new(Duration::from_secs(60));
            let category = 0usize;
            let t0 = Instant::now();
            assert!(throttle.should_emit(category, t0));
            for offset in 1..=1000u64 {
                assert!(
                    !throttle.should_emit(category, t0 + Duration::from_millis(offset)),
                    "category must stay suppressed regardless of mint cardinality"
                );
            }
            assert!(throttle.should_emit(category, t0 + Duration::from_secs(60)));
        }

        #[test]
        fn distinct_categories_emit_independently() {
            let mut throttle = FixedCategoryLogThrottle::<3>::new(Duration::from_secs(60));
            let t0 = Instant::now();
            assert!(throttle.should_emit(0, t0));
            assert!(throttle.should_emit(1, t0));
            assert!(!throttle.should_emit(0, t0 + Duration::from_secs(1)));
        }

        #[test]
        fn category_reopens_after_interval() {
            let mut throttle = FixedCategoryLogThrottle::<1>::new(Duration::from_secs(60));
            let t0 = Instant::now();
            assert!(throttle.should_emit(0, t0));
            assert!(!throttle.should_emit(0, t0 + Duration::from_secs(59)));
            assert!(throttle.should_emit(0, t0 + Duration::from_secs(60)));
        }
    }
}

#[repr(usize)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum V2InsufficientLogCategory {
    CandidatesLt2 = 0,
    NoFreshBuyQuote = 1,
    SingleDexCandidates = 2,
    NoCrossDexSellUnknown = 3,
    NoCrossDexSellMissingVault = 4,
    NoCrossDexSellMissingDlmmBins = 5,
    NoCrossDexSellQuoteNone = 6,
    NoCrossDexSellNotFresh = 7,
    NoCrossDexSellZeroOut = 8,
}

fn v2_insufficient_log_category(insufficient: &RoundTripInsufficient) -> V2InsufficientLogCategory {
    match insufficient.subreason {
        RoundTripInsufficientSubreason::CandidatesLt2 => V2InsufficientLogCategory::CandidatesLt2,
        RoundTripInsufficientSubreason::NoFreshBuyQuote => {
            V2InsufficientLogCategory::NoFreshBuyQuote
        }
        RoundTripInsufficientSubreason::SingleDexCandidates => {
            V2InsufficientLogCategory::SingleDexCandidates
        }
        RoundTripInsufficientSubreason::NoCrossDexSell => {
            match insufficient.no_cross_dex_sell_detail {
                Some(NoCrossDexSellDetailReason::SellMissingVault) => {
                    V2InsufficientLogCategory::NoCrossDexSellMissingVault
                }
                Some(NoCrossDexSellDetailReason::SellMissingDlmmBins) => {
                    V2InsufficientLogCategory::NoCrossDexSellMissingDlmmBins
                }
                Some(NoCrossDexSellDetailReason::SellQuoteNone) => {
                    V2InsufficientLogCategory::NoCrossDexSellQuoteNone
                }
                Some(NoCrossDexSellDetailReason::SellNotFresh) => {
                    V2InsufficientLogCategory::NoCrossDexSellNotFresh
                }
                Some(NoCrossDexSellDetailReason::SellZeroOut) => {
                    V2InsufficientLogCategory::NoCrossDexSellZeroOut
                }
                None => V2InsufficientLogCategory::NoCrossDexSellUnknown,
            }
        }
    }
}

static ARB_V2_INSUFFICIENT_LOG_THROTTLE: std::sync::LazyLock<
    parking_lot::Mutex<
        fixed_category_log_throttle::FixedCategoryLogThrottle<V2_INSUFFICIENT_LOG_CATEGORY_COUNT>,
    >,
> = std::sync::LazyLock::new(|| {
    parking_lot::Mutex::new(fixed_category_log_throttle::FixedCategoryLogThrottle::new(
        Duration::from_secs(HOT_PATH_LOG_THROTTLE_SECS),
    ))
});

fn insufficient_subreason_metric_label(subreason: RoundTripInsufficientSubreason) -> &'static str {
    match subreason {
        RoundTripInsufficientSubreason::CandidatesLt2 => "candidates_lt2",
        RoundTripInsufficientSubreason::NoFreshBuyQuote => "no_fresh_buy_quote",
        RoundTripInsufficientSubreason::NoCrossDexSell => "no_cross_dex_sell",
        RoundTripInsufficientSubreason::SingleDexCandidates => "single_dex_candidates",
    }
}

fn log_v2_round_trip_insufficient_pools(
    mint: &str,
    insufficient: &RoundTripInsufficient,
    candidates: &[RoundTripPoolCandidate<'_>],
    probe: u64,
    freshness: &QuoteFreshnessConfig,
    token_decimals: u8,
) {
    let category = v2_insufficient_log_category(insufficient);
    let now = Instant::now();
    if !ARB_V2_INSUFFICIENT_LOG_THROTTLE
        .lock()
        .should_emit(category as usize, now)
    {
        return;
    }
    let subreason = insufficient.subreason;
    let subreason_label = insufficient_subreason_metric_label(subreason);
    if subreason == RoundTripInsufficientSubreason::NoCrossDexSell {
        let dominant = insufficient
            .no_cross_dex_sell_detail
            .map(|d| d.as_metric_label())
            .unwrap_or("unknown");
        warn!(
            mint = %mint,
            subreason = subreason_label,
            dominant_sell_fail_reason = dominant,
            "arb v2 screen: insufficient pools (no cross-dex sell)"
        );
        log_v2_cross_dex_pair_failures_debug_sample(
            mint,
            candidates,
            probe,
            freshness,
            token_decimals,
        );
    } else {
        info!(
            mint = %mint,
            subreason = subreason_label,
            "arb v2 screen: insufficient pools"
        );
    }
}

fn log_v2_cross_dex_pair_failures_debug_sample(
    mint: &str,
    candidates: &[RoundTripPoolCandidate<'_>],
    probe: u64,
    freshness: &QuoteFreshnessConfig,
    token_decimals: u8,
) {
    let now = Instant::now();
    let mut logged = 0usize;
    'outer: for buy in candidates {
        let Some(buy_quote) = quote_exact_in_with_freshness(
            buy.pool,
            buy.vault,
            buy.dlmm_bins,
            NATIVE_SOL_MINT,
            &buy.pool.token_mint,
            probe,
            freshness,
        ) else {
            continue;
        };
        if !is_quote_fresh(&buy_quote, freshness, buy.vault, now) {
            continue;
        }
        for sell in candidates {
            if sell.dex == buy.dex {
                continue;
            }
            let Some(failure) = classify_cross_dex_sell_failure(
                sell,
                buy_quote.amount_out,
                freshness,
                now,
                token_decimals,
            ) else {
                continue;
            };
            debug!(
                mint = %mint,
                buy_dex = buy.dex,
                sell_dex = sell.dex,
                sell_fail_reason = failure.as_top_level_detail().as_metric_label(),
                "arb v2 screen: cross-dex pair sell failure sample"
            );
            logged += 1;
            if logged >= CROSS_DEX_PAIR_DEBUG_SAMPLE_MAX {
                break 'outer;
            }
        }
    }
}

fn record_eligibility_metrics(breakdown: &MintEligibilityBreakdown) {
    arb_two_hop_pool_gate_add(
        ArbTwoHopPoolGate::CandidatePools,
        breakdown.candidate_pools_total as u64,
    );
    arb_two_hop_pool_gate_add(
        ArbTwoHopPoolGate::InKnownPools,
        breakdown.known_pools as u64,
    );
    arb_two_hop_pool_gate_add(ArbTwoHopPoolGate::FreshPrice, breakdown.fresh_price as u64);
    arb_two_hop_pool_gate_add(
        ArbTwoHopPoolGate::HasReserveData,
        breakdown.has_reserve_data as u64,
    );
    arb_two_hop_pool_gate_add(
        ArbTwoHopPoolGate::HasTradeMid,
        breakdown.has_trade_mid as u64,
    );
    arb_two_hop_pool_gate_add(
        ArbTwoHopPoolGate::HasDecimals,
        breakdown.has_decimals as u64,
    );
    arb_two_hop_pool_gate_add(
        ArbTwoHopPoolGate::ComparablePricePresent,
        breakdown.comparable_price_present as u64,
    );
    arb_two_hop_pool_gate_add(
        ArbTwoHopPoolGate::ComparablePricePlausible,
        breakdown.comparable_price_plausible as u64,
    );
    arb_two_hop_pool_gate_add(
        ArbTwoHopPoolGate::EligiblePools,
        breakdown.eligible_pools as u64,
    );
    arb_two_hop_eligible_dexes_add(breakdown.eligible_dexes as u64);
    for (dex, count) in &breakdown.eligible_by_dex {
        arb_two_hop_eligible_pools_by_dex_add(dex, *count as u64);
    }
}

fn record_insufficient_subreason(reason: ArbTwoHopInsufficientSubreason) {
    arb_two_hop_insufficient_subreason_inc(reason);
}

fn record_reject_subreason(reason: ArbTwoHopRejectSubreason) {
    arb_two_hop_reject_subreason_inc(reason);
}

fn determine_insufficient_subreason(
    breakdown: &MintEligibilityBreakdown,
) -> ArbTwoHopInsufficientSubreason {
    if breakdown.known_pools < 2 && breakdown.candidate_pools_total >= 2 {
        return ArbTwoHopInsufficientSubreason::NotKnownPool;
    }
    if breakdown.comparable_price_present == 0 {
        if breakdown.has_reserve_data > 0 {
            return ArbTwoHopInsufficientSubreason::NoComparablePrice;
        }
        if breakdown.has_reserve_data == 0 && breakdown.has_trade_mid == 0 {
            return ArbTwoHopInsufficientSubreason::MissingReserves;
        }
        if breakdown.has_trade_mid == 0 {
            return ArbTwoHopInsufficientSubreason::MissingTradePrice;
        }
        return ArbTwoHopInsufficientSubreason::NoComparablePrice;
    }
    if breakdown.known_pools >= 2
        && breakdown.has_decimals < breakdown.known_pools
        && breakdown.eligible_pools < 2
    {
        return ArbTwoHopInsufficientSubreason::NoComparablePrice;
    }
    if breakdown.eligible_pools == 1 {
        return ArbTwoHopInsufficientSubreason::OnlyOneEligiblePool;
    }
    if breakdown.eligible_pools >= 2 && breakdown.eligible_dexes < 2 {
        return ArbTwoHopInsufficientSubreason::OnlyOneEligibleDex;
    }
    ArbTwoHopInsufficientSubreason::NoComparablePrice
}

fn analyze_pool_eligibility(
    pool: &PoolState,
    base_mint: &str,
    known_pools: &HashSet<String>,
    token_decimals: Option<u8>,
    vault_balances: &HashMap<String, VaultBalanceCache>,
    bin_arrays: &HashMap<String, HashMap<i64, BinArrayCache>>,
    max_age: Duration,
) -> PoolEligibilityRow {
    let is_known_dex = is_known_dex_label(&pool.dex);
    let known = is_known_dex && known_pools.contains(&pool.pool_address);
    let vault_entry = vault_balances.get(&pool.pool_address);
    let has_reserve_data = pool.has_reserve_data
        || vault_entry
            .map(|v| v.reserve_base > 0 && v.reserve_quote > 0)
            .unwrap_or(false);
    let has_trade_mid = trade_mid_sol_per_token(pool).is_some();
    let has_decimals = token_decimals.is_some();
    let fresh = known && is_pool_price_fresh(pool, vault_entry, max_age);

    let vault_reserves = vault_entry.map(|c| (c.reserve_base, c.reserve_quote));
    let dlmm_bins = bin_arrays.get(&pool.pool_address);
    let buy_price = if known && has_decimals {
        comparable_price_for_eligibility(
            pool,
            vault_reserves,
            token_decimals,
            base_mint,
            vault_entry,
            dlmm_bins,
            ComparablePriceSide::Buy,
        )
    } else {
        None
    };
    let sell_price = if known && has_decimals {
        comparable_price_for_eligibility(
            pool,
            vault_reserves,
            token_decimals,
            base_mint,
            vault_entry,
            dlmm_bins,
            ComparablePriceSide::Sell,
        )
    } else {
        None
    };
    let comparable_price_present = buy_price.is_some() || sell_price.is_some();
    let buy_plausible = buy_price
        .filter(|p| *p > Decimal::ZERO)
        .map(|p| is_plausible_sol_per_token_price(base_mint, p))
        .unwrap_or(false);
    let sell_plausible = sell_price
        .filter(|p| *p > Decimal::ZERO)
        .map(|p| is_plausible_sol_per_token_price(base_mint, p))
        .unwrap_or(false);
    let comparable_price_plausible = comparable_price_present && (buy_plausible || sell_plausible);
    let eligible = known && comparable_price_present;

    PoolEligibilityRow {
        pool_address: pool.pool_address.clone(),
        dex: pool.dex.clone(),
        known,
        has_reserve_data,
        has_trade_mid,
        has_decimals,
        fresh,
        buy_price,
        sell_price,
        buy_plausible,
        sell_plausible,
        comparable_price_present,
        comparable_price_plausible,
        eligible,
    }
}

/// Ancillary inputs for `check_arbitrage` (keeps signature within clippy limits).
struct ArbCheckContext<'a> {
    spread_warn_last: &'a RwLock<HashMap<String, Instant>>,
    data_quality_rejects: &'a AtomicU64,
    forensics: Option<&'a ArbEligibilityForensics>,
    v2_forensics: Option<&'a ArbV2EligibilityForensics>,
    /// When set, v2 screens run only for mints in the authoritative selection set (I-ARB-10b).
    selected_mints: Option<&'a HashSet<String>>,
    /// When set and the mint has pinned pools, round-trip candidates use only those pools.
    pinned_pools: Option<&'a HashSet<String>>,
}

fn pool_state_to_quote_input(
    pool: &PoolState,
    token_mint: &str,
    token_decimals: u8,
) -> QuotePoolInput {
    QuotePoolInput {
        pool_address: pool.pool_address.clone(),
        dex: pool.dex.clone(),
        token_mint: token_mint.to_string(),
        trade_price_buy: pool.trade_price_buy,
        trade_price_sell: pool.trade_price_sell,
        trade_updated_at: pool.last_update,
        has_reserve_data: pool.has_reserve_data,
        token_decimals,
    }
}

fn vault_cache_to_quote_input(vault: &VaultBalanceCache) -> QuoteVaultInput {
    QuoteVaultInput {
        reserve_base: vault.reserve_base,
        reserve_quote: vault.reserve_quote,
        update_slot: vault.update_slot,
        updated_at: vault.updated_at,
        active_id: vault.active_id,
        bin_step: vault.bin_step,
        dlmm_sol_is_x: vault.dlmm_sol_is_x,
        dlmm_token_x_mint: vault.dlmm_token_x_mint.clone(),
    }
}

impl TokenArbTracker {
    fn new(base_mint: &str) -> Self {
        Self {
            base_mint: base_mint.to_string(),
            pools: HashMap::new(),
            pool_accounts: HashMap::new(),
            token_program: None,
            token_decimals: None,
            last_intent_time: None,
        }
    }

    /// Store DEX pool accounts (from DexPoolAccounts event)
    fn set_pool_accounts(&mut self, pool_address: &str, accounts: Vec<String>) {
        self.pool_accounts
            .insert(pool_address.to_string(), accounts);
    }

    /// Get DEX pool accounts for a pool
    fn get_pool_accounts(&self, pool_address: &str) -> Option<&Vec<String>> {
        self.pool_accounts.get(pool_address)
    }

    /// Set token program (from TokenMintInfo event)
    fn set_token_program(&mut self, token_program: &str) {
        self.token_program = Some(token_program.to_string());
    }

    /// Add or update a pool for this token (keyed by pool_address)
    fn upsert_pool(&mut self, pool: PoolState) {
        self.pools.insert(pool.pool_address.clone(), pool);
    }

    fn pool_count_on_distinct_dexes(&self) -> usize {
        let mut dexes = HashSet::new();
        for pool in self.pools.values() {
            dexes.insert(pool.dex.as_str());
        }
        dexes.len()
    }

    fn build_eligibility_breakdown(
        &self,
        known_pools: &HashSet<String>,
        vault_balances: &HashMap<String, VaultBalanceCache>,
        bin_arrays: &HashMap<String, HashMap<i64, BinArrayCache>>,
    ) -> MintEligibilityBreakdown {
        let max_age = Duration::from_millis(MAX_PRICE_AGE_MS);
        let mut pool_rows = Vec::with_capacity(self.pools.len());
        let mut known_pools_count = 0usize;
        let mut fresh_price = 0usize;
        let mut has_reserve_data = 0usize;
        let mut has_trade_mid = 0usize;
        let mut has_decimals = 0usize;
        let mut comparable_price_present = 0usize;
        let mut comparable_price_plausible = 0usize;

        for pool in self.pools.values() {
            let row = analyze_pool_eligibility(
                pool,
                &self.base_mint,
                known_pools,
                self.token_decimals,
                vault_balances,
                bin_arrays,
                max_age,
            );
            if row.known {
                known_pools_count += 1;
            }
            if row.fresh {
                fresh_price += 1;
            }
            if row.has_reserve_data {
                has_reserve_data += 1;
            }
            if row.has_trade_mid {
                has_trade_mid += 1;
            }
            if row.has_decimals {
                has_decimals += 1;
            }
            if row.comparable_price_present {
                comparable_price_present += 1;
            }
            if row.comparable_price_plausible {
                comparable_price_plausible += 1;
            }
            pool_rows.push(row);
        }

        let mut eligible_by_dex: HashMap<String, usize> = HashMap::new();
        let mut eligible_pools = 0usize;
        for row in &pool_rows {
            if row.eligible {
                eligible_pools += 1;
                *eligible_by_dex.entry(row.dex.clone()).or_default() += 1;
            }
        }

        MintEligibilityBreakdown {
            mint: self.base_mint.clone(),
            candidate_pools_total: pool_rows.len(),
            known_pools: known_pools_count,
            fresh_price,
            has_reserve_data,
            has_trade_mid,
            has_decimals,
            comparable_price_present,
            comparable_price_plausible,
            eligible_pools,
            eligible_dexes: eligible_by_dex.len(),
            eligible_by_dex,
            reject_subreason: None,
            pool_rows,
        }
    }

    fn emit_eligibility_forensics(
        &self,
        breakdown: MintEligibilityBreakdown,
        forensics: Option<&ArbEligibilityForensics>,
    ) {
        record_eligibility_metrics(&breakdown);
        if let Some(subreason) = breakdown.reject_subreason {
            record_reject_subreason(subreason);
        }
        if let Some(collector) = forensics {
            collector.record(breakdown);
        }
    }

    /// Shadow pool_quote round-trip (metrics only; does not affect legacy decisions).
    fn run_arb_quote_shadow(
        &self,
        config: &ArbConfig,
        known_pools: &HashSet<String>,
        vault_balances: &HashMap<String, VaultBalanceCache>,
        bin_arrays: &HashMap<String, HashMap<i64, BinArrayCache>>,
        legacy_spread_bps: Option<i64>,
    ) {
        if !config.arb_quote_shadow_mode {
            return;
        }
        let Some(token_decimals) = self.token_decimals else {
            return;
        };
        let max_age = Duration::from_millis(MAX_PRICE_AGE_MS);
        let probe_sol = config.max_position_lamports;
        let mut best_profit: Option<i64> = None;
        let mut saw_incompatible_kind = false;

        let candidate_pools: Vec<&PoolState> = self
            .pools
            .values()
            .filter(|p| known_pools.contains(&p.pool_address))
            .filter(|p| is_known_dex_label(&p.dex))
            .filter(|p| p.dex != "pumpfun")
            .collect();

        for buy_pool in &candidate_pools {
            let buy_vault = vault_balances.get(&buy_pool.pool_address);
            if !is_pool_price_fresh(buy_pool, buy_vault, max_age) {
                continue;
            }
            let buy_input = pool_state_to_quote_input(buy_pool, &self.base_mint, token_decimals);
            let buy_vault_q = buy_vault.map(vault_cache_to_quote_input);
            let buy_bins = bin_arrays
                .get(&buy_pool.pool_address)
                .map(flatten_bin_array_cache);

            for sell_pool in &candidate_pools {
                if sell_pool.pool_address == buy_pool.pool_address || sell_pool.dex == buy_pool.dex
                {
                    continue;
                }
                let sell_vault = vault_balances.get(&sell_pool.pool_address);
                if !is_pool_price_fresh(sell_pool, sell_vault, max_age) {
                    continue;
                }
                let sell_input =
                    pool_state_to_quote_input(sell_pool, &self.base_mint, token_decimals);
                let sell_vault_q = sell_vault.map(vault_cache_to_quote_input);
                let sell_bins = bin_arrays
                    .get(&sell_pool.pool_address)
                    .map(flatten_bin_array_cache);

                if let Some(profit) = round_trip_profit_lamports(
                    &RoundTripLeg {
                        pool: &buy_input,
                        vault: buy_vault_q.as_ref(),
                        dlmm_bins: buy_bins.as_ref(),
                    },
                    &RoundTripLeg {
                        pool: &sell_input,
                        vault: sell_vault_q.as_ref(),
                        dlmm_bins: sell_bins.as_ref(),
                    },
                    probe_sol,
                    config.est_tx_cost_lamports,
                ) {
                    best_profit = Some(best_profit.map_or(profit, |b| b.max(profit)));
                    continue;
                }

                let Some(buy_quote) = quote_exact_in(
                    &buy_input,
                    buy_vault_q.as_ref(),
                    buy_bins.as_ref(),
                    NATIVE_SOL_MINT,
                    &self.base_mint,
                    probe_sol,
                ) else {
                    continue;
                };
                let Some(sell_quote) = quote_exact_in(
                    &sell_input,
                    sell_vault_q.as_ref(),
                    sell_bins.as_ref(),
                    &self.base_mint,
                    NATIVE_SOL_MINT,
                    buy_quote.amount_out,
                ) else {
                    continue;
                };
                if !quotes_pairable(&buy_quote, &sell_quote) {
                    saw_incompatible_kind = true;
                }
            }
        }

        record_arb_quote_shadow_round_trip(
            best_profit.unwrap_or(0),
            legacy_spread_bps,
            saw_incompatible_kind && best_profit.is_none(),
        );
    }

    fn quote_freshness_config(config: &ArbConfig) -> QuoteFreshnessConfig {
        QuoteFreshnessConfig {
            trade_ttl_ms: config.arb_quote_trade_ttl_ms,
            state_ttl_ms: config.arb_quote_state_ttl_ms,
        }
    }

    fn build_round_trip_candidates(
        &self,
        known_pools: &HashSet<String>,
        vault_balances: &HashMap<String, VaultBalanceCache>,
        bin_arrays: &HashMap<String, HashMap<i64, BinArrayCache>>,
        token_decimals: u8,
        pinned_pools: Option<&HashSet<String>>,
    ) -> Vec<OwnedRoundTripCandidate> {
        let mint_pinned_filter: Option<HashSet<&str>> = pinned_pools.map(|pinned| {
            self.pools
                .keys()
                .filter(|addr| pinned.contains(*addr))
                .map(|addr| addr.as_str())
                .collect()
        });

        self.pools
            .values()
            .filter(|p| known_pools.contains(&p.pool_address))
            .filter(|p| {
                mint_pinned_filter
                    .as_ref()
                    .is_none_or(|filter| filter.contains(p.pool_address.as_str()))
            })
            .filter(|p| is_known_dex_label(&p.dex))
            .filter(|p| p.dex != "pumpfun")
            .map(|pool| {
                let vault = vault_balances
                    .get(&pool.pool_address)
                    .map(vault_cache_to_quote_input);
                let bins = bin_arrays
                    .get(&pool.pool_address)
                    .map(flatten_bin_array_cache);
                (
                    pool_state_to_quote_input(pool, &self.base_mint, token_decimals),
                    vault,
                    bins,
                    pool.dex.clone(),
                )
            })
            .collect()
    }

    #[allow(clippy::too_many_arguments)]
    fn build_v2_eligibility_breakdown(
        &self,
        known_pools: &HashSet<String>,
        vault_balances: &HashMap<String, VaultBalanceCache>,
        bin_arrays: &HashMap<String, HashMap<i64, BinArrayCache>>,
        token_decimals: u8,
        probe: u64,
        freshness: &QuoteFreshnessConfig,
        reject_subreason: RoundTripInsufficientSubreason,
        pinned_pools: Option<&HashSet<String>>,
    ) -> MintV2EligibilityBreakdown {
        let owned_candidates = self.build_round_trip_candidates(
            known_pools,
            vault_balances,
            bin_arrays,
            token_decimals,
            pinned_pools,
        );
        let candidates: Vec<RoundTripPoolCandidate<'_>> = owned_candidates
            .iter()
            .map(|(pool, vault, bins, dex)| RoundTripPoolCandidate {
                pool,
                vault: vault.as_ref(),
                dlmm_bins: bins.as_ref(),
                dex,
            })
            .collect();

        let distinct_dexes: HashSet<&str> = candidates.iter().map(|c| c.dex).collect();
        let now = Instant::now();

        let mut pairing_token_amount: Option<u64> = None;
        let mut pairing_buy_quote: Option<PoolQuote> = None;
        for candidate in &candidates {
            let buy_quote = quote_exact_in_with_freshness(
                candidate.pool,
                candidate.vault,
                candidate.dlmm_bins,
                NATIVE_SOL_MINT,
                &candidate.pool.token_mint,
                probe,
                freshness,
            );
            let Some(buy_quote) = buy_quote else {
                continue;
            };
            if !is_quote_fresh(&buy_quote, freshness, candidate.vault, now) {
                continue;
            }
            let replace = match pairing_token_amount {
                None => true,
                Some(current) => buy_quote.amount_out > current,
            };
            if replace {
                pairing_token_amount = Some(buy_quote.amount_out);
                pairing_buy_quote = Some(buy_quote);
            }
        }

        let mut pool_rows =
            Vec::with_capacity(candidates.len().min(ELIGIBILITY_SNAPSHOT_POOL_ROWS));
        for candidate in candidates.iter().take(ELIGIBILITY_SNAPSHOT_POOL_ROWS) {
            let buy_quote = quote_exact_in_with_freshness(
                candidate.pool,
                candidate.vault,
                candidate.dlmm_bins,
                NATIVE_SOL_MINT,
                &candidate.pool.token_mint,
                probe,
                freshness,
            );
            let buy_quote_ok = buy_quote.is_some();
            let buy_quote_fresh = buy_quote
                .as_ref()
                .is_some_and(|q| is_quote_fresh(q, freshness, candidate.vault, now));

            let token_amount_in = pairing_token_amount;
            let (
                sell_quote_ok,
                sell_quote_fresh,
                sell_quote,
                sell_fail_reason,
                sell_quote_none_reason,
            ) = if let (Some(token_amount), Some(buy_q)) =
                (token_amount_in, pairing_buy_quote.as_ref())
            {
                let sell_quote = if let Some(vault) = candidate.vault {
                    quote_sell_round_trip(
                        candidate.pool,
                        vault,
                        candidate.dlmm_bins,
                        token_amount,
                        freshness,
                    )
                } else {
                    None
                };
                let ok = sell_quote.is_some();
                let fresh = sell_quote.as_ref().is_some_and(|q| {
                    is_quote_fresh(q, freshness, candidate.vault, now) && quotes_pairable(buy_q, q)
                });
                let failure = if ok {
                    None
                } else {
                    classify_cross_dex_sell_failure(
                        candidate,
                        token_amount,
                        freshness,
                        now,
                        token_decimals,
                    )
                };
                let fail = failure
                    .as_ref()
                    .map(|f| f.as_top_level_detail().as_metric_label().to_string());
                let none_reason = failure
                    .and_then(|f| f.sell_quote_none_subreason())
                    .map(|sub| sub.as_metric_label().to_string());
                (ok, fresh, sell_quote, fail, none_reason)
            } else {
                (false, false, None, None, None)
            };
            let sell_quote_kind = sell_quote.as_ref().map(|q| {
                match q.kind {
                    QuoteKind::ExecutableMarginal => "executable_marginal",
                    QuoteKind::LastTradeMid => "last_trade_mid",
                }
                .to_string()
            });

            pool_rows.push(PoolV2EligibilityRow {
                pool_address: candidate.pool.pool_address.clone(),
                dex: candidate.dex.to_string(),
                known: known_pools.contains(&candidate.pool.pool_address),
                has_vault: candidate.vault.is_some(),
                has_dlmm_bins: candidate.dlmm_bins.is_some(),
                buy_quote_ok,
                buy_quote_fresh,
                sell_quote_ok,
                sell_quote_fresh,
                sell_fail_reason,
                sell_quote_none_reason,
                token_amount_in,
                sell_quote_kind,
            });
        }

        MintV2EligibilityBreakdown {
            mint: self.base_mint.clone(),
            candidate_pools_total: self.pools.len(),
            v2_candidate_pools: owned_candidates.len(),
            distinct_dexes: distinct_dexes.len(),
            reject_subreason,
            pool_rows,
        }
    }

    /// I-ARB-6: profit-first 2-hop via round-trip quotes (no legacy mid-spread gates).
    fn check_arbitrage_v2(
        &self,
        config: &ArbConfig,
        known_pools: &HashSet<String>,
        vault_balances: &HashMap<String, VaultBalanceCache>,
        bin_arrays: &HashMap<String, HashMap<i64, BinArrayCache>>,
        v2_forensics: Option<&ArbV2EligibilityForensics>,
        check_ctx: &ArbCheckContext<'_>,
    ) -> Option<ArbOpportunity> {
        if let Some(selected_mints) = check_ctx.selected_mints {
            if !selected_mints.contains(&self.base_mint) {
                arb_two_hop_v2_screen_skipped_inc(ArbTwoHopV2ScreenSkipReason::MintNotSelected);
                return None;
            }
        }

        arb_two_hop_v2_screen_inc();
        if self.pool_count_on_distinct_dexes() >= 2 {
            arb_two_hop_v2_screen_multi_dex_inc();
        }
        try_record_arb_track_pin_before_first_screen_ms(&self.base_mint);

        if !config.two_hop_enabled {
            return None;
        }

        let token_decimals = self.token_decimals?;

        if self.base_mint == NATIVE_SOL_MINT {
            arb_two_hop_v2_rejected_inc(ArbTwoHopV2RejectReason::RoundTripUnprofitable);
            return None;
        }

        let freshness = Self::quote_freshness_config(config);
        let probe = config.arb_probe_lamports;
        let owned_candidates = self.build_round_trip_candidates(
            known_pools,
            vault_balances,
            bin_arrays,
            token_decimals,
            check_ctx.pinned_pools,
        );
        let candidates: Vec<RoundTripPoolCandidate<'_>> = owned_candidates
            .iter()
            .map(|(pool, vault, bins, dex)| RoundTripPoolCandidate {
                pool,
                vault: vault.as_ref(),
                dlmm_bins: bins.as_ref(),
                dex,
            })
            .collect();

        let selection = match select_round_trip_pools(&candidates, probe, &freshness) {
            Ok(selection) => selection,
            Err(RoundTripSelectFailure::InsufficientPools(insufficient)) => {
                record_v2_insufficient_subreason(&insufficient);
                log_v2_round_trip_insufficient_pools(
                    &self.base_mint,
                    &insufficient,
                    &candidates,
                    probe,
                    &freshness,
                    token_decimals,
                );
                arb_two_hop_v2_rejected_inc(ArbTwoHopV2RejectReason::InsufficientPools);
                if let Some(collector) = v2_forensics {
                    let breakdown = self.build_v2_eligibility_breakdown(
                        known_pools,
                        vault_balances,
                        bin_arrays,
                        token_decimals,
                        probe,
                        &freshness,
                        insufficient.subreason,
                        check_ctx.pinned_pools,
                    );
                    collector.record(breakdown);
                }
                return None;
            }
            Err(RoundTripSelectFailure::QuoteStale) => {
                arb_two_hop_v2_rejected_inc(ArbTwoHopV2RejectReason::QuoteStale);
                return None;
            }
            Err(RoundTripSelectFailure::IncompatibleQuoteKind) => {
                arb_two_hop_v2_incompatible_kind_inc();
                arb_two_hop_v2_rejected_inc(ArbTwoHopV2RejectReason::IncompatibleQuoteKind);
                return None;
            }
        };

        arb_two_hop_v2_round_trip_formable_inc();

        let buy_as_of_slot = selection.buy_quote.as_of_slot;
        let sell_as_of_slot = selection.sell_quote.as_of_slot;
        let slot_delta = buy_as_of_slot.abs_diff(sell_as_of_slot);
        record_arb_quote_pair_slot_delta(buy_as_of_slot, sell_as_of_slot);
        if config.arb_max_leg_slot_delta > 0 && slot_delta > config.arb_max_leg_slot_delta {
            arb_two_hop_v2_rejected_inc(ArbTwoHopV2RejectReason::SlotDeltaExceeded);
            return None;
        }

        let buy_pool = self.pools.get(&selection.buy_pool_address)?;
        let sell_pool = self.pools.get(&selection.sell_pool_address)?;

        let max_trade_sol =
            if buy_pool.liquidity_sol > Decimal::ZERO && sell_pool.liquidity_sol > Decimal::ZERO {
                buy_pool.liquidity_sol.min(sell_pool.liquidity_sol).min(
                    Decimal::from(config.max_position_lamports) / Decimal::from(1_000_000_000u64),
                )
            } else {
                Decimal::from(config.max_position_lamports) / Decimal::from(1_000_000_000u64)
            };
        let trade_amount_lamports = (max_trade_sol * Decimal::from(1_000_000_000u64))
            .to_string()
            .parse::<u64>()
            .unwrap_or(config.max_position_lamports)
            .max(probe);

        let profit_lamports = selection.sell_quote.amount_out as i64
            - probe as i64
            - config.est_tx_cost_lamports as i64;

        let buy_liquidity_unknown =
            !buy_pool.has_reserve_data && buy_pool.liquidity_sol <= Decimal::ZERO;
        let sell_liquidity_unknown =
            !sell_pool.has_reserve_data && sell_pool.liquidity_sol <= Decimal::ZERO;
        let effective_min_profit = if buy_liquidity_unknown && sell_liquidity_unknown {
            config.min_profit_lamports * 5
        } else {
            config.min_profit_lamports
        };

        let spread_bps = if probe > 0 {
            let gross = selection.sell_quote.amount_out as i64 - probe as i64;
            (gross * 10_000 / probe as i64).clamp(i64::MIN, i64::MAX) as i32
        } else {
            0
        };

        let max_spread = if self.base_mint == USDC_MINT || self.base_mint == USDT_MINT {
            STABLECOIN_MAX_SPREAD_BPS
        } else {
            MAX_REASONABLE_SPREAD_BPS
        };

        if spread_bps < config.min_spread_bps as i32 {
            record_arb_two_hop_v2_formable_gates(
                spread_bps,
                profit_lamports,
                ArbTwoHopV2FormableGateOutcome::RejectedSpreadBelow,
            );
            arb_two_hop_v2_rejected_inc(ArbTwoHopV2RejectReason::RoundTripSpreadBelowMin);
            return None;
        }

        if spread_bps as i64 > max_spread {
            record_arb_two_hop_v2_formable_gates(
                spread_bps,
                profit_lamports,
                ArbTwoHopV2FormableGateOutcome::RejectedSpreadAbove,
            );
            arb_two_hop_v2_rejected_inc(ArbTwoHopV2RejectReason::RoundTripSpreadAboveMax);
            return None;
        }

        if profit_lamports < effective_min_profit as i64 {
            record_arb_two_hop_v2_formable_gates(
                spread_bps,
                profit_lamports,
                ArbTwoHopV2FormableGateOutcome::RejectedProfitBelow,
            );
            arb_two_hop_v2_rejected_inc(ArbTwoHopV2RejectReason::RoundTripProfitBelowMin);
            return None;
        }

        record_arb_two_hop_v2_formable_gates(
            spread_bps,
            profit_lamports,
            ArbTwoHopV2FormableGateOutcome::PassedGates,
        );

        let buy_price = Decimal::from(selection.buy_quote.amount_in)
            / Decimal::from(1_000_000_000u64)
            / (Decimal::from(selection.buy_quote.amount_out)
                / Decimal::from(10u64.pow(token_decimals as u32)));
        let sell_price = Decimal::from(selection.sell_quote.amount_out)
            / Decimal::from(1_000_000_000u64)
            / (Decimal::from(selection.sell_quote.amount_in)
                / Decimal::from(10u64.pow(token_decimals as u32)));

        arb_two_hop_opportunity_inc();

        let scale = if probe > 0 {
            trade_amount_lamports as i128 / probe as i128
        } else {
            1
        };
        let estimated_profit_lamports = (profit_lamports as i128 * scale).max(0) as u64;

        Some(ArbOpportunity {
            base_mint: self.base_mint.clone(),
            buy_dex: selection.buy_dex,
            buy_pool: selection.buy_pool_address,
            buy_price,
            sell_dex: selection.sell_dex,
            sell_pool: selection.sell_pool_address,
            sell_price,
            spread_bps: spread_bps.max(0) as u32,
            trade_amount_lamports,
            estimated_profit_lamports,
        })
    }

    /// Check for arbitrage opportunity between DEXes
    /// Returns: Option<(buy_dex, sell_dex, spread_bps, estimated_profit_lamports)>
    fn check_arbitrage(
        &self,
        config: &ArbConfig,
        known_pools: &HashSet<String>,
        vault_balances: &HashMap<String, VaultBalanceCache>,
        bin_arrays: &HashMap<String, HashMap<i64, BinArrayCache>>,
        check_ctx: &ArbCheckContext<'_>,
    ) -> Option<ArbOpportunity> {
        if config.arb_two_hop_v2_enabled {
            return self.check_arbitrage_v2(
                config,
                known_pools,
                vault_balances,
                bin_arrays,
                check_ctx.v2_forensics,
                check_ctx,
            );
        }

        let spread_warn_last = check_ctx.spread_warn_last;
        let data_quality_rejects = check_ctx.data_quality_rejects;
        let forensics = check_ctx.forensics;
        if !config.two_hop_enabled {
            debug!(
                mint = %self.base_mint,
                "2-hop arb check skipped: two_hop_enabled=false"
            );
            return None;
        }

        let mut breakdown =
            self.build_eligibility_breakdown(known_pools, vault_balances, bin_arrays);

        self.run_arb_quote_shadow(config, known_pools, vault_balances, bin_arrays, None);

        let Some(_token_decimals) = self.token_decimals else {
            debug!(
                mint = %self.base_mint,
                "Arb check: token decimals unknown — no synthetic fallback"
            );
            breakdown.reject_subreason = Some(ArbTwoHopRejectSubreason::MissingDecimals);
            self.emit_eligibility_forensics(breakdown, forensics);
            arb_two_hop_rejected_inc(ArbTwoHopRejectReason::NoComparablePrice);
            return None;
        };

        let max_age = Duration::from_millis(MAX_PRICE_AGE_MS);
        let mut best_buy: Option<(&PoolState, Decimal)> = None;
        let mut best_sell: Option<(&PoolState, Decimal)> = None;

        for row in &breakdown.pool_rows {
            if !row.known {
                if is_known_dex_label(&row.dex) {
                    debug!(
                        pool = %row.pool_address,
                        dex = %row.dex,
                        mint = %self.base_mint,
                        "Pool filtered: not in market-data MASTER cache (parse_pool_account failed)"
                    );
                }
                continue;
            }
            let Some(pool) = self.pools.get(&row.pool_address) else {
                continue;
            };
            if !row.comparable_price_present {
                continue;
            }
            if let Some(price) = row.buy_price.filter(|p| *p > Decimal::ZERO) {
                if !row.buy_plausible {
                    data_quality_rejects.fetch_add(1, Ordering::Relaxed);
                    arb_two_hop_rejected_inc(ArbTwoHopRejectReason::DataQuality);
                    continue;
                }
                if best_buy.is_none() || price < best_buy.unwrap().1 {
                    best_buy = Some((pool, price));
                }
            }
            if let Some(price) = row.sell_price.filter(|p| *p > Decimal::ZERO) {
                if !row.sell_plausible {
                    data_quality_rejects.fetch_add(1, Ordering::Relaxed);
                    arb_two_hop_rejected_inc(ArbTwoHopRejectReason::DataQuality);
                    continue;
                }
                if best_sell.is_none() || price > best_sell.unwrap().1 {
                    best_sell = Some((pool, price));
                }
            }
        }

        let eligible_pools = breakdown.eligible_pools;

        if eligible_pools < 2 {
            debug!(
                mint = %self.base_mint,
                pools = eligible_pools,
                "Arb check: insufficient pools with comparable prices"
            );
            let insufficient = determine_insufficient_subreason(&breakdown);
            breakdown.reject_subreason = Some(insufficient.into());
            record_eligibility_metrics(&breakdown);
            record_insufficient_subreason(insufficient);
            if let Some(collector) = forensics {
                collector.record(breakdown);
            }
            arb_two_hop_rejected_inc(ArbTwoHopRejectReason::InsufficientPools);
            return None;
        }

        let Some((buy_pool, buy_price)) = best_buy else {
            breakdown.reject_subreason = Some(ArbTwoHopRejectSubreason::ImplausiblePrice);
            self.emit_eligibility_forensics(breakdown, forensics);
            arb_two_hop_rejected_inc(ArbTwoHopRejectReason::DataQuality);
            return None;
        };
        let Some((sell_pool, sell_price)) = best_sell else {
            breakdown.reject_subreason = Some(ArbTwoHopRejectSubreason::ImplausiblePrice);
            self.emit_eligibility_forensics(breakdown, forensics);
            arb_two_hop_rejected_inc(ArbTwoHopRejectReason::DataQuality);
            return None;
        };

        if !is_plausible_sol_per_token_price(&self.base_mint, buy_price)
            || !is_plausible_sol_per_token_price(&self.base_mint, sell_price)
        {
            data_quality_rejects.fetch_add(1, Ordering::Relaxed);
            breakdown.reject_subreason = Some(ArbTwoHopRejectSubreason::ImplausiblePrice);
            self.emit_eligibility_forensics(breakdown, forensics);
            arb_two_hop_rejected_inc(ArbTwoHopRejectReason::DataQuality);
            return None;
        }

        let buy_vault = vault_balances.get(&buy_pool.pool_address);
        let sell_vault = vault_balances.get(&sell_pool.pool_address);
        if !is_pool_price_fresh(buy_pool, buy_vault, max_age)
            || !is_pool_price_fresh(sell_pool, sell_vault, max_age)
        {
            debug!(
                mint = %self.base_mint,
                buy_pool = %buy_pool.pool_address,
                sell_pool = %sell_pool.pool_address,
                max_age_ms = MAX_PRICE_AGE_MS,
                "Arb check rejected: stale comparable price"
            );
            if !is_pool_price_fresh(buy_pool, buy_vault, max_age) {
                record_stale_price_freshness_metrics(buy_pool, buy_vault);
            }
            if !is_pool_price_fresh(sell_pool, sell_vault, max_age) {
                record_stale_price_freshness_metrics(sell_pool, sell_vault);
            }
            breakdown.reject_subreason = Some(ArbTwoHopRejectSubreason::StalePrice);
            self.emit_eligibility_forensics(breakdown, forensics);
            arb_two_hop_rejected_inc(ArbTwoHopRejectReason::StalePrice);
            return None;
        }

        if buy_pool.dex == sell_pool.dex {
            debug!(
                mint = %self.base_mint,
                dex = %buy_pool.dex,
                "Arb check rejected: same DEX for buy/sell"
            );
            breakdown.reject_subreason = Some(ArbTwoHopRejectSubreason::SameDexOnly);
            self.emit_eligibility_forensics(breakdown, forensics);
            arb_two_hop_rejected_inc(ArbTwoHopRejectReason::SameDex);
            return None;
        }

        if buy_pool.dex == "pumpfun" || sell_pool.dex == "pumpfun" {
            debug!(
                mint = %self.base_mint,
                buy_dex = %buy_pool.dex,
                sell_dex = %sell_pool.dex,
                "Arb check rejected: pumpfun (bonding curve) has no other pools to arb against"
            );
            record_eligibility_metrics(&breakdown);
            arb_two_hop_rejected_inc(ArbTwoHopRejectReason::Pumpfun);
            return None;
        }

        if buy_price <= Decimal::ZERO {
            record_eligibility_metrics(&breakdown);
            return None;
        }

        let spread = (sell_price - buy_price) / buy_price * Decimal::from(10000);
        let spread_bps = spread.round().to_i64().unwrap_or(i64::MAX);
        if config.arb_quote_shadow_mode {
            set_arb_quote_shadow_legacy_spread_bps(spread_bps);
        }

        if self.base_mint == NATIVE_SOL_MINT {
            debug!(
                mint = %self.base_mint,
                "Arb check rejected: Native SOL trades are wrap/unwrap, not arbitrage"
            );
            record_eligibility_metrics(&breakdown);
            arb_two_hop_rejected_inc(ArbTwoHopRejectReason::NativeSol);
            return None;
        }

        let max_spread = if self.base_mint == USDC_MINT || self.base_mint == USDT_MINT {
            STABLECOIN_MAX_SPREAD_BPS
        } else {
            MAX_REASONABLE_SPREAD_BPS
        };

        if spread_bps > max_spread {
            let should_warn = {
                let mut warn_map = spread_warn_last.write();
                let emit = match warn_map.get(&self.base_mint) {
                    Some(last) => last.elapsed() >= SPREAD_TOO_LARGE_WARN_COOLDOWN,
                    None => true,
                };
                if emit {
                    warn_map.insert(self.base_mint.clone(), Instant::now());
                }
                emit
            };
            if should_warn {
                warn!(
                    mint = %self.base_mint,
                    spread_bps = spread_bps,
                    max_spread = max_spread,
                    buy_price = %buy_price,
                    sell_price = %sell_price,
                    buy_dex = %buy_pool.dex,
                    sell_dex = %sell_pool.dex,
                    "Arb check rejected: spread too large (likely data error)"
                );
            }
            record_eligibility_metrics(&breakdown);
            arb_two_hop_rejected_inc(ArbTwoHopRejectReason::SpreadTooLarge);
            return None;
        }

        if spread_bps < config.min_spread_bps as i64 {
            info!(
                mint = %self.base_mint,
                buy_dex = %buy_pool.dex,
                sell_dex = %sell_pool.dex,
                buy_price = %buy_price,
                sell_price = %sell_price,
                spread_bps = spread_bps,
                min_spread = config.min_spread_bps,
                "Arb check rejected: spread below minimum"
            );
            record_eligibility_metrics(&breakdown);
            arb_two_hop_rejected_inc(ArbTwoHopRejectReason::SpreadBelowMin);
            return None;
        }

        let max_trade_sol =
            if buy_pool.liquidity_sol > Decimal::ZERO && sell_pool.liquidity_sol > Decimal::ZERO {
                buy_pool.liquidity_sol.min(sell_pool.liquidity_sol).min(
                    Decimal::from(config.max_position_lamports) / Decimal::from(1_000_000_000u64),
                )
            } else {
                Decimal::from(config.max_position_lamports) / Decimal::from(1_000_000_000u64)
            };

        let gross_profit = max_trade_sol * (spread / Decimal::from(10000));
        let gross_profit_lamports = (gross_profit * Decimal::from(1_000_000_000u64))
            .round()
            .to_u64()
            .unwrap_or(0);

        let net_profit = gross_profit_lamports.saturating_sub(config.est_tx_cost_lamports);

        let buy_liquidity_unknown =
            !buy_pool.has_reserve_data && buy_pool.liquidity_sol <= Decimal::ZERO;
        let sell_liquidity_unknown =
            !sell_pool.has_reserve_data && sell_pool.liquidity_sol <= Decimal::ZERO;
        let effective_min_profit = if buy_liquidity_unknown && sell_liquidity_unknown {
            config.min_profit_lamports * 5
        } else {
            config.min_profit_lamports
        };

        if buy_liquidity_unknown || sell_liquidity_unknown {
            debug!(
                mint = %self.base_mint,
                buy_liquidity = %buy_pool.liquidity_sol,
                sell_liquidity = %sell_pool.liquidity_sol,
                buy_reserve = buy_pool.has_reserve_data,
                sell_reserve = sell_pool.has_reserve_data,
                net_profit = net_profit,
                required_profit = effective_min_profit,
                "Profit threshold (5× only when both sides lack reserve/liquidity data)"
            );
        }

        if net_profit < effective_min_profit {
            info!(
                mint = %self.base_mint,
                buy_dex = %buy_pool.dex,
                sell_dex = %sell_pool.dex,
                spread_bps = spread_bps,
                gross_profit = gross_profit_lamports,
                tx_cost = config.est_tx_cost_lamports,
                net_profit = net_profit,
                min_profit = config.min_profit_lamports,
                effective_min_profit = effective_min_profit,
                buy_liquidity_known = !buy_liquidity_unknown,
                sell_liquidity_known = !sell_liquidity_unknown,
                "Arb check rejected: profit below minimum"
            );
            record_eligibility_metrics(&breakdown);
            arb_two_hop_rejected_inc(ArbTwoHopRejectReason::ProfitBelowMin);
            return None;
        }

        record_eligibility_metrics(&breakdown);
        arb_two_hop_opportunity_inc();

        let trade_amount_lamports = (max_trade_sol * Decimal::from(1_000_000_000u64))
            .to_string()
            .parse::<u64>()
            .unwrap_or(config.max_position_lamports);

        Some(ArbOpportunity {
            base_mint: self.base_mint.clone(),
            buy_dex: buy_pool.dex.clone(),
            buy_pool: buy_pool.pool_address.clone(),
            buy_price,
            sell_dex: sell_pool.dex.clone(),
            sell_pool: sell_pool.pool_address.clone(),
            sell_price,
            spread_bps: spread_bps as u32,
            trade_amount_lamports,
            estimated_profit_lamports: net_profit,
        })
    }
}

#[derive(Debug, Clone)]
struct ArbOpportunity {
    base_mint: String,
    buy_dex: String,
    buy_pool: String,
    buy_price: Decimal,
    sell_dex: String,
    sell_pool: String,
    sell_price: Decimal,
    spread_bps: u32,
    trade_amount_lamports: u64,
    estimated_profit_lamports: u64,
    // NOTE: expected_token_output is calculated in create_arb_intent() using ArbContext
    // because TokenArbTracker doesn't have access to vault_balances cache.
}

// ============================================================================
// MarketEvent ingress pipeline (decoupled NATS reader + prioritized worker)
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArbEventPriority {
    High,
    Low,
}

/// True when the pair may matter for 2-hop (SOL-quoted) or multi-hop (common quote on either side).
fn is_arb_relevant_pool_pair(base_mint: &str, quote_mint: &str) -> bool {
    is_common_quote_mint(quote_mint) || is_common_quote_mint(base_mint)
}

fn market_event_pool_key(event: &MarketEvent) -> Option<String> {
    match &event.kind {
        MarketEventKind::PoolCreated { pool_address, .. } => {
            Some(format!("{pool_address}:created"))
        }
        MarketEventKind::DexPoolAccounts { pool_address, .. } => {
            Some(format!("{pool_address}:accounts"))
        }
        MarketEventKind::PoolStateUpdate { pool_address, .. } => {
            Some(format!("{pool_address}:state"))
        }
        MarketEventKind::BinArrayUpdate {
            pool_address,
            bin_array_index,
            ..
        } => Some(format!("{pool_address}:bin:{bin_array_index}")),
        MarketEventKind::Trade { pool_address, .. } => Some(format!("{pool_address}:trade")),
        _ => None,
    }
}

fn classify_market_event_priority(
    event: &MarketEvent,
    known_pools: &HashSet<String>,
    pinned_pools: &HashSet<String>,
) -> ArbEventPriority {
    match &event.kind {
        MarketEventKind::Trade { .. } => ArbEventPriority::High,
        MarketEventKind::PoolStateUpdate { pool_address, .. }
        | MarketEventKind::BinArrayUpdate { pool_address, .. } => {
            if known_pools.contains(pool_address) || pinned_pools.contains(pool_address) {
                ArbEventPriority::High
            } else {
                ArbEventPriority::Low
            }
        }
        MarketEventKind::PoolCreated { .. }
        | MarketEventKind::DexPoolAccounts { .. }
        | MarketEventKind::TokenMintInfo { .. } => ArbEventPriority::Low,
        _ => ArbEventPriority::Low,
    }
}

/// Whether a `PoolCreated` should enter the LOW coalescer (arb-relevance filter only).
fn should_enqueue_pool_created(base_mint: &str, quote_mint: &str) -> bool {
    is_arb_relevant_pool_pair(base_mint, quote_mint)
}

/// NATS-reader ingress after deserialize: liveness is already marked; returns priority to enqueue.
fn arb_market_event_ingress_priority(
    event: &MarketEvent,
    known_pools: &HashSet<String>,
    pinned_pools: &HashSet<String>,
) -> Option<ArbEventPriority> {
    if let MarketEventKind::PoolCreated {
        base_mint,
        quote_mint,
        ..
    } = &event.kind
    {
        if !should_enqueue_pool_created(base_mint, quote_mint) {
            arb_subscriber_pool_created_skipped_inc();
            return None;
        }
    }
    if !is_arb_handled_market_event(&event.kind) {
        return None;
    }
    Some(classify_market_event_priority(
        event,
        known_pools,
        pinned_pools,
    ))
}

/// Kinds that `handle_market_event` processes; all others are no-ops for arb-strategy.
fn is_arb_handled_market_event(kind: &MarketEventKind) -> bool {
    matches!(
        kind,
        MarketEventKind::PoolCreated { .. }
            | MarketEventKind::Trade { .. }
            | MarketEventKind::DexPoolAccounts { .. }
            | MarketEventKind::PoolStateUpdate { .. }
            | MarketEventKind::BinArrayUpdate { .. }
            | MarketEventKind::TokenMintInfo { .. }
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LowCoalescerInsert {
    Queued,
    Coalesced,
    Dropped,
}

/// Latest-wins coalescer for LOW MarketEvents keyed by pool (or pool+bin index).
struct ArbLowEventCoalescer {
    by_pool: HashMap<String, MarketEvent>,
}

impl ArbLowEventCoalescer {
    fn new() -> Self {
        Self {
            by_pool: HashMap::new(),
        }
    }

    fn len(&self) -> usize {
        self.by_pool.len()
    }

    fn insert(&mut self, event: MarketEvent, cap: usize) -> LowCoalescerInsert {
        let Some(key) = market_event_pool_key(&event) else {
            if self.by_pool.len() >= cap {
                arb_subscriber_low_dropped_inc();
                return LowCoalescerInsert::Dropped;
            }
            let key = format!("__anon_{}", self.by_pool.len());
            self.by_pool.insert(key, event);
            return LowCoalescerInsert::Queued;
        };

        if let Some(existing) = self.by_pool.get_mut(&key) {
            *existing = event;
            arb_subscriber_low_coalesced_inc();
            return LowCoalescerInsert::Coalesced;
        }

        if self.by_pool.len() >= cap {
            if let Some(evict_key) = self.by_pool.keys().next().cloned() {
                self.by_pool.remove(&evict_key);
                arb_subscriber_low_dropped_inc();
            }
        }

        self.by_pool.insert(key, event);
        LowCoalescerInsert::Queued
    }

    fn drain(&mut self) -> Vec<MarketEvent> {
        self.by_pool.drain().map(|(_, event)| event).collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TrackerWriteCoalescerInsert {
    Queued,
    Coalesced,
}

/// Latest-wins coalescer for tracker-write ingress (PoolStateUpdate + DexPoolAccounts).
struct ArbTrackerWriteCoalescer {
    pool_state_updates: HashMap<String, ArbTrackerWriteJob>,
    dex_pool_accounts: HashMap<String, ArbTrackerWriteJob>,
}

impl ArbTrackerWriteCoalescer {
    fn new() -> Self {
        Self {
            pool_state_updates: HashMap::new(),
            dex_pool_accounts: HashMap::new(),
        }
    }

    fn pending_len(&self) -> usize {
        self.pool_state_updates.len() + self.dex_pool_accounts.len()
    }

    #[allow(clippy::too_many_arguments)]
    fn insert_pool_state_update(
        &mut self,
        pool_address: String,
        dex: String,
        reserve_base: u64,
        reserve_quote: u64,
        update_slot: u64,
        active_id: Option<i32>,
        bin_step: Option<u16>,
        base_mint: String,
        quote_mint: String,
        cap: usize,
    ) -> TrackerWriteCoalescerInsert {
        let job = ArbTrackerWriteJob::PoolStateUpdate {
            pool_address: pool_address.clone(),
            dex,
            reserve_base,
            reserve_quote,
            update_slot,
            active_id,
            bin_step,
            base_mint,
            quote_mint,
        };
        if let Some(existing) = self.pool_state_updates.get(&pool_address) {
            if let ArbTrackerWriteJob::PoolStateUpdate {
                update_slot: existing_slot,
                ..
            } = existing
            {
                if update_slot < *existing_slot {
                    arb_tracker_write_coalesced_inc();
                    return TrackerWriteCoalescerInsert::Coalesced;
                }
            }
            self.pool_state_updates.insert(pool_address, job);
            arb_tracker_write_coalesced_inc();
            return TrackerWriteCoalescerInsert::Coalesced;
        }
        if self.pending_len() >= cap {
            if let Some(evict_key) = self.pool_state_updates.keys().next().cloned() {
                self.pool_state_updates.remove(&evict_key);
                arb_tracker_write_enqueue_dropped_inc(ArbTrackerWriteJobType::PoolStateUpdate);
            } else if let Some(evict_key) = self.dex_pool_accounts.keys().next().cloned() {
                self.dex_pool_accounts.remove(&evict_key);
                arb_tracker_write_enqueue_dropped_inc(ArbTrackerWriteJobType::DexPoolAccounts);
            }
        }
        self.pool_state_updates.insert(pool_address, job);
        TrackerWriteCoalescerInsert::Queued
    }

    fn insert_dex_pool_accounts(
        &mut self,
        pool_address: String,
        base_mint: String,
        quote_mint: String,
        accounts: Vec<String>,
        cap: usize,
    ) -> TrackerWriteCoalescerInsert {
        let job = ArbTrackerWriteJob::DexPoolAccounts {
            pool_address: pool_address.clone(),
            base_mint,
            quote_mint,
            accounts,
        };
        if let std::collections::hash_map::Entry::Occupied(mut entry) =
            self.dex_pool_accounts.entry(pool_address.clone())
        {
            entry.insert(job);
            arb_tracker_write_coalesced_inc();
            return TrackerWriteCoalescerInsert::Coalesced;
        }
        if self.pending_len() >= cap {
            if let Some(evict_key) = self.pool_state_updates.keys().next().cloned() {
                self.pool_state_updates.remove(&evict_key);
                arb_tracker_write_enqueue_dropped_inc(ArbTrackerWriteJobType::PoolStateUpdate);
            } else if let Some(evict_key) = self.dex_pool_accounts.keys().next().cloned() {
                self.dex_pool_accounts.remove(&evict_key);
                arb_tracker_write_enqueue_dropped_inc(ArbTrackerWriteJobType::DexPoolAccounts);
            }
        }
        self.dex_pool_accounts.insert(pool_address, job);
        TrackerWriteCoalescerInsert::Queued
    }

    fn flush(&mut self, handle: &ArbTrackerWriteHandle) {
        let pool_keys: Vec<String> = self.pool_state_updates.keys().cloned().collect();
        for key in pool_keys {
            let Some(job) = self.pool_state_updates.remove(&key) else {
                continue;
            };
            match handle.try_enqueue_coalescer_flush(job, ArbTrackerWriteJobType::PoolStateUpdate) {
                Ok(()) => arb_tracker_write_coalesced_flushed_inc(),
                Err(job) => {
                    self.pool_state_updates.insert(key, *job);
                    return;
                }
            }
        }
        let dex_keys: Vec<String> = self.dex_pool_accounts.keys().cloned().collect();
        for key in dex_keys {
            let Some(job) = self.dex_pool_accounts.remove(&key) else {
                continue;
            };
            match handle.try_enqueue_coalescer_flush(job, ArbTrackerWriteJobType::DexPoolAccounts) {
                Ok(()) => arb_tracker_write_coalesced_flushed_inc(),
                Err(job) => {
                    self.dex_pool_accounts.insert(key, *job);
                    return;
                }
            }
        }
    }

    #[cfg(test)]
    fn drain_pool_state_for_test(&mut self) -> Vec<ArbTrackerWriteJob> {
        self.pool_state_updates
            .drain()
            .map(|(_, job)| job)
            .collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HighEnqueueOutcome {
    Enqueued,
    DowngradedToLow,
    Dropped,
    ChannelClosed,
}

/// Non-blocking HIGH ingress: never block the NATS reader on a full queue.
fn try_enqueue_high_priority(
    high_tx: &mpsc::Sender<MarketEvent>,
    low_coalescer: &parking_lot::Mutex<ArbLowEventCoalescer>,
    low_notify: &tokio::sync::Notify,
    event: MarketEvent,
) -> HighEnqueueOutcome {
    let depth = ARB_HIGH_EVENT_QUEUE_CAP.saturating_sub(high_tx.capacity());
    arb_subscriber_high_queue_depth_set(depth as u64);

    match high_tx.try_send(event) {
        Ok(()) => HighEnqueueOutcome::Enqueued,
        Err(tokio::sync::mpsc::error::TrySendError::Full(event)) => {
            if market_event_pool_key(&event).is_some() {
                let mut coalescer = low_coalescer.lock();
                coalescer.insert(event, ARB_LOW_COALESCER_CAP);
                arb_subscriber_low_queue_depth_set(coalescer.len() as u64);
                drop(coalescer);
                low_notify.notify_one();
                arb_subscriber_high_dropped_inc();
                HighEnqueueOutcome::DowngradedToLow
            } else {
                arb_subscriber_high_dropped_inc();
                HighEnqueueOutcome::Dropped
            }
        }
        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => HighEnqueueOutcome::ChannelClosed,
    }
}

/// Off-hot-loop 2-hop detection job (Scope D).
#[derive(Debug, Clone)]
struct ArbTwoHopTradeJob {
    pool_address: String,
    mint: String,
    quote_mint: String,
    sol_amount: u64,
    token_amount: u64,
    token_decimals: u8,
    is_buy: bool,
    dex: String,
    slot: Option<u64>,
    ts_unix_ms: u64,
}

/// Jobs for the off-hot-loop 2-hop worker (trade-driven screen + bin-update rescreen).
#[derive(Debug, Clone)]
enum ArbTwoHopWorkerJob {
    Trade(ArbTwoHopTradeJob),
    Rescreen { mint: String },
}

/// Result of fast `apply_trade_to_tracker` in the single writer (check_arbitrage runs outside).
/// `vault_balances` and `bin_arrays` are snapshotted in the writer immediately after apply
/// so `check_arbitrage` sees a consistent view with `tracker_snapshot`.
#[derive(Debug, Clone)]
struct ApplyTradeResult {
    tracker_snapshot: TokenArbTracker,
    config: ArbConfig,
    mint: String,
    /// Writer-side scoped vault snapshot (asserted in tests; v2 screen uses live snapshot).
    #[allow(dead_code)]
    vault_balances: HashMap<String, VaultBalanceCache>,
    /// Retained for writer-side snapshot consistency; v2 screen reads live cache (C1f).
    #[allow(dead_code)]
    bin_arrays: HashMap<String, HashMap<i64, BinArrayCache>>,
}

/// Serialized mutation jobs for `trackers` and `vault_balances` (single writer).
enum ArbTrackerWriteJob {
    SeedPoolCache {
        update: PoolCacheUpdate,
    },
    ApplyTrade {
        job: ArbTwoHopTradeJob,
        reply: oneshot::Sender<Option<ApplyTradeResult>>,
    },
    FinalizeOpportunity {
        mint: String,
        intent_cooldown_ms: u64,
        opp: ArbOpportunity,
        reply: oneshot::Sender<Option<ArbOpportunity>>,
    },
    PoolCreated {
        pool_address: String,
        base_mint: String,
        quote_mint: String,
        dex: String,
        liquidity_sol: Decimal,
    },
    DexPoolAccounts {
        pool_address: String,
        base_mint: String,
        quote_mint: String,
        accounts: Vec<String>,
    },
    TokenMintInfo {
        mint: String,
        token_program: String,
    },
    PoolStateUpdate {
        pool_address: String,
        dex: String,
        reserve_base: u64,
        reserve_quote: u64,
        update_slot: u64,
        active_id: Option<i32>,
        bin_step: Option<u16>,
        base_mint: String,
        quote_mint: String,
    },
}

#[derive(Clone)]
struct ArbTrackerWriteHandle {
    tx: mpsc::Sender<ArbTrackerWriteJob>,
    capacity: usize,
}

impl ArbTrackerWriteHandle {
    fn record_queue_depth(&self) {
        let depth = self.capacity.saturating_sub(self.tx.capacity());
        set_arb_tracker_write_queue_depth(depth as u64);
    }

    fn try_enqueue(&self, job: ArbTrackerWriteJob, job_type: ArbTrackerWriteJobType) -> bool {
        self.record_queue_depth();
        if self.tx.try_send(job).is_err() {
            arb_tracker_write_enqueue_dropped_inc(job_type);
            false
        } else {
            true
        }
    }

    /// Coalescer flush: return the job when the queue is full so it can be re-queued.
    fn try_enqueue_coalescer_flush(
        &self,
        job: ArbTrackerWriteJob,
        job_type: ArbTrackerWriteJobType,
    ) -> Result<(), Box<ArbTrackerWriteJob>> {
        self.record_queue_depth();
        match self.tx.try_send(job) {
            Ok(()) => Ok(()),
            Err(tokio::sync::mpsc::error::TrySendError::Full(job)) => Err(Box::new(job)),
            Err(tokio::sync::mpsc::error::TrySendError::Closed(job)) => {
                let _ = job;
                arb_tracker_write_coalescer_flush_lost_inc(job_type);
                Ok(())
            }
        }
    }

    #[allow(dead_code)] // used by integration tests
    async fn enqueue(&self, job: ArbTrackerWriteJob, job_type: ArbTrackerWriteJobType) {
        self.record_queue_depth();
        if self.tx.send(job).await.is_err() {
            arb_tracker_write_enqueue_dropped_inc(job_type);
        }
    }
}

fn record_v2_meteora_pinned_sell_bin_coverage(
    tracker: &TokenArbTracker,
    bin_arrays: &HashMap<String, HashMap<i64, BinArrayCache>>,
    pinned: &HashSet<String>,
) {
    for (pool_addr, pool) in &tracker.pools {
        if pool.dex != "meteora_dlmm" || !pinned.contains(pool_addr) {
            continue;
        }
        if bin_arrays.contains_key(pool_addr) {
            inc_arb_v2_screen_meteora_sell_bin_hit_total();
        } else {
            inc_arb_v2_screen_meteora_sell_bin_miss_total();
            inc_arb_pinned_meteora_pool_bin_cache_miss_total();
        }
    }
}

async fn finalize_arb_opportunity_from_check(
    ctx: &Arc<ArbContext>,
    mint: String,
    intent_cooldown_ms: u64,
    opp: ArbOpportunity,
    slot: Option<u64>,
    ts_unix_ms: Option<u64>,
) {
    let (finalize_tx, finalize_rx) = oneshot::channel();
    if !ctx.tracker_write.try_enqueue(
        ArbTrackerWriteJob::FinalizeOpportunity {
            mint: mint.clone(),
            intent_cooldown_ms,
            opp,
            reply: finalize_tx,
        },
        ArbTrackerWriteJobType::FinalizeOpportunity,
    ) {
        debug!("Dropped FinalizeOpportunity (tracker-write queue full)");
        return;
    }
    let finalized = match finalize_rx.await {
        Ok(opp) => opp,
        Err(_) => return,
    };
    if let Some(opp) = finalized {
        ARB_TRIANGLE_OPPORTUNITIES.fetch_add(1, Ordering::Relaxed);
        info!(
            mint = %opp.base_mint,
            buy_dex = %opp.buy_dex,
            sell_dex = %opp.sell_dex,
            spread_bps = opp.spread_bps,
            profit_lamports = opp.estimated_profit_lamports,
            "🔥 Arbitrage opportunity detected (two-hop worker)"
        );
        ctx.publish_arb_trade_signal_track_pins(&opp.base_mint, &opp.buy_pool, &opp.sell_pool);
        if let Some(mut intent) = create_arb_intent(ctx, &opp) {
            if let Some(slot) = slot {
                intent.metadata.insert("slot".to_string(), slot.to_string());
            }
            if let Some(ts_unix_ms) = ts_unix_ms {
                intent
                    .metadata
                    .insert("slot_seen_at_ms".to_string(), ts_unix_ms.to_string());
            }
            publish_arb_intent(ctx, &intent).await;
        }
    }
}

fn spawn_arb_two_hop_worker(ctx: Arc<ArbContext>, mut rx: mpsc::Receiver<ArbTwoHopWorkerJob>) {
    tokio::spawn(async move {
        while let Some(job) = rx.recv().await {
            let two_hop_enabled = ctx.config.read().two_hop_enabled;
            if !two_hop_enabled {
                continue;
            }

            match job {
                ArbTwoHopWorkerJob::Rescreen { mint } => {
                    let config = ctx.config.read().clone();
                    let intent_cooldown_ms = config.intent_cooldown_ms;
                    let tracker_snapshot = {
                        let trackers = ctx.trackers.read();
                        trackers.get(&mint).cloned()
                    };
                    let Some(tracker_snapshot) = tracker_snapshot else {
                        continue;
                    };
                    if tracker_snapshot.pool_count_on_distinct_dexes() < 2 {
                        continue;
                    }
                    let had_pending = ctx
                        .v2_sell_stale_recovery_pending
                        .read()
                        .contains_key(&mint);
                    let ctx_for_check = Arc::clone(&ctx);
                    let opp = tokio::task::spawn_blocking(move || {
                        ctx_for_check.two_hop_v2_check_and_maybe_schedule_recovery(
                            &tracker_snapshot,
                            &config,
                        )
                    })
                    .await
                    .ok()
                    .flatten();
                    if opp.is_none()
                        && had_pending
                        && ctx
                            .v2_sell_stale_recovery_pending
                            .read()
                            .contains_key(&mint)
                    {
                        inc_arb_v2_sell_stale_recovery_outcome_total("rescreen_still_stale");
                    }
                    let Some(opp) = opp else {
                        continue;
                    };
                    finalize_arb_opportunity_from_check(
                        &ctx,
                        mint,
                        intent_cooldown_ms,
                        opp,
                        None,
                        None,
                    )
                    .await;
                }
                ArbTwoHopWorkerJob::Trade(job) => {
                    let (reply_tx, reply_rx) = oneshot::channel();
                    // Flush coalesced PoolStateUpdates before ApplyTrade so writer applies fresh
                    // reserves (FIFO) before apply_trade_to_tracker.
                    ctx.flush_tracker_write_coalescer();
                    if !ctx.tracker_write.try_enqueue(
                        ArbTrackerWriteJob::ApplyTrade {
                            job: job.clone(),
                            reply: reply_tx,
                        },
                        ArbTrackerWriteJobType::ApplyTrade,
                    ) {
                        debug!("Dropped ApplyTrade (tracker-write queue full)");
                        continue;
                    }
                    set_arb_two_hop_blocked_on_apply_trade(true);
                    let apply_result = match reply_rx.await {
                        Ok(Some(result)) => result,
                        _ => {
                            set_arb_two_hop_blocked_on_apply_trade(false);
                            continue;
                        }
                    };
                    set_arb_two_hop_blocked_on_apply_trade(false);
                    let intent_cooldown_ms = apply_result.config.intent_cooldown_ms;
                    let mint = apply_result.mint.clone();
                    let config = apply_result.config.clone();
                    let tracker_snapshot = apply_result.tracker_snapshot.clone();
                    let slot = job.slot;
                    let ts_unix_ms = job.ts_unix_ms;
                    let ctx_for_check = Arc::clone(&ctx);
                    // C1f: live scoped vault/bin snapshot at screen time — ApplyTrade captures
                    // bins in the writer thread and can be stale vs the global cache.
                    let opp = tokio::task::spawn_blocking(move || {
                        ctx_for_check.two_hop_v2_check_and_maybe_schedule_recovery(
                            &tracker_snapshot,
                            &config,
                        )
                    })
                    .await
                    .ok()
                    .flatten();
                    let Some(opp) = opp else {
                        continue;
                    };
                    finalize_arb_opportunity_from_check(
                        &ctx,
                        mint,
                        intent_cooldown_ms,
                        opp,
                        slot,
                        Some(ts_unix_ms),
                    )
                    .await;
                }
            }
        }
        info!("arb-strategy two-hop worker stopped");
    });
}

async fn publish_arb_intent(ctx: &ArbContext, intent: &TradeIntent) {
    if let Err(e) = ctx.jsonl_writer.write(intent) {
        error!(error = %e, "Failed to write intent to JSONL");
    }

    if let Some(ref nats) = ctx.nats {
        if let Err(e) = nats.publish(TOPIC_TRADE_INTENTS, intent).await {
            warn!(error = %e, "Failed to publish intent to NATS");
        } else {
            NATS_MESSAGES_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
            INTENTS_GENERATED_TOTAL.fetch_add(1, Ordering::Relaxed);
            ctx.intents_generated.fetch_add(1, Ordering::Relaxed);
            info!(
                intent_id = %intent.intent_id,
                mint = %intent.resources.output_mint,
                spread_bps = intent.expected_roi_bps,
                "🎯 Arb intent published"
            );
        }
    }
}

async fn process_arb_market_event(
    ctx: &ArbContext,
    event: MarketEvent,
    priority: ArbEventPriority,
) {
    MARKET_EVENTS_CONSUMED_TOTAL.fetch_add(1, Ordering::Relaxed);
    match priority {
        ArbEventPriority::High => arb_subscriber_high_processed_inc(),
        ArbEventPriority::Low => arb_subscriber_low_processed_inc(),
    }

    if let Some(intent) = handle_market_event(ctx, &event).await {
        publish_arb_intent(ctx, &intent).await;
    }
}

fn spawn_arb_market_event_pipeline(
    ctx: Arc<ArbContext>,
    mut market_sub: ironcrab::nats::NatsSubscription,
) {
    let (high_tx, mut high_rx) = mpsc::channel::<MarketEvent>(ARB_HIGH_EVENT_QUEUE_CAP);
    let low_coalescer = Arc::new(parking_lot::Mutex::new(ArbLowEventCoalescer::new()));
    let low_notify = Arc::new(tokio::sync::Notify::new());

    let reader_ctx = ctx.clone();
    let reader_coalescer = low_coalescer.clone();
    let reader_notify = low_notify.clone();
    tokio::spawn(async move {
        while let Some(nats_msg) = market_sub.next().await {
            NATS_MESSAGES_RECEIVED_TOTAL.fetch_add(1, Ordering::Relaxed);
            reader_ctx.events_received.fetch_add(1, Ordering::Relaxed);

            let event = match serde_json::from_slice::<MarketEvent>(&nats_msg.payload) {
                Ok(event) => event,
                Err(e) => {
                    warn!(error = %e, "Failed to deserialize MarketEvent");
                    continue;
                }
            };

            // Count every deserialized MarketEvent as Geyser/NATS liveness before filters/drops.
            reader_ctx.mark_market_event_seen();

            let known_pools = reader_ctx.known_pools.read().clone();
            let pinned_pools = reader_ctx.arb_pinned_pools.read().clone();
            let Some(priority) =
                arb_market_event_ingress_priority(&event, &known_pools, &pinned_pools)
            else {
                continue;
            };

            match priority {
                ArbEventPriority::High => {
                    match try_enqueue_high_priority(
                        &high_tx,
                        &reader_coalescer,
                        &reader_notify,
                        event,
                    ) {
                        HighEnqueueOutcome::ChannelClosed => {
                            warn!("arb-strategy HIGH event queue closed; stopping NATS reader");
                            break;
                        }
                        HighEnqueueOutcome::Enqueued
                        | HighEnqueueOutcome::DowngradedToLow
                        | HighEnqueueOutcome::Dropped => {}
                    }
                    arb_subscriber_high_queue_depth_set(
                        ARB_HIGH_EVENT_QUEUE_CAP.saturating_sub(high_tx.capacity()) as u64,
                    );
                }
                ArbEventPriority::Low => {
                    let mut coalescer = reader_coalescer.lock();
                    coalescer.insert(event, ARB_LOW_COALESCER_CAP);
                    arb_subscriber_low_queue_depth_set(coalescer.len() as u64);
                    drop(coalescer);
                    reader_notify.notify_one();
                }
            }
        }
        info!("arb-strategy NATS MarketEvent reader stopped");
    });

    let worker_ctx = ctx.clone();
    let worker_coalescer = low_coalescer.clone();
    let worker_notify = low_notify.clone();
    tokio::spawn(async move {
        let mut low_interval = tokio::time::interval(Duration::from_millis(2));
        low_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut shutting_down = false;
        loop {
            while let Ok(event) = high_rx.try_recv() {
                process_arb_market_event(&worker_ctx, event, ArbEventPriority::High).await;
            }

            if !shutting_down {
                tokio::select! {
                    biased;
                    maybe_high = high_rx.recv() => {
                        match maybe_high {
                            Some(event) => {
                                process_arb_market_event(&worker_ctx, event, ArbEventPriority::High).await;
                            }
                            None => shutting_down = true,
                        }
                    }
                    _ = worker_notify.notified() => {}
                    _ = low_interval.tick() => {}
                }
            }

            let low_batch = {
                let mut coalescer = worker_coalescer.lock();
                let batch = coalescer.drain();
                arb_subscriber_low_queue_depth_set(coalescer.len() as u64);
                batch
            };
            for event in low_batch {
                while let Ok(high_event) = high_rx.try_recv() {
                    process_arb_market_event(&worker_ctx, high_event, ArbEventPriority::High).await;
                }
                process_arb_market_event(&worker_ctx, event, ArbEventPriority::Low).await;
            }

            worker_ctx.flush_tracker_write_coalescer();

            if shutting_down {
                break;
            }
        }
        info!("arb-strategy MarketEvent worker stopped");
    });
}

// ============================================================================
// Runtime Context
// ============================================================================

/// DexPoolAccounts side-map entry: (base_mint, quote_mint, accounts).
type PendingDexPoolAccountsEntry = (String, String, Vec<String>);

struct ArbContext {
    run_id: String,
    config: RwLock<ArbConfig>,
    nats: Option<NatsClient>,
    jsonl_writer: JsonlWriter,

    /// Token trackers for cross-DEX arbitrage
    trackers: RwLock<HashMap<String, TokenArbTracker>>,

    // Metrics
    events_received: AtomicU64,
    pools_tracked: AtomicU64,
    opportunities_found: AtomicU64,
    intents_generated: AtomicU64,
    intent_counter: AtomicU64,

    // Data quality metrics
    zero_amount_trades: AtomicU64,
    data_quality_rejects: AtomicU64,

    // =========================================================================
    // Geyser Connection Health
    // =========================================================================
    /// Last time we received any MarketEvent from NATS (market-data → Geyser).
    /// Used to detect Geyser connection failures. If no events for 30s, assume broken.
    /// This is different from per-pool staleness: inactive pools are still "fresh" data.
    last_market_event: RwLock<Instant>,

    // =========================================================================
    // Geyser-based Pool State Cache (from PoolStateUpdate / BinArrayUpdate)
    // =========================================================================
    /// Vault balances cache: pool_address → (reserve_base, reserve_quote, update_slot)
    /// Updated from PoolStateUpdate events (via market-data Geyser subscription)
    vault_balances: RwLock<HashMap<String, VaultBalanceCache>>,

    /// Meteora DLMM Bin Arrays cache: pool_address → bin_array_index → bins
    /// Updated from BinArrayUpdate events (via market-data Geyser subscription)
    bin_arrays: RwLock<HashMap<String, HashMap<i64, BinArrayCache>>>,

    // =========================================================================
    // SLAVE Cache: Known Pools from market-data MASTER (Single Source of Truth)
    // =========================================================================
    /// SLAVE LivePoolCache — same JetStream SSOT apply path as execution-engine.
    live_pool_cache: SharedLivePoolCache,

    /// Set of pool addresses that exist in market-data MASTER LivePoolCache.
    /// Updated from every parsable PoolCacheUpdate (PoolDiscovered, BalanceUpdated, PoolRemoved).
    /// ONLY generate intents for pools in this set - ensures execution-engine can execute them.
    known_pools: RwLock<HashSet<String>>,

    // =========================================================================
    // Multi-Hop Arbitrage (Shadow Mode by default)
    // =========================================================================
    /// Multi-hop arbitrage engine for N-hop cycle detection.
    /// Disabled by default (shadow_mode=true). See docs/MULTI_HOP_ARBITRAGE.md
    multi_hop: Arc<MultiHopArbitrage>,

    /// Per-mint last WARN time for "spread too large" deduplication.
    spread_too_large_warn_last: RwLock<HashMap<String, Instant>>,

    /// Bounded 2-hop eligibility forensics (rate-limited snapshots).
    eligibility_forensics: ArbEligibilityForensics,
    /// Bounded v2 round-trip eligibility forensics (rate-limited snapshots).
    v2_eligibility_forensics: ArbV2EligibilityForensics,

    /// Phase 3: pools published as active via `TOPIC_ARB_TRACK_REQUESTS`.
    arb_pinned_pools: RwLock<HashSet<String>>,
    /// Mints with at least one pool in the authoritative selected pin set (I-ARB-10b).
    arb_selected_mints: RwLock<HashSet<String>>,
    /// Bounded per-mint trade-signal pairs (mint -> buy/sell + recency).
    arb_trade_signal_pairs: RwLock<HashMap<String, ArbTradeSignalPair>>,
    /// LRU order for trade-signal pair eviction (oldest at front).
    arb_trade_signal_pair_order: RwLock<Vec<String>>,
    /// Cached per-mint selection snapshots (bounded LRU, selection worker only).
    arb_track_mint_snapshots: RwLock<ArbTrackMintSnapshotCache>,
    arb_track_selection: ArbTrackSelectionHandle,
    /// Phase 3: count of track_requests publishes (heartbeat).
    arb_track_published: AtomicU64,
    /// Scope D: enqueue-only sender for off-hot-loop 2-hop detection.
    two_hop_tx: mpsc::Sender<ArbTwoHopWorkerJob>,
    /// Single-writer channel for `trackers` / `vault_balances` mutations.
    tracker_write: ArbTrackerWriteHandle,
    /// Latest-wins coalescer for PoolStateUpdate / DexPoolAccounts before tracker-write queue.
    tracker_write_coalescer: parking_lot::Mutex<ArbTrackerWriteCoalescer>,
    /// C1h5: pending sell-leg stale recovery per mint (rate limit + buy/sell pair).
    v2_sell_stale_recovery_pending: RwLock<HashMap<String, V2SellStaleRecoveryPending>>,
    /// Global pool_address → DexPoolAccounts (bounded, survives cross-mint tracker gaps).
    pool_accounts_index: RwLock<HashMap<String, Vec<String>>>,
    /// DexPoolAccounts received before mint tracker exists (bounded side-map).
    pending_pool_accounts: RwLock<HashMap<String, PendingDexPoolAccountsEntry>>,
}

/// Cached vault balances from PoolStateUpdate events
#[allow(dead_code)]
#[derive(Debug, Clone)]
struct VaultBalanceCache {
    reserve_base: u64,
    reserve_quote: u64,
    update_slot: u64,
    // DLMM-specific (Option D: Bin Array Traversierung)
    active_id: Option<i32>,
    bin_step: Option<u16>,
    /// Wall-clock freshness for reserve-based price (Geyser PoolStateUpdate or SLAVE seed).
    updated_at: Instant,
    /// Meteora DLMM: on-chain token X is SOL (bins stay in native X/Y layout).
    dlmm_sol_is_x: bool,
    /// Meteora DLMM SSOT: on-chain `token_x_mint` (lb_pair order, not SOL-quoted remap).
    dlmm_token_x_mint: Option<String>,
}

/// Cached bin array data from BinArrayUpdate events
#[allow(dead_code)]
#[derive(Debug, Clone)]
struct BinArrayCache {
    bins: Vec<BinData>,
    update_slot: u64,
}

impl ArbContext {
    fn next_intent_id(&self) -> String {
        let n = self.intent_counter.fetch_add(1, Ordering::Relaxed);
        format!("arb-{}-{:06}", &self.run_id[..8], n)
    }

    /// Record that a MarketEvent was received on the NATS wire (Geyser liveness).
    fn mark_market_event_seen(&self) {
        *self.last_market_event.write() = Instant::now();
    }

    #[allow(clippy::too_many_arguments)]
    fn coalesce_pool_state_update(
        &self,
        pool_address: String,
        dex: String,
        reserve_base: u64,
        reserve_quote: u64,
        update_slot: u64,
        active_id: Option<i32>,
        bin_step: Option<u16>,
        base_mint: String,
        quote_mint: String,
    ) {
        let mut coalescer = self.tracker_write_coalescer.lock();
        coalescer.insert_pool_state_update(
            pool_address,
            dex,
            reserve_base,
            reserve_quote,
            update_slot,
            active_id,
            bin_step,
            base_mint,
            quote_mint,
            ARB_TRACKER_WRITE_COALESCER_CAP,
        );
        set_arb_tracker_write_coalescer_pending(coalescer.pending_len() as u64);
    }

    fn coalesce_dex_pool_accounts(
        &self,
        pool_address: String,
        base_mint: String,
        quote_mint: String,
        accounts: Vec<String>,
    ) {
        let mut coalescer = self.tracker_write_coalescer.lock();
        coalescer.insert_dex_pool_accounts(
            pool_address,
            base_mint,
            quote_mint,
            accounts,
            ARB_TRACKER_WRITE_COALESCER_CAP,
        );
        set_arb_tracker_write_coalescer_pending(coalescer.pending_len() as u64);
    }

    fn flush_tracker_write_coalescer(&self) {
        let mut coalescer = self.tracker_write_coalescer.lock();
        coalescer.flush(&self.tracker_write);
        set_arb_tracker_write_coalescer_pending(coalescer.pending_len() as u64);
    }

    /// Check if the Geyser connection is healthy.
    /// Returns true if we received a MarketEvent within GEYSER_CONNECTION_TIMEOUT_SECS.
    ///
    /// This is different from per-pool staleness:
    /// - Geyser streams directly from validator, no updates = pool inactive (data IS current)
    /// - If NO events at all, Geyser/NATS connection is broken
    fn is_geyser_connection_healthy(&self) -> bool {
        let last_event = *self.last_market_event.read();
        last_event.elapsed().as_secs() < GEYSER_CONNECTION_TIMEOUT_SECS
    }

    /// P1: Apply config update from control-plane (Runtime Configuration via UI)
    fn apply_config_update(&self, update: &ConfigUpdate) -> ConfigUpdateResponse {
        let mut config = self.config.write();
        let mut applied = Vec::new();
        let mut rejected = Vec::new();

        for (key, value) in &update.config {
            match key.as_str() {
                "min_spread_bps" => {
                    if let Some(v) = value.as_u64() {
                        if v > 0 && v <= 100_000 {
                            config.min_spread_bps = v as u32;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be 1-100000".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "min_profit_lamports" => {
                    if let Some(v) = value.as_u64() {
                        if v > 0 {
                            config.min_profit_lamports = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be > 0".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "max_position_lamports" => {
                    if let Some(v) = value.as_u64() {
                        if v > 0 {
                            config.max_position_lamports = v;
                            sync_arb_probe_to_max_position(&mut config);
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be > 0".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "est_tx_cost_lamports" => {
                    if let Some(v) = value.as_u64() {
                        if v > 0 {
                            config.est_tx_cost_lamports = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be > 0".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "max_slippage_bps" => {
                    if let Some(v) = value.as_u64() {
                        if v > 0 && v <= 10_000 {
                            config.max_slippage_bps = v as u32;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be 1-10000".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "intent_cooldown_ms" => {
                    if let Some(v) = value.as_u64() {
                        if v <= 3_600_000 {
                            config.intent_cooldown_ms = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be <= 3600000".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "intent_ttl_ms" => {
                    if let Some(v) = value.as_u64() {
                        if v > 0 && v <= 60_000 {
                            config.intent_ttl_ms = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be 1-60000".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                // =====================================================================
                // 2-HOP Arbitrage Config (hot-reload)
                // =====================================================================
                "two_hop_enabled" => {
                    if let Some(v) = value.as_bool() {
                        config.two_hop_enabled = v;
                        applied.push(key.clone());
                        info!(key = %key, new_value = %v, "Config updated");
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected bool".to_string()));
                    }
                }
                "arb_track_baseline_max_pools" => {
                    if let Some(v) = value.as_u64() {
                        if v > 0 && v <= 10_000 {
                            config.arb_track_baseline_max_pools = v as usize;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be 1-10000".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "arb_track_reconcile_interval_secs" => {
                    if let Some(v) = value.as_u64() {
                        if (10..=3_600).contains(&v) {
                            config.arb_track_reconcile_interval_secs = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be 10-3600".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "arb_quote_shadow_mode" => {
                    if let Some(v) = value.as_bool() {
                        config.arb_quote_shadow_mode = v;
                        applied.push(key.clone());
                        info!(key = %key, new_value = %v, "Config updated");
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected bool".to_string()));
                    }
                }
                "arb_two_hop_v2_enabled" => {
                    if let Some(v) = value.as_bool() {
                        config.arb_two_hop_v2_enabled = v;
                        applied.push(key.clone());
                        info!(key = %key, new_value = %v, "Config updated");
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected bool".to_string()));
                    }
                }
                "arb_probe_lamports" => {
                    if let Some(v) = value.as_u64() {
                        if v > 0 {
                            config.arb_probe_lamports = v;
                            config.arb_probe_follows_max_position = false;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be > 0".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "arb_probe_follows_max_position" => {
                    if let Some(v) = value.as_bool() {
                        config.arb_probe_follows_max_position = v;
                        if v {
                            sync_arb_probe_to_max_position(&mut config);
                        }
                        applied.push(key.clone());
                        info!(key = %key, new_value = %v, "Config updated");
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected bool".to_string()));
                    }
                }
                "arb_quote_trade_ttl_ms" => {
                    if let Some(v) = value.as_u64() {
                        if v > 0 {
                            config.arb_quote_trade_ttl_ms = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be > 0".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "arb_quote_state_ttl_ms" => {
                    if let Some(v) = value.as_u64() {
                        if v > 0 {
                            config.arb_quote_state_ttl_ms = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be > 0".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "arb_max_leg_slot_delta" => {
                    if let Some(v) = value.as_u64() {
                        if v <= 32 {
                            config.arb_max_leg_slot_delta = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected
                                .push((key.clone(), "Must be 0-32 (0 disables gate)".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                // Skip multi_hop_* keys here - they're handled in the second loop below
                k if k.starts_with("multi_hop_") => {}
                _ => rejected.push((key.clone(), format!("Unknown config key: {}", key))),
            }
        }

        // =====================================================================
        // Multi-Hop Config (applied to self.multi_hop, not ArbConfig)
        // =====================================================================
        drop(config); // Release ArbConfig lock before updating multi_hop

        let mut multi_hop_applied = Vec::new();
        let mut multi_hop_rejected = Vec::new();
        let mut multi_hop_config = self.multi_hop.get_config();

        for (key, value) in &update.config {
            match key.as_str() {
                "multi_hop_enabled" => {
                    if let Some(v) = value.as_bool() {
                        multi_hop_config.enabled = v;
                        multi_hop_applied.push(key.clone());
                        info!(key = %key, new_value = %v, "Multi-hop config updated");
                    } else {
                        multi_hop_rejected
                            .push((key.clone(), "Invalid type, expected bool".to_string()));
                    }
                }
                "multi_hop_shadow_mode" => {
                    if let Some(v) = value.as_bool() {
                        multi_hop_config.shadow_mode = v;
                        multi_hop_applied.push(key.clone());
                        info!(key = %key, new_value = %v, "Multi-hop config updated");
                    } else {
                        multi_hop_rejected
                            .push((key.clone(), "Invalid type, expected bool".to_string()));
                    }
                }
                "multi_hop_max_hops" => {
                    if let Some(v) = value.as_u64() {
                        if (3..=5).contains(&v) {
                            multi_hop_config.max_hops = v as usize;
                            multi_hop_applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Multi-hop config updated");
                        } else {
                            multi_hop_rejected.push((key.clone(), "Must be 3-5".to_string()));
                        }
                    } else {
                        multi_hop_rejected
                            .push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "multi_hop_beam_width" => {
                    if let Some(v) = value.as_u64() {
                        if (10..=200).contains(&v) {
                            multi_hop_config.beam_width = v as usize;
                            multi_hop_applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Multi-hop config updated");
                        } else {
                            multi_hop_rejected.push((key.clone(), "Must be 10-200".to_string()));
                        }
                    } else {
                        multi_hop_rejected
                            .push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "multi_hop_min_profit_bps" => {
                    if let Some(v) = value.as_i64() {
                        if (1..=1000).contains(&v) {
                            multi_hop_config.min_profit_bps = v as i32;
                            multi_hop_applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Multi-hop config updated");
                        } else {
                            multi_hop_rejected.push((key.clone(), "Must be 1-1000".to_string()));
                        }
                    } else {
                        multi_hop_rejected
                            .push((key.clone(), "Invalid type, expected i64".to_string()));
                    }
                }
                "multi_hop_max_cycles" => {
                    if let Some(v) = value.as_u64() {
                        if (1..=20).contains(&v) {
                            multi_hop_config.max_cycles = v as usize;
                            multi_hop_applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Multi-hop config updated");
                        } else {
                            multi_hop_rejected.push((key.clone(), "Must be 1-20".to_string()));
                        }
                    } else {
                        multi_hop_rejected
                            .push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "multi_hop_pool_alternatives" => {
                    if let Some(v) = value.as_u64() {
                        if (1..=10).contains(&v) {
                            multi_hop_config.pool_alternatives = v as usize;
                            multi_hop_applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Multi-hop config updated");
                        } else {
                            multi_hop_rejected.push((key.clone(), "Must be 1-10".to_string()));
                        }
                    } else {
                        multi_hop_rejected
                            .push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "multi_hop_min_liquidity_usd" => {
                    if let Some(v) = value.as_f64() {
                        if (0.0..=1_000_000.0).contains(&v) {
                            multi_hop_config.min_liquidity_usd = v;
                            multi_hop_applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Multi-hop config updated");
                        } else {
                            multi_hop_rejected.push((key.clone(), "Must be 0-1000000".to_string()));
                        }
                    } else {
                        multi_hop_rejected
                            .push((key.clone(), "Invalid type, expected f64".to_string()));
                    }
                }
                "multi_hop_input_lamports" => {
                    if let Some(v) = value.as_u64() {
                        if (1_000_000..=10_000_000_000).contains(&v) {
                            multi_hop_config.input_amount_lamports = v;
                            multi_hop_applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Multi-hop config updated");
                        } else {
                            multi_hop_rejected
                                .push((key.clone(), "Must be 1M-10B lamports".to_string()));
                        }
                    } else {
                        multi_hop_rejected
                            .push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "multi_hop_min_price_change_bps" => {
                    if let Some(v) = value.as_u64() {
                        if (1..=1000).contains(&v) {
                            multi_hop_config.min_price_change_bps = v as u32;
                            multi_hop_applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Multi-hop config updated");
                        } else {
                            multi_hop_rejected.push((key.clone(), "Must be 1-1000".to_string()));
                        }
                    } else {
                        multi_hop_rejected
                            .push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "multi_hop_token_cooldown_ms" => {
                    if let Some(v) = value.as_u64() {
                        if v <= 60_000 {
                            multi_hop_config.token_cooldown_ms = v;
                            multi_hop_applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Multi-hop config updated");
                        } else {
                            multi_hop_rejected.push((key.clone(), "Must be <= 60000".to_string()));
                        }
                    } else {
                        multi_hop_rejected
                            .push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                _ => {} // Ignore keys not related to multi-hop (already handled above)
            }
        }

        // Apply multi-hop config if any changes were made
        if !multi_hop_applied.is_empty() {
            self.multi_hop.update_config(multi_hop_config);
        }

        // Merge results
        applied.extend(multi_hop_applied);
        rejected.extend(multi_hop_rejected);

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

    /// Sync `pools_tracked` counter and Prometheus gauge from tracker state.
    fn sync_pools_tracked_gauge(&self) {
        let trackers = self.trackers.read();
        let total: usize = trackers.values().map(|t| t.pools.len()).sum();
        self.pools_tracked.store(total as u64, Ordering::Relaxed);
        POOLS_TRACKED_GAUGE.store(total as u64, Ordering::Relaxed);
    }

    fn upsert_pool_accounts_index(&self, pool_address: &str, accounts: &[String]) {
        let mut index = self.pool_accounts_index.write();
        if index.len() >= POOL_ACCOUNTS_INDEX_CAP && !index.contains_key(pool_address) {
            if let Some(evict_key) = index.keys().next().cloned() {
                index.remove(&evict_key);
            }
        }
        index.insert(pool_address.to_string(), accounts.to_vec());
    }

    /// Store DexPoolAccounts in index + mint trackers; pending buffer when no tracker exists yet.
    fn store_dex_pool_accounts(
        &self,
        pool_address: &str,
        base_mint: &str,
        quote_mint: &str,
        accounts: Vec<String>,
        backfill_source: Option<ArbPoolAccountsBackfillSource>,
    ) {
        if let Some(source) = backfill_source {
            inc_arb_pool_accounts_backfill(source);
        }
        self.upsert_pool_accounts_index(pool_address, &accounts);

        let mut trackers = self.trackers.write();
        let mints_to_store = [base_mint, quote_mint];
        let mut stored_in_tracker = false;
        for mint in &mints_to_store {
            if let Some(tracker) = trackers.get_mut(*mint) {
                tracker.set_pool_accounts(pool_address, accounts.clone());
                stored_in_tracker = true;
                debug!(
                    pool = %pool_address,
                    mint = %mint,
                    accounts_len = accounts.len(),
                    "DexPoolAccounts cached in tracker"
                );
            }
        }

        if stored_in_tracker {
            self.pending_pool_accounts.write().remove(pool_address);
            return;
        }

        let mut pending = self.pending_pool_accounts.write();
        if pending.len() >= PENDING_POOL_ACCOUNTS_CAP && !pending.contains_key(pool_address) {
            if let Some(evict_key) = pending.keys().next().cloned() {
                pending.remove(&evict_key);
            }
        }
        pending.insert(
            pool_address.to_string(),
            (base_mint.to_string(), quote_mint.to_string(), accounts),
        );
        debug!(
            pool = %pool_address,
            base_mint = %base_mint,
            quote_mint = %quote_mint,
            "DexPoolAccounts buffered pending tracker"
        );
    }

    /// Apply pending DexPoolAccounts when a tracker row is created for a pool pair.
    fn apply_pending_pool_accounts(&self, pool_address: &str, base_mint: &str, quote_mint: &str) {
        let pending_entry = self.pending_pool_accounts.read().get(pool_address).cloned();
        if let Some((pending_base, pending_quote, accounts)) = pending_entry {
            let matches_orientation = pending_base == base_mint && pending_quote == quote_mint
                || pending_base == quote_mint && pending_quote == base_mint;
            if matches_orientation {
                self.store_dex_pool_accounts(
                    pool_address,
                    &pending_base,
                    &pending_quote,
                    accounts,
                    Some(ArbPoolAccountsBackfillSource::PendingBuffer),
                );
            }
        }
    }

    fn resolve_pool_accounts(&self, pool_address: &str, prefer_mint: &str) -> Option<Vec<String>> {
        let trackers = self.trackers.read();
        if let Some(tracker) = trackers.get(prefer_mint) {
            if let Some(accounts) = tracker.get_pool_accounts(pool_address) {
                return Some(accounts.clone());
            }
        }
        for (mint, tracker) in trackers.iter() {
            if mint != prefer_mint {
                if let Some(accounts) = tracker.get_pool_accounts(pool_address) {
                    inc_arb_pool_accounts_backfill(ArbPoolAccountsBackfillSource::CrossMintLookup);
                    return Some(accounts.clone());
                }
            }
        }
        drop(trackers);

        if let Some(accounts) = self.pool_accounts_index.read().get(pool_address) {
            inc_arb_pool_accounts_backfill(ArbPoolAccountsBackfillSource::Index);
            return Some(accounts.clone());
        }

        if let Some((_, _, accounts)) = self.pending_pool_accounts.read().get(pool_address) {
            inc_arb_pool_accounts_backfill(ArbPoolAccountsBackfillSource::PendingBuffer);
            return Some(accounts.clone());
        }

        if let Ok(pool_pk) = Pubkey::from_str(pool_address) {
            if let Some((state, _, _)) = self.live_pool_cache.get_with_metadata(&pool_pk) {
                if let Some(accounts) = dex_pool_accounts_from_cached_state(&pool_pk, &state) {
                    if let Some((base_mint, quote_mint)) = pool_pair_mints_from_cached_state(&state)
                    {
                        self.store_dex_pool_accounts(
                            pool_address,
                            &base_mint,
                            &quote_mint,
                            accounts.clone(),
                            Some(ArbPoolAccountsBackfillSource::LiveCache),
                        );
                    }
                    return Some(accounts);
                }
            }
        }

        None
    }

    /// Update or create pool state from PoolCreated event.
    /// Returns the token mint when the tracker is multi-DEX after the update.
    fn handle_pool_created(
        &self,
        pool_address: &str,
        base_mint: &str,
        quote_mint: &str,
        dex: &str,
        liquidity_sol: Decimal,
    ) -> Option<String> {
        let token_mint = arb_tracked_token_mint(base_mint, quote_mint)?;
        if token_mint == NATIVE_SOL_MINT || is_stablecoin_mint(token_mint) {
            return None;
        }

        let mut trackers = self.trackers.write();
        let tracker = trackers
            .entry(token_mint.to_string())
            .or_insert_with(|| TokenArbTracker::new(token_mint));

        if tracker.pools.contains_key(pool_address) {
            return None;
        }

        let pool_state = PoolState {
            pool_address: pool_address.to_string(),
            dex: dex.to_string(),
            last_price: None,
            trade_price_buy: None,
            trade_price_sell: None,
            liquidity_sol,
            has_reserve_data: false,
            last_update: Instant::now(),
            trade_count: 0,
            dex_accounts: None, // Will be filled by DexPoolAccounts event
        };

        tracker.upsert_pool(pool_state);
        let is_multi_dex = tracker.pool_count_on_distinct_dexes() >= 2;
        drop(trackers);
        self.apply_pending_pool_accounts(pool_address, base_mint, quote_mint);
        self.sync_pools_tracked_gauge();
        debug!(
            mint = %token_mint,
            dex = %dex,
            pool = %pool_address,
            liquidity = %liquidity_sol,
            "Pool added to arb tracker from PoolCreated"
        );
        if is_multi_dex {
            Some(token_mint.to_string())
        } else {
            None
        }
    }

    /// Store DEX pool accounts from DexPoolAccounts event
    /// These are passed through to execution-engine in TradeIntent.resources.accounts
    /// so execution-engine needs ZERO RPC calls.
    ///
    /// CRITICAL: We store accounts under BOTH base_mint AND quote_mint keys because:
    /// - Orca pools have base_mint=WSOL, quote_mint=TOKEN
    /// - But TokenArbTracker is indexed by TOKEN mint
    /// - Without storing under both keys, Orca pools would never be found!
    fn handle_dex_pool_accounts(
        &self,
        pool_address: &str,
        base_mint: &str,
        quote_mint: &str,
        accounts: Vec<String>,
    ) {
        self.store_dex_pool_accounts(pool_address, base_mint, quote_mint, accounts, None);
    }

    /// Handle TokenMintInfo event - cache token program (SPL Token or Token-2022)
    /// This is passed through to execution-engine in TradeIntent.resources.token_program
    /// so execution-engine can create ATAs with the correct program.
    fn handle_token_mint_info(&self, mint: &str, token_program: &str) {
        let mut trackers = self.trackers.write();
        if let Some(tracker) = trackers.get_mut(mint) {
            tracker.set_token_program(token_program);
            debug!(
                mint = %mint,
                token_program = %token_program,
                is_token_2022 = token_program.contains("TokenzQd"),
                "TokenMintInfo: token program cached in tracker"
            );
        } else {
            // Create tracker if it doesn't exist yet (we may receive TokenMintInfo before pool events)
            let mut tracker = TokenArbTracker::new(mint);
            tracker.set_token_program(token_program);
            trackers.insert(mint.to_string(), tracker);
            debug!(
                mint = %mint,
                token_program = %token_program,
                "TokenMintInfo: new tracker created with token program"
            );
        }
    }

    /// Seed all arb-relevant pools after JetStream bootstrap.
    fn seed_all_trackers_from_live_pool_cache(&self) -> ArbWarmupBootstrapStats {
        let mut trackers = self.trackers.write();
        let mut vault_balances = self.vault_balances.write();
        let stats = seed_all_trackers_from_live_pool_cache(
            &self.live_pool_cache,
            &mut trackers,
            &mut vault_balances,
        );
        drop(trackers);
        drop(vault_balances);
        self.sync_pools_tracked_gauge();
        stats
    }

    /// Incremental tracker seed when a pool is discovered or balances update.
    fn seed_trackers_for_pool_cache_update(&self, update: &PoolCacheUpdate) -> bool {
        if matches!(update.update_type, PoolCacheUpdateType::PoolRemoved) {
            return false;
        }
        let Some(mint) = arb_tracked_token_mint(&update.base_mint, &update.quote_mint) else {
            return false;
        };
        if mint == NATIVE_SOL_MINT || is_stablecoin_mint(mint) {
            return false;
        }
        let trackers_wait = Instant::now();
        let mut trackers = self.trackers.write();
        record_arb_writer_lock_wait(ArbWriterLockKind::TrackersWrite, trackers_wait.elapsed());
        let vault_wait = Instant::now();
        let mut vault_balances = self.vault_balances.write();
        record_arb_writer_lock_wait(ArbWriterLockKind::VaultBalancesWrite, vault_wait.elapsed());
        let seeded = seed_token_tracker_from_live_pool_cache(
            mint,
            &self.live_pool_cache,
            &mut trackers,
            &mut vault_balances,
            Some(&update.pool_address),
        );
        drop(trackers);
        drop(vault_balances);
        if seeded > 0 {
            self.sync_pools_tracked_gauge();
            debug!(
                mint = %mint,
                pools_seeded = seeded,
                pool = %update.pool_address,
                "Tracker seeded from SLAVE LivePoolCache"
            );
            true
        } else {
            false
        }
    }

    /// P1: seed global `vault_balances` from SLAVE cache on JetStream PoolCacheUpdate for pinned pools.
    fn consume_vault_seed_from_pool_cache_update(&self, update: &PoolCacheUpdate) -> bool {
        if matches!(update.update_type, PoolCacheUpdateType::PoolRemoved) {
            return false;
        }
        let pool_address = update.pool_address.as_str();
        let is_pinned = self.arb_pinned_pools.read().contains(pool_address);
        let mints_with_pool: Vec<String> = if is_pinned {
            let selected = self.arb_selected_mints.read();
            let trackers = self.trackers.read();
            trackers
                .iter()
                .filter(|(mint, tracker)| {
                    selected.contains(mint.as_str()) && tracker.pools.contains_key(pool_address)
                })
                .map(|(mint, _)| mint.clone())
                .collect()
        } else {
            Vec::new()
        };
        if !is_pinned {
            return false;
        }
        let pin_class = "pin";
        let vault_wait = Instant::now();
        let mut vault_cache = self.vault_balances.write();
        record_arb_writer_lock_wait(ArbWriterLockKind::VaultBalancesWrite, vault_wait.elapsed());
        let seeded = if try_refresh_vault_from_live_cache(
            pool_address,
            &self.live_pool_cache,
            &mut vault_cache,
            pin_class,
        ) {
            inc_arb_vault_live_snapshot_refreshed_total();
            true
        } else if try_seed_vault_from_live_cache(
            pool_address,
            &self.live_pool_cache,
            &mut vault_cache,
            pin_class,
        ) {
            inc_arb_vault_live_snapshot_seeded_total();
            true
        } else {
            false
        };
        drop(vault_cache);
        if seeded {
            inc_arb_vault_seed_from_cache_ok_total();
            self.schedule_arb_vault_rescreen_for_mints(pool_address, &mints_with_pool);
        } else {
            inc_arb_vault_seed_from_cache_miss_total();
        }
        seeded
    }

    /// Handle PoolStateUpdate event - cache vault balances from Geyser
    /// This eliminates RPC calls to fetch vault balances during quoting.
    #[allow(clippy::too_many_arguments)]
    fn handle_pool_state_update(
        &self,
        pool_address: &str,
        dex: &str,
        reserve_base: u64,
        reserve_quote: u64,
        update_slot: u64,
        active_id: Option<i32>,
        bin_step: Option<u16>,
        base_mint: &str,
        quote_mint: &str,
    ) {
        // USDC/USDT quote reserves must not land in vault_balances: eligibility treats
        // reserve_quote as SOL lamports in reserve_mid_sol_per_token (I-15).
        if base_mint != NATIVE_SOL_MINT && quote_mint != NATIVE_SOL_MINT {
            return;
        }
        let (reserve_base, reserve_quote) =
            sol_quoted_vault_reserves(base_mint, quote_mint, reserve_base, reserve_quote);
        let vault_wait = Instant::now();
        let mut cache = self.vault_balances.write();
        record_arb_writer_lock_wait(ArbWriterLockKind::VaultBalancesWrite, vault_wait.elapsed());
        let should_update_vault = match cache.get(pool_address) {
            Some(existing) => update_slot >= existing.update_slot,
            None => true,
        };
        if !should_update_vault {
            return;
        }
        let is_new = !cache.contains_key(pool_address);
        let dlmm_token_x_mint = if dex == "meteora_dlmm" {
            resolve_dlmm_token_x_mint_for_pool_update(pool_address, &cache, &self.live_pool_cache)
        } else {
            cache
                .get(pool_address)
                .and_then(|v| v.dlmm_token_x_mint.clone())
        };
        let dlmm_sol_is_x = dlmm_token_x_mint.as_deref() == Some(NATIVE_SOL_MINT);
        cache.insert(
            pool_address.to_string(),
            VaultBalanceCache {
                reserve_base,
                reserve_quote,
                update_slot,
                active_id,
                bin_step,
                updated_at: Instant::now(),
                dlmm_sol_is_x,
                dlmm_token_x_mint,
            },
        );
        if is_new {
            debug!(
                pool = %pool_address,
                reserve_base,
                reserve_quote,
                slot = update_slot,
                active_id = ?active_id,
                bin_step = ?bin_step,
                "Vault balances cached (new pool)"
            );
        } else {
            debug!(
                pool = %pool_address,
                reserve_base,
                reserve_quote,
                slot = update_slot,
                "Vault balances updated"
            );
        }
        inc_arb_vault_balance_applied_total();
        drop(cache);

        // Mirror SOL liquidity + reserve flag into per-mint pool trackers (Geyser-only, no RPC)
        let liquidity_sol = Decimal::from(reserve_quote) / Decimal::from(1_000_000_000u64);
        let read_wait = Instant::now();
        let mints_with_pool: Vec<String> = {
            let trackers = self.trackers.read();
            record_arb_writer_lock_wait(ArbWriterLockKind::TrackersRead, read_wait.elapsed());
            trackers
                .iter()
                .filter(|(_, tracker)| tracker.pools.contains_key(pool_address))
                .map(|(mint, _)| mint.clone())
                .collect()
        };
        if mints_with_pool.is_empty() {
            return;
        }
        let write_wait = Instant::now();
        let mut trackers = self.trackers.write();
        record_arb_writer_lock_wait(ArbWriterLockKind::TrackersWrite, write_wait.elapsed());
        for mint in &mints_with_pool {
            let Some(tracker) = trackers.get_mut(mint) else {
                continue;
            };
            let Some(pool) = tracker.pools.get_mut(pool_address) else {
                continue;
            };
            pool.liquidity_sol = liquidity_sol;
            pool.has_reserve_data = reserve_base > 0 && reserve_quote > 0;
            pool.last_update = Instant::now();
            if let Some(token_decimals) = tracker.token_decimals {
                if reserves_plausible_for_comparable_price(
                    reserve_base,
                    reserve_quote,
                    token_decimals,
                    &tracker.base_mint,
                ) {
                    if let Some(mid) =
                        reserve_mid_sol_per_token(reserve_base, reserve_quote, token_decimals)
                    {
                        pool.last_price = Some(mid);
                    }
                }
            }
        }
        drop(trackers);
        for mint in &mints_with_pool {
            self.arb_track_selection.mark_dirty(mint);
        }
        self.schedule_arb_vault_rescreen_for_mints(pool_address, &mints_with_pool);
    }

    /// Handle BinArrayUpdate event - cache Meteora DLMM bin arrays from Geyser
    /// This eliminates RPC calls to fetch bin arrays during quoting.
    fn handle_bin_array_update(
        &self,
        pool_address: &str,
        bin_array_index: i64,
        bins: Vec<BinData>,
        update_slot: u64,
    ) {
        inc_arb_dlmm_bin_array_update_received_total();
        let mut cache = self.bin_arrays.write();
        let pool_cache = cache.entry(pool_address.to_string()).or_default();
        let bins_count = bins.len();
        let should_update = match pool_cache.get(&bin_array_index) {
            Some(existing) => update_slot >= existing.update_slot,
            None => true,
        };
        if !should_update {
            return;
        }
        pool_cache.insert(bin_array_index, BinArrayCache { bins, update_slot });
        inc_arb_dlmm_bin_array_update_applied_total();
        drop(cache);

        // Bin liquidity updates are a valid DLMM price signal (H3): refresh vault + pool timestamps.
        let now = Instant::now();
        {
            let pinned = self.arb_pinned_pools.read();
            let pin_class = if pinned.contains(pool_address) {
                "pin"
            } else {
                "cold"
            };
            let mut vault_cache = self.vault_balances.write();
            try_seed_dlmm_vault_on_bin_update(
                pool_address,
                update_slot,
                &self.live_pool_cache,
                &mut vault_cache,
            );
            if vault_cache.contains_key(pool_address)
                && !try_refresh_vault_from_live_cache(
                    pool_address,
                    &self.live_pool_cache,
                    &mut vault_cache,
                    pin_class,
                )
            {
                if let Some(v) = vault_cache.get_mut(pool_address) {
                    v.updated_at = now;
                }
            }
        }
        let read_wait = Instant::now();
        let mints_with_pool: Vec<String> = {
            let trackers = self.trackers.read();
            record_arb_writer_lock_wait(ArbWriterLockKind::TrackersRead, read_wait.elapsed());
            trackers
                .iter()
                .filter(|(_, tracker)| tracker.pools.contains_key(pool_address))
                .map(|(mint, _)| mint.clone())
                .collect()
        };
        if !mints_with_pool.is_empty() {
            let write_wait = Instant::now();
            let mut trackers = self.trackers.write();
            record_arb_writer_lock_wait(ArbWriterLockKind::TrackersWrite, write_wait.elapsed());
            for mint in &mints_with_pool {
                let Some(tracker) = trackers.get_mut(mint) else {
                    continue;
                };
                let Some(pool) = tracker.pools.get_mut(pool_address) else {
                    continue;
                };
                if pool.dex == "meteora_dlmm" {
                    pool.last_update = now;
                }
            }
            drop(trackers);
            for mint in &mints_with_pool {
                self.arb_track_selection.mark_dirty(mint);
            }
            self.schedule_arb_rescreen_for_mints(pool_address, &mints_with_pool);
        }

        debug!(
            pool = %pool_address,
            bin_array_index,
            bins_count,
            slot = update_slot,
            "Bin array cached"
        );
    }

    /// Get cached vault balances for a pool (returns None if not cached)
    #[allow(dead_code)]
    fn get_vault_balances(&self, pool_address: &str) -> Option<(u64, u64)> {
        self.vault_balances
            .read()
            .get(pool_address)
            .map(|c| (c.reserve_base, c.reserve_quote))
    }

    /// Calculate expected token output from buy using AMM constant product formula.
    ///
    /// For a SOL→Token swap on constant-product AMMs (Raydium, Raydium CPMM, Meteora CPMM):
    ///   token_out = reserve_token * sol_in / (reserve_sol + sol_in) * (1 - fee)
    ///
    /// For Meteora DLMM: Uses Bin Array Traversierung (Option D complete)
    ///   - Traverse bins starting from active_id
    ///   - Accumulate token output as we consume SOL in each bin
    ///   - Respects bin boundaries and concentrated liquidity
    ///
    /// Returns None if:
    /// - Reserves not cached (Geyser hasn't delivered PoolStateUpdate)
    /// - DEX not supported for reserve-based calculation
    fn calculate_expected_token_output(
        &self,
        buy_pool: &str,
        buy_dex: &str,
        sol_in_lamports: u64,
        _token_decimals: u8,
    ) -> Option<u64> {
        // Get cached pool state (includes reserves + DLMM-specific data)
        let cache = self.vault_balances.read();
        let pool_state = cache.get(buy_pool)?;

        let reserve_base = pool_state.reserve_base;
        let reserve_quote = pool_state.reserve_quote;

        // For most Solana DEX pools:
        // - base = Token
        // - quote = SOL/WSOL
        // So reserve_base = token reserve, reserve_quote = SOL reserve

        match buy_dex {
            "raydium" | "raydium_cpmm" | "meteora_cpmm" => {
                // Fee rates by DEX (in basis points)
                let fee_bps: u64 = match buy_dex {
                    "raydium" => 25,      // 0.25%
                    "raydium_cpmm" => 25, // 0.25%
                    "meteora_cpmm" => 25, // 0.25%
                    _ => 25,
                };

                // Constant product AMM formula:
                // token_out = reserve_token * sol_in / (reserve_sol + sol_in)
                // Then apply fee: token_out_after_fee = token_out * (10000 - fee_bps) / 10000

                // Use u128 to prevent overflow
                let reserve_token = reserve_base as u128;
                let reserve_sol = reserve_quote as u128;
                let sol_in = sol_in_lamports as u128;

                if reserve_sol == 0 || reserve_token == 0 {
                    warn!(
                        pool = %buy_pool,
                        reserve_sol,
                        reserve_token,
                        "Pool has zero reserves - cannot calculate token output"
                    );
                    return None;
                }

                // token_out_raw = reserve_token * sol_in / (reserve_sol + sol_in)
                let numerator = reserve_token.checked_mul(sol_in)?;
                let denominator = reserve_sol.checked_add(sol_in)?;
                let token_out_raw = numerator.checked_div(denominator)?;

                // Apply fee: token_out = token_out_raw * (10000 - fee_bps) / 10000
                let fee_multiplier = 10000u128 - fee_bps as u128;
                let token_out_after_fee = token_out_raw
                    .checked_mul(fee_multiplier)?
                    .checked_div(10000)?;

                let result = token_out_after_fee as u64;

                info!(
                    pool = %buy_pool,
                    dex = %buy_dex,
                    sol_in_lamports,
                    reserve_sol = %reserve_sol,
                    reserve_token = %reserve_token,
                    token_out_raw = %token_out_raw,
                    token_out_after_fee = result,
                    fee_bps,
                    "Calculated expected token output from reserves (Option D - AMM)"
                );

                Some(result)
            }

            "meteora_dlmm" => {
                self.calculate_dlmm_token_output(buy_pool, sol_in_lamports, pool_state)
            }

            _ => {
                debug!(
                    pool = %buy_pool,
                    dex = %buy_dex,
                    "Unknown DEX: using price-based estimation"
                );
                None
            }
        }
    }

    /// Calculate expected token output for Meteora DLMM using Bin Array Traversierung.
    ///
    /// DLMM pools have concentrated liquidity in discrete price bins.
    /// To calculate exact output, we need to traverse bins starting from active_id
    /// and accumulate token output as we consume SOL in each bin.
    ///
    /// Algorithm:
    /// 1. Start at active_id (current price bin)
    /// 2. For each bin: consume available SOL liquidity, accumulate token output
    /// 3. If bin depleted, move to next bin (higher price = less tokens per SOL)
    /// 4. Continue until all sol_in consumed or no more liquidity
    fn calculate_dlmm_token_output(
        &self,
        pool_address: &str,
        sol_in_lamports: u64,
        pool_state: &VaultBalanceCache,
    ) -> Option<u64> {
        let active_id = pool_state.active_id?;
        let bin_step = pool_state.bin_step?;
        let bin_arrays = self.get_bin_arrays(pool_address)?;
        if bin_arrays.is_empty() {
            debug!(
                pool = %pool_address,
                "DLMM: no bin arrays cached — omit expected_token_output (EE price-based fallback)"
            );
            return None;
        }

        let result = dlmm_token_output_from_bins(
            active_id,
            bin_step,
            sol_in_lamports,
            &bin_arrays,
            vault_dlmm_sol_is_x(pool_state),
        )?;

        if result == 0 {
            debug!(
                pool = %pool_address,
                sol_in_lamports,
                "DLMM bin walker returned zero output — omit expected_token_output"
            );
            return None;
        }

        info!(
            pool = %pool_address,
            sol_in_lamports,
            active_id,
            bin_step,
            tokens_after_fee = result,
            "Calculated expected token output from bin arrays (Option D - DLMM)"
        );

        Some(result)
    }

    /// Get cached bin arrays for a Meteora DLMM pool (returns None if not cached)
    #[allow(dead_code)]
    fn get_bin_arrays(&self, pool_address: &str) -> Option<HashMap<i64, Vec<BinData>>> {
        self.bin_arrays.read().get(pool_address).map(|arrays| {
            arrays
                .iter()
                .map(|(idx, cache)| (*idx, cache.bins.clone()))
                .collect()
        })
    }

    /// Update price from trade event
    ///
    /// Only processes trades with SOL as quote_mint. Trades with non-SOL quotes
    /// (e.g., USDC) are skipped to avoid comparing prices in different units.
    #[allow(clippy::too_many_arguments)]
    fn apply_trade_to_tracker(
        &self,
        pool_address: &str,
        mint: &str,
        quote_mint: &str,
        sol_amount: u64,
        token_amount: u64,
        token_decimals: u8,
        is_buy: bool,
        dex: &str,
    ) -> Option<(TokenArbTracker, ArbConfig)> {
        // CRITICAL: Only track SOL-quoted pools for price comparison.
        // Comparing TOKEN/SOL prices with TOKEN/USDC prices is invalid!
        if quote_mint != NATIVE_SOL_MINT {
            debug!(
                pool = %pool_address,
                mint = %mint,
                quote_mint = %quote_mint,
                dex = %dex,
                "Trade skipped: non-SOL quote (prices not comparable)"
            );
            return None;
        }

        // DATA QUALITY: Reject trades with zero amounts (parser failed to extract token balance)
        if token_amount == 0 || sol_amount == 0 {
            self.zero_amount_trades.fetch_add(1, Ordering::Relaxed);
            debug!(
                pool = %pool_address,
                mint = %mint,
                sol_amount = sol_amount,
                token_amount = token_amount,
                "Trade rejected: zero amount (parser failed to extract token balance)"
            );
            return None;
        }

        // DATA QUALITY: Filter dust trades (< 0.0001 SOL)
        if sol_amount < MIN_TRADE_VOLUME_LAMPORTS {
            debug!(
                pool = %pool_address,
                sol_amount = sol_amount,
                min_volume = MIN_TRADE_VOLUME_LAMPORTS,
                "Trade rejected: volume too low (dust trade)"
            );
            return None;
        }

        let price = trade_implied_sol_per_token(sol_amount, token_amount, token_decimals);

        trace!(
            pool = %pool_address,
            mint = %mint,
            sol_amount = sol_amount,
            token_amount = token_amount,
            token_decimals = token_decimals,
            is_buy = is_buy,
            price = %price,
            "Trade-implied SOL per token"
        );

        let config = self.config.read().clone();

        // Global Geyser connection health check (replaces per-pool staleness)
        if !self.is_geyser_connection_healthy() {
            warn!(
                mint = %mint,
                timeout_secs = GEYSER_CONNECTION_TIMEOUT_SECS,
                "Arb rejected: Geyser connection unhealthy (no events received)"
            );
            return None;
        }

        let vault_reserves = self
            .vault_balances
            .read()
            .get(pool_address)
            .map(|c| (c.reserve_base, c.reserve_quote));
        let vault_entry = self.vault_balances.read().get(pool_address).cloned();
        let dlmm_bins = self.bin_arrays.read().get(pool_address).cloned();

        let tracker_snapshot = {
            let trackers_wait = Instant::now();
            let mut trackers = self.trackers.write();
            record_arb_writer_lock_wait(ArbWriterLockKind::TrackersWrite, trackers_wait.elapsed());

            let tracker = trackers.entry(mint.to_string()).or_insert_with(|| {
                info!(mint = %mint, "Creating tracker from Trade event (no PoolCreated)");
                TokenArbTracker {
                    base_mint: mint.to_string(),
                    pools: HashMap::new(),
                    pool_accounts: HashMap::new(),
                    token_program: None,
                    token_decimals: None,
                    last_intent_time: None,
                }
            });

            tracker.token_decimals = Some(token_decimals);

            let effective_dex = if !dex.is_empty() && dex != "unknown" {
                dex.to_string()
            } else {
                pool_address.to_string()
            };

            let pool = tracker
                .pools
                .entry(pool_address.to_string())
                .or_insert_with(|| {
                    info!(pool = %pool_address, mint = %mint, dex = %effective_dex, "Creating pool from Trade event");
                    PoolState {
                        pool_address: pool_address.to_string(),
                        dex: effective_dex.clone(),
                        liquidity_sol: Decimal::ZERO,
                        has_reserve_data: false,
                        last_price: None,
                        trade_price_buy: None,
                        trade_price_sell: None,
                        trade_count: 0,
                        last_update: Instant::now(),
                        dex_accounts: None,
                    }
                });

            if is_buy {
                pool.trade_price_buy = Some(price);
            } else {
                pool.trade_price_sell = Some(price);
            }
            pool.last_price = comparable_price_sol_per_token(
                pool,
                vault_reserves,
                Some(token_decimals),
                mint,
                vault_entry.as_ref(),
                dlmm_bins.as_ref(),
                ComparablePriceSide::Buy,
            );
            pool.trade_count += 1;
            pool.last_update = Instant::now();
            trace!(
                pool = %pool_address,
                mint = %mint,
                dex = %pool.dex,
                comparable_price = ?pool.last_price,
                "Pool comparable price updated"
            );

            tracker.clone()
        };

        self.apply_pending_pool_accounts(pool_address, mint, quote_mint);
        if let Ok(pool_pk) = Pubkey::from_str(pool_address) {
            if let Some((state, _, _)) = self.live_pool_cache.get_with_metadata(&pool_pk) {
                let mut trackers = self.trackers.write();
                if let Some(tracker) = trackers.get_mut(mint) {
                    maybe_backfill_tracker_pool_accounts_from_cache(
                        pool_pk,
                        pool_address,
                        &state,
                        tracker,
                    );
                }
            }
        }

        Some((tracker_snapshot, config))
    }

    /// Scoped vault/bin snapshot for `check_arbitrage`: only pools in `tracker_snapshot`.
    fn snapshot_vault_bins_for_tracker(
        &self,
        tracker_snapshot: &TokenArbTracker,
    ) -> (
        HashMap<String, VaultBalanceCache>,
        HashMap<String, HashMap<i64, BinArrayCache>>,
    ) {
        let pool_keys: Vec<String> = tracker_snapshot.pools.keys().cloned().collect();
        let pinned_pools = self.arb_pinned_pools.read();
        let state_ttl_ms = self.config.read().arb_quote_state_ttl_ms;
        let vaults = self.vault_balances.read();
        let mut vault_balances = HashMap::with_capacity(pool_keys.len());
        for pool_key in &pool_keys {
            let pin_class = if pinned_pools.contains(pool_key) {
                "pin"
            } else {
                "cold"
            };
            if let Some(entry) = vaults.get(pool_key) {
                vault_balances.insert(pool_key.clone(), entry.clone());
                if try_refresh_vault_from_live_cache(
                    pool_key,
                    &self.live_pool_cache,
                    &mut vault_balances,
                    pin_class,
                ) {
                    inc_arb_vault_live_snapshot_refreshed_total();
                } else if try_overwrite_stale_pinned_vault_from_live_cache(
                    pool_key,
                    &self.live_pool_cache,
                    &mut vault_balances,
                    pin_class,
                    state_ttl_ms,
                ) {
                    inc_arb_vault_live_snapshot_seeded_total();
                }
            } else if try_seed_vault_from_live_cache(
                pool_key,
                &self.live_pool_cache,
                &mut vault_balances,
                pin_class,
            ) {
                inc_arb_vault_live_snapshot_seeded_total();
            }
        }
        drop(vaults);
        drop(pinned_pools);

        let bins = self.bin_arrays.read();
        let mut bin_arrays = HashMap::with_capacity(pool_keys.len());
        for pool_key in &pool_keys {
            if let Some(entry) = bins.get(pool_key) {
                bin_arrays.insert(pool_key.clone(), entry.clone());
            }
        }

        (vault_balances, bin_arrays)
    }

    /// Run v2 check with a live scoped vault/bin snapshot (C1f pin-coverage).
    fn check_arbitrage_for_tracker(
        &self,
        tracker_snapshot: &TokenArbTracker,
        config: &ArbConfig,
    ) -> Option<ArbOpportunity> {
        let (vault_balances, bin_arrays) = self.snapshot_vault_bins_for_tracker(tracker_snapshot);
        let pinned_pools = self.arb_pinned_pools.read();
        record_v2_meteora_pinned_sell_bin_coverage(tracker_snapshot, &bin_arrays, &pinned_pools);
        let known_pools = self.known_pools.read();
        let selected_mints = self.arb_selected_mints.read();
        tracker_snapshot.check_arbitrage(
            config,
            &known_pools,
            &vault_balances,
            &bin_arrays,
            &ArbCheckContext {
                spread_warn_last: &self.spread_too_large_warn_last,
                data_quality_rejects: &self.data_quality_rejects,
                forensics: Some(&self.eligibility_forensics),
                v2_forensics: Some(&self.v2_eligibility_forensics),
                selected_mints: Some(&selected_mints),
                pinned_pools: Some(&pinned_pools),
            },
        )
    }

    /// Returns true when the pending recovery buy/sell pair has a fresh cross-DEX sell leg.
    fn v2_pending_recovery_sell_leg_is_fresh(
        &self,
        tracker_snapshot: &TokenArbTracker,
        config: &ArbConfig,
        pending: &V2SellStaleRecoveryPending,
    ) -> bool {
        let token_decimals = match tracker_snapshot.token_decimals {
            Some(d) => d,
            None => return false,
        };
        let known_pools = self.known_pools.read();
        let (vault_balances, bin_arrays) = self.snapshot_vault_bins_for_tracker(tracker_snapshot);
        let freshness = TokenArbTracker::quote_freshness_config(config);
        let probe = config.arb_probe_lamports;
        let now = Instant::now();

        let owned_candidates = tracker_snapshot.build_round_trip_candidates(
            &known_pools,
            &vault_balances,
            &bin_arrays,
            token_decimals,
            None,
        );
        let buy = owned_candidates
            .iter()
            .find_map(|(pool, vault, bins, dex)| {
                if pool.pool_address != pending.buy_pool {
                    return None;
                }
                Some(RoundTripPoolCandidate {
                    pool,
                    vault: vault.as_ref(),
                    dlmm_bins: bins.as_ref(),
                    dex,
                })
            });
        let sell = owned_candidates
            .iter()
            .find_map(|(pool, vault, bins, dex)| {
                if pool.pool_address != pending.sell_pool {
                    return None;
                }
                Some(RoundTripPoolCandidate {
                    pool,
                    vault: vault.as_ref(),
                    dlmm_bins: bins.as_ref(),
                    dex,
                })
            });
        let (buy, sell) = match (buy, sell) {
            (Some(b), Some(s)) if b.dex != s.dex => (b, s),
            _ => return false,
        };
        let buy_quote = match quote_exact_in_with_freshness(
            buy.pool,
            buy.vault,
            buy.dlmm_bins,
            NATIVE_SOL_MINT,
            &buy.pool.token_mint,
            probe,
            &freshness,
        ) {
            Some(q) => q,
            None => return false,
        };
        if !is_quote_fresh(&buy_quote, &freshness, buy.vault, now) {
            return false;
        }
        classify_cross_dex_sell_failure(
            &sell,
            buy_quote.amount_out,
            &freshness,
            now,
            token_decimals,
        )
        .is_none()
    }

    /// Force QuoteReady republish for both cross-DEX legs (bypasses unchanged-pin skip).
    fn publish_v2_sell_leg_recovery_repins(self: &Arc<Self>, buy_pool: &str, sell_pool: &str) {
        use ironcrab::nats::arb_track_requests::{
            ArbTrackActiveEntry, ArbTrackActiveReason, ArbTrackReadiness,
        };
        let active = vec![
            ArbTrackActiveEntry {
                pool: buy_pool.to_string(),
                reason: ArbTrackActiveReason::MultiDex,
                readiness: ArbTrackReadiness::QuoteReady,
            },
            ArbTrackActiveEntry {
                pool: sell_pool.to_string(),
                reason: ArbTrackActiveReason::MultiDex,
                readiness: ArbTrackReadiness::QuoteReady,
            },
        ];
        inc_arb_v2_sell_stale_recovery_outcome_total("republish_both_legs");
        self.spawn_publish_arb_track_requests(active, Vec::new(), false);
    }

    /// Two-hop v2 screen plus optional C1h5 sell-leg recovery (spawn_blocking only).
    fn two_hop_v2_check_and_maybe_schedule_recovery(
        self: &Arc<Self>,
        tracker_snapshot: &TokenArbTracker,
        config: &ArbConfig,
    ) -> Option<ArbOpportunity> {
        let mint = &tracker_snapshot.base_mint;
        let opp = self.check_arbitrage_for_tracker(tracker_snapshot, config);
        if let Some(opp) = opp {
            if self
                .v2_sell_stale_recovery_pending
                .write()
                .remove(mint)
                .is_some()
            {
                inc_arb_v2_screen_sell_stale_then_fresh_after_pin_total();
                inc_arb_v2_sell_stale_recovery_outcome_total("fresh_after_pin");
            }
            return Some(opp);
        }

        let pending_snapshot = self
            .v2_sell_stale_recovery_pending
            .read()
            .get(mint)
            .cloned();
        if let Some(pending) = pending_snapshot {
            if self.v2_pending_recovery_sell_leg_is_fresh(tracker_snapshot, config, &pending) {
                self.v2_sell_stale_recovery_pending.write().remove(mint);
                inc_arb_v2_screen_sell_stale_then_fresh_after_pin_total();
                inc_arb_v2_sell_stale_recovery_outcome_total("fresh_after_pin");
            }
        }

        if config.arb_two_hop_v2_enabled {
            self.try_schedule_v2_sell_leg_recovery(tracker_snapshot, config);
        }
        None
    }

    /// C1h5: when v2 screen fails on cross-dex sell leg, force both-leg QuoteReady republish.
    /// Must run only from spawn_blocking (two-hop worker); clones lock snapshots before heavy work.
    fn try_schedule_v2_sell_leg_recovery(
        self: &Arc<Self>,
        tracker_snapshot: &TokenArbTracker,
        config: &ArbConfig,
    ) {
        let mint = &tracker_snapshot.base_mint;
        {
            let pending = self.v2_sell_stale_recovery_pending.read();
            if let Some(prev) = pending.get(mint) {
                if prev.scheduled_at.elapsed()
                    < Duration::from_millis(V2_SELL_STALE_RECOVERY_MIN_INTERVAL_MS)
                {
                    inc_arb_v2_sell_stale_recovery_outcome_total("skipped_rate_limit");
                    return;
                }
            }
        }

        let token_decimals = match tracker_snapshot.token_decimals {
            Some(d) => d,
            None => return,
        };

        let known_pools = self.known_pools.read().clone();
        let (vault_balances, bin_arrays) = self.snapshot_vault_bins_for_tracker(tracker_snapshot);

        let freshness = TokenArbTracker::quote_freshness_config(config);
        let probe = config.arb_probe_lamports;
        let owned_candidates = tracker_snapshot.build_round_trip_candidates(
            &known_pools,
            &vault_balances,
            &bin_arrays,
            token_decimals,
            None,
        );
        let candidates: Vec<RoundTripPoolCandidate<'_>> = owned_candidates
            .iter()
            .map(|(pool, vault, bins, dex)| RoundTripPoolCandidate {
                pool,
                vault: vault.as_ref(),
                dlmm_bins: bins.as_ref(),
                dex,
            })
            .collect();
        let Err(RoundTripSelectFailure::InsufficientPools(insufficient)) =
            select_round_trip_pools(&candidates, probe, &freshness)
        else {
            return;
        };
        if insufficient.subreason != RoundTripInsufficientSubreason::NoCrossDexSell {
            return;
        }
        let recoverable = matches!(
            insufficient.no_cross_dex_sell_detail,
            Some(
                NoCrossDexSellDetailReason::SellNotFresh
                    | NoCrossDexSellDetailReason::SellMissingVault
                    | NoCrossDexSellDetailReason::SellMissingDlmmBins
                    | NoCrossDexSellDetailReason::SellQuoteNone
            )
        );
        if !recoverable {
            return;
        }

        let now = Instant::now();
        let mut recovery_pair: Option<(String, String)> = None;
        let mut buy_checks = 0usize;
        'buy: for buy in &candidates {
            if buy_checks >= V2_SELL_STALE_RECOVERY_MAX_BUY_CANDIDATES {
                break;
            }
            let Some(buy_quote) = quote_exact_in_with_freshness(
                buy.pool,
                buy.vault,
                buy.dlmm_bins,
                NATIVE_SOL_MINT,
                &buy.pool.token_mint,
                probe,
                &freshness,
            ) else {
                continue;
            };
            if !is_quote_fresh(&buy_quote, &freshness, buy.vault, now) {
                continue;
            }
            buy_checks += 1;
            for sell in &candidates {
                if sell.dex == buy.dex {
                    continue;
                }
                if classify_cross_dex_sell_failure(
                    sell,
                    buy_quote.amount_out,
                    &freshness,
                    now,
                    token_decimals,
                )
                .is_some()
                {
                    recovery_pair = Some((
                        buy.pool.pool_address.clone(),
                        sell.pool.pool_address.clone(),
                    ));
                    break 'buy;
                }
            }
        }
        let Some((buy_pool, sell_pool)) = recovery_pair else {
            inc_arb_v2_sell_stale_recovery_outcome_total("skipped_no_stale_sell");
            return;
        };

        self.v2_sell_stale_recovery_pending.write().insert(
            mint.clone(),
            V2SellStaleRecoveryPending {
                scheduled_at: Instant::now(),
                buy_pool: buy_pool.clone(),
                sell_pool: sell_pool.clone(),
            },
        );
        inc_arb_v2_screen_sell_stale_recovery_scheduled_total();
        inc_arb_v2_sell_stale_recovery_outcome_total("scheduled");
        self.arb_track_selection.mark_dirty(mint);
        self.publish_v2_sell_leg_recovery_repins(&buy_pool, &sell_pool);
    }

    /// Re-screen selected mints when DLMM bins arrive after the last trade-driven screen (H4).
    fn schedule_arb_rescreen_for_mints(&self, pool_address: &str, mints: &[String]) {
        if mints.is_empty() || !self.config.read().two_hop_enabled {
            return;
        }
        let pinned = self.arb_pinned_pools.read();
        let selected = self.arb_selected_mints.read();
        let pool_is_pinned = pinned.contains(pool_address);
        let trackers = self.trackers.read();
        for mint in mints {
            if !selected.contains(mint) {
                continue;
            }
            let Some(tracker) = trackers.get(mint) else {
                continue;
            };
            if !pool_is_pinned {
                let Some(pool) = tracker.pools.get(pool_address) else {
                    continue;
                };
                if pool.dex != "meteora_dlmm" {
                    continue;
                }
            }
            if self
                .two_hop_tx
                .try_send(ArbTwoHopWorkerJob::Rescreen { mint: mint.clone() })
                .is_ok()
            {
                inc_arb_dlmm_bin_rescreen_scheduled_total();
            } else {
                debug!(
                    mint = %mint,
                    pool = %pool_address,
                    "arb two-hop rescreen queue full; dropping bin-update rescreen"
                );
            }
        }
    }

    /// Re-screen selected mints when vault balances arrive after the last trade-driven screen (C1vault).
    fn schedule_arb_vault_rescreen_for_mints(&self, pool_address: &str, mints: &[String]) {
        if mints.is_empty() || !self.config.read().two_hop_enabled {
            return;
        }
        let selected = self.arb_selected_mints.read();
        let trackers = self.trackers.read();
        for mint in mints {
            if !selected.contains(mint) {
                continue;
            }
            let Some(tracker) = trackers.get(mint) else {
                continue;
            };
            if !tracker.pools.contains_key(pool_address) {
                continue;
            }
            if self
                .two_hop_tx
                .try_send(ArbTwoHopWorkerJob::Rescreen { mint: mint.clone() })
                .is_ok()
            {
                inc_arb_vault_rescreen_scheduled_total();
            } else {
                debug!(
                    mint = %mint,
                    pool = %pool_address,
                    "arb two-hop rescreen queue full; dropping vault-update rescreen"
                );
            }
        }
    }

    fn finalize_trade_opportunity(
        &self,
        mint: &str,
        intent_cooldown_ms: u64,
        opp: ArbOpportunity,
    ) -> Option<ArbOpportunity> {
        let cooldown = Duration::from_millis(intent_cooldown_ms);
        let mut trackers = self.trackers.write();
        let tracker = trackers.get_mut(mint)?;
        if let Some(last_time) = tracker.last_intent_time {
            if last_time.elapsed() < cooldown {
                return None;
            }
        }

        tracker.last_intent_time = Some(Instant::now());
        self.opportunities_found.fetch_add(1, Ordering::Relaxed);
        Some(opp)
    }

    #[allow(dead_code)] // sync path retained for unit tests (`apply_trade_to_tracker` + `check_arbitrage`)
    #[allow(clippy::too_many_arguments)]
    fn handle_trade(
        &self,
        pool_address: &str,
        mint: &str,
        quote_mint: &str,
        sol_amount: u64,
        token_amount: u64,
        token_decimals: u8,
        is_buy: bool,
        dex: &str,
    ) -> Option<ArbOpportunity> {
        let (tracker_snapshot, config) = self.apply_trade_to_tracker(
            pool_address,
            mint,
            quote_mint,
            sol_amount,
            token_amount,
            token_decimals,
            is_buy,
            dex,
        )?;
        let known_pools = self.known_pools.read();
        let vault_balances = self.vault_balances.read();
        let bin_arrays = self.bin_arrays.read();
        let selected_mints = self.arb_selected_mints.read();
        let pinned_pools = self.arb_pinned_pools.read();
        let opp = tracker_snapshot.check_arbitrage(
            &config,
            &known_pools,
            &vault_balances,
            &bin_arrays,
            &ArbCheckContext {
                spread_warn_last: &self.spread_too_large_warn_last,
                data_quality_rejects: &self.data_quality_rejects,
                forensics: Some(&self.eligibility_forensics),
                v2_forensics: Some(&self.v2_eligibility_forensics),
                selected_mints: Some(&selected_mints),
                pinned_pools: Some(&pinned_pools),
            },
        )?;
        self.finalize_trade_opportunity(mint, config.intent_cooldown_ms, opp)
    }

    /// Get pool accounts for both buy and sell pools
    /// Returns (buy_accounts, sell_accounts) if available
    fn get_pool_accounts_for_arb(
        &self,
        opp: &ArbOpportunity,
    ) -> (Option<Vec<String>>, Option<Vec<String>>) {
        let buy_accounts = self.resolve_pool_accounts(&opp.buy_pool, &opp.base_mint);
        let sell_accounts = self.resolve_pool_accounts(&opp.sell_pool, &opp.base_mint);
        (buy_accounts, sell_accounts)
    }

    /// Get token program for a mint (from TokenMintInfo cache)
    fn get_token_program_for_mint(&self, mint: &str) -> Option<String> {
        let trackers = self.trackers.read();
        trackers.get(mint).and_then(|t| t.token_program.clone())
    }

    /// Token decimals for mint from tracker / LivePoolCache (I-15). Fallback 6 when unknown.
    fn get_token_decimals_for_mint(&self, mint: &str) -> u8 {
        self.trackers
            .read()
            .get(mint)
            .and_then(|t| t.token_decimals)
            .unwrap_or(6)
    }

    fn spawn_publish_arb_track_requests(
        self: &Arc<Self>,
        active: Vec<ArbTrackActiveEntry>,
        removed: Vec<ArbTrackRemovedEntry>,
        reconcile: bool,
    ) {
        if active.is_empty() && removed.is_empty() && !reconcile {
            return;
        }
        let Some(nats_src) = self.nats.as_ref() else {
            return;
        };
        let nats = nats_src.clone_for_spawned_publish();
        let update = ArbTrackRequestsUpdate {
            version: ARB_TRACK_REQUESTS_WIRE_VERSION,
            ts_unix_ms: wall_clock_unix_ms_now(),
            active,
            removed,
            reconcile,
        };

        let chunks = if reconcile {
            if arb_track_payload_bytes(&update) > ARB_TRACK_PUBLISH_MAX_PAYLOAD_BYTES {
                warn!(
                    payload_bytes = arb_track_payload_bytes(&update),
                    max_bytes = ARB_TRACK_PUBLISH_MAX_PAYLOAD_BYTES,
                    active_len = update.active.len(),
                    "ArbTrackRequests reconcile snapshot exceeds NATS publish budget; trimming active"
                );
                match trim_reconcile_update_to_budget(update) {
                    Some(trimmed) => vec![trimmed],
                    None => {
                        warn!(
                            max_bytes = ARB_TRACK_PUBLISH_MAX_PAYLOAD_BYTES,
                            "ArbTrackRequests reconcile snapshot still exceeds budget after trim; skipping publish"
                        );
                        return;
                    }
                }
            } else {
                vec![update]
            }
        } else {
            split_arb_track_requests_update(update)
        };

        if chunks.is_empty() {
            return;
        }

        self.arb_track_published
            .fetch_add(chunks.len() as u64, Ordering::Relaxed);
        tokio::spawn(async move {
            for chunk in chunks {
                match nats.publish(TOPIC_ARB_TRACK_REQUESTS, &chunk).await {
                    Ok(_) => {
                        record_arb_track_requests_messages_total();
                        record_arb_track_requests_publish_chunks_total();
                    }
                    Err(e) => {
                        record_arb_track_requests_publish_failed_total();
                        warn!(
                            error = %e,
                            topic = TOPIC_ARB_TRACK_REQUESTS,
                            payload_bytes = arb_track_payload_bytes(&chunk),
                            active_len = chunk.active.len(),
                            removed_len = chunk.removed.len(),
                            reconcile = chunk.reconcile,
                            "ArbTrackRequests NATS publish failed"
                        );
                    }
                }
            }
        });
    }

    fn collect_multi_dex_mint_ids(&self) -> Vec<String> {
        let trackers = self.trackers.read();
        let mut mints: Vec<String> = trackers
            .iter()
            .filter(|(_, tracker)| {
                tracker.pool_count_on_distinct_dexes() >= 2 && tracker.token_decimals.is_some()
            })
            .map(|(mint, _)| mint.clone())
            .collect();
        mints.sort();
        mints
    }

    fn build_track_mint_input(&self, mint: &str) -> Option<TrackMintInput> {
        let tracker = {
            let trackers = self.trackers.read();
            trackers.get(mint)?.clone()
        };
        if tracker.pool_count_on_distinct_dexes() < 2 {
            return None;
        }
        let token_decimals = tracker.token_decimals?;
        let pool_addresses: Vec<String> = tracker.pools.keys().cloned().collect();

        let known_for_pools: HashSet<String> = {
            let known_pools = self.known_pools.read();
            pool_addresses
                .iter()
                .filter(|pool| known_pools.contains(*pool))
                .cloned()
                .collect()
        };

        let vaults_for_pools: HashMap<String, VaultBalanceCache> = {
            let vault_balances = self.vault_balances.read();
            pool_addresses
                .iter()
                .filter_map(|pool| {
                    vault_balances
                        .get(pool)
                        .map(|vault| (pool.clone(), vault.clone()))
                })
                .collect()
        };

        let bins_for_pools: HashMap<String, HashMap<i64, BinArrayCache>> = {
            let bin_arrays = self.bin_arrays.read();
            pool_addresses
                .iter()
                .filter_map(|pool| {
                    bin_arrays
                        .get(pool)
                        .map(|bins| (pool.clone(), bins.clone()))
                })
                .collect()
        };

        let trade_signal_pools = {
            self.arb_trade_signal_pairs
                .read()
                .get(mint)
                .map(|pair| (pair.buy_pool.clone(), pair.sell_pool.clone()))
        };

        let mint_activity = tracker_mint_activity_unix_ms(&tracker, &vaults_for_pools);

        let pools: Vec<TrackPoolInput> = tracker
            .pools
            .values()
            .map(|pool| {
                let vault = vaults_for_pools.get(&pool.pool_address);
                TrackPoolInput {
                    pool_address: pool.pool_address.clone(),
                    dex: pool.dex.clone(),
                    known: known_for_pools.contains(&pool.pool_address),
                    quote_pool: pool_state_to_quote_input(pool, mint, token_decimals),
                    vault: vault.map(vault_cache_to_quote_input),
                    dlmm_bins: bins_for_pools
                        .get(&pool.pool_address)
                        .map(flatten_bin_array_cache),
                    token_decimals,
                    last_activity_unix_ms: pool_activity_unix_ms(pool, vault),
                }
            })
            .collect();

        Some(TrackMintInput {
            mint: mint.to_string(),
            pools,
            trade_signal_pools,
            last_activity_unix_ms: mint_activity,
        })
    }

    fn mandatory_protected_snapshot_mints(&self) -> HashSet<String> {
        let mut protected = HashSet::new();
        for mint in self.arb_trade_signal_pairs.read().keys() {
            protected.insert(mint.clone());
        }
        let pinned = self.arb_pinned_pools.read();
        let snapshots = self.arb_track_mint_snapshots.read();
        for (mint, entry) in &snapshots.entries {
            if entry
                .input
                .pools
                .iter()
                .any(|pool| pinned.contains(&pool.pool_address))
            {
                protected.insert(mint.clone());
            }
        }
        protected
    }

    fn rank_multi_dex_mints_by_activity(&self, mints: &[String]) -> Vec<String> {
        // Lock contract: `trackers` read guard only for cloning coarse rank rows.
        // Vault balances are not read here; per-mint snapshot build uses separate
        // short vault scopes in `build_track_mint_input`.
        let mut snapshot = {
            let trackers = self.trackers.read();
            build_multi_dex_coarse_rank_snapshot(&trackers, mints)
        };
        rank_coarse_rank_snapshot(&mut snapshot)
    }

    fn commit_mint_snapshot(
        &self,
        mint: &str,
        input: Option<TrackMintInput>,
        protected: &HashSet<String>,
    ) -> bool {
        let mut snapshots = self.arb_track_mint_snapshots.write();
        match input {
            Some(input) => {
                if snapshots.insert_bounded(mint.to_string(), input, protected) {
                    true
                } else {
                    drop(snapshots);
                    self.arb_track_selection
                        .pending_full_reconcile
                        .store(true, Ordering::Release);
                    false
                }
            }
            None => {
                snapshots.remove(mint);
                true
            }
        }
    }

    fn refresh_mint_snapshot(&self, mint: &str, protected: &HashSet<String>) {
        let input = self.build_track_mint_input(mint);
        if !self.commit_mint_snapshot(mint, input, protected) {
            self.arb_track_selection.mark_dirty(mint);
        }
    }

    fn run_arb_track_selection_from_snapshots(self: &Arc<Self>, reconcile: bool) {
        let mint_inputs: Vec<TrackMintInput> = self
            .arb_track_mint_snapshots
            .read()
            .values()
            .cloned()
            .collect();
        let config = self.config.read();
        let selection_config = TrackSelectionConfig {
            max_pools: config.arb_track_baseline_max_pools,
            max_pools_per_mint: 3,
            probe_lamports: config.arb_probe_lamports,
            freshness: QuoteFreshnessConfig {
                trade_ttl_ms: config.arb_quote_trade_ttl_ms,
                state_ttl_ms: config.arb_quote_state_ttl_ms,
            },
        };
        drop(config);

        let result = select_arb_track_pools(&mint_inputs, &selection_config);
        set_arb_track_selection_metrics(
            result.selected.len(),
            result.selected_mints,
            result.pair_complete_mints,
            result.orphan_pools,
            &result.candidate_counts,
        );

        let mut selected_pool_counts = TrackCandidateCounts::default();
        for pool in &result.selected {
            selected_pool_counts.record(pool.readiness);
        }
        set_arb_track_selected_pool_readiness_metrics(&selected_pool_counts);

        let selected_mint_set: HashSet<String> =
            result.selected.iter().map(|p| p.mint.clone()).collect();

        let budget_displaced: HashSet<String> = result.budget_displaced.into_iter().collect();
        let new_pools: HashSet<String> = result.selected.iter().map(|p| p.pool.clone()).collect();
        let old_pools = self.arb_pinned_pools.read().clone();

        if old_pools != new_pools {
            let mut pinned = self.arb_pinned_pools.write();
            *pinned = new_pools.clone();
        }

        {
            let mut selected_mints = self.arb_selected_mints.write();
            *selected_mints = selected_mint_set;
        }

        if !reconcile && old_pools == new_pools {
            record_arb_track_publish_skipped_unchanged_total();
            return;
        }

        let newly_active_mints: HashSet<String> = result
            .selected
            .iter()
            .filter(|p| !old_pools.contains(&p.pool))
            .map(|p| p.mint.clone())
            .collect();

        let active = if reconcile {
            result
                .selected
                .iter()
                .map(|p| ArbTrackActiveEntry {
                    pool: p.pool.clone(),
                    reason: if p.active_reason == ArbTrackActiveReason::TradeSignal {
                        ArbTrackActiveReason::TradeSignal
                    } else {
                        ArbTrackActiveReason::Baseline
                    },
                    readiness: track_pool_readiness_to_wire(p.readiness),
                })
                .collect()
        } else {
            result
                .selected
                .iter()
                .filter(|p| !old_pools.contains(&p.pool))
                .map(|p| ArbTrackActiveEntry {
                    pool: p.pool.clone(),
                    reason: p.active_reason,
                    readiness: track_pool_readiness_to_wire(p.readiness),
                })
                .collect::<Vec<_>>()
        };

        let removed = if reconcile {
            Vec::new()
        } else {
            old_pools
                .iter()
                .filter(|pool| !new_pools.contains(*pool))
                .map(|pool| {
                    let reason = arb_track_removal_reason(pool, &budget_displaced);
                    record_arb_track_removed_total(reason);
                    ArbTrackRemovedEntry {
                        pool: pool.clone(),
                        reason,
                    }
                })
                .collect()
        };

        let will_publish = reconcile || !active.is_empty() || !removed.is_empty();
        if !will_publish {
            record_arb_track_publish_skipped_unchanged_total();
            return;
        }

        for mint in &newly_active_mints {
            record_arb_proactive_pin_first_publish(mint);
        }

        if reconcile {
            self.spawn_publish_arb_track_requests(active, Vec::new(), true);
        } else {
            if !active.is_empty() {
                record_arb_proactive_track_publish_total();
            }
            self.spawn_publish_arb_track_requests(active, removed, false);
        }
    }

    fn mark_arb_track_mint_dirty(self: &Arc<Self>, mint: &str) {
        self.arb_track_selection.mark_dirty(mint);
    }

    fn reconcile_arb_track_baseline_publish(self: &Arc<Self>) {
        self.arb_track_selection.request_full_reconcile();
    }

    fn publish_proactive_arb_track_for_mint(self: &Arc<Self>, mint: &str) {
        self.mark_arb_track_mint_dirty(mint);
    }

    fn record_arb_trade_signal_pair(self: &Arc<Self>, mint: &str, buy_pool: &str, sell_pool: &str) {
        let seen_at = wall_clock_unix_ms_now();
        let mut pairs = self.arb_trade_signal_pairs.write();
        let mut order = self.arb_trade_signal_pair_order.write();
        if !pairs.contains_key(mint) {
            order.push(mint.to_string());
        }
        pairs.insert(
            mint.to_string(),
            ArbTradeSignalPair {
                buy_pool: buy_pool.to_string(),
                sell_pool: sell_pool.to_string(),
                seen_at_unix_ms: seen_at,
            },
        );
        if let Some(pos) = order.iter().position(|m| m == mint) {
            order.remove(pos);
        }
        order.push(mint.to_string());
        while pairs.len() > ARB_TRADE_SIGNAL_PAIRS_CAP {
            let Some(evict_mint) = order.first().cloned() else {
                break;
            };
            order.remove(0);
            pairs.remove(&evict_mint);
        }
    }

    fn publish_arb_trade_signal_track_pins(
        self: &Arc<Self>,
        mint: &str,
        buy_pool: &str,
        sell_pool: &str,
    ) {
        self.record_arb_trade_signal_pair(mint, buy_pool, sell_pool);
        self.mark_arb_track_mint_dirty(mint);
    }

    /// Heartbeat hook: schedule authoritative stale-pin cleanup via the selection worker.
    /// No tracker scan, pinned-set mutation, or direct publish on the async path.
    fn prune_arb_track_stale_pools(self: &Arc<Self>) {
        self.arb_track_selection.request_full_reconcile();
    }
}

// ============================================================================
// Intent Generation
// ============================================================================

/// PumpSwap (`pump_amm`) needs the full verified 14-account static set from
/// `DexPoolAccounts` (market-data verification). Partial or observation-only
/// cache rows must not produce swap intents.
fn pump_amm_pool_accounts_valid_for_swap(pool_address: &str, accounts: &[String]) -> bool {
    accounts.len() == 14 && accounts.first().map(|s| s.as_str()) == Some(pool_address)
}

/// Creates an arb intent from the opportunity.
/// Returns None if required DexPoolAccounts are missing for ANY pool.
///
/// GEYSER-FIRST PRINCIPLE (TARGET_ARCHITECTURE.md §4.5):
/// - NO RPC calls in hot path
/// - DexPoolAccounts must be available for BOTH buy and sell pools
/// - If Geyser hasn't delivered the data, RPC won't have it either (same validator)
/// - Missing data = REJECT intent, don't try RPC fallback
/// - For PumpSwap (`pump_amm`), cached accounts must be the full verified 14-account
///   set matching `pool_address` (not merely "some" accounts from observation).
fn create_arb_intent(ctx: &ArbContext, opp: &ArbOpportunity) -> Option<TradeIntent> {
    let config = ctx.config.read();

    // Get pool accounts from DexPoolAccounts events (NO RPC needed in execution-engine!)
    let (buy_accounts, sell_accounts) = ctx.get_pool_accounts_for_arb(opp);

    // GEYSER-FIRST: Require DexPoolAccounts for BOTH pools
    // This eliminates RPC fallback in execution-engine hot path.
    // If Geyser hasn't delivered the pool data yet, we reject early.
    if buy_accounts.is_none() {
        debug!(
            buy_pool = %opp.buy_pool,
            buy_dex = %opp.buy_dex,
            mint = %opp.base_mint,
            spread_bps = opp.spread_bps,
            "Rejecting arb: buy pool missing DexPoolAccounts (GEYSER-FIRST)"
        );
        ARB_REJECTED_MISSING_ACCOUNTS.fetch_add(1, Ordering::Relaxed);
        return None;
    }
    if sell_accounts.is_none() {
        debug!(
            sell_pool = %opp.sell_pool,
            sell_dex = %opp.sell_dex,
            mint = %opp.base_mint,
            spread_bps = opp.spread_bps,
            "Rejecting arb: sell pool missing DexPoolAccounts (GEYSER-FIRST)"
        );
        ARB_REJECTED_MISSING_ACCOUNTS.fetch_add(1, Ordering::Relaxed);
        return None;
    }

    if !is_arb_route_executable(&opp.buy_dex, &opp.sell_dex) {
        warn!(
            buy_dex = %opp.buy_dex,
            sell_dex = %opp.sell_dex,
            buy_pool = %opp.buy_pool,
            sell_pool = %opp.sell_pool,
            mint = %opp.base_mint,
            spread_bps = opp.spread_bps,
            reason = "unsupported_cross_dex_route",
            "Suppressing arb intent: EE cannot build cross-DEX plan for this route"
        );
        record_arb_intent_suppressed_unsupported_route();
        return None;
    }

    // Both pools have accounts - safe to proceed
    let buy_accts = buy_accounts.unwrap();
    let sell_accts = sell_accounts.unwrap();

    if opp.buy_dex == "pump_amm"
        && !pump_amm_pool_accounts_valid_for_swap(&opp.buy_pool, &buy_accts)
    {
        debug!(
            buy_pool = %opp.buy_pool,
            mint = %opp.base_mint,
            buy_accounts_len = buy_accts.len(),
            "Rejecting arb: buy pool has incomplete PumpSwap DexPoolAccounts (need 14 + accounts[0]==pool)"
        );
        ARB_REJECTED_MISSING_ACCOUNTS.fetch_add(1, Ordering::Relaxed);
        return None;
    }
    if opp.sell_dex == "pump_amm"
        && !pump_amm_pool_accounts_valid_for_swap(&opp.sell_pool, &sell_accts)
    {
        debug!(
            sell_pool = %opp.sell_pool,
            mint = %opp.base_mint,
            sell_accounts_len = sell_accts.len(),
            "Rejecting arb: sell pool has incomplete PumpSwap DexPoolAccounts (need 14 + accounts[0]==pool)"
        );
        ARB_REJECTED_MISSING_ACCOUNTS.fetch_add(1, Ordering::Relaxed);
        return None;
    }

    // Combine accounts: buy pool accounts + sell pool accounts
    // Format: buy accounts are prefixed with "buy:" and sell with "sell:" for disambiguation
    // execution-engine will parse these to build instructions without RPC
    let mut all_accounts = Vec::new();

    // Store buy accounts with marker
    all_accounts.push(format!("buy_pool_accounts_start:{}", buy_accts.len()));
    all_accounts.extend(buy_accts.iter().cloned());

    // Store sell accounts with marker
    all_accounts.push(format!("sell_pool_accounts_start:{}", sell_accts.len()));
    all_accounts.extend(sell_accts.iter().cloned());

    // Get token program from cache (from TokenMintInfo event)
    // This avoids IncorrectProgramId errors when creating ATAs for Token-2022 tokens
    let token_program = ctx.get_token_program_for_mint(&opp.base_mint);

    // =========================================================================
    // OPTION D: Calculate expected_token_output from pool reserves / bin walker
    // =========================================================================
    // EE uses this as buy amount_out and sell amount_in. Must match the canonical
    // `dlmm_token_output_from_bins` path in pool_quote (same orientation + bins).
    // When bins are incomplete or the walker yields a degenerate quote, omit metadata
    // so EE falls back to price-based sizing (15% safety margin).
    let token_decimals = ctx.get_token_decimals_for_mint(&opp.base_mint);
    let mut expected_token_output = ctx.calculate_expected_token_output(
        &opp.buy_pool,
        &opp.buy_dex,
        opp.trade_amount_lamports,
        token_decimals,
    );

    let price_based_estimate =
        price_based_token_output_raw(opp.trade_amount_lamports, opp.buy_price, token_decimals);

    if let Some(token_out) = expected_token_output {
        if !is_expected_token_output_plausible(
            token_out,
            price_based_estimate,
            opp.trade_amount_lamports,
        ) {
            warn!(
                mint = %opp.base_mint,
                buy_pool = %opp.buy_pool,
                buy_dex = %opp.buy_dex,
                trade_amount_lamports = opp.trade_amount_lamports,
                token_out,
                price_based_estimate = ?price_based_estimate,
                buy_price = %opp.buy_price,
                token_decimals,
                "Suppressing implausible expected_token_output — EE will use price-based fallback"
            );
            record_arb_intent_suppressed_implausible_token_out();
            expected_token_output = None;
        } else {
            debug!(
                buy_pool = %opp.buy_pool,
                buy_dex = %opp.buy_dex,
                sol_in = opp.trade_amount_lamports,
                token_out,
                price_based_estimate = ?price_based_estimate,
                token_decimals,
                "Option D: calculated expected_token_output from reserves/bin walker"
            );
        }
    } else {
        debug!(
            buy_pool = %opp.buy_pool,
            buy_dex = %opp.buy_dex,
            price_based_estimate = ?price_based_estimate,
            "Option D: no reserve/bin quote — EE price-based fallback"
        );
    }

    let resources = TradeResources {
        input_mint: "So11111111111111111111111111111111111111112".to_string(),
        output_mint: opp.base_mint.clone(),
        pools: vec![opp.buy_pool.clone(), opp.sell_pool.clone()],
        accounts: all_accounts,
        token_program: token_program.clone(),
    };

    // Both pools have accounts - no RPC fallback needed
    debug!(
        buy_pool = %opp.buy_pool,
        sell_pool = %opp.sell_pool,
        buy_dex = %opp.buy_dex,
        sell_dex = %opp.sell_dex,
        buy_accounts_len = buy_accts.len(),
        sell_accounts_len = sell_accts.len(),
        token_program = ?token_program,
        "Arb intent has complete pool accounts (GEYSER-FIRST compliant)"
    );

    let mut intent = TradeIntent::new(
        "arb-strategy",
        BUILD_VERSION,
        &ctx.run_id,
        ctx.next_intent_id(),
        "arb-strategy",
        IntentTier::Arb,         // Arbitrage: P75 × 1.3 fee (between Tier0 and Tier1)
        IntentOrigin::StrategyA, // Typ A - market-driven
        ExplicitAmount::new(opp.trade_amount_lamports, 9),
        resources,
        opp.spread_bps as i32,
        config.max_slippage_bps,
        TradeSide::Buy, // First leg: buy token
        TradingRegime::NotApplicable,
    );

    // Require atomic bundle execution
    intent = intent.with_bundle(Some(100_000)); // 0.0001 SOL tip

    // Add fee hints
    intent = intent.with_fee_hints(
        Some(400_000), // Cross-DEX arb needs more CU
        Some(100_000), // priority fee micro-lamports
        Some(1),       // elevated urgency
    );

    // Set TTL
    intent = intent.with_ttl_ms(config.intent_ttl_ms);

    // Add Cross-DEX metadata for execution-engine
    intent
        .metadata
        .insert("cross_dex_arb".to_string(), "true".to_string());
    intent
        .metadata
        .insert("buy_dex".to_string(), opp.buy_dex.clone());
    intent
        .metadata
        .insert("buy_pool".to_string(), opp.buy_pool.clone());
    intent
        .metadata
        .insert("buy_price".to_string(), opp.buy_price.to_string());
    intent
        .metadata
        .insert("sell_dex".to_string(), opp.sell_dex.clone());
    intent
        .metadata
        .insert("sell_pool".to_string(), opp.sell_pool.clone());
    intent
        .metadata
        .insert("sell_price".to_string(), opp.sell_price.to_string());
    intent
        .metadata
        .insert("spread_bps".to_string(), opp.spread_bps.to_string());
    intent.metadata.insert(
        "estimated_profit_lamports".to_string(),
        opp.estimated_profit_lamports.to_string(),
    );

    // =========================================================================
    // OPTION D: Pass expected_token_output to execution-engine (plausibility-gated)
    // =========================================================================
    // Omitted when reserve/bin quote is missing or failed the price-based plausibility gate.
    if let Some(token_out) = expected_token_output {
        intent
            .metadata
            .insert("expected_token_output".to_string(), token_out.to_string());
    }

    // Decision record: why this opportunity was chosen
    intent.metadata.insert("decision_reason".to_string(), format!(
        "Cross-DEX arb: Buy {} @ {} ({}), Sell @ {} ({}). Spread {}bps > min {}bps. Estimated profit {} lamports > min {}",
        opp.base_mint,
        opp.buy_price,
        opp.buy_dex,
        opp.sell_price,
        opp.sell_dex,
        opp.spread_bps,
        config.min_spread_bps,
        opp.estimated_profit_lamports,
        config.min_profit_lamports
    ));

    Some(intent)
}

fn apply_pool_cache_jetstream_message(ctx: &ArbContext, update: PoolCacheUpdate) {
    arb_strategy_pool_cache_update_seen_inc();
    if !matches!(update.update_type, PoolCacheUpdateType::PoolRemoved)
        && arb_tracked_token_mint(&update.base_mint, &update.quote_mint).is_none()
    {
        arb_strategy_pool_cache_update_skip_non_arb_quote_inc();
    }
    if sync_arb_slave_from_pool_cache_update(
        &ctx.live_pool_cache,
        &ctx.known_pools,
        ctx.multi_hop.as_ref(),
        &update,
    ) {
        ctx.multi_hop
            .touch_live_pool_quote_ready(&update.pool_address);
        if !ctx.tracker_write.try_enqueue(
            ArbTrackerWriteJob::SeedPoolCache {
                update: update.clone(),
            },
            ArbTrackerWriteJobType::SeedPoolCache,
        ) {
            debug!(
                pool = %update.pool_address,
                "Dropped PoolCache tracker seed (single-writer queue full)"
            );
            arb_strategy_pool_cache_update_skip_no_seed_inc();
            return;
        }
        debug!(
            pool = %update.pool_address,
            dex = %update.dex,
            update_type = ?update.update_type,
            "SLAVE CACHE: Pool cache update queued for tracker seed (JetStream)"
        );
    }
}

fn arb_tracker_write_job_type(job: &ArbTrackerWriteJob) -> ArbTrackerWriteJobType {
    match job {
        ArbTrackerWriteJob::SeedPoolCache { .. } => ArbTrackerWriteJobType::SeedPoolCache,
        ArbTrackerWriteJob::ApplyTrade { .. } => ArbTrackerWriteJobType::ApplyTrade,
        ArbTrackerWriteJob::FinalizeOpportunity { .. } => {
            ArbTrackerWriteJobType::FinalizeOpportunity
        }
        ArbTrackerWriteJob::PoolCreated { .. } => ArbTrackerWriteJobType::PoolCreated,
        ArbTrackerWriteJob::DexPoolAccounts { .. } => ArbTrackerWriteJobType::DexPoolAccounts,
        ArbTrackerWriteJob::TokenMintInfo { .. } => ArbTrackerWriteJobType::TokenMintInfo,
        ArbTrackerWriteJob::PoolStateUpdate { .. } => ArbTrackerWriteJobType::PoolStateUpdate,
    }
}

fn process_arb_tracker_write_job(ctx: Arc<ArbContext>, job: ArbTrackerWriteJob) {
    let job_type = arb_tracker_write_job_type(&job);
    arb_tracker_write_job_started(job_type);
    let started = Instant::now();

    match job {
        ArbTrackerWriteJob::SeedPoolCache { update } => {
            let vault_seeded = ctx.consume_vault_seed_from_pool_cache_update(&update);
            let tracker_seeded = ctx.seed_trackers_for_pool_cache_update(&update);
            if tracker_seeded || vault_seeded {
                arb_strategy_pool_cache_update_seeded_inc();
                if let Some(mint) = arb_tracked_token_mint(&update.base_mint, &update.quote_mint) {
                    ctx.publish_proactive_arb_track_for_mint(mint);
                }
            } else {
                arb_strategy_pool_cache_update_skip_no_seed_inc();
            }
        }
        ArbTrackerWriteJob::ApplyTrade { job, reply } => {
            let result = ctx
                .apply_trade_to_tracker(
                    &job.pool_address,
                    &job.mint,
                    &job.quote_mint,
                    job.sol_amount,
                    job.token_amount,
                    job.token_decimals,
                    job.is_buy,
                    &job.dex,
                )
                .map(|(tracker_snapshot, config)| {
                    if tracker_snapshot.pool_count_on_distinct_dexes() >= 2 {
                        ctx.publish_proactive_arb_track_for_mint(&job.mint);
                    }
                    let (vault_balances, bin_arrays) =
                        ctx.snapshot_vault_bins_for_tracker(&tracker_snapshot);
                    ApplyTradeResult {
                        tracker_snapshot,
                        config,
                        mint: job.mint.clone(),
                        vault_balances,
                        bin_arrays,
                    }
                });
            let _ = reply.send(result);
        }
        ArbTrackerWriteJob::FinalizeOpportunity {
            mint,
            intent_cooldown_ms,
            opp,
            reply,
        } => {
            let finalized = ctx.finalize_trade_opportunity(&mint, intent_cooldown_ms, opp);
            let _ = reply.send(finalized);
        }
        ArbTrackerWriteJob::PoolCreated {
            pool_address,
            base_mint,
            quote_mint,
            dex,
            liquidity_sol,
        } => {
            if let Some(mint) =
                ctx.handle_pool_created(&pool_address, &base_mint, &quote_mint, &dex, liquidity_sol)
            {
                ctx.publish_proactive_arb_track_for_mint(&mint);
            }
        }
        ArbTrackerWriteJob::DexPoolAccounts {
            pool_address,
            base_mint,
            quote_mint,
            accounts,
        } => {
            ctx.handle_dex_pool_accounts(&pool_address, &base_mint, &quote_mint, accounts);
        }
        ArbTrackerWriteJob::TokenMintInfo {
            mint,
            token_program,
        } => {
            ctx.handle_token_mint_info(&mint, &token_program);
        }
        ArbTrackerWriteJob::PoolStateUpdate {
            pool_address,
            dex,
            reserve_base,
            reserve_quote,
            update_slot,
            active_id,
            bin_step,
            base_mint,
            quote_mint,
        } => {
            ctx.handle_pool_state_update(
                &pool_address,
                &dex,
                reserve_base,
                reserve_quote,
                update_slot,
                active_id,
                bin_step,
                &base_mint,
                &quote_mint,
            );
        }
    }

    arb_tracker_write_job_finished(job_type, started.elapsed());
    arb_tracker_write_job_processed_inc(job_type);
}

fn maybe_arb_tracker_write_stall_watchdog_warn(queue_cap: usize) {
    tick_arb_tracker_write_seconds_since_last_finish();
    tick_arb_heartbeat_seconds_since_last_finish();

    let secs_since_finish = ARB_TRACKER_WRITE_SECONDS_SINCE_LAST_FINISH.load(Ordering::Relaxed);
    let queue_depth = ARB_TRACKER_WRITE_QUEUE_DEPTH.load(Ordering::Relaxed);
    let threshold_90 = (queue_cap as u64 * 90) / 100;
    let threshold_70 = (queue_cap as u64 * 70) / 100;

    let current_job_type = ARB_TRACKER_WRITE_CURRENT_JOB_TYPE.load(Ordering::Relaxed);
    let job_started_ms = ARB_TRACKER_WRITE_CURRENT_JOB_STARTED_UNIX_MS.load(Ordering::Relaxed);
    let now_ms = wall_clock_unix_ms_now();
    let job_duration_secs = if job_started_ms > 0 {
        now_ms.saturating_sub(job_started_ms) / 1000
    } else {
        0
    };
    let stuck_in_job = current_job_type > 0 && job_duration_secs > 30;
    let not_finishing = secs_since_finish > 30;
    if !stuck_in_job && !not_finishing {
        return;
    }

    let queue_threshold = if stuck_in_job {
        threshold_70
    } else {
        threshold_90
    };
    if queue_depth < queue_threshold {
        return;
    }

    let stall_kind = if stuck_in_job && not_finishing {
        "writer stuck in job and not finishing jobs"
    } else if stuck_in_job {
        "writer stuck in job"
    } else {
        "writer not finishing jobs"
    };

    let coalescer_pending = ARB_TRACKER_WRITE_COALESCER_PENDING.load(Ordering::Relaxed);
    let flush_lost_pool_state =
        arb_tracker_write_coalescer_flush_lost_total(ArbTrackerWriteJobType::PoolStateUpdate);
    let flush_lost_dex_accounts =
        arb_tracker_write_coalescer_flush_lost_total(ArbTrackerWriteJobType::DexPoolAccounts);
    let heartbeat_secs = ARB_HEARTBEAT_SECONDS_SINCE_LAST_FINISH.load(Ordering::Relaxed);

    warn!(
        stall_kind,
        current_job_type,
        job_duration_secs,
        queue_depth,
        queue_cap = queue_cap as u64,
        coalescer_pending,
        flush_lost_pool_state,
        flush_lost_dex_accounts,
        secs_since_finish,
        heartbeat_secs_since_finish = heartbeat_secs,
        "arb tracker write stall watchdog"
    );
    arb_tracker_write_stall_watchdog_inc();
}

/// Authoritative batch-mode decision for the selection worker (unit-tested).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArbTrackBatchPlan {
    Idle,
    /// Full reconcile requested but rate-limited; no dirty mints to process incrementally.
    DeferFullReconcile,
    /// Process dirty mints only; `keep_pending_full` preserves deferred full reconcile.
    Incremental {
        keep_pending_full: bool,
    },
    FullReconcile,
}

fn resolve_arb_track_batch_plan(
    wants_full_reconcile: bool,
    has_dirty_mints: bool,
    elapsed_since_full: Duration,
    full_reconcile_min: Duration,
) -> ArbTrackBatchPlan {
    let full_allowed = wants_full_reconcile && elapsed_since_full >= full_reconcile_min;
    if full_allowed {
        return ArbTrackBatchPlan::FullReconcile;
    }
    if wants_full_reconcile && has_dirty_mints {
        return ArbTrackBatchPlan::Incremental {
            keep_pending_full: true,
        };
    }
    if wants_full_reconcile {
        return ArbTrackBatchPlan::DeferFullReconcile;
    }
    if has_dirty_mints {
        return ArbTrackBatchPlan::Incremental {
            keep_pending_full: false,
        };
    }
    ArbTrackBatchPlan::Idle
}

fn run_arb_track_selection_batch(
    ctx: &Arc<ArbContext>,
    mut dirty_mints: Vec<String>,
    full_reconcile: bool,
) {
    let protected = ctx.mandatory_protected_snapshot_mints();

    if full_reconcile {
        let all_mints = ctx.collect_multi_dex_mint_ids();
        let ranked = ctx.rank_multi_dex_mints_by_activity(&all_mints);
        let admit = compute_snapshot_admit_set(&ranked, &protected, ARB_TRACK_MINT_SNAPSHOTS_CAP);
        let refresh_order = admit_refresh_order(&ranked, &admit);
        for mint in &refresh_order {
            ctx.refresh_mint_snapshot(mint, &protected);
        }
        ctx.arb_track_mint_snapshots
            .write()
            .retain(|mint| admit.contains(mint));
    } else {
        dirty_mints.sort();
        dirty_mints.dedup();
        if dirty_mints.len() > ARB_TRACK_INCREMENTAL_MINTS_MAX {
            ctx.arb_track_selection
                .pending_full_reconcile
                .store(true, Ordering::Release);
            dirty_mints.truncate(ARB_TRACK_INCREMENTAL_MINTS_MAX);
        }
        for mint in &dirty_mints {
            ctx.refresh_mint_snapshot(mint, &protected);
        }
    }

    ctx.run_arb_track_selection_from_snapshots(full_reconcile);
    record_arb_track_selection_recompute_total();
}

fn spawn_arb_track_selection_worker(ctx: Arc<ArbContext>, mut wake_rx: mpsc::Receiver<()>) {
    tokio::spawn(async move {
        let mut coalescer = ArbTrackSelectionCoalescer::default();
        let mut last_incremental = Instant::now() - Duration::from_secs(3600);
        let mut last_full_reconcile = Instant::now() - Duration::from_secs(3600);
        let full_reconcile_min = Duration::from_millis(ARB_TRACK_FULL_RECONCILE_MIN_INTERVAL_MS);

        while wake_rx.recv().await.is_some() {
            ctx.arb_track_selection.clear_wake_pending();
            ctx.arb_track_selection
                .drain_ingress_to_coalescer(&mut coalescer);

            let coalesce_deadline =
                Instant::now() + Duration::from_millis(ARB_TRACK_SELECTION_COALESCE_MS);
            while Instant::now() < coalesce_deadline {
                match wake_rx.try_recv() {
                    Ok(()) => {
                        ctx.arb_track_selection.clear_wake_pending();
                    }
                    Err(mpsc::error::TryRecvError::Empty) => {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                    Err(mpsc::error::TryRecvError::Disconnected) => return,
                }
                ctx.arb_track_selection
                    .drain_ingress_to_coalescer(&mut coalescer);
            }

            let (dirty_mints, dirty_overflow) = coalescer.take_batch();
            if dirty_overflow {
                ctx.arb_track_selection
                    .pending_full_reconcile
                    .store(true, Ordering::Release);
            }
            let wants_full_reconcile = ctx
                .arb_track_selection
                .pending_full_reconcile
                .load(Ordering::Acquire);
            let plan = resolve_arb_track_batch_plan(
                wants_full_reconcile,
                !dirty_mints.is_empty(),
                last_full_reconcile.elapsed(),
                full_reconcile_min,
            );

            match plan {
                ArbTrackBatchPlan::Idle => {}
                ArbTrackBatchPlan::DeferFullReconcile => {
                    ctx.arb_track_selection
                        .pending_full_reconcile
                        .store(true, Ordering::Release);
                    let sleep = full_reconcile_min.saturating_sub(last_full_reconcile.elapsed());
                    if sleep > Duration::ZERO {
                        tokio::time::sleep(sleep).await;
                    }
                    ctx.arb_track_selection.schedule_worker_wake();
                    continue;
                }
                ArbTrackBatchPlan::Incremental { keep_pending_full } => {
                    if keep_pending_full {
                        ctx.arb_track_selection
                            .pending_full_reconcile
                            .store(true, Ordering::Release);
                    }
                    let min_interval = Duration::from_millis(ARB_TRACK_INCREMENTAL_MIN_INTERVAL_MS);
                    let elapsed = last_incremental.elapsed();
                    if elapsed < min_interval {
                        tokio::time::sleep(min_interval - elapsed).await;
                    }
                    let ctx_blocking = Arc::clone(&ctx);
                    let dirty = dirty_mints;
                    let blocking = tokio::task::spawn_blocking(move || {
                        run_arb_track_selection_batch(&ctx_blocking, dirty, false);
                    });
                    if blocking.await.is_err() {
                        warn!("arb track selection blocking batch join failed");
                        ctx.arb_track_selection.record_blocking_join_failed();
                    }
                    last_incremental = Instant::now();
                    if keep_pending_full {
                        ctx.arb_track_selection.schedule_worker_wake();
                    }
                }
                ArbTrackBatchPlan::FullReconcile => {
                    let _ = ctx.arb_track_selection.take_pending_full();
                    last_full_reconcile = Instant::now();
                    let ctx_blocking = Arc::clone(&ctx);
                    let blocking = tokio::task::spawn_blocking(move || {
                        run_arb_track_selection_batch(&ctx_blocking, Vec::new(), true);
                    });
                    if blocking.await.is_err() {
                        warn!("arb track selection blocking batch join failed");
                        ctx.arb_track_selection.record_blocking_join_failed();
                    }
                    last_incremental = Instant::now();
                }
            }

            if ctx.arb_track_selection.ingress_has_work() {
                ctx.arb_track_selection.schedule_worker_wake();
            }
        }
        info!("arb-strategy track selection worker stopped");
    });
}

fn spawn_arb_tracker_write_worker(
    ctx: Arc<ArbContext>,
    mut rx: mpsc::Receiver<ArbTrackerWriteJob>,
) {
    arb_tracker_write_init_worker_state();

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            maybe_arb_tracker_write_stall_watchdog_warn(ARB_TRACKER_WRITE_QUEUE_CAP);
        }
    });

    tokio::spawn(async move {
        while let Some(job) = rx.recv().await {
            process_arb_tracker_write_job(Arc::clone(&ctx), job);
            ctx.tracker_write.record_queue_depth();
        }
        info!("arb-strategy tracker write worker stopped");
    });
}

fn spawn_arb_pool_cache_sync_worker(ctx: Arc<ArbContext>, consumer: JetStreamPullConsumer) {
    tokio::spawn(async move {
        loop {
            use futures::StreamExt;
            match consumer
                .fetch()
                .max_messages(100)
                .expires(Duration::from_millis(100))
                .messages()
                .await
            {
                Ok(mut messages) => {
                    let mut pending = Vec::new();
                    while let Some(msg_result) = messages.next().await {
                        match msg_result {
                            Ok(msg) => pending.push(msg),
                            Err(e) => {
                                warn!(error = %e, "Error receiving JetStream PoolCache message");
                            }
                        }
                    }
                    if pending.is_empty() {
                        arb_pool_cache_sync_fetch_empty_inc();
                        tokio::task::yield_now().await;
                        continue;
                    }

                    arb_pool_cache_sync_messages_add(pending.len() as u64);
                    set_arb_pool_cache_apply_batch_size_gauge(pending.len() as u64);
                    arb_pool_cache_apply_batches_inc();

                    while !pending.is_empty() {
                        let chunk_len = ARB_POOL_CACHE_APPLY_BATCH_MAX.min(pending.len());
                        let chunk: Vec<_> = pending.drain(..chunk_len).collect();
                        for msg in chunk {
                            NATS_MESSAGES_RECEIVED_TOTAL.fetch_add(1, Ordering::Relaxed);
                            match serde_json::from_slice::<PoolCacheUpdate>(&msg.payload) {
                                Ok(update) => {
                                    apply_pool_cache_jetstream_message(&ctx, update);
                                }
                                Err(e) => {
                                    warn!(error = %e, "Failed to deserialize PoolCacheUpdate from JetStream");
                                }
                            }
                            if let Err(e) = msg.ack().await {
                                warn!(error = %e, "Failed to ack JetStream PoolCache message");
                            }
                        }
                        arb_pool_cache_updates_applied_add(chunk_len as u64);
                        if !pending.is_empty() {
                            tokio::task::yield_now().await;
                        }
                    }
                }
                Err(e) => {
                    trace!(error = %e, "JetStream PoolCache fetch returned (timeout or no messages)");
                    tokio::task::yield_now().await;
                }
            }
        }
    });
}

fn spawn_arb_config_js_worker(ctx: Arc<ArbContext>, consumer: JetStreamPullConsumer) {
    tokio::spawn(async move {
        loop {
            use futures::StreamExt;
            if let Ok(mut messages) = consumer.fetch().max_messages(1).messages().await {
                while let Some(msg_result) = messages.next().await {
                    if let Ok(msg) = msg_result {
                        NATS_MESSAGES_RECEIVED_TOTAL.fetch_add(1, Ordering::Relaxed);
                        match serde_json::from_slice::<ConfigUpdate>(&msg.payload) {
                            Ok(update) => {
                                if update.target_component == "arb-strategy" {
                                    info!(
                                        component = %update.target_component,
                                        keys = ?update.config.keys(),
                                        source = "jetstream",
                                        "Applying config update"
                                    );
                                    let response = ctx.apply_config_update(&update);
                                    match response.status {
                                        ConfigUpdateStatus::Applied => info!(
                                            applied = ?response.applied_keys,
                                            "Config update applied"
                                        ),
                                        ConfigUpdateStatus::Rejected => warn!(
                                            rejected = ?response.rejected_keys,
                                            "Config update rejected"
                                        ),
                                        ConfigUpdateStatus::PartiallyApplied => warn!(
                                            applied = ?response.applied_keys,
                                            rejected = ?response.rejected_keys,
                                            "Config update partially applied"
                                        ),
                                    }
                                }
                            }
                            Err(e) => {
                                warn!(error = %e, "Failed to deserialize ConfigUpdate from JetStream");
                            }
                        }
                        let _ = msg.ack().await;
                    }
                }
            }
            tokio::task::yield_now().await;
        }
    });
}

// ============================================================================
// Main
// ============================================================================

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("arb_strategy=info".parse()?)
                .add_directive("ironcrab=info".parse()?)
                // async_nats logs slow-consumer INFO per dropped message — journald amplification.
                .add_directive("async_nats=warn".parse()?),
        )
        .init();

    let args = Args::parse();
    let run_id = Uuid::new_v4().to_string();

    let initial_config = load_initial_arb_config(&args.config);

    info!(
        run_id = %run_id,
        config = %args.config.display(),
        metrics_port = args.metrics_port,
        dry_run = args.dry_run,
        "Starting arb-strategy service (Typ A Market-Driven Arbitrage)"
    );

    // Start metrics server
    let metrics_addr = std::net::SocketAddr::from(([0, 0, 0, 0], args.metrics_port));
    tokio::spawn(async move {
        if let Err(e) = serve_metrics(metrics_addr, MetricsComponent::ArbStrategy).await {
            error!(error = %e, "Metrics server failed");
        }
    });
    info!(
        port = args.metrics_port,
        "Metrics server started at /metrics"
    );

    // === P0 Check: Ensure no wallet keys are loaded ===
    if std::env::var("IRONCRAB_KEYPAIR_JSON").is_ok()
        || std::env::var("IRONCRAB_KEYPAIR_B64").is_ok()
        || std::env::var("IRONCRAB_KEYPAIR_PATH").is_ok()
    {
        error!("ERROR: Wallet key environment variables detected!");
        error!("arb-strategy is KEYLESS per architecture. Remove key variables and restart.");
        std::process::exit(1);
    }

    // Setup JSONL writer
    let log_dir = args
        .log_dir
        .unwrap_or_else(|| PathBuf::from("trade_logs/arb_intents"));
    let jsonl_config = JsonlWriterConfig::new("arb_intents").with_log_dir(&log_dir);
    let jsonl_writer = JsonlWriter::new(jsonl_config)?;
    info!(log_dir = %log_dir.display(), "JSONL writer initialized");

    // Setup NATS
    let nats = if args.dry_run {
        info!("Dry-run mode: NATS publishing disabled");
        None
    } else {
        let mut config = NatsConfig::new(&args.nats_url, "arb-strategy");
        config.request_timeout = NatsConfig::request_timeout_from_env(180);
        let mut client = NatsClient::new(config);
        if let Err(e) = client.connect().await {
            error!(error = %e, "Failed to connect to NATS");
            return Err(e);
        }
        info!(url = %args.nats_url, "Connected to NATS");
        set_readiness_nats_connected(true);
        Some(client)
    };

    let live_pool_cache = create_shared_cache();
    let multi_hop = Arc::new(MultiHopArbitrage::new(
        MultiHopConfig::default(),
        live_pool_cache.clone(),
    ));

    let (multi_hop_intent_tx, mut multi_hop_intent_rx) = mpsc::channel::<MultiHopIntentBatch>(256);
    let _multi_hop_search_worker = multi_hop.clone().spawn_search_worker(
        multi_hop_intent_tx,
        "arb-strategy".to_string(),
        BUILD_VERSION.to_string(),
        run_id.clone(),
    );

    let (two_hop_tx, two_hop_rx) =
        mpsc::channel::<ArbTwoHopWorkerJob>(ARB_TWO_HOP_WORKER_QUEUE_CAP);
    let (tracker_write_tx, tracker_write_rx) =
        mpsc::channel::<ArbTrackerWriteJob>(ARB_TRACKER_WRITE_QUEUE_CAP);
    let tracker_write = ArbTrackerWriteHandle {
        tx: tracker_write_tx,
        capacity: ARB_TRACKER_WRITE_QUEUE_CAP,
    };
    let (arb_track_selection_wake_tx, arb_track_selection_wake_rx) =
        mpsc::channel::<()>(ARB_TRACK_SELECTION_WAKE_QUEUE_CAP);
    let arb_track_selection = ArbTrackSelectionHandle {
        ingress: ArbTrackSelectionIngress::default(),
        wake_tx: arb_track_selection_wake_tx,
        pending_full_reconcile: Arc::new(AtomicBool::new(false)),
    };

    let ctx = Arc::new(ArbContext {
        run_id: run_id.clone(),
        config: RwLock::new(initial_config),
        nats,
        jsonl_writer,
        trackers: RwLock::new(HashMap::new()),
        events_received: AtomicU64::new(0),
        pools_tracked: AtomicU64::new(0),
        opportunities_found: AtomicU64::new(0),
        intents_generated: AtomicU64::new(0),
        intent_counter: AtomicU64::new(0),
        zero_amount_trades: AtomicU64::new(0),
        data_quality_rejects: AtomicU64::new(0),
        last_market_event: RwLock::new(Instant::now()),
        vault_balances: RwLock::new(HashMap::new()),
        bin_arrays: RwLock::new(HashMap::new()),
        live_pool_cache,
        known_pools: RwLock::new(HashSet::new()),
        multi_hop,
        spread_too_large_warn_last: RwLock::new(HashMap::new()),
        eligibility_forensics: ArbEligibilityForensics::new(),
        v2_eligibility_forensics: ArbV2EligibilityForensics::new(),
        arb_pinned_pools: RwLock::new(HashSet::new()),
        arb_selected_mints: RwLock::new(HashSet::new()),
        arb_trade_signal_pairs: RwLock::new(HashMap::new()),
        arb_trade_signal_pair_order: RwLock::new(Vec::new()),
        arb_track_mint_snapshots: RwLock::new(ArbTrackMintSnapshotCache::default()),
        arb_track_selection,
        arb_track_published: AtomicU64::new(0),
        two_hop_tx,
        tracker_write,
        tracker_write_coalescer: parking_lot::Mutex::new(ArbTrackerWriteCoalescer::new()),
        v2_sell_stale_recovery_pending: RwLock::new(HashMap::new()),
        pool_accounts_index: RwLock::new(HashMap::new()),
        pending_pool_accounts: RwLock::new(HashMap::new()),
    });

    spawn_arb_tracker_write_worker(Arc::clone(&ctx), tracker_write_rx);
    spawn_arb_track_selection_worker(Arc::clone(&ctx), arb_track_selection_wake_rx);
    spawn_arb_two_hop_worker(Arc::clone(&ctx), two_hop_rx);

    // Bootstrap SLAVE LivePoolCache from JetStream (same path as execution-engine).
    // Live sync uses a separate `DeliverPolicy::New` consumer (momentum-bot pattern).
    if let Some(ref nats_client) = ctx.nats {
        match bootstrap_pool_cache_from_jetstream(nats_client, &ctx.live_pool_cache).await {
            Ok((pools_recovered, _consumer)) => {
                let known_count = populate_arb_slave_from_live_pool_cache(
                    &ctx.live_pool_cache,
                    &ctx.known_pools,
                    &ctx.multi_hop,
                );
                let warmup_stats = ctx.seed_all_trackers_from_live_pool_cache();
                ctx.multi_hop.warmup_quotes_from_live_pool_cache();
                let mh_stats = ctx.multi_hop.stats();
                let live_rows = ctx.live_pool_cache.len() as u64;
                arb_strategy_bootstrap_warmup_set(
                    live_rows,
                    known_count as u64,
                    warmup_stats.tracker_seed_candidates as u64,
                    warmup_stats.tracker_seeded_pools as u64,
                );
                info!(
                    pools_recovered,
                    known_pools = known_count,
                    live_pool_cache_rows = live_rows,
                    tracker_seed_candidates = warmup_stats.tracker_seed_candidates,
                    tracker_seeded_pools = warmup_stats.tracker_seeded_pools,
                    pools_tracked = ctx.pools_tracked.load(Ordering::Relaxed),
                    multi_hop_pools = mh_stats.graph_pools,
                    multi_hop_vertices = mh_stats.graph_vertices,
                    "SLAVE CACHE: known_pools and multi-hop graph recovered from JetStream"
                );
            }
            Err(e) => {
                warn!(error = %e, "SLAVE CACHE: JetStream bootstrap failed (will rely on incremental updates)");
            }
        }
    }

    // Subscribe to MarketEvents
    let market_subscription = if let Some(ref nats) = ctx.nats {
        match nats.subscribe(TOPIC_MARKET_EVENTS).await {
            Ok(sub) => {
                info!(topic = TOPIC_MARKET_EVENTS, "Subscribed to MarketEvents");
                Some(sub)
            }
            Err(e) => {
                error!(error = %e, "Failed to subscribe to MarketEvents");
                return Err(e);
            }
        }
    } else {
        None
    };

    // Subscribe to Config Updates via JetStream (preferred) with Core NATS fallback
    // JetStream persists the last config, so we get it even if we start after control-plane
    let (config_js_consumer, config_subscription) = if let Some(ref nats) = ctx.nats {
        use async_nats::jetstream;

        let js = jetstream::new(nats.client().clone());

        // Try JetStream first (preferred - persisted config)
        let js_consumer = match js.get_stream(CONFIG_STREAM_NAME).await {
            Ok(stream) => {
                match stream
                    .create_consumer(config_consumer_config("arb-strategy"))
                    .await
                {
                    Ok(consumer) => {
                        info!(
                            stream = CONFIG_STREAM_NAME,
                            subject = %config_subject("arb-strategy"),
                            "Subscribed to JetStream Config Updates (persisted)"
                        );

                        // Bootstrap: Pull the last config message (if any)
                        match consumer.fetch().max_messages(1).messages().await {
                            Ok(mut messages) => {
                                use futures::StreamExt;
                                while let Some(msg_result) = messages.next().await {
                                    if let Ok(msg) = msg_result {
                                        if let Ok(update) =
                                            serde_json::from_slice::<ConfigUpdate>(&msg.payload)
                                        {
                                            if update.target_component == "arb-strategy" {
                                                info!(keys = ?update.config.keys(), "Bootstrap: Applying config from JetStream");
                                                let _ = ctx.apply_config_update(&update);
                                            }
                                        }
                                        let _ = msg.ack().await;
                                    }
                                }
                            }
                            Err(e) => {
                                warn!(error = %e, "Failed to bootstrap config from JetStream");
                            }
                        }

                        Some(consumer)
                    }
                    Err(e) => {
                        warn!(error = %e, "Failed to create JetStream config consumer");
                        None
                    }
                }
            }
            Err(e) => {
                info!(error = %e, stream = CONFIG_STREAM_NAME, "JetStream CONFIG_UPDATES stream not found (control-plane may not be running yet)");
                None
            }
        };

        // Also subscribe to Core NATS topic as fallback (for backward compatibility)
        let core_sub = match nats.subscribe(TOPIC_CONFIG_RELOAD).await {
            Ok(sub) => {
                info!(
                    topic = TOPIC_CONFIG_RELOAD,
                    "Subscribed to Config Updates (Core NATS fallback)"
                );
                Some(sub)
            }
            Err(e) => {
                warn!(error = %e, topic = TOPIC_CONFIG_RELOAD, "Failed to subscribe to Config Updates");
                None
            }
        };

        (js_consumer, core_sub)
    } else {
        (None, None)
    };

    // Subscribe to PoolCacheUpdates from JetStream (SLAVE sync from market-data MASTER).
    // Dedicated live consumer (`DeliverPolicy::New`) — bootstrap `LastPerSubject` does not
    // deliver incremental updates after ack (same split as momentum-bot; H1).
    let pool_cache_consumer = if let Some(ref nats) = ctx.nats {
        use async_nats::jetstream;

        let jetstream = jetstream::new(nats.client().clone());

        match jetstream.get_stream(STREAM_NAME).await {
            Ok(stream) => {
                let config = arb_strategy_pool_cache_live_consumer_config();
                match stream.create_consumer(config).await {
                    Ok(consumer) => {
                        info!(
                            stream = STREAM_NAME,
                            deliver_policy = "New",
                            durable = "arb-strategy-pool-cache-live",
                            "Arb PoolCache live consumer created (separate from bootstrap LastPerSubject)"
                        );
                        Some(consumer)
                    }
                    Err(e) => {
                        warn!(error = %e, "Failed to create arb JetStream live PoolCache consumer");
                        None
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, stream = STREAM_NAME, "JetStream stream not found (market-data may not be running)");
                None
            }
        }
    } else {
        None
    };

    // Multi-hop intent publisher (decoupled from search worker)
    let multi_hop_publish_ctx = ctx.clone();
    tokio::spawn(async move {
        while let Some(batch) = multi_hop_intent_rx.recv().await {
            for mut intent in batch.intents {
                if let Some(slot) = batch.slot {
                    intent.metadata.insert("slot".to_string(), slot.to_string());
                }
                intent
                    .metadata
                    .insert("slot_seen_at_ms".to_string(), batch.seen_at_ms.to_string());
                if let Err(e) = multi_hop_publish_ctx.jsonl_writer.write(&intent) {
                    error!(error = %e, "Failed to write multi-hop intent to JSONL");
                }
                if let Some(ref nats) = multi_hop_publish_ctx.nats {
                    if let Err(e) = nats.publish(TOPIC_TRADE_INTENTS, &intent).await {
                        warn!(error = %e, "Failed to publish multi-hop intent");
                    } else {
                        NATS_MESSAGES_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                        INTENTS_GENERATED_TOTAL.fetch_add(1, Ordering::Relaxed);
                        multi_hop_publish_ctx
                            .intents_generated
                            .fetch_add(1, Ordering::Relaxed);
                        info!(
                            intent_id = %intent.intent_id,
                            hops = intent.hop_count(),
                            return_bps = intent.expected_roi_bps,
                            "🎯 Multi-hop arb intent published"
                        );
                    }
                }
            }
        }
    });

    // Subscribe to MarketEvents and spawn decoupled ingress pipeline (NATS reader + prioritized worker).
    if let Some(sub) = market_subscription {
        info!(
            topic = TOPIC_MARKET_EVENTS,
            high_queue_cap = ARB_HIGH_EVENT_QUEUE_CAP,
            low_coalescer_cap = ARB_LOW_COALESCER_CAP,
            "Starting MarketEvent ingress pipeline (HIGH/LOW priority)"
        );
        spawn_arb_market_event_pipeline(ctx.clone(), sub);
    }

    if let Some(consumer) = pool_cache_consumer {
        info!("Starting dedicated PoolCache JetStream sync worker (decoupled from main loop)");
        spawn_arb_pool_cache_sync_worker(ctx.clone(), consumer);
    }
    if let Some(consumer) = config_js_consumer {
        info!("Starting dedicated JetStream config consumer worker (decoupled from main loop)");
        spawn_arb_config_js_worker(ctx.clone(), consumer);
    }

    // Main event loop (Core NATS config fallback, heartbeat, reconcile — heavy JetStream offloaded)
    info!("Entering main event loop");

    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    let mut cfg_sub = config_subscription;
    let mut heartbeat_interval = tokio::time::interval(Duration::from_secs(60));
    let arb_reconcile_secs = ctx.config.read().arb_track_reconcile_interval_secs;
    let mut arb_track_reconcile_interval =
        tokio::time::interval(Duration::from_secs(arb_reconcile_secs.max(10)));
    arb_track_reconcile_interval.tick().await;
    let mut last_heartbeat_events_received = 0u64;
    let mut last_heartbeat_high_processed = 0u64;
    let mut last_heartbeat_market_events_consumed =
        MARKET_EVENTS_CONSUMED_TOTAL.load(Ordering::Relaxed);
    let mut consecutive_zero_consumed_heartbeats = 0u32;

    loop {
        tokio::select! {
            // Config updates (Core NATS fallback)
            msg = async {
                if let Some(ref mut sub) = cfg_sub {
                    sub.next().await
                } else {
                    std::future::pending::<Option<ironcrab::nats::NatsMessage>>().await
                }
            } => {
                if let Some(nats_msg) = msg {
                    match serde_json::from_slice::<ConfigUpdate>(&nats_msg.payload) {
                        Ok(update) => {
                            if update.target_component == "arb-strategy" {
                                info!(component = %update.target_component, keys = ?update.config.keys(), source = "core_nats", "Applying config update");
                                let response = ctx.apply_config_update(&update);
                                match response.status {
                                    ConfigUpdateStatus::Applied => info!(applied = ?response.applied_keys, "Config update applied"),
                                    ConfigUpdateStatus::Rejected => warn!(rejected = ?response.rejected_keys, "Config update rejected"),
                                    ConfigUpdateStatus::PartiallyApplied => warn!(applied = ?response.applied_keys, rejected = ?response.rejected_keys, "Config update partially applied"),
                                }
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

            // Heartbeat
            _ = heartbeat_interval.tick() => {
                tick_arb_heartbeat_seconds_since_last_finish();

                let (records, bytes) = ctx.jsonl_writer.stats();
                let trackers_read_start = Instant::now();
                let (multi_dex_tokens, tokens_tracked) = {
                    let trackers = ctx.trackers.read();
                    let multi_dex_tokens = trackers
                        .values()
                        .filter(|t| t.pool_count_on_distinct_dexes() >= 2)
                        .count();
                    let tokens_tracked = trackers.len();
                    (multi_dex_tokens, tokens_tracked)
                };
                record_arb_heartbeat_phase(
                    ArbHeartbeatPhase::TrackersRead,
                    trackers_read_start.elapsed(),
                );

                let known_pools_count = ctx.known_pools.read().len();
                let multi_hop_stats = ctx.multi_hop.stats();
                ctx.multi_hop.refresh_quote_readiness_metrics();

                let phase_start = Instant::now();
                ctx.eligibility_forensics.maybe_emit_snapshot();
                ctx.v2_eligibility_forensics.maybe_emit_snapshot();
                record_arb_heartbeat_phase(ArbHeartbeatPhase::MaybeEmit, phase_start.elapsed());

                let phase_start = Instant::now();
                ctx.sync_pools_tracked_gauge();
                record_arb_heartbeat_phase(ArbHeartbeatPhase::SyncPools, phase_start.elapsed());

                let phase_start = Instant::now();
                ctx.prune_arb_track_stale_pools();
                record_arb_heartbeat_phase(ArbHeartbeatPhase::Prune, phase_start.elapsed());

                TOKENS_TRACKED_GAUGE.store(tokens_tracked as u64, Ordering::Relaxed);

                let high_queue_depth = ARB_SUBSCRIBER_HIGH_QUEUE_DEPTH.load(Ordering::Relaxed);
                let high_processed = ARB_SUBSCRIBER_HIGH_PROCESSED_TOTAL.load(Ordering::Relaxed);
                let events_received = ctx.events_received.load(Ordering::Relaxed);
                let high_queue_cap = ARB_HIGH_EVENT_QUEUE_CAP as u64;
                if high_queue_depth.saturating_mul(100) / high_queue_cap.max(1)
                    >= ARB_HIGH_QUEUE_WARN_PCT
                {
                    warn!(
                        high_queue_depth,
                        high_queue_cap,
                        high_processed,
                        "arb HIGH event queue above 80% capacity"
                    );
                }
                let events_delta = events_received.saturating_sub(last_heartbeat_events_received);
                let high_processed_delta =
                    high_processed.saturating_sub(last_heartbeat_high_processed);
                let market_events_consumed =
                    MARKET_EVENTS_CONSUMED_TOTAL.load(Ordering::Relaxed);
                let consumed_delta = market_events_consumed
                    .saturating_sub(last_heartbeat_market_events_consumed);
                if consumed_delta == 0 && events_delta > 0 {
                    consecutive_zero_consumed_heartbeats =
                        consecutive_zero_consumed_heartbeats.saturating_add(1);
                    if consecutive_zero_consumed_heartbeats >= 2 {
                        warn!(
                            consumed_delta,
                            events_delta,
                            market_events_consumed,
                            events_received,
                            high_queue_depth,
                            "arb market event consumer stalled (MD events received but consumed delta=0 for 2+ heartbeats)"
                        );
                    }
                } else {
                    consecutive_zero_consumed_heartbeats = 0;
                }
                if events_delta > 0 && high_processed_delta == 0 && high_queue_depth > high_queue_cap / 2
                {
                    warn!(
                        events_delta,
                        high_queue_depth,
                        high_processed,
                        "arb event pipeline may be stalled (events received but HIGH queue not draining)"
                    );
                }
                last_heartbeat_events_received = events_received;
                last_heartbeat_high_processed = high_processed;
                last_heartbeat_market_events_consumed = market_events_consumed;

                let phase_start = Instant::now();
                info!(
                    events_received,
                    market_events_consumed,
                    market_events_consumed_delta = consumed_delta,
                    pools_tracked = ctx.pools_tracked.load(Ordering::Relaxed),
                    tokens_tracked,
                    multi_dex_tokens,
                    known_pools = known_pools_count,
                    opportunities_found = ctx.opportunities_found.load(Ordering::Relaxed),
                    intents_generated = ctx.intents_generated.load(Ordering::Relaxed),
                    intents_written = records,
                    bytes_written = bytes,
                    zero_amount_trades = ctx.zero_amount_trades.load(Ordering::Relaxed),
                    data_quality_rejects = ctx.data_quality_rejects.load(Ordering::Relaxed),
                    multi_hop_vertices = multi_hop_stats.graph_vertices,
                    multi_hop_pools = multi_hop_stats.graph_pools,
                    multi_hop_cycles_found = multi_hop_stats.cycles_found,
                    multi_hop_profitable = multi_hop_stats.cycles_profitable,
                    multi_hop_enabled = ctx.multi_hop.is_enabled(),
                    arb_track_requests_published =
                        ctx.arb_track_published.load(Ordering::Relaxed),
                    high_queue_depth,
                    high_processed,
                    "arb-strategy heartbeat (SLAVE cache sync from market-data MASTER)"
                );
                record_arb_heartbeat_phase(ArbHeartbeatPhase::InfoLog, phase_start.elapsed());
                arb_heartbeat_finished();
            }

            // Phase 3: baseline arb track_requests reconcile (strategy-owned pins).
            _ = arb_track_reconcile_interval.tick() => {
                ctx.reconcile_arb_track_baseline_publish();
            }

            _ = &mut shutdown => {
                info!("Shutdown signal received");
                break;
            }
        }
    }

    // Flush JSONL on shutdown
    ctx.jsonl_writer.flush()?;
    info!(run_id = %run_id, "arb-strategy shutdown complete");

    Ok(())
}

/// Handle a single MarketEvent
async fn handle_market_event(ctx: &ArbContext, event: &MarketEvent) -> Option<TradeIntent> {
    // Update Geyser connection health timestamp on every event
    *ctx.last_market_event.write() = Instant::now();

    match &event.kind {
        MarketEventKind::Trade {
            sol_amount,
            token_amount,
            ..
        } => {
            trace!(sol_amount, token_amount, "Trade event");
        }
        MarketEventKind::PoolCreated { pool_address, .. } => {
            trace!(pool = %pool_address, "PoolCreated event");
        }
        _ => {}
    }

    match &event.kind {
        MarketEventKind::PoolCreated {
            pool_address,
            base_mint,
            quote_mint,
            dex,
            initial_liquidity_sol,
        } => {
            let liquidity = initial_liquidity_sol.unwrap_or(Decimal::ZERO);
            let _ = ctx.tracker_write.try_enqueue(
                ArbTrackerWriteJob::PoolCreated {
                    pool_address: pool_address.clone(),
                    base_mint: base_mint.clone(),
                    quote_mint: quote_mint.clone(),
                    dex: dex.clone(),
                    liquidity_sol: liquidity,
                },
                ArbTrackerWriteJobType::PoolCreated,
            );
            None
        }

        MarketEventKind::Trade {
            pool_address,
            mint,
            quote_mint,
            sol_amount,
            token_amount,
            token_decimals,
            is_buy,
            dex,
            ..
        } => {
            // Multi-hop: Event-driven cycle detection on every trade
            // This runs in parallel with the existing 2-hop detection
            if ctx.multi_hop.is_enabled() {
                let (input_mint, output_mint) = if *is_buy {
                    (NATIVE_SOL_MINT, mint.as_str())
                } else {
                    (mint.as_str(), NATIVE_SOL_MINT)
                };

                ctx.multi_hop.enqueue_pool_price_update(
                    pool_address,
                    input_mint,
                    output_mint,
                    *sol_amount,
                    *token_amount,
                    event.slot,
                    event.header.ts_unix_ms,
                );
            }

            // Scope D: 2-hop detection off the prioritized market-event worker.
            if ctx.config.read().two_hop_enabled {
                let job = ArbTwoHopTradeJob {
                    pool_address: pool_address.clone(),
                    mint: mint.clone(),
                    quote_mint: quote_mint.clone(),
                    sol_amount: *sol_amount,
                    token_amount: *token_amount,
                    token_decimals: *token_decimals,
                    is_buy: *is_buy,
                    dex: dex.clone(),
                    slot: event.slot,
                    ts_unix_ms: event.header.ts_unix_ms,
                };
                if ctx
                    .two_hop_tx
                    .try_send(ArbTwoHopWorkerJob::Trade(job))
                    .is_err()
                {
                    debug!("arb two-hop worker queue full; dropping trade detection job");
                }
            }
            None
        }

        // Handle DexPoolAccounts - cache for deterministic IX building (NO RPC in execution-engine)
        MarketEventKind::DexPoolAccounts {
            dex,
            pool_address,
            base_mint,
            quote_mint,
            accounts,
        } => {
            debug!(
                dex = %dex,
                pool = %pool_address,
                base_mint = %base_mint,
                quote_mint = %quote_mint,
                accounts_len = accounts.len(),
                "Received DexPoolAccounts event"
            );
            ctx.coalesce_dex_pool_accounts(
                pool_address.clone(),
                base_mint.clone(),
                quote_mint.clone(),
                accounts.clone(),
            );
            None
        }

        // Handle PoolStateUpdate - cache vault balances from Geyser (eliminates RPC calls)
        MarketEventKind::PoolStateUpdate {
            pool_address,
            dex,
            reserve_base,
            reserve_quote,
            update_slot,
            active_id,
            bin_step,
            base_mint,
            quote_mint,
            ..
        } => {
            ctx.coalesce_pool_state_update(
                pool_address.clone(),
                dex.clone(),
                *reserve_base,
                *reserve_quote,
                *update_slot,
                *active_id,
                *bin_step,
                base_mint.clone(),
                quote_mint.clone(),
            );
            None
        }

        // Handle BinArrayUpdate - cache Meteora DLMM bin arrays from Geyser (eliminates RPC calls)
        MarketEventKind::BinArrayUpdate {
            pool_address,
            bin_array_index,
            bins,
            update_slot,
        } => {
            ctx.handle_bin_array_update(pool_address, *bin_array_index, bins.clone(), *update_slot);
            None
        }

        // Handle TokenMintInfo - cache token program (SPL Token vs Token-2022) for ATA creation
        MarketEventKind::TokenMintInfo {
            mint,
            token_program,
            ..
        } => {
            let _ = ctx.tracker_write.try_enqueue(
                ArbTrackerWriteJob::TokenMintInfo {
                    mint: mint.clone(),
                    token_program: token_program.clone(),
                },
                ArbTrackerWriteJobType::TokenMintInfo,
            );
            None
        }

        _ => None,
    }
}

#[cfg(test)]
mod event_pipeline_tests {
    use super::*;
    use ironcrab::ipc::MarketEventKind;

    const TEST_COMPONENT: &str = "test";
    const TEST_BUILD: &str = "0.0.0";
    const TEST_RUN: &str = "run-test";

    fn sample_trade_event(pool: &str) -> MarketEvent {
        MarketEvent::new(
            TEST_COMPONENT,
            TEST_BUILD,
            TEST_RUN,
            format!("evt-trade-{pool}"),
            "geyser",
            Some(1),
            MarketEventKind::Trade {
                pool_address: pool.to_string(),
                mint: "TokenMint11111111111111111111111111111111".to_string(),
                quote_mint: NATIVE_SOL_MINT.to_string(),
                trader: "Trader111111111111111111111111111111111111".to_string(),
                sol_amount: 1_000_000,
                token_amount: 1_000_000,
                token_decimals: 6,
                is_buy: true,
                signature: None,
                dex: "raydium".to_string(),
                creator: None,
                token_program: None,
            },
        )
    }

    fn sample_pool_created(pool: &str, base: &str, quote: &str) -> MarketEvent {
        MarketEvent::new(
            TEST_COMPONENT,
            TEST_BUILD,
            TEST_RUN,
            format!("evt-pc-{pool}"),
            "geyser",
            Some(1),
            MarketEventKind::PoolCreated {
                pool_address: pool.to_string(),
                base_mint: base.to_string(),
                quote_mint: quote.to_string(),
                dex: "raydium".to_string(),
                initial_liquidity_sol: Some(Decimal::ONE),
            },
        )
    }

    fn sample_bin_array_update(pool: &str, bin_array_index: i64, update_slot: u64) -> MarketEvent {
        MarketEvent::new(
            TEST_COMPONENT,
            TEST_BUILD,
            TEST_RUN,
            format!("evt-bin-{pool}-{bin_array_index}-{update_slot}"),
            "geyser",
            Some(1),
            MarketEventKind::BinArrayUpdate {
                pool_address: pool.to_string(),
                bin_array_index,
                bins: vec![ironcrab::ipc::BinData {
                    offset: 0,
                    amount_x: update_slot,
                    amount_y: 1,
                }],
                update_slot,
            },
        )
    }

    #[test]
    fn pool_created_filter_skips_non_relevant_pairs() {
        assert!(!should_enqueue_pool_created(
            "TokenA1111111111111111111111111111111111",
            "TokenB1111111111111111111111111111111111",
        ));
        assert!(should_enqueue_pool_created(
            "TokenMint11111111111111111111111111111111",
            NATIVE_SOL_MINT,
        ));
    }

    #[test]
    fn filtered_pool_created_marks_liveness_without_low_enqueue() {
        let last = RwLock::new(Instant::now() - Duration::from_secs(3600));
        let event = sample_pool_created(
            "pool-irrelevant",
            "TokenA1111111111111111111111111111111111",
            "TokenB1111111111111111111111111111111111",
        );

        *last.write() = Instant::now();
        assert!(
            last.read().elapsed().as_secs() < GEYSER_CONNECTION_TIMEOUT_SECS,
            "deserialized MarketEvent should refresh Geyser liveness before ingress filters"
        );

        let known = HashSet::new();
        let pinned = HashSet::new();
        let decision = arb_market_event_ingress_priority(&event, &known, &pinned);
        assert_eq!(decision, None);

        let mut coalescer = ArbLowEventCoalescer::new();
        if let Some(ArbEventPriority::Low) = decision {
            coalescer.insert(event, 16);
        }
        assert_eq!(coalescer.len(), 0);
    }

    #[test]
    fn trade_events_classify_as_high_priority() {
        let known = HashSet::new();
        let pinned = HashSet::new();
        let event = sample_trade_event("pool-trade");
        assert_eq!(
            classify_market_event_priority(&event, &known, &pinned),
            ArbEventPriority::High
        );
    }

    #[test]
    fn known_pool_state_update_is_high_unknown_is_low() {
        let mut known = HashSet::new();
        known.insert("pool-known".to_string());
        let high_event = MarketEvent::new(
            TEST_COMPONENT,
            TEST_BUILD,
            TEST_RUN,
            "evt-psu-high".to_string(),
            "geyser",
            Some(1),
            MarketEventKind::PoolStateUpdate {
                pool_address: "pool-known".to_string(),
                dex: "orca".to_string(),
                reserve_base: 1,
                reserve_quote: 1,
                update_slot: 1,
                active_id: None,
                bin_step: None,
                base_mint: NATIVE_SOL_MINT.to_string(),
                quote_mint: "TokenMint11111111111111111111111111111111".to_string(),
            },
        );
        let low_event = MarketEvent::new(
            TEST_COMPONENT,
            TEST_BUILD,
            TEST_RUN,
            "evt-psu-low".to_string(),
            "geyser",
            Some(1),
            MarketEventKind::PoolStateUpdate {
                pool_address: "pool-unknown".to_string(),
                dex: "orca".to_string(),
                reserve_base: 1,
                reserve_quote: 1,
                update_slot: 1,
                active_id: None,
                bin_step: None,
                base_mint: NATIVE_SOL_MINT.to_string(),
                quote_mint: "TokenMint11111111111111111111111111111111".to_string(),
            },
        );
        let pinned = HashSet::new();
        assert_eq!(
            classify_market_event_priority(&high_event, &known, &pinned),
            ArbEventPriority::High
        );
        assert_eq!(
            classify_market_event_priority(&low_event, &known, &pinned),
            ArbEventPriority::Low
        );
    }

    #[test]
    fn pinned_bin_array_update_is_high_even_when_pool_unknown() {
        let known = HashSet::new();
        let mut pinned = HashSet::new();
        pinned.insert("pool-pinned-dlmm".to_string());
        let event = sample_bin_array_update("pool-pinned-dlmm", 0, 100);
        assert_eq!(
            classify_market_event_priority(&event, &known, &pinned),
            ArbEventPriority::High
        );
    }

    #[test]
    fn low_coalescer_keeps_distinct_bin_array_indices_per_pool() {
        let mut coalescer = ArbLowEventCoalescer::new();
        let e0 = sample_bin_array_update("pool-dlmm", 0, 100);
        let e1 = sample_bin_array_update("pool-dlmm", 1, 200);
        assert_eq!(coalescer.insert(e0, 16), LowCoalescerInsert::Queued);
        assert_eq!(coalescer.insert(e1, 16), LowCoalescerInsert::Queued);
        let drained = coalescer.drain();
        assert_eq!(drained.len(), 2);
        let mut indices: Vec<i64> = drained
            .iter()
            .filter_map(|event| match &event.kind {
                MarketEventKind::BinArrayUpdate {
                    bin_array_index, ..
                } => Some(*bin_array_index),
                _ => None,
            })
            .collect();
        indices.sort_unstable();
        assert_eq!(indices, vec![0, 1]);
    }

    #[test]
    fn low_coalescer_coalesces_same_bin_array_index_latest_wins() {
        let mut coalescer = ArbLowEventCoalescer::new();
        let e_old = sample_bin_array_update("pool-dlmm", 3, 100);
        let e_new = sample_bin_array_update("pool-dlmm", 3, 999);
        assert_eq!(coalescer.insert(e_old, 16), LowCoalescerInsert::Queued);
        assert_eq!(coalescer.insert(e_new, 16), LowCoalescerInsert::Coalesced);
        let drained = coalescer.drain();
        assert_eq!(drained.len(), 1);
        assert_eq!(
            drained[0].kind,
            MarketEventKind::BinArrayUpdate {
                pool_address: "pool-dlmm".to_string(),
                bin_array_index: 3,
                bins: vec![ironcrab::ipc::BinData {
                    offset: 0,
                    amount_x: 999,
                    amount_y: 1,
                }],
                update_slot: 999,
            }
        );
    }

    #[test]
    fn low_coalescer_latest_wins_and_counts_coalesce() {
        let before = ironcrab::metrics::ARB_SUBSCRIBER_LOW_COALESCED_TOTAL.load(Ordering::Relaxed);
        let mut coalescer = ArbLowEventCoalescer::new();
        let e1 = sample_pool_created(
            "pool-1",
            "TokenMint11111111111111111111111111111111",
            NATIVE_SOL_MINT,
        );
        let e2 = sample_pool_created(
            "pool-1",
            "TokenMint22222222222222222222222222222222",
            NATIVE_SOL_MINT,
        );
        assert_eq!(coalescer.insert(e1, 16), LowCoalescerInsert::Queued);
        assert_eq!(coalescer.insert(e2, 16), LowCoalescerInsert::Coalesced);
        let drained = coalescer.drain();
        assert_eq!(drained.len(), 1);
        assert_eq!(
            drained[0].kind,
            MarketEventKind::PoolCreated {
                pool_address: "pool-1".to_string(),
                base_mint: "TokenMint22222222222222222222222222222222".to_string(),
                quote_mint: NATIVE_SOL_MINT.to_string(),
                dex: "raydium".to_string(),
                initial_liquidity_sol: Some(Decimal::ONE),
            }
        );
        let after = ironcrab::metrics::ARB_SUBSCRIBER_LOW_COALESCED_TOTAL.load(Ordering::Relaxed);
        assert!(after > before);
    }

    #[test]
    fn low_coalescer_eviction_increments_dropped_metric() {
        let before = ironcrab::metrics::ARB_SUBSCRIBER_LOW_DROPPED_TOTAL.load(Ordering::Relaxed);
        let mut coalescer = ArbLowEventCoalescer::new();
        for i in 0..5 {
            let pool = format!("pool-{i}");
            let event = sample_pool_created(
                &pool,
                "TokenMint11111111111111111111111111111111",
                NATIVE_SOL_MINT,
            );
            let _ = coalescer.insert(event, 2);
        }
        let after = ironcrab::metrics::ARB_SUBSCRIBER_LOW_DROPPED_TOTAL.load(Ordering::Relaxed);
        assert!(after > before);
    }

    #[test]
    fn high_priority_channel_does_not_drop_when_within_capacity() {
        let (tx, mut rx) = mpsc::channel::<MarketEvent>(8);
        let event = sample_trade_event("pool-h");
        tx.try_send(event)
            .expect("HIGH trade must enqueue without drop");
        assert!(rx.try_recv().is_ok());
    }

    #[test]
    fn high_priority_channel_drops_or_downgrades_when_full_instead_of_blocking() {
        let (tx, _rx) = mpsc::channel::<MarketEvent>(2);
        let coalescer = parking_lot::Mutex::new(ArbLowEventCoalescer::new());
        let notify = tokio::sync::Notify::new();

        let trade_a = sample_trade_event("pool-trade-a");
        let trade_b = sample_trade_event("pool-trade-b");
        let pool_state_event = MarketEvent::new(
            TEST_COMPONENT,
            TEST_BUILD,
            TEST_RUN,
            "evt-psu-full".to_string(),
            "geyser",
            Some(1),
            MarketEventKind::PoolStateUpdate {
                pool_address: "pool-known-full".to_string(),
                dex: "orca".to_string(),
                reserve_base: 1,
                reserve_quote: 1,
                update_slot: 1,
                active_id: None,
                bin_step: None,
                base_mint: NATIVE_SOL_MINT.to_string(),
                quote_mint: "TokenMint11111111111111111111111111111111".to_string(),
            },
        );

        assert_eq!(
            try_enqueue_high_priority(&tx, &coalescer, &notify, trade_a),
            HighEnqueueOutcome::Enqueued
        );
        assert_eq!(
            try_enqueue_high_priority(&tx, &coalescer, &notify, trade_b),
            HighEnqueueOutcome::Enqueued
        );
        assert_eq!(tx.capacity(), 0, "HIGH channel must be full");

        let before_dropped =
            ironcrab::metrics::ARB_SUBSCRIBER_HIGH_DROPPED_TOTAL.load(Ordering::Relaxed);
        assert_eq!(
            try_enqueue_high_priority(&tx, &coalescer, &notify, pool_state_event),
            HighEnqueueOutcome::DowngradedToLow
        );
        assert_eq!(tx.capacity(), 0);
        assert_eq!(coalescer.lock().len(), 1);
        assert!(
            ironcrab::metrics::ARB_SUBSCRIBER_HIGH_DROPPED_TOTAL.load(Ordering::Relaxed)
                > before_dropped
        );

        let trade_overflow = sample_trade_event("pool-trade-overflow");
        assert_eq!(
            try_enqueue_high_priority(&tx, &coalescer, &notify, trade_overflow),
            HighEnqueueOutcome::DowngradedToLow
        );
        assert_eq!(coalescer.lock().len(), 2);
    }

    fn sample_pool_state_update(
        pool: &str,
        slot: u64,
    ) -> (
        String,
        String,
        u64,
        u64,
        u64,
        Option<i32>,
        Option<u16>,
        String,
        String,
    ) {
        (
            pool.to_string(),
            "orca".to_string(),
            1_000_000,
            2_000_000_000,
            slot,
            None,
            None,
            NATIVE_SOL_MINT.to_string(),
            "TokenMint11111111111111111111111111111111".to_string(),
        )
    }

    #[test]
    fn tracker_write_coalescer_latest_slot_wins_on_flush() {
        let (tx, mut rx) = mpsc::channel::<ArbTrackerWriteJob>(8);
        let handle = ArbTrackerWriteHandle { tx, capacity: 8 };
        let mut coalescer = ArbTrackerWriteCoalescer::new();
        let (pool, dex, rb, rq, _, aid, bs, bm, qm) = sample_pool_state_update("pool-coalesce", 1);
        coalescer.insert_pool_state_update(
            pool.clone(),
            dex.clone(),
            rb,
            rq,
            1,
            aid,
            bs,
            bm.clone(),
            qm.clone(),
            16,
        );
        coalescer.insert_pool_state_update(pool, dex, rb, rq, 99, aid, bs, bm, qm, 16);
        assert_eq!(coalescer.pending_len(), 1);
        coalescer.flush(&handle);
        let job = rx.try_recv().expect("one coalesced pool state must flush");
        match job {
            ArbTrackerWriteJob::PoolStateUpdate { update_slot, .. } => {
                assert_eq!(update_slot, 99);
            }
            _ => panic!("expected PoolStateUpdate"),
        }
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn tracker_write_coalescer_discards_older_slot() {
        let mut coalescer = ArbTrackerWriteCoalescer::new();
        let (pool, dex, rb, rq, _, aid, bs, bm, qm) =
            sample_pool_state_update("pool-monotonic", 50);
        coalescer.insert_pool_state_update(
            pool.clone(),
            dex.clone(),
            rb,
            rq,
            50,
            aid,
            bs,
            bm.clone(),
            qm.clone(),
            16,
        );
        let result = coalescer.insert_pool_state_update(pool, dex, rb, rq, 10, aid, bs, bm, qm, 16);
        assert_eq!(result, TrackerWriteCoalescerInsert::Coalesced);
        let drained = coalescer.drain_pool_state_for_test();
        assert_eq!(drained.len(), 1);
        match &drained[0] {
            ArbTrackerWriteJob::PoolStateUpdate { update_slot, .. } => assert_eq!(*update_slot, 50),
            _ => panic!("expected PoolStateUpdate"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn coalesced_pool_state_in_apply_trade_snapshot_before_check() {
        use ironcrab::execution::live_pool_cache::create_shared_cache;

        let live_pool_cache = create_shared_cache();
        let log_dir = std::env::temp_dir().join(format!("arb_coalesce_snap_{}", Uuid::new_v4()));
        std::fs::create_dir_all(&log_dir).expect("test log dir");
        let jsonl_writer =
            JsonlWriter::new(JsonlWriterConfig::new("arb_coalesce_snap").with_log_dir(log_dir))
                .expect("jsonl writer");
        let (tracker_write_tx, tracker_write_rx) =
            mpsc::channel::<ArbTrackerWriteJob>(ARB_TRACKER_WRITE_QUEUE_CAP);
        let tracker_write = ArbTrackerWriteHandle {
            tx: tracker_write_tx,
            capacity: ARB_TRACKER_WRITE_QUEUE_CAP,
        };
        let (two_hop_tx, _two_hop_rx) = mpsc::channel::<ArbTwoHopWorkerJob>(1);

        let ctx = Arc::new(ArbContext {
            run_id: TEST_RUN.to_string(),
            config: RwLock::new(ArbConfig::default()),
            nats: None,
            jsonl_writer,
            trackers: RwLock::new(HashMap::new()),
            events_received: AtomicU64::new(0),
            pools_tracked: AtomicU64::new(0),
            opportunities_found: AtomicU64::new(0),
            intents_generated: AtomicU64::new(0),
            intent_counter: AtomicU64::new(0),
            zero_amount_trades: AtomicU64::new(0),
            data_quality_rejects: AtomicU64::new(0),
            last_market_event: RwLock::new(Instant::now()),
            vault_balances: RwLock::new(HashMap::new()),
            bin_arrays: RwLock::new(HashMap::new()),
            live_pool_cache,
            known_pools: RwLock::new(HashSet::new()),
            multi_hop: Arc::new(MultiHopArbitrage::new(
                MultiHopConfig::default(),
                create_shared_cache(),
            )),
            spread_too_large_warn_last: RwLock::new(HashMap::new()),
            eligibility_forensics: ArbEligibilityForensics::new(),
            v2_eligibility_forensics: ArbV2EligibilityForensics::new(),
            arb_pinned_pools: RwLock::new(HashSet::new()),
            arb_selected_mints: RwLock::new(HashSet::new()),
            arb_trade_signal_pairs: RwLock::new(HashMap::new()),
            arb_trade_signal_pair_order: RwLock::new(Vec::new()),
            arb_track_mint_snapshots: RwLock::new(ArbTrackMintSnapshotCache::default()),
            arb_track_selection: dummy_arb_track_selection_handle(),
            arb_track_published: AtomicU64::new(0),
            two_hop_tx,
            tracker_write,
            tracker_write_coalescer: parking_lot::Mutex::new(ArbTrackerWriteCoalescer::new()),
            v2_sell_stale_recovery_pending: RwLock::new(HashMap::new()),
            pool_accounts_index: RwLock::new(HashMap::new()),
            pending_pool_accounts: RwLock::new(HashMap::new()),
        });
        spawn_arb_tracker_write_worker(ctx.clone(), tracker_write_rx);

        let pool = "pool-coalesce-snap";
        let mint = "TokenMint11111111111111111111111111111111";
        ctx.handle_pool_created(pool, mint, NATIVE_SOL_MINT, "raydium", Decimal::ONE);

        const COALESCED_SLOT: u64 = 99;
        ctx.coalesce_pool_state_update(
            pool.to_string(),
            "raydium".to_string(),
            5_000_000,
            10_000_000_000,
            COALESCED_SLOT,
            None,
            None,
            NATIVE_SOL_MINT.to_string(),
            mint.to_string(),
        );

        let (reply_tx, reply_rx) = oneshot::channel();
        ctx.flush_tracker_write_coalescer();
        assert!(
            ctx.tracker_write.try_enqueue(
                ArbTrackerWriteJob::ApplyTrade {
                    job: ArbTwoHopTradeJob {
                        pool_address: pool.to_string(),
                        mint: mint.to_string(),
                        quote_mint: NATIVE_SOL_MINT.to_string(),
                        sol_amount: 10_000_000,
                        token_amount: 1_000_000,
                        token_decimals: 6,
                        is_buy: true,
                        dex: "raydium".to_string(),
                        slot: Some(1),
                        ts_unix_ms: 1,
                    },
                    reply: reply_tx,
                },
                ArbTrackerWriteJobType::ApplyTrade,
            ),
            "ApplyTrade enqueue"
        );

        let apply_result = tokio::time::timeout(Duration::from_millis(500), reply_rx)
            .await
            .expect("ApplyTrade reply timed out")
            .expect("ApplyTrade reply channel closed")
            .expect("ApplyTrade must succeed");
        let vault = apply_result
            .vault_balances
            .get(pool)
            .expect("coalesced pool state must be in ApplyTrade snapshot");
        assert_eq!(
            vault.update_slot, COALESCED_SLOT,
            "check_arbitrage snapshot must include flushed coalesced reserves"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn apply_trade_does_not_block_writer_pool_state_updates() {
        use ironcrab::execution::live_pool_cache::create_shared_cache;

        let live_pool_cache = create_shared_cache();
        let log_dir = std::env::temp_dir().join(format!("arb_writer_test_{}", Uuid::new_v4()));
        std::fs::create_dir_all(&log_dir).expect("test log dir");
        let jsonl_writer =
            JsonlWriter::new(JsonlWriterConfig::new("arb_writer_test").with_log_dir(log_dir))
                .expect("jsonl writer");
        let (tracker_write_tx, tracker_write_rx) =
            mpsc::channel::<ArbTrackerWriteJob>(ARB_TRACKER_WRITE_QUEUE_CAP);
        let tracker_write = ArbTrackerWriteHandle {
            tx: tracker_write_tx,
            capacity: ARB_TRACKER_WRITE_QUEUE_CAP,
        };
        let (two_hop_tx, _two_hop_rx) = mpsc::channel::<ArbTwoHopWorkerJob>(1);

        let ctx = Arc::new(ArbContext {
            run_id: TEST_RUN.to_string(),
            config: RwLock::new(ArbConfig::default()),
            nats: None,
            jsonl_writer,
            trackers: RwLock::new(HashMap::new()),
            events_received: AtomicU64::new(0),
            pools_tracked: AtomicU64::new(0),
            opportunities_found: AtomicU64::new(0),
            intents_generated: AtomicU64::new(0),
            intent_counter: AtomicU64::new(0),
            zero_amount_trades: AtomicU64::new(0),
            data_quality_rejects: AtomicU64::new(0),
            last_market_event: RwLock::new(Instant::now()),
            vault_balances: RwLock::new(HashMap::new()),
            bin_arrays: RwLock::new(HashMap::new()),
            live_pool_cache,
            known_pools: RwLock::new(HashSet::new()),
            multi_hop: Arc::new(MultiHopArbitrage::new(
                MultiHopConfig::default(),
                create_shared_cache(),
            )),
            spread_too_large_warn_last: RwLock::new(HashMap::new()),
            eligibility_forensics: ArbEligibilityForensics::new(),
            v2_eligibility_forensics: ArbV2EligibilityForensics::new(),
            arb_pinned_pools: RwLock::new(HashSet::new()),
            arb_selected_mints: RwLock::new(HashSet::new()),
            arb_trade_signal_pairs: RwLock::new(HashMap::new()),
            arb_trade_signal_pair_order: RwLock::new(Vec::new()),
            arb_track_mint_snapshots: RwLock::new(ArbTrackMintSnapshotCache::default()),
            arb_track_selection: dummy_arb_track_selection_handle(),
            arb_track_published: AtomicU64::new(0),
            two_hop_tx,
            tracker_write,
            tracker_write_coalescer: parking_lot::Mutex::new(ArbTrackerWriteCoalescer::new()),
            v2_sell_stale_recovery_pending: RwLock::new(HashMap::new()),
            pool_accounts_index: RwLock::new(HashMap::new()),
            pending_pool_accounts: RwLock::new(HashMap::new()),
        });
        spawn_arb_tracker_write_worker(ctx.clone(), tracker_write_rx);

        let pool = "pool-writer-decouple";
        let mint = "TokenMint11111111111111111111111111111111";
        ctx.handle_pool_created(pool, mint, NATIVE_SOL_MINT, "raydium", Decimal::ONE);

        ctx.coalesce_pool_state_update(
            pool.to_string(),
            "raydium".to_string(),
            5_000_000,
            10_000_000_000,
            42,
            None,
            None,
            NATIVE_SOL_MINT.to_string(),
            mint.to_string(),
        );

        let (reply_tx, reply_rx) = oneshot::channel();
        ctx.flush_tracker_write_coalescer();
        assert!(
            ctx.tracker_write.try_enqueue(
                ArbTrackerWriteJob::ApplyTrade {
                    job: ArbTwoHopTradeJob {
                        pool_address: pool.to_string(),
                        mint: mint.to_string(),
                        quote_mint: NATIVE_SOL_MINT.to_string(),
                        sol_amount: 10_000_000,
                        token_amount: 1_000_000,
                        token_decimals: 6,
                        is_buy: true,
                        dex: "raydium".to_string(),
                        slot: Some(1),
                        ts_unix_ms: 1,
                    },
                    reply: reply_tx,
                },
                ArbTrackerWriteJobType::ApplyTrade,
            ),
            "ApplyTrade enqueue"
        );

        let apply_result = tokio::time::timeout(Duration::from_millis(500), reply_rx)
            .await
            .expect("ApplyTrade reply timed out")
            .expect("ApplyTrade reply channel closed")
            .expect("ApplyTrade must succeed");
        assert!(
            apply_result
                .vault_balances
                .get(pool)
                .is_some_and(|v| v.update_slot == 42),
            "ApplyTrade snapshot must include flushed coalesced reserves"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn heartbeat_ticks_under_pool_cache_burst_and_parallel_trades() {
        use ironcrab::execution::live_pool_cache::create_shared_cache;

        let live_pool_cache = create_shared_cache();
        let log_dir = std::env::temp_dir().join(format!("arb_burst_test_{}", Uuid::new_v4()));
        std::fs::create_dir_all(&log_dir).expect("test log dir");
        let jsonl_writer =
            JsonlWriter::new(JsonlWriterConfig::new("arb_burst_test").with_log_dir(log_dir))
                .expect("jsonl writer");
        let (tracker_write_tx, tracker_write_rx) =
            mpsc::channel::<ArbTrackerWriteJob>(ARB_TRACKER_WRITE_QUEUE_CAP);
        let tracker_write = ArbTrackerWriteHandle {
            tx: tracker_write_tx,
            capacity: ARB_TRACKER_WRITE_QUEUE_CAP,
        };
        let (two_hop_tx, _two_hop_rx) = mpsc::channel::<ArbTwoHopWorkerJob>(1);

        let ctx = Arc::new(ArbContext {
            run_id: TEST_RUN.to_string(),
            config: RwLock::new(ArbConfig::default()),
            nats: None,
            jsonl_writer,
            trackers: RwLock::new(HashMap::new()),
            events_received: AtomicU64::new(0),
            pools_tracked: AtomicU64::new(0),
            opportunities_found: AtomicU64::new(0),
            intents_generated: AtomicU64::new(0),
            intent_counter: AtomicU64::new(0),
            zero_amount_trades: AtomicU64::new(0),
            data_quality_rejects: AtomicU64::new(0),
            last_market_event: RwLock::new(Instant::now()),
            vault_balances: RwLock::new(HashMap::new()),
            bin_arrays: RwLock::new(HashMap::new()),
            live_pool_cache,
            known_pools: RwLock::new(HashSet::new()),
            multi_hop: Arc::new(MultiHopArbitrage::new(
                MultiHopConfig::default(),
                create_shared_cache(),
            )),
            spread_too_large_warn_last: RwLock::new(HashMap::new()),
            eligibility_forensics: ArbEligibilityForensics::new(),
            v2_eligibility_forensics: ArbV2EligibilityForensics::new(),
            arb_pinned_pools: RwLock::new(HashSet::new()),
            arb_selected_mints: RwLock::new(HashSet::new()),
            arb_trade_signal_pairs: RwLock::new(HashMap::new()),
            arb_trade_signal_pair_order: RwLock::new(Vec::new()),
            arb_track_mint_snapshots: RwLock::new(ArbTrackMintSnapshotCache::default()),
            arb_track_selection: dummy_arb_track_selection_handle(),
            arb_track_published: AtomicU64::new(0),
            two_hop_tx,
            tracker_write,
            tracker_write_coalescer: parking_lot::Mutex::new(ArbTrackerWriteCoalescer::new()),
            v2_sell_stale_recovery_pending: RwLock::new(HashMap::new()),
            pool_accounts_index: RwLock::new(HashMap::new()),
            pending_pool_accounts: RwLock::new(HashMap::new()),
        });
        spawn_arb_tracker_write_worker(ctx.clone(), tracker_write_rx);

        let token_mint = Pubkey::new_unique().to_string();
        let trade_mint = token_mint.clone();
        let heartbeat_ctx = ctx.clone();
        let heartbeat = tokio::spawn(async move {
            let mut ticks = 0u32;
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let deadline = Instant::now() + Duration::from_secs(3);
            while Instant::now() < deadline {
                tokio::select! {
                    _ = interval.tick() => {
                        ticks += 1;
                        let _ = heartbeat_ctx.trackers.read().len();
                    }
                }
            }
            ticks
        });

        let burst_ctx = ctx.clone();
        let burst = tokio::spawn(async move {
            for i in 0..100usize {
                let pool = Pubkey::new_unique();
                let update = PoolCacheUpdate::new_balance_updated(
                    TEST_COMPONENT,
                    TEST_BUILD,
                    TEST_RUN,
                    pool.to_string(),
                    "raydium".to_string(),
                    token_mint.clone(),
                    NATIVE_SOL_MINT.to_string(),
                    1_000_000_000,
                    1_000_000_000,
                    i as u64 + 100,
                );
                apply_pool_cache_jetstream_message(&burst_ctx, update);
                if (i + 1) % ARB_POOL_CACHE_APPLY_BATCH_MAX == 0 {
                    tokio::task::yield_now().await;
                }
            }
        });

        let trade_ctx = ctx.clone();
        let trades = tokio::spawn(async move {
            for i in 0..50usize {
                let pool = format!("pool-trade-{i}");
                let (reply_tx, reply_rx) = oneshot::channel();
                let job = ArbTwoHopTradeJob {
                    pool_address: pool,
                    mint: trade_mint.clone(),
                    quote_mint: NATIVE_SOL_MINT.to_string(),
                    sol_amount: 10_000_000,
                    token_amount: 1_000_000,
                    token_decimals: 6,
                    is_buy: true,
                    dex: "raydium".to_string(),
                    slot: Some(1),
                    ts_unix_ms: 1,
                };
                trade_ctx
                    .tracker_write
                    .enqueue(
                        ArbTrackerWriteJob::ApplyTrade {
                            job,
                            reply: reply_tx,
                        },
                        ArbTrackerWriteJobType::ApplyTrade,
                    )
                    .await;
                let _ = tokio::time::timeout(Duration::from_secs(1), reply_rx).await;
            }
        });

        let ticks = tokio::time::timeout(Duration::from_secs(5), heartbeat)
            .await
            .expect("heartbeat task timed out")
            .expect("heartbeat task panicked");
        burst.await.expect("burst task panicked");
        trades.await.expect("trades task panicked");
        assert!(
            ticks >= 5,
            "expected heartbeat to tick under PoolCache burst + trade load, got {ticks}"
        );
    }

    fn sample_pool_state_market_event(pool: &str) -> MarketEvent {
        MarketEvent::new(
            TEST_COMPONENT,
            TEST_BUILD,
            TEST_RUN,
            format!("evt-psu-{pool}"),
            "geyser",
            Some(1),
            MarketEventKind::PoolStateUpdate {
                pool_address: pool.to_string(),
                dex: "raydium".to_string(),
                reserve_base: 5_000_000,
                reserve_quote: 10_000_000_000,
                update_slot: 1,
                active_id: None,
                bin_step: None,
                base_mint: NATIVE_SOL_MINT.to_string(),
                quote_mint: "TokenMint11111111111111111111111111111111".to_string(),
            },
        )
    }

    /// C1h5 regression: market-event consumer must keep progressing under load while
    /// sell-leg recovery runs on spawn_blocking (no pipeline hang).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn event_pipeline_continues_under_load_after_c1h5() {
        use ironcrab::execution::live_pool_cache::create_shared_cache;
        use ironcrab::metrics::ARB_SUBSCRIBER_HIGH_PROCESSED_TOTAL;

        let live_pool_cache = create_shared_cache();
        let log_dir = std::env::temp_dir().join(format!("arb_c1h5_pipeline_{}", Uuid::new_v4()));
        std::fs::create_dir_all(&log_dir).expect("test log dir");
        let jsonl_writer =
            JsonlWriter::new(JsonlWriterConfig::new("arb_c1h5_pipeline").with_log_dir(log_dir))
                .expect("jsonl writer");
        let (tracker_write_tx, tracker_write_rx) =
            mpsc::channel::<ArbTrackerWriteJob>(ARB_TRACKER_WRITE_QUEUE_CAP);
        let tracker_write = ArbTrackerWriteHandle {
            tx: tracker_write_tx,
            capacity: ARB_TRACKER_WRITE_QUEUE_CAP,
        };
        let (two_hop_tx, two_hop_rx) = mpsc::channel::<ArbTwoHopWorkerJob>(64);

        let config = ArbConfig {
            arb_two_hop_v2_enabled: true,
            two_hop_enabled: true,
            ..Default::default()
        };

        let mut known_pools = HashSet::new();
        for i in 0..8usize {
            known_pools.insert(format!("pool-{i}"));
        }

        let ctx = Arc::new(ArbContext {
            run_id: TEST_RUN.to_string(),
            config: RwLock::new(config),
            nats: None,
            jsonl_writer,
            trackers: RwLock::new(HashMap::new()),
            events_received: AtomicU64::new(0),
            pools_tracked: AtomicU64::new(0),
            opportunities_found: AtomicU64::new(0),
            intents_generated: AtomicU64::new(0),
            intent_counter: AtomicU64::new(0),
            zero_amount_trades: AtomicU64::new(0),
            data_quality_rejects: AtomicU64::new(0),
            last_market_event: RwLock::new(Instant::now()),
            vault_balances: RwLock::new(HashMap::new()),
            bin_arrays: RwLock::new(HashMap::new()),
            live_pool_cache,
            known_pools: RwLock::new(known_pools),
            multi_hop: Arc::new(MultiHopArbitrage::new(
                MultiHopConfig::default(),
                create_shared_cache(),
            )),
            spread_too_large_warn_last: RwLock::new(HashMap::new()),
            eligibility_forensics: ArbEligibilityForensics::new(),
            v2_eligibility_forensics: ArbV2EligibilityForensics::new(),
            arb_pinned_pools: RwLock::new({
                let mut pinned = HashSet::new();
                pinned.insert("pool-1".to_string());
                pinned
            }),
            arb_selected_mints: RwLock::new(HashSet::new()),
            arb_trade_signal_pairs: RwLock::new(HashMap::new()),
            arb_trade_signal_pair_order: RwLock::new(Vec::new()),
            arb_track_mint_snapshots: RwLock::new(ArbTrackMintSnapshotCache::default()),
            arb_track_selection: dummy_arb_track_selection_handle(),
            arb_track_published: AtomicU64::new(0),
            two_hop_tx,
            tracker_write,
            tracker_write_coalescer: parking_lot::Mutex::new(ArbTrackerWriteCoalescer::new()),
            v2_sell_stale_recovery_pending: RwLock::new(HashMap::new()),
            pool_accounts_index: RwLock::new(HashMap::new()),
            pending_pool_accounts: RwLock::new(HashMap::new()),
        });
        spawn_arb_tracker_write_worker(ctx.clone(), tracker_write_rx);
        spawn_arb_two_hop_worker(ctx.clone(), two_hop_rx);

        let consumed_before = MARKET_EVENTS_CONSUMED_TOTAL.load(Ordering::Relaxed);
        let high_before = ARB_SUBSCRIBER_HIGH_PROCESSED_TOTAL.load(Ordering::Relaxed);

        let events_ctx = ctx.clone();
        let pipeline = tokio::spawn(async move {
            for i in 0..120usize {
                let pool = format!("pool-{i}");
                let trade = sample_trade_event(&pool);
                process_arb_market_event(&events_ctx, trade, ArbEventPriority::High).await;
                let psu = sample_pool_state_market_event(&pool);
                process_arb_market_event(&events_ctx, psu, ArbEventPriority::High).await;
                if i % 10 == 0 {
                    tokio::task::yield_now().await;
                }
            }
        });

        let recovery_ctx = ctx.clone();
        let recovery = tokio::spawn(async move {
            let mint = "TokenMint11111111111111111111111111111111".to_string();
            for _ in 0..30usize {
                let tracker = {
                    let trackers = recovery_ctx.trackers.read();
                    trackers.get(&mint).cloned()
                };
                if let Some(tracker) = tracker {
                    let config = recovery_ctx.config.read().clone();
                    let ctx_blocking = recovery_ctx.clone();
                    let _ = tokio::task::spawn_blocking(move || {
                        ctx_blocking.two_hop_v2_check_and_maybe_schedule_recovery(&tracker, &config)
                    })
                    .await;
                }
                tokio::task::yield_now().await;
            }
        });

        tokio::time::timeout(Duration::from_secs(8), async {
            pipeline.await.expect("pipeline task panicked");
            recovery.await.expect("recovery task panicked");
        })
        .await
        .expect("event pipeline + recovery load test timed out");

        let consumed_after = MARKET_EVENTS_CONSUMED_TOTAL.load(Ordering::Relaxed);
        let high_after = ARB_SUBSCRIBER_HIGH_PROCESSED_TOTAL.load(Ordering::Relaxed);
        let consumed_delta = consumed_after.saturating_sub(consumed_before);
        let high_delta = high_after.saturating_sub(high_before);
        assert!(
            consumed_delta >= 240,
            "market_events_consumed must progress under C1h5 load (delta={consumed_delta})"
        );
        assert!(
            high_delta >= 240,
            "high_processed must progress under C1h5 load (delta={high_delta})"
        );
    }

    #[test]
    fn tracker_write_coalescer_flush_requeues_when_queue_full() {
        use ironcrab::metrics::{
            arb_tracker_write_coalescer_flush_lost_total, ArbTrackerWriteJobType,
        };

        let (tx, _rx) = mpsc::channel::<ArbTrackerWriteJob>(1);
        let handle = ArbTrackerWriteHandle { tx, capacity: 1 };
        assert!(handle.try_enqueue(
            ArbTrackerWriteJob::TokenMintInfo {
                mint: "TokenMint11111111111111111111111111111111".to_string(),
                token_program: "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_string(),
            },
            ArbTrackerWriteJobType::TokenMintInfo,
        ));

        let mut coalescer = ArbTrackerWriteCoalescer::new();
        coalescer.insert_pool_state_update(
            "pool-full".to_string(),
            "raydium".to_string(),
            1,
            1,
            1,
            None,
            None,
            NATIVE_SOL_MINT.to_string(),
            "TokenMint11111111111111111111111111111111".to_string(),
            16,
        );

        let before =
            arb_tracker_write_coalescer_flush_lost_total(ArbTrackerWriteJobType::PoolStateUpdate);
        coalescer.flush(&handle);
        let after =
            arb_tracker_write_coalescer_flush_lost_total(ArbTrackerWriteJobType::PoolStateUpdate);
        assert_eq!(
            before, after,
            "coalescer flush on full queue must not increment flush_lost"
        );
        assert_eq!(
            coalescer.pending_len(),
            1,
            "coalescer must retain pending job when queue is full"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn apply_trade_scoped_snapshot_ignores_global_cache_size() {
        use ironcrab::execution::live_pool_cache::create_shared_cache;

        let live_pool_cache = create_shared_cache();
        let log_dir = std::env::temp_dir().join(format!("arb_scoped_snap_{}", Uuid::new_v4()));
        std::fs::create_dir_all(&log_dir).expect("test log dir");
        let jsonl_writer =
            JsonlWriter::new(JsonlWriterConfig::new("arb_scoped_snap").with_log_dir(log_dir))
                .expect("jsonl writer");
        let (tracker_write_tx, tracker_write_rx) =
            mpsc::channel::<ArbTrackerWriteJob>(ARB_TRACKER_WRITE_QUEUE_CAP);
        let tracker_write = ArbTrackerWriteHandle {
            tx: tracker_write_tx,
            capacity: ARB_TRACKER_WRITE_QUEUE_CAP,
        };
        let (two_hop_tx, _two_hop_rx) = mpsc::channel::<ArbTwoHopWorkerJob>(1);

        let mut vault_balances = HashMap::new();
        for i in 0..12_000usize {
            vault_balances.insert(
                format!("dummy-pool-{i}"),
                VaultBalanceCache {
                    reserve_base: 1,
                    reserve_quote: 1,
                    update_slot: 1,
                    active_id: None,
                    bin_step: None,
                    updated_at: Instant::now(),
                    dlmm_sol_is_x: false,
                    dlmm_token_x_mint: None,
                },
            );
        }
        let tracker_pools = ["pool-a", "pool-b", "pool-c"];
        for pool in tracker_pools {
            vault_balances.insert(
                pool.to_string(),
                VaultBalanceCache {
                    reserve_base: 5_000_000,
                    reserve_quote: 10_000_000_000,
                    update_slot: 42,
                    active_id: None,
                    bin_step: None,
                    updated_at: Instant::now(),
                    dlmm_sol_is_x: false,
                    dlmm_token_x_mint: None,
                },
            );
        }

        let ctx = Arc::new(ArbContext {
            run_id: TEST_RUN.to_string(),
            config: RwLock::new(ArbConfig::default()),
            nats: None,
            jsonl_writer,
            trackers: RwLock::new(HashMap::new()),
            events_received: AtomicU64::new(0),
            pools_tracked: AtomicU64::new(0),
            opportunities_found: AtomicU64::new(0),
            intents_generated: AtomicU64::new(0),
            intent_counter: AtomicU64::new(0),
            zero_amount_trades: AtomicU64::new(0),
            data_quality_rejects: AtomicU64::new(0),
            last_market_event: RwLock::new(Instant::now()),
            vault_balances: RwLock::new(vault_balances),
            bin_arrays: RwLock::new(HashMap::new()),
            live_pool_cache,
            known_pools: RwLock::new(HashSet::new()),
            multi_hop: Arc::new(MultiHopArbitrage::new(
                MultiHopConfig::default(),
                create_shared_cache(),
            )),
            spread_too_large_warn_last: RwLock::new(HashMap::new()),
            eligibility_forensics: ArbEligibilityForensics::new(),
            v2_eligibility_forensics: ArbV2EligibilityForensics::new(),
            arb_pinned_pools: RwLock::new(HashSet::new()),
            arb_selected_mints: RwLock::new(HashSet::new()),
            arb_trade_signal_pairs: RwLock::new(HashMap::new()),
            arb_trade_signal_pair_order: RwLock::new(Vec::new()),
            arb_track_mint_snapshots: RwLock::new(ArbTrackMintSnapshotCache::default()),
            arb_track_selection: dummy_arb_track_selection_handle(),
            arb_track_published: AtomicU64::new(0),
            two_hop_tx,
            tracker_write,
            tracker_write_coalescer: parking_lot::Mutex::new(ArbTrackerWriteCoalescer::new()),
            v2_sell_stale_recovery_pending: RwLock::new(HashMap::new()),
            pool_accounts_index: RwLock::new(HashMap::new()),
            pending_pool_accounts: RwLock::new(HashMap::new()),
        });
        spawn_arb_tracker_write_worker(ctx.clone(), tracker_write_rx);

        let mint = "TokenMint11111111111111111111111111111111";
        for pool in tracker_pools {
            ctx.handle_pool_created(pool, mint, NATIVE_SOL_MINT, "raydium", Decimal::ONE);
        }

        let (reply_tx, reply_rx) = oneshot::channel();
        let started = Instant::now();
        assert!(
            ctx.tracker_write.try_enqueue(
                ArbTrackerWriteJob::ApplyTrade {
                    job: ArbTwoHopTradeJob {
                        pool_address: tracker_pools[0].to_string(),
                        mint: mint.to_string(),
                        quote_mint: NATIVE_SOL_MINT.to_string(),
                        sol_amount: 10_000_000,
                        token_amount: 1_000_000,
                        token_decimals: 6,
                        is_buy: true,
                        dex: "raydium".to_string(),
                        slot: Some(1),
                        ts_unix_ms: 1,
                    },
                    reply: reply_tx,
                },
                ArbTrackerWriteJobType::ApplyTrade,
            ),
            "ApplyTrade enqueue"
        );

        let apply_result = tokio::time::timeout(Duration::from_millis(500), reply_rx)
            .await
            .expect("ApplyTrade reply timed out")
            .expect("ApplyTrade reply channel closed")
            .expect("ApplyTrade must succeed");
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_millis(100),
            "ApplyTrade with scoped snapshot must stay fast (got {:?})",
            elapsed
        );
        assert_eq!(
            apply_result.vault_balances.len(),
            tracker_pools.len(),
            "scoped snapshot must include only tracker pools, not global cache"
        );
        for pool in tracker_pools {
            assert!(
                apply_result.vault_balances.contains_key(pool),
                "tracker pool {pool} missing from scoped vault snapshot"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn heartbeat_releases_trackers_read_before_maybe_emit() {
        use ironcrab::execution::live_pool_cache::create_shared_cache;

        let live_pool_cache = create_shared_cache();
        let log_dir = std::env::temp_dir().join(format!("arb_hb_release_{}", Uuid::new_v4()));
        std::fs::create_dir_all(&log_dir).expect("test log dir");
        let jsonl_writer =
            JsonlWriter::new(JsonlWriterConfig::new("arb_hb_release").with_log_dir(log_dir))
                .expect("jsonl writer");
        let (tracker_write_tx, tracker_write_rx) = mpsc::channel::<ArbTrackerWriteJob>(8);
        let tracker_write = ArbTrackerWriteHandle {
            tx: tracker_write_tx,
            capacity: 8,
        };
        let (two_hop_tx, _two_hop_rx) = mpsc::channel::<ArbTwoHopWorkerJob>(1);

        let ctx = Arc::new(ArbContext {
            run_id: TEST_RUN.to_string(),
            config: RwLock::new(ArbConfig::default()),
            nats: None,
            jsonl_writer,
            trackers: RwLock::new(HashMap::new()),
            events_received: AtomicU64::new(0),
            pools_tracked: AtomicU64::new(0),
            opportunities_found: AtomicU64::new(0),
            intents_generated: AtomicU64::new(0),
            intent_counter: AtomicU64::new(0),
            zero_amount_trades: AtomicU64::new(0),
            data_quality_rejects: AtomicU64::new(0),
            last_market_event: RwLock::new(Instant::now()),
            vault_balances: RwLock::new(HashMap::new()),
            bin_arrays: RwLock::new(HashMap::new()),
            live_pool_cache,
            known_pools: RwLock::new(HashSet::new()),
            multi_hop: Arc::new(MultiHopArbitrage::new(
                MultiHopConfig::default(),
                create_shared_cache(),
            )),
            spread_too_large_warn_last: RwLock::new(HashMap::new()),
            eligibility_forensics: ArbEligibilityForensics::new(),
            v2_eligibility_forensics: ArbV2EligibilityForensics::new(),
            arb_pinned_pools: RwLock::new(HashSet::new()),
            arb_selected_mints: RwLock::new(HashSet::new()),
            arb_trade_signal_pairs: RwLock::new(HashMap::new()),
            arb_trade_signal_pair_order: RwLock::new(Vec::new()),
            arb_track_mint_snapshots: RwLock::new(ArbTrackMintSnapshotCache::default()),
            arb_track_selection: dummy_arb_track_selection_handle(),
            arb_track_published: AtomicU64::new(0),
            two_hop_tx,
            tracker_write,
            tracker_write_coalescer: parking_lot::Mutex::new(ArbTrackerWriteCoalescer::new()),
            v2_sell_stale_recovery_pending: RwLock::new(HashMap::new()),
            pool_accounts_index: RwLock::new(HashMap::new()),
            pending_pool_accounts: RwLock::new(HashMap::new()),
        });
        spawn_arb_tracker_write_worker(ctx.clone(), tracker_write_rx);

        let pool = "pool-hb-release";
        let mint = "TokenMint11111111111111111111111111111111";
        ctx.handle_pool_created(pool, mint, NATIVE_SOL_MINT, "raydium", Decimal::ONE);

        let heartbeat_ctx = ctx.clone();
        let heartbeat_sim = std::thread::spawn(move || {
            let trackers = heartbeat_ctx.trackers.read();
            let _ = trackers.len();
            drop(trackers);
            std::thread::sleep(Duration::from_millis(300));
        });

        tokio::time::sleep(Duration::from_millis(20)).await;

        let (reply_tx, reply_rx) = oneshot::channel();
        let started = Instant::now();
        assert!(
            ctx.tracker_write.try_enqueue(
                ArbTrackerWriteJob::ApplyTrade {
                    job: ArbTwoHopTradeJob {
                        pool_address: pool.to_string(),
                        mint: mint.to_string(),
                        quote_mint: NATIVE_SOL_MINT.to_string(),
                        sol_amount: 10_000_000,
                        token_amount: 1_000_000,
                        token_decimals: 6,
                        is_buy: true,
                        dex: "raydium".to_string(),
                        slot: Some(1),
                        ts_unix_ms: 1,
                    },
                    reply: reply_tx,
                },
                ArbTrackerWriteJobType::ApplyTrade,
            ),
            "ApplyTrade enqueue"
        );

        let _ = tokio::time::timeout(Duration::from_millis(200), reply_rx)
            .await
            .expect("ApplyTrade must complete while heartbeat slow phase runs without read lock")
            .expect("ApplyTrade reply channel closed")
            .expect("ApplyTrade must succeed");
        assert!(
            started.elapsed() < Duration::from_millis(150),
            "writer must not block on heartbeat slow phase after trackers.read() release"
        );

        heartbeat_sim
            .join()
            .expect("heartbeat simulation thread panicked");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn writer_job_duration_histogram_recorded() {
        use ironcrab::execution::live_pool_cache::create_shared_cache;
        use ironcrab::metrics::{
            arb_tracker_write_job_duration_count, arb_tracker_write_job_finished_total,
            ArbTrackerWriteJobType,
        };

        let live_pool_cache = create_shared_cache();
        let log_dir = std::env::temp_dir().join(format!("arb_job_hist_{}", Uuid::new_v4()));
        std::fs::create_dir_all(&log_dir).expect("test log dir");
        let jsonl_writer =
            JsonlWriter::new(JsonlWriterConfig::new("arb_job_hist").with_log_dir(log_dir))
                .expect("jsonl writer");
        let (tracker_write_tx, tracker_write_rx) = mpsc::channel::<ArbTrackerWriteJob>(8);
        let tracker_write = ArbTrackerWriteHandle {
            tx: tracker_write_tx,
            capacity: 8,
        };
        let (two_hop_tx, _two_hop_rx) = mpsc::channel::<ArbTwoHopWorkerJob>(1);

        let ctx = Arc::new(ArbContext {
            run_id: TEST_RUN.to_string(),
            config: RwLock::new(ArbConfig::default()),
            nats: None,
            jsonl_writer,
            trackers: RwLock::new(HashMap::new()),
            events_received: AtomicU64::new(0),
            pools_tracked: AtomicU64::new(0),
            opportunities_found: AtomicU64::new(0),
            intents_generated: AtomicU64::new(0),
            intent_counter: AtomicU64::new(0),
            zero_amount_trades: AtomicU64::new(0),
            data_quality_rejects: AtomicU64::new(0),
            last_market_event: RwLock::new(Instant::now()),
            vault_balances: RwLock::new(HashMap::new()),
            bin_arrays: RwLock::new(HashMap::new()),
            live_pool_cache,
            known_pools: RwLock::new(HashSet::new()),
            multi_hop: Arc::new(MultiHopArbitrage::new(
                MultiHopConfig::default(),
                create_shared_cache(),
            )),
            spread_too_large_warn_last: RwLock::new(HashMap::new()),
            eligibility_forensics: ArbEligibilityForensics::new(),
            v2_eligibility_forensics: ArbV2EligibilityForensics::new(),
            arb_pinned_pools: RwLock::new(HashSet::new()),
            arb_selected_mints: RwLock::new(HashSet::new()),
            arb_trade_signal_pairs: RwLock::new(HashMap::new()),
            arb_trade_signal_pair_order: RwLock::new(Vec::new()),
            arb_track_mint_snapshots: RwLock::new(ArbTrackMintSnapshotCache::default()),
            arb_track_selection: dummy_arb_track_selection_handle(),
            arb_track_published: AtomicU64::new(0),
            two_hop_tx,
            tracker_write,
            tracker_write_coalescer: parking_lot::Mutex::new(ArbTrackerWriteCoalescer::new()),
            v2_sell_stale_recovery_pending: RwLock::new(HashMap::new()),
            pool_accounts_index: RwLock::new(HashMap::new()),
            pending_pool_accounts: RwLock::new(HashMap::new()),
        });
        spawn_arb_tracker_write_worker(ctx.clone(), tracker_write_rx);

        ctx.tracker_write
            .enqueue(
                ArbTrackerWriteJob::TokenMintInfo {
                    mint: "TokenMint11111111111111111111111111111111".to_string(),
                    token_program: "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_string(),
                },
                ArbTrackerWriteJobType::TokenMintInfo,
            )
            .await;

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if arb_tracker_write_job_finished_total(ArbTrackerWriteJobType::TokenMintInfo) > 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("writer job did not finish in time");

        assert!(
            arb_tracker_write_job_duration_count(ArbTrackerWriteJobType::TokenMintInfo) > 0,
            "writer job duration histogram must record finished jobs"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn heartbeat_read_lock_contention_blocks_writer_pool_state() {
        use ironcrab::execution::live_pool_cache::create_shared_cache;
        use ironcrab::metrics::{arb_writer_lock_wait_count, ArbWriterLockKind};

        let live_pool_cache = create_shared_cache();
        let log_dir = std::env::temp_dir().join(format!("arb_stall_repro_{}", Uuid::new_v4()));
        std::fs::create_dir_all(&log_dir).expect("test log dir");
        let jsonl_writer =
            JsonlWriter::new(JsonlWriterConfig::new("arb_stall_repro").with_log_dir(log_dir))
                .expect("jsonl writer");
        let (tracker_write_tx, tracker_write_rx) = mpsc::channel::<ArbTrackerWriteJob>(8);
        let tracker_write = ArbTrackerWriteHandle {
            tx: tracker_write_tx,
            capacity: 8,
        };
        let (two_hop_tx, _two_hop_rx) = mpsc::channel::<ArbTwoHopWorkerJob>(1);

        let ctx = Arc::new(ArbContext {
            run_id: TEST_RUN.to_string(),
            config: RwLock::new(ArbConfig::default()),
            nats: None,
            jsonl_writer,
            trackers: RwLock::new(HashMap::new()),
            events_received: AtomicU64::new(0),
            pools_tracked: AtomicU64::new(0),
            opportunities_found: AtomicU64::new(0),
            intents_generated: AtomicU64::new(0),
            intent_counter: AtomicU64::new(0),
            zero_amount_trades: AtomicU64::new(0),
            data_quality_rejects: AtomicU64::new(0),
            last_market_event: RwLock::new(Instant::now()),
            vault_balances: RwLock::new(HashMap::new()),
            bin_arrays: RwLock::new(HashMap::new()),
            live_pool_cache,
            known_pools: RwLock::new(HashSet::new()),
            multi_hop: Arc::new(MultiHopArbitrage::new(
                MultiHopConfig::default(),
                create_shared_cache(),
            )),
            spread_too_large_warn_last: RwLock::new(HashMap::new()),
            eligibility_forensics: ArbEligibilityForensics::new(),
            v2_eligibility_forensics: ArbV2EligibilityForensics::new(),
            arb_pinned_pools: RwLock::new(HashSet::new()),
            arb_selected_mints: RwLock::new(HashSet::new()),
            arb_trade_signal_pairs: RwLock::new(HashMap::new()),
            arb_trade_signal_pair_order: RwLock::new(Vec::new()),
            arb_track_mint_snapshots: RwLock::new(ArbTrackMintSnapshotCache::default()),
            arb_track_selection: dummy_arb_track_selection_handle(),
            arb_track_published: AtomicU64::new(0),
            two_hop_tx,
            tracker_write,
            tracker_write_coalescer: parking_lot::Mutex::new(ArbTrackerWriteCoalescer::new()),
            v2_sell_stale_recovery_pending: RwLock::new(HashMap::new()),
            pool_accounts_index: RwLock::new(HashMap::new()),
            pending_pool_accounts: RwLock::new(HashMap::new()),
        });
        spawn_arb_tracker_write_worker(ctx.clone(), tracker_write_rx);

        let pool = "pool-stall-repro";
        let mint = "TokenMint11111111111111111111111111111111";
        ctx.handle_pool_created(pool, mint, NATIVE_SOL_MINT, "raydium", Decimal::ONE);

        let contention_ctx = ctx.clone();
        let contention = std::thread::spawn(move || {
            let _read_guard = contention_ctx.trackers.read();
            std::thread::sleep(Duration::from_millis(400));
        });

        tokio::time::sleep(Duration::from_millis(20)).await;

        let write_ctx = ctx.clone();
        let writer = tokio::spawn(async move {
            write_ctx
                .tracker_write
                .enqueue(
                    ArbTrackerWriteJob::PoolStateUpdate {
                        pool_address: pool.to_string(),
                        dex: "raydium".to_string(),
                        reserve_base: 5_000_000,
                        reserve_quote: 10_000_000_000,
                        update_slot: 99,
                        active_id: None,
                        bin_step: None,
                        base_mint: NATIVE_SOL_MINT.to_string(),
                        quote_mint: mint.to_string(),
                    },
                    ArbTrackerWriteJobType::PoolStateUpdate,
                )
                .await;
        });

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            arb_writer_lock_wait_count(ArbWriterLockKind::TrackersWrite) > 0
                || arb_writer_lock_wait_count(ArbWriterLockKind::TrackersRead) > 0,
            "writer must observe lock wait while heartbeat-style read lock is held"
        );

        writer.await.expect("writer enqueue task panicked");
        contention.join().expect("contention thread panicked");
    }
}

#[cfg(test)]
fn test_arb_context(live_pool_cache: SharedLivePoolCache) -> ArbContext {
    let log_dir = std::env::temp_dir().join(format!("arb_ctx_test_{}", Uuid::new_v4()));
    std::fs::create_dir_all(&log_dir).expect("test log dir");
    let jsonl_writer = JsonlWriter::new(JsonlWriterConfig::new("arb_test").with_log_dir(log_dir))
        .expect("jsonl writer");
    ArbContext {
        run_id: "test-run".to_string(),
        config: RwLock::new(ArbConfig::default()),
        nats: None,
        jsonl_writer,
        trackers: RwLock::new(HashMap::new()),
        events_received: AtomicU64::new(0),
        pools_tracked: AtomicU64::new(0),
        opportunities_found: AtomicU64::new(0),
        intents_generated: AtomicU64::new(0),
        intent_counter: AtomicU64::new(0),
        zero_amount_trades: AtomicU64::new(0),
        data_quality_rejects: AtomicU64::new(0),
        last_market_event: RwLock::new(Instant::now()),
        vault_balances: RwLock::new(HashMap::new()),
        bin_arrays: RwLock::new(HashMap::new()),
        live_pool_cache: live_pool_cache.clone(),
        known_pools: RwLock::new(HashSet::new()),
        multi_hop: Arc::new(MultiHopArbitrage::new(
            MultiHopConfig::default(),
            live_pool_cache,
        )),
        spread_too_large_warn_last: RwLock::new(HashMap::new()),
        eligibility_forensics: ArbEligibilityForensics::new(),
        v2_eligibility_forensics: ArbV2EligibilityForensics::new(),
        arb_pinned_pools: RwLock::new(HashSet::new()),
        arb_selected_mints: RwLock::new(HashSet::new()),
        arb_trade_signal_pairs: RwLock::new(HashMap::new()),
        arb_trade_signal_pair_order: RwLock::new(Vec::new()),
        arb_track_mint_snapshots: RwLock::new(ArbTrackMintSnapshotCache::default()),
        arb_track_selection: dummy_arb_track_selection_handle(),
        arb_track_published: AtomicU64::new(0),
        two_hop_tx: {
            let (tx, _rx) = mpsc::channel(1);
            tx
        },
        tracker_write: {
            let (tx, _rx) = mpsc::channel(1);
            ArbTrackerWriteHandle { tx, capacity: 1 }
        },
        tracker_write_coalescer: parking_lot::Mutex::new(ArbTrackerWriteCoalescer::new()),
        v2_sell_stale_recovery_pending: RwLock::new(HashMap::new()),
        pool_accounts_index: RwLock::new(HashMap::new()),
        pending_pool_accounts: RwLock::new(HashMap::new()),
    }
}

#[cfg(test)]
mod two_hop_price_tests {
    use super::*;
    use ironcrab::execution::live_pool_cache::{
        create_shared_cache, CachedPoolState, MeteoraState, OrcaWhirlpoolState,
    };
    use ironcrab::ipc::PoolCacheUpdate;
    use rust_decimal::Decimal;
    use solana_sdk::pubkey::Pubkey;
    use std::time::Instant;

    const TEST_COMPONENT: &str = "test";
    const TEST_BUILD: &str = "0.0.0";
    const TEST_RUN: &str = "run-test";

    /// Fixture pools are calibrated for the legacy 0.01 SOL screening probe.
    fn with_small_v2_probe(mut config: ArbConfig) -> ArbConfig {
        config.arb_probe_lamports = DLMM_PROBE_SOL_LAMPORTS;
        config.arb_probe_follows_max_position = false;
        config
    }

    fn sample_pool(
        dex: &str,
        addr: &str,
        buy: Option<Decimal>,
        sell: Option<Decimal>,
    ) -> PoolState {
        PoolState {
            pool_address: addr.to_string(),
            dex: dex.to_string(),
            last_price: None,
            trade_price_buy: buy,
            trade_price_sell: sell,
            liquidity_sol: Decimal::ZERO,
            has_reserve_data: false,
            last_update: Instant::now(),
            trade_count: 1,
            dex_accounts: None,
        }
    }

    fn sample_vault(
        reserve_base: u64,
        reserve_quote: u64,
        active_id: Option<i32>,
        bin_step: Option<u16>,
        dlmm_sol_is_x: bool,
        dlmm_token_x_mint: Option<&str>,
    ) -> VaultBalanceCache {
        VaultBalanceCache {
            reserve_base,
            reserve_quote,
            update_slot: 1,
            active_id,
            bin_step,
            updated_at: Instant::now(),
            dlmm_sol_is_x,
            dlmm_token_x_mint: dlmm_token_x_mint.map(str::to_string),
        }
    }

    fn usdc_sol_dlmm_fixture(
        sol_is_x: bool,
        token_amount: u64,
        sol_amount: u64,
        active_id: i32,
        bin_step: u16,
    ) -> (HashMap<i64, BinArrayCache>, VaultBalanceCache, u8) {
        let array_index = active_id as i64 / 70;
        let (amount_x, amount_y) = if sol_is_x {
            (sol_amount, token_amount)
        } else {
            (token_amount, sol_amount)
        };
        let token_x_mint = if sol_is_x { NATIVE_SOL_MINT } else { USDC_MINT };
        let mut bin_arrays: HashMap<i64, BinArrayCache> = HashMap::new();
        bin_arrays.insert(
            array_index,
            BinArrayCache {
                bins: vec![BinData {
                    offset: (active_id as i64 % 70) as u8,
                    amount_x,
                    amount_y,
                }],
                update_slot: 1,
            },
        );
        let vault = sample_vault(
            token_amount,
            sol_amount,
            Some(active_id),
            Some(bin_step),
            sol_is_x,
            Some(token_x_mint),
        );
        (bin_arrays, vault, 6)
    }

    #[test]
    fn same_reserve_mid_on_two_dexes_yields_near_zero_spread() {
        let reserves = (1_000_000_000_000u64, 1_000_000_000u64);
        let mid = reserve_mid_sol_per_token(reserves.0, reserves.1, 6).unwrap();
        let pool_a = sample_pool("meteora_dlmm", "poolA", None, None);
        let pool_b = sample_pool("pump_amm", "poolB", None, None);
        let vault = sample_vault(reserves.0, reserves.1, None, None, false, None);
        let p_a = comparable_price_sol_per_token(
            &pool_a,
            Some(reserves),
            Some(6),
            "TokenMint11111111111111111111111111111111",
            Some(&vault),
            None,
            ComparablePriceSide::Buy,
        )
        .unwrap();
        let p_b = comparable_price_sol_per_token(
            &pool_b,
            Some(reserves),
            Some(6),
            "TokenMint11111111111111111111111111111111",
            Some(&vault),
            None,
            ComparablePriceSide::Buy,
        )
        .unwrap();
        assert_eq!(p_a, mid);
        assert_eq!(p_b, mid);
        let spread_bps = ((p_b - p_a) / p_a * Decimal::from(10000))
            .round()
            .to_i64()
            .unwrap();
        assert_eq!(spread_bps, 0);
    }

    #[test]
    fn buy_vs_sell_trade_mid_avoids_huge_artificial_spread() {
        let buy_price = trade_implied_sol_per_token(2_000_000_000, 1_000_000_000_000, 6);
        let sell_price = trade_implied_sol_per_token(500_000_000, 1_000_000_000_000, 6);
        let pool = sample_pool("orca", "poolO", Some(buy_price), Some(sell_price));
        let mid = comparable_price_sol_per_token(
            &pool,
            None,
            Some(6),
            "TokenMint11111111111111111111111111111111",
            None,
            None,
            ComparablePriceSide::Buy,
        )
        .unwrap();
        let naive_spread_bps = ((buy_price - sell_price) / sell_price * Decimal::from(10000))
            .round()
            .to_i64()
            .unwrap();
        let mid_spread_bps = ((mid - mid) / mid * Decimal::from(10000))
            .round()
            .to_i64()
            .unwrap();
        assert!(naive_spread_bps > 1000);
        assert_eq!(mid_spread_bps, 0);
    }

    #[test]
    fn liquidity_penalty_only_when_both_sides_lack_reserve_and_liquidity() {
        let buy_pool = PoolState {
            has_reserve_data: true,
            liquidity_sol: Decimal::ONE,
            ..sample_pool("meteora_dlmm", "poolBuy", None, None)
        };
        let sell_pool = PoolState {
            has_reserve_data: false,
            liquidity_sol: Decimal::ZERO,
            ..sample_pool("pump_amm", "poolSell", None, None)
        };
        let buy_unknown = !buy_pool.has_reserve_data && buy_pool.liquidity_sol <= Decimal::ZERO;
        let sell_unknown = !sell_pool.has_reserve_data && sell_pool.liquidity_sol <= Decimal::ZERO;
        assert!(!buy_unknown);
        assert!(sell_unknown);
        assert!(!(buy_unknown && sell_unknown));
    }

    #[test]
    fn dlmm_marginal_vs_amm_mid_no_spread_too_large() {
        // USDC/SOL DLMM: 1M USDC (6 dec) : 1000 SOL (9 dec) — both token_x orientations.
        let reserve_base = 1_000_000_000_000u64;
        let reserve_quote = 1_000_000_000_000u64;
        let active_id: i32 = 0;
        let bin_step: u16 = 10;

        for sol_is_x in [false, true] {
            let (bin_arrays, vault, token_decimals) =
                usdc_sol_dlmm_fixture(sol_is_x, reserve_base, reserve_quote, active_id, bin_step);
            let reserve_mid =
                reserve_mid_sol_per_token(reserve_base, reserve_quote, token_decimals).unwrap();

            let dlmm_pool = sample_pool("meteora_dlmm", "dlmmPool", None, None);
            let orca_pool = sample_pool("orca", "orcaPool", None, None);

            let p_dlmm = comparable_price_sol_per_token(
                &dlmm_pool,
                Some((reserve_base, reserve_quote)),
                Some(token_decimals),
                USDC_MINT,
                Some(&vault),
                Some(&bin_arrays),
                ComparablePriceSide::Buy,
            )
            .unwrap();
            let p_orca = comparable_price_sol_per_token(
                &orca_pool,
                Some((reserve_base, reserve_quote)),
                Some(token_decimals),
                USDC_MINT,
                Some(&vault),
                None,
                ComparablePriceSide::Buy,
            )
            .unwrap();

            let ratio = if p_dlmm > reserve_mid {
                p_dlmm / reserve_mid
            } else {
                reserve_mid / p_dlmm
            };
            assert!(
                ratio <= Decimal::from(2),
                "sol_is_x={sol_is_x}: DLMM marginal {p_dlmm} vs reserve mid {reserve_mid} (ratio {ratio})"
            );

            let spread_bps = ((p_orca - p_dlmm) / p_dlmm * Decimal::from(10000))
                .abs()
                .round()
                .to_i64()
                .unwrap();
            assert!(
                spread_bps < MAX_REASONABLE_SPREAD_BPS,
                "sol_is_x={sol_is_x}: DLMM marginal vs AMM mid spread {spread_bps} bps should be sane"
            );
        }
    }

    #[test]
    fn dlmm_incomplete_bin_arrays_falls_back_to_reserve_mid() {
        let reserve_base = 1_000_000_000_000u64;
        let reserve_quote = 1_000_000_000_000u64;
        let active_id: i32 = 0;
        let bin_step: u16 = 10;
        let expected_mid = reserve_mid_sol_per_token(reserve_base, reserve_quote, 6).unwrap();

        // Active bin 0 missing: only liquidity in array index 1 (bin_id 70).
        let mut bin_arrays: HashMap<i64, BinArrayCache> = HashMap::new();
        bin_arrays.insert(
            1,
            BinArrayCache {
                bins: vec![BinData {
                    offset: 0,
                    amount_x: reserve_base,
                    amount_y: reserve_quote,
                }],
                update_slot: 1,
            },
        );

        let vault = sample_vault(
            reserve_base,
            reserve_quote,
            Some(active_id),
            Some(bin_step),
            false,
            Some(USDC_MINT),
        );
        let dlmm_pool = sample_pool("meteora_dlmm", "dlmmPool", None, None);

        let price = comparable_price_sol_per_token(
            &dlmm_pool,
            Some((reserve_base, reserve_quote)),
            Some(6),
            USDC_MINT,
            Some(&vault),
            Some(&bin_arrays),
            ComparablePriceSide::Buy,
        )
        .expect("incomplete bin data must fall back to reserve mid, not None");

        assert_eq!(price, expected_mid);
    }

    #[test]
    fn reserve_fresh_trade_stale_passes_freshness_check() {
        let stale_trade = Instant::now() - Duration::from_millis(MAX_PRICE_AGE_MS + 5_000);
        let pool = PoolState {
            has_reserve_data: true,
            last_update: stale_trade,
            ..sample_pool("orca", "poolFresh", None, None)
        };
        let vault = VaultBalanceCache {
            reserve_base: 1_000_000_000_000,
            reserve_quote: 1_000_000_000,
            update_slot: 1,
            active_id: None,
            bin_step: None,
            updated_at: Instant::now(),
            dlmm_sol_is_x: false,
            dlmm_token_x_mint: None,
        };
        let max_age = Duration::from_millis(MAX_PRICE_AGE_MS);
        assert!(!pool.last_update.elapsed().le(&max_age));
        assert!(is_pool_price_fresh(&pool, Some(&vault), max_age));
    }

    #[test]
    fn dlmm_meta_fresh_without_reserves_passes_freshness_check() {
        let stale_trade = Instant::now() - Duration::from_millis(MAX_PRICE_AGE_MS + 5_000);
        let pool = PoolState {
            has_reserve_data: false,
            last_update: stale_trade,
            ..sample_pool("meteora_dlmm", "dlmmMeta", None, None)
        };
        let vault = VaultBalanceCache {
            reserve_base: 0,
            reserve_quote: 0,
            update_slot: 2,
            active_id: Some(0),
            bin_step: Some(10),
            updated_at: Instant::now(),
            dlmm_sol_is_x: true,
            dlmm_token_x_mint: Some(NATIVE_SOL_MINT.to_string()),
        };
        let max_age = Duration::from_millis(MAX_PRICE_AGE_MS);
        assert!(is_pool_price_fresh(&pool, Some(&vault), max_age));
    }

    #[test]
    fn bin_array_update_seeds_missing_vault_from_live_cache() {
        let cache = create_shared_cache();
        let pool_pk = Pubkey::new_unique();
        let token_mint = Pubkey::new_unique();
        let sol = Pubkey::from_str(NATIVE_SOL_MINT).unwrap();
        cache.upsert(
            pool_pk,
            CachedPoolState::Meteora(MeteoraState {
                token_x_mint: token_mint,
                token_y_mint: sol,
                reserve_x: Pubkey::new_unique(),
                reserve_y: Pubkey::new_unique(),
                active_id: 42,
                bin_step: 25,
                reserve_x_balance: Some(1_000_000),
                reserve_y_balance: Some(2_000_000_000),
            }),
            10,
        );
        let ctx = test_arb_context(cache);
        let pool_addr = pool_pk.to_string();
        assert!(!ctx.vault_balances.read().contains_key(&pool_addr));

        ctx.handle_bin_array_update(
            &pool_addr,
            0,
            vec![BinData {
                offset: 0,
                amount_x: 1_000_000,
                amount_y: 2_000_000_000,
            }],
            11,
        );

        let vaults = ctx.vault_balances.read();
        let vault = vaults.get(&pool_addr).expect("vault must be seeded");
        assert_eq!(vault.active_id, Some(42));
        assert_eq!(vault.bin_step, Some(25));
        assert!(vault.updated_at.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn bin_array_update_refreshes_dlmm_pool_freshness() {
        let cache = create_shared_cache();
        let ctx = test_arb_context(cache);
        let mint = "TokenMintBinArrayFresh1111111111111111111";
        let pool_addr = "dlmmPoolBinFresh";

        {
            let mut trackers = ctx.trackers.write();
            let tracker = TokenArbTracker::new(mint);
            trackers.insert(mint.to_string(), tracker);
        }
        {
            let mut trackers = ctx.trackers.write();
            let tracker = trackers.get_mut(mint).unwrap();
            tracker.upsert_pool(PoolState {
                last_update: Instant::now() - Duration::from_millis(MAX_PRICE_AGE_MS + 10_000),
                ..sample_pool("meteora_dlmm", pool_addr, None, None)
            });
        }
        ctx.vault_balances.write().insert(
            pool_addr.to_string(),
            VaultBalanceCache {
                reserve_base: 0,
                reserve_quote: 0,
                update_slot: 1,
                active_id: Some(0),
                bin_step: Some(10),
                updated_at: Instant::now() - Duration::from_millis(MAX_PRICE_AGE_MS + 10_000),
                dlmm_sol_is_x: true,
                dlmm_token_x_mint: Some(NATIVE_SOL_MINT.to_string()),
            },
        );

        let max_age = Duration::from_millis(MAX_PRICE_AGE_MS);
        let pool_before = ctx.trackers.read().get(mint).unwrap().pools[pool_addr].clone();
        let vault_before = ctx.vault_balances.read().get(pool_addr).unwrap().clone();
        assert!(!is_pool_price_fresh(
            &pool_before,
            Some(&vault_before),
            max_age
        ));

        ctx.handle_bin_array_update(
            pool_addr,
            0,
            vec![BinData {
                offset: 0,
                amount_x: 1_000_000,
                amount_y: 1_000_000_000,
            }],
            99,
        );

        let pool_after = ctx.trackers.read().get(mint).unwrap().pools[pool_addr].clone();
        let vault_after = ctx.vault_balances.read().get(pool_addr).unwrap().clone();
        assert!(is_pool_price_fresh(
            &pool_after,
            Some(&vault_after),
            max_age
        ));
    }

    #[test]
    fn scoped_snapshot_includes_bins_cached_after_bin_array_update() {
        let ctx = test_arb_context(create_shared_cache());
        let mint = "TokenMintScopedBinSnap1111111111111111111";
        let dlmm_pool = "dlmmScopedSnap";
        {
            let mut trackers = ctx.trackers.write();
            let mut tracker = TokenArbTracker::new(mint);
            tracker.upsert_pool(sample_pool("meteora_dlmm", dlmm_pool, None, None));
            trackers.insert(mint.to_string(), tracker);
        }
        let tracker = ctx.trackers.read().get(mint).unwrap().clone();
        let (_, bins_before) = ctx.snapshot_vault_bins_for_tracker(&tracker);
        assert!(!bins_before.contains_key(dlmm_pool));

        ctx.handle_bin_array_update(
            dlmm_pool,
            0,
            vec![BinData {
                offset: 0,
                amount_x: 1_000_000,
                amount_y: 1_000_000_000,
            }],
            42,
        );

        let (_, bins_after) = ctx.snapshot_vault_bins_for_tracker(&tracker);
        assert!(
            bins_after.contains_key(dlmm_pool),
            "live scoped snapshot must include bins written by BinArrayUpdate"
        );
    }

    #[test]
    fn scoped_snapshot_seeds_missing_vault_from_live_cache() {
        use ironcrab::metrics::ARB_VAULT_LIVE_SNAPSHOT_SEEDED_TOTAL;
        use std::sync::atomic::Ordering;

        let cache = create_shared_cache();
        let pool_pk = Pubkey::new_unique();
        let token_mint = Pubkey::new_unique();
        let sol = Pubkey::from_str(NATIVE_SOL_MINT).unwrap();
        cache.upsert(
            pool_pk,
            CachedPoolState::Meteora(MeteoraState {
                token_x_mint: token_mint,
                token_y_mint: sol,
                reserve_x: Pubkey::new_unique(),
                reserve_y: Pubkey::new_unique(),
                active_id: 10,
                bin_step: 20,
                reserve_x_balance: Some(500_000),
                reserve_y_balance: Some(1_500_000_000),
            }),
            5,
        );
        let ctx = test_arb_context(cache);
        let mint = "TokenMintScopedVaultSnap111111111111111111";
        let pool_addr = pool_pk.to_string();
        {
            let mut trackers = ctx.trackers.write();
            let mut tracker = TokenArbTracker::new(mint);
            tracker.upsert_pool(sample_pool("meteora_dlmm", &pool_addr, None, None));
            trackers.insert(mint.to_string(), tracker);
        }
        let tracker = ctx.trackers.read().get(mint).unwrap().clone();
        let seeded_before = ARB_VAULT_LIVE_SNAPSHOT_SEEDED_TOTAL.load(Ordering::Relaxed);
        let (vaults, _) = ctx.snapshot_vault_bins_for_tracker(&tracker);
        assert!(
            vaults.contains_key(&pool_addr),
            "snapshot must seed vault from SLAVE cache when vault_balances empty"
        );
        assert!(ARB_VAULT_LIVE_SNAPSHOT_SEEDED_TOTAL.load(Ordering::Relaxed) > seeded_before);
    }

    #[test]
    fn scoped_snapshot_refreshes_stale_vault_from_fresher_live_cache() {
        use ironcrab::arbitrage::pool_quote::STATE_TTL_MS;
        use ironcrab::metrics::{
            ARB_VAULT_LIVE_SNAPSHOT_REFRESHED_TOTAL, ARB_VAULT_LIVE_SNAPSHOT_SEEDED_TOTAL,
        };
        use std::sync::atomic::Ordering;

        let cache = create_shared_cache();
        let pool_pk = Pubkey::new_unique();
        let token_mint = Pubkey::new_unique();
        let sol = Pubkey::from_str(NATIVE_SOL_MINT).unwrap();
        cache.upsert(
            pool_pk,
            CachedPoolState::Meteora(MeteoraState {
                token_x_mint: token_mint,
                token_y_mint: sol,
                reserve_x: Pubkey::new_unique(),
                reserve_y: Pubkey::new_unique(),
                active_id: 10,
                bin_step: 20,
                reserve_x_balance: Some(800_000),
                reserve_y_balance: Some(2_000_000_000),
            }),
            50,
        );
        let ctx = test_arb_context(cache);
        let mint = "TokenMintScopedVaultRefresh111111111111111";
        let pool_addr = pool_pk.to_string();
        {
            let mut trackers = ctx.trackers.write();
            let mut tracker = TokenArbTracker::new(mint);
            tracker.upsert_pool(sample_pool("meteora_dlmm", &pool_addr, None, None));
            trackers.insert(mint.to_string(), tracker);
        }
        ctx.vault_balances.write().insert(
            pool_addr.clone(),
            VaultBalanceCache {
                reserve_base: 100_000,
                reserve_quote: 500_000_000,
                update_slot: 1,
                active_id: Some(0),
                bin_step: Some(10),
                updated_at: Instant::now() - Duration::from_millis(STATE_TTL_MS + 60_000),
                dlmm_sol_is_x: true,
                dlmm_token_x_mint: Some(NATIVE_SOL_MINT.to_string()),
            },
        );

        let tracker = ctx.trackers.read().get(mint).unwrap().clone();
        let seeded_before = ARB_VAULT_LIVE_SNAPSHOT_SEEDED_TOTAL.load(Ordering::Relaxed);
        let refreshed_before = ARB_VAULT_LIVE_SNAPSHOT_REFRESHED_TOTAL.load(Ordering::Relaxed);
        let (vaults, _) = ctx.snapshot_vault_bins_for_tracker(&tracker);
        let vault = vaults
            .get(&pool_addr)
            .expect("snapshot must include vault row");
        assert!(
            vault.updated_at.elapsed() <= Duration::from_millis(STATE_TTL_MS),
            "live refresh must replace stale vault_balances.updated_at"
        );
        assert_eq!(vault.update_slot, 50);
        assert_eq!(vault.reserve_base, 800_000);
        assert_eq!(vault.reserve_quote, 2_000_000_000);
        assert_eq!(
            ARB_VAULT_LIVE_SNAPSHOT_SEEDED_TOTAL.load(Ordering::Relaxed),
            seeded_before,
            "refresh path must not count as seed"
        );
        assert!(ARB_VAULT_LIVE_SNAPSHOT_REFRESHED_TOTAL.load(Ordering::Relaxed) > refreshed_before);
    }

    #[test]
    fn pool_state_update_increments_vault_balance_applied_on_update() {
        use ironcrab::metrics::ARB_VAULT_BALANCE_APPLIED_TOTAL;
        use std::sync::atomic::Ordering;

        let ctx = test_arb_context(create_shared_cache());
        let pool_addr = "orcaPoolStateUpdateApplied";
        let mint = "TokenMintVaultApplied111111111111111111111";
        {
            let mut trackers = ctx.trackers.write();
            let mut tracker = TokenArbTracker::new(mint);
            tracker.upsert_pool(sample_pool("orca", pool_addr, None, None));
            trackers.insert(mint.to_string(), tracker);
        }
        ctx.vault_balances.write().insert(
            pool_addr.to_string(),
            VaultBalanceCache {
                reserve_base: 1_000_000_000,
                reserve_quote: 1_000_000,
                update_slot: 10,
                active_id: None,
                bin_step: None,
                updated_at: Instant::now(),
                dlmm_sol_is_x: false,
                dlmm_token_x_mint: None,
            },
        );
        let before = ARB_VAULT_BALANCE_APPLIED_TOTAL.load(Ordering::Relaxed);
        ctx.handle_pool_state_update(
            pool_addr,
            "orca",
            1_000_000_000,
            1_100_000,
            20,
            None,
            None,
            mint,
            NATIVE_SOL_MINT,
        );
        assert!(
            ARB_VAULT_BALANCE_APPLIED_TOTAL.load(Ordering::Relaxed) > before,
            "PoolStateUpdate must increment applied counter on slot-advancing update"
        );
    }

    #[test]
    fn check_arbitrage_for_tracker_records_meteora_sell_bin_hit_when_cached() {
        let ctx = test_arb_context(create_shared_cache());
        let mint = "TokenMintDlmmHit1111111111111111111111111";
        let orca_pool = "orca_hit_pool";
        let dlmm_pool = "dlmm_hit_pool";
        let reserves = (1_000_000_000_000u64, 1_000_000_000u64);

        {
            let mut trackers = ctx.trackers.write();
            let mut tracker = TokenArbTracker::new(mint);
            tracker.token_decimals = Some(6);
            tracker.upsert_pool(sample_pool("orca", orca_pool, None, None));
            tracker.upsert_pool(sample_pool("meteora_dlmm", dlmm_pool, None, None));
            trackers.insert(mint.to_string(), tracker);
        }
        ctx.known_pools.write().insert(orca_pool.to_string());
        ctx.known_pools.write().insert(dlmm_pool.to_string());
        ctx.arb_pinned_pools
            .write()
            .extend([orca_pool.to_string(), dlmm_pool.to_string()]);
        ctx.arb_selected_mints.write().insert(mint.to_string());
        ctx.vault_balances.write().extend([
            (orca_pool.to_string(), vault(reserves.0, reserves.1)),
            (
                dlmm_pool.to_string(),
                VaultBalanceCache {
                    reserve_base: reserves.0,
                    reserve_quote: reserves.1,
                    update_slot: 1,
                    active_id: Some(0),
                    bin_step: Some(10),
                    updated_at: Instant::now(),
                    dlmm_sol_is_x: true,
                    dlmm_token_x_mint: Some(NATIVE_SOL_MINT.to_string()),
                },
            ),
        ]);
        ctx.handle_bin_array_update(
            dlmm_pool,
            0,
            vec![BinData {
                offset: 0,
                amount_x: reserves.0,
                amount_y: reserves.1,
            }],
            5,
        );

        let before_hit =
            ironcrab::metrics::ARB_V2_SCREEN_METEORA_SELL_BIN_HIT_TOTAL.load(Ordering::Relaxed);
        let tracker = ctx.trackers.read().get(mint).unwrap().clone();
        let config = with_small_v2_probe(ArbConfig {
            arb_two_hop_v2_enabled: true,
            two_hop_enabled: true,
            min_spread_bps: 1,
            min_profit_lamports: 1,
            est_tx_cost_lamports: 1,
            ..Default::default()
        });
        let _ = ctx.check_arbitrage_for_tracker(&tracker, &config);
        assert!(
            ironcrab::metrics::ARB_V2_SCREEN_METEORA_SELL_BIN_HIT_TOTAL.load(Ordering::Relaxed)
                > before_hit,
            "pinned Meteora sell must see dlmm_bins from live cache at screen time"
        );
    }

    #[test]
    fn bin_array_update_schedules_rescreen_for_selected_pinned_mint() {
        let cache = create_shared_cache();
        let (two_hop_tx, mut two_hop_rx) = mpsc::channel::<ArbTwoHopWorkerJob>(4);
        let mut ctx = test_arb_context(cache);
        ctx.two_hop_tx = two_hop_tx;
        ctx.config.write().two_hop_enabled = true;

        let mint = "TokenMintRescreen111111111111111111111111";
        let dlmm_pool = "dlmm_rescreen_pool";
        {
            let mut trackers = ctx.trackers.write();
            let mut tracker = TokenArbTracker::new(mint);
            tracker.upsert_pool(sample_pool("meteora_dlmm", dlmm_pool, None, None));
            trackers.insert(mint.to_string(), tracker);
        }
        ctx.arb_pinned_pools.write().insert(dlmm_pool.to_string());
        ctx.arb_selected_mints.write().insert(mint.to_string());

        let before =
            ironcrab::metrics::ARB_DLMM_BIN_RESCREEN_SCHEDULED_TOTAL.load(Ordering::Relaxed);
        ctx.handle_bin_array_update(
            dlmm_pool,
            0,
            vec![BinData {
                offset: 0,
                amount_x: 1,
                amount_y: 1,
            }],
            7,
        );
        assert!(
            ironcrab::metrics::ARB_DLMM_BIN_RESCREEN_SCHEDULED_TOTAL.load(Ordering::Relaxed)
                > before
        );
        match two_hop_rx.try_recv().expect("rescreen must be queued") {
            ArbTwoHopWorkerJob::Rescreen { mint: queued } => assert_eq!(queued, mint),
            other => panic!("expected rescreen job, got {other:?}"),
        }
    }

    #[test]
    fn tracker_seed_two_pools_no_trades_passes_insufficient_pools_gate() {
        let cache = create_shared_cache();
        let token_mint = Pubkey::new_unique();
        let pool_a = Pubkey::new_unique();
        let pool_b = Pubkey::new_unique();

        let update_orca = PoolCacheUpdate::new_balance_updated(
            TEST_COMPONENT,
            TEST_BUILD,
            TEST_RUN,
            pool_a.to_string(),
            "orca".to_string(),
            token_mint.to_string(),
            NATIVE_SOL_MINT.to_string(),
            1_000_000_000_000,
            1_000_000_000,
            1,
        );
        let update_pump = PoolCacheUpdate::new_balance_updated(
            TEST_COMPONENT,
            TEST_BUILD,
            TEST_RUN,
            pool_b.to_string(),
            "pump_amm".to_string(),
            token_mint.to_string(),
            NATIVE_SOL_MINT.to_string(),
            1_000_000_000_000,
            1_000_000_000,
            2,
        );
        ironcrab::execution::pool_cache_sync::apply_pool_cache_update(&cache, &update_orca);
        ironcrab::execution::pool_cache_sync::apply_pool_cache_update(&cache, &update_pump);

        let mut trackers = HashMap::new();
        let mut vault_balances = HashMap::new();
        let mint_str = token_mint.to_string();
        let seeded = seed_token_tracker_from_live_pool_cache(
            &mint_str,
            &cache,
            &mut trackers,
            &mut vault_balances,
            None,
        );
        assert_eq!(seeded, 2);

        let mut known_pools = HashSet::new();
        known_pools.insert(pool_a.to_string());
        known_pools.insert(pool_b.to_string());

        let tracker = trackers.get_mut(&mint_str).unwrap();
        tracker.token_decimals = Some(6);
        assert_eq!(tracker.pools.len(), 2);
        assert_eq!(tracker.pool_count_on_distinct_dexes(), 2);

        let config = ArbConfig::default();
        let bin_arrays: HashMap<String, HashMap<i64, BinArrayCache>> = HashMap::new();
        let spread_warn_last = RwLock::new(HashMap::new());
        let data_quality_rejects = AtomicU64::new(0);
        let opp = tracker.check_arbitrage(
            &config,
            &known_pools,
            &vault_balances,
            &bin_arrays,
            &ArbCheckContext {
                spread_warn_last: &spread_warn_last,
                data_quality_rejects: &data_quality_rejects,
                forensics: None,
                v2_forensics: None,
                selected_mints: None,
                pinned_pools: None,
            },
        );
        // Same reserves → spread ~0, rejected by spread_below_min not insufficient_pools
        assert!(
            opp.is_none(),
            "expected spread_below_min or similar, not insufficient_pools"
        );
        assert_eq!(tracker.pools.len(), 2);
    }

    #[test]
    fn check_arbitrage_v2_uses_round_trip_not_legacy_mids() {
        let cache = create_shared_cache();
        let token_mint = Pubkey::new_unique();
        let pool_a = Pubkey::new_unique();
        let pool_b = Pubkey::new_unique();
        let mint_str = token_mint.to_string();

        let update_orca = PoolCacheUpdate::new_balance_updated(
            TEST_COMPONENT,
            TEST_BUILD,
            TEST_RUN,
            pool_a.to_string(),
            "orca".to_string(),
            mint_str.clone(),
            NATIVE_SOL_MINT.to_string(),
            1_000_000_000_000,
            980_000_000,
            1,
        );
        let update_pump = PoolCacheUpdate::new_balance_updated(
            TEST_COMPONENT,
            TEST_BUILD,
            TEST_RUN,
            pool_b.to_string(),
            "pump_amm".to_string(),
            mint_str.clone(),
            NATIVE_SOL_MINT.to_string(),
            1_000_000_000_000,
            1_020_000_000,
            2,
        );
        ironcrab::execution::pool_cache_sync::apply_pool_cache_update(&cache, &update_orca);
        ironcrab::execution::pool_cache_sync::apply_pool_cache_update(&cache, &update_pump);

        let mut trackers = HashMap::new();
        let mut vault_balances = HashMap::new();
        seed_token_tracker_from_live_pool_cache(
            &mint_str,
            &cache,
            &mut trackers,
            &mut vault_balances,
            None,
        );

        let mut known_pools = HashSet::new();
        known_pools.insert(pool_a.to_string());
        known_pools.insert(pool_b.to_string());

        let tracker = trackers.get_mut(&mint_str).unwrap();
        tracker.token_decimals = Some(6);
        assert_eq!(tracker.pools.len(), 2, "expected two seeded pools");
        assert_eq!(vault_balances.len(), 2, "expected two vault entries");

        let config = with_small_v2_probe(ArbConfig {
            arb_two_hop_v2_enabled: true,
            min_spread_bps: 1,
            min_profit_lamports: 1,
            est_tx_cost_lamports: 1,
            ..Default::default()
        });

        let bin_arrays: HashMap<String, HashMap<i64, BinArrayCache>> = HashMap::new();
        let spread_warn_last = RwLock::new(HashMap::new());
        let data_quality_rejects = AtomicU64::new(0);
        let opp = tracker.check_arbitrage(
            &config,
            &known_pools,
            &vault_balances,
            &bin_arrays,
            &ArbCheckContext {
                spread_warn_last: &spread_warn_last,
                data_quality_rejects: &data_quality_rejects,
                forensics: None,
                v2_forensics: None,
                selected_mints: None,
                pinned_pools: None,
            },
        );
        let opp = opp.expect("v2 round-trip should find cross-dex edge");
        assert_eq!(opp.buy_pool, pool_a.to_string());
        assert_eq!(opp.sell_pool, pool_b.to_string());
        assert!(opp.spread_bps > 0);
        assert!(opp.estimated_profit_lamports > 0);
    }

    #[test]
    fn check_arbitrage_v2_rejects_excessive_slot_delta() {
        let cache = create_shared_cache();
        let token_mint = Pubkey::new_unique();
        let pool_a = Pubkey::new_unique();
        let pool_b = Pubkey::new_unique();
        let mint_str = token_mint.to_string();

        let update_orca = PoolCacheUpdate::new_balance_updated(
            TEST_COMPONENT,
            TEST_BUILD,
            TEST_RUN,
            pool_a.to_string(),
            "orca".to_string(),
            mint_str.clone(),
            NATIVE_SOL_MINT.to_string(),
            1_000_000_000_000,
            980_000_000,
            1,
        );
        let update_pump = PoolCacheUpdate::new_balance_updated(
            TEST_COMPONENT,
            TEST_BUILD,
            TEST_RUN,
            pool_b.to_string(),
            "pump_amm".to_string(),
            mint_str.clone(),
            NATIVE_SOL_MINT.to_string(),
            1_000_000_000_000,
            1_020_000_000,
            100,
        );
        ironcrab::execution::pool_cache_sync::apply_pool_cache_update(&cache, &update_orca);
        ironcrab::execution::pool_cache_sync::apply_pool_cache_update(&cache, &update_pump);

        let mut trackers = HashMap::new();
        let mut vault_balances = HashMap::new();
        seed_token_tracker_from_live_pool_cache(
            &mint_str,
            &cache,
            &mut trackers,
            &mut vault_balances,
            None,
        );

        let mut known_pools = HashSet::new();
        known_pools.insert(pool_a.to_string());
        known_pools.insert(pool_b.to_string());

        let tracker = trackers.get_mut(&mint_str).unwrap();
        tracker.token_decimals = Some(6);

        let config = with_small_v2_probe(ArbConfig {
            arb_two_hop_v2_enabled: true,
            arb_max_leg_slot_delta: 2,
            min_spread_bps: 1,
            min_profit_lamports: 1,
            est_tx_cost_lamports: 1,
            ..Default::default()
        });

        let bin_arrays: HashMap<String, HashMap<i64, BinArrayCache>> = HashMap::new();
        let spread_warn_last = RwLock::new(HashMap::new());
        let data_quality_rejects = AtomicU64::new(0);
        let before =
            ironcrab::metrics::ARB_TWO_HOP_V2_REJECTED_SLOT_DELTA_EXCEEDED.load(Ordering::Relaxed);
        let opp = tracker.check_arbitrage(
            &config,
            &known_pools,
            &vault_balances,
            &bin_arrays,
            &ArbCheckContext {
                spread_warn_last: &spread_warn_last,
                data_quality_rejects: &data_quality_rejects,
                forensics: None,
                v2_forensics: None,
                selected_mints: None,
                pinned_pools: None,
            },
        );
        assert!(opp.is_none(), "slot delta 99 should exceed default gate");
        assert!(
            ironcrab::metrics::ARB_TWO_HOP_V2_REJECTED_SLOT_DELTA_EXCEEDED.load(Ordering::Relaxed)
                > before
        );
    }

    #[test]
    fn sync_arb_probe_follows_max_position_by_default() {
        let config = ArbConfig::default();
        assert!(config.arb_probe_follows_max_position);
        assert_eq!(config.arb_probe_lamports, config.max_position_lamports);
    }

    #[test]
    fn max_position_update_syncs_probe_when_follows_enabled() {
        let mut config = ArbConfig {
            max_position_lamports: 250_000_000,
            ..Default::default()
        };
        sync_arb_probe_to_max_position(&mut config);
        assert_eq!(config.arb_probe_lamports, 250_000_000);
    }

    #[test]
    fn explicit_arb_probe_override_preserves_custom_probe_on_max_position_update() {
        let mut config = ArbConfig {
            arb_probe_lamports: 42_000_000,
            arb_probe_follows_max_position: false,
            max_position_lamports: 500_000_000,
            ..Default::default()
        };
        sync_arb_probe_to_max_position(&mut config);
        assert_eq!(config.arb_probe_lamports, 42_000_000);
    }

    #[test]
    fn check_arbitrage_v2_rejects_spread_below_min_with_split_reason() {
        use ironcrab::metrics::ARB_TWO_HOP_V2_REJECTED_ROUND_TRIP_SPREAD_BELOW_MIN;

        let cache = create_shared_cache();
        let token_mint = Pubkey::new_unique();
        let pool_a = Pubkey::new_unique();
        let pool_b = Pubkey::new_unique();
        let mint_str = token_mint.to_string();

        let update_orca = PoolCacheUpdate::new_balance_updated(
            TEST_COMPONENT,
            TEST_BUILD,
            TEST_RUN,
            pool_a.to_string(),
            "orca".to_string(),
            mint_str.clone(),
            NATIVE_SOL_MINT.to_string(),
            1_000_000_000_000,
            1_000_000_000,
            1,
        );
        let update_pump = PoolCacheUpdate::new_balance_updated(
            TEST_COMPONENT,
            TEST_BUILD,
            TEST_RUN,
            pool_b.to_string(),
            "pump_amm".to_string(),
            mint_str.clone(),
            NATIVE_SOL_MINT.to_string(),
            1_000_000_000_000,
            1_000_000_000,
            2,
        );
        ironcrab::execution::pool_cache_sync::apply_pool_cache_update(&cache, &update_orca);
        ironcrab::execution::pool_cache_sync::apply_pool_cache_update(&cache, &update_pump);

        let mut trackers = HashMap::new();
        let mut vault_balances = HashMap::new();
        seed_token_tracker_from_live_pool_cache(
            &mint_str,
            &cache,
            &mut trackers,
            &mut vault_balances,
            None,
        );

        let mut known_pools = HashSet::new();
        known_pools.insert(pool_a.to_string());
        known_pools.insert(pool_b.to_string());

        let tracker = trackers.get_mut(&mint_str).unwrap();
        tracker.token_decimals = Some(6);

        let config = with_small_v2_probe(ArbConfig {
            arb_two_hop_v2_enabled: true,
            min_spread_bps: 10_000,
            min_profit_lamports: 1,
            est_tx_cost_lamports: 1,
            ..Default::default()
        });

        let bin_arrays: HashMap<String, HashMap<i64, BinArrayCache>> = HashMap::new();
        let spread_warn_last = RwLock::new(HashMap::new());
        let data_quality_rejects = AtomicU64::new(0);
        let reject_before =
            ARB_TWO_HOP_V2_REJECTED_ROUND_TRIP_SPREAD_BELOW_MIN.load(Ordering::Relaxed);
        let opp = tracker.check_arbitrage(
            &config,
            &known_pools,
            &vault_balances,
            &bin_arrays,
            &ArbCheckContext {
                spread_warn_last: &spread_warn_last,
                data_quality_rejects: &data_quality_rejects,
                forensics: None,
                v2_forensics: None,
                selected_mints: None,
                pinned_pools: None,
            },
        );
        assert!(opp.is_none(), "equal reserves should fail spread gate");
        assert!(
            ARB_TWO_HOP_V2_REJECTED_ROUND_TRIP_SPREAD_BELOW_MIN.load(Ordering::Relaxed)
                > reject_before
        );
    }

    #[test]
    fn check_arbitrage_v2_rejects_profit_below_min_with_split_reason() {
        use ironcrab::metrics::ARB_TWO_HOP_V2_REJECTED_ROUND_TRIP_PROFIT_BELOW_MIN;

        let cache = create_shared_cache();
        let token_mint = Pubkey::new_unique();
        let pool_a = Pubkey::new_unique();
        let pool_b = Pubkey::new_unique();
        let mint_str = token_mint.to_string();

        let update_orca = PoolCacheUpdate::new_balance_updated(
            TEST_COMPONENT,
            TEST_BUILD,
            TEST_RUN,
            pool_a.to_string(),
            "orca".to_string(),
            mint_str.clone(),
            NATIVE_SOL_MINT.to_string(),
            1_000_000_000_000,
            980_000_000,
            1,
        );
        let update_pump = PoolCacheUpdate::new_balance_updated(
            TEST_COMPONENT,
            TEST_BUILD,
            TEST_RUN,
            pool_b.to_string(),
            "pump_amm".to_string(),
            mint_str.clone(),
            NATIVE_SOL_MINT.to_string(),
            1_000_000_000_000,
            1_020_000_000,
            2,
        );
        ironcrab::execution::pool_cache_sync::apply_pool_cache_update(&cache, &update_orca);
        ironcrab::execution::pool_cache_sync::apply_pool_cache_update(&cache, &update_pump);

        let mut trackers = HashMap::new();
        let mut vault_balances = HashMap::new();
        seed_token_tracker_from_live_pool_cache(
            &mint_str,
            &cache,
            &mut trackers,
            &mut vault_balances,
            None,
        );

        let mut known_pools = HashSet::new();
        known_pools.insert(pool_a.to_string());
        known_pools.insert(pool_b.to_string());

        let tracker = trackers.get_mut(&mint_str).unwrap();
        tracker.token_decimals = Some(6);

        let config = with_small_v2_probe(ArbConfig {
            arb_two_hop_v2_enabled: true,
            min_spread_bps: 1,
            min_profit_lamports: 1_000_000_000_000,
            est_tx_cost_lamports: 1,
            ..Default::default()
        });

        let bin_arrays: HashMap<String, HashMap<i64, BinArrayCache>> = HashMap::new();
        let spread_warn_last = RwLock::new(HashMap::new());
        let data_quality_rejects = AtomicU64::new(0);
        let reject_before =
            ARB_TWO_HOP_V2_REJECTED_ROUND_TRIP_PROFIT_BELOW_MIN.load(Ordering::Relaxed);
        let opp = tracker.check_arbitrage(
            &config,
            &known_pools,
            &vault_balances,
            &bin_arrays,
            &ArbCheckContext {
                spread_warn_last: &spread_warn_last,
                data_quality_rejects: &data_quality_rejects,
                forensics: None,
                v2_forensics: None,
                selected_mints: None,
                pinned_pools: None,
            },
        );
        assert!(opp.is_none(), "tiny edge should fail profit gate");
        assert!(
            ARB_TWO_HOP_V2_REJECTED_ROUND_TRIP_PROFIT_BELOW_MIN.load(Ordering::Relaxed)
                > reject_before
        );
    }

    fn run_v2_slot_skew_screen(buy_slot: u64, sell_slot: u64) {
        let cache = create_shared_cache();
        let token_mint = Pubkey::new_unique();
        let pool_a = Pubkey::new_unique();
        let pool_b = Pubkey::new_unique();
        let mint_str = token_mint.to_string();

        let update_orca = PoolCacheUpdate::new_balance_updated(
            TEST_COMPONENT,
            TEST_BUILD,
            TEST_RUN,
            pool_a.to_string(),
            "orca".to_string(),
            mint_str.clone(),
            NATIVE_SOL_MINT.to_string(),
            1_000_000_000_000,
            980_000_000,
            buy_slot,
        );
        let update_pump = PoolCacheUpdate::new_balance_updated(
            TEST_COMPONENT,
            TEST_BUILD,
            TEST_RUN,
            pool_b.to_string(),
            "pump_amm".to_string(),
            mint_str.clone(),
            NATIVE_SOL_MINT.to_string(),
            1_000_000_000_000,
            1_020_000_000,
            sell_slot,
        );
        ironcrab::execution::pool_cache_sync::apply_pool_cache_update(&cache, &update_orca);
        ironcrab::execution::pool_cache_sync::apply_pool_cache_update(&cache, &update_pump);

        let mut trackers = HashMap::new();
        let mut vault_balances = HashMap::new();
        seed_token_tracker_from_live_pool_cache(
            &mint_str,
            &cache,
            &mut trackers,
            &mut vault_balances,
            None,
        );

        let mut known_pools = HashSet::new();
        known_pools.insert(pool_a.to_string());
        known_pools.insert(pool_b.to_string());

        let tracker = trackers.get_mut(&mint_str).unwrap();
        tracker.token_decimals = Some(6);

        let config = with_small_v2_probe(ArbConfig {
            arb_two_hop_v2_enabled: true,
            arb_max_leg_slot_delta: 0,
            min_spread_bps: 1,
            min_profit_lamports: 1,
            est_tx_cost_lamports: 1,
            ..Default::default()
        });

        let bin_arrays: HashMap<String, HashMap<i64, BinArrayCache>> = HashMap::new();
        let spread_warn_last = RwLock::new(HashMap::new());
        let data_quality_rejects = AtomicU64::new(0);
        let _ = tracker.check_arbitrage(
            &config,
            &known_pools,
            &vault_balances,
            &bin_arrays,
            &ArbCheckContext {
                spread_warn_last: &spread_warn_last,
                data_quality_rejects: &data_quality_rejects,
                forensics: None,
                v2_forensics: None,
                selected_mints: None,
                pinned_pools: None,
            },
        );
    }

    #[test]
    fn check_arbitrage_v2_records_slot_skew_histogram_and_leg_attribution() {
        use ironcrab::metrics::{
            ARB_QUOTE_PAIR_SLOT_DELTA_SLOTS_COUNT, ARB_QUOTE_PAIR_SLOT_DELTA_SLOTS_SUM,
            ARB_QUOTE_PAIR_SLOT_SKEW_LEG_BUY_TOTAL, ARB_QUOTE_PAIR_SLOT_SKEW_LEG_EQUAL_TOTAL,
            ARB_QUOTE_PAIR_SLOT_SKEW_LEG_SELL_TOTAL,
        };

        let count_before = ARB_QUOTE_PAIR_SLOT_DELTA_SLOTS_COUNT.load(Ordering::Relaxed);
        let equal_before = ARB_QUOTE_PAIR_SLOT_SKEW_LEG_EQUAL_TOTAL.load(Ordering::Relaxed);
        let sum_before = ARB_QUOTE_PAIR_SLOT_DELTA_SLOTS_SUM.load(Ordering::Relaxed);
        run_v2_slot_skew_screen(50, 50);
        assert_eq!(
            ARB_QUOTE_PAIR_SLOT_DELTA_SLOTS_COUNT.load(Ordering::Relaxed),
            count_before + 1
        );
        assert_eq!(
            ARB_QUOTE_PAIR_SLOT_SKEW_LEG_EQUAL_TOTAL.load(Ordering::Relaxed),
            equal_before + 1
        );
        assert_eq!(
            ARB_QUOTE_PAIR_SLOT_DELTA_SLOTS_SUM.load(Ordering::Relaxed),
            sum_before
        );

        let buy_before = ARB_QUOTE_PAIR_SLOT_SKEW_LEG_BUY_TOTAL.load(Ordering::Relaxed);
        let sum_before = ARB_QUOTE_PAIR_SLOT_DELTA_SLOTS_SUM.load(Ordering::Relaxed);
        run_v2_slot_skew_screen(48, 50);
        assert_eq!(
            ARB_QUOTE_PAIR_SLOT_SKEW_LEG_BUY_TOTAL.load(Ordering::Relaxed),
            buy_before + 1
        );
        assert_eq!(
            ARB_QUOTE_PAIR_SLOT_DELTA_SLOTS_SUM.load(Ordering::Relaxed),
            sum_before + 2
        );

        let buy_before = ARB_QUOTE_PAIR_SLOT_SKEW_LEG_BUY_TOTAL.load(Ordering::Relaxed);
        let sell_before = ARB_QUOTE_PAIR_SLOT_SKEW_LEG_SELL_TOTAL.load(Ordering::Relaxed);
        let sum_before = ARB_QUOTE_PAIR_SLOT_DELTA_SLOTS_SUM.load(Ordering::Relaxed);
        run_v2_slot_skew_screen(1, 101);
        assert_eq!(
            ARB_QUOTE_PAIR_SLOT_SKEW_LEG_BUY_TOTAL.load(Ordering::Relaxed),
            buy_before + 1
        );
        assert_eq!(
            ARB_QUOTE_PAIR_SLOT_SKEW_LEG_SELL_TOTAL.load(Ordering::Relaxed),
            sell_before
        );
        assert_eq!(
            ARB_QUOTE_PAIR_SLOT_DELTA_SLOTS_SUM.load(Ordering::Relaxed),
            sum_before + 100
        );
    }

    #[test]
    fn handle_pool_created_returns_mint_when_multi_dex() {
        let cache = create_shared_cache();
        let ctx = test_arb_context(cache);

        let mint = "TokenMint11111111111111111111111111111111";
        assert!(ctx
            .handle_pool_created("poolA", mint, NATIVE_SOL_MINT, "orca", Decimal::ONE)
            .is_none());
        let multi = ctx
            .handle_pool_created("poolB", mint, NATIVE_SOL_MINT, "pump_amm", Decimal::ONE)
            .expect("second dex should make mint multi-dex");
        assert_eq!(multi, mint);
    }

    #[test]
    fn bootstrap_warmup_seeds_two_dex_pools_into_one_tracker() {
        let cache = create_shared_cache();
        let token_mint = Pubkey::new_unique();
        let pool_a = Pubkey::new_unique();
        let pool_b = Pubkey::new_unique();
        let mint_str = token_mint.to_string();

        let update_orca = PoolCacheUpdate::new_balance_updated(
            TEST_COMPONENT,
            TEST_BUILD,
            TEST_RUN,
            pool_a.to_string(),
            "orca".to_string(),
            mint_str.clone(),
            NATIVE_SOL_MINT.to_string(),
            1_000_000_000_000,
            1_000_000_000,
            1,
        );
        let update_pump = PoolCacheUpdate::new_balance_updated(
            TEST_COMPONENT,
            TEST_BUILD,
            TEST_RUN,
            pool_b.to_string(),
            "pump_amm".to_string(),
            mint_str.clone(),
            NATIVE_SOL_MINT.to_string(),
            1_000_000_000_000,
            1_000_000_000,
            2,
        );
        ironcrab::execution::pool_cache_sync::apply_pool_cache_update(&cache, &update_orca);
        ironcrab::execution::pool_cache_sync::apply_pool_cache_update(&cache, &update_pump);
        cache.set_mint_decimals(token_mint, 6);

        let mut trackers = HashMap::new();
        let mut vault_balances = HashMap::new();
        let stats =
            seed_all_trackers_from_live_pool_cache(&cache, &mut trackers, &mut vault_balances);
        assert_eq!(stats.tracker_seeded_pools, 2);
        assert_eq!(stats.tracker_seed_candidates, 2);
        let tracker = trackers.get(&mint_str).expect("single token tracker");
        assert_eq!(tracker.pools.len(), 2);
        assert_eq!(tracker.pool_count_on_distinct_dexes(), 2);
        assert_eq!(tracker.token_decimals, Some(6));
    }

    #[test]
    fn usdc_quoted_pool_seeds_without_synthetic_sol_reserve_price() {
        let cache = create_shared_cache();
        let token_mint = Pubkey::new_unique();
        let pool = Pubkey::new_unique();
        let mint_str = token_mint.to_string();

        let update = PoolCacheUpdate::new_balance_updated(
            TEST_COMPONENT,
            TEST_BUILD,
            TEST_RUN,
            pool.to_string(),
            "raydium_cpmm".to_string(),
            mint_str.clone(),
            USDC_MINT.to_string(),
            1_000_000_000_000,
            1_000_000_000,
            1,
        );
        ironcrab::execution::pool_cache_sync::apply_pool_cache_update(&cache, &update);
        cache.set_mint_decimals(token_mint, 6);

        let mut trackers = HashMap::new();
        let mut vault_balances = HashMap::new();
        let stats =
            seed_all_trackers_from_live_pool_cache(&cache, &mut trackers, &mut vault_balances);
        assert_eq!(stats.tracker_seeded_pools, 1);
        let tracker = trackers.get(&mint_str).unwrap();
        let pool_str = pool.to_string();
        let pool_state = tracker.pools.get(&pool_str).unwrap();
        assert!(
            !pool_state.has_reserve_data,
            "USDC-quoted warmup must not mark SOL-style reserve data"
        );
        assert!(
            pool_state.last_price.is_none(),
            "USDC-quoted reserves must not synthesize SOL/token mid"
        );
        assert!(
            !vault_balances.contains_key(&pool_str),
            "USDC-quoted warmup must not write vault_balances (reserve_quote is not SOL)"
        );
    }

    #[test]
    fn usdc_quoted_pool_eligibility_has_no_synthetic_comparable_price() {
        let cache = create_shared_cache();
        let token_mint = Pubkey::new_unique();
        let pool = Pubkey::new_unique();
        let mint_str = token_mint.to_string();
        let pool_str = pool.to_string();

        let update = PoolCacheUpdate::new_balance_updated(
            TEST_COMPONENT,
            TEST_BUILD,
            TEST_RUN,
            pool_str.clone(),
            "raydium_cpmm".to_string(),
            mint_str.clone(),
            USDC_MINT.to_string(),
            1_000_000_000_000,
            1_000_000_000,
            1,
        );
        ironcrab::execution::pool_cache_sync::apply_pool_cache_update(&cache, &update);
        cache.set_mint_decimals(token_mint, 6);

        let mut trackers = HashMap::new();
        let mut vault_balances = HashMap::new();
        let stats =
            seed_all_trackers_from_live_pool_cache(&cache, &mut trackers, &mut vault_balances);
        assert_eq!(stats.tracker_seeded_pools, 1);

        let tracker = trackers.get(&mint_str).unwrap();
        assert_eq!(tracker.token_decimals, Some(6));

        let mut known_pools = HashSet::new();
        known_pools.insert(pool_str.clone());
        let bin_arrays: HashMap<String, HashMap<i64, BinArrayCache>> = HashMap::new();
        let breakdown =
            tracker.build_eligibility_breakdown(&known_pools, &vault_balances, &bin_arrays);

        assert_eq!(breakdown.candidate_pools_total, 1);
        assert_eq!(breakdown.known_pools, 1);
        assert_eq!(
            breakdown.comparable_price_present, 0,
            "USDC vault reserves must not produce SOL/token comparable price"
        );
        assert_eq!(breakdown.comparable_price_plausible, 0);
        let row = breakdown.pool_rows.first().expect("one pool row");
        assert!(!row.comparable_price_present);
        assert!(!row.comparable_price_plausible);
        assert!(!row.has_reserve_data);
    }

    #[test]
    fn pool_state_update_usdc_quoted_does_not_write_vault_or_synthetic_price() {
        let cache = create_shared_cache();
        let token_mint = Pubkey::new_unique();
        let pool = Pubkey::new_unique();
        let mint_str = token_mint.to_string();
        let pool_str = pool.to_string();

        let update = PoolCacheUpdate::new_balance_updated(
            TEST_COMPONENT,
            TEST_BUILD,
            TEST_RUN,
            pool_str.clone(),
            "raydium_cpmm".to_string(),
            mint_str.clone(),
            USDC_MINT.to_string(),
            1_000_000_000_000,
            1_000_000_000,
            1,
        );
        ironcrab::execution::pool_cache_sync::apply_pool_cache_update(&cache, &update);
        cache.set_mint_decimals(token_mint, 6);

        let mut trackers = HashMap::new();
        let mut vault_balances = HashMap::new();
        let stats =
            seed_all_trackers_from_live_pool_cache(&cache, &mut trackers, &mut vault_balances);
        assert_eq!(stats.tracker_seeded_pools, 1);
        assert!(!vault_balances.contains_key(&pool_str));

        let ctx = test_arb_context(cache);
        *ctx.trackers.write() = trackers;
        *ctx.vault_balances.write() = vault_balances;

        ctx.handle_pool_state_update(
            &pool_str,
            "raydium_cpmm",
            2_000_000_000_000,
            2_000_000_000,
            99,
            None,
            None,
            &mint_str,
            USDC_MINT,
        );

        assert!(
            !ctx.vault_balances.read().contains_key(&pool_str),
            "USDC PoolStateUpdate must not write vault_balances (reserve_quote is not SOL)"
        );
        let trackers = ctx.trackers.read();
        let pool_state = trackers
            .get(&mint_str)
            .and_then(|t| t.pools.get(&pool_str))
            .expect("tracked USDC pool");
        assert!(
            !pool_state.has_reserve_data,
            "USDC PoolStateUpdate must not set SOL-style reserve flag"
        );
        assert!(
            pool_state.last_price.is_none(),
            "USDC PoolStateUpdate must not synthesize SOL/token last_price"
        );
    }

    #[test]
    fn pool_state_update_sol_quoted_updates_vault_and_reserve_data() {
        let cache = create_shared_cache();
        let token_mint = Pubkey::new_unique();
        let pool = Pubkey::new_unique();
        let mint_str = token_mint.to_string();
        let pool_str = pool.to_string();

        let update = PoolCacheUpdate::new_balance_updated(
            TEST_COMPONENT,
            TEST_BUILD,
            TEST_RUN,
            pool_str.clone(),
            "orca".to_string(),
            mint_str.clone(),
            NATIVE_SOL_MINT.to_string(),
            1_000_000_000_000,
            1_000_000_000,
            1,
        );
        ironcrab::execution::pool_cache_sync::apply_pool_cache_update(&cache, &update);
        cache.set_mint_decimals(token_mint, 6);

        let mut trackers = HashMap::new();
        let mut vault_balances = HashMap::new();
        seed_all_trackers_from_live_pool_cache(&cache, &mut trackers, &mut vault_balances);

        let ctx = test_arb_context(cache);
        *ctx.trackers.write() = trackers;
        *ctx.vault_balances.write() = vault_balances;

        let new_token_reserve = 2_000_000_000_000u64;
        let new_sol_reserve = 2_000_000_000u64;
        ctx.handle_pool_state_update(
            &pool_str,
            "orca",
            new_token_reserve,
            new_sol_reserve,
            99,
            None,
            None,
            &mint_str,
            NATIVE_SOL_MINT,
        );

        let vault_balances = ctx.vault_balances.read();
        let vault = vault_balances
            .get(&pool_str)
            .expect("SOL PoolStateUpdate must cache vault balances");
        assert_eq!(vault.reserve_base, new_token_reserve);
        assert_eq!(vault.reserve_quote, new_sol_reserve);
        assert_eq!(vault.update_slot, 99);

        let trackers = ctx.trackers.read();
        let pool_state = trackers
            .get(&mint_str)
            .and_then(|t| t.pools.get(&pool_str))
            .expect("tracked SOL pool");
        assert!(pool_state.has_reserve_data);
        assert!(
            pool_state.last_price.is_some(),
            "SOL PoolStateUpdate with decimals should set reserve-based last_price"
        );
    }

    #[test]
    fn incremental_balance_updated_seeds_without_full_scan() {
        let cache = create_shared_cache();
        let token_mint = Pubkey::new_unique();
        let pool = Pubkey::new_unique();
        let mint_str = token_mint.to_string();
        let pool_str = pool.to_string();

        let update = PoolCacheUpdate::new_balance_updated(
            TEST_COMPONENT,
            TEST_BUILD,
            TEST_RUN,
            pool_str.clone(),
            "orca".to_string(),
            mint_str.clone(),
            NATIVE_SOL_MINT.to_string(),
            1_000_000_000_000,
            1_000_000_000,
            1,
        );
        ironcrab::execution::pool_cache_sync::apply_pool_cache_update(&cache, &update);
        cache.set_mint_decimals(token_mint, 6);

        let mut trackers = HashMap::new();
        let mut vault_balances = HashMap::new();
        let seeded = seed_token_tracker_from_live_pool_cache(
            &mint_str,
            &cache,
            &mut trackers,
            &mut vault_balances,
            Some(&pool_str),
        );
        assert_eq!(seeded, 1);
        assert_eq!(trackers.len(), 1);
        assert_eq!(trackers.get(&mint_str).unwrap().pools.len(), 1);
    }

    #[test]
    fn partial_pool_without_reserves_is_skipped_not_synthesized() {
        let cache = create_shared_cache();
        let token_mint = Pubkey::new_unique();
        let pool = Pubkey::new_unique();
        let mint_str = token_mint.to_string();

        let update = PoolCacheUpdate::new_pool_discovered(
            TEST_COMPONENT,
            TEST_BUILD,
            TEST_RUN,
            pool.to_string(),
            "orca".to_string(),
            mint_str.clone(),
            NATIVE_SOL_MINT.to_string(),
            0,
            0,
            None,
            1,
        );
        ironcrab::execution::pool_cache_sync::apply_pool_cache_update(&cache, &update);

        let mut trackers = HashMap::new();
        let mut vault_balances = HashMap::new();
        let stats =
            seed_all_trackers_from_live_pool_cache(&cache, &mut trackers, &mut vault_balances);
        assert_eq!(stats.tracker_seeded_pools, 0);
        assert!(trackers.is_empty());
        assert!(vault_balances.is_empty());
    }

    #[test]
    fn seed_preserves_geyser_vault_when_jetstream_slot_is_stale() {
        use std::time::Duration;

        let cache = create_shared_cache();
        let token_mint = Pubkey::new_unique();
        let pool = Pubkey::new_unique();
        let mint_str = token_mint.to_string();
        let pool_str = pool.to_string();

        let stale_cache_update = PoolCacheUpdate::new_balance_updated(
            TEST_COMPONENT,
            TEST_BUILD,
            TEST_RUN,
            pool_str.clone(),
            "orca".to_string(),
            mint_str.clone(),
            NATIVE_SOL_MINT.to_string(),
            100,
            200,
            50,
        );
        ironcrab::execution::pool_cache_sync::apply_pool_cache_update(&cache, &stale_cache_update);

        let stale_vault_updated_at = Instant::now() - Duration::from_secs(400);
        let mut vault_balances = HashMap::from([(
            pool_str.clone(),
            VaultBalanceCache {
                reserve_base: 9_999,
                reserve_quote: 8_888,
                update_slot: 100,
                active_id: None,
                bin_step: None,
                updated_at: stale_vault_updated_at,
                dlmm_sol_is_x: false,
                dlmm_token_x_mint: None,
            },
        )]);
        let mut trackers = HashMap::new();

        seed_token_tracker_from_live_pool_cache(
            &mint_str,
            &cache,
            &mut trackers,
            &mut vault_balances,
            None,
        );

        let vault = vault_balances.get(&pool_str).unwrap();
        assert_eq!(vault.update_slot, 100);
        assert_eq!(vault.reserve_base, 9_999);
        assert_eq!(vault.reserve_quote, 8_888);
        assert!(
            vault.updated_at > stale_vault_updated_at,
            "stale vault.updated_at must follow fresher SLAVE cache age without slot regress"
        );
    }

    #[test]
    fn balance_updated_stale_slot_sustains_slave_age_and_refreshes_vault() {
        use std::time::Duration;

        let cache = create_shared_cache();
        let token_mint = Pubkey::new_unique();
        let pool = Pubkey::new_unique();
        let mint_str = token_mint.to_string();
        let pool_str = pool.to_string();

        let seed_update = PoolCacheUpdate::new_balance_updated(
            TEST_COMPONENT,
            TEST_BUILD,
            TEST_RUN,
            pool_str.clone(),
            "orca".to_string(),
            mint_str.clone(),
            NATIVE_SOL_MINT.to_string(),
            1_000_000_000_000,
            2_000_000_000,
            100,
        );
        ironcrab::execution::pool_cache_sync::apply_pool_cache_update(&cache, &seed_update);

        let ahead_state = cache.get(&pool).expect("seeded");
        let CachedPoolState::Orca(ref orca) = ahead_state else {
            panic!("expected orca");
        };
        let bumped = CachedPoolState::Orca(OrcaWhirlpoolState {
            token_mint_a: orca.token_mint_a,
            token_mint_b: orca.token_mint_b,
            token_vault_a: orca.token_vault_a,
            token_vault_b: orca.token_vault_b,
            tick_current_index: orca.tick_current_index,
            sqrt_price: orca.sqrt_price,
            liquidity: orca.liquidity,
            fee_rate: orca.fee_rate,
            protocol_fee_rate: orca.protocol_fee_rate,
            tick_spacing: orca.tick_spacing,
            vault_a_balance: orca.vault_a_balance,
            vault_b_balance: orca.vault_b_balance,
            token_a_program: orca.token_a_program,
            token_b_program: orca.token_b_program,
        });
        cache.upsert(pool, bumped, 200);

        std::thread::sleep(Duration::from_millis(40));
        let (_, _, age_before) = cache.get_with_metadata(&pool).expect("cached");
        assert!(age_before >= 30, "cache must age before heartbeat sustain");

        let stale_heartbeat = PoolCacheUpdate::new_balance_updated(
            TEST_COMPONENT,
            TEST_BUILD,
            TEST_RUN,
            pool_str.clone(),
            "orca".to_string(),
            mint_str.clone(),
            NATIVE_SOL_MINT.to_string(),
            1_000_000_000_000,
            2_000_000_000,
            100,
        );
        assert!(
            ironcrab::execution::pool_cache_sync::apply_pool_cache_update(&cache, &stale_heartbeat),
            "stale-slot heartbeat must apply age sustain"
        );

        let (_, slot_after, age_after) = cache.get_with_metadata(&pool).expect("cached");
        assert_eq!(slot_after, 200, "local Geyser slot must not regress");
        assert!(
            age_after < 20,
            "heartbeat BalanceUpdated must refresh SLAVE cache age"
        );

        let stale_vault_updated_at = Instant::now() - Duration::from_secs(400);
        let mut vault_balances = HashMap::from([(
            pool_str.clone(),
            VaultBalanceCache {
                reserve_base: 1_000_000_000_000,
                reserve_quote: 2_000_000_000,
                update_slot: 200,
                active_id: None,
                bin_step: None,
                updated_at: stale_vault_updated_at,
                dlmm_sol_is_x: false,
                dlmm_token_x_mint: None,
            },
        )]);
        let mut trackers = HashMap::new();

        seed_token_tracker_from_live_pool_cache(
            &mint_str,
            &cache,
            &mut trackers,
            &mut vault_balances,
            Some(&pool_str),
        );

        let vault = vault_balances.get(&pool_str).expect("vault");
        assert_eq!(vault.update_slot, 200);
        assert!(
            vault.updated_at > stale_vault_updated_at,
            "vault.updated_at must follow fresher cache age at seed"
        );
        let (_, _, age_ms) = cache.get_with_metadata(&pool).expect("cached");
        assert!(
            age_ms <= 120_000,
            "screen seed should land in le_120s freshness bucket, got age_ms={age_ms}"
        );
    }

    #[test]
    fn pool_cache_update_consume_seeds_pinned_vault_and_metrics() {
        use ironcrab::metrics::ARB_VAULT_SEED_FROM_CACHE_OK_TOTAL;
        use std::sync::atomic::Ordering;

        let cache = create_shared_cache();
        let ctx = test_arb_context(cache.clone());
        let token_mint = Pubkey::new_unique();
        let pool = Pubkey::new_unique();
        let mint_str = token_mint.to_string();
        let pool_str = pool.to_string();

        let update = PoolCacheUpdate::new_balance_updated(
            TEST_COMPONENT,
            TEST_BUILD,
            TEST_RUN,
            pool_str.clone(),
            "orca".to_string(),
            mint_str.clone(),
            NATIVE_SOL_MINT.to_string(),
            1_000_000_000_000,
            2_000_000_000,
            100,
        );
        ironcrab::execution::pool_cache_sync::apply_pool_cache_update(&cache, &update);

        ctx.arb_pinned_pools.write().insert(pool_str.clone());
        let ok_before = ARB_VAULT_SEED_FROM_CACHE_OK_TOTAL.load(Ordering::Relaxed);
        assert!(ctx.consume_vault_seed_from_pool_cache_update(&update));
        assert_eq!(
            ARB_VAULT_SEED_FROM_CACHE_OK_TOTAL.load(Ordering::Relaxed),
            ok_before + 1
        );
        assert!(ctx.vault_balances.read().contains_key(&pool_str));
    }

    #[test]
    fn pool_cache_update_consume_miss_when_pinned_pool_lacks_reserve_basis() {
        use ironcrab::metrics::ARB_VAULT_SEED_FROM_CACHE_MISS_TOTAL;
        use std::sync::atomic::Ordering;

        let cache = create_shared_cache();
        let ctx = test_arb_context(cache.clone());
        let token_mint = Pubkey::new_unique();
        let pool = Pubkey::new_unique();
        let pool_str = pool.to_string();
        let mint_str = token_mint.to_string();
        cache.upsert(
            pool,
            CachedPoolState::Orca(OrcaWhirlpoolState {
                token_mint_a: token_mint,
                token_mint_b: Pubkey::from_str(NATIVE_SOL_MINT).unwrap(),
                token_vault_a: Pubkey::new_unique(),
                token_vault_b: Pubkey::new_unique(),
                tick_current_index: 0,
                sqrt_price: 1,
                liquidity: 1,
                fee_rate: 300,
                protocol_fee_rate: 100,
                tick_spacing: 64,
                vault_a_balance: None,
                vault_b_balance: None,
                token_a_program: None,
                token_b_program: None,
            }),
            1,
        );
        let update = PoolCacheUpdate::new_balance_updated(
            TEST_COMPONENT,
            TEST_BUILD,
            TEST_RUN,
            pool_str.clone(),
            "orca".to_string(),
            mint_str,
            NATIVE_SOL_MINT.to_string(),
            0,
            0,
            1,
        );
        ctx.arb_pinned_pools.write().insert(pool_str);
        let miss_before = ARB_VAULT_SEED_FROM_CACHE_MISS_TOTAL.load(Ordering::Relaxed);
        assert!(!ctx.consume_vault_seed_from_pool_cache_update(&update));
        assert_eq!(
            ARB_VAULT_SEED_FROM_CACHE_MISS_TOTAL.load(Ordering::Relaxed),
            miss_before + 1
        );
    }

    #[test]
    fn live_snapshot_cache_age_records_pin_label() {
        use ironcrab::metrics::{
            ARB_VAULT_LIVE_SNAPSHOT_CACHE_AGE_PIN_SEED_LE_120S,
            ARB_VAULT_LIVE_SNAPSHOT_CACHE_AGE_PIN_SEED_LE_30S,
        };
        use std::sync::atomic::Ordering;

        let cache = create_shared_cache();
        let pool = Pubkey::new_unique();
        let pool_str = pool.to_string();
        let seed_update = PoolCacheUpdate::new_balance_updated(
            TEST_COMPONENT,
            TEST_BUILD,
            TEST_RUN,
            pool_str.clone(),
            "orca".to_string(),
            Pubkey::new_unique().to_string(),
            NATIVE_SOL_MINT.to_string(),
            1_000_000_000_000,
            2_000_000_000,
            100,
        );
        ironcrab::execution::pool_cache_sync::apply_pool_cache_update(&cache, &seed_update);

        let before_30s = ARB_VAULT_LIVE_SNAPSHOT_CACHE_AGE_PIN_SEED_LE_30S.load(Ordering::Relaxed);
        let before_120s =
            ARB_VAULT_LIVE_SNAPSHOT_CACHE_AGE_PIN_SEED_LE_120S.load(Ordering::Relaxed);
        let mut vault_balances = HashMap::new();
        assert!(try_seed_vault_from_live_cache(
            &pool_str,
            &cache,
            &mut vault_balances,
            "pin"
        ));
        let after_30s = ARB_VAULT_LIVE_SNAPSHOT_CACHE_AGE_PIN_SEED_LE_30S.load(Ordering::Relaxed);
        let after_120s = ARB_VAULT_LIVE_SNAPSHOT_CACHE_AGE_PIN_SEED_LE_120S.load(Ordering::Relaxed);
        assert!(
            after_30s > before_30s || after_120s > before_120s,
            "pin-labeled cache age bucket must increment for arb-pinned seed path"
        );
    }

    #[test]
    fn seed_updates_vault_when_jetstream_slot_is_newer_than_geyser() {
        let cache = create_shared_cache();
        let token_mint = Pubkey::new_unique();
        let pool = Pubkey::new_unique();
        let mint_str = token_mint.to_string();
        let pool_str = pool.to_string();

        let fresher_cache_update = PoolCacheUpdate::new_balance_updated(
            TEST_COMPONENT,
            TEST_BUILD,
            TEST_RUN,
            pool_str.clone(),
            "orca".to_string(),
            mint_str.clone(),
            NATIVE_SOL_MINT.to_string(),
            1_000_000_000_000,
            2_000_000_000,
            101,
        );
        ironcrab::execution::pool_cache_sync::apply_pool_cache_update(
            &cache,
            &fresher_cache_update,
        );

        let mut vault_balances = HashMap::from([(
            pool_str.clone(),
            VaultBalanceCache {
                reserve_base: 111,
                reserve_quote: 222,
                update_slot: 100,
                active_id: None,
                bin_step: None,
                updated_at: Instant::now() - Duration::from_secs(60),
                dlmm_sol_is_x: false,
                dlmm_token_x_mint: None,
            },
        )]);
        let mut trackers = HashMap::new();

        seed_token_tracker_from_live_pool_cache(
            &mint_str,
            &cache,
            &mut trackers,
            &mut vault_balances,
            None,
        );

        let vault = vault_balances.get(&pool_str).unwrap();
        assert_eq!(vault.update_slot, 101);
        assert_eq!(vault.reserve_base, 1_000_000_000_000);
        assert_eq!(vault.reserve_quote, 2_000_000_000);
    }

    #[test]
    fn incremental_only_pool_targets_single_cache_entry() {
        let cache = create_shared_cache();
        let token_mint = Pubkey::new_unique();
        let pool_a = Pubkey::new_unique();
        let pool_b = Pubkey::new_unique();
        let mint_str = token_mint.to_string();

        for (pool, slot) in [(pool_a, 1u64), (pool_b, 2u64)] {
            let update = PoolCacheUpdate::new_balance_updated(
                TEST_COMPONENT,
                TEST_BUILD,
                TEST_RUN,
                pool.to_string(),
                "orca".to_string(),
                mint_str.clone(),
                NATIVE_SOL_MINT.to_string(),
                1_000_000_000_000,
                1_000_000_000,
                slot,
            );
            ironcrab::execution::pool_cache_sync::apply_pool_cache_update(&cache, &update);
        }

        let mut trackers = HashMap::new();
        let mut vault_balances = HashMap::new();
        let seeded = seed_token_tracker_from_live_pool_cache(
            &mint_str,
            &cache,
            &mut trackers,
            &mut vault_balances,
            Some(&pool_a.to_string()),
        );
        assert_eq!(seeded, 1);
        let tracker = trackers.get(&mint_str).unwrap();
        assert!(tracker.pools.contains_key(&pool_a.to_string()));
        assert!(!tracker.pools.contains_key(&pool_b.to_string()));
        assert!(vault_balances.contains_key(&pool_a.to_string()));
        assert!(!vault_balances.contains_key(&pool_b.to_string()));
    }

    #[test]
    fn orca_wsol_usdc_mint_a_sol_comparable_price_sane() {
        let sol_lamports = 1_000_000_000u64;
        let usdc_raw = 65_000_000u64;
        let (token_reserve, sol_reserve) =
            orca_sol_quoted_vault_reserves(NATIVE_SOL_MINT, USDC_MINT, sol_lamports, usdc_raw)
                .expect("WSOL/USDC Orca pool");
        assert_eq!(token_reserve, usdc_raw);
        assert_eq!(sol_reserve, sol_lamports);

        let price = reserve_mid_sol_per_token(token_reserve, sol_reserve, 6).unwrap();
        let expected = Decimal::from(1u64) / Decimal::from(65u64);
        let ratio = if price > expected {
            price / expected
        } else {
            expected / price
        };
        assert!(
            ratio <= Decimal::from(2),
            "price {price} should be near 1/65 SOL/USDC, not 1e-7 or 0.026"
        );
        assert!(is_plausible_sol_per_token_price(USDC_MINT, price));
    }

    #[test]
    fn orca_usdc_wsol_swapped_orientation_same_price() {
        let sol_lamports = 1_000_000_000u64;
        let usdc_raw = 65_000_000u64;
        let price_a =
            orca_sol_quoted_vault_reserves(NATIVE_SOL_MINT, USDC_MINT, sol_lamports, usdc_raw)
                .and_then(|(tb, tq)| reserve_mid_sol_per_token(tb, tq, 6));
        let price_b =
            orca_sol_quoted_vault_reserves(USDC_MINT, NATIVE_SOL_MINT, usdc_raw, sol_lamports)
                .and_then(|(tb, tq)| reserve_mid_sol_per_token(tb, tq, 6));
        assert_eq!(price_a, price_b);
    }

    #[test]
    fn orca_and_dlmm_realistic_reserves_no_spread_too_large() {
        let reserve_base = 65_000_000u64;
        let reserve_quote = 1_000_000_000u64;
        let active_id: i32 = 0;
        let bin_step: u16 = 10;
        let (bin_arrays, vault, token_decimals) =
            usdc_sol_dlmm_fixture(false, reserve_base, reserve_quote, active_id, bin_step);

        let dlmm_pool = sample_pool("meteora_dlmm", "dlmmUsdc", None, None);
        let orca_pool = sample_pool("orca", "orcaUsdc", None, None);
        let p_dlmm = comparable_price_sol_per_token(
            &dlmm_pool,
            Some((reserve_base, reserve_quote)),
            Some(token_decimals),
            USDC_MINT,
            Some(&vault),
            Some(&bin_arrays),
            ComparablePriceSide::Buy,
        )
        .unwrap();
        let p_orca = comparable_price_sol_per_token(
            &orca_pool,
            Some((reserve_base, reserve_quote)),
            Some(token_decimals),
            USDC_MINT,
            Some(&vault),
            None,
            ComparablePriceSide::Buy,
        )
        .unwrap();
        let spread_bps = ((p_orca - p_dlmm) / p_dlmm * Decimal::from(10000))
            .abs()
            .round()
            .to_i64()
            .unwrap();
        assert!(
            spread_bps < STABLECOIN_MAX_SPREAD_BPS,
            "realistic Orca/DLMM reserves should not trip spread_too_large ({spread_bps} bps)"
        );
    }

    #[test]
    fn prod_like_swapped_reserves_rejected_not_spread_too_large() {
        let sol_in_base = 1_000_000_000u64;
        let usdc_in_quote = 65_000_000u64;
        assert!(!reserves_plausible_for_comparable_price(
            sol_in_base,
            usdc_in_quote,
            6,
            USDC_MINT
        ));

        let pool = sample_pool("orca", "orcaSwapped", None, None);
        let price = comparable_price_sol_per_token(
            &pool,
            Some((sol_in_base, usdc_in_quote)),
            Some(6),
            USDC_MINT,
            None,
            None,
            ComparablePriceSide::Buy,
        );
        assert!(
            price.is_none(),
            "prod-like swapped reserves must not produce comparable price"
        );

        let before_spread =
            ironcrab::metrics::ARB_TWO_HOP_REJECTED_SPREAD_TOO_LARGE.load(Ordering::Relaxed);
        let mut tracker = TokenArbTracker::new(USDC_MINT);
        tracker.token_decimals = Some(6);
        tracker.upsert_pool(pool);
        let mut known_pools = HashSet::new();
        known_pools.insert("orcaSwapped".to_string());
        let spread_warn_last = RwLock::new(HashMap::new());
        let data_quality_rejects = AtomicU64::new(0);
        let opp = tracker.check_arbitrage(
            &ArbConfig::default(),
            &known_pools,
            &HashMap::from([(
                "orcaSwapped".to_string(),
                VaultBalanceCache {
                    reserve_base: sol_in_base,
                    reserve_quote: usdc_in_quote,
                    update_slot: 1,
                    active_id: None,
                    bin_step: None,
                    updated_at: Instant::now(),
                    dlmm_sol_is_x: false,
                    dlmm_token_x_mint: None,
                },
            )]),
            &HashMap::new(),
            &ArbCheckContext {
                spread_warn_last: &spread_warn_last,
                data_quality_rejects: &data_quality_rejects,
                forensics: None,
                v2_forensics: None,
                selected_mints: None,
                pinned_pools: None,
            },
        );
        assert!(opp.is_none());
        assert_eq!(
            ironcrab::metrics::ARB_TWO_HOP_REJECTED_SPREAD_TOO_LARGE.load(Ordering::Relaxed),
            before_spread
        );
    }

    #[test]
    fn stablecoin_out_of_range_trade_mid_rejected() {
        let bad_buy = Decimal::from_str("0.000000094").unwrap();
        let bad_sell = Decimal::from_str("5.0").unwrap();
        assert!(!is_plausible_sol_per_token_price(USDC_MINT, bad_buy));
        assert!(!is_plausible_sol_per_token_price(USDC_MINT, bad_sell));

        let pool = sample_pool("orca", "orcaBad", Some(bad_buy), Some(bad_sell));
        let buy = comparable_price_sol_per_token(
            &pool,
            None,
            Some(6),
            USDC_MINT,
            None,
            None,
            ComparablePriceSide::Buy,
        );
        let sell = comparable_price_sol_per_token(
            &pool,
            None,
            Some(6),
            USDC_MINT,
            None,
            None,
            ComparablePriceSide::Sell,
        );
        assert!(buy.is_none());
        assert!(sell.is_none());
    }

    #[test]
    fn missing_decimals_no_synthetic_reserve_mid() {
        let pool = sample_pool("orca", "orcaNoDec", None, None);
        let reserves = (65_000_000u64, 1_000_000_000u64);
        let price = comparable_price_sol_per_token(
            &pool,
            Some(reserves),
            None,
            USDC_MINT,
            None,
            None,
            ComparablePriceSide::Buy,
        );
        assert!(price.is_none(), "must not assume 6 decimals when unknown");
    }

    fn check_with_forensics(
        tracker: &TokenArbTracker,
        known_pools: &HashSet<String>,
        vault_balances: &HashMap<String, VaultBalanceCache>,
        forensics: &ArbEligibilityForensics,
    ) -> Option<ArbOpportunity> {
        let spread_warn_last = RwLock::new(HashMap::new());
        let data_quality_rejects = AtomicU64::new(0);
        tracker.check_arbitrage(
            &ArbConfig::default(),
            known_pools,
            vault_balances,
            &HashMap::new(),
            &ArbCheckContext {
                spread_warn_last: &spread_warn_last,
                data_quality_rejects: &data_quality_rejects,
                forensics: Some(forensics),
                v2_forensics: None,
                selected_mints: None,
                pinned_pools: None,
            },
        )
    }

    fn check_with_v2_forensics(
        tracker: &TokenArbTracker,
        known_pools: &HashSet<String>,
        vault_balances: &HashMap<String, VaultBalanceCache>,
        v2_forensics: &ArbV2EligibilityForensics,
    ) -> Option<ArbOpportunity> {
        let spread_warn_last = RwLock::new(HashMap::new());
        let data_quality_rejects = AtomicU64::new(0);
        let config = with_small_v2_probe(ArbConfig {
            arb_two_hop_v2_enabled: true,
            ..Default::default()
        });
        tracker.check_arbitrage(
            &config,
            known_pools,
            vault_balances,
            &HashMap::new(),
            &ArbCheckContext {
                spread_warn_last: &spread_warn_last,
                data_quality_rejects: &data_quality_rejects,
                forensics: None,
                v2_forensics: Some(v2_forensics),
                selected_mints: None,
                pinned_pools: None,
            },
        )
    }

    fn vault(reserve_base: u64, reserve_quote: u64) -> VaultBalanceCache {
        sample_vault(reserve_base, reserve_quote, None, None, false, None)
    }

    #[test]
    fn forensics_not_known_pool_when_only_one_in_master_cache() {
        let before_known =
            ironcrab::metrics::ARB_TWO_HOP_INSUFFICIENT_NOT_KNOWN_POOL.load(Ordering::Relaxed);
        let reserves = (1_000_000_000_000u64, 1_000_000_000u64);
        let mut tracker = TokenArbTracker::new("TokenMint11111111111111111111111111111111");
        tracker.token_decimals = Some(6);
        tracker.upsert_pool(sample_pool("orca", "poolKnown", None, None));
        tracker.upsert_pool(sample_pool("pump_amm", "poolUnknown", None, None));

        let mut known_pools = HashSet::new();
        known_pools.insert("poolKnown".to_string());

        let vault_balances = HashMap::from([
            ("poolKnown".to_string(), vault(reserves.0, reserves.1)),
            ("poolUnknown".to_string(), vault(reserves.0, reserves.1)),
        ]);

        let forensics = ArbEligibilityForensics::new();
        assert!(
            check_with_forensics(&tracker, &known_pools, &vault_balances, &forensics).is_none()
        );
        assert!(
            ironcrab::metrics::ARB_TWO_HOP_INSUFFICIENT_NOT_KNOWN_POOL.load(Ordering::Relaxed)
                > before_known
        );
    }

    #[test]
    fn forensics_same_dex_only_when_both_pools_on_one_dex() {
        let before = ironcrab::metrics::ARB_TWO_HOP_REJECT_SAME_DEX_ONLY.load(Ordering::Relaxed);
        let reserves = (1_000_000_000_000u64, 1_000_000_000u64);
        let mint = "TokenMint22222222222222222222222222222222";
        let mut tracker = TokenArbTracker::new(mint);
        tracker.token_decimals = Some(6);
        tracker.upsert_pool(sample_pool("orca", "poolA", None, None));
        tracker.upsert_pool(sample_pool("orca", "poolB", None, None));

        let mut known_pools = HashSet::new();
        known_pools.insert("poolA".to_string());
        known_pools.insert("poolB".to_string());

        let vault_balances = HashMap::from([
            ("poolA".to_string(), vault(reserves.0, reserves.1)),
            ("poolB".to_string(), vault(reserves.0, reserves.1)),
        ]);

        let forensics = ArbEligibilityForensics::new();
        assert!(
            check_with_forensics(&tracker, &known_pools, &vault_balances, &forensics).is_none()
        );
        assert!(
            ironcrab::metrics::ARB_TWO_HOP_REJECT_SAME_DEX_ONLY.load(Ordering::Relaxed) > before
        );
    }

    #[test]
    fn forensics_stale_price_when_one_dex_stale() {
        let before = ironcrab::metrics::ARB_TWO_HOP_REJECT_STALE_PRICE.load(Ordering::Relaxed);
        let mint = "TokenMint33333333333333333333333333333333";
        let mut tracker = TokenArbTracker::new(mint);
        tracker.token_decimals = Some(6);
        tracker.upsert_pool(PoolState {
            has_reserve_data: true,
            ..sample_pool("orca", "poolFresh", None, None)
        });
        tracker.upsert_pool(PoolState {
            has_reserve_data: true,
            last_update: Instant::now() - Duration::from_millis(MAX_PRICE_AGE_MS + 5_000),
            ..sample_pool("pump_amm", "poolStale", None, None)
        });

        let mut known_pools = HashSet::new();
        known_pools.insert("poolFresh".to_string());
        known_pools.insert("poolStale".to_string());

        let vault_balances = HashMap::from([
            (
                "poolFresh".to_string(),
                vault(1_000_000_000_000, 1_000_000_000),
            ),
            (
                "poolStale".to_string(),
                VaultBalanceCache {
                    reserve_base: 500_000_000_000,
                    reserve_quote: 1_000_000_000,
                    updated_at: Instant::now() - Duration::from_millis(MAX_PRICE_AGE_MS + 5_000),
                    ..vault(500_000_000_000, 1_000_000_000)
                },
            ),
        ]);

        let forensics = ArbEligibilityForensics::new();
        assert!(
            check_with_forensics(&tracker, &known_pools, &vault_balances, &forensics).is_none()
        );
        assert!(ironcrab::metrics::ARB_TWO_HOP_REJECT_STALE_PRICE.load(Ordering::Relaxed) > before);
    }

    #[test]
    fn forensics_missing_decimals_subreason() {
        let before = ironcrab::metrics::ARB_TWO_HOP_REJECT_MISSING_DECIMALS.load(Ordering::Relaxed);
        let reserves = (1_000_000_000_000u64, 1_000_000_000u64);
        let mut tracker = TokenArbTracker::new("TokenMint44444444444444444444444444444444");
        tracker.upsert_pool(sample_pool("orca", "poolA", None, None));
        tracker.upsert_pool(sample_pool("pump_amm", "poolB", None, None));

        let mut known_pools = HashSet::new();
        known_pools.insert("poolA".to_string());
        known_pools.insert("poolB".to_string());
        let vault_balances = HashMap::from([
            ("poolA".to_string(), vault(reserves.0, reserves.1)),
            ("poolB".to_string(), vault(reserves.0, reserves.1)),
        ]);

        let forensics = ArbEligibilityForensics::new();
        assert!(
            check_with_forensics(&tracker, &known_pools, &vault_balances, &forensics).is_none()
        );
        assert!(
            ironcrab::metrics::ARB_TWO_HOP_REJECT_MISSING_DECIMALS.load(Ordering::Relaxed) > before
        );
    }

    #[test]
    fn forensics_implausible_stablecoin_not_spread_too_large() {
        let spread_before =
            ironcrab::metrics::ARB_TWO_HOP_REJECTED_SPREAD_TOO_LARGE.load(Ordering::Relaxed);
        let insufficient_before =
            ironcrab::metrics::ARB_TWO_HOP_REJECTED_INSUFFICIENT_POOLS.load(Ordering::Relaxed);

        let sol_in_base = 1_000_000_000u64;
        let usdc_in_quote = 65_000_000u64;
        let mut tracker = TokenArbTracker::new(USDC_MINT);
        tracker.token_decimals = Some(6);
        tracker.upsert_pool(sample_pool("orca", "orcaBad", None, None));
        tracker.upsert_pool(sample_pool("meteora_dlmm", "dlmmBad", None, None));

        let mut known_pools = HashSet::new();
        known_pools.insert("orcaBad".to_string());
        known_pools.insert("dlmmBad".to_string());
        let vault_balances = HashMap::from([
            ("orcaBad".to_string(), vault(sol_in_base, usdc_in_quote)),
            ("dlmmBad".to_string(), vault(sol_in_base, usdc_in_quote)),
        ]);

        let forensics = ArbEligibilityForensics::new();
        assert!(
            check_with_forensics(&tracker, &known_pools, &vault_balances, &forensics).is_none()
        );
        assert_eq!(
            ironcrab::metrics::ARB_TWO_HOP_REJECTED_SPREAD_TOO_LARGE.load(Ordering::Relaxed),
            spread_before
        );
        assert!(
            ironcrab::metrics::ARB_TWO_HOP_REJECTED_INSUFFICIENT_POOLS.load(Ordering::Relaxed)
                > insufficient_before
        );
    }

    #[test]
    fn determine_insufficient_subreason_reserve_data_without_trade_mid_is_no_comparable_price() {
        let breakdown = MintEligibilityBreakdown {
            mint: USDC_MINT.to_string(),
            candidate_pools_total: 2,
            known_pools: 2,
            fresh_price: 2,
            has_reserve_data: 2,
            has_trade_mid: 0,
            has_decimals: 2,
            comparable_price_present: 0,
            comparable_price_plausible: 0,
            eligible_pools: 0,
            eligible_dexes: 0,
            eligible_by_dex: HashMap::new(),
            reject_subreason: None,
            pool_rows: vec![],
        };
        assert_eq!(
            determine_insufficient_subreason(&breakdown),
            ArbTwoHopInsufficientSubreason::NoComparablePrice
        );
    }

    #[test]
    fn determine_insufficient_subreason_when_no_comparable_price() {
        let breakdown = MintEligibilityBreakdown {
            mint: USDC_MINT.to_string(),
            candidate_pools_total: 2,
            known_pools: 2,
            fresh_price: 2,
            has_reserve_data: 0,
            has_trade_mid: 0,
            has_decimals: 2,
            comparable_price_present: 0,
            comparable_price_plausible: 0,
            eligible_pools: 0,
            eligible_dexes: 0,
            eligible_by_dex: HashMap::new(),
            reject_subreason: None,
            pool_rows: vec![],
        };
        assert_eq!(
            determine_insufficient_subreason(&breakdown),
            ArbTwoHopInsufficientSubreason::MissingReserves
        );
    }

    #[test]
    fn determine_insufficient_subreason_prefers_not_known_pool_over_only_one_eligible() {
        let breakdown = MintEligibilityBreakdown {
            mint: "TokenMint11111111111111111111111111111111".to_string(),
            candidate_pools_total: 2,
            known_pools: 1,
            fresh_price: 2,
            has_reserve_data: 2,
            has_trade_mid: 0,
            has_decimals: 2,
            comparable_price_present: 1,
            comparable_price_plausible: 1,
            eligible_pools: 1,
            eligible_dexes: 1,
            eligible_by_dex: HashMap::new(),
            reject_subreason: None,
            pool_rows: vec![],
        };
        assert_eq!(
            determine_insufficient_subreason(&breakdown),
            ArbTwoHopInsufficientSubreason::NotKnownPool
        );
    }

    #[test]
    fn eligibility_snapshot_retains_pending_mints_beyond_top_n() {
        let forensics = ArbEligibilityForensics::new();
        let reserves = (1_000_000_000_000u64, 1_000_000_000u64);

        for i in 0..11 {
            let mint = format!("TokenMint{i:032}");
            let pool = format!("pool{i}");
            let mut tracker = TokenArbTracker::new(&mint);
            tracker.token_decimals = Some(6);
            tracker.upsert_pool(sample_pool("orca", &pool, None, None));
            let mut known_pools = HashSet::new();
            known_pools.insert(pool.clone());
            let vault_balances = HashMap::from([(pool, vault(reserves.0, reserves.1))]);
            let _ = check_with_forensics(&tracker, &known_pools, &vault_balances, &forensics);
        }

        assert_eq!(forensics.pending_mint_count(), 11);
        forensics.force_snapshot_ready();
        assert!(forensics.maybe_emit_snapshot());
        assert_eq!(forensics.snapshots_emitted_count(), 1);
        assert_eq!(
            forensics.pending_mint_count(),
            1,
            "only top 10 logged mints should be removed from pending"
        );
    }

    #[test]
    fn check_arbitrage_computes_comparable_price_once_per_pool_side() {
        reset_comparable_price_call_count();
        let reserves = (1_000_000_000_000u64, 1_000_000_000u64);
        let mut tracker = TokenArbTracker::new("TokenMint66666666666666666666666666666666");
        tracker.token_decimals = Some(6);
        tracker.upsert_pool(sample_pool("orca", "poolA", None, None));
        tracker.upsert_pool(sample_pool("pump_amm", "poolB", None, None));

        let mut known_pools = HashSet::new();
        known_pools.insert("poolA".to_string());
        known_pools.insert("poolB".to_string());
        let vault_balances = HashMap::from([
            ("poolA".to_string(), vault(reserves.0, reserves.1)),
            ("poolB".to_string(), vault(reserves.0, reserves.1)),
        ]);

        let forensics = ArbEligibilityForensics::new();
        let _ = check_with_forensics(&tracker, &known_pools, &vault_balances, &forensics);
        assert_eq!(
            comparable_price_call_count(),
            4,
            "buy+sell once per known pool"
        );
    }

    #[test]
    fn eligibility_snapshot_empty_pending_does_not_reset_cooldown() {
        let forensics = ArbEligibilityForensics::new();
        forensics.force_snapshot_ready();

        assert!(!forensics.maybe_emit_snapshot());
        assert_eq!(forensics.snapshots_emitted_count(), 0);

        let reserves = (1_000_000_000_000u64, 1_000_000_000u64);
        let mut tracker = TokenArbTracker::new("TokenMint33333333333333333333333333333333");
        tracker.token_decimals = Some(6);
        tracker.upsert_pool(sample_pool("orca", "poolOnly", None, None));

        let mut known_pools = HashSet::new();
        known_pools.insert("poolOnly".to_string());
        let vault_balances =
            HashMap::from([("poolOnly".to_string(), vault(reserves.0, reserves.1))]);

        let _ = check_with_forensics(&tracker, &known_pools, &vault_balances, &forensics);
        assert!(
            forensics.maybe_emit_snapshot(),
            "empty pending must not advance cooldown; mint should snapshot immediately"
        );
        assert_eq!(forensics.snapshots_emitted_count(), 1);
    }

    #[test]
    fn eligibility_snapshot_rate_limited_to_once_per_60s() {
        let forensics = ArbEligibilityForensics::new();
        let reserves = (1_000_000_000_000u64, 1_000_000_000u64);
        let mut tracker = TokenArbTracker::new("TokenMint55555555555555555555555555555555");
        tracker.token_decimals = Some(6);
        tracker.upsert_pool(sample_pool("orca", "poolOnly", None, None));

        let mut known_pools = HashSet::new();
        known_pools.insert("poolOnly".to_string());
        let vault_balances =
            HashMap::from([("poolOnly".to_string(), vault(reserves.0, reserves.1))]);

        forensics.force_snapshot_ready();
        let _ = check_with_forensics(&tracker, &known_pools, &vault_balances, &forensics);
        assert!(forensics.maybe_emit_snapshot());
        assert_eq!(forensics.snapshots_emitted_count(), 1);

        let _ = check_with_forensics(&tracker, &known_pools, &vault_balances, &forensics);
        assert!(!forensics.maybe_emit_snapshot());
        assert_eq!(forensics.snapshots_emitted_count(), 1);
    }

    #[test]
    fn v2_eligibility_snapshot_rate_limited_with_multiple_pending_mints() {
        let v2_forensics = ArbV2EligibilityForensics::new();
        let reserves = (1_000_000_000_000u64, 1_000_000_000u64);

        for i in 0..2 {
            let mint = format!("TokenMintV2{i:030}");
            let pool = format!("poolV2{i}");
            let mut tracker = TokenArbTracker::new(&mint);
            tracker.token_decimals = Some(6);
            tracker.upsert_pool(sample_pool("orca", &pool, None, None));
            let mut known_pools = HashSet::new();
            known_pools.insert(pool.clone());
            let vault_balances = HashMap::from([(pool, vault(reserves.0, reserves.1))]);
            let _ = check_with_v2_forensics(&tracker, &known_pools, &vault_balances, &v2_forensics);
        }

        assert_eq!(v2_forensics.pending_mint_count(), 2);
        v2_forensics.force_snapshot_ready();
        assert!(v2_forensics.maybe_emit_snapshot());
        assert_eq!(v2_forensics.snapshots_emitted_count(), 1);
        assert_eq!(v2_forensics.pending_mint_count(), 0);

        let mint = "TokenMintV2Extra000000000000000000000";
        let mut tracker = TokenArbTracker::new(mint);
        tracker.token_decimals = Some(6);
        tracker.upsert_pool(sample_pool("orca", "poolV2Extra", None, None));
        let mut known_pools = HashSet::new();
        known_pools.insert("poolV2Extra".to_string());
        let vault_balances =
            HashMap::from([("poolV2Extra".to_string(), vault(reserves.0, reserves.1))]);
        let _ = check_with_v2_forensics(&tracker, &known_pools, &vault_balances, &v2_forensics);
        assert!(!v2_forensics.maybe_emit_snapshot());
        assert_eq!(v2_forensics.snapshots_emitted_count(), 1);
    }

    #[test]
    fn check_arbitrage_v2_increments_multi_dex_screen_counter() {
        let before_multi =
            ironcrab::metrics::ARB_TWO_HOP_V2_SCREEN_MULTI_DEX_TOTAL.load(Ordering::Relaxed);
        let cache = create_shared_cache();
        let token_mint = Pubkey::new_unique();
        let pool_a = Pubkey::new_unique();
        let pool_b = Pubkey::new_unique();
        let mint_str = token_mint.to_string();

        let update_orca = PoolCacheUpdate::new_balance_updated(
            TEST_COMPONENT,
            TEST_BUILD,
            TEST_RUN,
            pool_a.to_string(),
            "orca".to_string(),
            mint_str.clone(),
            NATIVE_SOL_MINT.to_string(),
            1_000_000_000_000,
            980_000_000,
            1,
        );
        let update_pump = PoolCacheUpdate::new_balance_updated(
            TEST_COMPONENT,
            TEST_BUILD,
            TEST_RUN,
            pool_b.to_string(),
            "pump_amm".to_string(),
            mint_str.clone(),
            NATIVE_SOL_MINT.to_string(),
            1_000_000_000_000,
            1_020_000_000,
            2,
        );
        ironcrab::execution::pool_cache_sync::apply_pool_cache_update(&cache, &update_orca);
        ironcrab::execution::pool_cache_sync::apply_pool_cache_update(&cache, &update_pump);

        let mut trackers = HashMap::new();
        let mut vault_balances = HashMap::new();
        seed_token_tracker_from_live_pool_cache(
            &mint_str,
            &cache,
            &mut trackers,
            &mut vault_balances,
            None,
        );

        let mut known_pools = HashSet::new();
        known_pools.insert(pool_a.to_string());
        known_pools.insert(pool_b.to_string());

        let tracker = trackers.get_mut(&mint_str).unwrap();
        tracker.token_decimals = Some(6);
        assert_eq!(tracker.pool_count_on_distinct_dexes(), 2);

        let config = with_small_v2_probe(ArbConfig {
            arb_two_hop_v2_enabled: true,
            min_spread_bps: 1,
            min_profit_lamports: 1,
            est_tx_cost_lamports: 1,
            ..Default::default()
        });
        let spread_warn_last = RwLock::new(HashMap::new());
        let data_quality_rejects = AtomicU64::new(0);
        let _ = tracker.check_arbitrage(
            &config,
            &known_pools,
            &vault_balances,
            &HashMap::new(),
            &ArbCheckContext {
                spread_warn_last: &spread_warn_last,
                data_quality_rejects: &data_quality_rejects,
                forensics: None,
                v2_forensics: None,
                selected_mints: None,
                pinned_pools: None,
            },
        );
        assert!(
            ironcrab::metrics::ARB_TWO_HOP_V2_SCREEN_MULTI_DEX_TOTAL.load(Ordering::Relaxed)
                > before_multi
        );
    }

    #[test]
    fn v2_screen_skipped_when_mint_not_in_selected_set() {
        let before_skip = ironcrab::metrics::ARB_TWO_HOP_V2_SCREEN_SKIPPED_MINT_NOT_SELECTED
            .load(Ordering::Relaxed);
        let before_screen = ironcrab::metrics::ARB_TWO_HOP_V2_SCREEN_TOTAL.load(Ordering::Relaxed);

        let mint = "SelectedGateMint11111111111111111111111";
        let mut tracker = TokenArbTracker::new(mint);
        tracker.token_decimals = Some(6);
        tracker.upsert_pool(sample_pool("orca", "poolA", None, None));

        let mut known_pools = HashSet::new();
        known_pools.insert("poolA".to_string());
        let spread_warn_last = RwLock::new(HashMap::new());
        let data_quality_rejects = AtomicU64::new(0);
        let selected_mints = HashSet::new();
        let pinned_pools = HashSet::new();
        let config = with_small_v2_probe(ArbConfig {
            arb_two_hop_v2_enabled: true,
            ..Default::default()
        });

        let _ = tracker.check_arbitrage(
            &config,
            &known_pools,
            &HashMap::new(),
            &HashMap::new(),
            &ArbCheckContext {
                spread_warn_last: &spread_warn_last,
                data_quality_rejects: &data_quality_rejects,
                forensics: None,
                v2_forensics: None,
                selected_mints: Some(&selected_mints),
                pinned_pools: Some(&pinned_pools),
            },
        );

        assert_eq!(
            ironcrab::metrics::ARB_TWO_HOP_V2_SCREEN_SKIPPED_MINT_NOT_SELECTED
                .load(Ordering::Relaxed),
            before_skip + 1
        );
        assert_eq!(
            ironcrab::metrics::ARB_TWO_HOP_V2_SCREEN_TOTAL.load(Ordering::Relaxed),
            before_screen
        );
    }

    #[test]
    fn v2_round_trip_candidates_use_pinned_pools_only() {
        let before_formable =
            ironcrab::metrics::ARB_TWO_HOP_V2_ROUND_TRIP_FORMABLE_TOTAL.load(Ordering::Relaxed);
        let reserves = (1_000_000_000_000u64, 1_000_000_000u64);
        let mint = "PinnedOnlyMint1111111111111111111111111";
        let mut tracker = TokenArbTracker::new(mint);
        tracker.token_decimals = Some(6);
        tracker.upsert_pool(sample_pool("orca", "orca_pool", None, None));
        tracker.upsert_pool(sample_pool("pump_amm", "pump_pool", None, None));
        tracker.upsert_pool(sample_pool("raydium", "ray_pool", None, None));

        let mut known_pools = HashSet::new();
        known_pools.insert("orca_pool".to_string());
        known_pools.insert("pump_pool".to_string());
        known_pools.insert("ray_pool".to_string());

        let vault_balances = HashMap::from([
            ("orca_pool".to_string(), vault(reserves.0, reserves.1)),
            ("pump_pool".to_string(), vault(reserves.0, reserves.1)),
            ("ray_pool".to_string(), vault(reserves.0, reserves.1)),
        ]);

        let mut selected_mints = HashSet::new();
        selected_mints.insert(mint.to_string());
        let mut pinned_pools = HashSet::new();
        pinned_pools.insert("orca_pool".to_string());
        pinned_pools.insert("pump_pool".to_string());

        let spread_warn_last = RwLock::new(HashMap::new());
        let data_quality_rejects = AtomicU64::new(0);
        let config = with_small_v2_probe(ArbConfig {
            arb_two_hop_v2_enabled: true,
            min_spread_bps: 1,
            min_profit_lamports: 1,
            est_tx_cost_lamports: 1,
            ..Default::default()
        });

        let _ = tracker.check_arbitrage(
            &config,
            &known_pools,
            &vault_balances,
            &HashMap::new(),
            &ArbCheckContext {
                spread_warn_last: &spread_warn_last,
                data_quality_rejects: &data_quality_rejects,
                forensics: None,
                v2_forensics: None,
                selected_mints: Some(&selected_mints),
                pinned_pools: Some(&pinned_pools),
            },
        );

        assert!(
            ironcrab::metrics::ARB_TWO_HOP_V2_ROUND_TRIP_FORMABLE_TOTAL.load(Ordering::Relaxed)
                > before_formable,
            "pinned orca+pump pair should form a round trip even with extra non-pinned ray pool in tracker"
        );
    }

    #[test]
    fn arb_track_reconcile_and_proactive_use_same_selection() {
        let mint = TrackMintInput {
            mint: "MintShared111111111111111111111111111".to_string(),
            pools: vec![
                TrackPoolInput {
                    pool_address: "orca_pool".to_string(),
                    dex: "orca".to_string(),
                    known: true,
                    quote_pool: QuotePoolInput {
                        pool_address: "orca_pool".to_string(),
                        dex: "orca".to_string(),
                        token_mint: "MintShared111111111111111111111111111".to_string(),
                        trade_price_buy: None,
                        trade_price_sell: None,
                        trade_updated_at: Instant::now(),
                        has_reserve_data: true,
                        token_decimals: 6,
                    },
                    vault: Some(QuoteVaultInput {
                        reserve_base: 1_000_000_000_000,
                        reserve_quote: 1_000_000_000,
                        update_slot: 1,
                        updated_at: Instant::now(),
                        active_id: None,
                        bin_step: None,
                        dlmm_sol_is_x: false,
                        dlmm_token_x_mint: None,
                    }),
                    dlmm_bins: None,
                    token_decimals: 6,
                    last_activity_unix_ms: 1,
                },
                TrackPoolInput {
                    pool_address: "pump_pool".to_string(),
                    dex: "pump_amm".to_string(),
                    known: true,
                    quote_pool: QuotePoolInput {
                        pool_address: "pump_pool".to_string(),
                        dex: "pump_amm".to_string(),
                        token_mint: "MintShared111111111111111111111111111".to_string(),
                        trade_price_buy: None,
                        trade_price_sell: None,
                        trade_updated_at: Instant::now(),
                        has_reserve_data: true,
                        token_decimals: 6,
                    },
                    vault: Some(QuoteVaultInput {
                        reserve_base: 1_000_000_000_000,
                        reserve_quote: 2_000_000_000,
                        update_slot: 1,
                        updated_at: Instant::now(),
                        active_id: None,
                        bin_step: None,
                        dlmm_sol_is_x: false,
                        dlmm_token_x_mint: None,
                    }),
                    dlmm_bins: None,
                    token_decimals: 6,
                    last_activity_unix_ms: 2,
                },
            ],
            trade_signal_pools: None,
            last_activity_unix_ms: 2,
        };
        let config = TrackSelectionConfig {
            max_pools: ARB_TRACK_BASELINE_MAX_POOLS_DEFAULT,
            max_pools_per_mint: 3,
            probe_lamports: 10_000_000,
            freshness: QuoteFreshnessConfig::default(),
        };
        let proactive = select_arb_track_pools(std::slice::from_ref(&mint), &config);
        let reconcile = select_arb_track_pools(std::slice::from_ref(&mint), &config);
        let proactive_pools: HashSet<_> =
            proactive.selected.iter().map(|p| p.pool.clone()).collect();
        let reconcile_pools: HashSet<_> =
            reconcile.selected.iter().map(|p| p.pool.clone()).collect();
        assert_eq!(proactive_pools, reconcile_pools);
    }

    #[test]
    fn arb_track_selection_coalescer_bounds_burst_to_one_batch() {
        let mut coalescer = ArbTrackSelectionCoalescer::default();
        for i in 0..100 {
            coalescer.ingest_dirty(format!("mint_{i}"));
        }
        let (dirty, overflow) = coalescer.take_batch();
        assert!(!overflow);
        assert_eq!(dirty.len(), 100);
        let (dirty2, overflow2) = coalescer.take_batch();
        assert!(dirty2.is_empty());
        assert!(!overflow2);
    }

    #[test]
    fn arb_track_selection_coalescer_dirty_overflow_requests_full_reconcile() {
        let mut coalescer = ArbTrackSelectionCoalescer::default();
        for i in 0..ARB_TRACK_SELECTION_DIRTY_MINTS_CAP {
            assert!(!coalescer.ingest_dirty(format!("mint_{i}")));
        }
        assert!(coalescer.ingest_dirty("overflow_mint".to_string()));
        let (dirty, overflow) = coalescer.take_batch();
        assert!(
            overflow,
            "overflow must schedule authoritative full reconcile"
        );
        assert_eq!(dirty.len(), ARB_TRACK_SELECTION_DIRTY_MINTS_CAP);
        assert!(
            !dirty.iter().any(|m| m == "overflow_mint"),
            "overflow mint is not retained; full reconcile recovers from tracker truth"
        );
    }

    #[test]
    fn arb_track_selection_coalescer_burst_never_exceeds_dirty_cap() {
        let mut coalescer = ArbTrackSelectionCoalescer::default();
        let cap = ARB_TRACK_SELECTION_DIRTY_MINTS_CAP;
        let mut overflow_seen = false;
        for i in 0..cap + 5_000 {
            if coalescer.ingest_dirty(format!("burst_{i}")) {
                overflow_seen = true;
            }
        }
        assert!(overflow_seen);
        let (dirty, overflow) = coalescer.take_batch();
        assert!(overflow);
        assert_eq!(dirty.len(), cap);
    }

    #[test]
    fn arb_track_selection_coalescer_duplicate_dirty_at_cap_is_idempotent() {
        let mut coalescer = ArbTrackSelectionCoalescer::default();
        let cap = ARB_TRACK_SELECTION_DIRTY_MINTS_CAP;
        for i in 0..cap {
            assert!(!coalescer.ingest_dirty(format!("mint_{i}")));
        }
        assert!(!coalescer.ingest_dirty("mint_0".to_string()));
        let (dirty, overflow) = coalescer.take_batch();
        assert!(!overflow);
        assert_eq!(dirty.len(), cap);
    }

    #[test]
    fn resolve_arb_track_batch_plan_defers_full_with_dirty_as_incremental() {
        let min = Duration::from_secs(5);
        let plan = resolve_arb_track_batch_plan(true, true, Duration::from_millis(100), min);
        assert_eq!(
            plan,
            ArbTrackBatchPlan::Incremental {
                keep_pending_full: true
            }
        );
    }

    #[test]
    fn resolve_arb_track_batch_plan_keeps_full_pending_until_interval_elapsed() {
        let min = Duration::from_secs(5);
        let deferred = resolve_arb_track_batch_plan(true, true, Duration::from_secs(1), min);
        assert_eq!(
            deferred,
            ArbTrackBatchPlan::Incremental {
                keep_pending_full: true
            }
        );
        let eligible = resolve_arb_track_batch_plan(true, true, Duration::from_secs(5), min);
        assert_eq!(eligible, ArbTrackBatchPlan::FullReconcile);
    }

    #[test]
    fn resolve_arb_track_batch_plan_defers_full_without_dirty() {
        let plan = resolve_arb_track_batch_plan(
            true,
            false,
            Duration::from_millis(0),
            Duration::from_secs(5),
        );
        assert_eq!(plan, ArbTrackBatchPlan::DeferFullReconcile);
    }

    #[test]
    fn resolve_arb_track_batch_plan_incremental_without_pending_full_when_not_deferred() {
        let plan = resolve_arb_track_batch_plan(
            false,
            true,
            Duration::from_secs(0),
            Duration::from_secs(5),
        );
        assert_eq!(
            plan,
            ArbTrackBatchPlan::Incremental {
                keep_pending_full: false
            }
        );
    }

    #[test]
    fn arb_track_selection_ingress_deduplicates_burst() {
        let (handle, mut wake_rx) = test_arb_track_selection_handle();
        for _ in 0..1_000 {
            handle.mark_dirty("mint_a");
        }
        assert_eq!(handle.ingress_dirty_len(), 1);
        assert!(wake_rx.try_recv().is_ok());
        assert!(wake_rx.try_recv().is_err());
    }

    #[test]
    fn arb_track_selection_ingress_hard_cap_sets_overflow() {
        let (handle, _wake_rx) = test_arb_track_selection_handle();
        let cap = ARB_TRACK_SELECTION_INGRESS_DIRTY_CAP;
        for i in 0..cap {
            handle.mark_dirty(&format!("mint_{i}"));
        }
        assert_eq!(handle.ingress_dirty_len(), cap);
        handle.mark_dirty("overflow_mint");
        assert_eq!(handle.ingress_dirty_len(), cap);
        assert!(handle.pending_full_reconcile.load(Ordering::Acquire));
        let (_, overflow) = handle.drain_ingress_dirty();
        assert!(overflow);
    }

    #[test]
    fn arb_track_selection_request_full_reconcile_sets_pending() {
        let (handle, mut wake_rx) = test_arb_track_selection_handle();
        handle.request_full_reconcile();
        assert!(handle.pending_full_reconcile.load(Ordering::Acquire));
        assert!(wake_rx.try_recv().is_ok());
    }

    #[test]
    fn arb_track_mint_snapshot_cache_respects_cap() {
        let mut cache = ArbTrackMintSnapshotCache::default();
        let protected = HashSet::new();
        for i in 0..ARB_TRACK_MINT_SNAPSHOTS_CAP + 100 {
            let mint = format!("mint_{i:05}");
            let input = TrackMintInput {
                mint: mint.clone(),
                pools: Vec::new(),
                trade_signal_pools: None,
                last_activity_unix_ms: i as u64,
            };
            let _ = cache.insert_bounded(mint, input, &protected);
            assert!(cache.len() <= ARB_TRACK_MINT_SNAPSHOTS_CAP);
        }
    }

    #[test]
    fn arb_track_mint_snapshot_cache_never_exceeds_cap_when_incoming_protected() {
        let mut cache = ArbTrackMintSnapshotCache::default();
        let empty = HashSet::new();
        for i in 0..ARB_TRACK_MINT_SNAPSHOTS_CAP {
            let mint = format!("seed_{i:05}");
            let input = TrackMintInput {
                mint: mint.clone(),
                pools: Vec::new(),
                trade_signal_pools: None,
                last_activity_unix_ms: i as u64,
            };
            assert!(cache.insert_bounded(mint, input, &empty));
        }
        assert_eq!(cache.len(), ARB_TRACK_MINT_SNAPSHOTS_CAP);

        let mut all_incoming_protected: HashSet<String> = HashSet::new();
        for i in 0..512 {
            all_incoming_protected.insert(format!("incoming_{i:05}"));
        }
        for i in 0..512 {
            let mint = format!("incoming_{i:05}");
            let input = TrackMintInput {
                mint: mint.clone(),
                pools: Vec::new(),
                trade_signal_pools: None,
                last_activity_unix_ms: 10_000 + i as u64,
            };
            let _ = cache.insert_bounded(mint, input, &all_incoming_protected);
            assert!(
                cache.len() <= ARB_TRACK_MINT_SNAPSHOTS_CAP,
                "protected incoming must not bypass hard cap"
            );
        }
    }

    #[test]
    fn compute_snapshot_admit_set_is_deterministic_top_k() {
        let ranked: Vec<String> = (0..20).map(|i| format!("mint_{i:02}")).collect();
        let mut protected = HashSet::new();
        protected.insert("mint_19".to_string());
        protected.insert("mint_00".to_string());

        let admit_a = compute_snapshot_admit_set(&ranked, &protected, 5);
        let admit_b = compute_snapshot_admit_set(&ranked, &protected, 5);
        assert_eq!(admit_a, admit_b);
        assert_eq!(admit_a.len(), 5);
        assert!(admit_a.contains("mint_00"));
        assert!(admit_a.contains("mint_19"));
        assert!(admit_a.contains("mint_01"));
        assert!(admit_a.contains("mint_02"));
        assert!(admit_a.contains("mint_03"));
    }

    #[test]
    fn prune_arb_track_stale_pools_only_schedules_full_reconcile() {
        let cache = create_shared_cache();
        let ctx = Arc::new(test_arb_context(cache));
        let stale_pool = "stale_pool_addr";
        ctx.arb_pinned_pools.write().insert(stale_pool.to_string());
        assert!(!ctx
            .arb_track_selection
            .pending_full_reconcile
            .load(Ordering::Acquire));
        ctx.prune_arb_track_stale_pools();
        assert!(
            ctx.arb_track_selection
                .pending_full_reconcile
                .load(Ordering::Acquire),
            "heartbeat prune must arm authoritative full reconcile only"
        );
        assert!(
            ctx.arb_pinned_pools.read().contains(stale_pool),
            "heartbeat prune must not mutate pinned set directly"
        );
    }

    #[test]
    fn full_reconcile_clears_stale_pinned_pool_via_authoritative_selection() {
        let cache = create_shared_cache();
        let ctx = Arc::new(test_arb_context(cache));
        let stale_mint = "StalePinnedMint111111111111111111111111";
        let stale_pool = "stale_pool_addr";
        {
            let mut snapshots = ctx.arb_track_mint_snapshots.write();
            snapshots.insert_bounded(
                stale_mint.to_string(),
                TrackMintInput {
                    mint: stale_mint.to_string(),
                    pools: vec![
                        TrackPoolInput {
                            pool_address: stale_pool.to_string(),
                            dex: "orca".to_string(),
                            known: true,
                            quote_pool: QuotePoolInput {
                                pool_address: stale_pool.to_string(),
                                dex: "orca".to_string(),
                                token_mint: stale_mint.to_string(),
                                trade_price_buy: None,
                                trade_price_sell: None,
                                trade_updated_at: Instant::now(),
                                has_reserve_data: true,
                                token_decimals: 6,
                            },
                            vault: None,
                            dlmm_bins: None,
                            token_decimals: 6,
                            last_activity_unix_ms: 1,
                        },
                        TrackPoolInput {
                            pool_address: "other_pool".to_string(),
                            dex: "pump_amm".to_string(),
                            known: true,
                            quote_pool: QuotePoolInput {
                                pool_address: "other_pool".to_string(),
                                dex: "pump_amm".to_string(),
                                token_mint: stale_mint.to_string(),
                                trade_price_buy: None,
                                trade_price_sell: None,
                                trade_updated_at: Instant::now(),
                                has_reserve_data: true,
                                token_decimals: 6,
                            },
                            vault: None,
                            dlmm_bins: None,
                            token_decimals: 6,
                            last_activity_unix_ms: 2,
                        },
                    ],
                    trade_signal_pools: None,
                    last_activity_unix_ms: 2,
                },
                &HashSet::new(),
            );
        }
        ctx.arb_pinned_pools.write().insert(stale_pool.to_string());
        run_arb_track_selection_batch(&ctx, Vec::new(), true);
        assert!(
            !ctx.arb_pinned_pools.read().contains(stale_pool),
            "authoritative full reconcile must clear stale pins"
        );
        assert!(
            !ctx.arb_track_mint_snapshots
                .read()
                .entries
                .contains_key(stale_mint),
            "stale snapshot must be rebuilt/removed from tracker truth"
        );
    }

    #[test]
    fn admit_refresh_order_includes_unranked_protected_mints() {
        let ranked = vec!["mint_a".to_string(), "mint_b".to_string()];
        let mut admit = HashSet::new();
        admit.insert("mint_a".to_string());
        admit.insert("stale_pinned".to_string());
        let order = admit_refresh_order(&ranked, &admit);
        assert_eq!(order, vec!["mint_a", "stale_pinned"]);
    }

    #[test]
    fn full_reconcile_refresh_removes_stale_unranked_protected_snapshot() {
        let cache = create_shared_cache();
        let ctx = Arc::new(test_arb_context(cache));
        let stale_mint = "StalePinnedMint111111111111111111111111";
        let stale_pool = "stale_pool_addr";
        {
            let mut snapshots = ctx.arb_track_mint_snapshots.write();
            snapshots.insert_bounded(
                stale_mint.to_string(),
                TrackMintInput {
                    mint: stale_mint.to_string(),
                    pools: vec![TrackPoolInput {
                        pool_address: stale_pool.to_string(),
                        dex: "orca".to_string(),
                        known: true,
                        quote_pool: QuotePoolInput {
                            pool_address: stale_pool.to_string(),
                            dex: "orca".to_string(),
                            token_mint: stale_mint.to_string(),
                            trade_price_buy: None,
                            trade_price_sell: None,
                            trade_updated_at: Instant::now(),
                            has_reserve_data: true,
                            token_decimals: 6,
                        },
                        vault: None,
                        dlmm_bins: None,
                        token_decimals: 6,
                        last_activity_unix_ms: 1,
                    }],
                    trade_signal_pools: None,
                    last_activity_unix_ms: 1,
                },
                &HashSet::new(),
            );
        }
        ctx.arb_pinned_pools.write().insert(stale_pool.to_string());
        let ranked = vec!["active_mint".to_string()];
        let protected = ctx.mandatory_protected_snapshot_mints();
        assert!(protected.contains(stale_mint));
        let admit = compute_snapshot_admit_set(&ranked, &protected, ARB_TRACK_MINT_SNAPSHOTS_CAP);
        let refresh_order = admit_refresh_order(&ranked, &admit);
        assert!(
            refresh_order.iter().any(|m| m == stale_mint),
            "unranked protected mint must be refreshed during full reconcile"
        );
        for mint in &refresh_order {
            ctx.refresh_mint_snapshot(mint, &protected);
        }
        assert!(
            !ctx.arb_track_mint_snapshots
                .read()
                .entries
                .contains_key(stale_mint),
            "stale protected snapshot must be removed when tracker truth is absent"
        );
    }

    #[test]
    fn admit_refresh_order_preserves_rank_not_hashset_iteration() {
        let ranked: Vec<String> = (0..8).map(|i| format!("mint_{i}")).collect();
        let admit: HashSet<String> = ranked.iter().take(5).cloned().collect();
        let ordered = admit_refresh_order(&ranked, &admit);
        assert_eq!(ordered, ranked[..5]);
    }

    #[test]
    fn full_admit_refresh_produces_identical_generations_and_eviction_victim() {
        let ranked: Vec<String> = (0..8).map(|i| format!("mint_{i}")).collect();
        let admit = compute_snapshot_admit_set(&ranked, &HashSet::new(), 5);
        let refresh = admit_refresh_order(&ranked, &admit);

        let run = || -> (Vec<u64>, Option<String>) {
            let mut cache = ArbTrackMintSnapshotCache::default();
            let protected = HashSet::new();
            for mint in &refresh {
                let input = TrackMintInput {
                    mint: mint.clone(),
                    pools: Vec::new(),
                    trade_signal_pools: None,
                    last_activity_unix_ms: 0,
                };
                assert!(cache.insert_bounded(mint.clone(), input, &protected));
            }
            let gens = cache.test_access_generations_in_order(&refresh);
            let victim = cache.test_eviction_victim(&protected);
            (gens, victim)
        };

        let (gens_a, victim_a) = run();
        let (gens_b, victim_b) = run();
        assert_eq!(
            gens_a, gens_b,
            "rank-ordered refresh must assign identical generations"
        );
        assert_eq!(
            victim_a, victim_b,
            "rank-ordered refresh must pick identical eviction victim"
        );
        assert_eq!(victim_a.as_deref(), Some("mint_0"));
    }

    #[test]
    fn snapshot_cache_hot_touch_keeps_heap_bounded() {
        let mut cache = ArbTrackMintSnapshotCache::default();
        let protected = HashSet::new();
        for i in 0..ARB_TRACK_MINT_SNAPSHOTS_CAP {
            let mint = format!("mint_{i:05}");
            let input = TrackMintInput {
                mint: mint.clone(),
                pools: Vec::new(),
                trade_signal_pools: None,
                last_activity_unix_ms: i as u64,
            };
            cache.insert_bounded(mint, input, &protected);
        }
        let hot_mint = "mint_00000".to_string();
        let hot_input = TrackMintInput {
            mint: hot_mint.clone(),
            pools: Vec::new(),
            trade_signal_pools: None,
            last_activity_unix_ms: u64::MAX,
        };
        for gen in 0..10_000 {
            let input = TrackMintInput {
                last_activity_unix_ms: gen,
                ..hot_input.clone()
            };
            cache.insert_bounded(hot_mint.clone(), input, &protected);
        }
        let heap_bound = ARB_TRACK_MINT_SNAPSHOTS_CAP
            .saturating_mul(ArbTrackMintSnapshotCache::HEAP_COMPACT_FACTOR)
            .max(ArbTrackMintSnapshotCache::HEAP_COMPACT_MIN_EXTRA);
        assert!(
            cache.heap_len() <= heap_bound,
            "heap must compact and stay bounded after hot touches"
        );
        assert_eq!(cache.len(), ARB_TRACK_MINT_SNAPSHOTS_CAP);
    }

    #[test]
    fn arb_track_selection_blocking_join_failed_sets_pending_full() {
        use ironcrab::metrics::ARB_TRACK_SELECTION_BLOCKING_JOIN_FAILED_TOTAL;

        let (handle, _wake_rx) = test_arb_track_selection_handle();
        let before = ARB_TRACK_SELECTION_BLOCKING_JOIN_FAILED_TOTAL.load(Ordering::Relaxed);
        handle.record_blocking_join_failed();
        assert!(handle.pending_full_reconcile.load(Ordering::Acquire));
        assert_eq!(
            ARB_TRACK_SELECTION_BLOCKING_JOIN_FAILED_TOTAL.load(Ordering::Relaxed),
            before + 1
        );
    }

    #[test]
    fn reconcile_first_publish_records_newly_active_mints() {
        use ironcrab::metrics::{
            try_record_arb_track_pin_before_first_screen_ms,
            ARB_TRACK_PIN_BEFORE_FIRST_SCREEN_MS_COUNT,
        };

        let cache = create_shared_cache();
        let ctx = Arc::new(test_arb_context(cache));
        let mint = "MintReconcileFirst111111111111111111111111";
        let pool_a = "pool_reconcile_a";
        let pool_b = "pool_reconcile_b";
        {
            let mut snapshots = ctx.arb_track_mint_snapshots.write();
            snapshots.insert_bounded(
                mint.to_string(),
                TrackMintInput {
                    mint: mint.to_string(),
                    pools: vec![
                        TrackPoolInput {
                            pool_address: pool_a.to_string(),
                            dex: "orca".to_string(),
                            known: true,
                            quote_pool: QuotePoolInput {
                                pool_address: pool_a.to_string(),
                                dex: "orca".to_string(),
                                token_mint: mint.to_string(),
                                trade_price_buy: None,
                                trade_price_sell: None,
                                trade_updated_at: Instant::now(),
                                has_reserve_data: true,
                                token_decimals: 6,
                            },
                            vault: Some(QuoteVaultInput {
                                reserve_base: 1_000_000_000,
                                reserve_quote: 2_000_000_000,
                                update_slot: 1,
                                updated_at: Instant::now(),
                                active_id: None,
                                bin_step: None,
                                dlmm_sol_is_x: false,
                                dlmm_token_x_mint: None,
                            }),
                            dlmm_bins: None,
                            token_decimals: 6,
                            last_activity_unix_ms: 1,
                        },
                        TrackPoolInput {
                            pool_address: pool_b.to_string(),
                            dex: "pump_amm".to_string(),
                            known: true,
                            quote_pool: QuotePoolInput {
                                pool_address: pool_b.to_string(),
                                dex: "pump_amm".to_string(),
                                token_mint: mint.to_string(),
                                trade_price_buy: None,
                                trade_price_sell: None,
                                trade_updated_at: Instant::now(),
                                has_reserve_data: true,
                                token_decimals: 6,
                            },
                            vault: Some(QuoteVaultInput {
                                reserve_base: 1_000_000_000,
                                reserve_quote: 2_000_000_000,
                                update_slot: 1,
                                updated_at: Instant::now(),
                                active_id: None,
                                bin_step: None,
                                dlmm_sol_is_x: false,
                                dlmm_token_x_mint: None,
                            }),
                            dlmm_bins: None,
                            token_decimals: 6,
                            last_activity_unix_ms: 2,
                        },
                    ],
                    trade_signal_pools: None,
                    last_activity_unix_ms: 2,
                },
                &HashSet::new(),
            );
        }
        let before = ARB_TRACK_PIN_BEFORE_FIRST_SCREEN_MS_COUNT.load(Ordering::Relaxed);
        ctx.run_arb_track_selection_from_snapshots(true);
        try_record_arb_track_pin_before_first_screen_ms(mint);
        let after = ARB_TRACK_PIN_BEFORE_FIRST_SCREEN_MS_COUNT.load(Ordering::Relaxed);
        assert_eq!(
            after,
            before + 1,
            "reconcile publish must seed first-publish timing for newly active mints"
        );
    }

    #[test]
    fn arb_trade_signal_pair_preserves_actual_buy_sell_mapping() {
        let cache = create_shared_cache();
        let ctx = Arc::new(test_arb_context(cache));
        ctx.publish_arb_trade_signal_track_pins("mintA", "buy_pool", "sell_pool");
        let pair = ctx
            .arb_trade_signal_pairs
            .read()
            .get("mintA")
            .cloned()
            .unwrap();
        assert_eq!(pair.buy_pool, "buy_pool");
        assert_eq!(pair.sell_pool, "sell_pool");
    }

    #[test]
    fn arb_trade_signal_pairs_evict_oldest_not_lexicographic() {
        let cache = create_shared_cache();
        let ctx = Arc::new(test_arb_context(cache));
        for i in 0..=ARB_TRADE_SIGNAL_PAIRS_CAP {
            ctx.publish_arb_trade_signal_track_pins(
                &format!("mint_{i:03}"),
                &format!("buy_{i}"),
                &format!("sell_{i}"),
            );
        }
        let pairs = ctx.arb_trade_signal_pairs.read();
        assert_eq!(pairs.len(), ARB_TRADE_SIGNAL_PAIRS_CAP);
        assert!(!pairs.contains_key("mint_000"));
        assert!(pairs.contains_key(&format!("mint_{:03}", ARB_TRADE_SIGNAL_PAIRS_CAP)));
    }

    #[test]
    fn phase3_arb_track_requests_publish_serializes_reconcile_flag() {
        let update = ArbTrackRequestsUpdate {
            version: ARB_TRACK_REQUESTS_WIRE_VERSION,
            ts_unix_ms: 1_700_000_000,
            active: vec![ArbTrackActiveEntry {
                pool: "Pool111111111111111111111111111111111111111".to_string(),
                reason: ArbTrackActiveReason::Baseline,
                readiness: ArbTrackReadiness::Warmable,
            }],
            removed: vec![],
            reconcile: true,
        };
        let json = serde_json::to_string(&update).expect("serialize");
        assert!(json.contains("\"reconcile\":true"));
        let back: ArbTrackRequestsUpdate = serde_json::from_str(&json).expect("deserialize");
        assert!(back.reconcile);
        assert_eq!(back.active.len(), 1);
    }

    #[test]
    fn v2_insufficient_log_category_is_fixed_per_subreason_and_detail() {
        let no_fresh = RoundTripInsufficient::new(RoundTripInsufficientSubreason::NoFreshBuyQuote);
        assert_eq!(
            v2_insufficient_log_category(&no_fresh),
            V2InsufficientLogCategory::NoFreshBuyQuote
        );
        let cross_dex = RoundTripInsufficient {
            subreason: RoundTripInsufficientSubreason::NoCrossDexSell,
            no_cross_dex_sell_detail: Some(NoCrossDexSellDetailReason::SellMissingVault),
            sell_quote_none_detail_counts: None,
            sell_not_fresh_detail_counts: None,
            no_fresh_buy_quote_detail: None,
            state_stale_age_bucket_counts: None,
        };
        assert_eq!(
            v2_insufficient_log_category(&cross_dex),
            V2InsufficientLogCategory::NoCrossDexSellMissingVault
        );
    }

    #[test]
    fn v2_insufficient_log_path_has_no_dynamic_string_keys() {
        let src = include_str!("arb_strategy.rs");
        let start = src
            .find("fn log_v2_round_trip_insufficient_pools(")
            .expect("log_v2_round_trip_insufficient_pools");
        let end = src[start..]
            .find("fn log_v2_cross_dex_pair_failures_debug_sample")
            .expect("after log_v2_round_trip_insufficient_pools");
        let fn_body = &src[start..start + end];
        assert!(
            !fn_body.contains("format!("),
            "throttle decision must not allocate dynamic string keys"
        );
    }

    /// C1h5 v2: stale sell-leg schedules recovery; after vault refresh sell becomes fresh.
    #[test]
    fn v2_sell_leg_recovery_fresh_after_pin_without_full_opp() {
        use ironcrab::metrics::ARB_V2_SCREEN_SELL_STALE_THEN_FRESH_AFTER_PIN_TOTAL;

        let reserves = (1_000_000_000_000u64, 1_000_000_000u64);
        let mint = "TokenMintRecovery1111111111111111111111";
        let buy_pool = "dlmmBuyRecovery";
        let sell_pool = "pumpSellRecovery";

        let ctx = Arc::new(test_arb_context(create_shared_cache()));
        {
            let mut config = ctx.config.write();
            config.arb_two_hop_v2_enabled = true;
            config.two_hop_enabled = true;
            config.arb_probe_lamports = DLMM_PROBE_SOL_LAMPORTS;
            config.arb_probe_follows_max_position = false;
            config.arb_quote_state_ttl_ms = 120_000;
        }
        ctx.known_pools.write().insert(buy_pool.to_string());
        ctx.known_pools.write().insert(sell_pool.to_string());
        ctx.arb_selected_mints.write().insert(mint.to_string());
        ctx.arb_pinned_pools.write().insert(buy_pool.to_string());
        ctx.arb_pinned_pools.write().insert(sell_pool.to_string());

        let mut tracker = TokenArbTracker::new(mint);
        tracker.token_decimals = Some(6);
        tracker.upsert_pool(sample_pool("orca", buy_pool, None, None));
        tracker.upsert_pool(sample_pool("pump_amm", sell_pool, None, None));
        ctx.trackers.write().insert(mint.to_string(), tracker);

        let mut stale_sell_vault = sample_vault(reserves.0, reserves.1, None, None, false, None);
        stale_sell_vault.updated_at = Instant::now() - Duration::from_secs(300);
        ctx.vault_balances.write().insert(
            buy_pool.to_string(),
            sample_vault(reserves.0, reserves.1, None, None, false, None),
        );
        ctx.vault_balances
            .write()
            .insert(sell_pool.to_string(), stale_sell_vault);

        let config = ctx.config.read().clone();
        let tracker_snapshot = ctx.trackers.read().get(mint).unwrap().clone();
        ctx.try_schedule_v2_sell_leg_recovery(&tracker_snapshot, &config);
        assert!(
            ctx.v2_sell_stale_recovery_pending.read().contains_key(mint),
            "stale sell-leg must schedule recovery"
        );

        let fresh_before =
            ARB_V2_SCREEN_SELL_STALE_THEN_FRESH_AFTER_PIN_TOTAL.load(Ordering::Relaxed);
        ctx.vault_balances.write().insert(
            sell_pool.to_string(),
            sample_vault(reserves.0, reserves.1, None, None, false, None),
        );
        let tracker_snapshot = ctx.trackers.read().get(mint).unwrap().clone();
        let _ = ctx.two_hop_v2_check_and_maybe_schedule_recovery(&tracker_snapshot, &config);
        let fresh_after =
            ARB_V2_SCREEN_SELL_STALE_THEN_FRESH_AFTER_PIN_TOTAL.load(Ordering::Relaxed);
        assert!(
            fresh_after > fresh_before,
            "sell-leg freshness recovery must increment then_fresh counter"
        );
        assert!(
            !ctx.v2_sell_stale_recovery_pending.read().contains_key(mint),
            "pending recovery must clear after sell-leg becomes fresh"
        );
    }

    /// C1h5 v2: recovery republish includes both cross-DEX legs as QuoteReady.
    #[test]
    fn v2_sell_leg_recovery_republishes_both_legs() {
        use ironcrab::metrics::ARB_V2_SELL_STALE_RECOVERY_OUTCOME_REPUBLISH_BOTH_LEGS;

        let ctx = test_arb_context(create_shared_cache());
        let ctx = Arc::new(ctx);
        let before = ARB_V2_SELL_STALE_RECOVERY_OUTCOME_REPUBLISH_BOTH_LEGS.load(Ordering::Relaxed);
        ctx.publish_v2_sell_leg_recovery_repins("buy_pool_addr", "sell_pool_addr");
        let after = ARB_V2_SELL_STALE_RECOVERY_OUTCOME_REPUBLISH_BOTH_LEGS.load(Ordering::Relaxed);
        assert_eq!(after, before + 1, "recovery must record both-leg republish");
    }
}

#[cfg(test)]
mod expected_token_output_gate_tests {
    use ironcrab::arbitrage::{is_expected_token_output_plausible, price_based_token_output_raw};
    use rust_decimal::Decimal;
    use std::str::FromStr;

    /// Prod 2026-08-05: meteora_dlmm buy published `expected_token_output=11` for 0.1 SOL.
    #[test]
    fn degenerate_dlmm_token_out_not_plausible_for_publish_metadata() {
        let buy_price = Decimal::from_str("0.00006069").unwrap();
        let trade_amount = 100_000_000u64;
        let estimate = price_based_token_output_raw(trade_amount, buy_price, 6).expect("estimate");
        assert!(
            !is_expected_token_output_plausible(11, Some(estimate), trade_amount),
            "token_out=11 must be suppressed so EE uses price-based fallback"
        );
    }
}

#[cfg(test)]
mod pool_accounts_coverage_tests {
    use super::*;
    use ironcrab::execution::live_pool_cache::{CachedPoolState, MeteoraState, OrcaWhirlpoolState};
    use solana_sdk::pubkey::Pubkey;
    use std::sync::atomic::Ordering;
    use std::time::Instant;

    fn pool_state(dex: &str, addr: &str) -> PoolState {
        PoolState {
            pool_address: addr.to_string(),
            dex: dex.to_string(),
            last_price: None,
            trade_price_buy: None,
            trade_price_sell: None,
            liquidity_sol: Decimal::ONE,
            has_reserve_data: true,
            last_update: Instant::now(),
            trade_count: 1,
            dex_accounts: None,
        }
    }

    fn usdc_meteora_orca_cache(meteora_pool: Pubkey, orca_pool: Pubkey) -> SharedLivePoolCache {
        let cache = create_shared_cache();
        let usdc = Pubkey::from_str(USDC_MINT).unwrap();
        let sol = Pubkey::from_str(NATIVE_SOL_MINT).unwrap();
        cache.upsert(
            meteora_pool,
            CachedPoolState::Meteora(MeteoraState {
                token_x_mint: usdc,
                token_y_mint: sol,
                reserve_x: Pubkey::new_unique(),
                reserve_y: Pubkey::new_unique(),
                active_id: 100,
                bin_step: 25,
                reserve_x_balance: Some(1_000_000_000),
                reserve_y_balance: Some(2_000_000_000),
            }),
            1,
        );
        cache.upsert(
            orca_pool,
            CachedPoolState::Orca(OrcaWhirlpoolState {
                token_mint_a: sol,
                token_mint_b: usdc,
                token_vault_a: Pubkey::new_unique(),
                token_vault_b: Pubkey::new_unique(),
                tick_current_index: -100,
                sqrt_price: 1_000_000,
                liquidity: 1_000_000,
                fee_rate: 300,
                protocol_fee_rate: 100,
                tick_spacing: 64,
                vault_a_balance: Some(5_000_000_000),
                vault_b_balance: Some(50_000_000),
                token_a_program: None,
                token_b_program: None,
            }),
            1,
        );
        cache
    }

    #[test]
    fn cold_start_backfill_resolves_meteora_orca_accounts_for_usdc() {
        let meteora_pool = Pubkey::new_unique();
        let orca_pool = Pubkey::new_unique();
        let cache = usdc_meteora_orca_cache(meteora_pool, orca_pool);
        let ctx = test_arb_context(cache);

        let mut tracker = TokenArbTracker::new(USDC_MINT);
        tracker.token_decimals = Some(6);
        tracker.upsert_pool(pool_state("meteora_dlmm", &meteora_pool.to_string()));
        tracker.upsert_pool(pool_state("orca", &orca_pool.to_string()));
        ctx.trackers.write().insert(USDC_MINT.to_string(), tracker);

        let opp = ArbOpportunity {
            base_mint: USDC_MINT.to_string(),
            buy_dex: "meteora_dlmm".to_string(),
            buy_pool: meteora_pool.to_string(),
            buy_price: Decimal::ONE,
            sell_dex: "orca".to_string(),
            sell_pool: orca_pool.to_string(),
            sell_price: Decimal::ONE,
            spread_bps: 72,
            trade_amount_lamports: 10_000_000,
            estimated_profit_lamports: 673_000,
        };

        let before_missing = ARB_REJECTED_MISSING_ACCOUNTS.load(Ordering::Relaxed);
        let (buy, sell) = ctx.get_pool_accounts_for_arb(&opp);
        assert!(
            buy.is_some(),
            "buy pool accounts must backfill from LivePoolCache"
        );
        assert!(
            sell.is_some(),
            "sell pool accounts must backfill from LivePoolCache"
        );
        assert!(
            buy.unwrap().len() >= 7,
            "meteora DexPoolAccounts layout must include active_id/bin_step"
        );
        assert!(
            sell.unwrap().len() >= 5,
            "orca DexPoolAccounts layout must include vaults + tick metadata"
        );

        assert!(create_arb_intent(&ctx, &opp).is_none());
        assert_eq!(
            ARB_REJECTED_MISSING_ACCOUNTS.load(Ordering::Relaxed),
            before_missing,
            "missing-accounts reject must not fire when cache backfill succeeded"
        );
    }

    #[test]
    fn dex_pool_accounts_before_tracker_lands_after_pool_upsert() {
        let cache = create_shared_cache();
        let ctx = test_arb_context(cache);
        let pool = "PoolPending111111111111111111111111111111";
        let token_mint = "TokenMintPending1111111111111111111111";
        let accounts = vec![
            pool.to_string(),
            token_mint.to_string(),
            NATIVE_SOL_MINT.to_string(),
            "vaultA".to_string(),
            "vaultB".to_string(),
            "active_id:42".to_string(),
            "bin_step:10".to_string(),
        ];

        ctx.handle_dex_pool_accounts(pool, token_mint, NATIVE_SOL_MINT, accounts.clone());
        assert!(ctx.pending_pool_accounts.read().contains_key(pool));

        let mint = ctx.handle_pool_created(
            pool,
            token_mint,
            NATIVE_SOL_MINT,
            "meteora_dlmm",
            Decimal::ONE,
        );
        assert_eq!(mint, None);

        let trackers = ctx.trackers.read();
        let tracker = trackers.get(token_mint).unwrap();
        assert_eq!(tracker.get_pool_accounts(pool).cloned(), Some(accounts));
        assert!(!ctx.pending_pool_accounts.read().contains_key(pool));
    }

    #[test]
    fn cross_mint_lookup_finds_sell_pool_on_wsol_tracker() {
        let orca_pool = Pubkey::new_unique();
        let cache = create_shared_cache();
        let usdc = Pubkey::from_str(USDC_MINT).unwrap();
        let sol = Pubkey::from_str(NATIVE_SOL_MINT).unwrap();
        cache.upsert(
            orca_pool,
            CachedPoolState::Orca(OrcaWhirlpoolState {
                token_mint_a: sol,
                token_mint_b: usdc,
                token_vault_a: Pubkey::new_unique(),
                token_vault_b: Pubkey::new_unique(),
                tick_current_index: 0,
                sqrt_price: 1_000_000,
                liquidity: 1_000_000,
                fee_rate: 300,
                protocol_fee_rate: 100,
                tick_spacing: 64,
                vault_a_balance: Some(1_000_000_000),
                vault_b_balance: Some(1_000_000),
                token_a_program: None,
                token_b_program: None,
            }),
            1,
        );
        let ctx = test_arb_context(cache);

        let orca_accounts = vec![
            orca_pool.to_string(),
            NATIVE_SOL_MINT.to_string(),
            USDC_MINT.to_string(),
            "vaultA".to_string(),
            "vaultB".to_string(),
            "tick_current_index:0".to_string(),
            "tick_spacing:64".to_string(),
        ];
        let mut wsol_tracker = TokenArbTracker::new(NATIVE_SOL_MINT);
        wsol_tracker.set_pool_accounts(&orca_pool.to_string(), orca_accounts);
        ctx.trackers
            .write()
            .insert(NATIVE_SOL_MINT.to_string(), wsol_tracker);

        let mut usdc_tracker = TokenArbTracker::new(USDC_MINT);
        usdc_tracker.upsert_pool(pool_state("orca", &orca_pool.to_string()));
        ctx.trackers
            .write()
            .insert(USDC_MINT.to_string(), usdc_tracker);

        let opp = ArbOpportunity {
            base_mint: USDC_MINT.to_string(),
            buy_dex: "meteora_dlmm".to_string(),
            buy_pool: Pubkey::new_unique().to_string(),
            buy_price: Decimal::ONE,
            sell_dex: "orca".to_string(),
            sell_pool: orca_pool.to_string(),
            sell_price: Decimal::ONE,
            spread_bps: 50,
            trade_amount_lamports: 10_000_000,
            estimated_profit_lamports: 100_000,
        };

        let sell = ctx
            .get_pool_accounts_for_arb(&opp)
            .1
            .expect("sell accounts via cross-mint WSOL tracker");
        assert_eq!(sell[0], orca_pool.to_string());
    }
}

#[cfg(test)]
mod pump_amm_strategy_tests {
    use super::pump_amm_pool_accounts_valid_for_swap;

    #[test]
    fn pump_amm_requires_14_accounts_with_pool_as_first() {
        let pool = "PoolPubkey1111111111111111111111111111111111";
        let mut ok: Vec<String> = (0..14).map(|i| format!("A{i}")).collect();
        ok[0] = pool.to_string();
        assert!(pump_amm_pool_accounts_valid_for_swap(pool, &ok));
    }

    #[test]
    fn pump_amm_rejects_short_or_mismatched_accounts() {
        let pool = "PoolPubkey1111111111111111111111111111111111";
        let short: Vec<String> = (0..5).map(|i| format!("A{i}")).collect();
        assert!(!pump_amm_pool_accounts_valid_for_swap(pool, &short));

        let wrong_first: Vec<String> = (0..14).map(|i| format!("A{i}")).collect();
        assert!(!pump_amm_pool_accounts_valid_for_swap(pool, &wrong_first));
    }
}
