//! momentum-bot binary – Strategy Plane (EARLY + ESTABLISHED policies)
//!
//! Source of Truth: docs/TARGET_ARCHITECTURE.md §2.2
//!
//! Responsibilities:
//! - Subscribe to MarketEvents from NATS
//! - Classify regime: EARLY vs ESTABLISHED
//! - Apply momentum policy to generate TradeIntents
//! - Publish TradeIntents to NATS
//! - Write trade_intents JSONL for replay
//!
//! Trading Strategy (4 Filters):
//! 1. Liquidity Check: Min SOL, dev supply < 99%, no LP removals
//! 2. Buyer Velocity: Unique buyers, trades/sec, buy dominance
//! 3. SOL Inflow: Net buy volume, no large dumps
//! 4. Dev Behavior: Early sell = exit, rebuy = positive
//!
//! This binary does NOT:
//! - Load wallet keys
//! - Sign or send transactions
//! - Directly call RPC/Geyser (gets data via MarketEvents)

// Allow items after test module - functions are organized by logical grouping
#![allow(clippy::items_after_test_module)]

use anyhow::Result;
use clap::Parser;
use once_cell::sync::Lazy;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;

use ironcrab::config::MomentumCfg;
use ironcrab::execution::live_pool_cache::LivePoolCache;
use ironcrab::execution::pool_cache_sync;
use ironcrab::execution::quote_calculator;
use ironcrab::execution::tokens_per_sol;
use ironcrab::ipc::{
    ConfigUpdate, ConfigUpdateResponse, ConfigUpdateStatus, ControlRequest, ControlRequestKind,
    ExecutionResult, ExecutionStatus, ExplicitAmount, IntentOrigin, IntentTier, MarketEvent,
    MarketEventKind, TradeExecutionConstraints, TradeIntent, TradeResources, TradeSide,
    TradingRegime, NATIVE_SOL_MINT,
};
use ironcrab::metrics::{
    momentum_event_ts_latency_delta_ms, record_momentum_active_pools_messages_total,
    record_momentum_core_market_events_ingest_drain_batch,
    record_momentum_core_market_events_processed_kind,
    record_momentum_core_market_events_received_kind, record_momentum_ingest_to_process_us,
    record_momentum_market_events_last_applied_slot,
    record_momentum_market_events_subscription_max_dequeued_slot,
    record_momentum_nats_batch_prepare_us, record_momentum_signal_eval_us,
    record_momentum_tracker_rejected, record_momentum_tracker_trades_recorded,
    record_momentum_trades_received_no_tracker, serve_metrics,
    set_momentum_bot_process_start_unix_sec,
    set_momentum_market_events_ingest_max_wall_lag_ms_last_batch, set_readiness_nats_connected,
    try_record_momentum_event_to_ingest_ms, try_record_momentum_event_to_intent_publish_ms,
    try_record_momentum_intent_header_to_publish_ms,
    try_record_momentum_jetstream_poolcache_event_to_ingest_ms,
    try_record_momentum_publish_to_intent_ms, wall_clock_unix_ms_now, MetricsComponent,
    EXITS_GENERATED_TOTAL, FILTER_PASSED_TOTAL, FILTER_REJECTED_BUYER_QUALITY,
    FILTER_REJECTED_DEV_BEHAVIOR, FILTER_REJECTED_DOWNTREND, FILTER_REJECTED_INFLOW,
    FILTER_REJECTED_LIQUIDITY, FILTER_REJECTED_TOKEN_AGE, FILTER_REJECTED_TOTAL,
    FILTER_REJECTED_VELOCITY, INTENTS_GENERATED_TOTAL, MARKET_EVENTS_CONSUMED_TOTAL,
    NATS_ERRORS_TOTAL, NATS_MESSAGES_PUBLISHED_TOTAL, NATS_MESSAGES_RECEIVED_TOTAL,
    POOLS_TRACKED_GAUGE, TOKENS_TRACKED_GAUGE,
};
use ironcrab::nats::{
    config_consumer_config, config_subject, ensure_execution_results_stream,
    ensure_trade_intents_stream, execution_results_consumer_config,
    pool_cache_live_consumer_config, wallet_snapshot_consumer_config,
    wallet_snapshot_live_consumer_config, MomentumActivePinReason, MomentumActivePoolEntry,
    MomentumActivePoolsUpdate, MomentumRemovedPoolEntry, NatsClient, NatsConfig, NatsMessage,
    CONFIG_STREAM_NAME, EXECUTION_RESULTS_STREAM_NAME, STREAM_NAME, TOPIC_CONTROL_REQUESTS,
    TOPIC_EXECUTION_RESULTS, TOPIC_MARKET_EVENTS, TOPIC_MOMENTUM_ACTIVE_POOLS,
    TOPIC_MOMENTUM_MARKET_EVENTS, TOPIC_TRADE_INTENTS, WALLET_SNAPSHOT_STREAM_NAME,
};
use ironcrab::solana::dex_parser::SOL_MINT_PUBKEY;
use ironcrab::storage::{JsonlWriterConfig, QueuedJsonlWriter};
use tokio::sync::mpsc;

/// NATS topic for config reload (P1: Runtime Configuration via UI)
const TOPIC_CONFIG_RELOAD: &str = "ironcrab.control.config.reload";

/// NATS topic for control commands (position cleanup, etc.)
const TOPIC_CONTROL_COMMANDS: &str = "ironcrab.control.commands";

// P1 Crash Isolation: Systemd Watchdog support
#[cfg(unix)]
use sd_notify::NotifyState;

/// Build version for decision records
const BUILD_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Mainnet USDC — never a Momentum entry candidate when `base_mint` / trade `mint`.
const MOMENTUM_NON_TRADEABLE_USDC: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
/// Mainnet USDT
const MOMENTUM_NON_TRADEABLE_USDT: &str = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB";
/// System program id (native SOL / sentinel mint keys in some paths).
const MOMENTUM_SYSTEM_PROGRAM_ID: &str = "11111111111111111111111111111111";

/// Quote / wrapped-native / stable mints must not become pool-scoped Momentum entry trackers.
fn is_non_tradeable_momentum_mint(mint: &str) -> bool {
    mint == NATIVE_SOL_MINT
        || mint == "NATIVE_SOL"
        || mint == MOMENTUM_SYSTEM_PROGRAM_ID
        || mint == MOMENTUM_NON_TRADEABLE_USDC
        || mint == MOMENTUM_NON_TRADEABLE_USDT
}

/// JetStream replay dedup for orphaned BUY path — bounded so memory does not grow forever.
const ORPHANED_RECOVERED_INTENT_IDS_CAP: usize = 50_000;

/// Wire version for [`TOPIC_MOMENTUM_ACTIVE_POOLS`] JSON payloads (PR-D).
const MOMENTUM_ACTIVE_POOLS_WIRE_VERSION: u32 = 1;
/// Discovery/Validation trackers without a trade for this long are dropped (PR-D plan #4).
const MOMENTUM_STALE_DISCOVERY_SECS: u64 = 30 * 60;

/// Rate-limit `trade without tracker` warnings (per mint+pool).
const MOMENTUM_NO_TRACKER_WARN_COOLDOWN: Duration = Duration::from_secs(60);

/// Emit `debug!` ingest forensics every N trades on an active (non-rejected) tracker row.
const MOMENTUM_TRACKER_TRADE_INGEST_DEBUG_EVERY: usize = 50;

static MOMENTUM_NO_TRACKER_WARN_LAST: Lazy<parking_lot::Mutex<HashMap<String, Instant>>> =
    Lazy::new(|| parking_lot::Mutex::new(HashMap::new()));

#[derive(Debug, Default)]
struct MomentumActivePoolPublishQueue {
    active: Vec<MomentumActivePoolEntry>,
    removed: Vec<MomentumRemovedPoolEntry>,
}

/// Cooldown between repeated `Ensure*` ControlRequests for the same open-position pool recovery key.
const OPEN_POSITION_POOL_RECOVERY_COOLDOWN: Duration = Duration::from_secs(60);

/// Max recovery publishes per timed reconcile tick when executable quotes are still missing.
const OPEN_POSITION_POOL_RECOVERY_RETRY_MAX_PER_TICK: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OpenPositionPoolRecoveryKind {
    PumpfunBonding,
    /// PumpSwap pool_accounts refresh when the position already references the PumpAmm market pool.
    PumpAmmAccounts,
    /// After migration off bonding curve: discover PumpAmm state without treating the bonding-curve
    /// pubkey as a PumpAmm `pool_address_hint` (market-data derives/finds the AMM pool from mint).
    PumpAmmMigrationProbe,
    OrcaWhirlpool,
    MeteoraDlmm,
    RaydiumAmm,
    RaydiumCpmm,
    MeteoraCpmm,
}

/// Ensure* recovery channels per normalized DEX (see [`MomentumContext::normalize_dex_for_execution_engine`]).
///
/// PumpFun positions get **two** bounded channels: bonding-curve refresh plus PumpAmm discovery so
/// post-migration exits are not stuck on stale curve-only JetStream state.
const OPEN_POSITION_RECOVERY_PUMPFUN: &[OpenPositionPoolRecoveryKind] = &[
    OpenPositionPoolRecoveryKind::PumpfunBonding,
    OpenPositionPoolRecoveryKind::PumpAmmMigrationProbe,
];
const OPEN_POSITION_RECOVERY_PUMP_AMM: &[OpenPositionPoolRecoveryKind] =
    &[OpenPositionPoolRecoveryKind::PumpAmmAccounts];
const OPEN_POSITION_RECOVERY_ORCA: &[OpenPositionPoolRecoveryKind] =
    &[OpenPositionPoolRecoveryKind::OrcaWhirlpool];
const OPEN_POSITION_RECOVERY_METEORA_DLMM: &[OpenPositionPoolRecoveryKind] =
    &[OpenPositionPoolRecoveryKind::MeteoraDlmm];
const OPEN_POSITION_RECOVERY_RAYDIUM_AMM: &[OpenPositionPoolRecoveryKind] =
    &[OpenPositionPoolRecoveryKind::RaydiumAmm];
const OPEN_POSITION_RECOVERY_RAYDIUM_CPMM: &[OpenPositionPoolRecoveryKind] =
    &[OpenPositionPoolRecoveryKind::RaydiumCpmm];
const OPEN_POSITION_RECOVERY_METEORA_CPMM: &[OpenPositionPoolRecoveryKind] =
    &[OpenPositionPoolRecoveryKind::MeteoraCpmm];

fn open_position_pool_recovery_kinds_for_normalized_dex(
    dex: &str,
) -> &'static [OpenPositionPoolRecoveryKind] {
    match dex {
        "pumpfun" => OPEN_POSITION_RECOVERY_PUMPFUN,
        "pump_amm" => OPEN_POSITION_RECOVERY_PUMP_AMM,
        "orca" => OPEN_POSITION_RECOVERY_ORCA,
        "meteora_dlmm" => OPEN_POSITION_RECOVERY_METEORA_DLMM,
        "raydium" => OPEN_POSITION_RECOVERY_RAYDIUM_AMM,
        "raydium_cpmm" => OPEN_POSITION_RECOVERY_RAYDIUM_CPMM,
        "meteora_cpmm" => OPEN_POSITION_RECOVERY_METEORA_CPMM,
        _ => &[],
    }
}

fn open_position_pool_recovery_dedupe_key(
    mint: &str,
    pool: &str,
    kind: OpenPositionPoolRecoveryKind,
) -> String {
    format!("{mint}\x1f{pool}\x1f{kind:?}")
}

/// Core NATS delivers no publisher discovery: subscribing to [`TOPIC_MOMENTUM_MARKET_EVENTS`]
/// succeeds even when market-data does not publish there yet. If no message arrives within this
/// window, subscribe to [`TOPIC_MARKET_EVENTS`] so rolling deploys do not starve silently.
///
/// `sd_notify(READY)` is sent only after this probe (see main loop startup) so systemd
/// `WatchdogSec` in `docs/systemd/momentum-bot.service` does not elapse during the wait.
const MOMENTUM_MARKET_EVENTS_FIRST_MSG_FALLBACK: Duration = Duration::from_secs(30);

/// JetStream publish / other failures still re-queue dirty evaluation; PumpFun missing creator does not.
const ERR_PUMPFUN_MISSING_CREATOR_BUY: &str =
    "cannot generate pumpfun intent: missing dev_wallet/creator";

#[inline]
fn err_chain_contains_pumpfun_missing_creator_buy(e: &anyhow::Error) -> bool {
    e.chain()
        .any(|c| c.to_string().contains(ERR_PUMPFUN_MISSING_CREATOR_BUY))
}

/// Yield + watchdog ping every N Core NATS events so long ingest batches cannot starve
/// `tokio::select!` (WatchdogSec / activity) for tens of seconds.
const CORE_MARKET_EVENTS_INGEST_COOPERATIVE_YIELD_INTERVAL: usize = 32;

// --- Scope C (momentum event lifecycle): bounded ExecutionResult drains + observability ---
/// Max `ExecutionResult` messages per `tokio::select!` activation (fairness vs MarketEvent / PoolCache).
const EXECUTION_RESULT_SCHEDULED_DRAIN_MAX: usize = 16;
/// Extra bounded drain after trade/bonding-related `MarketEvent`s (anti-starvation).
const EXECUTION_RESULT_INTERLEAVED_DRAIN_MAX: usize = 8;
/// JetStream `expires` for the dedicated `select!` drain arm — bounded wait when the stream is empty.
/// Paired with [`EXECUTION_RESULT_SCHEDULED_POLL_INTERVAL`]: the arm runs on a timer, not every select poll.
const EXECUTION_RESULT_SCHEDULED_FETCH_EXPIRES: Duration = Duration::from_millis(12);
/// Poll `ExecutionResult` JetStream at most on this interval so idle fetches do not starve Core NATS.
const EXECUTION_RESULT_SCHEDULED_POLL_INTERVAL: Duration = Duration::from_millis(20);
/// JetStream `expires` for drains **awaited inside** MarketEvent / PoolCache arms — must stay short so empty streams do not add multi‑10ms stalls on hot paths.
const EXECUTION_RESULT_INTERLEAVED_FETCH_EXPIRES: Duration = Duration::from_millis(3);
/// Smaller batches reduce single-arm starvation of other `tokio::select!` branches.
const POOL_CACHE_UPDATE_FETCH_MAX: usize = 32;
/// Short idle wait so PoolCache fetches do not monopolize the runtime vs Core NATS MarketEvents.
const POOL_CACHE_UPDATE_FETCH_EXPIRES: Duration = Duration::from_millis(8);
/// JetStream `expires` for PoolCache pulls **awaited inside** Core NATS ingest (interleave after each batch) —
/// must stay short so empty streams do not add multi‑10ms stalls on hot paths (same rationale as
/// [`EXECUTION_RESULT_INTERLEAVED_FETCH_EXPIRES`]).
const POOL_CACHE_UPDATE_INTERLEAVED_FETCH_EXPIRES: Duration = Duration::from_millis(3);
/// Core NATS market-events subject (full fan-out). Momentum-bot prefers [`TOPIC_MOMENTUM_MARKET_EVENTS`].
/// Drain immediately queued messages per select activation so JetStream arms cannot starve the
/// high-volume MarketEvents subscriber (slow-consumer stall). Adaptive cap raises toward
/// [`CORE_MARKET_EVENTS_INGEST_DRAIN_CAP_CEILING`] from **ingest pressure** (consecutive drain-cap
/// hits), not from slot deltas vs. an independent live head.
const CORE_MARKET_EVENTS_INGEST_DRAIN_MAX: usize = 96;
const CORE_MARKET_EVENTS_INGEST_DRAIN_CAP_CEILING: usize = 320;
/// Bounded PoolCache JetStream pull when interleaving after a Core NATS ingest batch.
const POOL_CACHE_UPDATE_INTERLEAVE_AFTER_CORE_MAX: usize = 8;
/// Wallet snapshot JetStream pull cap per `select!` activation (fairness vs Core MarketEvents).
const WALLET_SNAPSHOT_FETCH_MAX: usize = 16;
/// Adjacent `BondingCurveProgress` messages on Core NATS: bounded coalesce before strategy work.
const BONDING_CURVE_PROGRESS_STREAK_MAX: usize = 32;

#[inline]
fn momentum_core_market_event_kind_core_nats_label(kind: &MarketEventKind) -> &'static str {
    match kind {
        MarketEventKind::PoolCreated { .. } => "PoolCreated",
        MarketEventKind::Trade { .. } => "Trade",
        MarketEventKind::BondingCurveProgress { .. } => "BondingCurveProgress",
        MarketEventKind::DexPoolAccounts { .. } => "DexPoolAccounts",
        MarketEventKind::WalletBalanceSnapshot { .. } => "WalletBalanceSnapshot",
        MarketEventKind::SlotUpdate { .. } => "SlotUpdate",
        MarketEventKind::PoolStateUpdate { .. } => "PoolStateUpdate",
        MarketEventKind::TokenMintInfo { .. } => "TokenMintInfo",
        _ => "Other",
    }
}

/// Adaptive Core NATS drain cap from consecutive **cap-saturated** ingest batches (see
/// [`ironcrab::metrics::record_momentum_core_market_events_ingest_drain_batch`]).
#[inline]
fn core_market_events_ingest_drain_cap_for_streak(streak: u32) -> usize {
    let bonus = (streak as usize).saturating_mul(16).min(224);
    CORE_MARKET_EVENTS_INGEST_DRAIN_MAX
        .saturating_add(bonus)
        .min(CORE_MARKET_EVENTS_INGEST_DRAIN_CAP_CEILING)
}

#[inline]
fn core_market_events_ingest_drain_cap_now() -> usize {
    let streak = ironcrab::metrics::MOMENTUM_CORE_MARKET_EVENTS_INGEST_CONSECUTIVE_CAP_HIT_STREAK
        .load(Ordering::Relaxed);
    core_market_events_ingest_drain_cap_for_streak(streak)
}

/// WSOL-leg reserve-derived `(token_mint, tokens_per_sol, token_decimals, token_ui, sol_ui)` from `PoolCacheUpdate`.
type PoolCacheDerivedTps = Option<(String, f64, u8, f64, f64)>;
struct BoundedIntentIdCache {
    set: HashSet<String>,
    order: VecDeque<String>,
    cap: usize,
}

impl BoundedIntentIdCache {
    fn new(cap: usize) -> Self {
        Self {
            set: HashSet::new(),
            order: VecDeque::new(),
            cap,
        }
    }

    fn insert(&mut self, id: String) -> bool {
        if self.set.contains(&id) {
            return false;
        }
        self.set.insert(id.clone());
        self.order.push_back(id);
        while self.order.len() > self.cap {
            if let Some(old) = self.order.pop_front() {
                self.set.remove(&old);
            }
        }
        true
    }

    fn remove(&mut self, id: &str) -> bool {
        let removed = self.set.remove(id);
        if removed {
            if let Some(pos) = self.order.iter().position(|x| x == id) {
                self.order.remove(pos);
            }
        }
        removed
    }
}

/// Drop guard: records `momentum_process_market_event_us` (full async scope of `process_market_event`).
struct MomentumProcessMarketEventTimer(std::time::Instant);
impl Drop for MomentumProcessMarketEventTimer {
    fn drop(&mut self) {
        let us = self.0.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
        ironcrab::metrics::record_momentum_process_market_event_us(us);
    }
}

/// Drop guard: records `momentum_record_trade_us` for hot-path `MomentumContext::record_trade`.
struct MomentumRecordTradeTimer(std::time::Instant);
impl Drop for MomentumRecordTradeTimer {
    fn drop(&mut self) {
        let us = self.0.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
        ironcrab::metrics::record_momentum_record_trade_us(us);
    }
}
#[derive(Parser, Debug)]
#[command(name = "momentum-bot")]
#[command(about = "IronCrab Strategy Plane – Momentum policy and TradeIntent generation")]
struct Args {
    /// Path to configuration file
    #[arg(short, long, default_value = "config.toml")]
    config: PathBuf,

    /// NATS server URL
    #[arg(long, env = "NATS_URL", default_value = "nats://localhost:4222")]
    nats_url: String,

    /// Prometheus metrics port
    #[arg(long, default_value = "9802")]
    metrics_port: u16,

    /// Log directory override
    #[arg(long, env = "IRONCRAB_LOG_DIR")]
    log_dir: Option<PathBuf>,

    /// Test mode: generate intents for allowlisted mints only
    #[arg(long)]
    test_mode: bool,

    /// Dry run: don't publish to NATS
    #[arg(long)]
    dry_run: bool,
}

/// Momentum policy configuration
///
/// All defaults are documented (DoD K) P0: No hidden defaults).
#[derive(Debug, Clone)]
struct MomentumConfig {
    /// Minimum liquidity (SOL) for EARLY regime. Default: 5.0 SOL
    early_min_liquidity_sol: f64,
    /// Minimum liquidity (SOL) for ESTABLISHED regime. Default: 20.0 SOL
    established_min_liquidity_sol: f64,
    /// Slot threshold for EARLY -> ESTABLISHED transition. Default: 1000 slots
    early_slot_threshold: u64,
    /// Max slippage BPS for EARLY trades. Default: 300 (3%)
    early_max_slippage_bps: u32,
    /// Max slippage BPS for ESTABLISHED trades. Default: 100 (1%)
    established_max_slippage_bps: u32,
    /// Default position size (SOL lamports). Default: 0.1 SOL
    default_position_lamports: u64,
    /// Allowlist for test mode (mints that trigger intents). Default: empty
    test_allowlist: HashSet<String>,

    // === Filter 1: Liquidity Check ===
    /// Max dev supply percentage (e.g., 90.0 = 90%). Default: 90%
    max_dev_supply_pct: f64,
    /// Window to detect LP removal (seconds). Default: 60s
    lp_removal_window_secs: u64,
    /// Min token age since discovery before entry (seconds). Default: 60s. `0` = disabled.
    min_token_age_secs: u64,

    // === Filter 2: Buyer Velocity ===
    /// Min unique buyers in early window. Default: 10
    min_unique_buyers: u32,
    /// Early window for buyer count (seconds). Default: 20s
    buyer_window_secs: u64,
    /// Min trades per minute for momentum (chain-slot window). Default: 30 (~0.5/s).
    min_trades_per_min: f64,
    /// Min buy dominance ratio (buys / total). Default: 0.6 (60%)
    min_buy_dominance: f64,

    // === Filter 3: SOL Inflow ===
    /// Min net SOL inflow in window (lamports). Default: 20 SOL
    min_sol_inflow_lamports: u64,
    /// Inflow window (seconds). Default: 30s
    inflow_window_secs: u64,
    /// Max single dump size (lamports). Default: 5 SOL
    max_single_dump_lamports: u64,

    // === Dev-Sell Re-Validation (pre-entry) ===
    /// Pause after pre-entry dev sell before re-evaluating standard soft gates (seconds). Default: 30
    dev_sell_revalidation_delay_secs: u64,

    // === Token Safety: Mint/Freeze Authority ===
    /// Require mint authority to be renounced (mint_authority == None) before entering.
    require_mint_authority_renounced: bool,
    /// Require freeze authority to be none before entering.
    require_freeze_authority_none: bool,

    // === Momentum v2 Entry: Probe-Buy + Scale-In ===
    /// Probe-buy size as fraction of `default_position_lamports` (0.0..=1.0)
    probe_buy_pct: f64,
    /// Time window (seconds) after probe fill to allow scale-in confirmation
    scale_in_confirm_window_secs: u64,
    /// Minimum `tokens_per_sol::pnl_pct(entry_price, executable_exit_quote)` (I-14) to allow
    /// scale-in; compared as strictly `exec_pnl > this` (default `0.0`).
    scale_in_min_probe_executable_pnl_pct: f64,

    // === Buyer Quality (anti-bot / concentration) ===
    /// Cap for top-1 buyer share (0.0..=1.0)
    top1_buyer_share_cap: f64,
    /// Cap for top-3 buyers combined share (0.0..=1.0)
    top3_buyer_share_cap: f64,
    /// Minimum ratio of repeat buyers (0.0..=1.0)
    repeat_buyer_min_ratio: f64,

    // === Trade Size Distribution (micro-buy spam) ===
    /// Minimum SOL trade size (lamports) used to classify "small buys"
    min_trade_size_lamports: u64,
    /// Maximum allowed ratio (0.0..=1.0) of buys below `min_trade_size_lamports`
    small_buy_ratio_cap: f64,

    // === Dump-Recovery Gate (anti-rug) ===
    dump_recovery_window_secs: u64,
    dump_recovery_min_buy_dominance: f64,
    dump_recovery_min_net_inflow_lamports: u64,
    dump_recovery_min_recovery_secs: u64,

    // === Filter 5: Price Trend / Downtrend Gate ===
    price_trend_filter_enabled: bool,
    price_trend_window_secs: u64,
    price_trend_min_trades: u32,
    price_trend_min_tps_rise_pct: f64,
    price_trend_max_drawdown_pct: f64,
    price_trend_recovery_min_buy_dominance: f64,
    price_trend_recovery_min_inflow_lamports: u64,
    price_trend_bucket_count: u32,
    price_trend_lower_highs_max_breaks: u32,
    price_trend_recovery_min_positive_buckets: u32,
    price_trend_recovery_min_secs: u64,
    price_trend_recovery_requires_no_lower_highs: bool,

    // === Exit Strategy ===
    /// Grace period (secs): normal hard stop suppressed; catastrophic threshold only.
    hard_stop_min_hold_secs: u64,
    /// Hard stop (%) during grace (catastrophic / rug). Default: 45%
    catastrophic_stop_loss_pct: f64,
    /// Hard stop-loss percentage from entry (e.g., 15 = -15%). Default: 15%
    hard_stop_loss_pct: f64,
    /// Trailing stop percentage from ATH (e.g., 20 = -20% from high). Default: 20%
    trailing_stop_pct: f64,
    /// Minimum profit to activate trailing stop (e.g., 10 = +10%). Default: 10%
    trailing_activation_pct: f64,
    /// Take profit percentage (e.g., 100 = +100% = 2x). Default: 100%
    take_profit_pct: f64,
    /// Minimum hold time (secs) before TAKE_PROFIT can trigger. Prevents false TP from
    /// wrong-pool price updates immediately after probe. Default: 5
    take_profit_min_hold_secs: u64,
    /// Max hold time in seconds before forced exit. Default: 300s (5 min)
    max_hold_time_secs: u64,
    /// Absolute max hold before TIME_EXIT regardless of velocity. 0 = disabled.
    max_hold_absolute_cap_secs: u64,
    /// TIME_EXIT gated on trades_per_min < min_trades_per_min. Default: true
    time_exit_requires_low_velocity: bool,
    /// Min hold before MOMENTUM_EXIT. Default: 60s
    momentum_exit_min_hold_secs: u64,
    /// Momentum exit only when PnL <= momentum_exit_max_pnl_pct. Default: true
    momentum_exit_only_when_losing: bool,
    /// PnL ceiling for momentum exit when only_when_losing is false.
    momentum_exit_max_pnl_pct: f64,
    /// Momentum exit: min buy ratio to stay in (e.g., 0.4 = 40% buys). Default: 0.4
    momentum_exit_buy_ratio: f64,
    /// Momentum exit window (seconds). Default: 30s
    momentum_exit_window_secs: u64,
    /// Min trades in momentum window to evaluate exit. Default: 5
    momentum_exit_min_trades: u32,
    /// Max slippage BPS for EXIT trades. Default: 9500 (95%)
    /// High value ensures sells succeed even at loss - prevents stuck positions.
    exit_max_slippage_bps: u32,
    /// Bonding curve exit: threshold in percent (e.g. 98.0 = exit when 98% complete).
    /// Set to 0.0 to disable. Default: 98.0
    bonding_curve_exit_pct: f64,
    /// Bonding curve exit: enable (default: false). When true, use bonding_curve_exit_threshold_bps.
    bonding_curve_exit_enabled: bool,
    /// Bonding curve exit: threshold in BPS (0–10000). Default: 9800 (98%).
    bonding_curve_exit_threshold_bps: u32,
}

impl Default for MomentumConfig {
    fn default() -> Self {
        Self {
            early_min_liquidity_sol: 5.0,           // 5 SOL min for early trades
            established_min_liquidity_sol: 20.0,    // 20 SOL min for established
            early_slot_threshold: 1000,             // ~400s until established
            early_max_slippage_bps: 300,            // 3% for volatile early stage
            established_max_slippage_bps: 100,      // 1% for stable pools
            default_position_lamports: 100_000_000, // 0.1 SOL per trade
            test_allowlist: HashSet::new(),         // empty = all mints allowed

            // Filter 1: Liquidity Check
            max_dev_supply_pct: 90.0,   // Max 90% dev supply
            lp_removal_window_secs: 60, // Track LP removals for 60s
            min_token_age_secs: 60,     // Ignore first minute after discovery

            // Filter 2: Buyer Velocity
            min_unique_buyers: 10,    // 10 unique buyers min
            buyer_window_secs: 20,    // in first 20 seconds
            min_trades_per_min: 30.0, // ≈0.5 trades/s momentum (per-minute threshold)
            min_buy_dominance: 0.6,   // 60% buys vs sells

            // Filter 3: SOL Inflow
            min_sol_inflow_lamports: 20_000_000_000, // 20 SOL net inflow
            inflow_window_secs: 30,                  // in 30 seconds
            max_single_dump_lamports: 5_000_000_000, // Max 5 SOL single sell

            // Dev-Sell Re-Validation
            dev_sell_revalidation_delay_secs: 30,

            // Token Safety
            require_mint_authority_renounced: false,
            require_freeze_authority_none: false,

            // Momentum v2 Entry
            probe_buy_pct: 0.25,
            scale_in_confirm_window_secs: 30,
            scale_in_min_probe_executable_pnl_pct: 0.0,

            // Buyer Quality
            top1_buyer_share_cap: 0.35,
            top3_buyer_share_cap: 0.60,
            repeat_buyer_min_ratio: 0.05,

            // Trade Size Distribution
            min_trade_size_lamports: 10_000_000, // 0.01 SOL
            small_buy_ratio_cap: 0.85,

            // Dump-Recovery
            dump_recovery_window_secs: 30,
            dump_recovery_min_buy_dominance: 0.55,
            dump_recovery_min_net_inflow_lamports: 1_000_000_000, // 1 SOL
            dump_recovery_min_recovery_secs: 10,

            // Filter 5: Price Trend
            price_trend_filter_enabled: true,
            price_trend_window_secs: 120,
            price_trend_min_trades: 18,
            price_trend_min_tps_rise_pct: 8.0,
            price_trend_max_drawdown_pct: 25.0,
            price_trend_recovery_min_buy_dominance: 0.60,
            price_trend_recovery_min_inflow_lamports: 500_000_000,
            price_trend_bucket_count: 5,
            price_trend_lower_highs_max_breaks: 0,
            price_trend_recovery_min_positive_buckets: 2,
            price_trend_recovery_min_secs: 15,
            price_trend_recovery_requires_no_lower_highs: true,

            // Exit Strategy
            hard_stop_min_hold_secs: 45,
            catastrophic_stop_loss_pct: 45.0,
            hard_stop_loss_pct: 15.0,      // -15% hard stop
            trailing_stop_pct: 20.0,       // -20% from ATH
            trailing_activation_pct: 10.0, // Activate trailing after +10%
            take_profit_pct: 100.0,        // Take profit at +100% (2x)
            take_profit_min_hold_secs: 5,  // No TP in first 5s (avoids wrong-pool price spike)
            max_hold_time_secs: 300,       // Max 5 minutes hold
            max_hold_absolute_cap_secs: 0,
            time_exit_requires_low_velocity: true,
            momentum_exit_min_hold_secs: 60,
            momentum_exit_only_when_losing: true,
            momentum_exit_max_pnl_pct: 0.0,
            momentum_exit_buy_ratio: 0.4,  // Exit if buy ratio < 40%
            momentum_exit_window_secs: 30, // Check last 30s of trades
            momentum_exit_min_trades: 5,   // Need 5+ trades to evaluate
            exit_max_slippage_bps: 9500,   // 95% - sell at any price rather than hold
            bonding_curve_exit_pct: 98.0,  // Exit when bonding curve is 98% complete
            bonding_curve_exit_enabled: false,
            bonding_curve_exit_threshold_bps: 9800, // 98%
        }
    }
}

impl MomentumConfig {
    /// Create MomentumConfig from TOML-loaded MomentumCfg
    fn from_cfg(cfg: &MomentumCfg) -> Self {
        Self {
            early_min_liquidity_sol: cfg.early_min_liquidity_sol,
            established_min_liquidity_sol: cfg.established_min_liquidity_sol,
            early_slot_threshold: cfg.early_slot_threshold,
            early_max_slippage_bps: cfg.early_max_slippage_bps,
            established_max_slippage_bps: cfg.established_max_slippage_bps,
            default_position_lamports: cfg.default_position_lamports,
            probe_buy_pct: cfg.probe_buy_pct,
            scale_in_confirm_window_secs: cfg.scale_in_confirm_window_secs,
            scale_in_min_probe_executable_pnl_pct: cfg.scale_in_min_probe_executable_pnl_pct,
            test_allowlist: HashSet::new(), // Not in TOML config
            max_dev_supply_pct: cfg.max_dev_supply_pct,
            lp_removal_window_secs: cfg.lp_removal_window_secs,
            min_token_age_secs: cfg.min_token_age_secs,
            min_unique_buyers: cfg.min_unique_buyers,
            buyer_window_secs: cfg.buyer_window_secs,
            min_trades_per_min: cfg.min_trades_per_min,
            min_buy_dominance: cfg.min_buy_dominance,
            min_sol_inflow_lamports: cfg.min_sol_inflow_lamports,
            inflow_window_secs: cfg.inflow_window_secs,
            max_single_dump_lamports: cfg.max_single_dump_lamports,
            dev_sell_revalidation_delay_secs: cfg.dev_sell_revalidation_delay_secs,
            require_mint_authority_renounced: cfg.require_mint_authority_renounced,
            require_freeze_authority_none: cfg.require_freeze_authority_none,
            top1_buyer_share_cap: cfg.top1_buyer_share_cap,
            top3_buyer_share_cap: cfg.top3_buyer_share_cap,
            repeat_buyer_min_ratio: cfg.repeat_buyer_min_ratio,
            min_trade_size_lamports: cfg.min_trade_size_lamports,
            small_buy_ratio_cap: cfg.small_buy_ratio_cap,
            dump_recovery_window_secs: cfg.dump_recovery_window_secs,
            dump_recovery_min_buy_dominance: cfg.dump_recovery_min_buy_dominance,
            dump_recovery_min_net_inflow_lamports: cfg.dump_recovery_min_net_inflow_lamports,
            dump_recovery_min_recovery_secs: cfg.dump_recovery_min_recovery_secs,
            price_trend_filter_enabled: cfg.price_trend_filter_enabled,
            price_trend_window_secs: cfg.price_trend_window_secs,
            price_trend_min_trades: cfg.price_trend_min_trades,
            price_trend_min_tps_rise_pct: cfg.price_trend_min_tps_rise_pct,
            price_trend_max_drawdown_pct: cfg.price_trend_max_drawdown_pct,
            price_trend_recovery_min_buy_dominance: cfg.price_trend_recovery_min_buy_dominance,
            price_trend_recovery_min_inflow_lamports: cfg.price_trend_recovery_min_inflow_lamports,
            price_trend_bucket_count: cfg.price_trend_bucket_count,
            price_trend_lower_highs_max_breaks: cfg.price_trend_lower_highs_max_breaks,
            price_trend_recovery_min_positive_buckets: cfg
                .price_trend_recovery_min_positive_buckets,
            price_trend_recovery_min_secs: cfg.price_trend_recovery_min_secs,
            price_trend_recovery_requires_no_lower_highs: cfg
                .price_trend_recovery_requires_no_lower_highs,
            hard_stop_min_hold_secs: cfg.hard_stop_min_hold_secs,
            catastrophic_stop_loss_pct: cfg.catastrophic_stop_loss_pct,
            hard_stop_loss_pct: cfg.hard_stop_loss_pct,
            trailing_stop_pct: cfg.trailing_stop_pct,
            trailing_activation_pct: cfg.trailing_activation_pct,
            take_profit_pct: cfg.take_profit_pct,
            take_profit_min_hold_secs: cfg.take_profit_min_hold_secs,
            max_hold_time_secs: cfg.max_hold_time_secs,
            max_hold_absolute_cap_secs: cfg.max_hold_absolute_cap_secs,
            time_exit_requires_low_velocity: cfg.time_exit_requires_low_velocity,
            momentum_exit_min_hold_secs: cfg.momentum_exit_min_hold_secs,
            momentum_exit_only_when_losing: cfg.momentum_exit_only_when_losing,
            momentum_exit_max_pnl_pct: cfg.momentum_exit_max_pnl_pct,
            momentum_exit_buy_ratio: cfg.momentum_exit_buy_ratio,
            momentum_exit_window_secs: cfg.momentum_exit_window_secs,
            momentum_exit_min_trades: cfg.momentum_exit_min_trades,
            exit_max_slippage_bps: cfg.exit_max_slippage_bps,
            bonding_curve_exit_pct: cfg.bonding_curve_exit_pct.unwrap_or(98.0),
            bonding_curve_exit_enabled: cfg.bonding_curve_exit_enabled,
            bonding_curve_exit_threshold_bps: cfg.bonding_curve_exit_threshold_bps,
        }
    }
}

// ============================================================================
// Token Tracking Structures for Strategy Filters
// ============================================================================

/// Tracks a single trade event
#[derive(Debug, Clone)]
struct TradeEvent {
    /// Legacy ingest-time clock — **not** used for strategy windows (burst-safe filters use [`Self::slot`]).
    #[allow(dead_code)]
    timestamp: Instant,
    /// Geyser / `MarketEvent.slot` for this trade. `0` = unknown (excluded from chain-window stats).
    slot: u64,
    trader: String,
    is_buy: bool,
    sol_amount: u64,   // in lamports
    token_amount: u64, // raw token units
    signature: String,
}

/// Approximate median ms per finalized slot for translating config **seconds** windows into **slot spans**
/// when no per-trade wall clock must be used (I-7: no `getBlockTime` on hot path).
const MOMENTUM_APPROX_SLOT_MS: u64 = 400;

#[inline]
fn momentum_secs_to_slot_span(secs: u64) -> u64 {
    if secs == 0 {
        return 0;
    }
    secs.saturating_mul(1000)
        .saturating_div(MOMENTUM_APPROX_SLOT_MS)
        .max(1)
}

#[inline]
fn momentum_trade_in_chain_slot_window(trade_slot: u64, head_slot: u64, window_slots: u64) -> bool {
    if trade_slot == 0 || head_slot == 0 || window_slots == 0 {
        return false;
    }
    head_slot.saturating_sub(trade_slot) <= window_slots
}

/// Trade-implied `tokens_per_sol` ratio (raw token / raw lamports). Lower = more valuable (I-14).
fn trade_implied_tps(sol_amount: u64, token_amount: u64) -> Option<f64> {
    if sol_amount == 0 || token_amount == 0 {
        return None;
    }
    let tps = token_amount as f64 / sol_amount as f64;
    if tps.is_finite() && tps > 0.0 {
        Some(tps)
    } else {
        None
    }
}

#[inline]
fn trade_age_slots(trade_slot: u64, head_slot: u64) -> Option<u64> {
    if trade_slot == 0 || head_slot == 0 {
        return None;
    }
    Some(head_slot.saturating_sub(trade_slot))
}

fn median_f64(mut values: Vec<f64>) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mid = values.len() / 2;
    if values.len().is_multiple_of(2) {
        Some((values[mid - 1] + values[mid]) / 2.0)
    } else {
        Some(values[mid])
    }
}

fn tps_rise_pct(earlier: f64, later: f64) -> f64 {
    if earlier <= 0.0 || later <= 0.0 {
        return 0.0;
    }
    (later / earlier - 1.0) * 100.0
}

fn clamp_price_trend_bucket_count(n: u32) -> usize {
    n.clamp(3, 8) as usize
}

/// Bucket index: 0 = latest (late), `bucket_count - 1` = earliest (early).
fn price_trend_bucket_index(age_slots: u64, window_slots: u64, bucket_count: usize) -> usize {
    let bucket_span = (window_slots / bucket_count as u64).max(1);
    usize::min(
        (age_slots / bucket_span) as usize,
        bucket_count.saturating_sub(1),
    )
}

fn price_trend_local_best_per_bucket(bucket_tps: &[Vec<f64>]) -> Vec<Option<f64>> {
    bucket_tps
        .iter()
        .map(|vals| vals.iter().copied().reduce(f64::min))
        .collect()
}

fn price_trend_median_per_bucket(bucket_tps: &[Vec<f64>]) -> Vec<Option<f64>> {
    bucket_tps
        .iter()
        .map(|vals| median_f64(vals.clone()))
        .collect()
}

/// Lower-highs pair count from early (high index) toward late (index 0).
fn price_trend_lower_highs_stats(local_best: &[Option<f64>]) -> (u32, u32) {
    let n = local_best.len();
    if n < 2 {
        return (0, 0);
    }
    let total_pairs = (n - 1) as u32;
    let mut satisfied = 0u32;
    for i in (1..n).rev() {
        if let (Some(older), Some(newer)) = (local_best[i], local_best[i - 1]) {
            if older < newer {
                satisfied = satisfied.saturating_add(1);
            }
        }
    }
    (satisfied, total_pairs)
}

fn price_trend_lower_highs_raw_match(local_best: &[Option<f64>]) -> bool {
    let (satisfied, total) = price_trend_lower_highs_stats(local_best);
    if total == 0 {
        return false;
    }
    let n = local_best.len();
    let (Some(early), Some(late)) = (local_best[n - 1], local_best[0]) else {
        return false;
    };
    satisfied == total && early < late
}

fn price_trend_lower_highs_signal_match(
    local_best: &[Option<f64>],
    min_tps_rise_pct: f64,
    max_breaks: u32,
) -> Option<(f64, u32, u32)> {
    let n = local_best.len();
    if n < 2 {
        return None;
    }
    let early = local_best[n - 1]?;
    let late = local_best[0]?;
    let rise = tps_rise_pct(early, late);
    if rise < min_tps_rise_pct {
        return None;
    }
    let (satisfied, total) = price_trend_lower_highs_stats(local_best);
    let breaks = total.saturating_sub(satisfied);
    if breaks > max_breaks {
        return None;
    }
    Some((rise, satisfied, breaks))
}

/// Confirmed upward price move: falling median_tps across the latest `min_positive` bucket pairs.
fn price_trend_recovery_slope_ok(medians: &[Option<f64>], min_positive: usize) -> bool {
    if min_positive == 0 {
        return true;
    }
    if min_positive >= medians.len() {
        return false;
    }
    for k in 0..min_positive {
        let Some(late) = medians[k] else {
            return false;
        };
        let Some(older) = medians[k + 1] else {
            return false;
        };
        if late >= older {
            return false;
        }
    }
    true
}

/// Chain-slot-window trades/min (same formula as [`TokenTracker::calculate_metrics`]).
fn trades_per_min_in_buyer_window(
    trades: &[TradeEvent],
    buyer_window_secs: u64,
    head_slot: u64,
) -> f64 {
    let window_slots = momentum_secs_to_slot_span(buyer_window_secs);
    let hs = head_slot.max(1);
    let trades_with_slot_in_window = trades
        .iter()
        .filter(|t| momentum_trade_in_chain_slot_window(t.slot, hs, window_slots))
        .filter(|t| t.slot > 0)
        .count() as u32;
    let window_secs = buyer_window_secs as f64;
    if window_secs > 0.0 {
        trades_with_slot_in_window as f64 / (window_secs / 60.0)
    } else {
        0.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
enum PositionEntrySource {
    Live,
    WalletSnapshot,
}

impl Default for PositionEntrySource {
    fn default() -> Self {
        Self::Live
    }
}

/// Tracks an open position for exit strategy
#[derive(Debug, Clone)]
struct PositionTracker {
    /// Token mint address
    mint: String,
    /// Token decimals (from MarketEventKind::TokenMintInfo)
    token_decimals: u8,
    /// Pool address for selling
    pool: String,
    /// DEX name
    dex: String,
    /// Entry time
    entry_time: Instant,
    /// Entry price (tokens_per_sol — LOWER = more valuable token)
    entry_price: f64,
    /// Amount of tokens held (raw)
    token_amount: u64,
    /// SOL invested (lamports)
    sol_invested: u64,
    /// Trade-session high since BUY: best holder price = lowest `tokens_per_sol` on this
    /// position pool from entry fill + trade-derived marks only (not PoolCache reserve marks).
    highest_price: f64,
    /// Last position-pool **trade** print or entry / scale-in fill `tokens_per_sol` (I-13).
    /// Not advanced by `update_mark_price` (reserve marks) — used for trade-based trailing trigger.
    trade_mark_tps: f64,
    /// Current estimated price
    current_price: f64,
    /// Recent trades for momentum calculation
    recent_trades: Vec<TradeEvent>,
    /// Has trailing stop been activated?
    trailing_active: bool,
    /// Exit intent already generated?
    exit_generated: bool,
    /// When we last generated an exit intent (for retry reconciliation)
    exit_generated_at: Option<Instant>,
    /// Last SELL error code (e.g. "Custom(6023)") — for retry cooldown tuning
    last_sell_error_code: Option<String>,
    /// When the last SELL failed (for 6023 → 15s cooldown)
    last_sell_fail_at: Option<Instant>,
    /// Number of consecutive SELL failures due to slippage (6002); for Phase 3 escalation
    sell_slippage_fail_count: u32,
    /// Origin of this position (live execution vs. wallet snapshot reconciliation)
    entry_source: PositionEntrySource,
    /// Token program (SPL Token or Token-2022) — persisted for correct ATA handling on SELL
    token_program: Option<String>,
    /// Current bonding curve progress (basis points, 0-10000). None if not PumpFun.
    bonding_curve_progress_bps: Option<u32>,
    /// PumpFun creator (for BC-SELL; only when dex == pumpfun)
    creator: Option<String>,
    /// Geyser/RPC confirmed slot of the entry BUY (0 = unknown / legacy).
    entry_confirmed_slot: u64,
    /// Last slot at which we applied a position price update (monotonic).
    last_price_slot: u64,
}

/// Executable (reserve-based) `tokens_per_sol` for selling `position.token_amount`, when available.
/// I-7: from LivePoolCache in-process only — no RPC.
/// I-13: If `marks_position_pool` is false (e.g. PumpSwap quote while position.pool is still PumpFun BC),
/// this quote is only for exit decisions — never apply `tokens_per_sol` as `current_price` on the position.
/// Price exits `STOP_LOSS` / `TAKE_PROFIT` are **quote-first**: they trigger only from a usable
/// executable reserve quote when present. **Current-price-only** triggers for those exits are
/// disabled — `current_price` may diverge from the executable for logging and reporting, but it is
/// not their trigger source (see `should_exit`). `TRAILING_STOP` **activation** uses peak PnL
/// implied by trade-session high (`highest_price`) vs `entry_price`; **trigger** uses drawdown from
/// session high to the position trade-mark (`trade_mark_tps`: trade prints + fills only), not the
/// executable quote. Optional guarded executable PnL may appear in logs for forensics. Alternate-pool
/// quotes may still inform `STOP_LOSS` / `TAKE_PROFIT` per routing policy but must not drive trailing
/// vs the position session high (I-13).
#[derive(Debug, Clone)]
struct ExitExecutableQuote {
    /// tokens_per_sol (UI): token_ui / sol_ui, matching `entry_price` / `current_price` (I-14).
    tokens_per_sol: f64,
    /// True when `tokens_per_sol` was computed from LivePoolCache reserve math (not a trade-ratio spike).
    pool_sourced: bool,
    /// Pool this quote was computed from (best SOL-out route among eligible cached pools).
    quote_pool: String,
    quote_dex: String,
    /// When true, it is safe to refresh mark-to-market from `tokens_per_sol` (`quote_pool == position.pool`).
    marks_position_pool: bool,
    /// Geyser slot from `LivePoolCache` metadata when available.
    source_slot: Option<u64>,
    cache_age_ms: Option<u64>,
}

/// Reserve-based quote is usable for quote-first price exits (I-14, I-15, I-16).
#[inline]
fn exit_executable_quote_is_usable(q: &ExitExecutableQuote) -> bool {
    q.pool_sourced && q.tokens_per_sol > 0.0 && q.tokens_per_sol.is_finite()
}

/// Conservative max `LivePoolCache` age for **price-based** exits (STOP / TP).
/// Below `quote_calculator`'s 5s warning so we do not SELL-trigger on long-stale reserve snapshots
/// when the chain may have moved (I-16).
const EXIT_QUOTE_MAX_CACHE_AGE_MS: u64 = 4_000;

const SPL_TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const SPL_TOKEN_2022_PROGRAM: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";

#[inline]
fn exit_quote_address_is_token_program_pseudopool(addr: &str) -> bool {
    addr == SPL_TOKEN_PROGRAM || addr == SPL_TOKEN_2022_PROGRAM
}

/// When DexPoolAccounts are known, require an explicit WSOL leg **and** the position mint in the
/// account list so we never treat a non-SOL pair as a SOL-out executable (I-15 companion).
fn dex_pool_accounts_include_wsol_and_position_mint(
    accounts: &[String],
    position_token_mint: &str,
) -> bool {
    if accounts.is_empty() {
        return false;
    }
    let has_wsol = accounts.iter().any(|a| a == WSOL_MINT);
    let has_mint = accounts.iter().any(|a| a == position_token_mint);
    has_wsol && has_mint
}

/// Returns `Some(reason_code)` when this quote must **not** drive price-based exits.
fn exit_quote_price_exit_guard_violation(
    pos: &PositionTracker,
    q: &ExitExecutableQuote,
    dex_pool_accounts: Option<&[String]>,
) -> Option<&'static str> {
    if !exit_executable_quote_is_usable(q) {
        return Some("NOT_POOL_SOURCED_OR_INVALID_TPS");
    }
    if exit_quote_address_is_token_program_pseudopool(q.quote_pool.as_str()) {
        return Some("PSEUDOPOOL_TOKEN_PROGRAM_ADDRESS");
    }
    if let Some(age) = q.cache_age_ms {
        if age > EXIT_QUOTE_MAX_CACHE_AGE_MS {
            return Some("STALE_CACHE_AGE_MS");
        }
    }
    if let Some(slot) = q.source_slot {
        if pos.entry_confirmed_slot > 0 && slot <= pos.entry_confirmed_slot {
            return Some("QUOTE_SLOT_NOT_AFTER_ENTRY_CONFIRM");
        }
        if pos.last_price_slot > 0 && slot < pos.last_price_slot {
            return Some("QUOTE_SLOT_BEFORE_LAST_POSITION_MARK_SLOT");
        }
    }
    if let Some(accs) = dex_pool_accounts {
        if !accs.is_empty()
            && !dex_pool_accounts_include_wsol_and_position_mint(accs, pos.mint.as_str())
        {
            return Some("QUOTE_POOL_ACCOUNTS_NOT_VERIFIED_SOL_PAIR");
        }
    }
    None
}

/// `exit_quote` = optional reserve quote; `price_exit_quote` = same row after freshness / SOL-leg
/// guards (subset of `exit_quote`).
#[allow(clippy::too_many_arguments)]
fn log_momentum_exit_price_decision(
    pos: &PositionTracker,
    exit_quote: Option<&ExitExecutableQuote>,
    price_exit_quote: Option<&ExitExecutableQuote>,
    exit_type: &str,
    decision: &'static str,
    reason_code: Option<&'static str>,
    event_or_check_source: &str,
    log_level: MomentumExitPriceLogLevel,
) {
    let exit_quote_present = exit_quote.is_some();
    let (quote_pool, quote_dex, q_tps, marks_pp, q_slot, q_age) = exit_quote
        .map(|q| {
            (
                q.quote_pool.as_str(),
                q.quote_dex.as_str(),
                q.tokens_per_sol,
                q.marks_position_pool,
                q.source_slot,
                q.cache_age_ms,
            )
        })
        .unwrap_or(("", "", f64::NAN, false, None, None));
    let quote_for_pnl = price_exit_quote.or(exit_quote);
    let quote_pnl_pct = quote_for_pnl
        .map(|q| tokens_per_sol::pnl_pct(pos.entry_price, q.tokens_per_sol))
        .map(|p| format!("{:.6}", p));
    let quote_dd_pct = quote_for_pnl
        .map(|q| tokens_per_sol::drawdown_from_ath_pct(pos.highest_price, q.tokens_per_sol))
        .map(|p| format!("{:.6}", p));
    let current_pnl = format!("{:.6}", pos.pnl_pct());
    let current_dd = format!("{:.6}", pos.drawdown_from_ath_pct());
    match log_level {
        MomentumExitPriceLogLevel::Info => {
            info!(
                mint = %pos.mint,
                position_pool = %pos.pool,
                position_dex = %pos.dex,
                position_token_amount_raw = pos.token_amount,
                entry_price = pos.entry_price,
                current_price = pos.current_price,
                highest_price = pos.highest_price,
                current_pnl_pct = %current_pnl,
                current_drawdown_pct = %current_dd,
                exit_quote_present,
                quote_pool = %quote_pool,
                quote_dex = %quote_dex,
                quote_tokens_per_sol = q_tps,
                quote_pnl_pct = %quote_pnl_pct.as_deref().unwrap_or("n/a"),
                quote_drawdown_pct = %quote_dd_pct.as_deref().unwrap_or("n/a"),
                quote_source_slot = ?q_slot,
                quote_cache_age_ms = ?q_age,
                marks_position_pool = marks_pp,
                event_or_check_source = %event_or_check_source,
                exit_type = %exit_type,
                decision = %decision,
                reason_code = ?reason_code,
                "momentum_exit_price_decision"
            );
        }
        MomentumExitPriceLogLevel::Warn => {
            warn!(
                mint = %pos.mint,
                position_pool = %pos.pool,
                position_dex = %pos.dex,
                position_token_amount_raw = pos.token_amount,
                entry_price = pos.entry_price,
                current_price = pos.current_price,
                highest_price = pos.highest_price,
                current_pnl_pct = %current_pnl,
                current_drawdown_pct = %current_dd,
                exit_quote_present,
                quote_pool = %quote_pool,
                quote_dex = %quote_dex,
                quote_tokens_per_sol = q_tps,
                quote_pnl_pct = %quote_pnl_pct.as_deref().unwrap_or("n/a"),
                quote_drawdown_pct = %quote_dd_pct.as_deref().unwrap_or("n/a"),
                quote_source_slot = ?q_slot,
                quote_cache_age_ms = ?q_age,
                marks_position_pool = marks_pp,
                event_or_check_source = %event_or_check_source,
                exit_type = %exit_type,
                decision = %decision,
                reason_code = ?reason_code,
                "momentum_exit_price_decision"
            );
        }
        MomentumExitPriceLogLevel::Debug => {
            debug!(
                mint = %pos.mint,
                position_pool = %pos.pool,
                position_dex = %pos.dex,
                position_token_amount_raw = pos.token_amount,
                entry_price = pos.entry_price,
                current_price = pos.current_price,
                highest_price = pos.highest_price,
                current_pnl_pct = %current_pnl,
                current_drawdown_pct = %current_dd,
                exit_quote_present,
                quote_pool = %quote_pool,
                quote_dex = %quote_dex,
                quote_tokens_per_sol = q_tps,
                quote_pnl_pct = %quote_pnl_pct.as_deref().unwrap_or("n/a"),
                quote_drawdown_pct = %quote_dd_pct.as_deref().unwrap_or("n/a"),
                quote_source_slot = ?q_slot,
                quote_cache_age_ms = ?q_age,
                marks_position_pool = marks_pp,
                event_or_check_source = %event_or_check_source,
                exit_type = %exit_type,
                decision = %decision,
                reason_code = ?reason_code,
                "momentum_exit_price_decision"
            );
        }
    }
}

#[derive(Clone, Copy)]
enum MomentumExitPriceLogLevel {
    Info,
    Warn,
    Debug,
}

#[derive(Clone, Debug)]
struct TimedExitReconcileCandidate {
    mint: String,
    pool: String,
    dex: String,
    token_amount: u64,
    hold_secs: u64,
    last_exit_age_secs: Option<u64>,
}

/// Serializable position state for JetStream KV persistence
///
/// This struct can be serialized to JSON and stored in NATS JetStream KV.
/// Unlike PositionTracker which uses std::time::Instant (not serializable),
/// this uses Unix timestamps for entry_time.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct PersistedPosition {
    /// Token mint address
    mint: String,
    /// Token decimals
    token_decimals: u8,
    /// Pool address for selling
    pool: String,
    /// DEX name
    dex: String,
    /// Entry time as Unix timestamp (milliseconds)
    entry_time_unix_ms: u64,
    /// Entry price (token per SOL)
    entry_price: f64,
    /// Amount of tokens held (raw)
    token_amount: u64,
    /// SOL invested (lamports)
    sol_invested: u64,
    /// Highest price seen since entry
    highest_price: f64,
    /// Last trade-mark tps on position pool (not reserve marks); default 0 = legacy → use current_price on load.
    #[serde(default)]
    trade_mark_tps: f64,
    /// Current estimated price
    current_price: f64,
    /// Has trailing stop been activated?
    trailing_active: bool,
    /// Exit intent already generated?
    exit_generated: bool,
    /// When exit intent was last generated (unix ms)
    #[serde(default)]
    exit_generated_at_unix_ms: Option<u64>,
    /// Origin of this position
    #[serde(default)]
    entry_source: PositionEntrySource,
    /// Token program (SPL Token or Token-2022) — for correct ATA handling on SELL
    #[serde(default)]
    token_program: Option<String>,
    /// Bonding curve progress (basis points, 0-10000). None if not PumpFun.
    #[serde(default)]
    bonding_curve_progress_bps: Option<u32>,
    /// PumpFun creator (for BC-SELL; only when dex == pumpfun)
    #[serde(default)]
    creator: Option<String>,
    /// Geyser/RPC slot of confirmed BUY (0 = unknown)
    #[serde(default)]
    entry_confirmed_slot: u64,
    /// Last slot applied to a position-pool price update (mark or trade-derived).
    #[serde(default)]
    last_price_slot: u64,
    /// Schema version for forward compatibility
    schema_version: u8,
}

impl PersistedPosition {
    const CURRENT_SCHEMA_VERSION: u8 = 4;

    /// Convert from PositionTracker to PersistedPosition
    fn from_tracker(tracker: &PositionTracker) -> Self {
        // Calculate entry_time_unix_ms from the elapsed time
        let elapsed = tracker.entry_time.elapsed();
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let entry_time_unix_ms = now_ms.saturating_sub(elapsed.as_millis() as u64);
        let exit_generated_at_unix_ms = tracker.exit_generated_at.map(|ts| {
            let exit_elapsed = ts.elapsed();
            now_ms.saturating_sub(exit_elapsed.as_millis() as u64)
        });

        Self {
            mint: tracker.mint.clone(),
            token_decimals: tracker.token_decimals,
            pool: tracker.pool.clone(),
            dex: tracker.dex.clone(),
            entry_time_unix_ms,
            entry_price: tracker.entry_price,
            token_amount: tracker.token_amount,
            sol_invested: tracker.sol_invested,
            highest_price: tracker.highest_price,
            trade_mark_tps: tracker.trade_mark_tps,
            current_price: tracker.current_price,
            trailing_active: tracker.trailing_active,
            exit_generated: tracker.exit_generated,
            exit_generated_at_unix_ms,
            entry_source: tracker.entry_source,
            token_program: tracker.token_program.clone(),
            bonding_curve_progress_bps: tracker.bonding_curve_progress_bps,
            creator: tracker.creator.clone(),
            entry_confirmed_slot: tracker.entry_confirmed_slot,
            last_price_slot: tracker.last_price_slot,
            schema_version: Self::CURRENT_SCHEMA_VERSION,
        }
    }

    /// Convert to PositionTracker, reconstructing entry_time from Unix timestamp
    fn to_tracker(&self) -> PositionTracker {
        // Calculate how long ago entry_time was
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let elapsed_ms = now_ms.saturating_sub(self.entry_time_unix_ms);
        let exit_elapsed_ms = self
            .exit_generated_at_unix_ms
            .map(|ms| now_ms.saturating_sub(ms));

        // Reconstruct entry_time as Instant::now() minus elapsed
        // Note: This is approximate but accurate enough for our purposes
        let entry_time = Instant::now()
            .checked_sub(Duration::from_millis(elapsed_ms))
            .unwrap_or_else(Instant::now);
        let exit_generated_at =
            exit_elapsed_ms.and_then(|ms| Instant::now().checked_sub(Duration::from_millis(ms)));

        // Never fall back to `current_price`: it may reflect PoolCache reserve marks only
        // (I-13 / trade-based trailing). Prefer trade-session fields from the snapshot.
        let trade_mark_tps = if self.trade_mark_tps > 0.0 && self.trade_mark_tps.is_finite() {
            self.trade_mark_tps
        } else if self.highest_price > 0.0 && self.highest_price.is_finite() {
            self.highest_price
        } else {
            self.entry_price
        };
        PositionTracker {
            mint: self.mint.clone(),
            token_decimals: self.token_decimals,
            pool: self.pool.clone(),
            dex: self.dex.clone(),
            entry_time,
            entry_price: self.entry_price,
            token_amount: self.token_amount,
            sol_invested: self.sol_invested,
            highest_price: self.highest_price,
            trade_mark_tps,
            current_price: self.current_price,
            recent_trades: Vec::new(), // Not persisted - will be rebuilt from live data
            trailing_active: self.trailing_active,
            exit_generated: self.exit_generated,
            exit_generated_at,
            last_sell_error_code: None,
            last_sell_fail_at: None,
            sell_slippage_fail_count: 0,
            entry_source: self.entry_source,
            token_program: self.token_program.clone(),
            bonding_curve_progress_bps: self.bonding_curve_progress_bps,
            creator: self.creator.clone(),
            entry_confirmed_slot: self.entry_confirmed_slot,
            last_price_slot: self.last_price_slot,
        }
    }
}

/// JetStream KV bucket name for position state
const POSITION_KV_BUCKET: &str = "POSITIONS";

/// Extract BUY confirmation slot for Scope 1 price-update gating (I-13 companion).
fn entry_confirmed_slot_from_execution(result: &ExecutionResult) -> u64 {
    result
        .confirmed_slot
        .or_else(|| {
            result
                .metadata
                .get("confirmed_slot")
                .and_then(|s| s.parse::<u64>().ok())
        })
        .unwrap_or(0)
}

impl PositionTracker {
    fn new(
        mint: &str,
        pool: &str,
        dex: &str,
        entry_price: f64,
        token_decimals: u8,
        token_amount: u64,
        sol_invested: u64,
    ) -> Self {
        Self {
            mint: mint.to_string(),
            token_decimals,
            pool: pool.to_string(),
            dex: dex.to_string(),
            entry_time: Instant::now(),
            entry_price,
            token_amount,
            sol_invested,
            highest_price: entry_price,
            trade_mark_tps: entry_price,
            current_price: entry_price,
            recent_trades: Vec::new(),
            trailing_active: false,
            exit_generated: false,
            exit_generated_at: None,
            last_sell_error_code: None,
            last_sell_fail_at: None,
            sell_slippage_fail_count: 0,
            entry_source: PositionEntrySource::Live,
            token_program: None,
            bonding_curve_progress_bps: None,
            creator: None,
            entry_confirmed_slot: 0,
            last_price_slot: 0,
        }
    }

    /// Set PumpFun creator (for BC-SELL; only when dex == pumpfun)
    fn set_creator(&mut self, creator: &str) {
        self.creator = Some(creator.to_string());
    }

    #[cfg(test)]
    fn set_entry_time_ago(&mut self, ago: Duration) {
        self.entry_time = Instant::now().checked_sub(ago).unwrap_or_else(Instant::now);
    }

    /// PoolCache / reserve **mark** only: updates `current_price` for reporting; does **not**
    /// advance trade-session high (`highest_price`). I-13: session high stays on position-pool
    /// fills + trade ratios, not reserve marks.
    fn update_mark_price(&mut self, new_price: f64) {
        self.current_price = new_price;
    }

    /// Position-pool **trade** print: updates mark and session high (lowest tps = best for holder).
    fn update_from_trade_price(&mut self, trade_tps: f64) {
        self.current_price = trade_tps;
        self.trade_mark_tps = trade_tps;
        self.highest_price = tokens_per_sol::updated_highest_price(self.highest_price, trade_tps);
    }

    /// Scale-in: blend `entry_price` (tokens_per_sol) by SOL weight using the **fill** tps for
    /// the new tranche — not `current_price` (mark can diverge). Resets trailing baseline to the
    /// scale-in fill (session high overwrite, `trailing_active = false`, fresh slot gate).
    fn add_investment(
        &mut self,
        additional_sol: u64,
        fill_entry_tps: f64,
        scale_in_confirmed_slot: u64,
    ) {
        if additional_sol == 0 {
            return;
        }

        let leg_tps = if fill_entry_tps > 0.0 && fill_entry_tps.is_finite() {
            fill_entry_tps
        } else {
            // Defensive: missing fill tps — keep prior entry for this leg's weight (avoid 0).
            self.entry_price
        };

        let old_highest = self.highest_price;
        let old_trailing = self.trailing_active;

        let old_sol = self.sol_invested.max(1);
        let new_sol = old_sol.saturating_add(additional_sol).max(1);
        let new_entry = ((self.entry_price * (old_sol as f64))
            + (leg_tps * (additional_sol as f64)))
            / (new_sol as f64);

        self.sol_invested = self.sol_invested.saturating_add(additional_sol);
        self.entry_price = new_entry;
        // Trailing baseline reset: new tranche — overwrite session high to scale-in fill (not min with probe ATH).
        self.highest_price = leg_tps;
        self.trade_mark_tps = leg_tps;
        self.current_price = leg_tps;
        self.trailing_active = false;
        self.recent_trades.clear();
        if scale_in_confirmed_slot > 0 {
            self.entry_confirmed_slot = self.entry_confirmed_slot.max(scale_in_confirmed_slot);
        }
        // Fresh slot gate for marks + trade prints: `update_position_price` / reserve hints reject
        // `source_slot` <= `last_price_slot`. Without this reset, a pre-scale-in `last_price_slot`
        // above the new `entry_confirmed_slot` can drop valid post-scale-in pool trades.
        self.last_price_slot = 0;

        info!(
            mint = %self.mint,
            event = "scale_in_trailing_baseline_reset",
            before_session_high_tps = old_highest,
            after_session_high_tps = self.highest_price,
            trailing_active_before = old_trailing,
            trailing_active_after = self.trailing_active,
            scale_in_confirmed_slot = scale_in_confirmed_slot,
            leg_fill_tps = leg_tps,
            "scale-in: trailing session baseline reset to fill"
        );
    }

    /// Record a trade for momentum tracking
    fn record_trade(&mut self, trade: TradeEvent) {
        self.recent_trades.push(trade);
        // Keep only last 100 trades
        if self.recent_trades.len() > 100 {
            self.recent_trades.remove(0);
        }
    }

    /// Calculate current P&L percentage.
    /// Prices are in tokens_per_sol: LOWER tps = more valuable token.
    /// SOL value of position = token_amount / tokens_per_sol.
    /// PnL = (entry_tps / current_tps - 1) * 100
    ///   - Token gets cheaper (current_tps UP): negative PnL (loss)
    ///   - Token gets more expensive (current_tps DOWN): positive PnL (gain)
    fn pnl_pct(&self) -> f64 {
        tokens_per_sol::pnl_pct(self.entry_price, self.current_price)
    }

    /// Drawdown of the mark vs trade-session high (same formula as `tokens_per_sol::drawdown_from_ath_pct`;
    /// `highest_price` is the session-best lowest tps).
    fn drawdown_from_ath_pct(&self) -> f64 {
        tokens_per_sol::drawdown_from_ath_pct(self.highest_price, self.current_price)
    }

    /// Check if we should exit this position. `exit_quote` is a reserve-based quote for selling
    /// `token_amount` from `LivePoolCache` (I-7: no RPC).
    ///
    /// Quote-first (price exits): `STOP_LOSS` and `TAKE_PROFIT` require a quote that passes
    /// [`exit_quote_price_exit_guard_violation`]. `current_price` alone never trips those exits.
    /// `TRAILING_STOP` **activation** follows peak PnL from trade-session high vs `entry_price`;
    /// **trigger** is drawdown from session high to `trade_mark_tps` (trade path + fills only), not
    /// the executable quote. Optional executable data is logged when present for forensics.
    fn should_exit(
        &mut self,
        config: &MomentumConfig,
        exit_quote: Option<&ExitExecutableQuote>,
        event_or_check_source: &str,
        chain_head_slot: u64,
        live_trades_per_min: Option<f64>,
    ) -> Option<(String, String)> {
        let structural_q = exit_quote.filter(|q| exit_executable_quote_is_usable(q));
        let price_exit_q =
            exit_quote.filter(|q| exit_quote_price_exit_guard_violation(self, q, None).is_none());
        let pnl = self.pnl_pct();
        let hold_secs = self.entry_time.elapsed().as_secs();

        let pnl_for_reporting = price_exit_q
            .map(|q| tokens_per_sol::pnl_pct(self.entry_price, q.tokens_per_sol))
            .unwrap_or_else(|| {
                structural_q
                    .map(|q| tokens_per_sol::pnl_pct(self.entry_price, q.tokens_per_sol))
                    .unwrap_or(pnl)
            });

        let effective_hard_stop = if hold_secs < config.hard_stop_min_hold_secs {
            config.catastrophic_stop_loss_pct
        } else {
            config.hard_stop_loss_pct
        };
        let hard_stop_in_grace = hold_secs < config.hard_stop_min_hold_secs;

        // 1. Hard stop — quote-first on **guarded** executable PnL (I-14, I-16).
        if let Some(q) = price_exit_q {
            let exec_pnl = tokens_per_sol::pnl_pct(self.entry_price, q.tokens_per_sol);
            if exec_pnl <= -effective_hard_stop {
                log_momentum_exit_price_decision(
                    self,
                    exit_quote,
                    Some(q),
                    "STOP_LOSS",
                    "allow",
                    None,
                    event_or_check_source,
                    MomentumExitPriceLogLevel::Info,
                );
                let grace_label = if hard_stop_in_grace { " (grace)" } else { "" };
                return Some((
                    "STOP_LOSS".to_string(),
                    format!(
                        "Hard stop{}: {:.1}% executable loss (limit: -{:.1}%)",
                        grace_label, exec_pnl, effective_hard_stop
                    ),
                ));
            }
        } else if pnl <= -effective_hard_stop {
            let suppressed =
                exit_quote.and_then(|q| exit_quote_price_exit_guard_violation(self, q, None));
            if let Some(code) = suppressed {
                log_momentum_exit_price_decision(
                    self,
                    exit_quote,
                    None,
                    "STOP_LOSS",
                    "suppress",
                    Some(code),
                    event_or_check_source,
                    MomentumExitPriceLogLevel::Warn,
                );
            } else {
                log_momentum_exit_price_decision(
                    self,
                    None,
                    None,
                    "STOP_LOSS",
                    "skip",
                    Some("NO_EXECUTABLE_QUOTE"),
                    event_or_check_source,
                    MomentumExitPriceLogLevel::Debug,
                );
            }
        }

        // 2. Take profit — guarded executable gain after min hold.
        if hold_secs >= config.take_profit_min_hold_secs {
            if let Some(q) = price_exit_q {
                let exec_pnl = tokens_per_sol::pnl_pct(self.entry_price, q.tokens_per_sol);
                if exec_pnl >= config.take_profit_pct {
                    trace!(
                        mint = %self.mint,
                        entry_price = self.entry_price,
                        current_price = self.current_price,
                        highest_price = self.highest_price,
                        pnl_pct = pnl,
                        take_profit_pct = config.take_profit_pct,
                        ratio_entry_over_current = self.entry_price / self.current_price,
                        "TAKE_PROFIT trigger inputs"
                    );
                    log_momentum_exit_price_decision(
                        self,
                        exit_quote,
                        Some(q),
                        "TAKE_PROFIT",
                        "allow",
                        None,
                        event_or_check_source,
                        MomentumExitPriceLogLevel::Info,
                    );
                    return Some((
                        "TAKE_PROFIT".to_string(),
                        format!(
                            "Take profit hit: +{:.1}% executable gain (target: +{:.1}%)",
                            exec_pnl, config.take_profit_pct
                        ),
                    ));
                }
            } else if pnl >= config.take_profit_pct {
                if let Some(q) = exit_quote {
                    if let Some(code) = exit_quote_price_exit_guard_violation(self, q, None) {
                        log_momentum_exit_price_decision(
                            self,
                            exit_quote,
                            None,
                            "TAKE_PROFIT",
                            "suppress",
                            Some(code),
                            event_or_check_source,
                            MomentumExitPriceLogLevel::Warn,
                        );
                    }
                } else {
                    log_momentum_exit_price_decision(
                        self,
                        None,
                        None,
                        "TAKE_PROFIT",
                        "skip",
                        Some("NO_EXECUTABLE_QUOTE"),
                        event_or_check_source,
                        MomentumExitPriceLogLevel::Debug,
                    );
                }
            }
        }

        // 2b. Bonding Curve Exit - curve nearing completion, sell before migration
        // A.2 Phase 7: Skip when bonding_curve_exit_enabled=false; else use _threshold_bps or legacy _pct
        if config.bonding_curve_exit_enabled || config.bonding_curve_exit_pct > 0.0 {
            if let Some(progress_bps) = self.bonding_curve_progress_bps {
                let threshold_bps = if config.bonding_curve_exit_enabled {
                    config.bonding_curve_exit_threshold_bps
                } else {
                    (config.bonding_curve_exit_pct * 100.0) as u32
                };
                if progress_bps >= threshold_bps {
                    return Some((
                        "BONDING_CURVE_EXIT".to_string(),
                        format!(
                            "Bonding curve {:.1}% complete (threshold: {:.1}%), P&L: {:.1}%",
                            progress_bps as f64 / 100.0,
                            threshold_bps as f64 / 100.0,
                            pnl
                        ),
                    ));
                }
            }
        }

        // 3. Trailing — session high (`highest_price`) only from entry + position-pool trades
        // (see `update_from_trade_price` / fills). PoolCache marks must not advance session high.
        // Activation = peak PnL implied by session high vs entry; trigger = trade mark vs session high.
        let session_peak_pnl_pct = tokens_per_sol::pnl_pct(self.entry_price, self.highest_price);
        let trade_mark_tps = self.trade_mark_tps;
        let drawdown_vs_session_high_trade =
            tokens_per_sol::drawdown_from_ath_pct(self.highest_price, trade_mark_tps);
        let drawdown_vs_session_high_exec = price_exit_q
            .map(|q| tokens_per_sol::drawdown_from_ath_pct(self.highest_price, q.tokens_per_sol));
        trace!(
            mint = %self.mint,
            session_high_tps = self.highest_price,
            trade_mark_tps,
            session_peak_pnl_pct,
            executable_quote_tps = ?price_exit_q.map(|q| q.tokens_per_sol),
            drawdown_from_session_high_pct = drawdown_vs_session_high_trade,
            drawdown_from_session_high_executable_pct = ?drawdown_vs_session_high_exec,
            trailing_active = self.trailing_active,
            trailing_would_activate = session_peak_pnl_pct >= config.trailing_activation_pct,
            "trailing_session_high_eval"
        );

        if session_peak_pnl_pct >= config.trailing_activation_pct {
            self.trailing_active = true;
        }

        if self.trailing_active && drawdown_vs_session_high_trade >= config.trailing_stop_pct {
            let exec_note = match price_exit_q {
                Some(q) => {
                    let exec_pnl = tokens_per_sol::pnl_pct(self.entry_price, q.tokens_per_sol);
                    format!(
                        " Optional executable P&L vs entry: {:.1}% (quote marks position pool: {}).",
                        exec_pnl, q.marks_position_pool
                    )
                }
                None => String::new(),
            };
            log_momentum_exit_price_decision(
                self,
                exit_quote,
                price_exit_q,
                "TRAILING_STOP",
                "allow",
                None,
                event_or_check_source,
                MomentumExitPriceLogLevel::Info,
            );
            return Some((
                "TRAILING_STOP".to_string(),
                format!(
                    "Trailing stop: {:.1}% drawdown from session high via trade mark (limit: {:.1}%).{}",
                    drawdown_vs_session_high_trade,
                    config.trailing_stop_pct,
                    exec_note
                ),
            ));
        }

        // 4. Time Exit — velocity-gated (dead token after max hold); optional absolute cap.
        if config.max_hold_time_secs > 0 && hold_secs >= config.max_hold_time_secs {
            let vel = live_trades_per_min.unwrap_or_else(|| {
                trades_per_min_in_buyer_window(
                    &self.recent_trades,
                    config.buyer_window_secs,
                    chain_head_slot,
                )
            });
            let absolute_cap = config.max_hold_absolute_cap_secs > 0
                && hold_secs >= config.max_hold_absolute_cap_secs;
            let velocity_dead = !config.time_exit_requires_low_velocity
                || config.min_trades_per_min <= 0.0
                || vel < config.min_trades_per_min;

            if absolute_cap || velocity_dead {
                let cap_note = if absolute_cap { ", absolute_cap" } else { "" };
                return Some((
                    "TIME_EXIT".to_string(),
                    format!(
                        "Max hold: {}s (limit: {}s), trades_per_min={:.1} (min={:.1}), P&L: {:.1}%{}",
                        hold_secs,
                        config.max_hold_time_secs,
                        vel,
                        config.min_trades_per_min,
                        pnl_for_reporting,
                        cap_note
                    ),
                ));
            }
            trace!(
                mint = %self.mint,
                hold_secs,
                trades_per_min = vel,
                min_trades_per_min = config.min_trades_per_min,
                "TIME_EXIT suppressed: hold >= max_hold but velocity still active"
            );
        }

        // 5. Momentum Exit - selling pressure detected (hold + PnL gated)
        let momentum_hold_ok = hold_secs >= config.momentum_exit_min_hold_secs
            && hold_secs >= config.hard_stop_min_hold_secs;
        let momentum_pnl_ok = if config.momentum_exit_only_when_losing {
            pnl_for_reporting <= 0.0
        } else {
            pnl_for_reporting <= config.momentum_exit_max_pnl_pct
        };

        if momentum_hold_ok && momentum_pnl_ok {
            // FIX-30d: Volume-weighted momentum ratio instead of trade-count ratio.
            let momentum_window_slots =
                momentum_secs_to_slot_span(config.momentum_exit_window_secs);
            let head = chain_head_slot.max(1);
            let recent: Vec<_> = self
                .recent_trades
                .iter()
                .filter(|t| {
                    momentum_trade_in_chain_slot_window(t.slot, head, momentum_window_slots)
                })
                .collect();

            if recent.len() >= config.momentum_exit_min_trades as usize {
                let buy_count = recent.iter().filter(|t| t.is_buy).count();
                let total = recent.len();
                let buy_vol: u64 = recent
                    .iter()
                    .filter(|t| t.is_buy)
                    .map(|t| t.sol_amount)
                    .sum();
                let total_vol: u64 = recent.iter().map(|t| t.sol_amount).sum();

                let buy_ratio = if total_vol > 0 {
                    buy_vol as f64 / total_vol as f64
                } else {
                    buy_count as f64 / total as f64
                };

                if buy_ratio < config.momentum_exit_buy_ratio {
                    return Some((
                        "MOMENTUM_EXIT".to_string(),
                        format!(
                            "Momentum fading: buy vol ratio {:.0}% < {:.0}% ({}b/{}t, buy_vol={:.4}SOL/total={:.4}SOL), P&L: {:.1}%",
                            buy_ratio * 100.0,
                            config.momentum_exit_buy_ratio * 100.0,
                            buy_count, total,
                            buy_vol as f64 / 1e9, total_vol as f64 / 1e9,
                            pnl_for_reporting
                        ),
                    ));
                }
            }
        }

        None // No exit signal
    }
}

/// Explicit lifecycle state for token tracking.
///
/// State machine transitions:
/// ```text
/// Discovery → Validation → ProbeBuyPending → PositionOpenProbe
///                      ↘                         ↓
///                       Rejected          ScaleInPending → PositionOpenFull
///                                                ↓
///                                            Rejected
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TrackerState {
    /// Initial state: Newly discovered token, collecting first trades.
    Discovery,
    /// Passed basic filters, waiting for velocity/quality thresholds.
    Validation,
    /// Probe buy intent sent, awaiting execution result.
    ProbeBuyPending { sent_at: Instant },
    /// Probe buy confirmed, position open with probe amount only.
    PositionOpenProbe { filled_at: Instant },
    /// Scale-in intent sent, awaiting execution result.
    ScaleInPending { sent_at: Instant },
    /// Full position open (probe + scale-in complete).
    PositionOpenFull { filled_at: Instant },
    /// Terminal state: Token rejected (filter fail, execution fail, timeout, etc.)
    Rejected,
}

impl Default for TrackerState {
    fn default() -> Self {
        Self::Discovery
    }
}

impl TrackerState {
    /// Returns true if in a terminal state (Rejected or fully filled)
    fn is_terminal(&self) -> bool {
        matches!(self, Self::Rejected | Self::PositionOpenFull { .. })
    }

    /// Returns true if any buy intent has been sent (pending or filled)
    #[allow(dead_code)] // Useful helper for future usage
    fn has_entry_started(&self) -> bool {
        !matches!(self, Self::Discovery | Self::Validation)
    }

    /// Returns true if probe was filled (position open)
    #[allow(dead_code)] // Useful helper for future usage
    fn has_probe_filled(&self) -> bool {
        matches!(
            self,
            Self::PositionOpenProbe { .. }
                | Self::ScaleInPending { .. }
                | Self::PositionOpenFull { .. }
        )
    }

    /// Returns true if waiting for any execution result
    #[allow(dead_code)] // Useful helper for future usage
    fn is_pending_execution(&self) -> bool {
        matches!(
            self,
            Self::ProbeBuyPending { .. } | Self::ScaleInPending { .. }
        )
    }
}

/// Tracks token metrics for strategy decisions
#[derive(Debug, Clone)]
struct TokenTracker {
    /// Token mint address
    mint: String,
    /// Pool address
    pool: String,
    /// DEX (raydium, orca, pumpfun)
    dex: String,
    /// When we first saw this token
    first_seen: Instant,
    /// First slot we saw
    first_slot: u64,
    /// Dev wallet address (creator)
    dev_wallet: Option<String>,
    /// Initial liquidity (SOL lamports)
    initial_liquidity: u64,
    /// Dev supply percentage at creation
    dev_supply_pct: Option<f64>,

    /// DEX-specific static accounts needed for deterministic tx building (e.g. pump_amm).
    /// Expected to be the v1 ordered list (len=14) from MarketEventKind::DexPoolAccounts.
    dex_pool_accounts: Option<Vec<String>>,

    // Trade tracking
    trades: Vec<TradeEvent>,
    unique_buyers: HashSet<String>,
    unique_sellers: HashSet<String>,

    // Aggregates
    total_buy_volume: u64,  // lamports
    total_sell_volume: u64, // lamports
    buy_count: u32,
    sell_count: u32,

    // Dev behavior
    dev_sold: bool,
    /// Wall-clock time of the most recent pre-entry dev sell (delay resets on each sell).
    dev_sell_observed_at: Option<Instant>,

    // LP tracking
    lp_removed: bool,
    /// Geyser slot of the LP removal event when known (`MarketEvent.slot`).
    lp_removal_slot: Option<u64>,
    /// When LP removal was first observed (always set in `record_lp_removal`) — hard-exit fallback
    /// when `lp_removal_slot` is unknown (I-7: no RPC; wallclock only).
    lp_removal_observed_at: Option<Instant>,

    /// Highest trade slot observed on this pool row (for chain-window head fallback).
    max_trade_slot: u64,

    // Dump-recovery gating (pre-entry), chain-time aware (slot / head slot)
    dump_observed_slot: Option<u64>,
    recovery_started_slot: Option<u64>,
    /// Chain slot when price-trend recovery (flow + slope) first satisfied (Filter 5).
    price_trend_recovery_since_slot: Option<u64>,

    /// Explicit lifecycle state (replaces probe_sent_at, probe_filled_at, etc.)
    state: TrackerState,
    /// Reason for rejection (only set when state == Rejected)
    blacklist_reason: Option<String>,

    /// PR-D: last time this pool-scoped row observed any market trade (`record_trade`), for stale Discovery cleanup.
    last_trade_at: Option<Instant>,

    /// Last WAIT_* filter class used for metrics/log dedupe (hot path CPU / NATS backpressure).
    last_wait_filter_obs_key: Option<String>,

    /// Session trade-high: min `trade_tps` over valid-slot trades (best holder price, I-14).
    session_best_tps: f64,
}

#[derive(Debug, Clone, Copy)]
struct BuyerQualityStats {
    total_buy_volume_lamports: u64,
    unique_buyers: u32,
    top1_share: f64,
    top3_share: f64,
    repeat_buyer_ratio: f64,
}

impl TokenTracker {
    fn new(mint: &str, pool: &str, dex: &str, slot: u64, initial_liquidity: u64) -> Self {
        Self {
            mint: mint.to_string(),
            pool: pool.to_string(),
            dex: dex.to_string(),
            first_seen: Instant::now(),
            first_slot: slot,
            dev_wallet: None,
            initial_liquidity,
            dev_supply_pct: None,
            dex_pool_accounts: None,
            trades: Vec::new(),
            unique_buyers: HashSet::new(),
            unique_sellers: HashSet::new(),
            total_buy_volume: 0,
            total_sell_volume: 0,
            buy_count: 0,
            sell_count: 0,
            dev_sold: false,
            dev_sell_observed_at: None,
            lp_removed: false,
            lp_removal_slot: None,
            lp_removal_observed_at: None,
            max_trade_slot: 0,
            dump_observed_slot: None,
            recovery_started_slot: None,
            price_trend_recovery_since_slot: None,
            state: TrackerState::Discovery,
            blacklist_reason: None,
            last_trade_at: None,
            last_wait_filter_obs_key: None,
            session_best_tps: f64::MAX,
        }
    }

    #[cfg(test)]
    fn last_wait_filter_obs_key_for_test(&self) -> Option<&str> {
        self.last_wait_filter_obs_key.as_deref()
    }

    /// PR-D tests: Discovery/Validation-Zeile mit simuliertem Alter (`first_seen` / `last_trade_at`).
    #[cfg(test)]
    pub(crate) fn test_fixture_stale_discovery(
        mint: &str,
        pool: &str,
        state: TrackerState,
        first_seen_age: std::time::Duration,
        last_trade_age: Option<std::time::Duration>,
    ) -> Self {
        let mut t = Self::new(mint, pool, "raydium", 1, 10_000_000_000);
        t.first_seen = Instant::now() - first_seen_age;
        t.state = state;
        t.last_trade_at = last_trade_age.map(|d| Instant::now() - d);
        t
    }

    // =========================================================================
    // State transition helpers
    // =========================================================================

    /// Check if token is rejected (terminal failure state)
    fn is_rejected(&self) -> bool {
        matches!(self.state, TrackerState::Rejected)
    }

    /// Check if entry flow is complete (no more intents should be generated)
    fn is_entry_complete(&self) -> bool {
        matches!(
            self.state,
            TrackerState::Rejected | TrackerState::PositionOpenFull { .. }
        )
    }

    /// Transition to Rejected state with reason
    fn reject(&mut self, reason: impl Into<String>) {
        let reason = reason.into();
        self.last_wait_filter_obs_key = None;
        self.state = TrackerState::Rejected;
        self.blacklist_reason = Some(reason.clone());
        record_momentum_tracker_rejected(reason.as_str());
        warn!(
            mint = %self.mint,
            pool = %self.pool,
            dex = %self.dex,
            reason = %reason,
            trade_count = self.trades.len(),
            "Tracker rejected (terminal)"
        );
    }

    /// Check if tracker was previously not rejected (for metrics)
    fn was_not_rejected(&self) -> bool {
        !self.is_rejected()
    }

    /// Monotonic chain head for slot-window filters: max(latest Geyser slot seen by bot, this row).
    #[inline]
    /// Token age since discovery (I-16 chain-first; wallclock fallback when slots missing).
    fn token_age_secs(&self, chain_head_slot: u64) -> (u64, bool) {
        let hs = self.chain_head_for_filters(chain_head_slot);
        if self.first_slot > 0 && hs > self.first_slot {
            let age_secs = (hs - self.first_slot).saturating_mul(MOMENTUM_APPROX_SLOT_MS) / 1000;
            (age_secs, true)
        } else {
            (self.first_seen.elapsed().as_secs(), false)
        }
    }

    fn chain_head_for_filters(&self, ctx_head_slot: u64) -> u64 {
        let local = self.max_trade_slot.max(self.first_slot);
        if ctx_head_slot > 0 {
            ctx_head_slot.max(local)
        } else {
            local.max(1)
        }
    }

    fn update_session_best_tps_from_trade(
        &mut self,
        sol_amount: u64,
        token_amount: u64,
        trade_slot: u64,
    ) {
        if trade_slot == 0 {
            return;
        }
        let Some(tps) = trade_implied_tps(sol_amount, token_amount) else {
            return;
        };
        self.session_best_tps = if self.session_best_tps == f64::MAX {
            tps
        } else {
            self.session_best_tps.min(tps)
        };
    }

    /// Filter 5: trade-implied downtrend gate (Geyser trades only, I-7).
    fn price_trend_wait_reason(
        &mut self,
        config: &MomentumConfig,
        head_slot: u64,
    ) -> Option<String> {
        if !config.price_trend_filter_enabled || config.price_trend_window_secs == 0 {
            self.price_trend_recovery_since_slot = None;
            return None;
        }

        let window_slots = momentum_secs_to_slot_span(config.price_trend_window_secs);
        if window_slots == 0 {
            self.price_trend_recovery_since_slot = None;
            return None;
        }

        let hs = self.chain_head_for_filters(head_slot);
        let bucket_count = clamp_price_trend_bucket_count(config.price_trend_bucket_count);
        let half = (window_slots / 2).max(1);

        let mut samples: u32 = 0;
        let mut bucket_tps: Vec<Vec<f64>> = vec![Vec::new(); bucket_count];
        let mut early_half_tps: Vec<f64> = Vec::new();
        let mut late_half_tps: Vec<f64> = Vec::new();
        let mut buy_count: u32 = 0;
        let mut sell_count: u32 = 0;
        let mut buy_vol: u64 = 0;
        let mut sell_vol: u64 = 0;

        for t in &self.trades {
            if !momentum_trade_in_chain_slot_window(t.slot, hs, window_slots) {
                continue;
            }
            let Some(tps) = trade_implied_tps(t.sol_amount, t.token_amount) else {
                continue;
            };
            let Some(age) = trade_age_slots(t.slot, hs) else {
                continue;
            };

            samples = samples.saturating_add(1);
            if t.is_buy {
                buy_count = buy_count.saturating_add(1);
                buy_vol = buy_vol.saturating_add(t.sol_amount);
            } else {
                sell_count = sell_count.saturating_add(1);
                sell_vol = sell_vol.saturating_add(t.sol_amount);
            }

            let bucket_idx = price_trend_bucket_index(age, window_slots, bucket_count);
            bucket_tps[bucket_idx].push(tps);
            if age <= half {
                late_half_tps.push(tps);
            } else {
                early_half_tps.push(tps);
            }
        }

        if samples < config.price_trend_min_trades {
            self.price_trend_recovery_since_slot = None;
            return None;
        }

        let window_secs = config.price_trend_window_secs;
        let local_best = price_trend_local_best_per_bucket(&bucket_tps);
        let bucket_medians = price_trend_median_per_bucket(&bucket_tps);

        // Signal A: multi-bucket lower highs (no recovery exception)
        if let Some((rise, _satisfied, breaks)) = price_trend_lower_highs_signal_match(
            &local_best,
            config.price_trend_min_tps_rise_pct,
            config.price_trend_lower_highs_max_breaks,
        ) {
            let bucket_vals: Vec<String> = local_best
                .iter()
                .rev()
                .map(|v| match v {
                    Some(x) => format!("{x:.3e}"),
                    None => "-".to_string(),
                })
                .collect();
            let (_, total_pairs) = price_trend_lower_highs_stats(&local_best);
            self.price_trend_recovery_since_slot = None;
            return Some(format!(
                "WAIT_DOWNTREND: lower_highs buckets=[{}] breaks={breaks}/{total_pairs} (+{rise:.1}% tps rise, window={window_secs}s, trades={samples})",
                bucket_vals.join(",")
            ));
        }

        // Signal B: drawdown from session trade-high + falling short slope
        if self.session_best_tps == f64::MAX || !self.session_best_tps.is_finite() {
            return None;
        }

        let mut current_tps = None;
        for t in self.trades.iter().rev() {
            if t.slot == 0 || !momentum_trade_in_chain_slot_window(t.slot, hs, window_slots) {
                continue;
            }
            if let Some(tps) = trade_implied_tps(t.sol_amount, t.token_amount) {
                current_tps = Some(tps);
                break;
            }
        }
        let current_tps = current_tps?;

        let drawdown_pct =
            tokens_per_sol::drawdown_from_ath_pct(self.session_best_tps, current_tps);
        let early_med = median_f64(early_half_tps)?;
        let late_med = median_f64(late_half_tps)?;
        let short_slope_tps_rise = tps_rise_pct(early_med, late_med);

        let total = buy_count.saturating_add(sell_count);
        let buy_dominance = if total == 0 {
            0.0
        } else {
            buy_count as f64 / total as f64
        };
        let net_inflow = buy_vol as i128 - sell_vol as i128;

        let flow_ok = net_inflow >= config.price_trend_recovery_min_inflow_lamports as i128
            && buy_dominance >= config.price_trend_recovery_min_buy_dominance;
        let min_positive = config
            .price_trend_recovery_min_positive_buckets
            .min(bucket_count.saturating_sub(1) as u32) as usize;
        let slope_ok = price_trend_recovery_slope_ok(&bucket_medians, min_positive);
        let lower_highs_blocks_recovery = config.price_trend_recovery_requires_no_lower_highs
            && price_trend_lower_highs_raw_match(&local_best);
        let instant_recovery = flow_ok && slope_ok && !lower_highs_blocks_recovery;

        let in_drawdown_wait = drawdown_pct >= config.price_trend_max_drawdown_pct
            && short_slope_tps_rise >= config.price_trend_min_tps_rise_pct;
        if in_drawdown_wait {
            if instant_recovery {
                self.price_trend_recovery_since_slot.get_or_insert(hs);
            } else {
                self.price_trend_recovery_since_slot = None;
            }
        } else {
            self.price_trend_recovery_since_slot = None;
        }

        let recovery_min_slots = momentum_secs_to_slot_span(config.price_trend_recovery_min_secs);
        let duration_ok = self
            .price_trend_recovery_since_slot
            .is_some_and(|rs| hs.saturating_sub(rs) >= recovery_min_slots);
        let recovery_ok = instant_recovery && duration_ok;

        if in_drawdown_wait && !recovery_ok {
            let recovery_detail = if !flow_ok || !slope_ok {
                "flow/slope not confirmed".to_string()
            } else if lower_highs_blocks_recovery {
                "blocked by lower_highs structure".to_string()
            } else if !duration_ok {
                format!(
                    "recovery stabilizing ({} / {} slots, ~{}s)",
                    hs.saturating_sub(self.price_trend_recovery_since_slot.unwrap_or(hs)),
                    recovery_min_slots,
                    config.price_trend_recovery_min_secs
                )
            } else {
                "recovery not confirmed".to_string()
            };
            return Some(format!(
                "WAIT_DOWNTREND: drawdown={drawdown_pct:.1}% from session_high, slope=+{short_slope_tps_rise:.1}% tps ({recovery_detail}, dom={:.0}%, inflow={:.2} SOL)",
                buy_dominance * 100.0,
                net_inflow as f64 / 1_000_000_000.0
            ));
        }

        None
    }

    fn dump_recovery_window_stats_at(
        &self,
        config: &MomentumConfig,
        head_slot: u64,
    ) -> Option<(f64, i128, u32)> {
        if config.dump_recovery_window_secs == 0 {
            return None;
        }

        let window_slots = momentum_secs_to_slot_span(config.dump_recovery_window_secs);
        let hs = self.chain_head_for_filters(head_slot);
        let mut buy_count: u32 = 0;
        let mut sell_count: u32 = 0;
        let mut buy_vol: u64 = 0;
        let mut sell_vol: u64 = 0;

        for t in self
            .trades
            .iter()
            .filter(|t| momentum_trade_in_chain_slot_window(t.slot, hs, window_slots))
        {
            if t.is_buy {
                buy_count = buy_count.saturating_add(1);
                buy_vol = buy_vol.saturating_add(t.sol_amount);
            } else {
                sell_count = sell_count.saturating_add(1);
                sell_vol = sell_vol.saturating_add(t.sol_amount);
            }
        }

        let total = buy_count.saturating_add(sell_count);
        if total == 0 {
            return None;
        }

        let buy_dominance = buy_count as f64 / total as f64;
        let net_inflow = buy_vol as i128 - sell_vol as i128;
        Some((buy_dominance, net_inflow, total))
    }

    fn dump_recovery_wait_reason(
        &mut self,
        config: &MomentumConfig,
        head_slot: u64,
    ) -> Option<String> {
        let (buy_dominance, net_inflow, samples) =
            self.dump_recovery_window_stats_at(config, head_slot)?;

        // Require a minimum sample size to avoid flapping on tiny windows.
        let min_samples = config.min_unique_buyers.max(5);
        if samples < min_samples {
            return None;
        }

        let window_slots = momentum_secs_to_slot_span(config.dump_recovery_window_secs);
        let hs = self.chain_head_for_filters(head_slot);

        // Expire old dump flags once the chain window rolled past the observation slot.
        if let Some(ds) = self.dump_observed_slot {
            if hs.saturating_sub(ds) > window_slots {
                self.dump_observed_slot = None;
                self.recovery_started_slot = None;
            }
        }

        // Dump detected = net outflow in the recovery window.
        if net_inflow < 0 {
            self.dump_observed_slot = Some(hs);
            self.recovery_started_slot = None;
            return Some(format!(
                "WAIT_CONFIRMATION: dump detected (net_inflow={:.2} SOL over ~{}s / {} slots @ head_slot {})",
                net_inflow as f64 / 1_000_000_000.0,
                config.dump_recovery_window_secs,
                window_slots,
                hs
            ));
        }

        // If we haven't observed a dump, don't gate on recovery.
        self.dump_observed_slot?;

        let recovery_ok = buy_dominance >= config.dump_recovery_min_buy_dominance
            && net_inflow >= config.dump_recovery_min_net_inflow_lamports as i128;

        if !recovery_ok {
            self.recovery_started_slot = None;
            return Some(format!(
                "WAIT_CONFIRMATION: dump recovery not confirmed yet (dom={:.0}% < {:.0}%, inflow={:.2} SOL < {:.2} SOL)",
                buy_dominance * 100.0,
                config.dump_recovery_min_buy_dominance * 100.0,
                net_inflow as f64 / 1_000_000_000.0,
                config.dump_recovery_min_net_inflow_lamports as f64 / 1_000_000_000.0
            ));
        }

        let recovery_min_slots = momentum_secs_to_slot_span(config.dump_recovery_min_recovery_secs);
        let rs = self.recovery_started_slot.get_or_insert(hs);
        if hs.saturating_sub(*rs) < recovery_min_slots {
            return Some(format!(
                "WAIT_CONFIRMATION: dump recovery stabilizing ({} / {} slots, ~{}s)",
                hs.saturating_sub(*rs),
                recovery_min_slots,
                config.dump_recovery_min_recovery_secs
            ));
        }

        // Recovery confirmed.
        self.dump_observed_slot = None;
        self.recovery_started_slot = None;
        None
    }

    fn dev_sell_revalidation_wait_reason(
        &self,
        config: &MomentumConfig,
        now: Instant,
    ) -> Option<String> {
        if self.dev_sell_observed_at.is_none() || !self.dev_sold {
            return None;
        }
        let observed = self.dev_sell_observed_at?;
        let since = now.duration_since(observed).as_secs();
        if since < config.dev_sell_revalidation_delay_secs {
            Some(format!(
                "WAIT_DEV_SELL_REVALIDATION: delay {}/{}s",
                since, config.dev_sell_revalidation_delay_secs
            ))
        } else {
            None
        }
    }

    /// Record a trade event (`trade_slot`: Geyser tx slot from `MarketEvent.slot`, `0` if unknown).
    #[allow(clippy::too_many_arguments)] // Trade fields + slot + config — explicit for tracker clarity
    fn record_trade(
        &mut self,
        trader: &str,
        is_buy: bool,
        sol_amount: u64,
        token_amount: u64,
        signature: &str,
        trade_slot: u64,
        config: &MomentumConfig,
    ) {
        if trade_slot > 0 {
            self.max_trade_slot = self.max_trade_slot.max(trade_slot);
            self.update_session_best_tps_from_trade(sol_amount, token_amount, trade_slot);
        }
        let trade = TradeEvent {
            timestamp: Instant::now(),
            slot: trade_slot,
            trader: trader.to_string(),
            is_buy,
            sol_amount,
            token_amount,
            signature: signature.to_string(),
        };

        if is_buy {
            self.unique_buyers.insert(trader.to_string());
            self.total_buy_volume += sol_amount;
            self.buy_count += 1;
        } else {
            self.unique_sellers.insert(trader.to_string());
            self.total_sell_volume += sol_amount;
            self.sell_count += 1;

            // Check for large dump
            if sol_amount > config.max_single_dump_lamports {
                self.reject(format!(
                    "Large dump detected: {} SOL",
                    sol_amount as f64 / 1_000_000_000.0
                ));
            }
        }

        // Dev behavior tracking (pre-entry: wait + revalidation delay, never hard-reject)
        if let Some(ref dev) = self.dev_wallet {
            if trader == dev && !is_buy {
                self.dev_sold = true;
                self.dev_sell_observed_at = Some(Instant::now());
                warn!(
                    mint = %self.mint,
                    dev_sell_slot = trade_slot,
                    first_slot = self.first_slot,
                    "Dev sold pre-entry — revalidation delay started"
                );
            }
        }

        self.trades.push(trade);
        self.last_trade_at = Some(Instant::now());
    }

    fn last_trade_ratio(&self) -> Option<(u64, u64)> {
        // Return (sol_lamports, token_amount_raw) from the most recent trade that has both.
        self.trades
            .iter()
            .rev()
            .find(|t| t.sol_amount > 0 && t.token_amount > 0)
            .map(|t| (t.sol_amount, t.token_amount))
    }

    fn micro_buy_stats_at(&self, config: &MomentumConfig, head_slot: u64) -> (u32, u32, f64) {
        let buyer_window_slots = momentum_secs_to_slot_span(config.buyer_window_secs);
        let hs = self.chain_head_for_filters(head_slot);

        let mut total_buys: u32 = 0;
        let mut small_buys: u32 = 0;

        for trade in self.trades.iter().filter(|t| {
            t.is_buy && momentum_trade_in_chain_slot_window(t.slot, hs, buyer_window_slots)
        }) {
            total_buys = total_buys.saturating_add(1);
            if trade.sol_amount < config.min_trade_size_lamports {
                small_buys = small_buys.saturating_add(1);
            }
        }

        let ratio = if total_buys == 0 {
            0.0
        } else {
            (small_buys as f64) / (total_buys as f64)
        };

        (total_buys, small_buys, ratio)
    }

    fn buyer_quality_stats_at(&self, config: &MomentumConfig, head_slot: u64) -> BuyerQualityStats {
        let buyer_window_slots = momentum_secs_to_slot_span(config.buyer_window_secs);
        let hs = self.chain_head_for_filters(head_slot);

        let mut per_wallet: HashMap<String, (u64, u32)> = HashMap::new();
        let mut total_buy_volume_lamports: u64 = 0;

        for trade in self.trades.iter().filter(|t| {
            t.is_buy && momentum_trade_in_chain_slot_window(t.slot, hs, buyer_window_slots)
        }) {
            let entry = per_wallet.entry(trade.trader.clone()).or_insert((0, 0));
            entry.0 = entry.0.saturating_add(trade.sol_amount);
            entry.1 = entry.1.saturating_add(1);
            total_buy_volume_lamports = total_buy_volume_lamports.saturating_add(trade.sol_amount);
        }

        let unique_buyers = per_wallet.len() as u32;
        if total_buy_volume_lamports == 0 || unique_buyers == 0 {
            return BuyerQualityStats {
                total_buy_volume_lamports,
                unique_buyers,
                top1_share: 0.0,
                top3_share: 0.0,
                repeat_buyer_ratio: 0.0,
            };
        }

        let mut volumes: Vec<u64> = per_wallet.values().map(|(v, _)| *v).collect();
        volumes.sort_unstable_by(|a, b| b.cmp(a));

        let top1 = volumes.first().copied().unwrap_or(0);
        let top3 = volumes.iter().take(3).copied().sum::<u64>();

        let repeat_buyers = per_wallet.values().filter(|(_, c)| *c >= 2).count() as u32;

        BuyerQualityStats {
            total_buy_volume_lamports,
            unique_buyers,
            top1_share: (top1 as f64) / (total_buy_volume_lamports as f64),
            top3_share: (top3 as f64) / (total_buy_volume_lamports as f64),
            repeat_buyer_ratio: (repeat_buyers as f64) / (unique_buyers as f64),
        }
    }

    /// Record LP removal (`slot`: `MarketEvent.slot` when known).
    fn record_lp_removal(&mut self, slot: Option<u64>) {
        self.lp_removed = true;
        // Treat slot 0 as unknown so wallclock LP exit path is not blocked.
        self.lp_removal_slot = slot.filter(|&s| s > 0).or(self.lp_removal_slot);
        self.lp_removal_observed_at.get_or_insert_with(Instant::now);
        self.reject("REJECT_LP_REMOVED");
        warn!(mint = %self.mint, lp_removal_slot = ?self.lp_removal_slot, "LP removed - blacklisting");
    }

    /// Set dev wallet and supply percentage
    fn set_dev_info(&mut self, dev_wallet: &str, supply_pct: f64, config: &MomentumConfig) {
        self.dev_wallet = Some(dev_wallet.to_string());
        self.dev_supply_pct = Some(supply_pct);

        if supply_pct > config.max_dev_supply_pct {
            self.reject(format!(
                "REJECT_DEV_SUPPLY_TOO_HIGH: {:.1}% > {:.1}%",
                supply_pct, config.max_dev_supply_pct
            ));
            warn!(mint = %self.mint, supply_pct, "Dev supply too high - blacklisting");
        }
    }

    /// Calculate metrics for strategy decision (chain-slot windows vs `head_slot`, burst-safe).
    fn calculate_metrics(&self, config: &MomentumConfig, head_slot: u64) -> TokenMetrics {
        let hs = self.chain_head_for_filters(head_slot);
        let buyer_window_slots = momentum_secs_to_slot_span(config.buyer_window_secs);
        let inflow_window_slots = momentum_secs_to_slot_span(config.inflow_window_secs);

        let recent_buyers: HashSet<_> = self
            .trades
            .iter()
            .filter(|t| {
                t.is_buy && momentum_trade_in_chain_slot_window(t.slot, hs, buyer_window_slots)
            })
            .map(|t| t.trader.clone())
            .collect();

        let (recent_buy_vol, recent_sell_vol) = self
            .trades
            .iter()
            .filter(|t| momentum_trade_in_chain_slot_window(t.slot, hs, inflow_window_slots))
            .fold((0u64, 0u64), |(b, s), t| {
                if t.is_buy {
                    (b + t.sol_amount, s)
                } else {
                    (b, s + t.sol_amount)
                }
            });

        let mut buy_count_in_window = 0u32;
        let mut sell_count_in_window = 0u32;
        for t in self
            .trades
            .iter()
            .filter(|t| momentum_trade_in_chain_slot_window(t.slot, hs, buyer_window_slots))
        {
            if t.is_buy {
                buy_count_in_window = buy_count_in_window.saturating_add(1);
            } else {
                sell_count_in_window = sell_count_in_window.saturating_add(1);
            }
        }

        let total_in_window = buy_count_in_window.saturating_add(sell_count_in_window);
        let buy_dominance = if total_in_window > 0 {
            buy_count_in_window as f64 / total_in_window as f64
        } else {
            0.0
        };

        let trades_per_min =
            trades_per_min_in_buyer_window(&self.trades, config.buyer_window_secs, hs);

        TokenMetrics {
            unique_buyers_in_window: recent_buyers.len() as u32,
            trades_per_min,
            buy_dominance,
            net_sol_inflow: recent_buy_vol.saturating_sub(recent_sell_vol),
            lp_removed: self.lp_removed,
            initial_liquidity_sol: self.initial_liquidity as f64 / 1_000_000_000.0,
        }
    }

    /// Emit FILTER_REJECTED_* + warn for a repeating non-terminal WAIT; dedupe identical wait classes.
    fn record_wait_filter_rejection_and_return(
        &mut self,
        category_counter: &std::sync::atomic::AtomicU64,
        reason: String,
        dedupe_class: &'static str,
    ) -> (bool, String) {
        if self.last_wait_filter_obs_key.as_deref() == Some(dedupe_class) {
            trace!(
                mint = %self.mint,
                pool = %self.pool,
                dex = %self.dex,
                reason = %reason,
                "Filter wait (deduped observability)"
            );
            return (false, reason);
        }
        self.last_wait_filter_obs_key = Some(dedupe_class.to_string());
        category_counter.fetch_add(1, Ordering::Relaxed);
        FILTER_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
        warn!(
            mint = %self.mint,
            pool = %self.pool,
            dex = %self.dex,
            reason = %reason,
            "🚫 Filter rejected"
        );
        (false, reason)
    }

    /// Check all 4 filters and return if we should trade.
    /// `chain_head_slot`: latest Geyser slot seen by momentum (trades + `SlotUpdate`); drives burst-safe windows.
    fn should_generate_intent(
        &mut self,
        config: &MomentumConfig,
        mint_info: Option<&MintInfo>,
        chain_head_slot: u64,
        wall_now: Instant,
    ) -> (bool, String) {
        // Already in terminal state
        if self.is_entry_complete() {
            return (
                false,
                self.blacklist_reason
                    .clone()
                    .unwrap_or_else(|| "Entry complete".to_string()),
            );
        }

        // Token safety gates: if enabled, we must have mint info and authorities must be safe.
        if config.require_mint_authority_renounced || config.require_freeze_authority_none {
            let Some(info) = mint_info else {
                return (false, "WAIT_MINT_INFO".to_string());
            };

            if config.require_mint_authority_renounced && info.mint_authority.is_some() {
                self.reject("REJECT_MINT_AUTHORITY_NOT_RENOUNCED");
                return (false, "REJECT_MINT_AUTHORITY_NOT_RENOUNCED".to_string());
            }

            if config.require_freeze_authority_none && info.freeze_authority.is_some() {
                self.reject("REJECT_FREEZE_AUTHORITY_SET");
                return (false, "REJECT_FREEZE_AUTHORITY_SET".to_string());
            }
        }

        let metrics = self.calculate_metrics(config, chain_head_slot);

        // Observability: slot span actually covered by configured windows (one line per pass-through).
        let hs = self.chain_head_for_filters(chain_head_slot);
        let bw = momentum_secs_to_slot_span(config.buyer_window_secs);
        let iw = momentum_secs_to_slot_span(config.inflow_window_secs);
        let cover = bw.max(iw);
        let (mut smin, mut smax) = (u64::MAX, 0u64);
        for t in &self.trades {
            if t.slot > 0 && momentum_trade_in_chain_slot_window(t.slot, hs, cover) {
                smin = smin.min(t.slot);
                smax = smax.max(t.slot);
            }
        }
        let chain_obs = if smin != u64::MAX {
            format!("trade_slots~{}..{} head={}", smin, smax, hs)
        } else {
            format!("trade_slots=none head={}", hs)
        };

        // Filter 1: Liquidity Check
        if metrics.initial_liquidity_sol < config.early_min_liquidity_sol {
            let reason = format!(
                "WAIT_INSUFFICIENT_LIQUIDITY: liq {:.2} SOL < {:.2} SOL",
                metrics.initial_liquidity_sol, config.early_min_liquidity_sol
            );
            return self.record_wait_filter_rejection_and_return(
                &FILTER_REJECTED_LIQUIDITY,
                reason,
                "WAIT_INSUFFICIENT_LIQUIDITY",
            );
        }

        // Filter 1b: LP removal
        if metrics.lp_removed {
            FILTER_REJECTED_LIQUIDITY.fetch_add(1, Ordering::Relaxed);
            FILTER_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
            self.reject("REJECT_LP_REMOVED");
            warn!(mint = %self.mint, pool = %self.pool, dex = %self.dex, "🚫 Filter rejected: LP_REMOVED (blacklisted)");
            return (false, "REJECT_LP_REMOVED".to_string());
        }

        // Filter 1c: Token age (skip launch-minute rugs; chain-slot first)
        if config.min_token_age_secs > 0 {
            let (age_secs, chain_time) = self.token_age_secs(chain_head_slot);
            if age_secs < config.min_token_age_secs {
                let time_note = if chain_time {
                    "~chain-time"
                } else {
                    "~wallclock"
                };
                let reason = format!(
                    "WAIT_TOKEN_AGE: token age {}s < {}s (first_slot={}, head={}, {})",
                    age_secs, config.min_token_age_secs, self.first_slot, hs, time_note
                );
                return self.record_wait_filter_rejection_and_return(
                    &FILTER_REJECTED_TOKEN_AGE,
                    reason,
                    "WAIT_TOKEN_AGE",
                );
            }
        }

        // Dev-sell revalidation delay (pre-entry WAIT; after delay standard filters decide on fresh window data)
        if let Some(wait) = self.dev_sell_revalidation_wait_reason(config, wall_now) {
            return self.record_wait_filter_rejection_and_return(
                &FILTER_REJECTED_DEV_BEHAVIOR,
                wait,
                "WAIT_DEV_SELL_REVALIDATION",
            );
        }

        // Filter 2: Buyer Velocity
        if metrics.unique_buyers_in_window < config.min_unique_buyers {
            let reason = format!(
                "WAIT_BUYER_WINDOW: buyers {} < {}",
                metrics.unique_buyers_in_window, config.min_unique_buyers
            );
            return self.record_wait_filter_rejection_and_return(
                &FILTER_REJECTED_VELOCITY,
                reason,
                "WAIT_BUYER_WINDOW",
            );
        }

        if metrics.trades_per_min < config.min_trades_per_min {
            let reason = format!(
                "WAIT_BUYER_WINDOW: trades_per_min {:.1} < {:.1}",
                metrics.trades_per_min, config.min_trades_per_min
            );
            return self.record_wait_filter_rejection_and_return(
                &FILTER_REJECTED_VELOCITY,
                reason,
                "WAIT_BUYER_WINDOW",
            );
        }

        if metrics.buy_dominance < config.min_buy_dominance {
            let reason = format!(
                "WAIT_BUYER_WINDOW: buy dominance {:.0}% < {:.0}%",
                metrics.buy_dominance * 100.0,
                config.min_buy_dominance * 100.0
            );
            return self.record_wait_filter_rejection_and_return(
                &FILTER_REJECTED_VELOCITY,
                reason,
                "WAIT_BUYER_WINDOW",
            );
        }

        // Filter 2c: Micro-buy spam (anti-bot)
        let (total_buys, small_buys, small_buy_ratio) =
            self.micro_buy_stats_at(config, chain_head_slot);
        let min_samples = config.min_unique_buyers.max(5);
        if total_buys >= min_samples && small_buy_ratio > config.small_buy_ratio_cap {
            FILTER_REJECTED_BUYER_QUALITY.fetch_add(1, Ordering::Relaxed);
            FILTER_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
            let reason = format!(
                "REJECT_MICRO_BUY_SPAM: small-buy ratio {:.0}% > {:.0}% (small={}, total={}, min_trade={:.4} SOL)",
                small_buy_ratio * 100.0,
                config.small_buy_ratio_cap * 100.0,
                small_buys,
                total_buys,
                config.min_trade_size_lamports as f64 / 1_000_000_000.0
            );
            self.reject(reason.clone());
            return (false, reason);
        }

        // Filter 2b: Buyer Quality (anti-bot / concentration) — same min_samples guard as micro-buy spam
        // so early pumps with few buyers cannot false-reject and silence entry eval (PR170).
        let bq = self.buyer_quality_stats_at(config, chain_head_slot);
        let buyer_quality_min_samples = config.min_unique_buyers.max(5);

        if bq.unique_buyers >= buyer_quality_min_samples {
            if bq.top1_share > config.top1_buyer_share_cap {
                FILTER_REJECTED_BUYER_QUALITY.fetch_add(1, Ordering::Relaxed);
                FILTER_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
                let reason = format!(
                    "REJECT_BOT_CONCENTRATION: top1 share {:.0}% > {:.0}% (buyers={}, buy_vol={:.2} SOL)",
                    bq.top1_share * 100.0,
                    config.top1_buyer_share_cap * 100.0,
                    bq.unique_buyers,
                    bq.total_buy_volume_lamports as f64 / 1_000_000_000.0
                );
                self.reject(reason.clone());
                return (false, reason);
            }

            if bq.top3_share > config.top3_buyer_share_cap {
                FILTER_REJECTED_BUYER_QUALITY.fetch_add(1, Ordering::Relaxed);
                FILTER_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
                let reason = format!(
                    "REJECT_BOT_CONCENTRATION: top3 share {:.0}% > {:.0}% (buyers={}, buy_vol={:.2} SOL)",
                    bq.top3_share * 100.0,
                    config.top3_buyer_share_cap * 100.0,
                    bq.unique_buyers,
                    bq.total_buy_volume_lamports as f64 / 1_000_000_000.0
                );
                self.reject(reason.clone());
                return (false, reason);
            }

            if bq.repeat_buyer_ratio < config.repeat_buyer_min_ratio {
                FILTER_REJECTED_BUYER_QUALITY.fetch_add(1, Ordering::Relaxed);
                FILTER_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
                let reason = format!(
                    "REJECT_BOT_CONCENTRATION: repeat buyer ratio {:.0}% < {:.0}% (buyers={}, buy_vol={:.2} SOL)",
                    bq.repeat_buyer_ratio * 100.0,
                    config.repeat_buyer_min_ratio * 100.0,
                    bq.unique_buyers,
                    bq.total_buy_volume_lamports as f64 / 1_000_000_000.0
                );
                self.reject(reason.clone());
                return (false, reason);
            }
        }

        // Filter 3: SOL Inflow
        if metrics.net_sol_inflow < config.min_sol_inflow_lamports {
            let reason = format!(
                "WAIT_BUYER_WINDOW: net inflow {:.2} SOL < {:.2} SOL",
                metrics.net_sol_inflow as f64 / 1_000_000_000.0,
                config.min_sol_inflow_lamports as f64 / 1_000_000_000.0
            );
            return self.record_wait_filter_rejection_and_return(
                &FILTER_REJECTED_INFLOW,
                reason,
                "WAIT_NET_SOL_INFLOW",
            );
        }

        // Filter 5: Price trend / downtrend gate (trade-implied tps, Geyser-only)
        if let Some(wait_reason) = self.price_trend_wait_reason(config, chain_head_slot) {
            return self.record_wait_filter_rejection_and_return(
                &FILTER_REJECTED_DOWNTREND,
                wait_reason,
                "WAIT_DOWNTREND",
            );
        }

        // Filter 3b: Dump-recovery gating (WAIT until recovery confirms after a dump)
        if let Some(wait_reason) = self.dump_recovery_wait_reason(config, chain_head_slot) {
            return (false, wait_reason);
        }

        // All filters passed!
        self.last_wait_filter_obs_key = None;
        FILTER_PASSED_TOTAL.fetch_add(1, Ordering::Relaxed);
        let reason = format!(
            "All filters passed: liq={:.1}SOL, buyers={}, vel={:.0} trades/min (chain-time), dom={:.0}%, inflow={:.1}SOL, bq(top1={:.0}%,top3={:.0}%,repeat={:.0}%,vol={:.2}SOL) | {}",
            metrics.initial_liquidity_sol,
            metrics.unique_buyers_in_window,
            metrics.trades_per_min,
            metrics.buy_dominance * 100.0,
            metrics.net_sol_inflow as f64 / 1_000_000_000.0,
            bq.top1_share * 100.0,
            bq.top3_share * 100.0,
            bq.repeat_buyer_ratio * 100.0,
            bq.total_buy_volume_lamports as f64 / 1_000_000_000.0,
            chain_obs
        );

        (true, reason)
    }
}

/// Calculated metrics for logging/decisions
#[derive(Debug)]
struct TokenMetrics {
    unique_buyers_in_window: u32,
    trades_per_min: f64,
    buy_dominance: f64,
    net_sol_inflow: u64,
    lp_removed: bool,
    initial_liquidity_sol: f64,
}

/// Cached mint metadata for risk gating.
#[derive(Debug, Clone)]
struct MintInfo {
    token_program: String,
    decimals: u8,
    supply: u64,
    mint_authority: Option<String>,
    freeze_authority: Option<String>,
    last_updated: Instant,
}

/// Cached info about a pending intent (awaiting execution result)
#[derive(Debug, Clone)]
struct PendingIntent {
    mint: String,
    pool: String,
    dex: String,
    side: TradeSide,
    /// For BUY intents, indicates probe vs scale-in.
    entry_kind: Option<EntryKind>,
    sol_amount: u64,   // For BUY: SOL invested
    token_amount: u64, // For SELL: tokens to sell
    created_at: Instant,
}

/// Parameters for registering a pending BUY lifecycle entry after JetStream publish.
struct PendingBuyPublishMeta<'a> {
    intent_id: &'a str,
    mint: &'a str,
    pool: &'a str,
    dex: &'a str,
    intended_sol: u64,
    entry_kind: Option<EntryKind>,
    signal_slot: u64,
    slot_seen_at_ms: u64,
    creator: Option<String>,
    token_program: Option<String>,
}

/// Geyser bonding-curve snapshot for a mint (merged slot-/ts-monotonic).
#[derive(Debug, Clone)]
struct CachedBondingCurveState {
    progress_bps: u32,
    /// Preserved for migration / exit routing extensions; progress alone drives `BONDING_CURVE_EXIT`.
    #[allow(dead_code)]
    complete: bool,
    slot: u64,
    ts_unix_ms: u64,
}

/// Scope B: last observed PumpFun bonding-curve **complete** (migration) signal for a mint.
/// Slot-/timestamp-monotonic merge only; complements LivePoolCache when rows are not yet present.
#[derive(Debug, Clone)]
struct CachedPumpfunMigrationCompleteEvidence {
    slot: u64,
    ts_unix_ms: u64,
}

/// Scope B: reserve-derived `tokens_per_sol` hint from `PoolCacheUpdate` for a `(token_mint, pool)`.
/// Applied only with I-13 pool match and the same slot gates as `update_position_price`.
#[derive(Debug, Clone)]
struct CachedPoolReservePriceHint {
    pool_address: String,
    #[allow(dead_code)]
    dex: String,
    tokens_per_sol: f64,
    slot: u64,
    ts_unix_ms: u64,
}

/// Pending BUY lifecycle (not a wallet position): holds bonding/migration state between
/// successful intent publish and ExecutionResult / `open_position`.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct PendingBuyEntry {
    mint: String,
    pool: String,
    dex: String,
    entry_kind: Option<EntryKind>,
    intended_sol: u64,
    intent_id: String,
    signal_slot: u64,
    slot_seen_at_ms: u64,
    creator: Option<String>,
    token_program: Option<String>,
}

/// Returns true if `(new_slot, new_ts)` is strictly after `(old_slot, old_ts)` for Geyser ordering.
fn bonding_geyser_observation_is_newer(
    new_slot: u64,
    new_ts: u64,
    old_slot: u64,
    old_ts: u64,
) -> bool {
    match new_slot.cmp(&old_slot) {
        std::cmp::Ordering::Greater => true,
        std::cmp::Ordering::Less => false,
        std::cmp::Ordering::Equal => new_ts > old_ts,
    }
}

/// `PoolCacheUpdate` reserve marks require exactly one WSOL leg so reserves map to token vs SOL (I-14).
#[inline]
fn pool_cache_has_exactly_one_wsol_leg(base_mint: &str, quote_mint: &str) -> bool {
    const WSOL: &str = "So11111111111111111111111111111111111111112";
    let base = base_mint == WSOL;
    let quote = quote_mint == WSOL;
    base ^ quote
}

struct OpenPositionParams<'a> {
    mint: &'a str,
    pool: &'a str,
    dex: &'a str,
    entry_price: f64,
    token_decimals: u8,
    token_amount: u64,
    sol_invested: u64,
    token_program: Option<String>,
    /// PumpFun creator (for BC-SELL when dex == pumpfun)
    creator: Option<String>,
    /// Confirmed landing slot of the BUY tx (`ExecutionResult.confirmed_slot`).
    entry_confirmed_slot: u64,
    /// Latest PumpFun bonding progress seen on Geyser before/at confirm (optional).
    initial_bonding: Option<CachedBondingCurveState>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EntryKind {
    Probe,
    ScaleIn,
}

type PendingPoolAccounts = (String, String, Vec<String>);

#[derive(Debug, Clone)]
struct EntrySignal {
    mint: String,
    pool: String,
    dex: String,
    sol_amount: u64,
    kind: EntryKind,
    reason: String,
}

/// Bounded queue for offloading TradeIntent JSONL + JetStream publish from the event loop.
const INTENT_PUBLISH_QUEUE_CAP: usize = 1024;
const INTENT_PUBLISH_ENQUEUE_TIMEOUT: Duration = Duration::from_millis(50);

static MOMENTUM_INTENT_PUBLISH_BACKPRESSURE_TOTAL: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

#[derive(Debug, Clone)]
enum IntentPublishPostAction {
    Exit {
        mint: String,
        source_event_ts_unix_ms: Option<u64>,
        slot_seen_at_ms: u64,
    },
    Buy {
        entry_signal: EntrySignal,
        effective_pool: String,
        effective_dex: String,
        token_program: Option<String>,
        signal_slot: u64,
        slot_seen_at_ms: u64,
    },
}

struct IntentPublishJob {
    intent: TradeIntent,
    post_action: IntentPublishPostAction,
}

type IntentPublishOrderRecorder = Arc<parking_lot::Mutex<Vec<String>>>;

/// Information about a pool for multi-pool routing
#[derive(Debug, Clone)]
struct PoolInfo {
    pool_address: String,
    dex: String,
    dex_pool_accounts: Option<Vec<String>>,
    #[allow(dead_code)] // Useful for debugging/forensics
    first_seen_slot: u64,
    last_trade_slot: u64,
    last_trade_ratio: Option<f64>, // SOL per token (for quotes)
    last_updated: std::time::Instant,
    /// PumpFun bonding curve complete flag (None = not PumpFun or unknown)
    /// When Some(true), the curve has migrated to PumpSwap AMM and SELLs
    /// through this pool will fail with Custom(6023).
    bonding_curve_complete: Option<bool>,
    /// Number of consecutive SELL failures on this pool (reset on success)
    sell_fail_count: u32,
    /// Timestamp of the last SELL failure (for cooldown calculation)
    last_sell_fail_at: Option<std::time::Instant>,
}

impl PoolInfo {
    fn new(pool_address: String, dex: String, slot: u64) -> Self {
        Self {
            pool_address,
            dex,
            dex_pool_accounts: None,
            first_seen_slot: slot,
            last_trade_slot: slot,
            last_trade_ratio: None,
            last_updated: std::time::Instant::now(),
            bonding_curve_complete: None,
            sell_fail_count: 0,
            last_sell_fail_at: None,
        }
    }
}

/// Runtime context for momentum-bot
struct MomentumContext {
    run_id: String,
    /// P1: Config in RwLock for runtime hot-reload
    config: parking_lot::RwLock<MomentumConfig>,
    nats: Option<NatsClient>,
    jsonl_writer: QueuedJsonlWriter,
    /// Held in `Option` so shutdown can `take()` the sender and drain the worker without the
    /// worker's `Arc<Self>` keeping the channel open.
    intent_publish_tx: parking_lot::Mutex<Option<mpsc::Sender<IntentPublishJob>>>,
    intent_counter: std::sync::atomic::AtomicU64,
    /// Track known pools (pool_address -> first_seen_slot)
    pool_first_seen: parking_lot::RwLock<std::collections::HashMap<String, u64>>,
    /// Pump.fun dev wallet info can arrive before PoolCreated.
    /// Store it by mint and apply once the TokenTracker exists.
    pending_dev_info: parking_lot::RwLock<HashMap<String, (String, f64)>>,
    /// DEX pool accounts can arrive before PoolCreated.
    /// Store by token mint and apply once the TokenTracker exists.
    pending_pool_accounts: parking_lot::RwLock<HashMap<String, PendingPoolAccounts>>,
    /// Mint metadata (authority/supply/decimals) can arrive independently of pool creation.
    mint_infos: parking_lot::RwLock<HashMap<String, MintInfo>>,
    /// Token trackers for strategy filters — **pool-scoped** key `(mint, pool)` via
    /// [`MomentumContext::tracker_storage_key`] so entry signals cannot reference a stale pool
    /// after migration / multi-pool activity on the same mint.
    token_trackers: parking_lot::RwLock<HashMap<String, TokenTracker>>,
    /// Mint → pool-scoped tracker storage keys for same-mint iteration without scanning all rows.
    tracker_keys_by_mint: parking_lot::RwLock<HashMap<String, BTreeSet<String>>>,
    /// Tracker rows that need entry evaluation on the next strategy tick(s).
    dirty_entry_tracker_keys: parking_lot::RwLock<BTreeSet<String>>,
    /// Pool-scoped (`mint\x1fpool`) cooldown after PumpFun BUY failed: missing creator — avoids dirty→signal hot loop (I‑12).
    pumpfun_buy_missing_creator_suppress_until: parking_lot::RwLock<HashMap<String, Instant>>,
    /// Position trackers for exit strategy (mint -> position)
    positions: parking_lot::RwLock<HashMap<String, PositionTracker>>,
    /// Pending intents awaiting execution results (intent_id -> PendingIntent)
    pending_intents: parking_lot::RwLock<HashMap<String, PendingIntent>>,
    /// Latest `BondingCurveProgress` per mint (Geyser slot/ts monotonic).
    latest_bonding_by_mint: parking_lot::RwLock<HashMap<String, CachedBondingCurveState>>,
    /// Scope B: latest PumpFun migration (`complete`) evidence per mint (Geyser slot/ts monotonic).
    latest_pumpfun_migration_complete_by_mint:
        parking_lot::RwLock<HashMap<String, CachedPumpfunMigrationCompleteEvidence>>,
    /// Scope B: latest reserve-derived mark `tokens_per_sol` per `(token_mint, pool)` from JetStream
    /// `PoolCacheUpdate` (slot/ts monotonic). Not a wallet or capital truth — only for position marks.
    latest_pool_reserve_price_hint_by_mint_pool:
        parking_lot::RwLock<HashMap<(String, String), CachedPoolReservePriceHint>>,
    /// Pending BUY lifecycle entries (intent_id -> entry), after successful JetStream publish.
    pending_buy_entries: parking_lot::RwLock<HashMap<String, PendingBuyEntry>>,
    /// At most one active pending BUY per mint: mint -> intent_id.
    pending_buy_mint_index: parking_lot::RwLock<HashMap<String, String>>,
    /// Multi-pool registry: All known pools per mint (mint -> Vec<PoolInfo>)
    mint_pools: parking_lot::RwLock<HashMap<String, Vec<PoolInfo>>>,
    /// SLAVE LivePoolCache — live `PoolCacheUpdate` from JetStream after start (no global snapshot replay).
    /// Provides reserve-based quoting for multi-pool routing (FIX-21).
    live_pool_cache: LivePoolCache,
    /// Dedupe / cooldown for startup + retry `Ensure*` ControlRequests to `market-data` for open positions.
    open_position_pool_recovery_last_sent: parking_lot::RwLock<HashMap<String, Instant>>,
    /// JetStream KV Store for position persistence (initialized lazily)
    position_kv: tokio::sync::OnceCell<async_nats::jetstream::kv::Store>,
    /// Mints with non-zero wallet balance that could not be reconciled at bootstrap
    /// because no pool was known yet. Checked when new pools are registered.
    orphaned_mints: parking_lot::RwLock<HashMap<String, (u64, u8)>>,
    /// Scope 56: in-memory idempotency for duplicate JetStream / replayed ExecutionResults
    /// on the orphaned-BUY path (no durable store in this scope).
    orphaned_recovered_intent_ids: parking_lot::RwLock<BoundedIntentIdCache>,
    /// Scope 57: latest JetStream `WalletBalanceSnapshot` per mint (authority hint for exit sizing; I-7 no RPC).
    latest_wallet_balance_raw_by_mint: parking_lot::RwLock<HashMap<String, u64>>,
    /// Stats — **unique mints** with at least one pool-scoped tracker row (not one increment per pool row).
    tokens_tracked: std::sync::atomic::AtomicU64,
    /// Prometheus / heartbeat: **unique tokens (mints)** that crossed into a rejected/blacklisted
    /// strategy state — **not** one increment per pool-scoped `TokenTracker` row. Deduped
    /// process-wide via [`Self::record_token_blacklisted_once`] and [`Self::blacklisted_mints_for_metric`].
    tokens_blacklisted: std::sync::atomic::AtomicU64,
    /// Mints already counted in `tokens_blacklisted` in this process (cross-handler metric dedupe).
    blacklisted_mints_for_metric: parking_lot::RwLock<HashSet<String>>,
    intents_generated: std::sync::atomic::AtomicU64,
    exits_generated: std::sync::atomic::AtomicU64,
    /// K Phase 1: Last event slot/ts for Slot-to-Send Latency propagation
    last_event_slot: std::sync::atomic::AtomicU64,
    last_event_ts_ms: std::sync::atomic::AtomicU64,
    /// PR-D: coalesced momentum active-pool NATS publishes (flushed on heartbeat / explicit tick).
    momentum_active_pool_publish_queue: parking_lot::Mutex<MomentumActivePoolPublishQueue>,
}

/// Merge `lp_removal_observed_at` across sibling pool trackers for the same mint (wallclock LP_REMOVAL path).
///
/// Per tracker, [`TokenTracker::record_lp_removal`] records the **first** local observation (`get_or_insert_with(Instant::now)`).
/// For mint-level hard-exit gating we take the **latest** sibling observation (legacy parity with `lp_removed_at` max-merge),
/// so `obs.elapsed()` vs `lp_removal_window_secs` is not skewed toward “too old” as with `min` (which could suppress exits).
#[inline]
fn merge_lp_removal_observed_at_mint_aggregate(
    existing: Option<Instant>,
    incoming: Option<Instant>,
) -> Option<Instant> {
    match (existing, incoming) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

impl MomentumContext {
    /// Increment [`Self::tokens_blacklisted`] at most once per mint for this process (metric dedupe).
    /// Tracker rows may still reject independently; this is observability only.
    fn record_token_blacklisted_once(&self, mint: &str) {
        let mut seen = self.blacklisted_mints_for_metric.write();
        if seen.insert(mint.to_string()) {
            self.tokens_blacklisted
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    fn next_intent_id(&self) -> String {
        let n = self
            .intent_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        format!("int-{}-{:06}", &self.run_id[..8], n)
    }

    fn queue_momentum_active_pool_active(
        &self,
        mint: &str,
        pool: &str,
        pin_reason: MomentumActivePinReason,
    ) {
        let mut g = self.momentum_active_pool_publish_queue.lock();
        g.active.push(MomentumActivePoolEntry {
            mint: mint.to_string(),
            pool: pool.to_string(),
            pin_reason,
        });
    }

    fn queue_momentum_active_pool_removed(&self, mint: &str, pool: &str, reason: &str) {
        // Open position on this pool still needs Geyser reserve pins; a Rejected tracker row
        // (e.g. LP removal, scale-in gate) must not publish `removed` until the position is gone.
        if self
            .positions
            .read()
            .get(mint)
            .is_some_and(|p| p.pool.as_str() == pool)
        {
            return;
        }
        let mut g = self.momentum_active_pool_publish_queue.lock();
        g.removed.push(MomentumRemovedPoolEntry {
            mint: mint.to_string(),
            pool: pool.to_string(),
            reason: reason.to_string(),
        });
    }

    fn flush_momentum_active_pool_publish_queue(self: &Arc<Self>) {
        if self.nats.is_none() {
            return;
        }
        let (active, removed) = {
            let mut g = self.momentum_active_pool_publish_queue.lock();
            if g.active.is_empty() && g.removed.is_empty() {
                return;
            }
            let a = std::mem::take(&mut g.active);
            let r = std::mem::take(&mut g.removed);
            (a, r)
        };
        self.spawn_publish_momentum_active_pools(active, removed, false);
    }

    fn spawn_publish_momentum_active_pools(
        self: &Arc<Self>,
        active: Vec<MomentumActivePoolEntry>,
        removed: Vec<MomentumRemovedPoolEntry>,
        full_active_snapshot: bool,
    ) {
        if active.is_empty() && removed.is_empty() && !full_active_snapshot {
            return;
        }
        let Some(nats_src) = self.nats.as_ref() else {
            return;
        };
        let nats = nats_src.clone_for_spawned_publish();
        let update = MomentumActivePoolsUpdate {
            version: MOMENTUM_ACTIVE_POOLS_WIRE_VERSION,
            ts_unix_ms: wall_clock_unix_ms_now(),
            active,
            removed,
            full_active_snapshot,
        };
        record_momentum_active_pools_messages_total();
        tokio::spawn(async move {
            if let Err(e) = nats.publish(TOPIC_MOMENTUM_ACTIVE_POOLS, &update).await {
                warn!(
                    error = %e,
                    topic = TOPIC_MOMENTUM_ACTIVE_POOLS,
                    "MomentumActivePools NATS publish failed"
                );
            }
        });
    }

    fn collect_momentum_active_pool_snapshot(&self) -> Vec<MomentumActivePoolEntry> {
        let positions = self.positions.read();
        let trackers = self.token_trackers.read();
        let mut out = Vec::new();
        for t in trackers.values() {
            if matches!(t.state, TrackerState::Rejected) {
                continue;
            }
            let pos_same_pool = positions
                .get(&t.mint)
                .is_some_and(|p| p.pool.as_str() == t.pool.as_str());
            let pin_reason = if pos_same_pool {
                MomentumActivePinReason::Position
            } else {
                MomentumActivePinReason::Tracker
            };
            out.push(MomentumActivePoolEntry {
                mint: t.mint.clone(),
                pool: t.pool.clone(),
                pin_reason,
            });
        }
        for (mint, pos) in positions.iter() {
            let key = Self::tracker_storage_key(mint, pos.pool.as_str());
            let position_fallback = match trackers.get(&key) {
                None => true,
                Some(t) => matches!(t.state, TrackerState::Rejected),
            };
            if position_fallback {
                out.push(MomentumActivePoolEntry {
                    mint: mint.clone(),
                    pool: pos.pool.clone(),
                    pin_reason: MomentumActivePinReason::Position,
                });
            }
        }
        out
    }

    fn reconcile_momentum_active_pools_snapshot_publish(self: &Arc<Self>) {
        if self.nats.is_none() {
            return;
        }
        let active = self.collect_momentum_active_pool_snapshot();
        self.spawn_publish_momentum_active_pools(active, Vec::new(), true);
    }

    fn prune_stale_discovery_trackers(&self) -> Vec<MomentumRemovedPoolEntry> {
        let stale = Duration::from_secs(MOMENTUM_STALE_DISCOVERY_SECS);
        let positions = self.positions.read();
        let mut trackers = self.token_trackers.write();
        let keys: Vec<String> = trackers.keys().cloned().collect();
        let mut removed = Vec::new();
        for key in keys {
            let Some(t) = trackers.get(&key) else {
                continue;
            };
            if !matches!(t.state, TrackerState::Discovery | TrackerState::Validation) {
                continue;
            }
            // "Ohne Trade seit 30 min": entweder nie `record_trade` (None) — dann nur prunen wenn
            // der Tracker schon mindestens `stale` in Discovery/Validation sitzt (`first_seen`);
            // oder letzter Markt-Trade älter als `stale`.
            let idle_too_long = match t.last_trade_at {
                None => t.first_seen.elapsed() > stale,
                Some(ts) => ts.elapsed() > stale,
            };
            if !idle_too_long {
                continue;
            }
            if positions
                .get(&t.mint)
                .is_some_and(|p| p.pool.as_str() == t.pool.as_str())
            {
                continue;
            }
            let mint = t.mint.clone();
            let pool = t.pool.clone();
            trackers.remove(&key);
            removed.push(MomentumRemovedPoolEntry {
                mint,
                pool,
                reason: "stale_discovery".to_string(),
            });
        }
        drop(trackers);
        drop(positions);
        let snap = self.token_trackers.read();
        self.refresh_tracker_keys_by_mint_index_from_map(&snap);
        let keys_live: std::collections::HashSet<String> = snap.keys().cloned().collect();
        drop(snap);
        self.dirty_entry_tracker_keys
            .write()
            .retain(|k| keys_live.contains(k));
        removed
    }

    fn maybe_queue_removed_after_position_closed(&self, mint: &str, pool: &str) {
        if self.positions.read().contains_key(mint) {
            return;
        }
        let tk = Self::tracker_storage_key(mint, pool);
        if let Some(t) = self.token_trackers.read().get(&tk) {
            // `collect_momentum_active_pool_snapshot` skips Rejected rows; a leftover Rejected
            // line must not block incremental `removed` after the position is gone (pins until
            // full snapshot otherwise).
            if !matches!(t.state, TrackerState::Rejected) {
                return;
            }
        }
        self.queue_momentum_active_pool_removed(mint, pool, "closed");
    }

    /// Same pool filtering as `find_best_sell_pool` (Scope 57 / I-13), shared for exit quotes.
    fn filtered_exit_quote_pool_refs<'a>(
        &self,
        mint: &str,
        original_pool: &str,
        original_dex: &str,
        candidates: &'a [PoolInfo],
    ) -> Result<Vec<&'a PoolInfo>, anyhow::Error> {
        let cache_migration_evidence = self.live_cache_pumpfun_complete_evidence(mint);
        let pool_flag_migration_evidence = candidates
            .iter()
            .any(|p| p.dex == "pumpfun" && p.bonding_curve_complete == Some(true));
        let complete_evidence = cache_migration_evidence || pool_flag_migration_evidence;

        let now = std::time::Instant::now();
        let fail_cooldown = std::time::Duration::from_secs(120);
        const MAX_FAIL_COUNT: u32 = 3;

        let valid: Vec<_> = candidates
            .iter()
            .filter(|p| {
                let has_accounts = p.dex_pool_accounts.is_some();
                let has_ratio = p.last_trade_ratio.is_some();
                let is_pumpfun = p.dex == "pumpfun";
                has_accounts || (is_pumpfun && has_ratio)
            })
            .collect();

        if valid.is_empty() {
            anyhow::bail!("No pools with valid data (accounts or pumpfun+ratio)");
        }

        let preferred: Vec<_> = valid
            .iter()
            .copied()
            .filter(|p| {
                if p.bonding_curve_complete == Some(true) {
                    debug!(
                        mint = %mint,
                        pool = %p.pool_address,
                        "find_best_sell_pool: skipping migrated PumpFun pool"
                    );
                    return false;
                }
                if p.sell_fail_count >= MAX_FAIL_COUNT {
                    if let Some(last_fail) = p.last_sell_fail_at {
                        if now.duration_since(last_fail) < fail_cooldown {
                            debug!(
                                mint = %mint,
                                pool = %p.pool_address,
                                dex = %p.dex,
                                sell_fail_count = p.sell_fail_count,
                                "find_best_sell_pool: skipping pool ({}x failures in cooldown)",
                                p.sell_fail_count
                            );
                            return false;
                        }
                    }
                }
                true
            })
            .collect();

        let usable = if preferred.is_empty() {
            warn!(
                mint = %mint,
                valid_count = valid.len(),
                "FIX-20: All pools excluded by migration/failure filters — using best-available fallback"
            );
            &valid
        } else {
            &preferred
        };

        let block_pumpswap_for_active_pumpfun = original_dex == "pumpfun" && !complete_evidence;

        if original_dex == "pumpfun" && complete_evidence {
            let has_pump_amm_candidate = usable.iter().any(|p| p.dex == "pump_amm");
            if has_pump_amm_candidate {
                info!(
                    mint = %mint,
                    "SCOPE57: PumpSwap exit route allowed — bonding curve complete / migration evidence"
                );
            }
        }

        let quote_pool_refs: Vec<&PoolInfo> = if block_pumpswap_for_active_pumpfun {
            let filtered: Vec<&PoolInfo> = usable
                .iter()
                .copied()
                .filter(|p| p.dex != "pump_amm")
                .collect();
            if filtered.is_empty() {
                warn!(
                    mint = %mint,
                    original_pool = %original_pool,
                    "SCOPE57: multi-pool usable set was PumpSwap-only — blocked for active PumpFun position (no migration evidence); widening to valid pools excluding PumpSwap"
                );
                let from_valid: Vec<&PoolInfo> = valid
                    .iter()
                    .copied()
                    .filter(|p| p.dex != "pump_amm")
                    .collect();
                if from_valid.is_empty() {
                    anyhow::bail!(
                        "SCOPE57: no non-PumpSwap exit pool for mint {} while position is active PumpFun (bonding curve not evidenced complete)",
                        mint
                    );
                }
                from_valid
            } else {
                let blocked_count = usable.iter().filter(|p| p.dex == "pump_amm").count();
                if blocked_count > 0 {
                    warn!(
                        mint = %mint,
                        blocked_pump_amm_candidates = blocked_count,
                        "SCOPE57: blocked PumpSwap candidate(s) — preferring PumpFun / non-PumpSwap until migration evidence"
                    );
                }
                filtered
            }
        } else {
            usable.to_vec()
        };

        Ok(quote_pool_refs)
    }

    /// Best reserve-based SELL quote from `LivePoolCache` for `pos.token_amount` (I-7: no RPC).
    /// Chooses max SOL-out among pools allowed by `filtered_exit_quote_pool_refs` (same as sell routing).
    /// I-13: PumpSwap quote after migration does not update position mark unless `marks_position_pool`.
    ///
    /// When `token_trackers` is `Some`, DexPoolAccount lookups use that map instead of
    /// `self.token_trackers.read()` — required when the caller already holds `token_trackers.write()`
    /// (e.g. scale-in gate inside `check_for_signals_eval_sorted_tracker_keys`).
    fn executable_exit_quote(
        &self,
        pos: &PositionTracker,
        token_trackers: Option<&HashMap<String, TokenTracker>>,
    ) -> Option<ExitExecutableQuote> {
        let token_mint = solana_sdk::pubkey::Pubkey::from_str(&pos.mint).ok()?;
        if pos.pool.is_empty() || pos.token_amount == 0 {
            return None;
        }

        let registry_pools: Vec<PoolInfo> = {
            let pools_guard = self.mint_pools.read();
            if let Some(candidates) = pools_guard.get(&pos.mint) {
                match self.filtered_exit_quote_pool_refs(&pos.mint, &pos.pool, &pos.dex, candidates)
                {
                    Ok(v) => v.into_iter().cloned().collect(),
                    Err(_) => Vec::new(),
                }
            } else {
                Vec::new()
            }
        };

        let mut try_rows: Vec<PoolInfo> = registry_pools;

        if try_rows.iter().all(|p| p.pool_address != pos.pool) {
            try_rows.push(PoolInfo::new(
                pos.pool.clone(),
                Self::normalize_dex_for_execution_engine(&pos.dex),
                0,
            ));
        }

        let mut seen = std::collections::HashSet::<String>::new();
        try_rows.retain(|p| seen.insert(p.pool_address.clone()));

        let mut best: Option<(ExitExecutableQuote, u64)> = None;

        for pool_row in try_rows {
            let pool_addr = pool_row.pool_address.clone();
            let dex = pool_row.dex.clone();
            let Ok(pool_pk) = solana_sdk::pubkey::Pubkey::from_str(&pool_addr) else {
                continue;
            };
            let Some((state, slot, age_ms)) = self.live_pool_cache.get_with_metadata(&pool_pk)
            else {
                continue;
            };
            let Ok(sol_out) =
                quote_calculator::quote_output_amount(&state, pos.token_amount, &token_mint)
            else {
                continue;
            };
            if sol_out == 0 {
                continue;
            }
            let tps =
                tokens_per_sol::ui_tokens_per_sol(pos.token_amount, pos.token_decimals, sol_out);
            if !tps.is_finite() || tps <= 0.0 {
                continue;
            }
            let marks_position_pool = pool_addr == pos.pool;
            let candidate = ExitExecutableQuote {
                tokens_per_sol: tps,
                pool_sourced: true,
                quote_pool: pool_addr.clone(),
                quote_dex: dex.clone(),
                marks_position_pool,
                source_slot: Some(slot),
                cache_age_ms: Some(age_ms),
            };
            let merged_accounts: Option<Vec<String>> =
                pool_row.dex_pool_accounts.clone().or_else(|| {
                    self.dex_pool_accounts_for_mint_pool(&pos.mint, &pool_addr, token_trackers)
                });
            if let Some(reason) =
                exit_quote_price_exit_guard_violation(pos, &candidate, merged_accounts.as_deref())
            {
                trace!(
                    mint = %pos.mint,
                    quote_pool = %candidate.quote_pool,
                    reason = %reason,
                    "executable_exit_quote candidate rejected by exit guards"
                );
                continue;
            }
            debug!(
                mint = %pos.mint,
                quote_pool = %candidate.quote_pool,
                quote_dex = %candidate.quote_dex,
                source_slot = ?candidate.source_slot,
                cache_age_ms = ?candidate.cache_age_ms,
                marks_position_pool,
                sol_out_lamports = sol_out,
                "executable_exit_quote candidate"
            );
            match best {
                None => best = Some((candidate, sol_out)),
                Some((_, best_sol)) if sol_out > best_sol => best = Some((candidate, sol_out)),
                _ => {}
            }
        }

        best.map(|(q, _)| q)
    }

    /// A.2: Normalize DEX names for execution-engine compatibility (pumpswap/PumpFunAmm → pump_amm)
    fn normalize_dex_for_execution_engine(dex: &str) -> String {
        match dex {
            "pumpswap" | "PumpFunAmm" | "PumpFun AMM" => "pump_amm".to_string(),
            "pumpfun" | "PumpFun" => "pumpfun".to_string(),
            _ => dex.to_string(),
        }
    }

    fn build_open_position_pool_control_request(
        run_id: &str,
        mint: &str,
        position_pool: &str,
        recovery_kind: OpenPositionPoolRecoveryKind,
    ) -> (ControlRequest, OpenPositionPoolRecoveryKind) {
        let request_id = format!("mom-pool-rec-{}", Uuid::new_v4());
        let kind_enum = match recovery_kind {
            OpenPositionPoolRecoveryKind::PumpfunBonding => {
                ControlRequestKind::EnsurePumpfunBondingCurve {
                    base_mint: mint.to_string(),
                }
            }
            OpenPositionPoolRecoveryKind::PumpAmmAccounts
            | OpenPositionPoolRecoveryKind::PumpAmmMigrationProbe => {
                ControlRequestKind::EnsurePumpAmmPoolAccounts {
                    base_mint: mint.to_string(),
                }
            }
            OpenPositionPoolRecoveryKind::OrcaWhirlpool => {
                ControlRequestKind::EnsureOrcaWhirlpoolPoolState {
                    base_mint: mint.to_string(),
                }
            }
            OpenPositionPoolRecoveryKind::MeteoraDlmm => {
                ControlRequestKind::EnsureMeteoraDlmmPoolState {
                    base_mint: mint.to_string(),
                }
            }
            OpenPositionPoolRecoveryKind::RaydiumAmm => {
                ControlRequestKind::EnsureRaydiumAmmPoolState {
                    base_mint: mint.to_string(),
                }
            }
            OpenPositionPoolRecoveryKind::RaydiumCpmm => {
                ControlRequestKind::EnsureRaydiumCpmmPoolState {
                    base_mint: mint.to_string(),
                }
            }
            OpenPositionPoolRecoveryKind::MeteoraCpmm => {
                ControlRequestKind::EnsureMeteoraCpmmPoolState {
                    base_mint: mint.to_string(),
                }
            }
        };
        let mut req = ControlRequest::new(
            "momentum-bot",
            BUILD_VERSION,
            run_id,
            request_id,
            "market-data",
            kind_enum,
        );
        match recovery_kind {
            OpenPositionPoolRecoveryKind::PumpfunBonding => {
                req.force_refresh_pumpfun = true;
            }
            OpenPositionPoolRecoveryKind::PumpAmmAccounts => {
                req.pool_address_hint = Some(position_pool.to_string());
                req.force_refresh = true;
            }
            OpenPositionPoolRecoveryKind::PumpAmmMigrationProbe => {
                req.force_refresh = true;
            }
            OpenPositionPoolRecoveryKind::OrcaWhirlpool => {
                req.pool_address_hint = Some(position_pool.to_string());
                req.force_refresh_orca = true;
            }
            OpenPositionPoolRecoveryKind::MeteoraDlmm => {
                req.pool_address_hint = Some(position_pool.to_string());
                req.force_refresh_meteora_dlmm = true;
            }
            OpenPositionPoolRecoveryKind::RaydiumAmm => {
                req.pool_address_hint = Some(position_pool.to_string());
                req.force_refresh_raydium_amm = true;
            }
            OpenPositionPoolRecoveryKind::RaydiumCpmm => {
                req.pool_address_hint = Some(position_pool.to_string());
                req.force_refresh_raydium_cpmm = true;
            }
            OpenPositionPoolRecoveryKind::MeteoraCpmm => {
                req.pool_address_hint = Some(position_pool.to_string());
                req.force_refresh_meteora_cpmm = true;
            }
        }
        (req, recovery_kind)
    }

    async fn publish_open_position_pool_recovery_if_allowed(
        self: &Arc<Self>,
        mint: &str,
        pool: &str,
        dex: &str,
        reason_tag: &'static str,
    ) {
        let Some(nats) = self.nats.as_ref() else {
            return;
        };
        if pool.is_empty() {
            return;
        }
        let dex_norm = Self::normalize_dex_for_execution_engine(dex);
        let kinds = open_position_pool_recovery_kinds_for_normalized_dex(dex_norm.as_str());
        if kinds.is_empty() {
            info!(
                mint = %mint,
                pool = %pool,
                dex = %dex,
                reason = reason_tag,
                "Momentum open-position pool recovery skipped: unsupported dex for Ensure* discovery"
            );
            return;
        }
        for &recovery_kind in kinds {
            let (req, kind) = Self::build_open_position_pool_control_request(
                self.run_id.as_str(),
                mint,
                pool,
                recovery_kind,
            );
            let key = open_position_pool_recovery_dedupe_key(mint, pool, kind);
            let now = Instant::now();
            {
                let map = self.open_position_pool_recovery_last_sent.read();
                if let Some(prev) = map.get(&key) {
                    if now.duration_since(*prev) < OPEN_POSITION_POOL_RECOVERY_COOLDOWN {
                        debug!(
                            mint = %mint,
                            pool = %pool,
                            dex = %dex,
                            reason = reason_tag,
                            request_kind = ?kind,
                            "Momentum open-position pool recovery deferred: cooldown"
                        );
                        continue;
                    }
                }
            }
            {
                self.open_position_pool_recovery_last_sent
                    .write()
                    .insert(key, now);
            }
            match nats.publish(TOPIC_CONTROL_REQUESTS, &req).await {
                Ok(true) => {
                    info!(
                        mint = %mint,
                        pool = %pool,
                        dex = %dex,
                        request_kind = ?kind,
                        reason = reason_tag,
                        "Momentum open-position pool recovery requested"
                    );
                }
                Ok(false) => {
                    warn!(
                        mint = %mint,
                        pool = %pool,
                        request_kind = ?kind,
                        reason = reason_tag,
                        "Momentum open-position pool recovery publish dropped (NATS publish returned false)"
                    );
                }
                Err(e) => {
                    warn!(
                        mint = %mint,
                        pool = %pool,
                        request_kind = ?kind,
                        reason = reason_tag,
                        error = %e,
                        "Momentum open-position pool recovery publish failed"
                    );
                }
            }
        }
    }

    async fn schedule_open_position_pool_recoveries(self: &Arc<Self>, reason_tag: &'static str) {
        let rows: Vec<(String, String, String)> = {
            let g = self.positions.read();
            g.iter()
                .map(|(m, p)| (m.clone(), p.pool.clone(), p.dex.clone()))
                .collect()
        };
        for (mint, pool, dex) in rows {
            self.publish_open_position_pool_recovery_if_allowed(&mint, &pool, &dex, reason_tag)
                .await;
        }
    }

    async fn tick_open_position_pool_recovery_retries(self: &Arc<Self>) {
        let mut rows: Vec<(String, String, String)> = Vec::new();
        {
            let positions = self.positions.read();
            for (mint, pos) in positions.iter() {
                if pos.pool.is_empty() || pos.token_amount == 0 {
                    continue;
                }
                if self.executable_exit_quote(pos, None).is_some() {
                    continue;
                }
                rows.push((mint.clone(), pos.pool.clone(), pos.dex.clone()));
            }
        }
        for (mint, pool, dex) in rows
            .into_iter()
            .take(OPEN_POSITION_POOL_RECOVERY_RETRY_MAX_PER_TICK)
        {
            self.publish_open_position_pool_recovery_if_allowed(
                &mint,
                &pool,
                &dex,
                "quote_missing_retry",
            )
            .await;
        }
    }

    /// Stable composite map key: one [`TokenTracker`] row per traded pool (same mint, different pools).
    #[inline]
    fn tracker_storage_key(mint: &str, pool: &str) -> String {
        format!("{mint}\x1f{pool}")
    }

    fn refresh_tracker_keys_by_mint_index_from_map(
        &self,
        trackers: &HashMap<String, TokenTracker>,
    ) {
        let mut map: HashMap<String, BTreeSet<String>> = HashMap::new();
        for (k, t) in trackers {
            map.entry(t.mint.clone()).or_default().insert(k.clone());
        }
        *self.tracker_keys_by_mint.write() = map;
    }

    fn register_tracker_key_for_mint_index(&self, mint: &str, key: &str) {
        self.tracker_keys_by_mint
            .write()
            .entry(mint.to_string())
            .or_default()
            .insert(key.to_string());
    }

    fn mark_entry_eval_dirty_key(&self, key: &str) {
        self.dirty_entry_tracker_keys
            .write()
            .insert(key.to_string());
    }

    fn mark_entry_eval_dirty_for_mint(&self, mint: &str) {
        let map = self.tracker_keys_by_mint.read();
        let Some(keys) = map.get(mint) else {
            return;
        };
        let mut dirty = self.dirty_entry_tracker_keys.write();
        for k in keys {
            dirty.insert(k.clone());
        }
    }

    /// After `check_for_signals*` advanced a tracker to `ProbeBuyPending` / `ScaleInPending` but
    /// intent generation or publish failed: revert to an evaluable state and optionally re-queue entry
    /// evaluation for this mint (all pools) so siblings are not blocked by a stuck `*Pending` row.
    pub(crate) fn unwind_stale_entry_pending_after_publish_failure(
        &self,
        signal: &EntrySignal,
        requeue_dirty: bool,
    ) {
        let sk = Self::tracker_storage_key(&signal.mint, &signal.pool);
        let mut trackers = self.token_trackers.write();
        if let Some(tr) = trackers.get_mut(&sk) {
            match signal.kind {
                EntryKind::Probe => {
                    if matches!(tr.state, TrackerState::ProbeBuyPending { .. }) {
                        tr.state = TrackerState::Validation;
                    }
                }
                EntryKind::ScaleIn => {
                    if matches!(tr.state, TrackerState::ScaleInPending { .. }) {
                        tr.state = TrackerState::PositionOpenProbe {
                            filled_at: Instant::now(),
                        };
                    }
                }
            }
        }
        drop(trackers);
        if requeue_dirty {
            self.mark_entry_eval_dirty_for_mint(&signal.mint);
        }
    }

    /// After PumpFun BUY failed for missing creator: suppress entry re-eval for this pool row until cooldown or dev info.
    pub(crate) fn pumpfun_buy_missing_creator_suppress_arm_key(&self, mint: &str, pool: &str) {
        const COOLDOWN: Duration = Duration::from_secs(45);
        let k = Self::tracker_storage_key(mint, pool);
        self.pumpfun_buy_missing_creator_suppress_until
            .write()
            .insert(k, Instant::now() + COOLDOWN);
    }

    pub(crate) fn pumpfun_buy_missing_creator_suppress_clear_for_mint(&self, mint: &str) {
        let prefix = format!("{mint}\x1f");
        self.pumpfun_buy_missing_creator_suppress_until
            .write()
            .retain(|k, _| !k.starts_with(&prefix));
    }

    /// True while pool-scoped PumpFun creator-missing BUY suppress is active (expires lazily on check).
    fn pumpfun_buy_missing_creator_suppress_active_key(&self, key: &str) -> bool {
        let mut m = self.pumpfun_buy_missing_creator_suppress_until.write();
        let now = Instant::now();
        match m.get(key) {
            Some(until) if *until > now => true,
            Some(_) => {
                m.remove(key);
                false
            }
            None => false,
        }
    }

    /// Same-mint tracker keys for hot-path reads; falls back to a mint-only map scan if the index
    /// is empty (e.g. tests that insert `TokenTracker` rows directly).
    fn tracker_storage_keys_for_mint_readonly_resilient(
        &self,
        mint: &str,
        trackers: &HashMap<String, TokenTracker>,
    ) -> Vec<String> {
        let mut keys: Vec<String> = self
            .tracker_keys_by_mint
            .read()
            .get(mint)
            .map(|s| {
                s.iter()
                    .filter(|k| trackers.contains_key(*k))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        if !keys.is_empty() {
            keys.sort_unstable();
            return keys;
        }
        let mut v: Vec<String> = trackers
            .iter()
            .filter(|(_, t)| t.mint == mint)
            .map(|(k, _)| k.clone())
            .collect();
        v.sort_unstable();
        v
    }

    /// Sibling pool rows for same mint (excluding `exclude_pool` composite key), with index + mint-only fallback.
    fn sibling_tracker_storage_keys_same_mint_excluding_pool(
        &self,
        mint: &str,
        exclude_pool: &str,
        trackers: &HashMap<String, TokenTracker>,
    ) -> Vec<String> {
        let self_key = Self::tracker_storage_key(mint, exclude_pool);
        let keys: Vec<String> = self
            .tracker_keys_by_mint
            .read()
            .get(mint)
            .map(|s| {
                s.iter()
                    .filter(|k| *k != &self_key && trackers.contains_key(*k))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        if !keys.is_empty() {
            return keys;
        }
        trackers
            .iter()
            .filter(|(_, t)| t.mint == mint && t.pool.as_str() != exclude_pool)
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// JetStream / lifecycle edges can leave `pending_buy_mint_index` pointing at a removed
    /// `pending_buy_entries` row. A missing entry must not block every pool tracker for the mint.
    fn prune_stale_pending_buy_mint_index(&self) {
        let entries = self.pending_buy_entries.read();
        let mut index = self.pending_buy_mint_index.write();
        index.retain(|mint_key, intent_id| {
            if entries.contains_key(intent_id.as_str()) {
                true
            } else {
                warn!(
                    mint = %mint_key,
                    intent_id = %intent_id,
                    "Stale pending_buy_mint_index: intent missing from pending_buy_entries — dropping index entry"
                );
                false
            }
        });
    }

    /// Returns `true` only for **`dex == "pumpfun"`** bonding-curve entry: do not emit BUY intents when
    /// this PumpFun pool is complete/migrated. **Early return for any other DEX** (Raydium, Orca,
    /// `pump_amm`, …): mint-wide LivePoolCache / sticky migration evidence is **never** applied to
    /// non-PumpFun pools — that evidence is intentionally conservative for **PumpFun-only** entry when
    /// per-pool `bonding_curve_complete` is missing or stale; it must **not** block `pump_amm` or other
    /// venues for the same mint (I-13 / pool ownership stays per tracker row).
    ///
    /// Evidence is Geyser/cache-only (no RPC): `mint_pools` row for this pool + [`Self::live_cache_pumpfun_complete_evidence`].
    fn pumpfun_entry_blocked_by_migration(&self, mint: &str, pool: &str, dex: &str) -> bool {
        if dex != "pumpfun" {
            return false;
        }
        let pool_row_complete = self
            .mint_pools
            .read()
            .get(mint)
            .and_then(|list| {
                list.iter()
                    .find(|p| p.pool_address == pool)
                    .map(|p| p.bonding_curve_complete == Some(true))
            })
            .unwrap_or(false);
        pool_row_complete || self.live_cache_pumpfun_complete_evidence(mint)
    }

    /// Record first-seen slot for a pool
    fn record_pool_seen(&self, pool_address: &str, slot: u64) {
        let mut pools = self.pool_first_seen.write();
        pools.entry(pool_address.to_string()).or_insert(slot);
    }

    fn record_mint_info(&self, mint: &str, info: MintInfo) {
        // Intentionally log/read all fields: they are part of the cached snapshot and used
        // for future strategy extensions (and avoids dead_code warnings under clippy -D warnings).
        let _ = info.last_updated;
        debug!(
            mint = %mint,
            token_program = %info.token_program,
            decimals = info.decimals,
            supply = info.supply,
            mint_authority_set = info.mint_authority.is_some(),
            freeze_authority_set = info.freeze_authority.is_some(),
            "Mint info cached"
        );
        let mut infos = self.mint_infos.write();
        infos.insert(mint.to_string(), info);
        self.mark_entry_eval_dirty_for_mint(mint);
    }

    /// Register or update a pool in the multi-pool registry.
    /// Also checks orphaned_mints for lazy reconciliation.
    fn register_pool(&self, mint: &str, pool_address: &str, dex: &str, slot: u64) {
        let dex = Self::normalize_dex_for_execution_engine(dex);
        {
            let mut pools = self.mint_pools.write();
            let pool_list = pools.entry(mint.to_string()).or_default();

            if let Some(existing) = pool_list
                .iter_mut()
                .find(|p| p.pool_address == pool_address)
            {
                if slot > existing.last_trade_slot {
                    existing.last_trade_slot = slot;
                    existing.last_updated = std::time::Instant::now();
                }
            } else {
                pool_list.push(PoolInfo::new(pool_address.to_string(), dex.clone(), slot));
                debug!(
                    mint = %mint,
                    pool = %pool_address,
                    dex = %dex,
                    total_pools = pool_list.len(),
                    "📍 Pool registered in multi-pool registry"
                );
            }
        } // release mint_pools write lock before reconciliation

        // FIX-35: Check if this mint was orphaned during bootstrap
        let orphan_data = self.orphaned_mints.write().remove(mint);
        if let Some((balance_raw, decimals)) = orphan_data {
            if let Some(reconciled) = self.build_reconciled_position(mint, balance_raw, decimals) {
                let hold_secs = reconciled.entry_time.elapsed().as_secs();
                self.positions
                    .write()
                    .insert(mint.to_string(), reconciled.clone());
                info!(
                    mint = %mint,
                    pool = %reconciled.pool,
                    dex = %reconciled.dex,
                    balance_raw,
                    hold_secs,
                    "🧭 Orphaned mint reconciled into position (pool now known)"
                );
            }
        }
    }

    /// Update pool trade data (ratio + slot).
    /// If the pool is not yet registered in mint_pools, auto-register it so
    /// exit intents can later find it via `find_best_sell_pool`.
    fn update_pool_trade_data(
        &self,
        mint: &str,
        pool_address: &str,
        dex: &str,
        sol_amount: u64,
        token_amount: u64,
        slot: u64,
    ) {
        let dex = Self::normalize_dex_for_execution_engine(dex);
        let mut is_new_pool = false;
        {
            let mut pools = self.mint_pools.write();
            let pool_list = pools.entry(mint.to_string()).or_default();

            let pool_info = if let Some(existing) = pool_list
                .iter_mut()
                .find(|p| p.pool_address == pool_address)
            {
                existing
            } else {
                is_new_pool = true;
                pool_list.push(PoolInfo::new(pool_address.to_string(), dex.clone(), slot));
                debug!(
                    mint = %mint,
                    pool = %pool_address,
                    dex = %dex,
                    "Pool auto-registered via trade event"
                );
                match pool_list.last_mut() {
                    Some(pi) => pi,
                    None => {
                        error!(mint = %mint, pool = %pool_address, "record_trade: pool_list empty after push (defensive)");
                        return;
                    }
                }
            };

            if token_amount > 0 {
                pool_info.last_trade_ratio = Some(sol_amount as f64 / token_amount as f64);
            }
            pool_info.last_trade_slot = slot;
            pool_info.last_updated = std::time::Instant::now();
        } // release mint_pools write lock

        // FIX-35/37: Check orphaned_mints when a new pool is auto-registered via trade data.
        // Previously only register_pool() (from PoolCreated events) checked orphans,
        // but pools discovered via trades (FIX-33) bypassed this path entirely.
        if is_new_pool {
            let orphan_data = self.orphaned_mints.write().remove(mint);
            if let Some((balance_raw, decimals)) = orphan_data {
                if let Some(reconciled) =
                    self.build_reconciled_position(mint, balance_raw, decimals)
                {
                    self.positions
                        .write()
                        .insert(mint.to_string(), reconciled.clone());
                    info!(
                        mint = %mint,
                        pool = %reconciled.pool,
                        dex = %reconciled.dex,
                        balance_raw,
                        "Orphaned mint reconciled into position (pool discovered via trade)"
                    );
                }
            }
        }
    }

    /// Update pool accounts (for swap instruction building)
    fn update_pool_accounts(&self, mint: &str, pool_address: &str, accounts: Vec<String>) {
        let mut pools = self.mint_pools.write();
        if let Some(pool_list) = pools.get_mut(mint) {
            if let Some(pool_info) = pool_list
                .iter_mut()
                .find(|p| p.pool_address == pool_address)
            {
                pool_info.dex_pool_accounts = Some(accounts);
                pool_info.last_updated = std::time::Instant::now();
            }
        }
    }

    /// Scope 57 / I-13: Cache-side PumpFun migration signals (Geyser/JetStream — no RPC),
    /// plus Scope B sticky migration evidence when LivePoolCache rows are not yet present.
    fn live_cache_pumpfun_complete_evidence(&self, mint: &str) -> bool {
        let sticky = self
            .latest_pumpfun_migration_complete_by_mint
            .read()
            .contains_key(mint);
        let Ok(pk) = solana_sdk::pubkey::Pubkey::from_str(mint) else {
            return sticky;
        };
        sticky
            || self
                .live_pool_cache
                .pumpfun_bonding_curve_complete_for_mint(&pk)
            || self.live_pool_cache.is_pumpfun_complete_for_mint(&pk) == Some(true)
    }

    fn select_reconcile_pool(&self, mint: &str) -> Option<(String, String, Option<f64>)> {
        let pools = self.mint_pools.read();
        let pool_list = pools.get(mint)?;
        let migration_evidence = self.live_cache_pumpfun_complete_evidence(mint)
            || pool_list
                .iter()
                .any(|p| p.dex == "pumpfun" && p.bonding_curve_complete == Some(true));

        let active_pumpfun: Vec<&PoolInfo> = pool_list
            .iter()
            .filter(|p| p.dex == "pumpfun" && p.bonding_curve_complete != Some(true))
            .collect();

        if !migration_evidence && !active_pumpfun.is_empty() {
            let best = active_pumpfun
                .into_iter()
                .max_by_key(|p| p.last_trade_slot)?;
            info!(
                mint = %mint,
                pool = %best.pool_address,
                dex = %best.dex,
                bonding_curve_complete = ?best.bonding_curve_complete,
                "SCOPE57: recovery prefers PumpFun bonding pool — no bonding-curve complete/migration evidence (avoid routing residual to PumpSwap by newest slot)"
            );
            return Some((
                best.pool_address.clone(),
                best.dex.clone(),
                best.last_trade_ratio,
            ));
        }

        if migration_evidence {
            info!(
                mint = %mint,
                "SCOPE57: migration/complete evidence present — PumpSwap reconciliation route allowed"
            );
        }

        let has_pumpfun_row = pool_list.iter().any(|p| p.dex == "pumpfun");
        let only_pumpswap_in_registry = pool_list.iter().all(|p| p.dex == "pump_amm");
        if !migration_evidence && !has_pumpfun_row && only_pumpswap_in_registry {
            let cache_incomplete_bc =
                solana_sdk::pubkey::Pubkey::from_str(mint)
                    .ok()
                    .is_some_and(|pk| {
                        self.live_pool_cache.is_pumpfun_complete_for_mint(&pk) == Some(false)
                    });
            if cache_incomplete_bc {
                warn!(
                    mint = %mint,
                    "SCOPE57: refuse PumpSwap-only reconciliation — LivePoolCache reports incomplete PumpFun bonding curve but mint_pools has no PumpFun row"
                );
            } else {
                warn!(
                    mint = %mint,
                    "SCOPE57: refuse PumpSwap-only reconciliation — registry lists only pump_amm pools and no migration evidence (cannot exclude active BC residual)"
                );
            }
            return None;
        }

        let best = pool_list.iter().max_by_key(|p| p.last_trade_slot)?;
        Some((
            best.pool_address.clone(),
            best.dex.clone(),
            best.last_trade_ratio,
        ))
    }

    fn build_reconciled_position(
        &self,
        mint: &str,
        balance_raw: u64,
        decimals: u8,
    ) -> Option<PositionTracker> {
        // FIX-36: SOL/WSOL is the quote currency, never a tradeable position
        if mint == "So11111111111111111111111111111111111111112"
            || mint == "NATIVE_SOL"
            || mint == "11111111111111111111111111111111"
        {
            return None;
        }
        let (pool, dex, ratio_opt) = self.select_reconcile_pool(mint)?;
        // ratio = sol_lamports / token_raw (raw units)
        // entry_price should be tokens_UI / SOL_UI = (1/ratio) * 10^(9 - decimals)
        let entry_price = ratio_opt
            .filter(|r| *r > 0.0)
            .map(|r| (1.0 / r) * 10f64.powi(9_i32 - decimals as i32))
            .unwrap_or(1.0);

        let mut tracker =
            PositionTracker::new(mint, &pool, &dex, entry_price, decimals, balance_raw, 0);
        tracker.entry_source = PositionEntrySource::WalletSnapshot;
        // Resolve token_program from mint_infos for wallet snapshot reconciliation
        tracker.token_program = self
            .mint_infos
            .read()
            .get(mint)
            .map(|m| m.token_program.clone())
            .filter(|tp| !tp.is_empty());
        // A.2: Resolve creator for pumpfun (BC-SELL after wallet reconciliation)
        if dex == "pumpfun" {
            let tk = Self::tracker_storage_key(mint, &pool);
            let tracker_creator = self
                .token_trackers
                .read()
                .get(&tk)
                .and_then(|t| t.dev_wallet.clone());
            if let Some(creator) = self.resolve_authoritative_creator(mint, tracker_creator) {
                tracker.set_creator(&creator);
            }
        }

        let config = self.config.read();
        if config.max_hold_time_secs > 0 {
            tracker.entry_time = Instant::now()
                .checked_sub(Duration::from_secs(config.max_hold_time_secs))
                .unwrap_or_else(Instant::now);
        }

        self.apply_latest_sticky_state_to_position(mint, &mut tracker);

        Some(tracker)
    }

    /// FIX-22: Cross-check creator from TokenTracker against LivePoolCache.
    /// LivePoolCache creator comes from on-chain bonding curve account data (bytes 49-80)
    /// and is authoritative. TokenTracker creator may come from instruction_accounts[7]
    /// which can be wrong for CPI/bundler-created tokens.
    /// Returns the best available creator, preferring LivePoolCache when available.
    fn resolve_authoritative_creator(
        &self,
        mint: &str,
        tracker_creator: Option<String>,
    ) -> Option<String> {
        // Try LivePoolCache (authoritative: from Geyser bonding curve account data)
        let cache_creator = {
            if let Ok(mint_pk) = solana_sdk::pubkey::Pubkey::from_str(mint) {
                let (bonding_curve, _) =
                    ironcrab::solana::dex::pumpfun::PumpFunDex::derive_bonding_curve_static(
                        &mint_pk,
                    );
                self.live_pool_cache
                    .get_pumpfun_creator(&bonding_curve)
                    .map(|pk| pk.to_string())
            } else {
                None
            }
        };

        match (&cache_creator, &tracker_creator) {
            (Some(cache), Some(tracker)) if cache != tracker => {
                warn!(
                    mint = %mint,
                    tracker_creator = %tracker,
                    cache_creator = %cache,
                    "FIX-22: Creator mismatch! LivePoolCache (authoritative) differs from TokenTracker — using cache value"
                );
                // Correct the TokenTracker for future calls
                let config = self.config.read().clone();
                let mut trackers = self.token_trackers.write();
                let keys = self.tracker_storage_keys_for_mint_readonly_resilient(mint, &trackers);
                for key in keys {
                    let Some(t) = trackers.get_mut(&key) else {
                        continue;
                    };
                    if t.mint == mint {
                        t.set_dev_info(cache, 0.0, &config);
                    }
                }
                self.pumpfun_buy_missing_creator_suppress_clear_for_mint(mint);
                self.mark_entry_eval_dirty_for_mint(mint);
                cache_creator
            }
            (Some(_cache), Some(_tracker)) => {
                // Both present and identical — use cache (authoritative)
                cache_creator
            }
            (Some(cache), None) => {
                debug!(
                    mint = %mint,
                    cache_creator = %cache,
                    "Creator resolved from LivePoolCache (TokenTracker had none)"
                );
                Some(cache.clone())
            }
            (None, Some(_)) => {
                // Only tracker has it — use it (LivePoolCache may not have this token)
                tracker_creator
            }
            (None, None) => None,
        }
    }

    /// Find best pool for selling tokens (highest SOL output)
    /// Returns (pool_address, dex, accounts, expected_sol_out, alternatives_checked)
    ///
    /// FIX-20: Exclusion logic for migrated PumpFun pools and recently-failed pools.
    /// Phase 1: Basic validity filter (accounts, trade data, age)
    /// Phase 2: Exclude migrated curves + pools with repeated failures
    /// Phase 3: Fallback if all preferred pools excluded → use best-available
    fn find_best_sell_pool(
        &self,
        mint: &str,
        token_amount: u64,
        original_pool: &str,
        original_dex: &str,
    ) -> Result<(String, String, Vec<String>, f64, usize)> {
        let pools = self.mint_pools.read();
        let candidates = pools
            .get(mint)
            .ok_or_else(|| anyhow::anyhow!("No pools known for mint {}", mint))?;

        let quote_pool_refs =
            self.filtered_exit_quote_pool_refs(mint, original_pool, original_dex, candidates)?;

        // FIX-21: Quote each pool using RESERVE-BASED calculation from LivePoolCache.
        // Fallback to last_trade_ratio only when cache has no data for a pool.
        let token_mint_pubkey = solana_sdk::pubkey::Pubkey::from_str(mint).ok();

        let mut quotes: Vec<(String, String, Vec<String>, f64, &str)> = Vec::new();
        for p in quote_pool_refs {
            let pool_pubkey = solana_sdk::pubkey::Pubkey::from_str(&p.pool_address).ok();

            // Try reserve-based quote from LivePoolCache first
            let cache_quote = pool_pubkey.and_then(|pk| {
                let state = self.live_pool_cache.get(&pk)?;
                // For SELL: input_mint = token_mint, output = SOL
                let input_mint = token_mint_pubkey.as_ref()?;
                match quote_calculator::quote_output_amount(&state, token_amount, input_mint) {
                    Ok(sol_out) if sol_out > 0 => Some(sol_out as f64),
                    _ => None,
                }
            });

            // A.2: PumpFun allows empty pool_accounts (EE derives from mint+creator)
            let accounts = p.dex_pool_accounts.clone().unwrap_or_default();
            let can_use = !accounts.is_empty() || p.dex == "pumpfun";

            if let Some(expected_sol) = cache_quote {
                if can_use {
                    quotes.push((
                        p.pool_address.clone(),
                        p.dex.clone(),
                        accounts,
                        expected_sol,
                        "cache",
                    ));
                }
            } else if let Some(ratio) = p.last_trade_ratio {
                // Fallback: approximate from last observed trade ratio
                if can_use {
                    let expected_sol = (token_amount as f64) * ratio;
                    quotes.push((
                        p.pool_address.clone(),
                        p.dex.clone(),
                        accounts,
                        expected_sol,
                        "ratio",
                    ));
                }
            }
        }

        if quotes.is_empty() {
            anyhow::bail!("No pools with valid quotes");
        }

        let alternatives_checked = quotes.len();

        // Sort by expected SOL output (descending)
        quotes.sort_by(|a, b| b.3.partial_cmp(&a.3).unwrap_or(std::cmp::Ordering::Equal));

        let best = &quotes[0];
        let best_pool = &best.0;
        let expected_sol = best.3;
        let quote_source = best.4;

        // Log if we're switching pools
        if best_pool != original_pool && alternatives_checked > 1 {
            let original_quote = quotes
                .iter()
                .find(|q| q.0 == original_pool)
                .map(|q| q.3)
                .unwrap_or(0.0);

            if original_quote > 0.0 {
                let improvement_pct = ((expected_sol / original_quote) - 1.0) * 100.0;
                info!(
                    mint = %mint,
                    original_pool = %original_pool,
                    best_pool = %best_pool,
                    best_dex = %best.1,
                    quote_source = %quote_source,
                    improvement_pct = %format!("{:.2}%", improvement_pct),
                    alternatives = alternatives_checked,
                    "🎯 Switching to better pool for exit (FIX-21)"
                );
            }
        } else if alternatives_checked > 1 {
            debug!(
                mint = %mint,
                best_pool = %best_pool,
                best_dex = %best.1,
                quote_source = %quote_source,
                expected_sol_lamports = expected_sol as u64,
                alternatives = alternatives_checked,
                "find_best_sell_pool: selected (FIX-21)"
            );
        }

        Ok((
            best.0.clone(),
            best.1.clone(),
            best.2.clone(),
            expected_sol,
            alternatives_checked,
        ))
    }

    /// Find best pool for buying tokens (highest token output for given SOL)
    /// Only used for ScaleIn entries (Probe entries prioritize speed over price)
    /// Returns (pool_address, dex, accounts, expected_tokens_out, alternatives_checked)
    fn find_best_buy_pool(
        &self,
        mint: &str,
        sol_amount: u64,
        original_pool: &str,
    ) -> Result<(String, String, Vec<String>, f64, usize)> {
        let pools = self.mint_pools.read();
        let candidates = pools
            .get(mint)
            .ok_or_else(|| anyhow::anyhow!("No pools known for mint {}", mint))?;

        let now = std::time::Instant::now();
        let max_age = std::time::Duration::from_secs(300); // Only use pools with trades in last 5min

        // Filter: must have dex_pool_accounts AND recent trade data
        let valid: Vec<_> = candidates
            .iter()
            .filter(|p| {
                p.dex_pool_accounts.is_some()
                    && p.last_trade_ratio.is_some()
                    && now.duration_since(p.last_updated) < max_age
            })
            .collect();

        if valid.is_empty() {
            anyhow::bail!("No pools with recent trade data and accounts available");
        }

        // FIX-21: Quote each pool using RESERVE-BASED calculation from LivePoolCache.
        // For BUY: input = SOL, output = tokens
        // Fallback to last_trade_ratio only when cache has no data.
        let sol_mint_pubkey = *SOL_MINT_PUBKEY;

        let mut quotes: Vec<(String, String, Vec<String>, f64)> = Vec::new();
        for p in &valid {
            let pool_pubkey = solana_sdk::pubkey::Pubkey::from_str(&p.pool_address).ok();

            // Try reserve-based quote from LivePoolCache
            let cache_quote = pool_pubkey.and_then(|pk| {
                let state = self.live_pool_cache.get(&pk)?;
                // For BUY: input_mint = SOL
                match quote_calculator::quote_output_amount(&state, sol_amount, &sol_mint_pubkey) {
                    Ok(tokens_out) if tokens_out > 0 => Some(tokens_out as f64),
                    _ => None,
                }
            });

            if let Some(expected_tokens) = cache_quote {
                if let Some(accounts) = p.dex_pool_accounts.clone() {
                    quotes.push((
                        p.pool_address.clone(),
                        p.dex.clone(),
                        accounts,
                        expected_tokens,
                    ));
                } else {
                    warn!(pool = %p.pool_address, "find_best_buy_pool: skipping pool (dex_pool_accounts None, filter mismatch)");
                }
            } else if let Some(ratio) = p.last_trade_ratio {
                if ratio > 0.0 {
                    if let Some(accounts) = p.dex_pool_accounts.clone() {
                        let expected_tokens = (sol_amount as f64) / ratio;
                        quotes.push((
                            p.pool_address.clone(),
                            p.dex.clone(),
                            accounts,
                            expected_tokens,
                        ));
                    } else {
                        warn!(pool = %p.pool_address, "find_best_buy_pool: skipping pool (dex_pool_accounts None, filter mismatch)");
                    }
                }
            }
        }

        if quotes.is_empty() {
            anyhow::bail!("No pools with valid quotes");
        }

        let alternatives_checked = quotes.len();

        // Sort by expected token output (descending - more tokens is better)
        quotes.sort_by(|a, b| b.3.partial_cmp(&a.3).unwrap_or(std::cmp::Ordering::Equal));

        let best = &quotes[0];
        let best_pool = &best.0;
        let expected_tokens = best.3;

        // Log if we're switching pools
        if best_pool != original_pool && alternatives_checked > 1 {
            let original_quote = quotes
                .iter()
                .find(|q| q.0 == original_pool)
                .map(|q| q.3)
                .unwrap_or(0.0);

            if original_quote > 0.0 {
                let improvement_pct = ((expected_tokens / original_quote) - 1.0) * 100.0;
                info!(
                    mint = %mint,
                    original_pool = %original_pool,
                    best_pool = %best_pool,
                    best_dex = %best.1,
                    improvement_pct = %format!("{:.2}%", improvement_pct),
                    alternatives = alternatives_checked,
                    "🎯 Switching to better pool for scale-in buy (FIX-21)"
                );
            }
        }

        Ok((
            best.0.clone(),
            best.1.clone(),
            best.2.clone(),
            expected_tokens,
            alternatives_checked,
        ))
    }

    /// Returns true if the DEX requires DexPoolAccounts in Intent.resources.accounts
    /// for deterministic TX building (no RPC in hot path).
    fn dex_requires_pool_accounts(dex: &str) -> bool {
        dex == "pump_amm" || dex == "meteora_dlmm"
    }

    /// Resolve cached Dex pool accounts for `(mint, pool)`.
    ///
    /// When `trackers_override` is `Some`, reads from that map only (no `token_trackers.read()`),
    /// so callers that already hold `token_trackers.write()` avoid deadlock.
    fn dex_pool_accounts_for_mint_pool(
        &self,
        mint: &str,
        pool: &str,
        trackers_override: Option<&HashMap<String, TokenTracker>>,
    ) -> Option<Vec<String>> {
        let key = Self::tracker_storage_key(mint, pool);
        let from_tracker = match trackers_override {
            Some(map) => map.get(&key).and_then(|t| t.dex_pool_accounts.clone()),
            None => self
                .token_trackers
                .read()
                .get(&key)
                .and_then(|t| t.dex_pool_accounts.clone()),
        };
        from_tracker.or_else(|| {
            let pending = self.pending_pool_accounts.read();
            pending
                .get(mint)
                .filter(|(_, p, _)| p == pool)
                .map(|(_, _, a)| a.clone())
        })
    }

    fn try_get_dex_pool_accounts_for_mint_pool(
        &self,
        mint: &str,
        pool: &str,
    ) -> Option<Vec<String>> {
        self.dex_pool_accounts_for_mint_pool(mint, pool, None)
    }

    /// Cache verified `DexPoolAccounts` from market-data for deterministic BUY IX building.
    /// PoolCreated / trade observation alone does not populate this — partial or short rows
    /// (especially for PumpSwap) are dropped so BUY cannot proceed on stale fragments.
    fn record_dex_pool_accounts(
        &self,
        dex: &str,
        pool_address: &str,
        base_mint: &str,
        quote_mint: &str,
        accounts: &[String],
    ) {
        let dex = Self::normalize_dex_for_execution_engine(dex);
        let is_pump_amm = dex == "pump_amm";

        // PumpSwap always requires exactly 14 accounts; all other DEXes need at least 3
        let min_accounts = if is_pump_amm { 14 } else { 3 };

        if accounts.len() < min_accounts {
            warn!(
                dex = %dex,
                pool = %pool_address,
                base = %base_mint,
                quote = %quote_mint,
                accounts_len = accounts.len(),
                min_required = min_accounts,
                "DexPoolAccounts ignored: insufficient accounts"
            );
            return;
        }

        // For pump_amm: enforce exactly 14 accounts
        if is_pump_amm && accounts.len() != 14 {
            warn!(
                dex = %dex,
                pool = %pool_address,
                accounts_len = accounts.len(),
                "DexPoolAccounts ignored: pump_amm requires exactly 14 accounts"
            );
            return;
        }

        if accounts.first().map(|s| s.as_str()) != Some(pool_address) {
            warn!(
                dex = %dex,
                pool = %pool_address,
                first = ?accounts.first(),
                "DexPoolAccounts ignored: accounts[0] must equal pool_address"
            );
            return;
        }

        // Identify the traded token mint (non-WSOL side for SOL pairs).
        let wsol = "So11111111111111111111111111111111111111112";
        let token_mint = if base_mint == wsol {
            quote_mint
        } else {
            base_mint
        };

        {
            let mut pending = self.pending_pool_accounts.write();
            pending.insert(
                token_mint.to_string(),
                (dex.clone(), pool_address.to_string(), accounts.to_vec()),
            );
        }

        // Apply immediately if tracker exists for this pool row.
        let mut trackers = self.token_trackers.write();
        let tk = Self::tracker_storage_key(token_mint, pool_address);
        if let Some(tracker) = trackers.get_mut(&tk) {
            tracker.dex = dex.clone(); // A.2: Ensure normalized DEX
            tracker.dex_pool_accounts = Some(accounts.to_vec());
            self.mark_entry_eval_dirty_key(&tk);
            debug!(
                mint = %token_mint,
                pool = %pool_address,
                dex = %dex,
                accounts_len = accounts.len(),
                "DexPoolAccounts applied to tracker"
            );
        }
    }

    /// Get or create a token tracker
    fn get_or_create_tracker(
        &self,
        mint: &str,
        pool: &str,
        dex: &str,
        slot: u64,
        liquidity: u64,
    ) -> bool {
        if is_non_tradeable_momentum_mint(mint) {
            return false;
        }
        let mint_already_tracked = self
            .tracker_keys_by_mint
            .read()
            .get(mint)
            .map(|s| !s.is_empty())
            .unwrap_or(false);
        let config = self.config.read().clone();
        let mut trackers = self.token_trackers.write();
        let key = Self::tracker_storage_key(mint, pool);
        use std::collections::hash_map::Entry;
        match trackers.entry(key.clone()) {
            Entry::Occupied(_) => false,
            Entry::Vacant(v) => {
                let tracker = v.insert(TokenTracker::new(mint, pool, dex, slot, liquidity));
                self.register_tracker_key_for_mint_index(mint, &key);

                // Apply any dev wallet info that arrived before the tracker existed.
                if let Some((dev_wallet, supply_pct)) =
                    self.pending_dev_info.read().get(mint).cloned()
                {
                    let was_not_rejected = tracker.was_not_rejected();
                    tracker.set_dev_info(&dev_wallet, supply_pct, &config);
                    if was_not_rejected && tracker.is_rejected() {
                        self.record_token_blacklisted_once(mint);
                        let reason = tracker.blacklist_reason.as_deref().unwrap_or("rejected");
                        self.queue_momentum_active_pool_removed(mint, pool, reason);
                    }
                }

                // Apply any DexPoolAccounts that arrived before the tracker existed.
                if let Some((dex_name, pool_addr, accounts)) =
                    self.pending_pool_accounts.read().get(mint).cloned()
                {
                    if pool_addr == pool {
                        tracker.dex = dex_name;
                        tracker.dex_pool_accounts = Some(accounts);
                    }
                }

                if !mint_already_tracked {
                    self.tokens_tracked
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                self.mark_entry_eval_dirty_key(&key);
                true // New tracker created
            }
        }
    }

    fn maybe_warn_trade_without_tracker(mint: &str, pool: &str, is_buy: bool) {
        let dedupe_key = format!("{mint}\x1f{pool}");
        let now = Instant::now();
        let mut last = MOMENTUM_NO_TRACKER_WARN_LAST.lock();
        let should_warn = last
            .get(&dedupe_key)
            .map(|t| now.duration_since(*t) >= MOMENTUM_NO_TRACKER_WARN_COOLDOWN)
            .unwrap_or(true);
        if should_warn {
            last.insert(dedupe_key, now);
            warn!(
                mint = %mint,
                pool = %pool,
                is_buy,
                "Trade received but no pool-scoped tracker row (discovery guard or pool key mismatch)"
            );
        }
    }

    /// Record a trade for a token **on a specific pool** (pool-scoped tracker row).
    #[allow(clippy::too_many_arguments)] // MarketEvent trade fields — keep explicit for hot path clarity
    fn record_trade(
        &self,
        mint: &str,
        pool: &str,
        trader: &str,
        is_buy: bool,
        sol_amount: u64,
        token_amount: u64,
        signature: &str,
        trade_slot: u64,
    ) -> bool {
        let _rt = MomentumRecordTradeTimer(Instant::now());
        let config = self.config.read().clone();
        let head_slot = self.last_event_slot.load(Ordering::Relaxed);
        let mut trackers = self.token_trackers.write();
        let key = Self::tracker_storage_key(mint, pool);
        let mut sync_sibling_dev_sell = false;
        let recorded = if let Some(tracker) = trackers.get_mut(&key) {
            let was_not_rejected = tracker.was_not_rejected();
            let dev_sell_obs_before = tracker.dev_sell_observed_at;
            tracker.record_trade(
                trader,
                is_buy,
                sol_amount,
                token_amount,
                signature,
                trade_slot,
                &config,
            );
            record_momentum_tracker_trades_recorded();
            if was_not_rejected && tracker.is_rejected() {
                self.record_token_blacklisted_once(mint);
                let reason = tracker.blacklist_reason.as_deref().unwrap_or("rejected");
                self.queue_momentum_active_pool_removed(mint, pool, reason);
            }
            sync_sibling_dev_sell = tracker.dev_sell_observed_at.is_some()
                && tracker.dev_sell_observed_at != dev_sell_obs_before;
            if !tracker.is_rejected()
                && tracker
                    .trades
                    .len()
                    .is_multiple_of(MOMENTUM_TRACKER_TRADE_INGEST_DEBUG_EVERY)
            {
                let metrics = tracker.calculate_metrics(&config, head_slot);
                debug!(
                    mint = %mint,
                    pool = %pool,
                    trades_len = tracker.trades.len(),
                    unique_buyers_in_window = metrics.unique_buyers_in_window,
                    head_slot,
                    "Tracker trade ingest sample"
                );
            }
            true
        } else {
            record_momentum_trades_received_no_tracker();
            false
        };
        self.mark_entry_eval_dirty_key(&key);
        // Same-mint sibling pools: propagate dev-sell revalidation state (WAIT, not reject).
        if sync_sibling_dev_sell {
            let Some((source_dev, dev_sell_at)) = trackers
                .get(&key)
                .map(|t| (t.dev_wallet.clone(), t.dev_sell_observed_at))
                .and_then(|(dev, at)| dev.zip(at))
            else {
                return recorded;
            };
            let sibling_keys =
                self.sibling_tracker_storage_keys_same_mint_excluding_pool(mint, pool, &trackers);
            for sk in sibling_keys {
                let Some(t) = trackers.get_mut(&sk) else {
                    continue;
                };
                if t.pool.as_str() == pool {
                    continue;
                }
                if t.is_rejected() {
                    continue;
                }
                if t.dev_wallet.as_deref() != Some(source_dev.as_str()) {
                    continue;
                }
                t.dev_sold = true;
                t.dev_sell_observed_at = Some(dev_sell_at);
                self.mark_entry_eval_dirty_key(&sk);
            }
        }
        recorded
    }

    /// Record dev info for a token
    fn record_dev_info(&self, mint: &str, dev_wallet: &str, supply_pct: f64) {
        let config = self.config.read().clone();
        {
            let mut pending = self.pending_dev_info.write();
            pending.insert(mint.to_string(), (dev_wallet.to_string(), supply_pct));
        }

        let mut trackers = self.token_trackers.write();
        self.refresh_tracker_keys_by_mint_index_from_map(&trackers);
        let keys: Vec<String> = self
            .tracker_keys_by_mint
            .read()
            .get(mint)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default();
        for key in keys {
            let Some(tracker) = trackers.get_mut(&key) else {
                continue;
            };
            let was_not_rejected = tracker.was_not_rejected();
            tracker.set_dev_info(dev_wallet, supply_pct, &config);
            if was_not_rejected && tracker.is_rejected() {
                self.record_token_blacklisted_once(mint);
                let reason = tracker.blacklist_reason.as_deref().unwrap_or("rejected");
                self.queue_momentum_active_pool_removed(
                    tracker.mint.as_str(),
                    tracker.pool.as_str(),
                    reason,
                );
            }
        }
        self.pumpfun_buy_missing_creator_suppress_clear_for_mint(mint);
        self.mark_entry_eval_dirty_for_mint(mint);
        // A.2: Also update position creator when dex is pumpfun (for BC-SELL after restart)
        {
            let mut positions = self.positions.write();
            if let Some(pos) = positions.get_mut(mint) {
                if pos.dex == "pumpfun" && pos.creator.is_none() {
                    pos.set_creator(dev_wallet);
                    debug!(mint = %mint, creator = %dev_wallet, "A.2: Set creator on position from record_dev_info");
                }
            }
        }
    }

    /// Record LP removal for a token (`slot`: `MarketEvent.slot` when known).
    fn record_lp_removal(&self, mint: &str, slot: Option<u64>) {
        let mut trackers = self.token_trackers.write();
        self.refresh_tracker_keys_by_mint_index_from_map(&trackers);
        let keys: Vec<String> = self
            .tracker_keys_by_mint
            .read()
            .get(mint)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default();
        for key in keys {
            let Some(tracker) = trackers.get_mut(&key) else {
                continue;
            };
            let was_not_rejected = tracker.was_not_rejected();
            tracker.record_lp_removal(slot);
            if was_not_rejected && tracker.is_rejected() {
                self.record_token_blacklisted_once(mint);
                let reason = tracker.blacklist_reason.as_deref().unwrap_or("rejected");
                self.queue_momentum_active_pool_removed(
                    tracker.mint.as_str(),
                    tracker.pool.as_str(),
                    reason,
                );
            }
        }
        self.mark_entry_eval_dirty_for_mint(mint);
    }

    /// Check if any tracked token should generate an intent (full scan; tests + compatibility).
    #[cfg(test)]
    fn check_for_signals(&self) -> Vec<EntrySignal> {
        self.prune_stale_pending_buy_mint_index();
        let config = self.config.read().clone();
        let mint_infos = self.mint_infos.read();
        let positions = self.positions.read();
        let pending_buy_index = self.pending_buy_mint_index.read();
        let pending_buy_entries = self.pending_buy_entries.read();
        let mut trackers = self.token_trackers.write();
        self.refresh_tracker_keys_by_mint_index_from_map(&trackers);
        let mut tracker_keys: Vec<String> = trackers.keys().cloned().collect();
        tracker_keys.sort_unstable();
        let chain_head_slot = self.last_event_slot.load(Ordering::Relaxed);
        self.check_for_signals_eval_sorted_tracker_keys(
            &config,
            &mint_infos,
            &positions,
            &pending_buy_index,
            &pending_buy_entries,
            &mut trackers,
            &tracker_keys,
            chain_head_slot,
        )
    }

    /// Strategy tick: evaluate **only** pool-scoped tracker rows marked dirty (event-driven entry path).
    fn check_for_signals_dirty_priority_tick(&self) -> Vec<EntrySignal> {
        self.prune_stale_pending_buy_mint_index();

        let mut dirty_keys: Vec<String> = {
            let mut d = self.dirty_entry_tracker_keys.write();
            let keys: Vec<_> = d.iter().cloned().collect();
            d.clear();
            keys
        };
        dirty_keys.sort_unstable();
        dirty_keys.dedup();
        if dirty_keys.is_empty() {
            return Vec::new();
        }

        let config = self.config.read().clone();
        let mint_infos = self.mint_infos.read();
        let positions = self.positions.read();
        let pending_buy_index = self.pending_buy_mint_index.read();
        let pending_buy_entries = self.pending_buy_entries.read();

        let mut trackers = self.token_trackers.write();
        dirty_keys.retain(|k| trackers.contains_key(k));
        if dirty_keys.is_empty() {
            return Vec::new();
        }

        let eval_t0 = Instant::now();
        let chain_head_slot = self.last_event_slot.load(Ordering::Relaxed);
        let signals = self.check_for_signals_eval_sorted_tracker_keys(
            &config,
            &mint_infos,
            &positions,
            &pending_buy_index,
            &pending_buy_entries,
            &mut trackers,
            &dirty_keys,
            chain_head_slot,
        );
        record_momentum_signal_eval_us(
            eval_t0.elapsed().as_micros().min(u128::from(u64::MAX)) as u64
        );
        signals
    }

    #[allow(clippy::too_many_arguments)]
    fn check_for_signals_eval_sorted_tracker_keys(
        &self,
        config: &MomentumConfig,
        mint_infos: &HashMap<String, MintInfo>,
        positions: &HashMap<String, PositionTracker>,
        pending_buy_index: &HashMap<String, String>,
        pending_buy_entries: &HashMap<String, PendingBuyEntry>,
        trackers: &mut HashMap<String, TokenTracker>,
        tracker_keys: &[String],
        chain_head_slot: u64,
    ) -> Vec<EntrySignal> {
        let wall_now = Instant::now();
        // Under the same write lock as the signal loop: pending probe/scale-in pools for mint-level serialization.
        let sibling_entry_busy: Vec<(String, String)> = trackers
            .values()
            .filter(|t| {
                matches!(
                    t.state,
                    TrackerState::ProbeBuyPending { .. } | TrackerState::ScaleInPending { .. }
                )
            })
            .map(|t| (t.mint.clone(), t.pool.clone()))
            .collect();

        let mut signals = Vec::new();
        // At most one entry signal (probe or scale-in) per mint per evaluation pass.
        // `pending_buy_mint_index` updates after publish; `mint_emitted_entry_this_tick` covers same-tick races.
        let mut mint_emitted_entry_this_tick: HashSet<String> = HashSet::new();

        let probe_sol = ((config.default_position_lamports as f64) * config.probe_buy_pct)
            .round()
            .clamp(0.0, config.default_position_lamports as f64) as u64;
        let scale_sol = config.default_position_lamports.saturating_sub(probe_sol);

        'tracker_keys: for key in tracker_keys {
            let mint = match trackers.get(key) {
                Some(t) => t.mint.clone(),
                None => continue,
            };

            // I-13 / multi-pool: only the position's pool may continue entry/scale on this mint.
            if let Some(pos) = positions.get(&mint) {
                let same_pool = trackers
                    .get(key)
                    .map(|t| t.pool == pos.pool)
                    .unwrap_or(false);
                if !same_pool {
                    continue;
                }
            }

            // At most one pending BUY per mint — ignore other pool trackers while one is in-flight.
            if let Some(intent_id) = pending_buy_index.get(&mint) {
                if let Some(entry) = pending_buy_entries.get(intent_id.as_str()) {
                    let pool_matches_pending = trackers
                        .get(key)
                        .map(|t| entry.pool.as_str() == t.pool.as_str())
                        .unwrap_or(false);
                    if !pool_matches_pending {
                        self.mark_entry_eval_dirty_key(key);
                        continue;
                    }
                }
                // If `intent_id` is missing from entries, index was stale; pruned above — do not block.
            }

            // Skip tokens in terminal states
            match trackers.get(key) {
                None => continue,
                Some(t) if t.is_entry_complete() => continue,
                Some(_) => {}
            }

            if self.pumpfun_buy_missing_creator_suppress_active_key(key) {
                if let Some(t) = trackers.get(key) {
                    if t.dex == "pumpfun" && t.dev_wallet.is_none() {
                        continue;
                    }
                }
            }

            let (this_pool, dex_for_migration) = match trackers.get(key) {
                Some(t) => (t.pool.clone(), t.dex.clone()),
                None => continue,
            };

            // Completed / migrated PumpFun bonding curve on this pool (or mint-level cache evidence).
            if self.pumpfun_entry_blocked_by_migration(&mint, &this_pool, &dex_for_migration) {
                let was_not_rejected = trackers
                    .get(key)
                    .expect("tracker key from iteration")
                    .was_not_rejected();
                {
                    let tracker = trackers.get_mut(key).expect("tracker key from iteration");
                    tracker.reject("REJECT_PUMPFUN_BONDING_COMPLETE");
                    if was_not_rejected && tracker.is_rejected() {
                        warn!(
                            mint = %mint,
                            pool = %tracker.pool,
                            dex = %tracker.dex,
                            "REJECT_PUMPFUN_BONDING_COMPLETE: skip entry — migrated bonding curve / complete evidence (pump_amm unaffected)"
                        );
                        self.record_token_blacklisted_once(&mint);
                        self.queue_momentum_active_pool_removed(
                            &mint,
                            tracker.pool.as_str(),
                            "rejected",
                        );
                    }
                }
                continue;
            }

            // Another pool for this mint already has an entry BUY in-flight — serialize mint-level entry.
            if sibling_entry_busy
                .iter()
                .any(|(m, p)| m == &mint && p.as_str() != this_pool.as_str())
            {
                self.mark_entry_eval_dirty_key(key);
                continue;
            }

            let mint_info = mint_infos.get(&mint);

            // 1) Probe-buy stage (Discovery or Validation state)
            let in_probe_stage = trackers
                .get(key)
                .map(|t| matches!(t.state, TrackerState::Discovery | TrackerState::Validation))
                .unwrap_or(false);
            if in_probe_stage {
                {
                    let tracker = trackers.get_mut(key).expect("tracker key from iteration");
                    let was_not_rejected = tracker.was_not_rejected();
                    let (should_trade, reason) = tracker.should_generate_intent(
                        config,
                        mint_info,
                        chain_head_slot,
                        wall_now,
                    );
                    if was_not_rejected && tracker.is_rejected() {
                        self.record_token_blacklisted_once(&mint);
                        let rej_reason = tracker.blacklist_reason.as_deref().unwrap_or("rejected");
                        self.queue_momentum_active_pool_removed(
                            &mint,
                            tracker.pool.as_str(),
                            rej_reason,
                        );
                    }

                    if should_trade {
                        if mint_emitted_entry_this_tick.contains(&mint) {
                            self.mark_entry_eval_dirty_key(key);
                            continue 'tracker_keys;
                        }
                        if probe_sol == 0 {
                            warn!(
                                mint = %mint,
                                pool = %tracker.pool,
                                dex = %tracker.dex,
                                default_position_lamports = config.default_position_lamports,
                                probe_buy_pct = config.probe_buy_pct,
                                "Entry signal suppressed: probe_sol rounds to 0; increase default_position_lamports or probe_buy_pct"
                            );
                            tracker.state = TrackerState::PositionOpenFull {
                                filled_at: Instant::now(),
                            };
                            continue 'tracker_keys;
                        }
                        tracker.state = TrackerState::ProbeBuyPending {
                            sent_at: Instant::now(),
                        };
                        mint_emitted_entry_this_tick.insert(mint.clone());
                        signals.push(EntrySignal {
                            mint,
                            pool: tracker.pool.clone(),
                            dex: tracker.dex.clone(),
                            sol_amount: probe_sol,
                            kind: EntryKind::Probe,
                            reason: format!("ENTER_PROBE_BUY: {reason}"),
                        });
                    } else if matches!(tracker.state, TrackerState::Discovery) {
                        // Move to validation state if not yet there
                        tracker.state = TrackerState::Validation;
                    }
                }
                continue;
            }

            // 2) Scale-in stage (only after probe fill, within confirm window)
            let probe_filled_at = trackers.get(key).and_then(|t| match t.state {
                TrackerState::PositionOpenProbe { filled_at } => Some(filled_at),
                _ => None,
            });
            if let Some(filled_at) = probe_filled_at {
                let now = Instant::now();
                if now.duration_since(filled_at).as_secs() > config.scale_in_confirm_window_secs {
                    // Confirmation window expired: keep probe position only.
                    ironcrab::metrics::record_momentum_scale_in_gate_blocked_total(
                        ironcrab::metrics::MomentumScaleInGateBlockedReason::WindowExpired,
                    );
                    let tracker = trackers.get_mut(key).expect("tracker key from iteration");
                    tracker.state = TrackerState::PositionOpenFull { filled_at };
                    continue;
                }

                // Scale-in gate (I-14, I-7): same `executable_exit_quote` quality as exits — no RPC.
                // Require strictly `exec_pnl > scale_in_min_probe_executable_pnl_pct` (default 0 ⇒ underwater probe never scales in).
                let Some(pos) = positions.get(&mint) else {
                    ironcrab::metrics::record_momentum_scale_in_gate_blocked_total(
                        ironcrab::metrics::MomentumScaleInGateBlockedReason::MissingProbeState,
                    );
                    trace!(
                        mint = %mint,
                        pool = %this_pool,
                        "SCALE_IN_WAIT: missing PositionTracker for mint"
                    );
                    self.mark_entry_eval_dirty_key(key);
                    continue;
                };
                let Some(ex_q) = self.executable_exit_quote(pos, Some(trackers)) else {
                    ironcrab::metrics::record_momentum_scale_in_gate_blocked_total(
                        ironcrab::metrics::MomentumScaleInGateBlockedReason::NoQuote,
                    );
                    trace!(
                        mint = %mint,
                        pool = %this_pool,
                        "SCALE_IN_WAIT: no executable_exit_quote"
                    );
                    self.mark_entry_eval_dirty_key(key);
                    continue;
                };
                let exec_pnl = tokens_per_sol::pnl_pct(pos.entry_price, ex_q.tokens_per_sol);
                let gate_ok = exec_pnl.partial_cmp(&config.scale_in_min_probe_executable_pnl_pct)
                    == Some(std::cmp::Ordering::Greater);
                if !gate_ok {
                    ironcrab::metrics::record_momentum_scale_in_gate_blocked_total(
                        ironcrab::metrics::MomentumScaleInGateBlockedReason::Pnl,
                    );
                    trace!(
                        mint = %mint,
                        pool = %this_pool,
                        exec_pnl_pct = exec_pnl,
                        min_required = config.scale_in_min_probe_executable_pnl_pct,
                        "SCALE_IN_WAIT: executable probe PnL not above threshold"
                    );
                    self.mark_entry_eval_dirty_key(key);
                    continue;
                }

                {
                    let tracker = trackers.get_mut(key).expect("tracker key from iteration");
                    let was_not_rejected = tracker.was_not_rejected();
                    let (should_trade, reason) = tracker.should_generate_intent(
                        config,
                        mint_info,
                        chain_head_slot,
                        wall_now,
                    );
                    if was_not_rejected && tracker.is_rejected() {
                        self.record_token_blacklisted_once(&mint);
                        let rej_reason = tracker.blacklist_reason.as_deref().unwrap_or("rejected");
                        self.queue_momentum_active_pool_removed(
                            &mint,
                            tracker.pool.as_str(),
                            rej_reason,
                        );
                    }

                    if should_trade {
                        if mint_emitted_entry_this_tick.contains(&mint) {
                            self.mark_entry_eval_dirty_key(key);
                            continue 'tracker_keys;
                        }
                        if scale_sol == 0 {
                            warn!(
                                mint = %mint,
                                pool = %tracker.pool,
                                dex = %tracker.dex,
                                default_position_lamports = config.default_position_lamports,
                                probe_buy_pct = config.probe_buy_pct,
                                "Scale-in suppressed: scale_sol is 0 (after probe rounding); increase default_position_lamports or adjust probe_buy_pct"
                            );
                            tracker.state = TrackerState::PositionOpenFull { filled_at };
                            continue 'tracker_keys;
                        }
                        tracker.state = TrackerState::ScaleInPending {
                            sent_at: Instant::now(),
                        };
                        mint_emitted_entry_this_tick.insert(mint.clone());
                        signals.push(EntrySignal {
                            mint,
                            pool: tracker.pool.clone(),
                            dex: tracker.dex.clone(),
                            sol_amount: scale_sol,
                            kind: EntryKind::ScaleIn,
                            reason: format!("ENTER_SCALE_IN: {reason}"),
                        });
                    }
                }
            }
        }

        signals
    }

    /// Clean up old trackers (older than 5 minutes).
    /// A.2 Phase 6: Never remove trackers for mints with open positions.
    fn cleanup_old_trackers(&self) {
        let open_mints: std::collections::HashSet<_> =
            self.positions.read().keys().cloned().collect();
        let cutoff = Duration::from_secs(300); // 5 minutes
        let pruned: Vec<(String, String, usize, String)> = {
            let trackers = self.token_trackers.read();
            trackers
                .values()
                .filter(|tracker| {
                    !open_mints.contains(&tracker.mint)
                        && tracker.first_seen.elapsed() >= cutoff
                        && tracker.state.is_terminal()
                })
                .map(|t| {
                    (
                        t.mint.clone(),
                        t.pool.clone(),
                        t.trades.len(),
                        format!("{:?}", t.state),
                    )
                })
                .collect()
        };
        for (mint, pool, trade_count, state) in &pruned {
            warn!(
                mint = %mint,
                pool = %pool,
                reason = "tracker_cleanup",
                trade_count,
                state = %state,
                "Removing terminal pool-scoped tracker (5m TTL)"
            );
            self.queue_momentum_active_pool_removed(mint, pool.as_str(), "tracker_cleanup");
        }
        let mut trackers = self.token_trackers.write();
        trackers.retain(|_, tracker| {
            open_mints.contains(&tracker.mint)
                || tracker.first_seen.elapsed() < cutoff
                || !tracker.state.is_terminal()
        });
        self.refresh_tracker_keys_by_mint_index_from_map(&trackers);
        self.dirty_entry_tracker_keys
            .write()
            .retain(|k| trackers.contains_key(k));
    }

    #[cfg(test)]
    pub(crate) fn test_sorted_tracker_storage_keys_for_mint(&self, mint: &str) -> Vec<String> {
        self.tracker_keys_by_mint
            .read()
            .get(mint)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }

    #[cfg(test)]
    pub(crate) fn test_resync_tracker_indexes(&self) {
        let snap = self.token_trackers.read();
        self.refresh_tracker_keys_by_mint_index_from_map(&snap);
        self.dirty_entry_tracker_keys
            .write()
            .retain(|k| snap.contains_key(k));
    }

    // =========================================================================
    // Position Management for Exit Strategy
    // =========================================================================

    /// Open a new position after buy intent is executed
    /// Also persists the position to JetStream KV for crash recovery
    fn open_position(self: &Arc<Self>, p: OpenPositionParams<'_>) {
        let mint_owned = p.mint.to_string();
        let tracker = {
            let mut positions = self.positions.write();
            if let Some(pos) = positions.get_mut(p.mint) {
                pos.token_amount = pos.token_amount.saturating_add(p.token_amount);
                pos.add_investment(p.sol_invested, p.entry_price, p.entry_confirmed_slot);
                // Scope 50: Scale-in increases total held — any prior exit intent was sized for
                // the old total (often probe-only). Reset exit latch so the next exit uses the
                // combined amount; avoids selling probe while scale-in tokens remain.
                pos.exit_generated = false;
                pos.exit_generated_at = None;
                // Keep the best-known decimals (prefer non-zero).
                if pos.token_decimals == 0 && p.token_decimals != 0 {
                    pos.token_decimals = p.token_decimals;
                }
                // Upgrade token_program if not yet known (e.g., probe had None, scale-in has Some)
                if pos.token_program.is_none() && p.token_program.is_some() {
                    pos.token_program = p.token_program.clone();
                }
                // A.2: Upgrade creator if not yet known (for pumpfun BC-SELL)
                if pos.creator.is_none() && p.creator.is_some() && pos.dex == "pumpfun" {
                    pos.creator = p.creator.clone();
                }
                if let Some(ref snap) = p.initial_bonding {
                    pos.bonding_curve_progress_bps = Some(
                        snap.progress_bps
                            .max(pos.bonding_curve_progress_bps.unwrap_or(0)),
                    );
                }
                info!(
                    mint = %p.mint,
                    additional_sol = p.sol_invested,
                    additional_tokens_raw = p.token_amount,
                    total_sol = pos.sol_invested,
                    total_tokens_raw = pos.token_amount,
                    token_program = ?pos.token_program,
                    "📈 Position scaled in"
                );
            } else {
                let mut new_tracker = PositionTracker::new(
                    p.mint,
                    p.pool,
                    p.dex,
                    p.entry_price,
                    p.token_decimals,
                    p.token_amount,
                    p.sol_invested,
                );
                new_tracker.token_program = p.token_program.clone();
                if p.dex == "pumpfun" {
                    if let Some(ref c) = p.creator {
                        new_tracker.set_creator(c);
                    }
                }
                new_tracker.entry_confirmed_slot = p.entry_confirmed_slot;
                if let Some(ref snap) = p.initial_bonding {
                    new_tracker.bonding_curve_progress_bps = Some(snap.progress_bps);
                }
                positions.insert(p.mint.to_string(), new_tracker);
                if p.entry_confirmed_slot == 0 {
                    warn!(
                        mint = %p.mint,
                        "Position opened without confirmed BUY slot — slot-monotonic price gating disabled (Scope 1)"
                    );
                }
                info!(
                    mint = %p.mint,
                    entry_price = p.entry_price,
                    sol_invested = p.sol_invested,
                    token_program = ?p.token_program,
                    "📈 Position opened"
                );
            }
            let _ = self.apply_latest_sticky_state_to_position(
                p.mint,
                positions.get_mut(p.mint).expect("position"),
            );
            positions.get(p.mint).expect("position").clone()
        };

        // Persist to KV asynchronously (fire-and-forget)
        let ctx = Arc::clone(self);
        tokio::spawn(async move {
            ctx.save_position_to_kv(&mint_owned, &tracker).await;
        });

        self.queue_momentum_active_pool_active(p.mint, p.pool, MomentumActivePinReason::Position);
        self.flush_momentum_active_pool_publish_queue();
    }

    /// Update position price from market trade or pool reserves.
    /// If `source_pool` is Some, only update when position.pool matches (prevents wrong-pool
    /// price pollution for multi-pool tokens, e.g. bonding curve + AMM). INVARIANTS.md I-13.
    /// `source_slot`: Geyser/update slot; required when `entry_confirmed_slot > 0` (Scope 1).
    fn update_position_price(
        &self,
        mint: &str,
        new_price: f64,
        trade: Option<TradeEvent>,
        source_pool: Option<&str>,
        source_slot: Option<u64>,
    ) -> bool {
        let mut positions = self.positions.write();
        if let Some(pos) = positions.get_mut(mint) {
            if !ironcrab::execution::position_utils::should_apply_position_price_update(
                &pos.pool,
                source_pool,
            ) {
                trace!(
                    mint = %mint,
                    position_pool = %pos.pool,
                    source_pool = ?source_pool,
                    "Skipping price update: source pool != position pool"
                );
                return false;
            }

            if pos.entry_confirmed_slot > 0 {
                let Some(slot) = source_slot else {
                    trace!(
                        mint = %mint,
                        entry_confirmed_slot = pos.entry_confirmed_slot,
                        "Skipping price update: missing source_slot while slot gate active"
                    );
                    return false;
                };
                if slot <= pos.entry_confirmed_slot {
                    trace!(
                        mint = %mint,
                        entry_confirmed_slot = pos.entry_confirmed_slot,
                        source_slot = slot,
                        "Skipping price update: event slot not after entry BUY confirm slot"
                    );
                    return false;
                }
                if slot <= pos.last_price_slot {
                    trace!(
                        mint = %mint,
                        last_price_slot = pos.last_price_slot,
                        source_slot = slot,
                        "Skipping price update: non-monotonic slot sequence"
                    );
                    return false;
                }
            }

            if let Some(t) = trade {
                pos.update_from_trade_price(new_price);
                pos.record_trade(t);
            } else {
                pos.update_mark_price(new_price);
            }
            if let Some(s) = source_slot {
                pos.last_price_slot = s;
            }
            return true;
        }
        false
    }

    /// Close position (after sell executed)
    /// Also removes the position from JetStream KV
    fn close_position(self: &Arc<Self>, mint: &str) {
        let mint_owned = mint.to_string();
        let (removed, closed_pool) = {
            let mut positions = self.positions.write();
            let pool = positions.get(mint).map(|p| p.pool.clone());
            let removed = positions.remove(mint);
            (removed, pool)
        };

        if let Some(pos) = removed {
            let pnl = pos.pnl_pct();
            let hold_secs = pos.entry_time.elapsed().as_secs();
            info!(
                mint = %pos.mint,
                pnl_pct = pnl,
                hold_time_secs = hold_secs,
                "📉 Position closed"
            );
        }

        // Drop bonding cache for this mint when the position is gone, unless a BUY is already
        // pending again (re-entry lifecycle must keep latest_bonding for confirm/open).
        // Same for Scope B sticky maps (migration + pool reserve hints).
        if !self.pending_buy_mint_index.read().contains_key(mint) {
            self.latest_bonding_by_mint.write().remove(mint);
            self.clear_scope_b_sticky_state_for_mint(mint);
        }

        if let Some(ref pool) = closed_pool {
            self.maybe_queue_removed_after_position_closed(mint, pool.as_str());
            self.flush_momentum_active_pool_publish_queue();
        }

        // Delete from KV asynchronously (fire-and-forget)
        let ctx = Arc::clone(self);
        tokio::spawn(async move {
            ctx.delete_position_from_kv(&mint_owned).await;
        });
    }

    // =========================================================================
    // JetStream KV Position Persistence
    // =========================================================================

    /// Get or initialize the JetStream KV store for positions
    async fn get_position_kv(&self) -> Option<&async_nats::jetstream::kv::Store> {
        let nats = self.nats.as_ref()?;

        // Try to get cached store first
        if let Some(store) = self.position_kv.get() {
            return Some(store);
        }

        // Initialize the KV bucket
        match nats.get_or_create_kv_bucket(POSITION_KV_BUCKET).await {
            Ok(store) => {
                // Try to set it (ignore if another task beat us)
                let _ = self.position_kv.set(store);
                self.position_kv.get()
            }
            Err(e) => {
                error!(error = %e, "Failed to create position KV bucket");
                None
            }
        }
    }

    /// Save a position to JetStream KV
    async fn save_position_to_kv(&self, mint: &str, tracker: &PositionTracker) {
        let Some(nats) = self.nats.as_ref() else {
            return;
        };
        let Some(store) = self.get_position_kv().await else {
            return;
        };

        let persisted = PersistedPosition::from_tracker(tracker);
        match nats.kv_put(store, mint, &persisted).await {
            Ok(rev) => {
                debug!(mint = %mint, revision = rev, "Position saved to KV");
            }
            Err(e) => {
                warn!(mint = %mint, error = %e, "Failed to save position to KV");
            }
        }
    }

    /// Delete a position from JetStream KV
    async fn delete_position_from_kv(&self, mint: &str) {
        let Some(nats) = self.nats.as_ref() else {
            return;
        };
        let Some(store) = self.get_position_kv().await else {
            return;
        };

        match nats.kv_delete(store, mint).await {
            Ok(()) => {
                debug!(mint = %mint, "Position deleted from KV");
            }
            Err(e) => {
                // Not a critical error - position might not exist in KV
                debug!(mint = %mint, error = %e, "Failed to delete position from KV (may not exist)");
            }
        }
    }

    /// Load all positions from JetStream KV on startup
    /// Note: Currently unused as we load directly in main for cleaner error handling,
    /// but kept for potential future use (e.g., hot-reload scenarios).
    #[allow(dead_code)]
    async fn load_positions_from_kv(&self) -> HashMap<String, PositionTracker> {
        let Some(nats) = self.nats.as_ref() else {
            info!("NATS not connected, skipping KV position recovery");
            return HashMap::new();
        };
        let Some(store) = self.get_position_kv().await else {
            warn!("Failed to get position KV store");
            return HashMap::new();
        };

        match nats.kv_get_all::<PersistedPosition>(store).await {
            Ok(persisted_positions) => {
                let mut positions = HashMap::new();
                for (mint, persisted) in persisted_positions {
                    let tracker = persisted.to_tracker();
                    let hold_secs = tracker.entry_time.elapsed().as_secs();
                    info!(
                        mint = %mint,
                        pool = %tracker.pool,
                        dex = %tracker.dex,
                        entry_price = tracker.entry_price,
                        sol_invested = tracker.sol_invested,
                        hold_secs = hold_secs,
                        "🔄 Position recovered from JetStream KV"
                    );
                    positions.insert(mint, tracker);
                }
                info!(
                    recovered = positions.len(),
                    "Position recovery from JetStream KV complete"
                );
                positions
            }
            Err(e) => {
                warn!(error = %e, "Failed to load positions from KV");
                HashMap::new()
            }
        }
    }

    /// Check all positions for exit signals
    /// Returns exit signals without marking exit_generated - caller must call mark_exit_generated() after successful publish
    fn check_for_exits(&self) -> Vec<(String, String, String, String, String, u64)> {
        // Returns: Vec<(mint, pool, dex, exit_type, reason, token_amount)>
        let config = self.config.read().clone();
        let chain_head_slot = self
            .last_event_slot
            .load(std::sync::atomic::Ordering::Relaxed)
            .max(1);

        // Snapshot tracker-derived hard-exit signals BEFORE locking positions,
        // to avoid lock-order deadlocks (market event handling locks trackers then positions).
        #[derive(Clone, Debug)]
        struct TrackerExitSignals {
            lp_removal_slot: Option<u64>,
            lp_removal_observed_at: Option<Instant>,
            dev_sell_slot: Option<u64>,
            dev_sell_observed_at: Option<Instant>,
            dev_sold_sig: Option<String>,
            dev_sold_sol: Option<u64>,
        }

        let (tracker_signals, trades_per_min_by_tracker_key): (
            HashMap<String, TrackerExitSignals>,
            HashMap<String, f64>,
        ) = {
            let trackers = self.token_trackers.read();
            let mut by_mint: HashMap<String, TrackerExitSignals> = HashMap::new();
            let mut tpm_by_tracker_key: HashMap<String, f64> = HashMap::new();
            for t in trackers.values() {
                let tpm = t.calculate_metrics(&config, chain_head_slot).trades_per_min;
                tpm_by_tracker_key.insert(Self::tracker_storage_key(&t.mint, &t.pool), tpm);
                let mint_key = t.mint.clone();
                let mut dev_sell_slot: Option<u64> = None;
                let mut dev_sell_observed_at: Option<Instant> = None;
                let mut dev_sold_sig: Option<String> = None;
                let mut dev_sold_sol: Option<u64> = None;

                if let Some(dev) = t.dev_wallet.as_ref() {
                    if let Some(last) = t
                        .trades
                        .iter()
                        .rev()
                        .find(|tr| !tr.is_buy && tr.trader == *dev)
                    {
                        dev_sell_observed_at = Some(last.timestamp);
                        if last.slot > 0 {
                            dev_sell_slot = Some(last.slot);
                        }
                        dev_sold_sig = Some(last.signature.clone());
                        dev_sold_sol = Some(last.sol_amount);
                    }
                }

                let incoming = TrackerExitSignals {
                    lp_removal_slot: t.lp_removal_slot,
                    lp_removal_observed_at: t.lp_removal_observed_at,
                    dev_sell_slot,
                    dev_sell_observed_at,
                    dev_sold_sig,
                    dev_sold_sol,
                };

                use std::collections::hash_map::Entry;
                match by_mint.entry(mint_key) {
                    Entry::Occupied(mut o) => {
                        let e = o.get_mut();
                        e.lp_removal_slot = match (e.lp_removal_slot, incoming.lp_removal_slot) {
                            (Some(a), Some(b)) => Some(a.max(b)),
                            (Some(a), None) => Some(a),
                            (None, Some(b)) => Some(b),
                            (None, None) => None,
                        };
                        // LP wallclock aggregate: latest wins (see [`merge_lp_removal_observed_at_mint_aggregate`]).
                        e.lp_removal_observed_at = merge_lp_removal_observed_at_mint_aggregate(
                            e.lp_removal_observed_at,
                            incoming.lp_removal_observed_at,
                        );
                        // dev_sell_observed_at: already “latest wins” (higher slot branch + `in_obs > cur` below).
                        if let Some(b) = incoming.dev_sell_slot {
                            if e.dev_sell_slot.map(|a| b > a).unwrap_or(true) {
                                e.dev_sell_slot = Some(b);
                                e.dev_sell_observed_at =
                                    incoming.dev_sell_observed_at.or(e.dev_sell_observed_at);
                                e.dev_sold_sig = incoming.dev_sold_sig;
                                e.dev_sold_sol = incoming.dev_sold_sol;
                            }
                        } else if let Some(in_obs) = incoming.dev_sell_observed_at {
                            if e.dev_sell_slot.is_none() {
                                let replace = e
                                    .dev_sell_observed_at
                                    .map(|cur| in_obs > cur)
                                    .unwrap_or(true);
                                if replace {
                                    e.dev_sell_observed_at = Some(in_obs);
                                    e.dev_sold_sig = incoming.dev_sold_sig;
                                    e.dev_sold_sol = incoming.dev_sold_sol;
                                }
                            }
                        }
                    }
                    Entry::Vacant(v) => {
                        v.insert(incoming);
                    }
                }
            }
            (by_mint, tpm_by_tracker_key)
        };

        // FIX-30b: No longer block exit checks on pending BUY intents.
        // All exits fire immediately. If momentum fades, we exit now rather than
        // waiting for a scale-in that no longer makes sense. Orphaned Buy Recovery
        // (handle_execution_result) handles any BUY that confirms after exit.

        let pending_sell_mints: std::collections::HashSet<String> = self
            .pending_intents
            .read()
            .values()
            .filter(|p| p.side == TradeSide::Sell)
            .map(|p| p.mint.clone())
            .collect();

        let mut positions = self.positions.write();
        let mut exits = Vec::new();

        for (mint, pos) in positions.iter_mut() {
            if pos.exit_generated {
                continue;
            }
            if pending_sell_mints.contains(mint) {
                continue;
            }

            // Hard exits (post-entry): dev sells, LP removed.
            if let Some(sig) = tracker_signals.get(mint) {
                if let Some(lp_s) = sig.lp_removal_slot {
                    let lp_window_slots = momentum_secs_to_slot_span(config.lp_removal_window_secs);
                    let lp_chain_recent = config.lp_removal_window_secs == 0
                        || chain_head_slot.saturating_sub(lp_s) <= lp_window_slots;

                    let lp_after_entry =
                        pos.entry_confirmed_slot > 0 && lp_s > pos.entry_confirmed_slot;
                    // Legacy: `last_price_slot` advances on every mark — comparing exit slots to it
                    // suppresses hard exits once any newer trade arrived.
                    let lp_after_entry_legacy = pos.entry_confirmed_slot == 0
                        && sig
                            .lp_removal_observed_at
                            .map(|obs| obs.checked_duration_since(pos.entry_time).is_some())
                            .unwrap_or(false);

                    if lp_chain_recent && (lp_after_entry || lp_after_entry_legacy) {
                        // Note: exit_generated is set by caller after successful publish
                        exits.push((
                            mint.clone(),
                            pos.pool.clone(),
                            pos.dex.clone(),
                            "LP_REMOVAL".to_string(),
                            format!(
                                "LP removed post-entry (lp_slot={}, entry_confirmed_slot={}, head_slot={})",
                                lp_s, pos.entry_confirmed_slot, chain_head_slot
                            ),
                            pos.token_amount,
                        ));
                        continue;
                    }
                } else if let Some(obs) = sig.lp_removal_observed_at {
                    // Slot unknown (e.g. Geyser `event.slot` missing): wallclock window + post-entry
                    // ordering so LP_REMOVAL hard exits cannot be skipped silently.
                    let lp_chain_recent = config.lp_removal_window_secs == 0
                        || obs.elapsed().as_secs() <= config.lp_removal_window_secs;
                    let lp_post_entry = obs.checked_duration_since(pos.entry_time).is_some();

                    if lp_chain_recent && lp_post_entry {
                        exits.push((
                            mint.clone(),
                            pos.pool.clone(),
                            pos.dex.clone(),
                            "LP_REMOVAL".to_string(),
                            format!(
                                "LP removed post-entry (lp_slot=unknown, wallclock, entry_confirmed_slot={}, head_slot={})",
                                pos.entry_confirmed_slot, chain_head_slot
                            ),
                            pos.token_amount,
                        ));
                        continue;
                    }
                }

                if let Some(ds) = sig.dev_sell_slot {
                    let dev_after_buy =
                        pos.entry_confirmed_slot > 0 && ds > pos.entry_confirmed_slot;
                    let dev_after_buy_legacy = pos.entry_confirmed_slot == 0
                        && sig
                            .dev_sell_observed_at
                            .map(|obs| obs.checked_duration_since(pos.entry_time).is_some())
                            .unwrap_or(false);

                    if dev_after_buy || dev_after_buy_legacy {
                        let sig_s = sig.dev_sold_sig.as_deref().unwrap_or("<unknown>");
                        let sol = sig.dev_sold_sol.unwrap_or(0);
                        // Note: exit_generated is set by caller after successful publish
                        exits.push((
                            mint.clone(),
                            pos.pool.clone(),
                            pos.dex.clone(),
                            "DEV_SELL".to_string(),
                            format!(
                                "Dev sold post-entry (dev_sell_slot={}, entry_confirmed_slot={}, sig={}, sol={:.4})",
                                ds, pos.entry_confirmed_slot, sig_s,
                                sol as f64 / 1_000_000_000.0
                            ),
                            pos.token_amount,
                        ));
                        continue;
                    }
                } else if let Some(dev_obs) = sig.dev_sell_observed_at {
                    let dev_post_buy = dev_obs.checked_duration_since(pos.entry_time).is_some();

                    if dev_post_buy {
                        let sig_s = sig.dev_sold_sig.as_deref().unwrap_or("<unknown>");
                        let sol = sig.dev_sold_sol.unwrap_or(0);
                        exits.push((
                            mint.clone(),
                            pos.pool.clone(),
                            pos.dex.clone(),
                            "DEV_SELL".to_string(),
                            format!(
                                "Dev sold post-entry (dev_sell_slot=unknown, wallclock, entry_confirmed_slot={}, sig={}, sol={:.4})",
                                pos.entry_confirmed_slot, sig_s,
                                sol as f64 / 1_000_000_000.0
                            ),
                            pos.token_amount,
                        ));
                        continue;
                    }
                }
            }

            let exit_q = self.executable_exit_quote(pos, None);
            let live_trades_per_min = trades_per_min_by_tracker_key
                .get(&Self::tracker_storage_key(mint, &pos.pool))
                .copied();
            if let Some((exit_type, reason)) = pos.should_exit(
                &config,
                exit_q.as_ref(),
                "exit_scan",
                chain_head_slot,
                live_trades_per_min,
            ) {
                // Note: exit_generated is set by caller after successful publish
                exits.push((
                    mint.clone(),
                    pos.pool.clone(),
                    pos.dex.clone(),
                    exit_type,
                    reason,
                    pos.token_amount,
                ));
            }
        }
        drop(positions);

        // FIX-30b: Cancel pending BUY intents for mints that are exiting.
        // Scale-in makes no sense when we're trying to exit.
        if !exits.is_empty() {
            let exit_mints: std::collections::HashSet<&str> =
                exits.iter().map(|(m, ..)| m.as_str()).collect();
            let mut pending = self.pending_intents.write();
            pending
                .retain(|_, p| !(exit_mints.contains(p.mint.as_str()) && p.side == TradeSide::Buy));
        }

        exits
    }

    fn collect_timed_exit_reconcile_candidates(
        &self,
        now: Instant,
    ) -> Vec<TimedExitReconcileCandidate> {
        let config = self.config.read().clone();
        if config.max_hold_time_secs == 0 {
            return Vec::new();
        }

        let pending_sells: std::collections::HashSet<String> = self
            .pending_intents
            .read()
            .values()
            .filter(|p| p.side == TradeSide::Sell)
            .map(|p| p.mint.clone())
            .collect();

        let mint_pools = self.mint_pools.read();
        let positions = self.positions.read();
        let mut candidates = Vec::new();

        for (mint, pos) in positions.iter() {
            let hold_secs = pos.entry_time.elapsed().as_secs();
            if hold_secs < config.max_hold_time_secs {
                continue;
            }

            if pending_sells.contains(mint) {
                continue;
            }

            // FIX-30c: Reconcile both cases:
            // a) exit_generated==true but never confirmed (retry after cooldown)
            // b) exit_generated==false — exit was never generated (e.g. was blocked)
            let last_exit_age = pos
                .exit_generated_at
                .and_then(|ts| now.checked_duration_since(ts).map(|d| d.as_secs()));

            if pos.exit_generated {
                if let Some(age) = last_exit_age {
                    // Phase 2: Progressive cooldowns based on sell_fail_count across pools
                    let sell_fail_count: u32 = mint_pools
                        .get(mint)
                        .map(|pools| pools.iter().map(|p| p.sell_fail_count).max().unwrap_or(0))
                        .unwrap_or(0);
                    let retry_after_secs: u64 = match sell_fail_count {
                        0 => 15,
                        1 => 30,
                        2 => 60,
                        _ => 120,
                    };
                    if age < retry_after_secs {
                        continue;
                    }
                }
            }

            let (pool, dex) = if !pos.pool.is_empty() && !pos.dex.is_empty() {
                (pos.pool.clone(), pos.dex.clone())
            } else {
                continue;
            };

            candidates.push(TimedExitReconcileCandidate {
                mint: mint.clone(),
                pool,
                dex,
                token_amount: pos.token_amount,
                hold_secs,
                last_exit_age_secs: last_exit_age,
            });
        }

        candidates
    }

    /// Check for exit signals and publish intents. Call on every price-updating event
    /// (PoolCacheUpdate, Trade) for sub-500ms reaction; strategy_interval is fallback.
    /// `source_event_ts_unix_ms`: reserved for a future path where each published exit intent is
    /// provably tied to one `MarketEvent` header (per-mint causality). **Broad** `check_for_exits()`
    /// runs must pass `None` so `momentum_event_to_intent_publish_ms` is not filled with a
    /// non-causal timestamp. Today all call sites use `None`.
    async fn process_exit_signals(self: &Arc<Self>, source_event_ts_unix_ms: Option<u64>) {
        let exits = self.check_for_exits();
        for (mint, pool, dex, exit_type, reason, token_amount) in exits {
            info!(
                mint = %mint,
                pool = %pool,
                exit_type = %exit_type,
                reason = %reason,
                token_amount = token_amount,
                "🚨 EXIT SIGNAL DETECTED"
            );

            if let Err(e) = generate_and_publish_exit_intent(
                self,
                &mint,
                &pool,
                &dex,
                &exit_type,
                &reason,
                token_amount,
                source_event_ts_unix_ms,
            )
            .await
            {
                error!(
                    error = %e,
                    mint = %mint,
                    "Failed to generate/publish sell intent - will retry on next event"
                );
            }
        }
    }

    async fn reconcile_timed_exits(self: &Arc<Self>) {
        let now = Instant::now();
        let candidates = self.collect_timed_exit_reconcile_candidates(now);
        if candidates.is_empty() {
            return;
        }

        for candidate in candidates {
            // FIX: Call should_exit to get actual exit type (STOP_LOSS/TAKE_PROFIT) instead of
            // always TIME_EXIT. Dashboard then shows correct reason for high-loss exits.
            let config = self.config.read().clone();
            let exit_q = {
                let positions = self.positions.read();
                positions
                    .get(&candidate.mint)
                    .and_then(|p| self.executable_exit_quote(p, None))
            };
            let chain_head_slot = self
                .last_event_slot
                .load(std::sync::atomic::Ordering::Relaxed)
                .max(1);
            let live_tpm = {
                let trackers = self.token_trackers.read();
                trackers
                    .get(&Self::tracker_storage_key(&candidate.mint, &candidate.pool))
                    .map(|t| t.calculate_metrics(&config, chain_head_slot).trades_per_min)
            };

            let exit_signal = {
                let mut positions = self.positions.write();
                if let Some(pos) = positions.get_mut(&candidate.mint) {
                    pos.should_exit(
                        &config,
                        exit_q.as_ref(),
                        "timed_exit_reconcile",
                        chain_head_slot,
                        live_tpm,
                    )
                } else {
                    None
                }
            };

            let Some((exit_type, reason)) = exit_signal else {
                trace!(
                    mint = %candidate.mint,
                    hold_secs = candidate.hold_secs,
                    "timed_exit_reconcile_skipped_active_velocity"
                );
                continue;
            };

            info!(
                mint = %candidate.mint,
                pool = %candidate.pool,
                dex = %candidate.dex,
                exit_type = %exit_type,
                hold_secs = candidate.hold_secs,
                last_exit_age_secs = candidate.last_exit_age_secs,
                "♻️  Retrying exit (reconcile)"
            );

            if let Err(e) = generate_and_publish_exit_intent(
                self,
                &candidate.mint,
                &candidate.pool,
                &candidate.dex,
                &exit_type,
                &reason,
                candidate.token_amount,
                None,
            )
            .await
            {
                error!(
                    error = %e,
                    mint = %candidate.mint,
                    "Failed to publish timed-exit reconcile intent"
                );
            }
        }
    }

    fn shutdown_intent_publish_sender(&self) {
        let _ = self.intent_publish_tx.lock().take();
    }

    fn rollback_exit_intent_after_publish_failure(&self, intent_id: &str, mint: &str) {
        self.pending_intents.write().remove(intent_id);
        let mut positions = self.positions.write();
        if let Some(pos) = positions.get_mut(mint) {
            pos.exit_generated = false;
            pos.exit_generated_at = None;
        }
        warn!(
            mint = %mint,
            intent_id = %intent_id,
            "Rolled back exit intent after JetStream publish failure — will retry"
        );
    }

    fn record_exit_intent_published(&self, mint: &str) {
        self.mark_exit_generated(mint);
        self.exits_generated
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Mark a position's exit as generated (call after successful SELL intent publish)
    fn mark_exit_generated(&self, mint: &str) {
        let mut positions = self.positions.write();
        if let Some(pos) = positions.get_mut(mint) {
            pos.exit_generated = true;
            pos.exit_generated_at = Some(Instant::now());
            debug!(mint = %mint, "Marked position exit_generated=true");
        }
    }

    /// Get position count for heartbeat
    fn position_count(&self) -> usize {
        self.positions.read().len()
    }

    /// Get pending intent count for heartbeat
    fn pending_count(&self) -> usize {
        self.pending_intents.read().len()
    }

    // =========================================================================
    // Pending Intent Management (for execution result handling)
    // =========================================================================

    /// Register a pending BUY intent
    fn register_buy_intent(
        &self,
        intent_id: &str,
        mint: &str,
        pool: &str,
        dex: &str,
        sol_amount: u64,
        entry_kind: Option<EntryKind>,
    ) {
        let mut pending = self.pending_intents.write();
        pending.insert(
            intent_id.to_string(),
            PendingIntent {
                mint: mint.to_string(),
                pool: pool.to_string(),
                dex: dex.to_string(),
                side: TradeSide::Buy,
                entry_kind,
                sol_amount,
                token_amount: 0,
                created_at: Instant::now(),
            },
        );
        debug!(intent_id = %intent_id, mint = %mint, "Registered pending BUY intent");
    }

    /// Register a pending SELL intent
    fn register_sell_intent(
        &self,
        intent_id: &str,
        mint: &str,
        pool: &str,
        dex: &str,
        token_amount: u64,
    ) {
        let mut pending = self.pending_intents.write();
        pending.insert(
            intent_id.to_string(),
            PendingIntent {
                mint: mint.to_string(),
                pool: pool.to_string(),
                dex: dex.to_string(),
                side: TradeSide::Sell,
                entry_kind: None,
                sol_amount: 0,
                token_amount,
                created_at: Instant::now(),
            },
        );
        debug!(intent_id = %intent_id, mint = %mint, "Registered pending SELL intent");
    }

    async fn enqueue_intent_publish(&self, job: IntentPublishJob) -> Result<()> {
        let tx = self
            .intent_publish_tx
            .lock()
            .clone()
            .ok_or_else(|| anyhow::anyhow!("intent publish worker closed"))?;
        match tx.try_send(job) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(job)) => {
                MOMENTUM_INTENT_PUBLISH_BACKPRESSURE_TOTAL
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                match tokio::time::timeout(INTENT_PUBLISH_ENQUEUE_TIMEOUT, tx.send(job)).await {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(_)) => anyhow::bail!("intent publish worker closed"),
                    Err(_) => {
                        MOMENTUM_INTENT_PUBLISH_BACKPRESSURE_TOTAL
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        anyhow::bail!("intent publish queue backpressure timeout");
                    }
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                anyhow::bail!("intent publish worker closed")
            }
        }
    }

    /// Merge a Geyser `BondingCurveProgress` observation (slot-/ts-monotonic). Updates global
    /// cache and syncs open positions.
    fn merge_bonding_curve_progress_geyser(
        &self,
        mint: &str,
        progress_bps: u32,
        complete: bool,
        slot: u64,
        ts_unix_ms: u64,
    ) {
        let cache_relevant = {
            let positions = self.positions.read();
            let pending_idx = self.pending_buy_mint_index.read();
            positions.contains_key(mint) || pending_idx.contains_key(mint)
        };

        if !cache_relevant {
            self.latest_bonding_by_mint.write().remove(mint);
            return;
        }

        {
            let mut map = self.latest_bonding_by_mint.write();
            let accept = match map.get(mint) {
                None => true,
                Some(prev) => bonding_geyser_observation_is_newer(
                    slot,
                    ts_unix_ms,
                    prev.slot,
                    prev.ts_unix_ms,
                ),
            };
            if accept {
                map.insert(
                    mint.to_string(),
                    CachedBondingCurveState {
                        progress_bps,
                        complete,
                        slot,
                        ts_unix_ms,
                    },
                );
            }
        }

        let snapshot = self.latest_bonding_by_mint.read().get(mint).cloned();

        let mut positions = self.positions.write();
        if let Some(pos) = positions.get_mut(mint) {
            if let Some(ref latest) = snapshot {
                // Progress can theoretically move backward on a newer slot (bad parse / race);
                // never regress below the best-known in-position value (matches scale-in max-guard).
                pos.bonding_curve_progress_bps = Some(
                    latest
                        .progress_bps
                        .max(pos.bonding_curve_progress_bps.unwrap_or(0)),
                );
            }
        }
        self.mark_entry_eval_dirty_for_mint(mint);
    }

    /// After successful BUY intent JetStream publish: track lifecycle + bonding cache (not a position).
    fn register_pending_buy_entry_after_publish(&self, meta: PendingBuyPublishMeta<'_>) {
        let mut entries = self.pending_buy_entries.write();
        let mut index = self.pending_buy_mint_index.write();
        if let Some(old_id) = index
            .get(meta.mint)
            .filter(|&id| id != meta.intent_id)
            .cloned()
        {
            entries.remove(&old_id);
        }
        index.insert(meta.mint.to_string(), meta.intent_id.to_string());
        entries.insert(
            meta.intent_id.to_string(),
            PendingBuyEntry {
                mint: meta.mint.to_string(),
                pool: meta.pool.to_string(),
                dex: meta.dex.to_string(),
                entry_kind: meta.entry_kind,
                intended_sol: meta.intended_sol,
                intent_id: meta.intent_id.to_string(),
                signal_slot: meta.signal_slot,
                slot_seen_at_ms: meta.slot_seen_at_ms,
                creator: meta.creator,
                token_program: meta.token_program,
            },
        );
        debug!(
            intent_id = %meta.intent_id,
            mint = %meta.mint,
            "Registered pending BUY entry lifecycle (post-publish)"
        );
        self.mark_entry_eval_dirty_for_mint(meta.mint);
    }

    fn remove_pending_buy_entry_by_intent(&self, intent_id: &str) {
        let removed_mint = {
            let mut entries = self.pending_buy_entries.write();
            if let Some(e) = entries.remove(intent_id) {
                let mut index = self.pending_buy_mint_index.write();
                if index.get(&e.mint).map(|id| id.as_str()) == Some(intent_id) {
                    index.remove(&e.mint);
                }
                debug!(
                    intent_id = %intent_id,
                    mint = %e.mint,
                    "Removed pending BUY entry lifecycle"
                );
                if self.positions.read().get(&e.mint).is_none() {
                    self.latest_bonding_by_mint.write().remove(&e.mint);
                    self.clear_scope_b_sticky_state_for_mint(&e.mint);
                }
                Some(e.mint)
            } else {
                None
            }
        };
        if let Some(m) = removed_mint {
            self.mark_entry_eval_dirty_for_mint(&m);
        }
    }

    #[cfg(test)]
    fn test_pending_buy_entry_present(&self, intent_id: &str, mint: &str) -> bool {
        self.pending_buy_entries.read().contains_key(intent_id)
            && self
                .pending_buy_mint_index
                .read()
                .get(mint)
                .map(|s| s.as_str())
                == Some(intent_id)
    }

    fn clone_latest_bonding_snapshot(&self, mint: &str) -> Option<CachedBondingCurveState> {
        self.latest_bonding_by_mint.read().get(mint).cloned()
    }

    /// Scope B: remove sticky pool hints + migration evidence when a position closes.
    fn clear_scope_b_sticky_state_for_mint(&self, mint: &str) {
        self.latest_pumpfun_migration_complete_by_mint
            .write()
            .remove(mint);
        self.latest_pool_reserve_price_hint_by_mint_pool
            .write()
            .retain(|key, _| key.0 != mint);
    }

    #[cfg(test)]
    pub(crate) fn test_has_pool_reserve_hint(&self, mint: &str, pool: &str) -> bool {
        self.latest_pool_reserve_price_hint_by_mint_pool
            .read()
            .contains_key(&(mint.to_string(), pool.to_string()))
    }

    #[cfg(test)]
    pub(crate) fn test_has_migration_sticky(&self, mint: &str) -> bool {
        self.latest_pumpfun_migration_complete_by_mint
            .read()
            .contains_key(mint)
    }

    /// Scope B: record PumpFun bonding-curve `complete` (migration) with slot-/ts-monotonic merge.
    pub(crate) fn merge_pumpfun_migration_complete_evidence(
        &self,
        mint: &str,
        slot: u64,
        ts_unix_ms: u64,
    ) {
        let mut merged = false;
        {
            let mut map = self.latest_pumpfun_migration_complete_by_mint.write();
            let accept = match map.get(mint) {
                None => true,
                Some(prev) => bonding_geyser_observation_is_newer(
                    slot,
                    ts_unix_ms,
                    prev.slot,
                    prev.ts_unix_ms,
                ),
            };
            if accept {
                map.insert(
                    mint.to_string(),
                    CachedPumpfunMigrationCompleteEvidence { slot, ts_unix_ms },
                );
                merged = true;
            }
        }
        if merged {
            self.mark_entry_eval_dirty_for_mint(mint);
        }
    }

    /// Scope B: migration evidence from execution-engine observations (6005, etc.) when Geyser slot
    /// metadata may be missing — uses `ExecutionResult.confirmed_slot` or last event slot.
    fn record_pumpfun_migration_complete_evidence_from_execution_observation(
        &self,
        mint: &str,
        result: &ExecutionResult,
    ) {
        let slot = result
            .confirmed_slot
            .unwrap_or_else(|| {
                self.last_event_slot
                    .load(std::sync::atomic::Ordering::Relaxed)
            })
            .max(
                self.last_event_slot
                    .load(std::sync::atomic::Ordering::Relaxed),
            );
        let ts_wall = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let ts = ts_wall.max(
            self.last_event_ts_ms
                .load(std::sync::atomic::Ordering::Relaxed),
        );
        self.merge_pumpfun_migration_complete_evidence(mint, slot, ts);
    }

    /// Shared reserve → `tokens_per_sol` pipeline for sticky hints and live position marks (single validation).
    fn derive_tokens_per_sol_from_pool_cache_update(
        &self,
        update: &ironcrab::ipc::PoolCacheUpdate,
    ) -> Option<(String, f64, u8, f64, f64)> {
        if update.base_reserve == 0 || update.quote_reserve == 0 {
            return None;
        }
        if !pool_cache_has_exactly_one_wsol_leg(&update.base_mint, &update.quote_mint) {
            return None;
        }
        const SOL_WSOL_MINT: &str = "So11111111111111111111111111111111111111112";
        let (token_mint, token_reserve, sol_reserve, token_decimals) =
            if update.base_mint == SOL_WSOL_MINT {
                let decimals = self
                    .mint_infos
                    .read()
                    .get(&update.quote_mint)
                    .map(|m| m.decimals)
                    .unwrap_or(6);
                (
                    update.quote_mint.clone(),
                    update.quote_reserve,
                    update.base_reserve,
                    decimals,
                )
            } else {
                let decimals = self
                    .mint_infos
                    .read()
                    .get(&update.base_mint)
                    .map(|m| m.decimals)
                    .unwrap_or(6);
                (
                    update.base_mint.clone(),
                    update.base_reserve,
                    update.quote_reserve,
                    decimals,
                )
            };
        let token_ui = token_reserve as f64 / 10f64.powi(token_decimals as i32);
        let sol_ui = sol_reserve as f64 / 1_000_000_000.0;
        if sol_ui <= 0.0 || !sol_ui.is_finite() || token_ui <= 0.0 || !token_ui.is_finite() {
            return None;
        }
        let tokens_per_sol = token_ui / sol_ui;
        if !tokens_per_sol.is_finite() || tokens_per_sol <= 0.0 {
            return None;
        }
        Some((token_mint, tokens_per_sol, token_decimals, token_ui, sol_ui))
    }

    fn merge_latest_pool_reserve_price_hint_from_derived(
        &self,
        update: &ironcrab::ipc::PoolCacheUpdate,
        token_mint: &str,
        tokens_per_sol: f64,
    ) {
        let key = (token_mint.to_string(), update.pool_address.clone());
        let mut map = self.latest_pool_reserve_price_hint_by_mint_pool.write();
        let accept = match map.get(&key) {
            None => true,
            Some(prev) => bonding_geyser_observation_is_newer(
                update.geyser_slot,
                update.header.ts_unix_ms,
                prev.slot,
                prev.ts_unix_ms,
            ),
        };
        if accept {
            map.insert(
                key,
                CachedPoolReservePriceHint {
                    pool_address: update.pool_address.clone(),
                    dex: update.dex.clone(),
                    tokens_per_sol,
                    slot: update.geyser_slot,
                    ts_unix_ms: update.header.ts_unix_ms,
                },
            );
        }
    }

    /// Scope B: merge a `PoolCacheUpdate` into the sticky reserve mark map (same monotonic rules as bonding).
    #[cfg(test)]
    pub(crate) fn merge_latest_pool_reserve_price_hint_from_update(
        &self,
        update: &ironcrab::ipc::PoolCacheUpdate,
    ) {
        let Some((ref token_mint, tokens_per_sol, _, _, _)) =
            self.derive_tokens_per_sol_from_pool_cache_update(update)
        else {
            return;
        };
        self.merge_latest_pool_reserve_price_hint_from_derived(update, token_mint, tokens_per_sol);
    }

    /// Scope B: apply `mint_infos`, cached bonding max-guard, and pool reserve hints to one position.
    /// Respects I-13 and Scope-1 slot monotonicity. Returns true when bonding or mark price changed
    /// (caller may trigger `process_exit_signals`).
    pub(crate) fn apply_latest_sticky_state_to_position(
        &self,
        mint: &str,
        pos: &mut PositionTracker,
    ) -> bool {
        let mut exit_maybe = false;

        if let Some(mi) = self.mint_infos.read().get(mint) {
            if mi.decimals > 0 && pos.token_decimals == 0 {
                pos.token_decimals = mi.decimals;
            }
            if pos
                .token_program
                .as_ref()
                .map(|s| s.is_empty())
                .unwrap_or(true)
                && !mi.token_program.is_empty()
            {
                pos.token_program = Some(mi.token_program.clone());
            }
        }

        if let Some(latest) = self.latest_bonding_by_mint.read().get(mint) {
            let merged_bps = latest
                .progress_bps
                .max(pos.bonding_curve_progress_bps.unwrap_or(0));
            if pos.bonding_curve_progress_bps != Some(merged_bps) {
                pos.bonding_curve_progress_bps = Some(merged_bps);
                exit_maybe = true;
            }
        }

        let hint_key = (mint.to_string(), pos.pool.clone());
        if let Some(hint) = self
            .latest_pool_reserve_price_hint_by_mint_pool
            .read()
            .get(&hint_key)
            .cloned()
        {
            if Self::try_apply_reserve_hint_as_position_price(pos, &hint) {
                exit_maybe = true;
            }
        }

        exit_maybe
    }

    /// Resolve `entry_kind` for orphaned BUY recovery (pending lifecycle or intent metadata).
    fn resolve_orphan_entry_kind(
        intent_id: &str,
        metadata: &HashMap<String, String>,
        pending_entries: &HashMap<String, PendingBuyEntry>,
    ) -> EntryKind {
        if let Some(entry) = pending_entries.get(intent_id) {
            if let Some(kind) = entry.entry_kind {
                return kind;
            }
        }
        match metadata.get("entry_kind").map(|s| s.as_str()) {
            Some("scale_in") => EntryKind::ScaleIn,
            Some("probe") => EntryKind::Probe,
            _ => EntryKind::Probe,
        }
    }

    /// After orphan `open_position`, align `TokenTracker` with the normal BUY-confirm state machine.
    fn apply_orphan_buy_tracker_state_after_recovery(
        &self,
        mint: &str,
        pool: &str,
        entry_kind: EntryKind,
    ) {
        let tk = Self::tracker_storage_key(mint, pool);
        let mut trackers = self.token_trackers.write();
        let Some(tr) = trackers.get_mut(&tk) else {
            return;
        };
        let filled_at = Instant::now();
        match entry_kind {
            EntryKind::ScaleIn => {
                tr.state = TrackerState::PositionOpenFull { filled_at };
                ironcrab::metrics::record_momentum_orphan_scale_in_recovery_total();
            }
            EntryKind::Probe => {
                tr.state = TrackerState::PositionOpenProbe { filled_at };
                ironcrab::metrics::record_momentum_orphan_probe_recovery_total();
            }
        }
        drop(trackers);
        self.mark_entry_eval_dirty_key(&tk);
    }

    fn cache_wallet_balance_snapshot_raw(&self, mint: &str, balance_raw: u64) {
        self.latest_wallet_balance_raw_by_mint
            .write()
            .insert(mint.to_string(), balance_raw);
    }

    /// Scope 57 interim exit sizing (PA-5 follow-up): prefer JetStream wallet snapshot when overlay drifts.
    fn resolve_exit_token_amount_raw(&self, mint: &str, hint_amount: u64) -> u64 {
        let overlay = self
            .positions
            .read()
            .get(mint)
            .map(|p| p.token_amount)
            .filter(|t| *t > 0)
            .unwrap_or(hint_amount);

        match self
            .latest_wallet_balance_raw_by_mint
            .read()
            .get(mint)
            .copied()
            .filter(|a| *a > 0)
        {
            Some(auth) => auth,
            None => {
                ironcrab::metrics::record_momentum_exit_amount_overlay_only_total();
                overlay
            }
        }
    }

    fn try_apply_reserve_hint_as_position_price(
        pos: &mut PositionTracker,
        hint: &CachedPoolReservePriceHint,
    ) -> bool {
        if !ironcrab::execution::position_utils::should_apply_position_price_update(
            &pos.pool,
            Some(&hint.pool_address),
        ) {
            return false;
        }
        if pos.entry_confirmed_slot > 0 {
            if hint.slot <= pos.entry_confirmed_slot {
                return false;
            }
            if hint.slot <= pos.last_price_slot {
                return false;
            }
        }
        if hint.tokens_per_sol <= 0.0 || !hint.tokens_per_sol.is_finite() {
            return false;
        }
        pos.update_mark_price(hint.tokens_per_sol);
        if hint.slot > 0 {
            pos.last_price_slot = pos.last_price_slot.max(hint.slot);
        }
        true
    }

    /// Handle execution result from execution-engine
    fn handle_execution_result(self: &Arc<Self>, result: &ExecutionResult) {
        // Find the pending intent by id (source is not authoritative).
        let pending_opt = {
            let mut pending = self.pending_intents.write();
            pending.remove(&result.intent_id)
        };

        let Some(pending) = pending_opt else {
            // === Liquidation / external sell handling ===
            // Liquidation sells are created by execution-engine (not momentum-bot),
            // so they won't be in pending_intents. When a confirmed sell from
            // execution-engine arrives, close the matching position by token_mint.
            // This prevents ghost positions that block max_open_positions.
            if result.status == ExecutionStatus::Confirmed
                && result.intent_id.starts_with("liquidation-")
            {
                if let Some(ref mint) = result.token_mint {
                    let has_position = self.positions.read().contains_key(mint);
                    if has_position {
                        info!(
                            intent_id = %result.intent_id,
                            mint = %mint,
                            source = %result.source,
                            signature = ?result.signature,
                            "LIQUIDATION SELL CONFIRMED (external) - Closing position"
                        );
                        self.close_position(mint);
                    } else {
                        debug!(
                            intent_id = %result.intent_id,
                            mint = %mint,
                            "Liquidation sell confirmed but no matching position found"
                        );
                    }
                } else {
                    warn!(
                        intent_id = %result.intent_id,
                        "Liquidation sell confirmed but token_mint missing in ExecutionResult"
                    );
                }
            }
            // === Orphaned buy recovery ===
            // If a confirmed BUY arrives after cleanup_stale_pending() removed
            // the pending intent (>2min), recover the position from the
            // ExecutionResult. New positions still need TokenTracker (pool/dex);
            // existing positions use `PositionTracker` (pool/dex/creator) — I-12.
            else if result.status == ExecutionStatus::Confirmed
                && result.metadata.get("side").map(|s| s.as_str()) == Some("BUY")
            {
                if let Some(ref mint) = result.token_mint {
                    if let Some(ref fill_out) = result.fill_out {
                        let idem_inserted = self
                            .orphaned_recovered_intent_ids
                            .write()
                            .insert(result.intent_id.clone());
                        if !idem_inserted {
                            debug!(
                                intent_id = %result.intent_id,
                                mint = %mint,
                                "Orphaned BUY: duplicate execution result (already applied) — skip"
                            );
                        } else {
                            let sol_invested = result
                                .wallet_sol_delta_lamports
                                .map(|d| d.unsigned_abs() as u64)
                                .or_else(|| result.fill_in.as_ref().map(|a| a.raw))
                                .unwrap_or(0);

                            let token_decimals = self
                                .mint_infos
                                .read()
                                .get(mint)
                                .map(|m| m.decimals)
                                .unwrap_or(fill_out.decimals);

                            let sol_ui = result
                                .fill_in
                                .as_ref()
                                .map(|a| a.ui_f64())
                                .unwrap_or(sol_invested as f64 / 1e9)
                                .max(0.0);
                            let tok_ui = fill_out.ui_f64().max(0.0);
                            let entry_price = if sol_ui > 0.0 { tok_ui / sol_ui } else { 1.0 };

                            let token_program = result
                                .metadata
                                .get("token_program")
                                .cloned()
                                .filter(|tp| !tp.is_empty())
                                .or_else(|| {
                                    self.mint_infos
                                        .read()
                                        .get(mint)
                                        .map(|m| m.token_program.clone())
                                        .filter(|tp| !tp.is_empty())
                                });

                            // Optional TokenTracker: upgrade creator for pumpfun, or new-position routing.
                            let tracker_info: Option<(String, String, Option<String>)> = {
                                let pending_snap = self
                                    .pending_buy_entries
                                    .read()
                                    .get(&result.intent_id)
                                    .cloned();
                                let trackers = self.token_trackers.read();
                                if let Some(ref e) = pending_snap {
                                    let tk = Self::tracker_storage_key(&e.mint, &e.pool);
                                    let creator = if e.dex == "pumpfun" {
                                        trackers.get(&tk).and_then(|tr| tr.dev_wallet.clone())
                                    } else {
                                        None
                                    };
                                    Some((e.pool.clone(), e.dex.clone(), creator))
                                } else if let Some(pool_addr) = result
                                    .metadata
                                    .get("pool")
                                    .cloned()
                                    .or_else(|| result.metadata.get("pools").cloned())
                                {
                                    let tk = Self::tracker_storage_key(
                                        mint.as_str(),
                                        pool_addr.as_str(),
                                    );
                                    trackers.get(&tk).map(|tr| {
                                        let creator = if tr.dex == "pumpfun" {
                                            tr.dev_wallet.clone()
                                        } else {
                                            None
                                        };
                                        (tr.pool.clone(), tr.dex.clone(), creator)
                                    })
                                } else {
                                    trackers
                                        .values()
                                        .find(|t| t.mint == mint.as_str())
                                        .map(|tr| {
                                            let creator = if tr.dex == "pumpfun" {
                                                tr.dev_wallet.clone()
                                            } else {
                                                None
                                            };
                                            (tr.pool.clone(), tr.dex.clone(), creator)
                                        })
                                }
                            };

                            let already_has_position: bool;
                            let existing: Option<PositionTracker> = {
                                let positions = self.positions.read();
                                let has = positions.contains_key(mint);
                                let snap = positions.get(mint).cloned();
                                already_has_position = has;
                                snap
                            };

                            if already_has_position {
                                // Existing position: pool/dex/creator from `PositionTracker` (restart-safe).
                                // TokenTracker is optional (e.g. dev_wallet for missing pumpfun creator).
                                let from_pos: Option<(String, String, Option<String>)> =
                                    existing.as_ref().and_then(|pos| {
                                        if pos.pool.is_empty() || pos.dex.is_empty() {
                                            return None;
                                        }
                                        let pool = pos.pool.clone();
                                        let dex = pos.dex.clone();
                                        let mut creator = pos.creator.clone();
                                        if pos.dex == "pumpfun" && creator.is_none() {
                                            if let Some((_, tr_dex, tr_c)) = tracker_info.as_ref() {
                                                if tr_dex == "pumpfun" {
                                                    creator = tr_c.clone();
                                                }
                                            }
                                        }
                                        Some((pool, dex, creator))
                                    });
                                let used_position_routing = from_pos.is_some();
                                if let Some((pool, dex, creator)) = from_pos.or(tracker_info) {
                                    let entry_kind = {
                                        let entries = self.pending_buy_entries.read();
                                        Self::resolve_orphan_entry_kind(
                                            &result.intent_id,
                                            &result.metadata,
                                            &entries,
                                        )
                                    };
                                    info!(
                                        intent_id = %result.intent_id,
                                        mint = %mint,
                                        pool = %pool,
                                        dex = %dex,
                                        sol_invested,
                                        token_amount_raw = fill_out.raw,
                                        used_position_routing = used_position_routing,
                                        entry_kind = ?entry_kind,
                                        "⚠️ ORPHANED BUY APPLIED TO EXISTING POSITION — scale-in fill \
                                         recovered after pending intent was dropped"
                                    );
                                    let initial_bonding = self.clone_latest_bonding_snapshot(mint);
                                    self.remove_pending_buy_entry_by_intent(&result.intent_id);
                                    self.open_position(OpenPositionParams {
                                        mint,
                                        pool: &pool,
                                        dex: &dex,
                                        entry_price,
                                        token_decimals,
                                        token_amount: fill_out.raw,
                                        sol_invested,
                                        token_program,
                                        creator,
                                        entry_confirmed_slot: entry_confirmed_slot_from_execution(
                                            result,
                                        ),
                                        initial_bonding,
                                    });
                                    self.apply_orphan_buy_tracker_state_after_recovery(
                                        mint, &pool, entry_kind,
                                    );
                                    let ctx_exit = Arc::clone(self);
                                    tokio::spawn(async move {
                                        ctx_exit.process_exit_signals(None).await;
                                    });
                                } else {
                                    self.remove_pending_buy_entry_by_intent(&result.intent_id);
                                    self.orphaned_recovered_intent_ids
                                        .write()
                                        .remove(&result.intent_id);
                                    warn!(
                                        intent_id = %result.intent_id,
                                        mint = %mint,
                                        "Orphaned scale-in BUY: could not resolve pool/dex (position \
                                         empty and no TokenTracker). I-12: not silently ignored"
                                    );
                                }
                            } else if let Some((pool, dex, creator)) = tracker_info {
                                let entry_kind = {
                                    let entries = self.pending_buy_entries.read();
                                    Self::resolve_orphan_entry_kind(
                                        &result.intent_id,
                                        &result.metadata,
                                        &entries,
                                    )
                                };
                                warn!(
                                    intent_id = %result.intent_id,
                                    mint = %mint,
                                    pool = %pool,
                                    dex = %dex,
                                    sol_invested,
                                    token_amount = fill_out.raw,
                                    entry_kind = ?entry_kind,
                                    "⚠️ ORPHANED BUY RECOVERED — pending intent expired, \
                                     creating position from ExecutionResult"
                                );

                                let initial_bonding = self.clone_latest_bonding_snapshot(mint);
                                self.remove_pending_buy_entry_by_intent(&result.intent_id);
                                self.open_position(OpenPositionParams {
                                    mint,
                                    pool: &pool,
                                    dex: &dex,
                                    entry_price,
                                    token_decimals,
                                    token_amount: fill_out.raw,
                                    sol_invested,
                                    token_program,
                                    creator,
                                    entry_confirmed_slot: entry_confirmed_slot_from_execution(
                                        result,
                                    ),
                                    initial_bonding,
                                });
                                self.apply_orphan_buy_tracker_state_after_recovery(
                                    mint, &pool, entry_kind,
                                );
                                let ctx_exit = Arc::clone(self);
                                tokio::spawn(async move {
                                    ctx_exit.process_exit_signals(None).await;
                                });
                            } else {
                                self.remove_pending_buy_entry_by_intent(&result.intent_id);
                                self.orphaned_recovered_intent_ids
                                    .write()
                                    .remove(&result.intent_id);
                                warn!(
                                    intent_id = %result.intent_id,
                                    mint = %mint,
                                    "Orphaned BUY confirmed but no TokenTracker found — \
                                     cannot recover position (pool/dex unknown)"
                                );
                            }
                        }
                    } else {
                        self.remove_pending_buy_entry_by_intent(&result.intent_id);
                        warn!(
                            intent_id = %result.intent_id,
                            mint = %mint,
                            "Orphaned BUY confirmed but fill_out missing — \
                             cannot recover position"
                        );
                    }
                }
            }
            // === Orphaned sell failure recovery ===
            // If a SELL failure arrives after pending intent was cleaned up,
            // reset exit_generated to allow retry.
            else if (result.status == ExecutionStatus::Failed
                || result.status == ExecutionStatus::Timeout)
                && result.metadata.get("side").map(|s| s.as_str()) == Some("SELL")
            {
                if let Some(ref mint) = result.token_mint {
                    let mut positions = self.positions.write();
                    if let Some(pos) = positions.get_mut(mint.as_str()) {
                        pos.exit_generated = false;
                        pos.exit_generated_at = None;
                        pos.last_sell_error_code = result.error_code.clone();
                        pos.last_sell_fail_at = Some(Instant::now());
                        if result
                            .error_code
                            .as_ref()
                            .map(|c| c.contains("6002"))
                            .unwrap_or(false)
                        {
                            pos.sell_slippage_fail_count =
                                pos.sell_slippage_fail_count.saturating_add(1);
                        }
                        warn!(
                            intent_id = %result.intent_id,
                            mint = %mint,
                            error_code = ?result.error_code,
                            sell_slippage_fail_count = pos.sell_slippage_fail_count,
                            "Reset exit_generated for orphaned sell failure — will retry"
                        );
                    }
                    drop(positions);

                    // 6005 (BondingCurveComplete): mark PumpFun complete for orphaned path
                    if result
                        .error_code
                        .as_ref()
                        .map(|c| c.contains("6005"))
                        .unwrap_or(false)
                    {
                        if let Ok(mint_pk) = solana_sdk::pubkey::Pubkey::from_str(mint) {
                            if self
                                .live_pool_cache
                                .mark_pumpfun_complete_for_mint(&mint_pk)
                            {
                                warn!(mint = %mint, "6005 (orphaned): Marked PumpFun bonding curve complete");
                            }
                        }
                        let mut pools = self.mint_pools.write();
                        if let Some(pool_list) = pools.get_mut(mint.as_str()) {
                            for pool_info in pool_list.iter_mut() {
                                if pool_info.dex == "pumpfun" {
                                    pool_info.bonding_curve_complete = Some(true);
                                }
                            }
                        }
                        self.record_pumpfun_migration_complete_evidence_from_execution_observation(
                            mint, result,
                        );
                    }

                    // FIX-20: Track pool failure for orphaned sell path too.
                    // Extract pool from execution result metadata or position tracker.
                    let pool_addr = result
                        .metadata
                        .get("pool")
                        .or_else(|| result.metadata.get("pools"))
                        .cloned();
                    if let Some(ref pool) = pool_addr {
                        let mut pools = self.mint_pools.write();
                        if let Some(pool_list) = pools.get_mut(mint.as_str()) {
                            if let Some(pool_info) =
                                pool_list.iter_mut().find(|p| p.pool_address == *pool)
                            {
                                pool_info.sell_fail_count += 1;
                                pool_info.last_sell_fail_at = Some(Instant::now());
                                warn!(
                                    mint = %mint,
                                    pool = %pool,
                                    sell_fail_count = pool_info.sell_fail_count,
                                    "FIX-20: Orphaned sell failure — pool tracked for exclusion"
                                );
                            }
                        }
                    }
                }
            } else {
                debug!(intent_id = %result.intent_id, "No pending intent found for execution result");
            }
            return;
        };

        if result.source != "momentum-bot"
            && !result.source.starts_with("4filter:")
            && !result.source.starts_with("exit:")
        {
            debug!(
                intent_id = %result.intent_id,
                source = %result.source,
                "ExecutionResult source mismatch, but intent_id matches pending intent"
            );
        }

        match result.status {
            ExecutionStatus::Confirmed => {
                match pending.side {
                    TradeSide::Buy => {
                        // BUY confirmed - open/scale position.
                        // Correctness invariant: never mutate position sizing without at least
                        // knowing the output token fill (token_amount).
                        let Some(fill_out) = result.fill_out.as_ref() else {
                            warn!(
                                intent_id = %result.intent_id,
                                mint = %pending.mint,
                                entry_kind = ?pending.entry_kind,
                                signature = ?result.signature,
                                fill_status = ?result.fill_status,
                                fill_unavailable_reason = ?result.fill_unavailable_reason,
                                "BUY confirmed but fill_out missing; skipping position update"
                            );

                            // For probe entry, this means we cannot establish a position safely.
                            // For scale-in, keep the existing position but don't adjust sizing.
                            let mut trackers = self.token_trackers.write();
                            let tk = Self::tracker_storage_key(&pending.mint, &pending.pool);
                            if let Some(tr) = trackers.get_mut(&tk) {
                                match pending.entry_kind {
                                    Some(EntryKind::ScaleIn) => {
                                        tr.state = TrackerState::PositionOpenFull {
                                            filled_at: Instant::now(),
                                        };
                                    }
                                    _ => {
                                        tr.reject("buy_confirmed_missing_fill_out");
                                    }
                                }
                            }
                            self.mark_entry_eval_dirty_key(&tk);

                            self.remove_pending_buy_entry_by_intent(&result.intent_id);
                            return;
                        };

                        // Use wallet_sol_delta (total SOL impact including fees + rent) for accurate PnL.
                        // Fall back to fill_in (swap amount) or intended SOL spend.
                        let sol_invested_raw = result
                            .wallet_sol_delta_lamports
                            .map(|d| d.unsigned_abs() as u64) // BUY delta is negative → abs
                            .or_else(|| result.fill_in.as_ref().map(|a| a.raw))
                            .unwrap_or(pending.sol_amount);

                        // Prefer decimals from market-data TokenMintInfo (Geyser), fall back to fill_out decimals.
                        let token_decimals = self
                            .mint_infos
                            .read()
                            .get(&pending.mint)
                            .map(|m| m.decimals)
                            .unwrap_or(fill_out.decimals);

                        // For entry_price (used for signal PnL), use swap amounts (fill_in/fill_out)
                        // to track token price movement, not total cost with fees/rent.
                        let sol_ui_for_price = result
                            .fill_in
                            .as_ref()
                            .map(|a| a.ui_f64())
                            .unwrap_or(sol_invested_raw as f64 / 1_000_000_000.0)
                            .max(0.0);
                        let tok_ui = fill_out.ui_f64().max(0.0);
                        let entry_price = if sol_ui_for_price > 0.0 {
                            tok_ui / sol_ui_for_price
                        } else {
                            1.0
                        };

                        let sol_invested = sol_invested_raw;
                        let token_amount = fill_out.raw;

                        info!(
                            intent_id = %result.intent_id,
                            mint = %pending.mint,
                            pool = %pending.pool,
                            sol_invested = sol_invested,
                            token_amount_raw = token_amount,
                            signature = ?result.signature,
                            "✅ BUY CONFIRMED - Opening position"
                        );

                        // Mark entry stage fill.
                        {
                            let mut trackers = self.token_trackers.write();
                            let tk = Self::tracker_storage_key(&pending.mint, &pending.pool);
                            if let Some(tr) = trackers.get_mut(&tk) {
                                match pending.entry_kind {
                                    Some(EntryKind::Probe) => {
                                        tr.state = TrackerState::PositionOpenProbe {
                                            filled_at: Instant::now(),
                                        };
                                    }
                                    Some(EntryKind::ScaleIn) => {
                                        tr.state = TrackerState::PositionOpenFull {
                                            filled_at: Instant::now(),
                                        };
                                    }
                                    None => {}
                                }
                            }
                            self.mark_entry_eval_dirty_key(&tk);
                        }

                        // Resolve token_program for this mint:
                        // 1. From ExecutionResult metadata (set by execution-engine in Fix 5)
                        // 2. From mint_infos cache (Geyser TokenMintInfo)
                        // 3. None (will be resolved at SELL-time from mint_infos)
                        let resolved_token_program = result
                            .metadata
                            .get("token_program")
                            .cloned()
                            .filter(|tp| !tp.is_empty())
                            .or_else(|| {
                                self.mint_infos
                                    .read()
                                    .get(&pending.mint)
                                    .map(|m| m.token_program.clone())
                                    .filter(|tp| !tp.is_empty())
                            });

                        trace!(
                            mint = %pending.mint,
                            entry_price,
                            sol_ui_for_price,
                            tok_ui,
                            fill_in_raw = ?result.fill_in.as_ref().map(|a| a.raw),
                            fill_out_raw = ?result.fill_out.as_ref().map(|a| a.raw),
                            "BUY CONFIRMED opening position"
                        );
                        // A.2: Resolve creator for pumpfun BC-SELL (Position → TokenTracker)
                        let creator = if pending.dex == "pumpfun" {
                            let tk = Self::tracker_storage_key(&pending.mint, &pending.pool);
                            self.token_trackers
                                .read()
                                .get(&tk)
                                .and_then(|t| t.dev_wallet.clone())
                        } else {
                            None
                        };
                        // JetStream replay: same intent_id may be processed on the pending path first,
                        // then redelivered with pending already consumed — orphan path must idempotently skip.
                        self.orphaned_recovered_intent_ids
                            .write()
                            .insert(result.intent_id.clone());
                        let initial_bonding = self.clone_latest_bonding_snapshot(&pending.mint);
                        self.remove_pending_buy_entry_by_intent(&result.intent_id);
                        self.open_position(OpenPositionParams {
                            mint: &pending.mint,
                            pool: &pending.pool,
                            dex: &pending.dex,
                            entry_price,
                            token_decimals,
                            token_amount,
                            sol_invested,
                            token_program: resolved_token_program,
                            creator,
                            entry_confirmed_slot: entry_confirmed_slot_from_execution(result),
                            initial_bonding,
                        });
                        let ctx_exit = Arc::clone(self);
                        tokio::spawn(async move {
                            ctx_exit.process_exit_signals(None).await;
                        });
                    }
                    TradeSide::Sell => {
                        // FIX-20: Reset pool failure count on successful sell.
                        {
                            let mut pools = self.mint_pools.write();
                            if let Some(pool_list) = pools.get_mut(&pending.mint) {
                                if let Some(pool_info) = pool_list
                                    .iter_mut()
                                    .find(|p| p.pool_address == pending.pool)
                                {
                                    if pool_info.sell_fail_count > 0 {
                                        info!(
                                            mint = %pending.mint,
                                            pool = %pending.pool,
                                            dex = %pending.dex,
                                            old_fail_count = pool_info.sell_fail_count,
                                            "FIX-20: Sell succeeded — resetting pool failure count"
                                        );
                                        pool_info.sell_fail_count = 0;
                                        pool_info.last_sell_fail_at = None;
                                    }
                                }
                            }
                        }

                        // SELL confirmed - close or reduce position.
                        // Defense-in-depth: if only a partial amount was sold
                        // (e.g., race between exit and scale-in), reduce the
                        // position instead of closing to prevent orphaned tokens.
                        let sold_amount = result
                            .fill_in
                            .as_ref()
                            .map(|f| f.raw)
                            .unwrap_or(pending.token_amount);
                        let pos_total = self
                            .positions
                            .read()
                            .get(&pending.mint)
                            .map(|p| p.token_amount)
                            .unwrap_or(0);

                        if pos_total > 0 && sold_amount < pos_total {
                            // Partial sell — reduce position, do NOT close.
                            // Reset exit_generated so the remainder can be sold on the next tick.
                            let remaining = pos_total.saturating_sub(sold_amount);
                            warn!(
                                intent_id = %result.intent_id,
                                mint = %pending.mint,
                                sold_tokens = sold_amount,
                                position_total = pos_total,
                                remaining_tokens = remaining,
                                signature = ?result.signature,
                                "⚠️ PARTIAL SELL CONFIRMED - Reducing position (NOT closing)"
                            );
                            {
                                let mut positions = self.positions.write();
                                if let Some(pos) = positions.get_mut(&pending.mint) {
                                    pos.token_amount = remaining;
                                    pos.exit_generated = false;
                                    pos.exit_generated_at = None;
                                }
                            }
                            self.cache_wallet_balance_snapshot_raw(&pending.mint, remaining);
                            // Persist reduced position to KV
                            if let Some(pos) = self.positions.read().get(&pending.mint) {
                                let tracker = pos.clone();
                                let mint_owned = pending.mint.clone();
                                let ctx = Arc::clone(self);
                                tokio::spawn(async move {
                                    ctx.save_position_to_kv(&mint_owned, &tracker).await;
                                });
                            }
                        } else {
                            // Full sell — close position entirely.
                            // Calculate realized PnL from wallet SOL delta.
                            let sell_proceeds_lamports =
                                result.wallet_sol_delta_lamports.unwrap_or(0);
                            let cost_basis_lamports = {
                                self.positions
                                    .read()
                                    .get(&pending.mint)
                                    .map(|p| p.sol_invested as i128)
                                    .unwrap_or(0)
                            };
                            let realized_pnl_lamports =
                                sell_proceeds_lamports + (-cost_basis_lamports);
                            let realized_pnl_pct = if cost_basis_lamports > 0 {
                                (realized_pnl_lamports as f64 / cost_basis_lamports as f64) * 100.0
                            } else {
                                0.0
                            };

                            info!(
                                intent_id = %result.intent_id,
                                mint = %pending.mint,
                                token_amount = pending.token_amount,
                                signature = ?result.signature,
                                cost_basis_lamports = cost_basis_lamports,
                                sell_proceeds_lamports = sell_proceeds_lamports,
                                realized_pnl_lamports = realized_pnl_lamports,
                                realized_pnl_pct = format!("{:.2}%", realized_pnl_pct),
                                "✅ SELL CONFIRMED - Closing position"
                            );
                            self.close_position(&pending.mint);
                        }
                    }
                }
            }
            ExecutionStatus::Failed => {
                warn!(
                    intent_id = %result.intent_id,
                    mint = %pending.mint,
                    side = ?pending.side,
                    error = ?result.error_message,
                    "❌ Execution FAILED"
                );
                // Don't open position on failure. For entry intents, stop retry-spam.
                if pending.side == TradeSide::Buy {
                    self.remove_pending_buy_entry_by_intent(&result.intent_id);
                    let mut trackers = self.token_trackers.write();
                    let tk = Self::tracker_storage_key(&pending.mint, &pending.pool);
                    if let Some(tr) = trackers.get_mut(&tk) {
                        tr.reject(format!(
                            "Entry execution failed ({:?})",
                            pending.entry_kind.unwrap_or(EntryKind::Probe)
                        ));
                    }
                    self.mark_entry_eval_dirty_key(&tk);
                } else if pending.side == TradeSide::Sell {
                    // Reset exit_generated so the next strategy tick retries the sell.
                    let mut positions = self.positions.write();
                    if let Some(pos) = positions.get_mut(&pending.mint) {
                        pos.exit_generated = false;
                        pos.exit_generated_at = None;
                        pos.last_sell_error_code = result.error_code.clone();
                        pos.last_sell_fail_at = Some(Instant::now());
                        if result
                            .error_code
                            .as_ref()
                            .map(|c| c.contains("6002"))
                            .unwrap_or(false)
                        {
                            pos.sell_slippage_fail_count =
                                pos.sell_slippage_fail_count.saturating_add(1);
                        }
                        warn!(
                            mint = %pending.mint,
                            error = ?result.error_message,
                            error_code = ?result.error_code,
                            sell_slippage_fail_count = pos.sell_slippage_fail_count,
                            "Reset exit_generated after sell FAILURE — will retry on next tick"
                        );
                    }
                    drop(positions);

                    // 6005 (BondingCurveComplete): mark PumpFun complete so find_best_sell_pool uses PumpSwap AMM
                    if result
                        .error_code
                        .as_ref()
                        .map(|c| c.contains("6005"))
                        .unwrap_or(false)
                    {
                        if let Ok(mint_pk) = solana_sdk::pubkey::Pubkey::from_str(&pending.mint) {
                            if self
                                .live_pool_cache
                                .mark_pumpfun_complete_for_mint(&mint_pk)
                            {
                                warn!(mint = %pending.mint, "6005: Marked PumpFun bonding curve complete — retry will use PumpSwap AMM");
                            }
                        }
                        let mut pools = self.mint_pools.write();
                        if let Some(pool_list) = pools.get_mut(&pending.mint) {
                            for pool_info in pool_list.iter_mut() {
                                if pool_info.dex == "pumpfun" {
                                    pool_info.bonding_curve_complete = Some(true);
                                }
                            }
                        }
                        self.record_pumpfun_migration_complete_evidence_from_execution_observation(
                            &pending.mint,
                            result,
                        );
                    }

                    // FIX-20: Track pool failure so find_best_sell_pool() prefers alternatives.
                    let mut pools = self.mint_pools.write();
                    if let Some(pool_list) = pools.get_mut(&pending.mint) {
                        if let Some(pool_info) = pool_list
                            .iter_mut()
                            .find(|p| p.pool_address == pending.pool)
                        {
                            pool_info.sell_fail_count += 1;
                            pool_info.last_sell_fail_at = Some(Instant::now());
                            warn!(
                                mint = %pending.mint,
                                pool = %pending.pool,
                                dex = %pending.dex,
                                sell_fail_count = pool_info.sell_fail_count,
                                "FIX-20: Pool sell failure tracked — will prefer alternatives on retry"
                            );
                        }
                    }
                }
            }
            ExecutionStatus::Timeout => {
                warn!(
                    intent_id = %result.intent_id,
                    mint = %pending.mint,
                    side = ?pending.side,
                    "⏱️ Execution TIMEOUT"
                );
                if pending.side == TradeSide::Buy {
                    self.remove_pending_buy_entry_by_intent(&result.intent_id);
                    let mut trackers = self.token_trackers.write();
                    let tk = Self::tracker_storage_key(&pending.mint, &pending.pool);
                    if let Some(tr) = trackers.get_mut(&tk) {
                        tr.reject(format!(
                            "Entry execution timeout ({:?})",
                            pending.entry_kind.unwrap_or(EntryKind::Probe)
                        ));
                    }
                    self.mark_entry_eval_dirty_key(&tk);
                } else if pending.side == TradeSide::Sell {
                    // Reset exit_generated so the next strategy tick retries the sell.
                    let mut positions = self.positions.write();
                    if let Some(pos) = positions.get_mut(&pending.mint) {
                        pos.exit_generated = false;
                        pos.exit_generated_at = None;
                        pos.last_sell_error_code = result.error_code.clone();
                        pos.last_sell_fail_at = Some(Instant::now());
                        if result
                            .error_code
                            .as_ref()
                            .map(|c| c.contains("6002"))
                            .unwrap_or(false)
                        {
                            pos.sell_slippage_fail_count =
                                pos.sell_slippage_fail_count.saturating_add(1);
                        }
                        warn!(
                            mint = %pending.mint,
                            error_code = ?result.error_code,
                            sell_slippage_fail_count = pos.sell_slippage_fail_count,
                            "Reset exit_generated after sell TIMEOUT — will retry on next tick"
                        );
                    }
                    drop(positions);

                    // 6005 (BondingCurveComplete): mark PumpFun complete
                    if result
                        .error_code
                        .as_ref()
                        .map(|c| c.contains("6005"))
                        .unwrap_or(false)
                    {
                        if let Ok(mint_pk) = solana_sdk::pubkey::Pubkey::from_str(&pending.mint) {
                            if self
                                .live_pool_cache
                                .mark_pumpfun_complete_for_mint(&mint_pk)
                            {
                                warn!(mint = %pending.mint, "6005: Marked PumpFun bonding curve complete — retry will use PumpSwap AMM");
                            }
                        }
                        let mut pools = self.mint_pools.write();
                        if let Some(pool_list) = pools.get_mut(&pending.mint) {
                            for pool_info in pool_list.iter_mut() {
                                if pool_info.dex == "pumpfun" {
                                    pool_info.bonding_curve_complete = Some(true);
                                }
                            }
                        }
                        self.record_pumpfun_migration_complete_evidence_from_execution_observation(
                            &pending.mint,
                            result,
                        );
                    }

                    // FIX-20: Track pool failure so find_best_sell_pool() prefers alternatives.
                    let mut pools = self.mint_pools.write();
                    if let Some(pool_list) = pools.get_mut(&pending.mint) {
                        if let Some(pool_info) = pool_list
                            .iter_mut()
                            .find(|p| p.pool_address == pending.pool)
                        {
                            pool_info.sell_fail_count += 1;
                            pool_info.last_sell_fail_at = Some(Instant::now());
                            warn!(
                                mint = %pending.mint,
                                pool = %pending.pool,
                                dex = %pending.dex,
                                sell_fail_count = pool_info.sell_fail_count,
                                "FIX-20: Pool sell timeout tracked — will prefer alternatives on retry"
                            );
                        }
                    }
                }
            }
            ExecutionStatus::Sent => {
                // Still in flight, shouldn't see this as final result
                debug!(intent_id = %result.intent_id, "Execution still in flight");
            }
        }
    }

    /// Cleanup stale pending intents (older than 2 minutes)
    fn cleanup_stale_pending(&self) {
        let mut pending = self.pending_intents.write();
        let cutoff = Duration::from_secs(120);
        let now = Instant::now();
        let before = pending.len();
        let stale_buy_intent_ids: Vec<String> = pending
            .iter()
            .filter(|(_, p)| {
                now.saturating_duration_since(p.created_at) >= cutoff && p.side == TradeSide::Buy
            })
            .map(|(id, _)| id.clone())
            .collect();
        pending.retain(|_, p| now.saturating_duration_since(p.created_at) < cutoff);
        let removed = before - pending.len();
        drop(pending);
        for id in stale_buy_intent_ids {
            self.remove_pending_buy_entry_by_intent(&id);
        }
        if removed > 0 {
            debug!(removed = removed, "Cleaned up stale pending intents");
        }
    }

    /// P1: Apply config update from control-plane (Runtime Configuration via UI)
    fn apply_config_update(&self, update: &ConfigUpdate) -> ConfigUpdateResponse {
        let mut config = self.config.write();
        let mut applied = Vec::new();
        let mut rejected = Vec::new();
        let dev_sell_delay_dual_key_update = update
            .config
            .contains_key("dev_sell_revalidation_delay_secs")
            && update.config.contains_key("cto_entry_delay_secs");

        for (key, value) in &update.config {
            match key.as_str() {
                "early_min_liquidity_sol" => {
                    if let Some(v) = value.as_f64() {
                        if v >= 0.0 {
                            config.early_min_liquidity_sol = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be >= 0".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected f64".to_string()));
                    }
                }
                "established_min_liquidity_sol" => {
                    if let Some(v) = value.as_f64() {
                        if v >= 0.0 {
                            config.established_min_liquidity_sol = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be >= 0".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected f64".to_string()));
                    }
                }
                "early_slot_threshold" => {
                    if let Some(v) = value.as_u64() {
                        if v > 0 {
                            config.early_slot_threshold = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be > 0".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "early_max_slippage_bps" => {
                    if let Some(v) = value.as_u64() {
                        if v > 0 && v <= 10000 {
                            config.early_max_slippage_bps = v as u32;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be 1-10000".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "established_max_slippage_bps" => {
                    if let Some(v) = value.as_u64() {
                        if v > 0 && v <= 10000 {
                            config.established_max_slippage_bps = v as u32;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be 1-10000".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "default_position_lamports" => {
                    if let Some(v) = value.as_u64() {
                        if v > 0 {
                            config.default_position_lamports = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be > 0".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }

                // === Momentum v2 Entry ===
                "probe_buy_pct" => {
                    if let Some(v) = value.as_f64() {
                        if (0.0..=1.0).contains(&v) {
                            config.probe_buy_pct = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be in [0.0, 1.0]".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected f64".to_string()));
                    }
                }
                "scale_in_confirm_window_secs" => {
                    if let Some(v) = value.as_u64() {
                        if v > 0 {
                            config.scale_in_confirm_window_secs = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be > 0".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "scale_in_min_probe_executable_pnl_pct" => {
                    if let Some(v) = value.as_f64() {
                        if v.is_finite() {
                            config.scale_in_min_probe_executable_pnl_pct = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be finite".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected f64".to_string()));
                    }
                }

                // === Buyer Quality ===
                "top1_buyer_share_cap" => {
                    if let Some(v) = value.as_f64() {
                        if (0.0..=1.0).contains(&v) {
                            config.top1_buyer_share_cap = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be in [0.0, 1.0]".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected f64".to_string()));
                    }
                }
                "top3_buyer_share_cap" => {
                    if let Some(v) = value.as_f64() {
                        if (0.0..=1.0).contains(&v) {
                            config.top3_buyer_share_cap = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be in [0.0, 1.0]".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected f64".to_string()));
                    }
                }
                "repeat_buyer_min_ratio" => {
                    if let Some(v) = value.as_f64() {
                        if (0.0..=1.0).contains(&v) {
                            config.repeat_buyer_min_ratio = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be in [0.0, 1.0]".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected f64".to_string()));
                    }
                }

                // === Trade Size Distribution ===
                "min_trade_size_lamports" => {
                    if let Some(v) = value.as_u64() {
                        if v > 0 {
                            config.min_trade_size_lamports = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be > 0".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "small_buy_ratio_cap" => {
                    if let Some(v) = value.as_f64() {
                        if (0.0..=1.0).contains(&v) {
                            config.small_buy_ratio_cap = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be in [0.0, 1.0]".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected f64".to_string()));
                    }
                }

                // === Dump-Recovery ===
                "dump_recovery_window_secs" => {
                    if let Some(v) = value.as_u64() {
                        if v > 0 {
                            config.dump_recovery_window_secs = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be > 0".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "dump_recovery_min_buy_dominance" => {
                    if let Some(v) = value.as_f64() {
                        if (0.0..=1.0).contains(&v) {
                            config.dump_recovery_min_buy_dominance = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be in [0.0, 1.0]".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected f64".to_string()));
                    }
                }
                "dump_recovery_min_net_inflow_lamports" => {
                    if let Some(v) = value.as_u64() {
                        config.dump_recovery_min_net_inflow_lamports = v;
                        applied.push(key.clone());
                        info!(key = %key, new_value = %v, "Config updated");
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "dump_recovery_min_recovery_secs" => {
                    if let Some(v) = value.as_u64() {
                        if v > 0 {
                            config.dump_recovery_min_recovery_secs = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be > 0".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }

                // === Filter 5: Price Trend / Downtrend ===
                "price_trend_filter_enabled" => {
                    if let Some(v) = value.as_bool() {
                        config.price_trend_filter_enabled = v;
                        applied.push(key.clone());
                        info!(key = %key, new_value = %v, "Config updated");
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected bool".to_string()));
                    }
                }
                "price_trend_window_secs" => {
                    if let Some(v) = value.as_u64() {
                        config.price_trend_window_secs = v;
                        applied.push(key.clone());
                        info!(key = %key, new_value = %v, "Config updated");
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "price_trend_min_trades" => {
                    if let Some(v) = value.as_u64() {
                        config.price_trend_min_trades = v as u32;
                        applied.push(key.clone());
                        info!(key = %key, new_value = %v, "Config updated");
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "price_trend_min_tps_rise_pct" => {
                    if let Some(v) = value.as_f64() {
                        if v >= 0.0 {
                            config.price_trend_min_tps_rise_pct = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be >= 0".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected f64".to_string()));
                    }
                }
                "price_trend_max_drawdown_pct" => {
                    if let Some(v) = value.as_f64() {
                        if v >= 0.0 {
                            config.price_trend_max_drawdown_pct = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be >= 0".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected f64".to_string()));
                    }
                }
                "price_trend_recovery_min_buy_dominance" => {
                    if let Some(v) = value.as_f64() {
                        if (0.0..=1.0).contains(&v) {
                            config.price_trend_recovery_min_buy_dominance = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be in [0.0, 1.0]".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected f64".to_string()));
                    }
                }
                "price_trend_recovery_min_inflow_lamports" => {
                    if let Some(v) = value.as_u64() {
                        config.price_trend_recovery_min_inflow_lamports = v;
                        applied.push(key.clone());
                        info!(key = %key, new_value = %v, "Config updated");
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "price_trend_bucket_count" => {
                    if let Some(v) = value.as_u64() {
                        if (3..=8).contains(&v) {
                            config.price_trend_bucket_count = v as u32;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be in [3, 8]".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "price_trend_lower_highs_max_breaks" => {
                    if let Some(v) = value.as_u64() {
                        config.price_trend_lower_highs_max_breaks = v as u32;
                        applied.push(key.clone());
                        info!(key = %key, new_value = %v, "Config updated");
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "price_trend_recovery_min_positive_buckets" => {
                    if let Some(v) = value.as_u64() {
                        let max_allowed =
                            clamp_price_trend_bucket_count(config.price_trend_bucket_count)
                                .saturating_sub(1) as u64;
                        if v > 0 && v <= max_allowed {
                            config.price_trend_recovery_min_positive_buckets = v as u32;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), format!("Must be in [1, {max_allowed}]")));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "price_trend_recovery_min_secs" => {
                    if let Some(v) = value.as_u64() {
                        config.price_trend_recovery_min_secs = v;
                        applied.push(key.clone());
                        info!(key = %key, new_value = %v, "Config updated");
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "price_trend_recovery_requires_no_lower_highs" => {
                    if let Some(v) = value.as_bool() {
                        config.price_trend_recovery_requires_no_lower_highs = v;
                        applied.push(key.clone());
                        info!(key = %key, new_value = %v, "Config updated");
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected bool".to_string()));
                    }
                }

                // === Dev-Sell Re-Validation ===
                "dev_sell_revalidation_delay_secs" => {
                    if let Some(v) = value.as_u64() {
                        if v > 0 {
                            config.dev_sell_revalidation_delay_secs =
                                if dev_sell_delay_dual_key_update {
                                    config.dev_sell_revalidation_delay_secs.max(v)
                                } else {
                                    v
                                };
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be > 0".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "cto_entry_delay_secs" => {
                    if let Some(v) = value.as_u64() {
                        if v > 0 {
                            warn!(
                                legacy_key = %key,
                                dev_sell_revalidation_delay_secs = v,
                                "JetStream config: deprecated key `cto_entry_delay_secs`; applied as `dev_sell_revalidation_delay_secs`"
                            );
                            config.dev_sell_revalidation_delay_secs =
                                config.dev_sell_revalidation_delay_secs.max(v);
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated (deprecated alias)");
                        } else {
                            rejected.push((key.clone(), "Must be > 0".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "cto_enabled"
                | "cto_confirm_window_secs"
                | "cto_min_unique_buyers"
                | "cto_min_buy_dominance"
                | "cto_min_net_inflow_lamports"
                | "dev_early_sell_window_secs"
                | "dev_rebuy_positive" => {
                    warn!(
                        key = %key,
                        "JetStream config: deprecated momentum key ignored (dev-sell revalidation delay replaces CTO / early-window gates)"
                    );
                    applied.push(key.clone());
                }
                "require_mint_authority_renounced" => {
                    if let Some(v) = value.as_bool() {
                        config.require_mint_authority_renounced = v;
                        applied.push(key.clone());
                        info!(key = %key, new_value = %v, "Config updated");
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected bool".to_string()));
                    }
                }
                "require_freeze_authority_none" => {
                    if let Some(v) = value.as_bool() {
                        config.require_freeze_authority_none = v;
                        applied.push(key.clone());
                        info!(key = %key, new_value = %v, "Config updated");
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected bool".to_string()));
                    }
                }

                // === Filter 1: Liquidity Check ===
                "max_dev_supply_pct" => {
                    if let Some(v) = value.as_f64() {
                        if (0.0..=100.0).contains(&v) {
                            config.max_dev_supply_pct = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be in [0.0, 100.0]".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected f64".to_string()));
                    }
                }
                "lp_removal_window_secs" => {
                    if let Some(v) = value.as_u64() {
                        if v > 0 {
                            config.lp_removal_window_secs = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be > 0".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "min_token_age_secs" => {
                    if let Some(v) = value.as_u64() {
                        config.min_token_age_secs = v;
                        applied.push(key.clone());
                        info!(key = %key, new_value = %v, "Config updated");
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }

                // === Filter 2: Buyer Velocity ===
                "min_unique_buyers" => {
                    if let Some(v) = value.as_u64() {
                        if v > 0 {
                            config.min_unique_buyers = v as u32;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be > 0".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "buyer_window_secs" => {
                    if let Some(v) = value.as_u64() {
                        if v > 0 {
                            config.buyer_window_secs = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be > 0".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "min_trades_per_min" => {
                    if let Some(v) = value.as_f64() {
                        if v >= 0.0 {
                            config.min_trades_per_min = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be >= 0".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected f64".to_string()));
                    }
                }
                "min_trades_per_sec" => {
                    if let Some(v) = value.as_f64() {
                        if v >= 0.0 {
                            let converted = v * 60.0;
                            warn!(
                                legacy_min_trades_per_sec = v,
                                min_trades_per_min = converted,
                                "JetStream config: deprecated key `min_trades_per_sec`; applied as `min_trades_per_min` (×60)"
                            );
                            config.min_trades_per_min = converted;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %converted, "Config updated (from deprecated min_trades_per_sec)");
                        } else {
                            rejected.push((key.clone(), "Must be >= 0".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected f64".to_string()));
                    }
                }
                "min_buy_dominance" => {
                    if let Some(v) = value.as_f64() {
                        if (0.0..=1.0).contains(&v) {
                            config.min_buy_dominance = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be in [0.0, 1.0]".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected f64".to_string()));
                    }
                }

                // === Filter 3: SOL Inflow ===
                "min_sol_inflow_lamports" => {
                    if let Some(v) = value.as_u64() {
                        config.min_sol_inflow_lamports = v;
                        applied.push(key.clone());
                        info!(key = %key, new_value = %v, "Config updated");
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "inflow_window_secs" => {
                    if let Some(v) = value.as_u64() {
                        if v > 0 {
                            config.inflow_window_secs = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be > 0".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "max_single_dump_lamports" => {
                    if let Some(v) = value.as_u64() {
                        config.max_single_dump_lamports = v;
                        applied.push(key.clone());
                        info!(key = %key, new_value = %v, "Config updated");
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }

                // === Exit Strategy ===
                "hard_stop_min_hold_secs" => {
                    if let Some(v) = value.as_u64() {
                        config.hard_stop_min_hold_secs = v;
                        applied.push(key.clone());
                        info!(key = %key, new_value = %v, "Config updated");
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "catastrophic_stop_loss_pct" => {
                    if let Some(v) = value.as_f64() {
                        if (0.0..=100.0).contains(&v) {
                            config.catastrophic_stop_loss_pct = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be in [0.0, 100.0]".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected f64".to_string()));
                    }
                }
                "hard_stop_loss_pct" => {
                    if let Some(v) = value.as_f64() {
                        if (0.0..=100.0).contains(&v) {
                            config.hard_stop_loss_pct = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be in [0.0, 100.0]".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected f64".to_string()));
                    }
                }
                "trailing_stop_pct" => {
                    if let Some(v) = value.as_f64() {
                        if (0.0..=100.0).contains(&v) {
                            config.trailing_stop_pct = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be in [0.0, 100.0]".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected f64".to_string()));
                    }
                }
                "trailing_activation_pct" => {
                    if let Some(v) = value.as_f64() {
                        if v >= 0.0 {
                            config.trailing_activation_pct = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be >= 0".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected f64".to_string()));
                    }
                }
                "take_profit_pct" => {
                    if let Some(v) = value.as_f64() {
                        if v >= 0.0 {
                            config.take_profit_pct = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be >= 0".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected f64".to_string()));
                    }
                }
                "max_hold_time_secs" => {
                    if let Some(v) = value.as_u64() {
                        if v > 0 {
                            config.max_hold_time_secs = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be > 0".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "max_hold_absolute_cap_secs" => {
                    if let Some(v) = value.as_u64() {
                        config.max_hold_absolute_cap_secs = v;
                        applied.push(key.clone());
                        info!(key = %key, new_value = %v, "Config updated");
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "time_exit_requires_low_velocity" => {
                    if let Some(v) = value.as_bool() {
                        config.time_exit_requires_low_velocity = v;
                        applied.push(key.clone());
                        info!(key = %key, new_value = %v, "Config updated");
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected bool".to_string()));
                    }
                }
                "momentum_exit_min_hold_secs" => {
                    if let Some(v) = value.as_u64() {
                        config.momentum_exit_min_hold_secs = v;
                        applied.push(key.clone());
                        info!(key = %key, new_value = %v, "Config updated");
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "momentum_exit_only_when_losing" => {
                    if let Some(v) = value.as_bool() {
                        config.momentum_exit_only_when_losing = v;
                        applied.push(key.clone());
                        info!(key = %key, new_value = %v, "Config updated");
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected bool".to_string()));
                    }
                }
                "momentum_exit_max_pnl_pct" => {
                    if let Some(v) = value.as_f64() {
                        config.momentum_exit_max_pnl_pct = v;
                        applied.push(key.clone());
                        info!(key = %key, new_value = %v, "Config updated");
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected f64".to_string()));
                    }
                }
                "momentum_exit_buy_ratio" => {
                    if let Some(v) = value.as_f64() {
                        if (0.0..=1.0).contains(&v) {
                            config.momentum_exit_buy_ratio = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be in [0.0, 1.0]".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected f64".to_string()));
                    }
                }
                "momentum_exit_window_secs" => {
                    if let Some(v) = value.as_u64() {
                        if v > 0 {
                            config.momentum_exit_window_secs = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be > 0".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "momentum_exit_min_trades" => {
                    if let Some(v) = value.as_u64() {
                        if v > 0 {
                            config.momentum_exit_min_trades = v as u32;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be > 0".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "bonding_curve_exit_pct" => {
                    if let Some(v) = value.as_f64() {
                        if (0.0..=100.0).contains(&v) {
                            config.bonding_curve_exit_pct = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be 0.0-100.0".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected f64".to_string()));
                    }
                }
                "bonding_curve_exit_enabled" => {
                    if let Some(v) = value.as_bool() {
                        config.bonding_curve_exit_enabled = v;
                        applied.push(key.clone());
                        info!(key = %key, new_value = %v, "Config updated");
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected bool".to_string()));
                    }
                }
                "bonding_curve_exit_threshold_bps" => {
                    if let Some(v) = value.as_u64() {
                        if v <= 10_000 {
                            config.bonding_curve_exit_threshold_bps = v as u32;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be 0-10000".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "exit_max_slippage_bps" => {
                    if let Some(v) = value.as_u64() {
                        if v <= 10_000 {
                            config.exit_max_slippage_bps = v as u32;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be 0-10000".to_string()));
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

/// `tokens_per_sol` for one confirmed BUY execution (same fill ratio as live `open_position`).
fn buy_fill_tokens_per_sol_from_execution(exec: &ExecutionResult) -> Option<f64> {
    let fill_out = exec.fill_out.as_ref()?;
    let exec_sol = exec
        .wallet_sol_delta_lamports
        .map(|d| d.unsigned_abs() as u64)
        .or_else(|| exec.fill_in.as_ref().map(|f| f.raw))
        .unwrap_or(0);
    let sol_ui_for_price = exec
        .fill_in
        .as_ref()
        .map(|a| a.ui_f64())
        .unwrap_or(exec_sol as f64 / 1_000_000_000.0)
        .max(0.0);
    let tok_ui = fill_out.ui_f64().max(0.0);
    if sol_ui_for_price > 0.0 {
        Some(tok_ui / sol_ui_for_price)
    } else {
        None
    }
}

/// Recover open positions from execution_results JSONL after restart
///
/// P0: Critical for preventing "forgotten" tokens that never get sold.
/// Reads recent execution_results (last 7 days) and reconstructs PositionTracker
/// for any confirmed BUYs without matching SELLs.
async fn recover_positions_from_jsonl(
    log_dir: &std::path::Path,
) -> Result<HashMap<String, PositionTracker>> {
    use std::fs::File;
    use std::io::{BufRead, BufReader};

    let mut positions: HashMap<String, PositionTracker> = HashMap::new();
    let executions_dir = log_dir.parent().unwrap_or(log_dir).join("executions");
    let intents_dir = log_dir.parent().unwrap_or(log_dir).join("intents");

    if !executions_dir.exists() {
        info!("No executions directory found, skipping position recovery");
        return Ok(positions);
    }

    info!(
        dir = %executions_dir.display(),
        "Scanning execution_results for open positions..."
    );

    // Build intent_id -> TradeIntent lookup for old executions without token_mint
    let mut intent_lookup: HashMap<String, serde_json::Value> = HashMap::new();
    if intents_dir.exists() {
        let today = chrono::Utc::now().date_naive();
        for days_ago in 0..7 {
            let date = (today - chrono::Duration::days(days_ago))
                .format("%Y%m%d")
                .to_string();
            let intents_path = intents_dir.join(format!("trade_intents-{}.jsonl", date));

            if !intents_path.exists() {
                continue;
            }

            if let Ok(file) = File::open(&intents_path) {
                let reader = BufReader::new(file);
                for line in reader.lines().map_while(Result::ok) {
                    if let Ok(intent) = serde_json::from_str::<serde_json::Value>(&line) {
                        if let Some(intent_id) = intent.get("intent_id").and_then(|v| v.as_str()) {
                            intent_lookup.insert(intent_id.to_string(), intent);
                        }
                    }
                }
            }
        }
        info!(
            cached_intents = intent_lookup.len(),
            "Built intent lookup for position recovery"
        );
    }

    // Track all BUYs and SELLs by mint
    let mut buys_by_mint: HashMap<String, Vec<ExecutionResult>> = HashMap::new();
    let mut sells: HashSet<String> = HashSet::new(); // mints that have been sold

    // Scan last 7 days of execution results
    let today = chrono::Utc::now().date_naive();
    for days_ago in 0..7 {
        let date = (today - chrono::Duration::days(days_ago))
            .format("%Y%m%d")
            .to_string();
        let jsonl_path = executions_dir.join(format!("execution_results-{}.jsonl", date));

        if !jsonl_path.exists() {
            continue;
        }

        let file = File::open(&jsonl_path)?;
        let reader = BufReader::new(file);

        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }

            let exec: ExecutionResult = match serde_json::from_str(&line) {
                Ok(e) => e,
                Err(_) => continue,
            };

            // Only process confirmed executions
            if exec.status != ExecutionStatus::Confirmed {
                continue;
            }

            // Determine BUY vs SELL from fill amounts
            // Key insight: For liquidations/SELLs, fill_in (tokens sold) is MUCH larger than fill_out (SOL received)
            // For BUYs, fill_out (tokens received) is large, fill_in may be absent or small
            let fill_in_raw = exec.fill_in.as_ref().map(|f| f.raw).unwrap_or(0);
            let fill_out_raw = exec.fill_out.as_ref().map(|f| f.raw).unwrap_or(0);

            // SELL: large fill_in (tokens being sold) - this takes priority
            // The tokens sold (fill_in) should be > 100k and larger than fill_out for it to be a SELL
            let is_sell = fill_in_raw > 100_000 && fill_in_raw > fill_out_raw;
            // BUY: large fill_out (tokens received), not a SELL
            let is_buy = fill_out_raw > 0 && !is_sell;

            // For BUYs: only track momentum-bot executions
            // For SELLs: track from any source (momentum-bot OR execution-engine liquidations)
            if is_buy && exec.source != "momentum-bot" {
                continue;
            }

            // Get token_mint (from new schema or fallback to intent lookup)
            let mint = if let Some(ref m) = exec.token_mint {
                m.clone()
            } else if is_buy {
                // Old schema - try to get mint from intent (only for BUYs since we filter source)
                let mint_from_intent = if let Some(intent) = intent_lookup.get(&exec.intent_id) {
                    // BUY: output_mint is the token
                    intent
                        .get("resources")
                        .and_then(|r| r.get("output_mint"))
                        .and_then(|m| m.as_str())
                        .map(|s| s.to_string())
                } else {
                    None
                };

                match mint_from_intent {
                    Some(m) => m,
                    None => continue, // Skip if we can't determine mint
                }
            } else {
                // SELL without token_mint - skip (can't determine which position to close)
                continue;
            };

            if is_sell {
                // SELL confirmed - mark this mint as closed
                // This includes liquidations from execution-engine!
                sells.insert(mint.clone());
                // Remove from open buys
                buys_by_mint.remove(&mint);
            } else if is_buy {
                // Only track if not already sold
                if !sells.contains(&mint) {
                    buys_by_mint.entry(mint.clone()).or_default().push(exec);
                }
            }
        }
    }

    // Now reconstruct PositionTracker for each open BUY
    for (mint, execs) in buys_by_mint.iter() {
        if execs.is_empty() {
            continue;
        }

        // Sum ALL BUY fills for this mint (probe + scale-in).
        // Previously only `execs.last()` was used, which would lose the probe
        // fill when a scale-in existed, causing partial sells on recovery.
        let mut token_amount: u64 = 0;
        let mut token_decimals: u8 = 0;
        let mut sol_invested: u64 = 0;
        let mut valid_fills = 0u32;

        for exec in execs.iter() {
            if let Some(fill_out) = exec.fill_out.as_ref() {
                token_amount = token_amount.saturating_add(fill_out.raw);
                if fill_out.decimals > 0 {
                    token_decimals = fill_out.decimals;
                }
                valid_fills += 1;
            }

            // Sum SOL invested from all BUYs.
            // Prefer wallet_sol_delta (total cost including fees + ATA rent) for accurate PnL.
            // Fall back to fill_in (swap amount only) if delta unavailable.
            let exec_sol = exec
                .wallet_sol_delta_lamports
                .map(|d| d.unsigned_abs() as u64)
                .or_else(|| exec.fill_in.as_ref().map(|f| f.raw))
                .unwrap_or(0);
            sol_invested = sol_invested.saturating_add(exec_sol);
        }

        if valid_fills == 0 {
            warn!(mint = %mint, buy_count = execs.len(), "All BUY executions missing fill_out, skipping");
            continue;
        }

        // Fallback if no SOL data at all
        if sol_invested == 0 {
            sol_invested = 1_000_000_000; // 1 SOL fallback
        }

        let sol_ui = sol_invested as f64 / 1e9;
        let tok_ui = token_amount as f64 / 10f64.powi(token_decimals as i32);
        let entry_price = if sol_ui > 0.0 { tok_ui / sol_ui } else { 1.0 };

        // Use the EARLIEST BUY for entry time (position open time)
        let first_exec = match execs.first() {
            Some(e) => e,
            None => {
                warn!(mint = %mint, "buys_by_mint: execs empty despite guard (defensive skip)");
                continue;
            }
        };
        let entry_time_estimate =
            chrono::DateTime::from_timestamp_millis(first_exec.header.ts_unix_ms as i64)
                .map(|dt| {
                    let elapsed = chrono::Utc::now().signed_duration_since(dt);
                    Instant::now() - Duration::from_secs(elapsed.num_seconds().max(0) as u64)
                })
                .unwrap_or_else(Instant::now);

        // Try to get pool/dex from the FIRST intent (probe BUY)
        let (pool, dex) = if let Some(intent) = intent_lookup.get(&first_exec.intent_id) {
            let pool_from_intent = intent
                .get("resources")
                .and_then(|r| r.get("pools"))
                .and_then(|p| p.as_array())
                .and_then(|arr| arr.first())
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("unknown_pool_{}", &mint[..12]));

            let dex_from_intent = intent
                .get("metadata")
                .and_then(|m| m.get("dex"))
                .and_then(|d| d.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| "unknown".to_string());

            (pool_from_intent, dex_from_intent)
        } else {
            // Fallback: use placeholder (will work for exits but routing might fail)
            (
                format!("unknown_pool_{}", &mint[..12]),
                "unknown".to_string(),
            )
        };

        {
            let mut tracker = PositionTracker::new(
                mint,
                &pool,
                &dex,
                entry_price,
                token_decimals,
                token_amount,
                sol_invested,
            );

            let max_buy_slot = execs
                .iter()
                .filter_map(|e| e.confirmed_slot)
                .max()
                .unwrap_or(0);
            tracker.entry_confirmed_slot = max_buy_slot;
            tracker.last_price_slot = max_buy_slot;

            // Probe + scale-in: match live `add_investment` trailing baseline (last BUY fill tps).
            if valid_fills > 1 {
                if let Some(last_exec) = execs
                    .iter()
                    .max_by_key(|e| (e.confirmed_slot.unwrap_or(0), e.header.ts_unix_ms))
                {
                    if let Some(leg_tps) = buy_fill_tokens_per_sol_from_execution(last_exec) {
                        if leg_tps > 0.0 && leg_tps.is_finite() {
                            tracker.highest_price = leg_tps;
                            tracker.trade_mark_tps = leg_tps;
                            tracker.current_price = leg_tps;
                            tracker.trailing_active = false;
                        }
                    }
                }
            }

            // Override entry_time to match actual trade time
            tracker.entry_time = entry_time_estimate;
            if valid_fills <= 1 {
                tracker.current_price = entry_price; // Will update from market events
            }

            // Resolve token_program from execution metadata (best source for recovery)
            tracker.token_program = execs.iter().find_map(|exec| {
                exec.metadata
                    .get("token_program")
                    .cloned()
                    .filter(|tp| !tp.is_empty())
            });

            info!(
                mint = %mint,
                buy_fills = valid_fills,
                token_amount_raw = token_amount,
                token_amount_ui = %tok_ui,
                entry_price = %entry_price,
                sol_invested_ui = %sol_ui,
                token_program = ?tracker.token_program,
                age_secs = %(Instant::now() - entry_time_estimate).as_secs(),
                "🔄 Position recovered from JSONL (summed {} BUY fills)", valid_fills
            );

            positions.insert(mint.clone(), tracker);
        }
    }

    if positions.is_empty() {
        info!("No open positions found in execution_results");
    } else {
        info!(
            recovered_positions = positions.len(),
            "Position recovery complete"
        );
    }

    Ok(positions)
}

/// Bootstrap WalletBalanceSnapshot events from JetStream (race-free recovery)
async fn bootstrap_wallet_snapshot_from_jetstream(ctx: &Arc<MomentumContext>) -> Result<usize> {
    use async_nats::jetstream;
    use futures::StreamExt;
    use std::collections::HashSet;

    let Some(ref nats_client) = ctx.nats else {
        return Ok(0);
    };

    let jetstream = jetstream::new(nats_client.client().clone());
    let stream = match jetstream.get_stream(WALLET_SNAPSHOT_STREAM_NAME).await {
        Ok(s) => s,
        Err(e) => {
            warn!(
                error = %e,
                stream = WALLET_SNAPSHOT_STREAM_NAME,
                "Wallet snapshot stream not found (market-data may not be running)"
            );
            return Ok(0);
        }
    };

    let consumer = stream
        .create_consumer(wallet_snapshot_consumer_config())
        .await?;
    let mut recovered = 0usize;
    let mut snapshot_mints: HashSet<String> = HashSet::new();
    let batch_size = 1000;

    loop {
        let mut messages = consumer.fetch().max_messages(batch_size).messages().await?;
        let mut batch_count = 0;

        while let Some(msg) = messages.next().await {
            let msg = match msg {
                Ok(m) => m,
                Err(e) => {
                    warn!(error = %e, "Error fetching wallet snapshot from JetStream");
                    continue;
                }
            };

            batch_count += 1;

            let event: MarketEvent = match serde_json::from_slice(&msg.payload) {
                Ok(e) => e,
                Err(e) => {
                    warn!(error = %e, "Failed to deserialize WalletBalanceSnapshot from JetStream");
                    if let Err(ack_err) = msg.ack().await {
                        warn!(error = %ack_err, "Failed to ack wallet snapshot message");
                    }
                    continue;
                }
            };

            if let MarketEventKind::WalletBalanceSnapshot { mint, .. } = &event.kind {
                snapshot_mints.insert(mint.clone());
                // JetStream bootstrap/replay: do not mix historical `ts_unix_ms` into
                // `momentum_event_to_ingest_ms` (live Core NATS ingest SLO only).
                let ingest_t0 = Instant::now();
                let apply_res = process_market_event(ctx, &event, false).await;
                record_momentum_ingest_to_process_us(
                    ingest_t0.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
                );
                match apply_res {
                    Err(e) => {
                        warn!(error = %e, "Failed to apply WalletBalanceSnapshot");
                    }
                    Ok(_) => {
                        recovered += 1;
                    }
                }
            }

            if let Err(ack_err) = msg.ack().await {
                warn!(error = %ack_err, "Failed to ack wallet snapshot message");
            }
        }

        if batch_count < batch_size {
            break;
        }
    }

    if recovered > 0 {
        info!(recovered, "✅ Wallet snapshots recovered from JetStream");

        let mut positions = ctx.positions.write();
        let mut removed = 0usize;
        positions.retain(|mint, _| {
            let keep = snapshot_mints.contains(mint);
            if !keep {
                removed += 1;
            }
            keep
        });
        if removed > 0 {
            info!(
                removed,
                remaining = positions.len(),
                "🧹 Closed positions not present in wallet snapshot"
            );
        }
    }

    Ok(recovered)
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("momentum_bot=info".parse()?)
                .add_directive("ironcrab=info".parse()?)
                // async_nats logs slow-consumer INFO lines per dropped message — journald amplification.
                .add_directive("async_nats=warn".parse()?),
        )
        .init();

    let args = Args::parse();
    let run_id = Uuid::new_v4().to_string();

    info!(
        run_id = %run_id,
        config = %args.config.display(),
        test_mode = args.test_mode,
        metrics_port = args.metrics_port,
        "Starting momentum-bot service"
    );
    if let Ok(d) = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        set_momentum_bot_process_start_unix_sec(d.as_secs());
    }

    // Start metrics server
    let metrics_addr = std::net::SocketAddr::from(([0, 0, 0, 0], args.metrics_port));
    tokio::spawn(async move {
        if let Err(e) = serve_metrics(metrics_addr, MetricsComponent::MomentumBot).await {
            error!(error = %e, "Metrics server failed");
        }
    });
    info!(
        port = args.metrics_port,
        "Metrics server started at /metrics"
    );

    // === P0 Check: Ensure no wallet keys are loaded ===
    // momentum-bot is KEYLESS per architecture
    if std::env::var("IRONCRAB_KEYPAIR_JSON").is_ok()
        || std::env::var("IRONCRAB_KEYPAIR_B64").is_ok()
        || std::env::var("IRONCRAB_KEYPAIR_PATH").is_ok()
    {
        error!("ERROR: Wallet key environment variables detected!");
        error!("momentum-bot is KEYLESS per architecture. Remove key variables and restart.");
        error!("Only execution-engine should have access to wallet keys.");
        std::process::exit(1);
    }

    // Setup config - load from TOML file with fallback to defaults
    let mut momentum_config = MomentumConfig::default();

    // Try to load config from file
    if args.config.exists() {
        match std::fs::read_to_string(&args.config) {
            Ok(toml_str) => {
                // Parse just the [momentum] section
                if let Ok(parsed) = toml::from_str::<toml::Value>(&toml_str) {
                    if let Some(momentum_table) = parsed.get("momentum") {
                        let mut mt = momentum_table.clone();
                        if let Some(table) = mt.as_table_mut() {
                            ironcrab::config::migrate_momentum_trade_velocity_keys_in_table(table);
                        }
                        match mt.try_into::<MomentumCfg>() {
                            Ok(cfg) => {
                                // Apply config to MomentumConfig
                                momentum_config = MomentumConfig::from_cfg(&cfg);
                                info!(
                                    config_path = %args.config.display(),
                                    min_sol_inflow = momentum_config.min_sol_inflow_lamports / 1_000_000_000,
                                    min_unique_buyers = momentum_config.min_unique_buyers,
                                    min_trades_per_min = momentum_config.min_trades_per_min,
                                    min_buy_dominance = momentum_config.min_buy_dominance,
                                    "Loaded momentum config from TOML"
                                );
                            }
                            Err(e) => {
                                warn!(error = %e, "Failed to parse [momentum] section, using defaults");
                            }
                        }
                    } else {
                        info!("No [momentum] section in config, using defaults");
                    }
                } else {
                    warn!(config_path = %args.config.display(), "Failed to parse TOML, using defaults");
                }
            }
            Err(e) => {
                warn!(error = %e, config_path = %args.config.display(), "Failed to read config file, using defaults");
            }
        }
    } else {
        info!(config_path = %args.config.display(), "Config file not found, using defaults");
    }

    if args.test_mode {
        // In test mode, add a test mint to allowlist
        momentum_config
            .test_allowlist
            .insert("TestMint111111111111111111111111111111111111".to_string());
        info!("Test mode enabled with allowlist");
    }

    // Setup JSONL writer
    let log_dir = args
        .log_dir
        .clone()
        .unwrap_or_else(|| PathBuf::from("trade_logs/intents"));
    let jsonl_config = JsonlWriterConfig::new("trade_intents").with_log_dir(&log_dir);
    let jsonl_writer = QueuedJsonlWriter::spawn(jsonl_config, INTENT_PUBLISH_QUEUE_CAP)?;

    info!(log_dir = %log_dir.display(), "Queued JSONL writer initialized for trade_intents");

    // Setup NATS first (needed for KV recovery)
    let nats = if args.dry_run {
        info!("Dry-run mode: NATS publishing disabled");
        None
    } else {
        let config = NatsConfig::new(&args.nats_url, "momentum-bot");
        let mut client = NatsClient::new(config);
        if let Err(e) = client.connect().await {
            warn!(error = %e, "Failed to connect to NATS (continuing without)");
            None
        } else {
            info!(url = %args.nats_url, "Connected to NATS");
            set_readiness_nats_connected(true);
            // Ensure TRADE_INTENTS JetStream stream exists (avoids Core NATS startup race)
            if let Err(e) = ensure_trade_intents_stream(client.client()).await {
                warn!(error = %e, "Failed to ensure TRADE_INTENTS stream (intents may be lost)");
            }
            if let Err(e) = ensure_execution_results_stream(client.client()).await {
                warn!(error = %e, "Failed to ensure EXECUTION_RESULTS stream (results may be lost)");
            }
            Some(client)
        }
    };

    // === P0: Recover open positions ===
    // FIX: Probe + Scale must be aggregated. JSONL sums ALL BUY fills (probe+scale) from
    // execution_results. KV can be stale (saved after probe, before scale processed).
    // Merge: JSONL authoritative for token_amount when available; KV for rest.
    let recovered_positions = {
        let mut kv_positions = HashMap::new();
        let mut jsonl_positions = HashMap::new();

        // Load from JetStream KV
        if let Some(ref nats_client) = nats {
            if let Ok(store) = nats_client
                .get_or_create_kv_bucket(POSITION_KV_BUCKET)
                .await
            {
                if let Ok(persisted_positions) =
                    nats_client.kv_get_all::<PersistedPosition>(&store).await
                {
                    for (mint, persisted) in persisted_positions {
                        kv_positions.insert(mint, persisted.to_tracker());
                    }
                    if !kv_positions.is_empty() {
                        info!(
                            count = kv_positions.len(),
                            "Loaded positions from JetStream KV"
                        );
                    }
                } else {
                    warn!("Failed to load positions from KV");
                }
            }
        }

        // Always try JSONL — sums probe+scale from execution_results (authoritative)
        match recover_positions_from_jsonl(&log_dir).await {
            Ok(positions) => {
                if !positions.is_empty() {
                    info!(
                        count = positions.len(),
                        "Loaded positions from JSONL (execution records)"
                    );
                    jsonl_positions = positions;
                }
            }
            Err(e) => {
                debug!(error = %e, "No JSONL recovery (optional merge source)");
            }
        }

        // Merge: JSONL authoritative (sums probe+scale from execution_results). KV for mints not in JSONL.
        let mut positions = HashMap::new();
        let mut merged_count = 0u32;
        for (mint, jsonl_tracker) in &jsonl_positions {
            let mut tracker = jsonl_tracker.clone();
            if let Some(kv_tracker) = kv_positions.get(mint) {
                // Both sources: prefer JSONL — it sums ALL BUY fills (probe+scale).
                // KV can be stale (saved after probe, before scale was processed).
                if jsonl_tracker.token_amount > kv_tracker.token_amount {
                    merged_count += 1;
                    info!(
                        mint = %mint,
                        kv_tokens = kv_tracker.token_amount,
                        jsonl_tokens = jsonl_tracker.token_amount,
                        "Probe+Scale merge: JSONL has full amount (KV was probe-only)"
                    );
                } else if jsonl_tracker.token_amount == kv_tracker.token_amount {
                    // JSONL reproduces scale-in fill baseline only; KV may hold post-scale trade path +
                    // trailing latch from live updates — do not rewind TRAILING_STOP inputs.
                    tracker.trade_mark_tps = kv_tracker.trade_mark_tps;
                    tracker.highest_price = kv_tracker.highest_price;
                    tracker.trailing_active = kv_tracker.trailing_active;
                    tracker.last_price_slot = kv_tracker.last_price_slot;
                    tracker.current_price = kv_tracker.current_price;
                }
            }
            positions.insert(mint.clone(), tracker);
        }
        for (mint, kv_tracker) in kv_positions {
            positions.entry(mint).or_insert(kv_tracker);
        }
        if merged_count > 0 {
            info!(
                merged = merged_count,
                "Corrected probe-only KV positions using JSONL"
            );
        }

        // Persist merged state to KV for future restarts
        if !positions.is_empty() && merged_count > 0 {
            if let Some(ref nats_client) = nats {
                if let Ok(store) = nats_client
                    .get_or_create_kv_bucket(POSITION_KV_BUCKET)
                    .await
                {
                    for (mint, tracker) in &positions {
                        let persisted = PersistedPosition::from_tracker(tracker);
                        if let Err(e) = nats_client.kv_put(&store, mint, &persisted).await {
                            warn!(mint = %mint, error = %e, "Failed to update KV with merged position");
                        }
                    }
                    info!("Updated JetStream KV with merged probe+scale positions");
                }
            }
        }

        positions
    };

    // FIX-21: Create SLAVE LivePoolCache for reserve-based quoting
    let live_pool_cache = LivePoolCache::new();

    let (intent_publish_tx, intent_publish_rx) = mpsc::channel(INTENT_PUBLISH_QUEUE_CAP);
    let nats_for_intent_worker = nats.as_ref().map(NatsClient::clone_for_spawned_publish);

    let ctx = Arc::new(MomentumContext {
        run_id: run_id.clone(),
        config: parking_lot::RwLock::new(momentum_config),
        nats,
        jsonl_writer,
        intent_publish_tx: parking_lot::Mutex::new(Some(intent_publish_tx)),
        intent_counter: std::sync::atomic::AtomicU64::new(0),
        pool_first_seen: parking_lot::RwLock::new(std::collections::HashMap::new()),
        pending_dev_info: parking_lot::RwLock::new(HashMap::new()),
        pending_pool_accounts: parking_lot::RwLock::new(HashMap::new()),
        mint_infos: parking_lot::RwLock::new(HashMap::new()),
        token_trackers: parking_lot::RwLock::new(HashMap::new()),
        tracker_keys_by_mint: parking_lot::RwLock::new(HashMap::new()),
        dirty_entry_tracker_keys: parking_lot::RwLock::new(BTreeSet::new()),
        pumpfun_buy_missing_creator_suppress_until: parking_lot::RwLock::new(HashMap::new()),
        positions: parking_lot::RwLock::new(recovered_positions),
        pending_intents: parking_lot::RwLock::new(HashMap::new()),
        latest_bonding_by_mint: parking_lot::RwLock::new(HashMap::new()),
        latest_pumpfun_migration_complete_by_mint: parking_lot::RwLock::new(HashMap::new()),
        latest_pool_reserve_price_hint_by_mint_pool: parking_lot::RwLock::new(HashMap::new()),
        pending_buy_entries: parking_lot::RwLock::new(HashMap::new()),
        pending_buy_mint_index: parking_lot::RwLock::new(HashMap::new()),
        mint_pools: parking_lot::RwLock::new(HashMap::new()),
        live_pool_cache,
        open_position_pool_recovery_last_sent: parking_lot::RwLock::new(HashMap::new()),
        position_kv: tokio::sync::OnceCell::new(),
        orphaned_mints: parking_lot::RwLock::new(HashMap::new()),
        orphaned_recovered_intent_ids: parking_lot::RwLock::new(BoundedIntentIdCache::new(
            ORPHANED_RECOVERED_INTENT_IDS_CAP,
        )),
        latest_wallet_balance_raw_by_mint: parking_lot::RwLock::new(HashMap::new()),
        tokens_tracked: std::sync::atomic::AtomicU64::new(0),
        tokens_blacklisted: std::sync::atomic::AtomicU64::new(0),
        blacklisted_mints_for_metric: parking_lot::RwLock::new(HashSet::new()),
        intents_generated: std::sync::atomic::AtomicU64::new(0),
        exits_generated: std::sync::atomic::AtomicU64::new(0),
        last_event_slot: std::sync::atomic::AtomicU64::new(0),
        last_event_ts_ms: std::sync::atomic::AtomicU64::new(0),
        momentum_active_pool_publish_queue: parking_lot::Mutex::new(
            MomentumActivePoolPublishQueue::default(),
        ),
    });

    let intent_publish_worker = spawn_intent_publish_worker(
        Arc::clone(&ctx),
        intent_publish_rx,
        nats_for_intent_worker,
        None,
    );

    // === P0: Wallet snapshot recovery from JetStream ===
    if let Ok(recovered) = bootstrap_wallet_snapshot_from_jetstream(&ctx).await {
        if recovered == 0 {
            debug!("No wallet snapshots recovered from JetStream");
        }
    }

    // Open positions: request authoritative pool rows from market-data (bounded, deduped; no RPC here).
    ctx.schedule_open_position_pool_recoveries("startup").await;

    // === CRITICAL: Check for immediate exits after position recovery ===
    // Recovered positions might already violate max_hold_time or stop-loss
    // This ensures we don't wait for a MarketEvent that may never come
    {
        let exits = ctx.check_for_exits();
        for (mint, pool, dex, exit_type, reason, token_amount) in exits {
            info!(
                mint = %mint,
                pool = %pool,
                exit_type = %exit_type,
                reason = %reason,
                token_amount = token_amount,
                "🚨 IMMEDIATE EXIT SIGNAL (recovered position)"
            );

            if let Err(e) = generate_and_publish_exit_intent(
                &ctx,
                &mint,
                &pool,
                &dex,
                &exit_type,
                &reason,
                token_amount,
                None,
            )
            .await
            {
                error!(error = %e, mint = %mint, "Failed to generate/publish immediate exit intent - will retry in main loop");
            }
        }
    }

    // === Main Loop: Process MarketEvents from NATS ===
    info!("Entering main event loop");

    let mut pending_market_nats_message: Option<NatsMessage> = None;

    // Subscribe to Momentum-filtered MarketEvents (reduces Core NATS fan-in vs full stream).
    let mut subscription = if let Some(ref nats) = ctx.nats {
        match nats.subscribe(TOPIC_MOMENTUM_MARKET_EVENTS).await {
            Ok(mut sub) => {
                info!(
                    topic = TOPIC_MOMENTUM_MARKET_EVENTS,
                    wait_secs = MOMENTUM_MARKET_EVENTS_FIRST_MSG_FALLBACK.as_secs(),
                    "Subscribed to Momentum-filtered MarketEvents; waiting for first message"
                );
                match tokio::time::timeout(MOMENTUM_MARKET_EVENTS_FIRST_MSG_FALLBACK, sub.next())
                    .await
                {
                    Ok(Some(first)) => {
                        info!(
                            topic = TOPIC_MOMENTUM_MARKET_EVENTS,
                            "Momentum-filtered MarketEvents topic is active"
                        );
                        pending_market_nats_message = Some(first);
                        Some(sub)
                    }
                    Ok(None) => {
                        warn!(
                            momentum_topic = TOPIC_MOMENTUM_MARKET_EVENTS,
                            fallback_topic = TOPIC_MARKET_EVENTS,
                            "Momentum MarketEvents subscription ended before first message; falling back to core MarketEvents topic"
                        );
                        drop(sub);
                        match nats.subscribe(TOPIC_MARKET_EVENTS).await {
                            Ok(sub) => {
                                info!(
                                    topic = TOPIC_MARKET_EVENTS,
                                    "Subscribed to MarketEvents (fallback)"
                                );
                                Some(sub)
                            }
                            Err(e2) => {
                                warn!(error = %e2, "Failed to subscribe to NATS, running in offline mode");
                                None
                            }
                        }
                    }
                    Err(_elapsed) => {
                        warn!(
                            momentum_topic = TOPIC_MOMENTUM_MARKET_EVENTS,
                            fallback_topic = TOPIC_MARKET_EVENTS,
                            wait_secs = MOMENTUM_MARKET_EVENTS_FIRST_MSG_FALLBACK.as_secs(),
                            "No message on momentum MarketEvents topic within wait window (likely no publisher or version skew); falling back to core MarketEvents topic"
                        );
                        drop(sub);
                        match nats.subscribe(TOPIC_MARKET_EVENTS).await {
                            Ok(sub) => {
                                info!(
                                    topic = TOPIC_MARKET_EVENTS,
                                    "Subscribed to MarketEvents (fallback)"
                                );
                                Some(sub)
                            }
                            Err(e2) => {
                                warn!(error = %e2, "Failed to subscribe to NATS, running in offline mode");
                                None
                            }
                        }
                    }
                }
            }
            Err(e) => {
                warn!(
                    error = %e,
                    momentum_topic = TOPIC_MOMENTUM_MARKET_EVENTS,
                    fallback_topic = TOPIC_MARKET_EVENTS,
                    "Failed to subscribe to momentum MarketEvents topic; falling back to core MarketEvents topic"
                );
                match nats.subscribe(TOPIC_MARKET_EVENTS).await {
                    Ok(sub) => {
                        info!(
                            topic = TOPIC_MARKET_EVENTS,
                            "Subscribed to MarketEvents (fallback)"
                        );
                        Some(sub)
                    }
                    Err(e2) => {
                        warn!(error = %e2, "Failed to subscribe to NATS, running in offline mode");
                        None
                    }
                }
            }
        }
    } else {
        info!("NATS not connected, running in offline mode");
        None
    };

    // P1: Subscribe to Config Updates (Runtime Configuration via UI)
    // Core NATS fallback subscription (for backward compatibility)
    let mut config_subscription = if let Some(ref nats) = ctx.nats {
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

    // P1: JetStream Config Consumer (persisted, solves race condition)
    let mut config_js_consumer = if let Some(ref nats) = ctx.nats {
        use async_nats::jetstream;

        let jetstream = jetstream::new(nats.client().clone());

        match jetstream.get_stream(CONFIG_STREAM_NAME).await {
            Ok(stream) => {
                match stream
                    .create_consumer(config_consumer_config("momentum-bot"))
                    .await
                {
                    Ok(consumer) => {
                        info!(
                            stream = CONFIG_STREAM_NAME,
                            subject = %config_subject("momentum-bot"),
                            "Subscribed to JetStream Config Updates (persisted)"
                        );

                        // Bootstrap: Try to get the last config from JetStream
                        match consumer.fetch().max_messages(1).messages().await {
                            Ok(mut messages) => {
                                use futures::StreamExt;
                                if let Some(Ok(msg)) = messages.next().await {
                                    match serde_json::from_slice::<ConfigUpdate>(&msg.payload) {
                                        Ok(update) => {
                                            info!(
                                                component = %update.target_component,
                                                keys = ?update.config.keys().collect::<Vec<_>>(),
                                                "Bootstrap: Applying config from JetStream"
                                            );
                                            let response = ctx.apply_config_update(&update);
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

                        Some(consumer)
                    }
                    Err(e) => {
                        warn!(error = %e, "Failed to create JetStream config consumer");
                        None
                    }
                }
            }
            Err(e) => {
                debug!(error = %e, stream = CONFIG_STREAM_NAME, "JetStream CONFIG_UPDATES stream not found (control-plane may not be running)");
                None
            }
        }
    } else {
        None
    };

    // JetStream consumer for ExecutionResults (persistent, enables replay after restart)
    let execution_js_consumer = if let Some(ref nats) = ctx.nats {
        use async_nats::jetstream;

        let jetstream = jetstream::new(nats.client().clone());
        match jetstream.get_stream(EXECUTION_RESULTS_STREAM_NAME).await {
            Ok(stream) => match stream
                .create_consumer(execution_results_consumer_config("momentum-bot"))
                .await
            {
                Ok(consumer) => {
                    info!(
                        stream = EXECUTION_RESULTS_STREAM_NAME,
                        topic = TOPIC_EXECUTION_RESULTS,
                        "Subscribed to ExecutionResults via JetStream (persistent)"
                    );
                    Some(consumer)
                }
                Err(e) => {
                    warn!(error = %e, "Failed to create execution results consumer");
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

    // JetStream consumer for live WalletBalanceSnapshot (position reconciliation).
    // market-data publishes only to JetStream (SSOT); no longer to TOPIC_MARKET_EVENTS.
    let mut wallet_snapshot_consumer_opt = if let Some(ref nats) = ctx.nats {
        use async_nats::jetstream;

        let jetstream = jetstream::new(nats.client().clone());
        match jetstream.get_stream(WALLET_SNAPSHOT_STREAM_NAME).await {
            Ok(stream) => match stream
                .create_consumer(wallet_snapshot_live_consumer_config())
                .await
            {
                Ok(consumer) => {
                    info!(
                        stream = WALLET_SNAPSHOT_STREAM_NAME,
                        "Subscribed to JetStream WalletBalanceSnapshot (live updates)"
                    );
                    Some(consumer)
                }
                Err(e) => {
                    warn!(error = %e, "Failed to create JetStream wallet snapshot consumer");
                    None
                }
            },
            Err(e) => {
                debug!(
                    error = %e,
                    stream = WALLET_SNAPSHOT_STREAM_NAME,
                    "Wallet snapshot stream not found"
                );
                None
            }
        }
    } else {
        None
    };

    // JetStream consumer for live PoolCacheUpdate only (`DeliverPolicy::New` — no global snapshot replay).
    let mut pool_cache_consumer_opt = if let Some(ref nats) = ctx.nats {
        use async_nats::jetstream;

        let jetstream = jetstream::new(nats.client().clone());
        match jetstream.get_stream(STREAM_NAME).await {
            Ok(stream) => match stream
                .create_consumer(pool_cache_live_consumer_config())
                .await
            {
                Ok(consumer) => {
                    info!(
                        stream = STREAM_NAME,
                        deliver_policy = "New",
                        durable = "momentum-bot-pool-cache-live",
                        "Momentum PoolCache live consumer created deliver_policy=New"
                    );
                    Some(consumer)
                }
                Err(e) => {
                    warn!(
                        error = %e,
                        stream = STREAM_NAME,
                        "Failed to create JetStream POOL_CACHE live consumer"
                    );
                    None
                }
            },
            Err(e) => {
                debug!(
                    error = %e,
                    stream = STREAM_NAME,
                    "POOL_CACHE JetStream stream not found (market-data may not be running)"
                );
                None
            }
        }
    } else {
        None
    };

    // Subscribe to Control Commands (manual position cleanup, etc.)
    let mut control_subscription = if let Some(ref nats) = ctx.nats {
        match nats.subscribe(TOPIC_CONTROL_COMMANDS).await {
            Ok(sub) => {
                info!(
                    topic = TOPIC_CONTROL_COMMANDS,
                    "Subscribed to Control Commands"
                );
                Some(sub)
            }
            Err(e) => {
                warn!(error = %e, "Failed to subscribe to Control Commands");
                None
            }
        }
    } else {
        None
    };

    // Heartbeat and stats tracking
    let mut heartbeat_interval = tokio::time::interval(std::time::Duration::from_secs(60));
    // 500ms fallback when no price updates; primary path is event-driven (PoolCacheUpdate, Trade)
    let mut strategy_interval = tokio::time::interval(std::time::Duration::from_millis(500));
    strategy_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut activity_interval = tokio::time::interval(std::time::Duration::from_secs(10));
    let mut reconcile_interval = tokio::time::interval(std::time::Duration::from_secs(15));
    let mut active_pools_snapshot_interval =
        tokio::time::interval(std::time::Duration::from_secs(30));
    let mut execution_result_scheduled_interval =
        tokio::time::interval(EXECUTION_RESULT_SCHEDULED_POLL_INTERVAL);
    execution_result_scheduled_interval
        .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut events_received: u64 = 0;
    let mut last_slot: u64 = 0;

    // Graceful shutdown: `ctrl_c` listens for SIGINT. For systemd, set `KillSignal=SIGINT` in the
    // unit file so `systemctl stop` delivers SIGINT and this path runs (JSONL flush).
    let shutdown_sigint = tokio::signal::ctrl_c();
    tokio::pin!(shutdown_sigint);

    // Defer READY until after NATS setup (including momentum first-message probe). With
    // Type=notify + WatchdogSec, READY starts the watchdog; the probe can block ~30s without
    // the main loop's periodic WATCHDOG pings yet.
    ironcrab::metrics::record_activity();
    #[cfg(unix)]
    {
        // NOTIFY_SOCKET must stay set for Watchdog pings in the main loop.
        let _ = sd_notify::notify(false, &[NotifyState::Ready]);
        debug!("Sent sd_notify READY to systemd");
    }

    loop {
        tokio::select! {
            // Keep /ready fresh even if there are no events.
            _ = activity_interval.tick() => {
                ironcrab::metrics::record_activity();

                // P1 Crash Isolation: Ping systemd watchdog frequently enough.
                #[cfg(unix)]
                let _ = sd_notify::notify(false, &[NotifyState::Watchdog]);
            }

            _ = &mut shutdown_sigint => {
                info!(
                    run_id = %run_id,
                    signal = "SIGINT",
                    "momentum-bot controlled shutdown (use KillSignal=SIGINT in systemd for graceful stop)"
                );
                break;
            }

            // Process incoming MarketEvents from NATS (Core NATS — first client subscription is
            // typically MarketEvents; `async_nats` "slow consumers for subscription N" correlates
            // with this high-volume path when JetStream arms monopolize the runtime).
            msg = async {
                if let Some(pending) = pending_market_nats_message.take() {
                    Some(pending)
                } else if let Some(ref mut sub) = subscription {
                    sub.next().await
                } else {
                    // No subscription - just wait forever (heartbeat will still fire)
                    std::future::pending::<Option<NatsMessage>>().await
                }
            } => {
                let mut nats_msg_opt = msg;
                use futures::FutureExt;
                while let Some(nats_msg) = nats_msg_opt.take() {
                    ironcrab::metrics::record_activity();
                    NATS_MESSAGES_RECEIVED_TOTAL.fetch_add(1, Ordering::Relaxed);
                    events_received += 1;

                    let effective_cap = core_market_events_ingest_drain_cap_now();
                    let mut raw_chunks: Vec<Vec<u8>> = vec![nats_msg.payload];
                    while raw_chunks.len() < effective_cap {
                        let Some(ref mut sub) = subscription else {
                            break;
                        };
                        match sub.next().now_or_never() {
                            Some(Some(nm)) => {
                                NATS_MESSAGES_RECEIVED_TOTAL
                                    .fetch_add(1, Ordering::Relaxed);
                                events_received += 1;
                                raw_chunks.push(nm.payload);
                            }
                            Some(None) | None => break,
                        }
                    }

                    let drain_count = raw_chunks.len();
                    let drain_hit_cap = drain_count >= effective_cap;
                    if drain_count > 1 {
                        trace!(
                            momentum_scope_c = "core_market_events_ingest_drain",
                            drain_count,
                            effective_cap,
                            floor_cap = CORE_MARKET_EVENTS_INGEST_DRAIN_MAX,
                            ceiling_cap = CORE_MARKET_EVENTS_INGEST_DRAIN_CAP_CEILING,
                            "Drained multiple Core NATS MarketEvent payloads in one select activation"
                        );
                    }

                    let batch_prep_t0 = Instant::now();
                    let mut deserialized: Vec<MarketEvent> = Vec::new();
                    for pl in raw_chunks {
                        match serde_json::from_slice::<MarketEvent>(&pl) {
                            Ok(e) => deserialized.push(e),
                            Err(e) => {
                                warn!(error = %e, "Failed to deserialize MarketEvent");
                            }
                        }
                    }

                    let mut max_dequeued_slot_this_batch: u64 = 0;
                    for ev in &deserialized {
                        if let Some(s) = ev.slot {
                            max_dequeued_slot_this_batch = max_dequeued_slot_this_batch.max(s);
                        }
                    }

                    let events_to_run =
                        flatten_market_events_for_ingest_ordered_batch(deserialized);
                    if max_dequeued_slot_this_batch > 0 {
                        record_momentum_market_events_subscription_max_dequeued_slot(
                            max_dequeued_slot_this_batch,
                        );
                    }

                    record_momentum_nats_batch_prepare_us(
                        batch_prep_t0
                            .elapsed()
                            .as_micros()
                            .min(u128::from(u64::MAX)) as u64,
                    );

                    let mut max_ingest_wall_lag_ms: u64 = 0;
                    for (idx, event) in events_to_run.into_iter().enumerate() {
                        let kind_lbl = momentum_core_market_event_kind_core_nats_label(&event.kind);
                        record_momentum_core_market_events_received_kind(kind_lbl);
                        let event_slot = event.slot;
                        if let Some(slot) = event_slot {
                            last_slot = last_slot.max(slot);
                        }
                        let scope_c_obs_latency =
                            momentum_scope_c_price_sensitive_market_kind(&event.kind);
                        let (scope_c_latency_t0, last_ev_slot_before) =
                            if scope_c_obs_latency {
                                let t0 = Instant::now();
                                let snap = ctx.last_event_slot.load(Ordering::Relaxed);
                                (Some(t0), snap)
                            } else {
                                (None, 0u64)
                            };

                        let now_ms = wall_clock_unix_ms_now();
                        // Core NATS only: measures producer `MarketEvent.header.ts_unix_ms` vs local wall.
                        // Large values often mean **stale producer timestamps** or clock skew — not
                        // necessarily momentum backlog. JetStream pool-cache lag is tracked separately
                        // via `try_record_momentum_jetstream_poolcache_event_to_ingest_ms`.
                        // Pipeline note: this is the **market-data → momentum** segment; downstream
                        // JetStream intent latency uses `try_record_momentum_intent_header_to_publish_ms`
                        // / `try_record_momentum_publish_to_intent_ms` / `try_record_momentum_event_to_intent_publish_ms`.
                        try_record_momentum_event_to_ingest_ms(now_ms, event.header.ts_unix_ms);
                        if let Some(ms) =
                            momentum_event_ts_latency_delta_ms(now_ms, event.header.ts_unix_ms)
                        {
                            max_ingest_wall_lag_ms = max_ingest_wall_lag_ms.max(ms);
                        }
                        let ingest_t0 = Instant::now();
                        match process_market_event(&ctx, &event, true).await {
                            Ok(need_exit) => {
                                record_momentum_core_market_events_processed_kind(kind_lbl);
                                record_momentum_ingest_to_process_us(
                                    ingest_t0
                                        .elapsed()
                                        .as_micros()
                                        .min(u128::from(u64::MAX)) as u64,
                                );
                                ctx.flush_momentum_active_pool_publish_queue();
                                let is_trade = matches!(
                                    event.kind,
                                    MarketEventKind::Trade { .. }
                                );
                                if should_invoke_exit_signals_after_market_event_processing(
                                    need_exit,
                                    is_trade,
                                    ctx.position_count(),
                                ) {
                                    // `check_for_exits()` scans all positions — one MarketEvent ts is not
                                    // causal per mint/pool. Omit `momentum_event_to_intent_publish_ms` here
                                    // until per-exit causality is threaded (PR-124 follow-up).
                                    ctx.process_exit_signals(None).await;
                                }
                            }
                            Err(e) => {
                                record_momentum_ingest_to_process_us(
                                    ingest_t0
                                        .elapsed()
                                        .as_micros()
                                        .min(u128::from(u64::MAX)) as u64,
                                );
                                warn!(
                                    error = %e,
                                    event_id = %event.event_id,
                                    "Failed to process market event"
                                );
                            }
                        }

                        if let Some(t0) = scope_c_latency_t0 {
                            let duration_ms = t0.elapsed().as_millis() as u64;
                            let slot_delta_vs_head = event_slot
                                .map(|s| last_ev_slot_before.saturating_sub(s));
                            debug!(
                                momentum_scope_c = "market_event_latency",
                                market_kind =
                                    momentum_scope_c_market_kind_tag(&event.kind),
                                duration_ms,
                                event_slot = ?event_slot,
                                last_event_slot = last_ev_slot_before,
                                slot_delta_vs_head = ?slot_delta_vs_head,
                                event_id = %event.event_id,
                                "Momentum trade/bonding-related MarketEvent processed",
                            );
                        }

                        if (idx + 1).rem_euclid(CORE_MARKET_EVENTS_INGEST_COOPERATIVE_YIELD_INTERVAL)
                            == 0
                        {
                            #[cfg(unix)]
                            {
                                let _ = sd_notify::notify(false, &[NotifyState::Watchdog]);
                            }
                            tokio::task::yield_now().await;
                        }
                    }

                    set_momentum_market_events_ingest_max_wall_lag_ms_last_batch(max_ingest_wall_lag_ms);

                    record_momentum_core_market_events_ingest_drain_batch(drain_count, effective_cap);

                    momentum_interleave_jetstream_after_core_market_batch(
                        &mut pool_cache_consumer_opt,
                        execution_js_consumer.as_ref(),
                        &ctx,
                        last_slot,
                    )
                    .await;

                    // Return to the scheduler so other `select!` arms (strategy ticks, etc.) get poll slots.
                    tokio::task::yield_now().await;

                    nats_msg_opt = if drain_hit_cap {
                        if let Some(ref mut sub) = subscription {
                            match sub.next().now_or_never() {
                                Some(Some(nm)) => Some(nm),
                                Some(None) | None => None,
                            }
                        } else {
                            None
                        }
                    } else {
                        None
                    };
                }
            },

            // P1: Handle Control Commands (manual position cleanup, etc.)
            msg = async {
                if let Some(ref mut sub) = control_subscription {
                    sub.next().await
                } else {
                    std::future::pending::<Option<ironcrab::nats::NatsMessage>>().await
                }
            } => {
                if let Some(nats_msg) = msg {
                    ironcrab::metrics::record_activity();
                    NATS_MESSAGES_RECEIVED_TOTAL.fetch_add(1, Ordering::Relaxed);

                    #[derive(serde::Deserialize)]
                    struct ControlCommand {
                        action: String,
                        #[serde(default)]
                        mint: Option<String>,
                    }

                    match serde_json::from_slice::<ControlCommand>(&nats_msg.payload) {
                        Ok(cmd) => {
                            match cmd.action.as_str() {
                                "close_position" => {
                                    if let Some(mint) = cmd.mint {
                                        let mut positions = ctx.positions.write();
                                        if positions.remove(&mint).is_some() {
                                            info!(
                                                mint = %mint,
                                                "✅ Position manually closed via control command"
                                            );
                                        } else {
                                            warn!(
                                                mint = %mint,
                                                "⚠️ Position not found for manual close"
                                            );
                                        }
                                    } else {
                                        warn!("close_position command missing 'mint' field");
                                    }
                                }
                                _ => {
                                    warn!(action = %cmd.action, "Unknown control command");
                                }
                            }
                        }
                        Err(e) => {
                            warn!(error = %e, "Failed to deserialize ControlCommand");
                        }
                    }
                }
            }

            // P1: Handle Config Updates (Runtime Configuration via UI) - Core NATS fallback
            msg = async {
                if let Some(ref mut sub) = config_subscription {
                    sub.next().await
                } else {
                    std::future::pending::<Option<ironcrab::nats::NatsMessage>>().await
                }
            } => {
                if let Some(nats_msg) = msg {
                    ironcrab::metrics::record_activity();
                    NATS_MESSAGES_RECEIVED_TOTAL.fetch_add(1, Ordering::Relaxed);
                    match serde_json::from_slice::<ConfigUpdate>(&nats_msg.payload) {
                        Ok(update) => {
                            // Only process if targeted at momentum-bot
                            if update.target_component == "momentum-bot" {
                                info!(
                                    component = %update.target_component,
                                    keys = ?update.config.keys().collect::<Vec<_>>(),
                                    "Received Config Update from control-plane (Core NATS)"
                                );
                                let response = ctx.apply_config_update(&update);
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

            // P1: Handle Config Updates via JetStream (preferred, persistent)
            msg = async {
                use futures::StreamExt;
                if let Some(ref mut consumer) = config_js_consumer {
                    match consumer.fetch().max_messages(1).expires(std::time::Duration::from_millis(25)).messages().await {
                        Ok(mut messages) => {
                            if let Some(Ok(msg)) = messages.next().await {
                                Some(msg)
                            } else {
                                None
                            }
                        }
                        Err(_) => None
                    }
                } else {
                    std::future::pending::<Option<async_nats::jetstream::message::Message>>().await
                }
            } => {
                if let Some(msg) = msg {
                    ironcrab::metrics::record_activity();
                    NATS_MESSAGES_RECEIVED_TOTAL.fetch_add(1, Ordering::Relaxed);
                    match serde_json::from_slice::<ConfigUpdate>(&msg.payload) {
                        Ok(update) => {
                            // Only process if targeted at momentum-bot
                            if update.target_component == "momentum-bot" {
                                info!(
                                    component = %update.target_component,
                                    keys = ?update.config.keys().collect::<Vec<_>>(),
                                    "Received Config Update from control-plane (JetStream)"
                                );
                                let response = ctx.apply_config_update(&update);
                                info!(
                                    status = ?response.status,
                                    applied = ?response.applied_keys,
                                    rejected = ?response.rejected_keys,
                                    "Config update processed"
                                );
                            } else {
                                debug!(component = %update.target_component, "Ignoring config update for other component");
                            }
                            if let Err(e) = msg.ack().await {
                                warn!(error = %e, "Failed to ack JetStream config message");
                            }
                        }
                        Err(e) => {
                            warn!(error = %e, "Failed to deserialize ConfigUpdate");
                            let _ = msg.ack().await;
                        }
                    }
                }
            }

            // Handle ExecutionResults (position management) via JetStream — bounded drain (Scope C).
            _ = execution_result_scheduled_interval.tick(), if execution_js_consumer.is_some() => {
                if let Some(ref consumer) = execution_js_consumer {
                    let _ = drain_execution_results(
                        consumer,
                        &ctx,
                        EXECUTION_RESULT_SCHEDULED_DRAIN_MAX,
                        EXECUTION_RESULT_SCHEDULED_FETCH_EXPIRES,
                        last_slot,
                        "scheduled_select_arm",
                    )
                    .await;
                }
            }

            // Process live WalletBalanceSnapshot from JetStream (SSOT for bot state)
            _ = async {
                use futures::StreamExt;
                if let Some(ref mut consumer) = wallet_snapshot_consumer_opt {
                    #[allow(clippy::single_match)]
                    match consumer
                        .fetch()
                        .max_messages(WALLET_SNAPSHOT_FETCH_MAX)
                        .expires(std::time::Duration::from_millis(25))
                        .messages()
                        .await
                    {
                        Ok(mut messages) => {
                            while let Some(msg_result) = messages.next().await {
                                match msg_result {
                                    Ok(msg) => {
                                        ironcrab::metrics::record_activity();
                                        NATS_MESSAGES_RECEIVED_TOTAL.fetch_add(1, Ordering::Relaxed);
                                        match serde_json::from_slice::<MarketEvent>(&msg.payload) {
                                            Ok(event) => {
                                                if let MarketEventKind::WalletBalanceSnapshot { .. } =
                                                    &event.kind
                                                {
                                                    let ingest_t0 = Instant::now();
                                                    match process_market_event(&ctx, &event, false)
                                                        .await
                                                    {
                                                        Ok(need_exit) => {
                                                            record_momentum_ingest_to_process_us(
                                                                ingest_t0
                                                                    .elapsed()
                                                                    .as_micros()
                                                                    .min(u128::from(u64::MAX))
                                                                    as u64,
                                                            );
                                                            ctx.flush_momentum_active_pool_publish_queue();
                                                            if need_exit && ctx.position_count() > 0 {
                                                                ctx.process_exit_signals(None).await;
                                                            }
                                                        }
                                                        Err(e) => {
                                                            record_momentum_ingest_to_process_us(
                                                                ingest_t0
                                                                    .elapsed()
                                                                    .as_micros()
                                                                    .min(u128::from(u64::MAX))
                                                                    as u64,
                                                            );
                                                            warn!(error = %e, "Failed to process WalletBalanceSnapshot from JetStream");
                                                        }
                                                    }
                                                }
                                            }
                                            Err(e) => {
                                                warn!(error = %e, "Failed to deserialize WalletBalanceSnapshot");
                                            }
                                        }
                                        if let Err(e) = msg.ack().await {
                                            warn!(error = %e, "Failed to ack wallet snapshot message");
                                        }
                                    }
                                    Err(e) => {
                                        debug!(error = %e, "Wallet snapshot fetch error");
                                    }
                                }
                            }
                            tokio::task::yield_now().await;
                        }
                        Err(_) => {}
                    }
                } else {
                    std::future::pending::<()>().await
                }
            } => {}

            // FIX-21: Process incremental PoolCacheUpdates (live JetStream consumer, `DeliverPolicy::New`)
            _ = async {
                use futures::StreamExt;
                if let Some(ref mut consumer) = pool_cache_consumer_opt {
                    match consumer
                        .fetch()
                        .max_messages(POOL_CACHE_UPDATE_FETCH_MAX)
                        .expires(POOL_CACHE_UPDATE_FETCH_EXPIRES)
                        .messages()
                        .await
                    {
                        Ok(mut messages) => {
                            let mut batch_messages: u32 = 0;
                            let mut batch_items: Vec<(
                                async_nats::jetstream::message::Message,
                                ironcrab::ipc::PoolCacheUpdate,
                            )> = Vec::new();

                            while let Some(msg_result) = messages.next().await {
                                match msg_result {
                                    Ok(msg) => {
                                        batch_messages = batch_messages.saturating_add(1);
                                        if let Ok(update) = serde_json::from_slice::<
                                            ironcrab::ipc::PoolCacheUpdate,
                                        >(&msg.payload)
                                        {
                                            batch_items.push((msg, update));
                                        } else {
                                            let _ = msg.ack().await;
                                        }
                                    }
                                    Err(e) => {
                                        trace!(error = %e, "PoolCacheUpdate fetch error");
                                    }
                                }
                            }

                            let outcome = momentum_apply_pool_cache_jetstream_batch_items(
                                &ctx,
                                batch_items,
                                batch_messages,
                                last_slot,
                                execution_js_consumer.as_ref(),
                            )
                            .await;
                            let execution_results_drained = outcome.execution_results_drained;
                            let msg_count = outcome.msg_count;
                            let position_price_updates_applied = outcome.position_price_updates_applied;
                            let pool_cache_process_accounted =
                                outcome.pool_cache_process_accounted;
                            let unique_pools_touched = outcome.unique_pools_touched;
                            let coalesced_price_path_keys = outcome.coalesced_price_path_keys;
                            let stale_price_path_candidates = outcome.stale_price_path_candidates;
                            let max_slot_lag_vs_head = outcome.max_slot_lag_vs_head;

                            let micros_u128 = pool_cache_process_accounted.as_micros();
                            let proc_us: u64 = micros_u128.min(u128::from(u64::MAX)) as u64;
                            // Ceil(ms) from total microseconds: (µs + 999) / 1000 with truncating division.
                            let proc_ms_ceil: u64 = ((micros_u128.saturating_add(999)) / 1000)
                                .min(u128::from(u64::MAX)) as u64;
                            if batch_messages > 0 {
                                debug!(
                                    momentum_scope_c = "pool_cache_batch",
                                    batch_messages,
                                    msg_count_deserialized = msg_count,
                                    unique_pools_touched,
                                    coalesced_price_path_keys,
                                    stale_price_path_candidates,
                                    position_price_updates = position_price_updates_applied,
                                    processing_duration_us = proc_us,
                                    processing_duration_ms_ceil = proc_ms_ceil,
                                    fetch_max = POOL_CACHE_UPDATE_FETCH_MAX,
                                    last_event_slot = ctx.last_event_slot.load(Ordering::Relaxed),
                                    max_slot_lag_vs_head = ?max_slot_lag_vs_head,
                                    execution_results_drained_after_batch = execution_results_drained,
                                    decision_gate_shard_hint_permille = {
                                        let derivable_total = stale_price_path_candidates
                                            .saturating_add(coalesced_price_path_keys);
                                        stale_price_path_candidates
                                            .saturating_mul(1000)
                                            .checked_div(derivable_total)
                                            .unwrap_or(0)
                                    },
                                    "SLAVE CACHE: PoolCacheUpdate batch (processing only, excludes batch fetch wait)",
                                );
                                tokio::task::yield_now().await;
                            }
                        }
                        Err(e) => {
                            trace!(error = %e, "PoolCacheUpdate consumer fetch error");
                        }
                    }
                } else {
                    // No consumer — wait forever (other arms will fire)
                    std::future::pending::<()>().await;
                }
            } => {}

            // Strategy evaluation tick (entry + exits) - frequent enough for probe/scale windows.
            _ = strategy_interval.tick() => {
                ironcrab::metrics::record_activity();

                let need_entry_scan = !ctx.token_trackers.read().is_empty()
                    || !ctx.pending_buy_entries.read().is_empty();
                if need_entry_scan {
                    // === Check for ENTRY signals ===
                    let signals = ctx.check_for_signals_dirty_priority_tick();
                    for s in signals {
                        info!(
                            mint = %s.mint,
                            pool = %s.pool,
                            dex = %s.dex,
                            kind = ?s.kind,
                            sol_amount = s.sol_amount,
                            reason = %s.reason,
                            "🎯 ENTRY SIGNAL DETECTED"
                        );

                        if let Err(e) = generate_and_publish_buy_intent(&ctx, &s).await {
                            error!(error = %e, mint = %s.mint, "Failed to generate/publish buy intent");
                        } else {
                            ctx.intents_generated
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                }

                // === Check for EXIT signals (fallback; primary = event-driven on PoolCacheUpdate/Trade) ===
                if ctx.position_count() > 0 {
                    ctx.process_exit_signals(None).await;
                }
            }

            // Timed-exit reconciliation (retry exits that were generated but never confirmed)
            _ = reconcile_interval.tick() => {
                ironcrab::metrics::record_activity();
                ctx.reconcile_timed_exits().await;
                ctx.tick_open_position_pool_recovery_retries().await;
            }

            // PR-D: periodic full active-pool snapshot for market-data idempotency.
            _ = active_pools_snapshot_interval.tick() => {
                ironcrab::metrics::record_activity();
                ctx.reconcile_momentum_active_pools_snapshot_publish();
            }

            // Periodic heartbeat
            _ = heartbeat_interval.tick() => {
                ironcrab::metrics::record_activity();
                let (records, bytes) = ctx.jsonl_writer.stats();
                let pools = ctx.pool_first_seen.read().len();
                let tokens_tracked = ctx.tokens_tracked.load(std::sync::atomic::Ordering::Relaxed);
                let tokens_blacklisted = ctx.tokens_blacklisted.load(std::sync::atomic::Ordering::Relaxed);
                let intents_generated = ctx.intents_generated.load(std::sync::atomic::Ordering::Relaxed);
                let exits_generated = ctx.exits_generated.load(std::sync::atomic::Ordering::Relaxed);
                let open_positions = ctx.position_count();
                let pending_intents = ctx.pending_count();

                // Update Prometheus metrics
                MARKET_EVENTS_CONSUMED_TOTAL.store(events_received, Ordering::Relaxed);
                POOLS_TRACKED_GAUGE.store(pools as u64, Ordering::Relaxed);
                TOKENS_TRACKED_GAUGE.store(tokens_tracked, Ordering::Relaxed);
                INTENTS_GENERATED_TOTAL.store(intents_generated, Ordering::Relaxed);
                EXITS_GENERATED_TOTAL.store(exits_generated, Ordering::Relaxed);

                info!(
                    events_received = events_received,
                    last_slot = last_slot,
                    intents_written = records,
                    bytes_written = bytes,
                    pools_tracked = pools,
                    tokens_tracked = tokens_tracked,
                    tokens_blacklisted = tokens_blacklisted,
                    intents_generated = intents_generated,
                    exits_generated = exits_generated,
                    open_positions = open_positions,
                    pending_intents = pending_intents,
                    "Momentum-bot heartbeat"
                );

                // Cleanup old trackers and stale pending intents
                ctx.cleanup_old_trackers();
                ctx.cleanup_stale_pending();
                let stale_removed = ctx.prune_stale_discovery_trackers();
                if !stale_removed.is_empty() {
                    if ctx.nats.is_some() {
                        ctx.spawn_publish_momentum_active_pools(Vec::new(), stale_removed, false);
                    } else {
                        let mut g = ctx.momentum_active_pool_publish_queue.lock();
                        g.removed.extend(stale_removed);
                    }
                }
                ctx.flush_momentum_active_pool_publish_queue();
            }
        }
    }

    info!(run_id = %run_id, "momentum-bot main loop ended; draining intent publish worker");
    ctx.shutdown_intent_publish_sender();
    if let Err(e) = intent_publish_worker.await {
        warn!(error = %e, "intent publish worker task join error");
    }
    ctx.jsonl_writer.flush()?;
    info!(run_id = %run_id, "momentum-bot shutdown complete");

    Ok(())
}

async fn intent_publish_jsonl_write(writer: &QueuedJsonlWriter, intent: &TradeIntent) {
    let intent = intent.clone();
    loop {
        if writer.try_write(intent.clone()) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

async fn intent_publish_worker_loop(
    ctx: Arc<MomentumContext>,
    mut rx: mpsc::Receiver<IntentPublishJob>,
    nats: Option<NatsClient>,
    order_recorder: Option<IntentPublishOrderRecorder>,
) {
    while let Some(job) = rx.recv().await {
        let intent = job.intent;
        intent_publish_jsonl_write(&ctx.jsonl_writer, &intent).await;

        let mut publish_ok = nats.is_none();
        if let Some(ref nats) = nats {
            match nats.jetstream_publish(TOPIC_TRADE_INTENTS, &intent).await {
                Ok(true) => {
                    NATS_MESSAGES_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                    let now_ms = wall_clock_unix_ms_now();
                    try_record_momentum_intent_header_to_publish_ms(
                        now_ms,
                        intent.header.ts_unix_ms,
                    );
                    publish_ok = true;
                }
                Ok(false) => {
                    NATS_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                    warn!(
                        intent_id = %intent.intent_id,
                        topic = TOPIC_TRADE_INTENTS,
                        "Intent JetStream publish dropped/failed in worker"
                    );
                }
                Err(e) => {
                    NATS_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                    warn!(
                        intent_id = %intent.intent_id,
                        error = %e,
                        "Intent JetStream publish error in worker"
                    );
                }
            }
        }

        match job.post_action {
            IntentPublishPostAction::Exit {
                mint,
                source_event_ts_unix_ms,
                slot_seen_at_ms,
            } => {
                if publish_ok {
                    if slot_seen_at_ms > 0 && slot_seen_at_ms <= intent.header.ts_unix_ms {
                        try_record_momentum_publish_to_intent_ms(
                            intent.header.ts_unix_ms,
                            slot_seen_at_ms,
                        );
                    }
                    if let Some(ts) = source_event_ts_unix_ms {
                        try_record_momentum_event_to_intent_publish_ms(
                            wall_clock_unix_ms_now(),
                            ts,
                        );
                    }
                    ctx.record_exit_intent_published(&mint);
                } else {
                    ctx.rollback_exit_intent_after_publish_failure(&intent.intent_id, &mint);
                }
            }
            IntentPublishPostAction::Buy {
                entry_signal,
                effective_pool,
                effective_dex,
                token_program,
                signal_slot,
                slot_seen_at_ms,
            } => {
                if publish_ok {
                    if slot_seen_at_ms > 0 && slot_seen_at_ms <= intent.header.ts_unix_ms {
                        try_record_momentum_publish_to_intent_ms(
                            intent.header.ts_unix_ms,
                            slot_seen_at_ms,
                        );
                    }
                    let meta_creator = intent.metadata.get("creator").cloned();
                    ctx.register_pending_buy_entry_after_publish(PendingBuyPublishMeta {
                        intent_id: &intent.intent_id,
                        mint: &entry_signal.mint,
                        pool: &effective_pool,
                        dex: &effective_dex,
                        intended_sol: entry_signal.sol_amount,
                        entry_kind: Some(entry_signal.kind),
                        signal_slot,
                        slot_seen_at_ms,
                        creator: meta_creator,
                        token_program,
                    });
                } else {
                    let intent_id = intent.intent_id.clone();
                    ctx.pending_intents.write().remove(&intent_id);
                    ctx.unwind_stale_entry_pending_after_publish_failure(&entry_signal, true);
                }
            }
        }

        if let Some(recorder) = order_recorder.as_ref() {
            recorder.lock().push(intent.intent_id.clone());
        }
    }
    info!("intent publish worker drained and exiting");
}

fn spawn_intent_publish_worker(
    ctx: Arc<MomentumContext>,
    rx: mpsc::Receiver<IntentPublishJob>,
    nats: Option<NatsClient>,
    order_recorder: Option<IntentPublishOrderRecorder>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(intent_publish_worker_loop(ctx, rx, nats, order_recorder))
}

/// Generate and publish a BUY TradeIntent based on an entry signal.
async fn generate_and_publish_buy_intent(
    ctx: &MomentumContext,
    signal: &EntrySignal,
) -> Result<()> {
    let intent_id = ctx.next_intent_id();
    let result = generate_and_publish_buy_intent_inner(ctx, signal, &intent_id).await;
    if let Err(ref e) = result {
        ctx.pending_intents.write().remove(&intent_id);
        let requeue_dirty = !err_chain_contains_pumpfun_missing_creator_buy(e);
        ctx.unwind_stale_entry_pending_after_publish_failure(signal, requeue_dirty);
        if !requeue_dirty {
            ctx.pumpfun_buy_missing_creator_suppress_arm_key(&signal.mint, &signal.pool);
            ironcrab::metrics::record_momentum_entry_buy_suppressed_missing_creator();
            warn!(
                mint = %signal.mint,
                pool = %signal.pool,
                reason = "ENTRY_BUY_DEFERRED_MISSING_CREATOR",
                cooldown_secs = 45_u64,
                "PumpFun BUY deferred: missing dev_wallet/creator — bounded suppress (no immediate dirty re-queue)"
            );
        }
    }
    result
}

async fn generate_and_publish_buy_intent_inner(
    ctx: &MomentumContext,
    signal: &EntrySignal,
    intent_id: &str,
) -> Result<()> {
    let max_slippage = { ctx.config.read().early_max_slippage_bps };

    // Assume SOL (So11111...) as quote mint for PumpFun/meme tokens
    let sol_mint = "So11111111111111111111111111111111111111112";

    // Multi-Pool Routing: For ScaleIn entries, find the best pool (price > speed)
    // For Probe entries, use original pool (speed is critical)
    let (effective_pool, effective_dex, routed_accounts, alternatives_checked) = match signal.kind {
        EntryKind::ScaleIn => {
            // Try to find better pool for scale-in
            match ctx.find_best_buy_pool(&signal.mint, signal.sol_amount, &signal.pool) {
                Ok((pool, dex, accounts, _expected_tokens, alts)) => {
                    (pool, dex, Some(accounts), alts)
                }
                Err(e) => {
                    // Fallback to original pool if routing fails
                    debug!(
                        mint = %signal.mint,
                        error = %e,
                        "Multi-pool routing failed for scale-in, using original pool"
                    );
                    (signal.pool.clone(), signal.dex.clone(), None, 1)
                }
            }
        }
        EntryKind::Probe => {
            // Probe: Speed is critical, skip multi-pool lookup
            (signal.pool.clone(), signal.dex.clone(), None, 1)
        }
    };

    // FIX-22: Cross-check creator with LivePoolCache (authoritative)
    let (creator_opt, last_trade_ratio_opt) = {
        let trackers = ctx.token_trackers.read();
        let tk = MomentumContext::tracker_storage_key(&signal.mint, &signal.pool);
        let tracker = trackers.get(&tk);
        let tracker_creator = tracker.and_then(|t| t.dev_wallet.clone());
        let ratio = tracker.and_then(|t| t.last_trade_ratio());
        drop(trackers);
        let resolved_creator = ctx.resolve_authoritative_creator(&signal.mint, tracker_creator);
        (resolved_creator, ratio)
    };

    let (token_decimals_opt, token_program_opt) = {
        let mint_infos = ctx.mint_infos.read();
        match mint_infos.get(&signal.mint) {
            Some(m) => (Some(m.decimals), Some(m.token_program.clone())),
            None => (None, None),
        }
    };

    // Fallback: Most memecoin DEXes use 6 decimals (pump.fun, raydium, orca, meteora)
    // This is safe because:
    // 1. All new memecoins launched via these DEXes use 6 decimals
    // 2. Established tokens (USDC=6, SOL=9) would already be in mint_infos cache
    // 3. Worst case: wrong decimals = intent rejected at execution (no financial loss)
    let token_decimals = match token_decimals_opt {
        Some(d) => d,
        None => {
            // Use 6 decimals as fallback - this covers 99%+ of memecoin launches
            warn!(
                mint = %signal.mint,
                dex = %signal.dex,
                "Using fallback decimals=6 (TokenMintInfo not yet received via Geyser)"
            );
            6
        }
    };

    let token_program_override = {
        let token_program_opt = token_program_opt
            .as_deref()
            .map(|tp| tp.trim())
            .filter(|tp| !tp.is_empty())
            .map(|tp| tp.to_string());

        let spl = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
        let spl22 = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";

        // Trust Geyser-sourced token_program for all known programs.
        // Previous guard dropped Token-2022 for pump tokens when MintInfo wasn't "final"
        // (supply==0, no authorities), causing IncorrectProgramId on BUY.
        // Since this data comes from Geyser (authoritative on-chain state), we trust it.
        match token_program_opt.as_deref() {
            Some(tp) if tp == spl || tp == spl22 => Some(tp.to_string()),
            Some(tp) => {
                warn!(
                    mint = %signal.mint,
                    dex = %effective_dex,
                    token_program = %tp,
                    "Ignoring unknown token_program override"
                );
                None
            }
            None => None,
        }
    };

    // Use routed_accounts from multi-pool routing if available, otherwise fetch
    let dex_requires_accounts = MomentumContext::dex_requires_pool_accounts(&effective_dex);
    let dex_accounts: Vec<String> = if let Some(accounts) = routed_accounts {
        // Multi-pool routing already provided validated accounts
        accounts
    } else if dex_requires_accounts {
        let dex_pool_accounts_opt =
            ctx.try_get_dex_pool_accounts_for_mint_pool(&signal.mint, &effective_pool);
        let Some(accounts) = dex_pool_accounts_opt else {
            warn!(
                mint = %signal.mint,
                pool = %effective_pool,
                dex = %effective_dex,
                "Skipping BUY intent: missing DexPoolAccounts for deterministic build"
            );
            anyhow::bail!("cannot generate intent: missing DexPoolAccounts")
        };

        // Validate accounts[0] matches the pool address
        if accounts.first().map(|s| s.as_str()) != Some(effective_pool.as_str()) {
            warn!(
                mint = %signal.mint,
                pool = %effective_pool,
                dex = %effective_dex,
                first = ?accounts.first(),
                "Skipping BUY intent: invalid DexPoolAccounts (accounts[0] != pool)"
            );
            anyhow::bail!("cannot generate intent: invalid DexPoolAccounts")
        }

        let is_pump_amm = effective_dex == "pump_amm";

        if is_pump_amm && accounts.len() != 14 {
            warn!(
                mint = %signal.mint,
                pool = %effective_pool,
                dex = %effective_dex,
                accounts_len = accounts.len(),
                "Skipping BUY intent: pump_amm requires exactly 14 accounts"
            );
            anyhow::bail!("cannot generate intent: invalid DexPoolAccounts")
        }

        if accounts.len() < 3 {
            warn!(
                mint = %signal.mint,
                pool = %effective_pool,
                dex = %effective_dex,
                accounts_len = accounts.len(),
                "Skipping BUY intent: DexPoolAccounts needs at least 3 entries"
            );
            anyhow::bail!("cannot generate intent: invalid DexPoolAccounts")
        }

        accounts
    } else {
        Vec::new()
    };

    let (last_sol_lamports, last_token_amount) = match last_trade_ratio_opt {
        Some(v) => v,
        None => {
            warn!(
                mint = %signal.mint,
                pool = %signal.pool,
                dex = %signal.dex,
                reason = %signal.reason,
                "Skipping BUY intent: no usable trade ratio yet (need sol_amount+token_amount)"
            );
            anyhow::bail!(
                "cannot generate intent: no usable trade ratio yet (need sol_amount+token_amount)"
            )
        }
    };

    // Estimate expected token output using last observed ratio.
    // expected_out_raw = sol_amount * last_token_amount / last_sol_lamports
    let expected_out_raw: u64 = ((signal.sol_amount as u128)
        .saturating_mul(last_token_amount as u128)
        / (last_sol_lamports as u128))
        .min(u64::MAX as u128) as u64;

    let min_out_raw: u64 = ((expected_out_raw as u128)
        .saturating_mul((10_000u32.saturating_sub(max_slippage)) as u128)
        / 10_000u128)
        .min(u64::MAX as u128) as u64;

    let reason_code = match signal.kind {
        EntryKind::Probe => "ENTER_PROBE_BUY",
        EntryKind::ScaleIn => "ENTER_SCALE_IN",
    };

    let mut intent = TradeIntent::new(
        "momentum-bot",
        BUILD_VERSION,
        &ctx.run_id,
        intent_id.to_string(),
        "momentum-bot",
        IntentTier::Tier1,
        IntentOrigin::StrategyA,
        ExplicitAmount::new(signal.sol_amount, 9),
        TradeResources {
            input_mint: sol_mint.to_string(),
            output_mint: signal.mint.to_string(),
            pools: vec![effective_pool.clone()],
            accounts: dex_accounts,
            token_program: token_program_override.clone(),
        },
        50, // Expected ROI: 0.5%
        max_slippage,
        TradeSide::Buy,
        TradingRegime::Early,
    )
    .with_ttl_ms(5000);

    // Provide deterministic execution constraints required by the execution-engine.
    intent.execution = Some(TradeExecutionConstraints {
        min_out: Some(ExplicitAmount::new(min_out_raw, token_decimals)),
    });

    // Keep legacy metadata for backward compatibility, and provide routing hints.
    intent
        .metadata
        .insert("min_out_raw".to_string(), min_out_raw.to_string());
    intent
        .metadata
        .insert("dex".to_string(), effective_dex.clone());
    intent
        .metadata
        .insert("market_order".to_string(), "true".to_string());
    intent
        .metadata
        .insert("reason_code".to_string(), reason_code.to_string());
    intent
        .metadata
        .insert("reason_detail".to_string(), signal.reason.clone());
    intent.metadata.insert(
        "entry_kind".to_string(),
        match signal.kind {
            EntryKind::Probe => "probe".to_string(),
            EntryKind::ScaleIn => "scale_in".to_string(),
        },
    );
    // Track multi-pool routing info
    if alternatives_checked > 1 {
        intent.metadata.insert(
            "alternatives_checked".to_string(),
            alternatives_checked.to_string(),
        );
        if effective_pool != signal.pool {
            intent
                .metadata
                .insert("original_pool".to_string(), signal.pool.clone());
        }
    }

    if effective_dex == "pumpfun" {
        let creator =
            creator_opt.ok_or_else(|| anyhow::anyhow!(ERR_PUMPFUN_MISSING_CREATOR_BUY))?;
        intent.metadata.insert("creator".to_string(), creator);
    } else if let Some(creator) = creator_opt {
        intent.metadata.insert("creator".to_string(), creator);
    }

    // Include current open positions count for execution-engine risk check.
    // execution-engine uses this instead of tracking positions itself (Single Source of Truth).
    let current_open_positions = ctx.positions.read().len();
    intent.metadata.insert(
        "current_open_positions".to_string(),
        current_open_positions.to_string(),
    );

    // K Phase 1: Slot-to-Send Latency - propagate slot from last event
    let slot = ctx
        .last_event_slot
        .load(std::sync::atomic::Ordering::Relaxed);
    let ts_ms = ctx
        .last_event_ts_ms
        .load(std::sync::atomic::Ordering::Relaxed);
    if slot > 0 {
        intent.metadata.insert("slot".to_string(), slot.to_string());
    }
    if ts_ms > 0 {
        intent
            .metadata
            .insert("slot_seen_at_ms".to_string(), ts_ms.to_string());
    }

    info!(
        intent_id = %intent.intent_id,
        pool = %effective_pool,
        mint = %signal.mint,
        dex = %effective_dex,
        kind = ?signal.kind,
        sol_amount = signal.sol_amount,
        reason = %signal.reason,
        alternatives_checked = alternatives_checked,
        "🚀 Generated BUY TradeIntent"
    );

    // Register pending intent for execution-result correlation before worker publish.
    ctx.register_buy_intent(
        intent_id,
        &signal.mint,
        &effective_pool,
        &effective_dex,
        signal.sol_amount,
        Some(signal.kind),
    );

    ctx.enqueue_intent_publish(IntentPublishJob {
        intent,
        post_action: IntentPublishPostAction::Buy {
            entry_signal: signal.clone(),
            effective_pool,
            effective_dex,
            token_program: token_program_override.clone(),
            signal_slot: slot,
            slot_seen_at_ms: ts_ms,
        },
    })
    .await?;

    Ok(())
}

#[inline]
fn momentum_scope_c_price_sensitive_market_kind(kind: &MarketEventKind) -> bool {
    matches!(
        kind,
        MarketEventKind::Trade { .. }
            | MarketEventKind::PoolCreated { .. }
            | MarketEventKind::BondingCurveProgress { .. }
    )
}

/// Predicate kept for unit tests (historical interleave heuristic; production uses
/// [`momentum_interleave_jetstream_after_core_market_batch`]).
#[cfg(test)]
#[inline]
fn market_events_batch_wants_interleaved_execution_result_drain(
    batch_had_price_sensitive_event: bool,
    position_count: usize,
    pending_execution_intents: usize,
) -> bool {
    batch_had_price_sensitive_event && (position_count > 0 || pending_execution_intents > 0)
}

/// `process_exit_signals` only applies to open positions; skip the scan when empty (hot path).
#[inline]
fn should_invoke_exit_signals_after_market_event_processing(
    need_exit: bool,
    is_trade: bool,
    position_count: usize,
) -> bool {
    position_count > 0 && (need_exit || is_trade)
}

#[inline]
fn momentum_scope_c_market_kind_tag(kind: &MarketEventKind) -> &'static str {
    match kind {
        MarketEventKind::PoolCreated { .. } => "PoolCreated",
        MarketEventKind::Trade { .. } => "Trade",
        MarketEventKind::BondingCurveProgress { .. } => "BondingCurveProgress",
        _ => "Other",
    }
}

/// Stats for Scope C bonding burst coalescing on Core NATS (per-mint latest Geyser slot/ts wins).
#[derive(Debug, Clone, Copy, Default)]
struct BondingStreakCoalesceStats {
    raw_messages: u32,
    emitted_events: u32,
    stale_dropped: u32,
}

/// Collapse a bounded streak of `BondingCurveProgress` events to one event per mint (deterministic mint order).
fn coalesce_bonding_curve_progress_streak(
    streak: Vec<MarketEvent>,
) -> (Vec<MarketEvent>, BondingStreakCoalesceStats) {
    let raw_messages = streak.len() as u32;
    if streak.is_empty() {
        return (Vec::new(), BondingStreakCoalesceStats::default());
    }
    let mut best_by_mint: HashMap<String, usize> = HashMap::new();
    for (i, e) in streak.iter().enumerate() {
        let MarketEventKind::BondingCurveProgress { mint, .. } = &e.kind else {
            continue;
        };
        match best_by_mint.get(mint.as_str()) {
            None => {
                best_by_mint.insert(mint.clone(), i);
            }
            Some(&wi) => {
                let cand = &streak[i];
                let prev = &streak[wi];
                let s_c = cand.slot.unwrap_or(0);
                let t_c = cand.header.ts_unix_ms;
                let s_p = prev.slot.unwrap_or(0);
                let t_p = prev.header.ts_unix_ms;
                if bonding_geyser_observation_is_newer(s_c, t_c, s_p, t_p)
                    || (s_c == s_p && t_c == t_p && i > wi)
                {
                    best_by_mint.insert(mint.clone(), i);
                }
            }
        }
    }
    let mut key_index: Vec<(String, usize)> = best_by_mint.into_iter().collect();
    key_index.sort_by(|a, b| a.0.cmp(&b.0));
    let emitted: Vec<MarketEvent> = key_index
        .into_iter()
        .map(|(_, idx)| streak[idx].clone())
        .collect();
    let emitted_events = emitted.len() as u32;
    let stale_dropped = raw_messages.saturating_sub(emitted_events);
    (
        emitted,
        BondingStreakCoalesceStats {
            raw_messages,
            emitted_events,
            stale_dropped,
        },
    )
}

/// Expand an ordered ingest batch: preserves non-BCP event order; consecutive
/// `BondingCurveProgress` runs are coalesced in chunks of at most
/// [`BONDING_CURVE_PROGRESS_STREAK_MAX`] (same semantics as the legacy
/// subscription streak reader).
fn flatten_market_events_for_ingest_ordered_batch(input: Vec<MarketEvent>) -> Vec<MarketEvent> {
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < input.len() {
        if matches!(input[i].kind, MarketEventKind::BondingCurveProgress { .. }) {
            let start = i;
            while i < input.len()
                && matches!(input[i].kind, MarketEventKind::BondingCurveProgress { .. })
            {
                i += 1;
            }
            let mut slice = &input[start..i];
            while !slice.is_empty() {
                let take = slice.len().min(BONDING_CURVE_PROGRESS_STREAK_MAX);
                let chunk: Vec<MarketEvent> = slice[..take].to_vec();
                slice = &slice[take..];
                let (coalesced, streak_stats) = coalesce_bonding_curve_progress_streak(chunk);
                if streak_stats.raw_messages > streak_stats.emitted_events {
                    let ratio_permille = (streak_stats.stale_dropped as u64)
                        .saturating_mul(1000)
                        .checked_div(streak_stats.raw_messages as u64)
                        .unwrap_or(0);
                    debug!(
                        momentum_scope_c = "bonding_curve_streak_coalesce",
                        raw_streak_messages = streak_stats.raw_messages,
                        emitted_after_coalesce = streak_stats.emitted_events,
                        stale_dropped = streak_stats.stale_dropped,
                        max_streak_cap = BONDING_CURVE_PROGRESS_STREAK_MAX,
                        decision_gate_stale_ratio_permille = ratio_permille,
                        "Momentum Scope C: coalesced BondingCurveProgress burst in ingest batch (per-mint latest-wins)",
                    );
                }
                out.extend(coalesced);
            }
        } else {
            out.push(input[i].clone());
            i += 1;
        }
    }
    out
}

/// Phase-2 winner per `pool_address` for reserve-derived marks / `update_position_price` in one JetStream batch.
/// Caller must apply every `PoolCacheUpdate` to `LivePoolCache` in arrival order before using winners here.
///
/// `derived[i]` must be `Some` iff `ctx.derive_tokens_per_sol_from_pool_cache_update(&updates[i]).is_some()`.
fn select_pool_cache_batch_price_path_winners(
    updates: &[ironcrab::ipc::PoolCacheUpdate],
    derived: &[PoolCacheDerivedTps],
) -> (HashMap<String, usize>, u32) {
    debug_assert_eq!(updates.len(), derived.len());
    let mut derive_ok_per_pool: HashMap<String, usize> = HashMap::new();
    let mut derive_ok_total: u32 = 0;
    for (idx, update) in updates.iter().enumerate() {
        if derived.get(idx).and_then(|d| d.as_ref()).is_none() {
            continue;
        }
        derive_ok_total = derive_ok_total.saturating_add(1);
        let pool = update.pool_address.clone();
        match derive_ok_per_pool.get(&pool) {
            None => {
                derive_ok_per_pool.insert(pool, idx);
            }
            Some(&best_idx) => {
                let best_u = &updates[best_idx];
                let s_c = update.geyser_slot;
                let t_c = update.header.ts_unix_ms;
                let s_p = best_u.geyser_slot;
                let t_p = best_u.header.ts_unix_ms;
                if bonding_geyser_observation_is_newer(s_c, t_c, s_p, t_p)
                    || (s_c == s_p && t_c == t_p && idx > best_idx)
                {
                    derive_ok_per_pool.insert(pool, idx);
                }
            }
        }
    }
    let winner_count = derive_ok_per_pool.len() as u32;
    let stale = derive_ok_total.saturating_sub(winner_count);
    (derive_ok_per_pool, stale)
}

/// Wall-clock lag from producer `RecordHeader.ts_unix_ms` to momentum-bot ingest (ms).
#[inline]
fn execution_result_ingest_lag_ms(result: &ExecutionResult, now_ms: u64) -> u64 {
    now_ms.saturating_sub(result.header.ts_unix_ms)
}

/// Outcome of applying a bounded JetStream `PoolCacheUpdate` batch (shared by `select!` arm and
/// post–Core-NATS interleave).
#[derive(Debug, Clone, Copy)]
struct PoolCacheJetstreamBatchOutcome {
    msg_count: u32,
    execution_results_drained: u32,
    position_price_updates_applied: u32,
    pool_cache_process_accounted: Duration,
    unique_pools_touched: u32,
    coalesced_price_path_keys: u32,
    stale_price_path_candidates: u32,
    max_slot_lag_vs_head: Option<u64>,
}

/// Apply `PoolCacheUpdate` messages already pulled from JetStream (apply + price-path + acks +
/// optional exit scan + bounded `ExecutionResult` drain).
async fn momentum_apply_pool_cache_jetstream_batch_items(
    ctx: &Arc<MomentumContext>,
    batch_items: Vec<(
        async_nats::jetstream::message::Message,
        ironcrab::ipc::PoolCacheUpdate,
    )>,
    _batch_messages: u32,
    last_slot: u64,
    execution_js_consumer: Option<
        &async_nats::jetstream::consumer::Consumer<async_nats::jetstream::consumer::pull::Config>,
    >,
) -> PoolCacheJetstreamBatchOutcome {
    let msg_count = batch_items.len() as u32;
    let mut pool_cache_process_accounted = Duration::ZERO;
    let mut position_price_updates_applied: u32 = 0;

    let phase1_t0 = Instant::now();
    let now_ms_js = wall_clock_unix_ms_now();
    for (_, update) in &batch_items {
        try_record_momentum_jetstream_poolcache_event_to_ingest_ms(
            now_ms_js,
            update.header.ts_unix_ms,
        );
        pool_cache_sync::apply_pool_cache_update(&ctx.live_pool_cache, update);
    }
    pool_cache_process_accounted = pool_cache_process_accounted.saturating_add(phase1_t0.elapsed());

    let derived: Vec<PoolCacheDerivedTps> = batch_items
        .iter()
        .map(|(_, u)| ctx.derive_tokens_per_sol_from_pool_cache_update(u))
        .collect();
    let updates_for_winners: Vec<ironcrab::ipc::PoolCacheUpdate> =
        batch_items.iter().map(|(_, u)| u.clone()).collect();
    let (price_winners, stale_price_path_candidates) =
        select_pool_cache_batch_price_path_winners(&updates_for_winners, &derived);

    let phase2_t0 = Instant::now();
    let coalesced_price_path_keys = price_winners.len() as u32;
    let unique_pools_touched: u32 = batch_items
        .iter()
        .map(|(_, u)| u.pool_address.as_str())
        .collect::<HashSet<_>>()
        .len() as u32;
    let mut max_slot_lag_vs_head: Option<u64> = None;
    if last_slot > 0 {
        for (_, u) in &batch_items {
            if u.geyser_slot > 0 {
                let lag = last_slot.saturating_sub(u.geyser_slot);
                max_slot_lag_vs_head = Some(max_slot_lag_vs_head.unwrap_or(0).max(lag));
            }
        }
    }

    for (_, idx) in price_winners {
        let (_, ref update) = batch_items[idx];
        let Some(Some((ref token_mint, tokens_per_sol, _, token_ui, sol_ui))) = derived.get(idx)
        else {
            continue;
        };
        ctx.merge_latest_pool_reserve_price_hint_from_derived(update, token_mint, *tokens_per_sol);
        if let Ok(mint_pk) = solana_sdk::pubkey::Pubkey::from_str(token_mint) {
            if ctx.live_pool_cache.is_pumpfun_complete_for_mint(&mint_pk) == Some(true)
                || ctx
                    .live_pool_cache
                    .pumpfun_bonding_curve_complete_for_mint(&mint_pk)
            {
                ctx.merge_pumpfun_migration_complete_evidence(
                    token_mint,
                    update.geyser_slot,
                    update.header.ts_unix_ms,
                );
            }
        }
        trace!(
            mint = %token_mint,
            pool = %update.pool_address,
            dex = %update.dex,
            base_reserve = update.base_reserve,
            quote_reserve = update.quote_reserve,
            base_mint = %update.base_mint,
            token_ui,
            sol_ui,
            tokens_per_sol,
            "PoolCacheUpdate updating position price"
        );
        if ctx.update_position_price(
            token_mint,
            *tokens_per_sol,
            None,
            Some(update.pool_address.as_str()),
            Some(update.geyser_slot),
        ) {
            position_price_updates_applied = position_price_updates_applied.saturating_add(1);
        }
    }
    pool_cache_process_accounted = pool_cache_process_accounted.saturating_add(phase2_t0.elapsed());

    for (msg, _) in batch_items {
        let _ = msg.ack().await;
    }

    let mut execution_results_drained: u32 = 0;
    if msg_count > 0 {
        trace!(
            updates = msg_count,
            "SLAVE CACHE: processed PoolCacheUpdates"
        );
        if ctx.position_count() > 0 {
            ctx.process_exit_signals(None).await;
        }
        if let Some(er_consumer) = execution_js_consumer {
            execution_results_drained = drain_execution_results(
                er_consumer,
                ctx,
                EXECUTION_RESULT_INTERLEAVED_DRAIN_MAX,
                EXECUTION_RESULT_INTERLEAVED_FETCH_EXPIRES,
                last_slot,
                "after_pool_cache_batch",
            )
            .await;
        }
    }

    PoolCacheJetstreamBatchOutcome {
        msg_count,
        execution_results_drained,
        position_price_updates_applied,
        pool_cache_process_accounted,
        unique_pools_touched,
        coalesced_price_path_keys,
        stale_price_path_candidates,
        max_slot_lag_vs_head,
    }
}

/// Bounded JetStream work after each Core NATS ingest batch so PoolCache / ExecutionResult paths
/// make progress even when `sub.next()` stays ready (no `biased` starvation).
async fn momentum_interleave_jetstream_after_core_market_batch(
    pool_cache_consumer_opt: &mut Option<
        async_nats::jetstream::consumer::Consumer<async_nats::jetstream::consumer::pull::Config>,
    >,
    execution_js_consumer: Option<
        &async_nats::jetstream::consumer::Consumer<async_nats::jetstream::consumer::pull::Config>,
    >,
    ctx: &Arc<MomentumContext>,
    last_slot: u64,
) {
    if let Some(er) = execution_js_consumer {
        let _ = drain_execution_results(
            er,
            ctx,
            6,
            EXECUTION_RESULT_INTERLEAVED_FETCH_EXPIRES,
            last_slot,
            "after_core_market_batch_guaranteed_er",
        )
        .await;
    }
    let Some(ref mut consumer) = pool_cache_consumer_opt else {
        return;
    };
    use futures::StreamExt;
    let Ok(mut messages) = consumer
        .fetch()
        .max_messages(POOL_CACHE_UPDATE_INTERLEAVE_AFTER_CORE_MAX)
        .expires(POOL_CACHE_UPDATE_INTERLEAVED_FETCH_EXPIRES)
        .messages()
        .await
    else {
        return;
    };
    let mut batch_items: Vec<(
        async_nats::jetstream::message::Message,
        ironcrab::ipc::PoolCacheUpdate,
    )> = Vec::new();
    let mut batch_messages: u32 = 0;
    while let Some(msg_result) = messages.next().await {
        match msg_result {
            Ok(msg) => {
                batch_messages = batch_messages.saturating_add(1);
                if let Ok(update) =
                    serde_json::from_slice::<ironcrab::ipc::PoolCacheUpdate>(&msg.payload)
                {
                    batch_items.push((msg, update));
                } else {
                    let _ = msg.ack().await;
                }
            }
            Err(e) => {
                trace!(error = %e, "PoolCacheUpdate interleave fetch error");
            }
        }
    }
    if batch_items.is_empty() {
        return;
    }
    let _ = momentum_apply_pool_cache_jetstream_batch_items(
        ctx,
        batch_items,
        batch_messages,
        last_slot,
        execution_js_consumer,
    )
    .await;
}

/// Bounded JetStream pull for `ExecutionResult` — no busy-wait, at most `max_messages` per call.
///
/// `fetch_expires`: use `EXECUTION_RESULT_SCHEDULED_FETCH_EXPIRES` from the dedicated `select!` arm;
/// use `EXECUTION_RESULT_INTERLEAVED_FETCH_EXPIRES` when this runs after a MarketEvents ingest batch
/// or PoolCache batch (bounded interleaved drain).
async fn drain_execution_results(
    consumer: &async_nats::jetstream::consumer::Consumer<
        async_nats::jetstream::consumer::pull::Config,
    >,
    ctx: &Arc<MomentumContext>,
    max_messages: usize,
    fetch_expires: Duration,
    last_head_slot: u64,
    interleave_source: &'static str,
) -> u32 {
    use futures::StreamExt;

    let mut messages = match consumer
        .fetch()
        .max_messages(max_messages)
        .expires(fetch_expires)
        .messages()
        .await
    {
        Ok(m) => m,
        Err(e) => {
            trace!(
                momentum_scope_c = "execution_result_drain",
                interleave_source,
                fetch_expires_ms = fetch_expires.as_millis() as u64,
                error = %e,
                "ExecutionResult JetStream fetch failed (may be empty)"
            );
            return 0;
        }
    };

    let mut processed: u32 = 0;
    while let Some(msg_res) = messages.next().await {
        match msg_res {
            Ok(msg) => {
                ironcrab::metrics::record_activity();
                NATS_MESSAGES_RECEIVED_TOTAL.fetch_add(1, Ordering::Relaxed);
                match serde_json::from_slice::<ExecutionResult>(&msg.payload) {
                    Ok(result) => {
                        let now_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_millis() as u64)
                            .unwrap_or(0);
                        let ingest_lag_ms = execution_result_ingest_lag_ms(&result, now_ms);
                        let slot_lag_vs_head = result
                            .confirmed_slot
                            .map(|c| last_head_slot.saturating_sub(c));
                        trace!(
                            momentum_scope_c = "execution_result_msg",
                            interleave_source,
                            intent_id = %result.intent_id,
                            status = ?result.status,
                            ingest_lag_ms,
                            confirmed_slot = ?result.confirmed_slot,
                            slot_lag_vs_last_event_slot = ?slot_lag_vs_head,
                            producer_ts_unix_ms = result.header.ts_unix_ms,
                        );
                        ctx.handle_execution_result(&result);
                        processed = processed.saturating_add(1);
                    }
                    Err(e) => {
                        warn!(
                            momentum_scope_c = "execution_result_drain",
                            interleave_source,
                            error = %e,
                            "Failed to deserialize ExecutionResult"
                        );
                    }
                }
                if let Err(e) = msg.ack().await {
                    warn!(
                        momentum_scope_c = "execution_result_drain",
                        interleave_source,
                        error = %e,
                        "Failed to ack ExecutionResult"
                    );
                }
            }
            Err(e) => {
                warn!(
                    momentum_scope_c = "execution_result_drain",
                    interleave_source,
                    error = %e,
                    "ExecutionResult stream message error"
                );
            }
        }
    }

    if processed > 0 {
        debug!(
            momentum_scope_c = "execution_result_drain",
            interleave_source,
            processed_count = processed,
            max_messages,
            fetch_expires_ms = fetch_expires.as_millis() as u64,
            fetch_expires_profile = if fetch_expires <= EXECUTION_RESULT_INTERLEAVED_FETCH_EXPIRES {
                "interleaved"
            } else {
                "scheduled"
            },
            last_head_slot,
            "ExecutionResult bounded drain completed"
        );
    }

    processed
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;
    use ironcrab::ipc::{FillStatus, PoolCacheUpdate, PoolCacheUpdateType, RecordHeader};
    use tempfile::TempDir;

    fn test_queued_jsonl_writer(dir: &std::path::Path) -> QueuedJsonlWriter {
        let jsonl_config = JsonlWriterConfig::new("trade_intents").with_log_dir(dir);
        QueuedJsonlWriter::spawn(jsonl_config, 64).expect("jsonl writer")
    }

    fn noop_intent_publish_tx() -> mpsc::Sender<IntentPublishJob> {
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        tx
    }

    async fn wait_for_intent_jsonl_records(ctx: &MomentumContext, min_records: u64) {
        for _ in 0..200 {
            let _ = ctx.jsonl_writer.flush();
            if ctx.jsonl_writer.stats().0 >= min_records {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!(
            "timed out waiting for >= {min_records} JSONL records (got {})",
            ctx.jsonl_writer.stats().0
        );
    }

    fn empty_test_context_with_publish_sender(
        jsonl_writer: QueuedJsonlWriter,
        config: MomentumConfig,
        intent_publish_tx: mpsc::Sender<IntentPublishJob>,
    ) -> Arc<MomentumContext> {
        Arc::new(MomentumContext {
            // `next_intent_id` prefixes with `run_id[..8]`; keep at least 8 chars.
            run_id: "run-test00".to_string(),
            config: parking_lot::RwLock::new(config),
            nats: None,
            jsonl_writer,
            intent_publish_tx: parking_lot::Mutex::new(Some(intent_publish_tx)),
            intent_counter: std::sync::atomic::AtomicU64::new(0),
            pool_first_seen: parking_lot::RwLock::new(std::collections::HashMap::new()),
            pending_dev_info: parking_lot::RwLock::new(HashMap::new()),
            pending_pool_accounts: parking_lot::RwLock::new(HashMap::new()),
            mint_infos: parking_lot::RwLock::new(HashMap::new()),
            token_trackers: parking_lot::RwLock::new(HashMap::new()),
            tracker_keys_by_mint: parking_lot::RwLock::new(HashMap::new()),
            dirty_entry_tracker_keys: parking_lot::RwLock::new(BTreeSet::new()),
            pumpfun_buy_missing_creator_suppress_until: parking_lot::RwLock::new(HashMap::new()),
            positions: parking_lot::RwLock::new(HashMap::new()),
            pending_intents: parking_lot::RwLock::new(HashMap::new()),
            latest_bonding_by_mint: parking_lot::RwLock::new(HashMap::new()),
            latest_pumpfun_migration_complete_by_mint: parking_lot::RwLock::new(HashMap::new()),
            latest_pool_reserve_price_hint_by_mint_pool: parking_lot::RwLock::new(HashMap::new()),
            pending_buy_entries: parking_lot::RwLock::new(HashMap::new()),
            pending_buy_mint_index: parking_lot::RwLock::new(HashMap::new()),
            mint_pools: parking_lot::RwLock::new(HashMap::new()),
            live_pool_cache: LivePoolCache::new(),
            open_position_pool_recovery_last_sent: parking_lot::RwLock::new(HashMap::new()),
            position_kv: tokio::sync::OnceCell::new(),
            orphaned_mints: parking_lot::RwLock::new(HashMap::new()),
            orphaned_recovered_intent_ids: parking_lot::RwLock::new(BoundedIntentIdCache::new(
                ORPHANED_RECOVERED_INTENT_IDS_CAP,
            )),
            latest_wallet_balance_raw_by_mint: parking_lot::RwLock::new(HashMap::new()),
            tokens_tracked: std::sync::atomic::AtomicU64::new(0),
            tokens_blacklisted: std::sync::atomic::AtomicU64::new(0),
            blacklisted_mints_for_metric: parking_lot::RwLock::new(HashSet::new()),
            intents_generated: std::sync::atomic::AtomicU64::new(0),
            exits_generated: std::sync::atomic::AtomicU64::new(0),
            last_event_slot: std::sync::atomic::AtomicU64::new(0),
            last_event_ts_ms: std::sync::atomic::AtomicU64::new(0),
            momentum_active_pool_publish_queue: parking_lot::Mutex::new(
                MomentumActivePoolPublishQueue::default(),
            ),
        })
    }

    fn empty_test_context_with_config(
        jsonl_writer: QueuedJsonlWriter,
        config: MomentumConfig,
    ) -> Arc<MomentumContext> {
        empty_test_context_with_publish_sender(jsonl_writer, config, noop_intent_publish_tx())
    }

    fn empty_test_context_with_intent_worker(
        jsonl_writer: QueuedJsonlWriter,
        config: MomentumConfig,
    ) -> Arc<MomentumContext> {
        let (intent_publish_tx, intent_publish_rx) = mpsc::channel(INTENT_PUBLISH_QUEUE_CAP);
        let ctx = empty_test_context_with_publish_sender(jsonl_writer, config, intent_publish_tx);
        // Detach worker for test lifetime: aborting the JoinHandle would stop in-flight publishes.
        std::mem::forget(spawn_intent_publish_worker(
            Arc::clone(&ctx),
            intent_publish_rx,
            None,
            None,
        ));
        ctx
    }

    fn empty_test_context(jsonl_writer: QueuedJsonlWriter) -> Arc<MomentumContext> {
        empty_test_context_with_config(jsonl_writer, MomentumConfig::default())
    }

    #[tokio::test]
    async fn exit_intent_enqueue_does_not_publish_in_caller_path() {
        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let (tx, mut rx) = mpsc::channel(INTENT_PUBLISH_QUEUE_CAP);
        let ctx =
            empty_test_context_with_publish_sender(jsonl_writer, MomentumConfig::default(), tx);

        let mint = "ExitEnqueueMinttttttttttttttttttttttttttttt";
        let pool = "ExitEnqueuePooltttttttttttttttttttttttttttt";
        ctx.register_pool(mint, pool, "raydium", 1);
        ctx.update_pool_accounts(mint, pool, vec!["a0".to_string(), "a1".to_string()]);
        ctx.open_position(OpenPositionParams {
            mint,
            pool,
            dex: "raydium",
            entry_price: 1.0,
            token_decimals: 6,
            token_amount: 1_000_000,
            sol_invested: 1_000_000,
            token_program: None,
            creator: None,
            entry_confirmed_slot: 1,
            initial_bonding: None,
        });

        let published_before = NATS_MESSAGES_PUBLISHED_TOTAL.load(Ordering::Relaxed);
        tokio::time::timeout(
            Duration::from_millis(50),
            generate_and_publish_exit_intent(
                ctx.as_ref(),
                mint,
                pool,
                "raydium",
                "TIME_EXIT",
                "enqueue test",
                1_000_000,
                None,
            ),
        )
        .await
        .expect("exit intent enqueue should return quickly without awaiting worker publish")
        .expect("exit intent enqueue");

        assert_eq!(
            NATS_MESSAGES_PUBLISHED_TOTAL.load(Ordering::Relaxed),
            published_before,
            "caller path must not JetStream-publish"
        );
        let job = rx
            .try_recv()
            .expect("exit intent must be enqueued for worker");
        assert_eq!(job.intent.side, TradeSide::Sell);
        assert!(!job.intent.intent_id.is_empty());
    }

    #[tokio::test]
    async fn intent_publish_worker_preserves_fifo_order() {
        let processed = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let (tx, rx) = mpsc::channel(INTENT_PUBLISH_QUEUE_CAP);
        let ctx =
            empty_test_context_with_publish_sender(jsonl_writer, MomentumConfig::default(), tx);
        std::mem::forget(spawn_intent_publish_worker(
            Arc::clone(&ctx),
            rx,
            None,
            Some(Arc::clone(&processed)),
        ));

        let ids = ["int-order-001", "int-order-002", "int-order-003"];
        for intent_id in ids {
            let intent = TradeIntent::new(
                "momentum-bot",
                BUILD_VERSION,
                &ctx.run_id,
                intent_id.to_string(),
                "momentum-bot",
                IntentTier::Tier1,
                IntentOrigin::StrategyA,
                ExplicitAmount::new(1, 6),
                TradeResources::default(),
                0,
                500,
                TradeSide::Sell,
                TradingRegime::Early,
            );
            ctx.enqueue_intent_publish(IntentPublishJob {
                intent,
                post_action: IntentPublishPostAction::Exit {
                    mint: "fifo-test-mint".to_string(),
                    source_event_ts_unix_ms: None,
                    slot_seen_at_ms: 0,
                },
            })
            .await
            .expect("enqueue");
        }

        for _ in 0..200 {
            if processed.lock().len() >= ids.len() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(&*processed.lock(), ids.as_slice());
    }

    #[test]
    fn lp_removal_observed_at_mint_merge_uses_max_for_wallclock_recency() {
        let base = Instant::now();
        let earlier = base.checked_sub(Duration::from_secs(120)).expect("instant");
        let later = base.checked_sub(Duration::from_secs(2)).expect("instant");
        assert_eq!(
            merge_lp_removal_observed_at_mint_aggregate(Some(earlier), Some(later)),
            Some(later)
        );
        let window_secs = 10u64;
        let merged = merge_lp_removal_observed_at_mint_aggregate(Some(earlier), Some(later))
            .expect("merged");
        assert!(
            merged.elapsed().as_secs() <= window_secs,
            "max-merge keeps LP removal observation recent for wallclock window gating"
        );
        let min_bug = earlier.min(later);
        assert!(
            min_bug.elapsed().as_secs() > window_secs,
            "min-merge would treat LP removal as outside window and suppress hard exit"
        );
    }

    #[test]
    fn momentum_pool_cache_live_consumer_is_new_not_last_per_subject() {
        use async_nats::jetstream;
        let live = ironcrab::nats::pool_cache_live_consumer_config();
        assert!(matches!(
            live.deliver_policy,
            jetstream::consumer::DeliverPolicy::New
        ));
        let slave = ironcrab::nats::slave_consumer_config();
        assert!(matches!(
            slave.deliver_policy,
            jetstream::consumer::DeliverPolicy::LastPerSubject
        ));
    }

    #[test]
    fn open_position_pool_recovery_kinds_mapping() {
        assert_eq!(
            open_position_pool_recovery_kinds_for_normalized_dex("pumpfun"),
            &[
                OpenPositionPoolRecoveryKind::PumpfunBonding,
                OpenPositionPoolRecoveryKind::PumpAmmMigrationProbe,
            ]
        );
        assert_eq!(
            open_position_pool_recovery_kinds_for_normalized_dex("pump_amm"),
            &[OpenPositionPoolRecoveryKind::PumpAmmAccounts]
        );
        assert!(open_position_pool_recovery_kinds_for_normalized_dex("unknown_dex").is_empty());
    }

    #[test]
    fn open_position_recovery_pumpfun_emits_bonding_and_pump_amm_migration_probe() {
        use ironcrab::ipc::ControlRequestKind;
        let bonding = MomentumContext::build_open_position_pool_control_request(
            "runidxxxx",
            "MintMintMintMintMintMintMintMintMintMintMintMint",
            "CrveCrveCrveCrveCrveCrveCrveCrveCrveCrveCrveCrve",
            OpenPositionPoolRecoveryKind::PumpfunBonding,
        );
        assert_eq!(bonding.1, OpenPositionPoolRecoveryKind::PumpfunBonding);
        assert!(bonding.0.force_refresh_pumpfun);
        assert!(matches!(
            bonding.0.kind,
            ControlRequestKind::EnsurePumpfunBondingCurve { .. }
        ));

        let amm_probe = MomentumContext::build_open_position_pool_control_request(
            "runidxxxx",
            "MintMintMintMintMintMintMintMintMintMintMintMint",
            "CrveCrveCrveCrveCrveCrveCrveCrveCrveCrveCrveCrve",
            OpenPositionPoolRecoveryKind::PumpAmmMigrationProbe,
        );
        assert_eq!(
            amm_probe.1,
            OpenPositionPoolRecoveryKind::PumpAmmMigrationProbe
        );
        assert!(amm_probe.0.force_refresh);
        assert!(amm_probe.0.pool_address_hint.is_none());
        assert!(matches!(
            amm_probe.0.kind,
            ControlRequestKind::EnsurePumpAmmPoolAccounts { .. }
        ));
    }

    #[test]
    fn open_position_recovery_control_request_pump_amm_pool_hint_and_force_refresh() {
        use ironcrab::ipc::ControlRequestKind;
        let (req, k) = MomentumContext::build_open_position_pool_control_request(
            "runidxxxx",
            "MintMintMintMintMintMintMintMintMintMintMintMint",
            "PoolPoolPoolPoolPoolPoolPoolPoolPoolPoolPoolPool",
            OpenPositionPoolRecoveryKind::PumpAmmAccounts,
        );
        assert_eq!(k, OpenPositionPoolRecoveryKind::PumpAmmAccounts);
        assert!(req.force_refresh);
        assert_eq!(
            req.pool_address_hint.as_deref(),
            Some("PoolPoolPoolPoolPoolPoolPoolPoolPoolPoolPoolPool")
        );
        assert!(matches!(
            req.kind,
            ControlRequestKind::EnsurePumpAmmPoolAccounts { .. }
        ));
    }

    const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";
    const USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
    const USDT_MINT: &str = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB";

    #[test]
    fn get_or_create_tracker_skips_non_tradeable_mints() {
        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);

        let before_tracked = ctx.tokens_tracked.load(Ordering::Relaxed);
        let cases = [
            (WSOL_MINT, "poolNt1"),
            ("NATIVE_SOL", "poolNt2"),
            ("11111111111111111111111111111111", "poolNt3"),
            (USDC_MINT, "poolNt4"),
            (USDT_MINT, "poolNt5"),
        ];
        for (mint, pool) in cases {
            assert!(
                !ctx.get_or_create_tracker(mint, pool, "raydium", 1, 10_000_000_000),
                "non-tradeable mint must not create tracker: {mint}"
            );
        }
        assert_eq!(
            ctx.tokens_tracked.load(Ordering::Relaxed),
            before_tracked,
            "tokens_tracked must not bump for skipped mints"
        );
        assert!(
            ctx.token_trackers.read().is_empty(),
            "no pool-scoped rows for quote/native/stable mints"
        );
    }

    #[test]
    fn prune_stale_discovery_retains_new_tracker_without_market_trades() {
        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);
        let mint = "MintPruneNew1111111111111111111111111111";
        let pool = "poolPruneNew1";
        let key = MomentumContext::tracker_storage_key(mint, pool);
        ctx.token_trackers.write().insert(
            key.clone(),
            TokenTracker::test_fixture_stale_discovery(
                mint,
                pool,
                TrackerState::Discovery,
                std::time::Duration::from_secs(120),
                None,
            ),
        );
        let removed = ctx.prune_stale_discovery_trackers();
        assert!(
            removed.is_empty(),
            "Discovery ohne Markt-Trade darf nicht sofort entfernt werden (first_seen jung)"
        );
        assert!(ctx.token_trackers.read().contains_key(&key));
    }

    #[test]
    fn prune_stale_discovery_removes_tracker_idle_over_30m_no_position() {
        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);
        let mint = "MintPruneOld1111111111111111111111111111";
        let pool = "poolPruneOld1";
        let key = MomentumContext::tracker_storage_key(mint, pool);
        ctx.token_trackers.write().insert(
            key.clone(),
            TokenTracker::test_fixture_stale_discovery(
                mint,
                pool,
                TrackerState::Validation,
                std::time::Duration::from_secs(31 * 60),
                None,
            ),
        );
        let removed = ctx.prune_stale_discovery_trackers();
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].reason, "stale_discovery");
        assert!(!ctx.token_trackers.read().contains_key(&key));
    }

    #[test]
    fn prune_stale_discovery_skips_when_open_position_same_pool() {
        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);
        let mint = "MintPrunePos1111111111111111111111111111";
        let pool = "poolPrunePos1";
        let key = MomentumContext::tracker_storage_key(mint, pool);
        ctx.token_trackers.write().insert(
            key.clone(),
            TokenTracker::test_fixture_stale_discovery(
                mint,
                pool,
                TrackerState::Discovery,
                std::time::Duration::from_secs(31 * 60),
                None,
            ),
        );
        ctx.positions.write().insert(
            mint.to_string(),
            PositionTracker::new(mint, pool, "raydium", 1.0, 6, 1_000, 1_000_000),
        );
        let removed = ctx.prune_stale_discovery_trackers();
        assert!(removed.is_empty());
        assert!(ctx.token_trackers.read().contains_key(&key));
    }

    #[test]
    fn momentum_active_pool_flush_skipped_without_nats_keeps_queue() {
        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);
        ctx.queue_momentum_active_pool_active(
            "MintAct11111111111111111111111111111111",
            "poolAct11111111111111111111111111111111",
            MomentumActivePinReason::Tracker,
        );
        ctx.flush_momentum_active_pool_publish_queue();
        let q = ctx.momentum_active_pool_publish_queue.lock();
        assert_eq!(q.active.len(), 1);
        assert!(q.removed.is_empty());
    }

    #[test]
    fn wait_filter_obs_dedupes_repeated_same_wait_class() {
        let mut cfg = MomentumConfig::default();
        cfg.early_min_liquidity_sol = 100.0;
        let mut tracker = TokenTracker::new(
            "MintWaitDedup11111111111111111111111111",
            "poolWaitDedup",
            "pumpfun",
            1,
            1_000_000_000,
        );

        let (_st1, r1) = tracker.should_generate_intent(&cfg, None, 50_000, Instant::now());
        assert!(r1.contains("WAIT_INSUFFICIENT_LIQUIDITY"));
        assert_eq!(
            tracker.last_wait_filter_obs_key_for_test(),
            Some("WAIT_INSUFFICIENT_LIQUIDITY")
        );

        let (_st2, r2) = tracker.should_generate_intent(&cfg, None, 50_000, Instant::now());
        assert!(r2.contains("WAIT_INSUFFICIENT_LIQUIDITY"));
        assert_eq!(
            tracker.last_wait_filter_obs_key_for_test(),
            Some("WAIT_INSUFFICIENT_LIQUIDITY")
        );

        cfg.early_min_liquidity_sol = 0.0;
        cfg.min_unique_buyers = 99;
        cfg.min_trades_per_min = 0.0;
        cfg.min_buy_dominance = 0.0;
        cfg.min_sol_inflow_lamports = 0;
        cfg.require_mint_authority_renounced = false;
        cfg.require_freeze_authority_none = false;
        cfg.top1_buyer_share_cap = 1.0;
        cfg.top3_buyer_share_cap = 1.0;
        cfg.repeat_buyer_min_ratio = 0.0;
        cfg.min_trade_size_lamports = 0;
        cfg.small_buy_ratio_cap = 1.0;

        let (_st3, r3) = tracker.should_generate_intent(&cfg, None, 50_000, Instant::now());
        assert!(r3.contains("WAIT_BUYER_WINDOW"));
        assert_eq!(
            tracker.last_wait_filter_obs_key_for_test(),
            Some("WAIT_BUYER_WINDOW"),
            "new wait class after liquidity gate passes must update dedupe key"
        );
    }

    #[test]
    fn velocity_trades_per_min_uses_chain_slot_span() {
        let mut cfg = MomentumConfig::default();
        cfg.early_min_liquidity_sol = 0.0;
        cfg.min_unique_buyers = 3;
        cfg.min_buy_dominance = 0.0;
        cfg.min_sol_inflow_lamports = 0;
        cfg.min_trades_per_min = 4.0;
        cfg.require_mint_authority_renounced = false;
        cfg.require_freeze_authority_none = false;
        cfg.buyer_window_secs = 120;
        cfg.top1_buyer_share_cap = 1.0;
        cfg.top3_buyer_share_cap = 1.0;
        cfg.repeat_buyer_min_ratio = 0.0;
        cfg.min_trade_size_lamports = 0;
        cfg.small_buy_ratio_cap = 1.0;
        cfg.dump_recovery_window_secs = 0;

        let mut tracker = TokenTracker::new(
            "MintVelMinM1111111111111111111111111111",
            "poolVelMinM111111111111111111111111111",
            "pumpfun",
            1,
            30_000_000_000,
        );
        let base = 199_900u64;
        for i in 0..9 {
            tracker.record_trade(
                &format!("buyer{i}"),
                true,
                100_000_000,
                1_000_000,
                &format!("sigv{i}"),
                base,
                &cfg,
            );
        }
        tracker.record_trade(
            "buyer_last",
            true,
            100_000_000,
            1_000_000,
            "sigv_last",
            base + 30,
            &cfg,
        );

        let (ok, reason) = tracker.should_generate_intent(&cfg, None, 200_000, Instant::now());
        assert!(ok, "expected pass at 30/min threshold: {reason}");
        assert!(reason.contains("trades/min"), "{reason}");

        cfg.min_trades_per_min = 100.0;
        let (ok2, reason2) = tracker.should_generate_intent(&cfg, None, 200_000, Instant::now());
        assert!(!ok2, "expected fail at 100/min threshold");
        assert!(reason2.contains("trades_per_min"), "{reason2}");
    }

    #[test]
    fn check_for_signals_emits_probe_for_tradeable_pumpfun_style_mint() {
        let cfg = {
            let mut c = MomentumConfig::default();
            c.default_position_lamports = 1_000;
            c.probe_buy_pct = 0.25;
            c.early_min_liquidity_sol = 0.0;
            c.min_unique_buyers = 0;
            c.min_trades_per_min = 0.0;
            c.min_buy_dominance = 0.0;
            c.min_sol_inflow_lamports = 0;
            c.require_mint_authority_renounced = false;
            c.require_freeze_authority_none = false;
            c.top1_buyer_share_cap = 1.0;
            c.top3_buyer_share_cap = 1.0;
            c.repeat_buyer_min_ratio = 0.0;
            c.min_trade_size_lamports = 0;
            c.small_buy_ratio_cap = 1.0;
            c.min_token_age_secs = 0;
            c
        };

        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);
        *ctx.config.write() = cfg.clone();

        let mint = "MintHotfixRegPump4444444444444444444444444444";
        let pool = "pumpPoolHotfixReg4444444444444444444444444444";

        assert!(ctx.get_or_create_tracker(mint, pool, "pumpfun", 1, 30_000_000_000));

        {
            let mut trackers = ctx.token_trackers.write();
            let tr = trackers
                .get_mut(&MomentumContext::tracker_storage_key(mint, pool))
                .expect("tracker");
            for i in 0..20 {
                tr.record_trade(
                    &format!("pf{i:03}"),
                    true,
                    200_000_000,
                    2_000_000,
                    &format!("sighot{i:03}"),
                    1 + i as u64,
                    &cfg,
                );
            }
        }

        let signals = ctx.check_for_signals();
        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].mint, mint);
        assert_eq!(signals[0].pool, pool);
        assert_eq!(signals[0].kind, EntryKind::Probe);
    }

    #[test]
    fn mint_index_registers_two_pool_rows_same_mint_and_resync_prunes() {
        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);

        let mint = "MintIdxTwin22222222222222222222222222222";
        let pool_a = "poolIdxA22222222222222222222222222222222";
        let pool_b = "poolIdxB22222222222222222222222222222222";

        assert!(ctx.get_or_create_tracker(mint, pool_a, "pumpfun", 1, 30_000_000_000));
        assert!(ctx.get_or_create_tracker(mint, pool_b, "raydium", 1, 10_000_000_000));

        let keys = ctx.test_sorted_tracker_storage_keys_for_mint(mint);
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&MomentumContext::tracker_storage_key(mint, pool_a)));
        assert!(keys.contains(&MomentumContext::tracker_storage_key(mint, pool_b)));

        let k_remove = MomentumContext::tracker_storage_key(mint, pool_a);
        assert!(ctx.token_trackers.write().remove(&k_remove).is_some());
        ctx.test_resync_tracker_indexes();

        let keys_after = ctx.test_sorted_tracker_storage_keys_for_mint(mint);
        assert_eq!(keys_after.len(), 1);
        assert_eq!(
            keys_after[0],
            MomentumContext::tracker_storage_key(mint, pool_b)
        );
    }

    #[test]
    fn record_dev_info_same_mint_index_leaves_other_mint_untouched() {
        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);

        let mint_a = "MintIdxSameA33333333333333333333333333333";
        let mint_b = "MintIdxOtherB3333333333333333333333333333";
        let pool_a1 = "poolIdxSameA133333333333333333333333333333";
        let pool_a2 = "poolIdxSameA233333333333333333333333333333";
        let pool_b1 = "poolIdxOtherB13333333333333333333333333333";

        assert!(ctx.get_or_create_tracker(mint_a, pool_a1, "pumpfun", 1, 30_000_000_000));
        assert!(ctx.get_or_create_tracker(mint_a, pool_a2, "raydium", 1, 10_000_000_000));
        assert!(ctx.get_or_create_tracker(mint_b, pool_b1, "pumpfun", 1, 30_000_000_000));

        ctx.record_dev_info(mint_a, "DevWalletSame333333333333333333333333333", 1.0);

        let trackers = ctx.token_trackers.read();
        assert!(trackers
            .get(&MomentumContext::tracker_storage_key(mint_b, pool_b1))
            .expect("mint_b tracker")
            .dev_wallet
            .is_none());

        let dev_a1 = trackers
            .get(&MomentumContext::tracker_storage_key(mint_a, pool_a1))
            .expect("mint_a pool1")
            .dev_wallet
            .clone();
        let dev_a2 = trackers
            .get(&MomentumContext::tracker_storage_key(mint_a, pool_a2))
            .expect("mint_a pool2")
            .dev_wallet
            .clone();
        drop(trackers);
        assert_eq!(
            dev_a1.as_deref(),
            Some("DevWalletSame333333333333333333333333333")
        );
        assert_eq!(
            dev_a2.as_deref(),
            Some("DevWalletSame333333333333333333333333333")
        );
    }

    #[test]
    fn dirty_entry_tick_evaluates_only_dirty_tracker_row() {
        let cfg = {
            let mut c = MomentumConfig::default();
            c.default_position_lamports = 1_000;
            c.probe_buy_pct = 0.25;
            c.early_min_liquidity_sol = 0.0;
            c.min_unique_buyers = 0;
            c.min_trades_per_min = 0.0;
            c.min_buy_dominance = 0.0;
            c.min_sol_inflow_lamports = 0;
            c.require_mint_authority_renounced = false;
            c.require_freeze_authority_none = false;
            c.top1_buyer_share_cap = 1.0;
            c.top3_buyer_share_cap = 1.0;
            c.repeat_buyer_min_ratio = 0.0;
            c.min_trade_size_lamports = 0;
            c.small_buy_ratio_cap = 1.0;
            c.min_token_age_secs = 0;
            c
        };

        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);
        *ctx.config.write() = cfg.clone();

        let mint = "MintDirtyEval555555555555555555555555555";
        let pool_hot = "poolDirtyHot55555555555555555555555555555";
        let pool_cold = "poolDirtyCold5555555555555555555555555555";

        assert!(ctx.get_or_create_tracker(mint, pool_hot, "pumpfun", 1, 30_000_000_000));
        assert!(ctx.get_or_create_tracker(mint, pool_cold, "pumpfun", 1, 30_000_000_000));

        {
            let mut trackers = ctx.token_trackers.write();
            let tr = trackers
                .get_mut(&MomentumContext::tracker_storage_key(mint, pool_hot))
                .expect("hot");
            for i in 0..20 {
                tr.record_trade(
                    &format!("pf{i:03}"),
                    true,
                    200_000_000,
                    2_000_000,
                    &format!("sighot{i:03}"),
                    1 + i as u64,
                    &cfg,
                );
            }
        }
        ctx.dirty_entry_tracker_keys.write().clear();
        let hot_key = MomentumContext::tracker_storage_key(mint, pool_hot);
        ctx.mark_entry_eval_dirty_key(&hot_key);

        let signals = ctx.check_for_signals_dirty_priority_tick();
        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].pool, pool_hot);

        let mut cfg_cold = cfg.clone();
        cfg_cold.min_unique_buyers = 99;
        {
            let mut trackers = ctx.token_trackers.write();
            let cold = trackers
                .get_mut(&MomentumContext::tracker_storage_key(mint, pool_cold))
                .expect("cold");
            cold.record_trade(
                "onlyOneBuyerColdPath5555555555555555555",
                true,
                200_000_000,
                2_000_000,
                "sigcold1",
                42,
                &cfg_cold,
            );
        }
        ctx.dirty_entry_tracker_keys.write().clear();
        ctx.mark_entry_eval_dirty_key(&MomentumContext::tracker_storage_key(mint, pool_cold));

        let signals_cold_only = ctx.check_for_signals_dirty_priority_tick();
        assert!(
            signals_cold_only.is_empty(),
            "cold pool with insufficient buyers must not emit when only it is dirty"
        );
    }

    #[test]
    fn dirty_same_mint_second_pool_key_stays_dirty_when_first_pool_emits_this_tick() {
        let cfg = {
            let mut c = MomentumConfig::default();
            c.default_position_lamports = 1_000;
            c.probe_buy_pct = 0.25;
            c.early_min_liquidity_sol = 0.0;
            c.min_unique_buyers = 0;
            c.min_trades_per_min = 0.0;
            c.min_buy_dominance = 0.0;
            c.min_sol_inflow_lamports = 0;
            c.require_mint_authority_renounced = false;
            c.require_freeze_authority_none = false;
            c.top1_buyer_share_cap = 1.0;
            c.top3_buyer_share_cap = 1.0;
            c.repeat_buyer_min_ratio = 0.0;
            c.min_trade_size_lamports = 0;
            c.small_buy_ratio_cap = 1.0;
            c.min_token_age_secs = 0;
            c
        };

        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);
        *ctx.config.write() = cfg.clone();

        let mint = "MintDirtySib6666666666666666666666666666";
        // Lexicographic order on storage key uses pool string; `aaa` sorts before `zzz`.
        let pool_first = "aaaPoolSib666666666666666666666666666666";
        let pool_second = "zzzPoolSib666666666666666666666666666666";

        assert!(ctx.get_or_create_tracker(mint, pool_first, "pumpfun", 1, 30_000_000_000));
        assert!(ctx.get_or_create_tracker(mint, pool_second, "pumpfun", 1, 30_000_000_000));

        for pool in [pool_first, pool_second] {
            let mut trackers = ctx.token_trackers.write();
            let tr = trackers
                .get_mut(&MomentumContext::tracker_storage_key(mint, pool))
                .expect("tracker");
            for i in 0..20 {
                tr.record_trade(
                    &format!("pf{i:03}"),
                    true,
                    200_000_000,
                    2_000_000,
                    &format!("sig{i:03}_{pool}"),
                    1 + i as u64,
                    &cfg,
                );
            }
        }

        ctx.dirty_entry_tracker_keys.write().clear();
        ctx.mark_entry_eval_dirty_key(&MomentumContext::tracker_storage_key(mint, pool_first));
        ctx.mark_entry_eval_dirty_key(&MomentumContext::tracker_storage_key(mint, pool_second));

        let signals = ctx.check_for_signals_dirty_priority_tick();
        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].pool, pool_first);

        let key_second = MomentumContext::tracker_storage_key(mint, pool_second);
        assert!(
            ctx.dirty_entry_tracker_keys.read().contains(&key_second),
            "second same-mint pool must stay dirty for a follow-up tick when capped at one emit/tick"
        );
    }

    #[tokio::test]
    async fn generate_buy_pumpfun_missing_creator_unwinds_probe_pending_and_skips_pending_intent() {
        let cfg = {
            let mut c = MomentumConfig::default();
            c.default_position_lamports = 1_000;
            c.probe_buy_pct = 0.25;
            c.early_min_liquidity_sol = 0.0;
            c.min_unique_buyers = 0;
            c.min_trades_per_min = 0.0;
            c.min_buy_dominance = 0.0;
            c.min_sol_inflow_lamports = 0;
            c.require_mint_authority_renounced = false;
            c.require_freeze_authority_none = false;
            c.top1_buyer_share_cap = 1.0;
            c.top3_buyer_share_cap = 1.0;
            c.repeat_buyer_min_ratio = 0.0;
            c.min_trade_size_lamports = 0;
            c.small_buy_ratio_cap = 1.0;
            c.min_token_age_secs = 0;
            c
        };

        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);
        *ctx.config.write() = cfg;

        let mint = "MintUnwindPF7777777777777777777777777777";
        let pool = "poolUnwindPf7777777777777777777777777777";

        assert!(ctx.get_or_create_tracker(mint, pool, "pumpfun", 1, 30_000_000_000));

        let sk = MomentumContext::tracker_storage_key(mint, pool);
        {
            let mut trackers = ctx.token_trackers.write();
            let tr = trackers.get_mut(&sk).expect("tracker");
            for i in 0..8 {
                tr.record_trade(
                    &format!("pf{i:03}"),
                    true,
                    200_000_000,
                    2_000_000,
                    &format!("sigunw{i:03}"),
                    1 + i as u64,
                    &ctx.config.read(),
                );
            }
            tr.state = TrackerState::ProbeBuyPending {
                sent_at: Instant::now(),
            };
            assert!(
                tr.dev_wallet.is_none(),
                "test expects missing pumpfun creator"
            );
        }

        let signal = EntrySignal {
            mint: mint.to_string(),
            pool: pool.to_string(),
            dex: "pumpfun".to_string(),
            sol_amount: 250,
            kind: EntryKind::Probe,
            reason: "ENTER_PROBE_BUY: test unwind".to_string(),
        };

        assert!(
            generate_and_publish_buy_intent(&ctx, &signal)
                .await
                .is_err(),
            "missing creator must fail pumpfun BUY build"
        );

        let trackers = ctx.token_trackers.read();
        let st = &trackers.get(&sk).expect("tracker post-unwind").state;
        assert!(
            matches!(st, TrackerState::Validation),
            "probe pending must unwind to Validation; got {st:?}"
        );
        drop(trackers);
        assert!(
            ctx.pending_intents.read().is_empty(),
            "JSONL/register_buy_intent must not run after creator failure — no ghost pending_intents row"
        );
    }

    #[test]
    fn unwind_probe_and_scale_in_pending_restores_evaluable_states() {
        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);

        let mint = "MintUnwindManual88888888888888888888888888";
        let pool = "poolUnwindMan88888888888888888888888888888";
        assert!(ctx.get_or_create_tracker(mint, pool, "raydium", 1, 10_000_000_000));

        let sk = MomentumContext::tracker_storage_key(mint, pool);
        {
            let mut trackers = ctx.token_trackers.write();
            let tr = trackers.get_mut(&sk).unwrap();
            tr.state = TrackerState::ProbeBuyPending {
                sent_at: Instant::now(),
            };
        }
        ctx.unwind_stale_entry_pending_after_publish_failure(
            &EntrySignal {
                mint: mint.to_string(),
                pool: pool.to_string(),
                dex: "raydium".to_string(),
                sol_amount: 1,
                kind: EntryKind::Probe,
                reason: "t".to_string(),
            },
            true,
        );
        {
            let trackers = ctx.token_trackers.read();
            assert!(matches!(
                trackers.get(&sk).unwrap().state,
                TrackerState::Validation
            ));
        }

        {
            let mut trackers = ctx.token_trackers.write();
            let tr = trackers.get_mut(&sk).unwrap();
            tr.state = TrackerState::ScaleInPending {
                sent_at: Instant::now(),
            };
        }
        ctx.unwind_stale_entry_pending_after_publish_failure(
            &EntrySignal {
                mint: mint.to_string(),
                pool: pool.to_string(),
                dex: "raydium".to_string(),
                sol_amount: 1,
                kind: EntryKind::ScaleIn,
                reason: "t".to_string(),
            },
            true,
        );
        let trackers = ctx.token_trackers.read();
        assert!(matches!(
            trackers.get(&sk).unwrap().state,
            TrackerState::PositionOpenProbe { .. }
        ));
    }

    #[test]
    fn is_non_tradeable_momentum_mint_covers_known_quote_assets() {
        assert!(is_non_tradeable_momentum_mint(WSOL_MINT));
        assert!(is_non_tradeable_momentum_mint("NATIVE_SOL"));
        assert!(is_non_tradeable_momentum_mint(
            "11111111111111111111111111111111"
        ));
        assert!(is_non_tradeable_momentum_mint(USDC_MINT));
        assert!(is_non_tradeable_momentum_mint(USDT_MINT));
        assert!(!is_non_tradeable_momentum_mint(
            "MintRealTokenHotfix111111111111111111"
        ));
    }

    fn pool_cache_update_stub(
        mint: &str,
        pool: &str,
        token_reserve_raw: u64,
        sol_lamports: u64,
        geyser_slot: u64,
        ts_unix_ms: u64,
    ) -> PoolCacheUpdate {
        let mut header = RecordHeader::new("test", BUILD_VERSION, "run");
        header.ts_unix_ms = ts_unix_ms;
        PoolCacheUpdate {
            header,
            update_type: PoolCacheUpdateType::BalanceUpdated,
            pool_address: pool.to_string(),
            dex: "raydium".to_string(),
            base_mint: mint.to_string(),
            quote_mint: WSOL_MINT.to_string(),
            base_reserve: token_reserve_raw,
            quote_reserve: sol_lamports,
            liquidity_lamports: None,
            geyser_slot,
            metadata: None,
        }
    }

    fn pool_cache_update_two_tokens_no_wsol(
        base_mint: &str,
        quote_mint: &str,
        pool: &str,
        base_reserve: u64,
        quote_reserve: u64,
        geyser_slot: u64,
        ts_unix_ms: u64,
    ) -> PoolCacheUpdate {
        let mut header = RecordHeader::new("test", BUILD_VERSION, "run");
        header.ts_unix_ms = ts_unix_ms;
        PoolCacheUpdate {
            header,
            update_type: PoolCacheUpdateType::BalanceUpdated,
            pool_address: pool.to_string(),
            dex: "raydium".to_string(),
            base_mint: base_mint.to_string(),
            quote_mint: quote_mint.to_string(),
            base_reserve,
            quote_reserve,
            liquidity_lamports: None,
            geyser_slot,
            metadata: None,
        }
    }

    #[test]
    fn sticky_pool_reserve_hint_non_wsol_pair_not_cached() {
        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);
        ctx.merge_latest_pool_reserve_price_hint_from_update(
            &pool_cache_update_two_tokens_no_wsol(
                "MintTokenLegA",
                "MintTokenLegB",
                "poolNonSol",
                1_000_000_000,
                1_000_000_000,
                500,
                1,
            ),
        );
        assert!(!ctx.test_has_pool_reserve_hint("MintTokenLegA", "poolNonSol"));
        assert!(!ctx.test_has_pool_reserve_hint("MintTokenLegB", "poolNonSol"));
    }

    /// Non-SOL/WSOL pair must not later move an opened position mark (I-14 / wrong quote leg).
    #[tokio::test]
    async fn sticky_pool_reserve_hint_non_wsol_pair_does_not_move_later_position_mark() {
        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);
        let mint = "MintLaterPos";
        ctx.record_mint_info(
            mint,
            MintInfo {
                token_program: String::new(),
                decimals: 6,
                supply: 0,
                mint_authority: None,
                freeze_authority: None,
                last_updated: Instant::now(),
            },
        );
        ctx.merge_latest_pool_reserve_price_hint_from_update(
            &pool_cache_update_two_tokens_no_wsol(
                mint,
                "OtherMintNoWsol",
                "poolNonSol",
                1,
                1,
                900,
                1,
            ),
        );
        assert!(!ctx.test_has_pool_reserve_hint(mint, "poolNonSol"));
        ctx.open_position(OpenPositionParams {
            mint,
            pool: "poolNonSol",
            dex: "raydium",
            entry_price: 77.0,
            token_decimals: 6,
            token_amount: 1,
            sol_invested: 1,
            token_program: None,
            creator: None,
            entry_confirmed_slot: 800,
            initial_bonding: None,
        });
        let pos = ctx.positions.read().get(mint).unwrap().clone();
        assert!(
            (pos.current_price - 77.0).abs() < 0.001,
            "non-WSOL PoolCache pair must not seed sticky hint; mark stayed entry, got {}",
            pos.current_price
        );
    }

    /// Scope B: older-slot `PoolCacheUpdate` must not replace a newer sticky reserve hint.
    #[tokio::test]
    async fn sticky_pool_reserve_hint_rejects_stale_slot_merge_by_apply() {
        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);
        let mint = "MintStickyPool";
        ctx.record_mint_info(
            mint,
            MintInfo {
                token_program: String::new(),
                decimals: 6,
                supply: 0,
                mint_authority: None,
                freeze_authority: None,
                last_updated: Instant::now(),
            },
        );
        ctx.merge_latest_pool_reserve_price_hint_from_update(&pool_cache_update_stub(
            mint,
            "poolA",
            2_000_000,
            1_000_000_000,
            300,
            1,
        ));
        ctx.merge_latest_pool_reserve_price_hint_from_update(&pool_cache_update_stub(
            mint,
            "poolA",
            99_000_000,
            1_000_000_000,
            100,
            2,
        ));
        ctx.open_position(OpenPositionParams {
            mint,
            pool: "poolA",
            dex: "raydium",
            entry_price: 50.0,
            token_decimals: 6,
            token_amount: 1_000_000,
            sol_invested: 1_000_000_000,
            token_program: None,
            creator: None,
            entry_confirmed_slot: 200,
            initial_bonding: None,
        });
        let pos = ctx.positions.read().get(mint).unwrap().clone();
        assert!(
            (pos.current_price - 2.0).abs() < 0.05,
            "expected newer-slot sticky tps ~2.0, got {}",
            pos.current_price
        );
        assert_eq!(pos.last_price_slot, 300);
    }

    /// Scope B: reserve hint merged before BUY open applies on `open_position` when slot ordering allows.
    #[tokio::test]
    async fn sticky_pool_reserve_hint_applies_on_open_after_confirm_slot() {
        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);
        let mint = "MintOpenApply";
        ctx.record_mint_info(
            mint,
            MintInfo {
                token_program: String::new(),
                decimals: 6,
                supply: 0,
                mint_authority: None,
                freeze_authority: None,
                last_updated: Instant::now(),
            },
        );
        ctx.merge_latest_pool_reserve_price_hint_from_update(&pool_cache_update_stub(
            mint,
            "poolA",
            1_000_000,
            1_000_000_000,
            600,
            3,
        ));
        ctx.open_position(OpenPositionParams {
            mint,
            pool: "poolA",
            dex: "raydium",
            entry_price: 50.0,
            token_decimals: 6,
            token_amount: 1_000_000,
            sol_invested: 1_000_000_000,
            token_program: None,
            creator: None,
            entry_confirmed_slot: 500,
            initial_bonding: None,
        });
        let pos = ctx.positions.read().get(mint).unwrap().clone();
        assert!(
            (pos.current_price - 1.0).abs() < 0.02,
            "expected sticky pool mark near 1.0 tps, got {}",
            pos.current_price
        );
        assert_eq!(pos.last_price_slot, 600);
    }

    /// Scope B: sticky hint for a different pool must not move the position mark (I-13).
    #[tokio::test]
    async fn sticky_pool_reserve_hint_wrong_pool_not_applied() {
        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);
        let mint = "MintOtherPool";
        ctx.record_mint_info(
            mint,
            MintInfo {
                token_program: String::new(),
                decimals: 6,
                supply: 0,
                mint_authority: None,
                freeze_authority: None,
                last_updated: Instant::now(),
            },
        );
        ctx.merge_latest_pool_reserve_price_hint_from_update(&pool_cache_update_stub(
            mint,
            "poolB",
            1_000_000,
            1_000_000_000,
            600,
            1,
        ));
        ctx.open_position(OpenPositionParams {
            mint,
            pool: "poolA",
            dex: "raydium",
            entry_price: 44.0,
            token_decimals: 6,
            token_amount: 1_000_000,
            sol_invested: 1_000_000_000,
            token_program: None,
            creator: None,
            entry_confirmed_slot: 400,
            initial_bonding: None,
        });
        let pos = ctx.positions.read().get(mint).unwrap().clone();
        assert!(
            (pos.current_price - 44.0).abs() < 0.001,
            "wrong-pool hint must not move mark, got {}",
            pos.current_price
        );
    }

    /// Scope B: `TokenMintInfo` cached in `mint_infos` upgrades an open position via apply.
    #[tokio::test]
    async fn sticky_mint_info_token_program_applied_when_mint_info_arrives() {
        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);
        let mint = "MintTP";
        ctx.open_position(OpenPositionParams {
            mint,
            pool: "p1",
            dex: "raydium",
            entry_price: 1.0,
            token_decimals: 6,
            token_amount: 1,
            sol_invested: 1,
            token_program: None,
            creator: None,
            entry_confirmed_slot: 0,
            initial_bonding: None,
        });
        ctx.record_mint_info(
            mint,
            MintInfo {
                token_program: "TokenzQdBNbLqP7VEhdkAS6EPFLC1PHnBqCXEpPxuEb".to_string(),
                decimals: 6,
                supply: 1,
                mint_authority: None,
                freeze_authority: None,
                last_updated: Instant::now(),
            },
        );
        let mut pos = ctx.positions.read().get(mint).unwrap().clone();
        ctx.apply_latest_sticky_state_to_position(mint, &mut pos);
        assert_eq!(
            pos.token_program.as_deref(),
            Some("TokenzQdBNbLqP7VEhdkAS6EPFLC1PHnBqCXEpPxuEb")
        );
    }

    #[test]
    fn sticky_pumpfun_migration_complete_surfaces_in_evidence_probe() {
        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);
        let mint = "NotAValidPubkeyString";
        ctx.merge_pumpfun_migration_complete_evidence(mint, 10, 20);
        assert!(ctx.live_cache_pumpfun_complete_evidence(mint));
    }

    /// Scope 1: Pre-entry/batched trades must not move `current_price`; slots must be monotonic.
    #[tokio::test]
    async fn scope1_slot_gate_position_price_updates() {
        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);

        ctx.open_position(OpenPositionParams {
            mint: "m1",
            pool: "poolA",
            dex: "raydium",
            entry_price: 10.0,
            token_decimals: 6,
            token_amount: 1_000_000,
            sol_invested: 1_000_000_000,
            token_program: None,
            creator: None,
            entry_confirmed_slot: 500,
            initial_bonding: None,
        });

        ctx.update_position_price("m1", 9.0, None, Some("poolA"), Some(400));
        assert_eq!(ctx.positions.read().get("m1").unwrap().current_price, 10.0);

        ctx.update_position_price("m1", 8.0, None, Some("poolA"), Some(501));
        assert_eq!(ctx.positions.read().get("m1").unwrap().current_price, 8.0);
        assert_eq!(ctx.positions.read().get("m1").unwrap().last_price_slot, 501);
        assert_eq!(
            ctx.positions.read().get("m1").unwrap().highest_price,
            10.0,
            "pool-cache mark must not advance trade-session high"
        );

        ctx.update_position_price("m1", 7.0, None, Some("poolA"), Some(501));
        assert_eq!(ctx.positions.read().get("m1").unwrap().current_price, 8.0);

        ctx.update_position_price("m1", 6.5, None, Some("poolA"), Some(502));
        assert_eq!(ctx.positions.read().get("m1").unwrap().current_price, 6.5);
        assert_eq!(ctx.positions.read().get("m1").unwrap().highest_price, 10.0);
    }

    /// Scale-in must blend fill `tokens_per_sol` for the new leg, not `current_price` (mark).
    #[test]
    fn scale_in_entry_price_blends_fill_tps_not_mark_price() {
        let mut pos = PositionTracker::new("mint", "pool", "dex", 100.0, 6, 1_000_000, 500_000_000);
        pos.current_price = 10.0;
        pos.add_investment(500_000_000, 300.0, 0);
        assert!(
            (pos.entry_price - 200.0).abs() < 1e-6,
            "expected weighted entry ~200 tps, got {}",
            pos.entry_price
        );
        assert_eq!(
            pos.highest_price, 300.0,
            "scale-in fill overwrites trailing session baseline to leg tps"
        );
        assert!((pos.trade_mark_tps - 300.0).abs() < 1e-9);
        assert!(!pos.trailing_active);
    }

    /// Scale-in resets session high / trade mark / trailing latch to the fill (not probe phase).
    #[test]
    fn scale_in_resets_trailing_baseline_and_deactivates_trailing() {
        let mut pos = PositionTracker::new("mint", "pool", "dex", 100.0, 6, 1_000_000, 500_000_000);
        pos.highest_price = 100.0;
        pos.trailing_active = true;
        pos.trade_mark_tps = 100.0;
        pos.current_price = 100.0;
        pos.recent_trades.push(TradeEvent {
            timestamp: Instant::now(),
            slot: 1,
            trader: "x".to_string(),
            is_buy: true,
            sol_amount: 1,
            token_amount: 1,
            signature: "s".to_string(),
        });
        pos.last_price_slot = 50_000;
        pos.add_investment(500_000_000, 90.0, 9_000);
        assert!((pos.highest_price - 90.0).abs() < 1e-9);
        assert!(!pos.trailing_active);
        assert_eq!(pos.entry_confirmed_slot, 9_000);
        assert_eq!(
            pos.last_price_slot, 0,
            "scale-in resets slot gate for trade-mark updates"
        );
        assert!((pos.trade_mark_tps - 90.0).abs() < 1e-9);
        assert!((pos.current_price - 90.0).abs() < 1e-9);
        assert!(pos.recent_trades.is_empty());
    }

    #[test]
    fn buyer_quality_stats_top1_top3_and_repeat_ratio() {
        let mut cfg = MomentumConfig::default();
        cfg.buyer_window_secs = 20;

        let now = Instant::now();
        let mut tracker = TokenTracker::new("mint", "pool", "dex", 1, 0);

        tracker.trades = vec![
            // Wallet A: 3 buys totaling 100
            TradeEvent {
                timestamp: now - Duration::from_secs(1),
                slot: 1000,
                trader: "A".to_string(),
                is_buy: true,
                sol_amount: 50,
                token_amount: 1,
                signature: "s1".to_string(),
            },
            TradeEvent {
                timestamp: now - Duration::from_secs(2),
                slot: 1001,
                trader: "A".to_string(),
                is_buy: true,
                sol_amount: 30,
                token_amount: 1,
                signature: "s2".to_string(),
            },
            TradeEvent {
                timestamp: now - Duration::from_secs(3),
                slot: 1002,
                trader: "A".to_string(),
                is_buy: true,
                sol_amount: 20,
                token_amount: 1,
                signature: "s3".to_string(),
            },
            // Wallet B: 1 buy 50
            TradeEvent {
                timestamp: now - Duration::from_secs(4),
                slot: 1003,
                trader: "B".to_string(),
                is_buy: true,
                sol_amount: 50,
                token_amount: 1,
                signature: "s4".to_string(),
            },
            // Wallet C: 1 buy 50
            TradeEvent {
                timestamp: now - Duration::from_secs(5),
                slot: 1004,
                trader: "C".to_string(),
                is_buy: true,
                sol_amount: 50,
                token_amount: 1,
                signature: "s5".to_string(),
            },
            // Wallet D: 1 sell (ignored)
            TradeEvent {
                timestamp: now - Duration::from_secs(6),
                slot: 1005,
                trader: "D".to_string(),
                is_buy: false,
                sol_amount: 999,
                token_amount: 1,
                signature: "s6".to_string(),
            },
        ];

        let bq = tracker.buyer_quality_stats_at(&cfg, 1050);
        assert_eq!(bq.unique_buyers, 3);
        assert_eq!(bq.total_buy_volume_lamports, 200);

        // A=100, B=50, C=50 => top1=50%, top3=100%
        assert!((bq.top1_share - 0.5).abs() < 1e-9);
        assert!((bq.top3_share - 1.0).abs() < 1e-9);

        // Repeat buyers: A only => 1/3
        assert!((bq.repeat_buyer_ratio - (1.0 / 3.0)).abs() < 1e-9);
    }

    /// Many trades ingested with the same wall clock but **distant Geyser slots** must not look like
    /// one dense activity burst in short buyer/inflow windows (NATS replay safety).
    #[test]
    fn chain_slot_windows_ignore_ingest_burst_wallclock_overlap() {
        let mut cfg = MomentumConfig::default();
        cfg.buyer_window_secs = 10;
        cfg.early_min_liquidity_sol = 0.0;
        cfg.min_unique_buyers = 3;
        cfg.min_trades_per_min = 0.0;
        cfg.min_buy_dominance = 0.0;
        cfg.min_sol_inflow_lamports = 0;
        cfg.require_mint_authority_renounced = false;
        cfg.require_freeze_authority_none = false;
        cfg.top1_buyer_share_cap = 1.0;
        cfg.top3_buyer_share_cap = 1.0;
        cfg.repeat_buyer_min_ratio = 0.0;
        cfg.min_trade_size_lamports = 0;
        cfg.small_buy_ratio_cap = 1.0;
        cfg.dump_recovery_window_secs = 0;

        let now = Instant::now();
        let mut tracker = TokenTracker::new("m", "p", "d", 1, 0);
        for (i, sl) in [0u64, 10_000, 20_000, 30_000, 40_000]
            .into_iter()
            .enumerate()
        {
            tracker.trades.push(TradeEvent {
                timestamp: now,
                slot: sl,
                trader: format!("u{i}"),
                is_buy: true,
                sol_amount: 1_000_000,
                token_amount: 1,
                signature: format!("s{i}"),
            });
            tracker.buy_count = tracker.buy_count.saturating_add(1);
            tracker.total_buy_volume = tracker.total_buy_volume.saturating_add(1_000_000);
        }

        let head = 40_005u64;
        let metrics = tracker.calculate_metrics(&cfg, head);
        // ~10s window ≈ 25 slots @ 400ms/slot — only the trade near `head` counts.
        assert_eq!(metrics.unique_buyers_in_window, 1);
    }

    #[test]
    fn pr170_tracker_records_five_distinct_buyers_in_window() {
        let mut cfg = MomentumConfig::default();
        cfg.buyer_window_secs = 120;
        cfg.early_min_liquidity_sol = 0.0;
        cfg.min_unique_buyers = 0;
        cfg.min_trades_per_min = 0.0;
        cfg.min_buy_dominance = 0.0;
        cfg.min_sol_inflow_lamports = 0;
        cfg.require_mint_authority_renounced = false;
        cfg.require_freeze_authority_none = false;
        cfg.top1_buyer_share_cap = 1.0;
        cfg.top3_buyer_share_cap = 1.0;
        cfg.repeat_buyer_min_ratio = 0.0;
        cfg.min_trade_size_lamports = 0;
        cfg.small_buy_ratio_cap = 1.0;
        cfg.dump_recovery_window_secs = 0;

        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);
        *ctx.config.write() = cfg.clone();

        let mint = "MintPr170Buyers111111111111111111111111111";
        let pool = "poolPr170Buyers11111111111111111111111111";
        assert!(ctx.get_or_create_tracker(mint, pool, "pumpfun", 100, 30_000_000_000));

        let head = 200u64;
        ctx.last_event_slot.store(head, Ordering::Relaxed);
        for i in 0..5 {
            assert!(ctx.record_trade(
                mint,
                pool,
                &format!("buyer{i}"),
                true,
                50_000_000,
                1_000_000,
                &format!("sig{i}"),
                head.saturating_sub(i as u64),
            ));
        }

        let metrics = ctx
            .token_trackers
            .read()
            .get(&MomentumContext::tracker_storage_key(mint, pool))
            .expect("tracker")
            .calculate_metrics(&cfg, head);
        assert_eq!(metrics.unique_buyers_in_window, 5);
    }

    #[test]
    fn pr170_trade_based_discovery_records_first_buy() {
        let cfg = MomentumConfig::default();

        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);
        *ctx.config.write() = cfg;

        let mint = "MintPr170Disc1111111111111111111111111111";
        let pool = "poolPr170Disc111111111111111111111111111";
        let evt = MarketEvent {
            header: RecordHeader::new("test", BUILD_VERSION, "run"),
            event_id: "ev-pr170-disc".into(),
            source: "geyser".into(),
            slot: Some(50),
            kind: MarketEventKind::Trade {
                pool_address: pool.to_string(),
                mint: mint.to_string(),
                quote_mint: WSOL_MINT.to_string(),
                trader: "buyer0".to_string(),
                is_buy: true,
                sol_amount: 10_000_000,
                token_amount: 1,
                token_decimals: 6,
                signature: Some("sig0".into()),
                dex: "pumpfun".into(),
                creator: None,
                token_program: None,
            },
        };

        let rt = tokio::runtime::Runtime::new().expect("runtime");
        rt.block_on(async {
            process_market_event(&ctx, &evt, true)
                .await
                .expect("trade discovery");
        });

        let tk = MomentumContext::tracker_storage_key(mint, pool);
        let trackers = ctx.token_trackers.read();
        let tr = trackers.get(&tk).expect("tracker row");
        assert_eq!(tr.trades.len(), 1);
    }

    #[test]
    fn pr170_reject_logs_terminal_state_and_reason() {
        let mut cfg = MomentumConfig::default();
        cfg.max_single_dump_lamports = 1;

        let mut tracker = TokenTracker::new("m", "p", "pumpfun", 1, 30_000_000_000);
        tracker.record_trade("dumper", false, 5_000_000_000, 1, "sig", 10, &cfg);
        assert!(tracker.is_rejected());
        assert!(tracker
            .blacklist_reason
            .as_deref()
            .unwrap_or("")
            .contains("Large dump"));
    }

    #[test]
    fn pr170_bot_concentration_waits_with_few_buyers_not_terminal_reject() {
        let mut cfg = MomentumConfig::default();
        cfg.early_min_liquidity_sol = 0.0;
        cfg.min_unique_buyers = 3;
        cfg.min_trades_per_min = 0.0;
        cfg.min_buy_dominance = 0.0;
        cfg.min_sol_inflow_lamports = 0;
        cfg.require_mint_authority_renounced = false;
        cfg.require_freeze_authority_none = false;
        cfg.top1_buyer_share_cap = 0.30;
        cfg.top3_buyer_share_cap = 1.0;
        cfg.repeat_buyer_min_ratio = 0.0;
        cfg.min_trade_size_lamports = 0;
        cfg.small_buy_ratio_cap = 1.0;
        cfg.dump_recovery_window_secs = 0;
        cfg.min_token_age_secs = 0;

        let mut tracker = TokenTracker::new("m", "p", "pumpfun", 100, 30_000_000_000);
        tracker.record_trade("whale", true, 900_000_000, 1, "s0", 150, &cfg);
        tracker.record_trade("retail", true, 50_000_000, 1, "s1", 151, &cfg);

        let (ok, reason) = tracker.should_generate_intent(&cfg, None, 200, Instant::now());
        assert!(!ok);
        assert!(!tracker.is_rejected(), "must not false-reject: {reason}");
        assert!(reason.contains("WAIT_BUYER_WINDOW"), "{reason}");
    }

    #[test]
    fn pr170_xhat_style_dev_sell_pump_accumulates_ten_buyers() {
        let mut cfg = MomentumConfig::default();
        cfg.early_min_liquidity_sol = 0.0;
        cfg.min_unique_buyers = 3;
        cfg.min_trades_per_min = 0.0;
        cfg.min_buy_dominance = 0.0;
        cfg.min_sol_inflow_lamports = 0;
        cfg.require_mint_authority_renounced = false;
        cfg.require_freeze_authority_none = false;
        cfg.top1_buyer_share_cap = 0.95;
        cfg.top3_buyer_share_cap = 1.0;
        cfg.repeat_buyer_min_ratio = 0.0;
        cfg.min_trade_size_lamports = 0;
        cfg.small_buy_ratio_cap = 1.0;
        cfg.dump_recovery_window_secs = 0;
        cfg.dev_sell_revalidation_delay_secs = 0;
        cfg.min_token_age_secs = 0;

        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);
        *ctx.config.write() = cfg.clone();

        let mint = "MintPr170Xhat1111111111111111111111111111";
        let pool = "poolPr170Xhat111111111111111111111111111";
        let dev = "DevWalletPr170Xhat1111111111111111111111";
        let head = 140u64;
        ctx.last_event_slot.store(head, Ordering::Relaxed);

        assert!(ctx.get_or_create_tracker(mint, pool, "pumpfun", 100, 30_000_000_000));
        {
            let mut trackers = ctx.token_trackers.write();
            let tr = trackers
                .get_mut(&MomentumContext::tracker_storage_key(mint, pool))
                .expect("tracker");
            tr.set_dev_info(dev, 1.0, &cfg);
        }

        let rt = tokio::runtime::Runtime::new().expect("runtime");
        rt.block_on(async {
            let dev_sell = MarketEvent {
                header: RecordHeader::new("test", BUILD_VERSION, "run"),
                event_id: "ev-dev-sell".into(),
                source: "geyser".into(),
                slot: Some(110),
                kind: MarketEventKind::Trade {
                    pool_address: pool.to_string(),
                    mint: mint.to_string(),
                    quote_mint: WSOL_MINT.to_string(),
                    trader: dev.to_string(),
                    is_buy: false,
                    sol_amount: 100_000_000,
                    token_amount: 1,
                    token_decimals: 6,
                    signature: Some("sig-dev".into()),
                    dex: "pumpfun".into(),
                    creator: Some(dev.to_string()),
                    token_program: None,
                },
            };
            process_market_event(&ctx, &dev_sell, true)
                .await
                .expect("dev sell");

            for i in 0..10 {
                let buy = MarketEvent {
                    header: RecordHeader::new("test", BUILD_VERSION, "run"),
                    event_id: format!("ev-buy-{i}"),
                    source: "geyser".into(),
                    slot: Some(120 + i),
                    kind: MarketEventKind::Trade {
                        pool_address: pool.to_string(),
                        mint: mint.to_string(),
                        quote_mint: WSOL_MINT.to_string(),
                        trader: format!("pumpBuyer{i}"),
                        is_buy: true,
                        sol_amount: 20_000_000,
                        token_amount: 1,
                        token_decimals: 6,
                        signature: Some(format!("sig-buy-{i}")),
                        dex: "pumpfun".into(),
                        creator: Some(dev.to_string()),
                        token_program: None,
                    },
                };
                process_market_event(&ctx, &buy, true)
                    .await
                    .expect("pump buy");
            }
        });

        let trackers = ctx.token_trackers.read();
        let tr = trackers
            .get(&MomentumContext::tracker_storage_key(mint, pool))
            .expect("tracker");
        assert!(
            !tr.is_rejected(),
            "dev-sell pump must not false-reject on bot concentration: {:?}",
            tr.blacklist_reason
        );
        let metrics = tr.calculate_metrics(&cfg, head);
        let trades_len = tr.trades.len();
        let buyers = metrics.unique_buyers_in_window;
        drop(trackers);
        assert!(
            buyers >= 10,
            "buyers in window {} trades_len {}",
            buyers,
            trades_len
        );
    }

    #[test]
    fn micro_buy_spam_rejects_when_ratio_too_high() {
        let mut cfg = MomentumConfig::default();
        cfg.buyer_window_secs = 60;
        cfg.inflow_window_secs = 60;

        // Disable other gates to isolate micro-buy behavior.
        cfg.early_min_liquidity_sol = 0.0;
        cfg.min_unique_buyers = 0;
        cfg.min_trades_per_min = 0.0;
        cfg.min_buy_dominance = 0.0;
        cfg.min_sol_inflow_lamports = 0;
        cfg.require_mint_authority_renounced = false;
        cfg.require_freeze_authority_none = false;
        cfg.top1_buyer_share_cap = 1.0;
        cfg.top3_buyer_share_cap = 1.0;
        cfg.repeat_buyer_min_ratio = 0.0;

        // Micro-buy gate params
        cfg.min_trade_size_lamports = 100;
        cfg.small_buy_ratio_cap = 0.60;
        cfg.min_token_age_secs = 0;

        let mut tracker = TokenTracker::new("mint", "pool", "dex", 1, 0);

        // 10 buys: 7 are micro (<100), 3 are normal => ratio 0.7 > 0.6
        for i in 0..7 {
            let trader = format!("w{i}");
            let sig = format!("s{i}");
            tracker.record_trade(
                trader.as_str(),
                true,
                50,
                1,
                sig.as_str(),
                10 + i as u64,
                &cfg,
            );
        }
        for i in 7..10 {
            let trader = format!("w{i}");
            let sig = format!("s{i}");
            tracker.record_trade(
                trader.as_str(),
                true,
                200,
                1,
                sig.as_str(),
                10 + i as u64,
                &cfg,
            );
        }

        let (should_trade, reason) = tracker.should_generate_intent(&cfg, None, 25, Instant::now());
        assert!(!should_trade);
        assert!(reason.contains("REJECT_MICRO_BUY_SPAM"));
    }

    #[test]
    fn find_best_sell_pool_skips_migrated_pumpfun() {
        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());

        let ctx = MomentumContext {
            run_id: "test".to_string(),
            config: parking_lot::RwLock::new(MomentumConfig::default()),
            nats: None,
            jsonl_writer,
            intent_publish_tx: parking_lot::Mutex::new(Some(noop_intent_publish_tx())),
            intent_counter: std::sync::atomic::AtomicU64::new(0),
            pool_first_seen: parking_lot::RwLock::new(std::collections::HashMap::new()),
            pending_dev_info: parking_lot::RwLock::new(HashMap::new()),
            pending_pool_accounts: parking_lot::RwLock::new(HashMap::new()),
            mint_infos: parking_lot::RwLock::new(HashMap::new()),
            token_trackers: parking_lot::RwLock::new(HashMap::new()),
            tracker_keys_by_mint: parking_lot::RwLock::new(HashMap::new()),
            dirty_entry_tracker_keys: parking_lot::RwLock::new(BTreeSet::new()),
            pumpfun_buy_missing_creator_suppress_until: parking_lot::RwLock::new(HashMap::new()),
            positions: parking_lot::RwLock::new(HashMap::new()),
            pending_intents: parking_lot::RwLock::new(HashMap::new()),
            latest_bonding_by_mint: parking_lot::RwLock::new(HashMap::new()),
            latest_pumpfun_migration_complete_by_mint: parking_lot::RwLock::new(HashMap::new()),
            latest_pool_reserve_price_hint_by_mint_pool: parking_lot::RwLock::new(HashMap::new()),
            pending_buy_entries: parking_lot::RwLock::new(HashMap::new()),
            pending_buy_mint_index: parking_lot::RwLock::new(HashMap::new()),
            mint_pools: parking_lot::RwLock::new(HashMap::new()),
            live_pool_cache: LivePoolCache::new(),
            open_position_pool_recovery_last_sent: parking_lot::RwLock::new(HashMap::new()),
            position_kv: tokio::sync::OnceCell::new(),
            orphaned_mints: parking_lot::RwLock::new(HashMap::new()),
            orphaned_recovered_intent_ids: parking_lot::RwLock::new(BoundedIntentIdCache::new(
                ORPHANED_RECOVERED_INTENT_IDS_CAP,
            )),
            latest_wallet_balance_raw_by_mint: parking_lot::RwLock::new(HashMap::new()),
            tokens_tracked: std::sync::atomic::AtomicU64::new(0),
            tokens_blacklisted: std::sync::atomic::AtomicU64::new(0),
            blacklisted_mints_for_metric: parking_lot::RwLock::new(HashSet::new()),
            intents_generated: std::sync::atomic::AtomicU64::new(0),
            exits_generated: std::sync::atomic::AtomicU64::new(0),
            last_event_slot: std::sync::atomic::AtomicU64::new(0),
            last_event_ts_ms: std::sync::atomic::AtomicU64::new(0),
            momentum_active_pool_publish_queue: parking_lot::Mutex::new(
                MomentumActivePoolPublishQueue::default(),
            ),
        };

        let mint = "So11111111111111111111111111111111111111112";
        let mut migrated = PoolInfo::new("pool_migrated".to_string(), "pumpfun".to_string(), 100);
        migrated.bonding_curve_complete = Some(true);
        migrated.last_trade_ratio = Some(1e-9);
        migrated.dex_pool_accounts = Some(vec!["acc1".to_string()]);

        let mut active = PoolInfo::new("pool_active".to_string(), "pumpfun".to_string(), 100);
        active.last_trade_ratio = Some(2e-9);
        active.dex_pool_accounts = Some(vec!["acc2".to_string()]);

        {
            let mut pools = ctx.mint_pools.write();
            pools.insert(mint.to_string(), vec![migrated, active]);
        }

        let (pool, _, _, _, _) = ctx
            .find_best_sell_pool(mint, 1_000_000, "pool_migrated", "pumpfun")
            .unwrap();
        assert_eq!(pool, "pool_active", "Should select non-migrated pool");
    }

    #[test]
    fn find_best_sell_pool_skips_high_fail_count_in_cooldown() {
        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());

        let ctx = MomentumContext {
            run_id: "test".to_string(),
            config: parking_lot::RwLock::new(MomentumConfig::default()),
            nats: None,
            jsonl_writer,
            intent_publish_tx: parking_lot::Mutex::new(Some(noop_intent_publish_tx())),
            intent_counter: std::sync::atomic::AtomicU64::new(0),
            pool_first_seen: parking_lot::RwLock::new(std::collections::HashMap::new()),
            pending_dev_info: parking_lot::RwLock::new(HashMap::new()),
            pending_pool_accounts: parking_lot::RwLock::new(HashMap::new()),
            mint_infos: parking_lot::RwLock::new(HashMap::new()),
            token_trackers: parking_lot::RwLock::new(HashMap::new()),
            tracker_keys_by_mint: parking_lot::RwLock::new(HashMap::new()),
            dirty_entry_tracker_keys: parking_lot::RwLock::new(BTreeSet::new()),
            pumpfun_buy_missing_creator_suppress_until: parking_lot::RwLock::new(HashMap::new()),
            positions: parking_lot::RwLock::new(HashMap::new()),
            pending_intents: parking_lot::RwLock::new(HashMap::new()),
            latest_bonding_by_mint: parking_lot::RwLock::new(HashMap::new()),
            latest_pumpfun_migration_complete_by_mint: parking_lot::RwLock::new(HashMap::new()),
            latest_pool_reserve_price_hint_by_mint_pool: parking_lot::RwLock::new(HashMap::new()),
            pending_buy_entries: parking_lot::RwLock::new(HashMap::new()),
            pending_buy_mint_index: parking_lot::RwLock::new(HashMap::new()),
            mint_pools: parking_lot::RwLock::new(HashMap::new()),
            live_pool_cache: LivePoolCache::new(),
            open_position_pool_recovery_last_sent: parking_lot::RwLock::new(HashMap::new()),
            position_kv: tokio::sync::OnceCell::new(),
            orphaned_mints: parking_lot::RwLock::new(HashMap::new()),
            orphaned_recovered_intent_ids: parking_lot::RwLock::new(BoundedIntentIdCache::new(
                ORPHANED_RECOVERED_INTENT_IDS_CAP,
            )),
            latest_wallet_balance_raw_by_mint: parking_lot::RwLock::new(HashMap::new()),
            tokens_tracked: std::sync::atomic::AtomicU64::new(0),
            tokens_blacklisted: std::sync::atomic::AtomicU64::new(0),
            blacklisted_mints_for_metric: parking_lot::RwLock::new(HashSet::new()),
            intents_generated: std::sync::atomic::AtomicU64::new(0),
            exits_generated: std::sync::atomic::AtomicU64::new(0),
            last_event_slot: std::sync::atomic::AtomicU64::new(0),
            last_event_ts_ms: std::sync::atomic::AtomicU64::new(0),
            momentum_active_pool_publish_queue: parking_lot::Mutex::new(
                MomentumActivePoolPublishQueue::default(),
            ),
        };

        let mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
        let mut failed = PoolInfo::new("pool_failed".to_string(), "raydium".to_string(), 100);
        failed.sell_fail_count = 3;
        failed.last_sell_fail_at = Some(Instant::now() - Duration::from_secs(10));
        failed.last_trade_ratio = Some(1e-9);
        failed.dex_pool_accounts = Some(vec!["acc1".to_string()]);

        let mut ok_pool = PoolInfo::new("pool_ok".to_string(), "raydium".to_string(), 100);
        ok_pool.last_trade_ratio = Some(1e-9);
        ok_pool.dex_pool_accounts = Some(vec!["acc2".to_string()]);

        {
            let mut pools = ctx.mint_pools.write();
            pools.insert(mint.to_string(), vec![failed, ok_pool]);
        }

        let (pool, _, _, _, _) = ctx
            .find_best_sell_pool(mint, 1_000_000, "pool_failed", "raydium")
            .unwrap();
        assert_eq!(
            pool, "pool_ok",
            "Should prefer pool without failures in cooldown"
        );
    }

    /// Scope 57: reconcile must not pick newest slot if that is PumpSwap while PumpFun BC is active.
    #[test]
    fn select_reconcile_pool_prefers_active_pumpfun_over_newer_pump_amm() {
        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);

        let mint = "y7bgE68ZWVodvVmMUWQhShAnjVTmJVGpdnC1wYspump";
        let mut pf = PoolInfo::new("PF_BC_POOL".to_string(), "pumpfun".to_string(), 100);
        pf.last_trade_slot = 100;
        pf.last_trade_ratio = Some(1e-9);
        pf.bonding_curve_complete = None;

        let mut amm = PoolInfo::new(
            "HS9UsHpMZLYzzbLwWXJfHzsRd8HmuzMLcutHwVKGt1P7".to_string(),
            "pump_amm".to_string(),
            500,
        );
        amm.last_trade_ratio = Some(5e-9);
        amm.dex_pool_accounts = Some(vec!["x".to_string(), "y".to_string(), "z".to_string()]);

        {
            let mut pools = ctx.mint_pools.write();
            pools.insert(mint.to_string(), vec![pf, amm]);
        }

        let (pool, dex, _) = ctx.select_reconcile_pool(mint).expect("reconcile pool");
        assert_eq!(pool, "PF_BC_POOL");
        assert_eq!(dex, "pumpfun");
    }

    #[test]
    fn select_reconcile_pool_refuses_pumpswap_only_without_migration_evidence() {
        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);

        let mint = "MintOnlyAmm11111111111111111111111111111111";
        let mut amm = PoolInfo::new("AMM_ONLY".to_string(), "pump_amm".to_string(), 500);
        amm.last_trade_ratio = Some(1e-9);
        amm.dex_pool_accounts = Some(vec!["a".to_string(), "b".to_string(), "c".to_string()]);

        {
            let mut pools = ctx.mint_pools.write();
            pools.insert(mint.to_string(), vec![amm]);
        }

        assert!(
            ctx.select_reconcile_pool(mint).is_none(),
            "PumpSwap-only registry without complete evidence must not silently reconcile"
        );
    }

    /// Scope 57: exit routing keeps PumpFun when migration is not evidenced (better PumpSwap quote ignored).
    #[test]
    fn find_best_sell_pool_keeps_original_pumpfun_when_not_complete() {
        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);

        let mint = "y7bgE68ZWVodvVmMUWQhShAnjVTmJVGpdnC1wYspump";
        let mut pf = PoolInfo::new("PF_BC_POOL".to_string(), "pumpfun".to_string(), 100);
        pf.last_trade_slot = 100;
        pf.last_trade_ratio = Some(1e-9);

        let mut amm = PoolInfo::new(
            "HS9UsHpMZLYzzbLwWXJfHzsRd8HmuzMLcutHwVKGt1P7".to_string(),
            "pump_amm".to_string(),
            900,
        );
        amm.last_trade_ratio = Some(50e-9);
        amm.dex_pool_accounts = Some(vec!["a".to_string(), "b".to_string(), "c".to_string()]);

        {
            let mut pools = ctx.mint_pools.write();
            pools.insert(mint.to_string(), vec![pf, amm]);
        }

        let (pool, dex, _, _, _) = ctx
            .find_best_sell_pool(mint, 1_000_000, "PF_BC_POOL", "pumpfun")
            .expect("sell pool");
        assert_eq!(pool, "PF_BC_POOL");
        assert_eq!(dex, "pumpfun");
    }

    /// Scope 57: after bonding curve complete (pool flag), PumpSwap may win multi-pool exit.
    #[test]
    fn find_best_sell_pool_allows_pump_amm_after_pumpfun_complete() {
        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);

        let mint = "MintPumpComplete111111111111111111111111111";
        let mut pf = PoolInfo::new("PF_OLD".to_string(), "pumpfun".to_string(), 50);
        pf.bonding_curve_complete = Some(true);
        pf.last_trade_ratio = Some(1e-9);
        pf.dex_pool_accounts = Some(vec!["p1".to_string()]);

        let mut amm = PoolInfo::new("AMM_NEW".to_string(), "pump_amm".to_string(), 500);
        amm.last_trade_ratio = Some(3e-9);
        amm.dex_pool_accounts = Some(vec!["a".to_string(), "b".to_string(), "c".to_string()]);

        {
            let mut pools = ctx.mint_pools.write();
            pools.insert(mint.to_string(), vec![pf, amm]);
        }

        let (pool, dex, _, _, _) = ctx
            .find_best_sell_pool(mint, 1_000_000, "PF_OLD", "pumpfun")
            .expect("sell pool");
        assert_eq!(pool, "AMM_NEW");
        assert_eq!(dex, "pump_amm");
    }

    /// Regression: y7-class mint — newer HS9 PumpSwap must not beat PumpFun BC without complete evidence.
    #[test]
    fn scope57_y7_class_exit_does_not_use_pump_amm_without_complete_evidence() {
        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);

        let mint = "y7bgE68ZWVodvVmMUWQhShAnjVTmJVGpdnC1wYspump";
        let mut pf = PoolInfo::new("PF_BC_Y7".to_string(), "pumpfun".to_string(), 200);
        pf.last_trade_ratio = Some(2e-9);
        pf.bonding_curve_complete = Some(false);

        let mut amm = PoolInfo::new(
            "HS9UsHpMZLYzzbLwWXJfHzsRd8HmuzMLcutHwVKGt1P7".to_string(),
            "pump_amm".to_string(),
            9_000_000,
        );
        amm.last_trade_ratio = Some(100e-9);
        amm.dex_pool_accounts = Some(vec!["p".to_string(); 14]);

        {
            let mut pools = ctx.mint_pools.write();
            pools.insert(mint.to_string(), vec![pf, amm]);
        }

        let (pool, dex, _, _, _) = ctx
            .find_best_sell_pool(mint, 16_650_263_074, "PF_BC_Y7", "pumpfun")
            .expect("sell pool");
        assert_ne!(dex, "pump_amm");
        assert_eq!(dex, "pumpfun");
        assert_eq!(pool, "PF_BC_Y7");
    }

    #[test]
    fn probe_then_scale_signal_flow() {
        use solana_sdk::pubkey::Pubkey;

        let mut cfg = MomentumConfig::default();
        cfg.default_position_lamports = 1_000;
        cfg.probe_buy_pct = 0.25;
        cfg.scale_in_confirm_window_secs = 30;
        cfg.scale_in_min_probe_executable_pnl_pct = 0.0;
        cfg.early_min_liquidity_sol = 0.0;
        cfg.min_unique_buyers = 0;
        cfg.min_trades_per_min = 0.0;
        cfg.min_buy_dominance = 0.0;
        cfg.min_sol_inflow_lamports = 0;
        cfg.require_mint_authority_renounced = false;
        cfg.require_freeze_authority_none = false;
        cfg.top1_buyer_share_cap = 1.0;
        cfg.top3_buyer_share_cap = 1.0;
        cfg.repeat_buyer_min_ratio = 0.0;
        cfg.min_trade_size_lamports = 0;
        cfg.small_buy_ratio_cap = 1.0;
        cfg.min_token_age_secs = 0;

        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);
        *ctx.config.write() = cfg;

        let mint = Pubkey::new_from_array([0xA1u8; 32]).to_string();
        let pool = Pubkey::new_from_array([0xB2u8; 32]).to_string();
        let sk = MomentumContext::tracker_storage_key(&mint, &pool);

        ctx.record_mint_info(
            &mint,
            MintInfo {
                token_program: String::new(),
                decimals: 6,
                supply: 0,
                mint_authority: None,
                freeze_authority: None,
                last_updated: Instant::now(),
            },
        );

        {
            let mut trackers = ctx.token_trackers.write();
            trackers.insert(sk.clone(), TokenTracker::new(&mint, &pool, "raydium", 1, 0));
        }
        {
            let mut pi = PoolInfo::new(pool.clone(), "raydium".to_string(), 100);
            pi.dex_pool_accounts = Some(vec![]);
            ctx.mint_pools.write().insert(mint.clone(), vec![pi]);
        }

        let signals = ctx.check_for_signals();
        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].mint, mint);
        assert_eq!(signals[0].kind, EntryKind::Probe);
        assert_eq!(signals[0].sol_amount, 250);

        {
            let mut trackers = ctx.token_trackers.write();
            let t = trackers.get_mut(&sk).unwrap();
            assert!(matches!(t.state, TrackerState::ProbeBuyPending { .. }));
            t.state = TrackerState::PositionOpenProbe {
                filled_at: Instant::now(),
            };
        }

        {
            let mut pos_map = ctx.positions.write();
            pos_map.insert(
                mint.clone(),
                PositionTracker::new(
                    &mint,
                    &pool,
                    "raydium",
                    5_727_593.0,
                    6,
                    7_159_492_133,
                    250_000_000,
                ),
            );
        }

        let upd = pool_cache_update_stub(
            &mint,
            &pool,
            10_000_000_000_000u64,
            1_000_000_000_000u64,
            900_000_000u64,
            1,
        );
        assert!(
            pool_cache_sync::apply_pool_cache_update(&ctx.live_pool_cache, &upd),
            "pool cache apply failed"
        );

        let signals = ctx.check_for_signals();
        assert_eq!(
            signals.len(),
            1,
            "expected scale-in when executable quote shows probe-side gain"
        );
        assert_eq!(signals[0].kind, EntryKind::ScaleIn);
        assert_eq!(signals[0].sol_amount, 750);

        let trackers = ctx.token_trackers.read();
        let t = trackers.get(&sk).unwrap();
        assert!(matches!(t.state, TrackerState::ScaleInPending { .. }));
    }

    #[test]
    fn scale_in_blocked_when_executable_probe_pnl_not_positive() {
        use solana_sdk::pubkey::Pubkey;

        let mut cfg = MomentumConfig::default();
        cfg.default_position_lamports = 1_000;
        cfg.probe_buy_pct = 0.25;
        cfg.early_min_liquidity_sol = 0.0;
        cfg.min_unique_buyers = 0;
        cfg.min_trades_per_min = 0.0;
        cfg.min_buy_dominance = 0.0;
        cfg.min_sol_inflow_lamports = 0;
        cfg.require_mint_authority_renounced = false;
        cfg.require_freeze_authority_none = false;
        cfg.top1_buyer_share_cap = 1.0;
        cfg.top3_buyer_share_cap = 1.0;
        cfg.repeat_buyer_min_ratio = 0.0;
        cfg.min_trade_size_lamports = 0;
        cfg.small_buy_ratio_cap = 1.0;
        cfg.min_token_age_secs = 0;

        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);
        *ctx.config.write() = cfg;

        let mint = Pubkey::new_from_array([0xC3u8; 32]).to_string();
        let pool = Pubkey::new_from_array([0xD4u8; 32]).to_string();
        let sk = MomentumContext::tracker_storage_key(&mint, &pool);

        ctx.record_mint_info(
            &mint,
            MintInfo {
                token_program: String::new(),
                decimals: 6,
                supply: 0,
                mint_authority: None,
                freeze_authority: None,
                last_updated: Instant::now(),
            },
        );

        ctx.token_trackers
            .write()
            .insert(sk.clone(), TokenTracker::new(&mint, &pool, "raydium", 1, 0));
        {
            let mut pi = PoolInfo::new(pool.clone(), "raydium".to_string(), 100);
            pi.dex_pool_accounts = Some(vec![]);
            ctx.mint_pools.write().insert(mint.clone(), vec![pi]);
        }

        assert!(ctx.check_for_signals().len() == 1);

        {
            let mut trackers = ctx.token_trackers.write();
            trackers.get_mut(&sk).unwrap().state = TrackerState::PositionOpenProbe {
                filled_at: Instant::now(),
            };
        }

        {
            let mut pos_map = ctx.positions.write();
            pos_map.insert(
                mint.clone(),
                PositionTracker::new(
                    &mint,
                    &pool,
                    "raydium",
                    5_727_593.0,
                    6,
                    7_159_492_133,
                    250_000_000,
                ),
            );
        }

        // Illiquid side: selling the probe size yields very little SOL → high `tokens_per_sol` vs entry → underwater on I-14.
        let upd = pool_cache_update_stub(
            &mint,
            &pool,
            100_000_000_000_000u64,
            50_000u64,
            900_000_001u64,
            1,
        );
        assert!(pool_cache_sync::apply_pool_cache_update(
            &ctx.live_pool_cache,
            &upd
        ));

        ctx.dirty_entry_tracker_keys.write().clear();
        assert!(
            ctx.check_for_signals().is_empty(),
            "scale-in must not fire when executable probe PnL is not strictly positive"
        );
        assert!(
            ctx.dirty_entry_tracker_keys.read().contains(&sk),
            "WAIT path must re-queue entry eval via dirty key"
        );
        let trackers = ctx.token_trackers.read();
        assert!(matches!(
            trackers.get(&sk).unwrap().state,
            TrackerState::PositionOpenProbe { .. }
        ));
    }

    #[test]
    fn scale_in_blocked_without_live_pool_quote_marks_dirty() {
        use solana_sdk::pubkey::Pubkey;

        let mut cfg = MomentumConfig::default();
        cfg.default_position_lamports = 1_000;
        cfg.probe_buy_pct = 0.25;
        cfg.early_min_liquidity_sol = 0.0;
        cfg.min_unique_buyers = 0;
        cfg.min_trades_per_min = 0.0;
        cfg.min_buy_dominance = 0.0;
        cfg.min_sol_inflow_lamports = 0;
        cfg.require_mint_authority_renounced = false;
        cfg.require_freeze_authority_none = false;
        cfg.top1_buyer_share_cap = 1.0;
        cfg.top3_buyer_share_cap = 1.0;
        cfg.repeat_buyer_min_ratio = 0.0;
        cfg.min_trade_size_lamports = 0;
        cfg.small_buy_ratio_cap = 1.0;
        cfg.min_token_age_secs = 0;

        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);
        *ctx.config.write() = cfg;

        let mint = Pubkey::new_from_array([0xE5u8; 32]).to_string();
        let pool = Pubkey::new_from_array([0xF6u8; 32]).to_string();
        let sk = MomentumContext::tracker_storage_key(&mint, &pool);

        ctx.record_mint_info(
            &mint,
            MintInfo {
                token_program: String::new(),
                decimals: 6,
                supply: 0,
                mint_authority: None,
                freeze_authority: None,
                last_updated: Instant::now(),
            },
        );

        ctx.token_trackers
            .write()
            .insert(sk.clone(), TokenTracker::new(&mint, &pool, "raydium", 1, 0));
        {
            let mut pi = PoolInfo::new(pool.clone(), "raydium".to_string(), 100);
            pi.dex_pool_accounts = Some(vec![]);
            ctx.mint_pools.write().insert(mint.clone(), vec![pi]);
        }

        assert_eq!(ctx.check_for_signals().len(), 1);
        {
            let mut trackers = ctx.token_trackers.write();
            trackers.get_mut(&sk).unwrap().state = TrackerState::PositionOpenProbe {
                filled_at: Instant::now(),
            };
        }
        {
            let mut pos_map = ctx.positions.write();
            pos_map.insert(
                mint.clone(),
                PositionTracker::new(
                    &mint,
                    &pool,
                    "raydium",
                    5_727_593.0,
                    6,
                    7_159_492_133,
                    250_000_000,
                ),
            );
        }

        ctx.dirty_entry_tracker_keys.write().clear();
        assert!(ctx.check_for_signals().is_empty());
        assert!(ctx.dirty_entry_tracker_keys.read().contains(&sk));
    }

    #[test]
    fn scale_in_blocked_when_min_executable_pnl_threshold_not_met() {
        use solana_sdk::pubkey::Pubkey;

        let mut cfg = MomentumConfig::default();
        cfg.default_position_lamports = 1_000;
        cfg.probe_buy_pct = 0.25;
        cfg.scale_in_min_probe_executable_pnl_pct = 1.0e12;
        cfg.early_min_liquidity_sol = 0.0;
        cfg.min_unique_buyers = 0;
        cfg.min_trades_per_min = 0.0;
        cfg.min_buy_dominance = 0.0;
        cfg.min_sol_inflow_lamports = 0;
        cfg.require_mint_authority_renounced = false;
        cfg.require_freeze_authority_none = false;
        cfg.top1_buyer_share_cap = 1.0;
        cfg.top3_buyer_share_cap = 1.0;
        cfg.repeat_buyer_min_ratio = 0.0;
        cfg.min_trade_size_lamports = 0;
        cfg.small_buy_ratio_cap = 1.0;
        cfg.min_token_age_secs = 0;

        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);
        *ctx.config.write() = cfg;

        let mint = Pubkey::new_from_array([0x11u8; 32]).to_string();
        let pool = Pubkey::new_from_array([0x22u8; 32]).to_string();
        let sk = MomentumContext::tracker_storage_key(&mint, &pool);

        ctx.record_mint_info(
            &mint,
            MintInfo {
                token_program: String::new(),
                decimals: 6,
                supply: 0,
                mint_authority: None,
                freeze_authority: None,
                last_updated: Instant::now(),
            },
        );

        ctx.token_trackers
            .write()
            .insert(sk.clone(), TokenTracker::new(&mint, &pool, "raydium", 1, 0));
        {
            let mut pi = PoolInfo::new(pool.clone(), "raydium".to_string(), 100);
            pi.dex_pool_accounts = Some(vec![]);
            ctx.mint_pools.write().insert(mint.clone(), vec![pi]);
        }

        assert_eq!(ctx.check_for_signals().len(), 1);
        {
            let mut trackers = ctx.token_trackers.write();
            trackers.get_mut(&sk).unwrap().state = TrackerState::PositionOpenProbe {
                filled_at: Instant::now(),
            };
        }
        {
            let mut pos_map = ctx.positions.write();
            pos_map.insert(
                mint.clone(),
                PositionTracker::new(
                    &mint,
                    &pool,
                    "raydium",
                    5_727_593.0,
                    6,
                    7_159_492_133,
                    250_000_000,
                ),
            );
        }

        let upd = pool_cache_update_stub(
            &mint,
            &pool,
            10_000_000_000_000u64,
            1_000_000_000_000u64,
            900_000_002u64,
            1,
        );
        assert!(pool_cache_sync::apply_pool_cache_update(
            &ctx.live_pool_cache,
            &upd
        ));

        ctx.dirty_entry_tracker_keys.write().clear();
        assert!(ctx.check_for_signals().is_empty());
        assert!(ctx.dirty_entry_tracker_keys.read().contains(&sk));
    }

    /// Pool-scoped trackers: activity on `pump_amm` must not emit a stale `pumpfun` probe for the same mint.
    #[test]
    fn pool_scoped_entry_probe_targets_active_pool_not_legacy_pumpfun_row() {
        let cfg = {
            let mut c = MomentumConfig::default();
            c.default_position_lamports = 1_000;
            c.probe_buy_pct = 0.25;
            c.early_min_liquidity_sol = 0.0;
            c.min_unique_buyers = 0;
            c.min_trades_per_min = 0.0;
            c.min_buy_dominance = 0.0;
            c.min_sol_inflow_lamports = 0;
            c.require_mint_authority_renounced = false;
            c.require_freeze_authority_none = false;
            c.top1_buyer_share_cap = 1.0;
            c.top3_buyer_share_cap = 1.0;
            c.repeat_buyer_min_ratio = 0.0;
            c.min_trade_size_lamports = 0;
            c.small_buy_ratio_cap = 1.0;
            c.min_token_age_secs = 0;
            c
        };

        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);
        *ctx.config.write() = cfg.clone();

        let mint = "MintPoolScoped111111111111111111111111111111";
        let pool_pf = "PumpPfLegacy11111111111111111111111111111111";
        let pool_amm = "pAMMActive111111111111111111111111111111111";

        ctx.token_trackers.write().insert(
            MomentumContext::tracker_storage_key(mint, pool_pf),
            TokenTracker::new(mint, pool_pf, "pumpfun", 1, 30_000_000_000),
        );

        ctx.register_pool(mint, pool_pf, "pumpfun", 1);
        {
            let mut pools = ctx.mint_pools.write();
            let list = pools.entry(mint.to_string()).or_default();
            if let Some(pi) = list.iter_mut().find(|p| p.pool_address == pool_pf) {
                pi.bonding_curve_complete = Some(true);
            }
        }

        {
            let mut trackers = ctx.token_trackers.write();
            let mut tr = TokenTracker::new(mint, pool_amm, "pump_amm", 1, 0);
            for i in 0..20 {
                tr.record_trade(
                    &format!("buyer{i:03}"),
                    true,
                    200_000_000,
                    2_000_000,
                    &format!("sig{i:03}"),
                    1 + i as u64,
                    &cfg,
                );
            }
            trackers.insert(MomentumContext::tracker_storage_key(mint, pool_amm), tr);
        }

        let signals = ctx.check_for_signals();
        assert_eq!(signals.len(), 1, "expected single probe for pump_amm pool");
        assert_eq!(signals[0].mint, mint);
        assert_eq!(signals[0].pool, pool_amm);
        assert_eq!(signals[0].dex, "pump_amm");
    }

    /// Completed pumpfun pool + mint migration evidence must not probe BUY on pumpfun; pump_amm remains eligible.
    #[test]
    fn pumpfun_complete_blocks_probe_while_pump_amm_remains_eligible() {
        let cfg = {
            let mut c = MomentumConfig::default();
            c.default_position_lamports = 1_000;
            c.probe_buy_pct = 0.25;
            c.early_min_liquidity_sol = 0.0;
            c.min_unique_buyers = 0;
            c.min_trades_per_min = 0.0;
            c.min_buy_dominance = 0.0;
            c.min_sol_inflow_lamports = 0;
            c.require_mint_authority_renounced = false;
            c.require_freeze_authority_none = false;
            c.top1_buyer_share_cap = 1.0;
            c.top3_buyer_share_cap = 1.0;
            c.repeat_buyer_min_ratio = 0.0;
            c.min_trade_size_lamports = 0;
            c.small_buy_ratio_cap = 1.0;
            c.min_token_age_secs = 0;
            c
        };

        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);
        *ctx.config.write() = cfg.clone();

        let mint = "MintMigEvidence222222222222222222222222222222";
        let pool_pf = "PumpPfDead2222222222222222222222222222222222";
        let pool_amm = "pAMMAlive2222222222222222222222222222222222";

        ctx.register_pool(mint, pool_pf, "pumpfun", 1);
        ctx.register_pool(mint, pool_amm, "pump_amm", 2);
        {
            let mut pools = ctx.mint_pools.write();
            let list = pools.get_mut(mint).expect("pools");
            for p in list.iter_mut() {
                if p.pool_address == pool_pf {
                    p.bonding_curve_complete = Some(true);
                }
            }
        }

        {
            let mut trackers = ctx.token_trackers.write();
            let mut tr_pf = TokenTracker::new(mint, pool_pf, "pumpfun", 1, 30_000_000_000);
            for i in 0..20 {
                tr_pf.record_trade(
                    &format!("bp{i:03}"),
                    true,
                    200_000_000,
                    2_000_000,
                    &format!("spf{i:03}"),
                    1 + i as u64,
                    &cfg,
                );
            }
            trackers.insert(MomentumContext::tracker_storage_key(mint, pool_pf), tr_pf);

            let mut tr_amm = TokenTracker::new(mint, pool_amm, "pump_amm", 1, 0);
            for i in 0..20 {
                tr_amm.record_trade(
                    &format!("ba{i:03}"),
                    true,
                    200_000_000,
                    2_000_000,
                    &format!("sam{i:03}"),
                    1 + i as u64,
                    &cfg,
                );
            }
            trackers.insert(MomentumContext::tracker_storage_key(mint, pool_amm), tr_amm);
        }

        ctx.merge_pumpfun_migration_complete_evidence(mint, 99, 1);

        let signals = ctx.check_for_signals();
        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].pool, pool_amm);
        assert_eq!(signals[0].dex, "pump_amm");

        let pf_rejected = {
            let trackers = ctx.token_trackers.read();
            trackers
                .get(&MomentumContext::tracker_storage_key(mint, pool_pf))
                .expect("pf tracker")
                .is_rejected()
        };
        assert!(pf_rejected);
    }

    /// Contract: [`MomentumContext::pumpfun_entry_blocked_by_migration`] is PumpFun-only; mint-wide
    /// migration evidence must not affect `pump_amm` eligibility via this helper.
    #[test]
    fn pumpfun_entry_blocked_by_migration_never_true_for_pump_amm_with_sticky_evidence() {
        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);

        let mint = "MintPFAmmGate444444444444444444444444444444";
        let pool_pf = "poolPFgate444444444444444444444444444444444";
        let pool_amm = "poolAMMgate44444444444444444444444444444444";

        ctx.register_pool(mint, pool_pf, "pumpfun", 1);
        ctx.merge_pumpfun_migration_complete_evidence(mint, 100, 1);

        assert!(
            !ctx.pumpfun_entry_blocked_by_migration(mint, pool_amm, "pump_amm"),
            "dex guard: pump_amm must not consult mint-wide PumpFun complete evidence in this helper"
        );
        assert!(
            ctx.pumpfun_entry_blocked_by_migration(mint, pool_pf, "pumpfun"),
            "pumpfun may use mint-wide evidence when pool row is not yet marked bonding_curve_complete"
        );
    }

    /// `tokens_blacklisted` is mint-level: one `record_dev_info` event rejects two pool rows → +1 not +2.
    #[test]
    fn tokens_blacklisted_once_per_mint_for_record_dev_info_two_pool_trackers() {
        let mut cfg = MomentumConfig::default();
        cfg.max_dev_supply_pct = 50.0;

        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);
        *ctx.config.write() = cfg;

        let mint = "MintDevBlacklistDedupe444444444444444444444444";
        let pool_a = "poolDevA444444444444444444444444444444444444";
        let pool_b = "poolDevB444444444444444444444444444444444444";

        {
            let mut trackers = ctx.token_trackers.write();
            trackers.insert(
                MomentumContext::tracker_storage_key(mint, pool_a),
                TokenTracker::new(mint, pool_a, "raydium", 1, 0),
            );
            trackers.insert(
                MomentumContext::tracker_storage_key(mint, pool_b),
                TokenTracker::new(mint, pool_b, "orca", 1, 0),
            );
        }

        let before = ctx.tokens_blacklisted.load(Ordering::Relaxed);
        ctx.record_dev_info(mint, "DevWallet444444444444444444444444444444444", 99.0);
        let after = ctx.tokens_blacklisted.load(Ordering::Relaxed);
        assert_eq!(
            after.saturating_sub(before),
            1,
            "tokens_blacklisted counts blacklisted mints, not pool-scoped tracker rows"
        );
    }

    /// `tokens_tracked` counts unique mints: two pool rows from `get_or_create_tracker` → +1 total.
    #[test]
    fn tokens_tracked_unique_mint_two_pool_rows_via_get_or_create_tracker() {
        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);

        let mint = "MintTracked777777777777777777777777777777777";
        let p1 = "poolTrk711111111111111111111111111111111111";
        let p2 = "poolTrk7222222222222222222222222222222222222";

        let before = ctx.tokens_tracked.load(Ordering::Relaxed);
        assert!(ctx.get_or_create_tracker(mint, p1, "raydium", 1, 0));
        assert!(ctx.get_or_create_tracker(mint, p2, "orca", 1, 0));
        assert!(!ctx.get_or_create_tracker(mint, p1, "raydium", 1, 0));
        let after = ctx.tokens_tracked.load(Ordering::Relaxed);
        assert_eq!(
            after.saturating_sub(before),
            1,
            "tokens_tracked is per-mint, not per pool-scoped tracker row"
        );
    }

    /// Pending dev applied inside `get_or_create_tracker`: two pool rows for same mint → +1 blacklist total.
    #[test]
    fn tokens_blacklisted_unique_mint_pending_dev_two_pool_rows_via_get_or_create_tracker() {
        let mut cfg = MomentumConfig::default();
        cfg.max_dev_supply_pct = 50.0;

        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);
        *ctx.config.write() = cfg;

        let mint = "MintPendingDev888888888888888888888888888888";
        let p1 = "poolDev811111111111111111111111111111111111";
        let p2 = "poolDev822222222222222222222222222222222222";

        ctx.pending_dev_info.write().insert(
            mint.to_string(),
            (
                "DevWallet8888888888888888888888888888888888".to_string(),
                99.0,
            ),
        );

        let before_bl = ctx.tokens_blacklisted.load(Ordering::Relaxed);
        let before_tr = ctx.tokens_tracked.load(Ordering::Relaxed);
        assert!(ctx.get_or_create_tracker(mint, p1, "raydium", 1, 0));
        assert!(ctx.get_or_create_tracker(mint, p2, "orca", 1, 0));
        let after_bl = ctx.tokens_blacklisted.load(Ordering::Relaxed);
        let after_tr = ctx.tokens_tracked.load(Ordering::Relaxed);

        assert_eq!(after_bl.saturating_sub(before_bl), 1);
        assert_eq!(after_tr.saturating_sub(before_tr), 1);

        let trackers = ctx.token_trackers.read();
        assert!(trackers
            .get(&MomentumContext::tracker_storage_key(mint, p1))
            .unwrap()
            .is_rejected());
        assert!(trackers
            .get(&MomentumContext::tracker_storage_key(mint, p2))
            .unwrap()
            .is_rejected());
    }

    /// PR #147: pending dev applied on vacant insert rejects immediately — queue `removed`, never `active`.
    #[test]
    fn get_or_create_tracker_pending_dev_immediate_reject_queues_removed_not_active_for_pin() {
        let mut cfg = MomentumConfig::default();
        cfg.max_dev_supply_pct = 50.0;

        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);
        *ctx.config.write() = cfg;

        let mint = "MintPendingRejectPin999999999999999999999999";
        let pool = "poolPendingRejectPin999999999999999999999999";

        ctx.pending_dev_info.write().insert(
            mint.to_string(),
            (
                "DevWallet9999999999999999999999999999999999".to_string(),
                99.0,
            ),
        );

        assert!(ctx.get_or_create_tracker(mint, pool, "raydium", 1, 0));

        let q = ctx.momentum_active_pool_publish_queue.lock();
        assert!(
            !q.active
                .iter()
                .any(|e| e.mint.as_str() == mint && e.pool.as_str() == pool),
            "rejected-on-create must not queue active pin"
        );
        assert!(
            q.removed
                .iter()
                .any(|r| r.mint.as_str() == mint && r.pool.as_str() == pool),
            "rejected-on-create must queue removed for market-data"
        );
    }

    /// `tokens_blacklisted` is mint-level: one LP-removal event rejects two pool rows → +1 not +2.
    #[test]
    fn tokens_blacklisted_once_per_mint_for_record_lp_removal_two_pool_trackers() {
        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);

        let mint = "MintLpBlacklistDedupe555555555555555555555555";
        let pool_a = "poolLpA5555555555555555555555555555555555555";
        let pool_b = "poolLpB5555555555555555555555555555555555555";

        {
            let mut trackers = ctx.token_trackers.write();
            trackers.insert(
                MomentumContext::tracker_storage_key(mint, pool_a),
                TokenTracker::new(mint, pool_a, "raydium", 1, 0),
            );
            trackers.insert(
                MomentumContext::tracker_storage_key(mint, pool_b),
                TokenTracker::new(mint, pool_b, "orca", 1, 0),
            );
        }

        let before = ctx.tokens_blacklisted.load(Ordering::Relaxed);
        ctx.record_lp_removal(mint, Some(99));
        let after = ctx.tokens_blacklisted.load(Ordering::Relaxed);
        assert_eq!(after.saturating_sub(before), 1);
        let trackers = ctx.token_trackers.read();
        assert!(trackers
            .get(&MomentumContext::tracker_storage_key(mint, pool_a))
            .unwrap()
            .is_rejected());
        assert!(trackers
            .get(&MomentumContext::tracker_storage_key(mint, pool_b))
            .unwrap()
            .is_rejected());
    }

    /// PumpFun migration gate: two pumpfun pool rows rejected in one `check_for_signals` → +1 not +2.
    #[test]
    fn tokens_blacklisted_once_per_mint_for_pumpfun_migration_gate_two_pools_one_pass() {
        let cfg = {
            let mut c = MomentumConfig::default();
            c.default_position_lamports = 1_000;
            c.probe_buy_pct = 0.25;
            c.early_min_liquidity_sol = 0.0;
            c.min_unique_buyers = 0;
            c.min_trades_per_min = 0.0;
            c.min_buy_dominance = 0.0;
            c.min_sol_inflow_lamports = 0;
            c.require_mint_authority_renounced = false;
            c.require_freeze_authority_none = false;
            c.top1_buyer_share_cap = 1.0;
            c.top3_buyer_share_cap = 1.0;
            c.repeat_buyer_min_ratio = 0.0;
            c.min_trade_size_lamports = 0;
            c.small_buy_ratio_cap = 1.0;
            c.min_token_age_secs = 0;
            c
        };

        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);
        *ctx.config.write() = cfg;

        let mint = "MintPumpBlacklist666666666666666666666666666";
        let p1 = "PfPool6111111111111111111111111111111111111";
        let p2 = "PfPool6222222222222222222222222222222222222";

        ctx.register_pool(mint, p1, "pumpfun", 1);
        ctx.register_pool(mint, p2, "pumpfun", 2);
        {
            let mut pools = ctx.mint_pools.write();
            let list = pools.get_mut(mint).expect("pools");
            for pi in list.iter_mut() {
                pi.bonding_curve_complete = Some(true);
            }
        }

        {
            let mut trackers = ctx.token_trackers.write();
            trackers.insert(
                MomentumContext::tracker_storage_key(mint, p1),
                TokenTracker::new(mint, p1, "pumpfun", 1, 30_000_000_000),
            );
            trackers.insert(
                MomentumContext::tracker_storage_key(mint, p2),
                TokenTracker::new(mint, p2, "pumpfun", 1, 30_000_000_000),
            );
        }

        let before = ctx.tokens_blacklisted.load(Ordering::Relaxed);
        let signals = ctx.check_for_signals();
        let after = ctx.tokens_blacklisted.load(Ordering::Relaxed);

        assert!(signals.is_empty());
        assert_eq!(after.saturating_sub(before), 1);
    }

    /// Two eligible pool trackers for the same mint must not both emit in one `check_for_signals` call.
    #[test]
    fn check_for_signals_serializes_mint_entry_one_signal_per_tick_two_pools() {
        let cfg = {
            let mut c = MomentumConfig::default();
            c.default_position_lamports = 1_000;
            c.probe_buy_pct = 0.25;
            c.early_min_liquidity_sol = 0.0;
            c.min_unique_buyers = 0;
            c.min_trades_per_min = 0.0;
            c.min_buy_dominance = 0.0;
            c.min_sol_inflow_lamports = 0;
            c.require_mint_authority_renounced = false;
            c.require_freeze_authority_none = false;
            c.top1_buyer_share_cap = 1.0;
            c.top3_buyer_share_cap = 1.0;
            c.repeat_buyer_min_ratio = 0.0;
            c.min_trade_size_lamports = 0;
            c.small_buy_ratio_cap = 1.0;
            c.min_token_age_secs = 0;
            c
        };

        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);
        *ctx.config.write() = cfg.clone();

        let mint = "MintDupGuard333333333333333333333333333333";
        let pool_a = "RayPool333333333333333333333333333333333333";
        let pool_b = "OrcaPool33333333333333333333333333333333333";

        {
            let mut trackers = ctx.token_trackers.write();
            for (pool, dex, buyer_prefix) in [(pool_a, "raydium", "ra"), (pool_b, "orca", "oc")] {
                let mut tr = TokenTracker::new(mint, pool, dex, 1, 0);
                for i in 0..20 {
                    tr.record_trade(
                        &format!("{buyer_prefix}{i:03}"),
                        true,
                        200_000_000,
                        2_000_000,
                        &format!("sig{dex}{i:03}"),
                        1 + i as u64,
                        &cfg,
                    );
                }
                trackers.insert(MomentumContext::tracker_storage_key(mint, pool), tr);
            }
        }

        let signals = ctx.check_for_signals();
        assert_eq!(
            signals.iter().filter(|s| s.mint == mint).count(),
            1,
            "mint-level duplicate guard: expected exactly one entry signal for mint"
        );
        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].kind, EntryKind::Probe);
    }

    /// Stale `pending_buy_mint_index` (intent_id not in `pending_buy_entries`) must not block entry.
    #[test]
    fn check_for_signals_prunes_stale_pending_buy_mint_index() {
        let cfg = {
            let mut c = MomentumConfig::default();
            c.default_position_lamports = 1_000;
            c.probe_buy_pct = 0.25;
            c.early_min_liquidity_sol = 0.0;
            c.min_unique_buyers = 0;
            c.min_trades_per_min = 0.0;
            c.min_buy_dominance = 0.0;
            c.min_sol_inflow_lamports = 0;
            c.require_mint_authority_renounced = false;
            c.require_freeze_authority_none = false;
            c.top1_buyer_share_cap = 1.0;
            c.top3_buyer_share_cap = 1.0;
            c.repeat_buyer_min_ratio = 0.0;
            c.min_trade_size_lamports = 0;
            c.small_buy_ratio_cap = 1.0;
            c.min_token_age_secs = 0;
            c
        };

        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);
        *ctx.config.write() = cfg.clone();

        let mint = "MintStalePending9999999999999999999999999999";
        let pool_a = "RayPool999999999999999999999999999999999999";
        let pool_b = "OrcaPool99999999999999999999999999999999999";

        {
            let mut trackers = ctx.token_trackers.write();
            for (pool, dex, buyer_prefix) in [(pool_a, "raydium", "s9"), (pool_b, "orca", "s8")] {
                let mut tr = TokenTracker::new(mint, pool, dex, 1, 0);
                for i in 0..20 {
                    tr.record_trade(
                        &format!("{buyer_prefix}{i:03}"),
                        true,
                        200_000_000,
                        2_000_000,
                        &format!("sigst{i:03}"),
                        1 + i as u64,
                        &cfg,
                    );
                }
                trackers.insert(MomentumContext::tracker_storage_key(mint, pool), tr);
            }
        }

        ctx.pending_buy_mint_index.write().insert(
            mint.to_string(),
            "ghost-intent-not-in-pending-buy-entries".to_string(),
        );
        assert!(ctx.pending_buy_mint_index.read().contains_key(mint));

        let signals = ctx.check_for_signals();
        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].mint, mint);
        assert!(
            !ctx.pending_buy_mint_index.read().contains_key(mint),
            "stale index entry should be pruned"
        );
    }

    /// `tokens_blacklisted`: at most one bump per mint per `check_for_signals` across PumpFun migration + strategy reject.
    #[test]
    fn tokens_blacklisted_at_most_once_per_check_for_signals_migration_plus_strategy() {
        let cfg = {
            let mut c = MomentumConfig::default();
            c.default_position_lamports = 1_000;
            c.probe_buy_pct = 0.25;
            c.early_min_liquidity_sol = 0.0;
            c.min_unique_buyers = 0;
            c.min_trades_per_min = 0.0;
            c.min_buy_dominance = 0.0;
            c.min_sol_inflow_lamports = 0;
            c.require_mint_authority_renounced = true;
            c.require_freeze_authority_none = false;
            c.top1_buyer_share_cap = 1.0;
            c.top3_buyer_share_cap = 1.0;
            c.repeat_buyer_min_ratio = 0.0;
            c.min_trade_size_lamports = 0;
            c.small_buy_ratio_cap = 1.0;
            c.min_token_age_secs = 0;
            c
        };

        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);
        *ctx.config.write() = cfg.clone();

        let mint = "MintBlDedupeMigStr7777777777777777777777777";
        let pool_pf = "poolPFDedMig777777777777777777777777777777";
        let pool_or = "poolOrcaDedMig7777777777777777777777777777";

        ctx.register_pool(mint, pool_pf, "pumpfun", 1);
        ctx.register_pool(mint, pool_or, "orca", 2);
        {
            let mut pools = ctx.mint_pools.write();
            let list = pools.get_mut(mint).expect("pools");
            for p in list.iter_mut() {
                if p.pool_address == pool_pf {
                    p.bonding_curve_complete = Some(true);
                }
            }
        }
        ctx.merge_pumpfun_migration_complete_evidence(mint, 99, 1);

        ctx.record_mint_info(
            mint,
            MintInfo {
                token_program: "spl-token".to_string(),
                decimals: 6,
                supply: 1,
                mint_authority: Some("auth1".to_string()),
                freeze_authority: None,
                last_updated: Instant::now(),
            },
        );

        {
            let mut trackers = ctx.token_trackers.write();
            for (pool, dex) in [(pool_pf, "pumpfun"), (pool_or, "orca")] {
                let mut tr = TokenTracker::new(mint, pool, dex, 1, 30_000_000_000);
                for i in 0..20 {
                    tr.record_trade(
                        &format!("u{i:03}"),
                        true,
                        200_000_000,
                        2_000_000,
                        &format!("sx{i}"),
                        1 + i as u64,
                        &cfg,
                    );
                }
                trackers.insert(MomentumContext::tracker_storage_key(mint, pool), tr);
            }
        }

        let before = ctx.tokens_blacklisted.load(Ordering::Relaxed);
        let _ = ctx.check_for_signals();
        let after = ctx.tokens_blacklisted.load(Ordering::Relaxed);
        assert_eq!(
            after.saturating_sub(before),
            1,
            "mint-level metric: single bump per check_for_signals across migration + strategy paths"
        );
    }

    /// Trade `creator` applies mint-wide before `record_trade`: sibling pool gets dev-sell WAIT state.
    #[test]
    fn dev_sell_sibling_pools_sync_wait_not_reject() {
        let cfg = MomentumConfig::default();

        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);
        *ctx.config.write() = cfg.clone();

        const WSOL: &str = "So11111111111111111111111111111111111111112";
        let mint = "MintTradDevSibling666666666666666666666666666";
        let pool_a = "poolDevSib6111111111111111111111111111111111";
        let pool_b = "poolDevSib6222222222222222222222222222222222";
        let dev = "DevWbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

        {
            let mut trackers = ctx.token_trackers.write();
            let mut tr_a = TokenTracker::new(mint, pool_a, "pumpfun", 1, 30_000_000_000);
            tr_a.record_trade("buyer01", true, 500_000_000, 1_000_000, "sig0", 10, &cfg);
            trackers.insert(MomentumContext::tracker_storage_key(mint, pool_a), tr_a);

            let mut tr_b = TokenTracker::new(mint, pool_b, "pumpfun", 1, 30_000_000_000);
            tr_b.record_trade("buyer02", true, 500_000_000, 1_000_000, "sig0b", 11, &cfg);
            trackers.insert(MomentumContext::tracker_storage_key(mint, pool_b), tr_b);
        }

        let evt = MarketEvent {
            header: RecordHeader::new("test", BUILD_VERSION, "run"),
            event_id: "ev-dev-sell-sibling".into(),
            source: "geyser".into(),
            slot: Some(42),
            kind: MarketEventKind::Trade {
                pool_address: pool_b.to_string(),
                mint: mint.to_string(),
                quote_mint: WSOL.to_string(),
                trader: dev.to_string(),
                is_buy: false,
                sol_amount: 50_000_000,
                token_amount: 1,
                token_decimals: 6,
                signature: Some("sig-ds".into()),
                dex: "pumpfun".into(),
                creator: Some(dev.to_string()),
                token_program: None,
            },
        };

        let rt = tokio::runtime::Runtime::new().expect("runtime");
        rt.block_on(async {
            process_market_event(&ctx, &evt, true)
                .await
                .expect("process trade");
        });

        let (sibling_rejected, sibling_dev_sold, sibling_has_obs) = {
            let trackers = ctx.token_trackers.read();
            let t = trackers
                .get(&MomentumContext::tracker_storage_key(mint, pool_a))
                .expect("sibling pool_a tracker (did not receive Trade)");
            (
                t.is_rejected(),
                t.dev_sold,
                t.dev_sell_observed_at.is_some(),
            )
        };
        assert!(
            !sibling_rejected,
            "pool_a sibling tracker must not reject on dev sell from pool_b"
        );
        assert!(sibling_dev_sold);
        assert!(sibling_has_obs);
    }

    /// Trade-driven reject: `tokens_blacklisted` bumps at most once per mint across pool rows.
    #[test]
    fn tokens_blacklisted_trade_rejection_once_per_mint_two_pool_trackers() {
        let mut cfg = MomentumConfig::default();
        cfg.max_single_dump_lamports = 1;

        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);
        *ctx.config.write() = cfg;

        let mint = "MintTradeBl00000000000000000000000000000000";
        let p1 = "poolTrBl0111111111111111111111111111111111";
        let p2 = "poolTrBl0222222222222222222222222222222222";

        {
            let mut trackers = ctx.token_trackers.write();
            trackers.insert(
                MomentumContext::tracker_storage_key(mint, p1),
                TokenTracker::new(mint, p1, "raydium", 1, 0),
            );
            trackers.insert(
                MomentumContext::tracker_storage_key(mint, p2),
                TokenTracker::new(mint, p2, "orca", 1, 0),
            );
        }

        let before = ctx.tokens_blacklisted.load(Ordering::Relaxed);
        ctx.record_trade(
            mint,
            p1,
            "dumper1",
            false,
            10_000_000_000,
            1,
            "dump_sig_1",
            100,
        );
        ctx.record_trade(
            mint,
            p2,
            "dumper2",
            false,
            10_000_000_000,
            1,
            "dump_sig_2",
            101,
        );
        let after = ctx.tokens_blacklisted.load(Ordering::Relaxed);
        assert_eq!(after.saturating_sub(before), 1);
        let trackers = ctx.token_trackers.read();
        assert!(trackers
            .get(&MomentumContext::tracker_storage_key(mint, p1))
            .unwrap()
            .is_rejected());
        assert!(trackers
            .get(&MomentumContext::tracker_storage_key(mint, p2))
            .unwrap()
            .is_rejected());
    }

    /// `tokens_blacklisted` dedupes across handlers: `record_trade` then `check_for_signals` → +1 total.
    #[test]
    fn tokens_blacklisted_once_per_mint_across_record_trade_then_check_for_signals() {
        let mut cfg = MomentumConfig::default();
        cfg.max_single_dump_lamports = 1;
        cfg.default_position_lamports = 1_000;
        cfg.probe_buy_pct = 0.25;
        cfg.early_min_liquidity_sol = 0.0;
        cfg.min_unique_buyers = 0;
        cfg.min_trades_per_min = 0.0;
        cfg.min_buy_dominance = 0.0;
        cfg.min_sol_inflow_lamports = 0;
        cfg.require_mint_authority_renounced = true;
        cfg.require_freeze_authority_none = false;
        cfg.top1_buyer_share_cap = 1.0;
        cfg.top3_buyer_share_cap = 1.0;
        cfg.repeat_buyer_min_ratio = 0.0;
        cfg.min_trade_size_lamports = 0;
        cfg.small_buy_ratio_cap = 1.0;
        cfg.min_token_age_secs = 0;

        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);
        *ctx.config.write() = cfg.clone();

        let mint = "MintCrossFn8888888888888888888888888888888";
        let pool_dump = "poolDump8888888888888888888888888888888888";
        let pool_strat = "poolStrat888888888888888888888888888888888";

        ctx.record_mint_info(
            mint,
            MintInfo {
                token_program: "spl-token".to_string(),
                decimals: 6,
                supply: 1,
                mint_authority: Some("mauth".to_string()),
                freeze_authority: None,
                last_updated: Instant::now(),
            },
        );

        {
            let mut trackers = ctx.token_trackers.write();
            for (pool, dex) in [(pool_dump, "raydium"), (pool_strat, "orca")] {
                let mut tr = TokenTracker::new(mint, pool, dex, 1, 30_000_000_000);
                for i in 0..20 {
                    tr.record_trade(
                        &format!("xf{i:03}"),
                        true,
                        200_000_000,
                        2_000_000,
                        &format!("sxg{i}"),
                        1 + i as u64,
                        &cfg,
                    );
                }
                trackers.insert(MomentumContext::tracker_storage_key(mint, pool), tr);
            }
        }

        let before = ctx.tokens_blacklisted.load(Ordering::Relaxed);
        ctx.record_trade(
            mint,
            pool_dump,
            "dumptrader",
            false,
            10_000_000_000,
            1,
            "sig-dump-cross",
            500,
        );
        let _ = ctx.check_for_signals();
        let after = ctx.tokens_blacklisted.load(Ordering::Relaxed);
        assert_eq!(
            after.saturating_sub(before),
            1,
            "same mint: first handler counts the metric; later rejections must not increment again"
        );

        assert!(ctx
            .token_trackers
            .read()
            .get(&MomentumContext::tracker_storage_key(mint, pool_strat))
            .expect("strat pool")
            .is_rejected());
    }

    #[test]
    fn dump_recovery_waits_then_allows_after_stabilization() {
        let mut cfg = MomentumConfig::default();

        // Disable other gates to isolate dump recovery.
        cfg.early_min_liquidity_sol = 0.0;
        cfg.min_unique_buyers = 0;
        cfg.min_trades_per_min = 0.0;
        cfg.min_buy_dominance = 0.0;
        cfg.min_sol_inflow_lamports = 0;
        cfg.require_mint_authority_renounced = false;
        cfg.require_freeze_authority_none = false;
        cfg.top1_buyer_share_cap = 1.0;
        cfg.top3_buyer_share_cap = 1.0;
        cfg.repeat_buyer_min_ratio = 0.0;
        cfg.min_trade_size_lamports = 0;
        cfg.small_buy_ratio_cap = 1.0;
        cfg.price_trend_filter_enabled = false;
        cfg.min_token_age_secs = 0;

        // Dump recovery params (window translated to ~slot span via MOMENTUM_APPROX_SLOT_MS)
        cfg.dump_recovery_window_secs = 30;
        cfg.dump_recovery_min_buy_dominance = 0.60;
        cfg.dump_recovery_min_net_inflow_lamports = 100;
        cfg.dump_recovery_min_recovery_secs = 10;

        let mut tracker = TokenTracker::new("mint", "pool", "dex", 5000, 0);

        // Dump: net outflow in chain window (slots near head 5050).
        for i in 0..5u64 {
            tracker.record_trade("w", false, 200, 1, &format!("sd{i}"), 5001 + i, &cfg);
        }
        let (should_trade, reason) =
            tracker.should_generate_intent(&cfg, None, 5050, Instant::now());
        assert!(!should_trade);
        assert!(reason.contains("WAIT_CONFIRMATION"));

        // Recovery: strong buy flow in same chain window.
        for i in 0..8u64 {
            tracker.record_trade("w", true, 300, 1, &format!("rb{i}"), 5010 + i, &cfg);
        }

        // Still stabilizing on slots (min_recovery_secs → slot span).
        let (should_trade, reason) =
            tracker.should_generate_intent(&cfg, None, 5070, Instant::now());
        assert!(!should_trade);
        assert!(reason.contains("WAIT_CONFIRMATION"));

        // Enough head advancement vs recovery start slot.
        let (should_trade, _reason) =
            tracker.should_generate_intent(&cfg, None, 5100, Instant::now());
        assert!(should_trade);
    }

    fn downtrend_filter_test_cfg() -> MomentumConfig {
        let mut cfg = relaxed_entry_gates_helper_cfg();
        cfg.price_trend_filter_enabled = true;
        cfg.price_trend_window_secs = 120;
        cfg.price_trend_min_trades = 18;
        cfg.price_trend_min_tps_rise_pct = 8.0;
        cfg.price_trend_max_drawdown_pct = 25.0;
        cfg.price_trend_recovery_min_buy_dominance = 0.60;
        cfg.price_trend_recovery_min_inflow_lamports = 500_000_000;
        cfg.price_trend_bucket_count = 5;
        cfg.price_trend_lower_highs_max_breaks = 0;
        cfg.price_trend_recovery_min_positive_buckets = 2;
        cfg.price_trend_recovery_min_secs = 15;
        cfg.price_trend_recovery_requires_no_lower_highs = true;
        cfg
    }

    struct PriceTrendBucketTradeArgs<'a> {
        tracker: &'a mut TokenTracker,
        cfg: &'a MomentumConfig,
        head: u64,
        bucket_idx: usize,
        tps: f64,
        count: u32,
        is_buy: bool,
        label: &'a str,
    }

    impl PriceTrendBucketTradeArgs<'_> {
        fn record(self) {
            let window_slots = momentum_secs_to_slot_span(self.cfg.price_trend_window_secs);
            let bucket_count = clamp_price_trend_bucket_count(self.cfg.price_trend_bucket_count);
            let bucket_span = (window_slots / bucket_count as u64).max(1);
            let age = self.bucket_idx as u64 * bucket_span + bucket_span / 2;
            let base_slot = self.head.saturating_sub(age);
            for i in 0..self.count {
                record_trade_implied_tps(
                    self.tracker,
                    self.cfg,
                    base_slot + u64::from(i),
                    self.tps,
                    self.is_buy,
                    &format!("{}_{}_{i}", self.label, self.bucket_idx),
                );
            }
        }
    }

    fn record_trade_implied_tps(
        tracker: &mut TokenTracker,
        cfg: &MomentumConfig,
        slot: u64,
        tps: f64,
        is_buy: bool,
        trader: &str,
    ) {
        let sol_amount = 1_000_000u64;
        let token_amount = ((tps * sol_amount as f64).max(1.0)) as u64;
        tracker.record_trade(
            trader,
            is_buy,
            sol_amount,
            token_amount,
            &format!("sig_{slot}_{trader}"),
            slot,
            cfg,
        );
    }

    #[test]
    fn downtrend_multi_bucket_lower_highs_blocks() {
        let cfg = downtrend_filter_test_cfg();
        let mut tracker = TokenTracker::new("mint", "pool", "dex", 1, 10_000_000_000);
        let head = 1_000u64;
        // bucket 0 = late (higher tps); bucket 4 = early (lower tps) → lower highs in price
        let rising_tps_late_to_early = [120.0, 115.0, 110.0, 105.0, 100.0];
        for (bucket_idx, tps) in rising_tps_late_to_early.iter().enumerate() {
            PriceTrendBucketTradeArgs {
                tracker: &mut tracker,
                cfg: &cfg,
                head,
                bucket_idx,
                tps: *tps,
                count: 4,
                is_buy: true,
                label: "lh",
            }
            .record();
        }

        let (should_trade, reason) =
            tracker.should_generate_intent(&cfg, None, head, Instant::now());
        assert!(!should_trade, "expected WAIT_DOWNTREND, got: {reason}");
        assert!(reason.contains("WAIT_DOWNTREND"));
        assert!(reason.contains("lower_highs"));
        assert!(reason.contains("breaks=0/"));
    }

    #[test]
    fn downtrend_lower_highs_blocks_entry() {
        downtrend_multi_bucket_lower_highs_blocks();
    }

    #[test]
    fn downtrend_drawdown_and_slope_blocks() {
        let cfg = downtrend_filter_test_cfg();
        let mut tracker = TokenTracker::new("mint", "pool", "dex", 1, 10_000_000_000);
        let head = 2_000u64;
        let window_slots = momentum_secs_to_slot_span(cfg.price_trend_window_secs);
        let half = (window_slots / 2).max(1);

        for i in 0..10u64 {
            let slot = head.saturating_sub(window_slots - 10 + i);
            record_trade_implied_tps(&mut tracker, &cfg, slot, 70.0, true, &format!("ath{i}"));
        }
        for i in 0..10u64 {
            let slot = head.saturating_sub(half + 20 + i);
            record_trade_implied_tps(&mut tracker, &cfg, slot, 130.0, false, &format!("fall{i}"));
        }
        for i in 0..10u64 {
            let slot = head.saturating_sub(5 + i);
            record_trade_implied_tps(&mut tracker, &cfg, slot, 125.0, false, &format!("late{i}"));
        }

        let (should_trade, reason) =
            tracker.should_generate_intent(&cfg, None, head, Instant::now());
        assert!(!should_trade, "expected WAIT_DOWNTREND, got: {reason}");
        assert!(reason.contains("WAIT_DOWNTREND"));
        assert!(reason.contains("drawdown"));
    }

    #[test]
    fn downtrend_recovery_exception_allows() {
        let mut cfg = downtrend_filter_test_cfg();
        cfg.price_trend_min_trades = 5;
        cfg.price_trend_recovery_min_secs = 10;
        cfg.price_trend_max_drawdown_pct = 5.0;
        cfg.price_trend_min_tps_rise_pct = 1.0;
        cfg.price_trend_recovery_requires_no_lower_highs = false;
        cfg.price_trend_recovery_min_inflow_lamports = 100;
        let mut tracker = TokenTracker::new("mint", "pool", "dex", 1, 10_000_000_000);
        let head = 3_000u64;
        let half = (momentum_secs_to_slot_span(cfg.price_trend_window_secs) / 2).max(1);

        for i in 0..8u64 {
            record_trade_implied_tps(
                &mut tracker,
                &cfg,
                head.saturating_sub(half + 80 + i),
                80.0,
                true,
                &format!("early{i}"),
            );
        }
        for i in 0..10u64 {
            record_trade_implied_tps(
                &mut tracker,
                &cfg,
                head.saturating_sub(20 + i),
                135.0,
                false,
                &format!("fall{i}"),
            );
        }
        PriceTrendBucketTradeArgs {
            tracker: &mut tracker,
            cfg: &cfg,
            head,
            bucket_idx: 2,
            tps: 128.0,
            count: 6,
            is_buy: false,
            label: "dump_b",
        }
        .record();
        PriceTrendBucketTradeArgs {
            tracker: &mut tracker,
            cfg: &cfg,
            head,
            bucket_idx: 1,
            tps: 120.0,
            count: 6,
            is_buy: true,
            label: "rec_mid",
        }
        .record();
        PriceTrendBucketTradeArgs {
            tracker: &mut tracker,
            cfg: &cfg,
            head,
            bucket_idx: 0,
            tps: 110.0,
            count: 6,
            is_buy: true,
            label: "rec_late",
        }
        .record();

        let wait = tracker.price_trend_wait_reason(&cfg, head);
        assert!(
            wait.is_some(),
            "recovery must not bypass before min_secs: {wait:?}"
        );
        assert!(
            wait.as_deref().unwrap().contains("stabilizing")
                || wait.as_deref().unwrap().contains("drawdown"),
            "unexpected wait: {wait:?}"
        );
        // Confirmed recovery after min_secs: see `downtrend_recovery_requires_min_secs`.
    }

    #[test]
    fn downtrend_early_pump_passes() {
        let cfg = downtrend_filter_test_cfg();
        let mut tracker = TokenTracker::new("mint", "pool", "dex", 1, 10_000_000_000);
        let head = 4_000u64;
        // bucket 0 = late (lower tps); bucket 4 = early (higher tps) → rising price, no lower highs
        let falling_tps_late_to_early = [100.0, 105.0, 110.0, 115.0, 120.0];
        for (bucket_idx, tps) in falling_tps_late_to_early.iter().enumerate() {
            PriceTrendBucketTradeArgs {
                tracker: &mut tracker,
                cfg: &cfg,
                head,
                bucket_idx,
                tps: *tps,
                count: 4,
                is_buy: true,
                label: "pump",
            }
            .record();
        }

        let (should_trade, reason) =
            tracker.should_generate_intent(&cfg, None, head, Instant::now());
        assert!(
            should_trade,
            "rising price should pass downtrend gate: {reason}"
        );
    }

    #[test]
    fn downtrend_bounce_breaks_strict_lower_highs_but_blocks_with_max_breaks_zero() {
        let cfg = downtrend_filter_test_cfg();
        let mut tracker = TokenTracker::new("mint", "pool", "dex", 1, 10_000_000_000);
        let head = 4_500u64;
        let window_slots = momentum_secs_to_slot_span(cfg.price_trend_window_secs);
        let half = (window_slots / 2).max(1);

        // 4/4 lower-high pairs monotonic; bucket-0 bounce breaks only the last pair (max_breaks=0).
        let bucket_tps = [118.0, 124.0, 116.0, 108.0, 100.0];
        for (bucket_idx, tps) in bucket_tps.iter().enumerate() {
            PriceTrendBucketTradeArgs {
                tracker: &mut tracker,
                cfg: &cfg,
                head,
                bucket_idx,
                tps: *tps,
                count: 4,
                is_buy: true,
                label: "lh",
            }
            .record();
        }
        PriceTrendBucketTradeArgs {
            tracker: &mut tracker,
            cfg: &cfg,
            head,
            bucket_idx: 4,
            tps: 50.0,
            count: 6,
            is_buy: true,
            label: "ath",
        }
        .record();
        // Late-window dump keeps short_slope positive (price still falling vs early half).
        for i in 0..8u64 {
            let slot = head.saturating_sub(half + 5 + i);
            record_trade_implied_tps(&mut tracker, &cfg, slot, 135.0, false, &format!("dump{i}"));
        }

        let (should_trade, reason) =
            tracker.should_generate_intent(&cfg, None, head, Instant::now());
        assert!(!should_trade, "FODL-like bounce must not enter: {reason}");
        assert!(reason.contains("WAIT_DOWNTREND"));
    }

    #[test]
    fn downtrend_recovery_requires_two_positive_buckets() {
        let medians_one_step = [Some(120.0), Some(130.0), None, None, None];
        assert!(!price_trend_recovery_slope_ok(&medians_one_step, 2));
        let medians_two_steps = [Some(115.0), Some(125.0), Some(135.0), None, None];
        assert!(price_trend_recovery_slope_ok(&medians_two_steps, 2));
    }

    #[test]
    fn downtrend_recovery_requires_min_secs() {
        let mut cfg = downtrend_filter_test_cfg();
        cfg.price_trend_min_trades = 5;
        cfg.price_trend_recovery_min_secs = 15;
        cfg.price_trend_max_drawdown_pct = 5.0;
        cfg.price_trend_min_tps_rise_pct = 1.0;
        cfg.price_trend_recovery_requires_no_lower_highs = false;
        cfg.price_trend_recovery_min_inflow_lamports = 100;
        let mut tracker = TokenTracker::new("mint", "pool", "dex", 1, 10_000_000_000);
        let head = 6_000u64;
        let half = (momentum_secs_to_slot_span(cfg.price_trend_window_secs) / 2).max(1);

        for i in 0..8u64 {
            record_trade_implied_tps(
                &mut tracker,
                &cfg,
                head.saturating_sub(half + 80 + i),
                70.0,
                true,
                &format!("early{i}"),
            );
        }
        for i in 0..8u64 {
            record_trade_implied_tps(
                &mut tracker,
                &cfg,
                head.saturating_sub(half + 20 + i),
                140.0,
                false,
                &format!("dump{i}"),
            );
        }
        PriceTrendBucketTradeArgs {
            tracker: &mut tracker,
            cfg: &cfg,
            head,
            bucket_idx: 0,
            tps: 115.0,
            count: 6,
            is_buy: true,
            label: "r0",
        }
        .record();
        PriceTrendBucketTradeArgs {
            tracker: &mut tracker,
            cfg: &cfg,
            head,
            bucket_idx: 1,
            tps: 125.0,
            count: 6,
            is_buy: true,
            label: "r1",
        }
        .record();

        let wait = tracker.price_trend_wait_reason(&cfg, head);
        assert!(wait.is_some(), "expected wait before min recovery secs");
        assert!(
            wait.as_deref().unwrap().contains("stabilizing")
                || wait.as_deref().unwrap().contains("drawdown"),
            "unexpected: {wait:?}"
        );

        let recovery_slots = momentum_secs_to_slot_span(cfg.price_trend_recovery_min_secs);
        let head_after = head + recovery_slots + 5;
        PriceTrendBucketTradeArgs {
            tracker: &mut tracker,
            cfg: &cfg,
            head: head_after,
            bucket_idx: 0,
            tps: 115.0,
            count: 6,
            is_buy: true,
            label: "r0b",
        }
        .record();
        PriceTrendBucketTradeArgs {
            tracker: &mut tracker,
            cfg: &cfg,
            head: head_after,
            bucket_idx: 1,
            tps: 125.0,
            count: 6,
            is_buy: true,
            label: "r1b",
        }
        .record();
        let wait_after = tracker.price_trend_wait_reason(&cfg, head_after);
        assert!(
            wait_after.is_none(),
            "recovery should be confirmed after min_secs: {wait_after:?}"
        );
    }

    #[test]
    fn downtrend_recovery_blocked_when_lower_highs_raw_match() {
        let mut cfg = downtrend_filter_test_cfg();
        cfg.price_trend_recovery_min_secs = 0;
        cfg.price_trend_recovery_requires_no_lower_highs = true;
        let mut tracker = TokenTracker::new("mint", "pool", "dex", 1, 10_000_000_000);
        let head = 6_500u64;
        let rising_tps_late_to_early = [132.0, 124.0, 116.0, 108.0, 100.0];
        for (bucket_idx, tps) in rising_tps_late_to_early.iter().enumerate() {
            PriceTrendBucketTradeArgs {
                tracker: &mut tracker,
                cfg: &cfg,
                head,
                bucket_idx,
                tps: *tps,
                count: 4,
                is_buy: true,
                label: "struct",
            }
            .record();
        }
        let (should_trade, reason) =
            tracker.should_generate_intent(&cfg, None, head, Instant::now());
        assert!(
            !should_trade,
            "recovery must be blocked by lower_highs: {reason}"
        );
        assert!(
            reason.contains("lower_highs"),
            "unexpected reason: {reason}"
        );
    }

    #[test]
    fn downtrend_fodl_bounce_scenario_blocks_entry() {
        let mut cfg = downtrend_filter_test_cfg();
        cfg.price_trend_recovery_min_buy_dominance = 0.65;
        cfg.price_trend_recovery_min_inflow_lamports = 1_500_000_000;
        let mut tracker = TokenTracker::new("mint", "pool", "dex", 1, 10_000_000_000);
        let head = 7_000u64;
        let window_slots = momentum_secs_to_slot_span(cfg.price_trend_window_secs);
        let half = (window_slots / 2).max(1);

        // Structural lower highs across 5 buckets (pump → dump), late bounce cannot recover.
        let rising_tps_late_to_early = [122.0, 118.0, 112.0, 105.0, 95.0];
        for (bucket_idx, tps) in rising_tps_late_to_early.iter().enumerate() {
            PriceTrendBucketTradeArgs {
                tracker: &mut tracker,
                cfg: &cfg,
                head,
                bucket_idx,
                tps: *tps,
                count: 4,
                is_buy: true,
                label: "fodl",
            }
            .record();
        }
        for i in 0..8u64 {
            let slot = head.saturating_sub(half + 5 + i);
            record_trade_implied_tps(&mut tracker, &cfg, slot, 128.0, true, &format!("bounce{i}"));
        }

        let (should_trade, reason) =
            tracker.should_generate_intent(&cfg, None, head, Instant::now());
        assert!(
            !should_trade,
            "FODL 2nd-bounce profile must WAIT_DOWNTREND: {reason}"
        );
        assert!(reason.contains("WAIT_DOWNTREND"));
    }

    #[test]
    fn downtrend_insufficient_trades_skips() {
        let cfg = downtrend_filter_test_cfg();
        let mut tracker = TokenTracker::new("mint", "pool", "dex", 1, 10_000_000_000);
        let head = 5_000u64;

        for i in 0..5u64 {
            record_trade_implied_tps(&mut tracker, &cfg, head - i, 150.0, true, &format!("t{i}"));
        }

        let (should_trade, _reason) =
            tracker.should_generate_intent(&cfg, None, head, Instant::now());
        assert!(should_trade, "filter inactive below min_trades");
    }

    #[test]
    fn session_best_tps_updates_on_record_trade() {
        let cfg = downtrend_filter_test_cfg();
        let mut tracker = TokenTracker::new("mint", "pool", "dex", 1, 0);
        assert_eq!(tracker.session_best_tps, f64::MAX);

        record_trade_implied_tps(&mut tracker, &cfg, 10, 100.0, true, "a");
        assert!((tracker.session_best_tps - 100.0).abs() < 1e-6);

        record_trade_implied_tps(&mut tracker, &cfg, 11, 80.0, true, "b");
        assert!((tracker.session_best_tps - 80.0).abs() < 1e-6);

        tracker.record_trade("c", true, 0, 1_000, "bad", 0, &cfg);
        assert!((tracker.session_best_tps - 80.0).abs() < 1e-6);
    }

    fn relaxed_entry_gates_helper_cfg() -> MomentumConfig {
        let mut cfg = MomentumConfig::default();
        cfg.early_min_liquidity_sol = 0.0;
        cfg.min_unique_buyers = 0;
        cfg.min_trades_per_min = 0.0;
        cfg.min_buy_dominance = 0.0;
        cfg.min_sol_inflow_lamports = 0;
        cfg.require_mint_authority_renounced = false;
        cfg.require_freeze_authority_none = false;
        cfg.top1_buyer_share_cap = 1.0;
        cfg.top3_buyer_share_cap = 1.0;
        cfg.repeat_buyer_min_ratio = 0.0;
        cfg.min_trade_size_lamports = 0;
        cfg.small_buy_ratio_cap = 1.0;
        cfg.dump_recovery_window_secs = 0;
        cfg.price_trend_filter_enabled = false;
        cfg.min_token_age_secs = 0;
        cfg
    }

    #[test]
    fn buy_dominance_uses_buyer_window_not_lifetime() {
        let mut cfg = relaxed_entry_gates_helper_cfg();
        cfg.buyer_window_secs = 60;
        cfg.min_buy_dominance = 0.0;

        let mut tracker = TokenTracker::new("mint", "pool", "dex", 1, 10_000_000_000);
        let head = 10_000u64;
        let window_slots = momentum_secs_to_slot_span(cfg.buyer_window_secs);

        // Lifetime-heavy buys outside the buyer window.
        for i in 0..20 {
            tracker.record_trade(
                &format!("old_buy{i}"),
                true,
                100_000_000,
                1,
                &format!("ob{i}"),
                head.saturating_sub(window_slots + 100 + i as u64),
                &cfg,
            );
        }
        // Recent sells dominate the buyer window.
        for i in 0..8 {
            tracker.record_trade(
                &format!("recent_sell{i}"),
                false,
                50_000_000,
                1,
                &format!("rs{i}"),
                head.saturating_sub(i as u64),
                &cfg,
            );
        }

        let metrics = tracker.calculate_metrics(&cfg, head);
        assert!(
            metrics.buy_dominance < 0.2,
            "window dominance should be low (sells in window), got {}",
            metrics.buy_dominance
        );
        let lifetime_dom =
            tracker.buy_count as f64 / (tracker.buy_count + tracker.sell_count) as f64;
        assert!(
            lifetime_dom > 0.6,
            "lifetime dominance should stay high for contrast, got {lifetime_dom}"
        );
    }

    #[test]
    fn trades_per_min_uses_buyer_window_not_lifetime() {
        let mut cfg = relaxed_entry_gates_helper_cfg();
        cfg.buyer_window_secs = 60;

        let mut tracker = TokenTracker::new("mint", "pool", "dex", 1, 10_000_000_000);
        let head = 20_000u64;
        let window_slots = momentum_secs_to_slot_span(cfg.buyer_window_secs);

        // Burst outside buyer window.
        for i in 0..50 {
            tracker.record_trade(
                &format!("burst{i}"),
                true,
                10_000_000,
                1,
                &format!("b{i}"),
                head.saturating_sub(window_slots + 200 + i as u64),
                &cfg,
            );
        }
        // Only two trades inside buyer window.
        tracker.record_trade("in1", true, 10_000_000, 1, "in1", head - 1, &cfg);
        tracker.record_trade("in2", true, 10_000_000, 1, "in2", head, &cfg);

        let metrics = tracker.calculate_metrics(&cfg, head);
        let expected = 2.0 / (cfg.buyer_window_secs as f64 / 60.0);
        assert!(
            (metrics.trades_per_min - expected).abs() < 0.01,
            "expected ~{expected} trades/min, got {}",
            metrics.trades_per_min
        );
    }

    #[test]
    fn token_age_blocks_when_young() {
        let mut cfg = relaxed_entry_gates_helper_cfg();
        cfg.min_token_age_secs = 60;

        let first_slot = 423_500_100_u64;
        let head = first_slot + 85; // ~34s @ 400ms/slot
        let mut tracker = TokenTracker::new("mint", "pool", "dex", first_slot, 10_000_000_000);

        let (ok, reason) = tracker.should_generate_intent(&cfg, None, head, Instant::now());
        assert!(!ok);
        assert!(reason.contains("WAIT_TOKEN_AGE"), "{reason}");
        assert!(reason.contains("34s"), "{reason}");
    }

    #[test]
    fn token_age_allows_when_old_enough() {
        let mut cfg = relaxed_entry_gates_helper_cfg();
        cfg.min_token_age_secs = 60;

        let first_slot = 1_000u64;
        let head = first_slot + momentum_secs_to_slot_span(60);
        let mut tracker = TokenTracker::new("mint", "pool", "dex", first_slot, 10_000_000_000);

        let (ok, reason) = tracker.should_generate_intent(&cfg, None, head, Instant::now());
        assert!(ok, "expected pass when chain age >= min, got: {reason}");
    }

    #[test]
    fn token_age_zero_disables_gate() {
        let mut cfg = relaxed_entry_gates_helper_cfg();
        cfg.min_token_age_secs = 0;

        let mut tracker = TokenTracker::new("mint", "pool", "dex", 1_000, 10_000_000_000);
        let (ok, reason) = tracker.should_generate_intent(&cfg, None, 1_001, Instant::now());
        assert!(ok, "gate disabled: {reason}");
    }

    #[test]
    fn token_age_fallback_first_seen() {
        let mut cfg = relaxed_entry_gates_helper_cfg();
        cfg.min_token_age_secs = 60;

        let mut tracker = TokenTracker::new("mint", "pool", "dex", 0, 10_000_000_000);
        tracker.first_seen = Instant::now() - Duration::from_secs(30);

        let (ok, reason) = tracker.should_generate_intent(&cfg, None, 0, Instant::now());
        assert!(!ok);
        assert!(reason.contains("WAIT_TOKEN_AGE"), "{reason}");
        assert!(reason.contains("~wallclock"), "{reason}");
    }

    #[test]
    fn dev_sell_pre_entry_waits_not_rejects() {
        let mut cfg = relaxed_entry_gates_helper_cfg();
        cfg.dev_sell_revalidation_delay_secs = 30;

        let mut tracker = TokenTracker::new("mint", "pool", "dex", 1, 10_000_000_000);
        tracker.dev_wallet = Some("dev".to_string());
        tracker.record_trade("dev", false, 1_000, 1, "sig", 50, &cfg);
        assert!(tracker.dev_sold);
        assert!(tracker.dev_sell_observed_at.is_some());

        let (should_trade, reason) =
            tracker.should_generate_intent(&cfg, None, 50_000, Instant::now());
        assert!(!should_trade);
        assert!(reason.contains("WAIT_DEV_SELL_REVALIDATION"), "{reason}");
        assert!(!tracker.is_rejected());
    }

    #[test]
    fn dev_sell_after_delay_requires_fresh_filter_pass() {
        let mut cfg = relaxed_entry_gates_helper_cfg();
        cfg.dev_sell_revalidation_delay_secs = 1;
        cfg.min_buy_dominance = 0.60;
        cfg.buyer_window_secs = 120;

        let mut tracker = TokenTracker::new("mint", "pool", "dex", 1, 10_000_000_000);
        tracker.dev_wallet = Some("dev".to_string());

        // Green window before dev sell.
        for i in 0..5 {
            tracker.record_trade(
                &format!("b{i}"),
                true,
                100_000_000,
                1,
                &format!("pb{i}"),
                100 + i,
                &cfg,
            );
        }
        tracker.record_trade("dev", false, 1_000, 1, "ds", 110, &cfg);
        tracker.dev_sell_observed_at = Some(Instant::now() - Duration::from_secs(2));

        // After delay: only sells in buyer window → WAIT_BUYER_WINDOW, not entry.
        for i in 0..6 {
            tracker.record_trade(
                &format!("s{i}"),
                false,
                50_000_000,
                1,
                &format!("ps{i}"),
                200 + i,
                &cfg,
            );
        }

        let (should_trade, reason) =
            tracker.should_generate_intent(&cfg, None, 300, Instant::now());
        assert!(!should_trade);
        assert!(reason.contains("WAIT_BUYER_WINDOW"), "{reason}");
        assert!(!tracker.is_rejected());
    }

    #[test]
    fn dev_sell_resets_delay_on_resell() {
        let mut cfg = relaxed_entry_gates_helper_cfg();
        cfg.dev_sell_revalidation_delay_secs = 30;

        let mut tracker = TokenTracker::new("mint", "pool", "dex", 1, 10_000_000_000);
        tracker.dev_wallet = Some("dev".to_string());

        tracker.record_trade("dev", false, 1_000, 1, "ds1", 50, &cfg);
        tracker.dev_sell_observed_at = Some(Instant::now() - Duration::from_secs(25));

        // Second dev sell before delay ends → timer reset.
        tracker.record_trade("dev", false, 1_000, 1, "ds2", 51, &cfg);

        let (should_trade, reason) =
            tracker.should_generate_intent(&cfg, None, 50_000, Instant::now());
        assert!(!should_trade);
        assert!(reason.contains("WAIT_DEV_SELL_REVALIDATION"), "{reason}");
        let observed = tracker.dev_sell_observed_at.expect("observed");
        assert!(observed.elapsed().as_secs() < 5);
    }

    #[tokio::test]
    async fn emitted_buy_intent_includes_reason_metadata_and_stable_source() {
        let mut cfg = MomentumConfig::default();

        // Disable all entry gates so we can emit an intent deterministically.
        cfg.early_min_liquidity_sol = 0.0;
        cfg.min_unique_buyers = 0;
        cfg.min_trades_per_min = 0.0;
        cfg.min_buy_dominance = 0.0;
        cfg.min_sol_inflow_lamports = 0;
        cfg.require_mint_authority_renounced = false;
        cfg.require_freeze_authority_none = false;
        cfg.top1_buyer_share_cap = 1.0;
        cfg.top3_buyer_share_cap = 1.0;
        cfg.repeat_buyer_min_ratio = 0.0;
        cfg.min_trade_size_lamports = 0;
        cfg.small_buy_ratio_cap = 1.0;
        cfg.min_token_age_secs = 0;

        cfg.default_position_lamports = 1_000;

        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context_with_intent_worker(jsonl_writer, cfg.clone());

        // Seed tracker with a last_trade_ratio so intent generation can compute min_out.
        {
            let mut trackers = ctx.token_trackers.write();
            let mut tracker = TokenTracker::new("mint", "pool", "dex", 1, 0);
            tracker.record_trade("w", true, 1_000_000_000, 1_000_000, "sig", 1, &cfg);
            trackers.insert(
                MomentumContext::tracker_storage_key("mint", "pool"),
                tracker,
            );
        }

        // Seed MintInfo so intent generation has decimals.
        {
            let mut infos = ctx.mint_infos.write();
            infos.insert(
                "mint".to_string(),
                MintInfo {
                    token_program: "spl-token".to_string(),
                    decimals: 6,
                    supply: 0,
                    mint_authority: None,
                    freeze_authority: None,
                    last_updated: Instant::now(),
                },
            );
        }

        let signal = EntrySignal {
            mint: "mint".to_string(),
            pool: "pool".to_string(),
            dex: "dex".to_string(),
            sol_amount: 250,
            kind: EntryKind::Probe,
            reason: "ENTER_PROBE_BUY: test".to_string(),
        };

        generate_and_publish_buy_intent(ctx.as_ref(), &signal)
            .await
            .expect("buy intent");
        wait_for_intent_jsonl_records(ctx.as_ref(), 1).await;

        let path = ctx
            .jsonl_writer
            .current_path()
            .expect("jsonl path should exist after write");
        let content = std::fs::read_to_string(path).expect("read jsonl");
        let line = content.lines().last().expect("at least one line");
        let intent: TradeIntent = serde_json::from_str(line).expect("parse TradeIntent");

        assert_eq!(intent.source, "momentum-bot");
        assert_eq!(
            intent.metadata.get("reason_code").map(|s| s.as_str()),
            Some("ENTER_PROBE_BUY")
        );
        assert_eq!(
            intent.metadata.get("reason_detail").map(|s| s.as_str()),
            Some("ENTER_PROBE_BUY: test")
        );
    }

    #[test]
    fn post_entry_dev_sell_triggers_hard_exit() {
        let mut cfg = MomentumConfig::default();

        // Make sure normal exits don't trigger during the test.
        cfg.hard_stop_loss_pct = 1_000.0;
        cfg.take_profit_pct = 1_000.0;
        cfg.trailing_stop_pct = 1_000.0;
        cfg.trailing_activation_pct = 1_000.0;
        cfg.max_hold_time_secs = 999_999;
        cfg.momentum_exit_buy_ratio = 0.0;
        cfg.momentum_exit_window_secs = 30;
        cfg.momentum_exit_min_trades = 999_999;

        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());

        let ctx = MomentumContext {
            run_id: "run-test".to_string(),
            config: parking_lot::RwLock::new(cfg.clone()),
            nats: None,
            jsonl_writer,
            intent_publish_tx: parking_lot::Mutex::new(Some(noop_intent_publish_tx())),
            intent_counter: std::sync::atomic::AtomicU64::new(0),
            pool_first_seen: parking_lot::RwLock::new(std::collections::HashMap::new()),
            pending_dev_info: parking_lot::RwLock::new(HashMap::new()),
            pending_pool_accounts: parking_lot::RwLock::new(HashMap::new()),
            mint_infos: parking_lot::RwLock::new(HashMap::new()),
            token_trackers: parking_lot::RwLock::new(HashMap::new()),
            tracker_keys_by_mint: parking_lot::RwLock::new(HashMap::new()),
            dirty_entry_tracker_keys: parking_lot::RwLock::new(BTreeSet::new()),
            pumpfun_buy_missing_creator_suppress_until: parking_lot::RwLock::new(HashMap::new()),
            positions: parking_lot::RwLock::new(HashMap::new()),
            pending_intents: parking_lot::RwLock::new(HashMap::new()),
            latest_bonding_by_mint: parking_lot::RwLock::new(HashMap::new()),
            latest_pumpfun_migration_complete_by_mint: parking_lot::RwLock::new(HashMap::new()),
            latest_pool_reserve_price_hint_by_mint_pool: parking_lot::RwLock::new(HashMap::new()),
            pending_buy_entries: parking_lot::RwLock::new(HashMap::new()),
            pending_buy_mint_index: parking_lot::RwLock::new(HashMap::new()),
            mint_pools: parking_lot::RwLock::new(HashMap::new()),
            live_pool_cache: LivePoolCache::new(),
            open_position_pool_recovery_last_sent: parking_lot::RwLock::new(HashMap::new()),
            position_kv: tokio::sync::OnceCell::new(),
            orphaned_mints: parking_lot::RwLock::new(HashMap::new()),
            orphaned_recovered_intent_ids: parking_lot::RwLock::new(BoundedIntentIdCache::new(
                ORPHANED_RECOVERED_INTENT_IDS_CAP,
            )),
            latest_wallet_balance_raw_by_mint: parking_lot::RwLock::new(HashMap::new()),
            tokens_tracked: std::sync::atomic::AtomicU64::new(0),
            tokens_blacklisted: std::sync::atomic::AtomicU64::new(0),
            blacklisted_mints_for_metric: parking_lot::RwLock::new(HashSet::new()),
            intents_generated: std::sync::atomic::AtomicU64::new(0),
            exits_generated: std::sync::atomic::AtomicU64::new(0),
            last_event_slot: std::sync::atomic::AtomicU64::new(0),
            last_event_ts_ms: std::sync::atomic::AtomicU64::new(0),
            momentum_active_pool_publish_queue: parking_lot::Mutex::new(
                MomentumActivePoolPublishQueue::default(),
            ),
        };

        // Open a position.
        {
            let mut positions = ctx.positions.write();
            let mut pos = PositionTracker::new("mint", "pool", "dex", 1.0, 6, 123, 1_000);
            pos.entry_time = Instant::now() - Duration::from_secs(10);
            pos.entry_confirmed_slot = 100;
            pos.last_price_slot = 150;
            positions.insert("mint".to_string(), pos);
        }

        // Record a dev sell after entry.
        {
            let mut trackers = ctx.token_trackers.write();
            let mut tracker = TokenTracker::new("mint", "pool", "dex", 1, 0);
            tracker.dev_wallet = Some("dev".to_string());
            tracker.record_trade("dev", false, 2_000_000_000, 1, "devsig", 200, &cfg);
            trackers.insert(
                MomentumContext::tracker_storage_key("mint", "pool"),
                tracker,
            );
        }
        ctx.last_event_slot
            .store(250, std::sync::atomic::Ordering::Relaxed);

        let exits = ctx.check_for_exits();
        assert_eq!(exits.len(), 1);
        assert_eq!(exits[0].0, "mint");
        assert_eq!(exits[0].3, "DEV_SELL");
        assert!(exits[0].4.contains("Dev sold post-entry"));
    }

    #[test]
    fn post_entry_lp_removal_triggers_hard_exit() {
        let mut cfg = MomentumConfig::default();

        // Make sure normal exits don't trigger during the test.
        cfg.hard_stop_loss_pct = 1_000.0;
        cfg.take_profit_pct = 1_000.0;
        cfg.trailing_stop_pct = 1_000.0;
        cfg.trailing_activation_pct = 1_000.0;
        cfg.max_hold_time_secs = 999_999;
        cfg.momentum_exit_buy_ratio = 0.0;
        cfg.momentum_exit_window_secs = 30;
        cfg.momentum_exit_min_trades = 999_999;

        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());

        let ctx = MomentumContext {
            run_id: "run-test".to_string(),
            config: parking_lot::RwLock::new(cfg.clone()),
            nats: None,
            jsonl_writer,
            intent_publish_tx: parking_lot::Mutex::new(Some(noop_intent_publish_tx())),
            intent_counter: std::sync::atomic::AtomicU64::new(0),
            pool_first_seen: parking_lot::RwLock::new(std::collections::HashMap::new()),
            pending_dev_info: parking_lot::RwLock::new(HashMap::new()),
            pending_pool_accounts: parking_lot::RwLock::new(HashMap::new()),
            mint_infos: parking_lot::RwLock::new(HashMap::new()),
            token_trackers: parking_lot::RwLock::new(HashMap::new()),
            tracker_keys_by_mint: parking_lot::RwLock::new(HashMap::new()),
            dirty_entry_tracker_keys: parking_lot::RwLock::new(BTreeSet::new()),
            pumpfun_buy_missing_creator_suppress_until: parking_lot::RwLock::new(HashMap::new()),
            positions: parking_lot::RwLock::new(HashMap::new()),
            pending_intents: parking_lot::RwLock::new(HashMap::new()),
            latest_bonding_by_mint: parking_lot::RwLock::new(HashMap::new()),
            latest_pumpfun_migration_complete_by_mint: parking_lot::RwLock::new(HashMap::new()),
            latest_pool_reserve_price_hint_by_mint_pool: parking_lot::RwLock::new(HashMap::new()),
            pending_buy_entries: parking_lot::RwLock::new(HashMap::new()),
            pending_buy_mint_index: parking_lot::RwLock::new(HashMap::new()),
            mint_pools: parking_lot::RwLock::new(HashMap::new()),
            live_pool_cache: LivePoolCache::new(),
            open_position_pool_recovery_last_sent: parking_lot::RwLock::new(HashMap::new()),
            position_kv: tokio::sync::OnceCell::new(),
            orphaned_mints: parking_lot::RwLock::new(HashMap::new()),
            orphaned_recovered_intent_ids: parking_lot::RwLock::new(BoundedIntentIdCache::new(
                ORPHANED_RECOVERED_INTENT_IDS_CAP,
            )),
            latest_wallet_balance_raw_by_mint: parking_lot::RwLock::new(HashMap::new()),
            tokens_tracked: std::sync::atomic::AtomicU64::new(0),
            tokens_blacklisted: std::sync::atomic::AtomicU64::new(0),
            blacklisted_mints_for_metric: parking_lot::RwLock::new(HashSet::new()),
            intents_generated: std::sync::atomic::AtomicU64::new(0),
            exits_generated: std::sync::atomic::AtomicU64::new(0),
            last_event_slot: std::sync::atomic::AtomicU64::new(0),
            last_event_ts_ms: std::sync::atomic::AtomicU64::new(0),
            momentum_active_pool_publish_queue: parking_lot::Mutex::new(
                MomentumActivePoolPublishQueue::default(),
            ),
        };

        // Open a position.
        {
            let mut positions = ctx.positions.write();
            let mut pos = PositionTracker::new("mint", "pool", "dex", 1.0, 6, 123, 1_000);
            pos.entry_time = Instant::now() - Duration::from_secs(10);
            pos.entry_confirmed_slot = 10;
            pos.last_price_slot = 20;
            positions.insert("mint".to_string(), pos);
        }

        // Record LP removal after entry.
        {
            let mut trackers = ctx.token_trackers.write();
            let mut tracker = TokenTracker::new("mint", "pool", "dex", 1, 0);
            tracker.record_lp_removal(Some(50));
            trackers.insert(
                MomentumContext::tracker_storage_key("mint", "pool"),
                tracker,
            );
        }
        ctx.last_event_slot
            .store(60, std::sync::atomic::Ordering::Relaxed);

        let exits = ctx.check_for_exits();
        assert_eq!(exits.len(), 1);
        assert_eq!(exits[0].0, "mint");
        assert_eq!(exits[0].3, "LP_REMOVAL");
        assert!(
            exits[0].4.contains("LP removed post-entry"),
            "reason={}",
            exits[0].4
        );
    }

    #[test]
    fn timed_exit_reconcile_candidates_require_stale_exit_and_no_pending_sell() {
        let mut cfg = MomentumConfig::default();
        cfg.max_hold_time_secs = 60;

        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());

        let ctx = MomentumContext {
            run_id: "run-test".to_string(),
            config: parking_lot::RwLock::new(cfg),
            nats: None,
            jsonl_writer,
            intent_publish_tx: parking_lot::Mutex::new(Some(noop_intent_publish_tx())),
            intent_counter: std::sync::atomic::AtomicU64::new(0),
            pool_first_seen: parking_lot::RwLock::new(std::collections::HashMap::new()),
            pending_dev_info: parking_lot::RwLock::new(HashMap::new()),
            pending_pool_accounts: parking_lot::RwLock::new(HashMap::new()),
            mint_infos: parking_lot::RwLock::new(HashMap::new()),
            token_trackers: parking_lot::RwLock::new(HashMap::new()),
            tracker_keys_by_mint: parking_lot::RwLock::new(HashMap::new()),
            dirty_entry_tracker_keys: parking_lot::RwLock::new(BTreeSet::new()),
            pumpfun_buy_missing_creator_suppress_until: parking_lot::RwLock::new(HashMap::new()),
            positions: parking_lot::RwLock::new(HashMap::new()),
            pending_intents: parking_lot::RwLock::new(HashMap::new()),
            latest_bonding_by_mint: parking_lot::RwLock::new(HashMap::new()),
            latest_pumpfun_migration_complete_by_mint: parking_lot::RwLock::new(HashMap::new()),
            latest_pool_reserve_price_hint_by_mint_pool: parking_lot::RwLock::new(HashMap::new()),
            pending_buy_entries: parking_lot::RwLock::new(HashMap::new()),
            pending_buy_mint_index: parking_lot::RwLock::new(HashMap::new()),
            mint_pools: parking_lot::RwLock::new(HashMap::new()),
            live_pool_cache: LivePoolCache::new(),
            open_position_pool_recovery_last_sent: parking_lot::RwLock::new(HashMap::new()),
            position_kv: tokio::sync::OnceCell::new(),
            orphaned_mints: parking_lot::RwLock::new(HashMap::new()),
            orphaned_recovered_intent_ids: parking_lot::RwLock::new(BoundedIntentIdCache::new(
                ORPHANED_RECOVERED_INTENT_IDS_CAP,
            )),
            latest_wallet_balance_raw_by_mint: parking_lot::RwLock::new(HashMap::new()),
            tokens_tracked: std::sync::atomic::AtomicU64::new(0),
            tokens_blacklisted: std::sync::atomic::AtomicU64::new(0),
            blacklisted_mints_for_metric: parking_lot::RwLock::new(HashSet::new()),
            intents_generated: std::sync::atomic::AtomicU64::new(0),
            exits_generated: std::sync::atomic::AtomicU64::new(0),
            last_event_slot: std::sync::atomic::AtomicU64::new(0),
            last_event_ts_ms: std::sync::atomic::AtomicU64::new(0),
            momentum_active_pool_publish_queue: parking_lot::Mutex::new(
                MomentumActivePoolPublishQueue::default(),
            ),
        };

        let now = Instant::now();
        {
            let mut positions = ctx.positions.write();
            let mut pos = PositionTracker::new("mint", "pool", "dex", 1.0, 6, 100, 1_000);
            pos.entry_time = now - Duration::from_secs(120);
            pos.exit_generated = true;
            pos.exit_generated_at = Some(now - Duration::from_secs(120));
            positions.insert("mint".to_string(), pos);
        }

        let candidates = ctx.collect_timed_exit_reconcile_candidates(now);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].mint, "mint");

        // Add a pending SELL for the same mint; should suppress reconcile.
        {
            let mut pending = ctx.pending_intents.write();
            pending.insert(
                "intent".to_string(),
                PendingIntent {
                    mint: "mint".to_string(),
                    pool: "pool".to_string(),
                    dex: "dex".to_string(),
                    side: TradeSide::Sell,
                    entry_kind: None,
                    sol_amount: 0,
                    token_amount: 100,
                    created_at: now,
                },
            );
        }

        let candidates_after = ctx.collect_timed_exit_reconcile_candidates(now);
        assert!(candidates_after.is_empty());
    }

    /// Scope 50: probe + scale-in aggregate; scale-in clears exit latch; exit intent uses live total.
    #[tokio::test]
    async fn scale_in_resets_exit_latch_and_exit_intent_uses_combined_position_amount() {
        let mut cfg = MomentumConfig::default();
        cfg.hard_stop_loss_pct = 1_000.0;
        cfg.take_profit_pct = 1_000.0;
        cfg.max_hold_time_secs = 999_999;

        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context_with_intent_worker(jsonl_writer, cfg);

        ctx.register_pool("mintX", "poolX", "raydium", 1);
        ctx.update_pool_trade_data("mintX", "poolX", "raydium", 1_000_000_000, 1_000_000, 1);
        ctx.update_pool_accounts("mintX", "poolX", vec!["a0".to_string(), "a1".to_string()]);

        ctx.open_position(OpenPositionParams {
            mint: "mintX",
            pool: "poolX",
            dex: "raydium",
            entry_price: 1.0,
            token_decimals: 6,
            token_amount: 14_099_749_285,
            sol_invested: 1_000_000,
            token_program: None,
            creator: None,
            entry_confirmed_slot: 100,
            initial_bonding: None,
        });
        ctx.mark_exit_generated("mintX");
        assert!(ctx.positions.read().get("mintX").unwrap().exit_generated);

        ctx.open_position(OpenPositionParams {
            mint: "mintX",
            pool: "poolX",
            dex: "raydium",
            entry_price: 1.0,
            token_decimals: 6,
            token_amount: 38_711_432_312,
            sol_invested: 2_000_000,
            token_program: None,
            creator: None,
            entry_confirmed_slot: 200,
            initial_bonding: None,
        });
        {
            let pos = ctx.positions.read().get("mintX").unwrap().clone();
            assert_eq!(
                pos.token_amount,
                14_099_749_285u64.saturating_add(38_711_432_312)
            );
            assert!(
                !pos.exit_generated,
                "scale-in must clear exit latch so a new exit sizes to full total"
            );
        }

        generate_and_publish_exit_intent(
            ctx.as_ref(),
            "mintX",
            "poolX",
            "raydium",
            "TIME_EXIT",
            "test",
            14_099_749_285,
            None,
        )
        .await
        .expect("exit intent");
        wait_for_intent_jsonl_records(ctx.as_ref(), 1).await;

        let path = ctx.jsonl_writer.current_path().expect("jsonl path");
        let content = std::fs::read_to_string(path).expect("read jsonl");
        let line = content.lines().last().expect("intent line");
        let intent: TradeIntent = serde_json::from_str(line).expect("parse TradeIntent");
        assert_eq!(
            intent.required_capital.raw,
            14_099_749_285u64.saturating_add(38_711_432_312),
            "exit must sell full tracked position, not stale probe hint"
        );
    }

    /// Scope 50: confirmed SELL smaller than tracked total reduces position and clears exit latch.
    #[tokio::test]
    async fn partial_sell_confirmed_reduces_tracker_and_timed_reconcile_uses_residual_amount() {
        let mut cfg = MomentumConfig::default();
        cfg.max_hold_time_secs = 60;

        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());

        let ctx = Arc::new(MomentumContext {
            run_id: "run-test".to_string(),
            config: parking_lot::RwLock::new(cfg.clone()),
            nats: None,
            jsonl_writer,
            intent_publish_tx: parking_lot::Mutex::new(Some(noop_intent_publish_tx())),
            intent_counter: std::sync::atomic::AtomicU64::new(0),
            pool_first_seen: parking_lot::RwLock::new(std::collections::HashMap::new()),
            pending_dev_info: parking_lot::RwLock::new(HashMap::new()),
            pending_pool_accounts: parking_lot::RwLock::new(HashMap::new()),
            mint_infos: parking_lot::RwLock::new(HashMap::new()),
            token_trackers: parking_lot::RwLock::new(HashMap::new()),
            tracker_keys_by_mint: parking_lot::RwLock::new(HashMap::new()),
            dirty_entry_tracker_keys: parking_lot::RwLock::new(BTreeSet::new()),
            pumpfun_buy_missing_creator_suppress_until: parking_lot::RwLock::new(HashMap::new()),
            positions: parking_lot::RwLock::new(HashMap::new()),
            pending_intents: parking_lot::RwLock::new(HashMap::new()),
            latest_bonding_by_mint: parking_lot::RwLock::new(HashMap::new()),
            latest_pumpfun_migration_complete_by_mint: parking_lot::RwLock::new(HashMap::new()),
            latest_pool_reserve_price_hint_by_mint_pool: parking_lot::RwLock::new(HashMap::new()),
            pending_buy_entries: parking_lot::RwLock::new(HashMap::new()),
            pending_buy_mint_index: parking_lot::RwLock::new(HashMap::new()),
            mint_pools: parking_lot::RwLock::new(HashMap::new()),
            live_pool_cache: LivePoolCache::new(),
            open_position_pool_recovery_last_sent: parking_lot::RwLock::new(HashMap::new()),
            position_kv: tokio::sync::OnceCell::new(),
            orphaned_mints: parking_lot::RwLock::new(HashMap::new()),
            orphaned_recovered_intent_ids: parking_lot::RwLock::new(BoundedIntentIdCache::new(
                ORPHANED_RECOVERED_INTENT_IDS_CAP,
            )),
            latest_wallet_balance_raw_by_mint: parking_lot::RwLock::new(HashMap::new()),
            tokens_tracked: std::sync::atomic::AtomicU64::new(0),
            tokens_blacklisted: std::sync::atomic::AtomicU64::new(0),
            blacklisted_mints_for_metric: parking_lot::RwLock::new(HashSet::new()),
            intents_generated: std::sync::atomic::AtomicU64::new(0),
            exits_generated: std::sync::atomic::AtomicU64::new(0),
            last_event_slot: std::sync::atomic::AtomicU64::new(0),
            last_event_ts_ms: std::sync::atomic::AtomicU64::new(0),
            momentum_active_pool_publish_queue: parking_lot::Mutex::new(
                MomentumActivePoolPublishQueue::default(),
            ),
        });

        ctx.open_position(OpenPositionParams {
            mint: "mintY",
            pool: "poolY",
            dex: "raydium",
            entry_price: 1.0,
            token_decimals: 6,
            token_amount: 52_811_181_597,
            sol_invested: 1_000_000,
            token_program: None,
            creator: None,
            entry_confirmed_slot: 0,
            initial_bonding: None,
        });
        ctx.mark_exit_generated("mintY");

        let probe_only = 14_099_749_285u64;
        ctx.register_sell_intent("sell-y-1", "mintY", "poolY", "raydium", probe_only);

        let mut meta = std::collections::HashMap::new();
        meta.insert("side".to_string(), "SELL".to_string());
        let result = ExecutionResult {
            header: RecordHeader::new("test", BUILD_VERSION, "run-test"),
            execution_id: "ex1".to_string(),
            decision_id: "dec1".to_string(),
            intent_id: "sell-y-1".to_string(),
            source: "momentum-bot".to_string(),
            token_mint: Some("mintY".to_string()),
            signature: Some("sig1".to_string()),
            bundle_id: None,
            status: ExecutionStatus::Confirmed,
            fill_in: Some(ExplicitAmount::new(probe_only, 6)),
            fill_out: Some(ExplicitAmount::new(50_000_000, 9)),
            fill_status: Some(FillStatus::Complete),
            fill_unavailable_reason: None,
            confirmed_slot: Some(1),
            block_time_unix_ms: None,
            fees: None,
            pnl: None,
            wallet_sol_delta_lamports: Some(50_000_000),
            error_message: None,
            error_code: None,
            latency_ms: Some(1),
            metadata: meta,
        };

        Arc::clone(&ctx).handle_execution_result(&result);

        let pos = ctx.positions.read().get("mintY").expect("position").clone();
        let expected_remaining = 52_811_181_597u64.saturating_sub(probe_only);
        assert_eq!(pos.token_amount, expected_remaining);
        assert!(!pos.exit_generated);

        let now = Instant::now();
        {
            let mut w = ctx.positions.write();
            let p = w.get_mut("mintY").unwrap();
            p.entry_time = now - Duration::from_secs(120);
            p.exit_generated = true;
            p.exit_generated_at = Some(now - Duration::from_secs(120));
        }

        let candidates = ctx.collect_timed_exit_reconcile_candidates(now);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].token_amount, expected_remaining);
    }

    /// Scope 50: SELL failure (e.g. resource lock) must clear exit latch for retries.
    #[tokio::test]
    async fn sell_execution_failure_resets_exit_generated() {
        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());

        let ctx = Arc::new(MomentumContext {
            run_id: "run-test".to_string(),
            config: parking_lot::RwLock::new(MomentumConfig::default()),
            nats: None,
            jsonl_writer,
            intent_publish_tx: parking_lot::Mutex::new(Some(noop_intent_publish_tx())),
            intent_counter: std::sync::atomic::AtomicU64::new(0),
            pool_first_seen: parking_lot::RwLock::new(std::collections::HashMap::new()),
            pending_dev_info: parking_lot::RwLock::new(HashMap::new()),
            pending_pool_accounts: parking_lot::RwLock::new(HashMap::new()),
            mint_infos: parking_lot::RwLock::new(HashMap::new()),
            token_trackers: parking_lot::RwLock::new(HashMap::new()),
            tracker_keys_by_mint: parking_lot::RwLock::new(HashMap::new()),
            dirty_entry_tracker_keys: parking_lot::RwLock::new(BTreeSet::new()),
            pumpfun_buy_missing_creator_suppress_until: parking_lot::RwLock::new(HashMap::new()),
            positions: parking_lot::RwLock::new(HashMap::new()),
            pending_intents: parking_lot::RwLock::new(HashMap::new()),
            latest_bonding_by_mint: parking_lot::RwLock::new(HashMap::new()),
            latest_pumpfun_migration_complete_by_mint: parking_lot::RwLock::new(HashMap::new()),
            latest_pool_reserve_price_hint_by_mint_pool: parking_lot::RwLock::new(HashMap::new()),
            pending_buy_entries: parking_lot::RwLock::new(HashMap::new()),
            pending_buy_mint_index: parking_lot::RwLock::new(HashMap::new()),
            mint_pools: parking_lot::RwLock::new(HashMap::new()),
            live_pool_cache: LivePoolCache::new(),
            open_position_pool_recovery_last_sent: parking_lot::RwLock::new(HashMap::new()),
            position_kv: tokio::sync::OnceCell::new(),
            orphaned_mints: parking_lot::RwLock::new(HashMap::new()),
            orphaned_recovered_intent_ids: parking_lot::RwLock::new(BoundedIntentIdCache::new(
                ORPHANED_RECOVERED_INTENT_IDS_CAP,
            )),
            latest_wallet_balance_raw_by_mint: parking_lot::RwLock::new(HashMap::new()),
            tokens_tracked: std::sync::atomic::AtomicU64::new(0),
            tokens_blacklisted: std::sync::atomic::AtomicU64::new(0),
            blacklisted_mints_for_metric: parking_lot::RwLock::new(HashSet::new()),
            intents_generated: std::sync::atomic::AtomicU64::new(0),
            exits_generated: std::sync::atomic::AtomicU64::new(0),
            last_event_slot: std::sync::atomic::AtomicU64::new(0),
            last_event_ts_ms: std::sync::atomic::AtomicU64::new(0),
            momentum_active_pool_publish_queue: parking_lot::Mutex::new(
                MomentumActivePoolPublishQueue::default(),
            ),
        });

        ctx.open_position(OpenPositionParams {
            mint: "mintZ",
            pool: "poolZ",
            dex: "raydium",
            entry_price: 1.0,
            token_decimals: 6,
            token_amount: 1_000,
            sol_invested: 1,
            token_program: None,
            creator: None,
            entry_confirmed_slot: 0,
            initial_bonding: None,
        });
        ctx.mark_exit_generated("mintZ");
        ctx.register_sell_intent("sell-z-1", "mintZ", "poolZ", "raydium", 1_000);

        let mut meta = std::collections::HashMap::new();
        meta.insert("side".to_string(), "SELL".to_string());
        let result = ExecutionResult {
            header: RecordHeader::new("test", BUILD_VERSION, "run-test"),
            execution_id: "exz".to_string(),
            decision_id: "decz".to_string(),
            intent_id: "sell-z-1".to_string(),
            source: "momentum-bot".to_string(),
            token_mint: Some("mintZ".to_string()),
            signature: None,
            bundle_id: None,
            status: ExecutionStatus::Failed,
            fill_in: None,
            fill_out: None,
            fill_status: None,
            fill_unavailable_reason: None,
            confirmed_slot: None,
            block_time_unix_ms: None,
            fees: None,
            pnl: None,
            wallet_sol_delta_lamports: None,
            error_message: Some("pool locked by int-other".to_string()),
            error_code: None,
            latency_ms: None,
            metadata: meta,
        };

        Arc::clone(&ctx).handle_execution_result(&result);
        let pos = ctx.positions.read().get("mintZ").unwrap().clone();
        assert!(!pos.exit_generated);
        assert_eq!(pos.token_amount, 1_000);
    }

    #[test]
    fn integration_style_wallet_snapshot_reconcile_creates_retry_candidate() {
        let mut cfg = MomentumConfig::default();
        cfg.max_hold_time_secs = 60;

        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());

        let ctx = MomentumContext {
            run_id: "run-test".to_string(),
            config: parking_lot::RwLock::new(cfg.clone()),
            nats: None,
            jsonl_writer,
            intent_publish_tx: parking_lot::Mutex::new(Some(noop_intent_publish_tx())),
            intent_counter: std::sync::atomic::AtomicU64::new(0),
            pool_first_seen: parking_lot::RwLock::new(std::collections::HashMap::new()),
            pending_dev_info: parking_lot::RwLock::new(HashMap::new()),
            pending_pool_accounts: parking_lot::RwLock::new(HashMap::new()),
            mint_infos: parking_lot::RwLock::new(HashMap::new()),
            token_trackers: parking_lot::RwLock::new(HashMap::new()),
            tracker_keys_by_mint: parking_lot::RwLock::new(HashMap::new()),
            dirty_entry_tracker_keys: parking_lot::RwLock::new(BTreeSet::new()),
            pumpfun_buy_missing_creator_suppress_until: parking_lot::RwLock::new(HashMap::new()),
            positions: parking_lot::RwLock::new(HashMap::new()),
            pending_intents: parking_lot::RwLock::new(HashMap::new()),
            latest_bonding_by_mint: parking_lot::RwLock::new(HashMap::new()),
            latest_pumpfun_migration_complete_by_mint: parking_lot::RwLock::new(HashMap::new()),
            latest_pool_reserve_price_hint_by_mint_pool: parking_lot::RwLock::new(HashMap::new()),
            pending_buy_entries: parking_lot::RwLock::new(HashMap::new()),
            pending_buy_mint_index: parking_lot::RwLock::new(HashMap::new()),
            mint_pools: parking_lot::RwLock::new(HashMap::new()),
            live_pool_cache: LivePoolCache::new(),
            open_position_pool_recovery_last_sent: parking_lot::RwLock::new(HashMap::new()),
            position_kv: tokio::sync::OnceCell::new(),
            orphaned_mints: parking_lot::RwLock::new(HashMap::new()),
            orphaned_recovered_intent_ids: parking_lot::RwLock::new(BoundedIntentIdCache::new(
                ORPHANED_RECOVERED_INTENT_IDS_CAP,
            )),
            latest_wallet_balance_raw_by_mint: parking_lot::RwLock::new(HashMap::new()),
            tokens_tracked: std::sync::atomic::AtomicU64::new(0),
            tokens_blacklisted: std::sync::atomic::AtomicU64::new(0),
            blacklisted_mints_for_metric: parking_lot::RwLock::new(HashSet::new()),
            intents_generated: std::sync::atomic::AtomicU64::new(0),
            exits_generated: std::sync::atomic::AtomicU64::new(0),
            last_event_slot: std::sync::atomic::AtomicU64::new(0),
            last_event_ts_ms: std::sync::atomic::AtomicU64::new(0),
            momentum_active_pool_publish_queue: parking_lot::Mutex::new(
                MomentumActivePoolPublishQueue::default(),
            ),
        };

        // Seed pool registry so reconciliation can pick a pool.
        ctx.register_pool("mint", "pool", "raydium", 1);
        ctx.update_pool_trade_data("mint", "pool", "raydium", 1_000_000_000, 1_000_000, 1);
        ctx.update_pool_accounts(
            "mint",
            "pool",
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
        );

        let mut reconciled = ctx
            .build_reconciled_position("mint", 1_000_000, 6)
            .expect("reconciled position");
        assert_eq!(reconciled.entry_source, PositionEntrySource::WalletSnapshot);
        assert!(reconciled.entry_time.elapsed().as_secs() >= cfg.max_hold_time_secs);

        let now = Instant::now();
        reconciled.exit_generated = true;
        reconciled.exit_generated_at = Some(now - Duration::from_secs(120));
        {
            let mut positions = ctx.positions.write();
            positions.insert("mint".to_string(), reconciled);
        }

        let candidates = ctx.collect_timed_exit_reconcile_candidates(now);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].mint, "mint");
    }

    /// PumpSwap BUY must not use partial account lists; `record_dex_pool_accounts` rejects them.
    #[test]
    fn pump_amm_dex_pool_accounts_short_list_not_cached() {
        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());

        let ctx = MomentumContext {
            run_id: "test".to_string(),
            config: parking_lot::RwLock::new(MomentumConfig::default()),
            nats: None,
            jsonl_writer,
            intent_publish_tx: parking_lot::Mutex::new(Some(noop_intent_publish_tx())),
            intent_counter: std::sync::atomic::AtomicU64::new(0),
            pool_first_seen: parking_lot::RwLock::new(std::collections::HashMap::new()),
            pending_dev_info: parking_lot::RwLock::new(HashMap::new()),
            pending_pool_accounts: parking_lot::RwLock::new(HashMap::new()),
            mint_infos: parking_lot::RwLock::new(HashMap::new()),
            token_trackers: parking_lot::RwLock::new(HashMap::new()),
            tracker_keys_by_mint: parking_lot::RwLock::new(HashMap::new()),
            dirty_entry_tracker_keys: parking_lot::RwLock::new(BTreeSet::new()),
            pumpfun_buy_missing_creator_suppress_until: parking_lot::RwLock::new(HashMap::new()),
            positions: parking_lot::RwLock::new(HashMap::new()),
            pending_intents: parking_lot::RwLock::new(HashMap::new()),
            latest_bonding_by_mint: parking_lot::RwLock::new(HashMap::new()),
            latest_pumpfun_migration_complete_by_mint: parking_lot::RwLock::new(HashMap::new()),
            latest_pool_reserve_price_hint_by_mint_pool: parking_lot::RwLock::new(HashMap::new()),
            pending_buy_entries: parking_lot::RwLock::new(HashMap::new()),
            pending_buy_mint_index: parking_lot::RwLock::new(HashMap::new()),
            mint_pools: parking_lot::RwLock::new(HashMap::new()),
            live_pool_cache: LivePoolCache::new(),
            open_position_pool_recovery_last_sent: parking_lot::RwLock::new(HashMap::new()),
            position_kv: tokio::sync::OnceCell::new(),
            orphaned_mints: parking_lot::RwLock::new(HashMap::new()),
            orphaned_recovered_intent_ids: parking_lot::RwLock::new(BoundedIntentIdCache::new(
                ORPHANED_RECOVERED_INTENT_IDS_CAP,
            )),
            latest_wallet_balance_raw_by_mint: parking_lot::RwLock::new(HashMap::new()),
            tokens_tracked: std::sync::atomic::AtomicU64::new(0),
            tokens_blacklisted: std::sync::atomic::AtomicU64::new(0),
            blacklisted_mints_for_metric: parking_lot::RwLock::new(HashSet::new()),
            intents_generated: std::sync::atomic::AtomicU64::new(0),
            exits_generated: std::sync::atomic::AtomicU64::new(0),
            last_event_slot: std::sync::atomic::AtomicU64::new(0),
            last_event_ts_ms: std::sync::atomic::AtomicU64::new(0),
            momentum_active_pool_publish_queue: parking_lot::Mutex::new(
                MomentumActivePoolPublishQueue::default(),
            ),
        };

        let mint = "TokenMint1111111111111111111111111111111111";
        let pool = "PoolPubkey1111111111111111111111111111111111";
        let wsol = "So11111111111111111111111111111111111111112";

        ctx.token_trackers.write().insert(
            MomentumContext::tracker_storage_key(mint, pool),
            TokenTracker::new(mint, pool, "pump_amm", 1, 0),
        );

        let short: Vec<String> = (0..5).map(|i| format!("A{i}")).collect();
        ctx.record_dex_pool_accounts("pump_amm", pool, mint, wsol, &short);

        assert!(
            ctx.try_get_dex_pool_accounts_for_mint_pool(mint, pool)
                .is_none(),
            "short PumpSwap account list must not satisfy BUY"
        );
        assert!(ctx.pending_pool_accounts.read().get(mint).is_none());
    }

    /// Full verified 14-account `DexPoolAccounts` row is required before BUY can resolve accounts.
    #[test]
    fn pump_amm_dex_pool_accounts_full_list_cached_for_buy() {
        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());

        let ctx = MomentumContext {
            run_id: "test".to_string(),
            config: parking_lot::RwLock::new(MomentumConfig::default()),
            nats: None,
            jsonl_writer,
            intent_publish_tx: parking_lot::Mutex::new(Some(noop_intent_publish_tx())),
            intent_counter: std::sync::atomic::AtomicU64::new(0),
            pool_first_seen: parking_lot::RwLock::new(std::collections::HashMap::new()),
            pending_dev_info: parking_lot::RwLock::new(HashMap::new()),
            pending_pool_accounts: parking_lot::RwLock::new(HashMap::new()),
            mint_infos: parking_lot::RwLock::new(HashMap::new()),
            token_trackers: parking_lot::RwLock::new(HashMap::new()),
            tracker_keys_by_mint: parking_lot::RwLock::new(HashMap::new()),
            dirty_entry_tracker_keys: parking_lot::RwLock::new(BTreeSet::new()),
            pumpfun_buy_missing_creator_suppress_until: parking_lot::RwLock::new(HashMap::new()),
            positions: parking_lot::RwLock::new(HashMap::new()),
            pending_intents: parking_lot::RwLock::new(HashMap::new()),
            latest_bonding_by_mint: parking_lot::RwLock::new(HashMap::new()),
            latest_pumpfun_migration_complete_by_mint: parking_lot::RwLock::new(HashMap::new()),
            latest_pool_reserve_price_hint_by_mint_pool: parking_lot::RwLock::new(HashMap::new()),
            pending_buy_entries: parking_lot::RwLock::new(HashMap::new()),
            pending_buy_mint_index: parking_lot::RwLock::new(HashMap::new()),
            mint_pools: parking_lot::RwLock::new(HashMap::new()),
            live_pool_cache: LivePoolCache::new(),
            open_position_pool_recovery_last_sent: parking_lot::RwLock::new(HashMap::new()),
            position_kv: tokio::sync::OnceCell::new(),
            orphaned_mints: parking_lot::RwLock::new(HashMap::new()),
            orphaned_recovered_intent_ids: parking_lot::RwLock::new(BoundedIntentIdCache::new(
                ORPHANED_RECOVERED_INTENT_IDS_CAP,
            )),
            latest_wallet_balance_raw_by_mint: parking_lot::RwLock::new(HashMap::new()),
            tokens_tracked: std::sync::atomic::AtomicU64::new(0),
            tokens_blacklisted: std::sync::atomic::AtomicU64::new(0),
            blacklisted_mints_for_metric: parking_lot::RwLock::new(HashSet::new()),
            intents_generated: std::sync::atomic::AtomicU64::new(0),
            exits_generated: std::sync::atomic::AtomicU64::new(0),
            last_event_slot: std::sync::atomic::AtomicU64::new(0),
            last_event_ts_ms: std::sync::atomic::AtomicU64::new(0),
            momentum_active_pool_publish_queue: parking_lot::Mutex::new(
                MomentumActivePoolPublishQueue::default(),
            ),
        };

        let mint = "TokenMint2222222222222222222222222222222222";
        let pool = "PoolPubkey2222222222222222222222222222222222";
        let wsol = "So11111111111111111111111111111111111111112";

        ctx.token_trackers.write().insert(
            MomentumContext::tracker_storage_key(mint, pool),
            TokenTracker::new(mint, pool, "pump_amm", 1, 0),
        );

        let mut accounts: Vec<String> = (0..14).map(|i| format!("Acc{i:02}")).collect();
        accounts[0] = pool.to_string();

        ctx.record_dex_pool_accounts("pump_amm", pool, mint, wsol, &accounts);

        let got = ctx
            .try_get_dex_pool_accounts_for_mint_pool(mint, pool)
            .expect("full verified PumpSwap account set should be available for BUY");
        assert_eq!(got.len(), 14);
        assert_eq!(got[0], pool);
    }

    // --- Quote-first price exits (ExitExecutableQuote, I-13 pool context) ---

    fn make_exit_config() -> MomentumConfig {
        let mut c = MomentumConfig::default();
        c.hard_stop_min_hold_secs = 0;
        c.catastrophic_stop_loss_pct = 1_000.0;
        c.hard_stop_loss_pct = 20.0;
        c.trailing_stop_pct = 20.0;
        c.trailing_activation_pct = 5.0;
        c.take_profit_pct = 100.0;
        c.take_profit_min_hold_secs = 0;
        c.max_hold_time_secs = 1_000_000;
        c.momentum_exit_min_hold_secs = 0;
        c
    }

    fn sample_exit_quote(tokens_per_sol: f64) -> ExitExecutableQuote {
        ExitExecutableQuote {
            tokens_per_sol,
            pool_sourced: true,
            quote_pool: "p".to_string(),
            quote_dex: "dex".to_string(),
            marks_position_pool: true,
            source_slot: Some(900_000_000),
            cache_age_ms: Some(0),
        }
    }

    #[tokio::test]
    async fn pool_cache_reserve_mark_does_not_advance_trailing_session_high() {
        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);
        let mint = "TrailSessMint1111111111111111111111111111";
        ctx.open_position(OpenPositionParams {
            mint,
            pool: "poolA",
            dex: "raydium",
            entry_price: 100.0,
            token_decimals: 6,
            token_amount: 1_000_000,
            sol_invested: 1_000_000_000,
            token_program: None,
            creator: None,
            entry_confirmed_slot: 500,
            initial_bonding: None,
        });
        ctx.update_position_price(mint, 89.0, None, Some("poolA"), Some(501));
        let live = ctx.positions.read().get(mint).unwrap().clone();
        assert_eq!(live.highest_price, 100.0);
        assert_eq!(live.current_price, 89.0);

        let c = make_exit_config();
        let mut ex = sample_exit_quote(89.0);
        ex.quote_pool = "poolA".to_string();
        let mut pos = live;
        pos.should_exit(&c, Some(&ex), "test", 9_000_000_000u64, None);
        assert!(
            !pos.trailing_active,
            "reserve mark must not fabricate session high for trailing activation"
        );
    }

    #[tokio::test]
    async fn trade_derived_price_advances_session_high_and_can_activate_trailing() {
        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);
        let mint = "TrailSessMint2222222222222222222222222222";
        ctx.open_position(OpenPositionParams {
            mint,
            pool: "poolA",
            dex: "raydium",
            entry_price: 100.0,
            token_decimals: 6,
            token_amount: 1_000_000,
            sol_invested: 1_000_000_000,
            token_program: None,
            creator: None,
            entry_confirmed_slot: 500,
            initial_bonding: None,
        });
        let te = TradeEvent {
            timestamp: Instant::now(),
            slot: 501,
            trader: "a".to_string(),
            is_buy: true,
            sol_amount: 1,
            token_amount: 1,
            signature: "sig".to_string(),
        };
        ctx.update_position_price(mint, 95.0, Some(te), Some("poolA"), Some(501));
        let live = ctx.positions.read().get(mint).unwrap().clone();
        assert!((live.highest_price - 95.0).abs() < 1e-6);

        let mut c = make_exit_config();
        c.trailing_activation_pct = 5.0;
        c.hard_stop_loss_pct = 99.0;
        c.take_profit_pct = 1_000.0;
        let mut pos = live;
        pos.should_exit(&c, None, "test", 9_000_000_000u64, None);
        assert!(
            pos.trailing_active,
            "session peak PnL from trade session high should activate trailing"
        );
    }

    #[tokio::test]
    async fn modest_trade_then_reserve_spike_does_not_activate_trailing() {
        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);
        let mint = "TrailSessMint3333333333333333333333333333";
        ctx.open_position(OpenPositionParams {
            mint,
            pool: "poolA",
            dex: "raydium",
            entry_price: 100.0,
            token_decimals: 6,
            token_amount: 1_000_000,
            sol_invested: 1_000_000_000,
            token_program: None,
            creator: None,
            entry_confirmed_slot: 500,
            initial_bonding: None,
        });
        let te = TradeEvent {
            timestamp: Instant::now(),
            slot: 501,
            trader: "b".to_string(),
            is_buy: true,
            sol_amount: 1,
            token_amount: 1,
            signature: "sig2".to_string(),
        };
        ctx.update_position_price(mint, 99.5, Some(te), Some("poolA"), Some(501));
        ctx.update_position_price(mint, 89.0, None, Some("poolA"), Some(502));
        let live = ctx.positions.read().get(mint).unwrap().clone();
        assert!((live.highest_price - 99.5).abs() < 1e-6);
        assert!((live.current_price - 89.0).abs() < 1e-6);

        let mut c = make_exit_config();
        c.trailing_activation_pct = 5.0;
        c.hard_stop_loss_pct = 99.0;
        c.take_profit_pct = 1_000.0;
        let mut ex = sample_exit_quote(89.0);
        ex.quote_pool = "poolA".to_string();
        let mut pos = live;
        pos.should_exit(&c, Some(&ex), "test", 9_000_000_000u64, None);
        assert!(
            !pos.trailing_active,
            "reserve spike must not activate trailing when session high only modestly improved"
        );
    }

    /// Production-like: bad mark would look stopped out; executable shows gain — no STOP (quote-first).
    #[test]
    fn hard_stop_not_fired_when_executable_profitable_despite_bad_mark() {
        let mut c = make_exit_config();
        c.hard_stop_loss_pct = 50.0; // "Hard stop" threshold includes -50.3% class

        let entry = 5_727_593.0;
        let stale = 12_736_430.0;
        let mut pos = PositionTracker::new("m", "p", "dex", entry, 6, 7_159_492_133, 0);
        pos.current_price = stale;
        pos.highest_price = stale.min(pos.highest_price);
        // Exec ~3,692,386 tps → pnl = (5.7M/3.7M-1)*100 = +~55% (I-14)
        let ex = sample_exit_quote(3_692_386.0);
        let r = pos.should_exit(&c, Some(&ex), "test", 9_000_000_000u64, None);
        assert!(
            r.is_none(),
            "expected no STOP_LOSS when executable is not past threshold, got {:?}",
            r
        );
    }

    #[test]
    fn hard_stop_allowed_when_executable_quote_confirms_loss() {
        let mut c = make_exit_config();
        c.hard_stop_loss_pct = 50.0;

        let entry = 5_727_593.0;
        let stale = 12_736_430.0;
        let mut pos = PositionTracker::new("m", "p", "dex", entry, 6, 7_159_492_133, 0);
        pos.current_price = stale;
        pos.highest_price = entry;

        // Executable also very high tps → also deep loss; allow STOP_LOSS
        let ex = sample_exit_quote(12_000_000.0);
        let (ty, _reason) = pos
            .should_exit(&c, Some(&ex), "test", 9_000_000_000u64, None)
            .expect("STOP_LOSS");
        assert_eq!(ty, "STOP_LOSS");
    }

    /// Scope D: paper stop-loss from trade-derived `current_price` alone cannot fire without a quote.
    #[test]
    fn hard_stop_not_fired_without_executable_quote() {
        let mut c = make_exit_config();
        c.hard_stop_loss_pct = 50.0;
        let entry = 100.0;
        let mut pos = PositionTracker::new("m", "p", "dex", entry, 6, 1_000_000, 0);
        pos.current_price = 300.0; // I-14: higher tps vs entry ⇒ loss
        let r = pos.should_exit(&c, None, "test", 9_000_000_000u64, None);
        assert!(
            r.is_none(),
            "STOP_LOSS must not fire without executable quote, got {:?}",
            r
        );
    }

    /// Quote-first: executable reserve shows deep loss while `current_price` mark is only mildly off.
    #[test]
    fn stop_loss_quote_first_fires_on_executable_even_if_current_pnl_mild() {
        let mut c = make_exit_config();
        c.hard_stop_loss_pct = 20.0;
        c.take_profit_pct = 1_000.0;
        let entry = 100.0;
        let mut pos = PositionTracker::new("m", "p", "dex", entry, 6, 1_000_000, 0);
        pos.current_price = 110.0; // ~-9.1% PnL vs entry
        pos.highest_price = entry;
        let ex = sample_exit_quote(250.0); // executable ~-150% loss
        let (ty, _) = pos
            .should_exit(&c, Some(&ex), "test", 9_000_000_000u64, None)
            .expect("STOP_LOSS");
        assert_eq!(ty, "STOP_LOSS");
    }

    /// Quote-first: `current_price` looks past hard stop but executable is better — do not exit.
    #[test]
    fn stop_loss_not_fired_when_only_current_pnl_past_stop_executable_disagrees() {
        let mut c = make_exit_config();
        c.hard_stop_loss_pct = 20.0;
        c.take_profit_pct = 1_000.0;
        let entry = 100.0;
        let mut pos = PositionTracker::new("m", "p", "dex", entry, 6, 1_000_000, 0);
        pos.current_price = 300.0; // mark looks like huge loss
        pos.highest_price = entry;
        let ex = sample_exit_quote(105.0); // executable only mild loss vs -20% threshold
        assert!(pos
            .should_exit(&c, Some(&ex), "test", 9_000_000_000u64, None)
            .is_none());
    }

    /// Wrong-pool executable may still drive STOP decision but must not overwrite `current_price` (I-13).
    #[test]
    fn stop_loss_wrong_pool_executable_does_not_update_current_price() {
        let mut c = make_exit_config();
        c.hard_stop_loss_pct = 20.0;
        c.take_profit_pct = 1_000.0;
        let entry = 100.0;
        let mut pos = PositionTracker::new("m", "pos_pool", "dex", entry, 6, 1_000_000, 0);
        pos.current_price = 110.0;
        pos.highest_price = entry;
        let mut ex = sample_exit_quote(250.0);
        ex.marks_position_pool = false;
        ex.quote_pool = "other_pool".to_string();
        let before = pos.current_price;
        let (ty, _) = pos
            .should_exit(&c, Some(&ex), "test", 9_000_000_000u64, None)
            .expect("STOP_LOSS");
        assert_eq!(ty, "STOP_LOSS");
        assert_eq!(
            pos.current_price, before,
            "alternate-pool quote must not refresh mark"
        );
    }

    /// I-13: alternate-pool executable must not drive TRAILING_STOP vs position-pool session high.
    #[test]
    fn trailing_stop_skipped_when_executable_quote_not_position_pool() {
        let mut c = make_exit_config();
        c.trailing_stop_pct = 15.0;
        c.trailing_activation_pct = 1.0;
        c.hard_stop_loss_pct = 99.0;
        c.take_profit_pct = 1_000.0;
        let entry = 100.0;
        let mut pos = PositionTracker::new("m", "pos_pool", "dex", entry, 6, 1_000_000, 0);
        pos.trailing_active = true;
        pos.highest_price = entry;
        pos.current_price = 102.0;
        let mut ex = sample_exit_quote(250.0);
        ex.marks_position_pool = false;
        ex.quote_pool = "other_pool".to_string();
        assert!(
            pos.should_exit(&c, Some(&ex), "test", 9_000_000_000u64, None)
                .is_none(),
            "wrong-pool executable must not trip TRAILING vs position session high"
        );
    }

    #[test]
    fn trailing_stop_fires_when_position_pool_executable_drawdown_exceeds_limit() {
        let mut c = make_exit_config();
        c.trailing_stop_pct = 15.0;
        c.trailing_activation_pct = 1.0;
        c.hard_stop_loss_pct = 99.0;
        c.take_profit_pct = 1_000.0;
        let entry = 100.0;
        let mut pos = PositionTracker::new("m", "pos_pool", "dex", entry, 6, 1_000_000, 0);
        pos.trailing_active = true;
        pos.highest_price = entry;
        pos.current_price = 102.0;
        pos.trade_mark_tps = 250.0;
        let mut ex = sample_exit_quote(250.0);
        ex.marks_position_pool = true;
        ex.quote_pool = "pos_pool".to_string();
        let (ty, reason) = pos
            .should_exit(&c, Some(&ex), "test", 9_000_000_000u64, None)
            .expect("TRAILING_STOP from position-pool quote");
        assert_eq!(ty, "TRAILING_STOP");
        assert!(
            reason.contains("session high"),
            "reason should say session high: {}",
            reason
        );
        assert!(
            !reason.contains("ATH"),
            "reason must not say ATH: {}",
            reason
        );
    }

    /// TP: `current` shows gain but no pool quote to confirm (or would disagree) → no TP
    #[test]
    fn take_profit_not_allowed_from_stale_or_unvalidated_quote() {
        let mut c = make_exit_config();
        c.take_profit_pct = 20.0;

        let entry = 100.0;
        // current far below entry = huge gain per I-14
        let mut pos = PositionTracker::new("m", "p", "dex", entry, 6, 1_000_000, 0);
        pos.current_price = 50.0; // pnl = +100%

        // No pool quote: suppress TP
        let r0 = pos.should_exit(&c, None, "test", 9_000_000_000u64, None);
        assert!(
            r0.is_none(),
            "expected TP suppressed without quote, got {:?}",
            r0
        );

        // Stale: exec says we are not at take-profit
        let ex = sample_exit_quote(95.0);
        let r1 = pos.should_exit(&c, Some(&ex), "test", 9_000_000_000u64, None);
        assert!(r1.is_none(), "expected TP suppression, got {:?}", r1);
    }

    /// Quote-first: executable shows take-profit even when `current_price` has not crossed.
    #[test]
    fn take_profit_fires_from_executable_quote_when_hold_met() {
        let mut c = make_exit_config();
        c.take_profit_pct = 20.0;
        c.take_profit_min_hold_secs = 0;
        c.hard_stop_loss_pct = 200.0;
        let entry = 100.0;
        let mut pos = PositionTracker::new("m", "p", "dex", entry, 6, 1_000_000, 0);
        pos.current_price = 130.0; // mark-only loss vs entry
        pos.highest_price = entry;
        let ex = sample_exit_quote(40.0); // executable large gain vs entry
        let (ty, reason) = pos
            .should_exit(&c, Some(&ex), "test", 9_000_000_000u64, None)
            .expect("TAKE_PROFIT");
        assert_eq!(ty, "TAKE_PROFIT");
        assert!(
            reason.contains("executable"),
            "reason should reflect executable: {}",
            reason
        );
    }

    /// Quote-first: STOP_LOSS fires when executable is past stop even if mark is only ~-30%.
    #[test]
    fn stop_loss_fires_when_executable_deeper_loss_than_current_mark() {
        let mut c = make_exit_config();
        c.hard_stop_loss_pct = 20.0;
        c.take_profit_pct = 1_000.0;
        let entry = 100.0;
        let current_tps = entry / 0.7; // ~-30% PnL (I-14)
        let exec_tps = entry / 0.5; // ~-50% PnL
        let mut pos = PositionTracker::new("m", "p", "dex", entry, 6, 1_000_000, 0);
        pos.current_price = current_tps;
        pos.highest_price = entry;
        let ex = sample_exit_quote(exec_tps);
        let cur_pnl = tokens_per_sol::pnl_pct(entry, current_tps);
        let exe_pnl = tokens_per_sol::pnl_pct(entry, exec_tps);
        assert!(
            cur_pnl < -28.0 && cur_pnl > -32.0,
            "expected ~-30% mark pnl, got {}",
            cur_pnl
        );
        assert!(
            exe_pnl < -45.0 && exe_pnl > -55.0,
            "expected ~-50% exec pnl, got {}",
            exe_pnl
        );
        let (ty, _) = pos
            .should_exit(&c, Some(&ex), "test", 9_000_000_000u64, None)
            .expect("STOP_LOSS");
        assert_eq!(ty, "STOP_LOSS");
    }

    /// Quote-first: TAKE_PROFIT fires on executable even when mark gain is lower than executable.
    #[test]
    fn take_profit_fires_when_executable_more_profitable_than_current_mark() {
        let mut c = make_exit_config();
        c.take_profit_pct = 20.0;
        c.take_profit_min_hold_secs = 0;
        c.hard_stop_loss_pct = 200.0;
        let entry = 100.0;
        let current_tps = entry / 1.3; // ~+30% PnL
        let exec_tps = entry / 1.6; // ~+60% PnL
        let mut pos = PositionTracker::new("m", "p", "dex", entry, 6, 1_000_000, 0);
        pos.current_price = current_tps;
        pos.highest_price = entry;
        let ex = sample_exit_quote(exec_tps);
        let cur_pnl = tokens_per_sol::pnl_pct(entry, current_tps);
        let exe_pnl = tokens_per_sol::pnl_pct(entry, exec_tps);
        assert!(
            cur_pnl > 25.0 && cur_pnl < 35.0,
            "expected ~+30% mark pnl, got {}",
            cur_pnl
        );
        assert!(
            exe_pnl > 55.0 && exe_pnl < 65.0,
            "expected ~+60% exec pnl, got {}",
            exe_pnl
        );
        let (ty, _) = pos
            .should_exit(&c, Some(&ex), "test", 9_000_000_000u64, None)
            .expect("TAKE_PROFIT");
        assert_eq!(ty, "TAKE_PROFIT");
    }

    /// Trade-based trailing: TRAILING_STOP from session high vs trade mark without executable quote.
    #[test]
    fn trailing_stop_fires_from_trade_mark_drawdown_without_executable_quote() {
        let mut c = make_exit_config();
        c.trailing_stop_pct = 20.0;
        c.trailing_activation_pct = 5.0;
        c.hard_stop_loss_pct = 200.0;
        c.take_profit_pct = 1_000.0;
        let entry = 100.0;
        let ath = 70.0_f64;
        let trade_tps = 90.0;
        let mut pos = PositionTracker::new("m", "p", "dex", entry, 6, 1_000_000, 0);
        pos.highest_price = ath;
        pos.trade_mark_tps = trade_tps;
        pos.current_price = trade_tps;
        pos.trailing_active = true;
        let trade_dd = tokens_per_sol::drawdown_from_ath_pct(ath, trade_tps);
        assert!(
            trade_dd >= c.trailing_stop_pct - 1e-6,
            "trade drawdown should breach threshold: {}",
            trade_dd
        );
        let (ty, reason) = pos
            .should_exit(&c, None, "test", 9_000_000_000u64, None)
            .expect("TRAILING_STOP");
        assert_eq!(ty, "TRAILING_STOP");
        assert!(
            reason.contains("trade mark"),
            "reason should reference trade mark: {}",
            reason
        );
    }

    /// Executable drawdown vs session high is extreme, but trade mark is mild → no TRAILING_STOP.
    #[test]
    fn trailing_stop_not_fired_when_only_executable_drawdown_is_high() {
        let mut c = make_exit_config();
        c.trailing_stop_pct = 20.0;
        c.trailing_activation_pct = 5.0;
        c.hard_stop_loss_pct = 200.0;
        c.take_profit_pct = 1_000.0;
        let ath = 70.0_f64;
        let trade_tps = 82.0;
        let exec_tps = 95.0;
        let mut pos = PositionTracker::new("m", "p", "dex", 100.0, 6, 1_000_000, 0);
        pos.highest_price = ath;
        pos.trade_mark_tps = trade_tps;
        pos.current_price = trade_tps;
        pos.trailing_active = true;
        let trade_dd = tokens_per_sol::drawdown_from_ath_pct(ath, trade_tps);
        let exec_dd = tokens_per_sol::drawdown_from_ath_pct(ath, exec_tps);
        assert!(
            trade_dd < c.trailing_stop_pct - 1e-6,
            "trade drawdown below threshold: {}",
            trade_dd
        );
        assert!(
            exec_dd >= c.trailing_stop_pct,
            "executable drawdown should be past threshold: {}",
            exec_dd
        );
        let ex = sample_exit_quote(exec_tps);
        assert!(
            pos.should_exit(&c, Some(&ex), "test", 9_000_000_000u64, None)
                .is_none(),
            "executable DD must not trip trailing when trade mark DD is below limit"
        );
    }

    #[test]
    fn time_exit_still_allowed_without_price_validation() {
        let c = make_exit_config();
        let entry = 100.0;
        let mut pos = PositionTracker::new("m", "p", "dex", entry, 6, 1_000_000, 0);
        pos.set_entry_time_ago(Duration::from_secs(c.max_hold_time_secs + 1));
        // Flat PnL so we reach TIME_EXIT (not hard stop) with no pool quote
        pos.current_price = entry;

        let (ty, reason) = pos
            .should_exit(&c, None, "test", 9_000_000_000u64, None)
            .expect("time exit");
        assert_eq!(ty, "TIME_EXIT");
        assert!(
            reason.contains("Max hold:")
                && reason.contains("trades_per_min")
                && !reason.to_lowercase().contains("hard stop"),
            "TIME_EXIT should include velocity detail, not hard stop: {}",
            reason
        );
    }

    #[test]
    fn time_exit_reporting_pnl_uses_executable_when_quote_valid() {
        let c = make_exit_config();
        let entry = 100.0;
        let mut pos = PositionTracker::new("m", "p", "dex", entry, 6, 1_000_000, 0);
        pos.set_entry_time_ago(Duration::from_secs(c.max_hold_time_secs + 1));
        pos.current_price = entry;
        let ex = sample_exit_quote(80.0);
        let (_, reason) = pos
            .should_exit(&c, Some(&ex), "test", 9_000_000_000u64, None)
            .expect("time exit");
        assert!(
            reason.contains("25.0%") || reason.contains("25.0"),
            "expected executable-backed reporting PnL in reason: {}",
            reason
        );
    }

    fn push_recent_trade_at_slot(
        pos: &mut PositionTracker,
        head_slot: u64,
        slot_offset: u64,
        is_buy: bool,
        sol_lamports: u64,
    ) {
        pos.recent_trades.push(TradeEvent {
            timestamp: Instant::now(),
            slot: head_slot.saturating_sub(slot_offset),
            trader: format!("tr{}", pos.recent_trades.len()),
            is_buy,
            sol_amount: sol_lamports,
            token_amount: 1,
            signature: format!("sig{}", pos.recent_trades.len()),
        });
    }

    #[test]
    fn hard_stop_grace_uses_catastrophic_threshold() {
        let mut c = make_exit_config();
        c.hard_stop_min_hold_secs = 45;
        c.catastrophic_stop_loss_pct = 45.0;
        c.hard_stop_loss_pct = 20.0;
        c.max_hold_time_secs = 1_000_000;
        let entry = 100.0;
        let mut pos = PositionTracker::new("m", "p", "dex", entry, 6, 1_000_000, 0);
        pos.set_entry_time_ago(Duration::from_secs(10));
        let mild_loss = sample_exit_quote(entry / 0.75);
        assert!(
            pos.should_exit(&c, Some(&mild_loss), "test", 9_000_000_000u64, None)
                .is_none(),
            "grace: -25% should not trip catastrophic 45%"
        );
        let severe_loss = sample_exit_quote(entry / 0.5);
        let (ty, reason) = pos
            .should_exit(&c, Some(&severe_loss), "test", 9_000_000_000u64, None)
            .expect("catastrophic stop");
        assert_eq!(ty, "STOP_LOSS");
        assert!(reason.contains("grace"), "expected grace label: {}", reason);
    }

    #[test]
    fn hard_stop_after_grace_uses_normal_threshold() {
        let mut c = make_exit_config();
        c.hard_stop_min_hold_secs = 45;
        c.catastrophic_stop_loss_pct = 45.0;
        c.hard_stop_loss_pct = 20.0;
        c.max_hold_time_secs = 1_000_000;
        let entry = 100.0;
        let mut pos = PositionTracker::new("m", "p", "dex", entry, 6, 1_000_000, 0);
        pos.set_entry_time_ago(Duration::from_secs(60));
        let loss = sample_exit_quote(entry / 0.75);
        let (ty, _) = pos
            .should_exit(&c, Some(&loss), "test", 9_000_000_000u64, None)
            .expect("normal hard stop after grace");
        assert_eq!(ty, "STOP_LOSS");
    }

    #[test]
    fn momentum_exit_suppressed_during_min_hold() {
        let mut c = make_exit_config();
        c.momentum_exit_min_hold_secs = 60;
        c.hard_stop_min_hold_secs = 45;
        c.max_hold_time_secs = 1_000_000;
        c.hard_stop_loss_pct = 99.0;
        c.take_profit_pct = 1_000.0;
        c.momentum_exit_buy_ratio = 0.5;
        c.momentum_exit_min_trades = 3;
        c.momentum_exit_window_secs = 30;
        let head = 10_000u64;
        let mut pos = PositionTracker::new("m", "p", "dex", 100.0, 6, 1_000_000, 0);
        pos.set_entry_time_ago(Duration::from_secs(20));
        for i in 0..6 {
            push_recent_trade_at_slot(&mut pos, head, i, false, 1_000_000_000);
        }
        assert!(
            pos.should_exit(&c, None, "test", head, None).is_none(),
            "momentum exit suppressed during min hold"
        );
    }

    #[test]
    fn momentum_exit_suppressed_when_in_profit() {
        let mut c = make_exit_config();
        c.momentum_exit_min_hold_secs = 60;
        c.hard_stop_min_hold_secs = 45;
        c.max_hold_time_secs = 1_000_000;
        c.hard_stop_loss_pct = 99.0;
        c.take_profit_pct = 1_000.0;
        c.momentum_exit_only_when_losing = true;
        c.momentum_exit_buy_ratio = 0.5;
        c.momentum_exit_min_trades = 3;
        let head = 10_000u64;
        let entry = 100.0;
        let mut pos = PositionTracker::new("m", "p", "dex", entry, 6, 1_000_000, 0);
        pos.set_entry_time_ago(Duration::from_secs(90));
        pos.current_price = 80.0;
        for i in 0..6 {
            push_recent_trade_at_slot(&mut pos, head, i, false, 1_000_000_000);
        }
        let ex = sample_exit_quote(80.0);
        assert!(
            pos.should_exit(&c, Some(&ex), "test", head, None).is_none(),
            "momentum exit suppressed when executable PnL is positive"
        );
    }

    #[test]
    fn momentum_exit_fires_when_losing_after_hold() {
        let mut c = make_exit_config();
        c.momentum_exit_min_hold_secs = 60;
        c.hard_stop_min_hold_secs = 45;
        c.max_hold_time_secs = 1_000_000;
        c.hard_stop_loss_pct = 99.0;
        c.take_profit_pct = 1_000.0;
        c.momentum_exit_buy_ratio = 0.5;
        c.momentum_exit_min_trades = 3;
        let head = 10_000u64;
        let entry = 100.0;
        let mut pos = PositionTracker::new("m", "p", "dex", entry, 6, 1_000_000, 0);
        pos.set_entry_time_ago(Duration::from_secs(90));
        pos.current_price = 110.0;
        for i in 0..6 {
            push_recent_trade_at_slot(&mut pos, head, i, false, 1_000_000_000);
        }
        let ex = sample_exit_quote(110.0);
        let (ty, _) = pos
            .should_exit(&c, Some(&ex), "test", head, None)
            .expect("momentum exit when losing");
        assert_eq!(ty, "MOMENTUM_EXIT");
    }

    #[test]
    fn time_exit_requires_low_velocity_after_max_hold() {
        let mut c = make_exit_config();
        c.max_hold_time_secs = 300;
        c.min_trades_per_min = 10.0;
        c.buyer_window_secs = 20;
        c.time_exit_requires_low_velocity = true;
        let head = 10_000u64;
        let mut pos = PositionTracker::new("m", "p", "dex", 100.0, 6, 1_000_000, 0);
        pos.set_entry_time_ago(Duration::from_secs(310));
        push_recent_trade_at_slot(&mut pos, head, 1, true, 1_000_000);
        push_recent_trade_at_slot(&mut pos, head, 2, true, 1_000_000);
        let (ty, reason) = pos
            .should_exit(&c, None, "test", head, Some(5.0))
            .expect("TIME_EXIT on low velocity");
        assert_eq!(ty, "TIME_EXIT");
        assert!(reason.contains("trades_per_min"), "{}", reason);
    }

    #[test]
    fn time_exit_suppressed_when_velocity_still_high() {
        let mut c = make_exit_config();
        c.max_hold_time_secs = 300;
        c.min_trades_per_min = 10.0;
        c.time_exit_requires_low_velocity = true;
        let mut pos = PositionTracker::new("m", "p", "dex", 100.0, 6, 1_000_000, 0);
        pos.set_entry_time_ago(Duration::from_secs(310));
        assert!(
            pos.should_exit(&c, None, "test", 9_000_000_000u64, Some(25.0))
                .is_none(),
            "high velocity must suppress TIME_EXIT"
        );
    }

    #[test]
    fn time_exit_absolute_cap_overrides_velocity() {
        let mut c = make_exit_config();
        c.max_hold_time_secs = 300;
        c.max_hold_absolute_cap_secs = 600;
        c.min_trades_per_min = 10.0;
        c.time_exit_requires_low_velocity = true;
        let mut pos = PositionTracker::new("m", "p", "dex", 100.0, 6, 1_000_000, 0);
        pos.set_entry_time_ago(Duration::from_secs(610));
        let (ty, reason) = pos
            .should_exit(&c, None, "test", 9_000_000_000u64, Some(50.0))
            .expect("absolute cap TIME_EXIT");
        assert_eq!(ty, "TIME_EXIT");
        assert!(reason.contains("absolute_cap"), "{}", reason);
    }

    #[test]
    fn trades_per_min_helper_matches_calculate_metrics() {
        let mut cfg = MomentumConfig::default();
        cfg.buyer_window_secs = 20;
        let head = 50_000u64;
        let mut tracker = TokenTracker::new("mint", "pool", "dex", 1, 0);
        for i in 0..5u64 {
            tracker.record_trade(
                &format!("b{i}"),
                true,
                1_000_000,
                1,
                &format!("sig{i}"),
                head - i,
                &cfg,
            );
        }
        let metrics = tracker.calculate_metrics(&cfg, head);
        let helper = trades_per_min_in_buyer_window(&tracker.trades, cfg.buyer_window_secs, head);
        assert!(
            (metrics.trades_per_min - helper).abs() < 1e-6,
            "metrics={} helper={}",
            metrics.trades_per_min,
            helper
        );
    }

    #[tokio::test]
    async fn reconcile_timed_exit_does_not_force_time_exit_when_should_exit_none() {
        let mut cfg = MomentumConfig::default();
        cfg.max_hold_time_secs = 300;
        cfg.min_trades_per_min = 10.0;
        cfg.time_exit_requires_low_velocity = true;
        cfg.hard_stop_loss_pct = 1_000.0;
        cfg.take_profit_pct = 1_000.0;

        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);
        *ctx.config.write() = cfg.clone();

        let mint = "TimedExitMint1111111111111111111111111111";
        ctx.open_position(OpenPositionParams {
            mint,
            pool: "poolA",
            dex: "raydium",
            entry_price: 100.0,
            token_decimals: 6,
            token_amount: 1_000_000,
            sol_invested: 1_000_000_000,
            token_program: None,
            creator: None,
            entry_confirmed_slot: 500,
            initial_bonding: None,
        });
        {
            let mut w = ctx.positions.write();
            w.get_mut(mint)
                .expect("position")
                .set_entry_time_ago(Duration::from_secs(400));
        }
        let head = 10_000u64;
        ctx.last_event_slot
            .store(head, std::sync::atomic::Ordering::Relaxed);
        let mut tracker = TokenTracker::new(mint, "poolA", "raydium", 1, 0);
        for i in 0..12u64 {
            tracker.record_trade(
                &format!("b{i}"),
                true,
                1_000_000,
                1,
                &format!("sig{i}"),
                head - i,
                &cfg,
            );
        }
        ctx.token_trackers
            .write()
            .insert(MomentumContext::tracker_storage_key(mint, "poolA"), tracker);

        let before = ctx
            .exits_generated
            .load(std::sync::atomic::Ordering::Relaxed);
        ctx.reconcile_timed_exits().await;
        let after = ctx
            .exits_generated
            .load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(
            after, before,
            "reconcile must not publish TIME_EXIT when should_exit is None"
        );
    }

    #[test]
    fn price_exit_suppressed_when_quote_slot_not_after_entry_confirm() {
        let mut c = make_exit_config();
        c.hard_stop_loss_pct = 20.0;
        c.take_profit_pct = 1_000.0;
        let mut pos = PositionTracker::new("m", "p", "dex", 100.0, 6, 1_000_000, 0);
        pos.entry_confirmed_slot = 500;
        pos.current_price = 110.0;
        pos.highest_price = 100.0;
        let mut ex = sample_exit_quote(250.0);
        ex.source_slot = Some(400);
        assert!(
            pos.should_exit(&c, Some(&ex), "test", 9_000_000_000u64, None)
                .is_none(),
            "older on-chain slot than BUY confirm must not authorize price exit"
        );
    }

    #[test]
    fn price_exit_suppressed_when_executable_cache_age_stale() {
        let mut c = make_exit_config();
        c.hard_stop_loss_pct = 20.0;
        c.take_profit_pct = 1_000.0;
        let entry = 100.0;
        let mut pos = PositionTracker::new("m", "p", "dex", entry, 6, 1_000_000, 0);
        pos.current_price = 110.0;
        pos.highest_price = entry;
        let mut ex = sample_exit_quote(250.0);
        ex.cache_age_ms = Some(EXIT_QUOTE_MAX_CACHE_AGE_MS + 1);
        assert!(
            pos.should_exit(&c, Some(&ex), "test", 9_000_000_000u64, None)
                .is_none(),
            "stale reserve snapshot must not drive STOP_LOSS"
        );
    }

    #[test]
    fn price_exit_suppressed_for_token_program_pubkey_as_quote_pool() {
        let mut c = make_exit_config();
        c.hard_stop_loss_pct = 20.0;
        c.take_profit_pct = 1_000.0;
        let entry = 100.0;
        let mut pos = PositionTracker::new("m", "p", "dex", entry, 6, 1_000_000, 0);
        pos.current_price = 110.0;
        pos.highest_price = entry;
        let mut ex = sample_exit_quote(250.0);
        ex.quote_pool = SPL_TOKEN_PROGRAM.to_string();
        assert!(
            pos.should_exit(&c, Some(&ex), "test", 9_000_000_000u64, None)
                .is_none(),
            "Token program address must not be treated as a pool id for exits"
        );
    }

    #[test]
    fn exit_guard_rejects_dex_accounts_without_wsol_and_mint() {
        let pos = PositionTracker::new("mint9", "pool", "dex", 100.0, 6, 1_000_000, 0);
        let ex = ExitExecutableQuote {
            tokens_per_sol: 50.0,
            pool_sourced: true,
            quote_pool: "pool".to_string(),
            quote_dex: "ray".to_string(),
            marks_position_pool: true,
            source_slot: Some(10),
            cache_age_ms: Some(0),
        };
        let bad = vec!["a".to_string(), "b".to_string()];
        assert_eq!(
            exit_quote_price_exit_guard_violation(&pos, &ex, Some(&bad)),
            Some("QUOTE_POOL_ACCOUNTS_NOT_VERIFIED_SOL_PAIR")
        );
        let ok = vec!["x".to_string(), WSOL_MINT.to_string(), "mint9".to_string()];
        assert!(exit_quote_price_exit_guard_violation(&pos, &ex, Some(&ok)).is_none());
    }

    /// Lower executable tps than entry → positive PnL in our convention
    #[test]
    fn tokens_per_sol_formula_not_inverted() {
        let entry = 5_727_593.0f64;
        let exec = 3_692_386.0f64; // more valuable (lower tps) than entry
        let p = tokens_per_sol::pnl_pct(entry, exec);
        assert!(p > 0.0, "lower exec tps should be profit, pnl={}", p);
    }

    /// Scope 56: 9mR7-like numbers — matched UI executable must allow STOP_LOSS (not fake +90k% PnL).
    #[test]
    fn scope56_stop_loss_allows_with_ui_matched_executable_quote() {
        let mut c = make_exit_config();
        c.hard_stop_loss_pct = 50.0;
        c.take_profit_pct = 1_000.0;

        let entry = 9_876_538.033_6;
        let current_stale = 30_000_000.0;
        let mut pos = PositionTracker::new("m9", "p9", "dex", entry, 6, 12_345_672_542, 0);
        pos.current_price = current_stale;
        pos.highest_price = entry;
        // Pool-correct sell quote (UI): same convention as `entry_price`
        let sol_out = 418_528u64;
        let exec_tps = tokens_per_sol::ui_tokens_per_sol(12_345_672_542, 6, sol_out);
        let ex = ExitExecutableQuote {
            tokens_per_sol: exec_tps,
            pool_sourced: true,
            quote_pool: "p9".to_string(),
            quote_dex: "dex".to_string(),
            marks_position_pool: true,
            source_slot: Some(900_000_000),
            cache_age_ms: Some(0),
        };
        let r = pos.should_exit(&c, Some(&ex), "test", 9_000_000_000u64, None);
        let (ty, _reason) = r.expect("STOP_LOSS must fire when loss is real");
        assert_eq!(ty, "STOP_LOSS");
    }

    /// Orphaned scale-in: existing probe position + confirmed BUY without pending → full total for exits.
    #[tokio::test]
    async fn scope56_orphan_scale_in_applies_to_existing_position() {
        let mut cfg = MomentumConfig::default();
        cfg.hard_stop_loss_pct = 1_000.0;
        cfg.take_profit_pct = 1_000.0;
        cfg.max_hold_time_secs = 300;

        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());

        let ctx = Arc::new(MomentumContext {
            run_id: "run-test".to_string(),
            config: parking_lot::RwLock::new(cfg),
            nats: None,
            jsonl_writer,
            intent_publish_tx: parking_lot::Mutex::new(Some(noop_intent_publish_tx())),
            intent_counter: std::sync::atomic::AtomicU64::new(0),
            pool_first_seen: parking_lot::RwLock::new(std::collections::HashMap::new()),
            pending_dev_info: parking_lot::RwLock::new(HashMap::new()),
            pending_pool_accounts: parking_lot::RwLock::new(HashMap::new()),
            mint_infos: parking_lot::RwLock::new(HashMap::new()),
            token_trackers: parking_lot::RwLock::new(HashMap::new()),
            tracker_keys_by_mint: parking_lot::RwLock::new(HashMap::new()),
            dirty_entry_tracker_keys: parking_lot::RwLock::new(BTreeSet::new()),
            pumpfun_buy_missing_creator_suppress_until: parking_lot::RwLock::new(HashMap::new()),
            positions: parking_lot::RwLock::new(HashMap::new()),
            pending_intents: parking_lot::RwLock::new(HashMap::new()),
            latest_bonding_by_mint: parking_lot::RwLock::new(HashMap::new()),
            latest_pumpfun_migration_complete_by_mint: parking_lot::RwLock::new(HashMap::new()),
            latest_pool_reserve_price_hint_by_mint_pool: parking_lot::RwLock::new(HashMap::new()),
            pending_buy_entries: parking_lot::RwLock::new(HashMap::new()),
            pending_buy_mint_index: parking_lot::RwLock::new(HashMap::new()),
            mint_pools: parking_lot::RwLock::new(HashMap::new()),
            live_pool_cache: LivePoolCache::new(),
            open_position_pool_recovery_last_sent: parking_lot::RwLock::new(HashMap::new()),
            position_kv: tokio::sync::OnceCell::new(),
            orphaned_mints: parking_lot::RwLock::new(HashMap::new()),
            orphaned_recovered_intent_ids: parking_lot::RwLock::new(BoundedIntentIdCache::new(
                ORPHANED_RECOVERED_INTENT_IDS_CAP,
            )),
            latest_wallet_balance_raw_by_mint: parking_lot::RwLock::new(HashMap::new()),
            tokens_tracked: std::sync::atomic::AtomicU64::new(0),
            tokens_blacklisted: std::sync::atomic::AtomicU64::new(0),
            blacklisted_mints_for_metric: parking_lot::RwLock::new(HashSet::new()),
            intents_generated: std::sync::atomic::AtomicU64::new(0),
            exits_generated: std::sync::atomic::AtomicU64::new(0),
            last_event_slot: std::sync::atomic::AtomicU64::new(0),
            last_event_ts_ms: std::sync::atomic::AtomicU64::new(0),
            momentum_active_pool_publish_queue: parking_lot::Mutex::new(
                MomentumActivePoolPublishQueue::default(),
            ),
        });

        let mint = "9mR7mmX55n1F56rKBBcfMrRzMoJjaZnSRs6fV1GRpump";
        let pool = "Pool9mR7ttttttttttttttttttttttttttttttttttt";
        ctx.register_pool(mint, pool, "pump_amm", 1);
        ctx.token_trackers.write().insert(
            MomentumContext::tracker_storage_key(mint, pool),
            TokenTracker::new(mint, pool, "pump_amm", 1, 0),
        );

        ctx.open_position(OpenPositionParams {
            mint,
            pool,
            dex: "pump_amm",
            entry_price: 1.0,
            token_decimals: 6,
            token_amount: 12_345_672_542,
            sol_invested: 1_250_000,
            token_program: None,
            creator: None,
            entry_confirmed_slot: 0,
            initial_bonding: None,
        });

        let scale_fill = 37_843_472_159u64;
        let sol_spent = 3_750_000u64;
        let mut meta = std::collections::HashMap::new();
        meta.insert("side".to_string(), "BUY".to_string());
        meta.insert("entry_kind".to_string(), "scale_in".to_string());
        let orphan = ExecutionResult {
            header: RecordHeader::new("test", BUILD_VERSION, "run-test"),
            execution_id: "ex-orphan".to_string(),
            decision_id: "dec-orphan".to_string(),
            intent_id: "int-orphan-scale-001".to_string(),
            source: "momentum-bot".to_string(),
            token_mint: Some(mint.to_string()),
            signature: Some("sig-orphan".to_string()),
            bundle_id: None,
            status: ExecutionStatus::Confirmed,
            fill_in: Some(ExplicitAmount::new(sol_spent, 9)),
            fill_out: Some(ExplicitAmount::new(scale_fill, 6)),
            fill_status: Some(FillStatus::Complete),
            fill_unavailable_reason: None,
            confirmed_slot: Some(1),
            block_time_unix_ms: None,
            fees: None,
            pnl: None,
            wallet_sol_delta_lamports: Some(-(sol_spent as i128)),
            error_message: None,
            error_code: None,
            latency_ms: Some(1),
            metadata: meta,
        };

        Arc::clone(&ctx).handle_execution_result(&orphan);

        let expected = 12_345_672_542u64.saturating_add(scale_fill);
        {
            let sk = MomentumContext::tracker_storage_key(mint, pool);
            let trackers = ctx.token_trackers.read();
            let tr = trackers.get(&sk).expect("tracker after orphan scale-in");
            assert!(
                matches!(tr.state, TrackerState::PositionOpenFull { .. }),
                "orphan scale-in on existing position must set PositionOpenFull"
            );
        }
        {
            let positions = ctx.positions.read();
            let pos = positions.get(mint).expect("position after orphan scale");
            assert_eq!(
                pos.token_amount, expected,
                "orphan scale-in must add to total"
            );
        }

        // Second delivery with same intent_id (JetStream replay) must not double-count
        Arc::clone(&ctx).handle_execution_result(&orphan);
        assert_eq!(
            ctx.positions.read().get(mint).unwrap().token_amount,
            expected,
            "duplicate ExecutionResult with same intent_id must not add twice"
        );

        // `check_for_exits` must size to full tracked total (not probe-only)
        {
            let mut w = ctx.positions.write();
            let p = w.get_mut(mint).expect("position for time exit");
            p.set_entry_time_ago(Duration::from_secs(400));
        }
        let exits = ctx.check_for_exits();
        assert_eq!(exits.len(), 1, "expected one TIME_EXIT");
        assert_eq!(exits[0].0, mint);
        assert_eq!(
            exits[0].5, expected,
            "exit must use full position after orphan scale-in, not probe size"
        );
    }

    /// Orphaned scale-in with no TokenTracker: `PositionTracker` alone supplies pool/dex.
    #[tokio::test]
    async fn scope56_orphan_scale_in_without_token_tracker_uses_position_routing() {
        let mut cfg = MomentumConfig::default();
        cfg.hard_stop_loss_pct = 1_000.0;
        cfg.take_profit_pct = 1_000.0;
        cfg.max_hold_time_secs = 300;

        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());

        let ctx = Arc::new(MomentumContext {
            run_id: "run-test".to_string(),
            config: parking_lot::RwLock::new(cfg),
            nats: None,
            jsonl_writer,
            intent_publish_tx: parking_lot::Mutex::new(Some(noop_intent_publish_tx())),
            intent_counter: std::sync::atomic::AtomicU64::new(0),
            pool_first_seen: parking_lot::RwLock::new(std::collections::HashMap::new()),
            pending_dev_info: parking_lot::RwLock::new(HashMap::new()),
            pending_pool_accounts: parking_lot::RwLock::new(HashMap::new()),
            mint_infos: parking_lot::RwLock::new(HashMap::new()),
            token_trackers: parking_lot::RwLock::new(HashMap::new()),
            tracker_keys_by_mint: parking_lot::RwLock::new(HashMap::new()),
            dirty_entry_tracker_keys: parking_lot::RwLock::new(BTreeSet::new()),
            pumpfun_buy_missing_creator_suppress_until: parking_lot::RwLock::new(HashMap::new()),
            positions: parking_lot::RwLock::new(HashMap::new()),
            pending_intents: parking_lot::RwLock::new(HashMap::new()),
            latest_bonding_by_mint: parking_lot::RwLock::new(HashMap::new()),
            latest_pumpfun_migration_complete_by_mint: parking_lot::RwLock::new(HashMap::new()),
            latest_pool_reserve_price_hint_by_mint_pool: parking_lot::RwLock::new(HashMap::new()),
            pending_buy_entries: parking_lot::RwLock::new(HashMap::new()),
            pending_buy_mint_index: parking_lot::RwLock::new(HashMap::new()),
            mint_pools: parking_lot::RwLock::new(HashMap::new()),
            live_pool_cache: LivePoolCache::new(),
            open_position_pool_recovery_last_sent: parking_lot::RwLock::new(HashMap::new()),
            position_kv: tokio::sync::OnceCell::new(),
            orphaned_mints: parking_lot::RwLock::new(HashMap::new()),
            orphaned_recovered_intent_ids: parking_lot::RwLock::new(BoundedIntentIdCache::new(
                ORPHANED_RECOVERED_INTENT_IDS_CAP,
            )),
            latest_wallet_balance_raw_by_mint: parking_lot::RwLock::new(HashMap::new()),
            tokens_tracked: std::sync::atomic::AtomicU64::new(0),
            tokens_blacklisted: std::sync::atomic::AtomicU64::new(0),
            blacklisted_mints_for_metric: parking_lot::RwLock::new(HashSet::new()),
            intents_generated: std::sync::atomic::AtomicU64::new(0),
            exits_generated: std::sync::atomic::AtomicU64::new(0),
            last_event_slot: std::sync::atomic::AtomicU64::new(0),
            last_event_ts_ms: std::sync::atomic::AtomicU64::new(0),
            momentum_active_pool_publish_queue: parking_lot::Mutex::new(
                MomentumActivePoolPublishQueue::default(),
            ),
        });

        let mint = "NoTrkMintttttttttttttttttttttttttttttttttttt";
        let pool = "PoolNoTrktttttttttttttttttttttttttttttttttt";
        ctx.register_pool(mint, pool, "pump_amm", 1);
        // Deliberately no `token_trackers` entry

        ctx.open_position(OpenPositionParams {
            mint,
            pool,
            dex: "pump_amm",
            entry_price: 1.0,
            token_decimals: 6,
            token_amount: 12_345_672_542,
            sol_invested: 1_250_000,
            token_program: None,
            creator: None,
            entry_confirmed_slot: 0,
            initial_bonding: None,
        });

        let scale_fill = 37_843_472_159u64;
        let sol_spent = 3_750_000u64;
        let mut meta = std::collections::HashMap::new();
        meta.insert("side".to_string(), "BUY".to_string());
        let orphan = ExecutionResult {
            header: RecordHeader::new("test", BUILD_VERSION, "run-test"),
            execution_id: "ex-orphan-nt".to_string(),
            decision_id: "dec-orphan-nt".to_string(),
            intent_id: "int-orphan-scale-no-trk-001".to_string(),
            source: "momentum-bot".to_string(),
            token_mint: Some(mint.to_string()),
            signature: Some("sig-orphan-nt".to_string()),
            bundle_id: None,
            status: ExecutionStatus::Confirmed,
            fill_in: Some(ExplicitAmount::new(sol_spent, 9)),
            fill_out: Some(ExplicitAmount::new(scale_fill, 6)),
            fill_status: Some(FillStatus::Complete),
            fill_unavailable_reason: None,
            confirmed_slot: Some(1),
            block_time_unix_ms: None,
            fees: None,
            pnl: None,
            wallet_sol_delta_lamports: Some(-(sol_spent as i128)),
            error_message: None,
            error_code: None,
            latency_ms: Some(1),
            metadata: meta,
        };

        Arc::clone(&ctx).handle_execution_result(&orphan);

        let expected = 12_345_672_542u64.saturating_add(scale_fill);
        assert_eq!(
            ctx.positions.read().get(mint).unwrap().token_amount,
            expected
        );

        Arc::clone(&ctx).handle_execution_result(&orphan);
        assert_eq!(
            ctx.positions.read().get(mint).unwrap().token_amount,
            expected
        );

        {
            let mut w = ctx.positions.write();
            let p = w.get_mut(mint).expect("pos");
            p.set_entry_time_ago(Duration::from_secs(400));
        }
        let exits = ctx.check_for_exits();
        assert_eq!(exits.len(), 1);
        assert_eq!(exits[0].5, expected);
    }

    /// Scope 57: orphan probe BUY must set `PositionOpenProbe` so scale-in gate can run.
    #[tokio::test]
    async fn scope57_orphan_probe_recovery_sets_position_open_probe() {
        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);

        let mint = "OrphProbeMintttttttttttttttttttttttttttttttt";
        let pool = "OrphProbePoolttttttttttttttttttttttttttttttt";
        ctx.register_pool(mint, pool, "raydium", 1);
        let sk = MomentumContext::tracker_storage_key(mint, pool);
        {
            let mut trackers = ctx.token_trackers.write();
            let mut tr = TokenTracker::new(mint, pool, "raydium", 1, 0);
            tr.state = TrackerState::ProbeBuyPending {
                sent_at: Instant::now(),
            };
            trackers.insert(sk.clone(), tr);
        }

        let probe_fill = 7_159_492_133u64;
        let sol_spent = 250_000_000u64;
        let mut meta = std::collections::HashMap::new();
        meta.insert("side".to_string(), "BUY".to_string());
        meta.insert("entry_kind".to_string(), "probe".to_string());
        let orphan = ExecutionResult {
            header: RecordHeader::new("test", BUILD_VERSION, "run-test"),
            execution_id: "ex-orphan-probe".to_string(),
            decision_id: "dec-orphan-probe".to_string(),
            intent_id: "int-orphan-probe-001".to_string(),
            source: "momentum-bot".to_string(),
            token_mint: Some(mint.to_string()),
            signature: Some("sig-orphan-probe".to_string()),
            bundle_id: None,
            status: ExecutionStatus::Confirmed,
            fill_in: Some(ExplicitAmount::new(sol_spent, 9)),
            fill_out: Some(ExplicitAmount::new(probe_fill, 6)),
            fill_status: Some(FillStatus::Complete),
            fill_unavailable_reason: None,
            confirmed_slot: Some(100),
            block_time_unix_ms: None,
            fees: None,
            pnl: None,
            wallet_sol_delta_lamports: Some(-(sol_spent as i128)),
            error_message: None,
            error_code: None,
            latency_ms: Some(1),
            metadata: meta,
        };

        Arc::clone(&ctx).handle_execution_result(&orphan);

        assert!(ctx.positions.read().contains_key(mint));
        let trackers = ctx.token_trackers.read();
        let tr = trackers.get(&sk).unwrap();
        assert!(
            matches!(tr.state, TrackerState::PositionOpenProbe { .. }),
            "orphan probe recovery must advance tracker to PositionOpenProbe"
        );
    }

    /// Scope 57: after orphan probe recovery, scale-in gate can emit ENTER_SCALE_IN within window.
    #[tokio::test]
    async fn scope57_scale_in_fires_after_orphan_probe_recovery() {
        use solana_sdk::pubkey::Pubkey;

        let mut cfg = MomentumConfig::default();
        cfg.default_position_lamports = 1_000;
        cfg.probe_buy_pct = 0.25;
        cfg.early_min_liquidity_sol = 0.0;
        cfg.min_unique_buyers = 0;
        cfg.min_trades_per_min = 0.0;
        cfg.min_buy_dominance = 0.0;
        cfg.min_sol_inflow_lamports = 0;
        cfg.require_mint_authority_renounced = false;
        cfg.require_freeze_authority_none = false;
        cfg.top1_buyer_share_cap = 1.0;
        cfg.top3_buyer_share_cap = 1.0;
        cfg.repeat_buyer_min_ratio = 0.0;
        cfg.min_trade_size_lamports = 0;
        cfg.small_buy_ratio_cap = 1.0;
        cfg.min_token_age_secs = 0;
        cfg.scale_in_confirm_window_secs = 120;
        cfg.scale_in_min_probe_executable_pnl_pct = 0.0;

        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);
        *ctx.config.write() = cfg;

        let mint = Pubkey::new_from_array([0xC3u8; 32]).to_string();
        let pool = Pubkey::new_from_array([0xD4u8; 32]).to_string();
        ctx.register_pool(&mint, &pool, "raydium", 1);
        let sk = MomentumContext::tracker_storage_key(&mint, &pool);
        {
            let mut trackers = ctx.token_trackers.write();
            trackers.insert(sk.clone(), TokenTracker::new(&mint, &pool, "raydium", 1, 0));
        }
        {
            let mut pi = PoolInfo::new(pool.clone(), "raydium".to_string(), 100);
            pi.dex_pool_accounts = Some(vec![]);
            ctx.mint_pools.write().insert(mint.clone(), vec![pi]);
        }

        let probe_fill = 7_159_492_133u64;
        let sol_spent = 250_000_000u64;
        let mut meta = std::collections::HashMap::new();
        meta.insert("side".to_string(), "BUY".to_string());
        meta.insert("entry_kind".to_string(), "probe".to_string());
        let orphan = ExecutionResult {
            header: RecordHeader::new("test", BUILD_VERSION, "run-test"),
            execution_id: "ex-orphan-probe-scale".to_string(),
            decision_id: "dec-orphan-probe-scale".to_string(),
            intent_id: "int-orphan-probe-scale-001".to_string(),
            source: "momentum-bot".to_string(),
            token_mint: Some(mint.clone()),
            signature: Some("sig-orphan-probe-scale".to_string()),
            bundle_id: None,
            status: ExecutionStatus::Confirmed,
            fill_in: Some(ExplicitAmount::new(sol_spent, 9)),
            fill_out: Some(ExplicitAmount::new(probe_fill, 6)),
            fill_status: Some(FillStatus::Complete),
            fill_unavailable_reason: None,
            confirmed_slot: Some(100),
            block_time_unix_ms: None,
            fees: None,
            pnl: None,
            wallet_sol_delta_lamports: Some(-(sol_spent as i128)),
            error_message: None,
            error_code: None,
            latency_ms: Some(1),
            metadata: meta,
        };
        Arc::clone(&ctx).handle_execution_result(&orphan);

        let upd = pool_cache_update_stub(
            &mint,
            &pool,
            10_000_000_000_000u64,
            1_000_000_000_000u64,
            900_000_000u64,
            1,
        );
        assert!(
            pool_cache_sync::apply_pool_cache_update(&ctx.live_pool_cache, &upd),
            "pool cache apply failed"
        );

        let signals = ctx.check_for_signals();
        assert_eq!(
            signals.len(),
            1,
            "scale-in expected after orphan probe state"
        );
        assert_eq!(signals[0].kind, EntryKind::ScaleIn);
    }

    /// Scope 57: exit sizing prefers JetStream wallet snapshot when overlay understates holdings.
    #[tokio::test]
    async fn scope57_exit_amount_uses_wallet_authority_when_overlay_smaller() {
        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context_with_intent_worker(jsonl_writer, MomentumConfig::default());

        let mint = "WalletAuthMintttttttttttttttttttttttttttttt";
        let pool = "WalletAuthPoolttttttttttttttttttttttttttttt";
        ctx.register_pool(mint, pool, "raydium", 1);
        ctx.update_pool_accounts(mint, pool, vec!["a0".to_string(), "a1".to_string()]);

        let probe_only = 14_099_749_285u64;
        let wallet_total = probe_only.saturating_add(38_711_432_312);
        ctx.open_position(OpenPositionParams {
            mint,
            pool,
            dex: "raydium",
            entry_price: 1.0,
            token_decimals: 6,
            token_amount: probe_only,
            sol_invested: 1_000_000,
            token_program: None,
            creator: None,
            entry_confirmed_slot: 100,
            initial_bonding: None,
        });
        ctx.cache_wallet_balance_snapshot_raw(mint, wallet_total);

        generate_and_publish_exit_intent(
            ctx.as_ref(),
            mint,
            pool,
            "raydium",
            "TIME_EXIT",
            "test",
            probe_only,
            None,
        )
        .await
        .expect("exit intent");
        wait_for_intent_jsonl_records(ctx.as_ref(), 1).await;

        let path = ctx.jsonl_writer.current_path().expect("jsonl path");
        let content = std::fs::read_to_string(path).expect("read jsonl");
        let line = content.lines().last().expect("intent line");
        let intent: TradeIntent = serde_json::from_str(line).expect("parse TradeIntent");
        assert_eq!(
            intent.required_capital.raw, wallet_total,
            "exit must use wallet authority when overlay is probe-only"
        );
    }

    /// Scope A: BondingCurveProgress at 100% before `open_position` is merged via `initial_bonding`.
    #[tokio::test]
    async fn pending_bonding_complete_before_open_applied_at_confirm() {
        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);

        let mint = "mintBC";
        ctx.register_pending_buy_entry_after_publish(PendingBuyPublishMeta {
            intent_id: "int-bc",
            mint,
            pool: "poolBC",
            dex: "pumpfun",
            intended_sol: 1_000_000,
            entry_kind: Some(EntryKind::Probe),
            signal_slot: 1,
            slot_seen_at_ms: 1,
            creator: None,
            token_program: None,
        });
        ctx.merge_bonding_curve_progress_geyser(mint, 10_000, true, 500, 1000);
        ctx.merge_bonding_curve_progress_geyser(mint, 5_000, false, 400, 2000);

        let snap = ctx.clone_latest_bonding_snapshot(mint).expect("bonding");
        assert_eq!(snap.progress_bps, 10_000);
        assert!(snap.complete);

        let initial = ctx.clone_latest_bonding_snapshot(mint);
        ctx.remove_pending_buy_entry_by_intent("int-bc");
        ctx.open_position(OpenPositionParams {
            mint,
            pool: "poolBC",
            dex: "pumpfun",
            entry_price: 1.0,
            token_decimals: 6,
            token_amount: 1_000_000,
            sol_invested: 1_000_000_000,
            token_program: None,
            creator: None,
            entry_confirmed_slot: 600,
            initial_bonding: initial,
        });
        let pos = ctx.positions.read().get(mint).expect("pos").clone();
        assert_eq!(pos.bonding_curve_progress_bps, Some(10_000));
    }

    #[test]
    fn bonding_geyser_rejects_stale_lower_slot_update() {
        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);
        let mint = "mStale";
        ctx.register_pending_buy_entry_after_publish(PendingBuyPublishMeta {
            intent_id: "int-stale",
            mint,
            pool: "poolS",
            dex: "pumpfun",
            intended_sol: 1_000_000,
            entry_kind: Some(EntryKind::Probe),
            signal_slot: 10,
            slot_seen_at_ms: 20,
            creator: None,
            token_program: None,
        });
        ctx.merge_bonding_curve_progress_geyser(mint, 10_000, true, 200, 10);
        ctx.merge_bonding_curve_progress_geyser(mint, 3_000, false, 100, 99);
        let s = ctx.clone_latest_bonding_snapshot(mint).unwrap();
        assert_eq!(s.progress_bps, 10_000);
    }

    #[test]
    fn failed_buy_removes_pending_buy_lifecycle() {
        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);
        ctx.register_pending_buy_entry_after_publish(PendingBuyPublishMeta {
            intent_id: "int-1",
            mint: "mintA",
            pool: "poolA",
            dex: "pumpfun",
            intended_sol: 1_000_000,
            entry_kind: Some(EntryKind::Probe),
            signal_slot: 10,
            slot_seen_at_ms: 20,
            creator: None,
            token_program: None,
        });
        assert!(ctx.test_pending_buy_entry_present("int-1", "mintA"));
        ctx.remove_pending_buy_entry_by_intent("int-1");
        assert!(!ctx.test_pending_buy_entry_present("int-1", "mintA"));
    }

    /// Newer-slot Geyser update with lower `progress_bps` must not regress position bonding (max-guard).
    #[tokio::test]
    async fn bonding_position_sync_max_guard_newer_slot_lower_progress() {
        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);
        let mint = "mintMaxGuard";

        ctx.register_pending_buy_entry_after_publish(PendingBuyPublishMeta {
            intent_id: "int-mg",
            mint,
            pool: "poolMG",
            dex: "pumpfun",
            intended_sol: 1_000_000,
            entry_kind: Some(EntryKind::Probe),
            signal_slot: 1,
            slot_seen_at_ms: 1,
            creator: None,
            token_program: None,
        });
        ctx.merge_bonding_curve_progress_geyser(mint, 10_000, true, 100, 1);
        let initial = ctx.clone_latest_bonding_snapshot(mint);
        ctx.open_position(OpenPositionParams {
            mint,
            pool: "poolMG",
            dex: "pumpfun",
            entry_price: 1.0,
            token_decimals: 6,
            token_amount: 1,
            sol_invested: 1,
            token_program: None,
            creator: None,
            entry_confirmed_slot: 150,
            initial_bonding: initial,
        });
        ctx.remove_pending_buy_entry_by_intent("int-mg");

        assert_eq!(
            ctx.positions
                .read()
                .get(mint)
                .unwrap()
                .bonding_curve_progress_bps,
            Some(10_000)
        );

        // Higher slot but lower progress — cache accepts (slot-monotonic); position must not regress.
        ctx.merge_bonding_curve_progress_geyser(mint, 3_000, false, 200, 2);
        assert_eq!(
            ctx.clone_latest_bonding_snapshot(mint)
                .unwrap()
                .progress_bps,
            3_000
        );
        assert_eq!(
            ctx.positions
                .read()
                .get(mint)
                .unwrap()
                .bonding_curve_progress_bps,
            Some(10_000)
        );
    }

    #[tokio::test]
    async fn close_position_clears_latest_bonding_without_pending_buy() {
        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);
        let mint = "mintClr";

        ctx.register_pending_buy_entry_after_publish(PendingBuyPublishMeta {
            intent_id: "int-clr",
            mint,
            pool: "poolClr",
            dex: "pumpfun",
            intended_sol: 1_000_000,
            entry_kind: Some(EntryKind::Probe),
            signal_slot: 1,
            slot_seen_at_ms: 1,
            creator: None,
            token_program: None,
        });
        ctx.merge_bonding_curve_progress_geyser(mint, 8_000, false, 10, 1);
        let initial = ctx.clone_latest_bonding_snapshot(mint);
        ctx.open_position(OpenPositionParams {
            mint,
            pool: "poolClr",
            dex: "pumpfun",
            entry_price: 1.0,
            token_decimals: 6,
            token_amount: 1,
            sol_invested: 1,
            token_program: None,
            creator: None,
            entry_confirmed_slot: 20,
            initial_bonding: initial,
        });
        ctx.remove_pending_buy_entry_by_intent("int-clr");

        ctx.record_mint_info(
            mint,
            MintInfo {
                token_program: String::new(),
                decimals: 6,
                supply: 0,
                mint_authority: None,
                freeze_authority: None,
                last_updated: Instant::now(),
            },
        );
        ctx.merge_pumpfun_migration_complete_evidence(mint, 400, 10);
        ctx.merge_latest_pool_reserve_price_hint_from_update(&pool_cache_update_stub(
            mint,
            "poolClr",
            1_000_000,
            1_000_000_000,
            410,
            10,
        ));
        assert!(ctx.test_has_migration_sticky(mint));
        assert!(ctx.test_has_pool_reserve_hint(mint, "poolClr"));

        Arc::clone(&ctx).close_position(mint);
        assert!(ctx.clone_latest_bonding_snapshot(mint).is_none());
        assert!(!ctx.test_has_migration_sticky(mint));
        assert!(!ctx.test_has_pool_reserve_hint(mint, "poolClr"));
    }

    #[tokio::test]
    async fn close_position_keeps_latest_bonding_when_pending_buy_exists() {
        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);
        let mint = "mintKeep";

        ctx.register_pending_buy_entry_after_publish(PendingBuyPublishMeta {
            intent_id: "int-reentry",
            mint,
            pool: "poolKeep",
            dex: "pumpfun",
            intended_sol: 2_000_000,
            entry_kind: Some(EntryKind::Probe),
            signal_slot: 1,
            slot_seen_at_ms: 2,
            creator: None,
            token_program: None,
        });
        ctx.merge_bonding_curve_progress_geyser(mint, 9_000, false, 50, 1);

        let initial = ctx.clone_latest_bonding_snapshot(mint);
        ctx.open_position(OpenPositionParams {
            mint,
            pool: "poolKeep",
            dex: "pumpfun",
            entry_price: 1.0,
            token_decimals: 6,
            token_amount: 1,
            sol_invested: 1,
            token_program: None,
            creator: None,
            entry_confirmed_slot: 60,
            initial_bonding: initial,
        });

        Arc::clone(&ctx).close_position(mint);
        let snap = ctx
            .clone_latest_bonding_snapshot(mint)
            .expect("bonding kept for pending BUY");
        assert_eq!(snap.progress_bps, 9_000);
        assert!(ctx.test_pending_buy_entry_present("int-reentry", mint));
    }

    #[tokio::test]
    async fn close_position_keeps_scope_b_sticky_when_pending_buy_exists() {
        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);
        let mint = "mintScopeBKeep";

        ctx.register_pending_buy_entry_after_publish(PendingBuyPublishMeta {
            intent_id: "int-scopeb-pend",
            mint,
            pool: "poolKeepB",
            dex: "pumpfun",
            intended_sol: 1_000_000,
            entry_kind: Some(EntryKind::Probe),
            signal_slot: 1,
            slot_seen_at_ms: 1,
            creator: None,
            token_program: None,
        });
        ctx.record_mint_info(
            mint,
            MintInfo {
                token_program: String::new(),
                decimals: 6,
                supply: 0,
                mint_authority: None,
                freeze_authority: None,
                last_updated: Instant::now(),
            },
        );
        ctx.merge_pumpfun_migration_complete_evidence(mint, 111, 5);
        ctx.merge_latest_pool_reserve_price_hint_from_update(&pool_cache_update_stub(
            mint,
            "poolKeepB",
            1_000_000,
            1_000_000_000,
            220,
            6,
        ));
        assert!(ctx.test_has_migration_sticky(mint));
        assert!(ctx.test_has_pool_reserve_hint(mint, "poolKeepB"));

        let initial = ctx.clone_latest_bonding_snapshot(mint);
        ctx.open_position(OpenPositionParams {
            mint,
            pool: "poolKeepB",
            dex: "pumpfun",
            entry_price: 1.0,
            token_decimals: 6,
            token_amount: 1,
            sol_invested: 1,
            token_program: None,
            creator: None,
            entry_confirmed_slot: 50,
            initial_bonding: initial,
        });

        Arc::clone(&ctx).close_position(mint);
        assert!(ctx.test_has_migration_sticky(mint));
        assert!(ctx.test_has_pool_reserve_hint(mint, "poolKeepB"));
        assert!(ctx.test_pending_buy_entry_present("int-scopeb-pend", mint));
    }

    #[test]
    fn scope_c_price_sensitive_market_kinds() {
        assert!(!momentum_scope_c_price_sensitive_market_kind(
            &MarketEventKind::SlotUpdate { current_slot: 42 }
        ));
        assert!(momentum_scope_c_price_sensitive_market_kind(
            &MarketEventKind::BondingCurveProgress {
                mint: "mint".into(),
                bonding_curve: "bc".into(),
                progress_bps: 1,
                complete: false,
            }
        ));
        assert_eq!(
            momentum_scope_c_market_kind_tag(&MarketEventKind::SlotUpdate { current_slot: 0 }),
            "Other"
        );
        assert_eq!(
            momentum_scope_c_market_kind_tag(&MarketEventKind::BondingCurveProgress {
                mint: "m".into(),
                bonding_curve: "b".into(),
                progress_bps: 0,
                complete: false,
            }),
            "BondingCurveProgress"
        );
    }

    #[test]
    fn market_events_batch_interleaved_drain_decision_once_per_batch_semantics() {
        // Multiple price-sensitive events in one batch still map to one drain decision:
        // eligibility is OR of batch flag with state, not per-event.
        assert!(!market_events_batch_wants_interleaved_execution_result_drain(false, 0, 0));
        assert!(!market_events_batch_wants_interleaved_execution_result_drain(true, 0, 0));
        assert!(market_events_batch_wants_interleaved_execution_result_drain(true, 1, 0));
        assert!(market_events_batch_wants_interleaved_execution_result_drain(true, 0, 1));
        assert!(!market_events_batch_wants_interleaved_execution_result_drain(false, 5, 5));
    }

    #[test]
    fn strategy_entry_tick_returns_empty_without_dirty_markers() {
        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);
        assert!(
            ctx.check_for_signals_dirty_priority_tick().is_empty(),
            "no periodic fallback: empty dirty set must not evaluate trackers"
        );
    }

    #[test]
    fn exit_signals_after_market_event_skipped_without_positions() {
        assert!(!should_invoke_exit_signals_after_market_event_processing(
            true, false, 0
        ));
        assert!(!should_invoke_exit_signals_after_market_event_processing(
            false, true, 0
        ));
        assert!(should_invoke_exit_signals_after_market_event_processing(
            true, false, 1
        ));
        assert!(should_invoke_exit_signals_after_market_event_processing(
            false, true, 1
        ));
        assert!(should_invoke_exit_signals_after_market_event_processing(
            true, true, 2
        ));
    }

    #[test]
    fn strategy_entry_tick_evaluates_only_explicitly_dirty_tracker() {
        let cfg = {
            let mut c = MomentumConfig::default();
            c.default_position_lamports = 1_000;
            c.probe_buy_pct = 0.25;
            c.early_min_liquidity_sol = 0.0;
            c.min_unique_buyers = 0;
            c.min_trades_per_min = 0.0;
            c.min_buy_dominance = 0.0;
            c.min_sol_inflow_lamports = 0;
            c.require_mint_authority_renounced = false;
            c.require_freeze_authority_none = false;
            c.top1_buyer_share_cap = 1.0;
            c.top3_buyer_share_cap = 1.0;
            c.repeat_buyer_min_ratio = 0.0;
            c.min_trade_size_lamports = 0;
            c.small_buy_ratio_cap = 1.0;
            c.min_token_age_secs = 0;
            c
        };

        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);
        *ctx.config.write() = cfg.clone();

        let mint = "MintDeferScan777777777777777777777777777";
        let pool = "poolDeferScan777777777777777777777777777";
        assert!(ctx.get_or_create_tracker(mint, pool, "pumpfun", 1, 30_000_000_000));
        {
            let mut trackers = ctx.token_trackers.write();
            let tr = trackers
                .get_mut(&MomentumContext::tracker_storage_key(mint, pool))
                .expect("tracker");
            for i in 0..20 {
                tr.record_trade(
                    &format!("pf{i:03}"),
                    true,
                    200_000_000,
                    2_000_000,
                    &format!("sig{i:03}"),
                    1 + i as u64,
                    &cfg,
                );
            }
        }
        ctx.dirty_entry_tracker_keys.write().clear();
        ctx.mark_entry_eval_dirty_key(&MomentumContext::tracker_storage_key(mint, pool));

        let signals = ctx.check_for_signals_dirty_priority_tick();
        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].pool, pool);
    }

    #[test]
    fn scope_c_execution_result_ingest_lag_ms_saturates() {
        use ironcrab::ipc::ExecutionStatus;
        let mut h = RecordHeader::new("c", BUILD_VERSION, "r");
        h.ts_unix_ms = 10_000;
        let er = ExecutionResult {
            header: h,
            execution_id: "e".into(),
            decision_id: "d".into(),
            intent_id: "i".into(),
            source: "momentum-bot".into(),
            token_mint: None,
            signature: None,
            bundle_id: None,
            status: ExecutionStatus::Confirmed,
            fill_in: None,
            fill_out: None,
            fill_status: None,
            fill_unavailable_reason: None,
            confirmed_slot: Some(99),
            block_time_unix_ms: None,
            fees: None,
            pnl: None,
            wallet_sol_delta_lamports: None,
            error_message: None,
            error_code: None,
            latency_ms: None,
            metadata: Default::default(),
        };
        assert_eq!(execution_result_ingest_lag_ms(&er, 10_050), 50);
        assert_eq!(execution_result_ingest_lag_ms(&er, 9_000), 0);
    }

    #[test]
    fn scope_c_interleaved_jetstream_expires_stays_short_vs_scheduled() {
        assert!(EXECUTION_RESULT_INTERLEAVED_FETCH_EXPIRES <= Duration::from_millis(5));
        assert!(EXECUTION_RESULT_SCHEDULED_FETCH_EXPIRES >= Duration::from_millis(8));
        assert!(
            EXECUTION_RESULT_INTERLEAVED_FETCH_EXPIRES < EXECUTION_RESULT_SCHEDULED_FETCH_EXPIRES
        );
    }

    #[test]
    fn core_market_events_ingest_drain_cap_respects_floor_ceiling_and_cap_hit_streak_bonus() {
        assert_eq!(
            core_market_events_ingest_drain_cap_for_streak(0),
            CORE_MARKET_EVENTS_INGEST_DRAIN_MAX
        );
        assert_eq!(
            core_market_events_ingest_drain_cap_for_streak(10),
            CORE_MARKET_EVENTS_INGEST_DRAIN_MAX + 10 * 16
        );
        assert_eq!(
            core_market_events_ingest_drain_cap_for_streak(10_000),
            CORE_MARKET_EVENTS_INGEST_DRAIN_CAP_CEILING
        );
    }

    fn mk_bonding_progress_event(
        mint: &str,
        slot: Option<u64>,
        ts_unix_ms: u64,
        progress_bps: u32,
    ) -> MarketEvent {
        let mut header = RecordHeader::new("test", BUILD_VERSION, "run");
        header.ts_unix_ms = ts_unix_ms;
        MarketEvent {
            header,
            event_id: format!("evt-{ts_unix_ms}-{progress_bps}"),
            source: "geyser".into(),
            slot,
            kind: MarketEventKind::BondingCurveProgress {
                mint: mint.to_string(),
                bonding_curve: "bc1".into(),
                progress_bps,
                complete: false,
            },
        }
    }

    #[test]
    fn scope_c_bonding_streak_coalesce_picks_latest_slot_per_mint() {
        let streak = vec![
            mk_bonding_progress_event("MintA", Some(10), 1, 1_000),
            mk_bonding_progress_event("MintA", Some(12), 1, 2_000),
            mk_bonding_progress_event("MintA", Some(11), 1, 3_000),
        ];
        let (out, stats) = coalesce_bonding_curve_progress_streak(streak);
        assert_eq!(stats.raw_messages, 3);
        assert_eq!(stats.emitted_events, 1);
        assert_eq!(stats.stale_dropped, 2);
        let MarketEventKind::BondingCurveProgress { progress_bps, .. } = &out[0].kind else {
            panic!("expected BondingCurveProgress");
        };
        assert_eq!(*progress_bps, 2_000);
        assert_eq!(out[0].slot, Some(12));
    }

    #[test]
    fn scope_c_bonding_streak_missing_slot_does_not_replace_slotted() {
        let streak = vec![
            mk_bonding_progress_event("MintB", Some(100), 1, 1_000),
            mk_bonding_progress_event("MintB", None, 9_999, 9_000),
        ];
        let (out, _) = coalesce_bonding_curve_progress_streak(streak);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].slot, Some(100));
    }

    #[test]
    fn scope_c_bonding_streak_two_mints_both_emitted_sorted() {
        let streak = vec![
            mk_bonding_progress_event("Zmint", Some(1), 1, 100),
            mk_bonding_progress_event("Amint", Some(2), 1, 200),
        ];
        let (out, stats) = coalesce_bonding_curve_progress_streak(streak);
        assert_eq!(stats.stale_dropped, 0);
        assert_eq!(out.len(), 2);
        let MarketEventKind::BondingCurveProgress { mint, .. } = &out[0].kind else {
            panic!();
        };
        assert_eq!(mint, "Amint");
    }

    fn mk_dummy_trade_event_for_flatten(event_id: &str) -> MarketEvent {
        let mut header = RecordHeader::new("test", BUILD_VERSION, "run");
        header.ts_unix_ms = 1;
        MarketEvent {
            header,
            event_id: event_id.into(),
            source: "geyser".into(),
            slot: Some(1),
            kind: MarketEventKind::Trade {
                pool_address: "poolFlat1".into(),
                mint: "MintFlatX".into(),
                quote_mint: "So11111111111111111111111111111111111111112".into(),
                trader: "trFlat".into(),
                is_buy: true,
                sol_amount: 1,
                token_amount: 1,
                token_decimals: 6,
                signature: None,
                dex: "pumpfun".into(),
                creator: None,
                token_program: None,
            },
        }
    }

    #[test]
    fn scope_c_flatten_ingest_batch_preserves_order_mixed_trade_and_bcp() {
        let v = vec![
            mk_dummy_trade_event_for_flatten("e0"),
            mk_bonding_progress_event("MintZ", Some(1), 1, 100),
            mk_bonding_progress_event("MintZ", Some(5), 2, 500),
            mk_dummy_trade_event_for_flatten("e1"),
        ];
        let out = flatten_market_events_for_ingest_ordered_batch(v);
        assert_eq!(out.len(), 3);
        assert!(matches!(out[0].kind, MarketEventKind::Trade { .. }));
        assert!(matches!(
            out[1].kind,
            MarketEventKind::BondingCurveProgress { .. }
        ));
        assert_eq!(out[1].slot, Some(5));
        assert!(matches!(out[2].kind, MarketEventKind::Trade { .. }));
    }

    #[test]
    fn scope_c_flatten_ingest_batch_chunks_long_bcp_streak() {
        let mut v = Vec::new();
        for i in 0_u64..40 {
            v.push(mk_bonding_progress_event(
                "MintLong",
                Some(i + 1),
                i,
                (i as u32).saturating_add(10),
            ));
        }
        let out = flatten_market_events_for_ingest_ordered_batch(v);
        assert_eq!(out.len(), 2);
        let MarketEventKind::BondingCurveProgress { progress_bps, .. } = &out[0].kind else {
            panic!("expected BCP");
        };
        assert_eq!(*progress_bps, 41);
        let MarketEventKind::BondingCurveProgress {
            progress_bps: p2, ..
        } = &out[1].kind
        else {
            panic!("expected BCP");
        };
        assert_eq!(*p2, 49);
    }

    #[test]
    fn scope_c_pool_cache_price_path_winner_and_stale_derive_count() {
        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);
        ctx.record_mint_info(
            "TokM",
            MintInfo {
                token_program: String::new(),
                decimals: 6,
                supply: 0,
                mint_authority: None,
                freeze_authority: None,
                last_updated: Instant::now(),
            },
        );
        let updates = vec![
            pool_cache_update_stub("TokM", "poolP", 1_000_000, 1_000_000_000, 10, 1),
            pool_cache_update_stub("TokM", "poolP", 2_000_000, 1_000_000_000, 30, 2),
            pool_cache_update_stub("TokM", "poolP", 3_000_000, 1_000_000_000, 20, 3),
        ];
        let derived: Vec<PoolCacheDerivedTps> = updates
            .iter()
            .map(|u| ctx.derive_tokens_per_sol_from_pool_cache_update(u))
            .collect();
        let (winners, stale) = select_pool_cache_batch_price_path_winners(&updates, &derived);
        assert_eq!(winners.get("poolP").copied(), Some(1));
        assert_eq!(stale, 2);
    }

    #[tokio::test]
    async fn scope_c_pool_cache_coalesced_price_respects_position_pool_only() {
        let tmp = TempDir::new().expect("tempdir");
        let jsonl_writer = test_queued_jsonl_writer(tmp.path());
        let ctx = empty_test_context(jsonl_writer);
        let mint = "TokPool";
        ctx.record_mint_info(
            mint,
            MintInfo {
                token_program: String::new(),
                decimals: 6,
                supply: 0,
                mint_authority: None,
                freeze_authority: None,
                last_updated: Instant::now(),
            },
        );
        ctx.open_position(OpenPositionParams {
            mint,
            pool: "posPool",
            dex: "raydium",
            entry_price: 10.0,
            token_decimals: 6,
            token_amount: 1_000_000,
            sol_invested: 1_000_000_000,
            token_program: None,
            creator: None,
            entry_confirmed_slot: 100,
            initial_bonding: None,
        });
        let updates = vec![
            pool_cache_update_stub(mint, "wrongPool", 5_000_000, 1_000_000_000, 200, 1),
            pool_cache_update_stub(mint, "posPool", 2_000_000, 1_000_000_000, 201, 2),
        ];
        let derived: Vec<PoolCacheDerivedTps> = updates
            .iter()
            .map(|u| ctx.derive_tokens_per_sol_from_pool_cache_update(u))
            .collect();
        let (winners, _) = select_pool_cache_batch_price_path_winners(&updates, &derived);
        let mut applied: u32 = 0;
        for (_, idx) in winners {
            let update = &updates[idx];
            let Some(Some((ref token_mint, tps, _, _, _))) = derived.get(idx) else {
                continue;
            };
            if ctx.update_position_price(
                token_mint,
                *tps,
                None,
                Some(update.pool_address.as_str()),
                Some(update.geyser_slot),
            ) {
                applied = applied.saturating_add(1);
            }
        }
        assert_eq!(applied, 1);
        let pos = ctx.positions.read().get(mint).unwrap().clone();
        assert_eq!(pos.last_price_slot, 201);
    }
}

/// Generate and publish a SELL intent for position exit
#[allow(clippy::too_many_arguments)]
async fn generate_and_publish_exit_intent(
    ctx: &MomentumContext,
    mint: &str,
    original_pool: &str,
    original_dex: &str,
    exit_type: &str,
    reason: &str,
    token_amount: u64,
    // When `Some`, successful publish records `momentum_event_to_intent_publish_ms` only if this
    // timestamp is causal for this intent (same decision chain). Use `None` for timer/reconcile,
    // bootstrap, and any `check_for_exits()`-driven scan where one event is not tied to each exit.
    source_event_ts_unix_ms: Option<u64>,
) -> Result<()> {
    // Authoritative sell size: overlay + optional JetStream wallet snapshot (Scope 57 / PA-5 interim).
    let token_amount = ctx.resolve_exit_token_amount_raw(mint, token_amount);

    // Phase 3: Slippage escalation — when prior sells failed with 6002, increase tolerance
    let max_slippage = {
        let base = ctx.config.read().exit_max_slippage_bps;
        let sell_slippage_fail_count = ctx
            .positions
            .read()
            .get(mint)
            .map(|p| p.sell_slippage_fail_count)
            .unwrap_or(0);
        if sell_slippage_fail_count == 0 {
            base
        } else {
            let escalation = match sell_slippage_fail_count {
                1 => 500,
                2 => 800,
                _ => 1500,
            };
            base.max(escalation)
        }
    };

    // SOL as output (selling tokens for SOL)
    let sol_mint = "So11111111111111111111111111111111111111112";

    // === MULTI-POOL ROUTING: Find best pool for exit ===
    let (pool, dex, pool_accounts, expected_sol, alternatives_checked, sell_routing) =
        match ctx.find_best_sell_pool(mint, token_amount, original_pool, original_dex) {
            Ok((pool, dex, accounts, expected, alts)) => (
                pool,
                dex,
                accounts,
                expected,
                alts,
                "multi_pool".to_string(),
            ),
            Err(e) => {
                // Fallback to original pool if multi-pool routing fails
                warn!(
                    mint = %mint,
                    original_pool = %original_pool,
                    error = %e,
                    "⚠️  Multi-pool routing failed, using original pool"
                );

                // Get accounts: try mint_pools for the specific pool first,
                // then try_get_dex_pool_accounts_for_mint, then empty fallback
                let accounts = {
                let pools = ctx.mint_pools.read();
                pools
                    .get(mint)
                    .and_then(|list| list.iter().find(|p| p.pool_address == original_pool))
                    .and_then(|p| p.dex_pool_accounts.clone())
            }
            .or_else(|| ctx.try_get_dex_pool_accounts_for_mint_pool(mint, original_pool))
            .unwrap_or_else(|| {
                warn!(
                    mint = %mint,
                    pool = %original_pool,
                    dex = %original_dex,
                    "Missing DexPoolAccounts for exit intent; EE will resolve from LivePoolCache"
                );
                Vec::new()
            });

                let routing = if original_dex == "pump_amm" {
                    "pumpswap_fallback"
                } else {
                    "primary"
                };

                (
                    original_pool.to_string(),
                    original_dex.to_string(),
                    accounts,
                    0.0,     // Unknown expected
                    0_usize, // No alternatives checked
                    routing.to_string(),
                )
            }
        };

    // Decimals depend on the token. Prefer decimals from the open position (which was seeded
    // from MarketEventKind::TokenMintInfo), fall back to mint_infos cache, then fallback to 6.
    let token_decimals = {
        let positions = ctx.positions.read();
        positions
            .get(mint)
            .map(|p| p.token_decimals)
            .filter(|d| *d != 0)
            .or_else(|| {
                let mint_infos = ctx.mint_infos.read();
                mint_infos.get(mint).map(|m| m.decimals)
            })
            .unwrap_or_else(|| {
                // Fallback to 6 decimals (standard for memecoins) if position/cache missing
                warn!(
                    mint = %mint,
                    "Using fallback decimals=6 for exit (position/cache missing)"
                );
                6
            })
    };

    let intent_id = ctx.next_intent_id();

    // FIX-22 / A.2: Creator resolution order — Position → TokenTracker (same pool row) → LivePoolCache
    let (creator_opt, last_trade_ratio_opt) = {
        let positions = ctx.positions.read();
        let position_creator = positions.get(mint).and_then(|p| p.creator.clone());
        let position_pool = positions.get(mint).map(|p| p.pool.clone());
        drop(positions);
        let trackers = ctx.token_trackers.read();
        let tracker = position_pool
            .as_ref()
            .and_then(|pool| trackers.get(&MomentumContext::tracker_storage_key(mint, pool)));
        let tracker_creator = tracker.and_then(|t| t.dev_wallet.clone());
        let ratio = tracker.and_then(|t| t.last_trade_ratio());
        drop(trackers);
        let base_creator = position_creator.or(tracker_creator);
        let resolved_creator = ctx.resolve_authoritative_creator(mint, base_creator);
        (resolved_creator, ratio)
    };

    let (last_sol_lamports, last_token_amount) = match last_trade_ratio_opt {
        Some((sol, tok)) => {
            debug!(
                mint = %mint,
                sol_lamports = sol,
                token_amount = tok,
                "Using cached trade ratio for exit"
            );
            (sol, tok)
        }
        None => {
            // CRITICAL FIX: For exits, ALWAYS allow selling even without MarketEvent data
            // Use position entry data or fallback to minimal safe ratio
            warn!(
                mint = %mint,
                pool = %pool,
                dex = %dex,
                exit_type = %exit_type,
                reason = %reason,
                "⚠️  No cached trade ratio - using fallback for emergency exit"
            );

            // Try to get from open position (entry price * token amount)
            let positions = ctx.positions.read();
            if let Some(pos) = positions.get(mint) {
                // Use position's current price estimate
                // entry_price is tokens_per_sol, so: sol_needed = token_amount / entry_price
                let sol_estimate = if pos.entry_price > 0.0 {
                    let token_ui = token_amount as f64 / 10f64.powi(token_decimals as i32);
                    let sol_ui = token_ui / pos.entry_price;
                    (sol_ui * 1e9) as u64
                } else {
                    // Absolute fallback: assume 1 token = 0.0000001 SOL (very pessimistic)
                    100_000 // 0.0001 SOL minimum
                };

                info!(
                    mint = %mint,
                    sol_estimate_lamports = sol_estimate,
                    token_amount = token_amount,
                    entry_price = pos.entry_price,
                    "💡 Using position entry price for exit ratio (emergency mode)"
                );

                (sol_estimate, token_amount)
            } else {
                // ABSOLUTE LAST RESORT: Assume worst-case ratio to force sell
                // Better to sell at terrible price than hold forever
                warn!(
                    mint = %mint,
                    "🆘 EMERGENCY EXIT MODE: No position data, using minimal ratio"
                );
                (100_000, token_amount) // Assume 0.0001 SOL minimum out
            }
        }
    };

    // Estimate expected SOL output using last observed ratio.
    // expected_out_sol_lamports = token_amount * last_sol_lamports / last_token_amount
    let expected_out_sol_lamports: u64 = ((token_amount as u128)
        .saturating_mul(last_sol_lamports as u128)
        / (last_token_amount as u128))
        .min(u64::MAX as u128) as u64;

    let min_out_raw: u64 = ((expected_out_sol_lamports as u128)
        .saturating_mul((10_000u32.saturating_sub(max_slippage)) as u128)
        / 10_000u128)
        .min(u64::MAX as u128) as u64;

    let reason_code = match exit_type {
        "DEV_SELL" => "EXIT_DEV_SELL",
        "LP_REMOVAL" => "EXIT_LP_REMOVAL",
        "STOP_LOSS" => "EXIT_HARD_STOP",
        "TRAILING_STOP" => "EXIT_TRAILING_STOP",
        "TIME_EXIT" => "EXIT_MAX_HOLD_TIME",
        "MOMENTUM_EXIT" => "EXIT_MOMENTUM_FADE",
        "TAKE_PROFIT" => "EXIT_TAKE_PROFIT",
        _ => "EXIT_UNKNOWN",
    };

    // Get token_program for Token-2022 support on SELL.
    // Priority: 1. From persisted position (survives restarts)
    //           2. From mint_infos cache (Geyser TokenMintInfo, in-memory only)
    let token_program_for_sell = {
        let positions = ctx.positions.read();
        positions
            .get(mint)
            .and_then(|p| p.token_program.clone())
            .filter(|tp| !tp.is_empty())
    }
    .or_else(|| {
        let mint_infos = ctx.mint_infos.read();
        mint_infos
            .get(mint)
            .map(|info| info.token_program.clone())
            .filter(|tp| !tp.is_empty())
    });

    if token_program_for_sell.is_some() {
        debug!(
            mint = %mint,
            token_program = ?token_program_for_sell,
            "SELL intent: using cached token_program for Token-2022 support"
        );
    }

    let mut intent = TradeIntent::new(
        "momentum-bot",
        BUILD_VERSION,
        &ctx.run_id,
        intent_id.clone(),
        "momentum-bot",
        IntentTier::Tier1,
        IntentOrigin::StrategyA,
        ExplicitAmount::new(token_amount, token_decimals),
        TradeResources {
            input_mint: mint.to_string(),      // Selling tokens
            output_mint: sol_mint.to_string(), // Receiving SOL
            pools: vec![pool.to_string()],
            accounts: pool_accounts, // Already validated from find_best_sell_pool
            token_program: token_program_for_sell, // Token-2022 support for SELL
        },
        0, // No expected ROI for exits
        max_slippage,
        TradeSide::Sell,
        TradingRegime::Early,
    )
    .with_ttl_ms(3000); // Shorter TTL for exits - urgency

    // Deterministic execution constraints: min_out in SOL lamports.
    intent.execution = Some(TradeExecutionConstraints {
        min_out: Some(ExplicitAmount::new(min_out_raw, 9)),
    });

    // Keep legacy metadata for backward compatibility, and provide routing hints.
    intent
        .metadata
        .insert("min_out_raw".to_string(), min_out_raw.to_string());
    intent.metadata.insert("dex".to_string(), dex.to_string());
    intent
        .metadata
        .insert("reason_code".to_string(), reason_code.to_string());
    intent
        .metadata
        .insert("sell_routing".to_string(), sell_routing);

    // Multi-pool routing metadata
    intent.metadata.insert(
        "multi_pool_alternatives_checked".to_string(),
        alternatives_checked.to_string(),
    );
    intent.metadata.insert(
        "multi_pool_original_pool".to_string(),
        original_pool.to_string(),
    );
    if expected_sol > 0.0 {
        intent.metadata.insert(
            "multi_pool_expected_sol".to_string(),
            format!("{:.9}", expected_sol / 1e9),
        );
    }

    intent
        .metadata
        .insert("reason_detail".to_string(), reason.to_string());
    intent
        .metadata
        .insert("exit_type".to_string(), exit_type.to_string());

    // Phase 2: Include prev_error_code for retry diagnostability
    if let Some(code) = ctx
        .positions
        .read()
        .get(mint)
        .and_then(|p| p.last_sell_error_code.as_ref())
    {
        intent
            .metadata
            .insert("prev_error_code".to_string(), code.clone());
    }

    // PumpFun bonding curve tx building requires the creator/dev wallet for PDA derivation.
    // pump_amm (PumpSwap) does NOT need creator — it uses pool_accounts instead.
    if dex == "pumpfun" {
        let creator = creator_opt.ok_or_else(|| {
            anyhow::anyhow!("cannot generate pumpfun exit: missing dev_wallet/creator")
        })?;
        intent.metadata.insert("creator".to_string(), creator);
    } else if let Some(creator) = creator_opt {
        intent.metadata.insert("creator".to_string(), creator);
    }

    // Close token ATA after full sell to recover rent (~0.002 SOL) for accurate PnL.
    // All momentum exits are full sells, so this is safe.
    intent
        .metadata
        .insert("close_token_ata".to_string(), "true".to_string());

    // Include current open positions count for execution-engine risk check.
    // execution-engine uses this instead of tracking positions itself (Single Source of Truth).
    let current_open_positions = ctx.positions.read().len();
    intent.metadata.insert(
        "current_open_positions".to_string(),
        current_open_positions.to_string(),
    );

    // K Phase 1: Slot-to-Send Latency - propagate slot from last event
    let slot = ctx
        .last_event_slot
        .load(std::sync::atomic::Ordering::Relaxed);
    let ts_ms = ctx
        .last_event_ts_ms
        .load(std::sync::atomic::Ordering::Relaxed);
    if slot > 0 {
        intent.metadata.insert("slot".to_string(), slot.to_string());
    }
    if ts_ms > 0 {
        intent
            .metadata
            .insert("slot_seen_at_ms".to_string(), ts_ms.to_string());
    }

    // Register pending intent BEFORE publishing
    ctx.register_sell_intent(&intent_id, mint, &pool, &dex, token_amount);

    info!(
        intent_id = %intent.intent_id,
        pool = %pool,
        mint = %mint,
        dex = %dex,
        exit_type = %exit_type,
        reason = %reason,
        token_amount = token_amount,
        "🔴 Generated EXIT TradeIntent"
    );

    if let Err(e) = ctx
        .enqueue_intent_publish(IntentPublishJob {
            intent,
            post_action: IntentPublishPostAction::Exit {
                mint: mint.to_string(),
                source_event_ts_unix_ms,
                slot_seen_at_ms: ts_ms,
            },
        })
        .await
    {
        ctx.pending_intents.write().remove(&intent_id);
        return Err(e);
    }

    Ok(())
}

/// Process a MarketEvent and update token trackers.
/// Returns `Ok(true)` when the caller should run `process_exit_signals()` (bonding/price-relevant sticky apply).
///
/// When `update_core_nats_subscription_slot_metrics` is true, updates
/// [`record_momentum_market_events_last_applied_slot`] for Core NATS backlog gauges; JetStream wallet
/// snapshot bootstrap/live replay must pass `false` so `momentum_market_events_internal_slot_delta_slots`
/// stays Core-NATS-scoped while `ctx.last_event_slot` still advances for strategy state on every path.
async fn process_market_event(
    ctx: &Arc<MomentumContext>,
    event: &MarketEvent,
    update_core_nats_subscription_slot_metrics: bool,
) -> Result<bool> {
    let _mt = MomentumProcessMarketEventTimer(Instant::now());
    // K Phase 1: Slot-to-Send Latency - store for intent metadata propagation
    if let Some(slot) = event.slot {
        ctx.last_event_slot
            .store(slot, std::sync::atomic::Ordering::Relaxed);
        if update_core_nats_subscription_slot_metrics {
            record_momentum_market_events_last_applied_slot(slot);
        }
    }
    ctx.last_event_ts_ms.store(
        event.header.ts_unix_ms,
        std::sync::atomic::Ordering::Relaxed,
    );

    match &event.kind {
        MarketEventKind::PoolCreated {
            pool_address,
            base_mint,
            quote_mint: _quote_mint,
            dex,
            initial_liquidity_sol,
        } => {
            let slot = event.slot.unwrap_or(0);
            let dex = MomentumContext::normalize_dex_for_execution_engine(dex);
            ctx.record_pool_seen(pool_address, slot);

            // Register pool in multi-pool registry
            ctx.register_pool(base_mint, pool_address, &dex, slot);

            let liq_sol = initial_liquidity_sol
                .map(|d| d.to_string().parse::<f64>().unwrap_or(0.0))
                .unwrap_or(0.0);

            debug!(
                pool = %pool_address,
                base = %base_mint,
                dex = %dex,
                liquidity = liq_sol,
                "🆕 Pool created - starting tracker"
            );

            // Initialize a TokenTracker for this token
            let config = ctx.config.read();
            let min_liq = config.early_min_liquidity_sol;
            drop(config);

            // Create tracker if liquidity meets minimum threshold
            if liq_sol >= min_liq {
                let slot = event.slot.unwrap_or(0);
                let liq_lamports = (liq_sol * 1_000_000_000.0) as u64;
                let created =
                    ctx.get_or_create_tracker(base_mint, pool_address, &dex, slot, liq_lamports);

                if created {
                    let tk = MomentumContext::tracker_storage_key(base_mint, pool_address);
                    let should_pin = ctx
                        .token_trackers
                        .read()
                        .get(&tk)
                        .is_some_and(|t| !t.is_rejected());
                    if should_pin {
                        info!(
                            mint = %base_mint,
                            pool = %pool_address,
                            dex = %dex,
                            liquidity = liq_sol,
                            "📊 Token tracker initialized"
                        );
                        ctx.queue_momentum_active_pool_active(
                            base_mint,
                            pool_address,
                            MomentumActivePinReason::Tracker,
                        );
                    }
                    ctx.flush_momentum_active_pool_publish_queue();
                }
            } else {
                debug!(
                    mint = %base_mint,
                    liquidity = liq_sol,
                    min_required = min_liq,
                    "Skipping token - insufficient liquidity"
                );
            }
        }

        MarketEventKind::DexPoolAccounts {
            dex,
            pool_address,
            base_mint,
            quote_mint,
            accounts,
        } => {
            ctx.record_pool_seen(pool_address, event.slot.unwrap_or(0));
            ctx.record_dex_pool_accounts(dex, pool_address, base_mint, quote_mint, accounts);

            // Update pool accounts in multi-pool registry
            ctx.update_pool_accounts(base_mint, pool_address, accounts.clone());
        }

        MarketEventKind::Trade {
            pool_address,
            mint,
            trader,
            is_buy,
            sol_amount,
            token_amount,
            signature,
            dex: event_dex,
            creator: trade_creator, // Creator from market-data cache (PumpFun)
            token_program: trade_token_program, // Token program from Geyser (PumpFun swaps)
            token_decimals: trade_token_decimals, // Decimals from post_token_balances (Geyser)
            ..  // Ignore quote_mint - we don't need it for momentum detection
        } => {
            // P1: Trade-based Token Discovery
            // If we missed the PoolCreated event (Geyser filter issues), discover via first trade
            let tracker_exists = ctx
                .token_trackers
                .read()
                .contains_key(&MomentumContext::tracker_storage_key(mint, pool_address));

            // Before trade-based tracker creation: dev sell on an untracked pool still needs a row so
            // `record_trade` and exit-signal merge see it (dev_wallet from siblings or creator==trader).
            let is_dev = trade_creator.as_ref().is_some_and(|c| c.as_str() == trader.as_str())
                || {
                    let trackers = ctx.token_trackers.read();
                    let keys = ctx.tracker_storage_keys_for_mint_readonly_resilient(mint.as_str(), &trackers);
                    keys.iter().any(|k| {
                        trackers.get(k).is_some_and(|t| {
                            t.dev_wallet
                                .as_ref()
                                .is_some_and(|dw| dw.as_str() == trader.as_str())
                        })
                    })
                };

            let mut discovery_created = false;
            if !tracker_exists
                && *sol_amount > 0
                && (*is_buy || is_dev)
                && !is_non_tradeable_momentum_mint(mint)
            {
                // Use DEX from event if available, otherwise infer from pool_address pattern
                let slot = event.slot.unwrap_or(0);
                let dex_raw = if !event_dex.is_empty() && event_dex != "unknown" {
                    event_dex.as_str()
                } else if pool_address.starts_with("pump") || pool_address.starts_with("pAMM") {
                    "pump_amm"
                } else {
                    "pumpfun" // Default assumption for Bonding Curve
                };
                let dex = MomentumContext::normalize_dex_for_execution_engine(dex_raw);

                debug!(
                    mint = %mint,
                    pool = %pool_address,
                    dex = %dex,
                    sol = *sol_amount,
                    "🔍 Trade-based discovery: PoolCreated was missed, initializing from trade"
                );

                // Initialize tracker with a conservative/default initial liquidity.
                // PumpFun bonding curve starts with a known ~30 SOL seed; if we missed the
                // PoolCreated event, we still want liquidity gating to behave as expected.
                let initial_liq_lamports = if dex == "pumpfun" {
                    30_000_000_000
                } else {
                    0
                };

                discovery_created =
                    ctx.get_or_create_tracker(mint, pool_address, &dex, slot, initial_liq_lamports);

                if discovery_created {
                    // A.2 Phase 5: Explicitly register pool when discovered via trade
                    ctx.register_pool(mint, pool_address, &dex, slot);
                    let tk = MomentumContext::tracker_storage_key(mint, pool_address);
                    let should_pin = ctx
                        .token_trackers
                        .read()
                        .get(&tk)
                        .is_some_and(|t| !t.is_rejected());
                    if should_pin {
                        info!(
                            mint = %mint,
                            pool = %pool_address,
                            dex = %dex,
                            discovery = "trade",
                            "📊 Token tracker initialized (trade-based discovery)"
                        );
                        ctx.queue_momentum_active_pool_active(
                            mint,
                            pool_address,
                            MomentumActivePinReason::Tracker,
                        );
                    }
                    ctx.flush_momentum_active_pool_publish_queue();
                }
            }

            // Record the trade in the tracker
            let sol_lamports = *sol_amount;
            let token_raw = *token_amount;
            let sig = signature.clone().unwrap_or_default();

            // Update trade data in multi-pool registry
            ctx.update_pool_trade_data(mint, pool_address, event_dex, sol_lamports, token_raw, event.slot.unwrap_or(0));

            // P1: Trade `creator` is mint-level PumpFun metadata (Geyser / market-data cache, no RPC).
            // Apply to every pool-scoped tracker for this mint **before** `record_trade` so sibling pools
            // run the same `dev_wallet` / dev-sell revalidation WAIT logic as the pool that
            // received the Trade event.
            if let Some(ref creator) = trade_creator {
                let config = ctx.config.read().clone();
                let mut trackers = ctx.token_trackers.write();
                let keys =
                    ctx.tracker_storage_keys_for_mint_readonly_resilient(mint.as_str(), &trackers);
                for key in keys {
                    let Some(tracker) = trackers.get_mut(&key) else {
                        continue;
                    };
                    if tracker.mint.as_str() != mint.as_str() {
                        continue;
                    }
                    if tracker.dev_wallet.is_none() {
                        tracker.set_dev_info(creator, 0.0, &config);
                        debug!(
                            mint = %mint,
                            pool = %tracker.pool,
                            creator = %creator,
                            "Set dev_wallet from Trade event (creator_cache, mint-wide)"
                        );
                    }
                }
                drop(trackers);
                ctx.mark_entry_eval_dirty_for_mint(mint);
                // A.2: Propagate creator to position for pumpfun (BC exit after restart)
                let mut positions = ctx.positions.write();
                if let Some(pos) = positions.get_mut(mint) {
                    if pos.dex == "pumpfun" && pos.creator.is_none() {
                        pos.set_creator(creator);
                        debug!(mint = %mint, creator = %creator, "A.2: Set creator on position from Trade event");
                    }
                }
            }

            let recorded = ctx.record_trade(
                mint,
                pool_address,
                trader,
                *is_buy,
                sol_lamports,
                token_raw,
                &sig,
                event.slot.unwrap_or(0),
            );
            if !recorded && !discovery_created {
                MomentumContext::maybe_warn_trade_without_tracker(mint, pool_address, *is_buy);
            }

            // P1: If Trade event carries token_program (from PumpFun Geyser parsing), cache it.
            // This enables deterministic ATA creation without waiting for TokenMintInfo event.
            // Critical fix for Token-2022 tokens where TokenMintInfo arrives AFTER intent generation.
            if let Some(ref tp) = trade_token_program {
                let mut infos = ctx.mint_infos.write();
                if !infos.contains_key(mint) {
                    // Create minimal MintInfo entry with token_program.
                    // Other fields will be updated when TokenMintInfo arrives.
                    infos.insert(
                        mint.to_string(),
                        MintInfo {
                            token_program: tp.clone(),
                            decimals: *trade_token_decimals, // Use decimals from Geyser post_token_balances
                            supply: 0,   // Unknown, will be updated by TokenMintInfo
                            mint_authority: None,
                            freeze_authority: None,
                            last_updated: Instant::now(),
                        },
                    );
                    info!(
                        mint = %mint,
                        token_program = %tp,
                        decimals = *trade_token_decimals,
                        "📦 Token info cached from Trade event (token_program + decimals)"
                    );
                } else {
                    // Entry exists - update decimals if it was set to default (9)
                    if let Some(info) = infos.get_mut(mint) {
                        if info.decimals == 9 && *trade_token_decimals != 9 {
                            info.decimals = *trade_token_decimals;
                            debug!(
                                mint = %mint,
                                decimals = *trade_token_decimals,
                                "📦 Updated decimals from Trade event"
                            );
                        }
                    }
                }
            } else if *trade_token_decimals != 9 {
                // No token_program in Trade, but we have decimals - cache them anyway
                let mut infos = ctx.mint_infos.write();
                if let Some(info) = infos.get_mut(mint) {
                    if info.decimals == 9 {
                        info.decimals = *trade_token_decimals;
                        debug!(
                            mint = %mint,
                            decimals = *trade_token_decimals,
                            "📦 Updated decimals from Trade event (no token_program)"
                        );
                    }
                } else {
                    // Create entry with decimals only, token_program unknown
                    infos.insert(
                        mint.to_string(),
                        MintInfo {
                            token_program: String::new(), // Unknown, will be updated by TokenMintInfo
                            decimals: *trade_token_decimals,
                            supply: 0,
                            mint_authority: None,
                            freeze_authority: None,
                            last_updated: Instant::now(),
                        },
                    );
                    debug!(
                        mint = %mint,
                        decimals = *trade_token_decimals,
                        "📦 Decimals cached from Trade event (token_program unknown)"
                    );
                }
            }

            if trade_token_program.is_some() || *trade_token_decimals != 9 {
                ctx.mark_entry_eval_dirty_for_mint(mint);
            }

            // Update open position price estimate (tokens_UI per SOL_UI) based on trade ratio.
            // CRITICAL: Use UI-normalized amounts (raw / 10^decimals) to match entry_price units.
            if sol_lamports > 0 && token_raw > 0 {
                let token_decimals = ctx.mint_infos.read().get(mint).map(|m| m.decimals).unwrap_or(6);
                let tok_ui = token_raw as f64 / 10f64.powi(token_decimals as i32);
                let sol_ui = sol_lamports as f64 / 1_000_000_000.0;
                let tokens_per_sol = tok_ui / sol_ui;
                trace!(
                    mint = %mint,
                    is_buy = *is_buy,
                    sol_lamports,
                    token_raw,
                    token_decimals,
                    tok_ui,
                    sol_ui,
                    tokens_per_sol,
                    "Trade event updating position price"
                );
                let trade = TradeEvent {
                    timestamp: Instant::now(),
                    slot: event.slot.unwrap_or(0),
                    trader: trader.to_string(),
                    is_buy: *is_buy,
                    sol_amount: sol_lamports,
                    token_amount: token_raw,
                    signature: sig.clone(),
                };
                ctx.update_position_price(
                    mint,
                    tokens_per_sol,
                    Some(trade),
                    Some(pool_address),
                    event.slot,
                );
            }

            debug!(
                pool = %pool_address,
                mint = %mint,
                trader = %trader,
                is_buy = is_buy,
                sol_lamports = sol_lamports,
                "Trade recorded"
            );
        }

        MarketEventKind::LiquidityRemoved {
            pool_address,
            mint,
            sol_amount,
            ..
        } => {
            // LP removal - potential rug signal
            let sol_lamports = *sol_amount;
            ctx.record_lp_removal(mint, event.slot);

            warn!(
                pool = %pool_address,
                mint = %mint,
                sol_removed = sol_lamports,
                "🚨 LP REMOVAL DETECTED - blacklisting token"
            );
        }

        MarketEventKind::DevWalletIdentified {
            mint,
            dev_wallet,
            supply_percentage,
        } => {
            ctx.record_dev_info(mint, dev_wallet, *supply_percentage);

            let config = ctx.config.read();
            let max_dev_pct = config.max_dev_supply_pct;
            drop(config);

            if *supply_percentage > max_dev_pct {
                // Blacklisting is applied inside TokenTracker::set_dev_info.
                // We still log here even if the tracker doesn't exist yet.
                warn!(
                    mint = %mint,
                    dev_wallet = %dev_wallet,
                    supply_pct = supply_percentage,
                    max_allowed = max_dev_pct,
                    "🚫 Dev wallet identified - supply too high (will be blacklisted)"
                );
            } else {
                info!(
                    mint = %mint,
                    dev_wallet = %dev_wallet,
                    supply_pct = supply_percentage,
                    "✅ Dev wallet identified - supply within limits"
                );
            }
        }

        MarketEventKind::TokenMintInfo {
            mint,
            token_program,
            decimals,
            supply,
            mint_authority,
            freeze_authority,
        } => {
            ctx.record_mint_info(
                mint,
                MintInfo {
                    token_program: token_program.clone(),
                    decimals: *decimals,
                    supply: *supply,
                    mint_authority: mint_authority.clone(),
                    freeze_authority: freeze_authority.clone(),
                    last_updated: Instant::now(),
                },
            );
            let exit_maybe = {
                let mut positions = ctx.positions.write();
                if let Some(pos) = positions.get_mut(mint) {
                    ctx.apply_latest_sticky_state_to_position(mint, pos)
                } else {
                    false
                }
            };
            return Ok(exit_maybe);
        }

        MarketEventKind::SlotUpdate { current_slot } => {
            // Advance chain head even when the flattened `MarketEvent.slot` is absent (I-16 / burst-safe filters).
            // Monotonic: ignore stale SlotUpdate (redelivery / reorder) so chain head never regresses.
            ctx.last_event_slot.fetch_max(
                *current_slot,
                std::sync::atomic::Ordering::Relaxed,
            );
            debug!(current_slot, "Slot update");
        }

        MarketEventKind::WalletBalanceSnapshot {
            mint,
            balance_raw,
            decimals,
            ..
        } => {
            // P0: Position Reconciliation after restart
            // Published by market-data at startup AND periodically to sync wallet state.
            // Handles: manual sales, emergency liquidations, external transfers.

            // FIX-36: Never track SOL/WSOL as a token position — it's the quote currency
            if mint == "So11111111111111111111111111111111111111112"
                || mint == "NATIVE_SOL"
                || mint == "11111111111111111111111111111111"
            {
                debug!(mint = %mint, balance_raw = *balance_raw, "Ignoring SOL/WSOL WalletBalanceSnapshot (not a tradeable position)");
                return Ok(false);
            }

            ctx.cache_wallet_balance_snapshot_raw(mint, *balance_raw);

            if *balance_raw == 0 {
                ctx.latest_wallet_balance_raw_by_mint.write().remove(mint);
                // Remove ghost position if wallet balance is zero
                let was_tracked = {
                    let mut positions = ctx.positions.write();
                    positions.remove(mint).is_some()
                };
                // FIX-35: Also remove from orphaned_mints if it was waiting for reconciliation
                ctx.orphaned_mints.write().remove(mint);
                if was_tracked {
                    info!(
                        mint = %mint,
                        "🧹 Position auto-closed: WalletBalanceSnapshot shows balance=0 (manual sale or external transfer)"
                    );
                    ctx.delete_position_from_kv(mint).await;
                } else {
                    debug!(
                        mint = %mint,
                        "WalletBalanceSnapshot: balance=0 but no position tracked (OK)"
                    );
                }
            } else {
                // Position exists in wallet - verify we're tracking it
                let has_position = { ctx.positions.read().contains_key(mint) };
                if has_position {
                    debug!(
                        mint = %mint,
                        balance_raw = *balance_raw,
                        "✅ WalletBalanceSnapshot: position verified in wallet"
                    );
                } else if let Some(reconciled) =
                    ctx.build_reconciled_position(mint, *balance_raw, *decimals)
                {
                    let hold_secs = reconciled.entry_time.elapsed().as_secs();
                    {
                        let mut positions = ctx.positions.write();
                        positions.insert(mint.to_string(), reconciled.clone());
                    }
                    info!(
                        mint = %mint,
                        pool = %reconciled.pool,
                        dex = %reconciled.dex,
                        balance_raw = *balance_raw,
                        hold_secs = hold_secs,
                        "🧭 Wallet snapshot reconciled into position (timed exit will apply)"
                    );
                    ctx.save_position_to_kv(mint, &reconciled).await;
                    return Ok(true);
                } else {
                    // Wallet has tokens but no pool known yet. Store as orphaned so
                    // we can reconcile later when PoolCreated/DexPoolAccounts arrives.
                    ctx.orphaned_mints.write().insert(mint.to_string(), (*balance_raw, *decimals));
                    warn!(
                        mint = %mint,
                        balance_raw = *balance_raw,
                        "WalletBalanceSnapshot: tokens present but no known pool — added to orphaned_mints for lazy reconciliation"
                    );
                }
            }
        }

        MarketEventKind::WalletSnapshotComplete {
            mints_in_wallet,
            is_periodic,
            ..
        } => {
            // P0: Ghost Position Cleanup
            // Close positions for mints NOT in the wallet (closed ATAs that Geyser doesn't report).
            // This is the definitive reconciliation after market-data completes a wallet scan.

            let mints_set: std::collections::HashSet<&str> =
                mints_in_wallet.iter().map(|s| s.as_str()).collect();

            // Grace period: positions younger than 90s are protected from ghost cleanup
            // to avoid race conditions where the wallet snapshot is stale after a restart
            // but the token was just recently purchased.
            const GHOST_CLEANUP_GRACE_SECS: u64 = 90;

            // Phase 1: Identify and remove ghost positions under the write lock (sync only).
            // Collect removed mints so we can delete from KV after releasing the lock.
            let mut ghost_mints: Vec<String> = Vec::new();
            let mut skipped_fresh: Vec<String> = Vec::new();
            let positions_tracked;
            {
                let mut positions = ctx.positions.write();
                positions_tracked = positions.len();
                let position_mints: Vec<String> = positions.keys().cloned().collect();

                for mint in position_mints {
                    if !mints_set.contains(mint.as_str()) {
                        // Check grace period before removing
                        let hold_secs = positions.get(&mint)
                            .map(|p| p.entry_time.elapsed().as_secs())
                            .unwrap_or(u64::MAX);

                        if hold_secs < GHOST_CLEANUP_GRACE_SECS {
                            // Position is too fresh — snapshot may be stale, skip for now
                            skipped_fresh.push(mint);
                            continue;
                        }

                        if let Some(removed) = positions.remove(&mint) {
                            warn!(
                                mint = %mint,
                                pool = %removed.pool,
                                dex = %removed.dex,
                                hold_secs = hold_secs,
                                is_periodic = is_periodic,
                                "👻 Ghost position closed: mint NOT in WalletSnapshotComplete (ATA was closed)"
                            );
                            ghost_mints.push(mint);
                        }
                    }
                }
            } // write lock released here – safe to await

            // Log skipped fresh positions
            for mint in &skipped_fresh {
                warn!(
                    mint = %mint,
                    grace_secs = GHOST_CLEANUP_GRACE_SECS,
                    is_periodic = is_periodic,
                    "⏳ Ghost cleanup skipped: position too fresh (grace period), will retry on next snapshot"
                );
            }

            // Phase 2: Delete from JetStream KV (async, no lock held)
            for mint in &ghost_mints {
                ctx.delete_position_from_kv(mint).await;
            }

            let closed_count = ghost_mints.len();
            if closed_count > 0 || !skipped_fresh.is_empty() {
                info!(
                    closed_count = closed_count,
                    skipped_fresh = skipped_fresh.len(),
                    mints_in_wallet = mints_in_wallet.len(),
                    is_periodic = is_periodic,
                    "✅ WalletSnapshotComplete: reconciliation finished"
                );
            } else {
                debug!(
                    mints_in_wallet = mints_in_wallet.len(),
                    positions_tracked = positions_tracked,
                    is_periodic = is_periodic,
                    "WalletSnapshotComplete: no ghost positions found"
                );
            }
        }

        MarketEventKind::BondingCurveProgress {
            mint,
            progress_bps,
            complete,
            ..
        } => {
            let slot = event.slot.unwrap_or(0);
            let ts_unix_ms = event.header.ts_unix_ms;
            ctx.merge_bonding_curve_progress_geyser(
                mint.as_str(),
                *progress_bps,
                *complete,
                slot,
                ts_unix_ms,
            );
            if *complete {
                ctx.merge_pumpfun_migration_complete_evidence(mint.as_str(), slot, ts_unix_ms);
            }

            // FIX-20: Mark PumpFun pools as migrated when bonding curve completes.
            // This prevents find_best_sell_pool() from selecting a completed PumpFun
            // bonding curve, which would fail on-chain with Custom(6023).
            if *complete {
                let mut pools = ctx.mint_pools.write();
                if let Some(pool_list) = pools.get_mut(mint.as_str()) {
                    for pool in pool_list.iter_mut() {
                        if pool.dex == "pumpfun" && pool.bonding_curve_complete != Some(true) {
                            pool.bonding_curve_complete = Some(true);
                            warn!(
                                mint = %mint,
                                pool = %pool.pool_address,
                                "FIX-20: PumpFun pool marked as migrated (bonding curve complete) — will prefer alternatives for SELL"
                            );
                        }
                    }
                }
                ctx.mark_entry_eval_dirty_for_mint(mint.as_str());
            }
        }

        _ => {
            trace!(event_id = %event.event_id, "Unhandled event type");
        }
    }

    Ok(false)
}
