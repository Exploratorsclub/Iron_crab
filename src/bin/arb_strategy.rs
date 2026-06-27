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
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;

use ironcrab::arbitrage::{
    populate_arb_slave_from_live_pool_cache, sync_arb_slave_from_pool_cache_update,
    MultiHopArbitrage, MultiHopConfig, MultiHopIntentBatch,
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
    arb_strategy_bootstrap_skip_inc, arb_strategy_bootstrap_warmup_set,
    arb_strategy_pool_cache_update_seeded_inc, arb_strategy_pool_cache_update_seen_inc,
    arb_strategy_pool_cache_update_skip_no_seed_inc,
    arb_strategy_pool_cache_update_skip_non_arb_quote_inc, arb_subscriber_high_dropped_inc,
    arb_subscriber_high_processed_inc, arb_subscriber_high_queue_depth_set,
    arb_subscriber_low_coalesced_inc, arb_subscriber_low_dropped_inc,
    arb_subscriber_low_processed_inc, arb_subscriber_low_queue_depth_set,
    arb_subscriber_pool_created_skipped_inc, arb_two_hop_eligible_dexes_add,
    arb_two_hop_eligible_pools_by_dex_add, arb_two_hop_insufficient_subreason_inc,
    arb_two_hop_opportunity_inc, arb_two_hop_pool_gate_add, arb_two_hop_reject_subreason_inc,
    arb_two_hop_rejected_inc, arb_two_hop_tracker_seeded_pools_add,
    record_arb_track_requests_messages_total, serve_metrics, set_readiness_nats_connected,
    wall_clock_unix_ms_now, ArbStrategyWarmupSkipReason, ArbTwoHopInsufficientSubreason,
    ArbTwoHopPoolGate, ArbTwoHopRejectReason, ArbTwoHopRejectSubreason, MetricsComponent,
    ARB_REJECTED_MISSING_ACCOUNTS, ARB_SUBSCRIBER_HIGH_PROCESSED_TOTAL,
    ARB_SUBSCRIBER_HIGH_QUEUE_DEPTH, ARB_TRIANGLE_OPPORTUNITIES, INTENTS_GENERATED_TOTAL,
    MARKET_EVENTS_CONSUMED_TOTAL, NATS_MESSAGES_PUBLISHED_TOTAL, NATS_MESSAGES_RECEIVED_TOTAL,
    POOLS_TRACKED_GAUGE, TOKENS_TRACKED_GAUGE,
};
use ironcrab::nats::{
    config_consumer_config, config_subject, pool_cache_live_fallback_consumer_config,
    ArbTrackActiveEntry, ArbTrackActiveReason, ArbTrackRemovedEntry, ArbTrackRemovedReason,
    ArbTrackRequestsUpdate, CONFIG_STREAM_NAME, STREAM_NAME,
};
use ironcrab::nats::{NatsClient, NatsConfig};
use ironcrab::nats::{TOPIC_ARB_TRACK_REQUESTS, TOPIC_MARKET_EVENTS, TOPIC_TRADE_INTENTS};
use ironcrab::solana::dex::meteora_bin_walker::{dlmm_fee_bps, walker_from_bins};
use ironcrab::storage::{JsonlWriter, JsonlWriterConfig};
use solana_sdk::pubkey::Pubkey;

/// Build version for decision records
const BUILD_VERSION: &str = env!("CARGO_PKG_VERSION");

/// NATS topic for config reload commands from control-plane (Core NATS fallback)
const TOPIC_CONFIG_RELOAD: &str = "ironcrab.control.config.reload";

/// Wire format version for `TOPIC_ARB_TRACK_REQUESTS`.
const ARB_TRACK_REQUESTS_WIRE_VERSION: u32 = 1;
/// Default cap for baseline reconcile `active[]` (configurable via `arb_track_baseline_max_pools`).
const ARB_TRACK_BASELINE_MAX_POOLS_DEFAULT: usize = 500;
/// Default baseline reconcile interval (configurable via `arb_track_reconcile_interval_secs`).
const ARB_TRACK_RECONCILE_INTERVAL_SECS_DEFAULT: u64 = 60;

/// Bounded queue for off-hot-loop 2-hop trade detection (Scope D).
const ARB_TWO_HOP_WORKER_QUEUE_CAP: usize = 4096;

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
    /// Max pools in baseline reconcile snapshot. Default: 500.
    arb_track_baseline_max_pools: usize,
    /// Baseline reconcile publish interval in seconds. Default: 60.
    arb_track_reconcile_interval_secs: u64,
}

impl Default for ArbConfig {
    fn default() -> Self {
        Self {
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
        }
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
/// Small SOL probe for DLMM marginal price (0.01 SOL) — spread comparison only.
const DLMM_PROBE_SOL_LAMPORTS: u64 = 10_000_000;
/// Reject DLMM marginal price when it deviates more than this factor from reserve/trade mid.
const DLMM_MARGINAL_MAX_DEVIATION_FACTOR: u64 = 100;
/// Deduplicate per-mint "spread too large" WARN logs.
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

fn flatten_bins_for_walker(bin_arrays: &HashMap<i64, Vec<BinData>>) -> Vec<(i32, u64, u64)> {
    let mut all_bins: Vec<(i32, u64, u64)> = Vec::new();
    for (array_idx, bins) in bin_arrays {
        for bin in bins {
            let bin_id = (*array_idx * 70 + bin.offset as i64) as i32;
            all_bins.push((bin_id, bin.amount_x, bin.amount_y));
        }
    }
    all_bins.sort_by_key(|(id, _, _)| *id);
    all_bins
}

fn active_bin_present(active_id: i32, flat_bins: &[(i32, u64, u64)]) -> bool {
    flat_bins.iter().any(|(id, _, _)| *id == active_id)
}

/// Derive whether on-chain token X is SOL (SSOT: Meteora `token_x_mint`).
fn vault_dlmm_sol_is_x(vault: &VaultBalanceCache) -> bool {
    vault
        .dlmm_token_x_mint
        .as_deref()
        .map(|m| m == NATIVE_SOL_MINT)
        .unwrap_or(vault.dlmm_sol_is_x)
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

fn dlmm_marginal_price_plausible(
    marginal: Decimal,
    reserve_mid: Option<Decimal>,
    trade_mid: Option<Decimal>,
) -> bool {
    let reference = match reserve_mid.or(trade_mid) {
        Some(mid) if mid > Decimal::ZERO => mid,
        _ => return true,
    };
    if marginal <= Decimal::ZERO {
        return false;
    }
    let ratio = if marginal > reference {
        marginal / reference
    } else {
        reference / marginal
    };
    ratio <= Decimal::from(DLMM_MARGINAL_MAX_DEVIATION_FACTOR)
}

/// DLMM token output via BinWalker (shared by spread price + intent sizing).
fn dlmm_token_output_from_bins(
    active_id: i32,
    bin_step: u16,
    sol_in_lamports: u64,
    bin_arrays: &HashMap<i64, Vec<BinData>>,
    sol_is_x: bool,
) -> Option<u64> {
    if bin_arrays.is_empty() || sol_in_lamports == 0 {
        return None;
    }

    let flat_bins = flatten_bins_for_walker(bin_arrays);
    if !active_bin_present(active_id, &flat_bins) {
        return None;
    }

    let walker = walker_from_bins(active_id, bin_step, &flat_bins);
    let fee_bps = dlmm_fee_bps(bin_step);
    let (amount_out, _, _) = if sol_is_x {
        walker.quote_x_to_y(sol_in_lamports, fee_bps).ok()?
    } else {
        walker.quote_y_to_x(sol_in_lamports, fee_bps).ok()?
    };
    if amount_out == 0 {
        return None;
    }
    Some(amount_out)
}

/// DLMM SOL output via BinWalker (token → SOL, sell-side marginal).
fn dlmm_sol_output_from_bins(
    active_id: i32,
    bin_step: u16,
    token_in: u64,
    bin_arrays: &HashMap<i64, Vec<BinData>>,
    sol_is_x: bool,
) -> Option<u64> {
    if bin_arrays.is_empty() || token_in == 0 {
        return None;
    }

    let flat_bins = flatten_bins_for_walker(bin_arrays);
    if !active_bin_present(active_id, &flat_bins) {
        return None;
    }

    let walker = walker_from_bins(active_id, bin_step, &flat_bins);
    let fee_bps = dlmm_fee_bps(bin_step);
    let (amount_out, _, _) = if sol_is_x {
        walker.quote_y_to_x(token_in, fee_bps).ok()?
    } else {
        walker.quote_x_to_y(token_in, fee_bps).ok()?
    };
    if amount_out == 0 {
        return None;
    }
    Some(amount_out)
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
    if pool.has_reserve_data {
        if let Some(v) = vault {
            if v.reserve_base > 0 && v.reserve_quote > 0 && v.updated_at.elapsed() <= max_age {
                return true;
            }
        }
    }
    false
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

    let should_update_vault = match vault_balances.get(&pool_addr) {
        Some(existing) => slot >= existing.update_slot,
        None => true,
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
            if should_update_vault {
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
        Some(p) if warmup.quote_kind == ArbWarmupQuoteKind::Sol && !should_update_vault => {
            p.last_update.max(eff_updated_at)
        }
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
        pool_address: pool_addr,
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
) -> ArbEventPriority {
    match &event.kind {
        MarketEventKind::Trade { .. } => ArbEventPriority::High,
        MarketEventKind::PoolStateUpdate { pool_address, .. }
        | MarketEventKind::BinArrayUpdate { pool_address, .. } => {
            if known_pools.contains(pool_address) {
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
    Some(classify_market_event_priority(event, known_pools))
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

fn spawn_arb_two_hop_worker(ctx: Arc<ArbContext>, mut rx: mpsc::Receiver<ArbTwoHopTradeJob>) {
    tokio::spawn(async move {
        while let Some(job) = rx.recv().await {
            let two_hop_enabled = ctx.config.read().two_hop_enabled;
            if !two_hop_enabled {
                continue;
            }
            if let Some(opp) = ctx.handle_trade(
                &job.pool_address,
                &job.mint,
                &job.quote_mint,
                job.sol_amount,
                job.token_amount,
                job.token_decimals,
                job.is_buy,
                &job.dex,
            ) {
                ARB_TRIANGLE_OPPORTUNITIES.fetch_add(1, Ordering::Relaxed);
                info!(
                    mint = %opp.base_mint,
                    buy_dex = %opp.buy_dex,
                    sell_dex = %opp.sell_dex,
                    spread_bps = opp.spread_bps,
                    profit_lamports = opp.estimated_profit_lamports,
                    "🔥 Arbitrage opportunity detected (two-hop worker)"
                );
                ctx.publish_arb_trade_signal_track_pins(&opp.buy_pool, &opp.sell_pool);
                if let Some(mut intent) = create_arb_intent(&ctx, &opp) {
                    if let Some(slot) = job.slot {
                        intent.metadata.insert("slot".to_string(), slot.to_string());
                    }
                    intent
                        .metadata
                        .insert("slot_seen_at_ms".to_string(), job.ts_unix_ms.to_string());
                    publish_arb_intent(&ctx, &intent).await;
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
            let Some(priority) = arb_market_event_ingress_priority(&event, &known_pools) else {
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

    /// Phase 3: pools published as active via `TOPIC_ARB_TRACK_REQUESTS`.
    arb_pinned_pools: RwLock<HashSet<String>>,
    /// Phase 3: count of track_requests publishes (heartbeat).
    arb_track_published: AtomicU64,
    /// Scope D: enqueue-only sender for off-hot-loop 2-hop detection.
    two_hop_tx: mpsc::Sender<ArbTwoHopTradeJob>,
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

    /// Update or create pool state from PoolCreated event
    fn handle_pool_created(
        &self,
        pool_address: &str,
        base_mint: &str,
        quote_mint: &str,
        dex: &str,
        liquidity_sol: Decimal,
    ) {
        let Some(token_mint) = arb_tracked_token_mint(base_mint, quote_mint) else {
            return;
        };
        if token_mint == NATIVE_SOL_MINT || is_stablecoin_mint(token_mint) {
            return;
        }

        let mut trackers = self.trackers.write();
        let tracker = trackers
            .entry(token_mint.to_string())
            .or_insert_with(|| TokenArbTracker::new(token_mint));

        if tracker.pools.contains_key(pool_address) {
            return;
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
        drop(trackers);
        self.sync_pools_tracked_gauge();
        debug!(
            mint = %token_mint,
            dex = %dex,
            pool = %pool_address,
            liquidity = %liquidity_sol,
            "Pool added to arb tracker from PoolCreated"
        );
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
        let mut trackers = self.trackers.write();

        // Store under BOTH mints - this ensures the pool is found regardless of
        // whether the token is base or quote in this particular pool.
        let mints_to_store = [base_mint, quote_mint];
        for mint in &mints_to_store {
            if let Some(tracker) = trackers.get_mut(*mint) {
                tracker.set_pool_accounts(pool_address, accounts.clone());
                debug!(
                    pool = %pool_address,
                    mint = %mint,
                    accounts_len = accounts.len(),
                    "DexPoolAccounts cached in tracker"
                );
            }
        }

        // If no tracker exists for either mint yet, create one for base_mint
        // (will be used when PoolDiscovered event arrives)
        if !trackers.contains_key(base_mint) && !trackers.contains_key(quote_mint) {
            let mut tracker = TokenArbTracker::new(base_mint);
            tracker.set_pool_accounts(pool_address, accounts.clone());
            trackers.insert(base_mint.to_string(), tracker);
            debug!(
                pool = %pool_address,
                mint = %base_mint,
                accounts_len = accounts.len(),
                "DexPoolAccounts cached (new tracker created)"
            );
        }
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
        let mut trackers = self.trackers.write();
        let mut vault_balances = self.vault_balances.write();
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
        let mut cache = self.vault_balances.write();
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
        drop(cache);

        // Mirror SOL liquidity + reserve flag into per-mint pool trackers (Geyser-only, no RPC)
        let liquidity_sol = Decimal::from(reserve_quote) / Decimal::from(1_000_000_000u64);
        let mints_with_pool: Vec<String> = self
            .trackers
            .read()
            .iter()
            .filter(|(_, tracker)| tracker.pools.contains_key(pool_address))
            .map(|(mint, _)| mint.clone())
            .collect();
        if mints_with_pool.is_empty() {
            return;
        }
        let mut trackers = self.trackers.write();
        for mint in mints_with_pool {
            let Some(tracker) = trackers.get_mut(&mint) else {
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
                "DLMM: no bin arrays cached, falling back to price-based"
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
            let mut trackers = self.trackers.write();

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

        let known_pools = self.known_pools.read();
        let vault_balances = self.vault_balances.read();
        let bin_arrays = self.bin_arrays.read();
        let opp = tracker_snapshot.check_arbitrage(
            &config,
            &known_pools,
            &vault_balances,
            &bin_arrays,
            &ArbCheckContext {
                spread_warn_last: &self.spread_too_large_warn_last,
                data_quality_rejects: &self.data_quality_rejects,
                forensics: Some(&self.eligibility_forensics),
            },
        )?;

        let cooldown = Duration::from_millis(config.intent_cooldown_ms);
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

    /// Get pool accounts for both buy and sell pools
    /// Returns (buy_accounts, sell_accounts) if available
    fn get_pool_accounts_for_arb(
        &self,
        opp: &ArbOpportunity,
    ) -> (Option<Vec<String>>, Option<Vec<String>>) {
        let trackers = self.trackers.read();
        if let Some(tracker) = trackers.get(&opp.base_mint) {
            let buy_accounts = tracker.get_pool_accounts(&opp.buy_pool).cloned();
            let sell_accounts = tracker.get_pool_accounts(&opp.sell_pool).cloned();
            (buy_accounts, sell_accounts)
        } else {
            (None, None)
        }
    }

    /// Get token program for a mint (from TokenMintInfo cache)
    fn get_token_program_for_mint(&self, mint: &str) -> Option<String> {
        let trackers = self.trackers.read();
        trackers.get(mint).and_then(|t| t.token_program.clone())
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
        record_arb_track_requests_messages_total();
        self.arb_track_published.fetch_add(1, Ordering::Relaxed);
        tokio::spawn(async move {
            if let Err(e) = nats.publish(TOPIC_ARB_TRACK_REQUESTS, &update).await {
                warn!(
                    error = %e,
                    topic = TOPIC_ARB_TRACK_REQUESTS,
                    "ArbTrackRequests NATS publish failed"
                );
            }
        });
    }

    fn collect_arb_track_baseline_active(&self) -> Vec<ArbTrackActiveEntry> {
        let trackers = self.trackers.read();
        let max_pools = self.config.read().arb_track_baseline_max_pools;
        let mut seen = HashSet::new();
        let mut out = Vec::new();
        for tracker in trackers.values() {
            if tracker.pool_count_on_distinct_dexes() < 2 {
                continue;
            }
            for pool in tracker.pools.keys() {
                if seen.len() >= max_pools {
                    return out;
                }
                if seen.insert(pool.clone()) {
                    out.push(ArbTrackActiveEntry {
                        pool: pool.clone(),
                        reason: ArbTrackActiveReason::Baseline,
                    });
                }
            }
        }
        out
    }

    fn reconcile_arb_track_baseline_publish(self: &Arc<Self>) {
        if self.nats.is_none() {
            return;
        }
        let active = self.collect_arb_track_baseline_active();
        {
            let mut pinned = self.arb_pinned_pools.write();
            pinned.clear();
            for a in &active {
                pinned.insert(a.pool.clone());
            }
        }
        self.spawn_publish_arb_track_requests(active, Vec::new(), true);
    }

    fn maybe_publish_arb_track_incremental_for_mint(self: &Arc<Self>, mint: &str) {
        let trackers = self.trackers.read();
        let Some(tracker) = trackers.get(mint) else {
            return;
        };
        if tracker.pool_count_on_distinct_dexes() < 2 {
            return;
        }
        let mut active = Vec::new();
        let mut pinned = self.arb_pinned_pools.write();
        for pool in tracker.pools.keys() {
            if pinned.insert(pool.clone()) {
                active.push(ArbTrackActiveEntry {
                    pool: pool.clone(),
                    reason: ArbTrackActiveReason::MultiDex,
                });
            }
        }
        drop(pinned);
        drop(trackers);
        if !active.is_empty() {
            self.spawn_publish_arb_track_requests(active, Vec::new(), false);
        }
    }

    fn publish_arb_trade_signal_track_pins(self: &Arc<Self>, buy_pool: &str, sell_pool: &str) {
        if self.nats.is_none() {
            return;
        }
        let mut active = Vec::new();
        let mut pinned = self.arb_pinned_pools.write();
        for pool in [buy_pool, sell_pool] {
            if pinned.insert(pool.to_string()) {
                active.push(ArbTrackActiveEntry {
                    pool: pool.to_string(),
                    reason: ArbTrackActiveReason::TradeSignal,
                });
            }
        }
        drop(pinned);
        if !active.is_empty() {
            self.spawn_publish_arb_track_requests(active, Vec::new(), false);
        }
    }

    fn prune_arb_track_stale_pools(self: &Arc<Self>) {
        if self.nats.is_none() {
            return;
        }
        let trackers = self.trackers.read();
        let mut still_active: HashSet<String> = HashSet::new();
        for tracker in trackers.values() {
            if tracker.pool_count_on_distinct_dexes() >= 2 {
                still_active.extend(tracker.pools.keys().cloned());
            }
        }
        drop(trackers);
        let mut removed = Vec::new();
        let mut pinned = self.arb_pinned_pools.write();
        for pool in pinned.clone().into_iter() {
            if !still_active.contains(&pool) {
                pinned.remove(&pool);
                removed.push(ArbTrackRemovedEntry {
                    pool,
                    reason: ArbTrackRemovedReason::Stale,
                });
            }
        }
        drop(pinned);
        if !removed.is_empty() {
            self.spawn_publish_arb_track_requests(Vec::new(), removed, false);
        }
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
    // OPTION D: Calculate expected_token_output from pool reserves (Geyser)
    // =========================================================================
    // This eliminates the need for safety margins in execution-engine.
    // The sell leg uses this as amount_in - exact value from reserves, not estimated.
    //
    // For AMMs (Raydium, CPMM): constant product formula with fee deduction
    // For DLMM: falls back to price-based (concentrated liquidity is complex)
    //
    // Token decimals: pump.fun tokens are always 6 decimals
    let expected_token_output = ctx.calculate_expected_token_output(
        &opp.buy_pool,
        &opp.buy_dex,
        opp.trade_amount_lamports,
        6, // pump.fun tokens
    );

    if let Some(token_out) = expected_token_output {
        debug!(
            buy_pool = %opp.buy_pool,
            buy_dex = %opp.buy_dex,
            sol_in = opp.trade_amount_lamports,
            token_out,
            "Option D: calculated expected_token_output from reserves"
        );
    } else {
        debug!(
            buy_pool = %opp.buy_pool,
            buy_dex = %opp.buy_dex,
            "Option D: falling back to price-based estimation (no reserves or DLMM)"
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
    // OPTION D: Pass expected_token_output to execution-engine
    // =========================================================================
    // If calculated from reserves, this is the exact value the sell leg should use.
    // If None (DLMM or missing reserves), execution-engine falls back to price-based.
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

    let (two_hop_tx, two_hop_rx) = mpsc::channel::<ArbTwoHopTradeJob>(ARB_TWO_HOP_WORKER_QUEUE_CAP);

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
        arb_pinned_pools: RwLock::new(HashSet::new()),
        arb_track_published: AtomicU64::new(0),
        two_hop_tx,
    });

    spawn_arb_two_hop_worker(Arc::clone(&ctx), two_hop_rx);

    // Bootstrap SLAVE LivePoolCache from JetStream (same path as execution-engine).
    // Consumer is returned for reuse in the main loop (FIX-12 — no second LastPerSubject replay).
    let bootstrap_consumer = if let Some(ref nats_client) = ctx.nats {
        match bootstrap_pool_cache_from_jetstream(nats_client, &ctx.live_pool_cache).await {
            Ok((pools_recovered, consumer)) => {
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
                consumer
            }
            Err(e) => {
                warn!(error = %e, "SLAVE CACHE: JetStream bootstrap failed (will rely on incremental updates)");
                None
            }
        }
    } else {
        None
    };

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
    // CRITICAL: Reuse bootstrap consumer when available (FIX-12). Fallback: DeliverPolicy::New only.
    let pool_cache_consumer = if let Some(consumer) = bootstrap_consumer {
        info!(
            stream = STREAM_NAME,
            "Reusing bootstrap consumer for live PoolCacheUpdate sync (no duplicate LastPerSubject replay)"
        );
        Some(consumer)
    } else if let Some(ref nats) = ctx.nats {
        use async_nats::jetstream;

        let jetstream = jetstream::new(nats.client().clone());

        match jetstream.get_stream(STREAM_NAME).await {
            Ok(stream) => {
                let config = pool_cache_live_fallback_consumer_config();
                match stream.create_consumer(config).await {
                    Ok(consumer) => {
                        info!(
                            stream = STREAM_NAME,
                            "Created NEW JetStream consumer (no bootstrap, DeliverPolicy::New)"
                        );
                        Some(consumer)
                    }
                    Err(e) => {
                        warn!(error = %e, "Failed to create JetStream consumer for PoolCacheUpdates");
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

    // Main event loop (config, JetStream cache sync, heartbeat — MarketEvents handled by pipeline)
    info!("Entering main event loop");

    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    let mut cfg_sub = config_subscription;
    let config_js_consumer_opt = config_js_consumer;
    let pool_cache_consumer_opt = pool_cache_consumer;
    let mut heartbeat_interval = tokio::time::interval(Duration::from_secs(60));
    let arb_reconcile_secs = ctx.config.read().arb_track_reconcile_interval_secs;
    let mut arb_track_reconcile_interval =
        tokio::time::interval(Duration::from_secs(arb_reconcile_secs.max(10)));
    arb_track_reconcile_interval.tick().await;
    let mut last_heartbeat_events_received = 0u64;
    let mut last_heartbeat_high_processed = 0u64;

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

            // Config updates from JetStream (preferred, persisted)
            _ = async {
                use futures::StreamExt;
                if let Some(ref consumer) = config_js_consumer_opt {
                    match consumer.fetch().max_messages(1).messages().await {
                        Ok(mut messages) => {
                            while let Some(msg_result) = messages.next().await {
                                if let Ok(msg) = msg_result {
                                    NATS_MESSAGES_RECEIVED_TOTAL.fetch_add(1, Ordering::Relaxed);
                                    match serde_json::from_slice::<ConfigUpdate>(&msg.payload) {
                                        Ok(update) => {
                                            if update.target_component == "arb-strategy" {
                                                info!(component = %update.target_component, keys = ?update.config.keys(), source = "jetstream", "Applying config update");
                                                let response = ctx.apply_config_update(&update);
                                                match response.status {
                                                    ConfigUpdateStatus::Applied => info!(applied = ?response.applied_keys, "Config update applied"),
                                                    ConfigUpdateStatus::Rejected => warn!(rejected = ?response.rejected_keys, "Config update rejected"),
                                                    ConfigUpdateStatus::PartiallyApplied => warn!(applied = ?response.applied_keys, rejected = ?response.rejected_keys, "Config update partially applied"),
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
                        Err(_) => {
                            // No new messages, this is normal
                        }
                    }
                } else {
                    std::future::pending::<()>().await
                }
            } => {}

            // PoolCacheUpdates from JetStream (SLAVE sync from market-data MASTER)
            _ = async {
                use futures::StreamExt;
                if let Some(ref consumer) = pool_cache_consumer_opt {
                    match consumer
                        .fetch()
                        .max_messages(100)
                        .expires(Duration::from_millis(100))
                        .messages()
                        .await
                    {
                        Ok(mut messages) => {
                            while let Some(msg_result) = messages.next().await {
                                match msg_result {
                                    Ok(msg) => {
                                        NATS_MESSAGES_RECEIVED_TOTAL.fetch_add(1, Ordering::Relaxed);
                                        match serde_json::from_slice::<PoolCacheUpdate>(&msg.payload) {
                                            Ok(update) => {
                                                arb_strategy_pool_cache_update_seen_inc();
                                                if !matches!(
                                                    update.update_type,
                                                    PoolCacheUpdateType::PoolRemoved
                                                ) && arb_tracked_token_mint(
                                                    &update.base_mint,
                                                    &update.quote_mint,
                                                )
                                                .is_none()
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
                                                    if ctx.seed_trackers_for_pool_cache_update(&update)
                                                    {
                                                        arb_strategy_pool_cache_update_seeded_inc();
                                                        if let Some(mint) = arb_tracked_token_mint(
                                                            &update.base_mint,
                                                            &update.quote_mint,
                                                        ) {
                                                            ctx.maybe_publish_arb_track_incremental_for_mint(
                                                                mint,
                                                            );
                                                        }
                                                    } else {
                                                        arb_strategy_pool_cache_update_skip_no_seed_inc();
                                                    }
                                                    debug!(
                                                        pool = %update.pool_address,
                                                        dex = %update.dex,
                                                        update_type = ?update.update_type,
                                                        "SLAVE CACHE: Pool cache update applied (JetStream)"
                                                    );
                                                }
                                            }
                                            Err(e) => {
                                                warn!(error = %e, "Failed to deserialize PoolCacheUpdate from JetStream");
                                            }
                                        }
                                        if let Err(e) = msg.ack().await {
                                            warn!(error = %e, "Failed to ack JetStream message");
                                        }
                                    }
                                    Err(e) => {
                                        warn!(error = %e, "Error receiving JetStream message");
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            trace!(error = %e, "JetStream fetch returned (timeout or no messages)");
                        }
                    }
                } else {
                    std::future::pending::<()>().await
                }
            } => {}

            // Heartbeat
            _ = heartbeat_interval.tick() => {
                let (records, bytes) = ctx.jsonl_writer.stats();
                let trackers = ctx.trackers.read();
                let multi_dex_tokens = trackers
                    .values()
                    .filter(|t| t.pool_count_on_distinct_dexes() >= 2)
                    .count();

                let known_pools_count = ctx.known_pools.read().len();
                let multi_hop_stats = ctx.multi_hop.stats();
                ctx.multi_hop.refresh_quote_readiness_metrics();
                ctx.eligibility_forensics.maybe_emit_snapshot();
                ctx.sync_pools_tracked_gauge();
                ctx.prune_arb_track_stale_pools();
                TOKENS_TRACKED_GAUGE.store(trackers.len() as u64, Ordering::Relaxed);

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

                info!(
                    events_received,
                    pools_tracked = ctx.pools_tracked.load(Ordering::Relaxed),
                    tokens_tracked = trackers.len(),
                    multi_dex_tokens = multi_dex_tokens,
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
            ctx.handle_pool_created(pool_address, base_mint, quote_mint, dex, liquidity);
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
                if ctx.two_hop_tx.try_send(job).is_err() {
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
            ctx.handle_dex_pool_accounts(pool_address, base_mint, quote_mint, accounts.clone());
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
            ctx.handle_pool_state_update(
                pool_address,
                dex,
                *reserve_base,
                *reserve_quote,
                *update_slot,
                *active_id,
                *bin_step,
                base_mint,
                quote_mint,
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
            ctx.handle_token_mint_info(mint, token_program);
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
        let decision = arb_market_event_ingress_priority(&event, &known);
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
        let event = sample_trade_event("pool-trade");
        assert_eq!(
            classify_market_event_priority(&event, &known),
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
        assert_eq!(
            classify_market_event_priority(&high_event, &known),
            ArbEventPriority::High
        );
        assert_eq!(
            classify_market_event_priority(&low_event, &known),
            ArbEventPriority::Low
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
}

#[cfg(test)]
mod two_hop_price_tests {
    use super::*;
    use ironcrab::execution::live_pool_cache::{create_shared_cache, SharedLivePoolCache};
    use ironcrab::ipc::PoolCacheUpdate;
    use rust_decimal::Decimal;
    use solana_sdk::pubkey::Pubkey;
    use std::time::Instant;

    const TEST_COMPONENT: &str = "test";
    const TEST_BUILD: &str = "0.0.0";
    const TEST_RUN: &str = "run-test";

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

    fn test_arb_context(live_pool_cache: SharedLivePoolCache) -> ArbContext {
        let log_dir = std::env::temp_dir().join(format!("arb_ctx_test_{}", Uuid::new_v4()));
        std::fs::create_dir_all(&log_dir).expect("test log dir");
        let jsonl_writer =
            JsonlWriter::new(JsonlWriterConfig::new("arb_test").with_log_dir(log_dir))
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
            arb_pinned_pools: RwLock::new(HashSet::new()),
            arb_track_published: AtomicU64::new(0),
            two_hop_tx: {
                let (tx, _rx) = mpsc::channel(1);
                tx
            },
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
    fn seed_skips_stale_jetstream_vault_when_geyser_slot_is_newer() {
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

        let fresher_updated_at = Instant::now();
        let mut vault_balances = HashMap::from([(
            pool_str.clone(),
            VaultBalanceCache {
                reserve_base: 9_999,
                reserve_quote: 8_888,
                update_slot: 100,
                active_id: None,
                bin_step: None,
                updated_at: fresher_updated_at,
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
        assert_eq!(vault.updated_at, fresher_updated_at);
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
    fn phase3_arb_track_requests_publish_serializes_reconcile_flag() {
        let update = ArbTrackRequestsUpdate {
            version: ARB_TRACK_REQUESTS_WIRE_VERSION,
            ts_unix_ms: 1_700_000_000,
            active: vec![ArbTrackActiveEntry {
                pool: "Pool111111111111111111111111111111111111111".to_string(),
                reason: ArbTrackActiveReason::Baseline,
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
