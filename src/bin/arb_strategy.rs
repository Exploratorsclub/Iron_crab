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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;

use ironcrab::arbitrage::{
    populate_arb_slave_from_live_pool_cache, sync_arb_slave_from_pool_cache_update,
    MultiHopArbitrage, MultiHopConfig,
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
    arb_two_hop_opportunity_inc, arb_two_hop_rejected_inc, arb_two_hop_tracker_seeded_pools_add,
    serve_metrics, set_readiness_nats_connected, ArbTwoHopRejectReason, MetricsComponent,
    ARB_REJECTED_MISSING_ACCOUNTS, ARB_TRIANGLE_OPPORTUNITIES, INTENTS_GENERATED_TOTAL,
    MARKET_EVENTS_CONSUMED_TOTAL, NATS_MESSAGES_PUBLISHED_TOTAL, NATS_MESSAGES_RECEIVED_TOTAL,
    POOLS_TRACKED_GAUGE, TOKENS_TRACKED_GAUGE,
};
use ironcrab::nats::{
    config_consumer_config, config_subject, pool_cache_live_fallback_consumer_config,
    CONFIG_STREAM_NAME, STREAM_NAME,
};
use ironcrab::nats::{NatsClient, NatsConfig};
use ironcrab::nats::{TOPIC_MARKET_EVENTS, TOPIC_TRADE_INTENTS};
use ironcrab::storage::{JsonlWriter, JsonlWriterConfig};

/// Build version for decision records
const BUILD_VERSION: &str = env!("CARGO_PKG_VERSION");

/// NATS topic for config reload commands from control-plane (Core NATS fallback)
const TOPIC_CONFIG_RELOAD: &str = "ironcrab.control.config.reload";

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
/// Geyser connection is considered broken if no MarketEvent received for this duration.
/// This is NOT about individual pool staleness - it's about connection health.
/// If Geyser is connected but a pool has no updates, the data IS current (pool is inactive).
const GEYSER_CONNECTION_TIMEOUT_SECS: u64 = 30;
const MIN_TRADE_VOLUME_LAMPORTS: u64 = 100_000; // 0.0001 SOL minimum (filter dust)
/// Max age for pool comparable prices used in 2-hop spread (aligns with Geyser health window).
const MAX_PRICE_AGE_MS: u64 = 30_000;
/// Small SOL probe for DLMM marginal price (0.01 SOL) — spread comparison only.
const DLMM_PROBE_SOL_LAMPORTS: u64 = 10_000_000;

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

/// DLMM token output via bin-array traversal (shared by spread price + intent sizing).
fn dlmm_token_output_from_bins(
    active_id: i32,
    bin_step: u16,
    sol_in_lamports: u64,
    bin_arrays: &HashMap<i64, Vec<BinData>>,
    sol_is_x: bool,
) -> Option<u64> {
    if bin_arrays.is_empty() {
        return None;
    }

    let active_array_index = active_id as i64 / 70;
    let active_bin_offset = (active_id as i64 % 70) as usize;
    let active_array = bin_arrays.get(&active_array_index)?;
    if !active_array
        .iter()
        .any(|b| b.offset as usize == active_bin_offset)
    {
        return None;
    }

    let mut remaining_sol = sol_in_lamports as u128;
    let mut total_tokens_out: u128 = 0;

    let mut all_bins: Vec<(i32, BinData)> = Vec::new();
    for (array_idx, bins) in bin_arrays {
        for bin in bins {
            let bin_id = (*array_idx * 70 + bin.offset as i64) as i32;
            all_bins.push((bin_id, bin.clone()));
        }
    }
    all_bins.sort_by_key(|(id, _)| *id);

    let relevant_bins: Vec<_> = all_bins
        .into_iter()
        .filter(|(id, _)| *id >= active_id)
        .collect();

    for (_bin_id, bin) in relevant_bins {
        if remaining_sol == 0 {
            break;
        }
        let (sol_in_bin, tokens_in_bin) = if sol_is_x {
            (bin.amount_x as u128, bin.amount_y as u128)
        } else {
            (bin.amount_y as u128, bin.amount_x as u128)
        };
        if tokens_in_bin == 0 {
            continue;
        }
        if sol_in_bin > 0 {
            let sol_to_use = remaining_sol.min(sol_in_bin);
            let tokens = sol_to_use
                .checked_mul(tokens_in_bin)?
                .checked_div(sol_in_bin)?;
            remaining_sol = remaining_sol.saturating_sub(sol_to_use);
            total_tokens_out = total_tokens_out.checked_add(tokens)?;
        }
    }

    let fee_bps = 10u128 + (bin_step as u128).min(100);
    let fee_multiplier = 10000u128 - fee_bps;
    let tokens_after_fee = total_tokens_out
        .checked_mul(fee_multiplier)?
        .checked_div(10000)?;

    Some(tokens_after_fee as u64)
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
    token_decimals: u8,
    vault_cache: Option<&VaultBalanceCache>,
    dlmm_bin_arrays: Option<&HashMap<i64, BinArrayCache>>,
) -> Option<Decimal> {
    if pool.dex == "meteora_dlmm" {
        if let (Some(vault), Some(arrays)) = (vault_cache, dlmm_bin_arrays) {
            if let (Some(active_id), Some(bin_step)) = (vault.active_id, vault.bin_step) {
                let flat = flatten_bin_array_cache(arrays);
                let sol_is_x = vault_cache.map(|v| v.dlmm_sol_is_x).unwrap_or(false);
                if let Some(tokens_out) = dlmm_token_output_from_bins(
                    active_id,
                    bin_step,
                    DLMM_PROBE_SOL_LAMPORTS,
                    &flat,
                    sol_is_x,
                ) {
                    if tokens_out > 0 {
                        return Some(trade_implied_sol_per_token(
                            DLMM_PROBE_SOL_LAMPORTS,
                            tokens_out,
                            token_decimals,
                        ));
                    }
                }
            }
        }
    }

    if let Some((reserve_base, reserve_quote)) = vault_reserves {
        if let Some(mid) = reserve_mid_sol_per_token(reserve_base, reserve_quote, token_decimals) {
            return Some(mid);
        }
    }
    match (pool.trade_price_buy, pool.trade_price_sell) {
        (Some(buy), Some(sell)) if buy > Decimal::ZERO && sell > Decimal::ZERO => {
            Some((buy + sell) / Decimal::from(2))
        }
        (Some(one), None) | (None, Some(one)) if one > Decimal::ZERO => Some(one),
        _ => None,
    }
}

/// SOL-quoted pool seed: (token_mint, reserve_base, reserve_quote_sol, active_id, bin_step).
type SolQuotedPoolSeed = (String, u64, u64, Option<i32>, Option<u16>);

/// Extract SOL-quoted token reserves from SLAVE CachedPoolState (base=token, quote=SOL).
fn sol_quoted_pool_seed(state: &CachedPoolState) -> Option<SolQuotedPoolSeed> {
    match state {
        CachedPoolState::Orca(s) => {
            let mint_a = s.token_mint_a.to_string();
            let mint_b = s.token_mint_b.to_string();
            let va = s.vault_a_balance?;
            let vb = s.vault_b_balance?;
            if mint_b == NATIVE_SOL_MINT {
                Some((mint_a, va, vb, None, None))
            } else if mint_a == NATIVE_SOL_MINT {
                Some((mint_b, vb, va, None, None))
            } else {
                None
            }
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

/// Seed TokenArbTracker pools for one mint from SLAVE LivePoolCache (Geyser-only, no RPC).
fn seed_token_tracker_from_live_pool_cache(
    mint: &str,
    live_pool_cache: &LivePoolCache,
    trackers: &mut HashMap<String, TokenArbTracker>,
    vault_balances: &mut HashMap<String, VaultBalanceCache>,
) -> usize {
    let mut seeded = 0usize;
    for (pool_pk, state) in live_pool_cache.iter() {
        let dex = state.dex_name();
        if !is_known_dex_label(dex) {
            continue;
        }
        let Some((token_mint, reserve_base, reserve_quote, active_id, bin_step)) =
            sol_quoted_pool_seed(&state)
        else {
            continue;
        };
        if token_mint != mint || reserve_base == 0 || reserve_quote == 0 {
            continue;
        }

        let pool_addr = pool_pk.to_string();
        let (_, slot, age_ms) =
            live_pool_cache
                .get_with_metadata(&pool_pk)
                .unwrap_or((state.clone(), 0, 0));
        let updated_at = Instant::now()
            .checked_sub(Duration::from_millis(age_ms))
            .unwrap_or_else(Instant::now);
        let dlmm_sol_is_x = matches!(
            state,
            CachedPoolState::Meteora(s) if s.token_x_mint.to_string() == NATIVE_SOL_MINT
        );

        vault_balances.insert(
            pool_addr.clone(),
            VaultBalanceCache {
                reserve_base,
                reserve_quote,
                update_slot: slot,
                active_id,
                bin_step,
                updated_at,
                dlmm_sol_is_x,
            },
        );

        let tracker = trackers
            .entry(mint.to_string())
            .or_insert_with(|| TokenArbTracker::new(mint));
        let token_decimals = tracker.token_decimals.unwrap_or(6);
        let liquidity_sol = Decimal::from(reserve_quote) / Decimal::from(1_000_000_000u64);
        let reserve_price = reserve_mid_sol_per_token(reserve_base, reserve_quote, token_decimals);
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
        let vault_entry = vault_balances.get(&pool_addr);
        let dlmm_bins = None::<&HashMap<i64, BinArrayCache>>;
        let seed_pool = PoolState {
            pool_address: pool_addr.clone(),
            dex: dex.to_string(),
            last_price: reserve_price,
            trade_price_buy,
            trade_price_sell,
            liquidity_sol,
            has_reserve_data: true,
            last_update: updated_at,
            trade_count,
            dex_accounts,
        };
        let last_price = comparable_price_sol_per_token(
            &seed_pool,
            Some((reserve_base, reserve_quote)),
            token_decimals,
            vault_entry,
            dlmm_bins,
        )
        .or(reserve_price);

        tracker.upsert_pool(PoolState {
            pool_address: pool_addr,
            dex: dex.to_string(),
            last_price,
            trade_price_buy: seed_pool.trade_price_buy,
            trade_price_sell: seed_pool.trade_price_sell,
            liquidity_sol,
            has_reserve_data: true,
            last_update: updated_at,
            trade_count: seed_pool.trade_count,
            dex_accounts: seed_pool.dex_accounts,
        });
        seeded += 1;
    }
    if seeded > 0 {
        arb_two_hop_tracker_seeded_pools_add(seeded as u64);
    }
    seeded
}

/// Seed all mints that have at least one SOL-quoted pool in SLAVE LivePoolCache.
fn seed_all_trackers_from_live_pool_cache(
    live_pool_cache: &LivePoolCache,
    trackers: &mut HashMap<String, TokenArbTracker>,
    vault_balances: &mut HashMap<String, VaultBalanceCache>,
) -> usize {
    let mut mints = HashSet::new();
    for (_, state) in live_pool_cache.iter() {
        if !is_known_dex_label(state.dex_name()) {
            continue;
        }
        if let Some((token_mint, rb, rq, _, _)) = sol_quoted_pool_seed(&state) {
            if rb > 0 && rq > 0 && token_mint != NATIVE_SOL_MINT {
                mints.insert(token_mint);
            }
        }
    }
    let mut total = 0usize;
    for mint in mints {
        total += seed_token_tracker_from_live_pool_cache(
            &mint,
            live_pool_cache,
            trackers,
            vault_balances,
        );
    }
    total
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
#[derive(Debug)]
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

    /// Check for arbitrage opportunity between DEXes
    /// Returns: Option<(buy_dex, sell_dex, spread_bps, estimated_profit_lamports)>
    fn check_arbitrage(
        &self,
        config: &ArbConfig,
        known_pools: &HashSet<String>,
        vault_balances: &HashMap<String, VaultBalanceCache>,
        bin_arrays: &HashMap<String, HashMap<i64, BinArrayCache>>,
    ) -> Option<ArbOpportunity> {
        // Check if 2-hop arbitrage is enabled
        if !config.two_hop_enabled {
            debug!(
                mint = %self.base_mint,
                "2-hop arb check skipped: two_hop_enabled=false"
            );
            return None;
        }

        let token_decimals = self.token_decimals.unwrap_or(6);

        // Build comparable prices per pool (reserve mid preferred over trade mid)
        let mut priced_pools: Vec<(&PoolState, Decimal)> = Vec::new();
        for pool in self.pools.values() {
            let is_known_dex = is_known_dex_label(&pool.dex);
            let in_master_cache = known_pools.contains(&pool.pool_address);
            if !is_known_dex || !in_master_cache {
                if is_known_dex && !in_master_cache {
                    debug!(
                        pool = %pool.pool_address,
                        dex = %pool.dex,
                        mint = %self.base_mint,
                        "Pool filtered: not in market-data MASTER cache (parse_pool_account failed)"
                    );
                }
                continue;
            }
            let vault_entry = vault_balances.get(&pool.pool_address);
            let vault_reserves = vault_entry.map(|c| (c.reserve_base, c.reserve_quote));
            let dlmm_bins = bin_arrays.get(&pool.pool_address);
            let Some(price) = comparable_price_sol_per_token(
                pool,
                vault_reserves,
                token_decimals,
                vault_entry,
                dlmm_bins,
            ) else {
                continue;
            };
            if price <= Decimal::ZERO {
                continue;
            }
            priced_pools.push((pool, price));
        }

        if priced_pools.len() < 2 {
            debug!(
                mint = %self.base_mint,
                pools = priced_pools.len(),
                "Arb check: insufficient pools with comparable prices"
            );
            arb_two_hop_rejected_inc(ArbTwoHopRejectReason::InsufficientPools);
            return None;
        }

        // Find cheapest pool to buy and most expensive pool to sell (may be same DEX — filtered below)
        let mut best_buy: Option<(&PoolState, Decimal)> = None;
        let mut best_sell: Option<(&PoolState, Decimal)> = None;

        for (pool, price) in &priced_pools {
            if best_buy.is_none() || *price < best_buy.unwrap().1 {
                best_buy = Some((pool, *price));
            }
            if best_sell.is_none() || *price > best_sell.unwrap().1 {
                best_sell = Some((pool, *price));
            }
        }

        let (buy_pool, buy_price) = best_buy?;
        let (sell_pool, sell_price) = best_sell?;

        // Staleness: trade-implied or Geyser reserve data must be fresh
        let max_age = Duration::from_millis(MAX_PRICE_AGE_MS);
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
            arb_two_hop_rejected_inc(ArbTwoHopRejectReason::StalePrice);
            return None;
        }

        // Don't arb same DEX
        if buy_pool.dex == sell_pool.dex {
            debug!(
                mint = %self.base_mint,
                dex = %buy_pool.dex,
                "Arb check rejected: same DEX for buy/sell"
            );
            arb_two_hop_rejected_inc(ArbTwoHopRejectReason::SameDex);
            return None;
        }

        // CRITICAL: Exclude pumpfun (bonding curve) from ALL arbitrage!
        if buy_pool.dex == "pumpfun" || sell_pool.dex == "pumpfun" {
            debug!(
                mint = %self.base_mint,
                buy_dex = %buy_pool.dex,
                sell_dex = %sell_pool.dex,
                "Arb check rejected: pumpfun (bonding curve) has no other pools to arb against"
            );
            arb_two_hop_rejected_inc(ArbTwoHopRejectReason::Pumpfun);
            return None;
        }

        // Calculate spread in bps
        // spread = (sell_price - buy_price) / buy_price * 10000
        if buy_price <= Decimal::ZERO {
            return None;
        }

        let spread = (sell_price - buy_price) / buy_price * Decimal::from(10000);
        // Convert to i64, handling large spreads correctly
        let spread_bps = spread.round().to_i64().unwrap_or(i64::MAX);

        // DATA QUALITY FILTERS

        // Filter 1: Exclude Native SOL arbitrage (these are wrap/unwrap, not real arb)
        if self.base_mint == NATIVE_SOL_MINT {
            debug!(
                mint = %self.base_mint,
                "Arb check rejected: Native SOL trades are wrap/unwrap, not arbitrage"
            );
            arb_two_hop_rejected_inc(ArbTwoHopRejectReason::NativeSol);
            return None;
        }

        // NOTE: Per-pool staleness check REMOVED.
        // Geyser streams directly from validator - if pool has no updates, that means:
        // - Pool is inactive (no trades/events), data IS current
        // - RPC would have same or older data
        // Geyser connection health is checked globally in ArbContext::is_geyser_connection_healthy()

        // Filter 2: Sanity check for unrealistic spreads
        let max_spread = if self.base_mint == USDC_MINT || self.base_mint == USDT_MINT {
            STABLECOIN_MAX_SPREAD_BPS
        } else {
            MAX_REASONABLE_SPREAD_BPS
        };

        if spread_bps > max_spread {
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
            arb_two_hop_rejected_inc(ArbTwoHopRejectReason::SpreadBelowMin);
            return None;
        }

        // Estimate profit
        // Use smaller liquidity pool as constraint (fallback to max_position if liquidity unknown)
        let max_trade_sol =
            if buy_pool.liquidity_sol > Decimal::ZERO && sell_pool.liquidity_sol > Decimal::ZERO {
                buy_pool.liquidity_sol.min(sell_pool.liquidity_sol).min(
                    Decimal::from(config.max_position_lamports) / Decimal::from(1_000_000_000u64),
                )
            } else {
                // Liquidity unknown (trade-based pools) - use max_position as conservative estimate
                Decimal::from(config.max_position_lamports) / Decimal::from(1_000_000_000u64)
            };

        // Gross profit = trade_amount * spread_pct
        let gross_profit = max_trade_sol * (spread / Decimal::from(10000));
        // Convert to lamports using proper Decimal methods
        let gross_profit_lamports = (gross_profit * Decimal::from(1_000_000_000u64))
            .round()
            .to_u64()
            .unwrap_or(0);

        // Net profit after tx costs
        let net_profit = gross_profit_lamports.saturating_sub(config.est_tx_cost_lamports);

        // 5× profit penalty only when BOTH sides lack Geyser reserve data and SOL liquidity
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
            arb_two_hop_rejected_inc(ArbTwoHopRejectReason::ProfitBelowMin);
            return None;
        }

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
    multi_hop: MultiHopArbitrage,
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

    /// Update or create pool state from PoolCreated event
    fn handle_pool_created(
        &self,
        pool_address: &str,
        base_mint: &str,
        quote_mint: &str,
        dex: &str,
        liquidity_sol: Decimal,
    ) {
        // Only track SOL pairs for now
        const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
        if quote_mint != SOL_MINT {
            return;
        }

        let mut trackers = self.trackers.write();
        let tracker = trackers
            .entry(base_mint.to_string())
            .or_insert_with(|| TokenArbTracker::new(base_mint));

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

        let is_new = !tracker.pools.contains_key(pool_address);
        tracker.upsert_pool(pool_state);

        if is_new {
            self.pools_tracked.fetch_add(1, Ordering::Relaxed);
            debug!(
                mint = %base_mint,
                dex = %dex,
                pool = %pool_address,
                liquidity = %liquidity_sol,
                pools = tracker.pools.len(),
                "Pool added to arb tracker"
            );
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

    /// Seed TokenArbTracker for one mint from SLAVE LivePoolCache (no RPC).
    fn seed_trackers_for_mint(&self, mint: &str) -> usize {
        let mut trackers = self.trackers.write();
        let mut vault_balances = self.vault_balances.write();
        seed_token_tracker_from_live_pool_cache(
            mint,
            &self.live_pool_cache,
            &mut trackers,
            &mut vault_balances,
        )
    }

    /// Seed all SOL-quoted mints after JetStream bootstrap.
    fn seed_all_trackers_from_live_pool_cache(&self) -> usize {
        let mut trackers = self.trackers.write();
        let mut vault_balances = self.vault_balances.write();
        seed_all_trackers_from_live_pool_cache(
            &self.live_pool_cache,
            &mut trackers,
            &mut vault_balances,
        )
    }

    /// Incremental tracker seed when a pool is discovered or balances update.
    fn seed_trackers_for_pool_cache_update(&self, update: &PoolCacheUpdate) {
        if matches!(update.update_type, PoolCacheUpdateType::PoolRemoved) {
            return;
        }
        let mint = if update.quote_mint == NATIVE_SOL_MINT {
            &update.base_mint
        } else if update.base_mint == NATIVE_SOL_MINT {
            &update.quote_mint
        } else {
            return;
        };
        if mint == NATIVE_SOL_MINT {
            return;
        }
        let seeded = self.seed_trackers_for_mint(mint);
        if seeded > 0 {
            debug!(
                mint = %mint,
                pools_seeded = seeded,
                pool = %update.pool_address,
                "Tracker seeded from SLAVE LivePoolCache"
            );
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
    ) {
        let mut cache = self.vault_balances.write();
        let is_new = !cache.contains_key(pool_address);
        let dlmm_sol_is_x = if dex == "meteora_dlmm" {
            base_mint == NATIVE_SOL_MINT
        } else {
            cache
                .get(pool_address)
                .map(|v| v.dlmm_sol_is_x)
                .unwrap_or(false)
        };
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

        // Mirror SOL liquidity + reserve flag into per-mint pool trackers (Geyser-only, no RPC)
        let liquidity_sol = Decimal::from(reserve_quote) / Decimal::from(1_000_000_000u64);
        let mut trackers = self.trackers.write();
        for tracker in trackers.values_mut() {
            if let Some(pool) = tracker.pools.get_mut(pool_address) {
                pool.liquidity_sol = liquidity_sol;
                pool.has_reserve_data = reserve_base > 0 && reserve_quote > 0;
                pool.last_update = Instant::now();
                let token_decimals = tracker.token_decimals.unwrap_or(6);
                if let Some(mid) =
                    reserve_mid_sol_per_token(reserve_base, reserve_quote, token_decimals)
                {
                    pool.last_price = Some(mid);
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
            pool_state.dlmm_sol_is_x,
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

        info!(
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
        let mut trackers = self.trackers.write();

        // Get or create tracker for this mint
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
        let vault_reserves = self
            .vault_balances
            .read()
            .get(pool_address)
            .map(|c| (c.reserve_base, c.reserve_quote));
        let vault_entry = self.vault_balances.read().get(pool_address).cloned();
        let dlmm_bins = self.bin_arrays.read().get(pool_address).cloned();
        pool.last_price = comparable_price_sol_per_token(
            pool,
            vault_reserves,
            token_decimals,
            vault_entry.as_ref(),
            dlmm_bins.as_ref(),
        );
        pool.trade_count += 1;
        pool.last_update = Instant::now();
        info!(
            pool = %pool_address,
            mint = %mint,
            dex = %pool.dex,
            comparable_price = ?pool.last_price,
            "Pool comparable price updated"
        );

        // Global Geyser connection health check (replaces per-pool staleness)
        // If no MarketEvents received for 30s, connection is broken - don't trade
        if !self.is_geyser_connection_healthy() {
            warn!(
                mint = %mint,
                timeout_secs = GEYSER_CONNECTION_TIMEOUT_SECS,
                "Arb rejected: Geyser connection unhealthy (no events received)"
            );
            return None;
        }

        // Check for arbitrage opportunity (with known_pools filter)
        let known_pools = self.known_pools.read();
        let vault_balances = self.vault_balances.read();
        let bin_arrays = self.bin_arrays.read();
        if let Some(opp) =
            tracker.check_arbitrage(&config, &known_pools, &vault_balances, &bin_arrays)
        {
            // Check cooldown
            let cooldown = Duration::from_millis(config.intent_cooldown_ms);
            if let Some(last_time) = tracker.last_intent_time {
                if last_time.elapsed() < cooldown {
                    return None;
                }
            }

            tracker.last_intent_time = Some(Instant::now());
            self.opportunities_found.fetch_add(1, Ordering::Relaxed);
            return Some(opp);
        }

        None
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
                .add_directive("ironcrab=info".parse()?),
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
    let multi_hop = MultiHopArbitrage::new(MultiHopConfig::default(), live_pool_cache.clone());

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
    });

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
                let tracker_seeded = ctx.seed_all_trackers_from_live_pool_cache();
                ctx.multi_hop.warmup_quotes_from_live_pool_cache();
                let mh_stats = ctx.multi_hop.stats();
                info!(
                    pools_recovered,
                    known_pools = known_count,
                    tracker_seeded_pools = tracker_seeded,
                    multi_hop_pools = mh_stats.graph_pools,
                    multi_hop_vertices = mh_stats.graph_vertices,
                    "SLAVE CACHE: known_pools and multi-hop graph recovered from JetStream"
                );
                POOLS_TRACKED_GAUGE.store(known_count as u64, Ordering::Relaxed);
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

    // Main event loop
    info!("Entering main event loop");

    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    let mut market_sub = market_subscription;
    let mut cfg_sub = config_subscription;
    let config_js_consumer_opt = config_js_consumer;
    let pool_cache_consumer_opt = pool_cache_consumer;
    let mut heartbeat_interval = tokio::time::interval(Duration::from_secs(60));

    loop {
        tokio::select! {
            // MarketEvents
            msg = async {
                if let Some(ref mut sub) = market_sub {
                    sub.next().await
                } else {
                    std::future::pending::<Option<ironcrab::nats::NatsMessage>>().await
                }
            } => {
                if let Some(nats_msg) = msg {
                    // Prometheus: count inbound NATS messages for this process
                    NATS_MESSAGES_RECEIVED_TOTAL.fetch_add(1, Ordering::Relaxed);
                    ctx.events_received.fetch_add(1, Ordering::Relaxed);

                    match serde_json::from_slice::<MarketEvent>(&nats_msg.payload) {
                        Ok(event) => {
                            // Prometheus: count consumed MarketEvents for this process
                            MARKET_EVENTS_CONSUMED_TOTAL.fetch_add(1, Ordering::Relaxed);
                            if let Some(intent) = handle_market_event(&ctx, &event).await {
                                // Write to JSONL
                                if let Err(e) = ctx.jsonl_writer.write(&intent) {
                                    error!(error = %e, "Failed to write intent to JSONL");
                                }

                                // Publish to NATS
                                if let Some(ref nats) = ctx.nats {
                                    if let Err(e) = nats.publish(TOPIC_TRADE_INTENTS, &intent).await {
                                        warn!(error = %e, "Failed to publish intent to NATS");
                                    } else {
                                        // Prometheus: count outbound NATS messages and intents
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
                        }
                        Err(e) => {
                            warn!(error = %e, "Failed to deserialize MarketEvent");
                        }
                    }
                }
            }

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
                                                if sync_arb_slave_from_pool_cache_update(
                                                    &ctx.live_pool_cache,
                                                    &ctx.known_pools,
                                                    &ctx.multi_hop,
                                                    &update,
                                                ) {
                                                    ctx.seed_trackers_for_pool_cache_update(&update);
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

                // Prometheus: publish current gauges for this process
                POOLS_TRACKED_GAUGE.store(ctx.pools_tracked.load(Ordering::Relaxed), Ordering::Relaxed);
                TOKENS_TRACKED_GAUGE.store(trackers.len() as u64, Ordering::Relaxed);

                info!(
                    events_received = ctx.events_received.load(Ordering::Relaxed),
                    pools_tracked = ctx.pools_tracked.load(Ordering::Relaxed),
                    tokens_tracked = trackers.len(),
                    multi_dex_tokens = multi_dex_tokens,
                    known_pools = known_pools_count,
                    opportunities_found = ctx.opportunities_found.load(Ordering::Relaxed),
                    intents_generated = ctx.intents_generated.load(Ordering::Relaxed),
                    intents_written = records,
                    bytes_written = bytes,
                    // Data quality metrics
                    zero_amount_trades = ctx.zero_amount_trades.load(Ordering::Relaxed),
                    data_quality_rejects = ctx.data_quality_rejects.load(Ordering::Relaxed),
                    // Multi-hop stats
                    multi_hop_vertices = multi_hop_stats.graph_vertices,
                    multi_hop_pools = multi_hop_stats.graph_pools,
                    multi_hop_cycles_found = multi_hop_stats.cycles_found,
                    multi_hop_profitable = multi_hop_stats.cycles_profitable,
                    multi_hop_enabled = ctx.multi_hop.is_enabled(),
                    "arb-strategy heartbeat (SLAVE cache sync from market-data MASTER)"
                );
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

    // Debug: Log event type to verify Trade events are arriving
    match &event.kind {
        MarketEventKind::Trade {
            sol_amount,
            token_amount,
            ..
        } => {
            info!(sol_amount, token_amount, "Received Trade event");
        }
        MarketEventKind::PoolCreated { .. } => {
            info!("Received PoolCreated event");
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
                // Determine input/output mints based on trade direction
                let (input_mint, output_mint) = if *is_buy {
                    // Buy token: SOL -> Token
                    (NATIVE_SOL_MINT, mint.as_str())
                } else {
                    // Sell token: Token -> SOL
                    (mint.as_str(), NATIVE_SOL_MINT)
                };

                let multi_hop_intents = ctx.multi_hop.on_pool_price_update(
                    pool_address,
                    input_mint,
                    output_mint,
                    *sol_amount,
                    *token_amount,
                    "arb-strategy",
                    BUILD_VERSION,
                    &ctx.run_id,
                );

                // Publish any multi-hop intents found
                for mut intent in multi_hop_intents {
                    // K Phase 1: Slot-to-Send Latency - propagate slot from event
                    if let Some(slot) = event.slot {
                        intent.metadata.insert("slot".to_string(), slot.to_string());
                    }
                    intent.metadata.insert(
                        "slot_seen_at_ms".to_string(),
                        event.header.ts_unix_ms.to_string(),
                    );
                    if let Err(e) = ctx.jsonl_writer.write(&intent) {
                        error!(error = %e, "Failed to write multi-hop intent to JSONL");
                    }
                    if let Some(ref nats) = ctx.nats {
                        if let Err(e) = nats.publish(TOPIC_TRADE_INTENTS, &intent).await {
                            warn!(error = %e, "Failed to publish multi-hop intent");
                        } else {
                            NATS_MESSAGES_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                            INTENTS_GENERATED_TOTAL.fetch_add(1, Ordering::Relaxed);
                            ctx.intents_generated.fetch_add(1, Ordering::Relaxed);
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

            // Existing 2-hop arbitrage detection
            if let Some(opp) = ctx.handle_trade(
                pool_address,
                mint,
                quote_mint,
                *sol_amount,
                *token_amount,
                *token_decimals,
                *is_buy,
                dex,
            ) {
                // Prometheus: count arbitrage opportunities detected
                ARB_TRIANGLE_OPPORTUNITIES.fetch_add(1, Ordering::Relaxed);
                info!(
                    mint = %opp.base_mint,
                    buy_dex = %opp.buy_dex,
                    sell_dex = %opp.sell_dex,
                    spread_bps = opp.spread_bps,
                    profit_lamports = opp.estimated_profit_lamports,
                    "🔥 Arbitrage opportunity detected!"
                );
                // create_arb_intent returns None if pump_amm is used but DexPoolAccounts are missing
                create_arb_intent(ctx, &opp).map(|mut intent| {
                    // K Phase 1: Slot-to-Send Latency - propagate slot from event
                    if let Some(slot) = event.slot {
                        intent.metadata.insert("slot".to_string(), slot.to_string());
                    }
                    intent.metadata.insert(
                        "slot_seen_at_ms".to_string(),
                        event.header.ts_unix_ms.to_string(),
                    );
                    intent
                })
            } else {
                None
            }
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
mod two_hop_price_tests {
    use super::*;
    use ironcrab::execution::live_pool_cache::create_shared_cache;
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
    ) -> VaultBalanceCache {
        VaultBalanceCache {
            reserve_base,
            reserve_quote,
            update_slot: 1,
            active_id,
            bin_step,
            updated_at: Instant::now(),
            dlmm_sol_is_x: false,
        }
    }

    #[test]
    fn same_reserve_mid_on_two_dexes_yields_near_zero_spread() {
        let reserves = (1_000_000_000_000u64, 1_000_000_000u64);
        let mid = reserve_mid_sol_per_token(reserves.0, reserves.1, 6).unwrap();
        let pool_a = sample_pool("meteora_dlmm", "poolA", None, None);
        let pool_b = sample_pool("pump_amm", "poolB", None, None);
        let vault = sample_vault(reserves.0, reserves.1, None, None);
        let p_a =
            comparable_price_sol_per_token(&pool_a, Some(reserves), 6, Some(&vault), None).unwrap();
        let p_b =
            comparable_price_sol_per_token(&pool_b, Some(reserves), 6, Some(&vault), None).unwrap();
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
        let mid = comparable_price_sol_per_token(&pool, None, 6, None, None).unwrap();
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
        // 1M tokens (6 dec) : 1 SOL on both sides
        let reserve_base = 1_000_000_000_000u64;
        let reserve_quote = 1_000_000_000u64;
        let active_id: i32 = 0;
        let bin_step: u16 = 10;
        let array_index = active_id as i64 / 70;

        let mut bin_arrays: HashMap<i64, BinArrayCache> = HashMap::new();
        bin_arrays.insert(
            array_index,
            BinArrayCache {
                bins: vec![BinData {
                    offset: 0,
                    amount_x: reserve_base,
                    amount_y: reserve_quote,
                }],
                update_slot: 1,
            },
        );

        let dlmm_pool = sample_pool("meteora_dlmm", "dlmmPool", None, None);
        let orca_pool = sample_pool("orca", "orcaPool", None, None);
        let vault = sample_vault(reserve_base, reserve_quote, Some(active_id), Some(bin_step));

        let p_dlmm = comparable_price_sol_per_token(
            &dlmm_pool,
            Some((reserve_base, reserve_quote)),
            6,
            Some(&vault),
            Some(&bin_arrays),
        )
        .unwrap();
        let p_orca = comparable_price_sol_per_token(
            &orca_pool,
            Some((reserve_base, reserve_quote)),
            6,
            Some(&vault),
            None,
        )
        .unwrap();

        let spread_bps = ((p_orca - p_dlmm) / p_dlmm * Decimal::from(10000))
            .abs()
            .round()
            .to_i64()
            .unwrap();
        assert!(
            spread_bps < MAX_REASONABLE_SPREAD_BPS,
            "DLMM marginal vs AMM mid spread {spread_bps} bps should be sane"
        );
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
        );
        assert_eq!(seeded, 2);

        let mut known_pools = HashSet::new();
        known_pools.insert(pool_a.to_string());
        known_pools.insert(pool_b.to_string());

        let tracker = trackers.get(&mint_str).unwrap();
        assert_eq!(tracker.pools.len(), 2);
        assert_eq!(tracker.pool_count_on_distinct_dexes(), 2);

        let config = ArbConfig::default();
        let bin_arrays: HashMap<String, HashMap<i64, BinArrayCache>> = HashMap::new();
        let opp = tracker.check_arbitrage(&config, &known_pools, &vault_balances, &bin_arrays);
        // Same reserves → spread ~0, rejected by spread_below_min not insufficient_pools
        assert!(
            opp.is_none(),
            "expected spread_below_min or similar, not insufficient_pools"
        );
        assert_eq!(tracker.pools.len(), 2);
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
