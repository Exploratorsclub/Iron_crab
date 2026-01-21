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
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use ironcrab::config::Config as AppConfig;
use ironcrab::ipc::{
    BinData, ConfigUpdate, ConfigUpdateResponse, ConfigUpdateStatus, ExplicitAmount, IntentOrigin,
    IntentTier, MarketEvent, MarketEventKind, PoolCacheUpdate, TradeIntent, TradeResources,
    TradeSide, TradingRegime,
};
use ironcrab::metrics::{
    serve_metrics, ARB_REJECTED_MISSING_ACCOUNTS, ARB_TRIANGLE_OPPORTUNITIES,
    INTENTS_GENERATED_TOTAL, MARKET_EVENTS_CONSUMED_TOTAL, NATS_MESSAGES_PUBLISHED_TOTAL,
    NATS_MESSAGES_RECEIVED_TOTAL, POOLS_TRACKED_GAUGE, TOKENS_TRACKED_GAUGE,
};
use ironcrab::nats::{pool_subject, slave_consumer_config, STREAM_NAME};
use ironcrab::nats::{NatsClient, NatsConfig};
use ironcrab::nats::{TOPIC_MARKET_EVENTS, TOPIC_POOL_CACHE_UPDATES, TOPIC_TRADE_INTENTS};
use ironcrab::storage::{JsonlWriter, JsonlWriterConfig};

/// Build version for decision records
const BUILD_VERSION: &str = env!("CARGO_PKG_VERSION");

/// NATS topic for config reload commands from control-plane
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

/// Tracks a pool's price/liquidity state
#[allow(dead_code)]
#[derive(Debug, Clone)]
struct PoolState {
    pool_address: String,
    dex: String,
    /// Last known price (quote per base, e.g., SOL per token)
    last_price: Option<Decimal>,
    /// Liquidity in SOL
    liquidity_sol: Decimal,
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
    /// Pool states by DEX name
    pools_by_dex: HashMap<String, PoolState>,
    /// Pool accounts by pool_address (from DexPoolAccounts events)
    /// Key: pool_address, Value: accounts vec
    pool_accounts: HashMap<String, Vec<String>>,
    /// Token program for base_mint (SPL Token or Token-2022), from TokenMintInfo event
    token_program: Option<String>,
    /// Last intent generated time
    last_intent_time: Option<Instant>,
}

impl TokenArbTracker {
    fn new(base_mint: &str) -> Self {
        Self {
            base_mint: base_mint.to_string(),
            pools_by_dex: HashMap::new(),
            pool_accounts: HashMap::new(),
            token_program: None,
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

    /// Add or update a pool for this token
    fn upsert_pool(&mut self, pool: PoolState) {
        self.pools_by_dex.insert(pool.dex.clone(), pool);
    }

    /// Check for arbitrage opportunity between DEXes
    /// Returns: Option<(buy_dex, sell_dex, spread_bps, estimated_profit_lamports)>
    fn check_arbitrage(
        &self,
        config: &ArbConfig,
        known_pools: &HashSet<String>,
    ) -> Option<ArbOpportunity> {
        // Need at least 2 DEXes with prices
        let pools_with_price: Vec<_> = self
            .pools_by_dex
            .values()
            .filter(|p| {
                let has_price = p.last_price.is_some();
                let is_known_dex = is_known_dex_label(&p.dex);
                let in_master_cache = known_pools.contains(&p.pool_address);

                // Log when pool is filtered out due to not being in MASTER cache
                if has_price && is_known_dex && !in_master_cache {
                    debug!(
                        pool = %p.pool_address,
                        dex = %p.dex,
                        mint = %self.base_mint,
                        "Pool filtered: not in market-data MASTER cache (parse_pool_account failed)"
                    );
                }

                has_price && is_known_dex && in_master_cache
            })
            .collect();

        if pools_with_price.len() < 2 {
            debug!(
                mint = %self.base_mint,
                pools = pools_with_price.len(),
                "Arb check: insufficient pools with prices"
            );
            return None;
        }

        // Find best buy (lowest price) and best sell (highest price)
        let mut best_buy: Option<&PoolState> = None;
        let mut best_sell: Option<&PoolState> = None;

        for pool in &pools_with_price {
            let price = pool.last_price.unwrap();

            if best_buy.is_none() || price < best_buy.unwrap().last_price.unwrap() {
                best_buy = Some(pool);
            }
            if best_sell.is_none() || price > best_sell.unwrap().last_price.unwrap() {
                best_sell = Some(pool);
            }
        }

        let buy_pool = best_buy?;
        let sell_pool = best_sell?;

        // Don't arb same DEX
        if buy_pool.dex == sell_pool.dex {
            debug!(
                mint = %self.base_mint,
                dex = %buy_pool.dex,
                "Arb check rejected: same DEX for buy/sell"
            );
            return None;
        }

        // CRITICAL: Exclude pumpfun (bonding curve) from ALL arbitrage!
        // While a token is on the bonding curve, there are NO other pools for it.
        // Other DEXes (Meteora, Orca, Raydium) only list tokens AFTER migration.
        // Therefore pumpfun arbitrage is NEVER valid - there's nothing to arb against.
        if buy_pool.dex == "pumpfun" || sell_pool.dex == "pumpfun" {
            debug!(
                mint = %self.base_mint,
                buy_dex = %buy_pool.dex,
                sell_dex = %sell_pool.dex,
                "Arb check rejected: pumpfun (bonding curve) has no other pools to arb against"
            );
            return None;
        }

        // TEMPORARY FIX: Disable Orca Whirlpool until full Geyser pool state tracking
        // Error 6023 (InvalidTickArraySequence) occurs when:
        // - Tick arrays don't exist on-chain (new/inactive pools)
        // - Pool tick changes between quote and swap (tick crossing)
        // - Cached pool state is stale
        // Code in orca.rs is correct, but we need real-time tick array validation via Geyser
        if buy_pool.dex == "orca" || sell_pool.dex == "orca" {
            debug!(
                mint = %self.base_mint,
                buy_dex = %buy_pool.dex,
                sell_dex = %sell_pool.dex,
                "Arb check rejected: Orca disabled (waiting for Geyser pool state tracking)"
            );
            return None;
        }

        let buy_price = buy_pool.last_price.unwrap();
        let sell_price = sell_pool.last_price.unwrap();

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

        // LIQUIDITY VALIDATION: For trade-discovered pools (liquidity=0), require higher profit threshold
        // This compensates for unknown actual liquidity and reduces risk
        let effective_min_profit = if buy_pool.liquidity_sol <= Decimal::ZERO
            || sell_pool.liquidity_sol <= Decimal::ZERO
        {
            // Require 5x normal profit for unknown liquidity pools
            config.min_profit_lamports * 5
        } else {
            config.min_profit_lamports
        };

        if buy_pool.liquidity_sol <= Decimal::ZERO || sell_pool.liquidity_sol <= Decimal::ZERO {
            debug!(
                mint = %self.base_mint,
                buy_liquidity = %buy_pool.liquidity_sol,
                sell_liquidity = %sell_pool.liquidity_sol,
                net_profit = net_profit,
                required_profit = effective_min_profit,
                "Using higher profit threshold for unknown liquidity pools"
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
                buy_liquidity_known = buy_pool.liquidity_sol > Decimal::ZERO,
                sell_liquidity_known = sell_pool.liquidity_sol > Decimal::ZERO,
                "Arb check rejected: profit below minimum"
            );
            return None;
        }

        Some(ArbOpportunity {
            base_mint: self.base_mint.clone(),
            buy_dex: buy_pool.dex.clone(),
            buy_pool: buy_pool.pool_address.clone(),
            buy_price,
            sell_dex: sell_pool.dex.clone(),
            sell_pool: sell_pool.pool_address.clone(),
            sell_price,
            spread_bps: spread_bps as u32,
            trade_amount_lamports: (max_trade_sol * Decimal::from(1_000_000_000u64))
                .to_string()
                .parse::<u64>()
                .unwrap_or(config.max_position_lamports),
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
    /// Set of pool addresses that exist in market-data MASTER LivePoolCache.
    /// Updated from PoolCacheUpdate::PoolDiscovered/PoolRemoved events.
    /// ONLY generate intents for pools in this set - ensures execution-engine can execute them.
    known_pools: RwLock<HashSet<String>>,
}

/// Cached vault balances from PoolStateUpdate events
#[allow(dead_code)]
#[derive(Debug, Clone)]
struct VaultBalanceCache {
    reserve_base: u64,
    reserve_quote: u64,
    update_slot: u64,
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
                _ => rejected.push((key.clone(), format!("Unknown config key: {}", key))),
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
            liquidity_sol,
            last_update: Instant::now(),
            trade_count: 0,
            dex_accounts: None, // Will be filled by DexPoolAccounts event
        };

        let is_new = !tracker.pools_by_dex.contains_key(dex);
        tracker.upsert_pool(pool_state);

        if is_new {
            self.pools_tracked.fetch_add(1, Ordering::Relaxed);
            debug!(
                mint = %base_mint,
                dex = %dex,
                pool = %pool_address,
                liquidity = %liquidity_sol,
                dexes = tracker.pools_by_dex.len(),
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

    /// Handle PoolStateUpdate event - cache vault balances from Geyser
    /// This eliminates RPC calls to fetch vault balances during quoting.
    fn handle_pool_state_update(
        &self,
        pool_address: &str,
        _dex: &str,
        reserve_base: u64,
        reserve_quote: u64,
        update_slot: u64,
    ) {
        let mut cache = self.vault_balances.write();
        let is_new = !cache.contains_key(pool_address);
        cache.insert(
            pool_address.to_string(),
            VaultBalanceCache {
                reserve_base,
                reserve_quote,
                update_slot,
            },
        );
        if is_new {
            debug!(
                pool = %pool_address,
                reserve_base,
                reserve_quote,
                slot = update_slot,
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
    #[allow(clippy::too_many_arguments)]
    fn handle_trade(
        &self,
        pool_address: &str,
        mint: &str,
        sol_amount: u64,
        token_amount: u64,
        token_decimals: u8,
        _is_buy: bool,
        dex: &str,
    ) -> Option<ArbOpportunity> {
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

        // Calculate price: SOL per token
        let sol_dec = Decimal::from(sol_amount) / Decimal::from(1_000_000_000u64);
        let token_divisor = 10u64.pow(token_decimals as u32);
        let token_dec = Decimal::from(token_amount) / Decimal::from(token_divisor);
        let price = sol_dec / token_dec;

        info!(
            pool = %pool_address,
            mint = %mint,
            sol_amount = sol_amount,
            token_amount = token_amount,
            token_decimals = token_decimals,
            price = %price,
            "Price calculated from trade"
        );

        let config = self.config.read().clone();
        let mut trackers = self.trackers.write();

        // Get or create tracker for this mint
        let tracker = trackers.entry(mint.to_string()).or_insert_with(|| {
            info!(mint = %mint, "Creating tracker from Trade event (no PoolCreated)");
            TokenArbTracker {
                base_mint: mint.to_string(),
                pools_by_dex: HashMap::new(),
                pool_accounts: HashMap::new(),
                token_program: None,
                last_intent_time: None,
            }
        });

        // Find or create pool for this pool_address
        // Prefer updating a pool we already know (from PoolCreated) by matching pool_address.
        let existing_dex_key = tracker
            .pools_by_dex
            .iter()
            .find(|(_, p)| p.pool_address == pool_address)
            .map(|(k, _)| k.clone());

        let pool = if let Some(dex_key) = existing_dex_key {
            tracker
                .pools_by_dex
                .get_mut(&dex_key)
                .expect("dex_key must exist")
        } else {
            // Use the DEX from the Trade event. If empty/unknown, use pool_address as key.
            let effective_dex = if !dex.is_empty() && dex != "unknown" {
                dex.to_string()
            } else {
                pool_address.to_string()
            };
            tracker
                .pools_by_dex
                .entry(effective_dex.clone())
                .or_insert_with(|| {
                    info!(pool = %pool_address, mint = %mint, dex = %effective_dex, "Creating pool from Trade event");
                    PoolState {
                        pool_address: pool_address.to_string(),
                        dex: effective_dex,
                        liquidity_sol: Decimal::ZERO, // Unknown liquidity
                        last_price: None,
                        trade_count: 0,
                        last_update: Instant::now(),
                        dex_accounts: None, // Will be filled by DexPoolAccounts event
                    }
                })
        };

        // Update price
        pool.last_price = Some(price);
        pool.trade_count += 1;
        pool.last_update = Instant::now();
        info!(pool = %pool_address, mint = %mint, dex = %pool.dex, price = %price, "Pool price updated");

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
        if let Some(opp) = tracker.check_arbitrage(&config, &known_pools) {
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

/// Creates an arb intent from the opportunity.
/// Returns None if required DexPoolAccounts are missing for ANY pool.
///
/// GEYSER-FIRST PRINCIPLE (TARGET_ARCHITECTURE.md §4.5):
/// - NO RPC calls in hot path
/// - DexPoolAccounts must be available for BOTH buy and sell pools
/// - If Geyser hasn't delivered the data, RPC won't have it either (same validator)
/// - Missing data = REJECT intent, don't try RPC fallback
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
        IntentTier::Tier1,
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

/// Bootstrap known_pools from JetStream (state recovery after restart)
///
/// This function pulls the last PoolCacheUpdate for each pool from JetStream,
/// giving arb-strategy immediate awareness of all parseable pools. After bootstrap,
/// the SLAVE subscribes to incremental updates via regular NATS subscription.
///
/// # Arguments
///
/// * `nats_client` - Connected NATS client
/// * `known_pools` - HashSet to populate with pool addresses
///
/// # Returns
///
/// Number of pools recovered from JetStream
async fn bootstrap_known_pools_from_jetstream(
    nats_client: &NatsClient,
    known_pools: &RwLock<HashSet<String>>,
) -> Result<usize> {
    use async_nats::jetstream;
    use futures::StreamExt;

    info!("SLAVE CACHE BOOTSTRAP: Pulling known pools from JetStream...");

    let jetstream = jetstream::new(nats_client.client().clone());

    // Get or create stream (idempotent)
    let stream = match jetstream.get_stream(STREAM_NAME).await {
        Ok(s) => s,
        Err(e) => {
            warn!(error = %e, stream = STREAM_NAME, "JetStream stream not found (market-data may not be running)");
            return Ok(0);
        }
    };

    // Create ephemeral consumer with LastPerSubject deliver policy
    let consumer_config = slave_consumer_config();
    let consumer = stream.create_consumer(consumer_config).await?;

    let mut pools_recovered = 0;
    let batch_size = 1000; // Fetch up to 1000 messages per batch

    // Fetch all available messages in batches until exhausted
    loop {
        let mut messages = consumer.fetch().max_messages(batch_size).messages().await?;
        let mut batch_count = 0;

        while let Some(msg) = messages.next().await {
            let msg = match msg {
                Ok(m) => m,
                Err(e) => {
                    warn!(error = %e, "Error fetching message from JetStream");
                    continue;
                }
            };

            batch_count += 1;

            // Deserialize PoolCacheUpdate
            let pool_update: PoolCacheUpdate = match serde_json::from_slice(&msg.payload) {
                Ok(u) => u,
                Err(e) => {
                    warn!(error = %e, "Failed to deserialize PoolCacheUpdate from JetStream");
                    if let Err(ack_err) = msg.ack().await {
                        warn!(error = %ack_err, "Failed to ack message");
                    }
                    continue;
                }
            };

            // Add pool to known_pools
            match pool_update.update_type {
                ironcrab::ipc::PoolCacheUpdateType::PoolDiscovered
                | ironcrab::ipc::PoolCacheUpdateType::BalanceUpdated => {
                    let mut pools = known_pools.write();
                    pools.insert(pool_update.pool_address.clone());
                    pools_recovered += 1;
                    debug!(
                        pool = %pool_update.pool_address,
                        dex = %pool_update.dex,
                        "SLAVE CACHE BOOTSTRAP: Recovered pool from JetStream"
                    );
                }
                ironcrab::ipc::PoolCacheUpdateType::PoolRemoved => {
                    // Skip removed pools during bootstrap
                }
            }

            if let Err(ack_err) = msg.ack().await {
                warn!(error = %ack_err, "Failed to ack message");
            }
        }

        // If we got fewer messages than batch_size, we've exhausted the stream
        if batch_count < batch_size {
            break;
        }
    }

    info!(
        pools_recovered,
        "SLAVE CACHE BOOTSTRAP: Complete (known_pools populated)"
    );
    Ok(pools_recovered)
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
        if let Err(e) = serve_metrics(metrics_addr).await {
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
        let config = NatsConfig::new(&args.nats_url, "arb-strategy");
        let mut client = NatsClient::new(config);
        if let Err(e) = client.connect().await {
            error!(error = %e, "Failed to connect to NATS");
            return Err(e);
        }
        info!(url = %args.nats_url, "Connected to NATS");
        Some(client)
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
        known_pools: RwLock::new(HashSet::new()),
    });

    // Bootstrap known_pools from JetStream (state recovery after restart)
    if let Some(ref nats_client) = ctx.nats {
        match bootstrap_known_pools_from_jetstream(nats_client, &ctx.known_pools).await {
            Ok(pools_recovered) => {
                info!(
                    pools_recovered,
                    "SLAVE CACHE: known_pools recovered from JetStream"
                );
                POOLS_TRACKED_GAUGE.store(pools_recovered as u64, Ordering::Relaxed);
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

    // Subscribe to Config Updates (runtime hot reload via control-plane)
    let config_subscription = if let Some(ref nats) = ctx.nats {
        match nats.subscribe(TOPIC_CONFIG_RELOAD).await {
            Ok(sub) => {
                info!(topic = TOPIC_CONFIG_RELOAD, "Subscribed to Config Updates");
                Some(sub)
            }
            Err(e) => {
                warn!(error = %e, topic = TOPIC_CONFIG_RELOAD, "Failed to subscribe to Config Updates");
                None
            }
        }
    } else {
        None
    };

    // Subscribe to PoolCacheUpdates (SLAVE sync from market-data MASTER)
    let pool_cache_subscription = if let Some(ref nats) = ctx.nats {
        match nats.subscribe(TOPIC_POOL_CACHE_UPDATES).await {
            Ok(sub) => {
                info!(
                    topic = TOPIC_POOL_CACHE_UPDATES,
                    "Subscribed to PoolCacheUpdates (SLAVE cache sync from market-data MASTER)"
                );
                Some(sub)
            }
            Err(e) => {
                error!(error = %e, "Failed to subscribe to PoolCacheUpdates");
                return Err(e);
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
    let mut pool_cache_sub = pool_cache_subscription;
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

            // Config updates
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
                                info!(component = %update.target_component, keys = ?update.config.keys(), "Applying config update");
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

            // PoolCacheUpdates (SLAVE sync from market-data MASTER)
            msg = async {
                if let Some(ref mut sub) = pool_cache_sub {
                    sub.next().await
                } else {
                    std::future::pending::<Option<ironcrab::nats::NatsMessage>>().await
                }
            } => {
                if let Some(nats_msg) = msg {
                    NATS_MESSAGES_RECEIVED_TOTAL.fetch_add(1, Ordering::Relaxed);
                    match serde_json::from_slice::<PoolCacheUpdate>(&nats_msg.payload) {
                        Ok(update) => {
                            use ironcrab::ipc::PoolCacheUpdateType;
                            match update.update_type {
                                PoolCacheUpdateType::PoolDiscovered => {
                                    ctx.known_pools.write().insert(update.pool_address.clone());
                                    debug!(pool = %update.pool_address, dex = %update.dex, "SLAVE CACHE: Pool added to known_pools");
                                }
                                PoolCacheUpdateType::PoolRemoved => {
                                    ctx.known_pools.write().remove(&update.pool_address);
                                    debug!(pool = %update.pool_address, "SLAVE CACHE: Pool removed from known_pools");
                                }
                                PoolCacheUpdateType::BalanceUpdated => {
                                    // Balance updates don't affect pool existence, just state
                                    debug!(pool = %update.pool_address, "SLAVE CACHE: Balance update received");
                                }
                            }
                        }
                        Err(e) => {
                            warn!(error = %e, "Failed to deserialize PoolCacheUpdate");
                        }
                    }
                }
            }

            // Heartbeat
            _ = heartbeat_interval.tick() => {
                let (records, bytes) = ctx.jsonl_writer.stats();
                let trackers = ctx.trackers.read();
                let multi_dex_tokens = trackers.values()
                    .filter(|t| t.pools_by_dex.len() >= 2)
                    .count();

                let known_pools_count = ctx.known_pools.read().len();

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
            sol_amount,
            token_amount,
            token_decimals,
            is_buy,
            dex,
            ..
        } => {
            if let Some(opp) = ctx.handle_trade(
                pool_address,
                mint,
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
                create_arb_intent(ctx, &opp)
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
            ..
        } => {
            ctx.handle_pool_state_update(
                pool_address,
                dex,
                *reserve_base,
                *reserve_quote,
                *update_slot,
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
