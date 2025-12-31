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

use anyhow::Result;
use clap::Parser;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use ironcrab::ipc::{
    ConfigUpdate, ConfigUpdateResponse, ConfigUpdateStatus, ExplicitAmount, IntentOrigin,
    IntentTier, MarketEvent, MarketEventKind, TradeIntent, TradeResources, TradeSide,
    TradingRegime,
};
use ironcrab::metrics::serve_metrics;
use ironcrab::nats::{NatsClient, NatsConfig, TOPIC_MARKET_EVENTS, TOPIC_TRADE_INTENTS};
use ironcrab::storage::{JsonlWriter, JsonlWriterConfig};

/// NATS topic for config reload (P1: Runtime Configuration via UI)
const TOPIC_CONFIG_RELOAD: &str = "ironcrab.control.config.reload";

// P1 Crash Isolation: Systemd Watchdog support
#[cfg(unix)]
use sd_notify::NotifyState;

/// Build version for decision records
const BUILD_VERSION: &str = env!("CARGO_PKG_VERSION");

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
    
    // === Filter 2: Buyer Velocity ===
    /// Min unique buyers in early window. Default: 10
    min_unique_buyers: u32,
    /// Early window for buyer count (seconds). Default: 20s
    buyer_window_secs: u64,
    /// Min trades per second for momentum. Default: 0.5
    min_trades_per_sec: f64,
    /// Min buy dominance ratio (buys / total). Default: 0.6 (60%)
    min_buy_dominance: f64,
    
    // === Filter 3: SOL Inflow ===
    /// Min net SOL inflow in window (lamports). Default: 20 SOL
    min_sol_inflow_lamports: u64,
    /// Inflow window (seconds). Default: 30s
    inflow_window_secs: u64,
    /// Max single dump size (lamports). Default: 5 SOL
    max_single_dump_lamports: u64,
    
    // === Filter 4: Dev Behavior ===
    /// Dev early sell triggers exit (seconds after pool creation). Default: 60s
    dev_early_sell_window_secs: u64,
    /// Dev rebuy is positive signal. Default: true
    dev_rebuy_positive: bool,
}

impl Default for MomentumConfig {
    fn default() -> Self {
        Self {
            early_min_liquidity_sol: 5.0,              // 5 SOL min for early trades
            established_min_liquidity_sol: 20.0,       // 20 SOL min for established
            early_slot_threshold: 1000,                // ~400s until established
            early_max_slippage_bps: 300,               // 3% for volatile early stage
            established_max_slippage_bps: 100,         // 1% for stable pools
            default_position_lamports: 100_000_000,    // 0.1 SOL per trade
            test_allowlist: HashSet::new(),            // empty = all mints allowed
            
            // Filter 1: Liquidity Check
            max_dev_supply_pct: 90.0,                  // Max 90% dev supply
            lp_removal_window_secs: 60,                // Track LP removals for 60s
            
            // Filter 2: Buyer Velocity
            min_unique_buyers: 10,                     // 10 unique buyers min
            buyer_window_secs: 20,                     // in first 20 seconds
            min_trades_per_sec: 0.5,                   // 0.5 trades/sec momentum
            min_buy_dominance: 0.6,                    // 60% buys vs sells
            
            // Filter 3: SOL Inflow
            min_sol_inflow_lamports: 20_000_000_000,   // 20 SOL net inflow
            inflow_window_secs: 30,                    // in 30 seconds
            max_single_dump_lamports: 5_000_000_000,   // Max 5 SOL single sell
            
            // Filter 4: Dev Behavior
            dev_early_sell_window_secs: 60,            // Dev sells in first 60s = bad
            dev_rebuy_positive: true,                  // Dev rebuy = positive signal
        }
    }
}

// ============================================================================
// Token Tracking Structures for Strategy Filters
// ============================================================================

/// Tracks a single trade event
#[derive(Debug, Clone)]
struct TradeEvent {
    timestamp: Instant,
    trader: String,
    is_buy: bool,
    sol_amount: u64,  // in lamports
    signature: String,
}

/// Tracks token metrics for strategy decisions
#[derive(Debug)]
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
    
    // Trade tracking
    trades: Vec<TradeEvent>,
    unique_buyers: HashSet<String>,
    unique_sellers: HashSet<String>,
    
    // Aggregates
    total_buy_volume: u64,      // lamports
    total_sell_volume: u64,     // lamports
    buy_count: u32,
    sell_count: u32,
    
    // Dev behavior
    dev_sold: bool,
    dev_sold_early: bool,       // Sold within dev_early_sell_window
    dev_rebought: bool,
    
    // LP tracking
    lp_removed: bool,
    lp_removal_time: Option<Instant>,
    
    // State
    intent_generated: bool,     // Already generated an intent for this token
    blacklisted: bool,          // Failed filters, don't trade
    blacklist_reason: Option<String>,
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
            trades: Vec::new(),
            unique_buyers: HashSet::new(),
            unique_sellers: HashSet::new(),
            total_buy_volume: 0,
            total_sell_volume: 0,
            buy_count: 0,
            sell_count: 0,
            dev_sold: false,
            dev_sold_early: false,
            dev_rebought: false,
            lp_removed: false,
            lp_removal_time: None,
            intent_generated: false,
            blacklisted: false,
            blacklist_reason: None,
        }
    }
    
    /// Record a trade event
    fn record_trade(&mut self, trader: &str, is_buy: bool, sol_amount: u64, signature: &str, config: &MomentumConfig) {
        let trade = TradeEvent {
            timestamp: Instant::now(),
            trader: trader.to_string(),
            is_buy,
            sol_amount,
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
                self.blacklisted = true;
                self.blacklist_reason = Some(format!(
                    "Large dump detected: {} SOL", 
                    sol_amount as f64 / 1_000_000_000.0
                ));
            }
        }
        
        // Dev behavior tracking
        if let Some(ref dev) = self.dev_wallet {
            if trader == dev {
                if is_buy && self.dev_sold {
                    self.dev_rebought = true;
                    info!(mint = %self.mint, "Dev rebought - positive signal");
                } else if !is_buy {
                    self.dev_sold = true;
                    let age = self.first_seen.elapsed();
                    if age.as_secs() < config.dev_early_sell_window_secs {
                        self.dev_sold_early = true;
                        self.blacklisted = true;
                        self.blacklist_reason = Some(format!(
                            "Dev sold early ({}s after creation)",
                            age.as_secs()
                        ));
                        warn!(mint = %self.mint, age_secs = age.as_secs(), "Dev sold early - blacklisting");
                    }
                }
            }
        }
        
        self.trades.push(trade);
    }
    
    /// Record LP removal
    fn record_lp_removal(&mut self) {
        self.lp_removed = true;
        self.lp_removal_time = Some(Instant::now());
        self.blacklisted = true;
        self.blacklist_reason = Some("LP removed".to_string());
        warn!(mint = %self.mint, "LP removed - blacklisting");
    }
    
    /// Set dev wallet and supply percentage
    fn set_dev_info(&mut self, dev_wallet: &str, supply_pct: f64, config: &MomentumConfig) {
        self.dev_wallet = Some(dev_wallet.to_string());
        self.dev_supply_pct = Some(supply_pct);
        
        if supply_pct > config.max_dev_supply_pct {
            self.blacklisted = true;
            self.blacklist_reason = Some(format!(
                "Dev supply too high: {:.1}% (max {:.1}%)",
                supply_pct, config.max_dev_supply_pct
            ));
            warn!(mint = %self.mint, supply_pct, "Dev supply too high - blacklisting");
        }
    }
    
    /// Calculate metrics for strategy decision
    fn calculate_metrics(&self, config: &MomentumConfig) -> TokenMetrics {
        let age = self.first_seen.elapsed();
        let age_secs = age.as_secs().max(1) as f64;
        
        // Filter recent trades within windows
        let now = Instant::now();
        let buyer_window = Duration::from_secs(config.buyer_window_secs);
        let inflow_window = Duration::from_secs(config.inflow_window_secs);
        
        let recent_buyers: HashSet<_> = self.trades.iter()
            .filter(|t| t.is_buy && now.duration_since(t.timestamp) < buyer_window)
            .map(|t| t.trader.clone())
            .collect();
            
        let (recent_buy_vol, recent_sell_vol) = self.trades.iter()
            .filter(|t| now.duration_since(t.timestamp) < inflow_window)
            .fold((0u64, 0u64), |(b, s), t| {
                if t.is_buy { (b + t.sol_amount, s) } else { (b, s + t.sol_amount) }
            });
        
        let total_trades = self.buy_count + self.sell_count;
        let trades_per_sec = total_trades as f64 / age_secs;
        let buy_dominance = if total_trades > 0 {
            self.buy_count as f64 / total_trades as f64
        } else {
            0.0
        };
        
        TokenMetrics {
            age_secs: age.as_secs(),
            unique_buyers_in_window: recent_buyers.len() as u32,
            total_unique_buyers: self.unique_buyers.len() as u32,
            trades_per_sec,
            buy_dominance,
            net_sol_inflow: recent_buy_vol.saturating_sub(recent_sell_vol),
            total_buy_volume: self.total_buy_volume,
            total_sell_volume: self.total_sell_volume,
            dev_sold_early: self.dev_sold_early,
            dev_rebought: self.dev_rebought,
            lp_removed: self.lp_removed,
            initial_liquidity_sol: self.initial_liquidity as f64 / 1_000_000_000.0,
        }
    }
    
    /// Check all 4 filters and return if we should trade
    fn should_generate_intent(&self, config: &MomentumConfig) -> (bool, String) {
        // Already generated or blacklisted
        if self.intent_generated {
            return (false, "Already generated intent".to_string());
        }
        if self.blacklisted {
            return (false, self.blacklist_reason.clone().unwrap_or("Blacklisted".to_string()));
        }
        
        let metrics = self.calculate_metrics(config);
        
        // Filter 1: Liquidity Check
        if metrics.initial_liquidity_sol < config.early_min_liquidity_sol {
            return (false, format!(
                "Liquidity too low: {:.2} SOL (min {:.2})",
                metrics.initial_liquidity_sol, config.early_min_liquidity_sol
            ));
        }
        
        // Filter 1b: LP removal
        if metrics.lp_removed {
            return (false, "LP removed".to_string());
        }
        
        // Filter 2: Buyer Velocity
        if metrics.unique_buyers_in_window < config.min_unique_buyers {
            return (false, format!(
                "Not enough buyers: {} (min {})",
                metrics.unique_buyers_in_window, config.min_unique_buyers
            ));
        }
        
        if metrics.trades_per_sec < config.min_trades_per_sec {
            return (false, format!(
                "Trade velocity too low: {:.2}/s (min {:.2})",
                metrics.trades_per_sec, config.min_trades_per_sec
            ));
        }
        
        if metrics.buy_dominance < config.min_buy_dominance {
            return (false, format!(
                "Buy dominance too low: {:.1}% (min {:.1}%)",
                metrics.buy_dominance * 100.0, config.min_buy_dominance * 100.0
            ));
        }
        
        // Filter 3: SOL Inflow
        if metrics.net_sol_inflow < config.min_sol_inflow_lamports {
            return (false, format!(
                "SOL inflow too low: {:.2} SOL (min {:.2})",
                metrics.net_sol_inflow as f64 / 1_000_000_000.0,
                config.min_sol_inflow_lamports as f64 / 1_000_000_000.0
            ));
        }
        
        // Filter 4: Dev Behavior
        if metrics.dev_sold_early {
            return (false, "Dev sold early".to_string());
        }
        
        // All filters passed!
        let reason = format!(
            "All filters passed: liq={:.1}SOL, buyers={}, vel={:.2}/s, dom={:.0}%, inflow={:.1}SOL",
            metrics.initial_liquidity_sol,
            metrics.unique_buyers_in_window,
            metrics.trades_per_sec,
            metrics.buy_dominance * 100.0,
            metrics.net_sol_inflow as f64 / 1_000_000_000.0
        );
        
        (true, reason)
    }
}

/// Calculated metrics for logging/decisions
#[derive(Debug)]
struct TokenMetrics {
    age_secs: u64,
    unique_buyers_in_window: u32,
    total_unique_buyers: u32,
    trades_per_sec: f64,
    buy_dominance: f64,
    net_sol_inflow: u64,
    total_buy_volume: u64,
    total_sell_volume: u64,
    dev_sold_early: bool,
    dev_rebought: bool,
    lp_removed: bool,
    initial_liquidity_sol: f64,
}

/// Runtime context for momentum-bot
struct MomentumContext {
    run_id: String,
    /// P1: Config in RwLock for runtime hot-reload
    config: parking_lot::RwLock<MomentumConfig>,
    nats: Option<NatsClient>,
    jsonl_writer: JsonlWriter,
    intent_counter: std::sync::atomic::AtomicU64,
    /// Track known pools (pool_address -> first_seen_slot)
    pool_first_seen: parking_lot::RwLock<std::collections::HashMap<String, u64>>,
    /// Token trackers for strategy filters (mint -> tracker)
    token_trackers: parking_lot::RwLock<HashMap<String, TokenTracker>>,
    /// Stats
    tokens_tracked: std::sync::atomic::AtomicU64,
    tokens_blacklisted: std::sync::atomic::AtomicU64,
    intents_generated: std::sync::atomic::AtomicU64,
}

impl MomentumContext {
    fn next_intent_id(&self) -> String {
        let n = self
            .intent_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        format!("int-{}-{:06}", &self.run_id[..8], n)
    }

    /// Classify trading regime for a pool
    fn classify_regime(&self, pool_address: &str, current_slot: u64) -> TradingRegime {
        let first_seen = self.pool_first_seen.read().get(pool_address).copied();
        let config = self.config.read();

        match first_seen {
            Some(first_slot) => {
                let age_slots = current_slot.saturating_sub(first_slot);
                if age_slots < config.early_slot_threshold {
                    TradingRegime::Early
                } else {
                    TradingRegime::Established
                }
            }
            None => TradingRegime::Early, // New pool = EARLY
        }
    }

    /// Record first-seen slot for a pool
    fn record_pool_seen(&self, pool_address: &str, slot: u64) {
        let mut pools = self.pool_first_seen.write();
        pools.entry(pool_address.to_string()).or_insert(slot);
    }
    
    /// Get or create a token tracker
    fn get_or_create_tracker(&self, mint: &str, pool: &str, dex: &str, slot: u64, liquidity: u64) -> bool {
        let mut trackers = self.token_trackers.write();
        if trackers.contains_key(mint) {
            false // Already exists
        } else {
            trackers.insert(mint.to_string(), TokenTracker::new(mint, pool, dex, slot, liquidity));
            self.tokens_tracked.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            true // New tracker created
        }
    }
    
    /// Record a trade for a token
    fn record_trade(&self, mint: &str, trader: &str, is_buy: bool, sol_amount: u64, signature: &str) {
        let config = self.config.read().clone();
        let mut trackers = self.token_trackers.write();
        if let Some(tracker) = trackers.get_mut(mint) {
            let was_blacklisted = tracker.blacklisted;
            tracker.record_trade(trader, is_buy, sol_amount, signature, &config);
            if !was_blacklisted && tracker.blacklisted {
                self.tokens_blacklisted.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }
    
    /// Record dev info for a token
    fn record_dev_info(&self, mint: &str, dev_wallet: &str, supply_pct: f64) {
        let config = self.config.read().clone();
        let mut trackers = self.token_trackers.write();
        if let Some(tracker) = trackers.get_mut(mint) {
            let was_blacklisted = tracker.blacklisted;
            tracker.set_dev_info(dev_wallet, supply_pct, &config);
            if !was_blacklisted && tracker.blacklisted {
                self.tokens_blacklisted.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }
    
    /// Record LP removal for a token
    fn record_lp_removal(&self, mint: &str) {
        let mut trackers = self.token_trackers.write();
        if let Some(tracker) = trackers.get_mut(mint) {
            let was_blacklisted = tracker.blacklisted;
            tracker.record_lp_removal();
            if !was_blacklisted && tracker.blacklisted {
                self.tokens_blacklisted.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }
    
    /// Check if any tracked token should generate an intent
    fn check_for_signals(&self) -> Vec<(String, String, String, String)> {
        // Returns: Vec<(mint, pool, dex, reason)>
        let config = self.config.read().clone();
        let mut trackers = self.token_trackers.write();
        let mut signals = Vec::new();
        
        for (mint, tracker) in trackers.iter_mut() {
            if tracker.intent_generated || tracker.blacklisted {
                continue;
            }
            
            let (should_trade, reason) = tracker.should_generate_intent(&config);
            if should_trade {
                tracker.intent_generated = true;
                signals.push((
                    mint.clone(),
                    tracker.pool.clone(),
                    tracker.dex.clone(),
                    reason,
                ));
            }
        }
        
        signals
    }
    
    /// Clean up old trackers (older than 5 minutes)
    fn cleanup_old_trackers(&self) {
        let mut trackers = self.token_trackers.write();
        let cutoff = Duration::from_secs(300); // 5 minutes
        trackers.retain(|_, tracker| {
            tracker.first_seen.elapsed() < cutoff || !tracker.intent_generated
        });
    }
    
    /// P1: Apply config update from control-plane (Runtime Configuration via UI)
    fn apply_config_update(&self, update: &ConfigUpdate) -> ConfigUpdateResponse {
        let mut config = self.config.write();
        let mut applied = Vec::new();
        let mut rejected = Vec::new();
        
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

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("momentum_bot=info".parse()?)
                .add_directive("ironcrab=info".parse()?),
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

    // Start metrics server
    let metrics_addr = std::net::SocketAddr::from(([0, 0, 0, 0], args.metrics_port));
    tokio::spawn(async move {
        if let Err(e) = serve_metrics(metrics_addr).await {
            error!(error = %e, "Metrics server failed");
        }
    });
    info!(port = args.metrics_port, "Metrics server started at /metrics");

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

    // Setup config
    let mut momentum_config = MomentumConfig::default();
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
    let jsonl_writer = JsonlWriter::new(jsonl_config)?;

    info!(log_dir = %log_dir.display(), "JSONL writer initialized");

    // Setup NATS
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
            Some(client)
        }
    };

    let ctx = Arc::new(MomentumContext {
        run_id: run_id.clone(),
        config: parking_lot::RwLock::new(momentum_config),
        nats,
        jsonl_writer,
        intent_counter: std::sync::atomic::AtomicU64::new(0),
        pool_first_seen: parking_lot::RwLock::new(std::collections::HashMap::new()),
        token_trackers: parking_lot::RwLock::new(HashMap::new()),
        tokens_tracked: std::sync::atomic::AtomicU64::new(0),
        tokens_blacklisted: std::sync::atomic::AtomicU64::new(0),
        intents_generated: std::sync::atomic::AtomicU64::new(0),
    });

    // === Main Loop: Process MarketEvents from NATS ===
    info!("Entering main event loop");
    
    // P1 Crash Isolation: Signal systemd that we're ready
    #[cfg(unix)]
    {
        let _ = sd_notify::notify(true, &[NotifyState::Ready]);
        debug!("Sent sd_notify READY to systemd");
    }

    // Subscribe to MarketEvents from NATS
    let mut subscription = if let Some(ref nats) = ctx.nats {
        match nats.subscribe(TOPIC_MARKET_EVENTS).await {
            Ok(sub) => {
                info!(topic = TOPIC_MARKET_EVENTS, "Subscribed to MarketEvents");
                Some(sub)
            }
            Err(e) => {
                warn!(error = %e, "Failed to subscribe to NATS, running in offline mode");
                None
            }
        }
    } else {
        info!("NATS not connected, running in offline mode");
        None
    };

    // P1: Subscribe to Config Updates (Runtime Configuration via UI)
    let mut config_subscription = if let Some(ref nats) = ctx.nats {
        match nats.subscribe(TOPIC_CONFIG_RELOAD).await {
            Ok(sub) => {
                info!(topic = TOPIC_CONFIG_RELOAD, "Subscribed to Config Updates");
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

    // Heartbeat and stats tracking
    let mut heartbeat_interval = tokio::time::interval(std::time::Duration::from_secs(60));
    let mut events_received: u64 = 0;
    let mut last_slot: u64 = 0;

    // Graceful shutdown handling
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            // Process incoming MarketEvents from NATS
            msg = async {
                if let Some(ref mut sub) = subscription {
                    sub.next().await
                } else {
                    // No subscription - just wait forever (heartbeat will still fire)
                    std::future::pending::<Option<ironcrab::nats::NatsMessage>>().await
                }
            } => {
                if let Some(nats_msg) = msg {
                    events_received += 1;

                    // Deserialize MarketEvent
                    match serde_json::from_slice::<MarketEvent>(&nats_msg.payload) {
                        Ok(event) => {
                            if let Some(slot) = event.slot {
                                last_slot = slot;
                            }

                            // Process the event
                            if let Err(e) = process_market_event(&ctx, &event).await {
                                warn!(error = %e, event_id = %event.event_id, "Failed to process market event");
                            }
                        }
                        Err(e) => {
                            warn!(error = %e, "Failed to deserialize MarketEvent");
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
                            // Only process if targeted at momentum-bot
                            if update.component == "momentum-bot" {
                                info!(
                                    component = %update.component,
                                    keys = ?update.config.keys().collect::<Vec<_>>(),
                                    "Received Config Update from control-plane"
                                );
                                let response = ctx.apply_config_update(&update);
                                info!(
                                    status = ?response.status,
                                    applied = ?response.applied_keys,
                                    rejected = ?response.rejected_keys,
                                    "Config update processed"
                                );
                            } else {
                                debug!(component = %update.component, "Ignoring config update for other component");
                            }
                        }
                        Err(e) => {
                            warn!(error = %e, "Failed to deserialize ConfigUpdate");
                        }
                    }
                }
            }

            // Periodic heartbeat
            _ = heartbeat_interval.tick() => {
                let (records, bytes) = ctx.jsonl_writer.stats();
                let pools = ctx.pool_first_seen.read().len();
                let tokens_tracked = ctx.tokens_tracked.load(std::sync::atomic::Ordering::Relaxed);
                let tokens_blacklisted = ctx.tokens_blacklisted.load(std::sync::atomic::Ordering::Relaxed);
                let intents_generated = ctx.intents_generated.load(std::sync::atomic::Ordering::Relaxed);
                
                info!(
                    events_received = events_received,
                    last_slot = last_slot,
                    intents_written = records,
                    bytes_written = bytes,
                    pools_tracked = pools,
                    tokens_tracked = tokens_tracked,
                    tokens_blacklisted = tokens_blacklisted,
                    intents_generated = intents_generated,
                    "Momentum-bot heartbeat"
                );
                
                // Cleanup old trackers
                ctx.cleanup_old_trackers();
                
                // Check for trading signals
                let signals = ctx.check_for_signals();
                for (mint, pool, dex, reason) in signals {
                    info!(
                        mint = %mint,
                        pool = %pool,
                        dex = %dex,
                        reason = %reason,
                        "🎯 TRADING SIGNAL DETECTED"
                    );
                    
                    // Generate and publish intent
                    if let Err(e) = generate_and_publish_intent(&ctx, &mint, &pool, &dex, &reason).await {
                        error!(error = %e, mint = %mint, "Failed to generate/publish intent");
                    } else {
                        ctx.intents_generated.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
                
                // P1 Crash Isolation: Ping systemd watchdog
                #[cfg(unix)]
                let _ = sd_notify::notify(true, &[NotifyState::Watchdog]);
            }

            _ = &mut shutdown => {
                info!("Shutdown signal received");
                break;
            }
        }
    }

    // Flush JSONL on shutdown
    ctx.jsonl_writer.flush()?;
    info!(run_id = %run_id, "momentum-bot shutdown complete");

    Ok(())
}

/// Generate and publish a TradeIntent based on a trading signal
async fn generate_and_publish_intent(
    ctx: &MomentumContext,
    mint: &str,
    pool: &str,
    dex: &str,
    reason: &str,
) -> Result<()> {
    let config = ctx.config.read();
    let position_lamports = config.default_position_lamports;
    let max_slippage = config.early_max_slippage_bps;
    drop(config);
    
    // Assume SOL (So11111...) as quote mint for PumpFun/meme tokens
    let sol_mint = "So11111111111111111111111111111111111111112";
    
    let intent = TradeIntent::new(
        "momentum-bot",
        BUILD_VERSION,
        &ctx.run_id,
        ctx.next_intent_id(),
        &format!("4filter:{}", reason),
        IntentTier::Tier1,
        IntentOrigin::StrategyA,
        ExplicitAmount::new(position_lamports, 9),
        TradeResources {
            input_mint: sol_mint.to_string(),
            output_mint: mint.to_string(),
            pools: vec![pool.to_string()],
            accounts: vec![],
        },
        50, // Expected ROI: 0.5%
        max_slippage,
        TradeSide::Buy,
        TradingRegime::Early,
    )
    .with_ttl_ms(5000);

    info!(
        intent_id = %intent.intent_id,
        pool = %pool,
        mint = %mint,
        dex = %dex,
        reason = %reason,
        "🚀 Generated 4-Filter TradeIntent"
    );

    // Write to JSONL (P0 requirement)
    ctx.jsonl_writer.write(&intent)?;

    // Publish to NATS
    if let Some(ref nats) = ctx.nats {
        nats.publish(TOPIC_TRADE_INTENTS, &intent).await?;
    }

    Ok(())
}

/// Process a MarketEvent and update token trackers
async fn process_market_event(ctx: &MomentumContext, event: &MarketEvent) -> Result<()> {
    match &event.kind {
        MarketEventKind::PoolCreated {
            pool_address,
            base_mint,
            quote_mint,
            dex,
            initial_liquidity_sol,
        } => {
            let slot = event.slot.unwrap_or(0);
            ctx.record_pool_seen(pool_address, slot);

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
            let min_liq = config.min_liquidity_sol;
            drop(config);
            
            // Create tracker if liquidity meets minimum threshold
            if liq_sol >= min_liq {
                let slot = event.slot.unwrap_or(0);
                let liq_lamports = (liq_sol * 1_000_000_000.0) as u64;
                let created = ctx.get_or_create_tracker(base_mint, pool_address, dex, slot, liq_lamports);
                
                if created {
                    info!(
                        mint = %base_mint,
                        pool = %pool_address,
                        dex = %dex,
                        liquidity = liq_sol,
                        "📊 Token tracker initialized"
                    );
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
        
        MarketEventKind::Trade {
            pool_address,
            mint,
            trader,
            is_buy,
            sol_amount,
            token_amount: _,
            signature,
        } => {
            // Record the trade in the tracker
            let sol_lamports = *sol_amount as u64;
            let sig = signature.clone().unwrap_or_default();
            
            // Check if this trader is the dev wallet
            let tracker_opt = {
                let trackers = ctx.token_trackers.read();
                trackers.get(mint).cloned()
            };
            
            if let Some(tracker) = tracker_opt {
                let is_dev = {
                    let t = tracker.read();
                    t.dev_wallet.as_ref() == Some(trader)
                };
                
                ctx.record_trade(mint, trader, *is_buy, sol_lamports, &sig);
                
                if is_dev {
                    // Record dev trade behavior
                    let mut t = tracker.write();
                    if *is_buy {
                        t.dev_rebought = true;
                        info!(mint = %mint, trader = %trader, "📈 Dev rebuy detected - positive signal");
                    } else {
                        t.dev_sold = true;
                        info!(mint = %mint, trader = %trader, sol = sol_lamports, "⚠️ Dev sell detected");
                    }
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
        }
        
        MarketEventKind::LiquidityRemoved {
            pool_address,
            mint,
            sol_amount,
            ..
        } => {
            // LP removal - potential rug signal
            let sol_lamports = *sol_amount as u64;
            ctx.record_lp_removal(mint);
            
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
                // Blacklist token with high dev supply
                let tracker_opt = {
                    let trackers = ctx.token_trackers.read();
                    trackers.get(mint).cloned()
                };
                
                if let Some(tracker) = tracker_opt {
                    let mut t = tracker.write();
                    if !t.blacklisted {
                        t.blacklisted = true;
                        t.blacklist_reason = Some(format!(
                            "Dev supply too high: {:.1}% > {:.1}%",
                            supply_percentage, max_dev_pct
                        ));
                        ctx.tokens_blacklisted.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        
                        warn!(
                            mint = %mint,
                            dev_supply = supply_percentage,
                            max_allowed = max_dev_pct,
                            "🚫 Token blacklisted - dev supply too high"
                        );
                    }
                }
            } else {
                info!(
                    mint = %mint,
                    dev_wallet = %dev_wallet,
                    supply_pct = supply_percentage,
                    "✅ Dev wallet identified - supply within limits"
                );
            }
        }
        
        MarketEventKind::SlotUpdate { current_slot } => {
            debug!(current_slot, "Slot update");
        }
        
        _ => {
            trace!(event_id = %event.event_id, "Unhandled event type");
        }
    }

    Ok(())
}
