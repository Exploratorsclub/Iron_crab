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
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;

use ironcrab::config::MomentumCfg;
use ironcrab::ipc::{
    ConfigUpdate, ConfigUpdateResponse, ConfigUpdateStatus, ExecutionResult, ExecutionStatus,
    ExplicitAmount, IntentOrigin, IntentTier, MarketEvent, MarketEventKind, TradeExecutionConstraints,
    TradeIntent, TradeResources, TradeSide, TradingRegime,
};
use ironcrab::metrics::{
    serve_metrics, FILTER_PASSED_TOTAL, FILTER_REJECTED_DEV_BEHAVIOR, FILTER_REJECTED_INFLOW,
    FILTER_REJECTED_LIQUIDITY, FILTER_REJECTED_TOTAL, FILTER_REJECTED_VELOCITY,
    INTENTS_GENERATED_TOTAL, MARKET_EVENTS_CONSUMED_TOTAL, NATS_ERRORS_TOTAL,
    NATS_MESSAGES_PUBLISHED_TOTAL, NATS_MESSAGES_RECEIVED_TOTAL, POOLS_TRACKED_GAUGE,
    TOKENS_TRACKED_GAUGE,
};
use ironcrab::nats::{
    NatsClient, NatsConfig, TOPIC_EXECUTION_RESULTS, TOPIC_MARKET_EVENTS, TOPIC_TRADE_INTENTS,
};
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

    // === CTO Mode (pre-entry dev sell handling) ===
    cto_enabled: bool,
    cto_entry_delay_secs: u64,
    cto_confirm_window_secs: u64,
    cto_min_unique_buyers: u32,
    cto_min_buy_dominance: f64,
    cto_min_net_inflow_lamports: u64,

    // === Exit Strategy ===
    /// Hard stop-loss percentage from entry (e.g., 15 = -15%). Default: 15%
    hard_stop_loss_pct: f64,
    /// Trailing stop percentage from ATH (e.g., 20 = -20% from high). Default: 20%
    trailing_stop_pct: f64,
    /// Minimum profit to activate trailing stop (e.g., 10 = +10%). Default: 10%
    trailing_activation_pct: f64,
    /// Take profit percentage (e.g., 100 = +100% = 2x). Default: 100%
    take_profit_pct: f64,
    /// Max hold time in seconds before forced exit. Default: 300s (5 min)
    max_hold_time_secs: u64,
    /// Momentum exit: min buy ratio to stay in (e.g., 0.4 = 40% buys). Default: 0.4
    momentum_exit_buy_ratio: f64,
    /// Momentum exit window (seconds). Default: 30s
    momentum_exit_window_secs: u64,
    /// Min trades in momentum window to evaluate exit. Default: 5
    momentum_exit_min_trades: u32,
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

            // Filter 2: Buyer Velocity
            min_unique_buyers: 10,   // 10 unique buyers min
            buyer_window_secs: 20,   // in first 20 seconds
            min_trades_per_sec: 0.5, // 0.5 trades/sec momentum
            min_buy_dominance: 0.6,  // 60% buys vs sells

            // Filter 3: SOL Inflow
            min_sol_inflow_lamports: 20_000_000_000, // 20 SOL net inflow
            inflow_window_secs: 30,                  // in 30 seconds
            max_single_dump_lamports: 5_000_000_000, // Max 5 SOL single sell

            // Filter 4: Dev Behavior
            dev_early_sell_window_secs: 60, // Dev sells in first 60s = bad
            dev_rebuy_positive: true,       // Dev rebuy = positive signal

            // Token Safety
            require_mint_authority_renounced: false,
            require_freeze_authority_none: false,

            // Momentum v2 Entry
            probe_buy_pct: 0.25,
            scale_in_confirm_window_secs: 30,

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

            // CTO Mode
            cto_enabled: false,
            cto_entry_delay_secs: 30,
            cto_confirm_window_secs: 30,
            cto_min_unique_buyers: 5,
            cto_min_buy_dominance: 0.55,
            cto_min_net_inflow_lamports: 1_000_000_000, // 1 SOL

            // Exit Strategy
            hard_stop_loss_pct: 15.0,      // -15% hard stop
            trailing_stop_pct: 20.0,       // -20% from ATH
            trailing_activation_pct: 10.0, // Activate trailing after +10%
            take_profit_pct: 100.0,        // Take profit at +100% (2x)
            max_hold_time_secs: 300,       // Max 5 minutes hold
            momentum_exit_buy_ratio: 0.4,  // Exit if buy ratio < 40%
            momentum_exit_window_secs: 30, // Check last 30s of trades
            momentum_exit_min_trades: 5,   // Need 5+ trades to evaluate
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
            test_allowlist: HashSet::new(), // Not in TOML config
            max_dev_supply_pct: cfg.max_dev_supply_pct,
            lp_removal_window_secs: cfg.lp_removal_window_secs,
            min_unique_buyers: cfg.min_unique_buyers,
            buyer_window_secs: cfg.buyer_window_secs,
            min_trades_per_sec: cfg.min_trades_per_sec,
            min_buy_dominance: cfg.min_buy_dominance,
            min_sol_inflow_lamports: cfg.min_sol_inflow_lamports,
            inflow_window_secs: cfg.inflow_window_secs,
            max_single_dump_lamports: cfg.max_single_dump_lamports,
            dev_early_sell_window_secs: cfg.dev_early_sell_window_secs,
            dev_rebuy_positive: cfg.dev_rebuy_positive,
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
            cto_enabled: cfg.cto_enabled,
            cto_entry_delay_secs: cfg.cto_entry_delay_secs,
            cto_confirm_window_secs: cfg.cto_confirm_window_secs,
            cto_min_unique_buyers: cfg.cto_min_unique_buyers,
            cto_min_buy_dominance: cfg.cto_min_buy_dominance,
            cto_min_net_inflow_lamports: cfg.cto_min_net_inflow_lamports,
            hard_stop_loss_pct: cfg.hard_stop_loss_pct,
            trailing_stop_pct: cfg.trailing_stop_pct,
            trailing_activation_pct: cfg.trailing_activation_pct,
            take_profit_pct: cfg.take_profit_pct,
            max_hold_time_secs: cfg.max_hold_time_secs,
            momentum_exit_buy_ratio: cfg.momentum_exit_buy_ratio,
            momentum_exit_window_secs: cfg.momentum_exit_window_secs,
            momentum_exit_min_trades: cfg.momentum_exit_min_trades,
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
    sol_amount: u64, // in lamports
    token_amount: u64, // raw token units
    signature: String,
}

/// Tracks an open position for exit strategy
#[derive(Debug, Clone)]
struct PositionTracker {
    /// Token mint address
    mint: String,
    /// Pool address for selling
    pool: String,
    /// DEX name
    dex: String,
    /// Entry time
    entry_time: Instant,
    /// Entry price (token per SOL, estimated from trade)
    entry_price: f64,
    /// Amount of tokens held (raw)
    token_amount: u64,
    /// SOL invested (lamports)
    sol_invested: u64,
    /// Highest price seen since entry
    highest_price: f64,
    /// Current estimated price
    current_price: f64,
    /// Recent trades for momentum calculation
    recent_trades: Vec<TradeEvent>,
    /// Has trailing stop been activated?
    trailing_active: bool,
    /// Exit intent already generated?
    exit_generated: bool,
}

impl PositionTracker {
    fn new(
        mint: &str,
        pool: &str,
        dex: &str,
        entry_price: f64,
        token_amount: u64,
        sol_invested: u64,
    ) -> Self {
        Self {
            mint: mint.to_string(),
            pool: pool.to_string(),
            dex: dex.to_string(),
            entry_time: Instant::now(),
            entry_price,
            token_amount,
            sol_invested,
            highest_price: entry_price,
            current_price: entry_price,
            recent_trades: Vec::new(),
            trailing_active: false,
            exit_generated: false,
        }
    }

    /// Update price and track ATH
    fn update_price(&mut self, new_price: f64) {
        self.current_price = new_price;
        if new_price > self.highest_price {
            self.highest_price = new_price;
        }
    }

    /// Record a trade for momentum tracking
    fn record_trade(&mut self, trade: TradeEvent) {
        self.recent_trades.push(trade);
        // Keep only last 100 trades
        if self.recent_trades.len() > 100 {
            self.recent_trades.remove(0);
        }
    }

    /// Calculate current P&L percentage
    fn pnl_pct(&self) -> f64 {
        if self.entry_price <= 0.0 {
            return 0.0;
        }
        ((self.current_price - self.entry_price) / self.entry_price) * 100.0
    }

    /// Calculate drawdown from ATH percentage
    fn drawdown_from_ath_pct(&self) -> f64 {
        if self.highest_price <= 0.0 {
            return 0.0;
        }
        ((self.highest_price - self.current_price) / self.highest_price) * 100.0
    }

    /// Check if we should exit this position
    fn should_exit(&mut self, config: &MomentumConfig) -> Option<(String, String)> {
        // Returns: Some((exit_type, reason)) or None

        let pnl = self.pnl_pct();
        let drawdown = self.drawdown_from_ath_pct();
        let hold_secs = self.entry_time.elapsed().as_secs();

        // 1. Hard Stop Loss - immediate exit
        if pnl <= -config.hard_stop_loss_pct {
            return Some((
                "STOP_LOSS".to_string(),
                format!(
                    "Hard stop hit: {:.1}% loss (limit: -{:.1}%)",
                    pnl, config.hard_stop_loss_pct
                ),
            ));
        }

        // 2. Take Profit - lock in gains
        if pnl >= config.take_profit_pct {
            return Some((
                "TAKE_PROFIT".to_string(),
                format!(
                    "Take profit hit: +{:.1}% gain (target: +{:.1}%)",
                    pnl, config.take_profit_pct
                ),
            ));
        }

        // 3. Trailing Stop - activate after profit threshold
        if pnl >= config.trailing_activation_pct {
            self.trailing_active = true;
        }

        if self.trailing_active && drawdown >= config.trailing_stop_pct {
            return Some((
                "TRAILING_STOP".to_string(),
                format!(
                    "Trailing stop hit: -{:.1}% from ATH (limit: -{:.1}%), P&L: {:.1}%",
                    drawdown, config.trailing_stop_pct, pnl
                ),
            ));
        }

        // 4. Time Exit - max hold time exceeded
        if hold_secs >= config.max_hold_time_secs {
            return Some((
                "TIME_EXIT".to_string(),
                format!(
                    "Max hold time exceeded: {}s (limit: {}s), P&L: {:.1}%",
                    hold_secs, config.max_hold_time_secs, pnl
                ),
            ));
        }

        // 5. Momentum Exit - selling pressure detected
        let momentum_window = Duration::from_secs(config.momentum_exit_window_secs);
        let now = Instant::now();
        let recent: Vec<_> = self
            .recent_trades
            .iter()
            .filter(|t| now.duration_since(t.timestamp) < momentum_window)
            .collect();

        if recent.len() >= config.momentum_exit_min_trades as usize {
            let buy_count = recent.iter().filter(|t| t.is_buy).count();
            let total = recent.len();
            let buy_ratio = buy_count as f64 / total as f64;

            if buy_ratio < config.momentum_exit_buy_ratio {
                return Some((
                    "MOMENTUM_EXIT".to_string(),
                    format!(
                        "Momentum fading: buy ratio {:.0}% < {:.0}% ({}b/{}t), P&L: {:.1}%",
                        buy_ratio * 100.0,
                        config.momentum_exit_buy_ratio * 100.0,
                        buy_count,
                        total,
                        pnl
                    ),
                ));
            }
        }

        None // No exit signal
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
    dev_sold_early: bool, // Sold within dev_early_sell_window
    dev_rebought: bool,

    // LP tracking
    lp_removed: bool,
    lp_removal_time: Option<Instant>,

    // State
    intent_generated: bool, // Already generated an intent for this token
    blacklisted: bool,      // Failed filters, don't trade
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
    fn record_trade(
        &mut self,
        trader: &str,
        is_buy: bool,
        sol_amount: u64,
        token_amount: u64,
        signature: &str,
        config: &MomentumConfig,
    ) {
        let trade = TradeEvent {
            timestamp: Instant::now(),
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

    fn last_trade_ratio(&self) -> Option<(u64, u64)> {
        // Return (sol_lamports, token_amount_raw) from the most recent trade that has both.
        self.trades
            .iter()
            .rev()
            .find(|t| t.sol_amount > 0 && t.token_amount > 0)
            .map(|t| (t.sol_amount, t.token_amount))
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

        let recent_buyers: HashSet<_> = self
            .trades
            .iter()
            .filter(|t| t.is_buy && now.duration_since(t.timestamp) < buyer_window)
            .map(|t| t.trader.clone())
            .collect();

        let (recent_buy_vol, recent_sell_vol) = self
            .trades
            .iter()
            .filter(|t| now.duration_since(t.timestamp) < inflow_window)
            .fold((0u64, 0u64), |(b, s), t| {
                if t.is_buy {
                    (b + t.sol_amount, s)
                } else {
                    (b, s + t.sol_amount)
                }
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
    fn should_generate_intent(
        &self,
        config: &MomentumConfig,
        mint_info: Option<&MintInfo>,
    ) -> (bool, String) {
        // Already generated or blacklisted
        if self.intent_generated {
            return (false, "Already generated intent".to_string());
        }
        if self.blacklisted {
            return (
                false,
                self.blacklist_reason
                    .clone()
                    .unwrap_or("Blacklisted".to_string()),
            );
        }

        // Token safety gates: if enabled, we must have mint info and authorities must be safe.
        if config.require_mint_authority_renounced || config.require_freeze_authority_none {
            let Some(info) = mint_info else {
                return (false, "Awaiting mint safety info".to_string());
            };

            if config.require_mint_authority_renounced && info.mint_authority.is_some() {
                return (false, "Mint authority not renounced".to_string());
            }

            if config.require_freeze_authority_none && info.freeze_authority.is_some() {
                return (false, "Freeze authority still set".to_string());
            }
        }

        let metrics = self.calculate_metrics(config);

        // Filter 1: Liquidity Check
        if metrics.initial_liquidity_sol < config.early_min_liquidity_sol {
            FILTER_REJECTED_LIQUIDITY.fetch_add(1, Ordering::Relaxed);
            FILTER_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
            return (
                false,
                format!(
                    "Liquidity too low: {:.2} SOL (min {:.2})",
                    metrics.initial_liquidity_sol, config.early_min_liquidity_sol
                ),
            );
        }

        // Filter 1b: LP removal
        if metrics.lp_removed {
            FILTER_REJECTED_LIQUIDITY.fetch_add(1, Ordering::Relaxed);
            FILTER_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
            return (false, "LP removed".to_string());
        }

        // Filter 2: Buyer Velocity
        if metrics.unique_buyers_in_window < config.min_unique_buyers {
            FILTER_REJECTED_VELOCITY.fetch_add(1, Ordering::Relaxed);
            FILTER_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
            return (
                false,
                format!(
                    "Not enough buyers: {} (min {})",
                    metrics.unique_buyers_in_window, config.min_unique_buyers
                ),
            );
        }

        if metrics.trades_per_sec < config.min_trades_per_sec {
            FILTER_REJECTED_VELOCITY.fetch_add(1, Ordering::Relaxed);
            FILTER_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
            return (
                false,
                format!(
                    "Trade velocity too low: {:.2}/s (min {:.2})",
                    metrics.trades_per_sec, config.min_trades_per_sec
                ),
            );
        }

        if metrics.buy_dominance < config.min_buy_dominance {
            FILTER_REJECTED_VELOCITY.fetch_add(1, Ordering::Relaxed);
            FILTER_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
            return (
                false,
                format!(
                    "Buy dominance too low: {:.1}% (min {:.1}%)",
                    metrics.buy_dominance * 100.0,
                    config.min_buy_dominance * 100.0
                ),
            );
        }

        // Filter 3: SOL Inflow
        if metrics.net_sol_inflow < config.min_sol_inflow_lamports {
            FILTER_REJECTED_INFLOW.fetch_add(1, Ordering::Relaxed);
            FILTER_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
            return (
                false,
                format!(
                    "SOL inflow too low: {:.2} SOL (min {:.2})",
                    metrics.net_sol_inflow as f64 / 1_000_000_000.0,
                    config.min_sol_inflow_lamports as f64 / 1_000_000_000.0
                ),
            );
        }

        // Filter 4: Dev Behavior
        if metrics.dev_sold_early {
            FILTER_REJECTED_DEV_BEHAVIOR.fetch_add(1, Ordering::Relaxed);
            FILTER_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
            return (false, "Dev sold early".to_string());
        }

        // All filters passed!
        FILTER_PASSED_TOTAL.fetch_add(1, Ordering::Relaxed);
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
    intent_id: String,
    mint: String,
    pool: String,
    dex: String,
    side: TradeSide,
    sol_amount: u64,   // For BUY: SOL invested
    token_amount: u64, // For SELL: tokens to sell
    created_at: Instant,
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
    /// Pump.fun dev wallet info can arrive before PoolCreated.
    /// Store it by mint and apply once the TokenTracker exists.
    pending_dev_info: parking_lot::RwLock<HashMap<String, (String, f64)>>,
    /// Mint metadata (authority/supply/decimals) can arrive independently of pool creation.
    mint_infos: parking_lot::RwLock<HashMap<String, MintInfo>>,
    /// Token trackers for strategy filters (mint -> tracker)
    token_trackers: parking_lot::RwLock<HashMap<String, TokenTracker>>,
    /// Position trackers for exit strategy (mint -> position)
    positions: parking_lot::RwLock<HashMap<String, PositionTracker>>,
    /// Pending intents awaiting execution results (intent_id -> PendingIntent)
    pending_intents: parking_lot::RwLock<HashMap<String, PendingIntent>>,
    /// Stats
    tokens_tracked: std::sync::atomic::AtomicU64,
    tokens_blacklisted: std::sync::atomic::AtomicU64,
    intents_generated: std::sync::atomic::AtomicU64,
    exits_generated: std::sync::atomic::AtomicU64,
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
        let config = self.config.read().clone();
        let mut trackers = self.token_trackers.write();
        if trackers.contains_key(mint) {
            false // Already exists
        } else {
            trackers.insert(
                mint.to_string(),
                TokenTracker::new(mint, pool, dex, slot, liquidity),
            );

            // Apply any dev wallet info that arrived before the tracker existed.
            if let Some((dev_wallet, supply_pct)) = self
                .pending_dev_info
                .read()
                .get(mint)
                .cloned()
            {
                if let Some(tracker) = trackers.get_mut(mint) {
                    let was_blacklisted = tracker.blacklisted;
                    tracker.set_dev_info(&dev_wallet, supply_pct, &config);
                    if !was_blacklisted && tracker.blacklisted {
                        self.tokens_blacklisted
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            }

            self.tokens_tracked
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            true // New tracker created
        }
    }

    /// Record a trade for a token
    fn record_trade(
        &self,
        mint: &str,
        trader: &str,
        is_buy: bool,
        sol_amount: u64,
        token_amount: u64,
        signature: &str,
    ) {
        let config = self.config.read().clone();
        let mut trackers = self.token_trackers.write();
        if let Some(tracker) = trackers.get_mut(mint) {
            let was_blacklisted = tracker.blacklisted;
            tracker.record_trade(trader, is_buy, sol_amount, token_amount, signature, &config);
            if !was_blacklisted && tracker.blacklisted {
                self.tokens_blacklisted
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }

    /// Record dev info for a token
    fn record_dev_info(&self, mint: &str, dev_wallet: &str, supply_pct: f64) {
        let config = self.config.read().clone();
        {
            let mut pending = self.pending_dev_info.write();
            pending.insert(mint.to_string(), (dev_wallet.to_string(), supply_pct));
        }

        let mut trackers = self.token_trackers.write();
        if let Some(tracker) = trackers.get_mut(mint) {
            let was_blacklisted = tracker.blacklisted;
            tracker.set_dev_info(dev_wallet, supply_pct, &config);
            if !was_blacklisted && tracker.blacklisted {
                self.tokens_blacklisted
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
                self.tokens_blacklisted
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }

    /// Check if any tracked token should generate an intent
    fn check_for_signals(&self) -> Vec<(String, String, String, String)> {
        // Returns: Vec<(mint, pool, dex, reason)>
        let config = self.config.read().clone();
        let mint_infos = self.mint_infos.read();
        let mut trackers = self.token_trackers.write();
        let mut signals = Vec::new();

        for (mint, tracker) in trackers.iter_mut() {
            if tracker.intent_generated || tracker.blacklisted {
                continue;
            }

            let mint_info = mint_infos.get(mint);
            let (should_trade, reason) = tracker.should_generate_intent(&config, mint_info);
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

    // =========================================================================
    // Position Management for Exit Strategy
    // =========================================================================

    /// Open a new position after buy intent is executed
    fn open_position(
        &self,
        mint: &str,
        pool: &str,
        dex: &str,
        entry_price: f64,
        token_amount: u64,
        sol_invested: u64,
    ) {
        let mut positions = self.positions.write();
        if positions.contains_key(mint) {
            warn!(mint = %mint, "Position already exists, not opening duplicate");
            return;
        }
        positions.insert(
            mint.to_string(),
            PositionTracker::new(mint, pool, dex, entry_price, token_amount, sol_invested),
        );
        info!(
            mint = %mint,
            entry_price = entry_price,
            sol_invested = sol_invested,
            "📈 Position opened"
        );
    }

    /// Update position price from market trade
    fn update_position_price(&self, mint: &str, new_price: f64, trade: Option<TradeEvent>) {
        let mut positions = self.positions.write();
        if let Some(pos) = positions.get_mut(mint) {
            pos.update_price(new_price);
            if let Some(t) = trade {
                pos.record_trade(t);
            }
        }
    }

    /// Close position (after sell executed)
    fn close_position(&self, mint: &str) {
        let mut positions = self.positions.write();
        if let Some(pos) = positions.remove(mint) {
            let pnl = pos.pnl_pct();
            let hold_secs = pos.entry_time.elapsed().as_secs();
            info!(
                mint = %mint,
                pnl_pct = pnl,
                hold_time_secs = hold_secs,
                "📉 Position closed"
            );
        }
    }

    /// Check all positions for exit signals
    fn check_for_exits(&self) -> Vec<(String, String, String, String, String, u64)> {
        // Returns: Vec<(mint, pool, dex, exit_type, reason, token_amount)>
        let config = self.config.read().clone();
        let mut positions = self.positions.write();
        let mut exits = Vec::new();

        for (mint, pos) in positions.iter_mut() {
            if pos.exit_generated {
                continue;
            }

            if let Some((exit_type, reason)) = pos.should_exit(&config) {
                pos.exit_generated = true;
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

        exits
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
    ) {
        let mut pending = self.pending_intents.write();
        pending.insert(
            intent_id.to_string(),
            PendingIntent {
                intent_id: intent_id.to_string(),
                mint: mint.to_string(),
                pool: pool.to_string(),
                dex: dex.to_string(),
                side: TradeSide::Buy,
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
                intent_id: intent_id.to_string(),
                mint: mint.to_string(),
                pool: pool.to_string(),
                dex: dex.to_string(),
                side: TradeSide::Sell,
                sol_amount: 0,
                token_amount,
                created_at: Instant::now(),
            },
        );
        debug!(intent_id = %intent_id, mint = %mint, "Registered pending SELL intent");
    }

    /// Handle execution result from execution-engine
    fn handle_execution_result(&self, result: &ExecutionResult) {
        // Only process results from our own intents
        if result.source != "momentum-bot"
            && !result.source.starts_with("4filter:")
            && !result.source.starts_with("exit:")
        {
            trace!(source = %result.source, "Ignoring execution result from other source");
            return;
        }

        // Find the pending intent
        let pending_opt = {
            let mut pending = self.pending_intents.write();
            pending.remove(&result.intent_id)
        };

        let Some(pending) = pending_opt else {
            debug!(intent_id = %result.intent_id, "No pending intent found for execution result");
            return;
        };

        match result.status {
            ExecutionStatus::Confirmed => {
                match pending.side {
                    TradeSide::Buy => {
                        // BUY confirmed - open position
                        // Estimate entry price: we don't have exact token amount from ExecutionResult
                        // For now, use a placeholder and update from market data
                        let estimated_price = 1.0; // Will be updated from market trades
                        let estimated_tokens = pending.sol_amount; // Placeholder

                        info!(
                            intent_id = %result.intent_id,
                            mint = %pending.mint,
                            pool = %pending.pool,
                            sol_invested = pending.sol_amount,
                            signature = ?result.signature,
                            "✅ BUY CONFIRMED - Opening position"
                        );

                        self.open_position(
                            &pending.mint,
                            &pending.pool,
                            &pending.dex,
                            estimated_price,
                            estimated_tokens,
                            pending.sol_amount,
                        );
                    }
                    TradeSide::Sell => {
                        // SELL confirmed - close position
                        info!(
                            intent_id = %result.intent_id,
                            mint = %pending.mint,
                            token_amount = pending.token_amount,
                            signature = ?result.signature,
                            pnl = ?result.pnl,
                            "✅ SELL CONFIRMED - Closing position"
                        );

                        self.close_position(&pending.mint);
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
                // Don't open position on failure
            }
            ExecutionStatus::Timeout => {
                warn!(
                    intent_id = %result.intent_id,
                    mint = %pending.mint,
                    side = ?pending.side,
                    "⏱️ Execution TIMEOUT"
                );
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
        let before = pending.len();
        pending.retain(|_, p| p.created_at.elapsed() < cutoff);
        let removed = before - pending.len();
        if removed > 0 {
            debug!(removed = removed, "Cleaned up stale pending intents");
        }
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

                // === CTO Mode ===
                "cto_enabled" => {
                    if let Some(v) = value.as_bool() {
                        config.cto_enabled = v;
                        applied.push(key.clone());
                        info!(key = %key, new_value = %v, "Config updated");
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected bool".to_string()));
                    }
                }
                "cto_entry_delay_secs" => {
                    if let Some(v) = value.as_u64() {
                        if v > 0 {
                            config.cto_entry_delay_secs = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be > 0".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "cto_confirm_window_secs" => {
                    if let Some(v) = value.as_u64() {
                        if v > 0 {
                            config.cto_confirm_window_secs = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be > 0".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "cto_min_unique_buyers" => {
                    if let Some(v) = value.as_u64() {
                        if v > 0 {
                            config.cto_min_unique_buyers = v as u32;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be > 0".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "cto_min_buy_dominance" => {
                    if let Some(v) = value.as_f64() {
                        if (0.0..=1.0).contains(&v) {
                            config.cto_min_buy_dominance = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be in [0.0, 1.0]".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected f64".to_string()));
                    }
                }
                "cto_min_net_inflow_lamports" => {
                    if let Some(v) = value.as_u64() {
                        config.cto_min_net_inflow_lamports = v;
                        applied.push(key.clone());
                        info!(key = %key, new_value = %v, "Config updated");
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
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
                        match momentum_table.clone().try_into::<MomentumCfg>() {
                            Ok(cfg) => {
                                // Apply config to MomentumConfig
                                momentum_config = MomentumConfig::from_cfg(&cfg);
                                info!(
                                    config_path = %args.config.display(),
                                    min_sol_inflow = momentum_config.min_sol_inflow_lamports / 1_000_000_000,
                                    min_unique_buyers = momentum_config.min_unique_buyers,
                                    min_trades_per_sec = momentum_config.min_trades_per_sec,
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
        pending_dev_info: parking_lot::RwLock::new(HashMap::new()),
        mint_infos: parking_lot::RwLock::new(HashMap::new()),
        token_trackers: parking_lot::RwLock::new(HashMap::new()),
        positions: parking_lot::RwLock::new(HashMap::new()),
        pending_intents: parking_lot::RwLock::new(HashMap::new()),
        tokens_tracked: std::sync::atomic::AtomicU64::new(0),
        tokens_blacklisted: std::sync::atomic::AtomicU64::new(0),
        intents_generated: std::sync::atomic::AtomicU64::new(0),
        exits_generated: std::sync::atomic::AtomicU64::new(0),
    });

    // === Main Loop: Process MarketEvents from NATS ===
    info!("Entering main event loop");

    // P1 Crash Isolation: Signal systemd that we're ready
    #[cfg(unix)]
    {
        // NOTE: Do NOT unset NOTIFY_SOCKET here; we need it for Watchdog pings.
        let _ = sd_notify::notify(false, &[NotifyState::Ready]);
        debug!("Sent sd_notify READY to systemd");
    }

    // Keep readiness fresh even when idle.
    ironcrab::metrics::record_activity();

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

    // Subscribe to ExecutionResults (for position management)
    let mut execution_subscription = if let Some(ref nats) = ctx.nats {
        match nats.subscribe(TOPIC_EXECUTION_RESULTS).await {
            Ok(sub) => {
                info!(
                    topic = TOPIC_EXECUTION_RESULTS,
                    "Subscribed to ExecutionResults"
                );
                Some(sub)
            }
            Err(e) => {
                warn!(error = %e, "Failed to subscribe to ExecutionResults");
                None
            }
        }
    } else {
        None
    };

    // Heartbeat and stats tracking
    let mut heartbeat_interval = tokio::time::interval(std::time::Duration::from_secs(60));
    let mut activity_interval = tokio::time::interval(std::time::Duration::from_secs(10));
    let mut events_received: u64 = 0;
    let mut last_slot: u64 = 0;

    // Graceful shutdown handling
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            // Keep /ready fresh even if there are no events.
            _ = activity_interval.tick() => {
                ironcrab::metrics::record_activity();

                // P1 Crash Isolation: Ping systemd watchdog frequently enough.
                #[cfg(unix)]
                let _ = sd_notify::notify(false, &[NotifyState::Watchdog]);
            }

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
                    ironcrab::metrics::record_activity();
                    NATS_MESSAGES_RECEIVED_TOTAL.fetch_add(1, Ordering::Relaxed);
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
                    ironcrab::metrics::record_activity();
                    NATS_MESSAGES_RECEIVED_TOTAL.fetch_add(1, Ordering::Relaxed);
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

            // Handle ExecutionResults (position management)
            msg = async {
                if let Some(ref mut sub) = execution_subscription {
                    sub.next().await
                } else {
                    std::future::pending::<Option<ironcrab::nats::NatsMessage>>().await
                }
            } => {
                if let Some(nats_msg) = msg {
                    ironcrab::metrics::record_activity();
                    NATS_MESSAGES_RECEIVED_TOTAL.fetch_add(1, Ordering::Relaxed);
                    match serde_json::from_slice::<ExecutionResult>(&nats_msg.payload) {
                        Ok(result) => {
                            debug!(
                                intent_id = %result.intent_id,
                                status = ?result.status,
                                source = %result.source,
                                "Received ExecutionResult"
                            );
                            ctx.handle_execution_result(&result);
                        }
                        Err(e) => {
                            warn!(error = %e, "Failed to deserialize ExecutionResult");
                        }
                    }
                }
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

                // === Check for ENTRY signals ===
                let signals = ctx.check_for_signals();
                for (mint, pool, dex, reason) in signals {
                    info!(
                        mint = %mint,
                        pool = %pool,
                        dex = %dex,
                        reason = %reason,
                        "🎯 ENTRY SIGNAL DETECTED"
                    );

                    // Generate and publish BUY intent
                    if let Err(e) = generate_and_publish_intent(&ctx, &mint, &pool, &dex, &reason).await {
                        error!(error = %e, mint = %mint, "Failed to generate/publish buy intent");
                    } else {
                        ctx.intents_generated.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }

                // === Check for EXIT signals ===
                let exits = ctx.check_for_exits();
                for (mint, pool, dex, exit_type, reason, token_amount) in exits {
                    info!(
                        mint = %mint,
                        pool = %pool,
                        exit_type = %exit_type,
                        reason = %reason,
                        token_amount = token_amount,
                        "🚨 EXIT SIGNAL DETECTED"
                    );

                    // Generate and publish SELL intent
                    if let Err(e) = generate_and_publish_exit_intent(&ctx, &mint, &pool, &dex, &exit_type, &reason, token_amount).await {
                        error!(error = %e, mint = %mint, "Failed to generate/publish sell intent");
                    } else {
                        ctx.exits_generated.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }

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

    let intent_id = ctx.next_intent_id();

    let (creator_opt, last_trade_ratio_opt) = {
        let trackers = ctx.token_trackers.read();
        let tracker = trackers.get(mint);
        (
            tracker.and_then(|t| t.dev_wallet.clone()),
            tracker.and_then(|t| t.last_trade_ratio()),
        )
    };

    let (last_sol_lamports, last_token_amount) = match last_trade_ratio_opt {
        Some(v) => v,
        None => {
            warn!(
                mint = %mint,
                pool = %pool,
                dex = %dex,
                reason = %reason,
                "Skipping BUY intent: no usable trade ratio yet (need sol_amount+token_amount)"
            );
            anyhow::bail!(
                "cannot generate intent: no usable trade ratio yet (need sol_amount+token_amount)"
            )
        }
    };

    // Estimate expected token output using last observed ratio.
    // expected_out_raw = position_lamports * last_token_amount / last_sol_lamports
    let expected_out_raw: u64 = ((position_lamports as u128)
        .saturating_mul(last_token_amount as u128)
        / (last_sol_lamports as u128))
        .min(u64::MAX as u128) as u64;

    let min_out_raw: u64 = ((expected_out_raw as u128)
        .saturating_mul((10_000u32.saturating_sub(max_slippage)) as u128)
        / 10_000u128)
        .min(u64::MAX as u128) as u64;

    let mut intent = TradeIntent::new(
        "momentum-bot",
        BUILD_VERSION,
        &ctx.run_id,
        intent_id.clone(),
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

    // Provide deterministic execution constraints required by the execution-engine.
    // Use 6 as common default decimals for Pump.fun tokens.
    intent.execution = Some(TradeExecutionConstraints {
        min_out: Some(ExplicitAmount::new(min_out_raw, 6)),
    });

    // Keep legacy metadata for backward compatibility, and provide routing hints.
    intent
        .metadata
        .insert("min_out_raw".to_string(), min_out_raw.to_string());
    intent.metadata.insert("dex".to_string(), dex.to_string());

    // Pump.fun tx building requires the creator/dev wallet.
    if dex == "pumpfun" {
        let creator = creator_opt
            .ok_or_else(|| anyhow::anyhow!("cannot generate pumpfun intent: missing dev_wallet/creator"))?;
        intent.metadata.insert("creator".to_string(), creator);
    }

    // Register pending intent BEFORE publishing
    ctx.register_buy_intent(&intent_id, mint, pool, dex, position_lamports);

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
        match nats.publish(TOPIC_TRADE_INTENTS, &intent).await {
            Ok(true) => {
                NATS_MESSAGES_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
            }
            Ok(false) => {
                NATS_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                anyhow::bail!("NATS publish dropped/failed topic={}", TOPIC_TRADE_INTENTS);
            }
            Err(e) => {
                NATS_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                return Err(e);
            }
        }
    }

    Ok(())
}

/// Generate and publish a SELL intent for position exit
async fn generate_and_publish_exit_intent(
    ctx: &MomentumContext,
    mint: &str,
    pool: &str,
    dex: &str,
    exit_type: &str,
    reason: &str,
    token_amount: u64,
) -> Result<()> {
    let config = ctx.config.read();
    let max_slippage = config.early_max_slippage_bps; // Use higher slippage for exits
    drop(config);

    // SOL as output (selling tokens for SOL)
    let sol_mint = "So11111111111111111111111111111111111111112";

    // Decimals depend on token, usually 6 or 9 for meme tokens
    // Use 6 as common default for PumpFun tokens
    let token_decimals = 6u8;

    let intent_id = ctx.next_intent_id();

    let (creator_opt, last_trade_ratio_opt) = {
        let trackers = ctx.token_trackers.read();
        let tracker = trackers.get(mint);
        (
            tracker.and_then(|t| t.dev_wallet.clone()),
            tracker.and_then(|t| t.last_trade_ratio()),
        )
    };

    let (last_sol_lamports, last_token_amount) = match last_trade_ratio_opt {
        Some(v) => v,
        None => {
            warn!(
                mint = %mint,
                pool = %pool,
                dex = %dex,
                exit_type = %exit_type,
                reason = %reason,
                "Skipping SELL intent: no usable trade ratio yet (need sol_amount+token_amount)"
            );
            anyhow::bail!(
                "cannot generate exit intent: no usable trade ratio yet (need sol_amount+token_amount)"
            )
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

    let mut intent = TradeIntent::new(
        "momentum-bot",
        BUILD_VERSION,
        &ctx.run_id,
        intent_id.clone(),
        &format!("exit:{}:{}", exit_type, reason),
        IntentTier::Tier1,
        IntentOrigin::StrategyA,
        ExplicitAmount::new(token_amount, token_decimals),
        TradeResources {
            input_mint: mint.to_string(),      // Selling tokens
            output_mint: sol_mint.to_string(), // Receiving SOL
            pools: vec![pool.to_string()],
            accounts: vec![],
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

    // Pump.fun sell tx building requires the creator/dev wallet.
    if dex == "pumpfun" {
        let creator = creator_opt
            .ok_or_else(|| anyhow::anyhow!("cannot generate pumpfun exit: missing dev_wallet/creator"))?;
        intent.metadata.insert("creator".to_string(), creator);
    }

    // Register pending intent BEFORE publishing
    ctx.register_sell_intent(&intent_id, mint, pool, dex, token_amount);

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

    // Write to JSONL (P0 requirement)
    ctx.jsonl_writer.write(&intent)?;

    // Publish to NATS
    if let Some(ref nats) = ctx.nats {
        match nats.publish(TOPIC_TRADE_INTENTS, &intent).await {
            Ok(true) => {
                NATS_MESSAGES_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
            }
            Ok(false) => {
                NATS_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                anyhow::bail!("NATS publish dropped/failed topic={}", TOPIC_TRADE_INTENTS);
            }
            Err(e) => {
                NATS_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                return Err(e);
            }
        }
    }

    Ok(())
}

/// Process a MarketEvent and update token trackers
async fn process_market_event(ctx: &MomentumContext, event: &MarketEvent) -> Result<()> {
    match &event.kind {
        MarketEventKind::PoolCreated {
            pool_address,
            base_mint,
            quote_mint: _quote_mint,
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
            let min_liq = config.early_min_liquidity_sol;
            drop(config);

            // Create tracker if liquidity meets minimum threshold
            if liq_sol >= min_liq {
                let slot = event.slot.unwrap_or(0);
                let liq_lamports = (liq_sol * 1_000_000_000.0) as u64;
                let created =
                    ctx.get_or_create_tracker(base_mint, pool_address, dex, slot, liq_lamports);

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
            token_amount,
            signature,
        } => {
            // Record the trade in the tracker
            let sol_lamports = *sol_amount as u64;
            let token_raw = *token_amount as u64;
            let sig = signature.clone().unwrap_or_default();

            // Check if this trader is the dev wallet and record dev behavior
            let is_dev = {
                let trackers = ctx.token_trackers.read();
                trackers
                    .get(mint)
                    .and_then(|t| t.dev_wallet.as_ref())
                    .map(|dw| dw == trader)
                    .unwrap_or(false)
            };

            ctx.record_trade(mint, trader, *is_buy, sol_lamports, token_raw, &sig);

            // Update open position price estimate (tokens per SOL) based on trade ratio.
            if sol_lamports > 0 && token_raw > 0 {
                let tokens_per_sol = (token_raw as f64) / (sol_lamports as f64 / 1_000_000_000.0);
                let trade = TradeEvent {
                    timestamp: Instant::now(),
                    trader: trader.to_string(),
                    is_buy: *is_buy,
                    sol_amount: sol_lamports,
                    token_amount: token_raw,
                    signature: sig.clone(),
                };
                ctx.update_position_price(mint, tokens_per_sol, Some(trade));
            }

            if is_dev {
                // Record dev trade behavior in tracker
                let mut trackers = ctx.token_trackers.write();
                if let Some(tracker) = trackers.get_mut(mint) {
                    if *is_buy {
                        tracker.dev_rebought = true;
                        info!(mint = %mint, trader = %trader, "📈 Dev rebuy detected - positive signal");
                    } else {
                        tracker.dev_sold = true;
                        info!(mint = %mint, trader = %trader, sol = sol_lamports, "⚠️ Dev sell detected");
                    }
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
