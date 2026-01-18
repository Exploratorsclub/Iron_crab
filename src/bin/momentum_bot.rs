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
    ExplicitAmount, IntentOrigin, IntentTier, MarketEvent, MarketEventKind,
    TradeExecutionConstraints, TradeIntent, TradeResources, TradeSide, TradingRegime,
};
use ironcrab::metrics::{
    serve_metrics, FILTER_PASSED_TOTAL, FILTER_REJECTED_BUYER_QUALITY,
    FILTER_REJECTED_DEV_BEHAVIOR, FILTER_REJECTED_INFLOW, FILTER_REJECTED_LIQUIDITY,
    FILTER_REJECTED_TOTAL, FILTER_REJECTED_VELOCITY, INTENTS_GENERATED_TOTAL,
    MARKET_EVENTS_CONSUMED_TOTAL, NATS_ERRORS_TOTAL, NATS_MESSAGES_PUBLISHED_TOTAL,
    NATS_MESSAGES_RECEIVED_TOTAL, POOLS_TRACKED_GAUGE, TOKENS_TRACKED_GAUGE,
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
    sol_amount: u64,   // in lamports
    token_amount: u64, // raw token units
    signature: String,
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

    fn add_investment(&mut self, additional_sol: u64) {
        if additional_sol == 0 {
            return;
        }

        // Heuristic: weighted-average entry price based on SOL invested.
        // Exact fill price/token amount is not available from ExecutionResult today.
        let old_sol = self.sol_invested.max(1);
        let new_sol = old_sol.saturating_add(additional_sol).max(1);
        let new_entry = ((self.entry_price * (old_sol as f64))
            + (self.current_price * (additional_sol as f64)))
            / (new_sol as f64);

        self.sol_invested = self.sol_invested.saturating_add(additional_sol);
        self.entry_price = new_entry;
        self.highest_price = self.highest_price.max(self.current_price);
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

    /// DEX-specific static accounts needed for deterministic tx building (e.g. PumpFunAmm).
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
    dev_sold_early: bool, // Sold within dev_early_sell_window
    dev_rebought: bool,

    // LP tracking
    lp_removed: bool,
    lp_removal_time: Option<Instant>,

    // Dump-recovery gating (pre-entry)
    dump_observed_at: Option<Instant>,
    recovery_started_at: Option<Instant>,

    // CTO mode (pre-entry dev-sell handling)
    cto_started_at: Option<Instant>,
    cto_recovery_confirmed: bool,

    // State
    /// Entry lifecycle: probe then optional scale-in.
    probe_sent_at: Option<Instant>,
    probe_filled_at: Option<Instant>,
    scale_sent_at: Option<Instant>,
    scale_filled_at: Option<Instant>,
    /// Terminal flag: entry complete (scale-in done or abandoned) and no further entry intents.
    intent_generated: bool,
    blacklisted: bool, // Failed filters, don't trade
    blacklist_reason: Option<String>,
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
            dev_sold_early: false,
            dev_rebought: false,
            lp_removed: false,
            lp_removal_time: None,
            dump_observed_at: None,
            recovery_started_at: None,
            cto_started_at: None,
            cto_recovery_confirmed: false,
            probe_sent_at: None,
            probe_filled_at: None,
            scale_sent_at: None,
            scale_filled_at: None,
            intent_generated: false,
            blacklisted: false,
            blacklist_reason: None,
        }
    }

    fn dump_recovery_window_stats_at(
        &self,
        config: &MomentumConfig,
        now: Instant,
    ) -> Option<(f64, i128, u32)> {
        if config.dump_recovery_window_secs == 0 {
            return None;
        }

        let window = Duration::from_secs(config.dump_recovery_window_secs);
        let mut buy_count: u32 = 0;
        let mut sell_count: u32 = 0;
        let mut buy_vol: u64 = 0;
        let mut sell_vol: u64 = 0;

        for t in self
            .trades
            .iter()
            .filter(|t| now.duration_since(t.timestamp) < window)
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
        now: Instant,
    ) -> Option<String> {
        let (buy_dominance, net_inflow, samples) =
            self.dump_recovery_window_stats_at(config, now)?;

        // Require a minimum sample size to avoid flapping on tiny windows.
        let min_samples = config.min_unique_buyers.max(5);
        if samples < min_samples {
            return None;
        }

        let window = Duration::from_secs(config.dump_recovery_window_secs);

        // Expire old dump flags once the window rolls over.
        if let Some(dump_at) = self.dump_observed_at {
            if now.duration_since(dump_at) > window {
                self.dump_observed_at = None;
                self.recovery_started_at = None;
            }
        }

        // Dump detected = net outflow in the recovery window.
        if net_inflow < 0 {
            self.dump_observed_at = Some(now);
            self.recovery_started_at = None;
            return Some(format!(
                "WAIT_CONFIRMATION: dump detected (net_inflow={:.2} SOL over {}s)",
                net_inflow as f64 / 1_000_000_000.0,
                config.dump_recovery_window_secs
            ));
        }

        // If we haven't observed a dump, don't gate on recovery.
        self.dump_observed_at?;

        let recovery_ok = buy_dominance >= config.dump_recovery_min_buy_dominance
            && net_inflow >= config.dump_recovery_min_net_inflow_lamports as i128;

        if !recovery_ok {
            self.recovery_started_at = None;
            return Some(format!(
                "WAIT_CONFIRMATION: dump recovery not confirmed yet (dom={:.0}% < {:.0}%, inflow={:.2} SOL < {:.2} SOL)",
                buy_dominance * 100.0,
                config.dump_recovery_min_buy_dominance * 100.0,
                net_inflow as f64 / 1_000_000_000.0,
                config.dump_recovery_min_net_inflow_lamports as f64 / 1_000_000_000.0
            ));
        }

        let start = self.recovery_started_at.get_or_insert(now);
        if now.duration_since(*start).as_secs() < config.dump_recovery_min_recovery_secs {
            return Some(format!(
                "WAIT_CONFIRMATION: dump recovery stabilizing ({}/{}s)",
                now.duration_since(*start).as_secs(),
                config.dump_recovery_min_recovery_secs
            ));
        }

        // Recovery confirmed.
        self.dump_observed_at = None;
        self.recovery_started_at = None;
        None
    }

    fn cto_confirm_stats_at(
        &self,
        config: &MomentumConfig,
        now: Instant,
    ) -> Option<(u32, f64, i128, u32)> {
        if config.cto_confirm_window_secs == 0 {
            return None;
        }

        let window = Duration::from_secs(config.cto_confirm_window_secs);
        let mut unique_buyers: HashSet<&str> = HashSet::new();
        let mut buy_count: u32 = 0;
        let mut sell_count: u32 = 0;
        let mut buy_vol: u64 = 0;
        let mut sell_vol: u64 = 0;

        for t in self
            .trades
            .iter()
            .filter(|t| now.duration_since(t.timestamp) < window)
        {
            if t.is_buy {
                buy_count = buy_count.saturating_add(1);
                buy_vol = buy_vol.saturating_add(t.sol_amount);
                unique_buyers.insert(t.trader.as_str());
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
        Some((unique_buyers.len() as u32, buy_dominance, net_inflow, total))
    }

    fn cto_wait_reason(&mut self, config: &MomentumConfig, now: Instant) -> Option<String> {
        if !config.cto_enabled {
            return None;
        }
        if self.cto_recovery_confirmed {
            return None;
        }

        let started = self.cto_started_at.get_or_insert(now);
        let since = now.duration_since(*started).as_secs();
        if since < config.cto_entry_delay_secs {
            return Some(format!(
                "CTO_WAIT_RECOVERY: entry delay {}/{}s",
                since, config.cto_entry_delay_secs
            ));
        }

        let Some((buyers, dom, net_inflow, samples)) = self.cto_confirm_stats_at(config, now)
        else {
            return Some("CTO_WAIT_RECOVERY: awaiting confirmation window data".to_string());
        };

        // Use configured threshold (separate from early min_unique_buyers) and a small sample guard.
        if samples < config.cto_min_unique_buyers.max(5) {
            return Some(format!(
                "CTO_WAIT_RECOVERY: not enough samples in confirm window ({} < {})",
                samples,
                config.cto_min_unique_buyers.max(5)
            ));
        }

        if buyers < config.cto_min_unique_buyers {
            return Some(format!(
                "CTO_WAIT_RECOVERY: not enough buyers in confirm window ({} < {})",
                buyers, config.cto_min_unique_buyers
            ));
        }

        if dom < config.cto_min_buy_dominance {
            return Some(format!(
                "CTO_WAIT_RECOVERY: buy dominance {:.0}% < {:.0}%",
                dom * 100.0,
                config.cto_min_buy_dominance * 100.0
            ));
        }

        if net_inflow < config.cto_min_net_inflow_lamports as i128 {
            return Some(format!(
                "CTO_WAIT_RECOVERY: net inflow {:.2} SOL < {:.2} SOL",
                net_inflow as f64 / 1_000_000_000.0,
                config.cto_min_net_inflow_lamports as f64 / 1_000_000_000.0
            ));
        }

        // Optional positive signal (non-gating): read config so it isn't dead, and enrich reason in logs.
        if config.dev_rebuy_positive && self.dev_rebought {
            debug!(mint = %self.mint, "CTO recovery has dev rebuy positive signal");
        }

        self.cto_recovery_confirmed = true;
        None
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

                        if config.cto_enabled {
                            // CTO mode: do not hard-reject pre-entry dev sells.
                            // Instead, mark CTO candidate and wait for recovery confirmation.
                            self.cto_started_at.get_or_insert_with(Instant::now);
                            warn!(
                                mint = %self.mint,
                                age_secs = age.as_secs(),
                                "Dev sold early - CTO candidate"
                            );
                        } else {
                            // No CTO mode: hard reject.
                            self.blacklisted = true;
                            self.blacklist_reason = Some("REJECT_DEV_SELL_EARLY".to_string());
                            warn!(
                                mint = %self.mint,
                                age_secs = age.as_secs(),
                                "Dev sold early - blacklisting"
                            );
                        }
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

    fn buyer_quality_stats(&self, config: &MomentumConfig) -> BuyerQualityStats {
        self.buyer_quality_stats_at(config, Instant::now())
    }

    fn micro_buy_stats_at(&self, config: &MomentumConfig, now: Instant) -> (u32, u32, f64) {
        let buyer_window = Duration::from_secs(config.buyer_window_secs);

        let mut total_buys: u32 = 0;
        let mut small_buys: u32 = 0;

        for trade in self
            .trades
            .iter()
            .filter(|t| t.is_buy && now.duration_since(t.timestamp) < buyer_window)
        {
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

    fn buyer_quality_stats_at(&self, config: &MomentumConfig, now: Instant) -> BuyerQualityStats {
        let buyer_window = Duration::from_secs(config.buyer_window_secs);

        let mut per_wallet: HashMap<String, (u64, u32)> = HashMap::new();
        let mut total_buy_volume_lamports: u64 = 0;

        for trade in self
            .trades
            .iter()
            .filter(|t| t.is_buy && now.duration_since(t.timestamp) < buyer_window)
        {
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

    /// Record LP removal
    fn record_lp_removal(&mut self) {
        self.lp_removed = true;
        self.lp_removal_time = Some(Instant::now());
        self.blacklisted = true;
        self.blacklist_reason = Some("REJECT_LP_REMOVED".to_string());
        warn!(mint = %self.mint, "LP removed - blacklisting");
    }

    /// Set dev wallet and supply percentage
    fn set_dev_info(&mut self, dev_wallet: &str, supply_pct: f64, config: &MomentumConfig) {
        self.dev_wallet = Some(dev_wallet.to_string());
        self.dev_supply_pct = Some(supply_pct);

        if supply_pct > config.max_dev_supply_pct {
            self.blacklisted = true;
            self.blacklist_reason = Some(format!(
                "REJECT_DEV_SUPPLY_TOO_HIGH: {:.1}% > {:.1}%",
                supply_pct, config.max_dev_supply_pct
            ));
            warn!(mint = %self.mint, supply_pct, "Dev supply too high - blacklisting");
        }
    }

    /// Calculate metrics for strategy decision
    fn calculate_metrics(&self, config: &MomentumConfig) -> TokenMetrics {
        let age = self.first_seen.elapsed();
        let age_secs = age.as_secs().max(1) as f64;

        // Keep first_slot as a recorded attribute (useful for debugging/forensics).
        let _first_slot = self.first_slot;

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
            unique_buyers_in_window: recent_buyers.len() as u32,
            trades_per_sec,
            buy_dominance,
            net_sol_inflow: recent_buy_vol.saturating_sub(recent_sell_vol),
            dev_sold_early: self.dev_sold_early,
            lp_removed: self.lp_removed,
            initial_liquidity_sol: self.initial_liquidity as f64 / 1_000_000_000.0,
        }
    }

    /// Check all 4 filters and return if we should trade
    fn should_generate_intent(
        &mut self,
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
                return (false, "WAIT_MINT_INFO".to_string());
            };

            if config.require_mint_authority_renounced && info.mint_authority.is_some() {
                self.blacklisted = true;
                self.blacklist_reason = Some("REJECT_MINT_AUTHORITY_NOT_RENOUNCED".to_string());
                return (false, "REJECT_MINT_AUTHORITY_NOT_RENOUNCED".to_string());
            }

            if config.require_freeze_authority_none && info.freeze_authority.is_some() {
                self.blacklisted = true;
                self.blacklist_reason = Some("REJECT_FREEZE_AUTHORITY_SET".to_string());
                return (false, "REJECT_FREEZE_AUTHORITY_SET".to_string());
            }
        }

        let metrics = self.calculate_metrics(config);

        // Filter 1: Liquidity Check
        if metrics.initial_liquidity_sol < config.early_min_liquidity_sol {
            FILTER_REJECTED_LIQUIDITY.fetch_add(1, Ordering::Relaxed);
            FILTER_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
            let reason = format!(
                "WAIT_INSUFFICIENT_LIQUIDITY: liq {:.2} SOL < {:.2} SOL",
                metrics.initial_liquidity_sol, config.early_min_liquidity_sol
            );
            warn!(mint = %self.mint, pool = %self.pool, dex = %self.dex, reason = %reason, "🚫 Filter rejected");
            return (false, reason);
        }

        // Filter 1b: LP removal
        if metrics.lp_removed {
            FILTER_REJECTED_LIQUIDITY.fetch_add(1, Ordering::Relaxed);
            FILTER_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
            self.blacklisted = true;
            self.blacklist_reason = Some("REJECT_LP_REMOVED".to_string());
            warn!(mint = %self.mint, pool = %self.pool, dex = %self.dex, "🚫 Filter rejected: LP_REMOVED (blacklisted)");
            return (false, "REJECT_LP_REMOVED".to_string());
        }

        // Filter 2: Buyer Velocity
        if metrics.unique_buyers_in_window < config.min_unique_buyers {
            FILTER_REJECTED_VELOCITY.fetch_add(1, Ordering::Relaxed);
            FILTER_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
            let reason = format!(
                "WAIT_BUYER_WINDOW: buyers {} < {}",
                metrics.unique_buyers_in_window, config.min_unique_buyers
            );
            warn!(mint = %self.mint, pool = %self.pool, dex = %self.dex, reason = %reason, "🚫 Filter rejected");
            return (false, reason);
        }

        if metrics.trades_per_sec < config.min_trades_per_sec {
            FILTER_REJECTED_VELOCITY.fetch_add(1, Ordering::Relaxed);
            FILTER_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
            let reason = format!(
                "WAIT_BUYER_WINDOW: trades_per_sec {:.3} < {:.3}",
                metrics.trades_per_sec, config.min_trades_per_sec
            );
            warn!(mint = %self.mint, pool = %self.pool, dex = %self.dex, reason = %reason, "🚫 Filter rejected");
            return (false, reason);
        }

        if metrics.buy_dominance < config.min_buy_dominance {
            FILTER_REJECTED_VELOCITY.fetch_add(1, Ordering::Relaxed);
            FILTER_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
            let reason = format!(
                "WAIT_BUYER_WINDOW: buy dominance {:.0}% < {:.0}%",
                metrics.buy_dominance * 100.0,
                config.min_buy_dominance * 100.0
            );
            warn!(mint = %self.mint, pool = %self.pool, dex = %self.dex, reason = %reason, "🚫 Filter rejected");
            return (false, reason);
        }

        // Filter 2c: Micro-buy spam (anti-bot)
        let (total_buys, small_buys, small_buy_ratio) =
            self.micro_buy_stats_at(config, Instant::now());
        let min_samples = config.min_unique_buyers.max(5);
        if total_buys >= min_samples && small_buy_ratio > config.small_buy_ratio_cap {
            FILTER_REJECTED_BUYER_QUALITY.fetch_add(1, Ordering::Relaxed);
            FILTER_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
            self.blacklisted = true;
            self.blacklist_reason = Some(format!(
                "REJECT_MICRO_BUY_SPAM: small-buy ratio {:.0}% > {:.0}% (small={}, total={}, min_trade={:.4} SOL)",
                small_buy_ratio * 100.0,
                config.small_buy_ratio_cap * 100.0,
                small_buys,
                total_buys,
                config.min_trade_size_lamports as f64 / 1_000_000_000.0
            ));
            return (
                false,
                self.blacklist_reason
                    .clone()
                    .unwrap_or_else(|| "REJECT_MICRO_BUY_SPAM".to_string()),
            );
        }

        // Filter 2b: Buyer Quality (anti-bot / concentration)
        let bq = self.buyer_quality_stats(config);

        if bq.top1_share > config.top1_buyer_share_cap {
            FILTER_REJECTED_BUYER_QUALITY.fetch_add(1, Ordering::Relaxed);
            FILTER_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
            self.blacklisted = true;
            self.blacklist_reason = Some(format!(
                "REJECT_BOT_CONCENTRATION: top1 share {:.0}% > {:.0}% (buyers={}, buy_vol={:.2} SOL)",
                bq.top1_share * 100.0,
                config.top1_buyer_share_cap * 100.0
                ,
                bq.unique_buyers,
                bq.total_buy_volume_lamports as f64 / 1_000_000_000.0
            ));
            return (
                false,
                self.blacklist_reason
                    .clone()
                    .unwrap_or_else(|| "Buyer concentration too high".to_string()),
            );
        }

        if bq.top3_share > config.top3_buyer_share_cap {
            FILTER_REJECTED_BUYER_QUALITY.fetch_add(1, Ordering::Relaxed);
            FILTER_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
            self.blacklisted = true;
            self.blacklist_reason = Some(format!(
                "REJECT_BOT_CONCENTRATION: top3 share {:.0}% > {:.0}% (buyers={}, buy_vol={:.2} SOL)",
                bq.top3_share * 100.0,
                config.top3_buyer_share_cap * 100.0
                ,
                bq.unique_buyers,
                bq.total_buy_volume_lamports as f64 / 1_000_000_000.0
            ));
            return (
                false,
                self.blacklist_reason
                    .clone()
                    .unwrap_or_else(|| "Buyer concentration too high".to_string()),
            );
        }

        if bq.repeat_buyer_ratio < config.repeat_buyer_min_ratio {
            FILTER_REJECTED_BUYER_QUALITY.fetch_add(1, Ordering::Relaxed);
            FILTER_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
            self.blacklisted = true;
            self.blacklist_reason = Some(format!(
                "REJECT_BOT_CONCENTRATION: repeat buyer ratio {:.0}% < {:.0}% (buyers={}, buy_vol={:.2} SOL)",
                bq.repeat_buyer_ratio * 100.0,
                config.repeat_buyer_min_ratio * 100.0
                ,
                bq.unique_buyers,
                bq.total_buy_volume_lamports as f64 / 1_000_000_000.0
            ));
            return (
                false,
                self.blacklist_reason
                    .clone()
                    .unwrap_or_else(|| "Repeat buyer ratio too low".to_string()),
            );
        }

        // Filter 3: SOL Inflow
        if metrics.net_sol_inflow < config.min_sol_inflow_lamports {
            FILTER_REJECTED_INFLOW.fetch_add(1, Ordering::Relaxed);
            FILTER_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
            let reason = format!(
                "WAIT_BUYER_WINDOW: net inflow {:.2} SOL < {:.2} SOL",
                metrics.net_sol_inflow as f64 / 1_000_000_000.0,
                config.min_sol_inflow_lamports as f64 / 1_000_000_000.0
            );
            warn!(mint = %self.mint, pool = %self.pool, dex = %self.dex, reason = %reason, "🚫 Filter rejected");
            return (false, reason);
        }

        // Filter 3b: Dump-recovery gating (WAIT until recovery confirms after a dump)
        if let Some(wait_reason) = self.dump_recovery_wait_reason(config, Instant::now()) {
            return (false, wait_reason);
        }

        // Filter 4: Dev Behavior (pre-entry)
        if metrics.dev_sold_early {
            if config.cto_enabled {
                if let Some(wait) = self.cto_wait_reason(config, Instant::now()) {
                    return (false, wait);
                }
            } else {
                FILTER_REJECTED_DEV_BEHAVIOR.fetch_add(1, Ordering::Relaxed);
                FILTER_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
                self.blacklisted = true;
                self.blacklist_reason = Some("REJECT_DEV_SELL_EARLY".to_string());
                return (false, "REJECT_DEV_SELL_EARLY".to_string());
            }
        }

        // All filters passed!
        FILTER_PASSED_TOTAL.fetch_add(1, Ordering::Relaxed);
        let reason = format!(
            "All filters passed: liq={:.1}SOL, buyers={}, vel={:.2}/s, dom={:.0}%, inflow={:.1}SOL, bq(top1={:.0}%,top3={:.0}%,repeat={:.0}%,vol={:.2}SOL)",
            metrics.initial_liquidity_sol,
            metrics.unique_buyers_in_window,
            metrics.trades_per_sec,
            metrics.buy_dominance * 100.0,
            metrics.net_sol_inflow as f64 / 1_000_000_000.0,
            bq.top1_share * 100.0,
            bq.top3_share * 100.0,
            bq.repeat_buyer_ratio * 100.0,
            bq.total_buy_volume_lamports as f64 / 1_000_000_000.0
        );

        (true, reason)
    }
}

/// Calculated metrics for logging/decisions
#[derive(Debug)]
struct TokenMetrics {
    unique_buyers_in_window: u32,
    trades_per_sec: f64,
    buy_dominance: f64,
    net_sol_inflow: u64,
    dev_sold_early: bool,
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

struct OpenPositionParams<'a> {
    mint: &'a str,
    pool: &'a str,
    dex: &'a str,
    entry_price: f64,
    token_decimals: u8,
    token_amount: u64,
    sol_invested: u64,
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
    /// DEX pool accounts can arrive before PoolCreated.
    /// Store by token mint and apply once the TokenTracker exists.
    pending_pool_accounts: parking_lot::RwLock<HashMap<String, PendingPoolAccounts>>,
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

    /// Returns true if the DEX requires DexPoolAccounts in Intent.resources.accounts
    /// for deterministic TX building (no RPC in hot path).
    ///
    /// DEXes that need accounts:
    /// - pump_amm/pumpswap: Always requires 14 accounts (pool, vaults, mints, etc.)
    /// - meteora_dlmm: Needs lb_pair, vaults, mints, active_id, bin_step, bitmap_extension
    /// - raydium: Needs AMM accounts (less critical - has LivePoolCache fallback)
    /// - orca: Needs whirlpool accounts (less critical - has LivePoolCache fallback)
    fn dex_requires_pool_accounts(dex: &str) -> bool {
        let dex_lower = dex.to_ascii_lowercase();
        matches!(
            dex_lower.as_str(),
            "pumpfunamm"
                | "pump_amm"
                | "pumpswap"
                | "pump-amm"
                | "meteora_dlmm"
                | "meteora-dlmm"
                | "meteoradlmm"
        )
    }

    fn try_get_dex_pool_accounts_for_mint(&self, mint: &str) -> Option<Vec<String>> {
        let trackers = self.token_trackers.read();
        trackers
            .get(mint)
            .and_then(|t| t.dex_pool_accounts.clone())
            .or_else(|| {
                let pending = self.pending_pool_accounts.read();
                pending.get(mint).map(|(_, _, a)| a.clone())
            })
    }

    fn record_dex_pool_accounts(
        &self,
        dex: &str,
        pool_address: &str,
        base_mint: &str,
        quote_mint: &str,
        accounts: &[String],
    ) {
        // Validate account format based on DEX type
        // - pump_amm/pumpswap: exactly 14 accounts
        // - meteora_dlmm: at least 3 (pool, token_x_mint, token_y_mint) + optional vaults + tagged values
        // - other DEXes: at least 3 accounts
        let dex_lower = dex.to_ascii_lowercase();
        let is_pump_amm = dex_lower == "pump_amm"
            || dex_lower == "pumpfunamm"
            || dex_lower == "pumpswap"
            || dex_lower == "pump-amm";

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
                (dex.to_string(), pool_address.to_string(), accounts.to_vec()),
            );
        }

        // Apply immediately if tracker exists.
        let mut trackers = self.token_trackers.write();
        if let Some(tracker) = trackers.get_mut(token_mint) {
            if tracker.pool != pool_address {
                debug!(
                    mint = %token_mint,
                    tracker_pool = %tracker.pool,
                    event_pool = %pool_address,
                    "DexPoolAccounts pool mismatch; keeping pending copy"
                );
                return;
            }
            tracker.dex_pool_accounts = Some(accounts.to_vec());
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
            if let Some((dev_wallet, supply_pct)) = self.pending_dev_info.read().get(mint).cloned()
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

            // Apply any DexPoolAccounts that arrived before the tracker existed.
            if let Some((dex_name, pool_addr, accounts)) =
                self.pending_pool_accounts.read().get(mint).cloned()
            {
                if let Some(tracker) = trackers.get_mut(mint) {
                    if tracker.pool == pool_addr {
                        tracker.dex = dex_name;
                        tracker.dex_pool_accounts = Some(accounts);
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
    fn check_for_signals(&self) -> Vec<EntrySignal> {
        // Returns entry intents (probe + optional scale-in)
        let config = self.config.read().clone();
        let mint_infos = self.mint_infos.read();
        let mut trackers = self.token_trackers.write();
        let mut signals = Vec::new();

        let probe_sol = ((config.default_position_lamports as f64) * config.probe_buy_pct)
            .round()
            .clamp(0.0, config.default_position_lamports as f64) as u64;
        let scale_sol = config.default_position_lamports.saturating_sub(probe_sol);

        for (mint, tracker) in trackers.iter_mut() {
            if tracker.blacklisted || tracker.intent_generated {
                continue;
            }

            let mint_info = mint_infos.get(mint);

            // 1) Probe-buy stage
            if tracker.probe_sent_at.is_none() {
                let was_blacklisted = tracker.blacklisted;
                let (should_trade, reason) = tracker.should_generate_intent(&config, mint_info);
                if !was_blacklisted && tracker.blacklisted {
                    self.tokens_blacklisted
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }

                if should_trade {
                    if probe_sol == 0 {
                        warn!(
                            mint = %mint,
                            pool = %tracker.pool,
                            dex = %tracker.dex,
                            default_position_lamports = config.default_position_lamports,
                            probe_buy_pct = config.probe_buy_pct,
                            "Entry signal suppressed: probe_sol rounds to 0; increase default_position_lamports or probe_buy_pct"
                        );
                        tracker.intent_generated = true;
                        continue;
                    }
                    tracker.probe_sent_at = Some(Instant::now());
                    signals.push(EntrySignal {
                        mint: mint.clone(),
                        pool: tracker.pool.clone(),
                        dex: tracker.dex.clone(),
                        sol_amount: probe_sol,
                        kind: EntryKind::Probe,
                        reason: format!("ENTER_PROBE_BUY: {reason}"),
                    });
                }
                continue;
            }

            // 2) Scale-in stage (only after probe fill, within confirm window)
            if tracker.probe_filled_at.is_some()
                && tracker.scale_sent_at.is_none()
                && tracker.scale_filled_at.is_none()
            {
                let now = Instant::now();
                let probe_filled_at = tracker.probe_filled_at.unwrap_or(now);
                if now.duration_since(probe_filled_at).as_secs()
                    > config.scale_in_confirm_window_secs
                {
                    // Confirmation window expired: keep probe position only.
                    tracker.intent_generated = true;
                    continue;
                }

                let was_blacklisted = tracker.blacklisted;
                let (should_trade, reason) = tracker.should_generate_intent(&config, mint_info);
                if !was_blacklisted && tracker.blacklisted {
                    self.tokens_blacklisted
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }

                if should_trade {
                    if scale_sol == 0 {
                        warn!(
                            mint = %mint,
                            pool = %tracker.pool,
                            dex = %tracker.dex,
                            default_position_lamports = config.default_position_lamports,
                            probe_buy_pct = config.probe_buy_pct,
                            "Scale-in suppressed: scale_sol is 0 (after probe rounding); increase default_position_lamports or adjust probe_buy_pct"
                        );
                        tracker.intent_generated = true;
                        continue;
                    }
                    tracker.scale_sent_at = Some(now);
                    signals.push(EntrySignal {
                        mint: mint.clone(),
                        pool: tracker.pool.clone(),
                        dex: tracker.dex.clone(),
                        sol_amount: scale_sol,
                        kind: EntryKind::ScaleIn,
                        reason: format!("ENTER_SCALE_IN: {reason}"),
                    });
                }
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
    fn open_position(&self, p: OpenPositionParams<'_>) {
        let mut positions = self.positions.write();
        if let Some(pos) = positions.get_mut(p.mint) {
            pos.token_amount = pos.token_amount.saturating_add(p.token_amount);
            pos.add_investment(p.sol_invested);
            // Keep the best-known decimals (prefer non-zero).
            if pos.token_decimals == 0 && p.token_decimals != 0 {
                pos.token_decimals = p.token_decimals;
            }
            info!(
                mint = %p.mint,
                additional_sol = p.sol_invested,
                additional_tokens_raw = p.token_amount,
                total_sol = pos.sol_invested,
                total_tokens_raw = pos.token_amount,
                "📈 Position scaled in"
            );
            return;
        }
        positions.insert(
            p.mint.to_string(),
            PositionTracker::new(
                p.mint,
                p.pool,
                p.dex,
                p.entry_price,
                p.token_decimals,
                p.token_amount,
                p.sol_invested,
            ),
        );
        info!(
            mint = %p.mint,
            entry_price = p.entry_price,
            sol_invested = p.sol_invested,
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
                mint = %pos.mint,
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
        let now = Instant::now();

        // Snapshot tracker-derived hard-exit signals BEFORE locking positions,
        // to avoid lock-order deadlocks (market event handling locks trackers then positions).
        #[derive(Clone, Debug)]
        struct TrackerExitSignals {
            lp_removed_at: Option<Instant>,
            dev_sold_at: Option<Instant>,
            dev_sold_sig: Option<String>,
            dev_sold_sol: Option<u64>,
        }

        let tracker_signals: HashMap<String, TrackerExitSignals> = {
            let trackers = self.token_trackers.read();
            trackers
                .iter()
                .map(|(mint, t)| {
                    let mut dev_sold_at: Option<Instant> = None;
                    let mut dev_sold_sig: Option<String> = None;
                    let mut dev_sold_sol: Option<u64> = None;

                    if let Some(dev) = t.dev_wallet.as_ref() {
                        if let Some(last) = t
                            .trades
                            .iter()
                            .rev()
                            .find(|tr| !tr.is_buy && tr.trader == *dev)
                        {
                            dev_sold_at = Some(last.timestamp);
                            dev_sold_sig = Some(last.signature.clone());
                            dev_sold_sol = Some(last.sol_amount);
                        }
                    }

                    (
                        mint.clone(),
                        TrackerExitSignals {
                            lp_removed_at: t.lp_removal_time,
                            dev_sold_at,
                            dev_sold_sig,
                            dev_sold_sol,
                        },
                    )
                })
                .collect()
        };

        let mut positions = self.positions.write();
        let mut exits = Vec::new();

        for (mint, pos) in positions.iter_mut() {
            if pos.exit_generated {
                continue;
            }

            // Hard exits (post-entry): dev sells, LP removed.
            if let Some(sig) = tracker_signals.get(mint) {
                if let Some(lp_at) = sig.lp_removed_at {
                    let within_window = config.lp_removal_window_secs == 0
                        || now.duration_since(lp_at)
                            <= Duration::from_secs(config.lp_removal_window_secs);

                    if lp_at > pos.entry_time && within_window {
                        pos.exit_generated = true;
                        exits.push((
                            mint.clone(),
                            pos.pool.clone(),
                            pos.dex.clone(),
                            "LP_REMOVAL".to_string(),
                            "LP removed post-entry".to_string(),
                            pos.token_amount,
                        ));
                        continue;
                    }
                }

                if let Some(dev_at) = sig.dev_sold_at {
                    if dev_at > pos.entry_time {
                        let sig_s = sig.dev_sold_sig.as_deref().unwrap_or("<unknown>");
                        let sol = sig.dev_sold_sol.unwrap_or(0);
                        pos.exit_generated = true;
                        exits.push((
                            mint.clone(),
                            pos.pool.clone(),
                            pos.dex.clone(),
                            "DEV_SELL".to_string(),
                            format!(
                                "Dev sold post-entry (sig={}, sol={:.4})",
                                sig_s,
                                sol as f64 / 1_000_000_000.0
                            ),
                            pos.token_amount,
                        ));
                        continue;
                    }
                }
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

    /// Handle execution result from execution-engine
    fn handle_execution_result(&self, result: &ExecutionResult) {
        // Find the pending intent by id (source is not authoritative).
        let pending_opt = {
            let mut pending = self.pending_intents.write();
            pending.remove(&result.intent_id)
        };

        let Some(pending) = pending_opt else {
            debug!(intent_id = %result.intent_id, "No pending intent found for execution result");
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
                            if let Some(tr) = trackers.get_mut(&pending.mint) {
                                match pending.entry_kind {
                                    Some(EntryKind::ScaleIn) => {
                                        tr.scale_filled_at = Some(Instant::now());
                                        tr.intent_generated = true;
                                    }
                                    _ => {
                                        tr.blacklisted = true;
                                        tr.blacklist_reason =
                                            Some("buy_confirmed_missing_fill_out".to_string());
                                    }
                                }
                            }

                            return;
                        };

                        // Fill-in may be missing (e.g., SOL lamport delta gated due to rent noise).
                        // Fall back to intended SOL spend from the pending intent.
                        let sol_invested_raw = result
                            .fill_in
                            .as_ref()
                            .map(|a| a.raw)
                            .unwrap_or(pending.sol_amount);

                        // Prefer decimals from market-data TokenMintInfo (Geyser), fall back to fill_out decimals.
                        let token_decimals = self
                            .mint_infos
                            .read()
                            .get(&pending.mint)
                            .map(|m| m.decimals)
                            .unwrap_or(fill_out.decimals);

                        let sol_ui = result
                            .fill_in
                            .as_ref()
                            .map(|a| a.as_f64())
                            .unwrap_or(sol_invested_raw as f64 / 1_000_000_000.0)
                            .max(0.0);
                        let tok_ui = fill_out.as_f64().max(0.0);
                        let entry_price = if sol_ui > 0.0 { tok_ui / sol_ui } else { 1.0 };

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
                            if let Some(tr) = trackers.get_mut(&pending.mint) {
                                match pending.entry_kind {
                                    Some(EntryKind::Probe) => {
                                        tr.probe_filled_at = Some(Instant::now());
                                    }
                                    Some(EntryKind::ScaleIn) => {
                                        tr.scale_filled_at = Some(Instant::now());
                                        tr.intent_generated = true;
                                    }
                                    None => {}
                                }
                            }
                        }

                        self.open_position(OpenPositionParams {
                            mint: &pending.mint,
                            pool: &pending.pool,
                            dex: &pending.dex,
                            entry_price,
                            token_decimals,
                            token_amount,
                            sol_invested,
                        });
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
                // Don't open position on failure. For entry intents, stop retry-spam.
                if pending.side == TradeSide::Buy {
                    let mut trackers = self.token_trackers.write();
                    if let Some(tr) = trackers.get_mut(&pending.mint) {
                        tr.blacklisted = true;
                        tr.blacklist_reason = Some(format!(
                            "Entry execution failed ({:?})",
                            pending.entry_kind.unwrap_or(EntryKind::Probe)
                        ));
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
                    let mut trackers = self.token_trackers.write();
                    if let Some(tr) = trackers.get_mut(&pending.mint) {
                        tr.blacklisted = true;
                        tr.blacklist_reason = Some(format!(
                            "Entry execution timeout ({:?})",
                            pending.entry_kind.unwrap_or(EntryKind::Probe)
                        ));
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
                "min_trades_per_sec" => {
                    if let Some(v) = value.as_f64() {
                        if v >= 0.0 {
                            config.min_trades_per_sec = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
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

                // === Filter 4: Dev Behavior ===
                "dev_early_sell_window_secs" => {
                    if let Some(v) = value.as_u64() {
                        if v > 0 {
                            config.dev_early_sell_window_secs = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be > 0".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "dev_rebuy_positive" => {
                    if let Some(v) = value.as_bool() {
                        config.dev_rebuy_positive = v;
                        applied.push(key.clone());
                        info!(key = %key, new_value = %v, "Config updated");
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected bool".to_string()));
                    }
                }

                // === Exit Strategy ===
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

/// Recover open positions from execution_results JSONL after restart
///
/// P0: Critical for preventing "forgotten" tokens that never get sold.
/// Reads recent execution_results (last 7 days) and reconstructs PositionTracker
/// for any confirmed BUYs without matching SELLs.
async fn recover_positions_from_jsonl(
    log_dir: &PathBuf,
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
                for line in reader.lines() {
                    if let Ok(line) = line {
                        if let Ok(intent) = serde_json::from_str::<serde_json::Value>(&line) {
                            if let Some(intent_id) = intent.get("intent_id").and_then(|v| v.as_str()) {
                                intent_lookup.insert(intent_id.to_string(), intent);
                            }
                        }
                    }
                }
            }
        }
        info!(cached_intents = intent_lookup.len(), "Built intent lookup for position recovery");
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

            // Only process momentum-bot executions
            if exec.source != "momentum-bot" {
                continue;
            }

            // Only process confirmed executions
            if exec.status != ExecutionStatus::Confirmed {
                continue;
            }

            // Get token_mint (from new schema or fallback to intent lookup)
            let mint = if let Some(ref m) = exec.token_mint {
                m.clone()
            } else {
                // Old schema - try to get mint from intent
                let mint_from_intent = if let Some(intent) = intent_lookup.get(&exec.intent_id) {
                    // Determine BUY vs SELL from fill amounts first
                    let has_fill_out = exec.fill_out.is_some() && exec.fill_out.as_ref().unwrap().raw > 0;
                    
                    if has_fill_out {
                        // BUY: output_mint is the token
                        intent
                            .get("resources")
                            .and_then(|r| r.get("output_mint"))
                            .and_then(|m| m.as_str())
                            .map(|s| s.to_string())
                    } else {
                        // SELL: input_mint is the token
                        intent
                            .get("resources")
                            .and_then(|r| r.get("input_mint"))
                            .and_then(|m| m.as_str())
                            .map(|s| s.to_string())
                    }
                } else {
                    None
                };
                
                match mint_from_intent {
                    Some(m) => m,
                    None => continue, // Skip if we can't determine mint
                }
            };

            // Determine BUY vs SELL from fill amounts
            let is_buy = exec.fill_out.is_some() && exec.fill_out.as_ref().unwrap().raw > 0;
            let is_sell = exec.fill_in.is_some() && exec.fill_in.as_ref().unwrap().raw > 100_000;

            if is_sell {
                // SELL confirmed - mark this mint as closed
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
        // Take the most recent BUY for this mint
        if let Some(exec) = execs.last() {
            let fill_out = match exec.fill_out.as_ref() {
                Some(f) => f,
                None => {
                    warn!(mint = %mint, "BUY execution missing fill_out, skipping");
                    continue;
                }
            };

            let token_amount = fill_out.raw;
            let token_decimals = fill_out.decimals;

            // Best-effort: estimate SOL invested and entry price
            let sol_invested = exec
                .fill_in
                .as_ref()
                .map(|f| f.raw)
                .or_else(|| exec.wallet_sol_delta_lamports.map(|d| d.abs() as u64))
                .unwrap_or(1_000_000_000); // Fallback: 1 SOL

            let sol_ui = sol_invested as f64 / 1e9;
            let tok_ui = token_amount as f64 / 10f64.powi(token_decimals as i32);
            let entry_price = if sol_ui > 0.0 { tok_ui / sol_ui } else { 1.0 };

            // Estimate entry time from timestamp (ms)
            let entry_time_estimate = chrono::DateTime::from_timestamp_millis(exec.header.ts_unix_ms as i64)
                .map(|dt| {
                    let elapsed = chrono::Utc::now().signed_duration_since(dt);
                    Instant::now() - Duration::from_secs(elapsed.num_seconds().max(0) as u64)
                })
                .unwrap_or_else(Instant::now);

            // We don't have pool/dex from execution_results (only mint)
            // Use placeholder values - exits will still work based on time/price
            let pool = format!("unknown_pool_{}", &mint[..8]);
            let dex = "unknown".to_string();

            let mut tracker = PositionTracker::new(
                mint,
                &pool,
                &dex,
                entry_price,
                token_decimals,
                token_amount,
                sol_invested,
            );

            // Override entry_time to match actual trade time
            tracker.entry_time = entry_time_estimate;
            tracker.current_price = entry_price; // Will update from market events

            info!(
                mint = %mint,
                token_amount_ui = %tok_ui,
                entry_price = %entry_price,
                sol_invested_ui = %sol_ui,
                age_secs = %(Instant::now() - entry_time_estimate).as_secs(),
                "🔄 Position recovered from JSONL"
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

    // === P0: Recover open positions from execution_results JSONL ===
    // Critical: Prevents "forgotten" tokens after restarts
    let recovered_positions = match recover_positions_from_jsonl(&log_dir).await {
        Ok(positions) => positions,
        Err(e) => {
            warn!(error = %e, "Failed to recover positions from JSONL, starting with empty state");
            HashMap::new()
        }
    };

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
        pending_pool_accounts: parking_lot::RwLock::new(HashMap::new()),
        mint_infos: parking_lot::RwLock::new(HashMap::new()),
        token_trackers: parking_lot::RwLock::new(HashMap::new()),
        positions: parking_lot::RwLock::new(recovered_positions),
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
    let mut strategy_interval = tokio::time::interval(std::time::Duration::from_secs(2));
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
                            if update.target_component == "momentum-bot" {
                                info!(
                                    component = %update.target_component,
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
                                debug!(component = %update.target_component, "Ignoring config update for other component");
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

            // Strategy evaluation tick (entry + exits) - frequent enough for probe/scale windows.
            _ = strategy_interval.tick() => {
                ironcrab::metrics::record_activity();

                // === Check for ENTRY signals ===
                let signals = ctx.check_for_signals();
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

                    if let Err(e) = generate_and_publish_exit_intent(&ctx, &mint, &pool, &dex, &exit_type, &reason, token_amount).await {
                        error!(error = %e, mint = %mint, "Failed to generate/publish sell intent");
                    } else {
                        ctx.exits_generated.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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

/// Generate and publish a BUY TradeIntent based on an entry signal.
async fn generate_and_publish_buy_intent(
    ctx: &MomentumContext,
    signal: &EntrySignal,
) -> Result<()> {
    let max_slippage = { ctx.config.read().early_max_slippage_bps };

    // Assume SOL (So11111...) as quote mint for PumpFun/meme tokens
    let sol_mint = "So11111111111111111111111111111111111111112";

    let intent_id = ctx.next_intent_id();

    let (creator_opt, last_trade_ratio_opt) = {
        let trackers = ctx.token_trackers.read();
        let tracker = trackers.get(&signal.mint);
        (
            tracker.and_then(|t| t.dev_wallet.clone()),
            tracker.and_then(|t| t.last_trade_ratio()),
        )
    };

    let token_decimals_opt = {
        let mint_infos = ctx.mint_infos.read();
        mint_infos.get(&signal.mint).map(|m| m.decimals)
    };

    // Fallback: pump.fun/pumpfun/pump_amm tokens ALWAYS have 6 decimals
    let is_pump_dex = matches!(
        signal.dex.to_lowercase().as_str(),
        "pumpfun" | "pump_amm" | "pump.fun"
    );

    let token_decimals = match token_decimals_opt {
        Some(d) => d,
        None if is_pump_dex => {
            // Pump.fun tokens always have 6 decimals - safe fallback
            info!(
                mint = %signal.mint,
                dex = %signal.dex,
                "Using pump.fun fallback decimals=6 (TokenMintInfo not received)"
            );
            6
        }
        None => {
            // Roll back stage markers so we can try again when mint decimals arrive.
            {
                let mut trackers = ctx.token_trackers.write();
                if let Some(tr) = trackers.get_mut(&signal.mint) {
                    match signal.kind {
                        EntryKind::Probe => tr.probe_sent_at = None,
                        EntryKind::ScaleIn => tr.scale_sent_at = None,
                    }
                }
            }
            warn!(
                mint = %signal.mint,
                pool = %signal.pool,
                dex = %signal.dex,
                reason = %signal.reason,
                "Skipping BUY intent: missing TokenMintInfo.decimals"
            );
            anyhow::bail!("cannot generate intent: missing TokenMintInfo.decimals")
        }
    };

    let dex_pool_accounts_opt = ctx.try_get_dex_pool_accounts_for_mint(&signal.mint);
    let dex_requires_accounts = MomentumContext::dex_requires_pool_accounts(&signal.dex);
    let dex_accounts: Vec<String> = if dex_requires_accounts {
        let Some(accounts) = dex_pool_accounts_opt else {
            // Roll back stage markers so we can try again when DexPoolAccounts arrives.
            {
                let mut trackers = ctx.token_trackers.write();
                if let Some(tr) = trackers.get_mut(&signal.mint) {
                    match signal.kind {
                        EntryKind::Probe => tr.probe_sent_at = None,
                        EntryKind::ScaleIn => tr.scale_sent_at = None,
                    }
                }
            }
            warn!(
                mint = %signal.mint,
                pool = %signal.pool,
                dex = %signal.dex,
                "Skipping BUY intent: missing DexPoolAccounts for deterministic build"
            );
            anyhow::bail!("cannot generate intent: missing DexPoolAccounts")
        };

        // Validate accounts[0] matches the pool address
        if accounts.first().map(|s| s.as_str()) != Some(signal.pool.as_str()) {
            warn!(
                mint = %signal.mint,
                pool = %signal.pool,
                dex = %signal.dex,
                first = ?accounts.first(),
                "Skipping BUY intent: invalid DexPoolAccounts (accounts[0] != pool)"
            );
            anyhow::bail!("cannot generate intent: invalid DexPoolAccounts")
        }

        // Validate minimum account count based on DEX
        // - pump_amm: exactly 14 accounts
        // - meteora_dlmm: at least 3 (pool, mints) + optional tagged values
        let dex_lower = signal.dex.to_ascii_lowercase();
        let is_pump_amm = dex_lower == "pump_amm"
            || dex_lower == "pumpfunamm"
            || dex_lower == "pumpswap"
            || dex_lower == "pump-amm";

        if is_pump_amm && accounts.len() != 14 {
            warn!(
                mint = %signal.mint,
                pool = %signal.pool,
                dex = %signal.dex,
                accounts_len = accounts.len(),
                "Skipping BUY intent: pump_amm requires exactly 14 accounts"
            );
            anyhow::bail!("cannot generate intent: invalid DexPoolAccounts")
        }

        if accounts.len() < 3 {
            warn!(
                mint = %signal.mint,
                pool = %signal.pool,
                dex = %signal.dex,
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
            // Roll back stage markers so we can try again when a usable trade ratio arrives.
            {
                let mut trackers = ctx.token_trackers.write();
                if let Some(tr) = trackers.get_mut(&signal.mint) {
                    match signal.kind {
                        EntryKind::Probe => tr.probe_sent_at = None,
                        EntryKind::ScaleIn => tr.scale_sent_at = None,
                    }
                }
            }
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
        intent_id.clone(),
        "momentum-bot",
        IntentTier::Tier1,
        IntentOrigin::StrategyA,
        ExplicitAmount::new(signal.sol_amount, 9),
        TradeResources {
            input_mint: sol_mint.to_string(),
            output_mint: signal.mint.to_string(),
            pools: vec![signal.pool.to_string()],
            accounts: dex_accounts,
            token_program: None, // Momentum-bot doesn't need Token-2022 support yet
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
        .insert("dex".to_string(), signal.dex.to_string());
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

    // Pump.fun and PumpSwap tx building require the creator/dev wallet.
    if signal.dex == "pumpfun"
        || signal.dex.eq_ignore_ascii_case("pump_amm")
        || signal.dex.eq_ignore_ascii_case("pumpswap")
        || signal.dex.eq_ignore_ascii_case("PumpFunAmm")
    {
        let creator = creator_opt.ok_or_else(|| {
            anyhow::anyhow!(
                "cannot generate {} intent: missing dev_wallet/creator",
                signal.dex
            )
        })?;
        intent.metadata.insert("creator".to_string(), creator);
    }

    // Register pending intent BEFORE publishing
    ctx.register_buy_intent(
        &intent_id,
        &signal.mint,
        &signal.pool,
        &signal.dex,
        signal.sol_amount,
        Some(signal.kind),
    );

    info!(
        intent_id = %intent.intent_id,
        pool = %signal.pool,
        mint = %signal.mint,
        dex = %signal.dex,
        kind = ?signal.kind,
        sol_amount = signal.sol_amount,
        reason = %signal.reason,
        "🚀 Generated BUY TradeIntent"
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

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;
    use tempfile::TempDir;

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
                trader: "A".to_string(),
                is_buy: true,
                sol_amount: 50,
                token_amount: 1,
                signature: "s1".to_string(),
            },
            TradeEvent {
                timestamp: now - Duration::from_secs(2),
                trader: "A".to_string(),
                is_buy: true,
                sol_amount: 30,
                token_amount: 1,
                signature: "s2".to_string(),
            },
            TradeEvent {
                timestamp: now - Duration::from_secs(3),
                trader: "A".to_string(),
                is_buy: true,
                sol_amount: 20,
                token_amount: 1,
                signature: "s3".to_string(),
            },
            // Wallet B: 1 buy 50
            TradeEvent {
                timestamp: now - Duration::from_secs(4),
                trader: "B".to_string(),
                is_buy: true,
                sol_amount: 50,
                token_amount: 1,
                signature: "s4".to_string(),
            },
            // Wallet C: 1 buy 50
            TradeEvent {
                timestamp: now - Duration::from_secs(5),
                trader: "C".to_string(),
                is_buy: true,
                sol_amount: 50,
                token_amount: 1,
                signature: "s5".to_string(),
            },
            // Wallet D: 1 sell (ignored)
            TradeEvent {
                timestamp: now - Duration::from_secs(6),
                trader: "D".to_string(),
                is_buy: false,
                sol_amount: 999,
                token_amount: 1,
                signature: "s6".to_string(),
            },
        ];

        let bq = tracker.buyer_quality_stats_at(&cfg, now);
        assert_eq!(bq.unique_buyers, 3);
        assert_eq!(bq.total_buy_volume_lamports, 200);

        // A=100, B=50, C=50 => top1=50%, top3=100%
        assert!((bq.top1_share - 0.5).abs() < 1e-9);
        assert!((bq.top3_share - 1.0).abs() < 1e-9);

        // Repeat buyers: A only => 1/3
        assert!((bq.repeat_buyer_ratio - (1.0 / 3.0)).abs() < 1e-9);
    }

    #[test]
    fn micro_buy_spam_rejects_when_ratio_too_high() {
        let mut cfg = MomentumConfig::default();
        cfg.buyer_window_secs = 60;
        cfg.inflow_window_secs = 60;

        // Disable other gates to isolate micro-buy behavior.
        cfg.early_min_liquidity_sol = 0.0;
        cfg.min_unique_buyers = 0;
        cfg.min_trades_per_sec = 0.0;
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

        let mut tracker = TokenTracker::new("mint", "pool", "dex", 1, 0);

        // 10 buys: 7 are micro (<100), 3 are normal => ratio 0.7 > 0.6
        for i in 0..7 {
            let trader = format!("w{i}");
            let sig = format!("s{i}");
            tracker.record_trade(trader.as_str(), true, 50, 1, sig.as_str(), &cfg);
        }
        for i in 7..10 {
            let trader = format!("w{i}");
            let sig = format!("s{i}");
            tracker.record_trade(trader.as_str(), true, 200, 1, sig.as_str(), &cfg);
        }

        let (should_trade, reason) = tracker.should_generate_intent(&cfg, None);
        assert!(!should_trade);
        assert!(reason.contains("REJECT_MICRO_BUY_SPAM"));
    }

    #[test]
    fn probe_then_scale_signal_flow() {
        let mut cfg = MomentumConfig::default();

        // Make the entry sizing deterministic.
        cfg.default_position_lamports = 1_000;
        cfg.probe_buy_pct = 0.25;
        cfg.scale_in_confirm_window_secs = 30;

        // Disable other gates to isolate probe/scale state machine.
        cfg.early_min_liquidity_sol = 0.0;
        cfg.min_unique_buyers = 0;
        cfg.min_trades_per_sec = 0.0;
        cfg.min_buy_dominance = 0.0;
        cfg.min_sol_inflow_lamports = 0;
        cfg.require_mint_authority_renounced = false;
        cfg.require_freeze_authority_none = false;
        cfg.top1_buyer_share_cap = 1.0;
        cfg.top3_buyer_share_cap = 1.0;
        cfg.repeat_buyer_min_ratio = 0.0;
        cfg.min_trade_size_lamports = 0;
        cfg.small_buy_ratio_cap = 1.0;

        let tmp = TempDir::new().expect("tempdir");
        let jsonl_config = JsonlWriterConfig::new("trade_intents").with_log_dir(tmp.path());
        let jsonl_writer = JsonlWriter::new(jsonl_config).expect("jsonl writer");

        let ctx = MomentumContext {
            run_id: "12345678".to_string(),
            config: parking_lot::RwLock::new(cfg),
            nats: None,
            jsonl_writer,
            intent_counter: std::sync::atomic::AtomicU64::new(0),
            pool_first_seen: parking_lot::RwLock::new(std::collections::HashMap::new()),
            pending_dev_info: parking_lot::RwLock::new(HashMap::new()),
            pending_pool_accounts: parking_lot::RwLock::new(HashMap::new()),
            mint_infos: parking_lot::RwLock::new(HashMap::new()),
            token_trackers: parking_lot::RwLock::new(HashMap::new()),
            positions: parking_lot::RwLock::new(HashMap::new()),
            pending_intents: parking_lot::RwLock::new(HashMap::new()),
            tokens_tracked: std::sync::atomic::AtomicU64::new(0),
            tokens_blacklisted: std::sync::atomic::AtomicU64::new(0),
            intents_generated: std::sync::atomic::AtomicU64::new(0),
            exits_generated: std::sync::atomic::AtomicU64::new(0),
        };

        // Insert a fresh tracker.
        {
            let mut trackers = ctx.token_trackers.write();
            trackers.insert(
                "mint".to_string(),
                TokenTracker::new("mint", "pool", "dex", 1, 0),
            );
        }

        // First pass should emit a probe signal.
        let signals = ctx.check_for_signals();
        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].mint, "mint");
        assert_eq!(signals[0].kind, EntryKind::Probe);
        assert_eq!(signals[0].sol_amount, 250);

        // Mark probe as filled.
        {
            let mut trackers = ctx.token_trackers.write();
            let t = trackers.get_mut("mint").unwrap();
            assert!(t.probe_sent_at.is_some());
            t.probe_filled_at = Some(Instant::now());
        }

        // Second pass should emit scale-in signal.
        let signals = ctx.check_for_signals();
        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].kind, EntryKind::ScaleIn);
        assert_eq!(signals[0].sol_amount, 750);

        {
            let trackers = ctx.token_trackers.read();
            let t = trackers.get("mint").unwrap();
            assert!(t.scale_sent_at.is_some());
        }
    }

    #[test]
    fn dump_recovery_waits_then_allows_after_stabilization() {
        let mut cfg = MomentumConfig::default();

        // Disable other gates to isolate dump recovery.
        cfg.early_min_liquidity_sol = 0.0;
        cfg.min_unique_buyers = 0;
        cfg.min_trades_per_sec = 0.0;
        cfg.min_buy_dominance = 0.0;
        cfg.min_sol_inflow_lamports = 0;
        cfg.require_mint_authority_renounced = false;
        cfg.require_freeze_authority_none = false;
        cfg.top1_buyer_share_cap = 1.0;
        cfg.top3_buyer_share_cap = 1.0;
        cfg.repeat_buyer_min_ratio = 0.0;
        cfg.min_trade_size_lamports = 0;
        cfg.small_buy_ratio_cap = 1.0;

        // Dump recovery params
        cfg.dump_recovery_window_secs = 30;
        cfg.dump_recovery_min_buy_dominance = 0.60;
        cfg.dump_recovery_min_net_inflow_lamports = 100;
        cfg.dump_recovery_min_recovery_secs = 10;

        let now = Instant::now();
        let mut tracker = TokenTracker::new("mint", "pool", "dex", 1, 0);

        // Create a dump: mostly sells in the window.
        for i in 0..5 {
            tracker.record_trade("w", false, 200, 1, &format!("sd{i}"), &cfg);
        }
        for (i, t) in tracker.trades.iter_mut().enumerate() {
            t.timestamp = now - Duration::from_secs(5 + i as u64);
        }

        let (should_trade, reason) = tracker.should_generate_intent(&cfg, None);
        assert!(!should_trade);
        assert!(reason.contains("WAIT_CONFIRMATION"));

        // Add recovery: net inflow positive + buy dominance above threshold.
        // 8 buys vs 5 sells => dominance ~61.5%.
        for i in 0..8 {
            tracker.record_trade("w", true, 300, 1, &format!("rb{i}"), &cfg);
        }
        // Make recovery trades within the window.
        for (i, t) in tracker.trades.iter_mut().enumerate() {
            t.timestamp = now - Duration::from_secs(1 + (i as u64 % 10));
        }

        // First evaluation should still WAIT because min_recovery_secs hasn't elapsed.
        let (should_trade, reason) = tracker.should_generate_intent(&cfg, None);
        assert!(!should_trade);
        assert!(reason.contains("WAIT_CONFIRMATION"));

        // Force stabilization window to be satisfied.
        tracker.recovery_started_at = Some(now - Duration::from_secs(11));
        let (should_trade, _reason) = tracker.should_generate_intent(&cfg, None);
        assert!(should_trade);
    }

    #[test]
    fn cto_disabled_rejects_dev_sell_early() {
        let mut cfg = MomentumConfig::default();

        // Disable other gates to isolate CTO behavior.
        cfg.early_min_liquidity_sol = 0.0;
        cfg.min_unique_buyers = 0;
        cfg.min_trades_per_sec = 0.0;
        cfg.min_buy_dominance = 0.0;
        cfg.min_sol_inflow_lamports = 0;
        cfg.require_mint_authority_renounced = false;
        cfg.require_freeze_authority_none = false;
        cfg.top1_buyer_share_cap = 1.0;
        cfg.top3_buyer_share_cap = 1.0;
        cfg.repeat_buyer_min_ratio = 0.0;
        cfg.min_trade_size_lamports = 0;
        cfg.small_buy_ratio_cap = 1.0;

        cfg.cto_enabled = false;
        cfg.dev_early_sell_window_secs = 999;

        let mut tracker = TokenTracker::new("mint", "pool", "dex", 1, 0);
        tracker.dev_wallet = Some("dev".to_string());

        // Dev sells early.
        tracker.record_trade("dev", false, 1_000, 1, "sig", &cfg);
        assert!(tracker.dev_sold_early);

        let (should_trade, reason) = tracker.should_generate_intent(&cfg, None);
        assert!(!should_trade);
        assert_eq!(reason, "REJECT_DEV_SELL_EARLY");
        assert!(tracker.blacklisted);
    }

    #[test]
    fn cto_enabled_waits_then_allows_after_recovery_confirm() {
        let mut cfg = MomentumConfig::default();

        // Disable other gates to isolate CTO behavior.
        cfg.early_min_liquidity_sol = 0.0;
        cfg.min_unique_buyers = 0;
        cfg.min_trades_per_sec = 0.0;
        cfg.min_buy_dominance = 0.0;
        cfg.min_sol_inflow_lamports = 0;
        cfg.require_mint_authority_renounced = false;
        cfg.require_freeze_authority_none = false;
        cfg.top1_buyer_share_cap = 1.0;
        cfg.top3_buyer_share_cap = 1.0;
        cfg.repeat_buyer_min_ratio = 0.0;
        cfg.min_trade_size_lamports = 0;
        cfg.small_buy_ratio_cap = 1.0;

        cfg.cto_enabled = true;
        cfg.cto_entry_delay_secs = 10;
        cfg.cto_confirm_window_secs = 30;
        cfg.cto_min_unique_buyers = 2;
        cfg.cto_min_buy_dominance = 0.60;
        cfg.cto_min_net_inflow_lamports = 100;
        cfg.dev_early_sell_window_secs = 999;

        // Disable dump-recovery gate to isolate CTO behavior.
        cfg.dump_recovery_window_secs = 0;

        let now = Instant::now();
        let mut tracker = TokenTracker::new("mint", "pool", "dex", 1, 0);
        tracker.dev_wallet = Some("dev".to_string());

        // Dev sells early -> CTO candidate.
        tracker.record_trade("dev", false, 200, 1, "sd", &cfg);
        assert!(tracker.dev_sold_early);

        // Before delay: WAIT.
        tracker.cto_started_at = Some(now);
        let (should_trade, reason) = tracker.should_generate_intent(&cfg, None);
        assert!(!should_trade);
        assert!(reason.contains("CTO_WAIT_RECOVERY"));

        // After delay, but without confirmation trades: still WAIT.
        tracker.cto_started_at = Some(now - Duration::from_secs(11));
        let (should_trade, reason) = tracker.should_generate_intent(&cfg, None);
        assert!(!should_trade);
        assert!(reason.contains("CTO_WAIT_RECOVERY"));

        // Add recovery trades in confirm window: include the initial dev sell as well.
        // With 6 sells total (dev + 5), we need >=9 buys to hit >=60% buy dominance.
        for i in 0..5 {
            tracker.record_trade("w", false, 50, 1, &format!("s{i}"), &cfg);
        }
        for i in 0..9 {
            let buyer = format!("b{i}");
            tracker.record_trade(&buyer, true, 120, 1, &format!("b{i}"), &cfg);
        }

        // Force all trades into confirm window.
        for (i, t) in tracker.trades.iter_mut().enumerate() {
            t.timestamp = now - Duration::from_secs(1 + (i as u64 % 10));
        }

        let (should_trade, _reason) = tracker.should_generate_intent(&cfg, None);
        assert!(should_trade);
        assert!(tracker.cto_recovery_confirmed);
    }

    #[test]
    fn emitted_buy_intent_includes_reason_metadata_and_stable_source() {
        let mut cfg = MomentumConfig::default();

        // Disable all entry gates so we can emit an intent deterministically.
        cfg.early_min_liquidity_sol = 0.0;
        cfg.min_unique_buyers = 0;
        cfg.min_trades_per_sec = 0.0;
        cfg.min_buy_dominance = 0.0;
        cfg.min_sol_inflow_lamports = 0;
        cfg.require_mint_authority_renounced = false;
        cfg.require_freeze_authority_none = false;
        cfg.top1_buyer_share_cap = 1.0;
        cfg.top3_buyer_share_cap = 1.0;
        cfg.repeat_buyer_min_ratio = 0.0;
        cfg.min_trade_size_lamports = 0;
        cfg.small_buy_ratio_cap = 1.0;

        cfg.default_position_lamports = 1_000;

        let tmp = TempDir::new().expect("tempdir");
        let jsonl_config = JsonlWriterConfig::new("trade_intents").with_log_dir(tmp.path());
        let jsonl_writer = JsonlWriter::new(jsonl_config).expect("jsonl writer");

        let ctx = MomentumContext {
            run_id: "run-test".to_string(),
            config: parking_lot::RwLock::new(cfg.clone()),
            nats: None,
            jsonl_writer,
            intent_counter: std::sync::atomic::AtomicU64::new(0),
            pool_first_seen: parking_lot::RwLock::new(std::collections::HashMap::new()),
            pending_dev_info: parking_lot::RwLock::new(HashMap::new()),
            pending_pool_accounts: parking_lot::RwLock::new(HashMap::new()),
            mint_infos: parking_lot::RwLock::new(HashMap::new()),
            token_trackers: parking_lot::RwLock::new(HashMap::new()),
            positions: parking_lot::RwLock::new(HashMap::new()),
            pending_intents: parking_lot::RwLock::new(HashMap::new()),
            tokens_tracked: std::sync::atomic::AtomicU64::new(0),
            tokens_blacklisted: std::sync::atomic::AtomicU64::new(0),
            intents_generated: std::sync::atomic::AtomicU64::new(0),
            exits_generated: std::sync::atomic::AtomicU64::new(0),
        };

        // Seed tracker with a last_trade_ratio so intent generation can compute min_out.
        {
            let mut trackers = ctx.token_trackers.write();
            let mut tracker = TokenTracker::new("mint", "pool", "dex", 1, 0);
            tracker.record_trade("w", true, 1_000_000_000, 1_000_000, "sig", &cfg);
            trackers.insert("mint".to_string(), tracker);
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

        let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
        rt.block_on(async {
            generate_and_publish_buy_intent(&ctx, &signal)
                .await
                .expect("buy intent");
        });

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
        let jsonl_config = JsonlWriterConfig::new("trade_intents").with_log_dir(tmp.path());
        let jsonl_writer = JsonlWriter::new(jsonl_config).expect("jsonl writer");

        let ctx = MomentumContext {
            run_id: "run-test".to_string(),
            config: parking_lot::RwLock::new(cfg.clone()),
            nats: None,
            jsonl_writer,
            intent_counter: std::sync::atomic::AtomicU64::new(0),
            pool_first_seen: parking_lot::RwLock::new(std::collections::HashMap::new()),
            pending_dev_info: parking_lot::RwLock::new(HashMap::new()),
            pending_pool_accounts: parking_lot::RwLock::new(HashMap::new()),
            mint_infos: parking_lot::RwLock::new(HashMap::new()),
            token_trackers: parking_lot::RwLock::new(HashMap::new()),
            positions: parking_lot::RwLock::new(HashMap::new()),
            pending_intents: parking_lot::RwLock::new(HashMap::new()),
            tokens_tracked: std::sync::atomic::AtomicU64::new(0),
            tokens_blacklisted: std::sync::atomic::AtomicU64::new(0),
            intents_generated: std::sync::atomic::AtomicU64::new(0),
            exits_generated: std::sync::atomic::AtomicU64::new(0),
        };

        // Open a position.
        {
            let mut positions = ctx.positions.write();
            let mut pos = PositionTracker::new("mint", "pool", "dex", 1.0, 6, 123, 1_000);
            pos.entry_time = Instant::now() - Duration::from_secs(10);
            positions.insert("mint".to_string(), pos);
        }

        // Record a dev sell after entry.
        {
            let mut trackers = ctx.token_trackers.write();
            let mut tracker = TokenTracker::new("mint", "pool", "dex", 1, 0);
            tracker.dev_wallet = Some("dev".to_string());
            tracker.record_trade("dev", false, 2_000_000_000, 1, "devsig", &cfg);
            if let Some(last) = tracker.trades.last_mut() {
                last.timestamp = Instant::now() - Duration::from_secs(1);
            }
            trackers.insert("mint".to_string(), tracker);
        }

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
        let jsonl_config = JsonlWriterConfig::new("trade_intents").with_log_dir(tmp.path());
        let jsonl_writer = JsonlWriter::new(jsonl_config).expect("jsonl writer");

        let ctx = MomentumContext {
            run_id: "run-test".to_string(),
            config: parking_lot::RwLock::new(cfg.clone()),
            nats: None,
            jsonl_writer,
            intent_counter: std::sync::atomic::AtomicU64::new(0),
            pool_first_seen: parking_lot::RwLock::new(std::collections::HashMap::new()),
            pending_dev_info: parking_lot::RwLock::new(HashMap::new()),
            pending_pool_accounts: parking_lot::RwLock::new(HashMap::new()),
            mint_infos: parking_lot::RwLock::new(HashMap::new()),
            token_trackers: parking_lot::RwLock::new(HashMap::new()),
            positions: parking_lot::RwLock::new(HashMap::new()),
            pending_intents: parking_lot::RwLock::new(HashMap::new()),
            tokens_tracked: std::sync::atomic::AtomicU64::new(0),
            tokens_blacklisted: std::sync::atomic::AtomicU64::new(0),
            intents_generated: std::sync::atomic::AtomicU64::new(0),
            exits_generated: std::sync::atomic::AtomicU64::new(0),
        };

        // Open a position.
        {
            let mut positions = ctx.positions.write();
            let mut pos = PositionTracker::new("mint", "pool", "dex", 1.0, 6, 123, 1_000);
            pos.entry_time = Instant::now() - Duration::from_secs(10);
            positions.insert("mint".to_string(), pos);
        }

        // Record LP removal after entry.
        {
            let mut trackers = ctx.token_trackers.write();
            let mut tracker = TokenTracker::new("mint", "pool", "dex", 1, 0);
            tracker.record_lp_removal();
            tracker.lp_removal_time = Some(Instant::now() - Duration::from_secs(1));
            trackers.insert("mint".to_string(), tracker);
        }

        let exits = ctx.check_for_exits();
        assert_eq!(exits.len(), 1);
        assert_eq!(exits[0].0, "mint");
        assert_eq!(exits[0].3, "LP_REMOVAL");
        assert_eq!(exits[0].4, "LP removed post-entry");
    }
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
    let max_slippage = { ctx.config.read().early_max_slippage_bps }; // Use higher slippage for exits

    // SOL as output (selling tokens for SOL)
    let sol_mint = "So11111111111111111111111111111111111111112";

    // Decimals depend on the token. Prefer decimals from the open position (which was seeded
    // from MarketEventKind::TokenMintInfo), fall back to mint_infos cache.
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
            .ok_or_else(|| {
                anyhow::anyhow!("cannot generate exit intent: missing TokenMintInfo.decimals")
            })?
    };

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
            accounts: {
                let requires = MomentumContext::dex_requires_pool_accounts(dex);
                if requires {
                    let accounts =
                        ctx.try_get_dex_pool_accounts_for_mint(mint)
                            .ok_or_else(|| {
                                anyhow::anyhow!(
                                    "cannot generate exit intent: missing DexPoolAccounts"
                                )
                            })?;
                    if accounts.len() != 14 || accounts.first().map(|s| s.as_str()) != Some(pool) {
                        anyhow::bail!("cannot generate exit intent: invalid DexPoolAccounts")
                    }
                    accounts
                } else {
                    Vec::new()
                }
            },
            token_program: None, // Momentum-bot doesn't need Token-2022 support yet
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
        .insert("reason_detail".to_string(), reason.to_string());
    intent
        .metadata
        .insert("exit_type".to_string(), exit_type.to_string());

    // Pump.fun and PumpSwap sell tx building require the creator/dev wallet.
    if dex == "pumpfun"
        || dex.eq_ignore_ascii_case("pump_amm")
        || dex.eq_ignore_ascii_case("pumpswap")
        || dex.eq_ignore_ascii_case("PumpFunAmm")
    {
        let creator = creator_opt.ok_or_else(|| {
            anyhow::anyhow!("cannot generate {} exit: missing dev_wallet/creator", dex)
        })?;
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

        MarketEventKind::DexPoolAccounts {
            dex,
            pool_address,
            base_mint,
            quote_mint,
            accounts,
        } => {
            ctx.record_pool_seen(pool_address, event.slot.unwrap_or(0));
            ctx.record_dex_pool_accounts(dex, pool_address, base_mint, quote_mint, accounts);
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
            ..  // Ignore token_decimals, we don't need it for momentum detection
        } => {
            // P1: Trade-based Token Discovery
            // If we missed the PoolCreated event (Geyser filter issues), discover via first trade
            let tracker_exists = ctx.token_trackers.read().contains_key(mint);

            if !tracker_exists && *is_buy && *sol_amount > 0 {
                // Use DEX from event if available, otherwise infer from pool_address pattern
                let slot = event.slot.unwrap_or(0);
                let dex = if !event_dex.is_empty() && event_dex != "unknown" {
                    event_dex.as_str()
                } else if pool_address.starts_with("pump") || pool_address.starts_with("pAMM") {
                    "pump_amm"
                } else {
                    "pumpfun" // Default assumption for Bonding Curve
                };

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

                let created =
                    ctx.get_or_create_tracker(mint, pool_address, dex, slot, initial_liq_lamports);

                if created {
                    info!(
                        mint = %mint,
                        pool = %pool_address,
                        dex = %dex,
                        discovery = "trade",
                        "📊 Token tracker initialized (trade-based discovery)"
                    );
                }
            }

            // Record the trade in the tracker
            let sol_lamports = *sol_amount;
            let token_raw = *token_amount;
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
            let sol_lamports = *sol_amount;
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
