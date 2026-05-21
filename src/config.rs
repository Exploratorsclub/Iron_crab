use anyhow::{anyhow, Result};
use serde::{Deserialize, Deserializer, Serialize};
use std::str::FromStr;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppCfg {
    pub name: String,
    pub log_level: String,
    pub autosave_state_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SolanaCfg {
    pub rpc_url: String,
    pub ws_url: String,
    /// Optional Geyser gRPC endpoint for <10ms account updates
    /// If set, uses Geyser instead of WebSocket for pool updates
    /// Example: "http://127.0.0.1:10000"
    #[serde(default)]
    pub geyser_grpc_url: Option<String>,
    /// Optional Helius RPC URL for mint validation (full transaction index)
    /// Used to verify token age with complete history that local validators lack
    /// Example: "https://mainnet.helius-rpc.com/?api-key=YOUR_KEY"
    #[serde(default)]
    pub helius_rpc_url: Option<String>,
    pub keypair_path: String,
    #[serde(default)]
    pub rpc_min_concurrency: Option<usize>,
    #[serde(default)]
    pub rpc_max_concurrency: Option<usize>,
    #[serde(default)]
    pub rpc_initial_concurrency: Option<usize>,
    #[serde(default)]
    pub rpc_inc_every_successes: Option<usize>,
    #[serde(default)]
    pub rpc_dec_on_rate_limit: Option<usize>,
    #[serde(default)]
    pub rpc_timeout_ms: Option<u64>,
    // WS options
    #[serde(default)]
    pub ws_failover_urls: Option<Vec<String>>, // additional endpoints to try for PubSub
    #[serde(default)]
    pub ws_connect_timeout_ms: Option<u64>, // connect timeout per attempt
    #[serde(default)]
    pub ws_max_backoff_ms: Option<u64>, // cap for exponential backoff
    #[serde(default)]
    pub ws_headers: Option<std::collections::HashMap<String, String>>, // optional auth headers for WS
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketCfg {
    pub name: String,
    pub allocation_pct: u32,
    pub strategy: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllocatorCfg {
    pub mode: String,
    pub rebalance_secs: u64,
    pub min_transfer_sol: f64,
}

// toml::Value implementiert kein Default – wir liefern selbst einen leeren Table
fn default_toml_value() -> toml::Value {
    toml::Value::Table(Default::default())
}

/// Migrates deprecated `[momentum]` key `min_trades_per_sec` to `min_trades_per_min` (multiply by 60).
/// If both keys exist, `min_trades_per_min` wins and the legacy key is dropped.
pub fn migrate_momentum_trade_velocity_keys_in_table(
    table: &mut toml::map::Map<String, toml::Value>,
) {
    use tracing::warn;
    if !table.contains_key("min_trades_per_sec") {
        return;
    }
    if table.contains_key("min_trades_per_min") {
        warn!(
            "`[momentum]` contains deprecated `min_trades_per_sec`; ignored in favour of `min_trades_per_min`"
        );
        table.remove("min_trades_per_sec");
        return;
    }
    let Some(raw) = table.remove("min_trades_per_sec") else {
        return;
    };
    let legacy = raw
        .as_float()
        .or_else(|| raw.as_integer().map(|i| i as f64))
        .unwrap_or(0.0);
    let converted = legacy * 60.0;
    warn!(
        legacy_min_trades_per_sec = legacy,
        min_trades_per_min = converted,
        "`min_trades_per_sec` is deprecated; converted to `min_trades_per_min` (×60; value was trades/s, not trades/min)"
    );
    table.insert(
        "min_trades_per_min".to_string(),
        toml::Value::Float(converted),
    );
}

fn momentum_cfg_from_toml_value(val: toml::Value) -> Result<MomentumCfg, String> {
    toml::to_string(&val)
        .map_err(|e| e.to_string())
        .and_then(|s| toml::from_str(&s).map_err(|e| e.to_string()))
}

pub(crate) fn deserialize_optional_momentum<'de, D>(
    deserializer: D,
) -> Result<Option<MomentumCfg>, D::Error>
where
    D: Deserializer<'de>,
{
    let opt = Option::<toml::Value>::deserialize(deserializer)?;
    let Some(mut v) = opt else {
        return Ok(None);
    };
    if let Some(t) = v.as_table_mut() {
        migrate_momentum_trade_velocity_keys_in_table(t);
    }
    momentum_cfg_from_toml_value(v)
        .map_err(serde::de::Error::custom)
        .map(Some)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyDef {
    pub kind: String, // "rust" | "python"
    #[serde(default)]
    pub module: Option<String>,
    #[serde(default)]
    pub class: Option<String>,
    #[serde(default = "default_toml_value")]
    pub params: toml::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub app: AppCfg,
    pub solana: SolanaCfg,
    pub markets: Vec<MarketCfg>,
    pub allocator: AllocatorCfg,
    pub strategies: std::collections::HashMap<String, StrategyDef>,
    #[serde(default)]
    pub arbitrage: Option<ArbCfg>,
    #[serde(default)]
    pub sniper: Option<SniperSettings>,
    #[serde(default)]
    pub orca: OrcaCfg,
    #[serde(default)]
    pub wallet_tracker: Option<WalletTrackerCfg>,
    #[serde(default, deserialize_with = "deserialize_optional_momentum")]
    pub momentum: Option<MomentumCfg>,
    #[serde(default)]
    pub execution_engine: Option<ExecutionEngineCfg>,
}

/// Execution Engine Configuration (for execution-engine binary)
/// TOML section: [execution_engine]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ExecutionEngineCfg {
    /// Enable Jito bundle submission for atomic execution
    #[serde(default)]
    pub jito_enabled: Option<bool>,
    /// Tip amount in lamports for Jito bundles (default: 10000)
    #[serde(default)]
    pub jito_tip_lamports: Option<u64>,
    /// Jito block engine region: frankfurt, amsterdam, ny, tokyo, slc
    #[serde(default)]
    pub jito_region: Option<String>,
    /// Address Lookup Table (ALT) pubkey for transaction size reduction.
    /// Required for cross-DEX arbitrage (transactions > 1232 bytes).
    /// Create with: cargo run --bin setup-alt
    #[serde(default)]
    pub address_lookup_table: Option<String>,
    /// WSOL Manager Configuration
    #[serde(default)]
    pub wsol_manager: Option<WsolManagerCfg>,
    /// Account Janitor Configuration (cleanup empty ATAs, dust)
    #[serde(default)]
    pub account_janitor: Option<AccountJanitorCfg>,
    /// TX Submission Configuration (TPU/Jito/RPC fallback)
    #[serde(default)]
    pub tx_submission: Option<TxSubmissionCfg>,
    /// Fee Policy Configuration (priority fees, compute limits)
    #[serde(default)]
    pub fee_policy: Option<FeePolicyCfg>,
    /// Commitment level for TX confirmation: `"confirmed"` (default, faster, reorg risk) or `"finalized"` (slower, lower reorg risk).
    /// When set to `"finalized"`, confirmation accepts only finalized on-chain status (stricter than `"confirmed"`).
    #[serde(default)]
    pub confirm_commitment: Option<String>,
}

/// Fee Policy Configuration
/// Controls compute units, priority fees, and cost limits.
/// TOML section: [execution_engine.fee_policy]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeePolicyCfg {
    /// Default compute units for transactions. Default: 200000
    #[serde(default = "default_compute_units")]
    pub default_compute_units: u32,
    /// Maximum compute units allowed. Default: 1400000
    #[serde(default = "default_max_compute_units")]
    pub max_compute_units: u32,
    /// Compute units for arbitrage transactions. Default: 400000
    #[serde(default = "default_arb_compute_units")]
    pub arb_compute_units: u32,
    /// Default priority fee (micro-lamports per CU). Default: 1000
    #[serde(default = "default_priority_fee")]
    pub default_priority_fee_micro_lamports: u64,
    /// Maximum priority fee allowed. Default: 100000
    #[serde(default = "default_max_priority_fee")]
    pub max_priority_fee_micro_lamports: u64,
    /// Priority fee for Tier0/urgent intents (liquidation, kills). Default: 100000
    #[serde(default = "default_tier0_priority_fee")]
    pub tier0_priority_fee_micro_lamports: u64,
    /// Maximum total TX cost in lamports. Default: 50000000 (0.05 SOL)
    #[serde(default = "default_max_tx_cost")]
    pub max_tx_cost_lamports: u64,
    /// Minimum profit after fees in basis points. Default: 10
    #[serde(default = "default_min_profit_after_fees")]
    pub min_profit_after_fees_bps: i32,
    /// Optional: override Tier0 priority fee for liquidation sells
    #[serde(default)]
    pub liquidation_priority_fee_micro_lamports: Option<u64>,
    /// Optional: override max priority fee for liquidation sells
    #[serde(default)]
    pub liquidation_max_priority_fee_micro_lamports: Option<u64>,
    /// Optional: override max total TX cost for liquidation sells (lamports)
    #[serde(default)]
    pub liquidation_max_tx_cost_lamports: Option<u64>,
}

fn default_compute_units() -> u32 {
    200_000
}
fn default_max_compute_units() -> u32 {
    1_400_000
}
fn default_arb_compute_units() -> u32 {
    400_000
}
fn default_priority_fee() -> u64 {
    1_000
}
fn default_max_priority_fee() -> u64 {
    1_000_000
} // 1 lamport/CU max
fn default_tier0_priority_fee() -> u64 {
    500_000
} // 0.5 lamports/CU for urgent
fn default_max_tx_cost() -> u64 {
    50_000_000
}
fn default_min_profit_after_fees() -> i32 {
    10
}

impl Default for FeePolicyCfg {
    fn default() -> Self {
        Self {
            default_compute_units: default_compute_units(),
            max_compute_units: default_max_compute_units(),
            arb_compute_units: default_arb_compute_units(),
            default_priority_fee_micro_lamports: default_priority_fee(),
            max_priority_fee_micro_lamports: default_max_priority_fee(),
            tier0_priority_fee_micro_lamports: default_tier0_priority_fee(),
            max_tx_cost_lamports: default_max_tx_cost(),
            min_profit_after_fees_bps: default_min_profit_after_fees(),
            liquidation_priority_fee_micro_lamports: None,
            liquidation_max_priority_fee_micro_lamports: None,
            liquidation_max_tx_cost_lamports: None,
        }
    }
}

/// TX Submission Configuration
/// Configures how transactions are submitted to the network.
/// Supports TPU Direct (fastest), Jito Bundles (MEV protection), and RPC (fallback).
/// TOML section: [execution_engine.tx_submission]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxSubmissionCfg {
    /// Primary submission method: "tpu", "jito", "rpc"
    /// TPU Direct is fastest (~50-100ms) but requires validator connectivity.
    /// Jito provides MEV protection for arbitrage.
    /// RPC is the fallback (~200-400ms).
    #[serde(default = "default_tx_primary_method")]
    pub primary_method: String,

    /// Fallback chain: methods to try if primary fails (in order)
    /// Example: ["jito", "rpc"]
    #[serde(default = "default_tx_fallback_chain")]
    pub fallback_chain: Vec<String>,

    /// Enable TPU Direct submission. Default: true
    /// Requires validator connectivity (uses RPC for leader schedule).
    #[serde(default = "default_tpu_enabled")]
    pub tpu_enabled: bool,

    /// Number of leader slots to fan out to. Default: 2
    /// Sends to current leader + next N leaders for redundancy.
    #[serde(default = "default_tpu_fanout_slots")]
    pub tpu_fanout_slots: u64,

    /// Number of times to forward TX to leaders. Default: 4
    #[serde(default = "default_tpu_leader_forward_count")]
    pub tpu_leader_forward_count: u64,

    /// Timeout per method before trying fallback (ms). Default: 2000
    #[serde(default = "default_tx_method_timeout_ms")]
    pub method_timeout_ms: u64,

    /// Retries per method before fallback. Default: 2
    #[serde(default = "default_tx_retries_per_method")]
    pub retries_per_method: u32,

    /// Skip TPU for bundle-required intents (always use Jito). Default: true
    #[serde(default = "default_skip_tpu_for_bundles")]
    pub skip_tpu_for_bundles: bool,

    /// Enable parallel send (TPU + RPC simultaneously). Default: true
    /// Sends via both TPU and RPC at the same time for maximum reliability.
    /// Solana deduplicates by signature, so only one TX lands and you pay fees once.
    /// Recommended for all non-bundle transactions (liquidations, sells, buys).
    #[serde(default = "default_parallel_send")]
    pub parallel_send: bool,

    /// TPU Leader Cache Health Check: Max slots the cache can be stale before reconnect. Default: 50
    /// If the leader cache is more than N slots behind current slot, trigger reconnect.
    #[serde(default = "default_tpu_cache_stale_threshold")]
    pub tpu_cache_stale_threshold: u64,

    /// TPU Leader Cache Health Check interval in seconds. Default: 10
    /// How often to check if the leader cache is stale.
    #[serde(default = "default_tpu_health_check_interval_secs")]
    pub tpu_health_check_interval_secs: u64,

    /// Reconnect threshold: consecutive send failures before reconnect. Default: 3
    #[serde(default = "default_tpu_reconnect_failure_threshold")]
    pub tpu_reconnect_failure_threshold: u32,
}

fn default_tx_primary_method() -> String {
    "tpu".into()
}
fn default_tx_fallback_chain() -> Vec<String> {
    vec!["jito".into(), "rpc".into()]
}
fn default_tpu_enabled() -> bool {
    true
}
fn default_tpu_fanout_slots() -> u64 {
    4 // Increased from 2 for better landing rate
}
fn default_tpu_leader_forward_count() -> u64 {
    4
}
fn default_tx_method_timeout_ms() -> u64 {
    2000
}
fn default_tx_retries_per_method() -> u32 {
    2
}
fn default_skip_tpu_for_bundles() -> bool {
    true
}
fn default_parallel_send() -> bool {
    true
}
fn default_tpu_cache_stale_threshold() -> u64 {
    50 // ~20 seconds worth of slots
}
fn default_tpu_health_check_interval_secs() -> u64 {
    10
}
fn default_tpu_reconnect_failure_threshold() -> u32 {
    3
}

impl Default for TxSubmissionCfg {
    fn default() -> Self {
        Self {
            primary_method: default_tx_primary_method(),
            fallback_chain: default_tx_fallback_chain(),
            tpu_enabled: default_tpu_enabled(),
            tpu_fanout_slots: default_tpu_fanout_slots(),
            tpu_leader_forward_count: default_tpu_leader_forward_count(),
            method_timeout_ms: default_tx_method_timeout_ms(),
            retries_per_method: default_tx_retries_per_method(),
            skip_tpu_for_bundles: default_skip_tpu_for_bundles(),
            parallel_send: default_parallel_send(),
            tpu_cache_stale_threshold: default_tpu_cache_stale_threshold(),
            tpu_health_check_interval_secs: default_tpu_health_check_interval_secs(),
            tpu_reconnect_failure_threshold: default_tpu_reconnect_failure_threshold(),
        }
    }
}

/// WSOL Manager Configuration
/// Maintains WSOL balance for efficient arbitrage transactions.
/// Professional bots don't wrap/unwrap in the arb TX itself.
/// TOML section: [execution_engine.wsol_manager]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsolManagerCfg {
    /// Enable WSOL management. Default: true
    #[serde(default = "default_wsol_enabled")]
    pub enabled: bool,
    /// Minimum WSOL balance in SOL. Below this triggers wrap.
    #[serde(default = "default_wsol_min")]
    pub min_wsol_sol: f64,
    /// Target WSOL balance in SOL after wrap.
    #[serde(default = "default_wsol_target")]
    pub target_wsol_sol: f64,
    /// Maximum WSOL balance in SOL. Above this triggers unwrap.
    #[serde(default = "default_wsol_max")]
    pub max_wsol_sol: f64,
    /// Minimum native SOL to keep (rent + buffer). Default: 0.1 SOL
    #[serde(default = "default_wsol_min_native")]
    pub min_native_sol: f64,
    /// Cooldown between wrap/unwrap operations in seconds. Default: 30
    #[serde(default = "default_wsol_cooldown")]
    pub cooldown_secs: u64,
    /// Dry-run mode: log actions but don't send TX. Default: false
    #[serde(default)]
    pub dry_run: bool,
}

fn default_wsol_enabled() -> bool {
    true
}
fn default_wsol_min() -> f64 {
    0.5
}
fn default_wsol_target() -> f64 {
    1.0
}
fn default_wsol_max() -> f64 {
    2.0
}
fn default_wsol_min_native() -> f64 {
    0.1
}
fn default_wsol_cooldown() -> u64 {
    30
}

impl Default for WsolManagerCfg {
    fn default() -> Self {
        Self {
            enabled: default_wsol_enabled(),
            min_wsol_sol: default_wsol_min(),
            target_wsol_sol: default_wsol_target(),
            max_wsol_sol: default_wsol_max(),
            min_native_sol: default_wsol_min_native(),
            cooldown_secs: default_wsol_cooldown(),
            dry_run: false,
        }
    }
}

/// Account Janitor Configuration
/// Cleans up empty ATAs to recover rent, and handles dust tokens.
/// TOML section: [execution_engine.account_janitor]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountJanitorCfg {
    /// Enable account janitor. Default: false
    #[serde(default)]
    pub enabled: bool,
    /// Interval for closing empty ATAs in seconds. Default: 3600 (1 hour)
    #[serde(default = "default_janitor_close_interval")]
    pub close_ata_interval_secs: u64,
    /// Minimum age of empty ATA before closing in seconds. Default: 86400 (24h)
    #[serde(default = "default_janitor_min_age")]
    pub close_ata_min_age_secs: u64,
    /// Maximum ATAs to close per run. Default: 10
    #[serde(default = "default_janitor_max_per_run")]
    pub close_ata_max_per_run: usize,
    /// Enable merge dust feature (consolidate duplicate ATAs). Default: false
    #[serde(default)]
    pub merge_dust_enabled: bool,
    /// Interval for merging duplicate ATAs in seconds. Default: 300 (5 min)
    #[serde(default = "default_janitor_merge_interval")]
    pub merge_dust_interval_secs: u64,
    /// Maximum token merges per run. Default: 5
    #[serde(default = "default_janitor_merge_max_per_run")]
    pub merge_dust_max_per_run: usize,
    /// Enable swap dust feature (swap small balances to SOL). Default: false
    #[serde(default)]
    pub swap_dust_enabled: bool,
    /// Interval for swapping dust tokens to SOL in seconds. Default: 86400 (24h)
    #[serde(default = "default_janitor_swap_interval")]
    pub swap_dust_interval_secs: u64,
    /// Minimum token value in SOL to consider for swap. Default: 0.001 SOL
    #[serde(default = "default_janitor_swap_min_value")]
    pub swap_dust_min_value_sol: f64,
    /// Maximum slippage for dust swaps in bps. Default: 500 (5%)
    #[serde(default = "default_janitor_swap_slippage")]
    pub swap_dust_max_slippage_bps: u32,
    /// Maximum swaps per run. Default: 5
    #[serde(default = "default_janitor_swap_max_per_run")]
    pub swap_dust_max_per_run: usize,
    /// Dry-run mode: log actions but don't send TX. Default: false
    #[serde(default)]
    pub dry_run: bool,
}

fn default_janitor_close_interval() -> u64 {
    3600
}
fn default_janitor_min_age() -> u64 {
    86400
}
fn default_janitor_max_per_run() -> usize {
    10
}
fn default_janitor_merge_interval() -> u64 {
    300
}
fn default_janitor_merge_max_per_run() -> usize {
    5
}
fn default_janitor_swap_interval() -> u64 {
    86400 // 24 hours
}
fn default_janitor_swap_min_value() -> f64 {
    0.001 // 0.001 SOL
}
fn default_janitor_swap_slippage() -> u32 {
    500 // 5%
}
fn default_janitor_swap_max_per_run() -> usize {
    5
}

impl Default for AccountJanitorCfg {
    fn default() -> Self {
        Self {
            enabled: false,
            close_ata_interval_secs: default_janitor_close_interval(),
            close_ata_min_age_secs: default_janitor_min_age(),
            close_ata_max_per_run: default_janitor_max_per_run(),
            merge_dust_enabled: false,
            merge_dust_interval_secs: default_janitor_merge_interval(),
            merge_dust_max_per_run: default_janitor_merge_max_per_run(),
            swap_dust_enabled: false,
            swap_dust_interval_secs: default_janitor_swap_interval(),
            swap_dust_min_value_sol: default_janitor_swap_min_value(),
            swap_dust_max_slippage_bps: default_janitor_swap_slippage(),
            swap_dust_max_per_run: default_janitor_swap_max_per_run(),
            dry_run: false,
        }
    }
}

/// Wallet Tracker Configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WalletTrackerCfg {
    /// Enable wallet tracking. Default: true
    #[serde(default = "default_wallet_tracking_enabled")]
    pub enabled: bool,

    /// List of known "smart money" wallet addresses
    #[serde(default)]
    pub smart_money_wallets: Vec<String>,

    /// List of known bad actor wallet addresses (rug pullers, scammers)
    #[serde(default)]
    pub bad_actor_wallets: Vec<String>,

    /// How many slots after pool creation to track "early buyers"
    #[serde(default = "default_early_buyer_slots")]
    pub early_buyer_slots: u64,

    /// Maximum number of early buyers to track per token
    #[serde(default = "default_max_early_buyers")]
    pub max_early_buyers_per_token: usize,

    /// Minimum SOL amount to consider a "whale" buy (lamports)
    #[serde(default = "default_whale_threshold")]
    pub whale_threshold_lamports: u64,

    /// Maximum wallets to keep in memory cache (LRU eviction)
    #[serde(default = "default_max_cached_wallets")]
    pub max_cached_wallets: usize,
}

fn default_wallet_tracking_enabled() -> bool {
    true
}
fn default_early_buyer_slots() -> u64 {
    100
} // ~40 seconds
fn default_max_early_buyers() -> usize {
    50
}
fn default_whale_threshold() -> u64 {
    10_000_000_000
} // 10 SOL
fn default_max_cached_wallets() -> usize {
    10_000
}

/// Momentum Strategy Configuration (for momentum-bot)
/// All thresholds are configurable to tune filter aggressiveness.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MomentumCfg {
    /// Minimum liquidity (SOL) for EARLY regime. Default: 5.0 SOL
    #[serde(default = "default_early_min_liquidity")]
    pub early_min_liquidity_sol: f64,
    /// Minimum liquidity (SOL) for ESTABLISHED regime. Default: 20.0 SOL
    #[serde(default = "default_established_min_liquidity")]
    pub established_min_liquidity_sol: f64,
    /// Slot threshold for EARLY -> ESTABLISHED transition. Default: 1000 slots
    #[serde(default = "default_early_slot_threshold")]
    pub early_slot_threshold: u64,
    /// Max slippage BPS for EARLY trades. Default: 300 (3%)
    #[serde(default = "default_early_slippage")]
    pub early_max_slippage_bps: u32,
    /// Max slippage BPS for ESTABLISHED trades. Default: 100 (1%)
    #[serde(default = "default_established_slippage")]
    pub established_max_slippage_bps: u32,
    /// Default position size (SOL lamports). Default: 0.1 SOL
    #[serde(default = "default_position_lamports")]
    pub default_position_lamports: u64,

    // === Momentum v2 Entry: Probe-Buy + Scale-In ===
    /// Probe-buy size as fraction of `default_position_lamports` (0.0..=1.0).
    /// Example: 0.25 means probe uses 25% of the default position size.
    /// Default: 0.25.
    #[serde(default = "default_probe_buy_pct")]
    pub probe_buy_pct: f64,
    /// Time window (seconds) after probe fill to allow scale-in confirmation.
    /// Default: 30s.
    #[serde(default = "default_scale_in_confirm_window_secs")]
    pub scale_in_confirm_window_secs: u64,
    /// Minimum executable probe PnL (percent, I-14 `tokens_per_sol::pnl_pct`) required before
    /// emitting a scale-in BUY. Compared strictly as `exec_pnl > this` (default `0.0` ⇒ must be
    /// strictly profitable vs probe `entry_price` on an `executable_exit_quote`). Scale-in only.
    #[serde(default = "default_scale_in_min_probe_executable_pnl_pct")]
    pub scale_in_min_probe_executable_pnl_pct: f64,

    // === Filter 1: Liquidity Check ===
    /// Max dev supply percentage (e.g., 90.0 = 90%). Default: 90%
    #[serde(default = "default_max_dev_supply_pct")]
    pub max_dev_supply_pct: f64,
    /// Window to detect LP removal (seconds). Default: 60s
    #[serde(default = "default_lp_removal_window")]
    pub lp_removal_window_secs: u64,

    // === Filter 2: Buyer Velocity ===
    /// Min unique buyers in early window. Default: 3 (was 10, too strict)
    #[serde(default = "default_min_unique_buyers")]
    pub min_unique_buyers: u32,
    /// Early window for buyer count (seconds). Default: 30s
    #[serde(default = "default_buyer_window")]
    pub buyer_window_secs: u64,
    /// Min trades per minute for momentum (burst-safe chain-slot window; see momentum-bot).
    /// Default: 12.0 (= 0.2 trades/s × 60; same serde fallback as legacy `min_trades_per_sec` default).
    /// Deprecated TOML key `min_trades_per_sec` is converted ×60.
    #[serde(default = "default_min_trades_per_min")]
    pub min_trades_per_min: f64,
    /// Min buy dominance ratio (buys / total). Default: 0.5 (was 0.6, too strict)
    #[serde(default = "default_min_buy_dominance")]
    pub min_buy_dominance: f64,

    // === Filter 3: SOL Inflow ===
    /// Min net SOL inflow in window (lamports). Default: 2 SOL (was 20 SOL, WAY too strict)
    #[serde(default = "default_min_sol_inflow")]
    pub min_sol_inflow_lamports: u64,
    /// Inflow window (seconds). Default: 30s
    #[serde(default = "default_inflow_window")]
    pub inflow_window_secs: u64,
    /// Max single dump size (lamports). Default: 10 SOL
    #[serde(default = "default_max_single_dump")]
    pub max_single_dump_lamports: u64,

    // === Filter 4: Dev Behavior ===
    /// Dev early sell triggers exit (seconds after pool creation). Default: 60s
    #[serde(default = "default_dev_early_sell_window")]
    pub dev_early_sell_window_secs: u64,
    /// Dev rebuy is positive signal. Default: true
    #[serde(default = "default_dev_rebuy_positive")]
    pub dev_rebuy_positive: bool,

    // === Token Safety: Mint/Freeze Authority (via TokenMintInfo MarketEvents) ===
    /// Require mint authority to be renounced (mint_authority == None) before entering.
    /// Default: false (configurable; some legit early tokens keep it briefly).
    #[serde(default = "default_require_mint_authority_renounced")]
    pub require_mint_authority_renounced: bool,
    /// Require freeze authority to be none before entering.
    /// Default: false.
    #[serde(default = "default_require_freeze_authority_none")]
    pub require_freeze_authority_none: bool,

    // === Exit Strategy ===
    /// Hard stop-loss percentage from entry (e.g., 15 = -15%). Default: 15%
    #[serde(default = "default_hard_stop_loss")]
    pub hard_stop_loss_pct: f64,
    /// Trailing stop percentage from ATH (e.g., 20 = -20% from high). Default: 20%
    #[serde(default = "default_trailing_stop")]
    pub trailing_stop_pct: f64,
    /// Minimum profit to activate trailing stop (e.g., 10 = +10%). Default: 10%
    #[serde(default = "default_trailing_activation")]
    pub trailing_activation_pct: f64,
    /// Take profit percentage (e.g., 100 = +100% = 2x). Default: 100%
    #[serde(default = "default_take_profit")]
    pub take_profit_pct: f64,
    /// Min hold (secs) before TAKE_PROFIT can fire. Prevents false TP from wrong-pool price.
    #[serde(default = "default_take_profit_min_hold_secs")]
    pub take_profit_min_hold_secs: u64,
    /// Max hold time in seconds before forced exit. Default: 300s (5 min)
    #[serde(default = "default_max_hold_time")]
    pub max_hold_time_secs: u64,
    /// Momentum exit: min buy ratio to stay in (e.g., 0.4 = 40% buys). Default: 0.4
    #[serde(default = "default_momentum_exit_ratio")]
    pub momentum_exit_buy_ratio: f64,
    /// Momentum exit window (seconds). Default: 30s
    #[serde(default = "default_momentum_exit_window")]
    pub momentum_exit_window_secs: u64,
    /// Min trades in momentum window to evaluate exit. Default: 5
    #[serde(default = "default_momentum_exit_min_trades")]
    pub momentum_exit_min_trades: u32,
    /// Max slippage BPS for EXIT trades. Default: 9500 (95%)
    /// High value ensures sells succeed even at loss - prevents stuck positions.
    #[serde(default = "default_exit_max_slippage_bps")]
    pub exit_max_slippage_bps: u32,
    /// Bonding curve exit: threshold in percent (e.g. 98.0 = exit when 98% complete).
    /// Set to 0.0 to disable. Default: 98.0
    pub bonding_curve_exit_pct: Option<f64>,
    /// Bonding curve exit: enable (default: false). When true, use bonding_curve_exit_threshold_bps.
    #[serde(default)]
    pub bonding_curve_exit_enabled: bool,
    /// Bonding curve exit: threshold in BPS (0–10000). Default: 9800 (98%).
    #[serde(default = "default_bonding_curve_exit_threshold_bps")]
    pub bonding_curve_exit_threshold_bps: u32,

    // === Buyer Quality (anti-bot / concentration) ===
    /// Cap for top-1 buyer share (0.0..=1.0) within the buyer window.
    /// Default: 0.35.
    #[serde(default = "default_top1_buyer_share_cap")]
    pub top1_buyer_share_cap: f64,
    /// Cap for top-3 buyers combined share (0.0..=1.0) within the buyer window.
    /// Default: 0.60.
    #[serde(default = "default_top3_buyer_share_cap")]
    pub top3_buyer_share_cap: f64,
    /// Minimum ratio of repeat buyers (0.0..=1.0) within the buyer window.
    /// Interpretation is strategy-defined; intended as an anti-spoof heuristic.
    /// Default: 0.05.
    #[serde(default = "default_repeat_buyer_min_ratio")]
    pub repeat_buyer_min_ratio: f64,

    // === Trade Size Distribution (micro-buy spam) ===
    /// Minimum SOL trade size (lamports) used to classify "small buys".
    /// Default: 0.01 SOL.
    #[serde(default = "default_min_trade_size_lamports")]
    pub min_trade_size_lamports: u64,
    /// Maximum allowed ratio (0.0..=1.0) of buys below `min_trade_size_lamports`.
    /// Default: 0.85.
    #[serde(default = "default_small_buy_ratio_cap")]
    pub small_buy_ratio_cap: f64,

    // === Dump-Recovery Gate (anti-rug) ===
    /// Recovery evaluation window (seconds) after a detected dump.
    /// Default: 30s.
    #[serde(default = "default_dump_recovery_window_secs")]
    pub dump_recovery_window_secs: u64,
    /// Minimum buy dominance (0.0..=1.0) required during recovery.
    /// Default: 0.55.
    #[serde(default = "default_dump_recovery_min_buy_dominance")]
    pub dump_recovery_min_buy_dominance: f64,
    /// Minimum net SOL inflow (lamports) required during recovery.
    /// Default: 1 SOL.
    #[serde(default = "default_dump_recovery_min_net_inflow_lamports")]
    pub dump_recovery_min_net_inflow_lamports: u64,
    /// Minimum continuous recovery time (seconds) before allowing entry.
    /// Default: 10s.
    #[serde(default = "default_dump_recovery_min_recovery_secs")]
    pub dump_recovery_min_recovery_secs: u64,

    // === CTO Mode (pre-entry dev sell handling) ===
    /// If true, a pre-entry dev sell transitions into CTO candidate state (wait-for-recovery)
    /// instead of hard reject.
    /// Default: false.
    #[serde(default = "default_cto_enabled")]
    pub cto_enabled: bool,
    /// Minimum delay (seconds) after pre-entry dev sell before CTO recovery evaluation.
    /// Default: 30s.
    #[serde(default = "default_cto_entry_delay_secs")]
    pub cto_entry_delay_secs: u64,
    /// Confirmation window (seconds) used to evaluate CTO recovery.
    /// Default: 30s.
    #[serde(default = "default_cto_confirm_window_secs")]
    pub cto_confirm_window_secs: u64,
    /// Minimum unique buyers required during CTO recovery.
    /// Default: 5.
    #[serde(default = "default_cto_min_unique_buyers")]
    pub cto_min_unique_buyers: u32,
    /// Minimum buy dominance required during CTO recovery.
    /// Default: 0.55.
    #[serde(default = "default_cto_min_buy_dominance")]
    pub cto_min_buy_dominance: f64,
    /// Minimum net SOL inflow (lamports) required during CTO recovery.
    /// Default: 1 SOL.
    #[serde(default = "default_cto_min_net_inflow_lamports")]
    pub cto_min_net_inflow_lamports: u64,
}

// Momentum config defaults - tuned to be less strict than original hardcoded values
fn default_early_min_liquidity() -> f64 {
    5.0
}
fn default_established_min_liquidity() -> f64 {
    20.0
}
fn default_early_slot_threshold() -> u64 {
    1000
}
fn default_early_slippage() -> u32 {
    300
}
fn default_established_slippage() -> u32 {
    100
}
fn default_position_lamports() -> u64 {
    100_000_000
} // 0.1 SOL
fn default_probe_buy_pct() -> f64 {
    0.25
}
fn default_scale_in_confirm_window_secs() -> u64 {
    30
}
fn default_scale_in_min_probe_executable_pnl_pct() -> f64 {
    0.0
}
fn default_max_dev_supply_pct() -> f64 {
    90.0
}
fn default_lp_removal_window() -> u64 {
    60
}
fn default_min_unique_buyers() -> u32 {
    3
} // Relaxed from 10
fn default_buyer_window() -> u64 {
    30
} // Extended from 20
fn default_min_trades_per_min() -> f64 {
    12.0
} // 0.2 trades/s × 60; matches legacy default_min_trades_per_sec (relaxed from 0.5/s)
fn default_min_buy_dominance() -> f64 {
    0.5
} // Relaxed from 0.6
fn default_min_sol_inflow() -> u64 {
    2_000_000_000
} // 2 SOL, relaxed from 20 SOL!
fn default_inflow_window() -> u64 {
    30
}
fn default_max_single_dump() -> u64 {
    10_000_000_000
} // 10 SOL
fn default_dev_early_sell_window() -> u64 {
    60
}
fn default_dev_rebuy_positive() -> bool {
    true
}
fn default_require_mint_authority_renounced() -> bool {
    false
}
fn default_require_freeze_authority_none() -> bool {
    false
}
fn default_hard_stop_loss() -> f64 {
    15.0
}
fn default_trailing_stop() -> f64 {
    20.0
}
fn default_trailing_activation() -> f64 {
    10.0
}
fn default_take_profit() -> f64 {
    100.0
}
fn default_take_profit_min_hold_secs() -> u64 {
    5
}
fn default_max_hold_time() -> u64 {
    300
}
fn default_momentum_exit_ratio() -> f64 {
    0.4
}
fn default_momentum_exit_window() -> u64 {
    30
}
fn default_momentum_exit_min_trades() -> u32 {
    5
}
fn default_exit_max_slippage_bps() -> u32 {
    9500 // 95% - sell at any price rather than hold
}

fn default_bonding_curve_exit_threshold_bps() -> u32 {
    9800 // 98% - exit when bonding curve is 98% complete
}

fn default_top1_buyer_share_cap() -> f64 {
    0.35
}
fn default_top3_buyer_share_cap() -> f64 {
    0.60
}
fn default_repeat_buyer_min_ratio() -> f64 {
    0.05
}

fn default_min_trade_size_lamports() -> u64 {
    10_000_000
} // 0.01 SOL
fn default_small_buy_ratio_cap() -> f64 {
    0.85
}

fn default_dump_recovery_window_secs() -> u64 {
    30
}
fn default_dump_recovery_min_buy_dominance() -> f64 {
    0.55
}
fn default_dump_recovery_min_net_inflow_lamports() -> u64 {
    1_000_000_000
} // 1 SOL
fn default_dump_recovery_min_recovery_secs() -> u64 {
    10
}

fn default_cto_enabled() -> bool {
    false
}
fn default_cto_entry_delay_secs() -> u64 {
    30
}
fn default_cto_confirm_window_secs() -> u64 {
    30
}
fn default_cto_min_unique_buyers() -> u32 {
    5
}
fn default_cto_min_buy_dominance() -> f64 {
    0.55
}
fn default_cto_min_net_inflow_lamports() -> u64 {
    1_000_000_000
} // 1 SOL

impl Default for MomentumCfg {
    fn default() -> Self {
        Self {
            early_min_liquidity_sol: default_early_min_liquidity(),
            established_min_liquidity_sol: default_established_min_liquidity(),
            early_slot_threshold: default_early_slot_threshold(),
            early_max_slippage_bps: default_early_slippage(),
            established_max_slippage_bps: default_established_slippage(),
            default_position_lamports: default_position_lamports(),
            probe_buy_pct: default_probe_buy_pct(),
            scale_in_confirm_window_secs: default_scale_in_confirm_window_secs(),
            scale_in_min_probe_executable_pnl_pct: default_scale_in_min_probe_executable_pnl_pct(),
            max_dev_supply_pct: default_max_dev_supply_pct(),
            lp_removal_window_secs: default_lp_removal_window(),
            min_unique_buyers: default_min_unique_buyers(),
            buyer_window_secs: default_buyer_window(),
            min_trades_per_min: default_min_trades_per_min(),
            min_buy_dominance: default_min_buy_dominance(),
            min_sol_inflow_lamports: default_min_sol_inflow(),
            inflow_window_secs: default_inflow_window(),
            max_single_dump_lamports: default_max_single_dump(),
            dev_early_sell_window_secs: default_dev_early_sell_window(),
            dev_rebuy_positive: default_dev_rebuy_positive(),
            require_mint_authority_renounced: default_require_mint_authority_renounced(),
            require_freeze_authority_none: default_require_freeze_authority_none(),
            hard_stop_loss_pct: default_hard_stop_loss(),
            trailing_stop_pct: default_trailing_stop(),
            trailing_activation_pct: default_trailing_activation(),
            take_profit_pct: default_take_profit(),
            take_profit_min_hold_secs: default_take_profit_min_hold_secs(),
            max_hold_time_secs: default_max_hold_time(),
            momentum_exit_buy_ratio: default_momentum_exit_ratio(),
            momentum_exit_window_secs: default_momentum_exit_window(),
            momentum_exit_min_trades: default_momentum_exit_min_trades(),
            exit_max_slippage_bps: default_exit_max_slippage_bps(),
            bonding_curve_exit_pct: Some(98.0),
            bonding_curve_exit_enabled: false,
            bonding_curve_exit_threshold_bps: default_bonding_curve_exit_threshold_bps(),
            top1_buyer_share_cap: default_top1_buyer_share_cap(),
            top3_buyer_share_cap: default_top3_buyer_share_cap(),
            repeat_buyer_min_ratio: default_repeat_buyer_min_ratio(),
            min_trade_size_lamports: default_min_trade_size_lamports(),
            small_buy_ratio_cap: default_small_buy_ratio_cap(),
            dump_recovery_window_secs: default_dump_recovery_window_secs(),
            dump_recovery_min_buy_dominance: default_dump_recovery_min_buy_dominance(),
            dump_recovery_min_net_inflow_lamports: default_dump_recovery_min_net_inflow_lamports(),
            dump_recovery_min_recovery_secs: default_dump_recovery_min_recovery_secs(),
            cto_enabled: default_cto_enabled(),
            cto_entry_delay_secs: default_cto_entry_delay_secs(),
            cto_confirm_window_secs: default_cto_confirm_window_secs(),
            cto_min_unique_buyers: default_cto_min_unique_buyers(),
            cto_min_buy_dominance: default_cto_min_buy_dominance(),
            cto_min_net_inflow_lamports: default_cto_min_net_inflow_lamports(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ArbPairCfg {
    pub in_mint: String,
    pub out_mint: String,
    pub ui_amount: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ArbCfg {
    pub pairs: Vec<ArbPairCfg>,
    #[serde(default)]
    pub interval_ms: Option<u64>,
    #[serde(default)]
    pub min_profit_bps: Option<u32>,
    #[serde(default)]
    pub est_tx_cost_lamports: Option<u64>,
    #[serde(default)]
    pub discovery: Option<ArbDiscoveryCfg>,
    #[serde(default)]
    pub execution: Option<ExecutionCfg>,
    /// Enable event-driven execution via WebSocket pool updates (much faster than polling)
    #[serde(default = "default_event_driven")]
    pub event_driven: bool,
}

fn default_event_driven() -> bool {
    true // Event-driven is now default for professional performance
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionCfg {
    #[serde(default = "default_max_slippage_bps")]
    pub max_slippage_bps: u32,
    #[serde(default = "default_min_profit_bps_to_execute")]
    pub min_profit_bps_to_execute: u32,
    #[serde(default = "default_max_position_lamports")]
    pub max_position_lamports: u64,
    #[serde(default = "default_dry_run")]
    pub dry_run: bool,
    /// If true, run `simulateTransaction` for the assembled arbitrage TX and log results.
    /// Useful for debugging why opportunities never execute successfully.
    #[serde(default)]
    pub simulate: bool,
    #[serde(default = "default_priority_fee_micro_lamports")]
    pub priority_fee_micro_lamports: u64,
}

fn default_max_slippage_bps() -> u32 {
    500
}
fn default_min_profit_bps_to_execute() -> u32 {
    50
}
fn default_max_position_lamports() -> u64 {
    5_000_000_000
}
fn default_dry_run() -> bool {
    true
}
fn default_priority_fee_micro_lamports() -> u64 {
    1_000
}

impl Default for ExecutionCfg {
    fn default() -> Self {
        Self {
            max_slippage_bps: default_max_slippage_bps(),
            min_profit_bps_to_execute: default_min_profit_bps_to_execute(),
            max_position_lamports: default_max_position_lamports(),
            dry_run: default_dry_run(),
            simulate: false,
            priority_fee_micro_lamports: default_priority_fee_micro_lamports(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ArbDiscoveryCfg {
    #[serde(default)]
    pub enable: bool,
    /// Mode: "discovery-only" (only log/CSV) or "full-auto" (feed discovered pairs into scanner)
    #[serde(default)]
    pub mode: Option<String>,
    /// Debug/diagnostic: also log any init-like lines even if they don't contain 'pool'/'whirlpool'
    /// This can generate noisy CSVs but helps verify WS pipeline end-to-end
    #[serde(default)]
    pub log_all_inits: bool,
    /// List of base/anchor tokens to focus on (e.g., SOL, USDC, USDT mints)
    #[serde(default)]
    pub base_tokens: Vec<String>,
    /// Minimum pool liquidity threshold when one side is SOL (in SOL)
    #[serde(default)]
    pub min_liquidity_sol: Option<f64>,
    /// Minimum pool liquidity threshold when one side is USD-stable (in USD)
    #[serde(default)]
    pub min_liquidity_usd: Option<f64>,
    /// Default UI amount to use when generating edges for scanning
    #[serde(default)]
    pub default_ui_amount: Option<f64>,
    /// Limit number of discovered pairs per base token
    #[serde(default)]
    pub top_n_per_base: Option<usize>,
    /// Discovery loop interval in seconds
    #[serde(default)]
    pub interval_secs: Option<u64>,
    /// Discovery sources: enable/disable individual DEX connectors (defaults: both enabled)
    #[serde(default)]
    pub enable_raydium: Option<bool>,
    #[serde(default)]
    pub enable_orca: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SniperSettings {
    pub max_buy_sol: f64,
    pub max_slippage_bps: u32,
    /// Optional: Program IDs to subscribe to for logs (DEX pool init events)
    /// Defaults to Raydium AMM v4 and Orca Whirlpool when omitted.
    #[serde(default)]
    pub program_ids: Option<Vec<String>>,
    #[serde(default)]
    pub blacklist_mints: Vec<String>,
    #[serde(default)]
    pub blacklist_owners: Vec<String>,
    #[serde(default)]
    pub min_pool_liquidity_sol: Option<f64>,
    #[serde(default)]
    pub require_freeze_auth_none: Option<bool>,
    #[serde(default)]
    pub require_mint_decimals_range: Option<(u8, u8)>,
    #[serde(default)]
    pub lp_top1_max_pct: Option<f64>,
    #[serde(default)]
    pub lp_top3_max_pct: Option<f64>,
    #[serde(default)]
    pub lp_top5_max_pct: Option<f64>,
    // --- Risk Layer (neu) ---
    #[serde(default)]
    pub max_position_sol: Option<f64>, // Max Depot-Notional für eine einzelne neue Position
    #[serde(default)]
    pub stop_loss_bps: Option<u32>, // z.B. 3000 = -30% unter Entry
    #[serde(default)]
    pub take_profit_bps: Option<u32>, // z.B. 10000 = +100% Gewinn
    #[serde(default)]
    pub daily_loss_limit_sol: Option<f64>, // harter Tagesverlust-Limiter
    // --- Erweiterte Risk & Limits ---
    #[serde(default)]
    pub max_open_positions: Option<usize>, // Gesamtanzahl paralleler Positionen
    #[serde(default)]
    pub per_mint_position_limit: Option<u32>, // Mehrfachkäufe je Mint (derzeit 1 unterstützt; Platzhalter)
    #[serde(default)]
    pub stop_loss_cooldown_secs: Option<u64>, // Cooldown nach SL Exit
    #[serde(default)]
    pub drawdown_scale_start: Option<f64>, // Anteil daily_loss_limit ab dem Kaufgrößen reduziert werden (0.3 = 30%)
    #[serde(default)]
    pub drawdown_max_reduction: Option<f64>, // Max Reduktion Kaufgröße (0.7 => bis 70% Reduktion)
    #[serde(default)]
    pub rolling_pnl_window: Option<usize>, // Fenster für Sharpe Approx
    #[serde(default)]
    pub hot_reload_secs: Option<u64>, // Interval für Config Reload
    #[serde(default)]
    pub pending_trade_ttl_secs: Option<u64>, // TTL für PendingTrade Einträge (Cleanup wenn kein Fill)
    // --- Partielle Exits Konfiguration ---
    #[serde(default)]
    pub take_profit_tiers: Option<Vec<TakeProfitTier>>, // Gestaffelte TP Ebenen; aufsteigend nach bps
    #[serde(default)]
    pub trailing_stop_bps: Option<u32>, // Optionaler Trailing Stop (Abstand in bps vom Hoch nach Erreichen erster TP Ebene)
    #[serde(default)]
    pub min_exit_notional_sol: Option<f64>, // Mindest-Notional für einen Exit (Dust-Vermeidung)
    // --- Data & Pricing / Adaptive Slippage ---
    #[serde(default)]
    pub oracle_sol_usd_override: Option<f64>, // Optionaler statischer SOL/USD Preis (Oracle Placeholder)
    #[serde(default)]
    pub adaptive_slippage_min_bps: Option<u32>, // Untere Grenze dynamischer Slippage
    #[serde(default)]
    pub adaptive_slippage_max_bps: Option<u32>, // Obere Grenze dynamischer Slippage
    #[serde(default)]
    pub adaptive_slippage_window: Option<usize>, // Fenstergröße für gemittelten Fill-Slippage Anteil
    #[serde(default)]
    pub adaptive_slippage_target_pct: Option<f64>, // Ziel-Slippage in Anteil (z.B. 0.002 = 0.2%)
    #[serde(default)]
    pub adaptive_slippage_step_bps: Option<u32>, // Anpassungsschritt in bps je Fill (z.B. 5)
    // Exit Evaluation
    #[serde(default)]
    pub exit_eval_interval_secs: Option<u64>, // Separates Intervall für Exit-Evaluation
    // Oracles
    #[serde(default)]
    pub oracle_pyth_sol_usd: Option<String>, // Pyth Price account pubkey for SOL/USD
    #[serde(default)]
    pub oracle_switchboard_sol_usd: Option<String>, // Switchboard aggregator pubkey for SOL/USD
    #[serde(default)]
    pub oracle_preference: Option<String>, // "pyth" | "switchboard" | "override"
    // Log Rotation & Retention
    #[serde(default)]
    pub log_retention_days: Option<u32>, // Anzahl Tage für Log-Aufbewahrung (default: 30)
    #[serde(default)]
    pub log_cleanup_interval_hours: Option<u32>, // Cleanup-Intervall in Stunden (default: 24)
    // Quantile-based slippage (statistical learning)
    #[serde(default)]
    pub quantile_slippage_enabled: Option<bool>, // Enable quantile-based min_out calculation
    #[serde(default)]
    pub quantile_confidence_level: Option<f64>, // P95 = 0.95 (95th percentile)
    #[serde(default)]
    pub quantile_min_samples: Option<usize>, // Minimum historical fills before using quantile
    #[serde(default)]
    pub quantile_max_sample_age_secs: Option<u64>, // Maximum age of samples in seconds
    #[serde(default)]
    pub quantile_fallback_slippage_bps: Option<u32>, // Fallback slippage when insufficient data
    #[serde(default)]
    pub max_holders: Option<usize>, // Max holders for new token check (default: 20)
    // Configurable slippage for specific scenarios (avoiding hardcoded values)
    #[serde(default)]
    pub pumpfun_buy_slippage_bps: Option<u32>, // Minimum slippage for Pump.fun buys (default: 2500 = 25%)
    #[serde(default)]
    pub emergency_exit_slippage_bps: Option<u32>, // Slippage for stop-loss/emergency exits (default: 5000 = 50%)

    // === TIME-BASED EXIT STRATEGY ===
    #[serde(default)]
    pub enable_time_based_exits: Option<bool>, // Enable time-based exits instead of price-based
    #[serde(default)]
    pub max_hold_secs: Option<u64>, // Maximum hold time before forced exit (default: 90)
    #[serde(default)]
    pub timed_exit_tiers: Option<Vec<TimedExitTier>>, // Timed exit tiers [{secs, fraction}]

    // === KILL SWITCHES (Geyser-based, override all other logic) ===
    #[serde(default)]
    pub kill_switch_enabled: Option<bool>, // Enable kill switch monitoring
    #[serde(default)]
    pub kill_switch_dev_sell: Option<bool>, // Exit immediately if dev/creator sells
    #[serde(default)]
    pub kill_switch_sell_burst_count: Option<u32>, // Number of sells to trigger burst exit (e.g., 3)
    #[serde(default)]
    pub kill_switch_sell_burst_sol: Option<f64>, // Total SOL sold in burst to trigger (e.g., 0.5)
    #[serde(default)]
    pub kill_switch_sell_burst_slots: Option<u64>, // Time window in slots for burst (e.g., 5)
    #[serde(default)]
    pub kill_switch_flow_ratio_min: Option<f64>, // Min buy/sell ratio before exit (e.g., 0.6)
    #[serde(default)]
    pub kill_switch_negative_flow_slots: Option<u64>, // Consecutive negative flow slots to exit (e.g., 3)

    // === JITO BUNDLE INTEGRATION ===
    #[serde(default)]
    pub jito_enabled: Option<bool>, // Enable Jito bundle submission for exits
    #[serde(default)]
    pub jito_tip_lamports: Option<u64>, // Tip amount in lamports (default: 10000 = 0.00001 SOL)
    #[serde(default)]
    pub jito_region: Option<String>, // Block engine region: frankfurt, amsterdam, ny, tokyo, slc
    #[serde(default)]
    pub jito_min_exit_fraction: Option<f64>, // Min exit fraction to use Jito (default: 0.25 = 25%)
    #[serde(default)]
    pub jito_min_exit_sol: Option<f64>, // Min SOL value to use Jito (default: 0.5 SOL)
    #[serde(default)]
    pub jito_for_emergency: Option<bool>, // Always use Jito for emergency/panic exits (default: true)
    #[serde(default)]
    pub jito_for_final_exit: Option<bool>, // Always use Jito for 100% full exits (default: true)

    // === PARALLEL EXIT EXECUTION ===
    #[serde(default)]
    pub parallel_exits: Option<bool>, // Execute multiple exits concurrently (default: true)
    #[serde(default)]
    pub max_parallel_exits: Option<usize>, // Max concurrent exit tasks (default: 5)
    #[serde(default)]
    pub bundle_exits: Option<bool>, // Bundle multiple exit TXs via Jito (default: false)
}

/// Timed exit tier - sell fraction after N seconds
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TimedExitTier {
    pub secs: u64,     // Seconds after entry to trigger this tier
    pub fraction: f64, // Fraction of REMAINING position to sell (0.0-1.0)
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TakeProfitTier {
    pub bps: u32,      // Gewinnschwelle (>= bps löst diese Stufe aus)
    pub fraction: f64, // Anteil der ursprünglichen Lot-Größe, der an dieser Stufe verkauft wird (nicht kumulativ)
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct OrcaCfg {
    /// Enable persistent SQLite cache for Orca vault reserves.
    /// Reduces RPC load and improves latency by caching balances across restarts.
    #[serde(default)]
    pub enable_reserve_cache: bool,
    /// Path to SQLite database for reserve cache (relative to config dir).
    /// Only used if enable_reserve_cache=true. Default: "orca_reserves.db"
    #[serde(default)]
    pub cache_path: Option<String>,
    /// Number of top pools to prefetch reserves for on startup.
    /// Higher values = more cache warming but slower startup. Default: 100
    #[serde(default)]
    pub prefetch_top_pools: Option<usize>,
    /// Interval in seconds for background vault balance refresh.
    /// Set to 0 to disable vault refresh completely (saves RPC load).
    /// Default: 5 seconds. Recommended: 30-60 for low RPC load.
    #[serde(default)]
    pub vault_refresh_interval_secs: Option<u64>,
}

impl Config {
    pub fn load(path: &std::path::Path) -> anyhow::Result<Self> {
        let raw = std::fs::read_to_string(path)?;
        let cfg: Config = toml::from_str(&raw)?;
        cfg.validate()?;
        Ok(cfg)
    }
}

impl Config {
    pub fn validate(&self) -> Result<()> {
        let mut errs: Vec<String> = Vec::new();

        // --- app ---
        if self.app.name.trim().is_empty() {
            errs.push("app.name must not be empty".into());
        }
        if self.app.autosave_state_secs == 0 {
            errs.push("app.autosave_state_secs must be > 0".into());
        }

        // --- solana ---
        if !(self.solana.rpc_url.starts_with("http://")
            || self.solana.rpc_url.starts_with("https://"))
        {
            errs.push(format!(
                "solana.rpc_url must start with http:// or https:// (got {})",
                self.solana.rpc_url
            ));
        }
        if !(self.solana.ws_url.starts_with("ws://") || self.solana.ws_url.starts_with("wss://")) {
            errs.push(format!(
                "solana.ws_url must start with ws:// or wss:// (got {})",
                self.solana.ws_url
            ));
        }
        if self.solana.keypair_path.trim().is_empty() {
            errs.push("solana.keypair_path must not be empty".into());
        } else {
            let p = std::path::Path::new(&self.solana.keypair_path);
            if !p.exists() {
                errs.push(format!(
                    "solana.keypair_path does not exist: {}",
                    self.solana.keypair_path
                ));
            }
        }
        // concurrency constraints
        let (minc, maxc, initc) = (
            self.solana.rpc_min_concurrency,
            self.solana.rpc_max_concurrency,
            self.solana.rpc_initial_concurrency,
        );
        if let (Some(min), Some(max)) = (minc, maxc) {
            if min == 0 {
                errs.push("solana.rpc_min_concurrency must be > 0".into());
            }
            if max == 0 {
                errs.push("solana.rpc_max_concurrency must be > 0".into());
            }
            if min > max {
                errs.push(format!(
                    "solana.rpc_min_concurrency ({min}) must be <= rpc_max_concurrency ({max})"
                ));
            }
        }
        if let (Some(init), Some(min), Some(max)) = (initc, minc, maxc) {
            if init < min || init > max {
                errs.push(format!(
                    "solana.rpc_initial_concurrency ({init}) must be within [{min},{max}]"
                ));
            }
        }
        if let Some(t) = self.solana.rpc_timeout_ms {
            if t == 0 {
                errs.push("solana.rpc_timeout_ms must be > 0".into());
            }
        }
        if let Some(b) = self.solana.ws_max_backoff_ms {
            if b == 0 {
                errs.push("solana.ws_max_backoff_ms must be > 0".into());
            }
        }
        if let Some(c) = self.solana.ws_connect_timeout_ms {
            if c == 0 {
                errs.push("solana.ws_connect_timeout_ms must be > 0".into());
            }
        }
        if let Some(vec) = self.solana.ws_failover_urls.as_ref() {
            for (i, u) in vec.iter().enumerate() {
                if !(u.starts_with("ws://") || u.starts_with("wss://")) {
                    errs.push(format!(
                        "solana.ws_failover_urls[{i}] must start with ws:// or wss:// (got {u})"
                    ));
                }
            }
        }

        // --- markets & allocator ---
        if self.markets.is_empty() {
            errs.push("markets must not be empty".into());
        }
        let mut sum_alloc: i64 = 0;
        for (i, m) in self.markets.iter().enumerate() {
            if m.name.trim().is_empty() {
                errs.push(format!("markets[{i}].name must not be empty"));
            }
            if !(0..=100).contains(&m.allocation_pct) {
                errs.push(format!(
                    "markets[{i}].allocation_pct must be 0..=100 (got {})",
                    m.allocation_pct
                ));
            }
            if !self.strategies.contains_key(&m.strategy) {
                errs.push(format!(
                    "markets[{i}].strategy '{}' not defined in [strategies]",
                    m.strategy
                ));
            }
            sum_alloc += m.allocation_pct as i64;
        }
        if sum_alloc != 100 {
            errs.push(format!(
                "markets allocation sum must be 100, got {sum_alloc}"
            ));
        }

        if self.allocator.mode.trim().is_empty() {
            errs.push("allocator.mode must not be empty".into());
        }
        if self.allocator.rebalance_secs == 0 {
            errs.push("allocator.rebalance_secs must be > 0".into());
        }
        if self.allocator.min_transfer_sol < 0.0 {
            errs.push("allocator.min_transfer_sol must be >= 0".into());
        }

        // --- strategies ---
        for (name, s) in &self.strategies {
            match s.kind.to_ascii_lowercase().as_str() {
                "rust" => { /* ok; module/class optional */ }
                "python" => {
                    if s.module.as_deref().unwrap_or("").is_empty() {
                        errs.push(format!(
                            "strategies['{name}']: python kind requires 'module'"
                        ));
                    }
                    if s.class.as_deref().unwrap_or("").is_empty() {
                        errs.push(format!(
                            "strategies['{name}']: python kind requires 'class'"
                        ));
                    }
                }
                other => errs.push(format!(
                    "strategies['{name}']: unknown kind '{other}' (expected 'rust' or 'python')"
                )),
            }
        }

        // --- arbitrage ---
        if let Some(a) = &self.arbitrage {
            for (i, p) in a.pairs.iter().enumerate() {
                if p.in_mint.trim().is_empty() || p.out_mint.trim().is_empty() {
                    errs.push(format!("arbitrage.pairs[{i}] mints must not be empty"));
                }
                if p.ui_amount <= 0.0 {
                    errs.push(format!("arbitrage.pairs[{i}].ui_amount must be > 0"));
                }
            }
            if let Some(bps) = a.min_profit_bps {
                if bps > 50_000 {
                    errs.push(format!("arbitrage.min_profit_bps too large: {bps}"));
                }
            }
            if let Some(cost) = a.est_tx_cost_lamports {
                if cost == 0 {
                    errs.push("arbitrage.est_tx_cost_lamports must be > 0".into());
                }
            }
            if let Some(intv) = a.interval_ms {
                if intv == 0 {
                    errs.push("arbitrage.interval_ms must be > 0".into());
                }
            }
            // discovery sub-config
            if let Some(d) = &a.discovery {
                if d.enable {
                    let ray_enabled = d.enable_raydium.unwrap_or(true);
                    let orca_enabled = d.enable_orca.unwrap_or(true);
                    if !ray_enabled && !orca_enabled {
                        errs.push("arbitrage.discovery: at least one of enable_raydium or enable_orca must be true".into());
                    }
                    if let Some(mode) = &d.mode {
                        if mode != "discovery-only" && mode != "full-auto" {
                            errs.push(
                                "arbitrage.discovery.mode must be 'discovery-only' or 'full-auto'"
                                    .into(),
                            );
                        }
                    }
                    if let Some(s) = d.interval_secs {
                        if s == 0 {
                            errs.push("arbitrage.discovery.interval_secs must be > 0".into());
                        }
                    }
                    if let Some(n) = d.top_n_per_base {
                        if n == 0 {
                            errs.push("arbitrage.discovery.top_n_per_base must be > 0".into());
                        }
                    }
                    if let Some(v) = d.default_ui_amount {
                        if v <= 0.0 {
                            errs.push("arbitrage.discovery.default_ui_amount must be > 0".into());
                        }
                    }
                }
            }
        }

        // --- sniper ---
        if let Some(s) = &self.sniper {
            if s.max_buy_sol < 0.0 {
                errs.push("sniper.max_buy_sol must be >= 0".into());
            }
            if s.max_slippage_bps > 50_000 {
                errs.push("sniper.max_slippage_bps unrealistic (>50000)".into());
            }
            if let Some(v) = s.min_pool_liquidity_sol {
                if v < 0.0 {
                    errs.push("sniper.min_pool_liquidity_sol must be >= 0".into());
                }
            }
            if let Some((lo, hi)) = s.require_mint_decimals_range {
                if lo > hi || hi > 12 {
                    errs.push("sniper.require_mint_decimals_range invalid (lo<=hi<=12)".into());
                }
            }
            for (label, v) in [
                ("lp_top1_max_pct", s.lp_top1_max_pct),
                ("lp_top3_max_pct", s.lp_top3_max_pct),
                ("lp_top5_max_pct", s.lp_top5_max_pct),
            ] {
                if let Some(x) = v {
                    if !(0.0..=100.0).contains(&x) {
                        errs.push(format!("sniper.{label} must be 0..=100"));
                    }
                }
            }
            if let Some(v) = s.max_position_sol {
                if v < 0.0 {
                    errs.push("sniper.max_position_sol must be >= 0".into());
                }
            }
            if let Some(v) = s.daily_loss_limit_sol {
                if v < 0.0 {
                    errs.push("sniper.daily_loss_limit_sol must be >= 0".into());
                }
            }
            if let Some(v) = s.stop_loss_cooldown_secs {
                if v == 0 {
                    errs.push("sniper.stop_loss_cooldown_secs must be > 0".into());
                }
            }
            if let Some(v) = s.drawdown_scale_start {
                if !(0.0..=1.0).contains(&v) {
                    errs.push("sniper.drawdown_scale_start must be 0..=1".into());
                }
            }
            if let Some(v) = s.drawdown_max_reduction {
                if !(0.0..=1.0).contains(&v) {
                    errs.push("sniper.drawdown_max_reduction must be 0..=1".into());
                }
            }
            if let Some(v) = s.rolling_pnl_window {
                if v == 0 {
                    errs.push("sniper.rolling_pnl_window must be > 0".into());
                }
            }
            if let Some(v) = s.hot_reload_secs {
                if v == 0 {
                    errs.push("sniper.hot_reload_secs must be > 0".into());
                }
            }
            if let Some(v) = s.pending_trade_ttl_secs {
                if v == 0 {
                    errs.push("sniper.pending_trade_ttl_secs must be > 0".into());
                }
            }
            if let Some(v) = s.trailing_stop_bps {
                if v > 100_000 {
                    errs.push("sniper.trailing_stop_bps unrealistic (>100000)".into());
                }
            }
            if let Some(v) = s.min_exit_notional_sol {
                if v < 0.0 {
                    errs.push("sniper.min_exit_notional_sol must be >= 0".into());
                }
            }
            if let Some(ts) = s.take_profit_tiers.as_ref() {
                if ts.is_empty() {
                    errs.push("sniper.take_profit_tiers must not be empty if provided".into());
                }
                let mut last_bps = 0u32;
                let mut sum_frac = 0.0f64;
                for (i, t) in ts.iter().enumerate() {
                    if t.bps == 0 {
                        errs.push(format!("sniper.take_profit_tiers[{i}].bps must be > 0"));
                    }
                    if t.bps < last_bps {
                        errs.push(
                            "sniper.take_profit_tiers must be sorted ascending by bps".into(),
                        );
                    }
                    if !(0.0..=1.0).contains(&t.fraction) {
                        errs.push(format!(
                            "sniper.take_profit_tiers[{i}].fraction must be 0..=1"
                        ));
                    }
                    sum_frac += t.fraction;
                    last_bps = t.bps;
                }
                if sum_frac > 1.000_000_1 {
                    errs.push(format!(
                        "sniper.take_profit_tiers fractions sum {sum_frac:.3} exceeds 1.0"
                    ));
                }
            }
            if let Some(pref) = s.oracle_preference.as_deref() {
                match pref.to_ascii_lowercase().as_str() {
                    "pyth" => {
                        if s.oracle_pyth_sol_usd.as_deref().unwrap_or("").is_empty() {
                            errs.push(
                                "sniper.oracle_pyth_sol_usd required when oracle_preference=pyth"
                                    .into(),
                            );
                        }
                    }
                    "switchboard" => {
                        if s.oracle_switchboard_sol_usd
                            .as_deref()
                            .unwrap_or("")
                            .is_empty()
                        {
                            errs.push("sniper.oracle_switchboard_sol_usd required when oracle_preference=switchboard".into());
                        }
                    }
                    "override" => {
                        if s.oracle_sol_usd_override.is_none() {
                            errs.push("sniper.oracle_sol_usd_override required when oracle_preference=override".into());
                        }
                    }
                    other => errs.push(format!(
                        "sniper.oracle_preference invalid: {other} (pyth|switchboard|override)"
                    )),
                }
            }
        }

        // Validate optional sniper.program_ids if provided
        if let Some(s) = &self.sniper {
            if let Some(pids) = s.program_ids.as_ref() {
                for (i, pid) in pids.iter().enumerate() {
                    if pid.trim().is_empty() {
                        errs.push(format!("sniper.program_ids[{i}] must not be empty"));
                        continue;
                    }
                    // Basic Pubkey validation
                    if solana_sdk::pubkey::Pubkey::from_str(pid).is_err() {
                        errs.push(format!(
                            "sniper.program_ids[{i}] is not a valid Solana pubkey: {}",
                            pid
                        ));
                    }
                }
            }
        }

        if errs.is_empty() {
            Ok(())
        } else {
            Err(anyhow!(format!("config invalid:\n- {}", errs.join("\n- "))))
        }
    }
}
