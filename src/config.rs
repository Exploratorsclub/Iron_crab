use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
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
