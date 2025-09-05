
use serde::{Deserialize, Serialize};

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
    pub keypair_path: String,
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
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ArbPairCfg { pub in_mint: String, pub out_mint: String, pub ui_amount: f64 }

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ArbCfg {
    pub pairs: Vec<ArbPairCfg>,
    #[serde(default)] pub interval_ms: Option<u64>,
    #[serde(default)] pub min_profit_bps: Option<u32>,
    #[serde(default)] pub est_tx_cost_lamports: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SniperSettings {
    pub max_buy_sol: f64,
    pub max_slippage_bps: u32,
    #[serde(default)] pub blacklist_mints: Vec<String>,
    #[serde(default)] pub blacklist_owners: Vec<String>,
    #[serde(default)] pub min_pool_liquidity_sol: Option<f64>,
    #[serde(default)] pub require_freeze_auth_none: Option<bool>,
    #[serde(default)] pub require_mint_decimals_range: Option<(u8,u8)>,
    #[serde(default)] pub lp_top1_max_pct: Option<f64>,
    #[serde(default)] pub lp_top3_max_pct: Option<f64>,
    #[serde(default)] pub lp_top5_max_pct: Option<f64>,
    // --- Risk Layer (neu) ---
    #[serde(default)] pub max_position_sol: Option<f64>,          // Max Depot-Notional für eine einzelne neue Position
    #[serde(default)] pub stop_loss_bps: Option<u32>,              // z.B. 3000 = -30% unter Entry
    #[serde(default)] pub take_profit_bps: Option<u32>,            // z.B. 10000 = +100% Gewinn
    #[serde(default)] pub daily_loss_limit_sol: Option<f64>,       // harter Tagesverlust-Limiter
    // --- Erweiterte Risk & Limits ---
    #[serde(default)] pub max_open_positions: Option<usize>,       // Gesamtanzahl paralleler Positionen
    #[serde(default)] pub per_mint_position_limit: Option<u32>,    // Mehrfachkäufe je Mint (derzeit 1 unterstützt; Platzhalter)
    #[serde(default)] pub stop_loss_cooldown_secs: Option<u64>,    // Cooldown nach SL Exit
    #[serde(default)] pub drawdown_scale_start: Option<f64>,       // Anteil daily_loss_limit ab dem Kaufgrößen reduziert werden (0.3 = 30%)
    #[serde(default)] pub drawdown_max_reduction: Option<f64>,     // Max Reduktion Kaufgröße (0.7 => bis 70% Reduktion)
    #[serde(default)] pub rolling_pnl_window: Option<usize>,       // Fenster für Sharpe Approx
    #[serde(default)] pub hot_reload_secs: Option<u64>,            // Interval für Config Reload
    #[serde(default)] pub pending_trade_ttl_secs: Option<u64>,     // TTL für PendingTrade Einträge (Cleanup wenn kein Fill)
    // --- Partielle Exits Konfiguration ---
    #[serde(default)] pub take_profit_tiers: Option<Vec<TakeProfitTier>>, // Gestaffelte TP Ebenen; aufsteigend nach bps
    #[serde(default)] pub trailing_stop_bps: Option<u32>,          // Optionaler Trailing Stop (Abstand in bps vom Hoch nach Erreichen erster TP Ebene)
    #[serde(default)] pub min_exit_notional_sol: Option<f64>,      // Mindest-Notional für einen Exit (Dust-Vermeidung)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TakeProfitTier {
    pub bps: u32,        // Gewinnschwelle (>= bps löst diese Stufe aus)
    pub fraction: f64,   // Anteil der ursprünglichen Lot-Größe, der an dieser Stufe verkauft wird (nicht kumulativ)
}

impl Config {
    pub fn load(path: &std::path::Path) -> anyhow::Result<Self> {
        let raw = std::fs::read_to_string(path)?;
        let cfg: Config = toml::from_str(&raw)?;
        Ok(cfg)
    }
}
