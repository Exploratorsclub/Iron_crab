
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
}

impl Config {
    pub fn load(path: &std::path::Path) -> anyhow::Result<Self> {
        let raw = std::fs::read_to_string(path)?;
        let cfg: Config = toml::from_str(&raw)?;
        Ok(cfg)
    }
}
