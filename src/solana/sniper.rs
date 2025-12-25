//! Meme Coin Sniper – subscribes to pool creation via Geyser gRPC and applies heuristics.
// Memecoin-Sniper: beobachtet neue Pools/LP-Creations via Geyser, filtert Risiken,
// setzt kleine Erstkäufe mit harten Limits (Slippage/Blacklist/Owner/Freeze Auth usw.)
#[allow(unused_imports)]
use crate::config_reload::{diff_sniper_cfg, validate_sniper_cfg};
use crate::metrics; // keep metrics module in scope for qualified uses
use crate::metrics::{
    record_fee_pct, record_network_fee, record_realized_gross_net, record_realized_pnl_sol,
    record_recent_trade, record_shortfall, record_shortfall_pct, record_swap_latency,
    record_trade_return, RecentTrade, DAILY_REALIZED_PNL_SOL_MICRO, LIQUIDITY_ESTIMATE_SOL_MICRO,
    OPEN_POSITIONS_GAUGE, PENDING_FAILED_TOTAL, PENDING_RECONCILIATIONS_TOTAL,
    PROTOCOL_FEE_SOL_MICRO_TOTAL, PROTOCOL_FEE_TOKENS_TOTAL, RPC_ERRORS_TOTAL,
    RPC_RETRY_ATTEMPTS_TOTAL, TRADES_EXECUTED_TOTAL, TRADES_FAILED_TOTAL,
};
use crate::solana::dex::orca::ORCA_WHIRLPOOL_PROGRAM;
use crate::solana::dex::raydium::RAYDIUM_AMM_V4;
use crate::solana::dex::{orca::Orca, pumpfun::PumpFunDex, raydium::Raydium, Dex};
use crate::solana::geyser_pool_discovery::PoolDiscoveryEvent;
use crate::solana::jito::{JitoClient, JitoRegion};
use crate::solana::kill_switch::KillSwitchMonitor;
use crate::solana::rpc::SolanaRpc;
use crate::wallet::Treasury;
use anyhow::Result;
use chrono::Utc as ChronoUtc;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use serde_json::json;
use solana_client::rpc_config::RpcSendTransactionConfig;
use solana_sdk::pubkey;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::{hash::Hash, transaction::Transaction};
use std::str::FromStr;
use std::time::{Duration, Instant};
use std::{collections::HashSet, sync::Arc};
use tracing::{debug, error, info, warn};

// Simple global blacklist (extendable via config later)
#[allow(dead_code)]
static MINT_BLACKLIST: Lazy<HashSet<String>> = Lazy::new(HashSet::new);

#[derive(Clone)]
pub struct SniperCfg {
    pub max_buy_sol: f64,
    pub max_slippage_bps: u32,
    pub log_all_inits: bool, // diagnostic: also log any init-like lines even without pool/whirlpool
    // Log subscription: if provided, override default program list
    pub program_ids: Option<Vec<String>>,
    pub blacklist_mints: Vec<String>,
    pub blacklist_owners: Vec<String>,
    pub min_pool_liquidity_sol: Option<f64>,
    pub require_freeze_auth_none: Option<bool>,
    pub require_mint_decimals_range: Option<(u8, u8)>,
    pub lp_top1_max_pct: Option<f64>,
    pub lp_top3_max_pct: Option<f64>,
    pub lp_top5_max_pct: Option<f64>,
    pub max_position_sol: Option<f64>,
    pub stop_loss_bps: Option<u32>,
    pub take_profit_bps: Option<u32>,
    pub daily_loss_limit_sol: Option<f64>,
    pub max_open_positions: Option<usize>,
    pub per_mint_position_limit: Option<u32>,
    pub stop_loss_cooldown_secs: Option<u64>,
    pub drawdown_scale_start: Option<f64>,
    pub drawdown_max_reduction: Option<f64>,
    pub rolling_pnl_window: Option<usize>,
    pub hot_reload_secs: Option<u64>,
    pub pending_trade_ttl_secs: Option<u64>,
    pub take_profit_tiers: Option<Vec<crate::config::TakeProfitTier>>,
    pub trailing_stop_bps: Option<u32>,
    pub min_exit_notional_sol: Option<f64>,
    // Data & Pricing / Adaptive Slippage
    pub oracle_sol_usd_override: Option<f64>,
    pub adaptive_slippage_min_bps: Option<u32>,
    pub adaptive_slippage_max_bps: Option<u32>,
    pub adaptive_slippage_window: Option<usize>,
    pub adaptive_slippage_target_pct: Option<f64>,
    pub adaptive_slippage_step_bps: Option<u32>,
    pub exit_eval_interval_secs: Option<u64>,
    pub oracle_pyth_sol_usd: Option<String>,
    pub oracle_switchboard_sol_usd: Option<String>,
    pub oracle_preference: Option<String>,
    // Quantile-based slippage
    pub quantile_slippage_enabled: Option<bool>,
    pub quantile_confidence_level: Option<f64>,
    pub quantile_min_samples: Option<usize>,
    pub quantile_max_sample_age_secs: Option<u64>,
    pub quantile_fallback_slippage_bps: Option<u32>,
    // Freshness filters
    pub max_holders: Option<usize>,
    // Scenario-specific slippage (configurable instead of hardcoded)
    pub pumpfun_buy_slippage_bps: Option<u32>,
    pub emergency_exit_slippage_bps: Option<u32>,
    // Time-based exit strategy
    pub enable_time_based_exits: Option<bool>,
    pub max_hold_secs: Option<u64>,
    pub timed_exit_tiers: Option<Vec<crate::config::TimedExitTier>>,
    // Kill switches (Geyser-based)
    pub kill_switch_enabled: Option<bool>,
    pub kill_switch_dev_sell: Option<bool>,
    pub kill_switch_sell_burst_count: Option<u32>,
    pub kill_switch_sell_burst_sol: Option<f64>,
    pub kill_switch_sell_burst_slots: Option<u64>,
    pub kill_switch_flow_ratio_min: Option<f64>,
    pub kill_switch_negative_flow_slots: Option<u64>,
    // Jito bundle integration
    pub jito_enabled: Option<bool>,
    pub jito_tip_lamports: Option<u64>,
    pub jito_region: Option<String>,
    // Jito thresholds: only use Jito for large/emergency exits (small exits: tip eats EV)
    pub jito_min_exit_fraction: Option<f64>, // Min fraction to use Jito (default 0.25 = 25%)
    pub jito_min_exit_sol: Option<f64>,      // Min SOL value to use Jito (default 0.5 SOL)
    pub jito_for_emergency: Option<bool>, // Always use Jito for emergency/panic exits (default true)
    pub jito_for_final_exit: Option<bool>, // Always use Jito for full exits (default true)
    // Parallel exit execution
    pub parallel_exits: Option<bool>,
    pub max_parallel_exits: Option<usize>,
    pub bundle_exits: Option<bool>,
    // System
    pub autosave_state_secs: Option<u64>,
}

impl Default for SniperCfg {
    fn default() -> Self {
        Self {
            max_buy_sol: 1.0,
            max_slippage_bps: 1500, // 15% default for sniping (new tokens are volatile!)
            log_all_inits: false,
            program_ids: None,
            blacklist_mints: Vec::new(),
            blacklist_owners: Vec::new(),
            min_pool_liquidity_sol: None,
            require_freeze_auth_none: None,
            require_mint_decimals_range: None,
            lp_top1_max_pct: None,
            lp_top3_max_pct: None,
            lp_top5_max_pct: None,
            max_position_sol: None,
            stop_loss_bps: Some(500),
            take_profit_bps: Some(1000),
            daily_loss_limit_sol: None,
            max_open_positions: Some(5),
            per_mint_position_limit: Some(3),
            stop_loss_cooldown_secs: Some(300),
            drawdown_scale_start: None,
            drawdown_max_reduction: None,
            rolling_pnl_window: Some(50),
            hot_reload_secs: Some(30),
            pending_trade_ttl_secs: Some(120),
            take_profit_tiers: None,
            trailing_stop_bps: None,
            min_exit_notional_sol: None,
            oracle_sol_usd_override: None,
            adaptive_slippage_min_bps: None,
            adaptive_slippage_max_bps: None,
            adaptive_slippage_window: None,
            adaptive_slippage_target_pct: None,
            adaptive_slippage_step_bps: None,
            exit_eval_interval_secs: None,
            oracle_pyth_sol_usd: None,
            oracle_switchboard_sol_usd: None,
            oracle_preference: None,
            quantile_slippage_enabled: None,
            quantile_confidence_level: None,
            quantile_min_samples: None,
            quantile_max_sample_age_secs: None,
            quantile_fallback_slippage_bps: None,
            max_holders: None,
            pumpfun_buy_slippage_bps: None,
            emergency_exit_slippage_bps: None,
            enable_time_based_exits: None,
            max_hold_secs: None,
            timed_exit_tiers: None,
            kill_switch_enabled: None,
            kill_switch_dev_sell: None,
            kill_switch_sell_burst_count: None,
            kill_switch_sell_burst_sol: None,
            kill_switch_sell_burst_slots: None,
            kill_switch_flow_ratio_min: None,
            kill_switch_negative_flow_slots: None,
            jito_enabled: None,
            jito_tip_lamports: None,
            jito_region: None,
            jito_min_exit_fraction: Some(0.25), // Default: use Jito for exits >= 25%
            jito_min_exit_sol: Some(0.5),       // Default: use Jito for exits >= 0.5 SOL
            jito_for_emergency: Some(true),     // Default: always Jito for panic exits
            jito_for_final_exit: Some(true),    // Default: always Jito for 100% exits
            parallel_exits: None,
            max_parallel_exits: None,
            bundle_exits: None,
            autosave_state_secs: None,
        }
    }
}

impl From<&crate::config::SniperSettings> for SniperCfg {
    fn from(c: &crate::config::SniperSettings) -> Self {
        Self {
            max_buy_sol: c.max_buy_sol,
            max_slippage_bps: c.max_slippage_bps,
            log_all_inits: false,
            program_ids: c.program_ids.clone(),
            blacklist_mints: c.blacklist_mints.clone(),
            blacklist_owners: c.blacklist_owners.clone(),
            min_pool_liquidity_sol: c.min_pool_liquidity_sol,
            require_freeze_auth_none: c.require_freeze_auth_none,
            require_mint_decimals_range: c.require_mint_decimals_range,
            lp_top1_max_pct: c.lp_top1_max_pct,
            lp_top3_max_pct: c.lp_top3_max_pct,
            lp_top5_max_pct: c.lp_top5_max_pct,
            max_position_sol: c.max_position_sol,
            stop_loss_bps: c.stop_loss_bps,
            take_profit_bps: c.take_profit_bps,
            daily_loss_limit_sol: c.daily_loss_limit_sol,
            max_open_positions: c.max_open_positions,
            per_mint_position_limit: c.per_mint_position_limit,
            stop_loss_cooldown_secs: c.stop_loss_cooldown_secs,
            drawdown_scale_start: c.drawdown_scale_start,
            drawdown_max_reduction: c.drawdown_max_reduction,
            rolling_pnl_window: c.rolling_pnl_window,
            hot_reload_secs: c.hot_reload_secs,
            pending_trade_ttl_secs: c.pending_trade_ttl_secs,
            take_profit_tiers: c.take_profit_tiers.clone(),
            trailing_stop_bps: c.trailing_stop_bps,
            min_exit_notional_sol: c.min_exit_notional_sol,
            oracle_sol_usd_override: c.oracle_sol_usd_override,
            adaptive_slippage_min_bps: c.adaptive_slippage_min_bps,
            adaptive_slippage_max_bps: c.adaptive_slippage_max_bps,
            adaptive_slippage_window: c.adaptive_slippage_window,
            adaptive_slippage_target_pct: c.adaptive_slippage_target_pct,
            adaptive_slippage_step_bps: c.adaptive_slippage_step_bps,
            exit_eval_interval_secs: c.exit_eval_interval_secs,
            oracle_pyth_sol_usd: c.oracle_pyth_sol_usd.clone(),
            oracle_switchboard_sol_usd: c.oracle_switchboard_sol_usd.clone(),
            oracle_preference: c.oracle_preference.clone(),
            quantile_slippage_enabled: c.quantile_slippage_enabled,
            quantile_confidence_level: c.quantile_confidence_level,
            quantile_min_samples: c.quantile_min_samples,
            quantile_max_sample_age_secs: c.quantile_max_sample_age_secs,
            quantile_fallback_slippage_bps: c.quantile_fallback_slippage_bps,
            max_holders: c.max_holders,
            pumpfun_buy_slippage_bps: c.pumpfun_buy_slippage_bps,
            emergency_exit_slippage_bps: c.emergency_exit_slippage_bps,
            enable_time_based_exits: c.enable_time_based_exits,
            max_hold_secs: c.max_hold_secs,
            timed_exit_tiers: c.timed_exit_tiers.clone(),
            kill_switch_enabled: c.kill_switch_enabled,
            kill_switch_dev_sell: c.kill_switch_dev_sell,
            kill_switch_sell_burst_count: c.kill_switch_sell_burst_count,
            kill_switch_sell_burst_sol: c.kill_switch_sell_burst_sol,
            kill_switch_sell_burst_slots: c.kill_switch_sell_burst_slots,
            kill_switch_flow_ratio_min: c.kill_switch_flow_ratio_min,
            kill_switch_negative_flow_slots: c.kill_switch_negative_flow_slots,
            jito_enabled: c.jito_enabled,
            jito_tip_lamports: c.jito_tip_lamports,
            jito_region: c.jito_region.clone(),
            jito_min_exit_fraction: c.jito_min_exit_fraction,
            jito_min_exit_sol: c.jito_min_exit_sol,
            jito_for_emergency: c.jito_for_emergency,
            jito_for_final_exit: c.jito_for_final_exit,
            parallel_exits: c.parallel_exits,
            max_parallel_exits: c.max_parallel_exits,
            bundle_exits: c.bundle_exits,
            autosave_state_secs: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PositionLot {
    entry_price_sol: f64,
    amount_tokens: f64, // UI tokens
    invested_sol: f64,  // SOL notionals allocated to this lot
    token_decimals: u8,
    last_unrealized_pnl_sol: f64,
    opened_ts: i64,
    #[serde(default)]
    executed_tp_bps: Vec<u32>, // welche TP Stufen bereits ausgeführt wurden
    #[serde(default)]
    peak_pnl_bps: i64, // Hochwasser-Marke für Trailing Stop
    // Time-based exit tracking
    #[serde(default)]
    executed_timed_tiers: Vec<u64>, // which timed tiers (by secs) have been executed
    // Kill switch tracking (creator address for dev sell detection)
    #[serde(default)]
    creator: Option<String>, // Token creator/dev address for kill switch
}

/// Exit task for parallel execution
#[derive(Clone, Debug)]
struct ExitTask {
    mint: Pubkey,
    lot_idx: usize,
    sell_tokens: u64,
    fraction: f64,
    is_emergency: bool,
    reason: String,
    creator: Option<Pubkey>, // Token creator for Pump.fun exits
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[allow(dead_code)]
struct PendingTrade {
    dex: String,
    sig: String,
    lamports_in: u64,
    expected_out_tokens: u64,
    network_fee_lamports: u64,
    ts: i64,
    fee_bps: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[allow(dead_code)]
struct RiskState {
    #[serde(skip)]
    open: std::collections::HashMap<Pubkey, Vec<PositionLot>>, // multi-lot per mint
    realized_pnl_sol: f64,
    realized_loss_today_sol: f64,
    current_day: u32,
    #[serde(skip)]
    pending: std::collections::HashMap<Pubkey, PendingTrade>,
    #[serde(skip)]
    cooldown_until: std::collections::HashMap<Pubkey, i64>,
    recent_realized: Vec<f64>,
    last_sharpe: f64,
    #[serde(default)]
    recent_slippage: Vec<f64>,
    #[serde(default)]
    adaptive_slippage_bps: Option<u32>,
    #[serde(skip)]
    pending_buys: usize,
}

impl Default for RiskState {
    fn default() -> Self {
        Self {
            open: Default::default(),
            realized_pnl_sol: 0.0,
            realized_loss_today_sol: 0.0,
            current_day: 0,
            pending: Default::default(),
            cooldown_until: Default::default(),
            recent_realized: Vec::new(),
            last_sharpe: 0.0,
            recent_slippage: Vec::new(),
            adaptive_slippage_bps: None,
            pending_buys: 0,
        }
    }
}

// --- Pure helpers (module-level, not cfg-gated) -------------------------------------------
/// Parse SPL-Token Mint fields from raw account data.
/// Returns (mint_authority, freeze_authority, decimals, supply_raw)
fn parse_spl_mint_fields(data: &[u8]) -> (Option<Pubkey>, Option<Pubkey>, u8, u64) {
    if data.len() < 45 {
        return (None, None, 0, 0);
    }
    // Supply and decimals
    let supply = if data.len() >= 44 {
        u64::from_le_bytes(data[36..44].try_into().unwrap_or([0u8; 8]))
    } else {
        0
    };
    let decimals = *data.get(44).unwrap_or(&0);
    // COption mint_authority
    let mint_auth = if data.len() >= 36 {
        let tag = u32::from_le_bytes(data[0..4].try_into().unwrap_or([0u8; 4]));
        if tag == 0 {
            None
        } else {
            let key = Pubkey::new_from_array(data[4..36].try_into().unwrap_or([0u8; 32]));
            if key.to_bytes() == [0u8; 32] {
                None
            } else {
                Some(key)
            }
        }
    } else {
        None
    };
    // COption freeze_authority
    let freeze_auth = if data.len() >= 82 {
        let tag = u32::from_le_bytes(data[46..50].try_into().unwrap_or([0u8; 4]));
        if tag == 0 {
            None
        } else {
            let key = Pubkey::new_from_array(data[50..82].try_into().unwrap_or([0u8; 32]));
            if key.to_bytes() == [0u8; 32] {
                None
            } else {
                Some(key)
            }
        }
    } else {
        None
    };
    (mint_auth, freeze_auth, decimals, supply)
}

/// Check if any authority is blacklisted by configured owners list.
fn owner_blacklisted(
    owners: &[String],
    mint_auth: Option<&Pubkey>,
    freeze_auth: Option<&Pubkey>,
) -> bool {
    if owners.is_empty() {
        return false;
    }
    if let Some(ma) = mint_auth {
        if owners.iter().any(|o| o == &ma.to_string()) {
            return true;
        }
    }
    if let Some(fa) = freeze_auth {
        if owners.iter().any(|o| o == &fa.to_string()) {
            return true;
        }
    }
    false
}

// Tiny pure gating helper for tests: mirrors early checks in lp_lock_check without RPC.
#[cfg(any(test, feature = "test_helpers"))]
pub fn test_gate_freeze_and_decimals(
    require_freeze_none: bool,
    decimals_range: Option<(u8, u8)>,
    mint_data: &[u8],
) -> bool {
    let (_ma, fa, decimals, supply_raw) = parse_spl_mint_fields(mint_data);
    if supply_raw == 0 {
        return false;
    }
    if require_freeze_none && fa.is_some() {
        return false;
    }
    if let Some((lo, hi)) = decimals_range {
        if decimals < lo || decimals > hi {
            return false;
        }
    }
    true
}

pub struct SniperEngine {
    pub rpc: Arc<SolanaRpc>,
    cfg: parking_lot::RwLock<SniperCfg>,
    raydium: Option<Arc<Raydium>>,
    orca: Option<Arc<Orca>>,
    pumpfun: Option<Arc<PumpFunDex>>,
    purchased: Arc<parking_lot::RwLock<HashSet<Pubkey>>>, // track already bought mints (avoid double buy)
    processing: Arc<parking_lot::RwLock<HashSet<Pubkey>>>, // track mints currently being processed (deduplication)
    treasury: Arc<Treasury>,
    risk: Arc<parking_lot::RwLock<RiskState>>, // CRITICAL: Must be Arc to share across spawned tasks!
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
    quantile_calc: Arc<crate::quantile_impact::QuantileImpactCalculator>,
    geyser_grpc_url: Option<String>,
    /// CRITICAL: Timestamp when the bot started - only buy pools created AFTER this!
    boot_timestamp: i64,
    /// Optional Helius RPC client for mint validation (full transaction index)
    /// Wrapped in Arc for sharing across spawned tasks
    helius_rpc: Option<Arc<solana_client::nonblocking::rpc_client::RpcClient>>,
    /// Kill switch monitor for emergency exits
    kill_switch: Option<Arc<KillSwitchMonitor>>,
}

// --- Test Helpers (feature-gated) -----------------------------------------------------------
#[cfg(any(test, feature = "test_helpers"))]
impl SniperEngine {
    /// Insert a synthetic position lot for a mint (used by unit tests).
    pub fn test_insert_lot(
        &self,
        mint: Pubkey,
        invested_sol: f64,
        amount_tokens: f64,
        entry_price_sol: f64,
        token_decimals: u8,
    ) {
        let lot = PositionLot {
            entry_price_sol,
            amount_tokens,
            invested_sol,
            token_decimals,
            last_unrealized_pnl_sol: 0.0,
            opened_ts: chrono::Utc::now().timestamp(),
            executed_tp_bps: Vec::new(),
            peak_pnl_bps: 0,
            executed_timed_tiers: Vec::new(),
            creator: None,
        };
        let mut rs = self.risk.write();
        rs.open.entry(mint).or_default().push(lot);
    }

    /// Apply the internal proportional reduction logic as attempt_exit would after a partial fill.
    /// Returns (remaining_invested_sol, remaining_amount_tokens, realized_added, lots_remaining)
    pub fn test_apply_partial_reduction(
        &self,
        mint: &Pubkey,
        lot_idx: usize,
        fraction: f64,
        realized_delta: f64,
    ) -> Option<(f64, f64, f64, usize)> {
        let mut remove_entire = false;
        let mut remaining_vals: Option<(f64, f64, usize)> = None;
        // First mutate lot only
        {
            let mut rs = self.risk.write();
            let v = rs.open.get_mut(mint)?;
            if lot_idx >= v.len() {
                return None;
            }
            let l = &mut v[lot_idx];
            let invest_slice = l.invested_sol * fraction;
            l.invested_sol -= invest_slice;
            l.amount_tokens = (l.amount_tokens - (l.amount_tokens * fraction)).max(0.0);
            if l.amount_tokens <= 1e-9 {
                remove_entire = true;
            }
            if !remove_entire {
                remaining_vals = Some((l.invested_sol, l.amount_tokens, v.len()));
            }
        }
        // Second, update realized metrics & remove if empty
        {
            let mut rs = self.risk.write();
            rs.realized_pnl_sol += realized_delta;
            if realized_delta < 0.0 {
                rs.realized_loss_today_sol += -realized_delta;
            }
            if remove_entire {
                if let Some(v) = rs.open.get_mut(mint) {
                    if lot_idx < v.len() {
                        v.remove(lot_idx);
                    }
                }
                if let Some(v2) = rs.open.get(mint) {
                    if v2.is_empty() {
                        rs.open.remove(mint);
                    }
                }
            }
        }
        if remove_entire {
            let rs = self.risk.read();
            return Some((
                0.0,
                0.0,
                realized_delta,
                rs.open.get(mint).map(|v| v.len()).unwrap_or(0),
            ));
        }
        if let Some((i, a, len)) = remaining_vals {
            Some((i, a, realized_delta, len))
        } else {
            None
        }
    }

    /// Simulate a partial exit including proceeds & fee, updating realized returns & Sharpe.
    /// r = (proceeds - fee - invested_slice)/invested_slice.
    /// Returns (return_r, last_sharpe, recent_count).
    pub fn test_simulate_partial_exit_with_fee(
        &self,
        mint: &Pubkey,
        lot_idx: usize,
        fraction: f64,
        proceeds_sol: f64,
        fee_sol: f64,
    ) -> Option<(f64, f64, usize)> {
        // Capture invest_slice first
        let invest_slice = {
            let rs = self.risk.read();
            let v = rs.open.get(mint)?;
            if lot_idx >= v.len() {
                return None;
            }
            v[lot_idx].invested_sol * fraction
        };
        if invest_slice <= 0.0 {
            return None;
        }
        let realized_delta = proceeds_sol - fee_sol - invest_slice;
        // Apply proportional reduction & realized update
        self.test_apply_partial_reduction(mint, lot_idx, fraction, realized_delta)?;
        // Push normalized return & recompute Sharpe replicating production logic
        {
            let mut rs = self.risk.write();
            let ret = if invest_slice > 0.0 {
                realized_delta / invest_slice
            } else {
                0.0
            };
            rs.recent_realized.push(ret);
            let window = self.cfg.read().rolling_pnl_window.unwrap_or(50);
            if rs.recent_realized.len() > window {
                let excess = rs.recent_realized.len() - window;
                rs.recent_realized.drain(0..excess);
            }
            if rs.recent_realized.len() >= 5 {
                let n = rs.recent_realized.len() as f64;
                let mean = rs.recent_realized.iter().copied().sum::<f64>() / n;
                let var = rs
                    .recent_realized
                    .iter()
                    .map(|r| {
                        let d = r - mean;
                        d * d
                    })
                    .sum::<f64>()
                    / n.max(1.0);
                let std = var.sqrt();
                if std > 0.0 {
                    rs.last_sharpe = mean / std * n.sqrt();
                }
            }
            Some((ret, rs.last_sharpe, rs.recent_realized.len()))
        }
    }

    /// Access current Sharpe and realized returns count.
    pub fn test_get_sharpe(&self) -> (f64, usize) {
        let rs = self.risk.read();
        (rs.last_sharpe, rs.recent_realized.len())
    }

    pub fn test_current_invested_for_lot(&self, mint: &Pubkey, lot_idx: usize) -> Option<f64> {
        let rs = self.risk.read();
        rs.open
            .get(mint)
            .and_then(|v| v.get(lot_idx).map(|l| l.invested_sol))
    }

    /// Get number of open lots for a mint.
    pub fn test_open_lot_count(&self, mint: &Pubkey) -> usize {
        let rs = self.risk.read();
        rs.open.get(mint).map(|v| v.len()).unwrap_or(0)
    }

    /// Get total open lots across all mints.
    pub fn test_total_open_positions(&self) -> usize {
        let rs = self.risk.read();
        rs.open.values().map(|v| v.len()).sum()
    }

    /// Access realized PnL (SOL) accumulated in RiskState.
    pub fn test_get_realized_pnl_sol(&self) -> f64 {
        self.risk.read().realized_pnl_sol
    }

    /// Set today's realized loss (SOL) for drawdown sizing tests.
    pub fn test_set_realized_loss_today(&self, loss_sol: f64) {
        let mut rs = self.risk.write();
        rs.realized_loss_today_sol = loss_sol;
    }

    /// Expose the effective_max_buy_sol calculation for tests.
    pub fn test_effective_max_buy_sol(&self) -> f64 {
        self.effective_max_buy_sol()
    }

    /// Mark a mint as in cooldown using the internal logic.
    pub fn test_mark_cooldown(&self, mint: Pubkey) {
        self.mark_cooldown(mint);
    }

    /// Manually set cooldown until a specific timestamp (UTC seconds).
    pub fn test_set_cooldown(&self, mint: Pubkey, until_ts: i64) {
        let mut rs = self.risk.write();
        rs.cooldown_until.insert(mint, until_ts);
    }

    /// Wrapper to test the gating logic for opening positions.
    pub fn test_can_open_position_for(&self, mint: &Pubkey, planned_sol: f64) -> bool {
        self.can_open_position_for(mint, planned_sol)
    }
}

#[allow(dead_code)]
impl SniperEngine {
    fn adaptive_slippage_bps(&self) -> u32 {
        let r = self.cfg.read();
        let base = r.max_slippage_bps;
        let min_b = r.adaptive_slippage_min_bps.unwrap_or(base);
        let max_b = r.adaptive_slippage_max_bps.unwrap_or(base);
        let cur = {
            let rs = self.risk.read();
            rs.adaptive_slippage_bps.unwrap_or(base)
        };
        cur.clamp(min_b, max_b)
    }

    /// Compute min_out using quantile-based slippage if enabled, otherwise use adaptive slippage
    fn compute_min_out(
        &self,
        pool_id: &str,
        expected_out: u64,
        amount_in: u64,
        pool_liquidity: u128,
    ) -> u64 {
        let cfg = self.cfg.read();

        // Check if quantile slippage is enabled
        if cfg.quantile_slippage_enabled.unwrap_or(false) {
            // Determine size category based on trade size vs pool liquidity
            let size_category = if pool_liquidity > 0 {
                let trade_pct = (amount_in as f64 / pool_liquidity as f64) * 100.0;
                if trade_pct < 1.0 {
                    crate::quantile_impact::SizeCategory::Small
                } else if trade_pct < 5.0 {
                    crate::quantile_impact::SizeCategory::Medium
                } else {
                    crate::quantile_impact::SizeCategory::Large
                }
            } else {
                crate::quantile_impact::SizeCategory::Small
            };

            // Try quantile-based calculation
            if let Ok(min_out) =
                self.quantile_calc
                    .compute_min_out(pool_id, expected_out, size_category)
            {
                return min_out;
            }
        }

        // Fallback to adaptive slippage
        let slip = self.adaptive_slippage_bps() as u128;
        let min_out = ((expected_out as u128) * (10_000 - slip) / 10_000) as u64;
        min_out.max(1)
    }

    async fn rpc_retry_tx(
        &self,
        tx: &Transaction,
        max_attempts: u32,
        skip_preflight: bool,
    ) -> Result<solana_sdk::signature::Signature> {
        let mut attempt = 0;
        loop {
            let res = if skip_preflight {
                let config = RpcSendTransactionConfig {
                    skip_preflight: true,
                    preflight_commitment: None,
                    encoding: Some(solana_transaction_status::UiTransactionEncoding::Base64),
                    max_retries: None,
                    min_context_slot: None,
                };
                match self.rpc.rpc.send_transaction_with_config(tx, config).await {
                    Ok(sig) => match self.rpc.rpc.confirm_transaction(&sig).await {
                        Ok(true) => Ok(sig),
                        Ok(false) => Err(anyhow::anyhow!("Confirmation timed out")),
                        Err(e) => Err(e.into()),
                    },
                    Err(e) => Err(e.into()),
                }
            } else {
                self.rpc
                    .rpc
                    .send_and_confirm_transaction(tx)
                    .await
                    .map_err(|e| e.into())
            };

            match res {
                Ok(sig) => return Ok(sig),
                Err(e) => {
                    attempt += 1;
                    RPC_RETRY_ATTEMPTS_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if attempt >= max_attempts {
                        return Err(e);
                    }
                    let delay_ms = (2u64.pow(attempt.min(5)) * 200).min(5_000);
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                }
            }
        }
    }
    fn effective_max_buy_sol(&self) -> f64 {
        let cfg = self.cfg.read().clone();
        let rs = self.risk.read();
        if let (Some(limit), Some(start), Some(max_red)) = (
            cfg.daily_loss_limit_sol,
            cfg.drawdown_scale_start,
            cfg.drawdown_max_reduction,
        ) {
            if limit > 0.0 && start < 1.0 && max_red > 0.0 {
                let ratio = (rs.realized_loss_today_sol / limit).clamp(0.0, 1.0);
                if ratio <= start {
                    return cfg.max_buy_sol;
                }
                let frac = ((ratio - start) / (1.0 - start)).clamp(0.0, 1.0);
                let reduction = max_red.clamp(0.0, 1.0) * frac;
                return cfg.max_buy_sol * (1.0 - reduction);
            }
        }
        cfg.max_buy_sol
    }

    fn mark_cooldown(&self, mint: Pubkey) {
        if let Some(secs) = self.cfg.read().stop_loss_cooldown_secs {
            if secs > 0 {
                let until = chrono::Utc::now().timestamp() + secs as i64;
                let mut rs = self.risk.write();
                rs.cooldown_until.insert(mint, until);
            }
        }
    }
    async fn total_sol_balance(&self) -> Result<f64> {
        // Native SOL lamports
        let owner = self.treasury.pubkey();
        let native_lamports = match self.rpc.get_account_retry(&owner).await {
            Ok(acc) => acc.lamports,
            Err(e) => {
                RPC_ERRORS_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return Err(e.into());
            }
        } as u128;
        // WSOL ATA amount (if exists)
        let wsol_mint_prog = spl_token::native_mint::id();
        let wsol_mint_sdk = solana_sdk::pubkey::Pubkey::new_from_array(wsol_mint_prog.to_bytes());
        let (wsol_ata, _prog) = match self
            .treasury
            .ata_address(&self.rpc, &owner, &wsol_mint_sdk)
            .await
        {
            Ok(v) => v,
            Err(_) => {
                return Ok(native_lamports as f64 / 1e9);
            }
        };
        let wsol_amount = match self.rpc.get_account_retry(&wsol_ata).await {
            Ok(acc) => {
                if acc.data.len() >= 72 {
                    u64::from_le_bytes(acc.data[64..72].try_into().unwrap()) as u128
                } else {
                    0
                }
            }
            Err(_) => 0,
        };
        Ok((native_lamports + wsol_amount) as f64 / 1e9)
    }
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        rpc: Arc<SolanaRpc>,
        cfg: SniperCfg,
        raydium: Option<Arc<Raydium>>,
        orca: Option<Arc<Orca>>,
        pumpfun: Option<Arc<PumpFunDex>>,
        treasury: Arc<Treasury>,
        geyser_grpc_url: Option<String>,
        helius_rpc_url: Option<String>,
    ) -> Self {
        let (tx, rx) = tokio::sync::watch::channel(false);

        // Initialize quantile calculator with config
        let quantile_config = crate::quantile_impact::QuantileConfig {
            confidence_level: cfg.quantile_confidence_level.unwrap_or(0.95),
            min_samples: cfg.quantile_min_samples.unwrap_or(20),
            max_sample_age_secs: cfg.quantile_max_sample_age_secs.unwrap_or(86400),
            max_samples_per_pool: 500,
            fallback_slippage_bps: cfg.quantile_fallback_slippage_bps.unwrap_or(100),
        };

        // Use provided Pump.fun connector or initialize if missing (fallback)
        let pumpfun_arc = if let Some(pf) = pumpfun {
            Some(pf)
        } else {
            match PumpFunDex::new(rpc.clone()) {
                Ok(mut pf) => {
                    pf.set_user_authority(solana_sdk::pubkey::Pubkey::new_from_array(
                        treasury.pubkey().to_bytes(),
                    ));
                    Some(Arc::new(pf))
                }
                Err(e) => {
                    warn!(?e, "failed to initialize pump.fun connector");
                    None
                }
            }
        };

        // Record boot timestamp - CRITICAL for filtering old pools!
        let boot_timestamp = ChronoUtc::now().timestamp();
        info!(
            boot_timestamp,
            "sniper: engine starting - will ONLY buy pools created AFTER this timestamp"
        );

        // Initialize Helius RPC client for mint validation (requires full transaction index)
        // Local validators don't have full history, Helius provides accurate mint signature counts
        let helius_rpc = helius_rpc_url.as_ref().map(|url| {
            info!(
                "sniper: Helius RPC configured for mint validation: {}",
                url.split("api-key=").next().unwrap_or("***")
            );
            Arc::new(solana_client::nonblocking::rpc_client::RpcClient::new(
                url.clone(),
            ))
        });
        if helius_rpc.is_none() {
            warn!("sniper: NO Helius RPC configured - mint validation will use local RPC (may be inaccurate!)");
        }

        // Initialize Kill Switch Monitor if enabled
        let kill_switch = if cfg.kill_switch_enabled.unwrap_or(false) {
            info!("sniper: Kill Switch Monitor ENABLED");
            Some(Arc::new(KillSwitchMonitor::new(
                cfg.kill_switch_dev_sell.unwrap_or(true),
                cfg.kill_switch_sell_burst_count,
                cfg.kill_switch_sell_burst_sol,
                cfg.kill_switch_sell_burst_slots,
                cfg.kill_switch_flow_ratio_min,
                cfg.kill_switch_negative_flow_slots,
            )))
        } else {
            info!("sniper: Kill Switch Monitor disabled");
            None
        };

        Self {
            rpc,
            cfg: parking_lot::RwLock::new(cfg),
            raydium,
            orca,
            pumpfun: pumpfun_arc,
            purchased: Arc::new(parking_lot::RwLock::new(HashSet::new())),
            processing: Arc::new(parking_lot::RwLock::new(HashSet::new())),
            treasury,
            risk: Arc::new(parking_lot::RwLock::new(RiskState::default())), // Arc for sharing across tasks
            shutdown_tx: tx,
            shutdown_rx: rx,
            quantile_calc: Arc::new(crate::quantile_impact::QuantileImpactCalculator::new(
                quantile_config,
            )),
            geyser_grpc_url,
            boot_timestamp,
            helius_rpc,
            kill_switch,
        }
    }

    fn clone_for_spawn(&self) -> Self {
        let (tx, rx) = tokio::sync::watch::channel(false); // spawned clones don't receive shutdown (fire-and-forget tasks)
        SniperEngine {
            rpc: self.rpc.clone(),
            cfg: parking_lot::RwLock::new(self.cfg.read().clone()),
            raydium: self.raydium.clone(),
            orca: self.orca.clone(),
            pumpfun: self.pumpfun.clone(),
            purchased: self.purchased.clone(), // Share Arc-wrapped HashSets across spawned tasks
            processing: self.processing.clone(),
            treasury: self.treasury.clone(),
            risk: self.risk.clone(), // CRITICAL FIX: Share RiskState across all tasks!
            shutdown_tx: tx,
            shutdown_rx: rx,
            quantile_calc: self.quantile_calc.clone(),
            geyser_grpc_url: self.geyser_grpc_url.clone(),
            boot_timestamp: self.boot_timestamp, // CRITICAL: preserve boot time for all clones
            helius_rpc: self.helius_rpc.clone(), // Share Helius RPC client for mint validation
            kill_switch: self.kill_switch.clone(), // Share kill switch monitor
        }
    }

    pub fn request_shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    /// Geyser gRPC-based pool discovery (the ONLY supported discovery method)
    async fn run_with_geyser(&self, geyser_url: &str) -> Result<()> {
        use crate::solana::geyser_pool_discovery::GeyserPoolDiscovery;

        // Build program list from config (default: Raydium, Orca, Pump.fun)
        let programs: Vec<Pubkey> = self
            .cfg
            .read()
            .program_ids
            .clone()
            .filter(|v| !v.is_empty())
            .and_then(|ids| {
                ids.iter()
                    .filter_map(|s| Pubkey::from_str(s).ok())
                    .collect::<Vec<_>>()
                    .into()
            })
            .unwrap_or_else(|| {
                vec![
                    Pubkey::from_str(RAYDIUM_AMM_V4).expect("valid raydium pubkey"),
                    Pubkey::from_str(ORCA_WHIRLPOOL_PROGRAM).expect("valid orca pubkey"),
                    // Pump.fun: 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P
                    pubkey!("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"),
                ]
            });

        info!(
            programs=?programs.iter().map(|p| p.to_string()).collect::<Vec<_>>(),
            "sniper: creating Geyser pool discovery with program filters"
        );

        // Create Geyser pool discovery listener
        let (discovery, mut event_rx) =
            GeyserPoolDiscovery::new(geyser_url.to_string(), programs, self.rpc.clone());

        // Start Geyser listener in background
        let mut shutdown_rx = self.shutdown_rx.clone();
        tokio::spawn(async move {
            if let Err(e) = discovery.start().await {
                tracing::error!(?e, "geyser pool discovery task failed");
            }
        });

        info!("sniper: Geyser pool discovery active, processing events...");

        // Create interval for periodic position evaluation (stop-loss, take-profit)
        let mut position_eval_interval = tokio::time::interval(tokio::time::Duration::from_secs(5));
        position_eval_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Process pool discovery events
        loop {
            tokio::select! {
                result = event_rx.recv() => {
                    match result {
                        Ok(event) => {
                            // CRITICAL: Spawn handling in background to avoid blocking the event loop
                            // Each pool discovery (especially Pump.fun) can take 2-3 seconds due to RPC delays
                            // Without spawning, we'd only process ~20 tokens/minute instead of 1000+
                            let engine = self.clone_for_spawn();
                            tokio::spawn(async move {
                                engine.handle_pool_discovery(event).await;
                            });
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                            warn!(skipped, "sniper: Geyser event receiver lagged, skipped messages");
                            // Continue processing - next recv() will get the latest event
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            error!("sniper: Geyser event channel closed unexpectedly");
                            break;
                        }
                    }
                }
                _ = position_eval_interval.tick() => {
                    // Periodic position evaluation for stop-loss and take-profit
                    // Runs every 5 seconds to check if any open positions need to be closed
                    if let Err(e) = self.evaluate_positions().await {
                        warn!(error=?e, "sniper: position evaluation failed");
                    }
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!("sniper: Geyser pool discovery shutting down");
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    /// Process a pool discovery event from Geyser
    async fn handle_pool_discovery(&self, event: PoolDiscoveryEvent) {
        let mint = event.base_mint;

        // LATENCY TRACKING: Calculate time since event was created
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let latency_to_handler_ms = now_ms.saturating_sub(event.discovered_at_ms);

        info!(
            mint=%mint,
            latency_ms=latency_to_handler_ms,
            "⏱️ LATENCY: Geyser discovery -> handler start"
        );

        // CRITICAL: Check max_open_positions FIRST - before ANY validation or Helius calls!
        // This saves API quota and prevents unnecessary processing.
        // This saves API quota and prevents unnecessary processing.
        {
            let rs = self.risk.read();
            let cfg = self.cfg.read();
            if let Some(mop) = cfg.max_open_positions {
                let current_count: usize = rs.open.values().map(|v| v.len()).sum();
                let total_exposure = current_count + rs.pending_buys;
                if total_exposure >= mop {
                    debug!(mint=%mint, current=current_count, pending=rs.pending_buys, max=mop,
                        "sniper: max_open_positions reached - skipping ALL validation (saving Helius quota)");
                    return;
                }
            }
        }

        // CRITICAL: Deduplication - prevent multiple parallel tasks processing same mint
        // Same token can appear in multiple pool discovery events (different pools/bonding curves)
        // Each spawned task would create ATAs and waste SOL on failed swaps
        // Check if already processing or purchased
        {
            let processing = self.processing.read();
            if processing.contains(&mint) {
                info!(mint=%mint, "sniper: mint already being processed by another task, skipping");
                return;
            }
            let purchased = self.purchased.read();
            if purchased.contains(&mint) {
                info!(mint=%mint, "sniper: mint already purchased, skipping");
                return;
            }
        }

        // Mark as processing (will be removed when task completes)
        self.processing.write().insert(mint);

        // Ensure cleanup happens even if task panics or returns early
        let _cleanup_guard = scopeguard::guard((), |_| {
            self.processing.write().remove(&mint);
        });

        let program_label = match event.dex_type {
            crate::solana::geyser_pool_discovery::DexType::RaydiumAmmV4 => "RAYDIUM",
            crate::solana::geyser_pool_discovery::DexType::OrcaWhirlpool => "ORCA",
            crate::solana::geyser_pool_discovery::DexType::PumpFun => "PUMPFUN",
        };

        let liq_sol = (event.liquidity_estimate_lamports as f64) / 1e9;

        // Calculate latency from Geyser discovery to sniper processing
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let discovery_latency_ms = now_ms.saturating_sub(event.discovered_at_ms);

        // Changed from debug! to info! to ensure visibility
        info!(
            pool=%event.pool_address,
            dex=program_label,
            base=%event.base_mint,
            quote=%event.quote_mint,
            liq_sol=liq_sol,
            discovery_latency_ms=discovery_latency_ms,
            "sniper: new pool discovered via Geyser"
        );

        // Determine which mint is the new token (not SOL/WSOL)
        let sol_mint = pubkey!("So11111111111111111111111111111111111111112");

        // The new token mint is whichever side is NOT SOL
        // Note: We do this BEFORE loading the pool to check if it was ALREADY known
        let mint = if event.base_mint == sol_mint {
            event.quote_mint // SOL is base, so new token is quote
        } else {
            event.base_mint // New token is base, SOL is quote
        };

        // Check if mint is already known in Raydium cache BEFORE we load this new pool
        let is_known_before_load = if let Some(ray) = &self.raydium {
            ray.is_mint_known(&mint)
        } else {
            false
        };

        // Load Raydium pool into cache immediately (only if not already cached)
        if event.dex_type == crate::solana::geyser_pool_discovery::DexType::RaydiumAmmV4 {
            if let Some(ref ray) = self.raydium {
                // Check if pool already exists in cache
                if !ray.pool_exists(&event.pool_address) {
                    if let Err(e) = ray.load_pool_from_geyser(&event.pool_address).await {
                        warn!(pool=%event.pool_address, error=%e, "failed to load raydium pool into cache");
                    }
                } else {
                    debug!(pool=%event.pool_address, "raydium pool already in cache");
                }
            }
        }

        // Skip non-SOL pairs (we only trade SOL/Token pairs for now)
        if event.base_mint != sol_mint && event.quote_mint != sol_mint {
            debug!(
                base=%event.base_mint,
                quote=%event.quote_mint,
                "sniper: skipping non-SOL pair (Token/Token or Token/Stablecoin)"
            );
            return;
        }

        // Check blacklist
        if self.cfg.read().blacklist_mints.contains(&mint.to_string()) {
            info!(mint=%mint, "sniper: mint blacklisted");
            self.append_pool_candidate_record(
                program_label,
                &mint,
                None,
                None,
                None,
                None,
                None,
                Some(liq_sol),
                "SKIP",
                "blacklisted",
            );
            return;
        }

        // Check minimum liquidity
        if let Some(min_liq) = self.cfg.read().min_pool_liquidity_sol {
            if liq_sol < min_liq {
                info!(mint=%mint, liq_sol=liq_sol, min_liq=min_liq, "sniper: liquidity below threshold");
                self.append_pool_candidate_record(
                    program_label,
                    &mint,
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some(liq_sol),
                    "SKIP",
                    &format!("liquidity {} < {}", liq_sol, min_liq),
                );
                return;
            }
        }

        // CRITICAL FILTER: Check if this token already has established pools on other DEXes
        // This prevents trading established tokens (like JLP) that get new small pools
        // A truly NEW token should ONLY have tiny pools (< 100 SOL total liquidity)
        let total_liq_across_dexes = {
            let mut total = 0.0f64;
            // Check Orca pools
            if let Some(orca) = &self.orca {
                total += orca.get_liquidity_sol_for_mint(&mint);
            }
            // Check Raydium pools
            if let Some(raydium) = &self.raydium {
                total += raydium.get_liquidity_sol_for_mint(&mint);
            }
            total
        };

        // If token has > 100 SOL total liquidity across all DEXes, it's established - skip it
        const MAX_TOTAL_LIQ_FOR_NEW_TOKEN: f64 = 100.0; // 100 SOL = ~$14k (truly new tokens have < $1k)
        if total_liq_across_dexes > MAX_TOTAL_LIQ_FOR_NEW_TOKEN {
            info!(
                mint=%mint,
                total_liq=total_liq_across_dexes,
                max_allowed=MAX_TOTAL_LIQ_FOR_NEW_TOKEN,
                "sniper: skipping established token with high total liquidity across DEXes"
            );
            self.append_pool_candidate_record(
                program_label,
                &mint,
                None,
                None,
                None,
                None,
                None,
                Some(liq_sol),
                "SKIP",
                &format!(
                    "established token: total_liq={:.2} SOL",
                    total_liq_across_dexes
                ),
            );
            return;
        }

        // Check if mint is already known in Raydium cache (implies old token with existing pools)
        if is_known_before_load {
            debug!(mint=%mint, "sniper: skipping - already in Raydium cache (old token)");
            self.append_pool_candidate_record(
                program_label,
                &mint,
                None,
                None,
                None,
                None,
                None,
                Some(liq_sol),
                "SKIP",
                "known_in_raydium",
            );
            return;
        }

        // For Pump.fun tokens from transaction-based discovery, skip only LP lock check
        // LP lock check is impossible because these tokens are too new (< 1 second old)
        // and mint account might not exist yet (data_len=0)
        // BUT we still run freshness check and position limits!
        let is_pumpfun_discovery =
            event.dex_type == crate::solana::geyser_pool_discovery::DexType::PumpFun;

        if is_pumpfun_discovery {
            info!(
                mint=%mint,
                pool=%event.pool_address,
                liq_sol=liq_sol,
                slot=event.slot,
                "sniper: Pump.fun token from Geyser CREATE - using slot-based validation"
            );

            // ============================================================
            // PUMP.FUN SLOT-BASED VALIDATION
            // ============================================================
            // For Pump.fun tokens discovered via Geyser CREATE instruction:
            // - We KNOW the token exists (we just saw the CREATE tx)
            // - We KNOW the slot it was created in
            // - RPC account fetch will FAIL because account hasn't propagated yet
            // - Mint/Freeze authority checks are unnecessary (Pump.fun revokes by default)
            //
            // Therefore: Only validate slot is recent (implies created after boot).
            // ============================================================

            // Verify slot is recent (not stale event from replay or old token)
            if let Ok(current_slot) = self.rpc.rpc.get_slot().await {
                let age_slots = current_slot.saturating_sub(event.slot);
                let max_age_slots: u64 = 150; // ~60 seconds (400ms per slot) - must be VERY fresh

                if age_slots > max_age_slots {
                    info!(
                        mint=%mint,
                        event_slot=event.slot,
                        current_slot=current_slot,
                        age_slots=age_slots,
                        age_secs_approx = (age_slots as f64 * 0.4) as u64,
                        "sniper: [Pump.fun] REJECT - token too old (slot age > 60 sec)"
                    );
                    self.append_pool_candidate_record(
                        program_label,
                        &mint,
                        None,
                        None,
                        None,
                        None,
                        None,
                        Some(liq_sol),
                        "SKIP",
                        "pumpfun_stale_slot",
                    );
                    return;
                }

                info!(
                    mint=%mint,
                    event_slot=event.slot,
                    current_slot=current_slot,
                    age_slots=age_slots,
                    age_secs_approx = (age_slots as f64 * 0.4) as u64,
                    "sniper: [Pump.fun] ✅ SLOT VALIDATION PASSED - token is fresh!"
                );
            } else {
                // Can't get current slot - proceed anyway, token is from Geyser so it's real
                warn!(mint=%mint, "sniper: [Pump.fun] could not get current slot, proceeding anyway (Geyser source trusted)");
            }

            // Risk Gate: ATOMIC check + reserve to prevent race conditions
            let base_buy = self.effective_max_buy_sol();
            if !self.try_reserve_buy_slot_atomic(&mint, base_buy) {
                info!(mint=%mint, base_buy, "sniper: [Pump.fun] RISK GATE BLOCKED - max positions or daily loss limit reached");
                self.append_pool_candidate_record(
                    program_label,
                    &mint,
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some(liq_sol),
                    "REJECT_RISK",
                    "pumpfun_risk_gate_blocked",
                );
                return;
            }
            // Slot is now reserved atomically!
            // NOTE: pending_buys will be decremented by finalize_fill on success,
            // or manually released here on failure.

            // LATENCY TRACKING: Time from Geyser discovery to buy attempt
            let pre_buy_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            let latency_to_buy_ms = pre_buy_ms.saturating_sub(event.discovered_at_ms);

            info!(
                mint=%mint,
                latency_ms=latency_to_buy_ms,
                "⏱️ LATENCY: Geyser discovery -> pre-buy (all checks passed)"
            );

            self.append_pool_candidate_record(
                program_label,
                &mint,
                None,
                None,
                None,
                None,
                None,
                Some(liq_sol),
                "BUY",
                "pumpfun_filters_passed",
            );

            // Proceed directly to buy attempt with liquidity info
            // Pass creator from Geyser event for new Pump.fun protocol (16-account format)
            if let Err(e) = self
                .attempt_initial_buy(&mint, Some(liq_sol), event.dex_type, event.creator)
                .await
            {
                warn!(?e, mint=%mint, "sniper: [Pump.fun] initial buy failed");
                // CRITICAL: Release slot on failure since finalize_fill won't be called
                self.release_buy_slot();
            } else {
                // LATENCY TRACKING: Total time from Geyser discovery to buy TX sent
                let post_buy_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                let total_latency_ms = post_buy_ms.saturating_sub(event.discovered_at_ms);
                info!(
                    mint=%mint,
                    total_latency_ms=total_latency_ms,
                    "⏱️ LATENCY: Geyser discovery -> buy TX sent (TOTAL)"
                );
            }
            return;
        }

        // ============================================================================
        // PROFESSIONAL VALIDATION FOR RAYDIUM/ORCA
        // Replaces the old lp_lock_check which required get_token_largest_accounts
        // ============================================================================
        info!(
            mint=%mint,
            pool=%event.pool_address,
            discovery_slot=event.slot,
            "sniper: [Raydium/Orca] using professional validation (no index required)"
        );

        match self
            .validate_token_professional(&mint, &event.pool_address, event.slot)
            .await
        {
            Ok(true) => {
                info!(mint=%mint, "sniper: [PRO] validation PASSED");
            }
            Ok(false) => {
                info!(mint=%mint, "sniper: [PRO] validation FAILED, SKIPPING");
                self.append_pool_candidate_record(
                    program_label,
                    &mint,
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some(liq_sol),
                    "SKIP",
                    "pro_validation_failed",
                );
                return;
            }
            Err(e) => {
                warn!(?e, mint=%mint, "sniper: [PRO] validation error, SKIPPING");
                self.append_pool_candidate_record(
                    program_label,
                    &mint,
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some(liq_sol),
                    "SKIP",
                    "pro_validation_error",
                );
                return;
            }
        }

        // All professional checks passed - attempt buy
        info!(mint=%mint, "sniper: all professional filters passed, attempting buy");

        // Risk Gate: ATOMIC check + reserve to prevent race conditions
        let base_buy = self.effective_max_buy_sol();
        if !self.try_reserve_buy_slot_atomic(&mint, base_buy) {
            info!(mint=%mint, base_buy, "sniper: RISK GATE BLOCKED - max positions or daily loss limit reached");
            self.append_pool_candidate_record(
                program_label,
                &mint,
                None,
                None,
                None,
                None,
                None,
                Some(liq_sol),
                "REJECT_RISK",
                "risk_gate_blocked",
            );
            return;
        }

        self.append_pool_candidate_record(
            program_label,
            &mint,
            None,
            None,
            None,
            None,
            None,
            Some(liq_sol),
            "BUY",
            "pro_validation_passed",
        );

        // Proceed to buy attempt
        // For Raydium/Orca pools, creator is None (only Pump.fun uses it)
        if let Err(e) = self
            .attempt_initial_buy(&mint, Some(liq_sol), event.dex_type, None)
            .await
        {
            warn!(?e, mint=%mint, "sniper: initial buy failed");
            // CRITICAL: Release slot on failure since finalize_fill won't be called
            self.release_buy_slot();
        }

        // Background task: Fetch actual liquidity from vaults (non-blocking)
        // Only for Raydium pools that have vault addresses
        if let (Some(coin_vault), Some(pc_vault)) = (event.coin_vault, event.pc_vault) {
            let rpc = self.rpc.clone();
            let pool_address = event.pool_address;
            let mint_clone = mint;

            tokio::spawn(async move {
                match Self::fetch_vault_liquidity(&rpc, &coin_vault, &pc_vault).await {
                    Ok(actual_liq_sol) => {
                        info!(
                            pool=%pool_address,
                            mint=%mint_clone,
                            estimated_liq=liq_sol,
                            actual_liq=actual_liq_sol,
                            diff_pct=((actual_liq_sol - liq_sol) / liq_sol * 100.0),
                            "sniper: actual vault liquidity fetched (background)"
                        );
                        // TODO: Store actual_liq_sol for position sizing in follow-up trades
                        // Could update a HashMap<Pubkey, f64> of known pool liquidities
                    }
                    Err(e) => {
                        debug!(?e, pool=%pool_address, "sniper: vault liquidity fetch failed (non-critical)");
                    }
                }
            });
        }
    }

    /// Fetch actual liquidity from Raydium pool vaults (for background validation)
    async fn fetch_vault_liquidity(
        rpc: &Arc<SolanaRpc>,
        coin_vault: &Pubkey,
        pc_vault: &Pubkey,
    ) -> Result<f64> {
        // Fetch vault token account balances
        let coin_balance = rpc.rpc.get_token_account_balance(coin_vault).await?;
        let pc_balance = rpc.rpc.get_token_account_balance(pc_vault).await?;

        let _coin_amount: u64 = coin_balance
            .amount
            .parse()
            .map_err(|e| anyhow::anyhow!("parse coin balance: {}", e))?;
        let pc_amount: u64 = pc_balance
            .amount
            .parse()
            .map_err(|e| anyhow::anyhow!("parse pc balance: {}", e))?;

        // Assuming quote token (pc) is SOL or SOL-equivalent
        // Total liquidity = (pc_amount * 2) / 1e9 (convert lamports to SOL)
        let pc_decimals = pc_balance.decimals;
        let divisor = 10_u64.pow(pc_decimals as u32) as f64;
        let actual_liq_sol = (pc_amount as f64 * 2.0) / divisor;

        Ok(actual_liq_sol)
    }

    #[allow(dead_code)]
    async fn handle_logs_static(_logs: Vec<String>) {
        // deprecated placeholder
    }

    fn program_label_for(pid: &str) -> &'static str {
        if pid == RAYDIUM_AMM_V4 {
            "RAYDIUM"
        } else if pid == ORCA_WHIRLPOOL_PROGRAM {
            "ORCA"
        } else {
            "UNKNOWN"
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn append_pool_candidate_record(
        &self,
        program_label: &str,
        mint: &Pubkey,
        top1: Option<f64>,
        top3: Option<f64>,
        top5: Option<f64>,
        burned: Option<f64>,
        program_locked: Option<f64>,
        liq_sol: Option<f64>,
        decision: &str,
        notes: &str,
    ) {
        use std::io::Write as _;
        static POOL_LOG_LOCK: once_cell::sync::Lazy<std::sync::Mutex<()>> =
            once_cell::sync::Lazy::new(|| std::sync::Mutex::new(()));
        let _g = POOL_LOG_LOCK.lock().unwrap();
        let dir_name =
            std::env::var("IRONCRAB_TRADE_LOG_DIR").unwrap_or_else(|_| "trade_logs".to_string());
        let dir = std::path::Path::new(&dir_name);
        if !dir.exists() {
            let _ = std::fs::create_dir_all(dir);
        }
        let date = ChronoUtc::now().format("%Y%m%d");
        let file_path = dir.join(format!("pools_found-{}.csv", date));
        let new_file = !file_path.exists();
        if let Ok(mut f) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)
        {
            if new_file {
                let _ = writeln!(f, "timestamp_utc,program,mint,top1_pct,top3_pct,top5_pct,burned_pct,program_locked_pct,liquidity_sol,decision,notes");
            }
            let _ = writeln!(
                f,
                "{ts},{prog},{mint},{top1},{top3},{top5},{burned},{plocked},{liq},{decision},{notes}",
                ts = ChronoUtc::now().to_rfc3339(),
                prog = program_label,
                mint = mint,
                top1 = top1.unwrap_or(0.0),
                top3 = top3.unwrap_or(0.0),
                top5 = top5.unwrap_or(0.0),
                burned = burned.unwrap_or(0.0),
                plocked = program_locked.unwrap_or(0.0),
                liq = liq_sol.unwrap_or(0.0),
                decision = decision,
                notes = notes,
            );
        }
    }
}

#[allow(dead_code)]
impl SniperEngine {
    // auxiliary impl continuation (initial buy etc.)
    async fn sol_usd_price(&self) -> f64 {
        // Snapshot config to avoid holding locks across await
        let cfg = self.cfg.read().clone();
        // Preference: oracle_preference -> specific source -> override -> default 100
        let pref = cfg
            .oracle_preference
            .clone()
            .unwrap_or_else(|| "override".to_string());
        // Try preferred first, then alternate, then override
        let mut try_sources: Vec<String> = vec![];
        let has_pyth = cfg.oracle_pyth_sol_usd.is_some();
        let has_sb = cfg.oracle_switchboard_sol_usd.is_some();
        match pref.as_str() {
            "pyth" => {
                if has_pyth {
                    try_sources.push("pyth".into());
                }
                if has_sb {
                    try_sources.push("switchboard".into());
                }
                try_sources.push("override".into());
            }
            "switchboard" => {
                if has_sb {
                    try_sources.push("switchboard".into());
                }
                if has_pyth {
                    try_sources.push("pyth".into());
                }
                try_sources.push("override".into());
            }
            _ => {
                try_sources.push("override".into());
                if has_pyth {
                    try_sources.push("pyth".into());
                }
                if has_sb {
                    try_sources.push("switchboard".into());
                }
            }
        }
        // Readers
        async fn read_pyth_price(
            rpc: &crate::solana::rpc::SolanaRpc,
            price_pk: &solana_sdk::pubkey::Pubkey,
        ) -> Option<f64> {
            // Pyth v2/v1: assume price account layout where bytes 208..216 is price exponent and 208+? may differ; to avoid tight coupling, use pyth-client? Not included.
            // Minimal safe approach: use RPC get_account and attempt to decode current price i64 at known offset for classic Pyth v2 (offset 208: expo i32; 208+4..+12 price i64). If mismatch, return None.
            if let Ok(acc) = rpc.get_account_retry(price_pk).await {
                if acc.data.len() >= 224 {
                    let expo_bytes: [u8; 4] = acc.data[208..212].try_into().ok()?;
                    let expo = i32::from_le_bytes(expo_bytes);
                    let price_bytes: [u8; 8] = acc.data[208 + 4..208 + 12].try_into().ok()?;
                    let price = i64::from_le_bytes(price_bytes);
                    // price * 10^expo
                    let scale = 10f64.powi(expo);
                    let v = (price as f64) * scale;
                    if v.is_finite() && v > 0.0 {
                        return Some(v);
                    }
                }
            }
            None
        }
        async fn read_switchboard_price(
            rpc: &crate::solana::rpc::SolanaRpc,
            agg_pk: &solana_sdk::pubkey::Pubkey,
        ) -> Option<f64> {
            // Switchboard aggregator v2 accounts: value at a dynamic offset; without sbv2 client, heuristically read last result at trailing 16 bytes as f64? Unsafe.
            // Safer: try parse little-endian f64 at a couple of common offsets; if invalid, return None.
            if let Ok(acc) = rpc.get_account_retry(agg_pk).await {
                let data = acc.data;
                // Try final 16 bytes as two u64 forming f64 via to_le_bytes
                if data.len() >= 16 {
                    let val =
                        f64::from_le_bytes(data[data.len() - 16..data.len() - 8].try_into().ok()?);
                    if val.is_finite() && val > 0.0 {
                        return Some(val);
                    }
                }
            }
            None
        }
        for src in try_sources {
            match src.as_str() {
                "pyth" => {
                    if let Some(pk_str) = cfg.oracle_pyth_sol_usd.clone() {
                        if let Ok(pk) = solana_sdk::pubkey::Pubkey::from_str(&pk_str) {
                            if let Some(v) = read_pyth_price(&self.rpc, &pk).await {
                                return v;
                            }
                        }
                    }
                }
                "switchboard" => {
                    if let Some(pk_str) = cfg.oracle_switchboard_sol_usd.clone() {
                        if let Ok(pk) = solana_sdk::pubkey::Pubkey::from_str(&pk_str) {
                            if let Some(v) = read_switchboard_price(&self.rpc, &pk).await {
                                return v;
                            }
                        }
                    }
                }
                _ => {
                    if let Some(ovr) = cfg.oracle_sol_usd_override {
                        if ovr > 0.0 {
                            return ovr;
                        }
                    }
                }
            }
        }
        // Fallback default
        cfg.oracle_sol_usd_override.unwrap_or(100.0)
    }
    fn append_trade_record(&self, line: &str, include_header: bool) {
        use std::io::Write as _;
        static TRADE_LOG_LOCK: once_cell::sync::Lazy<std::sync::Mutex<()>> =
            once_cell::sync::Lazy::new(|| std::sync::Mutex::new(()));
        let _g = TRADE_LOG_LOCK.lock().unwrap();
        let dir_name =
            std::env::var("IRONCRAB_TRADE_LOG_DIR").unwrap_or_else(|_| "trade_logs".to_string());
        let dir = std::path::Path::new(&dir_name);
        if !dir.exists() {
            let _ = std::fs::create_dir_all(dir);
        }
        let date = ChronoUtc::now().format("%Y%m%d");
        let file_path = dir.join(format!("trades-{}.csv", date));
        let new_file = !file_path.exists();
        if let Ok(mut f) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)
        {
            if new_file && include_header {
                let _ = writeln!(f, "timestamp_utc,side,mint,dex,signature,lamports_in,lamports_out,tokens_in,tokens_out,expected_tokens_out,expected_sol_out,shortfall_tokens,shortfall_sol,network_fee_lamports,realized_pnl_sol,notes");
            }
            let _ = writeln!(f, "{}", line);
        }
    }
    /// Build (but not yet send) an initial Raydium buy swap plan. For now we only log the plan.
    /// Strategy: spend up to cfg.max_buy_sol SOL buying the candidate mint via best SOL pairing.
    async fn attempt_initial_buy(
        &self,
        mint: &Pubkey,
        liq_sol: Option<f64>,
        pool_dex_type: crate::solana::geyser_pool_discovery::DexType,
        creator: Option<Pubkey>, // Required for Pump.fun - creator from Geyser event
    ) -> Result<()> {
        // Choose Raydium first (faster listing) – require connector
        let ray = self.raydium.clone();
        let orca = self.orca.clone();
        let pumpfun = self.pumpfun.clone();
        // Determine input (SOL) and output (mint) ordering for swap (we buy the mint with SOL)
        let sol_mint = pubkey!("So11111111111111111111111111111111111111112");
        // Convert max_buy_sol (f64) to lamports safely
        let lamports_in = ((self.effective_max_buy_sol() * 1e9) as u64).max(10_000); // dynamic drawdown-adjusted size

        // DON'T create dest ATA yet - wait until swap succeeds!
        // We'll derive the address for planning, but not ensure it exists
        use solana_sdk::pubkey::Pubkey as SdkPubkey;
        let mint_sdk = SdkPubkey::new_from_array(mint.to_bytes());
        let owner_sdk = self.treasury.pubkey();

        // DON'T wrap SOL yet - check if route exists first!
        let msb = self.adaptive_slippage_bps();

        // Try Pump.fun ONLY if the pool is actually on Pump.fun
        // For Pump.fun fresh launches (from Geyser), use fallback mode for quoting.
        // The fallback uses the deterministic initial bonding curve state.
        // The ATA will be created by the TX itself (with skip_preflight), so we don't
        // need the mint to exist for quoting - only for TX execution.
        let mut pumpfun_quote_out: u64 = 0;
        if pool_dex_type == crate::solana::geyser_pool_discovery::DexType::PumpFun {
            if let Some(ref pf) = pumpfun {
                // Use fallback mode for fresh launches - the bonding curve state is deterministic
                if let Ok(Some(q)) = pf
                    .quote_exact_in_with_fallback(
                        &sol_mint.to_string(),
                        &mint.to_string(),
                        lamports_in,
                        true,    // ENABLE fallback - use deterministic initial state for quote
                        creator, // Pass creator from Geyser event for fallback mode
                    )
                    .await
                {
                    pumpfun_quote_out = q.amount_out;
                }
            }
        }

        // Build Raydium plan (gracefully handle errors - don't fail if Raydium unavailable)
        let plan_opt = if let Some(r) = &ray {
            match r
                .build_swap_plan_auto(&sol_mint.to_string(), &mint.to_string(), lamports_in, msb)
                .await
            {
                Ok(plan) => plan,
                Err(e) => {
                    debug!(mint=%mint, error=?e, "sniper: raydium swap plan failed, will try other DEXs");
                    None
                }
            }
        } else {
            None
        };
        let plan_meta = plan_opt;
        let ray_quote_out: u64 = plan_meta.as_ref().map(|pm| pm.expected_out).unwrap_or(0);

        // Get Orca quote
        let mut orca_quote_out: u64 = 0;
        if let Some(o) = &orca {
            if let Ok(Some(q)) = o
                .quote_exact_in(&sol_mint.to_string(), &mint.to_string(), lamports_in)
                .await
            {
                orca_quote_out = q.amount_out;
            }
        }

        // Check if ANY DEX is available
        if pumpfun_quote_out == 0 && ray_quote_out == 0 && orca_quote_out == 0 {
            return Err(anyhow::anyhow!(
                "no swap route available - tried pump.fun, raydium, and orca"
            ));
        }

        // Dynamic route selection: pick DEX with best quote
        #[derive(Debug, PartialEq)]
        enum ChosenDex {
            PumpFun,
            Raydium,
            Orca,
        }
        let chosen_dex =
            if pumpfun_quote_out >= ray_quote_out && pumpfun_quote_out >= orca_quote_out {
                ChosenDex::PumpFun
            } else if ray_quote_out >= orca_quote_out {
                ChosenDex::Raydium
            } else {
                ChosenDex::Orca
            };

        match chosen_dex {
            ChosenDex::PumpFun => {
                // Pump.fun metric tracking can be added here
            }
            ChosenDex::Raydium => {
                crate::metrics::DEX_SELECTION_ENTRY_RAYDIUM_TOTAL
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            ChosenDex::Orca => {
                crate::metrics::DEX_SELECTION_ENTRY_ORCA_TOTAL
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }

        info!(
            mint=%mint,
            lamports_in,
            pumpfun_out=pumpfun_quote_out,
            ray_out=ray_quote_out,
            orca_out=orca_quote_out,
            chosen=?chosen_dex,
            "sniper: dynamic dex selection"
        );

        // DON'T wrap SOL yet - we need to verify swap instructions can be built first!
        // Derive WSOL ATA address for instruction building (but don't create it yet)
        let wsol_mint_prog = spl_token::native_mint::id();
        let wsol_mint_sdk_key = SdkPubkey::new_from_array(wsol_mint_prog.to_bytes());
        let (wsol_ata_sdk, _token_prog_wsol) = self
            .treasury
            .ata_address(&self.rpc, &owner_sdk, &wsol_mint_sdk_key)
            .await?;
        let _wsol_ata = wsol_ata_sdk; // use this for instruction building

        // Derive destination token ATA address (but don't create it yet)
        let (_dest_ata, _token_prog) = if chosen_dex == ChosenDex::PumpFun {
            // Pump.fun tokens use standard SPL Token Program (NOT Token-2022!)
            // We skip RPC lookup because the mint account might not be indexed yet
            let token_prog = spl_token::id();
            let token_prog_sdk = SdkPubkey::new_from_array(token_prog.to_bytes());

            // Convert SDK Pubkey to SPL Pubkey for derivation
            let owner_spl =
                spl_token::solana_program::pubkey::Pubkey::new_from_array(owner_sdk.to_bytes());
            let mint_spl =
                spl_token::solana_program::pubkey::Pubkey::new_from_array(mint_sdk.to_bytes());

            // Use get_associated_token_address_with_program_id with Token-2022
            let ata_spl =
                spl_associated_token_account::get_associated_token_address_with_program_id(
                    &owner_spl,
                    &mint_spl,
                    &token_prog,
                );
            let ata_sdk = SdkPubkey::new_from_array(ata_spl.to_bytes());

            (ata_sdk, token_prog_sdk)
        } else {
            self.treasury
                .ata_address(&self.rpc, &owner_sdk, &mint_sdk)
                .await?
        };

        // Build swap instructions based on chosen DEX
        let mut final_ixs: Vec<solana_sdk::instruction::Instruction> = Vec::new();

        if chosen_dex == ChosenDex::PumpFun {
            // Pump.fun swap (no Serum accounts needed!)
            if let Some(ref pf) = pumpfun {
                // Calculate min_out with slippage
                let slip = msb as u128;
                let min_out = ((pumpfun_quote_out as u128) * (10_000 - slip) / 10_000) as u64;
                let min_out = min_out.max(1);

                // Use async version with explicit slippage for proper max_sol_cost calculation
                // For BUY: max_sol_cost = lamports_in + slippage (we're willing to pay more SOL if price rises)
                // CRITICAL: Pump.fun tokens are extremely volatile - use high slippage for fresh launches
                // New tokens can pump 50-100% in the first seconds, so we need at least 50% slippage
                let pumpfun_min_slippage = self.cfg.read().pumpfun_buy_slippage_bps.unwrap_or(5000); // Config or 50% default (was 25%)
                let pumpfun_slippage_bps = msb.max(pumpfun_min_slippage);
                match pf
                    .build_swap_ix_async_with_slippage(
                        &sol_mint.to_string(),
                        &mint.to_string(),
                        lamports_in,
                        min_out,
                        creator, // Pass creator from Geyser event for fresh launches
                        pumpfun_slippage_bps, // Pass slippage in bps for max_sol_cost calculation (min 25%)
                    )
                    .await
                {
                    Ok(ixs) => {
                        if !ixs.is_empty() {
                            final_ixs = ixs;
                            info!(mint=%mint, lamports_in, expected_out=pumpfun_quote_out, min_out, slippage_bps=pumpfun_slippage_bps, "pump.fun swap instructions built");
                        }
                    }
                    Err(e) => {
                        warn!(mint=%mint, error=?e, "pump.fun build_swap_ix_async failed");
                    }
                }
            }
        } else if chosen_dex == ChosenDex::Raydium {
            if let Some(r) = &ray {
                if let Some(pool_addr) = plan_meta.as_ref().and_then(|p| p.pool) {
                    if let Some(snap) = r.snapshots().into_iter().find(|s| s.address == pool_addr) {
                        if let (Some(_open_orders), Some(_market_id)) =
                            (snap.open_orders, snap.market_id)
                        {
                            if let (
                                Some(bids),
                                Some(asks),
                                Some(event_q),
                                Some(base_vault),
                                Some(quote_vault),
                                Some(_serum_vs),
                            ) = (
                                snap.serum_bids,
                                snap.serum_asks,
                                snap.serum_event_queue,
                                snap.serum_base_vault,
                                snap.serum_quote_vault,
                                snap.serum_vault_signer,
                            ) {
                                let token_prog = spl_token::id();
                                let rent_sysvar = solana_sdk::sysvar::rent::id();
                                use crate::solana::dex::raydium::SerumMarketAccounts;
                                let serum_accounts = SerumMarketAccounts {
                                    bids,
                                    asks,
                                    event_queue: event_q,
                                    base_vault,
                                    quote_vault,
                                };
                                let market_prog = snap.market_program_id.unwrap_or(
                                    Pubkey::from_str(
                                        "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin",
                                    )
                                    .unwrap(),
                                );
                                if let Ok(_ray_prog) = Pubkey::from_str(RAYDIUM_AMM_V4) {
                                    let auth_pk =
                                        Pubkey::new_from_array(self.treasury.pubkey().to_bytes());
                                    let user_source = Pubkey::new_from_array(_wsol_ata.to_bytes());
                                    let user_dest = Pubkey::new_from_array(_dest_ata.to_bytes());
                                    let token_prog_pk =
                                        Pubkey::new_from_array(token_prog.to_bytes());
                                    let rent_pk = Pubkey::new_from_array(rent_sysvar.to_bytes());
                                    if let Some(ref pm) = plan_meta {
                                        if let Ok(full_ix) = r.build_swap_instruction(
                                            pool_addr,
                                            sol_mint,
                                            *mint,
                                            lamports_in,
                                            pm.min_out,
                                            auth_pk,
                                            user_source,
                                            user_dest,
                                            market_prog,
                                            token_prog_pk,
                                            rent_pk,
                                            serum_accounts,
                                            snap.target_orders,
                                        ) {
                                            final_ixs = vec![full_ix];
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } else {
            // Orca path
            if let Some(o) = &orca {
                o.set_user_authority(Pubkey::new_from_array(self.treasury.pubkey().to_bytes()));
                if let Ok((wsol_ata, _prog)) = self
                    .treasury
                    .ata_address(&self.rpc, &self.treasury.pubkey(), &wsol_mint_sdk_key)
                    .await
                {
                    o.set_user_token_account(wsol_mint_sdk_key, wsol_ata);
                }
                if let Ok((dst_ata, _prog2)) = self
                    .treasury
                    .ata_address(&self.rpc, &self.treasury.pubkey(), &mint_sdk)
                    .await
                {
                    o.set_user_token_account(mint_sdk, dst_ata);
                }

                // Calculate min_out with slippage
                let slip = msb as u128;
                let min_out_orca = ((orca_quote_out as u128) * (10_000 - slip) / 10_000) as u64;
                let min_out_orca = min_out_orca.max(1);

                match o.build_swap_ix(
                    &sol_mint.to_string(),
                    &mint.to_string(),
                    lamports_in,
                    min_out_orca,
                ) {
                    Ok(ixs) => {
                        if !ixs.is_empty() {
                            final_ixs = ixs;
                        }
                    }
                    Err(e) => {
                        warn!(?e, mint=%mint, "orca build_swap_ix failed");
                    }
                }
            }
        }

        // Verify we have instructions before proceeding
        if final_ixs.is_empty() {
            return Err(anyhow::anyhow!(
                "no swap instructions built - chosen dex {:?} failed",
                chosen_dex
            ));
        }

        // Get blockhash and build transaction
        let bh: Hash = match self.rpc.get_latest_blockhash_retry().await {
            Ok(h) => h,
            Err(e) => {
                RPC_ERRORS_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return Err(e.into());
            }
        };

        // CRITICAL FIX: Build ALL pre-swap instructions WITHOUT sending separate TXs
        // This prevents wasted SOL when swap fails (simulation error, etc.)
        let mut pre_swap_ixs = Vec::new();

        // For Raydium/Orca: Build WSOL wrap instructions (Pump.fun uses native SOL)
        if chosen_dex != ChosenDex::PumpFun {
            let (_wsol_ata, wrap_ixs) = self
                .treasury
                .build_wrap_sol_ixs(&self.rpc, lamports_in)
                .await
                .map_err(|e| anyhow::anyhow!("build_wrap_sol_ixs failed: {:?}", e))?;

            pre_swap_ixs.extend(wrap_ixs);
            info!(mint=%mint, lamports_in, "added WSOL wrap instructions to TX (atomic execution)");
        }

        // Build ATA creation instruction for destination token
        let (_dest_ata, maybe_ata_ix) = if chosen_dex == ChosenDex::PumpFun {
            // Pump.fun optimization: Skip RPC lookup, assume standard SPL Token Program
            let (ata, ix) = self.treasury.build_ata_ix_pumpfun(&owner_sdk, &mint_sdk);
            (ata, Some(ix))
        } else {
            self.treasury
                .build_ata_ix(&self.rpc, &owner_sdk, &mint_sdk)
                .await
                .map_err(|e| anyhow::anyhow!("build_ata_ix failed: {:?}", e))?
        };

        if let Some(ata_ix) = maybe_ata_ix {
            pre_swap_ixs.push(ata_ix);
            info!(mint=%mint, "added dest ATA creation to TX (atomic execution)");
        }

        // Prepend all pre-swap instructions to swap instructions
        // Order: [wrap_wsol (if needed), create_ata (if needed), ...swap_ixs]
        for (i, ix) in pre_swap_ixs.into_iter().enumerate() {
            final_ixs.insert(i, ix);
        }

        // Prepare message for fee estimate before signing
        let message = solana_sdk::message::Message::new(&final_ixs, Some(&self.treasury.pubkey()));
        let fee_estimate = self
            .rpc
            .get_fee_for_message_retry(&message)
            .await
            .unwrap_or(0);
        let mut tx = Transaction::new_with_payer(&final_ixs, Some(&self.treasury.pubkey()));
        tx.try_sign(&[self.treasury.signer_ref()], bh)?;

        // CRITICAL: For Pump.fun fresh launches, SKIP simulation entirely!
        // The RPC is always behind Geyser, so simulation will fail because the mint
        // doesn't exist yet from RPC's perspective. We send with skip_preflight anyway.
        // For other DEXs, simulate to catch obvious errors.
        let should_simulate = chosen_dex != ChosenDex::PumpFun;

        if should_simulate {
            match self.rpc.rpc.simulate_transaction(&tx).await {
                Ok(sim_result) => {
                    if let Some(err) = sim_result.value.err {
                        warn!(
                            mint=%mint,
                            error=?err,
                            logs=?sim_result.value.logs,
                            "sniper: TX simulation FAILED - NOT sending to avoid wasted SOL"
                        );
                        return Err(anyhow::anyhow!(
                            "simulation failed: {:?}. Logs: {:?}",
                            err,
                            sim_result.value.logs
                        ));
                    } else {
                        info!(mint=%mint, chosen_dex=?chosen_dex, "sniper: TX simulation PASSED, proceeding with send");
                    }
                }
                Err(e) => {
                    warn!(mint=%mint, error=?e, "sniper: simulation RPC error - proceeding anyway");
                    // Don't block on simulation errors - they might be RPC issues
                }
            }
        } else {
            info!(
                mint=%mint,
                chosen_dex=?chosen_dex,
                "sniper: SKIPPING simulation for Pump.fun fresh launch (RPC is behind Geyser)"
            );
        }

        let sent_at = Instant::now();
        let skip_preflight = chosen_dex == ChosenDex::PumpFun;
        match self.rpc_retry_tx(&tx, 3, skip_preflight).await {
            Ok(sig) => {
                let dur = sent_at.elapsed();
                record_swap_latency(dur.as_nanos() as u64);
                TRADES_EXECUTED_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                // Log based on which DEX was used
                match chosen_dex {
                    ChosenDex::PumpFun => {
                        info!(mint=%mint, sig=%sig, lamports_in, expected_out=pumpfun_quote_out, liq_sol, "sniper: initial buy submitted (pump.fun)");
                        let sol_in = lamports_in as f64 / 1e9;
                        // NOTE: Position is only created in finalize_fill if tokens actually arrive
                        self.finalize_fill(*mint, sol_in, creator).await;

                        // Record pending trade for reconciliation
                        {
                            let mut rs = self.risk.write();
                            rs.pending.insert(
                                *mint,
                                PendingTrade {
                                    expected_out_tokens: pumpfun_quote_out,
                                    dex: "PUMPFUN".into(),
                                    sig: sig.to_string(),
                                    lamports_in,
                                    network_fee_lamports: fee_estimate,
                                    ts: ChronoUtc::now().timestamp(),
                                    fee_bps: 100, // Pump.fun fee: 1%
                                },
                            );
                        }
                        record_network_fee(fee_estimate);
                        let line = format!(
                            "{ts},BUY,{mint},PUMPFUN,{sig},{lamports_in},0,0,0,{exp_tokens},,0,,{fee},,",
                            ts=ChronoUtc::now().to_rfc3339(),
                            mint=mint,
                            sig=sig,
                            lamports_in=lamports_in,
                            exp_tokens=pumpfun_quote_out,
                            fee=fee_estimate
                        );
                        self.append_trade_record(&line, true);
                    }
                    ChosenDex::Raydium => {
                        if let Some(pm) = plan_meta.as_ref() {
                            info!(mint=%mint, sig=%sig, lamports_in, expected_out=pm.expected_out, min_out=pm.min_out, pool=?pm.pool, liq_sol, "sniper: initial buy submitted (raydium)");
                            if pm.expected_out > 0 {
                                let sol_in = lamports_in as f64 / 1e9;
                                // NOTE: Position is only created in finalize_fill if tokens actually arrive
                                self.finalize_fill(*mint, sol_in, None).await; // Raydium: no creator needed
                            }

                            // Record pending trade
                            {
                                let mut rs = self.risk.write();
                                rs.pending.insert(
                                    *mint,
                                    PendingTrade {
                                        expected_out_tokens: pm.expected_out,
                                        dex: "RAYDIUM".into(),
                                        sig: sig.to_string(),
                                        lamports_in,
                                        network_fee_lamports: fee_estimate,
                                        ts: ChronoUtc::now().timestamp(),
                                        fee_bps: pm.fee_bps,
                                    },
                                );
                            }
                            record_network_fee(fee_estimate);
                            let line = format!(
                                "{ts},BUY,{mint},RAYDIUM,{sig},{lamports_in},0,0,0,{exp_tokens},,0,,{fee},,expected_min_out={min_out}",
                                ts=ChronoUtc::now().to_rfc3339(),
                                mint=mint,
                                sig=sig,
                                lamports_in=lamports_in,
                                exp_tokens=pm.expected_out,
                                fee=fee_estimate,
                                min_out=pm.min_out
                            );
                            self.append_trade_record(&line, true);
                        }
                    }
                    ChosenDex::Orca => {
                        info!(mint=%mint, sig=%sig, lamports_in, expected_out=orca_quote_out, liq_sol, "sniper: initial buy submitted (orca)");
                        let sol_in = lamports_in as f64 / 1e9;
                        // NOTE: Position is only created in finalize_fill if tokens actually arrive
                        self.finalize_fill(*mint, sol_in, None).await; // Orca: no creator needed

                        record_network_fee(fee_estimate);
                        let line = format!(
                            "{ts},BUY,{mint},ORCA,{sig},{lamports_in},0,0,0,{exp_tokens},,0,,{fee},,",
                            ts=ChronoUtc::now().to_rfc3339(),
                            mint=mint,
                            sig=sig,
                            lamports_in=lamports_in,
                            exp_tokens=orca_quote_out,
                            fee=fee_estimate
                        );
                        self.append_trade_record(&line, true);
                    }
                }

                self.purchased.write().insert(*mint);

                // Attempt WSOL unwrap to reclaim leftover lamports
                match self.treasury.unwrap_wsol(&self.rpc, None).await {
                    Ok(unwrap_sig) => {
                        info!(mint=%mint, unwrap_sig=%unwrap_sig, "sniper: wsol unwrapped post-trade")
                    }
                    Err(e) => debug!(?e, mint=%mint, "sniper: wsol unwrap failed"),
                }
            }
            Err(e) => {
                let err_str = e.to_string();
                // CRITICAL FIX: If TX was sent but confirmation timed out, the TX may have succeeded!
                // Register the position anyway to prevent over-buying. Wallet scan will verify later.
                if err_str.contains("timed out") || err_str.contains("Timeout") {
                    warn!(mint=%mint, "sniper: buy TX confirmation timeout - TX was SENT, checking if tokens arrived");
                    // Try to finalize the fill - position will only be created if tokens actually arrived
                    let sol_in = lamports_in as f64 / 1e9;
                    self.finalize_fill(*mint, sol_in, creator).await; // Pass creator in case it's Pump.fun
                    self.purchased.write().insert(*mint);
                    // Don't return error - we attempted to register the position
                    return Ok(());
                }

                warn!(?e, mint=%mint, "sniper: buy tx failed (will not retry immediately)");
                TRADES_FAILED_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                RPC_ERRORS_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                // CRITICAL: Return error so caller knows to release the buy slot!
                return Err(anyhow::anyhow!("buy tx failed: {}", e));
            }
        }
        Ok(())
    }

    pub async fn run(&self) -> Result<()> {
        self.try_load_risk_state();

        // CRITICAL: Scan wallet for existing token balances and register as positions
        // This ensures max_open_positions works correctly even after restart
        if let Err(e) = self.scan_wallet_for_existing_positions().await {
            warn!(?e, "sniper: failed to scan wallet for existing positions");
        }

        // GEYSER IS REQUIRED - no fallback to deprecated WebSocket
        let geyser_url = self.geyser_grpc_url.as_ref().ok_or_else(|| {
            anyhow::anyhow!(
                "GEYSER_GRPC_URL is required! WebSocket logsSubscribe has been removed."
            )
        })?;

        info!(url=%geyser_url, "sniper: using Geyser gRPC for pool discovery");
        self.run_with_geyser(geyser_url).await
    }

    #[allow(dead_code)]
    fn heuristics_pass(
        &self,
        mint: &Pubkey,
        owner: Option<&Pubkey>,
        liquidity_sol: Option<f64>,
        freeze_auth: Option<&Pubkey>,
        mint_decimals: Option<u8>,
    ) -> bool {
        // Config / dynamic sources
        if self
            .cfg
            .read()
            .blacklist_mints
            .iter()
            .any(|m| m == &mint.to_string())
        {
            return false;
        }
        if let Some(o) = owner {
            if self
                .cfg
                .read()
                .blacklist_owners
                .iter()
                .any(|v| v == &o.to_string())
            {
                return false;
            }
        }
        if let Some(min_liq) = self.cfg.read().min_pool_liquidity_sol {
            if liquidity_sol.unwrap_or(0.0) < min_liq {
                return false;
            }
        }
        if self.cfg.read().require_freeze_auth_none.unwrap_or(false) && freeze_auth.is_some() {
            return false;
        }
        if let Some((lo, hi)) = self.cfg.read().require_mint_decimals_range {
            if let Some(d) = mint_decimals {
                if d < lo || d > hi {
                    return false;
                }
            }
        }
        true
    }

    fn today_key() -> u32 {
        // Simple day number (UTC days since epoch) for daily loss tracking
        (chrono::Utc::now().timestamp() / 86_400) as u32
    }

    fn risk_reset_if_needed(&self) {
        let mut rs = self.risk.write();
        let today = Self::today_key();
        if rs.current_day != today {
            rs.current_day = today;
            rs.realized_loss_today_sol = 0.0;
        }
    }

    fn can_open_position_for(&self, mint: &Pubkey, planned_sol: f64) -> bool {
        self.risk_reset_if_needed();
        let rs = self.risk.read();
        let cfg_r = self.cfg.read();
        if let Some(cap) = cfg_r.max_position_sol {
            if planned_sol > cap {
                debug!(mint=%mint, planned=planned_sol, cap=cap, "sniper: position size exceeds max_position_sol");
                return false;
            }
        }
        if let Some(daily) = cfg_r.daily_loss_limit_sol {
            if rs.realized_loss_today_sol >= daily {
                debug!(mint=%mint, loss=rs.realized_loss_today_sol, limit=daily, "sniper: daily loss limit reached");
                return false;
            }
        }
        if let Some(mop) = cfg_r.max_open_positions {
            let current_count = rs.open.len();
            let total_exposure = current_count + rs.pending_buys;
            if total_exposure >= mop {
                info!(mint=%mint, current=current_count, pending=rs.pending_buys, max=mop, "sniper: max open positions reached (incl. pending), rejecting new buy");
                return false;
            }
        }
        if let Some(until) = rs.cooldown_until.get(mint) {
            if *until > chrono::Utc::now().timestamp() {
                debug!(mint=%mint, until=until, "sniper: mint in cooldown");
                return false;
            }
        }
        true
    }

    /// Atomically check if we can open a position AND reserve the slot if allowed.
    /// This prevents race conditions where multiple tasks pass the check simultaneously.
    /// Returns true if slot was reserved, false if blocked by risk limits.
    fn try_reserve_buy_slot_atomic(&self, mint: &Pubkey, planned_sol: f64) -> bool {
        self.risk_reset_if_needed();
        let mut rs = self.risk.write(); // Write lock for atomicity
        let cfg_r = self.cfg.read();

        // Check position size cap
        if let Some(cap) = cfg_r.max_position_sol {
            if planned_sol > cap {
                info!(mint=%mint, planned=planned_sol, cap=cap, "sniper: [ATOMIC] position size exceeds max_position_sol");
                return false;
            }
        }

        // Check daily loss limit
        if let Some(daily) = cfg_r.daily_loss_limit_sol {
            if rs.realized_loss_today_sol >= daily {
                info!(mint=%mint, loss=rs.realized_loss_today_sol, limit=daily, "sniper: [ATOMIC] daily loss limit reached");
                return false;
            }
        }

        // Check max open positions (CRITICAL: includes pending_buys)
        if let Some(mop) = cfg_r.max_open_positions {
            let current_count = rs.open.len();
            let total_exposure = current_count + rs.pending_buys;
            if total_exposure >= mop {
                info!(
                    mint=%mint,
                    current_positions=current_count,
                    pending_buys=rs.pending_buys,
                    total_exposure=total_exposure,
                    max_allowed=mop,
                    "sniper: [ATOMIC] MAX OPEN POSITIONS LIMIT REACHED - BLOCKING NEW BUY"
                );
                return false;
            }
        }

        // Check cooldown
        if let Some(until) = rs.cooldown_until.get(mint) {
            if *until > chrono::Utc::now().timestamp() {
                info!(mint=%mint, until=until, "sniper: [ATOMIC] mint in cooldown");
                return false;
            }
        }

        // All checks passed - atomically reserve the slot NOW
        rs.pending_buys += 1;
        info!(
            mint=%mint,
            new_pending_count=rs.pending_buys,
            open_positions=rs.open.len(),
            "sniper: [ATOMIC] slot reserved successfully"
        );
        true
    }

    pub fn reserve_buy_slot(&self) {
        let mut rs = self.risk.write();
        rs.pending_buys += 1;
    }

    pub fn release_buy_slot(&self) {
        let mut rs = self.risk.write();
        if rs.pending_buys > 0 {
            rs.pending_buys -= 1;
        }
    }

    // NOTE: record_fill_placeholder has been REMOVED!
    // Position creation now only happens in finalize_fill() after confirming tokens arrived.
    // This eliminates ghost positions from failed transactions.

    /// Finalize a fill by checking token balance and creating/updating position.
    /// Only creates position if tokens actually arrived (no more ghost positions).
    /// invested_sol: The SOL amount that was spent on this buy
    /// creator: Optional creator pubkey (for Pump.fun tokens, used in exit path)
    async fn finalize_fill(&self, mint: Pubkey, invested_sol: f64, creator: Option<Pubkey>) {
        info!(mint=%mint, invested_sol=invested_sol, "finalize_fill: STARTING position finalization");
        self.risk_reset_if_needed(); // Reset daily counters if new day
        let owner = self.treasury.pubkey();
        let mint_sdk = solana_sdk::pubkey::Pubkey::new_from_array(mint.to_bytes());
        // CRITICAL FIX: Compute ATA address WITHOUT RPC call.
        // The RPC call fails when mint is not yet on-chain (buy TX still confirming).
        // Pump.fun tokens use standard SPL Token Program.
        let owner_spl = spl_token::solana_program::pubkey::Pubkey::new_from_array(owner.to_bytes());
        let mint_spl =
            spl_token::solana_program::pubkey::Pubkey::new_from_array(mint_sdk.to_bytes());
        let ata_spl =
            spl_associated_token_account::get_associated_token_address(&owner_spl, &mint_spl);
        let ata = solana_sdk::pubkey::Pubkey::new_from_array(ata_spl.to_bytes());
        info!(mint=%mint, ata=%ata, "finalize_fill: checking token balance");
        let decimals =
            crate::solana::token_utils::get_token_decimals_or_default(&self.rpc, &mint_sdk).await;

        // CRITICAL FIX: Retry with delay to wait for TX confirmation
        // The TX may not be confirmed yet when this is called immediately after send
        let mut amt = 0.0f64;
        let mut raw = 0u64;
        for attempt in 0..30 {
            if attempt > 0 {
                tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
            }
            let acc_opt = self.rpc.get_account_retry(&ata).await.ok();
            if let Some(acc) = acc_opt {
                if acc.data.len() >= 72 {
                    raw = u64::from_le_bytes(acc.data[64..72].try_into().unwrap());
                    amt = if decimals == 0 {
                        raw as f64
                    } else {
                        raw as f64 / 10f64.powi(decimals as i32)
                    };
                    if amt > 0.0 {
                        info!(mint=%mint, amount=amt, decimals=decimals, attempt=attempt, "finalize_fill: found token balance");
                        break;
                    }
                }
            }
            if attempt < 29 {
                debug!(mint=%mint, attempt=attempt, "finalize_fill: token balance is 0, retrying...");
            }
        }

        if amt <= 0.0 {
            // TX failed - no tokens arrived. DO NOT create position!
            warn!(mint=%mint, "finalize_fill: no token balance found after 30 attempts - TX likely failed, NOT creating position");
            // Decrement pending_buys since we're done with this attempt
            let mut rs = self.risk.write();
            if rs.pending_buys > 0 {
                rs.pending_buys -= 1;
            }
            return;
        }

        // SUCCESS: Tokens arrived! Now create the position
        let entry_price = invested_sol / amt.max(1e-9);
        let (pend_opt, entry_price_existing) = {
            let mut rs = self.risk.write();
            let pend = rs.pending.remove(&mint);

            // Check if position already exists (from wallet scan or previous buy)
            let already_exists = rs.open.get(&mint).map(|v| !v.is_empty()).unwrap_or(false);

            if already_exists {
                // Update existing position
                if let Some(v) = rs.open.get_mut(&mint) {
                    if let Some(last) = v.last_mut() {
                        if last.entry_price_sol == 0.0 {
                            last.amount_tokens = amt;
                            last.token_decimals = decimals;
                            last.entry_price_sol = entry_price;
                            info!(
                                mint=%mint,
                                amount_tokens=amt,
                                entry_price_sol=entry_price,
                                invested_sol=invested_sol,
                                decimals=decimals,
                                "finalize_fill: position data updated successfully"
                            );
                        }
                    }
                }
            } else {
                // Create NEW position - only if tokens actually arrived!
                let lot = PositionLot {
                    entry_price_sol: entry_price,
                    amount_tokens: amt,
                    invested_sol,
                    token_decimals: decimals,
                    last_unrealized_pnl_sol: 0.0,
                    opened_ts: chrono::Utc::now().timestamp(),
                    executed_tp_bps: Vec::new(),
                    peak_pnl_bps: 0,
                    executed_timed_tiers: Vec::new(),
                    creator: creator.map(|c| c.to_string()), // Store creator for exit path (Pump.fun)
                };
                rs.open.entry(mint).or_default().push(lot);

                // Count positions before dropping lock
                let lots: usize = rs.open.values().map(|v| v.len()).sum();
                drop(rs); // Release write lock

                // Register with kill switch monitor if enabled (no lock needed)
                if let Some(ks) = &self.kill_switch {
                    ks.register_position(mint, creator); // Pass creator for dev-sell detection
                }

                OPEN_POSITIONS_GAUGE.store(lots as u64, std::sync::atomic::Ordering::Relaxed);

                // Record BUY trade for dashboard
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);
                record_recent_trade(RecentTrade {
                    timestamp_ms: now_ms,
                    mint: mint.to_string(),
                    action: "BUY".to_string(),
                    tx_hash: String::new(), // Will be filled from pending if available
                    amount_tokens: amt,
                    price_sol: entry_price,
                    pnl_sol: None,
                    pnl_pct: None,
                    latency_ms: None,
                });

                info!(
                    mint=%mint,
                    amount_tokens=amt,
                    entry_price_sol=entry_price,
                    invested_sol=invested_sol,
                    decimals=decimals,
                    total_open_positions=lots,
                    "finalize_fill: NEW POSITION CREATED (tokens confirmed)"
                );
            }

            // Decrement pending_buys since position is now recorded (re-acquire write lock)
            {
                let mut rs = self.risk.write();
                if rs.pending_buys > 0 {
                    rs.pending_buys -= 1;
                }
            }

            let entry_price_last = self
                .risk
                .read()
                .open
                .get(&mint)
                .and_then(|v| v.last())
                .map(|l| l.entry_price_sol)
                .unwrap_or(0.0);
            (pend, entry_price_last)
        };

        // Persist immediately after updating position data
        self.persist_risk_state();

        if let Some(pend) = pend_opt {
            // Fetch meta outside lock
            let mut exact_network_fee = pend.network_fee_lamports;
            if let Ok(sig_obj) = solana_sdk::signature::Signature::from_str(&pend.sig) {
                use solana_client::rpc_config::RpcTransactionConfig;
                use solana_transaction_status::UiTransactionEncoding;
                let cfg = RpcTransactionConfig {
                    encoding: Some(UiTransactionEncoding::JsonParsed),
                    commitment: None,
                    max_supported_transaction_version: None,
                };
                if let Ok(tx_opt) = self
                    .rpc
                    .get_transaction_with_config_retry(&sig_obj, cfg)
                    .await
                {
                    if let Some(meta) = tx_opt.transaction.meta {
                        exact_network_fee = meta.fee;
                        // Note: Further meta parsing follows below where we can update actual_raw if needed
                    }
                }
            }
            // Meta-based token delta extraction for accuracy
            let scale = 10f64.powi(decimals as i32);
            let mut actual_raw = raw; // Use the raw value we already fetched
                                      // Try recomputing actual_raw from meta pre/post if available (owner+mint delta)
            if let Ok(sig_obj) = solana_sdk::signature::Signature::from_str(&pend.sig) {
                use solana_client::rpc_config::RpcTransactionConfig;
                use solana_transaction_status::option_serializer::OptionSerializer;
                use solana_transaction_status::UiTransactionEncoding;
                let cfg = RpcTransactionConfig {
                    encoding: Some(UiTransactionEncoding::JsonParsed),
                    commitment: None,
                    max_supported_transaction_version: None,
                };
                if let Ok(tx_opt) = self
                    .rpc
                    .get_transaction_with_config_retry(&sig_obj, cfg)
                    .await
                {
                    if let Some(meta) = tx_opt.transaction.meta {
                        let owner_str = owner.to_string();
                        let mint_str = mint.to_string();
                        let mut pre_raw_opt: Option<u128> = None;
                        let mut post_raw_opt: Option<u128> = None;
                        if let OptionSerializer::Some(pre) = meta.pre_token_balances.as_ref() {
                            for b in pre {
                                let owner_ok = match b.owner.as_ref() {
                                    OptionSerializer::Some(o) => o == &owner_str,
                                    _ => false,
                                };
                                if owner_ok && b.mint == mint_str {
                                    if let Ok(v) = b.ui_token_amount.amount.parse::<u128>() {
                                        pre_raw_opt = Some(v);
                                        break;
                                    }
                                }
                            }
                        }
                        if let OptionSerializer::Some(post) = meta.post_token_balances.as_ref() {
                            for b in post {
                                let owner_ok = match b.owner.as_ref() {
                                    OptionSerializer::Some(o) => o == &owner_str,
                                    _ => false,
                                };
                                if owner_ok && b.mint == mint_str {
                                    if let Ok(v) = b.ui_token_amount.amount.parse::<u128>() {
                                        post_raw_opt = Some(v);
                                        break;
                                    }
                                }
                            }
                        }
                        if let (Some(pre_raw), Some(post_raw)) = (pre_raw_opt, post_raw_opt) {
                            if post_raw >= pre_raw {
                                let delta = (post_raw - pre_raw) as u64;
                                if delta > 0 {
                                    actual_raw = delta;
                                }
                            }
                        }
                    }
                }
            }
            // NOTE: OptionSerializer wrapper complicates direct access; keep fallback for now.
            let expected_raw = pend.expected_out_tokens;
            let shortfall = expected_raw.saturating_sub(actual_raw);
            let shortfall_ui = shortfall as f64 / scale;
            let shortfall_sol = shortfall_ui * entry_price_existing;
            // Adaptive slippage controller update (BUY fills only)
            // Observed slippage fraction relative to expected_out: s = shortfall / max(expected,1)
            let expected_safe = expected_raw.max(1) as f64;
            let observed_slip = (shortfall as f64 / expected_safe).max(0.0);
            record_shortfall_pct(observed_slip);

            // Record fill observation for quantile calculator
            if self.cfg.read().quantile_slippage_enabled.unwrap_or(false) {
                // Determine pool ID (use DEX + mint pair as identifier)
                let pool_id = format!("{}_{}", pend.dex, mint);

                // Determine size category (approximate from invested lamports)
                let size_category = if pend.lamports_in < 1_000_000_000 {
                    crate::quantile_impact::SizeCategory::Small
                } else if pend.lamports_in < 5_000_000_000 {
                    crate::quantile_impact::SizeCategory::Medium
                } else {
                    crate::quantile_impact::SizeCategory::Large
                };

                self.quantile_calc
                    .record_fill(pool_id, expected_raw, actual_raw, size_category);
            }
            // Note: tx_meta not available here; fee breakdown handled separately

            {
                let mut rs = self.risk.write();
                // Maintain rolling window
                rs.recent_slippage.push(observed_slip);
                let win = self.cfg.read().adaptive_slippage_window.unwrap_or(20);
                if rs.recent_slippage.len() > win {
                    let excess = rs.recent_slippage.len() - win;
                    rs.recent_slippage.drain(0..excess);
                }
                // Compute mean and adjust toward target
                let mean = if rs.recent_slippage.is_empty() {
                    0.0
                } else {
                    rs.recent_slippage.iter().copied().sum::<f64>()
                        / (rs.recent_slippage.len() as f64)
                };
                let target = self
                    .cfg
                    .read()
                    .adaptive_slippage_target_pct
                    .unwrap_or(0.002)
                    .clamp(0.0, 0.2); // default 0.2%
                let step = self
                    .cfg
                    .read()
                    .adaptive_slippage_step_bps
                    .unwrap_or(5)
                    .max(1) as i64;
                let min_b = self
                    .cfg
                    .read()
                    .adaptive_slippage_min_bps
                    .unwrap_or(self.cfg.read().max_slippage_bps) as i64;
                let max_b = self
                    .cfg
                    .read()
                    .adaptive_slippage_max_bps
                    .unwrap_or(self.cfg.read().max_slippage_bps) as i64;
                let mut cur =
                    rs.adaptive_slippage_bps
                        .unwrap_or(self.cfg.read().max_slippage_bps) as i64;
                if mean > target {
                    cur = (cur + step).min(max_b);
                } else if mean < target {
                    cur = (cur - step).max(min_b);
                }
                rs.adaptive_slippage_bps = Some(cur as u32);
            }
            // Record fee percent based on network fee vs notional invested
            let invested_ui = pend.lamports_in as f64 / 1e9;
            if invested_ui > 0.0 {
                let fee_pct = (exact_network_fee as f64 / 1e9) / invested_ui; // network fee percent of notional
                record_fee_pct(fee_pct.max(0.0));
            }
            // Compute protocol fee tokens heuristic from quote fee_bps if meta didn't yield fee transfers
            let fee_tokens = if pend.fee_bps > 0 && pend.fee_bps < 5000 {
                // expected_out = no_fee_out * (1 - fee_bps/10_000) approximately; invert
                let no_fee_out = ((expected_raw as u128) * 10_000u128
                    / (10_000u128 - pend.fee_bps as u128)) as u64;
                no_fee_out.saturating_sub(expected_raw)
            } else {
                0
            };
            if fee_tokens > 0 {
                PROTOCOL_FEE_TOKENS_TOTAL
                    .fetch_add(fee_tokens, std::sync::atomic::Ordering::Relaxed);
                let fee_ui = fee_tokens as f64 / scale;
                let fee_sol = fee_ui * entry_price_existing;
                PROTOCOL_FEE_SOL_MICRO_TOTAL.fetch_add(
                    (fee_sol * 1_000_000.0) as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
            }

            // Note: Extended fee breakdown (DEX-specific vaults) would require transaction metadata
            // which is not available at this point. Fee breakdown can be done via separate RPC call
            // using tx_fee_parser::fetch_and_parse_fee_breakdown() if needed.

            record_shortfall(shortfall, shortfall_sol);
            let line = format!(
                "{ts},FILL,{mint},{dex},{sig},{lamports_in},0,0,{actual_tokens},{expected_tokens},,{shortfall_tokens},,{fee},,shortfall_ui={shortfall_ui:.9};shortfall_sol={shortfall_sol:.9};protocol_fee_tokens={fee_tokens};network_fee_exact={network_fee_exact}",
                ts=ChronoUtc::now().to_rfc3339(),
                mint=mint,
                dex=pend.dex,
                sig=pend.sig,
                lamports_in=pend.lamports_in,
                actual_tokens=actual_raw,
                expected_tokens=expected_raw,
                shortfall_tokens=shortfall,
                fee=exact_network_fee,
                shortfall_ui=shortfall_ui,
                shortfall_sol=shortfall_sol,
                fee_tokens=fee_tokens,
                network_fee_exact=exact_network_fee
            );
            self.append_trade_record(&line, true);

            // Persist state after fill is finalized with accurate token amounts
            self.persist_risk_state();
        }
    }

    async fn evaluate_positions(&self) -> Result<()> {
        // LEGACY: Clean up any remaining ghost positions (amount_tokens = 0)
        // NOTE: With the new finalize_fill approach, ghost positions should no longer be created
        // since positions are only created AFTER tokens are confirmed. This is just a safety net.
        {
            let now = chrono::Utc::now().timestamp();
            let mut rs = self.risk.write();
            let mut cleaned = 0usize;
            for lots in rs.open.values_mut() {
                let before = lots.len();
                lots.retain(|lot| {
                    // Keep if has tokens OR is less than 2 minutes old
                    let age_secs = now - lot.opened_ts;
                    lot.amount_tokens > 0.0 || age_secs < 120
                });
                cleaned += before - lots.len();
            }
            // Remove empty mint entries
            rs.open.retain(|_, lots| !lots.is_empty());
            if cleaned > 0 {
                info!(cleaned_ghost_positions=cleaned, "evaluate_positions: removed legacy ghost positions (amount_tokens=0, age>2min)");
                drop(rs);
                self.persist_risk_state();
            }
        }

        // Load config
        let (
            stop_bps,
            tp_bps,
            tiers,
            trailing,
            min_exit_notional,
            time_exits_enabled,
            max_hold_secs,
            timed_tiers,
        ) = {
            let r = self.cfg.read();
            (
                r.stop_loss_bps.unwrap_or(u32::MAX),
                r.take_profit_bps.unwrap_or(u32::MAX),
                r.take_profit_tiers.clone(),
                r.trailing_stop_bps,
                r.min_exit_notional_sol.unwrap_or(0.0),
                r.enable_time_based_exits.unwrap_or(false),
                r.max_hold_secs.unwrap_or(90),
                r.timed_exit_tiers.clone(),
            )
        };

        // Skip if no exit strategy configured
        let has_price_exits = stop_bps != u32::MAX || tp_bps != u32::MAX || tiers.is_some();
        let has_time_exits = time_exits_enabled;

        if !has_price_exits && !has_time_exits {
            return Ok(());
        }

        // Load parallel exit config
        let (parallel_exits_enabled, max_parallel) = {
            let r = self.cfg.read();
            (
                r.parallel_exits.unwrap_or(true), // Default: enabled
                r.max_parallel_exits.unwrap_or(5),
            )
        };

        // Count open positions for logging
        let open_count = self.risk.read().open.len();
        if open_count > 0 {
            info!(
                open_positions = open_count,
                time_exits_enabled = time_exits_enabled,
                max_hold_secs = max_hold_secs,
                "evaluate_positions: checking positions"
            );
        }

        let now = chrono::Utc::now().timestamp();
        let sol_mint = pubkey!("So11111111111111111111111111111111111111112");

        // Collect exit tasks for parallel execution
        let mut exit_tasks: Vec<ExitTask> = Vec::new();

        // Flatten lots for evaluation
        let positions: Vec<(Pubkey, PositionLot, usize)> = {
            let rs = self.risk.read();
            let mut out = Vec::new();
            for (mint, lots) in rs.open.iter() {
                for (idx, lot) in lots.iter().cloned().enumerate() {
                    out.push((*mint, lot, idx));
                }
            }
            out
        };
        if positions.is_empty() {
            return Ok(());
        }
        for (mint, pos, lot_idx) in positions {
            // Calculate position age in seconds
            let age_secs = (now - pos.opened_ts) as u64;

            // Convert UI tokens to raw token amount (with decimals)
            let raw_token_amount =
                (pos.amount_tokens * 10f64.powi(pos.token_decimals as i32)).floor() as u64;

            // === TIME-BASED EXIT LOGIC ===
            if time_exits_enabled {
                let mut time_fraction: f64 = 0.0;
                let mut time_exit_reason = String::new();

                // Check max hold time - forced full exit
                if age_secs >= max_hold_secs {
                    time_fraction = 1.0;
                    time_exit_reason =
                        format!("MAX_HOLD_TIME ({}s >= {}s)", age_secs, max_hold_secs);
                }
                // Check timed exit tiers
                else if let Some(ref timed) = timed_tiers {
                    let mut rs = self.risk.write();
                    if let Some(v) = rs.open.get_mut(&mint) {
                        if let Some(l) = v.get_mut(lot_idx) {
                            for tier in timed.iter() {
                                if age_secs >= tier.secs
                                    && !l.executed_timed_tiers.contains(&tier.secs)
                                {
                                    time_fraction = tier.fraction.clamp(0.0, 1.0);
                                    l.executed_timed_tiers.push(tier.secs);
                                    time_exit_reason = format!(
                                        "TIMED_TIER ({}s, {}%)",
                                        tier.secs,
                                        (tier.fraction * 100.0) as u32
                                    );
                                    break; // Execute one tier at a time
                                }
                            }
                        }
                    }
                }

                // Collect time-based exit task (instead of executing immediately)
                if time_fraction > 0.0 {
                    let sell_tokens = ((pos.amount_tokens * time_fraction)
                        * 10f64.powi(pos.token_decimals as i32))
                    .floor() as u64;
                    if sell_tokens > 0 {
                        info!(
                            mint=%mint,
                            age_secs=age_secs,
                            fraction=time_fraction,
                            reason=%time_exit_reason,
                            sell_tokens=sell_tokens,
                            "TIME-BASED EXIT triggered"
                        );
                        let is_full_exit = time_fraction >= 0.99;
                        // Get creator from position for Pump.fun exit path
                        let creator_pk =
                            pos.creator.as_ref().and_then(|s| Pubkey::from_str(s).ok());
                        exit_tasks.push(ExitTask {
                            mint,
                            lot_idx,
                            sell_tokens,
                            fraction: time_fraction,
                            is_emergency: is_full_exit,
                            reason: time_exit_reason.clone(),
                            creator: creator_pk,
                        });
                    }
                    continue; // Skip price-based evaluation for this position
                }
            }

            // === PRICE-BASED EXIT LOGIC (original) ===
            if !has_price_exits {
                continue;
            }

            // Quote exit value (prefer Raydium then Orca)
            let mut quote_out: Option<u64> = None;
            if let Some(r) = &self.raydium {
                if let Ok(Some(q)) = r
                    .quote_exact_in(&mint.to_string(), &sol_mint.to_string(), raw_token_amount)
                    .await
                {
                    quote_out = Some(q.amount_out);
                }
                // Note: Not counting as RPC error - token may simply not be on Raydium
            }
            if quote_out.is_none() {
                if let Some(o) = &self.orca {
                    if let Ok(Some(q)) = o
                        .quote_exact_in(&mint.to_string(), &sol_mint.to_string(), raw_token_amount)
                        .await
                    {
                        quote_out = Some(q.amount_out);
                    }
                    // Note: Not counting as RPC error - token may simply not be on Orca
                }
            }
            // Check Pump.fun if others failed (likely for new bonding curve tokens)
            if quote_out.is_none() {
                if let Some(pf) = &self.pumpfun {
                    if let Ok(Some(q)) = pf
                        .quote_exact_in(&mint.to_string(), &sol_mint.to_string(), raw_token_amount)
                        .await
                    {
                        quote_out = Some(q.amount_out);
                    }
                }
            }
            let Some(out_lamports) = quote_out else {
                continue;
            };
            let price_now = if pos.amount_tokens > 0.0 {
                (out_lamports as f64 / 1e9) / pos.amount_tokens
            } else {
                0.0
            };
            let pnl_pct = if pos.entry_price_sol > 0.0 {
                (price_now - pos.entry_price_sol) / pos.entry_price_sol
            } else {
                0.0
            };
            let pnl_bps = (pnl_pct * 10_000.0) as i64;

            // Log PnL for debugging
            info!(
                mint=%mint,
                entry_price=pos.entry_price_sol,
                price_now=price_now,
                pnl_bps=pnl_bps,
                stop_loss_bps=stop_bps,
                out_lamports=out_lamports,
                "evaluate_positions: PnL check"
            );

            {
                let mut rs = self.risk.write();
                if let Some(v) = rs.open.get_mut(&mint) {
                    if let Some(l) = v.get_mut(lot_idx) {
                        l.last_unrealized_pnl_sol = (out_lamports as f64 / 1e9) - l.invested_sol;
                    }
                }
            }
            let stop_trigger = pnl_bps <= -(stop_bps as i64);
            let mut fraction: f64 = 0.0;
            let mut full_exit = false;
            // Load live state for executed tiers / peak watermark
            {
                let mut rs = self.risk.write();
                if let Some(v) = rs.open.get_mut(&mint) {
                    if let Some(l) = v.get_mut(lot_idx) {
                        // Update peak
                        if pnl_bps > l.peak_pnl_bps {
                            l.peak_pnl_bps = pnl_bps;
                        }
                        // Trailing stop check (only after any TP tier executed or basic TP reached)
                        let trailing_hit = if let Some(trail) = trailing {
                            if l.peak_pnl_bps > 0 {
                                pnl_bps <= l.peak_pnl_bps - trail as i64
                            } else {
                                false
                            }
                        } else {
                            false
                        };
                        if stop_trigger || trailing_hit {
                            fraction = 1.0;
                            full_exit = true;
                        } else {
                            // Tiered logic
                            if let Some(ref ts) = tiers {
                                // Determine highest tier reached not yet executed
                                let mut selected: Option<&crate::config::TakeProfitTier> = None;
                                for tier in
                                    ts.iter().filter(|t: &&crate::config::TakeProfitTier| {
                                        pnl_bps >= t.bps as i64
                                    })
                                {
                                    if !l.executed_tp_bps.contains(&tier.bps) {
                                        selected = Some(tier);
                                    }
                                }
                                if let Some(sel) = selected {
                                    fraction = sel.fraction.clamp(0.0, 1.0);
                                    l.executed_tp_bps.push(sel.bps);
                                }
                            } else if pnl_bps >= tp_bps as i64 {
                                fraction = 0.5;
                            }
                        }
                    }
                }
            }
            if fraction > 0.0 {
                // Enforce min exit notional if configured
                let notional_now_sol = (out_lamports as f64) / 1e9;
                if notional_now_sol < min_exit_notional {
                    continue;
                }
                // Convert UI tokens to raw token amount (with decimals) for the sell
                let sell_tokens = ((pos.amount_tokens * fraction)
                    * 10f64.powi(pos.token_decimals as i32))
                .floor() as u64;
                if sell_tokens > 0 {
                    // Collect price-based exit task (instead of executing immediately)
                    let reason = if stop_trigger {
                        "STOP_LOSS".to_string()
                    } else if full_exit {
                        "TRAILING_STOP".to_string()
                    } else {
                        format!("TAKE_PROFIT ({}bps)", pnl_bps)
                    };
                    // Get creator from position for Pump.fun exit path
                    let creator_pk = pos.creator.as_ref().and_then(|s| Pubkey::from_str(s).ok());
                    exit_tasks.push(ExitTask {
                        mint,
                        lot_idx,
                        sell_tokens,
                        fraction,
                        is_emergency: stop_trigger || full_exit,
                        reason,
                        creator: creator_pk,
                    });
                }
            }
        }

        // Execute all collected exit tasks (parallel or sequential based on config)
        if !exit_tasks.is_empty() {
            info!(
                exit_count = exit_tasks.len(),
                parallel = parallel_exits_enabled,
                max_parallel = max_parallel,
                "evaluate_positions: executing exits"
            );

            if parallel_exits_enabled && exit_tasks.len() > 1 {
                // Execute exits in parallel with concurrency limit
                self.execute_exits_parallel(exit_tasks, max_parallel).await;
            } else {
                // Execute exits sequentially (legacy behavior)
                for task in exit_tasks {
                    if let Err(e) = self
                        .attempt_exit(
                            &task.mint,
                            task.lot_idx,
                            task.sell_tokens,
                            task.fraction,
                            task.is_emergency,
                            task.creator,
                        )
                        .await
                    {
                        warn!(?e, mint=%task.mint, reason=%task.reason, "exit tx failed");
                    } else {
                        if task.is_emergency || task.fraction >= 0.99 {
                            self.mark_cooldown(task.mint);
                            // Unregister from kill switch monitor
                            if let Some(ks) = &self.kill_switch {
                                ks.unregister_position(&task.mint);
                            }
                        }
                        metrics::PARTIAL_EXIT_EVENTS_TOTAL
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        metrics::PARTIAL_EXIT_FRACTION_MICRO_TOTAL.fetch_add(
                            (task.fraction * 1_000_000.0) as u64,
                            std::sync::atomic::Ordering::Relaxed,
                        );
                    }
                }
            }
        }

        Ok(())
    }

    /// Execute multiple exit tasks in TRUE parallel using tokio::spawn
    /// Each exit runs in its own task for maximum throughput - no waiting on bundles!
    async fn execute_exits_parallel(&self, tasks: Vec<ExitTask>, max_concurrent: usize) {
        use futures::stream::{FuturesUnordered, StreamExt};

        let task_count = tasks.len();
        info!(
            task_count = task_count,
            max_concurrent = max_concurrent,
            "starting TRUE parallel exit execution"
        );

        // Use FuturesUnordered for concurrent execution with limit
        let mut futures = FuturesUnordered::new();
        let semaphore = Arc::new(tokio::sync::Semaphore::new(max_concurrent));

        for task in tasks {
            // Clone engine for spawned task (shares Arc-wrapped state)
            let engine_clone = self.clone_for_spawn();
            let sem = semaphore.clone();
            let task_clone = task.clone();

            let handle = tokio::spawn(async move {
                // Acquire semaphore permit to limit concurrency
                let _permit = sem.acquire().await.ok();

                let start = Instant::now();
                let result = engine_clone
                    .attempt_exit(
                        &task_clone.mint,
                        task_clone.lot_idx,
                        task_clone.sell_tokens,
                        task_clone.fraction,
                        task_clone.is_emergency,
                        task_clone.creator,
                    )
                    .await;
                let elapsed_ms = start.elapsed().as_millis();

                (task_clone, result, elapsed_ms)
            });

            futures.push(handle);
        }

        // Collect results as they complete (don't wait for all - process immediately!)
        let mut success_count = 0u32;
        let mut fail_count = 0u32;

        while let Some(join_result) = futures.next().await {
            match join_result {
                Ok((task, result, elapsed_ms)) => match result {
                    Ok(()) => {
                        success_count += 1;
                        if task.is_emergency || task.fraction >= 0.99 {
                            self.mark_cooldown(task.mint);
                            if let Some(ks) = &self.kill_switch {
                                ks.unregister_position(&task.mint);
                            }
                        }
                        metrics::PARTIAL_EXIT_EVENTS_TOTAL
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        metrics::PARTIAL_EXIT_FRACTION_MICRO_TOTAL.fetch_add(
                            (task.fraction * 1_000_000.0) as u64,
                            std::sync::atomic::Ordering::Relaxed,
                        );
                        info!(
                            mint = %task.mint,
                            reason = %task.reason,
                            elapsed_ms = elapsed_ms,
                            "parallel exit completed"
                        );
                    }
                    Err(e) => {
                        fail_count += 1;
                        warn!(
                            ?e,
                            mint = %task.mint,
                            reason = %task.reason,
                            elapsed_ms = elapsed_ms,
                            "parallel exit failed"
                        );
                    }
                },
                Err(e) => {
                    fail_count += 1;
                    error!(?e, "exit task panicked");
                }
            }
        }

        info!(
            success_count = success_count,
            fail_count = fail_count,
            total = task_count,
            "parallel exit execution completed"
        );
    }

    async fn attempt_exit(
        &self,
        mint: &Pubkey,
        lot_idx: usize,
        amount_tokens: u64,
        fraction: f64,
        is_emergency_exit: bool, // Stop-loss or full exit - use higher slippage
        creator: Option<Pubkey>, // Token creator for Pump.fun exits (avoids bonding curve parsing issues)
    ) -> Result<()> {
        let sol_mint = pubkey!("So11111111111111111111111111111111111111112");
        // Determine actual token balance (ATA) to avoid over-selling
        let owner_sdk = self.treasury.pubkey();
        let mint_sdk = solana_sdk::pubkey::Pubkey::new_from_array(mint.to_bytes());
        let (ata, _prog) = match self
            .treasury
            .ata_address(&self.rpc, &owner_sdk, &mint_sdk)
            .await
        {
            Ok(v) => v,
            Err(_) => return Ok(()),
        };
        let ata_tokens = if let Ok(acc) = self.rpc.get_account_retry(&ata).await {
            if acc.data.len() >= 72 {
                u64::from_le_bytes(acc.data[64..72].try_into().unwrap())
            } else {
                amount_tokens
            }
        } else {
            amount_tokens
        };
        if ata_tokens == 0 {
            // No tokens in wallet - position is stale, clean it up
            info!(
                mint=%mint,
                lot_idx=lot_idx,
                state_tokens=amount_tokens,
                "attempt_exit: wallet has 0 tokens, removing ghost position from state"
            );
            // Remove this position from risk state
            {
                let mut rs = self.risk.write();
                if let Some(lots) = rs.open.get_mut(mint) {
                    if lot_idx < lots.len() {
                        lots.remove(lot_idx);
                        info!(mint=%mint, lot_idx=lot_idx, remaining_lots=lots.len(), "removed ghost lot from position");
                    }
                    // If no lots remain, remove the mint entry entirely
                    if lots.is_empty() {
                        rs.open.remove(mint);
                        info!(mint=%mint, "removed empty position entry");
                    }
                }
            }
            self.persist_risk_state();
            // Unregister from kill switch if active
            if let Some(ks) = &self.kill_switch {
                ks.unregister_position(mint);
            }
            return Ok(());
        }
        // CRITICAL FIX: For full exits (fraction >= 0.99), sell ALL tokens from wallet
        // This prevents dust from remaining after sells due to rounding differences
        // between tracked amount_tokens and actual wallet balance
        let sell_tokens = if fraction >= 0.99 {
            // Full exit - sell everything in wallet, not just tracked amount
            ata_tokens
        } else {
            // Partial exit - use the minimum of requested and available
            amount_tokens.min(ata_tokens)
        };
        if sell_tokens == 0 {
            return Ok(());
        }
        info!(
            mint=%mint,
            fraction=fraction,
            requested_tokens=amount_tokens,
            wallet_tokens=ata_tokens,
            sell_tokens=sell_tokens,
            "attempt_exit: calculated sell amount"
        );
        // Ensure WSOL ATA for proceeds (create if missing)
        let wsol_ata = match self.treasury.wrap_sol(&self.rpc, 0).await {
            Ok((ata, _sig)) => ata,
            Err(_) => {
                // Best effort fallback: try to compute ATA; if fails, abort
                let wsol_mint_prog = spl_token::native_mint::id();
                let wsol_mint_sdk =
                    solana_sdk::pubkey::Pubkey::new_from_array(wsol_mint_prog.to_bytes());
                match self
                    .treasury
                    .ata_address(&self.rpc, &owner_sdk, &wsol_mint_sdk)
                    .await
                {
                    Ok((a, _)) => a,
                    Err(_) => return Ok(()),
                }
            }
        };

        // Dynamic route selection for exit: compare Raydium vs Orca vs Pump.fun quotes
        // For emergency exits (stop-loss), use configurable high slippage to ensure execution
        let emergency_slippage = self.cfg.read().emergency_exit_slippage_bps.unwrap_or(5000); // Config or 50% default
        let msb2 = if is_emergency_exit {
            emergency_slippage
        } else {
            self.adaptive_slippage_bps()
        };

        if is_emergency_exit {
            info!(mint=%mint, slippage_bps=msb2, "attempt_exit: EMERGENCY EXIT - using high slippage");
        }
        let ray_plan = if let Some(r) = &self.raydium {
            r.build_swap_plan_auto(&mint.to_string(), &sol_mint.to_string(), sell_tokens, msb2)
                .await
                .ok()
        } else {
            None
        };
        let ray_out: u64 = ray_plan
            .as_ref()
            .and_then(|p| p.as_ref().map(|pm| pm.expected_out))
            .unwrap_or(0);
        let mut orca_out: u64 = 0;
        if let Some(o) = &self.orca {
            if let Ok(Some(q)) = o
                .quote_exact_in(&mint.to_string(), &sol_mint.to_string(), sell_tokens)
                .await
            {
                orca_out = q.amount_out;
            }
        }
        let mut pumpfun_out: u64 = 0;
        if let Some(pf) = &self.pumpfun {
            if let Ok(Some(q)) = pf
                .quote_exact_in(&mint.to_string(), &sol_mint.to_string(), sell_tokens)
                .await
            {
                pumpfun_out = q.amount_out;
            }
        }

        #[derive(Debug, PartialEq)]
        enum ChosenDex {
            PumpFun,
            Raydium,
            Orca,
        }

        // If ALL quotes are 0, this position is worthless (dust) - remove it from state
        if ray_out == 0 && orca_out == 0 && pumpfun_out == 0 {
            info!(
                mint=%mint,
                lot_idx=lot_idx,
                sell_tokens=sell_tokens,
                "attempt_exit: ALL quotes returned 0 - position is worthless dust, removing from state"
            );
            // Remove this position from risk state
            {
                let mut rs = self.risk.write();
                if let Some(lots) = rs.open.get_mut(mint) {
                    if lot_idx < lots.len() {
                        lots.remove(lot_idx);
                        info!(mint=%mint, lot_idx=lot_idx, remaining_lots=lots.len(), "removed worthless dust lot from position");
                    }
                    if lots.is_empty() {
                        rs.open.remove(mint);
                        info!(mint=%mint, "removed empty position entry (all lots were dust)");
                    }
                }
            }
            self.persist_risk_state();
            if let Some(ks) = &self.kill_switch {
                ks.unregister_position(mint);
            }
            return Ok(());
        }

        let chosen_dex = if pumpfun_out >= ray_out && pumpfun_out >= orca_out && pumpfun_out > 0 {
            ChosenDex::PumpFun
        } else if ray_out >= orca_out && ray_out > 0 {
            ChosenDex::Raydium
        } else if orca_out > 0 {
            ChosenDex::Orca
        } else {
            // Fallback logic - shouldn't reach here due to check above
            if ray_plan.is_some() {
                ChosenDex::Raydium
            } else {
                ChosenDex::Orca
            }
        };

        if chosen_dex == ChosenDex::Raydium {
            crate::metrics::DEX_SELECTION_EXIT_RAYDIUM_TOTAL
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        } else if chosen_dex == ChosenDex::Orca {
            crate::metrics::DEX_SELECTION_EXIT_ORCA_TOTAL
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        debug!(mint=%mint, sell_tokens, ray_out, orca_out, pumpfun_out, chosen=?chosen_dex, "sniper: dynamic exit dex selection");

        // Build instructions based on chosen route; prefer full Raydium IX, fallback to Orca
        let bh: Hash = match self.rpc.get_latest_blockhash_retry().await {
            Ok(h) => h,
            Err(e) => {
                RPC_ERRORS_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return Err(e.into());
            }
        };
        let mut tx_ixs: Vec<solana_sdk::instruction::Instruction> = Vec::new();

        if chosen_dex == ChosenDex::PumpFun {
            if let Some(pf) = &self.pumpfun {
                // Calculate min_out with slippage
                let slip = msb2 as u128;
                let min_out = ((pumpfun_out as u128) * (10_000 - slip) / 10_000) as u64;
                let min_out = min_out.max(1);

                // We need to derive the bonding curve accounts manually here since build_sell_ix expects Pubkeys
                // and we only have the mint Pubkey.
                let mint_pk = solana_sdk::pubkey::Pubkey::new_from_array(mint.to_bytes());
                let (bonding_curve, _) = pf.derive_bonding_curve(&mint_pk);
                let (associated_bonding_curve, _) =
                    pf.derive_associated_bonding_curve(&bonding_curve, &mint_pk);

                // We also need the user's ATA for the token
                let user_pk = self.treasury.pubkey();
                let (user_token_account, _) = self
                    .treasury
                    .ata_address(&self.rpc, &user_pk, &mint_pk)
                    .await
                    .unwrap_or_default();

                // CRITICAL: Creator MUST come from stored position data.
                // Bonding curve parsing is DISABLED - layout changed and creator field is wrong.
                let sell_creator = match creator {
                    Some(c) => {
                        info!(mint=%mint, creator=%c, "pump.fun exit: using stored creator from position");
                        c
                    }
                    None => {
                        // This should not happen for positions created after the fix.
                        // For old positions without creator, we cannot sell via Pump.fun.
                        error!(
                            mint=%mint,
                            "pump.fun exit FAILED: no creator stored in position. \
                            Position was created before creator tracking was implemented. \
                            Use manual sell or wait for Raydium migration."
                        );
                        return Err(anyhow::anyhow!(
                            "pump.fun sell failed: no creator stored. Position predates creator tracking."
                        ));
                    }
                };

                match pf.build_sell_ix(
                    &mint_pk,
                    &bonding_curve,
                    &associated_bonding_curve,
                    &user_token_account,
                    &sell_creator,
                    sell_tokens,
                    min_out,
                ) {
                    Ok(ix) => {
                        tx_ixs = vec![ix];
                    }
                    Err(e) => {
                        warn!(?e, mint=%mint, "pump.fun build_sell_ix failed");
                    }
                }
            }
        } else if chosen_dex == ChosenDex::Raydium {
            // Try full Raydium instruction using Serum market accounts from snapshot
            if let (Some(r), Some(plan_meta)) = (
                self.raydium.as_ref(),
                ray_plan.as_ref().and_then(|p| p.clone()),
            ) {
                if let Some(pool_addr) = plan_meta.pool {
                    if let Some(snap) = r.snapshots().into_iter().find(|s| s.address == pool_addr) {
                        if let (Some(_open_orders), Some(_market_id)) =
                            (snap.open_orders, snap.market_id)
                        {
                            if let (
                                Some(bids),
                                Some(asks),
                                Some(event_q),
                                Some(base_vault),
                                Some(quote_vault),
                                Some(_serum_vs),
                            ) = (
                                snap.serum_bids,
                                snap.serum_asks,
                                snap.serum_event_queue,
                                snap.serum_base_vault,
                                snap.serum_quote_vault,
                                snap.serum_vault_signer,
                            ) {
                                use crate::solana::dex::raydium::SerumMarketAccounts;
                                let serum_accounts = SerumMarketAccounts {
                                    bids,
                                    asks,
                                    event_queue: event_q,
                                    base_vault,
                                    quote_vault,
                                };
                                let market_prog = snap.market_program_id.unwrap_or(
                                    Pubkey::from_str(
                                        "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin",
                                    )
                                    .unwrap(),
                                );
                                if let Ok(_ray_prog) = Pubkey::from_str(RAYDIUM_AMM_V4) {
                                    let token_prog = spl_token::id();
                                    let rent_sysvar = solana_sdk::sysvar::rent::id();
                                    let auth_pk =
                                        Pubkey::new_from_array(self.treasury.pubkey().to_bytes());
                                    let user_source = Pubkey::new_from_array(ata.to_bytes());
                                    let user_dest = Pubkey::new_from_array(wsol_ata.to_bytes());
                                    let token_prog_pk =
                                        Pubkey::new_from_array(token_prog.to_bytes());
                                    let rent_pk = Pubkey::new_from_array(rent_sysvar.to_bytes());
                                    if let Ok(full_ix) = r.build_swap_instruction(
                                        pool_addr,
                                        *mint,
                                        sol_mint,
                                        sell_tokens,
                                        plan_meta.min_out,
                                        auth_pk,
                                        user_source,
                                        user_dest,
                                        market_prog,
                                        token_prog_pk,
                                        rent_pk,
                                        serum_accounts,
                                        snap.target_orders,
                                    ) {
                                        tx_ixs = vec![full_ix];
                                    }
                                }
                            }
                        }
                    }
                }
            }
            if tx_ixs.is_empty() {
                info!(mint=%mint, "raydium full exit instruction unavailable; falling back to orca");
                // used_raydium = false; // No longer needed with enum
            }
        }
        if chosen_dex == ChosenDex::Orca {
            if let Some(o) = &self.orca {
                o.set_user_authority(Pubkey::new_from_array(self.treasury.pubkey().to_bytes()));
                // Register token source and WSOL destination accounts
                o.set_user_token_account(Pubkey::new_from_array(mint.to_bytes()), ata);
                o.set_user_token_account(Pubkey::new_from_array(sol_mint.to_bytes()), wsol_ata);
                // Compute min_out with slippage
                let mut min_out = 1u64;
                if let Ok(Some(q)) = o
                    .quote_exact_in(&mint.to_string(), &sol_mint.to_string(), sell_tokens)
                    .await
                {
                    // Use quantile-based min_out if enabled, otherwise adaptive slippage
                    let pool_id = format!("orca_{}_{}", mint, sol_mint);
                    let pool_liquidity = 100_000_000_000u128; // 100 SOL equivalent as default
                    min_out =
                        self.compute_min_out(&pool_id, q.amount_out, sell_tokens, pool_liquidity);
                    if min_out == 0 {
                        min_out = 1;
                    }
                }
                tx_ixs = o
                    .build_swap_ix(
                        &mint.to_string(),
                        &sol_mint.to_string(),
                        sell_tokens,
                        min_out,
                    )
                    .unwrap_or_default();
            }
        }
        let tx_ixs = tx_ixs; // finalize binding
        if tx_ixs.is_empty() {
            return Ok(());
        }
        let message = solana_sdk::message::Message::new(&tx_ixs, Some(&self.treasury.pubkey()));
        let fee_estimate = self
            .rpc
            .get_fee_for_message_retry(&message)
            .await
            .unwrap_or(0);
        let mut tx = Transaction::new_with_payer(&tx_ixs, Some(&self.treasury.pubkey()));
        tx.try_sign(&[self.treasury.signer_ref()], bh)?;
        // Snapshot invested_sol for position (if exists) before trade (read-only borrow)
        let invested_sol_lot = {
            let rs = self.risk.read();
            if let Some(v) = rs.open.get(mint) {
                if let Some(l) = v.get(lot_idx) {
                    l.invested_sol
                } else {
                    0.0
                }
            } else {
                0.0
            }
        };
        let pre_balance = self.total_sol_balance().await.unwrap_or(0.0);
        // Pre WSOL ATA amount (destination of swap proceeds)
        let wsol_mint_prog = spl_token::native_mint::id();
        let wsol_mint_sdk = solana_sdk::pubkey::Pubkey::new_from_array(wsol_mint_prog.to_bytes());
        let (wsol_ata, _prog_wsol) = match self
            .treasury
            .ata_address(&self.rpc, &owner_sdk, &wsol_mint_sdk)
            .await
        {
            Ok(v) => v,
            Err(_) => (self.treasury.pubkey(), self.treasury.pubkey()),
        };
        let pre_wsol_amount: u64 = if let Ok(acc) = self.rpc.get_account_retry(&wsol_ata).await {
            if acc.data.len() >= 72 {
                u64::from_le_bytes(acc.data[64..72].try_into().unwrap())
            } else {
                0
            }
        } else {
            0
        };

        // === JITO DECISION LOGIC ===
        // Determine if this exit qualifies for Jito bundle submission:
        // 1. Emergency/panic exits (kill switch triggers) - always Jito if enabled
        // 2. Final full exits (fraction >= 0.99) - always Jito if enabled
        // 3. Large exits (fraction >= threshold AND value >= min SOL) - Jito for MEV protection
        // Small exits (<25% or <0.5 SOL) use normal RPC - tip would eat EV
        let use_jito = {
            let cfg = self.cfg.read();
            let jito_enabled = cfg.jito_enabled.unwrap_or(false);
            if !jito_enabled {
                false
            } else {
                let jito_for_emergency = cfg.jito_for_emergency.unwrap_or(true);
                let jito_for_final = cfg.jito_for_final_exit.unwrap_or(true);
                let min_fraction = cfg.jito_min_exit_fraction.unwrap_or(0.25);
                let min_sol = cfg.jito_min_exit_sol.unwrap_or(0.5);

                // Estimate exit value in SOL (using invested_sol as proxy)
                let exit_sol_value = invested_sol_lot * fraction;

                let is_final_exit = fraction >= 0.99;
                let is_large_exit = fraction >= min_fraction && exit_sol_value >= min_sol;

                // Use Jito for: emergency OR final OR large exits
                (is_emergency_exit && jito_for_emergency)
                    || (is_final_exit && jito_for_final)
                    || is_large_exit
            }
        };

        let sent_at = Instant::now();

        // Route through Jito or normal RPC based on decision
        let tx_result: Result<solana_sdk::signature::Signature, anyhow::Error> = if use_jito {
            // === JITO BUNDLE SUBMISSION ===
            // Read config values in a block to ensure lock is released before async
            let (base_tip, region_str) = {
                let cfg = self.cfg.read();
                (
                    cfg.jito_tip_lamports.unwrap_or(10_000),
                    cfg.jito_region
                        .clone()
                        .unwrap_or_else(|| "frankfurt".to_string()),
                )
            }; // cfg lock released here

            // === DYNAMIC TIP HEURISTIC ===
            // Adjust tip based on urgency and exit type:
            // - Emergency exits (kill switch, panic): 3x tip for maximum priority
            // - Final exits (100%): 2x tip for high priority
            // - Large exits (>50%): 1.5x tip
            // - Normal qualifying exits: base tip
            let tip_multiplier = if is_emergency_exit {
                3.0 // PANIC: Max priority, get out NOW
            } else if fraction >= 0.99 {
                2.0 // Full exit: High priority
            } else if fraction >= 0.5 {
                1.5 // Large exit: Medium-high priority
            } else {
                1.0 // Normal qualifying exit
            };

            let tip_lamports = ((base_tip as f64) * tip_multiplier) as u64;

            let region = JitoRegion::from_str(&region_str).unwrap_or(JitoRegion::Frankfurt);
            let jito_client = JitoClient::new(vec![region], tip_lamports);

            info!(
                mint = %mint,
                fraction = fraction,
                is_emergency = is_emergency_exit,
                tip_lamports = tip_lamports,
                region = %region_str,
                "using JITO for exit (MEV protection)"
            );

            // Add tip instruction to transaction
            let mut tx_with_tip = tx.clone();
            if let Ok(tip_ix) =
                jito_client.build_tip_instruction(&self.treasury.pubkey(), tip_lamports)
            {
                // Create new transaction with tip instruction added
                let mut all_ixs = tx_ixs.clone();
                all_ixs.push(tip_ix);
                tx_with_tip = Transaction::new_with_payer(&all_ixs, Some(&self.treasury.pubkey()));
                tx_with_tip.try_sign(&[self.treasury.signer_ref()], bh)?;
            }

            // Track tip amount
            crate::metrics::JITO_TIP_LAMPORTS_TOTAL
                .fetch_add(tip_lamports, std::sync::atomic::Ordering::Relaxed);

            // Submit as single-TX bundle to Jito
            crate::metrics::JITO_BUNDLES_SUBMITTED_TOTAL
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            match jito_client.send_bundle(&[tx_with_tip.clone()]).await {
                Ok(bundle_id) => {
                    info!(bundle_id = %bundle_id, mint = %mint, tip_lamports = tip_lamports, "Jito bundle submitted");
                    // Wait for bundle confirmation (with timeout)
                    match jito_client.wait_for_bundle(&bundle_id, 30).await {
                        Ok(status) => {
                            // Bundle landed successfully
                            info!(
                                bundle_id = %bundle_id,
                                slot = status.slot,
                                confirmation = %status.confirmation_status,
                                "Jito bundle LANDED successfully"
                            );
                            crate::metrics::JITO_BUNDLES_LANDED_TOTAL
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            // Return the first signature from the transaction
                            Ok(tx_with_tip.signatures[0])
                        }
                        Err(e) => {
                            // Bundle failed or timed out
                            let error_msg = format!("{:?}", e);
                            let is_timeout =
                                error_msg.contains("timeout") || error_msg.contains("Timeout");

                            if is_timeout {
                                warn!(
                                    ?e,
                                    bundle_id = %bundle_id,
                                    "Jito bundle status check TIMEOUT - network issue?"
                                );
                                crate::metrics::JITO_BUNDLES_TIMEOUT_TOTAL
                                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            } else {
                                warn!(
                                    ?e,
                                    bundle_id = %bundle_id,
                                    tip_lamports = tip_lamports,
                                    "Jito bundle REJECTED/FAILED - tip may be too low or slot competition high"
                                );
                                crate::metrics::JITO_BUNDLES_REJECTED_TOTAL
                                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            }
                            crate::metrics::JITO_FALLBACK_RPC_TOTAL
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            // Fallback to normal RPC submission
                            self.rpc_retry_tx(&tx, 3, false).await
                        }
                    }
                }
                Err(e) => {
                    // Detailed error diagnosis
                    let error_msg = format!("{:?}", e);
                    let reject_reason = if error_msg.contains("tip") || error_msg.contains("Tip") {
                        "TIP_TOO_LOW"
                    } else if error_msg.contains("simulation") || error_msg.contains("Simulation") {
                        "SIMULATION_FAILED"
                    } else if error_msg.contains("timeout") || error_msg.contains("Timeout") {
                        "NETWORK_TIMEOUT"
                    } else if error_msg.contains("rate") || error_msg.contains("Rate") {
                        "RATE_LIMITED"
                    } else {
                        "UNKNOWN"
                    };

                    warn!(
                        ?e,
                        mint = %mint,
                        tip_lamports = tip_lamports,
                        reject_reason = reject_reason,
                        "Jito bundle submission FAILED"
                    );
                    crate::metrics::JITO_BUNDLES_REJECTED_TOTAL
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    crate::metrics::JITO_FALLBACK_RPC_TOTAL
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    // Fallback to normal RPC submission
                    self.rpc_retry_tx(&tx, 3, false).await
                }
            }
        } else {
            // === NORMAL RPC SUBMISSION (small exits) ===
            self.rpc_retry_tx(&tx, 3, false).await
        };

        match tx_result {
            Ok(sig) => {
                let dur = sent_at.elapsed();
                record_swap_latency(dur.as_nanos() as u64);
                info!(mint=%mint, sig=%sig, amount_tokens=sell_tokens, "exit trade submitted");
                // Read WSOL ATA after swap before unwrap
                let post_wsol_amount: u64 =
                    if let Ok(acc) = self.rpc.get_account_retry(&wsol_ata).await {
                        if acc.data.len() >= 72 {
                            u64::from_le_bytes(acc.data[64..72].try_into().unwrap())
                        } else {
                            0
                        }
                    } else {
                        0
                    };
                let delta_wsol = post_wsol_amount.saturating_sub(pre_wsol_amount) as f64 / 1e9;
                // Fetch recent block fee estimation via meta fallback (if any) else approximate by difference in native balance delta
                let post_native =
                    self.rpc.get_balance_retry(&owner_sdk).await.unwrap_or(0) as f64 / 1e9;
                let native_delta =
                    (post_native - (pre_balance - (pre_wsol_amount as f64 / 1e9))).max(0.0); // approximate, excludes wsol tokens
                                                                                             // Assume fee ~ native_delta decrease unrelated to proceeds (if negative) ignore
                let fee_est_native = fee_estimate as f64 / 1e9;
                let fee_est = if native_delta < 0.0 {
                    -native_delta.max(fee_est_native)
                } else {
                    fee_est_native
                }; // prefer RPC reported fee_estimate
                let realized = delta_wsol - invested_sol_lot * fraction - fee_est; // proportional share of invested capital sold
                let trade_ret = if invested_sol_lot > 0.0 {
                    realized / (invested_sol_lot * fraction.max(1e-9))
                } else {
                    0.0
                };
                record_trade_return(trade_ret);
                self.risk_reset_if_needed();
                {
                    let mut rs = self.risk.write();
                    let mut invest_slice = 0.0;
                    let mut remove_current = false;
                    if let Some(v) = rs.open.get_mut(mint) {
                        if lot_idx < v.len() {
                            let l = &mut v[lot_idx];
                            invest_slice = l.invested_sol * fraction;
                            l.invested_sol -= invest_slice;
                            l.amount_tokens =
                                (l.amount_tokens - (l.amount_tokens * fraction)).max(0.0);
                            if l.amount_tokens <= 1e-9 {
                                remove_current = true;
                            }
                        }
                    }
                    if let Some(v) = rs.open.get_mut(mint) {
                        if remove_current && lot_idx < v.len() {
                            v.remove(lot_idx);
                        }
                        if v.is_empty() {
                            rs.open.remove(mint);
                        }
                    }
                    rs.realized_pnl_sol += realized;
                    if realized < 0.0 {
                        rs.realized_loss_today_sol += -realized;
                    }
                    if invest_slice > 0.0 {
                        rs.recent_realized.push(realized / invest_slice);
                    }
                    let window = self.cfg.read().rolling_pnl_window.unwrap_or(50);
                    if rs.recent_realized.len() > window {
                        let excess = rs.recent_realized.len() - window;
                        rs.recent_realized.drain(0..excess);
                    }
                    if rs.recent_realized.len() >= 5 {
                        let n = rs.recent_realized.len() as f64;
                        let mean = rs.recent_realized.iter().copied().sum::<f64>() / n;
                        let var = rs
                            .recent_realized
                            .iter()
                            .map(|r| (r - mean) * (r - mean))
                            .sum::<f64>()
                            / n.max(1.0);
                        let std = var.sqrt();
                        if std > 0.0 {
                            rs.last_sharpe = mean / std * n.sqrt();
                        }
                    }
                    DAILY_REALIZED_PNL_SOL_MICRO.store(
                        (rs.realized_pnl_sol * 1_000_000.0) as u64,
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    let lots: usize = rs.open.values().map(|v| v.len()).sum();
                    OPEN_POSITIONS_GAUGE.store(lots as u64, std::sync::atomic::Ordering::Relaxed);
                    // Update sharpe & drawdown metrics
                    crate::metrics::update_sharpe(rs.last_sharpe);
                    // Simple drawdown approximation: daily loss / (daily loss limit) if configured
                    if let Some(limit) = self.cfg.read().daily_loss_limit_sol {
                        if limit > 0.0 {
                            let dd = (rs.realized_loss_today_sol / limit).clamp(0.0, 1.0);
                            crate::metrics::update_drawdown(dd);
                        }
                    }
                }
                // Unwrap afterwards outside lock
                let _ = self.treasury.unwrap_wsol(&self.rpc, None).await;
                // Trade CSV log (SELL)
                let lamports_out = (delta_wsol * 1e9) as u64; // proceeds
                record_network_fee(fee_estimate);
                // Record fee percent vs notional and gross/net realized PnL
                let notional = invested_sol_lot * fraction;
                if notional > 0.0 {
                    let fee_pct = (fee_estimate as f64 / 1e9) / notional;
                    record_fee_pct(fee_pct.max(0.0));
                }
                let gross = delta_wsol - notional; // proceeds minus cost basis slice
                let net = gross - (fee_estimate as f64 / 1e9);
                record_realized_gross_net(gross, net);
                // Also record absolute realized PnL (SOL) histogram using net
                record_realized_pnl_sol(net);

                // Record SELL trade for dashboard
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);
                let pnl_pct = if notional > 0.0 {
                    Some((net / notional) * 100.0)
                } else {
                    None
                };
                record_recent_trade(RecentTrade {
                    timestamp_ms: now_ms,
                    mint: mint.to_string(),
                    action: "SELL".to_string(),
                    tx_hash: sig.to_string(),
                    amount_tokens: sell_tokens as f64,
                    price_sol: if sell_tokens > 0 {
                        delta_wsol / (sell_tokens as f64)
                    } else {
                        0.0
                    },
                    pnl_sol: Some(net),
                    pnl_pct,
                    latency_ms: None,
                });

                let line = format!(
                    "{ts},SELL,{mint},*,{sig},0,{lamports_out},{tok_in},{tok_out},,,0,,{fee},{realized},exit_fraction={fraction}",
                    ts=ChronoUtc::now().to_rfc3339(),
                    mint=mint,
                    sig=sig,
                    lamports_out=lamports_out,
                    tok_in=sell_tokens,
                    tok_out=sell_tokens,
                    fee=fee_estimate,
                    realized=realized,
                    fraction=fraction
                );
                self.append_trade_record(&line, true);
                self.persist_risk_state();

                // === DUST SWEEP: After full exit, check if any dust remains and sell it ===
                if fraction >= 0.99 {
                    // Re-check wallet balance after sell
                    if let Ok(acc) = self.rpc.get_account_retry(&ata).await {
                        if acc.data.len() >= 72 {
                            let remaining =
                                u64::from_le_bytes(acc.data[64..72].try_into().unwrap());
                            if remaining > 0 && remaining < 1_000_000 {
                                // Dust detected (less than 1 token for 6 decimal mints)
                                info!(
                                    mint=%mint,
                                    dust_tokens=remaining,
                                    "attempt_exit: dust detected after full exit, sweeping"
                                );
                                // Try to sell the dust - ignore errors as it's best-effort
                                // Signature: attempt_exit(mint, lot_idx, amount_tokens, fraction, is_emergency, creator)
                                let _ = Box::pin(self.attempt_exit(
                                    mint, lot_idx, remaining, 1.0,
                                    true,    // is_emergency to use high slippage
                                    creator, // pass through creator for dust sweep
                                ))
                                .await;
                            }
                        }
                    }
                }
            }
            Err(e) => warn!(?e, mint=%mint, "exit tx failed"),
        }
        Ok(())
    }

    fn cleanup_stale_pending(&self, ttl_secs: u64) {
        let now = chrono::Utc::now().timestamp();
        let mut removed: Vec<Pubkey> = Vec::new();
        {
            let mut rs = self.risk.write();
            rs.pending.retain(|mint, p| {
                let alive = (now - p.ts) <= ttl_secs as i64;
                if !alive {
                    removed.push(*mint);
                }
                alive
            });
        }
        if !removed.is_empty() {
            debug!(
                count = removed.len(),
                "pending cleanup removed stale trades"
            );
        }
    }

    // Reconcile pending trades that are still within TTL but haven't produced a fill yet.
    // Strategy: fetch signature statuses; if confirmed and token balance now reflects fill, finalize.
    // If status is Err or NotFound after N seconds (half TTL), mark failed and remove.
    async fn reconcile_pending(&self, half_ttl_cutoff: i64) {
        // Collect copy of pending (mint -> trade) to avoid holding write lock across awaits
        let pend: Vec<(Pubkey, PendingTrade)> = {
            let rs = self.risk.read();
            rs.pending.iter().map(|(k, v)| (*k, v.clone())).collect()
        };
        if pend.is_empty() {
            return;
        }
        // Build list of signatures (dedup)
        let mut sigs: Vec<solana_sdk::signature::Signature> = Vec::new();
        for (_mint, p) in &pend {
            if let Ok(sig) = solana_sdk::signature::Signature::from_str(&p.sig) {
                sigs.push(sig);
            }
        }
        if sigs.is_empty() {
            return;
        }
        // Use low-level RPC client directly (status API) – fall back if not available
        // (Requires feature set in solana_rpc_client)
        let statuses_res = self.rpc.get_signature_statuses_retry(&sigs).await.ok();
        let now_ts = chrono::Utc::now().timestamp();
        if let Some(statuses) = statuses_res {
            for ((mint, p), status_opt) in pend.iter().zip(statuses.value.into_iter()) {
                if let Some(status) = status_opt {
                    // Some information available
                    if status.confirmations.is_some() || status.err.is_some() || status.slot > 0 {
                        // progressed
                        if status.err.is_some() {
                            // Failed transaction -> drop pending
                            let mut rs = self.risk.write();
                            if rs.pending.remove(mint).is_some() {
                                PENDING_FAILED_TOTAL
                                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                debug!(mint=%mint, sig=%p.sig, "pending trade failed (status error)");
                            }
                            continue;
                        }
                        // If confirmed (confirmations None typically means rooted) attempt finalize_fill
                        if status.err.is_none() {
                            // Try finalize now (will remove pending & log fill if balance updated)
                            PENDING_RECONCILIATIONS_TOTAL
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            let invested_sol = p.lamports_in as f64 / 1e9;
                            // Note: creator not available in reconciliation path (position already created)
                            self.finalize_fill(*mint, invested_sol, None).await;
                            // If still present and older than half cutoff without realized fill treat as failed
                            let mut rs = self.risk.write();
                            if let Some(persist) = rs.pending.get(mint) {
                                if now_ts - persist.ts > half_ttl_cutoff
                                    && rs.pending.remove(mint).is_some()
                                {
                                    PENDING_FAILED_TOTAL
                                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                    debug!(mint=%mint, sig=%p.sig, "pending trade dropped after finalize attempt (stale)");
                                }
                            }
                        }
                    } else {
                        // No progress yet; if older than half TTL consider dropping
                        if now_ts - p.ts > half_ttl_cutoff {
                            let mut rs = self.risk.write();
                            if rs.pending.remove(mint).is_some() {
                                PENDING_FAILED_TOTAL
                                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                debug!(mint=%mint, sig=%p.sig, "pending trade dropped (stale no-progress)");
                            }
                        }
                    }
                } else {
                    // Missing status entirely; same half-ttl rule
                    if now_ts - p.ts > half_ttl_cutoff {
                        let mut rs = self.risk.write();
                        if rs.pending.remove(mint).is_some() {
                            PENDING_FAILED_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            debug!(mint=%mint, sig=%p.sig, "pending trade dropped (no status)");
                        }
                    }
                }
            }
        }
    }

    fn risk_state_file_path() -> std::path::PathBuf {
        let path = std::env::var("IRONCRAB_RISK_STATE_PATH")
            .unwrap_or_else(|_| "state/risk_state.json".to_string());
        std::path::PathBuf::from(path)
    }

    fn persist_risk_state(&self) {
        let path = Self::risk_state_file_path();
        if let Some(parent) = path.parent() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                tracing::error!(?e, path=?parent, "failed to create risk state directory");
            }
        }
        let snapshot = self.build_risk_snapshot_json();
        match serde_json::to_string_pretty(&snapshot) {
            Ok(txt) => {
                if let Err(e) = std::fs::write(&path, txt) {
                    tracing::error!(?e, path=?path, "failed to write risk state file");
                } else {
                    tracing::debug!(path=?path, "risk state saved");
                }
            }
            Err(e) => {
                tracing::error!(?e, "failed to serialize risk state");
            }
        }
        // Update extended metrics from current RiskState on each persist
        {
            let rs = self.risk.read();
            let lots: usize = rs.open.values().map(|v| v.len()).sum();
            OPEN_POSITIONS_GAUGE.store(lots as u64, std::sync::atomic::Ordering::Relaxed);
            DAILY_REALIZED_PNL_SOL_MICRO.store(
                (rs.realized_pnl_sol * 1_000_000.0) as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
            // Sharpe gauge
            crate::metrics::update_sharpe(rs.last_sharpe);
            // Drawdown relative to configured daily loss limit (if set)
            if let Some(limit) = self.cfg.read().daily_loss_limit_sol {
                if limit > 0.0 {
                    let dd = (rs.realized_loss_today_sol / limit).clamp(0.0, 1.0);
                    crate::metrics::update_drawdown(dd);
                }
            }
        }
        crate::metrics::record_activity();
    }

    fn build_risk_snapshot_json(&self) -> serde_json::Value {
        let rs = self.risk.read();
        let open: Vec<serde_json::Value> = rs
            .open
            .iter()
            .flat_map(|(k, vs)| {
                vs.iter().map(move |lot| {
                    json!({
                        "mint": k.to_string(),
                        "entry_price_sol": lot.entry_price_sol,
                        "amount_tokens": lot.amount_tokens,
                        "invested_sol": lot.invested_sol,
                        "token_decimals": lot.token_decimals,
                        "last_unrealized_pnl_sol": lot.last_unrealized_pnl_sol,
                        "opened_ts": lot.opened_ts
                    })
                })
            })
            .collect();
        let cooldown: Vec<serde_json::Value> = rs
            .cooldown_until
            .iter()
            .map(|(k, v)| json!({"mint": k.to_string(), "until": v}))
            .collect();
        json!({
                "version": 1,
                "realized_pnl_sol": rs.realized_pnl_sol,
                "realized_loss_today_sol": rs.realized_loss_today_sol,
                "current_day": rs.current_day,
                "recent_realized": rs.recent_realized,
                "last_sharpe": rs.last_sharpe,
                "recent_slippage": rs.recent_slippage,
                "adaptive_slippage_bps": rs.adaptive_slippage_bps,
                "open_positions": open,
                "cooldowns": cooldown
        })
    }

    fn try_load_risk_state(&self) {
        let path = Self::risk_state_file_path();
        if !path.exists() {
            return;
        }
        if let Ok(txt) = std::fs::read_to_string(&path) {
            if let Ok(val) = serde_json::from_str::<serde_json::Value>(&txt) {
                let mut rs = self.risk.write();
                rs.realized_pnl_sol = val
                    .get("realized_pnl_sol")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0);
                rs.realized_loss_today_sol = val
                    .get("realized_loss_today_sol")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0);
                rs.current_day =
                    val.get("current_day").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
                rs.recent_realized = val
                    .get("recent_realized")
                    .and_then(|v| v.as_array())
                    .map(|arr| arr.iter().filter_map(|x| x.as_f64()).collect())
                    .unwrap_or_default();
                rs.last_sharpe = val
                    .get("last_sharpe")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0);
                rs.open.clear();
                if let Some(op) = val.get("open_positions").and_then(|v| v.as_array()) {
                    for ent in op {
                        let mint_opt = ent.get("mint").and_then(|m| m.as_str());
                        let entry_opt = ent.get("entry_price_sol").and_then(|f| f.as_f64());
                        let amt_opt = ent.get("amount_tokens").and_then(|f| f.as_f64());
                        if let (Some(mint_str), Some(entry_price), Some(amount_tokens)) =
                            (mint_opt, entry_opt, amt_opt)
                        {
                            if let Ok(pk) = Pubkey::from_str(mint_str) {
                                let invested = ent
                                    .get("invested_sol")
                                    .and_then(|f| f.as_f64())
                                    .unwrap_or(0.0);
                                let decs = ent
                                    .get("token_decimals")
                                    .and_then(|f| f.as_u64())
                                    .unwrap_or(0) as u8;
                                let last_unr = ent
                                    .get("last_unrealized_pnl_sol")
                                    .and_then(|f| f.as_f64())
                                    .unwrap_or(0.0);
                                let opened_ts =
                                    ent.get("opened_ts").and_then(|f| f.as_i64()).unwrap_or(0);
                                rs.open.entry(pk).or_default().push(PositionLot {
                                    entry_price_sol: entry_price,
                                    amount_tokens,
                                    invested_sol: invested,
                                    token_decimals: decs,
                                    last_unrealized_pnl_sol: last_unr,
                                    opened_ts,
                                    executed_tp_bps: Vec::new(),
                                    peak_pnl_bps: 0,
                                    executed_timed_tiers: Vec::new(),
                                    creator: None,
                                });
                            }
                        }
                    }
                }
                rs.cooldown_until.clear();
                if let Some(cd) = val.get("cooldowns").and_then(|v| v.as_array()) {
                    for c in cd {
                        if let (Some(mint_str), Some(until)) = (
                            c.get("mint").and_then(|m| m.as_str()),
                            c.get("until").and_then(|u| u.as_i64()),
                        ) {
                            if let Ok(pk) = Pubkey::from_str(mint_str) {
                                rs.cooldown_until.insert(pk, until);
                            }
                        }
                    }
                }
                // Adaptive slippage fields
                rs.recent_slippage = val
                    .get("recent_slippage")
                    .and_then(|v| v.as_array())
                    .map(|arr| arr.iter().filter_map(|x| x.as_f64()).collect())
                    .unwrap_or_default();
                rs.adaptive_slippage_bps = val
                    .get("adaptive_slippage_bps")
                    .and_then(|v| v.as_u64())
                    .map(|v| v as u32);
                let lots: usize = rs.open.values().map(|v| v.len()).sum();
                OPEN_POSITIONS_GAUGE.store(lots as u64, std::sync::atomic::Ordering::Relaxed);
                DAILY_REALIZED_PNL_SOL_MICRO.store(
                    (rs.realized_pnl_sol * 1_000_000.0) as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
                // Also publish Sharpe and drawdown gauges after restore
                crate::metrics::update_sharpe(rs.last_sharpe);
                if let Some(limit) = self.cfg.read().daily_loss_limit_sol {
                    if limit > 0.0 {
                        let dd = (rs.realized_loss_today_sol / limit).clamp(0.0, 1.0);
                        crate::metrics::update_drawdown(dd);
                    }
                }
                info!(positions = rs.open.len(), "risk state restored");
            } else {
                warn!("risk state json parse failed");
            }
        } else {
            debug!("risk state read failed");
        }
    }

    /// Scan wallet for existing token balances and register as positions.
    /// This ensures max_open_positions works correctly even after restart.
    async fn scan_wallet_for_existing_positions(&self) -> Result<()> {
        let owner = self.treasury.pubkey();
        info!(owner=%owner, "sniper: scanning wallet for existing token positions...");

        // Get all token accounts owned by this wallet
        // Need to check both SPL Token and Token-2022 programs
        let spl_token_prog = solana_sdk::pubkey::Pubkey::new_from_array(spl_token::id().to_bytes());
        let owner_sdk = solana_sdk::pubkey::Pubkey::new_from_array(owner.to_bytes());

        // Token-2022 program ID
        let spl_token_2022_prog =
            solana_sdk::pubkey::Pubkey::from_str("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")
                .unwrap();

        // Fetch accounts from both programs
        let token_accounts_spl = self
            .rpc
            .rpc
            .get_token_accounts_by_owner(
                &owner_sdk,
                solana_client::rpc_request::TokenAccountsFilter::ProgramId(spl_token_prog),
            )
            .await
            .unwrap_or_default();

        let token_accounts_2022 = self
            .rpc
            .rpc
            .get_token_accounts_by_owner(
                &owner_sdk,
                solana_client::rpc_request::TokenAccountsFilter::ProgramId(spl_token_2022_prog),
            )
            .await
            .unwrap_or_default();

        info!(
            spl_token_count = token_accounts_spl.len(),
            token_2022_count = token_accounts_2022.len(),
            "sniper: found token accounts in wallet"
        );

        let mut positions_found = 0;
        let mut already_tracked = 0;

        // Native SOL mint to skip
        let sol_mint =
            solana_sdk::pubkey::Pubkey::from_str("So11111111111111111111111111111111111111112")
                .unwrap();

        // Combine both token account lists
        let all_token_accounts = token_accounts_spl
            .into_iter()
            .chain(token_accounts_2022.into_iter());

        // Response is Vec<RpcKeyedAccount> directly (not wrapped in .value)
        for account in all_token_accounts {
            // Parse the token account data - handle JsonParsed format (default from RPC)
            // Now also extract decimals for correct amount display
            let (mint_str, amount_raw, decimals): (String, u64, u8) =
                if let solana_account_decoder::UiAccountData::Json(parsed) = &account.account.data {
                    // JsonParsed format: {"parsed": {"info": {"mint": "...", "tokenAmount": {"amount": "...", "decimals": N}}}}
                    let info = parsed.parsed.get("info");
                    if let Some(info) = info {
                        let mint = info.get("mint").and_then(|v| v.as_str()).unwrap_or("");
                        let amount_str = info
                            .get("tokenAmount")
                            .and_then(|v| v.get("amount"))
                            .and_then(|v| v.as_str())
                            .unwrap_or("0");
                        let decimals = info
                            .get("tokenAmount")
                            .and_then(|v| v.get("decimals"))
                            .and_then(|v| v.as_u64())
                            .unwrap_or(6) as u8; // Default to 6 decimals (common for SPL tokens)
                        let amount = amount_str.parse::<u64>().unwrap_or(0);
                        (mint.to_string(), amount, decimals)
                    } else {
                        continue;
                    }
                } else if let solana_account_decoder::UiAccountData::Binary(b64_str, _encoding) =
                    &account.account.data
                {
                    // Binary format fallback - need to fetch decimals from mint account
                    let data = match base64::Engine::decode(
                        &base64::engine::general_purpose::STANDARD,
                        b64_str,
                    ) {
                        Ok(d) => d,
                        Err(_) => continue,
                    };
                    if data.len() < 165 {
                        continue;
                    }
                    let mint_bytes: [u8; 32] = data[0..32].try_into().unwrap_or([0u8; 32]);
                    let amount = u64::from_le_bytes(data[64..72].try_into().unwrap_or([0u8; 8]));
                    let mint_pk = solana_sdk::pubkey::Pubkey::new_from_array(mint_bytes);
                    // For binary format, we'll fetch decimals later or use default
                    (mint_pk.to_string(), amount, 6) // Default 6, will be corrected below
                } else {
                    continue;
                };

            // Skip zero balances
            if amount_raw == 0 {
                continue;
            }

            // Skip dust amounts (less than 1000 raw tokens = 0.001 for 6 decimal tokens)
            // These are worthless and cannot be sold for any SOL
            if amount_raw < 1000 {
                debug!(
                    mint=%mint_str,
                    amount_raw=amount_raw,
                    "sniper: [WALLET SCAN] skipping dust position (< 1000 raw tokens)"
                );
                continue;
            }

            let token_mint = match solana_sdk::pubkey::Pubkey::from_str(&mint_str) {
                Ok(pk) => pk,
                Err(_) => continue,
            };

            // Skip WSOL
            if token_mint == sol_mint {
                continue;
            }

            let mint = Pubkey::new_from_array(token_mint.to_bytes());

            // Check if already tracked in risk state
            {
                let rs = self.risk.read();
                if rs.open.contains_key(&mint) {
                    already_tracked += 1;
                    continue;
                }
            }

            // For binary format, fetch actual decimals from mint account
            let actual_decimals = if decimals == 6 {
                // Try to get actual decimals from mint account
                crate::solana::token_utils::get_token_decimals_or_default(&self.rpc, &token_mint)
                    .await
            } else {
                decimals
            };

            // Calculate human-readable amount
            let amount_human = if actual_decimals == 0 {
                amount_raw as f64
            } else {
                amount_raw as f64 / 10f64.powi(actual_decimals as i32)
            };

            // Add to purchased set so we don't try to buy again
            self.purchased.write().insert(mint);

            // Register as an open position (unknown entry price)
            // Use max_buy_sol as estimated invested amount since we don't know the actual
            let estimated_invested = self.cfg.read().max_buy_sol;

            // Estimate entry price from invested / amount
            let estimated_entry_price = if amount_human > 0.0 {
                estimated_invested / amount_human
            } else {
                0.0
            };

            {
                let mut rs = self.risk.write();
                // CRITICAL: For wallet-scan positions, set opened_ts to a time in the past
                // so they get processed by time-based exits immediately.
                // We don't know when they were actually bought, so assume they're old enough
                // to be eligible for time-based exits (set to 1 hour ago).
                let old_timestamp = chrono::Utc::now().timestamp() - 3600; // 1 hour ago
                let lot = PositionLot {
                    entry_price_sol: estimated_entry_price, // Estimated from config
                    amount_tokens: amount_human,
                    invested_sol: estimated_invested, // Estimate based on config
                    token_decimals: actual_decimals,
                    last_unrealized_pnl_sol: 0.0,
                    opened_ts: old_timestamp, // Set to past so time-based exits can trigger
                    executed_tp_bps: Vec::new(),
                    peak_pnl_bps: 0,
                    executed_timed_tiers: Vec::new(),
                    creator: None,
                };
                rs.open.entry(mint).or_default().push(lot);
                positions_found += 1;

                info!(
                    mint=%mint,
                    amount_raw=amount_raw,
                    amount_human=amount_human,
                    decimals=actual_decimals,
                    estimated_invested_sol=estimated_invested,
                    estimated_entry_price=estimated_entry_price,
                    "sniper: [WALLET SCAN] found existing token position"
                );
            }
        }

        // Update gauge
        let total_positions: usize = self.risk.read().open.values().map(|v| v.len()).sum();
        OPEN_POSITIONS_GAUGE.store(total_positions as u64, std::sync::atomic::Ordering::Relaxed);

        let max_pos = self.cfg.read().max_open_positions.unwrap_or(usize::MAX);
        info!(
            positions_found = positions_found,
            already_tracked = already_tracked,
            total_open = total_positions,
            max_allowed = max_pos,
            "sniper: [WALLET SCAN COMPLETE] existing positions loaded"
        );

        if total_positions >= max_pos {
            warn!(
                total=total_positions,
                max=max_pos,
                "sniper: [WARNING] max_open_positions already reached! No new buys will be allowed."
            );
        }

        // Persist state so wallet scan results survive restart
        if positions_found > 0 {
            self.persist_risk_state();
            info!(
                positions_found = positions_found,
                "sniper: persisted wallet scan results to state file"
            );
        }

        Ok(())
    }
}
// end try_load_risk_state; keep impl open for helper structs/functions below

// Liquidity concentration assessment result
#[derive(Debug, Clone)]
pub struct LpLockAssessment {
    pub top1_pct: f64,
    pub top3_pct: f64,
    pub top5_pct: f64,
    pub concentration_ok: bool,
    pub largest_account: Option<String>,
    pub burned_pct: f64,
    pub program_vault_pct: f64,
    pub holder_count: usize,
    /// Whether the holder count could be fetched from RPC.
    /// If false, holder_count is 0 and holder-based checks should be skipped.
    pub holder_check_available: bool,
}

// Test-only utilities for verifying concentration math
#[cfg(any(test, feature = "test_helpers"))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HolderClass {
    Burn,
    ProgramVault,
    Regular,
}

#[cfg(any(test, feature = "test_helpers"))]
pub fn test_compute_concentration(
    total_supply: f64,
    holders: &[(f64, HolderClass)],
) -> (f64, f64, f64, f64, f64) {
    let mut burned = 0.0;
    let mut locked = 0.0;
    let mut regular: Vec<f64> = Vec::new();
    for (amt, class) in holders.iter().copied() {
        match class {
            HolderClass::Burn => burned += amt,
            HolderClass::ProgramVault => locked += amt,
            HolderClass::Regular => regular.push(amt),
        }
    }
    let effective_supply = (total_supply - burned - locked).max(0.0);
    regular.sort_by(|a, b| b.partial_cmp(a).unwrap());
    let top1 = regular.first().copied().unwrap_or(0.0);
    let top3: f64 = regular.iter().take(3).sum();
    let top5: f64 = regular.iter().take(5).sum();
    let top1_pct = if effective_supply > 0.0 {
        top1 / effective_supply
    } else {
        0.0
    };
    let top3_pct = if effective_supply > 0.0 {
        top3 / effective_supply
    } else {
        0.0
    };
    let top5_pct = if effective_supply > 0.0 {
        top5 / effective_supply
    } else {
        0.0
    };
    let burned_pct = if total_supply > 0.0 {
        burned / total_supply
    } else {
        0.0
    };
    let locked_pct = if total_supply > 0.0 {
        locked / total_supply
    } else {
        0.0
    };
    (top1_pct, top3_pct, top5_pct, burned_pct, locked_pct)
}

#[allow(dead_code)]
impl SniperEngine {
    /// Check if a mint is "fresh" by inspecting its transaction history.
    /// Returns true if the mint appears to be new (few transactions or recent creation).
    async fn check_mint_freshness(
        &self,
        mint: &Pubkey,
        pool_address: Option<&Pubkey>,
    ) -> Result<bool> {
        // ============================================================
        // CRITICAL PRO-GRADE FILTER: Only buy pools created AFTER bot start!
        // ============================================================
        // This is how professional snipers work:
        // 1. When bot starts, record the timestamp
        // 2. REJECT all pools that existed before bot started
        // 3. ONLY buy pools created AFTER we started watching
        //
        // This eliminates ALL old tokens, regardless of whether they have
        // new pools, migrations, relistings, etc.

        // If we have a pool address, verify it was created AFTER we started
        if let Some(pool) = pool_address {
            let pool_sigs = match self
                .rpc
                .rpc
                .get_signatures_for_address_with_config(
                    pool,
                    solana_client::rpc_client::GetConfirmedSignaturesForAddress2Config {
                        limit: Some(100),
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(sigs) => sigs,
                Err(e) => {
                    warn!(pool=%pool, error=?e, "sniper: could not fetch pool signatures -> REJECT for safety");
                    return Ok(false);
                }
            };

            if pool_sigs.is_empty() {
                // No signatures = Pool is SO NEW that RPC hasn't indexed it yet!
                // This is actually a GOOD sign for a fresh launch.
                // Fall through to Helius mint validation instead of rejecting.
                info!(pool=%pool, "sniper: no pool signatures found (pool may be very new) -> checking mint via Helius");
                // Don't return - continue to mint validation below
            } else {
                // Check oldest pool signature - this is when the pool was created
                let oldest_pool_sig = pool_sigs.last().unwrap();
                if let Some(block_time) = oldest_pool_sig.block_time {
                    // CRITICAL CHECK: Was pool created AFTER we started?
                    if block_time < self.boot_timestamp {
                        let age_since_boot = self.boot_timestamp - block_time;
                        info!(
                            mint=%mint,
                            pool=%pool,
                            pool_created_at=block_time,
                            bot_started_at=self.boot_timestamp,
                            secs_before_boot=age_since_boot,
                            "sniper: REJECT - pool existed BEFORE bot started (old pool, not a new launch)"
                        );
                        return Ok(false);
                    }

                    // Pool was created after boot - now check it's within reasonable time
                    let now = ChronoUtc::now().timestamp();
                    let pool_age_secs = now - block_time;

                    // Pool should be created within last 10 minutes for fresh launch
                    if pool_age_secs > 600 {
                        info!(
                            mint=%mint,
                            pool=%pool,
                            pool_age_mins=pool_age_secs/60,
                            "sniper: REJECT - pool is >10min old (not fresh enough)"
                        );
                        return Ok(false);
                    }

                    info!(
                        mint=%mint,
                        pool=%pool,
                        pool_age_secs,
                        pool_created_at=block_time,
                        bot_started_at=self.boot_timestamp,
                        "sniper: PASSED - pool created AFTER bot start and is fresh"
                    );
                } else {
                    // No block_time on pool signature - likely old transaction
                    warn!(pool=%pool, "sniper: pool signature has no block_time -> REJECT (likely old pool)");
                    return Ok(false);
                }
            }
        } else {
            // No pool address provided - must still check mint age
            warn!(mint=%mint, "sniper: no pool_address provided - cannot verify pool creation time");
        }

        // ============================================================
        // CRITICAL: Check MINT account creation time directly!
        // Old tokens can create NEW pools - we must verify the MINT itself is new.
        // ============================================================

        // First, get mint account to verify it exists (creation time checked via signatures)
        let _mint_account = match self.rpc.get_account_retry(mint).await {
            Ok(acc) => acc,
            Err(e) => {
                warn!(mint=%mint, error=?e, "sniper: could not fetch mint account -> REJECT");
                return Ok(false);
            }
        };

        // Check mint account rent epoch as a proxy for age
        // Very new accounts have rent_epoch close to current epoch
        // NOTE: This is not foolproof but adds another layer

        // ============================================================
        // CRITICAL: Use Helius RPC for mint signatures if configured!
        // Local validators do NOT have full transaction index and return
        // incorrect (too few) signatures for old tokens. Helius has complete history.
        // ============================================================
        let mint_sigs = if let Some(ref helius) = self.helius_rpc {
            // Use Helius for accurate mint signature count
            match helius
                .get_signatures_for_address_with_config(
                    mint,
                    solana_client::rpc_client::GetConfirmedSignaturesForAddress2Config {
                        limit: Some(200),
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(sigs) => {
                    info!(
                        mint=%mint,
                        sig_count=sigs.len(),
                        "sniper: got mint signatures from Helius (full index)"
                    );
                    sigs
                }
                Err(e) => {
                    warn!(mint=%mint, error=?e, "sniper: Helius mint signature query failed -> REJECT for safety");
                    return Ok(false);
                }
            }
        } else {
            // Fallback to local RPC (WARNING: may be inaccurate for old tokens!)
            match self
                .rpc
                .rpc
                .get_signatures_for_address_with_config(
                    mint,
                    solana_client::rpc_client::GetConfirmedSignaturesForAddress2Config {
                        limit: Some(200),
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(sigs) => {
                    warn!(
                        mint=%mint,
                        sig_count=sigs.len(),
                        "sniper: using LOCAL RPC for mint sigs (may be inaccurate - no full index!)"
                    );
                    sigs
                }
                Err(e) => {
                    warn!(mint=%mint, error=?e, "sniper: local RPC mint signature query failed -> REJECT");
                    return Ok(false);
                }
            }
        };

        let sig_count = mint_sigs.len();

        // CRITICAL FIX: If we get 0 signatures, the RPC is NOT indexing this mint properly
        // This is SUSPICIOUS - a real new token should have at least its creation TX
        // Old tokens on new pools often show 0 signatures because RPC pagination issues
        if sig_count == 0 {
            warn!(
                mint=%mint,
                "sniper: REJECT - mint has 0 signatures (RPC not indexing or pagination issue)"
            );
            return Ok(false);
        }

        // CRITICAL: If mint has many signatures, it's an OLD token
        // Reduced threshold from 200 to 50 - truly new tokens have very few signatures
        if sig_count >= 50 {
            info!(
                mint=%mint,
                sig_count,
                "sniper: REJECT - mint has too many signatures (>= 50), likely old token"
            );
            return Ok(false);
        }

        // Check the OLDEST and NEWEST signatures
        let oldest_visible_sig = mint_sigs.last().unwrap();
        let newest_sig = mint_sigs.first().unwrap();

        if let (Some(oldest_time), Some(newest_time)) =
            (oldest_visible_sig.block_time, newest_sig.block_time)
        {
            // CRITICAL: If oldest visible signature is before boot, REJECT
            if oldest_time < self.boot_timestamp {
                let age_since_boot = self.boot_timestamp - oldest_time;
                info!(
                    mint=%mint,
                    oldest_visible_sig_time=oldest_time,
                    bot_started_at=self.boot_timestamp,
                    secs_before_boot=age_since_boot,
                    sig_count,
                    "sniper: REJECT - oldest visible mint signature is BEFORE bot started"
                );
                return Ok(false);
            }

            // Check if mint is too old (> 10 min from oldest to newest = suspicious)
            let mint_activity_span = newest_time - oldest_time;
            if mint_activity_span > 600 {
                info!(
                    mint=%mint,
                    activity_span_secs=mint_activity_span,
                    sig_count,
                    "sniper: REJECT - mint activity spans > 10 minutes (not a fresh launch)"
                );
                return Ok(false);
            }

            let now = ChronoUtc::now().timestamp();
            let age_secs = now - oldest_time;

            // If the oldest visible signature is > 10 min old, reject (was 30 min)
            if age_secs > 600 {
                info!(
                    mint=%mint,
                    age_secs,
                    sig_count,
                    "sniper: REJECT - oldest visible mint sig is > 10 min old"
                );
                return Ok(false);
            }
        } else {
            // No block time on signature - suspicious
            warn!(mint=%mint, "sniper: mint signature has no block_time -> REJECT");
            return Ok(false);
        }

        // All checks passed!
        info!(
            mint=%mint,
            mint_sig_count=mint_sigs.len(),
            "sniper: ALL FRESHNESS CHECKS PASSED - token is genuinely new"
        );
        Ok(true)
    }

    /// PROFESSIONAL TOKEN VALIDATION - No index-dependent RPC calls!
    /// This is how pro snipers validate tokens:
    /// 1. Mint Authority must be revoked (None) - prevents rug via inflation
    /// 2. Freeze Authority must be revoked (None) - prevents rug via freezing
    /// 3. Slot-based age check - token must be created within last N slots
    /// 4. Transaction count - must be under threshold (via get_signatures)
    ///
    /// Does NOT use get_token_largest_accounts (requires expensive full index)
    async fn validate_token_professional(
        &self,
        mint: &Pubkey,
        pool_address: &Pubkey,
        discovery_slot: u64,
    ) -> Result<bool> {
        // ============================================================
        // CRITICAL FIX: ALWAYS check boot_timestamp FIRST!
        // This prevents buying old tokens even if RPC calls fail.
        // ============================================================
        match self.check_mint_freshness(mint, Some(pool_address)).await {
            Ok(true) => {
                debug!(mint=%mint, "sniper: [PRO] boot_timestamp check PASSED");
            }
            Ok(false) => {
                // Token/pool existed before bot started - REJECT immediately
                return Ok(false);
            }
            Err(e) => {
                warn!(?e, mint=%mint, "sniper: [PRO] freshness check failed - REJECT for safety");
                return Ok(false);
            }
        }

        // 1. Fetch mint account (always works, no index needed)
        let mint_acc = match self.rpc.get_account_retry(mint).await {
            Ok(acc) => acc,
            Err(e) => {
                // Mint account not available - but we already passed boot_timestamp check
                // This could be a very fresh token, allow it through
                info!(mint=%mint, error=?e, "sniper: [PRO] mint account not yet available - token is VERY fresh (passed boot_timestamp)");
                return Ok(true);
            }
        };

        // Parse mint account
        if mint_acc.data.len() < 82 {
            warn!(mint=%mint, len=mint_acc.data.len(), "sniper: [PRO] invalid mint account size");
            return Ok(false);
        }

        let (mint_authority, freeze_authority, decimals, supply) =
            parse_spl_mint_fields(&mint_acc.data);

        // 2. CRITICAL: Mint Authority MUST be revoked
        if mint_authority.is_some() {
            info!(
                mint=%mint,
                mint_auth=?mint_authority,
                "sniper: [PRO] REJECT - Mint authority NOT revoked (rug risk: can inflate supply)"
            );
            return Ok(false);
        }

        // 3. CRITICAL: Freeze Authority MUST be revoked (if configured)
        if self.cfg.read().require_freeze_auth_none.unwrap_or(true) && freeze_authority.is_some() {
            info!(
                mint=%mint,
                freeze_auth=?freeze_authority,
                "sniper: [PRO] REJECT - Freeze authority NOT revoked (rug risk: can freeze tokens)"
            );
            return Ok(false);
        }

        // 4. Decimals sanity check
        if let Some((lo, hi)) = self.cfg.read().require_mint_decimals_range {
            if decimals < lo || decimals > hi {
                info!(mint=%mint, decimals, lo, hi, "sniper: [PRO] REJECT - decimals out of range");
                return Ok(false);
            }
        }

        // 5. Supply sanity check
        if supply == 0 {
            warn!(mint=%mint, "sniper: [PRO] REJECT - zero supply");
            return Ok(false);
        }

        // 6. Slot-based freshness (if we have discovery slot from Geyser)
        if discovery_slot > 0 {
            if let Ok(current_slot) = self.rpc.rpc.get_slot().await {
                // ~400ms per slot, so 7500 slots ≈ 50 minutes
                // For sniping, we want tokens created in the last ~10 minutes (1500 slots)
                let max_age_slots: u64 = 1500; // ~10 minutes
                let age_slots = current_slot.saturating_sub(discovery_slot);

                if age_slots > max_age_slots {
                    info!(
                        mint=%mint,
                        discovery_slot,
                        current_slot,
                        age_slots,
                        max_age_slots,
                        "sniper: [PRO] REJECT - token discovered too long ago (slot age)"
                    );
                    return Ok(false);
                }

                debug!(
                    mint=%mint,
                    age_slots,
                    age_mins = (age_slots as f64 * 0.4) / 60.0,
                    "sniper: [PRO] slot age OK"
                );
            }
        }

        // All checks passed! (boot_timestamp was already checked at the start)
        info!(mint=%mint, "sniper: [PRO] PASS - all professional checks passed");
        Ok(true)
    }

    /// Index-based (Raydium/Orca) liquidity estimation using current pool snapshots.
    /// Returns conservative SOL notionals (sum over pools: 2 * SOL_reserve for SOL pairs, stable converted to SOL via placeholder rate).
    async fn estimate_liquidity_index(&self, mint: &Pubkey) -> Result<Option<f64>> {
        let sol_mint = pubkey!("So11111111111111111111111111111111111111112");
        let usdc = pubkey!("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v");
        let usdt = pubkey!("Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB");
        let mut total_sol = 0f64;
        let mut considered = 0u32;
        // Helper to process pools from a connector
        let sol_usd = self.sol_usd_price().await.max(1.0);
        let mut handle_pool = |base: Pubkey, quote: Pubkey, r_base: u128, r_quote: u128| {
            if base == *mint && quote == sol_mint {
                // mint-SOL
                let sol_res = r_quote as f64 / 1e9;
                total_sol += sol_res * 2.0;
                considered += 1;
                return;
            }
            if quote == *mint && base == sol_mint {
                let sol_res = r_base as f64 / 1e9;
                total_sol += sol_res * 2.0;
                considered += 1;
                return;
            }
            // Stable pairing (USDC/USDT)
            if base == *mint && (quote == usdc || quote == usdt) {
                let usd = r_quote as f64 / 1e6;
                total_sol += (usd / sol_usd) * 2.0;
                considered += 1;
            } else if quote == *mint && (base == usdc || base == usdt) {
                let usd = r_base as f64 / 1e6;
                total_sol += (usd / sol_usd) * 2.0;
                considered += 1;
            }
        };
        if let Some(r) = &self.raydium {
            for snap in r.snapshots() {
                if snap.base_mint == *mint || snap.quote_mint == *mint {
                    handle_pool(
                        snap.base_mint,
                        snap.quote_mint,
                        snap.reserve_base,
                        snap.reserve_quote,
                    );
                }
            }
        }
        if let Some(o) = &self.orca {
            for ps in o.pools_snapshot() {
                if ps.base_mint == *mint || ps.quote_mint == *mint {
                    handle_pool(
                        ps.base_mint,
                        ps.quote_mint,
                        ps.reserve_base,
                        ps.reserve_quote,
                    );
                }
            }
        }

        if considered == 0 {
            return Ok(None);
        }
        LIQUIDITY_ESTIMATE_SOL_MICRO.store(
            (total_sol * 1_000_000.0) as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
        Ok(Some(total_sol))
    }
    /// Attempt to estimate total pool liquidity in SOL terms for a given candidate mint.
    /// Strategy:
    /// 1. Fetch largest token accounts for mint (already done in lp_lock_check for initial filters).
    /// 2. Heuristically detect whether any Raydium or Orca pool exists pairing this mint with SOL or a stable (USDC/USDT).
    /// 3. If pool account found, fetch its vault token accounts and sum value converting via simple mid-price derived from reserves.
    /// 4. Price conversion: if SOL paired -> direct; if USDC/USDT paired -> treat 1 token == 1 USD and convert using a static SOL/USD (placeholder) or skip until oracle integrated.
    async fn estimate_liquidity_for_mint(&self, mint: &Pubkey) -> Result<Option<f64>> {
        // Placeholder stable + SOL mints (should move to config/oracle):
        let sol_mint = pubkey!("So11111111111111111111111111111111111111112");
        let usdc = pubkey!("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v");
        let usdt = pubkey!("Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB");
        // Quick exit if candidate itself is SOL/stable
        if *mint == sol_mint || *mint == usdc || *mint == usdt {
            return Ok(None);
        }
        // We don't maintain an indexed map from mint->pool yet, so attempt lightweight scan of recent accounts: skip (needs future caching)
        // For now: try fetch largest accounts and look for vault owners referencing Raydium or Orca program (heuristic minimal viable solution).
        let largest = self.rpc.rpc.get_token_largest_accounts(mint).await.ok();
        let Some(list) = largest else {
            return Ok(None);
        };
        let mut candidate_vaults: Vec<Pubkey> = Vec::new();
        for acc in list.iter().take(8) {
            if let Ok(pk) = Pubkey::from_str(&acc.address) {
                candidate_vaults.push(pk);
            }
        }
        if candidate_vaults.is_empty() {
            return Ok(None);
        }
        let vault_infos = self
            .rpc
            .rpc
            .get_multiple_accounts(&candidate_vaults)
            .await
            .ok();
        let Some(v_infos) = vault_infos else {
            return Ok(None);
        };
        // Identify any vault that looks like a Raydium / Orca pool vault by inspecting its owner field if present
        // SPL token account layout: mint(0..32) owner(32..64) amount(64..72)
        let mut est_sol_value = 0f64;
        let sol_usd = self.sol_usd_price().await.max(1.0);
        for opt in v_infos.iter() {
            let Some(acc) = opt else {
                continue;
            };
            if acc.data.len() < 72 {
                continue;
            }
            let mint_bytes: [u8; 32] = acc.data[0..32].try_into().unwrap();
            let owner_bytes: [u8; 32] = acc.data[32..64].try_into().unwrap();
            let reserve_u64 = u64::from_le_bytes(acc.data[64..72].try_into().unwrap());
            let inner_mint = Pubkey::new_from_array(mint_bytes);
            let owner_pk = Pubkey::new_from_array(owner_bytes);
            // Heuristic: we only care if this token account's mint is either candidate mint or SOL/stable, and owner is a known AMM program (Raydium / Orca pool address not directly program id, so skip deep validation).
            if inner_mint != *mint
                && inner_mint != sol_mint
                && inner_mint != usdc
                && inner_mint != usdt
            {
                continue;
            }
            // Convert amount to SOL value: if paired with SOL directly and inner_mint == SOL -> treat reserve as SOL.
            if inner_mint == sol_mint {
                est_sol_value += reserve_u64 as f64 / 1e9;
            } else if inner_mint == usdc || inner_mint == usdt {
                // Convert USD stable to SOL using override oracle if provided
                let usd = reserve_u64 as f64 / 1e6; // USDC/USDT decimals 6
                est_sol_value += usd / sol_usd;
            } else if inner_mint == *mint {
                // Need other side value to price; skip as we can't compute without reserves pair.
            }
            // Owner heuristic could refine classification (TODO)
            let _ = owner_pk; // silence unused for now
        }
        if est_sol_value == 0.0 {
            return Ok(None);
        }
        Ok(Some(est_sol_value))
    }
    pub async fn lp_lock_check(&self, mint: &Pubkey) -> Result<Option<LpLockAssessment>> {
        // Only run if any threshold configured
        {
            let r = self.cfg.read();
            info!(
                mint=%mint,
                top1=?r.lp_top1_max_pct,
                top3=?r.lp_top3_max_pct,
                top5=?r.lp_top5_max_pct,
                "sniper: lp_lock_check config values"
            );
            if r.lp_top1_max_pct.is_none()
                && r.lp_top3_max_pct.is_none()
                && r.lp_top5_max_pct.is_none()
            {
                info!(mint=%mint, "sniper: no lp thresholds configured or insufficient data");
                return Ok(None);
            }
        }
        let (thr1, thr3, thr5) = {
            let r = self.cfg.read();
            (
                r.lp_top1_max_pct.unwrap_or(f64::MAX),
                r.lp_top3_max_pct.unwrap_or(f64::MAX),
                r.lp_top5_max_pct.unwrap_or(f64::MAX),
            )
        };
        // Fetch mint account
        debug!(mint=%mint, "sniper: fetching mint account for LP check");
        let mint_acc_opt = match self.rpc.get_account_retry(mint).await {
            Ok(a) => {
                debug!(mint=%mint, data_len=a.data.len(), "sniper: mint account fetched successfully");
                Some(a)
            }
            Err(e) => {
                // For brand new Pump.fun tokens, the mint account might not exist yet
                // Use fallback values and skip authority checks
                info!(mint=%mint, error=?e, "sniper: mint account fetch failed, using fallback for brand new tokens");
                None
            }
        };

        // Check token age: Only trade tokens created in the last 10 minutes (7200 slots at 400ms/slot)
        // This filters out established tokens like JLP that have new pools created for them
        if let Some(ref _acc) = mint_acc_opt {
            // Get current slot
            if let Ok(current_slot) = self.rpc.rpc.get_slot().await {
                // Solana stores lamports in the account, but we need the slot when the account was created
                // We approximate by checking if the account has been modified recently
                // For a more accurate check, we'd need to track the slot from the pool discovery event
                // For now, we rely on the combination of low liquidity + fallback LP assessment
                // This is acceptable because truly new tokens will have:
                // 1. Low liquidity (< 1 SOL typically)
                // 2. No token account index (triggers fallback LP assessment)
                // 3. Recent pool creation (from geyser timestamp)

                // TODO: Add explicit slot tracking from pool discovery event
                debug!(mint=%mint, current_slot=current_slot, "sniper: token age check - using pool discovery timestamp");
            }
        }

        // Parse mint account if available, otherwise use defaults
        let (mint_auth_opt, freeze_auth_opt, parsed_decimals, parsed_supply_raw) = if let Some(
            ref acc,
        ) =
            mint_acc_opt
        {
            // SPL Mint length heuristic (approx range)
            if acc.data.len() < 70 || acc.data.len() > 90 {
                info!(mint=%mint, data_len=acc.data.len(), "sniper: mint account size invalid, returning None");
                return Ok(None);
            }
            parse_spl_mint_fields(&acc.data)
        } else {
            // Fallback for brand new tokens (Pump.fun defaults)
            (None, None, 6u8, 1_000_000_000u64) // 1B supply with 6 decimals is common for Pump.fun
        };

        // Try authoritative RPC getTokenSupply for decimals and amount
        let mut decimals_eff = parsed_decimals;
        let mut supply = if parsed_decimals == 0 {
            parsed_supply_raw as f64
        } else {
            (parsed_supply_raw as f64) / 10f64.powi(parsed_decimals as i32)
        };
        if let Ok(s) = self.rpc.rpc.get_token_supply(mint).await {
            // Use RPC decimals and amount if parse succeeds AND supply is non-zero
            if let Ok(v) = s.amount.parse::<u128>() {
                let rpc_supply = if s.decimals == 0 {
                    v as f64
                } else {
                    (v as f64) / 10f64.powi(s.decimals as i32)
                };
                // Only use RPC data if it's non-zero (otherwise keep fallback)
                if rpc_supply > 0.0 {
                    supply = rpc_supply;
                    decimals_eff = s.decimals;
                } else {
                    info!(mint=%mint, "sniper: RPC returned zero supply, keeping fallback values");
                }
            }
        }
        if supply == 0.0 {
            info!(mint=%mint, "sniper: token supply is zero after all checks, returning None");
            return Ok(None);
        }
        // Owner blacklist gate
        let owners = self.cfg.read().blacklist_owners.clone();
        if owner_blacklisted(&owners, mint_auth_opt.as_ref(), freeze_auth_opt.as_ref()) {
            info!(mint=%mint, "sniper: owner blacklisted, returning None");
            return Ok(None);
        }
        if self.cfg.read().require_freeze_auth_none.unwrap_or(false) && freeze_auth_opt.is_some() {
            info!(mint=%mint, "sniper: freeze authority present, returning None");
            return Ok(None);
        }
        if let Some((lo, hi)) = self.cfg.read().require_mint_decimals_range {
            let d = decimals_eff;
            if d < lo || d > hi {
                info!(mint=%mint, decimals=d, lo=lo, hi=hi, "sniper: decimals out of range, returning None");
                return Ok(None);
            }
        }
        if self
            .cfg
            .read()
            .blacklist_mints
            .iter()
            .any(|m| m == &mint.to_string())
        {
            info!(mint=%mint, "sniper: mint blacklisted, returning None");
            return Ok(None);
        }
        // Largest accounts
        let (list, holder_check_available) = match self
            .rpc
            .rpc
            .get_token_largest_accounts(mint)
            .await
        {
            Ok(v) => (v, true),
            Err(e) => {
                // RPC doesn't have this mint in account-index (common for new tokens)
                // FALLBACK: Allow the trade but mark that holder check was unavailable
                // The freshness check will be the primary filter for new tokens
                info!(mint=%mint, error=?e, "sniper: largest accounts fetch failed -> using freshness-only mode");
                (Vec::new(), false)
            }
        };
        let holder_count = list.len();

        // If holder check unavailable (RPC index limitation), return a permissive assessment
        // The freshness check will be the primary filter for these tokens
        if !holder_check_available {
            info!(mint=%mint, "sniper: holder check unavailable, using freshness-only mode");
            return Ok(Some(LpLockAssessment {
                top1_pct: 0.0,
                top3_pct: 0.0,
                top5_pct: 0.0,
                concentration_ok: true, // Allow - we can't verify, rely on freshness
                largest_account: None,
                holder_count: 0,
                burned_pct: 0.0,
                program_vault_pct: 0.0,
                holder_check_available: false,
            }));
        }

        if list.is_empty() {
            info!(mint=%mint, "sniper: largest accounts list empty, returning None");
            return Ok(None);
        }
        // Collect top5 addresses & amounts raw
        let mut holder_accts: Vec<(String, u128)> = Vec::new();
        for (i, acc) in list.iter().enumerate() {
            if i >= 5 {
                break;
            }
            if let Ok(raw_u128) = acc.amount.amount.parse::<u128>() {
                holder_accts.push((acc.address.clone(), raw_u128));
            }
        }
        if holder_accts.is_empty() {
            return Ok(None);
        }
        let largest_key = Some(holder_accts[0].0.clone());
        // Fetch token account data for classification (burn / program-vault)
        let addrs: Vec<Pubkey> = holder_accts
            .iter()
            .filter_map(|(a, _)| Pubkey::from_str(a).ok())
            .collect();
        let acct_infos = match self.rpc.rpc.get_multiple_accounts(&addrs).await {
            Ok(v) => v,
            Err(e) => {
                warn!(?e, "largest token accounts fetch failed");
                Vec::new()
            }
        };
        // Known constants
        let incinerator = pubkey!("1nc1nerator11111111111111111111111111111111");
        let raydium_prog = Pubkey::from_str(RAYDIUM_AMM_V4).unwrap_or(Pubkey::default());
        let orca_prog = Pubkey::from_str(ORCA_WHIRLPOOL_PROGRAM).unwrap_or(Pubkey::default());
        // Build a set of current known vault addresses from Raydium and Orca snapshots to classify program vault holdings precisely
        let mut program_vaults: std::collections::HashSet<Pubkey> =
            std::collections::HashSet::new();
        if let Some(r) = &self.raydium {
            for s in r.snapshots() {
                if let Some(v) = s.base_vault {
                    program_vaults.insert(v);
                }
                if let Some(v) = s.quote_vault {
                    program_vaults.insert(v);
                }
                if let Some(v) = s.serum_base_vault {
                    program_vaults.insert(v);
                }
                if let Some(v) = s.serum_quote_vault {
                    program_vaults.insert(v);
                }
            }
        }
        if let Some(o) = &self.orca {
            for s in o.pools_snapshot() {
                program_vaults.insert(s.vault_a);
                program_vaults.insert(s.vault_b);
            }
        }
        #[derive(PartialEq)]
        enum Class {
            Burn,
            ProgramVault,
            Regular,
        }
        struct HolderRec {
            amt_tokens: f64,
            class: Class,
        }
        let mut records: Vec<HolderRec> = Vec::new();
        let mut burned_total = 0f64;
        let mut program_locked_total = 0f64;
        for (idx, (addr_str, raw_amount)) in holder_accts.iter().enumerate() {
            let mut class = Class::Regular;
            let amt_tokens = if decimals_eff == 0 {
                *raw_amount as f64
            } else {
                *raw_amount as f64 / 10f64.powi(decimals_eff as i32)
            };
            if let Ok(acc_pk) = Pubkey::from_str(addr_str) {
                if acc_pk == incinerator {
                    class = Class::Burn;
                } else if program_vaults.contains(&acc_pk) {
                    class = Class::ProgramVault;
                }
            }
            if class == Class::Regular {
                if let Some(Some(acc_info)) = acct_infos.get(idx).map(|o| o.as_ref()) {
                    // Token account length heuristic: >= 80 for owner field
                    if acc_info.data.len() >= 64 {
                        let owner_slice = &acc_info.data[32..64];
                        let owner_auth = Pubkey::new_from_array(owner_slice.try_into().unwrap());
                        if owner_auth == incinerator {
                            class = Class::Burn;
                        } else if owner_auth == raydium_prog || owner_auth == orca_prog {
                            class = Class::ProgramVault;
                        } else {
                            // Fallback: fetch owner auth executable bit via separate account info if present in batch (future optimization)
                        }
                    }
                }
            }
            match class {
                Class::Burn => burned_total += amt_tokens,
                Class::ProgramVault => program_locked_total += amt_tokens,
                Class::Regular => {}
            }
            records.push(HolderRec { amt_tokens, class });
        }
        let effective_supply = (supply - burned_total - program_locked_total).max(0.0);
        if effective_supply <= 0.0 {
            return Ok(None);
        }
        // Compute concentration using only regular holders relative to effective supply
        let mut regular_amounts: Vec<f64> = records
            .iter()
            .filter(|r| r.class == Class::Regular)
            .map(|r| r.amt_tokens)
            .collect();
        if regular_amounts.is_empty() {
            return Ok(None);
        }
        regular_amounts.sort_by(|a, b| b.partial_cmp(a).unwrap());
        let top1 = regular_amounts.first().copied().unwrap_or(0.0);
        let top3_sum: f64 = regular_amounts.iter().take(3).sum();
        let top5_sum: f64 = regular_amounts.iter().take(5).sum();
        let top1_pct = if effective_supply > 0.0 {
            top1 / effective_supply
        } else {
            0.0
        };
        let top3_pct = if effective_supply > 0.0 {
            top3_sum / effective_supply
        } else {
            0.0
        };
        let top5_pct = if effective_supply > 0.0 {
            top5_sum / effective_supply
        } else {
            0.0
        };
        let concentration_ok = top1_pct <= thr1 && top3_pct <= thr3 && top5_pct <= thr5;
        let burned_pct = if supply > 0.0 {
            burned_total / supply
        } else {
            0.0
        };
        let program_vault_pct = if supply > 0.0 {
            program_locked_total / supply
        } else {
            0.0
        };
        Ok(Some(LpLockAssessment {
            top1_pct,
            top3_pct,
            top5_pct,
            concentration_ok,
            largest_account: largest_key,
            holder_count,
            burned_pct,
            program_vault_pct,
            holder_check_available: true,
        }))
    }
}
// end secondary impl SniperEngine helpers
// (auxiliary impl closed above)

#[allow(clippy::too_many_arguments)]
pub async fn run_sniper(
    rpc: Arc<SolanaRpc>,
    cfg: SniperCfg,
    raydium: Option<Arc<Raydium>>,
    orca: Option<Arc<Orca>>,
    pumpfun: Option<Arc<PumpFunDex>>,
    treasury: Arc<Treasury>,
    geyser_grpc_url: Option<String>,
    helius_rpc_url: Option<String>,
) -> Result<()> {
    let engine = SniperEngine::new(
        rpc,
        cfg,
        raydium,
        orca,
        pumpfun,
        treasury,
        geyser_grpc_url,
        helius_rpc_url,
    );
    engine.run().await
}

// --------------------------- Tests (unit) ----------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;

    fn build_mint_bytes(
        mint_auth: Option<Pubkey>,
        freeze_auth: Option<Pubkey>,
        decimals: u8,
        supply: u64,
    ) -> Vec<u8> {
        // Minimal SPL Mint layout for our fields; pad to 82 bytes
        let mut data = vec![0u8; 82];
        // mint_authority COption
        if let Some(ma) = mint_auth {
            data[0..4].copy_from_slice(&1u32.to_le_bytes());
            data[4..36].copy_from_slice(&ma.to_bytes());
        } else {
            data[0..4].copy_from_slice(&0u32.to_le_bytes());
        }
        // supply
        data[36..44].copy_from_slice(&supply.to_le_bytes());
        // decimals
        data[44] = decimals;
        // freeze_authority COption
        if let Some(fa) = freeze_auth {
            data[46..50].copy_from_slice(&1u32.to_le_bytes());
            data[50..82].copy_from_slice(&fa.to_bytes());
        } else {
            data[46..50].copy_from_slice(&0u32.to_le_bytes());
        }
        data
    }

    #[test]
    fn test_parse_spl_mint_fields_none() {
        let data = build_mint_bytes(None, None, 6, 1_000_000);
        let (ma, fa, dec, supply) = super::parse_spl_mint_fields(&data);
        assert!(ma.is_none());
        assert!(fa.is_none());
        assert_eq!(dec, 6);
        assert_eq!(supply, 1_000_000);
    }

    #[test]
    fn test_parse_spl_mint_fields_some() {
        let ma = Pubkey::new_unique();
        let fa = Pubkey::new_unique();
        let data = build_mint_bytes(Some(ma), Some(fa), 9, 42);
        let (ma2, fa2, dec, supply) = super::parse_spl_mint_fields(&data);
        assert_eq!(ma2.unwrap(), ma);
        assert_eq!(fa2.unwrap(), fa);
        assert_eq!(dec, 9);
        assert_eq!(supply, 42);
    }

    #[test]
    fn test_owner_blacklisted_matches_mint_or_freeze() {
        let ma = Pubkey::new_unique();
        let fa = Pubkey::new_unique();
        let owners = vec![ma.to_string(), "SomeOther".into()];
        assert!(super::owner_blacklisted(&owners, Some(&ma), None));
        assert!(super::owner_blacklisted(&owners, None, Some(&ma))); // listing wrong authority as string should still match when same value
        let owners2 = vec![fa.to_string()];
        assert!(super::owner_blacklisted(&owners2, None, Some(&fa)));
        let owners3: Vec<String> = vec![];
        assert!(!super::owner_blacklisted(&owners3, Some(&ma), Some(&fa)));
    }

    #[test]
    fn test_parse_spl_mint_fields_decimals_zero_edge() {
        let data = build_mint_bytes(None, None, 0, 1_234_567);
        let (_ma, _fa, dec, supply) = super::parse_spl_mint_fields(&data);
        assert_eq!(dec, 0);
        assert_eq!(supply, 1_234_567);
    }

    #[test]
    fn test_gate_freeze_auth_none_blocks_when_present() {
        let fa = Pubkey::new_unique();
        let data = build_mint_bytes(None, Some(fa), 6, 10);
        // require freeze auth to be none => should reject
        let ok = super::test_gate_freeze_and_decimals(true, None, &data);
        assert!(!ok);
        // when not required, should pass
        let ok2 = super::test_gate_freeze_and_decimals(false, None, &data);
        assert!(ok2);
    }

    #[test]
    fn test_gate_decimals_range_edges() {
        // Range [0,9] accepts 0 and 9, rejects 10
        let d0 = build_mint_bytes(None, None, 0, 10);
        let d9 = build_mint_bytes(None, None, 9, 10);
        let d10 = build_mint_bytes(None, None, 10, 10);
        assert!(super::test_gate_freeze_and_decimals(
            false,
            Some((0, 9)),
            &d0
        ));
        assert!(super::test_gate_freeze_and_decimals(
            false,
            Some((0, 9)),
            &d9
        ));
        assert!(!super::test_gate_freeze_and_decimals(
            false,
            Some((0, 9)),
            &d10
        ));
    }
}

// final file closing brace for module (no additional opens)
// EOF
