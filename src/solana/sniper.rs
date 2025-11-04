//! Meme Coin Sniper Skeleton – subscribes to pool creation logs and applies heuristics.
// Memecoin‑Sniper Skeleton: beobachtet neue Pools/LP‑Creations, filtert Risiken,
// setzt kleine Erstkäufe mit harten Limits (Slippage/Blacklist/Owner/Freeze Auth usw.)
#[allow(unused_imports)]
use crate::config_reload::{diff_sniper_cfg, validate_sniper_cfg};
use crate::solana::dex::{orca::Orca, raydium::Raydium, Dex};
use crate::solana::rpc::SolanaRpc;
use crate::wallet::Treasury;
use anyhow::Result;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::{hash::Hash, transaction::Transaction};
use std::str::FromStr;
use std::{collections::HashSet, sync::Arc};
use tracing::{debug, info, warn};
// (log subscription stub – real PubSub integration to be reintroduced with correct crate paths)
use once_cell::sync::Lazy;
use regex::Regex;

use crate::solana::dex::orca::ORCA_WHIRLPOOL_PROGRAM;
use crate::solana::dex::raydium::RAYDIUM_AMM_V4;
use chrono::Utc as ChronoUtc;
use futures::{SinkExt, StreamExt};
use std::time::{Duration, Instant};
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
// removed unused imports (OpenOptions, Write, OnceLazy alias, Mutex)
use crate::metrics; // for qualified partial exit metrics usage
use crate::metrics::{
    record_fee_pct, record_network_fee, record_realized_gross_net, record_realized_pnl_sol,
    record_shortfall, record_shortfall_pct, record_swap_latency, record_trade_return,
    DAILY_REALIZED_PNL_SOL_MICRO, LIQUIDITY_ESTIMATE_SOL_MICRO, OPEN_POSITIONS_GAUGE,
    PENDING_FAILED_TOTAL, PENDING_RECONCILIATIONS_TOTAL, PROTOCOL_FEE_SOL_MICRO_TOTAL,
    PROTOCOL_FEE_TOKENS_TOTAL, RPC_ERRORS_TOTAL, RPC_RETRY_ATTEMPTS_TOTAL, TRADES_EXECUTED_TOTAL,
    TRADES_FAILED_TOTAL, WS_ACTIVE_CONNECTIONS, WS_HEARTBEAT_MISSES_TOTAL, WS_MESSAGES_TOTAL,
    WS_RECONNECTS_TOTAL,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

// Simple global blacklist (extendable via config later)
#[allow(dead_code)]
static MINT_BLACKLIST: Lazy<HashSet<String>> = Lazy::new(HashSet::new);

#[derive(Clone)]
pub struct SniperCfg {
    pub max_buy_sol: f64,
    pub max_slippage_bps: u32,
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
}

impl Default for SniperCfg {
    fn default() -> Self {
        Self {
            max_buy_sol: 1.0,
            max_slippage_bps: 100,
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
        }
    }
}

impl From<&crate::config::SniperSettings> for SniperCfg {
    fn from(c: &crate::config::SniperSettings) -> Self {
        Self {
            max_buy_sol: c.max_buy_sol,
            max_slippage_bps: c.max_slippage_bps,
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
    purchased: parking_lot::RwLock<HashSet<Pubkey>>, // track already bought mints (avoid double buy)
    treasury: Arc<Treasury>,
    risk: parking_lot::RwLock<RiskState>,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
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
    async fn rpc_retry_tx(
        &self,
        tx: &Transaction,
        max_attempts: u32,
    ) -> Result<solana_sdk::signature::Signature> {
        let mut attempt = 0;
        loop {
            match self.rpc.rpc.send_and_confirm_transaction(tx).await {
                Ok(sig) => return Ok(sig),
                Err(e) => {
                    attempt += 1;
                    RPC_RETRY_ATTEMPTS_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if attempt >= max_attempts {
                        return Err(e.into());
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
    pub fn new(
        rpc: Arc<SolanaRpc>,
        cfg: SniperCfg,
        raydium: Option<Arc<Raydium>>,
        orca: Option<Arc<Orca>>,
        treasury: Arc<Treasury>,
    ) -> Self {
        let (tx, rx) = tokio::sync::watch::channel(false);
        Self {
            rpc,
            cfg: parking_lot::RwLock::new(cfg),
            raydium,
            orca,
            purchased: parking_lot::RwLock::new(HashSet::new()),
            treasury,
            risk: parking_lot::RwLock::new(RiskState::default()),
            shutdown_tx: tx,
            shutdown_rx: rx,
        }
    }

    fn clone_for_spawn(&self) -> Self {
        let (tx, rx) = tokio::sync::watch::channel(false); // spawned clones don't receive shutdown (fire-and-forget tasks)
        SniperEngine {
            rpc: self.rpc.clone(),
            cfg: parking_lot::RwLock::new(self.cfg.read().clone()),
            raydium: self.raydium.clone(),
            orca: self.orca.clone(),
            purchased: parking_lot::RwLock::new(HashSet::new()),
            treasury: self.treasury.clone(),
            risk: parking_lot::RwLock::new(RiskState::default()),
            shutdown_tx: tx,
            shutdown_rx: rx,
        }
    }

    pub fn request_shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    fn http_to_ws(url: &str) -> String {
        if url.starts_with("https://") {
            url.replacen("https://", "wss://", 1)
        } else if url.starts_with("http://") {
            url.replacen("http://", "ws://", 1)
        } else {
            url.to_string()
        }
    }

    pub async fn subscribe_logs(&self) -> Result<()> {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::http::{HeaderName, HeaderValue};
        // Build endpoint list: prefer explicit primary WS from config, else derive from RPC URL; then add failovers
        let mut endpoints: Vec<String> = if let Some(primary) = self.rpc.primary_ws_url() {
            vec![primary]
        } else {
            vec![Self::http_to_ws(&self.rpc.rpc.url())]
        };
        for e in self.rpc.ws_failovers().iter() {
            endpoints.push(Self::http_to_ws(e));
        }
        // Bounded work queue to decouple socket reading from processing; apply backpressure if slow
        let (logs_tx, mut logs_rx) = tokio::sync::mpsc::channel::<Vec<String>>(512);
        // Spawn a single worker that processes logs with backpressure
        let engine_for_worker = self.clone_for_spawn();
        tokio::spawn(async move {
            while let Some(lines) = logs_rx.recv().await {
                engine_for_worker.extract_and_evaluate(lines).await;
            }
        });
        let programs = vec![
            RAYDIUM_AMM_V4.to_string(),
            ORCA_WHIRLPOOL_PROGRAM.to_string(),
        ];
        for pid in programs {
            let urls = endpoints.clone();
            let engine_clone = self.clone_for_spawn();
            // Use the real shutdown channel to allow graceful termination of WS tasks
            let mut shutdown_rx = self.shutdown_rx.clone();
            // Clone sender for this task
            let logs_tx = logs_tx.clone();
            tokio::spawn(async move {
                let sub_req = json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "logsSubscribe",
                    "params": [ { "mentions": [pid] }, { "commitment": "processed" } ]
                });
                let mut attempt: u32 = 0;
                let mut url_idx: usize = 0;
                loop {
                    // Check for shutdown before attempting another connect
                    if *shutdown_rx.borrow() {
                        break;
                    }
                    let start_connect = Instant::now();
                    let url = urls
                        .get(url_idx % urls.len())
                        .cloned()
                        .unwrap_or_else(|| urls[0].clone());
                    // Build a proper client request so tungstenite generates required WS headers
                    let mut req = match url.as_str().into_client_request() {
                        Ok(r) => r,
                        Err(e) => {
                            tracing::warn!(?e, url=%url, "invalid websocket URL");
                            // rotate endpoint
                            attempt = attempt.wrapping_add(1);
                            WS_RECONNECTS_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            let _ = crate::solana::rpc::SolanaRpc::sleep_with_backoff(attempt, crate::solana::rpc::ErrorClass::Other).await;
                            continue;
                        }
                    };
                    {
                        // Only allow safe, non-reserved headers to avoid corrupting the WS handshake.
                        let headers = req.headers_mut();
                        for (k, v) in engine_clone.rpc.ws_headers().iter() {
                            let k_lc = k.to_ascii_lowercase();
                            let allow = k_lc == "sec-websocket-protocol" || k_lc == "authorization" || k_lc == "user-agent";
                            if allow {
                                if let (Ok(name), Ok(value)) = (
                                    HeaderName::from_bytes(k.as_bytes()),
                                    HeaderValue::from_str(v),
                                ) {
                                    headers.insert(name, value);
                                } else {
                                    tracing::debug!(key=%k, "skipping invalid websocket header value from config");
                                }
                            } else if k_lc.starts_with("sec-websocket-") || k_lc == "connection" || k_lc == "upgrade" || k_lc == "host" {
                                tracing::debug!(key=%k, "skipping reserved websocket header from config");
                            } else {
                                tracing::debug!(key=%k, "skipping non-allowlisted websocket header from config");
                            }
                        }
                    }
                    // Optional connect timeout wrapper
                    let connect_fut = tokio_tungstenite::connect_async(req);
                    let ms = engine_clone.rpc.ws_connect_timeout_ms();
                    let connect_res =
                        match tokio::time::timeout(Duration::from_millis(ms), connect_fut).await {
                            Ok(res) => res,
                            Err(_elapsed) => Err(WsError::Io(std::io::Error::new(
                                std::io::ErrorKind::TimedOut,
                                "ws connect timeout",
                            ))),
                        };
                    // default backoff class if we need to reconnect
                    let mut backoff_class = crate::solana::rpc::ErrorClass::Other;
                    match connect_res {
                        Ok((mut ws, resp)) => {
                            attempt = 0;
                            WS_ACTIVE_CONNECTIONS
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            if ws.send(Message::text(sub_req.to_string())).await.is_err() {
                                WS_ACTIVE_CONNECTIONS
                                    .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                                return;
                            }
                            info!(program_id = %pid, url=%url, status=?resp.status(), elapsed_ms = start_connect.elapsed().as_millis(), "sniper websocket connected");
                            let mut last_msg = Instant::now();
                            let mut heartbeat = tokio::time::interval(Duration::from_secs(30));
                            // Wait for subscribe confirm with id
                            let mut subscribed: bool = false;
                            loop {
                                tokio::select! {
                                    _ = shutdown_rx.changed() => {
                                        if *shutdown_rx.borrow() { break; }
                                    }
                                    _ = heartbeat.tick() => {
                                        // heartbeats: if no message for > 90s, consider stale and break to reconnect
                                        if last_msg.elapsed() > Duration::from_secs(90) {
                                            WS_HEARTBEAT_MISSES_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                            warn!(program_id=%pid, "ws heartbeat stale – reconnecting");
                                            break;
                                        } else {
                                            // send ping proactively
                                            if ws.send(Message::Ping(Vec::new())).await.is_err() { break; }
                                        }
                                    }
                                    maybe_msg = ws.next() => {
                                        match maybe_msg {
                                            Some(Ok(Message::Text(txt))) => {
                                                WS_MESSAGES_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                                last_msg = Instant::now();
                                                if !subscribed {
                                                    // Expect a response with a subscription id: {"result": <number>, "id": 1}
                                                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&txt) {
                                                        if v.get("id").and_then(|x| x.as_i64()) == Some(1) {
                                                            if v.get("result").is_some() {
                                                                subscribed = true;
                                                                debug!(program_id=%pid, "ws logsSubscribe confirmed");
                                                                continue;
                                                            } else if v.get("error").is_some() {
                                                                warn!(text=%txt, program_id=%pid, "ws subscribe error, reconnecting");
                                                                backoff_class = crate::solana::rpc::ErrorClass::Other;
                                                                break; // reconnect
                                                            }
                                                        }
                                                    }
                                                }
                                                if txt.contains("logsNotification") {
                                                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&txt) {
                                                        if let Some(arr) = v.pointer("/params/result/value/logs").and_then(|x| x.as_array()) {
                                                            let payload: Vec<String> = arr.iter().filter_map(|e| e.as_str().map(|s| s.to_string())).collect();
                                                            // Apply backpressure: try fast path; if full, await send with returned payload; if closed, break
                                                            match logs_tx.try_send(payload) {
                                                                Ok(_) => {}
                                                                Err(tokio::sync::mpsc::error::TrySendError::Full(p)) => {
                                                                    if let Err(e2) = logs_tx.send(p).await {
                                                                        warn!(?e2, program_id=%pid, "ws logs worker closed");
                                                                        break;
                                                                    }
                                                                }
                                                                Err(tokio::sync::mpsc::error::TrySendError::Closed(_p)) => {
                                                                    warn!(program_id=%pid, "ws logs worker closed (channel)");
                                                                    break;
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                            Some(Ok(Message::Binary(_))) => { WS_MESSAGES_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed); last_msg = Instant::now(); }
                                            Some(Ok(Message::Pong(_))) => { last_msg = Instant::now(); }
                                            Some(Ok(Message::Ping(p))) => { let _ = ws.send(Message::Pong(p)).await; last_msg = Instant::now(); }
                                            Some(Ok(Message::Close(_))) => { warn!(program_id=%pid, "ws closed by peer"); break; }
                                            Some(Ok(Message::Frame(_))) => { /* ignore internal frame */ }
                                            Some(Err(e)) => { warn!(?e, program_id=%pid, "ws error"); break; }
                                            None => { warn!(program_id=%pid, "ws stream ended"); break; }
                                        }
                                    }
                                }
                            }
                            WS_ACTIVE_CONNECTIONS
                                .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                        }
                        Err(e) => {
                            let mut status: Option<u16> = None;
                            // Extract HTTP status if available for adaptive backoff
                            if let WsError::Http(resp) = &e {
                                status = Some(resp.status().as_u16());
                            }
                            warn!(?e, status, program_id=%pid, url=%url, "failed websocket connect");
                            // On failure, rotate endpoint after a couple attempts to avoid sticky failures
                            if attempt % 3 == 2 {
                                url_idx = url_idx.wrapping_add(1);
                            }
                            backoff_class = match status {
                                Some(429) => crate::solana::rpc::ErrorClass::Http(429),
                                Some(503) => crate::solana::rpc::ErrorClass::Http(503),
                                Some(504) => crate::solana::rpc::ErrorClass::Http(504),
                                Some(500) => crate::solana::rpc::ErrorClass::Http(500),
                                Some(502) => crate::solana::rpc::ErrorClass::Http(502),
                                _ => crate::solana::rpc::ErrorClass::Other,
                            };
                        }
                    }
                    WS_RECONNECTS_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    attempt += 1;
                    // Use the shared backoff helper
                    tokio::select! {
                        _ = crate::solana::rpc::SolanaRpc::sleep_with_backoff(attempt, backoff_class) => {},
                        _ = shutdown_rx.changed() => {
                            if *shutdown_rx.borrow() { break; }
                        }
                    }
                    if *shutdown_rx.borrow() {
                        break;
                    }
                }
            });
        }
        Ok(())
    }

    #[allow(dead_code)]
    async fn handle_logs_static(_logs: Vec<String>) {
        // deprecated placeholder
    }

    async fn extract_and_evaluate(&self, logs: Vec<String>) {
        static BASE58_RE: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"[1-9A-HJ-NP-Za-km-z]{32,44}").unwrap());
        for line in logs {
            let lower = line.to_ascii_lowercase();
            if !(lower.contains("init") || lower.contains("initialize")) {
                continue;
            }
            if !(lower.contains("pool") || lower.contains("whirlpool")) {
                continue;
            }
            debug!(line = %line, "sniper: init-like log");
            let mut seen = std::collections::HashSet::new();
            for m in BASE58_RE.find_iter(&line) {
                let s = m.as_str();
                if !seen.insert(s) {
                    continue;
                }
                if let Ok(pk) = Pubkey::from_str(s) {
                    // Run LP concentration check (if thresholds configured)
                    match self.lp_lock_check(&pk).await {
                        Ok(Some(assess)) => {
                            if assess.concentration_ok {
                                // Attempt liquidity estimation (Raydium/Orca pool scan) if min_pool_liquidity_sol configured
                                let mut liq_sol: Option<f64> = None;
                                if self.cfg.read().min_pool_liquidity_sol.is_some() {
                                    match self.estimate_liquidity_index(&pk).await {
                                        Ok(v) => liq_sol = v,
                                        Err(_) => {
                                            liq_sol = self
                                                .estimate_liquidity_for_mint(&pk)
                                                .await
                                                .ok()
                                                .flatten();
                                        }
                                    }
                                }
                                info!(mint = %pk, top1 = assess.top1_pct, top3 = assess.top3_pct, top5 = assess.top5_pct, burned = assess.burned_pct, program_locked = assess.program_vault_pct, liq_sol, "sniper: candidate mint passes concentration");
                                // Gate by liquidity threshold if configured
                                let liq_ok = self
                                    .cfg
                                    .read()
                                    .min_pool_liquidity_sol
                                    .map(|thr| liq_sol.unwrap_or(0.0) >= thr)
                                    .unwrap_or(true);
                                if liq_ok {
                                    // Risk: Notional cap & daily loss limit pre-check
                                    let base_buy = self.effective_max_buy_sol();
                                    if !self.can_open_position_for(&pk, base_buy) {
                                        debug!(mint=%pk, "sniper: risk gate blocked new position (notional/daily loss)");
                                        continue;
                                    }
                                    // Avoid duplicate buys
                                    if !self.purchased.read().contains(&pk) {
                                        if let Err(e) = self.attempt_initial_buy(&pk, liq_sol).await
                                        {
                                            warn!(mint = %pk, error = ?e, "sniper: initial buy failed");
                                        }
                                    } else {
                                        debug!(mint = %pk, "sniper: already purchased, skip");
                                    }
                                } else {
                                    debug!(mint = %pk, liq_sol, "sniper: below min liquidity -> no buy");
                                }
                            } else {
                                debug!(mint = %pk, top1 = assess.top1_pct, top3 = assess.top3_pct, top5 = assess.top5_pct, "sniper: rejected by concentration");
                            }
                        }
                        Ok(None) => {
                            debug!(mint = %pk, "sniper: no lp thresholds configured or insufficient data");
                        }
                        Err(e) => {
                            debug!(mint = %pk, error = ?e, "sniper: lp check error");
                        }
                    }
                }
            }
        }
    }
} // close primary impl SniperEngine block before auxiliary helpers

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
    async fn attempt_initial_buy(&self, mint: &Pubkey, liq_sol: Option<f64>) -> Result<()> {
        // Choose Raydium first (faster listing) – require connector
        let ray = self.raydium.clone();
        let orca = self.orca.clone();
        // Determine input (SOL) and output (mint) ordering for swap (we buy the mint with SOL)
        let sol_mint = Pubkey::from_str("So11111111111111111111111111111111111111112").unwrap();
        // Convert max_buy_sol (f64) to lamports safely
        let lamports_in = ((self.effective_max_buy_sol() * 1e9) as u64).max(10_000); // dynamic drawdown-adjusted size
                                                                                     // Ensure destination ATA for target mint (may fail if mint malformed)
        use solana_sdk::pubkey::Pubkey as SdkPubkey;
        let mint_sdk = SdkPubkey::new_from_array(mint.to_bytes());
        let owner_sdk = self.treasury.pubkey();
        let _dest_ata = match self
            .treasury
            .ensure_ata(&self.rpc, &owner_sdk, &mint_sdk)
            .await
        {
            Ok(a) => a,
            Err(e) => {
                debug!(?e, mint=%mint, "ensure dest ATA fail");
                return Ok(());
            }
        };
        // Wrap SOL into WSOL ATA (Raydium expects token account)
        let (wsol_ata_sdk, _wrap_sig) = match self.treasury.wrap_sol(&self.rpc, lamports_in).await {
            Ok(v) => v,
            Err(e) => {
                debug!(?e, lamports_in, "wrap_sol failed");
                return Ok(());
            }
        };
        // Build auto plan for min_out computation & pool selection (Raydium)
        let msb = self.adaptive_slippage_bps();
        let plan_opt = if let Some(r) = &ray {
            r.build_swap_plan_auto(&sol_mint.to_string(), &mint.to_string(), lamports_in, msb)?
        } else {
            None
        };
        if plan_opt.is_none() && orca.is_none() {
            debug!(mint=%mint, "no raydium or orca route");
            return Ok(());
        }
        let plan_meta = plan_opt;
        // Dynamic route selection: compare Raydium vs Orca quotes and pick higher expected_out
        let mut used_raydium: bool = false;
        let ray_quote_out: u64 = plan_meta.as_ref().map(|pm| pm.expected_out).unwrap_or(0);
        let mut orca_quote_out: u64 = 0;
        if let Some(o) = &orca {
            if let Ok(Some(q)) = o
                .quote_exact_in(&sol_mint.to_string(), &mint.to_string(), lamports_in)
                .await
            {
                orca_quote_out = q.amount_out;
            }
        }
        if ray_quote_out > 0 || orca_quote_out > 0 {
            used_raydium = ray_quote_out >= orca_quote_out;
            if used_raydium {
                crate::metrics::DEX_SELECTION_ENTRY_RAYDIUM_TOTAL
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            } else {
                crate::metrics::DEX_SELECTION_ENTRY_ORCA_TOTAL
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            debug!(mint=%mint, lamports_in, ray_out=ray_quote_out, orca_out=orca_quote_out, chosen = if used_raydium { "RAYDIUM" } else { "ORCA" }, "sniper: dynamic dex selection");
        } else {
            // Fallback to previous heuristic if no quotes available
            if let Some(ref pm) = plan_meta {
                if pm.pool.is_some() {
                    used_raydium = true;
                }
            }
        }
        // Source WSOL ATA (after wrap) & destination token ATA
        let _wsol_ata = wsol_ata_sdk; // already ensured via wrap
        let (_dest_ata, _token_prog) = self
            .treasury
            .ata_address(&self.rpc, &owner_sdk, &mint_sdk)
            .await?;
        // Derive Serum market related accounts from snapshot (already stored inside Raydium SimplePool)
        // We still need serum bids/asks/event/base_vault/quote_vault; currently not exposed -> placeholder fetch skipped (in full impl these would be read separately)
        // For now abort if we cannot assemble full accounts; fallback to pseudo plan already logged earlier.
        // (Future: extend Raydium snapshot to include serum market detail accounts.)
        // Try to upgrade to full swap instruction if serum accounts present in snapshot
        // Do not allow pseudo Raydium ixs; require full instruction. Start with empty set.
        let mut final_ixs: Vec<solana_sdk::instruction::Instruction> = Vec::new();
        if used_raydium {
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
                                if let Ok(ray_prog) = Pubkey::from_str(RAYDIUM_AMM_V4) {
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
                                            ray_prog,
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
        }
        // If Raydium was chosen but we couldn't assemble a full instruction, abort Raydium path and try Orca below.
        if used_raydium && final_ixs.is_empty() {
            tracing::info!(mint=%mint, "raydium full instruction unavailable; falling back to orca/pseudo plan");
            used_raydium = false;
        }
        // Re-Quote unmittelbar vor Signatur: aktualisiere min_out/Route
        let mut plan_effective = plan_meta.clone();
        if used_raydium {
            if let Some(old_pm) = plan_effective.as_ref().or(plan_meta.as_ref()) {
                let _ = old_pm;
            }
            if let Some(r) = &ray {
                if let Ok(Some(new_pm)) = r.build_swap_plan_auto(
                    &sol_mint.to_string(),
                    &mint.to_string(),
                    lamports_in,
                    msb,
                ) {
                    // metrics: compare min_out
                    if let Some(old_pm) = plan_effective.as_ref().or(plan_meta.as_ref()) {
                        crate::metrics::REQUOTE_EVENTS_TOTAL
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let old = old_pm.min_out.max(1);
                        let newv = new_pm.min_out.max(1);
                        if newv >= old {
                            crate::metrics::REQUOTE_IMPROVED_TOTAL
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        } else {
                            crate::metrics::REQUOTE_WORSENED_TOTAL
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                        let ratio = (newv as f64 / old as f64) - 1.0; // signed
                        let micro =
                            (ratio * 1_000_000.0).clamp(i64::MIN as f64, i64::MAX as f64) as i64;
                        crate::metrics::REQUOTE_MIN_OUT_DELTA_RATIO_MICRO_SUM
                            .fetch_add(micro, std::sync::atomic::Ordering::Relaxed);
                    }
                    plan_effective = Some(new_pm);
                }
                // Rebuild final_ixs if possible (full Raydium instruction) using updated min_out
                if let (Some(pool_addr), Some(pm_eff)) = (
                    plan_effective.as_ref().and_then(|p| p.pool),
                    plan_effective.as_ref(),
                ) {
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
                                if let Ok(ray_prog) = Pubkey::from_str(RAYDIUM_AMM_V4) {
                                    let auth_pk =
                                        Pubkey::new_from_array(self.treasury.pubkey().to_bytes());
                                    let user_source = Pubkey::new_from_array(_wsol_ata.to_bytes());
                                    let user_dest = Pubkey::new_from_array(_dest_ata.to_bytes());
                                    let token_prog_pk =
                                        Pubkey::new_from_array(token_prog.to_bytes());
                                    let rent_pk = Pubkey::new_from_array(rent_sysvar.to_bytes());
                                    if let Ok(full_ix) = r.build_swap_instruction(
                                        pool_addr,
                                        sol_mint,
                                        *mint,
                                        lamports_in,
                                        pm_eff.min_out,
                                        auth_pk,
                                        user_source,
                                        user_dest,
                                        ray_prog,
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
        } else {
            // Orca path: recompute min_out and rebuild ixs
            if let Some(o) = &orca {
                let mut min_out_orca = 1u64;
                if let Ok(Some(q)) = o
                    .quote_exact_in(&sol_mint.to_string(), &mint.to_string(), lamports_in)
                    .await
                {
                    let slip = self.adaptive_slippage_bps() as u128;
                    min_out_orca = ((q.amount_out as u128) * (10_000 - slip) / 10_000) as u64;
                    if min_out_orca == 0 {
                        min_out_orca = 1;
                    }
                }
                // metrics: compare against previous effective min_out if any (we only track delta sign for Orca)
                crate::metrics::REQUOTE_EVENTS_TOTAL
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if let Some(old_pm) = plan_effective.as_ref().or(plan_meta.as_ref()) {
                    let old = old_pm.min_out.max(1);
                    if min_out_orca >= old {
                        crate::metrics::REQUOTE_IMPROVED_TOTAL
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    } else {
                        crate::metrics::REQUOTE_WORSENED_TOTAL
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    let ratio = (min_out_orca as f64 / old as f64) - 1.0;
                    let micro =
                        (ratio * 1_000_000.0).clamp(i64::MIN as f64, i64::MAX as f64) as i64;
                    crate::metrics::REQUOTE_MIN_OUT_DELTA_RATIO_MICRO_SUM
                        .fetch_add(micro, std::sync::atomic::Ordering::Relaxed);
                }
                if let Ok(ixs2) = o.build_swap_ix(
                    &sol_mint.to_string(),
                    &mint.to_string(),
                    lamports_in,
                    min_out_orca,
                ) {
                    if !ixs2.is_empty() {
                        final_ixs = ixs2;
                    }
                }
            }
        }
        let bh: Hash = match self.rpc.get_latest_blockhash_retry().await {
            Ok(h) => h,
            Err(e) => {
                RPC_ERRORS_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return Err(e.into());
            }
        };
        let tx_ixs: Vec<solana_sdk::instruction::Instruction> = if used_raydium {
            final_ixs
        } else {
            // Orca path: build swap ix directly
            if let Some(o) = &orca {
                let wsol_mint_prog = spl_token::native_mint::id();
                let wsol_mint_sdk = Pubkey::new_from_array(wsol_mint_prog.to_bytes());
                o.set_user_authority(Pubkey::new_from_array(self.treasury.pubkey().to_bytes()));
                if let Ok((wsol_ata, _prog)) = self
                    .treasury
                    .ata_address(&self.rpc, &self.treasury.pubkey(), &wsol_mint_sdk)
                    .await
                {
                    o.set_user_token_account(wsol_mint_sdk, wsol_ata);
                }
                if let Ok((dst_ata, _prog2)) = self
                    .treasury
                    .ata_address(&self.rpc, &self.treasury.pubkey(), &mint_sdk)
                    .await
                {
                    o.set_user_token_account(mint_sdk, dst_ata);
                }
                // Derive min_out with slippage tolerance via quote
                let mut min_out_orca = 1u64; // fallback
                if let Ok(Some(q)) = o
                    .quote_exact_in(&sol_mint.to_string(), &mint.to_string(), lamports_in)
                    .await
                {
                    let slip = self.cfg.read().max_slippage_bps as u128;
                    min_out_orca = ((q.amount_out as u128) * (10_000 - slip) / 10_000) as u64;
                    if min_out_orca == 0 {
                        min_out_orca = 1;
                    }
                }
                match o.build_swap_ix(
                    &sol_mint.to_string(),
                    &mint.to_string(),
                    lamports_in,
                    min_out_orca,
                ) {
                    Ok(ixs) => ixs,
                    Err(e) => {
                        debug!(?e, mint=%mint, "orca build_swap_ix failed");
                        Vec::new()
                    }
                }
            } else {
                Vec::new()
            }
        };
        if tx_ixs.is_empty() {
            debug!(mint=%mint, "no swap instructions built");
            return Ok(());
        }
        // Prepare message for fee estimate before signing
        let message = solana_sdk::message::Message::new(&tx_ixs, Some(&self.treasury.pubkey()));
        let fee_estimate = self
            .rpc
            .get_fee_for_message_retry(&message)
            .await
            .unwrap_or(0);
        let mut tx = Transaction::new_with_payer(&tx_ixs, Some(&self.treasury.pubkey()));
        tx.try_sign(&[self.treasury.signer_ref()], bh)?;
        let sent_at = Instant::now();
        match self.rpc_retry_tx(&tx, 3).await {
            Ok(sig) => {
                let dur = sent_at.elapsed();
                record_swap_latency(dur.as_nanos() as u64);
                TRADES_EXECUTED_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if used_raydium {
                    let pm = plan_effective.as_ref().or(plan_meta.as_ref()).unwrap();
                    info!(mint=%mint, sig=%sig, lamports_in, expected_out=pm.expected_out, min_out=pm.min_out, pool=?pm.pool, liq_sol, "sniper: initial buy submitted (raydium)");
                    // Approx position record (use expected_out as mid estimate)
                    if pm.expected_out > 0 {
                        let sol_in = lamports_in as f64 / 1e9;
                        self.record_fill_placeholder(*mint, sol_in);
                        self.finalize_fill(*mint).await;
                    }
                } else {
                    info!(mint=%mint, sig=%sig, lamports_in, liq_sol, "sniper: initial buy submitted (orca)");
                    // Orca: we only know lamports_in and min_out (very conservative); use min_out for price upper bound
                    let sol_in = lamports_in as f64 / 1e9;
                    self.record_fill_placeholder(*mint, sol_in);
                    self.finalize_fill(*mint).await;
                }
                self.purchased.write().insert(*mint);
                // Trade CSV log
                if used_raydium {
                    if let Some(pm) = plan_meta.as_ref() {
                        // Record pending expected tokens for later shortfall calculation
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
                            "{ts},BUY,{mint},RAYDIUM,{sig},{lamports_in},0,0,0,{exp_tokens},,{short_tokens},,{fee},,expected_min_out={min_out}",
                            ts=ChronoUtc::now().to_rfc3339(),
                            mint=mint,
                            sig=sig,
                            lamports_in=lamports_in,
                            exp_tokens=pm.expected_out,
                            short_tokens=0,
                            fee=fee_estimate,
                            min_out=pm.min_out
                        );
                        self.append_trade_record(&line, true);
                    }
                } else {
                    record_network_fee(fee_estimate);
                    let line = format!(
                        "{ts},BUY,{mint},ORCA,{sig},{lamports_in},0,0,0,0,,0,,{fee},,notes=orca_buy",
                        ts=ChronoUtc::now().to_rfc3339(),
                        mint=mint,
                        sig=sig,
                        lamports_in=lamports_in,
                        fee=fee_estimate
                    );
                    self.append_trade_record(&line, true);
                }
                // Attempt WSOL unwrap to reclaim leftover lamports
                match self.treasury.unwrap_wsol(&self.rpc, None).await {
                    Ok(unwrap_sig) => {
                        info!(mint=%mint, unwrap_sig=%unwrap_sig, "sniper: wsol unwrapped post-trade")
                    }
                    Err(e) => debug!(?e, mint=%mint, "sniper: wsol unwrap failed"),
                }
            }
            Err(e) => {
                warn!(?e, mint=%mint, "sniper: buy tx failed (will not retry immediately)");
                TRADES_FAILED_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                RPC_ERRORS_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
        // TODO: unwrap remaining WSOL (not done yet)
        Ok(())
    }

    pub async fn run(&self) -> Result<()> {
        self.try_load_risk_state();
        self.subscribe_logs().await?;
        info!("sniper engine running (skeleton)");
        // Main loop heartbeat (lightweight)
        let mut iv = tokio::time::interval(Duration::from_secs(15));
        // Separate Exit Evaluation Task (configurable interval)
        let exit_secs = self.cfg.read().exit_eval_interval_secs.unwrap_or(15).max(1);
        let engine_clone_exit = self.clone_for_spawn();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(exit_secs));
            loop {
                tick.tick().await;
                if let Err(e) = engine_clone_exit.evaluate_positions().await {
                    tracing::debug!(?e, "exit evaluation error");
                }
            }
        });
        // Autosave task
        let autosave_secs: u64 = std::env::var("IRONCRAB_RISK_AUTOSAVE_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(60);
        if autosave_secs > 0 {
            let engine_clone = self.clone_for_spawn();
            tokio::spawn(async move {
                let mut saver = tokio::time::interval(Duration::from_secs(autosave_secs));
                loop {
                    saver.tick().await;
                    engine_clone.persist_risk_state();
                }
            });
        }
        // Config Hot Reload (ENV IRONCRAB_SNIPER_RELOAD_PATH)
        if let Some(path) = std::env::var_os("IRONCRAB_SNIPER_RELOAD_PATH") {
            let path_buf = std::path::PathBuf::from(path);
            let _engine_clone_poll = self.clone_for_spawn();
            // Polling fallback (disabled if feature notify_watch active)
            #[cfg(not(feature = "notify_watch"))]
            {
                let path_poll = path_buf.clone();
                tokio::spawn(async move {
                    let mut ivr = tokio::time::interval(Duration::from_secs(30));
                    let mut last_mod = std::time::SystemTime::UNIX_EPOCH;
                    loop {
                        ivr.tick().await;
                        if let Ok(meta) = std::fs::metadata(&path_poll) {
                            if let Ok(modified) = meta.modified() {
                                if modified > last_mod {
                                    last_mod = modified;
                                    if let Ok(txt) = std::fs::read_to_string(&path_poll) {
                                        if let Ok(root) =
                                            toml::from_str::<crate::config::Config>(&txt)
                                        {
                                            if let Some(sn) = root.sniper.clone() {
                                                let new_cfg: SniperCfg = (&sn).into();
                                                let mut guard = _engine_clone_poll.cfg.write();
                                                let diff = diff_sniper_cfg(&guard, &new_cfg);
                                                if let Err(reason) = validate_sniper_cfg(&new_cfg) {
                                                    tracing::warn!(%reason, "rejecting config reload");
                                                } else {
                                                    *guard = new_cfg;
                                                    tracing::info!(diff=%diff, "sniper hot reload applied (poll)");
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                });
            }
            // File watch (if feature enabled)
            #[cfg(feature = "notify_watch")]
            {
                let engine_clone_watch = self.clone_for_spawn();
                let path_fw = path_buf.clone();
                tokio::spawn(async move {
                    let apply = move |new_cfg: SniperCfg, diff: String| {
                        let mut guard = engine_clone_watch.cfg.write();
                        if let Err(reason) = validate_sniper_cfg(&new_cfg) {
                            tracing::warn!(%reason, "rejecting config reload (watch)");
                            return;
                        }
                        *guard = new_cfg;
                        tracing::info!(diff=%diff, "sniper hot reload applied (watch)");
                    };
                    if let Err(e) = crate::config_reload::watch_and_reload(path_fw, apply).await {
                        tracing::warn!(?e, "file watch init failed, fallback to no reload");
                    }
                });
            }
            // SIGHUP handler (Unix only)
            #[cfg(unix)]
            {
                use std::sync::Arc as StdArc;
                let engine_arc = StdArc::new(self.clone_for_spawn());
                let path2 = path_buf.clone();
                let apply_cb = {
                    let engine_arc = engine_arc.clone();
                    StdArc::new(move |new_cfg: SniperCfg, _diff: String| {
                        let mut guard = engine_arc.cfg.write();
                        if let Err(reason) = validate_sniper_cfg(&new_cfg) {
                            tracing::warn!(%reason, "rejecting config reload (SIGHUP)");
                            return;
                        }
                        let diff = diff_sniper_cfg(&guard, &new_cfg);
                        *guard = new_cfg;
                        tracing::info!(diff=%diff, "sniper hot reload applied (SIGHUP)");
                    })
                };
                crate::config_reload::spawn_sighup_reload(path2, apply_cb);
            }
        }
        let shutdown_rx = self.shutdown_rx.clone();
        loop {
            if *shutdown_rx.borrow() {
                break;
            }
            iv.tick().await;
            let mb = self.cfg.read().max_buy_sol;
            debug!(max_buy_sol = mb, "sniper heartbeat");
            crate::metrics::record_activity();
            // Exit evaluation now handled by separate task
            // Pending trade TTL cleanup
            if let Some(ttl) = self.cfg.read().pending_trade_ttl_secs {
                if ttl > 0 {
                    self.cleanup_stale_pending(ttl);
                }
            }
            // Reconciliation pass (half of TTL age threshold)
            if let Some(ttl) = self.cfg.read().pending_trade_ttl_secs {
                if ttl >= 20 {
                    // minimal threshold
                    let cutoff = (ttl as i64) / 2; // drop if older than half TTL w/o status
                    let engine_clone = self.clone_for_spawn();
                    tokio::spawn(async move {
                        engine_clone.reconcile_pending(cutoff).await;
                    });
                }
            }
        }
        info!("sniper engine shutdown");
        self.persist_risk_state();
        Ok(())
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
                return false;
            }
        }
        if let Some(daily) = cfg_r.daily_loss_limit_sol {
            if rs.realized_loss_today_sol >= daily {
                return false;
            }
        }
        if let Some(mop) = cfg_r.max_open_positions {
            if rs.open.len() >= mop {
                return false;
            }
        }
        if let Some(until) = rs.cooldown_until.get(mint) {
            if *until > chrono::Utc::now().timestamp() {
                return false;
            }
        }
        true
    }

    fn record_fill_placeholder(&self, mint: Pubkey, invested_sol: f64) {
        self.risk_reset_if_needed();
        let mut rs = self.risk.write();
        // Respect per-mint lot limit if configured
        if let Some(limit) = self.cfg.read().per_mint_position_limit {
            if let Some(v) = rs.open.get(&mint) {
                if v.len() as u32 >= limit {
                    return;
                }
            }
        }
        let lot = PositionLot {
            entry_price_sol: 0.0,
            amount_tokens: 0.0,
            invested_sol,
            token_decimals: 0,
            last_unrealized_pnl_sol: 0.0,
            opened_ts: chrono::Utc::now().timestamp(),
            executed_tp_bps: Vec::new(),
            peak_pnl_bps: 0,
        };
        rs.open.entry(mint).or_default().push(lot);
        let lots: usize = rs.open.values().map(|v| v.len()).sum();
        OPEN_POSITIONS_GAUGE.store(lots as u64, std::sync::atomic::Ordering::Relaxed);
    }

    async fn finalize_fill(&self, mint: Pubkey) {
        let owner = self.treasury.pubkey();
        let mint_sdk = solana_sdk::pubkey::Pubkey::new_from_array(mint.to_bytes());
        let ata = match self
            .treasury
            .ata_address(&self.rpc, &owner, &mint_sdk)
            .await
        {
            Ok(v) => v.0,
            Err(_) => return,
        };
        let decimals =
            crate::solana::token_utils::get_token_decimals_or_default(&self.rpc, &mint_sdk).await;
        let acc_opt = self.rpc.get_account_retry(&ata).await.ok();
        if let Some(acc) = acc_opt {
            if acc.data.len() >= 72 {
                let raw = u64::from_le_bytes(acc.data[64..72].try_into().unwrap());
                let amt = if decimals == 0 {
                    raw as f64
                } else {
                    raw as f64 / 10f64.powi(decimals as i32)
                };
                if amt <= 0.0 {
                    return;
                }
                // Take snapshot of pending & position without holding lock across RPC
                let (pend_opt, entry_price_existing) = {
                    let mut rs = self.risk.write();
                    let pend = rs.pending.remove(&mint);
                    if let Some(v) = rs.open.get_mut(&mint) {
                        if let Some(last) = v.last_mut() {
                            if last.entry_price_sol == 0.0 {
                                last.amount_tokens = amt;
                                last.token_decimals = decimals;
                                last.entry_price_sol = last.invested_sol / amt.max(1e-9);
                            }
                        }
                    }
                    let entry_price_last = rs
                        .open
                        .get(&mint)
                        .and_then(|v| v.last())
                        .map(|l| l.entry_price_sol)
                        .unwrap_or(0.0);
                    (pend, entry_price_last)
                };
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
                    let mut actual_raw = (amt * scale).round() as u64; // fallback
                                                                       // Try recomputing actual_raw from meta pre/post if available (owner+mint delta)
                                                                       // Since we can't pass from the inner scope easily, recompute quickly here by fetching the tx again (cheap in local scope)
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
                                if let OptionSerializer::Some(pre) =
                                    meta.pre_token_balances.as_ref()
                                {
                                    for b in pre {
                                        let owner_ok = match b.owner.as_ref() {
                                            OptionSerializer::Some(o) => o == &owner_str,
                                            _ => false,
                                        };
                                        if owner_ok && b.mint == mint_str {
                                            if let Ok(v) = b.ui_token_amount.amount.parse::<u128>()
                                            {
                                                pre_raw_opt = Some(v);
                                                break;
                                            }
                                        }
                                    }
                                }
                                if let OptionSerializer::Some(post) =
                                    meta.post_token_balances.as_ref()
                                {
                                    for b in post {
                                        let owner_ok = match b.owner.as_ref() {
                                            OptionSerializer::Some(o) => o == &owner_str,
                                            _ => false,
                                        };
                                        if owner_ok && b.mint == mint_str {
                                            if let Ok(v) = b.ui_token_amount.amount.parse::<u128>()
                                            {
                                                post_raw_opt = Some(v);
                                                break;
                                            }
                                        }
                                    }
                                }
                                if let (Some(pre_raw), Some(post_raw)) = (pre_raw_opt, post_raw_opt)
                                {
                                    if post_raw >= pre_raw {
                                        let delta = (post_raw - pre_raw) as u64;
                                        if delta > 0 {
                                            actual_raw = delta;
                                        }
                                    }
                                }
                                // Protocol/referral fee attribution from meta is protocol-specific; pending detailed parsing of pool fee accounts.
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
                            .unwrap_or(self.cfg.read().max_slippage_bps)
                            as i64;
                        let max_b = self
                            .cfg
                            .read()
                            .adaptive_slippage_max_bps
                            .unwrap_or(self.cfg.read().max_slippage_bps)
                            as i64;
                        let mut cur = rs
                            .adaptive_slippage_bps
                            .unwrap_or(self.cfg.read().max_slippage_bps)
                            as i64;
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
                            / (10_000u128 - pend.fee_bps as u128))
                            as u64;
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
                }
            }
        }
    }

    async fn evaluate_positions(&self) -> Result<()> {
        // Skip if no thresholds configured
        {
            let r = self.cfg.read();
            if r.stop_loss_bps.is_none()
                && r.take_profit_bps.is_none()
                && r.take_profit_tiers.is_none()
            {
                return Ok(());
            }
        }
        let (stop_bps, tp_bps, tiers, trailing, min_exit_notional) = {
            let r = self.cfg.read();
            (
                r.stop_loss_bps.unwrap_or(u32::MAX),
                r.take_profit_bps.unwrap_or(u32::MAX),
                r.take_profit_tiers.clone(),
                r.trailing_stop_bps,
                r.min_exit_notional_sol.unwrap_or(0.0),
            )
        };
        let sol_mint = Pubkey::from_str("So11111111111111111111111111111111111111112").unwrap();
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
            // Quote exit value (prefer Raydium then Orca)
            let mut quote_out: Option<u64> = None;
            if let Some(r) = &self.raydium {
                if let Ok(Some(q)) = r
                    .quote_exact_in(
                        &mint.to_string(),
                        &sol_mint.to_string(),
                        pos.amount_tokens as u64,
                    )
                    .await
                {
                    quote_out = Some(q.amount_out);
                } else {
                    RPC_ERRORS_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
            if quote_out.is_none() {
                if let Some(o) = &self.orca {
                    if let Ok(Some(q)) = o
                        .quote_exact_in(
                            &mint.to_string(),
                            &sol_mint.to_string(),
                            pos.amount_tokens as u64,
                        )
                        .await
                    {
                        quote_out = Some(q.amount_out);
                    } else {
                        RPC_ERRORS_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
                let sell_tokens = (pos.amount_tokens * fraction).floor() as u64;
                if sell_tokens > 0 {
                    if let Err(e) = self
                        .attempt_exit(&mint, lot_idx, sell_tokens, fraction)
                        .await
                    {
                        warn!(?e, mint=%mint, "exit tx failed");
                    } else {
                        if full_exit || stop_trigger {
                            self.mark_cooldown(mint);
                        }
                        // metrics
                        metrics::PARTIAL_EXIT_EVENTS_TOTAL
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        metrics::PARTIAL_EXIT_FRACTION_MICRO_TOTAL.fetch_add(
                            (fraction * 1_000_000.0) as u64,
                            std::sync::atomic::Ordering::Relaxed,
                        );
                    }
                }
            }
        }
        Ok(())
    }

    async fn attempt_exit(
        &self,
        mint: &Pubkey,
        lot_idx: usize,
        amount_tokens: u64,
        fraction: f64,
    ) -> Result<()> {
        let sol_mint = Pubkey::from_str("So11111111111111111111111111111111111111112").unwrap();
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
            return Ok(());
        }
        // Cap sale to both requested amount_tokens and on-chain balance (safety)
        let sell_tokens = amount_tokens.min(ata_tokens);
        if sell_tokens == 0 {
            return Ok(());
        }
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

        // Dynamic route selection for exit: compare Raydium vs Orca quotes
        let msb2 = self.adaptive_slippage_bps();
        let ray_plan = if let Some(r) = &self.raydium {
            r.build_swap_plan_auto(&mint.to_string(), &sol_mint.to_string(), sell_tokens, msb2)
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
        let mut used_raydium = false;
        if ray_out > 0 || orca_out > 0 {
            used_raydium = ray_out >= orca_out;
            if used_raydium {
                crate::metrics::DEX_SELECTION_EXIT_RAYDIUM_TOTAL
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            } else {
                crate::metrics::DEX_SELECTION_EXIT_ORCA_TOTAL
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            debug!(mint=%mint, sell_tokens, ray_out, orca_out, chosen = if used_raydium { "RAYDIUM" } else { "ORCA" }, "sniper: dynamic exit dex selection");
        } else {
            // Fallback to Raydium if plan exists
            if let Some(ref p) = ray_plan {
                if p.as_ref().map(|rp| rp.pool.is_some()).unwrap_or(false) {
                    used_raydium = true;
                }
            }
        }

        // Build instructions based on chosen route; prefer full Raydium IX, fallback to Orca
        let bh: Hash = match self.rpc.get_latest_blockhash_retry().await {
            Ok(h) => h,
            Err(e) => {
                RPC_ERRORS_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return Err(e.into());
            }
        };
        let mut tx_ixs: Vec<solana_sdk::instruction::Instruction> = Vec::new();
        if used_raydium {
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
                                if let Ok(ray_prog) = Pubkey::from_str(RAYDIUM_AMM_V4) {
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
                                        ray_prog,
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
                used_raydium = false; // fallback
            }
        }
        if !used_raydium {
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
                    let msb3 = self.adaptive_slippage_bps() as u128;
                    min_out = ((q.amount_out as u128) * (10_000 - msb3) / 10_000) as u64;
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
        let sent_at = Instant::now();
        match self.rpc_retry_tx(&tx, 3).await {
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
                            self.finalize_fill(*mint).await;
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
            let _ = std::fs::create_dir_all(parent);
        }
        let snapshot = self.build_risk_snapshot_json();
        if let Ok(txt) = serde_json::to_string_pretty(&snapshot) {
            let _ = std::fs::write(&path, txt);
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

impl SniperEngine {
    /// Index-based (Raydium/Orca) liquidity estimation using current pool snapshots.
    /// Returns conservative SOL notionals (sum over pools: 2 * SOL_reserve for SOL pairs, stable converted to SOL via placeholder rate).
    async fn estimate_liquidity_index(&self, mint: &Pubkey) -> Result<Option<f64>> {
        let sol_mint = Pubkey::from_str("So11111111111111111111111111111111111111112").unwrap();
        let usdc = Pubkey::from_str("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v").unwrap();
        let usdt = Pubkey::from_str("Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB").unwrap();
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
        let sol_mint = Pubkey::from_str("So11111111111111111111111111111111111111112").unwrap();
        let usdc = Pubkey::from_str("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v").unwrap();
        let usdt = Pubkey::from_str("Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB").unwrap();
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
            if r.lp_top1_max_pct.is_none()
                && r.lp_top3_max_pct.is_none()
                && r.lp_top5_max_pct.is_none()
            {
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
        let mint_acc = match self.rpc.get_account_retry(mint).await {
            Ok(a) => a,
            Err(e) => {
                warn!(?e, "mint account fetch failed");
                return Ok(None);
            }
        };
        // SPL Mint length heuristic (approx range)
        if mint_acc.data.len() < 70 || mint_acc.data.len() > 90 {
            return Ok(None);
        }
        // Parse fields using helper and prefer RPC supply/decimals if available
        let (mint_auth_opt, freeze_auth_opt, parsed_decimals, parsed_supply_raw) =
            parse_spl_mint_fields(&mint_acc.data);
        // Try authoritative RPC getTokenSupply for decimals and amount
        let mut decimals_eff = parsed_decimals;
        let mut supply = if parsed_decimals == 0 {
            parsed_supply_raw as f64
        } else {
            (parsed_supply_raw as f64) / 10f64.powi(parsed_decimals as i32)
        };
        if let Ok(s) = self.rpc.rpc.get_token_supply(mint).await {
            // Use RPC decimals and amount if parse succeeds
            decimals_eff = s.decimals;
            if let Ok(v) = s.amount.parse::<u128>() {
                supply = if s.decimals == 0 {
                    v as f64
                } else {
                    (v as f64) / 10f64.powi(s.decimals as i32)
                };
            }
        }
        if supply == 0.0 {
            return Ok(None);
        }
        // Owner blacklist gate
        let owners = self.cfg.read().blacklist_owners.clone();
        if owner_blacklisted(&owners, mint_auth_opt.as_ref(), freeze_auth_opt.as_ref()) {
            return Ok(None);
        }
        if self.cfg.read().require_freeze_auth_none.unwrap_or(false) && freeze_auth_opt.is_some() {
            return Ok(None);
        }
        if let Some((lo, hi)) = self.cfg.read().require_mint_decimals_range {
            let d = decimals_eff;
            if d < lo || d > hi {
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
            return Ok(None);
        }
        // Largest accounts
        let list = match self.rpc.rpc.get_token_largest_accounts(mint).await {
            Ok(v) => v,
            Err(e) => {
                warn!(?e, "largest accounts fetch failed");
                return Ok(None);
            }
        };
        if list.is_empty() {
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
        let incinerator = Pubkey::from_str("1nc1nerator11111111111111111111111111111111").unwrap();
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
            burned_pct,
            program_vault_pct,
        }))
    }
}
// end secondary impl SniperEngine helpers
// (auxiliary impl closed above)

pub async fn run_sniper(
    rpc: Arc<SolanaRpc>,
    cfg: SniperCfg,
    raydium: Option<Arc<Raydium>>,
    orca: Option<Arc<Orca>>,
    treasury: Arc<Treasury>,
) -> Result<()> {
    let engine = SniperEngine::new(rpc, cfg, raydium, orca, treasury);
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
