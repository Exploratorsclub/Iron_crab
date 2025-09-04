//! Meme Coin Sniper Skeleton – subscribes to pool creation logs and applies heuristics.
// Memecoin‑Sniper Skeleton: beobachtet neue Pools/LP‑Creations, filtert Risiken,
// setzt kleine Erstkäufe mit harten Limits (Slippage/Blacklist/Owner/Freeze Auth usw.)
use std::{sync::Arc, collections::HashSet};
use anyhow::Result;
use tracing::{info, debug, warn};
use crate::solana::rpc::SolanaRpc;
use crate::config::SniperSettings;
use solana_sdk::pubkey::Pubkey;
// (log subscription stub – real PubSub integration to be reintroduced with correct crate paths)
use once_cell::sync::Lazy;

use crate::solana::dex::raydium::RAYDIUM_AMM_V4;
use crate::solana::dex::orca::ORCA_WHIRLPOOL_PROGRAM;
use futures::{StreamExt, SinkExt};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use serde_json::json;

// Simple global blacklist (extendable via config later)
#[allow(dead_code)]
static MINT_BLACKLIST: Lazy<HashSet<String>> = Lazy::new(|| HashSet::new());

#[derive(Clone)]
pub struct SniperCfg {
    pub max_buy_sol: f64,
    pub max_slippage_bps: u32,
    pub blacklist_mints: Vec<String>,
    pub blacklist_owners: Vec<String>,
    pub min_pool_liquidity_sol: Option<f64>,
    pub require_freeze_auth_none: Option<bool>,
    pub require_mint_decimals_range: Option<(u8,u8)>,
    pub lp_top1_max_pct: Option<f64>,
    pub lp_top3_max_pct: Option<f64>,
    pub lp_top5_max_pct: Option<f64>,
}

pub struct SniperEngine {
    pub rpc: Arc<SolanaRpc>,
    cfg: SniperCfg,
}

impl SniperEngine {
    pub fn new(rpc: Arc<SolanaRpc>, cfg: SniperCfg) -> Self { Self { rpc, cfg } }

    fn http_to_ws(url: &str) -> String {
        if url.starts_with("https://") { url.replacen("https://", "wss://", 1) }
        else if url.starts_with("http://") { url.replacen("http://", "ws://", 1) } else { url.to_string() }
    }

    pub async fn subscribe_logs(&self) -> Result<()> {
        let ws_url = Self::http_to_ws(&self.rpc.rpc.url());
        let programs = vec![RAYDIUM_AMM_V4.to_string(), ORCA_WHIRLPOOL_PROGRAM.to_string()];
        for pid in programs {
            let url = ws_url.clone();
            tokio::spawn(async move {
                let sub_req = json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "logsSubscribe",
                    "params": [ { "mentions": [pid] }, { "commitment": "processed" } ]
                });
                match connect_async(&url).await {
                    Ok((mut ws, _resp)) => {
                        if ws.send(Message::text(sub_req.to_string())).await.is_err() { return; }
                        info!(program_id = %RAYDIUM_AMM_V4, "sniper websocket connected (generic)");
                        while let Some(msg) = ws.next().await {
                            match msg {
                                Ok(Message::Text(txt)) => {
                                    if txt.contains("logsNotification") {
                                        // Extract logs array heuristically
                                        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&txt) {
                                            if let Some(arr) = v.pointer("/params/result/value/logs").and_then(|x| x.as_array()) {
                                                let lines: Vec<String> = arr.iter().filter_map(|e| e.as_str().map(|s| s.to_string())).collect();
                                                Self::handle_logs_static(lines).await;
                                            }
                                        }
                                    }
                                }
                                Ok(Message::Binary(_)) => {}
                                Ok(Message::Ping(p)) => { let _ = ws.send(Message::Pong(p)).await; }
                                Ok(Message::Close(_)) => { warn!("ws closed"); break; }
                                Err(e) => { warn!(?e, "ws error"); break; }
                                _ => {}
                            }
                        }
                    }
                    Err(e) => warn!(?e, "failed websocket connect"),
                }
            });
        }
        Ok(())
    }

    #[allow(dead_code)]
    async fn handle_logs_static(logs: Vec<String>) {
        // Placeholder (will be replaced with cfg/rpc richer version in next iteration)
        for l in logs { let lower = l.to_ascii_lowercase(); if (lower.contains("initialize") || lower.contains("init")) && (lower.contains("pool") || lower.contains("whirlpool")) { debug!(line = %l, "candidate pool init log"); } }
    }

    pub async fn run(&self) -> Result<()> {
        self.subscribe_logs().await?;
        info!("sniper engine running (skeleton)");
        // Placeholder periodic task: future spot for initial buys & SL/TP mgmt
        let mut iv = tokio::time::interval(std::time::Duration::from_secs(30));
        loop {
            iv.tick().await;
            debug!(max_buy_sol = self.cfg.max_buy_sol, "sniper heartbeat");
        }
    }

    #[allow(dead_code)]
    fn heuristics_pass(&self, mint: &Pubkey, owner: Option<&Pubkey>, liquidity_sol: Option<f64>, freeze_auth: Option<&Pubkey>, mint_decimals: Option<u8>) -> bool {
        // Config / dynamic sources
        if self.cfg.blacklist_mints.iter().any(|m| m == &mint.to_string()) { return false; }
        if let Some(o) = owner { if self.cfg.blacklist_owners.iter().any(|v| v == &o.to_string()) { return false; } }
        if let Some(min_liq) = self.cfg.min_pool_liquidity_sol { if liquidity_sol.unwrap_or(0.0) < min_liq { return false; } }
        if self.cfg.require_freeze_auth_none.unwrap_or(false) { if freeze_auth.is_some() { return false; } }
        if let Some((lo,hi)) = self.cfg.require_mint_decimals_range { if let Some(d) = mint_decimals { if d < lo || d > hi { return false; } } }
        true
    }
}

#[derive(Debug, Clone)]
pub struct LpLockAssessment {
    pub top1_pct: f64,
    pub top3_pct: f64,
    pub top5_pct: f64,
    pub concentration_ok: bool,
    pub largest_account: Option<String>,
}

impl SniperEngine {
    pub async fn lp_lock_check(&self, mint: &Pubkey) -> Result<Option<LpLockAssessment>> {
        // Only run if any threshold configured
        if self.cfg.lp_top1_max_pct.is_none() && self.cfg.lp_top3_max_pct.is_none() && self.cfg.lp_top5_max_pct.is_none() { return Ok(None); }
        let thr1 = self.cfg.lp_top1_max_pct.unwrap_or(f64::MAX);
        let thr3 = self.cfg.lp_top3_max_pct.unwrap_or(f64::MAX);
        let thr5 = self.cfg.lp_top5_max_pct.unwrap_or(f64::MAX);
        // Fetch mint account
        let mint_acc = match self.rpc.rpc.get_account(mint).await { Ok(a)=>a, Err(e)=> { warn!(?e, "mint account fetch failed"); return Ok(None); } };
    // SPL Mint length heuristic (approx range) & manual decode subset (legacy Token program)
    if mint_acc.data.len() < 70 || mint_acc.data.len() > 90 { return Ok(None); }
    let decimals = mint_acc.data.get(44).cloned().unwrap_or(0);
    let supply_le_bytes = &mint_acc.data[36..44];
    let supply_raw = u64::from_le_bytes(supply_le_bytes.try_into().unwrap());
    let supply_tokens = if decimals == 0 { supply_raw as f64 } else { (supply_raw as f64) / 10f64.powi(decimals as i32) };
    let supply = supply_tokens;
    if supply == 0.0 { return Ok(None); }
    if supply == 0.0 { return Ok(None); }
        // Largest accounts
        let list = match self.rpc.rpc.get_token_largest_accounts(mint).await { Ok(v)=>v, Err(e)=> { warn!(?e, "largest accounts fetch failed"); return Ok(None); } };
        let mut largest_key: Option<String> = None;
        let mut raw_amounts: Vec<f64> = Vec::new();
        for (i,acc) in list.iter().enumerate() {
            // acc.amount.amount raw units string nested path
            let raw_units_str = &acc.amount.amount;
            if let Ok(raw_u128) = raw_units_str.parse::<u128>() {
                let val_tokens = if decimals == 0 { raw_u128 as f64 } else { raw_u128 as f64 / 10f64.powi(decimals as i32) };
                raw_amounts.push(val_tokens);
                if i == 0 { largest_key = Some(acc.address.clone()); }
            }
            if i >= 4 { break; } // we only need top5 for now
        }
        if raw_amounts.is_empty() { return Ok(None); }
        // Sort descending just in case
        raw_amounts.sort_by(|a,b| b.partial_cmp(a).unwrap());
        let top1 = raw_amounts.get(0).copied().unwrap_or(0.0);
        let top3_sum: f64 = raw_amounts.iter().take(3).sum();
        let top5_sum: f64 = raw_amounts.iter().take(5).sum();
        let top1_pct = if supply>0.0 { top1 / supply } else { 0.0 };
        let top3_pct = if supply>0.0 { top3_sum / supply } else { 0.0 };
        let top5_pct = if supply>0.0 { top5_sum / supply } else { 0.0 };
        let concentration_ok = top1_pct <= thr1 && top3_pct <= thr3 && top5_pct <= thr5;
        Ok(Some(LpLockAssessment { top1_pct, top3_pct, top5_pct, concentration_ok, largest_account: largest_key }))
    }
}

pub async fn run_sniper(rpc: Arc<SolanaRpc>, cfg: SniperCfg) -> Result<()> {
    let engine = SniperEngine::new(rpc, cfg);
    engine.run().await
}

impl From<&SniperSettings> for SniperCfg {
    fn from(s: &SniperSettings) -> Self {
        Self {
            max_buy_sol: s.max_buy_sol,
            max_slippage_bps: s.max_slippage_bps,
            blacklist_mints: s.blacklist_mints.clone(),
            blacklist_owners: s.blacklist_owners.clone(),
            min_pool_liquidity_sol: s.min_pool_liquidity_sol,
            require_freeze_auth_none: s.require_freeze_auth_none,
            require_mint_decimals_range: s.require_mint_decimals_range,
            lp_top1_max_pct: s.lp_top1_max_pct,
            lp_top3_max_pct: s.lp_top3_max_pct,
            lp_top5_max_pct: s.lp_top5_max_pct,
        }
    }
}
