//! Meme Coin Sniper Skeleton – subscribes to pool creation logs and applies heuristics.
// Memecoin‑Sniper Skeleton: beobachtet neue Pools/LP‑Creations, filtert Risiken,
// setzt kleine Erstkäufe mit harten Limits (Slippage/Blacklist/Owner/Freeze Auth usw.)
use std::{sync::Arc, collections::HashSet};
use std::str::FromStr;
use anyhow::Result;
use tracing::{info, debug, warn};
use crate::solana::rpc::SolanaRpc;
use crate::config::SniperSettings;
use crate::solana::dex::{raydium::Raydium, orca::Orca, Dex};
use solana_sdk::pubkey::Pubkey;
// (log subscription stub – real PubSub integration to be reintroduced with correct crate paths)
use once_cell::sync::Lazy;
use regex::Regex;

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
    raydium: Option<Arc<Raydium>>,
    orca: Option<Arc<Orca>>,
}

impl SniperEngine {
    pub fn new(rpc: Arc<SolanaRpc>, cfg: SniperCfg, raydium: Option<Arc<Raydium>>, orca: Option<Arc<Orca>>) -> Self { Self { rpc, cfg, raydium, orca } }

    fn clone_for_spawn(&self) -> Self { SniperEngine { rpc: self.rpc.clone(), cfg: self.cfg.clone(), raydium: self.raydium.clone(), orca: self.orca.clone() } }

    fn http_to_ws(url: &str) -> String {
        if url.starts_with("https://") { url.replacen("https://", "wss://", 1) }
        else if url.starts_with("http://") { url.replacen("http://", "ws://", 1) } else { url.to_string() }
    }

    pub async fn subscribe_logs(&self) -> Result<()> {
        let ws_url = Self::http_to_ws(&self.rpc.rpc.url());
        let programs = vec![RAYDIUM_AMM_V4.to_string(), ORCA_WHIRLPOOL_PROGRAM.to_string()];
        for pid in programs {
            let url = ws_url.clone();
            let engine_clone = self.clone_for_spawn();
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
                                                engine_clone.extract_and_evaluate(lines).await;
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
    async fn handle_logs_static(_logs: Vec<String>) {
        // deprecated placeholder
    }

    async fn extract_and_evaluate(&self, logs: Vec<String>) {
        static BASE58_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"[1-9A-HJ-NP-Za-km-z]{32,44}").unwrap());
        for line in logs {
            let lower = line.to_ascii_lowercase();
            if !(lower.contains("init") || lower.contains("initialize")) { continue; }
            if !(lower.contains("pool") || lower.contains("whirlpool")) { continue; }
            debug!(line = %line, "sniper: init-like log");
            let mut seen = std::collections::HashSet::new();
            for m in BASE58_RE.find_iter(&line) {
                let s = m.as_str();
                if !seen.insert(s) { continue; }
                if let Ok(pk) = Pubkey::from_str(s) {
                    // Run LP concentration check (if thresholds configured)
                    match self.lp_lock_check(&pk).await {
                        Ok(Some(assess)) => {
                            if assess.concentration_ok {
                                // Attempt liquidity estimation (Raydium/Orca pool scan) if min_pool_liquidity_sol configured
                                let mut liq_sol: Option<f64> = None;
                                if self.cfg.min_pool_liquidity_sol.is_some() {
                                    match self.estimate_liquidity_index(&pk).await {
                                        Ok(v) => liq_sol = v,
                                        Err(_) => { liq_sol = self.estimate_liquidity_for_mint(&pk).await.ok().flatten(); }
                                    }
                                }
                                info!(mint = %pk, top1 = assess.top1_pct, top3 = assess.top3_pct, top5 = assess.top5_pct, burned = assess.burned_pct, program_locked = assess.program_vault_pct, liq_sol, "sniper: candidate mint passes concentration");
                            } else {
                                debug!(mint = %pk, top1 = assess.top1_pct, top3 = assess.top3_pct, top5 = assess.top5_pct, "sniper: rejected by concentration");
                            }
                        }
                        Ok(None) => { debug!(mint = %pk, "sniper: no lp thresholds configured or insufficient data"); }
                        Err(e) => { debug!(mint = %pk, error = ?e, "sniper: lp check error"); }
                    }
                }
            }
        }
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
    pub burned_pct: f64,
    pub program_vault_pct: f64,
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
        let mut handle_pool = |base: Pubkey, quote: Pubkey, r_base: u128, r_quote: u128| {
            if base == *mint && quote == sol_mint { // mint-SOL
                let sol_res = r_quote as f64 / 1e9; total_sol += sol_res * 2.0; considered += 1; return; }
            if quote == *mint && base == sol_mint { let sol_res = r_base as f64 / 1e9; total_sol += sol_res * 2.0; considered += 1; return; }
            // Stable pairing (USDC/USDT)
            let stable_rate = 100.0; // placeholder USD per SOL
            if (base == *mint && (quote == usdc || quote == usdt)) { let usd = r_quote as f64 / 1e6; total_sol += (usd / stable_rate) * 2.0; considered += 1; }
            else if (quote == *mint && (base == usdc || base == usdt)) { let usd = r_base as f64 / 1e6; total_sol += (usd / stable_rate) * 2.0; considered += 1; }
        };
        if let Some(r) = &self.raydium {
            // Access internal pools via snapshots
            for snap in r.snapshots() { if snap.base_mint == *mint || snap.quote_mint == *mint { handle_pool(snap.base_mint, snap.quote_mint, snap.reserve_base, snap.reserve_quote); } }
        }
    if let Some(_o) = &self.orca { /* TODO: expose Orca pool snapshots for index-based liquidity */ }
        
        if considered == 0 { return Ok(None); }
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
        if *mint == sol_mint || *mint == usdc || *mint == usdt { return Ok(None); }
        // We don't maintain an indexed map from mint->pool yet, so attempt lightweight scan of recent accounts: skip (needs future caching)
        // For now: try fetch largest accounts and look for vault owners referencing Raydium or Orca program (heuristic minimal viable solution).
        let largest = self.rpc.rpc.get_token_largest_accounts(mint).await.ok();
        let Some(list) = largest else { return Ok(None); };
        let mut candidate_vaults: Vec<Pubkey> = Vec::new();
        for acc in list.iter().take(8) {
            if let Ok(pk) = Pubkey::from_str(&acc.address) { candidate_vaults.push(pk); }
        }
        if candidate_vaults.is_empty() { return Ok(None); }
        let vault_infos = self.rpc.rpc.get_multiple_accounts(&candidate_vaults).await.ok();
        let Some(v_infos) = vault_infos else { return Ok(None); };
        // Identify any vault that looks like a Raydium / Orca pool vault by inspecting its owner field if present
        // SPL token account layout: mint(0..32) owner(32..64) amount(64..72)
        let mut est_sol_value = 0f64;
        for opt in v_infos.iter() {
            let Some(acc) = opt else { continue; };
            if acc.data.len() < 72 { continue; }
            let mint_bytes: [u8;32] = acc.data[0..32].try_into().unwrap();
            let owner_bytes: [u8;32] = acc.data[32..64].try_into().unwrap();
            let reserve_u64 = u64::from_le_bytes(acc.data[64..72].try_into().unwrap());
            let inner_mint = Pubkey::new_from_array(mint_bytes);
            let owner_pk = Pubkey::new_from_array(owner_bytes);
            // Heuristic: we only care if this token account's mint is either candidate mint or SOL/stable, and owner is a known AMM program (Raydium / Orca pool address not directly program id, so skip deep validation).
            if inner_mint != *mint && inner_mint != sol_mint && inner_mint != usdc && inner_mint != usdt { continue; }
            // Convert amount to SOL value: if paired with SOL directly and inner_mint == SOL -> treat reserve as SOL.
            if inner_mint == sol_mint { est_sol_value += reserve_u64 as f64 / 1e9; }
            else if inner_mint == usdc || inner_mint == usdt {
                // Placeholder USD->SOL conversion: assume 1 SOL = 100 USD (later replace with oracle)
                let usd = reserve_u64 as f64 / 1e6; // USDC/USDT decimals 6
                est_sol_value += usd / 100.0;
            } else if inner_mint == *mint {
                // Need other side value to price; skip as we can't compute without reserves pair.
            }
            // Owner heuristic could refine classification (TODO)
            let _ = owner_pk; // silence unused for now
        }
        if est_sol_value == 0.0 { return Ok(None); }
        Ok(Some(est_sol_value))
    }
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
        if list.is_empty() { return Ok(None); }
        // Collect top5 addresses & amounts raw
        let mut holder_accts: Vec<(String, u128)> = Vec::new();
        for (i, acc) in list.iter().enumerate() { if i >= 5 { break; } if let Ok(raw_u128) = acc.amount.amount.parse::<u128>() { holder_accts.push((acc.address.clone(), raw_u128)); } }
        if holder_accts.is_empty() { return Ok(None); }
        let largest_key = Some(holder_accts[0].0.clone());
        // Fetch token account data for classification (burn / program-vault)
        let addrs: Vec<Pubkey> = holder_accts.iter().filter_map(|(a,_)| Pubkey::from_str(a).ok()).collect();
        let acct_infos = match self.rpc.rpc.get_multiple_accounts(&addrs).await { Ok(v)=>v, Err(e)=> { warn!(?e, "largest token accounts fetch failed"); Vec::new() } };
        // Known constants
        let incinerator = Pubkey::from_str("1nc1nerator11111111111111111111111111111111").unwrap();
        let known_programs: Vec<Pubkey> = vec![
            Pubkey::from_str(RAYDIUM_AMM_V4).unwrap_or(Pubkey::default()),
            Pubkey::from_str(ORCA_WHIRLPOOL_PROGRAM).unwrap_or(Pubkey::default()),
        ];
        #[derive(PartialEq)] enum Class { Burn, ProgramVault, Regular }
        struct HolderRec { amt_tokens: f64, class: Class }
        let mut records: Vec<HolderRec> = Vec::new();
        let mut burned_total = 0f64;
        let mut program_locked_total = 0f64;
        for (idx, (addr_str, raw_amount)) in holder_accts.iter().enumerate() {
            let mut class = Class::Regular;
            let amt_tokens = if decimals == 0 { *raw_amount as f64 } else { *raw_amount as f64 / 10f64.powi(decimals as i32) };
            if let Ok(acc_pk) = Pubkey::from_str(addr_str) {
                if acc_pk == incinerator { class = Class::Burn; }
            }
            if class == Class::Regular {
                if let Some(Some(acc_info)) = acct_infos.get(idx).map(|o| o.as_ref()) {
                    // Token account length heuristic: >= 80 for owner field
                    if acc_info.data.len() >= 64 {
                        let owner_slice = &acc_info.data[32..64];
                        let owner_auth = Pubkey::new_from_array(owner_slice.try_into().unwrap());
                        if owner_auth == incinerator { class = Class::Burn; }
                        else if known_programs.iter().any(|p| *p == owner_auth) { class = Class::ProgramVault; }
                        else {
                            // Fallback: fetch owner auth executable bit via separate account info if present in batch (future optimization)
                        }
                    }
                }
            }
            match class { Class::Burn => burned_total += amt_tokens, Class::ProgramVault => program_locked_total += amt_tokens, Class::Regular => {} }
            records.push(HolderRec { amt_tokens, class });
        }
        let effective_supply = (supply - burned_total - program_locked_total).max(0.0);
        if effective_supply <= 0.0 { return Ok(None); }
        // Compute concentration using only regular holders relative to effective supply
        let mut regular_amounts: Vec<f64> = records.iter().filter(|r| r.class == Class::Regular).map(|r| r.amt_tokens).collect();
        if regular_amounts.is_empty() { return Ok(None); }
        regular_amounts.sort_by(|a,b| b.partial_cmp(a).unwrap());
        let top1 = regular_amounts.get(0).copied().unwrap_or(0.0);
        let top3_sum: f64 = regular_amounts.iter().take(3).sum();
        let top5_sum: f64 = regular_amounts.iter().take(5).sum();
        let top1_pct = if effective_supply>0.0 { top1 / effective_supply } else { 0.0 };
        let top3_pct = if effective_supply>0.0 { top3_sum / effective_supply } else { 0.0 };
        let top5_pct = if effective_supply>0.0 { top5_sum / effective_supply } else { 0.0 };
        let concentration_ok = top1_pct <= thr1 && top3_pct <= thr3 && top5_pct <= thr5;
        let burned_pct = if supply>0.0 { burned_total / supply } else { 0.0 };
        let program_vault_pct = if supply>0.0 { program_locked_total / supply } else { 0.0 };
        Ok(Some(LpLockAssessment { top1_pct, top3_pct, top5_pct, concentration_ok, largest_account: largest_key, burned_pct, program_vault_pct }))
    }
}

pub async fn run_sniper(rpc: Arc<SolanaRpc>, cfg: SniperCfg, raydium: Option<Arc<Raydium>>, orca: Option<Arc<Orca>>) -> Result<()> {
    let engine = SniperEngine::new(rpc, cfg, raydium, orca);
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
