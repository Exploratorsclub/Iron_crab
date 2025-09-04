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
use crate::wallet::Treasury;
use solana_sdk::{transaction::Transaction, hash::Hash};
use solana_sdk::pubkey::Pubkey;
// (log subscription stub – real PubSub integration to be reintroduced with correct crate paths)
use once_cell::sync::Lazy;
use regex::Regex;

use crate::solana::dex::raydium::RAYDIUM_AMM_V4;
use crate::solana::dex::orca::ORCA_WHIRLPOOL_PROGRAM;
use futures::{StreamExt, SinkExt};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use std::time::{Duration, Instant};
use std::fs::OpenOptions;
use std::io::Write;
use chrono::Utc as ChronoUtc;
use once_cell::sync::Lazy as OnceLazy;
use parking_lot::Mutex;
use crate::metrics::{
    TRADES_EXECUTED_TOTAL,
    TRADES_FAILED_TOTAL,
    OPEN_POSITIONS_GAUGE,
    DAILY_REALIZED_PNL_SOL_MICRO,
    LIQUIDITY_ESTIMATE_SOL_MICRO,
    RPC_ERRORS_TOTAL,
    record_swap_latency,
    record_shortfall,
    record_network_fee,
    record_trade_return,
    RPC_RETRY_ATTEMPTS_TOTAL,
    WS_RECONNECTS_TOTAL,
    PENDING_RECONCILIATIONS_TOTAL,
    PENDING_FAILED_TOTAL,
    PROTOCOL_FEE_TOKENS_TOTAL,
    PROTOCOL_FEE_SOL_MICRO_TOTAL,
};
use serde::{Serialize, Deserialize};
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
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Position {
    entry_price_sol: f64,
    amount_tokens: f64,
    invested_sol: f64,
    token_decimals: u8,
    last_unrealized_pnl_sol: f64,
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
    open: std::collections::HashMap<Pubkey, Position>,
    realized_pnl_sol: f64,
    realized_loss_today_sol: f64,
    current_day: u32,
    #[serde(skip)]
    pending: std::collections::HashMap<Pubkey, PendingTrade>,
    #[serde(skip)]
    cooldown_until: std::collections::HashMap<Pubkey, i64>,
    recent_realized: Vec<f64>,
    last_sharpe: f64,
}

impl Default for RiskState {
    fn default() -> Self {
    Self { open: Default::default(), realized_pnl_sol: 0.0, realized_loss_today_sol: 0.0, current_day: 0, pending: Default::default(), cooldown_until: Default::default(), recent_realized: Vec::new(), last_sharpe: 0.0 }
    }
}

pub struct SniperEngine {
    pub rpc: Arc<SolanaRpc>,
    cfg: parking_lot::RwLock<SniperCfg>,
    raydium: Option<Arc<Raydium>>,
    orca: Option<Arc<Orca>>,
    purchased: parking_lot::RwLock<HashSet<Pubkey>>, // track already bought mints (avoid double buy)
    treasury: Arc<Treasury>,
    risk: parking_lot::RwLock<RiskState>,
}

impl SniperEngine {
    async fn rpc_retry_tx(&self, tx: &Transaction, max_attempts: u32) -> Result<solana_sdk::signature::Signature> {
        let mut attempt = 0;
        loop {
            match self.rpc.rpc.send_and_confirm_transaction(tx).await {
                Ok(sig) => return Ok(sig),
                Err(e) => {
                    attempt += 1;
                    RPC_RETRY_ATTEMPTS_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if attempt >= max_attempts { return Err(e.into()); }
                    let delay_ms = (2u64.pow(attempt.min(5)) * 200).min(5_000);
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                }
            }
        }
    }
    fn effective_max_buy_sol(&self) -> f64 {
        let cfg = self.cfg.read().clone();
        let rs = self.risk.read();
        if let (Some(limit), Some(start), Some(max_red)) = (cfg.daily_loss_limit_sol, cfg.drawdown_scale_start, cfg.drawdown_max_reduction) {
            if limit > 0.0 && start < 1.0 && max_red > 0.0 {
                let ratio = (rs.realized_loss_today_sol / limit).clamp(0.0, 1.0);
                if ratio <= start { return cfg.max_buy_sol; }
                let frac = ((ratio - start) / (1.0 - start)).clamp(0.0, 1.0);
                let reduction = max_red.clamp(0.0,1.0) * frac;
                return cfg.max_buy_sol * (1.0 - reduction);
            }
        }
        cfg.max_buy_sol
    }

    fn mark_cooldown(&self, mint: Pubkey) {
        if let Some(secs) = self.cfg.read().stop_loss_cooldown_secs { if secs>0 { let until = chrono::Utc::now().timestamp() + secs as i64; let mut rs = self.risk.write(); rs.cooldown_until.insert(mint, until); } }
    }
    async fn total_sol_balance(&self) -> Result<f64> {
        // Native SOL lamports
    let owner = self.treasury.pubkey();
    let native_lamports = match self.rpc.rpc.get_account(&owner).await {
            Ok(acc) => acc.lamports,
            Err(e) => { RPC_ERRORS_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed); return Err(e.into()); }
        } as u128;
        // WSOL ATA amount (if exists)
    let wsol_mint_prog = spl_token::native_mint::id();
    let wsol_mint_sdk = solana_sdk::pubkey::Pubkey::new_from_array(wsol_mint_prog.to_bytes());
    let (wsol_ata, _prog) = match self.treasury.ata_address(&self.rpc, &owner, &wsol_mint_sdk).await {
            Ok(v) => v,
            Err(_) => { return Ok(native_lamports as f64 / 1e9); }
        };
        let wsol_amount = match self.rpc.rpc.get_account(&wsol_ata).await {
            Ok(acc) => {
                if acc.data.len() >= 72 { u64::from_le_bytes(acc.data[64..72].try_into().unwrap()) as u128 } else { 0 }
            }
            Err(_) => 0,
        };
        Ok((native_lamports + wsol_amount) as f64 / 1e9)
    }
    pub fn new(rpc: Arc<SolanaRpc>, cfg: SniperCfg, raydium: Option<Arc<Raydium>>, orca: Option<Arc<Orca>>, treasury: Arc<Treasury>) -> Self { Self { rpc, cfg: parking_lot::RwLock::new(cfg), raydium, orca, purchased: parking_lot::RwLock::new(HashSet::new()), treasury, risk: parking_lot::RwLock::new(RiskState::default()) } }

    fn clone_for_spawn(&self) -> Self { SniperEngine { rpc: self.rpc.clone(), cfg: parking_lot::RwLock::new(self.cfg.read().clone()), raydium: self.raydium.clone(), orca: self.orca.clone(), purchased: parking_lot::RwLock::new(HashSet::new()), treasury: self.treasury.clone(), risk: parking_lot::RwLock::new(RiskState::default()) } }

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
                let mut attempt: u32 = 0;
                loop {
                    match connect_async(&url).await {
                        Ok((mut ws, _resp)) => {
                            attempt = 0;
                            if ws.send(Message::text(sub_req.to_string())).await.is_err() { return; }
                            info!(program_id = %RAYDIUM_AMM_V4, "sniper websocket connected (generic)");
                            while let Some(msg) = ws.next().await {
                                match msg {
                                    Ok(Message::Text(txt)) => {
                                        if txt.contains("logsNotification") {
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
                        Err(e) => { warn!(?e, "failed websocket connect"); }
                    }
                    WS_RECONNECTS_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    attempt += 1;
                    let backoff_ms = (2u64.pow(attempt.min(6)) * 250).min(10_000);
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
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
                                if self.cfg.read().min_pool_liquidity_sol.is_some() {
                                    match self.estimate_liquidity_index(&pk).await {
                                        Ok(v) => liq_sol = v,
                                        Err(_) => { liq_sol = self.estimate_liquidity_for_mint(&pk).await.ok().flatten(); }
                                    }
                                }
                                info!(mint = %pk, top1 = assess.top1_pct, top3 = assess.top3_pct, top5 = assess.top5_pct, burned = assess.burned_pct, program_locked = assess.program_vault_pct, liq_sol, "sniper: candidate mint passes concentration");
                                // Gate by liquidity threshold if configured
                                let liq_ok = self.cfg.read().min_pool_liquidity_sol.map(|thr| liq_sol.unwrap_or(0.0) >= thr).unwrap_or(true);
                                if liq_ok {
                                    // Risk: Notional cap & daily loss limit pre-check
                                    let base_buy = self.effective_max_buy_sol();
                                    if !self.can_open_position_for(&pk, base_buy) {
                                        debug!(mint=%pk, "sniper: risk gate blocked new position (notional/daily loss)");
                                        continue;
                                    }
                                    // Avoid duplicate buys
                                    if !self.purchased.read().contains(&pk) {
                                        if let Err(e) = self.attempt_initial_buy(&pk, liq_sol).await { warn!(mint = %pk, error = ?e, "sniper: initial buy failed"); }
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
                        Ok(None) => { debug!(mint = %pk, "sniper: no lp thresholds configured or insufficient data"); }
                        Err(e) => { debug!(mint = %pk, error = ?e, "sniper: lp check error"); }
                    }
                }
            }
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
        let _dest_ata = match self.treasury.ensure_ata(&self.rpc, &owner_sdk, &mint_sdk).await { Ok(a)=>a, Err(e)=> { debug!(?e, mint=%mint, "ensure dest ATA fail"); return Ok(()); } };
        // Wrap SOL into WSOL ATA (Raydium expects token account)
    let (wsol_ata_sdk, _wrap_sig) = match self.treasury.wrap_sol(&self.rpc, lamports_in).await { Ok(v)=>v, Err(e)=> { debug!(?e, lamports_in, "wrap_sol failed"); return Ok(()); } };
    // Build auto plan for min_out computation & pool selection (Raydium)
    let msb = self.cfg.read().max_slippage_bps;
    let plan_opt = if let Some(r) = &ray { r.build_swap_plan_auto(&sol_mint.to_string(), &mint.to_string(), lamports_in, msb)? } else { None };
    if plan_opt.is_none() && orca.is_none() { debug!(mint=%mint, "no raydium or orca route"); return Ok(()); }
    let plan_meta = plan_opt;
    let mut used_raydium = false;
    if let Some(ref pm) = plan_meta { if pm.pool.is_some() { used_raydium = true; } }
    // Source WSOL ATA (after wrap) & destination token ATA
    let _wsol_ata = wsol_ata_sdk; // already ensured via wrap
    let (_dest_ata, _token_prog) = self.treasury.ata_address(&self.rpc, &owner_sdk, &mint_sdk).await?;
    // Derive Serum market related accounts from snapshot (already stored inside Raydium SimplePool)
    // We still need serum bids/asks/event/base_vault/quote_vault; currently not exposed -> placeholder fetch skipped (in full impl these would be read separately)
    // For now abort if we cannot assemble full accounts; fallback to pseudo plan already logged earlier.
    // (Future: extend Raydium snapshot to include serum market detail accounts.)
        // Try to upgrade to full swap instruction if serum accounts present in snapshot
        let mut final_ixs: Vec<solana_sdk::instruction::Instruction> = plan_meta.as_ref().map(|p| p.ixs.clone()).unwrap_or_default();
        if used_raydium {
            if let Some(r) = &ray {
                if let Some(pool_addr) = plan_meta.as_ref().and_then(|p| p.pool) {
                    if let Some(snap) = r.snapshots().into_iter().find(|s| s.address == pool_addr) {
                        if let (Some(_open_orders), Some(_market_id)) = (snap.open_orders, snap.market_id) {
                            if let (Some(bids), Some(asks), Some(event_q), Some(base_vault), Some(quote_vault), Some(_serum_vs)) = (snap.serum_bids, snap.serum_asks, snap.serum_event_queue, snap.serum_base_vault, snap.serum_quote_vault, snap.serum_vault_signer) {
                                let token_prog = spl_token::id();
                                let rent_sysvar = solana_sdk::sysvar::rent::id();
                                use crate::solana::dex::raydium::SerumMarketAccounts;
                                let serum_accounts = SerumMarketAccounts { bids, asks, event_queue: event_q, base_vault, quote_vault };
                                if let Ok(ray_prog) = Pubkey::from_str(RAYDIUM_AMM_V4) {
                                    let auth_pk = Pubkey::new_from_array(self.treasury.pubkey().to_bytes());
                                    let user_source = Pubkey::new_from_array(_wsol_ata.to_bytes());
                                    let user_dest = Pubkey::new_from_array(_dest_ata.to_bytes());
                                    let token_prog_pk = Pubkey::new_from_array(token_prog.to_bytes());
                                    let rent_pk = Pubkey::new_from_array(rent_sysvar.to_bytes());
                                    if let Some(ref pm) = plan_meta {
                                        if let Ok(full_ix) = r.build_swap_instruction(pool_addr, sol_mint, *mint, lamports_in, pm.min_out, auth_pk, user_source, user_dest, ray_prog, token_prog_pk, rent_pk, serum_accounts, snap.target_orders) {
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
    let bh: Hash = match self.rpc.rpc.get_latest_blockhash().await { Ok(h)=>h, Err(e)=> { RPC_ERRORS_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed); return Err(e.into()); } };
        let tx_ixs: Vec<solana_sdk::instruction::Instruction> = if used_raydium { final_ixs } else {
            // Orca path: build swap ix directly
            if let Some(o) = &orca {
                let wsol_mint_prog = spl_token::native_mint::id();
                let wsol_mint_sdk = Pubkey::new_from_array(wsol_mint_prog.to_bytes());
                o.set_user_authority(Pubkey::new_from_array(self.treasury.pubkey().to_bytes()));
                if let Ok((wsol_ata,_prog)) = self.treasury.ata_address(&self.rpc, &self.treasury.pubkey(), &wsol_mint_sdk).await {
                    o.set_user_token_account(wsol_mint_sdk, wsol_ata);
                }
                if let Ok((dst_ata,_prog2)) = self.treasury.ata_address(&self.rpc, &self.treasury.pubkey(), &mint_sdk).await {
                    o.set_user_token_account(mint_sdk, dst_ata);
                }
                // Derive min_out with slippage tolerance via quote
                let mut min_out_orca = 1u64; // fallback
                if let Ok(Some(q)) = o.quote_exact_in(&sol_mint.to_string(), &mint.to_string(), lamports_in).await {
                    let slip = self.cfg.read().max_slippage_bps as u128;
                    min_out_orca = ((q.amount_out as u128) * (10_000 - slip) / 10_000) as u64;
                    if min_out_orca == 0 { min_out_orca = 1; }
                }
                match o.build_swap_ix(&sol_mint.to_string(), &mint.to_string(), lamports_in, min_out_orca) {
                    Ok(ixs) => ixs,
                    Err(e) => { debug!(?e, mint=%mint, "orca build_swap_ix failed"); Vec::new() }
                }
            } else { Vec::new() }
        };
        if tx_ixs.is_empty() { debug!(mint=%mint, "no swap instructions built"); return Ok(()); }
    // Prepare message for fee estimate before signing
    let message = solana_sdk::message::Message::new(&tx_ixs, Some(&self.treasury.pubkey()));
    let fee_estimate = self.rpc.rpc.get_fee_for_message(&message).await.unwrap_or(0);
    let tx = Transaction::new_signed_with_payer(&tx_ixs, Some(&self.treasury.pubkey()), &[self.treasury.keypair.as_ref()], bh);
    let sent_at = Instant::now();
    match self.rpc_retry_tx(&tx, 3).await {
            Ok(sig) => {
        let dur = sent_at.elapsed();
        record_swap_latency(dur.as_nanos() as u64);
                TRADES_EXECUTED_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if used_raydium {
                    let pm = plan_meta.as_ref().unwrap();
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
                            rs.pending.insert(*mint, PendingTrade { expected_out_tokens: pm.expected_out, dex: "RAYDIUM".into(), sig: sig.to_string(), lamports_in, network_fee_lamports: fee_estimate, ts: ChronoUtc::now().timestamp(), fee_bps: pm.fee_bps });
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
                    Ok(unwrap_sig) => info!(mint=%mint, unwrap_sig=%unwrap_sig, "sniper: wsol unwrapped post-trade"),
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
        // Placeholder periodic task: future spot for initial buys & SL/TP mgmt
        let mut iv = tokio::time::interval(Duration::from_secs(15));
        // Autosave task
        let autosave_secs: u64 = std::env::var("IRONCRAB_RISK_AUTOSAVE_SECS").ok().and_then(|v| v.parse().ok()).unwrap_or(60);
        if autosave_secs > 0 {
            let engine_clone = self.clone_for_spawn();
            tokio::spawn(async move {
                let mut saver = tokio::time::interval(Duration::from_secs(autosave_secs));
                loop { saver.tick().await; engine_clone.persist_risk_state(); }
            });
        }
        // Hot reload task (env IRONCRAB_SNIPER_RELOAD_PATH)
        if let Some(path) = std::env::var_os("IRONCRAB_SNIPER_RELOAD_PATH") {
            let path_buf = std::path::PathBuf::from(path);
            let engine_clone = self.clone_for_spawn();
            tokio::spawn(async move {
                let mut ivr = tokio::time::interval(Duration::from_secs(30));
                let mut last_mod = std::time::SystemTime::UNIX_EPOCH;
                loop {
                    ivr.tick().await;
                    if let Ok(meta) = std::fs::metadata(&path_buf) {
                        if let Ok(modified) = meta.modified() {
                            if modified > last_mod {
                                last_mod = modified;
                                if let Ok(txt) = std::fs::read_to_string(&path_buf) {
                                    if let Ok(root) = toml::from_str::<crate::config::Config>(&txt) {
                                        if let Some(sn) = root.sniper.clone() {
                                            let new_cfg: SniperCfg = (&sn).into();
                                            *engine_clone.cfg.write() = new_cfg;
                                            tracing::info!("sniper hot reload applied");
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            });
        }
        loop {
            iv.tick().await;
            let mb = self.cfg.read().max_buy_sol;
            debug!(max_buy_sol = mb, "sniper heartbeat");
            // Evaluate open positions for SL/TP exits
            if let Err(e) = self.evaluate_positions().await { debug!(?e, "risk evaluation error"); }
            // Pending trade TTL cleanup
            if let Some(ttl) = self.cfg.read().pending_trade_ttl_secs { if ttl > 0 { self.cleanup_stale_pending(ttl); } }
            // Reconciliation pass (half of TTL age threshold)
            if let Some(ttl) = self.cfg.read().pending_trade_ttl_secs { if ttl >= 20 { // minimal threshold
                let cutoff = (ttl as i64) / 2; // drop if older than half TTL w/o status
                let engine_clone = self.clone_for_spawn();
                tokio::spawn(async move { engine_clone.reconcile_pending(cutoff).await; });
            } }
        }
    }

    #[allow(dead_code)]
    fn heuristics_pass(&self, mint: &Pubkey, owner: Option<&Pubkey>, liquidity_sol: Option<f64>, freeze_auth: Option<&Pubkey>, mint_decimals: Option<u8>) -> bool {
        // Config / dynamic sources
    if self.cfg.read().blacklist_mints.iter().any(|m| m == &mint.to_string()) { return false; }
    if let Some(o) = owner { if self.cfg.read().blacklist_owners.iter().any(|v| v == &o.to_string()) { return false; } }
    if let Some(min_liq) = self.cfg.read().min_pool_liquidity_sol { if liquidity_sol.unwrap_or(0.0) < min_liq { return false; } }
    if self.cfg.read().require_freeze_auth_none.unwrap_or(false) { if freeze_auth.is_some() { return false; } }
    if let Some((lo,hi)) = self.cfg.read().require_mint_decimals_range { if let Some(d) = mint_decimals { if d < lo || d > hi { return false; } } }
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
    if let Some(cap) = cfg_r.max_position_sol { if planned_sol > cap { return false; } }
    if let Some(daily) = cfg_r.daily_loss_limit_sol { if rs.realized_loss_today_sol >= daily { return false; } }
    if let Some(mop) = cfg_r.max_open_positions { if rs.open.len() >= mop { return false; } }
    if let Some(until) = rs.cooldown_until.get(mint) { if *until > chrono::Utc::now().timestamp() { return false; } }
        true
    }

    fn record_fill_placeholder(&self, mint: Pubkey, invested_sol: f64) {
        self.risk_reset_if_needed();
        let mut rs = self.risk.write();
        rs.open.entry(mint).or_insert(Position { entry_price_sol: 0.0, amount_tokens: 0.0, invested_sol, token_decimals: 0, last_unrealized_pnl_sol: 0.0 });
    OPEN_POSITIONS_GAUGE.store(rs.open.len() as u64, std::sync::atomic::Ordering::Relaxed);
    }

    fn append_trade_record(&self, line: &str, include_header: bool) {
        static TRADE_LOG_LOCK: OnceLazy<Mutex<()>> = OnceLazy::new(|| Mutex::new(()));
        let _g = TRADE_LOG_LOCK.lock();
        let dir_name = std::env::var("IRONCRAB_TRADE_LOG_DIR").unwrap_or_else(|_| "trade_logs".to_string());
        let dir = std::path::Path::new(&dir_name);
        if !dir.exists() { let _ = std::fs::create_dir_all(dir); }
        let date = ChronoUtc::now().format("%Y%m%d");
        let file_path = dir.join(format!("trades-{}.csv", date));
        let new_file = !file_path.exists();
        if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(&file_path) {
            if new_file && include_header {
                let _ = writeln!(f, "timestamp_utc,side,mint,dex,signature,lamports_in,lamports_out,tokens_in,tokens_out,expected_tokens_out,expected_sol_out,shortfall_tokens,shortfall_sol,network_fee_lamports,realized_pnl_sol,notes");
            }
            let _ = writeln!(f, "{}", line);
        }
    }

    async fn finalize_fill(&self, mint: Pubkey) {
        let owner = self.treasury.pubkey();
        let mint_sdk = solana_sdk::pubkey::Pubkey::new_from_array(mint.to_bytes());
        let ata = match self.treasury.ata_address(&self.rpc, &owner, &mint_sdk).await { Ok(v)=>v.0, Err(_)=> return };
        let decimals = if let Ok(mint_acc) = self.rpc.rpc.get_account(&mint).await { if mint_acc.data.len()>44 { mint_acc.data[44] } else {0} } else {0};
        let acc_opt = self.rpc.rpc.get_account(&ata).await.ok();
        if let Some(acc) = acc_opt { if acc.data.len()>=72 {
            let raw = u64::from_le_bytes(acc.data[64..72].try_into().unwrap());
            let amt = if decimals==0 { raw as f64 } else { raw as f64 / 10f64.powi(decimals as i32) };
            if amt <= 0.0 { return; }
            // Take snapshot of pending & position without holding lock across RPC
            let (pend_opt, entry_price_existing) = {
                let mut rs = self.risk.write();
                let pend = rs.pending.remove(&mint);
                if let Some(pos) = rs.open.get_mut(&mint) { if pos.entry_price_sol==0.0 { pos.amount_tokens = amt; pos.token_decimals = decimals; pos.entry_price_sol = pos.invested_sol / amt.max(1e-9); } }
                (pend, rs.open.get(&mint).map(|p| p.entry_price_sol).unwrap_or(0.0))
            };
            if let Some(pend) = pend_opt {
                // Fetch meta outside lock
                let mut exact_network_fee = pend.network_fee_lamports;
                if let Ok(sig_obj) = solana_sdk::signature::Signature::from_str(&pend.sig) {
                    use solana_transaction_status::UiTransactionEncoding;
                    use solana_client::rpc_config::RpcTransactionConfig;
                    let cfg = RpcTransactionConfig { encoding: Some(UiTransactionEncoding::JsonParsed), commitment: None, max_supported_transaction_version: None };
                    if let Ok(tx_opt) = self.rpc.rpc.get_transaction_with_config(&sig_obj, cfg).await { if let Some(meta) = tx_opt.transaction.meta { exact_network_fee = meta.fee; } }
                }
                // Meta-based token delta extraction for accuracy
                let scale = 10f64.powi(decimals as i32);
                let actual_raw = (amt * scale).round() as u64; // fallback
                let meta_shortfall_tokens: Option<u64> = None; // placeholder (meta parsing TODO)
                // NOTE: OptionSerializer wrapper complicates direct access; keep fallback for now.
                let expected_raw = pend.expected_out_tokens;
                let shortfall = meta_shortfall_tokens.unwrap_or_else(|| expected_raw.saturating_sub(actual_raw));
                let shortfall_ui = shortfall as f64 / scale;
                let shortfall_sol = shortfall_ui * entry_price_existing;
                // Compute protocol fee tokens exactly from fee_bps if available: expected_out already fee-deducted
                let fee_tokens = if pend.fee_bps > 0 && pend.fee_bps < 5000 {
                    // expected_out = no_fee_out * (1 - fee_bps/10_000) approximately; invert
                    let no_fee_out = ((expected_raw as u128) * 10_000u128 / (10_000u128 - pend.fee_bps as u128)) as u64;
                    no_fee_out.saturating_sub(expected_raw)
                } else { 0 };
                if fee_tokens > 0 {
                    PROTOCOL_FEE_TOKENS_TOTAL.fetch_add(fee_tokens as u64, std::sync::atomic::Ordering::Relaxed);
                    let fee_ui = fee_tokens as f64 / scale;
                    let fee_sol = fee_ui * entry_price_existing;
                    PROTOCOL_FEE_SOL_MICRO_TOTAL.fetch_add((fee_sol * 1_000_000.0) as u64, std::sync::atomic::Ordering::Relaxed);
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
        }}
    }

    async fn evaluate_positions(&self) -> Result<()> {
        // Skip if no thresholds configured
    { let r = self.cfg.read(); if r.stop_loss_bps.is_none() && r.take_profit_bps.is_none() { return Ok(()); } }
    let (stop_bps, tp_bps) = { let r = self.cfg.read(); (r.stop_loss_bps.unwrap_or(u32::MAX), r.take_profit_bps.unwrap_or(u32::MAX)) };
        let sol_mint = Pubkey::from_str("So11111111111111111111111111111111111111112").unwrap();
        let positions: Vec<(Pubkey, Position)> = {
            let rs = self.risk.read();
            rs.open.iter().map(|(k,v)| (*k, v.clone())).collect()
        };
        if positions.is_empty() { return Ok(()); }
        for (mint, pos) in positions {
            // Quote exit value (prefer Raydium then Orca)
            let mut quote_out: Option<u64> = None;
            if let Some(r) = &self.raydium {
                if let Ok(Some(q)) = r.quote_exact_in(&mint.to_string(), &sol_mint.to_string(), pos.amount_tokens as u64).await { quote_out = Some(q.amount_out); } else { RPC_ERRORS_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed); }
            }
            if quote_out.is_none() {
                if let Some(o) = &self.orca { if let Ok(Some(q)) = o.quote_exact_in(&mint.to_string(), &sol_mint.to_string(), pos.amount_tokens as u64).await { quote_out = Some(q.amount_out); } else { RPC_ERRORS_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed); } }
            }
            let Some(out_lamports) = quote_out else { continue; };
            let price_now = if pos.amount_tokens>0.0 { (out_lamports as f64 / 1e9) / pos.amount_tokens } else { 0.0 };
            let pnl_pct = if pos.entry_price_sol > 0.0 { (price_now - pos.entry_price_sol) / pos.entry_price_sol } else { 0.0 };
            let pnl_bps = (pnl_pct * 10_000.0) as i64;
            {
                let mut rs = self.risk.write();
                if let Some(p) = rs.open.get_mut(&mint) { p.last_unrealized_pnl_sol = (out_lamports as f64 / 1e9) - p.invested_sol; }
            }
            let stop_trigger = pnl_bps <= -(stop_bps as i64);
            let tp_trigger = pnl_bps >= tp_bps as i64;
            if stop_trigger || tp_trigger {
                debug!(mint=%mint, pnl_bps, stop_trigger, tp_trigger, "exit trigger");
                let is_stop = stop_trigger;
                if let Err(e) = self.attempt_exit(&mint, pos.amount_tokens as u64).await { warn!(?e, mint=%mint, "exit tx failed"); }
                else if is_stop { self.mark_cooldown(mint); }
            }
        }
        Ok(())
    }

    async fn attempt_exit(&self, mint: &Pubkey, amount_tokens: u64) -> Result<()> {
        let sol_mint = Pubkey::from_str("So11111111111111111111111111111111111111112").unwrap();
        // Determine actual token balance (ATA) to avoid over-selling
        let owner_sdk = self.treasury.pubkey();
        let mint_sdk = solana_sdk::pubkey::Pubkey::new_from_array(mint.to_bytes());
        let (ata,_prog) = match self.treasury.ata_address(&self.rpc, &owner_sdk, &mint_sdk).await { Ok(v)=>v, Err(_)=> return Ok(()) };
        let bal_tokens = if let Ok(acc) = self.rpc.rpc.get_account(&ata).await { if acc.data.len()>=72 { u64::from_le_bytes(acc.data[64..72].try_into().unwrap()) } else { amount_tokens } } else { amount_tokens };
        if bal_tokens == 0 { return Ok(()); }
    // Raydium plan token->SOL
        let mut used_raydium = false;
    let msb2 = self.cfg.read().max_slippage_bps;
    let ray_plan = if let Some(r) = &self.raydium { r.build_swap_plan_auto(&mint.to_string(), &sol_mint.to_string(), bal_tokens, msb2).ok() } else { None };
    if let Some(ref p) = ray_plan { if p.as_ref().map(|rp| rp.pool.is_some()).unwrap_or(false) { used_raydium = true; } }
    let bh: Hash = match self.rpc.rpc.get_latest_blockhash().await { Ok(h)=>h, Err(e)=> { RPC_ERRORS_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed); return Err(e.into()); } };
    let tx_ixs: Vec<solana_sdk::instruction::Instruction> = if used_raydium { ray_plan.as_ref().unwrap().as_ref().unwrap().ixs.clone() } else {
            if let Some(o) = &self.orca {
                // Register accounts (source token -> WSOL dest path requires wrap? Actually we receive WSOL then unwrap handled below.)
                o.set_user_authority(Pubkey::new_from_array(self.treasury.pubkey().to_bytes()));
                // Ensure WSOL ATA for destination
                let wsol_ata = match self.treasury.wrap_sol(&self.rpc, 0).await { Ok((ata,_sig)) => ata, Err(_) => self.treasury.pubkey() };
                // Source token ATA already found
                o.set_user_token_account(Pubkey::new_from_array(mint.to_bytes()), ata);
                o.set_user_token_account(Pubkey::new_from_array(sol_mint.to_bytes()), wsol_ata); // treat SOL as WSOL mint
                let mut min_out = 1u64;
                if let Ok(Some(q)) = o.quote_exact_in(&mint.to_string(), &sol_mint.to_string(), bal_tokens).await {
                    let msb3 = self.cfg.read().max_slippage_bps as u128;
                    min_out = ((q.amount_out as u128) * (10_000 - msb3) / 10_000) as u64;
                }
                o.build_swap_ix(&mint.to_string(), &sol_mint.to_string(), bal_tokens, min_out).unwrap_or_default()
            } else { Vec::new() }
        };
        if tx_ixs.is_empty() { return Ok(()); }
    let message = solana_sdk::message::Message::new(&tx_ixs, Some(&self.treasury.pubkey()));
    let fee_estimate = self.rpc.rpc.get_fee_for_message(&message).await.unwrap_or(0);
    let tx = Transaction::new_signed_with_payer(&tx_ixs, Some(&self.treasury.pubkey()), &[self.treasury.keypair.as_ref()], bh);
        // Snapshot invested_sol for position (if exists) before trade
        let invested_sol = {
            let rs = self.risk.read();
            rs.open.get(mint).map(|p| p.invested_sol).unwrap_or(0.0)
        };
        let pre_balance = self.total_sol_balance().await.unwrap_or(0.0);
    // Pre WSOL ATA amount (destination of swap proceeds)
    let wsol_mint_prog = spl_token::native_mint::id();
    let wsol_mint_sdk = solana_sdk::pubkey::Pubkey::new_from_array(wsol_mint_prog.to_bytes());
    let (wsol_ata, _prog_wsol) = match self.treasury.ata_address(&self.rpc, &owner_sdk, &wsol_mint_sdk).await { Ok(v)=>v, Err(_)=> (self.treasury.pubkey(), self.treasury.pubkey()) };
    let pre_wsol_amount: u64 = if let Ok(acc) = self.rpc.rpc.get_account(&wsol_ata).await { if acc.data.len()>=72 { u64::from_le_bytes(acc.data[64..72].try_into().unwrap()) } else {0} } else {0};
    let sent_at = Instant::now();
    match self.rpc_retry_tx(&tx, 3).await {
            Ok(sig) => {
                let dur = sent_at.elapsed();
                record_swap_latency(dur.as_nanos() as u64);
                info!(mint=%mint, sig=%sig, amount_tokens=bal_tokens, "exit trade submitted");
        // Read WSOL ATA after swap before unwrap
        let post_wsol_amount: u64 = if let Ok(acc) = self.rpc.rpc.get_account(&wsol_ata).await { if acc.data.len()>=72 { u64::from_le_bytes(acc.data[64..72].try_into().unwrap()) } else {0} } else {0};
        let delta_wsol = post_wsol_amount.saturating_sub(pre_wsol_amount) as f64 / 1e9;
        // Fetch recent block fee estimation via meta fallback (if any) else approximate by difference in native balance delta
        let post_native = self.rpc.rpc.get_balance(&owner_sdk).await.unwrap_or(0) as f64 / 1e9;
        let native_delta = (post_native - (pre_balance - (pre_wsol_amount as f64 / 1e9))).max(0.0); // approximate, excludes wsol tokens
        // Assume fee ~ native_delta decrease unrelated to proceeds (if negative) ignore
    let fee_est_native = fee_estimate as f64 / 1e9;
    let fee_est = if native_delta < 0.0 { -native_delta.max(fee_est_native) } else { fee_est_native }; // prefer RPC reported fee_estimate
    let realized = delta_wsol - invested_sol - fee_est;
    let trade_ret = if invested_sol > 0.0 { realized / invested_sol } else { 0.0 };
    record_trade_return(trade_ret);
                self.risk_reset_if_needed();
                {
                    let mut rs = self.risk.write();
                    let pos = rs.open.remove(mint);
                    rs.realized_pnl_sol += realized;
                    if realized < 0.0 { rs.realized_loss_today_sol += -realized; }
                    // Rolling return (pct of invested)
                    if let Some(p) = pos { if p.invested_sol > 0.0 { let ret = realized / p.invested_sol; rs.recent_realized.push(ret); }
                    }
                    let window = self.cfg.read().rolling_pnl_window.unwrap_or(50);
                    if rs.recent_realized.len() > window { let excess = rs.recent_realized.len() - window; rs.recent_realized.drain(0..excess); }
                    if rs.recent_realized.len() >= 5 { // compute simple Sharpe (mean / std * sqrt(n))
                        let n = rs.recent_realized.len() as f64;
                        let mean = rs.recent_realized.iter().copied().sum::<f64>() / n;
                        let var = rs.recent_realized.iter().map(|r| (r-mean)*(r-mean)).sum::<f64>() / n.max(1.0);
                        let std = var.sqrt();
                        if std > 0.0 { rs.last_sharpe = mean / std * n.sqrt(); }
                    }
                    DAILY_REALIZED_PNL_SOL_MICRO.store((rs.realized_pnl_sol * 1_000_000.0) as u64, std::sync::atomic::Ordering::Relaxed);
                    OPEN_POSITIONS_GAUGE.store(rs.open.len() as u64, std::sync::atomic::Ordering::Relaxed);
                }
                // Unwrap afterwards outside lock
                let _ = self.treasury.unwrap_wsol(&self.rpc, None).await;
                // Trade CSV log (SELL)
                let lamports_out = (delta_wsol * 1e9) as u64;
                record_network_fee(fee_estimate);
                let line = format!(
                    "{ts},SELL,{mint},*,{sig},0,{lamports_out},{tok_in},{tok_out},,,0,,{fee},{realized},exit_full",
                    ts=ChronoUtc::now().to_rfc3339(),
                    mint=mint,
                    sig=sig,
                    lamports_out=lamports_out,
                    tok_in=bal_tokens,
                    tok_out=bal_tokens,
                    fee=fee_estimate,
                    realized=realized
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
                if !alive { removed.push(*mint); }
                alive
            });
        }
        if !removed.is_empty() { debug!(count=removed.len(), "pending cleanup removed stale trades"); }
    }

    // Reconcile pending trades that are still within TTL but haven't produced a fill yet.
    // Strategy: fetch signature statuses; if confirmed and token balance now reflects fill, finalize.
    // If status is Err or NotFound after N seconds (half TTL), mark failed and remove.
    async fn reconcile_pending(&self, half_ttl_cutoff: i64) {
        // Collect copy of pending (mint -> trade) to avoid holding write lock across awaits
        let pend: Vec<(Pubkey, PendingTrade)> = {
            let rs = self.risk.read();
            rs.pending.iter().map(|(k,v)| (*k, v.clone())).collect()
        };
        if pend.is_empty() { return; }
        // Build list of signatures (dedup)
        let mut sigs: Vec<solana_sdk::signature::Signature> = Vec::new();
        for (_mint, p) in &pend { if let Ok(sig) = solana_sdk::signature::Signature::from_str(&p.sig) { sigs.push(sig); } }
        if sigs.is_empty() { return; }
        // Use low-level RPC client directly (status API) – fall back if not available
        // (Requires feature set in solana_rpc_client)
        let statuses_res = self.rpc.rpc.get_signature_statuses(&sigs).await.ok();
        let now_ts = chrono::Utc::now().timestamp();
        if let Some(statuses) = statuses_res {
            for ((mint, p), status_opt) in pend.iter().zip(statuses.value.into_iter()) {
                if let Some(status) = status_opt { // Some information available
                    if status.confirmations.is_some() || status.err.is_some() || status.slot > 0 { // progressed
                        if status.err.is_some() {
                            // Failed transaction -> drop pending
                            let mut rs = self.risk.write();
                            if rs.pending.remove(mint).is_some() {
                                PENDING_FAILED_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                debug!(mint=%mint, sig=%p.sig, "pending trade failed (status error)");
                            }
                            continue;
                        }
                        // If confirmed (confirmations None typically means rooted) attempt finalize_fill
                        if status.err.is_none() {
                            // Try finalize now (will remove pending & log fill)
                            PENDING_RECONCILIATIONS_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            self.finalize_fill(*mint).await;
                        }
                    } else {
                        // No progress yet; if older than half TTL consider dropping
                        if now_ts - p.ts > half_ttl_cutoff {
                            let mut rs = self.risk.write();
                            if rs.pending.remove(mint).is_some() {
                                PENDING_FAILED_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
        let path = std::env::var("IRONCRAB_RISK_STATE_PATH").unwrap_or_else(|_| "state/risk_state.json".to_string());
        std::path::PathBuf::from(path)
    }

    fn persist_risk_state(&self) {
        let path = Self::risk_state_file_path();
        if let Some(parent) = path.parent() { let _ = std::fs::create_dir_all(parent); }
        let snapshot = self.build_risk_snapshot_json();
        if let Ok(txt) = serde_json::to_string_pretty(&snapshot) { let _ = std::fs::write(&path, txt); }
    }

    fn build_risk_snapshot_json(&self) -> serde_json::Value {
        let rs = self.risk.read();
        let open: Vec<serde_json::Value> = rs.open.iter().map(|(k,v)| json!({
            "mint": k.to_string(),
            "entry_price_sol": v.entry_price_sol,
            "amount_tokens": v.amount_tokens,
            "invested_sol": v.invested_sol,
            "token_decimals": v.token_decimals,
            "last_unrealized_pnl_sol": v.last_unrealized_pnl_sol
        })).collect();
        let cooldown: Vec<serde_json::Value> = rs.cooldown_until.iter().map(|(k,v)| json!({"mint": k.to_string(), "until": v})).collect();
        json!({
            "version": 1,
            "realized_pnl_sol": rs.realized_pnl_sol,
            "realized_loss_today_sol": rs.realized_loss_today_sol,
            "current_day": rs.current_day,
            "recent_realized": rs.recent_realized,
            "last_sharpe": rs.last_sharpe,
            "open_positions": open,
            "cooldowns": cooldown
        })
    }

    fn try_load_risk_state(&self) {
        let path = Self::risk_state_file_path();
        if !path.exists() { return; }
        if let Ok(txt) = std::fs::read_to_string(&path) {
            if let Ok(val) = serde_json::from_str::<serde_json::Value>(&txt) {
                    let mut rs = self.risk.write();
                    rs.realized_pnl_sol = val.get("realized_pnl_sol").and_then(|v| v.as_f64()).unwrap_or(0.0);
                    rs.realized_loss_today_sol = val.get("realized_loss_today_sol").and_then(|v| v.as_f64()).unwrap_or(0.0);
                    rs.current_day = val.get("current_day").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
                    rs.recent_realized = val.get("recent_realized").and_then(|v| v.as_array()).map(|arr| arr.iter().filter_map(|x| x.as_f64()).collect()).unwrap_or_default();
                    rs.last_sharpe = val.get("last_sharpe").and_then(|v| v.as_f64()).unwrap_or(0.0);
                    rs.open.clear();
                    if let Some(op) = val.get("open_positions").and_then(|v| v.as_array()) {
                        for ent in op {
                            let mint_opt = ent.get("mint").and_then(|m| m.as_str());
                            let entry_opt = ent.get("entry_price_sol").and_then(|f| f.as_f64());
                            let amt_opt = ent.get("amount_tokens").and_then(|f| f.as_f64());
                            if let (Some(mint_str), Some(entry_price), Some(amount_tokens)) = (mint_opt, entry_opt, amt_opt) {
                                if let Ok(pk) = Pubkey::from_str(mint_str) {
                                    let invested = ent.get("invested_sol").and_then(|f| f.as_f64()).unwrap_or(0.0);
                                    let decs = ent.get("token_decimals").and_then(|f| f.as_u64()).unwrap_or(0) as u8;
                                    let last_unr = ent.get("last_unrealized_pnl_sol").and_then(|f| f.as_f64()).unwrap_or(0.0);
                                    rs.open.insert(pk, Position { entry_price_sol: entry_price, amount_tokens, invested_sol: invested, token_decimals: decs, last_unrealized_pnl_sol: last_unr });
                                }
                            }
                        }
                    }
                    rs.cooldown_until.clear();
                    if let Some(cd) = val.get("cooldowns").and_then(|v| v.as_array()) {
                        for c in cd { if let (Some(mint_str), Some(until)) = (c.get("mint").and_then(|m| m.as_str()), c.get("until").and_then(|u| u.as_i64())) { if let Ok(pk) = Pubkey::from_str(mint_str) { rs.cooldown_until.insert(pk, until); } } }
                    }
                    OPEN_POSITIONS_GAUGE.store(rs.open.len() as u64, std::sync::atomic::Ordering::Relaxed);
                    DAILY_REALIZED_PNL_SOL_MICRO.store((rs.realized_pnl_sol * 1_000_000.0) as u64, std::sync::atomic::Ordering::Relaxed);
                    info!(positions = rs.open.len(), "risk state restored");
            } else {
                warn!("risk state json parse failed");
            }
        } else {
            debug!("risk state read failed");
        }
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
            if base == *mint && (quote == usdc || quote == usdt) { let usd = r_quote as f64 / 1e6; total_sol += (usd / stable_rate) * 2.0; considered += 1; }
            else if quote == *mint && (base == usdc || base == usdt) { let usd = r_base as f64 / 1e6; total_sol += (usd / stable_rate) * 2.0; considered += 1; }
        };
        if let Some(r) = &self.raydium {
            for snap in r.snapshots() { if snap.base_mint == *mint || snap.quote_mint == *mint { handle_pool(snap.base_mint, snap.quote_mint, snap.reserve_base, snap.reserve_quote); } }
        }
        if let Some(o) = &self.orca {
            for ps in o.pools_snapshot() { if ps.base_mint == *mint || ps.quote_mint == *mint { handle_pool(ps.base_mint, ps.quote_mint, ps.reserve_base, ps.reserve_quote); } }
        }
        
    if considered == 0 { return Ok(None); }
    LIQUIDITY_ESTIMATE_SOL_MICRO.store((total_sol * 1_000_000.0) as u64, std::sync::atomic::Ordering::Relaxed);
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
    { let r = self.cfg.read(); if r.lp_top1_max_pct.is_none() && r.lp_top3_max_pct.is_none() && r.lp_top5_max_pct.is_none() { return Ok(None); } }
    let (thr1,thr3,thr5) = { let r = self.cfg.read(); (r.lp_top1_max_pct.unwrap_or(f64::MAX), r.lp_top3_max_pct.unwrap_or(f64::MAX), r.lp_top5_max_pct.unwrap_or(f64::MAX)) };
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

pub async fn run_sniper(rpc: Arc<SolanaRpc>, cfg: SniperCfg, raydium: Option<Arc<Raydium>>, orca: Option<Arc<Orca>>, treasury: Arc<Treasury>) -> Result<()> {
    let engine = SniperEngine::new(rpc, cfg, raydium, orca, treasury);
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
            max_position_sol: s.max_position_sol,
            stop_loss_bps: s.stop_loss_bps,
            take_profit_bps: s.take_profit_bps,
            daily_loss_limit_sol: s.daily_loss_limit_sol,
            max_open_positions: s.max_open_positions,
            per_mint_position_limit: s.per_mint_position_limit,
            stop_loss_cooldown_secs: s.stop_loss_cooldown_secs,
            drawdown_scale_start: s.drawdown_scale_start,
            drawdown_max_reduction: s.drawdown_max_reduction,
            rolling_pnl_window: s.rolling_pnl_window,
            hot_reload_secs: s.hot_reload_secs,
            pending_trade_ttl_secs: s.pending_trade_ttl_secs,
        }
    }
}
// EOF extra brace fix
