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
static MINT_BLACKLIST: Lazy<HashSet<String>> = Lazy::new(|| HashSet::new());

pub struct SniperCfg {
    pub max_buy_sol: f64,
    pub max_slippage_bps: u32,
    pub blacklist_mints: Vec<String>,
    pub blacklist_owners: Vec<String>,
    pub min_pool_liquidity_sol: Option<f64>,
    pub require_freeze_auth_none: Option<bool>,
    pub require_mint_decimals_range: Option<(u8,u8)>,
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
                                                Self::handle_logs("program", lines);
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
    fn handle_logs(program: &str, logs: Vec<String>) {
        // Very naive pool-detect heuristic: look for 'initialize' or 'Init' keywords.
        for l in logs {
            let lower = l.to_ascii_lowercase();
            if (lower.contains("initialize") || lower.contains("init")) && (lower.contains("pool") || lower.contains("whirlpool")) {
                debug!(program, line = %l, "candidate pool init log");
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
        }
    }
}
