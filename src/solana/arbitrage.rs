
//! Arbitrage‑Loop Skeleton: vergleicht Quotes zwischen DEXen und generiert TradeIntents

use anyhow::Result;
use std::sync::Arc;
use tracing::info;
use crate::{types::{TradeIntent, Token, Amount, Side}};
use super::{rpc::SolanaRpc, dex::{Dex, Quote}};
use crate::metrics::{ARB_TRIANGLE_ATTEMPTS, ARB_TRIANGLE_PROFITABLE};
use crate::solana::dex::router::Router;
use rust_decimal::Decimal;

pub struct ArbitrageEngine {
    pub rpc: Arc<SolanaRpc>,
    pub connectors: Vec<Arc<dyn Dex>>, // Raydium, Orca, ...
    pub router: Router,
    pub min_profit_bps: u32,
    pub est_tx_cost_lamports: u64,
}

impl ArbitrageEngine {
    pub fn new(rpc: Arc<SolanaRpc>, connectors: Vec<Arc<dyn Dex>>) -> Self { let router = Router::new(connectors.clone()); Self { rpc, connectors, router, min_profit_bps: 0, est_tx_cost_lamports: 0 } }

    pub fn with_profit_params(mut self, min_profit_bps: u32, est_tx_cost_lamports: u64) -> Self { self.min_profit_bps = min_profit_bps; self.est_tx_cost_lamports = est_tx_cost_lamports; self }

    pub async fn best_edge(&self, input_mint: &str, output_mint: &str, ui_amount: f64) -> Result<Option<TradeIntent>> {
        // refresh pools (could be throttled externally)
        for c in &self.connectors { c.refresh_pools().await.ok(); }
        let amount_in = (ui_amount * 10f64.powi(6)).round() as u64; // assume 6 decimals default for now
        let mut best: Option<(Arc<dyn Dex>, Quote)> = None;
        for c in &self.connectors {
            if let Ok(Some(q)) = c.quote_exact_in(input_mint, output_mint, amount_in).await {
                let better = best.as_ref().map(|(_,bq)| q.amount_out > bq.amount_out).unwrap_or(true);
                if better { best = Some((Arc::clone(c), q)); }
            }
        }
        if let Some((_dex, q)) = best {
            info!(out = q.amount_out, impact_bps = q.price_impact_bps, "best quote");
            let intent = TradeIntent {
                market: "aggregated".to_string(),
                base: Token { symbol: "BASE".into(), mint: input_mint.into(), decimals: 6 },
                quote: Token { symbol: "QUOTE".into(), mint: output_mint.into(), decimals: 6 },
                side: Side::Sell,
                amount: Amount { ui: Decimal::from_f64_retain(ui_amount).unwrap_or(Decimal::ZERO) },
                max_slippage_bps: 100, // placeholder
            };
            Ok(Some(intent))
        } else {
            Ok(None)
        }
    }

    /// Simple triangle A->B->C->A profit attempt (greedy best per edge using router).
    pub async fn triangle_cycle(&self, a: &str, b: &str, c: &str, ui_amount_a: f64) -> Result<Option<(String, u64)>> {
        let amount_in = (ui_amount_a * 10f64.powi(6)) as u64; // assume 6 decimals
    let hop1 = self.router.best_quote_exact_in(a, b, amount_in).await?; let h1 = match hop1 { Some(h)=>h, None=>return Ok(None) };
    let hop2 = self.router.best_quote_exact_in(b, c, h1.quote.amount_out).await?; let h2 = match hop2 { Some(h)=>h, None=>return Ok(None) };
    let hop3 = self.router.best_quote_exact_in(c, a, h2.quote.amount_out).await?; let h3 = match hop3 { Some(h)=>h, None=>return Ok(None) };
        let final_out = h3.quote.amount_out;
    if final_out > amount_in { return Ok(Some((format!("{}-{}-{}", a,b,c), final_out - amount_in))); }
        Ok(None)
    }

    /// Triangle with profit filter (net after est tx cost and min_profit_bps threshold). Returns net profit.
    pub async fn triangle_cycle_profit_checked(&self, a: &str, b: &str, c: &str, ui_amount_a: f64, decimals: u32) -> Result<Option<(String, u64)>> {
        let amount_in = (ui_amount_a * 10f64.powi(decimals as i32)) as u64;
    ARB_TRIANGLE_ATTEMPTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let path = self.triangle_cycle(a,b,c, ui_amount_a).await?; let (id, gross_profit) = match path { Some(p)=>p, None=>return Ok(None) };
        if let Some(net) = compute_net_profit(amount_in, amount_in + gross_profit, self.min_profit_bps, self.est_tx_cost_lamports) { return Ok(Some((id, net))); }
        Ok(None)
    }
}

/// Compute net profit (lamports of input token) after thresholds.
/// Returns Some(net_profit) if profit passes min_profit_bps AND exceeds est_tx_cost_lamports.
pub fn compute_net_profit(amount_in: u64, final_out: u64, min_profit_bps: u32, est_tx_cost_lamports: u64) -> Option<u64> {
    if final_out <= amount_in { return None; }
    let gross = final_out - amount_in;
    // bps = (gross / amount_in) * 10_000
    let bps = (gross as u128 * 10_000u128 / amount_in as u128) as u32;
    if bps < min_profit_bps { return None; }
    if gross <= est_tx_cost_lamports { return None; }
    let net = gross - est_tx_cost_lamports;
    if net == 0 { return None; }
    Some(net)
}
