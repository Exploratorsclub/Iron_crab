
//! Arbitrage‑Loop Skeleton: vergleicht Quotes zwischen DEXen und generiert TradeIntents

use anyhow::Result;
use std::sync::Arc;
use tracing::info;
use crate::{types::{TradeIntent, Token, Amount, Side}};
use super::{rpc::SolanaRpc, dex::{Dex, Quote}};
use rust_decimal::Decimal;

pub struct ArbitrageEngine {
    pub rpc: Arc<SolanaRpc>,
    pub connectors: Vec<Arc<dyn Dex>>, // Raydium, Orca, ...
}

impl ArbitrageEngine {
    pub fn new(rpc: Arc<SolanaRpc>, connectors: Vec<Arc<dyn Dex>>) -> Self { Self { rpc, connectors } }

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
}
