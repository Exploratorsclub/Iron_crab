//! Simple multi-DEX router (single hop best-out selection)
//! Future: multi-hop graph search (depth 2), path scoring, slippage-aware sorting.

use std::sync::Arc;
use anyhow::Result;
use futures::future::join_all;
use super::{Dex, Quote};

pub struct RouteQuote {
    pub dex_index: usize,
    pub quote: Quote,
}

pub struct Router {
    dexs: Vec<Arc<dyn Dex>>, // heterogeneous DEX connectors
}

#[derive(Debug, Clone)]
pub struct MultiHopSwapPlan {
    pub ixs: Vec<solana_sdk::instruction::Instruction>,
    pub hops: Vec<(usize, Quote)>,
    pub amount_in: u64,
    pub expected_out: u64,
    pub min_out: u64,
    pub slippage_bps: u32,
}

impl Router {
    pub fn new(dexs: Vec<Arc<dyn Dex>>) -> Self { Self { dexs } }

    /// Refresh all dexs sequentially (can parallelize later if RPC budget allows).
    pub async fn refresh_all(&self) -> Result<()> {
        for d in &self.dexs { d.refresh_pools().await?; }
        Ok(())
    }

    /// Fetch best single-hop quote across all registered DEXs.
    pub async fn best_quote_exact_in(&self, input_mint: &str, output_mint: &str, amount_in: u64) -> Result<Option<RouteQuote>> {
        if self.dexs.is_empty() { return Ok(None); }
        let futs = self.dexs.iter().map(|d| d.quote_exact_in(input_mint, output_mint, amount_in));
        let results = join_all(futs).await;
        let mut best: Option<(usize, Quote)> = None;
        for (i, r) in results.into_iter().enumerate() {
            if let Ok(Some(q)) = r {
                let replace = best.as_ref().map(|(_,b)| b.amount_out < q.amount_out).unwrap_or(true);
                if replace { best = Some((i, q)); }
            }
        }
        Ok(best.map(|(i,q)| RouteQuote { dex_index: i, quote: q }))
    }

    /// Naive depth-2 multi-hop: input -> mid -> output. Considers pairs from all DEXs.
    pub async fn best_quote_exact_in_hops2(&self, input_mint: &str, output_mint: &str, amount_in: u64) -> Result<Option<(Vec<(usize, Quote)>, u64)>> {
        if self.dexs.is_empty() { return Ok(None); }
        // Collect all unique mid mints
        use std::collections::HashSet;
        let mut mids = HashSet::new();
        for (_di, d) in self.dexs.iter().enumerate() {
            for (a,b) in d.list_pairs() {
                if a == input_mint { mids.insert(b.clone()); }
                if b == input_mint { mids.insert(a.clone()); }
                if a == output_mint { mids.insert(b.clone()); }
                if b == output_mint { mids.insert(a.clone()); }
            }
        }
        mids.remove(input_mint); mids.remove(output_mint);
        let mut best: Option<(Vec<(usize, Quote)>, u64)> = None;
        for mid in mids {
            // First hop quotes
            let mut first_hop: Option<(usize, Quote)> = None;
            for (i,d) in self.dexs.iter().enumerate() {
                if let Ok(Some(q)) = d.quote_exact_in(input_mint, &mid, amount_in).await {
                    let rep = first_hop.as_ref().map(|(_,b)| b.amount_out < q.amount_out).unwrap_or(true);
                    if rep { first_hop = Some((i,q)); }
                }
            }
            let (i1,q1) = match first_hop { Some(v)=>v, None=>continue };
            // Second hop quotes using q1.amount_out as input
            let mut second_hop: Option<(usize, Quote)> = None;
            for (i,d) in self.dexs.iter().enumerate() {
                if let Ok(Some(q)) = d.quote_exact_in(&mid, output_mint, q1.amount_out).await {
                    let rep = second_hop.as_ref().map(|(_,b)| b.amount_out < q.amount_out).unwrap_or(true);
                    if rep { second_hop = Some((i,q)); }
                }
            }
            let (i2,q2) = match second_hop { Some(v)=>v, None=>continue };
            let total_out = q2.amount_out;
            let replace = best.as_ref().map(|(_,bo)| total_out > *bo).unwrap_or(true);
            if replace { best = Some((vec![(i1,q1),(i2,q2)], total_out)); }
        }
        Ok(best)
    }

    /// Compute cumulative min_out for a sequence of hop quotes with per-hop slippage bps.
    pub fn cumulative_min_out(quotes: &[Quote], slippage_bps: u32) -> u64 {
        if quotes.is_empty() { return 0; }
        // Apply slippage only once on final amount (simplest conservative approach). Alternative: per-hop compounding.
        let final_out = quotes.last().unwrap().amount_out as u128;
        let keep = 10_000u128.saturating_sub(slippage_bps as u128);
        (final_out * keep / 10_000u128) as u64
    }

    /// Build a multi-hop (depth=2) swap plan with min_out (slippage applied on final output only).
    pub async fn build_best_hops2_plan_exact_in(&self, input_mint: &str, output_mint: &str, amount_in: u64, slippage_bps: u32) -> Result<Option<MultiHopSwapPlan>> {
        let best_path = self.best_quote_exact_in_hops2(input_mint, output_mint, amount_in).await?;
        let (hops, final_out) = match best_path { Some(v)=>v, None=>return Ok(None) };
        let min_out = Self::cumulative_min_out(&hops.iter().map(|(_,q)| q.clone()).collect::<Vec<_>>(), slippage_bps);
        // Build instructions per hop from each DEX via trait builder (uses per-hop min_out only on final hop for safety)
        let mut ixs = Vec::new();
        if hops.len() == 2 {
            let (d1, q1) = &hops[0];
            let (d2, q2) = &hops[1];
            let mid = q1.output_mint.clone(); // by construction q1.output == q2.input
            // First hop: min_out = 0 (feed second hop), second hop: global min_out.
            let hop1_ixs = self.dexs[*d1].build_swap_ix(&q1.input_mint, &mid, amount_in, 0)?;
            let hop2_ixs = self.dexs[*d2].build_swap_ix(&mid, &q2.output_mint, q1.amount_out, min_out)?;
            ixs.extend(hop1_ixs);
            ixs.extend(hop2_ixs);
        } else {
            // Fallback: single hop path already handled outside; return None.
        }
        Ok(Some(MultiHopSwapPlan { ixs, hops, amount_in, expected_out: final_out, min_out, slippage_bps }))
    }
}
