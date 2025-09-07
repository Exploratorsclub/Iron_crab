//! Arbitrage‑Loop Skeleton: vergleicht Quotes zwischen DEXen und generiert TradeIntents

use super::{
    dex::{Dex, Quote},
    rpc::SolanaRpc,
};
use crate::metrics::{
    ARB_TRIANGLE_ATTEMPTS, ARB_TRIANGLE_PROFITABLE, CYCLE_COMPLETED, CYCLE_PARTIAL_EXAMINED,
    CYCLE_PRUNED_BOUND, CYCLE_PRUNED_DOMINANCE,
};
use crate::solana::compute_budget_estimator as cbe;
use crate::solana::dex::router::Router;
use crate::types::{Amount, Side, Token, TradeIntent};
use anyhow::Result;
use rust_decimal::Decimal;
use solana_sdk::{
    hash::Hash, instruction::Instruction, signature::Keypair, signature::Signer,
    transaction::Transaction,
};
use std::sync::Arc;
use tracing::info;

/// Planned transaction containing ordered instructions plus bookkeeping.
#[derive(Debug, Clone)]
pub struct TransactionPlan {
    pub ixs: Vec<Instruction>,
    pub compute_unit_limit: u32,
    pub compute_unit_price_micro_lamports: u64,
    pub expected_profit: u64, // in input token raw units (post-estimate, pre-fees beyond priority)
    pub path_id: String,
}

#[derive(Debug, Clone)]
pub struct SimulationOutcome {
    pub units_consumed: Option<u64>,
    pub logs: Vec<String>,
    pub err: Option<String>,
}

#[derive(Debug, Clone)]
pub struct EdgeQuoteAgg {
    pub input_mint: String,
    pub output_mint: String,
    pub amount_in: u64,
    pub best_dex_index: usize,
    pub quote: Quote,
}

#[derive(Debug, Clone)]
pub struct CycleOpportunity {
    pub path: (String, String, String),
    pub amount_in: u64,
    pub gross_out: u64,
    pub gross_profit: u64,
    pub net_profit: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct GenericCycleOpportunity {
    pub path: Vec<String>, // first == last
    pub amount_in: u64,
    pub gross_out: u64,
    pub gross_profit: u64,
    pub net_profit: Option<u64>,
}

pub struct ArbitrageEngine {
    pub rpc: Arc<SolanaRpc>,
    pub connectors: Vec<Arc<dyn Dex>>, // Raydium, Orca, ...
    pub router: Router,
    pub min_profit_bps: u32,
    pub est_tx_cost_lamports: u64,
}

impl ArbitrageEngine {
    pub fn new(rpc: Arc<SolanaRpc>, connectors: Vec<Arc<dyn Dex>>) -> Self {
        let router = Router::new(connectors.clone());
        Self {
            rpc,
            connectors,
            router,
            min_profit_bps: 0,
            est_tx_cost_lamports: 0,
        }
    }

    pub fn with_profit_params(mut self, min_profit_bps: u32, est_tx_cost_lamports: u64) -> Self {
        self.min_profit_bps = min_profit_bps;
        self.est_tx_cost_lamports = est_tx_cost_lamports;
        self
    }

    pub async fn best_edge(
        &self,
        input_mint: &str,
        output_mint: &str,
        ui_amount: f64,
    ) -> Result<Option<TradeIntent>> {
        // refresh pools (could be throttled externally)
        for c in &self.connectors {
            c.refresh_pools().await.ok();
        }
        let amount_in = (ui_amount * 10f64.powi(6)).round() as u64; // assume 6 decimals default for now
        let mut best: Option<(Arc<dyn Dex>, Quote)> = None;
        for c in &self.connectors {
            if let Ok(Some(q)) = c.quote_exact_in(input_mint, output_mint, amount_in).await {
                let better = best
                    .as_ref()
                    .map(|(_, bq)| q.amount_out > bq.amount_out)
                    .unwrap_or(true);
                if better {
                    best = Some((Arc::clone(c), q));
                }
            }
        }
        if let Some((_dex, q)) = best {
            info!(
                out = q.amount_out,
                impact_bps = q.price_impact_bps,
                "best quote"
            );
            let intent = TradeIntent {
                market: "aggregated".to_string(),
                base: Token {
                    symbol: "BASE".into(),
                    mint: input_mint.into(),
                    decimals: 6,
                },
                quote: Token {
                    symbol: "QUOTE".into(),
                    mint: output_mint.into(),
                    decimals: 6,
                },
                side: Side::Sell,
                amount: Amount {
                    ui: Decimal::from_f64_retain(ui_amount).unwrap_or(Decimal::ZERO),
                },
                max_slippage_bps: 100, // placeholder
            };
            Ok(Some(intent))
        } else {
            Ok(None)
        }
    }

    /// Simple triangle A->B->C->A profit attempt (greedy best per edge using router).
    pub async fn triangle_cycle(
        &self,
        a: &str,
        b: &str,
        c: &str,
        ui_amount_a: f64,
    ) -> Result<Option<(String, u64)>> {
        let amount_in = (ui_amount_a * 10f64.powi(6)) as u64; // assume 6 decimals
        let hop1 = self.router.best_quote_exact_in(a, b, amount_in).await?;
        let h1 = match hop1 {
            Some(h) => h,
            None => return Ok(None),
        };
        let hop2 = self
            .router
            .best_quote_exact_in(b, c, h1.quote.amount_out)
            .await?;
        let h2 = match hop2 {
            Some(h) => h,
            None => return Ok(None),
        };
        let hop3 = self
            .router
            .best_quote_exact_in(c, a, h2.quote.amount_out)
            .await?;
        let h3 = match hop3 {
            Some(h) => h,
            None => return Ok(None),
        };
        let final_out = h3.quote.amount_out;
        if final_out > amount_in {
            return Ok(Some((format!("{}-{}-{}", a, b, c), final_out - amount_in)));
        }
        Ok(None)
    }

    /// Triangle with profit filter (net after est tx cost and min_profit_bps threshold). Returns net profit.
    pub async fn triangle_cycle_profit_checked(
        &self,
        a: &str,
        b: &str,
        c: &str,
        ui_amount_a: f64,
        decimals: u32,
    ) -> Result<Option<(String, u64)>> {
        let amount_in = (ui_amount_a * 10f64.powi(decimals as i32)) as u64;
        ARB_TRIANGLE_ATTEMPTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = self.triangle_cycle(a, b, c, ui_amount_a).await?;
        let (id, gross_profit) = match path {
            Some(p) => p,
            None => return Ok(None),
        };
        if let Some(net) = compute_net_profit(
            amount_in,
            amount_in + gross_profit,
            self.min_profit_bps,
            self.est_tx_cost_lamports,
        ) {
            return Ok(Some((id, net)));
        }
        Ok(None)
    }

    /// Assemble a transaction plan for a profitable triangle (A->B->C->A) using best hop quotes.
    /// Returns None if not profitable after filters or if any hop instructions can't be built.
    pub async fn assemble_triangle_plan(
        &self,
        a: &str,
        b: &str,
        c: &str,
        ui_amount_a: f64,
        decimals: u32,
        slippage_bps: u32,
        _fee_payer: &Keypair,
    ) -> Result<Option<TransactionPlan>> {
        let amount_in = (ui_amount_a * 10f64.powi(decimals as i32)) as u64;
        ARB_TRIANGLE_ATTEMPTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        // Hop1
        let hop1 = self.router.best_quote_exact_in(a, b, amount_in).await?;
        let h1 = match hop1 {
            Some(h) => h,
            None => return Ok(None),
        };
        // Hop2
        let hop2 = self
            .router
            .best_quote_exact_in(&h1.quote.output_mint, c, h1.quote.amount_out)
            .await?;
        let h2 = match hop2 {
            Some(h) => h,
            None => return Ok(None),
        };
        // Hop3
        let hop3 = self
            .router
            .best_quote_exact_in(&h2.quote.output_mint, a, h2.quote.amount_out)
            .await?;
        let h3 = match hop3 {
            Some(h) => h,
            None => return Ok(None),
        };
        let final_out = h3.quote.amount_out;
        if final_out <= amount_in {
            return Ok(None);
        }
        let _gross_profit = final_out - amount_in; // gross currently not returned directly; kept for potential logging
        let maybe_net = compute_net_profit(
            amount_in,
            final_out,
            self.min_profit_bps,
            self.est_tx_cost_lamports,
        );
        let net_profit = match maybe_net {
            Some(n) => n,
            None => return Ok(None),
        };
        ARB_TRIANGLE_PROFITABLE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        // Build instructions for each hop using DEX trait (min_out only on final hop applying slippage)
        let quotes_vec = vec![h1.quote.clone(), h2.quote.clone(), h3.quote.clone()];
        let min_out_final = Router::cumulative_min_out(&quotes_vec, slippage_bps);
        let dexs = self.router.dexs();
        let mut ixs: Vec<Instruction> = Vec::new();
        // Hop1: min_out = 0 (feed into hop2)
        let hop1_ixs = dexs[h1.dex_index].build_swap_ix(
            &h1.quote.input_mint,
            &h1.quote.output_mint,
            amount_in,
            0,
        )?;
        ixs.extend(hop1_ixs);
        // Hop2: min_out = 0 (feed into hop3)
        let hop2_ixs = dexs[h2.dex_index].build_swap_ix(
            &h2.quote.input_mint,
            &h2.quote.output_mint,
            h1.quote.amount_out,
            0,
        )?;
        ixs.extend(hop2_ixs);
        // Hop3: apply min_out
        let hop3_ixs = dexs[h3.dex_index].build_swap_ix(
            &h3.quote.input_mint,
            &h3.quote.output_mint,
            h2.quote.amount_out,
            min_out_final,
        )?;
        ixs.extend(hop3_ixs);
        // Compute budget estimation (single tx, 3 swaps => hops=3, ixs ~ 3 + 2 compute budget)
        let est =
            cbe::estimate_from_instructions(&ixs, 3, amount_in, cbe::EstimatorConfig::default());
        let plan = TransactionPlan {
            ixs,
            compute_unit_limit: est.compute_unit_limit,
            compute_unit_price_micro_lamports: est.compute_unit_price_micro_lamports,
            expected_profit: net_profit,
            path_id: format!("{}-{}-{}", a, b, c),
        };
        Ok(Some(plan))
    }

    /// Simulate a transaction plan (adds compute budget ixs at front).
    pub async fn simulate_transaction_plan(
        &self,
        plan: &TransactionPlan,
        fee_payer: &Keypair,
    ) -> Result<SimulationOutcome> {
        use crate::solana::compute_budget_helper as cbh;
        // Fetch recent blockhash
        let bh: Hash = self.rpc.rpc.get_latest_blockhash().await?;
        let mut ixs: Vec<Instruction> = Vec::new();
        if plan.compute_unit_limit > 0 {
            ixs.push(cbh::set_compute_unit_limit(plan.compute_unit_limit));
        }
        if plan.compute_unit_price_micro_lamports > 0 {
            ixs.push(cbh::set_compute_unit_price(
                plan.compute_unit_price_micro_lamports,
            ));
        }
        ixs.extend(plan.ixs.clone());
        let tx =
            Transaction::new_signed_with_payer(&ixs, Some(&fee_payer.pubkey()), &[fee_payer], bh);
        let sim = self.rpc.rpc.simulate_transaction(&tx).await?; // RpcSimulateTransactionResult
        let value = sim.value;
        let logs = value.logs.unwrap_or_default();
        let units_consumed = value.units_consumed;
        let err = value.err.map(|e| format!("{:?}", e));
        Ok(SimulationOutcome {
            units_consumed,
            logs,
            err,
        })
    }

    /// Aggregate best quotes for a list of (input, output) pairs using all connectors (does NOT refresh).
    pub async fn aggregate_best_edges(
        &self,
        pairs: &[(String, String)],
        amount_in: u64,
    ) -> Result<Vec<EdgeQuoteAgg>> {
        use futures::future::join_all;
        let mut results: Vec<EdgeQuoteAgg> = Vec::new();
        for (input, output) in pairs.iter() {
            // query all dexs concurrently
            let futs = self
                .connectors
                .iter()
                .map(|d| d.quote_exact_in(input, output, amount_in));
            let outs = join_all(futs).await;
            let mut best: Option<(usize, Quote)> = None;
            for (i, r) in outs.into_iter().enumerate() {
                if let Ok(Some(q)) = r {
                    let rep = best
                        .as_ref()
                        .map(|(_, b)| b.amount_out < q.amount_out)
                        .unwrap_or(true);
                    if rep {
                        best = Some((i, q));
                    }
                }
            }
            if let Some((di, q)) = best {
                results.push(EdgeQuoteAgg {
                    input_mint: input.clone(),
                    output_mint: output.clone(),
                    amount_in,
                    best_dex_index: di,
                    quote: q,
                });
            }
        }
        Ok(results)
    }

    /// Enumerate triangular cycles (A->B->C->A) over discovered token graph and return profitable opportunities (gross > in).
    /// Decimals assumed homogeneous (6) for now; future: per-mint decimals map.
    pub async fn enumerate_triangular_cycles(
        &self,
        base_tokens: &[String],
        amount_in: u64,
    ) -> Result<Vec<CycleOpportunity>> {
        use std::collections::{HashMap, HashSet};
        let mut pairs: HashSet<(String, String)> = HashSet::new();
        for d in &self.connectors {
            for (a, b) in d.list_pairs() {
                pairs.insert((a.clone(), b.clone()));
            }
        }
        // Build adjacency
        let mut adj: HashMap<String, HashSet<String>> = HashMap::new();
        for (a, b) in pairs.iter() {
            adj.entry(a.clone()).or_default().insert(b.clone());
            adj.entry(b.clone()).or_default().insert(a.clone());
        }
        let mut cycles = Vec::new();
        for base in base_tokens {
            if !adj.contains_key(base) {
                continue;
            }
            let neigh1 = adj.get(base).unwrap();
            for mid1 in neigh1.iter() {
                if mid1 == base {
                    continue;
                }
                let neigh2 = match adj.get(mid1) {
                    Some(n) => n,
                    None => continue,
                };
                for mid2 in neigh2.iter() {
                    if mid2 == mid1 || mid2 == base {
                        continue;
                    }
                    // require edge mid2 -> base exists to close cycle
                    if !adj.get(mid2).map(|s| s.contains(base)).unwrap_or(false) {
                        continue;
                    }
                    // Evaluate greedy cycle using router quotes
                    let h1 = self
                        .router
                        .best_quote_exact_in(base, mid1, amount_in)
                        .await?;
                    let h1 = match h1 {
                        Some(v) => v,
                        None => continue,
                    };
                    let h2 = self
                        .router
                        .best_quote_exact_in(&h1.quote.output_mint, mid2, h1.quote.amount_out)
                        .await?;
                    let h2 = match h2 {
                        Some(v) => v,
                        None => continue,
                    };
                    let h3 = self
                        .router
                        .best_quote_exact_in(&h2.quote.output_mint, base, h2.quote.amount_out)
                        .await?;
                    let h3 = match h3 {
                        Some(v) => v,
                        None => continue,
                    };
                    let final_out = h3.quote.amount_out;
                    if final_out <= amount_in {
                        continue;
                    }
                    let gross_profit = final_out - amount_in;
                    let net = compute_net_profit(
                        amount_in,
                        final_out,
                        self.min_profit_bps,
                        self.est_tx_cost_lamports,
                    );
                    cycles.push(CycleOpportunity {
                        path: (base.clone(), mid1.clone(), mid2.clone()),
                        amount_in,
                        gross_out: final_out,
                        gross_profit,
                        net_profit: net,
                    });
                }
            }
        }
        // Deduplicate cycles (A,B,C) vs (A,C,B): enforce order mid1 < mid2 lexicographically
        let mut uniq: HashMap<(String, String, String), CycleOpportunity> = HashMap::new();
        for c in cycles.into_iter() {
            let (a, b, c2) = &c.path;
            let key = if b < c2 {
                (a.clone(), b.clone(), c2.clone())
            } else {
                (a.clone(), c2.clone(), b.clone())
            };
            let replace = uniq
                .get(&key)
                .map(|e| e.gross_profit < c.gross_profit)
                .unwrap_or(true);
            if replace {
                uniq.insert(key, c);
            }
        }
        let mut v: Vec<_> = uniq.into_values().collect();
        v.sort_by(|x, y| y.gross_profit.cmp(&x.gross_profit));
        Ok(v)
    }

    /// Rank triangular cycles by net_profit (if available) otherwise by gross_profit. Returns top `limit`.
    pub async fn rank_triangular_cycles(
        &self,
        base_tokens: &[String],
        amount_in: u64,
        limit: usize,
    ) -> Result<Vec<CycleOpportunity>> {
        let mut cycles = self
            .enumerate_triangular_cycles(base_tokens, amount_in)
            .await?;
        cycles.sort_by(|a, b| {
            let an = a.net_profit.unwrap_or(a.gross_profit);
            let bn = b.net_profit.unwrap_or(b.gross_profit);
            bn.cmp(&an) // descending
        });
        if cycles.len() > limit {
            cycles.truncate(limit);
        }
        Ok(cycles)
    }

    /// Enumerate generic cycles up to `max_hops` (token count including start/end) starting from each base token.
    /// Example: max_hops=5 allows A->B->C->D->A (4 distinct edges, 5 tokens with return).
    pub async fn enumerate_cycles_generic(
        &self,
        base_tokens: &[String],
        amount_in: u64,
        max_hops: usize,
        max_cycles: usize,
    ) -> Result<Vec<GenericCycleOpportunity>> {
        use std::collections::{HashMap, HashSet};
        if max_hops < 4 {
            return Ok(Vec::new());
        }
        // Build adjacency from all connectors
        let mut adj: HashMap<String, HashSet<String>> = HashMap::new();
        for d in &self.connectors {
            for (a, b) in d.list_pairs() {
                adj.entry(a.clone()).or_default().insert(b.clone());
                adj.entry(b.clone()).or_default().insert(a.clone());
            }
        }
        let mut results: Vec<GenericCycleOpportunity> = Vec::new();
        // Dominance map: (last_token, depth) -> best_amount_out observed so far.
        let mut dominance: HashMap<(String, usize), u64> = HashMap::new();
        // Track best gross profit (for bound pruning)
        let mut best_gross_profit: u64 = 0;
        // Optimistic max per-hop multiplier in bps (start very high so pruning is safe / null until refined)
        let mut max_ratio_bps: u64 = 30_000; // 3.0x start optimistic (will reduce as we measure real quotes)
        for base in base_tokens {
            if !adj.contains_key(base) {
                continue;
            }
            // DFS stack: (path_tokens, current_amount_out)
            let mut stack: Vec<(Vec<String>, u64)> = vec![(vec![base.clone()], amount_in)];
            let mut seen_paths = HashSet::new();
            while let Some((path, amt)) = stack.pop() {
                if results.len() >= max_cycles {
                    break;
                }
                if path.len() > max_hops {
                    continue;
                }
                let current = path.last().unwrap();
                let neighbors = match adj.get(current) {
                    Some(n) => n,
                    None => continue,
                };
                for nxt in neighbors {
                    if results.len() >= max_cycles {
                        break;
                    }
                    // Closing condition
                    let closing = nxt == base && path.len() >= 3; // >=3 distinct tokens visited (path len >=3 before adding base)
                    if closing {
                        // Form cycle path including base again
                        let mut full = path.clone();
                        full.push(base.clone());
                        // Quote along edges to compute profit (re-run sequential quotes for accuracy)
                        let mut in_amount_close = amount_in;
                        let mut ok_close = true;
                        for w in 0..full.len() - 1 {
                            // last is closing base
                            let from = &full[w];
                            let to = &full[w + 1];
                            match self
                                .router
                                .best_quote_exact_in(from, to, in_amount_close)
                                .await?
                            {
                                Some(rq) => {
                                    // refine max_ratio_bps if this hop ratio is higher
                                    let ratio_bps = if in_amount_close == 0 {
                                        0
                                    } else {
                                        (rq.quote.amount_out as u128 * 10_000u128
                                            / in_amount_close as u128)
                                            as u64
                                    };
                                    if ratio_bps > max_ratio_bps {
                                        max_ratio_bps = ratio_bps;
                                    }
                                    in_amount_close = rq.quote.amount_out;
                                }
                                None => {
                                    ok_close = false;
                                    break;
                                }
                            }
                        }
                        if !ok_close {
                            continue;
                        }
                        if in_amount_close <= amount_in {
                            continue;
                        }
                        let gross_profit = in_amount_close - amount_in;
                        if gross_profit > best_gross_profit {
                            best_gross_profit = gross_profit;
                        }
                        let net = compute_net_profit(
                            amount_in,
                            in_amount_close,
                            self.min_profit_bps,
                            self.est_tx_cost_lamports,
                        );
                        // Dedup by sorted internal tokens (excluding duplicate base at end) + length
                        let key = {
                            let mut inner = full.clone();
                            inner.pop(); // remove trailing base
                            format!("{}:{}", inner.join("->"), inner.len())
                        };
                        let replace = seen_paths.insert(key);
                        if replace {
                            results.push(GenericCycleOpportunity {
                                path: full,
                                amount_in,
                                gross_out: in_amount_close,
                                gross_profit,
                                net_profit: net,
                            });
                        }
                        CYCLE_COMPLETED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        continue;
                    }
                    // Avoid revisiting base mid-path or repeating tokens (simple cycle, no repeats except closing base)
                    if nxt == base || path.contains(nxt) {
                        continue;
                    }
                    if path.len() + 1 > max_hops {
                        continue;
                    }
                    // Fetch quote for expansion edge (current -> nxt) to know new amount
                    let maybe_quote = self.router.best_quote_exact_in(current, nxt, amt).await?;
                    let rq = match maybe_quote {
                        Some(v) => v,
                        None => continue,
                    };
                    let new_amount = rq.quote.amount_out;
                    // Update optimistic max_ratio_bps if this hop higher
                    if amt > 0 {
                        let ratio_bps = (new_amount as u128 * 10_000u128 / amt as u128) as u64;
                        if ratio_bps > max_ratio_bps {
                            max_ratio_bps = ratio_bps;
                        }
                    }
                    // Dominance check
                    let depth_next = path.len(); // depth after adding nxt (0-based path len -1 edges so far)
                    let dom_key = (nxt.clone(), depth_next);
                    if let Some(best_amt) = dominance.get(&dom_key) {
                        if *best_amt >= new_amount {
                            CYCLE_PRUNED_DOMINANCE
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            continue;
                        }
                    }
                    dominance.insert(dom_key, new_amount);
                    // Upper-bound pruning: remaining edges (including closing back to base)
                    let remaining_edges = if max_hops > path.len() + 1 {
                        max_hops - (path.len() + 1)
                    } else {
                        0
                    }; // edges we could still traverse before hitting max_hops (excluding required closing which we allow within bound)
                    if remaining_edges > 0 {
                        // optimistic upper bound with exponent (remaining_edges + 1) to include closing hop
                        let exp = remaining_edges + 1;
                        let mut ub: u128 = new_amount as u128;
                        for _ in 0..exp {
                            ub = ub * (max_ratio_bps as u128) / 10_000u128;
                            if ub > u128::MAX / 2 {
                                break;
                            }
                        }
                        let ub_u64 = if ub > u128::from(u64::MAX) {
                            u64::MAX
                        } else {
                            ub as u64
                        };
                        if ub_u64 <= amount_in + best_gross_profit {
                            CYCLE_PRUNED_BOUND.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            continue;
                        }
                    }
                    let mut new_path = path.clone();
                    new_path.push(nxt.clone());
                    CYCLE_PARTIAL_EXAMINED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    stack.push((new_path, new_amount));
                }
            }
        }
        Ok(results)
    }

    /// Rank generic cycles (variable length) by net or gross profit.
    pub async fn rank_cycles_generic(
        &self,
        base_tokens: &[String],
        amount_in: u64,
        max_hops: usize,
        max_cycles: usize,
        limit: usize,
    ) -> Result<Vec<GenericCycleOpportunity>> {
        let mut cycles = self
            .enumerate_cycles_generic(base_tokens, amount_in, max_hops, max_cycles)
            .await?;
        cycles.sort_by(|a, b| {
            let an = a.net_profit.unwrap_or(a.gross_profit);
            let bn = b.net_profit.unwrap_or(b.gross_profit);
            bn.cmp(&an)
        });
        if cycles.len() > limit {
            cycles.truncate(limit);
        }
        Ok(cycles)
    }
}

/// Compute net profit (lamports of input token) after thresholds.
/// Returns Some(net_profit) if profit passes min_profit_bps AND exceeds est_tx_cost_lamports.
pub fn compute_net_profit(
    amount_in: u64,
    final_out: u64,
    min_profit_bps: u32,
    est_tx_cost_lamports: u64,
) -> Option<u64> {
    if final_out <= amount_in {
        return None;
    }
    let gross = final_out - amount_in;
    // bps = (gross / amount_in) * 10_000
    let bps = (gross as u128 * 10_000u128 / amount_in as u128) as u32;
    if bps < min_profit_bps {
        return None;
    }
    if gross <= est_tx_cost_lamports {
        return None;
    }
    let net = gross - est_tx_cost_lamports;
    if net == 0 {
        return None;
    }
    Some(net)
}
