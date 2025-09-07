use anyhow::{Result, anyhow};
use crate::solana::dex::Quote;
use super::types::{ActionSwap};
use crate::solana::dex::raydium::PoolSnapshot;
use crate::solana::dex::orca::OrcaPoolSnapshot;

#[derive(Debug, Clone)]
pub struct CfmPool { pub pool: String, pub base_mint: String, pub quote_mint: String, pub base_reserve: u128, pub quote_reserve: u128, pub fee_bps: u32, pub tick_spacing: Option<u16> }

pub trait MarketAdapter: Send + Sync {
    fn apply_swap(&mut self, action: &ActionSwap) -> Result<(u64, u64)>; // (in,out)
    fn quote(&self, input_mint: &str, output_mint: &str, amount_in: u64) -> Option<Quote>;
    /// Optional: handle a new pool announcement (default no-op)
    fn on_new_pool(&mut self, _pool: &str, _base_mint: &str, _quote_mint: &str, _fee_bps: u32) {}
    /// Optional: handle a price/reserve update for an existing pool (default no-op)
    fn on_price_update(&mut self, _pool: &str, _base_reserve: u128, _quote_reserve: u128, _fee_bps: u32) {}
}

pub struct CfmAdapter { pub pools: Vec<CfmPool> }
impl CfmAdapter {
    pub fn new() -> Self { Self { pools: vec![] } }
    pub fn upsert_pool(&mut self, p: CfmPool) {
        if let Some(pos) = self.pools.iter().position(|x| x.pool == p.pool) {
            // Merge: preserve existing tick_spacing if incoming is None (e.g., generic CFM JSON overrides)
            let mut merged = p;
            if merged.tick_spacing.is_none() {
                merged.tick_spacing = self.pools[pos].tick_spacing;
            }
            self.pools[pos] = merged;
        } else {
            self.pools.push(p);
        }
    }

    pub fn ingest_raydium(&mut self, snaps: &[PoolSnapshot]) {
        for s in snaps {
            self.upsert_pool(CfmPool { pool: s.address.to_string(), base_mint: s.base_mint.to_string(), quote_mint: s.quote_mint.to_string(), base_reserve: s.reserve_base, quote_reserve: s.reserve_quote, fee_bps: s.fee_bps, tick_spacing: None });
        }
    }

    pub fn ingest_orca(&mut self, snaps: &[OrcaPoolSnapshot]) {
        for s in snaps {
            // Orca snapshot may carry tick spacing; keep 30 bps fallback for fee if not provided elsewhere.
            self.upsert_pool(CfmPool { pool: s.address.to_string(), base_mint: s.base_mint.to_string(), quote_mint: s.quote_mint.to_string(), base_reserve: s.reserve_base, quote_reserve: s.reserve_quote, fee_bps: 30, tick_spacing: s.tick_spacing });
        }
    }
}
impl MarketAdapter for CfmAdapter {
    fn apply_swap(&mut self, action: &ActionSwap) -> Result<(u64,u64)> {
        let pool = self.pools.iter_mut().find(|p| (p.base_mint==action.input_mint && p.quote_mint==action.output_mint) || (p.base_mint==action.output_mint && p.quote_mint==action.input_mint)).ok_or_else(|| anyhow!("pool not found"))?;
        let forward = pool.base_mint==action.input_mint;
        let (rin, rout) = if forward { (&mut pool.base_reserve, &mut pool.quote_reserve) } else { (&mut pool.quote_reserve, &mut pool.base_reserve) };
        if *rin==0 || *rout==0 { return Err(anyhow!("empty reserves")); }
        let fee_adj = (action.amount_in as u128) * (10_000 - pool.fee_bps as u128)/10_000;
        let out = (fee_adj * *rout) / (*rin + fee_adj);
        *rin += fee_adj;
        *rout -= out;
        Ok((action.amount_in, out as u64))
    }
    fn quote(&self, input_mint: &str, output_mint: &str, amount_in: u64) -> Option<Quote> {
        let p = self.pools.iter().find(|p| (p.base_mint==input_mint && p.quote_mint==output_mint) || (p.base_mint==output_mint && p.quote_mint==input_mint))?;
        let forward = p.base_mint==input_mint;
        let (rin, rout) = if forward { (p.base_reserve, p.quote_reserve) } else { (p.quote_reserve, p.base_reserve) };
        if rin==0 || rout==0 { return None; }
        let fee_adj = (amount_in as u128) * (10_000 - p.fee_bps as u128)/10_000;
        let out = (fee_adj * rout)/(rin + fee_adj);
        let impact = (fee_adj * 10_000)/(rin + fee_adj);
    Some(Quote { amount_out: out as u64, price_impact_bps: impact as u32, route: vec![p.pool.clone()], fee_bps: p.fee_bps, in_reserve: rin, out_reserve: rout, input_mint: (if forward { p.base_mint.clone() } else { p.quote_mint.clone() }), output_mint: (if forward { p.quote_mint.clone() } else { p.base_mint.clone() }), tick_spacing: p.tick_spacing })
    }
    fn on_new_pool(&mut self, pool: &str, base_mint: &str, quote_mint: &str, fee_bps: u32) {
        // If pool exists keep reserves; else create with zero reserves (will be updated soon)
    if self.pools.iter().any(|p| p.pool == pool) { return; }
    self.pools.push(CfmPool { pool: pool.to_string(), base_mint: base_mint.to_string(), quote_mint: quote_mint.to_string(), base_reserve: 0, quote_reserve: 0, fee_bps, tick_spacing: None });
    }
    fn on_price_update(&mut self, pool: &str, base_reserve: u128, quote_reserve: u128, fee_bps: u32) {
        if let Some(p) = self.pools.iter_mut().find(|p| p.pool == pool) {
            p.base_reserve = base_reserve;
            p.quote_reserve = quote_reserve;
            p.fee_bps = fee_bps;
        } else {
            // unknown pool: create minimal entry
            self.pools.push(CfmPool { pool: pool.to_string(), base_mint: String::new(), quote_mint: String::new(), base_reserve, quote_reserve, fee_bps, tick_spacing: None });
        }
    }
}

// Impact/Slippage model wrapper (pluggable)
#[derive(Debug, Clone, Copy)]
pub enum ImpactModelKind { CpmM, Clmm }

pub trait ImpactModel: Send + Sync {
    fn expected_out(&self, in_reserve:u128, out_reserve:u128, amount_in:u64, fee_bps:u32, tick_spacing: Option<u16>) -> u64;
}

pub struct CpmMModel;
impl ImpactModel for CpmMModel {
    fn expected_out(&self, in_reserve:u128, out_reserve:u128, amount_in:u64, fee_bps:u32, _tick_spacing: Option<u16>) -> u64 {
        if in_reserve==0 || out_reserve==0 { return 0; }
        let fee_adj = (amount_in as u128) * (10_000 - fee_bps as u128)/10_000;
        ((fee_adj * out_reserve)/(in_reserve + fee_adj)) as u64
    }
}

// Placeholder for concentrated liquidity model
pub struct ClmmModel;
impl ImpactModel for ClmmModel {
    fn expected_out(&self, in_reserve:u128, out_reserve:u128, amount_in:u64, fee_bps:u32, tick_spacing: Option<u16>) -> u64 {
        // Base CPMM expectation
        let base = CpmMModel.expected_out(in_reserve, out_reserve, amount_in, fee_bps, tick_spacing) as u128;
        if in_reserve == 0 { return 0; }
        let rin = in_reserve.max(1);
        // Approx price move in bps from input relative to liquidity (small-trade approximation)
        let amount_less_fee = (amount_in as u128) * (10_000u128 - fee_bps as u128) / 10_000u128;
        let move_bps = ((amount_less_fee * 10_000u128) / rin) as u32;
        // Use observed tick spacing where available (Orca), default to 64.
        let spacing_bps = tick_spacing.unwrap_or(64) as u32;
        let ticks = if spacing_bps == 0 { 0 } else { move_bps / spacing_bps };
        // Penalty increases with ticks crossed and raw move; cap total penalty.
        let penalty_bps = ((ticks.saturating_mul(8)) + (move_bps / 20)).min(600) as u128; // 8 bps per tick + 5% of move, cap 600 bps
        let adj = base.saturating_mul(10_000u128 - penalty_bps) / 10_000u128;
        adj as u64
    }
}
