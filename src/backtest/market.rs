use anyhow::{Result, anyhow};
use crate::solana::dex::Quote;
use super::types::{ActionSwap};
use crate::solana::dex::raydium::PoolSnapshot;

#[derive(Debug, Clone)]
pub struct CfmPool { pub pool: String, pub base_mint: String, pub quote_mint: String, pub base_reserve: u128, pub quote_reserve: u128, pub fee_bps: u32 }

pub trait MarketAdapter: Send + Sync {
    fn apply_swap(&mut self, action: &ActionSwap) -> Result<(u64, u64)>; // (in,out)
    fn quote(&self, input_mint: &str, output_mint: &str, amount_in: u64) -> Option<Quote>;
}

pub struct CfmAdapter { pub pools: Vec<CfmPool> }
impl CfmAdapter {
    pub fn new() -> Self { Self { pools: vec![] } }
    pub fn upsert_pool(&mut self, p: CfmPool) {
        if let Some(pos) = self.pools.iter().position(|x| x.pool == p.pool) { self.pools[pos] = p; } else { self.pools.push(p); }
    }

    pub fn ingest_raydium(&mut self, snaps: &[PoolSnapshot]) {
        for s in snaps {
            self.upsert_pool(CfmPool { pool: s.address.to_string(), base_mint: s.base_mint.to_string(), quote_mint: s.quote_mint.to_string(), base_reserve: s.reserve_base, quote_reserve: s.reserve_quote, fee_bps: s.fee_bps });
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
    Some(Quote { amount_out: out as u64, price_impact_bps: impact as u32, route: vec![p.pool.clone()], fee_bps: p.fee_bps, in_reserve: rin, out_reserve: rout, input_mint: (if forward { p.base_mint.clone() } else { p.quote_mint.clone() }), output_mint: (if forward { p.quote_mint.clone() } else { p.base_mint.clone() }) })
    }
}
