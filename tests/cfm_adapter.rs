mod common;
use ironcrab::backtest::market::{CfmAdapter,CfmPool,MarketAdapter};
use ironcrab::backtest::types::ActionSwap;

fn sample_pool() -> CfmPool { CfmPool { pool: "X".into(), base_mint: "A".into(), quote_mint: "B".into(), base_reserve: 1_000_000_000, quote_reserve: 2_000_000_000, fee_bps: 30, tick_spacing: None } }

#[test]
fn quote_basic() { let ad = CfmAdapter { pools: vec![sample_pool()] }; let q = ad.quote("A","B", 1_000_000).unwrap(); assert!(q.amount_out>0); }

#[test]
fn swap_math_invariant() { let mut ad = CfmAdapter { pools: vec![sample_pool()] }; let before = ad.pools[0].base_reserve * ad.pools[0].quote_reserve; let action = ActionSwap { pool:"X".into(), input_mint:"A".into(), output_mint:"B".into(), amount_in:500_000, max_slippage_bps:200}; let _ = ad.apply_swap(&action).unwrap(); let after = ad.pools[0].base_reserve * ad.pools[0].quote_reserve; assert!(after >= before); }
