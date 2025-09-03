use ironcrab::backtest::market::{CfmPool, CfmAdapter, MarketAdapter};

// Quote Validation Invariant:
// Larger trade size => (non-decreasing) price impact bps AND marginal output per unit should not increase.
#[test]
fn quote_price_impact_monotonic() {
    let mut adapter = CfmAdapter { pools: vec![ CfmPool { pool:"P1".into(), base_mint:"BASE".into(), quote_mint:"QUOTE".into(), base_reserve: 1_000_000, quote_reserve: 2_000_000, fee_bps: 30 } ] };
    // amounts to test (in base mint)
    let amts = [10_u64, 100, 1_000, 5_000, 10_000, 20_000];
    let mut last_impact: Option<u32> = None;
    let mut last_unit_out: Option<f64> = None;
    for a in amts { 
        let q = adapter.quote("BASE","QUOTE", a).expect("quote");
        // impact monotonic non-decreasing
        if let Some(prev) = last_impact { assert!(q.price_impact_bps >= prev, "impact decreased: {} -> {}", prev, q.price_impact_bps); }
        // average output per unit should not increase with larger trade (due to curvature + fee)
        let unit_out = q.amount_out as f64 / a as f64;
        if let Some(prev_u) = last_unit_out { assert!(unit_out <= prev_u + 1e-9, "unit out increased: {:.8} -> {:.8}", prev_u, unit_out); }
        last_impact = Some(q.price_impact_bps);
        last_unit_out = Some(unit_out);
    }
}
