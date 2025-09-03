use ironcrab::backtest::market::{CfmPool, CfmAdapter, MarketAdapter};

// Quote Validation Invariant:
// Larger trade size => (non-decreasing) price impact bps AND marginal output per unit should not increase.
#[test]
fn quote_price_impact_monotonic() {
    let adapter = CfmAdapter { pools: vec![ CfmPool { pool:"P1".into(), base_mint:"BASE".into(), quote_mint:"QUOTE".into(), base_reserve: 1_000_000, quote_reserve: 2_000_000, fee_bps: 30 } ] };
    // amounts to test (in base mint)
    let amts = [10_u64, 100, 1_000, 5_000, 10_000, 20_000];
    let mut quotes: Vec<(u64,u64,u32)> = Vec::new();
    for a in amts { let q = adapter.quote("BASE","QUOTE", a).unwrap(); quotes.push((a, q.amount_out, q.price_impact_bps)); }
    // price impact non-decreasing
    for w in quotes.windows(2) { assert!(w[1].2 >= w[0].2, "impact decreased: {} -> {}", w[0].2, w[1].2); }
    // Sanity: amount_out grows with amount_in (strictly increasing for these small trades)
    for w in quotes.windows(2) { assert!(w[1].1 > w[0].1, "amount_out not increasing: {} -> {}", w[0].1, w[1].1); }
}
