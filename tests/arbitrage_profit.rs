use ironcrab::solana::arbitrage::compute_net_profit;

#[test]
fn profit_filter_accepts_and_rejects() {
    let amount_in = 1_000_000u64; // 1.0 units @ 6 decimals
                                  // Case 1: 3% gain, min_profit_bps=50 (0.5%), tx cost 1_000
    let final_out = 1_030_000u64; // +30_000
    let net = compute_net_profit(amount_in, final_out, 50, 1_000).expect("should pass");
    assert!(net > 28_000 && net <= 29_000, "net within expected window");
    // Case 2: 0.2% gain below threshold
    assert!(compute_net_profit(amount_in, 1_002_000u64, 50, 0).is_none());
    // Case 3: Gain wiped by tx cost
    assert!(compute_net_profit(amount_in, 1_005_000u64, 10, 5_000).is_none());
}
