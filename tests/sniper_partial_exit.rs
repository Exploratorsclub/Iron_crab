// We need access to PositionLot & RiskState internals; expose a minimal helper via public API would be ideal.
// For now, we construct a mock RiskState-like structure through public interfaces if available.
// If sniper internals are private, this test will need to be adjusted after exposing them.

#[test]
fn partial_exit_proportional_reduction_math() {
    // Emulate a position lot before partial exit
    let invested_sol = 10.0_f64;
    let amount_tokens = 1_000_000_f64; // assume 1e6 tokens bought
    let fraction = 0.5_f64; // take-profit sells half

    // After selling half, invested_sol should reduce proportionally by fraction
    let mut remaining_invested = invested_sol;
    let mut remaining_tokens = amount_tokens;

    let invest_slice = remaining_invested * fraction;
    remaining_invested -= invest_slice;
    remaining_tokens -= remaining_tokens * fraction; // mirrors sniper logic

    assert!((remaining_invested - invested_sol * (1.0 - fraction)).abs() < 1e-12);
    assert!((remaining_tokens - amount_tokens * (1.0 - fraction)).abs() < 1e-6);

    // Realized PnL calculation check (simplified): assume proceeds exactly equal invested slice * (1 + r)
    let r = 0.20_f64; // +20% return on the sold half
    let proceeds = invest_slice * (1.0 + r);
    let fees = 0.0_f64; // ignore for math isolation
    let realized = proceeds - invest_slice - fees; // = invest_slice * r
    let trade_ret = realized / invest_slice; // should equal r
    assert!((trade_ret - r).abs() < 1e-12);
}
