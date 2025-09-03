mod common;
use ironcrab::solana::dex::raydium::Raydium;

// We'll re-use Raydium::apply_slippage_min_out static helper.
// ...existing code...

#[test]
fn slippage_min_out() {
    let q_out = 1_000_000u64;
    let min_1pct = Raydium::apply_slippage_min_out(q_out, 100); // 1%
    assert_eq!(min_1pct, 990_000);
    let min_0 = Raydium::apply_slippage_min_out(q_out, 0);
    assert_eq!(min_0, q_out);
}

#[test]
fn slippage_bounds() {
    // 9999 bps means keep 1/10000 of amount -> floor(100 * 1 / 10000) = 0 with integer math
    assert_eq!(Raydium::apply_slippage_min_out(100, 9_999), 0); // extreme rounds down
    assert_eq!(Raydium::apply_slippage_min_out(100, 10_000), 0); // full loss
}
