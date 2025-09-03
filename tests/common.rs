use rust_decimal::Decimal;

pub fn assert_close(a: u128, b: u128, tol_bps: u64) {
    if a == b { return; }
    let diff = if a > b { a - b } else { b - a };
    let max = a.max(b);
    let bps = diff * 10_000 / max;
    assert!(bps as u64 <= tol_bps, "values differ: {a} vs {b} -> {bps} bps > {tol_bps}");
}

pub fn d(v: i64) -> Decimal { Decimal::from(v) }
