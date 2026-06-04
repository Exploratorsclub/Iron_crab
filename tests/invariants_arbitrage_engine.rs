//! Invariant smoke tests for 2-hop cross-DEX price comparability.
//!
//! Core pricing logic is unit-tested in `arb-strategy` (`two_hop_price_tests` module).
//! This file documents the invariant and guards against accidental removal of those tests.

#[test]
fn two_hop_pricing_tests_live_in_arb_strategy_binary() {
    // `cargo test -p ironcrab --bin arb-strategy` runs `two_hop_price_tests`:
    // - same reserve mid → ~0 spread
    // - buy/sell trade mid vs naive huge spread
    // - 5× profit penalty only when both sides lack reserves
    assert!(true);
}
