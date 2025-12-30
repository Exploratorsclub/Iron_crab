#![no_main]
//! Fuzz target for Pump.fun BondingCurveState parser
//! 
//! Tests that BondingCurveState::parse() handles arbitrary byte sequences safely
//! without panics or crashes.

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Exercise the Pump.fun bonding curve parser against arbitrary data
    let _ = ironcrab::solana::dex::pumpfun::BondingCurveState::parse(data);
});
