#![no_main]
//! Fuzz target for Raydium AMM V4 pool state parser
//! 
//! Tests that PoolV4::decode() handles arbitrary byte sequences safely
//! without panics or crashes.

use libfuzzer_sys::fuzz_target;
use solana_sdk::pubkey::Pubkey;

fuzz_target!(|data: &[u8]| {
    // Exercise the Raydium pool decoder against arbitrary data
    // Use a dummy address since we're testing the parsing logic
    let dummy_addr = Pubkey::new_unique();
    let _ = ironcrab::solana::dex::raydium::reader::PoolV4::decode(dummy_addr, data);
});
