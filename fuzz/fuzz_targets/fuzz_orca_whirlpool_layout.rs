#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Exercise lax and strict parsers against arbitrary data
    let _ = ironcrab::solana::dex::orca_whirlpool_layout::parse_whirlpool(data);
    let _ = ironcrab::solana::dex::orca_whirlpool_layout::parse_whirlpool_strict(data);
});
