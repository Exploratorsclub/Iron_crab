// Run with: cargo script verify_pda.rs
// Or add as a bin target

use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;

fn main() {
    // Pump.fun program ID
    let program_id = Pubkey::from_str("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P").unwrap();
    
    // Token mint from the failed transaction: "Jake"
    let token_mint = Pubkey::from_str("5VhWirxHD3akur1EDvayhHpj4Nzjf898CvyL8cuR0pump").unwrap();
    
    // Derive bonding curve PDA
    let (bonding_curve, bump) = Pubkey::find_program_address(
        &[b"bonding-curve", token_mint.as_ref()],
        &program_id
    );
    
    println!("Token Mint: {}", token_mint);
    println!("Derived Bonding Curve: {}", bonding_curve);
    println!("Bump: {}", bump);
    
    // Derive associated bonding curve
    let (associated_bc, abc_bump) = Pubkey::find_program_address(
        &[
            b"associated-bonding-curve",
            bonding_curve.as_ref(),
            token_mint.as_ref(),
        ],
        &program_id
    );
    
    println!("Derived Associated Bonding Curve: {}", associated_bc);
    println!("ABC Bump: {}", abc_bump);
}
