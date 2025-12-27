// Quick verification script for creator_vault PDA
// Run with: cargo script verify_creator_vault.rs

use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;

const PUMPFUN_PROGRAM_ID: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";

fn main() {
    let program_id = Pubkey::from_str(PUMPFUN_PROGRAM_ID).unwrap();
    
    // SUCCESSFUL TX: Creator = BQrLU1kzhtvz5tTZ8mg1tMU9YKnx2VGanTkRvF8Xfi9v
    // Expected creator_vault = AVMG6LhEZZGcoUyVZeFSzkKayHHcRbSuoPE9whsnX4fY
    let creator_success = Pubkey::from_str("BQrLU1kzhtvz5tTZ8mg1tMU9YKnx2VGanTkRvF8Xfi9v").unwrap();
    let (vault_success, bump) = Pubkey::find_program_address(
        &[b"creator-vault", creator_success.as_ref()],
        &program_id
    );
    println!("SUCCESS TX:");
    println!("  Creator: {}", creator_success);
    println!("  Derived creator_vault: {} (bump: {})", vault_success, bump);
    println!("  Expected: AVMG6LhEZZGcoUyVZeFSzkKayHHcRbSuoPE9whsnX4fY");
    println!("  Match: {}", vault_success.to_string() == "AVMG6LhEZZGcoUyVZeFSzkKayHHcRbSuoPE9whsnX4fY");
    println!();
    
    // FAILED TX: Creator = 7c67ZUkJqXVTpoXdNGzDanYYeL1kkJKNx52HGphewxUs
    let creator_fail = Pubkey::from_str("7c67ZUkJqXVTpoXdNGzDanYYeL1kkJKNx52HGphewxUs").unwrap();
    let (vault_fail, bump2) = Pubkey::find_program_address(
        &[b"creator-vault", creator_fail.as_ref()],
        &program_id
    );
    println!("FAILED TX:");
    println!("  Creator: {}", creator_fail);
    println!("  Derived creator_vault: {} (bump: {})", vault_fail, bump2);
}
