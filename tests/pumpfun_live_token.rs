//! Test Pump.fun integration with a real live token
//! Token: AA8Sb5tu2bvLWR2wJ8ueL1MnVAWeCDVQrPhJqog9pump

use anyhow::Result;
use ironcrab::solana::dex::pumpfun::PumpFunDex;
use ironcrab::solana::dex::Dex; // Trait needed for quote_exact_in method
use ironcrab::solana::rpc::SolanaRpc;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::sync::Arc;

#[tokio::test]
async fn test_pumpfun_live_token_quote() -> Result<()> {
    // Real token that just launched with 20% gain
    let token_mint = "AA8Sb5tu2bvLWR2wJ8ueL1MnVAWeCDVQrPhJqog9pump";
    let sol_mint = "So11111111111111111111111111111111111111112";
    
    // Connect to RPC
    let rpc_url = std::env::var("SOLANA_RPC_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:8899".to_string());
    
    println!("Connecting to RPC: {}", rpc_url);
    let rpc = Arc::new(SolanaRpc::new(&rpc_url));
    
    // Create PumpFun instance
    let pumpfun = PumpFunDex::new(rpc.clone())?;
    
    // Derive bonding curve address
    let token_pubkey = Pubkey::from_str(token_mint)?;
    let (bonding_curve, bump) = pumpfun.derive_bonding_curve(&token_pubkey);
    
    println!("\n=== Pump.fun Token Test ===");
    println!("Token Mint: {}", token_mint);
    println!("Bonding Curve: {}", bonding_curve);
    println!("Bump: {}", bump);
    
    // Try to fetch bonding curve account
    println!("\nFetching bonding curve account...");
    match pumpfun.fetch_bonding_curve(&bonding_curve).await {
        Ok(state) => {
            println!("✅ Bonding curve found!");
            println!("  Virtual SOL reserves: {}", state.virtual_sol_reserves);
            println!("  Virtual Token reserves: {}", state.virtual_token_reserves);
            println!("  Real SOL reserves: {}", state.real_sol_reserves);
            println!("  Real Token reserves: {}", state.real_token_reserves);
            println!("  Complete (migrated): {}", state.complete);
            
            // Try to get a quote for 0.01 SOL
            let amount_in = 10_000_000; // 0.01 SOL in lamports
            println!("\n=== Testing Quote ===");
            println!("Input: {} lamports (0.01 SOL)", amount_in);
            
            match pumpfun.quote_exact_in(sol_mint, token_mint, amount_in).await? {
                Some(quote) => {
                    println!("✅ Quote successful!");
                    println!("  Amount out: {}", quote.amount_out);
                    println!("  Price impact: {:.2}%", quote.price_impact_bps as f64 / 100.0);
                    println!("  Fee: {:.2}%", quote.fee_bps as f64 / 100.0);
                    println!("  Route: {}", quote.route.join(" -> "));
                }
                None => {
                    println!("❌ Quote returned None");
                    return Err(anyhow::anyhow!("Expected quote but got None"));
                }
            }
        }
        Err(e) => {
            println!("❌ Failed to fetch bonding curve: {:?}", e);
            
            // Check if account exists at all
            match rpc.get_account_retry(&bonding_curve).await {
                Ok(acc) => {
                    println!("  Account exists with {} bytes", acc.data.len());
                    println!("  Owner: {}", acc.owner);
                }
                Err(e2) => {
                    println!("  Account does not exist: {:?}", e2);
                }
            }
            
            return Err(e);
        }
    }
    
    Ok(())
}

#[tokio::test]
async fn test_pumpfun_pda_derivation() -> Result<()> {
    // Test that our PDA derivation matches expected pattern
    let token_mint = "AA8Sb5tu2bvLWR2wJ8ueL1MnVAWeCDVQrPhJqog9pump";
    let rpc_url = std::env::var("SOLANA_RPC_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:8899".to_string());
    
    let rpc = Arc::new(SolanaRpc::new(&rpc_url));
    let pumpfun = PumpFunDex::new(rpc.clone())?;
    
    let token_pubkey = Pubkey::from_str(token_mint)?;
    let (bonding_curve, bump) = pumpfun.derive_bonding_curve(&token_pubkey);
    
    println!("\n=== PDA Derivation Test ===");
    println!("Token: {}", token_mint);
    println!("Derived Bonding Curve: {}", bonding_curve);
    println!("Bump: {}", bump);
    
    // Verify it's a valid PDA
    assert!(bump < 255, "Bump should be valid");
    
    // Try alternative derivation to verify
    let program_id = Pubkey::from_str("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P")?;
    let (derived, derived_bump) = Pubkey::find_program_address(
        &[b"bonding-curve", token_pubkey.as_ref()],
        &program_id,
    );
    
    println!("Alternative derivation: {}", derived);
    println!("Alternative bump: {}", derived_bump);
    
    assert_eq!(bonding_curve, derived, "PDA derivations should match");
    assert_eq!(bump, derived_bump, "Bumps should match");
    
    Ok(())
}
