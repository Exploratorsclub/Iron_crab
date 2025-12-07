//! Test Pump.fun with real tokens currently trading on pump.fun
//! 
//! Test tokens:
//! - 2hyv4QRUEh1skwFfZi6SQU8DzAEts4b73G94gBUepump (9min old, 50% bonding curve)
//! - 2ZtDiM6sUCCfMexmuRsSXTCzgBe8qKba3mqKwnJ9pump (52min old, 100% bonding curve)

use anyhow::Result;
use ironcrab::solana::dex::pumpfun::PumpFunDex;
use ironcrab::solana::dex::Dex;
use ironcrab::solana::rpc::SolanaRpc;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::sync::Arc;

async fn test_token(rpc: &Arc<SolanaRpc>, token_mint: &str, description: &str) -> Result<()> {
    println!("\n{}", "=".repeat(60));
    println!("Testing: {}", description);
    println!("Token: {}", token_mint);
    println!("{}", "=".repeat(60));
    
    let pumpfun = PumpFunDex::new(rpc.clone())?;
    let token_pubkey = Pubkey::from_str(token_mint)?;
    
    // Step 1: Check if token mint exists
    println!("\n[1/4] Checking token mint existence...");
    match rpc.get_account_retry(&token_pubkey).await {
        Ok(account) => {
            println!("✅ Token mint exists");
            println!("   Owner: {}", account.owner);
            println!("   Lamports: {}", account.lamports);
            println!("   Data length: {} bytes", account.data.len());
        }
        Err(e) => {
            println!("❌ Token mint NOT found: {}", e);
            return Ok(());
        }
    }
    
    // Step 2: Derive and check bonding curve
    println!("\n[2/4] Deriving bonding curve PDA...");
    let (bonding_curve, bump) = pumpfun.derive_bonding_curve(&token_pubkey);
    println!("   Bonding Curve: {}", bonding_curve);
    println!("   Bump: {}", bump);
    
    // Step 3: Fetch bonding curve state
    println!("\n[3/4] Fetching bonding curve state...");
    match pumpfun.fetch_bonding_curve(&bonding_curve).await {
        Ok(state) => {
            println!("✅ Bonding curve found!");
            println!("   Virtual SOL: {} lamports ({:.4} SOL)", 
                state.virtual_sol_reserves, 
                state.virtual_sol_reserves as f64 / 1e9
            );
            println!("   Virtual Token: {}", state.virtual_token_reserves);
            println!("   Real SOL: {} lamports ({:.4} SOL)", 
                state.real_sol_reserves,
                state.real_sol_reserves as f64 / 1e9
            );
            println!("   Real Token: {}", state.real_token_reserves);
            println!("   Complete: {}", state.complete);
            
            // Calculate bonding curve progress
            let progress = if state.virtual_sol_reserves > 0 {
                (state.real_sol_reserves as f64 / state.virtual_sol_reserves as f64) * 100.0
            } else {
                0.0
            };
            println!("   Bonding Curve Progress: {:.2}%", progress);
        }
        Err(e) => {
            println!("❌ Bonding curve NOT found: {}", e);
            return Ok(());
        }
    }
    
    // Step 4: Test quote
    println!("\n[4/4] Testing quote for 0.01 SOL buy...");
    let sol_mint = "So11111111111111111111111111111111111111112";
    let amount_in = 10_000_000; // 0.01 SOL
    
    match pumpfun.quote_exact_in(sol_mint, token_mint, amount_in).await? {
        Some(quote) => {
            println!("✅ Quote successful!");
            println!("   Input: {} lamports (0.01 SOL)", amount_in);
            println!("   Output: {} tokens", quote.amount_out);
            println!("   Price Impact: {:.2} bps", quote.price_impact_bps);
            println!("   Fee: {:.2} bps", quote.fee_bps);
        }
        None => {
            println!("❌ Quote returned None (no route available)");
        }
    }
    
    Ok(())
}

#[tokio::test]
async fn test_pumpfun_with_real_tokens() -> Result<()> {
    let rpc_url = std::env::var("SOLANA_RPC_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:8899".to_string());
    
    println!("\n🚀 Testing Pump.fun with real tokens");
    println!("RPC: {}", rpc_url);
    
    let rpc = Arc::new(SolanaRpc::new(&rpc_url));
    
    // Test Token 1: 50% bonding curve
    test_token(
        &rpc,
        "2hyv4QRUEh1skwFfZi6SQU8DzAEts4b73G94gBUepump",
        "Token 1 - 9min old, 50% bonding curve"
    ).await?;
    
    // Test Token 2: 100% bonding curve (migrated to Raydium?)
    test_token(
        &rpc,
        "2ZtDiM6sUCCfMexmuRsSXTCzgBe8qKba3mqKwnJ9pump",
        "Token 2 - 52min old, 100% bonding curve"
    ).await?;
    
    println!("\n{}", "=".repeat(60));
    println!("✅ All tests complete!");
    println!("{}\n", "=".repeat(60));
    
    Ok(())
}
