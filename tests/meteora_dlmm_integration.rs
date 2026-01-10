//! Integration test: Meteora DLMM pool refresh and quote

use ironcrab::solana::dex::meteora_dlmm::MeteoraDlmm;
use ironcrab::solana::dex::Dex;
use ironcrab::solana::rpc::SolanaRpc;
use std::sync::Arc;

#[tokio::test]
#[ignore] // Requires mainnet RPC
async fn test_meteora_dlmm_refresh() {
    let rpc_url = std::env::var("RPC_URL")
        .unwrap_or_else(|_| "https://api.mainnet-beta.solana.com".to_string());

    let rpc = Arc::new(SolanaRpc::new(&rpc_url));
    let meteora = MeteoraDlmm::new(rpc);

    // Refresh pools from mainnet
    meteora.refresh_pools().await.expect("Refresh failed");

    let pools = meteora.list_pools();
    println!("Found {} DLMM pools", pools.len());

    assert!(
        !pools.is_empty(),
        "Should find at least one Meteora DLMM pool"
    );

    // List trading pairs
    let pairs = meteora.list_pairs();
    println!("Available pairs: {}", pairs.len());

    for (i, (mint_a, mint_b)) in pairs.iter().take(5).enumerate() {
        println!("  {}. {} <-> {}", i + 1, mint_a, mint_b);
    }
}

#[tokio::test]
#[ignore] // Requires mainnet RPC
async fn test_meteora_dlmm_quote_wsol_usdc() {
    let rpc_url = std::env::var("RPC_URL")
        .unwrap_or_else(|_| "https://api.mainnet-beta.solana.com".to_string());

    let rpc = Arc::new(SolanaRpc::new(&rpc_url));
    let meteora = MeteoraDlmm::new(rpc);

    meteora.refresh_pools().await.expect("Refresh failed");

    // Try to quote WSOL -> USDC (1 SOL = 1_000_000_000 lamports)
    let wsol = "So11111111111111111111111111111111111111112";
    let usdc = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";

    let quote = meteora
        .quote_exact_in(wsol, usdc, 1_000_000_000)
        .await
        .expect("Quote failed");

    if let Some(q) = quote {
        println!("Quote for 1 SOL -> USDC:");
        println!("  Amount out: {} (~${:.2})", q.amount_out, q.amount_out as f64 / 1_000_000.0);
        println!("  Price impact: {} bps", q.price_impact_bps);
        println!("  Fee: {} bps", q.fee_bps);
        println!("  Route: {:?}", q.route);

        assert!(q.amount_out > 0, "Should get positive amount out");
    } else {
        println!("No WSOL-USDC pool found");
    }
}
