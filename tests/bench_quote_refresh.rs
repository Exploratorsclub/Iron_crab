//! Lightweight timing test (not a criterion benchmark) to profile refresh & quote.
use std::sync::Arc;
use ironcrab::solana::{rpc::SolanaRpc, dex::{raydium::Raydium, orca::Orca, Dex}};
use solana_sdk::pubkey::Pubkey;

#[tokio::test]
async fn timing_refresh_and_quote() {
    // NOTE: Uses live RPC URL from env (if provided) else early return.
    let url = std::env::var("SOLANA_RPC_URL").unwrap_or_else(|_| "".into());
    if url.is_empty() { eprintln!("SKIP: set SOLANA_RPC_URL to run timing test"); return; }
    let rpc = Arc::new(SolanaRpc::new(&url));
    let ray = Arc::new(Raydium::new(rpc.clone()));
    let orca = Arc::new(Orca::new(rpc.clone()));
    let start = std::time::Instant::now();
    let _ = ray.refresh_pools().await; // ignore errors (timing only)
    let ray_ms = start.elapsed().as_millis();
    let start2 = std::time::Instant::now();
    let _ = orca.refresh_pools().await;
    let orca_ms = start2.elapsed().as_millis();
    eprintln!("refresh timings ms: raydium={} orca={}", ray_ms, orca_ms);
    // Attempt a random quote if any pairs exist
    if let Some((a,b)) = ray.list_pairs().into_iter().next() {
        let t0 = std::time::Instant::now();
        let _ = ray.quote_exact_in(&a, &b, 1_000_000).await; // 1 token w/6 decimals
        eprintln!("raydium single quote ms={} pair {}-{}", t0.elapsed().as_micros()/1000, &a, &b);
    }
    if let Some((a,b)) = orca.list_pairs().into_iter().next() {
        let t0 = std::time::Instant::now();
        let _ = orca.quote_exact_in(&a, &b, 1_000_000).await;
        eprintln!("orca single quote ms={} pair {}-{}", t0.elapsed().as_micros()/1000, &a, &b);
    }
}