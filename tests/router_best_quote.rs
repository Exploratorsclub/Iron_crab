use ironcrab::solana::dex::{orca::Orca, raydium::Raydium, router::Router, Dex};
use ironcrab::solana::rpc::SolanaRpc;
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;

// Basic best-quote selection test using mocked Orca pool and empty Raydium
#[tokio::test]
async fn router_picks_higher_out_amount() {
    // Use an invalid/dummy RPC URL to guarantee no real network dependency for this unit test.
    let rpc = Arc::new(SolanaRpc::new("http://localhost:0"));
    let raydium = Arc::new(Raydium::new(rpc.clone()));
    let orca = Arc::new(Orca::new(rpc.clone()));
    // Insert mock pool into Orca with deterministic reserves
    let base = Pubkey::new_from_array([3u8; 32]);
    let quote = Pubkey::new_from_array([4u8; 32]);
    orca.insert_mock_pool(base, quote, 1_000_000_000u128, 2_000_000_000u128, 30);

    // Skip refresh_pools to avoid network; we rely solely on the manually inserted mock pool.

    let router = Router::new(vec![
        raydium.clone() as Arc<dyn Dex>,
        orca.clone() as Arc<dyn Dex>,
    ]);
    let amount_in = 10_000u64;
    let best = router
        .best_quote_exact_in(&base.to_string(), &quote.to_string(), amount_in)
        .await
        .unwrap();
    assert!(best.is_some(), "expected a quote");
    let rq = best.unwrap();
    assert_eq!(rq.dex_index, 1, "Orca (index 1) should win with sole pool");
    assert!(rq.quote.amount_out > 0);
}
