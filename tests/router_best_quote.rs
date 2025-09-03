use std::sync::Arc;
use ironcrab::solana::dex::{raydium::Raydium, orca::Orca, router::Router, Dex};
use ironcrab::solana::rpc::SolanaRpc;
use solana_sdk::pubkey::Pubkey;

// Basic best-quote selection test using mocked Orca pool and empty Raydium
#[tokio::test]
async fn router_picks_higher_out_amount() {
    let rpc = Arc::new(SolanaRpc::new("https://api.mainnet-beta.solana.com")); // URL irrelevant for mock
    let raydium = Arc::new(Raydium::new(rpc.clone()));
    let orca = Arc::new(Orca::new(rpc.clone()));
    // Insert mock pool into Orca with deterministic reserves
    let base = Pubkey::new_from_array([3u8;32]);
    let quote = Pubkey::new_from_array([4u8;32]);
    orca.insert_mock_pool(base, quote, 1_000_000_000u128, 2_000_000_000u128, 30);

    // Refresh connectors (Raydium stays empty)
    raydium.refresh_pools().await.unwrap();
    orca.refresh_pools().await.unwrap(); // leaves our manual insert intact

    let router = Router::new(vec![raydium.clone() as Arc<dyn Dex>, orca.clone() as Arc<dyn Dex>]);
    let amount_in = 10_000u64;
    let best = router.best_quote_exact_in(&base.to_string(), &quote.to_string(), amount_in).await.unwrap();
    assert!(best.is_some(), "expected a quote");
    let rq = best.unwrap();
    assert_eq!(rq.dex_index, 1, "Orca (index 1) should win with sole pool");
    assert!(rq.quote.amount_out > 0);
}
