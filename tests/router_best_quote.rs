use ironcrab::execution::live_pool_cache::{
    CachedPoolState, LivePoolCache, OrcaWhirlpoolState, SharedLivePoolCache,
};
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

    // Orca with LivePoolCache (GEYSER-FIRST): cache provides vault reserves, no RPC in Hot Path
    let base = Pubkey::new_from_array([3u8; 32]);
    let quote = Pubkey::new_from_array([4u8; 32]);
    let pool_addr = base; // insert_mock_pool uses base as pool key
    let cache: SharedLivePoolCache = Arc::new(LivePoolCache::new());
    cache.upsert(
        pool_addr,
        CachedPoolState::Orca(OrcaWhirlpoolState {
            token_mint_a: base,
            token_mint_b: quote,
            token_vault_a: Pubkey::new_unique(),
            token_vault_b: Pubkey::new_unique(),
            tick_current_index: 0,
            sqrt_price: 1,
            liquidity: 1,
            fee_rate: 300,
            protocol_fee_rate: 0,
            tick_spacing: 64,
            vault_a_balance: Some(1_000_000_000),
            vault_b_balance: Some(2_000_000_000),
            token_a_program: None,
            token_b_program: None,
        }),
        100,
    );
    let orca = Arc::new(Orca::new_with_cache(rpc.clone(), None, Some(cache), false));
    orca.inject_cached_orca_state(
        &pool_addr,
        &OrcaWhirlpoolState {
            token_mint_a: base,
            token_mint_b: quote,
            token_vault_a: Pubkey::new_unique(),
            token_vault_b: Pubkey::new_unique(),
            tick_current_index: 0,
            sqrt_price: 1,
            liquidity: 1,
            fee_rate: 300,
            protocol_fee_rate: 0,
            tick_spacing: 64,
            vault_a_balance: Some(1_000_000_000),
            vault_b_balance: Some(2_000_000_000),
            token_a_program: None,
            token_b_program: None,
        },
    )
    .expect("inject_cached_orca_state");

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
