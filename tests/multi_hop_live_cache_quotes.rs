//! Multi-hop quotes from LivePoolCache (no trade events required).

use ironcrab::arbitrage::{
    CachedQuoteProvider, DexType, PoolEdge, PoolGraph, PoolRanker, QuoteProvider, WSOL_MINT,
};
use ironcrab::execution::live_pool_cache::{create_shared_cache, CachedPoolState, RaydiumAmmState};
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

fn wsol() -> Pubkey {
    Pubkey::from_str(WSOL_MINT).unwrap()
}

#[test]
fn live_pool_cache_quotes_rank_both_directions_without_trades() {
    let cache = create_shared_cache();
    let provider = Arc::new(CachedQuoteProvider::new(
        Duration::from_secs(30),
        cache.clone(),
    ));
    let wsol_mint = wsol();
    let token = Pubkey::new_unique();
    let pool = Pubkey::new_unique();
    let probe = 10_000_000u64;

    cache.upsert(
        pool,
        CachedPoolState::RaydiumAmm(RaydiumAmmState {
            base_mint: token,
            quote_mint: wsol_mint,
            coin_vault: Pubkey::new_unique(),
            pc_vault: Pubkey::new_unique(),
            base_decimals: 9,
            quote_decimals: 9,
            coin_reserve: Some(10_000_000_000_000),
            pc_reserve: Some(1_000_000_000_000),
            market_id: Pubkey::new_unique(),
            serum_bids: None,
            serum_asks: None,
            serum_event_queue: None,
        }),
        1,
    );

    let graph = PoolGraph::new();
    graph.upsert_pool(PoolEdge::new(
        pool,
        DexType::RaydiumAmmV4,
        wsol_mint,
        token,
        500_000.0,
        30,
    ));

    let ranker = PoolRanker::new(provider.clone());
    assert!(
        !ranker.rank_pools_from(&graph, &wsol_mint).is_empty(),
        "WSOL→token should rank via LivePoolCache"
    );
    assert!(
        !ranker.rank_pools_from(&graph, &token).is_empty(),
        "token→WSOL should rank via LivePoolCache"
    );

    let default_out = (probe as f64 * 0.99) as u64;
    let wsol_out = provider
        .get_cached_probe_quote(&pool, DexType::RaydiumAmmV4, &wsol_mint, &token, probe)
        .expect("WSOL→token quote");
    assert_ne!(wsol_out, default_out);
    let token_out = provider
        .get_cached_probe_quote(&pool, DexType::RaydiumAmmV4, &token, &wsol_mint, probe)
        .expect("token→WSOL quote");
    assert_ne!(token_out, default_out);
}
