//! Multi-hop search worker coalescing and quote-ready beam pruning.

use ironcrab::arbitrage::{DexType, MockQuoteProvider, PoolEdge, PoolGraph, PoolRanker, WSOL_MINT};
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;

fn test_pubkey(byte: u8) -> Pubkey {
    Pubkey::new_from_array([byte; 32])
}

#[test]
fn expansion_skips_pools_outside_quote_ready_set() {
    let graph = PoolGraph::new();
    let mut mock = MockQuoteProvider::new();

    let wsol = Pubkey::from_str(WSOL_MINT).unwrap();
    let usdc = test_pubkey(0x02);
    let token_a = test_pubkey(0x03);
    let quoted_pool = test_pubkey(0x10);
    let unquoted_pool = test_pubkey(0x11);
    let probe = 10_000_000u64;

    graph.upsert_pool(PoolEdge::new(
        quoted_pool,
        DexType::RaydiumAmmV4,
        wsol,
        usdc,
        1_000_000.0,
        30,
    ));
    graph.upsert_pool(PoolEdge::new(
        unquoted_pool,
        DexType::RaydiumAmmV4,
        wsol,
        token_a,
        900_000.0,
        30,
    ));

    mock.add_quote(quoted_pool, wsol, usdc, probe, 9_800_000);
    let mock = std::sync::Arc::new(mock);
    mock.reset_probe_lookup_count();
    let ranker = PoolRanker::new(mock.clone());
    let ranked = ranker.rank_pools_from(&graph, &wsol);

    assert_eq!(ranked.len(), 1);
    assert_eq!(ranked[0].0, usdc);
    assert_eq!(ranked[0].1.len(), 1);
    assert_eq!(ranked[0].1[0].edge.pool_address, quoted_pool);
    assert!(
        mock.probe_lookup_count() <= 2,
        "only quote-ready pool should be probed, got {}",
        mock.probe_lookup_count()
    );
}
