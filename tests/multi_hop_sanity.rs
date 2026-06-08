//! Multi-hop profit / return_bps sanity (shadow-mode signal quality).

use ironcrab::arbitrage::{
    clamp_edge_ratio, profit_to_return_bps, BeamCycleFinder, CycleFinderConfig, DexType,
    MockQuoteProvider, PoolEdge, PoolGraph, PoolRanker, SearchNode, MAX_CYCLE_PROFIT_MULTIPLIER,
    MAX_RETURN_BPS,
};
use solana_sdk::pubkey::Pubkey;

fn test_pubkey(byte: u8) -> Pubkey {
    Pubkey::new_from_array([byte; 32])
}

#[test]
fn return_bps_never_i32_max_on_extreme_profit() {
    let wsol = test_pubkey(0x01);
    let mut node = SearchNode::start(wsol);
    node.profit = 1e15;
    node.path = vec![wsol, test_pubkey(0x02), wsol];
    node.pools = vec![vec![], vec![]];

    let cycle = node.to_arb_cycle().expect("closed cycle");
    assert_ne!(cycle.estimated_return_bps, i32::MAX);
    assert_eq!(cycle.estimated_return_bps, MAX_RETURN_BPS);
    assert!(!cycle.is_trustworthy_profit_estimate());
}

#[test]
fn extreme_edge_ratio_hop_rejected_by_clamp() {
    let ratio = clamp_edge_ratio(500.0);
    assert!((ratio - 1.05).abs() < f64::EPSILON);

    let wsol = test_pubkey(0x01);
    let token = test_pubkey(0x02);
    let start = SearchNode::start(wsol);
    let edge = PoolEdge::new(
        test_pubkey(0x10),
        DexType::RaydiumAmmV4,
        wsol,
        token,
        10_000.0,
        30,
    );
    let expanded = start.expand(token, vec![edge], 500.0, 1.0, 10_000.0);
    assert!(expanded.profit <= MAX_CYCLE_PROFIT_MULTIPLIER);
    assert!(expanded.edge_ratio_clamped || expanded.profit_multiplier_capped);
}

#[test]
fn find_cycles_never_emits_i32_max_return_bps() {
    let graph = PoolGraph::new();
    let mut mock = MockQuoteProvider::new();
    let wsol = test_pubkey(0x01);
    let usdc = test_pubkey(0x02);
    let token_a = test_pubkey(0x03);
    let probe = 10_000_000u64;

    let p1 = test_pubkey(0x10);
    let p2 = test_pubkey(0x11);
    let p3 = test_pubkey(0x12);

    graph.upsert_pool(PoolEdge::new(
        p1,
        DexType::RaydiumAmmV4,
        wsol,
        usdc,
        1_000_000.0,
        30,
    ));
    graph.upsert_pool(PoolEdge::new(
        p2,
        DexType::RaydiumAmmV4,
        usdc,
        token_a,
        500_000.0,
        30,
    ));
    graph.upsert_pool(PoolEdge::new(
        p3,
        DexType::RaydiumAmmV4,
        token_a,
        wsol,
        300_000.0,
        30,
    ));

    mock.add_quote(p1, wsol, usdc, probe, probe * 200);
    mock.add_quote(p2, usdc, token_a, probe, probe * 200);
    mock.add_quote(p3, token_a, wsol, probe, probe * 200);

    let config = CycleFinderConfig {
        min_profit_bps: -1000,
        base_mint: wsol,
        ..Default::default()
    };
    let finder = BeamCycleFinder::new(config, PoolRanker::new(mock));
    let cycles = finder.find_cycles(&graph);

    for cycle in &cycles {
        assert_ne!(
            cycle.estimated_return_bps,
            i32::MAX,
            "return_bps must be clamped, not overflow cast"
        );
        assert!(cycle.estimated_return_bps <= MAX_RETURN_BPS);
    }
}

#[test]
fn two_hop_sol_token_sol_plausible_return_bps() {
    let graph = PoolGraph::new();
    let mut mock = MockQuoteProvider::new();
    let wsol = test_pubkey(0x01);
    let token = test_pubkey(0x02);
    let probe = 10_000_000u64;

    let pool_out = test_pubkey(0x10);
    let pool_back = test_pubkey(0x11);

    graph.upsert_pool(PoolEdge::new(
        pool_out,
        DexType::RaydiumAmmV4,
        wsol,
        token,
        500_000.0,
        30,
    ));
    graph.upsert_pool(PoolEdge::new(
        pool_back,
        DexType::RaydiumAmmV4,
        token,
        wsol,
        500_000.0,
        30,
    ));

    // ~2% edge per hop → ~4% round-trip (before sanity caps)
    mock.add_quote(pool_out, wsol, token, probe, 10_200_000);
    mock.add_quote(pool_back, token, wsol, probe, 10_200_000);

    let config = CycleFinderConfig {
        min_profit_bps: 30,
        base_mint: wsol,
        max_hops: 3,
        ..Default::default()
    };
    let finder = BeamCycleFinder::new(config, PoolRanker::new(mock));
    let cycles = finder.find_cycles(&graph);

    assert!(!cycles.is_empty(), "expected profitable 2-hop WSOL cycle");
    let best = &cycles[0];
    assert!(best.estimated_return_bps >= 30);
    assert_ne!(best.estimated_return_bps, i32::MAX);
    assert!(best.estimated_return_bps <= MAX_RETURN_BPS);
}

#[test]
fn profit_to_return_bps_clamp_semantics() {
    let (bps, sat) = profit_to_return_bps(100.0);
    assert!(sat);
    assert_eq!(bps, MAX_RETURN_BPS);
    let (bps2, sat2) = profit_to_return_bps(1.005);
    assert!(!sat2);
    assert_eq!(bps2, 50);
}
