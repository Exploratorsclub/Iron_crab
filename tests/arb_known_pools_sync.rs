//! Arb-strategy `known_pools` sync from JetStream PoolCacheUpdate (BalanceUpdated path).

use ironcrab::arbitrage::{
    populate_arb_slave_from_live_pool_cache, sync_arb_slave_from_pool_cache_update,
    MultiHopArbitrage, MultiHopConfig,
};
use ironcrab::execution::live_pool_cache::create_shared_cache;
use ironcrab::ipc::PoolCacheUpdate;
use parking_lot::RwLock;
use solana_sdk::pubkey::Pubkey;
use std::collections::HashSet;

const TEST_COMPONENT: &str = "test";
const TEST_BUILD: &str = "0.0.0";
const TEST_RUN: &str = "run-test";

fn empty_known_pools() -> RwLock<HashSet<String>> {
    RwLock::new(HashSet::new())
}

#[test]
fn balance_updated_only_populates_known_pools() {
    let cache = create_shared_cache();
    let known_pools = empty_known_pools();
    let multi_hop = MultiHopArbitrage::new(MultiHopConfig::default(), cache.clone());

    let pool = Pubkey::new_unique().to_string();
    let base_mint = Pubkey::new_unique().to_string();
    let quote_mint = "So11111111111111111111111111111111111111112";

    let update = PoolCacheUpdate::new_balance_updated(
        TEST_COMPONENT,
        TEST_BUILD,
        TEST_RUN,
        pool.to_string(),
        "raydium".to_string(),
        base_mint.to_string(),
        quote_mint.to_string(),
        1_000_000,
        2_000_000,
        42,
    );

    assert!(sync_arb_slave_from_pool_cache_update(
        &cache,
        &known_pools,
        &multi_hop,
        &update,
    ));
    assert!(known_pools.read().contains(&pool));
}

#[test]
fn pool_removed_evicts_known_pools_entry() {
    let cache = create_shared_cache();
    let known_pools = empty_known_pools();
    let multi_hop = MultiHopArbitrage::new(MultiHopConfig::default(), cache.clone());

    let pool = Pubkey::new_unique().to_string();
    let base_mint = Pubkey::new_unique().to_string();
    let quote_mint = "So11111111111111111111111111111111111111112";

    let discovered = PoolCacheUpdate::new_pool_discovered(
        TEST_COMPONENT,
        TEST_BUILD,
        TEST_RUN,
        pool.to_string(),
        "orca".to_string(),
        base_mint.to_string(),
        quote_mint.to_string(),
        1_000_000,
        2_000_000,
        None,
        1,
    );
    sync_arb_slave_from_pool_cache_update(&cache, &known_pools, &multi_hop, &discovered);
    assert!(known_pools.read().contains(&pool));

    let removed = PoolCacheUpdate::new_pool_removed(
        TEST_COMPONENT,
        TEST_BUILD,
        TEST_RUN,
        pool.to_string(),
        "orca".to_string(),
        2,
    );
    sync_arb_slave_from_pool_cache_update(&cache, &known_pools, &multi_hop, &removed);
    assert!(!known_pools.read().contains(&pool));
}

#[test]
fn pool_discovered_then_balance_updated_keeps_single_known_pools_entry() {
    let cache = create_shared_cache();
    let known_pools = empty_known_pools();
    let multi_hop = MultiHopArbitrage::new(MultiHopConfig::default(), cache.clone());

    let pool = Pubkey::new_unique().to_string();
    let base_mint = Pubkey::new_unique().to_string();
    let quote_mint = "So11111111111111111111111111111111111111112";

    let discovered = PoolCacheUpdate::new_pool_discovered(
        TEST_COMPONENT,
        TEST_BUILD,
        TEST_RUN,
        pool.to_string(),
        "meteora_dlmm".to_string(),
        base_mint.to_string(),
        quote_mint.to_string(),
        500_000,
        600_000,
        None,
        10,
    );
    sync_arb_slave_from_pool_cache_update(&cache, &known_pools, &multi_hop, &discovered);

    let balance = PoolCacheUpdate::new_balance_updated(
        TEST_COMPONENT,
        TEST_BUILD,
        TEST_RUN,
        pool.to_string(),
        "meteora_dlmm".to_string(),
        base_mint.to_string(),
        quote_mint.to_string(),
        700_000,
        800_000,
        11,
    );
    sync_arb_slave_from_pool_cache_update(&cache, &known_pools, &multi_hop, &balance);

    let pools: Vec<_> = known_pools.read().iter().cloned().collect();
    assert_eq!(pools.len(), 1);
    assert_eq!(pools[0], pool);
}

#[test]
fn balance_updated_upserts_multi_hop_idempotently() {
    let cache = create_shared_cache();
    let known_pools = empty_known_pools();
    let multi_hop = MultiHopArbitrage::new(MultiHopConfig::default(), cache.clone());

    let pool = Pubkey::new_unique().to_string();
    let base_mint = Pubkey::new_unique().to_string();
    let quote_mint = "So11111111111111111111111111111111111111112";

    let balance = PoolCacheUpdate::new_balance_updated(
        TEST_COMPONENT,
        TEST_BUILD,
        TEST_RUN,
        pool.to_string(),
        "raydium_cpmm".to_string(),
        base_mint.to_string(),
        quote_mint.to_string(),
        1_000_000,
        2_000_000,
        5,
    );
    sync_arb_slave_from_pool_cache_update(&cache, &known_pools, &multi_hop, &balance);
    let stats_after_first = multi_hop.stats();

    sync_arb_slave_from_pool_cache_update(&cache, &known_pools, &multi_hop, &balance);
    let stats_after_second = multi_hop.stats();

    assert_eq!(stats_after_first.graph_pools, 1);
    assert_eq!(stats_after_second.graph_pools, 1);
    assert!(known_pools.read().contains(&pool));
}

#[test]
fn populate_from_live_cache_matches_cache_len() {
    let cache = create_shared_cache();
    let known_pools = empty_known_pools();
    let multi_hop = MultiHopArbitrage::new(MultiHopConfig::default(), cache.clone());

    let pool = Pubkey::new_unique().to_string();
    let update = PoolCacheUpdate::new_balance_updated(
        TEST_COMPONENT,
        TEST_BUILD,
        TEST_RUN,
        pool.clone(),
        "orca".to_string(),
        Pubkey::new_unique().to_string(),
        "So11111111111111111111111111111111111111112".to_string(),
        1,
        2,
        1,
    );
    sync_arb_slave_from_pool_cache_update(&cache, &known_pools, &multi_hop, &update);

    known_pools.write().clear();
    let count = populate_arb_slave_from_live_pool_cache(&cache, &known_pools, &multi_hop);
    assert_eq!(count, cache.len());
    assert!(known_pools.read().contains(&pool));
}
