//! Arb-strategy SLAVE sync: `known_pools` + multi-hop graph from JetStream `PoolCacheUpdate`.
//!
//! Mirrors execution-engine `pool_cache_sync` apply path; arb keeps a `HashSet` gate for 2-hop
//! (`check_arbitrage`) without Pubkey parsing on every comparison.

use parking_lot::RwLock;
use solana_sdk::pubkey::Pubkey;
use spl_token::native_mint;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::arbitrage::MultiHopArbitrage;
use crate::execution::live_pool_cache::{CachedPoolState, LivePoolCache};
use crate::execution::pool_cache_sync::apply_pool_cache_update;
use crate::ipc::{PoolCacheUpdate, PoolCacheUpdateType};

static ARB_KNOWN_POOLS_SYNCED_BOOTSTRAP: AtomicU64 = AtomicU64::new(0);
static ARB_KNOWN_POOLS_SYNCED_INCREMENTAL: AtomicU64 = AtomicU64::new(0);

/// Prometheus-visible totals (optional heartbeat/debug).
pub fn arb_known_pools_synced_bootstrap_total() -> u64 {
    ARB_KNOWN_POOLS_SYNCED_BOOTSTRAP.load(Ordering::Relaxed)
}

pub fn arb_known_pools_synced_incremental_total() -> u64 {
    ARB_KNOWN_POOLS_SYNCED_INCREMENTAL.load(Ordering::Relaxed)
}

fn liquidity_usd_from_state(state: &CachedPoolState) -> f64 {
    let (base, quote) = match state {
        CachedPoolState::Orca(s) => (
            s.vault_a_balance.unwrap_or(0),
            s.vault_b_balance.unwrap_or(0),
        ),
        CachedPoolState::RaydiumAmm(s) => (s.coin_reserve.unwrap_or(0), s.pc_reserve.unwrap_or(0)),
        CachedPoolState::RaydiumCpmm(s) => (s.reserve_0.unwrap_or(0), s.reserve_1.unwrap_or(0)),
        CachedPoolState::Meteora(s) => (
            s.reserve_x_balance.unwrap_or(0),
            s.reserve_y_balance.unwrap_or(0),
        ),
        CachedPoolState::PumpAmm(s) => (s.base_reserve.unwrap_or(0), s.quote_reserve.unwrap_or(0)),
        CachedPoolState::PumpFun(s) => (s.virtual_token_reserves, s.virtual_sol_reserves),
        CachedPoolState::MeteoraCpmm(s) => (s.reserve_0, s.reserve_1),
    };
    let sol_side = base.max(quote) as f64 / 1e9 * 150.0;
    sol_side.max(10_000.0)
}

fn mints_from_state(state: &CachedPoolState) -> (Pubkey, Pubkey) {
    match state {
        CachedPoolState::Orca(s) => (s.token_mint_a, s.token_mint_b),
        CachedPoolState::RaydiumAmm(s) => (s.base_mint, s.quote_mint),
        CachedPoolState::RaydiumCpmm(s) => (s.token_0_mint, s.token_1_mint),
        CachedPoolState::Meteora(s) => (s.token_x_mint, s.token_y_mint),
        CachedPoolState::MeteoraCpmm(s) => (s.token_0_mint, s.token_1_mint),
        CachedPoolState::PumpFun(s) => (
            s.token_mint,
            Pubkey::new_from_array(native_mint::id().to_bytes()),
        ),
        CachedPoolState::PumpAmm(s) => (s.base_mint, s.quote_mint),
    }
}

fn upsert_multi_hop_from_cache(
    multi_hop: &MultiHopArbitrage,
    live_pool_cache: &LivePoolCache,
    pool_address: &str,
) {
    let Ok(pool_pk) = pool_address.parse::<Pubkey>() else {
        return;
    };
    let Some(state) = live_pool_cache.get(&pool_pk) else {
        return;
    };
    let (mint_a, mint_b) = mints_from_state(&state);
    multi_hop.upsert_pool(
        pool_address,
        state.dex_name(),
        &mint_a.to_string(),
        &mint_b.to_string(),
        liquidity_usd_from_state(&state),
        30,
    );
}

/// Apply JetStream update to SLAVE `LivePoolCache` and sync arb indexes (`known_pools`, multi-hop).
///
/// Returns `true` when `apply_pool_cache_update` modified the cache (PoolDiscovered / BalanceUpdated).
pub fn sync_arb_slave_from_pool_cache_update(
    live_pool_cache: &LivePoolCache,
    known_pools: &RwLock<HashSet<String>>,
    multi_hop: &MultiHopArbitrage,
    update: &PoolCacheUpdate,
) -> bool {
    match update.update_type {
        PoolCacheUpdateType::PoolRemoved => {
            known_pools.write().remove(&update.pool_address);
            multi_hop.remove_pool(&update.pool_address);
            false
        }
        PoolCacheUpdateType::PoolDiscovered | PoolCacheUpdateType::BalanceUpdated => {
            let applied = apply_pool_cache_update(live_pool_cache, update);
            if applied {
                known_pools.write().insert(update.pool_address.clone());
                upsert_multi_hop_from_cache(multi_hop, live_pool_cache, &update.pool_address);
                ARB_KNOWN_POOLS_SYNCED_INCREMENTAL.fetch_add(1, Ordering::Relaxed);
            }
            applied
        }
    }
}

/// Rebuild `known_pools` and multi-hop graph from SLAVE `LivePoolCache` after JetStream bootstrap.
pub fn populate_arb_slave_from_live_pool_cache(
    live_pool_cache: &LivePoolCache,
    known_pools: &RwLock<HashSet<String>>,
    multi_hop: &MultiHopArbitrage,
) -> usize {
    let mut pools = known_pools.write();
    pools.clear();
    let mut count = 0usize;
    for (pool_pk, state) in live_pool_cache.iter() {
        pools.insert(pool_pk.to_string());
        let (mint_a, mint_b) = mints_from_state(&state);
        multi_hop.upsert_pool(
            &pool_pk.to_string(),
            state.dex_name(),
            &mint_a.to_string(),
            &mint_b.to_string(),
            liquidity_usd_from_state(&state),
            30,
        );
        multi_hop.touch_live_pool_quote_ready(&pool_pk.to_string());
        count += 1;
    }
    ARB_KNOWN_POOLS_SYNCED_BOOTSTRAP.store(count as u64, Ordering::Relaxed);
    count
}
