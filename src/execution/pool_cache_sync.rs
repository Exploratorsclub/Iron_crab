//! Shared Pool Cache Sync — JetStream Bootstrap & Incremental Updates
//!
//! This module provides shared functions for building and maintaining a SLAVE
//! LivePoolCache from JetStream PoolCacheUpdate events. Used by both
//! execution-engine and momentum-bot.
//!
//! # Architecture
//!
//! ```text
//! market-data (MASTER LivePoolCache)
//!     │
//!     ├── publishes PoolCacheUpdate on JetStream
//!     │
//!     ├──→ execution-engine (SLAVE LivePoolCache)
//!     │
//!     └──→ momentum-bot    (SLAVE LivePoolCache)
//! ```

use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use tracing::{debug, info, warn};

use crate::execution::live_pool_cache::{
    CachedPoolState, LivePoolCache, MeteoraState, OrcaWhirlpoolState, PumpAmmState, PumpFunState,
    RaydiumAmmState, RaydiumCpmmState,
};
use crate::ipc::{PoolCacheUpdate, PoolCacheUpdateType};
use crate::nats::{slave_consumer_config, NatsClient, STREAM_NAME};

/// Extract base_reserve and quote_reserve from CachedPoolState (for merge logic).
fn extract_reserves(state: &CachedPoolState) -> (u64, u64) {
    match state {
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
        CachedPoolState::MeteoraCpmm(_) => (0, 0),
    }
}

/// Build minimal CachedPoolState from PoolCacheUpdate (for JetStream bootstrap/sync)
///
/// Since PoolCacheUpdate only contains reserves (not full account data), we create
/// minimal state structures. Full account updates from Geyser will refresh these later.
///
/// Use `base_reserve` and `quote_reserve` when provided (for merge); otherwise uses update values.
pub fn build_minimal_pool_state(update: &PoolCacheUpdate) -> Option<(Pubkey, CachedPoolState)> {
    build_minimal_pool_state_with_reserves(update, update.base_reserve, update.quote_reserve)
}

/// Build minimal state with explicit reserves (used for BalanceUpdated merge).
fn build_minimal_pool_state_with_reserves(
    update: &PoolCacheUpdate,
    base_reserve: u64,
    quote_reserve: u64,
) -> Option<(Pubkey, CachedPoolState)> {
    let pool_addr = Pubkey::from_str(&update.pool_address).ok()?;
    let base_mint = Pubkey::from_str(&update.base_mint).ok()?;
    let quote_mint = Pubkey::from_str(&update.quote_mint).ok()?;

    // Build minimal state based on DEX type
    let state = match update.dex.as_str() {
        "orca" => CachedPoolState::Orca(OrcaWhirlpoolState {
            token_mint_a: base_mint,
            token_mint_b: quote_mint,
            token_vault_a: Pubkey::default(),
            token_vault_b: Pubkey::default(),
            tick_current_index: 0,
            sqrt_price: 0,
            liquidity: 0,
            fee_rate: 0,
            protocol_fee_rate: 0,
            tick_spacing: 0,
            vault_a_balance: Some(base_reserve),
            vault_b_balance: Some(quote_reserve),
            token_a_program: None,
            token_b_program: None,
        }),
        "raydium" => {
            let serum_bids = update
                .metadata
                .as_ref()
                .and_then(|m| m.get("serum_bids"))
                .and_then(|s| Pubkey::from_str(s).ok());
            let serum_asks = update
                .metadata
                .as_ref()
                .and_then(|m| m.get("serum_asks"))
                .and_then(|s| Pubkey::from_str(s).ok());
            let serum_event_queue = update
                .metadata
                .as_ref()
                .and_then(|m| m.get("serum_event_queue"))
                .and_then(|s| Pubkey::from_str(s).ok());
            let market_id = update
                .metadata
                .as_ref()
                .and_then(|m| m.get("market_id"))
                .and_then(|s| Pubkey::from_str(s).ok())
                .unwrap_or_default();

            CachedPoolState::RaydiumAmm(RaydiumAmmState {
                base_mint,
                quote_mint,
                coin_vault: Pubkey::default(),
                pc_vault: Pubkey::default(),
                base_decimals: 0,
                quote_decimals: 0,
                coin_reserve: Some(base_reserve),
                pc_reserve: Some(quote_reserve),
                market_id,
                serum_bids,
                serum_asks,
                serum_event_queue,
            })
        }
        "raydium_cpmm" => CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
            token_0_mint: base_mint,
            token_1_mint: quote_mint,
            token_0_vault: Pubkey::default(),
            token_1_vault: Pubkey::default(),
            reserve_0: Some(base_reserve),
            reserve_1: Some(quote_reserve),
        }),
        "meteora_cpmm" | "meteora_dlmm" => CachedPoolState::Meteora(MeteoraState {
            token_x_mint: base_mint,
            token_y_mint: quote_mint,
            reserve_x: Pubkey::default(),
            reserve_y: Pubkey::default(),
            active_id: 0,
            bin_step: 0,
            reserve_x_balance: Some(base_reserve),
            reserve_y_balance: Some(quote_reserve),
        }),
        "pump_amm" => {
            let creator = update
                .metadata
                .as_ref()
                .and_then(|m| m.get("creator"))
                .and_then(|s| Pubkey::from_str(s).ok());

            let pool_accounts: Vec<Pubkey> = update
                .metadata
                .as_ref()
                .and_then(|m| m.get("pool_accounts"))
                .map(|s| {
                    s.split(',')
                        .filter_map(|a| Pubkey::from_str(a).ok())
                        .collect()
                })
                .unwrap_or_default();

            CachedPoolState::PumpAmm(PumpAmmState {
                base_mint,
                quote_mint,
                pool_base_token_account: Pubkey::default(),
                pool_quote_token_account: Pubkey::default(),
                base_reserve: Some(base_reserve),
                quote_reserve: Some(quote_reserve),
                pool_accounts,
                creator,
            })
        }
        "pumpfun" => {
            let creator = update
                .metadata
                .as_ref()
                .and_then(|m| m.get("creator"))
                .and_then(|s| Pubkey::from_str(s).ok())
                .unwrap_or_default();

            let associated_bonding_curve = update
                .metadata
                .as_ref()
                .and_then(|m| m.get("associated_bonding_curve"))
                .and_then(|s| Pubkey::from_str(s).ok())
                .unwrap_or_default();

            let complete = update
                .metadata
                .as_ref()
                .and_then(|m| m.get("complete"))
                .map(|s| s == "true")
                .unwrap_or(false);

            let real_token_reserves = update
                .metadata
                .as_ref()
                .and_then(|m| m.get("real_token_reserves"))
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
            let real_sol_reserves = update
                .metadata
                .as_ref()
                .and_then(|m| m.get("real_sol_reserves"))
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);

            if creator != Pubkey::default() {
                debug!(
                    pool = %pool_addr,
                    creator = %creator,
                    real_token_reserves,
                    real_sol_reserves,
                    "SLAVE CACHE: PumpFun state from PoolCacheUpdate metadata"
                );
            }

            CachedPoolState::PumpFun(PumpFunState {
                token_mint: base_mint,
                bonding_curve: pool_addr,
                associated_bonding_curve,
                virtual_sol_reserves: quote_reserve,
                virtual_token_reserves: base_reserve,
                real_sol_reserves,
                real_token_reserves,
                complete,
                creator,
                cashback_enabled: update
                    .metadata
                    .as_ref()
                    .and_then(|m| m.get("cashback_enabled"))
                    .map(|v| v == "true")
                    .unwrap_or(false), // Resolved from JetStream metadata (propagated by market-data from Geyser)
            })
        }
        _ => {
            debug!(dex = %update.dex, "Unsupported DEX type for minimal state");
            return None;
        }
    };

    Some((pool_addr, state))
}

/// P3 #13: Apply base_decimals and quote_decimals from PoolCacheUpdate metadata to LivePoolCache.
fn apply_decimals_from_metadata(cache: &LivePoolCache, update: &PoolCacheUpdate) {
    let meta = match update.metadata.as_ref() {
        Some(m) if !m.is_empty() => m,
        _ => return,
    };
    if let (Ok(base_mint), Some(d)) = (
        Pubkey::from_str(&update.base_mint),
        meta.get("base_decimals").and_then(|s| s.parse::<u8>().ok()),
    ) {
        cache.set_mint_decimals(base_mint, d);
    }
    if let (Ok(quote_mint), Some(d)) = (
        Pubkey::from_str(&update.quote_mint),
        meta.get("quote_decimals")
            .and_then(|s| s.parse::<u8>().ok()),
    ) {
        cache.set_mint_decimals(quote_mint, d);
    }
}

/// Apply a single PoolCacheUpdate to a LivePoolCache.
///
/// For BalanceUpdated: merges partial updates (one vault at a time) with existing cache state.
/// If the update has only base or only quote (the other is 0), we preserve the existing value
/// for the other reserve to avoid "out=0" quote failures (e.g. meteora: missing reserves).
///
/// Returns true if the cache was modified.
pub fn apply_pool_cache_update(cache: &LivePoolCache, update: &PoolCacheUpdate) -> bool {
    match update.update_type {
        PoolCacheUpdateType::PoolDiscovered => {
            if let Some((pool_addr, mut minimal_state)) = build_minimal_pool_state(update) {
                if update.dex == "pump_amm" {
                    if let Some(existing) = cache.get(&pool_addr) {
                        if let (
                            CachedPoolState::PumpAmm(ref existing_pump),
                            CachedPoolState::PumpAmm(ref mut new_pump),
                        ) = (&existing, &mut minimal_state)
                        {
                            if new_pump.pool_accounts.is_empty()
                                && !existing_pump.pool_accounts.is_empty()
                            {
                                new_pump.pool_accounts = existing_pump.pool_accounts.clone();
                            }
                            if new_pump.creator.is_none() && existing_pump.creator.is_some() {
                                new_pump.creator = existing_pump.creator;
                            }
                        }
                    }
                }
                cache.upsert(pool_addr, minimal_state, update.geyser_slot);
                if update.dex == "pump_amm" {
                    cache.merge_pump_amm_pool_accounts_readiness(
                        pool_addr,
                        update.effective_dex_readiness(),
                    );
                }
                if update.dex == "pumpfun" {
                    cache.merge_pumpfun_bonding_readiness(
                        pool_addr,
                        update.effective_dex_readiness(),
                    );
                }
                // P3 #13: Propagate base_decimals and quote_decimals to SLAVE cache
                apply_decimals_from_metadata(cache, update);
                return true;
            }
        }
        PoolCacheUpdateType::BalanceUpdated => {
            let pool_addr = match Pubkey::from_str(&update.pool_address) {
                Ok(p) => p,
                Err(_) => return false,
            };
            let existing = cache.get(&pool_addr);
            let (base_reserve, quote_reserve) = if let Some(ref ex) = existing {
                let (eb, eq) = extract_reserves(ex);
                let base = if update.base_reserve > 0 {
                    update.base_reserve
                } else {
                    eb
                };
                let quote = if update.quote_reserve > 0 {
                    update.quote_reserve
                } else {
                    eq
                };
                (base, quote)
            } else {
                (update.base_reserve, update.quote_reserve)
            };
            if let Some((addr, mut minimal_state)) =
                build_minimal_pool_state_with_reserves(update, base_reserve, quote_reserve)
            {
                // P3 #12: Preserve pool_accounts and creator for pump_amm when BalanceUpdated
                // has no metadata (BalanceUpdated never includes metadata). Otherwise we'd
                // overwrite good data from PoolDiscovered and force RPC fallback in Liquidation.
                if update.dex == "pump_amm" {
                    if let (
                        Some(CachedPoolState::PumpAmm(ref existing_pump)),
                        CachedPoolState::PumpAmm(ref mut new_pump),
                    ) = (existing.as_ref(), &mut minimal_state)
                    {
                        if new_pump.pool_accounts.is_empty()
                            && !existing_pump.pool_accounts.is_empty()
                        {
                            new_pump.pool_accounts = existing_pump.pool_accounts.clone();
                        }
                        if new_pump.creator.is_none() && existing_pump.creator.is_some() {
                            new_pump.creator = existing_pump.creator;
                        }
                    }
                }
                // Bug #25: Preserve cashback_enabled for pumpfun when BalanceUpdated has no metadata
                // (BalanceUpdated never includes metadata). Otherwise we'd overwrite true with false.
                if update.dex == "pumpfun" {
                    let metadata_has_cashback = update
                        .metadata
                        .as_ref()
                        .and_then(|m| m.get("cashback_enabled"))
                        .is_some();
                    if !metadata_has_cashback {
                        if let (
                            Some(CachedPoolState::PumpFun(ref existing_pump)),
                            CachedPoolState::PumpFun(ref mut new_pump),
                        ) = (existing.as_ref(), &mut minimal_state)
                        {
                            new_pump.cashback_enabled = existing_pump.cashback_enabled;
                        }
                    }
                }
                cache.upsert(addr, minimal_state, update.geyser_slot);
                if update.dex == "pump_amm" {
                    cache.merge_pump_amm_pool_accounts_readiness(
                        addr,
                        update.effective_dex_readiness(),
                    );
                }
                if update.dex == "pumpfun" {
                    cache.merge_pumpfun_bonding_readiness(addr, update.effective_dex_readiness());
                }
                // P3 #13: Apply decimals from metadata when present (e.g. BalanceUpdated with metadata)
                apply_decimals_from_metadata(cache, update);
                return true;
            }
        }
        PoolCacheUpdateType::PoolRemoved => {
            // Skip removed pools
        }
    }
    false
}

/// Bootstrap LivePoolCache from JetStream (state recovery after restart)
///
/// Pulls the last PoolCacheUpdate for each pool from JetStream, giving
/// immediate state recovery. After bootstrap, the caller should reuse
/// the returned consumer for incremental updates.
///
/// # Returns
///
/// Returns (pools_recovered, consumer) so the caller can reuse the consumer
/// in the main loop instead of creating a new one (which would replay all messages).
pub async fn bootstrap_pool_cache_from_jetstream(
    nats_client: &NatsClient,
    live_pool_cache: &LivePoolCache,
) -> anyhow::Result<(
    usize,
    Option<
        async_nats::jetstream::consumer::Consumer<async_nats::jetstream::consumer::pull::Config>,
    >,
)> {
    use async_nats::jetstream;
    use futures::StreamExt;

    info!("SLAVE CACHE BOOTSTRAP: Pulling state from JetStream...");

    let jetstream = jetstream::new(nats_client.client().clone());

    // Get stream (must already exist, created by market-data)
    let stream = match jetstream.get_stream(STREAM_NAME).await {
        Ok(s) => s,
        Err(e) => {
            warn!(error = %e, stream = STREAM_NAME, "JetStream stream not found (market-data may not be running)");
            return Ok((0, None));
        }
    };

    // Create ephemeral consumer with LastPerSubject deliver policy
    let consumer_config = slave_consumer_config();
    let consumer = stream.create_consumer(consumer_config).await?;

    let mut pools_recovered = 0;
    let batch_size = 1000;

    // Fetch all available messages in batches until exhausted
    loop {
        let mut messages = consumer.fetch().max_messages(batch_size).messages().await?;
        let mut batch_count = 0;

        while let Some(msg) = messages.next().await {
            let msg = match msg {
                Ok(m) => m,
                Err(e) => {
                    warn!(error = %e, "Error fetching message from JetStream");
                    continue;
                }
            };

            batch_count += 1;

            let pool_update: PoolCacheUpdate = match serde_json::from_slice(&msg.payload) {
                Ok(u) => u,
                Err(e) => {
                    warn!(error = %e, "Failed to deserialize PoolCacheUpdate from JetStream");
                    if let Err(ack_err) = msg.ack().await {
                        warn!(error = %ack_err, "Failed to ack message");
                    }
                    continue;
                }
            };

            if apply_pool_cache_update(live_pool_cache, &pool_update) {
                pools_recovered += 1;
            }

            if let Err(ack_err) = msg.ack().await {
                warn!(error = %ack_err, "Failed to ack message");
            }
        }

        if batch_count < batch_size {
            break;
        }
    }

    info!(pools_recovered, "SLAVE CACHE BOOTSTRAP: Complete");
    Ok((pools_recovered, Some(consumer)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ipc::DexPoolReadiness;

    /// A.30 Regression: cashback_enabled must be true when JetStream metadata contains
    /// cashback_enabled="true". Verifies pool_cache_sync propagates metadata correctly.
    #[test]
    fn test_jetstream_metadata_propagates_cashback_enabled_true() {
        let cache = LivePoolCache::new();
        let pool_addr = "14Nx7vjtSeMVWugP4zUq5EJkD97ZXKRFUCAPhJJ1pump";
        let base_mint = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
        let quote_mint = "So11111111111111111111111111111111111111112";

        let mut update = PoolCacheUpdate::new_pool_discovered(
            "test",
            "0.1.0",
            "run-123",
            pool_addr.to_string(),
            "pumpfun".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            1_000_000,
            100_000_000,
            Some(0),
            12345,
        );
        let mut meta = std::collections::HashMap::new();
        meta.insert(
            "creator".to_string(),
            "Creato11111111111111111111111111111111111111".to_string(),
        );
        meta.insert("complete".to_string(), "false".to_string());
        meta.insert(
            "real_token_reserves".to_string(),
            "793100000000000".to_string(),
        );
        meta.insert("real_sol_reserves".to_string(), "0".to_string());
        meta.insert("cashback_enabled".to_string(), "true".to_string());
        update.metadata = Some(meta);

        let applied = apply_pool_cache_update(&cache, &update);
        assert!(applied, "apply_pool_cache_update should succeed");

        let pool_pk = Pubkey::from_str(pool_addr).unwrap();
        let state = cache.get(&pool_pk).expect("cache should have pool");
        match state {
            CachedPoolState::PumpFun(s) => {
                assert!(s.cashback_enabled, "A.30: cashback_enabled must be true when JetStream metadata contains cashback_enabled=\"true\"");
            }
            other => panic!("expected PumpFun state, got {:?}", other),
        }
    }

    #[test]
    fn test_pump_amm_readiness_ready_enables_ready_only_pool_accounts() {
        let cache = LivePoolCache::new();
        let pool_market = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::new_unique();
        let accounts: Vec<Pubkey> = (0..14).map(|_| Pubkey::new_unique()).collect();
        // Fewer than 14 accounts: not legacy-authoritative → no Ready without explicit key.
        let mut meta_short = std::collections::HashMap::new();
        meta_short.insert(
            "pool_accounts".to_string(),
            accounts[..13]
                .iter()
                .map(|p| p.to_string())
                .collect::<Vec<_>>()
                .join(","),
        );

        let mut update = PoolCacheUpdate::new_pool_discovered(
            "test",
            "0.1.0",
            "run",
            pool_market.to_string(),
            "pump_amm".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            1,
            1,
            None,
            1,
        );
        update.metadata = Some(meta_short);
        assert!(apply_pool_cache_update(&cache, &update));
        assert!(cache
            .get_pump_amm_pool_accounts_by_base_mint(&base_mint)
            .is_some());
        assert!(cache
            .get_ready_pump_amm_pool_accounts_by_base_mint(&base_mint)
            .is_none());

        let mut meta_full = std::collections::HashMap::new();
        meta_full.insert(
            "pool_accounts".to_string(),
            accounts
                .iter()
                .map(|p| p.to_string())
                .collect::<Vec<_>>()
                .join(","),
        );
        let mut update = PoolCacheUpdate::new_pool_discovered(
            "test",
            "0.1.0",
            "run",
            pool_market.to_string(),
            "pump_amm".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            1,
            1,
            None,
            2,
        );
        update.metadata = Some(meta_full);
        update.set_dex_readiness_in_metadata(DexPoolReadiness::Ready);
        assert!(apply_pool_cache_update(&cache, &update));
        assert!(cache
            .get_ready_pump_amm_pool_accounts_by_base_mint(&base_mint)
            .is_some());
    }

    /// I-24d / legacy JetStream: struct literal with 14 pool_accounts + non-zero reserves, no readiness key.
    #[test]
    fn test_pump_amm_legacy_authoritative_no_readiness_key_is_ready() {
        let cache = LivePoolCache::new();
        let pool_market = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::new_unique();
        let accounts: Vec<Pubkey> = (0..14).map(|_| Pubkey::new_unique()).collect();
        let mut meta = std::collections::HashMap::new();
        meta.insert(
            "pool_accounts".to_string(),
            accounts
                .iter()
                .map(|p| p.to_string())
                .collect::<Vec<_>>()
                .join(","),
        );
        let mut update = PoolCacheUpdate::new_pool_discovered(
            "test",
            "0.1.0",
            "run",
            pool_market.to_string(),
            "pump_amm".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            100,
            200,
            None,
            1,
        );
        update.metadata = Some(meta);
        assert_eq!(
            update.effective_dex_readiness(),
            DexPoolReadiness::Ready,
            "legacy authoritative pump_amm update without dex_pool_readiness key"
        );
        assert!(apply_pool_cache_update(&cache, &update));
        assert!(cache
            .get_ready_pump_amm_pool_accounts_by_base_mint(&base_mint)
            .is_some());
    }

    #[test]
    fn test_pump_amm_readiness_merge_never_downgrades() {
        let cache = LivePoolCache::new();
        let pool_market = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::new_unique();
        let accounts: Vec<Pubkey> = (0..14).map(|_| Pubkey::new_unique()).collect();
        let mut meta = std::collections::HashMap::new();
        meta.insert(
            "pool_accounts".to_string(),
            accounts
                .iter()
                .map(|p| p.to_string())
                .collect::<Vec<_>>()
                .join(","),
        );

        let mut ready_update = PoolCacheUpdate::new_pool_discovered(
            "test",
            "0.1.0",
            "run",
            pool_market.to_string(),
            "pump_amm".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            1,
            1,
            None,
            1,
        );
        ready_update.metadata = Some(meta.clone());
        ready_update.set_dex_readiness_in_metadata(DexPoolReadiness::Ready);
        assert!(apply_pool_cache_update(&cache, &ready_update));
        assert!(cache
            .get_ready_pump_amm_pool_accounts_by_base_mint(&base_mint)
            .is_some());

        let mut weak_update = PoolCacheUpdate::new_pool_discovered(
            "test",
            "0.1.0",
            "run",
            pool_market.to_string(),
            "pump_amm".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            1,
            1,
            None,
            2,
        );
        weak_update.metadata = Some(meta);
        weak_update.set_dex_readiness_in_metadata(DexPoolReadiness::Observed);
        assert!(apply_pool_cache_update(&cache, &weak_update));
        assert!(
            cache
                .get_ready_pump_amm_pool_accounts_by_base_mint(&base_mint)
                .is_some(),
            "merge must not downgrade Ready to Observed"
        );
    }

    /// Bug #36 (PumpFun): Geyser / partial JetStream must not imply explicit Ready for bonding curve.
    #[test]
    fn test_pumpfun_partial_jetstream_not_explicit_ready() {
        let cache = LivePoolCache::new();
        let pool_addr = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::from_str(crate::ipc::NATIVE_SOL_MINT).unwrap();

        let mut update = PoolCacheUpdate::new_pool_discovered(
            "test",
            "0.1.0",
            "run",
            pool_addr.to_string(),
            "pumpfun".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            1_000_000,
            100_000_000,
            Some(0),
            1,
        );
        let mut meta = std::collections::HashMap::new();
        meta.insert("creator".to_string(), Pubkey::new_unique().to_string());
        meta.insert(
            "associated_bonding_curve".to_string(),
            Pubkey::new_unique().to_string(),
        );
        meta.insert("complete".to_string(), "false".to_string());
        meta.insert("real_token_reserves".to_string(), "100".to_string());
        meta.insert("real_sol_reserves".to_string(), "200".to_string());
        meta.insert("cashback_enabled".to_string(), "false".to_string());
        update.metadata = Some(meta);
        update.set_dex_readiness_in_metadata(DexPoolReadiness::Partial);

        assert!(apply_pool_cache_update(&cache, &update));
        assert!(
            !cache.pumpfun_bonding_curve_explicitly_ready(&pool_addr),
            "Partial Geyser path must not set explicit Ready"
        );
    }

    /// Bug #36: explicit Ready from cold-path style message; merge Observed must not downgrade.
    #[test]
    fn test_pumpfun_readiness_merge_ready_then_observed_stays_ready() {
        let cache = LivePoolCache::new();
        let pool_addr = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::from_str(crate::ipc::NATIVE_SOL_MINT).unwrap();

        let mut ready_up = PoolCacheUpdate::new_pool_discovered(
            "test",
            "0.1.0",
            "run",
            pool_addr.to_string(),
            "pumpfun".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            1,
            2,
            Some(0),
            1,
        );
        let mut m = std::collections::HashMap::new();
        m.insert("creator".to_string(), Pubkey::new_unique().to_string());
        m.insert(
            "associated_bonding_curve".to_string(),
            Pubkey::new_unique().to_string(),
        );
        m.insert("complete".to_string(), "false".to_string());
        m.insert("real_token_reserves".to_string(), "1".to_string());
        m.insert("real_sol_reserves".to_string(), "2".to_string());
        m.insert("cashback_enabled".to_string(), "false".to_string());
        ready_up.metadata = Some(m.clone());
        ready_up.set_dex_readiness_in_metadata(DexPoolReadiness::Ready);
        assert!(apply_pool_cache_update(&cache, &ready_up));
        assert!(cache.pumpfun_bonding_curve_explicitly_ready(&pool_addr));

        let mut weak = PoolCacheUpdate::new_pool_discovered(
            "test",
            "0.1.0",
            "run",
            pool_addr.to_string(),
            "pumpfun".to_string(),
            base_mint.to_string(),
            quote_mint.to_string(),
            1,
            2,
            Some(0),
            2,
        );
        weak.metadata = Some(m);
        weak.set_dex_readiness_in_metadata(DexPoolReadiness::Observed);
        assert!(apply_pool_cache_update(&cache, &weak));
        assert!(
            cache.pumpfun_bonding_curve_explicitly_ready(&pool_addr),
            "merge must not downgrade Ready to Observed for PumpFun"
        );
    }
}
