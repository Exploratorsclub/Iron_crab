//! Orca Connector – Skeleton (Whirlpool/Classic)

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use super::orca_reserve_cache::{OrcaReserveCache, ReserveEntry};
use super::{Dex, Quote};
use crate::execution::live_pool_cache::{CachedPoolState, SharedLivePoolCache};
use crate::solana::rpc::SolanaRpc;
use chrono::Utc;
use solana_sdk::instruction::Instruction;

pub const ORCA_WHIRLPOOL_PROGRAM: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"; // corrected
                                                                                        // Whirlpool account size is 653 bytes
pub const WHIRLPOOL_ACCOUNT_MIN_SIZE: usize = 650; // lower bound
pub const WHIRLPOOL_ACCOUNT_MAX_SIZE: usize = 653; // exact size

use super::orca_whirlpool_layout as layout;
use dashmap::DashMap;
use solana_sdk::pubkey::Pubkey;

#[allow(dead_code)]
#[derive(Clone, Debug)]
struct OrcaPool {
    base_mint: Pubkey,
    quote_mint: Pubkey,
    reserve_base: u128,
    reserve_quote: u128,
    fee_bps: u32,
    fee_tier: Option<Pubkey>,
    tick_spacing: Option<u16>,
    vault_a: Pubkey,
    vault_b: Pubkey,
    tick_current_index: Option<i32>,
    // Lazy-loading for reserve balances (loaded on-demand from vaults)
    cached_reserves: Option<(u128, u128)>,
    last_reserve_fetch: Option<std::time::SystemTime>,
    // Cached token program IDs (SPL Token vs Token-2022) - eliminates RPC calls in hot path
    base_mint_program: Option<Pubkey>,
    quote_mint_program: Option<Pubkey>,
}

#[derive(Clone, Debug)]
pub struct OrcaPoolSnapshot {
    pub address: Pubkey,
    pub base_mint: Pubkey,
    pub quote_mint: Pubkey,
    pub reserve_base: u128,
    pub reserve_quote: u128,
    pub tick_spacing: Option<u16>,
    pub vault_a: Pubkey,
    pub vault_b: Pubkey,
}

pub struct Orca {
    rpc: Arc<SolanaRpc>,
    pools: Arc<DashMap<Pubkey, OrcaPool>>, // keyed by a pseudo pool id (mint xor) for now
    #[allow(dead_code)]
    fee_tiers: Arc<DashMap<Pubkey, (u32, u16)>>,
    user_authority: Arc<std::sync::RwLock<Option<Pubkey>>>,
    user_token_accounts: Arc<DashMap<Pubkey, Pubkey>>, // mint -> user token account (ATA)
    mint_index: Arc<DashMap<Pubkey, Vec<Pubkey>>>,     // mint -> pools containing it
    reserve_cache: Option<Arc<OrcaReserveCache>>,
    /// Geyser-sourced LivePoolCache for real-time vault balances (GEYSER-FIRST)
    live_pool_cache: Option<SharedLivePoolCache>,
    cache_hits: Arc<AtomicU64>,
    cache_misses: Arc<AtomicU64>,
    /// When true (Hot Path): skip get_multiple_accounts tick validation to avoid RPC
    skip_tick_array_rpc_validation: Arc<AtomicBool>,
}

impl Orca {
    pub fn new(rpc: Arc<SolanaRpc>) -> Self {
        Self::new_with_cache(rpc, None, None)
    }

    /// Create Orca instance with optional persistent reserve cache and LivePoolCache.
    pub fn new_with_cache(
        rpc: Arc<SolanaRpc>,
        cache_path: Option<String>,
        live_pool_cache: Option<SharedLivePoolCache>,
    ) -> Self {
        let reserve_cache = cache_path
            .and_then(|path| OrcaReserveCache::new(&path, 300).ok())
            .map(Arc::new);

        Self {
            rpc,
            pools: Arc::new(DashMap::new()),
            fee_tiers: Arc::new(DashMap::new()),
            user_authority: Arc::new(std::sync::RwLock::new(None)),
            user_token_accounts: Arc::new(DashMap::new()),
            mint_index: Arc::new(DashMap::new()),
            reserve_cache,
            live_pool_cache,
            cache_hits: Arc::new(AtomicU64::new(0)),
            cache_misses: Arc::new(AtomicU64::new(0)),
            skip_tick_array_rpc_validation: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Set the global user authority (signer) used for swaps.
    pub fn set_user_authority(&self, auth: Pubkey) {
        *self.user_authority.write().unwrap() = Some(auth);
    }

    /// When true (Hot Path): skip tick array RPC validation to avoid get_multiple_accounts.
    /// Cold Path (Liquidation) keeps validation for safety.
    pub fn set_skip_tick_array_rpc_validation(&self, skip: bool) {
        self.skip_tick_array_rpc_validation
            .store(skip, Ordering::Relaxed);
    }

    /// Inject cached Orca Whirlpool state from LivePoolCache.
    ///
    /// This allows build_swap_ix to use fresh Geyser-sourced data
    /// instead of making RPC calls. The cached state includes the
    /// current tick_current_index which changes with price.
    ///
    /// Returns Ok(true) if state was injected, Ok(false) if pool already exists.
    pub fn inject_cached_orca_state(
        &self,
        pool_address: &Pubkey,
        state: &crate::execution::live_pool_cache::OrcaWhirlpoolState,
    ) -> Result<bool> {
        // Check if already exists
        if self.pools.contains_key(pool_address) {
            // Update existing entry with fresh tick data
            if let Some(mut entry) = self.pools.get_mut(pool_address) {
                entry.tick_current_index = Some(state.tick_current_index);
                if let Some(balance_a) = state.vault_a_balance {
                    entry.reserve_base = balance_a as u128;
                }
                if let Some(balance_b) = state.vault_b_balance {
                    entry.reserve_quote = balance_b as u128;
                }
                entry.cached_reserves = Some((entry.reserve_base, entry.reserve_quote));
                entry.last_reserve_fetch = Some(std::time::SystemTime::now());
            }
            return Ok(false);
        }

        // Create new pool entry from cached state
        let pool = OrcaPool {
            base_mint: state.token_mint_a,
            quote_mint: state.token_mint_b,
            reserve_base: state.vault_a_balance.unwrap_or(0) as u128,
            reserve_quote: state.vault_b_balance.unwrap_or(0) as u128,
            fee_bps: state.fee_rate as u32,
            fee_tier: None,
            tick_spacing: Some(state.tick_spacing),
            vault_a: state.token_vault_a,
            vault_b: state.token_vault_b,
            tick_current_index: Some(state.tick_current_index),
            cached_reserves: Some((
                state.vault_a_balance.unwrap_or(0) as u128,
                state.vault_b_balance.unwrap_or(0) as u128,
            )),
            last_reserve_fetch: Some(std::time::SystemTime::now()),
            base_mint_program: state.token_a_program,
            quote_mint_program: state.token_b_program,
        };

        self.pools.insert(*pool_address, pool);

        // Update mint index
        self.mint_index
            .entry(state.token_mint_a)
            .or_default()
            .push(*pool_address);
        self.mint_index
            .entry(state.token_mint_b)
            .or_default()
            .push(*pool_address);

        tracing::info!(
            pool = %pool_address,
            tick_current_index = state.tick_current_index,
            tick_spacing = state.tick_spacing,
            "orca: injected cached state from LivePoolCache"
        );

        Ok(true)
    }

    /// Get pool accounts in DexPoolAccounts format for a given pool address.
    ///
    /// Returns accounts in the format expected by tx_builder:
    /// - accounts[0] = pool_address (whirlpool)
    /// - accounts[1] = token_mint_a
    /// - accounts[2] = token_mint_b
    /// - accounts[3] = token_vault_a
    /// - accounts[4] = token_vault_b
    /// - accounts[5] = "tick_current_index:<value>"
    /// - accounts[6] = "tick_spacing:<value>"
    ///
    /// Returns None if pool is not cached.
    pub fn get_pool_accounts(&self, pool_address: &Pubkey) -> Option<Vec<String>> {
        self.pools.get(pool_address).map(|pool| {
            vec![
                pool_address.to_string(),
                pool.base_mint.to_string(),
                pool.quote_mint.to_string(),
                pool.vault_a.to_string(),
                pool.vault_b.to_string(),
                format!(
                    "tick_current_index:{}",
                    pool.tick_current_index.unwrap_or(0)
                ),
                format!("tick_spacing:{}", pool.tick_spacing.unwrap_or(64)),
            ]
        })
    }

    /// Register (or override) a user token account (ATA) for a given mint.
    pub fn set_user_token_account(&self, mint: Pubkey, ata: Pubkey) {
        self.user_token_accounts.insert(mint, ata);
    }

    /// Insert a single Whirlpool into the in-memory cache from a parsed on-chain account.
    ///
    /// This is useful for targeted tx planning (e.g., execution-engine) without performing a
    /// full `refresh_pools()` scan.
    pub fn insert_whirlpool_parsed(&self, id: Pubkey, parsed: layout::WhirlpoolParsed) {
        self.pools.insert(
            id,
            OrcaPool {
                base_mint: parsed.token_mint_a,
                quote_mint: parsed.token_mint_b,
                reserve_base: 0,
                reserve_quote: 0,
                fee_bps: parsed.fee_rate as u32,
                fee_tier: None,
                tick_spacing: Some(parsed.tick_spacing),
                vault_a: parsed.token_vault_a,
                vault_b: parsed.token_vault_b,
                tick_current_index: Some(parsed.tick_current_index),
                cached_reserves: None,
                last_reserve_fetch: None,
                base_mint_program: None,
                quote_mint_program: None,
            },
        );

        for m in [parsed.token_mint_a, parsed.token_mint_b] {
            self.mint_index
                .entry(m)
                .or_insert_with(|| Vec::with_capacity(2))
                .push(id);
        }
    }

    fn find_pool(&self, input: &Pubkey, output: &Pubkey) -> Option<(Pubkey, bool, OrcaPool)> {
        tracing::debug!(
            input = %input,
            output = %output,
            pools_count = self.pools.len(),
            "Orca find_pool searching for pair"
        );

        for p in self.pools.iter() {
            let forward = p.base_mint == *input && p.quote_mint == *output;
            let reverse = p.base_mint == *output && p.quote_mint == *input;
            tracing::trace!(
                pool_key = %p.key(),
                base_mint = %p.base_mint,
                quote_mint = %p.quote_mint,
                forward = forward,
                reverse = reverse,
                "Checking pool"
            );
            if forward || reverse {
                tracing::info!(
                    pool = %p.key(),
                    direction = if forward { "forward" } else { "reverse" },
                    "Orca pool found for pair"
                );
                return Some((*p.key(), forward, p.clone()));
            }
        }

        tracing::warn!(
            input = %input,
            output = %output,
            pools_count = self.pools.len(),
            "No Orca pool found for pair"
        );
        None
    }

    pub fn insert_mock_pool(
        &self,
        base: Pubkey,
        quote: Pubkey,
        reserve_base: u128,
        reserve_quote: u128,
        fee_bps: u32,
    ) {
        self.pools.insert(
            base,
            OrcaPool {
                base_mint: base,
                quote_mint: quote,
                reserve_base,
                reserve_quote,
                fee_bps,
                fee_tier: None,
                tick_spacing: None,
                vault_a: Pubkey::new_unique(),
                vault_b: Pubkey::new_unique(),
                tick_current_index: None,
                cached_reserves: Some((reserve_base, reserve_quote)),
                last_reserve_fetch: Some(std::time::SystemTime::now()),
                base_mint_program: None,
                quote_mint_program: None,
            },
        );
    }

    /// Pools that reference this mint.
    pub fn pools_for_mint(&self, mint: &Pubkey) -> Vec<Pubkey> {
        self.mint_index
            .get(mint)
            .map(|v| v.clone())
            .unwrap_or_default()
    }

    /// Get total liquidity in SOL for a given mint across all tracked pools.
    pub fn get_liquidity_sol_for_mint(&self, mint: &Pubkey) -> f64 {
        let sol_mint = Pubkey::from_str("So11111111111111111111111111111111111111112").unwrap();
        let mut total_sol = 0.0;

        if let Some(pools) = self.mint_index.get(mint) {
            for pool_pubkey in pools.iter() {
                if let Some(pool) = self.pools.get(pool_pubkey) {
                    if pool.base_mint == sol_mint {
                        total_sol += pool.reserve_base as f64 / 1e9;
                    } else if pool.quote_mint == sol_mint {
                        total_sol += pool.reserve_quote as f64 / 1e9;
                    }
                }
            }
        }
        total_sol
    }

    /// Return a lightweight snapshot of current pools (for read-only aggregation like liquidity indexing).
    pub fn pools_snapshot(&self) -> Vec<OrcaPoolSnapshot> {
        self.pools
            .iter()
            .map(|p| OrcaPoolSnapshot {
                address: *p.key(),
                base_mint: p.base_mint,
                quote_mint: p.quote_mint,
                reserve_base: p.reserve_base,
                reserve_quote: p.reserve_quote,
                tick_spacing: p.tick_spacing,
                vault_a: p.vault_a,
                vault_b: p.vault_b,
            })
            .collect()
    }

    /// Fetch the CURRENT tick_current_index from the pool on-chain.
    ///
    /// This is critical because the cached tick may be stale (price moved since cache update).
    /// Using the wrong tick leads to wrong tick array calculation → Error 6023.
    ///
    /// If we can't fetch the pool, we fall back to the cached tick (or 0).
    #[allow(dead_code)]
    async fn fetch_current_tick(&self, pool_id: &Pubkey, fallback_tick: Option<i32>) -> i32 {
        let fallback = fallback_tick.unwrap_or(0);

        match self.rpc.get_account_retry(pool_id).await {
            Ok(account) => {
                if account.data.len() >= layout::MIN_WHIRLPOOL_ACCOUNT_LEN {
                    // Read tick_current_index at offset 81 (i32 LE)
                    let tick_bytes: [u8; 4] = account.data
                        [layout::OFF_TICK_CURRENT..layout::OFF_TICK_CURRENT + 4]
                        .try_into()
                        .unwrap_or([0; 4]);
                    let current_tick = i32::from_le_bytes(tick_bytes);

                    if current_tick != fallback {
                        tracing::info!(
                            pool = %pool_id,
                            cached_tick = fallback,
                            current_tick = current_tick,
                            delta = current_tick - fallback,
                            "Orca: tick changed since cache - using fresh on-chain value"
                        );
                    }

                    current_tick
                } else {
                    tracing::warn!(
                        pool = %pool_id,
                        account_size = account.data.len(),
                        "Orca: pool account too small, using cached tick"
                    );
                    fallback
                }
            }
            Err(e) => {
                tracing::warn!(
                    pool = %pool_id,
                    error = %e,
                    "Orca: failed to fetch pool for current tick, using cached value"
                );
                fallback
            }
        }
    }

    /// Lazy-load reserves from vaults with Geyser-first strategy.
    /// LivePoolCache = einzige Quelle (wenn gesetzt). Cache-Miss → statische Reserves (kein RPC).
    /// RPC nur im Cold Path (live_pool_cache.is_none(), z.B. sell_all_keyless).
    async fn load_reserves_if_needed(&self, pool_id: &Pubkey, pool: &OrcaPool) -> (u128, u128) {
        // 0) LivePoolCache (Geyser) — einzige Reserve-Quelle im Hot Path
        if let Some(ref lpc) = self.live_pool_cache {
            if let Some(CachedPoolState::Orca(state)) = lpc.get(pool_id) {
                if let (Some(va), Some(vb)) = (state.vault_a_balance, state.vault_b_balance) {
                    if va > 0 && vb > 0 {
                        self.cache_hits.fetch_add(1, Ordering::Relaxed);
                        return (va as u128, vb as u128);
                    }
                }
            }
            // Cache miss: kein RPC, statische Reserves (meist 0,0 → quote_exact_in returns Ok(None))
            self.cache_misses.fetch_add(1, Ordering::Relaxed);
            tracing::debug!(
                pool = %pool_id,
                "orca: LivePoolCache miss, using static reserves (no RPC)"
            );
            return (pool.reserve_base, pool.reserve_quote);
        }

        // Cold Path: live_pool_cache.is_none() — RPC erlaubt
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
        tracing::warn!(
            pool = %pool_id,
            "orca: RPC fallback for vault reserves (Cold Path, no LivePoolCache)"
        );
        match self
            .rpc
            .rpc
            .get_multiple_accounts(&[pool.vault_a, pool.vault_b])
            .await
        {
            Ok(vaults) => {
                let mut reserves = (0u128, 0u128);
                if let Some(Some(v1)) = vaults.first() {
                    if v1.data.len() >= 72 {
                        reserves.0 = Self::parse_token_amount(&v1.data) as u128;
                    }
                }
                if let Some(Some(v2)) = vaults.get(1) {
                    if v2.data.len() >= 72 {
                        reserves.1 = Self::parse_token_amount(&v2.data) as u128;
                    }
                }
                // Store in persistent cache (Cold Path only)
                if let Some(ref db_cache) = self.reserve_cache {
                    let entry = ReserveEntry {
                        pool_address: *pool_id,
                        reserve_base: reserves.0,
                        reserve_quote: reserves.1,
                        cached_at: Utc::now(),
                    };
                    let _ = db_cache.set(&entry);
                }
                tracing::debug!(
                    pool = %pool_id,
                    vault_a = reserves.0,
                    vault_b = reserves.1,
                    "orca: fresh reserves fetched via RPC (Cold Path)"
                );
                reserves
            }
            Err(e) => {
                tracing::warn!(
                    pool = %pool_id,
                    error = %e,
                    "orca reserve fetch failed, using static reserves"
                );
                (pool.reserve_base, pool.reserve_quote)
            }
        }
    }

    /// Background task to prefetch reserves for top liquidity pools.
    /// Reduces latency during evaluation by warming up the cache.
    pub async fn prefetch_top_pools(&self, limit: usize) -> Result<()> {
        let pools_to_fetch: Vec<Pubkey> = self
            .pools
            .iter()
            .take(limit)
            .map(|entry| *entry.key())
            .collect();

        let mut prefetched = 0;
        for pool_id in pools_to_fetch {
            if let Some(pool) = self.pools.get(&pool_id) {
                let _ = self.load_reserves_if_needed(&pool_id, &pool).await;
                prefetched += 1;
            }
        }

        // Update pools_total metric
        crate::metrics::ORCA_POOLS_TOTAL.store(
            self.pools.len() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );

        tracing::info!(
            prefetched,
            total = self.pools.len(),
            cache_hits = self.cache_hits.load(Ordering::Relaxed),
            cache_misses = self.cache_misses.load(Ordering::Relaxed),
            "orca prefetch_top_pools() done"
        );
        Ok(())
    }

    /// Get cache statistics for monitoring.
    pub fn cache_stats(&self) -> (u64, u64) {
        (
            self.cache_hits.load(Ordering::Relaxed),
            self.cache_misses.load(Ordering::Relaxed),
        )
    }

    /// Background task: Batch-refresh vault balances for all pools (professional solution)
    /// Fetches vault balances in batches of 100 to minimize RPC load
    pub async fn batch_refresh_vault_balances(&self) -> Result<()> {
        let vaults: Vec<(Pubkey, Pubkey, Pubkey)> = self
            .pools
            .iter()
            .map(|entry| (*entry.key(), entry.vault_a, entry.vault_b))
            .collect();

        if vaults.is_empty() {
            return Ok(());
        }

        tracing::debug!(
            vault_count = vaults.len() * 2,
            "orca: batch refreshing vault balances"
        );

        let mut updated_pools = 0;
        let start = std::time::Instant::now();

        // Batch fetch vaults in chunks of 50 pools (= 100 vault accounts)
        // RPC limit is 100 accounts, each pool has 2 vaults
        for chunk in vaults.chunks(50) {
            let vault_pubkeys: Vec<Pubkey> = chunk
                .iter()
                .flat_map(|(_, va, vb)| vec![*va, *vb])
                .collect();

            match self.rpc.rpc.get_multiple_accounts(&vault_pubkeys).await {
                Ok(accounts) => {
                    // Process results (accounts come in pairs: vault_a, vault_b)
                    for (i, (pool_id, _va, _vb)) in chunk.iter().enumerate() {
                        let vault_a_account = accounts.get(i * 2).and_then(|a| a.as_ref());
                        let vault_b_account = accounts.get(i * 2 + 1).and_then(|a| a.as_ref());

                        let reserve_a = vault_a_account
                            .and_then(|acc| {
                                if acc.data.len() >= 72 {
                                    Some(Self::parse_token_amount(&acc.data) as u128)
                                } else {
                                    None
                                }
                            })
                            .unwrap_or(0);

                        let reserve_b = vault_b_account
                            .and_then(|acc| {
                                if acc.data.len() >= 72 {
                                    Some(Self::parse_token_amount(&acc.data) as u128)
                                } else {
                                    None
                                }
                            })
                            .unwrap_or(0);

                        if reserve_a > 0 || reserve_b > 0 {
                            if let Some(mut pool) = self.pools.get_mut(pool_id) {
                                pool.reserve_base = reserve_a;
                                pool.reserve_quote = reserve_b;
                                pool.cached_reserves = Some((reserve_a, reserve_b));
                                pool.last_reserve_fetch = Some(std::time::SystemTime::now());
                                updated_pools += 1;
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, chunk_size = chunk.len(), "Failed to fetch vault batch");
                }
            }

            // Small delay between batches to avoid overwhelming RPC
            if vaults.len() > 100 {
                tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            }
        }

        let elapsed = start.elapsed();
        tracing::info!(
            updated_pools,
            total_pools = vaults.len(),
            elapsed_ms = elapsed.as_millis(),
            "orca: batch vault refresh completed"
        );

        Ok(())
    }

    /// Get all vault addresses from current pools (for subscription)
    pub fn get_all_vaults(&self) -> Vec<Pubkey> {
        let mut vaults = Vec::new();

        for pool in self.pools.iter() {
            vaults.push(pool.vault_a);
            vaults.push(pool.vault_b);
        }

        // Deduplicate
        vaults.sort();
        vaults.dedup();

        vaults
    }
}

#[async_trait]
impl Dex for Orca {
    /// Refresh pool cache via RPC getProgramAccounts.
    ///
    /// ⚠️ **RPC FALLBACK ONLY** - Use Geyser-based pool discovery in production!
    ///
    /// **Feature-gated:** Only available with `rpc_fallback` feature.
    async fn refresh_pools(&self) -> Result<()> {
        #[cfg(not(feature = "rpc_fallback"))]
        {
            tracing::debug!("refresh_pools() disabled - rpc_fallback feature not enabled");
            return Ok(());
        }

        #[cfg(feature = "rpc_fallback")]
        {
            use solana_account_decoder::UiAccountEncoding;
            use solana_client::rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
            use solana_client::rpc_filter::RpcFilterType;
            use solana_sdk::pubkey::Pubkey;
            use std::str::FromStr;
            tracing::trace!("orca.refresh_pools() whirlpool fetch");
            let program_id = Pubkey::from_str(ORCA_WHIRLPOOL_PROGRAM)
                .map_err(|_| anyhow!("invalid whirlpool program id"))?;
            // DataSize filter (broad window) to reduce traffic
            let size_filter = RpcFilterType::DataSize(WHIRLPOOL_ACCOUNT_MAX_SIZE as u64);
            let filters = Some(vec![size_filter]);
            let acc_cfg = RpcAccountInfoConfig {
                encoding: Some(UiAccountEncoding::Base64),
                data_slice: None,
                commitment: None,
                min_context_slot: None,
            };
            let cfg = RpcProgramAccountsConfig {
                filters,
                account_config: acc_cfg,
                with_context: None,
                sort_results: None,
            };
            let accounts = self
                .rpc
                .get_program_accounts_with_config_retry(&program_id, cfg)
                .await?;
            let mut added = 0u32;
            let mut total_accounts = 0u32;
            let mut parsed_ok = 0u32;
            let mut zero_reserve = 0u32;
            self.mint_index.clear();
            for (addr, acc) in accounts.into_iter().take(5000) {
                // safety limit
                total_accounts += 1;
                if acc.data.len() < WHIRLPOOL_ACCOUNT_MIN_SIZE
                    || acc.data.len() > WHIRLPOOL_ACCOUNT_MAX_SIZE
                {
                    continue;
                }
                if let Some(parsed) = layout::parse_whirlpool(&acc.data) {
                    parsed_ok += 1;
                    // SKIP vault fetching during initial refresh (too slow - 5000+ RPC calls!)
                    // Reserves will be loaded on-demand when needed for quotes
                    let reserves = (0u128, 0u128);
                    zero_reserve += 1;
                    let id = addr;
                    self.pools.insert(
                        id,
                        OrcaPool {
                            base_mint: parsed.token_mint_a,
                            quote_mint: parsed.token_mint_b,
                            reserve_base: reserves.0,
                            reserve_quote: reserves.1,
                            fee_bps: parsed.fee_rate as u32,
                            fee_tier: None,
                            tick_spacing: Some(parsed.tick_spacing),
                            vault_a: parsed.token_vault_a,
                            vault_b: parsed.token_vault_b,
                            tick_current_index: Some(parsed.tick_current_index),
                            cached_reserves: if reserves.0 > 0 || reserves.1 > 0 {
                                Some(reserves)
                            } else {
                                None
                            },
                            last_reserve_fetch: if reserves.0 > 0 || reserves.1 > 0 {
                                Some(std::time::SystemTime::now())
                            } else {
                                None
                            },
                            base_mint_program: None,
                            quote_mint_program: None,
                        },
                    );
                    for m in [parsed.token_mint_a, parsed.token_mint_b] {
                        self.mint_index
                            .entry(m)
                            .or_insert_with(|| Vec::with_capacity(2))
                            .push(id);
                    }
                    added += 1;
                }
            }

            // Update pools_total metric
            crate::metrics::ORCA_POOLS_TOTAL.store(
                self.pools.len() as u64,
                std::sync::atomic::Ordering::Relaxed,
            );

            tracing::info!(
                total_accounts,
                parsed_ok,
                zero_reserve,
                added,
                total = self.pools.len(),
                "orca.refresh_pools() done"
            );
            Ok(())
        } // end #[cfg(feature = "rpc_fallback")]
    }

    /// Set pool data directly from accounts list (NO RPC calls).
    ///
    /// This is the preferred method for execution-engine. The accounts come from
    /// `Intent.resources.accounts` which were populated by arb-strategy from
    /// `DexPoolAccounts` events (Geyser data).
    ///
    /// Expected accounts format (from market_data.rs DexPoolAccounts):
    /// - accounts[0] = pool_address (whirlpool id)
    /// - accounts[1] = base_mint (token_mint_a)
    /// - accounts[2] = quote_mint (token_mint_b)
    /// - accounts[3] = coin_vault (token_vault_a)
    /// - accounts[4] = pc_vault (token_vault_b)
    fn set_pool_from_accounts(&self, pool_address: &str, accounts: &[String]) -> Result<()> {
        use std::str::FromStr;

        // Minimum required: pool_address, base_mint, quote_mint
        if accounts.len() < 3 {
            return Err(anyhow!(
                "orca set_pool_from_accounts requires at least 3 accounts, got {}",
                accounts.len()
            ));
        }

        let parse_pubkey = |s: &str, name: &str| -> Result<Pubkey> {
            Pubkey::from_str(s).map_err(|e| anyhow!("Invalid {} pubkey '{}': {}", name, s, e))
        };

        let pool_pk = parse_pubkey(pool_address, "pool_address")?;
        let token_mint_a = parse_pubkey(&accounts[1], "base_mint/token_mint_a")?;
        let token_mint_b = parse_pubkey(&accounts[2], "quote_mint/token_mint_b")?;

        // Vaults are optional
        let token_vault_a = if accounts.len() > 3 {
            parse_pubkey(&accounts[3], "coin_vault/token_vault_a")?
        } else {
            Pubkey::default()
        };

        let token_vault_b = if accounts.len() > 4 {
            parse_pubkey(&accounts[4], "pc_vault/token_vault_b")?
        } else {
            Pubkey::default()
        };

        // Validate pool_address matches accounts[0]
        let expected_pool = parse_pubkey(&accounts[0], "accounts[0]")?;
        if pool_pk != expected_pool {
            return Err(anyhow!(
                "pool_address {} does not match accounts[0] {}",
                pool_address,
                expected_pool
            ));
        }

        // Parse extended fields from LivePoolCache (format: "key:value")
        let mut tick_current_index: Option<i32> = None;
        let mut tick_spacing: Option<u16> = None;
        let mut base_mint_program: Option<Pubkey> = None;
        let mut quote_mint_program: Option<Pubkey> = None;

        for acct in accounts.iter().skip(5) {
            if let Some(val) = acct.strip_prefix("tick_current_index:") {
                tick_current_index = val.parse().ok();
            } else if let Some(val) = acct.strip_prefix("tick_spacing:") {
                tick_spacing = val.parse().ok();
            } else if let Some(val) = acct.strip_prefix("token_a_program:") {
                base_mint_program = Pubkey::from_str(val).ok();
            } else if let Some(val) = acct.strip_prefix("token_b_program:") {
                quote_mint_program = Pubkey::from_str(val).ok();
            }
        }

        // Create pool entry with all available data from LivePoolCache
        let pool = OrcaPool {
            base_mint: token_mint_a,
            quote_mint: token_mint_b,
            reserve_base: 0,
            reserve_quote: 0,
            fee_bps: 30, // Default, actual comes from on-chain
            fee_tier: None,
            tick_spacing, // From LivePoolCache!
            vault_a: token_vault_a,
            vault_b: token_vault_b,
            tick_current_index, // From LivePoolCache (fresh from Geyser)!
            cached_reserves: None,
            last_reserve_fetch: None,
            base_mint_program,  // From LivePoolCache - NO RPC NEEDED!
            quote_mint_program, // From LivePoolCache - NO RPC NEEDED!
        };

        tracing::debug!(
            pool = %pool_pk,
            token_a = %token_mint_a,
            token_b = %token_mint_b,
            tick = ?tick_current_index,
            tick_spacing = ?tick_spacing,
            token_a_prog = ?base_mint_program,
            token_b_prog = ?quote_mint_program,
            "orca pool set from LivePoolCache (NO RPC)"
        );

        self.pools.insert(pool_pk, pool);

        // Update mint index
        self.mint_index
            .entry(token_mint_a)
            .or_insert_with(|| Vec::with_capacity(2))
            .push(pool_pk);
        self.mint_index
            .entry(token_mint_b)
            .or_insert_with(|| Vec::with_capacity(2))
            .push(pool_pk);

        Ok(())
    }

    async fn quote_exact_in(
        &self,
        _input_mint: &str,
        _output_mint: &str,
        _amount_in: u64,
    ) -> Result<Option<Quote>> {
        use std::str::FromStr;
        let input = Pubkey::from_str(_input_mint).map_err(|_| anyhow!("bad input mint"))?;
        let output = Pubkey::from_str(_output_mint).map_err(|_| anyhow!("bad output mint"))?;
        let pool = match self.find_pool(&input, &output) {
            Some(p) => p,
            None => return Ok(None),
        };
        let (pid, forward, p) = pool;

        // Lazy-load fresh reserves if not cached
        let (rin, rout) = {
            let (base, quote) = self.load_reserves_if_needed(&pid, &p).await;
            if forward {
                (base, quote)
            } else {
                (quote, base)
            }
        };
        if rin == 0 || rout == 0 {
            return Ok(None);
        }
        let amount_in_u = _amount_in as u128;
        let fee_bps_u = p.fee_bps as u128;
        let amount_less_fee = amount_in_u * (10_000 - fee_bps_u) / 10_000;
        let out = (amount_less_fee * rout) / (rin + amount_less_fee);
        if out == 0 {
            return Ok(None);
        }
        let impact_bps = ((amount_less_fee * 10_000) / (rin + amount_less_fee)) as u32;
        Ok(Some(Quote {
            amount_out: out as u64,
            price_impact_bps: impact_bps,
            route: vec![pid.to_string()],
            fee_bps: p.fee_bps,
            in_reserve: rin,
            out_reserve: rout,
            input_mint: (if forward { input } else { output }).to_string(),
            output_mint: (if forward { output } else { input }).to_string(),
            tick_spacing: p.tick_spacing,
        }))
    }

    fn build_swap_ix(
        &self,
        _input_mint: &str,
        _output_mint: &str,
        _amount_in: u64,
        _min_out: u64,
    ) -> Result<Vec<Instruction>> {
        use solana_sdk::instruction::AccountMeta as AM;
        use std::str::FromStr;

        // Orca Whirlpool swap discriminator: SHA256("global:swap")[0..8]
        // From official SDK: [248, 198, 158, 145, 225, 117, 135, 200]
        const SWAP_DISCRIMINATOR: [u8; 8] = [0xf8, 0xc6, 0x9e, 0x91, 0xe1, 0x75, 0x87, 0xc8];

        let in_pk = Pubkey::from_str(_input_mint).map_err(|_| anyhow!("bad input mint"))?;
        let out_pk = Pubkey::from_str(_output_mint).map_err(|_| anyhow!("bad output mint"))?;
        let (pool_id, forward, pool) = self
            .find_pool(&in_pk, &out_pk)
            .ok_or_else(|| anyhow!("no orca pool for pair"))?;

        // Get user authority first (needed for ATAs)
        let authority = self
            .user_authority
            .read()
            .unwrap()
            .ok_or_else(|| anyhow!("orca user authority not set"))?;

        // Determine token A/B based on pool ordering (not swap direction)
        // Pool always has base_mint=A, quote_mint=B
        // forward=true means input=A, output=B (a_to_b=true)
        // forward=false means input=B, output=A (a_to_b=false)
        let a_to_b = forward;

        // NOTE: This sync function uses SPL Token program by default.
        // For Token-2022 support, use build_swap_ix_async() instead which
        // can fetch mint account owners to determine the correct token program.
        // Token owner accounts (user's ATAs)
        let derive_ata = |mint: &Pubkey| -> Pubkey {
            self.user_token_accounts
                .get(mint)
                .map(|v| *v)
                .unwrap_or_else(|| {
                    let owner_spl = spl_token::solana_program::pubkey::Pubkey::new_from_array(
                        authority.to_bytes(),
                    );
                    let mint_spl =
                        spl_token::solana_program::pubkey::Pubkey::new_from_array(mint.to_bytes());
                    let ata_spl =
                        spl_associated_token_account::get_associated_token_address_with_program_id(
                            &owner_spl,
                            &mint_spl,
                            &spl_token::id(),
                        );
                    Pubkey::new_from_array(ata_spl.to_bytes())
                })
        };

        let token_owner_account_a = derive_ata(&pool.base_mint); // Always token A
        let token_owner_account_b = derive_ata(&pool.quote_mint); // Always token B

        // Tick arrays for the swap range
        // Orca requires tick arrays covering the price range the swap might traverse
        // The tick arrays MUST be in the correct sequence relative to current tick
        let spacing = pool.tick_spacing.unwrap_or(64) as i32;
        let tick_now = pool.tick_current_index.unwrap_or(0);
        let ticks_per_array = spacing * TICK_ARRAY_SIZE;

        // Calculate the start index of the tick array containing current tick
        let current_array_start = get_tick_array_start_index(tick_now, spacing);

        // Log tick array calculation for debugging
        tracing::debug!(
            pool = %pool_id,
            tick_spacing = spacing,
            tick_current = tick_now,
            current_array_start = current_array_start,
            ticks_per_array = ticks_per_array,
            a_to_b = a_to_b,
            "orca: calculating tick arrays for swap"
        );

        // CRITICAL: Tick array sequence depends on swap direction
        // For a_to_b swaps: price goes DOWN, ticks DECREASE
        //   - First array must contain current tick
        //   - Subsequent arrays are at LOWER tick indices
        // For b_to_a swaps: price goes UP, ticks INCREASE
        //   - First array must contain current tick
        //   - Subsequent arrays are at HIGHER tick indices
        //
        // IMPORTANT (Error 6023 fix): The cached tick_current_index may be STALE!
        // If the price moved since the cache was updated, the tick may now be in a
        // different array. To handle this, we provide arrays on BOTH sides of the
        // current array, giving us tolerance for small price movements:
        //
        //   tick_array_0 = one array BEFORE current (lower ticks)
        //   tick_array_1 = current array (contains cached tick)
        //   tick_array_2 = one array AFTER current (higher ticks)
        //
        // This way, if the tick moved slightly in either direction, we still have
        // the correct array in our set. The swap will use whichever arrays it needs.
        //
        // Note: For large swaps that traverse multiple arrays in one direction,
        // this centered approach may not have enough range. But for our typical
        // arbitrage amounts (0.1 SOL), this is more than sufficient.
        let s_prev = current_array_start - ticks_per_array; // Previous array (lower ticks)
        let s_curr = current_array_start; // Current array
        let s_next = current_array_start + ticks_per_array; // Next array (higher ticks)

        // CRITICAL FIX: Orca Whirlpool requires tick_array_0 to contain the CURRENT tick!
        // The swap then traverses through tick_array_1 and tick_array_2 in the swap direction.
        // Previous code incorrectly put the "destination" array first, causing 6023 errors.
        //
        // Correct order per Orca SDK:
        // - tick_array_0: MUST contain current tick (s_curr)
        // - tick_array_1, tick_array_2: in swap direction
        //
        // For a_to_b (price decreases, ticks decrease): curr, prev, prev-1
        // For b_to_a (price increases, ticks increase): curr, next, next+1
        let (tick_array_0, tick_array_1, tick_array_2, start0, start1, start2) = if a_to_b {
            // A->B: price decreases, ticks decrease
            // tick_array_0 = current (contains current tick)
            // tick_array_1 = previous (lower ticks - swap direction)
            // tick_array_2 = previous-1 (even lower ticks - continued swap direction)
            let s_prev2 = s_prev - ticks_per_array;
            (
                derive_tick_array_pda(&pool_id, s_curr), // Current - MUST be first
                derive_tick_array_pda(&pool_id, s_prev), // Lower ticks (swap direction)
                derive_tick_array_pda(&pool_id, s_prev2), // Even lower (continued swap)
                s_curr,
                s_prev,
                s_prev2,
            )
        } else {
            // B->A: price increases, ticks increase
            // tick_array_0 = current (contains current tick)
            // tick_array_1 = next (higher ticks - swap direction)
            // tick_array_2 = next+1 (even higher ticks - continued swap direction)
            let s_next2 = s_next + ticks_per_array;
            (
                derive_tick_array_pda(&pool_id, s_curr), // Current - MUST be first
                derive_tick_array_pda(&pool_id, s_next), // Higher ticks (swap direction)
                derive_tick_array_pda(&pool_id, s_next2), // Even higher (continued swap)
                s_curr,
                s_next,
                s_next2,
            )
        };

        tracing::debug!(
            pool = %pool_id,
            tick_array_0 = %tick_array_0,
            tick_array_1 = %tick_array_1,
            tick_array_2 = %tick_array_2,
            start0 = start0,
            start1 = start1,
            start2 = start2,
            "orca: derived tick array PDAs"
        );

        let oracle = derive_oracle_pda(&pool_id);

        // Build instruction data per Orca Whirlpool Anchor IDL:
        // - discriminator: [u8; 8]
        // - amount: u64
        // - other_amount_threshold: u64
        // - sqrt_price_limit: u128
        // - amount_specified_is_input: bool
        // - a_to_b: bool
        let mut data = Vec::with_capacity(8 + 8 + 8 + 16 + 1 + 1);
        data.extend_from_slice(&SWAP_DISCRIMINATOR);
        data.extend_from_slice(&_amount_in.to_le_bytes()); // amount (exact input)
        data.extend_from_slice(&_min_out.to_le_bytes()); // other_amount_threshold (min output)
        data.extend_from_slice(&0u128.to_le_bytes()); // sqrt_price_limit (0 = no limit)
        data.push(1u8); // amount_specified_is_input = true
        data.push(if a_to_b { 1u8 } else { 0u8 }); // a_to_b

        let program_id =
            Pubkey::from_str(ORCA_WHIRLPOOL_PROGRAM).map_err(|_| anyhow!("orca program id"))?;

        // Account ordering per Orca Whirlpool swap instruction:
        // 0. token_program (SPL Token)
        // 1. token_authority (signer - user wallet)
        // 2. whirlpool (writable)
        // 3. token_owner_account_a (writable - user's token A ATA)
        // 4. token_vault_a (writable - pool's token A vault)
        // 5. token_owner_account_b (writable - user's token B ATA)
        // 6. token_vault_b (writable - pool's token B vault)
        // 7. tick_array_0 (writable)
        // 8. tick_array_1 (writable)
        // 9. tick_array_2 (writable)
        // 10. oracle (readonly)
        let token_program = Pubkey::new_from_array(spl_token::id().to_bytes());
        let accounts = vec![
            AM {
                pubkey: token_program,
                is_signer: false,
                is_writable: false,
            },
            AM {
                pubkey: authority,
                is_signer: true,
                is_writable: false,
            },
            AM {
                pubkey: pool_id,
                is_signer: false,
                is_writable: true,
            },
            AM {
                pubkey: token_owner_account_a,
                is_signer: false,
                is_writable: true,
            },
            AM {
                pubkey: pool.vault_a,
                is_signer: false,
                is_writable: true,
            },
            AM {
                pubkey: token_owner_account_b,
                is_signer: false,
                is_writable: true,
            },
            AM {
                pubkey: pool.vault_b,
                is_signer: false,
                is_writable: true,
            },
            AM {
                pubkey: tick_array_0,
                is_signer: false,
                is_writable: true,
            },
            AM {
                pubkey: tick_array_1,
                is_signer: false,
                is_writable: true,
            },
            AM {
                pubkey: tick_array_2,
                is_signer: false,
                is_writable: true,
            },
            AM {
                pubkey: oracle,
                is_signer: false,
                is_writable: false,
            },
        ];

        Ok(vec![Instruction {
            program_id,
            accounts,
            data,
        }])
    }

    /// Async version that fetches current tick_current_index from chain
    ///
    /// CRITICAL: The cached tick_current_index may be stale (price moved since cache update).
    /// Using stale tick leads to wrong tick array calculation → Error 6023 (InvalidTickArraySequence).
    /// This async version fetches the CURRENT tick from the pool on-chain.
    async fn build_swap_ix_async(
        &self,
        input_mint: &str,
        output_mint: &str,
        amount_in: u64,
        min_out: u64,
    ) -> Result<Vec<Instruction>> {
        use solana_sdk::instruction::AccountMeta as AM;
        use std::str::FromStr;

        const SWAP_DISCRIMINATOR: [u8; 8] = [0xf8, 0xc6, 0x9e, 0x91, 0xe1, 0x75, 0x87, 0xc8];

        let in_pk = Pubkey::from_str(input_mint).map_err(|_| anyhow!("bad input mint"))?;
        let out_pk = Pubkey::from_str(output_mint).map_err(|_| anyhow!("bad output mint"))?;
        let (pool_id, forward, pool) = self
            .find_pool(&in_pk, &out_pk)
            .ok_or_else(|| anyhow!("no orca pool for pair"))?;

        let authority = self
            .user_authority
            .read()
            .unwrap()
            .ok_or_else(|| anyhow!("orca user authority not set"))?;

        let a_to_b = forward;

        // CRITICAL: Use CACHED token programs from LivePoolCache - NO RPC IN HOT PATH!
        // Token programs (SPL Token vs Token-2022) are immutable per mint - cache them once.
        let spl_token_sdk = spl_token::id();

        // Use cached token programs if available, otherwise default to SPL Token
        // (Token-2022 is rare, most mints use SPL Token)
        let token_a_program = pool
            .base_mint_program
            .map(|p| spl_token::solana_program::pubkey::Pubkey::new_from_array(p.to_bytes()))
            .unwrap_or(spl_token_sdk);

        let token_b_program = pool
            .quote_mint_program
            .map(|p| spl_token::solana_program::pubkey::Pubkey::new_from_array(p.to_bytes()))
            .unwrap_or(spl_token_sdk);

        // Log if we're using cached vs default programs
        if pool.base_mint_program.is_some() || pool.quote_mint_program.is_some() {
            tracing::debug!(
                pool = %pool_id,
                token_a_program = %token_a_program,
                token_b_program = %token_b_program,
                "Orca: using CACHED token programs (no RPC)"
            );
        } else {
            tracing::warn!(
                pool = %pool_id,
                "Orca: token programs not cached, defaulting to SPL Token (potential Token-2022 issue!)"
            );
        }

        // Derive ATAs with correct token programs
        let owner_spl =
            spl_token::solana_program::pubkey::Pubkey::new_from_array(authority.to_bytes());

        let token_owner_account_a = self
            .user_token_accounts
            .get(&pool.base_mint)
            .map(|v| *v)
            .unwrap_or_else(|| {
                let mint_spl = spl_token::solana_program::pubkey::Pubkey::new_from_array(
                    pool.base_mint.to_bytes(),
                );
                let ata_spl =
                    spl_associated_token_account::get_associated_token_address_with_program_id(
                        &owner_spl,
                        &mint_spl,
                        &token_a_program,
                    );
                Pubkey::new_from_array(ata_spl.to_bytes())
            });

        let token_owner_account_b = self
            .user_token_accounts
            .get(&pool.quote_mint)
            .map(|v| *v)
            .unwrap_or_else(|| {
                let mint_spl = spl_token::solana_program::pubkey::Pubkey::new_from_array(
                    pool.quote_mint.to_bytes(),
                );
                let ata_spl =
                    spl_associated_token_account::get_associated_token_address_with_program_id(
                        &owner_spl,
                        &mint_spl,
                        &token_b_program,
                    );
                Pubkey::new_from_array(ata_spl.to_bytes())
            });

        // Use CACHED tick from LivePoolCache - NO RPC IN HOT PATH!
        // The tick was updated in real-time by Geyser subscription.
        let tick_now = pool.tick_current_index.ok_or_else(|| {
            anyhow!(
                "Orca pool {} has no cached tick_current_index - LivePoolCache not populated",
                pool_id
            )
        })?;

        let spacing = pool.tick_spacing.ok_or_else(|| {
            anyhow!(
                "Orca pool {} has no cached tick_spacing - LivePoolCache not populated",
                pool_id
            )
        })? as i32;

        let ticks_per_array = spacing * TICK_ARRAY_SIZE;

        let current_array_start = get_tick_array_start_index(tick_now, spacing);

        // Calculate position within current tick array (0 to ticks_per_array-1)
        let position_in_array = (tick_now - current_array_start).abs();
        let array_boundary_margin = ticks_per_array / 4; // 25% margin from boundary

        tracing::debug!(
            pool = %pool_id,
            tick_spacing = spacing,
            tick_current = tick_now,
            cached_tick = ?pool.tick_current_index,
            current_array_start = current_array_start,
            position_in_array = position_in_array,
            a_to_b = a_to_b,
            "orca: calculating tick arrays with FRESH tick from chain"
        );

        // IMPROVED: Select tick arrays to handle price movement in EITHER direction.
        //
        // Problem: Between fetch_current_tick() and simulation, price may move.
        // If we only select arrays in the swap direction, we fail with Error 6023.
        //
        // Solution: Always include the current array PLUS arrays in BOTH directions.
        // - tick_array_0: Current array (always contains current tick)
        // - tick_array_1: Array in swap direction (primary movement)
        // - tick_array_2: Array in OPPOSITE direction (handle reverse movement)
        //
        // This handles ~2 arrays worth of price movement in either direction.
        let (tick_array_0, tick_array_1, tick_array_2, start0, start1, start2) = {
            let s0 = current_array_start;
            // Primary direction based on swap type
            let s_primary = if a_to_b {
                s0 - ticks_per_array
            } else {
                s0 + ticks_per_array
            };
            // Opposite direction to handle price reversal
            let s_opposite = if a_to_b {
                s0 + ticks_per_array
            } else {
                s0 - ticks_per_array
            };

            // If tick is near the boundary in primary direction, extend further in that direction
            let near_low_boundary = position_in_array < array_boundary_margin;
            let near_high_boundary = position_in_array > (ticks_per_array - array_boundary_margin);

            let (s1, s2) = if a_to_b && near_low_boundary {
                // A→B swap, tick near low boundary - extend further in decreasing direction
                (s_primary, s_primary - ticks_per_array)
            } else if !a_to_b && near_high_boundary {
                // B→A swap, tick near high boundary - extend further in increasing direction
                (s_primary, s_primary + ticks_per_array)
            } else {
                // Normal case: cover both directions
                (s_primary, s_opposite)
            };

            (
                derive_tick_array_pda(&pool_id, s0),
                derive_tick_array_pda(&pool_id, s1),
                derive_tick_array_pda(&pool_id, s2),
                s0,
                s1,
                s2,
            )
        };

        tracing::debug!(
            pool = %pool_id,
            tick_array_0 = %tick_array_0,
            tick_array_1 = %tick_array_1,
            tick_array_2 = %tick_array_2,
            start0 = start0,
            start1 = start1,
            start2 = start2,
            "orca: derived tick array PDAs (async with fresh tick)"
        );

        // Validate tick arrays exist to avoid InvalidTickArraySequence (6023)
        // Hot Path: skip RPC to avoid get_multiple_accounts (I-4, I-7)
        if !self.skip_tick_array_rpc_validation.load(Ordering::Relaxed) {
            match self
                .rpc
                .rpc
                .get_multiple_accounts(&[tick_array_0, tick_array_1, tick_array_2])
                .await
            {
                Ok(accounts) => {
                    let missing: Vec<usize> = accounts
                        .iter()
                        .enumerate()
                        .filter_map(|(idx, acct)| if acct.is_none() { Some(idx) } else { None })
                        .collect();
                    if !missing.is_empty() {
                        tracing::warn!(
                            pool = %pool_id,
                            missing = ?missing,
                            "orca: missing tick array accounts (swap may fail)"
                        );
                        return Err(anyhow!("orca tick array accounts missing"));
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        pool = %pool_id,
                        error = %e,
                        "orca: failed to validate tick arrays (continuing)"
                    );
                }
            }
        }

        let oracle = derive_oracle_pda(&pool_id);

        // Build instruction data
        let mut data = Vec::with_capacity(8 + 8 + 8 + 16 + 1 + 1);
        data.extend_from_slice(&SWAP_DISCRIMINATOR);
        data.extend_from_slice(&amount_in.to_le_bytes());
        data.extend_from_slice(&min_out.to_le_bytes());
        data.extend_from_slice(&0u128.to_le_bytes()); // sqrt_price_limit = no limit
        data.push(1u8); // amount_specified_is_input = true
        data.push(if a_to_b { 1u8 } else { 0u8 }); // a_to_b

        let program_id =
            Pubkey::from_str(ORCA_WHIRLPOOL_PROGRAM).map_err(|_| anyhow!("orca program id"))?;
        let token_program = Pubkey::new_from_array(spl_token::id().to_bytes());

        let accounts = vec![
            AM {
                pubkey: token_program,
                is_signer: false,
                is_writable: false,
            },
            AM {
                pubkey: authority,
                is_signer: true,
                is_writable: false,
            },
            AM {
                pubkey: pool_id,
                is_signer: false,
                is_writable: true,
            },
            AM {
                pubkey: token_owner_account_a,
                is_signer: false,
                is_writable: true,
            },
            AM {
                pubkey: pool.vault_a,
                is_signer: false,
                is_writable: true,
            },
            AM {
                pubkey: token_owner_account_b,
                is_signer: false,
                is_writable: true,
            },
            AM {
                pubkey: pool.vault_b,
                is_signer: false,
                is_writable: true,
            },
            AM {
                pubkey: tick_array_0,
                is_signer: false,
                is_writable: true,
            },
            AM {
                pubkey: tick_array_1,
                is_signer: false,
                is_writable: true,
            },
            AM {
                pubkey: tick_array_2,
                is_signer: false,
                is_writable: true,
            },
            AM {
                pubkey: oracle,
                is_signer: false,
                is_writable: false,
            },
        ];

        Ok(vec![Instruction {
            program_id,
            accounts,
            data,
        }])
    }

    fn list_pairs(&self) -> Vec<(String, String)> {
        // Performance optimization: Filter out low-liquidity pools (<1 SOL total reserves)
        // This reduces arbitrage scan workload by ~80% with minimal missed opportunities
        const MIN_LIQUIDITY_LAMPORTS: u128 = 1_000_000_000; // 1 SOL = 1B lamports

        self.pools
            .iter()
            .filter(|entry| {
                let p = entry.value();
                // If reserves are loaded (non-zero), check liquidity threshold
                // If reserves are 0 (lazy-loaded), include pool (will be checked later)
                let total_reserves = p.reserve_base + p.reserve_quote;
                total_reserves == 0 || total_reserves >= MIN_LIQUIDITY_LAMPORTS
            })
            .map(|entry| {
                let p = entry.value();
                (p.base_mint.to_string(), p.quote_mint.to_string())
            })
            .collect()
    }

    async fn load_pool_by_address(&self, pool_address: &Pubkey) -> Result<()> {
        // Always fetch fresh data for tick_current_index accuracy
        // This is critical for correct tick array calculation
        tracing::info!(
            pool = %pool_address,
            cached = self.pools.contains_key(pool_address),
            "Fetching Orca whirlpool via RPC getAccount (always refresh for tick accuracy)"
        );

        // Fetch pool account via single getAccount RPC call
        let account = self.rpc.get_account_retry(pool_address).await?;

        tracing::debug!(
            pool = %pool_address,
            data_len = account.data.len(),
            "Orca pool account fetched, parsing whirlpool layout"
        );

        // Parse whirlpool layout
        let parsed = layout::parse_whirlpool(&account.data)
            .ok_or_else(|| anyhow::anyhow!("Failed to parse Orca whirlpool at {}", pool_address))?;

        // OPTIMIZATION: Fetch token programs ONCE during pool loading (not on every TX!)
        // This saves 2 RPC calls per swap instruction (~200-400ms latency reduction)
        let token_2022_id = Pubkey::from_str("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")?;
        let spl_token_id = Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")?;

        let (base_prog, quote_prog) = {
            // Batch fetch both mint accounts in parallel
            let (base_result, quote_result) = tokio::join!(
                self.rpc.get_account_retry(&parsed.token_mint_a),
                self.rpc.get_account_retry(&parsed.token_mint_b)
            );

            let base_prog = match base_result {
                Ok(acct) if acct.owner == token_2022_id => {
                    tracing::debug!(mint = %parsed.token_mint_a, "Orca pool: token A uses Token-2022");
                    Some(token_2022_id)
                }
                Ok(_) => Some(spl_token_id),
                Err(_) => None,
            };

            let quote_prog = match quote_result {
                Ok(acct) if acct.owner == token_2022_id => {
                    tracing::debug!(mint = %parsed.token_mint_b, "Orca pool: token B uses Token-2022");
                    Some(token_2022_id)
                }
                Ok(_) => Some(spl_token_id),
                Err(_) => None,
            };

            (base_prog, quote_prog)
        };

        // Insert/update cache with fresh data INCLUDING token programs
        let pool = OrcaPool {
            base_mint: parsed.token_mint_a,
            quote_mint: parsed.token_mint_b,
            reserve_base: 0, // Will be lazy-loaded
            reserve_quote: 0,
            fee_bps: (parsed.fee_rate / 100) as u32, // fee_rate is in hundredths of a bps
            fee_tier: None,
            tick_spacing: Some(parsed.tick_spacing),
            vault_a: parsed.token_vault_a,
            vault_b: parsed.token_vault_b,
            tick_current_index: Some(parsed.tick_current_index),
            cached_reserves: None,
            last_reserve_fetch: None,
            base_mint_program: base_prog,
            quote_mint_program: quote_prog,
        };

        let is_update = self.pools.contains_key(pool_address);
        self.pools.insert(*pool_address, pool.clone());

        // Update mint index only for new pools (avoid duplicates)
        if !is_update {
            self.mint_index
                .entry(pool.base_mint)
                .or_default()
                .push(*pool_address);
            self.mint_index
                .entry(pool.quote_mint)
                .or_default()
                .push(*pool_address);
        }

        tracing::info!(
            pool = %pool_address,
            base_mint = %pool.base_mint,
            quote_mint = %pool.quote_mint,
            tick_current = parsed.tick_current_index,
            tick_spacing = parsed.tick_spacing,
            is_update = is_update,
            pools_count = self.pools.len(),
            "Orca whirlpool {} successfully",
            if is_update { "refreshed" } else { "loaded" }
        );

        Ok(())
    }
}

impl Orca {
    fn parse_token_amount(data: &[u8]) -> u64 {
        if data.len() < 72 {
            return 0;
        }
        let mut arr = [0u8; 8];
        arr.copy_from_slice(&data[64..72]);
        u64::from_le_bytes(arr)
    }
}

// (Removed) WhirlpoolMeta replaced by canonical parser struct in layout module.

/// Orca Whirlpool constants
const TICK_ARRAY_SIZE: i32 = 88; // Number of ticks per TickArray

fn derive_tick_array_pda(pool: &Pubkey, start_tick_index: i32) -> Pubkey {
    let seeds: &[&[u8]] = &[
        b"tick_array",
        pool.as_ref(),
        &start_tick_index.to_le_bytes(),
    ];
    Pubkey::find_program_address(seeds, &Pubkey::from_str(ORCA_WHIRLPOOL_PROGRAM).unwrap()).0
}

fn derive_oracle_pda(pool: &Pubkey) -> Pubkey {
    let seeds: &[&[u8]] = &[b"oracle", pool.as_ref()];
    Pubkey::find_program_address(seeds, &Pubkey::from_str(ORCA_WHIRLPOOL_PROGRAM).unwrap()).0
}

/// Calculate the start tick index of the TickArray containing `tick_index`.
/// TickArrays are aligned on boundaries of `tick_spacing * TICK_ARRAY_SIZE`.
///
/// IMPORTANT: Orca Whirlpool tick arrays are indexed by their start_tick_index.
/// For negative ticks, we need floor division to get the correct array.
fn get_tick_array_start_index(tick_index: i32, tick_spacing: i32) -> i32 {
    let ticks_per_array = tick_spacing * TICK_ARRAY_SIZE;
    // Use euclidean div (floor division) to handle negative tick indices correctly
    // Example with tick_spacing=64, TICK_ARRAY_SIZE=88:
    //   ticks_per_array = 5632
    //   tick_index=100 -> array_index=0 -> start=0
    //   tick_index=-100 -> array_index=-1 -> start=-5632
    let array_index = tick_index.div_euclid(ticks_per_array);
    array_index * ticks_per_array
}

/// Validate that a tick array start index contains the given tick
#[allow(dead_code)]
fn tick_array_contains_tick(array_start: i32, tick_index: i32, tick_spacing: i32) -> bool {
    let ticks_per_array = tick_spacing * TICK_ARRAY_SIZE;
    let array_end = array_start + ticks_per_array;
    tick_index >= array_start && tick_index < array_end
}
