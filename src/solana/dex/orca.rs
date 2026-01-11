//! Orca Connector – Skeleton (Whirlpool/Classic)

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use super::orca_reserve_cache::{OrcaReserveCache, ReserveEntry};
use super::{Dex, Quote};
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
    cache_hits: Arc<AtomicU64>,
    cache_misses: Arc<AtomicU64>,
}

impl Orca {
    pub fn new(rpc: Arc<SolanaRpc>) -> Self {
        Self::new_with_cache(rpc, None)
    }

    /// Create Orca instance with optional persistent reserve cache.
    pub fn new_with_cache(rpc: Arc<SolanaRpc>, cache_path: Option<String>) -> Self {
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
            cache_hits: Arc::new(AtomicU64::new(0)),
            cache_misses: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Set the global user authority (signer) used for swaps.
    pub fn set_user_authority(&self, auth: Pubkey) {
        *self.user_authority.write().unwrap() = Some(auth);
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
        for p in self.pools.iter() {
            let forward = p.base_mint == *input && p.quote_mint == *output;
            let reverse = p.base_mint == *output && p.quote_mint == *input;
            if forward || reverse {
                return Some((*p.key(), forward, p.clone()));
            }
        }
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

    /// Lazy-load reserves from vaults with persistent SQLite cache fallback.
    /// Returns (reserve_base, reserve_quote) or falls back to pool's static reserves.
    /// Priority: (1) SQLite persistent cache (2) In-memory cache (3) RPC fetch
    async fn load_reserves_if_needed(&self, pool_id: &Pubkey, pool: &OrcaPool) -> (u128, u128) {
        const CACHE_TTL_SECS: u64 = 300; // 5 minutes

        // Try persistent SQLite cache first (survives restarts)
        if let Some(ref db_cache) = self.reserve_cache {
            if let Ok(Some(entry)) = db_cache.get(pool_id) {
                self.cache_hits.fetch_add(1, Ordering::Relaxed);
                // Log removed - too spammy
                return (entry.reserve_base, entry.reserve_quote);
            }
        }

        // Check if we have fresh in-memory cached reserves
        if let Some((cached_base, cached_quote)) = pool.cached_reserves {
            if let Some(fetch_time) = pool.last_reserve_fetch {
                if let Ok(elapsed) = fetch_time.elapsed() {
                    if elapsed.as_secs() < CACHE_TTL_SECS {
                        self.cache_hits.fetch_add(1, Ordering::Relaxed);
                        // Log removed - too spammy (hundreds per second)
                        return (cached_base, cached_quote);
                    }
                }
            }
        }

        // Cache miss: Fetch fresh reserves from RPC
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
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
                // Store in persistent cache
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
                    "orca: fresh reserves fetched via RPC"
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

    /// Replay: refresh Orca (Whirlpool) pools from a ReplayRpc store.
    /// Scans latest accounts, decodes Whirlpool states, optionally reads vault balances if present,
    /// and populates in-memory snapshots similar to live refresh.
    pub fn refresh_pools_replay(
        &self,
        replay: &crate::backtest::replay_rpc::ReplayRpc,
    ) -> anyhow::Result<()> {
        // Clear current index but keep pools map; we'll upsert
        self.mint_index.clear();
        let all = replay.all_latest();
        let mut added = 0u32;
        for (addr_str, bytes) in all.into_iter() {
            // Fast size gate to reduce decode attempts
            if bytes.len() < WHIRLPOOL_ACCOUNT_MIN_SIZE || bytes.len() > WHIRLPOOL_ACCOUNT_MAX_SIZE
            {
                continue;
            }
            if let Some(parsed) = layout::parse_whirlpool(&bytes) {
                let address = Pubkey::from_str(&addr_str).unwrap_or_else(|_| Pubkey::new_unique());
                // Try to fetch vault balances from replay if vault accounts are present in trace
                let mut reserves = (0u128, 0u128);
                let vaults =
                    replay.get_multiple_accounts(&[parsed.token_vault_a, parsed.token_vault_b]);
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
                // Insert/overwrite pool
                self.pools.insert(
                    address,
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
                    },
                );
                for m in [parsed.token_mint_a, parsed.token_mint_b] {
                    self.mint_index
                        .entry(m)
                        .or_insert_with(|| Vec::with_capacity(2))
                        .push(address);
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
            added,
            total = self.pools.len(),
            "orca.refresh_pools_replay() done"
        );
        Ok(())
    }
}

#[async_trait]
impl Dex for Orca {
    async fn refresh_pools(&self) -> Result<()> {
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
        let in_pk = Pubkey::from_str(_input_mint).map_err(|_| anyhow!("bad input mint"))?;
        let out_pk = Pubkey::from_str(_output_mint).map_err(|_| anyhow!("bad output mint"))?;
        let (pool_id, forward, pool) = self
            .find_pool(&in_pk, &out_pk)
            .ok_or_else(|| anyhow!("no orca pool for pair"))?;
        let spacing = pool.tick_spacing.unwrap_or(64) as i32;
        let tick_now = pool.tick_current_index.unwrap_or(0);
        let start0 = align_to_start(tick_now, spacing);
        let start1 = align_to_start(tick_now + spacing * 88, spacing);
        let start_1 = align_to_start(tick_now - spacing * 88, spacing);
        let tick_arrays = [start_1, start0, start1].map(|s| derive_tick_array_pda(&pool_id, s));
        let oracle = derive_oracle_pda(&pool_id);
        let mut data = Vec::with_capacity(1 + 8 + 8 + 1 + 16);
        data.push(0u8);
        data.extend_from_slice(&_amount_in.to_le_bytes());
        data.extend_from_slice(&_min_out.to_le_bytes());
        data.push(if forward { 1 } else { 0 });
        data.extend_from_slice(&0u128.to_le_bytes());
        let program_id =
            Pubkey::from_str(ORCA_WHIRLPOOL_PROGRAM).map_err(|_| anyhow!("orca program id"))?;
        let mut accounts = vec![
            AM {
                pubkey: pool_id,
                is_signer: false,
                is_writable: true,
            },
            AM {
                pubkey: pool.vault_a,
                is_signer: false,
                is_writable: true,
            },
            AM {
                pubkey: pool.vault_b,
                is_signer: false,
                is_writable: true,
            },
        ];
        if let Some(ft) = pool.fee_tier {
            accounts.push(AM {
                pubkey: ft,
                is_signer: false,
                is_writable: false,
            });
        }
        for t in &tick_arrays {
            accounts.push(AM {
                pubkey: *t,
                is_signer: false,
                is_writable: true,
            });
        }
        accounts.push(AM {
            pubkey: oracle,
            is_signer: false,
            is_writable: false,
        });
        // Real user authority & token accounts
        let authority = self
            .user_authority
            .read()
            .unwrap()
            .ok_or_else(|| anyhow!("orca user authority not set"))?;
        let (input_mint, output_mint) = if forward {
            (in_pk, out_pk)
        } else {
            (out_pk, in_pk)
        };
        // Auto-derive ATAs if not explicitly registered
        let user_source = self
            .user_token_accounts
            .get(&input_mint)
            .map(|v| *v)
            .unwrap_or_else(|| {
                // Convert to spl_token Pubkey for ATA derivation, then back to solana_sdk
                let owner_spl = spl_token::solana_program::pubkey::Pubkey::new_from_array(authority.to_bytes());
                let mint_spl = spl_token::solana_program::pubkey::Pubkey::new_from_array(input_mint.to_bytes());
                let ata_spl = spl_associated_token_account::get_associated_token_address_with_program_id(
                    &owner_spl, &mint_spl, &spl_token::id()
                );
                Pubkey::new_from_array(ata_spl.to_bytes())
            });
        let user_destination = self
            .user_token_accounts
            .get(&output_mint)
            .map(|v| *v)
            .unwrap_or_else(|| {
                let owner_spl = spl_token::solana_program::pubkey::Pubkey::new_from_array(authority.to_bytes());
                let mint_spl = spl_token::solana_program::pubkey::Pubkey::new_from_array(output_mint.to_bytes());
                let ata_spl = spl_associated_token_account::get_associated_token_address_with_program_id(
                    &owner_spl, &mint_spl, &spl_token::id()
                );
                Pubkey::new_from_array(ata_spl.to_bytes())
            });
        accounts.push(AM {
            pubkey: authority,
            is_signer: true,
            is_writable: false,
        });
        accounts.push(AM {
            pubkey: user_source,
            is_signer: false,
            is_writable: true,
        });
        accounts.push(AM {
            pubkey: user_destination,
            is_signer: false,
            is_writable: true,
        });
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
        // Check if already cached
        if self.pools.contains_key(pool_address) {
            return Ok(());
        }
        
        // Fetch pool account via single getAccount RPC call
        let account = self.rpc.get_account_retry(pool_address).await?;
        
        // Parse whirlpool layout
        let parsed = layout::parse_whirlpool(&account.data)
            .ok_or_else(|| anyhow::anyhow!("Failed to parse Orca whirlpool at {}", pool_address))?;
        
        // Insert into cache
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
        };
        
        self.pools.insert(*pool_address, pool.clone());
        
        // Update mint index
        self.mint_index
            .entry(pool.base_mint)
            .or_default()
            .push(*pool_address);
        self.mint_index
            .entry(pool.quote_mint)
            .or_default()
            .push(*pool_address);
        
        tracing::debug!(
            pool = %pool_address,
            base_mint = %pool.base_mint,
            quote_mint = %pool.quote_mint,
            "Loaded Orca whirlpool via single getAccount"
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

fn derive_tick_array_pda(pool: &Pubkey, start_tick: i32) -> Pubkey {
    let seeds: &[&[u8]] = &[b"tick_array", pool.as_ref(), &start_tick.to_le_bytes()];
    Pubkey::find_program_address(seeds, &Pubkey::from_str(ORCA_WHIRLPOOL_PROGRAM).unwrap()).0
}

fn derive_oracle_pda(pool: &Pubkey) -> Pubkey {
    let seeds: &[&[u8]] = &[b"oracle", pool.as_ref()];
    Pubkey::find_program_address(seeds, &Pubkey::from_str(ORCA_WHIRLPOOL_PROGRAM).unwrap()).0
}

fn align_to_start(tick: i32, spacing: i32) -> i32 {
    tick - (tick.rem_euclid(spacing))
}
