//! Live Pool Cache - Real-time pool state from Geyser for zero-RPC TX building
//!
//! This module provides a cache of pool states that is updated in real-time via Geyser.
//! The goal is to eliminate all RPC calls from the TX-building hot path, reducing
//! latency from 300-700ms to <50ms.
//!
//! # Vault Subscription
//!
//! The cache tracks vault accounts and notifies subscribers when new vaults are discovered.
//! This is necessary because vault accounts are SPL Token accounts owned by the Token Program,
//! not the DEX program, so they need explicit subscription in Geyser.
//!
//! # Architecture
//!
//! ```text
//! Geyser gRPC ──► LivePoolCache (DashMap per DEX)
//!                       │
//!                       ▼
//!               tx_builder.rs (reads from cache, no RPC)
//!                       │
//!                       ▼
//!               Fresh quote + TX instructions
//! ```
//!
//! # Supported DEXes
//!
//! - Orca Whirlpool: tick_current_index, sqrt_price, liquidity, vaults
//! - Raydium AMM V4: reserves, fees
//! - Raydium CPMM: reserves
//! - Meteora DLMM: active_id, bin_step, reserves
//! - PumpFun Bonding: virtual reserves
//! - PumpFun AMM (PumpSwap): reserves, pool accounts

use dashmap::DashMap;
use parking_lot::Mutex;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::watch;

use crate::ipc::DexPoolReadiness;

// Re-export parsers
use crate::solana::dex::meteora_dlmm_layout::DlmmPool;
use crate::solana::dex::orca_whirlpool_layout::{self, WhirlpoolParsed};

// ============================================================================
// DEX-specific cached state structs
// ============================================================================

/// Orca Whirlpool cached state
#[derive(Debug, Clone)]
pub struct OrcaWhirlpoolState {
    pub token_mint_a: Pubkey,
    pub token_mint_b: Pubkey,
    pub token_vault_a: Pubkey,
    pub token_vault_b: Pubkey,
    pub tick_current_index: i32,
    pub sqrt_price: u128,
    pub liquidity: u128,
    pub fee_rate: u16,
    pub protocol_fee_rate: u16,
    pub tick_spacing: u16,
    /// Vault balances (updated from vault account subscriptions)
    pub vault_a_balance: Option<u64>,
    pub vault_b_balance: Option<u64>,
    /// Token program IDs (SPL Token vs Token-2022) - CACHED to avoid RPC in hot path
    /// Detected once when pool is discovered, never changes for a mint
    pub token_a_program: Option<Pubkey>,
    pub token_b_program: Option<Pubkey>,
}

impl From<WhirlpoolParsed> for OrcaWhirlpoolState {
    fn from(p: WhirlpoolParsed) -> Self {
        Self {
            token_mint_a: p.token_mint_a,
            token_mint_b: p.token_mint_b,
            token_vault_a: p.token_vault_a,
            token_vault_b: p.token_vault_b,
            tick_current_index: p.tick_current_index,
            sqrt_price: p.sqrt_price,
            liquidity: p.liquidity,
            fee_rate: p.fee_rate,
            protocol_fee_rate: p.protocol_fee_rate,
            tick_spacing: p.tick_spacing,
            vault_a_balance: None,
            vault_b_balance: None,
            // Token programs will be set separately when discovered
            token_a_program: None,
            token_b_program: None,
        }
    }
}

/// Raydium AMM V4 cached state
#[derive(Debug, Clone)]
pub struct RaydiumAmmState {
    pub base_mint: Pubkey,
    pub quote_mint: Pubkey,
    pub coin_vault: Pubkey,
    pub pc_vault: Pubkey,
    pub base_decimals: u8,
    pub quote_decimals: u8,
    /// Vault balances
    pub coin_reserve: Option<u64>,
    pub pc_reserve: Option<u64>,
    /// Serum/OpenBook market (statisch, ändert sich nie)
    pub market_id: Pubkey,
    /// Serum accounts (bids, asks, event_queue) - werden einmal geladen
    pub serum_bids: Option<Pubkey>,
    pub serum_asks: Option<Pubkey>,
    pub serum_event_queue: Option<Pubkey>,
}

/// JetStream / MASTER [`DexPoolReadiness`] for Raydium AMM v4 (conservative, explicit).
///
/// `Ready` only when the static Serum/OpenBook accounts required for swap building are present
/// (`market_id`, bids, asks, event queue from Geyser parse or one-time cold-path fill) **and**
/// both vault reserves are known and non-zero (matches [`crate::execution::quote_calculator`] needs).
///
/// Reserves alone never imply `Ready` (swap path still needs Serum accounts — see `RaydiumSwapAccounts`).
#[must_use]
pub fn raydium_amm_readiness_for_pool_cache_update(s: &RaydiumAmmState) -> DexPoolReadiness {
    let static_ok = s.market_id != Pubkey::default()
        && s.serum_bids.is_some()
        && s.serum_asks.is_some()
        && s.serum_event_queue.is_some();
    let r_coin = s.coin_reserve.unwrap_or(0);
    let r_pc = s.pc_reserve.unwrap_or(0);
    // `coin_reserve` / `pc_reserve` are the two pool legs; both must be non-zero for a usable
    // constant-product quote (same idea as Raydium CPMM non-SOL pair).
    if static_ok && r_coin > 0 && r_pc > 0 {
        DexPoolReadiness::Ready
    } else if r_coin > 0 || r_pc > 0 {
        DexPoolReadiness::Partial
    } else {
        DexPoolReadiness::Observed
    }
}

/// Raydium CPMM cached state
#[derive(Debug, Clone)]
pub struct RaydiumCpmmState {
    pub token_0_mint: Pubkey,
    pub token_1_mint: Pubkey,
    pub token_0_vault: Pubkey,
    pub token_1_vault: Pubkey,
    pub reserve_0: Option<u64>,
    pub reserve_1: Option<u64>,
}

/// Meteora DLMM cached state
#[derive(Debug, Clone)]
pub struct MeteoraState {
    pub token_x_mint: Pubkey,
    pub token_y_mint: Pubkey,
    pub reserve_x: Pubkey,
    pub reserve_y: Pubkey,
    pub active_id: i32,
    pub bin_step: u16,
    /// Vault reserves
    pub reserve_x_balance: Option<u64>,
    pub reserve_y_balance: Option<u64>,
}

impl From<DlmmPool> for MeteoraState {
    fn from(p: DlmmPool) -> Self {
        Self {
            token_x_mint: p.token_x_mint,
            token_y_mint: p.token_y_mint,
            reserve_x: p.reserve_x,
            reserve_y: p.reserve_y,
            active_id: p.active_id,
            bin_step: p.bin_step,
            reserve_x_balance: None,
            reserve_y_balance: None,
        }
    }
}

/// PumpFun Bonding Curve cached state
#[derive(Debug, Clone)]
pub struct PumpFunState {
    pub token_mint: Pubkey,
    pub bonding_curve: Pubkey,
    pub associated_bonding_curve: Pubkey,
    pub virtual_sol_reserves: u64,
    pub virtual_token_reserves: u64,
    pub real_sol_reserves: u64,
    pub real_token_reserves: u64,
    pub complete: bool,
    /// Creator of the token (parsed from bonding curve account at offset 49-80)
    pub creator: Pubkey,
    /// Cashback enabled (byte 82 in bonding curve data). Required for SELL account layout since Feb 2026.
    pub cashback_enabled: bool,
}

/// PumpFun AMM (PumpSwap) cached state
#[derive(Debug, Clone)]
pub struct PumpAmmState {
    pub base_mint: Pubkey,
    pub quote_mint: Pubkey,
    pub pool_base_token_account: Pubkey,
    pub pool_quote_token_account: Pubkey,
    pub base_reserve: Option<u64>,
    pub quote_reserve: Option<u64>,
    /// Full list of accounts needed for swap instruction
    pub pool_accounts: Vec<Pubkey>,
    /// Creator of the token (parsed from pool account at offset 11-43)
    pub creator: Option<Pubkey>,
}

/// Meteora CPMM (DAMM V2) cached state
#[derive(Debug, Clone)]
pub struct MeteoraCpmmState {
    pub token_0_mint: Pubkey,
    pub token_1_mint: Pubkey,
    pub token_0_vault: Pubkey,
    pub token_1_vault: Pubkey,
    pub amm_config: Pubkey,
    pub observation_key: Pubkey,
    pub token_0_program: Pubkey,
    pub token_1_program: Pubkey,
    pub reserve_0: u64,
    pub reserve_1: u64,
    pub mint_0_decimals: u8,
    pub mint_1_decimals: u8,
    pub status: u8,
}

/// JetStream / MASTER [`DexPoolReadiness`] for Meteora CPMM (conservative, explicit).
///
/// This scope treats **non-zero reserves on both normalized pool legs** (base + quote per
/// [`crate::ipc::NATIVE_SOL_MINT`]-aware ordering, same idea as Raydium CPMM) as the smallest
/// load-bearing completeness signal. Pool layout fields come from Geyser parse; vault balances
/// still require both sides before `Ready`.
#[must_use]
pub fn meteora_cpmm_readiness_for_pool_cache_update(s: &MeteoraCpmmState) -> DexPoolReadiness {
    let sol = Pubkey::from_str(crate::ipc::NATIVE_SOL_MINT).unwrap_or_default();
    let r0 = s.reserve_0;
    let r1 = s.reserve_1;
    let (base_side_liq, quote_side_liq) = if s.token_1_mint == sol {
        (r0 > 0, r1 > 0)
    } else if s.token_0_mint == sol {
        (r1 > 0, r0 > 0)
    } else {
        (r0 > 0, r1 > 0)
    };
    if base_side_liq && quote_side_liq {
        DexPoolReadiness::Ready
    } else if r0 > 0 || r1 > 0 {
        DexPoolReadiness::Partial
    } else {
        DexPoolReadiness::Observed
    }
}

// ============================================================================
// Unified cached pool state enum
// ============================================================================

/// Cached pool state for any supported DEX
#[derive(Debug, Clone)]
pub enum CachedPoolState {
    Orca(OrcaWhirlpoolState),
    RaydiumAmm(RaydiumAmmState),
    RaydiumCpmm(RaydiumCpmmState),
    Meteora(MeteoraState),
    MeteoraCpmm(MeteoraCpmmState),
    PumpFun(PumpFunState),
    PumpAmm(PumpAmmState),
}

impl CachedPoolState {
    /// Get the DEX name for logging/metrics
    pub fn dex_name(&self) -> &'static str {
        match self {
            Self::Orca(_) => "orca",
            Self::RaydiumAmm(_) => "raydium",
            Self::RaydiumCpmm(_) => "raydium_cpmm",
            Self::Meteora(_) => "meteora_dlmm",
            Self::MeteoraCpmm(_) => "meteora_cpmm",
            Self::PumpFun(_) => "pumpfun",
            Self::PumpAmm(_) => "pump_amm",
        }
    }
}

// ============================================================================
// Cache entry with metadata
// ============================================================================

/// Cache entry with update timestamp and slot
#[derive(Debug, Clone)]
pub struct CacheEntry {
    pub state: CachedPoolState,
    pub slot: u64,
    pub updated_at: Instant,
}

impl CacheEntry {
    pub fn new(state: CachedPoolState, slot: u64) -> Self {
        Self {
            state,
            slot,
            updated_at: Instant::now(),
        }
    }

    /// Age in milliseconds since last update
    pub fn age_ms(&self) -> u64 {
        self.updated_at.elapsed().as_millis() as u64
    }

    /// Check if cache entry is stale (older than threshold)
    pub fn is_stale(&self, max_age_ms: u64) -> bool {
        self.age_ms() > max_age_ms
    }
}

// ============================================================================
// Main cache struct
// ============================================================================

/// Live Pool Cache - maintains real-time pool state from Geyser
///
/// Thread-safe via DashMap, designed for concurrent reads from TX builder
/// and writes from Geyser subscription task.
pub struct LivePoolCache {
    /// Pool states by pool address
    pools: DashMap<Pubkey, CacheEntry>,

    /// Vault to pool mapping (for updating reserves when vault accounts change)
    vault_to_pool: DashMap<Pubkey, (Pubkey, VaultPosition)>,

    /// Mint to token program mapping (SPL Token vs Token-2022)
    /// The owner of a mint account IS the token program
    /// This is the source of truth for token_program lookups - NO RPC needed!
    mint_programs: DashMap<Pubkey, Pubkey>,

    /// Mint decimals cache (decimals are immutable once a mint is created).
    /// Populated from Geyser TokenMintInfo events and PoolCacheUpdate metadata.
    /// This is the GEYSER-FIRST source of truth – avoids RPC calls in token_utils.
    mint_decimals: DashMap<Pubkey, u8>,

    /// Stats
    updates_total: AtomicU64,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,

    /// Max age before considering entry stale (default: 10 seconds)
    max_age_ms: u64,

    /// Watch channel sender for vault list updates
    /// Triggered when new vaults are discovered so Geyser can resubscribe
    vault_update_tx: Mutex<Option<watch::Sender<Vec<Pubkey>>>>,

    /// Count of vaults at last notification (to avoid spamming)
    last_notified_vault_count: AtomicU64,

    /// PumpSwap pool-market (pool address) → explicit readiness for cache-first `pool_accounts` use.
    /// Monotonic: [`DexPoolReadiness::merge`] on update; never stored inside `PumpAmmState` to avoid
    /// touching every constructor site.
    pool_accounts_readiness_by_market: DashMap<Pubkey, DexPoolReadiness>,

    /// PumpFun bonding-curve account → explicit [`DexPoolReadiness`] from JetStream / MASTER (Bug #36).
    /// Monotonic merge; not implied by [`Self::contains`] or non-empty bonding-curve state.
    pumpfun_bonding_readiness_by_curve: DashMap<Pubkey, DexPoolReadiness>,

    /// Raydium CPMM pool address → explicit [`DexPoolReadiness`] from JetStream / MASTER (Bug #36).
    raydium_cpmm_readiness_by_pool: DashMap<Pubkey, DexPoolReadiness>,

    /// Raydium AMM v4 pool address → explicit [`DexPoolReadiness`] from JetStream / MASTER (Bug #36).
    raydium_amm_readiness_by_pool: DashMap<Pubkey, DexPoolReadiness>,

    /// Meteora CPMM pool address → explicit [`DexPoolReadiness`] from JetStream / MASTER (Bug #36).
    meteora_cpmm_readiness_by_pool: DashMap<Pubkey, DexPoolReadiness>,
}

/// Which vault position (A/B or X/Y or 0/1) this vault represents
#[derive(Debug, Clone, Copy)]
pub enum VaultPosition {
    A, // Orca vault_a, Raydium coin_vault, Meteora reserve_x
    B, // Orca vault_b, Raydium pc_vault, Meteora reserve_y
}

impl Default for LivePoolCache {
    fn default() -> Self {
        Self::new()
    }
}

impl LivePoolCache {
    /// Create a new empty cache
    pub fn new() -> Self {
        Self {
            pools: DashMap::new(),
            vault_to_pool: DashMap::new(),
            mint_programs: DashMap::new(),
            mint_decimals: DashMap::new(),
            updates_total: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            max_age_ms: 10_000, // 10 seconds default
            vault_update_tx: Mutex::new(None),
            last_notified_vault_count: AtomicU64::new(0),
            pool_accounts_readiness_by_market: DashMap::new(),
            pumpfun_bonding_readiness_by_curve: DashMap::new(),
            raydium_cpmm_readiness_by_pool: DashMap::new(),
            raydium_amm_readiness_by_pool: DashMap::new(),
            meteora_cpmm_readiness_by_pool: DashMap::new(),
        }
    }

    /// Create cache with custom max age
    pub fn with_max_age_ms(max_age_ms: u64) -> Self {
        Self {
            max_age_ms,
            ..Self::new()
        }
    }

    // ========================================================================
    // Read API
    // ========================================================================

    /// Get cached pool state (returns None if not cached)
    pub fn get(&self, pool: &Pubkey) -> Option<CachedPoolState> {
        if let Some(entry) = self.pools.get(pool) {
            self.cache_hits.fetch_add(1, Ordering::Relaxed);
            Some(entry.state.clone())
        } else {
            self.cache_misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Get cached pool state with slot and age info
    pub fn get_with_metadata(&self, pool: &Pubkey) -> Option<(CachedPoolState, u64, u64)> {
        if let Some(entry) = self.pools.get(pool) {
            self.cache_hits.fetch_add(1, Ordering::Relaxed);
            Some((entry.state.clone(), entry.slot, entry.age_ms()))
        } else {
            self.cache_misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Check if pool is in cache
    pub fn contains(&self, pool: &Pubkey) -> bool {
        self.pools.contains_key(pool)
    }

    /// Get cache size
    pub fn len(&self) -> usize {
        self.pools.len()
    }

    /// Check if cache is empty
    pub fn is_empty(&self) -> bool {
        self.pools.is_empty()
    }

    /// Iterate over all cached pools (returns pool address and state)
    /// Useful for liquidation to find all pools of a specific DEX type.
    pub fn iter(&self) -> impl Iterator<Item = (Pubkey, CachedPoolState)> + '_ {
        self.pools
            .iter()
            .map(|entry| (*entry.key(), entry.value().state.clone()))
    }

    // ========================================================================
    // Write API (called from Geyser subscription task)
    // ========================================================================

    /// Update or insert pool state
    pub fn upsert(&self, pool: Pubkey, state: CachedPoolState, slot: u64) {
        // Register vault mappings for reserve updates
        self.register_vaults(&pool, &state);

        self.pools.insert(pool, CacheEntry::new(state, slot));
        self.updates_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Update vault balance for a pool
    pub fn update_vault_balance(&self, vault: &Pubkey, balance: u64, slot: u64) {
        if let Some(mapping) = self.vault_to_pool.get(vault) {
            let (pool_addr, position) = *mapping;
            if let Some(mut entry) = self.pools.get_mut(&pool_addr) {
                // Only update if slot is newer
                if slot >= entry.slot {
                    match &mut entry.state {
                        CachedPoolState::Orca(ref mut s) => match position {
                            VaultPosition::A => s.vault_a_balance = Some(balance),
                            VaultPosition::B => s.vault_b_balance = Some(balance),
                        },
                        CachedPoolState::RaydiumAmm(ref mut s) => match position {
                            VaultPosition::A => s.coin_reserve = Some(balance),
                            VaultPosition::B => s.pc_reserve = Some(balance),
                        },
                        CachedPoolState::RaydiumCpmm(ref mut s) => match position {
                            VaultPosition::A => s.reserve_0 = Some(balance),
                            VaultPosition::B => s.reserve_1 = Some(balance),
                        },
                        CachedPoolState::Meteora(ref mut s) => match position {
                            VaultPosition::A => s.reserve_x_balance = Some(balance),
                            VaultPosition::B => s.reserve_y_balance = Some(balance),
                        },
                        CachedPoolState::MeteoraCpmm(ref mut s) => match position {
                            VaultPosition::A => s.reserve_0 = balance,
                            VaultPosition::B => s.reserve_1 = balance,
                        },
                        CachedPoolState::PumpAmm(ref mut s) => match position {
                            VaultPosition::A => s.base_reserve = Some(balance),
                            VaultPosition::B => s.quote_reserve = Some(balance),
                        },
                        CachedPoolState::PumpFun(_) => {
                            // PumpFun bonding curve has virtual reserves in the account itself
                        }
                    }
                    entry.slot = slot;
                    entry.updated_at = Instant::now();
                }
            }
        }
    }

    /// Register vault → pool mappings for reserve updates
    fn register_vaults(&self, pool: &Pubkey, state: &CachedPoolState) {
        match state {
            CachedPoolState::Orca(s) => {
                self.vault_to_pool
                    .insert(s.token_vault_a, (*pool, VaultPosition::A));
                self.vault_to_pool
                    .insert(s.token_vault_b, (*pool, VaultPosition::B));
            }
            CachedPoolState::RaydiumAmm(s) => {
                self.vault_to_pool
                    .insert(s.coin_vault, (*pool, VaultPosition::A));
                self.vault_to_pool
                    .insert(s.pc_vault, (*pool, VaultPosition::B));
            }
            CachedPoolState::RaydiumCpmm(s) => {
                self.vault_to_pool
                    .insert(s.token_0_vault, (*pool, VaultPosition::A));
                self.vault_to_pool
                    .insert(s.token_1_vault, (*pool, VaultPosition::B));
            }
            CachedPoolState::Meteora(s) => {
                self.vault_to_pool
                    .insert(s.reserve_x, (*pool, VaultPosition::A));
                self.vault_to_pool
                    .insert(s.reserve_y, (*pool, VaultPosition::B));
            }
            CachedPoolState::MeteoraCpmm(s) => {
                self.vault_to_pool
                    .insert(s.token_0_vault, (*pool, VaultPosition::A));
                self.vault_to_pool
                    .insert(s.token_1_vault, (*pool, VaultPosition::B));
            }
            CachedPoolState::PumpAmm(s) => {
                self.vault_to_pool
                    .insert(s.pool_base_token_account, (*pool, VaultPosition::A));
                self.vault_to_pool
                    .insert(s.pool_quote_token_account, (*pool, VaultPosition::B));
            }
            CachedPoolState::PumpFun(_) => {
                // No vault accounts to register
            }
        }

        // Notify subscribers if vault count increased significantly
        self.notify_vault_update();
    }

    // ========================================================================
    // Maintenance
    // ========================================================================

    /// Remove stale entries (older than max_age_ms)
    pub fn evict_stale(&self) -> usize {
        let before = self.pools.len();
        self.pools
            .retain(|_, entry| !entry.is_stale(self.max_age_ms));
        let evicted = before.saturating_sub(self.pools.len());
        if evicted > 0 {
            tracing::debug!(evicted, "LivePoolCache: evicted stale entries");
        }
        evicted
    }

    /// Subscribe to vault list updates.
    /// Returns a receiver that will be notified when new vaults are discovered.
    /// The Geyser subscription task should use this to resubscribe when vaults change.
    pub fn subscribe_vault_updates(&self) -> watch::Receiver<Vec<Pubkey>> {
        let initial_vaults = self.get_tracked_vaults();
        let (tx, rx) = watch::channel(initial_vaults);
        *self.vault_update_tx.lock() = Some(tx);
        rx
    }

    /// Notify vault update subscribers (called when new vaults are registered)
    fn notify_vault_update(&self) {
        let current_count = self.vault_to_pool.len() as u64;
        let last_count = self.last_notified_vault_count.load(Ordering::Relaxed);

        // Only notify if vault count increased by at least 10 (avoid spamming)
        if current_count > last_count + 10 {
            if let Some(tx) = self.vault_update_tx.lock().as_ref() {
                let vaults = self.get_tracked_vaults();
                let _ = tx.send(vaults);
                self.last_notified_vault_count
                    .store(current_count, Ordering::Relaxed);
                tracing::debug!(
                    old_count = last_count,
                    new_count = current_count,
                    "LivePoolCache: notified vault update subscribers"
                );
            }
        }
    }

    /// Get all tracked vault addresses (for Geyser subscription)
    pub fn get_tracked_vaults(&self) -> Vec<Pubkey> {
        self.vault_to_pool.iter().map(|e| *e.key()).collect()
    }

    /// Get all tracked mint addresses (for Geyser subscription to detect token programs)
    /// Returns unique mints from all cached pools
    pub fn get_tracked_mints(&self) -> Vec<Pubkey> {
        use std::collections::HashSet;
        let mut mints = HashSet::new();

        for entry in self.pools.iter() {
            match &entry.value().state {
                CachedPoolState::Orca(s) => {
                    mints.insert(s.token_mint_a);
                    mints.insert(s.token_mint_b);
                }
                CachedPoolState::Meteora(s) => {
                    mints.insert(s.token_x_mint);
                    mints.insert(s.token_y_mint);
                }
                CachedPoolState::MeteoraCpmm(s) => {
                    mints.insert(s.token_0_mint);
                    mints.insert(s.token_1_mint);
                }
                CachedPoolState::RaydiumAmm(s) => {
                    mints.insert(s.base_mint);
                    mints.insert(s.quote_mint);
                }
                CachedPoolState::RaydiumCpmm(s) => {
                    mints.insert(s.token_0_mint);
                    mints.insert(s.token_1_mint);
                }
                CachedPoolState::PumpFun(s) => {
                    mints.insert(s.token_mint);
                }
                CachedPoolState::PumpAmm(s) => {
                    mints.insert(s.base_mint);
                    mints.insert(s.quote_mint);
                }
            }
        }

        mints.into_iter().collect()
    }

    /// Get the token program for a mint (SPL Token or Token-2022)
    /// Returns None if the mint has not been seen yet
    /// This is the GEYSER-FIRST source of truth - NO RPC calls needed!
    pub fn get_mint_program(&self, mint: &Pubkey) -> Option<Pubkey> {
        self.mint_programs.get(mint).map(|r| *r)
    }

    /// Get cached mint decimals (immutable once set – decimals never change for a mint)
    /// Returns None if the mint has not been seen yet via Geyser TokenMintInfo.
    /// This is the GEYSER-FIRST source of truth – NO RPC calls needed!
    pub fn get_mint_decimals(&self, mint: &Pubkey) -> Option<u8> {
        self.mint_decimals.get(mint).map(|r| *r)
    }

    /// Set mint decimals (called from Geyser TokenMintInfo or PoolCacheUpdate processing).
    /// Decimals are immutable on-chain, so we never need to overwrite.
    pub fn set_mint_decimals(&self, mint: Pubkey, decimals: u8) {
        self.mint_decimals.entry(mint).or_insert(decimals);
    }

    /// Fields from cached PumpFun bonding curve relevant to SELL simulation / recovery waits.
    ///
    /// Used by execution-engine to detect JetStream-applied `PoolCacheUpdate` after market-data
    /// RPC refresh (Cold Path recovery).
    pub fn pumpfun_bonding_curve_reserves_snapshot(
        &self,
        bonding_curve: &Pubkey,
    ) -> Option<(u64, u64, u64, u64, bool, bool)> {
        match self.get(bonding_curve)? {
            CachedPoolState::PumpFun(s) => Some((
                s.virtual_token_reserves,
                s.virtual_sol_reserves,
                s.real_token_reserves,
                s.real_sol_reserves,
                s.complete,
                s.cashback_enabled,
            )),
            _ => None,
        }
    }

    /// Get the creator from a PumpFun bonding curve cached state
    /// Returns None if the bonding curve is not cached or is not a PumpFun pool
    /// This is the GEYSER-FIRST source of truth - NO RPC calls needed!
    pub fn get_pumpfun_creator(&self, bonding_curve: &Pubkey) -> Option<Pubkey> {
        if let Some(entry) = self.pools.get(bonding_curve) {
            if let CachedPoolState::PumpFun(state) = &entry.state {
                // Only return if creator is not default (was actually parsed)
                if state.creator != Pubkey::default() {
                    return Some(state.creator);
                }
            }
        }
        None
    }

    /// Check if a PumpFun bonding curve for a given mint is marked as `complete`.
    /// Returns Some(true) if complete, Some(false) if not, None if not found.
    pub fn is_pumpfun_complete_for_mint(&self, mint: &Pubkey) -> Option<bool> {
        for entry in self.pools.iter() {
            if let CachedPoolState::PumpFun(s) = &entry.value().state {
                if s.token_mint == *mint {
                    return Some(s.complete);
                }
            }
        }
        None
    }

    /// Update token program for a mint (called when Geyser receives mint account update)
    /// The owner of a mint account IS the token program (SPL Token or Token-2022)
    pub fn update_mint_program(&self, mint: &Pubkey, token_program: Pubkey) {
        // Store in the central mint_programs map
        self.mint_programs.insert(*mint, token_program);

        // Also update pool states that reference this mint (for backwards compat)
        for mut entry in self.pools.iter_mut() {
            let updated = match &mut entry.value_mut().state {
                CachedPoolState::Orca(s) => {
                    let mut changed = false;
                    if s.token_mint_a == *mint && s.token_a_program != Some(token_program) {
                        s.token_a_program = Some(token_program);
                        changed = true;
                    }
                    if s.token_mint_b == *mint && s.token_b_program != Some(token_program) {
                        s.token_b_program = Some(token_program);
                        changed = true;
                    }
                    changed
                }
                CachedPoolState::Meteora(_) => {
                    // TODO: Add token_x_program/token_y_program to MeteoraState
                    false
                }
                _ => false, // Other DEXes don't need Token-2022 detection yet
            };

            if updated {
                tracing::debug!(
                    mint = %mint,
                    token_program = %token_program,
                    pool = %entry.key(),
                    "LivePoolCache: updated token program for mint"
                );
            }
        }
    }

    /// Set PumpSwap AMM pool_accounts for an existing PumpAmm cache entry.
    ///
    /// Called when:
    /// 1. execution-engine receives DexPoolAccounts events from market-data via NATS
    /// 2. Liquidation Phase 2 discovers pool_accounts via RPC
    ///
    /// Without this, PumpAmm entries parsed from Geyser have empty pool_accounts,
    /// making them unusable for tx building.
    pub fn set_pump_amm_pool_accounts(&self, pool: &Pubkey, accounts: Vec<Pubkey>) {
        if let Some(mut entry) = self.pools.get_mut(pool) {
            if let CachedPoolState::PumpAmm(ref mut s) = entry.value_mut().state {
                let was_empty = s.pool_accounts.is_empty();
                s.pool_accounts = accounts.clone();
                if was_empty {
                    tracing::info!(
                        pool = %pool,
                        accounts_len = accounts.len(),
                        "LivePoolCache: PumpAmm pool_accounts populated (was empty)"
                    );
                } else {
                    tracing::debug!(
                        pool = %pool,
                        accounts_len = accounts.len(),
                        "LivePoolCache: PumpAmm pool_accounts updated"
                    );
                }
            }
        } else {
            tracing::debug!(
                pool = %pool,
                "LivePoolCache: set_pump_amm_pool_accounts called but pool not in cache"
            );
        }
    }

    /// Set Raydium AMM Serum/OpenBook accounts (bids, asks, event_queue).
    ///
    /// These accounts are static (never change for a given pool) and only need
    /// to be fetched once per pool lifetime. Called after a one-time RPC fetch
    /// in tx_builder or market-data.
    pub fn set_raydium_serum_accounts(
        &self,
        pool: &Pubkey,
        serum_bids: Pubkey,
        serum_asks: Pubkey,
        serum_event_queue: Pubkey,
    ) {
        if let Some(mut entry) = self.pools.get_mut(pool) {
            if let CachedPoolState::RaydiumAmm(ref mut s) = entry.value_mut().state {
                let was_none = s.serum_bids.is_none();
                s.serum_bids = Some(serum_bids);
                s.serum_asks = Some(serum_asks);
                s.serum_event_queue = Some(serum_event_queue);
                if was_none {
                    tracing::info!(
                        pool = %pool,
                        bids = %serum_bids,
                        asks = %serum_asks,
                        event_queue = %serum_event_queue,
                        "LivePoolCache: Raydium serum accounts populated"
                    );
                }
            }
        }
    }

    /// Get PumpAmm pool_accounts by pool Pubkey from cache.
    ///
    /// Returns `Some(Vec<Pubkey>)` if the pool exists and has non-empty pool_accounts.
    /// Used by market-data as fallback when Geyser-parsed state has empty pool_accounts.
    pub fn get_pump_amm_pool_accounts(&self, pool: &Pubkey) -> Option<Vec<Pubkey>> {
        if let Some(entry) = self.pools.get(pool) {
            if let CachedPoolState::PumpAmm(ref s) = entry.value().state {
                if !s.pool_accounts.is_empty() {
                    return Some(s.pool_accounts.clone());
                }
            }
        }
        None
    }

    /// Lookup PumpAmm pool by base_mint.
    ///
    /// Returns the pool address for a PumpAmm entry that has the given base_mint.
    /// Used by liquidation to find PumpSwap AMM pools for tokens.
    pub fn find_pump_amm_pool_by_base_mint(&self, base_mint: &Pubkey) -> Option<Pubkey> {
        for entry in self.pools.iter() {
            if let CachedPoolState::PumpAmm(ref s) = entry.value().state {
                if s.base_mint == *base_mint {
                    return Some(*entry.key());
                }
            }
        }
        None
    }

    /// Mark a PumpFun bonding curve as `complete` for a given mint.
    /// Called when on-chain simulation returns 6005 (BondingCurveComplete) so
    /// subsequent attempts use Multi-Pool (PumpSwap AMM) instead.
    pub fn mark_pumpfun_complete_for_mint(&self, mint: &Pubkey) -> bool {
        for mut entry in self.pools.iter_mut() {
            if let CachedPoolState::PumpFun(ref mut s) = entry.value_mut().state {
                if s.token_mint == *mint && !s.complete {
                    s.complete = true;
                    return true;
                }
            }
        }
        false
    }

    /// Get PumpAmm reserves (base, quote) for a given base_mint from cache.
    ///
    /// Returns `Some((base_reserve, quote_reserve, pool_market))` if both reserves
    /// are available in the cache, `None` otherwise.
    /// This allows PumpFunAmmDex to skip RPC `get_vault_amount` calls when the cache
    /// already has fresh Geyser data.
    pub fn get_pump_amm_reserves_by_base_mint(
        &self,
        base_mint: &Pubkey,
    ) -> Option<(u64, u64, Pubkey)> {
        for entry in self.pools.iter() {
            if let CachedPoolState::PumpAmm(ref s) = entry.value().state {
                if s.base_mint == *base_mint {
                    if let (Some(base_r), Some(quote_r)) = (s.base_reserve, s.quote_reserve) {
                        return Some((base_r, quote_r, *entry.key()));
                    }
                }
            }
        }
        None
    }

    /// Get the pool market address for a PumpAmm pool by base_mint, regardless of whether
    /// pool_accounts or reserves are populated. Used as a fast-path in discover_pool_static
    /// to do a single getAccount instead of the slow getProgramAccounts scan.
    pub fn get_pump_amm_pool_address_by_base_mint(&self, base_mint: &Pubkey) -> Option<Pubkey> {
        for entry in self.pools.iter() {
            if let CachedPoolState::PumpAmm(ref s) = entry.value().state {
                if s.base_mint == *base_mint {
                    return Some(*entry.key());
                }
            }
        }
        None
    }

    /// Get all PumpAmm pools that have empty pool_accounts.
    /// Returns Vec<(pool_address, base_mint)> for pools that need pool_accounts seeding.
    pub fn get_pump_amm_pools_without_accounts(&self) -> Vec<(Pubkey, Pubkey)> {
        let mut result = Vec::new();
        for entry in self.pools.iter() {
            if let CachedPoolState::PumpAmm(ref s) = entry.value().state {
                if s.pool_accounts.is_empty() {
                    result.push((*entry.key(), s.base_mint));
                }
            }
        }
        result
    }

    /// Get PumpAmm pool_accounts for a given base_mint from cache.
    ///
    /// Returns `Some(Vec<Pubkey>)` if the pool_accounts are non-empty in the cache.
    /// This allows PumpFunAmmDex to skip the expensive RPC-based `discover_pool_static`
    /// when DexPoolAccounts have already been received from Geyser/market-data.
    pub fn get_pump_amm_pool_accounts_by_base_mint(
        &self,
        base_mint: &Pubkey,
    ) -> Option<Vec<Pubkey>> {
        for entry in self.pools.iter() {
            if let CachedPoolState::PumpAmm(ref s) = entry.value().state {
                if s.base_mint == *base_mint && !s.pool_accounts.is_empty() {
                    return Some(s.pool_accounts.clone());
                }
            }
        }
        None
    }

    /// Merge PumpSwap `pool_accounts` readiness for `pool_market` (monotonic — never downgrade).
    pub fn merge_pump_amm_pool_accounts_readiness(
        &self,
        pool_market: Pubkey,
        incoming: DexPoolReadiness,
    ) {
        self.pool_accounts_readiness_by_market
            .entry(pool_market)
            .and_modify(|stored| *stored = stored.merge(incoming))
            .or_insert(incoming);
    }

    /// Merge PumpFun bonding-curve readiness for `bonding_curve` (monotonic — never downgrade).
    pub fn merge_pumpfun_bonding_readiness(
        &self,
        bonding_curve: Pubkey,
        incoming: DexPoolReadiness,
    ) {
        self.pumpfun_bonding_readiness_by_curve
            .entry(bonding_curve)
            .and_modify(|stored| *stored = stored.merge(incoming))
            .or_insert(incoming);
    }

    /// Merge Raydium CPMM pool readiness for `pool` (monotonic — never downgrade).
    pub fn merge_raydium_cpmm_pool_readiness(&self, pool: Pubkey, incoming: DexPoolReadiness) {
        self.raydium_cpmm_readiness_by_pool
            .entry(pool)
            .and_modify(|stored| *stored = stored.merge(incoming))
            .or_insert(incoming);
    }

    /// Merge Raydium AMM v4 pool readiness for `pool` (monotonic — never downgrade).
    pub fn merge_raydium_amm_pool_readiness(&self, pool: Pubkey, incoming: DexPoolReadiness) {
        self.raydium_amm_readiness_by_pool
            .entry(pool)
            .and_modify(|stored| *stored = stored.merge(incoming))
            .or_insert(incoming);
    }

    /// Merge Meteora CPMM pool readiness for `pool` (monotonic — never downgrade).
    pub fn merge_meteora_cpmm_pool_readiness(&self, pool: Pubkey, incoming: DexPoolReadiness) {
        self.meteora_cpmm_readiness_by_pool
            .entry(pool)
            .and_modify(|stored| *stored = stored.merge(incoming))
            .or_insert(incoming);
    }

    /// `true` only when JetStream merge recorded [`DexPoolReadiness::Ready`] for this Raydium CPMM pool.
    #[must_use]
    pub fn raydium_cpmm_pool_explicitly_ready(&self, pool: &Pubkey) -> bool {
        self.raydium_cpmm_readiness_by_pool
            .get(pool)
            .map(|r| *r == DexPoolReadiness::Ready)
            .unwrap_or(false)
    }

    /// Monotonic merge result for this pool (tests / diagnostics).
    #[must_use]
    pub fn raydium_cpmm_readiness(&self, pool: &Pubkey) -> Option<DexPoolReadiness> {
        self.raydium_cpmm_readiness_by_pool.get(pool).map(|r| *r)
    }

    /// `true` only when JetStream merge recorded [`DexPoolReadiness::Ready`] for this Raydium AMM pool.
    #[must_use]
    pub fn raydium_amm_pool_explicitly_ready(&self, pool: &Pubkey) -> bool {
        self.raydium_amm_readiness_by_pool
            .get(pool)
            .map(|r| *r == DexPoolReadiness::Ready)
            .unwrap_or(false)
    }

    /// Monotonic merge result for this Raydium AMM pool (tests / diagnostics).
    #[must_use]
    pub fn raydium_amm_readiness(&self, pool: &Pubkey) -> Option<DexPoolReadiness> {
        self.raydium_amm_readiness_by_pool.get(pool).map(|r| *r)
    }

    /// `true` only when JetStream merge recorded [`DexPoolReadiness::Ready`] for this Meteora CPMM pool.
    #[must_use]
    pub fn meteora_cpmm_pool_explicitly_ready(&self, pool: &Pubkey) -> bool {
        self.meteora_cpmm_readiness_by_pool
            .get(pool)
            .map(|r| *r == DexPoolReadiness::Ready)
            .unwrap_or(false)
    }

    /// Monotonic merge result for this Meteora CPMM pool (tests / diagnostics).
    #[must_use]
    pub fn meteora_cpmm_readiness(&self, pool: &Pubkey) -> Option<DexPoolReadiness> {
        self.meteora_cpmm_readiness_by_pool.get(pool).map(|r| *r)
    }

    /// Mint-level helper: Raydium CPMM slice — explicit `Ready` only (no cache-hit heuristic).
    #[must_use]
    pub fn base_mint_has_explicit_raydium_cpmm_ready_pool(&self, base_mint: &Pubkey) -> bool {
        for entry in self.pools.iter() {
            if let CachedPoolState::RaydiumCpmm(s) = &entry.value().state {
                if s.token_0_mint != *base_mint && s.token_1_mint != *base_mint {
                    continue;
                }
                let pool = entry.key();
                if self.raydium_cpmm_pool_explicitly_ready(pool) {
                    return true;
                }
            }
        }
        false
    }

    /// Mint-level helper: Meteora CPMM slice — explicit `Ready` only (no cache-hit heuristic).
    #[must_use]
    pub fn base_mint_has_explicit_meteora_cpmm_ready_pool(&self, base_mint: &Pubkey) -> bool {
        for entry in self.pools.iter() {
            if let CachedPoolState::MeteoraCpmm(s) = &entry.value().state {
                if s.token_0_mint != *base_mint && s.token_1_mint != *base_mint {
                    continue;
                }
                let pool = entry.key();
                if self.meteora_cpmm_pool_explicitly_ready(pool) {
                    return true;
                }
            }
        }
        false
    }

    /// Mint-level helper: Raydium AMM v4 slice — explicit `Ready` only (no cache-hit heuristic).
    #[must_use]
    pub fn base_mint_has_explicit_raydium_amm_ready_pool(&self, base_mint: &Pubkey) -> bool {
        for entry in self.pools.iter() {
            if let CachedPoolState::RaydiumAmm(s) = &entry.value().state {
                if s.base_mint != *base_mint && s.quote_mint != *base_mint {
                    continue;
                }
                let pool = entry.key();
                if self.raydium_amm_pool_explicitly_ready(pool) {
                    return true;
                }
            }
        }
        false
    }

    /// Cold-path bootstrap: Raydium CPMM pools in cache that list `mint` as token_0 or token_1.
    /// Bounded iteration over the in-memory cache only (no chain scan).
    #[must_use]
    pub fn raydium_cpmm_pools_for_mint(&self, mint: &Pubkey) -> Vec<(Pubkey, RaydiumCpmmState)> {
        let mut out = Vec::new();
        for entry in self.pools.iter() {
            if let CachedPoolState::RaydiumCpmm(s) = &entry.value().state {
                if s.token_0_mint == *mint || s.token_1_mint == *mint {
                    out.push((*entry.key(), s.clone()));
                }
            }
        }
        out
    }

    /// `true` only after explicit JetStream / control-path merge recorded [`DexPoolReadiness::Ready`]
    /// for this bonding curve (Bug #36: cache hit alone is not ready).
    #[must_use]
    pub fn pumpfun_bonding_curve_explicitly_ready(&self, bonding_curve: &Pubkey) -> bool {
        self.pumpfun_bonding_readiness_by_curve
            .get(bonding_curve)
            .map(|r| *r == DexPoolReadiness::Ready)
            .unwrap_or(false)
    }

    /// Hot-path readiness for PumpSwap `pool_accounts`: explicit JetStream merge wins; if **no** entry
    /// exists in `pool_accounts_readiness_by_market` (legacy direct upsert / fixtures), treat as Ready
    /// only when state looks authoritative (≥14 accounts + both reserves `Some` and non-zero).
    /// Any stored `Partial` / `Observed` / `Ready` is authoritative and is **not** upgraded by this fallback.
    fn pump_amm_effective_ready_for_cache_first_accounts(
        &self,
        pool_market: &Pubkey,
        state: &PumpAmmState,
    ) -> bool {
        match self.pool_accounts_readiness_by_market.get(pool_market) {
            Some(r) => *r == DexPoolReadiness::Ready,
            None => {
                state.pool_accounts.len() >= 14
                    && state.base_reserve.map(|b| b > 0).unwrap_or(false)
                    && state.quote_reserve.map(|q| q > 0).unwrap_or(false)
            }
        }
    }

    /// PumpSwap `pool_accounts` for hot-path cache-first use only when effectively Ready (explicit merge or legacy authoritative state).
    pub fn get_ready_pump_amm_pool_accounts_by_base_mint(
        &self,
        base_mint: &Pubkey,
    ) -> Option<Vec<Pubkey>> {
        for entry in self.pools.iter() {
            if let CachedPoolState::PumpAmm(ref s) = entry.value().state {
                if s.base_mint == *base_mint
                    && !s.pool_accounts.is_empty()
                    && self.pump_amm_effective_ready_for_cache_first_accounts(entry.key(), s)
                {
                    return Some(s.pool_accounts.clone());
                }
            }
        }
        None
    }

    /// I-24d / Bug #27: Readiness for PumpSwap quotes after authoritative `PoolCacheUpdate`.
    ///
    /// `pool_accounts` alone is insufficient: SLAVE cache can still show stale
    /// `base_reserve`/`quote_reserve` (e.g. `None` or 0) until JetStream merge completes.
    /// Matches what [`crate::execution::quote_calculator::quote_output_amount`] requires
    /// for `CachedPoolState::PumpAmm`.
    pub fn pump_amm_quote_ready_by_base_mint(&self, base_mint: &Pubkey) -> bool {
        match self.get_pump_amm_pool_accounts_by_base_mint(base_mint) {
            Some(a) if a.len() >= 14 => {}
            _ => return false,
        }
        self.get_pump_amm_reserves_by_base_mint(base_mint)
            .map(|(b, q, _)| b > 0 && q > 0)
            .unwrap_or(false)
    }

    /// Execution-engine / TX-building: PumpSwap `pool_accounts` usable only when explicitly
    /// effectively Ready (see [`Self::get_ready_pump_amm_pool_accounts_by_base_mint`]) and
    /// the v1 account list is complete (≥14). Bug #36: a non-empty cache hit is not `ready`.
    pub fn pump_amm_swap_accounts_ready_by_base_mint(&self, base_mint: &Pubkey) -> bool {
        self.get_ready_pump_amm_pool_accounts_by_base_mint(base_mint)
            .map(|a| a.len() >= 14)
            .unwrap_or(false)
    }

    /// PumpSwap only, for mint-level **explicit** readiness: a pool for `base_mint` counts iff
    /// [`Self::pool_accounts_readiness_by_market`] has [`DexPoolReadiness::Ready`] for that
    /// pool market **and** `pool_accounts` is non-empty with ≥14 entries (swap v1 layout).
    ///
    /// Does **not** use the legacy fallback inside [`Self::pump_amm_effective_ready_for_cache_first_accounts`]
    /// (no readiness-map entry but 14+ accounts + reserves). That path remains for
    /// [`Self::get_ready_pump_amm_pool_accounts_by_base_mint`] / execution; this helper is
    /// intentionally narrower for [`Self::base_mint_has_any_ready_pool`].
    #[must_use]
    pub fn base_mint_has_explicit_pump_amm_ready_pool(&self, base_mint: &Pubkey) -> bool {
        for entry in self.pools.iter() {
            if let CachedPoolState::PumpAmm(ref s) = entry.value().state {
                if s.base_mint != *base_mint {
                    continue;
                }
                let pool_market = entry.key();
                let explicit_ready = self
                    .pool_accounts_readiness_by_market
                    .get(pool_market)
                    .map(|r| *r == DexPoolReadiness::Ready)
                    .unwrap_or(false);
                if explicit_ready && !s.pool_accounts.is_empty() && s.pool_accounts.len() >= 14 {
                    return true;
                }
            }
        }
        false
    }

    /// DEX-agnostic mint-level gate: does this **base mint** currently have **at least one**
    /// pool that is explicitly ready for the wallet-relevant slices we model today?
    ///
    /// This does **not** infer readiness from mint presence, `WalletSnapshotComplete`,
    /// `tracked_mints`, or a mere cache hit (Bug #36).
    ///
    /// **Signals consumed today:**
    /// - PumpSwap: [`Self::base_mint_has_explicit_pump_amm_ready_pool`] only — explicit
    ///   [`DexPoolReadiness::Ready`] in [`Self::pool_accounts_readiness_by_market`], no legacy
    ///   effective-ready fallback (see [`Self::pump_amm_effective_ready_for_cache_first_accounts`]).
    /// - PumpFun bonding curve: [`Self::pumpfun_bonding_curve_explicitly_ready`] on the
    ///   bonding-curve account for a cached [`PumpFunState`] whose `token_mint` equals `base_mint`.
    /// - Raydium CPMM: [`Self::base_mint_has_explicit_raydium_cpmm_ready_pool`] — explicit
    ///   [`DexPoolReadiness::Ready`] in [`Self::raydium_cpmm_readiness_by_pool`] only.
    /// - Raydium AMM v4: [`Self::base_mint_has_explicit_raydium_amm_ready_pool`] — explicit
    ///   [`DexPoolReadiness::Ready`] in [`Self::raydium_amm_readiness_by_pool`] only.
    /// - Meteora CPMM: [`Self::base_mint_has_explicit_meteora_cpmm_ready_pool`] — explicit
    ///   [`DexPoolReadiness::Ready`] in [`Self::meteora_cpmm_readiness_by_pool`] only.
    ///
    /// **Conservative:** Orca, Meteora DLMM, etc. are **not** treated as ready here until they have an
    /// explicit readiness path.
    #[must_use]
    pub fn base_mint_has_any_ready_pool(&self, base_mint: &Pubkey) -> bool {
        if self.base_mint_has_explicit_pump_amm_ready_pool(base_mint) {
            return true;
        }
        if self.base_mint_has_explicit_raydium_cpmm_ready_pool(base_mint) {
            return true;
        }
        if self.base_mint_has_explicit_meteora_cpmm_ready_pool(base_mint) {
            return true;
        }
        if self.base_mint_has_explicit_raydium_amm_ready_pool(base_mint) {
            return true;
        }
        for entry in self.pools.iter() {
            if let CachedPoolState::PumpFun(s) = &entry.value().state {
                if s.token_mint == *base_mint
                    && self.pumpfun_bonding_curve_explicitly_ready(entry.key())
                {
                    return true;
                }
            }
        }
        false
    }

    // ========================================================================
    // Stats
    // ========================================================================

    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            pool_count: self.pools.len(),
            vault_mappings: self.vault_to_pool.len(),
            updates_total: self.updates_total.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.cache_misses.load(Ordering::Relaxed),
        }
    }
}

/// Cache statistics
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub pool_count: usize,
    pub vault_mappings: usize,
    pub updates_total: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
}

impl CacheStats {
    /// Cache hit rate (0.0 - 1.0)
    pub fn hit_rate(&self) -> f64 {
        let total = self.cache_hits + self.cache_misses;
        if total == 0 {
            0.0
        } else {
            self.cache_hits as f64 / total as f64
        }
    }
}

// ============================================================================
// Parsers (re-use existing parsers from geyser_pool_discovery)
// ============================================================================

/// Parse Geyser account data into CachedPoolState
pub fn parse_pool_account(owner: &Pubkey, data: &[u8]) -> Option<CachedPoolState> {
    // Known DEX program IDs
    const RAYDIUM_AMM_V4: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
    const RAYDIUM_CPMM: &str = "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C";
    const ORCA_WHIRLPOOL: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";
    const METEORA_DLMM: &str = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo";
    const METEORA_CPMM: &str = "cpmmpPFsKiR4eeYnGSuXgkhLLgGL1j5FUZoJBJU9t9D";
    const PUMPFUN_PROGRAM: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
    const PUMPFUN_AMM: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";

    let owner_str = owner.to_string();

    match owner_str.as_str() {
        ORCA_WHIRLPOOL => parse_orca_whirlpool(data),
        RAYDIUM_AMM_V4 => parse_raydium_amm(data),
        RAYDIUM_CPMM => parse_raydium_cpmm(data),
        METEORA_DLMM => parse_meteora_dlmm(data),
        METEORA_CPMM => parse_meteora_cpmm(data),
        PUMPFUN_PROGRAM => parse_pumpfun_bonding(data),
        PUMPFUN_AMM => parse_pumpamm_pool(data),
        _ => None,
    }
}

fn parse_orca_whirlpool(data: &[u8]) -> Option<CachedPoolState> {
    let parsed = orca_whirlpool_layout::parse_whirlpool(data)?;
    Some(CachedPoolState::Orca(OrcaWhirlpoolState::from(parsed)))
}

fn parse_raydium_amm(data: &[u8]) -> Option<CachedPoolState> {
    // Raydium AMM v4 account layout: 752 bytes
    if data.len() != 752 {
        return None;
    }

    // Offset 0: status (u64)
    let status = u64::from_le_bytes(data[0..8].try_into().ok()?);
    if status == 0 {
        return None;
    }

    // Offset 32: coin_decimals, Offset 40: pc_decimals
    let coin_decimals = u64::from_le_bytes(data[32..40].try_into().ok()?) as u8;
    let pc_decimals = u64::from_le_bytes(data[40..48].try_into().ok()?) as u8;

    // Offset 336: coin_vault, Offset 368: pc_vault
    let coin_vault = Pubkey::new_from_array(data[336..368].try_into().ok()?);
    let pc_vault = Pubkey::new_from_array(data[368..400].try_into().ok()?);

    // Offset 400: coin_mint (base), Offset 432: pc_mint (quote)
    let base_mint = Pubkey::new_from_array(data[400..432].try_into().ok()?);
    let quote_mint = Pubkey::new_from_array(data[432..464].try_into().ok()?);

    // Offset 464: market_id (Serum/OpenBook)
    let market_id = Pubkey::new_from_array(data[464..496].try_into().ok()?);

    Some(CachedPoolState::RaydiumAmm(RaydiumAmmState {
        base_mint,
        quote_mint,
        coin_vault,
        pc_vault,
        base_decimals: coin_decimals,
        quote_decimals: pc_decimals,
        coin_reserve: None,
        pc_reserve: None,
        market_id,
        serum_bids: None,
        serum_asks: None,
        serum_event_queue: None,
    }))
}

fn parse_raydium_cpmm(data: &[u8]) -> Option<CachedPoolState> {
    // CPMM pool: 1024 bytes
    if data.len() != 1024 {
        return None;
    }

    let status = data[8];
    if status == 0 {
        return None;
    }

    // Offset 73: token_0_mint, 105: token_1_mint
    let token_0_mint = Pubkey::new_from_array(data[73..105].try_into().ok()?);
    let token_1_mint = Pubkey::new_from_array(data[105..137].try_into().ok()?);

    // Offset 137: token_0_vault, 169: token_1_vault
    let token_0_vault = Pubkey::new_from_array(data[137..169].try_into().ok()?);
    let token_1_vault = Pubkey::new_from_array(data[169..201].try_into().ok()?);

    Some(CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
        token_0_mint,
        token_1_mint,
        token_0_vault,
        token_1_vault,
        reserve_0: None,
        reserve_1: None,
    }))
}

fn parse_meteora_dlmm(data: &[u8]) -> Option<CachedPoolState> {
    // LB Pair: 904 bytes
    if data.len() != 904 {
        return None;
    }

    let parsed = DlmmPool::parse(data).ok()?;
    Some(CachedPoolState::Meteora(MeteoraState::from(parsed)))
}

fn parse_meteora_cpmm(data: &[u8]) -> Option<CachedPoolState> {
    use crate::solana::dex::meteora_cpmm_layout::{CpmmPool, CPMM_POOL_SIZE};

    // Meteora CPMM pool: 397 bytes
    if data.len() < CPMM_POOL_SIZE {
        return None;
    }

    let parsed = CpmmPool::parse(data).ok()?;

    // Skip inactive pools
    if !parsed.is_active() {
        return None;
    }

    Some(CachedPoolState::MeteoraCpmm(MeteoraCpmmState {
        token_0_mint: parsed.token_0_mint,
        token_1_mint: parsed.token_1_mint,
        token_0_vault: parsed.token_0_vault,
        token_1_vault: parsed.token_1_vault,
        amm_config: parsed.amm_config,
        observation_key: parsed.observation_key,
        token_0_program: parsed.token_0_program,
        token_1_program: parsed.token_1_program,
        reserve_0: 0, // Will be updated from vault balance
        reserve_1: 0, // Will be updated from vault balance
        mint_0_decimals: parsed.mint_0_decimals,
        mint_1_decimals: parsed.mint_1_decimals,
        status: parsed.status,
    }))
}

fn parse_pumpfun_bonding(data: &[u8]) -> Option<CachedPoolState> {
    // PumpFun bonding curve: 81+ bytes
    // Layout: 8 bytes discriminator + fields
    if data.len() < 81 {
        return None;
    }

    // Offset 8: virtual_token_reserves (u64)
    let virtual_token_reserves = u64::from_le_bytes(data[8..16].try_into().ok()?);
    // Offset 16: virtual_sol_reserves (u64)
    let virtual_sol_reserves = u64::from_le_bytes(data[16..24].try_into().ok()?);
    // Offset 24: real_token_reserves (u64)
    let real_token_reserves = u64::from_le_bytes(data[24..32].try_into().ok()?);
    // Offset 32: real_sol_reserves (u64)
    let real_sol_reserves = u64::from_le_bytes(data[32..40].try_into().ok()?);
    // Offset 40: token_total_supply (skip)
    // Offset 48: complete (bool)
    let complete = data.get(48).map(|&b| b != 0).unwrap_or(false);

    // Offset 49: creator (Pubkey, 32 bytes) - BEFORE mint in bonding curve layout
    let creator = Pubkey::new_from_array(data[49..81].try_into().ok()?);

    // Offset 82: cashback_enabled (since Feb 2026 cashback upgrade). Older tokens have shorter data.
    let cashback_enabled = if data.len() > 82 {
        data[82] != 0
    } else {
        false
    };

    // NOTE: token_mint is NOT in bonding curve data - it's derived from the bonding_curve pubkey
    // The caller must set token_mint and bonding_curve after parsing

    // Bonding curve and associated are derived PDAs, we don't have them here
    // They need to be provided from the caller or derived
    Some(CachedPoolState::PumpFun(PumpFunState {
        token_mint: Pubkey::default(), // Will be set by caller from bonding_curve derivation
        bonding_curve: Pubkey::default(), // Will be set by caller
        associated_bonding_curve: Pubkey::default(),
        virtual_sol_reserves,
        virtual_token_reserves,
        real_sol_reserves,
        real_token_reserves,
        complete,
        creator,
        cashback_enabled,
    }))
}

fn parse_pumpamm_pool(data: &[u8]) -> Option<CachedPoolState> {
    // PumpAMM pool: 211 bytes (based on observed data)
    if data.len() < 200 {
        return None;
    }

    // Offset 8: pool_bump (u8)
    // Offset 9: index (u16)
    // Offset 11: creator (Pubkey, 32 bytes)
    let creator = Pubkey::new_from_array(data[11..43].try_into().ok()?);
    // Offset 43: base_mint (Pubkey, 32 bytes)
    let base_mint = Pubkey::new_from_array(data[43..75].try_into().ok()?);
    // Offset 75: quote_mint (Pubkey, 32 bytes)
    let quote_mint = Pubkey::new_from_array(data[75..107].try_into().ok()?);
    // Offset 107: lp_mint (Pubkey)
    // Offset 139: pool_base_token_account (Pubkey, 32 bytes)
    let pool_base_token_account = Pubkey::new_from_array(data[139..171].try_into().ok()?);
    // Offset 171: pool_quote_token_account (Pubkey, 32 bytes)
    let pool_quote_token_account = Pubkey::new_from_array(data[171..203].try_into().ok()?);

    Some(CachedPoolState::PumpAmm(PumpAmmState {
        base_mint,
        quote_mint,
        pool_base_token_account,
        pool_quote_token_account,
        base_reserve: None,
        quote_reserve: None,
        pool_accounts: vec![], // Will be populated from DexPoolAccounts event
        creator: Some(creator),
    }))
}

// ============================================================================
// Thread-safe shared cache type
// ============================================================================

/// Shared cache reference (Arc wrapper for convenience)
pub type SharedLivePoolCache = Arc<LivePoolCache>;

/// Create a new shared cache
pub fn create_shared_cache() -> SharedLivePoolCache {
    Arc::new(LivePoolCache::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_cache_basic_operations() {
        let cache = LivePoolCache::new();
        let pool = Pubkey::new_unique();

        // Initially empty
        assert!(cache.get(&pool).is_none());
        assert!(!cache.contains(&pool));

        // Insert
        let state = CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
            token_0_mint: Pubkey::new_unique(),
            token_1_mint: Pubkey::new_unique(),
            token_0_vault: Pubkey::new_unique(),
            token_1_vault: Pubkey::new_unique(),
            reserve_0: Some(1_000_000),
            reserve_1: Some(2_000_000),
        });

        cache.upsert(pool, state.clone(), 100);

        // Now present
        assert!(cache.contains(&pool));
        assert!(cache.get(&pool).is_some());
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_cache_stats() {
        let cache = LivePoolCache::new();
        let pool = Pubkey::new_unique();

        // Miss
        let _ = cache.get(&pool);

        let stats = cache.stats();
        assert_eq!(stats.cache_misses, 1);
        assert_eq!(stats.cache_hits, 0);

        // Insert and hit
        cache.upsert(
            pool,
            CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
                token_0_mint: Pubkey::new_unique(),
                token_1_mint: Pubkey::new_unique(),
                token_0_vault: Pubkey::new_unique(),
                token_1_vault: Pubkey::new_unique(),
                reserve_0: None,
                reserve_1: None,
            }),
            100,
        );
        let _ = cache.get(&pool);

        let stats = cache.stats();
        assert_eq!(stats.cache_hits, 1);
        assert_eq!(stats.updates_total, 1);
    }

    #[test]
    fn test_cache_with_metadata() {
        let cache = LivePoolCache::new();
        let pool = Pubkey::new_unique();

        let state = CachedPoolState::PumpFun(PumpFunState {
            token_mint: Pubkey::new_unique(),
            bonding_curve: Pubkey::new_unique(),
            associated_bonding_curve: Pubkey::new_unique(),
            virtual_sol_reserves: 30_000_000_000,
            virtual_token_reserves: 1_000_000_000_000_000,
            real_sol_reserves: 0,
            real_token_reserves: 793_000_000_000_000,
            complete: false,
            creator: Pubkey::new_unique(),
            cashback_enabled: false,
        });

        cache.upsert(pool, state, 12345);

        // Get with metadata
        let (_, slot, age_ms) = cache.get_with_metadata(&pool).expect("should have entry");
        assert_eq!(slot, 12345);
        assert!(age_ms < 1000); // Should be very fresh
    }

    #[test]
    fn test_cache_stale_detection() {
        let cache = LivePoolCache::with_max_age_ms(100); // 100ms max age
        let pool = Pubkey::new_unique();

        let state = CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
            token_0_mint: Pubkey::new_unique(),
            token_1_mint: Pubkey::new_unique(),
            token_0_vault: Pubkey::new_unique(),
            token_1_vault: Pubkey::new_unique(),
            reserve_0: Some(1_000_000),
            reserve_1: Some(2_000_000),
        });

        cache.upsert(pool, state, 100);

        // Get entry and check freshness
        let is_stale = {
            let entry = cache.pools.get(&pool).expect("should have entry");
            entry.is_stale(1000)
        };
        assert!(!is_stale); // Not stale within 1s
    }

    #[test]
    fn test_cache_multiple_dex_types() {
        let cache = LivePoolCache::new();

        // Orca pool
        let orca_pool = Pubkey::new_unique();
        cache.upsert(
            orca_pool,
            CachedPoolState::Orca(OrcaWhirlpoolState {
                token_mint_a: Pubkey::new_unique(),
                token_mint_b: Pubkey::new_unique(),
                token_vault_a: Pubkey::new_unique(),
                token_vault_b: Pubkey::new_unique(),
                tick_current_index: -32768,
                sqrt_price: 1_000_000_000_000,
                liquidity: 5_000_000_000,
                fee_rate: 3000, // 0.3%
                protocol_fee_rate: 300,
                tick_spacing: 64,
                vault_a_balance: Some(1_000_000_000),
                vault_b_balance: Some(2_000_000_000),
                token_a_program: None,
                token_b_program: None,
            }),
            100,
        );

        // Raydium AMM pool
        let raydium_pool = Pubkey::new_unique();
        cache.upsert(
            raydium_pool,
            CachedPoolState::RaydiumAmm(RaydiumAmmState {
                base_mint: Pubkey::new_unique(),
                quote_mint: Pubkey::new_unique(),
                coin_vault: Pubkey::new_unique(),
                pc_vault: Pubkey::new_unique(),
                base_decimals: 9,
                quote_decimals: 6,
                coin_reserve: Some(10_000_000_000),
                pc_reserve: Some(50_000_000_000),
                market_id: Pubkey::new_unique(),
                serum_bids: None,
                serum_asks: None,
                serum_event_queue: None,
            }),
            100,
        );

        // Meteora pool
        let meteora_pool = Pubkey::new_unique();
        cache.upsert(
            meteora_pool,
            CachedPoolState::Meteora(MeteoraState {
                token_x_mint: Pubkey::new_unique(),
                token_y_mint: Pubkey::new_unique(),
                reserve_x: Pubkey::new_unique(),
                reserve_y: Pubkey::new_unique(),
                active_id: 8388608,
                bin_step: 20,
                reserve_x_balance: Some(5_000_000_000),
                reserve_y_balance: Some(10_000_000_000),
            }),
            100,
        );

        // PumpAmm pool
        let pumpamm_pool = Pubkey::new_unique();
        cache.upsert(
            pumpamm_pool,
            CachedPoolState::PumpAmm(PumpAmmState {
                base_mint: Pubkey::new_unique(),
                quote_mint: Pubkey::new_unique(),
                pool_base_token_account: Pubkey::new_unique(),
                pool_quote_token_account: Pubkey::new_unique(),
                base_reserve: Some(1_000_000_000_000),
                quote_reserve: Some(50_000_000_000),
                pool_accounts: vec![],
                creator: None,
            }),
            100,
        );

        // Verify all cached
        assert_eq!(cache.len(), 4);
        assert!(cache.contains(&orca_pool));
        assert!(cache.contains(&raydium_pool));
        assert!(cache.contains(&meteora_pool));
        assert!(cache.contains(&pumpamm_pool));

        // Verify DEX names
        assert_eq!(cache.get(&orca_pool).unwrap().dex_name(), "orca");
        assert_eq!(cache.get(&raydium_pool).unwrap().dex_name(), "raydium");
        assert_eq!(cache.get(&meteora_pool).unwrap().dex_name(), "meteora_dlmm");
        assert_eq!(cache.get(&pumpamm_pool).unwrap().dex_name(), "pump_amm");
    }

    #[test]
    fn test_cache_update_overwrites() {
        let cache = LivePoolCache::new();
        let pool = Pubkey::new_unique();

        // Insert with old reserves
        cache.upsert(
            pool,
            CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
                token_0_mint: Pubkey::new_unique(),
                token_1_mint: Pubkey::new_unique(),
                token_0_vault: Pubkey::new_unique(),
                token_1_vault: Pubkey::new_unique(),
                reserve_0: Some(1_000_000),
                reserve_1: Some(2_000_000),
            }),
            100,
        );

        // Update with new reserves
        cache.upsert(
            pool,
            CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
                token_0_mint: Pubkey::new_unique(),
                token_1_mint: Pubkey::new_unique(),
                token_0_vault: Pubkey::new_unique(),
                token_1_vault: Pubkey::new_unique(),
                reserve_0: Some(3_000_000),
                reserve_1: Some(4_000_000),
            }),
            101,
        );

        // Should still be 1 entry
        assert_eq!(cache.len(), 1);

        // Should have new reserves
        if let Some(CachedPoolState::RaydiumCpmm(state)) = cache.get(&pool) {
            assert_eq!(state.reserve_0, Some(3_000_000));
            assert_eq!(state.reserve_1, Some(4_000_000));
        } else {
            panic!("Expected RaydiumCpmm state");
        }
    }

    #[test]
    fn test_vault_to_pool_registered_via_upsert() {
        let cache = LivePoolCache::new();
        let pool = Pubkey::new_unique();
        let vault_a = Pubkey::new_unique();
        let vault_b = Pubkey::new_unique();

        // Insert a state with known vaults - vaults are registered in upsert
        let state = CachedPoolState::Orca(OrcaWhirlpoolState {
            token_mint_a: Pubkey::new_unique(),
            token_mint_b: Pubkey::new_unique(),
            token_vault_a: vault_a,
            token_vault_b: vault_b,
            tick_current_index: 0,
            sqrt_price: 1_000_000_000_000,
            liquidity: 1_000_000,
            fee_rate: 3000,
            protocol_fee_rate: 300,
            tick_spacing: 64,
            vault_a_balance: Some(1_000_000),
            vault_b_balance: Some(2_000_000),
            token_a_program: None,
            token_b_program: None,
        });

        cache.upsert(pool, state, 100);

        // Vaults should now be mapped to pool
        assert!(cache.vault_to_pool.contains_key(&vault_a));
        assert!(cache.vault_to_pool.contains_key(&vault_b));

        let (found_pool, pos) = *cache.vault_to_pool.get(&vault_a).unwrap();
        assert_eq!(found_pool, pool);
        assert!(matches!(pos, VaultPosition::A));
    }

    #[test]
    fn test_mint_program_cache_via_lookup() {
        let cache = LivePoolCache::new();

        // Unknown mint returns None (no RPC fallback in tests)
        let unknown_mint = Pubkey::new_unique();
        assert_eq!(cache.get_mint_program(&unknown_mint), None);

        // mint_programs is populated via cache_geyser when mint accounts are seen
        // Here we just verify the API works
        assert!(cache.mint_programs.is_empty());
    }

    // ========================================================================
    // Phase 1.1: PumpAmm-spezifische API (A.1 Geyser-First)
    // ========================================================================

    fn make_pump_amm_state(
        base_mint: Pubkey,
        quote_mint: Pubkey,
        base_reserve: Option<u64>,
        quote_reserve: Option<u64>,
        pool_accounts: Vec<Pubkey>,
    ) -> CachedPoolState {
        CachedPoolState::PumpAmm(PumpAmmState {
            base_mint,
            quote_mint,
            pool_base_token_account: Pubkey::new_unique(),
            pool_quote_token_account: Pubkey::new_unique(),
            base_reserve,
            quote_reserve,
            pool_accounts,
            creator: None,
        })
    }

    #[test]
    fn test_get_pump_amm_reserves_by_base_mint_hit() {
        let cache = LivePoolCache::new();
        let pool_market = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::new_unique();

        cache.upsert(
            pool_market,
            make_pump_amm_state(
                base_mint,
                quote_mint,
                Some(1_000_000_000),
                Some(50_000_000_000),
                vec![],
            ),
            100,
        );

        let result = cache.get_pump_amm_reserves_by_base_mint(&base_mint);
        assert_eq!(result, Some((1_000_000_000, 50_000_000_000, pool_market)));
    }

    #[test]
    fn test_get_pump_amm_reserves_by_base_mint_miss() {
        let cache = LivePoolCache::new();
        let unknown_base_mint = Pubkey::new_unique();

        let result = cache.get_pump_amm_reserves_by_base_mint(&unknown_base_mint);
        assert_eq!(result, None);

        // Also verify: cache with other DEX type returns None for unknown mint
        let pool = Pubkey::new_unique();
        cache.upsert(
            pool,
            CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
                token_0_mint: Pubkey::new_unique(),
                token_1_mint: Pubkey::new_unique(),
                token_0_vault: Pubkey::new_unique(),
                token_1_vault: Pubkey::new_unique(),
                reserve_0: Some(1_000_000),
                reserve_1: Some(2_000_000),
            }),
            100,
        );
        let result = cache.get_pump_amm_reserves_by_base_mint(&unknown_base_mint);
        assert_eq!(result, None);
    }

    #[test]
    fn test_get_pump_amm_reserves_by_base_mint_missing_reserves() {
        let cache = LivePoolCache::new();
        let pool_market = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::new_unique();

        cache.upsert(
            pool_market,
            make_pump_amm_state(base_mint, quote_mint, None, None, vec![]),
            100,
        );

        let result = cache.get_pump_amm_reserves_by_base_mint(&base_mint);
        assert_eq!(result, None);

        // Only base_reserve set
        cache.upsert(
            pool_market,
            make_pump_amm_state(base_mint, quote_mint, Some(1_000_000_000), None, vec![]),
            101,
        );
        let result = cache.get_pump_amm_reserves_by_base_mint(&base_mint);
        assert_eq!(result, None);
    }

    #[test]
    fn test_get_pump_amm_pool_accounts_by_base_mint_hit() {
        let cache = LivePoolCache::new();
        let pool_market = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::new_unique();
        let pool_accounts: Vec<Pubkey> = (0..14).map(|_| Pubkey::new_unique()).collect();

        cache.upsert(
            pool_market,
            make_pump_amm_state(
                base_mint,
                quote_mint,
                Some(1),
                Some(1),
                pool_accounts.clone(),
            ),
            100,
        );

        let result = cache.get_pump_amm_pool_accounts_by_base_mint(&base_mint);
        assert!(result.is_some());
        let accounts = result.unwrap();
        assert_eq!(accounts.len(), 14);
        assert_eq!(accounts, pool_accounts);
    }

    /// Legacy / Eval: direct upsert with 14 pool_accounts + non-degenerate reserves, no readiness map entry.
    #[test]
    fn test_get_ready_pump_amm_pool_accounts_legacy_fixture_no_readiness_map() {
        let cache = LivePoolCache::new();
        let pool_market = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::new_unique();
        let pool_accounts: Vec<Pubkey> = (0..14).map(|_| Pubkey::new_unique()).collect();

        cache.upsert(
            pool_market,
            make_pump_amm_state(
                base_mint,
                quote_mint,
                Some(1_000_000_000),
                Some(50_000_000_000),
                pool_accounts.clone(),
            ),
            100,
        );

        let ready = cache
            .get_ready_pump_amm_pool_accounts_by_base_mint(&base_mint)
            .expect("legacy authoritative upsert should satisfy ready-only gate");
        assert_eq!(ready, pool_accounts);
    }

    #[test]
    fn test_get_ready_pump_amm_pool_accounts_explicit_partial_blocks_legacy_fallback() {
        let cache = LivePoolCache::new();
        let pool_market = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::new_unique();
        let pool_accounts: Vec<Pubkey> = (0..14).map(|_| Pubkey::new_unique()).collect();

        cache.upsert(
            pool_market,
            make_pump_amm_state(
                base_mint,
                quote_mint,
                Some(1_000_000_000),
                Some(50_000_000_000),
                pool_accounts.clone(),
            ),
            100,
        );
        cache.merge_pump_amm_pool_accounts_readiness(pool_market, DexPoolReadiness::Partial);

        assert!(
            cache
                .get_ready_pump_amm_pool_accounts_by_base_mint(&base_mint)
                .is_none(),
            "explicit Partial must not be upgraded to Ready by legacy heuristic"
        );
        assert!(
            !cache.pump_amm_swap_accounts_ready_by_base_mint(&base_mint),
            "execution-engine gate: Partial must not count as swap-ready pool_accounts"
        );
    }

    #[test]
    fn test_pump_amm_swap_accounts_ready_matches_get_ready_gate() {
        let cache = LivePoolCache::new();
        let pool_market = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::new_unique();
        let pool_accounts: Vec<Pubkey> = (0..14).map(|_| Pubkey::new_unique()).collect();

        cache.upsert(
            pool_market,
            make_pump_amm_state(
                base_mint,
                quote_mint,
                Some(1_000_000_000),
                Some(50_000_000_000),
                pool_accounts.clone(),
            ),
            100,
        );
        assert!(cache.pump_amm_swap_accounts_ready_by_base_mint(&base_mint));

        cache.merge_pump_amm_pool_accounts_readiness(pool_market, DexPoolReadiness::Observed);
        assert!(!cache.pump_amm_swap_accounts_ready_by_base_mint(&base_mint));
    }

    #[test]
    fn test_get_pump_amm_pool_accounts_by_base_mint_empty() {
        let cache = LivePoolCache::new();
        let pool_market = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::new_unique();

        cache.upsert(
            pool_market,
            make_pump_amm_state(
                base_mint,
                quote_mint,
                Some(1_000_000_000),
                Some(50_000_000_000),
                vec![],
            ),
            100,
        );

        let result = cache.get_pump_amm_pool_accounts_by_base_mint(&base_mint);
        assert_eq!(result, None);
    }

    /// I-24d: After `ControlResponse=Ok`, execution-engine must not treat `pool_accounts`
    /// alone as quote-ready — reserves must be non-degenerate (matches incident B8bvg…).
    #[test]
    fn test_pump_amm_quote_ready_requires_accounts_and_nonzero_reserves() {
        let cache = LivePoolCache::new();
        let pool_market = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::new_unique();
        let pool_accounts: Vec<Pubkey> = (0..14).map(|_| Pubkey::new_unique()).collect();

        cache.upsert(
            pool_market,
            make_pump_amm_state(
                base_mint,
                quote_mint,
                Some(1_000_000_000),
                Some(50_000_000_000),
                pool_accounts.clone(),
            ),
            100,
        );
        assert!(cache.pump_amm_quote_ready_by_base_mint(&base_mint));

        // Stale / pre-hydration: accounts present but reserves missing (quote would fail).
        cache.upsert(
            pool_market,
            make_pump_amm_state(base_mint, quote_mint, None, None, pool_accounts.clone()),
            101,
        );
        assert!(!cache.pump_amm_quote_ready_by_base_mint(&base_mint));

        // Degenerate reserves
        cache.upsert(
            pool_market,
            make_pump_amm_state(
                base_mint,
                quote_mint,
                Some(0),
                Some(0),
                pool_accounts.clone(),
            ),
            102,
        );
        assert!(!cache.pump_amm_quote_ready_by_base_mint(&base_mint));

        cache.upsert(
            pool_market,
            make_pump_amm_state(
                base_mint,
                quote_mint,
                Some(1_000_000_000),
                Some(50_000_000_000),
                pool_accounts,
            ),
            103,
        );
        assert!(cache.pump_amm_quote_ready_by_base_mint(&base_mint));
    }

    #[test]
    fn test_set_pump_amm_pool_accounts() {
        let cache = LivePoolCache::new();
        let pool_market = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::new_unique();
        let accounts_to_set: Vec<Pubkey> = (0..14).map(|_| Pubkey::new_unique()).collect();

        cache.upsert(
            pool_market,
            make_pump_amm_state(base_mint, quote_mint, Some(1), Some(1), vec![]),
            100,
        );

        cache.set_pump_amm_pool_accounts(&pool_market, accounts_to_set.clone());

        let result = cache.get_pump_amm_pool_accounts_by_base_mint(&base_mint);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), accounts_to_set);

        let by_pool = cache.get_pump_amm_pool_accounts(&pool_market);
        assert!(by_pool.is_some());
        assert_eq!(by_pool.unwrap(), accounts_to_set);
    }

    /// Discovery Request path must NOT degrade existing reserves/creator.
    /// set_pump_amm_pool_accounts only updates pool_accounts; reserves and creator stay intact.
    #[test]
    fn test_set_pump_amm_pool_accounts_preserves_reserves_and_creator() {
        let cache = LivePoolCache::new();
        let pool_market = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::new_unique();
        let creator = Pubkey::new_unique();
        let base_reserve = 1_000_000_000u64;
        let quote_reserve = 50_000_000_000u64;
        let new_accounts: Vec<Pubkey> = (0..14).map(|_| Pubkey::new_unique()).collect();

        cache.upsert(
            pool_market,
            CachedPoolState::PumpAmm(PumpAmmState {
                base_mint,
                quote_mint,
                pool_base_token_account: Pubkey::new_unique(),
                pool_quote_token_account: Pubkey::new_unique(),
                base_reserve: Some(base_reserve),
                quote_reserve: Some(quote_reserve),
                pool_accounts: vec![],
                creator: Some(creator),
            }),
            100,
        );

        cache.set_pump_amm_pool_accounts(&pool_market, new_accounts.clone());

        let (r_base, r_quote, _) = cache
            .get_pump_amm_reserves_by_base_mint(&base_mint)
            .expect("reserves must be preserved");
        assert_eq!(r_base, base_reserve);
        assert_eq!(r_quote, quote_reserve);

        let accounts = cache
            .get_pump_amm_pool_accounts_by_base_mint(&base_mint)
            .expect("pool_accounts must be set");
        assert_eq!(accounts, new_accounts);

        if let Some(CachedPoolState::PumpAmm(s)) = cache.get(&pool_market) {
            assert_eq!(s.creator, Some(creator));
        } else {
            panic!("expected PumpAmm state");
        }
    }

    #[test]
    fn test_mark_pumpfun_complete_for_mint() {
        let cache = LivePoolCache::new();
        let pool = Pubkey::new_unique();
        let token_mint = Pubkey::new_unique();

        cache.upsert(
            pool,
            CachedPoolState::PumpFun(PumpFunState {
                token_mint,
                bonding_curve: Pubkey::new_unique(),
                associated_bonding_curve: Pubkey::new_unique(),
                virtual_sol_reserves: 30_000_000_000,
                virtual_token_reserves: 1_000_000_000_000_000,
                real_sol_reserves: 0,
                real_token_reserves: 793_100_000_000_000,
                complete: false,
                creator: Pubkey::new_unique(),
                cashback_enabled: false,
            }),
            100,
        );

        let result = cache.mark_pumpfun_complete_for_mint(&token_mint);
        assert!(result);

        if let Some(CachedPoolState::PumpFun(s)) = cache.get(&pool) {
            assert!(s.complete);
        } else {
            panic!("Expected PumpFun state");
        }
    }

    #[test]
    fn test_mark_pumpfun_complete_wrong_mint() {
        let cache = LivePoolCache::new();
        let pool = Pubkey::new_unique();
        let token_mint_a = Pubkey::new_unique();
        let wrong_mint = Pubkey::new_unique();

        cache.upsert(
            pool,
            CachedPoolState::PumpFun(PumpFunState {
                token_mint: token_mint_a,
                bonding_curve: Pubkey::new_unique(),
                associated_bonding_curve: Pubkey::new_unique(),
                virtual_sol_reserves: 30_000_000_000,
                virtual_token_reserves: 1_000_000_000_000_000,
                real_sol_reserves: 0,
                real_token_reserves: 793_100_000_000_000,
                complete: false,
                creator: Pubkey::new_unique(),
                cashback_enabled: false,
            }),
            100,
        );

        let result = cache.mark_pumpfun_complete_for_mint(&wrong_mint);
        assert!(!result);

        if let Some(CachedPoolState::PumpFun(s)) = cache.get(&pool) {
            assert!(!s.complete);
        } else {
            panic!("Expected PumpFun state");
        }
    }

    fn make_pumpfun_state_for_curve(token_mint: Pubkey, bonding_curve: Pubkey) -> CachedPoolState {
        CachedPoolState::PumpFun(PumpFunState {
            token_mint,
            bonding_curve,
            associated_bonding_curve: Pubkey::new_unique(),
            virtual_sol_reserves: 30_000_000_000,
            virtual_token_reserves: 1_000_000_000_000_000,
            real_sol_reserves: 0,
            real_token_reserves: 793_100_000_000_000,
            complete: false,
            creator: Pubkey::new_unique(),
            cashback_enabled: false,
        })
    }

    #[test]
    fn test_base_mint_has_any_ready_pool_pumpswap_ready() {
        let cache = LivePoolCache::new();
        let pool_market = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::new_unique();
        let pool_accounts: Vec<Pubkey> = (0..14).map(|_| Pubkey::new_unique()).collect();

        cache.upsert(
            pool_market,
            make_pump_amm_state(
                base_mint,
                quote_mint,
                Some(1_000_000_000),
                Some(50_000_000_000),
                pool_accounts,
            ),
            100,
        );
        cache.merge_pump_amm_pool_accounts_readiness(pool_market, DexPoolReadiness::Ready);

        assert!(cache.base_mint_has_any_ready_pool(&base_mint));
        assert!(cache.base_mint_has_explicit_pump_amm_ready_pool(&base_mint));
    }

    /// Legacy authoritative PumpAmm upsert (no `pool_accounts_readiness_by_market` entry) is
    /// swap-ready via [`Self::pump_amm_swap_accounts_ready_by_base_mint`] but must **not**
    /// satisfy [`Self::base_mint_has_any_ready_pool`] (explicit Ready merge only).
    #[test]
    fn test_base_mint_has_any_ready_pool_pumpswap_legacy_without_explicit_merge_is_false() {
        let cache = LivePoolCache::new();
        let pool_market = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::new_unique();
        let pool_accounts: Vec<Pubkey> = (0..14).map(|_| Pubkey::new_unique()).collect();

        cache.upsert(
            pool_market,
            make_pump_amm_state(
                base_mint,
                quote_mint,
                Some(1_000_000_000),
                Some(50_000_000_000),
                pool_accounts,
            ),
            100,
        );

        assert!(
            cache.pump_amm_swap_accounts_ready_by_base_mint(&base_mint),
            "legacy path still counts as swap-ready for execution-engine"
        );
        assert!(
            !cache.base_mint_has_explicit_pump_amm_ready_pool(&base_mint),
            "mint-level gate: no explicit Ready merge"
        );
        assert!(!cache.base_mint_has_any_ready_pool(&base_mint));

        cache.merge_pump_amm_pool_accounts_readiness(pool_market, DexPoolReadiness::Ready);
        assert!(cache.base_mint_has_any_ready_pool(&base_mint));
        assert!(cache.base_mint_has_explicit_pump_amm_ready_pool(&base_mint));
    }

    #[test]
    fn test_base_mint_has_any_ready_pool_pumpfun_bonding_ready() {
        let cache = LivePoolCache::new();
        let bonding_curve = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();

        cache.upsert(
            bonding_curve,
            make_pumpfun_state_for_curve(base_mint, bonding_curve),
            100,
        );
        cache.merge_pumpfun_bonding_readiness(bonding_curve, DexPoolReadiness::Ready);

        assert!(cache.base_mint_has_any_ready_pool(&base_mint));
    }

    #[test]
    fn test_base_mint_has_any_ready_pool_false_when_pumpswap_observed_only() {
        let cache = LivePoolCache::new();
        let pool_market = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::new_unique();
        let pool_accounts: Vec<Pubkey> = (0..14).map(|_| Pubkey::new_unique()).collect();

        cache.upsert(
            pool_market,
            make_pump_amm_state(
                base_mint,
                quote_mint,
                Some(1_000_000_000),
                Some(50_000_000_000),
                pool_accounts,
            ),
            100,
        );
        cache.merge_pump_amm_pool_accounts_readiness(pool_market, DexPoolReadiness::Observed);

        assert!(!cache.base_mint_has_any_ready_pool(&base_mint));
    }

    #[test]
    fn test_base_mint_has_any_ready_pool_false_when_pumpswap_partial_only() {
        let cache = LivePoolCache::new();
        let pool_market = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::new_unique();
        let pool_accounts: Vec<Pubkey> = (0..14).map(|_| Pubkey::new_unique()).collect();

        cache.upsert(
            pool_market,
            make_pump_amm_state(
                base_mint,
                quote_mint,
                Some(1_000_000_000),
                Some(50_000_000_000),
                pool_accounts,
            ),
            100,
        );
        cache.merge_pump_amm_pool_accounts_readiness(pool_market, DexPoolReadiness::Partial);

        assert!(!cache.base_mint_has_any_ready_pool(&base_mint));
    }

    #[test]
    fn test_base_mint_has_any_ready_pool_false_raydium_only_in_cache() {
        let cache = LivePoolCache::new();
        let base_mint = Pubkey::new_unique();
        let other = Pubkey::new_unique();

        cache.upsert(
            Pubkey::new_unique(),
            CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
                token_0_mint: base_mint,
                token_1_mint: other,
                token_0_vault: Pubkey::new_unique(),
                token_1_vault: Pubkey::new_unique(),
                reserve_0: Some(1_000_000),
                reserve_1: Some(2_000_000),
            }),
            100,
        );

        assert!(!cache.base_mint_has_any_ready_pool(&base_mint));
    }

    #[test]
    fn test_raydium_amm_readiness_helper_reserves_only_is_partial() {
        let base = Pubkey::new_unique();
        let quote = Pubkey::from_str(crate::ipc::NATIVE_SOL_MINT).unwrap();
        let s = RaydiumAmmState {
            base_mint: base,
            quote_mint: quote,
            coin_vault: Pubkey::new_unique(),
            pc_vault: Pubkey::new_unique(),
            base_decimals: 9,
            quote_decimals: 9,
            coin_reserve: Some(100),
            pc_reserve: Some(200),
            market_id: Pubkey::new_unique(),
            serum_bids: None,
            serum_asks: None,
            serum_event_queue: None,
        };
        assert_eq!(
            raydium_amm_readiness_for_pool_cache_update(&s),
            DexPoolReadiness::Partial
        );
    }

    #[test]
    fn test_raydium_amm_readiness_helper_ready_when_static_and_both_sides_liquid() {
        let base = Pubkey::new_unique();
        let quote = Pubkey::from_str(crate::ipc::NATIVE_SOL_MINT).unwrap();
        let s = RaydiumAmmState {
            base_mint: base,
            quote_mint: quote,
            coin_vault: Pubkey::new_unique(),
            pc_vault: Pubkey::new_unique(),
            base_decimals: 9,
            quote_decimals: 9,
            coin_reserve: Some(100),
            pc_reserve: Some(200),
            market_id: Pubkey::new_unique(),
            serum_bids: Some(Pubkey::new_unique()),
            serum_asks: Some(Pubkey::new_unique()),
            serum_event_queue: Some(Pubkey::new_unique()),
        };
        assert_eq!(
            raydium_amm_readiness_for_pool_cache_update(&s),
            DexPoolReadiness::Ready
        );
    }

    #[test]
    fn test_base_mint_has_any_ready_pool_raydium_amm_explicit_ready() {
        let cache = LivePoolCache::new();
        let pool = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::from_str(crate::ipc::NATIVE_SOL_MINT).unwrap();
        cache.upsert(
            pool,
            CachedPoolState::RaydiumAmm(RaydiumAmmState {
                base_mint,
                quote_mint,
                coin_vault: Pubkey::new_unique(),
                pc_vault: Pubkey::new_unique(),
                base_decimals: 9,
                quote_decimals: 9,
                coin_reserve: Some(1),
                pc_reserve: Some(2),
                market_id: Pubkey::new_unique(),
                serum_bids: Some(Pubkey::new_unique()),
                serum_asks: Some(Pubkey::new_unique()),
                serum_event_queue: Some(Pubkey::new_unique()),
            }),
            100,
        );
        cache.merge_raydium_amm_pool_readiness(pool, DexPoolReadiness::Ready);
        assert!(cache.base_mint_has_any_ready_pool(&base_mint));
        assert!(cache.base_mint_has_explicit_raydium_amm_ready_pool(&base_mint));
    }

    #[test]
    fn test_raydium_amm_readiness_merge_monotone() {
        let cache = LivePoolCache::new();
        let pool = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::from_str(crate::ipc::NATIVE_SOL_MINT).unwrap();
        cache.upsert(
            pool,
            CachedPoolState::RaydiumAmm(RaydiumAmmState {
                base_mint,
                quote_mint,
                coin_vault: Pubkey::new_unique(),
                pc_vault: Pubkey::new_unique(),
                base_decimals: 9,
                quote_decimals: 9,
                coin_reserve: Some(1),
                pc_reserve: Some(2),
                market_id: Pubkey::new_unique(),
                serum_bids: Some(Pubkey::new_unique()),
                serum_asks: Some(Pubkey::new_unique()),
                serum_event_queue: Some(Pubkey::new_unique()),
            }),
            100,
        );
        cache.merge_raydium_amm_pool_readiness(pool, DexPoolReadiness::Ready);
        assert!(cache.base_mint_has_any_ready_pool(&base_mint));
        cache.merge_raydium_amm_pool_readiness(pool, DexPoolReadiness::Observed);
        assert!(cache.base_mint_has_any_ready_pool(&base_mint));
    }

    #[test]
    fn test_base_mint_has_any_ready_pool_raydium_cpmm_explicit_ready() {
        let cache = LivePoolCache::new();
        let pool = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::new_unique();

        cache.upsert(
            pool,
            CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
                token_0_mint: base_mint,
                token_1_mint: quote_mint,
                token_0_vault: Pubkey::new_unique(),
                token_1_vault: Pubkey::new_unique(),
                reserve_0: Some(1_000_000),
                reserve_1: Some(2_000_000),
            }),
            100,
        );
        cache.merge_raydium_cpmm_pool_readiness(pool, DexPoolReadiness::Ready);

        assert!(cache.base_mint_has_any_ready_pool(&base_mint));
        assert!(cache.base_mint_has_explicit_raydium_cpmm_ready_pool(&base_mint));
    }

    #[test]
    fn test_base_mint_has_any_ready_pool_raydium_cpmm_partial_does_not_count() {
        let cache = LivePoolCache::new();
        let pool = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::new_unique();

        cache.upsert(
            pool,
            CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
                token_0_mint: base_mint,
                token_1_mint: quote_mint,
                token_0_vault: Pubkey::new_unique(),
                token_1_vault: Pubkey::new_unique(),
                reserve_0: Some(1_000_000),
                reserve_1: Some(0),
            }),
            100,
        );
        cache.merge_raydium_cpmm_pool_readiness(pool, DexPoolReadiness::Partial);

        assert!(!cache.base_mint_has_any_ready_pool(&base_mint));
    }

    #[test]
    fn test_raydium_cpmm_readiness_merge_monotone() {
        let cache = LivePoolCache::new();
        let pool = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::new_unique();

        cache.upsert(
            pool,
            CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
                token_0_mint: base_mint,
                token_1_mint: quote_mint,
                token_0_vault: Pubkey::new_unique(),
                token_1_vault: Pubkey::new_unique(),
                reserve_0: Some(1),
                reserve_1: Some(2),
            }),
            100,
        );
        cache.merge_raydium_cpmm_pool_readiness(pool, DexPoolReadiness::Ready);
        assert!(cache.base_mint_has_any_ready_pool(&base_mint));

        cache.merge_raydium_cpmm_pool_readiness(pool, DexPoolReadiness::Observed);
        assert!(cache.base_mint_has_any_ready_pool(&base_mint));
    }

    fn make_meteora_cpmm_state(
        token_0_mint: Pubkey,
        token_1_mint: Pubkey,
        reserve_0: u64,
        reserve_1: u64,
    ) -> MeteoraCpmmState {
        MeteoraCpmmState {
            token_0_mint,
            token_1_mint,
            token_0_vault: Pubkey::new_unique(),
            token_1_vault: Pubkey::new_unique(),
            amm_config: Pubkey::new_unique(),
            observation_key: Pubkey::new_unique(),
            token_0_program: Pubkey::new_unique(),
            token_1_program: Pubkey::new_unique(),
            reserve_0,
            reserve_1,
            mint_0_decimals: 6,
            mint_1_decimals: 9,
            status: 1,
        }
    }

    #[test]
    fn test_meteora_cpmm_readiness_helper_sol_quote_both_sides_ready() {
        let base = Pubkey::new_unique();
        let quote = Pubkey::from_str(crate::ipc::NATIVE_SOL_MINT).unwrap();
        let s = make_meteora_cpmm_state(base, quote, 1_000_000, 50_000_000_000);
        assert_eq!(
            meteora_cpmm_readiness_for_pool_cache_update(&s),
            DexPoolReadiness::Ready
        );
    }

    #[test]
    fn test_meteora_cpmm_readiness_helper_sol_as_token_0_one_side_only_partial() {
        let base = Pubkey::new_unique();
        let quote = Pubkey::from_str(crate::ipc::NATIVE_SOL_MINT).unwrap();
        let s = make_meteora_cpmm_state(quote, base, 50_000_000_000, 0);
        assert_eq!(
            meteora_cpmm_readiness_for_pool_cache_update(&s),
            DexPoolReadiness::Partial
        );
    }

    #[test]
    fn test_meteora_cpmm_readiness_helper_non_sol_pair_both_sides_ready() {
        let a = Pubkey::new_unique();
        let b = Pubkey::new_unique();
        let s = make_meteora_cpmm_state(a, b, 100, 200);
        assert_eq!(
            meteora_cpmm_readiness_for_pool_cache_update(&s),
            DexPoolReadiness::Ready
        );
    }

    #[test]
    fn test_meteora_cpmm_readiness_helper_observed_when_zero_reserves() {
        let a = Pubkey::new_unique();
        let b = Pubkey::new_unique();
        let s = make_meteora_cpmm_state(a, b, 0, 0);
        assert_eq!(
            meteora_cpmm_readiness_for_pool_cache_update(&s),
            DexPoolReadiness::Observed
        );
    }

    #[test]
    fn test_base_mint_has_any_ready_pool_meteora_cpmm_cache_hit_without_explicit_merge_is_false() {
        let cache = LivePoolCache::new();
        let pool = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::from_str(crate::ipc::NATIVE_SOL_MINT).unwrap();
        cache.upsert(
            pool,
            CachedPoolState::MeteoraCpmm(make_meteora_cpmm_state(
                base_mint,
                quote_mint,
                1_000_000,
                50_000_000_000,
            )),
            100,
        );
        assert!(
            !cache.base_mint_has_any_ready_pool(&base_mint),
            "Bug #36: reserves in cache must not imply mint-level ready without explicit merge"
        );
    }

    #[test]
    fn test_base_mint_has_any_ready_pool_meteora_cpmm_explicit_ready() {
        let cache = LivePoolCache::new();
        let pool = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::from_str(crate::ipc::NATIVE_SOL_MINT).unwrap();
        cache.upsert(
            pool,
            CachedPoolState::MeteoraCpmm(make_meteora_cpmm_state(base_mint, quote_mint, 1, 2)),
            100,
        );
        cache.merge_meteora_cpmm_pool_readiness(pool, DexPoolReadiness::Ready);
        assert!(cache.base_mint_has_any_ready_pool(&base_mint));
        assert!(cache.base_mint_has_explicit_meteora_cpmm_ready_pool(&base_mint));
    }

    #[test]
    fn test_base_mint_has_any_ready_pool_meteora_cpmm_partial_does_not_count() {
        let cache = LivePoolCache::new();
        let pool = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::from_str(crate::ipc::NATIVE_SOL_MINT).unwrap();
        cache.upsert(
            pool,
            CachedPoolState::MeteoraCpmm(make_meteora_cpmm_state(base_mint, quote_mint, 1, 0)),
            100,
        );
        cache.merge_meteora_cpmm_pool_readiness(pool, DexPoolReadiness::Partial);
        assert!(!cache.base_mint_has_any_ready_pool(&base_mint));
    }

    #[test]
    fn test_meteora_cpmm_readiness_merge_monotone() {
        let cache = LivePoolCache::new();
        let pool = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::from_str(crate::ipc::NATIVE_SOL_MINT).unwrap();
        cache.upsert(
            pool,
            CachedPoolState::MeteoraCpmm(make_meteora_cpmm_state(base_mint, quote_mint, 1, 2)),
            100,
        );
        cache.merge_meteora_cpmm_pool_readiness(pool, DexPoolReadiness::Ready);
        assert!(cache.base_mint_has_any_ready_pool(&base_mint));
        cache.merge_meteora_cpmm_pool_readiness(pool, DexPoolReadiness::Observed);
        assert!(cache.base_mint_has_any_ready_pool(&base_mint));
    }

    #[test]
    fn test_base_mint_has_any_ready_pool_pumpfun_cached_without_explicit_ready() {
        let cache = LivePoolCache::new();
        let bonding_curve = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();

        cache.upsert(
            bonding_curve,
            make_pumpfun_state_for_curve(base_mint, bonding_curve),
            100,
        );

        assert!(!cache.base_mint_has_any_ready_pool(&base_mint));
    }

    #[test]
    fn test_base_mint_has_any_ready_pool_pumpfun_merge_monotone() {
        let cache = LivePoolCache::new();
        let bonding_curve = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();

        cache.upsert(
            bonding_curve,
            make_pumpfun_state_for_curve(base_mint, bonding_curve),
            100,
        );
        cache.merge_pumpfun_bonding_readiness(bonding_curve, DexPoolReadiness::Ready);
        assert!(cache.base_mint_has_any_ready_pool(&base_mint));

        cache.merge_pumpfun_bonding_readiness(bonding_curve, DexPoolReadiness::Observed);
        assert!(cache.base_mint_has_any_ready_pool(&base_mint));
    }

    #[test]
    fn test_cache_entry_age() {
        let state = CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
            token_0_mint: Pubkey::new_unique(),
            token_1_mint: Pubkey::new_unique(),
            token_0_vault: Pubkey::new_unique(),
            token_1_vault: Pubkey::new_unique(),
            reserve_0: Some(1_000_000),
            reserve_1: Some(2_000_000),
        });

        let entry = CacheEntry::new(state, 100);

        // Should be very fresh
        assert!(entry.age_ms() < 100);
        assert!(!entry.is_stale(1000));

        // Sleep briefly and check again
        std::thread::sleep(std::time::Duration::from_millis(10));
        assert!(entry.age_ms() >= 10);
    }
}
