//! Multi-Hop Arbitrage Integration for arb-strategy
//!
//! This module provides the integration layer between the multi-hop cycle finder
//! and the arb-strategy main loop.
//!
//! Design (Event-Driven):
//! - `MultiHopArbitrage` wraps PoolGraph + PoolRanker + BeamCycleFinder
//! - Updates pool graph incrementally from MarketEvents
//! - **On each pool update**: Searches for cycles through the affected tokens
//! - Converts ArbCycle → TradeIntent with swap_path
//!
//! This is event-driven, NOT interval-based, to minimize latency.

use crate::arbitrage::{
    ArbCycle, BeamCycleFinder, CycleFinderConfig, DexType, PoolEdge, PoolGraph, PoolRanker,
    QuoteProvider, RankerConfig, MAX_RETURN_BPS,
};
use crate::execution::live_pool_cache::{CachedPoolState, SharedLivePoolCache};
use crate::execution::quote_calculator;
use crate::ipc::{
    ExplicitAmount, IntentOrigin, IntentTier, PoolAlternative, RecordHeader, SwapHop, TradeIntent,
    TradeResources, TradeSide, TradingRegime,
};
use crate::metrics::{
    multi_hop_cycle_rejected_sanity_inc, multi_hop_hop_missing_quote_inc,
    multi_hop_quote_from_cache_inc, multi_hop_quote_from_trade_cache_inc,
    multi_hop_quote_ready_pools_set, multi_hop_quote_ready_wsol_edge_pools_set,
    multi_hop_return_bps_saturated_inc, multi_hop_search_no_quote_neighbors_inc,
    multi_hop_search_worker_queue_depth_set, multi_hop_searches_coalesced_inc,
    multi_hop_shadow_logged_inc, MultiHopSanityRejectReason,
};
use parking_lot::RwLock;
use solana_sdk::pubkey::Pubkey;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Notify};
use tokio::task::JoinHandle;
use tracing::{debug, info, trace, warn};

/// WSOL mint (native SOL wrapped)
pub const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";

/// Probe amount for trade-cache normalization (0.01 SOL).
const PROBE_LAMPORTS: u64 = 10_000_000;

/// Top-N WSOL pools to seed trade-cache quotes after JetStream bootstrap.
const QUOTE_WARMUP_TOP_N: usize = 1000;

/// Hard cap on trade-cache entries after TTL sweep (evict oldest).
const MAX_QUOTE_CACHE_ENTRIES: usize = 50_000;

/// TTL sweep interval for the search worker (cold path).
const QUOTE_CACHE_CLEANUP_INTERVAL: Duration = Duration::from_secs(60);

/// Max dirty tokens queued before new mints are dropped (existing mints still coalesce).
const MAX_DIRTY_TOKENS: usize = 10_000;

/// Intents produced by the search worker (includes originating event metadata).
#[derive(Debug, Clone)]
pub struct MultiHopIntentBatch {
    pub intents: Vec<TradeIntent>,
    pub slot: Option<u64>,
    pub seen_at_ms: u64,
}

/// Incrementally maintained set of pools with a fresh trade-cache or LivePoolCache quote.
#[derive(Debug, Default)]
pub struct QuoteReadyIndex {
    pools: RwLock<HashSet<Pubkey>>,
}

impl QuoteReadyIndex {
    pub fn mark_ready(&self, pool: Pubkey) {
        self.pools.write().insert(pool);
    }

    pub fn remove(&self, pool: &Pubkey) {
        self.pools.write().remove(pool);
    }

    pub fn contains(&self, pool: &Pubkey) -> bool {
        self.pools.read().contains(pool)
    }

    pub fn len(&self) -> usize {
        self.pools.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.pools.read().is_empty()
    }

    pub fn snapshot_keys(&self) -> Vec<Pubkey> {
        self.pools.read().iter().copied().collect()
    }
}

/// Multi-hop arbitrage configuration
#[derive(Debug, Clone)]
pub struct MultiHopConfig {
    /// Enable multi-hop arbitrage (default: false for gradual rollout)
    pub enabled: bool,
    /// Maximum hops in a cycle (default: 4)
    pub max_hops: usize,
    /// Beam width for search (default: 50)
    pub beam_width: usize,
    /// Minimum profit in basis points (default: 30 = 0.3%)
    pub min_profit_bps: i32,
    /// Maximum cycles to return per search (default: 3)
    pub max_cycles: usize,
    /// Pool alternatives to keep per hop (default: 3)
    pub pool_alternatives: usize,
    /// Minimum liquidity in USD for a pool to be included (default: 1000)
    pub min_liquidity_usd: f64,
    /// Input amount for arbitrage in lamports (default: 100_000_000 = 0.1 SOL)
    pub input_amount_lamports: u64,
    /// Shadow mode: log opportunities but don't generate intents (default: true)
    pub shadow_mode: bool,
    /// Minimum price change in bps to trigger a search (default: 10 = 0.1%)
    /// Prevents excessive searches on tiny price movements
    pub min_price_change_bps: u32,
    /// Cooldown per token in ms (default: 100ms)
    /// Prevents searching the same token too frequently
    pub token_cooldown_ms: u64,
}

impl Default for MultiHopConfig {
    fn default() -> Self {
        Self {
            enabled: true, // Enabled for shadow mode testing
            max_hops: 4,
            beam_width: 50,
            min_profit_bps: 30,
            max_cycles: 3,
            pool_alternatives: 3,
            min_liquidity_usd: 1000.0,
            input_amount_lamports: 100_000_000, // 0.1 SOL
            shadow_mode: true,                  // Shadow mode by default - logs but doesn't trade
            min_price_change_bps: 10,
            token_cooldown_ms: 100,
        }
    }
}

/// Cache key: (pool, input_mint, output_mint)
type QuoteCacheKey = (Pubkey, Pubkey, Pubkey);

/// Cache value: (output_amount, timestamp)
type QuoteCacheValue = (u64, Instant);

/// Quote provider: trade-normalized cache, then LivePoolCache (Geyser). No synthetic fallback.
pub struct CachedQuoteProvider {
    /// Cache: (pool, input_mint, output_mint) -> (output_amount, timestamp)
    cache: RwLock<HashMap<QuoteCacheKey, QuoteCacheValue>>,
    /// pool -> (mint_a, mint_b) for O(1) freshness checks (no full-cache scan).
    pool_mints: RwLock<HashMap<Pubkey, (Pubkey, Pubkey)>>,
    /// Cache TTL
    cache_ttl: Duration,
    /// SLAVE LivePoolCache (same SSOT as execution-engine) — no RPC on miss.
    live_pool_cache: SharedLivePoolCache,
    /// Pools with at least one quotable direction (beam expansion pruning).
    quote_ready: QuoteReadyIndex,
    last_cleanup: RwLock<Instant>,
}

impl CachedQuoteProvider {
    pub fn new(cache_ttl: Duration, live_pool_cache: SharedLivePoolCache) -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
            pool_mints: RwLock::new(HashMap::new()),
            cache_ttl,
            live_pool_cache,
            quote_ready: QuoteReadyIndex::default(),
            last_cleanup: RwLock::new(Instant::now()),
        }
    }

    pub fn quote_ready_index(&self) -> &QuoteReadyIndex {
        &self.quote_ready
    }

    /// Mark pool ready when LivePoolCache gains reserves (incremental SSOT sync).
    pub fn mark_ready_from_live_pool_cache(&self, pool_address: &Pubkey) {
        let Some(state) = self.live_pool_cache.get(pool_address) else {
            return;
        };
        let (mint_a, mint_b) = Self::mints_from_state(&state);
        if self
            .try_quote_from_live_pool_cache(pool_address, &mint_a, PROBE_LAMPORTS)
            .is_some()
            || self
                .try_quote_from_live_pool_cache(pool_address, &mint_b, PROBE_LAMPORTS)
                .is_some()
        {
            self.quote_ready.mark_ready(*pool_address);
        }
    }

    fn resolve_pool_mints(&self, pool: &Pubkey) -> Option<(Pubkey, Pubkey)> {
        if let Some(mints) = self.pool_mints.read().get(pool).copied() {
            return Some(mints);
        }
        let state = self.live_pool_cache.get(pool)?;
        Some(Self::mints_from_state(&state))
    }

    fn trade_direction_fresh(
        &self,
        cache: &HashMap<QuoteCacheKey, QuoteCacheValue>,
        pool: Pubkey,
        input_mint: Pubkey,
        output_mint: Pubkey,
        now: Instant,
    ) -> bool {
        cache
            .get(&(pool, input_mint, output_mint))
            .is_some_and(|(_, ts)| now.duration_since(*ts) < self.cache_ttl)
    }

    fn pool_still_quote_ready(&self, pool: &Pubkey) -> bool {
        let now = Instant::now();
        if let Some((mint_a, mint_b)) = self.resolve_pool_mints(pool) {
            let cache = self.cache.read();
            if self.trade_direction_fresh(&cache, *pool, mint_a, mint_b, now)
                || self.trade_direction_fresh(&cache, *pool, mint_b, mint_a, now)
            {
                return true;
            }
        }

        let Some(state) = self.live_pool_cache.get(pool) else {
            return false;
        };
        let (mint_a, mint_b) = Self::mints_from_state(&state);
        self.try_quote_from_live_pool_cache(pool, &mint_a, PROBE_LAMPORTS)
            .is_some()
            || self
                .try_quote_from_live_pool_cache(pool, &mint_b, PROBE_LAMPORTS)
                .is_some()
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
                Pubkey::from_str(WSOL_MINT).unwrap_or_default(),
            ),
            CachedPoolState::PumpAmm(s) => (s.base_mint, s.quote_mint),
        }
    }

    fn liquidity_usd_estimate(state: &CachedPoolState) -> f64 {
        let (base, quote) = match state {
            CachedPoolState::Orca(s) => (
                s.vault_a_balance.unwrap_or(0),
                s.vault_b_balance.unwrap_or(0),
            ),
            CachedPoolState::RaydiumAmm(s) => {
                (s.coin_reserve.unwrap_or(0), s.pc_reserve.unwrap_or(0))
            }
            CachedPoolState::RaydiumCpmm(s) => (s.reserve_0.unwrap_or(0), s.reserve_1.unwrap_or(0)),
            CachedPoolState::Meteora(s) => (
                s.reserve_x_balance.unwrap_or(0),
                s.reserve_y_balance.unwrap_or(0),
            ),
            CachedPoolState::PumpAmm(s) => {
                (s.base_reserve.unwrap_or(0), s.quote_reserve.unwrap_or(0))
            }
            CachedPoolState::PumpFun(s) => (s.virtual_token_reserves, s.virtual_sol_reserves),
            CachedPoolState::MeteoraCpmm(s) => (s.reserve_0, s.reserve_1),
        };
        let sol_side = base.max(quote) as f64 / 1e9 * 150.0;
        sol_side.max(10_000.0)
    }

    fn try_quote_from_live_pool_cache(
        &self,
        pool_address: &Pubkey,
        input_mint: &Pubkey,
        amount_in: u64,
    ) -> Option<u64> {
        let state = self.live_pool_cache.get(pool_address)?;
        let out = quote_calculator::quote_output_amount(&state, amount_in, input_mint).ok()?;
        (out > 0).then_some(out)
    }

    fn quote_from_live_pool_cache(
        &self,
        pool_address: &Pubkey,
        input_mint: &Pubkey,
        amount_in: u64,
    ) -> Option<u64> {
        let out = self.try_quote_from_live_pool_cache(pool_address, input_mint, amount_in)?;
        multi_hop_quote_from_cache_inc();
        Some(out)
    }

    fn trade_cache_quote(
        &self,
        pool_address: &Pubkey,
        input_mint: &Pubkey,
        output_mint: &Pubkey,
        amount_in: u64,
    ) -> Option<u64> {
        let cache = self.cache.read();
        let (cached_output, ts) = cache.get(&(*pool_address, *input_mint, *output_mint))?;
        if ts.elapsed() >= self.cache_ttl {
            return None;
        }
        multi_hop_quote_from_trade_cache_inc();
        Some((*cached_output as u128 * amount_in as u128 / PROBE_LAMPORTS as u128) as u64)
    }

    /// Seed trade-cache probe quotes from LivePoolCache for top WSOL pools (cold-start).
    pub fn warmup_from_live_pool_cache(&self, top_n: usize) {
        let Ok(wsol) = Pubkey::from_str(WSOL_MINT) else {
            return;
        };

        let mut candidates: Vec<(Pubkey, CachedPoolState, f64)> = self
            .live_pool_cache
            .iter()
            .filter_map(|(pool_pk, state)| {
                let (mint_a, mint_b) = Self::mints_from_state(&state);
                if mint_a != wsol && mint_b != wsol {
                    return None;
                }
                let liq = Self::liquidity_usd_estimate(&state);
                Some((pool_pk, state, liq))
            })
            .collect();

        candidates.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));
        candidates.truncate(top_n);
        let pool_count = candidates.len();

        let mut seeded = 0usize;
        for (pool_pk, state, _) in candidates {
            let (mint_a, mint_b) = Self::mints_from_state(&state);
            let other = if mint_a == wsol { mint_b } else { mint_a };

            if let Some(out) = self.try_quote_from_live_pool_cache(&pool_pk, &wsol, PROBE_LAMPORTS)
            {
                self.update_quote(pool_pk, wsol, other, PROBE_LAMPORTS, out);
                self.quote_ready.mark_ready(pool_pk);
                seeded += 1;
            }
            if let Some(out) = self.try_quote_from_live_pool_cache(&pool_pk, &other, PROBE_LAMPORTS)
            {
                self.update_quote(pool_pk, other, wsol, PROBE_LAMPORTS, out);
                self.quote_ready.mark_ready(pool_pk);
                seeded += 1;
            }
        }

        if seeded > 0 {
            info!(
                pools = pool_count,
                quotes_seeded = seeded,
                "Multi-hop quote warmup from LivePoolCache"
            );
        }
    }

    /// Update quote cache from a trade event
    pub fn update_quote(
        &self,
        pool: Pubkey,
        input_mint: Pubkey,
        output_mint: Pubkey,
        input_amount: u64,
        output_amount: u64,
    ) {
        // Normalize to probe amount for consistent edge ratio
        let normalized_output = if input_amount > 0 {
            (output_amount as u128 * PROBE_LAMPORTS as u128 / input_amount as u128) as u64
        } else {
            0
        };

        self.pool_mints
            .write()
            .insert(pool, (input_mint, output_mint));

        let mut cache = self.cache.write();
        cache.insert(
            (pool, input_mint, output_mint),
            (normalized_output, Instant::now()),
        );
        drop(cache);
        self.quote_ready.mark_ready(pool);
    }

    /// Get cached edge ratio for a pool direction
    /// Returns: Option<(normalized_output, age_ms)>
    #[allow(dead_code)]
    pub fn get_cached_ratio(
        &self,
        pool: &Pubkey,
        input: &Pubkey,
        output: &Pubkey,
    ) -> Option<(u64, u64)> {
        let cache = self.cache.read();
        cache
            .get(&(*pool, *input, *output))
            .map(|(out, ts)| (*out, ts.elapsed().as_millis() as u64))
    }

    /// TTL sweep + bounded eviction (search worker cold path).
    pub fn cleanup(&self) {
        let mut cache = self.cache.write();
        let now = Instant::now();
        cache.retain(|_, (_, ts)| now.duration_since(*ts) < self.cache_ttl);

        if cache.len() > MAX_QUOTE_CACHE_ENTRIES {
            let mut entries: Vec<(QuoteCacheKey, Instant)> =
                cache.iter().map(|(k, (_, ts))| (*k, *ts)).collect();
            entries.sort_by_key(|(_, ts)| *ts);
            let to_remove = cache.len().saturating_sub(MAX_QUOTE_CACHE_ENTRIES);
            for (key, _) in entries.into_iter().take(to_remove) {
                cache.remove(&key);
            }
        }
    }

    /// Periodic TTL sweep from the search worker (not hot path).
    pub fn maybe_cleanup(&self) {
        let should_run = {
            let mut last = self.last_cleanup.write();
            if last.elapsed() < QUOTE_CACHE_CLEANUP_INTERVAL {
                false
            } else {
                *last = Instant::now();
                true
            }
        };
        if should_run {
            self.cleanup();
        }
    }

    /// Read-only quote-ready probe for metrics/diagnostics (does not evict stale index entries).
    pub fn probe_pool_quote_ready(&self, pool_address: &Pubkey) -> bool {
        self.quote_ready.contains(pool_address) && self.pool_still_quote_ready(pool_address)
    }

    /// Count pools that remain quote-ready after freshness probe; evicts stale index entries.
    pub fn count_fresh_quote_ready_pools(&self) -> u64 {
        let keys = self.quote_ready.snapshot_keys();
        keys.iter()
            .filter(|pool| self.is_pool_quote_ready(pool))
            .count() as u64
    }
}

impl QuoteProvider for CachedQuoteProvider {
    fn get_quote(
        &self,
        pool_address: &Pubkey,
        _dex: DexType,
        input_mint: &Pubkey,
        output_mint: &Pubkey,
        amount_in: u64,
    ) -> Option<u64> {
        if let Some(out) = self.trade_cache_quote(pool_address, input_mint, output_mint, amount_in)
        {
            return Some(out);
        }

        if let Some(out) = self.quote_from_live_pool_cache(pool_address, input_mint, amount_in) {
            return Some(out);
        }

        multi_hop_hop_missing_quote_inc();
        None
    }

    fn is_pool_quote_ready(&self, pool_address: &Pubkey) -> bool {
        if !self.quote_ready.contains(pool_address) {
            return false;
        }
        if self.pool_still_quote_ready(pool_address) {
            true
        } else {
            self.quote_ready.remove(pool_address);
            false
        }
    }

    fn has_cached_quote(
        &self,
        pool_address: &Pubkey,
        input_mint: &Pubkey,
        output_mint: &Pubkey,
    ) -> bool {
        let cache = self.cache.read();
        if cache
            .get(&(*pool_address, *input_mint, *output_mint))
            .is_some_and(|(_, ts)| ts.elapsed() < self.cache_ttl)
        {
            return true;
        }
        self.try_quote_from_live_pool_cache(pool_address, input_mint, PROBE_LAMPORTS)
            .is_some()
    }

    fn get_cached_probe_quote(
        &self,
        pool_address: &Pubkey,
        _dex: DexType,
        input_mint: &Pubkey,
        output_mint: &Pubkey,
        amount_in: u64,
    ) -> Option<u64> {
        if let Some(out) = self.trade_cache_quote(pool_address, input_mint, output_mint, amount_in)
        {
            return Some(out);
        }

        self.quote_from_live_pool_cache(pool_address, input_mint, amount_in)
    }
}

/// Token search state for cooldown tracking
struct TokenSearchState {
    last_search: Instant,
    last_price_ratio: Option<u64>, // Normalized price for change detection
}

/// Coalesced dirty-token entry (price + originating event metadata).
struct DirtyTokenEntry {
    price_ratio: u64,
    event_slot: Option<u64>,
    event_seen_at_ms: u64,
}

/// Multi-hop arbitrage engine (Event-Driven)
pub struct MultiHopArbitrage {
    config: RwLock<MultiHopConfig>,
    graph: PoolGraph,
    quote_provider: Arc<CachedQuoteProvider>,
    /// Per-token cooldown tracking
    token_state: RwLock<HashMap<Pubkey, TokenSearchState>>,
    /// Pool -> (mint_a, mint_b) reverse index for quick lookup
    pool_mints: RwLock<HashMap<Pubkey, (Pubkey, Pubkey)>>,
    /// Coalescing queue: latest price ratio per dirty token (search worker drains).
    dirty_tokens: RwLock<HashMap<Pubkey, DirtyTokenEntry>>,
    search_notify: Arc<Notify>,
    /// Stats
    searches_triggered: AtomicU64,
    searches_skipped_cooldown: AtomicU64,
    searches_skipped_small_change: AtomicU64,
    cycles_found: AtomicU64,
    cycles_profitable: AtomicU64,
    intents_generated: AtomicU64,
}

impl MultiHopArbitrage {
    pub fn new(config: MultiHopConfig, live_pool_cache: SharedLivePoolCache) -> Self {
        Self {
            config: RwLock::new(config),
            graph: PoolGraph::new(),
            quote_provider: Arc::new(CachedQuoteProvider::new(
                Duration::from_secs(30),
                live_pool_cache,
            )),
            token_state: RwLock::new(HashMap::new()),
            pool_mints: RwLock::new(HashMap::new()),
            dirty_tokens: RwLock::new(HashMap::new()),
            search_notify: Arc::new(Notify::new()),
            searches_triggered: AtomicU64::new(0),
            searches_skipped_cooldown: AtomicU64::new(0),
            searches_skipped_small_change: AtomicU64::new(0),
            cycles_found: AtomicU64::new(0),
            cycles_profitable: AtomicU64::new(0),
            intents_generated: AtomicU64::new(0),
        }
    }

    /// Add/update a pool in the graph
    /// Does NOT trigger a search - use `on_pool_price_update` for that
    pub fn upsert_pool(
        &self,
        pool_address: &str,
        dex: &str,
        mint_a: &str,
        mint_b: &str,
        liquidity_usd: f64,
        fee_bps: u16,
    ) {
        let config = self.config.read();

        if liquidity_usd < config.min_liquidity_usd {
            return;
        }

        let pool = match Pubkey::from_str(pool_address) {
            Ok(p) => p,
            Err(_) => return,
        };
        let mint_a_pk = match Pubkey::from_str(mint_a) {
            Ok(m) => m,
            Err(_) => return,
        };
        let mint_b_pk = match Pubkey::from_str(mint_b) {
            Ok(m) => m,
            Err(_) => return,
        };
        let dex_type = match dex.parse::<DexType>() {
            Ok(d) => d,
            Err(_) => return,
        };

        let edge = PoolEdge::new(pool, dex_type, mint_a_pk, mint_b_pk, liquidity_usd, fee_bps);
        self.graph.upsert_pool(edge);

        // Track pool -> mints mapping
        self.pool_mints.write().insert(pool, (mint_a_pk, mint_b_pk));
    }

    /// Remove a pool from the graph
    pub fn remove_pool(&self, pool_address: &str) {
        if let Ok(pool) = Pubkey::from_str(pool_address) {
            self.graph.remove_pool(&pool);
            self.pool_mints.write().remove(&pool);
        }
    }

    /// Hot-path enqueue: update quote cache and coalesce dirty tokens (O(1), no search).
    #[allow(clippy::too_many_arguments)]
    pub fn enqueue_pool_price_update(
        &self,
        pool_address: &str,
        input_mint: &str,
        output_mint: &str,
        input_amount: u64,
        output_amount: u64,
        event_slot: Option<u64>,
        event_seen_at_ms: u64,
    ) {
        if !self.config.read().enabled {
            return;
        }

        let pool = match Pubkey::from_str(pool_address) {
            Ok(p) => p,
            Err(_) => return,
        };
        let input = match Pubkey::from_str(input_mint) {
            Ok(m) => m,
            Err(_) => return,
        };
        let output = match Pubkey::from_str(output_mint) {
            Ok(m) => m,
            Err(_) => return,
        };

        self.quote_provider
            .update_quote(pool, input, output, input_amount, output_amount);

        if input_amount == 0 || output_amount == 0 {
            return;
        }

        let normalized_price = (output_amount as u128 * 10_000 / input_amount as u128) as u64;
        let (mint_a, mint_b) = match self.pool_mints.read().get(&pool) {
            Some(&mints) => mints,
            None => (input, output),
        };

        let output_ratio = (input_amount as u128 * 10_000 / output_amount as u128) as u64;
        for &mint in &[mint_a, mint_b] {
            let ratio = if mint == input {
                normalized_price
            } else if mint == output {
                output_ratio
            } else {
                continue;
            };
            self.enqueue_dirty_token(mint, ratio, event_slot, event_seen_at_ms);
        }

        self.search_notify.notify_one();
    }

    fn enqueue_dirty_token(
        &self,
        token: Pubkey,
        price_ratio: u64,
        event_slot: Option<u64>,
        event_seen_at_ms: u64,
    ) {
        let mut dirty = self.dirty_tokens.write();
        if dirty.len() >= MAX_DIRTY_TOKENS && !dirty.contains_key(&token) {
            return;
        }
        let entry = DirtyTokenEntry {
            price_ratio,
            event_slot,
            event_seen_at_ms,
        };
        if dirty.insert(token, entry).is_some() {
            multi_hop_searches_coalesced_inc();
        }
        multi_hop_search_worker_queue_depth_set(dirty.len() as u64);
    }

    fn drain_dirty_tokens(&self) -> Vec<(Pubkey, DirtyTokenEntry)> {
        let mut dirty = self.dirty_tokens.write();
        if dirty.is_empty() {
            return vec![];
        }
        let batch: Vec<_> = dirty.drain().collect();
        multi_hop_search_worker_queue_depth_set(0);
        batch
    }

    fn batch_event_meta_for_tokens(
        batch: &[(Pubkey, DirtyTokenEntry)],
        tokens: &[Pubkey],
    ) -> (Option<u64>, u64) {
        let token_set: HashSet<_> = tokens.iter().collect();
        batch
            .iter()
            .filter(|(token, _)| token_set.contains(token))
            .min_by_key(|(_, entry)| entry.event_seen_at_ms)
            .map(|(_, entry)| (entry.event_slot, entry.event_seen_at_ms))
            .unwrap_or((None, 0))
    }

    /// Worker batch: apply cooldown/price filters then run beam search.
    fn process_dirty_batch(
        &self,
        component: &str,
        build: &str,
        run_id: &str,
    ) -> MultiHopIntentBatch {
        self.quote_provider.maybe_cleanup();

        let config = self.config.read().clone();
        let batch = self.drain_dirty_tokens();
        if batch.is_empty() {
            return MultiHopIntentBatch {
                intents: vec![],
                slot: None,
                seen_at_ms: 0,
            };
        }

        if !config.enabled {
            return MultiHopIntentBatch {
                intents: vec![],
                slot: None,
                seen_at_ms: 0,
            };
        }

        let token_ratios: Vec<(Pubkey, u64)> = batch
            .iter()
            .map(|(token, entry)| (*token, entry.price_ratio))
            .collect();
        let tokens_to_search = self.filter_tokens_for_search(&token_ratios, &config);
        let (slot, seen_at_ms) = Self::batch_event_meta_for_tokens(&batch, &tokens_to_search);
        if tokens_to_search.is_empty() {
            return MultiHopIntentBatch {
                intents: vec![],
                slot,
                seen_at_ms,
            };
        }

        self.searches_triggered.fetch_add(1, Ordering::Relaxed);
        let intents = self.search_from_tokens(&tokens_to_search, &config, component, build, run_id);
        MultiHopIntentBatch {
            intents,
            slot,
            seen_at_ms,
        }
    }

    /// Dedicated search worker (decoupled from NATS event loop).
    pub fn spawn_search_worker(
        self: Arc<Self>,
        intent_tx: mpsc::Sender<MultiHopIntentBatch>,
        component: String,
        build: String,
        run_id: String,
    ) -> JoinHandle<()> {
        let notify = self.search_notify.clone();
        tokio::spawn(async move {
            loop {
                notify.notified().await;
                let batch = self.process_dirty_batch(&component, &build, &run_id);
                if !batch.intents.is_empty() && intent_tx.send(batch).await.is_err() {
                    warn!("Multi-hop search worker: intent channel closed, continuing search");
                }
            }
        })
    }

    /// Incrementally mark pool quote-ready when LivePoolCache reserves update.
    pub fn touch_live_pool_quote_ready(&self, pool_address: &str) {
        if let Ok(pool) = Pubkey::from_str(pool_address) {
            self.quote_provider.mark_ready_from_live_pool_cache(&pool);
        }
    }

    /// Legacy synchronous entry (tests only).
    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub fn on_pool_price_update(
        &self,
        pool_address: &str,
        input_mint: &str,
        output_mint: &str,
        input_amount: u64,
        output_amount: u64,
        component: &str,
        build: &str,
        run_id: &str,
    ) -> Vec<TradeIntent> {
        self.enqueue_pool_price_update(
            pool_address,
            input_mint,
            output_mint,
            input_amount,
            output_amount,
            None,
            0,
        );
        self.process_dirty_batch(component, build, run_id).intents
    }

    #[cfg(test)]
    pub fn dirty_token_count(&self) -> usize {
        self.dirty_tokens.read().len()
    }

    #[cfg(test)]
    pub(crate) fn search_from_tokens_for_test(
        &self,
        tokens: &[Pubkey],
        config: &MultiHopConfig,
        component: &str,
        build: &str,
        run_id: &str,
    ) -> Vec<TradeIntent> {
        self.search_from_tokens(tokens, config, component, build, run_id)
    }

    /// Filter tokens that should be searched (cooldown + per-mint price change check)
    fn filter_tokens_for_search(
        &self,
        token_ratios: &[(Pubkey, u64)],
        config: &MultiHopConfig,
    ) -> Vec<Pubkey> {
        let now = Instant::now();
        let cooldown = Duration::from_millis(config.token_cooldown_ms);
        let mut state = self.token_state.write();
        let mut result = Vec::new();

        for &(token, new_price_ratio) in token_ratios {
            let entry = state.entry(token).or_insert(TokenSearchState {
                last_search: Instant::now() - cooldown, // Allow immediate first search
                last_price_ratio: None,
            });

            // Check cooldown
            if now.duration_since(entry.last_search) < cooldown {
                self.searches_skipped_cooldown
                    .fetch_add(1, Ordering::Relaxed);
                trace!(token = %token, "Skipping search: cooldown");
                continue;
            }

            // Check price change threshold (directional ratio for this mint)
            if let Some(last_ratio) = entry.last_price_ratio {
                let change_bps = if last_ratio > 0 {
                    ((new_price_ratio as i64 - last_ratio as i64).abs() * 10_000
                        / last_ratio as i64) as u32
                } else {
                    u32::MAX
                };

                if change_bps < config.min_price_change_bps {
                    self.searches_skipped_small_change
                        .fetch_add(1, Ordering::Relaxed);
                    trace!(token = %token, change_bps, "Skipping search: small price change");
                    continue;
                }
            }

            // Update state and add to search list
            entry.last_search = now;
            entry.last_price_ratio = Some(new_price_ratio);
            result.push(token);
        }

        result
    }

    /// Search for cycles starting from specific tokens
    fn search_from_tokens(
        &self,
        tokens: &[Pubkey],
        config: &MultiHopConfig,
        component: &str,
        build: &str,
        run_id: &str,
    ) -> Vec<TradeIntent> {
        // We only search cycles that start/end at WSOL (base currency)
        let wsol = match Pubkey::from_str(WSOL_MINT) {
            Ok(w) => w,
            Err(_) => return vec![],
        };

        // Only search if one of the affected tokens connects to WSOL
        // or if WSOL itself is affected
        let wsol_connected = tokens.contains(&wsol)
            || tokens
                .iter()
                .any(|t| !self.graph.pools_between(t, &wsol).is_empty());

        if !wsol_connected {
            trace!("Skipping search: affected tokens not connected to WSOL");
            return vec![];
        }

        // Build finder for focused search
        let finder_config = CycleFinderConfig {
            beam_width: config.beam_width,
            max_hops: config.max_hops,
            min_profit_bps: config.min_profit_bps,
            max_results: config.max_cycles,
            base_mint: wsol,
            pool_alternatives: config.pool_alternatives,
        };

        let ranker_config = RankerConfig {
            probe_amount: config.input_amount_lamports,
            ..Default::default()
        };

        let ranker = PoolRanker::with_config(ranker_config, self.quote_provider.clone());
        let finder = BeamCycleFinder::new(finder_config, ranker);

        // Targeted search: only expand subgraph around affected tokens
        let cycles = finder.find_cycles_through(&self.graph, tokens);
        self.cycles_found
            .fetch_add(cycles.len() as u64, Ordering::Relaxed);

        if cycles.is_empty() {
            let mut search_lacked_quote_neighbors = false;
            for token in tokens {
                let neighbors = self.graph.neighbors(token);
                let quote_ready_neighbors = neighbors
                    .iter()
                    .flat_map(|(_, edges)| edges)
                    .filter(|edge| {
                        self.quote_provider
                            .probe_pool_quote_ready(&edge.pool_address)
                    })
                    .count();
                if quote_ready_neighbors < 2 {
                    search_lacked_quote_neighbors = true;
                    break;
                }
            }
            if search_lacked_quote_neighbors {
                multi_hop_search_no_quote_neighbors_inc();
            }
        }

        // Filter: trustworthy profit estimate + min threshold
        let profitable: Vec<_> = cycles
            .into_iter()
            .filter(|c| {
                if !c.is_trustworthy_profit_estimate() {
                    if c.return_bps_saturated {
                        multi_hop_return_bps_saturated_inc();
                        multi_hop_cycle_rejected_sanity_inc(
                            MultiHopSanityRejectReason::ReturnBpsCap,
                        );
                    }
                    if c.profit_multiplier_capped {
                        multi_hop_cycle_rejected_sanity_inc(MultiHopSanityRejectReason::ProfitCap);
                    }
                    if c.edge_ratio_clamped {
                        multi_hop_cycle_rejected_sanity_inc(MultiHopSanityRejectReason::EdgeRatio);
                    }
                    return false;
                }
                c.estimated_return_bps >= config.min_profit_bps
            })
            .collect();

        self.cycles_profitable
            .fetch_add(profitable.len() as u64, Ordering::Relaxed);

        if profitable.is_empty() {
            return vec![];
        }

        debug!(
            found = profitable.len(),
            best_bps = profitable
                .first()
                .map(|c| c.estimated_return_bps)
                .unwrap_or(0),
            affected_tokens = tokens.len(),
            "Multi-hop cycles found"
        );

        // Shadow mode: log but don't generate intents
        if config.shadow_mode {
            for (i, cycle) in profitable.iter().enumerate() {
                multi_hop_shadow_logged_inc();
                let near_cap = cycle.estimated_return_bps > MAX_RETURN_BPS.saturating_sub(500);
                info!(
                    rank = i + 1,
                    return_bps = cycle.estimated_return_bps,
                    return_bps_capped = false,
                    sanity_flags = if near_cap { "near_return_cap" } else { "" },
                    hops = cycle.hop_count(),
                    min_liquidity = cycle.min_liquidity_usd,
                    path = ?cycle.path.iter().map(|p| p.to_string()).collect::<Vec<_>>(),
                    "[SHADOW] Multi-hop opportunity"
                );
            }
            return vec![];
        }

        // Generate intents
        let intents: Vec<_> = profitable
            .iter()
            .filter_map(|cycle| self.cycle_to_intent(cycle, config, component, build, run_id))
            .collect();

        self.intents_generated
            .fetch_add(intents.len() as u64, Ordering::Relaxed);
        intents
    }

    /// Convert an ArbCycle to a TradeIntent with swap_path
    fn cycle_to_intent(
        &self,
        cycle: &ArbCycle,
        config: &MultiHopConfig,
        component: &str,
        build: &str,
        run_id: &str,
    ) -> Option<TradeIntent> {
        if !cycle.is_valid() {
            warn!("Invalid cycle: path doesn't start and end at same token");
            return None;
        }

        // Build swap path
        let mut swap_path = Vec::with_capacity(cycle.hop_count());

        for (i, pool_options) in cycle.pools.iter().enumerate() {
            if pool_options.is_empty() {
                warn!(hop = i, "Cycle has empty pool options for hop");
                return None;
            }

            let primary = &pool_options[0];
            let input_mint = cycle.path[i].to_string();
            let output_mint = cycle.path[i + 1].to_string();

            let alternatives: Vec<PoolAlternative> = pool_options
                .iter()
                .skip(1)
                .map(|alt| PoolAlternative {
                    pool_address: alt.pool_address.to_string(),
                    dex: alt.dex.to_string(),
                    expected_output: 0,
                })
                .collect();

            swap_path.push(SwapHop::with_alternatives(
                primary.pool_address.to_string(),
                primary.dex.to_string(),
                input_mint,
                output_mint,
                0,
                alternatives,
            ));
        }

        let intent_id = format!("mh-{}", uuid::Uuid::new_v4());

        let intent = TradeIntent {
            header: RecordHeader::new(component, build, run_id),
            intent_id,
            source: "arb-strategy".to_string(),
            tier: IntentTier::Arb, // Arbitrage: P75 × 1.3 fee (between Tier0 and Tier1)
            origin_type: IntentOrigin::StrategyA,
            deadline_slot: None,
            ttl_ms: Some(1000),
            required_capital: ExplicitAmount::new(config.input_amount_lamports, 9),
            resources: TradeResources {
                input_mint: WSOL_MINT.to_string(),
                output_mint: WSOL_MINT.to_string(),
                pools: swap_path.iter().map(|h| h.pool_address.clone()).collect(),
                accounts: vec![],
                token_program: None,
            },
            expected_roi_bps: cycle.estimated_return_bps,
            max_slippage_bps: 100,
            side: TradeSide::Buy,
            regime: TradingRegime::NotApplicable,
            trigger_event_id: None,
            require_bundle: Some(true),
            bundle_tip_lamports: None,
            hint_compute_units: Some(400_000),
            hint_priority_fee_micro_lamports: None,
            hint_urgency: Some(2),
            metadata: {
                let mut m = std::collections::HashMap::new();
                m.insert("multi_hop".to_string(), "true".to_string());
                m.insert("hop_count".to_string(), cycle.hop_count().to_string());
                m.insert(
                    "min_liquidity_usd".to_string(),
                    format!("{:.0}", cycle.min_liquidity_usd),
                );
                m
            },
            execution: None,
            swap_path: Some(swap_path),
        };

        if let Err(e) = intent.validate_swap_path() {
            warn!(error = %e, "Invalid swap path generated");
            return None;
        }

        Some(intent)
    }

    /// Get stats for monitoring
    pub fn stats(&self) -> MultiHopStats {
        MultiHopStats {
            graph_vertices: self.graph.stats().total_vertices,
            graph_pools: self.graph.stats().total_pools,
            searches_triggered: self.searches_triggered.load(Ordering::Relaxed),
            searches_skipped_cooldown: self.searches_skipped_cooldown.load(Ordering::Relaxed),
            searches_skipped_small_change: self
                .searches_skipped_small_change
                .load(Ordering::Relaxed),
            cycles_found: self.cycles_found.load(Ordering::Relaxed),
            cycles_profitable: self.cycles_profitable.load(Ordering::Relaxed),
            intents_generated: self.intents_generated.load(Ordering::Relaxed),
        }
    }

    /// Get current configuration (clone for hot-reload updates)
    pub fn get_config(&self) -> MultiHopConfig {
        self.config.read().clone()
    }

    /// Update configuration at runtime
    pub fn update_config(&self, config: MultiHopConfig) {
        *self.config.write() = config;
    }

    /// Check if multi-hop is enabled
    pub fn is_enabled(&self) -> bool {
        self.config.read().enabled
    }

    /// Seed trade-cache probe quotes from LivePoolCache (post-bootstrap cold-start).
    pub fn warmup_quotes_from_live_pool_cache(&self) {
        self.quote_provider
            .warmup_from_live_pool_cache(QUOTE_WARMUP_TOP_N);
    }

    /// Publish quote-readiness gauges for Prometheus (no algorithm change).
    pub fn refresh_quote_readiness_metrics(&self) {
        let ready_total = self.quote_provider.count_fresh_quote_ready_pools();
        multi_hop_quote_ready_pools_set(ready_total);

        let wsol = match Pubkey::from_str(WSOL_MINT) {
            Ok(w) => w,
            Err(_) => return,
        };
        let wsol_edge = self
            .pool_mints
            .read()
            .iter()
            .filter(|(pool, (mint_a, mint_b))| {
                (*mint_a == wsol || *mint_b == wsol)
                    && self.quote_provider.probe_pool_quote_ready(pool)
            })
            .count() as u64;
        multi_hop_quote_ready_wsol_edge_pools_set(wsol_edge);
    }
}

/// Stats for monitoring
#[derive(Debug, Clone, Default)]
pub struct MultiHopStats {
    pub graph_vertices: usize,
    pub graph_pools: usize,
    pub searches_triggered: u64,
    pub searches_skipped_cooldown: u64,
    pub searches_skipped_small_change: u64,
    pub cycles_found: u64,
    pub cycles_profitable: u64,
    pub intents_generated: u64,
}

// Make CachedQuoteProvider work with Arc
impl QuoteProvider for Arc<CachedQuoteProvider> {
    fn get_quote(
        &self,
        pool_address: &Pubkey,
        dex: DexType,
        input_mint: &Pubkey,
        output_mint: &Pubkey,
        amount_in: u64,
    ) -> Option<u64> {
        (**self).get_quote(pool_address, dex, input_mint, output_mint, amount_in)
    }

    fn has_cached_quote(
        &self,
        pool_address: &Pubkey,
        input_mint: &Pubkey,
        output_mint: &Pubkey,
    ) -> bool {
        (**self).has_cached_quote(pool_address, input_mint, output_mint)
    }

    fn is_pool_quote_ready(&self, pool_address: &Pubkey) -> bool {
        (**self).is_pool_quote_ready(pool_address)
    }

    fn get_cached_probe_quote(
        &self,
        pool_address: &Pubkey,
        dex: DexType,
        input_mint: &Pubkey,
        output_mint: &Pubkey,
        amount_in: u64,
    ) -> Option<u64> {
        (**self).get_cached_probe_quote(pool_address, dex, input_mint, output_mint, amount_in)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::live_pool_cache::{
        create_shared_cache, CachedPoolState, RaydiumAmmState,
    };
    use serial_test::serial;

    #[test]
    fn test_multi_hop_config_default() {
        let config = MultiHopConfig::default();
        assert!(config.enabled);
        assert!(config.shadow_mode);
        assert_eq!(config.max_hops, 4);
        assert_eq!(config.min_price_change_bps, 10);
        assert_eq!(config.token_cooldown_ms, 100);
    }

    #[test]
    fn test_pool_upsert() {
        let arb = MultiHopArbitrage::new(
            MultiHopConfig {
                enabled: true,
                min_liquidity_usd: 0.0,
                ..Default::default()
            },
            create_shared_cache(),
        );

        arb.upsert_pool(
            "11111111111111111111111111111111",
            "raydium",
            WSOL_MINT,
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
            100_000.0,
            30,
        );

        let stats = arb.stats();
        assert_eq!(stats.graph_vertices, 2);
        assert_eq!(stats.graph_pools, 1);
    }

    #[test]
    fn test_live_pool_cache_quote_without_trade() {
        let cache = create_shared_cache();
        let provider = CachedQuoteProvider::new(Duration::from_secs(30), cache.clone());

        let wsol = Pubkey::from_str(WSOL_MINT).unwrap();
        let token = Pubkey::new_unique();
        let pool = Pubkey::new_unique();

        cache.upsert(
            pool,
            CachedPoolState::RaydiumAmm(RaydiumAmmState {
                base_mint: token,
                quote_mint: wsol,
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

        let probe = PROBE_LAMPORTS;
        let out = provider
            .get_cached_probe_quote(&pool, DexType::RaydiumAmmV4, &wsol, &token, probe)
            .expect("LivePoolCache quote expected");
        assert!(out > 0);
    }

    #[test]
    fn test_get_quote_without_cache_returns_none() {
        let provider = CachedQuoteProvider::new(Duration::from_secs(30), create_shared_cache());

        let pool = Pubkey::new_unique();
        let input = Pubkey::new_unique();
        let output = Pubkey::new_unique();

        let result = provider.get_quote(
            &pool,
            DexType::RaydiumAmmV4,
            &input,
            &output,
            PROBE_LAMPORTS,
        );
        assert!(
            result.is_none(),
            "get_quote must return None without cache data"
        );
    }

    #[test]
    fn test_cached_quote_provider() {
        let provider = CachedQuoteProvider::new(Duration::from_secs(30), create_shared_cache());

        let pool = Pubkey::new_unique();
        let input = Pubkey::new_unique();
        let output = Pubkey::new_unique();

        provider.update_quote(pool, input, output, 1_000_000, 980_000);

        let result = provider.get_quote(&pool, DexType::RaydiumAmmV4, &input, &output, 10_000_000);
        assert!(result.is_some());

        let out = result.unwrap();
        assert!(out > 9_700_000 && out < 9_900_000, "Got {out}");
    }

    #[test]
    #[serial]
    fn search_from_tokens_increments_no_quote_neighbors_once_per_search() {
        use crate::metrics::MULTI_HOP_SEARCH_NO_QUOTE_NEIGHBORS_TOTAL;
        use std::sync::atomic::Ordering;

        let before = MULTI_HOP_SEARCH_NO_QUOTE_NEIGHBORS_TOTAL.load(Ordering::Relaxed);
        let arb = MultiHopArbitrage::new(
            MultiHopConfig {
                enabled: true,
                min_liquidity_usd: 0.0,
                min_profit_bps: i32::MAX,
                ..Default::default()
            },
            create_shared_cache(),
        );

        let token_a = Pubkey::new_unique();
        let token_b = Pubkey::new_unique();
        let pool_a = Pubkey::new_unique();
        let pool_b = Pubkey::new_unique();

        arb.upsert_pool(
            &pool_a.to_string(),
            "raydium",
            WSOL_MINT,
            &token_a.to_string(),
            100_000.0,
            30,
        );
        arb.upsert_pool(
            &pool_b.to_string(),
            "raydium",
            WSOL_MINT,
            &token_b.to_string(),
            100_000.0,
            30,
        );

        let config = MultiHopConfig {
            enabled: true,
            min_liquidity_usd: 0.0,
            min_profit_bps: i32::MAX,
            ..Default::default()
        };
        let _ = arb.search_from_tokens_for_test(
            &[token_a, token_b],
            &config,
            "test",
            "0.1.0",
            "test-run",
        );

        assert_eq!(
            MULTI_HOP_SEARCH_NO_QUOTE_NEIGHBORS_TOTAL.load(Ordering::Relaxed),
            before + 1,
            "coalesced multi-token search should increment no-quote-neighbors once"
        );
    }

    #[test]
    fn count_fresh_quote_ready_pools_excludes_stale_index_entries() {
        let provider = CachedQuoteProvider::new(Duration::from_secs(30), create_shared_cache());
        let fresh_pool = Pubkey::new_unique();
        let stale_pool = Pubkey::new_unique();
        let input = Pubkey::new_unique();
        let output = Pubkey::new_unique();

        provider.update_quote(fresh_pool, input, output, 1_000_000, 980_000);
        provider.quote_ready_index().mark_ready(stale_pool);

        assert_eq!(provider.quote_ready_index().len(), 2);
        assert_eq!(provider.count_fresh_quote_ready_pools(), 1);
        assert_eq!(provider.quote_ready_index().len(), 1);
    }

    #[test]
    fn test_event_driven_disabled() {
        let arb = MultiHopArbitrage::new(
            MultiHopConfig {
                enabled: false, // Disabled
                ..Default::default()
            },
            create_shared_cache(),
        );

        let intents = arb.on_pool_price_update(
            "11111111111111111111111111111111",
            WSOL_MINT,
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
            1_000_000,
            990_000,
            "test",
            "0.1.0",
            "test-run",
        );

        assert!(intents.is_empty());
        assert_eq!(arb.stats().searches_triggered, 0);
    }

    #[test]
    fn enqueue_does_not_search_synchronously() {
        let arb = MultiHopArbitrage::new(
            MultiHopConfig {
                enabled: true,
                min_liquidity_usd: 0.0,
                min_price_change_bps: 0,
                token_cooldown_ms: 0,
                ..Default::default()
            },
            create_shared_cache(),
        );

        arb.upsert_pool(
            "11111111111111111111111111111111",
            "raydium",
            WSOL_MINT,
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
            100_000.0,
            30,
        );

        arb.enqueue_pool_price_update(
            "11111111111111111111111111111111",
            WSOL_MINT,
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
            1_000_000,
            990_000,
            None,
            0,
        );

        assert!(arb.dirty_token_count() > 0);
        assert_eq!(arb.stats().searches_triggered, 0);
    }

    #[test]
    fn pool_quote_ready_check_is_constant_per_pool() {
        let provider = CachedQuoteProvider::new(Duration::from_secs(30), create_shared_cache());
        let pool = Pubkey::new_unique();
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();

        provider.update_quote(pool, mint_a, mint_b, 1_000_000, 990_000);

        for i in 0..5_000 {
            let noise_pool = Pubkey::new_unique();
            let noise_mint = Pubkey::new_unique();
            provider.update_quote(
                noise_pool,
                noise_mint,
                Pubkey::new_unique(),
                1_000_000,
                990_000 + i,
            );
        }

        assert!(provider.is_pool_quote_ready(&pool));
    }

    #[test]
    fn quote_cache_cleanup_evicts_stale_entries() {
        let provider = CachedQuoteProvider::new(Duration::from_millis(1), create_shared_cache());
        let pool = Pubkey::new_unique();
        let input = Pubkey::new_unique();
        let output = Pubkey::new_unique();
        provider.update_quote(pool, input, output, 1_000_000, 990_000);
        std::thread::sleep(Duration::from_millis(5));
        provider.cleanup();
        assert!(!provider.is_pool_quote_ready(&pool));
    }

    #[test]
    #[serial]
    fn coalesced_dirty_tokens_produce_single_search() {
        let arb = MultiHopArbitrage::new(
            MultiHopConfig {
                enabled: true,
                min_liquidity_usd: 0.0,
                min_price_change_bps: 0,
                token_cooldown_ms: 0,
                ..Default::default()
            },
            create_shared_cache(),
        );

        arb.upsert_pool(
            "11111111111111111111111111111111111",
            "raydium",
            WSOL_MINT,
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
            100_000.0,
            30,
        );

        let usdc = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
        for i in 0..5 {
            arb.enqueue_pool_price_update(
                "11111111111111111111111111111111",
                WSOL_MINT,
                usdc,
                1_000_000 + i,
                990_000,
                None,
                0,
            );
        }

        assert_eq!(arb.dirty_token_count(), 2, "WSOL + USDC dirty entries");
        let _ = arb.process_dirty_batch("test", "0.1.0", "run");
        assert_eq!(arb.stats().searches_triggered, 1);
        assert_eq!(arb.dirty_token_count(), 0);
    }
}
