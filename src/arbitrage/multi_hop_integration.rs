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
use crate::ipc::{
    ExplicitAmount, IntentOrigin, IntentTier, PoolAlternative, RecordHeader, SwapHop, TradeIntent,
    TradeResources, TradeSide, TradingRegime,
};
use crate::metrics::{
    multi_hop_cycle_rejected_sanity_inc, multi_hop_return_bps_saturated_inc,
    multi_hop_shadow_logged_inc, MultiHopSanityRejectReason,
};
use parking_lot::RwLock;
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, info, trace, warn};

/// WSOL mint (native SOL wrapped)
pub const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";

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

/// Quote provider that uses cached pool data
pub struct CachedQuoteProvider {
    /// Cache: (pool, input_mint, output_mint) -> (output_amount, timestamp)
    cache: RwLock<HashMap<QuoteCacheKey, QuoteCacheValue>>,
    /// Cache TTL
    cache_ttl: Duration,
    /// Default edge ratio when no quote available (conservative)
    default_edge_ratio: f64,
}

impl CachedQuoteProvider {
    pub fn new(cache_ttl: Duration) -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
            cache_ttl,
            default_edge_ratio: 0.99, // Assume 1% loss when no data
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
        let probe = 10_000_000u64; // 0.01 SOL
        let normalized_output = if input_amount > 0 {
            (output_amount as u128 * probe as u128 / input_amount as u128) as u64
        } else {
            0
        };

        let mut cache = self.cache.write();
        cache.insert(
            (pool, input_mint, output_mint),
            (normalized_output, Instant::now()),
        );
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

    /// Clear stale entries
    #[allow(dead_code)]
    pub fn cleanup(&self) {
        let mut cache = self.cache.write();
        let now = Instant::now();
        cache.retain(|_, (_, ts)| now.duration_since(*ts) < self.cache_ttl);
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
        let cache = self.cache.read();

        if let Some((cached_output, ts)) = cache.get(&(*pool_address, *input_mint, *output_mint)) {
            if ts.elapsed() < self.cache_ttl {
                let probe = 10_000_000u64;
                return Some((*cached_output as u128 * amount_in as u128 / probe as u128) as u64);
            }
        }

        // No cached quote - return conservative estimate (ranker skips via has_cached_quote)
        Some((amount_in as f64 * self.default_edge_ratio) as u64)
    }

    fn has_cached_quote(
        &self,
        pool_address: &Pubkey,
        input_mint: &Pubkey,
        output_mint: &Pubkey,
    ) -> bool {
        let cache = self.cache.read();
        cache
            .get(&(*pool_address, *input_mint, *output_mint))
            .is_some_and(|(_, ts)| ts.elapsed() < self.cache_ttl)
    }
}

/// Token search state for cooldown tracking
struct TokenSearchState {
    last_search: Instant,
    last_price_ratio: Option<u64>, // Normalized price for change detection
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
    /// Stats
    searches_triggered: AtomicU64,
    searches_skipped_cooldown: AtomicU64,
    searches_skipped_small_change: AtomicU64,
    cycles_found: AtomicU64,
    cycles_profitable: AtomicU64,
    intents_generated: AtomicU64,
}

impl MultiHopArbitrage {
    pub fn new(config: MultiHopConfig) -> Self {
        Self {
            config: RwLock::new(config),
            graph: PoolGraph::new(),
            quote_provider: Arc::new(CachedQuoteProvider::new(Duration::from_secs(30))),
            token_state: RwLock::new(HashMap::new()),
            pool_mints: RwLock::new(HashMap::new()),
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

    /// EVENT-DRIVEN: Called when a pool price updates (trade observed)
    ///
    /// This is the main entry point for cycle detection.
    /// Returns intents if profitable cycles found (empty if shadow_mode).
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
        let config = self.config.read().clone();

        if !config.enabled {
            return vec![];
        }

        // Parse addresses
        let pool = match Pubkey::from_str(pool_address) {
            Ok(p) => p,
            Err(_) => return vec![],
        };
        let input = match Pubkey::from_str(input_mint) {
            Ok(m) => m,
            Err(_) => return vec![],
        };
        let output = match Pubkey::from_str(output_mint) {
            Ok(m) => m,
            Err(_) => return vec![],
        };

        // Update quote cache
        self.quote_provider
            .update_quote(pool, input, output, input_amount, output_amount);

        // Check if this is a significant price change
        let normalized_price = if input_amount > 0 {
            (output_amount as u128 * 10_000 / input_amount as u128) as u64
        } else {
            return vec![];
        };

        // Get the affected tokens (both sides of the pool)
        let (mint_a, mint_b) = match self.pool_mints.read().get(&pool) {
            Some(&mints) => mints,
            None => (input, output), // Fallback if not in index
        };

        // Check cooldown and price change for both tokens
        let tokens_to_search =
            self.filter_tokens_for_search(&[mint_a, mint_b], normalized_price, &config);

        if tokens_to_search.is_empty() {
            return vec![];
        }

        self.searches_triggered.fetch_add(1, Ordering::Relaxed);

        // Search for cycles starting from affected tokens
        self.search_from_tokens(&tokens_to_search, &config, component, build, run_id)
    }

    /// Filter tokens that should be searched (cooldown + price change check)
    fn filter_tokens_for_search(
        &self,
        tokens: &[Pubkey],
        new_price_ratio: u64,
        config: &MultiHopConfig,
    ) -> Vec<Pubkey> {
        let now = Instant::now();
        let cooldown = Duration::from_millis(config.token_cooldown_ms);
        let mut state = self.token_state.write();
        let mut result = Vec::new();

        for &token in tokens {
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

            // Check price change threshold
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

        // Find cycles (searches from WSOL through the graph)
        let cycles = finder.find_cycles(&self.graph);
        self.cycles_found
            .fetch_add(cycles.len() as u64, Ordering::Relaxed);

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
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let arb = MultiHopArbitrage::new(MultiHopConfig {
            enabled: true,
            min_liquidity_usd: 0.0,
            ..Default::default()
        });

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
    fn test_cached_quote_provider() {
        let provider = CachedQuoteProvider::new(Duration::from_secs(30));

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
    fn test_event_driven_disabled() {
        let arb = MultiHopArbitrage::new(MultiHopConfig {
            enabled: false, // Disabled
            ..Default::default()
        });

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
}
