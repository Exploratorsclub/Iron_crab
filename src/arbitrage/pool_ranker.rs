//! Pool Ranker - Pre-compute edge ratios for efficient arbitrage search
//!
//! CRITICAL: Uses probe-based quotes (NOT spot prices!) for edge_ratio calculation.
//! This accounts for:
//! - Trading fees
//! - Curve shape (CPMM, DLMM bins, etc.)
//! - Current liquidity distribution
//!
//! From External Review:
//! > "edge_ratio = quote_out(probe_amount) / probe_amount"
//! > This is MORE ACCURATE than spot price because it includes fees & curve effects.

use super::pool_graph::PoolGraph;
use super::types::{clamp_edge_ratio, DexType, PoolEdge, RankedPool};
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;

/// Default probe amount in lamports (0.01 SOL = 10_000_000 lamports)
const DEFAULT_PROBE_LAMPORTS: u64 = 10_000_000;

/// Liquidity baseline for dampened scoring (in USD)
const LIQUIDITY_BASELINE_USD: f64 = 100_000.0;

/// Minimum liquidity score (clamp lower bound)
const MIN_LIQUIDITY_SCORE: f64 = 0.3;

/// Maximum liquidity score (clamp upper bound)
const MAX_LIQUIDITY_SCORE: f64 = 1.5;

/// Pool ranking configuration
#[derive(Debug, Clone)]
pub struct RankerConfig {
    /// Probe amount for quote-based edge ratio (in base token smallest unit)
    pub probe_amount: u64,
    /// Liquidity baseline for dampened scoring
    pub liquidity_baseline_usd: f64,
    /// Min liquidity score (clamp lower bound)
    pub min_liquidity_score: f64,
    /// Max liquidity score (clamp upper bound)
    pub max_liquidity_score: f64,
}

impl Default for RankerConfig {
    fn default() -> Self {
        Self {
            probe_amount: DEFAULT_PROBE_LAMPORTS,
            liquidity_baseline_usd: LIQUIDITY_BASELINE_USD,
            min_liquidity_score: MIN_LIQUIDITY_SCORE,
            max_liquidity_score: MAX_LIQUIDITY_SCORE,
        }
    }
}

/// Trait for quote providers (DEX connectors implement this)
#[allow(async_fn_in_trait)]
pub trait QuoteProvider: Send + Sync {
    /// Get quote output amount for a swap
    ///
    /// Returns: output amount in smallest units, or None if quote unavailable
    fn get_quote(
        &self,
        pool_address: &Pubkey,
        dex: DexType,
        input_mint: &Pubkey,
        output_mint: &Pubkey,
        amount_in: u64,
    ) -> Option<u64>;

    /// Whether a fresh cached probe quote exists for this pool direction.
    /// Default `true` (mocks / providers without cache distinction).
    fn has_cached_quote(
        &self,
        _pool_address: &Pubkey,
        _input_mint: &Pubkey,
        _output_mint: &Pubkey,
    ) -> bool {
        true
    }

    /// Fresh cached probe quote, or `None` if missing/stale.
    /// Default uses separate cache checks (fine for mocks); TTL caches should override.
    fn get_cached_probe_quote(
        &self,
        pool_address: &Pubkey,
        dex: DexType,
        input_mint: &Pubkey,
        output_mint: &Pubkey,
        amount_in: u64,
    ) -> Option<u64> {
        if !self.has_cached_quote(pool_address, input_mint, output_mint) {
            return None;
        }
        self.get_quote(pool_address, dex, input_mint, output_mint, amount_in)
    }
}

/// Pool ranker that pre-computes edge ratios using probe quotes
pub struct PoolRanker<Q: QuoteProvider> {
    config: RankerConfig,
    quote_provider: Q,
    /// Cached max edge ratio per token (for upper bound pruning)
    max_edge_ratio: parking_lot::RwLock<HashMap<Pubkey, f64>>,
}

impl<Q: QuoteProvider> PoolRanker<Q> {
    pub fn new(quote_provider: Q) -> Self {
        Self {
            config: RankerConfig::default(),
            quote_provider,
            max_edge_ratio: parking_lot::RwLock::new(HashMap::new()),
        }
    }

    pub fn with_config(config: RankerConfig, quote_provider: Q) -> Self {
        Self {
            config,
            quote_provider,
            max_edge_ratio: parking_lot::RwLock::new(HashMap::new()),
        }
    }

    /// Rank all pools for a given input token, computing edge ratios via probe quotes
    ///
    /// Returns: Vec<(output_mint, Vec<RankedPool>)> sorted by combined score (descending)
    pub fn rank_pools_from(
        &self,
        graph: &PoolGraph,
        input_mint: &Pubkey,
    ) -> Vec<(Pubkey, Vec<RankedPool>)> {
        let neighbors = graph.neighbors(input_mint);
        let mut results = Vec::with_capacity(neighbors.len());
        let mut max_edge_updates = Vec::new();

        for (output_mint, pools) in neighbors {
            let mut ranked_pools = Vec::with_capacity(pools.len());

            for edge in pools {
                if let Some(ranked) = self.rank_single_pool(&edge, input_mint, &output_mint) {
                    // Track max edge ratio for upper bound pruning
                    max_edge_updates.push((*input_mint, clamp_edge_ratio(ranked.edge_ratio)));
                    ranked_pools.push(ranked);
                }
            }

            // Sort by combined score (descending)
            ranked_pools.sort_by(|a, b| {
                b.combined_score()
                    .partial_cmp(&a.combined_score())
                    .unwrap_or(std::cmp::Ordering::Equal)
            });

            if !ranked_pools.is_empty() {
                results.push((output_mint, ranked_pools));
            }
        }

        // Update max edge ratios
        self.update_max_edge_ratios(max_edge_updates);

        // Sort results by best pool's score (descending)
        results.sort_by(|a, b| {
            let score_a = a.1.first().map(|p| p.combined_score()).unwrap_or(0.0);
            let score_b = b.1.first().map(|p| p.combined_score()).unwrap_or(0.0);
            score_b
                .partial_cmp(&score_a)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        results
    }

    /// Rank a single pool, computing edge ratio via probe quote
    fn rank_single_pool(
        &self,
        edge: &PoolEdge,
        input_mint: &Pubkey,
        output_mint: &Pubkey,
    ) -> Option<RankedPool> {
        let Some(quote_out) = self.quote_provider.get_cached_probe_quote(
            &edge.pool_address,
            edge.dex,
            input_mint,
            output_mint,
            self.config.probe_amount,
        ) else {
            crate::metrics::multi_hop_hop_missing_quote_inc();
            return None;
        };

        // Edge ratio = output / input (accounts for fees & curve); clamp in beam expand
        let edge_ratio = quote_out as f64 / self.config.probe_amount as f64;

        // Dampened liquidity score (clamped, NOT sqrt!)
        // From external review: clamp(liquidity/baseline, 0.3, 1.5)
        let liquidity_score = (edge.liquidity_usd / self.config.liquidity_baseline_usd).clamp(
            self.config.min_liquidity_score,
            self.config.max_liquidity_score,
        );

        Some(RankedPool {
            edge: edge.clone(),
            edge_ratio,
            liquidity_score,
        })
    }

    /// Get maximum edge ratio for a token (for upper bound pruning)
    ///
    /// Returns 1.5 (optimistic) if no data yet, to avoid premature pruning
    pub fn max_edge_ratio_for(&self, token: &Pubkey) -> f64 {
        self.max_edge_ratio
            .read()
            .get(token)
            .copied()
            // Use optimistic default (1.5) to avoid pruning paths before we have data
            .unwrap_or(1.5)
    }

    /// Compute upper bound for remaining path
    ///
    /// Formula: current_profit × max_edge_ratio[current_token]^remaining_hops
    pub fn compute_upper_bound(
        &self,
        current_profit: f64,
        current_token: &Pubkey,
        remaining_hops: usize,
    ) -> f64 {
        let max_ratio = self.max_edge_ratio_for(current_token);
        current_profit * max_ratio.powi(remaining_hops as i32)
    }

    /// Clear cached edge ratios (call when pools update significantly)
    pub fn clear_cache(&self) {
        self.max_edge_ratio.write().clear();
    }

    /// Whether a fresh cached probe quote exists for this hop direction.
    pub fn hop_has_cached_quote(
        &self,
        pool_address: &Pubkey,
        input_mint: &Pubkey,
        output_mint: &Pubkey,
    ) -> bool {
        self.quote_provider
            .has_cached_quote(pool_address, input_mint, output_mint)
    }

    fn update_max_edge_ratios(&self, updates: Vec<(Pubkey, f64)>) {
        if updates.is_empty() {
            return;
        }

        let mut max_edge = self.max_edge_ratio.write();
        for (token, ratio) in updates {
            let entry = max_edge.entry(token).or_insert(0.0);
            if ratio > *entry {
                *entry = ratio;
            }
        }
    }
}

/// Mock quote provider for testing
#[cfg(any(test, feature = "test_helpers"))]
pub struct MockQuoteProvider {
    quotes: HashMap<(Pubkey, Pubkey, Pubkey), u64>, // (pool, input, output) -> quote
    /// Probe-quote lookups (for subgraph search tests).
    probe_lookups: std::sync::atomic::AtomicU64,
}

#[cfg(any(test, feature = "test_helpers"))]
impl MockQuoteProvider {
    pub fn new() -> Self {
        Self {
            quotes: HashMap::new(),
            probe_lookups: std::sync::atomic::AtomicU64::new(0),
        }
    }

    pub fn probe_lookup_count(&self) -> u64 {
        self.probe_lookups
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn reset_probe_lookup_count(&self) {
        self.probe_lookups
            .store(0, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn add_quote(
        &mut self,
        pool: Pubkey,
        input: Pubkey,
        output: Pubkey,
        input_amount: u64,
        output_amount: u64,
    ) {
        // Store with input amount as part of key for more realistic testing
        let _ = input_amount; // We ignore input amount for simplicity in mock
        self.quotes.insert((pool, input, output), output_amount);
    }

    pub fn remove_quote(&mut self, pool: Pubkey, input: Pubkey, output: Pubkey) {
        self.quotes.remove(&(pool, input, output));
    }
}

#[cfg(any(test, feature = "test_helpers"))]
impl Default for MockQuoteProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(any(test, feature = "test_helpers"))]
impl QuoteProvider for std::sync::Arc<MockQuoteProvider> {
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

#[cfg(any(test, feature = "test_helpers"))]
impl QuoteProvider for MockQuoteProvider {
    fn get_quote(
        &self,
        pool_address: &Pubkey,
        _dex: DexType,
        input_mint: &Pubkey,
        output_mint: &Pubkey,
        _amount_in: u64,
    ) -> Option<u64> {
        self.quotes
            .get(&(*pool_address, *input_mint, *output_mint))
            .copied()
    }

    fn has_cached_quote(
        &self,
        pool_address: &Pubkey,
        input_mint: &Pubkey,
        output_mint: &Pubkey,
    ) -> bool {
        self.quotes
            .contains_key(&(*pool_address, *input_mint, *output_mint))
    }

    fn get_cached_probe_quote(
        &self,
        pool_address: &Pubkey,
        dex: DexType,
        input_mint: &Pubkey,
        output_mint: &Pubkey,
        amount_in: u64,
    ) -> Option<u64> {
        self.probe_lookups
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if !self.has_cached_quote(pool_address, input_mint, output_mint) {
            return None;
        }
        self.get_quote(pool_address, dex, input_mint, output_mint, amount_in)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_pubkey(byte: u8) -> Pubkey {
        Pubkey::new_from_array([byte; 32])
    }

    fn make_edge(pool: u8, mint_a: u8, mint_b: u8, liquidity: f64) -> PoolEdge {
        PoolEdge::new(
            test_pubkey(pool),
            DexType::RaydiumAmmV4,
            test_pubkey(mint_a),
            test_pubkey(mint_b),
            liquidity,
            30,
        )
    }

    #[test]
    fn test_edge_ratio_calculation() {
        let wsol = test_pubkey(0x01);
        let usdc = test_pubkey(0x02);
        let pool = test_pubkey(0x10);

        let mut mock = MockQuoteProvider::new();
        // 10M lamports in -> 9.7M out (3% fee/slippage)
        mock.add_quote(pool, wsol, usdc, DEFAULT_PROBE_LAMPORTS, 9_700_000);

        let ranker = PoolRanker::new(mock);
        let graph = PoolGraph::new();
        graph.upsert_pool(make_edge(0x10, 0x01, 0x02, 100_000.0));

        let ranked = ranker.rank_pools_from(&graph, &wsol);
        assert_eq!(ranked.len(), 1);

        let (output_mint, pools) = &ranked[0];
        assert_eq!(*output_mint, usdc);
        assert_eq!(pools.len(), 1);

        // Edge ratio should be 0.97 (97%)
        let ratio = pools[0].edge_ratio;
        assert!((ratio - 0.97).abs() < 0.001, "Expected ~0.97, got {ratio}");
    }

    #[test]
    fn test_liquidity_score_clamping() {
        let wsol = test_pubkey(0x01);
        let pool_low_liq = test_pubkey(0x10);
        let pool_high_liq = test_pubkey(0x11);

        let mut mock = MockQuoteProvider::new();
        mock.add_quote(
            pool_low_liq,
            wsol,
            test_pubkey(0x02),
            DEFAULT_PROBE_LAMPORTS,
            9_800_000,
        );
        mock.add_quote(
            pool_high_liq,
            wsol,
            test_pubkey(0x03),
            DEFAULT_PROBE_LAMPORTS,
            9_800_000,
        );

        let ranker = PoolRanker::new(mock);
        let graph = PoolGraph::new();

        // Very low liquidity pool ($1k) - should clamp to MIN (0.3)
        graph.upsert_pool(make_edge(0x10, 0x01, 0x02, 1_000.0));
        // Very high liquidity pool ($1M) - should clamp to MAX (1.5)
        graph.upsert_pool(make_edge(0x11, 0x01, 0x03, 1_000_000.0));

        let ranked = ranker.rank_pools_from(&graph, &wsol);

        // Find the pools
        let low_liq_pool = ranked
            .iter()
            .find(|(m, _)| *m == test_pubkey(0x02))
            .map(|(_, p)| &p[0]);
        let high_liq_pool = ranked
            .iter()
            .find(|(m, _)| *m == test_pubkey(0x03))
            .map(|(_, p)| &p[0]);

        assert!(low_liq_pool.is_some());
        assert!(high_liq_pool.is_some());

        // Check clamping
        let low_score = low_liq_pool.unwrap().liquidity_score;
        let high_score = high_liq_pool.unwrap().liquidity_score;

        assert!(
            (low_score - MIN_LIQUIDITY_SCORE).abs() < 0.01,
            "Low liq score should be clamped to {MIN_LIQUIDITY_SCORE}, got {low_score}"
        );
        assert!(
            (high_score - MAX_LIQUIDITY_SCORE).abs() < 0.01,
            "High liq score should be clamped to {MAX_LIQUIDITY_SCORE}, got {high_score}"
        );
    }

    #[test]
    fn test_upper_bound_computation() {
        let wsol = test_pubkey(0x01);
        let usdc = test_pubkey(0x02);
        let pool = test_pubkey(0x10);

        let mut mock = MockQuoteProvider::new();
        // 2% profit edge ratio
        mock.add_quote(pool, wsol, usdc, DEFAULT_PROBE_LAMPORTS, 10_200_000);

        let ranker = PoolRanker::new(mock);
        let graph = PoolGraph::new();
        graph.upsert_pool(make_edge(0x10, 0x01, 0x02, 100_000.0));

        // Trigger edge ratio caching
        let _ = ranker.rank_pools_from(&graph, &wsol);

        // Check max edge ratio was cached
        let max_ratio = ranker.max_edge_ratio_for(&wsol);
        assert!(
            (max_ratio - 1.02).abs() < 0.001,
            "Expected ~1.02, got {max_ratio}"
        );

        // Upper bound with 3 remaining hops
        let upper = ranker.compute_upper_bound(1.0, &wsol, 3);
        let expected = 1.02_f64.powi(3); // ~1.0612
        assert!(
            (upper - expected).abs() < 0.001,
            "Expected ~{expected}, got {upper}"
        );
    }

    #[test]
    fn test_ranking_order() {
        let wsol = test_pubkey(0x01);
        let usdc = test_pubkey(0x02);

        let pool_a = test_pubkey(0x10);
        let pool_b = test_pubkey(0x11);

        let mut mock = MockQuoteProvider::new();
        // Pool A: lower ratio but same liquidity
        mock.add_quote(pool_a, wsol, usdc, DEFAULT_PROBE_LAMPORTS, 9_500_000);
        // Pool B: higher ratio
        mock.add_quote(pool_b, wsol, usdc, DEFAULT_PROBE_LAMPORTS, 9_800_000);

        let ranker = PoolRanker::new(mock);
        let graph = PoolGraph::new();
        graph.upsert_pool(make_edge(0x10, 0x01, 0x02, 100_000.0));
        graph.upsert_pool(make_edge(0x11, 0x01, 0x02, 100_000.0));

        let ranked = ranker.rank_pools_from(&graph, &wsol);
        let (_, pools) = &ranked[0];

        // Pool B (higher ratio) should be first
        assert_eq!(pools[0].edge.pool_address, pool_b);
        assert_eq!(pools[1].edge.pool_address, pool_a);
    }
}
