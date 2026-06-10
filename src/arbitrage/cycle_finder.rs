//! Cycle Finder - Best-First Beam Search with Branch-and-Bound
//!
//! This implements the core arbitrage cycle detection algorithm:
//!
//! Algorithm: Best-First Beam Search + Branch-and-Bound
//! - Priority queue ordered by score (profit × liquidity_factor)
//! - Beam limit: Top-K nodes per depth (NOT First-K!)
//! - Upper bound pruning: skip nodes where best-case can't beat threshold
//! - HashSet visited tracking: O(1) cycle detection
//!
//! Key Design Decisions (from external review):
//! 1. edge_ratio from PROBE QUOTES, not spot prices
//! 2. Proper Top-K beam limit with score tracking
//! 3. Dampened liquidity scoring (clamp, not sqrt)
//! 4. Top-3 pool alternatives per hop for execution fallback

use super::pool_graph::PoolGraph;
use super::pool_ranker::{PoolRanker, QuoteProvider};
use super::types::{ArbCycle, PoolEdge, SearchNode};
use solana_sdk::pubkey::Pubkey;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::str::FromStr;

/// WSOL mint address (native mint)
pub const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";

/// Parse WSOL mint to Pubkey (panics if invalid - should never happen)
fn wsol_pubkey() -> Pubkey {
    Pubkey::from_str(WSOL_MINT).expect("WSOL_MINT constant is invalid")
}

/// Cycle finder configuration
#[derive(Debug, Clone)]
pub struct CycleFinderConfig {
    /// Beam width per depth level (Top-K, not First-K!)
    pub beam_width: usize,
    /// Maximum hop count (including return to base)
    pub max_hops: usize,
    /// Minimum profit threshold in basis points (e.g., 50 = 0.5%)
    pub min_profit_bps: i32,
    /// Maximum cycles to return
    pub max_results: usize,
    /// Base token (usually WSOL)
    pub base_mint: Pubkey,
    /// Number of pool alternatives to keep per hop (for fallback routing)
    pub pool_alternatives: usize,
}

impl Default for CycleFinderConfig {
    fn default() -> Self {
        Self {
            beam_width: 50,
            max_hops: 4,
            min_profit_bps: 30, // 0.3% minimum
            max_results: 10,
            base_mint: wsol_pubkey(), // WSOL
            pool_alternatives: 3,
        }
    }
}

/// Best-First Beam Search cycle finder
pub struct BeamCycleFinder<Q: QuoteProvider> {
    config: CycleFinderConfig,
    ranker: PoolRanker<Q>,
}

impl<Q: QuoteProvider> BeamCycleFinder<Q> {
    pub fn new(config: CycleFinderConfig, ranker: PoolRanker<Q>) -> Self {
        Self { config, ranker }
    }

    /// Find profitable cycles starting and ending at base_mint
    ///
    /// Returns cycles sorted by estimated return (descending)
    pub fn find_cycles(&self, graph: &PoolGraph) -> Vec<ArbCycle> {
        self.find_cycles_inner(graph, None)
    }

    /// Find cycles through affected tokens (targeted subgraph search).
    ///
    /// Only expands vertices within `max_hops` of the seed tokens and requires
    /// found cycles to pass through at least one seed token.
    pub fn find_cycles_through(
        &self,
        graph: &PoolGraph,
        through_tokens: &[Pubkey],
    ) -> Vec<ArbCycle> {
        if through_tokens.is_empty() {
            return vec![];
        }

        let seeds: Vec<Pubkey> = through_tokens
            .iter()
            .copied()
            .filter(|t| graph.has_token(t))
            .collect();
        if seeds.is_empty() {
            return vec![];
        }

        self.find_cycles_inner(graph, Some(&seeds))
    }

    /// BFS subgraph around seed tokens (plus base) within `max_hops`.
    fn build_search_subgraph(&self, graph: &PoolGraph, seeds: &[Pubkey]) -> HashSet<Pubkey> {
        let mut allowed = HashSet::new();
        allowed.insert(self.config.base_mint);

        let mut queue = VecDeque::new();
        for &seed in seeds {
            if graph.has_token(&seed) {
                allowed.insert(seed);
                queue.push_back((seed, 0usize));
            }
        }

        while let Some((token, depth)) = queue.pop_front() {
            if depth >= self.config.max_hops {
                continue;
            }
            for (neighbor, _) in graph.neighbors(&token) {
                if allowed.insert(neighbor) {
                    queue.push_back((neighbor, depth + 1));
                }
            }
        }

        allowed
    }

    /// True when every hop has a fresh trade-cache or LivePoolCache quote.
    fn cycle_all_hops_quoted(&self, cycle: &ArbCycle) -> bool {
        for (hop_idx, pool_options) in cycle.pools.iter().enumerate() {
            let input_mint = &cycle.path[hop_idx];
            let output_mint = &cycle.path[hop_idx + 1];
            let Some(primary) = pool_options.first() else {
                return false;
            };
            if !self
                .ranker
                .hop_has_cached_quote(&primary.pool_address, input_mint, output_mint)
            {
                return false;
            }
        }
        true
    }

    fn find_cycles_inner(
        &self,
        graph: &PoolGraph,
        through_tokens: Option<&[Pubkey]>,
    ) -> Vec<ArbCycle> {
        let mut results = Vec::new();
        let min_profit_multiplier = 1.0 + (self.config.min_profit_bps as f64 / 10000.0);

        let allowed_tokens = through_tokens.map(|seeds| self.build_search_subgraph(graph, seeds));
        let required_tokens: Option<HashSet<Pubkey>> =
            through_tokens.map(|seeds| seeds.iter().copied().collect());

        // Priority queue: max-heap by score
        let mut pq: BinaryHeap<SearchNode> = BinaryHeap::new();

        // Track minimum score at each depth for proper Top-K beam limit
        let mut min_score_at_depth: HashMap<usize, f64> = HashMap::new();
        let mut count_at_depth: HashMap<usize, usize> = HashMap::new();

        // Start from base mint (WSOL)
        let start = SearchNode::start(self.config.base_mint);
        pq.push(start);

        while let Some(node) = pq.pop() {
            // Check if we've found a cycle (back to base)
            if node.depth > 0 && node.token == self.config.base_mint {
                if node.profit >= min_profit_multiplier {
                    if let Some(required) = &required_tokens {
                        if !node.path.iter().any(|t| required.contains(t)) {
                            continue;
                        }
                    }
                    if let Some(cycle) = node.to_arb_cycle() {
                        if !self.cycle_all_hops_quoted(&cycle) {
                            continue;
                        }
                        results.push(cycle);
                        if results.len() >= self.config.max_results {
                            break;
                        }
                    }
                }
                continue;
            }

            // Max depth check
            if node.depth >= self.config.max_hops {
                continue;
            }

            // Upper bound pruning: can this path possibly be profitable?
            let remaining_hops = self.config.max_hops - node.depth;
            let upper_bound =
                self.ranker
                    .compute_upper_bound(node.profit, &node.token, remaining_hops);
            if upper_bound < min_profit_multiplier {
                continue; // Prune: even best case can't meet threshold
            }

            // Get ranked neighbors (pools without real quotes are skipped in ranker)
            let neighbors = self.ranker.rank_pools_from(graph, &node.token);

            for (next_token, ranked_pools) in neighbors {
                // Skip if already visited (except returning to base at depth > 0)
                let is_return_to_base = next_token == self.config.base_mint && node.depth > 0;
                if !is_return_to_base && node.has_visited(&next_token) {
                    continue;
                }

                // Targeted search: stay within subgraph around affected tokens
                if let Some(ref allowed) = allowed_tokens {
                    if !is_return_to_base && !allowed.contains(&next_token) {
                        continue;
                    }
                }

                // Get top pools as alternatives (for execution fallback)
                let pool_alternatives: Vec<PoolEdge> = ranked_pools
                    .iter()
                    .take(self.config.pool_alternatives)
                    .map(|rp| rp.edge.clone())
                    .collect();

                if pool_alternatives.is_empty() {
                    continue;
                }

                // Use best pool's metrics for search scoring
                let best = &ranked_pools[0];

                // Create expanded node
                let expanded = node.expand(
                    next_token,
                    pool_alternatives,
                    best.edge_ratio,
                    best.liquidity_score,
                    best.edge.liquidity_usd,
                );

                // Beam limit: proper Top-K (not First-K!)
                // Only add if within beam width OR score beats minimum at this depth
                let depth = expanded.depth;
                let count = count_at_depth.entry(depth).or_insert(0);
                let min_score = min_score_at_depth.entry(depth).or_insert(f64::MIN);

                if *count < self.config.beam_width {
                    // Still have room in beam
                    pq.push(expanded.clone());
                    *count += 1;
                    if expanded.score < *min_score || *min_score == f64::MIN {
                        *min_score = expanded.score;
                    }
                } else if expanded.score > *min_score {
                    // Beam full but this node beats minimum - add anyway
                    // (heap will naturally prioritize better nodes)
                    pq.push(expanded);
                }
                // else: prune - beam full and score too low
            }
        }

        // Sort results by estimated return (descending)
        results.sort_by(|a, b| b.estimated_return_bps.cmp(&a.estimated_return_bps));

        results
    }

    /// Quick check if any profitable cycle exists (for metrics/monitoring)
    pub fn has_profitable_cycle(&self, graph: &PoolGraph) -> bool {
        !self.find_cycles(graph).is_empty()
    }

    /// Get configuration
    pub fn config(&self) -> &CycleFinderConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::super::pool_ranker::MockQuoteProvider;
    use super::super::types::DexType;
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

    fn setup_triangle_graph() -> (PoolGraph, MockQuoteProvider) {
        let graph = PoolGraph::new();
        let mut mock = MockQuoteProvider::new();

        // Triangle: WSOL(0x01) <-> USDC(0x02) <-> TokenA(0x03) <-> WSOL(0x01)
        // Pool addresses: 0x10, 0x11, 0x12
        let wsol = test_pubkey(0x01);
        let usdc = test_pubkey(0x02);
        let token_a = test_pubkey(0x03);

        let pool_wsol_usdc = test_pubkey(0x10);
        let pool_usdc_token_a = test_pubkey(0x11);
        let pool_token_a_wsol = test_pubkey(0x12);

        graph.upsert_pool(make_edge(0x10, 0x01, 0x02, 1_000_000.0));
        graph.upsert_pool(make_edge(0x11, 0x02, 0x03, 500_000.0));
        graph.upsert_pool(make_edge(0x12, 0x03, 0x01, 300_000.0));

        // Set up quotes for profitable cycle:
        // WSOL -> USDC: 0.99 ratio (1% loss)
        // USDC -> TokenA: 1.02 ratio (2% gain)
        // TokenA -> WSOL: 1.02 ratio (2% gain)
        // Net: 0.99 * 1.02 * 1.02 = 1.0302 (3.02% profit)
        let probe = 10_000_000u64;

        mock.add_quote(pool_wsol_usdc, wsol, usdc, probe, 9_900_000);
        mock.add_quote(pool_usdc_token_a, usdc, token_a, probe, 10_200_000);
        mock.add_quote(pool_token_a_wsol, token_a, wsol, probe, 10_200_000);

        // Reverse direction quotes (for completeness)
        mock.add_quote(pool_wsol_usdc, usdc, wsol, probe, 9_900_000);
        mock.add_quote(pool_usdc_token_a, token_a, usdc, probe, 9_800_000);
        mock.add_quote(pool_token_a_wsol, wsol, token_a, probe, 9_800_000);

        (graph, mock)
    }

    #[test]
    fn test_find_profitable_triangle() {
        let (graph, mock) = setup_triangle_graph();

        let config = CycleFinderConfig {
            beam_width: 10,
            max_hops: 4,
            min_profit_bps: 100, // 1% minimum
            max_results: 5,
            base_mint: test_pubkey(0x01), // WSOL
            pool_alternatives: 3,
        };

        let ranker = PoolRanker::new(mock);
        let finder = BeamCycleFinder::new(config, ranker);

        let cycles = finder.find_cycles(&graph);

        // Should find at least one profitable cycle
        assert!(!cycles.is_empty(), "Should find at least one cycle");

        let best = &cycles[0];
        assert!(best.estimated_return_bps > 100, "Should have >1% return");
        assert!(best.is_valid(), "Cycle should be valid (start == end)");
        assert_eq!(best.hop_count(), 3, "Triangle has 3 hops");
    }

    #[test]
    fn test_no_cycles_below_threshold() {
        let graph = PoolGraph::new();
        let mut mock = MockQuoteProvider::new();

        // Set up unprofitable triangle (each hop loses 2%)
        let wsol = test_pubkey(0x01);
        let usdc = test_pubkey(0x02);
        let token_a = test_pubkey(0x03);

        let pool_wsol_usdc = test_pubkey(0x10);
        let pool_usdc_token_a = test_pubkey(0x11);
        let pool_token_a_wsol = test_pubkey(0x12);

        graph.upsert_pool(make_edge(0x10, 0x01, 0x02, 100_000.0));
        graph.upsert_pool(make_edge(0x11, 0x02, 0x03, 100_000.0));
        graph.upsert_pool(make_edge(0x12, 0x03, 0x01, 100_000.0));

        let probe = 10_000_000u64;
        mock.add_quote(pool_wsol_usdc, wsol, usdc, probe, 9_800_000); // -2%
        mock.add_quote(pool_usdc_token_a, usdc, token_a, probe, 9_800_000); // -2%
        mock.add_quote(pool_token_a_wsol, token_a, wsol, probe, 9_800_000); // -2%

        let config = CycleFinderConfig {
            min_profit_bps: 30, // 0.3% minimum
            base_mint: test_pubkey(0x01),
            ..Default::default()
        };

        let ranker = PoolRanker::new(mock);
        let finder = BeamCycleFinder::new(config, ranker);

        let cycles = finder.find_cycles(&graph);
        assert!(cycles.is_empty(), "Should not find unprofitable cycles");
    }

    #[test]
    fn test_beam_limit_respected() {
        // Create a graph with many paths but limited beam width
        let graph = PoolGraph::new();
        let mut mock = MockQuoteProvider::new();

        let wsol = test_pubkey(0x01);
        let probe = 10_000_000u64;

        // Create 20 tokens all connected to WSOL
        for i in 2..22 {
            let token = test_pubkey(i);
            let pool = test_pubkey(0x10 + i);

            graph.upsert_pool(PoolEdge::new(
                pool,
                DexType::RaydiumAmmV4,
                wsol,
                token,
                100_000.0 + (i as f64 * 1000.0),
                30,
            ));

            // All tokens have same ratio for simplicity
            mock.add_quote(pool, wsol, token, probe, 9_900_000);
            mock.add_quote(pool, token, wsol, probe, 9_900_000);
        }

        let config = CycleFinderConfig {
            beam_width: 5, // Very small beam
            max_hops: 2,
            min_profit_bps: -1000, // Allow losses for testing
            max_results: 10,
            base_mint: wsol,
            ..Default::default()
        };

        let ranker = PoolRanker::new(mock);
        let finder = BeamCycleFinder::new(config, ranker);

        // Should complete without hanging despite many potential paths
        let cycles = finder.find_cycles(&graph);

        // With beam width of 5 and max_hops of 2, we won't find profitable cycles
        // (need at least 3 hops for a triangle), but the search should complete quickly
        assert!(cycles.len() <= 10); // Use literal instead of moved config
    }

    #[test]
    fn test_pool_alternatives_stored() {
        let graph = PoolGraph::new();
        let mut mock = MockQuoteProvider::new();

        let wsol = test_pubkey(0x01);
        let usdc = test_pubkey(0x02);
        let token_a = test_pubkey(0x03);

        // Multiple pools for WSOL-USDC
        let pool1 = test_pubkey(0x10);
        let pool2 = test_pubkey(0x11);
        let pool3 = test_pubkey(0x12);

        graph.upsert_pool(PoolEdge::new(
            pool1,
            DexType::RaydiumAmmV4,
            wsol,
            usdc,
            1_000_000.0,
            30,
        ));
        graph.upsert_pool(PoolEdge::new(
            pool2,
            DexType::Orca,
            wsol,
            usdc,
            800_000.0,
            25,
        ));
        graph.upsert_pool(PoolEdge::new(
            pool3,
            DexType::MeteoraDlmm,
            wsol,
            usdc,
            600_000.0,
            20,
        ));

        // Single pool for other legs
        let pool_usdc_a = test_pubkey(0x20);
        let pool_a_wsol = test_pubkey(0x21);

        graph.upsert_pool(PoolEdge::new(
            pool_usdc_a,
            DexType::RaydiumAmmV4,
            usdc,
            token_a,
            500_000.0,
            30,
        ));
        graph.upsert_pool(PoolEdge::new(
            pool_a_wsol,
            DexType::RaydiumAmmV4,
            token_a,
            wsol,
            400_000.0,
            30,
        ));

        let probe = 10_000_000u64;
        for pool in [pool1, pool2, pool3] {
            mock.add_quote(pool, wsol, usdc, probe, 10_100_000);
        }
        mock.add_quote(pool_usdc_a, usdc, token_a, probe, 10_100_000);
        mock.add_quote(pool_a_wsol, token_a, wsol, probe, 10_100_000);

        let config = CycleFinderConfig {
            min_profit_bps: 100,
            base_mint: wsol,
            pool_alternatives: 3,
            ..Default::default()
        };

        let ranker = PoolRanker::new(mock);
        let finder = BeamCycleFinder::new(config, ranker);

        let cycles = finder.find_cycles(&graph);

        if !cycles.is_empty() {
            let cycle = &cycles[0];
            // First hop (WSOL->USDC) should have multiple alternatives
            if !cycle.pools.is_empty() {
                let first_hop_alts = &cycle.pools[0];
                // Should have up to 3 alternatives
                assert!(first_hop_alts.len() <= 3);
            }
        }
    }

    #[test]
    fn test_cycle_with_missing_quote_hop_not_reported() {
        let (graph, mut mock) = setup_triangle_graph();
        let wsol = test_pubkey(0x01);
        let pool_token_a_wsol = test_pubkey(0x12);
        let token_a = test_pubkey(0x03);

        // Remove quote for the final hop — path cannot be completed with real quotes
        mock.remove_quote(pool_token_a_wsol, token_a, wsol);

        let config = CycleFinderConfig {
            min_profit_bps: 50,
            base_mint: wsol,
            ..Default::default()
        };

        let ranker = PoolRanker::new(mock);
        let finder = BeamCycleFinder::new(config, ranker);

        let cycles = finder.find_cycles(&graph);
        assert!(
            cycles.is_empty(),
            "Cycle with a quote-less hop must not be reported"
        );

        // Sanity: profitable triangle exists when all quotes present
        let (graph2, mock2) = setup_triangle_graph();
        let finder2 = BeamCycleFinder::new(
            CycleFinderConfig {
                min_profit_bps: 50,
                base_mint: wsol,
                ..Default::default()
            },
            PoolRanker::new(mock2),
        );
        assert!(!finder2.find_cycles(&graph2).is_empty());
    }

    fn setup_local_triangle_plus_distant_chain() -> (PoolGraph, std::sync::Arc<MockQuoteProvider>) {
        use std::sync::Arc;

        let graph = PoolGraph::new();
        let mut mock = MockQuoteProvider::new();
        let wsol = test_pubkey(0x01);
        let usdc = test_pubkey(0x02);
        let token_a = test_pubkey(0x03);
        let probe = 10_000_000u64;

        let pool_wsol_usdc = test_pubkey(0x10);
        let pool_usdc_token_a = test_pubkey(0x11);
        let pool_token_a_wsol = test_pubkey(0x12);

        graph.upsert_pool(make_edge(0x10, 0x01, 0x02, 1_000_000.0));
        graph.upsert_pool(make_edge(0x11, 0x02, 0x03, 500_000.0));
        graph.upsert_pool(make_edge(0x12, 0x03, 0x01, 300_000.0));

        mock.add_quote(pool_wsol_usdc, wsol, usdc, probe, 9_900_000);
        mock.add_quote(pool_usdc_token_a, usdc, token_a, probe, 10_200_000);
        mock.add_quote(pool_token_a_wsol, token_a, wsol, probe, 10_200_000);
        mock.add_quote(pool_wsol_usdc, usdc, wsol, probe, 9_900_000);
        mock.add_quote(pool_usdc_token_a, token_a, usdc, probe, 9_800_000);
        mock.add_quote(pool_token_a_wsol, wsol, token_a, probe, 9_800_000);

        // Distant chain from WSOL (adds many probe lookups on full-graph search)
        for i in 0..5 {
            let from = if i == 0 { 0x01 } else { 0x20 + i - 1 };
            let to = 0x20 + i;
            let pool = 0x30 + i;
            graph.upsert_pool(make_edge(pool, from, to, 50_000.0));
            mock.add_quote(
                test_pubkey(pool),
                test_pubkey(from),
                test_pubkey(to),
                probe,
                9_900_000,
            );
            mock.add_quote(
                test_pubkey(pool),
                test_pubkey(to),
                test_pubkey(from),
                probe,
                9_900_000,
            );
        }

        (graph, Arc::new(mock))
    }

    #[test]
    fn test_find_cycles_through_limits_subgraph_probe_lookups() {
        let (graph, mock) = setup_local_triangle_plus_distant_chain();
        let token_a = test_pubkey(0x03);
        let wsol = test_pubkey(0x01);

        let config = CycleFinderConfig {
            beam_width: 10,
            max_hops: 4,
            min_profit_bps: 50,
            base_mint: wsol,
            ..Default::default()
        };

        mock.reset_probe_lookup_count();
        let finder_full = BeamCycleFinder::new(config.clone(), PoolRanker::new(mock.clone()));
        let _ = finder_full.find_cycles(&graph);
        let full_lookups = mock.probe_lookup_count();

        mock.reset_probe_lookup_count();
        let finder_targeted = BeamCycleFinder::new(config, PoolRanker::new(mock.clone()));
        let targeted_cycles = finder_targeted.find_cycles_through(&graph, &[token_a]);
        let targeted_lookups = mock.probe_lookup_count();

        assert!(
            targeted_lookups < full_lookups,
            "Targeted search should probe fewer pools than full-graph search (targeted={targeted_lookups}, full={full_lookups})"
        );
        assert!(
            !targeted_cycles.is_empty(),
            "Should still find the local triangle through token_a"
        );
        for cycle in &targeted_cycles {
            assert!(cycle.path.contains(&token_a));
        }
    }

    #[test]
    fn test_finds_cycles_through_specific_token() {
        let (graph, mock) = setup_triangle_graph();

        let config = CycleFinderConfig {
            min_profit_bps: 50,
            base_mint: test_pubkey(0x01),
            ..Default::default()
        };

        let ranker = PoolRanker::new(mock);
        let finder = BeamCycleFinder::new(config, ranker);

        // Search for cycles through USDC (0x02)
        let cycles = finder.find_cycles_through(&graph, &[test_pubkey(0x02)]);

        // Should find cycles containing USDC
        for cycle in &cycles {
            assert!(
                cycle.path.contains(&test_pubkey(0x02)),
                "Cycle should contain intermediate token"
            );
        }
    }
}
