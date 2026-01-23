//! Pool Graph - Adjacency list representation for multi-hop arbitrage
//!
//! The pool graph is the core data structure for cycle detection:
//! - Vertices = Token mints
//! - Edges = Pools (bidirectional, since swaps work both ways)
//!
//! Design notes:
//! - Uses HashMap adjacency list for O(1) neighbor lookup
//! - Stores multiple pools per token pair (Top-K for fallback routing)
//! - Thread-safe with RwLock for concurrent reads during search

use super::types::PoolEdge;
use parking_lot::RwLock;
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;

/// Maximum pools to keep per token pair (Top-K for fallback routing)
const MAX_POOLS_PER_PAIR: usize = 3;

/// Pool graph for multi-hop arbitrage cycle detection
///
/// Structure: mint -> { neighbor_mint -> [PoolEdge, ...] }
/// Each token pair can have multiple pools (different DEXes or pool types)
pub struct PoolGraph {
    /// Adjacency list: token -> (neighbor_token -> pools)
    adj: RwLock<HashMap<Pubkey, HashMap<Pubkey, Vec<PoolEdge>>>>,
    /// Reverse index: pool_address -> (mint_a, mint_b)
    pool_index: RwLock<HashMap<Pubkey, (Pubkey, Pubkey)>>,
    /// Stats
    stats: RwLock<GraphStats>,
}

#[derive(Debug, Default, Clone)]
pub struct GraphStats {
    pub total_vertices: usize,
    pub total_edges: usize,
    pub total_pools: usize,
    pub last_update_ts: u64,
}

impl PoolGraph {
    /// Create empty graph
    pub fn new() -> Self {
        Self {
            adj: RwLock::new(HashMap::new()),
            pool_index: RwLock::new(HashMap::new()),
            stats: RwLock::new(GraphStats::default()),
        }
    }

    /// Insert or update a pool edge
    ///
    /// If pool already exists: update it
    /// If new pool: add it (up to MAX_POOLS_PER_PAIR per pair)
    pub fn upsert_pool(&self, edge: PoolEdge) {
        let mut adj = self.adj.write();
        let mut pool_index = self.pool_index.write();

        // Check if pool already exists
        if let Some((old_mint_a, old_mint_b)) = pool_index.get(&edge.pool_address) {
            // Pool exists - remove old entry if mints changed
            if *old_mint_a != edge.mint_a || *old_mint_b != edge.mint_b {
                self.remove_pool_from_adj(&mut adj, &edge.pool_address, old_mint_a, old_mint_b);
            }
        }

        // Add/update in both directions (undirected graph)
        self.add_edge_to_adj(&mut adj, edge.mint_a, edge.mint_b, edge.clone());
        self.add_edge_to_adj(&mut adj, edge.mint_b, edge.mint_a, edge.clone());

        // Update reverse index
        pool_index.insert(edge.pool_address, (edge.mint_a, edge.mint_b));

        // Update stats
        self.refresh_stats_locked(&adj);
    }

    /// Remove a pool by address
    pub fn remove_pool(&self, pool_address: &Pubkey) {
        let mut adj = self.adj.write();
        let mut pool_index = self.pool_index.write();

        if let Some((mint_a, mint_b)) = pool_index.remove(pool_address) {
            self.remove_pool_from_adj(&mut adj, pool_address, &mint_a, &mint_b);
            self.refresh_stats_locked(&adj);
        }
    }

    /// Get all neighbors of a token with their pools
    ///
    /// Returns: Vec<(neighbor_mint, Vec<PoolEdge>)>
    /// Pools are sorted by liquidity (descending) for ranking
    pub fn neighbors(&self, mint: &Pubkey) -> Vec<(Pubkey, Vec<PoolEdge>)> {
        let adj = self.adj.read();

        adj.get(mint)
            .map(|neighbors| {
                neighbors
                    .iter()
                    .map(|(neighbor, pools)| {
                        let mut sorted_pools = pools.clone();
                        sorted_pools.sort_by(|a, b| {
                            b.liquidity_usd
                                .partial_cmp(&a.liquidity_usd)
                                .unwrap_or(std::cmp::Ordering::Equal)
                        });
                        (*neighbor, sorted_pools)
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get pools between two specific mints
    pub fn pools_between(&self, mint_a: &Pubkey, mint_b: &Pubkey) -> Vec<PoolEdge> {
        let adj = self.adj.read();

        adj.get(mint_a)
            .and_then(|neighbors| neighbors.get(mint_b))
            .cloned()
            .unwrap_or_default()
    }

    /// Check if a token exists in the graph
    pub fn has_token(&self, mint: &Pubkey) -> bool {
        self.adj.read().contains_key(mint)
    }

    /// Get all tokens (vertices) in the graph
    pub fn all_tokens(&self) -> Vec<Pubkey> {
        self.adj.read().keys().cloned().collect()
    }

    /// Get current stats
    pub fn stats(&self) -> GraphStats {
        self.stats.read().clone()
    }

    /// Count unique neighbors for a token (degree)
    pub fn degree(&self, mint: &Pubkey) -> usize {
        self.adj
            .read()
            .get(mint)
            .map(|n| n.len())
            .unwrap_or(0)
    }

    /// Clear all data
    pub fn clear(&self) {
        self.adj.write().clear();
        self.pool_index.write().clear();
        *self.stats.write() = GraphStats::default();
    }

    /// Bulk insert pools (more efficient than individual upserts)
    pub fn bulk_upsert(&self, edges: Vec<PoolEdge>) {
        let mut adj = self.adj.write();
        let mut pool_index = self.pool_index.write();

        for edge in edges {
            // Check if pool already exists
            if let Some((old_mint_a, old_mint_b)) = pool_index.get(&edge.pool_address) {
                if *old_mint_a != edge.mint_a || *old_mint_b != edge.mint_b {
                    self.remove_pool_from_adj(&mut adj, &edge.pool_address, old_mint_a, old_mint_b);
                }
            }

            self.add_edge_to_adj(&mut adj, edge.mint_a, edge.mint_b, edge.clone());
            self.add_edge_to_adj(&mut adj, edge.mint_b, edge.mint_a, edge.clone());
            pool_index.insert(edge.pool_address, (edge.mint_a, edge.mint_b));
        }

        self.refresh_stats_locked(&adj);
    }

    // ─── Private Helpers ───────────────────────────────────────────────────

    fn add_edge_to_adj(
        &self,
        adj: &mut HashMap<Pubkey, HashMap<Pubkey, Vec<PoolEdge>>>,
        from: Pubkey,
        to: Pubkey,
        edge: PoolEdge,
    ) {
        let neighbors = adj.entry(from).or_default();
        let pools = neighbors.entry(to).or_default();

        // Check if pool already exists in this direction
        if let Some(idx) = pools.iter().position(|p| p.pool_address == edge.pool_address) {
            // Update existing
            pools[idx] = edge;
        } else {
            // Add new
            pools.push(edge);

            // Keep only Top-K by liquidity
            if pools.len() > MAX_POOLS_PER_PAIR {
                pools.sort_by(|a, b| {
                    b.liquidity_usd
                        .partial_cmp(&a.liquidity_usd)
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
                pools.truncate(MAX_POOLS_PER_PAIR);
            }
        }
    }

    fn remove_pool_from_adj(
        &self,
        adj: &mut HashMap<Pubkey, HashMap<Pubkey, Vec<PoolEdge>>>,
        pool_address: &Pubkey,
        mint_a: &Pubkey,
        mint_b: &Pubkey,
    ) {
        // Remove from mint_a -> mint_b
        if let Some(neighbors) = adj.get_mut(mint_a) {
            if let Some(pools) = neighbors.get_mut(mint_b) {
                pools.retain(|p| p.pool_address != *pool_address);
                if pools.is_empty() {
                    neighbors.remove(mint_b);
                }
            }
            if neighbors.is_empty() {
                adj.remove(mint_a);
            }
        }

        // Remove from mint_b -> mint_a
        if let Some(neighbors) = adj.get_mut(mint_b) {
            if let Some(pools) = neighbors.get_mut(mint_a) {
                pools.retain(|p| p.pool_address != *pool_address);
                if pools.is_empty() {
                    neighbors.remove(mint_a);
                }
            }
            if neighbors.is_empty() {
                adj.remove(mint_b);
            }
        }
    }

    fn refresh_stats_locked(&self, adj: &HashMap<Pubkey, HashMap<Pubkey, Vec<PoolEdge>>>) {
        let mut stats = self.stats.write();
        stats.total_vertices = adj.len();

        let mut edge_count = 0;
        let mut pool_count = 0;
        for neighbors in adj.values() {
            edge_count += neighbors.len();
            for pools in neighbors.values() {
                pool_count += pools.len();
            }
        }
        // Divide by 2 because we store both directions
        stats.total_edges = edge_count / 2;
        stats.total_pools = pool_count / 2;
        stats.last_update_ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
    }
}

impl Default for PoolGraph {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
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

    #[test]
    fn test_basic_insert_and_lookup() {
        let graph = PoolGraph::new();
        let wsol = test_pubkey(0x01);
        let usdc = test_pubkey(0x02);

        let edge = make_edge(0x10, 0x01, 0x02, 1_000_000.0);
        graph.upsert_pool(edge);

        assert!(graph.has_token(&wsol));
        assert!(graph.has_token(&usdc));
        assert_eq!(graph.degree(&wsol), 1);
        assert_eq!(graph.degree(&usdc), 1);

        let neighbors = graph.neighbors(&wsol);
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0].0, usdc);
        assert_eq!(neighbors[0].1.len(), 1);
    }

    #[test]
    fn test_multiple_pools_same_pair() {
        let graph = PoolGraph::new();

        // Add 3 pools for same pair with different liquidity
        graph.upsert_pool(make_edge(0x10, 0x01, 0x02, 100.0));
        graph.upsert_pool(make_edge(0x11, 0x01, 0x02, 300.0));
        graph.upsert_pool(make_edge(0x12, 0x01, 0x02, 200.0));

        let pools = graph.pools_between(&test_pubkey(0x01), &test_pubkey(0x02));
        assert_eq!(pools.len(), 3);

        // Should be sorted by liquidity descending
        let neighbors = graph.neighbors(&test_pubkey(0x01));
        assert_eq!(neighbors[0].1[0].liquidity_usd, 300.0);
        assert_eq!(neighbors[0].1[1].liquidity_usd, 200.0);
        assert_eq!(neighbors[0].1[2].liquidity_usd, 100.0);
    }

    #[test]
    fn test_max_pools_per_pair() {
        let graph = PoolGraph::new();

        // Add 5 pools (should keep only Top-3)
        graph.upsert_pool(make_edge(0x10, 0x01, 0x02, 100.0));
        graph.upsert_pool(make_edge(0x11, 0x01, 0x02, 500.0));
        graph.upsert_pool(make_edge(0x12, 0x01, 0x02, 200.0));
        graph.upsert_pool(make_edge(0x13, 0x01, 0x02, 400.0));
        graph.upsert_pool(make_edge(0x14, 0x01, 0x02, 300.0));

        let pools = graph.pools_between(&test_pubkey(0x01), &test_pubkey(0x02));
        assert_eq!(pools.len(), MAX_POOLS_PER_PAIR);

        // Should have 500, 400, 300 (top 3 by liquidity)
        let liq: Vec<f64> = pools.iter().map(|p| p.liquidity_usd).collect();
        assert!(liq.contains(&500.0));
        assert!(liq.contains(&400.0));
        assert!(liq.contains(&300.0));
        assert!(!liq.contains(&200.0));
        assert!(!liq.contains(&100.0));
    }

    #[test]
    fn test_remove_pool() {
        let graph = PoolGraph::new();

        graph.upsert_pool(make_edge(0x10, 0x01, 0x02, 1000.0));
        graph.upsert_pool(make_edge(0x11, 0x01, 0x02, 2000.0));

        assert_eq!(graph.pools_between(&test_pubkey(0x01), &test_pubkey(0x02)).len(), 2);

        graph.remove_pool(&test_pubkey(0x10));

        let pools = graph.pools_between(&test_pubkey(0x01), &test_pubkey(0x02));
        assert_eq!(pools.len(), 1);
        assert_eq!(pools[0].pool_address, test_pubkey(0x11));
    }

    #[test]
    fn test_graph_with_triangle() {
        let graph = PoolGraph::new();
        let wsol = test_pubkey(0x01);
        let usdc = test_pubkey(0x02);
        let sol = test_pubkey(0x03);

        // Create a triangle: WSOL <-> USDC <-> SOL <-> WSOL
        graph.upsert_pool(make_edge(0x10, 0x01, 0x02, 1000.0)); // WSOL-USDC
        graph.upsert_pool(make_edge(0x11, 0x02, 0x03, 2000.0)); // USDC-SOL
        graph.upsert_pool(make_edge(0x12, 0x03, 0x01, 1500.0)); // SOL-WSOL

        let stats = graph.stats();
        assert_eq!(stats.total_vertices, 3);
        assert_eq!(stats.total_edges, 3);
        assert_eq!(stats.total_pools, 3);

        // Each vertex should have 2 neighbors
        assert_eq!(graph.degree(&wsol), 2);
        assert_eq!(graph.degree(&usdc), 2);
        assert_eq!(graph.degree(&sol), 2);
    }

    #[test]
    fn test_bulk_upsert() {
        let graph = PoolGraph::new();

        let edges = vec![
            make_edge(0x10, 0x01, 0x02, 1000.0),
            make_edge(0x11, 0x02, 0x03, 2000.0),
            make_edge(0x12, 0x03, 0x01, 1500.0),
        ];

        graph.bulk_upsert(edges);

        let stats = graph.stats();
        assert_eq!(stats.total_pools, 3);
        assert_eq!(stats.total_vertices, 3);
    }
}
