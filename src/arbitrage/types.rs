//! Multi-Hop Arbitrage Types
//!
//! Shared type definitions for the multi-hop arbitrage system.

use solana_sdk::pubkey::Pubkey;
use std::collections::HashSet;
use std::str::FromStr;

/// Error when parsing DexType from string
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseDexTypeError(String);

impl std::fmt::Display for ParseDexTypeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "unknown DEX type: {}", self.0)
    }
}

impl std::error::Error for ParseDexTypeError {}

/// Supported DEX types for arbitrage routing
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DexType {
    RaydiumAmmV4,
    RaydiumCpmm,
    Orca,
    MeteoraDlmm,
    MeteoraCpmm,
    PumpSwapAmm,
}

impl FromStr for DexType {
    type Err = ParseDexTypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "raydium" | "raydium_amm" | "raydium_amm_v4" => Ok(Self::RaydiumAmmV4),
            "raydium_cpmm" => Ok(Self::RaydiumCpmm),
            "orca" | "orca_whirlpool" => Ok(Self::Orca),
            "meteora_dlmm" | "meteora" => Ok(Self::MeteoraDlmm),
            "meteora_cpmm" => Ok(Self::MeteoraCpmm),
            "pump_amm" | "pumpswap" | "pump_swap_amm" => Ok(Self::PumpSwapAmm),
            _ => Err(ParseDexTypeError(s.to_string())),
        }
    }
}

impl DexType {
    /// Convert to string for serialization
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::RaydiumAmmV4 => "raydium",
            Self::RaydiumCpmm => "raydium_cpmm",
            Self::Orca => "orca",
            Self::MeteoraDlmm => "meteora_dlmm",
            Self::MeteoraCpmm => "meteora_cpmm",
            Self::PumpSwapAmm => "pump_amm",
        }
    }
}

impl std::fmt::Display for DexType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Edge in the pool graph (a pool = an edge between two mints)
///
/// Note: This is an UNDIRECTED edge. The `mint_a`/`mint_b` ordering
/// is canonical (mint_a < mint_b) but does NOT indicate swap direction.
/// Swap direction is determined by the search algorithm context.
#[derive(Debug, Clone)]
pub struct PoolEdge {
    pub pool_address: Pubkey,
    pub dex: DexType,
    /// Canonical mint A (mint_a < mint_b for consistent hashing)
    pub mint_a: Pubkey,
    /// Canonical mint B
    pub mint_b: Pubkey,
    /// Liquidity in USD (for routing priority)
    pub liquidity_usd: f64,
    /// Fee in basis points
    pub fee_bps: u16,
    /// Last update timestamp (unix millis)
    pub updated_at: u64,
}

impl PoolEdge {
    /// Create new edge with canonical mint ordering
    pub fn new(
        pool_address: Pubkey,
        dex: DexType,
        mint_a: Pubkey,
        mint_b: Pubkey,
        liquidity_usd: f64,
        fee_bps: u16,
    ) -> Self {
        // Ensure canonical ordering
        let (mint_a, mint_b) = if mint_a < mint_b {
            (mint_a, mint_b)
        } else {
            (mint_b, mint_a)
        };

        Self {
            pool_address,
            dex,
            mint_a,
            mint_b,
            liquidity_usd,
            fee_bps,
            updated_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
        }
    }

    /// Get the other mint given one mint of the pair
    pub fn other_mint(&self, mint: &Pubkey) -> Option<Pubkey> {
        if *mint == self.mint_a {
            Some(self.mint_b)
        } else if *mint == self.mint_b {
            Some(self.mint_a)
        } else {
            None
        }
    }
}

/// Ranked pool with pre-computed metrics for efficient search
#[derive(Debug, Clone)]
pub struct RankedPool {
    pub edge: PoolEdge,
    /// Pre-computed: output/input ratio based on PROBE QUOTE (not spot price!)
    /// This accounts for fees and curve shape at the probe amount.
    pub edge_ratio: f64,
    /// Dampened liquidity score (clamped, not sqrt)
    pub liquidity_score: f64,
}

impl RankedPool {
    /// Combined score for ranking (profit × liquidity_factor)
    pub fn combined_score(&self) -> f64 {
        self.edge_ratio * self.liquidity_score
    }
}

/// Maximum profit multiplier during beam search (10× = +900% raw product).
pub const MAX_CYCLE_PROFIT_MULTIPLIER: f64 = 10.0;

/// Per-hop edge ratio bounds (probe quote output/input). Rejects pathological quotes.
pub const MIN_EDGE_RATIO: f64 = 0.01;
pub const MAX_EDGE_RATIO: f64 = 1.05;

/// `estimated_return_bps` clamp: −100% .. +500% (interpretable shadow/live signal).
pub const MIN_RETURN_BPS: i32 = -10_000;
pub const MAX_RETURN_BPS: i32 = 50_000;

/// Clamp per-hop edge ratio before multiplying into cumulative profit.
#[inline]
pub fn clamp_edge_ratio(edge_ratio: f64) -> f64 {
    edge_ratio.clamp(MIN_EDGE_RATIO, MAX_EDGE_RATIO)
}

/// Convert profit multiplier to basis points with saturating clamp.
/// Returns `(bps, saturated)` where `saturated` means the raw value was outside bounds.
#[inline]
pub fn profit_to_return_bps(profit: f64) -> (i32, bool) {
    let raw = (profit - 1.0) * 10_000.0;
    if !raw.is_finite() {
        return (MAX_RETURN_BPS, true);
    }
    let saturated = raw < f64::from(MIN_RETURN_BPS) || raw > f64::from(MAX_RETURN_BPS);
    let bps = raw
        .round()
        .clamp(f64::from(MIN_RETURN_BPS), f64::from(MAX_RETURN_BPS)) as i32;
    (bps, saturated)
}

/// A found arbitrage cycle with pool alternatives for fallback routing
#[derive(Debug, Clone)]
pub struct ArbCycle {
    /// Path: [WSOL, Token_A, Token_B, ..., WSOL]
    pub path: Vec<Pubkey>,
    /// Pools for each hop - WITH ALTERNATIVES for execution fallbacks!
    /// pools[hop_idx][0] = Best Pool, pools[hop_idx][1..] = Fallbacks
    pub pools: Vec<Vec<PoolEdge>>,
    /// Estimated return in basis points (before slippage), clamped to [`MIN_RETURN_BPS`, `MAX_RETURN_BPS`]
    pub estimated_return_bps: i32,
    /// Minimum liquidity in the path (USD)
    pub min_liquidity_usd: f64,
    /// True when cumulative profit hit [`MAX_CYCLE_PROFIT_MULTIPLIER`] during search.
    pub profit_multiplier_capped: bool,
    /// True when `estimated_return_bps` required clamping (or profit cap implies untrustworthy ROI).
    pub return_bps_saturated: bool,
    /// True when any hop used an edge ratio outside [`MIN_EDGE_RATIO`, `MAX_EDGE_RATIO`].
    pub edge_ratio_clamped: bool,
}

impl ArbCycle {
    /// Profit estimate is safe to compare against `min_profit_bps` / shadow logging.
    pub fn is_trustworthy_profit_estimate(&self) -> bool {
        !self.return_bps_saturated && !self.profit_multiplier_capped
    }

    /// Number of hops in the cycle
    pub fn hop_count(&self) -> usize {
        self.pools.len()
    }

    /// Get primary pools (best option for each hop)
    pub fn primary_pools(&self) -> Vec<&PoolEdge> {
        self.pools.iter().filter_map(|alts| alts.first()).collect()
    }

    /// Check if cycle is valid (starts and ends at same token)
    pub fn is_valid(&self) -> bool {
        self.path.len() >= 3
            && self.path.first() == self.path.last()
            && self.pools.len() == self.path.len() - 1
    }
}

/// Search node for the priority queue in beam search
#[derive(Debug, Clone)]
pub struct SearchNode {
    /// Current token position
    pub token: Pubkey,
    /// Path taken so far: [start_token, token1, token2, ..., current_token]
    pub path: Vec<Pubkey>,
    /// Pools for each hop - with alternatives for fallback
    pub pools: Vec<Vec<PoolEdge>>,
    /// Current profit multiplier (1.0 = break-even)
    pub profit: f64,
    /// Score for prioritization (higher = better)
    pub score: f64,
    /// Depth (hop count)
    pub depth: usize,
    /// Minimum liquidity along the path
    pub min_liquidity: f64,
    /// Visited tokens for O(1) lookup (instead of O(n) path.contains)
    pub visited: HashSet<Pubkey>,
    /// Cumulative profit hit [`MAX_CYCLE_PROFIT_MULTIPLIER`] on at least one expand step.
    pub profit_multiplier_capped: bool,
    /// At least one hop used an edge ratio outside sane bounds (after clamp).
    pub edge_ratio_clamped: bool,
}

impl SearchNode {
    /// Create start node for search
    pub fn start(base_mint: Pubkey) -> Self {
        let mut visited = HashSet::new();
        visited.insert(base_mint);

        Self {
            token: base_mint,
            path: vec![base_mint],
            pools: vec![],
            profit: 1.0,
            score: 1.0,
            depth: 0,
            min_liquidity: f64::MAX,
            visited,
            profit_multiplier_capped: false,
            edge_ratio_clamped: false,
        }
    }

    /// Expand to a new node by taking a hop
    pub fn expand(
        &self,
        next_token: Pubkey,
        pool_alternatives: Vec<PoolEdge>,
        edge_ratio: f64,
        liquidity_score: f64,
        liquidity_usd: f64,
    ) -> Self {
        let mut new_path = self.path.clone();
        new_path.push(next_token);

        let mut new_pools = self.pools.clone();
        new_pools.push(pool_alternatives);

        let clamped_ratio = clamp_edge_ratio(edge_ratio);
        let edge_clamped = (clamped_ratio - edge_ratio).abs() > f64::EPSILON;
        let uncapped_profit = self.profit * clamped_ratio;
        let profit_hit_cap = uncapped_profit > MAX_CYCLE_PROFIT_MULTIPLIER;
        let new_profit = uncapped_profit.min(MAX_CYCLE_PROFIT_MULTIPLIER);
        let new_liquidity = self.min_liquidity.min(liquidity_usd);
        let new_score = new_profit * liquidity_score;

        let mut new_visited = self.visited.clone();
        new_visited.insert(next_token);

        Self {
            token: next_token,
            path: new_path,
            pools: new_pools,
            profit: new_profit,
            score: new_score,
            depth: self.depth + 1,
            min_liquidity: new_liquidity,
            visited: new_visited,
            profit_multiplier_capped: self.profit_multiplier_capped || profit_hit_cap,
            edge_ratio_clamped: self.edge_ratio_clamped || edge_clamped,
        }
    }

    /// Check if a token has been visited (O(1))
    pub fn has_visited(&self, token: &Pubkey) -> bool {
        self.visited.contains(token)
    }

    /// Convert completed cycle to ArbCycle
    pub fn to_arb_cycle(&self) -> Option<ArbCycle> {
        if self.path.len() < 3 || self.path.first() != self.path.last() {
            return None;
        }

        let (return_bps, return_saturated) = profit_to_return_bps(self.profit);
        let return_bps_saturated = return_saturated || self.profit_multiplier_capped;

        Some(ArbCycle {
            path: self.path.clone(),
            pools: self.pools.clone(),
            estimated_return_bps: return_bps,
            min_liquidity_usd: self.min_liquidity,
            profit_multiplier_capped: self.profit_multiplier_capped,
            return_bps_saturated,
            edge_ratio_clamped: self.edge_ratio_clamped,
        })
    }
}

// Implement ordering for BinaryHeap (max-heap by score)
impl PartialEq for SearchNode {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score
    }
}

impl Eq for SearchNode {}

impl PartialOrd for SearchNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SearchNode {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse for max-heap (higher score = higher priority)
        self.score
            .partial_cmp(&other.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_pubkey(byte: u8) -> Pubkey {
        Pubkey::new_from_array([byte; 32])
    }

    #[test]
    fn test_pool_edge_canonical_ordering() {
        let mint_high = test_pubkey(0xFF);
        let mint_low = test_pubkey(0x01);
        let pool = test_pubkey(0x42);

        // Even if we pass mints in wrong order, they should be canonicalized
        let edge = PoolEdge::new(pool, DexType::RaydiumAmmV4, mint_high, mint_low, 1000.0, 30);

        assert_eq!(edge.mint_a, mint_low);
        assert_eq!(edge.mint_b, mint_high);
    }

    #[test]
    fn test_pool_edge_other_mint() {
        let mint_a = test_pubkey(0x01);
        let mint_b = test_pubkey(0x02);
        let pool = test_pubkey(0x42);

        let edge = PoolEdge::new(pool, DexType::RaydiumAmmV4, mint_a, mint_b, 1000.0, 30);

        assert_eq!(edge.other_mint(&mint_a), Some(mint_b));
        assert_eq!(edge.other_mint(&mint_b), Some(mint_a));
        assert_eq!(edge.other_mint(&test_pubkey(0x99)), None);
    }

    #[test]
    fn test_search_node_expand() {
        let wsol = test_pubkey(0x01); // Use test pubkey instead of native_mint
        let token_a = test_pubkey(0x0A);

        let start = SearchNode::start(wsol);
        assert_eq!(start.depth, 0);
        assert!(start.has_visited(&wsol));
        assert!(!start.has_visited(&token_a));

        let edge = PoolEdge::new(
            test_pubkey(0x42),
            DexType::RaydiumAmmV4,
            wsol,
            token_a,
            5000.0,
            30,
        );

        let expanded = start.expand(token_a, vec![edge], 1.02, 1.0, 5000.0);

        assert_eq!(expanded.depth, 1);
        assert_eq!(expanded.profit, 1.02);
        assert!(expanded.has_visited(&wsol));
        assert!(expanded.has_visited(&token_a));
    }

    #[test]
    fn test_search_node_ordering() {
        let wsol = test_pubkey(0x01); // Use test pubkey instead of native_mint

        let mut node1 = SearchNode::start(wsol);
        node1.score = 1.5;

        let mut node2 = SearchNode::start(wsol);
        node2.score = 2.0;

        // Higher score should be "greater" for max-heap
        assert!(node2 > node1);
    }

    #[test]
    fn test_profit_overflow_never_returns_i32_max() {
        let wsol = test_pubkey(0x01);
        let mut node = SearchNode::start(wsol);
        // Simulate pathological cumulative profit (would overflow naive cast)
        node.profit = 1e12;
        node.path = vec![wsol, test_pubkey(0x02), wsol];
        node.pools = vec![vec![], vec![]];

        let cycle = node.to_arb_cycle().expect("valid closed path");
        assert_ne!(cycle.estimated_return_bps, i32::MAX);
        assert_eq!(cycle.estimated_return_bps, MAX_RETURN_BPS);
        assert!(cycle.return_bps_saturated);
        assert!(!cycle.is_trustworthy_profit_estimate());
    }

    #[test]
    fn test_extreme_edge_ratio_clamped() {
        let wsol = test_pubkey(0x01);
        let token_a = test_pubkey(0x0A);
        let start = SearchNode::start(wsol);
        let edge = PoolEdge::new(
            test_pubkey(0x42),
            DexType::RaydiumAmmV4,
            wsol,
            token_a,
            5000.0,
            30,
        );
        let expanded = start.expand(token_a, vec![edge], 999.0, 1.0, 5000.0);
        assert!(expanded.edge_ratio_clamped);
        assert!((expanded.profit - 1.05).abs() < f64::EPSILON);
    }

    #[test]
    fn test_profit_multiplier_cap_after_many_hops() {
        let wsol = test_pubkey(0x01);
        let token = test_pubkey(0x0A);
        let edge = PoolEdge::new(
            test_pubkey(0x42),
            DexType::RaydiumAmmV4,
            wsol,
            token,
            5000.0,
            30,
        );
        let mut node = SearchNode::start(wsol);
        for _ in 0..80 {
            node = node.expand(token, vec![edge.clone()], 1.05, 1.0, 5000.0);
        }
        assert!(node.profit_multiplier_capped);
        assert!((node.profit - MAX_CYCLE_PROFIT_MULTIPLIER).abs() < f64::EPSILON);
    }

    #[test]
    fn test_dex_type_parsing() {
        assert_eq!(
            "raydium".parse::<DexType>().ok(),
            Some(DexType::RaydiumAmmV4)
        );
        assert_eq!(
            "RAYDIUM_CPMM".parse::<DexType>().ok(),
            Some(DexType::RaydiumCpmm)
        );
        assert_eq!("orca".parse::<DexType>().ok(), Some(DexType::Orca));
        assert_eq!(
            "meteora_dlmm".parse::<DexType>().ok(),
            Some(DexType::MeteoraDlmm)
        );
        assert!("unknown_dex".parse::<DexType>().is_err());
    }
}
