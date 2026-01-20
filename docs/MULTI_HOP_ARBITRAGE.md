# Multi-Hop Arbitrage Implementation

**Status**: Planning  
**Created**: 2026-01-21  
**Priority**: Medium (nach WsolManager/Janitor stable)

## Motivation

Aktuell: 2-Hop Arbitrage (WSOL → Token → WSOL auf verschiedenen DEXes)

Neu: N-Hop Arbitrage (Triangular/Circular Arbitrage)
- WSOL → A → B → WSOL
- WSOL → A → B → C → WSOL
- Mehr Opportunities, weniger Konkurrenz (komplexere Pfade)

**Vorteil gegenüber Jupiter:**
- Wir haben Echtzeit-Geyser-Data
- Jupiter Quote-API hat ~100-500ms Latenz
- Wir sehen Pool-Imbalances bevor Jupiter sie reflektiert

## Architektur-Entscheidung: Pool-Graph Location

### Option A: market-data (NICHT GEWÄHLT)

```
market-data
├── Geyser Pool Updates
├── Pool-Graph Builder ← NEU
└── Publishes: MarketEvents + GraphUpdates (NATS)

arb-strategy
├── Konsumiert GraphUpdates
└── Cycle Detection
```

**Nachteile:**
- Zusätzlicher NATS Event-Typ (GraphUpdate)
- market-data wird komplexer (Strategy-Logik)
- Latenz: Graph-Update → Serialize → NATS → Deserialize

### Option B: arb-strategy (GEWÄHLT ✅)

```
market-data
├── Geyser Pool Updates
└── Publishes: MarketEvents (unverändert)

arb-strategy
├── Konsumiert MarketEvents
├── Pool-Graph Builder ← NEU (lokal)
├── Cycle Detection ← NEU
└── Publishes: TradeIntent (mit swap_path)

execution-engine
├── LivePoolCache (aktuelle Quotes)
├── Multi-Hop Quote Validation ← NEU
└── Multi-Hop TX Builder ← NEU
```

**Vorteile:**
- Keine zusätzliche NATS-Latenz
- market-data bleibt schlank (nur Daten-Normalisierung)
- Graph-Struktur kann für Cycle-Detection optimiert werden
- Separation of Concerns: Strategy-Logik in Strategy-Binary

**Begründung:**
- Graph = Topologie (welche Pools verbinden welche Tokens)
- Topologie ändert sich selten (neuer Pool = seltenes Event)
- Cycle-Detection ist Strategy-Logik, nicht Daten-Layer
- execution-engine hat LivePoolCache für aktuelle Quotes

---

## Implementation Plan

### Phase 1: Pool-Graph in arb-strategy

#### 1.1 Pool-Graph Struktur

```rust
// src/bin/arb_strategy.rs oder src/arbitrage/pool_graph.rs

use std::collections::HashMap;
use solana_sdk::pubkey::Pubkey;

/// Kante im Pool-Graph (ein Pool = eine Kante zwischen zwei Mints)
#[derive(Debug, Clone)]
pub struct PoolEdge {
    pub pool_address: Pubkey,
    pub dex: DexType,
    pub mint_a: Pubkey,
    pub mint_b: Pubkey,
    /// Liquidity in USD (für Routing-Priorität)
    pub liquidity_usd: f64,
    /// Fee in basis points
    pub fee_bps: u16,
    /// Last update timestamp
    pub updated_at: u64,
}

/// Pool-Graph: Mint → Mint → Vec<Pools>
/// Mehrere Pools können das gleiche Paar handeln (verschiedene DEXes)
pub struct PoolGraph {
    /// Adjacency list: mint_a → mint_b → pools
    edges: HashMap<Pubkey, HashMap<Pubkey, Vec<PoolEdge>>>,
    /// Reverse lookup: pool_address → edge index
    pool_index: HashMap<Pubkey, (Pubkey, Pubkey)>,
    /// Total number of edges
    edge_count: usize,
}

impl PoolGraph {
    pub fn new() -> Self {
        Self {
            edges: HashMap::new(),
            pool_index: HashMap::new(),
            edge_count: 0,
        }
    }

    /// Add or update a pool edge
    pub fn upsert_pool(&mut self, edge: PoolEdge) {
        let (mint_a, mint_b) = if edge.mint_a < edge.mint_b {
            (edge.mint_a, edge.mint_b)
        } else {
            (edge.mint_b, edge.mint_a)
        };

        // Add edge in both directions (undirected graph)
        self.add_directed_edge(mint_a, mint_b, edge.clone());
        self.add_directed_edge(mint_b, mint_a, edge.clone());

        self.pool_index.insert(edge.pool_address, (mint_a, mint_b));
    }

    fn add_directed_edge(&mut self, from: Pubkey, to: Pubkey, edge: PoolEdge) {
        let neighbors = self.edges.entry(from).or_default();
        let pools = neighbors.entry(to).or_default();

        // Update existing or insert new
        if let Some(existing) = pools.iter_mut().find(|p| p.pool_address == edge.pool_address) {
            *existing = edge;
        } else {
            pools.push(edge);
            self.edge_count += 1;
        }
    }

    /// Remove a pool (when it becomes inactive)
    pub fn remove_pool(&mut self, pool_address: &Pubkey) {
        if let Some((mint_a, mint_b)) = self.pool_index.remove(pool_address) {
            self.remove_directed_edge(&mint_a, &mint_b, pool_address);
            self.remove_directed_edge(&mint_b, &mint_a, pool_address);
        }
    }

    fn remove_directed_edge(&mut self, from: &Pubkey, to: &Pubkey, pool: &Pubkey) {
        if let Some(neighbors) = self.edges.get_mut(from) {
            if let Some(pools) = neighbors.get_mut(to) {
                pools.retain(|p| p.pool_address != *pool);
                self.edge_count = self.edge_count.saturating_sub(1);
            }
        }
    }

    /// Get all neighbors of a mint
    pub fn neighbors(&self, mint: &Pubkey) -> impl Iterator<Item = (&Pubkey, &Vec<PoolEdge>)> {
        self.edges.get(mint).into_iter().flat_map(|m| m.iter())
    }

    /// Get pools connecting two mints
    pub fn get_pools(&self, mint_a: &Pubkey, mint_b: &Pubkey) -> Option<&Vec<PoolEdge>> {
        self.edges.get(mint_a).and_then(|m| m.get(mint_b))
    }

    /// Get all mints in the graph
    pub fn all_mints(&self) -> impl Iterator<Item = &Pubkey> {
        self.edges.keys()
    }

    /// Statistics
    pub fn stats(&self) -> GraphStats {
        GraphStats {
            mint_count: self.edges.len(),
            edge_count: self.edge_count,
            pool_count: self.pool_index.len(),
        }
    }
}

#[derive(Debug)]
pub struct GraphStats {
    pub mint_count: usize,
    pub edge_count: usize,
    pub pool_count: usize,
}
```

#### 1.2 Graph Update Handler

```rust
impl ArbStrategy {
    /// Handle MarketEvent and update pool graph
    fn handle_market_event(&mut self, event: &MarketEvent) {
        match event {
            MarketEvent::PoolUpdate(update) => {
                // Convert to PoolEdge and update graph
                let edge = PoolEdge {
                    pool_address: update.pool_address.parse().unwrap(),
                    dex: update.dex.clone().into(),
                    mint_a: update.mint_a.parse().unwrap(),
                    mint_b: update.mint_b.parse().unwrap(),
                    liquidity_usd: update.liquidity_usd,
                    fee_bps: update.fee_bps,
                    updated_at: update.timestamp,
                };
                self.pool_graph.upsert_pool(edge);
            }
            MarketEvent::PoolRemoved(addr) => {
                if let Ok(pubkey) = addr.parse() {
                    self.pool_graph.remove_pool(&pubkey);
                }
            }
            _ => {}
        }
    }
}
```

---

### Phase 2: Cycle Detection (Bellman-Ford)

#### 2.1 Algorithmus-Wahl

| Algorithmus | Komplexität | Vorteile | Nachteile |
|-------------|-------------|----------|-----------|
| **Bellman-Ford** | O(V*E) | Findet negative Cycles | Langsam bei großen Graphen |
| **SPFA** | O(V*E) avg O(k*E) | Schneller als BF in Practice | Worst-case wie BF |
| **DFS + Pruning** | O(V+E) per start | Schnell für begrenzte Tiefe | Findet nicht alle Cycles |

**Empfehlung: DFS mit Tiefenlimit (max 4 Hops)**

Für Arbitrage brauchen wir:
- Cycles die bei WSOL starten und enden
- Maximale Tiefe begrenzen (CU-Budget)
- Nur profitable Cycles (positive Return)

#### 2.2 Cycle Finder

```rust
/// Ein gefundener Arbitrage-Cycle
#[derive(Debug, Clone)]
pub struct ArbCycle {
    /// Pfad: [WSOL, Token_A, Token_B, ..., WSOL]
    pub path: Vec<Pubkey>,
    /// Pools für jeden Hop
    pub pools: Vec<PoolEdge>,
    /// Geschätzter Return (vor Fees/Slippage)
    pub estimated_return_bps: i32,
}

/// Cycle Finder für Multi-Hop Arbitrage
pub struct CycleFinder {
    /// Maximum hops (2 = current behavior, 3-4 = multi-hop)
    max_hops: usize,
    /// Minimum liquidity per pool (USD)
    min_liquidity_usd: f64,
    /// Start/End token (WSOL)
    base_mint: Pubkey,
}

impl CycleFinder {
    pub fn new(max_hops: usize, min_liquidity_usd: f64) -> Self {
        Self {
            max_hops,
            min_liquidity_usd,
            base_mint: spl_token::native_mint::id(),
        }
    }

    /// Find all cycles starting and ending at WSOL
    pub fn find_cycles(&self, graph: &PoolGraph, price_cache: &PriceCache) -> Vec<ArbCycle> {
        let mut cycles = Vec::new();
        let mut visited = HashSet::new();
        let mut path = vec![self.base_mint];
        let mut pools = Vec::new();

        self.dfs(
            graph,
            price_cache,
            &self.base_mint,
            &mut visited,
            &mut path,
            &mut pools,
            &mut cycles,
            1.0, // Starting with 1.0 units
        );

        // Sort by estimated return (descending)
        cycles.sort_by(|a, b| b.estimated_return_bps.cmp(&a.estimated_return_bps));
        cycles
    }

    fn dfs(
        &self,
        graph: &PoolGraph,
        price_cache: &PriceCache,
        current: &Pubkey,
        visited: &mut HashSet<Pubkey>,
        path: &mut Vec<Pubkey>,
        pools: &mut Vec<PoolEdge>,
        cycles: &mut Vec<ArbCycle>,
        current_amount: f64,
    ) {
        // Check if we've returned to WSOL (cycle complete)
        if path.len() > 2 && *current == self.base_mint {
            let return_bps = ((current_amount - 1.0) * 10000.0) as i32;
            if return_bps > 0 {
                cycles.push(ArbCycle {
                    path: path.clone(),
                    pools: pools.clone(),
                    estimated_return_bps: return_bps,
                });
            }
            return;
        }

        // Max depth reached
        if path.len() > self.max_hops {
            return;
        }

        // Explore neighbors
        for (next_mint, pool_options) in graph.neighbors(current) {
            // Skip if already visited (except WSOL at the end)
            if visited.contains(next_mint) && *next_mint != self.base_mint {
                continue;
            }

            // Find best pool for this hop
            let best_pool = pool_options
                .iter()
                .filter(|p| p.liquidity_usd >= self.min_liquidity_usd)
                .max_by(|a, b| a.liquidity_usd.partial_cmp(&b.liquidity_usd).unwrap());

            if let Some(pool) = best_pool {
                // Estimate output (simplified - real implementation uses pool math)
                let fee_multiplier = 1.0 - (pool.fee_bps as f64 / 10000.0);
                let price = price_cache.get_price(current, next_mint).unwrap_or(1.0);
                let output = current_amount * price * fee_multiplier;

                visited.insert(*current);
                path.push(*next_mint);
                pools.push(pool.clone());

                self.dfs(graph, price_cache, next_mint, visited, path, pools, cycles, output);

                pools.pop();
                path.pop();
                visited.remove(current);
            }
        }
    }
}
```

---

### Phase 3: TradeIntent Schema Extension

#### 3.1 IPC Schema Update

```rust
// src/ipc/schema.rs

/// Extended TradeIntent for multi-hop
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeIntent {
    // ... existing fields ...

    /// Multi-hop swap path (None = legacy 2-hop)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub swap_path: Option<Vec<SwapHop>>,
}

/// Single hop in a multi-hop swap
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwapHop {
    /// Pool address
    pub pool_address: String,
    /// DEX type
    pub dex: String,
    /// Input mint for this hop
    pub input_mint: String,
    /// Output mint for this hop
    pub output_mint: String,
    /// Expected output amount (for slippage check)
    pub expected_output: u64,
}
```

#### 3.2 Intent Creation in arb-strategy

```rust
fn create_multi_hop_intent(cycle: &ArbCycle, input_amount: u64) -> TradeIntent {
    let hops: Vec<SwapHop> = cycle.pools.iter().enumerate().map(|(i, pool)| {
        SwapHop {
            pool_address: pool.pool_address.to_string(),
            dex: pool.dex.to_string(),
            input_mint: cycle.path[i].to_string(),
            output_mint: cycle.path[i + 1].to_string(),
            expected_output: 0, // Calculated by execution-engine
        }
    }).collect();

    TradeIntent {
        intent_id: generate_intent_id(),
        source: "arb-strategy".to_string(),
        intent_type: "multi_hop_arb".to_string(),
        input_mint: cycle.path[0].to_string(),
        output_mint: cycle.path[0].to_string(), // Same (WSOL → WSOL)
        input_amount,
        swap_path: Some(hops),
        // ... other fields ...
    }
}
```

---

### Phase 4: execution-engine Multi-Hop Support

#### 4.1 Multi-Hop Quote

```rust
// src/execution/quote_calculator.rs

impl QuoteCalculator {
    /// Quote a multi-hop path
    pub async fn quote_multi_hop(
        &self,
        path: &[SwapHop],
        input_amount: u64,
    ) -> Result<MultiHopQuote> {
        let mut current_amount = input_amount;
        let mut quotes = Vec::with_capacity(path.len());

        for hop in path {
            let pool = self.pool_cache.get_pool(&hop.pool_address)?;
            let quote = pool.quote_exact_in(current_amount)?;

            current_amount = quote.output_amount;
            quotes.push(HopQuote {
                input_amount: quote.input_amount,
                output_amount: quote.output_amount,
                price_impact_bps: quote.price_impact_bps,
                fee_amount: quote.fee_amount,
            });
        }

        Ok(MultiHopQuote {
            input_amount,
            output_amount: current_amount,
            hops: quotes,
            total_price_impact_bps: quotes.iter().map(|q| q.price_impact_bps).sum(),
        })
    }
}
```

#### 4.2 Multi-Hop TX Builder

```rust
// src/solana/cross_dex_handler.rs

impl CrossDexHandler {
    /// Build multi-hop swap transaction
    pub fn build_multi_hop_swap(
        &self,
        path: &[SwapHop],
        input_amount: u64,
        min_output: u64,
        wallet: &Pubkey,
    ) -> Result<Vec<Instruction>> {
        let mut instructions = Vec::new();

        // 1. Create intermediate ATAs if needed
        for hop in path {
            if hop.output_mint != WSOL_MINT.to_string() {
                let ata_ix = self.maybe_create_ata(wallet, &hop.output_mint)?;
                if let Some(ix) = ata_ix {
                    instructions.push(ix);
                }
            }
        }

        // 2. Build swap instructions for each hop
        let mut current_amount = input_amount;
        for (i, hop) in path.iter().enumerate() {
            let is_last = i == path.len() - 1;
            let min_out = if is_last { min_output } else { 1 }; // Only enforce on last hop

            let swap_ix = self.build_swap_ix(
                &hop.pool_address,
                &hop.dex,
                &hop.input_mint,
                &hop.output_mint,
                current_amount,
                min_out,
            )?;

            instructions.extend(swap_ix);

            // Update current_amount for next hop (from quote)
            current_amount = hop.expected_output;
        }

        Ok(instructions)
    }
}
```

---

## CU Budget Analysis

| Hops | Estimated CU | Feasibility |
|------|--------------|-------------|
| 2 (current) | ~250-300k | ✅ Comfortable |
| 3 | ~350-400k | ✅ OK |
| 4 | ~450-500k | ⚠️ Near limit |
| 5+ | >550k | ❌ Risky |

**Recommendation: Max 4 hops**

CU Breakdown per Hop:
- ATA Creation (if needed): ~20k
- Raydium Swap: ~80-100k
- Orca Swap: ~100-120k
- Meteora Swap: ~80-100k

---

## Slippage Considerations

Multi-hop slippage akkumuliert:
```
Total Slippage ≈ (1 - slippage_per_hop)^n - 1

Example (0.5% per hop):
- 2 hops: 1.0%
- 3 hops: 1.5%
- 4 hops: 2.0%
```

**Mitigation:**
1. Höhere min_output Anforderung für mehr Hops
2. Nur Pools mit hoher Liquidität
3. Dynamic Slippage basierend auf Hop-Count

---

## Implementation Checklist

### Phase 1: Pool-Graph (arb-strategy)
- [ ] `PoolGraph` struct implementieren
- [ ] `PoolEdge` struct implementieren
- [ ] Graph update handler für MarketEvents
- [ ] Unit tests für Graph operations

### Phase 2: Cycle Detection
- [ ] `CycleFinder` struct implementieren
- [ ] DFS mit Tiefenlimit
- [ ] Profit-Estimation (vor Fees)
- [ ] Unit tests für Cycle Detection

### Phase 3: TradeIntent Extension
- [ ] `SwapHop` struct zu IPC Schema
- [ ] `swap_path` field zu TradeIntent
- [ ] Backward-compatible (None = legacy)
- [ ] arb-strategy Intent creation

### Phase 4: execution-engine
- [ ] `quote_multi_hop()` in QuoteCalculator
- [ ] `build_multi_hop_swap()` in CrossDexHandler
- [ ] Handle `swap_path` in Intent processing
- [ ] Slippage handling für multi-hop

### Phase 5: Testing
- [ ] Unit tests für alle neuen Komponenten
- [ ] Integration test: 3-hop cycle
- [ ] Dry-run auf Server
- [ ] CU Measurement für verschiedene Hop-Counts

---

## Rollout Plan

1. **Dev**: Implement + Unit Tests
2. **Staging**: Enable with `max_hops = 3`, dry_run
3. **Prod Phase 1**: Enable 3-hop mit conservative Slippage
4. **Prod Phase 2**: Enable 4-hop nach Monitoring

---

## Open Questions

1. **Price Cache für Cycle-Finder**: Eigener Cache oder LivePoolCache teilen?
   - Tendenz: Eigener vereinfachter Cache (nur für Estimation)

2. **Parallel Cycle Detection**: Mehrere Cycles gleichzeitig finden?
   - Tendenz: Ja, aber nur besten Intent emittieren (Capital Lock)

3. **Dynamic Max Hops**: Basierend auf CU-Budget automatisch?
   - Tendenz: Statisch konfigurierbar erstmal

---

## Dependencies

Keine neuen crates benötigt. Nutzt:
- `std::collections::HashMap` für Graph
- Existing Pool/DEX abstractions
- Existing IPC/NATS infrastructure
