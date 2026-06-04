# Multi-Hop Arbitrage Implementation

**Status**: ❌ Not Started (Planning Only)  
**Created**: 2025-01-21  
**Updated**: 2025-01-23 (Review Feedback eingearbeitet)  
**Priority**: Medium (nach WsolManager/Janitor stable)

> ⚠️ **Hinweis**: Dieses Dokument beschreibt eine **geplante** Erweiterung.  
> Es existiert noch **kein Code** für Multi-Hop Arbitrage.  
> Aktuell läuft nur 2-Hop Arbitrage (WSOL → Token → WSOL).

---

## 🔍 Review Feedback (2025-01-23)

Das Design wurde von einem Senior-Reviewer analysiert. **Gesamturteil: Realistisch produktionsfähig** mit folgenden kritischen Anpassungen:

### ✅ Was bestätigt wurde
- Architektur (Pool-Graph in arb-strategy)
- Best-First Beam Search + Branch-and-Bound Ansatz
- CU-Budget Limits
- Overall Flow

### ⚠️ Kritische Fixes (eingearbeitet)

| Problem | Lösung | Status |
|---------|--------|--------|
| `edge_ratio` basiert auf Spot-Preis (ignoriert Slippage) | **Probe-based Quote** mit fixed amount | ✅ Fixed |
| Beam-Limit zählt "first K" statt "best K" | **Proper Top-K Selection** mit Score-Threshold | ✅ Fixed |
| `sqrt(liquidity)` überdominiert Scoring | **Gedämpftes Scoring** mit log() oder clamp() | ✅ Fixed |
| Nur 1 Pool pro Pair (fragil) | **Pool-Fallbacks** (Top-3) im Intent | ✅ Fixed |

### 📝 Weitere Empfehlungen (nice-to-have)
- `node.path.contains()` O(n) → `visited: HashSet` für Performance
- Early `best_profit` seeding aus 2-Hop Scan
- `edge_count` Tracking korrigieren (Metrics)

### Profit / `return_bps` Sanity (Shadow-Mode, 2026-06)

Shadow-Mode loggt Zyklen zur Prod-Validierung. Ohne Caps explodieren kumulative `edge_ratio`-Produkte und `estimated_return_bps` kann `i32::MAX` werden (kein interpretierbares Signal).

| Konstante | Wert | Semantik |
|-----------|------|----------|
| `MIN_EDGE_RATIO` / `MAX_EDGE_RATIO` | 0.01 / 1.05 | Per-Hop Clamp auf Probe-Quote |
| `MAX_CYCLE_PROFIT_MULTIPLIER` | 10.0 | Cumulative profit cap während Beam-Search |
| `MIN_RETURN_BPS` / `MAX_RETURN_BPS` | −10_000 / 50_000 | −100% .. +500% ROI-Schätzung |
| `is_trustworthy_profit_estimate()` | — | `false` bei Cap/Saturierung → nicht „profitable“, kein Intent |

Prometheus (arb-strategy :9803): `multi_hop_return_bps_saturated_total`, `multi_hop_cycle_rejected_sanity_total{reason=edge_ratio|profit_cap|return_bps_cap}`, `multi_hop_hop_missing_quote_total`, `multi_hop_shadow_logged_total`.

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

### Phase 2: Cycle Detection (Best-First Beam Search + Branch-and-Bound)

#### 2.1 Algorithmus-Wahl

| Algorithmus | Komplexität | Vorteile | Nachteile |
|-------------|-------------|----------|----------|
| **Bellman-Ford** | O(V*E) | Findet negative Cycles | ❌ Langsam, "blind" - relaxiert alle Kanten |
| **SPFA** | O(V*E) avg O(k*E) | Schneller als BF in Practice | ❌ Worst-case wie BF |
| **DFS + Pruning** | O(V+E) per start | Schnell für begrenzte Tiefe | ❌ Keine Score-basierte Priorisierung |
| **A*** | O(E + V log V) | Optimal mit Heuristik | ❌ Admissible Heuristik für Cycles schwer |
| **Best-First Beam Search** | O(K × D × B) | Score-basiert, Memory-bounded | ⚠️ Kann Optimum verpassen bei kleinem K |

**Empfehlung: Best-First Beam Search + Branch-and-Bound ✅**

Warum:
- **Best-First**: Expandiert vielversprechendste Pfade zuerst (Priority Queue nach Score)
- **Beam Limit**: Verhindert kombinatorische Explosion (nur Top K pro Tiefenlevel)
- **Branch-and-Bound**: Pruned Pfade die mathematisch nicht gewinnen können (Upper Bound)
- **Depth Constraint**: Max 4 Hops (CU-Budget)

> ⚠️ **Terminologie-Hinweis**: "Priority Queue DFS" ist semantisch falsch!
> - DFS = Stack (LIFO) → Tiefe zuerst
> - BFS = Queue (FIFO) → Breite zuerst  
> - **Best-First = Priority Queue** → Bester Score zuerst

Für Arbitrage brauchen wir:
- Cycles die bei WSOL starten und enden
- Maximale Tiefe begrenzen (CU-Budget)
- Nur profitable Cycles (positive Return)
- Schnelle Eliminierung unprofitabler Pfade

#### 2.2 Pre-Compute Phase (Pool Ranking & Upper Bounds)

Vor der Suche werden statische Daten berechnet (alle ~30s refreshen):

```rust
/// Pre-computed data für effiziente Cycle-Suche
pub struct PoolRanker {
    /// Top K Pools pro Token-Paar (nach Score sortiert)
    /// ⚠️ WICHTIG: Top-3 statt nur Top-1 für Fallback-Support
    top_pools: HashMap<(Pubkey, Pubkey), Vec<RankedPool>>,
    
    /// Max Edge Ratio pro Token (für Upper Bound Pruning)
    /// = bestes erreichbares Profit-Verhältnis von diesem Token aus
    max_edge_ratio: HashMap<Pubkey, f64>,
    
    /// Blacklisted Tokens (bekannte Rugs, < $1k Liquidity insgesamt)
    blacklist: HashSet<Pubkey>,
    
    /// Probe amount für Quote-basierte edge_ratio Berechnung
    /// Fixed amount (z.B. $100-$500 equivalent) statt Spot-Preis
    probe_amount_lamports: u64,
}

/// Konfiguration für Liquidity-Scoring
const LIQUIDITY_BASELINE: f64 = 10_000.0; // $10k als Referenz
const LIQUIDITY_CLAMP_MIN: f64 = 0.3;     // Minimum multiplier
const LIQUIDITY_CLAMP_MAX: f64 = 1.5;     // Maximum multiplier

#[derive(Debug, Clone)]
pub struct RankedPool {
    pub edge: PoolEdge,
    /// Pre-computed: output/input ratio basierend auf PROBE QUOTE (nicht Spot!)
    /// ⚠️ KRITISCH: Muss quote_out(probe_amount) / probe_amount sein
    pub edge_ratio: f64,
    /// Gedämpfter Liquidity-Score (log-basiert oder clamped)
    pub liquidity_score: f64,
}

impl PoolRanker {
    pub fn new(probe_amount_lamports: u64) -> Self {
        Self {
            top_pools: HashMap::new(),
            max_edge_ratio: HashMap::new(),
            blacklist: HashSet::new(),
            probe_amount_lamports,
        }
    }

    /// Refresh rankings (call every ~30s)
    /// 
    /// ⚠️ WICHTIG: edge_ratio MUSS quote-basiert sein, nicht Spot-Preis!
    /// Spot-Preis ignoriert: Trade Size, Curve Shape, Directional Imbalance
    pub fn refresh(&mut self, graph: &PoolGraph, pool_cache: &LivePoolCache) {
        self.top_pools.clear();
        self.max_edge_ratio.clear();
        
        // 1. Rank pools per token pair
        for (mint_a, neighbors) in graph.edges.iter() {
            for (mint_b, pools) in neighbors.iter() {
                let mut ranked: Vec<RankedPool> = pools
                    .iter()
                    .filter(|p| p.liquidity_usd >= 1000.0) // Min $1k
                    .filter(|p| !self.blacklist.contains(&p.mint_a) 
                             && !self.blacklist.contains(&p.mint_b))
                    .filter_map(|p| {
                        // ═══════════════════════════════════════════════════
                        // KRITISCH: Quote-basierte edge_ratio (nicht Spot!)
                        // ═══════════════════════════════════════════════════
                        let quote_result = pool_cache
                            .get_pool(&p.pool_address)
                            .and_then(|pool| pool.quote_exact_in(self.probe_amount_lamports).ok());
                        
                        let edge_ratio = match quote_result {
                            Some(quote) => quote.output_amount as f64 / self.probe_amount_lamports as f64,
                            None => return None, // Skip pools ohne valides Quote
                        };
                        
                        // ═══════════════════════════════════════════════════
                        // KORRIGIERT: Gedämpftes Liquidity-Scoring
                        // sqrt() war zu dominant → log() oder clamp()
                        // ═══════════════════════════════════════════════════
                        // Option A: Log-basiert
                        // let liquidity_score = (1.0 + p.liquidity_usd).ln();
                        
                        // Option B: Clamped (bevorzugt - vorhersagbarer)
                        let liquidity_factor = (p.liquidity_usd / LIQUIDITY_BASELINE)
                            .clamp(LIQUIDITY_CLAMP_MIN, LIQUIDITY_CLAMP_MAX);
                        let liquidity_score = liquidity_factor;
                        
                        Some(RankedPool {
                            edge: p.clone(),
                            edge_ratio,
                            liquidity_score,
                        })
                    })
                    .collect();
                
                // Sort by combined score (profit × liquidity_factor)
                ranked.sort_by(|a, b| {
                    let score_a = a.edge_ratio * a.liquidity_score;
                    let score_b = b.edge_ratio * b.liquidity_score;
                    score_b.partial_cmp(&score_a).unwrap()
                });
                
                // ═══════════════════════════════════════════════════
                // KORRIGIERT: Top-3 statt Top-1 für Fallback-Support
                // ═══════════════════════════════════════════════════
                ranked.truncate(3); // Top 3 pro Paar (für Execution Fallbacks)
                
                if !ranked.is_empty() {
                    self.top_pools.insert((*mint_a, *mint_b), ranked);
                }
            }
        }
        
        // 2. Compute max_edge_ratio per token
        for ((mint_a, _mint_b), pools) in &self.top_pools {
            if let Some(best) = pools.first() {
                let current = self.max_edge_ratio.entry(*mint_a).or_insert(0.0);
                *current = current.max(best.edge_ratio);
            }
        }
    }
    
    /// Get pre-ranked pools for a token pair (Top-3 für Fallbacks)
    pub fn get_top_pools(&self, from: &Pubkey, to: &Pubkey) -> Option<&Vec<RankedPool>> {
        self.top_pools.get(&(*from, *to))
    }
    
    /// Get max possible edge ratio from this token (for upper bound)
    pub fn max_ratio(&self, token: &Pubkey) -> f64 {
        *self.max_edge_ratio.get(token).unwrap_or(&1.0)
    }
}
```

#### 2.3 Best-First Beam Search mit Branch-and-Bound

```rust
use std::collections::{BinaryHeap, HashSet, HashMap};
use std::cmp::Ordering;

/// Ein gefundener Arbitrage-Cycle
#[derive(Debug, Clone)]
pub struct ArbCycle {
    /// Pfad: [WSOL, Token_A, Token_B, ..., WSOL]
    pub path: Vec<Pubkey>,
    /// Pools für jeden Hop - MIT ALTERNATIVEN für Execution Fallbacks!
    /// pools[hop_idx][0] = Best Pool, pools[hop_idx][1..] = Fallbacks
    pub pools: Vec<Vec<PoolEdge>>,
    /// Geschätzter Return (vor Slippage)
    pub estimated_return_bps: i32,
    /// Minimale Liquidity im Pfad
    pub min_liquidity_usd: f64,
}

/// Search Node für Priority Queue
#[derive(Debug, Clone)]
struct SearchNode {
    token: Pubkey,
    path: Vec<Pubkey>,
    /// Pools für jeden Hop - jetzt mit Alternativen für Fallback!
    pools: Vec<Vec<PoolEdge>>, // Vec<Vec<>> statt Vec<> für Top-3 Fallbacks
    /// Aktueller Profit-Multiplikator (1.0 = break-even)
    profit: f64,
    /// Score für Priorisierung (höher = besser)
    score: f64,
    /// Tiefe (Hop-Count)
    depth: usize,
    /// Minimale Liquidity entlang des Pfades
    min_liquidity: f64,
    /// Visited tokens für O(1) lookup statt O(n) path.contains()
    visited: HashSet<Pubkey>,
}

// Für BinaryHeap (max-heap nach score)
impl Ord for SearchNode {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score.partial_cmp(&other.score).unwrap_or(Ordering::Equal)
    }
}
impl PartialOrd for SearchNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for SearchNode {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score
    }
}
impl Eq for SearchNode {}

/// Cycle Finder mit Best-First Beam Search + Branch-and-Bound
pub struct BeamCycleFinder {
    /// Maximum hops (empfohlen: 4)
    max_depth: usize,
    /// Beam width pro Tiefenlevel (empfohlen: 20)
    beam_width: usize,
    /// Epsilon für Pruning-Toleranz (empfohlen: 0.01 = 1%)
    epsilon: f64,
    /// Minimum profit in bps um als Cycle zu zählen
    min_profit_bps: i32,
    /// Start/End token (WSOL)
    base_mint: Pubkey,
}

impl BeamCycleFinder {
    pub fn new(max_depth: usize, beam_width: usize) -> Self {
        Self {
            max_depth,
            beam_width,
            epsilon: 0.01,
            min_profit_bps: 10, // Min 0.1% profit
            base_mint: spl_token::native_mint::id(),
        }
    }

    /// Find best arbitrage cycles using Best-First Beam Search + Branch-and-Bound
    pub fn find_cycles(
        &self,
        graph: &PoolGraph,
        ranker: &PoolRanker,
    ) -> Vec<ArbCycle> {
        let mut cycles = Vec::new();
        let mut best_profit = 1.0; // Track best found (für Pruning)
        
        // Priority Queue (max-heap nach score)
        let mut queue = BinaryHeap::new();
        
        // Start node: WSOL mit profit = 1.0
        let mut start_visited = HashSet::new();
        start_visited.insert(self.base_mint);
        
        queue.push(SearchNode {
            token: self.base_mint,
            path: vec![self.base_mint],
            pools: vec![],
            profit: 1.0,
            score: 1.0,
            depth: 0,
            min_liquidity: f64::MAX,
            visited: start_visited,
        });
        
        // ═══════════════════════════════════════════════════════════════════
        // KORRIGIERT: Proper Beam Limit mit Score-Threshold
        // Alte Implementierung zählte "first K" statt "best K"!
        // ═══════════════════════════════════════════════════════════════════
        // Track minimum score pro depth level für echtes Top-K
        let mut min_score_at_depth: HashMap<usize, f64> = HashMap::new();
        let mut count_at_depth: HashMap<usize, usize> = HashMap::new();
        
        while let Some(node) = queue.pop() {
            // ═══════════════════════════════════════════════════════════
            // 1. DEPTH CONSTRAINT
            // ═══════════════════════════════════════════════════════════
            if node.depth > self.max_depth {
                continue;
            }
            
            // ═══════════════════════════════════════════════════════════
            // 2. CYCLE COMPLETE? (zurück bei WSOL)
            // ═══════════════════════════════════════════════════════════
            if node.token == self.base_mint && node.depth > 1 {
                let return_bps = ((node.profit - 1.0) * 10000.0) as i32;
                if return_bps >= self.min_profit_bps {
                    best_profit = best_profit.max(node.profit);
                    cycles.push(ArbCycle {
                        path: node.path.clone(),
                        pools: node.pools.clone(),
                        estimated_return_bps: return_bps,
                        min_liquidity_usd: node.min_liquidity,
                    });
                }
                continue;
            }
            
            // ═══════════════════════════════════════════════════════════
            // 3. BRANCH-AND-BOUND: Upper Bound Pruning
            // ═══════════════════════════════════════════════════════════
            let remaining_hops = self.max_depth - node.depth;
            let max_ratio = ranker.max_ratio(&node.token);
            let upper_bound = node.profit * max_ratio.powi(remaining_hops as i32);
            
            // Prune wenn selbst optimistischste Schätzung nicht gewinnen kann
            if upper_bound < best_profit * (1.0 + self.epsilon) {
                continue; // PRUNE - dieser Pfad kann nicht besser werden
            }
            
            // ═══════════════════════════════════════════════════════════
            // 4. BEAM LIMIT: KORRIGIERT - Top-K nach Score (nicht First-K!)
            // ═══════════════════════════════════════════════════════════
            let next_depth = node.depth + 1;
            let count = count_at_depth.entry(next_depth).or_insert(0);
            let min_score = min_score_at_depth.entry(next_depth).or_insert(0.0);
            
            // Wenn Beam voll ist: nur noch bessere Nodes akzeptieren
            if *count >= self.beam_width && node.score <= *min_score {
                continue; // Score nicht gut genug für dieses Level
            }
            
            // ═══════════════════════════════════════════════════════════
            // 5. EXPAND: Nachbarn erkunden (sortiert nach Pre-computed Ranking)
            // ═══════════════════════════════════════════════════════════
            for (next_mint, _) in graph.neighbors(&node.token) {
                // KORRIGIERT: O(1) visited check statt O(n) path.contains()
                if node.visited.contains(next_mint) && *next_mint != self.base_mint {
                    continue;
                }
                
                // Hole pre-ranked Pools (jetzt Top-3 für Fallbacks)
                let Some(ranked_pools) = ranker.get_top_pools(&node.token, next_mint) else {
                    continue;
                };
                
                // Nimm besten Pool für Profit-Berechnung
                let Some(best) = ranked_pools.first() else {
                    continue;
                };
                
                let child_profit = node.profit * best.edge_ratio;
                let child_liquidity = node.min_liquidity.min(best.edge.liquidity_usd);
                
                // ═══════════════════════════════════════════════════════
                // KORRIGIERT: Gedämpftes Scoring (nicht sqrt!)
                // sqrt() war zu dominant für high-liquidity Pools
                // ═══════════════════════════════════════════════════════
                let child_score = child_profit * best.liquidity_score; // Bereits gedämpft
                
                let mut child_path = node.path.clone();
                child_path.push(*next_mint);
                
                // KORRIGIERT: Speichere ALLE ranked pools (Top-3) für Fallbacks
                let mut child_pools = node.pools.clone();
                let pool_alternatives: Vec<PoolEdge> = ranked_pools
                    .iter()
                    .map(|rp| rp.edge.clone())
                    .collect();
                child_pools.push(pool_alternatives);
                
                let mut child_visited = node.visited.clone();
                child_visited.insert(*next_mint);
                
                queue.push(SearchNode {
                    token: *next_mint,
                    path: child_path,
                    pools: child_pools,
                    profit: child_profit,
                    score: child_score,
                    depth: next_depth,
                    min_liquidity: child_liquidity,
                    visited: child_visited,
                });
                
                // Update beam tracking
                *count += 1;
                if *count <= self.beam_width {
                    *min_score = min_score.min(child_score);
                }
            }
        }
        
        // Sort by estimated return (descending)
        cycles.sort_by(|a, b| b.estimated_return_bps.cmp(&a.estimated_return_bps));
        cycles
    }
}
```

#### 2.4 Algorithmus-Visualisierung

```
                    ┌─────────────────────────────────────────────────┐
                    │          Best-First Beam Search Flow            │
                    └─────────────────────────────────────────────────┘

     Priority Queue (Max-Heap nach Score)
     ┌─────────────────────────────────────┐
     │ [WSOL, score=1.0, depth=0]          │ ← Start
     └─────────────────────────────────────┘
                      │
                      ▼
     ┌─────────────────────────────────────┐
     │ Pop: WSOL                           │
     │ Expand: SOL→USDC, SOL→RAY, SOL→JUP │
     │ Push children (sorted by score)    │
     └─────────────────────────────────────┘
                      │
                      ▼
     ┌─────────────────────────────────────┐
     │ [USDC, p=1.02, score=45.2, d=1]     │ ← Best
     │ [RAY,  p=1.01, score=32.1, d=1]     │
     │ [JUP,  p=0.99, score=28.5, d=1]     │
     └─────────────────────────────────────┘
                      │
         ┌────────────┴────────────┐
         │                         │
         ▼                         ▼
    Pop: USDC                  Beam Limit
    Upper Bound Check:         (max 20 pro level)
    ub = 1.02 × 1.05² = 1.12        │
    if ub < best×(1+ε) → PRUNE      │
         │                         │
         ▼                         │
    Expand: USDC→RAY, USDC→SOL     │
         │                         │
         ▼                         │
    ┌──────────────────────────────┘
    │
    ▼
    Pop: [USDC→SOL, p=1.04, d=2]  ← Cycle gefunden!
    return_bps = (1.04 - 1) × 10000 = 400 bps = 4%
    best_profit = 1.04
    
    Continue search für bessere Cycles...
```
```

---

### Phase 3: Pre-Compute Integration

Der `PoolRanker` aus Abschnitt 2.2 muss in die Main-Loop integriert werden:

```rust
impl ArbStrategy {
    /// Main loop mit Pre-Compute Refresh
    async fn run(&mut self) {
        let mut ranking_interval = tokio::time::interval(Duration::from_secs(30));
        
        loop {
            tokio::select! {
                // Market Events verarbeiten
                Some(event) = self.events_rx.recv() => {
                    self.handle_market_event(&event);
                }
                
                // Pre-Compute Rankings alle 30s refreshen
                _ = ranking_interval.tick() => {
                    let start = Instant::now();
                    self.ranker.refresh(&self.pool_graph, &self.price_cache);
                    let stats = self.pool_graph.stats();
                    tracing::info!(
                        pools = stats.pool_count,
                        mints = stats.mint_count,
                        refresh_ms = start.elapsed().as_millis(),
                        "Pool rankings refreshed"
                    );
                }
                
                // Cycle Detection triggern (z.B. alle 100ms oder on-demand)
                _ = self.cycle_trigger.recv() => {
                    let cycles = self.cycle_finder.find_cycles(&self.pool_graph, &self.ranker);
                    if let Some(best) = cycles.first() {
                        if best.estimated_return_bps >= self.config.min_profit_bps {
                            let intent = self.create_multi_hop_intent(best);
                            self.publish_intent(intent).await;
                        }
                    }
                }
            }
        }
    }
}
```

---

### Phase 4: TradeIntent Schema Extension

#### 4.1 IPC Schema Update

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
/// ⚠️ KORRIGIERT: Mit Pool-Alternativen für Execution Fallbacks!
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwapHop {
    /// Primary pool address (best option from search)
    pub pool_address: String,
    /// DEX type
    pub dex: String,
    /// Input mint for this hop
    pub input_mint: String,
    /// Output mint for this hop
    pub output_mint: String,
    /// Expected output amount (for slippage check)
    pub expected_output: u64,
    /// Alternative pools for this hop (fallbacks if primary fails)
    /// Sorted by preference (best first)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pool_alternatives: Vec<PoolAlternative>,
}

/// Alternative pool for fallback routing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolAlternative {
    pub pool_address: String,
    pub dex: String,
    /// Expected output (may differ from primary)
    pub expected_output: u64,
}
```

#### 4.2 Intent Creation in arb-strategy

```rust
fn create_multi_hop_intent(cycle: &ArbCycle, input_amount: u64) -> TradeIntent {
    // cycle.pools ist jetzt Vec<Vec<PoolEdge>> - mit Alternativen!
    let hops: Vec<SwapHop> = cycle.pools.iter().enumerate().map(|(i, pool_options)| {
        let primary = &pool_options[0]; // Best pool
        
        // Fallback-Pools (Index 1+)
        let alternatives: Vec<PoolAlternative> = pool_options
            .iter()
            .skip(1)
            .map(|alt| PoolAlternative {
                pool_address: alt.pool_address.to_string(),
                dex: alt.dex.to_string(),
                expected_output: 0, // Calculated by execution-engine
            })
            .collect();
        
        SwapHop {
            pool_address: primary.pool_address.to_string(),
            dex: primary.dex.to_string(),
            input_mint: cycle.path[i].to_string(),
            output_mint: cycle.path[i + 1].to_string(),
            expected_output: 0, // Calculated by execution-engine
            pool_alternatives: alternatives,
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

### Phase 5: execution-engine Multi-Hop Support

#### 5.1 Multi-Hop Quote

```rust
// src/execution/quote_calculator.rs

impl QuoteCalculator {
    /// Quote a multi-hop path with fallback support
    /// 
    /// ⚠️ KORRIGIERT: Bei Quote-Fehler auf pool_alternatives fallbacken
    pub async fn quote_multi_hop(
        &self,
        path: &[SwapHop],
        input_amount: u64,
    ) -> Result<MultiHopQuote> {
        let mut current_amount = input_amount;
        let mut quotes = Vec::with_capacity(path.len());
        let mut selected_pools = Vec::with_capacity(path.len());

        for hop in path {
            // Versuche Primary Pool
            let quote_result = self.try_quote_hop(hop, current_amount).await;
            
            let (quote, pool_used) = match quote_result {
                Ok(q) => (q, hop.pool_address.clone()),
                Err(_) => {
                    // ═══════════════════════════════════════════════════
                    // KORRIGIERT: Fallback auf Alternative Pools
                    // ═══════════════════════════════════════════════════
                    let mut fallback_quote = None;
                    for alt in &hop.pool_alternatives {
                        if let Ok(q) = self.try_quote_pool(&alt.pool_address, current_amount).await {
                            fallback_quote = Some((q, alt.pool_address.clone()));
                            break;
                        }
                    }
                    fallback_quote.ok_or_else(|| anyhow!("All pools failed for hop"))?
                }
            };

            current_amount = quote.output_amount;
            quotes.push(HopQuote {
                input_amount: quote.input_amount,
                output_amount: quote.output_amount,
                price_impact_bps: quote.price_impact_bps,
                fee_amount: quote.fee_amount,
                pool_used: pool_used.clone(), // Track welcher Pool genutzt wurde
            });
            selected_pools.push(pool_used);
        }

        Ok(MultiHopQuote {
            input_amount,
            output_amount: current_amount,
            hops: quotes,
            selected_pools, // Für TX Builder
            total_price_impact_bps: quotes.iter().map(|q| q.price_impact_bps).sum(),
        })
    }
    
    async fn try_quote_hop(&self, hop: &SwapHop, amount: u64) -> Result<PoolQuote> {
        self.try_quote_pool(&hop.pool_address, amount).await
    }
    
    async fn try_quote_pool(&self, pool_address: &str, amount: u64) -> Result<PoolQuote> {
        let pool = self.pool_cache.get_pool(pool_address)?;
        pool.quote_exact_in(amount)
    }
}
```

#### 5.2 Multi-Hop TX Builder

```rust
// src/solana/cross_dex_handler.rs

impl CrossDexHandler {
    /// Build multi-hop swap transaction
    /// 
    /// ⚠️ KORRIGIERT: Nutzt selected_pools aus Quote (nach Fallback-Resolution)
    pub fn build_multi_hop_swap(
        &self,
        path: &[SwapHop],
        quote: &MultiHopQuote, // Enthält selected_pools nach Fallback
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

        // 2. Build swap instructions for each hop (mit resolved pools)
        let mut current_amount = input_amount;
        for (i, (hop, pool_address)) in path.iter().zip(&quote.selected_pools).enumerate() {
            let is_last = i == path.len() - 1;
            let min_out = if is_last { min_output } else { 1 }; // Only enforce on last hop

            // Nutze den Pool der beim Quote ausgewählt wurde (kann Fallback sein!)
            let swap_ix = self.build_swap_ix(
                pool_address, // Aus Quote, nicht aus hop.pool_address!
                &hop.dex,
                &hop.input_mint,
                &hop.output_mint,
                current_amount,
                min_out,
            )?;

            instructions.extend(swap_ix);

            // Update current_amount for next hop (from quote)
            current_amount = quote.hops[i].output_amount;
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
- [ ] `PoolEdge` struct implementieren (ungerichtet!)
- [ ] Graph update handler für MarketEvents
- [ ] Unit tests für Graph operations

### Phase 2: Pre-Compute & Ranking ⚠️ KRITISCH
- [ ] `PoolRanker` struct implementieren
- [ ] **KRITISCH**: `edge_ratio` via probe-based Quote (nicht Spot-Preis!)
- [ ] `probe_amount_lamports` konfigurierbar (~$100-500)
- [ ] `max_edge_ratio` Berechnung pro Token
- [ ] **KORRIGIERT**: Top-3 Pool Selection (für Fallbacks)
- [ ] **KORRIGIERT**: Gedämpftes Liquidity-Scoring (log oder clamp)
- [ ] Blacklist-Handling (Rugs, Low-Liquidity)
- [ ] Periodic Refresh (~30s)
- [ ] Unit tests für Ranking

### Phase 3: Cycle Detection (Best-First Beam Search) ⚠️ KRITISCH
- [ ] `BeamCycleFinder` struct implementieren
- [ ] `SearchNode` mit Score-basierter Priority Queue
- [ ] **KRITISCH**: `visited: HashSet` für O(1) lookup (nicht path.contains!)
- [ ] Branch-and-Bound Upper Bound Pruning
- [ ] **KORRIGIERT**: Proper Beam Limit (Top-K nach Score, nicht First-K!)
- [ ] Depth Constraint (max 4 Hops)
- [ ] **KORRIGIERT**: Scoring mit gedämpfter Liquidity
- [ ] **KORRIGIERT**: Pool-Alternativen in ArbCycle speichern
- [ ] Unit tests für Cycle Detection
- [ ] Benchmark: Vergleich mit naivem DFS

### Phase 4: Main Loop Integration
- [ ] Pre-Compute Refresh Interval (30s)
- [ ] Cycle Detection Trigger
- [ ] Best Cycle Selection + Intent Emission
- [ ] Metrics/Logging für Cycle-Finder Performance
- [ ] `SwapHop` struct zu IPC Schema (mit `pool_alternatives`!)
- [ ] `PoolAlternative` struct für Fallbacks
- [ ] `swap_path` field zu TradeIntent
- [ ] Backward-compatible (None = legacy)
- [ ] arb-strategy Intent creation

### Phase 5: execution-engine ⚠️ KRITISCH
- [ ] **KORRIGIERT**: `quote_multi_hop()` mit Fallback-Support
- [ ] Fallback auf `pool_alternatives` bei Quote-Fehler
- [ ] `MultiHopQuote.selected_pools` tracken
- [ ] **KORRIGIERT**: `build_multi_hop_swap()` nutzt selected_pools
- [ ] Handle `swap_path` in Intent processing
- [ ] Slippage handling für multi-hop

### Phase 6: Testing
- [ ] Unit tests für alle neuen Komponenten
- [ ] Integration test: 3-hop cycle
- [ ] Integration test: Fallback bei Pool-Fehler
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

4. **Beam Width Tuning**: Wie groß sollte K sein?
   - Trade-off: Größer K = mehr Coverage, langsamer
   - Empfehlung: K=20 als Start, Benchmark verschiedene Werte

5. **RL-based Scoring** (Future Work): Score durch Learned Policy adjustieren?
   - Tendenz: Erstmal statisches Scoring, RL als Optimierung später
   - Problem: Arbitrage-Landscape ändert sich ständig (neue Pools, MEV-Konkurrenz)

---

## Algorithmus-Komplexität

| Komponente | Komplexität | Erklärung |
|------------|-------------|------------|
| Pre-Compute | O(E × log K) | Sortieren + Top-K Selection pro Edge |
| Beam Search | O(K × D × B) | K=Beam Width, D=Max Depth, B=Avg Branching |
| Upper Bound Check | O(1) | Lookup + Multiplikation |
| **Gesamt** | **O(E log K + K × D × B)** | Deutlich besser als O(V × E) von Bellman-Ford |

Beispiel mit realistischen Zahlen:
- E = 50,000 Pools
- K = 20 Beam Width
- D = 4 Max Depth  
- B = 10 Avg Branching

→ ~50k × 3 + 20 × 4 × 10 = **150,800 Operationen**

vs Bellman-Ford: V × E = 10,000 × 50,000 = **500,000,000 Operationen**

**Speed-up: ~3300x**

---

## Dependencies

Keine neuen crates benötigt. Nutzt:
- `std::collections::HashMap` für Graph
- Existing Pool/DEX abstractions
- Existing IPC/NATS infrastructure
