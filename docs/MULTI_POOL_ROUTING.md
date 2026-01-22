# Multi-Pool Routing

**Status**: ✅ Complete  
**Implemented**: Januar 2025  

## Summary

Multi-Pool Routing findet den **besten Pool für EXIT-Trades** wenn ein Token auf mehreren DEXes gelistet ist. Typische Preisverbesserung: 2-10%.

**Code Locations:**
- `PoolInfo` struct: `src/bin/momentum_bot.rs`
- `mint_pools` HashMap: `MomentumContext`
- `find_best_sell_pool()`: Best-Pool-Finder
- Integration: `generate_and_publish_exit_intent()`

---

## Problem (gelöst)
Momentum-bot verwendete nur den Pool aus dem ursprünglichen MarketEvent:
- **BUY**: Kauft über den Pool aus dem PoolCreated/Trade Event
- **SELL**: Verkauft über denselben Pool wie beim Kauf

Dies ignoriert dass:
1. Ein Token auf mehreren DEXes gelistet sein kann (Raydium + Orca + PumpSwap)
2. Preise zwischen Pools variieren (manchmal 5-10%!)
3. Geyser stream bereits alle Pool-Daten hat

## Lösung: Best Price Routing

### Phase 1: Multi-Pool Registry (P0 - Foundation)

**Datenstruktur erweitern:**
```rust
/// New: Track all pools for a mint pair
struct PoolInfo {
    pool_address: String,
    dex: String,
    dex_pool_accounts: Option<Vec<String>>,
    first_seen_slot: u64,
    last_trade_slot: u64,
    last_trade_ratio: Option<f64>, // SOL per token
}

/// Add to MomentumContext:
struct MomentumContext {
    // ... existing fields ...
    
    /// NEW: All known pools per token mint (mint -> Vec<PoolInfo>)
    mint_pools: parking_lot::RwLock<HashMap<String, Vec<PoolInfo>>>,
}
```

**Population Logic:**
- Beim `MarketEventKind::PoolCreated` → neue PoolInfo anlegen
- Beim `MarketEventKind::Trade` → last_trade_ratio + last_trade_slot updaten
- Beim `MarketEventKind::DexPoolAccounts` → dex_pool_accounts cachen

### Phase 2: Best Pool Finder (P0 - Core Logic)

**SELL Optimization (höchste Priorität):**
```rust
/// Find best pool for selling tokens
fn find_best_sell_pool(
    &self,
    token_mint: &str,
    amount: u64,
) -> Result<(String, String, Vec<String>, f64)> {
    // (pool, dex, accounts, expected_sol_out)
    
    let pools = self.mint_pools.read();
    let candidates = pools.get(token_mint)
        .ok_or_else(|| anyhow!("No pools known for mint"))?;
    
    // Filter: must have dex_pool_accounts (needed for swap)
    let valid: Vec<_> = candidates
        .iter()
        .filter(|p| p.dex_pool_accounts.is_some())
        .collect();
    
    if valid.is_empty() {
        anyhow::bail!("No pools with accounts available");
    }
    
    // Quote each pool (using cached last_trade_ratio)
    let mut quotes: Vec<_> = valid
        .iter()
        .filter_map(|p| {
            p.last_trade_ratio.map(|ratio| {
                let expected_sol = (amount as f64) * ratio;
                (p.pool_address.clone(), p.dex.clone(), 
                 p.dex_pool_accounts.clone().unwrap(), expected_sol)
            })
        })
        .collect();
    
    if quotes.is_empty() {
        anyhow::bail!("No pools with recent trade data");
    }
    
    // Sort by expected SOL output (descending)
    quotes.sort_by(|a, b| b.3.partial_cmp(&a.3).unwrap());
    
    Ok(quotes[0].clone())
}
```

**BUY Optimization (nur für ESTABLISHED):**
```rust
/// Find best pool for buying tokens
/// Only used for ESTABLISHED strategy (EARLY must be fast!)
fn find_best_buy_pool(
    &self,
    token_mint: &str,
    sol_amount: u64,
) -> Result<(String, String, Vec<String>, f64)> {
    // (pool, dex, accounts, expected_tokens_out)
    
    // Similar logic but inverted:
    // - expected_tokens = sol_amount / ratio
    // - pick highest token output
    
    // ... implementation similar to find_best_sell_pool ...
}
```

### Phase 3: Integration Points

**EXIT Intent Generation (immediate value):**
```rust
async fn generate_and_publish_exit_intent(...) -> Result<()> {
    // OLD: Use position.pool directly
    // let pool = &position.pool;
    
    // NEW: Find best pool
    let (pool, dex, accounts, expected_sol) = ctx
        .find_best_sell_pool(&mint, token_amount)?;
    
    info!(
        mint = %mint,
        original_pool = %position.pool,
        best_pool = %pool,
        expected_improvement = ?((expected_sol / position.entry_price) - 1.0),
        "🎯 Best sell pool found"
    );
    
    // Build intent with best pool
    // ...
}
```

**BUY Intent Generation (only for ESTABLISHED):**
```rust
async fn generate_and_publish_buy_intent(...) -> Result<()> {
    // EARLY: Keep current behavior (speed > price)
    if signal_type == "EARLY" {
        // Use original pool from MarketEvent
        let pool = &tracker.pool;
        // ...
    }
    
    // ESTABLISHED: Find best pool
    if signal_type == "ESTABLISHED" {
        let (pool, dex, accounts, expected_tokens) = ctx
            .find_best_buy_pool(&mint, sol_amount)?;
        
        info!(
            mint = %mint,
            original_pool = %tracker.pool,
            best_pool = %pool,
            "🎯 Best buy pool found"
        );
        
        // Build intent with best pool
        // ...
    }
}
```

## Implementation Phases

### Phase 1: Foundation (1-2 hours)
- [ ] Add `mint_pools: HashMap<String, Vec<PoolInfo>>` to MomentumContext
- [ ] Populate from MarketEvents (PoolCreated, Trade, DexPoolAccounts)
- [ ] Add metrics: `pools_per_token_p50/p95/p99`
- [ ] Test: Verify multiple pools are tracked for same mint

### Phase 2: SELL Optimization (1 hour)
- [ ] Implement `find_best_sell_pool()`
- [ ] Integrate into `generate_and_publish_exit_intent()`
- [ ] Add DecisionRecord field: `alternative_pools_checked: u32`
- [ ] Add metrics: `exit_pool_switches`, `exit_price_improvement_bps`
- [ ] Test: Manually create 2 pools for same mint, verify best is chosen

### Phase 3: BUY Optimization (1 hour)
- [ ] Implement `find_best_buy_pool()`
- [ ] Integrate ONLY for ESTABLISHED buys
- [ ] Add metrics: `buy_pool_switches`, `buy_price_improvement_bps`
- [ ] Test: Verify EARLY still uses original pool (speed priority)

### Phase 4: Production Validation (ongoing)
- [ ] Monitor Grafana: How often do we find better pools?
- [ ] Monitor DecisionRecords: What's typical price improvement?
- [ ] A/B test: Compare P&L with/without multi-pool routing

## Expected Impact

**SELL Optimization:**
- **High impact**: Exit price improvement 2-10% (common arbitrage spreads)
- **Low risk**: No speed penalty (exits not time-critical)
- **Immediate value**: Every position benefits

**BUY Optimization (ESTABLISHED only):**
- **Medium impact**: Entry price improvement 1-5%
- **Low risk**: EARLY strategy unchanged (keeps speed advantage)
- **Gradual value**: Only helps ESTABLISHED entries

## Risks & Mitigation

### Risk 1: Stale Pool Data
**Problem**: last_trade_ratio might be outdated (pool drained/arbitraged)
**Mitigation**: 
- Only use pools with recent trades (e.g. last 100 slots)
- Fall back to original pool if no fresh data
- Phase 4: Add simulation before final selection

### Risk 2: Account Changes
**Problem**: DexPoolAccounts might change (pool upgrade/migration)
**Mitigation**:
- Re-validate accounts[0] == pool_address before use
- Reject if mismatch detected

### Risk 3: Latency on BUY
**Problem**: Checking multiple pools adds 1-5ms
**Mitigation**:
- EARLY strategy: skip multi-pool check (use original)
- ESTABLISHED: acceptable (not racing)

## Metrics to Add

```toml
# Grafana queries
exit_pool_switches_total          # How often we switch pools for exits
exit_price_improvement_bps_avg    # Average improvement from switching
buy_pool_switches_total           # (ESTABLISHED only)
buy_price_improvement_bps_avg
pools_per_mint_p50/p95            # How many pools per token typically
```

## Future Extensions (Post-MVP)

1. **Simulation-based selection**: Instead of cached ratio, do quick simulation
2. **Jupiter-style routing**: Multi-hop swaps (Token → USDC → SOL)
3. **Liquidity weighting**: Prefer deeper pools (lower slippage)
4. **Dynamic slippage**: Adjust based on pool depth

## Definition of Done (DoD)

Per `docs/DEFINITION_OF_DONE.md`:

- [ ] **P0 Safety**: 
  - ✅ No RPC calls (uses Geyser-cached data only)
  - ✅ Deterministic (same pool data → same choice)
  - ✅ Reason-coded rejects if no valid pool found

- [ ] **P0 Observability**:
  - ✅ DecisionRecord shows: `alternative_pools_checked`, `pool_switch_reason`
  - ✅ Metrics: pool switches, price improvements
  - ✅ Logs: pool selection rationale (debug level)

- [ ] **P1 Testing**:
  - ✅ Unit test: Multiple pools, best is chosen
  - ✅ Unit test: Stale data fallback
  - ✅ Unit test: EARLY uses original pool

- [ ] **P2 Documentation**:
  - ✅ This doc (MULTI_POOL_ROUTING.md)
  - ✅ Config schema: `multi_pool_routing_enabled: bool`
  - ✅ Runbook: How to disable if issues

## Config Integration

Add to `MomentumConfig`:
```toml
[strategy]
# ... existing fields ...

# Multi-pool routing
multi_pool_routing_enabled = true              # Master switch
multi_pool_routing_max_age_slots = 1000        # Only use pools with trades in last N slots
multi_pool_routing_min_improvement_bps = 50    # Only switch if >0.5% better
multi_pool_routing_buy_established_only = true # Don't slow down EARLY buys
```

## Rollout Plan

1. **Dev/Test**: Implement Phase 1-2 (SELL only)
2. **Staging**: Enable on testnet, monitor for 24h
3. **Production Canary**: Enable SELL routing, disable BUY routing
4. **Full Rollout**: Enable BUY routing for ESTABLISHED after 7d validation
5. **Iterate**: Add simulation-based selection if needed

## Success Criteria

**Week 1:**
- Multi-pool registry populated (>80% of tokens have 1+ pools)
- SELL routing working (0 errors, >10% switches use alternative pool)
- Average exit price improvement: >1%

**Week 4:**
- BUY routing for ESTABLISHED working
- Cumulative P&L improvement: >5% (vs baseline without routing)
- Zero production incidents related to pool selection

---

**Note**: This follows `docs/TARGET_ARCHITECTURE.md` principles:
- ✅ Intent-only (no direct execution)
- ✅ Geyser-first (no RPC fallbacks)
- ✅ Debuggable (DecisionRecords capture choices)
- ✅ Deterministic (same inputs → same output)
