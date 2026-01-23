# Multi-Pool Routing

**Status**: ✅ Complete (SELL + BUY Optimization)  
**Implemented**: Januar 2025  

---

## Summary

Multi-Pool Routing findet den **besten Pool** wenn ein Token auf mehreren DEXes gelistet ist:
- **SELL**: Alle Exit-Trades nutzen Multi-Pool Routing (höchste SOL-Ausgabe)
- **BUY (ScaleIn)**: Scale-In Entries nutzen Multi-Pool Routing (mehr Tokens)
- **BUY (Probe)**: Probe Entries verwenden den Original-Pool (Geschwindigkeit kritisch)

Typische Preisverbesserung: 2-10%.

**Code Locations:**
- `PoolInfo` struct: `src/bin/momentum_bot.rs`
- `mint_pools` HashMap: `MomentumContext`
- `find_best_sell_pool()`: Best-Pool-Finder für SELL
- `find_best_buy_pool()`: Best-Pool-Finder für BUY (ScaleIn only)
- Integration: `generate_and_publish_exit_intent()`, `generate_and_publish_buy_intent()`

---

## Problem

Ohne Multi-Pool Routing verwendet momentum-bot nur den Pool aus dem ursprünglichen MarketEvent:
- **BUY**: Kauft über den Pool aus dem PoolCreated/Trade Event
- **SELL**: Verkauft über denselben Pool wie beim Kauf

Dies ignoriert dass:
1. Ein Token auf mehreren DEXes gelistet sein kann (Raydium + Orca + PumpSwap)
2. Preise zwischen Pools variieren (manchmal 5-10%!)
3. Geyser stream bereits alle Pool-Daten hat

---

## Implementierung

### Multi-Pool Registry

Alle bekannten Pools pro Token-Mint werden gecacht:

```rust
struct PoolInfo {
    pool_address: String,
    dex: String,
    dex_pool_accounts: Option<Vec<String>>,
    first_seen_slot: u64,
    last_trade_slot: u64,
    last_trade_ratio: Option<f64>, // SOL per token
}

struct MomentumContext {
    /// All known pools per token mint (mint -> Vec<PoolInfo>)
    mint_pools: parking_lot::RwLock<HashMap<String, Vec<PoolInfo>>>,
}
```

**Population:**
- `MarketEventKind::PoolCreated` → neue PoolInfo anlegen
- `MarketEventKind::Trade` → last_trade_ratio + last_trade_slot updaten
- `MarketEventKind::DexPoolAccounts` → dex_pool_accounts cachen

### SELL Optimization (✅ Implementiert)

```rust
fn find_best_sell_pool(
    &self,
    token_mint: &str,
    amount: u64,
    original_pool: &str,
) -> Result<(String, String, Vec<String>, f64)> {
    // Returns: (pool, dex, accounts, expected_sol_out)
    
    let pools = self.mint_pools.read();
    let candidates = pools.get(token_mint)?;
    
    // Filter: must have dex_pool_accounts
    let valid: Vec<_> = candidates
        .iter()
        .filter(|p| p.dex_pool_accounts.is_some())
        .collect();
    
    // Quote each pool using cached last_trade_ratio
    // Sort by expected SOL output (descending)
    // Return best pool
}
```

**Integration in Exit Intent:**
```rust
async fn generate_and_publish_exit_intent(...) {
    let (pool, dex, accounts, expected_sol) = ctx
        .find_best_sell_pool(&mint, token_amount, &position.pool)?;
    
    // Build intent with best pool
}
```

### BUY Optimization (✅ Implementiert - ScaleIn only)

```rust
fn find_best_buy_pool(
    &self,
    mint: &str,
    sol_amount: u64,
    original_pool: &str,
) -> Result<(String, String, Vec<String>, f64, usize)> {
    // Returns: (pool, dex, accounts, expected_tokens_out, alternatives_checked)
    
    // Same logic as find_best_sell_pool but inverted:
    // - expected_tokens = sol_amount / ratio (ratio is SOL per token)
    // - Sort by highest token output
}
```

**Integration in Buy Intent (ScaleIn only):**
```rust
async fn generate_and_publish_buy_intent(...) {
    let (effective_pool, effective_dex, routed_accounts, alternatives_checked) =
        match signal.kind {
            EntryKind::ScaleIn => {
                // Find best pool for scale-in (price > speed)
                ctx.find_best_buy_pool(&signal.mint, signal.sol_amount, &signal.pool)?
            }
            EntryKind::Probe => {
                // Probe: Speed is critical, skip multi-pool lookup
                (signal.pool.clone(), signal.dex.clone(), None, 1)
            }
        };
    
    // Build intent with effective_pool
}
```

---

## Risiken & Mitigations

| Risiko | Mitigation |
|--------|------------|
| Stale Pool Data | Nur Pools mit recent trades, Fallback auf Original |
| Account Changes | Validate accounts[0] == pool_address |
| Latency (BUY) | EARLY: skip check, ESTABLISHED: acceptable |

---

## Future Work

1. **Simulation-based selection**: Statt cached ratio echte Simulation
2. **Multi-hop routing**: Token → USDC → SOL
3. **Liquidity weighting**: Prefer deeper pools

---

## See Also

- [DEX_IMPLEMENTATION.md](DEX_IMPLEMENTATION.md) - Supported DEXes
- [TARGET_ARCHITECTURE.md](TARGET_ARCHITECTURE.md) - System architecture
