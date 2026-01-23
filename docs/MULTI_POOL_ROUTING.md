# Multi-Pool Routing

**Status**: ✅ SELL Optimization Complete | ⏳ BUY Optimization Planned  
**Implemented**: Januar 2025  

---

## Summary

Multi-Pool Routing findet den **besten Pool für EXIT-Trades** wenn ein Token auf mehreren DEXes gelistet ist. Typische Preisverbesserung: 2-10%.

**Code Locations:**
- `PoolInfo` struct: `src/bin/momentum_bot.rs`
- `mint_pools` HashMap: `MomentumContext`
- `find_best_sell_pool()`: Best-Pool-Finder
- Integration: `generate_and_publish_exit_intent()`

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

### BUY Optimization (⏳ Nicht implementiert)

BUY Optimization für ESTABLISHED entries ist geplant aber noch nicht implementiert:
- EARLY: Immer Original-Pool (Geschwindigkeit > Preis)
- ESTABLISHED: Könnte besten Pool suchen (nicht zeitkritisch)

---

## Risiken & Mitigations

| Risiko | Mitigation |
|--------|------------|
| Stale Pool Data | Nur Pools mit recent trades, Fallback auf Original |
| Account Changes | Validate accounts[0] == pool_address |
| Latency (BUY) | EARLY: skip check, ESTABLISHED: acceptable |

---

## Future Work

1. **BUY Optimization**: `find_best_buy_pool()` für ESTABLISHED entries
2. **Simulation-based selection**: Statt cached ratio echte Simulation
3. **Multi-hop routing**: Token → USDC → SOL
4. **Liquidity weighting**: Prefer deeper pools

---

## See Also

- [DEX_IMPLEMENTATION.md](DEX_IMPLEMENTATION.md) - Supported DEXes
- [TARGET_ARCHITECTURE.md](TARGET_ARCHITECTURE.md) - System architecture
