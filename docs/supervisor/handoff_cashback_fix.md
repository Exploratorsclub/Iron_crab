# HANDOFF: Fix cashback_enabled JetStream Propagation (3-teilig)

## PFLICHT: Lese vor jeder Aenderung die folgenden Dateien:
- docs/INVARIANTS.md
- docs/KNOWN_BUG_PATTERNS.md (insbesondere #25 REGRESSIERT)
- AGENTS.md (STOP-CHECK Rules)

## Kontext
PumpFun SELL Transaktionen scheitern mit Custom(6024) Overflow weil `cashback_enabled` im SLAVE LivePoolCache immer `false` ist. Die Ursachenkette:

1. `market_data.rs` publiziert PoolCacheUpdate auf JetStream OHNE `cashback_enabled` in metadata
2. `pool_cache_sync.rs` hardcodet `cashback_enabled: false` (Zeile 227)
3. `build_swap_ix_async_with_slippage` bekommt Cache-HIT mit `cashback_enabled=false`
4. `build_sell_ix` laesst `user_volume_accumulator` Account weg
5. PumpFun Overflow(6024)

Reproduziert und bestaetigt durch On-Chain Simulation:
- SELL mit `cashback=true` + `user_volume_accumulator` → **SUCCESS** (Error: None)
- SELL mit `cashback=false` (missing uva) → **Custom(6024) Overflow**

## Aenderung 1: src/bin/market_data.rs (~Zeile 2626)

Im Block `CachedPoolState::PumpFun(s) =>` wo JetStream PoolCacheUpdate metadata befuellt wird.
FUEGE HINZU nach der Zeile `meta.insert("real_sol_reserves".to_string(), s.real_sol_reserves.to_string());`:

```rust
meta.insert("cashback_enabled".to_string(), s.cashback_enabled.to_string());
```

## Aenderung 2: src/execution/pool_cache_sync.rs (Zeile 227)

ERSETZE:
```rust
cashback_enabled: false, // JetStream metadata may not have it; safe default — resolved at TX build time via RPC
```

MIT:
```rust
cashback_enabled: update.metadata.as_ref()
    .and_then(|m| m.get("cashback_enabled"))
    .map(|v| v == "true")
    .unwrap_or(false), // Resolved from JetStream metadata (propagated by market-data from Geyser)
```

## Aenderung 3: src/solana/dex/pumpfun.rs (build_swap_ix_async_with_slippage)

In ALLEN DREI Creator-Resolution-Branches (~Zeilen 1196, 1224, 1254) gibt es jeweils dieses Pattern:

```rust
let cashback = match self.get_bonding_curve_from_cache(&bonding_curve) {
    Some(state) => state.cashback_enabled,
    None => {
        if allow_rpc_fallback {
            match self.fetch_bonding_curve_fast(&bonding_curve).await {
                Some(state) => {
                    warn!(
                        bonding_curve = %bonding_curve,
                        cashback_enabled = state.cashback_enabled,
                        "cashback_enabled resolved via RPC fallback (cache miss)"
                    );
                    state.cashback_enabled
                }
                None => false,
            }
        } else {
            // Hot Path: Cache miss → cashback_enabled=false (I-7, no RPC)
            false
        }
    }
};
```

ERSETZE JEDES der drei Vorkommen MIT:

```rust
let cashback = if allow_rpc_fallback {
    // Cold Path (Liquidation): ALWAYS verify via RPC — JetStream cache may have stale cashback_enabled
    match self.fetch_bonding_curve_fast(&bonding_curve).await {
        Some(state) => state.cashback_enabled,
        None => self.get_bonding_curve_from_cache(&bonding_curve)
            .map(|s| s.cashback_enabled)
            .unwrap_or(false),
    }
} else {
    // Hot Path: trust Geyser-fed cache (I-7: no RPC)
    self.get_bonding_curve_from_cache(&bonding_curve)
        .map(|s| s.cashback_enabled)
        .unwrap_or(false)
};
```

## STOP-CHECK Hinweise
- **Check 1 (I-7)**: Die RPC-Aenderung in pumpfun.rs ist NUR im Cold Path (hinter `allow_rpc_fallback==true`). KEIN neuer RPC im Hot Path.
- **Check 2 (Consistency)**: Das Pattern `allow_rpc_fallback` wird bereits in allen DEX-Modulen verwendet.
- **Check 3 (Architecture)**: Keine Aenderung an Architecture, nur korrekte Propagierung existierender Daten.
- **Check 5 (Repo-Isolation)**: Keine Referenz auf Iron_crab-eval.

## Erlaubte Dateien
- `src/bin/market_data.rs`
- `src/execution/pool_cache_sync.rs`
- `src/solana/dex/pumpfun.rs`

## Nach den Aenderungen
```bash
cargo fmt
cargo clippy --all-targets -- -D warnings
cargo test --quiet
```
