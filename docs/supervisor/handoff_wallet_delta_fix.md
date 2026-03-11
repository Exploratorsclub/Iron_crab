# Handoff: Wallet-Delta Fix — Atomare SOL/WSOL-Updates

## Kontext

Die Prometheus-Metrik `wallet_total_sol_lamports` zeigt falsche Werte nach WSOL Wrap/Unwrap. Die Event-Handler fuer NATIVE_SOL und WSOL in `execution_engine.rs` aktualisieren jeweils BEIDE Werte via `update_wallet_balances()`, was zu Race-Conditions fuehrt.

## Aufgaben

### 1. `src/storage/locks.rs` — Zwei neue Methoden hinzufuegen

Fuege neben dem bestehenden `update_wallet_balances()` zwei neue oeffentliche Methoden hinzu:

```rust
/// Update only native SOL balance (from Geyser NATIVE_SOL event).
/// Does NOT touch WSOL — each event handler only updates its own value.
pub fn update_native_sol_only(&self, sol_lamports: u64) {
    *self.available_sol.write() = sol_lamports;
}

/// Update only WSOL balance (from Geyser WSOL event).
/// Does NOT touch native SOL — each event handler only updates its own value.
pub fn update_wsol_only(&self, wsol_lamports: u64) {
    *self.available_wsol.write() = wsol_lamports;
    self.wsol_initialized.store(true, std::sync::atomic::Ordering::Relaxed);
}
```

`update_wallet_balances()` bleibt bestehen — wird weiterhin beim Bootstrap verwendet.

### 2. `src/bin/execution_engine.rs` — Event-Handler entkoppeln

Suche den WalletBalanceSnapshot Event-Handler (ca. Zeile 5779-5795). Ersetze die beiden Handler-Bloecke:

**VORHER (NATIVE_SOL Handler):**
```rust
if mint == "NATIVE_SOL" {
    let wsol = ctx.lock_manager.wsol_balance();
    let wsol_opt = if wsol > 0 { Some(wsol) } else { None };
    ctx.lock_manager.update_wallet_balances(*balance_raw, wsol_opt);
    if let Some(ref tx) = ctx.wsol_balance_tx {
        let wsol = ctx.lock_manager.wsol_balance();
        let _ = tx.try_send((*balance_raw, Some(wsol)));
    }
}
```

**NACHHER:**
```rust
if mint == "NATIVE_SOL" {
    ctx.lock_manager.update_native_sol_only(*balance_raw);
    if let Some(ref tx) = ctx.wsol_balance_tx {
        let wsol = ctx.lock_manager.wsol_balance();
        let _ = tx.try_send((*balance_raw, Some(wsol)));
    }
}
```

**VORHER (WSOL Handler):**
```rust
} else if mint == WSOL_MINT || mint == SOL_MINT {
    let sol = ctx.lock_manager.total_native_sol();
    ctx.lock_manager.update_wallet_balances(sol, Some(*balance_raw));
    if let Some(ref tx) = ctx.wsol_balance_tx {
        let _ = tx.try_send((sol, Some(*balance_raw)));
    }
}
```

**NACHHER:**
```rust
} else if mint == WSOL_MINT || mint == SOL_MINT {
    ctx.lock_manager.update_wsol_only(*balance_raw);
    if let Some(ref tx) = ctx.wsol_balance_tx {
        let sol = ctx.lock_manager.total_native_sol();
        let _ = tx.try_send((sol, Some(*balance_raw)));
    }
}
```

### 3. `docs/grafana_multiprocess_dashboard.json` — 24h Delta Query glaetten

Suche die Query fuer "24h Wallet Delta" Panel. Ersetze:

**VORHER:**
```
(wallet_total_sol_lamports{job=\"execution-engine\"} - wallet_total_sol_lamports{job=\"execution-engine\"} offset 24h) / 1e9
```

**NACHHER:**
```
(avg_over_time(wallet_total_sol_lamports{job=\"execution-engine\"}[5m]) - avg_over_time(wallet_total_sol_lamports{job=\"execution-engine\"}[5m] offset 24h)) / 1e9
```

## Wichtige Hinweise

- Pruefe `INVARIANTS.md` und `KNOWN_BUG_PATTERNS.md` vor Beginn.
- `update_wallet_balances()` NICHT entfernen — wird beim Bootstrap gebraucht (Zeilen 4078, 4437).
- Der Heartbeat-Tick (Zeile 5902-5904) bleibt UNVERAENDERT — der liest frisch total_native_sol() + wsol_balance().
- Nach dem Fix: `cargo fmt`, `cargo clippy -- -D warnings`, `cargo test`
