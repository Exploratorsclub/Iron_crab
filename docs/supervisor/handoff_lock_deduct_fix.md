# Handoff: update_native_sol_only — Lock-Abzug Fix

## Problem

`update_native_sol_only()` setzt `available_sol` direkt auf den On-Chain-Wert von Geyser. Aber `total_native_sol()` berechnet `available_sol + sum(capital_locks)`. Da der On-Chain-Wert die gelockten Betraege bereits enthaelt (Locks sind nur In-Memory), fuehrt das zu Doppelzaehlung.

Beispiel:
- On-Chain: 2B SOL, Lock: 500M
- update_native_sol_only(2B) → available_sol = 2B
- total_native_sol() = 2B + 500M = 2.5B (FALSCH, sollte 2B sein)

## Fix

In `src/storage/locks.rs`, Methode `update_native_sol_only` aendern:

**VORHER:**
```rust
pub fn update_native_sol_only(&self, sol_lamports: u64) {
    *self.available_sol.write() = sol_lamports;
}
```

**NACHHER:**
```rust
pub fn update_native_sol_only(&self, sol_lamports: u64) {
    let locked: u64 = self
        .capital_locks
        .read()
        .values()
        .map(|l| l.sol_lamports)
        .sum();
    *self.available_sol.write() = sol_lamports.saturating_sub(locked);
}
```

## Pruefung

- Nach dem Fix: `cargo fmt`, `cargo clippy -- -D warnings`, `cargo test`
- Keine weiteren Dateien betroffen — nur diese eine Methode in locks.rs
