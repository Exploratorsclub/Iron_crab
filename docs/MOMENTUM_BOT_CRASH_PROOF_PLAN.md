# Plan: Momentum-Bot Crash-Proof (unwrap/expect eliminieren)

**Status: Implementiert** (alle 4 Phasen umgesetzt)

## Ziel

Alle `unwrap()` und `expect()` im Runtime-Pfad durch defensives Error-Handling ersetzen, sodass keine Panics mehr auftreten. Stattdessen: Log + Skip/Return/Bail.

---

## Betroffene Stellen (Runtime, keine Tests)

| # | Zeile | Aufruf | Priorität |
|---|-------|--------|-----------|
| 1 | 1869 | `pool_list.last_mut().unwrap()` | Hoch |
| 2 | 2129, 2139 | `p.dex_pool_accounts.clone().unwrap()` | Mittel |
| 3 | 2239 | `Pubkey::from_str(...).unwrap()` | Niedrig |
| 4 | 2259, 2268 | `p.dex_pool_accounts.clone().unwrap()` | Mittel |
| 5 | 4524 | `execs.first().unwrap()` | Niedrig |
| 6 | 4977 | `ctx_clone.nats.as_ref().unwrap()` | Mittel |

---

## Umsetzungsplan

### 1. Zeile 1869: `pool_list.last_mut().unwrap()`

**Kontext:** Direkt nach `pool_list.push(PoolInfo::new(...))` – theoretisch nie leer.

**Maßnahme:**
```rust
// Vorher:
pool_list.last_mut().unwrap()

// Nachher:
pool_list.last_mut().expect("pool_list non-empty after push")
// ODER defensiv:
match pool_list.last_mut() {
    Some(pi) => pi,
    None => {
        error!(mint = %mint, pool = %pool_address, "record_trade: pool_list empty after push (defensive)");
        return; // oder: continue im umgebenden Kontext
    }
}
```

**Empfehlung:** `expect` mit Klartext reicht – der Push garantiert mindestens ein Element. Falls gewünscht: `if let Some(pi) = pool_list.last_mut()` mit frühem Return bei `None`.

---

### 2. Zeile 2129, 2139, 2259, 2268: `dex_pool_accounts.clone().unwrap()`

**Kontext:** `valid`/`usable` sind gefiltert mit `p.dex_pool_accounts.is_some()`. Trotzdem: Race oder Filter-Bug könnte `None` liefern.

**Maßnahme:** Statt `.unwrap()` nur Pools mit `Some` in die `quotes` aufnehmen:

```rust
// find_best_sell_pool (Zeile 2125–2143):
if let Some(expected_sol) = cache_quote {
    if let Some(accounts) = p.dex_pool_accounts.clone() {
        quotes.push((p.pool_address.clone(), p.dex.clone(), accounts, expected_sol, "cache"));
    } else {
        warn!(pool = %p.pool_address, "Skipping pool: dex_pool_accounts None (filter mismatch)");
    }
} else if let Some(ratio) = p.last_trade_ratio {
    if let Some(accounts) = p.dex_pool_accounts.clone() {
        let expected_sol = (token_amount as f64) * ratio;
        quotes.push((p.pool_address.clone(), p.dex.clone(), accounts, expected_sol, "ratio"));
    } else {
        warn!(pool = %p.pool_address, "Skipping pool: dex_pool_accounts None (filter mismatch)");
    }
}

// find_best_buy_pool (Zeile 2255–2271): analog
```

**Vorteil:** Kein Panic, nur Skip + Warnung bei inkonsistenten Daten.

---

### 3. Zeile 2239: `Pubkey::from_str(SOL_MINT).unwrap()`

**Kontext:** `SOL_MINT` ist eine bekannte Konstante – Parse kann nur bei Tippfehler scheitern.

**Maßnahme:**
```rust
// Vorher:
let sol_mint_pubkey = Pubkey::from_str("So11111111111111111111111111111111111111112").unwrap();

// Nachher:
const SOL_MINT_STR: &str = "So11111111111111111111111111111111111111112";
let sol_mint_pubkey = match Pubkey::from_str(SOL_MINT_STR) {
    Ok(pk) => pk,
    Err(e) => {
        error!(error = %e, "Invalid SOL_MINT constant - hardcoded fallback failed");
        anyhow::bail!("Invalid SOL_MINT constant");
    }
};
```

**Alternative:** Konstante `sol_mint_pubkey` einmal beim Laden definieren, dann kein Parse nötig.

---

### 4. Zeile 4524: `execs.first().unwrap()`

**Kontext:** `for (mint, execs) in buys_by_mint.iter()` mit `if execs.is_empty() { continue }` – also nie leer.

**Maßnahme:**
```rust
let first_exec = match execs.first() {
    Some(e) => e,
    None => {
        warn!(mint = %mint, "buys_by_mint: execs empty despite guard (defensive skip)");
        continue;
    }
};
```

---

### 5. Zeile 4977: `ctx_clone.nats.as_ref().unwrap()`

**Kontext:** `if ctx.nats.is_some()` vor dem `tokio::spawn` – also nur gesetzt, wenn NATS existiert.

**Maßnahme:**
```rust
if let Some(nats) = ctx_clone.nats.as_ref() {
    tokio::spawn(async move {
        match pool_cache_sync::bootstrap_pool_cache_from_jetstream(nats, &ctx_clone.live_pool_cache).await {
            // ...
        }
    });
} else {
    let _ = tx.send(None);
}
```

**Änderung:** `if ctx.nats.is_some()` durch `if let Some(nats) = ctx.nats.as_ref()` ersetzen und `nats` in den Spawn übergeben – dann kein `unwrap` mehr nötig.

---

## Reihenfolge der Umsetzung

1. **Phase 1 (schnell):** #5 (NATS) – einfach umstellbar, keine Logikänderung.
2. **Phase 2 (wichtig):** #2 (dex_pool_accounts) – 4 Stellen, defensiv mit Skip.
3. **Phase 3:** #1 (pool_list), #4 (first_exec) – defensive Guards.
4. **Phase 4:** #3 (SOL_MINT Parse) – optional, da Konstante.

---

## Tests

- Unit-Tests weiterhin mit `expect` in Tests – dort sind Panics akzeptabel.
- Manuelle Prüfung: `cargo test -p ironcrab --bin momentum-bot` nach jeder Phase.
- Optional: Fuzzing/Property-Tests mit `None`/leeren Collections, um neue Panics zu finden.

---

## Entscheidungsrecord

- Keine neuen `unwrap`/`expect` im Runtime-Pfad.
- Bei unerwarteten Zuständen: `warn`/`error` + Skip oder `bail`, kein Panic.
- Testcode bleibt unverändert (außer expliziter Anpassungen).
