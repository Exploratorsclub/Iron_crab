# Handoff: Positions-Counter + Liquidation Fix

Implementiere alle vier Fixes in der angegebenen Reihenfolge.
Pruefe nach jedem Fix: `cargo fmt`, `cargo clippy -- -D warnings`, `cargo test`.
Am Ende muessen ALLE drei Checks bestanden sein.

Lese ZUERST docs/INVARIANTS.md und docs/KNOWN_BUG_PATTERNS.md.

---

## Fix A: open_positions als abgeleiteter Wert (WICHTIGSTER FIX)

### Ziel
Eliminiere den separaten `open_positions: AtomicUsize` Counter in `ExecutionContext`.
Stattdessen: `get_open_positions()` zaehlt non-zero Token-Balances im LockManager.

### Schritt 1: Neuer Method in src/storage/locks.rs

Fuege diese Methode zu `impl LockManager` hinzu (nach `available_token_balance`):

```rust
/// Count the number of token mints with non-zero available balance.
/// Used as Single Source of Truth for open positions count,
/// replacing the error-prone dual-path AtomicUsize counter.
pub fn count_non_zero_token_balances(&self) -> usize {
    self.available_tokens
        .read()
        .values()
        .filter(|&&balance| balance > 0)
        .count()
}
```

### Schritt 2: Aendere get_open_positions() in src/bin/execution_engine.rs

VORHER (suche nach `fn get_open_positions`):
```rust
fn get_open_positions(&self) -> usize {
    self.open_positions
        .load(std::sync::atomic::Ordering::Relaxed)
}
```

NACHHER:
```rust
fn get_open_positions(&self) -> usize {
    self.lock_manager.count_non_zero_token_balances()
}
```

### Schritt 3: Entferne alle open_positions fetch_add / fetch_sub

Suche in execution_engine.rs nach ALLEN Stellen die `ctx.open_positions.fetch_add` oder `ctx.open_positions.fetch_sub` oder `ctx.open_positions.load` aufrufen. Es gibt mehrere Stellen:

**A) Execution Result Handler (ca. Zeile 8062-8128):**
Entferne die Counter-Modifikation. Der OPEN_POSITIONS_GAUGE wird stattdessen am Ende des Handlers aktualisiert:

VORHER (BUY-Pfad):
```rust
TradeSide::Buy => {
    let prev = ctx.open_positions.fetch_add(1, Ordering::Relaxed);
    OPEN_POSITIONS_GAUGE.store((prev + 1) as u64, Ordering::Relaxed);
    // ... LockManager token balance update bleibt ...
}
```
NACHHER (BUY-Pfad):
```rust
TradeSide::Buy => {
    // ... LockManager token balance update bleibt UNVERAENDERT ...
}
```
Entferne NUR die zwei Zeilen `let prev = ctx.open_positions.fetch_add(1, ...)` und `OPEN_POSITIONS_GAUGE.store(...)`.
Der gesamte LockManager-Code darunter (fill_raw, add_available_token_balance etc.) bleibt!

VORHER (SELL-Pfad):
```rust
TradeSide::Sell => {
    let prev = ctx.open_positions.load(Ordering::Relaxed);
    if prev > 0 {
        ctx.open_positions.fetch_sub(1, Ordering::Relaxed);
        OPEN_POSITIONS_GAUGE.store((prev - 1) as u64, Ordering::Relaxed);
    } else {
        OPEN_POSITIONS_GAUGE.store(0, Ordering::Relaxed);
    }
    // ... clear_available_token_balance bleibt ...
}
```
NACHHER (SELL-Pfad):
```rust
TradeSide::Sell => {
    // ... clear_available_token_balance bleibt UNVERAENDERT ...
}
```
Entferne die open_positions.load/fetch_sub/GAUGE Zeilen. Der LockManager clear-Code bleibt!

WICHTIG: Am ENDE des gesamten match-Blocks (nach Buy und Sell), fuege hinzu:
```rust
OPEN_POSITIONS_GAUGE.store(ctx.get_open_positions() as u64, Ordering::Relaxed);
```

**B) WalletBalanceSnapshot Handler (ca. Zeile 5801-5832):**
Entferne ALLE open_positions fetch_add/fetch_sub/load Zeilen und die zugehoerigen OPEN_POSITIONS_GAUGE.store Aufrufe.
Der `ctx.lock_manager.set_available_token_balance(...)` Aufruf BLEIBT — das ist die eigentliche Datenquelle.
Entferne auch die zugehoerigen info!() Logging-Aufrufe die `open_positions` erwaehnen (oder aendere sie so dass sie count_non_zero_token_balances() nutzen).

**C) Bootstrap / Init (ca. Zeile 4535-4557, 4796):**
Suche nach `ctx.open_positions.store(...)` oder `open_positions.store(...)`. Entferne diese.
Der periodische OPEN_POSITIONS_GAUGE Update (Zeile 5887) aendert sich zu:
```rust
OPEN_POSITIONS_GAUGE.store(ctx.get_open_positions() as u64, Ordering::Relaxed);
```
(Das ist vermutlich bereits so — pruefe und behalte es bei.)

### Schritt 4: Entferne das Feld open_positions aus ExecutionContext

Suche in der `ExecutionContext` struct-Definition nach:
```rust
open_positions: std::sync::atomic::AtomicUsize,
```
Entferne dieses Feld.

Suche dann nach ALLEN Stellen wo `open_positions:` im Struct-Initializer vorkommt (es gibt mindestens zwei: den produktiven Init und den Test-Init). Entferne das Feld dort.

### Schritt 5: StateSnapshot Kompatibilitaet

`StateSnapshot.open_positions` BLEIBT erhalten fuer die Persistenz.
Beim SPEICHERN: `open_positions: ctx.get_open_positions()`.
Beim LADEN: der Wert wird ignoriert (LockManager ist Single Source of Truth).
Pruefe `save_state()` und `load_state()` und stelle sicher dass keine Zuweisungen an den entfernten AtomicUsize mehr existieren.

---

## Fix B1: cashback_enabled RPC-Fallback

### Ziel
In `build_swap_ix_async_with_slippage` (src/solana/dex/pumpfun.rs) soll `cashback_enabled` bei Cache-Miss per RPC aufgeloest werden (Liquidation = Cold Path, RPC erlaubt).

### Aenderung

Suche in pumpfun.rs nach allen Stellen wo `cashback_enabled` aus dem Cache gelesen wird mit `unwrap_or(false)`:

```rust
let cashback = self
    .get_bonding_curve_from_cache(&bonding_curve)
    .map(|s| s.cashback_enabled)
    .unwrap_or(false);
```

Es gibt 3 solche Stellen (fuer fallback_creator, cached_creator, und get_creator_from_cache Pfade).

NACHHER: Ersetze jede dieser 3 Stellen durch:

```rust
let cashback = self
    .get_bonding_curve_from_cache(&bonding_curve)
    .map(|s| s.cashback_enabled)
    .unwrap_or_else(|| {
        if let Some(state) = self.get_bonding_curve_from_rpc_sync(&bonding_curve) {
            state.cashback_enabled
        } else {
            false
        }
    });
```

ABER: `build_swap_ix_async_with_slippage` ist async. `fetch_bonding_curve_fast` ist auch async.
Daher muss der Fallback ebenfalls async sein.

Da die Funktion bereits async ist, kannst du den Fallback so schreiben:

```rust
let cashback = match self.get_bonding_curve_from_cache(&bonding_curve) {
    Some(state) => state.cashback_enabled,
    None => {
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
    }
};
```

Ersetze ALLE drei `unwrap_or(false)` Stellen. Die `fetch_bonding_curve_fast` Methode existiert bereits — sie liest das BondingCurve-Account per RPC und parst den State.

### Zusaetzlich: pool_cache_sync.rs

In `src/execution/pool_cache_sync.rs` Zeile 227, aendere den Kommentar:
```rust
cashback_enabled: false, // JetStream metadata may not have it; safe default — resolved at TX build time via RPC
```

---

## Fix B2: Liquidation als tokio::spawn

### Ziel
`run_liquidation_job` darf die Main-Loop nicht blockieren.

### Aenderung in src/bin/execution_engine.rs

Suche nach (ca. Zeile 5639-5652):
```rust
if active && liquidate_positions {
    // Check if liquidation is already in progress BEFORE spawning
    if ctx.liquidation_in_progress.load(Ordering::SeqCst) {
        warn!("KillSwitch: Liquidation already in progress, ignoring duplicate request");
    } else {
        let slippage = max_slippage_bps.unwrap_or(9900);
        let ttl = ttl_ms.unwrap_or(60_000);
        ExecutionContext::run_liquidation_job(
            Arc::clone(&ctx),
            slippage,
            ttl,
            reason,
        )
        .await;
    }
}
```

NACHHER:
```rust
if active && liquidate_positions {
    if ctx.liquidation_in_progress.load(Ordering::SeqCst) {
        warn!("KillSwitch: Liquidation already in progress, ignoring duplicate request");
    } else {
        let slippage = max_slippage_bps.unwrap_or(9900);
        let ttl = ttl_ms.unwrap_or(60_000);
        let ctx_spawn = Arc::clone(&ctx);
        tokio::spawn(async move {
            ExecutionContext::run_liquidation_job(
                ctx_spawn,
                slippage,
                ttl,
                reason,
            )
            .await;
        });
    }
}
```

HINWEIS: `reason` ist `Option<String>` und muss in den Closure moved werden. Da `reason` bereits owned ist (String, nicht &str), sollte der Move ohne Probleme funktionieren. Pruefe den Typ und fuege ggf. `.clone()` hinzu falls noetig.

---

## Fix B3: Liquidation-Retry fuer fehlgeschlagene Token

### Ziel
Nach dem ersten Liquidations-Durchlauf sollen fehlgeschlagene Token erneut versucht werden.

### Aenderung in src/bin/execution_engine.rs

In `run_liquidation_job`, nach dem ersten Intent-Processing-Loop (ca. Zeile 2620-2654) und VOR dem cleanup:

```rust
// === Retry Phase: Re-scan wallet for tokens still present ===
// First pass may have failed for some tokens (stale quotes, RPC issues, sim failures).
// Wait for first-pass TXs to confirm, then re-scan and retry failed tokens.
info!("Liquidation first pass complete. Waiting before retry scan...");
tokio::time::sleep(Duration::from_secs(10)).await;
#[cfg(unix)]
maybe_ping_watchdog();

let retry_rpc_accounts = ctx
    .rpc
    .rpc
    .get_token_accounts_by_owner(&owner, TokenAccountsFilter::ProgramId(token_program_id))
    .await
    .unwrap_or_default();

let mut retry_count = 0u32;
for ta in &retry_rpc_accounts {
    let parsed = match &ta.account.data {
        UiAccountData::Json(parsed) => parsed,
        _ => continue,
    };
    let info = match parsed.parsed.get("info") {
        Some(v) => v,
        None => continue,
    };
    let mint_str = info
        .get("mint")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let balance_str = info
        .get("tokenAmount")
        .and_then(|v| v.get("amount"))
        .and_then(|v| v.as_str())
        .unwrap_or("0");
    let balance_raw: u64 = balance_str.parse().unwrap_or(0);

    if balance_raw == 0 || mint_str == SOL_MINT || mint_str == WSOL_MINT {
        continue;
    }

    retry_count += 1;
    warn!(
        mint = %mint_str,
        balance_raw,
        "LIQUIDATION RETRY: Token still in wallet after first pass"
    );
}

if retry_count > 0 {
    warn!(
        remaining_tokens = retry_count,
        "Liquidation: {} tokens still in wallet — full retry would require re-routing (logged for diagnostics)",
        retry_count
    );
}
```

Dies ist erstmal nur Diagnostik (Logging). Ein vollstaendiger Retry-Loop wuerde die gleiche Routing-Logik wiederholen und ist komplex. Das Logging gibt uns aber die Information ob Token tatsaechlich verbleiben.

---

## Zusammenfassung der zu aendernden Dateien

| Datei | Aenderung |
|-------|-----------|
| `src/storage/locks.rs` | + `count_non_zero_token_balances()` Methode |
| `src/bin/execution_engine.rs` | Fix A: Counter ableiten, fetch_add/sub entfernen. Fix B2: tokio::spawn. Fix B3: Retry-Diagnostik |
| `src/solana/dex/pumpfun.rs` | Fix B1: cashback_enabled RPC-Fallback |
| `src/execution/pool_cache_sync.rs` | Kommentar-Update |

## Pruefung
Am Ende MUESSEN bestehen:
- `cargo fmt --check`
- `cargo clippy -- -D warnings`
- `cargo test`
