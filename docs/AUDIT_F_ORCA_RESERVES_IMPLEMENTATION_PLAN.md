# Audit-F: Orca Reserve-Fetching – Implementierungsplan

## Kontext

**Audit-F** ([ARCHITECTURE_AUDIT_2026-02-07.md](ARCHITECTURE_AUDIT_2026-02-07.md), [BUGS_FIXES.md](BUGS_FIXES.md)):
- **Problem**: `load_reserves_if_needed()` – 5-Minuten-TTL, bei Cache-Miss RPC-Fallback. Bei 50+ Pools bis zu 50+ RPC-Calls.
- **Ort**: [src/solana/dex/orca.rs](src/solana/dex/orca.rs), Zeile 410–495

Detaillierte Architektur-Analyse: [AUDIT_F_ARCHITECTURE_ANALYSIS.md](AUDIT_F_ARCHITECTURE_ANALYSIS.md)

---

## Teil 1: Architekturbereinigung (Single Source of Truth)

### Problem

Aktuell existieren drei konkurrierende Reserve-Quellen – Verstoß gegen Single Source of Truth:

| Quelle | Population | Status |
|--------|------------|--------|
| LivePoolCache | Geyser → market-data → JetStream | Korrekte Single Source |
| SQLite (OrcaReserveCache) | RPC → Write bei Fetch | Legacy, **unbenutzt** (`cache_path` überall `None`) |
| In-Memory (`pool.cached_reserves`) | Kopie von LivePoolCache oder RPC, 5-min TTL | Redundant, arbiträre TTL |

### Maßnahmen

1. **SQLite entfernen** aus dem Hot Path:
   - `reserve_cache` wird bei gesetztem `live_pool_cache` ignoriert (oder komplett entfernt, da ohnehin nie genutzt).
   - Konstruktor-Aufrufe bleiben bei `cache_path: None`.

2. **In-Memory-TTL entfernen**:
   - Die Prüfung auf `pool.cached_reserves` mit 5-min TTL entfällt.
   - Alle Reserve-Lookups gehen ausschließlich über LivePoolCache (wenn vorhanden).
   - `cached_reserves` und `last_reserve_fetch` können als interne Spiegelung von LivePoolCache-Daten bleiben (für `inject_cached_orca_state`), werden aber in `load_reserves_if_needed` nicht mehr als eigene Cache-Schicht genutzt.

3. **Neue Lookup-Logik**:
   - LivePoolCache = einzige Quelle (wenn `live_pool_cache.is_some()`)
   - Cache-Miss → `(pool.reserve_base, pool.reserve_quote)` (kein RPC)
   - RPC nur wenn `live_pool_cache.is_none()` (Cold Path)

---

## Teil 2: Statische Reserves – Detaillierte Erklärung

### Was sind „statische Reserves“?

`pool.reserve_base` und `pool.reserve_quote` sind Felder der `OrcaPool`-Struct. Sie speichern die **zuletzt bekannten** Vault-Balances (Token-Mengen in den beiden Vault-Accounts des Pools). Diese Werte werden **nicht live geholt**, sondern stammen aus früheren Quellen – daher „statisch“.

### Woher kommen sie? (Datenfluss im Detail)

```
                    ┌─────────────────────────────────────────────────────────┐
                    │                  OrcaPool.reserve_base/quote             │
                    └─────────────────────────────────────────────────────────┘
                                               ▲
         ┌────────────────────────────────────┼────────────────────────────────────┐
         │                                    │                                      │
   ┌─────┴─────┐                    ┌────────┴────────┐                    ┌────────┴────────┐
   │ Parse     │                    │ inject_cached  │                    │ RPC (nur Cold   │
   │ Whirlpool │                    │ _orca_state    │                    │ Path)           │
   └─────┬─────┘                    └────────┬───────┘                    └────────┬────────┘
         │                                   │                                      │
   Whirlpool-Account                  LivePoolCache                          get_multiple_accounts
   enthält nur Vault-                 (Geyser → market-data)                  (Vault-Accounts)
   Adressen, keine Balances
         │                                   │                                      │
         ▼                                   ▼                                      ▼
   reserve_base = 0                    Echte Geyser-Daten                   Aktuelle on-chain
   reserve_quote = 0                   (vault_a_balance,                    Balances
                                       vault_b_balance)
```

| Herkunft | reserve_base / reserve_quote | Wann |
|----------|-----------------------------|------|
| **Whirlpool-Account-Parse** (z.B. `load_whirlpool`) | **0, 0** | Beim ersten Laden: Das Whirlpool-Konto enthält nur Vault-Adressen, keine Balances. Die Balances stehen in separaten Token-Accounts. |
| **inject_cached_orca_state** | Von LivePoolCache (Geyser) | Beim Bootstrap oder wenn execution_engine Orca-States aus dem Cache injiziert. |
| **RPC-Fetch** (aktuell) | Von `get_multiple_accounts` | Beim bisherigen RPC-Fallback – wird für Hot Path entfernt. |

### Typischer Ablauf bei Cache-Miss

1. Pool wurde per Whirlpool-Parse oder via `inject_cached_orca_state` angelegt.
2. **LivePoolCache-Miss**: Pool nicht im Cache oder Vault-Balances fehlen/0.
3. Fallback: `(pool.reserve_base, pool.reserve_quote)` – **ohne** weiteren RPC-Call.

In den meisten Fällen sind das **0, 0**, weil:
- neue Pools initial mit 0, 0 erstellt werden;
- bei gültigen Daten aus Geyser normalerweise LivePoolCache getroffen wird (kein Miss).

### Was passiert mit (0, 0)?

In `quote_exact_in` (orca.rs, ca. Zeile 905–906):

```rust
if rin == 0 || rout == 0 {
    return Ok(None);
}
```

→ Es wird **kein Quote** zurückgegeben (`Ok(None)`). Es findet **kein Handel** statt. Der Pool wird für diesen Request übersprungen.

### Funktioniert es, damit korrekt zu handeln?

**Kurz: Ja – im Sinne von „keine falschen Trades“.** Der Handel erfolgt korrekt, wenn LivePoolCache trifft; bei Cache-Miss mit statischen Reserves ist das Verhalten defensiv, aber sicher.

| Szenario | reserve_base/quote | Ergebnis | Korrektheit |
|----------|-------------------|----------|-------------|
| **LivePoolCache Hit** | Von Geyser (echte Balances) | Normale Quote, Trade möglich | ✅ Korrekt – echte Daten |
| **Cache-Miss, Pool frisch geparst** | (0, 0) | `Ok(None)` → kein Quote | ✅ Kein falscher Trade |
| **Cache-Miss, Pool aus inject** | Ehemalige Geyser-Werte (non-zero) | Quote möglich, aber evtl. leicht veraltet | ✅ Abgefangen durch `min_out` + Simulation |
| **Cache-Miss, Pool nie injiziert** | (0, 0) | `Ok(None)` → kein Quote | ✅ Kein falscher Trade |

**Warum sind falsche Trades ausgeschlossen?**

1. **(0, 0)**: `quote_exact_in` lehnt ab → kein Trade.
2. **(non-zero, veraltet)**: Die Simulation vor dem Senden prüft das Ergebnis; `min_out` schützt vor Slippage. Der Trade würde nur ausgeführt, wenn er unter den aktuellen Bedingungen noch gültig ist.
3. **LivePoolCache** ist die maßgebliche Quelle für den Hot Path – Geyser-Daten sind zeitnah und stimmen mit den Vault-Updates überein.

**Praktische Konsequenz**: Bei Cache-Miss wird entweder nicht gequotet (0,0) oder mit potenziell leicht veralteten, aber aus Geyser stammenden Werten. Im letzteren Fall sorgt die Simulation dafür, dass kein schlechter Trade durchgeht. Es können höchstens **Chancen verpasst** werden (Pool nicht gequotet), aber keine **fehlerhaften Trades** entstehen.

---

## Ist-Zustand (vor Bereinigung)

### Aktuelle Lookup-Reihenfolge in `load_reserves_if_needed`

1. **LivePoolCache** (Geyser): Wenn `vault_a_balance` und `vault_b_balance` beide `Some` → Rückgabe
2. **SQLite** (reserve_cache): De facto unbenutzt
3. **In-Memory** (`pool.cached_reserves`): 5-Minuten-TTL
4. **RPC-Fallback**: Architekturverstoß im Hot Path

### Geyser-Infrastruktur

- **market-data** subscribt zu Geyser für Token-Accounts (Vaults).
- Bei Vault-Update: `ctx.live_pool_cache.update_vault_balance()` + `PoolCacheUpdate::BalanceUpdated` an JetStream.
- **execution_engine** empfängt `PoolCacheUpdate` via JetStream und schreibt in den SLAVE LivePoolCache.
- `build_minimal_pool_state` setzt `vault_a_balance` und `vault_b_balance` aus `base_reserve`/`quote_reserve`.

---

## Umsetzungsstrategie (analog zu PumpFunAmmDex / Audit-C)

### Regel

**Wenn `live_pool_cache.is_some()` und Cache-Miss:**
- Kein RPC
- Rückgabe: `(pool.reserve_base, pool.reserve_quote)` (meist 0,0 → kein Quote)

**Wenn `live_pool_cache.is_none()` (Cold Path):**
- RPC-Fallback bleibt erlaubt

### Konkret

1. **LivePoolCache**: Wenn beide Vault-Balances `Some` und beide > 0 → nutzen.
2. **Cache-Miss bei gesetztem LivePoolCache**: Kein RPC, Rückgabe `(pool.reserve_base, pool.reserve_quote)`.
3. **SQLite-Schritt entfernen** (nicht mehr prüfen).
4. **In-Memory-TTL-Schritt entfernen** (nicht mehr prüfen).
5. **RPC** nur, wenn `live_pool_cache.is_none()`.

## Code-Änderungen

### Datei: [src/solana/dex/orca.rs](src/solana/dex/orca.rs)

**In `load_reserves_if_needed`:**

1. **LivePoolCache** als einzige Quelle (wenn gesetzt):
   - Hit (beide Vault-Balances Some und beide > 0): Rückgabe, Cache-Hit zählen.
   - Pool im Cache, aber Vault-Daten unvollständig/0: Rückgabe `(pool.reserve_base, pool.reserve_quote)`, kein RPC.

2. **SQLite-Prüfung entfernen** (Zeilen 424–431): Block komplett streichen.

3. **In-Memory-TTL-Prüfung entfernen** (Zeilen 433–444): Block komplett streichen.

4. **RPC-Fallback**:
   - Wenn `live_pool_cache.is_some()`: Kein RPC, Rückgabe `(pool.reserve_base, pool.reserve_quote)`.
   - Wenn `live_pool_cache.is_none()`: RPC wie bisher.

**Pseudocode der neuen Logik:**

```
fn load_reserves_if_needed:
  if let Some(lpc) = self.live_pool_cache {
      if let Some(Orca(state)) = lpc.get(pool_id) {
          if let (Some(va), Some(vb)) = (state.vault_a_balance, state.vault_b_balance) {
              if va > 0 && vb > 0 {
                  cache_hits++; return (va, vb);
              }
          }
          cache_misses++; return (pool.reserve_base, pool.reserve_quote);  // kein RPC
      }
      cache_misses++; return (pool.reserve_base, pool.reserve_quote);  // Pool nicht im Cache
  }
  // Cold Path: live_pool_cache == None
  match rpc.get_multiple_accounts(...) { ... }  // RPC erlaubt
```

### SQLite / OrcaReserveCache (optional)

- **Option A**: Code belassen, aber in `load_reserves_if_needed` nicht mehr aufrufen (bereits der Fall, da `reserve_cache` überall `None`).
- **Option B**: `reserve_cache`-Feld und `OrcaReserveCache`-Import komplett entfernen (größerer Refaktor, separater PR).

### Abnahmekriterien

1. **Hot Path**: Bei gesetztem LivePoolCache keine RPC-Calls; bei Miss `(pool.reserve_base, pool.reserve_quote)` (meist 0,0 → Ok(None)).
2. **Cold Path**: RPC-Fallback unverändert.
3. **LivePoolCache Hit**: Verhalten wie bisher.
4. `cargo check` und `cargo clippy` erfolgreich.

## Risiko

- **Kein Quote bei (0,0)**: Pool wird nicht gequotet, wenn Geyser-Daten fehlen – bewusst konservativ, kein falscher Trade.
- **Stale Quote** (theoretisch bei non-zero aus alter inject): Durch `min_out` und Simulation abgefangen.

## Dateien

| Datei | Änderung |
|-------|----------|
| [src/solana/dex/orca.rs](src/solana/dex/orca.rs) | `load_reserves_if_needed`: Nur LivePoolCache + statischer Fallback; SQLite- und In-Memory-TTL-Blöcke entfernen; RPC nur bei `live_pool_cache.is_none()` |
| [docs/BUGS_FIXES.md](docs/BUGS_FIXES.md) | Audit-F auf BEHOBEN setzen |
| [docs/ARCHITECTURE_AUDIT_2026-02-07.md](docs/ARCHITECTURE_AUDIT_2026-02-07.md) | BUG F Status aktualisieren |
