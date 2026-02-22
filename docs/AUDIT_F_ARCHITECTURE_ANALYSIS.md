# Audit-F: Orca Reserve Caches – Architektur-Analyse

## Deine Fragen

1. Werden RPC-Calls nur gemacht, wenn LivePoolCache keine Daten hat?
2. Stammt der SQLite-Cache vom alten Monolith?
3. Warum haben beide Caches 5-Minuten-TTL?
4. Warum werden überhaupt extra Caches benötigt?
5. Verletzt das das Single-Source-of-Truth-Prinzip?

## Kurzantwort

Ja – es gibt ein grundlegendes Architekturproblem. Es existieren mehrere konkurrierende „Quellen“, und legacy Caches widersprechen Single Source of Truth.

---

## Aktueller Zustand

### Drei Reserve-Quellen in `load_reserves_if_needed`

| # | Quelle | Population | TTL | In Produktion |
|---|--------|------------|-----|---------------|
| 1 | **LivePoolCache** | Geyser → market-data → JetStream | Keine (immer aktuell) | JA |
| 2 | **SQLite (OrcaReserveCache)** | RPC → Write bei Fetch | 5 min | NEIN – `cache_path` ist überall `None` |
| 3 | **In-Memory (pool.cached_reserves)** | LivePoolCache (inject) oder RPC | 5 min | JA |

### RPC wird ausgelöst wenn:

- LivePoolCache den Pool nicht hat **oder**
- Vault-Balances im LivePoolCache `None` sind **und**
- SQLite leer/abgelaufen (effektiv immer, weil nicht genutzt) **und**
- In-Memory abgelaufen (älter als 5 min)

### SQLite – Legacy und tot

- `Orca::new_with_cache(rpc, cache_path, live_pool_cache)` wird überall mit `cache_path: None` aufgerufen:
  - `execution_engine.rs`, `cross_dex_handler.rs`, `tx_builder.rs`
- Config hat `enable_reserve_cache` und `cache_path`, aber niemand übergibt sie an Orca.
- SQLite ist praktisch Dead Code.

### Warum 5 Minuten TTL?

- Historische Konstante aus der Zeit vor Geyser.
- Damals: RPC → in-memory + SQLite, TTL um RPC-Last zu begrenzen.
- Heute: LivePoolCache ist die vorgesehene Quelle; TTL auf einer Kopie macht architektonisch keinen Sinn.

---

## Single Source of Truth

**Sollzustand:**

```
Geyser (on-chain) → market-data → LivePoolCache (MASTER)
                                        │
                                        ▼
                              JetStream (PoolCacheUpdate)
                                        │
                    ┌───────────────────┼───────────────────┐
                    ▼                   ▼                   ▼
            execution_engine    momentum_bot         arb_strategy
                    │                   │                   │
                    └───────────────────┴───────────────────┘
                                        │
                              LivePoolCache (SLAVE)
                                        │
                              Einzige Quelle für Reserves
```

**Istzustand:**

- LivePoolCache = Source 1 (korrekt)
- SQLite = Source 2 (RPC-abgeleitet, aktuell ungenutzt)
- In-Memory = Kopie von 1 oder 2, mit 5-min-TTL

Damit gibt es mehrere konkurrierende Quellen und Verstöße gegen Single Source of Truth.

---

## Architektur-Fix (Vorschlag)

### 1. LivePoolCache als einzige Quelle

- Bei gesetztem `live_pool_cache`:
  - LivePoolCache-Hit → Reserves nutzen
  - LivePoolCache-Miss → kein RPC, stattdessen `(pool.reserve_base, pool.reserve_quote)` (statische Fallbacks)

### 2. SQLite entfernen (bzw. deaktivieren)

- Entweder komplett entfernen oder nur für Cold-Path-Tools (z.B. sell_all_keyless ohne LivePoolCache) optional lassen.
- In Hot-Path- und Standard-Setups nicht mehr nutzen.

### 3. In-Memory-TTL entfernen

- `pool.cached_reserves` und `last_reserve_fetch` werden von `inject_cached_orca_state` aus LivePoolCache befüllt.
- Statt lokaler TTL: immer erst LivePoolCache prüfen.
- Die lokale Kopie dient nur als Spiegel; wenn LivePoolCache nichts hat, bringen 5 Minuten TTL keinen Vorteil.

### 4. Klare Regel für `load_reserves_if_needed`

```
wenn live_pool_cache.is_some():
    LivePoolCache prüfen
    wenn vollständige Daten → nutzen
    sonst → (pool.reserve_base, pool.reserve_quote), kein RPC

wenn live_pool_cache.is_none():  // Cold Path (z.B. sell_all_keyless)
    RPC-Fallback erlaubt
```

---

## Fazit

- Du hast recht: Es handelt sich um ein Architekturproblem.
- Die zusätzlichen Caches (SQLite, in-memory mit TTL) stammen aus der Vor-Geyser-Zeit.
- Heute ist nur LivePoolCache die vorgesehene Single Source of Truth.
- Der Audit-F-Plan sollte um diese Architekturbereinigung ergänzt werden: SQLite und in-memory-TTL zurückbauen, LivePoolCache als einzige Reserve-Quelle im Hot Path durchsetzen.
