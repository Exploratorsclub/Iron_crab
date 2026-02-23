# IronCrab Architektur-Audit – 2026-02-23

> **⚠️ DEPRECATED:** In `ARCHITECTURE_AUDIT.md` (konsolidiert mit main 2026-02-07) zusammengeführt. Bitte `docs/ARCHITECTURE_AUDIT.md` verwenden.

---

## Kontext

Vollständiges Architektur-Audit der gesamten Codebasis mit Fokus auf:
1. **Architektur-Konformität** — Einhaltung von Hot-Path-Regeln (Geyser-First, keine RPC)
2. **RPC im Hot Path** — Stellen, die durch Geyser/LivePoolCache ersetzt werden können
3. **Single Source of Truth (SSOT)** — Verletzungen und Doppelquellen
4. **Logische Fehler** — Inkonsistenzen und potenzielle Bugs

**Referenzdokumente:** `INVARIANTS.md`, `ORDER_LIFECYCLE.md`, `KNOWN_BUG_PATTERNS.md`, `.cursor/rules/ironcrab-core.mdc`, `ARCHITECTURE_AUDIT_2026-02-07.md`

---

## Legende Schweregrade

| Symbol | Bedeutung |
|--------|-----------|
| **KRITISCH** | RPC im Hot-Path (Buy/Sell/Arb Pipeline) — verursacht direkte Latenz |
| **VERSTOSS** | RPC wo Geyser-Daten vorhanden sind/sein sollten |
| **SSOT** | Verletzung Single Source of Truth |
| **LOGIK** | Potenzieller logischer Fehler |
| **AKZEPTABEL** | Unvermeidlich (Simulation, TX-Send, Blockhash) |
| **BOOTSTRAP** | Einmalige Initialisierung beim Start |
| **COLD PATH** | RPC erlaubt (Liquidation, Manual, Cleanup) |

---

## 1. RPC-ANALYSE: Hot Path vs. Geyser-Ersetzbarkeit

### 1.1 KRITISCH — RPC im Hot Path (durch Geyser ersetzbar)

#### A. Pump.fun Bonding Curve (`pumpfun.rs`)

| Zeile | Call | Problem | Geyser-Alternative |
|-------|------|---------|---------------------|
| 309 | `get_account_retry(bonding_curve)` in `fetch_bonding_curve()` | Bonding-Curve-Fetch bei Cache-Miss | `CachedPoolState::PumpFun` via LivePoolCache |
| 322 | `get_account(bonding_curve)` in `fetch_bonding_curve_fast()` | Sniping-Pfad, Timeout-Call | Geyser-Event beim Pool-Create |
| 1124 | `get_account_retry(&bonding_curve)` in `build_swap_ix_async` | Creator-Resolution RPC-Fallback | LivePoolCache hat Creator via `get_pumpfun_creator()` |

**Status:** LivePoolCache wird bereits für Quote genutzt (`get_bonding_curve_from_cache`), aber bei Cache-Miss → RPC. `build_swap_ix_async` hat Creator-RPC-Fallback.

#### B. PumpSwap AMM (`pumpfun_amm.rs`)

| Zeile | Call | Problem | Geyser-Alternative |
|-------|------|---------|---------------------|
| 308-310 | `get_token_accounts_by_owner_with_filter()` | Token-Account-Discovery per RPC | Wallet-ATAs aus Geyser-Subscription |
| 388, 483, 542 | `rpc_get_account_owner_*`, `get_multiple_accounts` | Pool-Discovery bei Cache-Miss | LivePoolCache `pool_accounts` / `get_pump_amm_reserves` |
| 638, 659 | `get_multiple_accounts` (global_config) | PDA/Config-Fetch | LivePoolCache |
| 1843, 1921 | `get_account_opt_retry`, `get_account_retry` | Pool/ATA-Fetch im Build-Pfad | LivePoolCache |

**Status:** `new_with_cache()` existiert; bei Cache-Miss erfolgt RPC-Fallback. CrossDexHandler und execution-engine nutzen `new_with_cache` wenn LivePoolCache vorhanden.

#### C. CrossDexHandler: PumpFun ohne LivePoolCache (NEU — LOGIK/VERSTOSS)

| Datei | Zeile | Problem |
|-------|-------|---------|
| `cross_dex_handler.rs` | 206 | `PumpFunDex::new(Arc::clone(&self.rpc), None)` — **hardcodiert None** für `live_pool_cache` |

**Analyse:** CrossDexHandler hat `pool_cache: Option<Arc<LivePoolCache>>` und nutzt ihn für Raydium, PumpFunAmm, Meteora, Orca — aber **nicht für PumpFun Bonding Curve**. Beim Arb-Swap-Build für `pumpfun` wird daher immer RPC-Fallback für Creator ausgelöst (`get_account_retry` in `build_swap_ix_async`).

**Fix:** `PumpFunDex::new(Arc::clone(&self.rpc), self.pool_cache.clone())` — Cache an PumpFun übergeben.

#### D. Orca Whirlpool (`orca.rs`)

| Zeile | Call | Problem | Geyser-Alternative |
|-------|------|---------|---------------------|
| 440 | `get_multiple_accounts(&[vault_a, vault_b])` | Vault-Balances bei Cache-Miss | LivePoolCache `OrcaWhirlpoolState` |
| 1372 | `get_multiple_accounts(&[tick_array_*])` | Tick-Array-Validierung im Build-Pfad | Geyser oder pre-cached tick arrays |

**Status:** Orca nutzt LivePoolCache; bei Miss → statische Reserves (kein RPC im Hot Path laut FIX). Tick-Arrays können weiterhin RPC erfordern.

#### E. Raydium (`raydium.rs`)

| Zeile | Call | Problem | Geyser-Alternative |
|-------|------|---------|---------------------|
| 194 | `get_account_retry(pool_address)` in `load_pool_from_geyser()` | Funktion heißt „from_geyser“, macht aber RPC (3×300ms) | Geyser-Account-Update direkt parsen |
| 1276 | `get_account_retry(&market_id)` in `fetch_and_populate_serum_accounts` | Serum-Market im Hot Path | Cache oder Bootstrap |
| 1336-1337 | `get_token_account_balance()` in `fetch_and_update_reserves()` | Vault-Balances per RPC | Geyser-Vault-Updates → LivePoolCache |

**Status:** LivePoolCache-Priorität implementiert; RPC nur bei Cache-Miss.

#### F. Raydium CPMM (`raydium_cpmm.rs`)

| Zeile | Call | Problem |
|-------|------|---------|
| 237-238 | `get_account_retry(&vault_0/1)` | Vault-Balances per RPC bei Cache-Miss |

**Status:** LivePoolCache-First; RPC-Fallback bei Miss.

#### G. Meteora DLMM (`meteora_dlmm.rs`)

| Zeile | Call | Problem |
|-------|------|---------|
| 240 | `get_account(pool_addr)` in `update_reserve_balances()` | Pool-Fetch für Vault-Adressen |
| 269-270 | `get_account_retry(&reserve_x/y)` | Vault-Balances per RPC |
| 480 | `get_account_retry(pool_address)` | Pool-Fetch im Build-Pfad |

**Status:** LivePoolCache-First; RPC bei Miss.

#### H. TX-Builder (`tx_builder.rs`)

| Zeile | Call | Problem |
|-------|------|---------|
| 218 | `get_account(pool_id)` in `fetch_orca_from_rpc()` | Orca-Pool-Fallback im TX-Build |
| 523, 1378 | `load_pool_from_geyser()` (Raydium) | Bis zu 3×300ms RPC-Retries |
| 1518 | `load_pool_by_address()` (Meteora Multi-hop) | Pool-Fetch per RPC |

**Status:** Fallback-Pfade; LivePoolCache wird bevorzugt.

#### I. Arbitrage (`execution.rs`)

| Zeile | Call | Problem |
|-------|------|---------|
| 129 | `get_balance_retry(&wallet.pubkey())` | Balance-Check vor Arb-Execution | LockManager/Geyser-Wallet-Snapshot |

**Status:** VERSTOSS — Wallet-Balance könnte aus JetStream/LockManager kommen.

---

### 1.2 AKZEPTABEL — RPC (unvermeidlich oder Cold Path)

| Modul | Call | Begründung |
|-------|------|------------|
| `execution_engine.rs` | `get_latest_blockhash()`, `simulate_transaction()`, `send_transaction` | Simulation + TX-Send |
| `execution_engine.rs` | `get_token_accounts_by_owner()` in `rpc_wallet_scan_for_liquidation` | Cold Path — Liquidation |
| `wallet.rs`, `wsol_manager.rs` | `get_balance`, `send_and_confirm` | Utility/Cold Path |
| `market_data.rs` | `get_multiple_accounts` beim Bootstrap | BOOTSTRAP |
| `account_janitor.rs` | `send_and_confirm_transaction` | Housekeeping |
| `sell_all.rs`, `sell_all_keyless.rs` | Diverse RPC | Cold Path — Emergency-Tools |

---

## 2. Single Source of Truth (SSOT) — Analyse

### 2.1 Pool-Daten: MASTER/SLAVE korrekt

| Komponente | Rolle | Quelle | Status |
|------------|------|--------|--------|
| market-data | MASTER | Geyser → LivePoolCache, publiziert PoolCacheUpdate | ✅ |
| execution-engine | SLAVE | JetStream PoolCacheUpdate → LivePoolCache | ✅ |
| momentum-bot | SLAVE | JetStream PoolCacheUpdate → LivePoolCache | ✅ |

**Dokumentation:** `pool_cache_sync.rs`, `jetstream.rs` — MASTER = market-data, SLAVEs = execution-engine + momentum-bot.

### 2.2 SSOT-Verletzungen

#### A. CrossDexHandler: PumpFun ohne PoolCache

**Problem:** CrossDexHandler verfügt über `pool_cache`, übergibt ihn aber nicht an PumpFunDex. Dadurch:
- PumpFun nutzt eigene Creator/Bonding-Curve-Quelle (RPC) statt LivePoolCache
- Arb-Pfad für pumpfun hat andere Datenquelle als Momentum-Pfad

**Konformität:** ❌ SSOT verletzt — zwei Quellen für PumpFun Creator/Bonding-Curve (Cache vs. RPC).

#### B. Pool-Matching bei Preis-Updates (FIX-38)

**Status:** ✅ Eingehalten — `update_position_price()` prüft `source_pool == position.pool` vor Update. Keine Wrong-Pool-Preisverschmutzung.

**Code:** `momentum_bot.rs:2801–2810` — `if pos.pool != pool { return; }`.

#### C. PumpSwap AMM: quote_mint hardcodiert

**Datei:** `dex_parser.rs:952`

```rust
let quote_mint = Pubkey::from_str("So11111111111111111111111111111111111111112").unwrap();
```

**Problem:** PumpSwap AMM unterstützt primär SOL-Paare; für TOKEN/USDC-Pools wäre quote_mint falsch. Aktuell nur theoretisch relevant, da PumpSwap typischerweise SOL-Quote nutzt.

**Status:** ⚠️ Potenzieller SSOT-Verstoß bei non-SOL-PumpSwap-Pools; Risiko gering.

#### D. Meteora DLMM / Raydium CPMM quote_mint

**Status:** ✅ BEHOBEN — `extract_quote_mint` bzw. vault-mint-basierte Ableitung in `dex_parser.rs` (Meteora ~1387, Raydium CPMM ~1476).

---

## 3. Logische Fehler und Inkonsistenzen

### 3.1 CrossDexHandler: PumpFun ohne LivePoolCache

**Beschreibung:** Siehe Abschnitt 1.1.C und 2.2.A.

**Auswirkung:** Erster Arb-Swap über PumpFun Bonding Curve löst Creator-RPC aus; Latenz + Inkonsistenz.

### 3.2 Arbitrage: get_balance_retry vor Execution

**Beschreibung:** `execution.rs:129` — RPC-Balance-Check vor Triangle-Arb.

**Auswirkung:** Zusätzliche Latenz; LockManager/Geyser-Snapshot wäre konsistenter.

### 3.3 load_pool_from_geyser() — irreführender Name

**Datei:** `raydium.rs:183`

**Problem:** Funktionsname suggeriert Geyser-Quelle, tatsächlich werden bis zu 3 RPC-Retries durchgeführt.

**Empfehlung:** Umbenennen in `load_pool_from_rpc_fallback()` oder Ähnliches.

### 3.4 PumpFun SELL: real_reserves == 0 Guard

**Status:** ✅ Implementiert — `pumpfun.rs:867–879` prüft `real_token_reserves`; bei Migrated-Token → `Ok(None)` für Multi-Pool-Routing.

---

## 4. Architektur-Konformität: Übersicht

### 4.1 Hot-Path-Regeln (I-4, I-7)

| Regel | Status | Anmerkung |
|-------|--------|-----------|
| HOT PATH: GEYSER-ONLY | ⚠️ Teilweise | RPC-Fallbacks bei Cache-Miss in PumpFun, PumpSwap, Orca, Raydium, Meteora |
| Keine blockierenden RPC im Hot Path | ⚠️ Teilweise | CrossDexHandler PumpFun, arbitrage get_balance |
| Latenz-Ziel <1s | ⚠️ Gefährdet | Mehrere RPC-Fallbacks addieren sich |

### 4.2 Pool-Matching (I-13)

| Regel | Status |
|-------|--------|
| Preis-Updates nur bei source_pool == position.pool | ✅ Eingehalten |

### 4.3 Invarianten-Checkliste

- [x] Kein RPC im Hot Path? → ⚠️ Noch RPC-Fallbacks
- [x] Pool-Matching bei Preis-Updates? → ✅ Ja
- [x] tokens_per_sol-Konvention? → ✅ Ja
- [x] Simulation vor jedem Send? → ✅ Ja
- [x] Decision Record pro Intent? → ✅ Ja
- [x] Keine Keys außer in execution-engine? → ✅ Ja

---

## 5. Zusammenfassung: Priorisierte Handlungsempfehlungen

### Priorität 1 — Sofort (Latenz + Korrektheit)

| # | Problem | Fix |
|---|---------|-----|
| 1 | CrossDexHandler: PumpFun ohne LivePoolCache | `PumpFunDex::new(rpc, self.pool_cache.clone())` in `cross_dex_handler.rs:206` |
| 2 | Arbitrage: get_balance_retry | LockManager/available_sol vor RPC prüfen; RPC nur als Fallback |

### Priorität 2 — Kurzfristig (Architektur)

| # | Problem | Fix |
|---|---------|-----|
| 3 | Raydium load_pool_from_geyser() | Umbenennen; ggf. Geyser-Account-Update direkt parsen |
| 4 | PumpFun build_swap: Creator-RPC | Sicherstellen, dass LivePoolCache Creator immer liefert (market-data) |
| 5 | Orca/Raydium/Meteora Tick-Array/Vault RPC | Geyser-Subscription für alle Hot-Path-Accounts prüfen |

### Priorität 3 — Langfristig

| # | Thema |
|---|-------|
| 6 | PumpSwap AMM: Vollständige LivePoolCache-Nutzung bei Pool-Discovery |
| 7 | PumpFun quote: Kein RPC bei Bonding-Curve-Cache-Miss (Geyser-Subscription sicherstellen) |
| 8 | Globaler Decimals-Cache aus Geyser (token_utils bereits LivePoolCache-First) |

---

## 6. Geänderte/Neue Erkenntnisse vs. ARCHITECTURE_AUDIT_2026-02-07

| Thema | 2026-02-07 | 2026-02-23 |
|-------|------------|------------|
| CrossDexHandler PumpFun Cache | Nicht erwähnt | **NEU:** PumpFun erhält keinen pool_cache |
| Pool-Matching (FIX-38) | Dokumentiert | Bestätigt eingehalten |
| Meteora/Raydium CPMM quote_mint | TODO | Implementiert (extract_quote_mint) |
| PumpSwap quote_mint hardcode | BUG H | Bestätigt, PumpSwap typischerweise SOL |
| Arbitrage get_balance | VERSTOSS | Bestätigt, LockManager-Alternative fehlt |

---

## 7. Anhang: RPC-Call-Übersicht (Grep-Referenz)

Für vollständige Auflistung siehe `grep`-Ergebnisse zu:
- `get_account`, `get_account_retry`, `get_account_opt_retry`
- `get_token_accounts_by_owner`, `get_multiple_accounts`
- `get_balance`, `get_balance_retry`
- `rpc_call` (pumpfun_amm)

**Dateien mit Hot-Path-RPC:** pumpfun.rs, pumpfun_amm.rs, orca.rs, raydium.rs, raydium_cpmm.rs, meteora_dlmm.rs, tx_builder.rs, execution.rs (arbitrage), cross_dex_handler.rs (PumpFun ohne Cache).
