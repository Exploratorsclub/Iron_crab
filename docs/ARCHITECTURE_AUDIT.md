# IronCrab Architektur-Audit – Konsolidierte Fassung

**Stand:** 2026-02-27 | **Quellen:** main (ARCHITECTURE_AUDIT_2026-02-07) + arch-audit-tsw (ARCHITECTURE_AUDIT_2026-02-23) | **Korrekturen 2026-02-21:** PumpFun + Raydium load_pool_from_rpc_fallback – nur Cold Path, Hot Path erledigt | **2026-02-27:** P3 #11 Vault-Balances – allow_rpc_on_miss, Hot Path RPC-frei

> Dieses Dokument ist die einzige aktuelle Architektur-Audit-Quelle. Enthält die **vollständige** Revert-Analyse, alle RPC-Matrix-Tabellen, DEX-Details, BUG-Beschreibungen, **CrossDexHandler PumpFun-Befund** (tsw) sowie SSOT- und Cherry-Pick-Empfehlungen.

---

## Inhaltsverzeichnis

1. [Kontext und Legende](#1-kontext-und-legende)
2. [REVERT-ANALYSE: Verlorene sinnvolle Änderungen](#2-revert-analyse-verlorene-sinnvolle-änderungen)
3. [RPC-ANALYSE: Hot Path vs. Geyser-Ersetzbarkeit](#3-rpc-analyse-hot-path-vs-geyser-ersetzbarkeit)
4. [Single Source of Truth (SSOT)](#4-single-source-of-truth-ssot)
5. [Logik-Bugs und Architektur-Probleme](#5-logik-bugs-und-architektur-probleme)
6. [Zusammenfassung RPC-Calls im Hot Path](#6-zusammenfassung-rpc-calls-im-hot-path)
7. [Neue Erkenntnisse 2026-02-23 vs. 2026-02-07](#7-neue-erkenntnisse-2026-02-23-vs-2026-02-07)
8. [Cherry-Pick- und Priorisierte Empfehlungen](#8-cherry-pick--und-priorisierte-empfehlungen)
9. [Architektur-Regeln (Referenz)](#9-architektur-regeln-referenz)

---

## 1. Kontext und Legende

### 1.1 Kontext

Systematisches Audit aller RPC-Calls im Codebase mit Fokus auf:
- Hot-Path Latenz (Momentum-Buy, Arb, Sell)
- Geyser-First Architektur-Verstöße
- Killswitch/Liquidation Zuverlässigkeit
- Logik-Bugs und Inkonsistenzen
- **Single Source of Truth** — Verletzungen und Doppelquellen

### 1.2 Update 2026-02-11 / Revert vom 2026-02-09

Am 2026-02-09 wurde der Branch auf `e341c04b` zurückgesetzt (Hard-Reset), weil Änderungen aus 18 Commits ungewollt die Liquidation zerstört und Architekturprinzipien verletzt hatten. Danach wurden 6 gezielte Fixes für Liquidation und Grafana wieder hinzugefügt.

| Metrik | Wert |
|--------|------|
| Revertete Commits | 18 (von `e341c04b` bis `b22bb0a9`) |
| Verlorene Zeilen (netto) | ~+1633 / -176 |
| Betroffene Dateien | 18 |
| Wieder hergestellte Commits | 6 |
| **Noch fehlende Änderungen** | ~1450 Zeilen in 15 Dateien |

### 1.3 Legende Schweregrade

| Symbol | Bedeutung |
|--------|-----------|
| **KRITISCH** | RPC im Hot-Path (Buy/Sell/Arb Pipeline) – verursacht direkte Latenz |
| **VERSTOSS** | RPC wo Geyser-Daten vorhanden sind/sein sollten |
| **SSOT** | Verletzung Single Source of Truth |
| **LOGIK** | Potenzieller logischer Fehler |
| **AKZEPTABEL** | Unvermeidlich (Simulation, TX-Send, Blockhash) |
| **BOOTSTRAP** | Einmalige Initialisierung beim Start |
| **COLD PATH** | RPC erlaubt (Liquidation, Manual, Cleanup) |
| **CLEANUP** | Post-Trade Housekeeping (niedrigere Priorität) |

---

## 2. REVERT-ANALYSE: Verlorene sinnvolle Änderungen

### A.1 — PumpSwap AMM: Geyser-First Integration (KRITISCH – sollte zurück)

**Dateien**: `pumpfun_amm.rs` (+261 Zeilen), `live_pool_cache.rs` (+56 Zeilen), `cross_dex_handler.rs` (+11 Zeilen)

**Was verloren ging**:
- `PumpFunAmmDex::new_with_cache()` — LivePoolCache-Anbindung für RPC-freies Quoting
- `quote_exact_in()` liest Reserves aus Geyser-Cache statt RPC (ZERO RPC Quote)
- `discover_pool_static()` prüft erst LivePoolCache für `pool_accounts` vor RPC-Discovery
- `LivePoolCache::get_pump_amm_reserves_by_base_mint()` — Cache-Zugriff für PumpAmm Reserves
- `LivePoolCache::get_pump_amm_pool_accounts_by_base_mint()` — Cache-Zugriff für Pool-Accounts
- `LivePoolCache::mark_pumpfun_complete_for_mint()` — 6005 → Mark als complete
- `try_parse_pool_static_from_market_account_inner()` mit prefetched data (vermeidet doppelten RPC)
- TX-History-Fallback für `load_pool_by_address` bei Token-2022/Non-Standard Pools
- Besseres Logging (`eprintln!` → `warn!()` mit structured fields)
- `CrossDexHandler` nutzt `new_with_cache()` statt `new()`

**Status (2026-02-23)**: ⚠️ `new_with_cache()` existiert wieder; CrossDexHandler und execution-engine nutzen es für pump_amm. Bei Cache-Miss erfolgt RPC-Fallback.

**Bewertung**: Priorität 1 für vollständige Cherry-Pick (noch Rest-RPC bei Miss).

### A.2 — Momentum-Bot: Bonding-Curve Exit Config (MITTEL – sollte zurück)

**Dateien**: `momentum_bot.rs` (+219 Zeilen), `config.rs` (+16 Zeilen)

**Was verloren ging**:
- Config-Felder: `bonding_curve_exit_enabled` (default: false), `bonding_curve_exit_threshold_bps` (default: 9800 = 98%)
- Hot-Reload via UI für beide Felder
- Hot-Reload für `exit_max_slippage_bps`
- `creator`-Feld auf `PositionTracker` und `PersistedPosition` (für korrekte PumpFun-Sells)
- Creator-Resolution Fallback: Position → TokenTracker
- DEX-Name-Normalisierung (`pumpswap`/`PumpFunAmm` → `pump_amm`) für execution-engine Kompatibilität
- Creator nur noch für `pumpfun` (Bonding Curve) required, nicht für `pump_amm` (PumpSwap AMM)
- Multi-Pool-Routing: 5min-Freshness-Filter entfernt (stale Pools sind valide für Exit)
- Sekundärer Fallback: Pools mit `dex_pool_accounts` aber ohne `trade_ratio`
- `register_pool()` bei Trade-Events (findet Pools die `PoolCreated` verpasst haben)
- `cleanup_old_trackers()` bewahrt Trackers für offene Positionen (Lock-Order-Fix)

**Status (2026-02-21)**: ✅ BEHOBEN — Config `bonding_curve_exit_*`, Creator auf PositionTracker, `normalize_dex_for_execution_engine`, `resolve_authoritative_creator` und Hot-Reload sind implementiert.

**Bewertung**: War Priorität 2 für Cherry-Pick; jetzt implementiert.

### A.3 — Market-Data: Wallet-Tracking & JetStream Verbesserungen (MITTEL – sollte zurück)

**Datei**: `market_data.rs` (+331 Zeilen)

**Was verloren ging** (jetzt umgesetzt):
- Besseres Logging für Bootstrap Owner-Scan (non-zero counts, diagnostics)
- WSOL-Seeding in `TrackedWallet` beim Bootstrap (für korrekte WSOL-Anzeige in Grafana)
- Initiales `WalletBalanceUpdate` nach Bootstrap (statt erst beim nächsten Geyser-Event)
- PumpAmm `pool_accounts` werden IMMER im PoolCacheUpdate propagiert (nicht nur bei Creator)
- `pool_accounts` Fallback: *interner* Fallback innerhalb von market-data (Geyser-Parse leer → eigener LivePoolCache)
- SELL → JetStream `WalletBalanceSnapshot(0)` schreiben + ATA untracking
- Bessere Fehler-Logs bei fehlendem `token_account`/`token_program` in ExecutionResult
- `TrackedWallet` als `Arc<TrackedWallet>` für delayed re-publish task: **obsolet** — war Workaround für Core-NATS-Race; JetStream ersetzt dies vollständig

**Status (2026-02-21)**: ✅ IMPLEMENTIERT — Alle Punkte umgesetzt (außer obsoletem Arc/delayed re-publish).

**Bewertung**: Priorität 2 für Cherry-Pick; jetzt implementiert.

### A.4 — Execution-Engine: Liquidation-Robustheit (TEILWEISE ZURÜCK)

**Datei**: `execution_engine.rs`

**Was wieder hergestellt wurde** (6 Commits nach Revert):
- ✅ RPC Wallet-Scan Fallback für Liquidation (`rpc_wallet_scan_for_liquidation`)
- ✅ JetStream → RPC Fallback bei leeren/stale Snapshots
- ✅ LockManager Seeding mit RPC-Balances
- ✅ Liquidation-Routing: Multi-Pool first, PumpFun last
- ✅ `side` in ExecutionResult Metadata
- ✅ Liquidation-Sells bypassen `sell_token_balance` Preflight
- ✅ `PumpFunAmmDex::new_with_cache()` Verwendung in Liquidation (2026-02-21)
- ✅ `emit_sim_failed_decision()` gibt `Err` zurück für Retry-Detection
- ✅ 6005-Retry-Mechanismus (2026-02-25): Bei BondingCurveComplete auf pumpfun → automatisch Retry mit PumpSwap AMM
- ✅ SELL → token_account + token_program in ExecutionResult Metadata für market-data ATA-Tracking (Momentum-Sells, 2026-02-21)

**Entfällt** (bewusst nicht umgesetzt):
- `AVAILABLE_TRADING_CAPITAL_LAMPORTS` — SOL-Fallback-Anzeige im WSOL-Panel bereits behoben; Dashboard zeigt korrekt.

**Status**: ✅ IMPLEMENTIERT — Kern-Liquidation, 6005-Retry, SELL-Metadata für market-data ATA-untracking.

### A.5 — Pump.fun Bonding Curve: SELL bei migrierten Tokens (SINNVOLL – sollte zurück)

**Datei**: `pumpfun.rs` (+11 Zeilen)

**Was verloren ging**:
- Bei SELL: Wenn `real_token_reserves == 0 UND real_sol_reserves == 0` aber `virtual_reserves > 0` → Quote ablehnen statt stale Quote zu generieren
- Alte Logik: `warn!` + trotzdem Quote generieren → führte zu 6023 on-chain Fehler
- Neue Logik: `info!` + `return Ok(None)` → Multi-Pool-Routing findet PumpSwap AMM

**Status (2026-02-23)**: ✅ BEHOBEN — Guard in `pumpfun.rs` (Zeile 867-879) und `quote_calculator.rs` (Zeile 400-407) aktiv.

**Bewertung**: War Priorität 1 für Cherry-Pick; jetzt implementiert.

### A.6 — TX-Builder: Cache-Capped min_out für Pump.fun BUY (SINNVOLL)

**Datei**: `tx_builder.rs` (+29 Zeilen)

**Was verloren ging**:
- Bei Pump.fun BUY: `min_out` wird mit frischem Cache-Quote gekappt
- Vermeidet Error 6002 ("Too much SOL required") wenn sich die Bonding Curve zwischen Intent-Publish und TX-Build verschoben hat

**Status (2026-02-21)**: ✅ BEHOBEN — FIX-28 in tx_builder.rs: min_out wird aus Cache berechnet und mit Intent-Wert gekappt.

**Bewertung**: War Priorität 2 für Cherry-Pick; jetzt implementiert.

### A.7 — Metrics: `available_trading_capital_lamports` (NICE-TO-HAVE)

**Datei**: `metrics.rs` (+6 Zeilen)

**Was verloren ging**: Neue Prometheus-Metrik `available_trading_capital_lamports` mit klarerem Namen für Grafana

**Status**: ⛔ ENTFÄLLT — SOL-Fallback-Anzeige im WSOL-Panel wurde behoben; Dashboard zeigt WSOL und Wallet-Gesamtbalance korrekt. Keine weitere Metrik erforderlich.

### A.8 — Dokumentation & Scripts (TEILWEISE RELEVANT)

**Verlorene Dateien**:
- `docs/WSOL_DASHBOARD_AND_BC_EXIT_PLAN.md` — Plan-Dokument für WSOL-Dashboard und BC-Exit
- `docs/JETSTREAM_POSITION_RECONCILIATION_ANALYSIS.md` — JetStream-Analyse
- `scripts/check_wallet.sh` — Diagnose-Script

**Status**: Docs können rekonstruiert werden; Scripts waren diagnostisch

---

## 3. RPC-ANALYSE: Hot Path vs. Geyser-Ersetzbarkeit

### 3.1 EXECUTION ENGINE (`src/bin/execution_engine.rs`)

#### AKZEPTABEL – Simulation & TX-Sending

| Zeile | Call | Bewertung |
|-------|------|-----------|
| ~6971, ~7001 | `get_latest_blockhash()` in `simulate_transaction()` | AKZEPTABEL – Simulation braucht Blockhash |
| ~7040 | `simulate_transaction_with_config()` | AKZEPTABEL – Simulation ist Pflicht (simulate-gated) |
| ~6122, ~7102-7104 | `get_latest_blockhash_retry()` in `send_transaction_rpc/with_fallback` | AKZEPTABEL – TX-Send braucht Blockhash |
| ~7148 | `send_transaction_with_config()` | AKZEPTABEL – Finale TX-Übermittlung |

#### BOOTSTRAP

| Zeile | Call | Bewertung |
|-------|------|-----------|
| ~3693 | `get_latest_blockhash()` beim Start | BOOTSTRAP – Einmaliger Healthcheck |

#### AKZEPTABEL (Cold Path – Liquidation)

| Zeile | Call | Bewertung |
|-------|------|-----------|
| ~1299-1391 | `get_token_accounts_by_owner()` in `rpc_wallet_scan_for_liquidation()` | AKZEPTABEL – RPC-Fallback für manuelle Liquidation (Cold Path) |

#### VERSTOSS (Cold Path, aber dokumentiert)

| Zeile | Call | Problem | Geyser-Alternative |
|-------|------|---------|---------------------|
| ~2071-2088 | `get_token_accounts_by_owner()` in `cleanup_wallet_after_liquidation()` | RPC-Scan aller Token-Accounts nach Liquidation | Wallet-Snapshots aus market-data/JetStream |
| ~2092 | `get_account(&wsol_ata)` in `cleanup_wallet_after_liquidation()` | WSOL-Check per RPC | WSOL-Status aus Geyser/WalletSnapshot |
| ~2347 | `get_account(&token_account_pk)` in Manual-Burn-Job | Token-Account per RPC bei manueller Burn-Anfrage | Account-Daten aus LivePoolCache oder WalletSnapshot |
| ~2400 | `get_token_decimals_or_default()` in Manual-Burn-Job | Mint-Decimals per RPC | Aus WalletSnapshot/LivePoolCache |
| ~2420 | `get_account(&bc)` in Manual-Burn-Job | Bonding-Curve-Check per RPC für Route-Validation | LivePoolCache hat Bonding-Curve-State |

**Hinweis**: Cleanup/Manual Burn = Cold Path; RPC per Architektur erlaubt. Für zuverlässiges Schließen aller leeren ATAs ist autoritativer on-chain-Zustand erforderlich.

---

### 3.2 DEX-MODULE – KRITISCHSTE VERSTÖSSE

#### `pumpfun.rs` – Pump.fun Bonding Curve

| Zeile | Call | Bewertung | Aufrufer |
|-------|------|-----------|----------|
| 309 | `get_account_retry(bonding_curve)` in `fetch_bonding_curve()` | **AKZEPTABEL (Cold Path)** | Nur Tests / direkte Aufrufe |
| 322 | `get_account(bonding_curve)` in `fetch_bonding_curve_fast()` | **AKZEPTABEL (Cold Path)** | Nur via `quote_exact_in` |
| 572 | `fetch_bonding_curve_fast()` in `quote_exact_in_with_fallback()` | **AKZEPTABEL (Cold Path)** | Wird nirgends aufgerufen (Dead Code) |
| 1112 / 1124 | `get_account_retry(&bonding_curve)` in `build_swap_ix_async` | **AKZEPTABEL (Cold Path)** | tx_builder übergibt immer `Some(creator)` → Fallback praktisch nie |

**Status (2026-02-21 Korrektur):** Alle PumpFun-RPC-Aufrufe liegen ausschließlich im **Cold Path**:
- `quote_exact_in`: Nur aufgerufen von Liquidation (run_liquidation_job), Manual Burn (Route-Exists-Check), sell_all_keyless. CrossDexHandler schließt PumpFun aus Arb aus → kein Hot-Path-Aufruf.
- `build_swap_ix_async` RPC-Fallback: tx_builder verlangt Creator in Intent-Metadata und übergibt `Some(creator)` – Fallback wird nicht getroffen.
- **Keine Änderung erforderlich** – RPC im Cold Path ist architekturgerecht. Entfernen würde Liquidation für reine BC-Token brechen.

#### 3.2.1 CrossDexHandler: PumpFun Bonding Curve aus Arb entfernt ✅ 2026-02

**Status:** BEHOBEN – PumpFun (Bonding Curve) vollständig aus CrossDexHandler/Arb entfernt.

**Begründung:** Tokens auf der Bonding Curve haben keine anderen Pools → keine Arbitrage-Möglichkeit. arb-strategy lehnt pumpfun bereits ab. CrossDexHandler initialisiert PumpFun nicht mehr; alle pumpfun-spezifischen Arb-Pfade entfernt.

**Priorität:** War P1 – durch vollständige Entfernung gelöst (kein SSOT-Konflikt mehr).

#### `pumpfun_amm.rs` – PumpSwap AMM

| Zeile | Call | Problem | Geyser-Alternative |
|-------|------|---------|---------------------|
| 378-465 | **Eigener `rpc_call()` Wrapper** | ARCHITEKTUR-VERSTOSS – Eigene HTTP-RPC-Implementierung parallel zum offiziellen RPC-Client | — |
| 308-310 | `get_token_accounts_by_owner_with_filter()` | **KRITISCH** – Token-Account-Discovery per RPC | Wallet-ATAs aus Geyser-Subscription |
| 388, 483, 542 | `rpc_get_account_owner_*`, `get_multiple_accounts` | Pool-Discovery bei Cache-Miss | LivePoolCache `pool_accounts` / `get_pump_amm_reserves` |
| 638, 659 | `get_multiple_accounts` (global_config) | PDA/Config-Fetch | LivePoolCache |
| 1843, 1921 | `get_account_opt_retry`, `get_account_retry` | Pool/ATA-Fetch im Build-Pfad | LivePoolCache |
| 1388, 1824, 1875, 2044 | `rpc_call_tx_history` (getSignaturesForAddress, getTransaction) | VERSTOSS – Transaction-History per RPC | Geyser-Transactions |

**Status:** `new_with_cache()` existiert; CrossDexHandler und execution-engine nutzen es. Bei Cache-Miss → RPC-Fallback.

#### `orca.rs` – Orca Whirlpool

| Zeile | Call | Problem | Geyser-Alternative |
|-------|------|---------|---------------------|
| 440 / 455 | `get_multiple_accounts([vault_a, vault_b])` in `load_reserves_if_needed()` | **KRITISCH** – Vault-Balances bei Cache-Miss | LivePoolCache `OrcaWhirlpoolState` |
| 1372 / 1387 | `get_multiple_accounts(&[tick_arrays])` in `build_swap_ix_async` | **KRITISCH** – Tick-Array-Validierung per RPC | Geyser oder pre-cached tick arrays |

**Status:** Orca nutzt LivePoolCache; bei Miss → statische Reserves (kein RPC im Hot Path laut FIX-F). Tick-Arrays können weiterhin RPC erfordern.

#### `raydium.rs` – Raydium AMM

| Zeile | Call | Bewertung | Geyser-Alternative |
|-------|------|-----------|---------------------|
| 194 | `get_account_retry(pool_address)` in `load_pool_from_rpc_fallback()` | AKZEPTABEL (Cold Path) | ERLEDIGT – nur Liquidation; Hot Path lehnt ab |
| 1264 / 1276 | `get_account_retry(&market_id)` in `fetch_and_populate_serum_accounts` | **KRITISCH** – Serum-Market im Hot Path | Cache oder Bootstrap |
| 1324-1325 / 1336-1337 | `get_token_account_balance()` in `fetch_and_update_reserves()` | **ERLEDIGT P3#11** | Geyser-Vault-Updates → LivePoolCache |

**Status:** Umbenennung load_pool_from_geyser → load_pool_from_rpc_fallback (2026-02-21). Vault-Balances (2026-02-27): `allow_rpc_on_miss` – Hot Path (tx_builder, cross_dex_handler) mit `false` → Cache-Miss = Err, kein RPC. Cold Path (Liquidation, Burn): `true` → RPC-Fallback. Raydium RPC-Retries: 20×500ms → 3×300ms (korrigiert).

#### `meteora_dlmm.rs` – Meteora DLMM

| Zeile | Call | Problem | Geyser-Alternative |
|-------|------|---------|---------------------|
| 240 | `get_account(pool_addr)` in `update_reserve_balances()` | **ERLEDIGT P3#11** – Pool-Fetch nur Cold Path | LivePoolCache hat Meteora-State |
| 269-270 | `get_account_retry(&reserve_x/y)` | **ERLEDIGT P3#11** – Vault-Balances nur Cold Path | Geyser trackt diese Accounts |
| 480 | `get_account_retry(pool_address)` | Pool-Fetch im Build-Pfad | LivePoolCache |

**Status:** `allow_rpc_on_miss` (2026-02-27). Hot Path: Cache-Miss = Err. Cold Path: RPC-Fallback erlaubt.

#### `raydium_cpmm.rs` – Raydium CPMM

| Zeile | Call | Problem | Geyser-Alternative |
|-------|------|---------|---------------------|
| 237-238 | `get_account_retry(&vault_0/1)` | **ERLEDIGT P3#11** – Vault-Balances nur Cold Path | Geyser → LivePoolCache |

**Status:** `allow_rpc_on_miss` (2026-02-27). Hot Path: Cache-Miss = Err. Cold Path (account_janitor): RPC-Fallback erlaubt.

---

### 3.3 TX-INFRASTRUKTUR

#### `tx_builder.rs`

| Zeile | Call | Problem | Geyser-Alternative |
|-------|------|---------|---------------------|
| 218 | `get_account(pool_id)` in `fetch_orca_from_rpc()` | **KRITISCH** – Orca-Whirlpool-Fallback im TX-Build-Pfad | LivePoolCache (`CachedPoolState::Orca`) |
| 523 | `load_pool_from_rpc_fallback()` Raydium | **AKZEPTABEL** – Nur bei ExecutionMevB (Liquidation). Hot Path lehnt bei Cache-Miss ab → kein RPC. | ✅ ERLEDIGT |
| 1518 | `load_pool_by_address()` Multi-hop Meteora | **KRITISCH** – Pool-Fetch per RPC | LivePoolCache |

**Revert-Impact:** Cache-capped `min_out` für Pump.fun BUY fehlt (A.6) – Error 6002 Risiko.

#### `tx_sender.rs`, `tpu_client.rs`

| Zeile | Call | Bewertung |
|-------|------|-----------|
| tx_sender 459 | `send_transaction_with_config()` | AKZEPTABEL – RPC-Fallback in TPU → Jito → RPC Chain |
| tpu_client 151, 211 | `get_slot()` | AKZEPTABEL – Slot-Query für Leader-Schedule |

#### Arbitrage

**Status:** `solana::execution::ExecutionEngine` (Monolith-Artefakt mit `get_balance_retry`) wurde entfernt. Produktiver Arb-Flow nutzt CrossDexHandler + LockManager (kein RPC für Balance-Check).

| Modul | Call | Bewertung |
|-------|------|-----------|
| arbitrage.rs 315, 328 | `get_latest_blockhash()`, `simulate_transaction()` | AKZEPTABEL – Simulation (nur in Tests/Discovery) |

---

### 3.4 WALLET & TOKEN UTILS

#### `token_utils.rs`

| Zeile | Call | Problem | Geyser-Alternative |
|-------|------|---------|---------------------|
| 13 | `get_token_supply(mint)` | VERSTOSS – Mint-Decimals per RPC | Geyser liefert `TokenMintInfo` mit Decimals |
| 18 | `get_account(mint)` (Fallback) | VERSTOSS – Dasselbe als Fallback | LivePoolCache oder Geyser-Mint-Subscription |
| 33, 37 | `get_token_supply()` + `get_account()` | VERSTOSS – Gleiche Logik | Gleiche Lösung |

**Status (2026-02-07):** ✅ BEHOBEN — token_utils wird ausschließlich im Cold Path aufgerufen. Hot Path nutzt mint_infos/TokenMintInfo bzw. post_token_balances aus Geyser.

#### `wallet.rs`

| Zeile | Call | Problem | Geyser-Alternative |
|-------|------|---------|---------------------|
| 211 | `get_balance()` | VERSTOSS im Hot-Path / AKZEPTABEL für Utility | Geyser-Balance-Tracking |
| 220 | `get_account(mint)` für Token-Programm-Erkennung | VERSTOSS | Geyser-Mint-Info |
| 268 | `get_account(&ata)` für ATA-Existenz-Check | VERSTOSS wenn im Hot-Path | Geyser-Account-Subscription |
| 325, 364, 449, 567 | `get_latest_blockhash()` + `send_and_confirm_transaction()` | AKZEPTABEL – TX-Sending |
| 385 | `get_account(&to_ata)` | VERSTOSS | Geyser-Account-Subscription |

#### `wsol_manager.rs`

| Zeile | Call | Problem | Geyser-Alternative |
|-------|------|---------|---------------------|
| 501 | `get_balance()` | VERSTOSS – SOL-Balance per RPC | Geyser-Wallet-Tracking |
| 530 | `get_token_account_balance()` | VERSTOSS – WSOL-Balance per RPC | Geyser-Wallet-Tracking |
| 846, 895 | `get_latest_blockhash()` + `send_and_confirm_transaction()` | AKZEPTABEL – Wrap/Unwrap TX-Sending |

#### `account_janitor.rs`

| Zeile | Call | Bewertung |
|-------|------|-----------|
| 618, 834, 1075 | `get_latest_blockhash_retry()` + `send_and_confirm_transaction()` | AKZEPTABEL – Housekeeping-TXs |

---

### 3.5 SONSTIGE BINARIES

- **sell_all.rs / sell_all_keyless.rs**: Emergency-Tools – RPC akzeptabel (Cold Path).
- **market_data.rs** (~676, ~800): `get_multiple_accounts(&keys)` — BOOTSTRAP beim Start vor Geyser-Subscription.

---

### 3.6 AKZEPTABEL (Zusammenfassung)

- execution_engine: simulate, send_transaction, get_latest_blockhash
- Liquidation, cleanup_wallet, Manual Burn, sell_all, market_data Bootstrap: COLD PATH
- **pumpfun.rs**: Alle RPC-Calls nur im Cold Path – kein Hot-Path-Aufruf
- **raydium.rs** `load_pool_from_rpc_fallback`: Nur Cold Path (Liquidation); Hot Path lehnt bei Cache-Miss ab – erledigt
- account_janitor, wsol_manager: TX-Sending

---

## 4. Single Source of Truth (SSOT)

### 4.1 Pool-Daten: MASTER/SLAVE korrekt ✅

| Komponente | Rolle | Quelle | Status |
|------------|------|--------|--------|
| market-data | MASTER | Geyser → LivePoolCache, publiziert PoolCacheUpdate | ✅ |
| execution-engine | SLAVE | JetStream PoolCacheUpdate → LivePoolCache | ✅ |
| momentum-bot | SLAVE | JetStream PoolCacheUpdate → LivePoolCache | ✅ |

**Dokumentation:** `pool_cache_sync.rs`, `jetstream.rs`

### 4.2 SSOT-Verletzungen

| Problem | Status |
|---------|--------|
| **CrossDexHandler: PumpFun** | ✅ Entfernt – PumpFun BC nicht für Arb geeignet (keine anderen Pools) |
| Pool-Matching (FIX-38) | ✅ Eingehalten – `update_position_price()` prüft `source_pool == position.pool` |
| PumpSwap AMM quote_mint hardcodiert (`dex_parser.rs:952`) | ⚠️ Potenziell bei non-SOL-PumpSwap-Pools; Risiko gering |
| Meteora DLMM / Raydium CPMM quote_mint | ✅ Behoben – `extract_quote_mint` / vault-mint-basiert |

### 4.3 Invarianten-Checkliste (INVARIANTS.md)

- [ ] Kein RPC im Hot Path? → ⚠️ PumpSwap P3#12 erledigt (allow_rpc_on_miss); Rest: Tick-Arrays, Serum
- [x] Pool-Matching bei Preis-Updates? → ✅ Ja
- [x] tokens_per_sol-Konvention? → ✅ Ja
- [x] Simulation vor jedem Send? → ✅ Ja
- [x] Decision Record pro Intent? → ✅ Ja
- [x] Keine Keys außer in execution-engine? → ✅ Ja

---

## 5. Logik-Bugs und Architektur-Probleme

### BUG A: Killswitch-Liquidation – Token werden übersprungen

**Problem:** Bei `run_liquidation_job()` gibt es mehrere Pfade wo Token übersprungen werden: `min_out_sol.is_none()`, Creator fehlt im Cache, `pool_accounts_v1_for_base_mint()` gibt `None`.

**Status:** ✅ TEILWEISE BEHOBEN — Liquidation versucht Multi-Pool zuerst, PumpFun als Fallback. 6005-Retry bei BondingCurveComplete implementiert (2026-02-25).

### BUG B: `load_pool_from_geyser()` in `raydium.rs` macht RPC

**Zeile ~194:** Funktion hieß „from_geyser“, machte aber RPC. Umbenannt in `load_pool_from_rpc_fallback()`.

**Status:** ERLEDIGT (2026-02-21). Umbenannt zu `load_pool_from_rpc_fallback()`. Aktueller Stand: Hot Path ruft die Funktion nicht auf – build_tx_plan mit allow_rpc_fallback=false lehnt bei Raydium-Cache-Miss ab. Cold Path (Liquidation) nutzt RPC – architekturgerecht.

### BUG C: PumpFunAmmDex hat eigene RPC-Infrastruktur

**Zeilen 378-465:** Eigener `rpc_call()` HTTP-Client.

**Status:** ✅ ERLEDIGT (2026-02-28, P3 #12) — allow_rpc_on_miss: Hot Path (CrossDexHandler) false, Cold Path (Liquidation) true. RPC-Code als Cold Path only dokumentiert.

### BUG D: Token-Decimals per RPC

**Status:** ✅ BEHOBEN — token_utils nur im Cold Path. Hot Path nutzt Geyser/mint_infos.

### BUG E: `cleanup_wallet_after_liquidation()` macht RPC

**Status:** ✅ AKZEPTIERT (by design) — Cold Path; autoritativer on-chain-Zustand erforderlich.

### BUG F: Orca Reserve-Fetching 5min TTL

**Status:** ✅ BEHOBEN — LivePoolCache einzige Reserve-Quelle im Hot Path (AUDIT_F).

### BUG G: Stale JetStream Wallet-Snapshots → Ghost Open Positions

**Status:** ✅ BEHOBEN (Commit `43941752`) — Zero-balance Overrides für nicht abgedeckte Mints.

### BUG H: Meteora DLMM / Raydium CPMM hardcoded SOL quote_mint

**Status:** ✅ BEHOBEN (2026-02-23) — `extract_quote_mint` bzw. vault-mint-basierte Ableitung. PumpSwap AMM: quote_mint weiterhin hardcodiert (typischerweise SOL-Paare).

### BUG I (vormals G): PumpFun SELL migriert

**Status:** ✅ BEHOBEN — Guard für `real_reserves == 0` in `pumpfun.rs` und `quote_calculator.rs` aktiv.

---

### Weitere logische Inkonsistenzen (2026-02-23)

| Thema | Beschreibung |
|-------|--------------|
| load_pool_from_geyser() / Raydium | ✅ BEHOBEN — Umbenannt zu load_pool_from_rpc_fallback; Hot Path lehnt ab; keine RPC-Calls im Hot Path |
| Arbitrage get_balance_retry | Zusätzliche Latenz; LockManager-Alternative fehlt |

---

## 6. Zusammenfassung RPC-Calls im Hot Path

### Gesamtzählung

| Modul | Anzahl Hot Path | Schweregrad |
|-------|-----------------|-------------|
| pumpfun.rs | 0 | — (nur Cold Path: Liquidation, Manual Burn, sell_all) |
| pumpfun_amm.rs | 6+ | KRITISCH |
| orca.rs | 2 | KRITISCH (Vaults ok; Tick-Arrays offen) |
| raydium.rs | 1 | KRITISCH (Vaults erledigt P3#11; Serum-Market offen) |
| raydium_cpmm.rs | 0 | — (Vaults erledigt P3#11) |
| meteora_dlmm.rs | 1 | KRITISCH (Vaults erledigt P3#11; Pool-Fetch offen) |
| tx_builder.rs | 4 | KRITISCH (Fallbacks) |
| cross_dex_handler.rs | 0 | — (PumpFun aus Arb entfernt) |
| **TOTAL** | **14+** | — (Vault-Balances Raydium/CPMM/Meteora P3#11 erledigt) |

### Geschätzte Latenz-Auswirkung

Typischer **Momentum-Buy** (PumpFun BC):
1. Quote → `quote_calculator` aus LivePoolCache (kein RPC im Hot Path)
2. PumpSwap AMM Quote = +500–3000ms RPC bei Cache-Miss (sollte: 0ms mit LivePoolCache)
3. TX-Build → Creator aus Intent-Metadata (tx_builder verlangt ihn); kein PumpFun-RPC
4. Simulation = +100–500ms (unvermeidlich)
5. TX-Send = +50–400ms (unvermeidlich)

*Hinweis: PumpFun `quote_exact_in`/`fetch_bonding_curve_fast` wird nur von Cold Path (Liquidation, Manual Burn, sell_all) aufgerufen – nicht von Momentum-Buy.*

| Metrik | Wert |
|--------|------|
| **Aktuelle Gesamtlatenz** | ~1500–8000ms |
| **Optimierte Latenz (nur Sim+Send)** | ~200–900ms |
| **Potenzial** | 3–8× schneller |

---

## 7. Neue Erkenntnisse 2026-02-23 vs. 2026-02-07

| Thema | 2026-02-07 | 2026-02-23 |
|-------|------------|------------|
| CrossDexHandler PumpFun Cache | Nicht erwähnt | **NEU:** PumpFun erhält keinen pool_cache |
| **PumpFun RPC Hot vs. Cold** | Als KRITISCH/Hot Path markiert | **KORRIGIERT 2026-02-21:** Caller-Analyse: nur Cold Path. Keine Änderung. |
| **Raydium load_pool_from_rpc_fallback** | Priorität 3 #10 als offen | **ERLEDIGT 2026-02-21:** Hot Path lehnt bei Cache-Miss ab (allow_rpc_fallback=false). RPC nur bei Liquidation – architekturgerecht. |
| **Raydium/RaydiumCpmm/Meteora Vault-Balances** | Priorität 3 #11 als offen | **ERLEDIGT 2026-02-27:** `allow_rpc_on_miss` – Hot Path lehnt bei Cache-Miss ab; Cold Path RPC-Fallback. |
| Pool-Matching (FIX-38) | Dokumentiert | Bestätigt eingehalten |
| Meteora/Raydium CPMM quote_mint | BUG H offen | Implementiert (extract_quote_mint) |
| PumpFun SELL migriert | A.5 fehlt | ✅ Guard implementiert (real_reserves) |
| Raydium RPC-Retries | 20 × 500ms | 3 × 300ms (korrigiert) |
| PumpSwap quote_mint hardcode | BUG H | Bestätigt, PumpSwap typischerweise SOL |
| Arbitrage get_balance | VERSTOSS | ✅ Erledigt – solana::execution entfernt (Dead-Code) |

---

## 8. Cherry-Pick- und Priorisierte Empfehlungen

### Priorität 1 — Sofort (Latenz + Korrektheit)

| # | Problem | Fix | Risiko |
|---|---------|-----|--------|
| ~~1~~ | ~~CrossDexHandler: PumpFun~~ | ✅ Erledigt – PumpFun aus Arb entfernt (keine Arb-Möglichkeit auf BC) | — |
| ~~2~~ | ~~Arbitrage get_balance_retry~~ | ✅ Erledigt – solana::execution (Monolith-Dead-Code) entfernt | — |

### Priorität 2 — Bald (Robustheit)

| # | Quelle | Beschreibung | Risiko |
|---|--------|-------------|--------|
| ~~3~~ | ~~tx_builder.rs~~ | ✅ A.6 Cache-capped min_out – implementiert | — |
| ~~4~~ | ~~momentum_bot.rs~~ | ✅ A.2 Creator-Handling, DEX-Normalisierung – implementiert | — |
| ~~5~~ | ~~market_data.rs~~ | ✅ A.3 WSOL-Seeding, PumpAmm pool_accounts – implementiert | — |
| ~~6~~ | ~~Raydium~~ | ✅ BUG B behoben – Umbenennung + Hot-Path RPC-Eliminierung | — |
| ~~7~~ | ~~PumpFun Creator~~ | ✅ Erledigt – Creator zuverlässig aus LivePoolCache (token_mint-Anreicherung, BondingCurveUpdate-Fallback) | — |
| ~~8~~ | ~~metrics.rs~~ | A.7 available_trading_capital_lamports – entfällt (Dashboard ok) | — |

### Priorität 3 — Langfristig (Architektur)

| # | Problem | Fix |
|---|---------|-----|
| ~~9~~ | ~~PumpFun BC-Fetch per RPC~~ | **N/A** – Nur Cold Path (Liquidation, Manual Burn, sell_all). Keine Änderung – RPC architekturgerecht. Entfernen würde Liquidation brechen. |
| ~~10~~ | ~~Raydium `load_pool_from_rpc_fallback()`~~ | ✅ ERLEDIGT – Kein RPC im Hot Path. Hot Path lehnt bei Cache-Miss ab. RPC nur im Cold Path (Liquidation) – erlaubt. |
| ~~11~~ | ~~Orca/Raydium/Meteora Vault-Balances~~ | ✅ ERLEDIGT (2026-02-27) – `allow_rpc_on_miss` in Raydium, RaydiumCpmm, MeteoraDlmm. Hot Path: Cache-Miss = Err. Cold Path: RPC-Fallback. Orca Vaults bereits frei; Tick-Arrays separat. |
| ~~12~~ | ~~pumpfun_amm eigene RPC-Infrastruktur~~ | ✅ ERLEDIGT (2026-02-28): allow_rpc_on_miss, Cold Path dokumentiert |
| 13 | Token-Decimals (token_utils) | Globalen Decimals-Cache aus Geyser-Mint-Info |
| 14 | A.2, A.3 | Creator-Handling, WSOL-Seeding zurückholen |

### Cherry-Pick aus Revert (Priorität 1–2)

| # | Quelle | Beschreibung | Risiko |
|---|--------|-------------|--------|
| 1 | pumpfun_amm.rs, live_pool_cache.rs, cross_dex_handler.rs | PumpSwap AMM Geyser-First (vollständig) | Niedrig |
| 2 | pumpfun.rs | SELL migriert – bereits implementiert | — |
| 3 | execution_engine.rs | emit_sim_failed_decision() → Err für 6005-Retry | Niedrig |

---

## 9. Architektur-Regeln (Referenz)

Dokumentiert in `.cursor/rules/ironcrab-core.mdc`:

- **Hot Path** (Buy/Sell/Arb): Geyser-First, KEINE blocking RPC-Calls. Ziel: <1s Latenz.
- **Cold Path** (Liquidation, Cleanup): RPC-Calls akzeptabel. Sicherheit > Geschwindigkeit.
- **Bootstrap**: RPC beim Start akzeptabel für initiale Daten.
- **Simulation + TX-Send**: Immer RPC (unvermeidlich).
- **Nie** RPC aus Cold Paths entfernen um zu „optimieren“.
- **Nie** RPC in Hot Paths ohne explizite Freigabe.

---

## Anhang: REVERT – Kurzreferenz

| Kategorie | Status |
|-----------|--------|
| A.1 PumpSwap Geyser-First | ⚠️ new_with_cache vorhanden, RPC bei Miss |
| A.2 Bonding-Curve Exit | ✅ Implementiert |
| A.3 Market-Data Wallet-Tracking | ✅ Implementiert |
| A.4 Liquidation (6005-Retry, SELL-Metadata) | ✅ Implementiert |
| A.5 PumpFun SELL migriert | ✅ Behoben |
| A.6 Cache-capped min_out | ✅ Behoben |
| A.7 available_trading_capital_lamports | ⛔ Entfällt (Dashboard ok) |

---

## Referenzen

- `INVARIANTS.md`, `KNOWN_BUG_PATTERNS.md`, `ORDER_LIFECYCLE.md`
- `AUDIT_F_ORCA_RESERVES_IMPLEMENTATION_PLAN.md`, `AUDIT_E_IMPLEMENTATION_PLAN.md`
- `.cursor/rules/ironcrab-core.mdc`
- `ARCHITECTURE_AUDIT_2026-02-07.md`, `ARCHITECTURE_AUDIT_2026-02-23.md` (Quellen, deprecated)

---

*Konsolidiert: 2026-02-23. Korrekturen 2026-02-21: PumpFun + Raydium load_pool_from_rpc_fallback – nur Cold Path, Hot Path erledigt. 2026-02-27: P3 #11 Vault-Balances – allow_rpc_on_miss implementiert. 2026-02-28: P3 #12 PumpSwap allow_rpc_on_miss, Cold Path dokumentiert.*
