# IronCrab Architektur-Audit – 2026-02-07 (aktualisiert 2026-02-11)

## Kontext

Systematisches Audit aller RPC-Calls im Codebase mit Fokus auf:
- Hot-Path Latenz (Momentum-Buy, Arb, Sell)
- Geyser-First Architektur-Verstöße
- Killswitch/Liquidation Zuverlässigkeit
- Logik-Bugs und Inkonsistenzen

### Update 2026-02-11: Revert-Analyse

Am 2026-02-09 wurde der Branch auf `e341c04b` zurückgesetzt (Hard-Reset), weil Änderungen
aus 18 Commits (bis `b22bb0a9`) ungewollt die Liquidation zerstört und teilweise
Architekturprinzipien verletzt hatten. Danach wurden 6 gezielte Fixes für Liquidation
und Grafana-Dashboard wieder hinzugefügt. Dieses Dokument dokumentiert:

1. Welche **sinnvollen Änderungen** durch den Revert verloren gingen
2. Welche davon **wieder hergestellt** wurden
3. Aktueller **Status der RPC-Calls im Hot Path**
4. **Empfehlungen** für selektives Cherry-Picking

## Legende Schweregrade

| Symbol | Bedeutung |
|--------|-----------|
| **KRITISCH** | RPC im Hot-Path (Buy/Sell/Arb Pipeline) – verursacht direkte Latenz |
| **VERSTOSS** | RPC wo Geyser-Daten vorhanden sind/sein sollten |
| **AKZEPTABEL** | Unvermeidlich (Simulation, TX-Send, Blockhash) |
| **BOOTSTRAP** | Einmalige Initialisierung beim Start |
| **CLEANUP** | Post-Trade Housekeeping (niedrigere Priorität) |

---

## REVERT-ANALYSE: Verlorene sinnvolle Änderungen

### Übersicht Revert

| Metrik | Wert |
|--------|------|
| Revertete Commits | 18 (von `e341c04b` bis `b22bb0a9`) |
| Verlorene Zeilen (netto) | ~+1633 / -176 |
| Betroffene Dateien | 18 |
| Wieder hergestellte Commits | 6 (Liquidation + Grafana Fixes) |
| **Noch fehlende Änderungen** | ~1450 Zeilen in 15 Dateien |

### A. Verlorene Änderungen nach Kategorie

#### A.1 — PumpSwap AMM: Geyser-First Integration (KRITISCH – sollte zurück)

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

**Status**: ❌ FEHLT — PumpSwap AMM macht aktuell RPC-Calls im Hot Path

**Bewertung**: Dies war die wichtigste Änderung zur Reduzierung der Hot-Path-Latenz. Ohne diese Integration macht jeder PumpSwap-AMM-Quote mindestens 2-3 RPC-Calls (Pool-Discovery + Vault-Reserves). **Priorität 1 für Cherry-Pick.**

#### A.2 — Momentum-Bot: Bonding-Curve Exit Config (MITTEL – sollte zurück)

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

**Status**: ❌ FEHLT — Bonding-Curve Exit ist nicht aktiv, Creator-Handling und DEX-Normalisierung fehlen

**Bewertung**: Die eigentliche Exit-LOGIK (Check ob curve_progress >= threshold → emit sell) war noch NICHT implementiert, nur die Config-Infrastruktur und das Creator-Handling. Trotzdem sind die Creator-/DEX-Normalisierung und Pool-Registry-Verbesserungen wichtig für Exit-Zuverlässigkeit. **Priorität 2 für Cherry-Pick.**

#### A.3 — Market-Data: Wallet-Tracking & JetStream Verbesserungen (MITTEL – sollte zurück)

**Datei**: `market_data.rs` (+331 Zeilen)

**Was verloren ging**:
- Besseres Logging für Bootstrap Owner-Scan (non-zero counts, diagnostics)
- WSOL-Seeding in `TrackedWallet` beim Bootstrap (für korrekte WSOL-Anzeige in Grafana)
- Initiales `WalletBalanceUpdate` nach Bootstrap (statt erst beim nächsten Geyser-Event)
- PumpAmm `pool_accounts` werden IMMER im PoolCacheUpdate propagiert (nicht nur bei Creator)
- `pool_accounts` Fallback aus MASTER LivePoolCache wenn Geyser-Parse noch keine hat
- SELL → JetStream `WalletBalanceSnapshot(0)` schreiben + ATA untracking
- Bessere Fehler-Logs bei fehlendem `token_account`/`token_program` in ExecutionResult
- `TrackedWallet` als `Arc<TrackedWallet>` für delayed re-publish task

**Status**: ❌ FEHLT — Wallet-Tracking ist weniger robust, WSOL-Anzeige in Grafana ggf. inkorrekt

**Bewertung**: Verbessert die Robustheit des gesamten Wallet-Tracking-Systems und behebt das WSOL-Display-Problem in Grafana. **Priorität 2 für Cherry-Pick.**

#### A.4 — Execution-Engine: Liquidation-Robustheit (TEILWEISE ZURÜCK)

**Datei**: `execution_engine.rs`

**Was wieder hergestellt wurde** (6 Commits nach Revert):
- ✅ RPC Wallet-Scan Fallback für Liquidation (`rpc_wallet_scan_for_liquidation`)
- ✅ JetStream → RPC Fallback bei leeren/stale Snapshots
- ✅ LockManager Seeding mit RPC-Balances
- ✅ Liquidation-Routing: Multi-Pool first, PumpFun last
- ✅ `side` in ExecutionResult Metadata
- ✅ Liquidation-Sells bypassen `sell_token_balance` Preflight

**Was noch fehlt**:
- ❌ 6005-Retry-Mechanismus (bei BondingCurveComplete → automatisch multi-pool retry)
- ❌ `PumpFunAmmDex::new_with_cache()` Verwendung in Liquidation (nutzt aktuell `new()`)
- ❌ SELL → token_account + token_program in ExecutionResult Metadata für market-data ATA-Tracking
- ❌ `emit_sim_failed_decision()` gibt `Err` zurück (statt `Ok`) für Retry-Detection
- ❌ `AVAILABLE_TRADING_CAPITAL_LAMPORTS` Metrik

**Status**: ⚠️ TEILWEISE — Kern-Liquidation funktioniert, 6005-Retry und Cache-Nutzung fehlen

#### A.5 — Pump.fun Bonding Curve: SELL bei migrierten Tokens (SINNVOLL – sollte zurück)

**Datei**: `pumpfun.rs` (+11 Zeilen)

**Was verloren ging**:
- Bei SELL: Wenn `real_token_reserves == 0 UND real_sol_reserves == 0` aber `virtual_reserves > 0` → Quote ablehnen statt stale Quote zu generieren
- Alte Logik: `warn!` + trotzdem Quote generieren → führte zu 6023 on-chain Fehler
- Neue Logik: `info!` + `return Ok(None)` → Multi-Pool-Routing findet PumpSwap AMM

**Status**: ❌ FEHLT — PumpFun-Quoter kann stale Quotes für migrierte Tokens generieren

**Bewertung**: Ohne diesen Fix können SELL-Intents für migrierte Tokens auf der Bonding Curve landen und mit Error 6023 scheitern, statt über PumpSwap AMM geroutet zu werden. **Priorität 1 für Cherry-Pick.**

#### A.6 — TX-Builder: Cache-Capped min_out für Pump.fun BUY (SINNVOLL)

**Datei**: `tx_builder.rs` (+29 Zeilen)

**Was verloren ging**:
- Bei Pump.fun BUY: `min_out` wird mit frischem Cache-Quote gekappt
- Vermeidet Error 6002 ("Too much SOL required") wenn sich die Bonding Curve zwischen Intent-Publish und TX-Build verschoben hat

**Status**: ❌ FEHLT — BUY-Transaktionen können mit 6002 scheitern bei schnellen Kursbewegungen

**Bewertung**: **Priorität 2 für Cherry-Pick.**

#### A.7 — Metrics: `available_trading_capital_lamports` (NICE-TO-HAVE)

**Datei**: `metrics.rs` (+6 Zeilen)

**Was verloren ging**: Neue Prometheus-Metrik `available_trading_capital_lamports` mit klarerem Namen für Grafana

**Status**: ❌ FEHLT — Grafana nutzt `available_sol_lamports` (funktioniert, aber Name ist verwirrend)

#### A.8 — Dokumentation & Scripts (TEILWEISE RELEVANT)

**Verlorene Dateien**:
- `docs/WSOL_DASHBOARD_AND_BC_EXIT_PLAN.md` — Plan-Dokument für WSOL-Dashboard und BC-Exit
- `docs/JETSTREAM_POSITION_RECONCILIATION_ANALYSIS.md` — JetStream-Analyse
- `scripts/check_wallet.sh` — Diagnose-Script

**Status**: Docs können rekonstruiert werden; Scripts waren diagnostisch

---

## 1. EXECUTION ENGINE (`src/bin/execution_engine.rs`)

### AKZEPTABEL – Simulation & TX-Sending

| Zeile | Call | Bewertung |
|-------|------|-----------|
| ~6971, ~7001 | `get_latest_blockhash()` in `simulate_transaction()` | AKZEPTABEL – Simulation braucht Blockhash |
| ~7040 | `simulate_transaction_with_config()` | AKZEPTABEL – Simulation ist Pflicht (simulate-gated) |
| ~6122, ~7102-7104 | `get_latest_blockhash_retry()` in `send_transaction_rpc/with_fallback` | AKZEPTABEL – TX-Send braucht Blockhash |
| ~7148 | `send_transaction_with_config()` | AKZEPTABEL – Finale TX-Übermittlung |

### BOOTSTRAP

| Zeile | Call | Bewertung |
|-------|------|-----------|
| ~3693 | `get_latest_blockhash()` beim Start | BOOTSTRAP – Einmaliger Healthcheck |

### AKZEPTABEL (Cold Path – Liquidation)

| Zeile | Call | Bewertung |
|-------|------|-----------|
| ~1299-1391 | `get_token_accounts_by_owner()` in `rpc_wallet_scan_for_liquidation()` | AKZEPTABEL – RPC-Fallback für manuelle Liquidation (Cold Path) |

### VERSTOSS

| Zeile | Call | Problem | Geyser-Alternative |
|-------|------|---------|---------------------|
| ~2071-2088 | `get_token_accounts_by_owner()` in `cleanup_wallet_after_liquidation()` | **VERSTOSS** – RPC-Scan aller Token-Accounts nach Liquidation | Wallet-Snapshots aus market-data/JetStream |
| ~2092 | `get_account(&wsol_ata)` in `cleanup_wallet_after_liquidation()` | **VERSTOSS** – WSOL-Check per RPC | WSOL-Status aus Geyser/WalletSnapshot |
| ~2347 | `get_account(&token_account_pk)` in Manual-Burn-Job | **VERSTOSS** – Token-Account per RPC bei manueller Burn-Anfrage | Account-Daten aus LivePoolCache oder WalletSnapshot |
| ~2400 | `get_token_decimals_or_default()` in Manual-Burn-Job | **VERSTOSS** – Mint-Decimals per RPC | Aus WalletSnapshot/LivePoolCache |
| ~2420 | `get_account(&bc)` in Manual-Burn-Job | **VERSTOSS** – Bonding-Curve-Check per RPC für Route-Validation | LivePoolCache hat Bonding-Curve-State |

---

## 2. DEX-MODULE – KRITISCHSTE VERSTÖSSE

### `src/solana/dex/pumpfun.rs` – Pump.fun Bonding Curve

| Zeile | Call | Problem | Geyser-Alternative |
|-------|------|---------|---------------------|
| 309 | `get_account_retry(bonding_curve)` in `fetch_bonding_curve()` | **KRITISCH** – Bonding-Curve-Fetch bei jedem Quote im Hot-Path | LivePoolCache `CachedPoolState::PumpFun` hat die BC-Daten bereits via Geyser |
| 322 | `get_account(bonding_curve)` in `fetch_bonding_curve_fast()` | **KRITISCH** – Derselbe Call mit Timeout (Sniping-Pfad) | Geyser-Event beim Pool-Create liefert die initialen Daten sofort |
| 572 | `fetch_bonding_curve_fast()` in `quote_exact_in_with_fallback()` retry | **KRITISCH** – RPC in Retry-Loop | Cache-basiertes Quoting |
| 1112 | `get_account_retry(&bonding_curve)` in `build_swap_ix_async` | **KRITISCH** – BC-Fetch für Creator-Auflösung direkt im TX-Build-Pfad! | LivePoolCache sollte den Creator haben |

**Revert-Impact**: Die SELL-Logik für migrierte Tokens (real_reserves == 0 → reject statt stale quote) fehlt ebenfalls (Abschnitt A.5).

### `src/solana/dex/pumpfun_amm.rs` – PumpSwap AMM

| Zeile | Call | Problem | Geyser-Alternative |
|-------|------|---------|---------------------|
| 378-465 | **Eigener `rpc_call()` Wrapper** | **ARCHITEKTUR-VERSTOSS** – Eigene HTTP-RPC-Implementierung parallel zum offiziellen RPC-Client |
| 472, 506 | `rpc_call("getAccountInfo")` | **VERSTOSS** – Account-Daten per RPC | LivePoolCache / Geyser |
| 594, 624 | `rpc_call("getTokenAccountsByOwner")` | **KRITISCH** – Token-Account-Discovery per RPC im Quote/Build-Pfad | Wallet-ATAs aus Geyser-Subscription |
| 685-703 | `derive_existing_pda()` mit `rpc_get_account_owner_and_executable()` | **VERSTOSS** – PDA-Existenzprüfung per RPC | PDA-Adressen sind deterministisch |
| 705-748 | `try_parse_pool_static_from_market_account()` | **VERSTOSS** – Pool-Account per RPC parsen | Sollte aus LivePoolCache kommen |
| 1388, 1824, 1875, 2044 | `rpc_call_tx_history("getSignaturesForAddress")`, `rpc_call_tx_history("getTransaction")` | **VERSTOSS** – Transaction-History per RPC | Geyser-Transactions |

**Revert-Impact**: ❌ Die gesamte Geyser-First Integration (`new_with_cache`, LivePoolCache-Zugriff für Reserves und Pool-Accounts) **fehlt**. Jeder PumpSwap-AMM-Quote macht aktuell 2-3 RPC-Calls.

### `src/solana/dex/orca.rs` – Orca Whirlpool

| Zeile | Call | Problem | Geyser-Alternative |
|-------|------|---------|---------------------|
| 455 | `get_multiple_accounts([vault_a, vault_b])` in `load_reserves_if_needed()` | **KRITISCH** – Vault-Balances per RPC bei Cache-Miss | Geyser-Vault-Updates → LivePoolCache |
| 1387 | `get_multiple_accounts(&[tick_arrays])` in `build_swap_ix_async` | **KRITISCH** – Tick-Array-Validierung per RPC | Geyser oder pre-cached tick arrays |

### `src/solana/dex/raydium.rs` – Raydium AMM

| Zeile | Call | Problem | Geyser-Alternative |
|-------|------|---------|---------------------|
| 194 | `get_account_retry(pool_address)` in `load_pool_from_geyser()` | **VERSTOSS** – Funktion heißt "from_geyser" aber macht RPC-Call! 20 Retries × 500ms | Geyser-Account-Update direkt parsen |
| 1264 | `get_account_retry(&market_id)` in `fetch_and_populate_serum_accounts` | **KRITISCH** – Serum-Market im Hot Path | Cache oder Bootstrap |
| 1324-1325 | `get_token_account_balance()` in `fetch_and_update_reserves()` | **KRITISCH** – Vault-Balances per RPC on-demand | Geyser-Vault-Updates → LivePoolCache |

### `src/solana/dex/meteora_dlmm.rs` – Meteora DLMM

| Zeile | Call | Problem | Geyser-Alternative |
|-------|------|---------|---------------------|
| 240 | `get_account(pool_addr)` in `update_reserve_balances()` | **VERSTOSS** – Pool-Account-Fetch für Vault-Adressen | LivePoolCache hat Meteora-State |
| 269-270 | `get_account_retry(&reserve_x/y)` | **KRITISCH** – Vault-Balances per RPC bei jedem Quote | Geyser trackt diese Accounts; market-data publiziert `BinArrayUpdate` |

### `src/solana/dex/raydium_cpmm.rs` – Raydium CPMM

| Zeile | Call | Problem | Geyser-Alternative |
|-------|------|---------|---------------------|
| 237-238 | `get_account_retry(&vault_0/1)` | **KRITISCH** – Vault-Balances per RPC | Geyser → LivePoolCache |

---

## 3. TX-INFRASTRUKTUR

### `src/execution/tx_builder.rs`

| Zeile | Call | Problem | Geyser-Alternative |
|-------|------|---------|---------------------|
| 218 | `get_account(pool_id)` in `fetch_orca_from_rpc()` | **KRITISCH** – Orca-Whirlpool-Fetch als Fallback im TX-Build-Pfad | LivePoolCache (`CachedPoolState::Orca`) |
| 523 | `load_pool_from_geyser()` Raydium Fallback | **KRITISCH** – Bis zu 10s Latenz durch 20 RPC Retries | Geyser-Update direkt nutzen |
| 1370 | `load_pool_from_geyser()` Multi-hop Raydium | **KRITISCH** – Gleicher 20-Retry RPC-Call | Gleiche Lösung |
| 1518 | `load_pool_by_address()` Multi-hop Meteora | **KRITISCH** – Pool-Fetch per RPC | LivePoolCache |

**Revert-Impact**: ❌ Cache-capped `min_out` für Pump.fun BUY fehlt (Error 6002 Risiko bei schnellen Kursbewegungen).

### `src/solana/tx_sender.rs`

| Zeile | Call | Bewertung |
|-------|------|-----------|
| 459 | `send_transaction_with_config()` | AKZEPTABEL – RPC-Fallback in TPU → Jito → RPC Chain |

### `src/solana/tpu_client.rs`

| Zeile | Call | Bewertung |
|-------|------|-----------|
| 151, 211 | `get_slot()` | AKZEPTABEL – Slot-Query für Leader-Schedule (TPU-Routing) |

### `src/solana/arbitrage.rs`

| Zeile | Call | Bewertung |
|-------|------|-----------|
| 315 | `get_latest_blockhash()` | AKZEPTABEL – Simulation |
| 328 | `simulate_transaction()` | AKZEPTABEL – Simulate-gated |
| 129 | `get_balance_retry()` | **VERSTOSS** – Balance-Check per RPC vor Arb-Execution |

---

## 4. WALLET & TOKEN UTILS

### `src/solana/token_utils.rs`

| Zeile | Call | Problem | Geyser-Alternative |
|-------|------|---------|---------------------|
| 13 | `get_token_supply(mint)` | **VERSTOSS** – Mint-Decimals per RPC | Geyser liefert `TokenMintInfo` mit Decimals |
| 18 | `get_account(mint)` (Fallback) | **VERSTOSS** – Dasselbe als Fallback | LivePoolCache oder Geyser-Mint-Subscription |
| 33, 37 | `get_token_supply()` + `get_account()` | **VERSTOSS** – Gleiche Logik | Gleiche Lösung |

### `src/wallet.rs`

| Zeile | Call | Problem | Geyser-Alternative |
|-------|------|---------|---------------------|
| 211 | `get_balance()` | **VERSTOSS** im Hot-Path / AKZEPTABEL für Utility | Geyser-Balance-Tracking |
| 220 | `get_account(mint)` für Token-Programm-Erkennung | **VERSTOSS** | Geyser-Mint-Info |
| 268 | `get_account(&ata)` für ATA-Existenz-Check | **VERSTOSS** wenn im Hot-Path | Geyser-Account-Subscription |
| 325, 364, 449, 567 | `get_latest_blockhash()` + `send_and_confirm_transaction()` | AKZEPTABEL – TX-Sending |
| 385 | `get_account(&to_ata)` | **VERSTOSS** | Geyser-Account-Subscription |

### `src/execution/wsol_manager.rs`

| Zeile | Call | Problem | Geyser-Alternative |
|-------|------|---------|---------------------|
| 501 | `get_balance()` | **VERSTOSS** – SOL-Balance per RPC | Geyser-Wallet-Tracking |
| 530 | `get_token_account_balance()` | **VERSTOSS** – WSOL-Balance per RPC | Geyser-Wallet-Tracking |
| 846, 895 | `get_latest_blockhash()` + `send_and_confirm_transaction()` | AKZEPTABEL – Wrap/Unwrap TX-Sending |

### `src/execution/account_janitor.rs`

| Zeile | Call | Bewertung |
|-------|------|-----------|
| 618, 834, 1075 | `get_latest_blockhash_retry()` + `send_and_confirm_transaction()` | AKZEPTABEL – Housekeeping-TXs |

---

## 5. SONSTIGE BINARIES

### `src/bin/sell_all.rs` / `src/bin/sell_all_keyless.rs`

Emergency-Tools – hier sind RPC-Calls akzeptabel, da dies keine Hot-Path-Binaries sind.

### `src/bin/market_data.rs`

| Zeile | Call | Bewertung |
|-------|------|-----------|
| ~676 | `get_multiple_accounts(&keys)` | BOOTSTRAP – Initiale Account-Daten beim Start vor Geyser-Subscription |

---

## 6. WEITERE ARCHITEKTUR-PROBLEME & LOGIK-BUGS

### BUG A: Killswitch-Liquidation – Token werden übersprungen

**Problem**: Bei `run_liquidation_job()` gibt es mehrere Pfade wo Token übersprungen werden:

1. `min_out_sol.is_none()` → Token wird übersprungen wenn kein DEX einen Quote liefert
2. Pump.fun-Quote erfolgreich, aber Creator fehlt im Cache → degradiert zu Multi-Pool
3. PumpSwap-Quote erfolgreich, aber `pool_accounts_v1_for_base_mint()` gibt `None` zurück

**Status nach Revert+Fixes**: ✅ TEILWEISE BEHOBEN — Liquidation versucht jetzt Multi-Pool zuerst, PumpFun als Fallback. 6005-Auto-Retry fehlt aber noch.

### BUG B: `load_pool_from_geyser()` in `raydium.rs` macht 20 RPC-Retries

**Zeile ~194**: Die Funktion heißt `load_pool_from_geyser()` aber macht bis zu **20 RPC-Calls mit 500ms Delay** (= bis zu 10 Sekunden Latenz).

**Status**: ❌ UNVERÄNDERT — Immer noch 20 RPC-Retries im Namen von "Geyser".

### BUG C: PumpFunAmmDex hat eigene RPC-Infrastruktur

**Zeilen 378-465**: Eigener `rpc_call()` HTTP-Client mit eigenem Retry/Rate-Limiting.

**Status**: ❌ UNVERÄNDERT — Die Geyser-First Integration (LivePoolCache) die das Problem teilweise gelöst hätte, wurde durch den Revert entfernt.

### BUG D: Token-Decimals werden immer per RPC geholt

**`token_utils.rs`** wird an vielen Stellen aufgerufen. Jeder Call macht 1-2 RPC-Requests.

**Status**: ✅ BEHOBEN — token_utils wird ausschließlich im Cold Path aufgerufen (execution_engine Manual Burn, sell_all, sell_all_keyless, wallet). Hot Path (momentum_bot, arb_strategy, dex_parser) nutzt mint_infos/TokenMintInfo bzw. post_token_balances aus Geyser — keine RPC-Calls für Decimals. execution_engine Manual Burn nutzt LivePoolCache. RPC in sell_all/wallet ist laut Architektur-Regeln für Cold Path akzeptabel.

### BUG E: `cleanup_wallet_after_liquidation()` macht RPC statt Geyser

**Zeile ~2071-2088**: Inkonsistenz — Liquidation = JetStream/RPC-basiert, Cleanup = RPC-basiert.

**Status**: ✅ AKZEPTIERT (by design) — Cleanup nach Liquidation ist Cold Path. RPC ist per Architektur erlaubt. Für das zuverlässige Schließen aller leeren ATAs ist der autoritative on-chain-Zustand (getTokenAccountsByOwner) erforderlich; Geyser/JetStream könnte Stale-Daten liefern und ATAs übersehen.

### BUG F: Orca Reserve-Fetching hat 5min TTL mit RPC-Fallback

**`load_reserves_if_needed()`**: 5-Minuten-Cache, dann RPC-Fallback. Bei 50+ Pools = 50+ RPC-Calls.

**Status**: ✅ BEHOBEN — Architekturbereinigung (siehe `AUDIT_F_ORCA_RESERVES_IMPLEMENTATION_PLAN.md`): LivePoolCache ist einzige Reserve-Quelle im Hot Path. SQLite- und In-Memory-TTL-Schritte entfernt. Bei Cache-Miss: statische Reserves (pool.reserve_base/quote), kein RPC. RPC nur im Cold Path (live_pool_cache.is_none(), z.B. sell_all_keyless).

### BUG G (NEU): Stale JetStream Wallet-Snapshots → Ghost Open Positions

**Problem**: `MAX_BOOTSTRAP_MINTS = 30` in `market_data.rs` begrenzt den Bootstrap auf 30 Mints. JetStream hat aber 99+ Einträge akkumuliert. Mints jenseits des Limits werden beim Restart NICHT mit aktuellen Balancen überschrieben. Alte non-zero Einträge für bereits verkaufte/geschlossene ATAs bleiben bestehen. `execution_engine` liest beim Bootstrap ALLE JetStream-Einträge und zählt die stale non-zero Einträge fälschlicherweise als offene Positionen.

**Ursache**: Wenn ein Token verkauft und das ATA geschlossen wird, publiziert Geyser ein zero-balance Update. Wenn aber market-data in der Zwischenzeit neugestartet wurde und das ATA on-chain nicht mehr existiert, findet der Owner-Scan das ATA nicht → kein zero-balance Override → stale Eintrag bleibt.

**Fix**: Step 2.5 in market-data Bootstrap: Nach Publizierung der Snapshots für `known_mints` werden ALLE verbleibenden JetStream-Einträge enumeriert und zero-balance Overrides für nicht abgedeckte non-zero Mints publiziert. Scoped via `filter_subject` auf das aktuelle Wallet. Keine zusätzlichen RPC-Calls.

**Status**: ✅ BEHOBEN (Commit `43941752`)

### BUG H (NEU): Meteora DLMM / Raydium CPMM hardcoded SOL quote_mint → false arbitrage

**Problem**: `parse_meteora_transaction()` in `dex_parser.rs` (Zeile ~1372) setzt `quote_mint = SOL_MINT_PUBKEY` mit einem TODO-Kommentar, statt den tatsächlichen quote_mint aus dem Pool-State zu extrahieren. Gleiches Problem bei `parse_raydium_cpmm_transaction()` (Zeile ~1494) und `parse_raydium_v4_swap()` (Zeile ~408).

**Auswirkung**: Meteora DLMM Pools mit non-SOL Quotes (z.B. TOKEN/USDC) passieren den `quote_mint != SOL_MINT` Filter in `arb_strategy.rs` (Zeile ~922, ~1395) unerkannt. Preise von TOKEN/USDC Pools werden fälschlicherweise mit TOKEN/SOL Pools verglichen → falsche Arb-Signale. Dies ist exakt der Bug der im CHANGELOG als "cross-DEX price comparison bug" dokumentiert wurde.

**Betroffen**: `src/solana/dex_parser.rs` — `parse_meteora_transaction()`, `parse_raydium_cpmm_transaction()`, `parse_raydium_v4_swap()`

**Status**: ❌ OFFEN — TODO-Kommentar vorhanden aber nicht implementiert. Benötigt Pool-State-Lookup oder Account-Key-basierte quote_mint-Erkennung.

### ~~BUG I (vormals G): PumpFun SELL generiert stale Quote für migrierte Tokens~~ ✅ BEHOBEN

**`pumpfun.rs` Zeile ~888**: Guard für `real_reserves == 0 && virtual_reserves > 0` existiert wieder im Code (`return Ok(None)`). Multi-Pool-Routing wird korrekt getriggert.

**Status**: ✅ BEHOBEN — Guard in `pumpfun.rs` (Zeile 888-902) und `quote_calculator.rs` (Zeile 400-407) aktiv. Verbleibende Sell-Probleme (stale Cache, kein Pool-Failure-Tracking) werden unter BUG-A / FIX-20 adressiert.

---

## 7. ZUSAMMENFASSUNG RPC-CALLS IM HOT PATH (aktuell)

### Gesamtzählung Hot-Path RPC-Call-Sites

| Modul | Anzahl | Schweregrad |
|-------|--------|-------------|
| `pumpfun.rs` | 4 | KRITISCH |
| `pumpfun_amm.rs` | 6+ | KRITISCH (alles RPC, kein Cache) |
| `orca.rs` | 2 | KRITISCH (Fallback) |
| `raydium.rs` | 3 | KRITISCH |
| `raydium_cpmm.rs` | 2 | KRITISCH |
| `meteora_dlmm.rs` | 3 | KRITISCH |
| `tx_builder.rs` | 4 | KRITISCH (Fallbacks) |
| **TOTAL** | **24+** | |

### Geschätzte Latenz-Auswirkung (Hot Path)

Ein typischer **Momentum-Buy** durchläuft aktuell:
1. Quote → PumpFun `fetch_bonding_curve_fast()` = **+200-2000ms RPC** (sollte: 0ms aus Cache)
2. PumpSwap AMM Quote = **+500-3000ms RPC** (sollte: 0ms mit LivePoolCache)
3. TX-Build → ggf. Creator-RPC-Fallback = **+500-2000ms** (sollte: 0ms)
4. Simulation → `simulate_transaction()` = **+100-500ms** (unvermeidlich)
5. TX-Send → `send_transaction` = **+50-400ms** (unvermeidlich)

**Aktuelle Gesamtlatenz**: ~1500-8000ms
**Optimierte Latenz (nur Sim+Send)**: ~200-900ms
**Möglicher Geschwindigkeitsgewinn**: **3-8x**

---

## 8. CHERRY-PICK EMPFEHLUNGEN

### Priorität 1 — Sofort zurückholen (Latenz + Korrektheit)

| # | Quelle | Beschreibung | Risiko |
|---|--------|-------------|--------|
| 1 | `pumpfun_amm.rs`, `live_pool_cache.rs`, `cross_dex_handler.rs` | PumpSwap AMM Geyser-First Integration (LivePoolCache) | Niedrig — isoliert, gut getestet |
| 2 | `pumpfun.rs` | SELL bei migrierten Tokens → `return Ok(None)` statt stale quote | Minimal — 11 Zeilen, klar definiert |
| 3 | `execution_engine.rs` | `emit_sim_failed_decision()` → `Err` für 6005-Retry | Niedrig — 3 Zeilen |

### Priorität 2 — Bald zurückholen (Robustheit)

| # | Quelle | Beschreibung | Risiko |
|---|--------|-------------|--------|
| 4 | `momentum_bot.rs`, `config.rs` | Creator-Handling, DEX-Normalisierung, Pool-Registry | Mittel — größerer Diff, sorgfältig cherry-picken |
| 5 | `market_data.rs` | WSOL-Seeding, PumpAmm pool_accounts Propagation, SELL→JetStream(0) | Mittel — 331 Zeilen, viele Verbesserungen |
| 6 | `tx_builder.rs` | Cache-capped min_out für PumpFun BUY | Niedrig — isoliert |
| 7 | `metrics.rs` | `available_trading_capital_lamports` | Minimal |

### Priorität 3 — Langfristig (Architektur)

| # | Problem | Fix |
|---|---------|-----|
| 8 | PumpFun BC-Fetch per RPC (`pumpfun.rs`) | Quote aus `CachedPoolState::PumpFun` berechnen |
| 9 | Raydium `load_pool_from_geyser()` 20 RPC-Retries | Geyser-Account-Update direkt parsen |
| 10 | Orca/Raydium/Meteora Vault-Balances per RPC | Geyser-Vault-Subscription → LivePoolCache |
| 11 | Token-Decimals per RPC (`token_utils.rs`) | Globalen Decimals-Cache aus Geyser-Mint-Info |
| 12 | `pumpfun_amm.rs` eigene RPC-Infrastruktur | Komplett auf LivePoolCache umstellen |

---

## 9. ARCHITEKTUR-REGELN (Referenz)

Dokumentiert in `.cursor/rules/ironcrab-core.mdc`:

- **Hot Path** (Buy/Sell/Arb): Geyser-First, KEINE blocking RPC-Calls. Ziel: <1s Latenz.
- **Cold Path** (Liquidation, Cleanup): RPC-Calls akzeptabel. Sicherheit > Geschwindigkeit.
- **Bootstrap**: RPC beim Start akzeptabel für initiale Daten.
- **Simulation + TX-Send**: Immer RPC (unvermeidlich).
