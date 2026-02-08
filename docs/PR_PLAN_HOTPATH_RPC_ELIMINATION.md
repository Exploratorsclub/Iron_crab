# PR-Plan: Hot-Path RPC Elimination (Priorität 1)

> Referenz: `docs/ARCHITECTURE_AUDIT_2026-02-07.md`
> Erstellt: 2026-02-07
> Ziel: Alle vermeidbaren RPC-Calls aus dem Hot-Path entfernen → 3-5x Latenz-Gewinn

---

## Übersicht

| PR | Titel | Status | Dateien | Aufwand |
|----|-------|--------|---------|---------|
| PR 1 | Globaler Mint-Decimals-Cache (GEYSER-FIRST) | ✅ DEPLOYED | 6 | ~3h |
| Fix 1 | Creator über NATS propagieren (Killswitch Bug) | ✅ DEPLOYED | 2 | ~1h |
| Fix 2 | Simulation-Error Logging (Diagnose) | ✅ DEPLOYED | 1 | ~30min |
| Fix 3 | Position-Tracking: Liquidation-Sells schließen Positionen | ✅ DEPLOYED | 2 | ~1h |
| Fix 4 | Ghost-Balance-Fix: Wallet-Snapshot-Bootstrap schreibt balance=0 | ✅ DEPLOYED | 1 | ~30min |
| PR 2 | PumpFun Bonding-Curve Quotes aus Cache | ✅ DEPLOYED | 2 | ~4h |
| Fix 5 | ExecutionResult Metadata + LockManager Seed + WSOL Logging | ✅ DEPLOYED | 3 | ~2h |
| Fix 6 | Startup Owner-Scan: getTokenAccountsByOwner Discovery | ✅ DEPLOYED | 1 | ~1h |
| Fix 7 | Token-2022 StateWithExtensions: Account-Unpack für Extensions | ✅ DEPLOYED | 1 | ~30min |
| PR 3 | Vault-Balance-Reads aus Cache (Orca/Raydium/Meteora/CPMM) | 🔧 IN PROGRESS | 5 | ~5h |
| PR 4 | tx_builder Orca-State aus Cache | ⬜ TODO | 1 | ~1h |
| PR 5 | Blockhash via Geyser (kein RPC für Blockhash) | ⬜ TODO | 2 | ~3h |

---

## PR 1: Globaler Mint-Decimals-Cache (GEYSER-FIRST)

**Branch**: `fix/decimals-cache-geyser`
**Status**: ✅ DEPLOYED (2026-02-07)
**Risiko**: Niedrig
**Eliminiert**: 1-2 RPC-Calls pro `get_token_decimals_or_default()` Aufruf

### Architektur-Kontext (MASTER/SLAVE)

Der `LivePoolCache` existiert in **zwei** Instanzen:
- **market-data** (MASTER): Direkt von Geyser befüllt, Single Source of Truth
- **execution-engine** (SLAVE): Über NATS synchronisiert, lokale Kopie

Decimals fließen über **drei** Kanäle in den SLAVE Cache:
1. `TokenMintInfo` NATS Events (für alle tracked Mints, Haupt-Kanal)
2. `WalletBalanceSnapshot` NATS Events (für eigene Tokens, Neben-Kanal)
3. RPC-Fallback → Write-Back in Cache (nur bei kaltem Cache)

### Aufgaben

- [x] `src/execution/live_pool_cache.rs`: `mint_decimals: DashMap<Pubkey, u8>` Feld hinzufügen
- [x] `src/execution/live_pool_cache.rs`: `get_mint_decimals(&mint) -> Option<u8>` Methode
- [x] `src/execution/live_pool_cache.rs`: `set_mint_decimals(mint, decimals)` Methode
- [x] `src/solana/token_utils.rs`: `get_token_decimals_or_default()` erweitern: Cache → RPC-Fallback + Write-Back
- [x] `src/solana/token_utils.rs`: `try_token_decimals()` gleiche Erweiterung
- [x] Aufrufer anpassen: Cache durchreichen wo LivePoolCache verfügbar (execution_engine, sell_all, sell_all_keyless, wallet)
- [x] `src/bin/execution_engine.rs`: WalletBalanceSnapshot → Decimals in SLAVE Cache
- [x] `src/metrics.rs`: `MINT_DECIMALS_SOURCE_CACHE` Counter + Prometheus-Export
- [x] `src/bin/market_data.rs`: Bei Geyser `TokenMintInfo` → Decimals in MASTER LivePoolCache
- [x] **`src/bin/execution_engine.rs`: Subscribe auf `TOPIC_MARKET_EVENTS`, filter `TokenMintInfo` → Decimals + token_program in SLAVE LivePoolCache**
- [x] `cargo check` erfolgreich (0 errors, 0 warnings)
- [ ] `cargo clippy` keine neuen Warnings

### Akzeptanzkriterien

- [ ] SLAVE Cache empfängt Decimals aus `TokenMintInfo` Events (Haupt-Kanal für ALLE Mints)
- [ ] SLAVE Cache empfängt Decimals aus `WalletBalanceSnapshot` (Neben-Kanal für eigene Tokens)
- [ ] MASTER Cache in market-data wird ebenfalls befüllt (Konsistenz)
- [ ] Wenn Cache vorhanden: 0 RPC-Calls für bekannte Mints
- [ ] Wenn Cache leer/kalt: RPC-Fallback funktioniert wie bisher + Write-Back
- [ ] Metrik zeigt Cache-Hit-Rate

---

## Fix 1: Creator über NATS propagieren (Killswitch Bug Root Cause)

**Branch**: `architecture-rebuild`
**Status**: ✅ DEPLOYED (2026-02-07)
**Risiko**: Niedrig
**Problem**: `market-data` cached PumpFun/PumpSwap Creator korrekt im MASTER LivePoolCache, propagiert ihn aber NICHT über `PoolCacheUpdate.metadata` an den SLAVE Cache in `execution-engine`. → Killswitch kann PumpFun-Token nicht verkaufen weil `metadata.contains_key("creator")` fehlschlägt.

### Aufgaben

- [x] `src/bin/market_data.rs`: Bei `PoolCacheUpdate::PoolDiscovered` für PumpFun + PumpAmm den Creator, associated_bonding_curve und complete in `metadata` HashMap einfügen
- [x] `src/bin/execution_engine.rs`: `build_minimal_pool_state()` – Creator, associated_bonding_curve, complete und pool_accounts aus `update.metadata` extrahieren
- [ ] `cargo check` + `cargo clippy` erfolgreich
- [ ] Deploy + Killswitch-Test

### Akzeptanzkriterien

- [ ] SLAVE LivePoolCache enthält Creator für PumpFun-Pools
- [ ] Killswitch Liquidation erkennt PumpFun-Bonding-Curve mit Creator → Quote akzeptiert
- [ ] Kein neuer RPC-Call eingeführt

---

## Fix 2: Besseres Simulation-Error Logging

**Branch**: `architecture-rebuild`
**Status**: ✅ DEPLOYED (2026-02-07)
**Risiko**: Keins (nur Logging)
**Problem**: Bei Simulation-Failures wird nur "Intent simulation failed" geloggt, ohne `error_code`, `logs_preview`, DEX oder Mint. Fehlerdiagnose ist praktisch unmöglich.

### Aufgaben

- [x] `src/bin/execution_engine.rs` `emit_sim_failed_decision()`: error_code, logs_preview (truncated), DEX, side, mint in WARN-Log aufnehmen
- [x] `src/bin/execution_engine.rs` Liquidation: Detailliertes Logging bei Intent-Erstellung (quote_attempts, routing, creator_present)
- [x] `src/bin/execution_engine.rs` Liquidation: Detailliertes Logging wenn kein Route gefunden (LIQUIDATION SKIP mit allen quote_attempts)
- [ ] `cargo check` + `cargo clippy` erfolgreich

### Akzeptanzkriterien

- [ ] Simulation-Failures enthalten: error_code, program logs (truncated), DEX, mint
- [ ] Liquidation-Skips enthalten: alle quote_attempts (welche DEX wurde probiert, warum fehlgeschlagen)
- [ ] Keine Performance-Auswirkung (Logging nur bei Fehler)

---

## Fix 3: Position-Tracking: Liquidation-Sells schließen Positionen

**Branch**: `architecture-rebuild`
**Status**: ✅ DEPLOYED (2026-02-07)
**Risiko**: Niedrig (nur Logging + Position-Cleanup bei confirmed sells)
**Problem**: Liquidation-Sells werden von execution-engine erstellt, nicht von momentum-bot. momentum-bot kennt diese Intents nicht (`pending_intents` leer) → `close_position()` wird nie aufgerufen → Ghost Positions → `max_open_positions` blockiert neue Trades.

### Root Cause

- `momentum-bot.handle_execution_result()` sucht Intent in `pending_intents` Map
- Liquidation-Intent-IDs (`liquidation-...`) sind NIE in dieser Map
- → `return` ohne Position zu schließen
- execution-engine's `open_positions` Counter wird ebenfalls nie dekrementiert bei Sells

### Aufgaben

- [x] `src/bin/momentum_bot.rs`: Bei unbekannter Intent-ID + Confirmed + `liquidation-*` Prefix → Position über `token_mint` schließen
- [x] `src/bin/execution_engine.rs`: `open_positions` Counter bei confirmed BUY/SELL inkrementieren/dekrementieren (statt nur Dashboard-Gauge)
- [x] `cargo check` + `cargo clippy` erfolgreich (0 errors, 0 warnings)
- [ ] Deploy + Test

### Akzeptanzkriterien

- [ ] Liquidation-Sells schließen Positionen in momentum-bot
- [ ] execution-engine `open_positions` Counter bleibt konsistent mit Wallet
- [ ] `max_open_positions` Limit wird nicht dauerhaft blockiert
- [ ] Normales Position-Tracking (momentum-bot eigene Intents) funktioniert unverändert

---

## Fix 4: Ghost-Balance-Fix: Wallet-Snapshot-Bootstrap schreibt balance=0

**Branch**: `architecture-rebuild`
**Status**: ✅ DEPLOYED (2026-02-08)
**Risiko**: Niedrig
**Problem**: `publish_wallet_snapshot()` in `market-data` übersprang das Publizieren von `balance_raw = 0` für Token deren ATA on-chain nicht mehr existiert. Stattdessen wurde die stale Balance aus JetStream beibehalten → permanente Ghost-Positionen die nie gecleaned werden konnten.

### Root Cause

- Zeile 747-754 in `market_data.rs`: Wenn `getMultipleAccounts` für die ATA `None` zurückgibt, prüfte der Code ob der letzte JetStream-Snapshot `balance_raw > 0` hatte. Falls ja: `continue` → kein `balance_raw = 0` publiziert.
- Zusätzlich wurde der Mint zu `mints_in_wallet` hinzugefügt → `WalletSnapshotComplete` behauptete fälschlich, Token sei in der Wallet.
- → `momentum-bot` erkannte den Ghost nicht → permanente Ghost-Position.

### Fix

- `continue`-Protection entfernt: Wenn ATA nicht on-chain existiert → `balance_raw = 0` wird publiziert.
- Info-Log für gecleante stale Balances hinzugefügt.
- Kommentar korrigiert: Kein periodischer Refresh nötig (Geyser-basierte Live-Updates sind korrekt).

### Aufgaben

- [x] `src/bin/market_data.rs`: Ghost-Balance-Protection entfernen, `balance_raw = 0` bei fehlender ATA publizieren
- [x] `src/bin/market_data.rs`: Doc-Kommentar korrigieren (kein periodischer Refresh, Startup + Geyser reicht)
- [x] `cargo check` + `cargo clippy` erfolgreich (0 errors, 0 warnings)
- [x] Deploy + Killswitch-Test → Liquidation erfolgreich ✅

### Verifizierung (Geyser Wallet-Tracking)

Die Geyser-basierte Wallet-Balance-Implementierung wurde geprüft und ist korrekt:
- **Startup**: `publish_wallet_snapshot()` per `getMultipleAccounts` (1 RPC-Call, legitimt)
- **Live**: `ExecutionResult` → `market-data` tracked neue ATAs dynamisch → Geyser-Resubscribe
- **Geyser**: Account-Updates für tracked ATAs → `WalletBalanceSnapshot` über NATS + JetStream
- **Bekannte Lücke**: Geschlossene ATAs (Geyser meldet keine Deletions) → gefixt durch Startup-RPC-Call

---

## PR 2: PumpFun Bonding-Curve Quotes aus LivePoolCache

**Branch**: `architecture-rebuild`
**Status**: ✅ DEPLOYED (2026-02-08)
**Risiko**: Mittel (Pump.fun = häufigster DEX)
**Eliminiert**: 200-2000ms pro Quote + 500-2000ms im TX-Build

### Aufgaben

- [x] `src/solana/dex/pumpfun.rs`: `PumpFunDex::new()` um optionalen `SharedLivePoolCache` Parameter erweitern
- [x] `src/solana/dex/pumpfun.rs`: `get_bonding_curve_from_cache()` Hilfsmethode → `CachedPoolState::PumpFun` → `BondingCurveState`
- [x] `src/solana/dex/pumpfun.rs`: `get_creator_from_cache()` Hilfsmethode → LivePoolCache Creator-Lookup
- [x] `src/solana/dex/pumpfun.rs`: `quote_exact_in()` umbauen:
  - Zuerst: `cache.get(bonding_curve)` → `CachedPoolState::PumpFun` → Quote aus cached `virtual_*_reserves` berechnen
  - Fallback: Bestehender RPC-Pfad (`fetch_bonding_curve_fast()`) mit WARN-Log
- [x] `src/solana/dex/pumpfun.rs`: `build_swap_ix_async()` Creator-Auflösung erweitert:
  - 1. `fallback_creator` (aus Intent-Metadata)
  - 2. `cached_creators` DashMap (aus vorherigen Lookups)
  - 3. `get_creator_from_cache()` (LivePoolCache, GEYSER-FIRST) ← NEU
  - 4. RPC-Fallback (mit WARN-Log, sollte nicht passieren)
- [x] `src/bin/execution_engine.rs`: Cache an `PumpFunDex::new()` durchreichen:
  - `run_liquidation_job()` ✅
  - `run_burn_job()` ✅
- [x] `src/execution/tx_builder.rs`: Cache an `PumpFunDex::new()` durchreichen ✅
- [x] Alle Call-Sites angepasst: `sell_all.rs`, `sell_all_keyless.rs`, `cross_dex_handler.rs`, Tests → `None`
- [x] `cargo check` + `cargo clippy` erfolgreich (0 errors, 0 warnings)
- [ ] Deploy + Test

### Akzeptanzkriterien

- [ ] Pump.fun Quotes kommen aus Cache wenn Geyser-Daten vorhanden
- [ ] Creator wird aus Cache aufgelöst (kein RPC im TX-Build)
- [ ] WARN-Log bei jedem RPC-Fallback
- [ ] Liquidation funktioniert weiterhin korrekt
- [ ] Kein Verhaltensunterschied für sell_all.rs (Emergency-Tool ohne Cache)

---

## Fix 5: ExecutionResult Metadata + LockManager Seed + WSOL Logging

**Branch**: `architecture-rebuild`
**Status**: ✅ DEPLOYED (2026-02-08)
**Risiko**: Mittel (betrifft Trade-Lifecycle: BUY → SELL Fähigkeit)
**Problem**: Nach einem bestätigten BUY scheitern SELL-Intents an `SIM_INSUFFICIENT_BALANCE`. Dashboard zeigt SOL statt WSOL.

### Root Cause (3 zusammenhängende Bugs)

1. **ExecutionResult fehlende Metadata**: Reguläre BUY-Intents von `momentum-bot` enthalten `token_account` und `token_program` NICHT in der Intent-Metadata. `execution-engine` kopiert blindlings `intent.metadata` in den ExecutionResult. `market-data` prüft diese Felder und macht `continue` (silent skip) wenn sie fehlen → ATA wird nie bei Geyser registriert → keine Balance-Updates für das neue Token.

2. **Kein sofortiges LockManager-Update nach BUY**: Nach einem Confirmed BUY wird nur der `open_positions`-Counter inkrementiert. `LockManager.available_tokens` wird NIE mit dem gekauften Betrag befüllt. Das System verlässt sich komplett auf den Geyser-Pipeline Roundtrip (market-data → Geyser ATA subscribe → Geyser account update → WalletBalanceSnapshot → NATS → execution-engine), der aber wegen Bug 1 nie ankommt.

3. **WSOL nie initialisiert**: `WalletBalanceUpdate` Logs sind auf `debug!`-Level → unsichtbar in Produktion. `wsol_initialized` wird nie `true` → `available_trading_capital()` fällt auf SOL zurück → Dashboard zeigt SOL statt WSOL.

### Fix

1. **ExecutionResult Metadata anreichern**: Bei BUY die ATA deterministisch ableiten (PDA aus wallet + output_mint + token_program, kein RPC) und zusammen mit `token_program` und `mint_decimals` in die ExecutionResult-Metadata einfügen. Die Daten sind bereits im Intent verfügbar (`TradeResources.token_program`, `resources.output_mint`).

2. **LockManager sofort seeden**: Nach Confirmed BUY den `fill_out.raw` Betrag direkt in `LockManager.set_available_token_balance()` schreiben. Das überbrückt die Geyser-Pipeline-Latenz. Nach Confirmed SELL die Balance auf 0 setzen.

3. **Logging hochstufen**: Alle `WalletBalanceUpdate`-Logs von `debug!` auf `info!` hochstufen (market-data Publisher, execution-engine Consumer, LockManager Update).

### Aufgaben

- [x] `src/bin/execution_engine.rs`: Nach Confirmed BUY ATA + token_program + mint_decimals in ExecutionResult-Metadata einfügen (PDA-Ableitung, kein RPC)
- [x] `src/bin/execution_engine.rs`: Nach Confirmed BUY `confirmed_buy_fill_out_raw` capturen und `LockManager.set_available_token_balance()` aufrufen
- [x] `src/bin/execution_engine.rs`: Nach Confirmed SELL `LockManager.set_available_token_balance(mint, 0)` aufrufen
- [x] `src/bin/execution_engine.rs`: WalletBalanceUpdate Consumer-Log von `debug!` auf `info!` hochgestuft
- [x] `src/bin/market_data.rs`: `WalletBalanceUpdate: publishing to NATS` von `debug!` auf `info!` hochgestuft
- [x] `src/bin/market_data.rs`: `ExecutionResult: tracked wallet ATA/mint` von `debug!` auf `info!` hochgestuft
- [x] `src/storage/locks.rs`: `LockManager wallet balances updated` von `debug!` auf `info!` hochgestuft
- [x] `cargo check` + `cargo clippy` erfolgreich (0 errors, 0 neue warnings)
- [ ] Deploy + Test

### Akzeptanzkriterien

- [ ] Nach BUY: market-data loggt `ExecutionResult: tracked wallet ATA/mint` (INFO-Level sichtbar)
- [ ] Nach BUY: execution-engine loggt `LockManager: seeded token balance from confirmed BUY fill`
- [ ] SELL-Intents für gekaufte Token werden NICHT mehr mit `SIM_INSUFFICIENT_BALANCE` abgelehnt
- [ ] Dashboard zeigt WSOL (nicht SOL) nach Geyser-Pipeline-Initialisierung
- [ ] WalletBalanceUpdate-Flow ist in Logs nachvollziehbar (market-data → NATS → execution-engine)

---

## Fix 6: Startup Owner-Scan: getTokenAccountsByOwner Discovery

**Branch**: `architecture-rebuild`
**Status**: ✅ DEPLOYED (2026-02-08)
**Risiko**: Niedrig (nur Startup, kein Hot-Path)
**Problem**: Der Startup-Wallet-Snapshot nutzte nur `getMultipleAccounts` auf bekannte Mints aus JetStream. Token die nie in JetStream gelandet sind (z.B. wegen Bug in ATA-Tracking, externe Käufe über Phantom/Jupiter) waren für den Bot unsichtbar → Liquidation fand 0 Token, Positionen wurden nicht erkannt.

### Root Cause

- `publish_wallet_snapshot()` liest nur Mints aus JetStream → `getMultipleAccounts` für bekannte ATAs
- Token die vor Fix 5 gekauft wurden hatten fehlende ExecutionResult-Metadata → `market-data` trackte die ATAs nie → nie in JetStream
- Catch-22: Startup findet nur was bereits bekannt ist, erkennt keine unbekannten Token

### Fix

- Beim Startup (NICHT bei periodischen Refreshes) werden vor dem `getMultipleAccounts`-Call zwei `getTokenAccountsByOwner` RPC-Calls gemacht (SPL Token + Token-2022)
- Entdeckte Mints werden in die `known_mints` Liste gemergt
- Danach läuft der bestehende `getMultipleAccounts`-Flow mit der erweiterten Liste
- Dies stellt sicher dass der Bot beim Start den wahren On-Chain-Wallet-Status kennt

### RPC-Budget

- Startup: +2 RPC-Calls (`getTokenAccountsByOwner` x2) — einmalig, legitim
- Laufzeit: 0 RPC-Calls (Geyser + ExecutionResult-basiertes ATA-Tracking)

### Aufgaben

- [x] `src/bin/market_data.rs`: `getTokenAccountsByOwner` für SPL Token + Token-2022 im Startup-Pfad
- [x] `src/bin/market_data.rs`: Merge entdeckter Mints in `known_mints` + `cached_mint_meta`
- [x] `src/bin/market_data.rs`: Doc-Kommentar und Log-Messages aktualisiert
- [x] `cargo check` + `cargo clippy` erfolgreich (0 errors, 0 neue warnings)
- [ ] Deploy + Test

### Akzeptanzkriterien

- [ ] Startup entdeckt Token die nicht in JetStream sind (z.B. extern gekaufte Token)
- [ ] Entdeckte Token werden korrekt in JetStream gespeichert (für zukünftige Restarts)
- [ ] Liquidation findet alle Token in der Wallet (nicht nur bekannte)
- [ ] Geschlossene Positionen werden korrekt auf balance=0 gesetzt
- [ ] Kein Owner-Scan im Laufzeit-Betrieb (nur Startup)

---

## Fix 7: Token-2022 StateWithExtensions — Account-Unpack für Extensions

**Branch**: `architecture-rebuild`
**Status**: ✅ DEPLOYED (2026-02-08)
**Risiko**: Niedrig
**Problem**: `spl_token_2022::state::Account::unpack()` (via `Pack` trait) erwartet exakt 165 Bytes. Token-2022 Accounts mit Extensions (z.B. `ImmutableOwner` bei PumpFun-Token) sind 170+ Bytes. → `unpack()` scheitert stillschweigend → Token-2022 Balances werden als 0 behandelt → Liquidation überspringt diese Token.

### Root Cause

- `Pack::unpack()` prüft `input.len() != Self::LEN` (165) → `Err(InvalidAccountData)` bei 170 Bytes
- `if let Ok(ta) = ...unpack(...)` Pattern fängt den Error ab aber loggt ihn nicht → stilles Versagen
- Betrifft sowohl Startup-Bootstrap als auch Geyser-Laufzeit-Updates für Token-2022

### Fix

- 2 Stellen in `src/bin/market_data.rs` von `Account::unpack()` auf `StateWithExtensions::<Account>::unpack()` umgestellt
- `StateWithExtensions` extrahiert die Base-Account-Daten (165 Bytes) korrekt unabhängig von Extension-Daten

### Aufgaben

- [x] `src/bin/market_data.rs`: `publish_wallet_snapshot()` Token-2022 ATA-Verarbeitung → `StateWithExtensions`
- [x] `src/bin/market_data.rs`: Geyser ATA-Balance-Tracking für Token-2022 → `StateWithExtensions`
- [x] `cargo check` + `cargo clippy` erfolgreich (0 errors, 0 neue warnings)
- [x] Deploy + Test → Liquidation erfolgreich für Token-2022 Token ✅

### Verifizierung

- Server-Test: Beide Token-2022 Token (`9NLRXSZu...pump`, `2wqczBTB...pump`) hatten `data_len=170` (Extensions vorhanden)
- Nach Fix: Alle 3 Token korrekt liquidiert (1x SPL Token + 2x Token-2022)

---

## PR 3: Vault-Balance-Reads aus Cache (Orca, Raydium, Meteora, CPMM)

**Branch**: `architecture-rebuild`
**Status**: 🔧 IN PROGRESS
**Risiko**: Mittel
**Eliminiert**: 2-4 RPC-Calls pro Quote bei jedem dieser DEXe

### Gemeinsames Pattern

Jedes DEX-Modul bekommt:
1. Optionalen `SharedLivePoolCache` in der Struct
2. Vault-Balance-Lookup: Cache → In-Memory → RPC-Fallback
3. WARN-Log bei RPC-Fallback

### Aufgaben – Orca (`src/solana/dex/orca.rs`)

- [x] `Orca` Struct: `live_pool_cache: Option<SharedLivePoolCache>` Feld
- [x] `Orca::new_with_cache()`: LivePoolCache-Parameter hinzugefügt
- [x] `load_reserves_if_needed()`: LivePoolCache als höchste Priorität (vor SQLite + RPC)
- [x] RPC-Fallback mit WARN-Log markiert
- [ ] Metrik: `ORCA_RESERVE_CACHE_HIT` vs `ORCA_RESERVE_RPC_FALLBACK` (Future: Prometheus)

### Aufgaben – Raydium (`src/solana/dex/raydium.rs`)

- [x] `Raydium` Struct: `live_pool_cache: Option<SharedLivePoolCache>` Feld
- [x] `Raydium::new_with_live_cache()`: Cache-Parameter hinzugefügt
- [x] `fetch_and_update_reserves()`: LivePoolCache als höchste Priorität (vor RPC)
- [x] RPC-Fallback mit WARN-Log markiert
- [ ] `load_pool_from_geyser()`: 20-Retry entfernen (separate PR, P2)
- [ ] Metrik: `RAYDIUM_RESERVE_CACHE_HIT` vs `RAYDIUM_RESERVE_RPC_FALLBACK` (Future: Prometheus)

### Aufgaben – Meteora DLMM (`src/solana/dex/meteora_dlmm.rs`)

- [x] `MeteoraDlmm` Struct: `live_pool_cache: Option<SharedLivePoolCache>` Feld
- [x] `MeteoraDlmm::new_with_live_cache()`: Cache-Parameter hinzugefügt
- [x] `update_reserve_balances()`: LivePoolCache als höchste Priorität (vor RPC)
- [x] RPC-Fallback mit WARN-Log markiert
- [ ] Metrik: `METEORA_RESERVE_CACHE_HIT` vs `METEORA_RESERVE_RPC_FALLBACK` (Future: Prometheus)

### Aufgaben – Raydium CPMM (`src/solana/dex/raydium_cpmm.rs`)

- [x] `RaydiumCpmm` Struct: `live_pool_cache: Option<SharedLivePoolCache>` Feld
- [x] `RaydiumCpmm::new_with_live_cache()`: Cache-Parameter hinzugefügt
- [x] `update_reserve_balances()`: LivePoolCache als höchste Priorität (vor RPC)
- [x] RPC-Fallback mit WARN-Log markiert
- [ ] Metrik: `CPMM_RESERVE_CACHE_HIT` vs `CPMM_RESERVE_RPC_FALLBACK` (Future: Prometheus)

### Aufgaben – Execution Engine + tx_builder + cross_dex_handler

- [x] `src/bin/execution_engine.rs`: `run_liquidation_job()` — Cache an Orca, Raydium, Meteora
- [x] `src/bin/execution_engine.rs`: `run_burn_job()` — Cache an Raydium
- [x] `src/execution/tx_builder.rs`: Alle 6 DEX-Konstruktoren mit Cache (single-hop + multi-hop)
- [x] `src/solana/cross_dex_handler.rs`: `init_dexes()` — Cache an Raydium, Meteora, Orca
- [x] `src/bin/sell_all.rs`, `sell_all_keyless.rs`: Unverändert (`None`, Emergency-Tools ohne Cache)
- [x] `cargo check` + `cargo clippy` erfolgreich (0 errors, 0 warnings)

### Akzeptanzkriterien

- [ ] Vault-Balances kommen aus Cache wenn Geyser-Daten vorhanden
- [ ] RPC-Fallback funktioniert bei kaltem Cache (Startup, neue Pools)
- [ ] WARN-Log bei jedem RPC-Fallback
- [ ] Metriken zeigen Cache-Hit-Rate pro DEX
- [ ] Liquidation funktioniert weiterhin

---

## PR 4: tx_builder Orca-State aus Cache

**Branch**: `fix/txbuilder-orca-cache`
**Status**: ⬜ TODO
**Risiko**: Niedrig
**Eliminiert**: 1 RPC-Call im TX-Build-Pfad

### Aufgaben

- [ ] `src/execution/tx_builder.rs`: `fetch_orca_from_rpc()` umbauen zu `fetch_orca_state()`
- [ ] Neuer Lookup-Pfad: LivePoolCache `CachedPoolState::Orca` → RPC-Fallback
- [ ] `build_tx_plan()` hat bereits `cache: Option<&SharedLivePoolCache>` – nur an `fetch_orca_state()` durchreichen
- [ ] WARN-Log bei RPC-Fallback
- [ ] Metrik: `TX_BUILDER_ORCA_CACHE_HIT` vs `TX_BUILDER_ORCA_RPC_FALLBACK`

### Akzeptanzkriterien

- [ ] Orca-State kommt aus Cache wenn verfügbar
- [ ] RPC-Fallback bei Cache-Miss funktioniert
- [ ] TX-Build schlägt nicht fehl wenn Cache leer

---

## Allgemeine Regeln für alle PRs

### Vor dem Coden

- [ ] Plan in Cursor vorstellen, Approval holen
- [ ] Branch vom aktuellen `architecture-rebuild` erstellen

### Beim Coden

- [ ] RPC bleibt immer als letzter Fallback erhalten
- [ ] WARN-Log bei jedem RPC-Fallback im Hot-Path
- [ ] Metriken für Cache-Hit vs RPC-Fallback
- [ ] Keine neuen NATS-Topics (bestehende nutzen)
- [ ] Kleine, isolierte Commits

### Nach dem Coden

- [ ] `cargo build` erfolgreich
- [ ] `cargo clippy` keine neuen Warnings
- [ ] Bestehende Tests bestehen
- [ ] PR-Description mit Referenz auf Audit-Dokument
- [ ] Decision Record im Commit (Inputs, Checks, Outcome)

---

## PR 5: Blockhash via Geyser (kein RPC für Blockhash)

**Branch**: TBD
**Status**: ⬜ TODO
**Risiko**: Mittel (betrifft TX-Build + Simulation)
**Eliminiert**: 1-2 RPC `getLatestBlockhash` Calls pro TX (Simulation + Send)

### Hintergrund

Aktuell ruft `simulate_transaction()` und `send_transaction_rpc()` jeweils `ctx.rpc.rpc.get_latest_blockhash().await` auf. Das sind 100-300ms pro Call im Hot-Path. Yellowstone-gRPC bietet zwei Alternativen:
1. **`GetLatestBlockhash` gRPC Methode** – Direkt aus dem Validator, kein RPC-Roundtrip
2. **`blocks_meta` Subscription** – Streaming-basiert, Blockhash wird bei jedem neuen Block automatisch aktualisiert

### Aufgaben

- [ ] `src/bin/market_data.rs`: `blocks_meta` Geyser-Subscription hinzufügen
- [ ] `src/ipc/schema.rs`: `LatestBlockhash` NATS-Nachricht definieren (blockhash, last_valid_block_height, slot)
- [ ] `src/bin/market_data.rs`: Bei neuem Block → `LatestBlockhash` über NATS publizieren
- [ ] `src/bin/execution_engine.rs`: NATS-Subscription für `LatestBlockhash` → `AtomicBlockhash` updaten
- [ ] `src/bin/execution_engine.rs`: `simulate_transaction()` + `send_transaction_rpc()` → lokalen Blockhash verwenden statt RPC
- [ ] Fallback: Wenn lokaler Blockhash älter als X Slots → RPC-Fallback mit WARN-Log
- [ ] Metrik: `BLOCKHASH_SOURCE_GEYSER` vs `BLOCKHASH_SOURCE_RPC_FALLBACK`

### Akzeptanzkriterien

- [ ] Blockhash kommt aus Geyser-Stream (0 RPC Calls im Normalfall)
- [ ] Fallback auf RPC wenn Geyser-Stream ausfällt oder Blockhash zu alt
- [ ] TX-Simulation und -Send funktionieren identisch
- [ ] Kein Blockhash-Expiry in Produktion (Freshness-Check)
- [ ] Latenz-Verbesserung messbar (Prometheus Histogram)

---

## Nicht in Scope (Priorität 2+, separate PRs)

Diese Items sind im Audit dokumentiert aber NICHT Teil dieser PR-Serie:

- [ ] `pumpfun_amm.rs` eigene RPC-Infrastruktur refactoren (P2, #9)
- [ ] `raydium.rs:185` `load_pool_from_geyser()` 20-Retry entfernen (P2, #10) – teilweise in PR 3
- [ ] `execution_engine.rs:2071` Cleanup per RPC → JetStream (P2, #11)
- [ ] `wsol_manager.rs` Balance per RPC → Geyser (P2, #12)
- [ ] `execution.rs:129` Arb-Balance per RPC → Geyser (P2, #13)
- [ ] Killswitch-Retry-Logik für übersprungene Token (P3, #14-16)
