# PR-Plan: Hot-Path RPC Elimination (Priorität 1)

> Referenz: `docs/ARCHITECTURE_AUDIT_2026-02-07.md`
> Erstellt: 2026-02-07
> Ziel: Alle vermeidbaren RPC-Calls aus dem Hot-Path entfernen → 3-5x Latenz-Gewinn

---

## Übersicht

| PR | Titel | Status | Dateien | Aufwand |
|----|-------|--------|---------|---------|
| PR 1 | Globaler Mint-Decimals-Cache (GEYSER-FIRST) | 🔧 IN PROGRESS | 6 | ~3h |
| PR 2 | PumpFun Bonding-Curve Quotes aus Cache | ⬜ TODO | 2 | ~4h |
| PR 3 | Vault-Balance-Reads aus Cache (Orca/Raydium/Meteora/CPMM) | ⬜ TODO | 5 | ~5h |
| PR 4 | tx_builder Orca-State aus Cache | ⬜ TODO | 1 | ~1h |

---

## PR 1: Globaler Mint-Decimals-Cache (GEYSER-FIRST)

**Branch**: `fix/decimals-cache-geyser`
**Status**: 🔧 IN PROGRESS
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

## PR 2: PumpFun Bonding-Curve Quotes aus LivePoolCache

**Branch**: `fix/pumpfun-geyser-quotes`
**Status**: ⬜ TODO
**Risiko**: Mittel (Pump.fun = häufigster DEX)
**Eliminiert**: 200-2000ms pro Quote + 500-2000ms im TX-Build

### Aufgaben

- [ ] `src/solana/dex/pumpfun.rs`: `PumpFunDex::new()` um optionalen `SharedLivePoolCache` Parameter erweitern
- [ ] `src/solana/dex/pumpfun.rs`: `quote_exact_in()` umbauen:
  - Zuerst: `cache.get(bonding_curve)` → `CachedPoolState::PumpFun` → Quote aus cached `virtual_*_reserves` berechnen
  - Fallback: Bestehender RPC-Pfad (`fetch_bonding_curve_fast()`)
- [ ] `src/solana/dex/pumpfun.rs`: `build_swap_ix()` Creator-Auflösung:
  - Zuerst: `fallback_creator` Parameter (bereits vorhanden)
  - Dann: `cache.get_pumpfun_creator()` (bereits implementiert in LivePoolCache)
  - Letzter Fallback: RPC (mit WARN-Log, sollte in Produktion nicht passieren)
- [ ] `src/bin/execution_engine.rs`: Cache an `PumpFunDex::new()` durchreichen in:
  - `run_liquidation_job()` (Zeile ~1369)
  - Intent-Processing / TX-Build-Pfad
- [ ] Metrik: Counter `PUMPFUN_QUOTE_CACHE_HIT` vs `PUMPFUN_QUOTE_RPC_FALLBACK`
- [ ] Testen: Quote-Ergebnisse Cache vs RPC vergleichen (sollten identisch sein bei gleichem Slot)

### Akzeptanzkriterien

- [ ] Pump.fun Quotes kommen aus Cache wenn Geyser-Daten vorhanden
- [ ] Creator wird aus Cache aufgelöst (kein RPC im TX-Build)
- [ ] WARN-Log bei jedem RPC-Fallback
- [ ] Liquidation funktioniert weiterhin korrekt
- [ ] Kein Verhaltensunterschied für sell_all.rs (Emergency-Tool ohne Cache)

---

## PR 3: Vault-Balance-Reads aus Cache (Orca, Raydium, Meteora, CPMM)

**Branch**: `fix/vault-balances-from-cache`
**Status**: ⬜ TODO
**Risiko**: Mittel
**Eliminiert**: 2-4 RPC-Calls pro Quote bei jedem dieser DEXe

### Gemeinsames Pattern

Jedes DEX-Modul bekommt:
1. Optionalen `SharedLivePoolCache` in der Struct
2. Vault-Balance-Lookup: Cache → In-Memory → RPC-Fallback
3. WARN-Log bei RPC-Fallback

### Aufgaben – Orca (`src/solana/dex/orca.rs`)

- [ ] `Orca` Struct: `cache: Option<SharedLivePoolCache>` Feld
- [ ] `Orca::new()`: Cache-Parameter
- [ ] `load_reserves_if_needed()`: LivePoolCache `OrcaWhirlpoolState.vault_a_balance`/`vault_b_balance` als erste Lookup-Quelle
- [ ] `batch_refresh_vault_balances()`: Skip wenn Cache frisch (Geyser liefert Updates)
- [ ] Metrik: `ORCA_RESERVE_CACHE_HIT` vs `ORCA_RESERVE_RPC_FALLBACK`

### Aufgaben – Raydium (`src/solana/dex/raydium.rs`)

- [ ] `Raydium` Struct: `cache: Option<SharedLivePoolCache>` Feld
- [ ] `Raydium::new()`: Cache-Parameter
- [ ] `fetch_and_update_reserves()`: LivePoolCache `RaydiumAmmState.coin_reserve`/`pc_reserve` als erste Quelle
- [ ] `load_pool_from_geyser()`: Account-Daten direkt aus Geyser-Event parsen, 20-Retry-RPC-Loop entfernen
- [ ] Metrik: `RAYDIUM_RESERVE_CACHE_HIT` vs `RAYDIUM_RESERVE_RPC_FALLBACK`

### Aufgaben – Meteora DLMM (`src/solana/dex/meteora_dlmm.rs`)

- [ ] `MeteoraDlmm` Struct: `cache: Option<SharedLivePoolCache>` Feld
- [ ] `MeteoraDlmm::new()`: Cache-Parameter
- [ ] `update_reserve_balances()`: LivePoolCache `MeteoraState.reserve_x_balance`/`reserve_y_balance` als erste Quelle
- [ ] Pool-Account-Fetch (Zeile 216): Cache-Lookup statt RPC
- [ ] Metrik: `METEORA_RESERVE_CACHE_HIT` vs `METEORA_RESERVE_RPC_FALLBACK`

### Aufgaben – Raydium CPMM (`src/solana/dex/raydium_cpmm.rs`)

- [ ] `RaydiumCpmm` Struct: `cache: Option<SharedLivePoolCache>` Feld
- [ ] `RaydiumCpmm::new()`: Cache-Parameter
- [ ] `update_reserve_balances()`: LivePoolCache `RaydiumCpmmState.reserve_0`/`reserve_1` als erste Quelle
- [ ] Metrik: `CPMM_RESERVE_CACHE_HIT` vs `CPMM_RESERVE_RPC_FALLBACK`

### Aufgaben – Execution Engine

- [ ] `src/bin/execution_engine.rs`: Cache an alle DEX-Konstruktoren durchreichen:
  - `Orca::new()` mit Cache
  - `Raydium::new()` mit Cache
  - `MeteoraDlmm::new()` mit Cache
  - In `run_liquidation_job()` und generellem Intent-Processing

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

## Nicht in Scope (Priorität 2+, separate PRs)

Diese Items sind im Audit dokumentiert aber NICHT Teil dieser PR-Serie:

- [ ] `pumpfun_amm.rs` eigene RPC-Infrastruktur refactoren (P2, #9)
- [ ] `raydium.rs:185` `load_pool_from_geyser()` 20-Retry entfernen (P2, #10) – teilweise in PR 3
- [ ] `execution_engine.rs:2071` Cleanup per RPC → JetStream (P2, #11)
- [ ] `wsol_manager.rs` Balance per RPC → Geyser (P2, #12)
- [ ] `execution.rs:129` Arb-Balance per RPC → Geyser (P2, #13)
- [ ] Killswitch-Retry-Logik für übersprungene Token (P3, #14-16)
