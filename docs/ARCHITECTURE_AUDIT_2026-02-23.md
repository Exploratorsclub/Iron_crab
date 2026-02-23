# IronCrab Architektur-Audit – 2026-02-23

> **⚠️ DEPRECATED:** Dieses Dokument wurde in `ARCHITECTURE_AUDIT.md` (konsolidierte Fassung) zusammengeführt. Bitte `docs/ARCHITECTURE_AUDIT.md` verwenden.

---

## Kontext

Vollständiges Architektur-Audit der gesamten Codebasis mit Fokus auf:
1. **Architekturkonformität** gemäß TARGET_ARCHITECTURE.md und INVARIANTS.md
2. **RPC-Calls im Hot Path** – welche können durch Geyser-Daten ersetzt werden
3. **Single Source of Truth (SSOT)** – Verletzungen und potenzielle Inkonsistenzen
4. **Logische Fehler** – Bugs und Architektur-Widersprüche

**Referenzdokumente:** `ARCHITECTURE_AUDIT_2026-02-07.md`, `INVARIANTS.md`, `KNOWN_BUG_PATTERNS.md`, `.cursor/rules/ironcrab-core.mdc`

---

## Legende

| Symbol | Bedeutung |
|--------|------------|
| **KRITISCH** | RPC im Hot-Path (Buy/Sell/Arb) – direkte Latenz-Auswirkung |
| **VERSTOSS** | RPC wo Geyser/LivePoolCache-Daten vorhanden sind/sein sollten |
| **AKZEPTABEL** | Unvermeidlich (Simulation, TX-Send, Blockhash) |
| **BOOTSTRAP** | Einmalige Initialisierung beim Start |
| **COLD PATH** | Liquidation, Manual Actions – RPC per Architektur erlaubt |

---

## 1. RPC-CALLS IM HOT PATH (Geyser-Ersetzbarkeit)

### 1.1 DEX-Module – KRITISCHSTE VERSTÖSSE

#### `src/solana/dex/pumpfun.rs` – Pump.fun Bonding Curve

| Zeile | Call | Problem | Geyser-Alternative |
|-------|------|---------|---------------------|
| 309 | `get_account_retry(bonding_curve)` in `fetch_bonding_curve()` | **KRITISCH** – BC-Fetch bei Cache-Miss | LivePoolCache `CachedPoolState::PumpFun` via Geyser |
| 322 | `get_account(bonding_curve)` in `fetch_bonding_curve_fast()` | **KRITISCH** – RPC in Retry-Loop (bis 5×200ms) | Geyser-Event bei Pool-Create |
| 769 | `get_account_retry(token_mint)` | **VERSTOSS** – Token-Mint-Existenz-Check | Geyser TokenMintInfo |
| 1124 | `get_account_retry(bonding_curve)` in `build_swap_ix_async` | **KRITISCH** – Creator-Auflösung im TX-Build | LivePoolCache Creator |

**Status:** PumpFun hat GEYSER-FIRST für Quote (`get_bonding_curve_from_cache`), aber bei Cache-Miss erfolgen bis zu 5 RPC-Retries. Creator-RPC in `build_swap_ix_async` im TX-Build-Pfad.

---

#### `src/solana/dex/pumpfun_amm.rs` – PumpSwap AMM

| Zeile | Call | Problem | Geyser-Alternative |
|-------|------|---------|---------------------|
| 483, 659 | `get_multiple_accounts(chunk)` | **KRITISCH** – Pool-Discovery/Account-Resolve per RPC | LivePoolCache `get_pump_amm_reserves_by_base_mint`, `pool_accounts` |
| 542, 815, 968 | `rpc_get_account_owner_executable_and_data()` | **KRITISCH** – PDA/ATA-Prüfung im Hot Path | Geyser-Account-Subscription, LivePoolCache |
| 308-310 | `get_token_accounts_by_owner_with_filter()` | **KRITISCH** – Token-Account-Discovery | Geyser-Wallet-Subscription |
| 1843, 1921 | `get_account_opt_retry`, `get_account_retry` | **KRITISCH** – Pool/Load-Fetch | LivePoolCache |

**Status:** `PumpFunAmmDex` hat `new_with_cache()` und GEYSER-FIRST-Logik (Zeilen 182-201, 1324-1371, 2185-2236). Bei Cache-Hit: 0 RPC. Bei Cache-Miss: ausgiebige RPC-Fallbacks. execution-engine und CrossDexHandler nutzen `new_with_cache` wenn LivePoolCache verfügbar.

---

#### `src/solana/dex/raydium.rs` – Raydium AMM V4

| Zeile | Call | Problem | Geyser-Alternative |
|-------|------|---------|---------------------|
| 194 | `get_account_retry(pool_address)` in `load_pool_from_geyser()` | **KRITISCH** – Funktion heißt "from_geyser", macht aber RPC! Bis zu 3 Retries | Geyser-Account-Update parsen |
| 791 | `get_account(market_id)` | **VERSTOSS** | LivePoolCache Serum-Accounts |
| 1276 | `get_account_retry(market_id)` in `fetch_and_populate_serum_accounts` | **KRITISCH** – Serum-Market im Hot Path | Cache oder Bootstrap |
| 1324-1325 | `get_token_account_balance()` in `fetch_and_update_reserves()` | **KRITISCH** – Vault-Balances on-demand | Geyser → LivePoolCache |

**Status:** `load_pool_from_geyser()` ist irreführend benannt – führt RPC durch. LivePoolCache-Priorität in `fetch_and_update_reserves()` (Zeile 1305-1331) vorhanden.

---

#### `src/solana/dex/raydium_cpmm.rs` – Raydium CPMM

| Zeile | Call | Problem | Geyser-Alternative |
|-------|------|---------|---------------------|
| 237-238 | `get_account_retry(vault_0/1)` | **KRITISCH** – Vault-Balances per RPC | Geyser → LivePoolCache |

**Status:** `new_with_live_cache()` existiert. Bei LivePoolCache-Hit: 0 RPC. Bei Miss: RPC-Fallback.

---

#### `src/solana/dex/orca.rs` – Orca Whirlpool

| Zeile | Call | Problem | Geyser-Alternative |
|-------|------|---------|---------------------|
| 367 | `get_account_retry(pool_id)` | **VERSTOSS** – Pool-Load | LivePoolCache |
| 440 | `get_multiple_accounts([vault_a, vault_b])` | **KRITISCH** – Vault-Balances bei Cache-Miss | Geyser → LivePoolCache |
| 555 | `get_multiple_accounts(vault_pubkeys)` | **KRITISCH** | LivePoolCache |
| 1372 | `get_multiple_accounts([tick_array_*])` in `build_swap_ix_async` | **KRITISCH** – Tick-Array-Validierung | Geyser oder pre-cached |
| 1510, 1530-1531 | `get_account_retry()` | **VERSTOSS** | LivePoolCache |

**Status:** Gemäß `AUDIT_F_ORCA_RESERVES_IMPLEMENTATION_PLAN.md` – LivePoolCache einzige Reserve-Quelle im Hot Path. Bei Cache-Miss: statische Reserves, **kein RPC** (Zeile 409-431). RPC nur im Cold Path (`live_pool_cache.is_none()`).

---

#### `src/solana/dex/meteora_dlmm.rs` – Meteora DLMM

| Zeile | Call | Problem | Geyser-Alternative |
|-------|------|---------|---------------------|
| 240 | `get_account(pool_addr)` | **VERSTOSS** – Vault-Adressen bei zero/default | LivePoolCache |
| 269-270 | `get_account_retry(reserve_x/y)` | **KRITISCH** – Vault-Balances per RPC | Geyser → LivePoolCache |
| 480 | `get_account_retry(pool_address)` | **VERSTOSS** | LivePoolCache |

**Status:** `new_with_live_cache()` vorhanden. LivePoolCache-Priorität in `update_reserve_balances()` (Zeile 206-265). Bei Miss: RPC-Fallback.

---

### 1.2 TX-Infrastruktur

#### `src/execution/tx_builder.rs`

| Zeile | Call | Problem | Geyser-Alternative |
|-------|------|---------|---------------------|
| 218 | `get_account(pool_id)` in `fetch_orca_from_rpc()` | **KRITISCH** – Orca-Pool-Fetch als Fallback im TX-Build | LivePoolCache `CachedPoolState::Orca` |
| 523 | `load_pool_from_geyser()` Raydium | **KRITISCH** – Bis zu 3×300ms RPC-Retries | Geyser-Update direkt |
| 1378 | `load_pool_from_geyser()` Multi-hop Raydium | **KRITISCH** | Gleiche Lösung |
| 1518 | `load_pool_by_address()` Multi-hop Meteora | **KRITISCH** | LivePoolCache |

---

### 1.3 Arbitrage – Hot Path

#### `src/solana/execution.rs`

| Zeile | Call | Problem | Geyser-Alternative |
|-------|------|---------|---------------------|
| 129 | `get_balance_retry(wallet)` | **VERSTOSS** – Balance-Check vor Arb-Execution | Geyser-Wallet-Tracking, LockManager available_balance |

**Kontext:** Arb-Execution ist Hot Path. Balance für Gas-Check sollte aus LockManager oder JetStream-Wallet-Snapshot kommen.

---

### 1.4 Arbitrage (`src/solana/arbitrage.rs`)

| Zeile | Call | Bewertung |
|-------|------|-----------|
| 315 | `get_latest_blockhash()` | AKZEPTABEL – Simulation |
| 328 | `simulate_transaction()` | AKZEPTABEL – Simulate-gated |

---

### 1.5 AKZEPTABEL (Simulation, TX-Send, Bootstrap)

| Modul | Call | Bewertung |
|-------|------|-----------|
| execution_engine | `get_latest_blockhash()`, `simulate_transaction()`, `send_transaction_*()` | AKZEPTABEL |
| execution_engine | `get_token_accounts_by_owner()` in `rpc_wallet_scan_for_liquidation()` | COLD PATH |
| execution_engine | `get_account(bc)` Creator-Fallback bei Liquidation | COLD PATH |
| execution_engine | `cleanup_wallet_after_liquidation()` | COLD PATH |
| tx_sender | `send_transaction_with_config()` | AKZEPTABEL |
| tpu_client | `get_slot()` | AKZEPTABEL |
| market_data | `get_multiple_accounts()` beim Bootstrap | BOOTSTRAP |
| sell_all, sell_all_keyless | RPC-Calls | COLD PATH (Emergency-Tools) |
| wallet, wsol_manager | `send_and_confirm_transaction()` | AKZEPTABEL |
| account_janitor | `send_and_confirm_transaction()` | AKZEPTABEL |

---

### 1.6 Zusammenfassung RPC im Hot Path

| Modul | Anzahl kritischer Stellen | Status |
|-------|--------------------------|--------|
| pumpfun.rs | 4 | GEYSER-FIRST bei Quote; RPC bei Cache-Miss + Creator |
| pumpfun_amm.rs | 6+ | GEYSER-FIRST implementiert; RPC bei Cache-Miss |
| orca.rs | 2 (Fallback) | LivePoolCache-only im Hot Path; RPC nur Cold Path |
| raydium.rs | 4 | LivePoolCache-Priorität; `load_pool_from_geyser` irreführend |
| raydium_cpmm.rs | 2 | LivePoolCache-Priorität |
| meteora_dlmm.rs | 3 | LivePoolCache-Priorität |
| tx_builder.rs | 4 | Fallbacks bei Cache-Miss |
| execution.rs (arb) | 1 | **VERSTOSS** – get_balance_retry |

**Geschätzte Latenz-Optimierung:** Durch konsequente Geyser-Nutzung: 3–8× schneller (aktuell ~1,5–8s → Ziel ~0,2–0,9s).

---

## 2. SINGLE SOURCE OF TRUTH (SSOT)

### 2.1 MASTER/SLAVE Pool-Cache-Architektur ✅

**Design (pool_cache_sync.rs, jetstream.rs):**

```
market-data (MASTER LivePoolCache)
    │
    ├── Geyser → parse_pool_account → LivePoolCache.upsert()
    ├── publish PoolCacheUpdate → JetStream (ironcrab.pool_cache.*)
    │
    ├──→ execution-engine (SLAVE) – bootstrap + incremental consumer
    └──→ momentum-bot (SLAVE) – bootstrap + incremental consumer
```

**Bewertung:** Korrekt. market-data ist **einziger Schreiber** für Pool-State. execution-engine und momentum-bot lesen nur via JetStream. Kein SSOT-Verstoß.

---

### 2.2 tx_builder Write-Back in SLAVE LivePoolCache

**Stellen:** `tx_builder.rs` Zeilen 601, 1529:

```rust
// Write back to SLAVE LivePoolCache for subsequent trades
```

**Kontext:** Wenn tx_builder Pool-State via RPC lädt (z.B. Raydium `load_pool_from_geyser`), schreibt er diesen in den lokalen LivePoolCache.

**Bewertung:** **Potenzielle SSOT-Weichheit.** Der SLAVE-Cache erhält Daten aus zwei Quellen:
1. JetStream (MASTER → SLAVE Sync)
2. RPC-Fallback-Write-Back aus tx_builder

**Risiko:** RPC-Daten können älter oder neuer als Geyser-Daten sein. Für "subsequent trades" in derselben Session ist das pragmatisch, aber es gibt kein klares Vorrang-Schema bei Konflikten.

**Empfehlung:** Explizit dokumentieren oder Timestamp/Slot-basiertes Merge (neuerer Slot gewinnt).

---

### 2.3 Mint-Decimals – Mehrere Quellen

**Quellen für Token-Decimals:**
1. **LivePoolCache** (`set_mint_decimals`, `get_mint_decimals`) – von market-data/Geyser
2. **token_utils** – LivePoolCache → RPC `get_token_supply`/`get_account` Fallback
3. **Geyser** `post_token_balances` / `TokenMintInfo`
4. **mint_infos** Cache in momentum_bot

**Bewertung:** LivePoolCache und Geyser sind autoritativ. token_utils nutzt LivePoolCache zuerst. Cold-Path-Fallback auf RPC ist akzeptabel. **Kein klarer SSOT-Verstoß**, aber mehrere Caches (mint_infos vs. LivePoolCache) – Konsistenz abhängig von Sync-Reihenfolge.

---

### 2.4 Wallet-Balance / LockManager

**Quellen:**
1. **JetStream** `WalletBalanceSnapshot` (market-data → execution-engine)
2. **LockManager** – `available_sol`, `available_wsol` (gefeed von ExecutionResult, RPC-Fallback)
3. **RPC** `get_balance`, `get_token_accounts_by_owner` – Liquidation, Bootstrap

**Bewertung:** LockManager ist autoritativ für "available for trading". ExecutionResult + JetStream füttern ihn. RPC nur im Cold Path. **SSOT eingehalten.**

---

### 2.5 Positions / TrackerState

**Autoritativ:** momentum_bot `PositionTracker`, `PersistedPosition` – aus ExecutionResult + TokenTracker.

**Bewertung:** Keine Duplikation. execution-engine schreibt ExecutionResult; momentum-bot konsumiert. **SSOT eingehalten.**

---

### 2.6 DEX-Namen / quote_mint

**Problem (KNOWN_BUG_PATTERNS §12):** `quote_mint = SOL_MINT_PUBKEY` war in einigen DEX-Parsern hardcodet.

**Aktueller Stand (dex_parser.rs):**
- Raydium AMM V4: `extract_quote_mint()` aus Token-Balances ✅
- Orca: `quote_mint` aus Parsed-Whirlpool ✅
- Meteora DLMM: `extract_quote_mint()` ✅
- Raydium CPMM: `quote_mint` aus Vault-Mints ✅
- PumpFun BC: `quote_mint: *SOL_MINT_PUBKEY` (Zeile 901) – Bonding Curve ist immer SOL-quoted ✅
- PumpFun AMM: `quote_mint` hardcodet als WSOL (Zeile 952), obwohl `instruction_accounts[4]` der echte quote_mint wäre – für TOKEN/USDC-Pools könnte das falsche Arb-Signale liefern ⚠️

**Bewertung:** Die früheren hardcodierten Fälle (Meteora, Raydium CPMM) sind gefixt. PumpFun AMM: besser `instruction_accounts[4]` nutzen als Fallback für non-SOL-Quotes.

---

## 3. LOGISCHE FEHLER & ARCHITEKTUR-WIDERSPRÜCHE

### 3.1 Irreführender Funktionsname: `load_pool_from_geyser()`

**Datei:** `raydium.rs` Zeile ~185  
**Problem:** Name suggeriert Geyser-Nutzung, Implementierung macht RPC-Calls.  
**Empfehlung:** Umbenennen in `load_pool_from_rpc()` oder `load_pool_rpc_fallback()`.

---

### 3.2 CrossDexHandler ohne LivePoolCache

**Kontext:** CrossDexHandler wird mit `with_pool_cache()` initialisiert, wenn `live_pool_cache` gesetzt ist. Wenn nicht (z.B. NATS nicht verbunden), nutzt PumpFunAmm `new()` ohne Cache → vollständiger RPC-Pfad.

**Bewertung:** Architektonisch vertretbar (Arb optional). Sollte aber geloggt werden, wenn CrossDexHandler ohne Cache läuft.

---

### 3.3 sell_all / sell_all_keyless ohne LivePoolCache

**Dateien:** `sell_all.rs`, `sell_all_keyless.rs`  
**Kontext:** Nutzen `Raydium::new()`, `PumpFunDex::new(..., None)`, `PumpFunAmmDex::new()` – keine LivePoolCache-Integration.

**Bewertung:** Cold-Path-Tools (Emergency/Manual). RPC per Architektur erlaubt. **Kein Verstoß.**

---

### 3.4 Arb-Execution get_balance_retry

**Datei:** `execution.rs` Zeile 129  
**Problem:** Balance-Check per RPC vor Arb-TX-Build. Arb ist Hot Path.  
**Empfehlung:** LockManager `available_sol` oder JetStream Wallet-Snapshot verwenden.

---

### 3.5 Token-2022 / Custom token_program

**Kontext (KNOWN_BUG_PATTERNS §16):** token_program muss aus Trade/ExecutionResult kommen, nicht hardcodet.  
**Status:** LivePoolCache speichert `token_a_program`/`token_b_program`. cross_dex_handler nutzt Intent + Cache. **Architektur passt.**

---

## 4. ARCHITEKTURKONFORMITÄT

### 4.1 Hot Path = GEYSER-ONLY (INVARIANTE I-4)

| Bereich | Konform? | Anmerkung |
|---------|----------|-----------|
| Momentum Buy/Sell | Teilweise | PumpFun, PumpFunAmm, Orca, Raydium, Meteora haben GEYSER-FIRST; RPC bei Cache-Miss |
| Arb Execution | Nein | `get_balance_retry` in execution.rs |
| TX-Build | Teilweise | Fallbacks zu RPC bei Cache-Miss; Creator-RPC bei PumpFun |
| Liquidation | Ja | RPC im Cold Path erlaubt |

---

### 4.2 Cold Path RPC (INVARIANTE I-5)

Liquidation, cleanup_wallet_after_liquidation, Manual Burn, sell_all, Bootstrap: RPC korrekt genutzt. **Konform.**

---

### 4.3 Single-Signer / Intent-Only (INVARIANTS I-1, I-2)

- Nur execution-engine lädt Keys und signiert.
- market-data, momentum-bot, arb-strategy erzeugen nur TradeIntent / MarketEvents.  
**Konform.**

---

### 4.4 Simulate-gated (INVARIANTE I-9)

Simulation vor Send wird eingehalten. **Konform.**

---

### 4.5 Pool-Matching (INVARIANTE I-13)

Preis-Updates für Positionen nur wenn `source_pool == position.pool`. FIX-38. **Konform.**

---

## 5. EMPFEHLUNGEN (Priorisiert)

### Priorität 1 – Sofort

| # | Aktion | Datei/Modul |
|---|--------|-------------|
| 1 | Arb Balance-Check: RPC ersetzen durch LockManager/JetStream | `execution.rs` |
| 2 | `load_pool_from_geyser` umbenennen in `load_pool_from_rpc` | `raydium.rs` |

### Priorität 2 – Kurzfristig

| # | Aktion |
|---|--------|
| 3 | PumpFun Creator: Vollständig aus LivePoolCache, kein RPC in `build_swap_ix_async` |
| 4 | tx_builder Write-Back: Slot/Timestamp-Merge dokumentieren oder einbauen |
| 5 | CrossDexHandler ohne Cache: explizit loggen |

### Priorität 3 – Mittelfristig

| # | Aktion |
|---|--------|
| 6 | Raydium `load_pool_from_geyser`: Geyser-Account-Update direkt parsen |
| 7 | Orca Tick-Array: Geyser-Subscription oder Pre-Cache |
| 8 | Meteora/ Raydium Vault-Balances: Konsequente Geyser-Subscription |

---

## 6. TABELLE: RPC-STELLEN NACH PFAD

| Pfad | Modul | RPC-Call | Ersetzbar durch |
|------|-------|----------|-------------------|
| Hot | pumpfun | fetch_bonding_curve (Cache-Miss) | LivePoolCache |
| Hot | pumpfun | build_swap_ix creator | LivePoolCache |
| Hot | pumpfun_amm | Pool-Discovery, Reserves (Cache-Miss) | LivePoolCache |
| Hot | raydium | load_pool_from_geyser | Geyser-Parse |
| Hot | raydium | fetch_and_update_reserves (Miss) | LivePoolCache |
| Hot | raydium_cpmm | Vault-Fetch (Miss) | LivePoolCache |
| Hot | meteora_dlmm | update_reserve_balances (Miss) | LivePoolCache |
| Hot | orca | Vault-Fetch (nur Cold Path) | N/A |
| Hot | tx_builder | fetch_orca_from_rpc, load_pool_* | LivePoolCache |
| Hot | execution (arb) | get_balance_retry | LockManager/JetStream |
| Cold | execution_engine | Liquidation RPC-Fallbacks | By-Design |
| Cold | cleanup_wallet | get_token_accounts, get_account | By-Design |
| Bootstrap | market_data | get_multiple_accounts | By-Design |

---

*Audit erstellt: 2026-02-23*
