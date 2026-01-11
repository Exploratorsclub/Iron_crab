# Architecture Violations & Required Changes

**Erstellt:** 2026-01-11  
**Aktualisiert:** 2026-01-11 (Jito Config Fix deployed)  
**Status:** 🟡 Jito Config gefixt - Noch R2 (pump_amm pool discovery) offen  
**Source of Truth:** `docs/TARGET_ARCHITECTURE.md`, `docs/ROLE_SEPARATION.md`

---

## 🚨 Aktuelle Reject-Gründe (Live Decision Records 2026-01-11)

Nach Deploy werden arb-intents mit folgenden Fehlern rejected:

### ✅ R1: "orca user authority not set" - BEHOBEN
- **Status:** FIXED in commit b401e14
- **Lösung:** `set_user_authority()` wird jetzt aufgerufen für alle DEX Connectors

### ✅ R3: "Intent requires atomic bundle but Jito not configured" - BEHOBEN
- **Status:** FIXED in commit 17cfeec
- **Ursache:** `ExecutionConfig` wurde mit `..Default::default()` initialisiert, Jito-Werte aus TOML ignoriert
- **Lösung:** 
  - `jito_enabled`, `jito_tip_lamports`, `jito_region` zu Root `Config` struct hinzugefügt
  - execution-engine lädt Jito config jetzt aus TOML-Datei
  - Logging für Jito config values beim Startup hinzugefügt

### ❌ R2: "pump_amm pool not discovered/cached for base_mint=So11..." - OFFEN
- **Check:** `tx_plan`
- **Detail:** base_mint ist WSOL (`So11...2`) statt des Token-Mints
- **Ursache:** `load_pool_by_address()` fügt Pool in `pools_by_base` mit `base_mint` ein, aber bei manchen Intents wird WSOL als base_mint interpretiert
- **Hypothese:** arb-strategy sendet buy_pool/sell_pool vertauscht ODER Intent hat falschen token_mint
- **Nächster Schritt:** Debug-Log um Intent-Inhalte zu analysieren

---

## Executive Summary

Während der Fehlerbehebung für "No buy quote available from meteora_dlmm/pump_amm" wurden mehrere architektonische Prinzipien verletzt:

1. **~~Pool Discovery in der execution-engine~~** ✅ BEHOBEN - `discover_pool_on_demand()` entfernt
2. **~~RPC-basierte On-Demand Discovery (arb-strategy)~~** ✅ BEHOBEN - CrossDexHandler nutzt jetzt Intent-Metadaten
3. **~~Duplizierte DEX Connector Instanzen~~** ✅ BEHOBEN - execution-engine lädt Pools nur via `load_pool_by_address()` (single getAccount, kein getProgramAccounts)
4. **Pool State nicht über MarketEvents propagiert** ❌ OFFEN - arb-strategy muss Quote-Daten im Intent senden (bereits vorhanden, aber execution-engine muss validieren)
5. **Momentum-Bot Pfad macht RPC Quotes** ❌ OFFEN - execution.rs ruft quote_exact_in() für SELL-Intents auf

---

## Bereits durchgeführte Fixes

### ✅ F1: `discover_pool_on_demand()` aus MeteoraDlmm entfernt
- **Datei:** `src/solana/dex/meteora_dlmm.rs`
- **Änderung:** RPC-basierte Pool Discovery komplett entfernt
- **Verhalten jetzt:** Wenn Pool nicht im Cache → Quote schlägt fehl → Intent wird rejected

### ✅ F2: CrossDexHandler nutzt Intent-Metadaten statt RPC Quotes
- **Datei:** `src/solana/cross_dex_handler.rs`
- **Änderung:** `validate_arb_opportunity()` extrahiert `spread_bps`, `estimated_profit_lamports`, `buy_price`, `sell_price` aus Intent-Metadaten
- **Entfernt:** `estimate_min_amount_in_for_target_out()` (RPC binary search)
- **Verhalten jetzt:** arb-strategy ist Source of Truth für Quote-Daten

### ✅ F3: `load_pool_by_address()` für Intent-basiertes Pool Loading
- **Dateien:** `src/solana/dex/mod.rs`, `raydium.rs`, `orca.rs`, `meteora_dlmm.rs`, `pumpfun_amm.rs`
- **Änderung:** Neues Dex-Trait-Methode `load_pool_by_address(&self, pool_address: &Pubkey) -> Result<()>`
- **Verhalten:** Lädt einzelne Pools via single getAccount RPC (akzeptabel) basierend auf Intent-Metadaten
- **Wichtig:** KEIN getProgramAccounts - nur spezifische Pool-Adressen aus dem Intent
- **CrossDexHandler:** Ruft `load_pool_by_address()` vor `build_swap_ix()` auf

---

## Verstöße gegen TARGET_ARCHITECTURE.md

### ~~V1: Pool Discovery außerhalb des Data Plane~~ ✅ BEHOBEN

**Spezifikation (Sektion 2.1 + 4.2):**
> "Data Plane: `market-data` (Rust) - Aufgabe: **einmalige** Markt-Daten-Ingestion und Normalisierung."
> 
> "GeyserPoolDiscovery handles ALL pool discovery via Geyser events"
> 
> "DEX Connectors: Store pool state received from `MarketEvents` (not RPC!)"

**Aktuelle Implementierung (❌ FALSCH):**
- `MeteoraDlmm::discover_pool_on_demand()` in `src/solana/dex/meteora_dlmm.rs` (Lines 75-145)
- `PumpFunAmmDex::discover_pool_static()` in `src/solana/dex/pumpfun_amm.rs`
- Diese Methoden machen RPC `getProgramAccounts` Calls
- `CrossDexHandler` in execution-engine ruft diese Discovery-Methoden auf

**Problem:**
- Discovery findet an zwei Stellen statt: market-data (Geyser) + execution-engine (RPC)
- Duplizierte, inkonsistente Pool-Daten
- RPC-Discovery ist 40-80x langsamer als Geyser
- Verstößt gegen "Data Plane lädt Daten **einmal**"

---

### V2: RPC statt Geyser für Pool Discovery

**Spezifikation (Sektion 4.2):**
> "**OLD (❌ Wrong):** DEX Connectors call `refresh_pools()` via RPC"
> 
> "**NEW (✅ Correct):** GeyserPoolDiscovery handles ALL pool discovery"
> 
> "`refresh_pools()` exists ONLY as fallback for: Bootstrap/initialization, Testing/development, Emergency fallback"

**Aktuelle Implementierung (❌ FALSCH):**
- `discover_pool_on_demand()` macht `getProgramAccounts` im Hot Path
- `quote_exact_in()` ruft on-demand RPC Discovery auf
- Kein Fallback-Flag, immer aktiv

**Problem:**
- RPC im Trading Hot Path = 400-800ms Latenz + Rate Limits
- Verstößt gegen "Geyser-First Architecture"
- Erzeugt RPC Timeouts (wie in den Decision Records gesehen)

---

### V3: DEX Connectors haben eigene RPC-Instanzen

**Spezifikation (Sektion 4.2):**
> "DEX Connectors: Provide `quote_exact_in()` for pricing, Provide `build_swap_ix()` for transaction building, Store pool state **received from MarketEvents**"

**Aktuelle Implementierung (❌ FALSCH):**
- `MeteoraDlmm::new(rpc)` - hat eigene RPC Instanz
- `PumpFunAmmDex::new(rpc, rpc_url, ...)` - hat eigene RPC Instanz
- DEX Connectors fetchen Pool-Daten selbst via RPC
- `CrossDexHandler` in execution-engine initialisiert eigene DEX Connector Instanzen

**Problem:**
- Pool State ist nicht synchron zwischen market-data und execution-engine
- Jeder Prozess hat eigene Sicht auf Pool-Daten
- Verstößt gegen Single Source of Truth

---

### V4: execution-engine hat Data Plane Verantwortlichkeiten

**Spezifikation (Sektion 2.3):**
> "Execution Plane: `execution-engine` (Rust) - Einzige Instanz mit Keys. Aufgaben:
> - Global Arbitration
> - Capital Locks + Resource Locks  
> - Tx Plan → Simulate → Send → Confirm
> - Fee/Compute/Tip Policy zentral"

**NICHT** in der Spezifikation für execution-engine:
- Pool Discovery
- DEX State Management
- RPC Data Fetching

**Aktuelle Implementierung (❌ FALSCH):**
- `CrossDexHandler::init_dexes()` initialisiert DEX Connectors
- `CrossDexHandler::validate_arb_opportunity()` macht RPC Calls für Quotes
- `update_reserve_balances()` fetcht Vault Balances via RPC

**Problem:**
- execution-engine macht Data Plane Arbeit
- Vermischt Signing/Execution mit Data Ingestion
- Verstößt gegen Separation of Concerns

---

### V5: Pool State nicht in MarketEvents/Intents

**Spezifikation (Sektion 4.1):**
> ```
> Geyser Account Update (New Pool)
>     ↓
> GeyserPoolDiscovery::process_account_update()
>     ↓
> Parse pool data (mint, vaults, fee, reserves)
>     ↓
> PoolDiscoveryEvent
>     ↓
> market-data publishes MarketEvent::PoolCreated
>     ↓
> Strategies (momentum-bot, arb-strategy) receive event
> ```

**Aktuelle Implementierung (❌ TEILWEISE):**
- `MarketEvent::PoolCreated` existiert, aber enthält nicht alle Pool-Daten
- `TradeIntent` enthält nur Pool-Adressen, keine Pool-State (Reserves, Vaults)
- arb-strategy hat Pool-Daten (von Geyser), aber execution-engine nicht

**Problem:**
- execution-engine muss Pool-Daten re-discovern
- Intent enthält nicht genug Informationen für Quote-Validation
- arb-strategy's Pool-Sicht ist nicht mit execution-engine synchron

---

### V6: Momentum-Bot Pfad macht RPC Quotes in execution-engine

**Spezifikation (Sektion 4.2):**
> "DEX Connectors: Store pool state **received from MarketEvents** (not RPC!)"
> 
> "execution-engine: Use Intent metadata for validation, NOT RPC calls"

**Aktuelle Implementierung (❌ FALSCH):**
- `src/solana/execution.rs` ruft `quote_exact_in()` für SELL-Intents auf
- `src/solana/dex/router.rs` macht RPC-basierte Quote-Berechnung
- Momentum-Bot sendet Intents ohne Quote-Metadaten
- execution-engine muss Quote selbst via RPC ermitteln

**Betroffene Dateien:**
- `src/solana/execution.rs` - `handle_sell_intent()` 
- `src/solana/dex/router.rs` - `quote_exact_in()` calls
- `src/bin/momentum_bot.rs` - sendet keine Quote-Metadaten im Intent

**Problem:**
- RPC Latenz im Trading Hot Path (400-800ms)
- Inkonsistente Quotes zwischen momentum-bot (Geyser) und execution-engine (RPC)
- Verstößt gegen "arb-strategy/momentum-bot ist Source of Truth für Quotes"

**Soll-Zustand:**
```rust
// momentum-bot (HAT Geyser-Daten) sollte Quote-Daten mitschicken:
intent.metadata.insert("sell_quote_amount_out", quote.amount_out.to_string());
intent.metadata.insert("sell_quote_price", price.to_string());
intent.metadata.insert("sell_pool", pool_address.clone());
intent.metadata.insert("sell_dex", dex_name.clone());

// execution-engine sollte diese Daten NUR validieren, NICHT re-quoten
```

---

## Required Changes (Priorisiert)

### P0 - Kritisch (vor Production)

#### C1: Pool State über MarketEvents propagieren
**Datei:** `src/ipc/schema.rs`

```rust
// MarketEvent::PoolCreated sollte enthalten:
pub struct PoolCreatedEvent {
    pub pool_address: String,
    pub dex: String,
    pub mint_a: String,
    pub mint_b: String,
    pub vault_a: String,        // NEU
    pub vault_b: String,        // NEU
    pub reserve_a: u64,         // NEU
    pub reserve_b: u64,         // NEU
    pub fee_bps: u32,           // NEU
    pub tick_spacing: Option<i32>, // NEU (für DLMM/Whirlpool)
}
```

#### C2: Pool State in TradeIntent Metadata ✅ BEREITS VORHANDEN
**Datei:** `src/bin/arb_strategy.rs`

arb-strategy sendet **bereits** alle Quote-Daten im Intent (siehe Lines 800-840):
```rust
intent.metadata.insert("spread_bps", spread_bps.to_string());
intent.metadata.insert("estimated_profit_lamports", profit.to_string());
intent.metadata.insert("buy_price", buy_price.to_string());
intent.metadata.insert("sell_price", sell_price.to_string());
intent.metadata.insert("buy_dex", buy_dex.clone());
intent.metadata.insert("sell_dex", sell_dex.clone());
intent.metadata.insert("buy_pool", buy_pool.clone());
intent.metadata.insert("sell_pool", sell_pool.clone());
```

CrossDexHandler validiert jetzt nur noch diese Intent-Metadaten (✅ ERLEDIGT).

#### C5: Momentum-Bot SELL Path auf Intent-Metadaten umstellen ❌ OFFEN
**Betroffene Dateien:**
- `src/bin/momentum_bot.rs` - muss Quote-Daten im SELL-Intent senden
- `src/solana/execution.rs` - muss Intent-Metadaten statt RPC-Quotes verwenden

**Aktueller Zustand:**
```rust
// momentum-bot sendet SELL-Intent OHNE Quote-Daten:
let intent = TradeIntent::new_sell(...);
// Keine metadata für sell_quote, sell_pool, sell_dex

// execution-engine muss dann RPC-Quote holen:
let quote = dex_connector.quote_exact_in(token_mint, SOL_MINT, amount).await?;
```

**Soll-Zustand:**
```rust
// momentum-bot (HAT Geyser-Daten) sendet Quote-Daten:
intent.metadata.insert("sell_quote_amount_out", quote.amount_out.to_string());
intent.metadata.insert("sell_quote_min_out", min_out.to_string());
intent.metadata.insert("sell_pool", pool_address.clone());
intent.metadata.insert("sell_dex", dex_name.clone());

// execution-engine validiert nur noch Metadaten, KEIN RPC re-quote
```

**Priorität:** P1 (nach arb-strategy Path stabilisiert)

---

#### ~~C3: On-Demand Discovery aus DEX Connectors entfernen~~ ✅ ERLEDIGT
**Dateien:**
- ~~`src/solana/dex/meteora_dlmm.rs` - `discover_pool_on_demand()` entfernen~~ ✅
- `src/solana/dex/pumpfun_amm.rs` - RPC-Discovery auf Fallback-Only reduzieren (OFFEN)

DEX Connectors sollen Pool-Daten **nur** aus:
1. Geyser Events (via market-data → MarketEvents)
2. Intent Metadata (mitgeschickt von arb-strategy)

#### ~~C4: CrossDexHandler Redesign~~ ✅ ERLEDIGT
**Datei:** `src/solana/cross_dex_handler.rs`

CrossDexHandler:
- ~~**NICHT** eigene DEX Connector Instanzen haben~~ (noch nötig für IX Building)
- ~~**NICHT** RPC Calls für Pool Discovery machen~~ ✅
- ~~Pool-Daten aus Intent Metadata verwenden~~ ✅
- ~~Nur `build_swap_ix()` aufrufen (keine `quote_exact_in()`)~~ ✅

```rust
// VORHER (❌):
let buy_quote = buy_connector.quote_exact_in(SOL_MINT, token_mint, trade_amount).await?;

// NACHHER (✅):
let spread_bps = intent.metadata.get("spread_bps").and_then(|s| s.parse().ok());
let estimated_profit = intent.metadata.get("estimated_profit_lamports").and_then(|s| s.parse().ok());
// Validation based on intent metadata, no RPC re-quoting
```

---

### P1 - Wichtig (vor Skalierung)

#### C5: Zentrale Pool State Registry in market-data
**Neue Datei:** `src/bin/market_data.rs` (oder separates Modul)

- Pool State Cache (gefüllt durch Geyser)
- Optional: NATS Request/Reply für Pool State Queries
- execution-engine kann Pool State abfragen, wenn Intent nicht alle Daten hat

#### C6: Geyser Subscription für Vault Balances
**Datei:** `src/solana/geyser_pool_discovery.rs`

- Subscribe auf Vault Token Accounts (nicht nur Pool Accounts)
- Propagiere Reserve Updates via `MarketEvent::PoolStateUpdate`

---

### P2 - Nice to Have

#### C7: Pool State Caching in execution-engine
- Cache Pool State aus empfangenen MarketEvents
- TTL-basierte Invalidierung
- Fallback: Request an market-data via NATS

#### C8: DEX Connector als Pure Functions
- Keine State in DEX Connectors
- `quote_exact_in(pool_state, amount) -> Quote`
- `build_swap_ix(pool_state, params) -> Vec<Instruction>`

---

## Datenfluss (Soll-Zustand)

```
Geyser gRPC
    │
    ▼
market-data
    ├── GeyserPoolDiscovery (Pool + Vault Account Updates)
    ├── Pool State Cache (single source of truth)
    └── Publishes: MarketEvent::PoolCreated, MarketEvent::Trade, MarketEvent::PoolStateUpdate
          │
          ▼
       NATS
          │
    ┌─────┴─────┐
    ▼           ▼
arb-strategy   momentum-bot
    │
    │ Has full pool state from MarketEvents
    │
    ▼
TradeIntent (includes pool state in metadata)
    │
    ▼
execution-engine
    │
    ├── Uses pool state FROM INTENT (not RPC!)
    ├── build_swap_ix() with provided pool state
    ├── Simulate + Send + Confirm
    │
    ▼
ExecutionResult
```

---

## Migration Plan

### Phase 1: Intent Enrichment ✅ ERLEDIGT
1. ✅ arb-strategy fügt alle Pool-Daten zu Intent Metadata hinzu
2. ✅ execution-engine verwendet Metadata statt RPC Discovery für arb-intents
3. ⚠️ momentum-bot SELL-Path nutzt noch RPC (akzeptiert für MVP)

### Phase 2: MarketEvent Enrichment (TODO)
1. `PoolCreatedEvent` um alle Pool-State Felder erweitern
2. `PoolStateUpdate` Event für Reserve Changes
3. Strategies cachen Pool State aus MarketEvents

### Phase 3: CrossDexHandler Cleanup ✅ ERLEDIGT
1. ✅ RPC-abhängige Quote-Validation entfernt (validate_arb_opportunity)
2. ⚠️ DEX Connectors haben noch State (für IX Building nötig)
3. ✅ Pool State aus Intent für arb-strategy Path

---

## Metriken für Erfolg

- [x] Keine `getProgramAccounts` Calls im arb-strategy Hot Path
- [x] execution-engine macht 0 RPC Calls für arb-intent Pool Discovery
- [ ] Pool State Latenz: Geyser → execution-engine < 50ms (nicht messbar ohne deploy)
- [ ] Keine "rpc timeout" Decision Records für arb-intents
- [x] arb-strategy ist Source of Truth für Quote-Daten (Intent Metadata)

---

## Offene Punkte (P1/P2)

1. **momentum-bot SELL Path**: Nutzt noch RPC quote_exact_in()
2. **PoolCreatedEvent Enrichment**: Fehlende Felder (vault, reserves, fee)
3. **Pool State Registry**: Zentrale Registry in market-data für Request/Reply

---

## Referenzen

- `docs/TARGET_ARCHITECTURE.md` - Sektion 2.1, 4.1, 4.2, 4.5
- `docs/ROLE_SEPARATION.md` - Prozess-Zugriffsmatrix
- `docs/STORAGE_CONVENTIONS.md` - Hot Path Safe Pattern
