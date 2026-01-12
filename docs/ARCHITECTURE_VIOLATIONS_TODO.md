# Architecture Violations & Required Changes

**Erstellt:** 2026-01-11  
**Aktualisiert:** 2026-01-12 (TODO-7/8 komplett inkl. Consumer-Cache)  
**Status:** 🟢 TODO-1 bis TODO-8 komplett - Verbleibend: TODO-9 Momentum SELL (P3)  
**Source of Truth:** `docs/TARGET_ARCHITECTURE.md`, `docs/ROLE_SEPARATION.md`

---

## 📋 KONKRETE TODO-LISTE (Sortiert nach technischer Relevanz)

### Phase 1: Akute Bugs beheben (Blocking für Arb-Execution)

#### TODO-1: pump_amm DexPoolAccounts Missing ✅ FIXED
**Priorität:** 🔴 P0 (blockiert Live-Arb)  
**Reject-Grund:** R2 "pump_amm pool not discovered/cached for base_mint=..."  

**Gefundene Ursache:**
- `DexPoolAccounts` Events werden von `market-data` emittiert (828k+ Events/Tag)
- Aber arb-strategy erzeugt Intents BEVOR die Accounts gecached sind
- Nur ~7% der Arb-Intents hatten `sell_pool_accounts` wenn pump_amm verwendet wurde
- execution-engine rejected dann mit "pump_amm pool not discovered/cached"

**Fix implementiert in `src/bin/arb_strategy.rs`:**
```rust
// pump_amm requires DexPoolAccounts per TARGET_ARCHITECTURE.md
// Reject early if pump_amm is used but accounts are missing
if opp.buy_dex == "pump_amm" && buy_accounts.is_none() {
    warn!(..., "Rejecting arb: pump_amm buy pool missing DexPoolAccounts");
    ARB_REJECTED_MISSING_ACCOUNTS.fetch_add(1, Ordering::Relaxed);
    return None;
}
if opp.sell_dex == "pump_amm" && sell_accounts.is_none() {
    warn!(..., "Rejecting arb: pump_amm sell pool missing DexPoolAccounts");
    ARB_REJECTED_MISSING_ACCOUNTS.fetch_add(1, Ordering::Relaxed);
    return None;
}
```

**Neue Metrik:** `arb_rejected_missing_accounts_total` in Prometheus

---

#### TODO-2: pump_amm Pool Discovery mit DexPoolAccounts ✅ FIXED
**Priorität:** 🔴 P0 (Race Condition gefixt)

**Problem-Analyse:**
- pump_amm (pAMMBay...) hat keine Pool Creation Events - Pools sind nur via Trades sichtbar
- Die 12-14 benötigten Accounts (vaults, fee accounts, etc.) kommen aus Transaction Account Keys
- PoolCreated wurde VOR DexPoolAccounts emittiert → arb-strategy sah Pool bevor Accounts da waren

**Lösung implementiert in `src/bin/market_data.rs`:**
```rust
// Check if this is the FIRST trade for this pool (new pool discovery)
let is_first_trade = ctx.known_pump_amm_pools.write().insert(*pool_address);

// If first trade, emit PoolCreated FIRST (before DexPoolAccounts)
// This ensures arb-strategy sees PoolCreated + DexPoolAccounts together
if is_first_trade {
    info!(..., "pump_amm pool discovered via first trade - emitting PoolCreated + DexPoolAccounts");
    // Emit PoolCreated event
    ...
}
// Always emit DexPoolAccounts on pump_amm trades
...
```

**Neues Verhalten:**
1. pump_amm Pools werden NICHT bei Account Update emittiert (weil keine 12-14 Accounts verfügbar)
2. Beim ERSTEN Trade: `PoolCreated` + `DexPoolAccounts` werden ZUSAMMEN emittiert
3. Bei weiteren Trades: nur `DexPoolAccounts` (für Account-Updates)

**Warum das funktioniert:**
- Arbitrage: Braucht Preisdifferenzen → ohne Trades gibt's keinen Preis → kein Verlust
- Momentum: Reagiert auf Preisbewegungen → ohne Trades keine Bewegung → kein Verlust

---

### Phase 2: Geyser-Conformance für Cross-DEX Arb (Latenz-Optimierung)

#### TODO-3: `DexPoolAccounts` Events für andere DEXes emittieren ✅ FIXED
**Priorität:** 🟠 P1 (eliminiert RPC-Fallback-Latenz)  
**Verstöße:** D1, M1  
**Datei:** `src/bin/market_data.rs`

**Lösung implementiert:**
Bei Pool Discovery via `geyser_pool_discovery.rs` werden jetzt `DexPoolAccounts` Events 
für ALLE DEXes emittiert, die `coin_vault` und `pc_vault` im `PoolDiscoveryEvent` haben.

```rust
// Nach PoolCreated Event, wenn Vault-Informationen vorhanden:
if pool_event.coin_vault.is_some() || pool_event.pc_vault.is_some() {
    let accounts = vec![
        pool_event.pool_address,
        pool_event.base_mint,
        pool_event.quote_mint,
        pool_event.coin_vault,   // wenn vorhanden
        pool_event.pc_vault,     // wenn vorhanden
        pool_event.creator,      // wenn vorhanden (PumpFun)
    ];
    // Emit DexPoolAccounts event...
}
```

**Unterstützte DEXes:**
- ✅ Raydium AMM V4 (coin_vault, pc_vault aus 752-byte Account)
- ✅ Raydium CPMM (token_0_vault, token_1_vault aus 1024-byte Account)
- ✅ Orca Whirlpool (token_vault_a, token_vault_b aus parsed Account)
- ✅ Meteora DLMM (reserve_x, reserve_y aus 904-byte LB Pair)
- ✅ PumpFun (bonding curve - hat creator)
- ✅ pump_amm (via first trade, siehe TODO-2)

---

#### TODO-4: `set_pool_from_accounts()` für alle DEXes implementieren ✅ FIXED
**Priorität:** 🟠 P1 (ermöglicht RPC-freies Pool-Loading)  
**Verstöße:** D2, M2  
**Dateien:**
- `src/solana/dex/meteora_dlmm.rs` ✅
- `src/solana/dex/raydium_cpmm.rs` ✅
- `src/solana/dex/raydium.rs` ✅
- `src/solana/dex/orca.rs` ✅

**Implementierung:**
Alle DEX Connectors haben jetzt `set_pool_from_accounts()` implementiert:

```rust
// Gemeinsames Format für alle DEXes:
fn set_pool_from_accounts(&self, pool_address: &str, accounts: &[String]) -> Result<()> {
    // accounts[0] = pool_address
    // accounts[1] = base_mint
    // accounts[2] = quote_mint
    // accounts[3] = coin_vault (optional)
    // accounts[4] = pc_vault (optional)
    // ...zusätzliche DEX-spezifische Felder
}
```

**Hinweise:**
- Meteora DLMM: bin_step/active_id nicht in DexPoolAccounts, verwendet Defaults
- Raydium AMM V4: Serum-Accounts nicht in DexPoolAccounts, IX-Building braucht ggf. RPC
- Orca: tick_spacing/tick_current_index nicht in DexPoolAccounts
- pump_amm: vollständig (12-14 Accounts aus Trade-TX)

**Erwartetes Ergebnis:** `cross_dex_handler.rs` kann `set_pool_from_accounts()` für alle DEXes aufrufen ✅

---

#### TODO-5: arb-strategy sendet Pool-Accounts im Intent ✅ ALREADY IMPLEMENTED
**Priorität:** 🟠 P1 (vervollständigt Geyser-Pipeline)  
**Datei:** `src/bin/arb_strategy.rs`

**Bereits implementiert in `create_arb_intent()`:**

1. ✅ `DexPoolAccounts` Events werden in `pool_accounts` HashMap gecached
   - `handle_dex_pool_accounts()` speichert Accounts per Pool
   - `get_pool_accounts_for_arb()` holt Accounts für Buy/Sell-Pools

2. ✅ Intent.resources.accounts wird befüllt:
   ```rust
   // Format: "buy_pool_accounts_start:N" + N accounts + "sell_pool_accounts_start:M" + M accounts
   if let Some(buy_accts) = &buy_accounts {
       all_accounts.push(format!("buy_pool_accounts_start:{}", buy_accts.len()));
       all_accounts.extend(buy_accts.iter().cloned());
   }
   ```

3. ✅ `cross_dex_handler.rs` verarbeitet Intent-Accounts:
   - `parse_pool_accounts_from_intent()` extrahiert buy/sell Accounts
   - `set_pool_from_accounts()` wird für beide Pools aufgerufen

**Erwartetes Ergebnis:** execution-engine macht 0 RPC Calls für Pool-Loading bei Arb-Intents mit Accounts ✅

---

### Phase 3: RPC-Elimination im Hot Path (Performance)

#### TODO-6: `refresh_pools()` mit Feature-Flag schützen ✅ FIXED
**Priorität:** 🟡 P2 (verhindert versehentliche RPC-Scans)  
**Verstöße:** D3, M3  
**Dateien:** alle DEX Connectors

**Implementierung:**
1. ✅ Feature-Flag `rpc_fallback` in `Cargo.toml` hinzugefügt
2. ✅ `refresh_pools()` in allen DEX Connectors geschützt mit:
   ```rust
   async fn refresh_pools(&self) -> Result<()> {
       #[cfg(not(feature = "rpc_fallback"))]
       {
           debug!("refresh_pools() disabled - rpc_fallback feature not enabled");
           return Ok(());
       }
       
       #[cfg(feature = "rpc_fallback")]
       { /* ... actual RPC scan code ... */ }
   }
   ```
3. ✅ RPC-spezifische Imports und Konstanten ebenfalls feature-gated
4. ✅ In Production: Ohne `--features rpc_fallback` returnen alle `refresh_pools()` sofort `Ok(())`

**Geschützte DEXes:**
- ✅ `meteora_dlmm.rs`
- ✅ `raydium_cpmm.rs`
- ✅ `raydium.rs`
- ✅ `orca.rs`
- ✅ `pumpfun_amm.rs` (war bereits no-op)

**Nutzung:**
```bash
# Production (default): Kein RPC-Scan
cargo build --release

# Bootstrap/Testing: Mit RPC-Fallback
cargo build --release --features rpc_fallback
```

**Erwartetes Ergebnis:** Keine versehentlichen getProgramAccounts im Production Hot Path ✅

---

#### TODO-7: Vault Balances via Geyser Account Subscription ✅ FIXED
**Priorität:** 🟡 P2 (eliminiert RPC Calls für Vault Balances)  
**Verstöße:** D4, M4  

**Analyse (warum Geyser statt Intent-Metadata):**
Die Target Architecture definiert klar: "Pool State Updates (Reserves, Liquidity)" via MarketEvents,
"Store pool state received from MarketEvents (not RPC!)". Die Intent-Metadata Lösung würde den
Data Plane umgehen und zu veralteten Daten führen (Intent-Zeit ≠ Execution-Zeit).

**Implementierte Lösung (Option B - Geyser Account Subscription):**
1. [x] `MarketEventKind::PoolStateUpdate` in `src/ipc/schema.rs` hinzugefügt
   - Enthält `reserve_base`, `reserve_quote`, `pool_address`, `dex`, `base_mint`, `quote_mint`, `update_slot`
2. [x] `VaultInfo` Tracking-Struktur in `market-data.rs`
   - Maps vault_address → (pool_address, dex, base_mint, quote_mint, is_base_vault)
3. [x] Vault Account Registration bei PoolDiscovery
   - Vaults werden automatisch zu `tracked_vaults` hinzugefügt
   - GeyserListener resubscribed automatisch bei neuen Vaults
4. [x] `PoolStateUpdate` Event bei Vault Balance Changes
   - Emittiert via NATS wenn Geyser Account Update für tracked Vault kommt
   - Consumers können lokalen Pool-Cache aktualisieren

**Dateien geändert:**
- `src/ipc/schema.rs` - PoolStateUpdate Event-Typ
- `src/bin/market_data.rs` - VaultInfo, tracked_vaults, Geyser Integration

**Vorteile gegenüber Intent-Metadata:**
- Daten-Frische: <10ms via Geyser vs. potentiell Slots-alte Intent-Daten
- Target Architecture konform: Data Plane lädt einmal
- Einheitlicher Datenfluss: Alle Consumer bekommen dieselben Updates
- Debugging: Event-Stream nachvollziehbar

**Implementiert:**
- [x] `PoolStateUpdate` Event in schema.rs ✅
- [x] Vault Tracking + Geyser Subscription in market-data ✅
- [x] Consumer-Cache in arb-strategy (`vault_balances: HashMap`) ✅
- [x] Event Handler `handle_pool_state_update()` ✅

**Erwartetes Ergebnis:** Vault Balances via Geyser statt RPC ✅

---

#### TODO-8: Meteora Bin Arrays via Geyser Account Subscription ✅ FIXED
**Priorität:** 🟡 P2 (eliminiert bis zu 8 RPC Calls pro Quote)  
**Verstöße:** D5, M5  
**Dateien:**
- `src/ipc/schema.rs` - `MarketEventKind::BinArrayUpdate`, `BinData`
- `src/bin/market_data.rs` - Bin Array Tracking + Geyser Subscription + Event Emission

**Architektur-Konformität (wie TODO-7):**
Die Target Architecture definiert: "Pool State Updates (Reserves, Liquidity)" via MarketEvents.
Bin Arrays enthalten Liquidity-Verteilung und müssen via Data Plane (market-data) geladen werden,
nicht per Lazy-PDA im Consumer. Analog zu Vault Balances.

**Implementierung (Full Geyser - Option A):**

1. **MarketEventKind::BinArrayUpdate** in `src/ipc/schema.rs`:
   ```rust
   BinArrayUpdate {
       pool_address: String,
       bin_array_index: i64,
       /// Serialized bin data (compact format)
       bins: Vec<BinData>,
       update_slot: u64,
   }
   ```

2. **BinArrayInfo Tracking** in `market-data.rs`:
   ```rust
   struct BinArrayInfo {
       pool_address: Pubkey,
       bin_array_index: i64,
       // PDA derived once, then tracked
   }
   tracked_bin_arrays: HashMap<Pubkey, BinArrayInfo>
   ```

3. **Registration bei PoolDiscovery:**
   - Bei Meteora DLMM PoolCreated: active_id aus Pool Account lesen
   - Bin Array PDAs für ±3 Arrays um active_id ableiten
   - Zu `tracked_bin_arrays` hinzufügen
   - GeyserListener resubscribed

4. **BinArrayUpdate bei Geyser Account Update:**
   - Wenn tracked Bin Array Account sich ändert
   - Parse Bin Data, emit `BinArrayUpdate` via NATS

5. **Consumer Cache:**
   - `arb-strategy` / `execution-engine`: Lokaler Bin Array Cache
   - Update bei `BinArrayUpdate` Events
   - `meteora_swap_builder.rs`: Cached Bins statt `fetch_bin_arrays()` RPC

**Komplexität vs. Vaults:**
| Aspekt | Vaults (TODO-7) | Bin Arrays (TODO-8) |
|--------|----------------|---------------------|
| Accounts/Pool | 2 (statisch) | 3-7 (dynamisch um active_id) |
| Update-Frequenz | Bei jedem Swap | Bei jedem Swap |
| Parse-Komplexität | 8 bytes (u64) | ~5KB pro Array |
| PDA-Derivation | Aus Pool Account | Aus active_id + Index |

**Schritte:**
1. [x] `MarketEventKind::BinArrayUpdate` in schema.rs ✅
2. [x] `BinArrayInfo` + `tracked_bin_arrays` in market-data.rs ✅
3. [x] Bei Meteora PoolDiscovery: Bin Array PDAs registrieren ✅
4. [x] Geyser Account Update → BinArrayUpdate Event emittieren ✅
5. [x] Consumer: Bin Array Cache + Update-Handler ✅ (`bin_arrays: HashMap` in arb-strategy)
6. [x] Event Handler `handle_bin_array_update()` ✅

**Erwartetes Ergebnis:** Bin Arrays via Geyser statt RPC, architektonisch konsistent mit TODO-7 ✅

---

### Phase 4: Momentum-Bot Geyser-Conformance (Separate Pipeline)

#### TODO-9: Momentum-Bot SELL Path auf Intent-Metadata umstellen ⬜ TODO
**Priorität:** 🟢 P3 (separater Code-Path, nicht arb-blocking)  
**Verstöße:** V6  
**Dateien:**
- `src/bin/momentum_bot.rs`
- `src/solana/execution.rs`

**Schritte:**
1. [ ] momentum-bot: Quote-Daten im SELL-Intent mitschicken
   ```rust
   intent.metadata.insert("sell_quote_amount_out", quote.amount_out.to_string());
   intent.metadata.insert("sell_pool", pool_address.clone());
   intent.metadata.insert("sell_dex", dex_name.clone());
   ```
2. [ ] execution.rs: Intent-Metadata statt RPC-Quote verwenden
3. [ ] Fallback für alte Intents ohne Metadata

**Erwartetes Ergebnis:** momentum-bot SELL-Intents brauchen keine RPC-Quotes in execution-engine

---

## 📊 Fortschritts-Tracking

| TODO | Status | Blocker | ETA |
|------|--------|---------|-----|
| TODO-1 | ✅ | - | Done |
| TODO-2 | ✅ | - | Done |
| TODO-3 | ✅ | - | Done |
| TODO-4 | ✅ | - | Done |
| TODO-5 | ✅ | - | Done |
| TODO-6 | ✅ | - | Done |
| TODO-7 | ✅ | - | Done |
| TODO-8 | ✅ | - | Done |
| TODO-9 | ⬜ | - | 2h |

**Kritischer Pfad:** TODO-1 → TODO-8 ✅ komplett (Geyser-Pipeline funktional inkl. Consumer-Cache)

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

## 🔴 DEX Connectors: TARGET_ARCHITECTURE Verstöße (Audit 2026-01-11)

`DexPoolAccounts` Events werden **NUR für `pump_amm`** emittiert. Alle anderen DEXes verletzen das Geyser-First-Prinzip.

### Übersicht: DEX Geyser-Conformance

| DEX | DexPoolAccounts Events | set_pool_from_accounts() | refresh_pools() RPC | Vault RPC | Status |
|-----|------------------------|--------------------------|---------------------|-----------|--------|
| pump_amm | ✅ | ✅ | N/A | N/A | **Konform** |
| pumpfun | ❌ (bonding curve) | ❌ | N/A | N/A | Teilweise |
| meteora_dlmm | ❌ | ❌ | getProgramAccounts | get_account×2 | **Verletzt** |
| raydium_cpmm | ❌ | ❌ | getProgramAccounts | get_account×2 | **Verletzt** |
| raydium_amm_v4 | ❌ | ❌ | getProgramAccounts | get_account×2 | **Verletzt** |
| orca | ❌ | ❌ | getProgramAccounts | get_account×2 | **Verletzt** |

### ❌ D1: Keine `DexPoolAccounts` Events für Nicht-PumpAMM DEXes
- **Datei:** `src/bin/market_data.rs` (Lines 737-768)
- **Problem:** `DexPoolAccounts` Events werden **NUR für `pump_amm`** emittiert
- **Betroffene DEXes:** meteora_dlmm, raydium_cpmm, raydium_amm_v4, orca
- **Auswirkung:** arb-strategy/execution-engine bekommen keine Pool-Accounts
- **Workaround aktuell:** RPC Fallback via `load_pool_by_address()` in cross_dex_handler.rs
- **Spezifikation verletzt:** TARGET_ARCHITECTURE.md Section 4.1

### ❌ D2: `set_pool_from_accounts()` fehlt in allen DEXes außer pump_amm
- **Betroffene Dateien:**
  - `src/solana/dex/meteora_dlmm.rs` - ❌ Nicht implementiert
  - `src/solana/dex/raydium_cpmm.rs` - ❌ Nicht implementiert
  - `src/solana/dex/raydium.rs` - ❌ Nicht implementiert (AMM V4)
  - `src/solana/dex/orca.rs` - ❌ Nicht implementiert
- **Problem:** Nur `pumpfun_amm.rs` implementiert `set_pool_from_accounts()`
- **Spezifikation verletzt:** TARGET_ARCHITECTURE.md Section 4.2

### ❌ D3: `refresh_pools()` macht `getProgramAccounts` RPC Calls
- **Betroffene Dateien:**
  - `src/solana/dex/meteora_dlmm.rs` (Lines 214-234)
  - `src/solana/dex/raydium_cpmm.rs` (Lines 304-323)
  - `src/solana/dex/orca.rs` (Line 576)
- **Problem:** Teure RPC-Scans statt Geyser Account Updates
- **Spezifikation verletzt:** TARGET_ARCHITECTURE.md Section 4.2 "refresh_pools() exists ONLY as fallback"

### ❌ D4: Vault Balances via RPC statt Geyser/Intent
- **Betroffene Dateien:**
  - `src/solana/dex/meteora_dlmm.rs` (Lines 105-106) - `update_reserve_balances()`
  - `src/solana/dex/raydium_cpmm.rs` (Lines 198-199) - `update_reserve_balances()`
  - `src/solana/dex/orca.rs` - ähnlich
- **Problem:** 2x `get_account_retry()` pro Quote im Hot Path
- **Spezifikation verletzt:** TARGET_ARCHITECTURE.md Section 4.5

### ❌ D5: Meteora Bin Arrays via RPC
- **Datei:** `src/solana/dex/meteora_swap_builder.rs` (Lines 274, 307)
- **Problem:** Bis zu 7 `get_account_retry()` + 1 `getProgramAccounts` für Bin Arrays
- **Spezifikation verletzt:** TARGET_ARCHITECTURE.md Section 4.2

---

## 🔴 Meteora DLMM: Detaillierte Verstöße (Audit 2026-01-11)

Meteora DLMM Implementation ist **unvollständig** und verletzt mehrere architektonische Prinzipien:

### ❌ M1: Keine `DexPoolAccounts` Events für Meteora DLMM
- **Datei:** `src/bin/market_data.rs` (Lines 737-768)
- **Problem:** `DexPoolAccounts` Events werden **NUR für `pump_amm`** emittiert
- **Auswirkung:** arb-strategy/execution-engine bekommen keine Pool-Accounts für Meteora
- **Workaround aktuell:** RPC Fallback via `load_pool_by_address()` in cross_dex_handler.rs
- **Spezifikation verletzt:** TARGET_ARCHITECTURE.md Section 4.1 "GeyserPoolDiscovery handles ALL pool discovery"

### ❌ M2: `set_pool_from_accounts()` fehlt in `meteora_dlmm.rs`
- **Datei:** `src/solana/dex/meteora_dlmm.rs`
- **Problem:** Nur `pumpfun_amm.rs` implementiert `set_pool_from_accounts()`
- **Auswirkung:** Intent-Accounts aus `DexPoolAccounts` Events können nicht direkt genutzt werden
- **Folge:** Erzwingt RPC Calls via `load_pool_by_address()` im Hot Path
- **Spezifikation verletzt:** TARGET_ARCHITECTURE.md Section 4.2 "DEX Connectors: Store pool state received from MarketEvents"

### ❌ M3: `refresh_pools()` macht `getProgramAccounts` RPC Call
- **Datei:** `src/solana/dex/meteora_dlmm.rs` (Lines 214-234)
- **Problem:** Scannt alle 904-byte DLMM Pools via RPC (expensive!)
- **Kommentar im Code:** "⚠️ RPC FALLBACK ONLY" aber **kein Feature-Flag oder Guard**
- **Auswirkung:** Kann im Hot Path aufgerufen werden und 400-800ms Latenz erzeugen
- **Spezifikation verletzt:** TARGET_ARCHITECTURE.md Section 4.2 "refresh_pools() exists ONLY as fallback"

### ❌ M4: `update_reserve_balances()` macht RPC Calls für Vault Balances
- **Datei:** `src/solana/dex/meteora_dlmm.rs` (Lines 93-126)
- **Problem:** Fetcht Reserve Balances via `rpc.get_account_retry()` für beide Vaults
- **Auswirkung:** 2x RPC Calls pro Quote/IX Building im Hot Path
- **Soll-Zustand:** Vault Balances sollen aus Geyser Account Updates oder Intent-Metadata kommen
- **Spezifikation verletzt:** TARGET_ARCHITECTURE.md Section 4.5 "Never use RPC for: Real-time pool updates"

### ❌ M5: `meteora_swap_builder.rs` macht `getProgramAccounts` für Bin Arrays
- **Datei:** `src/solana/dex/meteora_swap_builder.rs` (Lines 283-318)
- **Problem:** `fetch_bin_arrays()` macht:
  1. 7x `get_account_retry()` für PDA-basierte Bin Arrays
  2. Falls <2 Arrays: `getProgramAccounts` mit memcmp Filter (Line 307)
- **Auswirkung:** Bis zu 7 RPC Calls + 1 getProgramAccounts im IX-Building Hot Path
- **Soll-Zustand:** Bin Arrays sollten mit Pool im Intent kommen (Geyser Account Updates)
- **Spezifikation verletzt:** TARGET_ARCHITECTURE.md Section 4.2 "DEX Connectors must NOT do getProgramAccounts"

### ❌ M6: RPC Fallback in `cross_dex_handler.rs` für Meteora/Orca
- **Datei:** `src/solana/cross_dex_handler.rs` (Lines 497-527)
- **Problem:** Wenn Intent keine Accounts hat, wird `load_pool_by_address()` aufgerufen
- **Code:** "Single getAccount RPC fallback for Meteora/Orca (acceptable)"
- **Bewertung:** **Nicht akzeptabel** nach TARGET_ARCHITECTURE.md - Data Plane soll Daten liefern
- **Workaround:** Weil M1 nicht gelöst ist (keine DexPoolAccounts für Meteora)

### Zusammenfassung Meteora DLMM

| Check | Status | Problem |
|-------|--------|---------|
| DexPoolAccounts Events | ❌ | Nur pump_amm emittiert Events |
| set_pool_from_accounts() | ❌ | Nicht implementiert |
| refresh_pools() Guard | ❌ | Kein Feature-Flag, RPC im Hot Path möglich |
| Vault Balance via Geyser | ❌ | RPC Calls in update_reserve_balances() |
| Bin Arrays via Geyser | ❌ | getProgramAccounts in swap_builder |
| Intent-basiertes Pool Loading | ⚠️ | Fallback via RPC load_pool_by_address() |

**Erforderliche Änderungen (Priorität P0 für Geyser-Conformance):**
1. **M1 Fix:** `market_data.rs` erweitern: `DexPoolAccounts` auch für Meteora DLMM emittieren
2. **M2 Fix:** `set_pool_from_accounts()` in `meteora_dlmm.rs` implementieren
3. **M4 Fix:** Vault Balances aus Intent-Metadata oder Geyser Account Updates
4. **M5 Fix:** Bin Arrays in `DexPoolAccounts` Event inkludieren ODER lazy PDA-Derivation ohne RPC

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

---

### P0 - Meteora DLMM Geyser-Conformance (NEU)

#### C6: DexPoolAccounts Events für Meteora DLMM emittieren ❌ OFFEN
**Datei:** `src/bin/market_data.rs`

**Problem:** `DexPoolAccounts` werden nur für `pump_amm` emittiert (Lines 737-768).

**Erforderliche Änderung:**
```rust
// Nach dem Parsen von Meteora DLMM Account Updates:
if let DexType::MeteoraDlmm = dex_type {
    let accounts_event = MarketEvent::new(
        "market-data",
        BUILD_VERSION,
        run_id,
        ctx.next_event_id(),
        "geyser",
        Some(slot),
        MarketEventKind::DexPoolAccounts {
            dex: "meteora_dlmm".to_string(),
            pool_address: pool_address.to_string(),
            base_mint: token_x_mint.to_string(),
            quote_mint: token_y_mint.to_string(),
            accounts: vec![
                pool_address.to_string(),
                reserve_x.to_string(),     // Vault X
                reserve_y.to_string(),     // Vault Y
                token_x_mint.to_string(),
                token_y_mint.to_string(),
                oracle.map(|o| o.to_string()).unwrap_or_default(),
                // ... weitere relevante Accounts
            ],
        },
    );
    // Publish to NATS...
}
```

#### C7: `set_pool_from_accounts()` für Meteora DLMM implementieren ❌ OFFEN
**Datei:** `src/solana/dex/meteora_dlmm.rs`

**Erforderliche Änderung:**
```rust
impl Dex for MeteoraDlmm {
    fn set_pool_from_accounts(&self, pool_address: &str, accounts: &[String]) -> Result<()> {
        // Parse accounts from DexPoolAccounts event:
        // [0] = pool_address
        // [1] = reserve_x (vault)
        // [2] = reserve_y (vault)
        // [3] = token_x_mint
        // [4] = token_y_mint
        // ...
        
        let pool_pk = Pubkey::from_str(pool_address)?;
        let vault_x = Pubkey::from_str(accounts.get(1).ok_or(anyhow!("missing vault_x"))?)?;
        let vault_y = Pubkey::from_str(accounts.get(2).ok_or(anyhow!("missing vault_y"))?)?;
        let token_x = Pubkey::from_str(accounts.get(3).ok_or(anyhow!("missing token_x"))?)?;
        let token_y = Pubkey::from_str(accounts.get(4).ok_or(anyhow!("missing token_y"))?)?;
        
        // Insert into pool cache (NO RPC!)
        let pool = DlmmPool {
            token_x_mint: token_x,
            token_y_mint: token_y,
            reserve_x: vault_x,
            reserve_y: vault_y,
            // ... weitere Felder aus accounts parsen
        };
        
        self.pools.insert(pool_pk, PoolCache {
            address: pool_pk,
            pool,
            reserve_x_balance: None, // Will be filled from intent metadata
            reserve_y_balance: None,
            last_updated: std::time::SystemTime::now(),
        });
        
        Ok(())
    }
}
```

#### C8: Bin Arrays aus Geyser/Intent statt RPC ❌ OFFEN (P1)
**Datei:** `src/solana/dex/meteora_swap_builder.rs`

**Problem:** `fetch_bin_arrays()` macht bis zu 7 RPC Calls + 1 getProgramAccounts.

**Optionen:**
1. **Option A:** Bin Array PDAs in `DexPoolAccounts` inkludieren (präferiert)
2. **Option B:** Lazy PDA-Derivation ohne RPC, Bin Data aus Intent-Metadata

**Priorität:** P1 (nach C6/C7 gelöst)

---

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
3. ⚠️ momentum-bot SELL-Path nutzt noch RPC 

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
- [ ] **NEU:** Meteora DLMM: `DexPoolAccounts` Events werden emittiert
- [ ] **NEU:** Meteora DLMM: `set_pool_from_accounts()` implementiert
- [ ] **NEU:** Meteora DLMM: Keine RPC Calls in `build_swap_ix()` Hot Path

---

## Offene Punkte (P1/P2)

1. **momentum-bot SELL Path**: Nutzt noch RPC quote_exact_in()
2. **PoolCreatedEvent Enrichment**: Fehlende Felder (vault, reserves, fee)
3. **Pool State Registry**: Zentrale Registry in market-data für Request/Reply
4. **NEU - DEX Geyser-Conformance P0:**
   - D1: `DexPoolAccounts` Events für alle DEXes emittieren (market_data.rs)
   - D2: `set_pool_from_accounts()` für alle DEXes implementieren
5. **NEU - DEX Geyser-Conformance P1:**
   - D3: `refresh_pools()` Guards (nur Fallback/Bootstrap, nicht Hot Path)
   - D4: Vault Balances aus Geyser/Intent statt RPC
   - D5: Meteora Bin Arrays ohne getProgramAccounts

---

## Referenzen

- `docs/TARGET_ARCHITECTURE.md` - Sektion 2.1, 4.1, 4.2, 4.5
- `docs/ROLE_SEPARATION.md` - Prozess-Zugriffsmatrix
- `docs/STORAGE_CONVENTIONS.md` - Hot Path Safe Pattern
