# Live Pool Cache Implementation (Option C)

**Ziel:** Eliminiere RPC-Calls aus dem TX-Bau-Pfad durch direkten Geyser-Cache in execution-engine.

**Erwartete Verbesserung:**
| Metrik | Aktuell | Nach Umbau |
|--------|---------|------------|
| RPC Calls pro TX | 3-5 | 0 |
| TX-Bau Latenz | 300-700ms | <50ms |
| Quote Age bei TX-Bau | 500-3000ms | <50ms |
| Intent TTL | 3000ms | 500ms |
| Custom(1) Rate | ~50% | <10% |

---

## Phase 1: LivePoolCache Modul (3-4h)

### 1.1 Neues Modul erstellen
- [x] `src/execution/live_pool_cache.rs` erstellen
- [x] Struct `LivePoolCache` mit DashMap für alle DEX-Typen
- [x] Enum `CachedPoolState` für DEX-spezifische Daten

### 1.2 DEX-spezifische State-Structs
- [x] `OrcaWhirlpoolState`: tick_current_index, sqrt_price, liquidity, vault_a, vault_b
- [x] `RaydiumAmmState`: reserve_base, reserve_quote, fees, serum_market_accounts
- [x] `RaydiumCpmmState`: reserve_0, reserve_1
- [x] `MeteoraState`: active_id, bin_step, bin_arrays (sparse), reserves
- [x] `PumpFunState`: virtual_sol_reserves, virtual_token_reserves, real_reserves
- [x] `PumpAmmState`: reserves, fee_config, pool_accounts

### 1.3 Geyser Subscription in execution-engine
- [x] `src/execution/cache_geyser.rs` erstellt
- [x] Geyser-Client initialisieren (wie in market-data)
- [x] Account-Filter für alle 6 DEX-Programme
- [x] Account-Update-Handler der Cache aktualisiert
- [x] Reconnect-Logic bei Geyser-Disconnect

### 1.4 Parser für Account-Daten (wiederverwenden aus DEX-Connectors)
- [x] `parse_orca_whirlpool(data: &[u8]) -> Option<OrcaWhirlpoolState>`
- [x] `parse_raydium_amm(data: &[u8]) -> Option<RaydiumAmmState>`
- [x] `parse_raydium_cpmm(data: &[u8]) -> Option<RaydiumCpmmState>`
- [x] `parse_meteora_dlmm(data: &[u8]) -> Option<MeteoraState>`
- [x] `parse_pumpfun_bonding(data: &[u8]) -> Option<PumpFunState>`
- [x] `parse_pumpamm_pool(data: &[u8]) -> Option<PumpAmmState>`

### 1.5 Cache-API
- [x] `fn get(&self, pool: &Pubkey) -> Option<CachedPoolState>`
- [x] `fn get_with_metadata(&self, pool: &Pubkey) -> Option<(CachedPoolState, u64, u64)>`
- [x] `fn upsert(&self, pool: Pubkey, state: CachedPoolState, slot: u64)`
- [x] `fn contains(&self, pool: &Pubkey) -> bool`

---

## Phase 2: TX-Builder nutzt Cache statt RPC (2-3h)

### 2.1 tx_builder.rs anpassen
- [ ] `build_tx_plan()` erhält `&LivePoolCache` Parameter
- [ ] Orca: Cache statt `rpc.get_account(&pool_id)`
- [ ] Orca: Cache statt `fetch_current_tick()`
- [ ] Raydium: Cache statt `load_pool_from_geyser()`
- [ ] Raydium: Serum-Accounts aus Cache (statisch, ändern sich nie)
- [ ] Meteora: Cache statt `fetch_current_active_id()`
- [ ] Meteora: Bin-Arrays aus Cache
- [ ] PumpFun: Cache statt `get_account(bonding_curve)`
- [ ] PumpAmm: Cache statt `get_multiple_accounts()`

### 2.2 cross_dex_handler.rs anpassen
- [ ] `build_swap_plan()` erhält `&LivePoolCache` Parameter
- [ ] Alle DEX-Pfade nutzen Cache

### 2.3 Fallback-Logik
- [ ] Wenn Pool nicht im Cache: RPC als Fallback (mit Warning-Log)
- [ ] Metric: `cache_miss_total{dex="..."}`

---

## Phase 3: Quote-Berechnung in execution-engine (2h)

### 3.1 Quote-Funktionen aus Cache
- [ ] `calculate_orca_quote(state: &OrcaWhirlpoolState, amount_in: u64, a_to_b: bool) -> u64`
- [ ] `calculate_raydium_quote(state: &RaydiumAmmState, amount_in: u64, base_to_quote: bool) -> u64`
- [ ] `calculate_meteora_quote(state: &MeteoraState, amount_in: u64, x_to_y: bool) -> u64`
- [ ] `calculate_pumpfun_quote(state: &PumpFunState, amount_in: u64, buy: bool) -> u64`
- [ ] `calculate_pumpamm_quote(state: &PumpAmmState, amount_in: u64, buy: bool) -> u64`

### 3.2 Fresh min_out Berechnung
- [ ] `fn calculate_fresh_min_out(cache: &LivePoolCache, intent: &TradeIntent) -> Result<u64>`
- [ ] Slippage anwenden: `quote * (10000 - slippage_bps) / 10000`

### 3.3 Integration in Intent-Processing
- [ ] Wenn `intent.execution.min_out` fehlt: frisch berechnen
- [ ] Log: "Recalculated min_out from cache: old={} new={}"

---

## Phase 4: Intent-Schema + arb-strategy anpassen (1h)

### 4.1 TradeIntent anpassen
- [ ] `execution.min_out` bleibt optional
- [ ] Doku: "Wenn None, berechnet execution-engine aus Live-Cache"

### 4.2 arb-strategy anpassen
- [ ] Kein `min_out` mehr in Intent setzen
- [ ] Nur `max_slippage_bps` setzen
- [ ] TTL reduzieren: `intent_ttl_ms = 1000` (kann später auf 500 gesenkt werden)

### 4.3 momentum-bot anpassen (optional)
- [ ] Gleiche Logik: `min_out` optional
- [ ] Oder: Momentum behält min_out (Quote ist relativ frisch)

---

## Phase 5: Testen + Deploy (1-2h)

### 5.1 Lokale Tests
- [ ] Unit-Tests für Parser
- [ ] Unit-Tests für Quote-Berechnung
- [ ] Integration-Test: Cache-Update → TX-Bau

### 5.2 Staging/Dry-Run
- [ ] Deploy mit `dry_run = true`
- [ ] Vergleiche Cache-Quotes vs RPC-Quotes
- [ ] Log Cache-Miss-Rate

### 5.3 Production
- [ ] Deploy mit `dry_run = false`
- [ ] Monitor Custom(1) Rate
- [ ] Monitor TX-Bau Latenz

---

## Abhängigkeiten

```
Phase 1 (Cache) ──► Phase 2 (TX-Builder) ──► Phase 3 (Quote) ──► Phase 4 (Intent) ──► Phase 5 (Deploy)
```

---

## Risiken + Mitigationen

| Risiko | Wahrscheinlichkeit | Mitigation |
|--------|-------------------|------------|
| Geyser-Disconnect | Mittel | Reconnect-Logic + RPC-Fallback |
| Cache stale (kein Update) | Niedrig | Slot-Age Check, Warning wenn >5s alt |
| Parser-Bug | Niedrig | Wiederverwenden bestehender Parser |
| Memory-Explosion | Niedrig | Cache-Eviction nach 10min inaktiv |

---

## Metrics (neu)

- `live_pool_cache_size{dex="..."}` - Anzahl Pools im Cache
- `live_pool_cache_update_latency_ms` - Zeit von Geyser-Event bis Cache-Update
- `live_pool_cache_miss_total{dex="..."}` - Cache-Misses (Fallback zu RPC)
- `live_pool_cache_age_ms{dex="..."}` - Alter des Cache-Eintrags bei Nutzung
- `tx_build_latency_ms` - Zeit für TX-Bau (vorher/nachher vergleichen)

---

## Commit-Plan

1. `feat(execution): add LivePoolCache module with Geyser subscription`
2. `refactor(tx_builder): use LivePoolCache instead of RPC calls`
3. `feat(execution): calculate fresh min_out from cache`
4. `refactor(arb-strategy): remove min_out from intents, reduce TTL`
5. `deploy: enable live pool cache in production`
