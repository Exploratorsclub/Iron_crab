# A.1 PumpSwap Geyser-First — Testplan

**Zweck:** Tests definieren, mit denen der A.1 Fix verifiziert werden kann, bevor die Implementierung startet.

**Stand:** 2026-02-23

---

## 1. Analyse: Vorhandene Tests

### 1.1 Bestehende Abdeckung

| Modul | Relevante Tests | Lücke für A.1 |
|-------|-----------------|----------------|
| `live_pool_cache.rs` | `test_cache_multiple_dex_types` (PumpAmm upsert), `test_vault_to_pool_registered_via_upsert` | Keine Tests für `get_pump_amm_reserves_by_base_mint`, `get_pump_amm_pool_accounts_by_base_mint`, `mark_pumpfun_complete_for_mint`, `set_pump_amm_pool_accounts` |
| `quote_calculator.rs` | `test_pump_amm_quote_buy`, `test_pump_amm_quote_sell` | Nur Formel mit `PumpAmmState`; keine DEX-Integration, kein LivePoolCache |
| `pumpfun_amm.rs` | **Keine** | Vollständig ohne Unit-Tests |
| `execution_pumpfun_builder.rs` | PumpFun **Bonding Curve** (PumpFunDex) | Testet PumpSwap AMM nicht |
| `pumpfun_live_token.rs` / `pumpfun_real_tokens.rs` | PumpFun Bonding Curve, RPC-Integration | Kein PumpFunAmmDex, kein Cache-First |
| `dex_connector_contracts.rs` | In DoD referenziert | **Datei existiert nicht** (evtl. Revert/Umbenennung) |

### 1.2 Fazit

**Es gibt aktuell keine Tests, die den A.1 Fix verifizieren.** Die vorhandenen Tests decken weder PumpFunAmmDex noch die Geyser-First-Logik (Cache statt RPC) ab.

---

## 2. Benötigte Tests (Priorisiert)

### Phase 1: Unit-Tests (ohne RPC, ohne Netzwerk)

#### 1.1 LivePoolCache — PumpAmm-spezifische API

**Datei:** `src/execution/live_pool_cache.rs` (neuer `#[cfg(test)]` Block oder Erweiterung)

| Test | Beschreibung | Erwartung |
|------|--------------|-----------|
| `test_get_pump_amm_reserves_by_base_mint_hit` | Cache mit PumpAmm-Eintrag (base_reserve, quote_reserve gesetzt) → `get_pump_amm_reserves_by_base_mint(base_mint)` | `Some((base_r, quote_r, pool_market))` |
| `test_get_pump_amm_reserves_by_base_mint_miss` | Cache leer oder base_mint unbekannt | `None` |
| `test_get_pump_amm_reserves_by_base_mint_missing_reserves` | PumpAmm-Eintrag vorhanden, aber base_reserve/quote_reserve = None | `None` |
| `test_get_pump_amm_pool_accounts_by_base_mint_hit` | Cache mit PumpAmm + 14 pool_accounts | `Some(accounts)` |
| `test_get_pump_amm_pool_accounts_by_base_mint_empty` | pool_accounts leer | `None` |
| `test_set_pump_amm_pool_accounts` | `set_pump_amm_pool_accounts` für existierenden Pool | Pool hat pool_accounts; `get_pump_amm_pool_accounts_by_base_mint` liefert sie |
| `test_mark_pumpfun_complete_for_mint` | PumpFun-Eintrag mit token_mint, `complete=false` → `mark_pumpfun_complete_for_mint` | `true`; Eintrag hat `complete=true` |
| `test_mark_pumpfun_complete_wrong_mint` | Mint nicht im Cache | `false` |

#### 1.2 PumpFunAmmDex — Cache-First (Mock RPC)

**Datei:** `src/solana/dex/pumpfun_amm.rs` (neuer `#[cfg(test)]` mod)

**Herausforderung:** PumpFunAmmDex nutzt `Arc<SolanaRpc>`; für Unit-Tests brauchen wir entweder:
- **Option A:** Mock/Stub-RPC (z.B. `MockSolanaRpc` oder `SolanaRpc` mit `None`/Dummy-URL, Tests schlagen bei echten RPC-Calls fehl)
- **Option B:** Nur die Pfade testen, die **ohne** RPC laufen (Cache-Hit-Fälle)

**Empfehlung:** Option B — Tests prüfen explizit: **Mit LivePoolCache und Cache-Hit → kein RPC-Call**.

| Test | Beschreibung | Erwartung |
|------|--------------|-----------|
| `test_quote_exact_in_cache_hit_no_rpc` | `PumpFunAmmDex::new_with_cache(rpc, cache)` mit vorab gefülltem Cache (base_mint, reserves) | `quote_exact_in(base, quote, amount)` → `Some(Quote)`; **kein RPC** (mit RPC-Mock würde kein Call erfolgen) |
| `test_quote_exact_in_cache_miss_returns_none` | Cache ohne Eintrag für base_mint | `quote_exact_in` → `None` (Hot Path: kein RPC-Fallback) |
| `test_pool_accounts_v1_for_base_mint_cache_hit` | Cache mit pool_accounts für base_mint | `pool_accounts_v1_for_base_mint` → `Some(14 accounts)` |
| `test_pool_accounts_v1_for_base_mint_cache_miss` | Cache ohne Eintrag | `None` |
| `test_build_swap_ix_from_pool_accounts` | Statischer Aufruf mit 14 gültigen Pubkeys | `Vec<Instruction>` mit mindestens 1 Instruction, Program-ID = PumpSwap AMM |

**Hinweis:** `quote_exact_in` und `pool_accounts_v1_for_base_mint` sind `async`. Tests können mit `#[tokio::test]` oder `tokio::runtime::Runtime::new().block_on()` laufen. RPC wird nie aufgerufen, wenn Cache gesetzt ist und Hit liefert — oder wir nutzen einen Dummy-RPC, der bei Aufruf `panic!()` macht (sicherstellen, dass der Pfad nie erreicht wird).

#### 1.3 CrossDexHandler / execution-engine — new_with_cache

| Test | Beschreibung | Erwartung |
|------|--------------|-----------|
| `test_cross_dex_handler_pump_amm_uses_cache` | CrossDexHandler mit `pool_cache: Some(cache)` → PumpAmm-DEX | `PumpFunAmmDex` hat `live_pool_cache.is_some()` (via Reflektion oder indirekt: Quote mit Cache funktioniert) |
| (Optional) `test_liquidation_pump_amm_uses_cache` | `run_liquidation_job` mit `live_pool_cache` | PumpAmm wird mit `new_with_cache` erstellt |

*Implementierungshinweis:* CrossDexHandler und execution-engine kapseln die DEX-Instanzen. Ein expliziter Test könnte prüfen: Wenn ein Intent mit pool_accounts aus dem Cache verarbeitet wird, erfolgt kein RPC-Call (z.B. über Metrics oder Log-Spy). Alternativ: Assertion in Unit-Test, dass `pool_cache` an `PumpFunAmmDex::new_with_cache` übergeben wird (Code-Review + manueller Test).

---

### Phase 2: Integrationstests (mit Mock/Stub)

#### 2.1 PumpFunAmmDex + LivePoolCache Roundtrip

**Datei:** `tests/pump_amm_geyser_first_test.rs` (neu)

| Test | Beschreibung | Erwartung |
|------|--------------|-----------|
| `test_quote_from_cache_no_rpc` | 1. LivePoolCache mit PumpAmm (base_mint, reserves, pool_accounts) füllen. 2. PumpFunAmmDex::new_with_cache. 3. quote_exact_in aufrufen. 4. RPC-Mock: kein get_multiple_accounts, get_account etc. | `Some(Quote)` mit plausiblen amount_out, price_impact_bps |
| `test_pool_accounts_from_cache_no_rpc` | Wie oben, `pool_accounts_v1_for_base_mint` | `Some(14 accounts)` |
| `test_build_swap_ix_with_cached_accounts` | pool_accounts im Cache → `build_swap_ix_from_pool_accounts` | Valide Instruction-Liste |

**RPC-Mock-Strategie:**  
- `SolanaRpc` mit nicht erreichbarer URL (z.B. `http://127.0.0.1:0`) — bei Cache-Hit wird RPC nie aufgerufen.  
- Oder: Eigenes `trait SolanaRpcLike` + `MockRpc` das bei jedem Call `panic!` — Tests dürfen nur Cache-Hit-Pfade durchlaufen.

---

### Phase 3: Contract-Tests (DoD-konform)

**Datei:** `tests/dex_connector_contracts.rs` (neu — DoD referenziert diese Datei, existiert aber nicht)

Für **PumpSwap AMM** (analog zu DoD §H Connector Contract Tests):

| Test | Beschreibung | Erwartung |
|------|--------------|-----------|
| `contract_pump_amm_quote_monotonic` | Größeres amount_in → größeres amount_out (mit Cache) | Monotonie |
| `contract_pump_amm_price_impact_non_decreasing` | Größeres amount_in → höhere price_impact_bps | Nicht sinkend |
| `contract_pump_amm_unknown_pair_returns_none` | base_mint nicht im Cache | `None` |
| `contract_pump_amm_zero_input` | amount_in = 0 | `None` oder amount_out = 0 |
| `contract_pump_amm_build_ix_valid_accounts` | `build_swap_ix_from_pool_accounts` mit 14 Pubkeys | Keine leere Instruction, korrekte Program-ID |

---

## 3. Implementierungsreihenfolge

1. **LivePoolCache-Tests** (Phase 1.1) — niedriges Risiko, keine neuen Abhängigkeiten.
2. **PumpFunAmmDex-Tests** (Phase 1.2) — mit Dummy-RPC; nur Cache-Hit-Pfade.
3. **Integrationstest** (Phase 2.1) — `tests/pump_amm_geyser_first_test.rs` als Regressionstest.
4. **Contract-Tests** (Phase 3) — optional, aber DoD-konform.

---

## 4. Abnahme

- [ ] Alle Phase-1-Tests grün.
- [ ] Phase-2-Test `test_quote_from_cache_no_rpc` grün (bestätigt: kein RPC bei Cache-Hit).
- [ ] Nach A.1-Implementierung: gleiche Tests weiterhin grün; keine Regressions.

---

## 5. Nicht abgedeckt (Cold Path / RPC-Fallback)

- RPC-Fallback bei Cache-Miss wird **nicht** getestet (Cold Path, explizit erlaubt).
- TX-History-Fallback für `load_pool_by_address` — würde echten RPC oder aufwendigen Mock erfordern; optional für spätere Phase.
