# Handoff: PumpSwap Extended (24-acc) Sell-Layout — JetStream/SLAVE Propagation nach EnsurePumpAmmPoolAccounts

## Regel-Verweis (Pflicht, zuerst lesen)

**WICHTIG:** Lies und befolge die STOP-CHECK Regeln in `AGENTS.md` und `.cursor/rules/ironcrab-core.mdc` BEVOR du eine Datei änderst. Wenn eine geplante Änderung gegen eine Regel verstößt, STOPPE sofort und melde den Verstoß statt die Änderung durchzuführen.

---

## Kontext (Produktion / Forensik)

- Token `y7bgE68ZWVodvVmMUWQhShAnjVTmJVGpdnC1wYspump` (PumpSwap AMM Pool `HS9UsHpMZLYzzbLwWXJfHzsRd8HmuzMLcutHwVKGt1P7`): **market-data** loggt bei `EnsurePumpAmmPoolAccounts` `force_refresh=true` korrekt **`layout=Extended { third_meta: … }`** und publiziert JetStream `PoolCacheUpdate` mit Keys `pump_amm_sell_cashback_remaining`, `pump_amm_sell_cashback_third_meta`, `pump_amm_sell_layout_ready`, ggf. `pump_amm_sell_layout_authoritative`.
- **execution-engine** Scope44-Log im selben Lauf: **`sell_extended=false`**, **`sell_cashback_third_meta=None`** → `tx_builder` baut **21-Account**-SELL statt **24** → Simulation **Custom(6023)** Overflow in `pump-amm sell.rs` / Fee-CPI-Reihenfolge.
- Ursache liegt **nicht** an fehlender Erkennung nur in market-data (dort authoritative), sondern am **Zeit-/Merge-Zustand im SLAVE** (`LivePoolCache` der execution-engine): `cache.pump_amm_sell_extended_layout(pool)` noch `(false, None)` beim zweiten Plan/Sim-Schritt nach Recovery.

Hypothese zur Verifikation im Code:

1. **`wait_for_usable_pump_amm_cache_state`** (execution_engine.rs) wartet nur auf `pump_amm_quote_ready_by_base_mint` **und** `pump_amm_swap_accounts_ready_by_base_mint`. Letztere nutzt u.a. **`pump_amm_effective_ready_for_cache_first_accounts`**. Wenn **`pump_amm_sell_extended_flag_by_market` noch false** ist, ist **`pump_amm_sell_layout_complete_for_ready`** für „extended required“ Pools fälschlich **true** im nicht-extended Zweig (`requires_extended=false` ⇒ complete) — dann gilt das Row als „ready“, obwohl der Verkauf **24 Metas** braucht.

2. Oder: JetStream-Nachricht mit **Extended-Metadaten** trifft **nach** bounded wait / **nach** rebuild ein (Ordering).

3. Oder: `apply_pool_cache_update` überschreibt/schneidet **`pump_amm_sell_*`** bei PoolDiscovered/BalanceUpdated (bekanntes Muster BUG #28) — Regressionstest erwünscht.

Referenzimplementierung **`build_swap_ix_from_pool_accounts`** (pumpfun_amm.rs): SELL ohne `sell_requires_cashback_remaining` = 21 Metas; mit Flag + `third_meta` = 24 Metas.

---

## Task

**Ziel:** Sicherstellen, dass nach **`EnsurePumpAmmPoolAccounts` / `force_refresh`** der **SLAVE der execution-engine** dieselbe authoritative **PumpSwap Sell-Extended-Information** verwendet wie market-data, **bevor** der Cold-Path (**Liquidation**, Kill-Switch, …) **`build_swap_ix_from_pool_accounts`** ausführt.

Konkret (Implementierung offen — du entscheidst nach Evidence):

- Ggf. **`wait_for_usable_pump_amm_cache_state`** erweitern oder **pool_market-spezifischen** Wait nach Recovery (Polling `cache.pump_amm_sell_extended_layout` / `pump_amm_sell_layout_ready` konsistent zu `merge_pump_amm_sell_extended_layout`/`set_pump_amm_sell_layout_authoritative`).
- Ggf. **synchrone Merge** wenn I-24d das hergibt (ohne Hot-Path-RPC).
- Sicherstellen: **kein** erfolgreiches `pump_amm_swap_accounts_ready` / kein weiterer Cold-Path-Schritt wenn **extended** laut MASTER/authority **true**, SLAVE aber **third_meta fehlt**.

---

## Relevante Invarianten (Volltext)

- **I-4 HOT PATH GEYSER-ONLY**: Keine neuen blockierenden RPC-Calls auf Discovery/Buy/Sell Hot Path; Änderungen an Waits dürfen **Cold-Path / Liquidation / Discovery-Wait-Pfade** nicht in den Hot-Path ziehen.
- **I-5 COLD PATH**: RPC dort erlaubt; dieser Fix betrifft primär Merge/Timing nach market-data Responses und JetStream.
- **I-9 SIMULATE-GATED**: Nie senden ohne erfolgreiche Simulation; Fix darf Simulation nicht umgehen.
- **I-12 DECISION RECORD**: Intents ohne sauberes Outcome/Checks nicht „verschlucken“.
- **I-24a JETSTREAM SSOT**: Bot-Zustand (PoolCache) konsistent mit publizierten **`PoolCacheUpdate`**-Feldern — Extended-Sell-Metadaten müssen im SLAVE ankommen oder der Builder darf erst planen, wenn konsistent.

---

## Bestehendes Pattern / Code-Anker

| Ort | Rolle |
|-----|--------|
| `market_data.rs` (~5433–5496) | `EnsurePumpAmmPoolAccounts`: MASTER `set_pump_amm_sell_layout_authoritative`, JetStream meta `pump_amm_sell_cashback_remaining` / `third_meta` |
| `execution_engine.rs` `wait_for_usable_pump_amm_cache_state` | Aktueller Wait — prüfen ob extended abgedeckt |
| Cold-path recovery (~9425–9474) | `request_discovery_and_wait` + `wait_for_usable_pump_amm_cache_state` + `continue` rebuild |
| `live_pool_cache.rs` | `pump_amm_sell_extended_layout`, `pump_amm_sell_layout_complete_for_ready`, `get_explicit_jetstream_ready_pump_amm_pool_accounts_v14_for_pool_market`, Tests `test_pump_amm_swap_accounts_ready_false_when_extended_layout_missing_third_meta` |
| `pool_cache_sync.rs` `apply_pool_cache_update` | Merge JetStream Metadata → Extended-Flags |
| `tx_builder.rs` PumpAmm Branch | Liest `cache.pump_amm_sell_extended_layout(&pool_id)` für `build_swap_ix_from_pool_accounts` |

---

## Erlaubte Dateien (vorschlagsweise)

- `Iron_crab/src/bin/execution_engine.rs`
- `Iron_crab/src/execution/live_pool_cache.rs`
- `Iron_crab/src/execution/pool_cache_sync.rs`
- `Iron_crab/src/bin/market_data.rs` (nur falls Publish/Meta-Lücken)
- `Iron_crab/src/execution/tx_builder.rs` (nur falls Guard nötig — minimal)
- `Iron_crab/src/solana/dex/pumpfun_amm.rs` (nur falls Contract-Hilfsfunktionen)
- Bestehende Tests erweitern / neue Unit-Tests unter `Iron_crab/tests/` oder `#[cfg(test)]` in selben Modulen

---

## Verboten

- Keine RPC-Hot-Path-Erweiterung (I-7).
- Kein Senden bei Sim-Fail (I-9).
- Kein großes Refactor außerhalb PumpSwap-Cold-Path/Pool-Cache.
- Keine Änderungen an `Iron_crab-eval/tests/` (Eval-Repo — falls separater Test-Scope, nur auf Anweisung).

---

## Prüf-Befehle (Impl-Repo)

```bash
cargo fmt --check
cargo clippy -- -D warnings
cargo test
```

Eval Level 5 / vollständige Eval-Suite gemäß CI nach Merge-Koordination.

---

## Evidence / Referenz

- Regressionskandidat: gleicher Mint/Pool; Erfolg wenn nach Recovery Scope44 **`sell_extended=true`** und `sell_cashback_third_meta=Some(...)` oder Build explizit wartet, bis SLAVE-Flags gesetzt sind.
- `KNOWN_BUG_PATTERNS.md` #28, #34, #35 — Merge/Overwrite/Fee — nicht duplizieren, sondern verifizieren.
