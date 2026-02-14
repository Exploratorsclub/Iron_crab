# IronCrab — Bug-Tracker & Fixes

Erstellt: 2026-02-13 | Branch: `architecture-rebuild`

---

## 1. BEHOBENE BUGS (Fixes deployed/committed)

### FIX-01: Revert fehlerhafter Commits → `e341c04b`
**Datum**: 2026-02-09
**Problem**: 18 Commits (bis `b22bb0a9`) hatten ungewollt die Liquidation zerstört und Architekturprinzipien verletzt (RPC-Calls im Hot Path).
**Fix**: Hard-Reset auf `e341c04b`, danach selektive Re-Integration.

### FIX-02: Multi-DEX Retry-Pfad für Liquidation
**Datum**: 2026-02-09
**Problem**: Bei `BondingCurveComplete` (Error 6005) wurde nur ein DEX versucht. Zweiter Token wurde nicht über PumpSwap AMM probiert.
**Fix**: Liquidation versucht jetzt Multi-Pool zuerst, PumpFun als Fallback. Alle DEXes werden durchprobiert.

### FIX-03: Grafana Liquidation als "buy" angezeigt
**Datum**: 2026-02-09
**Problem**: Liquidation-Sells wurden im Dashboard als "buy" klassifiziert.
**Fix**: `side`-Feld korrekt in ExecutionResult Metadata gesetzt.

### FIX-04: PnL-Berechnung >100% Verlust
**Datum**: 2026-02-09
**Problem**: PnL zeigte >100% Verlust für erfolgreich verkaufte Tokens.
**Fix**: PnL-Berechnung in `trades_server.py` korrigiert (Division durch korrekte Basis).

### FIX-05: Geyser Reconnect bei neuer ATA
**Datum**: 2026-02-11
**Problem**: Jedes Mal wenn ein Token gekauft wurde und eine neue ATA erstellt wurde, musste der gesamte Geyser-Stream reconnecten (`subscribe_once()`).
**Fix**: Migration von `subscribe_once()` zu `subscribe_with_request()` + `SinkExt` in 3 Modulen. Neue ATAs werden dynamisch hinzugefügt ohne Stream-Reconnect.

### FIX-06: Bonding Curve Exit Signal für Momentum-Bot
**Datum**: 2026-02-11
**Problem**: Kein Exit-Signal basierend auf Bonding-Curve-Fortschritt. Tokens konnten nicht automatisch verkauft werden bevor die Curve migriert.
**Fix**: Neuer konfigurierbarer `bonding_curve_exit_threshold` (Default 98%) mit Hot-Reload via UI. Basiert auf Geyser-Daten, keine RPC-Calls.

### FIX-07: Grafana Dashboard — Run-basierte Trades & 24h PnL
**Datum**: 2026-02-11
**Problem**: Dashboard zeigte nur 20 Trades; kein 24h PnL-Wert.
**Fix**: Alle Trades des aktuellen Runs + letzte 20 vom vorigen Run. Neue Panels für Wallet-Delta und Realized PnL.

### FIX-08: WALLET_TOTAL_SOL_LAMPARTS Metrik (Locked Capital)
**Datum**: 2026-02-11
**Problem**: Metrik enthielt nur unlocked SOL, nicht das in Trades gebundene Kapital.
**Fix**: `total_sol()` + `wsol_balance()` aus LockManager statt nur `available_sol()`.

### FIX-09: Sensitive Credentials in Version Control
**Datum**: 2026-02-11
**Problem**: Server-IP, Username und Port in `.github/copilot-instructions.md`.
**Fix**: Credentials durch Platzhalter ersetzt.

### FIX-10: WsolManager Konsistenz (LockManager.available_wsol)
**Datum**: 2026-02-12
**Problem**: `fetch_and_update_balances()` aktualisierte Prometheus-Gauge aber nicht `LockManager.available_wsol`.
**Fix**: WSOL-Updates werden konsistent über LockManager propagiert.

### FIX-11: WsolManager RPC-Fallback entfernt
**Datum**: 2026-02-12
**Problem**: 60s RPC-Polling im NATS-Modus verletzte Geyser-First-Architektur.
**Fix**: RPC-Fallback und Polling-Only-Modus entfernt. WsolManager arbeitet vollständig über NATS/Geyser-Events.

### FIX-12: Doppelter JetStream Consumer in execution-engine
**Datum**: 2026-02-12
**Problem**: `execution_engine` erstellte zwei separate ephemere JetStream-Consumer für `POOL_CACHE` Updates → Race Conditions, verpasste Updates, Delays.
**Fix**: Einzelner Consumer wird wiederverwendet für Bootstrap und Runtime.
**Commit**: `a29ecfb6`

### FIX-13: RPC-Fallback für Creator bei Liquidation
**Datum**: 2026-02-12
**Problem**: PumpFun-Liquidation scheiterte wenn Creator nicht im LivePoolCache war.
**Fix**: Bei Liquidation wird Creator per RPC nachgeladen falls nicht im Cache (Cold Path — architekturkonform).
**Commit**: `a29ecfb6`

### FIX-14: Ghost Positions durch stale JetStream Snapshots
**Datum**: 2026-02-12
**Problem**: `MAX_BOOTSTRAP_MINTS=30` begrenzte den Bootstrap. Stale non-zero JetStream-Einträge blieben bestehen → falsche Open-Position-Anzeige.
**Fix**: Step 2.5 in market-data Bootstrap: Alle verbleibenden JetStream-Einträge werden enumeriert, zero-balance Overrides für nicht abgedeckte non-zero Mints publiziert.
**Commit**: `43941752`

### FIX-15: Hardcoded quote_mint in DEX-Parsern (Bug H)
**Datum**: 2026-02-12
**Problem**: `parse_meteora_transaction()`, `parse_raydium_cpmm_transaction()` und `parse_raydium_v4_swap()` setzten `quote_mint = SOL_MINT_PUBKEY` hardcoded → false Arbitrage-Signale für non-SOL Pairs.
**Fix**: Dynamische quote_mint-Extraktion aus Transaction-Token-Balances.
**Commit**: `0b1b724e`

### FIX-16: Initiales WalletBalanceUpdate bei market-data Startup
**Datum**: 2026-02-13
**Problem**: execution-engine startete mit Default 1.0 SOL und wurde erst beim ersten Geyser-Event aktualisiert → falsche WSOL/SOL-Anzeige, kein Wrapping.
**Fix**: market-data publiziert beim Bootstrap ein initiales `WalletBalanceUpdate` mit SOL+WSOL-Balances.
**Commit**: `c1e8d667`

### FIX-17: fill_in/fill_out Accuracy (False Take-Profit Triggers)
**Datum**: 2026-02-13
**Schweregrad**: CRITICAL
**Problem**: Bei BUY mit `lamport_noise=true` fiel `fill_in` auf `intent.required_capital` zurück → bis zu 29x falsch. SELL `fill_out` war immer `None` bei ATA-Lifecycle. Falsche entry_price → falsche Take-Profit/Stop-Loss Entscheidungen.
**Fix**: Dreistufige Fallback-Kette: (1) Inner-Instruction-Parsing für System.transfer, (2) Rent-Adjusted Lamport Delta, (3) intent capital als letzter Ausweg. Dashboard PnL konsistent auf wallet_delta umgestellt.
**Dateien**: `src/bin/execution_engine.rs`, `scripts/trades_server.py`

### FIX-18: Bug B — Orphaned Buy Recovery
**Datum**: 2026-02-13
**Problem**: Race Condition: `cleanup_stale_pending()` entfernte pending intent bevor `ExecutionResult` ankam → Position nie erstellt → kein Sell.
**Fix**: Orphaned Buy Recovery: Wenn confirmed BUY ohne pending intent → Position aus ExecutionResult + TokenTracker rekonstruieren.
**Dateien**: `src/bin/momentum_bot.rs`

### FIX-19: Bug C — Sell-Retry nach Failure/Timeout
**Datum**: 2026-02-13
**Problem**: `exit_generated` wurde bei Sell-Failures nicht zurückgesetzt → kein Retry bis `max_hold_time` + `reconcile_timed_exits()`.
**Fix**: Unconditional Reset von `exit_generated` in Failed/Timeout Handlern für Sell-Side. Gilt für normalen und orphaned Pfad.
**Dateien**: `src/bin/momentum_bot.rs`

---

## 2. OFFENE BUGS (Analyse erforderlich / Fix ausstehend)

### BUG-A: PumpFun Custom(6023) — Intermittierende Sell-Fehler
**Schweregrad**: HOCH
**Betroffene Tokens** (2026-02-13 Run): `64HemTH7`, `34c3bPRz`
**Symptom**: Momentum-Bot Sell-Versuche scheitern wiederholt mit `Custom(6023)` ("NotEnoughTokensToSell"), obwohl Liquidation auf denselben Tokens erfolgreich ist.

**Root Cause (detailliert)**:

Drei zusammenwirkende Probleme verhindern erfolgreiche SELLs bei migrierten PumpFun-Tokens:

1. **`find_best_sell_pool()` ist DEX-agnostisch** — Die Pool-Auswahl im Momentum-Bot basiert nur auf `last_trade_ratio` und `last_updated`. Es gibt **keinen Check ob eine PumpFun Bonding Curve `complete=true`** ist. Wenn ein Token migriert wird, hat der PumpFun-Pool oft noch aktuelle Trade-Daten (von vor der Migration) und wird weiter als "bester" Pool ausgewählt.

2. **Kein Pool-Failure-Tracking** — Wenn ein SELL auf einem Pool scheitert, wird `exit_generated=false` zurückgesetzt (Bug-C Fix), aber der gescheiterte Pool wird **nicht markiert**. Beim nächsten Tick wählt `find_best_sell_pool()` denselben Pool erneut aus → endlose Wiederholung des selben Fehlers.

3. **Kein Multi-Pool-Fallback in der Execution Engine für normale SELLs** — Die Liquidation hat einen 3-Phasen-Routing-Pfad (Multi-Pool → LivePoolCache → PumpFun-Fallback). Normale SELL-Intents vom Momentum-Bot verwenden **nur** den vom Intent spezifizierten DEX. Wenn dieser scheitert, wird der Intent abgelehnt — kein automatischer Versuch mit alternativen DEXes.

**Warum Liquidation funktioniert**: Der `handle_sell_liquidation()`-Pfad probiert in Phase 1 zuerst PumpSwap AMM, Meteora, Raydium und Orca. Für migrierte Tokens findet er den PumpSwap-AMM-Pool und verkauft dort erfolgreich.

**Hinweis BUG-I**: Der Guard in `pumpfun.rs` (Zeile 888-902: `real_reserves == 0 && virtual_reserves > 0 → return Ok(None)`) **existiert bereits** im aktuellen Code. Der Architecture Audit Status "REGRESSION DURCH REVERT" ist **veraltet**. Dieser Guard fängt den Fall ab, wenn die Migration im Cache sichtbar ist. Bug-A tritt auf, wenn die Migration im Cache **noch nicht sichtbar** ist oder `real_token_reserves` nur stale (nicht 0) sind.

**Status**: ✅ BEHOBEN — FIX-20 (Pool-Migration & Failure-Tracking) + FIX-21 (Reserve-basiertes Quoting)

---

#### FIX-20 Plan: Bug-A — PumpFun Sell-Failure mit Pool-Migration & Failure-Tracking

**Ziel**: Momentum-Bot soll migrierte PumpFun-Pools automatisch meiden und bei wiederholten Sell-Fehlern auf alternative Pools wechseln.

**Keine RPC-Calls im Hot Path.** Alle Daten kommen aus Geyser/NATS Events.

---

**Teil 1: `PoolInfo` struct erweitern** (momentum_bot.rs)

```rust
struct PoolInfo {
    pool_address: String,
    dex: String,
    dex_pool_accounts: Option<Vec<String>>,
    first_seen_slot: u64,
    last_trade_slot: u64,
    last_trade_ratio: Option<f64>,
    last_updated: std::time::Instant,
    // --- NEU ---
    /// PumpFun bonding curve complete flag (None = nicht PumpFun oder unbekannt)
    bonding_curve_complete: Option<bool>,
    /// Anzahl fehlgeschlagener SELL-Versuche auf diesem Pool
    sell_fail_count: u32,
    /// Zeitpunkt des letzten SELL-Fehlers
    last_sell_fail_at: Option<std::time::Instant>,
}
```

`PoolInfo::new()` initialisiert die neuen Felder mit `None`/`0`/`None`.

---

**Teil 2: BondingCurveProgress → Pool-Migration erkennen** (momentum_bot.rs)

Im `MarketEventKind::BondingCurveProgress` Handler (~Zeile 7093):

```rust
MarketEventKind::BondingCurveProgress { mint, progress_bps, complete, .. } => {
    // Bestehend: Position-Tracker updaten
    let mut positions = ctx.positions.write();
    if let Some(pos) = positions.get_mut(mint.as_str()) {
        pos.bonding_curve_progress_bps = Some(*progress_bps);
    }
    drop(positions);

    // NEU: Pool-Migration-Status in mint_pools aktualisieren
    if *complete {
        let mut pools = ctx.mint_pools.write();
        if let Some(pool_list) = pools.get_mut(mint.as_str()) {
            for pool in pool_list.iter_mut() {
                if pool.dex == "pumpfun" {
                    pool.bonding_curve_complete = Some(true);
                    warn!(
                        mint = %mint,
                        pool = %pool.pool_address,
                        "PumpFun pool marked as migrated (bonding curve complete)"
                    );
                }
            }
        }
    }
}
```

---

**Teil 3: Sell-Failure → Pool-Failure-Count erhöhen** (momentum_bot.rs)

Im `ExecutionStatus::Failed` und `ExecutionStatus::Timeout` Handler für Sell-Side:
(Im bestehenden Block der Bug-C Fix-Logik, nach `exit_generated = false`)

```rust
} else if pending.side == TradeSide::Sell {
    // [Bestehend: Bug-C Fix] Reset exit_generated
    let mut positions = self.positions.write();
    if let Some(pos) = positions.get_mut(&pending.mint) {
        pos.exit_generated = false;
        pos.exit_generated_at = None;
    }
    drop(positions);

    // NEU: Pool-Failure-Count erhöhen
    let mut pools = self.mint_pools.write();
    if let Some(pool_list) = pools.get_mut(&pending.mint) {
        if let Some(pool_info) = pool_list.iter_mut().find(|p| p.pool_address == pending.pool) {
            pool_info.sell_fail_count += 1;
            pool_info.last_sell_fail_at = Some(Instant::now());
            warn!(
                mint = %pending.mint,
                pool = %pending.pool,
                dex = %pending.dex,
                sell_fail_count = pool_info.sell_fail_count,
                "Pool sell failure tracked — will prefer alternatives on retry"
            );
        }
    }
}
```

**Wichtig**: Auch im Orphaned-Sell-Recovery-Pfad (wenn `pending_opt.is_none()` und `exit_generated` Reset erfolgt) den gleichen Pool-Failure-Count inkrementieren. Dafür muss der Pool aus dem `ExecutionResult`-Metadaten oder aus der Position extrahiert werden.

---

**Teil 4: `find_best_sell_pool()` — Exclusion-Logik** (momentum_bot.rs)

```rust
fn find_best_sell_pool(&self, mint: &str, token_amount: u64, original_pool: &str)
    -> Result<(String, String, Vec<String>, f64, usize)>
{
    let pools = self.mint_pools.read();
    let candidates = pools
        .get(mint)
        .ok_or_else(|| anyhow::anyhow!("No pools known for mint {}", mint))?;

    let now = std::time::Instant::now();
    let max_age = std::time::Duration::from_secs(300);
    let fail_cooldown = std::time::Duration::from_secs(120);  // NEU
    const MAX_FAIL_COUNT: u32 = 3;                             // NEU

    // Phase 1: Filter gültige Pools
    let valid: Vec<_> = candidates
        .iter()
        .filter(|p| {
            p.dex_pool_accounts.is_some()
                && p.last_trade_ratio.is_some()
                && now.duration_since(p.last_updated) < max_age
        })
        .collect();

    // Phase 2: Exclusion (migrierte + kürzlich gescheiterte Pools)
    let preferred: Vec<_> = valid.iter()
        .filter(|p| {
            // Skip: PumpFun-Pool mit bestätigter Migration
            if p.bonding_curve_complete == Some(true) {
                return false;
            }
            // Skip: Pool mit >= MAX_FAIL_COUNT Fehlern im Cooldown-Fenster
            if p.sell_fail_count >= MAX_FAIL_COUNT {
                if let Some(last_fail) = p.last_sell_fail_at {
                    if now.duration_since(last_fail) < fail_cooldown {
                        return false;
                    }
                }
            }
            true
        })
        .collect();

    // Phase 3: Wenn alle excludiert → Fallback auf Pool mit niedrigstem fail_count
    let usable = if preferred.is_empty() {
        warn!(mint = %mint, valid_count = valid.len(),
            "All pools excluded by migration/failure — using best-available fallback");
        &valid
    } else {
        &preferred
    };

    // [Bestehender Code: Quotes berechnen, beste Route wählen]
    // ...
}
```

---

**Teil 5: Sell-Success → Failure-Count zurücksetzen** (momentum_bot.rs)

Im `ExecutionStatus::Confirmed` Handler für `TradeSide::Sell`:

```rust
// NEU: Bei erfolgreichem Sell den Failure-Count des Pools zurücksetzen
let mut pools = self.mint_pools.write();
if let Some(pool_list) = pools.get_mut(&pending.mint) {
    if let Some(pool_info) = pool_list.iter_mut().find(|p| p.pool_address == pending.pool) {
        if pool_info.sell_fail_count > 0 {
            info!(
                mint = %pending.mint, pool = %pending.pool,
                old_fail_count = pool_info.sell_fail_count,
                "Sell succeeded — resetting pool failure count"
            );
            pool_info.sell_fail_count = 0;
            pool_info.last_sell_fail_at = None;
        }
    }
}
```

---

**Zusammenfassung der Änderungen**:

| Datei | Änderung | Risiko |
|-------|----------|--------|
| `src/bin/momentum_bot.rs` | `PoolInfo` struct: 3 neue Felder | Minimal — rein additiv |
| `src/bin/momentum_bot.rs` | `BondingCurveProgress` Handler: Pool-Migration-Flag setzen | Minimal — nur Metadata |
| `src/bin/momentum_bot.rs` | `ExecutionStatus::Failed/Timeout` Sell: Pool-Fail-Count | Niedrig — neben bestehendem Bug-C Fix |
| `src/bin/momentum_bot.rs` | `find_best_sell_pool()`: Exclusion-Filter | Mittel — Kern-Routing-Logik, aber mit Fallback |
| `src/bin/momentum_bot.rs` | `ExecutionStatus::Confirmed` Sell: Fail-Count Reset | Minimal — rein additiv |

**Kein RPC im Hot Path. Keine neuen NATS Topics. Keine Architektur-Änderung.**

**Erwartete Wirkung**: Migrierte PumpFun-Pools werden nach dem `BondingCurveProgress` Event sofort gemieden. Selbst ohne dieses Event werden Pools nach 3 gescheiterten Sells für 120s ausgeschlossen, sodass der Bot auf PumpSwap AMM, Meteora, Raydium oder Orca wechselt.

---

#### FIX-21: Reserve-basiertes Multi-Pool-Routing (SLAVE LivePoolCache)
**Datum**: 2026-02-13
**Problem**: FIX-20 behebt die Exclusion-Logik, aber `find_best_sell_pool()` und `find_best_buy_pool()` nutzen weiterhin `last_trade_ratio` (grobe Approximation aus dem letzten beobachteten Trade) statt echter Reserve-basierter Quotes. Das führt zu suboptimaler Pool-Auswahl.

**Root Cause**: Der Momentum-Bot hatte keinen Zugriff auf den `LivePoolCache`, der in `market-data` (MASTER) und `execution-engine` (SLAVE) vorhanden war. Die Pool-Auswahl war daher nicht datengetrieben.

**Lösung**:
1. **Shared Modul** `src/execution/pool_cache_sync.rs` — Extrahiert `build_minimal_pool_state()`, `apply_pool_cache_update()` und `bootstrap_pool_cache_from_jetstream()` aus `execution_engine.rs` in ein wiederverwendbares Modul.
2. **SLAVE LivePoolCache im Momentum-Bot** — `MomentumContext` bekommt einen eigenen `LivePoolCache`, der beim Start aus JetStream gebootstrapt und laufend per `PoolCacheUpdate` Events aktualisiert wird.
3. **Reserve-basiertes Quoting** — Neue `quote_output_amount()` API in `quote_calculator.rs` berechnet Output-Beträge direkt aus `CachedPoolState` (ohne `TradeIntent`). `find_best_sell_pool()` und `find_best_buy_pool()` nutzen primär Cache-Quotes, Fallback auf `last_trade_ratio`.

**Dateien**:
| Datei | Änderung |
|-------|----------|
| `src/execution/pool_cache_sync.rs` | NEU — Shared Bootstrap/Sync |
| `src/execution/mod.rs` | Modul registriert |
| `src/execution/quote_calculator.rs` | `quote_output_amount()` API |
| `src/bin/execution_engine.rs` | Nutzt shared Modul |
| `src/bin/momentum_bot.rs` | LivePoolCache + JetStream Consumer + reserve-basierte Quotes |

**Kein RPC im Hot Path. Keine neuen NATS Topics. Architektur-konform (SLAVE Cache Pattern).**

### ~~BUG-B: Momentum-Bot verliert Position — Kein Sell-Intent generiert~~ ✅ BEHOBEN
**Schweregrad**: KRITISCH → **BEHOBEN** (2026-02-13)
**Fix**: Orphaned Buy Recovery in `handle_execution_result()`: Wenn ein `ExecutionResult` mit `status == Confirmed` und `side == BUY` eintrifft aber kein `pending_intent` existiert, wird die Position aus `ExecutionResult` Metadaten + `TokenTracker` rekonstruiert.
**Dateien**: `src/bin/momentum_bot.rs`

### ~~BUG-C: Momentum-Bot Retry-Bug — Ein Versuch, dann Aufgabe~~ ✅ BEHOBEN
**Schweregrad**: HOCH → **BEHOBEN** (2026-02-13)
**Fix**: `exit_generated` wird jetzt in `ExecutionStatus::Failed` und `ExecutionStatus::Timeout` Handlern für Sell-Side-Trades zurückgesetzt. Gilt sowohl für den normalen Pending-Intent-Pfad als auch für den Orphaned-Sell-Recovery-Pfad (konsistentes unconditional Reset).
**Dateien**: `src/bin/momentum_bot.rs`

### ~~BUG-D: Falscher Creator im Cache → ConstraintSeeds bei SELL~~ ✅ BEHOBEN
**Schweregrad**: HOCH → **BEHOBEN** (2026-02-14, FIX-22)
**Betroffene Tokens** (2026-02-13 Run): `64HemTH7`, `34c3bPRz`
**Symptom**: Alle Momentum-Bot SELL-Versuche scheiterten mit `Custom(2006)` (ConstraintSeeds) weil der `creator_vault` PDA aus einem falschen Creator abgeleitet wurde. Liquidation per RPC-Fallback funktionierte.

**Root Cause (detailliert)**:

Zwei zusammenwirkende Fehler:

1. **`instruction_accounts[7]` ist nicht immer der Creator**: `parse_pumpfun_create()` und `geyser_pool_discovery` extrahieren den Creator aus `instruction_accounts[7]` der CREATE-Transaktion. Bei Tokens die über CPI (Bundler, Launchpads) erstellt werden, kann der Account an Index 7 von der Bonding-Curve-Account-Daten (`data[49..81]`) abweichen.

2. **First-Write-Wins Cache blockiert Korrektur**: In `market_data.rs`:
   - `PoolCreated` Handler schreibt `creator_cache[mint]` **unconditional** (Zeile 2638)
   - `BondingCurveUpdate` Handler hat `contains_key`-Guard → **SKIP** wenn PoolCreated zuerst kam
   - `DevWalletIdentified` aus BondingCurveUpdate wird **nicht emittiert** → autoritativer Creator erreicht Momentum-Bot nie

**Server-Log-Evidenz**:

| Token | Momentum-Bot Creator | Korrekter Creator (RPC) | Sell-Ergebnis |
|-------|---------------------|------------------------|---------------|
| `64HemTH7` | `Ca8hHy...WMynz` | `B62Dvk...JhMYo` | ~20x `Custom(2006)` |
| `34c3bPRz` | `E77jVj...q1UP` | `GfBB85...4dqf` | ~20x `Custom(2006)` |

**Fix**: FIX-22 (siehe unten)

#### FIX-22: Autoritative Creator-Quelle + LivePoolCache Cross-Check
**Datum**: 2026-02-14
**Problem**: Falscher Creator in `creator_cache` und `TokenTracker.dev_wallet` durch nicht-autoritativen `instruction_accounts[7]` bei CPI-erstellten Tokens. `BondingCurveUpdate` (autoritativ) wurde durch `contains_key`-Guard blockiert.

**Lösung (2 Teile)**:

1. **market_data.rs — BondingCurveUpdate als autoritative Quelle**:
   - `pool_creator_cache`: `contains_key`-Guard entfernt → immer überschreiben
   - `creator_cache`: `contains_key`-Guard ersetzt durch Mismatch-Detection → immer überschreiben
   - `DevWalletIdentified`: Wird emittiert wenn Creator neu oder **anders** (Korrektur-Event)
   - WARN-Log bei Mismatch für Produktions-Diagnostik

2. **momentum_bot.rs — LivePoolCache Cross-Check**:
   - Neue Methode `resolve_authoritative_creator()` auf `MomentumContext`
   - Bei Entry- und Exit-Intents: Creator aus `TokenTracker.dev_wallet` wird gegen `LivePoolCache.get_pumpfun_creator()` geprüft
   - LivePoolCache-Wert (Geyser-Account-Daten) hat Vorrang → korrigiert auch TokenTracker
   - Fallback: TokenTracker-Wert wenn LivePoolCache den Token nicht kennt

**Dateien**:
| Datei | Änderung |
|-------|----------|
| `src/bin/market_data.rs` | BondingCurveUpdate: autoritative Cache-Writes + Mismatch-WARN |
| `src/bin/momentum_bot.rs` | `resolve_authoritative_creator()` + Cross-Check bei Entry/Exit |

**Kein RPC im Hot Path. Keine neuen NATS Topics.**

---

## 3. BEKANNTE ARCHITEKTUR-PROBLEME (aus Architecture Audit)

Diese Bugs sind im Detail in `docs/ARCHITECTURE_AUDIT_2026-02-07.md` dokumentiert:

| ID | Problem | Schweregrad | Status |
|----|---------|-------------|--------|
| Audit-A | Killswitch-Liquidation überspringt Tokens | ⚠️ TEILWEISE BEHOBEN | FIX-02, FIX-12, FIX-13 |
| Audit-B | `load_pool_from_geyser()` macht 20 RPC-Retries | ❌ OFFEN | Priorität 3 |
| Audit-C | PumpFunAmmDex eigene RPC-Infrastruktur | ❌ OFFEN | Priorität 3 |
| Audit-D | Token-Decimals immer per RPC | ❌ OFFEN | Priorität 3 |
| Audit-E | `cleanup_wallet_after_liquidation()` per RPC | ❌ OFFEN | Priorität 3 |
| Audit-F | Orca Reserve-Fetching 5min TTL + RPC | ❌ OFFEN | Priorität 3 |
| Audit-G | Stale JetStream Wallet-Snapshots | ✅ BEHOBEN | FIX-14 |
| Audit-H | Hardcoded quote_mint in DEX-Parsern | ✅ BEHOBEN | FIX-15 |
| Audit-I | PumpFun SELL stale Quote für migrierte Tokens | ✅ BEHOBEN | Guard in pumpfun.rs (Z.888-902). Restprobleme → BUG-A/FIX-20 |

---

## FIX-17: CRITICAL — fill_in/fill_out Accuracy (False Take-Profit Triggers)

**Datum**: 2026-02-13  
**Schweregrad**: CRITICAL — Bot traf Trading-Entscheidungen auf Basis falscher Preisdaten

### Problem
Bei BUY-Trades mit `lamport_noise=true` (ATA wird erstellt) fiel `fill_in` auf `intent.required_capital` zurück.
Dies war katastrophal falsch wenn die DEX weniger SOL akzeptiert als beabsichtigt (z.B. PumpFun Bonding Curve fast voll):

- **D39XKvFT**: `fill_in` = 0.00125 SOL (intent), **real**: 0.000043 SOL → **28.6x Fehler**
- Falsche `entry_price` → Momentum-Bot sah +2949.3% Gain statt real ~6.5%
- Take-Profit wurde fälschlicherweise ausgelöst, Token mit Verlust verkauft

Bei SELL-Trades war `fill_out` immer `None` wenn `lamport_noise=true` (ATA geschlossen), weil der
native SOL Fallback durch das Lifecycle-Noise-Gate blockiert wurde.

### Root Cause
`compute_intent_fills_best_effort()` in `execution_engine.rs`:
- Zeile 424-428 (alt): `lamport_noise → fill_in = intent.required_capital` (kann 29x falsch sein)
- Zeile 439 (alt): `lamport_noise → fill_out = None` (SELL SOL-Erlös fehlt komplett)

### Fix
Neue dreistufige Fallback-Kette für native SOL-Legs mit `lamport_noise`:

1. **Inner Instruction Parsing** (`extract_swap_sol_from_inner_instructions`):
   - Parst `meta.inner_instructions` nach System Program `transfer` Instruktionen
   - Filtert `createAccount` aus (das ist ATA-Rent, kein Swap)
   - Genaueste Methode: erfasst Swap-Betrag + DEX-Fees (ohne ATA-Rent)

2. **Rent-Adjusted Lamport Delta**:
   - `compute_wallet_lamport_delta_best_effort` gibt jetzt auch `rent_adjustment` zurück
   - Bereinigtes Delta = `raw_delta + rent_created - rent_refunded`
   - Entfernt ~96% des Errors (ATA-Rent ist ~2.04M lamports)

3. **intent.required_capital** (letzter Ausweg mit WARN-Log)

### Dashboard PnL
`trades_server.py`: SELL proceeds nutzt jetzt explizit `wallet_delta` (konsistent mit BUY cost).
ATA-Rent hebt sich auf. Dashboard zeigt realen Wallet-Impact inklusive aller Fees.

### Dateien
- `src/bin/execution_engine.rs`: `compute_wallet_lamport_delta_best_effort`, `extract_swap_sol_from_inner_instructions`, `compute_intent_fills_best_effort`
- `scripts/trades_server.py`: PnL-Berechnung in 3 Blöcken (run, last, 24h)

---

## 4. VERLORENE ÄNDERUNGEN DURCH REVERT (Cherry-Pick Status)

| Priorität | Beschreibung | Status |
|-----------|-------------|--------|
| **CRITICAL** | fill_in/fill_out Accuracy (FIX-17) | ✅ FIXED |
| P1 | PumpSwap AMM Geyser-First Integration | ❌ FEHLT |
| P1 | PumpFun SELL migrierte Tokens → `Ok(None)` | ✅ FIXED (Guard existiert in pumpfun.rs Z.888-902) |
| P1 | `emit_sim_failed_decision()` → `Err` für Retry | ✅ FIXED (Zeile 7799) |
| P2 | Creator-Handling & DEX-Normalisierung | ❌ FEHLT |
| P2 | Market-Data WSOL-Seeding & Pool-Propagation | ⚠️ TEILWEISE (FIX-16) |
| P2 | TX-Builder Cache-capped min_out | ❌ FEHLT |
| P3 | `available_trading_capital_lamports` Metrik | ❌ FEHLT |
