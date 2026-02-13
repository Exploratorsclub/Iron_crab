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

---

## 2. OFFENE BUGS (Analyse erforderlich / Fix ausstehend)

### BUG-A: PumpFun Custom(6023) — Intermittierende Sell-Fehler
**Schweregrad**: HOCH
**Betroffene Tokens** (2026-02-13 Run): `64HemTH7`, `34c3bPRz`
**Symptom**: Momentum-Bot Sell-Versuche scheitern wiederholt mit `Custom(6023)` ("NotEnoughTokensToSell"), obwohl Liquidation auf denselben Tokens erfolgreich ist.
**Analyse**:
- Momentum-Bot verwendet `sell_routing=primary` → Quote basiert auf gecachten `real_token_reserves`
- Liquidation verwendet `sell_routing=pumpfun_fallback` → frisches RPC-Quote mit Slippage
- Cache-Wert von `real_token_reserves` kann veraltet sein → on-chain Reject
- Zusammenhang mit **BUG-I** im Architecture Audit (stale Quote für migrierte Tokens)
**Status**: ❌ OFFEN
**Nächster Schritt**: Fix aus Architecture Audit A.5 (SELL bei migrierten Tokens → `return Ok(None)`) re-implementieren

### BUG-B: Momentum-Bot verliert Position — Kein Sell-Intent generiert
**Schweregrad**: KRITISCH
**Betroffener Token** (2026-02-13 Run): `JBzDdsaB`
**Symptom**: Bot kauft Token (Buy-Intent bestätigt), generiert aber danach **keinerlei Sell-Intents**. Position wird komplett "vergessen".

**Root Cause**: Race Condition zwischen `cleanup_stale_pending()` und `ExecutionResult`-Verarbeitung.

1. Buy-Intent wird in `pending_intents` registriert (Zeile 2900)
2. `cleanup_stale_pending()` (Zeile 3252) entfernt den Intent nach **2 Minuten** (`Duration::from_secs(120)`)
3. Wenn das `ExecutionResult` erst nach dem Cleanup eintrifft, gibt `pending_intents.remove(&result.intent_id)` `None` zurück (Zeile 2947)
4. Der Early-Return-Pfad (Zeile 2983-2986) behandelt nur Liquidation-Confirms — alle anderen werden mit `debug!` geloggt und verworfen
5. `open_position()` (Zeile 3114) wird **nie aufgerufen** → kein `PositionTracker` erstellt → kein Sell je generiert

**Betroffene Code-Stellen**:
- `cleanup_stale_pending()`: Zeile 3252-3262
- `handle_execution_result()` Early Return: Zeile 2950-2986
- `open_position()`: nur erreichbar wenn `pending_opt` `Some` ist (Zeile 3114)

**Fix-Vorschlag**:
1. Wenn ein `ExecutionResult` mit `status == Confirmed` und `side == Buy` eintrifft, aber kein `pending_intent` existiert → Position trotzdem erstellen anhand von `result.token_mint` und `result.fill_out`
2. Alternativ: Cleanup-Timeout erhöhen oder nur für fehlgeschlagene/rejected Intents anwenden
3. Logging von `warn!` statt `debug!` wenn ein bestätigter Buy ohne pending Intent verworfen wird

**Status**: ❌ OFFEN — Root Cause identifiziert, Fix ausstehend

### BUG-C: Momentum-Bot Retry-Bug — Ein Versuch, dann Aufgabe
**Schweregrad**: HOCH
**Betroffener Token** (2026-02-13 Run): `ANe7aVGP`
**Symptom**: Bot versucht einen einzigen Sell (scheitert mit `Custom(6003)` / `TooLittleSolReceived`), danach werden keine weiteren Sell-Versuche unternommen. Über 1 Stunde ohne Retry, obwohl andere Tokens im selben Zeitraum dutzende Retries hatten.

**Root Cause**: `exit_generated` wird bei Sell-Failures **nicht zurückgesetzt**.

1. Sell-Intent wird publiziert → `mark_exit_generated()` setzt `exit_generated = true` (Zeile 4954)
2. Sell scheitert mit `Custom(6003)` → `ExecutionStatus::Failed` Handler (Zeile 3209-3227) behandelt nur Buy-Failures mit Tracker-Reject, aber **resettet `exit_generated` nicht** für Sell-Failures
3. Nächster Strategy-Tick: `check_for_exits()` iteriert über Positionen → `if pos.exit_generated { continue; }` (Zeile 2677) **überspringt die Position**
4. Einziger verbleibender Retry-Pfad: `reconcile_timed_exits()` (Zeile 2752), der aber erfordert:
   - `hold_secs >= max_hold_time_secs` (erst nach Ablauf der maximalen Haltezeit)
   - Hard-coded `TIME_EXIT` als Exit-Typ
   - 60-Sekunden-Cooldown zwischen Retries

**Warum andere Tokens retried haben (`64HemTH7`, `34c3bPRz`)**:
- Diese Tokens haben vermutlich `max_hold_time_secs` überschritten → wurden von `reconcile_timed_exits()` erfasst
- Oder es gab Partial-Fills (Zeile 3156-3161), die `exit_generated = false` setzen — der einzige Pfad der Sell-Retries ermöglicht

**Betroffene Code-Stellen**:
- `mark_exit_generated()`: Zeile 2866 — setzt `exit_generated = true`
- `check_for_exits()`: Zeile 2677 — überspringt wenn `exit_generated == true`
- `ExecutionStatus::Failed` Handler: Zeile 3209-3227 — **kein Reset** für Sell-Failures
- Partial-Fill Reset: Zeile 3156-3161 — **einziger Pfad** der `exit_generated = false` setzt
- `reconcile_timed_exits()`: Zeile 2752 — Fallback-Retry, aber nur für TIME_EXIT nach max_hold

**Fix-Vorschlag**:
```rust
ExecutionStatus::Failed => {
    warn!(..., "❌ Execution FAILED");
    if pending.side == TradeSide::Buy {
        // ... existing buy failure handling
    } else if pending.side == TradeSide::Sell {
        // Reset exit_generated to allow retry on next tick
        let mut positions = self.positions.write();
        if let Some(pos) = positions.get_mut(&pending.mint) {
            pos.exit_generated = false;
            pos.exit_generated_at = None;
            warn!(mint = %pending.mint, "Reset exit_generated after sell failure - will retry");
        }
    }
}
```

**Status**: ❌ OFFEN — Root Cause identifiziert, Fix ausstehend

### BUG-D: Falscher Creator im LivePoolCache
**Schweregrad**: MITTEL
**Betroffene Tokens** (2026-02-13 Run): `64HemTH7`, `34c3bPRz`
**Symptom**: Der Creator der beim Buy gespeichert wurde (aus `token_tracker.dev_wallet`) unterscheidet sich vom Creator der bei der Liquidation verwendet wurde (aus `LivePoolCache`/RPC-Fallback).
**Analyse**:
- Momentum-Bot speichert Creator aus dem `TokenTracker` (stammt vermutlich aus dem initialen Discovery-Event)
- Liquidation lädt Creator frisch per RPC (seit FIX-13)
- Der RPC-geladene Creator hat funktioniert (Liquidation erfolgreich)
- Der im Momentum-Bot gespeicherte Creator hat nicht funktioniert (alle Sells gescheitert)
- Mögliche Ursachen:
  1. `dev_wallet` im TokenTracker wird aus einem falschen Feld des Discovery-Events befüllt
  2. Cache-Corruption bei PumpFun Bonding-Curve Account-Updates
  3. Race Condition: Creator wird aus einem Event überschrieben, das nicht den echten Creator enthält
**Status**: ❌ OFFEN — Ursachenanalyse erforderlich
**Nächster Schritt**: Prüfen wie `dev_wallet` in TokenTracker gesetzt wird. Vergleich der Creator-Werte zwischen Buy-Zeit und Liquidations-Zeit.

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
| Audit-I | PumpFun SELL stale Quote für migrierte Tokens | ❌ OFFEN | Verknüpft mit BUG-A |

---

## 4. VERLORENE ÄNDERUNGEN DURCH REVERT (Cherry-Pick Status)

| Priorität | Beschreibung | Status |
|-----------|-------------|--------|
| P1 | PumpSwap AMM Geyser-First Integration | ❌ FEHLT |
| P1 | PumpFun SELL migrierte Tokens → `Ok(None)` | ❌ FEHLT |
| P1 | `emit_sim_failed_decision()` → `Err` für Retry | ❌ FEHLT |
| P2 | Creator-Handling & DEX-Normalisierung | ❌ FEHLT |
| P2 | Market-Data WSOL-Seeding & Pool-Propagation | ⚠️ TEILWEISE (FIX-16) |
| P2 | TX-Builder Cache-capped min_out | ❌ FEHLT |
| P3 | `available_trading_capital_lamports` Metrik | ❌ FEHLT |
