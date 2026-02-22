# Analyse: TAKE_PROFIT mit Verlust + TIMED_EXIT statt STOP_LOSS

## 1. Warum TAKE_PROFIT-Trades im Dashboard Minus zeigen

### Ursache: Zwei verschiedene PnL-Berechnungen

| Ort | Quelle | Zeitpunkt |
|-----|--------|-----------|
| **momentum_bot** (Exit-Grund) | `pnl_pct()` aus Live-Pool-Preisen (Geyser/cache) | **Vor** der Transaktion |
| **trades_server** (Dashboard PnL) | `proceeds_sol - sold_cost` aus ExecutionResult | **Nach** der Transaktion |

Der **Exit-Grund** „TAKE_PROFIT“ stammt aus der Sicht des Bots **vor** dem Verkauf. Der **Dashboard-PnL** basiert auf dem tatsächlich gelandeten Trade.

### Warum der tatsächliche PnL negativ sein kann

1. **Preis-Gap zwischen Quote und Execution**
   - Der Bot sieht z.B. +235 % aus dem Cache und löst TAKE_PROFIT aus.
   - Bis die TX bestätigt ist, kann der Preis stark gefallen sein (Slippage, Frontrunning, illiquider Markt).

2. **Probe+Scale Cost-Basis**
   - Bei mehreren BUYs (Probe + Scale-In) baut der trades_server `avg_cost` aus allen BUYs.
   - Wenn `wallet_sol_delta` oder Reihenfolge/Beträge falsch sind, wird `sold_cost` zu hoch → PnL zu niedrig/negativ.

3. **wallet_sol_delta bei PumpSwap-SELL**
   - Output ist WSOL, `wallet_sol_delta` misst native SOL.
   - Wenn Tracking/Unwrap falsch ist, können die Proceeds unterschätzt werden.

### Konkreter Fall aus dem Screenshot

- **Detail:** „Take profit hit: +235.9% gain (target: +30.0“
- **Dashboard PnL:** -1,07 %
- **Interpretation:** Der Bot hat bei +235,9 % (Cache) Exit ausgelöst, der tatsächliche Verkauf brachte jedoch weniger ein als die Cost-Basis.

---

## 2. Warum Trades mit großem Verlust TIMED_EXIT haben statt STOP_LOSS

### Reconcile-Pfad erzwingt immer TIME_EXIT

`reconcile_timed_exits()` läuft alle 15 Sekunden und verkauft Positionen mit `hold_secs >= max_hold_time_secs`. Dabei wird **immer** `exit_type = "TIME_EXIT"` gesetzt – unabhängig von PnL oder ursprünglichem Exit-Grund:

```rust
// momentum_bot.rs ~Z.3104
generate_and_publish_exit_intent(..., "TIME_EXIT", &reason, ...)
```

### Typische Abläufe für „TIMED_EXIT mit -50 %“

1. **Erst STOP_LOSS versucht, dann Reconcile**
   - STOP_LOSS wird ausgelöst, Intent wird publiziert, Execution schlägt fehl (Simulation, Liquidity, …).
   - Reconcile erkennt „hold >= max_hold“ und sendet erneut einen Exit – diesmal als **TIME_EXIT**.
   - Das Dashboard zeigt den letzten erfolgreichen Verkauf mit `exit_type = TIME_EXIT`, obwohl der erste Versuch STOP_LOSS war.

2. **Exit wurde nie generiert**
   - Preis-Daten waren veraltet (z.B. kein Pool-Cache-Update für inaktiven Token).
   - `current_price ≈ entry_price` → `pnl_pct() ≈ 0` → STOP_LOSS feuert nicht.
   - Bei `hold_secs >= max_hold_time` holt Reconcile die Position mit **TIME_EXIT** nach.

3. **Config: hard_stop_loss_pct**
   - Aktuell: `hard_stop_loss_pct = 15`.
   - Bei -30 % oder -50 % hätte STOP_LOSS eigentlich ausgelöst werden müssen.
   - Dass stattdessen TIMED_EXIT erscheint, spricht für Fall 1 oder 2.

### Reihenfolge der Exits in `should_exit()`

1. STOP_LOSS (zuerst)
2. TAKE_PROFIT
3. Bonding-Curve-Exit
4. Trailing Stop
5. TIME_EXIT (max. Hold-Zeit)

STOP_LOSS hat Vorrang. Wenn trotzdem TIME_EXIT im Dashboard steht, wurde der Verkauf sehr wahrscheinlich über den Reconcile-Pfad ausgelöst (Retry nach vorherigem Fehlschlag oder Exit war nie generiert worden).

---

## 3. Mögliche Anpassungen

### Für TAKE_PROFIT / PnL-Anzeige

- **trades_server**: FIX-39 — SELL proceeds nutzt jetzt value_sol (fill_out) statt wallet_sol_delta; für PumpSwap-SELL ist Output WSOL, wallet_delta enthält nur Rent/Fees.
- **Exit-Logik**: Konservativeres TAKE_PROFIT (z.B. höheres Ziel oder Slippage-Puffer), um Abstand zwischen Quote und Execution zu verringern.
- **Tracking**: Sicherstellen, dass bei PumpSwap-SELL die Proceeds (WSOL/native SOL) korrekt erfasst werden.

### Für TIMED_EXIT / STOP_LOSS

- **Reconcile**: Vor dem Senden eines TIME_EXIT-Intents `should_exit()` erneut aufrufen und, falls STOP_LOSS greift, den Intent mit `exit_type = "STOP_LOSS"` statt `"TIME_EXIT"` senden.
- **Preis-Updates**: Prüfen, ob für „stille“ Token (keine Trades) weiterhin Pool-Reserve-Updates für `current_price` kommen (FIX-30a).
- **Logging**: Bei Reconcile-Exits mit hohem Verlust loggen, ob zuvor ein anderer Exit (z.B. STOP_LOSS) fehlgeschlagen ist.

---

## Referenzen

- `docs/PNL_AND_CRASH_DIAGNOSIS.md` – PnL-Inversion
- `docs/BUGS_FIXES.md` – FIX-PNL, FIX-30 (Exit-Logik)
- `src/bin/momentum_bot.rs` – `should_exit`, `reconcile_timed_exits`, `collect_timed_exit_reconcile_candidates`
- `scripts/trades_server.py` – PnL-Berechnung aus ExecutionResult
