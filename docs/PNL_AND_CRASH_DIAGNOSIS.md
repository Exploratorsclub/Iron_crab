# Diagnose: PnL-Anzeige invertiert & Momentum-Bot Crashes

## 1. PnL-Anzeige: Stop-Loss = Gewinn, Take-Profit = Verlust

### Symptom
Im Dashboard zeigen STOP_LOSS Trades fälschlich Gewinn und TAKE_PROFIT Trades fälschlich Verlust.

### Bekannter Kontext (BUGS_FIXES.md FIX-PNL)
- **2026-02-14**: In momentum_bot wurde die Exit-Signal-Logik (wann TP/SL feuert) korrigiert — TP feuerte bei Verlusten, SL bei Gewinnen.
- Die **Anzeige** im Dashboard kommt von `scripts/trades_server.py`, nicht von momentum_bot.

### PnL-Berechnung im trades_server

```
pnl_sol = proceeds_sol - sold_cost
sold_cost = avg_cost * amount_tokens  (aus Position: cost_sol / tokens)
proceeds_sol = wallet_delta (wenn > 0) ODER value_sol (fill_out)
```

**Mögliche Ursachen für Inversion:**

1. **Falsche Formel**: Wenn irgendwo `pnl_sol = sold_cost - proceeds_sol` steht → Inversion.
2. **Proceeds vs Cost vertauscht**: Falls für SELL fälschlich `sold_cost` als Ertrag und `proceeds` als Aufwand verwendet wird.
3. **wallet_sol_delta vs value_sol**: Für PumpSwap-SELLs ist output WSOL (nicht native SOL). `wallet_sol_delta` = native SOL nur Rent/Fees, **nicht** Swap-Erlöse. FIX-39: SELL proceeds nutzt jetzt **value_sol (fill_out)** primär, wallet_delta nur als Fallback.
4. **Cost-Basis-Verwechslung**: Bei Probe+Scale könnten BUYs in falscher Reihenfolge oder mit falschen Beträgen zur Position addiert werden → `avg_cost` und `sold_cost` falsch.

### Zu prüfen
- [x] `trades_server.py`: `pnl_sol = proceeds_sol - sold_cost` (korrekt) — FIX-39: proceeds = value_sol (fill_out) für SELL
- [x] Execution-Engine: Für SELL ist `fill_out` = SOL/WSOL empfangen, `fill_in` = Tokens verkauft
- [x] PumpSwap-SELL: value_sol (fill_out) als proceeds, wallet_delta nur Fallback

---

## 2. Momentum-Bot Crashes

### Potenzielle Panic-Stellen (unwrap/expect)

| Datei | Zeile | Risiko |
|-------|-------|--------|
| momentum_bot.rs | 1869 | `pool_list.last_mut().unwrap()` — wenn `pool_list` leer |
| momentum_bot.rs | 2129, 2139, 2259, 2268 | `p.dex_pool_accounts.clone().unwrap()` — wenn `dex_pool_accounts` None |
| momentum_bot.rs | 2239 | `.unwrap()` bei Pool-Parsing |
| momentum_bot.rs | 2153 | `partial_cmp().unwrap_or(...)` — bei NaN/Inf |
| momentum_bot.rs | 4524 | `execs.first().unwrap()` — wenn execs leer (aber vorher geprüft) |

### Wahrscheinlichste Crash-Ursachen

1. **`dex_pool_accounts.is_none()`**  
   In `find_best_sell_pool` und `find_best_buy_pool` wird nur mit `.filter(p.dex_pool_accounts.is_some())` gefiltert. Danach wird `.unwrap()` auf `dex_pool_accounts` aufgerufen. Wenn zwischen Filter und Zugriff eine Race vorkommt oder die Logik sich geändert hat, kann hier panic entstehen.

2. **`pool_list.last_mut().unwrap()`**  
   In `record_trade` wird ein neuer Pool zu `pool_list` gepusht und dann `pool_list.last_mut().unwrap()` verwendet. Theoretisch sicher, aber wenn `push` fehlschlägt oder ein anderer Pfad `pool_list` leer lässt, panic.

3. **LivePoolCache / Quote-Calculator**  
   Wenn `quote_calculator::quote_output_amount` oder Cache-Zugriffe unexpected None/Err liefern und nicht abgefangen werden.

### Empfohlene Maßnahmen
- Alle `.unwrap()` in heißem Pfad durch `ok_or_else()?` oder defensive Checks ersetzen.
- `dex_pool_accounts` vor Nutzung erneut prüfen.
- Systemd/Crash-Logs (`journalctl -u momentum-bot`) und Stacktraces auswerten.

---

## 3. Nächste Schritte

1. **PnL**: Server-Log prüfen — welche `proceeds_sol`, `sold_cost`, `value_sol`, `wallet_delta` werden für invertierte Trades geloggt?
2. **Crash**: `journalctl -u momentum-bot -n 500 --no-pager` nach Panic-/Error-Zeilen durchsuchen.
3. **Reproduktion**: Bei reproduzierbarem Crash Debug-Logs mit Backtrace aktivieren (`RUST_BACKTRACE=1`).
