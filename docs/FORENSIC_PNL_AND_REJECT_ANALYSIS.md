# Forensische Analyse: PnL-Inversion, Rejects, und TIME_EXIT

**Datum**: 2026-02-21  
**Basis**: Server-Logs (decision_records, execution_results), Code-Review

---

## 1. Reject-Gründe (decision_records)

| primary_reject_reason | Bedeutung | Häufigkeit (Beobachtung) |
|----------------------|-----------|---------------------------|
| **KILL_SWITCH_ACTIVE** | Kill-Switch war aktiv, alle buys blockiert | Sehr häufig (Streak) |
| **LOCK_RESOURCE_CONFLICT** | Pool oder Mint bereits durch anderen Intent gesperrt | Häufig (parallele Exits) |
| **UiTransactionError(0, Custom(2))** | Invalid Mint (TokenzQ-Program, z.B. falscher Mint/ATA) | Mehrfach |
| **UiTransactionError(2, Custom(11))** | CloseAccount: „Non-native account can only be closed if its balance is zero“ | Bei einigen SELLs – Ursache unklar (sell_balance_hint vs. tatsächlicher Verbrauch?) |
| **UNSUPPORTED_INTENT** | Bonding curve completed → PumpSwap AMM nötig | Bei migrierten Pools |
| **AccountNotInitialized (3012)** | user_token_out beim Swap nicht initialisiert | Selten |

### Konkrete Log-Zitate

```
"primary_reject_reason":"KILL_SWITCH_ACTIVE"
"primary_reject_reason":"LOCK_RESOURCE_CONFLICT","details":"pool locked by int-390418ca-000006"
"primary_reject_reason":"UiTransactionError(InstructionError(2, Custom(11)))"
"Error: Non-native account can only be closed if its balance is zero"
```

### Empfehlung Rejects

1. **KILL_SWITCH**: Erwartbar, wenn manuell gesetzt. Kontrolle über Control-Plane prüfen.
2. **LOCK_RESOURCE_CONFLICT**: Reduzieren durch weniger parallele Exits oder bessere Intent-Serialisierung.
3. **Custom(11) CloseAccount**: Execution-Engine baut SELL-TX mit CloseAccount für ATA, obwohl nach Transfer noch Restbestand bleibt (teilverkauf, Rundung, Fees). → TX-Plan prüfen: CloseAccount nur bei vollständigem Verkauf.
4. **Custom(2) Invalid Mint**: TokenzQ-ATA für falschen Mint oder abweichendes Token-Program.

---

## 2. Forensische PnL-Analyse (DiodVEgfP71rwV8Fa6gmtf1TsQ7TsPWwiGd5GiVFpump)

### Execution-Records (execution_results)

**BUY (Probe)** – exe-db8a43df-001731:
- `fill_in`: 0.001201771 SOL (eingesetzt)
- `fill_out`: 18 797 Tokens (erhalten)
- Entry-Preis (tokens_per_sol): 18 797 / 0.001201771 ≈ **15 650 000**

**SELL (TAKE_PROFIT)** – exe-db8a43df-001732:
- `reason_detail`: "Take profit hit: +173.0% gain (target: +30.0%)"
- `fill_in`: 18 797 Tokens
- `fill_out`: 0.001172096 SOL
- Verkaufspreis (tokens_per_sol): 18 797 / 0.001172096 ≈ **16 039 000**

### Tatsächlicher PnL

- Kosten (BUY): 0.001201771 SOL
- Erlös (SELL): 0.001172096 SOL  
→ **Verlust ≈ 2,5 %** (0.001172096 < 0.001201771)

Der Bot meldete **+173 % Gewinn**, der tatsächliche Trade war **≈ -2,5 % Verlust**.

### Korrekte PnL-Formel (tokens_per_sol)

- Höhere `tokens_per_sol` = günstigerer Token (mehr Tokens pro SOL).
- Einstieg: 15,65M tps, aktuell: 16,04M tps → Token wurde günstiger → Verlust.
- Korrekt: `pnl_pct = (entry/current - 1) * 100 = (15,65/16,04 - 1) * 100 ≈ -2,4 %`.

### Warum zeigt der Bot +173 %?

Für `(entry/current - 1) * 100 = 173` braucht es `entry/current ≈ 2,73`, also `current ≈ entry / 2,73`.

- Mit `entry ≈ 15,65M` folgt `current ≈ 5,73M`.

**Interpretation**: Der Bot verwendet eine `current_price`, die ca. 2,73× kleiner ist als der tatsächliche Verkaufspreis. Das spricht für:

1. **Falsche Semantik**: `current_price` könnte `sol_per_token` oder eine andere Einheit sein, während die Formel `tokens_per_sol` erwartet.
2. **Base/Quote vertauscht in PoolCacheUpdate**: Statt `tokens_per_sol = base_ui/quote_ui` wird fälschlich `quote_ui/base_ui` verwendet.
3. **Veralteter/anderer Pool**: `current_price` stammt von einem anderen oder sehr alten Pool-Update.
4. **Stark verzögerte Updates**: `current_price` spiegelt einen viel besseren Kurs als zum Ausführungszeitpunkt wider.

### Weitere bestätigte Fälle (5WgxiiQKrPinyenHgdx15uLZVK1T2LYXwpVjZ18xpump)

- TAKE_PROFIT "+173 %": Verlust ≈ 2,5 %
- TAKE_PROFIT "+60,2 %": Verlust ≈ 2,5 %

Das Muster passt zur Hypothese: **Die Exit-Signale zeigen massiv falsche PnL-Werte im Vergleich zum tatsächlichen Trade.**

---

## 3. FIX-PNL und aktuelle Formel

Aus `docs/BUGS_FIXES.md`:

- **Alt (falsch)**: `((current - entry) / entry) * 100` (für sol_per_token)
- **Neu (FIX-PNL)**: `((entry / current) - 1) * 100` (für tokens_per_sol)

Die Formel ist für `tokens_per_sol` korrekt. Das Problem liegt eher bei den **Eingabewerten** `entry_price` und `current_price`:

- `entry_price` kommt aus BUY-Fills (tok_ui / sol_ui) – entspricht `tokens_per_sol` ✓
- `current_price` kommt aus PoolCacheUpdate (`base_ui/quote_ui`) oder Trade-Events

Wenn `current_price` z.B. aus vertauschtem base/quote berechnet wird, entstehen fehlerhafte, oft invertiert wirkende Signale.

---

## 4. PoolCacheUpdate-Semantik (alle DEXe)

| DEX | base_reserve | quote_reserve | tokens_per_sol |
|-----|--------------|---------------|----------------|
| PumpFun | virtual_token_reserves | virtual_sol_reserves | base/quote ✓ |
| PumpAmm | base_reserve (token) | quote_reserve (SOL) | base/quote ✓ |
| Raydium | coin_reserve (token) | pc_reserve (SOL) | base/quote ✓ |
| Orca | vault_a | vault_b | hängt von token_mint_a/b ab |
| Meteora | reserve_x | reserve_y | hängt von token_x/y ab |

Die `update.base_mint` bestimmt, welche Position aktualisiert wird; base_reserve gehört zu base_mint.

---

## 5. TIME_EXIT-Häufigkeit

- `max_hold_time_secs = 300` → jede Position läuft nach 5 Minuten in TIME_EXIT.
- Reconcile läuft alle 15 s und setzt bei `hold_secs >= max_hold_time` immer `exit_type = "TIME_EXIT"`.
- Wenn STOP_LOSS oder TAKE_PROFIT zuerst fehlschlagen, übernimmt Reconcile und schickt TIME_EXIT.

**Konsequenz**: Bei kurzem `max_hold_time` wird TIME_EXIT relativ oft ausgelöst, auch wenn der Kurs eigentlich STOP_LOSS oder TAKE_PROFIT verdient hätte.

**Vorschlag**: `max_hold_time_secs` erhöhen (z.B. 900–1800 s), damit TIME_EXIT eher für „stale Pools“ genutzt wird.

---

## 6. Diagnose-Logging (bereits eingebaut)

In `should_exit()`:

- Bei TAKE_PROFIT: `info!(mint, entry_price, current_price, pnl_pct, "TAKE_PROFIT trigger")`
- Bei STOP_LOSS: `info!(mint, entry_price, current_price, pnl_pct, "STOP_LOSS trigger")`

Die Werte erscheinen in den normalen Logs bei jedem Auslöser.

---

## 7. Nächste Schritte (teilweise umgesetzt)

- [x] **Diagnose**: info-Level-Logging für entry_price/current_price/pnl_pct bei TAKE_PROFIT/STOP_LOSS
- [x] **PoolCacheUpdate**: Base/Quote-Semantik für alle DEXe dokumentiert
- [ ] **CloseAccount Custom(11)**: Ursache klären — ATAs müssen weiterhin geschlossen werden (Kapital-Rückgewinnung)
- [ ] **Reconcile**: Bei Reconcile-Exits `should_exit()` erneut aufrufen und echten Exit-Typ verwenden
- [ ] **max_hold_time**: Erhöhen (z.B. 900–1800 s) für weniger TIME_EXIT bei normalen Kurssituationen

---

## Referenzen

- `trade_logs/decisions/decision_records*.jsonl`
- `trade_logs/executions/execution_results*.jsonl`
- `docs/BUGS_FIXES.md` – FIX-PNL, FIX-17
- `src/bin/momentum_bot.rs` – `pnl_pct()`, `should_exit()`, `update_position_price()`
- `scripts/trades_server.py` – PnL aus ExecutionResult
