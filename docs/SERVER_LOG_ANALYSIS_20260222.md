# Server-Log-Analyse ironcrab-prod (22. Feb 2026)

**Zeitraum:** 01.02.2026 – 22.02.2026  
**Quellen:** `journalctl -u execution-engine -u momentum-bot`, `trade_logs/executions/*.jsonl`

---

## 1. Slippage-Fehler (6002, SlippageExceeded, slippage, FailedConfirmed)

### Ergebnis

| Suchbegriff       | Treffer |
|-------------------|---------|
| `6002`            | Nur als Teil von Timestamps (z.B. `.600258Z`) – **keine** Program-Error-0x1772-Treffer |
| `SlippageExceeded`| **0** |
| `slippage`        | Info-Logs (z.B. `slippage_bps=500`) – keine Fehler |
| `FailedConfirmed` | **1** |

### FailedConfirmed (22.02.2026)

```
Feb 22 20:47:25 execution-engine: Intent processed intent_id=int-61db5fd7-000001 decision_id=dec-7e6b4b4b-033188 outcome=FailedConfirmed
```

- **Kontext:** PumpAmm-Pools wurden kurz vorher in LivePoolCache geladen (`pool_accounts populated (was empty)`). Token-Account: `2MABzh9qqkabN6nsY1uzfaJH3nAjqHPccQUw4mvuZSZ4`
- **Interpretation:** TX wurde gesendet und on-chain bestätigt, Programm-Result war jedoch fehlgeschlagen (z.B. Slippage überschritten, Abbruch im Programm). Entspricht der Semantik aus `INTENTS_EXECUTED_AND_WSOL_KILLSWITCH_ANALYSIS.md`: TX ausgeführt, aber nicht als „executed“ gezählt.

### Fazit

- Keine expliziten `SlippageExceeded` (0x1772) in den Logs.
- Eine TX mit `FailedConfirmed` – vermutlich Slippage oder ähnlicher on-chain-Fehler.

---

## 2. Cache-Stale-Warnungen (cache entry is stale, quote_calc, pool not in cache)

### Treffer

| Log-Eintrag | Anzahl |
|-------------|--------|
| `cache entry is stale` | **3** (alle 22.02.) |
| `quote_calc`          | gleich (quote_calculator) |
| `pool not in cache`    | **0** |

### Beispiele (22.02.2026)

```
Feb 22 20:46:36 quote_calculator: quote_calc: cache entry is stale, quote may be inaccurate pool=3D6P46M2D7M7oZPzGqSCvRaUdBFWjqJFubETJVXCKm8H age_ms=26161 slot=402034872
Feb 22 20:46:36 quote_calculator: quote_calc: calculation failed pool=3D6P46M2D7M7oZPzGqSCvRaUdBFWjqJFubETJVXCKm8H dex=meteora_dlmm error=meteora: missing reserves (in=19892667585, out=0)
Feb 22 21:16:48 quote_calculator: quote_calc: cache entry is stale ... pool=B746Ci36ybJtvUhmzRAS2iTawbaqmHcWT7oXtaxRbBZc age_ms=5166
Feb 22 21:16:49 quote_calculator: quote_calc: cache entry is stale ... pool=B746Ci36ybJtvUhmzRAS2iTawbaqmHcWT7oXtaxRbBZc age_ms=5888
```

### Beobachtungen

- Meteora-DLMM-Pool `3D6P46M2...`: Cache-Eintrag ca. 26 s alt, Quote-Berechnung fehlgeschlagen (fehlende Reserves: `out=0`). **→ FIX-41**: BalanceUpdated partielle Updates mergen jetzt mit bestehendem Cache statt die andere Reserve mit 0 zu überschreiben.
- Pumpfun-Pool `B746Ci36...`: Cache 5–6 s alt, Quote potenziell ungenau.

---

## 3. Probe + Take Profit mit Verlust

### Methode

- Python-Skript über alle `execution_results-202602*.jsonl`
- Filter: `EXIT_TAKE_PROFIT` + `SELL` + `wallet_sol_delta_lamports < 0`

### Ergebnis

**Keine TAKE_PROFIT-SELLs mit negativem PnL gefunden.**

- Alle TAKE_PROFIT-Exits im Februar 2026 haben positives `wallet_sol_delta_lamports` (Gewinn).
- Typische Werte: `+0.003–0.005 SOL` pro SELL.

---

## 4. Pool-Cache (LivePoolCache bei TX-Build)

### „pool not in cache“

- **0 Treffer** im analysierten Zeitraum.
- Keine Hinweise darauf, dass Pools bei TX-Build nicht im Cache gefunden wurden.

### „capped intent min_out“

- **6 Treffer** (22.02.2026):

```
tx_plan: capped intent min_out with fresh cache quote intent_min_out=155893 cache_min_out=57639 capped=57639 delta_pct=63.0
tx_plan: capped intent min_out with fresh cache quote intent_min_out=38321623853 cache_min_out=37756844683 capped=37756844683 delta_pct=1.5
tx_plan: capped intent min_out with fresh cache quote intent_min_out=282565 cache_min_out=178101 capped=178101 delta_pct=37.0
tx_plan: capped intent min_out with fresh cache quote intent_min_out=157544 cache_min_out=52926 capped=52926 delta_pct=66.4
tx_plan: capped intent min_out with fresh cache quote intent_min_out=162063 cache_min_out=57390 capped=57390 delta_pct=64.6
tx_plan: capped intent min_out with fresh cache quote intent_min_out=169340 cache_min_out=63840 capped=63840 delta_pct=62.3
```

### Interpretation

- `capped intent min_out`: Intent liefert höheres `min_out` als die aktuelle Cache-Quote. Es wird das niedrigere, cache-basierte `min_out` verwendet → Slippageschutz.
- Hohe `delta_pct` (37–66 %) deuten darauf hin, dass der Intent bei volatilen Pools deutlich optimistischere Erwartungen hatte als der LivePoolCache.
- Ein Fall mit `delta_pct=1.5` – Quote sehr nahe am Intent.
- Pools werden im LivePoolCache gefunden („pool_accounts populated“) – keine Cache-Misses für TX-Build.

---

## 5. Zusammenfassung

| Thema                    | Befund |
|--------------------------|--------|
| Slippage-Errors (6002)   | Keine expliziten SlippageExceeded; 1× FailedConfirmed |
| Cache stale              | 3× quote_calc-Warnungen, 1× Meteora „missing reserves“ |
| pool not in cache        | 0 Treffer |
| Probe+TP mit Verlust     | Keine TAKE_PROFIT-SELLs mit negativem PnL |
| Pool-Cache bei TX-Build  | Pools im Cache; 6× capped intent min_out (teilweise hohe delta_pct) |
