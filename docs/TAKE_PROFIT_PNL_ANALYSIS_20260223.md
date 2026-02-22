# Analyse: TAKE_PROFIT falsche Gewinn-Wahrnehmung (22.–23. Feb 2026)

## Symptom

Der **Momentum-Bot** glaubt, er habe +173 % Gewinn und triggert TAKE_PROFIT — tatsächlich ergibt der Verkauf on-chain einen Verlust. Stop-Loss hätte greifen müssen. Das Problem liegt **nicht** im Dashboard (trades_server), sondern in der **internen PnL-Berechnung des Momentum-Bots**.

## Root Cause: Noch ungeklärt

**Hinweis:** Die Annahme „Preis von falschem Pool“ (FIX-43) war falsch:
- Token auf Bonding Curve: Es gibt **keinen** anderen Pool.
- Migrierte Token: Multi-Pool nutzt den besten verfügbaren Pool.

Die eigentliche Ursache für „+173 % gain“ bei realem Verlust muss weiter analysiert werden (entry_price vs. current_price, Bonding-Curve-spezifische Formel, Trade-Event-Konvention, etc.).

---

## Dashboard: Asymmetrische Datenquellen (FIX-42)

**FIX-39** hatte nur die **SELL**-Seite angepasst:

| Leg   | Vor FIX-39                        | Nach FIX-39                 |
|-------|-----------------------------------|-----------------------------|
| SELL  | proceeds = wallet_delta (falsch)  | proceeds = value_sol (fill_out) |
| BUY   | cost = wallet_delta              | cost = wallet_delta *(unverändert)* |

### Warum ist `wallet_delta` für BUY problematisch?

- Beim **BUY** wird typisch **WSOL** (Token) gespendet, nicht native SOL.
- `wallet_sol_delta_lamports` misst nur die native SOL-Änderung.
- Bei PumpFun/PumpSwap-BUY: native SOL ändert sich kaum (Rent für ATA, Fees).
- `wallet_delta` kann daher stark von der tatsächlichen Swap-Summe abweichen (zu klein oder zu groß je nach TX-Struktur).

### Folge

- **BUY cost** basiert auf `wallet_delta` → oft falsch.
- **SELL proceeds** basiert auf `fill_out` → korrekt.
- Asymmetrie → systematische PnL-Fehler.

## Fix (FIX-42)

**BUY cost** nutzt jetzt dieselbe Logik wie SELL proceeds: **value_sol (fill_in)** wird bevorzugt, `wallet_delta` nur als Fallback.

```python
# Vorher:
cost_sol = abs(wallet_delta) if wallet_delta is not None else value_sol

# Nachher:
cost_sol = value_sol if (value_sol is not None and value_sol > 0) else None
if cost_sol is None and wallet_delta is not None:
    cost_sol = abs(wallet_delta)
```

- BUY: `value_sol` = fill_in (tatsächlich für den Swap ausgegebene SOL/WSOL).
- SELL: `value_sol` = fill_out (tatsächlich erhaltene SOL/WSOL).
- Beide Seiten verwenden damit die Fills als primäre Quelle.

## Weitere Erkenntnisse aus Server-Analyse

- `fill_out` ist bei TAKE_PROFIT-SELLs vorhanden (FIX-39 greift).
- Die `positions`-Logik für mehrere BUYs (Probe + Scale-In) ist korrekt.
- Teilverkäufe werden proportional zur durchschnittlichen Cost-Basis berechnet.

## Dateien

- `scripts/trades_server.py`: BUY-Cost-Logik in `read_recent_trades`, `read_trades_by_run`, `compute_pnl_24h`
- `docs/BUGS_FIXES.md`: FIX-42
