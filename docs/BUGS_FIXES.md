# IronCrab — BUGS_FIXES

## FIX-45 Momentum Drawdown from ATH falsch (2025-04-XX)

| Symptom | Trailing Stop zeigt -1986% from ATH bei realen -2.4%, PnL korrekt (-1.2%). |
|---------|--------------------------------------------------------------------------|
| **Root Cause** | `drawdown_from_ath_pct()` nutzte falsche Formel: (entry/current - 1) statt (current/highest - 1). Zudem wurde highest_price als maximaler Preis (höchster tps = teuerst) getrackt, obwohl bei PumpFun niedrigste tps = ATH. |
| **Fix** | `drawdown_from_ath_pct()` nutzt `(current / highest - 1)*100`. `highest_price` tracktet minimalen `tokens_per_sol`. Identisch mit FIX-PNL für pnl_pct(). |
| **Betroffene Module** | momentum-bot, position.rs |
| **Regression-Prüfung** | PnL und Drawdown nutzen dieselbe Preisquelle (tokens_per_sol), Formeln konsistent. |
| **Tags** | [momentum, drawdown, ath, fix] |
