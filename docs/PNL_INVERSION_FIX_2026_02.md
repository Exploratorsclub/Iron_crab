# PnL-Inversion und Exit-Type Fix (2026-02-21)

## Problem

- **STOP_LOSS** zeigt im Dashboard **Gewinn** (z.B. +2.78 %)
- **TAKE_PROFIT** zeigt **Verlust** (z.B. -1.19 %)
- Exit-Typ und tatsächlicher PnL passen nicht zusammen

## Root Cause

1. **Base/Quote-Inversion in PoolCacheUpdate**: Bei manchen DEXen (z.B. Orca/Meteora) kann `base_mint` = SOL sein, sodass `base_reserve/quote_reserve` = SOL/Token statt Token/SOL. Damit wurde `current_price` (tokens_per_sol) invertiert berechnet → `pnl_pct()` im Bot lieferte falsches Vorzeichen.
2. **Reconcile erzwingt TIME_EXIT**: Bei Retry nach fehlgeschlagenem Exit wurde immer `exit_type = "TIME_EXIT"` gesetzt, auch wenn PnL eigentlich STOP_LOSS oder TAKE_PROFIT war.

## Fixes

### 1. Base/Quote-Korrektur (momentum_bot.rs)

- Vor der Preisaktualisierung prüfen: Ist `base_mint` == SOL/WSOL?
- Ja → Quote ist Token: `tokens_per_sol = quote_ui / base_ui`, Position für `quote_mint` aktualisieren.
- Nein → Normal: `tokens_per_sol = base_ui / quote_ui`, Position für `base_mint` aktualisieren.

### 2. Reconcile nutzt should_exit (momentum_bot.rs)

- `reconcile_timed_exits()` ruft vor jedem Exit-Intent `should_exit()` auf.
- Der tatsächliche Exit-Typ (STOP_LOSS, TAKE_PROFIT, TIME_EXIT, …) wird verwendet statt pauschal TIME_EXIT.
- Dashboard zeigt dadurch den korrekten Grund für hohe Verluste/Profite.

### 3. Bootstrap-Diagnostik (execution_engine.rs)

- Wenn der Wallet-Snapshot-Bootstrap `None` zurückgibt (keine JetStream-Daten), wird geloggt, dass `persisted open_positions` verwendet wird.
- Hilft beim Debuggen von Open-Positions-Anzeigen nach frühem Start der execution-engine.

### 4. systemd

- execution-engine.service hat bereits `After=market-data.service`.
- Keine weitere Änderung nötig.

## Referenzen

- `docs/FORENSIC_PNL_AND_REJECT_ANALYSIS.md` – Base/Quote-Hypothese
- `docs/TAKE_PROFIT_AND_TIMED_EXIT_ANALYSIS.md` – Zwei PnL-Quellen
- `src/bin/momentum_bot.rs` – `update_position_price`, `reconcile_timed_exits`
