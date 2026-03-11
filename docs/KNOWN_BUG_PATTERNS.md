# IronCrab — Known Bug Patterns

**Zweck:** Wiederkehrende Bug-Muster erkennen und vermeiden. Bei ähnlichen Symptomen: zuerst hier prüfen.

**Quelle:** `docs/BUGS_FIXES.md`, Analyse-Docs, Git-Commits (wiederkehrende Fixes)

---

## 1. Wrong-Pool Price Pollution → TAKE_PROFIT bei realem Verlust

| Symptom | TAKE_PROFIT zeigt "+200% gain", tatsächlicher PnL negativ. Oft ~1 Sekunde nach Probe-Buy. |
|---------|--------------------------------------------------------------------------------------------|
| **Root Cause** | Preis-Updates (Trade, PoolCacheUpdate) von **anderem** Pool als Position → `current_price` falsch → `pnl_pct()` fälschlich hoch → TAKE_PROFIT feuert |
| **Fix** | FIX-38: Pool-Matching. `update_position_price()` nur anwenden wenn `source_pool == position.pool`. `take_profit_min_hold_secs` (Default 5s). |
| **Prüfen bei** | Preis-Updates für Positionen, Multi-Pool-Tokens (Bonding Curve + AMM), PnL-Änderungen |

---

## 2. fill_in / fill_out falsch → falsche entry_price

| Symptom | entry_price bis zu 29x falsch. TAKE_PROFIT feuert obwohl Verlust. STOP_LOSS feuert bei Gewinn. |
|---------|-----------------------------------------------------------------------------------------------|
| **Root Cause** | BUY: `fill_in` fiel auf `intent.required_capital` zurück (lamport_noise/ATA-Lifecycle). SELL: `fill_out` war `None` bei ATA-close. |
| **Fix** | FIX-17: Dreistufige Fallback-Kette — Inner-Instruction-Parsing, Rent-Adjusted Lamport Delta, intent capital als letzter Ausweg. |
| **Prüfen bei** | ExecutionResult-Metadata, entry_price-Berechnung, ATA create/close Szenarien |

---

## 3. Invertierte PnL-Formel (tokens_per_sol)

| Symptom | TAKE_PROFIT bei Verlust, STOP_LOSS bei Gewinn. "TAKE_PROFIT +167%" bei real -20% PnL. |
|---------|---------------------------------------------------------------------------------------|
| **Root Cause** | Formeln für `SOL_per_token` verwendet, obwohl intern `tokens_per_sol` (höher = billigerer Token). `highest_price` trackte falsch. |
| **Fix** | FIX-PNL: `pnl_pct = (entry/current - 1)*100`. `highest_price` = `min` (niedrigster tps = bester Preis). |
| **Prüfen bei** | Jede Änderung an `pnl_pct()`, `drawdown_from_ath_pct()`, `update_price()`, `add_investment()` |

---

## 4. RPC im Hot Path

| Symptom | Latenz >1s, Timeouts, Rejects. "Too slow" für Discovery/Buy/Sell. |
|---------|-------------------------------------------------------------------|
| **Root Cause** | RPC-Calls (getProgramAccounts, getMultipleAccounts, getTokenAccountsByOwner) im normalen Trading-Flow. |
| **Fix** | Geyser + LivePoolCache für Hot Path. RPC nur Cold Path (Liquidation, Bootstrap, Manual). FIX-11, FIX-23, FIX-29. |
| **Prüfen bei** | Jeder neue RPC-Call — ist er Hot oder Cold Path? |

---

## 5. Ghost Positions

| Symptom | OPEN_POSITIONS zeigt 8–10 obwohl nur 1–2 echt. Oder 0 obwohl Tokens in Wallet. |
|---------|-------------------------------------------------------------------------------|
| **Root Cause** | Stale JetStream Snapshots (non-zero für verkaufte Tokens). Balance-Transitionen (non-zero→0) wurden nicht gezählt. Owner-Scan ignoriert bei vollem Bootstrap-Cap. |
| **Fix** | FIX-14, FIX-24, FIX-26, FIX-37. Balance-Transitionen in Main-Loop. SELL→zero-balance an JetStream. Owner-Scan umgeht Bootstrap-Cap. |
| **Prüfen bei** | Bootstrap, Wallet-Snapshot-Merge, SELL-Handler mit close_token_ata |

---

## 6. Orphaned Buy (Position nie erstellt)

| Symptom | BUY confirmed, aber keine Position → kein SELL. Token bleibt in Wallet. |
|---------|-----------------------------------------------------------------------|
| **Root Cause** | `cleanup_stale_pending()` entfernte Pending Intent bevor `ExecutionResult` ankam (Race). |
| **Fix** | FIX-18: Orphaned Buy Recovery. Confirmed BUY ohne Pending → Position aus ExecutionResult + TokenTracker rekonstruieren. |
| **Prüfen bei** | Pending-Intent-Cleanup, ExecutionResult-Handler, timing-kritische Flows |

---

## 7. exit_generated nicht zurückgesetzt → kein Sell-Retry

| Symptom | SELL-Intent fehlgeschlagen, aber kein erneuter Versuch bis max_hold_time. |
|---------|--------------------------------------------------------------------------------|
| **Root Cause** | `exit_generated` wurde bei Failed/Timeout nicht zurückgesetzt → Bot denkt Exit wurde gesendet. |
| **Fix** | FIX-19: Unconditional Reset von `exit_generated` in Failed/Timeout für Sell-Side (normal + orphaned). |
| **Prüfen bei** | SELL-Failure-Handler, reconcile_timed_exits, exit lifecycle |

---

## 8. Doppelter JetStream Consumer

| Symptom | Race Conditions, verpasste Updates, Delays. Inkonsistente Pool/Position-Daten. |
|---------|------------------------------------------------------------------------------|
| **Root Cause** | Zwei separate ephemere Consumers für denselben Stream (Bootstrap + Runtime). |
| **Fix** | FIX-12: Einzelner Consumer für Bootstrap und Runtime wiederverwenden. |
| **Prüfen bei** | JetStream-Integration, neue Streams/Consumer |

---

## 9. Hardcoded / nicht-kanonische DEX-Namen

| Symptom | Routing-Fehler, Creator unnötig erzwungen, defensive Multi-Varianten-Checks. |
|---------|-------------------------------------------------------------------------------|
| **Root Cause** | Verschiedene Enums mit unterschiedlichen `to_string()` Outputs. `"raydium_amm_v4"` vs `"raydium"`, `"pump_swap_amm"` vs `"pump_amm"`. |
| **Fix** | FIX-15, FIX-25: Kanonische Namen in `arbitrage/types.rs`. Creator nur für `pumpfun` Pflicht. |
| **Prüfen bei** | DEX-String-Vergleiche, neue DEX-Integration |

---

## 10. min_out zu hoch (PumpFun BUY)

| Symptom | Error 6002 "Too much SOL required" / SlippageExceeded on-chain. |
|---------|---------------------------------------------------------------|
| **Root Cause** | `min_out` aus Intent übernommen ohne Capping. Bonding Curve verschob sich zwischen Intent und TX-Build. |
| **Fix** | FIX-28: Intent + Cache Quote berechnen, Minimum (konservativer) verwenden. |
| **Prüfen bei** | PumpFun BUY, tx_builder, min_out Berechnung |

---

## 11. WSOL / wallet_sol_delta Tracking

| Symptom | PnL falsch, Proceeds unterschätzt. SELL-Output ist WSOL, wallet_delta misst native SOL. |
|---------|--------------------------------------------------------------------------------------|
| **Root Cause** | PumpSwap-SELL Output = WSOL. `wallet_sol_delta` enthält nur Rent/Fees, nicht die eigentlichen Proceeds. |
| **Fix** | trades_server: value_sol (fill_out) statt wallet_sol_delta für SELL. FIX-39 (in BUGS_FIXES). |
| **Prüfen bei** | PnL-Berechnung, PumpFun/PumpSwap SELL, ExecutionResult fill_out |

---

## 12. Stale quote_mint / false Arbitrage

| Symptom | Falsche Arbitrage-Signale für non-SOL Pairs. |
|---------|---------------------------------------------|
| **Root Cause** | `quote_mint = SOL_MINT_PUBKEY` hardcoded in DEX-Parsern. |
| **Fix** | FIX-15: Dynamische quote_mint-Extraktion aus Transaction Token-Balances. |
| **Prüfen bei** | Meteora, Raydium, CPMM Parser |

---

## 13. TAKE_PROFIT Iteration-Loop (mehrfach gefixt)

| Symptom | TAKE_PROFIT wurde 6+ mal gefixt: PnL-Logik, Preis-Quelle, Logging, Revert wegen falscher Root Cause. |
|---------|----------------------------------------------------------------------------------------------------|
| **Root Cause** | TAKE_PROFIT hat viele Abhängigkeiten (entry_price, current_price, pool, fill_out, tokens_per_sol). Fixes adressierten oft nur ein Symptom. |
| **Fix** | Immer INVARIANTS + KNOWN_BUG_PATTERNS prüfen. Root Cause mit Logs verifizieren bevor fixen. Keine „Quick Fixes" ohne Evidenz. FIX-38 (Pool-Matching) war der entscheidende Fix. |
| **Prüfen bei** | Jeder TAKE_PROFIT/PnL-Bug — Pattern 1, 2, 3, 11 zuerst durchgehen |

---

## 14. PumpFun/PumpSwap pool_accounts Account-Count

| Symptom | Panic, Wrong Instruction, Error 6002. „12 vs 14 accounts", „21 vs 23 accounts". |
|---------|--------------------------------------------------------------------------------|
| **Root Cause** | Verschiedene Formate: BUY vs SELL, Bonding Curve vs AMM. `pool_accounts_v1_for_base_mint` ≥14. PumpSwap BUY braucht 23 (mit global_volume_accumulator), SELL 21. |
| **Fix** | FIX-23, 839cb9d2: 12 oder 14 je nach Format. **Guard-Check:** In `parse_pumpfun_amm_transaction()` NICHT `!= 23` prüfen — das blockiert alle SELL-TXs (21 Accounts). Stattdessen: `if instruction_accounts.len() < 21 { return None; }`. Referenz: ee4c938f (Einführung), 049290d8 (teilweiser Fix). |
| **Prüfen bei** | pumpfun_amm, tx_builder, dex_parser, neue PumpFun/PumpSwap-Änderungen |

---

## 15. LockManager Double-Counting / SELL Race

| Symptom | Verfügbares Kapital falsch. SIM_INSUFFICIENT_BALANCE obwohl Balance da. Doppelte Zählung bei BUY-Fill + Balance-Update. |
|---------|-------------------------------------------------------------------------------------------------------------------------|
| **Root Cause** | BUY-Fill und Balance-Transition werden beide gezählt. SELL-Amount Race: LockManager sieht alte Balance. |
| **Fix** | FIX-8: LockManager BUY-Fill-Akkumulation + Live Token-Balance-Sync. ExecutionResult-Metadata für sofortiges LockManager-Seed. |
| **Prüfen bei** | LockManager, ExecutionResult-Handler, try_lock_capital |

---

## 16. Token-2022 / Custom token_program

| Symptom | ATA-Parsing schlägt fehl (>165 bytes), SELL ohne token_program, falsche Decimals, Liquidation ignoriert Token-2022. |
|---------|---------------------------------------------------------------------------------------------------------------------|
| **Root Cause** | Token-2022 hat Account-Extensions. `token_program` muss aus Trade/ExecutionResult kommen, nicht hardcoded. TokenMintInfo kann später als Trade kommen. |
| **Fix** | Geyser post_token_balances für token_program. mint_infos Cache mit Decimals. ATA-Größe für Extensions. SELL-Intent mit token_program. |
| **Prüfen bei** | ATA-Parsing, PumpFun Trade-Events, Liquidation token_program, tx_builder |

---

## 17. WSOL Lifecycle (mehrere Facetten)

| Symptom | WSOL als Position getrackt. WsolManager wrap/unwrap zur falschen Zeit. KillSwitch + WSOL Race. ATA nach Janitor-Close. |
|---------|-----------------------------------------------------------------------------------------------------------------------|
| **Root Cause** | WSOL ≠ tradeable Position. WsolManager braucht WalletBalanceUpdate. Bei KillSwitch kein Wrap. Nach Janitor close ATA muss WsolManager re-enable können. |
| **Fix** | FIX-36: WSOL von tradeable positions ausschließen. FIX-16: Initial WalletBalanceUpdate. 28976bd1: Kein Wrap bei KillSwitch. |
| **Prüfen bei** | Wallet-Snapshot, WsolManager, open_positions, LockManager available_wsol |

---

## 18. Liquidation: Stale Data vs. RPC

| Symptom | SIM_INSUFFICIENT_BALANCE bei Liquidation. Liquidation überspringt Tokens. Falsche Creator/Vault-Daten. |
|---------|--------------------------------------------------------------------------------------------------------|
| **Root Cause** | JetStream/Wallet-Snapshot kann stale sein (bereits verkauft). RPC getTokenAccountsByOwner für autoritativen State. Multi-Pool-Reihenfolge: PumpFun bonding curve zuletzt. |
| **Fix** | FIX-13: RPC-Fallback für Creator (Cold Path). 04f5572a: LockManager mit RPC-Balances seeden. A0ea1c71: Multi-Pool zuerst. |
| **Prüfen bei** | Liquidation flow, LockManager Bootstrap, Pool-Discovery-Reihenfolge |

---

## 19. Fix-Revert-Fix (falsche Root Cause)

| Symptom | Fix deployed, Bug bleibt oder wird schlimmer. Commit „revert … due to incorrect root cause assumption". |
|---------|---------------------------------------------------------------------------------------------------------|
| **Root Cause** | Erster Fix adressierte Symptom, nicht Ursache. Weitere Änderungen bauen auf falscher Annahme auf. |
| **Fix** | Vor dem Fixen: Logs/Runtime-Evidenz sammeln. Hypothese validieren. Bei Unsicherheit: kleiner Fix, deployen, beobachten. Nicht spekulativ „verbessern". |
| **Prüfen bei** | Jeder komplexe Bug — siehe d87b2d4d (TAKE_PROFIT Revert) |

---

## 20. DEX Swap Instruction Account Order

| Symptom | Simulation failed, Custom error. Meteora/Orca/CPMM Swap schlägt fehl. |
|---------|----------------------------------------------------------------------|
| **Root Cause** | Account-Reihenfolge pro DEX anders. Offizielle IDL/Program-Docs müssen konsultiert werden. oracle, event_authority, bin_arrays oft vergessen. |
| **Fix** | 7775b211, aa4b80c3, e264669d: Account-Order per offiziellem IDL. Immer Mainnet-Referenz-TX parsen. |
| **Prüfen bei** | build_swap_ix, neue DEX-Connector, Meteora bin_array_bitmap_extension |

---

## 21. PumpFun Custom(6024) Overflow — Fehlendes bonding_curve_v2 Account

| Symptom | Alle PumpFun Bonding Curve Trades schlagen mit `Custom(6024)` (Overflow) fehl. |
|---------|-------------------------------------------------------------------------------|
| **Root Cause** | PumpFun Cashback-Upgrade (Feb 2026) führte `bonding_curve_v2` PDA als Pflicht-Account ein. Fehlt in build_buy_ix und build_sell_ix. |
| **Fix** | bonding_curve_v2 PDA als letztes Account in build_buy_ix (17 total) und build_sell_ix (15/16 total). Ref: plan_pumpfun_6024_cashback_fix.md |
| **Prüfen bei** | PumpFun BUY/SELL, tx_builder, neue PumpFun-Protokoll-Updates |

---

## 22. FIX-38 Simulation Bypass

| Symptom | Fehlerhafte TX werden on-chain gesendet trotz Simulationsfehler. Custom(2) ATA-Create, Custom(6023) PumpFun SELL. |
|---------|------------------------------------------------------------------------------------------------------------------|
| **Root Cause** | FIX-38 umging Simulation bei "bekannten" transienten Fehlern (State-Lag). Zu aggressiv — sendet fehlerhafte TX. |
| **Fix** | FIX-38 entfernt. Simulation nutzt jetzt "processed" Commitment (aligniert mit Geyser). Kein Bypass mehr. |
| **Prüfen bei** | simulate_transaction, process_intent, Commitment-Konfiguration |

---

## 23. Non-Atomic SOL/WSOL Wallet Updates — Falsche wallet_total_sol_lamports Metrik

| Symptom | 24h Wallet Delta zeigt grossen positiven Wert obwohl alle TX fehlgeschlagen. WSOL Wrap/Unwrap veraendert das scheinbare Delta. |
|---------|--------------------------------------------------------------------------------------------------------------------------------|
| **Root Cause** | Event-Handler fuer NATIVE_SOL und WSOL riefen beide `update_wallet_balances()` auf und ueberschrieben dabei den jeweils anderen Wert mit einem veralteten Snapshot. WSOL-Handler las zudem `total_native_sol()` (inkl. Capital-Locks) und schrieb es als `available_sol` zurueck — Doppelzaehlung bei aktiven Locks. |
| **Fix** | Neue Methoden `update_native_sol_only()` und `update_wsol_only()` in LockManager. Jeder Event-Handler aktualisiert nur seinen eigenen Wert. Grafana-Query nutzt `avg_over_time([5m])` statt Einzelpunkt. |
| **Pruefen bei** | WalletBalanceSnapshot Handler, LockManager Balance-Updates, Grafana Wallet-Metriken |

---

## Quick-Check: Bei neuem Bug

1. Sieht das wie eines der Muster oben?
2. Liegt der betroffene Code im Hot oder Cold Path?
3. Wird ein Pool/Quote/Fill von der richtigen Quelle genutzt?
4. Wird ein State (exit_generated, pending, position) korrekt zurückgesetzt?
5. Sind DEX-Namen/Decimals/Units konsistent?
6. Bin ich sicher in der Root Cause? (Pattern 19: Kein Fix ohne Evidenz)
