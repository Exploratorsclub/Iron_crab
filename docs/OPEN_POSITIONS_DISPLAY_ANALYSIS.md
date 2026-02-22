# Open Positions Display-Analyse (Grafana Dashboard)

## Beobachtung

- **Wallet**: Nur WSOL-ATA + 1 anderer Token
- **Erwartung**: Open Positions = 1 (nur der eine Trade-Token)
- **Grafana**: Zeigt 2 (oder WSOL wird fälschlich mitgezählt)

## Code-Review: Wird WSOL ausgeschlossen?

### 1. Bootstrap: `bootstrap_open_positions_from_wallet_snapshot`

**Datei**: `src/bin/execution_engine.rs` ~3746–3755

```rust
// FIX-36: Skip SOL/WSOL — they're the quote currency, not tradeable positions
if mint == SOL_MINT
    || mint == "NATIVE_SOL"
    || mint == "11111111111111111111111111111111"
{
    continue;
}
```

- `SOL_MINT` = `"So11111111111111111111111111111111111111112"` (identisch zu `WSOL_MINT`)
- WSOL-Snapshots aus market-data nutzen genau diese Mint-Adresse
- **→ WSOL wird im Bootstrap nicht mitgezählt**

### 2. Balance-Transitions (JetStream-Loop)

**Datei**: `src/bin/execution_engine.rs` ~5627–5681

```rust
if mint == "NATIVE_SOL" { /* LockManager only */ }
else if mint == WSOL_MINT || mint == SOL_MINT { /* LockManager only */ }
else {
    // Regular token: open_positions tracking
    if old > 0 && *balance_raw == 0 { fetch_sub(1); }
    else if old == 0 && *balance_raw > 0 { fetch_add(1); }
}
```

- WSOL-Updates landen im zweiten Branch: nur LockManager-Update, kein `open_positions`
- **→ WSOL wird auch bei Live-Updates nie als Position gezählt**

### 3. Bestätigte Trades (BUY/SELL)

- BUY: `fetch_add(1)` nur für echte Token-Käufe
- SELL: `fetch_sub(1)` nur bei bestätigtem Verkauf
- WSOL-Wrap ist kein BUY-Intent → hier kein `fetch_add`
- **→ WSOL wird bei Trades nicht mitgezählt**

## Mögliche Ursachen für Anzeige 2

### A) Veralteter persisted State (am wahrscheinlichsten)

1. execution-engine startet vor oder gleichzeitig mit market-data
2. Keine Wallet-Snapshots in JetStream → Bootstrap liefert `None`
3. Es wird `initial_positions` aus dem Snapshot-File verwendet
4. Wenn dort ein veralteter Wert steht (z.B. 2 von einem früheren Run), bleibt 2 erhalten

**Relevanz**: Ohne `After=market-data.service` kann execution-engine vor dem ersten Publish starten.

### B) WSOL-Filter trifft nicht (theoretisch)

- Wenn der `mint`-String aus dem Snapshot abweicht (anderes Encoding, anderer Key), würde der `mint == SOL_MINT` Check fehlschlagen
- `SOL_MINT` und `WSOL_MINT` sind in allen relevanten Quellen identisch (`"So11111111111111111111111111111111111111112"`)
- **Wahrscheinlichkeit gering**, aber prüfenswert durch Logging bei geskippten Mints

### C) Doppelte Zählung über verschiedene Pfade

- Bootstrap setzt `open_positions = 1` (korrekt)
- Später: Balance-Transition für den echten Token (0 → non-zero) → `fetch_add(1)` → 2
- Ursache: Bootstrap hat den Token schon als „Position“ gezählt, LockManager hatte noch keinen Eintrag
- Beim ersten Snapshot für den Token: `old = 0`, `balance_raw > 0` → erneutes `fetch_add(1)`

**→ Race zwischen Bootstrap und Balance-Transition.**

Bootstrap schreibt `open_positions` direkt aus `mints.len()`, schreibt aber **nicht** die zugehörigen Balances in den LockManager. `bootstrap_token_balances_from_wallet_snapshot` macht das separat. Wenn die Reihenfolge so ist:

1. `bootstrap_open_positions_from_wallet_snapshot` → `open_positions = 1`
2. `bootstrap_token_balances_from_wallet_snapshot` → setzt Balance für TokenA in LockManager
3. Main Loop: WalletSnapshot für TokenA kommt (evtl. erneut, Geyser-Nachricht) – `old` ist bereits > 0 durch Bootstrap
4. `old != balance_raw` → nur Info-Log, kein `fetch_add`

Damit wäre eine Doppelerhöhung nicht erklärt, außer es gibt einen Pfad, bei dem der Token zweimal mit 0→non-zero behandelt wird.

### D) Persistierter Snapshot speichert falschen Wert

- Snapshot wird periodisch mit `ctx.open_positions` geschrieben
- Ein früherer Bug oder ein seltener Race hat `open_positions = 2` persistiert
- Nach Restart: Bootstrap liefert `None` → es bleibt 2

## Empfehlungen

1. **Startreihenfolge prüfen**  
   `execution-engine.service`: `After=market-data.service` sicherstellen (siehe `docs/WSOL_DISPLAY_AND_MANAGER_FIX_PLAN.md`), damit Bootstrap aus JetStream Daten bekommt.

2. **Zusätzliche Absicherung im Bootstrap**  
   `WSOL_MINT` explizit in der Skip-Liste ergänzen (redundant zu `SOL_MINT`, aber dokumentiert).

3. **Diagnose-Logging**  
   Beim Überspringen von WSOL/SOL einen Debug-Log schreiben (nur für Analyse, kann später entfernt werden).

4. **Bootstrap vs. persistierter State**  
   Wenn Bootstrap `None` zurückgibt und nur alter persisted State genutzt wird: klar loggen, dass „open_positions aus JetStream-Bootstrap nicht verfügbar, verwende persisted open_positions = X“.

5. **Grafana-Metrik verifizieren**  
   Prüfen, ob die Prometheus-Metrik `open_positions` nur von `ironcrab-execution-engine` kommt und nicht von anderen Jobs überdeckt wird.
