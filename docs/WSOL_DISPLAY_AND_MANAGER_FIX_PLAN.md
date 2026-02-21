# WSOL-Anzeige & WsolManager Fix-Plan (2026-02-21)

## Problemzusammenfassung

Nach Liquidation (Killswitch + liquidate) und Neustart:
1. **WSOL-Anzeige zeigt 1.0** obwohl keine WSOL-ATA existiert
2. **WsolManager wickelt nicht** – keine WSOL-Bereitstellung
3. **Momentum-Bot Intents werden rejected** – kein verfügbares Handelskapital

## Root-Cause-Analyse

### A) Herkunft der falschen "1 WSOL"

| Quelle | Ursache |
|--------|---------|
| **LockManager.available_trading_capital()** | Bei `wsol_initialized=false` wird `available_sol` zurückgegeben (Fallback) |
| **initial_sol_lamports** | execution-engine Default = 1_000_000_000 (1 SOL) bei fehlendem Bootstrap |
| **JetStream (DeliverPolicy: LastPerSubject)** | Stale WSOL-Eintrag aus früherem Run – wenn keine WSOL=0 publiziert wurde, bleibt alter Wert (z.B. 1 SOL) |
| **market-data Bootstrap** | Bei fehlender WSOL-ATA: `bootstrap_wsol_balance = None` → publiziert `wsol_lamports: None` und **kein** WSOL-Snapshot nach JetStream |

### B) Warum WsolManager nicht wickelt

1. **Kein WalletBalanceUpdate mit wsol_lamports: Some(0)** wenn ATA geschlossen
   - Geyser sendet keine Updates für gelöschte Konten
   - ExecutionResult-Pfad publiziert WalletBalanceSnapshot(0) zu JetStream/Core NATS, aber **nicht** WalletBalanceUpdate zu `wallet_balance_topic`
   - WsolManager subscribed nur `wallet_balance_topic`

2. **Bootstrap mit wsol_lamports: None**
   - WsolManager aktualisiert `wsol_balance` nur bei `Some(x)` – bei `None` bleibt alter Wert
   - Wenn vorher 1 SOL in JetStream: execution-engine liest es, LockManager zeigt 1
   - WsolManager: `sol_balance` kommt von WalletBalanceUpdate, `wsol_balance` wird nicht aktualisiert

3. **Startreihenfolge-Race**
   - execution-engine hat **kein** After=market-data.service
   - Beide starten parallel → execution-engine bootstrapt evtl. vor market-data Publish
   - execution-engine bleibt bei initial_sol=1e9 bis JetStream-Bootstrap

### C) Warum "Available WSOL" Fallback falsch ist

- Konzeptionell: "Available WSOL" soll **nur** WSOL zeigen
- Bei `wsol_initialized=false` wird native SOL angezeigt → irreführend
- Korrekt: Immer `available_wsol` anzeigen; 0 wenn nicht initialisiert

---

## Fix-Plan (6 Änderungen)

### Fix 1: LockManager – "Available WSOL" immer echtes WSOL

**Datei:** `src/storage/locks.rs`

- `available_trading_capital()` unverändert (wird für Capital-Lock-Checks genutzt – BUY braucht Kapital, Fallback auf SOL ist für die Lock-Logik ok)
- **Neue Methode** `available_wsol_for_display()` oder klare Trennung: Metrik "Available WSOL" nutzt ausschließlich `available_wsol()`
- **Änderung in execution_engine.rs:** `AVAILABLE_SOL_LAMPORTS` für die Grafana-"Available WSOL"-Metrik: Immer `lock_manager.available_wsol()` schreiben, **nicht** `available_trading_capital()`

**Ergebnis:** Anzeige "Available WSOL" = 0 wenn keine WSOL-ATA existiert.

---

### Fix 2: market-data Bootstrap – WSOL immer publizieren (auch 0)

**Datei:** `src/bin/market_data.rs`

- `wsol_lamports` beim Bootstrap: `Some(bootstrap_wsol_balance.unwrap_or(0))` statt `bootstrap_wsol_balance`
- Wenn getTokenAccountsByOwner keine WSOL-ATA findet → explizit `Some(0)` senden
- JetStream: WSOL-Snapshot **immer** publizieren (auch bei balance_raw=0), nicht nur `if let Some(wsol_bal)`

**Ergebnis:** Nach Neustart mit geschlossener WSOL-ATA: WalletBalanceUpdate(sol, Some(0)) und JetStream WSOL=0.

---

### Fix 3: ExecutionResult SELL – WalletBalanceUpdate bei WSOL-ATA Close

**Datei:** `src/bin/market_data.rs`

- Beim bestätigten SELL: Wenn `mint_str == WSOL_MINT` (oder ATA war WSOL), zusätzlich WalletBalanceUpdate(sol, Some(0)) zu `wallet_balance_topic` publizieren
- So erfahren WsolManager und LockManager sofort, dass WSOL = 0

**Ergebnis:** Sofortige Benachrichtigung bei WSOL-ATA-Close, WsolManager kann Wrap auslösen.

---

### Fix 4: execution-engine – Metrik "Available WSOL" = available_wsol

**Dateien:** `src/bin/execution_engine.rs`, evtl. `src/metrics.rs`

- Überall wo `AVAILABLE_SOL_LAMPORTS` für die "Available WSOL"-Anzeige gesetzt wird: `lock_manager.available_wsol()` statt `available_trading_capital()` verwenden
- Grafana-Query bleibt `available_sol_lamports` – Wert ist nun immer echtes WSOL
- `available_trading_capital()` weiterhin für Capital-Lock-Checks (BUY) nutzen – dort ist SOL-Fallback sinnvoll

**Hinweis:** Die Metrik heißt `available_sol_lamports` (Grafana "Available WSOL"). Semantik: Dieses Gauge zeigt ab sofort nur noch WSOL, nie native SOL.

---

### Fix 5: Startreihenfolge – market-data vor execution-engine

**Dateien:** `docs/systemd/execution-engine.service`, `deploy_new.sh`

- `execution-engine.service`: `After=network-online.target nats.service market-data.service` hinzufügen
- Optional in deploy_new.sh: explizit `systemctl start market-data` vor dem Rest, oder `Requires=market-data.service` (stärkere Kopplung)
- Empfehlung: `After=market-data.service` reicht – systemd startet market-data zuerst, dann execution-engine

**Ergebnis:** market-data hat Zeit zum Bootstrap und Publish, bevor execution-engine JetStream liest und LockManager initialisiert.

---

### Fix 6: execution-engine Bootstrap – WSOL=0 aus JetStream korrekt anwenden

**Datei:** `src/bin/execution_engine.rs`

- In `bootstrap_token_balances_from_wallet_snapshot`: Wenn kein WSOL-Snapshot in JetStream, aber NATIVE_SOL vorhanden: `bootstrap_wsol = Some(0)` setzen (nicht None)
- Oder: market-data publiziert dank Fix 2 immer WSOL (auch 0), daher sollte JetStream einen WSOL-Eintrag haben
- Zusätzlich: Bei `update_wallet_balances(sol, None)` wenn sol von NATIVE_SOL kommt und wir nie WSOL gesehen haben: explizit `update_wallet_balances(sol, Some(0))` aufrufen – so wird `wsol_initialized=true` und `available_wsol=0`

**Vereinfachung:** Da Fix 2 sicherstellt, dass market-data immer WSOL (auch 0) publiziert, sollte execution-engine von JetStream bootstrap_wsol = Some(0) oder den echten Wert bekommen. Fallback in execution-engine: Wenn NATIVE_SOL da ist, aber kein WSOL-Eintrag – `wsol = Some(0)` annehmen (konservativ).

---

## Zusammenfassung der Dateiänderungen

| Datei | Änderung |
|-------|----------|
| `src/storage/locks.rs` | Keine – Logik bleibt |
| `src/bin/execution_engine.rs` | AVAILABLE_SOL_LAMPORTS aus `available_wsol()` statt `available_trading_capital()`; ggf. Bootstrap-Fallback für WSOL=0 |
| `src/bin/market_data.rs` | Bootstrap: wsol_lamports = Some(balance.unwrap_or(0)); JetStream WSOL immer publizieren; ExecutionResult SELL: bei WSOL-ATA Close WalletBalanceUpdate(sol, Some(0)) |
| `docs/systemd/execution-engine.service` | After=market-data.service |

---

## Reihenfolge der Implementierung

1. Fix 2 (market-data Bootstrap) – Basis für korrekte Daten
2. Fix 3 (ExecutionResult WalletBalanceUpdate) – Runtime-Benachrichtigung
3. Fix 4 (execution-engine Metrik) – richtige Anzeige
4. Fix 5 (systemd) – Startreihenfolge
5. Fix 6 (Bootstrap-Fallback) – falls JetStream leer/nicht aktuell
