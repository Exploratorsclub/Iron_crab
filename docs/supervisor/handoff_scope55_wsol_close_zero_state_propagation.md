# Handoff: Scope 55 - WSOL close muss Zero-State end-to-end in Dashboard/Metriken propagieren

WICHTIG: Lies und befolge die STOP-CHECK Regeln in `AGENTS.md` und `.cursor/rules/ironcrab-core.mdc` BEVOR du eine Datei aenderst. Wenn eine geplante Aenderung gegen eine Regel verstoesst, STOPPE sofort und melde den Verstoss statt die Aenderung durchzufuehren.

## Task-Beschreibung

Nach Merge von Scope 54 funktioniert die Kill-Switch-Liquidation fachlich wieder: die beiden PumpSwap-SELLs wurden erfolgreich durch die Liquidation ausgefuehrt.

Offenes Folgeproblem:

- Nach der erfolgreichen Liquidation wird die WSOL-ATA spaeter korrekt geschlossen / unwrapped.
- Trotzdem bleibt im Runtime-State und damit im Dashboard `available_wsol` bei ca. `1.000187121 SOL` haengen.
- Dadurch ist auch die Anzeige `SOL + WSOL` um ca. `1 SOL` zu hoch.

Ziel dieses Scopes:

1. Root Cause des stale `available_wsol` / `wallet_total` nach `WSOL ATA close` beheben.
2. Sicherstellen, dass ein erfolgreicher WSOL-Unwrap / ATA-Close **end-to-end** zu `WSOL=0` fuehrt:
   - `market-data` TrackedWallet / WalletSnapshot
   - JetStream `WalletBalanceSnapshot`
   - `execution-engine` LockManager / Metriken / `/status`
   - Dashboard-Anzeige
3. Keine symptomatische UI-Korrektur; der Fix muss am State-Ursprung sitzen.

## Harte Runtime-Evidenz

### A. Die erfolgreichen Sells kamen von der Liquidation, nicht von Momentum

Server-Logs (23.04.2026):

1. `14:10:29`:
   - `KILL_SWITCH_ACTIVATED ... liquidate=True`
   - `Kill switch updated active=true liquidate_positions=true`
   - `Starting liquidation job`
2. Direkt danach wurden zwei Liquidations-Intents gebaut und erfolgreich gesendet:
   - `liquidation-5e0e5590-c58c-483c-9af1-9c0ae6772b16`
   - `liquidation-ee0714e4-d279-4487-a768-60d5c9258b34`
3. Beide wurden spaeter `Confirmed`.
4. Momentum erzeugte zwar kurz danach Exit-Intents:
   - `int-44d53d27-000001`
   - `int-44d53d27-000002`
   aber diese wurden mit `SIM_INSUFFICIENT_BALANCE` abgelehnt.

Schluss:

- Die echten Sells wurden durch die **Liquidation** gemacht.
- Momentum hat nur nachlaufende Exit-Intents erzeugt, nachdem die Token bereits verkauft waren.

### B. WSOL close passiert, aber `available_wsol` bleibt stale

Server-Logs:

1. `14:09:56`:
   - `Triggered WsolManager after kill switch reset (can wrap now) sol=3.282538854 wsol=0.0`
   - `WSOL below minimum, wrapping ... wrap_amount=1.0`
2. `14:10:10`:
   - `Wrapped SOL → WSOL ... amount=1.0`
3. `14:10:12` Heartbeat:
   - `available_wsol=1000000000`
4. Nach den beiden Liquidations-SELLs:
   - `14:11:12` Heartbeat:
     - `available_wsol=1000187121`
     - `native_sol=2276600894`
5. `14:11:20`:
   - `Unwrapped WSOL (closed ATA) ...`
6. Danach bleibt der Wert falsch:
   - `14:11:42` Heartbeat:
     - `available_wsol=1000187121`
   - `14:12:12` Heartbeat:
     - `available_wsol=1000187121`
7. Gleichzeitig steigt `native_sol` nach dem Unwrap wieder an:
   - auf `3282960455`

Schluss:

- Der WSOL-Close / Unwrap ist on-chain erfolgreich.
- Der rueckfliessende SOL-Betrag landet wieder im nativen SOL.
- Aber der WSOL-Wert wird im Runtime-State **nicht auf 0** gesetzt.

## Root-Cause-These

Die staerkste aktuelle Root Cause ist:

1. `execution-engine` sendet den WSOL-ATA-Close und loggt Erfolg, setzt aber den lokalen WSOL-State nicht sofort auf `0`.
2. `market-data` verarbeitet den geschlossenen WSOL-ATA offenbar nicht als `WSOL=0`, sondern laesst den letzten parsebaren WSOL-Balancewert stehen.
3. Dadurch bleibt `LockManager.available_wsol()` stale und speist weiterhin:
   - Heartbeat
   - Prometheus `available_sol_lamports`
   - `wallet_total_sol_lamports`
   - Dashboard

Besonders verdaechtig:

- `market_data.rs` im WSOL-ATA-Geyser-Pfad:
  - Wenn `try_parse_token_account_balance(&account_update.data)` nach dem ATA-Close nichts mehr parsen kann, wird aktuell einfach `continue` gemacht.
  - Genau dann wird also der Zustand `ATA existiert nicht mehr => WSOL=0` vermutlich **nicht** propagiert.

Wichtig:

- Das Problem ist **nicht** primaer ein Frontend-Bug.
- Das Problem ist **nicht** mehr der eigentliche Liquidations-/SELL-Pfad.
- Das Problem ist ein `WSOL close -> zero snapshot / zero balance propagation`-Fehler.

## Relevante Invarianten (Volltext)

### I-4 Geyser-First
HOT PATH (Discovery, Buy, Sell, Monitoring): GEYSER-ONLY. Keine neuen blockierenden RPC-Calls. Wenn ein Fix sowohl Hot als auch Cold Path beruehrt, darf er keinen neuen unbedingten RPC im normalen Trading-Flow einfuehren.

### I-5 Cold Path
COLD PATH (Liquidation, Manual Actions, Bootstrap): RPC erlaubt. Safety und correctness vor Speed. Dieser Scope betrifft aber primaer State-Propagation nach bestaetigtem WSOL-Close und sollte nach Moeglichkeit ohne neue RPC-Abhaengigkeiten auskommen.

### I-7 Hot-Path RPC-Freiheit
Nie RPC im normalen Trading-Hot-Path ohne explizite Freigabe. Kein neuer periodischer RPC-Poll nur fuer WSOL-/Dashboard-Korrektur.

### I-9 Simulation-Gate
Wenn Simulation fehlschlaegt, darf keine Transaktion gesendet werden. Dieser Scope darf keine Sende-/Simulationslogik aufweichen.

### I-12 Decision Record
Der Fix darf bestehende ExecutionResult-/Failure-/Audit-Pfade nicht entfernen oder still umgehen.

### I-24d Cold-Path Discovery nur per Request/Reply
Keine neue lokale Discovery-/Truth-Architektur in `execution-engine`. Der Scope ist Wallet-/WSOL-State-Propagation, nicht ein Architekturumbau fuer Discovery.

## Relevante Bug-Patterns

- `KNOWN_BUG_PATTERNS.md` #5
  - stale Wallet-/Snapshot-State und fehlende Balance-Transitionen fuehren zu falschem Runtime-Zustand
- `KNOWN_BUG_PATTERNS.md` #11
  - WSOL und native SOL duerfen nicht vermischt oder falsch interpretiert werden
- `KNOWN_BUG_PATTERNS.md` #17
  - WSOL Lifecycle: WsolManager, WalletBalanceUpdate, ATA-close, KillSwitch-Races
- `KNOWN_BUG_PATTERNS.md` #23
  - nicht-atomische SOL/WSOL-Updates fuehren zu falscher `wallet_total_sol_lamports`

## Bestehendes Pattern

Der bestehende Soll-Pfad ist:

1. `market-data` ist SSOT fuer Wallet-Snapshots auf JetStream.
2. `execution-engine` / LockManager konsumiert WalletBalanceSnapshot / WalletBalanceUpdate und speist daraus:
   - `available_wsol`
   - `wallet_total_sol_lamports`
   - Heartbeat / Metriken / Dashboard
3. Nach einem WSOL-ATA-Close muss derselbe State-Pfad einen **expliziten Zero-State** produzieren.

Der Fix soll dieses Pattern **vollenden**, nicht durch UI-Hardcodes oder Sonderlogik ersetzen.

## Erwartete Arbeitsschritte

Bitte arbeite in dieser Reihenfolge:

### A. Exakte Root Cause beweisen

Beweise vor dem Fix, an welcher Stelle der `WSOL=0`-Zustand verloren geht:

1. `execution-engine` sendet WSOL-close, aber nullt lokalen State nicht?
2. `market-data` erkennt geschlossenen WSOL-ATA nicht als `0`?
3. JetStream publiziert keinen WSOL-Zero-Snapshot?
4. `execution-engine`/LockManager konsumiert vorhandenen WSOL-Zero-State nicht?

### B. Narrow Fix: WSOL close muss Zero-State produzieren

Korrigiere genau diese Luecke so, dass nach erfolgreichem WSOL-close:

1. `tracked_wallet.last_wsol_balance` / `wsol_seen` korrekt auf `0` uebergehen
2. ein autoritativer `WalletBalanceSnapshot` fuer WSOL `balance_raw=0` entsteht
3. `execution-engine` / LockManager `available_wsol()` auf `0` faellt
4. `wallet_total_sol_lamports` danach nur noch `native_sol + 0` spiegelt

Erlaubte Richtungen:

- Geyser-/Account-Close-Handling fuer WSOL-ATA explizit als `0` behandeln
- bei bestaetigtem WSOL-close einen expliziten Zero-Snapshot publizieren
- LockManager-/Metrik-Refresh nach bestaetigtem Close sauber nachziehen

Nicht das Ziel:

- Dashboard-Frontend hardcoden
- Query-seitige kosmetische Korrektur
- neuer periodischer RPC-Fallback fuer WSOL
- genereller Refactor des Wallet-Systems

### C. Regressionstest / Nachweis

Bitte enge Tests bzw. belastbare Runtime-Nachweise hinzufuegen fuer:

1. WSOL-ATA wird geschlossen / unwrapped -> resultierender WSOL-State wird `0`
2. `available_wsol()` bleibt danach nicht auf stale Vorwert
3. `wallet_total_sol_lamports` zaehlt danach WSOL nicht doppelt

## Akzeptanzkriterien

- Nach erfolgreichem WSOL-close zeigt der Runtime-State `available_wsol = 0`.
- `wallet_total_sol_lamports` reflektiert danach nur noch nativen SOL plus echten verbleibenden WSOL-Rest.
- Das Dashboard zeigt nach Liquidation / WSOL-close nicht weiter `1 WSOL`, wenn die ATA geschlossen ist.
- Kein neuer Hot-Path-RPC.
- Kein UI-Hardcode.
- Kein Simulations-/TX-Sendelogik-Umbau.

## Erlaubte Dateien

- `src/bin/market_data.rs`
- `src/bin/execution_engine.rs`
- `src/storage/locks.rs`
- kleine, direkt zugehoerige Tests / Hilfsfunktionen im Impl-Repo

Nur falls direkt noetig:

- `src/metrics.rs`
- `scripts/trades_server.py`
- kleine Dashboard-/API-nahe Stellen, aber nur wenn nachweislich der Ursprung **nicht** im Runtime-State liegt

## Verboten

- Kein Eval-Repo
- Kein neuer periodischer RPC-Poll nur fuer WSOL-Sync
- Kein Frontend-Hardcode `wenn kill switch dann wsol=0`
- Kein grosser Wallet-/Snapshot-Refactor
- Keine Aenderung am erfolgreichen Scope-54-Sell-Resolver ausser direkt notwendige Test-/Build-Anpassung

## Pruef-Befehle

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --quiet
```

## Erwarteter Abschlussbericht

Bitte am Ende kurz nennen:

- welche STOP-CHECKs geprueft wurden
- wo genau der `WSOL=0`-Zustand verloren ging
- welche Datei / welcher Pfad die Zero-Propagation jetzt sicherstellt
- ob der Fix in `market-data`, `execution-engine` oder beiden lag
- welche Tests / Checks gelaufen sind
