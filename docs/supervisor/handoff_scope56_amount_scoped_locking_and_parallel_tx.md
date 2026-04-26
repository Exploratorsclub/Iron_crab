# Handoff: Scope 56 - Amount-scoped locking statt globaler Mint-Blockaden

WICHTIG: Lies und befolge die STOP-CHECK Regeln in `AGENTS.md` und `.cursor/rules/ironcrab-core.mdc` BEVOR du eine Datei aenderst. Wenn eine geplante Aenderung gegen eine Regel verstoesst, STOPPE sofort und melde den Verstoss statt die Aenderung durchzufuehren.

## Task-Beschreibung

Der aktuelle Locking-Pfad in `execution-engine` / `LockManager` blockiert parallele TXs zu grob.

Konkretes Runtime-Symptom aus dem letzten Testlauf:

- Ein BUY war bereits gesendet, aber noch nicht bestaetigt.
- Waehrenddessen kam ein STOP_LOSS-SELL fuer ein anderes Token herein.
- Der SELL wurde mit `LOCK_RESOURCE_CONFLICT` rejected.
- Das ist fachlich falsch, wenn der SELL nur wegen eines globalen Mint-Locks auf demselben Quote-/Output-Mint (praktisch `WSOL`) blockiert wird.

Die Nutzeranforderung ist explizit:

1. BUYs duerfen nur `SOL/WSOL` in der **tatsaechlich benoetigten Hoehe** locken.
2. SELLs duerfen nur den **verkauften Token-Mint** in der **tatsaechlich benoetigten Hoehe** locken.
3. Ein Mint, das die TX **nur erhaelt**, darf nicht als globaler Konflikt-Lock andere TXs blockieren.
4. Der Bot muss mehrere TXs parallel ausfuehren koennen, wenn genug reale Wallet-Bestaende vorhanden sind.
5. Locks sind dafuer da, Handel mit **nicht vorhandenen** / bereits reservierten Bestaenden zu verhindern, nicht um alle Intents mit demselben Mint global zu serialisieren.

Ziel dieses Scopes:

1. Die globale Mint-Blockade im Resource-Lock-Pfad so zuschneiden, dass sie nicht mehr sinnlos `BUY` gegen `SELL` oder mehrere parallele TXs mit ausreichendem Bestand blockiert.
2. Das bereits gute amount-scoped Capital-Lock-Pattern erhalten und als Primaer-Schutz fuer Wallet-Bestaende nutzen.
3. Nur dort Resource-Locks behalten, wo wirklich eine **echte Write-/Race-Hazard** existiert, nicht bloss ein gemeinsamer Mint-Name.

## Harte Runtime-Evidenz

### A. Konkreter Fehlfall aus Logs

Server-Run 23.04.2026:

1. `15:40:31.947Z`
   - BUY `int-23d8b342-000001` wurde via TPU gesendet
2. `15:40:36.612Z`
   - SELL / Stop-Loss `int-23d8b342-000002` wurde direkt mit `LOCK_RESOURCE_CONFLICT` rejected
3. `15:40:44.662Z`
   - Der fruehere BUY `int-23d8b342-000001` wurde erst spaeter bestaetigt

Schluss:

- Der SELL wurde nicht wegen fehlendem Token-Bestand oder Simulationsfehler blockiert.
- Er lief in eine zu grobe Locking-Regel, waehrend ein anderer Intent noch in-flight war.

### B. Der aktuelle Code lockt zu grob

Der aktuelle Check-Pfad in `execution-engine` macht:

1. Resource-Locks fuer **alle Pools**
2. danach Resource-Locks fuer **beide Mints**
   - `intent.resources.input_mint`
   - `intent.resources.output_mint`
3. erst danach den amount-scoped Capital Lock

Das ist der problematische Ist-Zustand:

- ein BUY kann nicht nur die verbrauchte `WSOL`-Menge reservieren, sondern zusaetzlich den Mint selbst als globale Ressource blockieren
- ein SELL kann dadurch blockiert werden, obwohl er `WSOL` nur **als Output erhaelt** und dafuer gar keinen Wallet-Bestand verbraucht

### C. Das bestehende amount-scoped Pattern ist bereits vorhanden

Wichtig:

- `try_lock_capital()` ist bereits amount-scoped
- BUY reserviert `sol_lamports` / Trading-Capital mengenbasiert
- SELL reserviert Token-Bestaende mengenbasiert ueber `tokens[mint] = required_raw`

Das ist das richtige Basis-Pattern und soll **nicht** durch neue globale Locks verdeckt werden.

## Root-Cause-These

Die staerkste aktuelle Root Cause ist:

1. `Capital Lock` ist bereits korrekt auf tatsaechlich verbrauchte Wallet-Bestaende modelliert.
2. Der vorgelagerte `Resource Lock` ist jedoch zu grob, weil er Mints global exklusiv behandelt.
3. Dadurch entstehen `LOCK_RESOURCE_CONFLICT`-Rejects in Faellen, die eigentlich nur ueber amount-scoped Capital Locks beurteilt werden duerften.
4. Besonders falsch ist das fuer Mints, die eine TX **nur als Output** erhaelt (`SELL -> WSOL`).

## Relevante Invarianten (Volltext)

### I-4 Geyser-First
HOT PATH (Discovery, Buy, Sell, Monitoring): GEYSER-ONLY. Keine neuen blockierenden RPC-Calls. Wenn ein Fix sowohl Hot als auch Cold Path beruehrt, darf er keinen neuen unbedingten RPC im normalen Trading-Flow einfuehren.

### I-5 Cold Path
COLD PATH (Liquidation, Manual Actions, Bootstrap): RPC erlaubt. Safety und correctness vor Speed. Dieser Scope betrifft primaer den normalen Execution-/Locking-Pfad; ein Fix darf nicht ueber neuen RPC im Hot Path erreicht werden.

### I-7 Hot-Path RPC-Freiheit
Nie RPC im normalen Trading-Hot-Path ohne explizite Freigabe. Kein neuer Poll / kein Discovery-Fallback als "Locking-Fix".

### I-9 Simulation-Gate
Wenn Simulation fehlschlaegt, darf keine Transaktion gesendet werden. Dieser Scope darf keine Simulations- oder Sendelogik aufweichen.

### I-12 Decision Record
Der Fix darf bestehende Rejected-/Decision-/Audit-Pfade nicht entfernen oder still umgehen. Wenn etwas weiterhin abgelehnt wird, muss der Grund sauber sichtbar bleiben.

### I-20 Capital Locks
Keine Ueberbuchung. `LockManager.try_lock_capital()`.

### I-21 Resource Locks
Accounts/Pools/ATAs die Konflikte erzeugen werden gelockt.

Wichtig fuer diesen Scope:

- Diese Invariante erlaubt **echte Konflikt-Ressourcen** zu locken.
- Sie rechtfertigt **keine** globale Mint-Exklusivitaet ohne reale Write-/Race-Hazard.

### I-22 Idempotency
Engine vermeidet doppelte Verarbeitung (Intent-ID, Tx-Signature, in-flight Registry).

## Relevante Bug-Patterns

- `KNOWN_BUG_PATTERNS.md` #4
  - Kein RPC im Hot Path
- `KNOWN_BUG_PATTERNS.md` #15
  - LockManager Double-Counting / SELL Race
- `KNOWN_BUG_PATTERNS.md` #19
  - Kein Fix ohne harte Runtime-Evidenz; keine spekulative Gross-Loesung
- `KNOWN_BUG_PATTERNS.md` #23
  - Nicht-atomische Wallet-/Lock-Updates koennen Folgeschaeden erzeugen; vorhandene amount-scoped Balance-Methoden nicht kaputt refactoren

## Bestehendes Pattern

Das bestehende Soll-Pattern, das wiederverwendet werden soll:

1. `LockManager.available_tokens` repraesentiert freie, lockbare Token-Bestaende.
2. `set_available_token_balance()` und `try_lock_capital()` arbeiten bereits mengenbasiert.
3. Bei SELLs wird heute schon `tokens[input_mint] = required_raw` reserviert.
4. Damit ist die Wallet-Schutzlogik fuer "du darfst nicht mehr verkaufen als frei verfuegbar ist" bereits vorhanden.

Der Fix soll dieses Pattern **staerken**, nicht durch neue globale Exklusiv-Locks ueberdecken.

## Erwartete Arbeitsschritte

Bitte arbeite in dieser Reihenfolge:

### A. Exakte Konfliktquelle im aktuellen Pfad eingrenzen

Beweise im Code sauber:

1. welche Resource-Locks heute fuer BUY und SELL gesetzt werden
2. welche davon echte Write-Konflikte abdecken
3. welche davon nur symbolische / globale Mint-Konflikte erzeugen
4. ob ein SELL aktuell auch fuer ein reines Output-Mint einen Resource-Lock setzt

### B. Narrow Fix: Locks auf tatsaechlich verbrauchte Wallet-Ressourcen zuschneiden

Zielbild:

1. BUY:
   - amount-scoped Capital Lock auf benoetigtes `SOL/WSOL`
   - **kein** globaler Mint-Conflict nur weil `WSOL`/Quote-Mint in der Route vorkommt
2. SELL:
   - amount-scoped Capital Lock auf den **verkauften** Token-Mint
   - **kein** globaler Mint-Conflict auf das Output-/Empfangs-Mint
3. Wenn zwei Intents denselben Mint nur im Namen teilen, aber genug freier Bestand fuer die tatsaechlich verbrauchte Seite da ist, duerfen sie sich nicht automatisch blockieren.

Erlaubte Richtungen:

- Mint-Resource-Locking fuer Output-/received mints entfernen oder gezielt einschränken
- Resource-Locks auf echte Konflikt-Ressourcen reduzieren
- Capital-Lock als kanonischen Mengen-Schutz ausbauen / klarer machen
- bestehende Tests in `storage/locks.rs` und eng benachbarte Execution-/Lock-Tests erweitern

Nicht das Ziel:

- globaler Locking-Refactor fuer alles und jeden
- neuer RPC-/Discovery-/State-Rebuild-Pfad
- Hot-Path-Waiting auf Heartbeats / Background-Tasks
- Locking ueber UI-/Dashboard-State reparieren

### C. Regressionstests / Nachweis

Bitte fuege enge Tests hinzu fuer mindestens:

1. BUY lockt nur die benoetigte Kapitalmenge; ein anderer Intent wird nicht allein wegen gemeinsamer Mint-Referenz abgewiesen.
2. SELL gegen Token A wird nicht durch einen kleinen BUY blockiert, der `WSOL` nur als Input/Quote verwendet.
3. Ein SELL, der Mint X verkauft, blockiert nicht einen anderen Intent nur deshalb, weil Mint X oder `WSOL` als Output vorkommt.
4. Wenn zwei Intents mehr vom selben **verbrauchten** Mint locken wollen als frei verfuegbar ist, greift weiterhin der amount-scoped Capital-Lock sauber.
5. Idempotency und bestehende Reject-/Decision-Pfade bleiben intakt.

## Akzeptanzkriterien

- Ein kleiner BUY darf keinen SELL nur wegen globalem Mint-Lock auf `WSOL` blockieren.
- BUYs locken nur den benoetigten `SOL/WSOL`-Betrag.
- SELLs locken nur den benoetigten verkauften Token-Betrag.
- Output-/received mints erzeugen nicht standardmaessig einen globalen Konflikt-Lock.
- Mehrere TXs koennen parallel laufen, wenn genug freie Wallet-Bestaende vorhanden sind.
- Echte Ueberbuchung wird weiterhin korrekt verhindert.
- Kein neuer Hot-Path-RPC.
- Keine Aufweichung von Simulation / Decision Record / Idempotency.

## Erlaubte Dateien

- `src/storage/locks.rs`
- `src/bin/execution_engine.rs`
- kleine direkt zugehoerige Tests im Impl-Repo

Nur falls direkt noetig:

- eng benachbarte Lock-/Intent-Hilfsfunktionen im Impl-Repo

## Verboten

- Kein Eval-Repo
- Kein neuer RPC-/Poll-/Discovery-Pfad im Hot Path
- Kein grosser allgemeiner Scheduler-/Arbitration-Refactor
- Kein UI-/Dashboard-Workaround
- Kein neuer globaler "alles parallel erlauben"-Bypass ohne harte Mengen-/Konfliktregeln

## Pruef-Befehle

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --quiet
```

## Erwarteter Abschlussbericht

Bitte am Ende kurz nennen:

- welche STOP-CHECKs geprueft wurden
- welche konkrete aktuelle Lock-Regel den falschen Konflikt erzeugt hat
- welche Resource-Locks erhalten blieben und warum
- welche globalen Mint-Locks entfernt / eingegrenzt wurden
- wie der Fix jetzt parallele TXs bei ausreichendem Bestand erlaubt
- welche Tests / Checks gelaufen sind
