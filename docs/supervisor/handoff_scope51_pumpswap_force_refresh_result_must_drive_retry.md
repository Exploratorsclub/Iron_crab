# Handoff: Scope 51 - Erfolgreicher PumpSwap `force_refresh` muss den strukturellen Retry wirklich speisen

WICHTIG: Lies und befolge die STOP-CHECK Regeln in `AGENTS.md` und `.cursor/rules/ironcrab-core.mdc` BEVOR du eine Datei aenderst. Wenn eine geplante Aenderung gegen eine Regel verstoesst, STOPPE sofort und melde den Verstoss statt die Aenderung durchzufuehren.

## Task-Beschreibung

Nach Merge von Scope 50 und neuem Deploy ist das Fehlerbild **nicht mehr** dasselbe wie zuvor auf der `market-data`-Seite.

Die neue harte Laufzeit-Evidenz zeigt:

1. `execution-engine` triggert bei strukturellem PumpSwap-Sim-Fail korrekt `EnsurePumpAmmPoolAccounts(force_refresh=true)`.
2. `market-data` fuehrt den Cold-Path-Refresh erfolgreich aus.
3. `market-data` beobachtet fuer **beide** betroffenen Pools jetzt erfolgreich ein **authoritative Extended SELL layout** aus externer History.
4. `market-data` publiziert `PoolCacheUpdate` und antwortet fuer beide Requests mit `ControlResponse status=Ok`.
5. Trotzdem baut die `execution-engine` den anschliessenden Retry laut Logs weiter mit
   - `pool_accounts_source="intent_resources"`
   - demselben 14er-`v14_csv`
   - und einem `sell_ix_accounts_csv`, in dem das frisch beobachtete `Extended { third_meta: ... }` **nicht** auftaucht.
6. Danach scheitert die Simulation wieder identisch in `pump_amm` bei `GetFees` mit `Custom(6023)`.

Das bedeutet:

- Scope 50 hat den **SELL-layout observer** erfolgreich gefixt.
- Der neue Blocker liegt jetzt sehr wahrscheinlich in der **Propagation / Consumption** des erfolgreichen `force_refresh`-Ergebnisses.
- Die `execution-engine` bekommt zwar `status=Ok`, aber der strukturelle Retry nutzt die frischen Daten offenbar **nicht wirklich**.

Ziel dieses Scopes:

1. Die genaue Naht finden, an der der erfolgreiche `market-data`-Refresh im strukturellen Retry verloren geht.
2. Den Retry so korrigieren, dass er nach `status=Ok` **wirklich mit dem frischen PumpSwap-Zustand** neu baut.
3. Speziell fuer die beiden betroffenen Pools sicherstellen, dass das beobachtete **Extended SELL layout** (inkl. `third_meta`) den rebuilt Sell-IX erreicht.
4. Keine neue lokale Discovery-/Truth-Logik in `execution-engine`.

## Harte Runtime-Evidenz

Betroffene Pools / Mints:

- Mint `E7UaWyQoDgvUTvgQLxbR3oVyYpf3eq2hN95RzrwQpump`
  - Pool `B8bvg3KzXzGAq51QjirhPTw5ChhiZWn2kNwvQd3YZFN8`
- Mint `GwQjXZvDTVVWyadJAvjx9upEZsFFToVQHY5NRrZ6wzTR`
  - Pool `5rNMGrJ3V2vUY3GAuxiVKZmKCn6c5N6n7Ld5EWvgceVX`

### market-data: neuer Erfolg ist belegt

Fuer `B8bvg...` / Request `bc5219aa-6c63-42e7-a2d4-96d98c2bdb2e`:

- `force_refresh — skipping LivePoolCache pool_accounts; authoritative RPC parse`
- `pool_address hint provided, trying direct getAccount (fast path)`
- `no signatures available for authoritative SELL-layout observation ... log_ctx="local_force_refresh_sell_layout"`
- externer Scan:
  - `stage="getSignaturesForAddress" ... provider_status=ok`
  - `stage="getTransaction" ... provider_status=ok`
  - `candidate instruction decoded ... sell_candidates_seen=1`
- entscheidend:
  - `authoritative SELL-layout observed ... layout=Extended { third_meta: 9WzvoBKQoFN9qVQvKFHrJpFVDsb6DrF2mGMJEcqDX5ur }`
- danach:
  - `Published PoolCacheUpdate to JetStream`
  - `ControlResponse published ... status=Ok`

Fuer `5rNM...` / Request `c41fe552-42ba-404d-a224-b3a9e6cb9ec6`:

- gleiches Muster:
  - erfolgreicher externer Scan
  - `authoritative SELL-layout observed ... layout=Extended { third_meta: 9kFvRrefToeYrye3Hmjrm5D3z8KQ7reXHJTyLv91YScL }`
  - `Published PoolCacheUpdate to JetStream`
  - `ControlResponse published ... status=Ok`

### execution-engine: Retry nutzt offenbar nicht den frischen Zustand

Direkt **vor** und **nach** dem `status=Ok` fuer beide Pools:

- erster Build:
  - `pool_accounts_source="intent_resources"`
  - `v14_csv=...` (14 Accounts)
  - `sell_ix_accounts_csv=...`
- `ControlResponse received ... status=Ok`
- danach `PumpSwap cold-path recovery ... rebuilding tx (one retry)`
- zweiter Build:
  - **wieder** `pool_accounts_source="intent_resources"`
  - wieder derselbe 14er-`v14_csv`
  - `sell_ix_accounts_csv` zeigt **nicht** das frisch beobachtete `third_meta`
- danach wieder:
  - `Instruction: Sell`
  - `Instruction: GetFees`
  - `UiTransactionError(InstructionError(1, Custom(6023)))`

Wichtig:

- Das ist **kein** Helius-/Observer-Problem mehr.
- Das ist **kein** `ControlResponse status=Error` mehr.
- Das ist jetzt ein **Request/Reply-Consumption / Retry-Rebuild**-Problem.

## Relevante Invarianten (Volltext)

### I-4 Geyser-First
HOT PATH (Discovery, Buy, Sell, Monitoring): GEYSER-ONLY. Keine neuen blockierenden RPC-Calls.

### I-5 Cold Path
COLD PATH (Liquidation, Manual Actions, Bootstrap): RPC erlaubt. Safety und correctness vor Speed. Autoritativer On-Chain-State darf hier nachgeladen werden.

### I-7 Hot-Path RPC-Freiheit
Nie RPC im normalen Trading-Hot-Path ohne explizite Freigabe. Der Fix darf keinen neuen unbedingten RPC-Pfad fuer regulaere Buys/Sells einfuehren.

### I-9 Simulation-Gate
Wenn Simulation fehlschlaegt, darf keine Transaktion gesendet werden. Dieser Scope darf keinen Simulations-Bypass einfuehren.

### I-12 Decision Record
Wenn ein PumpSwap-Sell scheitert, darf der Intent nicht still verschwinden. Bestehende Decision-/Failure-Pfade muessen erhalten bleiben.

### I-24d Cold-Path Discovery nur per Request/Reply
`execution-engine` darf fehlende oder unbrauchbare PumpSwap-Accounts im Cold Path weder selbst discovern noch lokal als Truth in den SLAVE Cache schreiben. Discovery, MASTER-Write und JetStream-Publikation bleiben bei `market-data`. `execution-engine` darf nur den korrelierten Request/Reply-Pfad anstossen und bounded auf die autoritative Antwort warten.

## Relevante Bug-Patterns

- `KNOWN_BUG_PATTERNS.md` #19
  - Kein Symptomfix ohne harte Runtime-Evidenz.
- `KNOWN_BUG_PATTERNS.md` #34
  - Cold-Path Recovery darf nicht effektiv wieder denselben kaputten PumpSwap-Satz verwenden.
- `KNOWN_BUG_PATTERNS.md` #35
  - Reale PumpSwap-Accountwerte aus erfolgreicher Mainnet-/Refresh-Evidenz muessen end-to-end erhalten bleiben.
- `KNOWN_BUG_PATTERNS.md` #36
  - Cache-Hit / vorhandener Teilzustand ist nicht automatisch `ready`.
- `KNOWN_BUG_PATTERNS.md` #14
  - PumpSwap SELL-Layout nicht wieder auf starre 14er-/21er-Annahmen reduzieren, wenn die autoritative Beobachtung `Extended` liefert.

## Bestehendes Pattern

Bitte auf dem bestehenden Request/Reply-Recovery-Pfad aufsetzen:

- `execution-engine`:
  - struktureller Sim-Fail → `EnsurePumpAmmPoolAccounts(force_refresh=true)`
  - bounded Warten auf korrelierte `ControlResponse`
  - danach genau **ein** Rebuild/Retry
- `market-data`:
  - autoritativer Refresh
  - MASTER-Write / JetStream-Publikation
  - `ControlResponse`

Der Fix soll diese Semantik **vollenden**, nicht ersetzen.

Die Kernfrage fuer diesen Scope ist:

- Warum baut der Retry nach `status=Ok` weiter mit `pool_accounts_source="intent_resources"` und effektiv demselben alten 14er-Zustand?

## Erwartete Aenderung

Bitte arbeite in dieser Reihenfolge:

### A. Exakte Consumption-Luecke beweisen

Bevor du Code aenderst, beweise anhand des aktuellen GitHub-Stands und der betroffenen Pfade:

1. wo der strukturelle Retry seine PumpSwap-Daten fuer den Rebuild hernimmt
2. warum nach `ControlResponse status=Ok` trotzdem weiter `intent_resources` / alter Zustand benutzt wird
3. ob der Fehler in
   - fehlendem Warten auf frischen SLAVE-/JetStream-Zustand,
   - falscher Prioritaet im Rebuild,
   - nicht aktualisierten Intent-Resources,
   - oder abgeschnittenem Extended-Layout / `third_meta`
   liegt

### B. Narrow Fix: erfolgreicher force_refresh muss den Rebuild speisen

Korrigiere genau diese Consumption-Luecke so, dass fuer den strukturellen Retry:

1. der frische `market-data`-Zustand wirklich verwendet wird
2. das beobachtete `Extended SELL layout` den Rebuild erreicht
3. das frische `third_meta` im rebuilt PumpSwap-Sell-IX verwendbar wird

Erlaubte Richtungen:

- Retry wartet bounded auf den frischen, sichtbaren SLAVE-/JetStream-Zustand
- Retry priorisiert frischen Cache-/Refresh-State vor stale `intent_resources`
- Intent-/Rebuild-Pfad wird gezielt mit dem frischen autoritativen Ergebnis aktualisiert

Nicht das Ziel:

- lokale Discovery in `execution-engine`
- neuer direkter RPC-Truth in `execution-engine`
- zweiter Architekturumbau des Request/Reply-Pfads
- weiterer Helius-/Observer-Umbau

### C. Sichtbare Runtime-Evidenz fuer den Fix

Bitte die bestehende Runtime-Sichtbarkeit so erweitern oder erhalten, dass beim naechsten Produktivlauf klar lesbar ist:

1. welche Quelle der Retry fuer den Rebuild verwendet
2. ob das beobachtete `Extended`-Layout / `third_meta` uebernommen wurde
3. dass der Rebuild **nicht** mehr bloss denselben alten `intent_resources`-Satz wiederverwendet

Konkrete Minimalerwartung:

- ein stabiler Loghinweis im Retry-/Builder-Pfad, der sichtbar macht, ob der rebuilt PumpSwap-Sell aus frischem force-refresh/JetStream-State statt aus stale `intent_resources` stammt

## Akzeptanzkriterien

- Nach `ControlResponse status=Ok` baut der strukturelle Retry fuer PumpSwap **nicht mehr** effektiv mit demselben stale `intent_resources`-Zustand.
- Fuer die beiden betroffenen Pools kann der Rebuild das beobachtete `Extended SELL layout` inklusive `third_meta` nutzen.
- Die Logs machen lesbar, dass der Retry den frischen force-refresh-Zustand wirklich konsumiert.
- Kein neuer Hot-Path-RPC.
- Kein lokaler Truth / keine lokale Discovery in `execution-engine`.
- Kein Simulations-Bypass.
- Genau ein bounded Retry bleibt erhalten.

## Erlaubte Dateien

- `src/bin/execution_engine.rs`
- `src/execution/tx_builder.rs`
- `src/execution/pool_cache_sync.rs` nur falls direkt noetig, um den frischen Zustand im Retry sichtbar/verbrauchbar zu machen
- `src/bin/market_data.rs` nur falls fuer minimale, direkt noetige Propagation / Logging wirklich erforderlich
- kleine, direkt zugehoerige Tests oder Hilfsfunktionen im Impl-Repo

## Verboten

- Kein Eval-Repo
- Kein neuer lokaler Discovery-/RPC-Truth in `execution-engine`
- Kein neuer Hot-Path-RPC
- Kein unbounded Warten auf JetStream / Cache
- Kein zweiter Retry
- Kein Simulations-Bypass
- Kein grosser Multi-DEX-Umbau
- Keine realen API-Keys / Secrets in Code oder Doku committen

## Pruef-Befehle

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --quiet
```

## Erwarteter Abschlussbericht

Bitte am Ende kurz nennen:

- welche STOP-CHECKs geprueft wurden
- wo die Consumption-Luecke exakt lag
- welche Quelle der Retry vorher nutzte und welche er nach dem Fix nutzt
- wie `Extended` / `third_meta` jetzt den rebuilt Sell-IX erreicht
- welche Logs/Runtime-Felder den erfolgreichen Consumption-Fix sichtbar machen
- welche Tests / Checks gelaufen sind
