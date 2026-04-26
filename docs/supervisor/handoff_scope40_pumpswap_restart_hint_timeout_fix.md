# Handoff: Scope 40 - PumpSwap Restart/Hint Discovery Gap + realistisches Cold-Path-Timeout

WICHTIG: Lies und befolge die STOP-CHECK Regeln in AGENTS.md und .cursor/rules/ironcrab-core.mdc BEVOR du eine Datei aenderst. Wenn eine geplante Aenderung gegen eine Regel verstoesst, STOPPE sofort und melde den Verstoss statt die Aenderung durchzufuehren.

## Task-Beschreibung

Fixe den naechsten engen Produktionsfehler im PumpSwap-Cold-Path nach Restart:

- Kill-Switch / Liquidation scheitert fuer bestimmte completed PumpFun -> PumpSwap Tokens aktuell **vor der Simulation** mit `QUOTE_UNAVAILABLE`.
- `execution-engine` sendet korrekt `EnsurePumpAmmPoolAccounts`, wartet aber nur `15s`.
- Im realen Produktionssystem braucht der erste gueltige RPC-Fallback fuer genau diese Mints (`getProgramAccounts` mit base/quote memcmp auf dem lokalen Validator) **ca. 24-25s**.
- Damit ist der aktuelle `15s`-Timeout fuer den bestehenden Fallback-Pfad real zu kurz.
- Gleichzeitig ist das eigentliche Root-Cause-Symptom: Der Cold Path kommt nach Restart fuer diese Tokens ohne verwendbaren `pool_address_hint` / ohne nutzbaren ready-Zustand an und faellt deshalb ueberhaupt auf den teuren globalen Scan zurueck, statt den schnellen bekannten Pool-Pfad zu nutzen.

Ziel dieses Scopes:

1. Den **eigentlichen Root Cause** schliessen: Nach Restart / Bootstrap / Cold-Path-Discovery soll fuer wallet-relevante PumpSwap-Pools ein gezielter bekannter Pool-Pfad greifen, statt blind im `getProgramAccounts`-Scan zu landen.
2. Das **Safety-Net** ergaenzen: Solange es noch legitime Cold-Path-Faelle ohne Hint geben kann, darf der `execution-engine` nicht schon nach `15s` abbrechen, wenn ein gueltiger lokaler Validator-Call real ~25s braucht.
3. Kein lokaler RPC-Discovery-Pfad in `execution-engine`; die Architektur `market-data -> MASTER -> JetStream -> SLAVE -> ControlResponse` bleibt erhalten.

## Aktueller Befund / Runtime-Evidenz

Produktiv beobachtete problematische Mints:

- `E7UaWyQoDgvUTvgQLxbR3oVyYpf3eq2hN95RzrwQpump`
- `GwQjXZvDTVVWyadJAvjx9upEZsFFToVQHY5NRrZ6wzTR`

Zu diesen Mints direkt am Produktivhost gegen `127.0.0.1:8899` gemessen:

- `getProgramAccounts(pAMMBay6..., memcmp base_mint + quote_mint=WSOL)`:
  - Mint `E7...pump`: **24.997s**, Ergebnis `count=1`, Pool `B8bvg3KzXzGAq51QjirhPTw5ChhiZWn2kNwvQd3YZFN8`
  - Mint `GwQ...zTR`: **24.266s**, Ergebnis `count=1`, Pool `5rNMGrJ3V2vUY3GAuxiVKZmKCn6c5N6n7Ld5EWvgceVX`
- `getAccountInfo(pool)` fuer diese beiden bereits bekannten Pool-Adressen:
  - jeweils **~0.001s**

Relevanter Schluss:

- Der RPC kann die Daten korrekt liefern.
- Das Problem ist **nicht** "Call unmoeglich / fehlender Index".
- Das Problem ist:
  - fehlender/nicht genutzter Hint-/Ready-Pfad nach Restart
  - plus ein unrealistisch kurzes `15s`-Budget fuer einen weiterhin vorhandenen legitimen Cold-Path-Fallback

## Relevante Invarianten (Volltext)

### I-4 Hot Path = Geyser-First
HOT PATH (Discovery, Buy, Sell, Monitoring): GEYSER-ONLY. Keine neuen blockierenden RPC-Calls. Wenn ein Pfad sowohl Hot als auch Cold Path beruehrt, darf der Fix keinen neuen blockierenden Engine-RPC in den Hot Path schleusen.

### I-5 Cold Path
COLD PATH (Liquidation, Manual Actions, Bootstrap): RPC erlaubt. Safety und correctness vor Speed. Autoritativer On-Chain-State darf hier nachgeladen werden.

### I-7 Hot-Path RPC-Freiheit
Nie RPC im normalen Trading-Hot-Path ohne explizite Freigabe. Kein neuer lokaler Discovery-RPC in `execution-engine`.

### I-9 Simulation-Gate
Wenn Simulation fehlschlaegt, darf keine Transaktion gesendet werden. Dieser Scope darf nur Discovery/Readiness/Timeout sauber machen, nicht die Simulation umgehen.

### I-12 Decision Record
Wenn Discovery oder Wait weiterhin fehlschlaegt, muss der bestehende Reject-/Decision-Record-Pfad erhalten bleiben. Keine stille Ablehnung ohne bestaetigtes Outcome.

### I-24d Cold-Path Discovery nur per Request/Reply
`execution-engine` darf fehlende oder unbrauchbare PumpSwap-`pool_accounts` im Cold Path weder selbst discovern noch lokal als Truth in den SLAVE Cache schreiben. Discovery, MASTER-Write und JetStream-Publikation bleiben bei `market-data`. `execution-engine` darf nur den korrelierten Request/Reply-Pfad anstossen und bounded auf die autoritative Antwort warten.

### Architektur-Regel: Cache-Hit ist nicht automatisch ready
Ein Pool oder Mint darf nicht nur deshalb als verwendbar gelten, weil irgendein Cache-Eintrag existiert. Fuer den entscheidenden Pfad muss ein verwendbarer Zustand vorliegen (`observed` / `partial` / `ready` bzw. aequivalentes Verhalten). Teilzustand nach Restart darf nicht still wie `ready` behandelt werden.

## Bestehendes Pattern

Nutze und erhalte die bereits vorhandenen Muster:

1. `execution-engine` sendet im Cold Path einen korrelierten `EnsurePumpAmmPoolAccounts` Request an `market-data`.
2. `market-data` bleibt autoritativ fuer RPC-Discovery, MASTER-Write und JetStream-Publish.
3. Wenn die Pool-Adresse bekannt ist, ist der schnelle Pfad bereits klar belegt:
   - `getAccountInfo(pool)` ist praktisch sofort
   - `getProgramAccounts` ist fuer PumpSwap als globaler Scan real ~25s
4. `pumpfun_amm.rs` enthaelt bereits das gewünschte Architektur-Muster:
   - vorhandene `pool_accounts` -> zero-RPC
   - bekannte Pool-Adresse -> single `getAccount`
   - globaler `getProgramAccounts`-Scan nur als letzter Cold-Path-Fallback

Das Problem ist also nicht, dass der Fast-Path fehlt. Das Problem ist, dass er im Restart-/Bootstrap-/Request-Reply-Verlauf fuer diese Pools nicht rechtzeitig/nutzbar ankommt.

## Relevante Bug-Patterns

- `KNOWN_BUG_PATTERNS.md` #31:
  - Bekannter PumpSwap-Pool darf nicht unnoetig ueber `getProgramAccounts` gesucht werden, wenn die Pool-Adresse schon bekannt ist.
- `KNOWN_BUG_PATTERNS.md` #33:
  - Nach Restart koennen `pool_accounts` im Bootstrap fehlen, wenn sie nicht persistent ueber JetStream erhalten bleiben.
- `KNOWN_BUG_PATTERNS.md` #34:
  - Cold-Path-Recovery darf nicht cache-first immer wieder denselben stale Zustand recyceln.
- `KNOWN_BUG_PATTERNS.md` #36:
  - Cache-Praesenz ist nicht automatisch `ready`.
- `KNOWN_BUG_PATTERNS.md` #19:
  - Kein spekulativer Grossfix. Root Cause mit Runtime-Evidenz schliessen.

## Erwartete Aenderung

Schneide den kleinsten sinnvollen Impl-Scope, der **beides** erreicht:

### A. Root Cause schliessen

Sorge dafuer, dass die beiden oben beschriebenen PumpSwap-Restart-/Cold-Path-Faelle moeglichst ueber den gezielten Fast-Path laufen statt ueber den 25s-Scan.

Erlaubte Richtungen:

1. Persistenz / Bootstrap / Replikation von `pool_address_hint` bzw. der fuer den Fast-Path notwendigen PumpSwap-Informationen verbessern.
2. Sicherstellen, dass `EnsurePumpAmmPoolAccounts` im relevanten Restart-/Cold-Path-Fall den bekannten Pool-Pfad tatsaechlich verwenden kann.
3. Falls der bekannte Pool bereits im MASTER/SLAVE-Kontext vorhanden ist, darf er nicht durch spaetere Updates "unready" degradiert oder ueberschrieben werden.

Wichtig:

- Bevorzuge gezielte Persistenz / Replikation / Hint-Erhaltung.
- Kein neuer globaler Discovery-Mechanismus in der Engine.
- Kein "wir warten einfach laenger und lassen den langsamen Scan Standard werden" als alleinige Loesung.

### B. Safety-Net korrigieren

Der aktuelle `15s`-Timeout im `execution-engine` ist fuer den noch vorhandenen legitimen Cold-Path-Fallback real zu kurz. Deshalb:

1. Erhoehe das bounded Cold-Path-Request/Reply-Timeout auf einen realistischen Wert oberhalb der gemessenen Produktionslatenz.
2. Wertschaetzung:
   - gemessen ~25s
   - daher sollte das Budget nicht unter `30s` liegen
   - mit Reserve ist `45s` plausibel
3. Diese Erhoehung ist **Pflicht als Safety-Net**, aber **nicht allein ausreichend**, wenn der Root Cause unangetastet bleibt.

## Akzeptanzkriterien

- Nach Restart / Bootstrap / Cold-Path-Discovery ist fuer wallet-relevante PumpSwap-Pools der bekannte Pool-/Hint-Pfad wieder belastbar erreichbar.
- Der teure `getProgramAccounts`-Scan ist fuer diese Faelle nicht mehr der primaere oder einzige Weg.
- `execution-engine` bricht einen legitimen Cold-Path-Request nicht mehr schon nach `15s` ab, wenn der lokale Validator real ~25s fuer den Fallback braucht.
- Kein neuer lokaler RPC-Discovery-Pfad in `execution-engine`.
- Keine Aufweichung des Simulation-Gates.
- Keine Verletzung von I-24d.
- Der Scope bleibt auf PumpSwap Restart/Bootstrap/Hint/Timeout fokussiert und weitet sich nicht zu einem generischen DEX-Refactor aus.

## Erlaubte Dateien

- `Iron_crab/src/bin/market_data.rs`
- `Iron_crab/src/bin/execution_engine.rs`
- `Iron_crab/src/solana/dex/pumpfun_amm.rs`
- `Iron_crab/src/cache/live_pool_cache.rs`
- `Iron_crab/src/pool_cache_sync.rs`

Falls wirklich noetig:

- eng benachbarte PumpSwap-/Cache-/IPC-Datei mit kurzer Begruendung im Abschlussbericht

## Verboten

- Keine Aenderungen im Eval-Repo
- Kein lokaler RPC-Discovery-/Write-Path in `execution-engine`
- Keine lokalen SLAVE-Truth-Writes in der Engine
- Kein Umgehen der Simulation
- Kein grosser Multi-DEX-Refactor
- Keine Rueckkehr zu `getTokenAccountsByOwner`-Fallbacks oder anderen validator-index-abhaengigen Pfaden, die bereits als fehleranfaellig bekannt sind

## Pruef-Befehle

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --quiet
cargo test --features test_helpers --quiet
```

## Erwarteter Abschlussbericht

Bitte nenne am Ende kurz:

- welche STOP-CHECKs geprueft wurden
- welche Root Cause konkret geschlossen wurde
- an welcher Stelle der bekannte Pool-/Hint-Pfad nach Restart jetzt erhalten oder wiederhergestellt wird
- wie das neue Timeout begruendet und gesetzt wurde
- welche Tests / Checks gelaufen sind
