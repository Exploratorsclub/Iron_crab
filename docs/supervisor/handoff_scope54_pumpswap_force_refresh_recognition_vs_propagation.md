# Handoff: Scope 54 - PumpSwap force_refresh klassifiziert oder propagiert Extended SELL-Layout falsch

WICHTIG: Lies und befolge die STOP-CHECK Regeln in `AGENTS.md` und `.cursor/rules/ironcrab-core.mdc` BEVOR du eine Datei aenderst. Wenn eine geplante Aenderung gegen eine Regel verstoesst, STOPPE sofort und melde den Verstoss statt die Aenderung durchzufuehren.

## Task-Beschreibung

Nach den bereits gemergten Scopes `#78`, `#79`, `#81`, `#82`, `#83` scheitern Kill-Switch-Liquidationen fuer zwei PumpSwap-Pools weiterhin mit `Custom(6023) / Overflow`.

Wichtig:

- Mixed deploy ist ausgeschlossen.
- Der Server lief garantiert auf dem gemergten Stand von `PR #83`.
- Der alte Fehler "Builder benutzt noch altes Layout trotz neuer Erkennung" ist gegen den gemergten Stand **nicht mehr** die primaere Arbeitsthese.

Die verbleibende Arbeitsfrage ist jetzt eng:

1. **Erkennt `market-data` beim autoritativen `force_refresh=true` das Extended SELL-Layout fuer diese Pools falsch als `Base`?**
2. **Oder erkennt `market-data` das Extended Layout korrekt, aber verliert es bei Publish / JetStream / SLAVE-Merge / Retry-Consumption?**

Der Scope dieses Handoffs ist genau diese Unterscheidung und der minimale Fix dafuer.

## Harte Runtime-Evidenz

Betroffene Mints / Pools:

- Mint `E7UaWyQoDgvUTvgQLxbR3oVyYpf3eq2hN95RzrwQpump`
  - Pool `B8bvg3KzXzGAq51QjirhPTw5ChhiZWn2kNwvQd3YZFN8`
- Mint `GwQjXZvDTVVWyadJAvjx9upEZsFFToVQHY5NRrZ6wzTR`
  - Pool `5rNMGrJ3V2vUY3GAuxiVKZmKCn6c5N6n7Ld5EWvgceVX`

Beobachtetes Runtime-Bild:

1. `execution-engine` triggert bei strukturellem PumpSwap-Sim-Fail korrekt `EnsurePumpAmmPoolAccounts(force_refresh=true)`.
2. `execution-engine` wartet bounded auf `ControlResponse`.
3. Danach baut der Retry sichtbar mit
   - `pool_accounts_source="slave_explicit_jetstream_ready_v14"`
   - aber trotzdem
   - `sell_extended=false`
   - `sell_cashback_third_meta=None`
4. Danach scheitert der Sell erneut mit `Custom(6023)`.

Wichtig:

- Das zeigt: der Retry-Pfad aus `#83` ist aktiv.
- Die Engine baut also nicht einfach blind weiter mit alten Intent-Accounts.
- Trotzdem kommt beim Retry effektiv wieder ein **Basislayout** an.

## Harte GitHub-/Code-Evidenz

Nur gemergte PRs sind Source of Truth:

- `#78` - dynamisches PumpSwap SELL-Layout + JetStream-Metadaten
- `#79` - `force_refresh` ist autoritativ; unresolved Extended SELL darf nicht als normales Ready/Ok durchrutschen
- `#81` - strukturierte Scope-49-Diagnostik fuer SELL-layout force_refresh
- `#82` - `jsonParsed`/CPI `programId` Observer-Haertung fuer SELL-layout
- `#83` - Retry konsumiert explicit JetStream-ready v14 nach `force_refresh`

Relevante Schlussfolgerung daraus:

- Der verbleibende Fehler liegt mit hoher Wahrscheinlichkeit **vor** dem eigentlichen SELL-Bau.
- Am ehesten:
  - falsche autoritative `Base`-Klassifikation im `market-data`-Observer
  - oder korrekte Erkennung, aber falsche / unvollstaendige Propagation der SELL-layout-Metadaten

## Plausibelster Grundfehler

Die staerkste aktuelle Root-Cause-These ist:

Der autoritative `force_refresh`-Observer behandelt einen erfolgreichen historischen `21`-Account-Sell fuer denselben Pool / dieselbe Base-Mint als ausreichend autoritativ und liefert daraus faelschlich `Base`, obwohl der Pool aktuell das `24`-Account-Extended-SELL-Layout braucht.

Wenn das nicht die Ursache ist, dann muss bewiesen werden, dass `market-data` intern korrekt `Extended` bestimmt, aber die autoritativen Metadaten auf dem Weg in JetStream / SLAVE / Retry-Consumption verloren gehen.

## Relevante Invarianten (Volltext)

### I-4 Geyser-First
HOT PATH (Discovery, Buy, Sell, Monitoring): GEYSER-ONLY. Keine neuen blockierenden RPC-Calls. Wenn ein Pfad sowohl Hot als auch Cold Path beruehrt, darf der Fix keinen neuen blockierenden Engine-RPC oder externen RPC in den Hot Path schleusen.

### I-5 Cold Path
COLD PATH (Liquidation, Manual Actions, Bootstrap): RPC erlaubt. Safety und correctness vor Speed. Autoritativer On-Chain-State darf hier nachgeladen werden.

### I-7 Hot-Path RPC-Freiheit
Nie RPC im normalen Trading-Hot-Path ohne explizite Freigabe. Keine neue Feature-Erkennung, die versehentlich im Hot Path unbedingte RPCs ausloest.

### I-9 Simulation-Gate
Wenn Simulation fehlschlaegt, darf keine Transaktion gesendet werden. Dieser Scope darf das Simulations-Gate nicht aufweichen oder bypassen.

### I-12 Decision Record
Wenn der Sell-Pfad wegen fehlender autoritativer Layout-Information fehlschlaegt, muessen Decision-Record und bestehende Reject-Pfade erhalten bleiben. Keine stille Ablehnung.

### I-24d Cold-Path Discovery nur per Request/Reply
`execution-engine` darf fehlende oder unbrauchbare PumpSwap-Accounts im Cold Path weder selbst discovern noch lokal als Truth in den SLAVE Cache schreiben. Discovery, MASTER-Write und JetStream-Publikation bleiben bei `market-data`. `execution-engine` darf nur den korrelierten Request/Reply-Pfad anstossen und bounded auf die autoritative Antwort warten.

## Relevante Bug-Patterns

- `KNOWN_BUG_PATTERNS.md` #14
  - PumpSwap Account-Count / Layout-Annahmen duerfen nicht starr sein.
- `KNOWN_BUG_PATTERNS.md` #19
  - kein Symptomfix ohne harte Runtime-Evidenz
- `KNOWN_BUG_PATTERNS.md` #25
  - relevantes Muster: korrekte Feature-Erkennung in `market-data`, Propagation ueber JetStream, SLAVE darf nicht mit falschem Default leben
- `KNOWN_BUG_PATTERNS.md` #34
  - Cold-Path Recovery darf nicht effektiv denselben kaputten PumpSwap-Zustand erneut liefern
- `KNOWN_BUG_PATTERNS.md` #36
  - Cache-Hit / Teilzustand ist nicht automatisch ready

## Bestehendes Pattern

Bitte **nicht** wieder am allgemeinen `tx_builder` symptomatisch schrauben.

Der Fix soll auf dem bereits gemergten Pattern aufsetzen:

- `market-data` ist Discovery-/Refresh-Autoritaet
- `force_refresh=true` ist autoritativ
- JetStream / `PoolCacheUpdate` ist SSOT
- SLAVE konsumiert autoritativen Zustand
- Retry baut mit dem frischen SSOT-Zustand

## Erwartete Arbeitsschritte

Bitte arbeite in dieser Reihenfolge:

### A. Exakt beweisen, wo der Fehler entsteht

Beweise vor dem Fix, ob der Fehler in:

1. **Erkennung** liegt:
   - `market-data` / `pumpfun_amm.rs` bestimmt beim autoritativen force_refresh faelschlich `Base`
2. **Propagation** liegt:
   - `market-data` bestimmt intern korrekt `Extended`, aber publiziert / merged die SELL-layout-Metadaten nicht korrekt
3. **Consumption** liegt:
   - `execution-engine` / SLAVE hat den korrekten Zustand, konsumiert ihn aber beim Retry nicht korrekt

Wenn moeglich, liefere diesen Beweis aus dem aktuellen GitHub-Code und vorhandener Runtime-Evidenz.

### B. Minimalen Fix genau an der echten Fehlerstelle umsetzen

Erlaubte Richtungen:

- Observer-/Resolver-Logik aendern, damit `Base` nicht zu frueh als autoritative Wahrheit gilt
- Konfliktfall `21er`-vs-`24er`-Evidenz sauber behandeln
- autoritative SELL-layout-Metadaten im `PoolCacheUpdate` korrekt persistieren
- SLAVE-/Retry-Consumption fixen, falls dort die SELL-layout-Metadaten abgeschnitten werden

Nicht erlaubt:

- blindes Hardcoding `immer 24`
- lokale Discovery / lokaler Truth in `execution-engine`
- Simulations-Bypass
- neuer Hot-Path-RPC

### C. Tests genau fuer diesen Konfliktfall

Bitte fuege enge Regressionstests hinzu, die mindestens einen dieser Faelle absichern:

1. Pool hat historische `21er`-SELL-Evidenz und aktuelle `24er`-SELL-Evidenz -> Resolver darf nicht faelschlich autoritativ `Base` liefern
2. `market-data` publiziert autoritativ `Extended` -> JetStream/SLAVE konsumiert `pump_amm_sell_cashback_remaining`, `pump_amm_sell_cashback_third_meta`, `pump_amm_sell_layout_ready`
3. Retry nach `force_refresh` baut fuer betroffene Pools nicht erneut mit `sell_extended=false`, wenn der autoritative Zustand `Extended` ist

## Akzeptanzkriterien

- Es ist technisch klar belegt, ob der Fehler in Erkennung, Propagation oder Consumption lag.
- Fuer die betroffenen Pools kann `force_refresh=true` nicht mehr faelschlich einen autoritativen `Base`-Zustand liefern, wenn aktuell `Extended` noetig ist.
- Falls die Erkennung korrekt war, werden die autoritativen SELL-layout-Metadaten end-to-end durch JetStream / SLAVE / Retry sichtbar und nutzbar.
- Der Retry baut fuer betroffene Pools nicht mehr erneut mit `sell_extended=false`, wenn der autoritative Zustand `Extended` ist.
- Kein neuer Hot-Path-RPC.
- Kein lokaler Engine-Truth.
- Kein Simulations-Bypass.

## Erlaubte Dateien

- `src/solana/dex/pumpfun_amm.rs`
- `src/bin/market_data.rs`
- `src/execution/live_pool_cache.rs`
- `src/execution/pool_cache_sync.rs`
- `src/bin/execution_engine.rs` nur falls fuer die eigentliche Retry-Consumption minimal noetig
- `src/execution/tx_builder.rs` nur falls fuer die Consumption-/Source-Prioritaet minimal noetig
- enge Tests im selben Repo

## Verboten

- Kein Eval-Repo
- Kein globales Hardcoding `PumpSwap SELL = 24`
- Kein neuer lokaler Discovery-/RPC-Truth in `execution-engine`
- Kein neuer Hot-Path-RPC
- Kein unbounded externer Scan
- Kein Simulations-Bypass
- Kein grosser Multi-DEX-Refactor

## Pruef-Befehle

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --quiet
cargo test --features test_helpers --quiet
```

## Erwarteter Abschlussbericht

Bitte am Ende kurz nennen:

- welche STOP-CHECKs geprueft wurden
- ob die echte Root Cause in **Erkennung**, **Propagation** oder **Consumption** lag
- welche konkrete Fehlannahme / Code-Stelle dafuer verantwortlich war
- wie der Konfliktfall `21er`-vs-`24er`-Evidenz jetzt behandelt wird
- wie `Extended` / `third_meta` jetzt end-to-end sichtbar bleibt
- welche Tests / Checks gelaufen sind
