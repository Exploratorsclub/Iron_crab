# Handoff: Scope 39 - PumpSwap 6013 muss force_refresh triggern

WICHTIG: Lies und befolge die STOP-CHECK Regeln in AGENTS.md und .cursor/rules/ironcrab-core.mdc BEVOR du eine Datei aenderst. Wenn eine geplante Aenderung gegen eine Regel verstoesst, STOPPE sofort und melde den Verstoss statt die Aenderung durchzufuehren.

## Task-Beschreibung

Fixe einen engen Produktionsfehler im `execution-engine`-Recovery-Gate fuer PumpSwap AMM:

- Kill-Switch / Liquidation scheitert auf Mainnet weiterhin mit `InvalidProtocolFeeRecipient` / `Custom(6013)`.
- Die aktuelle Cold-Path-Recovery fuer PumpSwap sendet `EnsurePumpAmmPoolAccounts(force_refresh=true)` nur fuer einen begrenzten Satz struktureller Simulationsfehler.
- `Custom(6013)` ist aktuell **nicht** in diesem Gate enthalten.
- Dadurch wird bei genau diesem Fehlerbild **kein** autoritativer Request/Reply-Refresh an `market-data` ausgeloest.
- Der naechste Plan-/Simulationsversuch verwendet damit effektiv denselben bereits vorhandenen PumpSwap-Account-Satz weiter.

Ziel dieses Scopes:

1. `Custom(6013)` / `InvalidProtocolFeeRecipient` muss im PumpSwap-Recovery-Gate als struktureller Simulationsfehler behandelt werden.
2. Dadurch muss im Cold Path derselbe vorhandene Request/Reply-Mechanismus (`EnsurePumpAmmPoolAccounts` mit `force_refresh=true`) anspringen.
3. Füge einen fokussierten Regressionstest hinzu, der diesen Vertrag absichert.
4. Dieser Scope soll **nicht** den tieferen Root Cause in `market-data`/Discovery/Parser fixen. Er soll nur sicherstellen, dass `6013` ueberhaupt den autoritativen Force-Refresh ausloest.

## Aktueller Befund / Runtime-Evidenz

- Produktiv beobachtete fehlschlagende Pools:
  - Pool `B8bvg3KzXzGAq51QjirhPTw5ChhiZWn2kNwvQd3YZFN8`
  - Pool `5rNMGrJ3V2vUY3GAuxiVKZmKCn6c5N6n7Ld5EWvgceVX`
- Produktiv beobachtete fehlschlagende Mints:
  - `E7UaWyQoDgvUTvgQLxbR3oVyYpf3eq2hN95RzrwQpump`
  - `GwQjXZvDTVVWyadJAvjx9upEZsFFToVQHY5NRrZ6wzTR`
- In `src/bin/execution_engine.rs` matcht `is_pump_amm_structural_sim_error(...)` derzeit nur:
  - `6023`
  - `Overflow`
  - `0x1787`
- Genau dieser Matcher gate't sowohl:
  - den asynchronen Hot-Path-Refresh fuer regulaere PumpSwap-Sells
  - als auch den synchronen Cold-Path-Refresh mit `request_discovery_and_wait(..., force_refresh=true)` fuer Liquidation / `sell_all`
- Produktiv wurde bestaetigt: bei `6013` wurde **kein** PumpSwap-`force_refresh` Request/Reply ausgelost.

## Relevante Invarianten (Volltext)

### I-5 Cold Path
COLD PATH (Liquidation, Manual Actions, Bootstrap): RPC erlaubt. Safety und correctness vor Speed. Autoritativer On-Chain-State darf hier nachgeladen werden.

### I-7 Hot Path RPC-Freiheit
Im normalen Trading-Hot-Path sind keine neuen blockierenden RPC-Calls erlaubt. Wenn ein Pfad sowohl im Hot als auch im Cold Path verwendet wird, muss die bestehende Trennung erhalten bleiben.

### I-9 Simulation-Gate
Wenn Simulation fehlschlaegt, darf keine Transaktion gesendet werden. Recovery-/Discovery-Schritte duerfen nur dazu dienen, anschliessend erneut sauber zu planen und zu simulieren, nicht die Simulation zu umgehen.

### I-24d Cold-Path Discovery nur per Request/Reply
`execution-engine` darf fehlende oder unbrauchbare PumpSwap-`pool_accounts` im Cold Path weder selbst discovern noch lokal als Ersatz-Truth in den SLAVE Cache schreiben. Discovery, MASTER-Write und JetStream-Publikation bleiben bei `market-data`. `execution-engine` darf nur den korrelierten Request/Reply-Pfad anstossen und bounded auf die autoritative Antwort warten.

### A.43 PumpSwap Cold-Path Recovery: force_refresh und pool_address_hint
Loest die Cold-Path-Recovery nach strukturellem PumpSwap-Simulationsfehler einen Pfad mit `force_refresh` aus, darf dieselbe stale 14er-`pool_accounts`-Liste aus dem SLAVE LivePoolCache nicht unveraendert als Truth zurueckkommen. Der Hint-Pfad ueber `pool_address_hint` bleibt Teil des beobachtbaren Recovery-Vertrags.

## Bestehendes Pattern

Nutze exakt das bestehende Pattern in `execution-engine.rs`:

- Ein Helper `is_pump_amm_structural_sim_error(...)` kapselt die Fehlerfamilie.
- Derselbe Helper wird bereits an beiden relevanten Stellen wiederverwendet:
  - Hot-Path async healing fuer regulaere PumpSwap-Sells
  - Cold-Path sync Request/Reply-Recovery fuer Liquidation / `sell_all`
- Es gibt bereits ein eingebettetes Testmodul in `src/bin/execution_engine.rs` mit dem Test
  - `pump_amm_structural_sim_error_matches_cold_path_recovery_pattern`

Bevorzugtes minimales Vorgehen:

1. Erweitere den bestehenden Helper so, dass `6013` / `InvalidProtocolFeeRecipient` als struktureller PumpSwap-Simulationsfehler gilt.
2. Passe die vorhandene Testabdeckung im selben File an bzw. erweitere sie fokussiert.
3. Optional: passe die zugehoerigen Log-Texte an, wenn sie explizit nur `6023/Overflow family` behaupten und dadurch nach dem Fix missverstaendlich waeren.

## Relevante Bug-Patterns

- `KNOWN_BUG_PATTERNS.md` #34:
  - Cold-Path-Recovery darf nicht cache-first wieder denselben stale PumpSwap-Account-Satz recyceln.
  - Genau deshalb muss `force_refresh=true` fuer strukturelle Simulationsfehler zuverlaessig angestossen werden.
- `KNOWN_BUG_PATTERNS.md` #35:
  - `protocol_fee_recipient` darf nicht global kanonisiert oder echte Beobachtungen ueberschrieben werden.
  - Dieser Scope soll das **nicht** direkt fixen, aber er muss sicherstellen, dass `6013` zumindest den autoritativen Refresh-Pfad triggert.
- `KNOWN_BUG_PATTERNS.md` #19:
  - Kein spekulativer Grossfix. Kleiner, evidenzbasierter Schritt.

## Erwartete Aenderung

Schneide den kleinstmoeglichen Impl-Scope, der Folgendes erreicht:

1. `is_pump_amm_structural_sim_error(...)` erkennt `Custom(6013)` bzw. `InvalidProtocolFeeRecipient`.
2. Damit laeuft fuer Cold-Path-PumpSwap-Sells (`is_cold_path_recovery_sell`) derselbe bestehende `request_discovery_and_wait(..., true)`-Pfad auch bei `6013`.
3. Die bestehende Hot-/Cold-Path-Architektur bleibt unveraendert:
   - Hot Path: nur async publish / kein blockierender lokaler RPC
   - Cold Path: bounded Request/Reply an `market-data`
4. Ein Regressionstest im bereits vorhandenen Testmodul sichert ab, dass `6013` Teil dieses strukturellen Fehlersatzes ist.

## Akzeptanzkriterien

- `Custom(6013)` wird im PumpSwap-Recovery-Gate als struktureller Fehler erkannt.
- Der bestehende Cold-Path-Refresh-Mechanismus kann dadurch bei `6013` anspringen.
- Kein neuer lokaler RPC-Discovery-Pfad in `execution-engine`.
- Keine Aufweichung des Simulation-Gates.
- Regressionstest vorhanden und gruen.
- Scope bleibt klein und auf das Gate fokussiert.

## Erlaubte Dateien

- `Iron_crab/src/bin/execution_engine.rs`

## Verboten

- Keine Aenderungen an `market_data.rs`
- Keine Aenderungen an `pumpfun_amm.rs`
- Keine Aenderungen an `dex_parser.rs`
- Keine Aenderungen im Eval-Repo
- Kein neuer lokaler RPC-/Cache-Heal-Pfad in `execution-engine`
- Kein Umgehen der Simulation
- Kein grosser Root-Cause-Fix fuer `protocol_fee_recipient` in diesem Scope

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
- welche Stelle im Recovery-Gate angepasst wurde
- wie der Regressionstest den `6013`-Fall absichert
- welche Checks / Tests gelaufen sind
