# Handoff: Scope 35 - PumpSwap sell-all Intent -> bounded pre-plan discovery

WICHTIG: Lies und befolge die STOP-CHECK Regeln in AGENTS.md und .cursor/rules/ironcrab-core.mdc BEVOR du eine Datei aenderst. Wenn eine geplante Aenderung gegen eine Regel verstoesst, STOPPE sofort und melde den Verstoss statt die Aenderung durchzufuehren.

## Task-Beschreibung

Schliesse den verbleibenden PumpSwap-Intent->ControlRequest-Gap fuer den manuellen Cold Path.

Aktueller Befund aus `Iron_crab-eval` PR #24 (`A.43` Retry):
- Zwei plausible oeffentliche Intent-Shapes wurden E2E gegen `execution-engine` getestet:
  - `source=ui-manual`, `sell_all=true`, `dex=pump_amm`, `resources.pools[0]`
  - danach erweitert auf `source=sell-all`, `sell_all=true`, `dex=pump_amm`, `resources.pools[0]`, `resources.accounts[0]` (Quell-ATA), `resources.token_program=SPL`, passende `token_account` / `token_program` Metadaten
- Beide Shapes fuehren **nicht** zu einem beobachtbaren `EnsurePumpAmmPoolAccounts` auf der Control-Plane; der E2E-Test timed sauber aus.
- Das bedeutet: Trotz PR #65 ist der beabsichtigte schmale A.43-Wire-Slice fuer diesen Intent-Shape noch nicht stabil beobachtbar.

Ziel dieses Scopes:
- Fuer **PumpSwap Cold-Path sell-all / manual safety tooling** mit explizitem Pool-Hint (`resources.pools[0]`) muss `execution-engine` einen **bounded** `EnsurePumpAmmPoolAccounts`-Request an `market-data` senden, wenn vor dem eigentlichen Sendepfad keine **nutzbaren** PumpSwap-`pool_accounts` vorliegen.
- Danach: bounded auf autoritativen SLAVE-Folgezustand warten, Tx genau einmal neu aufbauen / weiterlaufen lassen.
- Kein Overreach: nur dieser schmale Cold-Path-Slice, kein allgemeiner Hot-Path-SELL.

Wichtig: Der Scope ist **kein** allgemeiner Eval-Fix. Es geht um echtes Runtime-Verhalten in `Iron_crab`, damit der bereits dokumentierte `pool_address_hint`-Vertrag fuer den manuellen PumpSwap-Cold-Path wirklich beobachtbar wird.

## Relevante Invarianten (Volltext)

### I-24d Cold-Path Discovery nur per Request/Reply
Wenn `execution-engine` im Cold Path fehlende Pool-Daten fuer einen Trade braucht, darf es diese Daten **nicht selbst** per lokaler Discovery/RPC beschaffen und auch **nicht lokal** in den SLAVE-Cache schreiben. Stattdessen muss `execution-engine` einen gezielten Request an `market-data` schicken, auf eine korrelierte `ControlResponse` warten und anschliessend den ueber JetStream replizierten SLAVE-Zustand verwenden. Discovery, MASTER-Write und JetStream-Publish bleiben bei `market-data`.

### I-24e PumpSwap Hint-Pfad
Der explizite Pool-Hint aus dem Intent-Modell (`TradeIntent.resources.pools[0]`) muss auf dem Wire-Contract als `ControlRequest.pool_address_hint` gefuehrt werden. Ein ungueltiger oder nicht nutzbarer Hint darf nicht in einen unbounded globalen Discovery-Scan im `execution-engine` ausweichen. Der bounded Discovery-/Recovery-Pfad bleibt bei `market-data`.

### I-7 Hot Path RPC-Freiheit
Im normalen Trading-Hot-Path sind keine neuen RPC-Calls erlaubt. RPC darf nur im Cold Path stattfinden und dort nur auf der dafuer vorgesehenen Seite (`market-data`), nicht als lokaler Shortcut in `execution-engine`.

### I-4 Geyser-First
Bestehende Geyser-/JetStream-First-Muster duerfen nicht durch direkte lokale RPC- oder cache-bypass Logik im `execution-engine` ersetzt werden. Autoritativer State kommt weiter aus MASTER -> JetStream -> SLAVE.

### I-9 Simulation-Gate
Es duerfen keine Transaktionen gesendet werden, die die Simulation nicht erfolgreich passiert haben. Ein Recovery-/Discovery-Schritt darf nur dazu dienen, anschliessend erneut sauber zu planen/simulieren, nicht die Simulation zu umgehen.

### I-12 Decision Record
Ein Intent darf nicht still verschwinden. Wenn der neue bounded Discovery-Slice nicht erfolgreich zu nutzbarem Zustand fuehrt, muss der bestehende Entscheidungs-/Reject-Pfad sauber erhalten bleiben.

## Bestehendes Pattern

Nutze vorhandene bounded PumpSwap-Discovery-Muster in `src/bin/execution_engine.rs` als Vorlage, statt ein neues Pattern zu erfinden:

1. **Cold-Path quote/recovery mit missing ready `pool_accounts`:**
   - Im Liquidation-/Routing-Pfad wird bei fehlenden `ready` PumpSwap-`pool_accounts` bereits `request_discovery_and_wait(...)` verwendet.
   - Danach wird bounded auf `wait_for_usable_pump_amm_cache_state(...)` gewartet.
   - Erst dann werden die `pool_accounts` erneut aus dem SLAVE-Cache gelesen.

2. **6005-Retry-Pfad:**
   - Auch dort gilt: kein lokaler RPC im `execution-engine`, sondern `request_discovery_and_wait(...)` + bounded Wait auf den replizierten Cache-Zustand.

3. **Bestehender struktureller PumpSwap-Recovery-Pfad (PR #65):**
   - Nach einem PumpSwap-structural-sim-fail gibt es bereits einen bounded `EnsurePumpAmmPoolAccounts(force_refresh=true)`-Pfad fuer `is_cold_path_recovery_sell(...)`.
   - Dieser Scope hier soll den **frueheren** Gap schliessen: Wenn der Intent den manuellen Cold-Path-Hint schon mitbringt, aber der Pfad **vor** diesem structural-sim-recovery-Slice mangels nutzbarer Accounts/State nicht bis zu einem beobachtbaren Request kommt, soll dieselbe Request/Reply-Architektur auch dort greifen.

## Erwartete Aenderung

Schneide den kleinstmoeglichen Impl-Scope, der Folgendes erreicht:

1. Nur fuer **Cold-Path-Recovery-Sells** (`is_cold_path_recovery_sell(...)`) mit `dex=pump_amm`.
2. Nur wenn ein expliziter Pool-Hint aus dem Intent verfuegbar ist (`resources.pools[0]` / bestehende Hint-Helfer).
3. Wenn im relevanten Plan-/Build-Einstieg **keine nutzbaren PumpSwap-`pool_accounts`** vorliegen oder der Build sonst an genau diesem fehlenden/nicht nutzbaren PumpSwap-State haengen bleibt:
   - `execution-engine` sendet bounded `EnsurePumpAmmPoolAccounts` an `market-data`
   - mit `pool_address_hint` aus dem Intent
   - ohne lokalen RPC
   - ohne lokale SLAVE-Cache-Writes
4. Danach bounded auf replizierten nutzbaren PumpSwap-Cache-Zustand warten.
5. Danach Tx genau einmal neu planen / fortsetzen.
6. Wenn der autoritative Zustand nicht kommt: sauberer bestehender Reject-/Failure-Pfad, kein Hang, kein stilles Drop.

Wichtig:
- Bitte fuehre **keinen** allgemeinen neuen Recovery-Hook fuer alle SELLs ein.
- Bitte **kein** broad "immer vor build_tx_plan requesten".
- Der Slice soll so klein wie moeglich bleiben und nur den durch PR #24 belegten manuellen PumpSwap-Cold-Path schliessen.

## Erlaubte Dateien

- `src/bin/execution_engine.rs`

Falls absolut noetig und nur mit klarer Begruendung:
- eng benachbarte Helper-Datei in `src/execution/` oder `src/ipc/`, **aber bitte nur wenn `execution_engine.rs` allein wirklich nicht reicht**.

## Verboten

- Keine Aenderungen an `src/bin/market_data.rs`
- Keine Aenderungen an Eval-Tests / `Iron_crab-eval`
- Keine neuen lokalen RPC-Calls in `execution-engine`
- Keine lokalen Writes in den SLAVE `LivePoolCache`
- Kein neuer unbounded Wait / kein neuer Hang
- Keine Ausweitung auf reguläre Momentum-/Hot-Path-SELLs
- Kein Bypass der Simulation
- Keine neue globale Discovery ohne Hint

## Pruef-Befehle

```bash
cargo fmt --check
cargo clippy -- -D warnings
cargo test
```

## Abschluss-Notiz fuer den PR

Bitte im PR klar benennen:
- welcher konkrete Vorher-Nachher-Gap aus A.43 / PR #24 geschlossen wird
- an welcher Stelle der bounded Request/Reply-Slice jetzt einsetzt
- warum der Scope kein Hot-Path-RPC und kein Overclaim ist
