# Handoff: Scope 35b - PumpSwap pre-plan discovery placement fix

WICHTIG: Lies und befolge die STOP-CHECK Regeln in AGENTS.md und .cursor/rules/ironcrab-core.mdc BEVOR du eine Datei aenderst. Wenn eine geplante Aenderung gegen eine Regel verstoesst, STOPPE sofort und melde den Verstoss statt die Aenderung durchzufuehren.

## Task-Beschreibung

PR #66 hat den richtigen **Mechanismus** implementiert (bounded `EnsurePumpAmmPoolAccounts` + bounded Wait auf replizierten SLAVE-Zustand), aber an der **falschen Stelle** eingehakt.

Kritischer Befund:
- Der neue Scope-35-Block sitzt **nach** erfolgreichem `build_tx_plan`.
- Der belegte A.43-Gap liegt aber **vorher**:
  - `tx_builder::build_tx_plan` bricht fuer PumpSwap mit leerem `resources.accounts` bereits mit `TxPlanOutcome::Unsupported(RejectReason::UnsupportedIntent)` ab, wenn der Hint-Pool nicht im Cache ist oder `pool_accounts` fehlen/zu kurz sind.
  - `process_intent` rejectet danach sofort.
  - Ein nachgelagerter Pre-Sim-Discovery-Block wird in genau diesem Fall nie erreicht.

Ziel dieses Scopes:
- Den bereits richtigen bounded Request/Reply-Mechanismus an die **wirksame Stelle** verschieben bzw. dort einhaengen, wo der fatal belegte A.43-Fall tatsaechlich entsteht.
- Also: PumpSwap sell-all/manual Cold-Path mit explizitem Pool-Hint und leeren `resources.accounts` darf **nicht** vorher mit `UnsupportedIntent` sterben, ohne dass einmal bounded `EnsurePumpAmmPoolAccounts` an `market-data` gesendet wurde.

## Relevante Invarianten (Volltext)

### I-24d Cold-Path Discovery nur per Request/Reply
Wenn `execution-engine` im Cold Path fehlende Pool-Daten fuer einen Trade braucht, darf es diese Daten nicht selbst lokal per RPC/Discovery beschaffen und auch nicht lokal in den SLAVE-Cache schreiben. Stattdessen muss `execution-engine` einen gezielten Request an `market-data` schicken, auf eine korrelierte `ControlResponse` warten und danach den ueber JetStream replizierten SLAVE-Zustand verwenden.

### I-24e PumpSwap Hint-Pfad
Der explizite Pool-Hint aus dem Intent-Modell (`TradeIntent.resources.pools[0]`) muss im Wire-Contract als `ControlRequest.pool_address_hint` gefuehrt werden. Kein unbounded globaler Discovery-Scan in `execution-engine`.

### I-7 Hot Path RPC-Freiheit
Keine neuen lokalen RPC-Calls im `execution-engine`. Discovery bleibt via `market-data`.

### I-4 Geyser-First
Kein lokales Cache-Heilen in der Engine. Autoritativer Zustand kommt weiter aus MASTER -> JetStream -> SLAVE.

### I-9 Simulation-Gate
Der neue Slice darf nur den fehlenden autoritativen Zustand vor dem Plan-/Sim-Pfad beschaffen; er darf die Simulation nicht umgehen.

### I-12 Decision Record
Wenn bounded Discovery fehlschlaegt, muss der bestehende Reject-/Decision-Record-Pfad sauber erhalten bleiben. Kein stilles Drop.

## Bestehendes Pattern

Nutze die bereits vorhandenen bounded PumpSwap-Discovery-Bausteine:
- `request_discovery_and_wait(...)`
- `wait_for_usable_pump_amm_cache_state(...)`
- bestehende Gate-Logik fuer den schmalen Cold-Path-Sell-Fall

Wichtig: Der Mechanismus aus PR #66 ist nicht das Problem. Nur die **Platzierung** ist falsch.

## Erwartete Aenderung

Schneide den kleinsten moeglichen Fix, der Folgendes erreicht:

1. Nur fuer den schmalen Scope:
   - `is_cold_path_recovery_sell(...)`
   - `metadata.dex == "pump_amm"`
   - expliziter Pool-Hint in `resources.pools[0]`
   - `resources.accounts` leer
2. Wenn genau dieser PumpSwap-Hint-/sell_all-Fall vor dem ersten Plan-Build noch keine nutzbaren `pool_accounts` hat:
   - bounded `EnsurePumpAmmPoolAccounts` an `market-data`
   - `pool_address_hint` aus dem Intent
   - bounded Wait auf replizierten nutzbaren PumpSwap-SLAVE-Zustand
   - dann genau ein Retry des Plan-/Sim-Loops
3. Falls bounded Discovery/Wartepfad fehlschlaegt:
   - sauberer bestehender Reject-/Failure-Pfad
4. Der Einstieg muss dort greifen, wo der aktuelle A.43-Fall sonst mit `UnsupportedIntent` sterben wuerde.

Erlaubte Umsetzungsrichtungen:
- **Bevorzugt:** Vor den ersten `build_tx_plan`-Versuch in derselben Loop verschieben.
- **Alternativ:** Den schmalen PumpSwap-`UnsupportedIntent`-Shape gezielt abfangen und in bounded Discovery + `continue` uebersetzen.

## Sekundaerer Korrekturpunkt

Bitte die Gate-Schwelle mit dem realen PumpSwap-Builder-Pfad abstimmen:
- `tx_builder` akzeptiert fuer PumpSwap-SELL aktuell auch 12er-Account-Saetze,
- waehrend der neue Gate-Helfer in PR #66 ueber `pump_amm_swap_accounts_ready_by_base_mint(...)` implizit 14 nutzt.

Bitte inkonsistente 12-vs-14-Logik vermeiden, damit der Scope nicht zu breit oder zu eng wird.

## Erlaubte Dateien

- `src/bin/execution_engine.rs`

Falls wirklich noetig:
- eng benachbarte Datei, aber bitte nur mit klarer Begruendung

## Verboten

- Keine Aenderungen an `market_data.rs`
- Keine Aenderungen an `Iron_crab-eval`
- Keine lokalen RPC-Calls in `execution-engine`
- Keine lokalen SLAVE-Cache-Writes
- Kein neuer unbounded Wait
- Keine Ausweitung auf regulaere Momentum-/Hot-Path-SELLs

## Pruef-Befehle

```bash
cargo fmt --check
cargo clippy -- -D warnings
cargo test
```

## PR-Notiz

Bitte im PR explizit nennen:
- warum PR #66 in der ersten Version den A.43-Fall noch nicht schloss
- an welcher Stelle der wirksame Einstieg jetzt sitzt
- dass weiter kein Engine-RPC und kein Overclaim eingefuehrt wurde
