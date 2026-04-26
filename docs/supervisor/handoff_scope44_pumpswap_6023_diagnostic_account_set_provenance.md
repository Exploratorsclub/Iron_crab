# Handoff: Scope 44 - PumpSwap `6023 Overflow` Diagnose ueber exakten Sell-Account-Satz

WICHTIG: Lies und befolge die STOP-CHECK Regeln in AGENTS.md und .cursor/rules/ironcrab-core.mdc BEVOR du eine Datei aenderst. Wenn eine geplante Aenderung gegen eine Regel verstoesst, STOPPE sofort und melde den Verstoss statt die Aenderung durchzufuehren.

## Task-Beschreibung

Dies ist **ein Diagnose-Scope, kein Blind-Fix-Scope**.

Nach Scope 43 kommt die Kill-Switch-Liquidation jetzt bis zur Simulation durch, scheitert aber fuer zwei PumpSwap-Token-2022-Sells **im PumpSwap-Programm selbst** mit:

- `UiTransactionError(InstructionError(1, Custom(6023)))`
- `AnchorError ... Error Code: Overflow. Error Number: 6023. Error Message: Overflow.`

Wichtig:

- Der alte Discovery-/Helius-Gate-Blocker ist fuer diesen Lauf **nicht** mehr die primaere Ursache.
- `tx_plan` wird erfolgreich gebaut.
- Der Fehler sitzt jetzt in der **semantischen Korrektheit des verwendeten PumpSwap-Sell-Account-Satzes** oder eines eng benachbarten Sell-Build-/Recovery-Schritts.

Der User will fuer diesen Scope **Root-Cause-Diagnostik**, nicht spekulative Korrekturen.

## Aktuelle Runtime-Evidenz

### Betroffene Mints

- `E7UaWyQoDgvUTvgQLxbR3oVyYpf3eq2hN95RzrwQpump`
- `GwQjXZvDTVVWyadJAvjx9upEZsFFToVQHY5NRrZ6wzTR`

### Produktionslauf heute

Kill-Switch / Liquidation:

- `execution-engine` startet den Liquidationsjob erfolgreich.
- Danach bleiben genau diese zwei Tokens nach dem ersten Pass im Wallet.
- `trade_logs/executions/execution_results-20260403.jsonl` zeigt fuer beide:
  - `dex="pump_amm"`
  - `sell_routing="multi_pool"`
  - `token_program="TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"`
  - `status="failed"`
  - `error_code="UiTransactionError(InstructionError(1, Custom(6023)))"`

### Decision Records

`trade_logs/decisions/decision_records-20260403.jsonl`:

- `dec-fa501bf7-294603`
- `dec-fa501bf7-294604`

Beide haben:

- `tx_plan` = passed
- `simulation` = failed
- `reason_code = "SIM_FAILED"`
- `primary_reject_reason = UiTransactionError(InstructionError(1, Custom(6023)))`

### Simulationsspur

Die `logs_preview` zeigen bei beiden praktisch denselben Verlauf:

- `AToken ... CreateIdempotent` erfolgreich
- `Pump AMM ... Instruction: Sell`
- `pfee ... Instruction: GetFees` erfolgreich
- mehrere `TransferChecked` erfolgreich
- erst danach:
  - `AnchorError thrown in programs/pump-amm/src/instructions/swap/sell.rs:206`
  - `Error Code: Overflow`
  - `Error Number: 6023`

Wichtiger Schluss:

- Das ist **kein** frueher Discovery-Fail mehr.
- Das ist auch **kein** offensichtlicher `6013 InvalidProtocolFeeRecipient`-Fail mehr.
- Der verwendete Sell-Pfad ist formell valide genug, um tief in `sell.rs` zu laufen, kippt aber dort semantisch.

## Bisherige Root-Cause-Eingrenzung

### Was bereits eher ausgeschlossen ist

1. **Zu spaetes Helius-Gate / Discovery-Fail**
   - Scope 43 wurde gemergt und deployed.
   - Dieser Lauf kommt bis zur Simulation.

2. **Offensichtlicher Token-2022-Programm-Fehler im Sell-Builder**
   - Der aktuelle GitHub-Stand von `build_swap_ix_from_pool_accounts(...)` setzt `base_token_program` dynamisch.
   - Es gibt bereits einen gezielten Test fuer den Token-2022-Sell-Pfad.

3. **Reiner `6013`-Pfad**
   - Aktueller Lauf zeigt `6023 Overflow`, nicht `6013`.

### Wahrscheinlichste Root-Cause-Klasse

Der aktuell wahrscheinlichste Fehler ist:

- ein **semantisch falscher, aber formal gueltiger** PumpSwap-14er-Account-Satz
- oder ein eng benachbarter Recovery-/Builder-Schritt, der diesen Satz fuer den Sell benutzt

Besonders verdaechtig sind weiterhin pool-spezifische Sell-Accounts, die im RPC-/heuristischen Parse-Pfad rekonstruiert werden:

- `protocol_fee_recipient`
- `protocol_fee_recipient_ta`
- `coin_creator_vault_ata`
- `coin_creator_vault_authority`
- ggf. `global_volume_accumulator` / `fee_config` / `fee_program`, falls ein formell valider, aber semantisch falscher Satz entsteht

## Warum dieser Diagnose-Scope noetig ist

Der aktuelle Stand in `pumpfun_amm.rs` ist fuer PumpSwap-Rekonstruktion nicht komplett deterministisch:

- es gibt feste / kanonische Teile
- aber weiterhin auch heuristische Rekonstruktion aus:
  - Market-Bytes
  - `global_config`
  - gescannten Kandidaten-Pubkeys
  - abgeleiteten ATAs
  - Authority-Kandidaten

Dadurch kann ein Satz entstehen, der:

- fruehe Account-Checks besteht,
- `GetFees` und erste Transfers bestehen laesst,
- aber spaeter im eigentlichen Sell mit `6023 Overflow` scheitert.

Genau diesen Punkt soll dieser Scope beweisbar machen.

## Ziel dieses Scopes

Der Scope soll **den exakten Ursprung des im fehlschlagenden Sell verwendeten PumpSwap-Account-Satzes beweisen**.

Konkret soll am Ende klar sein:

1. **Welcher exakte 14er-Account-Satz** fuer den Sell verwendet wurde
2. **Aus welcher Quelle** dieser Satz kam:
   - bestehender Cache
   - `force_refresh` fast path mit `pool_address_hint`
   - heuristische Rekonstruktion
   - lokaler TX-History-Pfad
   - externer bounded Fallback
3. **Welche Teilfelder deterministisch** und welche **heuristisch** aufgeloest wurden
4. Ob sich der verwendete Satz von einer **erfolgreichen Mainnet-Referenz-Sell-TX desselben Pools** unterscheidet

## Relevante Invarianten (Volltext)

### I-4 Hot Path = Geyser-First
HOT PATH (Discovery, Buy, Sell, Monitoring): GEYSER-ONLY. Keine neuen blockierenden RPC-Calls. Wenn ein Pfad sowohl Hot als auch Cold Path beruehrt, darf der Fix keinen neuen blockierenden Engine-RPC oder externen RPC in den Hot Path schleusen.

### I-5 Cold Path
COLD PATH (Liquidation, Manual Actions, Bootstrap): RPC erlaubt. Safety und correctness vor Speed. Autoritativer On-Chain-State darf hier nachgeladen werden.

### I-7 Hot-Path RPC-Freiheit
Nie RPC im normalen Trading-Hot-Path ohne explizite Freigabe. Keine neue Diagnostik, die versehentlich im Hot Path unbedingte RPCs ausloest.

### I-9 Simulation-Gate
Wenn Simulation fehlschlaegt, darf keine Transaktion gesendet werden. Dieser Scope darf das Simulations-Gate nicht aufweichen oder bypassen.

### I-12 Decision Record
Wenn Diagnostik oder ein enger Zusatzpfad fehlschlaegt, muessen Decision-Record und bestehende Reject-Pfade erhalten bleiben. Keine stille Ablehnung.

### I-24d Cold-Path Discovery nur per Request/Reply
`execution-engine` darf fehlende oder unbrauchbare PumpSwap-`pool_accounts` im Cold Path weder selbst discovern noch lokal als Truth in den SLAVE Cache schreiben. Discovery, MASTER-Write und JetStream-Publikation bleiben bei `market-data`. `execution-engine` darf nur den korrelierten Request/Reply-Pfad anstossen und bounded auf die autoritative Antwort warten.

## Bestehendes Pattern

### A. Diagnose vor Fix

Bitte diesem Pattern folgen:

- keine spekulative Korrektur zuerst
- zuerst exakte Provenienz des verwendeten Sell-Account-Satzes sichtbar machen
- dann gegen funktionierende Referenz vergleichen
- erst danach, wenn der Root Cause **eindeutig** ist, einen Folge-Scope fuer den eigentlichen Fix ableiten

### B. Cold-Path Recovery bleibt in `market-data`

Bestehendes Architekturpattern beibehalten:

- `execution-engine` bleibt Client
- `market-data` bleibt Autoritaet fuer Refresh / Discovery / MASTER / JetStream
- keine lokale Engine-Truth fuer PumpSwap-Accounts

### C. Referenz gegen echte Mainnet-Sell-TX

Bestehendes Bug-Pattern fuer DEX-Account-Probleme:

- nicht nur gegen Vermutungen pruefen
- sondern gegen **erfolgreiche Mainnet-Referenz-TX desselben Pools**, falls technisch machbar

## Relevante Bug-Patterns

- `KNOWN_BUG_PATTERNS.md` #19
  - kein Fix ohne harte Runtime-Evidenz
- `KNOWN_BUG_PATTERNS.md` #20
  - DEX-Account-Order nur gegen echte Mainnet-Referenz fixen
- `KNOWN_BUG_PATTERNS.md` #34
  - Cold-Path Recovery darf nicht erneut denselben kaputten Zustand liefern
- `KNOWN_BUG_PATTERNS.md` #35
  - keine globale Kanonisierung von `protocol_fee_recipient`
- `KNOWN_BUG_PATTERNS.md` #36
  - Cache-Hit ist nicht automatisch `ready`

OpenBrain:

- Es gibt keinen starken direkten Memory-Treffer fuer genau diesen PumpSwap-`6023`-Fall.
- Der naechstliegende allgemeine Treffer ist nur das generische Muster `Account Order mismatch`.
- Fuer diesen Scope ist daher die heutige Runtime-Evidenz wichtiger als alte Annahmen.

## Erwartete Aenderung

Bitte arbeite in dieser Reihenfolge:

### A. Exakten verwendeten Sell-Account-Satz sichtbar machen

Baue gezielte, knappe Diagnostik ein, die fuer den fehlschlagenden PumpSwap-Sell sichtbar macht:

- welchen finalen 14er-Account-Satz `market-data` publiziert / rekonstruiert hat
- welchen Satz `execution-engine` / `tx_builder` tatsaechlich fuer den Simulationslauf benutzt
- ob der Satz aus:
  - Cache
  - `force_refresh`
  - `pool_address_hint` fast path
  - heuristischem Parse
  - lokalem TX-history
  - externem bounded Fallback
  stammt

Wichtig:

- Logs muessen konkret, aber nicht spammy sein
- nur fuer den Cold-Path-Diagnosefall
- keine massenhafte Dauer-Verbose-Logik fuer den Hot Path

### B. Provenienz pro kritischem Teilaccount sichtbar machen

Bitte insbesondere kenntlich machen, wie diese Felder bestimmt wurden:

- `protocol_fee_recipient`
- `protocol_fee_recipient_ta`
- `coin_creator_vault_ata`
- `coin_creator_vault_authority`
- `global_volume_accumulator`
- `fee_config`
- `fee_program`

Ziel:

- nicht nur den Endsatz sehen
- sondern auch, **welcher Teil deterministisch** und **welcher Teil heuristisch** kam

### C. Vergleich gegen erfolgreiche Mainnet-Referenz ermoeglichen

Wenn es im Scope sauber und klein machbar ist:

- ergaenze eine enge Hilfsmoeglichkeit / Diagnostik, mit der ein erfolgreicher Referenz-Sell desselben Pools gegen den aktuell rekonstruierten Satz verglichen werden kann

Wichtig:

- kein unbounded Tooling
- keine neue Architektur
- kein grosser Debug-Framework-Umbau

Wenn ein vollautomatischer Referenzvergleich zu gross waere, ist auch ok:

- den aktuell verwendeten Satz
- und die Herkunft jedes kritischen Felds
- so zu loggen, dass der Supervisor ihn danach manuell mit einer erfolgreichen Mainnet-Sell-TX vergleichen kann

### D. Recovery-Pfad nicht verschlechtern

Falls du fuer die Diagnostik einen engen Zusatz im Recovery-/Retry-Pfad brauchst:

- nur minimal
- kein neues Verhalten im Hot Path
- kein Simulation-Bypass
- keine neue lokale Engine-Truth

## Akzeptanzkriterien

- Der Scope liefert harte Evidenz, **welcher exakte PumpSwap-Sell-Account-Satz** bei `6023` verwendet wurde
- Der Scope macht sichtbar, **woher** der Satz kam
- Fuer die kritischen Teilfelder ist sichtbar, welche davon heuristisch rekonstruiert wurden
- Die Logs/Evidenz reichen aus, um den Satz gegen eine erfolgreiche Mainnet-Referenz zu vergleichen
- Kein neuer Hot-Path-RPC
- Kein Simulation-Bypass
- Kein grosser Refactor
- Kein spekulativer "Fix vielleicht hilft's"-Commit ohne belegte Root Cause

## Erlaubte Dateien

- `Iron_crab/src/solana/dex/pumpfun_amm.rs`
- `Iron_crab/src/bin/market_data.rs`
- `Iron_crab/src/bin/execution_engine.rs` nur wenn fuer enge Cold-Path-Diagnostik oder Retry-Provenienz wirklich noetig
- `Iron_crab/src/execution/tx_builder.rs` nur wenn fuer die exakte final verwendete Sell-Account-Reihenfolge / Provenienz wirklich noetig
- kleiner benachbarter Test-/Hilfsabschnitt nur wenn eng begruendet

## Verboten

- Kein Eval-Repo
- Kein neuer lokaler Discovery-/Write-Pfad in `execution-engine`
- Kein neuer externer RPC im Hot Path
- Kein unbounded externer Scan
- Keine globale `protocol_fee_recipient`-Kanonisierung
- Kein breiter Multi-DEX-Refactor
- Kein Simulations-Bypass
- Kein spekulativer funktionaler Fix ohne belegte Root Cause

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
- welcher exakte 14er-PumpSwap-Account-Satz fuer die `6023`-Sells verwendet wurde
- aus welcher Quelle der Satz kam
- welche der kritischen Felder deterministisch vs. heuristisch bestimmt wurden
- ob der Satz von einer erfolgreichen Mainnet-Referenz-Sell-TX abweicht und falls ja: an welchen Positionen
- ob aus der Evidenz bereits ein klarer Folge-Fix-Scope ableitbar ist
- welche Tests / Checks gelaufen sind
