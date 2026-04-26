# Handoff: Scope 50 - PumpSwap force_refresh muss aggregator-dominierte SELL-Historie dekodieren statt sie als `no_sell_candidates` zu verwerfen

WICHTIG: Lies und befolge die STOP-CHECK Regeln in `AGENTS.md` und `.cursor/rules/ironcrab-core.mdc` BEVOR du eine Datei aenderst. Wenn eine geplante Aenderung gegen eine Regel verstoesst, STOPPE sofort und melde den Verstoss statt die Aenderung durchzufuehren.

## Task-Beschreibung

Nach Merge von Scope 49 und Redeploy ist die aktuelle Root Cause jetzt enger bewiesen:

- `execution-engine` triggert bei strukturellem PumpSwap-Sim-Fail korrekt `EnsurePumpAmmPoolAccounts(force_refresh=true)`.
- `market-data` fuehrt den bounded Cold-Path-Refresh aus.
- lokaler Verlauf ist fuer die betroffenen Pools leer.
- der externe Helius-Fallback laeuft diesmal **nicht** in Timeout, **nicht** in `429`, **nicht** in Budget-Exhaustion.
- stattdessen liefert der externe Fallback erfolgreich Signaturen und Transaktionen, aber der Beobachter erkennt darin **keine** verwertbaren PumpSwap-SELL-Kandidaten:
  - `ext_provider_status="ok"`
  - `ext_signatures_returned=40`
  - `ext_transactions_fetched=39` bzw. `32`
  - `ext_pump_amm_ix_seen=0`
  - `ext_sell_candidates_seen=0`
  - `termination_reason=local_history_empty_external_no_sell_candidates`

Der User hat zusaetzlich bestaetigt:

- auf Solscan sind fuer diese beiden Tokens fast nur SELLs ueber Aggregatoren / Router sichtbar
- regulaere direkte PumpSwap-SELLs sind dort kaum oder gar nicht zu sehen

Damit ist die wahrscheinlichste Root Cause fuer den noch offenen force-refresh-Blocker:

1. Helius liefert brauchbare TX-Historie
2. aber der bounded Observer dekodiert die **tatsaechliche PumpSwap-SELL-Form in Aggregator-/Router-TXs** nicht korrekt
3. deshalb bleibt `sell_layout_ready=false`
4. `market-data` publiziert `ControlResponse status=Error`
5. `execution-engine` bekommt keinen autoritativ aufgeloesten SELL-Layout-Refresh und die Simulation endet weiter in `Custom(6023)`

Ziel dieses Scopes:

1. Den bounded Cold-Path-Observer so erweitern, dass er **aggregator-dominierte PumpSwap-SELL-Historie** erkennt, statt sie pauschal als `no_sell_candidates` wegzuwerfen.
2. Den Fix strikt auf den bestehenden force-refresh / Helius-History-Pfad begrenzen.
3. Keine spekulative "rate mal das Layout aus dem Cache"-Heuristik einfuehren.
4. Keine Ausweitung der Request-Budgets als primaeren Fix.

## Harte Runtime-Evidenz

Betroffene Pools / Mints:

- Mint `E7UaWyQoDgvUTvgQLxbR3oVyYpf3eq2hN95RzrwQpump`
  - Pool `B8bvg3KzXzGAq51QjirhPTw5ChhiZWn2kNwvQd3YZFN8`
- Mint `GwQjXZvDTVVWyadJAvjx9upEZsFFToVQHY5NRrZ6wzTR`
  - Pool `5rNMGrJ3V2vUY3GAuxiVKZmKCn6c5N6n7Ld5EWvgceVX`

Relevante Logs vom neuen Lauf:

### market-data

Fuer beide Pools:

- `local_history_probe=empty`
- `external_attempted=true`
- `ext_provider_status="ok"`
- `ext_signatures_returned=40`
- `ext_transactions_fetched=39` bzw. `32`
- `ext_pump_amm_ix_seen=0`
- `ext_sell_candidates_seen=0`
- `termination_reason=local_history_empty_external_no_sell_candidates`
- `sell_layout_ready=false after force_refresh`
- `force_refresh result published as Partial (authoritative SELL layout unresolved)`
- `ControlResponse published ... status=Error`

### execution-engine

Danach fuer beide Intents weiter:

- Simulationspfad endet erneut in `UiTransactionError(InstructionError(1, Custom(6023)))`

Wichtig:

- Das ist **kein** Timeout-/Rate-Limit-Fall mehr.
- Das ist **kein** "Helius hat nichts geliefert"-Fall.
- Das ist ein **TX-Inhalts-/Dekodierungsproblem** im Observer.

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
Wenn ein PumpSwap-Sell wegen fehlender autoritativer Layout-Information scheitert, darf der Intent nicht still verschwinden. Bestehende Decision-/Failure-Pfade muessen erhalten bleiben.

### I-24d Cold-Path Discovery nur per Request/Reply
`execution-engine` darf fehlende oder unbrauchbare PumpSwap-Accounts im Cold Path weder selbst discovern noch lokal als Truth in den SLAVE Cache schreiben. Discovery, MASTER-Write und JetStream-Publikation bleiben bei `market-data`. `execution-engine` darf nur den korrelierten Request/Reply-Pfad anstossen und bounded auf die autoritative Antwort warten.

## Relevante Bug-Patterns

- `KNOWN_BUG_PATTERNS.md` #14
  - PumpSwap SELL-Parsing darf nicht wieder an einer zu starren Account-Count-Annahme haengen.
- `KNOWN_BUG_PATTERNS.md` #19
  - Kein Symptomfix ohne harte Runtime-Evidenz.
- `KNOWN_BUG_PATTERNS.md` #20
  - DEX-Account-Order nur gegen echte Mainnet-Referenz-TXs und reale On-Chain-Form fixen.
- `KNOWN_BUG_PATTERNS.md` #34
  - Cold-Path force_refresh darf nicht erneut nur denselben unbrauchbaren Teilzustand liefern.
- `KNOWN_BUG_PATTERNS.md` #36
  - Teilzustand / Cache-Hit ist nicht automatisch `ready`.

## Bestehendes Pattern

Bitte direkt auf dem bestehenden bounded force-refresh / SELL-layout-Observer aufsetzen:

- `resolve_authoritative_sell_layout_for_force_refresh(...)`
- `observe_authoritative_sell_layout_from_tx_history_with_rpc(...)`
- die existierenden Helper fuer:
  - `parse_account_keys(...)`
  - `extend_with_loaded_addresses(...)`
  - `collect_all_instructions(...)`
- die bestehende Scope-49-Telemetrie in `PumpAmmForceRefreshSellLayoutDiag`

Wichtig fuer diesen Scope:

- Der Code sammelt bereits Top-Level- **und** `innerInstructions`.
- Der Fix soll deshalb nicht pauschal "inner instructions fehlen" annehmen, sondern die **tatsaechliche Aggregator-/Router-Form** gegen die aktuellen Parser-Annahmen pruefen.
- Wenn Helius fuer diese Router-TXs `accounts` anders liefert (z. B. andere Laenge, andere Darstellung, andere Program-Position oder nur indirekt dekodierbare PumpSwap-CPI-Form), dann muss genau diese Form bounded unterstuetzt werden.

## Erwartete Aenderung

Bitte arbeite in dieser Reihenfolge:

### A. Reale Aggregator-/Router-TX-Form gegen aktuelle Candidate-Logik pruefen

Bevor du den Code aenderst, pruefe fuer die betroffenen Pools anhand echter externer TX-Historie:

1. ob PumpSwap-SELLs als Top-Level-Ix oder nur als CPI / `innerInstructions` auftauchen
2. wie `accounts` dort konkret repraesentiert sind
3. ob die aktuelle Candidate-Logik an
   - harter Account-Laenge,
   - falscher Index-Annahme,
   - Program-ID-Position,
   - oder alternativem Helius-Format scheitert

Wenn deine Untersuchung zeigt, dass die aktuelle Annahme `accounts.len() == 21` fuer den externen Observer zu eng oder an der falschen Stelle angewandt wird, dann behebe **genau das** und nichts Groesseres.

### B. Bounded Support fuer die reale SELL-Candidate-Form

Erweitere den externen SELL-layout-Observer so, dass er fuer diese realen Router-/Aggregator-TXs mindestens:

1. PumpSwap-Instruktionen in der gelieferten Repräsentation erkennt
2. gueltige SELL-Kandidaten davon trennt, was nur Router-Huelle ist
3. bei erfolgreichem Treffer das autoritative SELL-Layout wie bisher aus einem echten beobachteten Candidate ableitet

Erlaubt:

- kleine Helper fuer Instruction-/Account-Normalisierung
- Unterstuetzung der real beobachteten alternativen Account-Repräsentation
- Unterstuetzung der real beobachteten alternativen SELL-Candidate-Form
- kleine, gezielte Erweiterung der Telemetrie fuer "ix gesehen aber nicht dekodierbar"

Nicht das Ziel:

- unbounded History-Scan
- neues heuristisches Layout-Raten nur aus Cache / Pool-State
- neue RPC-Quelle
- Hot-Path-Healing in `execution-engine`
- grosser Umbau der gesamten PumpSwap-Architektur

### C. Praezisere Klassifikation falls noch kein Layout ableitbar ist

Falls nach der Erweiterung weiter kein Layout ableitbar ist, soll die Telemetrie **praeziser** werden als das heutige generische `no_sell_candidates`.

Beispiele fuer akzeptable neue Klassifikationen:

- `router_shell_seen_but_no_pump_amm_cpi`
- `pump_amm_ix_seen_but_account_shape_unsupported`
- `pump_amm_sell_candidate_seen_but_layout_not_derivable`

Wichtig:

- Nur stabile, strukturierte Klassifikation
- keine losen Debug-Prints
- keine Log-Spam-Schleifen

## Akzeptanzkriterien

- Fuer mindestens einen der beiden produktiven Pools ist im force-refresh-Log sichtbar, dass der Observer in der externen History jetzt **PumpSwap-Ix oder SELL-Kandidaten** erkennt, statt bei `ext_pump_amm_ix_seen=0` stehen zu bleiben.
- Wenn reale SELL-Candidates vorhanden sind, kann der Observer daraus das autoritative SELL-Layout aufloesen und `sell_layout_ready=true` erreichen.
- Wenn trotz realer Aggregator-Historie weiter kein Layout ableitbar ist, ist aus der Telemetrie klar lesbar, **welche konkrete Form** noch nicht dekodiert wird.
- Kein neuer Hot-Path-RPC.
- Kein Simulations-Bypass.
- Keine Aufweichung von I-24d.
- Keine Verbreiterung des Helius-Budgets als primaerer Fix.

## Erlaubte Dateien

- `src/solana/dex/pumpfun_amm.rs`
- `src/bin/market_data.rs` nur falls fuer stabile Klassifikation / Logging wirklich noetig
- kleine, direkt zugehoerige Tests oder Hilfsfunktionen im Impl-Repo

## Verboten

- Keine Aenderungen in `execution-engine`
- Kein Eval-Repo
- Kein unbounded Helius-Scan
- Kein neuer synchroner RPC-Pfad fuer regulaere Buys/Sells
- Kein spekulativer Pool-State-Only-Fallback, der ohne echte Beobachtung `sell_layout_ready` erzwingt
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
- welche reale Aggregator-/Router-Form in der TX-Historie beobachtet wurde
- woran die bisherige Candidate-Logik konkret gescheitert ist
- welche kleine Parser-/Observer-Erweiterung du implementiert hast
- wie sich `ext_pump_amm_ix_seen`, `ext_sell_candidates_seen` und `termination_reason` dadurch veraendern
- welche Tests / Checks gelaufen sind
