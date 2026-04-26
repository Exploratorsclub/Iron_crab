# Handoff: Scope 42 - PumpSwap bounded Helius fallback in market-data + strukturierte lokale Parse-Fail-Reason-Codes

WICHTIG: Lies und befolge die STOP-CHECK Regeln in AGENTS.md und .cursor/rules/ironcrab-core.mdc BEVOR du eine Datei aenderst. Wenn eine geplante Aenderung gegen eine Regel verstoesst, STOPPE sofort und melde den Verstoss statt die Aenderung durchzufuehren.

## Task-Beschreibung

Fixe den naechsten produktiven Cold-Path-Blocker fuer PumpSwap AMM bei zwei realen, bereits migrierten PumpFun -> PumpSwap Tokens.

Aktueller Produktionsstand nach Scope 41:

- `market-data` und `execution-engine` laufen auf dem Merge-Stand `cddc37c6` (PR #73 gemerged).
- Die Scope-41-Aenderung ist also deployt.
- Trotzdem scheitert die Liquidation fuer beide betroffenen Mints weiterhin **vor der Simulation** mit:
  - `ControlResponse(status=Error)`
  - `pump_amm pool discovery failed`
  - `QUOTE_UNAVAILABLE`

Der User hat fuer den naechsten Scope explizit entschieden:

1. **bounded Helius fallback**
2. **strukturierte lokale parse fail reason codes**

Wichtig dabei:

- Der Helius-Fallback muss **in `market-data`** passieren.
- Er darf **nur** anspringen, wenn der **lokale Validator** die benoetigten Daten nicht liefern kann.
- Kein Helius im Hot Path.
- Kein lokaler Discovery-/Write-Pfad im `execution-engine`.

## Aktueller Befund / Runtime-Evidenz

Betroffene Mints:

- `E7UaWyQoDgvUTvgQLxbR3oVyYpf3eq2hN95RzrwQpump`
- `GwQjXZvDTVVWyadJAvjx9upEZsFFToVQHY5NRrZ6wzTR`

### 1. Produktionslogs vom aktuellen Lauf nach Scope-41-Deploy

`execution-engine`:

- fuer `E7...pump`
  - `02:32:37Z`: `EnsurePumpAmmPoolAccounts` request
  - `02:33:20Z`: korrelierte `ControlResponse(status=Error)`
  - danach `LIQUIDATION SKIP ... pump_amm=err_discovery pump_amm pool discovery failed`
  - danach `Intent rejected ... reason=QUOTE_UNAVAILABLE`

- fuer `GwQ...zTR`
  - `02:33:20Z`: `EnsurePumpAmmPoolAccounts` request
  - `02:34:04Z`: korrelierte `ControlResponse(status=Error)`
  - danach derselbe `LIQUIDATION SKIP`
  - danach wieder `QUOTE_UNAVAILABLE`

Wichtiger Schluss:

- Es gibt in diesem Lauf **keine** Simulation fuer diese beiden Liquidationen.
- Es gibt in diesem Lauf **keinen** `6013`/`InvalidProtocolFeeRecipient`-Pfad.
- Der Fehler sitzt weiterhin im Discovery-/State-Reconstruction-Gate.

`market-data`:

- `EnsurePumpAmmPoolAccounts received/start`
- `LivePoolCache miss for pool discovery, falling back to RPC`
- am Ende:
  - `terminal outcome error (discovery failed) ... error=pump_amm pool discovery failed`

### 2. Der deployte Stand ist wirklich Scope 41

Auf dem Server:

- Repo-HEAD: `cddc37c6a8f9790df2769afaf6103ce97d654051`
- letzter Commit:
  - `Merge pull request #73 from Exploratorsclub/cursor/scope41-pumpswap-static-parse-or-helius`

Die deployte `market-data` Binary enthaelt bereits die Scope-41-Strings:

- `protocol fee accounts from global_config fixed offset`
- `no Fee-Program PDA guessing`

### 3. Lokaler Validator: Markt-Findung funktioniert, TX-History weiterhin nicht

Direkt am Produktions-Validator `127.0.0.1:8899` geprueft:

- `getProgramAccounts(pAMMBay6..., memcmp base_mint + WSOL)` liefert weiterhin jeweils **genau 1** Markt:
  - `E7...pump` -> `B8bvg3KzXzGAq51QjirhPTw5ChhiZWn2kNwvQd3YZFN8`
  - `GwQ...zTR` -> `5rNMGrJ3V2vUY3GAuxiVKZmKCn6c5N6n7Ld5EWvgceVX`

- `getSignaturesForAddress(...)` liefert fuer diese beiden Pool-Maerkte weiterhin:
  - `0`
  - `0`

Wichtiger Schluss:

- Der lokale Validator kann die Pool-Maerkte finden.
- Der lokale Validator liefert fuer die benoetigte TX-History weiterhin nichts Verwendbares.

### 4. Scope-41-Logstrings tauchen im Runtime-Fail nicht sichtbar auf

In den Produktionslogs des fehlgeschlagenen Laufs tauchen **keine** klaren inneren Parse-Fail-Strings aus `pumpfun_amm.rs` auf, z. B.:

- `protocol fee accounts from global_config fixed offset`
- `pump_amm market parse FAIL: no protocol fee recipient token account`
- `pump_amm market parse FAIL: protocol_fee_recipient unresolved after market + global_config paths`
- `pump_amm market parse FAIL: no creator vault token account`
- `pump_amm market parse FAIL: fee_config or global_volume_accumulator is default`

Das bedeutet:

- Die innere Ursache bleibt fuer Supervisor/User derzeit zu grob (`pump_amm pool discovery failed`).
- Genau deshalb braucht dieser Scope strukturierte lokale Reason-Codes.

## Ziel dieses Scopes

1. **Den lokalen Failure-Pfad sichtbar machen**
   - `market-data` soll bei lokalem Parse-/Discovery-Fail den **konkreten Grundcode** loggen.

2. **Bounded Helius fallback in `market-data` bauen**
   - nur im Cold Path
   - nur wenn der lokale Validator die benoetigten Daten nicht liefern konnte
   - nur nach lokalem Pool-Markt-Find und lokalem Parse-Fail
   - kein primaerer Discovery-Pfad fuer normale Faelle

3. **Die bestehende Architekturgrenze beibehalten**
   - `execution-engine` bleibt Request/Reply-Client
   - `market-data` bleibt Autoritaet fuer Discovery, MASTER-Write und `PoolCacheUpdate`

## Relevante Invarianten (Volltext)

### I-4 Hot Path = Geyser-First
HOT PATH (Discovery, Buy, Sell, Monitoring): GEYSER-ONLY. Keine neuen blockierenden RPC-Calls. Wenn ein Pfad sowohl Hot als auch Cold Path beruehrt, darf der Fix keinen neuen blockierenden Engine-RPC oder externen RPC in den Hot Path schleusen.

### I-5 Cold Path
COLD PATH (Liquidation, Manual Actions, Bootstrap): RPC erlaubt. Safety und correctness vor Speed. Autoritativer On-Chain-State darf hier nachgeladen werden.

### I-7 Hot-Path RPC-Freiheit
Nie RPC im normalen Trading-Hot-Path ohne explizite Freigabe. Kein lokaler Discovery-RPC im `execution-engine`.

### I-9 Simulation-Gate
Wenn Simulation fehlschlaegt, darf keine Transaktion gesendet werden. Dieser Scope darf nur Discovery/State-Reconstruction verbessern, nicht die Simulation umgehen.

### I-12 Decision Record
Wenn lokaler Parse und auch der bounded Helius fallback scheitern, muss der bestehende Reject-/Decision-Record-Pfad erhalten bleiben. Keine stille Ablehnung.

### I-24d Cold-Path Discovery nur per Request/Reply
`execution-engine` darf fehlende oder unbrauchbare PumpSwap-`pool_accounts` im Cold Path weder selbst discovern noch lokal als Truth in den SLAVE Cache schreiben. Discovery, MASTER-Write und JetStream-Publikation bleiben bei `market-data`. `execution-engine` darf nur den korrelierten Request/Reply-Pfad anstossen und bounded auf die autoritative Antwort warten.

## Bestehendes Pattern

### A. Autoritative Cold-Path Request/Reply in `market-data`

Bestehendes Pattern bei anderen DEXen in `src/bin/market_data.rs`:

- `EnsureRaydiumCpmmPoolState`, `EnsureRaydiumAmmPoolState`, `EnsureOrcaWhirlpoolPoolState`, `EnsureMeteoraDlmmPoolState`
- Semantik:
  - cache-first nur wenn `force_refresh=false`
  - bei `force_refresh=true` immer der RPC-Refresh-Pfad
  - am Ende explizite `ControlResponseStatus::Ok` oder `ControlResponseStatus::Error`

PumpSwap muss dieselbe Architektur behalten:

- kein lokaler Recovery-RPC in `execution-engine`
- keine Engine-seitige Heilung des SLAVE-Cache
- `market-data` publiziert den autoritativen `ControlResponse`

### B. Lokaler PumpSwap-Parse in `pumpfun_amm.rs`

Aktueller lokaler Ablauf in `src/solana/dex/pumpfun_amm.rs`:

1. `discover_pool_static(...)`
2. Markt-Findung lokal (`getAccount` Fast-Path fuer bekannte Pools, sonst `getProgramAccounts`)
3. `try_parse_pool_static_from_market_account_inner(...)`
4. wenn das nichts Verwendbares ergibt -> `discover_pool_static_via_tx_history_market_only(...)`

Der neue Scope soll **nicht** diesen Ablauf ersetzen, sondern:

- lokale Failure-Grundcodes sichtbar machen
- danach einen **engen externen Cold-Path-Fallback** nur fuer den echten lokalen Datenluecken-Fall ergaenzen

### C. Bestehende Helius-Konfiguration wiederverwenden

Es gibt bereits Helius-Kontext im Repo:

- `src/config.rs`: `solana.helius_rpc_url`
- `config.example.toml`: Helius RPC als optionale Konfiguration dokumentiert

Bitte dieses bestehende Config-Pattern wiederverwenden, statt eine zweite parallele Helius-Konfiguration zu erfinden.

## Relevante Bug-Patterns

- `KNOWN_BUG_PATTERNS.md` #19:
  - Kein Symptomfix ohne Root Cause / Runtime-Evidenz.
- `KNOWN_BUG_PATTERNS.md` #31:
  - Bekannte Pools nicht unnoetig ueber globale Scans behandeln.
- `KNOWN_BUG_PATTERNS.md` #32:
  - Keine Rueckkehr zu validator-index-abhaengigen Owner-/ATA-Fallbacks.
- `KNOWN_BUG_PATTERNS.md` #34:
  - Cold-Path Recovery darf nicht cache-first denselben kaputten Zustand erneut liefern.
- `KNOWN_BUG_PATTERNS.md` #35:
  - Keine globale Kanonisierung von `protocol_fee_recipient`.
- `KNOWN_BUG_PATTERNS.md` #36:
  - Cache/Teilzustand ist nicht automatisch `ready`.

## Erwartete Aenderung

Bitte arbeite in dieser Reihenfolge:

### A. Strukturierten lokalen Parse-Fail sichtbar machen

Fuehre in `pumpfun_amm.rs` fuer den lokalen Cold-Path-Discovery-/Parse-Pfad strukturierte Fail-Reasons ein.

Ziel:

- Wenn `try_parse_pool_static_from_market_account_inner(...)` oder der umgebende lokale Discovery-Pfad scheitert, soll am Ende nicht nur `None` / generisch `pump_amm pool discovery failed` entstehen.
- Stattdessen soll `market-data` einen **konkreten Reason-Code** sehen und loggen koennen.

Beispiele fuer Reason-Codes (Namen duerfen leicht abweichen, aber bitte strukturiert und stabil):

- `pool_market_not_found`
- `pool_market_owner_mismatch`
- `pool_market_layout_mismatch`
- `protocol_fee_recipient_missing`
- `protocol_fee_recipient_ta_missing`
- `creator_vault_missing`
- `fee_config_missing`
- `global_volume_accumulator_missing`
- `tx_history_unavailable`
- `helius_unconfigured`
- `helius_failed`

Wichtig:

- Kein reines String-Chaos.
- Lieber ein kleiner enum / klarer strukturierter Fehlertyp mit sauberer Umwandlung in Logs.
- Der finale `ControlResponse(error=...)` darf weiterhin kompakt bleiben, aber die Logs muessen den Grund klar zeigen.

### B. Bounded Helius fallback nur in `market-data`

Falls der lokale Pfad scheitert, baue einen **engen** Helius-Fallback mit diesen Bedingungen:

1. **Nur in `market-data`**
2. **Nur Cold Path**
3. **Nur nachdem**:
   - der Pool-Markt lokal gefunden wurde
   - der lokale Parse keinen verwendbaren `PumpAmmPoolStatic` bauen konnte
   - und der lokale tx-history fallback unbrauchbar ist oder keine Signaturen liefert
4. **Nur wenn `helius_rpc_url` konfiguriert ist**
5. **Bounded**
   - kurzer Timeout
   - keine Endlosschleife
   - keine unbounded Retries
6. **Nicht als primaere Quelle**
   - lokale Validatordaten bleiben immer erste Wahl

Wichtig:

- Der Helius-Fallback soll nur anspringen, wenn der **lokale Validator die benoetigten Daten nicht liefern kann**, nicht schon bei einem normalen lokalen Cache-Miss.
- Bitte moeglichst den **kleinsten noetigen externen Zugriff** verwenden.
- Wenn es reicht, genau eine Referenz-TX oder genau ein fehlendes Account-Detail von Helius zu holen, dann bitte genau das tun statt eines breiten externen Discovery-Scans.

### C. Erfolgsweg unveraendert autoritativ machen

Wenn der Helius-Fallback einen vollstaendigen, verwendbaren `PumpAmmPoolStatic` liefert:

- `market-data` bleibt Autoritaet
- MASTER-/JetStream-Write-Pfad bleibt derselbe wie bei lokal erfolgreichem Discovery
- `execution-engine` bekommt normal `ControlResponseStatus::Ok`

### D. Failure-Pfad sauber halten

Wenn auch Helius den Satz nicht liefern kann:

- bestehender `ControlResponseStatus::Error`-Pfad bleibt erhalten
- aber mit klaren Logs:
  - lokaler parse fail reason
  - lokaler tx-history unavailable
  - Helius fallback attempted
  - Helius fallback success/failure

## Akzeptanzkriterien

- Der lokale PumpSwap-Discovery-/Parse-Fail fuer die beiden produktiven Tokens ist in Logs **konkret** benannt, nicht nur generisch.
- Es gibt einen bounded Helius fallback **nur in `market-data`**.
- Der Fallback springt **nur** an, wenn der lokale Validator die benoetigten Daten nicht liefern konnte.
- Kein Helius im Hot Path.
- Kein lokaler Engine-RPC.
- Keine globale Kanonisierung von `protocol_fee_recipient`.
- Simulation-Gate unveraendert.
- Bestehender Request/Reply-Architekturvertrag bleibt intakt.

## Erlaubte Dateien

- `Iron_crab/src/solana/dex/pumpfun_amm.rs`
- `Iron_crab/src/bin/market_data.rs`
- `Iron_crab/src/config.rs`
- `Iron_crab/config.example.toml` nur wenn fuer Dokumentation des optionalen Helius-Fallbacks wirklich noetig
- kleine benachbarte Hilfsdatei nur wenn sauber begruendet

## Verboten

- Keine Aenderungen im Eval-Repo
- Kein neuer lokaler Discovery-/Write-Pfad in `execution-engine`
- Kein Helius im Hot Path
- Kein unbounded externer Fallback
- Keine globale `protocol_fee_recipient`-Kanonisierung
- Keine Rueckkehr zu `getTokenAccountsByOwner`-/validator-index-abhaengigen Fallbacks
- Kein grosser Multi-DEX-Refactor
- Kein Commit von realen API-Keys / Secrets / serverlokalen Konfigurationswerten

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
- welcher konkrete lokale Parse-Reason-Code fuer die zwei produktiven Pools sichtbar wurde
- unter welchen **engen** Bedingungen der Helius-Fallback anspringt
- welcher minimale externe Datenzugriff genutzt wird
- wie der Erfolg wieder in den normalen `market-data` -> MASTER -> JetStream -> `ControlResponse`-Pfad integriert wird
- welche Tests / Checks gelaufen sind
