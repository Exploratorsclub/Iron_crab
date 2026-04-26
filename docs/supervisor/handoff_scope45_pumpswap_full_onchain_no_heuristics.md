# Handoff: Scope 45 - PumpSwap v14 nur aus autoritativen Onchain-Daten, keine Heuristiken mehr

WICHTIG: Lies und befolge die STOP-CHECK Regeln in `AGENTS.md` und `.cursor/rules/ironcrab-core.mdc` BEVOR du eine Datei aenderst. Wenn eine geplante Aenderung gegen eine Regel verstoesst, STOPPE sofort und melde den Verstoss statt die Aenderung durchzufuehren.

## Task-Beschreibung

Der User hat explizit klargestellt:

- PumpSwap-`pool_accounts` im Cold Path sollen **nicht mehr heuristisch rekonstruiert** werden.
- Der verwendete v14-Satz soll **vollstaendig aus echten Onchain-Daten** gebaut werden.
- Wenn ein Feld nicht autoritativ aus Onchain-Daten bestimmt werden kann, darf **kein geratenes / heuristisches** `pool_accounts`-Set publiziert werden.

Der naechste Scope ist daher **kein enger Einzel-Fix fuer nur ein Feld**, sondern ein Architektur-Fix fuer den PumpSwap-Cold-Path:

- `market-data` darf fuer PumpSwap im Cold Path nur noch einen v14-Satz publizieren, wenn **alle kritischen Felder autoritativ** belegt sind.
- Heuristische Rekonstruktion (`Heuristic`, `pda_seed_probe_*`, groesster Kandidat, owner/candidate guessing usw.) muss aus dem Cold-Path-Publish-Pfad entfernt werden.
- Falls die autoritative Ableitung fuer einen Pool nicht moeglich ist, muss der Request **sauber fehlschlagen**, statt einen semantisch falschen aber formal gueltigen Satz zu publizieren.

## Aktuelle Runtime-Evidenz

### Echte Liquidation heute

Der eigentliche Kill-Switch-Liquidationslauf begann um `16:20:59`:

- `control-plane.audit`: `KILL_SWITCH_ACTIVATED ... liquidate=True`
- `execution-engine`: `Starting liquidation job`
- 2 Liquidation-Intents wurden vorbereitet:
  - Mint `E7UaWyQoDgvUTvgQLxbR3oVyYpf3eq2hN95RzrwQpump`, Pool `B8bvg3KzXzGAq51QjirhPTw5ChhiZWn2kNwvQd3YZFN8`
  - Mint `GwQjXZvDTVVWyadJAvjx9upEZsFFToVQHY5NRrZ6wzTR`, Pool `5rNMGrJ3V2vUY3GAuxiVKZmKCn6c5N6n7Ld5EWvgceVX`

Beide Intents:

- laufen bis zur Simulation
- triggern danach `EnsurePumpAmmPoolAccounts(force_refresh=true)`
- bekommen `ControlResponse(status=Ok)`
- werden mit frisch hydratisierten Reserves neu gebaut
- scheitern **trotzdem** erneut mit:
  - `UiTransactionError(InstructionError(1, Custom(6023)))`

### Scope-44-Evidenz fuer beide Pools

Fuer beide problematischen Pools zeigen die Scope-44-Logs:

- `protocol_fee_recipient`: `Deterministic`
- `protocol_fee_recipient_ta`: `Deterministic`
- `coin_creator_vault_ata`: `MarketLayout`
- `coin_creator_vault_authority`: `MarketLayout`
- `fee_config`: `Deterministic`
- `fee_program`: `Deterministic`
- **`global_volume_accumulator`: `Heuristic`**
  - Tag: `pda_seed_probe_global_volume_accumulator`

Wichtiger Punkt:

- `market-data` hydriert die Vault-Reserves frisch per RPC
- `execution-engine` bekommt den frischen `ControlResponse`
- der **gleiche semantische Satz** bleibt bestehen
- damit ist der verbleibende Fehler sehr wahrscheinlich der weiterhin heuristische Teil des v14-Satzes

### Konkrete Runtime-Logs

Pool `B8bvg3KzXzGAq51QjirhPTw5ChhiZWn2kNwvQd3YZFN8`:

- `EnsurePumpAmmPoolAccounts(force_refresh=true)` erfolgreich
- `base_reserve=978293693023444`
- `quote_reserve=18265952287`
- `gva_field=Heuristic`
- Retry-Simulation endet erneut in `Custom(6023)`

Pool `5rNMGrJ3V2vUY3GAuxiVKZmKCn6c5N6n7Ld5EWvgceVX`:

- `EnsurePumpAmmPoolAccounts(force_refresh=true)` erfolgreich
- `base_reserve=983275135334710`
- `quote_reserve=18039115234`
- `gva_field=Heuristic`
- Retry-Simulation endet erneut in `Custom(6023)`

## Ziel dieses Scopes

Bitte stelle sicher:

1. Im PumpSwap-Cold-Path wird ein v14-Satz nur dann publiziert, wenn **alle kritischen Felder autoritativ aus Onchain-Daten** bestimmt wurden.
2. Heuristische Aufloesungen duerfen im Cold-Path-Publish-Pfad **nicht** mehr zu `Ok(pool_accounts)` fuehren.
3. Wenn eine autoritative Quelle fehlt, liefert `market-data` **Error/NotFound**, statt einen geratenen Satz zu emitten.
4. Scope-44-Diagnostik bleibt erhalten und zeigt danach fuer den Cold Path **keine `Heuristic`-Felder mehr** fuer erfolgreich publizierte PumpSwap-v14-Saetze.

## Was als "echte Onchain-Daten" fuer diesen Scope zaehlt

Erlaubt sind ausschliesslich Quellen, die direkt oder verifiziert aus Onchain-Staat stammen:

- bekannte Onchain-Account-Bytes (z. B. Market-Account, `global_config`, andere eindeutig adressierbare Accounts)
- verifizierte kanonische PDAs, wenn Seed + Ziel-Account fachlich belastbar sind und nicht nur "best effort guessed"
- direkt beobachtete erfolgreiche Onchain-Swap-Instruction-Accounts aus TX-History
  - lokal oder bounded ueber Helius
  - nur im Cold Path
- RPC-Reads auf eindeutig adressierte Accounts im Cold Path

Nicht erlaubt fuer den final publizierten Cold-Path-v14-Satz sind:

- groesster / "beste" Kandidat aus mehreren moeglichen Accounts
- owner-/candidate-Guessing
- pda-seed probing mit mehreren moeglichen Varianten ohne kanonischen Beweis
- heuristische Auswahl "embedded candidate", "largest data len", "try this seed and hope"
- jede Logik, die in Scope-44 weiter als `Heuristic` klassifiziert wuerde

## Relevante Invarianten (Volltext)

### I-4 Hot Path = Geyser-First
HOT PATH (Discovery, Buy, Sell, Monitoring): GEYSER-ONLY. Keine neuen blockierenden RPC-Calls. Wenn ein Pfad sowohl Hot als auch Cold Path beruehrt, darf der Fix keinen neuen blockierenden Engine-RPC oder externen RPC in den Hot Path schleusen.

### I-5 Cold Path
COLD PATH (Liquidation, Manual Actions, Bootstrap): RPC erlaubt. Safety und correctness vor Speed. Autoritativer On-Chain-State darf hier nachgeladen werden.

### I-7 Hot-Path RPC-Freiheit
Nie RPC im normalen Trading-Hot-Path ohne explizite Freigabe. Keine neue Diagnostik oder neue Onchain-Verifikation, die versehentlich im Hot Path unbedingte RPCs ausloest.

### I-9 Simulation-Gate
Wenn Simulation fehlschlaegt, darf keine Transaktion gesendet werden. Dieser Scope darf das Simulations-Gate nicht aufweichen oder bypassen.

### I-12 Decision Record
Wenn der Cold-Path-Request wegen fehlender autoritativer Daten fehlschlaegt, muessen Decision-Record und bestehende Reject-Pfade erhalten bleiben. Keine stille Ablehnung.

### I-24d Cold-Path Discovery nur per Request/Reply
`execution-engine` darf fehlende oder unbrauchbare PumpSwap-`pool_accounts` im Cold Path weder selbst discovern noch lokal als Truth in den SLAVE Cache schreiben. Discovery, MASTER-Write und JetStream-Publikation bleiben bei `market-data`. `execution-engine` darf nur den korrelierten Request/Reply-Pfad anstossen und bounded auf die autoritative Antwort warten.

## Bestehendes Pattern

### A. Kein heuristischer Satz mehr im Cold Path

Das Kern-Pattern fuer diesen Scope soll sein:

- normaler Cache-/Hot-Path bleibt unveraendert latent arm
- Cold-Path `force_refresh=true` = autoritative Onchain-Aufloesung
- wenn Autoritaet nicht vollstaendig hergestellt werden kann:
  - **kein** `Ok(pool_accounts)`
  - stattdessen sauberer Fehler

### B. Force-Refresh bleibt bei `market-data`

Bestehendes Architekturpattern beibehalten:

- `execution-engine` bleibt Client
- `market-data` bleibt Autoritaet
- kein lokaler Engine-Truth-Write
- kein Engine-seitiger RPC-Discovery-Ersatz

### C. Referenz an echte erfolgreiche Onchain-Sells

Fuer Felder, die nicht aus festen Layout-Offsets oder eindeutig adressierten Accounts kommen:

- benutze nur echte Onchain-Beobachtung
- bevorzugt erfolgreiche Swap-Instruction-Accounts derselben Pool-/Programmfamilie
- bounded only, Cold Path only

## Relevante Bug-Patterns

- `KNOWN_BUG_PATTERNS.md` #19
  - kein Fix ohne harte Runtime-Evidenz
- `KNOWN_BUG_PATTERNS.md` #20
  - DEX-Account-Order / Account-Satz nur gegen echte Mainnet-Referenz korrigieren
- `KNOWN_BUG_PATTERNS.md` #32
  - keine Validator-/Index-abhaengigen owner-scan-Heuristiken
- `KNOWN_BUG_PATTERNS.md` #34
  - Cold-Path Recovery darf nicht erneut denselben kaputten Satz liefern
- `KNOWN_BUG_PATTERNS.md` #35
  - keine globale Kanonisierung nicht-globaler Accounts
- `KNOWN_BUG_PATTERNS.md` #36
  - Cache-Hit ist nicht automatisch `ready`

OpenBrain-relevante Treffer:

- "Cold-Path Recovery ist cache-first statt echter force refresh" - bereits adressiert, jetzt naechster Schritt: keine heuristische Rest-Rekonstruktion mehr
- "Known real PumpSwap pools not resolving to complete pool_accounts in market-data" - weiterhin relevant
- "Validator tx-history unavailable ... parser depended on tx-history or heuristics" - wichtig fuer bounded cold-path TX-history als echte Onchain-Quelle statt heuristischem Ersatz

## Erwartete Aenderung

Bitte arbeite in dieser Reihenfolge:

### A. Alle kritischen PumpSwap-v14-Felder nach Autoritaetsstatus klassifizieren

Pruefe fuer den Cold-Path-Publish-Pfad systematisch:

- `protocol_fee_recipient`
- `protocol_fee_recipient_ta`
- `coin_creator_vault_ata`
- `coin_creator_vault_authority`
- `global_volume_accumulator`
- `fee_config`
- `fee_program`

Ziel:

- fuer jedes Feld exakt eine autoritative Quelle
- oder explizit "nicht autoritativ bestimmbar"

### B. Heuristische Aufloesungen aus dem finalen Cold-Path-Ok-Pfad entfernen

Insbesondere:

- `Heuristic` darf fuer erfolgreich publizierte PumpSwap-v14-Saetze im Cold Path nicht mehr vorkommen
- `pda_seed_probe_global_volume_accumulator` darf nicht mehr zu einem `Ok(pool_accounts)` fuehren
- falls das Feld derzeit nur heuristisch gefunden wird, muss der Code auf eine autoritative Quelle umgestellt werden

### C. Wenn notwendig: bounded echte Onchain-Beobachtung statt Guessing

Falls ein Feld nicht aus Market-Layout oder eindeutig adressierbarem Account stammt, ist folgendes erlaubt:

- erfolgreicher Onchain-Sell/Swap-Instruction-Account-Satz derselben Pool-Familie als Beobachtungsquelle
- bounded lokale TX-History
- bounded Helius-TX-History im Cold Path

Aber:

- keine unbounded Scans
- keine Hot-Path-Nutzung
- keine "if not found, then guess a PDA/candidate and continue"

### D. Fail closed statt falsch publizieren

Wenn ein PumpSwap-v14-Satz im Cold Path nicht vollstaendig autoritativ gebaut werden kann:

- Request sauber mit Fehler beenden
- keine MASTER-/JetStream-Publikation eines teilgeratenen Satzes
- Scope-44-Diagnostik soll den fehlenden autoritativen Baustein explizit zeigen

### E. Scope-44-Diagnostik beibehalten

Bitte die aktuelle Diagnostik erhalten und so anpassen, dass nach dem Fix fuer erfolgreich publizierte Cold-Path-Saetze sichtbar ist:

- `pfr_field`, `pfr_ta_field`, `cc_vault_ata_field`, `cc_auth_field`, `gva_field`, `fee_cfg_field`, `fee_prog_field`
- und dass fuer `Ok(pool_accounts)` kein Feld mehr `Heuristic` ist

## Akzeptanzkriterien

- Fuer den PumpSwap-Cold-Path werden erfolgreich publizierte v14-Saetze nur noch aus autoritativen Onchain-Daten gebaut
- `global_volume_accumulator` ist nicht mehr heuristisch
- generell darf ein erfolgreich publizierter Cold-Path-Satz keine `Heuristic`-Klassifikation mehr enthalten
- wenn ein Feld nicht autoritativ ableitbar ist, wird der Request sauber abgelehnt statt mit geratenem Satz beantwortet
- keine neue Engine-Truth
- kein neuer Hot-Path-RPC
- kein Simulation-Bypass
- kein grosser Multi-DEX-Refactor

## Erlaubte Dateien

- `Iron_crab/src/solana/dex/pumpfun_amm.rs`
- `Iron_crab/src/bin/market_data.rs`
- `Iron_crab/src/execution/tx_builder.rs` nur falls Diagnose-/Konsistenzpfad wirklich noetig
- `Iron_crab/src/bin/execution_engine.rs` nur falls fuer bestehende Cold-Path-Recovery-/Error-Semantik minimal noetig
- enge Tests im selben Repo, wenn sie genau diesen Cold-Path-Contract absichern

## Verboten

- Kein Eval-Repo
- Kein neuer lokaler Discovery-/Write-Pfad in `execution-engine`
- Kein Hot-Path-RPC
- Kein unbounded externer Scan
- Kein heuristischer Fallback im finalen Cold-Path-Ok-Pfad
- Kein spekulativer Blind-Fix ohne klare Autoritaetsquelle
- Keine Umgehung des bestehenden Simulation-Gates

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
- welche Felder jetzt aus welcher autoritativen Onchain-Quelle kommen
- ob fuer erfolgreich publizierte Cold-Path-Saetze noch irgendein Feld `Heuristic` ist
- falls nein: explizit bestaetigen
- falls doch: STOP melden und genau sagen welches Feld weiterhin nicht autoritativ aufloesbar ist
- welche Tests / Checks gelaufen sind
