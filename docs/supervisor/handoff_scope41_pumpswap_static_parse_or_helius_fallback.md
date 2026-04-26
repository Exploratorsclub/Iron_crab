# Handoff: Scope 41 - PumpSwap static-parse blocker exakt bestimmen, tx-history-frei fixen oder engen Helius-Cold-Path-Fallback bauen

WICHTIG: Lies und befolge die STOP-CHECK Regeln in AGENTS.md und .cursor/rules/ironcrab-core.mdc BEVOR du eine Datei aenderst. Wenn eine geplante Aenderung gegen eine Regel verstoesst, STOPPE sofort und melde den Verstoss statt die Aenderung durchzufuehren.

## Task-Beschreibung

Fixe den naechsten engen Produktionsfehler im PumpSwap-Cold-Path fuer zwei reale, bereits migrierte PumpFun -> PumpSwap Tokens.

Aktueller Produktionsstand nach Scope 40:

- Der fruehere `15s`-Timeout ist **nicht mehr** der blocker.
- `execution-engine` wartet jetzt lang genug und bekommt fuer beide Mints eine korrelierte `ControlResponse(status=Error)` von `market-data`.
- Die Liquidation scheitert weiterhin **vor der Simulation** mit `QUOTE_UNAVAILABLE`, weil `market-data` mit
  - `error=pump_amm pool discovery failed`
  antwortet.

Ziel dieses Scopes:

1. **Exakt bestimmen**, welcher Baustein von `PumpAmmPoolStatic` fuer die beiden realen Pools im lokalen Parse scheitert.
2. **Bevorzugt**: den Parse tx-history-frei reparieren, wenn der fehlende Account deterministisch aus Market-State / Global-Config / PDA-Regeln / weiteren on-chain Reads ableitbar ist.
3. **Nur falls das fuer diese Edge Cases nicht deterministisch moeglich ist**: einen sehr engen, bounded **Helius-Fallback im Cold Path von `market-data`** einfuehren.

Wichtig:

- Kein spekulativer Fix.
- Erst den exakten blocker im aktuellen Parse-Pfad bestimmen.
- Danach kleinsten sicheren Fix schneiden.

## Aktueller Befund / Runtime-Evidenz

Betroffene Mints:

- `E7UaWyQoDgvUTvgQLxbR3oVyYpf3eq2hN95RzrwQpump`
- `GwQjXZvDTVVWyadJAvjx9upEZsFFToVQHY5NRrZ6wzTR`

Per lokalem Validator `127.0.0.1:8899` gemessen:

### 1. Pool-Markt-Find funktioniert

- `getProgramAccounts(pAMMBay6..., memcmp base_mint + quote_mint=WSOL)` liefert:
  - fuer `E7...pump` -> **1 Pool** in ~`24.186s`: `B8bvg3KzXzGAq51QjirhPTw5ChhiZWn2kNwvQd3YZFN8`
  - fuer `GwQ...zTR` -> **1 Pool** in ~`24.55s`: `5rNMGrJ3V2vUY3GAuxiVKZmKCn6c5N6n7Ld5EWvgceVX`

### 2. Aktueller tx-history fallback ist auf dem lokalen Validator unbrauchbar

`getSignaturesForAddress(...)` liefert aktuell **0** fuer:

- beide Pool-Markets
- beide Token-Mints
- beide Bonding-Curves

Konkret:

- Pool `B8bvg3KzXzGAq51QjirhPTw5ChhiZWn2kNwvQd3YZFN8` -> `0 sigs`
- Pool `5rNMGrJ3V2vUY3GAuxiVKZmKCn6c5N6n7Ld5EWvgceVX` -> `0 sigs`
- Mint `E7UaWyQoDgvUTvgQLxbR3oVyYpf3eq2hN95RzrwQpump` -> `0 sigs`
- Mint `GwQjXZvDTVVWyadJAvjx9upEZsFFToVQHY5NRrZ6wzTR` -> `0 sigs`
- Bonding curves:
  - `Et2vD6DRuqucyQivR77maBSeDt42eAiPURJujAXZyHxM` -> `0 sigs`
  - `7iGhtTK7hTo1fsEa4ZoDc7JYasqK7tmP7iiwCgbDrLkq` -> `0 sigs`

### 3. Forensische Market-Account-Gegenprobe

Fuer beide Pools direkt on-chain:

- owner = `pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA`
- space = `301`
- `quote_mint = WSOL`
- `creator_seed` ist vorhanden / nicht default
- es gibt genau:
  - **einen** Base-Token-Account
  - **einen** Quote-Token-Account
- beide gehoeren jeweils dem Pool selbst
- es gibt **keinen** zweiten eingebetteten Quote-Token-Account im Market, der direkt als Fee-TA dienen wuerde
- die `global_config` enthaelt in der gescannten Account-Menge **keine** Token-Accounts fuer diese beiden Base-Mints oder fuer WSOL

### 4. Produktionslogs

`market-data` zeigt fuer Bootstrap **und** spaeter fuer die echte Liquidation denselben Verlauf:

- `EnsurePumpAmmPoolAccounts start ... pool_address_hint=None has_pool_hint=false`
- `pump_amm: LivePoolCache miss for pool discovery, falling back to RPC`
- danach `terminal outcome error ... error=pump_amm pool discovery failed`

Wichtiger Schluss:

- Der Pool-Markt wird gefunden.
- Der Request timed nicht mehr aus.
- Der Fehler liegt jetzt **im Aufbau eines verwendbaren `PumpAmmPoolStatic`**.

## Relevante Hypothese

Auf Basis der aktuellen Evidenz ist der **wahrscheinlichste** blocker:

- `protocol_fee_recipient` / `protocol_fee_recipient_ta`

Warum:

- `creator_seed` ist vorhanden, also ist der Creator-Vault-Pfad eher tx-history-frei rekonstruierbar.
- Es gibt keinen zweiten eingebetteten Quote-Token-Account im Market.
- `protocol_fee_recipient` ist laut Code explizit **pool- und beobachtungsspezifisch**, also nicht global kanonisierbar.
- Genau an diesem Themenfeld gab es in der Vergangenheit bereits die fehlgeschlagene Kanonisierung.

Wichtig:

- Diese Hypothese ist stark, aber in diesem Scope bitte **sauber verifizieren**, nicht nur annehmen.

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
Wenn lokale Rekonstruktion und auch der engste erlaubte Fallback scheitern, muss der bestehende Reject-/Decision-Record-Pfad erhalten bleiben. Keine stille Ablehnung.

### I-24d Cold-Path Discovery nur per Request/Reply
`execution-engine` darf fehlende oder unbrauchbare PumpSwap-`pool_accounts` im Cold Path weder selbst discovern noch lokal als Truth in den SLAVE Cache schreiben. Discovery, MASTER-Write und JetStream-Publikation bleiben bei `market-data`. `execution-engine` darf nur den korrelierten Request/Reply-Pfad anstossen und bounded auf die autoritative Antwort warten.

## Bestehendes Pattern

Der aktuelle Discovery-Pfad in `pumpfun_amm.rs` ist:

1. Cache (`pool_accounts`) -> zero-RPC
2. bekannte Pool-Adresse -> `getAccount(pool_market)` Fast-Path
3. `getProgramAccounts(base_mint + WSOL)` -> Pool-Markt finden
4. `try_parse_pool_static_from_market_account(...)`
5. nur wenn das keinen verwendbaren statischen Satz liefert -> `discover_pool_static_via_tx_history_market_only(...)`

Dieser Scope soll **nicht** den Architekturvertrag sprengen, sondern den Schritt `4` fuer reale Pools verbessern und den Schritt `5` nur dann extern fallbacken, wenn der lokale Validator fuer TX-history real nichts liefert.

## Relevante Bug-Patterns

- `KNOWN_BUG_PATTERNS.md` #19:
  - Kein Symptomfix ohne Root Cause.
- `KNOWN_BUG_PATTERNS.md` #31:
  - Bekannter Pool darf nicht unnoetig ueber globale Scans behandelt werden.
- `KNOWN_BUG_PATTERNS.md` #32:
  - Keine Rueckkehr zu validator-index-abhaengigen Token-Owner-Fallbacks.
- `KNOWN_BUG_PATTERNS.md` #33:
  - Restart-/Bootstrap-Luecken fuer PumpSwap.
- `KNOWN_BUG_PATTERNS.md` #35:
  - `protocol_fee_recipient` darf **nicht** global kanonisiert werden.
- `KNOWN_BUG_PATTERNS.md` #36:
  - Cache/Teilzustand ist nicht automatisch `ready`.

## Erwartete Aenderung

Bitte arbeite in dieser Reihenfolge:

### A. Exakten blocker bestimmen

Bestimme im aktuellen Codepfad **konkret**, welcher Teil von `PumpAmmPoolStatic` fuer diese beiden realen Pools den lokalen Parse scheitern laesst.

Bevorzugte Kandidaten:

- `protocol_fee_recipient`
- `protocol_fee_recipient_ta`
- sonst nur wenn belegt:
  - `coin_creator_vault_*`
  - `fee_config`
  - `global_volume_accumulator`

Bitte im PR / Abschlussbericht klar sagen:

- welcher Account wirklich fehlte oder unzuverlaessig war
- warum der aktuelle lokale Parse ihn nicht sicher gewinnen konnte

### B. Bevorzugte Loesung: tx-history-freie Rekonstruktion

Wenn der fehlende Account fuer diese Pools deterministisch aus:

- Market-State
- Global-Config
- zusaetzlichen on-chain Reads
- oder verifizierten PDA-Regeln

rekonstruiert werden kann, dann bitte **diesen** Weg implementieren und den tx-history fallback fuer diesen Fall ueberfluessig machen.

Besonders wichtig:

- keine globale Fake-Kanonisierung von `protocol_fee_recipient`
- nur verifizierte seeds / echte on-chain observed state / ableitbare PDAs
- wenn nur die ATA fehlt, darf sie aus einem **bekannten echten Recipient** abgeleitet werden
- wenn der Recipient selbst nicht sicher bekannt ist: nicht raten

### C. Nur falls B nicht sicher moeglich ist: bounded Helius fallback

Falls der lokale tx-history-/parse-Pfad fuer diese Pools **nicht** sicher deterministisch geschlossen werden kann, ist ein enger Helius-Fallback erlaubt, aber nur unter diesen Bedingungen:

1. **Nur in `market-data`**
2. **Nur Cold Path**
3. **Erst nachdem**
   - der Pool-Markt lokal gefunden wurde
   - der lokale Market-Parse keinen verwendbaren Satz liefern konnte
   - und der lokale tx-history fallback unbrauchbar ist / keine Signaturen liefert
4. **Bounded**
   - kurze Timeout-Grenze
   - kein unbounded Retry
5. **Keine Nutzung im Hot Path**
6. **Klare Logs / Metriken**
   - lokaler Parse failed
   - lokaler tx-history unavailable
   - Helius fallback used
   - Helius success / failure

Wenn du Helius nutzt:

- bitte nur den kleinsten noetigen Datenzugriff
- idealerweise nur fuer genau den fehlenden `PumpAmmPoolStatic`-Baustein oder eine einzige Referenz-TX
- nicht als primaere Quelle fuer normale Pools

## Akzeptanzkriterien

- Fuer die beiden produktiv betroffenen Pools ist klar benannt, welcher `PumpAmmPoolStatic`-Baustein bisher den Parse scheitern liess.
- Wenn tx-history-frei sicher moeglich:
  - lokaler Parse baut den Satz ohne TX-history fallback vollstaendig.
- Wenn tx-history-frei nicht sicher moeglich:
  - es gibt einen engen bounded Helius-Cold-Path-Fallback nur fuer den echten Edge Case.
- Keine globale Kanonisierung von `protocol_fee_recipient`.
- Kein neuer Hot-Path-RPC / kein Helius im Hot Path.
- Kein lokaler Engine-RPC.
- Simulation-Gate unveraendert.

## Erlaubte Dateien

- `Iron_crab/src/solana/dex/pumpfun_amm.rs`
- `Iron_crab/src/bin/market_data.rs`
- eng benachbarte RPC-/Config-/Env-Datei nur wenn fuer bounded Helius fallback wirklich noetig

Falls wirklich erforderlich:

- kleine Hilfsdatei fuer Helius-/RPC-Integration mit kurzer Begruendung im Abschlussbericht

## Verboten

- Keine Aenderungen im Eval-Repo
- Kein neuer lokaler Discovery-/Write-Pfad in `execution-engine`
- Keine globale `protocol_fee_recipient`-Kanonisierung
- Kein Hot-Path-Helius
- Kein unbounded externer Fallback
- Keine Rueckkehr zu `getTokenAccountsByOwner`-artigen validator-index-abhaengigen Fallbacks
- Kein grosser Multi-DEX-Refactor

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
- welcher `PumpAmmPoolStatic`-Baustein fuer die beiden Pools wirklich der blocker war
- ob der finale Fix tx-history-frei ist oder einen bounded Helius fallback braucht
- falls Helius eingebaut wurde: unter welchen engen Bedingungen er anspringt
- welche Tests / Checks gelaufen sind
