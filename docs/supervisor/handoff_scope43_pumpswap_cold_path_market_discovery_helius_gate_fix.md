# Handoff: Scope 43 - PumpSwap Cold-Path Helius fallback auch fuer lokale Market-Discovery-Fehler

WICHTIG: Lies und befolge die STOP-CHECK Regeln in AGENTS.md und .cursor/rules/ironcrab-core.mdc BEVOR du eine Datei aenderst. Wenn eine geplante Aenderung gegen eine Regel verstoesst, STOPPE sofort und melde den Verstoss statt die Aenderung durchzufuehren.

## Task-Beschreibung

Fixe den naechsten produktiven Cold-Path-Blocker fuer PumpSwap AMM:

- `market-data` hat Scope 42 deployt.
- `solana.helius_rpc_url` ist auf dem Server geladen.
- Trotzdem scheitert die Kill-Switch-Liquidation weiterhin vor der Simulation mit:
  - `ControlResponse(status=Error)`
  - `pump_amm pool discovery failed`
  - `QUOTE_UNAVAILABLE`

Der heute belegte Root Cause ist:

- Der bounded Helius fallback aus Scope 42 ist **zu spaet im Discovery-Pfad eingehangen**.
- Er wird nur erreicht, wenn lokal bereits ein `pool_market` gefunden wurde (`markets.first()` / lokale Pool-Markt-Findung erfolgreich).
- Fuer die beiden produktiven Mints wird der Fehler im Cold Path aber offenbar bereits **vor** diesem Gate erreicht.
- Ergebnis: `Helius` ist konfiguriert und deployt, wird fuer diese Requests aber nie benutzt.

Der User will explizit:

- fuer **jeden** Discovery-Request im Cold Path bei einem lokalen Validator-Error ein bounded Helius fallback,
- damit die Liquidation auch dann weiterkommt, wenn der lokale Validator den benoetigten Discovery-State nicht liefern kann.

Wichtig:

- Der Fallback bleibt **nur** in `market-data`.
- Kein Helius im Hot Path.
- Kein lokaler Discovery-/Write-Pfad im `execution-engine`.
- Kein unbounded externer Scan.

## Aktuelle Runtime-Evidenz

### Betroffene Mints

- `E7UaWyQoDgvUTvgQLxbR3oVyYpf3eq2hN95RzrwQpump`
- `GwQjXZvDTVVWyadJAvjx9upEZsFFToVQHY5NRrZ6wzTR`

### Produktionslauf heute

`execution-engine`:

- `18:10:19Z`: Kill-Switch mit `liquidate_positions=true`
- `18:10:20Z`: Discovery fuer `E7...pump`
- `18:11:03Z`: korrelierte `ControlResponse(status=Error)` fuer `E7...pump`
- danach:
  - `LIQUIDATION SKIP ... pump_amm=err_discovery pump_amm pool discovery failed`
  - `Intent rejected ... reason=QUOTE_UNAVAILABLE`
- `18:11:03Z`: Discovery fuer `GwQ...zTR`
- `18:11:46Z`: korrelierte `ControlResponse(status=Error)` fuer `GwQ...zTR`
- danach derselbe Reject-Pfad

Wichtiger Schluss:

- Es gibt heute **keine** Simulation dieser beiden Sells.
- Der Fehler sitzt im Discovery-/State-Reconstruction-Gate, nicht im Sell-Ix.

`market-data`:

- startet mit:
  - `Loaded solana.helius_rpc_url for bounded PumpSwap TX-history fallback`
- fuer beide Requests:
  - `EnsurePumpAmmPoolAccounts received/start`
  - `LivePoolCache miss for pool discovery, falling back to RPC`
  - nach ca. 43s:
    - `terminal outcome error (discovery failed) ... error=pump_amm pool discovery failed`
    - `ControlResponse published ... status=Error`

### Der kritische neue Befund

In den Logs des fehlgeschlagenen Laufs fehlen **vollstaendig** die Scope-42-Hinweise darauf, dass der externe Fallback ueberhaupt betreten wurde:

- kein `pump_amm attempting TX-history fallback for market ...`
- kein `bounded external TX-history fallback starting`
- kein `bounded external TX-history SUCCESS`
- kein `helius_unconfigured`
- kein `helius_failed`
- keine strukturierten lokalen `local_parse_fail_reason`-Logs fuer diese Requests

Das bedeutet:

- Der Request erreicht den Scope-42-Helius-Pfad fuer diese beiden Mints gar nicht.
- Die Ursache liegt sehr wahrscheinlich **vor** dem heutigen `markets.first()`-Gate.

### Server-Stand ist korrekt

Auf dem Server:

- Repo: `/home/ironcrab/Iron_crab`
- Repo-HEAD: `286f10cf460598da3caa48cc33027371a766cf9e`
- letzter Commit:
  - `Merge pull request #74 from Exploratorsclub/cursor/scope42-pumpswap-bounded-helius-reasons`

Scope 42 ist also wirklich deployt.

## Vorherige Versuche: Hat lokale Market-Discovery frueher funktioniert?

Ja, bei den vorigen Fehlversuchen war die Lage anders:

- Frueher wurde direkt am Produktions-Validator belegt:
  - `getProgramAccounts(pAMMBay6..., memcmp base_mint + WSOL)` fand fuer beide betroffenen Mints jeweils genau **einen** Pool-Markt
  - `getSignaturesForAddress(pool_market)` lieferte aber `0`
- Das war der Grund fuer Scope 42:
  - lokaler Markt-Fund funktionierte
  - lokaler TX-History-/Static-Reconstruction-Pfad scheiterte
  - bounded Helius wurde nur fuer genau diesen Fall eingebaut

Heute ist der Befund enger:

- der Fehler liegt fuer diese Requests offenbar **noch frueher**
- also im lokalen Markt-Findungs-/fruehen Discovery-Gate selbst
- genau deshalb wurde `Helius` heute nie erreicht

## Ziel dieses Scopes

1. Bounded Helius fallback so erweitern, dass er bei **jedem lokalen Validator-Error im Cold-Path-Discovery-Pfad** erreichbar ist
2. Nicht erst nach lokal erfolgreichem `pool_market`-Fund
3. Weiterhin:
   - `market-data` bleibt Autoritaet
   - `execution-engine` bleibt reiner Request/Reply-Client
   - Hot Path bleibt ohne Helius

## Relevante Invarianten (Volltext)

### I-4 Hot Path = Geyser-First
HOT PATH (Discovery, Buy, Sell, Monitoring): GEYSER-ONLY. Keine neuen blockierenden RPC-Calls. Wenn ein Pfad sowohl Hot als auch Cold Path beruehrt, darf der Fix keinen neuen blockierenden Engine-RPC oder externen RPC in den Hot Path schleusen.

### I-5 Cold Path
COLD PATH (Liquidation, Manual Actions, Bootstrap): RPC erlaubt. Safety und correctness vor Speed. Autoritativer On-Chain-State darf hier nachgeladen werden.

### I-7 Hot-Path RPC-Freiheit
Nie RPC im normalen Trading-Hot-Path ohne explizite Freigabe. Kein lokaler Discovery-RPC und kein externer RPC im `execution-engine`.

### I-9 Simulation-Gate
Wenn Simulation fehlschlaegt, darf keine Transaktion gesendet werden. Dieser Scope darf nur Discovery/State-Reconstruction verbessern, nicht die Simulation umgehen.

### I-12 Decision Record
Wenn lokaler Pfad und auch bounded Helius fallback scheitern, muss der bestehende Reject-/Decision-Record-Pfad erhalten bleiben. Keine stille Ablehnung.

### I-24d Cold-Path Discovery nur per Request/Reply
`execution-engine` darf fehlende oder unbrauchbare PumpSwap-`pool_accounts` im Cold Path weder selbst discovern noch lokal als Truth in den SLAVE Cache schreiben. Discovery, MASTER-Write und JetStream-Publikation bleiben bei `market-data`. `execution-engine` darf nur den korrelierten Request/Reply-Pfad anstossen und bounded auf die autoritative Antwort warten.

## Bestehendes Pattern

### A. Cold-Path-Heilung bleibt in `market-data`

Bestehendes Pattern:

- `execution-engine` sendet `EnsurePumpAmmPoolAccounts`
- `market-data` fuehrt autoritative Discovery aus
- Erfolg:
  - MASTER-Write
  - JetStream / `PoolCacheUpdate`
  - `ControlResponseStatus::Ok`
- Fehler:
  - `ControlResponseStatus::Error`

Bitte dieses Architekturpattern beibehalten.

### B. Scope-42-Helius ist derzeit zu eng gegated

Aktueller Zustand in `pumpfun_amm.rs`:

- lokale Markt-Findung via `discover_pool_markets_via_program_accounts(base_mint)`
- spaeter:
  - `if let Some(m) = markets.first().copied() { ... }`
  - lokaler TX-history fallback
  - danach bounded external TX-history fallback

Problem:

- wenn der lokale Validator schon **vor** `markets.first()` keinen brauchbaren Markt liefert, ist `Helius` unerreichbar

### C. Wiederverwendung der vorhandenen Helius-Konfiguration

Es gibt bereits:

- `src/config.rs`: `solana.helius_rpc_url`
- `market-data` laedt diese Config bereits erfolgreich

Bitte keine zweite Helius-Konfiguration einbauen.

## Relevante Bug-Patterns

- `KNOWN_BUG_PATTERNS.md` #19:
  - kein Symptomfix ohne Runtime-Evidenz
- `KNOWN_BUG_PATTERNS.md` #31:
  - bekannte Pools nicht unnoetig ueber globale Scans behandeln
- `KNOWN_BUG_PATTERNS.md` #32:
  - keine Rueckkehr zu validator-index-abhaengigen Owner-/ATA-Fallbacks
- `KNOWN_BUG_PATTERNS.md` #34:
  - Cold-Path Recovery darf nicht erneut denselben kaputten Zustand liefern
- `KNOWN_BUG_PATTERNS.md` #35:
  - keine globale Kanonisierung von `protocol_fee_recipient`
- `KNOWN_BUG_PATTERNS.md` #36:
  - Cache/Teilzustand ist nicht automatisch `ready`

## Erwartete Aenderung

Bitte arbeite in dieser Reihenfolge:

### A. Helius-Gate auf den gesamten lokalen Cold-Path-Discovery-Fehlerraum erweitern

Der bounded Helius fallback soll nicht nur nach lokalem Markt-Find + lokalem TX-history-Fail erreichbar sein, sondern fuer **lokale Validator-Errors entlang des gesamten Cold-Path-Discovery-Requests**.

Konkret soll bounded Helius einspringen, wenn der lokale Validator im Cold Path einen benoetigten Discovery-Schritt nicht liefern kann, z. B.:

- `discover_pool_markets_via_program_accounts(base_mint)` liefert Fehler oder keinen verwertbaren Markt
- lokaler Marktparse scheitert
- lokaler TX-history fallback ist leer / unbrauchbar
- lokaler RPC liefert harte Errors

Wichtig:

- lokale Validator-Daten bleiben immer **erste Wahl**
- der externe Zugriff bleibt **bounded**
- aber es darf keinen fruehen lokalen Error-Punkt mehr geben, der den Request mit `Error` beendet, **bevor** der bounded Helius fallback ueberhaupt versucht wurde

### B. Zielbild: "lokaler Validator zuerst, externer bounded Fallback fuer jeden lokalen Discovery-Error im Cold Path"

Der Userwunsch fuer diesen Scope ist explizit:

- fuer **jeden** Discovery-Request im Cold Path bei einem lokalen Validator-Error das Helius fallback ausloesen

Bitte setze das so um, dass es technisch eng und bounded bleibt:

- nur in `market-data`
- nur fuer wallet-relevante / angeforderte Mints
- nur im Cold Path
- kurzer Timeout
- keine Endlosschleife
- keine primaere externe Discovery fuer normale Faelle

Wenn es technisch sinnvoll ist, den bounded Helius-Pfad in zwei kleine Stufen zu trennen, ist das ok:

1. bounded externer Markt-Find
2. bounded externer Static-/TX-history-Rebuild

aber bitte keine unbounded Variante.

### C. Logs / Diagnose sichtbar halten

Bitte die Logs so halten oder erweitern, dass klar ist:

- an welchem lokalen Discovery-Schritt es zuerst gescheitert ist
- ob der externe bounded Helius fallback betreten wurde
- welcher minimale externe Datenzugriff benutzt wurde
- ob der Erfolg aus:
  - externer Markt-Findung
  - externem Static-Parse
  - externem TX-history-Rebuild
  stammt

### D. Erfolgsweg unveraendert autoritativ halten

Wenn der externe bounded Pfad erfolgreich einen verwendbaren PumpSwap-State liefert:

- `market-data` bleibt Autoritaet
- derselbe MASTER -> JetStream -> `ControlResponseStatus::Ok`-Pfad bleibt bestehen
- `execution-engine` aendert sich nicht

## Akzeptanzkriterien

- Fuer die beiden produktiven Mints endet der Cold-Path-Request nicht mehr vorzeitig nur wegen lokalem Validator-Error vor `markets.first()`
- bounded Helius ist fuer den gesamten lokalen Cold-Path-Discovery-Fehlerraum erreichbar
- kein Helius im Hot Path
- kein lokaler Engine-RPC
- kein unbounded externer Scan
- bestehender Request/Reply-Architekturvertrag bleibt intakt
- Logs machen sichtbar:
  - lokaler erster Fehlerpunkt
  - externer Fallback versucht / nicht versucht
  - externer Erfolg / Misserfolg

## Erlaubte Dateien

- `Iron_crab/src/solana/dex/pumpfun_amm.rs`
- `Iron_crab/src/bin/market_data.rs`
- `Iron_crab/src/config.rs` nur wenn wirklich noetig
- `Iron_crab/config.example.toml` nur wenn fuer Doku wirklich noetig
- kleine benachbarte Hilfsdatei nur wenn sauber begruendet

## Verboten

- Keine Aenderungen im Eval-Repo
- Kein neuer lokaler Discovery-/Write-Pfad in `execution-engine`
- Kein Helius im Hot Path
- Kein unbounded externer Fallback
- Keine globale `protocol_fee_recipient`-Kanonisierung
- Keine Rueckkehr zu validator-index-abhaengigen Owner-/ATA-Fallbacks
- Kein grosser Multi-DEX-Refactor
- Kein Commit von API-Keys / Secrets / serverlokalen Konfigurationswerten

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
- ob der bounded Helius-Pfad jetzt auch bei lokalem Markt-Findungs-/fruehem Discovery-Fehler betreten wird
- welche lokalen Error-Kategorien jetzt den bounded Helius fallback ausloesen
- welcher minimale externe Datenzugriff genutzt wird
- wie der Erfolg wieder in den normalen `market-data` -> MASTER -> JetStream -> `ControlResponse`-Pfad integriert wird
- welche Tests / Checks gelaufen sind
