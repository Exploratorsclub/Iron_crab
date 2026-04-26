# Handoff: Scope 47 - PumpSwap force_refresh muss autoritatives Extended-SELL-Layout liefern

WICHTIG: Lies und befolge die STOP-CHECK Regeln in `AGENTS.md` und `.cursor/rules/ironcrab-core.mdc` BEVOR du eine Datei aenderst. Wenn eine geplante Aenderung gegen eine Regel verstoesst, STOPPE sofort und melde den Verstoss statt die Aenderung durchzufuehren.

## Task-Beschreibung

Es gibt neue Runtime-Evidenz aus der Produktion nach Deploy von `architecture-rebuild` auf Commit `49c18ab`.

Der vorherige Scope fuer dynamisches PumpSwap-SELL-Layout ist gemergt, aber **Kill-Switch / Liquidation** scheitert fuer zwei Token weiterhin mit **strukturellem PumpSwap-SELL-Simulationsfehler `Custom(6023)`**.

Die neue Root Cause ist enger:

- `execution-engine` erkennt den Fehler korrekt als strukturellen PumpSwap-SELL-Fail.
- Der Cold-Path-Request/Reply an `market-data` (`EnsurePumpAmmPoolAccounts(force_refresh=true)`) wird korrekt ausgeloest.
- `market-data` antwortet auch mit `status=Ok`.
- Der anschliessende Retry baut aber effektiv **denselben unbrauchbaren SELL erneut** und scheitert wieder mit `Custom(6023)`.

Die neue Arbeitsannahme mit harter Evidenz ist:

1. Die beiden betroffenen Pools benoetigen das **neue erweiterte PumpSwap-SELL-Layout**.
2. `market-data` liefert im Recovery-Pfad aktuell zwar `pool_accounts` / Reserven / PoolCacheUpdate, aber **nicht autoritativ das benoetigte Extended-SELL-Layout** fuer genau diesen Refresh.
3. Dadurch liest der SLAVE nach dem Cold-Path-Refresh weiter einen Zustand, der fuer den SELL-Builder nicht `ready` ist.

Ziel dieses Scopes:

1. `EnsurePumpAmmPoolAccounts(force_refresh=true)` muss fuer betroffene PumpSwap-Pools nicht nur `pool_accounts` / Reserven liefern, sondern auch das **autoritative Extended-SELL-Layout**.
2. Dieses Layout muss ueber `market-data` -> JetStream -> SLAVE Cache sichtbar werden.
3. Der naechste Cold-Path-Retry muss daraus einen **anderen, erweiterten SELL** bauen koennen.
4. Kein neuer Hot-Path-RPC und keine lokale Engine-Truth.

## Aktuelle Runtime-Evidenz

### Betroffene Tokens / Pools

- Mint `E7UaWyQoDgvUTvgQLxbR3oVyYpf3eq2hN95RzrwQpump`
  - Pool `B8bvg3KzXzGAq51QjirhPTw5ChhiZWn2kNwvQd3YZFN8`
- Mint `GwQjXZvDTVVWyadJAvjx9upEZsFFToVQHY5NRrZ6wzTR`
  - Pool `5rNMGrJ3V2vUY3GAuxiVKZmKCn6c5N6n7Ld5EWvgceVX`

### execution-engine Logs

Beobachtet im Produktionslauf `2026-04-08 06:16:10` bis `06:16:11`:

- Beide Liquidation-Intents werden als `pump_amm` vorbereitet.
- Beide Simulationen schlagen mit `UiTransactionError(InstructionError(1, Custom(6023)))` fehl.
- execution-engine klassifiziert das korrekt als strukturellen PumpSwap-SELL-Fehler und triggert den eingebauten Cold-Path-Recovery-Schritt:
  - `PumpSwap cold-path recovery: simulation structural (6013/6023/Overflow family) — force-refresh pool_accounts (market-data RPC), rebuilding tx (one retry)`
- Nach dem autoritativen `ControlResponse(status=Ok)` wird der Retry gebaut.
- Der Retry scheitert fuer beide Pools **erneut mit demselben `Custom(6023)`**.

Entscheidender Befund:

- Der Cold-Path-Request/Reply-Mechanismus funktioniert.
- Der **Inhalt** des zurueckgelieferten / propagierten Zustands reicht fuer den erweiterten SELL offenbar noch nicht.

### market-data Logs

Fuer beide Requests zeigt `market-data`:

- `EnsurePumpAmmPoolAccounts received ... force_refresh=true`
- `pool_address hint provided, trying direct getAccount (fast path)`
- `PumpAmmPoolStatic from pool_address hint (fast path)`
- `EnsurePumpAmmPoolAccounts: Published PoolCacheUpdate to JetStream`
- `ControlResponse published ... status=Ok`

Wichtig:

- Der Fast-Path ueber den `pool_address_hint` funktioniert.
- In der korrelierten Runtime-Evidenz ist aber **kein nachweisbares Extended-SELL-Merkmal** fuer diese beiden Pools sichtbar.
- Dadurch spricht alles dafuer, dass der Hint-/Fast-Path zwar einen gueltigen Pool findet, aber **nicht den SELL-layout-relevanten Zusatzkontext autoritativ bestimmt / propagiert**.

## GitHub-Stand / Eng verdächtige Stellen

Aktueller GitHub-Stand: `architecture-rebuild` nach Merge von PR #78.

### `src/bin/market_data.rs`

Relevant:

- `handle_ensure_pump_amm_pool_accounts(...)`
- `hydrate_pump_amm_reserves_if_needed(...)`
- Publikation von:
  - `pump_amm_sell_cashback_remaining`
  - `pump_amm_sell_cashback_third_meta`

Der Code **hat bereits** die Metadata-Schluessel fuer das Extended-SELL-Layout.
Der Fehler ist daher sehr wahrscheinlich **nicht** "Schluessel existieren gar nicht", sondern:

- Der Force-Refresh-Pfad berechnet / setzt sie fuer diese Pools nicht korrekt.
- Oder der Hint-/Fast-Path kommt an einem `PumpAmmPoolStatic` vorbei, in dem das Extended-SELL-Layout noch nicht autoritativ erkannt wurde.

### `src/solana/dex/pumpfun_amm.rs`

Relevant:

- `pool_accounts_v1_for_base_mint_with_hint_diagnostic(...)`
- `try_parse_pool_static_from_market_account_inner(...)`
- `build_swap_ix_from_pool_accounts(...)`

Der Builder kann Extended-SELL bereits, wenn folgende Inputs autoritativ vorhanden sind:

- `sell_requires_cashback_remaining`
- `sell_cashback_third_meta`

Damit liegt der Verdacht enger auf:

- Discovery / Fast-Path / Parsing der SELL-layout-relevanten Merkmale
- nicht primaer auf dem eigentlichen Builder

### `src/execution/tx_builder.rs`

`tx_builder` liest bereits:

- `cache.pump_amm_sell_extended_layout(&pool_id)`
- und uebergibt die Werte an `build_swap_ix_from_pool_accounts(...)`

Deshalb ist `tx_builder` fuer diesen Scope **vermutlich nicht der Hauptfix-Ort**.

## Relevante Invarianten (Volltext)

### I-4 Hot Path = Geyser-First
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

### A.43 PumpSwap Cold-Path Recovery: force_refresh und pool_address_hint
Loest die Cold-Path-Recovery nach strukturellem PumpSwap-Simulationsfehler einen Pfad mit `force_refresh` aus, darf dieselbe stale / unvollstaendige PumpSwap-Information nicht unveraendert als Truth zurueckkommen. Der Hint-Pfad ueber `pool_address_hint` bleibt Teil des beobachtbaren Recovery-Vertrags.

## Bestehendes Pattern, das du explizit wiederverwenden sollst

### Pattern 1: Cache-Hit ist nicht automatisch `ready`

Siehe `KNOWN_BUG_PATTERNS.md` #36:

- "im Cache vorhanden" ist nicht dasselbe wie "vollstaendig sell-ready"
- Teilzustand darf nicht zu frueh wie autoritativer Vollzustand behandelt werden

### Pattern 2: Analogie zu PumpFun cashback_enabled

Siehe `KNOWN_BUG_PATTERNS.md` #25:

- Bei PumpFun war der Root Cause: JetStream-/Cache-Zustand war vorhanden, aber das layout-relevante Feature (`cashback_enabled`) fehlte bzw. stand auf `false`
- Der Fix war 3-teilig:
  1. `market-data` propagiert das Feature
  2. `pool_cache_sync` liest es
  3. Cold Path vertraut Cache-HIT nicht blind, wenn das autoritative Feature fehlt

Genau dieses Muster sollst du hier als Referenz verwenden, aber fuer **PumpSwap Extended-SELL**.

### Pattern 3: Cold-Path force_refresh darf nicht cache-first dieselbe Teilwahrheit liefern

Siehe `KNOWN_BUG_PATTERNS.md` #34:

- Force-Refresh muss semantisch wirklich ein autoritativer Refresh sein
- nicht nur "cache-first, aber mit Response=Ok"

## Konkrete Erwartung an den Fix

Bitte schneide einen **engen** Scope, der genau dieses Problem loest:

1. Finde im PumpSwap-Discovery-/Hint-/Fast-Path den Punkt, an dem das Extended-SELL-Layout fuer diese Pools verloren geht oder nie autoritativ bestimmt wird.
2. Sorge dafuer, dass `EnsurePumpAmmPoolAccounts(force_refresh=true)` fuer betroffene Pools das benoetigte Extended-SELL-Signal **wirklich** mitliefert:
   - mindestens das Flag / Merkmal, dass der Pool den erweiterten SELL braucht
   - plus den nicht-deterministischen Zusatzkontext (`sell_cashback_third_meta`), falls erforderlich
3. Stelle sicher, dass der MASTER diesen Zustand ueber JetStream publiziert und der SLAVE ihn im Cache behaelt.
4. Der Cold-Path-Retry danach muss daraus einen **erweiterten** SELL bauen koennen.

## Sehr wichtige Abgrenzung

Dieser Scope soll **nicht**:

- einen neuen lokalen Reparaturpfad in `execution-engine` bauen
- das Simulations-Gate lockern
- global "immer 24" hardcoden
- den gesamten PumpSwap-Builder neu schreiben
- mehrere DEXe gleichzeitig refactoren

Es geht eng nur um:

- **force_refresh / market-data / authoritative propagation**
- fuer **PumpSwap Extended-SELL-Layout**

## Erwartete technische Richtung

Die wahrscheinlich richtige Richtung ist:

1. Im Hint-/Fast-Path (`pool_address_hint` -> `getAccount`) nicht nur `PumpAmmPoolStatic` fuer den Basisfall konstruieren, sondern auch SELL-layout-relevante Zusatzmerkmale autoritativ bestimmen.
2. Falls der Fast-Path diese Information nicht liefern kann, darf `force_refresh=true` nicht still mit unvollstaendigem `ok` enden. Dann braucht es einen sauberen erweiterten autoritativen Schritt im Cold Path.
3. Die bestehenden Metadata-Schluessel in `market_data.rs`
   - `pump_amm_sell_cashback_remaining`
   - `pump_amm_sell_cashback_third_meta`
   muessen fuer diese Pools danach gesetzt und beobachtbar propagiert werden.
4. Wenn der erweiterte Pfad wirklich erkannt ist, aber der dritte Meta-Wert unbekannt bleibt, soll der Zustand **nicht** als sell-ready Vollwahrheit behandelt werden.

## Akzeptanzkriterien

- Fuer den Force-Refresh-Pfad von `EnsurePumpAmmPoolAccounts` ist klar getrennt:
  - Basislayout-only
  - Extended-SELL-layout vorhanden
- `market-data` publiziert fuer betroffene Pools das autoritative Extended-SELL-Signal in den PoolCacheUpdate-Metadata.
- Der SLAVE kann das danach lesen und behalten.
- Der Builder muss danach nicht raten.
- Kein neuer Hot-Path-RPC.
- Kein neuer lokaler Engine-Truth.
- Kein globales Hardcoding "SELL = 24".
- Fokussierte Tests / Regressionen vorhanden.

## Relevante Runtime-Referenzen fuer Tests / Diagnose

- Mint `E7UaWyQoDgvUTvgQLxbR3oVyYpf3eq2hN95RzrwQpump`
- Pool `B8bvg3KzXzGAq51QjirhPTw5ChhiZWn2kNwvQd3YZFN8`
- Mint `GwQjXZvDTVVWyadJAvjx9upEZsFFToVQHY5NRrZ6wzTR`
- Pool `5rNMGrJ3V2vUY3GAuxiVKZmKCn6c5N6n7Ld5EWvgceVX`
- Produktionsfehler:
  - `UiTransactionError(InstructionError(1, Custom(6023)))`
  - erster Sim-Fail
  - `EnsurePumpAmmPoolAccounts(force_refresh=true)` -> `status=Ok`
  - Retry -> wieder `Custom(6023)`

## Erlaubte Dateien

- `Iron_crab/src/bin/market_data.rs`
- `Iron_crab/src/solana/dex/pumpfun_amm.rs`
- `Iron_crab/src/execution/live_pool_cache.rs`
- `Iron_crab/src/execution/pool_cache_sync.rs`
- enge Tests im Impl-Repo, wenn sie genau diesen Contract absichern

## Verboten

- Keine Aenderungen im Eval-Repo
- Kein neuer lokaler Reparatur-/Truth-Pfad in `execution-engine`
- Kein globales Hardcoding "PumpSwap SELL = 24"
- Keine heuristische Blindannahme ohne autoritativen Nachweis
- Kein neuer blockierender RPC im Hot Path
- Kein Simulation-Bypass
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
- wo genau das Extended-SELL-Layout im Force-Refresh-Pfad bisher verloren ging
- welche Stelle das jetzt autoritativ liefert
- wie MASTER -> JetStream -> SLAVE den neuen Zustand propagiert
- welche Tests / Checks gelaufen sind
- ob der Standardfall weiter beim Basislayout bleibt
