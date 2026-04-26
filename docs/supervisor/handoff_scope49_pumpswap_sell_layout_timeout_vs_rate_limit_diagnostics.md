# Handoff: Scope 49 - PumpSwap force_refresh SELL-layout muss Timeout, Rate-Limit und Anfragegroesse sauber unterscheiden

WICHTIG: Lies und befolge die STOP-CHECK Regeln in `AGENTS.md` und `.cursor/rules/ironcrab-core.mdc` BEVOR du eine Datei aenderst. Wenn eine geplante Aenderung gegen eine Regel verstoesst, STOPPE sofort und melde den Verstoss statt die Aenderung durchzufuehren.

## Task-Beschreibung

Nach Merge von Scope 48 und Redeploy scheitert die Liquidation fuer die zwei bekannten PumpSwap-Pools weiterhin.

Die neue Root Cause ist jetzt enger belegt als zuvor:

- `execution-engine` simuliert einen PumpSwap SELL.
- Bei strukturellem Sim-Fail triggert es korrekt `EnsurePumpAmmPoolAccounts(force_refresh=true)`.
- `market-data` loest den autoritativen force-refresh an.
- Der lokale SELL-layout observer findet keine Signaturen.
- Der externe SELL-layout fallback kommt **nicht** zu einem verwertbaren Ergebnis innerhalb des Budgets.
- `market-data` publiziert deshalb nur einen **Partial**-Zustand:
  - `sell_cashback_remaining=false`
  - `sell_cashback_third_meta=None`
- Danach geht die `ControlResponse` wieder als `status=Error` zurueck.

Der aktuell sichtbare Fehler ist also:

1. **lokal keine SELL-Historie**
2. **externer SELL-layout fallback endet im Timeout-Budget**
3. **force_refresh bleibt sell-layout-unresolved**

Was noch **nicht** sauber unterschieden ist:

- echtes HTTP-Rate-Limit / `429`
- zu grosse oder zu breite Helius-Anfragen
- Netzwerk-/Provider-Latenz
- zu viele sequentielle oder parallele externe Subrequests im Observer
- Logik bleibt vor dem ersten brauchbaren Treffer haengen

Der User hat zusaetzlich klargestellt:

- es wird ein **Free Helius API Key** verwendet
- **zu grosse Anfragen** sind dort realistisch problematisch

Ziel dieses Scopes:

1. Den externen PumpSwap-SELL-layout fallback so instrumentieren, dass diese Ursachen **sauber unterscheidbar** werden.
2. Nur wenn noetig, den externen Zugriff **eng und bounded kleiner** machen, damit der diagnostische Pfad auf einem Free-Key nicht durch unnötig breite Requests kollabiert.
3. Noch **kein** grosser Architekturumbau. Noch **kein** spekulativer Funktionsfix ohne neue Evidenz.

## Harte Runtime-Evidenz

Betroffene Pools / Mints:

- Mint `E7UaWyQoDgvUTvgQLxbR3oVyYpf3eq2hN95RzrwQpump`
  - Pool `B8bvg3KzXzGAq51QjirhPTw5ChhiZWn2kNwvQd3YZFN8`
- Mint `GwQjXZvDTVVWyadJAvjx9upEZsFFToVQHY5NRrZ6wzTR`
  - Pool `5rNMGrJ3V2vUY3GAuxiVKZmKCn6c5N6n7Ld5EWvgceVX`

Relevante Logs vom neuen Lauf:

### execution-engine

- erster Simulationsversuch scheitert fuer beide Pools wieder mit:
  - `UiTransactionError(InstructionError(1, Custom(6023)))`
- danach korrekt:
  - `EnsurePumpAmmPoolAccounts(force_refresh=true)`
- Antwort fuer beide Requests:
  - `ControlResponse ... status=Error`

### market-data

Fuer `B8bvg...`:

- `force_refresh — skipping LivePoolCache pool_accounts; authoritative RPC parse`
- `pool_address hint provided, trying direct getAccount (fast path)`
- `no signatures available for authoritative SELL-layout observation ... log_ctx="local_force_refresh_sell_layout"`
- `bounded external SELL-layout observation timed out ... timeout_secs=12`
- `force_refresh result published as Partial (authoritative SELL layout unresolved) ... sell_cashback_remaining=false sell_cashback_third_meta=None`
- `ControlResponse published ... status=Error`

Fuer `5rNM...`:

- gleiches Muster:
  - lokaler SELL-layout observer ohne Signaturen
  - danach kein autoritativ nachgewiesenes Extended-SELL-layout
  - `ControlResponse status=Error`

Wichtig:

- Es gibt derzeit **keinen** geloggten `429`.
- Es gibt derzeit **keinen** geloggten expliziten Helius-Fehlercode.
- Sichtbar ist nur das Endsymptom `timed out`.

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

- `KNOWN_BUG_PATTERNS.md` #19
  - Kein Symptomfix ohne harte Runtime-Evidenz.
- `KNOWN_BUG_PATTERNS.md` #34
  - Cold-Path force_refresh darf nicht wieder nur denselben Partial-/Stale-Zustand liefern.
- `KNOWN_BUG_PATTERNS.md` #36
  - Cache/Teilzustand ist nicht automatisch `ready`.
- `KNOWN_BUG_PATTERNS.md` #4
  - Kein neuer RPC im Hot Path.

## Bestehendes Pattern

Bitte direkt auf dem bestehenden force-refresh / SELL-layout-Observer-Pfad aufsetzen:

- `resolve_authoritative_sell_layout_for_force_refresh(...)`
- `observe_authoritative_sell_layout_from_tx_history_with_rpc(...)`
- bestehender bounded externer SELL-layout fallback
- `market-data` Kontrolllogik fuer `sell_layout_ready`

Die richtige Richtung ist hier:

1. **erst Sichtbarkeit**
2. **dann enges Budgeting**
3. **noch kein groesserer Semantik-Umbau**

## Erwartete Aenderung

Bitte arbeite in dieser Reihenfolge:

### A. Stufenweise Telemetrie fuer den externen SELL-layout fallback

Instrumentiere den externen force-refresh SELL-layout-Pfad so, dass klar wird:

1. welche externe RPC-Methode aufgerufen wird
2. wie gross die jeweilige Anfrage ist
3. wie viele Signaturen / TXs / Instruktionen betrachtet werden
4. wie lange jede Stufe dauert
5. ob ein echter HTTP-/RPC-Fehler vorliegt
6. ob der Fehler ein Timeout, `429`, anderer Provider-Fehler oder leerer Treffer ist

Mindestens gewuenschte Log-Felder / Diagnosepunkte:

- `stage`
  - z. B. `getSignaturesForAddress`, `getTransaction`, `scan_transactions`, `decode_instruction`
- `elapsed_ms`
- `pool`
- `base_mint`
- `request_limit`
- `signatures_returned`
- `transactions_fetched`
- `sell_candidates_seen`
- `provider_status`
  - z. B. `ok`, `timeout`, `http_429`, `http_5xx`, `rpc_error`, `empty`
- `termination_reason`
  - z. B. `layout_found`, `no_sell_candidates`, `request_budget_exhausted`, `timeout_budget_exhausted`, `provider_rate_limited`

Wichtig:

- bitte kein Log-Spam fuer jeden normalen Hot-Path-Fall
- die Telemetrie soll vor allem fuer `force_refresh` / Cold Path greifen
- strukturierte und stabile Logtexte, nicht lose Debug-Fragmente

### B. Enge Budgetierung fuer Free-Helius-Key

Wenn der bestehende externe SELL-layout fallback aktuell zu breit fragt, darfst du ihn **eng bounded kleiner** machen, aber nur in einer Form, die weiterhin saubere Diagnose erlaubt.

Erlaubt ist:

- kleines Signatur-Limit
- kleine TX-Fetch-Batches
- klare harte Obergrenzen fuer Anzahl externer Calls
- frueher Abbruch mit explizitem `termination_reason`
- keine grossen oder unbounded Sammelabfragen

Nicht das Ziel:

- blindes "Timeout erhoehen und hoffen"
- brute-force history scan
- mehr Parallelismus
- Helius als primaere Quelle

Wichtig:

- Wenn du Request-Groessen reduzierst, muss das im Abschlussbericht explizit genannt werden.
- Wenn die Reduktion nur die Diagnose verbessert, ist das ok.
- Wenn dadurch die Erfolgsquote im Cold-Path zufaellig schon steigt, ist das gut, aber nicht die primaere Scope-Definition.

### C. Fehlerklassifikation bis zur `ControlResponse`

Bitte sorge dafuer, dass `market-data` vor dem finalen `status=Error` klar loggt, **warum** `sell_layout_ready` false blieb.

Beispiele:

- `local_history_empty + external_timeout`
- `local_history_empty + external_http_429`
- `local_history_empty + external_empty_result`
- `local_history_empty + external_budget_exhausted`

Die `ControlResponse` selbst darf kompakt bleiben, aber der Supervisor muss aus den Logs klar lesen koennen, was passiert ist.

## Akzeptanzkriterien

- Fuer den externen PumpSwap-SELL-layout fallback ist sichtbar, ob der Abbruch von:
  - Timeout
  - Rate-Limit
  - leerem Ergebnis
  - zu grossem / zu breitem Request-Budget
  - oder anderem RPC-Fehler kommt
- Die Diagnose ist fuer die zwei produktiven Pools reproduzierbar lesbar.
- Kein neuer Hot-Path-RPC.
- Kein neuer lokaler `execution-engine`-Fixpfad.
- Kein grosser Architekturumbau.
- Wenn Anfragegroessen reduziert wurden, bleibt alles bounded und explizit dokumentiert.

## Erlaubte Dateien

- `src/solana/dex/pumpfun_amm.rs`
- `src/bin/market_data.rs`
- kleine, direkt zugehoerige Tests oder Hilfsfunktionen im Impl-Repo

## Verboten

- Keine Aenderungen in `execution-engine`
- Kein Eval-Repo
- Kein unbounded Helius-Scan
- Kein Hot-Path-Helius
- Kein globaler Architekturumbau des PumpSwap-Flows
- Kein "Timeout einfach massiv erhoehen" ohne Diagnosegewinn
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
- welche Stufen des externen SELL-layout fallbacks jetzt sichtbar geloggt werden
- wie Timeout vs. `429` vs. leeres Ergebnis unterschieden wird
- ob du die Anfragegroesse / das Budget reduziert hast
- welche neuen `termination_reason` / Fehlerklassen existieren
- welche Tests / Checks gelaufen sind
