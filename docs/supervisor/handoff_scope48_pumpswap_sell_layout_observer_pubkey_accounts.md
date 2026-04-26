# Handoff: Scope 48 - PumpSwap autoritativer SELL-Layout-Observer muss Pubkey-Accountlisten aus RPC/Helius verstehen

WICHTIG: Lies und befolge die STOP-CHECK Regeln in `AGENTS.md` und `.cursor/rules/ironcrab-core.mdc` BEVOR du eine Datei aenderst. Wenn eine geplante Aenderung gegen eine Regel verstoesst, STOPPE sofort und melde den Verstoss statt die Aenderung durchzufuehren.

## Task-Beschreibung

Nach Deploy der bisherigen PumpSwap-SELL-Layout-Fixes scheitert die Liquidation fuer zwei bekannte Pools weiterhin. Die neue Root Cause ist jetzt eng belegt:

- `execution-engine` triggert korrekt `EnsurePumpAmmPoolAccounts(force_refresh=true)`.
- `market-data` antwortet im autoritativen Refresh-Pfad mit `ControlResponse status=Error`.
- Der exakte Fehlergrund ist: `authoritative PumpSwap SELL layout unresolved after force_refresh`.
- Ursache dafuer ist nicht mehr ein fehlender Helius-Fallback an sich, sondern dass der SELL-Layout-Observer in `pumpfun_amm.rs` reale PumpSwap-SELL-Transaktionen aus der Historie verwirft, wenn `accounts` nicht als Index-Array, sondern als Pubkey-Strings geliefert werden.

Der minimale Fix-Scope ist daher:

1. Den Observer fuer autoritatives PumpSwap-SELL-Layout so erweitern, dass er **beide** RPC-Formate versteht:
   - `accounts` als numerische Indizes
   - `accounts` als direkte Pubkey-Strings
2. Dadurch muss `resolve_authoritative_sell_layout_for_force_refresh(...)` fuer die betroffenen Pools aus TX-Historie wieder `Base` bzw. `Extended { third_meta }` ableiten koennen.
3. `force_refresh=true` darf fuer diese Pools danach nicht mehr in `Unknown` enden, wenn auf Helius/RPC passende SELL-Evidenz vorhanden ist.
4. Keine neue Architektur. Kein State-Decode-Umbau. Kein Umbau des Builders. Kein neuer Engine-Reparaturpfad.

## Harte Evidenz / exakter Root Cause

Aktueller relevanter Kontrollfluss:

1. `market-data` verarbeitet `EnsurePumpAmmPoolAccounts(force_refresh=true)`.
2. `resolve_authoritative_sell_layout_for_force_refresh(...)` versucht autoritatives SELL-Layout zu bestimmen.
3. Wenn lokale Historie fehlt, wird bounded RPC/Helius-Historie verwendet.
4. In realen betroffenen SELL-Transaktionen liefert das RPC aber `accounts` teils als **Pubkey-String-Liste**.
5. Der aktuelle Observer erwartet dort effektiv ein **Index-Array** und verwirft diese Instruktionen.
6. Ergebnis: kein beobachtbares SELL-Layout, also `PumpAmmAuthoritativeSellLayout::Unknown`.
7. Dadurch bleibt `sell_layout_ready = false`.
8. Dadurch publiziert `market-data` bei `force_refresh=true` den Fehler:
   - `authoritative PumpSwap SELL layout unresolved after force_refresh`

Das ist der exakte Fehlergrund fuer `ControlResponse status=Error`.

## Betroffene Runtime-Beispiele

Betroffene Pools / Mints:

- Mint `E7UaWyQoDgvUTvgQLxbR3oVyYpf3eq2hN95RzrwQpump`
  - Pool `B8bvg3KzXzGAq51QjirhPTw5ChhiZWn2kNwvQd3YZFN8`
- Mint `GwQjXZvDTVVWyadJAvjx9upEZsFFToVQHY5NRrZ6wzTR`
  - Pool `5rNMGrJ3V2vUY3GAuxiVKZmKCn6c5N6n7Ld5EWvgceVX`

Relevante Beobachtung aus echter Helius-Historie:

- erfolgreiche PumpSwap-SELLs mit `acct_len = 24` existieren
- aber `accounts` ist dort teilweise von der Form:
  - `["B8bvg3Kz...", "8vEKCs6U...", "ADyA8hde...", ...]`
- also **Pubkeys direkt**, nicht `[0, 2, 3, ...]`

Genau deshalb greift der Helius-Fallback inhaltlich aktuell nicht: Historie ist da, aber der Observer dekodiert das Instruktionsformat unvollstaendig.

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

## Bestehendes Pattern

Bitte an das bereits bestehende Parser-/Observer-Muster anknuepfen, statt einen neuen Nebenpfad zu bauen:

- `resolve_authoritative_sell_layout_for_force_refresh(...)`
- `observe_authoritative_sell_layout_from_tx_history_with_rpc(...)`
- `pump_amm_sell_layout_observation_from_parsed_swap_ix(...)`
- `pump_amm_pool_static_from_parsed_swap_ix(...)`

Die richtige Richtung ist **Parser-Erweiterung**, nicht Architekturwechsel.

Konkretes Pattern:

1. Bestehende Index-Array-Unterstuetzung muss erhalten bleiben.
2. Zusaetzlich soll derselbe Beobachter direkte Pubkey-Accountlisten akzeptieren.
3. Die Ableitung von:
   - Pool
   - Mint
   - `Base` vs. `Extended`
   - `third_meta`
   muss fuer beide Formate konsistent sein.
4. Wenn bei einem 24er-SELL das dritte Meta nicht belastbar extrahiert werden kann, darf das Layout **nicht** faelschlich als autoritativ-ready gelten.

## Erwartete technische Richtung

Wahrscheinlich minimal und passend:

1. Fuehre fuer geparste Swap-Instruktionen eine gemeinsame Abstraktion fuer Account-Referenzen ein:
   - entweder Indizes in `account_keys`
   - oder direkte Pubkeys
2. Nutze diese Abstraktion im bestehenden PumpSwap-SELL-Observer.
3. Halte die bestehende 21/24-Layout-Logik unveraendert im Sinn:
   - `21` => `Base`
   - `24` => `Extended { third_meta }`
4. Keine Aenderung an der Semantik von `force_refresh`.
5. Keine Aufweichung von `sell_layout_ready`.

## Akzeptanzkriterien

- Der Observer verarbeitet PumpSwap-SELLs aus TX-Historie sowohl bei Index-Accounts als auch bei Pubkey-Accounts.
- Fuer die betroffenen Pools kann der autoritative Refresh-Pfad wieder ein SELL-Layout beweisen, statt in `Unknown` zu enden.
- `force_refresh=true` endet nicht mehr faelschlich mit `authoritative PumpSwap SELL layout unresolved after force_refresh`, wenn passende SELL-Historie vorhanden ist.
- Das bestehende 21/24-Modell bleibt erhalten.
- Kein neuer Hot-Path-RPC.
- Kein globales Hardcoding fuer SELL-Metas.
- Keine neuen Workarounds in `execution-engine`.
- Fokussierte Regressionstests decken beide Accountformate ab.

## Erlaubte Dateien

- `src/solana/dex/pumpfun_amm.rs`
- enge, direkt zugehoerige Tests im Impl-Repo

## Verboten

- Keine Aenderungen an `src/bin/market_data.rs`, ausser es ist absolut minimal noetig, um einen rein parserseitigen Compile-/Test-Fix zu verdrahten. Bevorzugt: gar nicht anfassen.
- Keine Aenderungen in `execution-engine`
- Kein Eval-Repo
- Kein globales Hardcoding `SELL = 24`
- Kein neuer lokaler Truth-/Fallback-Pfad
- Kein State-Decode-Umbau aus Geyser/Pool-State in diesem Scope
- Kein Multi-DEX-Refactor

## Pruef-Befehle

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --quiet
```

## Erwarteter Abschlussbericht

Bitte am Ende kurz nennen:

- welche STOP-CHECKs geprueft wurden
- welche Funktion vorher Pubkey-Accountlisten verworfen hat
- wie der Parser jetzt beide Accountformen behandelt
- welche Tests die beiden Formate abdecken
- warum dadurch `force_refresh` jetzt wieder autoritatives SELL-Layout ableiten kann
