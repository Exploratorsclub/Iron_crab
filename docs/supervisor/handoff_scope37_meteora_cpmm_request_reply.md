# Handoff: Scope 37 - Meteora CPMM Request/Reply in market-data

WICHTIG: Lies und befolge die STOP-CHECK Regeln in AGENTS.md und .cursor/rules/ironcrab-core.mdc BEVOR du eine Datei aenderst. Wenn eine geplante Aenderung gegen eine Regel verstoesst, STOPPE sofort und melde den Verstoss statt die Aenderung durchzufuehren.

## Task-Beschreibung

Implementiere den naechsten kleinen Cold-Path-Request/Reply-Slice fuer **Meteora CPMM**.

Aktueller Stand:
- `market-data` hat bereits autoritative `Ensure...`-Handler fuer `PumpSwap`, `PumpFun Bonding Curve`, `Orca Whirlpool`, `Meteora DLMM`, `Raydium AMM` und jetzt auch `Raydium CPMM`.
- Fuer **Meteora CPMM** existiert bereits ein **bounded cache-scoped Cold-Path-Verify-/Reserve-Fallback-Muster** in `market_data.rs`, aber **noch kein eigener Request/Reply-Control-Path**.
- Ziel ist **Paritaet zum bestehenden Meteora-DLMM-/Raydium-CPMM-Pattern**, aber nur fuer `Meteora CPMM` und nur in `market-data` + IPC-Schema.

Ziel dieses Scopes:
- Neuer `ControlRequestKind::EnsureMeteoraCpmmPoolState { base_mint }`
- Neues Top-Level-Flag `force_refresh_meteora_cpmm`
- `market-data` bekommt einen **cache-scoped** autoritativen Handler fuer Meteora CPMM:
  - optional `pool_address_hint`
  - keine globalen `getProgramAccounts`
  - nur Pools verwenden, die bereits im `LivePoolCache` / MASTER vorhanden sind
  - bounded RPC fuer Pool-Account + Vault-Reserves
  - MASTER aktualisieren, `PoolCacheUpdate` nach JetStream publizieren, terminale `ControlResponse` senden

Wichtig:
- Dies ist **kein** `execution-engine`-Scope.
- Dies ist **kein** Eval-Scope.
- Der Scope soll so klein wie moeglich bleiben und nur die autoritative Control-Plane fuer `Meteora CPMM` herstellen.

## Relevante Invarianten (Volltext)

### I-24d Cold-Path Discovery nur per Request/Reply
Wenn `execution-engine` im Cold Path fehlende Pool-Daten fuer einen Trade braucht, darf es diese Daten **nicht selbst** per lokaler Discovery/RPC beschaffen und auch **nicht lokal** in den SLAVE-Cache schreiben. Stattdessen muss `execution-engine` einen gezielten Request an `market-data` schicken, auf eine korrelierte `ControlResponse` warten und anschliessend den ueber JetStream replizierten SLAVE-Zustand verwenden. Discovery, MASTER-Write und JetStream-Publish bleiben bei `market-data`.

### I-7 Hot Path RPC-Freiheit
Im normalen Trading-Hot-Path sind keine neuen RPC-Calls erlaubt. RPC darf nur im Cold Path stattfinden und dort nur auf der dafuer vorgesehenen Seite (`market-data`), nicht als lokaler Shortcut in `execution-engine`.

### I-4 Geyser-First
Bestehende Geyser-/JetStream-First-Muster duerfen nicht durch direkte lokale RPC- oder cache-bypass Logik im `execution-engine` ersetzt werden. Autoritativer State kommt weiter aus MASTER -> JetStream -> SLAVE.

### I-9 Simulation-Gate
Es duerfen keine Transaktionen gesendet werden, die die Simulation nicht erfolgreich passiert haben. Ein Recovery-/Discovery-Schritt darf nur dazu dienen, anschliessend erneut sauber zu planen/simulieren, nicht die Simulation zu umgehen.

### I-12 Decision Record
Ein Intent darf nicht still verschwinden. Wenn der neue Request/Reply-Pfad keinen nutzbaren Zustand liefert, muss ein sauberer terminaler Fehler (`NotFound` oder `Error`) beobachtbar bleiben; kein Hang, kein stilles Drop.

## Bestehendes Pattern

Nutze **bestehende DEX-Request/Reply-Handler in `src/bin/market_data.rs`** als Vorlage, insbesondere `EnsureMeteoraDlmmPoolState` und den gerade gemergten `Raydium CPMM`-Slice:

1. **Schema-/Wire-Pattern (`src/ipc/schema.rs`)**
   - eigener `ControlRequestKind::Ensure...`
   - eigenes Top-Level `force_refresh_...` Boolean im `ControlRequest`
   - `pool_address_hint` bleibt top-level fuer backward-compatible fast path

2. **Meteora DLMM Pattern (`src/bin/market_data.rs`)**
   - wenn `!force_refresh`:
     - bei `pool_address_hint` + bereits explicit Ready -> direkt `Ok`
     - oder wenn Mint bereits explicit Ready hat -> direkt `Ok`
   - bei `force_refresh=true`:
     - **kein cache-first short-circuit**
   - candidate pools nur aus `LivePoolCache`
   - bei Hint:
     - wrong DEX row -> `Error`
     - mint mismatch -> `Error`
     - hint nicht im Cache -> `NotFound`
   - pro Candidate bounded RPC refresh
   - `Ok` nur wenn `Ready` **und** JetStream publish erfolgreich
   - `Error`, wenn MASTER zwar Ready ist, JetStream publish aber fehlschlaegt
   - `Partial` darf publiziert werden, aber es wird weiter nach `Ready` gesucht

3. **Bestehendes Meteora-CPMM-Kaltpfad-Muster (`handle_wallet_bootstrap_meteora_cpmm_verify_for_mint`)**
   - bereits cache-scoped
   - kein globaler Scan
   - `get_account` fuer Pool-Account
   - parse zu `MeteoraCpmmState`
   - per-vault RPC-Balances lesen
   - `meteora_cpmm_readiness_for_pool_cache_update(...)`
   - `PoolCacheUpdate::BalanceUpdated` mit
     - `meteora_cpmm_vaults`
     - `meteora_cpmm_onchain_mints`
     - Readiness

Dieses bestehende Bootstrap-Muster soll **nicht neu erfunden**, sondern fuer einen autoritativen `EnsureMeteoraCpmmPoolState`-Handler wiederverwendet bzw. in einen kleinen Helper extrahiert werden.

## Erwartete Aenderung

Schneide den kleinstmoeglichen Impl-Scope, der Folgendes erreicht:

1. Fuege `EnsureMeteoraCpmmPoolState { base_mint }` in `src/ipc/schema.rs` hinzu.
2. Fuege ein neues Top-Level-Flag `force_refresh_meteora_cpmm` in `ControlRequest` hinzu.
3. Ergaenze `market-data` um einen neuen Handler `EnsureMeteoraCpmmPoolState` nach dem existierenden Meteora-DLMM-/Raydium-CPMM-Muster:
   - optionaler `pool_address_hint`
   - cache-scoped candidate selection nur aus `LivePoolCache`
   - kein globaler `getProgramAccounts` scan
4. Wenn `!force_refresh`:
   - `Ok` short-circuit bei explicit Ready (Hint oder Mint)
5. Wenn `force_refresh=true`:
   - immer in den bounded RPC refresh path gehen
   - kein cache-first short-circuit
6. Pro Candidate:
   - Pool-Account per RPC laden
   - zu `MeteoraCpmmState` parsen
   - Vault-Balances per RPC laden
   - MASTER / `LivePoolCache` aktualisieren
   - Readiness mit `meteora_cpmm_readiness_for_pool_cache_update(...)` mergen
   - `PoolCacheUpdate::BalanceUpdated` mit `meteora_cpmm_vaults` + `meteora_cpmm_onchain_mints` + Readiness publizieren
7. Terminales Verhalten:
   - `Ok` nur wenn `Ready` + JetStream publish erfolgreich
   - `NotFound` wenn kein passender CPMM-Row im Cache vorhanden ist
   - `Error` wenn RPC refresh keinen JetStream-Ready-Zustand erzeugt oder JetStream publish fuer einen Ready-Row fehlschlaegt
8. Verdrahtung in beide Control-Request-Loops von `market-data`:
   - normal
   - simulation / dry-run

## Erlaubte Dateien

- `Iron_crab/src/ipc/schema.rs`
- `Iron_crab/src/bin/market_data.rs`
- `Iron_crab/src/execution/live_pool_cache.rs` **nur wenn** ein winziger Helper wirklich noetig ist

## Verboten

- Keine Aenderungen an `execution_engine.rs`
- Keine Aenderungen im Eval-Repo
- Kein globaler `getProgramAccounts`-Scan fuer diesen Request/Reply-Pfad
- Keine Erweiterung auf andere DEXe in diesem Scope
- Keine neuen Hot-Path-RPCs
- Kein lokaler Discovery-/SSOT-Shortcut ausserhalb von `market-data`

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
- welche Dateien geaendert wurden
- ob `EnsureMeteoraCpmmPoolState` + `force_refresh_meteora_cpmm` jetzt komplett verdrahtet sind
- welche Tests / Checks gelaufen sind
