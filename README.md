# IronCrab – Solana‑First Tradingbot (Rust)

Version: **0.4.0** (Agave / Solana 3.x)

Multi-Process-Architektur mit NATS IPC, JetStream State Recovery und Geyser-First Data Plane.

## Spezifikation & Regeln

| Dokument | Ort |
|----------|-----|
| TARGET_ARCHITECTURE | [Iron_crab-eval/docs/spec/](https://github.com/Exploratorsclub/Iron_crab-eval) |
| DEFINITION_OF_DONE | Iron_crab-eval/docs/spec/ |
| **INVARIANTS** | `docs/INVARIANTS.md` (niemals verletzen) |
| **KNOWN_BUG_PATTERNS** | `docs/KNOWN_BUG_PATTERNS.md` (bei Bugs prüfen) |
| RUNBOOK_PROD | `docs/RUNBOOK_PROD.md` |

Die vollständige Spec liegt im [Iron_crab-eval](https://github.com/Exploratorsclub/Iron_crab-eval)-Repo; siehe `docs/SPEC_LOCATION.md`.

## Kernprinzipien (P0)

- **Single-Signer**: Nur `execution-engine` lädt Keys und signiert/sendet.
- **Intent-only**: Strategien/Worker erzeugen nur `TradeIntent`s.
- **Simulate-gated**: Simulation-fail ⇒ nie senden (besonders Arbitrage).
- **Decision Records**: Jede Entscheidung ist forensisch nachvollziehbar.
- **GEYSER-FIRST**: Hot Path nutzt nur Geyser/LivePoolCache; RPC nur im Cold Path (Liquidation, Bootstrap, manuelle Aktionen).

## Architektur (Datenfluss)

```text
Geyser/RPC
    │
    ▼
market-data (Geyser Ingest, Pool Discovery)
    │  MarketEvents, WalletBalanceUpdates
    ▼
NATS (ironcrab.v1.*)
    │
    ├─► momentum-bot (EARLY/ESTABLISHED) ─► TradeIntents ─┐
    ├─► arb-strategy (Multi-Pool Arbitrage) ─► TradeIntents ─┤
    │                                                        │
    └─► execution-engine ◄────────────────────────────────────┘
            │  LivePoolCache, QuoteCalculator, CrossDexHandler
            ▼
        Plan → Simulate → Send → Confirm
            │
            ▼
        DecisionRecords, ExecutionResults → NATS / JSONL
```

## Binaries

| Binary | Port | Aufgabe |
|--------|------|---------|
| market-data | 9801 | Geyser Ingest, Pool Discovery, MarketEvents, WalletBalanceUpdates |
| momentum-bot | 9802 | EARLY/ESTABLISHED Regime, TradeIntents |
| arb-strategy | 9803 | Multi-Pool Arbitrage, TradeIntents |
| execution-engine | 9804 | Intent-Verarbeitung, TX-Build, Signieren, Senden (einziger mit Keys) |
| control-plane | 8080 | REST API, Kill-Switch, Config |
| trades-server | 9899 | Grafana Infinity Datasource |

Hilfs-Tools: `raydium_pools`, `sell-all`, `latency_stress`, `pump-amm-tx-probe`, `manual-swap`, `burn-manual-keyless`, `setup-alt`.

## DEX-Unterstützung (Geyser-First)

| DEX | Discovery | Pool State | Swaps |
|-----|-----------|------------|-------|
| Raydium AMM V4 | Geyser Account | Geyser Account | ✅ |
| Raydium CPMM | Geyser Account | Geyser Account | ✅ |
| Orca Whirlpool | Geyser Account | Geyser Account | ✅ |
| Meteora DLMM | Geyser Account | Geyser + PDA | ✅ |
| PumpFun | Geyser TX | Geyser TX | ✅ |
| PumpSwap (PumpFun AMM) | Geyser TX | Geyser Account | ✅ |

## Quickstart

### Voraussetzungen

- Rust (1.89.0 empfohlen)
- Solana RPC (lokal oder Provider)
- NATS (z.B. `nats://localhost:4222`)
- Geyser gRPC (für market-data)

### Konfiguration

```powershell
cp config.example.toml config.toml
# config.toml anpassen: [solana].rpc_url, [geyser].url, [execution].keypair_path
```

### Build & Run

```powershell
cargo build --release

# Services einzeln starten (in getrennten Terminals):
cargo run --release --bin market-data    -- --config config.toml
cargo run --release --bin momentum-bot    -- --config config.toml
cargo run --release --bin arb-strategy    -- --config config.toml
cargo run --release --bin execution-engine -- --config config.toml
cargo run --release --bin control-plane  -- --config config.toml
```

Oder über Skripte: `run_new.ps1` / `run_new.sh` (siehe `docs/SCRIPTS_README.md`).

## Eval-Tests (Level 5)

Eval-Tests liegen im Sibling-Repo [Iron_crab-eval](https://github.com/Exploratorsclub/Iron_crab-eval). CI cloned es als `Iron_crab-eval` und führt `cargo test` aus.

```powershell
cd ..\Iron_crab-eval
cargo test
```

## Golden Replay

Deterministische Replay-Tests für die Execution Pipeline:

```powershell
cargo run --bin execution-engine -- --replay --replay-intents tests/fixtures/golden_replays/liquidation_6005_retry_intents.jsonl --replay-output target/replay_out
```

Fixtures: `tests/fixtures/golden_replays/` (z.B. `normal_trade`, `rejected_trade`, `sim_failed`, `liquidation_6005_retry`).

## Keypair & Sicherheit

**Keys nur in execution-engine.** Alle anderen Binaries sind keyless.

Keypair-Quellen (Priorität):

1. `IRONCRAB_KEYPAIR_JSON` – JSON-Array (32/64 Bytes)
2. `IRONCRAB_KEYPAIR_B64` – Base64
3. `IRONCRAB_KEYPAIR_PATH` – Dateipfad

Strikte Pfad-Validierung: `IRONCRAB_KEYPAIR_STRICT=1`, `IRONCRAB_KEYPAIR_ALLOWED_DIRS=...`

## Tests

```powershell
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```

Feature `test_helpers`: zusätzliche Hilfen für Unit-/Integrationstests (nicht im Release).

## Features

- `python` – Python-Strategien (pyo3)
- `jito` – Jito Bundle Integration
- `notify_watch` – File-Watching für Config-Hot-Reload
- `legacy_monolith` / `legacy_sniper` – Legacy-Modi (nicht empfohlen)
- `rpc_fallback` – RPC-Fallback für Bootstrap (nicht für Produktion)

## Validator-Entrypoint-Latenz

Schnellste Mainnet-Entrypoints ermitteln:

```bash
docs/tools/entrypoint_latency_test_v2.sh --with-gossip --rpc
```

Ergebnisse in `/tmp/entrypoints_latency.csv`; Top-Einträge als `--entrypoint host:8001` nutzen.

## Metriken & Monitoring

| Service | Port | Endpoint |
|---------|------|----------|
| market-data | 9801 | `/metrics` |
| momentum-bot | 9802 | `/metrics` |
| arb-strategy | 9803 | `/metrics` |
| execution-engine | 9804 | `/metrics` |

Prometheus-Scrape-Config: `docs/grafana_dashboard_example.json`

## Wichtige Module (execution-engine)

- **LivePoolCache**: Geyser-basierter Pool-State-Cache
- **QuoteCalculator**: min_out aus LivePoolCache
- **WsolManager**: WSOL Wrap/Unwrap
- **AccountJanitor**: ATA-Cleanup, Rent-Recovery
- **CrossDexHandler**: Einheitliche DEX-Swaps
- **6005-Retry**: Bei PumpFun BondingCurveComplete (6005) → Retry mit PumpSwap AMM

## Weitere Dokumentation

- `docs/RUNBOOK_PROD.md` – Produktionsbetrieb
- `docs/VALIDATOR_SETUP.md` – Validator-Setup
- `docs/CONFIG_SCHEMA.md` – Konfigurationsschema
- `CHANGELOG.md` – Änderungsprotokoll
