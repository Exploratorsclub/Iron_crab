# IronCrab – Solana‑First Tradingbot (Rust)

Version: **0.4.0** (Agave / Solana 3.x)

Multi-Process-Architektur mit NATS IPC, JetStream State Recovery und Geyser-First Data Plane.

**Mitentwickeln:** [CONTRIBUTING.md](CONTRIBUTING.md) — gemeinsamer Branch ist **`architecture-rebuild`**. Spec und Eval-Tests: [Iron_crab-eval](https://github.com/Exploratorsclub/Iron_crab-eval) (`main`). `architecture-rebuild-next` ist nur die aktive Maintainer-Entwicklung.

## Spezifikation & Regeln

| Dokument | Ort |
|----------|-----|
| **CONTRIBUTING** | [CONTRIBUTING.md](CONTRIBUTING.md) |
| TARGET_ARCHITECTURE | [Iron_crab-eval/docs/spec/](https://github.com/Exploratorsclub/Iron_crab-eval/blob/main/docs/spec/TARGET_ARCHITECTURE.md) |
| DEFINITION_OF_DONE | Iron_crab-eval/docs/spec/ |
| **INVARIANTS** | `docs/INVARIANTS.md` (P0) und [Eval-Spec](https://github.com/Exploratorsclub/Iron_crab-eval/blob/main/docs/spec/INVARIANTS.md) |
| **KNOWN_BUG_PATTERNS** | `docs/KNOWN_BUG_PATTERNS.md` (bei Bugs prüfen) |
| RUNBOOK_PROD | `docs/RUNBOOK_PROD.md` (Maintainer) |

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
| arb-strategy | 9803 | Multi-Pool Arbitrage, TradeIntents (eigenes Binary) |
| execution-engine | 9804 | Intent-Verarbeitung, TX-Build, Signieren, Senden (einziger mit Keys) |
| position-manager | 9805 | Einziger Writer KV `POSITION_AUTHORITY` (Positions-Daten-SSOT) |
| control-plane | 8080 | REST API, Kill-Switch, Config (**Python**, nicht Cargo) |
| trades-server | 9899 | Grafana Infinity Datasource (**Python**, `scripts/trades_server.py`) |

Hilfs-Tools (Cargo): `raydium-pools`, `sell-all`, `latency-stress`, `pump-amm-tx-probe`, `manual-swap`, `burn-manual-keyless`, `setup-alt`.

## DEX-Unterstützung (Geyser-First)

| DEX | Discovery | Pool State | Swaps |
|-----|-----------|------------|-------|
| Raydium AMM V4 | Geyser Account | Geyser Account | ✅ |
| Raydium CPMM | Geyser Account | Geyser Account | ✅ |
| Orca Whirlpool | Geyser Account | Geyser Account | ✅ |
| Meteora DLMM | Geyser Account | Geyser + PDA | ✅ |
| Meteora CPMM | Geyser Account | Geyser Account | ✅ |
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
# Anpassen: [solana].rpc_url, [solana].geyser_grpc_url, [solana].keypair_path
# market-data Geyser: --geyser-url oder GEYSER_URL (Default :10000)
# execution-engine Keys: IRONCRAB_KEYPAIR_PATH (nicht [execution].keypair_path)
```

### Build & Run

```powershell
cargo build --release

# Services einzeln starten (in getrennten Terminals):
cargo run --release --bin market-data       -- --config config.toml
cargo run --release --bin momentum-bot      -- --config config.toml
cargo run --release --bin arb-strategy      -- --config config.toml
cargo run --release --bin execution-engine  -- --config config.toml
cargo run --release --bin position-manager  -- --config config.toml
# control-plane ist Python: uvicorn / python control_plane/main.py (Port 8080)
```

Oder über Skripte: `run_new.ps1` / `run_new.sh` (siehe `docs/SCRIPTS_README.md`).

## Eval-Tests (Level 5)

Eval-Tests liegen im Sibling-Repo [Iron_crab-eval](https://github.com/Exploratorsclub/Iron_crab-eval).

- **Impl-CI** führt nach `fmt`/`clippy`/`cargo test` den Job **Eval (Level 5)** aus: volle Suite gegen den PR-Checkout.
- Das Eval-Repo selbst hat auf `main` nur ein **schlankes** Gate (fmt/check/build/clippy **ohne** Tests) — Details in `docs/LEVEL5_EVAL_WORKFLOW.md` und [Iron_crab-eval/CONTRIBUTING.md](https://github.com/Exploratorsclub/Iron_crab-eval/blob/main/CONTRIBUTING.md).

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
| position-manager | 9805 | `/metrics` |

Prometheus-Scrape-Config: `docs/grafana_dashboard_example.json`

## Wichtige Module (execution-engine)

- **LivePoolCache**: Geyser-basierter Pool-State-Cache
- **QuoteCalculator**: min_out aus LivePoolCache
- **WsolManager**: WSOL Wrap/Unwrap
- **AccountJanitor**: ATA-Cleanup, Rent-Recovery
- **CrossDexHandler**: Einheitliche DEX-Swaps
- **6005-Retry**: Bei PumpFun BondingCurveComplete (6005) → Retry mit PumpSwap AMM

## Weitere Dokumentation

- [CONTRIBUTING.md](CONTRIBUTING.md) – Onboarding für Mitentwickler (Code / Spec / Tests)
- `docs/RUNBOOK_PROD.md` – Produktionsbetrieb (Maintainer)
- `docs/VALIDATOR_SETUP.md` – Agave + Yellowstone Geyser (aktueller Stand, kein Januar-2026-Rollout)
- `docs/CONFIG_SCHEMA.md` – Konfigurationsschema
- `CHANGELOG.md` – Änderungsprotokoll
