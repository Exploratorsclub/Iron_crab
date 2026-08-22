# IronCrab Scripts & Deployment

Convenience Scripts für Build, Deployment und lokale Entwicklung.

**Stand:** 2026-08-22. Production-Deploy bleibt Maintainer-Sache (`docs/RUNBOOK_PROD.md`). Onboarding: [CONTRIBUTING.md](../CONTRIBUTING.md).

## Production Deployment (Linux Server)

### deploy.sh / deploy_new.sh
Das Haupt-Deployment-Script für den Production-Server.

```bash
# Standard-Deployment (Build + Install + Restart)
./deploy.sh

# Nur bestimmten Service neu deployen
./deploy.sh --component execution-engine

# Ohne Rebuild (nur Service-Restart)
./deploy.sh --skip-build

# Legacy Monolith (deprecated)
./deploy.sh --legacy
```

`deploy.sh` ist ein Wrapper um `deploy_new.sh`. Das Script:
1. Pullt von `architecture-rebuild`
2. Baut die **fünf** Rust-Binaries (release): `market-data`, `momentum-bot`, `arb-strategy`, `execution-engine`, `position-manager`
3. Setzt Python venv für control-plane auf
4. Installiert systemd-Units inkl. `position-manager`
5. Startet die Trading-Services (siehe Tabelle unten)

`docs/systemd/ironcrab.target` listet in `Wants=` derzeit sechs Units **ohne** `position-manager`; `deploy_new.sh` startet `position-manager` trotzdem. Live-Status immer per `systemctl` prüfen.

### Service Management (Server)
```bash
# Alle Services
sudo systemctl start ironcrab.target
sudo systemctl stop ironcrab.target
sudo systemctl restart ironcrab.target

# Einzelne Services
sudo systemctl restart execution-engine
sudo systemctl status momentum-bot
journalctl -u market-data -f
```

## Local Development (Windows)

### run_local.ps1
SSH-Tunnel zum Server + optionales lokales UI.

```powershell
# Tunnel + UI starten
.\run_local.ps1 -Action start -Host ironcrab-prod

# Ohne SSH-Config Alias
.\run_local.ps1 -Action start -Host 109.230.239.43 -User ironcrab -Port 2222

# Nur Tunnel (ohne UI)
.\run_local.ps1 -Action start -NoUi

# Status prüfen
.\run_local.ps1 -Action status

# Stoppen
.\run_local.ps1 -Action stop
```

Forwarded Ports:
- `8080` → Control Plane API
- `9801-9805` → Prometheus Metrics (inkl. position-manager)
- `3000` → Grafana
- `9090` → Prometheus

### run_ui.ps1 / run_ui.cmd
Startet das lokale Vite UI (nach Tunnel-Aufbau).

```powershell
# PowerShell
.\run_ui.ps1

# Oder CMD (falls PowerShell npm blockt)
run_ui.cmd
```

### build.ps1
Lokaler Build für Entwicklung.

```powershell
# Debug Build
.\build.ps1

# Release Build
.\build.ps1 -Release

# Hilfe
.\build.ps1 -Help
```

## Unix/Linux Scripts

### build.sh
```bash
# Debug Build
./build.sh

# Release Build
./build.sh --release
```

### run.sh / run_new.sh
Lokales manuelles Starten (für Dev/Test, nicht für Production).

```bash
# run_new.sh startet die Multi-Prozess-Binaries
./run_new.sh --config my_config.server.toml
```

**Hinweis**: Auf dem Production-Server immer `deploy.sh` und systemd verwenden, nicht `run_new.sh`.

## Built Binaries (Multi-Process)

| Binary | Port | Zweck |
|--------|------|-------|
| `market-data` | 9801 | Geyser-Ingest, Pool Discovery, MarketEvents |
| `momentum-bot` | 9802 | EARLY + ESTABLISHED, nur TradeIntents |
| `arb-strategy` | 9803 | Multi-Pool Arbitrage, nur TradeIntents |
| `execution-engine` | 9804 | Single-Signer, Tx Build/Sim/Send |
| `position-manager` | 9805 | Keyless Positions-KV / PositionAuthority |

Python-Services:
- `control_plane/main.py` — REST API, Config, Kill-Switch (8080)
- `scripts/trades_server.py` — Grafana Infinity Datasource (9899)

### trades_server Run-Mode Performance (P174)

`GET /trades?mode=run` loads **today** from `recent_trades` + `execution_results*` (rotated segments).
**Yesterday** is tail-only (~500 recent lines) for prev-run context — no full execution scan.

After deploying P174, remove any prod hotfix override:

```bash
# /etc/systemd/system/trades-server.service.d/override.conf
# Delete IRONCRAB_TRADES_DAYS_LOOKBACK=0 (and reload/restart trades-server)
sudo systemctl daemon-reload && sudo systemctl restart trades-server
```

Env vars: `IRONCRAB_TRADES_CACHE_TTL_SEC` (default 15), `IRONCRAB_TRADES_RUN_PREV_RECENT_TAIL`, `IRONCRAB_TRADES_RUN_PREV_EXEC_TAIL`.

## Prerequisites

1. **Rust 1.89+**: Install from https://rustup.rs/
2. **Python 3.11+**: Für control-plane und trades-server
3. **NATS Server**: IPC zwischen Prozessen
4. **Configuration**: `my_config.server.toml` (basierend auf `config.example.toml`)

## Quick Start (Server)

1. SSH zum Server:
   ```bash
   ssh ironcrab-prod
   ```

2. Deploy:
   ```bash
   cd ~/Iron_crab
   ./deploy.sh
   ```

3. Status prüfen:
   ```bash
   sudo systemctl status ironcrab.target
   ```

## Quick Start (Lokale Entwicklung)

1. Tunnel aufbauen:
   ```powershell
   .\run_local.ps1 -Action start -Host ironcrab-prod
   ```

2. UI öffnen: http://localhost:5173

3. Control Plane API: http://localhost:8080

## Metrics Endpoints

| Service | Port | URL |
|---------|------|-----|
| market-data | 9801 | http://localhost:9801/metrics |
| momentum-bot | 9802 | http://localhost:9802/metrics |
| arb-strategy | 9803 | http://localhost:9803/metrics |
| execution-engine | 9804 | http://localhost:9804/metrics |
| position-manager | 9805 | http://localhost:9805/metrics |
| Control Plane | 8080 | http://localhost:8080 |
| Trades API | 9899 | http://localhost:9899/trades |

Fuer PumpSwap Async-Healing im `execution-engine` sind aktuell diese Counter
relevant:

- `pumpswap_hot_path_healing_trigger_total`
- `pumpswap_hot_path_healing_cooldown_suppressed_total`
- `pumpswap_hot_path_healing_async_publish_success_total`
- `pumpswap_hot_path_healing_async_publish_fail_total`
- `pumpswap_hot_path_healing_skipped_no_nats_total`

## Grafana Dashboards

Import aus `docs/`:
- `grafana_multiprocess_dashboard.json` — Haupt-Dashboard
- `grafana_arbitrage_dashboard.json` — Arb-spezifisch
- `grafana_sniper_dashboard.json` — Momentum-spezifisch

## Siehe auch

- [RUNBOOK_PROD.md](RUNBOOK_PROD.md) — Production (Maintainer)
- [VALIDATOR_SETUP.md](VALIDATOR_SETUP.md) — Agave + Geyser
- [LOCAL_SETUP.md](LOCAL_SETUP.md) — Lokale Entwicklung
- Spec: Iron_crab-eval `docs/spec/TARGET_ARCHITECTURE.md`