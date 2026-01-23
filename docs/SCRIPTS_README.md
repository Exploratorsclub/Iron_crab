# IronCrab Scripts & Deployment

Convenience Scripts für Build, Deployment und lokale Entwicklung der Multi-Prozess-Architektur.

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

`deploy.sh` ist ein Wrapper, der `deploy_new.sh` aufruft. Das Script:
1. Pullt von `architecture-rebuild` Branch
2. Baut alle 4 Rust-Binaries (release)
3. Setzt Python venv für control-plane auf
4. Installiert alle systemd Services
5. Startet `ironcrab.target` (alle 6 Services)

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
- `9801-9804` → Prometheus Metrics
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

Nach dem Build sind folgende Binaries verfügbar:

| Binary | Zweck |
|--------|-------|
| `market-data` | Geyser-Ingest, Pool Discovery, MarketEvents |
| `momentum-bot` | Strategy: EARLY + ESTABLISHED Policies |
| `arb-strategy` | Strategy: Multi-Pool Arbitrage |
| `execution-engine` | Single-Signer, Tx Build/Sim/Send |

Plus Python-Services:
- `control_plane/main.py` — REST API, Config, Kill-Switch
- `scripts/trades_server.py` — Grafana Infinity Datasource

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
| Control Plane | 8080 | http://localhost:8080 |
| Trades API | 9899 | http://localhost:9899/trades |

## Grafana Dashboards

Import aus `docs/`:
- `grafana_multiprocess_dashboard.json` — Haupt-Dashboard
- `grafana_arbitrage_dashboard.json` — Arb-spezifisch
- `grafana_sniper_dashboard.json` — Momentum-spezifisch

## Siehe auch

- [RUNBOOK_PROD.md](RUNBOOK_PROD.md) — Vollständige Production-Anleitung
- [TARGET_ARCHITECTURE.md](TARGET_ARCHITECTURE.md) — Architektur-Dokumentation
- [LOCAL_SETUP.md](LOCAL_SETUP.md) — Lokale Entwicklungsumgebung