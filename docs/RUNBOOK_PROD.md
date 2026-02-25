# IronCrab — Production Runbook (Multi-Process Architecture)

Go-Live Anleitung für den Betrieb auf dem Validator-Server. Der Bot läuft auf dem gleichen Server wie der Agave Validator für minimale Latenz.

## Architektur-Übersicht

IronCrab verwendet eine **Multi-Prozess-Architektur** mit 6 separaten Services:

| Service | Port | Funktion |
|---------|------|----------|
| `market-data` | 9801 | Geyser-Ingest, Pool Discovery, MarketEvents |
| `momentum-bot` | 9802 | Strategy: EARLY + ESTABLISHED Policies |
| `arb-strategy` | 9803 | Strategy: Multi-Pool Arbitrage |
| `execution-engine` | 9804 | Single-Signer, Tx Build/Sim/Send |
| `control-plane` | 8080 | REST API, Config, Kill-Switch |
| `trades-server` | 9899 | Grafana Infinity Datasource |

**NATS** ist der IPC-Layer zwischen den Prozessen.

```
Geyser/RPC
    │
    ▼
market-data ── MarketEvents ──► momentum-bot ── TradeIntents ──► execution-engine
                            └─► arb-strategy ─────────────────┘
                                                                      │
                                                    ExecutionResults ◄┘
                                                                      │
                                              control-plane / trades-server
```

## Voraussetzungen

- Debian 13 (oder kompatible Linux-Distribution)
- Rust Toolchain installiert (`rustup`)
- Python 3.11+ mit venv
- NATS Server (`nats-server` package)
- Agave Validator auf dem gleichen Server (siehe `docs/VALIDATOR_SETUP.md`)
- Funded Keypair unter `/home/ironcrab/.config/solana/id.json`
- Mindestens 0.5–1.0 SOL für Rent und Fees

## Dateien

| Datei | Zweck |
|-------|-------|
| `my_config.server.toml` | Produktions-Config (alle Rust-Binaries) |
| `deploy.sh` | Wrapper-Script → ruft `deploy_new.sh` auf |
| `deploy_new.sh` | Build + Systemd Install + Restart |
| `docs/systemd/*.service` | Service-Files für alle 6 Prozesse |
| `docs/systemd/ironcrab.target` | Orchestriert alle Services |
| `docs/grafana_*.json` | Grafana Dashboards |

## Einmalige Konfiguration

1) **Config anpassen** (`my_config.server.toml`):
   ```toml
   [solana]
   rpc_url = "http://127.0.0.1:8899"      # Lokaler Validator
   ws_url = "ws://127.0.0.1:8900"         # Lokaler Validator WS
   keypair_path = "/home/ironcrab/.config/solana/id.json"
   
   [geyser]
   endpoint = "http://127.0.0.1:10000"    # Lokaler Geyser gRPC
   
   [nats]
   url = "nats://127.0.0.1:4222"
   ```

2) **Keypair Balance prüfen**: `solana balance`

3) **NATS Server aktivieren**:
   ```bash
   sudo systemctl enable --now nats-server
   ```

4) **Optional**: Grafana Dashboards importieren

## Deployment

### Standard-Deployment (empfohlen)
```bash
./deploy.sh
```

Dies führt automatisch aus:
1. `git pull origin architecture-rebuild`
2. Cargo Build aller 4 Rust-Binaries (release)
3. Python venv Setup für control-plane
4. Systemd Services installieren
5. `ironcrab.target` starten (alle 6 Services)
6. Status-Check

### Optionen
```bash
# Nur bestimmten Service neu deployen
./deploy.sh --component execution-engine

# Ohne Rebuild (nur Service-Restart)
./deploy.sh --skip-build

# Legacy Monolith (deprecated)
./deploy.sh --legacy
```

## Service Management

### Alle Services starten/stoppen
```bash
sudo systemctl start ironcrab.target   # Startet alle 6 Services
sudo systemctl stop ironcrab.target    # Stoppt alle 6 Services
sudo systemctl restart ironcrab.target # Restart alle
```

### Einzelne Services
```bash
sudo systemctl restart execution-engine
sudo systemctl status momentum-bot
sudo systemctl stop arb-strategy
```

### Logs
```bash
# Live Logs einzelner Service
journalctl -u execution-engine -f
journalctl -u market-data -f
journalctl -u momentum-bot -f
journalctl -u arb-strategy -f
journalctl -u control-plane -f
journalctl -u trades-server -f

# Alle IronCrab Services kombiniert
journalctl -u 'market-data' -u 'momentum-bot' -u 'arb-strategy' -u 'execution-engine' -u 'control-plane' -f

# Letzte 100 Zeilen
journalctl -u execution-engine -n 100 --no-pager
```

## Endpoints

| Endpoint | URL | Zweck |
|----------|-----|-------|
| market-data Metrics | `http://localhost:9801/metrics` | Prometheus |
| momentum-bot Metrics | `http://localhost:9802/metrics` | Prometheus |
| arb-strategy Metrics | `http://localhost:9803/metrics` | Prometheus |
| execution-engine Metrics | `http://localhost:9804/metrics` | Prometheus |
| Control Plane API | `http://localhost:8080` | REST API |
| Trades API | `http://localhost:9899/trades` | Grafana Infinity |

## Dashboard-Interpretation (WSOL-first)

Aktuelle Architektur handelt primär in WSOL (kein Auto-Unwrap). Daher gelten
folgende Interpretations-Regeln:

- **WSOL-Balance (ATA)** = Trading-Liquiditaet (entscheidend fuer Buy/Scale-In)
- **WSOL-Wrap-Events** = Nachschub/Buffer fuer Trading
- **Open Positions** = Strategie-Sicht (korrekt nur mit aktivem Reconciliation)
- **ExecutionResults (Confirmed/Sent/Rejected)** = reale Handelsausfuehrung
- **Available WSOL** = Lock/Budget-Metrik (WSOL ATA Balance, nicht native SOL)

Weniger aussagekraeftig:

- **Wallet SOL Balance** als Trading-Kapital (WSOL ist primär)

Merksatz: **"WSOL ist Kapital, SOL ist Reserve."**

Hinweis (Wallet-Snapshot JetStream):
Die Wallet-Snapshot-Recovery ist aktuell nicht nach Wallet gefiltert. Das ist
ok, solange nur ein Wallet genutzt wird. Bei Multi-Wallet-Betrieb muss ein
Wallet-Filter im Consumer ergaenzt werden.

## JSONL Logs (Decision Records)

Alle Prozesse schreiben append-only JSONL nach `trade_logs/`:

```bash
# Decision Records (execution-engine)
ls -1t trade_logs/decisions/
tail -f trade_logs/decisions/decision_records-$(date +%Y%m%d).jsonl

# Trade Intents (momentum-bot, arb-strategy)
ls -1t trade_logs/intents/
ls -1t trade_logs/arb_intents/

# Market Events (market-data)
ls -1t trade_logs/market_events/
```

### Arb Reject Analysis (häufige Debug-Aufgabe)
```bash
# Letzte 400 Arb-Decision Records, Reject Codes zählen
f=$(ls -1t trade_logs/decisions/decision_records-*.jsonl | head -n 1)
tail -n 400 "$f" | grep '"source":"arb-strategy"' | \
  grep -o '"reason_code":"[^"]*"' | cut -d'"' -f4 | sort | uniq -c | sort -nr
```

## Safety Checklist

- [ ] Starte mit kleinen Limits: `max_buy_sol = 0.02`
- [ ] `require_freeze_auth_none = true` beibehalten
- [ ] `send_enabled = false` für Dry-Run Tests
- [ ] Logs auf Rate Limiting und Errors überwachen
- [ ] Decision Records auf `simulation_failed` prüfen
- [ ] Limits erst nach mehreren erfolgreichen Sessions erhöhen

## Troubleshooting

| Problem | Diagnose | Lösung |
|---------|----------|--------|
| Service startet nicht | `journalctl -u <service> -n 50` | Fehlermeldung lesen |
| NATS Verbindung fehlgeschlagen | `systemctl status nats-server` | NATS starten |
| Validator nicht erreichbar | `curl http://127.0.0.1:8899/health` | Validator prüfen |
| Geyser Verbindung fehlgeschlagen | market-data Logs prüfen | Geyser Plugin Config |
| Keypair Permissions | `ls -la ~/.config/solana/id.json` | `chmod 600` |
| Keine Intents ankommen | momentum-bot Logs | Filter zu strikt? |
| Simulation immer failed | execution-engine Logs | RPC/Balance Problem |

### Service-spezifische Checks

```bash
# market-data: Geyser Verbindung
journalctl -u market-data | grep -i "geyser\|connected"

# momentum-bot: Intent Rate
journalctl -u momentum-bot | grep "TradeIntent"

# execution-engine: Lock/Simulation
journalctl -u execution-engine | grep -i "lock\|simulation"

# NATS Topics prüfen
nats sub "ironcrab.v1.>" --count 5
```

## CPU Pinning

Die Rust-Services sind auf dedizierte CPUs gepinnt (vermeidet Validator-Interferenz):

| Service | CPUAffinity |
|---------|-------------|
| market-data | 56-59 |
| momentum-bot | 52-55 |
| arb-strategy | 48-51 |
| execution-engine | 60-63 |

Konfiguriert in `docs/systemd/*.service`.

## Kill-Switch

```bash
# Via Control Plane API
curl -X POST http://localhost:8080/kill

# Via NATS direkt
nats pub ironcrab.control.kill "{}"

# Manuell alle Services stoppen
sudo systemctl stop ironcrab.target
```

## Lokale Entwicklung (Windows/macOS)

Für lokale UI-Entwicklung mit SSH-Tunnel zum Server:

```powershell
# Windows: Tunnel + UI starten
.\run_local.ps1 -Action start -Host ironcrab-prod

# Status prüfen
.\run_local.ps1 -Action status

# Stoppen
.\run_local.ps1 -Action stop
```

Siehe `.github/copilot-instructions.md` für Details.

## Siehe auch

- [Iron_crab-eval/docs/spec/](https://github.com/Exploratorsclub/Iron_crab-eval) — Spezifikation (TARGET_ARCHITECTURE, ROLE_SEPARATION, DEFINITION_OF_DONE, STORAGE_CONVENTIONS)
- [VALIDATOR_SETUP.md](VALIDATOR_SETUP.md) — Validator + Geyser Konfiguration
