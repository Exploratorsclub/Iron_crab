# IronCrab — Production Runbook (Debian/Linux)

Go-Live Anleitung für den Betrieb auf dem Validator-Server. Der Bot läuft auf dem gleichen Server wie der Agave Validator für minimale Latenz.

## Voraussetzungen
- Debian 13 (oder kompatible Linux-Distribution)
- Rust Toolchain installiert (`rustup`)
- Agave Validator läuft auf dem gleichen Server (siehe `docs/VALIDATOR_SETUP.md`)
- Funded Keypair unter `/home/ironcrab/.config/solana/id.json`
- Mindestens 0.5–1.0 SOL für Rent und Fees

## Dateien
- `my_config.server.toml` — Produktions-Config mit konservativen Limits
- `run.sh` — Startet den Bot mit gegebener Config
- `docs/systemd/ironcrab.service` — Systemd Service Template
- `docs/grafana_*.json` — Grafana Dashboards

## Einmalige Konfiguration
1) Config anpassen (`my_config.server.toml`):
   ```toml
   [solana]
   rpc_url = "http://127.0.0.1:8899"      # Lokaler Validator
   ws_url = "ws://127.0.0.1:8900"         # Lokaler Validator WS
   keypair_path = "/home/ironcrab/.config/solana/id.json"
   
   [geyser]
   endpoint = "http://127.0.0.1:10000"    # Lokaler Geyser gRPC
   ```
2) Keypair Balance prüfen: `solana balance`
3) Optional: Grafana Dashboard importieren

## Build
```bash
# Release Build (empfohlen für Produktion)
./build.sh --release

# Debug Build (für Entwicklung)
./build.sh
```

## Manuell starten (zum Testen)
```bash
./run.sh --release --config my_config.server.toml
```

Prometheus Metrics: `http://localhost:9898/metrics`

## Systemd Service (Produktion)

### Installation
```bash
# Service-Datei anpassen und kopieren
sudo cp docs/systemd/ironcrab.service /etc/systemd/system/
sudo nano /etc/systemd/system/ironcrab.service
# Pfade anpassen: User, Group, WorkingDirectory, ExecStart

# Aktivieren und starten
sudo systemctl daemon-reload
sudo systemctl enable --now ironcrab
sudo systemctl status ironcrab --no-pager
```

### Logs
```bash
# Live Logs
journalctl -u ironcrab -f

# Letzte 100 Zeilen
journalctl -u ironcrab -n 100
```

### Neustart nach Config-Änderung
```bash
sudo systemctl restart ironcrab
```

## Safety Checklist
- Starte mit kleinen Limits: `max_buy_sol = 0.02`, strikte Filter aktiv
- `require_freeze_auth_none = true` und LP Konzentrations-Caps beibehalten
- Logs auf Rate Limiting und Errors überwachen
- Fills und PnL via CSV Logs verifizieren (`trade_logs/`)
- Limits erst nach mehreren erfolgreichen Sessions erhöhen

## Troubleshooting
| Problem | Lösung |
|---------|--------|
| Config Validation Error | Fehlermeldung lesen, Felder/Pfade korrigieren |
| Validator nicht erreichbar | `curl http://127.0.0.1:8899/health` prüfen |
| Geyser Verbindung fehlgeschlagen | Validator Geyser Plugin Config prüfen |
| Keypair Permissions | `chmod 600 ~/.config/solana/id.json` |
| Hohe Pool-Ablehnungen | `min_pool_liquidity_sol` senken oder Decimals-Range erweitern |

## CPU Pinning
Der Bot ist auf CPUs 48-63 gepinnt (oberes Viertel), um den Validator nicht zu stören.
Siehe `docs/systemd/ironcrab.service` → `CPUAffinity=48-63`

## Siehe auch
- [VALIDATOR_SETUP.md](VALIDATOR_SETUP.md) — Validator + Geyser Konfiguration
- [GEYSER_SETUP.md](GEYSER_SETUP.md) — Geyser gRPC Plugin Details
- [BACKTESTING.md](BACKTESTING.md) — Strategie-Testing ohne Live-Trading
