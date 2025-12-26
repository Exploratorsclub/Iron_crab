# Validator Setup für IronCrab

Optimierte Konfiguration für Agave 3.0.11 Non-Voting RPC Validator.

## Aktuelle Konfiguration

Service-Datei: `/etc/systemd/system/agave-validator.service`  
Referenz: [agave-validator-optimized.service](agave-validator-optimized.service)

### Umgesetzte Optimierungen

| Parameter | Wert | Beschreibung |
|-----------|------|--------------|
| `--rpc-threads` | 32 | 4x mehr parallele RPC Requests |
| `--accounts-db-cache-limit-mb` | 262144 | 256GB Cache für Account-Daten |
| `--accounts-index-scan-results-limit-mb` | 8192 | Größere Batch-Queries |
| `--tpu-connection-pool-size` | 1024 | Mehr TX-Connections |
| `--geyser-plugin-config` | Aktiviert | Real-time Pool Discovery |

### Account Indexes (DEX-spezifisch)

```bash
--account-index program-id
--account-index spl-token-owner
--account-index spl-token-mint
--account-index-include-key 675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8  # Raydium AMM V4
--account-index-include-key whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc  # Orca Whirlpool
```

## Geyser Plugin

Config: `/home/sol/geyser-config.json` (siehe [geyser-grpc-plugin-config.json](geyser-grpc-plugin-config.json))

| Modul | Zweck |
|-------|-------|
| Pool Discovery | Neue Raydium/Orca/Pump.fun Pools (<10ms) |
| Transaction Subscription | Pump.fun CREATE Detection |
| Kill Switch | Dev-Sell Detection, Sell-Burst Monitoring |
| ATA Confirmation | Balance-Updates für TX Confirmation |

## Deployment

```bash
# Config editieren
sudo nano /etc/systemd/system/agave-validator.service

# Systemd neu laden
sudo systemctl daemon-reload
sudo systemctl restart agave-validator

# Status prüfen
sudo systemctl status agave-validator
journalctl -u agave-validator -f
```

## IronCrab Config

Nach Validator-Setup in `my_config.server.toml`:

```toml
[solana]
rpc_url = "http://127.0.0.1:8899"
ws_url = "ws://127.0.0.1:8900"
geyser_grpc_url = "http://127.0.0.1:10000"
rpc_timeout_ms = 15000
rpc_max_concurrency = 96
rpc_min_concurrency = 32
rpc_initial_concurrency = 48
```

## Performance

| Metrik | Public RPC | Eigener Validator |
|--------|------------|-------------------|
| RPC Latenz | 500-2000ms | 10-50ms |
| Pool Discovery | 2-5s | <100ms (Geyser) |
| Quote Refresh | 500ms | 50ms |
| Arbitrage Capture Rate | 5-10% | 60-80% |

## Monitoring

```bash
# RPC Health
curl -s http://127.0.0.1:8899 -X POST \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"getHealth"}' | jq

# IronCrab Logs
journalctl -u ironcrab -f

# Geyser Events
journalctl -u ironcrab -f | grep -i geyser
```

## Troubleshooting

### "Too many open files"
```bash
sudo sysctl -w fs.file-max=2000000
echo "fs.file-max=2000000" | sudo tee -a /etc/sysctl.conf
```

### Hohe CPU (>95%)
Reduziere `--rpc-threads` auf 24 oder 16.

### Validator crasht
```bash
journalctl -u agave-validator -n 100 --no-pager
```
