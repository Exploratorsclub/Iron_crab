# IronCrab — Minimal Go‑Live Runbook

This short runbook helps you go live safely with conservative defaults. Read everything before running.

## Prerequisites
- Windows with PowerShell
- Rust toolchain installed
- A reliable Solana RPC/WS provider (paid is recommended). Update URLs in `my_config.toml` accordingly.
- A funded keypair file. On Windows, use an absolute path in `my_config.toml` like:
  `C:\Users\<you>\.config\solana\id.json`
- Sufficient SOL for rent and fees (recommend at least 0.5–1.0 SOL to start).

## Files
- `my_config.toml` — minimal production config with conservative limits.
- `run.ps1` — start the bot with a given config.
- `docs/grafana_dashboard_example.json` — ready-to-import Grafana dashboard.

## One‑time checks
1) Update `my_config.toml`:
   - `solana.rpc_url` and `solana.ws_url`
   - `solana.keypair_path` (absolute Windows path)
   - `sniper.oracle_sol_usd_override` (set to current price if using `override`)
2) Confirm your keypair has enough SOL.
3) Optional: import Grafana dashboard and set Prometheus datasource.

## Build
- Debug build (faster):
  - From repo root run PowerShell: `./build.ps1`
- Release build (recommended for live):
  - `./build.ps1 -Release`

## Run (Windows)
- Debug binary with custom config:
  - `./run.ps1 -Config "my_config.toml"`
- Release binary:
  - `./run.ps1 -Release -Config "my_config.toml"`

The bot serves Prometheus metrics at http://localhost:9898/metrics

## Run (Linux server) & systemd service
- Debug/Release Build:
  - `./build.sh` oder `./build.sh --release`
- Manuell starten:
  - `./run.sh --release --config my_config.server.toml`

### Als systemd Service betreiben
1) Datei anpassen und installieren:
   - Vorlage: `docs/systemd/ironcrab.service`
   - Passe `User`, `Group`, `WorkingDirectory`, `ExecStart` an deine Pfade an
   - Kopiere sie nach `/etc/systemd/system/ironcrab.service`
2) Aktivieren und starten:
```
sudo systemctl daemon-reload
sudo systemctl enable --now ironcrab
sudo systemctl status ironcrab --no-pager
```
3) Logs ansehen:
```
journalctl -u ironcrab -f
```

Der Metrics‑Exporter läuft standardmäßig auf `0.0.0.0:9898` mit Pfad `/metrics`.

## Safety checklist
- Start with very small limits: `sniper.max_buy_sol = 0.02`, strict filters enabled.
- Keep `require_freeze_auth_none = true` and reasonable LP concentration caps.
- Watch logs for rate limiting and errors; adjust RPC concurrency if needed.
- Verify fills and PnL via CSV logs. Ensure adaptive slippage settles near target shortfall.
- Consider raising limits gradually only after several successful sessions.

## Troubleshooting
- Config validation error: the process prints aggregated validation messages. Fix fields or paths.
- RPC/WS connectivity: test with `solana config get` or try alternate providers.
- Keypair permissions: ensure the file is readable by your user and the path is correct.
- High rejections due to pool filters: temporarily lower `min_pool_liquidity_sol` or widen decimals range cautiously.

## Notes
- The sample Rust strategy remains defined but unused. Production flow is controlled via the `[sniper]` section and the `snipe-default` strategy referenced by `markets`.
- To test strategy wiring without live trading, use the backtesting tools and `docs/BACKTESTING.md`.
