
# IronCrab – Solana‑First Tradingbot (Rust)

Version: **0.3.1-dev** (Agave / Solana 3.x line)  
Legacy (Solana 1.18 baseline): tag `v0.2.1-solana1_18`.

> Migration in progress. See `MIGRATION.md` for details on the upgrade from the legacy 1.18 toolchain to Agave / 3.x crates. The active development branch is `solana3x_clean` (may be renamed / merged soon).

## Features (aktueller Stand)
Core
- Treasury: ATA Erstellung, SPL Transfers, WSOL wrap/unwrap
- Engine: Strategie-Interface (Rust; optional Python via Feature `python`)

DEX & Routing
- Raydium: Pool Scan, Quotes, Swap Plan (Compute Budget), Full Swap IX
- Orca Whirlpool: Strukturierter Parser, Fee Tier Accounts, Swap IX Builder (Tick Arrays + Oracle PDAs)
- Routing: Single-Hop + Depth‑2 Multi-Hop (finales globales min_out)

Sniper & Risk
- WS Log Subscription (Pool Create Events)
- Heuristiken (Blacklist, Liquidity, FreezeAuth, Decimals Range)
- SL/TP Evaluation (Stop-Loss / Take-Profit Trigger BPS)
- Liquidity Index (Raydium + Orca Snapshot)
- Realized PnL: Exakter Exit via WSOL Delta
- Unrealized PnL: Periodische Quote Aktualisierung
- Daily Loss Limit & Max Position Size
- Dynamische Positionsgröße bei Drawdown (Scaling)
- Stop-Loss Cooldown je Mint
- Rolling Realized Return Window + Sharpe Approx (Tests für Fee-Impact & Window-Truncation)
 - Sharpe & Drawdown Gauges (Prometheus: `ironcrab_sharpe_ratio`, `ironcrab_drawdown_pct`)
 - Liveness (`/live`) & Readiness (`/ready`) Endpoints
 - Config Hot Reload (ENV: `IRONCRAB_SNIPER_RELOAD_PATH`) mit Diff Logging (Änderungen werden geloggt)
	 - Watcher Feature (`--features notify_watch`) ersetzt 30s Polling
	 - Unix: SIGHUP (`kill -HUP <pid>`) triggert sofortigen Reload
- Multi-Lot Positions & Partielle Exits (TP Teilverkauf, SL Vollausstieg)
- Persistente Risk-State Snapshot (JSON) + Autosave
- Graceful Shutdown (Snapshot Flush)

Metrics & Observability
 - Prometheus Exporter (Port 9898) – Trades, RPC Errors, Open Positions, Realized PnL (µSOL), Liquidity
 - Histograms: Swap Latency & Quote Latency
 - Aggregates: Shortfall Tokens & SOL, Network Fees (Lamports)
 - Rolling PnL / Sharpe (intern) + Gauges (`ironcrab_sharpe_ratio`, `ironcrab_drawdown_pct`)
 - PnL Distribution Histogram (`ironcrab_trade_return_bucket` inkl. +Inf Bucket)
 - Geplante Erweiterungen: Fee Type Breakdown (Protocol vs Referral) & zusätzliche PnL / Shortfall Histograms

Trade Logging
- CSV Rotation: `trade_logs/trades-YYYYMMDD.csv` (override Pfad via `IRONCRAB_TRADE_LOG_DIR`)
- Felder: Zeit, Side, Mint, DEX, Signature, In/Out Lamports, Tokens, Expected, Shortfall, Fee, Realized PnL, Notes
- Shortfall Analyse (expected vs actual Fill Tokens) & Fee Schätzung (`get_fee_for_message`)

Backtest & Tools
- Backtest Engine + Tests (Slippage Enforcement)
- CLI: `raydium_pools`, `backtest_driver`

## Build & Run (PowerShell)
```powershell
cargo run --release -- --config .\config.example.toml
```

### Echtzeit Config Reload (Watcher & SIGHUP)
Watcher aktivieren (statt Polling):
```powershell
cargo run --release --no-default-features --features notify_watch -- --config .\config.example.toml
```
Hot-Reload Pfad setzen (Windows PowerShell):
```powershell
$env:IRONCRAB_SNIPER_RELOAD_PATH="C:\\full\\path\\to\\config.toml";
cargo run --release --features notify_watch -- --config $env:IRONCRAB_SNIPER_RELOAD_PATH
```
Unix Beispiel mit SIGHUP Trigger:
```bash
IRONCRAB_SNIPER_RELOAD_PATH=./config.toml ./target/release/ironcrab --config ./config.toml &
kill -HUP $!  # Sofortiger Reload
```

### Python‑Strategien (optional)
```powershell
cargo run --release --features python -- --config .\config.example.toml
```

### Raydium Pool‑Reader CLI
```powershell
$env:RPC_URL="http://127.0.0.1:8899"
cargo run --bin raydium_pools -- --mint So11111111111111111111111111111111111111112 --active
```

## Hinweise / Roadmap Auszug
- Siehe `docs/TASKS.md` für detaillierte Meilensteine
- Nächste Schritte: Exakte Fee Aufschlüsselung (Protocol/Referral), SIGHUP Trigger für Config Reload (Unix), zusätzliche Histograms (Absolute Realized PnL, Shortfall %), finalisiertes Grafana Dashboard
- MEV/Jito Bundles & Adaptive Slippage geplant

## Test Helpers
Feature `test_helpers` stellt gezielte Methoden bereit (`test_insert_lot`, `test_simulate_partial_exit_with_fee`, Sharpe Abfragen) für deterministische Unit-/Integrationstests ohne Netzwerk‑Side‑Effects. Standardmäßig nicht im Release aktiv.

## Metrics Scrape Beispiel
Prometheus config snippet:
```yaml
scrape_configs:
	- job_name: ironcrab
		static_configs:
			- targets: ['127.0.0.1:9898']
```

### Verfügbare Metrics (Auszug)
Bereits implementiert:
- `ironcrab_sharpe_ratio` (Gauge) – Rolling Window Sharpe
- `ironcrab_drawdown_pct` (Gauge) – approximierter aktueller Drawdown (0..1)
- `ironcrab_build_info{version="x.y.z"}` – Build/Version Kennzahl
- `ironcrab_trade_return_bucket` – Realized Return Distribution (+Inf Bucket)
- Netzwerk / Shortfall / Fee Aggregationen (`*_total`)

Geplant / Offen:
- `ironcrab_fee_breakdown_total{type="protocol|network|referral"}` – Feingranulare Fee Typen
- Histogram für absoluten Realized PnL (SOL)
- Shortfall Prozent Histogram
- Weitere Route / Quote Performance Metriken

Grafana Dashboard Skeleton: `docs/grafana_dashboard_example.json` (finale Panels & Alerts pending)

## Trade Log Beispiel (gekürzt)
```
timestamp_utc,side,mint,dex,signature,lamports_in,lamports_out,tokens_in,tokens_out,expected_tokens_out,expected_sol_out,shortfall_tokens,shortfall_sol,network_fee_lamports,realized_pnl_sol,notes
2025-09-04T12:00:00Z,BUY,So111...,RAYDIUM,5abc..,100000000,0,0,0,1234500,,0,,5000,,expected_min_out=1200000
2025-09-04T12:03:10Z,FILL,So111...,RAYDIUM,5abc..,100000000,0,0,1229000,1234500,,5500,,5000,,shortfall_ui=0.00055;shortfall_sol=0.00001
```

