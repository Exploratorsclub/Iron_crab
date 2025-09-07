
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
 - Brutto vs Netto Realized PnL Gauges (`gross_realized_pnl_sol`, `net_realized_pnl_sol`)
 - Fee-% Histogram (`fee_percent_bucket{le="..."}`) basierend auf Fee / Notional
 - PnL Distribution Histogram (`ironcrab_trade_return_bucket` inkl. +Inf Bucket)
 - Geplante Erweiterungen: Fee Type Breakdown (Protocol vs Referral) & zusätzliche PnL / Shortfall Histograms

Trade Logging
- CSV Rotation: `trade_logs/trades-YYYYMMDD.csv` (override Pfad via `IRONCRAB_TRADE_LOG_DIR`)
- Felder: Zeit, Side, Mint, DEX, Signature, In/Out Lamports, Tokens, Expected, Shortfall, Fee, Realized PnL, Notes
- Shortfall Analyse (expected vs actual Fill Tokens) & Fee Schätzung (`get_fee_for_message`)

Backtest & Tools
- Backtest Engine + Tests (Slippage Enforcement)
- CLI: `raydium_pools`, `backtest_driver`

### Backtest: Replay & Impact Modelle
- Replay Loader: JSONL/JSON Trace Dateien werden eingelesen (`backtest::replay::load_trace`). Unterstützt aktuell Slot & Log Events; Account Events folgen.
- Impact Model: Pluggable Slippage/Impact Modell für Backtests. CLI Schalter `--impact cpmm|clmm|none` auf `backtest_driver`.
- Beispiel (PowerShell):
```powershell
cargo run --bin backtest_driver -- --replay-trace .\traces\sample.jsonl --replay-start 250000000 --replay-end 250000120 --impact cpmm
```
Hinweis: Ohne `--replay-trace` generiert der Driver eine minimale Slot-Sequenz. `clmm` ist derzeit ein Platzhalter (CPMM-Fallback).

## Build & Run (PowerShell)
```powershell
cargo run --release -- --config .\config.example.toml
```

## Security: Keypair ENV loaders & redacting logger

- Keypair sources (priority):
	1. IRONCRAB_KEYPAIR_JSON – JSON array of 32 or 64 bytes (secret key)
	2. IRONCRAB_KEYPAIR_B64 – base64 of 32 or 64 bytes
	3. IRONCRAB_KEYPAIR_BASE58 – base58 secret key string
	4. IRONCRAB_KEYPAIR_PATH – path to the standard Solana keypair file

- Strict path validation (for PATH):
	- Enable with IRONCRAB_KEYPAIR_STRICT=1
	- Allow-list directories via IRONCRAB_KEYPAIR_ALLOWED_DIRS (separate with ; or ,)
	- Defaults if unset: %APPDATA%\Solana, %USERPROFILE%\.config\solana, .\secrets

- Examples (PowerShell):
```powershell
# JSON (32/64 bytes)
$env:IRONCRAB_KEYPAIR_JSON='[1,2,3, ... ,255]';

# Base64
$env:IRONCRAB_KEYPAIR_B64='BASE64_SECRET_HERE';

# Base58
$env:IRONCRAB_KEYPAIR_BASE58='BASE58_SECRET_HERE';

# File path with strict allow-list
$env:IRONCRAB_KEYPAIR_PATH='C:\\keys\\id.json';
$env:IRONCRAB_KEYPAIR_STRICT='1';
$env:IRONCRAB_KEYPAIR_ALLOWED_DIRS='C:\\keys;C:\\Users\\<you>\\AppData\\Roaming\\Solana';
```

- Redacting logger:
	- Enabled by default in binaries; respects RUST_LOG (e.g., RUST_LOG=info,ironcrab=debug)
	- Redacts fields with names containing: secret, private, seed, mnemonic, keypair, sk, kp; also large JSON-like byte arrays
	- Best-effort protection; do not log secrets explicitly

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

## Konfig – neue Felder (Sniper)
Die Sniper‑Konfiguration wurde um optionale Felder für gestaffelte Teilverkäufe, Trailing‑Stop, minimale Exit‑Notional und Pending‑TTL erweitert.

- take_profit_tiers: Array von Ebenen mit Bps‑Schwelle und Fraction (nicht kumulativ). Aufsteigend sortiert empfohlen.
- trailing_stop_bps: Optionaler Trailing‑Stop Abstand in bps ab dem bisherigen Höchst‑PnL des Lots.
- min_exit_notional_sol: Mindest‑Notional in SOL für einen Exit; kleine Restbeträge werden übersprungen.
- pending_trade_ttl_secs: TTL in Sekunden; Pending Trades werden nach Ablauf bereinigt und per Reconciliation verifiziert.
- exit_eval_interval_secs: Intervall (Sekunden) für die separate Exit‑Evaluation Task.

Beispiel (TOML):
```toml
[sniper]
# Rebuy/Exit/Risk Parameter (Auszug)
take_profit_tiers = [
	{ bps = 500,  fraction = 0.30 },
	{ bps = 1000, fraction = 0.30 },
	{ bps = 1500, fraction = 0.40 },
]
trailing_stop_bps = 300
min_exit_notional_sol = 0.02
pending_trade_ttl_secs = 120
```

Hinweise:
- Fractionen sind pro Ebene (z. B. 0.30 = 30% des zum Zeitpunkt offenen Lot‑Volumens)
- Tiers sollten aufsteigend nach bps definiert werden; Werte außerhalb realistischer Spannen werden ignoriert.
- Greift, falls keine Tiers gesetzt sind: Legacy‑Fallback (einfacher TP/SL).

### Python‑Strategien (optional)
```powershell
cargo run --release --features python -- --config .\config.example.toml
```
Backtesting (IPC Variante): Für Backtests steht zusätzlich eine einfache IPC‑Strategie zur Verfügung, die bei jedem Event ein Python‑Skript als Prozess startet, das eine `StrategyDecision` als JSON an stdout schreibt. Erwartetes Protokoll: stdin erhält genau ein Zeile JSON des `SimEvent`, stdout liefert genau eine `StrategyDecision` JSON.

Beispiel Python (vereinfachtes Echo):
```python
import sys, json
ev = json.loads(sys.stdin.readline())
print(json.dumps({"actions": []}))
```
Verwendung im Backtest‑Code: `PyProcStrategy::new("python", ["strategies/sample.py"])` (unter Feature `python`).

CLI Komfort (Feature `python_ipc` – pyo3 nicht benötigt, funktioniert ohne Python Headers):
```powershell
cargo run --bin backtest_driver --features python_ipc -- --replay-trace .\traces\sample.jsonl --py-script .\strategies\sample.py
```
Linux Beispiel:
```bash
cargo run --bin backtest_driver --features python_ipc -- --replay-trace ./traces/sample.jsonl --py-script ./strategies/sample.py
```

#### Strategy Interface (Backtest IPC Schema)
- Input event (one JSON line): SimEvent
	- { ts_ms: number, kind: "SlotAdvance" | "CfmPriceUpdate" | "NewPool" | "TradeFill" | "ScenarioMeta" | "Log", ... }
- Output decision (one JSON line): StrategyDecision
	- { actions: [ { "Swap": { pool, input_mint, output_mint, amount_in, max_slippage_bps } } ] }

Beispiele:
- `strategies/sample.py`: Minimale Klasse mit `on_tick()` Rückgabe (Demo)
- `strategies/sample_worker.py`: Zeilen‑Protokoll Worker, der eingehende Events liest und leere Entscheidungen zurückgibt


### Raydium Pool‑Reader CLI
```powershell
$env:RPC_URL="http://127.0.0.1:8899"
cargo run --bin raydium_pools -- --mint So11111111111111111111111111111111111111112 --active
```

## Hinweise / Roadmap Auszug
- Siehe `docs/TASKS.md` für detaillierte Meilensteine
- Nächste Schritte: Exakte Fee Aufschlüsselung (Protocol/Referral), zusätzliche Histograms (Absolute Realized PnL, Shortfall %), finalisiertes Grafana Dashboard
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
 - `gross_realized_pnl_sol`, `net_realized_pnl_sol` – Session‑aggregierte Brutto/Netto PnL (SOL)
 - `fee_percent_bucket{le=...}` – Gebührenanteil gemessen am Notional pro Trade (inkl. +Inf Bucket via Count)
- Netzwerk / Shortfall / Fee Aggregationen (`*_total`)
	- `network_fees_lamports_total` – Summierte Netzwerkgebühren (Lamports)
	- `shortfall_tokens_total`, `shortfall_sol_total` – Aggregierte Shortfalls
	- `fills_total` – Anzahl bestätigter Fills
	- `pending_reconciliations_total`, `pending_failed_total` – PendingTrade Reconciliation
	- `partial_exit_events_total`, `partial_exit_fraction_sum` – Partielle Exit‑Ausführungen und kumulierte Fraktionen
	- `requote_events_total`, `requote_improved_total`, `requote_worsened_total`, `requote_min_out_delta_ratio_sum` – Re‑Quote Effekte vor Signatur
	- `dex_selection_entry_raydium_total`, `dex_selection_entry_orca_total` – DEX‑Auswahl bei Entry
	- `dex_selection_exit_raydium_total`, `dex_selection_exit_orca_total` – DEX‑Auswahl bei Exit

Geplant / Offen:
- `ironcrab_fee_breakdown_total{type="protocol|network|referral"}` – Feingranulare Fee Typen
- Absolute Realized PnL Histogram (SOL): `realized_pnl_sol_bucket{le=...}`, plus `realized_pnl_sol_sum` und `realized_pnl_sol_count` – DONE
- Shortfall Prozent Histogram: `shortfall_percent_bucket{le=...}` – DONE
- Weitere Route / Quote Performance Metriken

Grafana Dashboard Skeleton: `docs/grafana_dashboard_example.json` (finale Panels & Alerts pending)

## Fee / Meta‑Parsing
BUY‑FILLs nutzen `postTokenBalances - preTokenBalances` (Transaction Meta, JsonParsed) für die tatsächlich erhaltene Tokenmenge (Treasury‑Owner). Shortfall & Protokoll‑Fees werden daraus bzw. heuristisch über `fee_bps` abgeleitet und in `protocol_fee_tokens_total`/`protocol_fee_sol_total` aggregiert. Exakte Referral/Protocol‑Splits sind DEX‑spezifisch und folgen.

## Trade Log Beispiel (gekürzt)
```
timestamp_utc,side,mint,dex,signature,lamports_in,lamports_out,tokens_in,tokens_out,expected_tokens_out,expected_sol_out,shortfall_tokens,shortfall_sol,network_fee_lamports,realized_pnl_sol,notes
2025-09-04T12:00:00Z,BUY,So111...,RAYDIUM,5abc..,100000000,0,0,0,1234500,,0,,5000,,expected_min_out=1200000
2025-09-04T12:03:10Z,FILL,So111...,RAYDIUM,5abc..,100000000,0,0,1229000,1234500,,5500,,5000,,shortfall_ui=0.00055;shortfall_sol=0.00001
```

## Data & Pricing

- SOL/USD Preisquellen: `oracle_preference` steuert Reihenfolge ("pyth" | "switchboard" | "override"). Setze `oracle_pyth_sol_usd` (Pyth Price Account) bzw. `oracle_switchboard_sol_usd` (Aggregator). Fallback: `oracle_sol_usd_override`.
- SOL/USD Override: `sniper.oracle_sol_usd_override` konvertiert USDC/USDT‑Reserven in SOL für Liquidity‑Schätzungen, wenn Oracles fehlen.
- Adaptive Slippage: Rolling Mean der beobachteten BUY‑Shortfalls (tatsächlich erhaltene Tokens vs. expected) steuert die effektive Slippage‑Bps. Ziel‑Slippage `adaptive_slippage_target_pct`, Schrittweite `adaptive_slippage_step_bps`, Grenzen `adaptive_slippage_min_bps`/`max_bps`. Zustand wird im Risk‑Snapshot persistiert.

## RPC Concurrency & Rate Limits

Der RPC‑Client passt die erlaubte Parallelität dynamisch an. Rate‑Limit Treffer (HTTP 429/"Too Many Requests"/Throttle) und Timeouts verringern das Fenster, anhaltend erfolgreiche Requests erhöhen es schrittweise.

Konfiguration (TOML unter `[solana]`):
- `rpc_min_concurrency` (usize)
- `rpc_max_concurrency` (usize)
- `rpc_initial_concurrency` (usize)
- `rpc_inc_every_successes` (usize)
- `rpc_dec_on_rate_limit` (usize)
- `rpc_timeout_ms` (u64)

Metrics:
- `rpc_rate_limit_hits_total`, `rpc_timeouts_total`, `rpc_backoff_ms_total`
- `rpc_inflight`, `rpc_allowed_concurrency`, `rpc_concurrency_adjustments_total`

