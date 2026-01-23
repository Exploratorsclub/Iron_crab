# IronCrab – Solana‑First Tradingbot (Rust)

Version: **0.4.0** (Agave / Solana 3.x)

**Multi-Process Architektur** mit 6 unabhängigen Services, NATS IPC, JetStream State Recovery und co-located Validator (Agave 3.x) für minimale Latenz.

## Architektur

**Source of Truth:**
- `docs/TARGET_ARCHITECTURE.md`
- `docs/DEFINITION_OF_DONE.md`
- `docs/STORAGE_CONVENTIONS.md`
- `docs/RUNBOOK_PROD.md`

**Non-negotiables (P0):**
- **Single-Signer**: Nur die Execution Engine signiert/sendet.
- **Intent-only**: Strategien/Worker erzeugen nur `TradeIntent`s.
- **Simulate-gated**: Simulation-fail ⇒ niemals senden (insb. Arbitrage).
- **Decision Records**: Jede Entscheidung ist forensisch nachvollziehbar.- **GEYSER-FIRST**: Keine RPC-Calls im Hot Path – alle Daten aus Geyser.

## GEYSER-FIRST Architecture

**Kernprinzip:** Alle Echtzeit-Daten kommen aus Geyser gRPC (<10ms Latenz). RPC ist nur Fallback für historische Daten oder Bootstrap.

### Token Program Detection (Token-2022 Support)

Token-Programm (SPL Token vs Token-2022) wird über Geyser erkannt und im Intent mitgesendet:

```
market-data (Geyser)
    → TokenMintInfo Event (token_program = owner of mint account)
    → NATS
    → arb-strategy (speichert in TokenArbTracker)
    → TradeIntent.resources.token_program
    → NATS
    → execution-engine (nutzt für ATA-Erstellung)
```

**Priority für Token Program Detection:**
1. Intent-provided (`TradeResources.token_program`) – höchste Priorität
2. LivePoolCache (Geyser-basiert)
3. DEX hint (pump.fun → immer SPL Token)
4. Default: SPL Token

### DEX-spezifische GEYSER-FIRST Implementation

| DEX | Discovery | Pool State | Swaps | Status |
|-----|-----------|------------|-------|--------|
| Raydium AMM | Geyser Account | Geyser Account | Sync | ✅ |
| Raydium CPMM | Geyser Account | Geyser Account | Sync | ✅ |
| Orca Whirlpool | Geyser Account | Geyser Account | Sync | ✅ |
| Meteora DLMM | Geyser Account | Geyser + PDA derivation | Sync (bin arrays via PDA) | ✅ |
| PumpFun | Geyser TX | Geyser TX | Sync | ✅ |
| PumpSwap | Geyser TX | Geyser Account | Sync | ✅ |
## Target Architecture (High-Level)

Kernaussage: Kein „klassischer Sniper“ (kein „alle neuen Mints sofort kaufen“). Stattdessen: Data Plane lädt/normalisiert Markt-Daten einmal; Momentum-Policy erzeugt Intents; Execution Engine führt deterministisch aus.

```text
Geyser/RPC
	│
	▼
market-data  ── MarketEvents ──►  momentum-bot  ── TradeIntents ──►  execution-engine  ── ExecutionResults ──►  control-plane/UI
																		 │
																		 └──────────── (optional) arb-strategy (Typ A) ─────┘
```

## Rebuild Quickstart (WIP)

Dieser Abschnitt beschreibt den **geplanten** neuen Startpunkt gemäß Target Architecture. Falls Code/Tasks/Configs dafür noch fehlen, gilt die Doku als Richtlinie – nicht als „läuft schon“.

- Binaries/Prozesse: `market-data`, `momentum-bot`, `execution-engine`, `control-plane` (optional: `arb-strategy` Typ A)
- NATS Topics (Minimum): `MarketEvents`, `TradeIntents`, `ExecutionResults`, `ControlRequests`
- Local Dev Ziel: erst Vertical Slice (1 DEX / 1 Pair / 1 Policy / 1 Tx-Typ) bis P0 aus `docs/DEFINITION_OF_DONE.md` erfüllt ist
- Storage P0: append-only Flat Files für Events/Intents/Decisions/Executions gemäß `docs/STORAGE_CONVENTIONS.md`

## Validator Entry Point Latency Testing
To select the fastest Solana mainnet-beta entrypoints (Gossip port 8001) from your Frankfurt host you can use the helper script:

```bash
docs/tools/entrypoint_latency_test_v2.sh --with-gossip --rpc
```

Features:
- TCP handshake timing (ms) for Gossip (8001) and optional RPC (8899)
- CSV output saved to `/tmp/entrypoints_latency.csv`
- Sorted recommendation list (top 4 printed as ready `--entrypoint host:8001` lines)
- Optional gossip peer discovery (requires `solana-gossip` in PATH)

Common usage patterns:

```bash
# Default entrypoints only (Gossip latency)
docs/tools/entrypoint_latency_test_v2.sh

# Also measure RPC port connect times
docs/tools/entrypoint_latency_test_v2.sh --rpc

# Discover additional low-latency peers via gossip
docs/tools/entrypoint_latency_test_v2.sh --with-gossip

# Custom host list
docs/tools/entrypoint_latency_test_v2.sh --host entrypoint.mainnet-beta.anza.xyz,entrypoint3.mainnet-beta.anza.xyz --rpc

# Limit default list to first N (e.g. 4)
docs/tools/entrypoint_latency_test_v2.sh --limit 4
```

Interpreting results:
- Prefer TCP 8001 connect times < 70ms (Frankfurt typical best ~15ms).
- Keep 3–6 entrypoints: 2–3 very low latency + 1–2 backups.
- Re-run occasionally; anycast routing can shift.
- After adding peers: validator peer count should exceed ~40 quickly for faster snapshot propagation.

Add lines produced by the script into your validator launch (each as separate `--entrypoint host:8001`). Review trust & stability before adding unknown peers.

## Features (Legacy-Code / aktueller Stand – wird abgelöst)

Wichtig: Die folgende Liste beschreibt primär den aktuellen Legacy/Monolith-Stand im Repo. Neue Entwicklung soll sich an der Target Architecture orientieren (siehe `docs/TARGET_ARCHITECTURE.md`) und muss die DoD erfüllen.

Core
- Treasury: ATA Erstellung, SPL Transfers, WSOL wrap/unwrap
- Engine: Strategie-Interface (Rust; optional Python via Feature `python`)

DEX & Routing
- Raydium: Pool Scan, Quotes, Swap Plan (Compute Budget), Full Swap IX
- Orca Whirlpool: Strukturierter Parser, Fee Tier Accounts, Swap IX Builder (Tick Arrays + Oracle PDAs)
- Pump.fun: Bonding Curve Detection, Buy/Sell via Transaction Subscription
- Routing: Single-Hop + Depth‑2/3 Multi-Hop (finales globales min_out)
- Arbitrage Engine: Triangular Cycle Detection (A→B→C→A), Net Profit Filter, Atomic TX Plans
- Arbitrage Auto‑Discovery: Optionaler Discovery‑Loop (Raydium + Orca) filtert liquide Paare und füttert den Scanner (Modus: CSV‑only oder Full‑Auto)
- Jito Bundle Integration: Atomare Arbitrage TXs zur Vermeidung von Frontrunning (Feature: `jito`)

Geyser gRPC (Low-Latency Data)
- Pool Discovery: Geyser-basierte Pool-Erkennung für Raydium, Orca, Pump.fun (<10ms vs ~400ms RPC)
- Transaction Subscription: Pump.fun CREATE Instruction Detection via Discriminator Check
- ATA Confirmation: Dynamische Subscription für spezifische ATAs (nicht gesamtes Token Program)
- Kill Switch: Echtzeit Dev-Sell Detection, Sell-Burst Monitoring, Negative Flow Detection

Sniper & Risk (Legacy)
- Geyser Pool Discovery (ersetzt WS Log Subscription)
- WS Resilience: Subscribe-ACK Gating, Bounded Backpressure, Heartbeat/Staleness, Multi-Endpoint Failover, optionale Auth-Header
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
- Zeitbasierte Exits: `max_hold_secs` für Zwangsverkauf, `timed_exit_tiers` für gestaffelte Teilverkäufe
- Kill Switch: Automatischer Exit bei Dev-Sell, Sell-Burst oder negativem Flow (Geyser-basiert)
- Jito Bundle Exits: Atomare Emergency-Exits mit konfigurierbaren Thresholds
- Persistente Risk-State Snapshot (JSON) + Autosave
- Graceful Shutdown (Snapshot Flush)

Metrics & Observability
 - Prometheus Exporter (Port 9898) – Trades, RPC Errors, Open Positions, Realized PnL (µSOL), Liquidity
 - Histograms: Swap Latency & Quote Latency
 - Aggregates: Shortfall Tokens & SOL, Network Fees (Lamports)
 - Rolling PnL / Sharpe (intern) + Gauges (`ironcrab_sharpe_ratio`, `ironcrab_drawdown_pct`)
 - Brutto vs Netto Realized PnL Gauges (`gross_realized_pnl_sol`, `net_realized_pnl_sol`)
 - Fee-% Histogram (`fee_percent_bucket{le="..."}`) basierend auf Fee / Notional
 - PnL Distribution Histogram (`ironcrab_trade_return_bucket` inkl. +Inf Bucket; Werte werden gegen Bucket‑Grenzen geklammert)
 - Zusatz‑Zähler: Zero‑Reserve Skips und Decimals‑Quellen (`mint_decimals_*`)
 - Geplante Erweiterungen: Fee Type Breakdown (Protocol vs Referral) & zusätzliche PnL / Shortfall Histograms

Trade Logging
- CSV Rotation: `trade_logs/trades-YYYYMMDD.csv` (override Pfad via `IRONCRAB_TRADE_LOG_DIR`)
- Felder: Zeit, Side, Mint, DEX, Signature, In/Out Lamports, Tokens, Expected, Shortfall, Fee, Realized PnL, Notes
- Shortfall Analyse (expected vs actual Fill Tokens) & Fee Schätzung (`get_fee_for_message`)

Tools
- CLI: `raydium_pools` - Pool information viewer

## 🚀 Local Development Guide

**Wichtig:** Keys gehören ausschließlich in die `execution-engine`. Alle anderen Prozesse sind keyless und erzeugen nur Intents.

### Prerequisites
1. **Rust Installation**: Ensure you have Rust installed (latest stable)
2. **Solana Wallet**: Have a Solana keypair with sufficient SOL balance
3. **RPC Access**: Local node (recommended) or RPC provider endpoint

### Step 1: Configuration Setup
Copy and customize the configuration file:
```powershell
# Copy example config
cp config.example.toml my_config.toml
```

**Critical Settings to Review** (in `my_config.toml`):
```toml
[solana]
rpc_url = "http://127.0.0.1:8899"     # Your RPC endpoint
ws_url  = "ws://127.0.0.1:8900"       # WebSocket endpoint  
keypair_path = "path/to/your/id.json" # Your wallet keypair

[sniper]
# START SMALL - These are CONSERVATIVE defaults
max_buy_sol = 0.02                    # Max 0.02 SOL per trade
max_position_sol = 0.10               # Max 0.1 SOL total position
daily_loss_limit_sol = 0.30           # Stop trading if lose 0.3 SOL/day
stop_loss_bps = 3000                  # -30% stop loss (AGGRESSIVE!)
take_profit_bps = 1000                # +10% take profit
```

Optional: enable Arbitrage Auto‑Discovery
```toml
[arbitrage]
interval_ms = 2000
min_profit_bps = 10
est_tx_cost_lamports = 10000

[arbitrage.discovery]
enable = true
mode = "discovery-only"   # use "full-auto" to feed discovered pairs into the scanner
base_tokens = [
	"So11111111111111111111111111111111111111112", # SOL
	"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", # USDC
	"Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB", # USDT
]
min_liquidity_sol = 20.0
min_liquidity_usd = 10000.0
default_ui_amount = 0.05
top_n_per_base = 20
interval_secs = 30
```

### Step 2: Wallet Setup
Choose one of the following methods to provide your keypair:

**Option A: Config File** (simplest)
```toml
[solana]
keypair_path = "~/.config/solana/id.json"
```

**Option B: Environment Variables** (more secure)
```powershell
# JSON format (recommended)
$env:IRONCRAB_KEYPAIR_JSON='[123,45,67,...]'  # Your 64-byte keypair

# Or Base64
$env:IRONCRAB_KEYPAIR_B64='YOUR_BASE64_KEY_HERE'

# Or file path with security
$env:IRONCRAB_KEYPAIR_PATH='C:\secure\path\id.json'
$env:IRONCRAB_KEYPAIR_STRICT='1'
$env:IRONCRAB_KEYPAIR_ALLOWED_DIRS='C:\secure\path'
```

### Step 3: Verify Wallet Balance
Ensure your wallet has adequate SOL:
- **Minimum**: 0.5 SOL (for rent + fees + initial positions)
- **Recommended**: 2-5 SOL for proper testing
- **Note**: The bot will use `max_buy_sol` × `max_open_positions` maximum

### Step 4: Build and Run
```powershell
# Build (first time)
cargo build --release

# Run with your config
cargo run --release -- --config my_config.toml
```

### Step 5: Monitor the Bot
Once running, monitor these endpoints:

1. **Logs**: Check console output for trade activity
2. **Metrics**: `http://localhost:9898/metrics` (Prometheus format)
3. **Health**: 
   - `http://localhost:9898/live` (liveness check)
   - `http://localhost:9898/ready` (readiness check)
4. **Trade Logs**: `./trade_logs/trades-YYYYMMDD.csv`

### Step 6: Safety Checklist ⚠️
Before going live, verify:
- [ ] `max_buy_sol` is appropriately small for your risk tolerance
- [ ] `daily_loss_limit_sol` will protect your capital  
- [ ] `stop_loss_bps` is not too aggressive (consider 1500-2000 instead of 3000)
- [ ] Your RPC endpoint is reliable and fast
- [ ] You understand that live trading is HIGH RISK
- [ ] You've tested with small amounts first

### Common Commands
```powershell
# Run with Python strategies enabled
cargo run --release --features python -- --config my_config.toml

# Run with file watching for hot config reload  
cargo run --release --features notify_watch -- --config my_config.toml

# Check pool information
cargo run --bin raydium_pools -- --rpc-url http://127.0.0.1:8899

# Stress test latency
cargo run --bin latency_stress -- --duration-secs 30 --concurrency 16
```

### Troubleshooting
- **Connection Issues**: Verify RPC/WS URLs are accessible
- **Insufficient Balance**: Ensure wallet has enough SOL for trades + fees
- **No Trades**: Check if pools meet your filtering criteria (`min_pool_liquidity_sol`, etc.)
- **High CPU**: Reduce `rpc_max_concurrency` or increase `exit_eval_interval_secs`
 - **No arbitrage pairs**: In discovery‑only mode we only log CSVs (`trade_logs/arb_pairs-YYYYMMDD.csv`). Switch to `full-auto` to scan discovered pairs.

---

### Load / Stress: Quote & Swap Latency
Neues Binary `latency_stress` misst parallel Quote‑ und Swap‑Plan‑Latenzen unter Last.

- RPC via `--rpc-url` oder ENV `SOLANA_RPC_URL`
- Konfigurierbare Dauer und Parallelität: `--duration-secs`, `--concurrency`
- Operation‑Mix gewichtet: `--w-single`, `--w-hops2`, `--w-hops3`, `--w-plan2`
- Paar‑Pinning: `--pairs A->B,B->C` (Trennzeichen `->`, `:`, `,` unterstützt)

Beispiel (PowerShell):
```powershell
$env:SOLANA_RPC_URL="http://127.0.0.1:8899";
cargo run --bin latency_stress -- --duration-secs 30 --concurrency 64
```

## Build & Run (PowerShell)
```powershell
cargo run --release -- --config .\config.example.toml
```

### Fuzzing
Parser (Orca Whirlpool Layout) werden via `cargo-fuzz` getestet:
```bash
cargo install cargo-fuzz
cd fuzz
cargo fuzz run fuzz_replay_log_parser -- -max_total_time=60
cargo fuzz run fuzz_orca_whirlpool_layout -- -max_total_time=60
```
Crash-Artefakte landen in `fuzz/artifacts/<target>/`, minimieren via `cargo fuzz tmin <target> <crash-file>`.

### Grafana‑Panels (Hinweise)
- Quote Latenz: `quote_latency_seconds_*` (Heatmap/Histogram + P50/P90)
- Swap‑Plan Latenz: `swap_latency_seconds_*`
- Trade Return: `trade_return_bucket` (+Inf Bucket via Count) – Note: Werte werden geklammert
- Realized PnL (SOL): `realized_pnl_sol_bucket`, `realized_pnl_sol_sum`, `realized_pnl_sol_count`
- Fees/Shortfall: `fee_percent_bucket`, `shortfall_percent_bucket`, Summen `network_fees_lamports_total`, `shortfall_sol_total`
- Resilience: `raydium_pools_skipped_zero_reserve_total`, `orca_pools_skipped_zero_reserve_total`
- Decimals‑Quellen: `mint_decimals_source_supply_total`, `mint_decimals_source_account_total`, `mint_decimals_fallback_default_total`
- Risk Gauges: `ironcrab_sharpe_ratio`, `ironcrab_drawdown_pct`, `open_positions`

### Beispiel: SampleRustStrategy konfigurieren
Füge in deiner TOML Config unter `[strategies]` einen Eintrag mit `kind = "rust"` hinzu und setze Parameter:

```toml
[strategies.sample_rust]
kind = "rust"
params = { base_mint = "So11111111111111111111111111111111111111112", quote_mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", base_symbol = "SOL", quote_symbol = "USDC", side = "buy", amount_ui = 0.01, max_slippage_bps = 100, interval_ms = 15000 }

[[markets]]
name = "SOL-USDC"
allocation_pct = 100
strategy = "sample_rust"
```

Hinweise:
- `interval_ms` limitiert die Tick‑Emission (ein Intent alle N Millisekunden).
- `base_decimals`/`quote_decimals` sind optional; fehlen sie, werden sie über RPC ermittelt (mit robustem Fallback).
- `side` akzeptiert `buy` oder `sell`.

Dashboard Import (Kurz):
- Grafana → Dashboards → Import
- Datei: `docs/grafana_dashboard_example.json`
- Prometheus‑Datasource auswählen/anpassen (Name/Alias)
- Enthält u. a.: Zero‑Reserve Skips, Decimals‑Quellen (`mint_decimals_*`), Quote/Swap‑Latenzen

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
- enable_time_based_exits: Aktiviert zeitbasierte Exits (Zwangsverkauf nach Haltezeit).
- max_hold_secs: Maximale Haltezeit in Sekunden, danach 100% Exit.
- timed_exit_tiers: Array gestaffelter Zeitaustritte `[{secs, fraction}]`.

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

# Zeitbasierte Exits
enable_time_based_exits = true
max_hold_secs = 90                # Zwangsverkauf nach 90 Sekunden
timed_exit_tiers = [
	{ secs = 30, fraction = 0.25 },  # Nach 30s: 25% verkaufen
	{ secs = 60, fraction = 0.50 },  # Nach 60s: 50% verkaufen
]

# Kill Switch (Geyser-basiert)
kill_switch_enabled = true
kill_switch_dev_sell = true       # Exit bei Dev-Sell Detection
kill_switch_sell_burst_count = 5  # Exit bei 5+ Sells
kill_switch_sell_burst_sol = 10.0 # Exit bei >10 SOL Sell-Volumen
kill_switch_flow_ratio_min = 0.3  # Exit wenn Buy/Sell Ratio < 0.3

# Jito Bundle Exits
jito_enabled = true
jito_tip_lamports = 10000         # 0.00001 SOL Tip
jito_for_emergency = true         # Immer Jito für Emergency Exits
jito_min_exit_sol = 0.5           # Jito nur für Exits > 0.5 SOL
```

Hinweise:
- Fractionen sind pro Ebene (z. B. 0.30 = 30% des zum Zeitpunkt offenen Lot‑Volumens)
- Tiers sollten aufsteigend nach bps definiert werden; Werte außerhalb realistischer Spannen werden ignoriert.
- Greift, falls keine Tiers gesetzt sind: Legacy‑Fallback (einfacher TP/SL).

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
- ✅ Multi-Process Architektur implementiert (6 Services)
- ✅ JetStream State Recovery implementiert
- ✅ LivePoolCache + QuoteCalculator implementiert
- ✅ Jito Bundle Integration implementiert (Feature `jito`)
- ✅ Geyser gRPC Pool Discovery & Kill Switch implementiert
- ✅ WsolManager + AccountJanitor implementiert

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
 - `realized_pnl_sol_bucket{le=...}` – Absolutes Realized PnL Histogram (SOL) inkl. Sum/Count
 - `shortfall_percent_bucket{le=...}` – Shortfall Prozent Histogram inkl. +Inf Bucket via Count
- Netzwerk / Shortfall / Fee Aggregationen (`*_total`)
	- `network_fees_lamports_total` – Summierte Netzwerkgebühren (Lamports)
	- `shortfall_tokens_total`, `shortfall_sol_total` – Aggregierte Shortfalls
	- `fills_total` – Anzahl bestätigter Fills
	- `pending_reconciliations_total`, `pending_failed_total` – PendingTrade Reconciliation
	- `partial_exit_events_total`, `partial_exit_fraction_sum` – Partielle Exit‑Ausführungen und kumulierte Fraktionen
	- `requote_events_total`, `requote_improved_total`, `requote_worsened_total`, `requote_min_out_delta_ratio_sum` – Re‑Quote Effekte vor Signatur
	- `dex_selection_entry_raydium_total`, `dex_selection_entry_orca_total` – DEX‑Auswahl bei Entry
	- `dex_selection_exit_raydium_total`, `dex_selection_exit_orca_total` – DEX‑Auswahl bei Exit
 - Latenz‑Histograms:
	 - `quote_latency_seconds_bucket{le=...}`, `quote_latency_seconds_sum`, `quote_latency_seconds_count`
	 - `swap_latency_seconds_bucket{le=...}`, `swap_latency_seconds_sum`, `swap_latency_seconds_count`
 - Resilience & Data Helpers:
	 - `raydium_pools_skipped_zero_reserve_total`, `orca_pools_skipped_zero_reserve_total`
	 - `mint_decimals_source_supply_total`, `mint_decimals_source_account_total`, `mint_decimals_fallback_default_total`

Geplant / Offen:
- `ironcrab_fee_breakdown_total{type="protocol|network|referral"}` – Feingranulare Fee Typen
- Weitere Route / Quote Performance Metriken

Grafana Dashboard: `docs/grafana_dashboard_example.json` (Panels finalisiert; optionale Alerts pending)

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

- Mint Decimals Auflösung: Primär `getTokenSupply.decimals`; Fallback: Byte 44 des Mint‑Accounts; sonst 0 (Warnung). Quelle wird über `mint_decimals_*` Metriken gezählt. Wallet & Sniper nutzen denselben Helper.

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

## Erweiterte Risk-Parameter (Sniper)

Diese Parameter steuern Notional-Limits, Tagesverlust-Gating, Positionsanzahl, Cooldowns sowie eine dynamische Drawdown-Skalierung der Kaufgröße. Alle Felder sind optional, konservative Defaults blockieren keine Käufe, sofern nicht gesetzt.

Konfiguration (TOML unter `[sniper]`):

- max_position_sol (f64)
  - Cap pro neuer Position (Notional in SOL). Wenn gesetzt, wird vor einem neuen BUY geprüft, ob die geplante Lot-Größe darüber liegt; andernfalls wird der Einstieg verworfen.
- daily_loss_limit_sol (f64)
  - Harte Tagesverlust-Grenze in SOL. Überschreitet `realized_loss_today_sol` diese Grenze, werden neue Einstiege blockiert. Dient auch als Basis für die Drawdown-Skalierung (s. unten).
- max_open_positions (usize)
  - Obergrenze paralleler offener Positionen. Bei Erreichen werden weitere Einstiege gesperrt.
- per_mint_position_limit (u32)
  - Anzahl erlaubter Lots pro Mint (Mehrfachkäufe). Bei Erreichen: keine neuen Lots für diesen Mint.
- stop_loss_cooldown_secs (u64)
  - Nach einem SL-Exit wird der betroffene Mint für `secs` in einen Cooldown versetzt (keine Re-Entries). Wird intern automatisch gesetzt und per Risk-State persistiert.
- drawdown_scale_start (f64, Anteil 0..1)
  - Ab welchem Anteil des `daily_loss_limit_sol` die Lot-Kaufgröße linear reduziert wird. Beispiel: 0.30 bedeutet, dass bis 30% Verlust keine Reduktion erfolgt; darüber beginnt die Skalierung bis zur Maximalreduktion.
- drawdown_max_reduction (f64, Anteil 0..1)
  - Maximale Reduktion der Kaufgröße bei 100% des Tagesverlust-Limits (oberhalb `drawdown_scale_start`). Beispiel: 0.7 entspricht bis zu 70% Reduktion.
- rolling_pnl_window (usize)
  - Fenstergröße für die Sharpe-Approximation und Rolling-Return-Metriken (persistiert im Risk-State; beeinflusst Visualisierung, nicht die Gating-Logik).

Formel: dynamische Kaufgröße
- Die effektive Kaufgröße wird intern als `effective_max_buy_sol()` berechnet. Pseudocode:
  - Wenn `daily_loss_limit_sol`, `drawdown_scale_start`, `drawdown_max_reduction` gesetzt:
    - `ratio = clamp(realized_loss_today_sol / daily_loss_limit_sol, 0..1)`
    - Ist `ratio <= drawdown_scale_start`, nutze `max_buy_sol` unverändert
    - Sonst: `frac = (ratio - start) / (1 - start)`; `reduction = drawdown_max_reduction * frac`
    - Effektive Größe: `max_buy_sol * (1 - reduction)`
  - Andernfalls: `max_buy_sol`

Gating-Reihenfolge (vereinfacht)
1) Cooldown je Mint (falls aktiv) und per-mint-Limit
2) max_open_positions und max_position_sol
3) daily_loss_limit_sol (harte Sperre)
4) Drawdown-Skalierung der Kaufgröße (wirkt nur dämpfend, nicht sperrend)

Persistenz & ENV
- Risk-State Snapshot wird periodisch gespeichert (Autosave) und beim Start geladen:
  - IRONCRAB_RISK_STATE_PATH: Datei-Pfad (Default: `state/risk_state.json`)
  - IRONCRAB_RISK_AUTOSAVE_SECS: Intervall in Sekunden für Autosave (0 = aus)
- Metriken: `ironcrab_drawdown_pct` zeigt den aktuellen Drawdown-Anteil relativ zum konfigurierten Tagesverlust-Limit an.

Beispiel (TOML)
```toml
[sniper]
# Kern-Limits
max_buy_sol = 0.5
max_position_sol = 1.0
max_open_positions = 4
per_mint_position_limit = 2
stop_loss_cooldown_secs = 600

# Tagesverlust & Drawdown Skalierung
daily_loss_limit_sol = 2.0
drawdown_scale_start = 0.30
drawdown_max_reduction = 0.70

# Rolling Fenster (Metriken)
rolling_pnl_window = 200
```

Hinweise & Best Practices
- Setze `daily_loss_limit_sol` konservativ (z. B. 1–3 SOL) für klare Tagesrisiko-Kappung.
- Wähle `drawdown_scale_start` nicht zu niedrig; sinnvoll sind 0.2–0.5. Kombiniere mit `drawdown_max_reduction` zwischen 0.4–0.8.
- `max_open_positions` begrenzt Exponierung über Mints hinweg; in illiquiden Phasen reduzieren.
- `stop_loss_cooldown_secs` verhindert sofortige Re-Entries in fallende Messer. 5–15 Minuten sind praktikable Startwerte.
- Änderungen per Hot-Reload möglich; Unterschiede werden im Log über `diff_sniper_cfg` ausgegeben.

