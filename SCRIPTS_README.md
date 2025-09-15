# IronCrab Start Scripts

This directory contains convenience scripts for building, running, and backtesting the IronCrab trading bot.

## Windows Scripts (PowerShell)

### build.ps1
Builds the IronCrab project and all binaries.

```powershell
# Basic debug build
.\build.ps1

# Release build
.\build.ps1 -Release

# Build with features
.\build.ps1 -Features -FeatureList "python,notify_watch"

# Show help
.\build.ps1 -Help
```

### run.ps1
Runs the main IronCrab trading bot.

```powershell
# Run with default config
.\run.ps1

# Run release build
.\run.ps1 -Release

# Run with custom config
.\run.ps1 -Config "my_config.toml"

# Build and run
.\run.ps1 -Build -Release

# Show help
.\run.ps1 -Help
```

### backtest.ps1
Runs backtesting with various options.

```powershell
# Basic backtest with trace data
.\backtest.ps1 -ReplayTrace "data\trace.jsonl.gz" -ReplayStart 250000000 -ReplayEnd 250001000

# Backtest with Python strategy
.\backtest.ps1 -PyScript "strategies\sample.py" -ReplayTrace "data\trace.jsonl.gz"

# Backtest with impact modeling
.\backtest.ps1 -Impact "clmm" -ImpactExtraFeeBps 10

# Show help
.\backtest.ps1 -Help
```

## Unix/Linux/macOS Scripts (Bash)

First, make the scripts executable:
```bash
chmod +x build.sh run.sh backtest.sh
```

### build.sh
Builds the IronCrab project and all binaries.

```bash
# Basic debug build
./build.sh

# Release build
./build.sh --release

# Build with features
./build.sh --features --feature-list "python,notify_watch"

# Show help
./build.sh --help
```

### run.sh
Runs the main IronCrab trading bot.

```bash
# Run with default config
./run.sh

# Run release build
./run.sh --release

# Run with custom config
./run.sh --config "my_config.toml"

# Build and run
./run.sh --build --release

# Show help
./run.sh --help
```

### backtest.sh
Runs backtesting with various options.

```bash
# Basic backtest with trace data
./backtest.sh --replay-trace "data/trace.jsonl.gz" --replay-start 250000000 --replay-end 250001000

# Backtest with Python strategy
./backtest.sh --py-script "strategies/sample.py" --replay-trace "data/trace.jsonl.gz"

# Backtest with impact modeling
./backtest.sh --impact "clmm" --impact-extra-fee-bps 10

# Show help
./backtest.sh --help
```

## Prerequisites

1. **Rust and Cargo**: Install from https://rustup.rs/
2. **Configuration file**: Copy `config.example.toml` to your own config file
3. **For backtesting**: You need recorded trace data (use the `recorder` binary)
4. **For Python strategies**: Python with required packages

## Built Binaries

After building, the following binaries will be available:

- `ironcrab` / `ironcrab.exe` - Main trading bot
- `backtest_driver` / `backtest_driver.exe` - Backtesting engine  
- `recorder` / `recorder.exe` - Data recorder for backtesting
- `latency_stress` / `latency_stress.exe` - Performance testing
- `raydium_pools` / `raydium_pools.exe` - Pool analysis tool

## Quick Start

1. Copy and customize the configuration:
   ```bash
   cp config.example.toml my_config.toml
   # Edit my_config.toml with your settings
   ```

2. Build the project:
   ```bash
   # Windows
   .\build.ps1 -Release
   
   # Unix/Linux/macOS
   ./build.sh --release
   ```

3. Run the trading bot:
   ```bash
   # Windows
   .\run.ps1 -Release -Config "my_config.toml"
   
   # Unix/Linux/macOS
   ./run.sh --release --config "my_config.toml"
   ```

## Monitoring

When running, metrics are available at: http://localhost:9898/metrics

You can import the Grafana dashboard from `docs/grafana_dashboard_example.json` for monitoring.