# Changelog

All notable changes to this project will be documented in this file.

## [0.4.0] - 2026-01-23
### Added
- **LivePoolCache**: In-memory pool cache fed by Geyser events with JetStream persistence for state recovery
- **QuoteCalculator**: Zero-RPC quote calculation for Raydium AMM, Orca Whirlpool, Meteora DLMM
- **WsolManager**: Event-driven WSOL balance management (wrap/unwrap) via NATS wallet balance updates
- **AccountJanitor**: Background task for closing empty ATAs and recovering rent
- **JetStream Integration**: Persistent POOL_CACHE stream for pool state recovery after restarts
- **TrackedWallet**: Geyser-based wallet balance tracking in market-data binary
- Comprehensive unit tests for LivePoolCache (9 tests) and QuoteCalculator (15 tests)
- `docs/VALIDATOR_SETUP.md`: Consolidated validator deployment guide with optimizations

### Changed
- **Multi-Process Architecture**: Fully migrated from monolith to 6 independent services:
  - `execution-engine` (port 9804): Intent processing, TX building, signing, sending
  - `momentum-bot` (port 9802): Momentum strategy, TradeIntent generation
  - `market-data` (port 9801): Geyser ingest, pool discovery, MarketEvents publishing
  - `arb-strategy` (port 9803): Cross-DEX arbitrage detection
  - `control-plane` (port 8080): FastAPI dashboard, risk controls
  - `trades-server` (port 9899): Trade history API
- **NATS IPC**: All inter-process communication via NATS pub/sub (versioned topics `ironcrab.v1.*`)
- **Systemd Orchestration**: `ironcrab.target` orchestrates all services with proper dependencies
- **Arb-TX Optimization**: Removed WSOL wrap from swap plan (~21k CU saved per TX)
- Validator config: Ledger 100M slots, 320GB cache, 16GB scan buffer, 9 account index keys

### Removed
- **Backtest Module**: Deleted entire `src/backtest/` directory (10 files), `backtest_driver.rs`, `recorder.rs`
- **Legacy Scripts**: Removed `run.ps1`, `run.sh`, `backtest.ps1`, `backtest.sh`
- **Obsolete Docs**: Deleted `BACKTESTING.md`, `TRADE_PARSING_STATUS.md`, `QUANTILE_SLIPPAGE.md`, `REAL_SEND_*.md`, `MULTI_POOL_ROUTING_IMPLEMENTATION.md`
- **Dead Code**: Removed `refresh_pools_replay()` from Raydium/Orca DEX connectors, `quantile_impact.rs`

### Fixed
- Token-2022 ATA creation in cross-DEX arbitrage
- Meteora DLMM bin_array_bitmap_extension errors (AccountOwnedByWrongProgram)
- Orca Whirlpool missing tick_current_index/tick_spacing fields

### Documentation
- Updated `TARGET_ARCHITECTURE.md` with JetStream, LivePoolCache, metrics ports
- Rewrote `RUNBOOK_PROD.md` for multi-process operations
- Rewrote `SCRIPTS_README.md` with current deploy/monitoring scripts
- Consolidated `VALIDATOR_SETUP.md` (merged optimization deployment guide)

## [0.3.2-dev] - 2026-01-16
### Added
- **Token-2022 Support via Intent**: `TradeResources.token_program` field allows strategies to pass token program info directly in intents, avoiding `IncorrectProgramId` errors for Token-2022 tokens.
- `arb-strategy` now processes `TokenMintInfo` events and includes `token_program` in arb intents.
- `cross_dex_handler` prioritizes intent-provided `token_program` over cache lookups.

### Changed
- **GEYSER-FIRST for Meteora DLMM**: Removed all RPC calls from hot path:
  - `fetch_current_active_id()` replaced with Geyser-cached `active_id`
  - `fetch_bin_arrays_direct()` replaced with deterministic PDA derivation
  - New `build_swap_with_bins_sync()` method for zero-RPC swap building
  - New `derive_bin_arrays_for_active_id()` for bin array PDA calculation
- Token program detection now uses 4-tier priority: Intent → Cache → DEX hint → Default (SPL Token)

### Fixed
- Token-2022 ATA creation failures in cross-DEX arbitrage (was defaulting to SPL Token)
- Meteora DLMM swaps no longer require RPC calls during execution
- **Meteora DLMM `bin_array_bitmap_extension` error (3007)**: Fixed "AccountOwnedByWrongProgram" errors by using `program_id` as placeholder for optional bitmap extension (per official Meteora SDK pattern)
- **Orca Whirlpool UNSUPPORTED_INTENT errors**: Fixed missing `tick_current_index` and `tick_spacing` in Orca pools:
  - Added `tick_current_index: Option<i32>` and `tick_spacing: Option<u16>` fields to `PoolData` and `PoolDiscoveryEvent` structs
  - `parse_orca_pool()` now extracts these values from Whirlpool account data
  - `market_data` now includes Orca tick fields in `DexPoolAccounts` events (format: `tick_current_index:<value>`, `tick_spacing:<value>`)
  - Enables zero-RPC Orca swap building via LivePoolCache

## [0.3.1-dev] - 2025-09-04
### Added
- Multi-lot position model & partial exit logic (TP fractional, SL full) with proportional invested capital adjustment.
- Risk state JSON persistence (autosave + reload on startup).
- Graceful shutdown (watch channel) with final snapshot flush.
- Test helpers feature flag (`test_helpers`) exposing deterministic state mutation & Sharpe simulation helpers.
- Unit tests: proportional partial exit math, state mutation & Sharpe (fee impact, rolling window truncation), fee-inclusive vs no-fee Sharpe comparison.
- Sharpe internal computation stabilization (window trimming, >=5 samples requirement).

### Changed
- README & TASKS updated to reflect new sniper & risk features.
- Sniper configuration now implements `Default`.

### Fixed
- Removed unreachable code pattern in sniper loop (clean shutdown break).
- Numerous minor warnings (unused imports/variables, unnecessary parentheses) across tests.

### Pending / Next
- CI pipeline (clippy, fmt, test, audit).
- Precise fee meta parsing (protocol/referrer, compute budget overhead) replacing heuristics.
- Sharpe & drawdown gauges exposed via metrics exporter.
- +Inf bucket trade_return histogram & absolute realized PnL histogram.

## [0.2.x]
- Legacy Solana 1.18 line (see tags)

