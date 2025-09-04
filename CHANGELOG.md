# Changelog

All notable changes to this project will be documented in this file.

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

