# Backtesting quickstart

Build SimEvents with `backtest::replay::build_events_from_trace` or craft minimal events.
Assemble a `CfmAdapter` market with pools or ingest `OrcaPoolSnapshot`/`Raydium` snapshots.
Implement a `BacktestStrategy` stub and run via `BacktestEngine`.
Use `backtest::scenario::Scenario` to parameterize size/slippage and impact knobs.

Example (sketch):

```rust
use ironcrab::backtest::{engine::BacktestEngine, market::CfmAdapter, scenario::Scenario, types::{BacktestStrategy, SimContext, StrategyDecision}};
# struct MyStrat; impl BacktestStrategy for MyStrat { fn on_tick(&self, _:&SimContext)->StrategyDecision{ StrategyDecision{actions:vec![]} } }
# let events = vec![]; let market = CfmAdapter::new(); let portfolio = ironcrab::backtest::types::Portfolio::new();
let engine = BacktestEngine::new(MyStrat, market, portfolio, events);
let sc = Scenario { name: "smoke".into(), sizes: vec![100_000], slippages_bps: vec![100], impact: None };
let _engine = sc.run(engine)?;
# Ok::<(), anyhow::Error>(())
```