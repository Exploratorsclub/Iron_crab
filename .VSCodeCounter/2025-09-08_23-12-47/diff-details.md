# Diff Details

Date : 2025-09-08 23:12:47

Directory c:\\Users\\Robert Onuk\\Desktop\\Trading_bot\\Iron_crab\\tests

Total : 65 files,  -9737 codes, -650 comments, -358 blanks, all -10745 lines

[Summary](results.md) / [Details](details.md) / [Diff Summary](diff.md) / Diff Details

## Files
| filename | language | code | comment | blank | total |
| :--- | :--- | ---: | ---: | ---: | ---: |
| [src/audit.rs](/src/audit.rs) | Rust | -90 | -8 | -12 | -110 |
| [src/backtest/engine.rs](/src/backtest/engine.rs) | Rust | -605 | -32 | -21 | -658 |
| [src/backtest/impact.rs](/src/backtest/impact.rs) | Rust | -56 | -8 | -5 | -69 |
| [src/backtest/market.rs](/src/backtest/market.rs) | Rust | -239 | -12 | -12 | -263 |
| [src/backtest/mod.rs](/src/backtest/mod.rs) | Rust | -10 | -1 | -1 | -12 |
| [src/backtest/py\_strategy.rs](/src/backtest/py_strategy.rs) | Rust | -3 | -6 | -2 | -11 |
| [src/backtest/replay.rs](/src/backtest/replay.rs) | Rust | -183 | -19 | -11 | -213 |
| [src/backtest/replay\_rpc.rs](/src/backtest/replay_rpc.rs) | Rust | -57 | -5 | -9 | -71 |
| [src/backtest/scenario.rs](/src/backtest/scenario.rs) | Rust | -87 | -6 | -5 | -98 |
| [src/backtest/types.rs](/src/backtest/types.rs) | Rust | -115 | -6 | -17 | -138 |
| [src/backtest/validation.rs](/src/backtest/validation.rs) | Rust | -108 | -6 | -6 | -120 |
| [src/bin/backtest\_driver.rs](/src/bin/backtest_driver.rs) | Rust | -289 | -26 | -5 | -320 |
| [src/bin/latency\_stress.rs](/src/bin/latency_stress.rs) | Rust | -252 | -23 | -13 | -288 |
| [src/bin/raydium\_pools.rs](/src/bin/raydium_pools.rs) | Rust | -100 | 0 | -9 | -109 |
| [src/bin/recorder.rs](/src/bin/recorder.rs) | Rust | -172 | -14 | -11 | -197 |
| [src/config.rs](/src/config.rs) | Rust | -488 | -15 | -22 | -525 |
| [src/config\_reload.rs](/src/config_reload.rs) | Rust | -133 | -4 | -4 | -141 |
| [src/engine/allocator.rs](/src/engine/allocator.rs) | Rust | -30 | -1 | -4 | -35 |
| [src/engine/mod.rs](/src/engine/mod.rs) | Rust | -253 | -17 | -17 | -287 |
| [src/engine/py\_strategy.rs](/src/engine/py_strategy.rs) | Rust | -65 | -7 | -7 | -79 |
| [src/engine/strategy.rs](/src/engine/strategy.rs) | Rust | -28 | -6 | -9 | -43 |
| [src/lib.rs](/src/lib.rs) | Rust | -9 | 0 | -1 | -10 |
| [src/main.rs](/src/main.rs) | Rust | -34 | -6 | -9 | -49 |
| [src/metrics.rs](/src/metrics.rs) | Rust | -717 | -43 | -23 | -783 |
| [src/solana/arbitrage.rs](/src/solana/arbitrage.rs) | Rust | -683 | -49 | -21 | -753 |
| [src/solana/compute\_budget\_estimator.rs](/src/solana/compute_budget_estimator.rs) | Rust | -61 | -6 | -6 | -73 |
| [src/solana/compute\_budget\_helper.rs](/src/solana/compute_budget_helper.rs) | Rust | -27 | 0 | -1 | -28 |
| [src/solana/dex/mod.rs](/src/solana/dex/mod.rs) | Rust | -37 | -1 | -4 | -42 |
| [src/solana/dex/orca.rs](/src/solana/dex/orca.rs) | Rust | -484 | -23 | -25 | -532 |
| [src/solana/dex/orca\_whirlpool\_layout.rs](/src/solana/dex/orca_whirlpool_layout.rs) | Rust | -159 | -46 | -10 | -215 |
| [src/solana/dex/raydium.rs](/src/solana/dex/raydium.rs) | Rust | -1,233 | -66 | -43 | -1,342 |
| [src/solana/dex/router.rs](/src/solana/dex/router.rs) | Rust | -312 | -25 | -13 | -350 |
| [src/solana/mod.rs](/src/solana/mod.rs) | Rust | -9 | 0 | -1 | -10 |
| [src/solana/rpc.rs](/src/solana/rpc.rs) | Rust | -576 | -12 | -29 | -617 |
| [src/solana/sniper.rs](/src/solana/sniper.rs) | Rust | -2,949 | -215 | -55 | -3,219 |
| [src/solana/token\_utils.rs](/src/solana/token_utils.rs) | Rust | -37 | -5 | -3 | -45 |
| [src/types.rs](/src/types.rs) | Rust | -26 | 0 | -5 | -31 |
| [src/wallet.rs](/src/wallet.rs) | Rust | -401 | -47 | -37 | -485 |
| [tests/arbitrage\_cycle\_generic.rs](/tests/arbitrage_cycle_generic.rs) | Rust | 104 | 1 | 4 | 109 |
| [tests/arbitrage\_cycle\_pruning.rs](/tests/arbitrage_cycle_pruning.rs) | Rust | 98 | 2 | 3 | 103 |
| [tests/arbitrage\_edge\_aggregate.rs](/tests/arbitrage_edge_aggregate.rs) | Rust | 65 | 0 | 4 | 69 |
| [tests/arbitrage\_profit.rs](/tests/arbitrage_profit.rs) | Rust | 10 | 3 | 2 | 15 |
| [tests/arbitrage\_profit\_ranking.rs](/tests/arbitrage_profit_ranking.rs) | Rust | 110 | 6 | 4 | 120 |
| [tests/backtest\_engine.rs](/tests/backtest_engine.rs) | Rust | 85 | 3 | 5 | 93 |
| [tests/bench\_quote\_refresh.rs](/tests/bench_quote_refresh.rs) | Rust | 43 | 3 | 2 | 48 |
| [tests/cfm\_adapter.rs](/tests/cfm_adapter.rs) | Rust | 39 | 0 | 4 | 43 |
| [tests/common.rs](/tests/common.rs) | Rust | 18 | 0 | 3 | 21 |
| [tests/compute\_budget\_estimator.rs](/tests/compute_budget_estimator.rs) | Rust | 29 | 0 | 3 | 32 |
| [tests/impact\_model.rs](/tests/impact_model.rs) | Rust | 53 | 1 | 3 | 57 |
| [tests/integration\_buy\_fill\_sell.rs](/tests/integration_buy_fill_sell.rs) | Rust | 42 | 12 | 8 | 62 |
| [tests/raydium\_quote.rs](/tests/raydium_quote.rs) | Rust | 15 | 3 | 4 | 22 |
| [tests/raydium\_quote\_validation.rs](/tests/raydium_quote_validation.rs) | Rust | 37 | 5 | 2 | 44 |
| [tests/raydium\_simulation.rs](/tests/raydium_simulation.rs) | Rust | 43 | 8 | 2 | 53 |
| [tests/raydium\_swap\_ix.rs](/tests/raydium_swap_ix.rs) | Rust | 32 | 6 | 2 | 40 |
| [tests/raydium\_swap\_plan.rs](/tests/raydium_swap_plan.rs) | Rust | 23 | 4 | 3 | 30 |
| [tests/replay\_deterministic.rs](/tests/replay_deterministic.rs) | Rust | 48 | 4 | 5 | 57 |
| [tests/risk\_drawdown\_and\_cooldown.rs](/tests/risk_drawdown_and_cooldown.rs) | Rust | 80 | 13 | 12 | 105 |
| [tests/router\_best\_quote.rs](/tests/router_best_quote.rs) | Rust | 26 | 4 | 4 | 34 |
| [tests/router\_hops2\_plan.rs](/tests/router_hops2_plan.rs) | Rust | 47 | 14 | 11 | 72 |
| [tests/router\_min\_out.rs](/tests/router_min_out.rs) | Rust | 31 | 1 | 2 | 34 |
| [tests/sniper\_partial\_exit.rs](/tests/sniper_partial_exit.rs) | Rust | 19 | 6 | 6 | 31 |
| [tests/sniper\_partial\_exit\_state.rs](/tests/sniper_partial_exit_state.rs) | Rust | 54 | 4 | 7 | 65 |
| [tests/sniper\_sharpe\_fee\_window.rs](/tests/sniper_sharpe_fee_window.rs) | Rust | 93 | 3 | 10 | 106 |
| [tests/sniper\_sharpe\_update.rs](/tests/sniper_sharpe_update.rs) | Rust | 46 | 2 | 6 | 54 |
| [tests/stub\_strategy\_signal\_quote\_sim.rs](/tests/stub_strategy_signal_quote_sim.rs) | Rust | 90 | 8 | 9 | 107 |

[Summary](results.md) / [Details](details.md) / [Diff Summary](diff.md) / Diff Details