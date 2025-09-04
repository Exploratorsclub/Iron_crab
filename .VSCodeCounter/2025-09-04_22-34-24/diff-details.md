# Diff Details

Date : 2025-09-04 22:34:24

Directory c:\\Users\\Robert Onuk\\Desktop\\Trading_bot\\Iron_crab\\src

Total : 44 files,  3163 codes, 339 comments, 270 blanks, all 3772 lines

[Summary](results.md) / [Details](details.md) / [Diff Summary](diff.md) / Diff Details

## Files
| filename | language | code | comment | blank | total |
| :--- | :--- | ---: | ---: | ---: | ---: |
| [src/backtest/engine.rs](/src/backtest/engine.rs) | Rust | 64 | 2 | 6 | 72 |
| [src/backtest/market.rs](/src/backtest/market.rs) | Rust | 45 | 0 | 5 | 50 |
| [src/backtest/mod.rs](/src/backtest/mod.rs) | Rust | 3 | 1 | 1 | 5 |
| [src/backtest/types.rs](/src/backtest/types.rs) | Rust | 42 | 0 | 11 | 53 |
| [src/bin/backtest\_driver.rs](/src/bin/backtest_driver.rs) | Rust | 21 | 2 | 2 | 25 |
| [src/bin/raydium\_pools.rs](/src/bin/raydium_pools.rs) | Rust | 69 | 0 | 10 | 79 |
| [src/config.rs](/src/config.rs) | Rust | 91 | 3 | 13 | 107 |
| [src/engine/allocator.rs](/src/engine/allocator.rs) | Rust | 21 | 1 | 5 | 27 |
| [src/engine/mod.rs](/src/engine/mod.rs) | Rust | 150 | 9 | 17 | 176 |
| [src/engine/py\_strategy.rs](/src/engine/py_strategy.rs) | Rust | 42 | 2 | 8 | 52 |
| [src/engine/strategy.rs](/src/engine/strategy.rs) | Rust | 20 | 3 | 7 | 30 |
| [src/lib.rs](/src/lib.rs) | Rust | 7 | 0 | 2 | 9 |
| [src/main.rs](/src/main.rs) | Rust | 35 | 5 | 10 | 50 |
| [src/metrics.rs](/src/metrics.rs) | Rust | 185 | 20 | 12 | 217 |
| [src/solana/arbitrage.rs](/src/solana/arbitrage.rs) | Rust | 298 | 48 | 22 | 368 |
| [src/solana/compute\_budget\_estimator.rs](/src/solana/compute_budget_estimator.rs) | Rust | 45 | 6 | 6 | 57 |
| [src/solana/compute\_budget\_helper.rs](/src/solana/compute_budget_helper.rs) | Rust | 1 | 0 | 1 | 2 |
| [src/solana/dex/mod.rs](/src/solana/dex/mod.rs) | Rust | 25 | 1 | 5 | 31 |
| [src/solana/dex/orca.rs](/src/solana/dex/orca.rs) | Rust | 184 | 12 | 26 | 222 |
| [src/solana/dex/orca\_whirlpool\_layout.rs](/src/solana/dex/orca_whirlpool_layout.rs) | Rust | 82 | 46 | 11 | 139 |
| [src/solana/dex/raydium.rs](/src/solana/dex/raydium.rs) | Rust | 639 | 56 | 42 | 737 |
| [src/solana/dex/router.rs](/src/solana/dex/router.rs) | Rust | 161 | 25 | 13 | 199 |
| [src/solana/mod.rs](/src/solana/mod.rs) | Rust | 6 | 0 | 2 | 8 |
| [src/solana/rpc.rs](/src/solana/rpc.rs) | Rust | 12 | 0 | 4 | 16 |
| [src/solana/sniper.rs](/src/solana/sniper.rs) | Rust | 1,066 | 118 | 39 | 1,223 |
| [src/types.rs](/src/types.rs) | Rust | 23 | 0 | 6 | 29 |
| [src/wallet.rs](/src/wallet.rs) | Rust | 223 | 33 | 35 | 291 |
| [tests/arbitrage\_cycle\_generic.rs](/tests/arbitrage_cycle_generic.rs) | Rust | -43 | -1 | -3 | -47 |
| [tests/arbitrage\_cycle\_pruning.rs](/tests/arbitrage_cycle_pruning.rs) | Rust | -33 | -2 | -2 | -37 |
| [tests/arbitrage\_edge\_aggregate.rs](/tests/arbitrage_edge_aggregate.rs) | Rust | -31 | 0 | -3 | -34 |
| [tests/arbitrage\_profit.rs](/tests/arbitrage_profit.rs) | Rust | -10 | -3 | -1 | -14 |
| [tests/arbitrage\_profit\_ranking.rs](/tests/arbitrage_profit_ranking.rs) | Rust | -45 | -6 | -3 | -54 |
| [tests/backtest\_engine.rs](/tests/backtest_engine.rs) | Rust | -39 | -3 | -5 | -47 |
| [tests/bench\_quote\_refresh.rs](/tests/bench_quote_refresh.rs) | Rust | -28 | -3 | -1 | -32 |
| [tests/cfm\_adapter.rs](/tests/cfm_adapter.rs) | Rust | -8 | 0 | -4 | -12 |
| [tests/common.rs](/tests/common.rs) | Rust | -11 | 0 | -3 | -14 |
| [tests/compute\_budget\_estimator.rs](/tests/compute_budget_estimator.rs) | Rust | -15 | 0 | -3 | -18 |
| [tests/raydium\_quote.rs](/tests/raydium_quote.rs) | Rust | -15 | -3 | -4 | -22 |
| [tests/raydium\_quote\_validation.rs](/tests/raydium_quote_validation.rs) | Rust | -10 | -5 | -2 | -17 |
| [tests/raydium\_simulation.rs](/tests/raydium_simulation.rs) | Rust | -24 | -8 | -2 | -34 |
| [tests/raydium\_swap\_ix.rs](/tests/raydium_swap_ix.rs) | Rust | -30 | -6 | -2 | -38 |
| [tests/raydium\_swap\_plan.rs](/tests/raydium_swap_plan.rs) | Rust | -15 | -4 | -3 | -22 |
| [tests/router\_best\_quote.rs](/tests/router_best_quote.rs) | Rust | -20 | -4 | -4 | -28 |
| [tests/sniper\_partial\_exit.rs](/tests/sniper_partial_exit.rs) | Rust | -20 | -6 | -6 | -32 |

[Summary](results.md) / [Details](details.md) / [Diff Summary](diff.md) / Diff Details