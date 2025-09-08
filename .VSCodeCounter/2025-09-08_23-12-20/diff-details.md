# Diff Details

Date : 2025-09-08 23:12:20

Directory c:\\Users\\Robert Onuk\\Desktop\\Trading_bot\\Iron_crab\\src

Total : 38 files,  7557 codes, 373 comments, 167 blanks, all 8097 lines

[Summary](results.md) / [Details](details.md) / [Diff Summary](diff.md) / Diff Details

## Files
| filename | language | code | comment | blank | total |
| :--- | :--- | ---: | ---: | ---: | ---: |
| [src/audit.rs](/src/audit.rs) | Rust | 90 | 8 | 12 | 110 |
| [src/backtest/engine.rs](/src/backtest/engine.rs) | Rust | 541 | 30 | 15 | 586 |
| [src/backtest/impact.rs](/src/backtest/impact.rs) | Rust | 56 | 8 | 5 | 69 |
| [src/backtest/market.rs](/src/backtest/market.rs) | Rust | 194 | 12 | 7 | 213 |
| [src/backtest/mod.rs](/src/backtest/mod.rs) | Rust | 7 | 0 | 0 | 7 |
| [src/backtest/py\_strategy.rs](/src/backtest/py_strategy.rs) | Rust | 3 | 6 | 2 | 11 |
| [src/backtest/replay.rs](/src/backtest/replay.rs) | Rust | 183 | 19 | 11 | 213 |
| [src/backtest/replay\_rpc.rs](/src/backtest/replay_rpc.rs) | Rust | 57 | 5 | 9 | 71 |
| [src/backtest/scenario.rs](/src/backtest/scenario.rs) | Rust | 87 | 6 | 5 | 98 |
| [src/backtest/types.rs](/src/backtest/types.rs) | Rust | 73 | 6 | 6 | 85 |
| [src/backtest/validation.rs](/src/backtest/validation.rs) | Rust | 108 | 6 | 6 | 120 |
| [src/bin/backtest\_driver.rs](/src/bin/backtest_driver.rs) | Rust | 268 | 24 | 3 | 295 |
| [src/bin/latency\_stress.rs](/src/bin/latency_stress.rs) | Rust | 252 | 23 | 13 | 288 |
| [src/bin/raydium\_pools.rs](/src/bin/raydium_pools.rs) | Rust | 31 | 0 | -1 | 30 |
| [src/bin/recorder.rs](/src/bin/recorder.rs) | Rust | 172 | 14 | 11 | 197 |
| [src/config.rs](/src/config.rs) | Rust | 397 | 12 | 9 | 418 |
| [src/config\_reload.rs](/src/config_reload.rs) | Rust | 133 | 4 | 4 | 141 |
| [src/engine/allocator.rs](/src/engine/allocator.rs) | Rust | 9 | 0 | -1 | 8 |
| [src/engine/mod.rs](/src/engine/mod.rs) | Rust | 103 | 8 | 0 | 111 |
| [src/engine/py\_strategy.rs](/src/engine/py_strategy.rs) | Rust | 23 | 5 | -1 | 27 |
| [src/engine/strategy.rs](/src/engine/strategy.rs) | Rust | 8 | 3 | 2 | 13 |
| [src/lib.rs](/src/lib.rs) | Rust | 2 | 0 | -1 | 1 |
| [src/main.rs](/src/main.rs) | Rust | -1 | 1 | -1 | -1 |
| [src/metrics.rs](/src/metrics.rs) | Rust | 532 | 23 | 11 | 566 |
| [src/solana/arbitrage.rs](/src/solana/arbitrage.rs) | Rust | 385 | 1 | -1 | 385 |
| [src/solana/compute\_budget\_estimator.rs](/src/solana/compute_budget_estimator.rs) | Rust | 16 | 0 | 0 | 16 |
| [src/solana/compute\_budget\_helper.rs](/src/solana/compute_budget_helper.rs) | Rust | 26 | 0 | 0 | 26 |
| [src/solana/dex/mod.rs](/src/solana/dex/mod.rs) | Rust | 12 | 0 | -1 | 11 |
| [src/solana/dex/orca.rs](/src/solana/dex/orca.rs) | Rust | 300 | 11 | -1 | 310 |
| [src/solana/dex/orca\_whirlpool\_layout.rs](/src/solana/dex/orca_whirlpool_layout.rs) | Rust | 77 | 0 | -1 | 76 |
| [src/solana/dex/raydium.rs](/src/solana/dex/raydium.rs) | Rust | 594 | 10 | 1 | 605 |
| [src/solana/dex/router.rs](/src/solana/dex/router.rs) | Rust | 151 | 0 | 0 | 151 |
| [src/solana/mod.rs](/src/solana/mod.rs) | Rust | 3 | 0 | -1 | 2 |
| [src/solana/rpc.rs](/src/solana/rpc.rs) | Rust | 564 | 12 | 25 | 601 |
| [src/solana/sniper.rs](/src/solana/sniper.rs) | Rust | 1,883 | 97 | 16 | 1,996 |
| [src/solana/token\_utils.rs](/src/solana/token_utils.rs) | Rust | 37 | 5 | 3 | 45 |
| [src/types.rs](/src/types.rs) | Rust | 3 | 0 | -1 | 2 |
| [src/wallet.rs](/src/wallet.rs) | Rust | 178 | 14 | 2 | 194 |

[Summary](results.md) / [Details](details.md) / [Diff Summary](diff.md) / Diff Details