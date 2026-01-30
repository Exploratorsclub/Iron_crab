# Diff Details

Date : 2026-01-29 20:40:40

Directory c:\\Users\\Robert Onuk\\Desktop\\Trading_bot\\Iron_crab\\src

Total : 85 files,  44416 codes, 8402 comments, 5679 blanks, all 58497 lines

[Summary](results.md) / [Details](details.md) / [Diff Summary](diff.md) / Diff Details

## Files
| filename | language | code | comment | blank | total |
| :--- | :--- | ---: | ---: | ---: | ---: |
| [control\_plane/README.md](/control_plane/README.md) | Markdown | -66 | 0 | -24 | -90 |
| [control\_plane/\_\_init\_\_.py](/control_plane/__init__.py) | Python | 0 | -2 | -1 | -3 |
| [control\_plane/main.py](/control_plane/main.py) | Python | -936 | -282 | -198 | -1,416 |
| [control\_plane/requirements.txt](/control_plane/requirements.txt) | pip requirements | -5 | -2 | -2 | -9 |
| [src/arbitrage/cycle\_finder.rs](/src/arbitrage/cycle_finder.rs) | Rust | 341 | 75 | 79 | 495 |
| [src/arbitrage/mod.rs](/src/arbitrage/mod.rs) | Rust | 14 | 29 | 4 | 47 |
| [src/arbitrage/multi\_hop\_integration.rs](/src/arbitrage/multi_hop_integration.rs) | Rust | 552 | 79 | 85 | 716 |
| [src/arbitrage/pool\_graph.rs](/src/arbitrage/pool_graph.rs) | Rust | 294 | 55 | 63 | 412 |
| [src/arbitrage/pool\_ranker.rs](/src/arbitrage/pool_ranker.rs) | Rust | 313 | 61 | 64 | 438 |
| [src/arbitrage/types.rs](/src/arbitrage/types.rs) | Rust | 291 | 50 | 54 | 395 |
| [src/audit.rs](/src/audit.rs) | Rust | 90 | 8 | 12 | 110 |
| [src/bin/arb\_strategy.rs](/src/bin/arb_strategy.rs) | Rust | 1,966 | 378 | 234 | 2,578 |
| [src/bin/burn\_manual\_keyless.rs](/src/bin/burn_manual_keyless.rs) | Rust | 173 | 8 | 41 | 222 |
| [src/bin/execution\_engine.rs](/src/bin/execution_engine.rs) | Rust | 5,498 | 588 | 544 | 6,630 |
| [src/bin/latency\_stress.rs](/src/bin/latency_stress.rs) | Rust | 252 | 23 | 13 | 288 |
| [src/bin/manual\_swap.rs](/src/bin/manual_swap.rs) | Rust | 80 | 35 | 22 | 137 |
| [src/bin/market\_data.rs](/src/bin/market_data.rs) | Rust | 1,843 | 278 | 205 | 2,326 |
| [src/bin/momentum\_bot.rs](/src/bin/momentum_bot.rs) | Rust | 4,910 | 587 | 556 | 6,053 |
| [src/bin/pump\_amm\_tx\_probe.rs](/src/bin/pump_amm_tx_probe.rs) | Rust | 306 | 7 | 37 | 350 |
| [src/bin/purge\_nats\_pool\_cache.rs](/src/bin/purge_nats_pool_cache.rs) | Rust | 23 | 1 | 9 | 33 |
| [src/bin/raydium\_pools.rs](/src/bin/raydium_pools.rs) | Rust | 100 | 0 | 9 | 109 |
| [src/bin/sell\_all.rs](/src/bin/sell_all.rs) | Rust | 7 | 1,004 | 3 | 1,014 |
| [src/bin/sell\_all\_keyless.rs](/src/bin/sell_all_keyless.rs) | Rust | 445 | 20 | 77 | 542 |
| [src/bin/setup\_alt.rs](/src/bin/setup_alt.rs) | Rust | 186 | 44 | 41 | 271 |
| [src/config.rs](/src/config.rs) | Rust | 1,279 | 221 | 84 | 1,584 |
| [src/execution/account\_janitor.rs](/src/execution/account_janitor.rs) | Rust | 995 | 132 | 189 | 1,316 |
| [src/execution/cache\_geyser.rs](/src/execution/cache_geyser.rs) | Rust | 344 | 51 | 50 | 445 |
| [src/execution/live\_pool\_cache.rs](/src/execution/live_pool_cache.rs) | Rust | 889 | 203 | 142 | 1,234 |
| [src/execution/mod.rs](/src/execution/mod.rs) | Rust | 6 | 0 | 1 | 7 |
| [src/execution/quote\_calculator.rs](/src/execution/quote_calculator.rs) | Rust | 619 | 113 | 119 | 851 |
| [src/execution/tx\_builder.rs](/src/execution/tx_builder.rs) | Rust | 1,192 | 129 | 124 | 1,445 |
| [src/execution/wsol\_manager.rs](/src/execution/wsol_manager.rs) | Rust | 818 | 166 | 137 | 1,121 |
| [src/ipc/mod.rs](/src/ipc/mod.rs) | Rust | 4 | 7 | 3 | 14 |
| [src/ipc/reason\_codes.rs](/src/ipc/reason_codes.rs) | Rust | 217 | 61 | 30 | 308 |
| [src/ipc/schema.rs](/src/ipc/schema.rs) | Rust | 1,332 | 476 | 222 | 2,030 |
| [src/lib.rs](/src/lib.rs) | Rust | 12 | 0 | 1 | 13 |
| [src/metrics.rs](/src/metrics.rs) | Rust | 1,244 | 106 | 58 | 1,408 |
| [src/nats/client.rs](/src/nats/client.rs) | Rust | 275 | 50 | 42 | 367 |
| [src/nats/jetstream.rs](/src/nats/jetstream.rs) | Rust | 122 | 89 | 19 | 230 |
| [src/nats/mod.rs](/src/nats/mod.rs) | Rust | 6 | 10 | 3 | 19 |
| [src/nats/topics.rs](/src/nats/topics.rs) | Rust | 50 | 29 | 22 | 101 |
| [src/solana/account\_listener.rs](/src/solana/account_listener.rs) | Rust | 159 | 20 | 25 | 204 |
| [src/solana/address\_lookup\_table.rs](/src/solana/address_lookup_table.rs) | Rust | 156 | 42 | 28 | 226 |
| [src/solana/arbitrage.rs](/src/solana/arbitrage.rs) | Rust | 948 | 98 | 53 | 1,099 |
| [src/solana/compute\_budget\_estimator.rs](/src/solana/compute_budget_estimator.rs) | Rust | 61 | 17 | 6 | 84 |
| [src/solana/compute\_budget\_helper.rs](/src/solana/compute_budget_helper.rs) | Rust | 27 | 0 | 1 | 28 |
| [src/solana/cross\_dex\_handler.rs](/src/solana/cross_dex_handler.rs) | Rust | 945 | 290 | 98 | 1,333 |
| [src/solana/dex/meteora\_bin\_array\_layout.rs](/src/solana/dex/meteora_bin_array_layout.rs) | Rust | 94 | 43 | 36 | 173 |
| [src/solana/dex/meteora\_bin\_walker.rs](/src/solana/dex/meteora_bin_walker.rs) | Rust | 169 | 42 | 55 | 266 |
| [src/solana/dex/meteora\_cpmm.rs](/src/solana/dex/meteora_cpmm.rs) | Rust | 511 | 121 | 91 | 723 |
| [src/solana/dex/meteora\_cpmm\_layout.rs](/src/solana/dex/meteora_cpmm_layout.rs) | Rust | 120 | 43 | 19 | 182 |
| [src/solana/dex/meteora\_dlmm.rs](/src/solana/dex/meteora_dlmm.rs) | Rust | 652 | 161 | 115 | 928 |
| [src/solana/dex/meteora\_dlmm\_layout.rs](/src/solana/dex/meteora_dlmm_layout.rs) | Rust | 91 | 36 | 30 | 157 |
| [src/solana/dex/meteora\_swap\_builder.rs](/src/solana/dex/meteora_swap_builder.rs) | Rust | 241 | 141 | 52 | 434 |
| [src/solana/dex/mod.rs](/src/solana/dex/mod.rs) | Rust | 66 | 26 | 9 | 101 |
| [src/solana/dex/orca.rs](/src/solana/dex/orca.rs) | Rust | 1,243 | 228 | 148 | 1,619 |
| [src/solana/dex/orca\_reserve\_cache.rs](/src/solana/dex/orca_reserve_cache.rs) | Rust | 132 | 9 | 15 | 156 |
| [src/solana/dex/orca\_whirlpool\_layout.rs](/src/solana/dex/orca_whirlpool_layout.rs) | Rust | 159 | 46 | 10 | 215 |
| [src/solana/dex/pumpfun.rs](/src/solana/dex/pumpfun.rs) | Rust | 795 | 218 | 112 | 1,125 |
| [src/solana/dex/pumpfun\_amm.rs](/src/solana/dex/pumpfun_amm.rs) | Rust | 2,324 | 274 | 268 | 2,866 |
| [src/solana/dex/raydium.rs](/src/solana/dex/raydium.rs) | Rust | 1,501 | 163 | 108 | 1,772 |
| [src/solana/dex/raydium\_cpmm.rs](/src/solana/dex/raydium_cpmm.rs) | Rust | 457 | 102 | 97 | 656 |
| [src/solana/dex/router.rs](/src/solana/dex/router.rs) | Rust | 334 | 28 | 17 | 379 |
| [src/solana/dex\_parser.rs](/src/solana/dex_parser.rs) | Rust | 987 | 264 | 165 | 1,416 |
| [src/solana/execution.rs](/src/solana/execution.rs) | Rust | 360 | 69 | 63 | 492 |
| [src/solana/geyser\_listener.rs](/src/solana/geyser_listener.rs) | Rust | 489 | 60 | 56 | 605 |
| [src/solana/geyser\_pool\_discovery.rs](/src/solana/geyser_pool_discovery.rs) | Rust | 546 | 152 | 77 | 775 |
| [src/solana/geyser\_tx\_confirm.rs](/src/solana/geyser_tx_confirm.rs) | Rust | 440 | 111 | 82 | 633 |
| [src/solana/geyser\_tx\_confirm\_windows.rs](/src/solana/geyser_tx_confirm_windows.rs) | Rust | 39 | 22 | 10 | 71 |
| [src/solana/jito.rs](/src/solana/jito.rs) | Rust | 415 | 74 | 59 | 548 |
| [src/solana/kill\_switch.rs](/src/solana/kill_switch.rs) | Rust | 270 | 41 | 43 | 354 |
| [src/solana/mod.rs](/src/solana/mod.rs) | Rust | 24 | 0 | 1 | 25 |
| [src/solana/priority\_fee\_tracker.rs](/src/solana/priority_fee_tracker.rs) | Rust | 216 | 63 | 53 | 332 |
| [src/solana/rpc.rs](/src/solana/rpc.rs) | Rust | 725 | 25 | 46 | 796 |
| [src/solana/token\_utils.rs](/src/solana/token_utils.rs) | Rust | 37 | 5 | 3 | 45 |
| [src/solana/tpu\_client.rs](/src/solana/tpu_client.rs) | Rust | 268 | 69 | 57 | 394 |
| [src/solana/tx\_sender.rs](/src/solana/tx_sender.rs) | Rust | 455 | 72 | 56 | 583 |
| [src/solana/wallet\_tracker.rs](/src/solana/wallet_tracker.rs) | Rust | 396 | 40 | 62 | 498 |
| [src/storage/jsonl\_writer.rs](/src/storage/jsonl_writer.rs) | Rust | 233 | 44 | 52 | 329 |
| [src/storage/locks.rs](/src/storage/locks.rs) | Rust | 556 | 128 | 132 | 816 |
| [src/storage/mod.rs](/src/storage/mod.rs) | Rust | 4 | 9 | 3 | 16 |
| [src/tx\_fee\_parser.rs](/src/tx_fee_parser.rs) | Rust | 125 | 27 | 23 | 175 |
| [src/types.rs](/src/types.rs) | Rust | 60 | 11 | 12 | 83 |
| [src/wallet.rs](/src/wallet.rs) | Rust | 495 | 80 | 55 | 630 |
| [src/wallet\_test\_utils.rs](/src/wallet_test_utils.rs) | Rust | 10 | 1 | 3 | 14 |

[Summary](results.md) / [Details](details.md) / [Diff Summary](diff.md) / Diff Details