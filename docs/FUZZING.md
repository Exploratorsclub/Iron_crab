# Fuzzing IronCrab Parsers

We use libFuzzer via cargo-fuzz to stress parsers:
- Replay log/trace loader (`backtest::replay::load_trace`/`build_events_from_trace`)
- Orca Whirlpool account layout parser (lax and strict)

## Setup
```
cargo install cargo-fuzz
```

## Targets
- fuzz_replay_log_parser: Generates JSONL-like inputs to exercise the trace loader and event builder.
- fuzz_orca_whirlpool_layout: Feeds arbitrary bytes into both Whirlpool parsers.

## Run
```
cd fuzz
cargo fuzz run fuzz_replay_log_parser -- -max_total_time=60
cargo fuzz run fuzz_orca_whirlpool_layout -- -max_total_time=60
```

## Notes
- Targets run in a separate fuzz workspace under `fuzz/` using the main crate as a dependency.
- Crashes will be saved under `fuzz/artifacts/<target>/`. Minimize with:
```
cargo fuzz tmin <target> <path-to-crash>
```
