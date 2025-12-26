# IronCrab AI Coding Guide

High-frequency Solana trading bot with meme token sniper and arbitrage modules. Runs alongside a self-hosted Agave 3.0.11 validator for minimal latency.
Das ziel ist einen hochfrequenz solana bot zu erstellen mit einem meme token sniper und einem arbitrage modul. Es wird ein eigener agave 3.0.11 validator auf dem gleichen server wie der bot betrieben die config ist im file agave-validator-optimized.service einsehbar. Der bot soll in der lage sein meme tokens sofort nach deren launch zu kaufen und arbitrage möglichkeiten zwischen verschiedenen dezentralen exchanges auf der solana blockchain zu erkennen und auszunutzen. Alle entwicklungen sollen dokumentiert werden und der code soll sauber und wartbar sein. Der bot soll in der lage sein mehrere transaktionen pro sekunde durchzuführen und dabei die netzwerkgebühren zu minimieren. Sicherheitsaspekte sind besonders zu beachten, insbesondere im Umgang mit privaten schlüsseln und sensiblen daten. Der bot soll so konzipiert sein, dass er leicht erweitert und angepasst werden kann, um auf zukünftige änderungen im solana-ökosystem reagieren zu können. Es soll eine benutzeroberfläche geben, die es ermöglicht, den bot zu konfigurieren und seine leistung in Echtzeit zu überwachen. Alle entwicklungen sollen unter berücksichtigung der geltenden rechtlichen rahmenbedingungen erfolgen. Der bot soll in der lage sein, auf verschiedene marktsituationen zu reagieren und seine strategie entsprechend anzupassen. Der bot soll in der lage sein, mit anderen solana-bots zu konkurrieren und sich einen wettbewerbsvorteil zu verschaffen. Der bot soll in der lage sein, große mengen an daten in kurzer zeit zu verarbeiten und schnelle entscheidungen zu treffen. Der bot soll in der lage sein, fehler zu erkennen und sich selbst zu korrigieren, um eine hohe verfügbarkeit zu gewährleisten. Der bot soll in der lage sein, verschiedene meme tokens zu analysieren und deren potenzial für schnelle gewinne zu bewerten. Der bot soll in der lage sein, seine strategie basierend auf historischen daten und aktuellen markttrends anzupassen. Der bot soll in der lage sein, mit minimaler latenz zu arbeiten, um schnelle transaktionen zu ermöglichen. Der bot soll in der lage sein, verschiedene dezentrale exchanges zu integrieren und deren liquidität zu nutzen. Der bot soll in der lage sein, benachrichtigungen zu senden, wenn bestimmte ereignisse eintreten, wie z.b. erfolgreiche käufe oder arbitrage-möglichkeiten. Der bot soll in der lage sein, seine leistung kontinuierlich zu überwachen und optimierungen vorzunehmen, um die effizienz zu steigern. Der bot soll in der lage sein, verschiedene risikomanagement-strategien zu implementieren, um verluste zu minimieren. Der bot soll in der lage sein, mit verschiedenen solana-wallets zu arbeiten und deren sicherheit zu gewährleisten.

Geyser gegen über rpc calls bevorzugen für höhere performance.
Alle arbitrage transaktionen sollen atomar sein und über jito gesendet werden um fehlgeschlagene transaktionen und fronntrunning zu vermeiden.
Keine eigenständigen ssh verbindungsversuche zum server wenn ssh benötigt wird den user darauf hinweisen und warten bis eine verbindung hergestellt wird.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                         SniperEngine                            │
│  (src/solana/sniper.rs - 6000+ lines, core trading logic)      │
├─────────────────────────────────────────────────────────────────┤
│  Geyser gRPC          │  DEX Connectors       │  Risk Manager   │
│  - Pool Discovery     │  - Raydium            │  - Position     │
│  - ATA Confirmation   │  - Orca Whirlpool     │    Tracking     │
│  - Kill Switch        │  - Pump.fun           │  - Stop Loss/TP │
│                       │  - Router (multi-hop) │  - Daily Limits │
├─────────────────────────────────────────────────────────────────┤
│  Treasury (wallet.rs)  │  Metrics (9898)      │  Jito Bundles   │
└─────────────────────────────────────────────────────────────────┘
```

## Critical Dev Workflows

### Build & Test
```bash
cargo fmt --check              # CI enforces this
cargo clippy -- -D warnings    # Must pass with zero warnings
cargo test --features test_helpers  # Enables SniperEngine test helpers
```

### Local Run
```bash
cargo run --release -- --config my_config.toml
```

### CI Requirements (`.github/workflows/ci.yml`)
- Rust 1.89.0 with `protobuf-compiler` for Geyser gRPC
- All tests must pass with both default and `test_helpers` features
- Clippy warnings are errors

## Project-Specific Patterns

### Geyser over RPC
**Always prefer Geyser gRPC for real-time data** - reduces latency from ~400ms to <10ms:
```rust
// ✅ Good: Geyser-based pool discovery
use crate::solana::geyser_pool_discovery::GeyserPoolDiscovery;

// ❌ Avoid: RPC polling for real-time events
```

### DEX Trait Pattern (`src/solana/dex/mod.rs`)
All DEX connectors implement the `Dex` trait:
```rust
#[async_trait]
pub trait Dex: Send + Sync {
    async fn refresh_pools(&self) -> Result<()>;
    async fn quote_exact_in(&self, input_mint: &str, output_mint: &str, amount_in: u64) -> Result<Option<Quote>>;
    fn build_swap_ix(&self, input_mint: &str, output_mint: &str, amount_in: u64, min_out: u64) -> Result<Vec<Instruction>>;
}
```

### Test Helper Pattern
Use `#[cfg(any(test, feature = "test_helpers"))]` to expose internal methods for testing:
```rust
#[cfg(any(test, feature = "test_helpers"))]
impl SniperEngine {
    pub fn test_insert_lot(&self, mint: Pubkey, ...) { ... }
    pub fn test_get_realized_pnl_sol(&self) -> f64 { ... }
}
```

### Concurrency with `parking_lot`
Use `parking_lot::RwLock` instead of `std::sync::RwLock` for better performance:
```rust
risk: Arc<parking_lot::RwLock<RiskState>>
```

## Key Integration Points

### Validator Config
See `docs/agave-validator-optimized.service` - critical account indexes:
- `675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8` (Raydium AMM V4)
- `whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc` (Orca Whirlpool)

### Geyser Config (`docs/geyser-grpc-plugin-config.json`)
Must match validator's `--geyser-plugin-config` path.

### Jito for Atomic Arbitrage
All arbitrage TXs should use Jito bundles to prevent frontrunning:
```rust
// Feature-gated: cargo build --features jito
use crate::solana::jito::JitoClient;
```

## Geyser Configuration

### Plugin Config (`docs/geyser-grpc-plugin-config.json`)
```json
{
  "libPath": "/home/sol/geyser-plugins/solana_geyser_plugin_grpc.so",
  "bind_address": "127.0.0.1:10001",
  "accounts": [
    { "owner": "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8" },  // Raydium AMM V4
    { "owner": "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc" }   // Orca Whirlpool
  ]
}
```

### Geyser Modules
| Module | Purpose |
|--------|---------|
| `geyser_pool_discovery.rs` | Pool creation events for sniping |
| `geyser_kill_switch.rs` | Real-time dev sell detection, flow monitoring |
| `geyser_tx_confirm.rs` | Dynamic ATA subscription for balance confirmation |

### Dynamic ATA Subscription Pattern
For efficient TX confirmation, subscribe only to specific ATA addresses (not entire Token Program):
```rust
// ✅ Good: Dynamic subscription to specific ATAs
let ata = get_associated_token_address(&wallet, &mint);
geyser_confirm.watch_ata(ata, expected_balance).await;

// ❌ Avoid: Subscribing to entire Token Program (millions of updates/sec)
```

## Risk Management

### RiskState (`src/solana/sniper.rs`)
Central risk tracking with the following components:
```rust
struct RiskState {
    open: HashMap<Pubkey, Vec<PositionLot>>,  // Multi-lot per mint
    realized_pnl_sol: f64,
    realized_loss_today_sol: f64,
    cooldown_until: HashMap<Pubkey, i64>,     // Per-mint cooldown after SL
    recent_realized: Vec<f64>,                // Rolling window for Sharpe
    adaptive_slippage_bps: Option<u32>,       // Auto-adjusted slippage
}
```

### Key Risk Parameters (TOML Config)
```toml
[sniper]
stop_loss_bps = 500              # 5% stop-loss
take_profit_bps = 1000           # 10% take-profit
daily_loss_limit_sol = 5.0       # Max daily loss
max_open_positions = 5           # Concurrent position limit
stop_loss_cooldown_secs = 300    # 5min cooldown after SL trigger
max_position_sol = 2.0           # Max SOL per position

# Drawdown scaling (reduce size during drawdown)
drawdown_scale_start = 0.1       # Start scaling at 10% drawdown
drawdown_max_reduction = 0.5     # Reduce max to 50% at max drawdown
```

### Tiered Take-Profit
```toml
[[sniper.take_profit_tiers]]
threshold_bps = 500   # At +5%
exit_fraction = 0.25  # Sell 25%

[[sniper.take_profit_tiers]]
threshold_bps = 1000  # At +10%
exit_fraction = 0.50  # Sell 50%
```

### Time-Based Exits
Force exits after holding too long (prevents bag-holding):
```toml
[sniper]
enable_time_based_exits = true
max_hold_secs = 90               # Force full exit after 90 seconds

# Tiered time exits (sell fractions at intervals)
[[sniper.timed_exit_tiers]]
secs = 30        # After 30 seconds
fraction = 0.25  # Sell 25%

[[sniper.timed_exit_tiers]]
secs = 60        # After 60 seconds
fraction = 0.50  # Sell 50%
```

**Logic**: `max_hold_secs` triggers 100% exit; timed tiers are partial exits tracked per-lot via `executed_timed_tiers`.

## Sniper Module (`src/solana/sniper.rs`)

The `SniperEngine` is the core trading engine (~6000 LOC). Key responsibilities:

### Entry Flow
1. **Pool Discovery** (Geyser): Detect new Raydium/Orca/Pump.fun pools
2. **Token Validation**: Check freeze authority, decimals, LP concentration
3. **Risk Check**: Open positions, daily loss limits, cooldowns
4. **Execute Buy**: Build swap IX, send TX, track pending

### Exit Flow
1. **Price Monitoring**: Continuous quote fetching via Router
2. **Stop-Loss/Take-Profit**: BPS-based triggers with tiered exits
3. **Time-Based Exits**: Force exits after `max_hold_secs`
4. **Kill Switch**: Emergency exit on dev sells or negative flow

### Key Structs
```rust
struct SniperCfg { ... }       // All config parameters
struct RiskState { ... }       // Position tracking, PnL, cooldowns
struct PositionLot { ... }     // Individual position with entry price
struct PendingTrade { ... }    // In-flight TX tracking
```

### Exit Priority Order
1. **Kill Switch** (emergency) → Jito bundle, max slippage
2. **Stop-Loss** → Immediate exit, triggers cooldown
3. **Take-Profit Tiers** → Partial exits at profit thresholds
4. **Time-Based Exits** → Partial/full exits after time thresholds
5. **Trailing Stop** → Dynamic SL that follows price up

## Arbitrage Module (`src/solana/arbitrage.rs`)

The `ArbitrageEngine` scans for triangular arbitrage opportunities across DEX pools.

### How It Works
1. **Pool Discovery**: Syncs Raydium (700k+ pools) and Orca (4k+ pools) snapshots
2. **Cycle Enumeration**: For each base token, DFS searches for 3-hop paths (A→B→C→A)
3. **Quote Evaluation**: Gets best quotes per hop via Router
4. **Profitability Filter**: Net profit after DEX fees + TX costs + slippage
5. **Execution**: Atomic Jito bundles (prevents frontrunning)

### Configuration
```toml
[arbitrage]
interval_ms = 2000                    # Scan every 2 seconds
min_profit_bps = 10                   # Minimum 10 bps profit
est_tx_cost_lamports = 5000000        # 0.05 SOL estimated cost

[arbitrage.discovery]
enable = true
mode = "full-auto"
base_tokens = [
    "So11111111111111111111111111111111111111112",    # SOL
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
]
min_liquidity_sol = 20.0
```

### Key Methods
```rust
impl ArbitrageEngine {
    // Enumerate all profitable triangles
    pub async fn enumerate_triangular_cycles(&self, base_tokens: &[String], amount_in: u64) -> Result<Vec<CycleOpportunity>>;
    
    // Build atomic TX plan for a triangle
    pub async fn assemble_triangle_plan(&self, a, b, c, amount, slippage_bps) -> Result<Option<TransactionPlan>>;
    
    // Simulate before sending
    pub async fn simulate_transaction_plan(&self, plan, fee_payer) -> Result<SimulationOutcome>;
}
```

### Metrics
| Metric | Description |
|--------|-------------|
| `arb_triangle_attempts_total` | Triangles evaluated |
| `arb_triangle_profitable_total` | Profitable cycles found |
| `arb_triangle_opportunities_total` | Opportunities per scan |

### Decimal Normalization
All amounts normalized to 9 decimals (SOL standard) for profit comparison:
```rust
fn normalize_amount(amount: u64, from_decimals: u8, to_decimals: u8) -> u64;
fn get_mint_decimals_fast(&self, mint: &str) -> u8;  // Hardcoded for common tokens
```

## Config Hot-Reload

**Feature-gated**: `cargo build --features notify_watch`

### Mechanisms
1. **File Watcher** (`notify_watch` feature): Watches config file for changes
2. **SIGHUP Handler** (Unix only): `kill -HUP <pid>` triggers reload

### Usage
```bash
# With file watcher
cargo run --release --features notify_watch -- --config my_config.toml

# Via environment variable
$env:IRONCRAB_SNIPER_RELOAD_PATH = "my_config.toml"
cargo run --release --features notify_watch
```

### Reload Behavior
- Changes are validated via `validate_sniper_cfg()` before applying
- Diff is logged via `diff_sniper_cfg()` showing what changed
- Invalid configs are rejected (bot continues with old config)
- Reload interval configurable: `hot_reload_secs = 30`

```rust
// src/config_reload.rs
pub fn diff_sniper_cfg(old: &SniperCfg, new: &SniperCfg) -> String;
pub fn validate_sniper_cfg(cfg: &SniperCfg) -> Result<(), String>;
```

## Backtest Engine

### Location
`src/backtest/engine.rs` - Historical simulation framework

### Running Backtests
```bash
# Windows
.\backtest.ps1 -config config.example.toml -start 2024-01-01 -end 2024-12-01

# Linux
./backtest.sh --config config.example.toml --start 2024-01-01 --end 2024-12-01
```

### Key Concepts
- Replay historical pool events from Geyser logs
- Simulate fills with realistic slippage model
- Track virtual PnL with same RiskState logic as live trading
- Output metrics compatible with Grafana dashboard

## Common Gotchas

1. **SPL Token Balance Offset**: Token account balance is at bytes `[64..72]`, not `[0..8]`
2. **Pump.fun 1% Fee**: Actual swap amount = `sol_sent * 0.99` (4 transfers per buy)
3. **ATA Must Exist Before Swap**: Use `spl_associated_token_account::get_associated_token_address` to derive, but check existence
4. **Commitment Levels**: Use `Confirmed` for speed (~2s), not `Finalized` (~20s) unless critical
5. **Config Reload**: Changes require `notify_watch` feature; validate before applying
6. **RiskState Persistence**: Positions auto-saved to `state.json` every `autosave_state_secs`

## File Quick Reference

| Path | Purpose |
|------|---------|
| `src/solana/sniper.rs` | Main trading engine (6000+ LOC) |
| `src/solana/arbitrage.rs` | Triangular arbitrage cycle detection |
| `src/solana/dex/*.rs` | DEX connectors (Raydium, Orca, Pump.fun) |
| `src/solana/geyser_*.rs` | Real-time Geyser streams |
| `src/config.rs` | TOML config parsing |
| `src/config_reload.rs` | Hot-reload, diff & validation |
| `src/metrics.rs` | Prometheus metrics |
| `src/backtest/engine.rs` | Historical backtesting |
| `tests/*.rs` | Integration tests |
| `docs/*.service` | Systemd configs |

## SSH/Server Operations
**Never attempt SSH connections autonomously** - always inform the user and wait for confirmation when server access is needed.

---
description: 'Rust programming language coding conventions and best practices'
applyTo: '**/*.rs'
---

# Rust Coding Conventions and Best Practices

Follow idiomatic Rust practices and community standards when writing Rust code. 

These instructions are based on [The Rust Book](https://doc.rust-lang.org/book/), [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/), [RFC 430 naming conventions](https://github.com/rust-lang/rfcs/blob/master/text/0430-finalizing-naming-conventions.md), and the broader Rust community at [users.rust-lang.org](https://users.rust-lang.org).

## General Instructions

- Always prioritize readability, safety, and maintainability.
- Use strong typing and leverage Rust's ownership system for memory safety.
- Break down complex functions into smaller, more manageable functions.
- For algorithm-related code, include explanations of the approach used.
- Write code with good maintainability practices, including comments on why certain design decisions were made.
- Handle errors gracefully using `Result<T, E>` and provide meaningful error messages.
- For external dependencies, mention their usage and purpose in documentation.
- Use consistent naming conventions following [RFC 430](https://github.com/rust-lang/rfcs/blob/master/text/0430-finalizing-naming-conventions.md).
- Write idiomatic, safe, and efficient Rust code that follows the borrow checker's rules.
- Ensure code compiles without warnings.

## Patterns to Follow

- Use modules (`mod`) and public interfaces (`pub`) to encapsulate logic.
- Handle errors properly using `?`, `match`, or `if let`.
- Use `serde` for serialization and `thiserror` or `anyhow` for custom errors.
- Implement traits to abstract services or external dependencies.
- Structure async code using `async/await` and `tokio` or `async-std`.
- Prefer enums over flags and states for type safety.
- Use builders for complex object creation.
- Split binary and library code (`main.rs` vs `lib.rs`) for testability and reuse.
- Use `rayon` for data parallelism and CPU-bound tasks.
- Use iterators instead of index-based loops as they're often faster and safer.
- Use `&str` instead of `String` for function parameters when you don't need ownership.
- Prefer borrowing and zero-copy operations to avoid unnecessary allocations.

### Ownership, Borrowing, and Lifetimes

- Prefer borrowing (`&T`) over cloning unless ownership transfer is necessary.
- Use `&mut T` when you need to modify borrowed data.
- Explicitly annotate lifetimes when the compiler cannot infer them.
- Use `Rc<T>` for single-threaded reference counting and `Arc<T>` for thread-safe reference counting.
- Use `RefCell<T>` for interior mutability in single-threaded contexts and `Mutex<T>` or `RwLock<T>` for multi-threaded contexts.

## Patterns to Avoid

- Don't use `unwrap()` or `expect()` unless absolutely necessary—prefer proper error handling.
- Avoid panics in library code—return `Result` instead.
- Don't rely on global mutable state—use dependency injection or thread-safe containers.
- Avoid deeply nested logic—refactor with functions or combinators.
- Don't ignore warnings—treat them as errors during CI.
- Avoid `unsafe` unless required and fully documented.
- Don't overuse `clone()`, use borrowing instead of cloning unless ownership transfer is needed.
- Avoid premature `collect()`, keep iterators lazy until you actually need the collection.
- Avoid unnecessary allocations—prefer borrowing and zero-copy operations.

## Code Style and Formatting

- Follow the Rust Style Guide and use `rustfmt` for automatic formatting.
- Keep lines under 100 characters when possible.
- Place function and struct documentation immediately before the item using `///`.
- Use `cargo clippy` to catch common mistakes and enforce best practices.

## Error Handling

- Use `Result<T, E>` for recoverable errors and `panic!` only for unrecoverable errors.
- Prefer `?` operator over `unwrap()` or `expect()` for error propagation.
- Create custom error types using `thiserror` or implement `std::error::Error`.
- Use `Option<T>` for values that may or may not exist.
- Provide meaningful error messages and context.
- Error types should be meaningful and well-behaved (implement standard traits).
- Validate function arguments and return appropriate errors for invalid input.

## API Design Guidelines

### Common Traits Implementation
Eagerly implement common traits where appropriate:
- `Copy`, `Clone`, `Eq`, `PartialEq`, `Ord`, `PartialOrd`, `Hash`, `Debug`, `Display`, `Default`
- Use standard conversion traits: `From`, `AsRef`, `AsMut`
- Collections should implement `FromIterator` and `Extend`
- Note: `Send` and `Sync` are auto-implemented by the compiler when safe; avoid manual implementation unless using `unsafe` code

### Type Safety and Predictability
- Use newtypes to provide static distinctions
- Arguments should convey meaning through types; prefer specific types over generic `bool` parameters
- Use `Option<T>` appropriately for truly optional values
- Functions with a clear receiver should be methods
- Only smart pointers should implement `Deref` and `DerefMut`

### Future Proofing
- Use sealed traits to protect against downstream implementations
- Structs should have private fields
- Functions should validate their arguments
- All public types must implement `Debug`

## Testing and Documentation

- Write comprehensive unit tests using `#[cfg(test)]` modules and `#[test]` annotations.
- Use test modules alongside the code they test (`mod tests { ... }`).
- Write integration tests in `tests/` directory with descriptive filenames.
- Write clear and concise comments for each function, struct, enum, and complex logic.
- Ensure functions have descriptive names and include comprehensive documentation.
- Document all public APIs with rustdoc (`///` comments) following the [API Guidelines](https://rust-lang.github.io/api-guidelines/).
- Use `#[doc(hidden)]` to hide implementation details from public documentation.
- Document error conditions, panic scenarios, and safety considerations.
- Examples should use `?` operator, not `unwrap()` or deprecated `try!` macro.

## Project Organization

- Use semantic versioning in `Cargo.toml`.
- Include comprehensive metadata: `description`, `license`, `repository`, `keywords`, `categories`.
- Use feature flags for optional functionality.
- Organize code into modules using `mod.rs` or named files.
- Keep `main.rs` or `lib.rs` minimal - move logic to modules.

## Quality Checklist

Before publishing or reviewing Rust code, ensure:

### Core Requirements
- [ ] **Naming**: Follows RFC 430 naming conventions
- [ ] **Traits**: Implements `Debug`, `Clone`, `PartialEq` where appropriate
- [ ] **Error Handling**: Uses `Result<T, E>` and provides meaningful error types
- [ ] **Documentation**: All public items have rustdoc comments with examples
- [ ] **Testing**: Comprehensive test coverage including edge cases

### Safety and Quality
- [ ] **Safety**: No unnecessary `unsafe` code, proper error handling
- [ ] **Performance**: Efficient use of iterators, minimal allocations
- [ ] **API Design**: Functions are predictable, flexible, and type-safe
- [ ] **Future Proofing**: Private fields in structs, sealed traits where appropriate
- [ ] **Tooling**: Code passes `cargo fmt`, `cargo clippy`, and `cargo test`
