# IronCrab AI Coding Guide

Dieses Repository wird aktuell **Debuggable-First** in eine klar getrennte Multi-Prozess-Architektur umgebaut.

**Source of Truth (bei Widerspruch gewinnt diese Doku):**
- `docs/TARGET_ARCHITECTURE.md`
- `docs/DEFINITION_OF_DONE.md`
- `docs/STORAGE_CONVENTIONS.md`

**Grundregeln:**
- Geyser gegenüber RPC bevorzugen (Latenz/Last).
- Arbitrage-Ausführungen atomar (Bundle) und über Jito senden.
- Keine eigenständigen SSH-Verbindungsversuche: wenn SSH nötig ist, User informieren und warten.

## Zielarchitektur (Debuggable-First Umbau)

Diese Repo-Historie enthält AI-schnellgeschriebenen Code. Priorität ist daher **Debuggability + Determinismus + Safety** vor Feature-Breite.

**Non-negotiables (P0 / Live-verboten ohne Erfüllung)**
- **Single-Signer**: Nur die Execution Engine darf signieren/senden. Strategien/Worker/Bots sind **keyless**.
- **Intent-only**: Strategien/Worker erzeugen ausschließlich `TradeIntent`s. Keine direkten RPC/TPU/Jito Sends außerhalb der Execution Engine.
- **Simulate-gated**: Wenn Simulation fehlschlägt, wird **nie** gesendet (insb. Arbitrage).
- **Decision Records**: Jede Entscheidung muss nachvollziehbar sein (Input-Snapshots + Reasons + Outcome).
- **Units/Decimals explizit**: Keine impliziten UI/raw Konventionen; jede Amount ist eindeutig normalisiert oder trägt Decimals.

**Process Boundaries (für Debuggability & Fault Isolation)**
- System wird als **mehrere Binaries/Prozesse** aufgebaut (nicht als ein riesiger Monolith), mindestens:
    - `execution-engine` (Rust): Arbitration + Locks + Tx Plan/Sim/Send/Confirm, besitzt Keys.
    - `market-data` (Rust): Geyser/Events ingest + Cache/Normalisierung + Discovery Worker → `MarketEvents` (lädt Daten einmal).
    - `momentum-bot` (Rust): konsumiert `MarketEvents` und erzeugt `TradeIntent` (Policies: EARLY + ESTABLISHED).
    - `control-plane` (FastAPI): Start/Stop/Config/Risk/Monitoring; niemals Teil des Trading-Hot-Path; keine Keys.

**Architektur-Topologie (vereinfachte Sicht):**

```
Geyser/RPC
    │
    ▼
market-data  ── MarketEvents ──►  momentum-bot  ── TradeIntents ──►  execution-engine  ── ExecutionResults ──►  control-plane/UI
                                                                         │
                                                                         └──────────── (optional) arb-strategy (Typ A) ─────┘

Hinweis:
- Typ A (marktgetriebene Arbitrage) gehört in Strategy Plane (erzeugt Intents).
- Typ B (reaktive Tx-abhängige MEV) sind Worker **in** der execution-engine.
```

**MVP-Regel („First Results Fast“ ohne Debug-Sumpf)**
- Immer zuerst einen **kleinen Vertical Slice** liefern (1 DEX / 1 Pair / 1 Strategy / 1 Tx-Typ), bis `docs/DEFINITION_OF_DONE.md` (P0) erfüllt ist.
- Keine neuen Features, wenn sie nicht Decision Records + Reason-coded Rejects + (wo passend) Simulation-Gates mitliefern.

## Critical Dev Workflows

### Build & Test
```bash
cargo fmt --check              # CI enforces this
cargo clippy -- -D warnings    # Must pass with zero warnings
cargo test --features test_helpers  # Enables internal test helpers
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
impl Engine {
    pub fn test_insert_position_lot(&self, mint: Pubkey, ...) { ... }
    pub fn test_get_realized_pnl_sol(&self) -> f64 { ... }
}
```

### Concurrency with `parking_lot`
Use `parking_lot::RwLock` instead of `std::sync::RwLock` for better performance:
```rust
state: Arc<parking_lot::RwLock<State>>
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

Die Geyser-Plugin-Konfiguration muss zu Validator + Plugin-Pfad passen.
Siehe `docs/geyser-grpc-plugin-config.json`.

## Dynamic Subscription Pattern

Für Confirmation/Balance-Watches nur spezifische Accounts (z. B. ATA) subscriben; keine Vollabos auf Token Program.

## Storage / Replay

- Hot-Path-safe: keine DB/FS-Blocker im Execution Hot Path.
- Append-only Flat Files als P0-Standard, siehe `docs/STORAGE_CONVENTIONS.md`.

## Legacy-Code Hinweis (wichtig)

Dieses Repo enthält noch Legacy-/Monolith-Code aus der Vorgängerphase.
Neue Änderungen sollen sich an `docs/TARGET_ARCHITECTURE.md` orientieren (neue Binaries/Prozesse, Intent-only, Single-Signer), statt den Legacy-Monolithen weiter auszubauen.

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
