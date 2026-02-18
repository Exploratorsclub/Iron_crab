# IronCrab AI Coding Guide

Dieses Repository wird aktuell **Debuggable-First** in eine klar getrennte Multi-Prozess-Architektur umgebaut.

**Source of Truth (bei Widerspruch gewinnt diese Doku):**
- `docs/TARGET_ARCHITECTURE.md`
- `docs/DEFINITION_OF_DONE.md`
- `docs/STORAGE_CONVENTIONS.md`
- `docs/ROLE_SEPARATION.md`

## Grundregeln (P0 / Live-verboten ohne Erfüllung)

- **Single-Signer**: Nur `execution-engine` lädt Keys und signiert/sendet.
- **Intent-only**: Alle anderen Prozesse sind keyless und erzeugen nur `TradeIntent`s bzw. `MarketEvents`.
- **Simulate-gated**: Simulation fail ⇒ niemals senden (insb. Arbitrage).
- **Decision Records**: Jede Entscheidung ist forensisch nachvollziehbar (Inputs, Checks, Outcome).
- **Units/Decimals explizit**: Keine impliziten Konventionen; Beträge müssen eindeutig sein.

Weitere Leitlinien:
- **Geyser gegenüber RPC bevorzugen** (Latenz/Last). RPC/WS nur als Fallback.
- **Arbitrage atomar** (Bundle/Jito) oder reason-coded reject.

## SSH/Server Operations

- SSH/Server-Commands sind erlaubt, **wenn der User sie explizit anfordert** (z. B. Deploy, Logs, Status prüfen) oder ausdrücklich zustimmt.
- Kein ungefragtes "mal schnell einloggen" oder Scannen/Probieren von Hosts/Ports.
- In bestehenden SSH-Terminals: keine automatischen `exit`/Disconnects.
- Preferred Auth: Key-based Login via `ssh-agent` (Passphrase nur einmal pro Session über `ssh-add`).

**SSH Login via Alias (empfohlen)**

Wenn lokal ein SSH-Config Alias existiert (z. B. `ironcrab-prod`), dann bevorzugt damit arbeiten (kürzer, weniger Fehler, Port/User/Key kommen aus `~/.ssh/config`).

Minimaler Config-Block (Windows/Linux/macOS):
```sshconfig
Host ironcrab-prod
    HostName .....
    User ironcrab
    Port ....
    IdentityFile ~/.ssh/id_ed.....
    IdentitiesOnly yes
```

Login:
```bash
ssh ironcrab-prod
```

Port-Forward (Control Plane):
```bash
ssh -L 8080:127.0.0.1:8080 ironcrab-prod
```

## Zielarchitektur (Debuggable-First Umbau)

Diese Repo-Historie enthält AI-schnellgeschriebenen Code. Priorität ist daher **Debuggability + Determinismus + Safety** vor Feature-Breite.

P0 Non-Negotiables stehen oben unter „Grundregeln“.

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

Hinweis: Wenn du Tests oder Clippy laufen lässt, ändere keine “unrelated” Dinge.

### Local Run
```bash
cargo run --release -- --config my_config.toml
```

## Ops / Repo Scripts (do not invent new ones)

### Deploy (Server, systemd)

Default = Multi-Process Deploy:
```bash
./deploy.sh
```

Direkt (mit Flags):
```bash
./deploy_new.sh
./deploy_new.sh --skip-build
./deploy_new.sh --component execution-engine
```

Legacy/Monolith (nur wenn explizit gewollt):
```bash
./deploy.sh --legacy
```

## Server Test Recipes (Dry-Run / No-Send)

Ziel: End-to-end testen (Strategy → NATS → execution-engine → DecisionRecord), ohne On-Chain Send.

### Publish a test SELL intent into NATS

Server hat häufig PEP 668 (externally managed Python) ⇒ **immer venv verwenden**.

1) Venv anlegen + NATS client installieren (einmalig):
```bash
ssh ironcrab-prod "mkdir -p ~/nats_tools; python3 -m venv ~/nats_tools/venv; ~/nats_tools/venv/bin/python -m pip install -q nats-py"
```

2) Publisher Script kopieren:
```bash
scp tools/nats_publish_sell_intent.py ironcrab-prod:~/nats_tools/nats_publish_sell_intent.py
```

3) Intent publishen (prints intent_id):
```bash
ssh ironcrab-prod "EXPECTED_ROI_BPS=2000 MAX_SLIPPAGE_BPS=100 ~/nats_tools/venv/bin/python ~/nats_tools/nats_publish_sell_intent.py"
```

### Validate processing without sudo/journalctl

Wenn `sudo journalctl` nicht möglich ist (no TTY), nutze DecisionRecord JSONL:

```bash
ssh ironcrab-prod "cd ~/Iron_crab; ls -1t trade_logs/decisions | head -n 3"
ssh ironcrab-prod "cd ~/Iron_crab; grep -n '<intent_id>' trade_logs/decisions/decision_records-$(date +%Y%m%d).jsonl | tail -n 1"
```

Erwartung (SELL dry-run Pipeline):
- `sell_token_balance` check erscheint (ATA + available/required)
- `capital_lock` zeigt `token:<mint>`
- Outcome endet bei `send_enabled=false` (oder später bei `send_not_implemented`, je nach Config)

### UI/Dashboard (lokal, via SSH Port Forwarding)

Prerequisite: Tunnel zum server-side Control Plane (läuft serverseitig auf `127.0.0.1:8080`):
```bash
ssh -L 8080:127.0.0.1:8080 ironcrab@<server>
```

Wenn du *alle* relevanten Ports (Control Plane + Grafana + Prometheus + Metrics) forwarden willst **und** Vite automatisch in einem separaten Prozess starten möchtest, nutze das Repo-Helper-Script:

Windows (SSH Tunnel + Vite UI in separatem Prozess):
```powershell
.\run_local.ps1 -Action start -Host <SERVER_IP> -User <USERNAME> -Port <PORT>
.\run_local.ps1 -Action start -Host ironcrab-prod
.\run_local.ps1 -Action status
.\run_local.ps1 -Action stop
```

Optional:
- Tunnel ohne UI: `.\run_local.ps1 -Action start -NoUi`
- UI ohne Tunnel: `.\run_local.ps1 -Action start -NoTunnel`
- Keyfile: `.\run_local.ps1 -Action start -IdentityFile C:\path\to\id_rsa`

Dann Vite UI lokal starten:

Windows:
```powershell
./run_ui.ps1
```

Windows (wenn PowerShell `npm.ps1` blockt):
```bat
run_ui.cmd
```

macOS/Linux:
```bash
cd ui
npm install
npm run dev
```

### CI Requirements (`.github/workflows/ci.yml`)
- Rust 1.89.0 with `protobuf-compiler` for Geyser gRPC
- All tests must pass with both default and `test_helpers` features
- Clippy warnings are errors

## Project-Specific Patterns

### IPC Schema / Decision Records
- IPC Types liegen in `src/ipc/schema.rs`.
- JSONL Storage muss die Minimalfelder aus `RecordHeader` enthalten (`schema_version`, `ts_unix_ms`, `component`, `build`, `run_id`).
- Amounts müssen als `ExplicitAmount` modelliert werden (raw + decimals + optional ui).

### NATS Topics (ist-Stand)

Rust (versioniert, canonical): `src/nats/topics.rs`
- `ironcrab.v1.market_events`
- `ironcrab.v1.trade_intents`
- `ironcrab.v1.execution_results`
- `ironcrab.v1.control_requests` / `ironcrab.v1.control_responses`
- `ironcrab.v1.decision_records`

Control-Plane (Python) nutzt aktuell (und Rust-Binaries hören teils darauf):
- `ironcrab.control.commands`
- `ironcrab.control.kill`
- `ironcrab.control.config.reload`

Regel:
- Keine neuen ad-hoc Topics erfinden.
- Wenn du Control/IPC anfasst: Topics vereinheitlichen oder wenigstens klar dokumentieren (prefer: Versioned Topics).

### Logs / Replay (JSONL)
- Root ist `IRONCRAB_LOG_DIR` oder Default `trade_logs/`.
- Binaries schreiben append-only JSONL mit täglicher Rotation via `JsonlWriter` (`src/storage/jsonl_writer.rs`).
- Default Subdirs (ist-Stand):
    - market-data: `trade_logs/market_events/` (`market_events-YYYYMMDD.jsonl`)
    - momentum-bot: `trade_logs/intents/` (`trade_intents-YYYYMMDD.jsonl`)
    - arb-strategy: `trade_logs/arb_intents/` (`arb_intents-YYYYMMDD.jsonl`)
    - execution-engine: `trade_logs/decisions/`, `trade_logs/executions/`
- Hot-Path safe: wenn FS/DB riskant wäre, `AsyncJsonlWriter` nutzen oder buffering/queueing.

### Metrics Ports (multi-process)
- market-data: `9801`, momentum-bot: `9802`, arb-strategy: `9803`, execution-engine: `9804` (jeweils `/metrics`).

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

### Key Handling (Single-Signer Enforcement)
- `execution-engine` ist der einzige Prozess mit Key-Loading/Signing/Sending.
- Keyless Prozesse müssen beim Erkennen von Key-Env-Vars sofort `exit(1)` (mindestens `IRONCRAB_KEYPAIR_JSON|B64|PATH`; wenn du das anfasst, nimm auch `IRONCRAB_KEYPAIR_BASE58` dazu und halte es konsistent zu `docs/ROLE_SEPARATION.md`).
- Wenn du Key-Loading anfasst: bevorzugt eine zentrale Implementierung (z. B. `Treasury::load_from_env()` aus `src/wallet.rs`) statt duplizierter Loader-Logik.

## Dynamic Subscription Pattern

Für Confirmation/Balance-Watches nur spezifische Accounts (z. B. ATA) subscriben; keine Vollabos auf Token Program.

## Legacy-Code Hinweis (wichtig)

Dieses Repo enthält noch Legacy-/Monolith-Code aus der Vorgängerphase.
Neue Änderungen sollen sich an `docs/TARGET_ARCHITECTURE.md` orientieren (neue Binaries/Prozesse, Intent-only, Single-Signer), statt den Legacy-Monolithen weiter auszubauen.

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