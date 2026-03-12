# IronCrab Agent Instructions

## Cursor Cloud specific instructions

### Codebase overview

IronCrab is a Solana-first multi-process trading bot in Rust (v0.4.0, Agave/Solana 3.x). Architecture: Geyser gRPC → market-data → NATS → strategies → execution-engine → Solana. See `README.md` for full architecture diagram.

### Services

| Service | Type | Port | Start command |
|---|---|---|---|
| NATS (JetStream) | Infra | 4222 | `nats-server -js -p 4222` |
| control-plane | Python (FastAPI) | 8080 | `cd /workspace && uvicorn control_plane.main:app --host 127.0.0.1 --port 8080` |
| UI | React/Vite | 5173 | `cd /workspace/ui && npm run dev` |
| market-data | Rust | 9801 | Requires Geyser gRPC endpoint (external) |
| momentum-bot | Rust | 9802 | Requires NATS + market-data running |
| arb-strategy | Rust | 9803 | Requires NATS + market-data running |
| execution-engine | Rust | 9804 | Requires NATS + Solana RPC + Keypair |

### Lint / Test / Build (see README.md)

- **Format**: `cargo fmt --all -- --check`
- **Lint**: `cargo clippy --all-targets -- -D warnings`
- **Test**: `cargo test --quiet` (176+ unit tests, also `cargo test --features test_helpers`)
- **Build**: `cargo build` (dev) / `cargo build --release` (prod)
- **UI build**: `cd ui && npm run build`

### Non-obvious caveats

- `protobuf-compiler` and `libprotobuf-dev` are **required** system packages for building — the `yellowstone-grpc-proto` dependency needs `protoc`.
- Rust 1.89.0 is pinned in `rust-toolchain.toml` and auto-selected by rustup in `/workspace`.
- The Rust trading binaries (market-data, momentum-bot, arb-strategy, execution-engine) require external Solana infrastructure (Geyser gRPC, RPC endpoint, NATS). They cannot run standalone in a sandboxed cloud VM without those connections.
- The control-plane and UI can run locally with just NATS. The control-plane connects to NATS on startup and shows warnings for unreachable Rust components — this is expected.
- The kill switch can be toggled via `POST /kill` and verified via `GET /health` (`kill_switch_inactive` field).
- `config.example.toml` → `config.toml` is needed before running any Rust binary. The example config sets `dry_run = true` by default.
- Python packages install to `~/.local/bin` — ensure this is on `PATH` when running `uvicorn`.
- NATS server binary is installed at `/usr/local/bin/nats-server`.
