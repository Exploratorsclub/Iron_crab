
# IronCrab – Solana‑First Tradingbot (Rust)

Version: **0.3.0-dev** (Agave / Solana 3.x line)  
Legacy (Solana 1.18 baseline): tag (to be created) `v0.2.1-solana1_18`.

> Migration in progress. See `MIGRATION.md` for details on the upgrade from the legacy 1.18 toolchain to Agave / 3.x crates. The active development branch is `solana3x_clean` (may be renamed / merged soon).

## Features
- Treasury: ATA Erstellung, SPL Transfers, WSOL wrap/unwrap
- Engine: Allocator + Strategie‑Interface (Rust / optional Python via Feature)
- DEX: Raydium / Orca Skeletons, Raydium Pool Reader + Quoting
- Backtest: Slippage Enforcement & Tests
- CLI Tools: `raydium_pools`, `backtest_driver`

## Build & Run (PowerShell)
```powershell
cargo clean
Remove-Item Cargo.lock -ErrorAction SilentlyContinue
cargo run --release -- --config .\config.example.toml
```

### Python‑Strategien (optional)
```powershell
cargo run --release --features python -- --config .\config.example.toml
```

### Raydium Pool‑Reader CLI
```powershell
$env:RPC_URL="http://127.0.0.1:8899"
cargo run --bin raydium_pools -- --mint So11111111111111111111111111111111111111112 --active
```

## Hinweise
- Struktur: Library + mehrere Binaries (erweiterbar)
- Solana Client / Agave Crates: **3.x**
- (Legacy) Vor-Upgrade Code: siehe Tag `v0.2.1-solana1_18`
- Siehe `MIGRATION.md` für offene Schritte (Swap Builder, PDA Ableitungen, Arbitrage Planner, Orca Parität)
