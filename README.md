
# IronCrab – Solana‑First Tradingbot (Rust)

Version: **0.2.1** (Solana 2.x / Agave crates 3.x)

## Features
- Treasury mit ATA‑Erstellung, SPL‑Transfers, WSOL wrap/unwrap
- Engine mit Allocator + Strategie‑Interface (Rust/Python via Feature)
- DEX‑Skeletons (Raydium/Orca) + Raydium Pool‑Reader (on‑chain)
- Beispiel‑CLI `raydium_pools` zum Testen der Pool‑Reader

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
- Dieses Repo ist als **lib + bin** strukturiert. Andere Binaries können die Bibliothek `ironcrab` direkt nutzen.
- Solana‑Crates sind auf **3.x** gesetzt (Agave). SPL auf **7/8/9** (ATA/Token/Token‑2022).
