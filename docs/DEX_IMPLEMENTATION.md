# DEX Implementation Reference

**Stand:** 2026-08-22  
**Gilt für:** `architecture-rebuild`

IronCrab unterstützt **7 DEXes** für Discovery, Quotes und Swap-IXs. Program-IDs und Membership steuert **market-data** (explizites Geyser-Set), nicht die Yellowstone-Plugin-JSON. Siehe `docs/GEYSER_FILTER_UPDATE.md` und `docs/VALIDATOR_SETUP.md`.

Meteora DAMM v2 (`cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG`) ist **nicht** die IronCrab-Konstante `METEORA_CPMM`.

## Program IDs (`src/bin/market_data.rs`)

| DEX | Program ID | Discovery | Quote / Swap |
|-----|------------|-----------|--------------|
| Raydium AMM V4 | `675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8` | Geyser Account | ja |
| Raydium CPMM | `CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C` | Geyser Account | ja |
| Orca Whirlpool | `whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc` | Geyser Account | ja |
| PumpFun Bonding | `6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P` | Geyser TX + Account | ja |
| PumpSwap AMM | `pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA` | Geyser TX + Account | ja |
| Meteora DLMM | `LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo` | Geyser Account + Bin-PDAs | ja |
| Meteora CPMM | `cpmmpPFsKiR4eeYnGSuXgkhLLgGL1j5FUZoJBJU9t9D` | Geyser Account | ja |

## Module (ohne Zeilenzahlen)

Connectoren unter `src/solana/dex/`. Quoting und IX-Bau laufen über den gemeinsamen `Dex`-Pfad und `CrossDexHandler`; Live-State kommt aus `LivePoolCache` (JetStream-Slave in EE/Arb, Geyser-Master in market-data).

| Bereich | Typische Dateien |
|---------|------------------|
| Meteora DLMM | `meteora_dlmm.rs`, `meteora_dlmm_layout.rs`, `meteora_bin_walker.rs`, `meteora_bin_array_layout.rs`, `meteora_swap_builder.rs` |
| Meteora CPMM | `meteora_cpmm.rs`, `meteora_cpmm_layout.rs` |
| Raydium CPMM | `raydium_cpmm.rs` |
| Raydium AMM V4 / Orca / PumpFun / PumpSwap | jeweilige `dex/`-Module plus Parser in `dex_parser.rs` |

Hot Path: Cache-Hit, **kein RPC**. Cold Path (Liquidation, Bootstrap, manuelles Ensure): RPC hinter `allow_rpc_on_miss` / Request-Reply an market-data.

## DLMM Quotes

DLMM quoted über **Bin-Walking** (`meteora_bin_walker.rs`) aus gecachten Bin-Arrays, nicht über RPC-Fetches im Hot Path.

## Referenzen

- [GEYSER_FILTER_UPDATE.md](GEYSER_FILTER_UPDATE.md)
- [LIVE_POOL_CACHE_IMPLEMENTATION.md](LIVE_POOL_CACHE_IMPLEMENTATION.md)
- Eval-Spec: [TARGET_ARCHITECTURE.md](https://github.com/Exploratorsclub/Iron_crab-eval/blob/main/docs/spec/TARGET_ARCHITECTURE.md)
