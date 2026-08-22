# Geyser Filtering: Client-Side

**Stand:** 2026-08-22

## Plugin-JSON (minimal)

Das Yellowstone-gRPC-Plugin in der Repo-Vorlage hat **keine** server-seitigen Owner-Filter. Kanonische Datei: `docs/geyser-grpc-plugin-config.json`.

```json
{
  "libpath": "/usr/local/lib/solana/libyellowstone_grpc_geyser.so",
  "grpc": {
    "address": "127.0.0.1:10000"
  }
}
```

Auf dem Server: `/home/sol/geyser-config.json`, gebunden in `agave-validator` via `--geyser-plugin-config`. Port **10000**.

## Filtering in market-data

DEX-Program-IDs und Account-Membership steuert **market-data** (explizites Geyser-Set, Track-Requests, Wallet-/Momentum-/Arb-Pins). Konstanten in `src/bin/market_data.rs`:

| DEX | Program ID |
|-----|------------|
| Raydium AMM V4 | `675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8` |
| Raydium CPMM | `CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C` |
| Orca Whirlpool | `whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc` |
| PumpFun | `6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P` |
| PumpSwap | `pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA` |
| Meteora DLMM | `LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo` |
| Meteora CPMM | `cpmmpPFsKiR4eeYnGSuXgkhLLgGL1j5FUZoJBJU9t9D` |

PumpFun/PumpSwap: zusätzlich TX-basierte Discovery. Neue Program-IDs: Code + Restart **market-data**, kein Validator-Restart nur für Filter.

Caps und Full-Reconnect: `[market_data_geyser]` in der Config (`docs/CONFIG_SCHEMA.md`). Architektur: `docs/VALIDATOR_SETUP.md`.

## Referenzen

- [Yellowstone gRPC](https://github.com/rpcpool/yellowstone-grpc)
- [VALIDATOR_SETUP.md](VALIDATOR_SETUP.md)
