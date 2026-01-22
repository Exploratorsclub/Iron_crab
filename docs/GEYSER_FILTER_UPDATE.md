# Geyser Filtering: Client-Side Implementation

## Status: Implemented ✅

**Stand**: Januar 2026

## Architektur-Entscheidung

### Yellowstone gRPC Plugin Limitation

Das Yellowstone gRPC Plugin unterstützt **keine server-seitigen Account Owner Filter**.
Die `geyser-grpc-plugin-config.json` enthält daher nur minimale Konfiguration:

```json
{
  "libpath": "/opt/geyser/libgeyser_grpc.so",
  "grpc": {
    "address": "0.0.0.0:10000"
  }
}
```

### Konsequenz: Client-Side Filtering

Das Filtering nach DEX Program IDs erfolgt **client-seitig** in `market-data`:

**File**: `src/bin/market_data.rs`

```rust
// DEX Program IDs for client-side filtering
const RAYDIUM_AMM_V4: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const ORCA_WHIRLPOOL: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";
const PUMPFUN_BONDING: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
const PUMPSWAP_AMM: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
const METEORA_DLMM: &str = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo";
const METEORA_CPMM: &str = "cpmmpPPPsf4FLyJpqjBnFMVAYkdNrmnHb9Wrt8KLPZC";
```

## Unterstützte DEXes

| DEX | Program ID | Pool Discovery |
|-----|-----------|----------------|
| Raydium AMM V4 | `675kPX9...` | Geyser Account Updates |
| Orca Whirlpool | `whirLbM...` | Geyser Account Updates |
| PumpFun Bonding | `6EF8rre...` | Geyser + Trade-Based |
| PumpSwap AMM | `pAMMBay...` | Geyser + Trade-Based |
| Meteora DLMM | `LBUZKhR...` | Geyser Account Updates |
| Meteora CPMM | `cpmmpPP...` | Geyser Account Updates |

## Trade-Based Discovery (Fallback)

Für PumpFun/PumpSwap nutzt `momentum-bot` zusätzlich **Trade-Based Discovery**:

```rust
// Falls Geyser-Event verpasst wird, kann ein Trade das Token tracken
if is_trade_event && !token_already_tracked {
    let dex = infer_dex_from_pool_address(pool_address);
    initialize_token_tracker(mint, dex);
}
```

## Deployment

Kein Validator-Restart erforderlich für Filter-Änderungen, da das Filtering client-seitig erfolgt.

**Neue DEXes hinzufügen**:
1. Program ID in `market_data.rs` hinzufügen
2. `cargo build --release --bin market-data`
3. `./deploy_new.sh --component market-data`

## Performance Consideration

Client-seitiges Filtering bedeutet höhere Bandbreite vom Geyser zum Client.
Bei hoher Last kann dies zu Backpressure führen.

**Mitigations**:
- Geyser läuft lokal auf dem gleichen Server
- NATS JetStream für Event-Buffering
- `market-data` filtert früh und verwirft irrelevante Events

## Referenzen

- [Yellowstone gRPC Plugin](https://github.com/rpcpool/yellowstone-grpc)
- [docs/TARGET_ARCHITECTURE.md](TARGET_ARCHITECTURE.md) - Process Boundaries
- [src/bin/market_data.rs](../src/bin/market_data.rs) - Client-side Implementation
