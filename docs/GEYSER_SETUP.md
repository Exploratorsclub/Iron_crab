# Professional Geyser gRPC Setup for Sniper Bot

## Architecture Overview

```
Agave Validator (3.0.11)
    ↓ (gRPC stream)
Yellowstone Geyser Plugin
    ↓ (accounts + transactions)
Sniper Bot
    ↓ (parsed pool data)
Trading Logic
```

## Step 1: Validator Geyser Configuration

Your validator needs the Yellowstone Geyser plugin. Check if it's running:

```bash
# On validator server
journalctl -u agave-validator -f | grep -i geyser
netstat -tuln | grep 10000  # Check if gRPC port is open
```

### Required Geyser Plugin Config (`geyser-grpc-config.json`):

```json
{
  "libpath": "/opt/solana/plugins/libyellowstone_grpc_geyser.so",
  "bind_address": "127.0.0.1:10000",
  "log": {
    "level": "info"
  },
  "grpc": {
    "max_decoding_message_size": "4_194_304",
    "channel_capacity": "100_000",
    "unary_concurrency_limit": 100,
    "unary_disabled": false
  },
  "block_fail_action": "log"
}
```

**Important:** Account filters are set in the **client code**, NOT in this config file!
The bot's `GeyserListener` handles the subscription filters.

### Validator Startup Flag:

```bash
agave-validator \
  --geyser-plugin-config /path/to/geyser-grpc-config.json \
  --rpc-port 8899 \
  --dynamic-port-range 8000-8020 \
  --entrypoint entrypoint.mainnet-beta.solana.com:8001 \
  --expected-genesis-hash 5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d \
  --wal-recovery-mode skip_any_corrupted_record \
  --limit-ledger-size 50000000 \
  --enable-rpc-transaction-history \
  --enable-extended-tx-metadata-storage \
  --rpc-pubsub-enable-block-subscription \
  --full-rpc-api \
  --account-index program-id \
  --account-index spl-token-owner \
  --account-index spl-token-mint
```

## Step 2: Install Yellowstone Geyser Plugin

```bash
# Download pre-built or build from source
git clone https://github.com/rpcpool/yellowstone-grpc.git
cd yellowstone-grpc/yellowstone-grpc-geyser
cargo build --release

# Copy to validator plugins directory
sudo cp target/release/libyellowstone_grpc_geyser.so /opt/solana/plugins/
```

## Step 3: Advantages over WebSocket

| Method | Latency | Data Quality | CPU Usage | Reliability |
|--------|---------|--------------|-----------|-------------|
| **Geyser gRPC** | **<5ms** | **Full account data** | **Low** | **99.9%** |
| WebSocket logs | 50-200ms | Text parsing | High | 95% |
| REST polling | 500-2000ms | Delayed | Medium | 90% |

### Geyser Benefits:

1. **Account Updates Stream:** Get full account data when pool is created
2. **Transaction Stream:** Get parsed transaction with all accounts
3. **No Regex Parsing:** Structured protobuf data
4. **Backpressure:** Built-in flow control
5. **Reconnection:** Automatic with slot tracking

## Step 4: Bot Integration

The bot will use `yellowstone-grpc-client` crate:

```rust
// Instead of WebSocket logsSubscribe
let mut geyser_client = GeyserGrpcClient::connect(url).await?;

// Subscribe to account updates
let accounts_stream = geyser_client.subscribe_accounts(
    vec!["675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"]
).await?;

// Process new pool accounts as they appear
while let Some(update) = accounts_stream.next().await {
    let account = update.account;
    let pubkey = update.pubkey;
    
    // Parse pool data directly from account bytes
    if let Some(pool) = parse_raydium_pool(&account.data) {
        process_new_pool(pubkey, pool).await?;
    }
}
```

## Step 5: Network Optimization

For sub-10ms latency:

```bash
# On validator server
sudo sysctl -w net.core.rmem_max=134217728
sudo sysctl -w net.core.wmem_max=134217728
sudo sysctl -w net.ipv4.tcp_rmem="4096 87380 134217728"
sudo sysctl -w net.ipv4.tcp_wmem="4096 65536 134217728"
sudo sysctl -w net.core.netdev_max_backlog=5000

# Disable TCP slow start
sudo sysctl -w net.ipv4.tcp_slow_start_after_idle=0
```

## Next Steps

1. Check if Geyser plugin is installed: `ls -la /opt/solana/plugins/`
2. Verify gRPC endpoint: `grpcurl -plaintext 127.0.0.1:10000 list`
3. Bot migration: Replace WebSocket subscriptions with Geyser streams

