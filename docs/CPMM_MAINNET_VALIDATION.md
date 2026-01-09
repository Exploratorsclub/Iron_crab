# CPMM Mainnet Validation - Server Testing Guide

**Status**: Ready for deployment testing  
**Branch**: `architecture-rebuild`  
**Commit**: Latest (Meteora DLMM + Raydium CPMM)

---

## Quick Deploy & Test

### 1. Pull Latest Code on Server

```bash
ssh ironcrab-prod
cd ~/Iron_crab
git fetch origin
git checkout architecture-rebuild
git pull origin architecture-rebuild
```

### 2. Build Release Binaries

```bash
# Build market-data with CPMM support
cargo build --release --bin market-data

# Should see successful compilation (warnings OK)
```

### 3. Validate CPMM Pools from Mainnet

**Option A: Python Validator (Quick Check)**

```bash
# Install dependencies if needed
python3 -m venv ~/cpmm_venv
source ~/cpmm_venv/bin/activate
pip install requests

# Run validator
cd ~/Iron_crab
python3 tools/validate_cpmm_pools.py

# Check output
cat cpmm_pools_analysis.json | jq '.[0]'
```

Expected output:
```json
{
  "pubkey": "...",
  "size": 752,  // or actual size found
  "discriminator": "...",
  "status": 1,
  "pubkey_offset_16": "...",
  "u64_offset_176": 2500  // fee rate example
}
```

**Option B: Rust Integration Test (Full Validation)**

```bash
# Create integration test
cd ~/Iron_crab
```

Create `tests/cpmm_mainnet_validation.rs`:

```rust
#[tokio::test]
#[ignore]  // Run with --ignored flag
async fn test_cpmm_mainnet_pools() {
    use ironcrab::solana::dex::raydium_cpmm::RaydiumCpmm;
    use ironcrab::solana::dex::Dex;
    use ironcrab::solana::rpc::SolanaRpc;
    use std::sync::Arc;
    
    let rpc = Arc::new(SolanaRpc::new("https://mainnet.helius-rpc.com/?api-key=YOUR_KEY"));
    let cpmm = RaydiumCpmm::new(rpc.clone());
    
    // Fetch pools
    cpmm.refresh_pools().await.expect("Failed to refresh pools");
    
    let pools = cpmm.list_pools();
    println!("Found {} CPMM pools", pools.len());
    
    assert!(pools.len() > 0, "Should find at least one CPMM pool");
    
    // Test quote on first pool
    let pairs = cpmm.list_pairs();
    if let Some((mint_in, mint_out)) = pairs.first() {
        let quote = cpmm.quote_exact_in(mint_in, mint_out, 1_000_000).await;
        println!("Quote: {:?}", quote);
        assert!(quote.is_ok());
    }
}
```

Run test:
```bash
cargo test --test cpmm_mainnet_validation --ignored -- --nocapture
```

### 4. Verify Account Size

If Python validator finds different size than 752:

```bash
# Update Rust constant
cd ~/Iron_crab
# Edit src/solana/dex/raydium_cpmm.rs
# Change: const CPMM_POOL_ACCOUNT_SIZE: usize = <ACTUAL_SIZE>;

# Rebuild
cargo build --release --bin market-data
```

### 5. Test market-data with CPMM

**Check Config:**

```bash
cat ~/Iron_crab/my_config.server.toml | grep -A 5 "\[market_data\]"
```

Should have:
```toml
[market_data]
enable_raydium_cpmm = true
enable_meteora_dlmm = true
```

**Dry Run Test:**

```bash
# Stop current market-data if running
sudo systemctl stop ironcrab-market-data

# Run manually to see CPMM events
cd ~/Iron_crab
RUST_LOG=info ./target/release/market-data --config my_config.server.toml

# Watch for logs like:
# [INFO] Loaded CPMM pool: <pubkey> (<mint0>/<mint1>)
# [INFO] Found N Raydium CPMM pools
```

### 6. Restart Services (if all good)

```bash
# Restart market-data with new binary
sudo systemctl restart ironcrab-market-data

# Check logs
sudo journalctl -u ironcrab-market-data -f | grep -i cpmm
```

---

## Expected Validation Results

### ✅ Success Criteria

1. **Python Validator**:
   - Finds CPMM pools (at least 1+)
   - Account size determined (likely 752 or close)
   - Discriminator present (8 bytes)
   - Pubkeys at offsets 16, 48, 80, 112, 144 non-zero
   - Fee rate u64 at offset 176+ reasonable (e.g., 2500 = 0.25%)

2. **Rust Build**:
   - ✅ Compiles without errors
   - ⚠️ Warnings OK (unused fields, etc.)

3. **Integration Test**:
   - refresh_pools() succeeds
   - Finds pools (N > 0)
   - quote_exact_in() returns valid quote
   - No panics or unwrap failures

4. **market-data Logs**:
   ```
   [INFO] Fetching Raydium CPMM pools via getProgramAccounts
   [INFO] Found N Raydium CPMM pools
   [INFO] Loaded CPMM pool: <addr> (<mint0>/<mint1>)
   ```

### ❌ Failure Scenarios

**Scenario 1: No pools found**
- Check program ID correct: `CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C`
- Verify RPC endpoint working
- Try without size filter

**Scenario 2: Parse errors**
- Account size mismatch → Update `CPMM_POOL_ACCOUNT_SIZE`
- Offset misalignment → Analyze raw bytes in Python output
- Check discriminator matches expected pattern

**Scenario 3: Quote calculation fails**
- Reserve balances zero → Pools not initialized yet
- Overflow in calculation → Check reserve sizes
- Fee rate invalid → Verify offset 176+ parsing

---

## Troubleshooting Commands

```bash
# Check if CPMM pools exist on mainnet
curl -X POST https://mainnet.helius-rpc.com/?api-key=YOUR_KEY \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "getProgramAccounts",
    "params": [
      "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C",
      {"encoding": "base64", "filters": [{"dataSize": 752}]}
    ],
    "id": 1
  }' | jq '.result | length'

# If no results, try other sizes
for size in 512 800 1024; do
  echo "Size $size:"
  curl -X POST ... -d "{..., \"filters\": [{\"dataSize\": $size}]}" | jq '.result | length'
done

# Check validator account-index
ssh ironcrab-prod
ps aux | grep solana-validator | grep -o 'account-index-include-key [^ ]*' | grep CPMM

# Should see: account-index-include-key CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C
```

---

## Next Steps After Validation

1. ✅ **Validation Passed** → Integration tests → arb-strategy
2. ⚠️ **Size Mismatch** → Update constant → Rebuild → Re-test
3. ❌ **No Pools Found** → Verify program ID → Check mainnet → Contact Raydium

---

**Questions?** Check:
- `docs/METEORA_CPMM_IMPLEMENTATION.md` (Architecture)
- `src/solana/dex/raydium_cpmm.rs` (Implementation)
- `tools/validate_cpmm_pools.py` (Validator source)
