# Deployment and Testing Guide

## Quick Deployment

Execute these commands on the server to deploy the transaction subscription implementation:

```bash
# Navigate to project directory
cd ~/Iron_crab

# Pull latest changes
git fetch origin solana3x_clean
git checkout solana3x_clean
git pull

# Rebuild with new transaction subscription code
./build.sh

# Restart the service
sudo systemctl restart ironcrab

# Monitor logs for transaction events
sudo journalctl -u ironcrab -f | grep -E "TRANSACTION DETECTED|processing transactions"
```

## What to Expect

### 1. Transaction Processing Logs

You should see periodic logs showing transaction processing:

```
INFO geyser_listener: processing transactions total_transactions=100 slot=385160000
INFO geyser_listener: processing transactions total_transactions=200 slot=385160050
```

This confirms transactions are being received and processed.

### 2. Pool Creation Transaction Logs

When a new pool is created, you'll see detailed transaction analysis:

```
INFO geyser_pool_discovery: TRANSACTION DETECTED - analyzing for token mint
  signature=5KJ7... 
  slot=385160123
  dex=PumpFun
  account_count=8
  accounts=["AdMYAaoLeoxad...", "HhJpBhRRn...", "WAMSM...", ...]
```

### 3. Account Keys Analysis

The `accounts` array in the log contains all pubkeys from the transaction. We need to:

1. **Capture 5-10 Pump.fun pool creation transactions**
2. **Find each account on Solscan** to identify which is the token mint
3. **Determine the consistent account index** (likely 2 or 3)

Example analysis:
```
Transaction: 5KJ7...
accounts[0]: AdMYAaoLeoxad... → Solscan shows: "Bonding Curve PDA"
accounts[1]: HhJpBhRRn... → Solscan shows: "Associated Bonding Curve"
accounts[2]: WAMSM... → Solscan shows: "Token Mint" ← THIS IS IT!
accounts[3]: So111... → "Native SOL"
...
```

## Data Collection Commands

### Capture Transaction Logs

```bash
# Save 30 minutes of transaction logs for analysis
sudo journalctl -u ironcrab --since "30 minutes ago" | grep "TRANSACTION DETECTED" > ~/tx_analysis.log

# Filter only Pump.fun transactions
grep "dex=PumpFun" ~/tx_analysis.log > ~/pumpfun_transactions.log
```

### Extract Account Arrays

```bash
# Extract just the account arrays for easier analysis
grep "accounts=" ~/pumpfun_transactions.log | sed 's/.*accounts=\[/[/' > ~/account_keys.txt
```

### Check Transaction on Solscan

For each signature in the logs:
1. Go to https://solscan.io
2. Paste the signature
3. View the "Instruction Accounts" section
4. Identify which account is the token mint (look for "Token Mint" label)

## Monitoring Commands

### Real-time Transaction Monitoring

```bash
# Watch transaction processing (updates every 100 txs)
sudo journalctl -u ironcrab -f | grep "processing transactions"

# Watch for pool creation transactions
sudo journalctl -u ironcrab -f | grep "TRANSACTION DETECTED"

# Watch for any errors
sudo journalctl -u ironcrab -f | grep -i "error\|warn"
```

### Check Service Status

```bash
# Service health
sudo systemctl status ironcrab

# Recent errors
sudo journalctl -u ironcrab -n 50 --no-pager | grep -i error

# Performance metrics
sudo journalctl -u ironcrab --since "5 minutes ago" | grep -E "total_transactions|account_update_count"
```

## Expected Performance

### Normal Operation

- **Transaction throughput:** 50-200 transactions/second during busy periods
- **Relevant transactions:** 1-5 pool creations per minute
- **CPU impact:** <5% increase vs accounts-only
- **Memory:** ~+50MB for transaction buffers

### Log Volume

- **Transaction logs:** Every 100 transactions (~1-2 per second during peak)
- **Pool detection logs:** 1-5 per minute
- **Account update logs:** Every 1000 updates (~1 per second)

## Troubleshooting

### No Transaction Logs Appearing

**Problem:** No "processing transactions" logs after 5 minutes

**Possible Causes:**
1. Geyser not configured with transaction notifier
2. Transaction filters too restrictive
3. Service not fully restarted

**Solutions:**
```bash
# Check Geyser config (on validator)
cat /path/to/geyser-plugin-config.json | grep -A5 transaction

# Restart with full service reload
sudo systemctl stop ironcrab
sleep 5
sudo systemctl start ironcrab

# Check for startup errors
sudo journalctl -u ironcrab --since "2 minutes ago" | grep -i error
```

### Transaction Logs But No Pool Detections

**Problem:** Seeing "processing transactions" but no "TRANSACTION DETECTED"

**Possible Causes:**
1. No pool creations happening during monitoring period
2. DEX program ID not in account_keys
3. Transaction filter excluding pool creations

**Solutions:**
```bash
# Wait longer - pool creations are intermittent (1-5 per minute)
# Check if validator is fully synced
solana slot --url http://127.0.0.1:8899

# Look for ANY transaction with Pump.fun program
sudo journalctl -u ironcrab -f | grep "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
```

### High CPU Usage

**Problem:** CPU usage >20% after restart

**Possible Causes:**
1. Too many transactions (no filters applied)
2. Memory leak in transaction processing
3. Broadcast channel overflow

**Solutions:**
```bash
# Check transaction volume
sudo journalctl -u ironcrab --since "5 minutes ago" | grep "processing transactions" | tail -1

# If >1000 txs/sec, filters aren't working - check logs for filter errors
sudo journalctl -u ironcrab | grep -i "filter\|subscribe"

# Restart if needed
sudo systemctl restart ironcrab
```

## Next Steps After Data Collection

Once you have 5-10 pool creation transactions with account keys:

1. **Analyze account indices** across all transactions
2. **Confirm consistent pattern** (e.g., token mint always at index 2)
3. **Update code** in `geyser_pool_discovery.rs`:
   ```rust
   let token_mint = tx_update.account_keys.get(2).copied()?;
   ```
4. **Remove `return None;`** to start emitting pool discovery events
5. **Deploy and test** with real pool creations
6. **Verify correct mints** appear in sniper logs

## Success Criteria

✅ Transaction logs appearing regularly
✅ Pool creation transactions detected and logged
✅ Account keys extracted and logged
✅ Token mint account index identified
✅ Code updated to extract mint from correct index
✅ Pool discovery events emitted with valid mints
✅ Sniper successfully buys new tokens (not `12xtdJLo...`)

---

**Current Status:** Ready for deployment and data collection phase.
**Next Action:** Deploy to server and capture transaction logs for analysis.
