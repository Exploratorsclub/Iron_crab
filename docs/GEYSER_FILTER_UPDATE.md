# Geyser Filter Update: PumpFun & PumpSwap Support

## Problem Statement

**Bisherige Situation**:
- Geyser Filter hatte nur Raydium (675k...) und Orca (whir...)
- **70-80% aller PumpFun/PumpSwap PoolCreated Events wurden NICHT empfangen**
- Grund: Geyser filtert nach Program Owner, nicht nach Transaction-Typ
- Arbitrage-Modul bekam KEINE Daten für PumpFun/PumpSwap Cross-DEX Opportunities

**Impact**:
- momentum-bot verpasste ~70% der PumpFun Launches
- Trade-based Discovery musste als Fallback implementiert werden (c1f093b)
- arb-strategy hatte KEINE PumpFun/PumpSwap Pool-Daten für Cross-DEX Arbitrage

## Solution: Erweiterte Geyser Filter

### 1. Geyser Config Update

**File**: `docs/geyser-grpc-plugin-config.json`

**Neu hinzugefügt**:
```json
{
  "owner": "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"  // PumpFun Bonding Curve
},
{
  "owner": "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"  // PumpSwap AMM
}
```

**Vollständige Filter-Liste** (4 DEXes):
1. **Raydium AMM V4**: `675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8`
2. **Orca Whirlpool**: `whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc`
3. **PumpFun**: `6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P`
4. **PumpSwap (AMM)**: `pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA`

### 2. momentum-bot PumpSwap Support

**Changes in** `src/bin/momentum_bot.rs`:

#### A. DEX Pool Accounts Detection
```rust
fn dex_requires_pool_accounts(dex: &str) -> bool {
    dex.eq_ignore_ascii_case("PumpFunAmm")
        || dex.eq_ignore_ascii_case("pump_amm")
        || dex.eq_ignore_ascii_case("PumpSwap")
        || dex.eq_ignore_ascii_case("pumpswap")
        || dex.eq_ignore_ascii_case("pump-amm")  // NEW
}
```

#### B. Creator/Dev Wallet Requirement
Erweitert für Buy & Sell Intents:
```rust
if signal.dex == "pumpfun" 
    || signal.dex.eq_ignore_ascii_case("pump_amm")
    || signal.dex.eq_ignore_ascii_case("pumpswap")
    || signal.dex.eq_ignore_ascii_case("PumpFunAmm")
{
    // PumpSwap requires creator just like PumpFun
    let creator = creator_opt.ok_or_else(|| {
        anyhow::anyhow!("cannot generate {} intent: missing dev_wallet/creator", signal.dex)
    })?;
    intent.metadata.insert("creator".to_string(), creator);
}
```

#### C. Trade-Based Discovery Heuristic
```rust
let dex = if pool_address.starts_with("pump") || pool_address.starts_with("pAMM") {
    "pump_amm"  // PumpSwap AMM pools
} else {
    "pumpfun"   // Bonding Curve (default)
};
```

**Rationale**: PumpSwap Pool-Adressen haben oft erkennbare Prefixes, was eine bessere DEX-Zuordnung ermöglicht.

### 3. arb-strategy Cross-DEX Benefits

**Neue Arbitrage-Möglichkeiten**:
- **PumpFun ↔ Raydium**: Token auf Bonding Curve vs. AMM Pool
- **PumpSwap ↔ Raydium**: Graduated PumpFun tokens (nach Bonding Curve Completion)
- **PumpSwap ↔ Orca**: Cross-DEX für migrierte Tokens
- **PumpFun ↔ PumpSwap**: Arbitrage zwischen Bonding Curve und AMM Phase

**Expected Impact**:
- `pools_tracked` wird deutlich steigen (jetzt 4 DEXes statt 2)
- `multi_dex_tokens` Metrik zeigt Tokens auf mehreren DEXes
- `opportunities_found` sollte signifikant steigen bei hoher Volatilität

## Deployment Instructions

### Step 1: Server Vorbereitung

**Check Validator Status**:
```bash
ssh ironcrab-prod "sudo systemctl status solana-validator"
```

### Step 2: Geyser Config Update

**Copy new config to server**:
```bash
scp docs/geyser-grpc-plugin-config.json ironcrab-prod:/home/sol/geyser-grpc-plugin-config.json
```

**Verify config**:
```bash
ssh ironcrab-prod "cat /home/sol/geyser-grpc-plugin-config.json | jq '.accounts'"
```

Expected output: 4 account owners

### Step 3: Validator Restart

**CRITICAL**: Validator restart required for Geyser config changes!

```bash
ssh ironcrab-prod "sudo systemctl restart solana-validator"
```

**Monitor restart** (sollte ~30-60 sekunden dauern):
```bash
ssh ironcrab-prod "sudo journalctl -u solana-validator -f -n 100"
```

**Look for**:
- Geyser plugin loaded: `libPath: /home/sol/geyser-plugins/solana_geyser_plugin_grpc.so`
- Account filters: `accounts: [...]` (should show 4 owners)
- Catchup progress: `Slot: XXXXX`

### Step 4: Deploy Updated Binaries

**Build and deploy momentum-bot** (mit PumpSwap Support):
```bash
ssh ironcrab-prod "cd ~/Iron_crab && bash deploy_new.sh --component momentum-bot"
```

**Optional: Restart arb-strategy** (für clean state):
```bash
ssh ironcrab-prod "sudo systemctl restart arb-strategy"
```

### Step 5: Verification

**Check market-data receives PumpFun/PumpSwap events**:
```bash
ssh ironcrab-prod "sudo journalctl -u market-data -f | grep -E 'PumpFun|PumpSwap|pump_amm'"
```

Expected:
```
🆕 PumpFun CREATE detected
🆕 Pool created: dex=pump_amm
📊 Token tracker initialized: dex=pumpswap
```

**Check momentum-bot processes PumpSwap trades**:
```bash
ssh ironcrab-prod "sudo journalctl -u momentum-bot -f | grep -E 'pump_amm|pumpswap'"
```

Expected:
```
📊 Token tracker initialized (trade-based discovery) dex=pump_amm
🎯 ENTRY SIGNAL DETECTED dex=pumpswap
```

**Check arb-strategy multi-DEX tracking**:
```bash
ssh ironcrab-prod "curl -s http://localhost:9803/metrics | grep -E 'pools_tracked|multi_dex'"
```

Expected: Higher numbers after Geyser update

### Step 6: Monitor for 10 Minutes

**Critical Metrics** (via Grafana or curl):

1. **market-data**:
   - `events_published{type="PoolCreated"}` → Should include pump_amm/pumpswap
   - `pools_discovered{dex="pump_amm"}` → Should be > 0

2. **momentum-bot**:
   - `tokens_tracked_total` → Should increase faster
   - `filter_rejected{reason="WAIT_INSUFFICIENT_LIQUIDITY"}` → Watch for pump_amm

3. **arb-strategy**:
   - `pools_tracked_gauge` → Should be much higher (4 DEXes)
   - `arb_triangle_opportunities` → Watch for cross-DEX opps

## Rollback Plan

If issues arise:

### Option A: Revert Geyser Config Only
```bash
# Use old config (2 DEXes only)
ssh ironcrab-prod "cd ~/Iron_crab && git checkout HEAD~1 docs/geyser-grpc-plugin-config.json"
scp docs/geyser-grpc-plugin-config.json ironcrab-prod:/home/sol/geyser-grpc-plugin-config.json
ssh ironcrab-prod "sudo systemctl restart solana-validator"
```

### Option B: Revert Code Changes
```bash
git revert <commit-hash>
git push origin architecture-rebuild
ssh ironcrab-prod "cd ~/Iron_crab && bash deploy_new.sh --component momentum-bot"
```

## Performance Considerations

### NATS Load

**Increased Message Volume**:
- PumpFun PoolCreated: +30-50/hour (estimate)
- PumpSwap PoolCreated: +10-20/hour (graduated tokens)
- Trade Events: +200-500/minute (during active market)

**Monitor NATS**:
```bash
ssh ironcrab-prod "nats server report jetstream"
```

**Watch for**:
- Messages In/Out rate
- Consumer lag (should be <100ms)
- Pending messages (should stay near 0)

### Validator Resource Impact

**Expected Changes**:
- CPU: +2-5% (more account writes processed)
- Memory: Negligible (Geyser filter in plugin)
- Disk I/O: +5-10% (more events logged)

**Monitor**:
```bash
ssh ironcrab-prod "htop"
# Watch solana-validator process
# CPU should stay <80%
# Memory should stay <80%
```

### market-data Resource Impact

**Expected Changes**:
- Event processing rate: +50-100%
- JSONL write rate: +30-50%
- CPU: +10-20% (more dex_parser calls)

**Mitigation** (if needed):
- Increase `--event-buffer-size` in market-data args
- Monitor disk space: `df -h ~/Iron_crab/trade_logs/`

## Testing Recommendations

### Local Testing (Optional)

**Before server deployment**, test locally:

```bash
# 1. Update config
cp docs/geyser-grpc-plugin-config.json /path/to/local/validator/geyser-config.json

# 2. Restart local validator
solana-validator --geyser-plugin-config /path/to/geyser-config.json ...

# 3. Run market-data locally
cargo run --release --features nats --bin market-data -- --config config.toml

# 4. Watch for PumpSwap events
tail -f trade_logs/market_events/market_events-$(date +%Y%m%d).jsonl | grep pump_amm
```

### Production Staging (Recommended)

**Gradual Rollout**:
1. Deploy to test validator first (if available)
2. Monitor for 1 hour
3. Verify no crashes, no excessive CPU/memory
4. Then deploy to production

## Post-Deployment Checklist

- [ ] Validator restarted successfully
- [ ] Geyser plugin loaded (4 account filters visible)
- [ ] market-data receiving PumpFun/PumpSwap PoolCreated events
- [ ] momentum-bot initializing trackers for pump_amm DEX
- [ ] arb-strategy tracking pools across 4 DEXes
- [ ] No NATS consumer lag (check `nats sub ironcrab.v1.market_events`)
- [ ] Grafana dashboards showing increased activity
- [ ] DecisionRecords JSONL contains pump_amm/pumpswap entries

## Expected Outcomes

### Immediate (0-30 minutes):
- ✅ First PumpFun PoolCreated in market-data logs
- ✅ First PumpSwap PoolCreated in market-data logs
- ✅ momentum-bot tracking pump_amm tokens
- ✅ arb-strategy showing multi-DEX tokens

### Short-term (1-4 hours):
- ✅ tokens_tracked increases by 30-50%
- ✅ First trade-based discovery with dex=pump_amm
- ✅ arb-strategy finds first cross-DEX opportunity (e.g., PumpFun↔Raydium)
- ✅ No degradation in other metrics (Raydium/Orca still work)

### Long-term (24 hours+):
- ✅ Reduced trade-based discovery percentage (more PoolCreated events received)
- ✅ Cross-DEX arbitrage opportunities detected regularly
- ✅ Higher filter pass-through rate (more pools have complete data)

## Support & Troubleshooting

### Issue: No PumpFun Events After Restart

**Check**:
```bash
ssh ironcrab-prod "sudo journalctl -u solana-validator | grep -i geyser | grep -i 6EF8"
```

**Expected**: Account filter for `6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P` loaded

**Fix**: Verify config file path matches validator `--geyser-plugin-config` arg

### Issue: High NATS Consumer Lag

**Check**:
```bash
ssh ironcrab-prod "nats consumer ls ironcrab.v1.market_events"
```

**Fix**: Increase momentum-bot/arb-strategy instances or optimize processing

### Issue: arb-strategy Not Finding Opportunities

**Check**:
```bash
ssh ironcrab-prod "curl -s http://localhost:9803/metrics | grep pools_tracked"
# Should be 50+ after 30 minutes
```

**Verify pools have prices**:
```bash
ssh ironcrab-prod "sudo journalctl -u arb-strategy -n 100 | grep 'last_price'"
```

**Fix**: Need more Trade events to establish prices

## References

- Geyser Plugin Docs: https://docs.solana.com/developing/plugins/geyser-plugins
- PumpFun Program: https://solscan.io/account/6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P
- PumpSwap AMM: https://solscan.io/account/pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA
- Trade-Based Discovery Feature: Commit c1f093b
- Target Architecture: docs/TARGET_ARCHITECTURE.md
