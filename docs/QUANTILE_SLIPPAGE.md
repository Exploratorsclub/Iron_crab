# Quantile-Based Slippage Protection

> **⚠️ Status: Optional Feature – Currently DISABLED**
> 
> This is an **advanced optional feature** that is not enabled by default.
> Production config uses **Adaptive Slippage** (`adaptive_slippage_*` settings).
> To enable quantile-based slippage, set `quantile_slippage_enabled = true` in your config.

## Overview

The quantile-based slippage system replaces fixed slippage percentages with statistical learning from historical trade fills. Instead of applying a constant percentage (e.g., 1% slippage), the system:

1. **Learns** from actual fill shortfalls on a per-pool basis
2. **Calculates** statistical percentiles (P95, P99) of historical shortfalls
3. **Applies** confidence-level based min_out calculations
4. **Adapts** to each pool's unique characteristics

## Why Quantile-Based?

### Traditional Approaches

**Fixed Slippage (e.g., 1%)**:
- Simple but inflexible
- Same protection for all pools regardless of behavior
- Either too conservative (losing trades) or too risky (getting sandwiched)

**Adaptive Slippage (mean-based)**:
- Better than fixed, adjusts based on observed shortfalls
- Uses mean (average) shortfall
- Problem: Vulnerable to outliers and tail risks

### Quantile-Based Approach

**Statistical Learning**:
- Uses P95 (95th percentile) by default
- Means: "95% of historical fills had shortfall ≤ this value"
- Protects against tail risks while being data-driven
- Example: If historical shortfalls are [0%, 0.5%, 1%, 2%, 10%], P95 = 10%
  - Mean = 2.7% (pulls down by extremes)
  - P95 = 10% (captures worst-case scenarios)
  - With 20% safety buffer: min_out allows 12% shortfall

## Architecture

### Components

1. **`QuantileImpactCalculator`** (`src/quantile_impact.rs`)
   - Stores observations per pool in VecDeque (max 500 samples/pool)
   - Tracks shortfall percentages with timestamps
   - Cleans up stale observations (>24h by default)

2. **`FillObservation`**
   ```rust
   struct FillObservation {
       pool_id: String,           // Unique pool identifier
       expected_out: u64,         // Quoted output amount
       actual_out: u64,           // Actual received amount
       shortfall_pct: f64,        // (expected - actual) / expected
       timestamp_ms: i64,         // Unix timestamp
       size_category: SizeCategory, // Small/Medium/Large
   }
   ```

3. **Size Categories**
   - **Small**: <1% of pool liquidity
   - **Medium**: 1-5% of pool liquidity
   - **Large**: ≥5% of pool liquidity
   - Separate statistics per category (larger trades = higher shortfall)

### Integration Points

#### 1. Fill Recording (Transaction Reconciliation)
Location: `src/solana/sniper.rs` ~line 2350

```rust
// After each successful fill
self.quantile_calc.record_fill(
    pool_id,           // Format: "dex_inputMint_outputMint"
    expected_raw,      // From quote
    actual_raw,        // From on-chain balance delta
    size_category,     // Determined from trade size vs liquidity
);
```

#### 2. Min_Out Calculation
Location: `src/solana/sniper.rs` lines 581-603

```rust
fn compute_min_out(&self, pool_id: &str, expected_out: u64, 
                   amount_in: u64, pool_liquidity: u128) -> u64 {
    // Determine size category
    let size_category = if pool_liquidity > 0 {
        let trade_pct = (amount_in as f64 / pool_liquidity as f64) * 100.0;
        if trade_pct < 1.0 { Small }
        else if trade_pct < 5.0 { Medium }
        else { Large }
    } else { Small };
    
    // Try quantile if enabled & sufficient data
    if self.cfg.read().quantile_slippage_enabled.unwrap_or(false) {
        return self.quantile_calc.compute_min_out(
            pool_id, expected_out, amount_in, size_category
        );
    }
    
    // Fallback to adaptive slippage
    let slip = self.adaptive_slippage_bps() as u128;
    ((expected_out as u128) * (10_000 - slip) / 10_000) as u64
}
```

#### 3. Usage in Trade Execution
Replaces hardcoded calculations:

**Before**:
```rust
let slip = self.adaptive_slippage_bps() as u128;
min_out = ((quote.amount_out as u128) * (10_000 - slip) / 10_000) as u64;
```

**After**:
```rust
let pool_id = format!("orca_{}_{}", sol_mint, mint);
let pool_liquidity = 100_000_000_000u128; // estimate or from pool data
min_out = self.compute_min_out(&pool_id, quote.amount_out, lamports_in, pool_liquidity);
```

## Configuration

### Config File (`config.example.toml`)

```toml
[sniper]
# Enable quantile-based slippage (default: false)
quantile_slippage_enabled = true

# Confidence level: P95 = 0.95 (95th percentile)
# Higher = more conservative, lower = more aggressive
# Common values: 0.90 (P90), 0.95 (P95), 0.99 (P99)
quantile_confidence_level = 0.95

# Minimum samples before using quantile (default: 20)
# Below this, falls back to adaptive slippage
quantile_min_samples = 20

# Maximum age of samples in seconds (default: 86400 = 24h)
# Older observations are discarded
quantile_max_sample_age_secs = 86400

# Fallback slippage in basis points when insufficient data
# 100 bps = 1%
quantile_fallback_slippage_bps = 100
```

### Algorithm Details

#### Percentile Calculation
```rust
fn compute_percentile(sorted_samples: &[f64], confidence: f64) -> f64 {
    let n = sorted_samples.len();
    if n == 0 { return 0.0; }
    
    let index = (confidence * (n - 1) as f64) as usize;
    sorted_samples[index]
}
```

#### Min_Out Formula
```rust
// 1. Get valid observations for pool + size category
let observations = filter_by_pool_and_age_and_size(pool_id, size_category);

// 2. Check minimum sample threshold
if observations.len() < config.min_samples {
    return fallback_adaptive_slippage();
}

// 3. Extract shortfall percentages and sort
let mut shortfalls: Vec<f64> = observations.iter()
    .map(|obs| obs.shortfall_pct)
    .collect();
shortfalls.sort_by(|a, b| a.partial_cmp(b).unwrap());

// 4. Calculate P95 shortfall
let p95 = compute_percentile(&shortfalls, 0.95);

// 5. Apply 20% safety buffer
let adjusted_shortfall = (p95 * 1.2).min(0.5); // cap at 50%

// 6. Calculate min_out
let min_out = (expected_out as f64 * (1.0 - adjusted_shortfall)) as u64;
```

## Performance Characteristics

### Memory Usage
- **Per Pool**: VecDeque with max 500 observations
- **Per Observation**: ~88 bytes (pool_id, 2 u64s, f64, i64, enum)
- **Total**: ~44 KB per pool (500 * 88 bytes)
- **Typical Load**: 10-50 active pools = 440 KB - 2.2 MB

### Computational Cost
- **Recording**: O(1) insert + O(1) trim (if >500 samples)
- **Calculation**: O(n log n) sort per pool (n typically 20-500)
- **Lookup**: O(1) HashMap lookup
- **Cleanup**: O(pools * samples) periodic scan (runs every N fills)

### Concurrency
- **Read Operations**: Lockless via `Arc` cloning
- **Write Operations**: `RwLock` for observation updates
- **Contention**: Minimal (writes only on fill completion)

## Example Scenarios

### Scenario 1: New Pool (Cold Start)
```
Observations: 0 fills recorded
Behavior: Falls back to adaptive slippage (100 bps = 1%)
Reason: Not enough data (min 20 samples)
```

### Scenario 2: Stable Pool
```
Observations: 50 fills with shortfalls [0.1%, 0.2%, ..., 0.8%]
P95: 0.7%
Safety buffer: 0.7% * 1.2 = 0.84%
min_out: expected_out * (1 - 0.0084)
Behavior: 0.84% slippage (tighter than default 1%)
```

### Scenario 3: Volatile Pool
```
Observations: 30 fills with shortfalls [0%, 1%, 2%, 5%, 15%]
P95: 15%
Safety buffer: 15% * 1.2 = 18%
min_out: expected_out * (1 - 0.18)
Behavior: 18% slippage (much wider protection)
```

### Scenario 4: Large Trade in Thin Pool
```
Trade size: 10 SOL
Pool liquidity: 50 SOL
Size category: Large (20% of pool)
Observations for Large trades: 5 fills with shortfalls [3%, 5%, 8%]
Behavior: Falls back to adaptive (only 5 samples < 20 min)
```

## Monitoring & Debugging

### Metrics (Prometheus)
```prometheus
# Total fills recorded
quantile_fill_observations_total{pool_id="orca_SOL_USDC"}

# Current sample count per pool
quantile_samples_per_pool{pool_id="orca_SOL_USDC"}

# P95 shortfall per pool
quantile_p95_shortfall_pct{pool_id="orca_SOL_USDC"}

# Fallback usage (insufficient data)
quantile_fallback_total
```

### Logs
```rust
debug!(
    pool=%pool_id, 
    samples=%observations.len(), 
    p95=%p95_shortfall,
    min_out=%computed_min_out,
    "quantile: computed min_out"
);
```

### Export/Import
```rust
// Export observations to JSON (e.g., before shutdown)
let json = calculator.export_observations()?;
std::fs::write("quantile_observations.json", json)?;

// Import observations (e.g., on startup)
let json = std::fs::read_to_string("quantile_observations.json")?;
calculator.import_observations(&json)?;
```

## Testing

### Unit Tests (`src/quantile_impact.rs`)

```rust
#[test]
fn test_compute_percentile() {
    let samples = vec![1.0, 2.0, 3.0, 4.0, 5.0];
    assert_eq!(compute_percentile(&samples, 0.50), 3.0); // P50
    assert_eq!(compute_percentile(&samples, 0.95), 5.0); // P95
}

#[test]
fn test_fallback_behavior() {
    // With <20 samples, should fallback
    let calc = QuantileImpactCalculator::new(config);
    for i in 0..10 {
        calc.record_fill(pool_id, 1000, 990, Small);
    }
    let min_out = calc.compute_min_out(pool_id, 1000, 100, Small);
    // Should use fallback_slippage_bps (100) = 1%
    assert_eq!(min_out, 990);
}
```

## Migration Guide

### From Fixed Slippage
1. Set `quantile_slippage_enabled = true`
2. Keep existing `max_slippage_bps` as `quantile_fallback_slippage_bps`
3. Monitor first 24 hours (learning phase)
4. Check P95 values per pool in metrics
5. Adjust `quantile_confidence_level` if needed

### From Adaptive Slippage
1. Enable quantile alongside adaptive (quantile has priority)
2. Quantile falls back to adaptive when insufficient data
3. Compare performance over 1 week
4. Gradually increase confidence level if too many fills fail

## Known Limitations

1. **Cold Start**: Requires 20+ fills per pool before activation
2. **Liquidity Estimation**: Currently uses conservative default (100 SOL)
   - Future: Integrate with pool snapshot data
3. **Cross-Pool Learning**: Each pool learns independently
   - Future: Pool clustering for similar behaviors
4. **Active DEXes**: Sniper monitors **Pump.fun** (primary) and **Raydium** (secondary)
   - Orca is **disabled** in production config (only established pools, no new launches)
   - See `program_ids` in `my_config.server.toml`
5. **Memory Growth**: Unbounded pool count (mitigated by cleanup)
   - Future: LRU eviction for inactive pools

## Future Enhancements

1. **Pool Liquidity Integration**: Use actual pool reserves from snapshots
2. **Cluster-Based Learning**: Group similar pools (e.g., all small-cap meme tokens)
3. **Dynamic Confidence Levels**: Adjust P95 → P99 during high volatility
4. **Multi-Hop Quotes**: Apply quantile to each hop in multi-leg routes
5. **MEV Protection**: Integrate with Jito bundles for front-run resistant fills
6. **Real-Time Stats API**: Expose pool statistics via HTTP endpoint

## References

- **Implementation**: `src/quantile_impact.rs`
- **Integration**: `src/solana/sniper.rs` (`compute_min_out()`, `quantile_calc`)
- **Config**: `config.example.toml` (siehe `quantile_*` Optionen)
- **Tests**: `src/quantile_impact.rs` (#[cfg(test)])

## Support

For questions or issues:
1. Check metrics in Prometheus (port 9898)
2. Enable debug logging: `RUST_LOG=ironcrab::quantile_impact=debug`
3. Verify config: `quantile_slippage_enabled = true` in `my_config.server.toml`
4. Inspect exported observations: Call `export_observations()` and check JSON

---

**Status**: ✅ Implemented, ⚠️ **Disabled by default** (use Adaptive Slippage first)
**Version**: 1.0.0
