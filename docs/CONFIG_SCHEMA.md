# Hot-Reload Configuration Schema

This document defines all runtime-configurable parameters for each IronCrab binary.
All parameters can be updated via the Control Plane API without restarting services.

## Overview

Configuration updates are published to NATS topic `ironcrab.control.config.reload` and processed by each binary.

### Update Flow
```
Control Plane (POST /config)
    │
    ▼
NATS: ironcrab.control.config.reload
    │
    ├──► execution-engine (applies ExecutionConfig)
    ├──► momentum-bot (applies MomentumConfig)
    └──► market-data (applies MarketDataConfig)
```

### API Usage
```bash
# Update execution-engine config
curl -X POST http://localhost:8000/config \
  -H "Content-Type: application/json" \
  -H "X-API-Key: YOUR_ADMIN_KEY" \
  -d '{
    "component": "execution-engine",
    "config": {
      "max_slippage_bps": 500,
      "daily_loss_limit_lamports": 5000000000
    }
  }'
```

---

## Execution Engine

Component name: `execution-engine`

### Risk Limits

| Key | Type | Default | Range | Description |
|-----|------|---------|-------|-------------|
| `max_position_size_lamports` | u64 | 500_000_000 | > 0 | Maximum SOL per single position (0.5 SOL) |
| `max_concurrent_positions` | u32 | 5 | > 0 | Maximum open positions at once |
| `daily_loss_limit_lamports` | u64 | 5_000_000_000 | > 0 | Daily loss limit before auto-kill (5 SOL) |
| `max_slippage_bps` | u32 | 500 | 1-10000 | Maximum slippage in basis points (5%) |

### Fee Policy

| Key | Type | Default | Range | Description |
|-----|------|---------|-------|-------------|
| `max_compute_units` | u32 | 400_000 | > 0 | Maximum compute units per transaction |
| `max_priority_fee_lamports` | u64 | 10_000_000 | >= 0 | Maximum priority fee (0.01 SOL) |
| `max_total_fee_lamports` | u64 | 50_000_000 | >= 0 | Maximum total transaction cost (0.05 SOL) |
| `min_profit_after_fees_lamports` | u64 | 100_000 | >= 0 | Minimum profit to execute (0.0001 SOL) |

### Validation Rules
- `max_slippage_bps` must be between 1 and 10000 (0.01% to 100%)
- All `_lamports` values must be positive integers
- Fee limits cannot exceed position size limits

---

## Momentum Bot

Component name: `momentum-bot`

### Liquidity Thresholds

| Key | Type | Default | Range | Description |
|-----|------|---------|-------|-------------|
| `early_min_liquidity_sol` | f64 | 5.0 | >= 0 | Minimum pool liquidity for EARLY regime trades |
| `established_min_liquidity_sol` | f64 | 20.0 | >= 0 | Minimum pool liquidity for ESTABLISHED regime trades |

### Regime Classification

| Key | Type | Default | Range | Description |
|-----|------|---------|-------|-------------|
| `early_slot_threshold` | u64 | 1000 | > 0 | Slots until pool transitions from EARLY to ESTABLISHED (~400s) |

### Slippage Settings

| Key | Type | Default | Range | Description |
|-----|------|---------|-------|-------------|
| `early_max_slippage_bps` | u32 | 300 | 1-10000 | Max slippage for EARLY trades (3%) |
| `established_max_slippage_bps` | u32 | 100 | 1-10000 | Max slippage for ESTABLISHED trades (1%) |

### Position Sizing

| Key | Type | Default | Range | Description |
|-----|------|---------|-------|-------------|
| `default_position_lamports` | u64 | 100_000_000 | > 0 | Default position size (0.1 SOL) |

### Validation Rules
- Liquidity values must be >= 0 (0 means disabled)
- `early_slot_threshold` must be > 0
- Slippage BPS must be between 1 and 10000
- `default_position_lamports` must be > 0

---

## Market Data

Component name: `market-data`

### DEX Discovery

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enable_raydium` | bool | true | Enable Raydium AMM V4 pool discovery |
| `enable_orca` | bool | true | Enable Orca Whirlpool discovery |
| `enable_pumpfun` | bool | true | Enable PumpFun bonding curve discovery |

### Rate Limiting

| Key | Type | Default | Range | Description |
|-----|------|---------|-------|-------------|
| `max_events_per_sec` | u32 | 10_000 | 1-1_000_000 | Maximum MarketEvents emitted per second |

### Validation Rules
- Boolean values must be `true` or `false`
- `max_events_per_sec` must be between 1 and 1,000,000

---

## Response Format

All config updates return a `ConfigUpdateResponse`:

```json
{
  "status": "Applied",        // or "PartiallyApplied", "Rejected"
  "applied_keys": ["max_slippage_bps", "daily_loss_limit_lamports"],
  "rejected_keys": [
    ["unknown_key", "Unknown config key: unknown_key"],
    ["bad_value", "Must be > 0"]
  ],
  "new_snapshot_id": "snap-20251231-123456"
}
```

### Status Values
- `Applied`: All keys were successfully updated
- `PartiallyApplied`: Some keys were updated, others rejected
- `Rejected`: All keys were rejected (validation failed or unknown)

---

## Best Practices

### Gradual Changes
Make incremental config changes rather than large jumps:
```bash
# Good: Gradual slippage adjustment
curl ... -d '{"component": "momentum-bot", "config": {"early_max_slippage_bps": 350}}'
# Wait, observe
curl ... -d '{"component": "momentum-bot", "config": {"early_max_slippage_bps": 400}}'

# Bad: Large jump
curl ... -d '{"component": "momentum-bot", "config": {"early_max_slippage_bps": 800}}'
```

### Monitoring After Changes
After config updates, monitor:
1. Prometheus metrics for rejection rates
2. Decision records for new reject reasons
3. Grafana dashboards for PnL impact

### Emergency Rollback
If a config change causes issues:
```bash
# Reset to safe defaults
curl -X POST http://localhost:8000/config \
  -H "X-API-Key: $ADMIN_KEY" \
  -d '{
    "component": "execution-engine",
    "config": {
      "max_slippage_bps": 300,
      "daily_loss_limit_lamports": 1000000000
    }
  }'
```

Or use the kill switch:
```bash
curl -X POST http://localhost:8000/kill \
  -H "X-API-Key: $ADMIN_KEY" \
  -d '{"reason": "Bad config, rolling back"}'
```

---

## Appendix: ConfigUpdate Schema

```rust
// src/ipc/schema.rs
pub struct ConfigUpdate {
    pub component: String,           // "execution-engine", "momentum-bot", "market-data"
    pub config: HashMap<String, Value>,  // Key-value pairs
    pub source: String,              // "control-plane"
    pub timestamp: DateTime<Utc>,
}

pub struct ConfigUpdateResponse {
    pub status: ConfigUpdateStatus,
    pub applied_keys: Vec<String>,
    pub rejected_keys: Vec<(String, String)>,  // (key, reason)
    pub new_snapshot_id: Option<String>,
}

pub enum ConfigUpdateStatus {
    Applied,
    PartiallyApplied,
    Rejected,
}
```
