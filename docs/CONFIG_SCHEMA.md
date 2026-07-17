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
curl -X POST http://localhost:8080/config \
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
| `max_open_positions` | u32 | 5 | > 0 | Maximum open positions at once (new token mints). Momentum scale-in BUYs (`metadata.entry_kind=scale_in`) with an existing LockManager balance on `output_mint` do not count against this limit. |
| `daily_loss_limit_lamports` | u64 | 5_000_000_000 | > 0 | Daily loss limit before auto-kill (5 SOL) |
| `max_slippage_bps` | u32 | 500 | 1-10000 | Maximum slippage in basis points (5%) |

### Operational (Send/Confirm)

| Key | Type | Default | Range | Description |
|-----|------|---------|-------|-------------|
| `simulation_timeout_ms` | u64 | 2000 | 100-30000 | Simulation timeout (ms) |
| `confirmation_timeout_ms` | u64 | 30000 | 500-300000 | Confirmation timeout after send (ms) |
| `confirm_commitment` | string | "confirmed" | finalized, confirmed | Commitment level for TX confirmation. Default `"confirmed"`: lower confirmation latency with **reorg risk** (slot can still reorganize). `"finalized"`: typical ~12–15s extra latency, stronger fork resistance. **market-data** Geyser `transactions_status` subscription uses this value at market-data startup (restart market-data to apply consistently). execution-engine waits on JetStream `WalletTxConfirmed` only (no RPC fallback). |
| `jetstream_tx_confirm_enabled` | bool | true | true/false | Wait for `WalletTxConfirmed` on JetStream after send (PR3). **Deprecated alias:** `geyser_confirm_enabled` (hot-reload still accepted). No RPC fallback on timeout (I-7). |
| `rebroadcast_interval_ms` | u64 | 2000 | 500-30000 | Interval between rebroadcasts of the same signed TX during confirm wait (ms) |
| `max_rebroadcasts` | u32 | 5 | 0-20 | Max rebroadcast attempts per TX during confirm wait |
| `rebroadcast_use_tpu` | bool | true* | true/false | Rebroadcast via TxSender/TPU when available; RPC fallback on failure. *Default follows `[execution_engine.tx_submission].tpu_enabled` when unset. |
| `send_enabled` | bool | false | true/false | If true, engine signs and submits transactions |

### Fee policy (`[execution_engine.fee_policy]` TOML / hot-reload via nested updates)

| Key | Type | Default | Range | Description |
|-----|------|---------|-------|-------------|
| `tier1_fee_percentile` | u8 | 50 | 25, 50, 75, 90 | Percentile base for Tier1 dynamic fee (execution-engine recomputes from NATS `PriorityFeePercentiles`; does not raise static floor) |
| `tier1_fee_multiplier` | f64 | 1.2 | > 0 | Multiplier applied to Tier1 percentile base; effective fee remains `max(dynamic, static_floor)` |

Prometheus (execution-engine `:9803/metrics`): `tx_send_to_confirm_ms`, `tx_confirmed_slot_delta_slots`, `tx_priority_fee_source_total{source}`, `tx_rebroadcast_total`, `tx_rebroadcast_method_total{method}`.

### Validation Rules
- `max_slippage_bps` must be between 1 and 10000 (0.01% to 100%)
- All `_lamports` values must be positive integers
- `send_enabled=true` is rejected if wallet keys are not configured
- `confirm_timeout_ms` must be between 500 and 300000
- `confirm_commitment` must be one of: finalized, confirmed
- `preflight_commitment` must be one of: processed, confirmed, finalized (or null)

---

## Momentum Bot

Component name: `momentum-bot`

### Liquidity Thresholds

| Key | Type | Default | Range | Description |
|-----|------|---------|-------|-------------|
| `early_min_liquidity_sol` | f64 | 3.0 | >= 0 | Minimum pool liquidity for EARLY regime trades |
| `established_min_liquidity_sol` | f64 | 10.0 | >= 0 | Minimum pool liquidity for ESTABLISHED regime trades |

### Regime Classification

| Key | Type | Default | Range | Description |
|-----|------|---------|-------|-------------|
| `early_slot_threshold` | u64 | 1000 | > 0 | Slots until pool transitions from EARLY to ESTABLISHED (~400s) |

### Slippage Settings

| Key | Type | Default | Range | Description |
|-----|------|---------|-------|-------------|
| `early_max_slippage_bps` | u32 | 500 | 1-10000 | Max slippage for EARLY trades (5%) |
| `established_max_slippage_bps` | u32 | 200 | 1-10000 | Max slippage for ESTABLISHED trades (2%) |

### Position Sizing

| Key | Type | Default | Range | Description |
|-----|------|---------|-------|-------------|
| `default_position_lamports` | u64 | 5_000_000 | > 0 | Default position size (0.005 SOL) |

### Momentum v2 Entry

| Key | Type | Default | Range | Description |
|-----|------|---------|-------|-------------|
| `probe_buy_pct` | f64 | 0.25 | 0.0-1.0 | Fraction of `default_position_lamports` used for probe buy |
| `scale_in_confirm_window_secs` | u64 | 30 | > 0 | Time window to confirm post-probe before scale-in |
| `scale_in_min_probe_executable_pnl_pct` | f64 | 0.0 | finite | **Scale-in only:** minimum executable probe PnL in percent (I-14 `pnl_pct` vs probe `entry_price` on `executable_exit_quote`). Scale-in is emitted only if `exec_pnl >` this value (strict inequality; default `0.0` requires strictly positive executable PnL). No blacklist on wait. |

### Buyer Quality (Concentration / Repeat Buyers)

| Key | Type | Default | Range | Description |
|-----|------|---------|-------|-------------|
| `top1_buyer_share_cap` | f64 | 0.35 | 0.0-1.0 | Reject if top buyer share exceeds cap |
| `top3_buyer_share_cap` | f64 | 0.60 | 0.0-1.0 | Reject if top3 buyers share exceeds cap |
| `repeat_buyer_min_ratio` | f64 | 0.05 | 0.0-1.0 | Minimum ratio of repeat buyers in window |

### Micro-buy Spam Filter (Trade Size Distribution)

| Key | Type | Default | Range | Description |
|-----|------|---------|-------|-------------|
| `min_trade_size_lamports` | u64 | 10_000_000 | > 0 | Trades below this SOL size are considered "small" |
| `small_buy_ratio_cap` | f64 | 0.85 | 0.0-1.0 | Reject if too many buys are small (spam) |

### Dump Recovery Gate

| Key | Type | Default | Range | Description |
|-----|------|---------|-------|-------------|
| `dump_recovery_window_secs` | u64 | 30 | > 0 | Window size for recovery stats |
| `dump_recovery_min_buy_dominance` | f64 | 0.55 | 0.0-1.0 | Minimum buy dominance to consider recovered |
| `dump_recovery_min_net_inflow_lamports` | u64 | 1_000_000_000 | >= 0 | Minimum net SOL inflow for recovery |
| `dump_recovery_min_recovery_secs` | u64 | 10 | > 0 | Minimum time after dump before allowing entry |

### Filter 5: Price Trend / Downtrend Gate (pre-entry)

Trade-implied `tokens_per_sol` from Geyser trades only (no RPC). Emits `WAIT_DOWNTREND` (soft wait).

| Key | Type | Default | Range | Description |
|-----|------|---------|-------|-------------|
| `price_trend_filter_enabled` | bool | true | true/false | Master switch |
| `price_trend_window_secs` | u64 | 120 | >= 0 | Chain-slot trend window (`0` = off) |
| `price_trend_min_trades` | u32 | 18 | >= 0 | Min trades with `slot > 0` in window |
| `price_trend_min_tps_rise_pct` | f64 | 8.0 | >= 0 | Min tps rise (%) for lower-highs / slope |
| `price_trend_max_drawdown_pct` | f64 | 25.0 | >= 0 | Drawdown from session trade-high (%) |
| `price_trend_recovery_min_buy_dominance` | f64 | 0.60 | 0.0-1.0 | Recovery exception: min buy dominance |
| `price_trend_recovery_min_inflow_lamports` | u64 | 500_000_000 | >= 0 | Recovery exception: min net inflow |
| `price_trend_bucket_count` | u32 | 5 | 3–8 | Sub-buckets for lower-highs + recovery slope |
| `price_trend_lower_highs_max_breaks` | u32 | 0 | >= 0 | Max monotonicity breaks in lower-highs chain |
| `price_trend_recovery_min_positive_buckets` | u32 | 2 | >= 1 | Min consecutive late buckets with falling median_tps |
| `price_trend_recovery_min_secs` | u64 | 15 | >= 0 | Min continuous recovery duration (chain slots) |
| `price_trend_recovery_requires_no_lower_highs` | bool | true | true/false | Disable recovery when strict lower-highs matches |

### Dev-Sell Re-Validation (pre-entry)

| Key | Type | Default | Range | Description |
|-----|------|---------|-------|-------------|
| `dev_sell_revalidation_delay_secs` | u64 | 30 | > 0 | Pause after any pre-entry dev sell; standard soft gates must pass on fresh window data after delay |

Deprecated (ignored with warn log): `cto_*`, `dev_early_sell_window_secs`, `dev_rebuy_positive`. Legacy `cto_entry_delay_secs` migrates to `dev_sell_revalidation_delay_secs` (max if both set).

### Mint Safety Gates

| Key | Type | Default | Range | Description |
|-----|------|---------|-------|-------------|
| `require_mint_authority_renounced` | bool | false | true/false | Require TokenMintInfo.mint_authority == None |
| `require_freeze_authority_none` | bool | false | true/false | Require TokenMintInfo.freeze_authority == None |

### Filter 1: Liquidity & Dev Supply

| Key | Type | Default | Range | Description |
|-----|------|---------|-------|-------------|
| `max_dev_supply_pct` | f64 | 95.0 | 0-100 | Max dev supply percentage before rejecting |
| `lp_removal_window_secs` | u64 | 60 | > 0 | Track LP removals for N seconds |
| `min_token_age_secs` | u64 | 60 | >= 0 | Min seconds since discovery before probe/scale-in; `0` disables (Filter 1c) |

### Filter 2: Buyer Velocity

| Key | Type | Default | Range | Description |
|-----|------|---------|-------|-------------|
| `min_unique_buyers` | u64 | 3 | > 0 | Minimum unique buyers in window |
| `buyer_window_secs` | u64 | 120 | > 0 | Time window for buyer tracking (seconds) |
| `min_trades_per_min` | f64 | 30.0 | >= 0 | Minimum trades per minute for momentum (chain-slot window). Deprecated: `min_trades_per_sec` is accepted for one release cycle and converted (×60) with a warn log — do not treat the numeric value as trades/min without conversion |
| `min_buy_dominance` | f64 | 0.45 | 0.0-1.0 | Minimum buy ratio (0.45 = 45%) |

### Filter 3: SOL Inflow

| Key | Type | Default | Range | Description |
|-----|------|---------|-------|-------------|
| `min_sol_inflow_lamports` | u64 | 500_000_000 | >= 0 | Minimum net SOL inflow (0.5 SOL) |
| `inflow_window_secs` | u64 | 60 | > 0 | Time window for SOL inflow tracking |
| `max_single_dump_lamports` | u64 | 20_000_000_000 | > 0 | Max allowed single sell (20 SOL) |


### Mint Safety Gates

| Key | Type | Default | Range | Description |
|-----|------|---------|-------|-------------|
| `hard_stop_min_hold_secs` | u64 | 45 | >= 0 | Grace period: normal hard stop suppressed (catastrophic only) |
| `catastrophic_stop_loss_pct` | f64 | 45.0 | > 0 | Hard stop threshold during grace (%) |
| `hard_stop_loss_pct` | f64 | 15.0 | > 0 | Hard stop-loss from entry after grace (15 = 15%) |
| `trailing_stop_pct` | f64 | 20.0 | > 0 | Trailing stop from ATH (20 = 20%) |
| `trailing_activation_pct` | f64 | 10.0 | >= 0 | Min profit to activate trailing (10 = 10%) |
| `take_profit_pct` | f64 | 100.0 | > 0 | Take profit target (100 = 2x) |
| `max_hold_time_secs` | u64 | 300 | > 0 | Min hold before TIME_EXIT is evaluated (5 min) |
| `max_hold_absolute_cap_secs` | u64 | 0 | >= 0 | Absolute max hold; TIME_EXIT regardless of velocity (0 = off) |
| `time_exit_requires_low_velocity` | bool | true | true/false | TIME_EXIT only when `trades_per_min` < `min_trades_per_min` |
| `momentum_exit_min_hold_secs` | u64 | 60 | >= 0 | Min hold before MOMENTUM_EXIT |
| `momentum_exit_only_when_losing` | bool | true | true/false | Momentum exit only when PnL <= `momentum_exit_max_pnl_pct` |
| `momentum_exit_max_pnl_pct` | f64 | 0.0 | — | PnL ceiling for momentum exit |
| `momentum_exit_buy_ratio` | f64 | 0.4 | 0.0-1.0 | Min buy ratio to stay in position |
| `momentum_exit_window_secs` | u64 | 30 | > 0 | Check last N seconds for momentum |
| `momentum_exit_min_trades` | u64 | 5 | > 0 | Min trades needed to evaluate exit |
| `bonding_curve_exit_pct` | f64 | 98.0 | 0.0-100.0 | Bonding curve exit threshold (%). Exit when PumpFun curve reaches this completion. 0 = disabled. Hot-reload: yes |

### Validation Rules
- Liquidity values must be >= 0 (0 means disabled)
- `early_slot_threshold` must be > 0
- Slippage BPS must be between 1 and 10000
- `default_position_lamports` must be > 0
- Percent-like values (`*_pct`, `*_cap`, `*_ratio`, `*_dominance`) must be in [0.0, 1.0]
- `*_window_secs`, `*_delay_secs`, `*_recovery_secs` must be > 0

---

## Arb Strategy

Component name: `arb-strategy`

### Arbitrage Parameters

| Key | Type | Default | Range | Description |
|-----|------|---------|-------|-------------|
| `min_spread_bps` | u32 | 50 | > 0 | Minimum spread between DEX prices (0.5%) |
| `min_profit_lamports` | u64 | 10_000_000 | > 0 | Min net profit after tx cost (0.01 SOL) |
| `max_position_lamports` | u64 | 1_000_000_000 | > 0 | Max notional per arb intent (1 SOL) |
| `est_tx_cost_lamports` | u64 | 50_000 | > 0 | Estimated tx cost for profit gating |
| `max_slippage_bps` | u32 | 100 | 1-10000 | Max slippage included in intent (1%) |
| `intent_cooldown_ms` | u64 | 5_000 | > 0 | Cooldown per mint/pair before next intent |
| `intent_ttl_ms` | u64 | 3_000 | > 0 | Intent time-to-live (ms) |

### Validation Rules
- `min_spread_bps` must be > 0
- `min_profit_lamports` must be > `est_tx_cost_lamports`
- Slippage BPS must be between 1 and 10000

---

## Market Data

Component name: `market-data`

### DEX Discovery

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enable_raydium` | bool | true | Enable Raydium AMM V4 pool discovery |
| `enable_raydium_cpmm` | bool | true | Enable Raydium CPMM (concentrated liquidity) discovery |
| `enable_orca` | bool | true | Enable Orca Whirlpool discovery |
| `enable_pumpfun` | bool | true | Enable PumpFun bonding curve discovery |
| `enable_pumpswap` | bool | true | Enable PumpSwap AMM (graduated tokens) discovery |
| `enable_meteora_dlmm` | bool | true | Enable Meteora DLMM (dynamic AMM) discovery |
| `enable_meteora_cpmm` | bool | true | Enable Meteora CPMM discovery |

### Rate Limiting

| Key | Type | Default | Range | Description |
|-----|------|---------|-------|-------------|
| `max_events_per_sec` | u32 | 10_000 | 1-1_000_000 | Maximum MarketEvents emitted per second |

### Geyser explicit accounts (PR-B)

Loaded from root TOML section `[market_data_geyser]` in `config.toml` (not the JetStream `market-data` DEX toggles).

| Key | Type | Default | Range | Description |
|-----|------|---------|-------|-------------|
| `max_tracked_accounts` | usize | 500000 | 1000-500000 | Max combined explicit Yellowstone accounts (mints + vaults + bin arrays + wallet list). LRU evicts unpinned rows when exceeded. |
| `geyser_full_reconnect_threshold` | usize | 10000 | 1000-500000 | When combined explicit accounts exceed this, `market-data` forces a full Geyser gRPC reconnect on subscription changes instead of many in-place subscribe updates. |

### Validation Rules
- Boolean values must be `true` or `false`
- `max_events_per_sec` must be between 1 and 1,000,000
- `max_tracked_accounts` and `geyser_full_reconnect_threshold` must be between 1000 and 500000 (when set via control-plane JSON; TOML should use same bounds)
- **Ops note (2026-07):** Prod default `500000` leaves ~300k headroom below historical self-hosted Yellowstone crash levels (~800k+ explicit accounts). Self-hosted Yellowstone has no `filters.account_max` plugin cap; admission is enforced in `market-data` only (I-MD-7).

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
curl -X POST http://localhost:8080/config \
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
curl -X POST http://localhost:8080/kill \
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
