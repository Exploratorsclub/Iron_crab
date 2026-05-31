# Momentum v2 Spec (Strategy Plane)

## Implementation Status: ✅ Complete

**Last Verified**: Januar 2025

| Feature | Status | Notes |
|---------|--------|-------|
| Two-Phase Entry (Probe + Scale-In) | ✅ | `probe_buy_pct`, `scale_in_confirm_window_secs` |
| Explicit State Machine | ✅ | `TrackerState` enum with 7 states |
| Reason Codes | ✅ | `REJECT_*`, `WAIT_*`, `EXIT_*` |
| fill_in/fill_out Position Accounting | ✅ | From `ExecutionResult` |
| DexPoolAccounts (14 accounts) | ✅ | PumpSwap deterministic routing |
| TokenMintInfo Gates | ✅ | `mint_authority`, `freeze_authority` |
| Dev-Sell Re-Validation | ✅ | `dev_sell_revalidation_delay_secs`, `WAIT_DEV_SELL_REVALIDATION` |
| Buyer Quality / Anti-Bot | ✅ | `top1_buyer_share_cap`, `small_buy_ratio_cap` |
| Exit Policies | ✅ | `hard_stop`, `trailing_stop`, `take_profit`, `max_hold_time` |
| Dev Sell Detection | ✅ | Pre-entry WAIT + revalidation, post-entry exit |
| LP Removal Detection | ✅ | `REJECT_LP_REMOVED` |
| PendingIntent Tracking | ✅ | Correlation via `intent_id` |

---

Scope: This document defines the *state machine*, *decision outcomes*, and *reason-coded* rejects/exits for Momentum v2.

Constraints (from repo architecture):
- `momentum-bot` is **keyless** and emits **TradeIntents only**.
- `execution-engine` is the **single signer** and enforces simulate-gated send.
- All decisions must be **forensically explainable** (reason-codes, inputs, thresholds).

---

## 1) Core Concept

Momentum v2 is a two-phase entry:
1) **Probe-Buy**: a small position opened as soon as minimal safety + momentum gates pass.
2) **Scale-In**: a second buy to reach full size only if post-probe confirmation holds.

The design goal is: *as early as possible* while filtering common scam/rug patterns via stateful, reason-coded gates.

---

## 2) State Machine

Each token (mint) is tracked with an explicit `TrackerState` enum.

### Implementation: `TrackerState` Enum

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TrackerState {
    /// Initial state: Newly discovered token, collecting first trades.
    Discovery,
    /// Passed basic filters, waiting for velocity/quality thresholds.
    Validation,
    /// Probe buy intent sent, awaiting execution result.
    ProbeBuyPending { sent_at: Instant },
    /// Probe buy confirmed, position open with probe amount only.
    PositionOpenProbe { filled_at: Instant },
    /// Scale-in intent sent, awaiting execution result.
    ScaleInPending { sent_at: Instant },
    /// Full position open (probe + scale-in complete).
    PositionOpenFull { filled_at: Instant },
    /// Terminal state: Token rejected (filter fail, execution fail, timeout, etc.)
    Rejected,
}
```

### States
- **Discovery**
  - Token is first observed (pool created or first trade observed).
  - We allocate internal tracker state and begin collecting events.

- **Validation**
  - Pre-entry gating phase.
  - We evaluate safety + momentum + anti-bot heuristics.
  - Output is either: reject, wait, or proceed to Probe-Buy.

- **ProbeBuyPending**
  - We have emitted a probe BUY intent and are awaiting its execution result.
  - Carries `sent_at` timestamp for timeout tracking.

- **PositionOpenProbe**
  - Probe position is open.
  - Carries `filled_at` timestamp for scale-in window calculation.
  - We gather confirmation signals for Scale-In.

- **ScaleInPending**
  - We have emitted a scale-in BUY intent and are awaiting its execution result.
  - Carries `sent_at` timestamp for timeout tracking.

- **PositionOpenFull**
  - Full position is open (probe + scale-in complete).
  - Carries `filled_at` timestamp for max hold time calculation.
  - Exit policy is active.

- **Rejected**
  - Terminal state for this token tracker (within the tracker TTL).
  - Accompanied by `blacklist_reason` field explaining why.

### State Transitions

```text
Discovery → Validation → ProbeBuyPending → PositionOpenProbe
                      ↘                         ↓
                       Rejected          ScaleInPending → PositionOpenFull
                                               ↓
                                           Rejected
```

- Discovery → Validation (on first signal evaluation)
- Validation → ProbeBuyPending (when minimal gates pass)
- Validation → Rejected (when a hard reject condition occurs)
- ProbeBuyPending → PositionOpenProbe (on buy success)
- ProbeBuyPending → Rejected (on execution failure/timeout)
- PositionOpenProbe → ScaleInPending (on confirmation pass within window)
- PositionOpenProbe → PositionOpenFull (when window expires - probe only)
- ScaleInPending → PositionOpenFull (on buy success)
- ScaleInPending → Rejected (on execution failure/timeout)

**Note**: Dev-sell revalidation delay and Dump-Recovery are handled as **sub-states within Validation** via additional fields (`dev_sell_observed_at`, `dump_observed_at`, `recovery_started_slot`), not as separate `TrackerState` variants.

---

## 3) Decision Outcomes (Strategy Plane)

Momentum-bot decisions must map to one of these outcomes; each outcome must carry a primary reason-code.

### Outcomes
- **TRACK**: create/update tracker; no trading action.
- **WAIT**: keep tracking; not enough evidence yet.
- **REJECT**: permanently stop tracking (for TTL window).
- **EMIT_INTENT_PROBE_BUY**: publish a probe BUY `TradeIntent`.
- **EMIT_INTENT_SCALE_IN**: publish a scale-in BUY `TradeIntent`.
- **EMIT_INTENT_EXIT_SELL**: publish a SELL `TradeIntent`.

### Deterministic intent metadata
- Must include routing hints like `dex`, `pool_address`, and any required extra fields (e.g., pumpfun `creator`).
- Must include *reason* + *reason_code* in intent metadata for later DecisionRecord correlation.

---

## 4) Pre-entry vs Post-entry Rules

### Pre-entry (no position open)
**Primary objective:** reject obvious rugs/bots while staying early.

Hard gates (always reject):
- LP removed / liquidity pulled.
- Invalid or missing required DEX pool accounts (non-deterministic build risk).

Configurable safety gates (typical default: ON for real money):
- Mint safety (requires `TokenMintInfo`):
  - `mint_authority == None` if `require_mint_authority_renounced=true`.
  - `freeze_authority == None` if `require_freeze_authority_none=true`.

Soft gates (WAIT until sufficient data):
- Not enough unique buyers in the early window.
- Trade velocity / buy dominance below thresholds.
- Net SOL inflow below threshold.

CTO behavior (pre-entry only):
- If dev sells early and we do **not** hold a position:
  - If CTO mode enabled: transition to **CTO_Candidate** instead of hard reject.
  - If CTO mode disabled: treat as reject.

### Post-entry (position open)
**Primary objective:** protect capital.

Hard exit (immediate exit intent):
- Dev sells (post-entry) → **EXIT_DEV_SELL**.
- LP removed → **EXIT_LP_REMOVAL**.
- Hard stop-loss reached.

Conditional exits:
- Trailing stop after activation threshold.
- Momentum fade (buy ratio below threshold in recent window).

**Quote-first price exits (STOP_LOSS / TAKE_PROFIT):** these fire only when a usable **executable reserve quote** breaches the configured threshold. **Current-price-only** triggers for those exits are disabled; the trade-ratio / reserve mark may diverge for logging but is not the trigger source. **TRAILING_STOP** uses two layers: **activation** follows peak PnL implied by the **trade-session high** on the position pool (entry fill + post-entry trade-derived `tokens_per_sol` on that pool, slot-gated), not PoolCache reserve marks and not the executable quote; **trigger** drawdown compares that session high to the last **trade mark** on the position pool (trade prints + entry/scale-in fills), not the executable quote — PoolCache-only marks must not trip trailing. Scale-in overwrites the trailing baseline (session high, trade mark, `trailing_active`, confirm slot) to the scale-in fill. An alternate-pool quote may still inform STOP/TAKE per routing policy, but must not drive trailing vs the position session high (I-13).


## 5) Required Signals & Data Sources

### Required MarketEvents
- `Trade { mint, trader, is_buy, sol_amount, token_amount, signature? }`
- `LiquidityRemoved { mint, sol_amount, token_amount, signature? }`
- `DevWalletIdentified { mint, dev_wallet, supply_percentage }` (used as pump.fun `metadata.creator` when trading bonding-curve pumpfun)
- `TokenMintInfo { mint, token_program, decimals, supply, mint_authority, freeze_authority }`

### Data-source constraints
- Prefer **Geyser-first**.
- Avoid TX-history dependence for core gates.
- LP lock is a soft heuristic and can be spoofed; do not treat it as a sole hard gate.

---

## 6) Reason Codes (Canonical)

Reason codes are uppercase snake case. A decision must include exactly one **primary** reason code; additional context may be added in details/metadata.

### Tracking / Wait
- `WAIT_INSUFFICIENT_LIQUIDITY`
- `WAIT_MINT_INFO`
- `WAIT_BUYER_WINDOW`
- `WAIT_CONFIRMATION`
- `WAIT_DEV_SELL_REVALIDATION`

### Reject (pre-entry)
- `REJECT_LP_REMOVED`
- `REJECT_DEV_SUPPLY_TOO_HIGH`
- `REJECT_MINT_AUTHORITY_NOT_RENOUNCED`
- `REJECT_FREEZE_AUTHORITY_SET`
- `REJECT_INSUFFICIENT_BUYERS`
- `REJECT_LOW_TRADE_VELOCITY`
- `REJECT_LOW_BUY_DOMINANCE`
- `REJECT_LOW_SOL_INFLOW`
- `REJECT_BOT_CONCENTRATION` (top buyer share too high)
- `REJECT_MICRO_BUY_SPAM` (small-buy ratio too high)
- `REJECT_UNSUPPORTED_TOKEN_PROGRAM` (token program not supported by current execution path)

### Intents
- `ENTER_PROBE_BUY`
- `ENTER_SCALE_IN`

### Exit
- `EXIT_DEV_SELL`
- `EXIT_LP_REMOVAL`
- `EXIT_HARD_STOP`
- `EXIT_TAKE_PROFIT`
- `EXIT_TRAILING_STOP`
- `EXIT_MOMENTUM_FADE`
- `EXIT_MAX_HOLD_TIME`

---

## 7) Implementation Notes (Non-normative)

- Keep the strategy logic side-effect-free except for emitting intents and writing JSONL.
- Every time an intent is emitted, store a local `PendingIntent` so execution results can be correlated.
- The strategy should prefer returning WAIT (with reason) rather than rejecting when missing data is expected to arrive soon (e.g., `TokenMintInfo`).

---

## 8) End-to-End IPC Contracts (Normative)

This section is the integration “source of truth” across:
- Upstream: `market-data` → `MarketEvent`
- Strategy plane: `momentum-bot` → `TradeIntent`
- Downstream: `execution-engine` → `DecisionRecord` + `ExecutionResult`

If anything in this spec conflicts with code contracts in `src/ipc/schema.rs`, this spec must be updated to match.

### Topics
- `ironcrab.v1.market_events`
- `ironcrab.v1.trade_intents`
- `ironcrab.v1.decision_records`
- `ironcrab.v1.execution_results`

Note: hot-reload config currently uses the legacy topic `ironcrab.control.config.reload`.

### Correlation keys
- `run_id` scopes a process run.
- `intent_id` is the primary correlation key across intents/decisions/execution.
- `decision_id` connects a `DecisionRecord` to a later `ExecutionResult`.

---

## 9) Upstream Requirements: `market-data` (Normative)

Momentum v2 relies on `market-data` producing deterministic, replayable events.

### Required MarketEvents

#### 9.1 `TokenMintInfo` (mint safety + decimals)
- Must be produced from Geyser account updates for tracked mints.
- Strategy behavior:
  - If mint is not yet tracked / mint info not yet observed: `WAIT_MINT_INFO`.
  - Once observed, strategy must treat the values as authoritative for:
    - `decimals` (position accounting, `ExplicitAmount` units)
    - mint/freeze authority gates.
    - token program gating: if `token_program` is not supported by the intended DEX execution path, strategy must `REJECT_UNSUPPORTED_TOKEN_PROGRAM`.

#### 9.2 `DexPoolAccounts` (deterministic PumpSwap/Pump AMM execution)
- For `dex="PumpFunAmm"`, `market-data` must emit:
  - `pool_address` (this is the pool id to be used in intents)
  - `base_mint`, `quote_mint`
  - `accounts` as the *v1 ordered list* (length must be exactly 14):
    - `[0] pool_market`
    - `[1] global_config`
    - `[2] base_mint`
    - `[3] quote_mint`
    - `[4] pool_base_vault`
    - `[5] pool_quote_vault`
    - `[6] protocol_fee_recipient`
    - `[7] protocol_fee_recipient_ta`
    - `[8] event_authority`
    - `[9] coin_creator_vault_ata`
    - `[10] coin_creator_vault_authority`
    - `[11] global_volume_accumulator`
    - `[12] fee_config`
    - `[13] fee_program`

Strategy behavior:
- If strategy wants to trade PumpFunAmm and does not have `DexPoolAccounts` yet: `WAIT_BUYER_WINDOW` (or a more specific WAIT) rather than emitting an intent that will be rejected for non-determinism.

#### 9.3 `Trade` + `LiquidityRemoved` (core signals)
- These events power the buyer-quality, micro-buy, dump-recovery, dev-sell, and LP removal gates.

---

## 10) Strategy → Engine Contract: `TradeIntent` (Normative)

### 10.1 Required invariant fields
- `source` MUST be a stable producer identity (for Momentum v2: `"momentum-bot"`).
- `intent_id` MUST be globally unique.
- `required_capital` is the **amount-in** (see `src/ipc/schema.rs`).
- `resources.input_mint` / `resources.output_mint` MUST match the trade side semantics:
  - BUY: input = SOL, output = token
  - SELL: input = token, output = SOL

### 10.2 Deterministic routing resources (DEX-specific)

#### Pump.fun bonding curve ("pumpfun")
Goal: allow `execution-engine` to plan deterministically.

- `resources.pools` MUST contain exactly one pool id:
  - `resources.pools[0] = bonding_curve_pubkey`
- `metadata` MUST include:
  - `creator = <creator_pubkey>` (required for pump.fun tx build; engine rejects if missing)

Source for `creator`:
- Momentum-bot should populate `metadata.creator` from the latest `MarketEventKind::DevWalletIdentified.dev_wallet` for that mint.
- `metadata` SHOULD include:
  - `dex = "pumpfun"`

#### PumpSwap / Pump AMM ("pump_amm" / `PumpFunAmm`)
Goal: **no RPC account discovery** during tx build.

Current constraints (implementation-derived):
- Only WSOL pairs are supported (SOL ↔ token). Token ↔ token is unsupported.
- Current tx build path assumes SPL Token (Tokenkeg) for token transfers/ATAs.
  - If `TokenMintInfo.token_program` indicates Token-2022, strategy must reject until engine supports Token-2022 for this DEX path.

- `resources.pools` MUST contain exactly one pool id:
  - `resources.pools[0] = pool_address`
- `resources.accounts` MUST contain exactly 14 pubkeys and MUST be the v1 ordered list from `MarketEventKind::DexPoolAccounts.accounts`.
- `resources.accounts[0]` MUST equal `resources.pools[0]` (pool id must match).

If any of the above is violated, the intent is expected to be rejected by the engine.

#### Raydium / Orca
- `resources.pools` should include the pool id.
- `resources.accounts` may be omitted; engine can fetch if needed.

### 10.3 Execution constraints (min_out)

To be simulate-gated and deterministic, momentum intents MUST set `execution.min_out`:
- BUY (SOL → token): `min_out` is token raw units with token decimals.
- SELL (token → SOL): `min_out` is lamports (decimals=9).

Decimals rule (non-negotiable):
- Token decimals MUST come from `MarketEventKind::TokenMintInfo.decimals` (Geyser-derived).
- The strategy MUST NOT hard-code token decimals (e.g. `6`) for position sizing, `required_capital`, or `execution.min_out`.

`max_slippage_bps` is the strategy’s tolerance; engine may enforce a stricter policy.

### 10.4 Canonical reason metadata

Momentum must emit:
- `metadata["reason_code"] = <canonical_code>`
- `metadata["reason_detail"] = <freeform detail>`

Optional but recommended:
- `metadata["entry_kind"] = "probe"|"scale_in"` for BUY intents
- `metadata["exit_type"] = <string>` for SELL intents
- `metadata["dex"] = "pumpfun"|"pump_amm"|"raydium"|"orca"` when known

---

## 11) Engine → Strategy Contract (Normative)

### 11.1 `DecisionRecord` (audit trail)

Momentum-bot should treat DecisionRecords as the forensics source for:
- why an intent was rejected (incl. simulate failures)
- whether send is disabled (`send_enabled=false`)

### 11.2 `ExecutionResult` (position state transitions)

Momentum-bot must only transition state based on `ExecutionResult.status`:
- `Confirmed`: trade is considered executed.
- `Failed`/`Timeout`: trade is considered not executed (unless an eventual confirm arrives later).

#### The real missing piece: fill accounting

For Momentum v2 to be *complete and correct* (position sizing, exits, PnL), strategy must know the actual fill amounts.

Status: implemented via `ExecutionResult` optional fill fields.

`ExecutionResult` MAY include:
- `fill_in: ExplicitAmount` (actual amount-in)
- `fill_out: ExplicitAmount` (actual amount-out)

Additionally, `ExecutionResult` MAY include explicit diagnostics:
- `fill_status: FillStatus` (`Complete` | `Partial` | `Unavailable`)
- `fill_unavailable_reason: FillUnavailableReason` (set when fills are unavailable)

Strategy requirements:
- On `ExecutionStatus::Confirmed`, the strategy MUST update position sizing only from confirmed fills.
- For BUY confirms, the strategy MUST treat missing `fill_out` as a correctness blocker (do not create/scale a position from placeholders).
- The strategy SHOULD log/emit the diagnostics (`fill_status`, `fill_unavailable_reason`) for forensics.

Note: A separate companion record type (e.g. `ExecutionFill`) is not required at the moment.

---

## 12) Position Accounting & Exit Sizing (Normative)

### 12.1 Position model
Momentum-bot must maintain a per-mint position record that includes:
- `entry_intent_id` (probe) and optional `scale_in_intent_id`
- `token_decimals` (from `TokenMintInfo`)
- `token_amount_raw` (from confirmed fills)
- `sol_spent_lamports` and `sol_received_lamports` (from confirmed fills)

### 12.2 Exit sell amount
- SELL intents must use `required_capital.raw = token_amount_to_sell_raw`.
- Default: sell 100% of `token_amount_raw` for hard exits.
- If partial exits are added later, they must be expressed as deterministic raw token amounts (never “percent only”).

### 12.3 Pending intent handling
- A token may have at most one pending entry intent (probe or scale-in) and at most one pending exit intent.
- If an entry intent `Timeout`s, strategy must not assume it executed.
- If an exit intent `Timeout`s, strategy must keep monitoring and may re-issue with a new intent id after a cooldown (cooldown policy is config-driven).

---

## 13) Runtime Config Keys (Normative inventory)

The following keys must be supported for Momentum v2 tuning via the config-reload mechanism:

Entry & regime:
- `early_min_liquidity_sol`
- `established_min_liquidity_sol`
- `early_slot_threshold`
- `early_max_slippage_bps`
- `established_max_slippage_bps`
- `default_position_lamports`
- `probe_buy_pct`
- `scale_in_confirm_window_secs`

Buyer quality / anti-bot:
- `top1_buyer_share_cap`
- `top3_buyer_share_cap`
- `repeat_buyer_min_ratio`
- `min_trade_size_lamports`
- `small_buy_ratio_cap`

Dump recovery:
- `dump_recovery_window_secs`
- `dump_recovery_min_buy_dominance`
- `dump_recovery_min_net_inflow_lamports`
- `dump_recovery_min_recovery_secs`

Dev-sell revalidation (pre-entry):
- `dev_sell_revalidation_delay_secs`

Mint safety:
- `require_mint_authority_renounced`
- `require_freeze_authority_none`

Note: [docs/CONFIG_SCHEMA.md](docs/CONFIG_SCHEMA.md) should be updated to include the full Momentum v2 key inventory.

---

## 14) Definition of “Momentum Works Completely” (Acceptance Criteria)

Momentum v2 is considered complete when the following end-to-end properties hold:

1) Deterministic execution
- For PumpFunAmm, intents include `resources.pools[0]` and the 14 pool accounts from `DexPoolAccounts`.
- Engine can build without ad-hoc RPC discovery for PumpFunAmm.

2) Simulate-gated safety
- Engine produces a `DecisionRecord` for each processed intent with checks + outcome.
- If simulation fails, the intent is never sent.

3) Correct position accounting
- On `ExecutionResult::Confirmed`, momentum-bot updates position using actual fills via `ExecutionResult.fill_in`/`fill_out`.
- Exits sell the actual held token amount.

4) Forensic explainability
- Every emitted intent includes `metadata.reason_code` and `metadata.reason_detail`.
- Strategy state transitions can be reconstructed from JSONL + NATS streams by `intent_id`.
