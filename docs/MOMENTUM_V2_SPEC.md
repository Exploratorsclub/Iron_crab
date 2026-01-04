# Momentum v2 Spec (Strategy Plane)

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

Each token (mint) is tracked with a single state.

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
  - We continue monitoring for disqualifying signals.

- **PositionOpen (Probe)**
  - Probe position is open.
  - We gather confirmation signals for Scale-In.

- **ScaleInPending**
  - We have emitted a scale-in BUY intent and are awaiting its execution result.

- **PositionOpen (Full)**
  - Full position is open.
  - Exit policy is active.

- **CTO_Candidate**
  - Special pre-entry state used when dev sells early *before* we enter.
  - We do not hard-reject; we wait for recovery confirmation.

- **ExitPending**
  - A SELL intent has been emitted; we are awaiting execution result.

- **Rejected**
  - Terminal state for this token tracker (within the tracker TTL).

### State Transitions (high level)
- Discovery → Validation
- Validation → ProbeBuyPending (when minimal gates pass)
- Validation → CTO_Candidate (when dev sells early pre-entry, CTO mode enabled)
- Validation → Rejected (when a hard reject condition occurs)
- CTO_Candidate → ProbeBuyPending (when CTO recovery confirms)
- ProbeBuyPending → PositionOpen (Probe) (on buy success)
- PositionOpen (Probe) → ScaleInPending (on confirmation pass)
- ScaleInPending → PositionOpen (Full) (on buy success)
- Any Open state → ExitPending (on exit signal)
- Any state → Rejected (on LP rug, etc., if configured as hard)

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

---

## 5) Required Signals & Data Sources

### Required MarketEvents
- `Trade { mint, trader, is_buy, sol_amount, token_amount, signature? }`
- `LiquidityRemoved { mint, sol_amount, token_amount, signature? }`
- `DevWalletIdentified { mint, dev_wallet, supply_percentage }` (pumpfun creator)
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

### Reject (pre-entry)
- `REJECT_LP_REMOVED`
- `REJECT_DEV_SUPPLY_TOO_HIGH`
- `REJECT_DEV_SELL_EARLY`
- `REJECT_MINT_AUTHORITY_NOT_RENOUNCED`
- `REJECT_FREEZE_AUTHORITY_SET`
- `REJECT_INSUFFICIENT_BUYERS`
- `REJECT_LOW_TRADE_VELOCITY`
- `REJECT_LOW_BUY_DOMINANCE`
- `REJECT_LOW_SOL_INFLOW`
- `REJECT_BOT_CONCENTRATION` (top buyer share too high)
- `REJECT_MICRO_BUY_SPAM` (small-buy ratio too high)

### CTO Mode
- `CTO_WAIT_RECOVERY`
- `CTO_RECOVERY_CONFIRMED`

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
