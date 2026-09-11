# AGENTS.md

## Cursor Cloud specific instructions

This project is a Rust-based Solana trading bot. Cloud Agents work on this repo in isolation (no sibling repos available).

**Shared branch (humans / PRs):** `architecture-rebuild`. Maintainer active development: `architecture-rebuild-next`. Onboarding: [CONTRIBUTING.md](CONTRIBUTING.md).

### Build & Test

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --quiet
cargo test --features test_helpers --quiet
```

### Required system dependencies

- Rust toolchain 1.89.0 (with clippy, rustfmt)
- protobuf-compiler, libprotobuf-dev, libssl-dev

---

## Mandatory Rules (STOP-CHECK)

**THESE RULES ARE BINDING. THEY ARE NOT SUGGESTIONS.**

Before modifying ANY file, run ALL checks below. If any check fails: **STOP IMMEDIATELY**, do NOT make the change, and report the violation.

### Check 1: Hot Path RPC (I-7) — CRITICAL

Does your change add an RPC call (fetch, get_account, get_multiple_accounts, etc.) or make an existing RPC call unconditional?

- **Check**: Is the changed function called from the Hot Path? Hot Path = everything reachable from `process_intent`, `build_tx_plan(allow_rpc_fallback=false)`, Momentum/Arb trading flow.
- **If yes**: STOP. Report: "STOP: Planned change would introduce RPC in Hot Path (I-7 violation). Function [X] is called by [Y] in Hot Path."
- **Exception**: RPC is allowed if it executes EXCLUSIVELY in the Cold Path (e.g. behind `allow_rpc_fallback == true` or `allow_rpc_on_miss == true`).

### Check 2: Existing Pattern (Consistency)

Do other modules (Raydium, Orca, Meteora, PumpSwap AMM) already have a pattern for the same concern (e.g. `allow_rpc_on_miss: bool`)?

- **If yes**: Your code MUST use the same pattern. Deviation = STOP and report.

### Check 3: Architecture Boundaries

- Changing Hot Path execution logic, RPC usage, or key-loading? Only with explicit approval in the handoff.
- Changing files outside the task scope? STOP and report.

### Check 4: Simulation-Gate (I-9)

Does your change send a TX without successful simulation? STOP.

### Check 5: Repo-Isolation (Level-5 Separation)

Do you read or reference files from `Iron_crab-eval/tests/` or `Iron_crab-eval/src/`?

- **If yes**: STOP IMMEDIATELY. You must NOT read eval tests. Reading eval tests violates Level-5 separation.
- **Allowed**: `Iron_crab-eval/docs/` (Spec, Plans) if referenced in the handoff.

**If all 5 checks pass: proceed with the change.**
**After the change: briefly document which checks you performed.**

---

## Core Rules

- Read `docs/INVARIANTS.md` (never violate) and `docs/KNOWN_BUG_PATTERNS.md` (check for similar patterns) before making changes.
- Propose a plan before coding.
- Do not change architecture without explicit approval.
- Prefer small, isolated changes.
- Single-Signer: only `execution-engine` loads keys and signs/sends.
- Intent-only: other processes are keyless and only emit `TradeIntent` or `MarketEvents`.
- Simulation-gated: if simulation fails, never send (especially arbitrage).
- Decision Records required for every decision (inputs, checks, outcome).
- Amounts must be explicit (units/decimals); no implicit conventions.
- Prefer Geyser over RPC/WS; use RPC/WS only as fallback in cold path.

## Hot Path vs. Cold Path (CRITICAL)

- **HOT PATH** (Discovery, Buy, Sell, Monitoring): GEYSER-ONLY. No blocking RPC calls. Latency target < 250ms.
- **COLD PATH** (Liquidation, Manual Actions, Bootstrap): RPC allowed. Safety over speed.
- NEVER remove RPC from Cold Paths — breaks safety.
- NEVER add RPC to Hot Paths without explicit approval — breaks latency.
- Functions called from BOTH paths: use `allow_rpc_fallback: bool` or `allow_rpc_on_miss: bool` parameter.
