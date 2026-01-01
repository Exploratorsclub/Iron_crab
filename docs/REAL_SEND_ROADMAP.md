# Real Send Roadmap (Execution Engine)

Date: 2026-01-02

This roadmap describes the concrete steps to turn the `execution-engine` into a **real on-chain sender** (mainnet-beta) that produces **real Solana signatures**, confirms transactions, and writes **forensic Decision Records** consistent with `docs/DEFINITION_OF_DONE.md` and `docs/TARGET_ARCHITECTURE.md`.

Context assumptions (per ops reality):
- The local Agave validator service is a **non-voting mainnet-beta RPC** (see `docs/agave-validator-optimized.service`): it connects to mainnet entrypoints and exposes RPC on `:8899`.
- This validator may not retain full transaction history for older lookups; the legacy monolith used **Helius** for *history* queries.
- **History is optional for sending.** A real send path must only rely on RPC endpoints that are present on the local validator (simulate/send/status/confirm). Helius can remain a secondary source for historical inspection or analytics.

---

## 0) Problem Statement (Current State)

Current `execution-engine` send path is an MVP stub:
- Simulation is stubbed (always succeeds).
- Send is stubbed (fake signature like `sig-mvp-*`).

This causes a hard mismatch:
- UI/metrics may show “executed/sent”,
- but Solscan cannot show anything because there is no real signature.

**Goal:** Replace stub behavior with a real implementation for one narrow vertical slice (MVP), then expand.

Implementation tracking:
- See `docs/REAL_SEND_IMPLEMENTATION_ISSUES.md` for a PR-sized issue checklist.

---

## 1) North Star: Pipeline + Artifacts (DoD-aligned)

Target pipeline per DoD C (P0):

`Intent -> Arbitration -> Plan -> Simulate -> Send -> Confirm -> Accounting`

For each intent we must produce:
- DecisionRecord with:
  - checks (pass/fail + reason codes)
  - simulate result (real RPC simulateTransaction summary)
  - send result (real signature or bundle id)
  - outcome in a correct terminal class: `Rejected | Expired | SimFailed | Sent | Confirmed | FailedConfirmed`
- ExecutionResults (optional in MVP, but needed for PnL attribution later).

---

## 2) MVP Scope (Minimize risk, maximize determinism)

**MVP = one DEX + one intent type + one happy-path.**

Recommended MVP slice:
- Intent: `TradeSide::Buy` only
- DEX: Pump.fun buy (because the strategy already assumes SOL -> token for new mints)
- Amount: `required_capital.raw` lamports (SOL)
- Slippage: `max_slippage_bps`
- Pool/account binding: use `TradeIntent.resources.pools[0]` (or bonding curve PDA derivation, depending on Pump.fun semantics)

Explicit non-goals for MVP:
- Sell intents
- Raydium/Orca
- Jito bundles / atomic execution
- Multi-hop routing

Acceptance criteria for MVP:
1. DecisionRecord contains a **real base58 Solana signature**.
2. Signature is visible on Solscan (mainnet-beta), and wallet shows the tx.
3. `simulateTransaction` runs and gates sending (simulation failure => no send).
4. Outcome transitions: at minimum `SimFailed` or `Confirmed` (not “Sent” only).

---

## 3) Phase Plan

### Phase A — Plumbing: unify RPC + signer + config

**Why:** execution-engine must use the same robust RPC stack as other on-chain code, and must keep single-signer rules.

Tasks:
- Use `crate::solana::rpc::SolanaRpc` (nonblocking client, retry/limiter) inside the execution-engine send path instead of `solana_client::rpc_client::RpcClient`.
- Centralize key loading in one place (preferred: `crate::wallet::Treasury::load_from_env()` or equivalent). Avoid duplicated key loaders.
- Add a clear config switch for send backend:
  - `send_enabled` (already exists)
  - `send_backend = "rpc" | "jito"` (future)
  - `confirm_mode = "rpc_status" | "geyser"` (future)

Acceptance criteria:
- execution-engine can fetch `getLatestBlockhash` and `getBalance` through `SolanaRpc`.
- Key loading happens exactly once and only in execution-engine.


### Phase B — Tx Plan: build instructions from TradeIntent (MVP: Pump.fun buy)

**Why:** Without a deterministic tx builder, execution is not real.

Tasks:
- Implement a small builder module (suggested path: `src/execution/tx_builder.rs` or `src/bin/execution_engine/tx_builder.rs`) that:
  - Validates intent is supported (MVP):
    - `origin_type == StrategyA`
    - `side == Buy`
    - `resources.input_mint` is SOL/WSOL mint
    - `resources.pools.len() == 1` (or pump.fun derivation is possible)
  - Derives/ensures required token accounts:
    - Owner = engine wallet pubkey
    - Destination ATA for output mint
    - WSOL ATA handling (wrap/unwrap) if needed
  - Creates instruction list:
    - compute budget ix (limit + price) per `FeePolicy`
    - Pump.fun swap instruction(s)
    - optional ATA create ix (if missing)

Notes:
- This should not use the legacy `Dex::build_swap_ix` API directly for Raydium yet.
- Pump.fun already has adapter code in `src/solana/dex/pumpfun.rs`; extend/consume it to build correct ixs for the engine wallet.

Acceptance criteria:
- Builder produces a deterministic `Vec<Instruction>` for the supported intent.
- All required accounts are derived from intent + wallet only (no hidden globals).


### Phase C — Simulation gate: real simulateTransaction

**Why:** DoD C P0 requires simulate-gated sending.

Tasks:
- Replace stub `simulate_transaction()` with a real implementation:
  - Build transaction from intent
  - Fill recent blockhash
  - Call `rpc.simulate_transaction` with appropriate config (commitment, sigVerify, replaceRecentBlockhash if needed)
  - Parse:
    - `err`
    - `logs` preview
    - `units_consumed`
- On failure, emit `DecisionOutcome::SimFailed` with a reason code (not free text only).

Acceptance criteria:
- Any simulation error blocks send.
- DecisionRecord’s `simulate` is real (not MVP text).


### Phase D — Send + Confirm: produce real signature and correct outcomes

**Why:** “Sent” is not enough for operator truth; confirmation is required.

Tasks:
- Send:
  - Use `rpc.send_transaction` (or `send_transaction_with_config`) with explicit options:
    - preflight commitment
    - `skip_preflight` should be a deliberate config, default false for MVP safety
  - Record returned signature (base58)
- Confirm:
  - Implement one confirmation method:
    - `getSignatureStatuses` polling with timeout
  - Map to DecisionOutcome:
    - confirmed => `Confirmed`
    - not confirmed by timeout => `Sent` or `FailedConfirmed` depending on status detail

Acceptance criteria:
- DecisionRecord includes real signature.
- Outcome reaches `Confirmed` in normal success.


### Phase E — Expand support (only after MVP works)

1) Sell intents
- Must handle destination SOL/WSOL correctly.

2) Raydium
- Existing `Raydium::build_swap_ix` uses placeholder user accounts; refactor to accept real user authority/source/destination.
- Option A: evolve the `Dex` trait to support a “planned swap” object (ixs + required accounts).
- Option B: keep trait, add an execution-layer adapter that patches placeholder accounts (riskier).

3) Orca
- Similar: require tx builder that supplies correct token program and ATA accounts.

4) Jito bundles (atomic arb)
- Only after single-tx RPC send works end-to-end.
- Bundle path must be simulate-gated and confirmed (or rejected with clear reason).

---

## 4) Observability / Operator UX Rules

To avoid future confusion:
- Do not label a trade “executed” unless we have:
  - a real signature (for RPC send), or
  - a real bundle id (for Jito).
- Prefer “confirmed” as the success KPI.

Recommended metrics additions (minimal):
- `tx_send_attempts_total`
- `tx_send_success_total`
- `tx_confirmed_total`
- `tx_confirm_timeout_total`

---

## 5) RPC History: how Helius fits (without breaking architecture)

Keep Helius usage out of the hot path:
- Sending + confirmation should use the local validator.
- For *history / rich inspection* (older tx lookups, parsed tx, token balances), add an optional “analytics/inspector” path:
  - either in control-plane
  - or in an offline ingestor

This matches `docs/TARGET_ARCHITECTURE.md` (DB/history not in hot path).

---

## 6) Concrete Code Hotspots (starting points)

- `src/bin/execution_engine.rs`
  - Replace the MVP stub simulate/send with real Plan/Sim/Send/Confirm steps.
- `src/solana/rpc.rs`
  - Use `SolanaRpc` for nonblocking calls and rate-limited retries.
- `src/solana/dex/pumpfun.rs`
  - Extend to build swap ix(s) for a provided user authority.
- `src/solana/dex/raydium.rs`
  - Refactor later: current `build_swap_ix` uses placeholder user accounts.

---

## 7) Safety Notes

- Only enable real sending on a dedicated wallet with strict limits.
- Keep `simulate` mandatory for MVP.
- Keep `max_position_size_lamports` low until confirmations are reliable.

