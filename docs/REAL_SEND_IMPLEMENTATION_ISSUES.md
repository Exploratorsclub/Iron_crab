# Real Send – Tracked Implementation Issues

Date: 2026-01-02

This is a **tracked**, PR-sized issue list to implement Real Send in the `execution-engine`, aligned with:
- `docs/DEFINITION_OF_DONE.md`
- `docs/TARGET_ARCHITECTURE.md`
- `docs/REAL_SEND_ROADMAP.md`

Conventions:
- Each item is sized to land as a single PR.
- Each item has explicit acceptance criteria.
- Keep scope minimal (MVP vertical slice first).

Build/test environment note:
- Per `docs/LOCAL_SETUP.md`, Windows-native builds are currently not supported because the Geyser dependency chain (`yellowstone-grpc-proto` → `protobuf-src`) fails on Windows.
- For step-by-step implementation and verification, run builds/tests in **WSL2 (Ubuntu)** or on the Linux server/CI.

Status snapshot (2026-01-02):
- Done: execution-engine no longer emits fake signature-like values; send path is honest (rejects instead of claiming sent).
- Done (SELL safety, dry-run compatible): SELL token balance preflight + token capital locking (validated via DecisionRecords).
- Not done: Real simulation via RPC (`simulateTransaction`) and Real Send/Confirm.

---

## RS-0 — Baseline / Guardrails

- [ ] **RS-0.0 – Ensure a working Linux build loop (WSL2 recommended)**
  - Scope: make sure the developer workflow can compile + run tests reliably.
  - Acceptance:
    - `cargo test --test ipc_schema_roundtrip` runs successfully in WSL2.
  - Notes (current):
    - ✅ Verified on Linux server: `source ~/.cargo/env; cargo test --test ipc_schema_roundtrip`.
    - ⏳ WSL2 loop still pending (Windows-native remains unsupported).
  - How:
    - Follow `docs/LOCAL_SETUP.md` section “Recommended: Build via WSL2”.

- [ ] **RS-0.1 – Remove remaining MVP-stub metrics/strings (prep)**
  - Scope: eliminate any remaining “MVP stub” labeling paths that could be misread as real execution.
  - Acceptance:
    - No DecisionRecord is written with fake signature-like values.
    - Logs/metrics do not claim “sent” unless a real signature exists.
  - Current state:
    - ✅ Fake signature-like values removed.
    - ⚠️ Simulation still contains stub strings until RS-3.1 is done.
  - Touch:
    - `src/bin/execution_engine.rs`
  - Verify:
    - `cargo test -q --test ipc_schema_roundtrip` (WSL2/Linux)

---

## RS-1 — Plumbing: RPC + Signer integration (no send yet)

- [x] **RS-1.1 – Use `SolanaRpc` inside execution-engine**
  - Scope:
    - Replace blocking `solana_client::rpc_client::RpcClient` usage with `crate::solana::rpc::SolanaRpc` (nonblocking, retry/limiter).
    - Keep config-driven `rpc_url/ws_url`.
  - Acceptance:
    - execution-engine successfully calls `getLatestBlockhash` and `getBalance` through `SolanaRpc`.
    - No new key material is loaded outside execution-engine.
  - Touch:
    - `src/bin/execution_engine.rs`
    - (possibly) `src/config.rs` / config structs if needed
  - Verify:
    - `cargo test -q --test ipc_schema_roundtrip` (WSL2/Linux)

- [x] **RS-1.2 – Centralize key loading via `Treasury` (single-signer hardening)**
  - Scope:
    - Replace local key loader in execution-engine with the canonical wallet loader (`src/wallet.rs` / `Treasury::load_from_env()` if available).
    - Preserve current env var names and server unit configuration.
  - Acceptance:
    - Only execution-engine loads keys.
    - No duplicated key-loading logic remains.
  - Touch:
    - `src/bin/execution_engine.rs`
    - `src/wallet.rs` (only if API gaps exist)
  - Verify:
    - `cargo test -q` (WSL2/Linux)

---

## RS-2 — Tx Plan: deterministic instruction builder (MVP: Pump.fun BUY)

- [x] **RS-2.1 – Add TxBuilder module + intent validation**
  - Scope:
    - Add a focused builder that accepts `(intent, wallet_pubkey)` and returns either:
      - `UnsupportedIntent` (reason-coded), or
      - `PlannedTx { instructions, signers, lookup_tables? }` (keep minimal for MVP).
    - Validate MVP constraints:
      - `side == Buy`
      - `resources.input_mint` is SOL/WSOL
      - exactly one pool (or pump.fun derivation path available)
  - Acceptance:
    - Unsupported intents are rejected with explicit reason code and DecisionRecord.
    - Supported intents produce a deterministic instruction list.
  - Touch:
    - Add: `src/execution/tx_builder.rs` (or similar)
    - `src/bin/execution_engine.rs`
  - Verify:
    - `cargo test -q --test ipc_schema_roundtrip` (WSL2/Linux)
  - Notes (current):
    - Implemented `ironcrab::execution::tx_builder` and wired a `tx_plan` check into execution-engine.
    - Planning is currently enforced only when `send_enabled=true` to avoid breaking dry-run workflows.
    - Pump.fun BUY planning currently requires `metadata.creator` and `metadata.min_out_raw` (raw u64).

- [x] **RS-2.2 – Pump.fun adapter: build BUY ix for a provided user authority**
  - Scope:
    - Extend `src/solana/dex/pumpfun.rs` to produce swap instructions for the engine wallet.
    - Ensure ATA derivations and token program handling are correct.
  - Acceptance:
    - A unit test can build the instruction list without RPC calls (pure derivations).
  - Touch:
    - `src/solana/dex/pumpfun.rs`
    - tests: add `tests/execution_pumpfun_builder.rs` (new)
  - Verify:
    - `cargo test -q --test execution_pumpfun_builder` (WSL2/Linux)

---

## RS-3 — Simulation Gate: real `simulateTransaction`

- [x] **RS-3.1 – Replace stub simulation with real RPC simulate**
  - Scope:
    - Build tx from intent (using RS-2 builder), set recent blockhash, then call `simulateTransaction`.
    - Record `err`, log preview, and `units_consumed` into `SimulationResult`.
  - Acceptance:
    - On simulation error, intent ends as `DecisionOutcome::SimFailed` and **no send occurs**.
    - DecisionRecords include real simulation output (not stub strings).
  - Touch:
    - `src/bin/execution_engine.rs`
    - `src/ipc/schema.rs` only if SimulationResult needs small extensions (prefer not)
  - Verify:
    - `cargo test -q --test ipc_schema_roundtrip` (WSL2/Linux)
  - Notes (current):
    - execution-engine now builds a TxPlan first, then calls `simulateTransaction` via nonblocking RPC.
    - Simulation is unsigned (`sig_verify=false`) and uses `replace_recent_blockhash=true`.
    - Pump.fun BUY planning requires `metadata.creator` and `metadata.min_out_raw`.

---

## RS-4 — Real Send + Confirm (RPC path)

- [x] **RS-4.1 – Implement real sendTransaction (RPC) and capture real signature**
  - Scope:
    - After successful simulation: send tx via RPC.
    - Record returned signature into `SendResult.signature`.
    - Do not claim `Sent` unless the RPC returned a real signature.
  - Acceptance:
    - DecisionRecord includes a real base58 signature string.
  - Touch:
    - `src/bin/execution_engine.rs`
  - Verify:
    - `cargo test -q --test ipc_schema_roundtrip` (WSL2/Linux)

- [x] **RS-4.2 – Confirmation: `getSignatureStatuses` polling + outcome mapping**
  - Scope:
    - Poll for confirmation with timeout.
    - Map to `DecisionOutcome`:
      - confirmed => `Confirmed`
      - timeout / ambiguous => `Sent` (only if signature exists)
      - RPC status shows error => `FailedConfirmed`
  - Acceptance:
    - Outcome reaches `Confirmed` on successful mainnet tx.
    - DecisionRecord includes confirmation-relevant details (at least outcome and timing).
  - Touch:
    - `src/bin/execution_engine.rs`
    - (optional) `src/ipc/schema.rs` if you want explicit confirm fields later (can be deferred)
  - Verify:
    - `cargo test -q --test ipc_schema_roundtrip` (WSL2/Linux)

---

## RS-5 — Metrics + Operator Truth

- [x] **RS-5.1 – Align metrics with real states**
  - Scope:
    - Add minimal counters:
      - `tx_send_attempts_total`
      - `tx_send_success_total`
      - `tx_confirmed_total`
      - `tx_confirm_timeout_total`
    - Ensure dashboards can distinguish `Sent` vs `Confirmed`.
  - Acceptance:
    - No metric named “executed” increments on stub paths.
    - `Confirmed` is the primary success KPI.
  - Touch:
    - `src/metrics.rs`
    - `src/bin/execution_engine.rs`
  - Verify:
    - `cargo test -q` (WSL2/Linux)

---

## RS-6 — Config Surface (minimal, safe defaults)

- [x] **RS-6.1 – Add explicit send/confirm configuration knobs (documented)**
  - Scope:
    - Ensure `send_enabled` is clearly documented.
    - Add `confirm_timeout_ms` (or secs), `preflight_commitment`, and `skip_preflight` as explicit config options.
    - Update docs.
  - Acceptance:
    - No hidden defaults: config schema/doc reflects actual runtime behavior.
  - Touch:
    - `docs/CONFIG_SCHEMA.md`
    - `src/config.rs` / config structs
    - `src/bin/execution_engine.rs`
  - Verify:
    - `cargo test -q` (WSL2/Linux)

---

## RS-7 — Post-MVP extensions (only after RS-4 is proven)

- [ ] **RS-7.1 – Support SELL intents (Pump.fun)**
  - Acceptance:
    - Real signature + confirmation for sell path.
  - Pre-work already landed (dry-run safe, no send yet):
    - SELL token balance preflight (ATA derivation + RPC balance read)
    - SELL token capital locking to prevent overlapping exits

- [ ] **RS-7.2 – Raydium: remove placeholder user accounts in `build_swap_ix`**
  - Scope:
    - Refactor Raydium adapter to accept real `(user_authority, user_source, user_destination)`.
  - Acceptance:
    - No Pubkey::default placeholders remain in production tx building.

- [ ] **RS-7.3 – Orca support**

- [ ] **RS-7.4 – Jito bundle path for `require_bundle=true` intents**

