# Migration Guide: Solana 1.18 -> Agave / Solana 3.x

Status: Draft (to be finalized before merge of 3.x branch into `main`)

## Summary
The project is moving from the legacy Solana 1.18 toolchain/runtime assumptions to the modern Agave (Solana 2.x runtime crates / 3.x client crates). A clean branch (`solana3x_clean`) was created without build artifacts to enable pushing.

## Branch & Tag Layout
- `main` – still on pre‑upgrade (Solana 1.18 baseline) as of tag `v0.2.1`
- `solana3x_clean` – active development on Agave / 3.x
- Recommended legacy tag: `v0.2.1-solana1_18` (create once to freeze old state)

## Versioning Plan
- Next release after merge: `v0.3.0` (semantic: minor -> potential API adjustments; consider `1.0.0` only after stable strategy & swap APIs)

## Key Changes
| Area | Old (1.18) | New (Agave / 3.x) | Notes |
|------|------------|-------------------|-------|
| Solana crates | monolithic older versions | Split Agave crates (e.g. `solana_rpc_client`, `solana_transaction`) | More granular dependencies |
| SPL crates | Might require entrypoint features | Using `no-entrypoint` to avoid duplicate symbols | Cleaner linkage |
| Pool Reading | Limited scaffolding | Raydium pool reader implemented | Basis for quoting/arbitrage |
| Backtest | Early skeleton | Slippage enforcement + tests | Deterministic rejection paths |
| Strategy | Rust only | Optional Python (`python` feature) | FFI boundary still experimental |

## Developer Actions After Checkout
```powershell
# Ensure clean deps
cargo clean
# Build normally
cargo build
# Run tests
cargo test -- --nocapture
```

## Slippage Enforcement Behavior
Backtest engine rejects a simulated swap if `actual_out < min_out` (based on configured slippage bps). Rejection is logged & surfaced through test assertions.

## To Do Before Merge
- [ ] Tag legacy state (`v0.2.1-solana1_18`)
- [ ] Decide branch naming (rename `solana3x_clean` -> `solana3.x` or merge directly)
- [ ] Add CI (fmt + clippy + test) matrix for stable toolchain
- [ ] Add basic integration smoke test (Raydium pool fetch)
- [ ] Document required RPC config / rate limits
- [ ] Fill out swap instruction builder for Raydium
- [ ] PDA derivations (authority, vaults, open orders) abstraction
- [ ] Arbitrage path planner skeleton (multi-hop placeholder)
- [ ] Orca adapter parity checks

## Potential Breaking Points
- Public types in `engine/strategy.rs` may evolve (consider feature gating unstable APIs)
- Introduction of compute budget + priority fee config in future commits could alter config schema

## Rollback Procedure
If critical issues appear after merge:
1. Create hotfix branch from legacy tag `v0.2.1-solana1_18`
2. Apply emergency patches
3. Publish as `v0.2.2` (legacy line) while fixing 3.x line separately

## FAQ
Q: Why orphan branch instead of filter-rewrite?  
A: Faster; no collaborators had pulled the large-object history yet. Clean root commit avoids Git LFS or history surgery.

Q: Can we still diff old code?  
A: Yes, tag + legacy branch preserve snapshot.

Q: Will tests differ?  
A: Same semantics; only dependency graph + new modules added.

---
Draft – iterate as features land.
