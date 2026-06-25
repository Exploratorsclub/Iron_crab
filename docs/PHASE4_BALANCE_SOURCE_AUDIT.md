# Phase 4 — Balance / Position Source Audit (P1)

**Plan:** `Iron_crab-eval/docs/plans/plan_hybrid_rollback_tracking_architecture_20260623.md` §6.2–6.3  
**Datum:** 2026-06-25  
**Scope:** `momentum_bot.rs` — Strategy-Position (`PositionTracker`) vs JetStream `WalletBalanceSnapshot`

## SSOT-Ziel (Plan §6.2)

| Concern | SSOT |
|---------|------|
| On-chain Wallet Balance | JetStream `WalletBalanceSnapshot` + Geyser (market-data publiziert) |
| Offene Position (Strategy) | `PositionTracker` + **bestätigte** `ExecutionResult` fills |
| Pool-Preis | Trades / PoolCache mit I-13 (`source_pool == position.pool`) |

**P2-Regel:** Geyser/JetStream-Snapshot ist **Hint / Plausibilität / Divergence-Metrik** — darf bestätigte `token_amount` nicht rückwärts überschreiben. Ausnahme: explizite Cold/Bootstrap-Pfade (Orphan, Wallet-Reconcile).

## Mutations- und Lese-Sites

| Site (fn/Modul) | Liest/Mutiert | Aktuelle Quelle | Soll-SSOT | P2-Aktion |
|-----------------|---------------|-----------------|-----------|-----------|
| `handle_execution_result` — BUY `Confirmed` (pending path) | Mutiert `PositionTracker.token_amount` via `open_position` | `ExecutionResult.fill_out.raw`, `fill_in` / `wallet_sol_delta` für SOL | ExecutionResult | **Behalten** — SSOT; nach Confirm Hint-Cache auf Fill setzen |
| `handle_execution_result` — SELL `Confirmed` (pending path) | Reduziert `token_amount` oder `close_position` | `ExecutionResult.fill_in.raw` (sold), partial vs full | ExecutionResult | **Behalten** — SSOT; Hint-Cache auf `remaining` / remove |
| `handle_execution_result` — Orphan BUY `Confirmed` | `open_position` scale-in / new | `fill_out.raw`, routing aus Position/TokenTracker/metadata | ExecutionResult | **Behalten** — Cold recovery; idempotent via `orphaned_recovered_intent_ids` |
| `handle_execution_result` — Liquidation SELL `Confirmed` | `close_position` | External EE intent; kein pending | ExecutionResult (EE) | **Behalten** — Regression only (I-24d) |
| `open_position` | `token_amount` add / new `PositionTracker` | Caller params (aus ExecutionResult im Live-Pfad) | ExecutionResult | **Behalten** — einzige Live-Mutations-API |
| `close_position` | Entfernt Position | Caller (SELL confirm / guarded snapshot) | ExecutionResult (Live) | **Behalten**; Divergence-Gauge für Mint clearen |
| `resolve_exit_token_amount_raw` | Liest sizing für SELL-Intent | War: JetStream-Snapshot bevorzugt (Scope 57) | Position `token_amount` (confirmed) | **P2:** Overlay SSOT; Snapshot nur Fallback + Divergence |
| `cache_wallet_balance_snapshot_raw` | Schreibt `latest_wallet_balance_raw_by_mint` | JetStream / ExecutionResult Hint | Hint only | **Behalten** — kein direktes Position-Overwrite |
| `process_market_event` — `WalletBalanceSnapshot` balance > 0, Position existiert | Liest Position; cache Snapshot | Snapshot + Position verify log | Position SSOT | **P2:** Divergence-Metrik; **kein** `token_amount`-Write |
| `process_market_event` — `WalletBalanceSnapshot` balance = 0 | `close_position` (auto) | Snapshot zero | ExecutionResult für `Live` | **P2:** Auto-close nur `WalletSnapshot`-Entry oder kein pending SELL |
| `process_market_event` — `WalletBalanceSnapshot` balance > 0, keine Position | `build_reconciled_position` / `orphaned_mints` | Snapshot + pool registry | Snapshot (Cold bootstrap) | **Behalten** — dokumentiert Cold Path |
| `process_market_event` — `WalletSnapshotComplete` | Ghost cleanup `positions.remove` | Wallet scan mint set | Cold reconcile | **Behalten** — Grace 90s; kein Hot-Path-RPC |
| `bootstrap_wallet_snapshot_from_jetstream` | Replay Snapshots; `positions.retain` | JetStream replay | Cold bootstrap | **Behalten** — Startup only |
| `register_pool` / `try_orphan_reconcile` | `build_reconciled_position` | `orphaned_mints` + Snapshot balance | Cold bootstrap | **Behalten** |
| `build_reconciled_position` | Neue Position `token_amount = balance_raw` | Wallet snapshot + `mint_pools` | Snapshot (Cold) | **Behalten** — `entry_source = WalletSnapshot` |
| `recover_positions_from_jsonl` + KV merge (startup) | `token_amount` merge | JSONL ExecutionResults > KV | ExecutionResult (Cold) | **Behalten** — bereits JSONL-authoritative |
| `check_for_exits` / `publish_sell_intent` | Liest `token_amount` via `resolve_exit_token_amount_raw` | Position + Hint | Position SSOT | **P2** via `resolve_exit_token_amount_raw` |
| **execution-engine** `LockManager` (`set_available_token_balance`, `add_available_token_balance`) | EE token balance für Locks / max_open | ExecutionResult confirm + Geyser snapshot | EE: ExecutionResult SSOT (parallel P2 in EE) | **Nicht in diesem PR** — Momentum hat keinen LockManager; Metrik vergleicht Position vs Snapshot |

## Divergence-Metrik (P4)

`momentum_wallet_balance_divergence_lamports{mint}` — signed `position.token_amount - snapshot.balance_raw` für offene Positionen; `momentum_wallet_balance_divergence_total` bei neuem Drift-Event.

Cardinality: nur Mints mit offener Position und `divergence != 0`.

## Referenzen

- KNOWN_BUG_PATTERNS #5 (Ghost Positions) — dual-path balance updates
- FIX-17 — `fill_in` / `fill_out` Kette für entry_price und sizing
- Scope 57 Kommentar `latest_wallet_balance_raw_by_mint` — nach P2: Hint, nicht Authority für confirmed size
