# Momentum Active Pools — market-data subscriber

Topic: `ironcrab.v1.momentum.active_pools` (wire: `MomentumActivePoolsUpdate`).

## market-data subscriber (Phase 2b)

1. NATS main-loop deserializes `MomentumActivePoolsUpdate` and non-blocking `try_send`s into `momentum_tracking_coalesce`.
2. `momentum_tracking_coalesce` merges bursts (union `active`/`removed`, `full_active_snapshot` semantics), debounces per `geyser_sync_batch_debounce_ms()`, then enqueues **one** `TrackWorkerCommand::ApplyMomentumActivePools` on **`md-track-worker`** (bounded queue).
3. **No** `MdStateCommand::ApplyMomentumActivePools` and **no** immediate `sync_geyser_tracked_accounts()` per message.
4. Track-worker applies pin/unpin to `DesiredExplicitSet` / `tracked_*`, then coalesced Geyser push (500 ms, delta-only).
5. On full track-worker queue: drop + `market_data_momentum_track_worker_enqueue_dropped_total` (and general tracking enqueue dropped).

Full spec: `Iron_crab-eval/docs/spec/MOMENTUM_ACTIVE_POOLS.md`.
