# IronCrab — Bug-Tracker & Fixes

Erstellt: 2026-02-13 | Branch: `architecture-rebuild`

---

## 1. BEHOBENE BUGS (Fixes deployed/committed)

### HYBRID-PHASE5e: ingest/jsonl/md-state Modul-Extraktion + Legacy-Cleanup (Monolith-Slice)
**Datum**: 2026-06-26  
**Problem**: `market_data.rs` (~14.4k LOC nach 5d) — JSONL-Filter/Enqueue, Geyser-Account-Ingest-Filter und md-state Worker/Command-Enum lagen inline im Bin; verbotene `RegisterReservesAfterTrade` / `RegisterPoolVaultsFromAccount` md-state Commands noch im Enum.  
**Fix**: Reine Modul-Grenze ohne Verhaltensänderung: (1) **`src/market_data/jsonl/`** — `market_event_should_jsonl`, `JsonlHost`, `spawn_market_data_jsonl_writer`. (2) **`src/market_data/ingest/`** — `IngestHost`, `account_geyser_*` Filter, `geyser_tx_involves_wallet` Re-Export. (3) **`src/market_data/md_state/`** — `MdStateCommand` (ohne Legacy-Register-Varianten), Worker-Loop, Coalesce, `spawn_md_state_worker`. Bin: `impl JsonlHost` / `IngestHost` / `MdStateContext` auf `MarketDataContext`; dünne JSONL-Enqueue-Wrapper. **Invarianten**: I-4b (bounded try_enqueue, keine tracked_*-Reads in ingest/), I-4c (kein reconcile_arb in ingest), I-7 (kein neuer RPC).  
**Dateien**: `src/market_data/{jsonl,ingest,md_state}/*.rs`, `src/market_data/mod.rs`, `src/bin/market_data.rs`, `docs/BUGS_FIXES.md`

### HYBRID-PHASE5d: cold Ensure* Modul-Extraktion (Monolith-Slice, I-24d)
**Datum**: 2026-06-26  
**Problem**: `market_data.rs` (~14.5k LOC nach 5c) — I-24d Cold-Path `Ensure*` Handler, cache-scoped `cold_path_rpc_refresh_*`, PumpSwap SELL-layout Helpers und `defer_discovery_if_md_state_pressure` lagen inline im Bin.  
**Fix**: Reine Modul-Grenze ohne Verhaltensänderung: (1) **`src/market_data/cold/host.rs`** — `ColdHost` trait + `publish_control_response`. (2) **`rpc_refresh.rs`** — DEX-spezifische cache-scoped RPC refresh. (3) **`ensure_{pump,pumpfun,orca,meteora,raydium}.rs`** — öffentliche `handle_ensure_*` APIs. (4) **`defer.rs`**, **`pump_layout.rs`**. Bin: Control-Dispatch + wallet bootstrap rufen `ironcrab::market_data::cold::*`; `impl ColdHost for MarketDataContext`. **Invarianten**: I-24d (Discovery nur market-data), I-7 (kein neuer Hot-Path-RPC), I-4 (Cache/Geyser vor RPC short-circuit unverändert).  
**Dateien**: `src/market_data/cold/*.rs`, `src/market_data/mod.rs`, `src/bin/market_data.rs`, `docs/BUGS_FIXES.md`

### HYBRID-PHASE5c: md-account-publish Modul-Extraktion (Monolith-Slice)
**Datum**: 2026-06-26  
**Problem**: `market_data.rs` (~18k LOC) — dedizierter `md-publish` Tokio-Runtime-Thread, `AccountPathNatsJob` Queue, Worker-Loop/Dispatcher und Core-NATS-Helpers lagen inline im Bin.  
**Fix**: Reine Modul-Grenze ohne Verhaltensänderung: (1) **`src/market_data/publish/account.rs`** — `AccountPathNatsJob`, bounded `try_send` enqueue, Worker-Pool, Dispatcher, `spawn_md_account_publish_runtime`. (2) **`core.rs`** — `publish_market_event_core_and_momentum_ex`, momentum fan-out classification. (3) **`host.rs`** — `PublishHost` trait; Bin `impl PublishHost for MarketDataContext`. Bin: dünne Wrapper + unveränderte Ingest-Enqueue-Semantik. **Invarianten**: I-4b (bounded try_send), I-7 (kein neuer RPC, nur NATS/JetStream).  
**Dateien**: `src/market_data/publish/{mod,account,core,host}.rs`, `src/bin/market_data.rs`, `docs/BUGS_FIXES.md`

### HYBRID-PHASE5b: md-sidefx Worker Modul-Extraktion (Monolith-Slice)
**Datum**: 2026-06-26  
**Problem**: `market_data.rs` (~20k LOC) — md-sidefx OS-Thread (Sidefx-Worker, Commands, Handler) lag inline im Bin.  
**Fix**: Reine Modul-Grenze ohne Verhaltensänderung: (1) **`src/market_data/sidefx/worker.rs`** — `MdSidefxCommand`, bounded enqueue, burst coalesce, `spawn_md_sidefx_worker`. (2) **`handlers.rs`** — alle `md_sidefx_process_*` Handler (cache + NATS only; Phase-1: kein md-state Register aus parse). (3) **`host.rs`** — `SidefxWorkerHost` trait; Bin `MarketDataSidefxHost` wired. (4) **`pool_publish.rs`** — JetStream metadata helpers. Bin: dünne Eval-grep Wrapper + `impl SidefxWorkerHost`. **Invarianten**: I-4b (Sidefx enqueued kein Register aus parse), I-7 (kein neuer RPC).  
**Dateien**: `src/market_data/sidefx/{mod,worker,handlers,host,pool_publish}.rs`, `src/bin/market_data.rs`, `docs/BUGS_FIXES.md`

### HYBRID-PHASE5a: Track-Worker + Geyser-Sync Modul-Extraktion (Monolith-Slice)
**Datum**: 2026-06-25  
**Problem**: `market_data.rs` (~20k LOC) Monolith — Track-Worker, Coalescer und Geyser-Sync-Flush lagen inline im Bin, erschwerten Slice 5b/5c (ingest/publish/cold).  
**Fix**: Reine Modul-Grenze ohne Verhaltensänderung: (1) **`src/market_data/track/worker.rs`** — `TrackWorkerCommand`, `md-track-worker` OS-Thread, bounded enqueue, `track_worker_process_command`. (2) **`geyser_sync.rs`** — `rebuild_desired_explicit_set_from_ctx`, `track_worker_execute_coalesced_push` (delta-only, 500 ms coalesce). (3) **`coalesce.rs`** — Momentum/Arb NATS-Coalescer + merge helpers. (4) Bin importiert via `ironcrab::market_data::track::*`; `TrackWorkerContext` impl auf `MarketDataContext`. **spec_compliance**: Phase 1–3 bereits behoben (Arb reconcile weg, Momentum/Arb auf track-worker); offen: Gesamt-Monolith <8k LOC (5b/5c). **Invarianten**: I-4b (1 OS thread, bounded enqueue unverändert), I-7 (kein neuer RPC).  
**Dateien**: `src/market_data/track/{worker,geyser_sync,coalesce}.rs`, `src/market_data/track/mod.rs`, `src/bin/market_data.rs`, `docs/BUGS_FIXES.md`

### Phase-2b-MOMENTUM-TRACK-WORKER: Momentum Active Pools bypass md-state
**Datum**: 2026-06-24  
**Problem**: Nach Phase 2a liefen Momentum NATS + `momentum_tracking_coalesce` weiterhin über `MdStateCommand::ApplyMomentumActivePools` auf die md-state-Queue (8192 cap) — reproduzierte Prod-Stall (Post-Deploy Phase 2a).  
**Fix**: (1) **NATS-Subscriber + Coalescer** enqueuen nur noch `TrackWorkerCommand::ApplyMomentumActivePools` auf `md-track-worker` (bounded, Drop + Metrik bei voller Queue). (2) **`MdStateCommand::ApplyMomentumActivePools` entfernt** — md-state bearbeitet keine Momentum-Tracking-Arbeit mehr. (3) Pin/unpin + `DesiredExplicitSet` + coalesced Geyser push (500 ms, delta-only) bleiben im Track-Worker. (4) Metriken: `market_data_track_worker_queue_depth`, `market_data_momentum_track_worker_enqueue_dropped_total`. **Invarianten**: I-4b, I-7, kein RPC Hot Path.  
**Dateien**: `src/bin/market_data.rs`, `src/metrics.rs`, `docs/BUGS_FIXES.md`

### Phase-2a-DESIRED-EXPLICIT-SET: md-track-worker + coalesced delta-only Geyser push
**Datum**: 2026-06-23  
**Problem**: Nach Phase 1 blieb md-state Dumpingground für Momentum `ApplyMomentumActivePools`, Wallet-Pins und Geyser-Sync-Bursts — Queue konnte weiter wachsen.  
**Fix**: (1) **`DesiredExplicitSet`** (`src/market_data/track/desired_set.rs`) als SSOT für explizite Geyser-Pubkeys (Consumer-Refcount `Wallet`/`Momentum`/`Arb`, Pin-Priority-Eviction). (2) **`md-track-worker`** OS-Thread: Track-Commands + coalesced Push (500 ms, delta-only, skip bei leerem Delta). (3) md-state enqueued Track-Worker statt inline `sync_geyser_tracked_accounts_*`. (4) Metriken: `market_data_geyser_explicit_set_size`, `market_data_geyser_subscribe_delta_pubkeys`, `market_data_track_request_coalesce_batches_total`. **Invarianten**: I-4b, I-7, kein RPC Hot Path.  
**Dateien**: `src/market_data/**`, `src/bin/market_data.rs`, `src/metrics.rs`, `src/lib.rs`

### PR237-MD-STATE-WRITER-STARVATION: md-state frozen mid-burst (Queue @8192, bursts_completed Δ=0)
**Datum**: 2026-06-23  
**Problem** (Prod post-PR235 `4696edc`, ~18 min Soak): `burst_in_progress=1` dauerhaft, `jobs_processed`/`bursts_completed` flat, `enqueue_dropped` +8547/30s, md-state `wchan=futex_wait_queue`.  
**Root Cause**: Ingest Hot-Path (`account_geyser_update_might_be_relevant`, `account_geyser_dispatch_priority_high`) hielt kontinuierlich kurze Read-Locks auf `tracked_vaults`/`tracked_mints`/`tracked_bin_arrays`; md-state Writer (Register/Touch/Momentum) verhungerte mid-burst. Zusätzlich `TouchPool` Full-Map-Scan O(alle tracked).  
**Fix**: (1) **Scope A**: `TrackedMembershipSnapshot` via `ArcSwap`, Refresh am Burst-Ende — Ingest ohne `tracked_*`.read(). (2) **Scope B**: `pool_tracked_legs` Reverse-Index, O(legs) Touch; Trade-Pfad `TradePoolLruTouch` → md-sidefx Scratch statt `TouchPool`. (3) **Scope C**: Metriken `md_state_writer_wait_us`, `tracked_membership_snapshot_age_ms`. **Invarianten**: I-4b, PR233 Single-Writer, keine Cap-Erhöhung, kein RPC Hot Path.  
**Dateien**: `src/bin/market_data.rs`, `src/metrics.rs`, `Cargo.toml`, `docs/BUGS_FIXES.md`

### PR234-MD-STATE-STALL: md-state Worker Stall post PR233 (Bounded Evict/Sync)
**Datum**: 2026-06-22  
**Problem** (Prod `3cf97b7`, PR233 merged): Tokio-Freeze behoben, aber md-state hängt in unbounded Evict+Sync — Queue am Cap 8192, ~466 Drops/s, `arb_reconcile_attempts_total` 0.  
**Root Cause**: (1) Evict-Loop unbounded O(k×n) auf md-state. (2) Burst 256 Jobs + vollständiger Flush ohne ms-Budget. (3) Flush-Slot ohne Release nach md-state-Flush.  
**Fix**: Budgetierte Evict/Sync, `ContinueGeyserEvict`, `release_geyser_sync_flush_slot`, `md-state-liveness` OS-Thread, neue Metriken. **Invarianten**: I-4b, PR233 Single-Writer, keine Cap-Erhöhung.  
**Dateien**: `src/bin/market_data.rs`, `src/metrics.rs`, `docs/BUGS_FIXES.md`, `docs/RUNBOOK_PROD.md`

### PR233-TOKIO-FREEZE-SINGLE-WRITER: Global Ingest Freeze ~6min post PR232 (Dual Writer + Tokio Liveness Blind)
**Datum**: 2026-06-21  
**Problem** (Prod `bf79402`, PR232 merged): ~6 min nach Restart Global-Freeze — alle Counter flat, Geyser Recv-Q wächst (2+ MB), alle `tokio-runtime-w` + `md-state` auf `futex_wait_queue`. `global_ingest_stalls_total=0` weil Liveness-Task auf eingefrorener Tokio-Runtime. md-state depth=1, drops=0 — kein Queue-Flood, sondern Runtime-Deadlock durch parallele `tracked_*`-Mutation auf md-state **und** Tokio (`sync_geyser_tracked_accounts_batched_flush`).  
**Root Cause**: (1) `schedule_geyser_sync_batch_debounced` spawnte Evict+Broadcast auf Ingest-Tokio während md-state parallel mutierte. (2) `apply_arb_multi_dex_pins_for_pool` hielt Write-Locks auf `arb_pin_pubkeys` + `tracked_vaults` + `tracked_bin_arrays` gleichzeitig. (3) `touch_tracked_vault_pubkey` O(n) Full-Map-Scan pro Vault. (4) PR167 Liveness via `tokio::spawn` — blind bei Runtime-Freeze.  
**Fix**: (1) `MdStateCommand::FlushGeyserSyncDebounced` — Debounce auf OS-Thread, Sync nur auf md-state. (2) Arb-Pin-Promote: kurze Lock-Phasen, feste Order `arb_pin_pubkeys` → `tracked_vaults` → `tracked_bin_arrays`. (3) O(1) Vault-Touch via `sibling_vault`. (4) `md-ingest-liveness` OS-Thread + `market_data_tokio_liveness_stalls_total`. (5) Subscription-Burst: Full-Reconnect bei Jump >50 in 30s; Flush-Rate-Cap 4/s. **Invarianten**: I-4b, I-7, keine Cap-Erhöhung.  
**Dateien**: `src/bin/market_data.rs`, `src/metrics.rs`, `src/solana/geyser_listener.rs`, `docs/BUGS_FIXES.md`, `docs/RUNBOOK_PROD.md`

### ACCOUNT-WORKER-BACKLOG-PR230-FOLLOWUP: Account-Worker-Backlog + md-state Saturation (Post PR #230)
**Datum**: 2026-06-21  
**Problem** (Prod post-PR230 `6b0d5bd`, ~11h Killswitch): Global-Freeze behoben, aber struktureller Account-Worker-Backlog (`market_data_account_worker_queue_depth` ~34k, LOW-only) und md-state Drops (`geyser_tracking_enqueue_dropped_total` ~38M bei Queue-Cap 8192). Ingest ~457/s vs. Handler-Durchsatz ~245/s (2 Worker).  
**Root Cause**: (1) `account_geyser_update_might_be_relevant` gab für **jeden** DEX-Program-Owner `true` zurück — widerspricht PR230 Hot-Pool-Intent. (2) `md_sidefx_process_live_pool_cache_account_update` enqueued bei jedem Pool-Parse 3 md-state Jobs ohne Hot-Pool-Gate. (3) Worker-Queue-Gauge: `inc` vor `send().await` + `pending_low` ohne sofortiges `dec` bei HIGH-preempt-LOW.  
**Fix**: (1) Relevance-Filter: DEX Pool-State nur bei `is_hot_pool` / `pool_mint_map` / `high_priority_bonding_curves` / wallet-tracked Pump.fun. (2) md-sidefx: md-state Enqueues nur für Hot Pools; Cache-upsert bleibt. (3) `account_geyser_dispatch_priority_high`: `is_hot_pool` (inkl. Arb-LRU). (4) Metrik: `inc` nach erfolgreichem `send`; `dec` beim `low.recv()` vor `pending_low`. **Nicht**: Worker 2→8, Cap-Erhöhung allein, RPC, Non-Hot Vault-Coverage. **Invarianten**: I-4b, I-7, PR230 UnifiedHotPoolRegistry.  
**Dateien**: `src/bin/market_data.rs`, `docs/BUGS_FIXES.md`

### PHASE-UNIFIED-HOT-POOL-REGISTRY: Prod Global-Freeze ~5s nach Restart (Subscription-Sync-Sturm)
**Datum**: 2026-06-20  
**Problem** (Prod `architecture-rebuild` @ `70fb34f`): Nach Restart ~5s Global-Freeze — TX/Account/`head_slot` still, `geyser_*_session_connected=1`. Parallele Momentum-`ActivePoolSet` + Arb-LRU/Reconcile + `tracking_changed`-Sync (Commit `65cff8a`) erzeugten Subscription-Sync-Sturm gegen Caps/LRU.  
**Root Cause**: Doppelte Hot-Set-Pfade (Momentum pins, Arb reconcile inline auf Trade/Account, Pin-Metadata löste debounced Geyser-Sync aus ohne netto neue Subscription-Pubkeys). Vault-Subscribe als Coverage-Mechanismus skalierte nicht.  
**Fix** (PR2): (1) **`UnifiedHotPoolRegistry`** — Momentum ∪ Arb Top-K, dedupliziert, budget-bounded, Single-Writer `md-state`. (2) **Sync nur bei `new_explicit \ old_explicit` nicht leer** — kein `tracking_changed`→Sync. (3) Trade/Account: kein inline `reconcile_arb_multi_dex_for_pool`; Coalesce `RegisterReservesAfterTrade` / `RegisterPoolVaultsFromAccount`. (4) Vault-Rows nur für Hot Pools; **Cache-first `BalanceUpdated`** via JetStream ohne Vault-Geyser-Sub. (5) Metriken: `market_data_hot_pool_registry_pools`, `market_data_geyser_sync_skipped_no_delta_total`, `market_data_balance_updated_from_cache_total`. (6) Bugbot follow-up: `try_publish_balance_updated_from_cache` skip nur bei `pool_has_live_vault_geyser_feed` (Vault-Pubkeys in `last_synced_explicit_pubkeys`), nicht bei bloßer `tracked_vaults`-Row vor Sync-Flush. **Invarianten**: I-7, I-4b, I-16, keine Cap-Erhöhung, keine neuen NATS-Topics.  
**Dateien**: `src/bin/market_data.rs`, `src/metrics.rs`, `docs/BUGS_FIXES.md`

### P172-TRADES-SERVER-TAIL-JSONL-SEGMENTS: Grafana Timeout on Large execution_results JSONL
**Datum**: 2026-06-04  
**Problem** (Prod `ironcrab-prod`): `trades-server` active but `:9899` timed out — single-threaded HTTP blocked on `/pnl_24h` full-file scan. `execution_results-20260604.jsonl` ~103k lines / 109 MB; `load_all_trades([0,1,2])` read every line of three days (~540k lines). Dashboard empty while WORLDCUP-Sell was in logs.  
**Root Cause**: Daily UTC filename rotation only; no size cap on active segment. `trades_server.py` used `for line in f` (O(n) per file). `metrics.rs` `read_trades_from_jsonl` loaded all lines into `Vec` before tail (unchanged in P172; Python path fixed).  
**Fix**: (1) `trades_server`: `_iter_jsonl_tail` (reverse chunk read), `IRONCRAB_TRADES_JSONL_TAIL_LINES` default 15000; lookback `[0]` limit / `[0,1]` run+pnl. (2) `JsonlWriter` optional same-day segments for `execution_results` only (default 32 MiB / 50k records → `.2.jsonl`, …). (3) trades_server reads all segments per day with tail per segment.  
**Dateien**: `scripts/trades_server.py`, `src/storage/jsonl_writer.rs`, `src/bin/execution_engine.rs`, `docs/BUGS_FIXES.md`, `docs/RUNBOOK_PROD.md`

### P171-TRADES-SERVER-DUPES-UTC-BLOCKTIME: Grafana Duplicate Recent Trades + UTC Date Split
**Datum**: 2026-05-31  
**Problem** (Prod `142be782`, Grafana „Recent Trades (Current Run)“): doppelte Zeilen pro `tx_hash` — eine mit leerem `run_id`/null `reason`, eine vollständig; `:9899/trades?mode=run` zeigte 19 Dupes bei ~26 Trades.  
**Root Cause**: PR #176 `load_all_trades` — `_load_trades_from_execution_jsonl` nutzte **lokales** `datetime.now()`, `_load_trades_from_recent_jsonl` **UTC** (wie Rust-Writer). Um Mitternacht CEST: Execution-Tag `20260531` (leer), Recent `20260530` (50 Zeilen); Tag `days_ago=1` lieferte Execution ohne Merge mit Recent → 102 Einträge / 47 doppelte `tx_hash`. Zusätzlich `read_trades_by_run` hing `run_id`-lose Recent-Zeilen an die aktuelle Run-Liste.  
**Fix**: (1) `_utc_date_str` für beide Loader. (2) Merge pro `days_ago` wenn Execution + Recent; finale `_dedupe_trades_by_tx_hash` (Execution gewinnt). (3) Append-Loop in `read_trades_by_run` entfernt. (4) `block_time_unix_ms` in `ExecutionResult` / `RecentTrade` (Cold-Path `getTransaction`); `trades_server` setzt `timestamp_ms` aus Block-UTC mit Fallback `ts_unix_ms`.  
**Dateien**: `scripts/trades_server.py`, `src/bin/execution_engine.rs`, `src/ipc/schema.rs`, `src/metrics.rs`, `docs/BUGS_FIXES.md`

### PR170-MOMENTUM-TRADE-INGEST-GAP: Active-Pool Tracker Forensics + False Bot-Concentration Reject
**Datum**: 2026-05-30  
**Problem** (Prod XHAT `Eyav991r…pump`, 2026-05-30 14:00–14:05 UTC): market-data JSONL zeigte 99+ Buys / 15+ distinct Buyers im Pump-Fenster; momentum blieb bei `buyers 2 < 3` ohne Logs während des Pumps. Tracker existierte (Dev-Sell/CTO sichtbar), Eval aber stumm.  
**Root Cause**: `REJECT_BOT_CONCENTRATION` feuerte ohne `min_samples`-Guard (im Gegensatz zu `REJECT_MICRO_BUY_SPAM`) — bei 2–4 frühen Buys oft 100% top1 → terminal `Rejected` → `check_for_signals` skippt (`is_entry_complete`) → keine `WAIT_BUYER_WINDOW`-Logs trotz weiterer MD-Trades; nach 5-Min-Cleanup Re-Discovery mit leerem Tracker. Zusätzlich: `MomentumContext::record_trade` no-op ohne Tracker ohne Metrik/Log.  
**Fix** (PR170): (1) Buyer-Quality-Reject nur wenn `unique_buyers >= min_unique_buyers.max(5)` (gleiches Pattern wie Micro-Buy-Spam). (2) Prometheus: `momentum_tracker_trades_recorded_total`, `momentum_trades_received_no_tracker_total`, `momentum_tracker_rejected_*_total`. (3) `warn!` bei jedem `TokenTracker::reject`, rate-limited warn bei Trade ohne Tracker, cleanup log mit `trade_count`/`state`, debug ingest sample alle 50 Trades. (4) Unit-Tests: 5-Buyer-Fenster, trade-discovery, CTO+10-Buyer-Regression. **Invarianten**: I-7 (kein RPC), I-12 (Reject/Removal sichtbar), keine Schwellen-Tuning-Änderung an `min_unique_buyers`/`buyer_window_secs`.  
**Dateien**: `src/bin/momentum_bot.rs`, `src/metrics.rs`, `docs/BUGS_FIXES.md`

### PHASE-R-R4B-ACCOUNT-WORKERS-DISCOVERY-DEFER: Account Workers 2 + LivePoolCache off Ingest
**Datum**: 2026-05-30  
**Problem** (Prod post-R4 `1d963ae`, Soak FAIL): TX-Stopp ~20 s (besser als R3 ~3 s, reicht nicht); Teil-Freeze — Account + Head leben, aber `md-sidefx` jobs Δ0 bei Queue ~385 und `md-state` jobs Δ0 bei Queue ~703 (Lock-Deadlock/Convoy mit 8 Account-Workern + schwerem Account-Handler: `live_pool_cache` writes, Discovery-Publish, `.await` auf Publish-Pfad).  
**Fix** (R4b): (1) **`MARKET_DATA_ACCOUNT_WORKER_COUNT` 8→2** (PR141-Fallback), per-shard Cap 5000 (Gesamt-Backpressure ~10k). (2) **`LivePoolCacheAccountUpdate` + `LivePoolCacheMintDecimals`** auf `md-sidefx` — Account-Ingest ohne `live_pool_cache` upsert/merge/set_mint_decimals. (3) **TX `wallet_events`**: nur `try_enqueue` Publish (kein blockierender NATS-Fallback ohne `publish_tx`). (4) Gauge `market_data_account_worker_count`. Unit grep-guards: kein `live_pool_cache` write im Account-Handler. **Invarianten**: I-4b, I-7, I-4 Geyser-first (defer execution only). **Nicht**: Deploy, kein R1–R4 Revert, kein P169d. Evidenz: `Iron_crab-eval/docs/supervisor/phase0_post_r4_deploy_20260530.md`.  
**Dateien**: `src/bin/market_data.rs`, `src/metrics.rs`, `docs/BUGS_FIXES.md`

### PHASE-R-R4-MD-SIDEFX-DEFERRED: Deferred Side-Effects off Geyser Ingest
**Datum**: 2026-05-30  
**Problem** (Plan Phase R / Prod post-R3 `e4ebd09`): Global-Freeze ~3 s nach Restart bleibt — `md-state` entlastet `tracked_*`, Tokio-Ingest blockiert weiter auf `pool_mint_map.write()`, Pump-AMM-Burst, DevWallet, BondingCurve-Pfad und `tracked_vaults.read()` in Account-Handlern (8 Worker + TX-Task Lock-Convoy).  
**Fix** (R4): (1) **`md-sidefx` OS-Thread** (`std::thread`, bounded `sync_channel` Cap 4096, `try_enqueue` only). (2) **TX defer**: PumpFun `pool_mint_map`, PumpFun DevWallet, PumpAmm create/first-trade + DexPoolAccounts/JetStream, generic first-trade accounts. (3) **Account defer**: BondingCurve DevWallet + fallback cache; vault balance ticks (pairing read in sidefx). (4) **Publish**: sync enqueue via PR160 `account_path_try_enqueue_job` (kein NATS-await im Worker). (5) **Burst coalesce** identische Pool-Keys vor `pool_mint_map.write`. Metriken: `market_data_md_sidefx_queue_depth`, `_jobs_processed_total`, `_enqueue_dropped_total`. **Invarianten**: I-4b, I-7, I-16. **Nicht**: R1/R2/R3 revert, kein P169d Tokio-Coalescer. Prod-Evidenz: `Iron_crab-eval/docs/supervisor/phase0_post_r3_deploy_20260530.md`.  
**Dateien**: `src/bin/market_data.rs`, `src/metrics.rs`, `docs/BUGS_FIXES.md`

### PHASE-R-R3-SUBSCRIPTION-COALESCE-TIMEOUT: Geyser Subscription Sink Coalesce + 2s Timeout
**Datum**: 2026-05-29  
**Problem** (Plan `plan_market_data_ingest_rebuild.md` Phase R3 / PR167 Nachzug): Startup-Burst lieferte **14+** in-place `subscription updated` in wenigen Sekunden; blockierendes `subscribe_tx.send().await` im Subscription-Updater konnte die Read-Loop verhungern lassen (`geyser_sync_pending=1`, Send-Q staut, Ingest tot). PR167 adressierte das teilweise (400 ms Rate-Limit, 2 s Timeout); Plan I-4b verlangt explizit **500 ms** latest-wins coalesce und harten Reconnect bei Sink-Backpressure.  
**Fix** (R3): (1) **Dedicated `subscription_updater`**: `push_subscribe_request_to_sink` mit `timeout(2s)` — bei Timeout/Closed **nie** unbegrenzt warten → `sink_fail` → `SessionExit::HardReconnect`. (2) **Coalesce**: `coalesce_pending_subscription` (latest full snapshot) + **max 1 Send / 500 ms** (PR167 400 ms → 500 ms per Plan); weitere Updates während Send/Fenster mergen in ein Pending. (3) **`geyser_sync_pending`**: `sync_geyser_tracked_accounts_batched_flush` setzt pending **am Anfang** auf 0; `schedule_geyser_sync_batch_debounced` ohne Runtime-Handle cleared pending ebenfalls. (4) Metrik `market_data_geyser_subscription_send_timeout_total`. (5) Unit-Tests: 10× notify → 1 Send; blockierter Sink → Timeout + Reconnect-Notify. **Invarianten**: kein RPC (I-7), I-4b (kein ewiges await), I-16 Recovery. Kombiniert mit R2 `md-state` debounced sync.  
**Dateien**: `src/solana/geyser_listener.rs`, `src/bin/market_data.rs`, `src/metrics.rs`, `docs/BUGS_FIXES.md`

### PHASE-R-R2-MD-STATE-THREAD: Tracked-State Single-Writer off Tokio Pool
**Datum**: 2026-05-29  
**Problem** (Phase-0-Diagnose / Plan `plan_market_data_ingest_rebuild.md` Phase R): Nach PR169a/b/c serialisierte ein **Tokio-Actor** Tracking-Mutationen, lief aber auf dem **selben Runtime-Pool** wie TX-Task und 8 Account-Worker. Prod-Freeze (`futex_wait_queue`, `tracking_queue_depth`≈1367 bei Cap 8192): parallele `tracked_*`-Locks + `schedule_geyser_sync` pro Job → Lock-Convoy; TX-Task blockiert.  
**Fix** (R2): (1) **`md-state` OS-Thread** (`std::thread`, bounded `sync_channel`, `try_enqueue`) — einziger Writer für `tracked_*`, Touch-LRU, Eviction, debounced Geyser-Sync. (2) **Touch off Ingest**: Account/TX-Handler enqueuen `TouchVault` / `TouchBinArray` / `TouchPool` statt `touch_tracked_*` direkt. (3) **Burst coalesce**: Worker drain bis `MARKET_DATA_MD_STATE_BURST_MAX` (256), **ein** `schedule_geyser_sync_batch_debounced()` pro Burst. (4) **Momentum** chunked apply sync auf `md-state` (`std::thread::yield_now`, kein Tokio im Worker). (5) Queue-Depth-Gauge = aktuelle Channel-Tiefe (wie JSONL-Writer). Metrik-Alias `market_data_md_state_queue_depth`. **Invarianten**: I-4b (kein blockierendes tracked work im Ingest), I-7 unverändert, PR167 Startup-Debounce beibehalten. **Nicht** in R2: `geyser_listener` Subscription-Timeout (R3), deferred side-effects Queue (R4).  
**Dateien**: `src/bin/market_data.rs`, `src/metrics.rs`, `docs/BUGS_FIXES.md`

### PHASE-R-R1-JSONL-HOT-PATH: JSONL-Serialisierung off Geyser-Ingest-Thread
**Datum**: 2026-05-29  
**Problem** (Phase-0-Diagnose / Plan `plan_market_data_ingest_rebuild.md` Phase R): PR165 brachte `QueuedJsonlWriter` + dedizierten `jsonl-writer`-Thread, aber `try_write` rief weiterhin `serde_json::to_string` auf dem Geyser-Ingest-Thread auf (CPU + Allocation im Hot Path, I-4b-Verstoß).  
**Fix** (R1): (1) `QueuedJsonlMsg::MarketEvent(Box<MarketEvent>)` — Ingest nur `clone` + bounded `try_send`; Serialisierung + Disk-I/O ausschließlich im `jsonl-writer`-Thread. (2) Generisches `try_write` serialisiert via `FnOnce` auf Writer-Thread (Tests/Cold Path). (3) Metriken: `market_data_jsonl_queue_depth`, `market_data_jsonl_records_written_total`. (4) Audit: kein sync `JsonlWriter::write` in Geyser-Handlern; alle Pfade über `write_market_event_jsonl`. **Invarianten**: I-4b (keine schwere Arbeit im Ingest), I-7 unverändert. **Hinweis**: R2 (`md-state`) folgt für Prod-Freeze-Fix; R1 allein reicht für Soak nicht.  
**Dateien**: `src/storage/jsonl_writer.rs`, `src/bin/market_data.rs`, `src/metrics.rs`, `docs/BUGS_FIXES.md`

### PR169c-MOMENTUM-COALESCE-ACTOR-BUDGET: Startup Global-Freeze nach Momentum-Flood
**Datum**: 2026-05-29  
**Problem** (Prod Restart-Smoke `38f355e` nach P169b): ~10 s nach `systemctl restart market-data` **Global-Freeze** — `tx_handler`, `account_updates`, `head_slot` alle Δ0; letztes `Parsed DEX transaction` ~10 s nach Restart; **99×** Momentum-NATS → 99× `ApplyMomentumActivePools` serial + **18×** `subscription updated` (2→719 accounts) in 9 s; `geyser_sync_immediate_total` = 0 (P169b hält), Session verbunden, Runtime tot. P169b serialisierte Writer, coalesced aber nicht den Momentum-Enqueue-Sturm.  
**Restart-Smoke (+8 / +20 / +60 s nach Restart 17:12:36 CEST):**

| Zeitpunkt | `tx_handler` | `account_updates` | `head_slot` | `tracking_jobs` | `tracking_queue` |
|-----------|--------------|-------------------|-------------|-----------------|-------------------|
| +8 s | 3636 | 8210 | 422963221 | 2474 | 46 |
| +20 s | **3636** Δ0 | **8210** Δ0 | **422963221** Δ0 | **2474** Δ0 | **46** Δ0 |
| +60 s | **3636** Δ0 | **8210** Δ0 | **422963221** Δ0 | **2474** Δ0 | **46** Δ0 |

**Fix** (P169c): (1) **`momentum_tracking_coalesce` Task** — NATS `try_send` non-blocking; merge burst (`union active`, `removed`, `full_active_snapshot`-Semantik); debounce aligned mit `geyser_sync_batch_debounce_ms()` (Startup min 250 ms); **ein** `ApplyMomentumActivePools` pro Fenster an Actor. Metriken: `market_data_momentum_coalesced_messages_total`, `market_data_momentum_coalesced_batches_total`. (2) **Actor budget**: `yield_now` + `record_market_data_tokio_progress()` nach jedem Job; große Momentum-Updates (>32 Einträge) in Chunks à 16 mit Yield, **ein** debounced sync am Ende. (3) Kein neuer RPC, kein immediate sync (I-7, I-4).  
**Dateien**: `src/bin/market_data.rs`, `src/metrics.rs`, `docs/BUGS_FIXES.md`

### PR169b-GEYSER-TRACKING-ACTOR-MOMENTUM-WALLET-CONFIG: Momentum / Wallet / Config Single-Writer
**Datum**: 2026-05-29  
**Problem** (Prod `b75fcc8` nach P169a-Deploy): Actor lief (`geyser_tracking_jobs_processed_total` ≈ 1132), aber **Global-Freeze unverändert** — `tx_handler_processed`, `geyser_account_listener_account_updates`, `geyser_head_slot` alle Δ0 nach ~6 s; `market_data_momentum_active_pool_messages_total` = **85** (Restart-Flood); `geyser_sync_immediate_total` = **110** (immediate sync storm); `geyser_merge_pending` = **1** (debounce-Timer feuert nie); `account_worker_queue_depth` = **192** (stehend); Tokio-Threads sleeping, Geyser Recv-Q ~2 MB. P169a serialisierte Account+TX, aber **Momentum Active Pools**, **Wallet-Snapshot** und **Config `max_tracked_accounts`** mutierten weiterhin parallel `tracked_*` und riefen `sync_geyser_tracked_accounts()` immediate auf.  
**Fix** (P169b): (1) **Actor-Commands** erweitert: `ApplyMomentumActivePools`, `TrackWalletMint`, `ScheduleGeyserSyncAfterConfigChange`. (2) **Momentum-NATS** / Simulation: nur `geyser_tracking_try_enqueue(ApplyMomentumActivePools)` — Logik im Actor, ein debounced `schedule_geyser_sync_batch_debounced()` pro Update (kein immediate sync). (3) **Wallet**: Actor vor `publish_wallet_snapshot` gespawnt; Bootstrap + ExecutionResults → `TrackWalletMint` enqueue. (4) **Config** `max_tracked_accounts`: debounced sync via Actor (Bootstrap ohne Actor: einmaliger cold-path immediate sync). (5) **TX `TrackMint`**: enqueue nur wenn Mint noch nicht in `tracked_mints`. **Invarianten**: kein RPC (I-7), Geyser-first (I-4).  
**Dateien**: `src/bin/market_data.rs`, `docs/BUGS_FIXES.md`

### PR169a-GEYSER-TRACKING-SINGLE-WRITER-ACTOR: Account + TX Path (Lock-Convoy Fix)
**Datum**: 2026-05-28  
**Problem** (Prod nach PR167): Startup-Burst mit **8 Account-Workern + TX-Task** mutiert parallel `tracked_vaults` / `tracked_mints` und ruft `sync_geyser_tracked_accounts()` auf → Lock-Convoy auf `MarketDataContext` → Tokio-Runtime tot (Global-Freeze-Symptom).  
**Fix** (P169a, Account+TX only): (1) **`GeyserTrackingActor`** — ein FIFO-Worker (`MARKET_DATA_GEYSER_TRACKING_QUEUE_CAP=8192`), einziger Writer für Tracking-Mutationen; nach Änderung nur `schedule_geyser_sync_batch_debounced()` (kein immediate sync auf Hot Path). (2) **Commands**: `RegisterReservesAfterTrade`, `RegisterPoolVaultsFromAccount`, `TrackMint`. (3) **Account-Pfad**: Vault-Register-Blöcke (Raydium CPMM, Meteora CPMM/DLMM, PumpAmm) → enqueue statt `tracked_vaults.write()` + sync im Handler. (4) **TX-Pfad**: PR167 `TxDeferredSideEffect`-Worker entfernt, in Actor gemerged. (5) `register_geyser_reserves_impl` ohne `GeyserReserveEndSync` am Ende (nur `changed` return). Metriken: `market_data_geyser_tracking_queue_depth`, `market_data_geyser_tracking_enqueue_dropped_total`, `market_data_geyser_tracking_jobs_processed_total`. **P169b** (Momentum/Wallet/Config wiring) bewusst ausgelassen. **Invarianten**: kein RPC (I-7), Geyser-first (I-4).  
**Dateien**: `src/bin/market_data.rs`, `src/metrics.rs`, `docs/BUGS_FIXES.md`

### PR167-MARKET-DATA-GLOBAL-INGEST-FREEZE: Global Ingest Stall, Subscription Backpressure, Lock Scope
**Datum**: 2026-05-28  
**Problem** (Prod `fa27eea` nach PR165/166): ~3 s nach Restart **kompletter** Ingest-Tod — `tx_handler_processed`, `geyser_account_listener_account_updates`, `market_data_geyser_head_slot` alle still 15+ s; NATS-Trades 0; Kernel Geyser Recv-Q ~1,76 MB (Daten kommen, `stream.next()` pollt nicht), Send-Q ~2,10 MB (Subscription-Sink staut); 14× `subscription updated` in 3 s; `geyser_sync_pending=1`; PR166 TX-Watchdog blind (Head auch still). Ursache: Tokio-Runtime-Kollaps unter Startup-Burst — Lock-Convoy auf `MarketDataContext` + blockierendes `subscribe_tx.send().await` im Subscription-Updater.  
**Fix**: (1) **Global Ingest Stall Detector** (10 s Sample, 120 s Grace, 50 s Stall): alle drei Counter flat → `market_data_global_ingest_stalls_total`, Reconnect **TX + Account** Session, danach `exit(1)`; ersetzt PR165 Tokio- + PR166 TX-only-Watchdogs. (2) **Subscription Sink**: coalesce (latest wins), Rate-Limit 400 ms, 2 s send-timeout → `HardReconnect` statt ewig await; Startup tracked-set jump >50 in 1 s → `SubscriptionRebuild`. (3) **Pin/Sync**: Startup-Debounce min 250 ms (120 s); deferred TX worker (reserve register + geyser sync schedule); Lock-Scope in TX-Handler (pool_mint_map write → drop → await). **Invarianten**: kein RPC (I-7), PR165/166/164 unangetastet (Watchdogs konsolidiert, nicht revertiert). **Bricht PR141–166 Symptom-Kreis** — strukturell, nicht weiterer partieller Watchdog.  
**Dateien**: `src/bin/market_data.rs`, `src/solana/geyser_listener.rs`, `src/metrics.rs`, `docs/BUGS_FIXES.md`

### PR166-MARKET-DATA-TX-INGEST-STALL: TX-Handler-Blockade, Payload-Liveness-Metriken, NATS-Noise-Policy
**Datum**: 2026-05-28  
**Problem** (Prod `0c04bac` nach PR165): `market_data_tx_channel_lag_ms_count` friert (~1128); `handle_geyser_transaction` verarbeitet keine weiteren TXs (`futex_wait_queue`); `geyser_tx_listener_transactions_total` steigt weiter (Zähler vor Payload-Check) → TX-Liveness-Reconnect greift nicht; `Parsed DEX transaction` / NATS `Trade` stoppen; Pool-Discovery/Account-Pfad lebt.  
**Fix**: (1) **TX-Hot-Path**: neuer Mint → `schedule_geyser_sync_batch_debounced` statt sofortigem `sync_geyser_tracked_accounts()` (kein Sync-Lock-Sturm mit Account-Workern). (2) **Metriken**: `market_data_tx_handler_processed_total` + `market_data_tx_handler_last_progress_unix_ms` am Handler-Start; `geyser_tx_listener_transactions_total` / `geyser_tx_listener_payload_broadcast_total` nur bei erfolgreichem Payload-Broadcast; TX-Liveness in `geyser_tx_listener` nutzt Handler-Counter. (3) **TX-Stall-Watchdog** (10 s, 120 s Grace, 60 s Fenster): `market_data_tx_handler_stalls_total`, Reconnect-Request an TX-Session, danach `exit(1)` wie PR165. (4) **NATS**: `market_event_should_nats_core` filtert `AccountUpdate` / `TransactionDetected`; unparsed Geyser-Fallbacks early-return + Drop-Metriken. **Invarianten**: kein RPC im Geyser-Ingest (I-7), PR165/164/160 unangetastet.  
**Dateien**: `src/bin/market_data.rs`, `src/solana/geyser_listener.rs`, `src/metrics.rs`, `docs/BUGS_FIXES.md`

### PR165-MARKET-DATA-RUNTIME-LIVENESS: Tokio-Liveness, JSONL off Hot-Path, PumpAmm-RPC aus Geyser-Handler entfernt
**Datum**: 2026-05-27  
**Problem** (Prod `bdfbcda`): `market-data` friert ~14 s nach Restart ein — Prozess lebt, JSONL `market_events` wächst auf Multi-GB (sync Flush + Mutex), `/metrics` und `/live` timeouten (selbe Tokio-Runtime), systemd restartet nicht (PR163 OS-Watchdog pingt weiter), hunderte `pump_amm: pre-loaded vault balances via RPC (Cold Start Bootstrap)` aus `handle_geyser_account` (nicht Wallet-Bootstrap).  
**Fix**: (1) **Tokio-Liveness-Task** (10 s): kein Fortschritt >45 s nach 120 s Startup-Grace → `market_data_tokio_liveness_stalls_total`, Watchdog-Pings stoppen, `sd_notify(Stop)` + `exit(1)` für `Restart=always`. (2) **`/metrics`/`/live`** auf dediziertem `md-metrics` Thread + `current_thread` Tokio (wie PR160 Publish-Isolation). (3) **JSONL**: `QueuedJsonlWriter` (`jsonl-writer` OS-Thread, bounded `try_enqueue`); `flush_each_write` respektiert; periodischer Flush 1 s; **kein** JSONL für `AccountUpdate` / `TransactionDetected`. (4) **Geyser-Handler**: alle RPC aus `handle_geyser_account` entfernt (PumpAmm Cold-Start-RPC + Raydium-Serum-`tokio::spawn`); Reserves nur aus Geyser/Vault-Ticks; Wallet/Ensure*-Cold-Path unverändert. (5) **P1**: `execution_results_deduper` Lock nicht über `await`; PumpAmm-Vault-Registrierung in ersten 60 s debounced `sync`. **Invarianten**: I-7 (kein RPC im Geyser-Ingest), I-4 Geyser-first, PR160/161/164 unangetastet.  
**Dateien**: `src/bin/market_data.rs`, `src/storage/jsonl_writer.rs`, `src/metrics.rs`, `docs/BUGS_FIXES.md`

### PR164-GEYSER-SPLIT-TX-SACRED: zwei gRPC-Sessions (TX heilig + Account/Cuckoo); TX-Liveness-Reconnect
**Datum**: 2026-05-27  
**Problem** (Prod nach PR #162): Nach Startup-Burst in-place `SubscribeRequest`-Updates liefert Yellowstone weiter **Account**-Updates, aber **keine** DEX-`Transaction`-Updates mehr (`Parsed DEX transaction` stoppt; `market_data_geyser_to_publish_ms_trade_count` friert) — **ohne** Stream-Error/Reconnect-Metrik. Ursache: eine Session teilte sich TX-Filter und Cuckoo-Pin-Updates über denselben `subscribe_tx`-Sink; der letzte Full-Replace tötet den TX-Teil still.  
**Fix**: (1) **`GeyserTxListener`**: nur `build_tx_subscribe_request` (DEX-`transactions` + `blocks_meta`), **kein** `subscribe_tx.send` nach Init, **keine** tracked-Pins. (2) **`GeyserAccountListener`**: nur `build_account_subscribe_request` (Owner-`accounts` + Cuckoo; **leere** `transactions`); Merge→`combined_tracked` + async Sink (#162) nur hier. (3) **Metriken**: `geyser_tx_listener_transactions_total`, `geyser_account_listener_account_updates_total`, `geyser_tx_listener_subscribe_updates_total` (0 nach Init), `geyser_account_listener_subscribe_updates_total`, `geyser_tx_listener_liveness_reconnects_total`; `geyser_connected` nur wenn **beide** Sessions up. (4) **TX-Liveness**: wenn 60 s keine TX-Updates **und** `market_data_geyser_head_slot` steigt → nur TX-Session outer reconnect + Log. (5) **Optional**: `PoolCreated`-Dedup pro `pool_address` vor Publish. **Invarianten**: kein RPC (I-7), Geyser-first (I-4/I-16), PR160/#161 unangetastet.  
**Dateien**: `src/solana/geyser_listener.rs`, `src/bin/market_data.rs`, `src/metrics.rs`, `src/solana/geyser_pool_discovery.rs`, `docs/BUGS_FIXES.md`

### PR163-WATCHDOG-OS-THREAD: systemd-Watchdog-Ping auf dediziertem OS-Thread (Timer-Starvation)
**Datum**: 2026-05-27  
**Problem** (Prod post #160/#161): `market-data.service: Watchdog timeout (limit 30s)` → ABRT / Crash-Loop ohne Panic oder Geyser-Fehler. Der Reserve-Ping aus **PR151** lief als `tokio::spawn` + `interval(5s)` auf dem **Haupt-Runtime**; unter Last (Geyser-Ingest, Account-Worker, Publish-Pipeline) **Timer-Starvation** → kein `sd_notify(WATCHDOG)` innerhalb `WatchdogSec=30`. Gleiche Fehlerklasse wie **PR155** (NATS-Publish auf isolierter Runtime).  
**Fix**: (1) **Dedizierter Thread** `md-watchdog`: `std::thread::sleep(5s)` + `sd_notify(WATCHDOG)` in einer Endlosschleife — unabhängig vom Tokio-Scheduler. (2) **Start** unmittelbar nach `sd_notify(Ready)` (vor langem Geyser-/JetStream-Setup), damit der Watchdog nach Ready nie verhungert. (3) **Entfernt**: Tokio-Watchdog-Task in `run_geyser_loop` sowie redundante `Watchdog`-Pings im `activity_interval`-Arm und im Simulations-`select!` (ein kanonischer Ping-Pfad). **`cfg(test)`**: kein Watchdog-Thread (keine Hintergrund-Threads in Unit-Tests). **Invarianten**: kein RPC (I-7), Geyser/NATS-Pipeline unverändert (Scope nur `market_data` binary).  
**Dateien**: `src/bin/market_data.rs`, `docs/BUGS_FIXES.md`

### PR162-GEYSER-INGEST-LIVENESS: eine Geyser-Session + Subscription-Sink aus Read-Loop; Pool-Discovery off Main-`select!`
**Datum**: 2026-05-27  
**Problem** (Prod post #160): `head_slot` / `nats_messages_published_total` frieren ~60 s nach Restart; `geyser_listener: heartbeat` fehlt; letztes `subscription updated` beim Startup-Burst; paralleles `GeyserPoolDiscovery` (zweite gRPC-Connection) liefert weiter Meteora-Logs — Haupt-Read-Loop liefert keine Nutzlast mehr (Ingest-Starvation). Publish-Pipeline (#160) war nicht der Blocker.  
**Fix**: (1) **`GeyserPoolDiscovery` entfernt** — `PoolDiscoveryIngest::spawn_unified` hängt an denselben `GeyserListener`-Broadcasts wie TX/Account (`subscribe_account_updates` / `subscribe_transaction_updates`); genau **eine** `subscribe_with_request`-Session. (2) **`subscribe_tx.send` nicht mehr im Stream-`select!`**: Pending `SubscribeRequest` in `Mutex<Option<_>>` + `Notify`; dedizierter Task awaited nur den Yellowstone-Sink; Read-Loop bleibt auf `stream.next()` fair (Heartbeats, Blockhash, Metrik `geyser_listener_stream_messages_total`). (3) **Pool-Discovery-MarketEvents**: dedizierter Tokio-Task `recv` auf `mpsc` (bounded **10000**) + `handle_pool_discovery_market_event` — kein Arm mehr im Main-`select!` (Fairness wie #151 TX-Ingest). **Invarianten**: kein neuer Hot-Path-RPC (I-7), PR160 Publish-Runtime unangetastet, I-16 kein stilles Droppen zulässig (Enqueue-Drop-Metriken PR160 bleiben).  
**Dateien**: `src/bin/market_data.rs`, `src/solana/geyser_listener.rs`, `src/solana/geyser_pool_discovery.rs`, `src/metrics.rs`, `src/solana/dex/meteora_dlmm.rs`, `src/solana/dex/raydium_cpmm.rs`, `docs/BUGS_FIXES.md`

### PR161-GEYSER-MERGE-COALESCING: Merge-Task `combined_tracked` debounced wie TX-Path-Sync
**Datum**: 2026-05-27  
**Problem** (Prod post #159): Startup hunderte `subscription updated (NO reconnect)` in wenigen Sekunden — Cuckoo (#159) macht Updates billiger, aber **jeder** Merge feuerte sofort `combined_tracked_tx.send` (bis zu 4× pro `broadcast_tracked_geyser_explicit_to_merge` wegen vier `watch`-Channels) → Read-Loop-Last und NATS-Backpressure-Risiko.  
**Fix**: Gleiches Debounce-Pattern wie `schedule_geyser_sync_batch_debounced`: bei jedem `watch`-Trigger wird ein `tokio::spawn`+`sleep(geyser_sync_batch_ms)` (Default **35 ms**, Clamp **10–100**, TOML `[market_data_geyser] geyser_sync_batch_ms`) neu geplant; vorheriger Timer wird abgebrochen; nach Ablauf **ein** Merge+`send` + `geyser_metrics_set_subscription_accounts` + `refresh_geyser_pins_gauge`. Kein Sekunden-Debounce im Trading-Pfad; nur Sync-Grenze Merge→GeyserListener (I-16: Verzögerung maximal `geyser_sync_batch_ms`). Metriken: `market_data_geyser_merge_coalesced_total`, `market_data_geyser_merge_immediate_total` (reserved), `market_data_geyser_merge_pending`. `market_data_geyser_sync_*` unverändert.  
**Dateien**: `src/bin/market_data.rs`, `src/metrics.rs`, `docs/BUGS_FIXES.md`

### PR160-MAIN-LOOP-NONBLOCKING-NATS-PUBLISH-PIPELINE: Geyser-`select!` und TX-Pfad enqueue-only; Publish-Runtime isoliert
**Datum**: 2026-05-27  
**Problem** (Prod nach #159): `nats_messages_published_total` friert; massiv `NATS publish timeout (backpressure)`; `account_publish_queue_depth` entleert nicht. Ursache: **ein** serialer Publish-Worker auf geteiltem Client + **synchrones** NATS in `run_geyser_loop`-`select!` (MintInfo, Blockhash, PoolDiscovery, ExecutionResults-JetStream) und im TX-Task — Main-Loop blockiert → Fairness bricht → Pipeline verhungert (I-7-äquivalent: Hot-Path darf nicht auf NATS-I/O warten).  
**Fix**: (1) **Alle** genannten Pfade nur noch `account_path_enqueue_*` (`try_send` auf Hauptqueue, Drop-Metrik `market_data_account_publish_enqueue_dropped_total` bei vollem Channel). (2) **TX-Task** (`handle_geyser_transaction`): Core/JetStream/Priority-Fee wie Account-Pfad über Queue; neues Job-Variant `CoreTopicJson` für `TOPIC_PRIORITY_FEE_SAMPLES`. (3) **Publish-Runtime**: eigener `std::thread` `md-publish` + `tokio` multi-thread; **Dispatcher** round-robin auf **N** Worker-Queues (`MARKET_DATA_PUBLISH_WORKER_COUNT`, Default **4**); jeder Worker eigene `NatsClient`-Verbindung `market-data-publish-{id}` aus `connection_config_template()` (nicht `clone_for_spawned_publish`); **2 s** `timeout` pro Job → `market_data_account_publish_worker_stalls_total`, nach Stall frische TCP-Verbindung + `market_data_account_publish_worker_reconnects_total`; Histogram `market_data_account_publish_worker_job_duration_us`; Gauges `market_data_account_publish_worker_last_success_unix_ms{worker}`. **Nicht** reintroduziert: #156 Watchdog-/Churn-Heuristiken. **Invarianten**: kein RPC im Hot Path (I-7), keine NATS-Schema-Änderung (I-23), Geyser-Subscription-Logik unverändert (Scope B).  
**Dateien**: `src/bin/market_data.rs`, `src/metrics.rs`, `src/nats/client.rs`, `docs/BUGS_FIXES.md`

### TX-PATH-GEYSER-ADMISSION-BATCH: Explizite Vault/Bin-Subscribe nur bei Admission + debounced Sync
**Datum**: 2026-05-26  
**Problem**: Nach PR #146 registrierte der TX-Trade-Pfad Reserve-Vaults/Bins via `register_geyser_reserves_after_trade` **ohne** `admit_geyser_explicit_pool_assets`; bei hohem Trade-Durchsatz (PR #151) folgte ein Sturm synchroner `sync_geyser_tracked_accounts()`-Aufrufe → Geyser-Subscription-Churn / NATS-Last (Prod-Ausfälle; Workarounds #153–#156 in #157 revertiert).  
**Fix**: (1) **TX-Pfad**: `LivePoolCache`-Hit, `pool_mints_for_geyser_explicit_tracking`, dann `admit_geyser_explicit_pool_assets` — erst danach `register_geyser_reserves_impl` mit `GeyserReserveEndSync::Batched`. (2) **Batch-Sync**: Debounce per `tokio::spawn` + `sleep(geyser_sync_batch_ms)` (Default **35 ms**, TOML `[market_data_geyser] geyser_sync_batch_ms`, Clamp **10–100**); erneuter Trade resettet den Timer (ein Flush). **Momentum** / `max_tracked_accounts` / Wallet-Tracks: weiterhin **Immediate** via bestehendem `sync_geyser_tracked_accounts()`. (3) `touch_tracked_pool_vaults_and_bins` nur noch, wenn der Pool **bereits** in den expliziten Track-Maps liegt (`touch_tracked_pool_vaults_and_bins_if_tracked`). Metriken: `market_data_geyser_sync_batch_total`, `market_data_geyser_sync_immediate_total`, `market_data_geyser_sync_pending`. **Invarianten**: kein RPC im Hot Path (I-7), PR-B Admission konsistent mit Account-Pfad, kein Re-Intro #153–#156.  
**Dateien**: `src/bin/market_data.rs`, `src/metrics.rs`, `src/config.rs`, `docs/BUGS_FIXES.md`, `docs/RUNBOOK_PROD.md`

### PR151-NATS-WATCHDOG-FOLLOWUP: TX DevWallet async NATS + systemd-Watchdog unter Backpressure
**Datum**: 2026-05-26  
**Problem** (Prod `ironcrab-prod`): Nach PR #151 (Creator-Latenz korrekt) blieb der TX-Fast-Path `maybe_emit_dev_wallet_after_pool_mint_map` bei **synchronem** `publish_market_event_core_and_momentum_ex().await` im dedizierten Geyser-Tx-Task. Unter NATS-Client-Backpressure (100 ms Publish-Timeout) blockierten viele aufeinanderfolgende Versuche den Tokio-Scheduler; der Main-`select!` kam nicht rechtzeitig zum 10 s-`activity_interval` → **kein** `sd_notify(WATCHDOG)` → systemd `WatchdogSec=30` killte `market-data` in einer Schleife. Symptome: `nats_messages_published_total` friert, Logs `NATS publish timeout (backpressure)` / `Market event publish to NATS (core) dropped or failed`.  
**Fix**: (1) **TX-Fast-Path** nutzt dieselbe **async**-Queue wie der Account-Pfad: `account_path_enqueue_core_market_event` mit `publish_tx` (kein direktes Await auf Core-Publish im Tx-Ingest-Task wenn Queue existiert). `account_publish_tx`-Worker wird **vor** dem Tx-`tokio::spawn` gestartet und an `handle_geyser_transaction` durchgereicht. (2) **Dedizierter Watchdog-Task** `tokio::time::interval(5 s)` + `sd_notify(WATCHDOG)` (nur unix), unabhängig von Geyser-`select!` und NATS-Last — Reserve zu `WatchdogSec=30`. Idempotenz / `pool_creator_cache` / Metrik `market_data_pool_mint_map_to_devwallet_ms` (Enqueue-/Vorbereitungszeitpunkt) unverändert. **Invarianten**: kein neuer RPC (I-7), keine NATS-Schema-Änderung (I-23), kein Creator aus Swap-Accounts (I-4).  
**Dateien**: `src/bin/market_data.rs`, `docs/BUGS_FIXES.md`, `docs/RUNBOOK_PROD.md`

### ACCOUNT-PATH-TX-PARITY-CREATOR: DevWallet/Creator-Latenz — TX-Fast-Path + strategische Account-Queues
**Datum**: 2026-05-25  
**Problem** (Prod-Referenz „WONDERBRA“, 2026-05-24): Trade-Discovery ohne `PoolCreated`; `BondingCurveUpdate` füllte `pool_creator_cache`, aber `DevWalletIdentified` wurde nur bei **bekanntem** `pool_mint_map` emittiert — Updates **vor** dem ersten Trade gingen verloren (kein Re-Emit). Zugleich verarbeiteten **8** Account-Worker-FIFO-Shards massenweise PumpFun-Bonding-Curves; strategisch relevante Pools lagen **Minuten** in der Wall-Clock-Warteschlange hinter irrelevanten Updates (`ENTRY_BUY_DEFERRED_MISSING_CREATOR` / BUY-Gate).  
**Fix**: (1) **TX-Fast-Path** `maybe_emit_dev_wallet_after_pool_mint_map`: nach `pool_mint_map.insert` (PumpFun-Trade / `PoolCreated`) synchron im TX-Task prüfen, ob `pool_creator_cache` bereits den autoritativen Creator hat — dann `DevWalletIdentified` wie Account-Pfad (FIX-22-Mismatch-WARN unverändert); Metrik `market_data_pool_mint_map_to_devwallet_ms` (Geyser-TX-`grpc_recv_at` → Publish). (2) **Zwei Queues pro Shard** (HIGH/LOW) mit strikt HIGH-vor-LOW-Drain; Admission HIGH u. a. bei `pool_mint_map`, `high_priority_bonding_curves` (First-Trade-Hook), `active_pool_set`-Pins, `wallet_tracks_mint`; Gauges `market_data_account_high_priority_queue_depth` / `market_data_account_low_priority_queue_depth`. (3) **Account-Pfad** nach erfolgreichem `DevWalletIdentified`-Publish: `market_data_bonding_curve_grpc_to_devwallet_ms` (Bonding-Curve-`grpc_recv_at` → Publish). **Verboten** unverändert: kein Creator aus Swap-Accounts (I-4 / BUG-D), kein RPC im Hot Path (I-7), keine NATS-Schema-Änderungen (I-23).  
**Dateien**: `src/bin/market_data.rs`, `src/metrics.rs`, `docs/BUGS_FIXES.md`, `docs/RUNBOOK_PROD.md`

### MARKET-DATA-ACCOUNT-THROUGHPUT-P0: Account-Pfad Durchsatz + async NATS-Publish
**Datum**: 2026-05-21  
**Problem**: Dedizierter Account-`recv`-Task (#140) beseitigte `select!`-Starvation, aber `handle_geyser_account` blieb **seriell**: hohe Geyser-Account-Rate ⇒ `market_data_account_broadcast_queue_depth` im **Tausender-Bereich**, `market_data_account_channel_lag_ms` p50 **~2,4 s** (Tx-Pfad blieb ~ms).  
**Fix**: (1) **8 Worker** mit `tokio::mpsc` pro Shard (`hash(pubkey) % 8`, je **1250** Kapazität ≈ **10k** Backpressure-Budget), Recv-Task misst weiter `account_channel_lag_ms` und `account_broadcast_queue_depth`. (2) **Dedizierter Publish-Worker** (`clone_for_spawned_publish`) mit `mpsc` (**16384**): Worker **awaiten nicht** `jetstream_publish` / `publish_market_event_core_and_momentum_ex` — Jobs als `serde_json::Value` (JetStream) bzw. `Box<MarketEvent>` (Core). (3) **Early-Filter** im Recv-Task: unbekannte Owner ohne Wallet-/tracked_mints/vaults/bin_arrays und ohne DEX-Program-IDs → `market_data_account_early_drop_total`. **Kein** neuer Hot-Path-RPC (I-7); Cold-Path `tokio::spawn`+RPC unverändert.  
**Ordering / I-24b**: Global **eine** Publish-FIFO-Queue (keine per-Pool-Shards); Account-Updates **pro Pubkey** bleiben durch Worker-Sharding seriell — Cross-Pubkey-Reihenfolge kann sich ändern (Trade-Pfad unverändert).  
**Dateien**: `src/bin/market_data.rs`, `src/metrics.rs`, `docs/BUGS_FIXES.md`, `docs/RUNBOOK_PROD.md`

### MOMENTUM-VELOCITY-TRADES-PER-MIN-2026-05-20: Entry-Velocity-Filter — `trades/min` statt `trades/s`
**Datum**: 2026-05-20  
**Problem**: Schwelle als **Trades pro Sekunde** war in Prod (z. B. JetStream `min_trades_per_sec` ≈ 5) extrem streng; viele Pools mit 3–4 Trades/s fielen dauerhaft durch (`filter_passed_total` blieb 0).  
**Fix**: Kanonische Einheit **`min_trades_per_min`** / Metrik **`trades_per_min`** (gleiche Ketten-Slot-Zeitbasis wie zuvor, nur ×60 für Anzeige/Schwelle). TOML/JetStream-Key **`min_trades_per_sec`** wird weiterhin akzeptiert: Wert ×60 + **warn** (kein stilles 1:1 als „pro Minute“). Default im Code: **30/min** (≈0.5/s).  
**UI-Follow-up** (Control-Plane): `ui/src/pages/ComponentDetail.tsx` zeigt und speichert **`min_trades_per_min`** (Default UI **1.2** wie `my_config.server.toml`); beim Laden wird **`min_trades_per_sec`** aus dem Draft entfernt bzw. einmalig ×60 migriert; Speichern strippt den deprecated Key.  
**Dateien**: `src/config.rs`, `src/bin/momentum_bot.rs`, `my_config.server.toml`, `docs/CONFIG_SCHEMA.md`, `docs/BUGS_FIXES.md`, `docs/RUNBOOK_PROD.md`, `ui/src/pages/ComponentDetail.tsx`

### FIX-MOM-LP-OBSERVED-AT-MINT-MERGE-MAX-2026-05-18: LP-Removal Wallclock — Mint-Aggregat `max` statt `min` (PR #134 Follow-up)
**Datum**: 2026-05-18  
**Problem**: Beim Merge von `TrackerExitSignals` pro Mint über mehrere Pool-Tracker war `lp_removal_observed_at` mit `min` zusammengeführt — das verschärfte das Wallclock-Fenster künstlich und konnte LP-Hard-Exits still unterdrücken (Bugbot Low auf PR #134).  
**Fix**: Mint-Aggregat nutzt wieder **`max`** (späteste Sibling-Beobachtung), analog zur Legacy-Logik für `lp_removed_at`; Unit-Test gegen `min`-Regression.

### FIX-MOM-ENTRY-FILTERS-CHAIN-SLOT-2026-05-17: Momentum Entry-/Filter-Fenster vs. Ingest-Burst (Kettenzeit)
**Datum**: 2026-05-17  
**Problem**: `TokenTracker` und Entry-Filter nutzten `Instant::now()` beim Ingest für Fenster (Käufer, Inflow, Velocity, Microbuy/Buyer-Quality). Bei NATS-/Market-Data-Bursts wirkten viele historische Trades gleichzeitig „frisch“ — Filter bestanden, obwohl die Kette schon lange stillstand (Chart vs. Geyser-Slot).  
**Fix**: Fenster und Raten über **Geyser-`slot`** normiert (`MarketEvent.slot`, Head `last_event_slot`); Sekunden-Konfig → Slot-Spanne via `MOMENTUM_APPROX_SLOT_MS` (~400 ms/Slot), **kein** `getBlockTime`/RPC im Hot Path. **`DEV_SELL`** / LP-Post-Entry-Hard-Exits: **Slot** des Dev-Sells bzw. LP-Removal vs. `entry_confirmed_slot` (Fallback-Doku bei Slot 0). `RecordHeader.ts_unix_ms` unverändert nur für Transport-/Latenzmetriken. Unit-Test: gleicher Ingest-Instant, weit auseinanderliegende Slots — kein künstliches 10 s-Wallclock-Fenster.  
**Dateien**: `src/bin/momentum_bot.rs`, `docs/BUGS_FIXES.md`

### FIX-MOM-POOLCACHE-LIVE-ONLY-2026-05-16: Momentum kein globales POOL_CACHE LastPerSubject-Replay
**Datum**: 2026-05-16  
**Problem**: `momentum-bot` nutzte `bootstrap_pool_cache_from_jetstream` mit `LastPerSubject` und übernahm denselben Consumer — hunderttausende Snapshot-Messages erzeugten JetStream-Backpressure und blockierten die Core-NATS-Verarbeitung trotz priorisierter Strategy-Fixes.  
**Fix**: Runtime-Consumer mit `DeliverPolicy::New` und durable `momentum-bot-pool-cache-live` (`pool_cache_live_consumer_config`); kein globaler Bootstrap mehr. Offene Positionen triggern bounded/deduped `Ensure*` ControlRequests an `market-data` (Startup + Retry-Tick wenn keine executable Quote). **PumpFun-Positionen:** parallel `EnsurePumpfunBondingCurve(force_refresh_pumpfun)` und `EnsurePumpAmmPoolAccounts(force_refresh)` **ohne** `pool_address_hint` auf der Bonding-Curve-Adresse (PumpAmm-Discovery für Migration/Exit); reine `pump_amm`-Positionen unverändert mit Pool-Hint.  
**Dateien**: `src/bin/momentum_bot.rs`, `src/nats/jetstream.rs`, `docs/BUGS_FIXES.md`

### FIX-MOM-STRATEGY-INGEST-2026-05-16: Strategy-Tick vs. Core-NATS-Drain (PR111-Follow-up, PR128 erweitert)
**Datum**: 2026-05-16 (Follow-up: strikt event-/dirty-getrieben, kein periodischer Hot-Path-Scan)  
**Problem**: Periodischer Entry-Full-/Safety-Scan über viele TokenTracker hielt `token_trackers` unter Write-Lock und band zusammen mit `MissedTickBehavior::Burst` auf dem 500-ms-Strategy-Intervall den Core-NATS-Ingest; Momentum verarbeitete nur noch niedrige zweistellige Events/s trotz schnellem `process_market_event`. Auch ein **budgetierter** periodischer Scan bleibt Hot-Path-Scheduler-Zeit und kann NATS-Drain erneut verdrängen (Regression-Klasse wie PR121).  
**Fix**: (1) `strategy_interval` mit `MissedTickBehavior::Skip`. (2) **Kein** periodischer Entry-Scan mehr im Live-Hot-Path — `check_for_signals_dirty_priority_tick` wertet **ausschließlich** explizit dirty-markierte Pool-Tracker aus; leeres Dirty-Set → sofortiger Return. (3) Nach gesättigtem Core-NATS-Batch (`drain_count == effective_cap`) weiterhin optional sofort nächste Nachricht in derselben `select!`-Aktivierung ziehen (Drain-Priorität). Korrektheit über vollständiges `mark_entry_eval_dirty_*` auf den MarketEvent-/State-Pfaden, nicht über einen Scan-Ersatz.  
**Dateien**: `src/bin/momentum_bot.rs`, `docs/BUGS_FIXES.md`

### FIX-MOM-NATS-FAIRNESS: Momentum Core-NATS MarketEvents Slow-Consumer-Stall
**Datum**: 2026-05-13  
**Problem**: In `momentum_bot` verarbeitete der `tokio::select!`-Hauptloop pro Aktivierung praktisch nur **eine** Core-NATS-MarketEvent-Nachricht (außer kleinen `BondingCurveProgress`-Streaks per `now_or_never`), während JetStream-Arme (PoolCache, Wallet-Snapshots) große Batches und der 500-ms-Strategy-Tick viel CPU banden. Folge: Client-Buffer wächst, `async_nats` meldet massenhaft Slow-Consumer, `last_slot`/Ingest stagnieren.  
**Fix**: Gebündeltes, kappenbegrenztes Draining (`CORE_MARKET_EVENTS_INGEST_DRAIN_MAX`) mit erhaltener Reihenfolge; `flatten_market_events_for_ingest_ordered_batch` für BCP-Coalescing wie zuvor; etwas kleinere PoolCache-/Wallet-Fetch-Kappen; `tokio::task::yield_now` nach schweren Armen; Strategy-Tick überspringt `check_for_signals` ohne Tracker/Pending-BUY und `process_exit_signals` ohne offene Positionen; Default-Tracing `async_nats=warn` gegen INFO-Log-Sturm (Nebenmaßnahme).  
**Dateien**: `src/bin/momentum_bot.rs`

### FIX-MOM-INGEST-THROUGHPUT: MarketEvents-Batch — ein interleaved ExecutionResult-Drain, Exit-Scan nur mit Positionen
**Datum**: 2026-05-13  
**Problem**: Nach FIX-MOM-NATS-FAIRNESS lief der Ingest wieder, aber der Durchsatz blieb niedrig: Pro Event im 48er-Drain wurden `process_exit_signals().await` (auch ohne offene Position) und ein JetStream-`ExecutionResult`-Fetch mit kurzem `expires` ausgeführt — amortisiert bis zu ~48× Fetch-Latenz pro Batch.  
**Fix**: `drain_execution_results` (interleaved) höchstens **einmal** nach vollständiger Verarbeitung eines MarketEvents-Batches, nur wenn das Batch preissensitiv war und Positionen oder ausstehende Execution-Intents existieren; `process_exit_signals` in MarketEvents-, PoolCache- und Wallet-Snapshot-Pfaden nur bei `position_count() > 0`. Geplanter `select!`-Arm für ExecutionResults unverändert.  
**Dateien**: `src/bin/momentum_bot.rs`

### FIX-MOM-QUOTE-FIRST-PRICE-EXITS: STOP/TP/Trailing aus LivePoolCache-Quote
**Datum**: 2026-05-14  
**Problem**: `STOP_LOSS`/`TAKE_PROFIT`/`TRAILING_STOP` trippen nur, wenn `current_price` die Schwelle erreichte; die validierte `ExitExecutableQuote` widersprach oft nicht rechtzeitig. Positions fielen durch zu `TIME_EXIT`, obwohl die ausführbare Reserve-Quote schon weit unter Stop lag (Mark vs. tatsächlicher Sell-Preis).  
**Fix**: Quote-first: bei nutzbarer `ExitExecutableQuote` (pool_sourced, finite `tokens_per_sol`) entscheiden `STOP_LOSS` und `TAKE_PROFIT` nach executable PnL; `current_price` allein triggert sie nicht (keine sekundäre „Stale-Mark“-Suppressionsstufe mehr). **`TRAILING_STOP`:** executable Drawdown nur, wenn `quote_pool == position.pool` (`marks_position_pool`); sonst kein Vergleich gegen Positions-Pool-ATH (I-13). Ohne nutzbare Quote feuern die drei Preis-Exits nicht aus Mark-/Trade-Rauschen. `TIME_EXIT` unverändert; Reporting-PnL nutzt weiter die Quote wenn vorhanden.  
**Dateien**: `src/bin/momentum_bot.rs`, `docs/MOMENTUM_V2_SPEC.md`, `docs/BUGS_FIXES.md`

### FIX-MOM-INGEST-FAIRNESS-2: Core-NATS-Durchsatz vs. JetStream/State — Scheduling + Druck-Metriken (Follow-up inkl.)
**Datum**: 2026-05-15 (Follow-up Review 2026-05-16)  
**Problem**: Trotz Batch-Drain blieb der effektive Core-NATS-Durchsatz deutlich unter der Publish-Rate: JetStream-Arme mit langem `expires`, der ständig startende geplante `ExecutionResult`-Fetch und `select!`-Fairness begünstigten andere Zweige; festes Drain-Cap (48) war oft gesättigt, ohne dass die Hot-Path-Zeit pro Event hoch war. **Review-Follow-up:** `tokio::select! { biased; }` mit MarketEvents vor PoolCache/ER/Wallet/Strategy konnte bei dauerbereitem NATS die unteren Arme verhungern; Slot-Differenz `latest_seen - last_processed` aus demselben verzögerten Consumer-Buffer war **kein** echter Live-Head-Backlog und täuschte adaptive Caps / Grafana.  
**Fix**: (1) **Kein `biased`** im Haupt-`select!`; nach jedem Core-NATS-Ingest-Batch garantiert `momentum_interleave_jetstream_after_core_market_batch` (begrenzter ER-Drain + begrenzter PoolCache-Pull), damit State/Exit-Pfade auch bei NATS-Backlog weiterlaufen. (2) Geplanter `ExecutionResult`-Drain nur auf `interval(EXECUTION_RESULT_SCHEDULED_POLL_INTERVAL)`; kürzere `expires` für PoolCache-/Config-/Wallet-Leerpfade; adaptives Core-Drain-Cap (Boden 96, Decke 320) aus **aufeinanderfolgenden Cap-saturierten** Drain-Batches (`momentum_core_market_events_ingest_consecutive_cap_hit_streak`), nicht aus Slot-Delta vs. fiktivem Live-Head. (3) Prometheus: `momentum_market_events_subscription_max_dequeued_slot`, `momentum_market_events_last_applied_slot`, `momentum_market_events_internal_slot_delta_slots` (explizit nur Subscription vs. angewandt, **nicht** Chain/NATS-Head); Cap-Hit-/Drain-Zähler; per-Kind received/processed inkl. **PoolStateUpdate** und **TokenMintInfo** (bounded statische Counter).  
**Dateien**: `src/bin/momentum_bot.rs`, `src/metrics.rs`, `docs/BUGS_FIXES.md`

### FIX-MOM-REALTIME-2026-05-16: Watchdog-Starvation, fehlender Creator-Loop, Backlog-Metriken, Momentum-Fanout-Zähler
**Datum**: 2026-05-16  
**Problem**: (1) Lange Core-NATS-Ingest-Schleifen ohne Rückkehr in `tokio::select!` konnten `WatchdogSec`/Activity-Arme verhungern lassen (systemd-Neustarts, Metrik kurz `Connection refused`). (2) PumpFun BUY scheiterte bei fehlendem Creator; `unwind` markierte sofort wieder `dirty` → enge ENTRY/ERROR-Schleife. (3) `momentum_market_events_internal_slot_delta_slots` ≈ 0 verbarg Wall-Clock-Backlog. (4) Publish-Rate auf `ironcrab.v1.market_events.momentum` war gegenüber Core-`market_events_published_total` nicht separat sichtbar.  
**Fix**: (1) Alle 32 verarbeiteten Events: `yield_now` + `sd_notify(WATCHDOG)` (unix). (2) `unwind_stale_entry_pending_after_publish_failure(..., requeue_dirty)`; bei fehlendem Creator `requeue_dirty=false`, pool-scoped 45s-Suppress-Map + Counter `momentum_entry_buy_suppressed_missing_creator_total`; Clear bei `record_dev_info` / LivePoolCache-Creator-Korrektur. (3) Gauge `momentum_market_events_ingest_max_wall_lag_ms_last_batch`, `momentum_bot_process_start_unix_seconds`. (4) `market_events_momentum_fanout_published_total` in market-data. (5) systemd: `KillSignal=SIGINT` — `tokio::signal::ctrl_c` für Flush; **kein** zweiter SIGTERM-Arm im selben `select!` (rustc Never-Typ-Inference mit Pin/`recv` in diesem Binary).  
**Dateien**: `src/bin/momentum_bot.rs`, `src/bin/market_data.rs`, `src/metrics.rs`, `docs/systemd/momentum-bot.service`, `docs/BUGS_FIXES.md`

### FIX-MOM-CORE-SLOT-METRICS-JETSTREAM-2: `last_applied_slot`-Gauge nur Core-NATS (Follow-up Bugbot)
**Datum**: 2026-05-16  
**Problem**: Nach Entfernen von `record_momentum_market_events_subscription_max_dequeued_slot` im JetStream-Wallet-Arm aktualisierte `process_market_event` weiterhin global `record_momentum_market_events_last_applied_slot`; JetStream-`WalletBalanceSnapshot`-Slots konnten die Gauge vorziehen und `momentum_market_events_internal_slot_delta_slots` gegenüber reinem Core-`max_dequeued` wieder ~0 saturieren.  
**Fix**: Parameter `update_core_nats_subscription_slot_metrics` an `process_market_event`: Gauge nur bei Core-NATS-Ingest (`true`); JetStream Bootstrap + Live-Wallet mit `false`. `ctx.last_event_slot` / `last_event_ts_ms` unverändert pro Event.  
**Dateien**: `src/bin/momentum_bot.rs`, `src/metrics.rs`, `docs/BUGS_FIXES.md`

### MOM-OBS-LATENCY: Prometheus-Histogramme für Momentum-Hot-Path- und E2E-Latenzen
**Datum**: 2026-05-15  
**Zweck** (kein Bugfix): Operative Messbarkeit nach Throughput-Slices — `momentum_event_to_ingest_ms`, `momentum_event_to_intent_publish_ms` (nur mit kausalem `MarketEvent.ts_unix_ms`; Exit-Pfade setzen `source_event_ts_unix_ms` explizit), interne µs-Histogramme für `process_market_event`, `record_trade`, Signal-Eval (dirty vs. Full-Scan), NATS-Batch-Deserialize/Flatten; Counter `momentum_latency_event_ts_invalid_total` bei `ts_unix_ms==0` oder Werte in der „Zukunft“ vs. lokaler Wanduhr. **`momentum_event_to_ingest_ms`**: nur Live-Ingest aus dem Core-NATS-MarketEvents-Arm — **kein** JetStream-Wallet-Snapshot-Bootstrap (`bootstrap_wallet_snapshot_from_jetstream`), damit historische Replay-Timestamps die SLO nicht verfälschen. Keine Strategie-/RPC-/Topic-Änderungen.  
**Follow-up (PIPELINE-LATENCY-METRICS, 2026-05-19)**: Zusätzlich `momentum_intent_header_to_publish_ms_*` und `momentum_publish_to_intent_ms_*` bei erfolgreichem JetStream-Publish von BUY/SELL; segmentierte market-data-/execution-Histogramme siehe **PIPELINE-LATENCY-METRICS**.  
**Dateien**: `src/metrics.rs`, `src/bin/momentum_bot.rs`

### PIPELINE-LATENCY-METRICS: segmentierte Histogramme market-data → momentum → execution
**Datum**: 2026-05-19  
**Zweck** (Observability only): Kette A–I in `docs/RUNBOOK_PROD.md` — Geyser→Core-Publish (market-data), Momentum-Ingest und Intent-Publish, execution `process_intent` bis Confirm sowie Slot-Lag am Send. Kein Trading-Verhalten, kein Hot-Path-RPC.  
**Dateien**: `src/metrics.rs`, `src/bin/market_data.rs`, `src/bin/momentum_bot.rs`, `src/bin/execution_engine.rs`, `docs/RUNBOOK_PROD.md`, `docs/BUGS_FIXES.md`

### INGEST-LAG-METRICS: Geyser-Listener vs. market-data Broadcast (2026-05-20)
**Datum**: 2026-05-20  
**Zweck** (Observability only): Trennung von (a) Zeit in `tokio::sync::broadcast` + Event-Loop-Scheduling zwischen Geyser-Listener-`send` und market-data-`recv` und (b) Wall-Zeit `market_data_trade_after_bonding_publish_ms` (B★) sowie (c) reiner Ketten-Abstand `market_data_bonding_to_trade_slot_delta_slots` (Geyser-Slots, I-16). **Kein** `getSlot`/`getBlockTime`/`getTransaction` pro Event; nur `Instant` und bestehende `slot`-Felder.  
**Metriken**: `market_data_tx_channel_lag_ms`, `market_data_account_channel_lag_ms`, `market_data_tx_broadcast_lagged_total`, `market_data_account_broadcast_lagged_total`, `market_data_bonding_to_trade_slot_delta_slots`, `market_data_tx_broadcast_queue_depth`, `market_data_account_broadcast_queue_depth` (Account-Fairness).  
**Dateien**: `src/solana/geyser_listener.rs`, `src/bin/market_data.rs`, `src/metrics.rs`, `docs/RUNBOOK_PROD.md`, `docs/BUGS_FIXES.md`  
**Follow-up**: Fairness umgesetzt in **MARKET-DATA-TX-INGEST-FAIRNESS** (dedizierter Tx-Ingest-Task) und **MARKET-DATA-ACCOUNT-INGEST-FAIRNESS** (dedizierter Account-Ingest-Task).

### MARKET-DATA-TX-INGEST-FAIRNESS: dedizierter Geyser-Tx-Consumer (Option A)
**Datum**: 2026-05-20  
**Problem**: `market_data_tx_channel_lag_ms` p50/p99 extrem hoch trotz ~0,5 ms `market_data_geyser_to_publish_ms_trade` p50 — `transaction_rx.recv()` im gemeinsamen `tokio::select!` kam zu selten, wenn der Account-Arm lange arbeitete (Backlog im `broadcast`-Buffer; MATRIX-Muster: kleines Slot-Delta + große Wall-Lag).  
**Fix**: **Option A** — `tokio::spawn` eines dedizierten Tasks, der ausschließlich `transaction_rx.recv()` schleift und die bisherige Tx-Verarbeitung in `handle_geyser_transaction` ausführt; Haupt-`select!` ohne Tx-Arm. Bei `RecvError::Closed` beendet ein `watch`-Signal die Geyser-Schleife (`listener_handle.abort()`). Neue Gauge `market_data_tx_broadcast_queue_depth` (Restlänge im Tx-`broadcast`-Receiver nach jedem `recv`). Keine neuen RPC-Calls, keine NATS-Schema-Änderungen.  
**Dateien**: `src/bin/market_data.rs`, `src/metrics.rs`, `docs/RUNBOOK_PROD.md`, `docs/BUGS_FIXES.md`

### MARKET-DATA-ACCOUNT-INGEST-FAIRNESS: dedizierter Geyser-Account-Consumer (Option A)
**Datum**: 2026-05-20  
**Problem**: `market_data_account_channel_lag_ms` p50/p99 hoch trotz schnellem Publish (`market_data_geyser_to_publish_ms_*`) — `account_rx.recv()` im gemeinsamen `tokio::select!` kam zu selten, wenn JetStream-, Control- und andere Arme lange arbeiteten (Backlog im Account-`broadcast`-Buffer; B★ groß bei kleinem Slot-Delta).  
**Fix**: **Option A** — `tokio::spawn` eines dedizierten Tasks, der ausschließlich `account_rx.recv()` schleift und die bisherige Account-Verarbeitung in `handle_geyser_account` ausführt; Haupt-`select!` ohne Account-Arm. Bei `RecvError::Closed` beendet ein zweites `watch`-Signal die Geyser-Schleife (`listener_handle.abort()`). Neue Gauge `market_data_account_broadcast_queue_depth` (Restlänge nach jedem `recv`). Zusätzlich: `parking_lot`-Guards so begrenzt, dass der Account-Ingest-Task `Send` bleibt (keine Guards über `.await`). Keine neuen RPC-Calls im Hot-Path, keine NATS-Schema-Änderungen.  
**Dateien**: `src/bin/market_data.rs`, `src/metrics.rs`, `docs/RUNBOOK_PROD.md`, `docs/BUGS_FIXES.md`

### FIX-MOM-EXIT-QUOTE-GUARDS: Frische Reserve-Quotes + getrennte Ingest-Latenzen
**Datum**: 2026-05-15  
**Problem**: `momentum_event_to_ingest_ms` wirkte extrem hoch, ohne klare Trennung von JetStream-`PoolCacheUpdate`-Timestamps vs. Core-NATS; price-based Exits konnten theoretisch auf sehr alten Cache-Zeilen oder nicht verifizierten SOL-Paaren basieren.  
**Fix**: Harte Guards für price-based Exits (`cache_age_ms` ≤ 4s, Slot vs. `entry_confirmed_slot`/`last_price_slot`, Token-Program-Pseudopool-Adressen, optionale DexPoolAccounts-WSOL+Mint-Prüfung in `executable_exit_quote`); strukturierte `momentum_exit_price_decision`-Logs (allow/suppress/skip); neues Histogramm `momentum_jetstream_poolcache_event_to_ingest_ms` parallel zu `momentum_event_to_ingest_ms` (nur Core NATS).  
**Dateien**: `src/bin/momentum_bot.rs`, `src/metrics.rs`, `docs/BUGS_FIXES.md`

### FIX-01: Revert fehlerhafter Commits → `e341c04b`
**Datum**: 2026-02-09
**Problem**: 18 Commits (bis `b22bb0a9`) hatten ungewollt die Liquidation zerstört und Architekturprinzipien verletzt (RPC-Calls im Hot Path).
**Fix**: Hard-Reset auf `e341c04b`, danach selektive Re-Integration.

### FIX-02: Multi-DEX Retry-Pfad für Liquidation
**Datum**: 2026-02-09
**Problem**: Bei `BondingCurveComplete` (Error 6005) wurde nur ein DEX versucht. Zweiter Token wurde nicht über PumpSwap AMM probiert.
**Fix**: Liquidation versucht jetzt Multi-Pool zuerst, PumpFun als Fallback. Alle DEXes werden durchprobiert.

### FIX-44: 6005-Retry-Mechanismus in Liquidation (ARCHITECTURE_AUDIT A.4)
**Datum**: 2026-02-25
**Problem**: Wenn eine Liquidation-Sell-Route über PumpFun Bonding Curve gewählt wurde und die Simulation mit 6005 (BondingCurveComplete) fehlschlägt, wurde der Intent verworfen statt mit PumpSwap AMM zu retrien.
**Fix**: Automatischer Retry: Bei Sim-Fail mit 6005 und dex=pumpfun wird `mark_pumpfun_complete_for_mint` gesetzt, eine frische PumpSwap-Quote geholt und ein neuer Intent mit pump_amm erstellt und verarbeitet. Library-Funktion `ironcrab::execution::error_detection::is_6005_bonding_curve_complete` für Fehlererkennung.

### FIX-03: Grafana Liquidation als "buy" angezeigt
**Datum**: 2026-02-09
**Problem**: Liquidation-Sells wurden im Dashboard als "buy" klassifiziert.
**Fix**: `side`-Feld korrekt in ExecutionResult Metadata gesetzt.

### FIX-04: PnL-Berechnung >100% Verlust
**Datum**: 2026-02-09
**Problem**: PnL zeigte >100% Verlust für erfolgreich verkaufte Tokens.
**Fix**: PnL-Berechnung in `trades_server.py` korrigiert (Division durch korrekte Basis).

### FIX-05: Geyser Reconnect bei neuer ATA
**Datum**: 2026-02-11
**Problem**: Jedes Mal wenn ein Token gekauft wurde und eine neue ATA erstellt wurde, musste der gesamte Geyser-Stream reconnecten (`subscribe_once()`).
**Fix**: Migration von `subscribe_once()` zu `subscribe_with_request()` + `SinkExt` in 3 Modulen. Neue ATAs werden dynamisch hinzugefügt ohne Stream-Reconnect.

### FIX-06: Bonding Curve Exit Signal für Momentum-Bot
**Datum**: 2026-02-11
**Problem**: Kein Exit-Signal basierend auf Bonding-Curve-Fortschritt. Tokens konnten nicht automatisch verkauft werden bevor die Curve migriert.
**Fix**: Neuer konfigurierbarer `bonding_curve_exit_threshold` (Default 98%) mit Hot-Reload via UI. Basiert auf Geyser-Daten, keine RPC-Calls.

### FIX-07: Grafana Dashboard — Run-basierte Trades & 24h PnL
**Datum**: 2026-02-11
**Problem**: Dashboard zeigte nur 20 Trades; kein 24h PnL-Wert.
**Fix**: Alle Trades des aktuellen Runs + letzte 20 vom vorigen Run. Neue Panels für Wallet-Delta und Realized PnL.

### FIX-08: WALLET_TOTAL_SOL_LAMPARTS Metrik (Locked Capital)
**Datum**: 2026-02-11
**Problem**: Metrik enthielt nur unlocked SOL, nicht das in Trades gebundene Kapital.
**Fix**: `total_sol()` + `wsol_balance()` aus LockManager statt nur `available_sol()`.

### FIX-09: Sensitive Credentials in Version Control
**Datum**: 2026-02-11
**Problem**: Server-IP, Username und Port in `.github/copilot-instructions.md`.
**Fix**: Credentials durch Platzhalter ersetzt.

### FIX-10: WsolManager Konsistenz (LockManager.available_wsol)
**Datum**: 2026-02-12
**Problem**: `fetch_and_update_balances()` aktualisierte Prometheus-Gauge aber nicht `LockManager.available_wsol`.
**Fix**: WSOL-Updates werden konsistent über LockManager propagiert.

### FIX-11: WsolManager RPC-Fallback entfernt
**Datum**: 2026-02-12
**Problem**: 60s RPC-Polling im NATS-Modus verletzte Geyser-First-Architektur.
**Fix**: RPC-Fallback und Polling-Only-Modus entfernt. WsolManager arbeitet vollständig über NATS/Geyser-Events.

### FIX-12: Doppelter JetStream Consumer in execution-engine
**Datum**: 2026-02-12
**Problem**: `execution_engine` erstellte zwei separate ephemere JetStream-Consumer für `POOL_CACHE` Updates → Race Conditions, verpasste Updates, Delays.
**Fix**: Einzelner Consumer wird wiederverwendet für Bootstrap und Runtime.
**Commit**: `a29ecfb6`

### FIX-13: RPC-Fallback für Creator bei Liquidation
**Datum**: 2026-02-12
**Problem**: PumpFun-Liquidation scheiterte wenn Creator nicht im LivePoolCache war.
**Fix**: Bei Liquidation wird Creator per RPC nachgeladen falls nicht im Cache (Cold Path — architekturkonform).
**Commit**: `a29ecfb6`

### FIX-14: Ghost Positions durch stale JetStream Snapshots
**Datum**: 2026-02-12
**Problem**: `MAX_BOOTSTRAP_MINTS=30` begrenzte den Bootstrap. Stale non-zero JetStream-Einträge blieben bestehen → falsche Open-Position-Anzeige.
**Fix**: Step 2.5 in market-data Bootstrap: Alle verbleibenden JetStream-Einträge werden enumeriert, zero-balance Overrides für nicht abgedeckte non-zero Mints publiziert.
**Commit**: `43941752`

### FIX-15: Hardcoded quote_mint in DEX-Parsern (Bug H)
**Datum**: 2026-02-12
**Problem**: `parse_meteora_transaction()`, `parse_raydium_cpmm_transaction()` und `parse_raydium_v4_swap()` setzten `quote_mint = SOL_MINT_PUBKEY` hardcoded → false Arbitrage-Signale für non-SOL Pairs.
**Fix**: Dynamische quote_mint-Extraktion aus Transaction-Token-Balances.
**Commit**: `0b1b724e`

### FIX-16: Initiales WalletBalanceUpdate bei market-data Startup
**Datum**: 2026-02-13
**Problem**: execution-engine startete mit Default 1.0 SOL und wurde erst beim ersten Geyser-Event aktualisiert → falsche WSOL/SOL-Anzeige, kein Wrapping.
**Fix**: market-data publiziert beim Bootstrap ein initiales `WalletBalanceUpdate` mit SOL+WSOL-Balances.
**Commit**: `c1e8d667`

### FIX-17: fill_in/fill_out Accuracy (False Take-Profit Triggers)
**Datum**: 2026-02-13
**Schweregrad**: CRITICAL
**Problem**: Bei BUY mit `lamport_noise=true` fiel `fill_in` auf `intent.required_capital` zurück → bis zu 29x falsch. SELL `fill_out` war immer `None` bei ATA-Lifecycle. Falsche entry_price → falsche Take-Profit/Stop-Loss Entscheidungen.
**Fix**: Dreistufige Fallback-Kette: (1) Inner-Instruction-Parsing für System.transfer, (2) Rent-Adjusted Lamport Delta, (3) intent capital als letzter Ausweg. Dashboard PnL konsistent auf wallet_delta umgestellt.
**Dateien**: `src/bin/execution_engine.rs`, `scripts/trades_server.py`

### FIX-SCOPE1: Momentum Preisupdates Slot-monoton (plan_momentum_price_integrity Scope 1)
**Datum**: 2026-05-10
**Problem**: Alte Trades (niedriger Geyser-Slot) konnten nach BUY-Bestätigung verarbeitet werden und `current_price`/`tokens_per_sol` für offene Positionen verfälschen → Schein-TAKE_PROFIT bei real negativem PnL (Event-Zeit vs. Slot-Reihenfolge).
**Fix**: `ExecutionResult.confirmed_slot` + Metadata-Spiegel wird beim Confirm gesetzt (Bundle-/Geyser-/RPC-Slot). `PositionTracker` erhält `entry_confirmed_slot` und `last_price_slot`; `update_position_price` akzeptiert nur Updates mit `source_slot > entry_confirmed_slot` und strikt monoton steigend; Pool-Match (I-13) unverändert. Trade- und PoolCache-Pfade reichen Geyser-Slot durch.
**Dateien**: `src/bin/execution_engine.rs`, `src/bin/momentum_bot.rs`

### FIX-SCOPE-B: Momentum „sticky latest state“ (Lifecycle Scope B, non-bonding)
**Datum**: 2026-05-10
**Problem**: Neben `BondingCurveProgress` (Scope A) konnten weitere Geyser-/JetStream-Zustände (PumpFun-Migration, reserve-basierte Pool-Marks, `TokenMintInfo`) vor Positionserstellung ankommen und fehlten bei späterem BUY-open, Orphan-Recovery oder Wallet-Reconcile — gleiche Klasse Race wie beim Bonding-Snapshot.
**Fix**: Slot-/ts-monotone Maps für PumpFun-Migration (`complete`) und reserve-basierte `(mint, pool)`-Preis-Hints aus `PoolCacheUpdate`; `live_cache_pumpfun_complete_evidence` berücksichtigt die Migration-Sticky-Map; gemeinsamer `apply_latest_sticky_state_to_position` nach `open_position`, Reconcile-Pfaden, `TokenMintInfo`, plus bestehende PoolCache- und 6005-Pfade schreiben die Sticky-Maps; I-13 und Scope-1-Slot-Gates beim Apply; **`close_position` räumt Sticky nur ohne pending BUY** (analog `latest_bonding_by_mint`). **Reserve-Sticky und Live-PoolCache-Preisupdate nur wenn genau eine Mint-Seite WSOL ist** — keine Marks aus Token/Token-Paaren (I-14 / non-SOL-Quote).
**Dateien**: `src/bin/momentum_bot.rs`

### FIX-18: Bug B — Orphaned Buy Recovery
**Datum**: 2026-02-13
**Problem**: Race Condition: `cleanup_stale_pending()` entfernte pending intent bevor `ExecutionResult` ankam → Position nie erstellt → kein Sell.
**Fix**: Orphaned Buy Recovery: Wenn confirmed BUY ohne pending intent → Position aus ExecutionResult + TokenTracker rekonstruieren.
**Dateien**: `src/bin/momentum_bot.rs`

### FIX-19: Bug C — Sell-Retry nach Failure/Timeout
**Datum**: 2026-02-13
**Problem**: `exit_generated` wurde bei Sell-Failures nicht zurückgesetzt → kein Retry bis `max_hold_time` + `reconcile_timed_exits()`.
**Fix**: Unconditional Reset von `exit_generated` in Failed/Timeout Handlern für Sell-Side. Gilt für normalen und orphaned Pfad.
**Dateien**: `src/bin/momentum_bot.rs`

### FIX-23: PumpSwap AMM Geyser-First Discovery (P1 Cherry-Pick)
**Datum**: 2026-02-14
**Problem**: `discover_pool_static()` ging direkt zu RPC (`getProgramAccounts` + `getMultipleAccounts`) — ~500-3000ms Latenz — obwohl der LivePoolCache bereits alle 14 Pool-Accounts hatte.
**Fix**: LivePoolCache-Check am Anfang von `discover_pool_static()`: Konstruiert `PumpAmmPoolStatic` direkt aus den 14 gecachten `pool_accounts` (ZERO RPC). RPC-Fallback bleibt für uncached Pools. Zusätzlich: Bounds-Check in `pool_accounts_v1_for_base_mint()` von `>= 12` auf `>= 14` korrigiert (verhindert Out-of-Bounds Panics).
**Dateien**: `src/solana/dex/pumpfun_amm.rs`

### FIX-PNL: CRITICAL — Invertierte PnL-Formel (tokens_per_sol)
**Datum**: 2026-02-14
**Schweregrad**: CRITICAL — Alle Exit-Signale (Take-Profit, Stop-Loss, Drawdown) waren invertiert
**Problem**: `pnl_pct()`, `update_price()`, `drawdown_from_ath_pct()` und `add_investment()` in `momentum_bot.rs` verwendeten Formeln für `SOL_per_token` Preise, obwohl intern `tokens_per_SOL` Preise verwendet werden (höherer Wert = billigerer Token). Folge:
- Take-Profit feuerte bei Verlusten, Stop-Loss bei Gewinnen
- `highest_price` trackte den teuersten Preis (höchster `tokens_per_sol` = billigster Token) statt den besten Preis für den Holder
- Server-Log-Evidenz: "TAKE_PROFIT +167%" bei tatsächlich -20% PnL

**Fix**:
- `pnl_pct()`: `((current - entry) / entry) * 100` → `((entry / current) - 1) * 100`
- `update_price()`: `if new > highest` → `if new < highest` (niedrigster tokens_per_sol = bester Preis)
- `drawdown_from_ath_pct()`: `((highest - current) / highest) * 100` → `((current / highest) - 1) * 100`
- `add_investment()`: `.max()` → `.min()` für highest_price Tracking
**Dateien**: `src/bin/momentum_bot.rs`
**Commit**: `31b0d56c`

### FIX-24: Ghost Open Positions + Wallet Balance Bootstrap
**Datum**: 2026-02-17
**Problem A — Ghost Open Positions**: Nach einem Neustart zeigte `OPEN_POSITIONS_GAUGE` 8-10 statt der tatsächlich offenen Positionen (~1). Bootstrap las stale JetStream Wallet-Snapshots mit Non-Zero-Balances für bereits verkaufte Tokens → `open_positions = 8`. Geyser korrigierte die Balances auf 0 in LockManager, aber `open_positions` wurde NUR bei bestätigten BUY/SELL Trades aktualisiert, nicht bei Balance-Transitionen via Geyser.

**Problem B — Wallet Balance Start = 1.0 SOL**: `initial_sol_lamports` CLI-Argument hatte Default 1 SOL (hardcoded). Die initiale `WalletBalanceUpdate` von market-data ging über Core NATS (fire-and-forget), execution-engine hatte ggf. noch nicht subscribed → Message verloren.

**Problem C — Doppelter JetStream Consumer**: Zwei unabhängige Consumers für `WALLET_SNAPSHOT`-Stream: Background-Task (tokio::spawn) + Main-Loop-Handler. Redundant.

**Fix**:
1. Main-Loop Balance-Transitionen: `non-zero → 0` decrementiert `open_positions`, `0 → non-zero` inkrementiert
2. SOL/WSOL JetStream Bootstrap: `bootstrap_token_balances_from_wallet_snapshot()` liest jetzt auch SOL (`NATIVE_SOL` Sentinel) und WSOL aus JetStream
3. market-data publiziert SOL/WSOL als WalletBalanceSnapshot zu JetStream (persistent, kein fire-and-forget)
4. Redundanten Background-Task entfernt (single consumer im Main-Loop)
**Dateien**: `src/bin/execution_engine.rs`, `src/bin/market_data.rs`
**Commits**: `5b359806`, `e5b2a0eb`

### FIX-26: Market-Data WSOL-Seeding & Pool-Propagation (Rest)
**Datum**: 2026-02-18
**Problem**: Nach FIX-16 + FIX-24 waren noch 4 Punkte aus dem P2 Cherry-Pick "WSOL-Seeding & Pool-Propagation" offen:

1. **SELL → JetStream zero-balance + ATA Untracking**: Geyser liefert keine Updates für geschlossene/gelöschte Token Accounts. Bei einem confirmed SELL mit `close_token_ata=true` blieb ein staler non-zero JetStream-Eintrag bestehen → Ghost Positions nach Restart.
2. **PumpAmm `pool_accounts` an `creator` gekoppelt**: `PoolCacheUpdate` Metadata für PumpAmm wurde nur propagiert wenn `creator` vorhanden war. Downstream (SLAVE Caches) benötigen `pool_accounts` unabhängig vom Creator.
3. **Kein MASTER LivePoolCache Fallback**: Wenn Geyser-Parse leere `pool_accounts` lieferte, gab es keinen Fallback auf den MASTER Cache.
4. **Stille `continue` bei fehlender Metadata**: Fehlende `token_account`/`token_program` in `ExecutionResult` wurde ohne Warnung übersprungen.

**Fix**:
1. Im ExecutionResult-Handler: Bei confirmed SELL → `WalletBalanceSnapshot(balance_raw: 0)` an JetStream + Core NATS publizieren + ATA aus Geyser-Tracking entfernen
2. PumpAmm PoolCacheUpdate Metadata: `pool_accounts` und `creator` jetzt unabhängig voneinander propagiert
3. DexPoolAccounts → MASTER LivePoolCache populieren; PoolCacheUpdate-Builder nutzt Fallback auf MASTER Cache via `get_pump_amm_pool_accounts()`
4. `warn!` Logs bei fehlender `token_account`/`token_program` Metadata

**Dateien**: `src/bin/market_data.rs`, `src/execution/live_pool_cache.rs`

### FIX-29: Audit-B — Raydium RPC-Elimination (Serum-Account Caching)
**Datum**: 2026-02-18
**Problem**: Raydium Swaps hatten zwei RPC-Bottlenecks im Hot Path:
1. `load_pool_from_geyser()` mit 20 RPC-Retries × 500ms = bis zu 10s Latenz
2. `fetch_and_populate_serum_accounts()` wurde **immer** aufgerufen (auch bei Cache-Hit), obwohl Serum/OpenBook Accounts statisch sind

**Root Cause**: Serum/OpenBook Market Accounts (bids, asks, event_queue) wurden nie gecacht. Jeder Raydium Trade brauchte einen RPC-Call für diese statischen Daten.

**Fix**:
1. `live_pool_cache.rs`: `set_raydium_serum_accounts()` Methode hinzugefügt
2. `pool_cache_sync.rs`: Serum-Metadata (`serum_bids`, `serum_asks`, `serum_event_queue`, `market_id`) aus PoolCacheUpdate parsen
3. `tx_builder.rs`: Serum-Accounts aus Cache nutzen, RPC nur bei Cache-Miss. Nach RPC-Fetch in SLAVE Cache zurückschreiben
4. `market_data.rs`: Einmaliger Cold Path RPC pro Pool via `tokio::spawn`, Ergebnis in MASTER Cache + PoolCacheUpdate Metadata propagiert. `raydium_serum_fetched` Set verhindert doppelte Fetches
5. `raydium.rs`: Retry-Count 20→3, Delay 500→300ms. Max Latenz 10s→0.9s

**Ergebnis**: Raydium Swaps auf bekannten Pools: **ZERO RPC-Calls im Hot Path**. Neue Pools: 1x RPC in market-data (Cold Path), danach gecacht und via NATS propagiert.
**Dateien**: `src/execution/live_pool_cache.rs`, `src/execution/pool_cache_sync.rs`, `src/execution/tx_builder.rs`, `src/bin/market_data.rs`, `src/solana/dex/raydium.rs`

### FIX-28: TX-Builder Cache-capped min_out (P2 Cherry-Pick)
**Datum**: 2026-02-18
**Problem**: Bei PumpFun BUY-Transaktionen wurde `min_out` aus dem TradeIntent direkt übernommen, ohne gegen einen frischen Cache-Quote zu prüfen. Wenn sich die Bonding Curve zwischen Intent-Erstellung und TX-Build verschob (schnelle Kursbewegungen), war `min_out` zu hoch → Error 6002 ("Too much SOL required" / SlippageExceeded) on-chain.
**Root Cause**: `build_tx_plan()` nutzte `min_out` aus dem Intent ohne Capping. Die `calculate_fresh_min_out()` Infrastruktur existierte bereits, wurde aber nur aufgerufen wenn `min_out` im Intent **fehlte**.
**Fix**: Beide Werte (Intent + Cache) werden berechnet. Bei zwei vorhandenen Werten wird das Minimum (konservativere) verwendet. Logging zeigt das Delta in Prozent wenn gecappt wird. Kein Fallback geändert — wenn nur ein Wert verfügbar ist, wird er wie bisher verwendet.
**Datei**: `src/execution/tx_builder.rs`

### FIX-27: Fehlende `[[bin]]` Einträge in Cargo.toml
**Datum**: 2026-02-18
**Problem**: `autobins = false` in `Cargo.toml` deaktiviert automatische Binary-Erkennung. `src/bin/burn_manual_keyless.rs` und `src/bin/manual_swap.rs` hatten gültige `#[tokio::main] async fn main()` Funktionen, waren aber nicht in den `[[bin]]` Sektionen deklariert → konnten nicht gebaut werden.
**Fix**: `[[bin]]` Einträge für `burn-manual-keyless` und `manual-swap` hinzugefügt.
**Datei**: `Cargo.toml`

### FIX-25: DEX-Normalisierung + Creator-Scope (P2 Cherry-Pick)
**Datum**: 2026-02-18
**Problem 1 — Nicht-kanonische DEX-Namen**: Drei separate `DexType` Enums mit unterschiedlichen `to_string()` Outputs. `arbitrage/types.rs` produzierte `"raydium_amm_v4"` (statt `"raydium"`) und `"pump_swap_amm"` (statt `"pump_amm"`). Consumer-Code hatte ~25 defensive Multi-Varianten-Checks für Varianten die nie ankamen.

**Problem 2 — Creator für pump_amm erzwungen**: `momentum_bot.rs` erzwang Creator für `pump_amm`/`pumpswap`/`PumpFunAmm` bei BUY und SELL Intents (`ok_or_else → Error`), obwohl `tx_builder.rs` den Creator NUR für `pumpfun` (Bonding Curve) verwendet. PumpSwap AMM nutzt `pool_accounts` (14 Accounts), nicht den Creator.

**Fix**:
1. `arbitrage/types.rs`: `as_str()` auf kanonische Namen korrigiert (`"raydium"`, `"pump_amm"`)
2. `market_data.rs`: Hardcoded `"pump_amm"` → `DexType::PumpFunAmm.to_string()`
3. `momentum_bot.rs`: SELL Creator-Scope korrigiert — Creator nur für `"pumpfun"` Pflicht, optional für andere DEXes
4. `ipc/schema.rs`: Doc-Kommentar aktualisiert
5. Viele Consumer-Bereinigungen waren bereits in früheren Fixes erfolgt (execution_engine, cross_dex_handler, tx_builder, pool_cache_sync)
**Dateien**: `src/arbitrage/types.rs`, `src/bin/market_data.rs`, `src/bin/momentum_bot.rs`, `src/ipc/schema.rs`

---

## 2. BEKANNTE OFFENE ISSUES

### ISSUE-1: `duplicate field slot` Deserialisierungsfehler
**Schweregrad**: NIEDRIG (Warn-Level, verhindert einzelne Events)
**Symptom**: `Failed to deserialize MarketEvent error=duplicate field 'slot' at line 1 column 295` in momentum-bot und arb-strategy.
**Root Cause**: `MarketEvent` hat `slot: Option<u64>` auf Top-Level UND `#[serde(flatten)]` auf `kind: MarketEventKind`. `LatestBlockhash { slot: u64 }` kollidierte mit dem Top-Level `slot` beim Serialisieren.
**Fix**: `#[serde(rename = "blockhash_slot")]` auf `LatestBlockhash.slot` — JSON-Feld heißt jetzt `"blockhash_slot"`, Rust-Feld bleibt `slot`.
**Status**: ✅ BEHOBEN
**Datei**: `src/ipc/schema.rs`

### ISSUE-2: DEX-Name Inkonsistenz + Creator für pump_amm unnötig (P2 Cherry-Pick)
**Schweregrad**: MITTEL — Führt zu Ad-hoc-Workarounds, potentiellen Routing-Fehlern und unnötigen Creator-Lookups
**Status**: ✅ BEHOBEN — FIX-25

### ISSUE-3: Fehlende automatische Retention für trade_logs (market_events)
**Schweregrad**: HOCH — Produktionsvorfall 2026-02-21: Root-Partition 100% voll, Deploy fehlgeschlagen
**Symptom**: `market_events-YYYYMMDD.jsonl` wachsen ungebremst (~85–110 GB/Tag). Ohne Rotation füllt sich die Disk.
**Root Cause**: STORAGE_CONVENTIONS definiert 7–30 Tage Retention, aber es existiert **kein Janitor/Cron** für automatische Löschung. Rotation läuft nicht asynchron.
**Status**: ⏳ OFFEN — Manuelle Bereinigung durchgeführt (48 alte Dateien gelöscht, ~2,8 TB freigegeben). Automatische Retention noch einzuführen.
**Fix**: Cron/Janitor implementieren: JSONL-Dateien in `trade_logs/market_events/`, `trade_logs/intents/`, etc. älter als 7–14 Tage löschen. Asynchron, außerhalb Hot Path.
**Referenz**: STORAGE_CONVENTIONS.md §5 Retention

---

#### Analyse

**Root Cause**: Drei separate `DexType` Enums mit unterschiedlichen String-Outputs:

| Enum | Datei | Raydium V4 | PumpSwap AMM |
|------|-------|------------|--------------|
| `dex_parser::DexType` | `dex_parser.rs:118` | `"raydium"` ✅ | `"pump_amm"` ✅ |
| `geyser_pool_discovery::DexType` | `geyser_pool_discovery.rs:707` | `"raydium"` ✅ | (kein PumpFunAmm) |
| `arbitrage::types::DexType` | `arbitrage/types.rs:23` | `"raydium_amm_v4"` ❌ | `"pump_swap_amm"` ❌ |

Die Quellen (`market-data` via `dex_parser::DexType::to_string()`) produzieren **bereits korrekte kanonische Namen**. Das Chaos entsteht durch:

1. **`arbitrage/types.rs` ist eine QUELLE nicht-kanonischer Namen** — `as_str()` (Z.52) gibt `"raydium_amm_v4"` statt `"raydium"` zurück, (Z.57) gibt `"pump_swap_amm"` statt `"pump_amm"` zurück
2. **Defensive Multi-Varianten-Checks im momentum_bot** — akzeptieren Varianten die nie ankommen (`"pumpswap"`, `"PumpFunAmm"`, `"pumpfunamm"`, `"pump-amm"`, `"meteora-dlmm"`, `"meteoradlmm"`)
3. **`contains("pump")` Wildcard-Match** — in `execution_engine.rs:1537` und `cross_dex_handler.rs:114` (unsauber)
4. **Creator fälschlich für pump_amm erzwungen** — `momentum_bot.rs` Z.5826-5829 und Z.6901-6904 erzwingen Creator für `pump_amm`, obwohl `tx_builder.rs` ihn NUR für `pumpfun` benötigt

**Alle 25+ nicht-kanonischen Referenzen** (gruppiert):

| Datei | Zeile(n) | Nicht-kanonische Varianten | Art |
|-------|----------|---------------------------|-----|
| `arbitrage/types.rs` | 37 | `"raydium_amm"`, `"raydium_amm_v4"` | Consumer (from_str) |
| `arbitrage/types.rs` | 39 | `"orca_whirlpool"` | Consumer (from_str) |
| `arbitrage/types.rs` | 40 | `"meteora"` | Consumer (from_str) |
| `arbitrage/types.rs` | 42 | `"pumpswap"`, `"pump_swap_amm"` | Consumer (from_str) |
| `arbitrage/types.rs` | 52 | `"raydium_amm_v4"` | **SOURCE** (as_str) |
| `arbitrage/types.rs` | 57 | `"pump_swap_amm"` | **SOURCE** (as_str) |
| `momentum_bot.rs` | 2251-2257 | `"pumpfunamm"`, `"pumpswap"`, `"pump-amm"`, `"meteora-dlmm"`, `"meteoradlmm"` | Consumer |
| `momentum_bot.rs` | 2285-2288 | `"pumpfunamm"`, `"pumpswap"`, `"pump-amm"` | Consumer |
| `momentum_bot.rs` | 5683-5686 | `"pumpfunamm"`, `"pumpswap"`, `"pump-amm"` | Consumer |
| `momentum_bot.rs` | 5827-5829 | `"pump_amm"`, `"pumpswap"`, `"PumpFunAmm"` (case-insensitive) | Consumer |
| `momentum_bot.rs` | 6679 | `"pump_amm"` (case-insensitive) | Consumer |
| `momentum_bot.rs` | 6902-6904 | `"pump_amm"`, `"pumpswap"`, `"PumpFunAmm"` (case-insensitive) | Consumer |
| `execution_engine.rs` | 1537 | `contains("pump")` | Consumer (Wildcard) |
| `execution_engine.rs` | 5070 | `"PumpFunAmm"` | Consumer |
| `cross_dex_handler.rs` | 114 | `contains("pump")` | Consumer (Wildcard) |
| `tx_builder.rs` | 1183 | `"raydium_amm"`, `"raydium_amm_v4"` | Consumer |
| `tx_builder.rs` | 1205 | `"orca_whirlpool"` | Consumer |
| `tx_builder.rs` | 1217 | `"meteora"` | Consumer |
| `pool_cache_sync.rs` | 57 | `"raydium_amm"` | Consumer |
| `market_data.rs` | 2832 | `"pump_amm"` (hardcoded statt DexType) | Source (korrekt aber inkonsistent) |

---

#### FIX-25 Plan: DEX-Normalisierung an der Quelle + Consumer-Bereinigung

**Ziel**: Alle Quellen produzieren kanonische Namen. Alle Consumer vergleichen nur mit kanonischen Namen. Kein neues Workaround-Modul — das Problem wird an der Wurzel behoben.

**Keine RPC-Calls. Keine neuen NATS Topics. Keine Architektur-Änderung.**

---

**Teil 1: Quellen korrigieren** — Nicht-kanonische SOURCES fixen

| Datei | Zeile | Alt | Neu |
|-------|-------|-----|-----|
| `arbitrage/types.rs` | 52 | `Self::RaydiumAmmV4 => "raydium_amm_v4"` | `Self::RaydiumAmmV4 => "raydium"` |
| `arbitrage/types.rs` | 57 | `Self::PumpSwapAmm => "pump_swap_amm"` | `Self::PumpSwapAmm => "pump_amm"` |
| `market_data.rs` | 2832 | `dex: "pump_amm".to_string()` | `dex: DexType::PumpFunAmm.to_string()` |

**Risiko**: Minimal — Die Consumer akzeptieren bereits die kanonischen Namen.

---

**Teil 2: Consumer bereinigen** — Multi-Varianten-Checks durch einfache `==` ersetzen

Da alle Quellen nach Teil 1 kanonische Namen produzieren, können die defensiven Fallback-Varianten entfernt werden.

**`momentum_bot.rs`** (~6 Stellen):

| Zeile (ca.) | Alt | Neu |
|-------------|-----|-----|
| 2247-2258 | `dex_requires_pool_accounts()` mit `to_ascii_lowercase()` + 7 Varianten | `dex == "pump_amm" \|\| dex == "meteora_dlmm"` |
| 2284-2288 | `is_pump_amm` mit `to_ascii_lowercase()` + 4 Varianten | `dex == "pump_amm"` |
| 5682-5686 | `is_pump_amm` mit `to_ascii_lowercase()` + 4 Varianten | `dex == "pump_amm"` |
| 5826-5829 | `eq_ignore_ascii_case()` für 3 Varianten | `dex == "pumpfun"` (siehe Teil 3) |
| 6679 | `eq_ignore_ascii_case("pump_amm")` | `dex == "pump_amm"` |
| 6901-6904 | `eq_ignore_ascii_case()` für 3 Varianten | `dex == "pumpfun"` (siehe Teil 3) |

**`execution_engine.rs`** (2 Stellen):

| Zeile | Alt | Neu |
|-------|-----|-----|
| 1537 | `dex_lower.contains("pump") \|\| dex_lower == "pumpfun" \|\| dex_lower == "pump_amm"` | `dex == "pumpfun" \|\| dex == "pump_amm"` |
| 5070 | `dex == "pump_amm" \|\| dex == "PumpFunAmm"` | `dex == "pump_amm"` |

**`cross_dex_handler.rs`** (1 Stelle):

| Zeile | Alt | Neu |
|-------|-----|-----|
| 114 | `dex_lower.contains("pump") \|\| dex_lower == "pumpfun" \|\| dex_lower == "pump_amm"` | `dex == "pumpfun" \|\| dex == "pump_amm"` |

**`tx_builder.rs`** (3 Stellen):

| Zeile | Alt | Neu |
|-------|-----|-----|
| 1183 | `"raydium" \| "raydium_amm" \| "raydium_amm_v4"` | `"raydium"` |
| 1205 | `"orca" \| "orca_whirlpool"` | `"orca"` |
| 1217 | `"meteora_dlmm" \| "meteora"` | `"meteora_dlmm"` |

**`pool_cache_sync.rs`** (1 Stelle):

| Zeile | Alt | Neu |
|-------|-----|-----|
| 57 | `"raydium_amm" \| "raydium"` | `"raydium"` |

**`arbitrage/types.rs`** (from_str — hier bewusst Toleranz behalten):

Die `from_str()` Varianten (`"raydium_amm"`, `"pumpswap"`, etc.) können optional bleiben als Parsing-Toleranz für alte JetStream-Einträge. Da `as_str()` (Teil 1) jetzt kanonisch ist, fließen keine neuen nicht-kanonischen Strings mehr ins System.

---

**Teil 3: Creator-Scope korrigieren** (momentum_bot.rs — 2 Stellen)

**Problem**: Creator wird für `pump_amm` erzwungen (Error wenn fehlend), obwohl `tx_builder.rs` ihn NUR für `pumpfun` verwendet. PumpSwap AMM nutzt `pool_accounts` (14 Accounts), nicht den Creator.

**`generate_and_publish_entry_intent()`** (Z.~5826):
```rust
// ALT: Creator Pflicht für pumpfun + pump_amm + pumpswap + PumpFunAmm
if effective_dex == "pumpfun"
    || effective_dex.eq_ignore_ascii_case("pump_amm")
    || effective_dex.eq_ignore_ascii_case("pumpswap")
    || effective_dex.eq_ignore_ascii_case("PumpFunAmm")
{
    let creator = creator_opt.ok_or_else(|| ...)?;  // ERROR wenn fehlt
    intent.metadata.insert("creator", creator);
}

// NEU: Creator Pflicht NUR für pumpfun (Bonding Curve)
if effective_dex == "pumpfun" {
    let creator = creator_opt.ok_or_else(|| ...)?;
    intent.metadata.insert("creator".to_string(), creator);
} else if let Some(creator) = creator_opt {
    intent.metadata.insert("creator".to_string(), creator);
}
```

**`generate_and_publish_exit_intent()`** (Z.~6901): Identische Änderung.

---

**Zusammenfassung**:

| Datei | Änderungen | Risiko |
|-------|------------|--------|
| `src/arbitrage/types.rs` | `as_str()` auf kanonische Namen korrigieren | Minimal — Consumer akzeptieren bereits |
| `src/bin/market_data.rs` | Hardcoded String → `DexType::to_string()` | Minimal — selber Wert |
| `src/bin/momentum_bot.rs` | ~6 Multi-Varianten-Checks vereinfachen + Creator-Scope (2 Stellen) | Mittel — mechanisch |
| `src/bin/execution_engine.rs` | 2 Stellen: `contains("pump")` + `"PumpFunAmm"` entfernen | Niedrig |
| `src/solana/cross_dex_handler.rs` | 1 Stelle: `contains("pump")` entfernen | Niedrig |
| `src/execution/tx_builder.rs` | 3 Stellen: Fallback-Varianten entfernen | Niedrig |
| `src/execution/pool_cache_sync.rs` | 1 Stelle: `"raydium_amm"` Fallback entfernen | Niedrig |

**Kein neues Modul. Kein Workaround. Korrektur an der Wurzel.**

**Erwartete Wirkung**:
- Alle DEX-Quellen produzieren kanonische Namen → keine Normalisierung nötig
- ~25 nicht-kanonische Referenzen entfernt → Code lesbarer und wartbarer
- Creator nur noch für `pumpfun` (Bonding Curve) erzwungen
- `pump_amm` Intents funktionieren auch ohne Creator (aktueller Fehler behoben)
- `arb_strategy` erhält korrekte DEX-Namen (kein separater Fix nötig)

---

## 3. OFFENE BUGS (Analyse erforderlich / Fix ausstehend)

### BUG-A: PumpFun Custom(6023) — Intermittierende Sell-Fehler
**Schweregrad**: HOCH
**Betroffene Tokens** (2026-02-13 Run): `64HemTH7`, `34c3bPRz`
**Symptom**: Momentum-Bot Sell-Versuche scheitern wiederholt mit `Custom(6023)` ("NotEnoughTokensToSell"), obwohl Liquidation auf denselben Tokens erfolgreich ist.

**Root Cause (detailliert)**:

Drei zusammenwirkende Probleme verhindern erfolgreiche SELLs bei migrierten PumpFun-Tokens:

1. **`find_best_sell_pool()` ist DEX-agnostisch** — Die Pool-Auswahl im Momentum-Bot basiert nur auf `last_trade_ratio` und `last_updated`. Es gibt **keinen Check ob eine PumpFun Bonding Curve `complete=true`** ist. Wenn ein Token migriert wird, hat der PumpFun-Pool oft noch aktuelle Trade-Daten (von vor der Migration) und wird weiter als "bester" Pool ausgewählt.

2. **Kein Pool-Failure-Tracking** — Wenn ein SELL auf einem Pool scheitert, wird `exit_generated=false` zurückgesetzt (Bug-C Fix), aber der gescheiterte Pool wird **nicht markiert**. Beim nächsten Tick wählt `find_best_sell_pool()` denselben Pool erneut aus → endlose Wiederholung des selben Fehlers.

3. **Kein Multi-Pool-Fallback in der Execution Engine für normale SELLs** — Die Liquidation hat einen 3-Phasen-Routing-Pfad (Multi-Pool → LivePoolCache → PumpFun-Fallback). Normale SELL-Intents vom Momentum-Bot verwenden **nur** den vom Intent spezifizierten DEX. Wenn dieser scheitert, wird der Intent abgelehnt — kein automatischer Versuch mit alternativen DEXes.

**Warum Liquidation funktioniert**: Der `handle_sell_liquidation()`-Pfad probiert in Phase 1 zuerst PumpSwap AMM, Meteora, Raydium und Orca. Für migrierte Tokens findet er den PumpSwap-AMM-Pool und verkauft dort erfolgreich.

**Hinweis BUG-I**: Der Guard in `pumpfun.rs` (Zeile 888-902: `real_reserves == 0 && virtual_reserves > 0 → return Ok(None)`) **existiert bereits** im aktuellen Code. Der Architecture Audit Status "REGRESSION DURCH REVERT" ist **veraltet**. Dieser Guard fängt den Fall ab, wenn die Migration im Cache sichtbar ist. Bug-A tritt auf, wenn die Migration im Cache **noch nicht sichtbar** ist oder `real_token_reserves` nur stale (nicht 0) sind.

**Status**: ✅ BEHOBEN — FIX-20 (Pool-Migration & Failure-Tracking) + FIX-21 (Reserve-basiertes Quoting)

---

#### FIX-20 Plan: Bug-A — PumpFun Sell-Failure mit Pool-Migration & Failure-Tracking

**Ziel**: Momentum-Bot soll migrierte PumpFun-Pools automatisch meiden und bei wiederholten Sell-Fehlern auf alternative Pools wechseln.

**Keine RPC-Calls im Hot Path.** Alle Daten kommen aus Geyser/NATS Events.

---

**Teil 1: `PoolInfo` struct erweitern** (momentum_bot.rs)

```rust
struct PoolInfo {
    pool_address: String,
    dex: String,
    dex_pool_accounts: Option<Vec<String>>,
    first_seen_slot: u64,
    last_trade_slot: u64,
    last_trade_ratio: Option<f64>,
    last_updated: std::time::Instant,
    // --- NEU ---
    /// PumpFun bonding curve complete flag (None = nicht PumpFun oder unbekannt)
    bonding_curve_complete: Option<bool>,
    /// Anzahl fehlgeschlagener SELL-Versuche auf diesem Pool
    sell_fail_count: u32,
    /// Zeitpunkt des letzten SELL-Fehlers
    last_sell_fail_at: Option<std::time::Instant>,
}
```

`PoolInfo::new()` initialisiert die neuen Felder mit `None`/`0`/`None`.

---

**Teil 2: BondingCurveProgress → Pool-Migration erkennen** (momentum_bot.rs)

Im `MarketEventKind::BondingCurveProgress` Handler (~Zeile 7093):

```rust
MarketEventKind::BondingCurveProgress { mint, progress_bps, complete, .. } => {
    // Bestehend: Position-Tracker updaten
    let mut positions = ctx.positions.write();
    if let Some(pos) = positions.get_mut(mint.as_str()) {
        pos.bonding_curve_progress_bps = Some(*progress_bps);
    }
    drop(positions);

    // NEU: Pool-Migration-Status in mint_pools aktualisieren
    if *complete {
        let mut pools = ctx.mint_pools.write();
        if let Some(pool_list) = pools.get_mut(mint.as_str()) {
            for pool in pool_list.iter_mut() {
                if pool.dex == "pumpfun" {
                    pool.bonding_curve_complete = Some(true);
                    warn!(
                        mint = %mint,
                        pool = %pool.pool_address,
                        "PumpFun pool marked as migrated (bonding curve complete)"
                    );
                }
            }
        }
    }
}
```

---

**Teil 3: Sell-Failure → Pool-Failure-Count erhöhen** (momentum_bot.rs)

Im `ExecutionStatus::Failed` und `ExecutionStatus::Timeout` Handler für Sell-Side:
(Im bestehenden Block der Bug-C Fix-Logik, nach `exit_generated = false`)

```rust
} else if pending.side == TradeSide::Sell {
    // [Bestehend: Bug-C Fix] Reset exit_generated
    let mut positions = self.positions.write();
    if let Some(pos) = positions.get_mut(&pending.mint) {
        pos.exit_generated = false;
        pos.exit_generated_at = None;
    }
    drop(positions);

    // NEU: Pool-Failure-Count erhöhen
    let mut pools = self.mint_pools.write();
    if let Some(pool_list) = pools.get_mut(&pending.mint) {
        if let Some(pool_info) = pool_list.iter_mut().find(|p| p.pool_address == pending.pool) {
            pool_info.sell_fail_count += 1;
            pool_info.last_sell_fail_at = Some(Instant::now());
            warn!(
                mint = %pending.mint,
                pool = %pending.pool,
                dex = %pending.dex,
                sell_fail_count = pool_info.sell_fail_count,
                "Pool sell failure tracked — will prefer alternatives on retry"
            );
        }
    }
}
```

**Wichtig**: Auch im Orphaned-Sell-Recovery-Pfad (wenn `pending_opt.is_none()` und `exit_generated` Reset erfolgt) den gleichen Pool-Failure-Count inkrementieren. Dafür muss der Pool aus dem `ExecutionResult`-Metadaten oder aus der Position extrahiert werden.

---

**Teil 4: `find_best_sell_pool()` — Exclusion-Logik** (momentum_bot.rs)

```rust
fn find_best_sell_pool(&self, mint: &str, token_amount: u64, original_pool: &str)
    -> Result<(String, String, Vec<String>, f64, usize)>
{
    let pools = self.mint_pools.read();
    let candidates = pools
        .get(mint)
        .ok_or_else(|| anyhow::anyhow!("No pools known for mint {}", mint))?;

    let now = std::time::Instant::now();
    let max_age = std::time::Duration::from_secs(300);
    let fail_cooldown = std::time::Duration::from_secs(120);  // NEU
    const MAX_FAIL_COUNT: u32 = 3;                             // NEU

    // Phase 1: Filter gültige Pools
    let valid: Vec<_> = candidates
        .iter()
        .filter(|p| {
            p.dex_pool_accounts.is_some()
                && p.last_trade_ratio.is_some()
                && now.duration_since(p.last_updated) < max_age
        })
        .collect();

    // Phase 2: Exclusion (migrierte + kürzlich gescheiterte Pools)
    let preferred: Vec<_> = valid.iter()
        .filter(|p| {
            // Skip: PumpFun-Pool mit bestätigter Migration
            if p.bonding_curve_complete == Some(true) {
                return false;
            }
            // Skip: Pool mit >= MAX_FAIL_COUNT Fehlern im Cooldown-Fenster
            if p.sell_fail_count >= MAX_FAIL_COUNT {
                if let Some(last_fail) = p.last_sell_fail_at {
                    if now.duration_since(last_fail) < fail_cooldown {
                        return false;
                    }
                }
            }
            true
        })
        .collect();

    // Phase 3: Wenn alle excludiert → Fallback auf Pool mit niedrigstem fail_count
    let usable = if preferred.is_empty() {
        warn!(mint = %mint, valid_count = valid.len(),
            "All pools excluded by migration/failure — using best-available fallback");
        &valid
    } else {
        &preferred
    };

    // [Bestehender Code: Quotes berechnen, beste Route wählen]
    // ...
}
```

---

**Teil 5: Sell-Success → Failure-Count zurücksetzen** (momentum_bot.rs)

Im `ExecutionStatus::Confirmed` Handler für `TradeSide::Sell`:

```rust
// NEU: Bei erfolgreichem Sell den Failure-Count des Pools zurücksetzen
let mut pools = self.mint_pools.write();
if let Some(pool_list) = pools.get_mut(&pending.mint) {
    if let Some(pool_info) = pool_list.iter_mut().find(|p| p.pool_address == pending.pool) {
        if pool_info.sell_fail_count > 0 {
            info!(
                mint = %pending.mint, pool = %pending.pool,
                old_fail_count = pool_info.sell_fail_count,
                "Sell succeeded — resetting pool failure count"
            );
            pool_info.sell_fail_count = 0;
            pool_info.last_sell_fail_at = None;
        }
    }
}
```

---

**Zusammenfassung der Änderungen**:

| Datei | Änderung | Risiko |
|-------|----------|--------|
| `src/bin/momentum_bot.rs` | `PoolInfo` struct: 3 neue Felder | Minimal — rein additiv |
| `src/bin/momentum_bot.rs` | `BondingCurveProgress` Handler: Pool-Migration-Flag setzen | Minimal — nur Metadata |
| `src/bin/momentum_bot.rs` | `ExecutionStatus::Failed/Timeout` Sell: Pool-Fail-Count | Niedrig — neben bestehendem Bug-C Fix |
| `src/bin/momentum_bot.rs` | `find_best_sell_pool()`: Exclusion-Filter | Mittel — Kern-Routing-Logik, aber mit Fallback |
| `src/bin/momentum_bot.rs` | `ExecutionStatus::Confirmed` Sell: Fail-Count Reset | Minimal — rein additiv |

**Kein RPC im Hot Path. Keine neuen NATS Topics. Keine Architektur-Änderung.**

**Erwartete Wirkung**: Migrierte PumpFun-Pools werden nach dem `BondingCurveProgress` Event sofort gemieden. Selbst ohne dieses Event werden Pools nach 3 gescheiterten Sells für 120s ausgeschlossen, sodass der Bot auf PumpSwap AMM, Meteora, Raydium oder Orca wechselt.

---

#### FIX-21: Reserve-basiertes Multi-Pool-Routing (SLAVE LivePoolCache)
**Datum**: 2026-02-13
**Problem**: FIX-20 behebt die Exclusion-Logik, aber `find_best_sell_pool()` und `find_best_buy_pool()` nutzen weiterhin `last_trade_ratio` (grobe Approximation aus dem letzten beobachteten Trade) statt echter Reserve-basierter Quotes. Das führt zu suboptimaler Pool-Auswahl.

**Root Cause**: Der Momentum-Bot hatte keinen Zugriff auf den `LivePoolCache`, der in `market-data` (MASTER) und `execution-engine` (SLAVE) vorhanden war. Die Pool-Auswahl war daher nicht datengetrieben.

**Lösung**:
1. **Shared Modul** `src/execution/pool_cache_sync.rs` — Extrahiert `build_minimal_pool_state()`, `apply_pool_cache_update()` und `bootstrap_pool_cache_from_jetstream()` aus `execution_engine.rs` in ein wiederverwendbares Modul.
2. **SLAVE LivePoolCache im Momentum-Bot** — `MomentumContext` bekommt einen eigenen `LivePoolCache`, der beim Start aus JetStream gebootstrapt und laufend per `PoolCacheUpdate` Events aktualisiert wird.
3. **Reserve-basiertes Quoting** — Neue `quote_output_amount()` API in `quote_calculator.rs` berechnet Output-Beträge direkt aus `CachedPoolState` (ohne `TradeIntent`). `find_best_sell_pool()` und `find_best_buy_pool()` nutzen primär Cache-Quotes, Fallback auf `last_trade_ratio`.

**Dateien**:
| Datei | Änderung |
|-------|----------|
| `src/execution/pool_cache_sync.rs` | NEU — Shared Bootstrap/Sync |
| `src/execution/mod.rs` | Modul registriert |
| `src/execution/quote_calculator.rs` | `quote_output_amount()` API |
| `src/bin/execution_engine.rs` | Nutzt shared Modul |
| `src/bin/momentum_bot.rs` | LivePoolCache + JetStream Consumer + reserve-basierte Quotes |

**Kein RPC im Hot Path. Keine neuen NATS Topics. Architektur-konform (SLAVE Cache Pattern).**

### ~~BUG-B: Momentum-Bot verliert Position — Kein Sell-Intent generiert~~ ✅ BEHOBEN
**Schweregrad**: KRITISCH → **BEHOBEN** (2026-02-13)
**Fix**: Orphaned Buy Recovery in `handle_execution_result()`: Wenn ein `ExecutionResult` mit `status == Confirmed` und `side == BUY` eintrifft aber kein `pending_intent` existiert, wird die Position aus `ExecutionResult` Metadaten + `TokenTracker` rekonstruiert.
**Dateien**: `src/bin/momentum_bot.rs`

### ~~BUG-C: Momentum-Bot Retry-Bug — Ein Versuch, dann Aufgabe~~ ✅ BEHOBEN
**Schweregrad**: HOCH → **BEHOBEN** (2026-02-13)
**Fix**: `exit_generated` wird jetzt in `ExecutionStatus::Failed` und `ExecutionStatus::Timeout` Handlern für Sell-Side-Trades zurückgesetzt. Gilt sowohl für den normalen Pending-Intent-Pfad als auch für den Orphaned-Sell-Recovery-Pfad (konsistentes unconditional Reset).
**Dateien**: `src/bin/momentum_bot.rs`

### ~~BUG-D: Falscher Creator im Cache → ConstraintSeeds bei SELL~~ ✅ BEHOBEN
**Schweregrad**: HOCH → **BEHOBEN** (2026-02-14, FIX-22)
**Betroffene Tokens** (2026-02-13 Run): `64HemTH7`, `34c3bPRz`
**Symptom**: Alle Momentum-Bot SELL-Versuche scheiterten mit `Custom(2006)` (ConstraintSeeds) weil der `creator_vault` PDA aus einem falschen Creator abgeleitet wurde. Liquidation per RPC-Fallback funktionierte.

**Root Cause (detailliert)**:

Zwei zusammenwirkende Fehler:

1. **`instruction_accounts[7]` ist nicht immer der Creator**: `parse_pumpfun_create()` und `geyser_pool_discovery` extrahieren den Creator aus `instruction_accounts[7]` der CREATE-Transaktion. Bei Tokens die über CPI (Bundler, Launchpads) erstellt werden, kann der Account an Index 7 von der Bonding-Curve-Account-Daten (`data[49..81]`) abweichen.

2. **First-Write-Wins Cache blockiert Korrektur**: In `market_data.rs`:
   - `PoolCreated` Handler schreibt `creator_cache[mint]` **unconditional** (Zeile 2638)
   - `BondingCurveUpdate` Handler hat `contains_key`-Guard → **SKIP** wenn PoolCreated zuerst kam
   - `DevWalletIdentified` aus BondingCurveUpdate wird **nicht emittiert** → autoritativer Creator erreicht Momentum-Bot nie

**Server-Log-Evidenz**:

| Token | Momentum-Bot Creator | Korrekter Creator (RPC) | Sell-Ergebnis |
|-------|---------------------|------------------------|---------------|
| `64HemTH7` | `Ca8hHy...WMynz` | `B62Dvk...JhMYo` | ~20x `Custom(2006)` |
| `34c3bPRz` | `E77jVj...q1UP` | `GfBB85...4dqf` | ~20x `Custom(2006)` |

**Fix**: FIX-22 (siehe unten)

#### FIX-22: Autoritative Creator-Quelle + LivePoolCache Cross-Check
**Datum**: 2026-02-14
**Problem**: Falscher Creator in `creator_cache` und `TokenTracker.dev_wallet` durch nicht-autoritativen `instruction_accounts[7]` bei CPI-erstellten Tokens. `BondingCurveUpdate` (autoritativ) wurde durch `contains_key`-Guard blockiert.

**Lösung (2 Teile)**:

1. **market_data.rs — BondingCurveUpdate als autoritative Quelle**:
   - `pool_creator_cache`: `contains_key`-Guard entfernt → immer überschreiben
   - `creator_cache`: `contains_key`-Guard ersetzt durch Mismatch-Detection → immer überschreiben
   - `DevWalletIdentified`: Wird emittiert wenn Creator neu oder **anders** (Korrektur-Event)
   - WARN-Log bei Mismatch für Produktions-Diagnostik

2. **momentum_bot.rs — LivePoolCache Cross-Check**:
   - Neue Methode `resolve_authoritative_creator()` auf `MomentumContext`
   - Bei Entry- und Exit-Intents: Creator aus `TokenTracker.dev_wallet` wird gegen `LivePoolCache.get_pumpfun_creator()` geprüft
   - LivePoolCache-Wert (Geyser-Account-Daten) hat Vorrang → korrigiert auch TokenTracker
   - Fallback: TokenTracker-Wert wenn LivePoolCache den Token nicht kennt

**Dateien**:
| Datei | Änderung |
|-------|----------|
| `src/bin/market_data.rs` | BondingCurveUpdate: autoritative Cache-Writes + Mismatch-WARN |
| `src/bin/momentum_bot.rs` | `resolve_authoritative_creator()` + Cross-Check bei Entry/Exit |

**Kein RPC im Hot Path. Keine neuen NATS Topics.**

---

### BUG-30: Exit-Logic Gesamtproblem — Take Profit feuert nicht, Timed Exit überschritten, Momentum unzuverlässig
**Schweregrad**: KRITISCH
**Datum**: 2026-02-19
**Symptome**:
- Take Profit bei +81% PnL nicht ausgelöst (Config war 30%)
- Timed Exit bei 656s statt max 300s
- Momentum Exit unzuverlässig (5 Bot-Sells überstimmen 2 echte Buys)

**Root Cause — 6 miteinander verflochtene Bugs:**

**BUG-30a: Position Price nur aus Trade Events (KRITISCH — Phase 1)**
`current_price` wird ausschließlich von `MarketEventKind::Trade` Events aktualisiert (`momentum_bot.rs` Z.7184). Kein Update aus `PoolCacheUpdate` Reserves. Wenn keine Trades für einen Token kommen, bleibt `current_price` bei `entry_price` → PnL = 0% → Take Profit feuert nie. Das Dashboard zeigt korrekte PnL aus Fill-Daten, der Bot berechnet intern 0%.

**BUG-30b: Pending BUY blockiert ALLE Exit-Checks (KRITISCH — Phase 1)**
`check_for_exits()` überspringt Positionen mit pending BUY komplett (`momentum_bot.rs` Z.2855: `continue`), inklusive STOP_LOSS, TAKE_PROFIT und TIME_EXIT. Wenn ein Scale-In-BUY pending ist und die Execution Engine sequentiell arbeitet (BUG-30e), kann der BUY minutenlang pending bleiben. In dieser Zeit feuert kein Exit.

**BUG-30c: Reconciliation erfordert exit_generated==true (HOCH — Phase 1)**
`collect_timed_exit_reconcile_candidates()` überspringt Positionen mit `exit_generated==false` (`momentum_bot.rs` Z.2950). Wenn der initiale Exit wegen pending BUY (BUG-30b) nie generiert wurde, greift die Reconciliation nicht. Diese Positionen bleiben indefinit offen.

**BUG-30d: Momentum Exit nur Count-basiert, kein Volumen (MITTEL — Phase 1)**
`buy_ratio = buy_count / total` (`momentum_bot.rs` Z.732-734) ignoriert das SOL-Volumen der Trades. `TradeEvent.sol_amount` existiert, wird aber nicht genutzt. 5 Bot-Sells à 0.001 SOL überstimmen 2 echte Buys à 10 SOL. Volumengewichtete Berechnung erkennt echte Kaufkraft vs. Wash Trading.

**BUG-30e: Sequentielle Intent-Verarbeitung in Execution Engine (KRITISCH — Phase 2)**
`process_intent()` in `execution_engine.rs` awaits `confirm_signature_status()` (bis 30s Timeout) pro Intent. Bei Queue-Tiefe 5 wartet Intent #5 bis zu 150s. Dies verstärkt BUG-30b massiv: Pending BUYs bleiben minutenlang in der Queue, während alle Exits blockiert sind. Fix: Fire-and-Forget TX Sending + Parallele Verarbeitung.

**BUG-30f: RPC-Polling TX Confirmation + TPU Leader Cache Stale (HOCH — Phase 3)**
TX Confirmation nutzt `get_signature_statuses()` Polling mit exponential Backoff statt Geyser Subscription. `GeyserTxConfirm` Modul existiert (`geyser_tx_confirm.rs`), wird aber nur für ATA-Watching genutzt. TPU WebSocket hat kein Keepalive → Leader Cache wird stale → TX an falschen Validator → 7-100s bis Landing. Fix: Geyser-basierte Confirmation + TPU WS Keepalive.

**Status**: BUG-30a bis BUG-30d → ✅ BEHOBEN (FIX-30, Phase 1) | BUG-30e → ✅ BEHOBEN (FIX-31, Phase 2) | BUG-30f → ✅ BEHOBEN (FIX-32, Phase 3)

---

### FIX-30: Phase 1 — Exit-Logic Überholung (BUG-30a bis BUG-30d)
**Datum**: 2026-02-19

**Änderungen in `src/bin/momentum_bot.rs`:**

1. **Preis-Update aus Pool-Reserves** (BUG-30a): Nach `apply_pool_cache_update()` im PoolCacheUpdate-Processing wird `tokens_per_sol` aus `base_reserve / quote_reserve` berechnet und `update_position_price()` aufgerufen. Take Profit, Stop Loss und Trailing Stop reagieren sofort auf Preisänderungen aus Geyser Pool-State.

2. **Exit-Checks nicht bei pending BUY blockieren** (BUG-30b): `pending_buy_mints` Block in `check_for_exits()` komplett entfernt. Alle Exits feuern sofort. Bei Exit wird der pending BUY für die betroffene Mint aus `pending_intents` entfernt. Orphaned Buy Recovery (Z.3155-3247) fängt später confirmte BUYs ab.

3. **Reconciliation-Fix** (BUG-30c): `collect_timed_exit_reconcile_candidates()` behandelt jetzt auch Positionen mit `exit_generated==false`. Bestehende `pending_sells`-Prüfung verhindert doppelte SELLs.

4. **Volumengewichtete Momentum-Berechnung** (BUG-30d): `buy_ratio` in `should_exit()` nutzt `buy_sol_volume / total_sol_volume` statt `buy_count / total_count`. Fallback auf Count bei `total_vol == 0`.

---

### FIX-31: Phase 2 — Parallele Intent-Verarbeitung (BUG-30e)
**Datum**: 2026-02-19

**Problem**: `process_intent()` blockierte die Main-Loop der Execution Engine komplett, weil `confirm_signature_status()` (RPC-Polling) bis zu 30s pro Intent wartete. Bei 5 queued Intents dauerte es bis zu 150s bis der letzte verarbeitet wurde. Dies verstärkte BUG-30b massiv (Exits wurden durch blockierte Main-Loop verzögert).

**Änderungen in `src/bin/execution_engine.rs`:**

1. **Neues Config-Feld `max_concurrent_intents`** (u32, default: 4, range: 1-16): Startup-only Parameter, steuert die maximale Anzahl parallel verarbeiteter Intents. Hot-Reload wird acknowledged aber erfordert Neustart.

2. **Semaphore-basierte Concurrency-Kontrolle**: `intent_semaphore: Arc<tokio::sync::Semaphore>` in `ExecutionContext` begrenzt parallele Verarbeitung. LockManager's `try_lock_resource()` / `try_lock_capital()` verhindert Konflikte bei gleichzeitigen Intents für denselben Mint oder bei Kapitalüberschreitung.

3. **tokio::spawn + JoinSet**: Intents werden via `task_set.spawn()` parallel verarbeitet statt sequentiell awaited. Completed Tasks werden im Periodic Tick via `try_join_next()` gedraint.

4. **Graceful Shutdown**: Bei Shutdown wird `intent_semaphore.close()` aufgerufen (verhindert neue Akquisen), dann werden alle In-Flight Tasks mit 60s Timeout via `task_set.join_next()` abgewartet. Bei Timeout: `task_set.abort_all()`.

5. **Signer Send-Safety**: `let signer: &dyn Signer` Typ-Annotationen entfernt — `treasury.signer_ref()` gibt `&(dyn Signer + Send + Sync)` zurück, die explizite Annotation auf `&dyn Signer` strippte die Send+Sync Bounds und verhinderte `tokio::spawn`.

**Änderungen in `src/metrics.rs`:**

6. **Neue Metrik `CONCURRENT_INTENTS_GAUGE`** (AtomicU64): Zeigt die aktuelle Anzahl parallel laufender Intents im Prometheus-Endpoint (`ironcrab_concurrent_intents`).

**Thread-Safety**: Alle shared State in `ExecutionContext` war bereits thread-safe (LockManager: `parking_lot::RwLock`, RPC: `Arc<SolanaRpc>` mit AdaptiveLimiter, JSONL Writers: `Mutex`, alle Prometheus-Counter: `Atomic*`, Config: `RwLock`). `process_intent()` selbst brauchte keine internen Änderungen.

---

### FIX-32: Phase 3 — TX Latenz Optimierung / Geyser-basierte Confirmation (BUG-30f)
**Datum**: 2026-02-19  
**Status (PR3)**: **superseded** — EE Geyser TX watcher removed; confirm via market-data JetStream (`WalletTxConfirmed`). Siehe PR3 `refactor(confirm): remove EE Geyser TX watcher`.

**PR3.1 Hotfix (2026-06-06)**: Orphan-Confirm-Buffer in `execution_engine.rs` — `WalletTxConfirmed` kann im 100ms Main-Loop-Poll vor `register_wallet_tx_confirm_waiter` (nach Send) ankommen. Confirms ohne Waiter werden 120s in `recent_orphan_tx_confirms` gepuffert; bei spaeterer Registrierung sofort gematcht. Metriken: `tx_confirm_jetstream_orphan_buffered_total`, `_orphan_hit_total`, `_orphan_evicted_total`. Kein RPC-Fallback (I-7).

**Problem**: TX Confirmation nutzte `get_signature_statuses()` RPC-Polling mit exponentiellem Backoff (50ms→1000ms). Beobachtete Latenzen: 7-100s. Zusätzlich: TPU WebSocket hatte kein proaktives Keepalive, Leader Cache wurde stale, TXs gingen an falsche Validatoren.

**Änderungen in `src/solana/geyser_tx_confirm.rs` (entfernt in PR3):**

1. **Zweiter Geyser-Stream für TX Confirmation**: `run_tx_watcher()` startet einen dedizierten Geyser `transactions_status`-Stream gefiltert auf `account_include: [wallet_pubkey]`. Verarbeitet `UpdateOneof::TransactionStatus` und `UpdateOneof::Transaction`. O(1) HashMap-Lookup für Signature-Matching.

2. **Erweitertes `TxConfirmationResult`**: Neue Felder `error: Option<String>` (Fehlergrund bei failed TX) und `elapsed_ms: u64` (Latenz für Metriken).

3. **`with_geyser()` erweitert**: Nimmt jetzt `wallet_pubkey: Pubkey` als Parameter. Startet sowohl ATA-Watcher als auch TX-Watcher.

4. **`register_tx()` sendet `WatchTx` Command**: Informiert den TX-Watcher-Task über neue Signatures (aktuell subscribed der Stream alle Wallet-TXs und matcht per HashMap).

5. **`on_transaction_failed()`**: Neue Methode für on-chain fehlgeschlagene TXs (Slippage, etc.).

6. **Auto-Reconnect und Timeout-Cleanup**: TX-Watcher reconnected automatisch bei Stream-Disconnects. Periodischer Cleanup alle 5s für timed-out Signatures.

**Änderungen in `src/bin/execution_engine.rs`:**

7. **`confirm_signature_status()` refactored**: Geyser-First Strategie mit RPC-Polling Fallback. `confirm_via_geyser()` nutzt `tokio::select!` auf oneshot::Receiver vs. Timeout. `confirm_via_rpc_polling()` enthält die bisherige Polling-Logik als Fallback.

8. **Separate Rebroadcast-Task**: `spawn_rebroadcast_loop()` läuft parallel zur Confirmation (konfigurierbar: `rebroadcast_interval_ms`, `max_rebroadcasts`).

9. **Neue Config-Felder**: `geyser_confirm_enabled` (bool, default: true), `rebroadcast_interval_ms` (u64, default: 2000), `max_rebroadcasts` (u32, default: 5). Alle hot-reloadable.

10. **`tx_confirm: Arc<GeyserTxConfirm>` in ExecutionContext**: Initialisiert beim Startup mit Geyser URL und Wallet-Pubkey falls verfügbar.

**Änderungen in `src/solana/tpu_client.rs`:**

11. **`TPU_CACHE_STALE_TOTAL` Metrik**: Inkrementiert in `check_leader_cache_health()` bei Staleness-Detection.

12. **`TPU_RECONNECT_TOTAL` Metrik**: Inkrementiert in `reconnect()` bei erfolgreichem Reconnect.

**Änderungen in `src/solana/tx_sender.rs`:**

13. **Reconnect Rate-Limit gesenkt**: Von 30s auf 15s in `send_via_tpu()` und `spawn_health_check_task()`.

**Änderungen in `src/metrics.rs`:**

14. **Neue Metriken**: `TX_CONFIRM_GEYSER_TOTAL`, `TX_CONFIRM_RPC_FALLBACK_TOTAL`, `TX_CONFIRM_LATENCY_MS`, `TPU_RECONNECT_TOTAL`, `TPU_CACHE_STALE_TOTAL`, `GEYSER_TX_WATCHER_CONNECTED`.

### FIX-33: Fehlende DexPoolAccounts bei SELL Exits

**Datum**: 2026-02-11  
**Schweregrad**: CRITICAL — Gekaufte Tokens konnten nicht verkauft werden  
**Symptom**: `Custom(6023)` ("NotEnoughTokensToSell") und `Custom(11)` bei SELL-Transaktionen. Logs: "Missing DexPoolAccounts for exit intent; falling back to empty accounts".

**Root Cause**: `update_pool_trade_data()` aktualisierte nur existierende Einträge in `mint_pools`, erstellte aber keine neuen. Pools die nur über Trade-Events entdeckt wurden fehlten. Zusätzlich nutzte `generate_and_publish_exit_intent()` den PositionTracker nicht als Fallback.

**Fix (3 Ebenen)**:

1. **Pool-Auto-Registrierung** (`momentum_bot.rs`): `update_pool_trade_data()` erstellt jetzt automatisch neue `PoolInfo`-Einträge wenn ein Pool für einen Mint unbekannt ist.
2. **PositionTracker-Fallback** (`momentum_bot.rs`): `generate_and_publish_exit_intent()` nutzt jetzt `mint_pools` für den spezifischen Pool als primäre Quelle, dann `try_get_dex_pool_accounts_for_mint()`, mit Warnung statt Abbruch wenn beides fehlt.
3. **TX-Builder Fallback** (`tx_builder.rs`): Bereits implementiert für PumpSwap AMM (LivePoolCache-Fallback Z.605-614). Keine zusätzlichen Änderungen nötig.

**Dateien**: `src/bin/momentum_bot.rs`

### FIX-34: Token ATA-Erstellung für BUY bei 3 DEX-Pfaden (Token-2022)

**Datum**: 2026-02-11  
**Schweregrad**: CRITICAL — BUY-Transaktionen scheiterten an fehlender ATA  
**Symptom**: `Custom(2)` auf `ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL` (Associated Token Program) bei BUY-Simulationen.

**Root Cause**: PumpSwap AMM, Orca und Raydium BUY-Pfade in `tx_builder.rs` erstellten keine Token-ATA vor dem Swap. Für Token-2022 muss `create_associated_token_account_idempotent` mit der korrekten `token_program_id` aufgerufen werden.

**Fix**:

- **FIX-34a PumpSwap AMM**: `create_associated_token_account_idempotent` als erste Instruction bei BUYs, mit `intent.resources.token_program` für Token-2022.
- **FIX-34b Orca**: ATA-Erstellung bei BUYs + hardcoded `spl_token::id()` durch dynamisches `intent.resources.token_program` ersetzt.
- **FIX-34c Raydium**: ATA-Erstellung bei BUYs + hardcoded `spl_token::id()` durch dynamisches `intent.resources.token_program` ersetzt.

Alle drei Pfade erstellen WSOL-ATA bei SELLs (für empfangene SOL).

**Dateien**: `src/execution/tx_builder.rs`

### FIX-35: Position-Reconciliation nach Restart (Lazy Reconciliation)

**Datum**: 2026-02-11  
**Schweregrad**: MEDIUM — Alte Positionen wurden nach Restart nicht getracked  
**Symptom**: Tokens im Wallet sichtbar aber nicht vom Bot verwaltet. `build_reconciled_position()` scheitert wenn `mint_pools` nach Restart noch leer ist.

**Root Cause**: `mint_pools` ist in-memory. Nach Restart sind Pool-Infos erst verfügbar wenn PoolCreated/DexPoolAccounts Events eintreffen. WalletBalanceSnapshots können aber vorher verarbeitet werden.

**Fix**: `orphaned_mints: HashMap<String, (u64, u8)>` im `MomentumContext`. Wenn `build_reconciled_position()` scheitert, wird der Mint mit `(balance_raw, decimals)` gespeichert. Bei jedem `register_pool()` wird geprüft ob ein orphaned Mint jetzt reconciled werden kann. Balance=0 Snapshots entfernen Mints aus dem Set.

**Dateien**: `src/bin/momentum_bot.rs`

### FIX-36: WSOL wird als tradeable Position getrackt

**Datum**: 2026-02-11  
**Schweregrad**: HIGH — Verursacht falsche "Open Positions", Endlos-SELL-Intents und Intent-Rejections  
**Symptom**: WSOL (`So11111111111111111111111111111111111111112`) wurde durch `WalletBalanceSnapshot` als Token-Position reconciled. Daraufhin versuchte der Bot wiederholt WSOL per `TIMED_EXIT` zu verkaufen, was jedes Mal mit `SIM_INSUFFICIENT_BALANCE` oder `Custom(11)` scheiterte. Dies verfälschte die "Open Positions"-Anzeige und flutete den Intent-Stream mit sinnlosen Sells.

**Root Cause**: `WalletBalanceSnapshot`-Handler und `build_reconciled_position()` filterten WSOL nicht aus. Da WSOL immer eine Wallet-Balance hat, wurde es als unbekannte Position interpretiert und reconciled.

**Fix**: 
1. Early-return im `WalletBalanceSnapshot`-Handler: SOL-Mint wird sofort übersprungen
2. Guard in `build_reconciled_position()`: gibt `None` zurück für SOL/WSOL
3. Erkennt alle drei SOL-Varianten: `So11111111111111111111111111111111111111112` (WSOL), `NATIVE_SOL` (market-data Label), `11111111111111111111111111111111` (System Program)

**Dateien**: `src/bin/momentum_bot.rs`

### FIX-37: Owner-Scan Mints werden bei vollem Bootstrap-Cap ignoriert

**Datum**: 2026-02-19  
**Schweregrad**: HIGH — Wallet-Tokens werden beim Startup nicht erkannt  
**Symptom**: Nach Restart zeigt Bot 0 Positionen obwohl 2 Token-2022 Tokens (ANDREW, TRUMPIUS) in der Wallet sind. `mints_in_wallet=0` im Log obwohl `getTokenAccountsByOwner` die Tokens findet.

**Root Cause**: `MAX_BOOTSTRAP_MINTS = 30`. JetStream Recovery füllt 30 Plätze mit stale Mints aus alten Snapshots. Die anschließende Owner-Scan Merge-Logik prüft `known_mints.len() < MAX_BOOTSTRAP_MINTS` — da bereits 30 Mints vorhanden, werden reale Wallet-Tokens nicht hinzugefügt und somit nie verarbeitet.

**Fix**: Owner-Scan Mints mit realer Wallet-Balance umgehen das `MAX_BOOTSTRAP_MINTS`-Cap. Sie repräsentieren tatsächliche Wallet-Inhalte und haben immer Vorrang vor stale JetStream-Einträgen.

**Dateien**: `src/bin/market_data.rs`

### FIX-38: Wrong-Pool Price Pollution → falsche TAKE_PROFIT (realer Verlust)

**Datum**: 2026-02-21  
**Schweregrad**: CRITICAL — TAKE_PROFIT feuert bei +200% laut Bot, tatsächlicher PnL negativ  
**Symptom**: Trades mit "Take profit hit: +205% gain" im Detail, aber PnL (SOL) und PnL % negativ. Oft ~1 Sekunde nach Probe-Buy.

**Root Cause**: `update_position_price()` akzeptierte Preis-Updates von **beliebigen** Pools. Bei Multi-Pool-Tokens (Bonding Curve + AMM) wurde `current_price` mit Daten eines anderen Pools überschrieben → falsches `pnl_pct()` → TAKE_PROFIT trotz realem Verlust.

**Fix**:
1. **Pool-Matching**: Trade- und PoolCacheUpdate-Updates nur anwenden, wenn `source_pool == position.pool`
2. **take_profit_min_hold_secs** (Default 5): TAKE_PROFIT erst nach Mindest-Haltedauer möglich

**Dateien**: `src/bin/momentum_bot.rs`, `src/config.rs`  
**Details**: `docs/TAKE_PROFIT_FALSE_GAIN_FIX_20260221.md`

---

## 4. BEKANNTE ARCHITEKTUR-PROBLEME (aus Architecture Audit)

Diese Bugs sind im Detail in `docs/ARCHITECTURE_AUDIT_2026-02-07.md` dokumentiert:

| ID | Problem | Schweregrad | Status |
|----|---------|-------------|--------|
| Audit-A | Killswitch-Liquidation überspringt Tokens | ⚠️ TEILWEISE BEHOBEN | FIX-02, FIX-12, FIX-13 |
| Audit-B | `load_pool_from_geyser()` macht 20 RPC-Retries | ✅ FIXED (FIX-29) | Serum-Caching, RPC-Elimination Hot Path |
| Audit-C | PumpFunAmmDex eigene RPC-Infrastruktur | ✅ BEHOBEN | Hot Path: Cache-Miss→None; Cold Path: SolanaRpc statt reqwest |
| Audit-D | Token-Decimals immer per RPC | ✅ BEHOBEN | token_utils nur Cold Path; Hot Path nutzt mint_infos/TokenMintInfo; LivePoolCache für execution_engine; RPC in sell_all/wallet akzeptabel |
| Audit-E | `cleanup_wallet_after_liquidation()` per RPC | ✅ AKZEPTIERT | Cold Path; RPC für autoritativen Zustand nötig – alle leeren ATAs müssen zuverlässig geschlossen werden |
| Audit-F | Orca Reserve-Fetching 5min TTL + RPC | ✅ BEHOBEN | LivePoolCache einzige Quelle; Cache-Miss→statische Reserves (kein RPC); RPC nur Cold Path (`live_pool_cache.is_none()`) |
| Audit-G | Stale JetStream Wallet-Snapshots | ✅ BEHOBEN | FIX-14 |
| Audit-H | Hardcoded quote_mint in DEX-Parsern | ✅ BEHOBEN | FIX-15 |
| Audit-I | PumpFun SELL stale Quote für migrierte Tokens | ✅ BEHOBEN | Guard in pumpfun.rs (Z.888-902). Restprobleme → BUG-A/FIX-20 |

---

## FIX-17: CRITICAL — fill_in/fill_out Accuracy (False Take-Profit Triggers)

**Datum**: 2026-02-13  
**Schweregrad**: CRITICAL — Bot traf Trading-Entscheidungen auf Basis falscher Preisdaten

### Problem
Bei BUY-Trades mit `lamport_noise=true` (ATA wird erstellt) fiel `fill_in` auf `intent.required_capital` zurück.
Dies war katastrophal falsch wenn die DEX weniger SOL akzeptiert als beabsichtigt (z.B. PumpFun Bonding Curve fast voll):

- **D39XKvFT**: `fill_in` = 0.00125 SOL (intent), **real**: 0.000043 SOL → **28.6x Fehler**
- Falsche `entry_price` → Momentum-Bot sah +2949.3% Gain statt real ~6.5%
- Take-Profit wurde fälschlicherweise ausgelöst, Token mit Verlust verkauft

Bei SELL-Trades war `fill_out` immer `None` wenn `lamport_noise=true` (ATA geschlossen), weil der
native SOL Fallback durch das Lifecycle-Noise-Gate blockiert wurde.

### Root Cause
`compute_intent_fills_best_effort()` in `execution_engine.rs`:
- Zeile 424-428 (alt): `lamport_noise → fill_in = intent.required_capital` (kann 29x falsch sein)
- Zeile 439 (alt): `lamport_noise → fill_out = None` (SELL SOL-Erlös fehlt komplett)

### Fix
Neue dreistufige Fallback-Kette für native SOL-Legs mit `lamport_noise`:

1. **Inner Instruction Parsing** (`extract_swap_sol_from_inner_instructions`):
   - Parst `meta.inner_instructions` nach System Program `transfer` Instruktionen
   - Filtert `createAccount` aus (das ist ATA-Rent, kein Swap)
   - Genaueste Methode: erfasst Swap-Betrag + DEX-Fees (ohne ATA-Rent)

2. **Rent-Adjusted Lamport Delta**:
   - `compute_wallet_lamport_delta_best_effort` gibt jetzt auch `rent_adjustment` zurück
   - Bereinigtes Delta = `raw_delta + rent_created - rent_refunded`
   - Entfernt ~96% des Errors (ATA-Rent ist ~2.04M lamports)

3. **intent.required_capital** (letzter Ausweg mit WARN-Log)

### Dashboard PnL
`trades_server.py`: SELL proceeds nutzt jetzt explizit `wallet_delta` (konsistent mit BUY cost).
ATA-Rent hebt sich auf. Dashboard zeigt realen Wallet-Impact inklusive aller Fees.

### Dateien
- `src/bin/execution_engine.rs`: `compute_wallet_lamport_delta_best_effort`, `extract_swap_sol_from_inner_instructions`, `compute_intent_fills_best_effort`
- `scripts/trades_server.py`: PnL-Berechnung in 3 Blöcken (run, last, 24h)

---

## FIX-38: Token-2022 Simulation State-Lag Bypass

**Status:** ✅ FIXED

### Problem
Seit PumpFun auf Token-2022 migriert hat, scheitern BUY- und SELL-Simulationen häufig:

- **Custom(2) auf BUY (Instruction 0):** `create_associated_token_account_idempotent` ruft Token-2022's `GetAccountDataSize` auf, das den Mint-Account lesen muss. Bei neu erstellten Tokens ist der Mint-Account im lokalen RPC-Node noch nicht synchronisiert → "Invalid Mint".
- **Custom(6023) auf SELL (Instruction 1):** PumpFun "NotEnoughTokensToSell" — der BUY wurde gerade on-chain bestätigt, aber die lokale Simulation sieht noch 0 Tokens im ATA.

### Root Cause
Der lokale Agave-Validator (non-voting RPC) hinkt der Chain um 1-5 Sekunden hinterher. Simulationen für brandneue Tokens (BUY) oder frisch gekaufte Tokens (SELL) nutzen veralteten State. Die on-chain Validators haben den aktuellen State.

**Beweis:**
- Derselbe Token-2022 Mint (`2yXRu77p...pump`) wurde on-chain erfolgreich mit Token-2022 ATA gekauft (GetAccountDataSize = 170 bytes, InitializeImmutableOwner + InitializeAccount3).
- Spätere BUYs für andere Token-2022 Mints scheitern identisch in der Simulation.
- Alle geprüften Mints sind verifiziert Token-2022 (`owner=TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb`).

### Fix
Simulation-Bypass für zwei spezifische, transiente Fehlermuster:

1. `InstructionError(0, Custom(2))` + `side=Buy` → Token-2022 ATA state lag
2. `Custom(6023)` + `side=Sell` + `dex=pumpfun` → Balance state lag

Bei Erkennung: Simulation als "passed" markieren mit Bypass-Reason, TX direkt senden.

**Risiko:** Minimal — bei echtem Fehler (Mint existiert wirklich nicht) scheitert die TX on-chain, Kosten = TX-Fee (~0.000005 SOL). PumpFun BUY hat max_sol_cost Slippage-Schutz.

### Dateien
- `src/bin/execution_engine.rs`: Simulation-Bypass-Logik nach `simulate_transaction()`

---

## FIX-40: INTENTS_EXECUTED nur Confirmed + WsolManager erst nach Trade

**Status:** ✅ FIXED

### Problem 1: Keine "executed" Intents bei Slippage-TX
TX on-chain mit Slippage = `FailedConfirmed`. `INTENTS_EXECUTED_TOTAL` zählte nur `Confirmed`, nicht `FailedConfirmed`.

### Problem 2: WsolManager wickelt erst nach dem ersten Trade
WsolManager ist event-getrieben und führt `check_and_act()` nur bei `WalletBalanceUpdate`-NATS-Nachricht aus. Nach Killswitch-Reset wurde keine WalletBalanceUpdate gesendet → WsolManager wartet auf die nächste Geyser-Update (nach Trade) → keine WSOL vor dem ersten Trade.

### Fix
1. `INTENTS_EXECUTED_TOTAL`: Zählt jetzt `Confirmed` **und** `FailedConfirmed` (TX on-chain = executed).
2. Bei `ResetKillSwitch`: Publish `WalletBalanceUpdate` mit aktuellen LockManager-Balances an `wallet_balance_topic` → WsolManager erhält sofort Trigger und kann wrap.

### Dateien
- `src/bin/execution_engine.rs`: INTENTS_EXECUTED_TOTAL-Logik; WalletBalanceUpdate bei ResetKillSwitch
- `docs/INTENTS_EXECUTED_AND_WSOL_KILLSWITCH_ANALYSIS.md`: Analyse

---

## FIX-41: Meteora DLMM „out=0“ / BalanceUpdated partielle Updates überschreiben Reserves

**Status:** ✅ FIXED  
**Datum:** 2026-02-22

### Problem
Quote-Berechnung schlug fehl mit `meteora: missing reserves (in=19892667585, out=0)`. Der SLAVE LivePoolCache erhielt `PoolCacheUpdate::BalanceUpdated` mit nur einem Vault (base oder quote); der andere Wert war 0. Durch vollständiges Ersetzen des Pool-States wurde die andere Reserve mit 0 überschrieben → Quote-Calculator bekam `out=0`.

### Root Cause
market-data publiziert BalanceUpdated, wenn ein einzelnes Vault ein Geyser-Update erhält. Wenn das andere Vault noch nicht aktualisiert wurde, steht in `tracked_vaults` dafür `last_balance=0`. Das ergibt partielle Updates `(base, 0)` oder `(0, quote)`. `apply_pool_cache_update` hat den kompletten Pool-State ersetzt statt zu mergen.

### Fix
Bei `BalanceUpdated`: Vor dem Upsert mit dem bestehenden Cache-Stand mergen. Wenn `update.base_reserve > 0` und `update.quote_reserve == 0`, wird der bestehende `quote_reserve` aus dem Cache beibehalten (und umgekehrt). So wird kein bekannter Wert mit 0 überschrieben.

### Dateien
- `src/execution/pool_cache_sync.rs`: `extract_reserves()`, `build_minimal_pool_state_with_reserves()`, Merge-Logik in `apply_pool_cache_update()`

---

## FIX-39: TAKE_PROFIT Dashboard PnL invertiert (SELL proceeds falsch)

**Status:** ✅ FIXED

### Problem
TAKE_PROFIT-Trades zeigten im Dashboard fälschlich Verlust (negative PnL), obwohl der Detail-Text "+176.3% gain" anzeigte. Die PnL-Spalten (SOL, %) waren invertiert.

### Root Cause
Für SELL-Trades nutzte `trades_server.py` **wallet_sol_delta** als primäre Quelle für `proceeds_sol`. Bei PumpSwap/PumpFun-SELL ist der Swap-Output **WSOL** (Token), nicht native SOL. `wallet_sol_delta` misst nur native SOL (Rent-Rückerstattung ~0.002 SOL minus Fees) — **nicht** die tatsächlichen Swap-Erlöse. Dadurch wurde z.B. 0.002 SOL als proceeds verwendet statt 0.015 SOL → pnl_sol = 0.002 - 0.01 = -0.008 (fälschlicher Verlust).

### Fix
SELL `proceeds_sol` nutzt jetzt **value_sol (fill_out)** als primäre Quelle — das sind die tatsächlichen Swap-Erlöse (WSOL/SOL). `wallet_delta` nur als Fallback wenn `value_sol` fehlt.

### Dateien
- `scripts/trades_server.py`: SELL proceeds = value_sol (fill_out) statt wallet_delta in allen 3 PnL-Berechnungsblöcken

---

## FIX-43: (REVERTED) Momentum falscher TAKE_PROFIT — Pool-Filter

**Status:** ❌ REVERTED (falsche Ursachenannahme)  
**Datum:** 2026-02-23

### Problem
Der Momentum-Bot triggert TAKE_PROFIT mit „+173 % gain“, aber der tatsächliche Verkauf on-chain ergibt Verlust.

### Ursprüngliche Annahme (falsch)
`current_price` von „falschem Pool“ (Meteora/PumpSwap statt Bonding Curve).

### Korrektur
- Token auf Bonding Curve: Es gibt **keinen** anderen Pool — nur die Bonding Curve.
- Migrierte Token: Multi-Pool wählt den besten verfügbaren Pool.
- Der Pool-Filter war daher nicht die richtige Lösung; die eigentliche Ursache des falschen PnL ist weiterhin ungeklärt.

---

## FIX-42: TAKE_PROFIT falsche Verluste (Dashboard) — BUY cost nutzte wallet_delta statt fill_in

**Status:** ✅ FIXED  
**Datum:** 2026-02-23

### Problem
TAKE_PROFIT-Trades zeigten weiterhin fälschlich hohe Verluste (z.B. -64 %), obwohl der Bot „+173 % gain“ meldete. FIX-39 hatte nur SELL proceeds auf value_sol umgestellt; BUY cost nutzte weiterhin wallet_delta.

### Root Cause
Asymmetrie: BUY cost = wallet_delta (native SOL), SELL proceeds = value_sol (fill_out). Bei WSOL-BUYs misst wallet_delta nicht den Swap-Betrag (nur Rent/Fees) → falsche Cost-Basis → systematische PnL-Fehler.

### Fix
BUY cost bevorzugt jetzt value_sol (fill_in) — die tatsächlich für den Swap verwendete SOL/WSOL-Menge. wallet_delta nur als Fallback. BUY und SELL verwenden damit konsistent die Fills.

### Dateien
- `scripts/trades_server.py`: BUY cost = value_sol (fill_in) bevorzugt in allen 3 PnL-Blöcken
- `docs/TAKE_PROFIT_PNL_ANALYSIS_20260223.md`: Analyse

---

## FIX-47: Momentum — BondingCurveProgress vor BUY-Confirm ging verloren (Pending-Entry-Lifecycle)

**Status:** ✅ FIXED  
**Datum:** 2026-05-10

### Problem
`BondingCurveProgress` wurde nur auf eine bestehende `PositionTracker`-Zeile geschrieben. Trifft der Geyser-State vor der BUY-Bestätigung / vor `ExecutionResult` ein, ging der Fortschritt (z. B. 10000 bps + complete) verloren — kein `BONDING_CURVE_EXIT`, später z. B. nur `TIME_EXIT`.

### Fix
- Globales `latest_bonding_by_mint` mit slot-/ts-monotonem Merge.
- Nach erfolgreichem JetStream-Publish: `PendingBuyEntry` (keine Position, kein Position-Count).
- Bei Confirm: Snapshot auf `open_position` über `initial_bonding`; Lifecycle-Eintrag entfernen; optional `process_exit_signals` per `tokio::spawn` nach erfolgreichem Open.
- Failed/Timeout BUY und fehlendes `fill_out`: Lifecycle bereinigen; `cleanup_stale_pending` räumt Lifecycle für abgelaufene BUY-Pendings mit auf.

### Dateien
- `src/bin/momentum_bot.rs`

### Tags
[momentum, pumpfun, bonding_curve, geyser, i-4, scope-a]

---

## 5. VERLORENE ÄNDERUNGEN DURCH REVERT (Cherry-Pick Status)

| Priorität | Beschreibung | Status |
|-----------|-------------|--------|
| **CRITICAL** | fill_in/fill_out Accuracy (FIX-17) | ✅ FIXED |
| **CRITICAL** | Invertierte PnL-Formel (FIX-PNL) | ✅ FIXED |
| P1 | PumpSwap AMM Geyser-First Integration | ✅ FIXED (FIX-23) |
| P1 | PumpFun SELL migrierte Tokens → `Ok(None)` | ✅ FIXED (Guard in pumpfun.rs) |
| P1 | `emit_sim_failed_decision()` → `Err` für Retry | ✅ FIXED |
| P1 | Ghost Positions + Wallet Balance Bootstrap | ✅ FIXED (FIX-24) |
| P2 | Creator-Handling & DEX-Normalisierung | ✅ FIXED (FIX-25) |
| P2 | Market-Data WSOL-Seeding & Pool-Propagation | ✅ FIXED (FIX-16 + FIX-24 + FIX-26) |
| P2 | TX-Builder Cache-capped min_out | ✅ FIXED (FIX-28) |
| P3 | `available_trading_capital_lamports` Metrik | ✅ FIXED (Grafana Label bereits korrigiert auf "Available WSOL") |

---

## FIX-45: Momentum Drawdown from ATH falsch (2025-04-XX)

| Symptom | Trailing Stop zeigt -1986% from ATH bei realen -2.4%, PnL korrekt (-1.2%). |
|---------|--------------------------------------------------------------------------|
| **Root Cause** | `drawdown_from_ath_pct()` nutzte falsche Formel: (entry/current - 1) statt (current/highest - 1). Zudem wurde highest_price als maximaler Preis (höchster tps = teuerst) getrackt, obwohl bei PumpFun niedrigste tps = ATH. |
| **Fix** | `drawdown_from_ath_pct()` nutzt `(current / highest - 1)*100`. `highest_price` tracktet minimalen `tokens_per_sol`. Identisch mit FIX-PNL für pnl_pct(). |
| **Betroffene Module** | momentum-bot, position.rs |
| **Regression-Prüfung** | PnL und Drawdown nutzen dieselbe Preisquelle (tokens_per_sol), Formeln konsistent. |
| **Tags** | [momentum, drawdown, ath, fix] |

---

## FIX-46: Momentum Cost Basis / Scale-In `entry_price` und `ExplicitAmount` ohne `ui`

| Symptom | Decision Records: extreme Trailing/TP-Prozente (−80 % from ATH, +300 % TP) bei real nahezu flat/Verlust; Dashboard spiegelt Backend. |
|---------|----------------------------------------------------------------------------------------------------------------------------------------|
| **Root Cause** | (1) `PositionTracker::add_investment` gewichtete Scale-In mit `current_price` (Mark) statt Fill-`tokens_per_sol` der neuen Tranche. (2) `ExplicitAmount::as_f64()` lieferte bei deserialisierten Fills ohne `ui` **0** — `entry_price` aus `tok_ui/sol_ui` wurde falsch. |
| **Fix** | `add_investment(additional_sol, fill_entry_tps)`; `open_position` reicht Fill-`p.entry_price` durch. `ExplicitAmount::ui_f64()` leitet aus `raw`/`decimals` ab; `as_f64()` delegiert dorthin; BUY-Bestätigung und Orphan-Recovery nutzen `ui_f64()` für Fill-UI. |
| **Betroffene Module** | `src/bin/momentum_bot.rs`, `src/ipc/schema.rs` |
| **Regression-Prüfung** | Unit-Test: gewichteter Scale-In-Blend nutzt Fill-`tokens_per_sol` statt Marktpreis; `ExplicitAmount` ohne `ui` in JSON. |
| **Tags** | [momentum, scale-in, entry_price, explicit_amount, i-15] |

---

## FIX-47: Momentum price-based exits (TP/SL/Trailing) binden an ausführbare LivePoolCache-Quotes (Scope D)

| Symptom | TAKE_PROFIT / STOP_LOSS / TRAILING_STOP triggern auf Trade-Ratio-`current_price` oder verzerrte Ticks; ausführbarer PumpSwap-Exit erst spät im `find_best_sell_pool`, nicht in der Exit-Entscheidung. |
|---------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Root Cause** | `exit_action_for_price_signal` erlaubte SL/Trailing ohne `ExitExecutableQuote`; `executable_exit_quote` nutzte nur `position.pool` statt bester erlaubter Multi-Pool-Route. |
| **Fix** | (1) TP/SL/Trailing nur bei gültigem Reserve-Quote für `token_amount`; sonst Suppress (`NoExecutableQuote`). (2) `executable_exit_quote` wählt max SOL-out unter denselben Filtern wie `find_best_sell_pool` (inkl. Migration/PumpSwap nach Evidence). (3) Mark-Update aus Quote nur wenn `marks_position_pool` (I-13). Reason-Strings nutzen executable PnL/Drawdown. |
| **Betroffene Module** | `src/bin/momentum_bot.rs` |
| **TODO / Follow-up** | Striktere Freshness-Grenze (z. B. `cache_age_ms`) für price-based exits optional ergänzen — Metadaten (`source_slot`, `cache_age_ms`) sind im Quote bereits mitgeführt. |
| **Tags** | [momentum, exit, live_pool_cache, i-13, i-16, scope-d] |

---

## FIX-48: Momentum Scope C — priorisierte / bounded `ExecutionResult`-Drains und Event-Latenz-Logs

| Symptom | `ExecutionResult`-JetStream-Verarbeitung kann hinter grossen `PoolCacheUpdate`-Batches oder dichten Trade/Bonding-`MarketEvent`s verzögert werden; Lifecycle-/Positionsupdates kommen zu spät. |
|---------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Root Cause** | `tokio::select!`-Arm zog bis zu 50 `ExecutionResult`s pro Aktivierung; Pool-Cache-Fetch verarbeitete bis zu 100 Updates in einem Rutsch — wenig Zwischenraum fuer faire Interleaving mit anderen Armen. |
| **Fix** | (1) `drain_execution_results`: bounded Pull (`max_messages` 16 scheduled / 8 interleaved), strukturierte `trace!`/`debug!` mit stabilen Feldern (`momentum_scope_c`, `ingest_lag_ms`, `slot_lag_vs_last_event_slot`, `fetch_expires_profile`). (2) **Zwei JetStream-`expires`-Werte:** `EXECUTION_RESULT_SCHEDULED_FETCH_EXPIRES` (~80ms) nur im dedizierten `select!`-Arm; `EXECUTION_RESULT_INTERLEAVED_FETCH_EXPIRES` (wenige ms) für `after_market_event` / `after_pool_cache_batch`, damit leere Streams die Hot-Path-Arme nicht spürbar blockieren. (3) Pool-Cache-Fetch-Limit 48; Batch-Dauer und Message-Zahlen geloggt; MarketEvent-Latenz für schwere Kinds. |
| **Betroffene Module** | `src/bin/momentum_bot.rs` |
| **Regression-Prüfung** | `process_exit_signals` unmittelbar nach Pool-Cache-Preis-Updates unverändert; Scope-D Quote-Gating unangetastet; Unit-Tests fuer Scope-C-Hilfsfunktionen. |
| **Tags** | [momentum, jetstream, execution_result, latency, scope-c, i-24a] |

---

## FIX-49: Momentum Scope C — Latest-State-Coalescing (Bonding + PoolCache) und Lag-/Backlog-Decision-Gate

| Symptom | Dichte `BondingCurveProgress`- und `PoolCacheUpdate`-Bursts erzeugen redundante Strategie-/Preisarbeit; Observability zeigte Verarbeitungsdauer, aber wenig belastbare Kennzahlen fuer spaeteres Sharding. |
|---------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Root Cause** | Core-NATS verarbeitet je `select!`-Runde nur ein MarketEvent; PoolCache-JetStream wendete fuer jede Nachricht sofort den teuren Reserve-/Positions-Preis-Pfad an, obwohl `LivePoolCache` bereits slot-/merge-faehig ist und `merge_bonding_curve_progress_geyser` ohnehin monoton arbeitet. |
| **Fix** | (1) **BondingCurveProgress:** Bis zu 32 aufeinanderfolgende Nachrichten per `now_or_never` ziehen, pro Mint per Geyser `(slot, ts)` auf einen Gewinner reduzieren, deterministisch nach Mint sortiert ausfuehren; strukturiertes `debug!` mit `stale_dropped` und `decision_gate_stale_ratio_permille`. (2) **PoolCacheUpdate:** Zwei Phasen — alle Updates in Batch-Reihenfolge auf `LivePoolCache` anwenden, danach hoechstens **einen** WSOL-abgeleiteten Preis-/Sticky-Pfad pro `pool_address` und Batch (gleiche Monotonie-Regel wie Scope B); `position_price_updates` zaehlt nur noch tatsaechlich angewendete `update_position_price`-Updates (`bool`-Rueckgabe). Erweiterte Batch-Logs: `unique_pools_touched`, `coalesced_price_path_keys`, `stale_price_path_candidates`, `max_slot_lag_vs_head` (nur wenn `last_slot` und Update-Slot > 0), `execution_results_drained_after_batch`, `decision_gate_shard_hint_permille`. (3) `update_position_price` liefert `bool` fuer saubere Metrik. |
| **Betroffene Module** | `src/bin/momentum_bot.rs` |
| **Regression-Pruefung** | Neue Unit-Tests fuer Bonding-Coalescing, Pool-Winner-Auswahl, Stale-Zaehlung und I-13 Pool-Match; `cargo test` / `cargo clippy --all-targets -D warnings`. |
| **Tags** | [momentum, scope-c, coalescing, observability, jetstream, i-13, i-16] |

---

## FIX-50: Momentum Entry — pool-scoped `TokenTracker` + PumpFun-Migration-Gate (kein stale `pumpfun`-Probe)

| Symptom | Probe-BUY-Intents auf alter `pumpfun` Bonding-Curve-Adresse nach Migration; Execution verwirft mit `UNSUPPORTED_INTENT` / „bonding curve is complete (migrated)“. Frische `pump_amm`-Aktivitaet desselben Mints wurde auf den Mint-gescopten Tracker aggregiert und falscher Pool im Signal ausgegeben. |
|---------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Root Cause** | `token_trackers` war nur mint-keyed; `record_trade` aktualisierte immer dieselbe Tracker-Zeile; `EntrySignal.pool` kam aus `tracker.pool` beim ersten Pool — Pool-wechsel/Migration verschmutzten die Entry-Entscheidung (Failure-Pattern `momentum_pool_scoped_entry`). |
| **Fix** | (1) Map-Key `(mint, pool)` via `tracker_storage_key`; Trades/Creator/Dev-Flows pool-gezielt; mint-weite Guards (Position-Pool I-13, max. ein pending BUY pro Mint, Serialisierung wenn anderer Pool `ProbeBuyPending`/`ScaleInPending`). (2) Vor BUY-Signal: `pumpfun_entry_blocked_by_migration` aus `mint_pools`-Row und Geyser/LivePoolCache-Evidence — nur fuer `dex == pumpfun`; `pump_amm`/andere DEX unberuehrt. (3) BUY-/Exit-Hilfen: `try_get_dex_pool_accounts_for_mint_pool`, ExecutionResult- und Exit-Pfade auf Pool-Key. |
| **Betroffene Module** | `src/bin/momentum_bot.rs` |
| **Regression-Pruefung** | `pool_scoped_entry_probe_targets_active_pool_not_legacy_pumpfun_row`, `pumpfun_complete_blocks_probe_while_pump_amm_remains_eligible`; `cargo fmt`, `cargo clippy --all-targets -D warnings`, `cargo test`. |
| **Tags** | [momentum, entry, pumpfun, migration, multi-pool, i-13, i-16, i-7] |

---

## MD-STATE-DROP-ROOT-CAUSE: md-state Queue Cap-Stall nach PR231 (2026-06-21)

| Symptom | Nach PR231 (`account_worker_queue_depth=0`, `early_drop` OK): `md_state_queue_depth` dauerhaft 8192, `geyser_tracking_jobs_processed_total` 0/s, `geyser_tracking_enqueue_dropped_total` ~1000/s. |
|---------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Root Cause** | (RC-1) `apply_arb_multi_dex_pins_for_pool` lief bei jedem `RegisterPoolVaultsFromAccount` auch bei idempotentem Register. (RC-2) Triple-Enqueue pro Hot-Pool Account-Upsert (`UpdateArbCoverageIndex` + `Register` + `ArbMultiDexReconcile`). (RC-3) Jeder Vault-Balance-Tick enqueued `TouchVault` (O(pool) Scan). (RC-4) `TouchBinArray` direkt aus Account-Worker ohne Coalesce. (RC-5) `Register` auch wenn Vault-Rows bereits stabil. |
| **Fix** | Reconcile/Pin-Promotion nur bei `vaults_changed`; redundante sidefx-Enqueues entfernt; `pool_needs_tracking_refresh_after_cache_upsert`-Guard; Vault/Bin LRU-Touches per md-sidefx-Burst coalesced → `TouchTrackedLruBatch`; `touch_tracked_vault_pubkey` O(1). **Keine** Queue-Cap-Erhöhung. |
| **Betroffene Module** | `src/bin/market_data.rs` |
| **Regression-Prüfung** | `cargo test --bin market-data`; Prod: `geyser_tracking_jobs_processed_total` > 0, Drops ≈ 0, `md_state_queue_depth` unter Cap. |
| **Tags** | [market-data, md-state, geyser, hot-path, i-4b, i-16, pr230, pr231] |

---

## HYBRID-PHASE1: Ingest/Sidefx entkoppelt von tracked_* RwLocks + Register-Flood gestoppt (2026-06-23)

| Symptom | Nach PR237 weiterhin `md_state_queue_depth` @8192, `burst_in_progress=1`, ~45× `tracked_vaults.read()` in Sidefx, massenhaft `RegisterReservesAfterTrade` / `RegisterPoolVaultsFromAccount` aus TX/Account-Parse. |
|---------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Root Cause** | PR237 deckte nur zwei Ingest-Filter ab; Vault-Balance-Ticks, Bin-Array-Parse und Account-Cache-Upsert blockierten weiter auf `tracked_*` Maps; Trade- und Account-Sidefx enqueued Register-Jobs in md-state. |
| **Fix** | (1) `TrackedMembershipSnapshot` um `SnapshotVaultView` / `SnapshotBinArrayView` mit `Arc<AtomicU64>` Balances erweitert; nur md-state refreshed Snapshot. (2) Alle Ingest/Sidefx-Pfade auf Snapshot umgestellt (0 `tracked_*.read()`). (3) `RegisterReservesAfterTrade` + Arb-Reconcile aus TX-Handler entfernt; `RegisterPoolVaultsFromAccount` aus Sidefx-Flush entfernt. Vault-Ticks nutzen `snapshot_vault_pair_balances` + LivePoolCache. |
| **Betroffene Module** | `src/bin/market_data.rs` |
| **Regression-Prüfung** | `phase1_*` source-body tests; `cargo fmt`, `cargo clippy`, `cargo test`; Eval Level 5. |
| **Tags** | [market-data, hybrid-rollback, phase1, i-4b, ingest, md-sidefx, pr238] |

---

## HYBRID-PHASE2C: ArbMultiDex-Reconcile aus md-state entfernt (2026-06-24)

| Symptom | md-state-Queue weiterhin mit Arb-Entscheidungs-/Reconcile-Jobs belastet; Trade-Pfad und UnifiedHotPoolRegistry triggerten Arb-Pin-Heuristik in MD statt Strategy/Track-Worker. |
|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Root Cause** | Phase 2b zog Momentum auf Track-Worker; verblieben: `ArbMultiDexReconcile`, `UpdateArbCoverageIndex`, `reconcile_arb_multi_dex_*`, Arb-Zweig in `UnifiedHotPoolRegistry`, Trade→Arb-Reconcile-Enqueues. |
| **Fix** | (1) `MdStateCommand::ArbMultiDexReconcile` + `UpdateArbCoverageIndex` + Handler/Coalesce entfernt. (2) `ArbCoverageIndex`, reconcile/pin-Hilfsfunktionen und Arb-Registry-Felder gelöscht. (3) `register_geyser_reserves_after_trade` nur noch für Momentum-Hot-Pools. (4) `GeyserPinReason::ArbMultiDex` legacy-read-only. (5) `phase2c_*` Source-Body-Tests. Arb-Track-Requests = Phase 3. |
| **Betroffene Module** | `src/bin/market_data.rs` |
| **Regression-Prüfung** | `cargo fmt`, `cargo clippy -D warnings`, `cargo test`, `phase1_*`/`phase2a_*`/`phase2b_*`/`phase2c_*`; Eval Level 5 (separater Eval-PR für I-4c). |
| **Tags** | [market-data, hybrid-rollback, phase2c, i-4c, md-state, arb, pr241] |

---

## HYBRID-PHASE3: Arb track_requests NATS + MD track-worker consumer (2026-06-25)

| Symptom | Nach Phase 2c fehlten Arb-Geyser-Pins; arb-strategy Metrics HTTP :9803 timeout unter Last (2-hop sync im MarketEvent-Worker). |
|---------|--------------------------------------------------------------------------------------------------------------------------------|
| **Root Cause** | Arb-Pinning war strategy-owned noch nicht wieder angebunden; `handle_trade` / `check_arbitrage` blockierte den priorisierten MarketEvent-Worker synchron. |
| **Fix** | (1) Neues Topic `ironcrab.v1.arb.track_requests` — arb-strategy publiziert, market-data subscribed + `md-track-worker` (`ApplyArbTrackRequests`). (2) Pool-zentrische Pins mit `GeyserPinReason::ArbMultiDex`, Wallet/Momentum geschützt. (3) Baseline-Reconcile ~60 s + inkrementelle `multi_dex`/`trade_signal` publishes. (4) 2-hop Detection auf dedizierten `arb_two_hop_worker` Channel (Scope D). |
| **Betroffene Module** | `src/nats/arb_track_requests.rs`, `src/bin/market_data.rs`, `src/bin/arb_strategy.rs`, `src/metrics.rs` |
| **Regression-Prüfung** | `cargo fmt`, `cargo clippy -D warnings`, `cargo test`, `phase3_*`; Eval Level 5. |
| **Tags** | [arb-strategy, market-data, hybrid-rollback, phase3, i-4e, nats, track-worker, pr242] |

---

## HYBRID-PHASE4: Momentum Position/Wallet SSOT P1–P2–P4 (2026-06-25)

| Symptom | Ghost positions / exit sizing drift when JetStream `WalletBalanceSnapshot` races with confirmed `ExecutionResult` fills. |
|---------|-----------------------------------------------------------------------------------------------------------------------------|
| **Root Cause** | Dual-path balance authority: Scope 57 preferred wallet snapshot over `PositionTracker` for exit sizing; snapshot `balance=0` could auto-close Live positions before SELL confirm. |
| **Fix** | (P1) Audit `docs/PHASE4_BALANCE_SOURCE_AUDIT.md`. (P2) Confirmed BUY/SELL mutates `token_amount` only via `ExecutionResult`; snapshot hint-only + guarded zero-close for `WalletSnapshot` entry source. (P4) `momentum_wallet_balance_divergence_lamports{mint}` + `momentum_wallet_balance_divergence_total`. |
| **Betroffene Module** | `src/bin/momentum_bot.rs`, `src/metrics.rs`, `docs/PHASE4_BALANCE_SOURCE_AUDIT.md` |
| **Regression-Prüfung** | `cargo fmt`, `cargo clippy -D warnings`, `cargo test`, `cargo test phase4_`; Eval Level 5. |
| **Tags** | [momentum-bot, hybrid-rollback, phase4, wallet-ssot, i-13, ghost-positions, pr-phase4] |

---

## MD-PHASE4-TX-INGEST: Geyser TX Handler nach `ingest/` extrahiert + systemd StateDirectory (2026-07-06)

| Symptom | `market_data.rs` Monolith ~15.5k LOC; TX-Ingest-Logik im Bin schwer wartbar; Prod-Deploy #269 `exit 226/NAMESPACE` weil `/var/lib/ironcrab` ohne `StateDirectory` fehlte. |
|---------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Root Cause** | Phase 1–3 Dual-Consumer prod-abgenommen, aber Monolith-Abbau (Plan Phase 4) noch offen; systemd `ReadWritePaths=/var/lib/ironcrab` ohne vorab erstelltes Verzeichnis. |
| **Fix** | (1) `handle_geyser_transaction_update` + TX-only Parse/Emit-Hilfen nach `src/market_data/ingest/tx_handler.rs` / `tx_parse.rs`; `TxIngestHost`-Trait. (2) Bin: dünner `handle_geyser_transaction`-Wrapper. (3) `StateDirectory=ironcrab` in `docs/systemd/market-data.service`. Semantik P1/P2/P3 unverändert. |
| **Betroffene Module** | `src/market_data/ingest/`, `src/bin/market_data.rs`, `docs/systemd/market-data.service` |
| **Regression-Prüfung** | `cargo fmt`, `cargo clippy -D warnings`, `cargo test`, `cargo test phase4_`; Eval Level 5. |
| **Tags** | [market-data, hybrid-rollback, phase4, tx-ingest, monolith-slice, systemd, i-md-1, pr-phase4-tx] |

---

## MD-PHASE4B-ACCOUNT-INGEST: Geyser Account Handler nach `ingest/` extrahiert (2026-07-06)

| Symptom | `market_data.rs` Monolith ~15k LOC nach Slice 1; Account-Ingest-Logik (~500 LOC) noch im Bin. |
|---------|--------------------------------------------------------------------------------------------------|
| **Root Cause** | Phase 4 Slice 1 extrahierte nur TX-Pfad; Account-Pfad (`handle_geyser_account`) blieb im Monolith. |
| **Fix** | (1) `handle_geyser_account_update` + Account-only Parse-Hilfen nach `src/market_data/ingest/account_handler.rs` / `account_parse.rs`; `AccountIngestHost`-Trait. (2) Bin: dünner `handle_geyser_account`-Wrapper. (3) `phase4b_*` grep-Tests. Semantik P2 enrichment / I-MD-4 unverändert. |
| **Betroffene Module** | `src/market_data/ingest/`, `src/bin/market_data.rs` |
| **Regression-Prüfung** | `cargo fmt`, `cargo clippy -D warnings`, `cargo test`, `cargo test phase4_`; Eval Level 5. |
| **Tags** | [market-data, hybrid-rollback, phase4b, account-ingest, monolith-slice, i-md-2, i-md-4, pr-phase4b] |
