
# IronCrab – Tasks & Meilensteine

## 1) Treasury & Transfers
- [x] SOL-Balance lesen (`Treasury::sol_balance`)
- [x] ATA-Berechnung & Erstellung (spl-associated-token-account)
- [x] SOL/Token-Transfers + Rent Exemption
- [x] WSOL wrap/unwrap (sync_native / close)

## 2) DEX-Connectoren
- [x] Raydium: Pool Scan (`refresh_pools`), PDA Ableitungen (amm authority, serum vault signer), Fee Extraction, Vault Balances
- [x] Raydium Quote (`quote_exact_in`) + Slippage Helper + Invarianten Tests
- [x] Raydium Swap Plan (`build_swap_plan`): Compute Budget (limit + price) + min_out Assertions
- [x] Raydium Full Swap Instruction (`build_swap_instruction`) – nutzt Snapshot Felder (open_orders, market, authority, target_orders)
- [x] Orca Whirlpool: Heuristische Pool Decodierung (Mints, Vaults, FeeTier Key, Tick Spacing, Tick Index), Vault Balances
- [x] Orca Fee Tier Accounts: Fetch + Override heuristischen Fee Scan
- [x] Orca Swap (Placeholder Whirlpool Swap IX) mit Tick Array & Oracle PDAs (heuristisch) + Vaults + FeeTier
- [x] Routing: Single-Hop Best Quote + Depth‑2 Multi-Hop (Quote + Ausführungs-IX Kette)
- [x] Multi-Hop min_out Aggregation (nur finaler Hop slippage)
- [x] Gemeinsames Quote-Struct erweitert (input_mint/output_mint) für Routen-Rekonstruktion
- [x] Orca: Replace Heuristics mit Echtem Whirlpool Layout (feste Offsets / strukturiertes Parsing)
- [x] Mint->Pool Mapping Index (Raydium & Orca) für schnelle Liquidity Lookups (Sniper)
- [x] Orca: Echte User Accounts & Token Owner Accounts in Swap IX (statt Platzhalter Pubkeys) (Setters + validation in build_swap_ix)
- [x] Raydium: Offene Verbesserung – Vault Fallbacks entfernen (immer echte Vaults nutzen + skip invalid fee / zero reserves)
- [x] Safety: Further structural validation (Orca tick spacing bounds, vault/mint cross-check, tick index sanity)
- [x] Benchmarks für Quote & Refresh (Lightweight timing harness test `bench_quote_refresh.rs`)

Status: Kernfunktionen der DEX-Connectoren (Quote, Swap-Plan, Swap-IX, Multi-Hop Routing) sind implementiert; verbleiben sind Genauigkeit (Whirlpool Layout), echte Kontoersetzung und Hardening.

## 3) Arbitrage
- [x] DEX Quotes aggregieren, beste Edge wählen (aggregate_best_edges + best_edge)
	- Erweiterungen: enumerate_triangular_cycles, TransactionPlan + Simulation
- [x] Profit Ranking (rank_triangular_cycles) für Triangular Cycles
- [x] N>3 Hop Cycle Enumeration (enumerate_cycles_generic + ranking)
	- [x] Dominance + Upper-Bound Pruning (verlustfrei) für generic cycle search
	- [x] Cycle Search Metrics (partial examined, dominance pruned, bound pruned, completed)
- [ ] Jito/MEV-Integration (später)
- [x] Pre-TX Simulation (RPC `simulateTransaction`)

## 4) Sniper
- [x] Grund-SniperEngine Skeleton + Config Struct
- [x] WS-Logs Subscribe (Pool-Create) via raw WebSocket (manual logsSubscribe JSON-RPC)
- [x] Heuristiken: Config Felder (Blacklist Mints/Owners, Min Liquidity, FreezeAuth None, Decimals Range) implementiert; Runtime Checks integriert – echte Pool Account / Mint Fetch & LP-Lock Analyse implementiert
- [~] Pool Account & Mint Fetch (Owner, FreezeAuth, Decimals, Supply, Liquidity) – WebSocket detection in place; detailed account fetch & liquidity calc pending
		- [x] Liquidity Heuristik: Index-basierte SOL/Stable Schätzung (Raydium Pools + Fallback largest accounts; Orca TODO Snaps) (Oracle TODO)
- [x] LP-Lock / Konzentrations-Heuristik (Top1/Top3/Top5 via largestAccounts + Log-Integration)
- [~] LP-Lock / Token Distribution Heuristik (Burn + Program-Owned Vault Erkennung integriert in Konzentrationsberechnung; weitere Verfeinerung offen)
- [x] Erstkauf TX Skeleton (Raydium Auto Swap Plan -> TX bauen & senden, WSOL wrap, ATA ensure, Purchase Tracking, Post-Trade WSOL Unwrap)
- [x] SL/TP Grundlogik (Stop-Loss & Take-Profit Trigger, periodische Evaluation)
- [x] Realized PnL über WSOL-Delta beim Exit (transaktions-lokal)
- [x] Unrealized PnL Update pro Evaluationszyklus
- [x] Trade CSV Logging (rotierend pro Tag, Env `IRONCRAB_TRADE_LOG_DIR`)
- [x] PendingTrade Map für FILL Shortfall (expected vs actual Tokens) – integriert
- [x] Shortfall Berechnung (Tokens & SOL Äquivalent) bei FILL
- [x] Netzwerk Fee Schätzung via `get_fee_for_message`
- [ ] Erweiterte Fee Aufschlüsselung (Protocol Fee, Referrer, Compute Budget Overhead via Meta)
	- [x] Grundlegende Protokoll Fee Approx (Output Token Fee Heuristik + Metrics `protocol_fee_tokens_total`, `protocol_fee_sol_total`)
	- [x] Exakte Network Fee (Transaction Meta)
	- [~] Token Fee via postTokenBalances (wrapper type TODO; fallback heuristic retained)
- [x] Partielle Exit-Unterstützung (erste Version: teilweiser TP (50%), vollständiger SL Exit); weitere gestaffelte SELL Reihenfolge optional
- [x] Persistenter Positionssnapshot (Reload nach Neustart, multi-lot Unterstützung)
  
### Nächste Micro-Tasks (aktualisiert)
1. Orca: Vollständige Whirlpool Layout Implementierung (strukturierter Parser, feste Offsets, verifizierte Tick Index & Liquidity Fields) – DONE (strict parser + semantic validation)
2. Orca Swap: Ersetzen der Platzhalter User Accounts (Authority, Source/Dest Token Accounts) + Option für sqrt_price_limit – DONE (Sniper nutzt echte Authority & ATAs, min_out via Quote + Slippage)
3. Raydium: Entfernung von Vault-Fallbacks (immer echte Vault Pubkeys) & zusätzliche Hard Validation (target_orders optional, aber loggen)
4. Router: Depth‑2 Pfad Benchmark + Depth‑3 (erste Greedy Implementierung + Pruning) – DONE (Depth‑3 implementiert)
5. Arbitrage Aggregator: Triangular Path (A-B-C-A) Profit Check (greedy implemented) + Erweiterung für generische Zyklen
	- [x] Net Profit Filter (min_profit_bps + est_tx_cost_lamports)
	- [x] TransactionPlan Scaffold (triangle assembly)
	- [x] Pre-TX Simulation (RPC simulateTransaction)
6. Compute Budget: Dynamische CU-Schätzung (historische Simulation / heuristics) statt fixer Werte
	- [x] Erste heuristische Implementierung (Estimator Modul + Raydium build_swap_plan_auto)
7. Metrics & Observability
	- [x] Prometheus HTTP Exporter (Port 9898)
	- [x] Swap Latency Histogram
	- [x] Quote Latenz Histogramm
	- [x] Trade / RPC Error / Open Positions / Realized PnL / Liquidity Gauges & Counter
	- [x] Slippage / Shortfall Metrics (Aggregierte Shortfall Tokens & SOL)
	- [x] Fee Breakdown (Aggregierte Netzwerk Fees)
	- [x] PnL Distribution Histogram (realized trade return buckets; -90%..+200% with cumulative buckets)
8. CI Pipeline (fmt, clippy, test, optional wasm build)
9. Config Hot-Reload (Signal / File Watch) für Routing/Slippage Parameter
10. Risk Layer
	- [x] Max Notional pro Trade (Config `max_position_sol`)
	- [x] Daily Loss Limit (`daily_loss_limit_sol` + Tagesreset)
	- [x] Positions Open Gauge
	- [x] Cooldown pro Mint nach SL Exit (Konfig `stop_loss_cooldown_secs`)
	- [x] Dynamischer Size-Scaler (Drawdown Adjust: `drawdown_scale_start`, `drawdown_max_reduction`)
11. Sniper: Orca Pools in Index-Liquidität aufnehmen – DONE (Snapshot integriert)
12. Sniper: Verbesserte min_out Berechnung für Raydium & Orca (quantile impact) – PARTIAL (Orca basic slippage, quantile TODO)

### Neue Folge-Themen
- Persistenz: Speichern offener Positionen & Tages-PnL in lokaler DB (sled oder sqlite)
- Konfigurierbare Rotation / Aufräumen alter Trade Logs (> N Tage)
- MEV / Jito Bundle Versand für Front-Run Schutz
- Adaptive Slippage: dynamische Anpassung basierend auf beobachteter Ausführungsabweichung
- Simulation basierter Pre-Trade Impact Score

## 5) Risk & Limits
- [x] Positions-/Notional-Limit (max_position_sol pro Trade)
- [x] Daily Loss Limit Tracking + Reset Tageswechsel
- [x] Realized & Unrealized PnL intern (Realized via WSOL Delta, Unrealized via Quotes)
- [x] Per-Mint Positions-Limit (konfigurierbar, multi-lot enforced)
- [x] Cooldowns nach Stop-Loss (HashMap cooldown_until)
- [x] Erweiterte PnL Reports (rolling window + Sharpe Approx `rolling_pnl_window`)
- [x] Config Hot-Reload (ENV `IRONCRAB_SNIPER_RELOAD_PATH`, 30s Interval)
- [x] Persistente RiskState Speicherung (Positions & Sharpe) (Grundpersistenz + Multi-Lot fertig; Sharpe + erweiterte Metriken DONE)
	- [x] Basis Persistenz (JSON Snapshot: ENV `IRONCRAB_RISK_STATE_PATH`, Autosave `IRONCRAB_RISK_AUTOSAVE_SECS`)

## 6) Infra & Observability
- [x] Structured Metrics (Prometheus Exporter Port 9898)
- [x] Trade CSV Logs (rotierend, Shortfall & Fees)
- [x] Config-Reload (Polling + Diff Logging + optional File Watch feature `notify_watch`; SIGHUP signal trigger on Unix)
- [x] CI: `cargo fmt`, `cargo clippy`, Tests (GitHub Actions Workflow)
- [x] Liveness / Readiness Endpoints (/live, /ready)
- [x] Sharpe & Drawdown Gauges + +Inf Bucket & Build Info Metric
- [x] Grafana Dashboard Beispiel (JSON) (Panels finalized; see docs/grafana_dashboard_example.json; alert suggestions in README or Grafana alerting)

## 7) Production Hardening & Open Roadmap

### Execution & Routing
- [x] Stabiler WebSocket PubSub (Reconnect, Backoff, Heartbeat)
	- [x] Reconnect + Exponential Backoff + Metric (`ws_reconnects_total` + jitter)
	- [x] Heartbeat Timer + Stale Detection (90s silence -> reconnect, metric `ws_heartbeat_misses_total`)
	- [x] Message Counting & Active Connections (`ws_messages_total`, `ws_active_connections`)
	- [x] Adaptive Backoff w/ server error codes (DONE)
	- [x] Multi-endpoint failover / rotating RPC WS URLs
- [x] Volle Raydium Serum Market Accounts erzwungen (kein Fallback)
- [x]  (Raydium vs Orca dynamisch)
- [x] Partielle Exits (gestaffelte SELL Orders / Positionsplits)
- [x] Retry & Backoff Strategie bei transienten RPC Fehlern
	- [x] Grund-RPC Retry (Exponentiell, 3 Versuche, Metric `rpc_retry_attempts_total`)
- [x] PendingTrade TTL + Reconciliation (Zombie Pending entfernen)
	- [x] TTL Cleanup (Config `pending_trade_ttl_secs`) – implemented
	- [x] Reconciliation (Signature status check + finalize/dismiss logic, metrics `pending_reconciliations_total`, `pending_failed_total`)
- [x] Re-Quote unmittelbar vor Signatur (Front‑run Schutz / aktualisiertes min_out)

### State & Persistence
- [x] Persistenter RiskState Snapshot (offene Positionen, realized PnL, rolling returns, Sharpe)
- [x] Per-Mint Mehrfachpositionen (konfigurierbar per_mint_position_limit)
- [x] Periodische Flush / Recover Logik beim Shutdown

### Fees & PnL Genauigkeit
- [~] Protocol / LP / Referral Fees Extraktion aus Transaction Meta (postTokenBalances Delta & Meta-basierte Aggregation implementiert; DEX-spezifische Fee-Vault Attribution pending)
- [x] Brutto vs Netto PnL Metriken (separate Gauges)
- [x] Fee % des Notionals als Histogram

### Data & Pricing
- [x] Echte SOL/USD & Stable Preise via Oracle (Pyth / Switchboard) – basic readers wired with safe fallbacks (preference + override)
- [x] Präzisere Liquidity Schätzung (Reserven * Mid-Price, multi-pool Aggregation) – uses SOL/USD override when paired with stables
- [x] Adaptive Slippage basierend auf empirischer Fill-Abweichung – rolling mean shortfall drives slippage bps toward target within min/max; persisted in risk state

### Stability & Concurrency
- [x] Separate Task für Exit Evaluation (konfigurierbares Intervall) – `exit_eval_interval_secs` steuert das Intervall
- [x] Graceful Shutdown (Flush, Snapshot, Metrics finalisieren via watch channel)
- [x] Rate Limit Erkennung + adaptiver Parallelismus
	- [x] Adaptive RPC Concurrency Limiter (spin-wait permits, dynamic allowed window, success-driven increase, rate-limit/timeout-driven decrease)
	- [x] Error classification (429/Too Many Requests/Throttle -> rate limit, timeouts) with retries + exponential backoff
	- [x] Metrics: rpc_rate_limit_hits_total, rpc_timeouts_total, rpc_backoff_ms_total, rpc_inflight, rpc_allowed_concurrency, rpc_concurrency_adjustments_total
	- [x] Configurable knobs (rpc_min/max/initial_concurrency, rpc_inc_every_successes, rpc_dec_on_rate_limit, rpc_timeout_ms)
	- [x] Test-Helpers Feature (`test_helpers`) für deterministische State Mutation / Sharpe Tests

### Logging & Observability Erweiterungen
- [x] +Inf Bucket für trade_return Histogram (Prometheus Konvention)
- [x] Absolutes Realized PnL Histogram (SOL)
 - [x] Shortfall Prozent Histogram
- [x] Sharpe & Drawdown Gauges
- [x] Build / Version Metric
	- [x] Interne Sharpe Validierung (Fee Impact & Rolling Window Truncation Tests)

### Backtesting & Strategy
- [x] `py_strategy` FFI (pyo3 oder IPC) für externe Signale
	- [x] API‑Contract: JSON Schema für `StrategyDecision` (Backtest akzeptiert JSON; Engine-pyo3 erwartet `on_tick()->JSON`)
	- [x] IPC Pfad (Feature `python_ipc`): Persistenter Subprozess mit Line‑Protocol; Timeout + Circuit Breaker
	- [x] pyo3 Pfad (Feature `python`): `engine::py_strategy::PyStrategy` lädt Modul/Klasse und ruft `on_tick()` unter GIL; Param‑JSON Übergabe
	- [x] Strategy‑Lifecycle: `init`, `on_tick`, `on_fill`, `on_exit` – Backtest IPC voll integriert; Engine‑pyo3 Tick integriert
	- [x] Sandbox & Isolation: Backtest IPC per‑call Timeout + Restart/Circuit; Engine Runtime Timeout/Panic‑Catch + Circuit
	- [x] Beispielstrategie (`strategies/sample.py`, `strategies/sample_worker.py`) + README‑Hinweis
	- [x] CLI (Backtest): `--py-script` schaltet Python‑IPC Strategie ein
- [x] Deterministischer Replay-Modus (Slot Iterator)
	- [x] Slot‑Iterator (Start..End) mit lokalem Cache für Blöcke/Transaktionen/Logs (in‑memory `ReplayStore` für Slots/Logs/Accounts)
	- [x] Mock `SolanaRpc` für Replays (get_account/get_multiple_accounts/logs + `all_latest()` aus Trace)
		- Implementiert: `ReplayRpc` mit `get_account`, `get_multiple_accounts`, `logs_in_range`, `all_latest` (genutzt für Decoding)
		- Offen: optionales direktes `get_program_accounts` API (derzeit durch `all_latest` + Filter ersetzt)
	- [x] Determinismus: feste Timestamps via `slot_ms`, seedbares RNG Feld reserviert (für Impact/Noise); deterministischer Ablauf im Backtest
	- [x] Recorder‑Tool: Live‑Stream (Blöcke/Logs/Accounts) in Dateien schreiben (kompakt, komprimiert)
		- Neues Binary `ironcrab-recorder`: schreibt JSONL‑Trace (gzip, .jsonl.gz) kompatibel zu `TraceEvent` (Slot/Log/Account base64)
		- Logs via WS logsSubscribe (Raydium/Orca), Accounts via periodischem get_program_accounts Dump
	- [x] CLI/Config: `--replay*` Flags (Slot‑Range, Quelle, slot_ms, seed) implementiert: `--replay-trace`, `--replay-start`, `--replay-end`, `--replay-slot-ms`, `--replay-seed` (Metriken DONE: replay_mode, start/end slot, slot_ms, seed, events/slots/new_pools/price_updates, ingested pools)
	- [x] Trace Loader: JSON/JSONL → TraceEvent → SimEvent (Slots, Logs, Account→NewPool+CfmPriceUpdate)
	- [x] Golden Tests: Minimaler Replay‑Test (Deterministische Slot‑TS, Account‑Mapping)
	- [x] Raydium Replay Refresh: Pool‑Snapshots aus `ReplayRpc` lesen (`fetch_pools_replay`/`refresh_pools_replay`) und im Backtest‑Driver vorinitialisieren
	- [x] Orca Replay Refresh: Whirlpool‑Snapshots aus `ReplayRpc` lesen (`refresh_pools_replay`) und im Backtest‑Driver vorinitialisieren
- [x] Impact / Slippage Modell im Backtest
	- [x] DEX‑spezifische Modelle: Raydium (CPMM) vs. Orca Whirlpool (konzentrierte Liquidität, Tick‑Kreuzungen)
		- Implementiert: Pluggable ImpactModel (CPMM exakt, CLMM mit konservativer Zusatz‑Penalty für große Trades)
		- CLI: `--impact cpmm|clmm|none` wählt Modell für min_out‑Berechnung
	- [x] Gebühren & Ticks: Zusätzliche Protocol/Referral Fee (bps) + einfache Latenz‑Penalty
		- Neue Flags: `--impact-extra-fee-bps` (Output‑Abschlag) und Nutzung `--replay-slot-ms` für Latenzmodell (`10 bps/Slot`, Kappung)
	- [x] Shortfall‑Noise: Stochastischer Aufschlag (Normalverteilung, 0‑trunkiert) auf `max_slippage_bps`
		- Flags: `--impact-noise-mean-bps`, `--impact-noise-std-bps`, Seed via `--replay-seed` (deterministisch)
	- [x] Szenario‑Runner: Parametrisierte Sweeps (Size, Slippage Bps) + ScenarioMeta‑Injection; Impact‑Knobs (extra fees, noise, latency)
	- [x] Validierung: Vergleich Backtest‑PnL vs. historische Live‑Trades (Fehlerbänder) – DONE (Backtest-Driver `--validate-live-csv`; Report: n, MAE, MAPE, within 1/2/5%)

### Security & Key Handling
- [~] Gesicherte Keypair Ladepfade / optional KMS
	- [x] Gesicherte Ladepfade (Strict Mode + erlaubte Verzeichnisse, ENV Loader JSON/B64/Base58)
	- [ ] KMS‑Backend (Feature‑gated Remote Signer)
- [x] Keine Private Keys in Logs (Audit Layer)
- [x] Config Validierung (Schema + Constraints)

### Tests & CI
- [x] Unit: drawdown sizing, cooldown gating, trade_return bucketing
- [x] Integration: Mock RPC Buy->Fill->Sell Lifecycle
- [x] Fuzz: Log Parser & Pool Snapshot Decoder (cargo-fuzz targets for replay log parser and Orca Whirlpool layout)
- [x] Load / Stress: Quote & Swap Latency unter Last (Neues Binary `latency_stress` für parallele Quote-/Swap-Plan Messungen)
	- Features: Pairs‑Pinning (`--pairs A->B`), gewichteter Mix (`--w-single|--w-hops2|--w-hops3|--w-plan2`), Dauer/Parallelität konfigurierbar
- [ ] GitHub Actions: clippy, fmt, test, audit
	- [x] Multi-Lot Partial Exit Mathe Test
	- [x] Partial Exit State Mutation & Fee / Sharpe Window Tests (gated via feature)

### Resilience & Edge Cases
- [x] 0-Reserve Pool Handling (Skip & Metric)
	- Metrics: `raydium_pools_skipped_zero_reserve_total`, `orca_pools_skipped_zero_reserve_total`
- [x] Token ohne Decimals Info (Fallback & Warn)
	- Behavior: Use getTokenSupply.decimals; fallback to mint[44]; else default 0 with warn
	- Metrics: `mint_decimals_source_supply_total`, `mint_decimals_source_account_total`, `mint_decimals_fallback_default_total`
- [x] Overflow / Extremwerte Guard für Returns & Fees
	- Returns: clamp to histogram bounds; sum saturates to i64 micro units
	- Fees/Shortfall %: sanitize NaN/Inf; clamp to [0,1]
	- Hinweis: Wallet und Sniper nutzen gemeinsamen Decimals‑Helper (`solana::token_utils`)

### Developer Experience / Config
- [ ] Erweiterte Dokumentation der neuen Risk Parameter
- [x] Diff Logging bei Hot Reload (was hat sich geändert?)
	- Implementiert: `diff_sniper_cfg` vergleicht Felder & loggt Änderungen (Hot Reload Task)
	- Optional Echtzeit File Watch via Feature `notify_watch` (aktivieren mit `--features notify_watch`)
	- SIGHUP Signal Handler (Unix) als alternativer Trigger – implemented
- [ ] Start Skripte (Windows/Unix) für build/run/backtest
- [ ] Konfigurierbare Log Rotation & Retention (N Tage)

#### Go‑Live Wiring (offen)
- [ ] Engine::execute finalisieren – TradeIntent → DEX Routing (Raydium/Orca), `build_swap_plan(_auto)` + `build_swap_instruction`, TX signieren/senden, Metrics/CSV‑Logs aktualisieren
- [ ] DummyRustStrategy ersetzen oder Beispiel‑Rust‑Strategie hinzufügen, die echte `TradeIntent`s produziert (kleine, sichere Notionals; konfigurierbar)
- [ ] config.example.toml um minimalen `[sniper]`‑Block erweitern (sichere Defaults: Limits, Slippage, Cooldowns), Quickstart in README ergänzen
- [ ] main.rs: Treasury‑Laden mit ENV‑Fallback erlauben (`Treasury::load_from_env().or_else(|_| Treasury::load(path))`) für `IRONCRAB_KEYPAIR_*`
- [ ] Quickstart: Hinweis auf ausreichende SOL‑Balance (Rent+Fees), RPC/WS Erreichbarkeit, Metriken auf :9898
- [ ] Tests: Unit‑Tests für Token‑Decimals Fallback‑Pfad (supply/account/default) und Clamping‑Logik (returns/fee/shortfall %)
- [ ] Grafana: Panels für Zero‑Reserve Skips, Decimals‑Quellen (`mint_decimals_*`) und Quote/Swap‑Latenzen finalisieren

### Priorisierte Reihenfolge (Empfehlung)
1. PendingTrade TTL & State Persistenz — DONE
2. Retry/Backoff + WebSocket Stabilität — DONE
3. Protocol Fee Parsing & Multi-Position Support — PARTIAL (DEX-spezifische Fee-Vault Attribution PENDING)
4. Partielle Exits & Best-Route Auswahl — DONE
5. Tests & CI Pipeline — PARTIAL
6. Sharpe/Drawdown Gauges & +Inf Bucket — DONE
7. Oracle Preise & Adaptive Slippage — DONE
8. Backtest FFI & Impact Modell — PENDING

