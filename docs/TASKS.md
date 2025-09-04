
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
- [~] Heuristiken: Config Felder (Blacklist Mints/Owners, Min Liquidity, FreezeAuth None, Decimals Range) implementiert; Runtime Checks integriert – fehlend: echte Pool Account / Mint Fetch & LP-Lock Analyse
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
4. Router: Depth‑2 Pfad Benchmark + Depth‑3 (erste Greedy Implementierung + Pruning) [Depth‑3 implemented]
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
- [~] Persistente RiskState Speicherung (Positions & Sharpe) (Grundpersistenz + Multi-Lot fertig; Sharpe + erweiterte Metriken offen)
	- [x] Basis Persistenz (JSON Snapshot: ENV `IRONCRAB_RISK_STATE_PATH`, Autosave `IRONCRAB_RISK_AUTOSAVE_SECS`)

## 6) Infra & Observability
- [x] Structured Metrics (Prometheus Exporter Port 9898)
- [x] Trade CSV Logs (rotierend, Shortfall & Fees)
- [~] Config-Reload (Polling + Diff Logging + optional File Watch feature `notify_watch`; SIGHUP signal trigger PENDING)
- [x] CI: `cargo fmt`, `cargo clippy`, Tests (GitHub Actions Workflow)
- [x] Liveness / Readiness Endpoints (/live, /ready)
- [x] Sharpe & Drawdown Gauges + +Inf Bucket & Build Info Metric
- [ ] Grafana Dashboard Beispiel (JSON) (Skeleton committed; finalize panels & alerts PENDING)

## 7) Production Hardening & Open Roadmap

### Execution & Routing
- [ ] Stabiler WebSocket PubSub (Reconnect, Backoff, Heartbeat)
	- [x] Reconnect + Exponential Backoff + Metric (`ws_reconnects_total`)
- [ ] Volle Raydium Serum Market Accounts erzwungen (kein Fallback)
- [ ] Multi-DEX Best-Route Auswahl (Raydium vs Orca dynamisch)
- [ ] Partielle Exits (gestaffelte SELL Orders / Positionsplits)
- [ ] Retry & Backoff Strategie bei transienten RPC Fehlern
	- [x] Grund-RPC Retry (Exponentiell, 3 Versuche, Metric `rpc_retry_attempts_total`)
- [ ] PendingTrade TTL + Reconciliation (Zombie Pending entfernen)
	- [x] TTL Cleanup (Config `pending_trade_ttl_secs`) – implemented
	- [x] Reconciliation (Signature status check + finalize/dismiss logic, metrics `pending_reconciliations_total`, `pending_failed_total`)
- [ ] Re-Quote unmittelbar vor Signatur (Front‑run Schutz / aktualisiertes min_out)

### State & Persistence
- [ ] Persistenter RiskState Snapshot (offene Positionen, realized PnL, rolling returns, Sharpe)
- [ ] Per-Mint Mehrfachpositionen (konfigurierbar per_mint_position_limit)
- [ ] Periodische Flush / Recover Logik beim Shutdown

### Fees & PnL Genauigkeit
- [ ] Protocol / LP / Referral Fees Extraktion aus Transaction Meta
- [ ] Brutto vs Netto PnL Metriken (separate Gauges)
- [ ] Fee % des Notionals als Histogram

### Data & Pricing
- [ ] Echte SOL/USD & Stable Preise via Oracle (Pyth / Switchboard)
- [ ] Präzisere Liquidity Schätzung (Reserven * Mid-Price, multi-pool Aggregation)
- [ ] Adaptive Slippage basierend auf empirischer Fill-Abweichung

### Stability & Concurrency
- [ ] Separate Task für Exit Evaluation (konfigurierbares Intervall)
- [x] Graceful Shutdown (Flush, Snapshot, Metrics finalisieren via watch channel)
- [ ] Rate Limit Erkennung + adaptiver Parallelismus
	- [x] Test-Helpers Feature (`test_helpers`) für deterministische State Mutation / Sharpe Tests

### Logging & Observability Erweiterungen
- [x] +Inf Bucket für trade_return Histogram (Prometheus Konvention)
- [ ] Absolutes Realized PnL Histogram (SOL)
- [ ] Shortfall Prozent Histogram
- [x] Sharpe & Drawdown Gauges
- [x] Build / Version Metric
	- [x] Interne Sharpe Validierung (Fee Impact & Rolling Window Truncation Tests)

### Backtesting & Strategy
- [ ] `py_strategy` FFI (pyo3 oder IPC) für externe Signale
- [ ] Deterministischer Replay-Modus (Slot Iterator)
- [ ] Impact / Slippage Modell im Backtest

### Security & Key Handling
- [ ] Gesicherte Keypair Ladepfade / optional KMS
- [ ] Keine Private Keys in Logs (Audit Layer)
- [ ] Config Validierung (Schema + Constraints)

### Tests & CI
- [ ] Unit: drawdown sizing, cooldown gating, trade_return bucketing
- [ ] Integration: Mock RPC Buy->Fill->Sell Lifecycle
- [ ] Fuzz: Log Parser & Pool Snapshot Decoder
- [ ] Load / Stress: Quote & Swap Latency unter Last
- [ ] GitHub Actions: clippy, fmt, test, audit
	- [x] Multi-Lot Partial Exit Mathe Test
	- [x] Partial Exit State Mutation & Fee / Sharpe Window Tests (gated via feature)

### Resilience & Edge Cases
- [ ] 0-Reserve Pool Handling (Skip & Metric)
- [ ] Token ohne Decimals Info (Fallback & Warn)
- [ ] Overflow / Extremwerte Guard für Returns & Fees

### Developer Experience / Config
- [ ] Erweiterte Dokumentation der neuen Risk Parameter
- [x] Diff Logging bei Hot Reload (was hat sich geändert?)
	- Implementiert: `diff_sniper_cfg` vergleicht Felder & loggt Änderungen (Hot Reload Task)
	- Optional Echtzeit File Watch via Feature `notify_watch` (aktivieren mit `--features notify_watch`)
	- Ausstehend: SIGHUP Signal Handler (Unix) als alternativer Trigger
- [ ] Start Skripte (Windows/Unix) für build/run/backtest
- [ ] Konfigurierbare Log Rotation & Retention (N Tage)

### Priorisierte Reihenfolge (Empfehlung)
1. PendingTrade TTL & State Persistenz
2. Retry/Backoff + WebSocket Stabilität
3. Protocol Fee Parsing & Multi-Position Support
4. Partielle Exits & Best-Route Auswahl
5. Tests & CI Pipeline
6. Sharpe/Drawdown Gauges & +Inf Bucket
7. Oracle Preise & Adaptive Slippage
8. Backtest FFI & Impact Modell

