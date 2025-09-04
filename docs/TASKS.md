
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
- [ ] Partielle Exit-Unterstützung (mehrere SELLs pro Position)
- [ ] Persistenter Positionssnapshot (Reload nach Neustart)
  
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
	- [ ] PnL Distribution Histogram / Buckets (optional)
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
- [ ] Per-Mint Positions-Limit (Mehrfachpositionen – derzeit 1 enforced)
- [x] Cooldowns nach Stop-Loss (HashMap cooldown_until)
- [x] Erweiterte PnL Reports (rolling window + Sharpe Approx `rolling_pnl_window`)
- [x] Config Hot-Reload (ENV `IRONCRAB_SNIPER_RELOAD_PATH`, 30s Interval)
- [ ] Persistente RiskState Speicherung (Positions & Sharpe) (TODO)

## 6) Infra & Observability
- [x] Structured Metrics (Prometheus Exporter Port 9898)
- [x] Trade CSV Logs (rotierend, Shortfall & Fees)
- [ ] Config-Reload (SIGHUP / File Watch)
- [ ] CI: `cargo fmt`, `cargo clippy`, Tests
- [ ] Liveness / Readiness Endpoints
- [ ] Grafana Dashboard Beispiel (JSON)
