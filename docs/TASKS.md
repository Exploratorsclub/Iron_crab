
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
- [ ] Jito/MEV-Integration (später)
- [x] Pre-TX Simulation (RPC `simulateTransaction`)

## 4) Sniper
- [ ] WS-Logs Subscribe (Pool-Create)
- [ ] Heuristiken (Blacklist, FreezeAuth, Owner, LP-Lock)
- [ ] Erstkauf + enge SL/TP
  
### Nächste Micro-Tasks (aktualisiert)
1. Orca: Vollständige Whirlpool Layout Implementierung (strukturierter Parser, feste Offsets, verifizierte Tick Index & Liquidity Fields)
2. Orca Swap: Ersetzen der Platzhalter User Accounts (Authority, Source/Dest Token Accounts) + Option für sqrt_price_limit
3. Raydium: Entfernung von Vault-Fallbacks (immer echte Vault Pubkeys) & zusätzliche Hard Validation (target_orders optional, aber loggen)
4. Router: Depth‑2 Pfad Benchmark + Depth‑3 (erste Greedy Implementierung + Pruning) [Depth‑3 implemented]
5. Arbitrage Aggregator: Triangular Path (A-B-C-A) Profit Check (greedy implemented) + Erweiterung für generische Zyklen
	- [x] Net Profit Filter (min_profit_bps + est_tx_cost_lamports)
	- [x] TransactionPlan Scaffold (triangle assembly)
	- [x] Pre-TX Simulation (RPC simulateTransaction)
6. Compute Budget: Dynamische CU-Schätzung (historische Simulation / heuristics) statt fixer Werte
	- [x] Erste heuristische Implementierung (Estimator Modul + Raydium build_swap_plan_auto)
7. Metrics: Pool Count, Quote Latency, Routing Auswahlgrund (DEX, hops)
	- [x] Basic counters (quotes, successes, hop types, triangle attempts/profitable, avg latency)
8. CI Pipeline (fmt, clippy, test, optional wasm build)
9. Config Hot-Reload (Signal / File Watch) für Routing/Slippage Parameter
10. Risk Layer: Max Notional pro Trade + Daily Loss Limit Skeleton

## 5) Risk & Limits
- [ ] Positions-/Notional-Limits je Markt
- [ ] Cooldowns / Daily Loss Limit
- [ ] P&L Tracking + Reporting

## 6) Infra
- [ ] Config-Reload (SIGHUP)
- [ ] Structured Metrics (Prometheus)
- [ ] CI: `cargo fmt`, `cargo clippy`, Tests
