
# IronCrab – Tasks & Meilensteine

## 1) Treasury & Transfers
- [x] SOL-Balance lesen (`Treasury::sol_balance`)
- [x] ATA-Berechnung & Erstellung (spl-associated-token-account)
- [x] SOL/Token-Transfers + Rent Exemption
- [x] WSOL wrap/unwrap (sync_native / close)

## 2) DEX-Connectoren
- [x] Raydium Pools scannen (`refresh_pools`) – läuft (Heuristik Fee-Extraction, Vault Balances)
- [x] Quote-Berechnung `quote_exact_in` (Fee + Price Impact, Slippage-MinOut Helper)
	- [x] Slippage-Enforcement im Engine (mit Execution Records / Reject Logging)
	- [ ] Quote-Validierung gegen invarianten Test (größere Inputs -> höhere Impact Bps) – erweitern
  
 [ ] Swap-IX bauen (echter Raydium BaseIn) + ComputeBudget + Prioritätsgebühren
	- [ ] Erweiterung `SimplePool` um open_orders / market_id / authority PDA
	- [ ] Ableitung AmmAuthority PDA + Serum Vault Signer
	- [ ] Einfügen ComputeBudget (CU limit + price) & optional Prioritäts-Fee Instruktionen
	- [ ] Realistische Account-Reihenfolge testen (Simulation)
	- [ ] MinOut aus Quote+Slippage ableiten & Assertion
- [ ] Orca Whirlpool/Classic analog
- [ ] Routen-Suche (1–2 Hops)

## 3) Arbitrage
- [ ] DEX Quotes aggregieren, beste Edge wählen
	- (Partial vorhanden: Grundgerüst `arbitrage.rs`, aber keine finale Auswahl-/TX-Pipeline)
- [ ] Jito/MEV-Integration (später)
- [ ] Pre-TX Simulation (RPC `simulateTransaction`)

## 4) Sniper
- [ ] WS-Logs Subscribe (Pool-Create)
- [ ] Heuristiken (Blacklist, FreezeAuth, Owner, LP-Lock)
- [ ] Erstkauf + enge SL/TP
  
### Nächste Micro-Tasks (Prior Vorschlag)
1. (DONE) Slippage-Enforcement in Backtest Engine (Abbruch + Execution Record)
2. Raydium `SimplePool` um OpenOrders / Market / Authority erweitern
3. Hilfsfunktion: Ableitung `amm_authority` PDA & Serum Vault Signer
4. Echten Raydium Swap Instruction Builder + Simulationstest (`simulateTransaction` Dry-Run)
5. Multi-Hop Routing Entwurf (Graph: Mints als Knoten, Pools als Kanten, BFS bis Tiefe 2, Score = erwarteter Output)
6. Arbitrage Aggregator: Sammeln aller DEX Quotes (derzeit nur Raydium) -> Struktur vorbereiten für Orca
7. Orca Reader Grundgerüst (nur fetch + quote) für Routing-Basis
8. Sniper: WS Subscription Skeleton (nur Connect + Log Filter) vorbereiten
9. CI Skeleton (GitHub Actions YAML: fmt + clippy + tests)
10. Metrics Placeholder (Prometheus Encoder + single gauge für Pool Count)

## 5) Risk & Limits
- [ ] Positions-/Notional-Limits je Markt
- [ ] Cooldowns / Daily Loss Limit
- [ ] P&L Tracking + Reporting

## 6) Infra
- [ ] Config-Reload (SIGHUP)
- [ ] Structured Metrics (Prometheus)
- [ ] CI: `cargo fmt`, `cargo clippy`, Tests
