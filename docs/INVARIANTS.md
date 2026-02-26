# IronCrab — Invariants

**Zweck:** AI darf diese Regeln **niemals** verletzen. Bei Refactors/Features: Invariants prüfen.

**Quellen:** TARGET_ARCHITECTURE.md, DEFINITION_OF_DONE.md, ROLE_SEPARATION.md, .cursor/rules/ironcrab-core.mdc

---

## 1. Sicherheit und Keys

| ID | Invariante | Verletzung = |
|----|------------|--------------|
| I-1 | **Single-Signer**: Nur execution-engine lädt Keys und signiert/sendet | Architekturbruch |
| I-2 | **Intent-only**: market-data, momentum-bot, arb-strategy, control-plane sind **keyless** — erzeugen nur TradeIntent oder MarketEvents | Key-Leak-Risiko |
| I-3 | Prozesse außer execution-engine **crashen mit exit(1)** wenn Key-Env-Vars erkannt | DoD §A |

---

## 2. Hot Path vs. Cold Path

| ID | Invariante | Verletzung = |
|----|------------|--------------|
| I-4 | **HOT PATH** (Discovery, Buy, Sell, Monitoring): **GEYSER-ONLY**. Keine blockierenden RPC-Calls. Latenz-Ziel unter 1s Discovery bis TX on-chain. | Latenz-Bruch |
| I-5 | **COLD PATH** (Liquidation, Manual Actions, Bootstrap): RPC erlaubt. Safety und correctness vor Speed. getTokenAccountsByOwner, getMultipleAccounts für autoritativen On-Chain-State. | — |
| I-6 | **Nie** RPC aus Cold Paths entfernen um zu "optimieren" — bricht safety-kritische Flows. | Safety-Bruch |
| I-7 | **Nie** RPC in Hot Paths ohne explizite Freigabe — bricht Latenz-Anforderungen. | Architekturverletzung |
| I-8 | Bei RPC-Refactoring: **immer** prüfen ob Hot oder Cold Path betroffen. Änderungen die beide Pfade berühren = explizite Freigabe nötig. | — |

---

## 3. Execution und Simulation

| ID | Invariante | Verletzung = |
|----|------------|--------------|
| I-9 | **Simulate-gated**: Wenn Simulation fehlschlägt — **nie senden** (besonders Arbitrage). | Kapitalverlust-Risiko |
| I-10 | Einziger Pipeline-Pfad: Intent → Arbitration → Plan → Simulate → (Send) → Confirm → Accounting | Undefiniertes Verhalten |
| I-11 | Jeder Intent endet in **genau einem** Outcome: Rejected, Expired, SimFailed, Sent, Confirmed, FailedConfirmed | DoD §C |
| I-12 | **Decision Record** pro Intent — Inputs, Checks, Outcome. Keine stille Ablehnung. | Forensik-Unmöglich |

---

## 4. Daten und Preise

| ID | Invariante | Verletzung = |
|----|------------|--------------|
| I-13 | **Pool-Matching**: Position-Preis-Updates (Trade, PoolCacheUpdate) nur anwenden wenn source_pool == position.pool. Bei Multi-Pool-Tokens sonst falsche PnL und TAKE_PROFIT bei Verlust. | FIX-38 |
| I-14 | **tokens_per_sol** Konvention: LOWER = token wertvoller. pnl_pct = (entry/current - 1)*100. highest_price = niedrigster tps (bester Preis für Holder). | Invertierte Exit-Signale |
| I-15 | **Amounts explizit**: Jede Zahl hat raw vs ui und decimals. Keine impliziten Konventionen. | Falsche Slippage/Quotes |
| I-16 | **Geyser/LivePoolCache** ist autoritativ im Hot Path. RPC/WS nur Fallback (Cold Path). | Latenz + Cache-Inkonsistenz |

---

## 5. Arbitrage und MEV

| ID | Invariante | Verletzung = |
|----|------------|--------------|
| I-17 | **Typ A (Strategy Arbitrage)**: marktgetrieben, erzeugt nur TradeIntent, keine Parent-Tx vorausgesetzt. | — |
| I-18 | **Typ B (Execution MEV)**: reaktiv, existiert nur relativ zu konkreter Parent-Tx oder Engine-State. Kein dauerhaftes Market-Scanning. | — |
| I-19 | **Atomic Arbitrage**: Cross-DEX Arb atomar (Bundle) oder verworfen. Keine Teilfills ohne definiertes Recovery. | Partial-Loss |

---

## 6. Locks und Kapital

| ID | Invariante | Verletzung = |
|----|------------|--------------|
| I-20 | **Capital Locks**: Keine Überbuchung. LockManager.try_lock_capital(). | Doppelte Ausführung |
| I-21 | **Resource Locks**: Accounts/Pools/ATAs die Konflikte erzeugen werden gelockt. | Race Conditions |
| I-22 | **Idempotency**: Engine vermeidet doppelte Verarbeitung (Intent-ID, Tx-Signature, in-flight Registry). | Doppel-Trades |

---

## 7. NATS und Topics

| ID | Invariante | Verletzung = |
|----|------------|--------------|
| I-23 | Keine neuen ad-hoc NATS Topics. An versioned Topics halten oder klar dokumentieren. | Topic-Chaos |
| I-24 | Topics: ironcrab.v1.market_events, ironcrab.v1.trade_intents, ironcrab.v1.execution_results, ironcrab.v1.decision_records (siehe src/nats/topics.rs). | — |
| I-24a | **JetStream = SSOT für Bot-Zustand**: Wallet-Balances, Positionen, Pool-Cache, Config gehören in JetStream (persistent). Konsumenten bootstrappen und holen Live-Updates von dort. | Zustands-Drift |
| I-24b | **Core NATS = Market Events**: Chain-Daten (Trades, Blocks, Preise) als Echtzeit-Events. Kein Bot-Zustand über Core NATS — Datenflut zu hoch, keine Persistenz. | — |

---

## 8. Entwicklungs-Workflow

| ID | Invariante | Verletzung = |
|----|------------|--------------|
| I-25 | Plan vor dem Coden. Kleine, isolierte Änderungen bevorzugen. | Side-Effects |
| I-26 | Architektur-Änderungen nur mit expliziter Freigabe. | Scope Creep |
| I-27 | SSH/Server-Befehle nur wenn User explizit anfordert oder genehmigt. | — |

---

## Checkliste vor PR / AI-Änderung

- [ ] Kein RPC im Hot Path?
- [ ] Pool-Matching bei Preis-Updates für Positionen?
- [ ] tokens_per_sol Konvention eingehalten?
- [ ] Simulation vor jedem Send?
- [ ] Decision Record für jeden Intent?
- [ ] Keine Keys außer in execution-engine?
