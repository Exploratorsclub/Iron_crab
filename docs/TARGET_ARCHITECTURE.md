# Target Architecture (Debuggable-First, Data Plane, Momentum-Only)

Dieses Dokument ist die **konsolidierte Zielarchitektur** für IronCrab, basierend auf `solana_trading_system_architecture2.md`, aber mit den wichtigen Korrekturen aus der späteren Diskussion:

- **Kein Sniper im klassischen Sinne** (kein „alle neuen Mints sofort kaufen“).
- **Data Plane** lädt/normalisiert Markt-Daten **einmal**.
- **Momentum** ist die primäre Strategie (Early + Established als Policies/Regimes).
- **MEV ist eine Execution-Fähigkeit**: Arbitrage/Backrun/etc. sind **Worker im MEV-Layer** der Execution Engine.

---

## 1) Leitprinzipien

- **Hot Path = Rust & In-Memory** (Execution, Arbitration, Locks, Tx build).
- **Single-Signer**: Nur die Execution Engine signiert/sendet.
- **Intent-only**: Alle Strategien/Worker erzeugen nur `TradeIntent`s.
- **Simulate-gated** (insb. Arbitrage): Simulation-fail ⇒ **nie senden**.
- **Decision Records**: Jede Entscheidung ist forensisch nachvollziehbar.
- **Data Plane**: Markt-Daten werden **nicht mehrfach geladen**.

Abnahme: Siehe `docs/DEFINITION_OF_DONE.md`.

---

## 2) Komponentenübersicht (Binaries/Prozesse)

### 2.1 Data Plane: `market-data` (Rust)

Aufgabe: **einmalige** Markt-Daten-Ingestion und Normalisierung.

- Geyser ingest (prefered), optional RPC/WS fallback
- Pool/Account Cache (in-memory)
- Normalisierte `MarketEvents` publizieren (NATS)
- **Discovery Worker**: erkennt neue Mints/Pools als Events/Features (kein Buy)

Output:
- `MarketEvents` (Pub/Sub)
- Optional: `MarketSnapshots` (für Replay/Backtest)

### 2.2 Strategy Plane: `momentum-bot` (Rust)

Aufgabe: Aus `MarketEvents` **Signale** ableiten und `TradeIntent`s erzeugen.

- **Regime Classifier** (deterministisch): `EARLY` vs `ESTABLISHED`
- **Ein gemeinsamer Feature-Extractor**, zwei Policies:
  - **EARLY Momentum Policy**: strenge Safety/Filter, dünne Datenlage, hohe Manipulationsgefahr
  - **ESTABLISHED Momentum Policy**: klassische Momentum-Logik (Breakout/vol expansion etc.)

Wichtig:
- Kein Signieren/Senden
- Keine direkte DEX-Ausführung

Output:
- `TradeIntents` (Request/Reply oder Pub/Sub)

### 2.2.1 Optional (empfohlen): `arb-strategy` (Rust)

**Wichtig zur Einordnung (vermeidet dauernde Verwirrung):**

Es gibt **zwei Kategorien**, die beide „Arbitrage“ heißen können, aber architektonisch verschieden sind:

**Typ A – Markt-getriebene Arbitrage (Strategy Arbitrage)**
- existiert ohne konkrete fremde Parent-Tx
- kann über mehrere Slots gültig sein
- braucht Preisfeeds/Quotes + Slippage-Modelle + Risk/Kapital-Logik
- **gehört in die Strategy Plane** (gleichrangig mit Momentum)

**Typ B – Reaktive / Tx-abhängige MEV (Execution MEV)**
- existiert *nur*, weil eine Parent-Tx (eigene oder beobachtete) existiert
- z. B. Backrun, Bundle Ordering, Fee/CU Optimierung, Liquidation-triggered Aktionen
- **gehört als Worker in den MEV-Layer der Execution Engine**

Für Typ A ist ein eigener Strategy-Worker sinnvoll:
- konsumiert `MarketEvents` aus `market-data`
- berechnet EV/ROI und erzeugt `TradeIntent`s
- signiert/sendet nie selbst

Hinweis: Typ A kann als eigenes Binary (`arb-strategy`) laufen oder als separater Worker im `momentum-bot`.
Für Debuggability/Fault Isolation ist ein eigenes Binary meist klarer.

### 2.3 Execution Plane: `execution-engine` (Rust)

Einzige Instanz mit Keys. Aufgaben:

- Global Arbitration (EV × urgency × deadline)
- Capital Locks + Resource Locks
- Tx Plan → Simulate → Send → Confirm
- Fee/Compute/Tip Policy zentral

**MEV-Layer (in-proc Worker, keine separaten „MEV Bots“):**
- `ExecutionArbWorker` (reaktiv; Tx-/Engine-State-getrieben, **nicht** marktgetriebener Scanner)
- `BackrunWorker`
- `Liquidation/Re-Arb Worker`
- `JIT Liquidity Worker`
- `Fee/Compute Param Worker`

### 2.4 Control/UI: `control-plane` (FastAPI) + UI (React)

- Start/Stop, Config, Risk Limits, Alerts
- Zeigt Decisions/Status live (nicht Trading Hot Path)
- UI für Kontrolle/Realtime; Grafana für Forensik/Trends

---

## 3) Kommunikations-Topologie (NATS)

Topics (Minimum):
- `MarketEvents` (market-data → consumers)
- `TradeIntents` (momentum-bot/MEV-worker → execution-engine)
- `ExecutionResults` (execution-engine → UI/control/analytics)
- `ControlRequests` (control-plane ↔ execution-engine)

Regel: **Kein Bot darf direkt senden/signieren** – nur Intents.

---

## 4) Datenfluss (ohne doppelte Datenladung)

```text
Geyser/RPC
  │
  ▼
market-data (cache + normalize + discovery)
  │  MarketEvents
  ▼
NATS
  │
  ├─► momentum-bot (EARLY/ESTABLISHED policies) ─► TradeIntents ─┐
  │                                                            │
  └─► execution-engine MEV workers (reactive) ─► internal Intents ├─► Plan/Sim/Send
                                                                │
                                                                ▼
                                                        ExecutionResults
```

---

## 5) Storage / Datenbank (wichtig für Debuggability, nicht Hot Path)

Ziel: Debuggability durch **Replay + Decision Records**, ohne den Hot Path zu blockieren.

Kernaussage:
- **Prometheus/Grafana** = Metriken/Trends/Alerting
- **Flat Files** = Replay/Forensik/Regression (deterministisch)
- **DB (ClickHouse/Timescale)** = Analytics/Ad-hoc Queries (nicht zeitkritisch)

### 5.0 Hot-Path Safe Ingestion Pattern (Pflicht)

Der Trading Hot Path darf niemals auf DB-IO warten.

Regel:
- Hot Path schreibt nur **append-only** (lokal) oder in eine **In-Memory Queue**.
- Ein **async ingestor** (separater Task/Prozess) übernimmt Persistenz in DB.
- Wenn Persistenz/DB ausfällt, muss Trading weiterlaufen (mit Backpressure/Drop Policy, aber ohne Deadlock).

Empfohlener Standard:
- `market-data` schreibt `MarketEvents` optional in Flat Files.
- `momentum-bot` schreibt `TradeIntents` in Flat Files.
- `execution-engine` schreibt `Decision Records` + `ExecutionResults` in Flat Files.
- Optional: ein `analytics-ingestor` liest Flat Files/Stream und schreibt nach ClickHouse/Timescale.

### 5.0.1 Was gehört wohin?

| Artefakt | Zweck | Speicher | Produzent |
|---|---|---|---|
| Metrics (latency, success/fail, ROI, queue depth) | Live Monitoring + Alerting | Prometheus | alle Binaries |
| MarketEvents (roh/normalisiert) | Replay + Debug „Input war so“ | Flat Files (bincode/parquet) | market-data |
| TradeIntents | Replay + Audit „Strategie wollte X“ | Flat Files | momentum-bot + execution-engine (interne Intents) |
| Decision Records | Debug „warum wurde gehandelt/abgelehnt“ | Flat Files (jsonl/bincode) | execution-engine |
| ExecutionResults (sig, slot, fees, pnl attribution) | Audit + Auswertung | Flat Files + optional DB | execution-engine |
| Long-term Analytics (PnL, cohorts, drilldowns) | Offline/Ad-hoc Queries | ClickHouse/Timescale | analytics-ingestor |

### 5.1 P0 (minimal, sofort umsetzbar)
- **Flat files** (bincode/jsonl/parquet) für:
  - `MarketEvents` (optional sample/filtered)
  - `TradeIntents`
  - `Decision Records`
  - `ExecutionResults`
- Rotations-/Retention-Policy

### 5.2 P1/P2 (Analytics)
- Optional Analytics DB:
  - ClickHouse **oder** TimescaleDB
- Zweck:
  - Langzeit-Auswertungen, Queries, Profit attribution, Debug-Forensik

Wichtig:
- DB ist **nicht** Teil des Trading Hot Path.
- Der Hot Path schreibt nur append-only (queue/file), ein async ingestor schreibt in DB.

---

## 6) Warum kein „Sniper“ mehr

- Ohne unfairen Speed-Vorteil ist „mint sofort kaufen“ strukturell negative EV.
- Stattdessen: **Discovery** liefert Signale/Features, und **Momentum** entscheidet erst nach bestätigenden Kriterien.

---

## 7) MVP (First Results Fast, aber debugbar)

1) `market-data`: Geyser ingest + Discovery Worker + MarketEvents
2) `momentum-bot`: nur EARLY-Regime + TradeIntent
3) `execution-engine`: Locks + Simulate-gate + Decision Records + (optional send)

Erst danach:
- ESTABLISHED-Regime
- MEV Workers erweitern
- Analytics DB

