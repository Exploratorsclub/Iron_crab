# Definition of Done (DoD) – Umbau auf die Zielarchitektur

Diese Checkliste ist die **Abnahme-Definition** für den Umbau zur Referenzarchitektur aus `solana_trading_system_architecture2.md`.

Ziel: **deterministisch, debugbar, sicher** – und zwar mit messbaren Kriterien, nicht Bauchgefühl.

## DoD-Levels (Gates)

- **P0 (Blocker / Live-verboten)**: Muss erfüllt sein, bevor irgendein echter Kapital-Risk (Mainnet mit Keys) erlaubt ist.
- **P1 (Produktionsfähig)**: Muss erfüllt sein, bevor Skalierung/mehrere Strategien/MEV-Worker ernsthaft betrieben werden.
- **P2 (Professional / Wettbewerb)**: Performance-/Komfort-/Hardening-Ziele.

---

## A) Security & Key-Ownership (Architektur §3.1, §5, §13)

### P0
- [ ] **Single-Signer erzwingbar**: Es existiert genau **eine** Komponente, die signieren darf (Execution Engine). Alle anderen Prozesse laufen keyless.
- [ ] **Kein „rogue send“ möglich**: Strategy-Bots/Worker haben keinerlei Send-/Sign-Codepfad (kein RPC send, kein TPU send, kein Jito send).
- [ ] **Key-Material ist nicht im Hot Path leakbar**: Keine Secrets in Logs/Events; keine Keys in Env Vars; klare Storage-Quelle (z. B. File + OS ACL oder Vault).
- [ ] **Panic/Kill Switch**: Ein globaler Kill Switch kann Trading deterministisch deaktivieren (Control Plane + Engine-seitig), inkl. Nachweis in Logs/Metrics.

### P1
- [ ] **Role separation**: Control Plane kann Parameter ändern/stoppen, aber niemals signieren.
- [ ] **Least privilege**: Bots/Worker besitzen nur NATS- oder gRPC-Creds, keine Wallets.

---

## B) Intent Model & Contracts (Architektur §3.2)

### P0
- [ ] **TradeIntent ist das einzige externe Trading-Interface**: Jeder Trade entsteht aus einem TradeIntent (auch interne Worker-Intents).
- [ ] **TradeIntent enthält harte Felder**: `required_capital`, `deadline/ttl`, `resources` (Accounts/Pools), `expected_value/ev`, `max_slippage`, `source`, `tier`, `urgency`.
- [ ] **Units sind eindeutig**: Jede Zahl ist explizit `raw` vs `ui` und trägt `decimals` oder ist normiert (z. B. 9-decimal standard). Keine impliziten Konventionen.

### P1
- [ ] **Versionierung**: Intents/Events sind versioniert (`schema_version`), und die Engine ist rückwärtskompatibel für mindestens 1 Version.

---

## C) Deterministische Execution Pipeline (Architektur §5)

### P0
- [ ] **Einziger Pipeline-Pfad**: `Intent -> Arbitration -> Plan -> Simulate -> (Send) -> Confirm -> Accounting`.
- [ ] **Simulation ist Gatekeeper**: Wenn `simulate` fehlschlägt, wird **nie** gesendet. (Arb: zwingend; Sniper: mindestens optionaler Mode.)
- [ ] **Idempotency**: Engine kann bei Restart doppelte Verarbeitung vermeiden (z. B. Intent-ID, Tx-Signature, in-flight registry).
- [ ] **Outcome-Klassen**: Jeder Intent endet in genau einem Zustand: `Rejected` / `Expired` / `SimFailed` / `Sent` / `Confirmed` / `FailedConfirmed`.

### P1
- [ ] **Atomic Arbitrage**: Triangular/Cross-DEX Arb wird atomar gesendet (Bundle) oder verworfen; keine Teilfills ohne definiertes Recovery.
- [ ] **Fee/Compute Policies sind zentral**: compute budget, priority fee, tip-Policy sind Engine-owned (nicht in Strategien verteilt).

---

## D) Global Arbitration, Locks & No Self-Competition (Architektur §3.1, §4, §5)

### P0
- [ ] **Capital Locks**: Jede Execution reserviert Kapital eindeutig (SOL + Token), kein Überbuchen möglich.
- [ ] **Resource Locks**: Accounts/Pools/ATAs, die Konflikte erzeugen können, werden gelockt (oder es gibt eine bewusste Konflikt-Policy).
- [ ] **Preemption-Regeln implementiert**: Tier0 kann Tier1 preempten; Tier1 darf Tier0 niemals verdrängen.

### P1
- [ ] **Fairness/Starvation Policy**: Dauerhafte Verdrängung wird begrenzt (z. B. max preemptions pro Worker/Slot).

---

## D.1) Invariants: Typ A vs Typ B (Arbitrage/MEV Einordnung)

Ziel: Verhindert „Arbitrage gehört wohin?“-Verwirrung durch harte Abnahmekriterien.

### P0
- [ ] **Typ A (Strategy Arbitrage) = marktgetrieben**: darf Market-Scanning/Quotes/EV-Ranking betreiben und erzeugt nur `TradeIntent`s; sie darf keine Parent-Tx voraussetzen.
- [ ] **Typ B (Execution MEV) = reaktiv/Tx-abhängig**: existiert nur in Bezug auf eine konkrete Parent-Tx oder Engine-State (z. B. eigene Pending-Tx, Bundle, observed Tx) und erzeugt interne Intents/Optimierungen; sie betreibt kein dauerhaftes Market-Scanning.
- [ ] **Decision Records enthalten Klassifikation**: jeder Intent/Decision Record enthält `origin_type = A|B` (oder äquivalent) + reason-coded Begründung, damit Post-Mortems klar sind.

---

## E) Observability: Decision Records (Architektur §10) – „Warum hat er das getan?“

### P0
- [ ] **Decision Record pro Intent**: Für jeden Intent existiert ein strukturierter Record (JSON/protobuf/bincode), der die Entscheidung nachvollziehbar macht.
- [ ] **Record enthält Inputs**: Quotes/Route, Config-Snapshot-ID, Risk/State-Snapshot-ID, Balances/Locks, TTL/Deadline.
- [ ] **Record enthält Checks**: Liste pass/fail pro Invariant/Rule mit konkreter Begründung.
- [ ] **Record enthält Output**: Plan-Hash, simulate result (err + log preview), send result (signature/bundle id), confirm status.
- [ ] **Korrelation**: Jede Tx/Bundlesignature ist über Decision-ID und Intent-ID auffindbar.

### P1
- [ ] **UI/Control zeigt Entscheidungen**: In der UI/Control Plane kann man die letzten N Decisions ansehen (inkl. „rejected reasons“).

---

## F) Metrics: Prometheus/Grafana Abnahme (Architektur §10)

### P0
- [ ] **Pflicht-Metriken vorhanden**: 
  - `intents_received_total` (labels: source, tier)
  - `intents_rejected_total` (label: reason)
  - `plans_built_total`
  - `simulate_failed_total` (label: error_code)
  - `tx_sent_total`, `tx_confirmed_total`, `tx_failed_total`
  - `decision_latency_ms` (P50/P95/P99)
- [ ] **„No silent failure“**: Es gibt keine Fehlerpfade ohne Metric + Decision Record.

### P1
- [ ] **Per-Strategy/Per-Worker Attribution**: Profit/fees/latency sind pro source/worker sichtbar.

---

## G) Storage & Replay (Architektur §11)

### P0
- [ ] **Replay-Paket definierbar**: Für einen Zeitraum kann man MarketEvents + Intents + Decisions exportieren (Flat files).
- [ ] **Deterministischer Replay-Run**: Offline-Replay reproduziert Decisions für denselben Input-Stream (mindestens für `Rejected/Planned/SimFailed`).

### P1
- [ ] **Golden Replays**: Es gibt mindestens 3 gespeicherte „golden“ Replay-Szenarien, die in CI laufen.

---

## H) Connectoren & Datenquellen: „Untrusted until proven“ (Architektur §6)

### P0
- [ ] **Connector Contract Tests**: Für jeden DEX-Connector existieren Tests, die prüfen:
  - Quote-Ausgabe plausibel (monotonie/decimals)
  - Instruction-Builder erzeugt valide Accounts (layout checks)
  - Simulation für einfache Swap-Transaktion ist reproduzierbar (im Testnetz/Localnet/Recorded)
- [ ] **Unit-Normalisierung**: Ein zentraler Layer normalisiert amounts/decimals (keine DEX-spezifischen Sonderregeln verteilt im Code).

### P1
- [ ] **Fuzz/Property Tests**: Mindestens 1 Property-Test pro kritischem Parser/Layout (z. B. Whirlpool/Raydium states).

---

## I) Control Plane & Bus (Architektur §7, §8, §9)

### P0
- [ ] **NATS Topics fixiert**: `MarketEvents`, `TradeIntents`, `ExecutionResults`, `ControlRequests` sind definiert, versioniert und dokumentiert.
- [ ] **Request/Reply für Control**: Start/Stop, risk limits, config reload laufen über request/reply (mit Timeout + Ack).
- [ ] **Hot Path bleibt Rust**: Kein Python/HTTP im Execution Hot Path.

### P1
- [ ] **RBAC (minimal)**: Mindestens Admin/Viewer Rollen (UI/API), Auditing der Control-Aktionen.

---

## J) Risk & Correctness (Architektur §5 + deine Zielanforderung „macht nichts, was er nicht soll“)

### P0
- [ ] **Explizite Risk Invariants**: z. B. `max_position`, `daily_loss_limit`, `max_open_positions`, `max_slippage` sind als Engine-Checks implementiert.
- [ ] **Hard Fail mit Reason**: Wenn Risk verletzt wäre, wird der Intent rejected mit eindeutigem `reason_code` (nicht freitext-only).
- [ ] **No hidden defaults**: Jede Default-Policy ist dokumentiert und im Decision Record sichtbar.

### P1
- [ ] **State Consistency**: PnL/positions sind nach Restart konsistent (persisted snapshots + idempotency).

---

## K) Performance / Latenz (Architektur §1, §6)

### P2
- [ ] **Hot path allocations**: Kritische Pfade sind allocation-bewusst (Profiling vorhanden).
- [ ] **Slot-to-send Latency**: P50/P95/P99 Ziele sind definiert und in Grafana sichtbar.
- [ ] **TPU/Relayer Path**: Execution nutzt TPU/Relayer (nicht `sendTransaction`), mit klaren Fallback-Regeln.

---

## L) Process Separation & Binaries (Debuggability / Fault Isolation)

### P0
- [ ] **Execution ist ein eigenes Binary/Prozess**: Signing/Sending/Locks leben in einer separaten Execution Engine (Single-Signer). Kein anderer Prozess hat Key-/Send-Rechte.
- [ ] **Klare Schnittstelle**: Kommunikation Strategie/Worker → Execution erfolgt nur über Intents (Bus/IPC), nicht über direkte Funktionsaufrufe, die Seiteneffekte verstecken.

### P1
- [ ] **Bots getrennt startbar**: Sniper (Scout), Arbitrage-Scanner und ggf. Momentum sind eigene Binaries/Services (start/stop separat), um Fehlerquellen isoliert debuggen zu können.
- [ ] **Crash-Isolation**: Crash eines Bots darf Execution nicht crashen; Crash der Control Plane darf Trading nicht beeinflussen.

---

## Praktische Umbau-Reihenfolge (empfohlen)

1) **P0**: Single-Signer + Intent Contract + Decision Records + Sim-Gate + Locks
2) **P1**: Preemption + Profit Attribution + Golden Replays + Connector Contract Tests
3) **P2**: Performance/TPU hardening + mehr Worker + Scaling

---

## Abnahme-„Stop Rule“ (gegen €100 Debugging)

Wenn eine neue Funktionalität nicht mindestens erfüllt:
- Decision Record vollständig,
- simulate-gated (oder bewusst deaktiviert mit dokumentierter Begründung),
- reason-coded rejects,

…dann gilt sie als **nicht fertig** und darf nicht mit realem Kapital laufen.
