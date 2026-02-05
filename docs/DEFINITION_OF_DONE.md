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
- [x] **Single-Signer erzwingbar**: Es existiert genau **eine** Komponente, die signieren darf (Execution Engine). Alle anderen Prozesse laufen keyless.
  - ✅ `execution-engine` ist das einzige Binary mit Key-Loading
  - ✅ `market-data` warnt wenn Key-Env-Vars gesetzt sind
  - ✅ `momentum-bot` exit(1) wenn Key-Env-Vars erkannt werden
- [x] **Kein „rogue send" möglich**: Strategy-Bots/Worker haben keinerlei Send-/Sign-Codepfad (kein RPC send, kein TPU send, kein Jito send).
  - ✅ Binaries `market-data` und `momentum-bot` erzeugen nur Events/Intents
- [x] **Key-Material ist nicht im Hot Path leakbar**: Keine Secrets in Logs/Events; keine Keys in Env Vars; klare Storage-Quelle (z. B. File + OS ACL oder Vault).
  - ✅ Keys nur via `KEYPAIR_PATH` File, nicht als Env-Var-Inhalt
  - ✅ Kein Logging von Key-Material in allen Binaries
- [x] **Panic/Kill Switch**: Ein globaler Kill Switch kann Trading deterministisch deaktivieren (Control Plane + Engine-seitig), inkl. Nachweis in Logs/Metrics.
  - ✅ `control_plane/main.py`: POST /kill → publishes zu `ironcrab.control.kill`
  - ✅ `execution-engine` subscribed NATS kill topic → sets `kill_switch_active`

### P1
- [x] **Role separation**: Control Plane kann Parameter ändern/stoppen, aber niemals signieren.
  - ✅ Control Plane ist keyless und blockt Start, wenn Key-Env-Vars gesetzt sind (`control_plane/main.py`)
  - ✅ Signing/Sending findet ausschließlich in der Execution Engine statt
- [x] **Least privilege**: Bots/Worker besitzen nur NATS- oder gRPC-Creds, keine Wallets.
  - ✅ Nur die Execution Engine lädt Keys; andere Prozesse sind keyless (siehe P0 oben)

---

## B) Intent Model & Contracts (Architektur §3.2)

### P0
- [x] **TradeIntent ist das einzige externe Trading-Interface**: Jeder Trade entsteht aus einem TradeIntent (auch interne Worker-Intents).
  - ✅ `src/ipc/schema.rs`: `TradeIntent` struct mit allen Pflichtfeldern
  - ✅ `momentum-bot` erzeugt nur TradeIntents, keine direkten Trades
- [x] **TradeIntent enthält harte Felder**: `required_capital`, `deadline/ttl`, `resources` (Accounts/Pools), `expected_value/ev`, `max_slippage`, `source`, `tier`, `urgency`.
  - ✅ Alle Felder implementiert in `TradeIntent` struct
- [x] **Units sind eindeutig**: Jede Zahl ist explizit `raw` vs `ui` und trägt `decimals` oder ist normiert (z. B. 9-decimal standard). Keine impliziten Konventionen.
  - ✅ `ExplicitAmount` struct mit `raw`, `decimals`, `ui` Feldern

### P1
- [x] **Versionierung**: Intents/Events sind versioniert (`schema_version`), und die Engine ist rückwärtskompatibel für mindestens 1 Version.
  - ✅ `SCHEMA_VERSION = 1` in `src/ipc/schema.rs`

---

## C) Deterministische Execution Pipeline (Architektur §5)

### P0
- [x] **Einziger Pipeline-Pfad**: `Intent -> Arbitration -> Plan -> Simulate -> (Send) -> Confirm -> Accounting`.
  - ✅ `execution-engine` implementiert Pipeline: idempotency → TTL → lock → simulate → decision
- [x] **Simulation ist Gatekeeper**: Wenn `simulate` fehlschlägt, wird **nie** gesendet. (Arb: zwingend; Sniper: mindestens optionaler Mode.)
  - ✅ `simulate_transaction()` muss `success: true` zurückgeben, sonst wird rejected
- [x] **Idempotency**: Engine kann bei Restart doppelte Verarbeitung vermeiden (z. B. Intent-ID, Tx-Signature, in-flight registry).
  - ✅ `LockManager.is_duplicate()` und `mark_processed()` in `src/storage/locks.rs`
- [x] **Outcome-Klassen**: Jeder Intent endet in genau einem Zustand: `Rejected` / `Expired` / `SimFailed` / `Sent` / `Confirmed` / `FailedConfirmed`.
  - ✅ `DecisionOutcome` enum in `src/ipc/schema.rs`

### P1
- [x] **Atomic Arbitrage**: Triangular/Cross-DEX Arb wird atomar gesendet (Bundle) oder verworfen; keine Teilfills ohne definiertes Recovery.
  - ✅ `TradeIntent.require_bundle` Feld für atomic execution requirement
  - ✅ `JitoClient` in execution-engine für Bundle Submission
  - ✅ `RejectReason::BundleFailed/BundleTimeout/BundleNotConfigured` für atomicity violations
  - ✅ Process checks bundle requirement before send; rejects if Jito not configured
- [x] **Fee/Compute Policies sind zentral**: compute budget, priority fee, tip-Policy sind Engine-owned (nicht in Strategien verteilt).
  - ✅ `FeePolicy` struct in `src/ipc/schema.rs` with compute/fee/cost limits
  - ✅ `TradeIntent.hint_*` fields for strategy fee hints (engine has final authority)
  - ✅ `RejectReason::FeeComputeExceedsLimit/FeePriorityExceedsLimit/FeeExceedsMaxCost/FeeUnprofitable`
  - ✅ `FeePolicy.compute_units_for_intent()` / `priority_fee_for_intent()` / `is_profitable_after_fees()`
  - ✅ Process checks in execution-engine apply fee policy before capital lock

---

## D) Global Arbitration, Locks & No Self-Competition (Architektur §3.1, §4, §5)

### P0
- [x] **Capital Locks**: Jede Execution reserviert Kapital eindeutig (SOL + Token), kein Überbuchen möglich.
  - ✅ `LockManager.try_lock_capital()` in `src/storage/locks.rs`
- [x] **Resource Locks**: Accounts/Pools/ATAs, die Konflikte erzeugen können, werden gelockt (oder es gibt eine bewusste Konflikt-Policy).
  - ✅ `LockManager.try_lock_resource()` mit `ResourceType` enum
- [x] **Preemption-Regeln implementiert**: Tier0 kann Tier1 preempten; Tier1 darf Tier0 niemals verdrängen.
  - ✅ `LockHolder.tier` für Priorität (niedriger = höher)
  - ✅ `LockResult::AcquiredByPreemption` in `src/storage/locks.rs`
  - ✅ Preemption-Logik in `try_lock_resource()`

### P1
- [x] **Fairness/Starvation Policy**: Dauerhafte Verdrängung wird begrenzt (z. B. max preemptions pro Worker/Slot).
  - ✅ `FairnessPolicy` struct in `src/ipc/schema.rs` with max_preemptions, window, protection duration
  - ✅ `FairnessTracker` in `src/storage/locks.rs` tracks preemption events per source
  - ✅ `RejectReason::FairnessStarved/FairnessBlocked` for starvation protection
  - ✅ `LockHolder.source` field for fairness attribution
  - ✅ `try_lock_resource()` checks fairness policy before allowing preemption
  - ✅ Starved sources get temporary protection from further preemption

---

## D.1) Invariants: Typ A vs Typ B (Arbitrage/MEV Einordnung)

Ziel: Verhindert „Arbitrage gehört wohin?“-Verwirrung durch harte Abnahmekriterien.

### P0
- [x] **Typ A (Strategy Arbitrage) = marktgetrieben**: darf Market-Scanning/Quotes/EV-Ranking betreiben und erzeugt nur `TradeIntent`s; sie darf keine Parent-Tx voraussetzen.
  - ✅ `IntentOrigin::StrategyA` in `src/ipc/schema.rs`
- [x] **Typ B (Execution MEV) = reaktiv/Tx-abhängig**: existiert nur in Bezug auf eine konkrete Parent-Tx oder Engine-State (z. B. eigene Pending-Tx, Bundle, observed Tx) und erzeugt interne Intents/Optimierungen; sie betreibt kein dauerhaftes Market-Scanning.
  - ✅ `IntentOrigin::ExecutionMevB` in `src/ipc/schema.rs`
- [x] **Decision Records enthalten Klassifikation**: jeder Intent/Decision Record enthält `origin_type = A|B` (oder äquivalent) + reason-coded Begründung, damit Post-Mortems klar sind.
  - ✅ `origin_type` Feld in `TradeIntent` und `DecisionRecord`

---

## E) Observability: Decision Records (Architektur §10) – „Warum hat er das getan?“

### P0
- [x] **Decision Record pro Intent**: Für jeden Intent existiert ein strukturierter Record (JSON/protobuf/bincode), der die Entscheidung nachvollziehbar macht.
  - ✅ `DecisionRecord` struct in `src/ipc/schema.rs`
  - ✅ Wird in `execution-engine` für jeden Intent geschrieben
- [x] **Record enthält Inputs**: Quotes/Route, Config-Snapshot-ID, Risk/State-Snapshot-ID, Balances/Locks, TTL/Deadline.
  - ✅ `config_snapshot_id`, `input_snapshots` HashMap in `DecisionRecord`
- [x] **Record enthält Checks**: Liste pass/fail pro Invariant/Rule mit konkreter Begründung.
  - ✅ `checks: Vec<CheckResult>` mit `check_name`, `passed`, `reason_code`, `details`
- [x] **Record enthält Output**: Plan-Hash, simulate result (err + log preview), send result (signature/bundle id), confirm status.
  - ✅ `plan_hash`, `simulate: SimulationResult`, `send: SendResult`, `outcome`
- [x] **Korrelation**: Jede Tx/Bundlesignature ist über Decision-ID und Intent-ID auffindbar.
  - ✅ `decision_id`, `intent_id` Felder; `ExecutionResult.signature`

### P1
- [x] **UI/Control zeigt Entscheidungen**: In der UI/Control Plane kann man die letzten N Decisions ansehen (inkl. „rejected reasons").
  - Hinweis: Das ist **Live/Recent Debug-Ansicht** (Operator-UI). Time-Series Charts/Trends/Alerting gehören in **Prometheus/Grafana** (siehe Abschnitt F).
  - ✅ `DecisionRecord`, `DecisionQuery`, `DecisionStats` models in `control_plane/main.py`
  - ✅ `GET /decisions` - List recent decisions with filters (limit, source, outcome, since, intent_id)
  - ✅ `GET /decisions/stats` - Aggregated statistics (by outcome, source, reject reason)
  - ✅ `GET /decisions/{decision_id}` - Get specific decision
  - ✅ `POST /decisions/query` - Complex query with full stats
  - ✅ NATS subscriber for `ironcrab.v1.decision_records` (live updates)
  - ✅ In-memory ring buffer cache (1000 decisions)
  - ✅ Audit logging for all decision views

---

## F) Metrics: Prometheus/Grafana Abnahme (Architektur §10)

Leitlinie: **Charts/Trends über Zeit** (Latenz, Reject-Rates, PnL/ROI, Queue Depth) gehören hierher (Prometheus → Grafana), nicht in die Control/UI.

### P0
- [x] **Pflicht-Metriken vorhanden**: 
  - `intents_received_total` (labels: source, tier)
  - `intents_rejected_total` (label: reason)
  - `plans_built_total`
  - `simulate_failed_total` (label: error_code)
  - `tx_sent_total`, `tx_confirmed_total`, `tx_failed_total`
  - `decision_latency_ms` (P50/P95/P99)
  - ✅ `src/metrics.rs`: `serve_metrics()` für Prometheus scraping
  - ✅ Alle 3 Binaries exposen Metriken (Ports 9801/9802/9803)
- [x] **„No silent failure"**: Es gibt keine Fehlerpfade ohne Metric + Decision Record.
  - ✅ Alle Errors in `execution-engine` werden mit `error!`/`warn!` + Reason-Code geloggt
  - ✅ `emit_rejected_decision()` schreibt Decision Record + loggt mit reason

### P1
- [x] **Per-Strategy/Per-Worker Attribution**: Profit/fees/latency sind pro source/worker sichtbar.
  - ✅ `source` Feld in `DecisionRecord` (propagiert von `TradeIntent.source`)
  - ✅ `source` Feld in `ExecutionResult` (propagiert von `TradeIntent.source`)
  - ✅ Alle Konstruktoren aktualisiert: `new_rejected()`, `new_sim_failed()`, `new_sent()`
  - ✅ Tests in `tests/ipc_schema_roundtrip.rs` validieren Source-Attribution

---

## G) Storage & Replay (Architektur §11)

### P0
- [x] **Replay-Paket definierbar**: Für einen Zeitraum kann man MarketEvents + Intents + Decisions exportieren (Flat files).
  - ✅ `JsonlWriter` mit täglicher Rotation schreibt alle Records
  - ✅ Pfadformat: `{prefix}/{date}/{stream}-{N}.jsonl`
- [x] **Deterministischer Replay-Run**: Offline-Replay reproduziert Decisions für denselben Input-Stream (mindestens für `Rejected/Planned/SimFailed`).
  - ✅ `tests/replay_deterministic.rs`: 4 Tests für Decision Record Determinismus
  - ✅ `test_decision_record_roundtrip`, `test_replay_determinism`, `test_decision_outcomes_correct`, `test_jsonl_append_integrity`

### P1
- [x] **Golden Replays**: Es gibt mindestens 3 gespeicherte „golden“ Replay-Szenarien, die in CI laufen.
  - ✅ Fixtures: `tests/fixtures/golden_replays/` (normal_trade, rejected_trade, sim_failed)
  - ✅ Tests: `tests/golden_replay_test.rs`

---

## H) Connectoren & Datenquellen: „Untrusted until proven“ (Architektur §6)

### P0
- [x] **Connector Contract Tests**: Für jeden DEX-Connector existieren Tests, die prüfen:
  - Quote-Ausgabe plausibel (monotonie/decimals)
  - Instruction-Builder erzeugt valide Accounts (layout checks)
  - Simulation für einfache Swap-Transaktion ist reproduzierbar (im Testnetz/Localnet/Recorded)
  - ✅ `tests/dex_connector_contracts.rs`: 4 Contract Tests
  - ✅ `contract_cfm_quote_monotonic`, `contract_cfm_price_impact_non_decreasing`
  - ✅ `contract_cfm_unknown_pair_returns_none`, `contract_cfm_zero_input`
- [x] **Unit-Normalisierung**: Ein zentraler Layer normalisiert amounts/decimals (keine DEX-spezifischen Sonderregeln verteilt im Code).
  - ✅ `ExplicitAmount` in `src/ipc/schema.rs` mit `raw`, `decimals`, `ui`
  - ✅ Helper-Methoden: `sol_from_lamports()`, `sol_from_ui()`, `from_ui()`, `as_f64()`

### P1
- [x] **Fuzz/Property Tests**: Mindestens 1 Property-Test pro kritischem Parser/Layout (z. B. Whirlpool/Raydium states).
  - ✅ `fuzz/fuzz_targets/fuzz_orca_whirlpool_layout.rs`
  - ✅ `fuzz/fuzz_targets/fuzz_raydium_pool_v4.rs`
  - ✅ `fuzz/fuzz_targets/fuzz_pumpfun_bonding_curve.rs`
  - ✅ `fuzz/fuzz_targets/fuzz_replay_log_parser.rs`

---

## I) Control Plane & Bus (Architektur §7, §8, §9)

### P0
- [x] **NATS Topics fixiert**: `MarketEvents`, `TradeIntents`, `ExecutionResults`, `ControlRequests` sind definiert, versioniert und dokumentiert.
  - ✅ `src/nats/topics.rs`: TOPIC_MARKET_EVENTS, TOPIC_TRADE_INTENTS, etc.
  - ✅ Version: `ironcrab.v1.*` Format
- [x] **Request/Reply für Control**: Start/Stop, risk limits, config reload laufen über request/reply (mit Timeout + Ack).
  - ✅ `control_plane/main.py`: POST /command/{component} → NATS request/reply
  - ✅ TOPIC_CONTROL_REQUESTS für Commands
- [x] **Hot Path bleibt Rust**: Kein Python/HTTP im Execution Hot Path.
  - ✅ Alle drei Binaries sind Rust
  - ✅ Control Plane (Python) nur für Management, nicht im Trading Hot Path

### P1
- [x] **RBAC (minimal)**: Mindestens Admin/Viewer Rollen (UI/API), Auditing der Control-Aktionen.
  - ✅ `control_plane/main.py`: Admin/Viewer via `X-API-Key`
  - ✅ Audit-Log `control_plane_audit.log`
- [x] **Runtime-Konfiguration via UI**: Alle Binary-Parameter (MomentumConfig, ExecutionConfig, etc.) sind über Control Plane/UI änderbar ohne Neustart.
  - MomentumConfig: Liquidity-Thresholds, Slippage, Position Size
  - ExecutionConfig: Risk Limits (max_position, daily_loss, max_slippage)
  - MarketDataConfig: DEX enables, rate limits
  - Änderungen werden über NATS gepusht und von Binaries hot-reloaded
  - ✅ Control Plane publisht Config Updates (`POST /config`)
  - ✅ Execution Engine subscribed `ironcrab.control.config.reload` und wendet Updates an
  - ✅ Momentum Bot subscribed + `apply_config_update()` für MomentumConfig
  - ✅ Market Data subscribed + `apply_config_update()` für MarketDataConfig

---

## J) Risk & Correctness (Architektur §5 + deine Zielanforderung „macht nichts, was er nicht soll“)

### P0
- [x] **Explizite Risk Invariants**: z. B. `max_position`, `daily_loss_limit`, `max_open_positions`, `max_slippage` sind als Engine-Checks implementiert.
  - ✅ `ExecutionConfig` in `execution-engine` mit 4 Risk-Parametern:
    - `max_position_size_lamports` (default 0.5 SOL)
    - `daily_loss_limit_lamports` (default 5 SOL)
    - `max_open_positions` (default 5)
    - `max_slippage_bps` (default 500 = 5%)
  - ✅ 4 Risk Checks in `process_intent()` vor Capital Lock
- [x] **Hard Fail mit Reason**: Wenn Risk verletzt wäre, wird der Intent rejected mit eindeutigem `reason_code` (nicht freitext-only).
  - ✅ `RejectReason` enum in `src/ipc/reason_codes.rs` mit 20+ Codes
  - ✅ `primary_reject_reason` in DecisionRecord
- [x] **No hidden defaults**: Jede Default-Policy ist dokumentiert und im Decision Record sichtbar.
  - ✅ Alle 6 Config-Structs dokumentiert mit Default-Werten:
    - `ExecutionConfig`, `MomentumConfig`, `NatsConfig`
    - `JsonlWriterConfig`, `EstimatorConfig`, `QuantileConfig`
  - ✅ `config_snapshot_id` in DecisionRecord für Korrelation

### P1
- [x] **State Consistency**: PnL/positions sind nach Restart konsistent (persisted snapshots + idempotency).
  - ✅ Execution Engine Snapshot: `execution_state.json` (daily loss, open positions, counters, processed intents)
  - ✅ Idempotency Restore: processed intents werden aus Snapshot zurückgeladen

---

## K) Performance / Latenz (Architektur §1, §6)

### P2 (Future Optimization)
- [ ] **Hot path allocations**: Kritische Pfade sind allocation-bewusst (Profiling vorhanden).
  - Bedeutet: Im Trading Hot Path (Intent → Quote → TX Build → Send) keine Heap-Allocations
  - Tools: `heaptrack`, `dhat`, Custom Allocator
  - Status: Nicht kritisch ohne <10ms Latenz-Anforderungen
- [ ] **Slot-to-send Latency**: P50/P95/P99 Ziele sind definiert und in Grafana sichtbar.
  - Typische Ziele: P50 <100ms, P95 <200ms, P99 <500ms
  - Messung: Geyser-Slot-Time → TX-Send-Time
  - Status: Metriken nicht implementiert
- [ ] **TPU/Relayer Path**: Execution nutzt TPU/Relayer (nicht nur `sendTransaction`), mit klaren Fallback-Regeln.
  - Aktuell: Jito Bundles (Arb) + RPC sendTransaction (Fallback)
  - TPU Direct wäre schneller (~50-100ms vs ~200-400ms)
  - Status: Jito ist ausreichend für Arb; TPU Direct ist optional

---

## K.1) P2 Performance Roadmap

Diese Sektion dokumentiert den Weg zu professioneller Latenz-Optimierung.

### Aktueller Stand (Januar 2026)
- **Server**: Frankfurt VPS mit eigenem Non-Voting Validator
- **Geyser**: Lokal am Validator (<1ms Event-Latenz)
- **TX Submission**: Jito Bundles (Arb) + RPC sendTransaction (Fallback)
- **Geschätzte Slot-to-Send**: ~300-500ms (nicht gemessen)

### Phase 1: Metriken & Baseline (Voraussetzung für alles)
**Ziel:** Verstehen wo die Zeit verloren geht

- [ ] Slot-to-Send Histogram in Prometheus/Grafana
  ```rust
  // In execution_engine.rs nach TX Send
  let latency_ms = now_ms - slot_timestamp_ms;
  metrics::histogram!("tx_slot_to_send_ms", latency_ms);
  ```
- [ ] Breakdown: Geyser-Event → Intent → Lock → Quote → Build → Send
- [ ] Grafana Dashboard mit P50/P95/P99 Latenz

### Phase 2: Code-Konsolidierung (NATS Optional)
**Ziel:** Weniger Netzwerk-Hops im Hot Path

Aktuell:
```
Geyser → market-data → NATS → arb-strategy → NATS → execution-engine
        ~~~~~~~~~~~~   ~~~~~               ~~~~~
        (Process)      (IPC)               (IPC)
```

Ziel (Single-Process Mode für Arb):
```
Geyser → [market-data + arb-strategy + execution-engine in-process]
```

- [ ] Feature-Flag `--single-process` für latenz-kritische Strategien
- [ ] NATS nur für Control Plane / Debugging, nicht im Hot Path
- [ ] In-Process Channels (tokio::mpsc) statt NATS für Arb

### Phase 3: TPU Direct Implementation
**Ziel:** ~100-200ms statt ~300-400ms

**Voraussetzungen:**
| Requirement | Dein Setup | Status |
|-------------|-----------|--------|
| Eigener Validator | ✅ Non-Voting | Vorhanden |
| Leader Schedule | Aus Geyser/RPC | Zu implementieren |
| QUIC Client | solana-tpu-client | Zu implementieren |
| Stake (optional) | 0 SOL | Ohne Stake funktioniert TPU auch |

**TPU ohne Stake:**
- ✅ Funktioniert bei normaler Netzwerk-Last (90% der Zeit)
- ⚠️ Bei Congestion werden gestakte Validators bevorzugt (Stake-weighted QoS)
- Für Arbitrage oft ausreichend, da Arb-Opportunities selten bei Peak-Congestion

#### Schritt 1: Dependencies (Cargo.toml)

```toml
[dependencies]
# Bereits vorhanden (prüfen ob Version passt):
solana-client = "2.1"           # RpcClient
solana-sdk = "2.1"              # Transaction, Keypair, etc.

# NEU für TPU Direct:
solana-tpu-client = "2.1"       # TpuClient, TpuClientConfig
solana-connection-cache = "2.1" # ConnectionCache für QUIC
```

**Hinweis:** Version muss zu deiner Solana-Version passen (aktuell Agave 3.x → Solana SDK 2.x).

#### Schritt 2: Config-Erweiterung (src/config.rs)

```rust
#[derive(Debug, Clone, Deserialize)]
pub struct TxSubmissionConfig {
    /// Primary submission method: "tpu", "jito", "rpc"
    pub primary_method: String,       // default: "tpu"
    
    /// Fallback chain (in order)
    pub fallback_chain: Vec<String>,  // default: ["jito", "rpc"]
    
    /// TPU-specific settings
    pub tpu_fanout_slots: u64,        // default: 2 (send to next N leaders)
    pub tpu_leader_forward_count: u64, // default: 4
    
    /// Timeout before trying next method
    pub method_timeout_ms: u64,       // default: 2000
    
    /// Retry on each method
    pub retries_per_method: u32,      // default: 2
}

impl Default for TxSubmissionConfig {
    fn default() -> Self {
        Self {
            primary_method: "tpu".into(),
            fallback_chain: vec!["jito".into(), "rpc".into()],
            tpu_fanout_slots: 2,
            tpu_leader_forward_count: 4,
            method_timeout_ms: 2000,
            retries_per_method: 2,
        }
    }
}
```

#### Schritt 3: TpuClient Setup (src/solana/tpu_client.rs - NEU)

```rust
use solana_client::rpc_client::RpcClient;
use solana_tpu_client::tpu_client::{TpuClient, TpuClientConfig, TpuSenderError};
use solana_sdk::{signature::Signature, transaction::Transaction};
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct TpuSubmitter {
    tpu_client: Arc<RwLock<Option<TpuClient>>>,
    rpc_client: Arc<RpcClient>,
    ws_url: String,
    config: TpuClientConfig,
}

impl TpuSubmitter {
    pub async fn new(
        rpc_client: Arc<RpcClient>,
        ws_url: &str,
        fanout_slots: u64,
        leader_forward_count: u64,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let config = TpuClientConfig {
            fanout_slots,
            ..TpuClientConfig::default()
        };
        
        // TpuClient erstellen (verbindet sich mit Leader Schedule)
        let tpu_client = TpuClient::new(
            Arc::clone(&rpc_client),
            ws_url,
            config.clone(),
        )?;
        
        Ok(Self {
            tpu_client: Arc::new(RwLock::new(Some(tpu_client))),
            rpc_client,
            ws_url: ws_url.to_string(),
            config,
        })
    }
    
    /// Send transaction via TPU Direct (QUIC)
    pub async fn send_transaction(&self, tx: &Transaction) -> Result<Signature, TpuSenderError> {
        let guard = self.tpu_client.read().await;
        if let Some(client) = guard.as_ref() {
            // Sendet an aktuelle + nächste Leader (fanout_slots)
            client.send_transaction(tx)
        } else {
            Err(TpuSenderError::Custom("TPU client not initialized".into()))
        }
    }
    
    /// Reconnect bei Verbindungsproblemen
    pub async fn reconnect(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut guard = self.tpu_client.write().await;
        *guard = Some(TpuClient::new(
            Arc::clone(&self.rpc_client),
            &self.ws_url,
            self.config.clone(),
        )?);
        Ok(())
    }
}
```

#### Schritt 4: Unified TX Sender (src/solana/tx_sender.rs - NEU)

```rust
use crate::solana::tpu_client::TpuSubmitter;
use crate::solana::jito::JitoClient;
use solana_client::rpc_client::RpcClient;
use solana_sdk::{signature::Signature, transaction::Transaction};

pub enum SendMethod {
    Tpu,
    Jito,
    Rpc,
}

pub struct TxSender {
    tpu: Option<TpuSubmitter>,
    jito: Option<JitoClient>,
    rpc: Arc<RpcClient>,
    config: TxSubmissionConfig,
}

impl TxSender {
    /// Send with automatic fallback chain
    pub async fn send_with_fallback(
        &self,
        tx: &Transaction,
        require_bundle: bool,  // From TradeIntent
    ) -> Result<(Signature, SendMethod), SendError> {
        
        // Bei Bundle-Requirement: Nur Jito erlaubt
        if require_bundle {
            return self.send_via_jito(tx).await
                .map(|sig| (sig, SendMethod::Jito));
        }
        
        // Normale Fallback-Chain: TPU → Jito → RPC
        let methods = std::iter::once(self.config.primary_method.as_str())
            .chain(self.config.fallback_chain.iter().map(|s| s.as_str()));
        
        let mut last_error = None;
        
        for method in methods {
            let result = match method {
                "tpu" => self.send_via_tpu(tx).await.map(|s| (s, SendMethod::Tpu)),
                "jito" => self.send_via_jito(tx).await.map(|s| (s, SendMethod::Jito)),
                "rpc" => self.send_via_rpc(tx).await.map(|s| (s, SendMethod::Rpc)),
                _ => continue,
            };
            
            match result {
                Ok(r) => {
                    // Metrik: welche Methode hat funktioniert
                    metrics::counter!("tx_send_success", 1, "method" => method);
                    return Ok(r);
                }
                Err(e) => {
                    metrics::counter!("tx_send_fallback", 1, 
                        "from" => method, 
                        "reason" => e.to_string()
                    );
                    last_error = Some(e);
                }
            }
        }
        
        Err(last_error.unwrap_or(SendError::NoMethodAvailable))
    }
    
    async fn send_via_tpu(&self, tx: &Transaction) -> Result<Signature, SendError> {
        let tpu = self.tpu.as_ref().ok_or(SendError::MethodNotConfigured("tpu"))?;
        
        tokio::time::timeout(
            Duration::from_millis(self.config.method_timeout_ms),
            tpu.send_transaction(tx)
        )
        .await
        .map_err(|_| SendError::Timeout("tpu"))?
        .map_err(|e| SendError::TpuError(e.to_string()))
    }
    
    async fn send_via_jito(&self, tx: &Transaction) -> Result<Signature, SendError> {
        let jito = self.jito.as_ref().ok_or(SendError::MethodNotConfigured("jito"))?;
        // ... existing Jito logic
    }
    
    async fn send_via_rpc(&self, tx: &Transaction) -> Result<Signature, SendError> {
        // ... existing RPC logic
    }
}
```

#### Schritt 5: Integration in Execution Engine

```rust
// src/bin/execution_engine.rs

// Bei Startup:
let tx_sender = TxSender::new(
    rpc_client.clone(),
    config.solana.ws_url.clone(),
    config.tx_submission.clone(),
    jito_client,  // Optional
).await?;

// Bei TX Send (in process_intent):
let (signature, method) = tx_sender.send_with_fallback(
    &signed_tx,
    intent.require_bundle,
).await?;

// Decision Record erweitern:
decision.send_method = Some(format!("{:?}", method));
```

#### Schritt 6: Config File Update (my_config.server.toml)

```toml
[tx_submission]
primary_method = "tpu"           # "tpu" | "jito" | "rpc"
fallback_chain = ["jito", "rpc"]
tpu_fanout_slots = 2             # Send to current + next N leaders
tpu_leader_forward_count = 4
method_timeout_ms = 2000
retries_per_method = 2
```

#### Schritt 7: Metriken für Monitoring

```rust
// Neue Metriken in src/metrics.rs:

// TX Submission Methode
metrics::counter!("tx_send_success", "method" => "tpu|jito|rpc");
metrics::counter!("tx_send_fallback", "from" => "...", "reason" => "...");

// TPU-spezifisch
metrics::histogram!("tpu_send_latency_ms", latency);
metrics::counter!("tpu_reconnect_total");
metrics::gauge!("tpu_leader_slots_ahead", slots);

// Vergleich
metrics::histogram!("tx_slot_to_confirm_ms", latency, "method" => "tpu|jito|rpc");
```

#### Checkliste für TPU-Umbau

- [ ] **Cargo.toml**: `solana-tpu-client` + `solana-connection-cache` hinzufügen
- [ ] **src/config.rs**: `TxSubmissionConfig` struct
- [ ] **src/solana/tpu_client.rs**: `TpuSubmitter` wrapper (NEU)
- [ ] **src/solana/tx_sender.rs**: `TxSender` mit Fallback-Chain (NEU)
- [ ] **src/solana/mod.rs**: Module exportieren
- [ ] **src/bin/execution_engine.rs**: `TxSender` integrieren
- [ ] **my_config.server.toml**: `[tx_submission]` Section
- [ ] **src/ipc/schema.rs**: `send_method` Feld in `DecisionRecord`
- [ ] **src/metrics.rs**: TPU-Metriken
- [ ] **Test**: Dry-run mit `primary_method = "rpc"` (kein Risiko)
- [ ] **Test**: TPU auf Devnet/Testnet
- [ ] **Deploy**: TPU auf Mainnet mit Monitoring

#### Validator Config: Keine Änderung nötig!

Dein Non-Voting Validator ist bereits korrekt konfiguriert:
```bash
--rpc-port 8899              # ✅ Für Leader Schedule / Cluster Info
--dynamic-port-range 8000-8025  # ✅ TPU Ports
--tpu-connection-pool-size 1024 # ✅ Genug Connections
```

Der TPU Client nutzt deinen Validator nur als **Datenquelle** (Leader Schedule, Cluster Nodes), nicht als Sender.

**Fallback-Strategie:**
```
1. TPU Direct (schnellster Pfad)
2. Jito Bundle (MEV-Protection, wenn Arb)
3. RPC sendTransaction (Fallback bei TPU-Fehler)
```

### Phase 4: Server-Migration (Optional)
**Ziel:** <50ms zum Validator-Cluster

| Standort | Latenz zu Validators | Kosten |
|----------|---------------------|--------|
| Frankfurt (aktuell) | ~20-50ms | € |
| Amsterdam | ~10-30ms | € |
| US East (Ashburn) | ~5-20ms | €€ |
| Co-Location | <1ms | €€€€ |

**Empfehlung:** Frankfurt/Amsterdam ist für Retail-Arb ausreichend. Co-Location nur sinnvoll wenn ihr gegen Jump/Wintermute antreten wollt (>$100k/Monat Infrastruktur).

### Phase 5: Voting Validator mit Stake (Optional)
**Ziel:** Stake-weighted QoS Priority

**Break-Even Rechnung:**
- Voting Validator Kosten: ~1.1 SOL/Tag (~$200/Tag bei $180/SOL)
- Stake für merkliche QoS: ~10,000 SOL ($1.8M)
- Stake-Weighted QoS bringt ~10-20% bessere Inclusion bei Congestion

**Fazit:** Nur sinnvoll für professionelle Trading-Firmen mit >$1M Kapital.

### Realistische Latenz-Ziele

| Phase | Slot-to-Send P50 | Investment |
|-------|-----------------|------------|
| Aktuell (Jito + Frankfurt) | ~300-500ms | € |
| + TPU Direct | ~150-300ms | €€ |
| + Amsterdam Migration | ~100-200ms | €€ |
| + Single-Process | ~50-150ms | Code-Arbeit |
| + Co-Location + Stake | ~10-50ms | €€€€€ |

**Gegen professionelle Market Maker (Jump, Wintermute, etc.):**
- Sie haben: Co-Location, 100k+ SOL Stake, dedizierte Infra
- Ihre Latenz: <10ms
- Strategie: Nicht auf Latenz konkurrieren, sondern auf:
  - Weniger kompetitive Pairs (Long-Tail Tokens)
  - Längere Arbitrage-Fenster (Cross-DEX mit illiquiden Pools)
  - Information Edge (bessere Signale, nicht schnellere Execution)

---

## L) Process Separation & Binaries (Debuggability / Fault Isolation)

### P0
- [x] **Execution ist ein eigenes Binary/Prozess**: Signing/Sending/Locks leben in einer separaten Execution Engine (Single-Signer). Kein anderer Prozess hat Key-/Send-Rechte.
  - ✅ `src/bin/execution_engine.rs` als separates Binary
- [x] **Klare Schnittstelle**: Kommunikation Strategie/Worker → Execution erfolgt nur über Intents (Bus/IPC), nicht über direkte Funktionsaufrufe, die Seiteneffekte verstecken.
  - ✅ `TradeIntent` über NATS/JSONL

### P1
- [x] **Bots getrennt startbar**: Sniper (Scout), Arbitrage-Scanner und ggf. Momentum sind eigene Binaries/Services (start/stop separat), um Fehlerquellen isoliert debuggen zu können.
  - ✅ `market-data`, `momentum-bot`, `execution-engine` als separate Binaries
  - ✅ `run_new.ps1` / `run_new.sh` zum separaten Starten
- [x] **Crash-Isolation**: Crash eines Bots darf Execution nicht crashen; Crash der Control Plane darf Trading nicht beeinflussen.
  - ✅ Separate Prozesse/Binaries; keine In-Process Kopplung
  - ✅ Trading-Hot-Path ist unabhängig von der Control Plane (Management-only)

---

## Praktische Umbau-Reihenfolge (empfohlen)

1) **P0**: Single-Signer + Intent Contract + Decision Records + Sim-Gate + Locks
2) **P1**: Preemption + Profit Attribution + Golden Replays + Connector Contract Tests
3) **P2**: Performance/TPU hardening + mehr Worker + Scaling

---

## TODO (Phase 2): Wallet Tracking ohne RPC-Scanning (Option C)

Ziel: market-data erkennt auch **manuelle Wallet-Aktionen** (Phantom/Jupiter/Transfers), ohne dass execution-engine Events der einzige Trigger sind.

- [ ] **Option C: TX-Inferenz für ATA Lifecycle**: market-data parst relevante Instruktionen (Associated Token Program create/close, idempotent create) und ruft denselben internen „track this ATA/mint“ Pfad auf wie Option B (ExecutionResult-getrieben).  
  Abnahme: **keine periodischen Wallet-RPC-Scans**, aber trotzdem Erkennung von neu erstellten/geschlossenen ATAs und schneller Geyser-Resubscribe.

---

## Abnahme-„Stop Rule“ (gegen €100 Debugging)

Wenn eine neue Funktionalität nicht mindestens erfüllt:
- Decision Record vollständig,
- simulate-gated (oder bewusst deaktiviert mit dokumentierter Begründung),
- reason-coded rejects,

…dann gilt sie als **nicht fertig** und darf nicht mit realem Kapital laufen.
---

## M) Live-Test Protokoll (Phase 1-3)

### Phase 1: Local Integration (2025-12-31) ✅
- [x] Alle 3 Binaries starten ohne Crash
- [x] NATS Verbindung funktioniert
- [x] JSONL Logs werden geschrieben
- [x] Market Events fließen (Simulate Mode: 1500+ Events)
- [x] Decision Records werden generiert
- [x] Idempotency-Check funktioniert (`LOCK_DUPLICATE_INTENT`)

### Phase 2: Mainnet Dry-Run (2025-12-31) ✅
- [x] Geyser-Verbindung zum lokalen Validator (Port 10000)
- [x] Echte Mainnet-Events (Raydium, Orca, PumpFun, Meteora)
- [x] 250.000+ Market Events pro Tag verarbeitet
- [x] NATS IPC zwischen allen Services funktioniert
- [x] Momentum-Bot empfängt Events (12.505+ events_received)
- [x] Execution-Engine im Dry-Run (keine echten TXs)

### Phase 3: Mainnet Live (2026-01-21) ✅
- [x] Wallet konfiguriert: `Ase7z1mRLps2cTNQnRHpLyQL4Q5FHwonjmZnYCTuUDZM`
- [x] TX Sending: ENABLED
- [x] Safety Limits aktiv:
  - Max Trade: 0.01 SOL (10M lamports)
  - Daily Loss Limit: 0.3 SOL (300M lamports)
  - Max Slippage: 3% (300 bps)
- [x] arb-strategy generiert Intents
- [x] Arb Intents werden verarbeitet (Decision Records)
- [x] Geyser-First Architecture vollständig implementiert

### Phase 4: Architecture Rebuild (2026-01-21) ✅
- [x] Multi-Process Architecture (market-data, momentum-bot, arb-strategy, execution-engine)
- [x] Geyser-basierte Pool Discovery für alle DEXes
- [x] DexPoolAccounts Events für alle DEXes
- [x] set_pool_from_accounts() für alle DEX Connectors
- [x] PoolStateUpdate Events (Vault Balances via Geyser)
- [x] BinArrayUpdate Events (Meteora Bin Arrays via Geyser)
- [x] WsolManager (Background WSOL Management)
- [x] AccountJanitor (Empty ATA Cleanup)
- [x] Alle Architecture Violations behoben

### Infrastruktur-Status (2026-01-21)
| Service | Port | Status | Funktion |
|---------|------|--------|----------|
| agave-validator | 8899/10000 | ✅ | Mainnet RPC + Geyser |
| nats-server | 4222 | ✅ | IPC Bus |
| market-data | 9801 | ✅ | Geyser → MarketEvents |
| momentum-bot | 9802 | ✅ | Events → Momentum Intents |
| arb-strategy | 9803 | ✅ | Events → Arb Intents |
| execution-engine | 9804 | ✅ | Intents → TXs |
| control-plane | 8080 | ✅ | Monitoring API |

