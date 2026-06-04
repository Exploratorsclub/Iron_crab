# IronCrab — Production Runbook (Multi-Process Architecture)

Go-Live Anleitung für den Betrieb auf dem Validator-Server. Der Bot läuft auf dem gleichen Server wie der Agave Validator für minimale Latenz.

## Architektur-Übersicht

IronCrab verwendet eine **Multi-Prozess-Architektur** mit 6 separaten Services:

| Service | Port | Funktion |
|---------|------|----------|
| `market-data` | 9801 | Geyser-Ingest, Pool Discovery, MarketEvents |
| `momentum-bot` | 9802 | Strategy: EARLY + ESTABLISHED Policies |
| `arb-strategy` | 9803 | Strategy: Multi-Pool Arbitrage |
| `execution-engine` | 9804 | Single-Signer, Tx Build/Sim/Send |
| `control-plane` | 8080 | REST API, Config, Kill-Switch |
| `trades-server` | 9899 | Grafana Infinity Datasource |

**NATS** ist der IPC-Layer zwischen den Prozessen.

```
Geyser/RPC
    │
    ▼
market-data ── MarketEvents ──► momentum-bot ── TradeIntents ──► execution-engine
                            └─► arb-strategy ─────────────────┘
                                                                      │
                                                    ExecutionResults ◄┘
                                                                      │
                                              control-plane / trades-server
```

## Voraussetzungen

- Debian 13 (oder kompatible Linux-Distribution)
- Rust Toolchain installiert (`rustup`)
- Python 3.11+ mit venv
- NATS Server (`nats-server` package)
- Agave Validator auf dem gleichen Server (siehe `docs/VALIDATOR_SETUP.md`)
- Funded Keypair unter `/home/ironcrab/.config/solana/id.json`
- Mindestens 0.5–1.0 SOL für Rent und Fees

## Dateien

| Datei | Zweck |
|-------|-------|
| `my_config.server.toml` | Produktions-Config (alle Rust-Binaries) |
| `deploy.sh` | Wrapper-Script → ruft `deploy_new.sh` auf |
| `deploy_new.sh` | Build + Systemd Install + Restart |
| `docs/systemd/*.service` | Service-Files für alle 6 Prozesse |
| `docs/systemd/ironcrab.target` | Orchestriert alle Services |
| `docs/grafana_*.json` | Grafana Dashboards |

## Einmalige Konfiguration

1) **Config anpassen** (`my_config.server.toml`):
   ```toml
   [solana]
   rpc_url = "http://127.0.0.1:8899"      # Lokaler Validator
   ws_url = "ws://127.0.0.1:8900"         # Lokaler Validator WS
   keypair_path = "/home/ironcrab/.config/solana/id.json"
   
   [geyser]
   endpoint = "http://127.0.0.1:10000"    # Lokaler Geyser gRPC
   
   [nats]
   url = "nats://127.0.0.1:4222"
   ```

2) **Keypair Balance prüfen**: `solana balance`

3) **NATS Server aktivieren**:
   ```bash
   sudo systemctl enable --now nats-server
   ```

4) **Optional**: Grafana Dashboards importieren

## Deployment

### Standard-Deployment (empfohlen)
```bash
./deploy.sh
```

Dies führt automatisch aus:
1. `git pull origin architecture-rebuild`
2. Cargo Build aller 4 Rust-Binaries (release)
3. Python venv Setup für control-plane
4. Systemd Services installieren
5. `ironcrab.target` starten (alle 6 Services)
6. Status-Check

### Optionen
```bash
# Nur bestimmten Service neu deployen
./deploy.sh --component execution-engine

# Ohne Rebuild (nur Service-Restart)
./deploy.sh --skip-build

# Legacy Monolith (deprecated)
./deploy.sh --legacy
```

## Service Management

### Alle Services starten/stoppen
```bash
sudo systemctl start ironcrab.target   # Startet alle 6 Services
sudo systemctl stop ironcrab.target    # Stoppt alle 6 Services
sudo systemctl restart ironcrab.target # Restart alle
```

### Einzelne Services
```bash
sudo systemctl restart execution-engine
sudo systemctl status momentum-bot
sudo systemctl stop arb-strategy
```

### Logs
```bash
# Live Logs einzelner Service
journalctl -u execution-engine -f
journalctl -u market-data -f
journalctl -u momentum-bot -f
journalctl -u arb-strategy -f
journalctl -u control-plane -f
journalctl -u trades-server -f

# Alle IronCrab Services kombiniert
journalctl -u 'market-data' -u 'momentum-bot' -u 'arb-strategy' -u 'execution-engine' -u 'control-plane' -f

# Letzte 100 Zeilen
journalctl -u execution-engine -n 100 --no-pager
```

## Endpoints

| Endpoint | URL | Zweck |
|----------|-----|-------|
| market-data Metrics | `http://localhost:9801/metrics` | Prometheus |
| momentum-bot Metrics | `http://localhost:9802/metrics` | Prometheus |
| arb-strategy Metrics | `http://localhost:9803/metrics` | Prometheus |
| execution-engine Metrics | `http://localhost:9804/metrics` | Prometheus |
| Control Plane API | `http://localhost:8080` | REST API |
| Trades API | `http://localhost:9899/trades` | Grafana Infinity |

### Geyser / market-data Metriken (PR #143)

Nach dem Geyser-Stream-Resilienz-Deploy ersetzen **neue** Prometheus-Namen die bisherigen Legacy-Zähler. Manuelle Grafana-Panels oder Alerts, die noch die alten Namen nutzen, zeigen dann keine Daten mehr.

| Alt (entfernt) | Neu | Typ | Kurzbedeutung |
|----------------|-----|-----|----------------|
| `geyser_reconnects_total` | `geyser_reconnect_total{reason="stream_ended"}` | counter (Text-Export) | Resubscribe nach Stream-Ende |
| — | `geyser_reconnect_total{reason="stream_error"}` | counter | harter Fehler → neuer Connect |
| — | `geyser_reconnect_total{reason="sink_gone"}` | counter | Subscription-Sink weg |
| `geyser_errors_total` | `geyser_stream_errors_total` | counter | gRPC-`Err` auf dem Stream |
| — | `geyser_connected` | gauge 0/1 | 1 = verbunden |

**Job-Label:** Prometheus scraped `market-data` typischerweise als `job="market-data"` (siehe `prometheus_multiprocess.yml`). In PromQL und Dashboards immer `{job="market-data"}` verwenden.

**Schnellcheck auf dem Server:**

```bash
curl -s http://localhost:9801/metrics | grep '^geyser_'
```

**Incident-Debug:** `journalctl -u market-data` (Stream, Reconnects) und im importierten Multi-Process-Dashboard die Zeile **Market Data Service** → Panels **Geyser Connected** / **Geyser Reconnects (5m rate)**.

**Betrieb:** `docs/systemd/market-data.service` setzt `Restart=always` (transiente Stream-Abbrüche sollen die Unit nicht dauerhaft inaktiv lassen). Nach Deploy in Grafana Dashboard JSON neu importieren bzw. Refresh; Explore: `geyser_connected{job="market-data"}`. **systemd-Watchdog:** Zusätzlich zum bestehenden Ping im Haupt-`select!` pingt ein **dedizierter Task alle 5 s** (`sd_notify(WATCHDOG)`), damit `WatchdogSec=30` nicht verfehlt wird, wenn NATS-Backpressure oder Geyser-Arbeit den Main-Loop verzögert (**PR151-NATS-WATCHDOG-FOLLOWUP** in `docs/BUGS_FIXES.md`).

**Prometheus / `rate()`:** Der Text-Export der Metriken kann je nach Prometheus-Konfiguration abweichen — nach Deploy in Explore verifizieren, z. B. `rate(geyser_reconnect_total{job="market-data"}[5m])` und `rate(geyser_stream_errors_total{job="market-data"}[5m])` liefern sinnvolle Zeitreihen.

### Pipeline-Latenz (Prometheus-Histogramme, A–I)

Segmentierte End-to-End-Latenzen über die drei Prozesse **market-data → momentum-bot → execution-engine**.
Alle Werte nutzen konsistent `RecordHeader.ts_unix_ms` bzw. Geyser-`Instant::now()`-Startpunkte am Publish-Pfad; Details in `docs/BUGS_FIXES.md` (Eintrag **PIPELINE-LATENCY-METRICS**).

**Ingest-Lag (Geyser vs. market-data Broadcast)** — siehe `docs/BUGS_FIXES.md` (**INGEST-LAG-METRICS**):

| Signal | Metrik | Kurzinterpretation |
|--------|--------|-------------------|
| Channel-Queue | `market_data_tx_channel_lag_ms`, `market_data_account_channel_lag_ms` | Zeit zwischen Listener-`send` und market-data-`recv` (Broadcast + Tokio-Scheduling), **kein** Netzwerk danach |
| Tx-Broadcast-Backlog (Gauge) | `market_data_tx_broadcast_queue_depth` | Nach jedem erfolgreichen Tx-`recv` im **dedizierten Tx-Ingest-Task**: verbleibende Nachrichten im `broadcast`-Receiver (sollte unter Last ~0 bleiben; siehe **MARKET-DATA-TX-INGEST-FAIRNESS**) |
| Account-Broadcast-Backlog (Gauge) | `market_data_account_broadcast_queue_depth` | Nach jedem erfolgreichen Account-`recv` im **dedizierten Account-Ingest-Task**: verbleibende Nachrichten im Account-`broadcast`-Receiver (sollte unter Last ~0 bleiben; siehe **MARKET-DATA-ACCOUNT-INGEST-FAIRNESS**) |
| Account-Worker-Queues (Gauge) | `market_data_account_worker_queue_depth` | Summe der Nachrichten in den **8** per-`pubkey`-Shard-`mpsc`-Queues zwischen Recv und Worker (**MARKET-DATA-ACCOUNT-THROUGHPUT-P0**) |
| Account-Publish-Queue (Gauge) | `market_data_account_publish_queue_depth` | Jobs in der dedizierten NATS-Publish-`mpsc` (JetStream + Core MarketEvent) |
| Account-Early-Drop (Counter) | `market_data_account_early_drop_total` | Billiger Relevanz-Filter im Recv-Task (kein DEX-Parse) |
| Account-Handler-Zeit (Histogram, µs) | `market_data_account_handler_duration_us_*` | Wall-Time pro Worker für `handle_geyser_account` (ohne Warteschlangen-Wartezeit) |
| Drops | `market_data_tx_broadcast_lagged_total`, `market_data_account_broadcast_lagged_total` | Summe der übersprungenen Nachrichten bei `RecvError::Lagged` |
| Ketten vs. Wall | `market_data_bonding_to_trade_slot_delta_slots` | Slot-Delta letztes `BondingCurveProgress` → nächstes Pump.fun-`Trade` (I-16: Geyser-Slots) |

**Entscheidungsbaum (nach Metrik-Deploy):**

- `market_data_tx_channel_lag_ms` / `market_data_account_channel_lag_ms` **p99 hoch** → Backlog/Fairness eher in market-data (Broadcast/Loop), nicht „langsamer Publish“ allein.
- Channel-Lag **niedrig**, aber `market_data_trade_after_bonding_publish_ms` (**B★**) weiter hoch → eher Geyser/Subscription/Vor market-data.
- `market_data_bonding_to_trade_slot_delta_slots` **hoch** → kettenbedingte Lücke zwischen Bonding- und Trade-Sicht.
- Slot-Delta **klein**, B★ **groß** → Wall-Lag ohne Slot-Erklärung (MATRIX-Muster: Scheduling/Stream).

**Prod-Gate (Tx-Ingest-Fairness, nach Deploy):** Ziel ist niedriger `market_data_tx_channel_lag_ms` bei gleichzeitig niedrigem `market_data_tx_broadcast_queue_depth` und **ohne** Regression bei `market_data_geyser_to_publish_ms_trade` (Publish-Pfad unverändert schnell). Beispiel-PromQL (5m-Fenster, Namen an `job` anpassen):

```promql
histogram_quantile(0.50, sum(rate(market_data_tx_channel_lag_ms_bucket[5m])) by (le))
histogram_quantile(0.99, sum(rate(market_data_tx_channel_lag_ms_bucket[5m])) by (le))
market_data_tx_broadcast_queue_depth
histogram_quantile(0.50, sum(rate(market_data_geyser_to_publish_ms_trade_bucket[5m])) by (le))
```

**Prod-Smoke (Geyser explicit sync, nach Deploy):** TX-Pfad coalesced Sync — erwarte steigende `rate(market_data_geyser_sync_batch_total)` bei Trade-Last, **ohne** proportionales Wachstum von `geyser_tracked_accounts` durch unpinned Fremdpools; `market_data_geyser_sync_pending` sollte meist 0 sein.

```promql
market_data_geyser_sync_batch_total
market_data_geyser_sync_immediate_total
market_data_geyser_sync_pending
geyser_tracked_accounts
```

**Prod-Gate (Account-Ingest-Fairness + Throughput P0, nach Deploy):** Ziel: `market_data_account_channel_lag_ms` **p50 < 20 ms**, **p95 < 100 ms**; `market_data_account_broadcast_queue_depth` **p50 ≈ 0**, **p99 < 10**; `market_data_account_worker_queue_depth` und `market_data_account_publish_queue_depth` dauerhaft niedrig (kein anhaltendes NATS-Await im Account-Handler). `market_data_account_broadcast_lagged_total == 0`. Tx-Metriken (`market_data_tx_channel_lag_ms`, `market_data_tx_broadcast_queue_depth`, `market_data_geyser_to_publish_ms_trade`) ohne Regression. Optional B★ (`market_data_trade_after_bonding_publish_ms`) und `market_data_bonding_to_trade_slot_delta_slots` beobachten.

```promql
histogram_quantile(0.50, sum(rate(market_data_account_channel_lag_ms_bucket[5m])) by (le))
histogram_quantile(0.95, sum(rate(market_data_account_channel_lag_ms_bucket[5m])) by (le))
histogram_quantile(0.99, sum(rate(market_data_account_channel_lag_ms_bucket[5m])) by (le))
market_data_account_broadcast_queue_depth
market_data_account_worker_queue_depth
market_data_account_high_priority_queue_depth
market_data_account_low_priority_queue_depth
market_data_account_publish_queue_depth
market_data_account_early_drop_total
histogram_quantile(0.99, sum(rate(market_data_pool_mint_map_to_devwallet_ms_bucket[5m])) by (le))
histogram_quantile(0.99, sum(rate(market_data_bonding_curve_grpc_to_devwallet_ms_bucket[5m])) by (le))
histogram_quantile(0.50, sum(rate(market_data_account_handler_duration_us_bucket[5m])) by (le))
market_data_account_broadcast_lagged_total
histogram_quantile(0.50, sum(rate(market_data_tx_channel_lag_ms_bucket[5m])) by (le))
histogram_quantile(0.50, sum(rate(market_data_geyser_to_publish_ms_trade_bucket[5m])) by (le))
```

| Segment | Metrik (Präfix je nach Export) | Kurzinterpretation |
|--------|--------------------------------|---------------------|
| A | `market_data_geyser_to_publish_ms_*` (Suffix `_trade`, `_bonding_curve`, …) | Geyser-Eingang bis erfolgreicher Core-NATS-Publish |
| B | `market_data_slot_lag_at_publish_slots_*` | Slot-Differenz am Publish (market-data) |
| C | `momentum_event_to_ingest_ms_*` | MarketEvent-Producer-Zeit bis Momentum-Ingest (nur Core NATS) |
| D | `momentum_intent_header_to_publish_ms_*` | Intent-Header-Zeit bis JetStream-TradeIntent-Publish |
| E | `momentum_publish_to_intent_ms_*` | Kausales Event-`ts_unix_ms` bis Intent-Header (Momentum-intern) |
| F | `execution_intent_header_to_receive_ms_*` | Intent-Header bis erster Zeile `process_intent` |
| G | `execution_process_intent_us_*` | Gesamtdauer `process_intent` (Mikrosekunden) |
| H | `execution_intent_to_confirm_ms_*` | Intent-Header bis bestätigtes On-Chain-Outcome |
| I | `execution_slot_lag_at_send_slots_*` | `cached_blockhash.slot` minus Intent-Metadaten-`slot` nach erfolgreichem Send |

Beispiel **rate()** über 5m (Namen an eueren `job`/Labels anpassen):

```promql
histogram_quantile(0.99, sum(rate(momentum_intent_header_to_publish_ms_bucket[5m])) by (le))
histogram_quantile(0.99, sum(rate(execution_process_intent_us_bucket[5m])) by (le)) / 1e6
```

### PumpSwap Async-Healing Metrics

Im `execution-engine`-Metrics-Endpoint sind fuer den Hot-Path-Healing-Pfad jetzt
zusetzlich folgende Counter relevant:

- `pumpswap_hot_path_healing_trigger_total`
- `pumpswap_hot_path_healing_cooldown_suppressed_total`
- `pumpswap_hot_path_healing_async_publish_success_total`
- `pumpswap_hot_path_healing_async_publish_fail_total`
- `pumpswap_hot_path_healing_skipped_no_nats_total`

Interpretation:

- `trigger_total` steigt, wenn ein regulaerer PumpSwap-Hot-Path-SELL nach
  strukturellem Sim-Fail einen async `force_refresh` publishen wuerde.
- `cooldown_suppressed_total` steigt, wenn derselbe Pfad wegen lokalem
  Mint-Cooldown bewusst unterdrueckt wird.
- `async_publish_success_total` bedeutet `nats.publish -> Ok(true)`.
- `async_publish_fail_total` bedeutet `Ok(false)` oder `Err(...)`; in diesem
  Fall startet kein erfolgreicher Healing-Cooldown.
- `skipped_no_nats_total` zeigt, dass der Healing-Pfad erreicht wurde, aber
  kein NATS-Client verfuegbar war.

## Dashboard-Interpretation (WSOL-first)

Aktuelle Architektur handelt primär in WSOL (kein Auto-Unwrap). Daher gelten
folgende Interpretations-Regeln:

- **WSOL-Balance (ATA)** = Trading-Liquiditaet (entscheidend fuer Buy/Scale-In)
- **WSOL-Wrap-Events** = Nachschub/Buffer fuer Trading
- **Open Positions** = Strategie-Sicht (korrekt nur mit aktivem Reconciliation)
- **ExecutionResults (Confirmed/Sent/Rejected)** = reale Handelsausfuehrung
- **Available WSOL** = Lock/Budget-Metrik (WSOL ATA Balance, nicht native SOL)

Weniger aussagekraeftig:

- **Wallet SOL Balance** als Trading-Kapital (WSOL ist primär)

Merksatz: **"WSOL ist Kapital, SOL ist Reserve."**

Hinweis (Wallet-Snapshot JetStream):
Die Wallet-Snapshot-Recovery ist aktuell nicht nach Wallet gefiltert. Das ist
ok, solange nur ein Wallet genutzt wird. Bei Multi-Wallet-Betrieb muss ein
Wallet-Filter im Consumer ergaenzt werden.

## JSONL Logs (Decision Records)

Alle Prozesse schreiben append-only JSONL nach `trade_logs/`:

```bash
# Decision Records (execution-engine)
ls -1t trade_logs/decisions/
tail -f trade_logs/decisions/decision_records-$(date +%Y%m%d).jsonl

# Trade Intents (momentum-bot, arb-strategy)
ls -1t trade_logs/intents/
ls -1t trade_logs/arb_intents/

# Market Events (market-data)
ls -1t trade_logs/market_events/
```

### Arb Reject Analysis (häufige Debug-Aufgabe)
```bash
# Letzte 400 Arb-Decision Records, Reject Codes zählen
f=$(ls -1t trade_logs/decisions/decision_records-*.jsonl | head -n 1)
tail -n 400 "$f" | grep '"source":"arb-strategy"' | \
  grep -o '"reason_code":"[^"]*"' | cut -d'"' -f4 | sort | uniq -c | sort -nr
```

## Safety Checklist

- [ ] Starte mit kleinen Limits: `max_buy_sol = 0.02`
- [ ] `require_freeze_auth_none = true` beibehalten
- [ ] `send_enabled = false` für Dry-Run Tests
- [ ] Logs auf Rate Limiting und Errors überwachen
- [ ] Decision Records auf `simulation_failed` prüfen
- [ ] Limits erst nach mehreren erfolgreichen Sessions erhöhen

## Troubleshooting

| Problem | Diagnose | Lösung |
|---------|----------|--------|
| Service startet nicht | `journalctl -u <service> -n 50` | Fehlermeldung lesen |
| NATS Verbindung fehlgeschlagen | `systemctl status nats-server` | NATS starten |
| Validator nicht erreichbar | `curl http://127.0.0.1:8899/health` | Validator prüfen |
| Geyser Verbindung fehlgeschlagen | market-data Logs prüfen | Geyser Plugin Config |
| Keypair Permissions | `ls -la ~/.config/solana/id.json` | `chmod 600` |
| Keine Intents ankommen | momentum-bot Logs, `filter_passed_total` | Filter zu strikt? Nach Deploy #138 u. a. häufig **`WAIT_BUYER_WINDOW`** (Käuferzahl) **und** Velocity. Velocity ist jetzt **`min_trades_per_min`** (nicht mehr `/s`); Prod-JetStream nicht mit altem `5/s` als `5/min` lesen — Start-Tuning z. B. **45–90/min**, beobachten. Deprecated `min_trades_per_sec` in Updates wird ×60 gewarnt. |
| Simulation immer failed | execution-engine Logs | RPC/Balance Problem |

### Service-spezifische Checks

```bash
# market-data: Geyser Verbindung
journalctl -u market-data | grep -i "geyser\|connected"

# momentum-bot: Intent Rate
journalctl -u momentum-bot | grep "TradeIntent"

# execution-engine: Lock/Simulation
journalctl -u execution-engine | grep -i "lock\|simulation"

# NATS Topics prüfen
nats sub "ironcrab.v1.>" --count 5
```

## CPU Pinning

Die Rust-Services sind auf dedizierte CPUs gepinnt (vermeidet Validator-Interferenz):

| Service | CPUAffinity |
|---------|-------------|
| market-data | 56-59 |
| momentum-bot | 52-55 |
| arb-strategy | 48-51 |
| execution-engine | 60-63 |

Konfiguriert in `docs/systemd/*.service`.

## Kill-Switch

```bash
# Via Control Plane API
curl -X POST http://localhost:8080/kill

# Via NATS direkt
nats pub ironcrab.control.kill "{}"

# Manuell alle Services stoppen
sudo systemctl stop ironcrab.target
```

## Lokale Entwicklung (Windows/macOS)

Für lokale UI-Entwicklung mit SSH-Tunnel zum Server:

```powershell
# Windows: Tunnel + UI starten
.\run_local.ps1 -Action start -Host ironcrab-prod

# Status prüfen
.\run_local.ps1 -Action status

# Stoppen
.\run_local.ps1 -Action stop
```

Siehe `.github/copilot-instructions.md` für Details.

## 2-Hop Cross-DEX (arb-strategy)

Nach `two_hop_enabled=true` (Control Plane / UI) liefert **arb-strategy** (Port 9803) Prometheus-Metriken für Debug und Prod-Gate:

```bash
curl -s http://127.0.0.1:9803/metrics | grep arb_two_hop
```

| Metrik | Bedeutung |
|--------|-----------|
| `arb_two_hop_opportunities_total` | Spread + Profit-Filter bestanden (vor Intent-Build) |
| `arb_two_hop_rejected_total{reason="spread_too_large"}` | Vergleichbare Preise wahrscheinlich inkonsistent (>10% bps, bzw. 2% Stable) |
| `arb_two_hop_rejected_total{reason="spread_below_min"}` | Spread unter `min_spread_bps` |
| `arb_two_hop_rejected_total{reason="profit_below_min"}` | Geschätzter Netto-Profit unter Schwellwert |
| `arb_two_hop_rejected_total{reason="stale_price"}` | Pool-Preis älter als 30s |
| `arb_two_hop_rejected_total{reason="insufficient_pools"}` | Weniger als 2 Pools mit vergleichbarem Preis (MASTER cache) |

**Erwartung nach Preis-Fix:** `spread_too_large` sinkt stark; `opportunities_total` oder plausible `profit_below_min` (mit bekannter Liquidität auf mindestens einer Seite) steigen.

Logs: `journalctl -u arb-strategy | grep "Arb check rejected"`

## Siehe auch

- [Iron_crab-eval/docs/spec/](https://github.com/Exploratorsclub/Iron_crab-eval) — Spezifikation (TARGET_ARCHITECTURE, ROLE_SEPARATION, DEFINITION_OF_DONE, STORAGE_CONVENTIONS)
- [VALIDATOR_SETUP.md](VALIDATOR_SETUP.md) — Validator + Geyser Konfiguration
