# Storage Conventions (P0) – Replay & Decision Records

Dieses Dokument definiert minimale, **deterministische** Storage-Konventionen (Dateinamen, Schema-Versionierung, Rotation), passend zur Zielarchitektur in `docs/TARGET_ARCHITECTURE.md`.

Ziel: Replay/Forensik darf niemals am „irgendwie geloggt“ scheitern.

---

## 1) Grundregeln

- **Hot Path safe**: Trading-Pfade dürfen nicht auf DB/FS blockieren.
- **Append-only**: Write pattern ist append-only, keine In-Place Updates.
- **Schema-versioniert**: Jede Datei-/Record-Klasse hat `schema_version`.
- **Korrelation**: Alles lässt sich über IDs verknüpfen:
  - `event_id` (MarketEvent)
  - `intent_id` (TradeIntent)
  - `decision_id` (Decision Record)
  - `execution_id` (Execution result)

---

## 2) Log-Root & Layout

Root-Verzeichnis (Default):
- `IRONCRAB_LOG_DIR` falls gesetzt, sonst `trade_logs/`

Unterverzeichnisse:
- `trade_logs/market_events/`
- `trade_logs/intents/`
- `trade_logs/decisions/`
- `trade_logs/executions/`

---

## 3) Dateinamen (Rotation)

Rotation: **täglich** (UTC) + optional Größenlimit.

Namensschema:
- `market_events-YYYYMMDD.jsonl`
- `trade_intents-YYYYMMDD.jsonl`
- `decision_records-YYYYMMDD.jsonl`
- `execution_results-YYYYMMDD.jsonl`

Optional (Parquet für Analytics):
- `market_events-YYYYMMDD.parquet`
- `execution_results-YYYYMMDD.parquet`

---

## 4) Record-Header (Pflichtfelder)

Jeder JSONL-Record beginnt logisch mit:

- `schema_version` (u32)
- `ts_unix_ms` (u64)
- `component` (string) z. B. `market-data`, `momentum-bot`, `execution-engine`
- `build` (string) z. B. git SHA oder semver
- `run_id` (uuid/string) – Prozesslauf-ID

Zusätzlich pro Typ:

### 4.1 MarketEvents
- `event_id`
- `source` (geyser/rpc/ws)
- `slot` (wenn vorhanden)
- `kind` (pool_created, swap_observed, price_update, …)
- `payload` (normalisiert)

### 4.2 TradeIntents
- `intent_id`
- `source` (momentum/arb-strategy/execution-worker)
- `tier` (0/1)
- `deadline_slot` oder `ttl_ms`
- `required_capital` (units explizit)
- `resources` (mints/pools/accounts)
- `expected_value` / `roi_bps`

### 4.3 Decision Records
- `decision_id`
- `intent_id`
- `regime` (EARLY/ESTABLISHED/NA)
- `checks[]` (reason-coded pass/fail)
- `plan_hash`
- `simulate` { ok/err, logs_preview }
- `send` { bundle_id/signature }
- `confirm` { status, slot }

### 4.4 Execution Results
- `execution_id`
- `decision_id`
- `signature` / `bundle_id`
- `status` (sent/confirmed/failed)
- `fees` (lamports, tip, cu)
- `pnl` (gross/net, units)

---

## 5) Retention

Default:
- JSONL: 7–30 Tage (konfigurierbar)
- Parquet: optional länger

Regel:
- Rotation/Deletion läuft **asynchron** (nicht im Hot Path).

---

## 6) Minimaler Replay-„Bundle“

Ein Replay-Case besteht aus:
- `market_events-*.jsonl` (Input)
- `trade_intents-*.jsonl`
- `decision_records-*.jsonl`
- `execution_results-*.jsonl`
- `config.toml` (genauer Snapshot)

Damit kann man:
- Entscheidungen reproduzieren
- Reject-Gründe vergleichen
- Regression Tests bauen
