# Server-Diagnose: Alle Intents Rejected

**Ziel:** Schnell herausfinden, warum nach einem Deployment alle Intents vom execution-engine abgelehnt werden.

---

## 1. Schnellcheck – primary_reject_reason aus Decision Records

Die Execution Engine schreibt jeden abgelehnten Intent als JSONL in die Decision Records. Das Feld `primary_reject_reason` enthält den genauen Grund.

**Auf dem Server ausführen** (im Verzeichnis wo `trade_logs` liegt, z.B. `/home/ironcrab/Iron_crab`):

```bash
# Heutige Decision Records durchsuchen (UTC-Datum)
TODAY=$(date -u +%Y%m%d)
LOG_DIR="${IRONCRAB_LOG_DIR:-trade_logs}"
DECISION_FILE="$LOG_DIR/decisions/decision_records-$TODAY.jsonl"

if [ -f "$DECISION_FILE" ]; then
  echo "=== Reject-Gründe (Anzahl) ==="
  jq -r 'select(.primary_reject_reason != null) | .primary_reject_reason' "$DECISION_FILE" 2>/dev/null | sort | uniq -c | sort -rn
  echo ""
  echo "=== Letzte 3 abgelehnte Records (relevante Felder) ==="
  jq -c 'select(.outcome == "Rejected" or .primary_reject_reason != null) | {intent_id, primary_reject_reason, outcome, ts_unix_ms}' "$DECISION_FILE" 2>/dev/null | tail -3
else
  echo "Keine Datei: $DECISION_FILE"
  echo "Alternativ alle JSONL in decisions/ prüfen:"
  for f in "$LOG_DIR"/decisions/decision_records-*.jsonl; do
    [ -f "$f" ] && echo "--- $f ---" && jq -r 'select(.primary_reject_reason != null) | .primary_reject_reason' "$f" 2>/dev/null | sort | uniq -c
  done
fi
```

---

## 2. journalctl – Laufzeit-Logs

```bash
# Execution Engine: letzte Rejects und Startup
sudo journalctl -u execution-engine -n 200 --no-pager | grep -E "reject|Reject|send_enabled|kill_switch|Transaction sending|no_keys|dry_run|simulate_only"

# Momentum Bot: filter rejections (falls Intents gar nicht erstellt werden)
sudo journalctl -u momentum-bot -n 200 --no-pager | grep -E "reject|Reject|FILTER_REJECTED|filter"
```

---

## 3. Kill Switch prüfen

Der Kill Switch wird in `execution_state.json` gespeichert und übersteht Neustarts.

```bash
LOG_DIR="${IRONCRAB_LOG_DIR:-trade_logs}"
STATE_FILE="$LOG_DIR/execution_state.json"

if [ -f "$STATE_FILE" ]; then
  echo "=== Kill Switch Status ==="
  jq '{kill_switch_active, run_id, saved_at}' "$STATE_FILE" 2>/dev/null
else
  echo "Keine State-Datei: $STATE_FILE"
fi
```

**Wenn `kill_switch_active: true`:** Kill Switch zurücksetzen (z.B. über Control Plane oder manuell `kill_switch_active` auf `false` setzen und Service neu starten).

---

## 4. send_enabled prüfen

`send_enabled=false` führt zu `primary_reject_reason: "send_disabled"`.

Gründe:
- `--simulate-only` oder `--dry-run` gesetzt
- Keine Wallet-Keys (IRONCRAB_KEYPAIR_PATH fehlt/falsch/Berechtigungsfehler)

```bash
# Startup-Log der Execution Engine prüfen
sudo journalctl -u execution-engine -b --no-pager | head -80
```

Nach folgenden Meldungen suchen:
- `"Transaction sending ENABLED"` → send_enabled = true
- `"Transaction sending DISABLED"` + Grund (`dry_run`, `simulate_only`, `no_keys`)

---

## 5. Häufige Reject-Gründe & Gegenmaßnahmen

| primary_reject_reason | Bedeutung | Gegenmaßnahme |
|-----------------------|-----------|---------------|
| `KillSwitchActive` | Kill Switch aktiv | Reset via Control Plane oder `execution_state.json` |
| `send_disabled` | Senden deaktiviert | Keys prüfen; `--dry-run`/`--simulate-only` entfernen |
| `SimFailed` | Simulation fehlgeschlagen | RPC/Geyser, Pool-Quote; sim_logs in Decision Record prüfen |
| `SimSlippageExceeded` | Slippage zu hoch | `max_slippage_bps` anpassen oder Marktvolatilität |
| `RiskMaxOpenPositions` | Max offene Positionen | Positionen schließen oder Limit erhöhen |
| `RiskDailyLossLimit` | Tägliches Verlustlimit | Limit erhöhen oder auf nächsten Tag warten |
| `LockCapitalConflict` | Kapital bereits reserviert | Warten oder Lock Manager prüfen |
| `BundleNotConfigured` | Jito nicht konfiguriert | Jito config prüfen oder Bundle-Anforderung entfernen |
| `QuoteUnavailable` | Keine Quote für Route | Pool-Cache, DEX-Support, Mint prüfen |

---

## 6. Einzeiler zum schnellen Abruf

```bash
# Reject-Gründe der letzten Stunde
cd /home/ironcrab/Iron_crab  # oder dein WorkingDirectory
jq -r 'select(.primary_reject_reason != null) | .primary_reject_reason' trade_logs/decisions/decision_records-$(date -u +%Y%m%d).jsonl 2>/dev/null | sort | uniq -c | sort -rn
```
