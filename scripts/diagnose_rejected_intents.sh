#!/bin/bash
# Diagnose: Warum werden alle Intents rejected?
# Run on server, e.g.: bash scripts/diagnose_rejected_intents.sh
set -e

cd "${IRONCRAB_HOME:-${HOME}/Iron_crab}"
LOG_DIR="${IRONCRAB_LOG_DIR:-trade_logs}"
TODAY=$(date -u +%Y%m%d)
DECISION_FILE="$LOG_DIR/decisions/decision_records-$TODAY.jsonl"
STATE_FILE="$LOG_DIR/execution_state.json"

echo "=== 1. PRIMARY REJECT REASONS (aus decision_records-$TODAY.jsonl) ==="
if [ -f "$DECISION_FILE" ]; then
  jq -r 'select(.primary_reject_reason != null) | .primary_reject_reason' "$DECISION_FILE" 2>/dev/null | sort | uniq -c | sort -rn
  if [ $? -ne 0 ]; then
    echo "(jq fehlgesch - alternativ: tail -50 $DECISION_FILE)"
    tail -20 "$DECISION_FILE" | grep -o '"primary_reject_reason":"[^"]*"' | sort | uniq -c
  fi
else
  # Fallback: neueste decision file
  F=$(ls -1t "$LOG_DIR"/decisions/decision_records-*.jsonl 2>/dev/null | head -1)
  if [ -n "$F" ]; then
    echo "Datei: $F"
    jq -r 'select(.primary_reject_reason != null) | .primary_reject_reason' "$F" 2>/dev/null | sort | uniq -c | sort -rn | tail -20
  else
    echo "Keine decision_records gefunden unter $LOG_DIR/decisions/"
  fi
fi

echo ""
echo "=== 2. KILL SWITCH (execution_state.json) ==="
if [ -f "$STATE_FILE" ]; then
  jq '{kill_switch_active, run_id, saved_at}' "$STATE_FILE" 2>/dev/null || cat "$STATE_FILE" | head -20
else
  echo "Keine State-Datei: $STATE_FILE"
fi

echo ""
echo "=== 3. SEND_ENABLED / Startup (journalctl) ==="
sudo journalctl -u execution-engine -b --no-pager 2>/dev/null | grep -E "Transaction sending|send_enabled|dry_run|simulate_only|no_keys|Wallet keys" | head -10

echo ""
echo "=== 4. Letzte Rejects im Detail (falls vorhanden) ==="
if [ -f "$DECISION_FILE" ]; then
  jq -c 'select(.primary_reject_reason != null) | {intent_id, primary_reject_reason, source, outcome}' "$DECISION_FILE" 2>/dev/null | tail -5
fi

echo ""
echo "=== 5. SimFailed/QuoteUnavailable Details (letzte 3) ==="
if [ -f "$DECISION_FILE" ]; then
  jq -c 'select(.primary_reject_reason == "SIM_FAILED" or .primary_reject_reason == "QuoteUnavailable") | {intent_id, primary_reject_reason, simulate: .simulate.error_code}' "$DECISION_FILE" 2>/dev/null | tail -3
fi

echo ""
echo "=== 6. Service-Status ==="
sudo systemctl is-active momentum-bot execution-engine market-data 2>/dev/null | paste - - - - 2>/dev/null || echo "systemctl nicht verfügbar"
