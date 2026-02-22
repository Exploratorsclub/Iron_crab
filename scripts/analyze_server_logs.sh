#!/bin/bash
# Server-Log-Analyse fÃ¼r IronCrab
# AusfÃ¼hren auf dem IronCrab-Server (z.B. per SSH)
#
# Pfade (gemÃ¤ÃŸ systemd/docs):
# - IRONCRAB_LOG_DIR = /home/ironcrab/Iron_crab/trade_logs (falls gesetzt)
# - Fallback: trade_logs/ (relativ zum Projektroot)
# - execution_results: trade_logs/executions/execution_results-YYYYMMDD.jsonl

set -e
ROOT="${IRONCRAB_LOG_DIR:-/home/ironcrab/Iron_crab/trade_logs}"
EXEC_DIR="${ROOT}/executions"
LOG_ROOT="${ROOT%/*}"

echo "=== IronCrab Log-Analyse ==="
echo "ROOT (trade_logs): $ROOT"
echo ""

echo "--- 1. Zugriff auf trade_logs/executions/ ---"
if [ -d "$EXEC_DIR" ]; then
    echo "OK: Verzeichnis existiert"
    ls -la "$EXEC_DIR"
    JSONL_COUNT=$(find "$EXEC_DIR" -name "execution_results-*.jsonl" 2>/dev/null | wc -l)
    echo "Anzahl execution_results-*.jsonl: $JSONL_COUNT"
else
    echo "WARNUNG: $EXEC_DIR existiert nicht."
fi
echo ""

echo "--- 2. TAKE_PROFIT in Logs ---"
if command -v journalctl &>/dev/null; then
    journalctl -u execution-engine -u momentum-bot --no-pager -n 500 2>/dev/null | grep -i "TAKE_PROFIT" || echo "(keine Treffer)"
fi
echo ""

echo "--- 3. wallet_sol_delta in JSONL ---"
for f in "$EXEC_DIR"/execution_results-*.jsonl; do
    [ -f "$f" ] || continue
    COUNT=$(grep -c "wallet_sol_delta" "$f" 2>/dev/null || echo 0)
    echo "$(basename "$f"): $COUNT Zeilen"
done
echo ""

echo "--- 4. SELL-Beispiele mit wallet_sol_delta_lamports + fill_out ---"
SAMPLE_COUNT=0
for f in "$EXEC_DIR"/execution_results-*.jsonl; do
    [ -f "$f" ] || continue
    while IFS= read -r line; do
        if echo "$line" | grep -qE '"side"[[:space:]]*:[[:space:]]*"([Ss]ell|sell)"' && echo "$line" | grep -q "wallet_sol_delta_lamports" && echo "$line" | grep -q "fill_out"; then
            echo "--- SELL ---"
            echo "$line" | python3 -m json.tool 2>/dev/null || echo "$line"
            SAMPLE_COUNT=$((SAMPLE_COUNT + 1))
            [ "$SAMPLE_COUNT" -ge 5 ] && break 2
        fi
    done < "$f"
done
[ "$SAMPLE_COUNT" -eq 0 ] && echo "Keine passenden SELL-DatensÃ¤tze."
echo "=== Ende ==="
