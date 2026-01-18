#!/bin/bash
cd ~/Iron_crab

echo "=== KILL SWITCH STATUS ==="
f=$(ls -1t trade_logs/decisions/*.jsonl | head -n 1)
tail -100 "$f" | grep KILL_SWITCH_ACTIVE | tail -3 | jq '.checks.kill_switch_active, .execution_state.kill_switch_active' 2>/dev/null || echo "Cannot parse JSON"

echo ""
echo "=== execution-engine logs: kill switch ==="
sudo journalctl -u execution-engine --since '10 min ago' | grep -i kill | tail -10

echo ""
echo "=== Last 5 SIM_FAILED decisions ==="
tail -200 "$f" | grep '"reason_code":"SIM_FAILED"' | tail -5 | jq -r '[.intent_id, .checks.simulation_error // "N/A"] | @tsv'

echo ""
echo "=== Last 5 BUNDLE_FAILED decisions ==="
tail -200 "$f" | grep '"reason_code":"BUNDLE_FAILED"' | tail -5 | jq -r '[.intent_id, .checks.bundle_error // "N/A"] | @tsv'

echo ""
echo "=== Execution results: last 10 entries ==="
exec_file=$(ls -1t trade_logs/executions/*.jsonl 2>/dev/null | head -n 1)
if [ -n "$exec_file" ]; then
    tail -10 "$exec_file" | jq -r '[.outcome, .tx_signature // "N/A", .error_reason // "N/A"] | @tsv' 2>/dev/null || tail -10 "$exec_file"
else
    echo "No execution results file"
fi

echo ""
echo "=== Config: send_enabled check ==="
grep -E 'send_enabled|kill_switch' ~/Iron_crab/my_config.server.toml | head -10
