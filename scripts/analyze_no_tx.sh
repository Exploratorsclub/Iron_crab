#!/bin/bash
cd ~/Iron_crab

echo "=== ARB STRATEGY: Last 30 decisions ==="
f=$(ls -1t trade_logs/decisions/decision_records-*.jsonl 2>/dev/null | head -n 1)
if [ -n "$f" ]; then
    tail -300 "$f" | grep '"source":"arb-strategy"' | tail -30 | \
    jq -r '[.outcome, .reason_code // "N/A", .checks.roi_bps // "N/A", .checks.simulated_profit_lamports // "N/A"] | @tsv' 2>/dev/null || \
    tail -300 "$f" | grep arb-strategy | tail -30 | grep -o '"outcome":"[^"]*"' | cut -d'"' -f4 | sort | uniq -c
else
    echo "No decision records found"
fi

echo ""
echo "=== ARB STRATEGY: Rejection reason summary (last 500) ==="
if [ -n "$f" ]; then
    tail -500 "$f" | grep '"source":"arb-strategy"' | \
    grep -o '"reason_code":"[^"]*"' | cut -d'"' -f4 | sort | uniq -c | sort -nr | head -15
fi

echo ""
echo "=== EXECUTION ENGINE: Last 20 decisions (any source) ==="
tail -100 "$f" | grep -v arb-strategy | tail -20 | \
jq -r '[.source, .outcome, .reason_code // "N/A", .intent_id] | @tsv' 2>/dev/null || \
tail -100 "$f" | grep -v arb-strategy | tail -20 | head -5

echo ""
echo "=== EXECUTION RESULTS: Count by outcome (today) ==="
exec_file=$(ls -1t trade_logs/executions/execution_results-*.jsonl 2>/dev/null | head -n 1)
if [ -n "$exec_file" ]; then
    cat "$exec_file" | jq -r '.outcome' | sort | uniq -c
else
    echo "No execution results found"
fi

echo ""
echo "=== ARB INTENTS: Published count (last hour) ==="
intent_file=$(ls -1t trade_logs/arb_intents/arb_intents-*.jsonl 2>/dev/null | head -n 1)
if [ -n "$intent_file" ]; then
    one_hour_ago=$(($(date +%s) - 3600))
    one_hour_ago_ms=$((one_hour_ago * 1000))
    grep -o '"ts_unix_ms":[0-9]*' "$intent_file" | cut -d: -f2 | \
    awk -v cutoff="$one_hour_ago_ms" '$1 > cutoff' | wc -l
else
    echo "No arb intent log found"
fi

echo ""
echo "=== SERVICES STATUS ==="
sudo systemctl is-active arb-strategy execution-engine momentum-bot market-data | paste -d' ' - - - -
