#!/bin/bash
# Check BlueWhale ghost position status

MINT="9V7jznWgdN6tjMaJ6Bq11ZVQMkza6Zh45atgXbVmpump"
cd ~/Iron_crab

echo "=== BlueWhale Ghost Position Check ==="
echo "Mint: $MINT"
echo ""

# Check latest intents
echo "--- Intent Logs (last 3 entries) ---"
f=$(ls -1t trade_logs/intents/*.jsonl 2>/dev/null | head -n 1)
if [ -n "$f" ]; then
    echo "File: $f"
    grep "$MINT" "$f" 2>/dev/null | tail -n 3 | while read -r line; do
        action=$(echo "$line" | grep -o '"action":"[^"]*"' | cut -d'"' -f4)
        echo "  - Action: $action"
    done
else
    echo "No intent logs found"
fi

echo ""
echo "--- Decision Records (wallet snapshot events) ---"
d=$(ls -1t trade_logs/decisions/*.jsonl 2>/dev/null | head -n 1)
if [ -n "$d" ]; then
    echo "File: $d"
    grep -i "wallet.*snapshot\|$MINT" "$d" 2>/dev/null | tail -n 5
else
    echo "No decision records found"
fi

echo ""
echo "--- RPC: Check actual wallet balance ---"
# This will show if token account exists and balance
ATA="EiYjmozEXcbYLLMhrUtu15j1VG4fHPkvkwgnrET391gZ"
curl -s -X POST https://mainnet.helius-rpc.com/?api-key=96755862-7b83-484a-9f7a-2c0620253cc1 \
  -H "Content-Type: application/json" \
  -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getTokenAccountBalance\",\"params\":[\"$ATA\"]}" \
  | grep -o '"value":{[^}]*}' || echo "Account not found (already closed)"
