#!/bin/bash
# Test config update endpoint

cat > /tmp/cfg.json << 'JSONEOF'
{"component": "execution-engine", "config": {"send_enabled": false}}
JSONEOF

echo "Request body:"
cat /tmp/cfg.json

echo ""
echo "Response:"
curl -s -X POST http://127.0.0.1:8080/config \
  -H 'Content-Type: application/json' \
  -d @/tmp/cfg.json
echo ""
