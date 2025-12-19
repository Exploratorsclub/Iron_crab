#!/bin/bash
# RPC Health Test Script
# Führe auf dem Server aus: bash scripts/test_rpc.sh

RPC_URL="${1:-http://127.0.0.1:8899}"

echo "=== Solana RPC Test für: $RPC_URL ==="
echo ""

# 1. Health Check
echo "1) Health Check..."
HEALTH=$(curl -s "$RPC_URL/health" 2>&1)
echo "   Ergebnis: $HEALTH"
echo ""

# 2. GetVersion
echo "2) GetVersion..."
VERSION=$(curl -s -X POST -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"getVersion"}' \
  "$RPC_URL" 2>&1)
echo "   $VERSION"
echo ""

# 3. GetSlot (aktueller Slot)
echo "3) GetSlot..."
SLOT=$(curl -s -X POST -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"getSlot"}' \
  "$RPC_URL" 2>&1)
echo "   $SLOT"
echo ""

# 4. GetHealth (neuere Methode)
echo "4) GetHealth (RPC-Methode)..."
HEALTH2=$(curl -s -X POST -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"getHealth"}' \
  "$RPC_URL" 2>&1)
echo "   $HEALTH2"
echo ""

# 5. GetSignaturesForAddress (mit bekanntem Token - z.B. SOL wrapped)
echo "5) GetSignaturesForAddress (SOL Mint)..."
SIGS=$(curl -s -X POST -H "Content-Type: application/json" \
  -d '{
    "jsonrpc":"2.0",
    "id":1,
    "method":"getSignaturesForAddress",
    "params":[
      "So11111111111111111111111111111111111111112",
      {"limit":5}
    ]
  }' \
  "$RPC_URL" 2>&1)
echo "   Ergebnis (gekürzt): ${SIGS:0:500}..."
echo ""

# 6. GetAccountInfo (prüft ob Account-Abfragen funktionieren)
echo "6) GetAccountInfo (Raydium AMM Program)..."
ACCT=$(curl -s -X POST -H "Content-Type: application/json" \
  -d '{
    "jsonrpc":"2.0",
    "id":1,
    "method":"getAccountInfo",
    "params":[
      "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
      {"encoding":"base64"}
    ]
  }' \
  "$RPC_URL" 2>&1)
if echo "$ACCT" | grep -q '"result"'; then
  echo "   ✓ Account gefunden"
else
  echo "   ✗ Fehler: $ACCT"
fi
echo ""

# 7. GetLatestBlockhash
echo "7) GetLatestBlockhash..."
BLOCKHASH=$(curl -s -X POST -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"getLatestBlockhash"}' \
  "$RPC_URL" 2>&1)
if echo "$BLOCKHASH" | grep -q '"blockhash"'; then
  echo "   ✓ Blockhash erhalten"
else
  echo "   ✗ Fehler: $BLOCKHASH"
fi
echo ""

# 8. Zeitmessung für getSignaturesForAddress (das was der Bot oft nutzt)
echo "8) Performance-Test: getSignaturesForAddress..."
START=$(date +%s%3N)
for i in {1..5}; do
  curl -s -X POST -H "Content-Type: application/json" \
    -d '{
      "jsonrpc":"2.0",
      "id":'$i',
      "method":"getSignaturesForAddress",
      "params":[
        "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
        {"limit":200}
      ]
    }' \
    "$RPC_URL" > /dev/null 2>&1
done
END=$(date +%s%3N)
ELAPSED=$((END - START))
AVG=$((ELAPSED / 5))
echo "   5 Abfragen in ${ELAPSED}ms (Durchschnitt: ${AVG}ms)"
echo ""

# 9. Geyser gRPC Check (optional)
echo "9) Geyser gRPC Check (http://127.0.0.1:10000)..."
GEYSER=$(curl -s --connect-timeout 2 "http://127.0.0.1:10000" 2>&1)
if [ -z "$GEYSER" ]; then
  echo "   Keine Antwort (normal für gRPC, Port erreichbar)"
elif echo "$GEYSER" | grep -q "error"; then
  echo "   ✗ Fehler: $GEYSER"
else
  echo "   Antwort: $GEYSER"
fi
echo ""

echo "=== Test abgeschlossen ==="
