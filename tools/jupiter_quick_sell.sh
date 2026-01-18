#!/bin/bash
# Quick sell via Jupiter - finds best price across ALL DEXes

set -e

# Solana CLI path
SOLANA_BIN="${SOLANA_BIN:-$HOME/.local/share/solana/install/active_release/bin}"
export PATH="$SOLANA_BIN:$PATH"

TOKEN_MINT="$1"
AMOUNT="$2"

if [ -z "$TOKEN_MINT" ] || [ -z "$AMOUNT" ]; then
    echo "Usage: ./jupiter_quick_sell.sh <TOKEN_MINT> <AMOUNT_RAW>"
    echo ""
    echo "Example:"
    echo "  ./jupiter_quick_sell.sh 9V7jznWgdN6tjMaJ6Bq11ZVQMkza6Zh45atgXbVmpump 1494544945"
    exit 1
fi

# Get wallet from keypair file or env
KEYPAIR_PATH="${IRONCRAB_KEYPAIR_PATH:-$HOME/.config/solana/id.json}"

if [ ! -f "$KEYPAIR_PATH" ]; then
    echo "ERROR: Keypair not found at $KEYPAIR_PATH"
    echo "Set IRONCRAB_KEYPAIR_PATH environment variable"
    exit 1
fi

WALLET_PUBKEY=$(solana-keygen pubkey "$KEYPAIR_PATH")
echo "🪙 Wallet: $WALLET_PUBKEY"
echo "🎯 Selling: $AMOUNT tokens of $TOKEN_MINT"
echo ""

# Get Jupiter quote
echo "📡 Getting best quote from Jupiter..."
QUOTE=$(curl -s "https://quote-api.jup.ag/v6/quote?inputMint=${TOKEN_MINT}&outputMint=So11111111111111111111111111111111111111112&amount=${AMOUNT}&slippageBps=1000")

# Extract route info
IN_AMOUNT=$(echo "$QUOTE" | jq -r '.inAmount')
OUT_AMOUNT=$(echo "$QUOTE" | jq -r '.outAmount')
ROUTE=$(echo "$QUOTE" | jq -r '.routePlan[].swapInfo.label' | tr '\n' ' → ')

OUT_SOL=$(echo "scale=8; $OUT_AMOUNT / 1000000000" | bc)

echo "✅ Best route: $ROUTE"
echo "💰 You will receive: $OUT_SOL SOL"
echo ""

read -p "Proceed with swap? (yes/no): " CONFIRM

if [ "$CONFIRM" != "yes" ]; then
    echo "❌ Cancelled"
    exit 0
fi

# Create swap transaction
echo "📝 Creating swap transaction..."
SWAP_RESPONSE=$(curl -s -X POST "https://quote-api.jup.ag/v6/swap" \
    -H "Content-Type: application/json" \
    -d "{
        \"quoteResponse\": $QUOTE,
        \"userPublicKey\": \"$WALLET_PUBKEY\",
        \"wrapAndUnwrapSol\": true,
        \"dynamicComputeUnitLimit\": true,
        \"prioritizationFeeLamports\": \"auto\"
    }")

SWAP_TX=$(echo "$SWAP_RESPONSE" | jq -r '.swapTransaction')

if [ "$SWAP_TX" == "null" ] || [ -z "$SWAP_TX" ]; then
    echo "❌ ERROR: Failed to create swap transaction"
    echo "$SWAP_RESPONSE" | jq '.'
    exit 1
fi

# Decode, sign, and send
echo "✍️  Signing and sending transaction..."
echo "$SWAP_TX" | base64 -d > /tmp/swap_tx.bin

# Sign with solana CLI
solana sign /tmp/swap_tx.bin --keypair "$KEYPAIR_PATH" -o /tmp/swap_tx_signed.bin

# Send
TX_SIG=$(solana send-transaction /tmp/swap_tx_signed.bin --url https://mainnet.helius-rpc.com/?api-key=96755862-7b83-484a-9f7a-2c0620253cc1 --commitment confirmed)

echo ""
echo "✅ TRANSACTION SENT!"
echo "📝 Signature: $TX_SIG"
echo "🔗 Solscan: https://solscan.io/tx/$TX_SIG"

# Cleanup
rm -f /tmp/swap_tx.bin /tmp/swap_tx_signed.bin

echo ""
echo "⏳ Waiting for confirmation..."
solana confirm "$TX_SIG" --url https://mainnet.helius-rpc.com/?api-key=96755862-7b83-484a-9f7a-2c0620253cc1

echo "✅ Transaction confirmed!"
