#!/bin/bash
set -e

cd ~/Iron_crab

# Install solders
.venv/bin/pip install -q solders

# Extract wallet pubkey
WALLET_PUBKEY=$(.venv/bin/python3 <<'PYEOF'
import json
from solders.keypair import Keypair
with open('/home/ironcrab/.config/solana/id.json') as f:
    kp = Keypair.from_bytes(bytes(json.load(f)))
print(str(kp.pubkey()))
PYEOF
)

echo "Wallet: $WALLET_PUBKEY"

# Create systemd override
sudo mkdir -p /etc/systemd/system/market-data.service.d/
echo "[Service]" | sudo tee /etc/systemd/system/market-data.service.d/wallet.conf > /dev/null
echo "Environment=\"IRONCRAB_WALLET_PUBKEY=$WALLET_PUBKEY\"" | sudo tee -a /etc/systemd/system/market-data.service.d/wallet.conf > /dev/null

# Reload and restart
sudo systemctl daemon-reload
sudo systemctl restart market-data momentum-bot

echo "Services restarted with wallet env"
sleep 2

echo "--- market-data logs (wallet snapshot) ---"
sudo journalctl -u market-data --since '10 sec ago' | grep -i wallet || echo "No wallet snapshot logs yet"

echo ""
echo "--- Service status ---"
sudo systemctl status market-data momentum-bot --no-pager -l | grep -E '(Active:|market-data|momentum-bot)'
