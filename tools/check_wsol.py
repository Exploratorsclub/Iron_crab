#!/usr/bin/env python3
"""Check WSOL ATA balance for the wallet."""
import json
import subprocess
import sys

WALLET = "CjJy3xB3U5cCMGV4v3axLATm6ZTQzVeSwrrHCZsQBX9M"
WSOL_MINT = "So11111111111111111111111111111111111111112"
RPC = "http://127.0.0.1:8899"

# Get token accounts for WSOL
payload = {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "getTokenAccountsByOwner",
    "params": [
        WALLET,
        {"mint": WSOL_MINT},
        {"encoding": "jsonParsed"}
    ]
}

result = subprocess.run(
    ["curl", "-s", "-H", "Content-Type: application/json", "-d", json.dumps(payload), RPC],
    capture_output=True, text=True
)

data = json.loads(result.stdout)
accounts = data.get("result", {}).get("value", [])

print(f"WSOL ATAs found: {len(accounts)}")
for acc in accounts:
    pubkey = acc["pubkey"]
    info = acc["account"]["data"]["parsed"]["info"]
    amount = info["tokenAmount"]["uiAmountString"]
    print(f"  {pubkey}: {amount} WSOL")

if not accounts:
    print("No WSOL ATAs - all unwrapped to native SOL!")
