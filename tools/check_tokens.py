#!/usr/bin/env python3
"""Check token accounts for a wallet."""
import urllib.request
import json
import sys

WALLET = "Ase7z1mRLps2cTNQnRHpLyQL4Q5FHwonjmZnYCTuUDZM"
RPC_URL = "https://mainnet.helius-rpc.com/?api-key=96755862-7b83-484a-9f7a-2c0620253cc1"

payload = {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "getTokenAccountsByOwner",
    "params": [
        WALLET,
        {"programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"},
        {"encoding": "jsonParsed"}
    ]
}

req = urllib.request.Request(
    RPC_URL,
    data=json.dumps(payload).encode(),
    headers={"Content-Type": "application/json"}
)

try:
    resp = json.load(urllib.request.urlopen(req, timeout=10))
    accounts = resp.get("result", {}).get("value", [])
    
    if not accounts:
        print("No token accounts found")
        sys.exit(0)
    
    print(f"Found {len(accounts)} token account(s):")
    for acc in accounts:
        info = acc["account"]["data"]["parsed"]["info"]
        mint = info["mint"]
        amount = info["tokenAmount"]["uiAmountString"]
        decimals = info["tokenAmount"]["decimals"]
        print(f"  {mint}: {amount} (decimals={decimals})")
except Exception as e:
    print(f"Error: {e}")
    sys.exit(1)
