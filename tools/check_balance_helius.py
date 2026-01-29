#!/usr/bin/env python3
"""Check token balances via Helius mainnet RPC (bypasses local validator)."""

import json
import urllib.request
import sys

WALLET = "Ase7z1mRLps2cTNQnRHpLyQL4Q5FHwonjmZnYCTuUDZM"
HELIUS_RPC = "https://mainnet.helius-rpc.com/?api-key=96755862-7b83-484a-9f7a-2c0620253cc1"

def main():
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
        HELIUS_RPC,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"}
    )
    
    try:
        resp = urllib.request.urlopen(req, timeout=15)
        data = json.loads(resp.read())
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    
    print(f"Token accounts for {WALLET} (via Helius):\n")
    
    accounts = data.get("result", {}).get("value", [])
    found = 0
    for acc in accounts:
        info = acc["account"]["data"]["parsed"]["info"]
        mint = info["mint"]
        bal = info["tokenAmount"]["uiAmountString"]
        if float(bal) > 0:
            found += 1
            print(f"  {mint}: {bal}")
    
    if found == 0:
        print("  (no tokens with balance > 0)")
    
    print(f"\nTotal: {found} token(s) with balance")

if __name__ == "__main__":
    main()
