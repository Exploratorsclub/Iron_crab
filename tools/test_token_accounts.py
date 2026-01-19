#!/usr/bin/env python3
"""Test if getTokenAccountsByOwner works on local validator."""
import json
import urllib.request

WALLET = "Ase7z1mRLps2cTNQnRHpLyQL4Q5FHwonjmZnYCTuUDZM"
RPC_URL = "http://127.0.0.1:8899"

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
    resp = urllib.request.urlopen(req, timeout=30)
    result = json.loads(resp.read().decode())
    
    if "error" in result:
        print(f"ERROR: {result['error']}")
    elif "result" in result:
        accounts = result["result"]["value"]
        print(f"SUCCESS! Found {len(accounts)} token accounts:")
        for acc in accounts:
            info = acc["account"]["data"]["parsed"]["info"]
            print(f"  - Mint: {info['mint'][:20]}... Balance: {info['tokenAmount']['uiAmountString']}")
    else:
        print(f"Unexpected response: {result}")
except Exception as e:
    print(f"Request failed: {e}")
