#!/usr/bin/env python3
"""Check SOL and token balances for a wallet via local RPC."""
import json
import urllib.request
import sys

RPC_URL = "http://127.0.0.1:8899"
WALLET = sys.argv[1] if len(sys.argv) > 1 else "Ase7z1mRLps2cTNQnRHpLyQL4Q5FHwonjmZnYCTuUDZM"

def rpc_call(method, params):
    req = urllib.request.Request(
        RPC_URL,
        data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode(),
        headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode())

# Get SOL balance
result = rpc_call("getBalance", [WALLET])
if "result" in result:
    lamports = result["result"]["value"]
    sol = lamports / 1e9
    print(f"SOL Balance: {sol:.6f} SOL ({lamports} lamports)")
else:
    print(f"Error: {result}")

# Get token accounts
result = rpc_call("getTokenAccountsByOwner", [
    WALLET,
    {"programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"},
    {"encoding": "jsonParsed"}
])

if "result" in result and result["result"]["value"]:
    print(f"\nToken Accounts ({len(result['result']['value'])}):")
    for acc in result["result"]["value"]:
        info = acc["account"]["data"]["parsed"]["info"]
        mint = info["mint"]
        amount = info["tokenAmount"]["uiAmountString"]
        if float(amount) > 0:
            print(f"  {mint}: {amount}")
else:
    print("\nNo token accounts with balance found")
