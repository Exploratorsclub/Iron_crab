#!/usr/bin/env python3
"""Check SOL and token balances for a wallet via local RPC.

Supports both SPL Token and Token-2022 programs.
"""
import json
import urllib.request
import sys

RPC_URL = "http://127.0.0.1:8899"
WALLET = sys.argv[1] if len(sys.argv) > 1 else "Ase7z1mRLps2cTNQnRHpLyQL4Q5FHwonjmZnYCTuUDZM"

# Token program IDs
SPL_TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN_2022_PROGRAM = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"

def rpc_call(method, params):
    req = urllib.request.Request(
        RPC_URL,
        data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode(),
        headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode())

def get_token_accounts(wallet, program_id):
    """Get token accounts for a specific token program."""
    result = rpc_call("getTokenAccountsByOwner", [
        wallet,
        {"programId": program_id},
        {"encoding": "jsonParsed"}
    ])
    if "result" in result and result["result"]["value"]:
        return result["result"]["value"]
    return []

# Get SOL balance
result = rpc_call("getBalance", [WALLET])
if "result" in result:
    lamports = result["result"]["value"]
    sol = lamports / 1e9
    print(f"SOL Balance: {sol:.6f} SOL ({lamports} lamports)")
else:
    print(f"Error: {result}")

# Get token accounts from BOTH programs
spl_accounts = get_token_accounts(WALLET, SPL_TOKEN_PROGRAM)
token_2022_accounts = get_token_accounts(WALLET, TOKEN_2022_PROGRAM)

all_accounts = []
for acc in spl_accounts:
    acc["_program"] = "SPL Token"
    all_accounts.append(acc)
for acc in token_2022_accounts:
    acc["_program"] = "Token-2022"
    all_accounts.append(acc)

if all_accounts:
    print(f"\nToken Accounts ({len(all_accounts)} total: {len(spl_accounts)} SPL, {len(token_2022_accounts)} Token-2022):")
    for acc in all_accounts:
        info = acc["account"]["data"]["parsed"]["info"]
        mint = info["mint"]
        amount = info["tokenAmount"]["uiAmountString"]
        program = acc["_program"]
        if float(amount) > 0:
            print(f"  [{program}] {mint}: {amount}")
else:
    print("\nNo token accounts with balance found")
