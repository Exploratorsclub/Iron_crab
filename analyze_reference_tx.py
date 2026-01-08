#!/usr/bin/env python3
"""Analyze reference PumpSwap AMM transaction to verify account indices."""

import requests
import json

HELIUS_RPC = "https://mainnet.helius-rpc.com/?api-key=96755862-7b83-484a-9f7a-2c0620253cc1"
REF_TX = "3nj499thZ6JrdrC2WGGGRKoSC5Ydrat9gxP3XEnW5JK5ZWnXPzHE2QuAX8y7gvfsjRaLxCy3qkn6BYc1sxtfYiiY"

print(f"Analyzing TX: {REF_TX}\n")

PUMPFUN_AMM_PROGRAM_ID = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
EXPECTED_MARKET = "3zAgKE7aYLiSpGFmxrZSNGNPKi5e7JzYncmLDfZSbj1F"
EXPECTED_BASE_MINT = "7Vyudp37HypeMjBsbwTfq8ERx6WXm75JwmrWiob5pump"

payload = {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "getTransaction",
    "params": [REF_TX, {"encoding": "json", "maxSupportedTransactionVersion": 0}]
}

resp = requests.post(HELIUS_RPC, json=payload)
data = resp.json()

if "error" in data:
    print(f"RPC Error: {data['error']}")
    exit(1)

result = data.get("result")
if not result:
    print("No result in response")
    exit(1)

# Check block time
block_time = result.get("blockTime")
if block_time:
    from datetime import datetime
    dt = datetime.fromtimestamp(block_time)
    print(f"Block Time: {dt} ({block_time})\n")

tx = result["transaction"]
msg = tx["message"]
meta = data["result"]["meta"]

# Parse account keys
account_keys = msg.get("accountKeys", [])

# Extend with loaded addresses
if "loadedAddresses" in meta and meta["loadedAddresses"]:
    loaded = meta["loadedAddresses"]
    if "writable" in loaded:
        account_keys.extend(loaded["writable"])
    if "readonly" in loaded:
        account_keys.extend(loaded["readonly"])

print(f"Total account keys: {len(account_keys)}")
print(f"\nAccount Keys:")
for i, key in enumerate(account_keys):
    marker = ""
    if key == EXPECTED_MARKET:
        marker = " <--- MARKET"
    elif key == EXPECTED_BASE_MINT:
        marker = " <--- BASE_MINT"
    elif key == "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA":
        marker = " <--- PUMPFUN_AMM_PROGRAM"
    print(f"  [{i:2d}] {key}{marker}")

# Find PumpSwap AMM instructions
print(f"\nInstructions:")
for idx, ix in enumerate(msg.get("instructions", [])):
    program_idx = ix.get("programIdIndex")
    program_id = account_keys[program_idx] if program_idx < len(account_keys) else None
    
    if program_id == PUMPFUN_AMM_PROGRAM_ID:
        accounts = ix.get("accounts", [])
        print(f"\n  Instruction #{idx}: PumpSwap AMM")
        print(f"    Program ID Index: {program_idx}")
        print(f"    Account count: {len(accounts)}")
        print(f"    Account indices: {accounts}")
        print(f"\n    Mapped accounts:")
        for i, acc_idx in enumerate(accounts):
            acc = account_keys[acc_idx] if acc_idx < len(account_keys) else "OUT_OF_BOUNDS"
            marker = ""
            if acc == EXPECTED_MARKET:
                marker = " <--- MARKET"
            elif acc == EXPECTED_BASE_MINT:
                marker = " <--- BASE_MINT"
            print(f"      accounts[{i:2d}] = accountKeys[{acc_idx:2d}] = {acc}{marker}")
