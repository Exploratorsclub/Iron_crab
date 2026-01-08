#!/usr/bin/env python3
"""Check if reference TX is in getSignaturesForAddress results."""

import requests
import json

HELIUS_RPC = "https://mainnet.helius-rpc.com/?api-key=96755862-7b83-484a-9f7a-2c0620253cc1"
MARKET = "3zAgKE7aYLiSpGFmxrZSNGNPKi5e7JzYncmLDfZSbj1F"
REF_TX = "3nj499thZ6JrdrC2WGGGRKoSC5Ydrat9gxP3XEnW5JK5ZWnXPzHE2QuAX8y7gvfsjRaLxCy3qkn6BYc1sxtfYiiY"

payload = {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "getSignaturesForAddress",
    "params": [MARKET, {"limit": 200}]
}

print(f"Fetching signatures for market: {MARKET}")
print(f"Looking for TX: {REF_TX}\n")

resp = requests.post(HELIUS_RPC, json=payload)
data = resp.json()

if "error" in data:
    print(f"RPC Error: {data['error']}")
    exit(1)

result = data.get("result")
if not result:
    print("No result in response")
    exit(1)

print(f"Found {len(result)} signatures\n")

# Check if reference TX is in list
found = False
for i, sig_info in enumerate(result):
    sig = sig_info.get("signature")
    if sig == REF_TX:
        print(f"✅ FOUND reference TX at index {i}!")
        print(f"   Signature: {sig}")
        print(f"   Block Time: {sig_info.get('blockTime')}")
        print(f"   Slot: {sig_info.get('slot')}")
        print(f"   Error: {sig_info.get('err')}")
        found = True
        break

if not found:
    print(f"❌ Reference TX NOT FOUND in {len(result)} signatures!")
    print(f"\nFirst signature: {result[0].get('signature')}")
    print(f"Last signature: {result[-1].get('signature')}")
    
    # Show some sample signatures
    print(f"\nFirst 5 signatures:")
    for i in range(min(5, len(result))):
        sig_info = result[i]
        from datetime import datetime
        block_time = sig_info.get('blockTime')
        dt = datetime.fromtimestamp(block_time) if block_time else "Unknown"
        print(f"  [{i}] {sig_info.get('signature')} @ {dt}")
