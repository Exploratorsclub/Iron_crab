#!/usr/bin/env python3
"""Find the best (most liquid) Meteora DLMM pool for analysis"""

import requests
import base64

url = "https://mainnet.helius-rpc.com/?api-key=96755862-7b83-484a-9f7a-2c0620253cc1"

# Fetch multiple pools
payload = {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "getProgramAccounts",
    "params": [
        "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo",
        {
            "encoding": "base64",
            "filters": [{"dataSize": 904}],
            "limit": 20
        }
    ]
}

print("Fetching top 20 Meteora DLMM pools...")
r = requests.post(url, json=payload)
result = r.json()

if "result" not in result or not result["result"]:
    print("No pools found!")
    exit(1)

pools = result["result"]
print(f"Found {len(pools)} pools\n")

# Analyze each pool to find the most liquid one
print("Analyzing pools by lamports (proxy for liquidity)...\n")
print(f"{'Rank':<5} {'Pubkey':<45} {'Lamports':>15} {'SOL':>12}")
print("=" * 85)

sorted_pools = sorted(pools, key=lambda p: p["account"]["lamports"], reverse=True)

for i, pool in enumerate(sorted_pools[:10], 1):
    pubkey = pool["pubkey"]
    lamports = pool["account"]["lamports"]
    sol = lamports / 1e9
    print(f"{i:<5} {pubkey:<45} {lamports:>15,} {sol:>12.4f}")

# Save the best pool
best_pool = sorted_pools[0]
data_b64 = best_pool["account"]["data"][0]
data = base64.b64decode(data_b64)

print(f"\n\nBest pool (highest lamports):")
print(f"Pubkey: {best_pool['pubkey']}")
print(f"Lamports: {best_pool['account']['lamports']:,}")
print(f"Size: {len(data)} bytes")

print(f"\nSaving to meteora_best_pool.bin...")
with open("meteora_best_pool.bin", "wb") as f:
    f.write(data)

print("\nFirst 256 bytes (hex):")
for i in range(0, min(256, len(data)), 16):
    hex_str = ' '.join(f'{b:02x}' for b in data[i:i+16])
    print(f"{i:04x}:  {hex_str}")

print("\nDone! Use this pool for implementation.")
