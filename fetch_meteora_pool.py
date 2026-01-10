#!/usr/bin/env python3
import requests
import base64

url = "https://mainnet.helius-rpc.com/?api-key=96755862-7b83-484a-9f7a-2c0620253cc1"

payload = {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "getProgramAccounts",
    "params": [
        "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo",
        {
            "encoding": "base64",
            "filters": [{"dataSize": 904}],
            "limit": 1
        }
    ]
}

print("Fetching Meteora DLMM pool...")
r = requests.post(url, json=payload)
result = r.json()

if "result" not in result or not result["result"]:
    print("No pools found!")
    exit(1)

pool = result["result"][0]
data_b64 = pool["account"]["data"][0]
data = base64.b64decode(data_b64)

print(f"\n=== Meteora DLMM Pool ===")
print(f"Pubkey: {pool['pubkey']}")
print(f"Size: {len(data)} bytes")
print(f"\nFirst 256 bytes (hex):")

for i in range(0, min(256, len(data)), 16):
    hex_str = ' '.join(f'{b:02x}' for b in data[i:i+16])
    ascii_str = ''.join(chr(b) if 32 <= b < 127 else '.' for b in data[i:i+16])
    print(f"{i:04x}:  {hex_str:<48}  {ascii_str}")

print(f"\n\nSaving full data to meteora_pool_data.bin...")
with open("meteora_pool_data.bin", "wb") as f:
    f.write(data)

print("Done!")
