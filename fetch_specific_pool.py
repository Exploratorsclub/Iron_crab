#!/usr/bin/env python3
import requests
import base64

url = "https://mainnet.helius-rpc.com/?api-key=96755862-7b83-484a-9f7a-2c0620253cc1"

# Fetch the specific pool
pool_pubkey = "EYj9xKw6ZszwpyNibHY7JD5o3QgTVrSdcBp1fMJhrR9o"

payload = {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "getAccountInfo",
    "params": [
        pool_pubkey,
        {"encoding": "base64"}
    ]
}

print(f"Fetching pool: {pool_pubkey}")
r = requests.post(url, json=payload)
result = r.json()

if "result" not in result or result["result"]["value"] is None:
    print("Pool not found or null!")
    print(result)
    exit(1)

account = result["result"]["value"]
data_b64 = account["data"][0]
data = base64.b64decode(data_b64)

print(f"\n=== Meteora WSOL-USDC Pool ===")
print(f"Pubkey: {pool_pubkey}")
print(f"Owner: {account['owner']}")
print(f"Size: {len(data)} bytes")
print(f"Lamports: {account['lamports']}")

print(f"\nFirst 512 bytes (hex dump):")
for i in range(0, min(512, len(data)), 16):
    hex_str = ' '.join(f'{b:02x}' for b in data[i:i+16])
    ascii_str = ''.join(chr(b) if 32 <= b < 127 else '.' for b in data[i:i+16])
    print(f"{i:04x}:  {hex_str:<48}  {ascii_str}")

# Look for pubkeys (32 bytes)
print(f"\n\nSearching for Pubkeys (32-byte aligned):")
for i in range(0, min(300, len(data)), 32):
    chunk = data[i:i+32]
    if len(chunk) == 32 and chunk != b'\x00' * 32:
        # Try to decode as base58
        try:
            from base58 import b58encode
            pubkey_str = b58encode(chunk).decode('ascii')
            print(f"  Offset {i:3d} (0x{i:02x}): {pubkey_str}")
        except:
            # Fallback to hex
            hex_str = ''.join(f'{b:02x}' for b in chunk)
            print(f"  Offset {i:3d} (0x{i:02x}): {hex_str}")

print(f"\n\nSaving full data to meteora_wsol_usdc_pool.bin...")
with open("meteora_wsol_usdc_pool.bin", "wb") as f:
    f.write(data)

print("Done!")
