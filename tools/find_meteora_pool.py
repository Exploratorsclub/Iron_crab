#!/usr/bin/env python3
"""Find Meteora pools for a token mint."""
import json
import urllib.request
import sys

RPC_URL = "http://127.0.0.1:8899"
MINT = sys.argv[1] if len(sys.argv) > 1 else "BFuy9AJYKekZ2hik7b5mPhsunGscegi9vPY2bwzzBAGS"

# Meteora program IDs
METEORA_DLMM = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo"
METEORA_CPMM = "cpmmpPFsKiR4eeYnGSuXgkhLLgGL1j5FUZoJBJU9t9D"  # DAMM V2

def rpc_call(method, params):
    req = urllib.request.Request(
        RPC_URL,
        data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode(),
        headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode())

print(f"Searching pools for mint: {MINT}")
print()

# Check the pool address from dextools (AfEUx...NWCZ)
# Full address needed - let's get it from logs
pool_addr = "AfEUxLVKixaD3RFbBcELEbFquFuCEgxJqo7u5XmNWCZ"

print(f"Checking pool: {pool_addr}")
result = rpc_call("getAccountInfo", [pool_addr, {"encoding": "base64"}])
if result.get("result") and result["result"]["value"]:
    owner = result["result"]["value"]["owner"]
    data_len = len(result["result"]["value"]["data"][0]) if result["result"]["value"]["data"] else 0
    print(f"  Owner (Program): {owner}")
    print(f"  Data length: {data_len}")
    
    if owner == METEORA_DLMM:
        print("  Type: Meteora DLMM (bin-based)")
    elif owner == METEORA_CPMM:
        print("  Type: Meteora CPMM / DAMM V2 (constant product)")
    else:
        print(f"  Type: Unknown program")
else:
    print("  Pool not found or error")

# Also check what pool the liquidation tried to use
print()
print("Checking pool from liquidation attempt: BL4UVxA3fK5euJWzb6RfiKdKbEqWVBmcFMpG9YM1dU3X")
pool_addr2 = "BL4UVxA3fK5euJWzb6RfiKdKbEqWVBmcFMpG9YM1dU3X"
result = rpc_call("getAccountInfo", [pool_addr2, {"encoding": "base64"}])
if result.get("result") and result["result"]["value"]:
    owner = result["result"]["value"]["owner"]
    print(f"  Owner (Program): {owner}")
    if owner == METEORA_DLMM:
        print("  Type: Meteora DLMM (bin-based)")
    elif owner == METEORA_CPMM:
        print("  Type: Meteora CPMM / DAMM V2 (constant product)")
else:
    print("  Pool not found")
