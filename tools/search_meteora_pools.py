#!/usr/bin/env python3
"""Search for all Meteora pools containing a specific mint."""
import json
import urllib.request
import base64

RPC_URL = "http://127.0.0.1:8899"
MINT = "BFuy9AJYKekZ2hik7b5mPhsunGscegi9vPY2bwzzBAGS"  # Delphi
SOL_MINT = "So11111111111111111111111111111111111111112"

# Meteora program IDs
METEORA_DLMM = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo"
METEORA_CPMM = "cpmmpPFsKiR4eeYnGSuXgkhLLgGL1j5FUZoJBJU9t9D"  # DAMM V2
METEORA_POOLS = "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB"  # Dynamic Pools (older)

def rpc_call(method, params):
    req = urllib.request.Request(
        RPC_URL,
        data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode(),
        headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode())

print(f"Searching for Meteora pools with Delphi ({MINT[:8]}...)")
print()

# Get program accounts for Meteora CPMM (DAMM V2)
print(f"Checking Meteora CPMM (DAMM V2) program: {METEORA_CPMM}")
try:
    # CPMM pool layout has mints at specific offsets
    # We search for pools containing our mint
    result = rpc_call("getProgramAccounts", [
        METEORA_CPMM,
        {
            "encoding": "base64",
            "filters": [
                {"dataSize": 637}  # CPMM pool size
            ]
        }
    ])
    
    if result.get("result"):
        pools = result["result"]
        print(f"Found {len(pools)} CPMM pools total")
        
        # Check each pool for our mint
        for pool in pools:
            pubkey = pool["pubkey"]
            data = base64.b64decode(pool["account"]["data"][0])
            
            # CPMM layout: token0_mint at offset 72, token1_mint at offset 104
            if len(data) >= 136:
                mint0 = base64.b64encode(data[72:104]).decode()
                mint1 = base64.b64encode(data[104:136]).decode()
                
                # Check if either mint matches (as raw bytes comparison)
                data_hex = data.hex()
                mint_bytes = bytes([int(MINT[i:i+2], 16) if i < len(MINT) else 0 for i in range(0, 64, 2)])
                
                # Simpler: just check if the mint pubkey bytes appear in the data
                from base58 import b58decode
                try:
                    mint_bytes = b58decode(MINT)
                    if mint_bytes in data:
                        print(f"  FOUND CPMM pool: {pubkey}")
                        # Extract reserve info
                except:
                    pass
    else:
        print(f"Error or no pools: {result.get('error')}")
except Exception as e:
    print(f"Error: {e}")

print()
print("Checking Meteora Dynamic Pools program:", METEORA_POOLS)
try:
    result = rpc_call("getProgramAccounts", [
        METEORA_POOLS,
        {
            "encoding": "base64",
            "filters": [
                {"dataSize": 944}  # Dynamic pool size
            ]
        }
    ])
    
    if result.get("result"):
        pools = result["result"]
        print(f"Found {len(pools)} Dynamic pools total")
        
        from base58 import b58decode
        mint_bytes = b58decode(MINT)
        
        for pool in pools:
            pubkey = pool["pubkey"]
            data = base64.b64decode(pool["account"]["data"][0])
            if mint_bytes in data:
                print(f"  FOUND Dynamic pool: {pubkey}")
except Exception as e:
    print(f"Error: {e}")
