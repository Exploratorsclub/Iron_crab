#!/usr/bin/env python3
"""Find the exact Delphi CPMM pool address and analyze it."""
import json
import urllib.request
import base64
import struct
import base58

RPC_URL = "https://mainnet.helius-rpc.com/?api-key=96755862-7b83-484a-9f7a-2c0620253cc1"
METEORA_CPMM_PROGRAM = "cpmmpPFsKiR4eeYnGSuXgkhLLgGL1j5FUZoJBJU9t9D"

def rpc_call(method, params):
    req = urllib.request.Request(
        RPC_URL,
        data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode(),
        headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode())

def bytes_to_pubkey(data):
    return base58.b58encode(data).decode()

print("Searching for Meteora CPMM pools...")
print()

# Get program accounts for Meteora CPMM
# Filter by data size (CPMM pools are ~397 bytes based on layout)
result = rpc_call("getProgramAccounts", [
    METEORA_CPMM_PROGRAM,
    {
        "encoding": "base64",
        "filters": [
            {"dataSize": 637}  # Try different sizes
        ]
    }
])

if result.get("error"):
    print(f"Error with size 637: {result['error']}")
    # Try another size
    result = rpc_call("getProgramAccounts", [
        METEORA_CPMM_PROGRAM,
        {
            "encoding": "base64",
            "dataSlice": {"offset": 0, "length": 0}  # Just get addresses
        }
    ])
    
if result.get("result"):
    pools = result["result"]
    print(f"Found {len(pools)} CPMM pools")
    
    # Find Delphi pool - check first few
    DELPHI_MINT = "BFuy9AJYKekZ2hik7b5mPhsunGscegi9vPY2bwzzBAGS"
    delphi_bytes = base58.b58decode(DELPHI_MINT)
    
    for pool in pools[:50]:  # Check first 50
        pubkey = pool["pubkey"]
        
        # Starts with AfEU?
        if pubkey.startswith("AfEU"):
            print(f"\n🎯 Found pool starting with AfEU: {pubkey}")
            
        # Get full pool data
        pool_info = rpc_call("getAccountInfo", [pubkey, {"encoding": "base64"}])
        if pool_info.get("result") and pool_info["result"]["value"]:
            data = base64.b64decode(pool_info["result"]["value"]["data"][0])
            
            # Check if Delphi mint is in this pool
            if delphi_bytes in data:
                print(f"\n🎯 FOUND DELPHI CPMM POOL: {pubkey}")
                print(f"   Data size: {len(data)} bytes")
                
                # Parse the pool
                offset = 8  # Skip discriminator
                
                amm_config = bytes_to_pubkey(data[offset:offset+32]); offset += 32
                pool_creator = bytes_to_pubkey(data[offset:offset+32]); offset += 32
                token_0_vault = bytes_to_pubkey(data[offset:offset+32]); offset += 32
                token_1_vault = bytes_to_pubkey(data[offset:offset+32]); offset += 32
                lp_mint = bytes_to_pubkey(data[offset:offset+32]); offset += 32
                token_0_mint = bytes_to_pubkey(data[offset:offset+32]); offset += 32
                token_1_mint = bytes_to_pubkey(data[offset:offset+32]); offset += 32
                token_0_program = bytes_to_pubkey(data[offset:offset+32]); offset += 32
                token_1_program = bytes_to_pubkey(data[offset:offset+32]); offset += 32
                
                print(f"   amm_config: {amm_config}")
                print(f"   token_0_mint: {token_0_mint}")
                print(f"   token_1_mint: {token_1_mint}")
                print(f"   token_0_vault: {token_0_vault}")
                print(f"   token_1_vault: {token_1_vault}")
                print(f"   token_0_program: {token_0_program}")
                print(f"   token_1_program: {token_1_program}")
                
                # Get vault balances
                v0 = rpc_call("getTokenAccountBalance", [token_0_vault])
                v1 = rpc_call("getTokenAccountBalance", [token_1_vault])
                
                if v0.get("result") and v0["result"]["value"]:
                    print(f"   token_0_vault balance: {v0['result']['value']['uiAmountString']}")
                if v1.get("result") and v1["result"]["value"]:
                    print(f"   token_1_vault balance: {v1['result']['value']['uiAmountString']}")
                
                break
else:
    print(f"Error: {result.get('error', 'Unknown')}")

# Also try to directly check if a known SOL/token pool exists
print("\n\nTrying to find any CPMM pool with SOL...")
SOL_MINT = "So11111111111111111111111111111111111111112"
sol_bytes = base58.b58decode(SOL_MINT)

# Get a few sample pools
sample_result = rpc_call("getProgramAccounts", [
    METEORA_CPMM_PROGRAM,
    {
        "encoding": "base64",
        "filters": [
            {"memcmp": {"offset": 200, "bytes": SOL_MINT}}  # token_1_mint = SOL
        ]
    }
])

if sample_result.get("result"):
    print(f"Found {len(sample_result['result'])} pools with SOL as token_1")
    if sample_result["result"]:
        pool = sample_result["result"][0]
        print(f"\nSample pool: {pool['pubkey']}")
        data = base64.b64decode(pool["account"]["data"][0])
        print(f"Data size: {len(data)} bytes")
