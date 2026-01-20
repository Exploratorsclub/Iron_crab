#!/usr/bin/env python3
"""Check Meteora DLMM pool details and bin arrays."""
import json
import urllib.request
import base64
import struct

RPC_URL = "http://127.0.0.1:8899"

def rpc_call(method, params):
    req = urllib.request.Request(
        RPC_URL,
        data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode(),
        headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode())

# DLMM Pool that failed
POOL = "BL4UVxA3fK5euJWzb6RfiKdKbEqWVBmcFMpG9YM1dU3X"
DLMM_PROGRAM = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo"

print(f"Checking DLMM Pool: {POOL}")
print()

result = rpc_call("getAccountInfo", [POOL, {"encoding": "base64"}])
if result.get("result") and result["result"]["value"]:
    owner = result["result"]["value"]["owner"]
    data = base64.b64decode(result["result"]["value"]["data"][0])
    lamports = result["result"]["value"]["lamports"]
    
    print(f"Owner: {owner}")
    print(f"Lamports: {lamports}")
    print(f"Data size: {len(data)} bytes")
    
    if owner == DLMM_PROGRAM:
        print("Type: Meteora DLMM (bin-based)")
        
        # Parse LB Pair structure (simplified)
        # Layout reference: https://github.com/MeteoraAg/dlmm-sdk
        # Discriminator: 8 bytes
        # parameters: ParameterState
        # active_id: i32 at some offset
        # bin_step: u16
        
        if len(data) >= 904:  # LB Pair account size
            # Try to find key fields
            # bin_step is usually early in the structure
            bin_step = struct.unpack_from("<H", data, 64)[0]  # Approximate offset
            active_id = struct.unpack_from("<i", data, 72)[0]  # Approximate offset
            
            print(f"bin_step (approx): {bin_step}")
            print(f"active_id (approx): {active_id}")
            
            # Token mints
            token_x_mint = base64.b64encode(data[128:160]).decode()  # Approximate
            token_y_mint = base64.b64encode(data[160:192]).decode()  # Approximate
            
            # Reserve vaults
            reserve_x = base64.b64encode(data[192:224]).decode()  # Approximate
            reserve_y = base64.b64encode(data[224:256]).decode()  # Approximate
            
            print()
            print("Checking bin_array accounts...")
            
            # Derive bin_array PDAs
            # bin_array is derived from: [lb_pair, "bin_array", bin_array_index]
            import hashlib
            
            # For active_id, we need to find the bin_array_index
            # bin_array_index = active_id / bins_per_array (usually 70)
            bins_per_array = 70
            bin_array_idx = active_id // bins_per_array
            
            print(f"Expected bin_array_index for active_id {active_id}: {bin_array_idx}")
            
            # Check a few bin arrays around the active area
            for idx in range(bin_array_idx - 2, bin_array_idx + 3):
                # PDA derivation would require the actual seeds
                # For now, let's just note what we're looking for
                print(f"  Would check bin_array index {idx}")
else:
    print("Pool not found!")

print()
print("=" * 60)
print("The issue: bin_arrays are derived PDAs that need to exist")
print("If no liquidity was ever added at those price bins, the")
print("bin_array accounts don't exist on-chain.")
print()
print("Solution options:")
print("1. Add Meteora CPMM/DAMM V2 support (different pool type)")
print("2. Use Raydium if available") 
print("3. Use Jupiter aggregator API for routing")
