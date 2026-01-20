#!/usr/bin/env python3
"""Analyze Meteora CPMM (DAMM V2) pool layout."""
import json
import urllib.request
import base64
import struct
from base58 import b58decode, b58encode

RPC_URL = "http://127.0.0.1:8899"

# Meteora CPMM Program ID (DAMM V2)
METEORA_CPMM_PROGRAM = "cpmmpPFsKiR4eeYnGSuXgkhLLgGL1j5FUZoJBJU9t9D"
DELPHI_MINT = "BFuy9AJYKekZ2hik7b5mPhsunGscegi9vPY2bwzzBAGS"
SOL_MINT = "So11111111111111111111111111111111111111112"

def rpc_call(method, params):
    req = urllib.request.Request(
        RPC_URL,
        data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode(),
        headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode())

def pubkey_at(data, offset):
    """Extract 32-byte pubkey at offset and encode as base58."""
    return b58encode(data[offset:offset+32]).decode()

print("=" * 70)
print("Meteora CPMM (DAMM V2) Pool Analysis")
print("=" * 70)
print()

# Check program exists
print(f"Program: {METEORA_CPMM_PROGRAM}")
result = rpc_call("getAccountInfo", [METEORA_CPMM_PROGRAM, {"encoding": "base64"}])
if result.get("result") and result["result"]["value"]:
    print(f"  ✅ Program exists, executable={result['result']['value'].get('executable')}")
else:
    print("  ❌ Program not found")

print()
print("Searching for Delphi/SOL CPMM pool...")

# The pool address from dextools screenshot was AfEUx...NWCZ
# Let's try to derive or find it

# Try searching for pools by checking known pool patterns
# CPMM pools are PDAs derived from the mints

# Method 1: Try to find via getTokenAccountsByOwner for the mint
# Check if there's a token account for Delphi owned by a CPMM pool

# Let's check the screenshot pool address pattern
# "AfEUx...NWCZ" - we need the full address

# Let's look at what accounts the Delphi mint has
print(f"\nSearching for token accounts of Delphi mint...")
result = rpc_call("getTokenLargestAccounts", [DELPHI_MINT])
if result.get("result") and result["result"]["value"]:
    print(f"Found {len(result['result']['value'])} token accounts")
    for acc in result["result"]["value"][:10]:  # Top 10
        addr = acc["address"]
        amount = acc["uiAmountString"]
        
        # Check if this account's owner is a CPMM pool
        acc_info = rpc_call("getAccountInfo", [addr, {"encoding": "jsonParsed"}])
        if acc_info.get("result") and acc_info["result"]["value"]:
            owner = acc_info["result"]["value"]["owner"]
            parsed = acc_info["result"]["value"].get("data", {})
            if isinstance(parsed, dict) and "parsed" in parsed:
                token_owner = parsed["parsed"]["info"].get("owner", "?")
                
                # Check if token_owner is a CPMM pool
                pool_info = rpc_call("getAccountInfo", [token_owner, {"encoding": "base64"}])
                if pool_info.get("result") and pool_info["result"]["value"]:
                    pool_owner = pool_info["result"]["value"]["owner"]
                    if pool_owner == METEORA_CPMM_PROGRAM:
                        print(f"\n🎯 FOUND CPMM POOL!")
                        print(f"   Pool: {token_owner}")
                        print(f"   Token Account: {addr}")
                        print(f"   Amount: {amount}")
                        
                        # Analyze pool layout
                        pool_data = base64.b64decode(pool_info["result"]["value"]["data"][0])
                        print(f"   Pool data size: {len(pool_data)} bytes")
                        
                        # Try to parse pool layout
                        # Typical CPMM layout (from Meteora SDK):
                        # - discriminator: 8 bytes
                        # - amm_config: 32 bytes (pubkey)
                        # - pool_creator: 32 bytes (pubkey)  
                        # - token_0_vault: 32 bytes (pubkey)
                        # - token_1_vault: 32 bytes (pubkey)
                        # - lp_mint: 32 bytes (pubkey)
                        # - token_0_mint: 32 bytes (pubkey)
                        # - token_1_mint: 32 bytes (pubkey)
                        # - token_0_program: 32 bytes (pubkey)
                        # - token_1_program: 32 bytes (pubkey)
                        # ... more fields
                        
                        if len(pool_data) >= 296:  # Minimum expected size
                            print("\n   Pool Layout:")
                            print(f"   [8] discriminator: {pool_data[:8].hex()}")
                            print(f"   [8:40] amm_config: {pubkey_at(pool_data, 8)}")
                            print(f"   [40:72] pool_creator: {pubkey_at(pool_data, 40)}")
                            print(f"   [72:104] token_0_vault: {pubkey_at(pool_data, 72)}")
                            print(f"   [104:136] token_1_vault: {pubkey_at(pool_data, 104)}")
                            print(f"   [136:168] lp_mint: {pubkey_at(pool_data, 136)}")
                            print(f"   [168:200] token_0_mint: {pubkey_at(pool_data, 168)}")
                            print(f"   [200:232] token_1_mint: {pubkey_at(pool_data, 200)}")
                            print(f"   [232:264] token_0_program: {pubkey_at(pool_data, 232)}")
                            print(f"   [264:296] token_1_program: {pubkey_at(pool_data, 264)}")
                        
                        break
                else:
                    if addr.startswith("AfEU"):
                        print(f"   {addr}: {amount} (owner: {token_owner})")

print()
print("=" * 70)
