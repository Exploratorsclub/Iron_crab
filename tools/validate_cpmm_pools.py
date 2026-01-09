#!/usr/bin/env python3
"""
Raydium CPMM Pool Validator
Fetches real CPMM pools from mainnet and validates parser offsets
"""

import base64
import json
import sys
from typing import Optional
import requests

# Helius RPC (or use your own)
RPC_URL = "https://mainnet.helius-rpc.com/?api-key=96755862-7b83-484a-9f7a-2c0620253cc1"

RAYDIUM_CPMM_PROGRAM = "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C"

def rpc_request(method: str, params: list) -> dict:
    """Make RPC request"""
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params
    }
    
    response = requests.post(RPC_URL, json=payload, timeout=30)
    response.raise_for_status()
    
    result = response.json()
    if "error" in result:
        raise Exception(f"RPC Error: {result['error']}")
    
    return result["result"]

def get_cpmm_pools(limit: int = 10) -> list:
    """Fetch CPMM pools via getProgramAccounts"""
    print(f"Fetching CPMM pools from {RAYDIUM_CPMM_PROGRAM}...")
    
    # Try different account sizes to find correct one
    for size in [752, 800, 1024, 512, 256]:
        print(f"  Trying account size: {size} bytes...")
        
        try:
            result = rpc_request("getProgramAccounts", [
                RAYDIUM_CPMM_PROGRAM,
                {
                    "encoding": "base64",
                    "filters": [
                        {"dataSize": size}
                    ]
                }
            ])
            
            if result:
                print(f"  ✅ Found {len(result)} pools with size {size}")
                return result[:limit]
        except Exception as e:
            print(f"  ❌ Size {size}: {e}")
            continue
    
    # If no exact size works, try without size filter
    print("  Trying without size filter (may be slow)...")
    result = rpc_request("getProgramAccounts", [
        RAYDIUM_CPMM_PROGRAM,
        {
            "encoding": "base64",
            "dataSlice": {"offset": 0, "length": 1024}  # First 1KB only
        }
    ])
    
    print(f"  ✅ Found {len(result)} pools (no size filter)")
    return result[:limit]

def parse_pubkey(data: bytes, offset: int) -> str:
    """Parse 32-byte Pubkey at offset"""
    pubkey_bytes = data[offset:offset+32]
    # Convert to base58 (simplified - just show hex for now)
    return pubkey_bytes.hex()

def analyze_pool(pubkey: str, account_data: str) -> dict:
    """Analyze CPMM pool account structure"""
    data = base64.b64decode(account_data)
    
    print(f"\n{'='*80}")
    print(f"Pool: {pubkey}")
    print(f"Account Size: {len(data)} bytes")
    print(f"{'='*80}")
    
    # First 200 bytes (header)
    print("\nFirst 200 bytes (hex):")
    for i in range(0, min(200, len(data)), 32):
        chunk = data[i:i+32]
        hex_str = chunk.hex()
        ascii_str = ''.join(chr(b) if 32 <= b < 127 else '.' for b in chunk)
        print(f"  {i:04d}: {hex_str}  {ascii_str}")
    
    analysis = {
        "pubkey": pubkey,
        "size": len(data),
        "discriminator": data[0:8].hex() if len(data) >= 8 else None,
    }
    
    # Try to parse key fields
    if len(data) >= 200:
        print("\nAttempting to parse structure:")
        
        # Anchor discriminator (8 bytes)
        discriminator = data[0:8]
        print(f"  Discriminator: {discriminator.hex()}")
        analysis["discriminator"] = discriminator.hex()
        
        # Status (byte 8)
        if len(data) > 8:
            status = data[8]
            print(f"  Status (offset 8): {status}")
            analysis["status"] = status
        
        # Try parsing potential Pubkeys at various offsets
        print("\n  Potential Pubkeys:")
        for offset in [16, 48, 80, 112, 144, 176]:
            if len(data) >= offset + 32:
                pubkey_hex = parse_pubkey(data, offset)
                # Check if looks like valid pubkey (not all zeros)
                is_valid = not all(b == 0 for b in data[offset:offset+32])
                marker = "✅" if is_valid else "❌"
                print(f"    Offset {offset:3d}: {pubkey_hex[:16]}... {marker}")
                analysis[f"pubkey_offset_{offset}"] = pubkey_hex
        
        # Try parsing u64 values (potential fee rates, timestamps)
        print("\n  Potential u64 values:")
        for offset in [176, 184, 192, 200, 208]:
            if len(data) >= offset + 8:
                value = int.from_bytes(data[offset:offset+8], 'little')
                print(f"    Offset {offset:3d}: {value:20d} (0x{value:016x})")
                analysis[f"u64_offset_{offset}"] = value
    
    return analysis

def main():
    print("Raydium CPMM Pool Validator")
    print("="*80)
    
    # Fetch pools
    pools = get_cpmm_pools(limit=5)
    
    if not pools:
        print("❌ No CPMM pools found!")
        sys.exit(1)
    
    print(f"\n✅ Found {len(pools)} CPMM pools")
    
    # Analyze each pool
    analyses = []
    for entry in pools:
        pubkey = entry["pubkey"]
        account_data = entry["account"]["data"][0]  # base64 string
        
        analysis = analyze_pool(pubkey, account_data)
        analyses.append(analysis)
    
    # Save to JSON
    output_file = "cpmm_pools_analysis.json"
    with open(output_file, 'w') as f:
        json.dump(analyses, f, indent=2)
    
    print(f"\n{'='*80}")
    print(f"✅ Analysis saved to {output_file}")
    print(f"{'='*80}")
    
    # Summary
    print("\nSummary:")
    print(f"  Pools analyzed: {len(analyses)}")
    if analyses:
        sizes = set(a["size"] for a in analyses)
        print(f"  Account sizes: {sorted(sizes)}")
        print(f"  Discriminators: {set(a.get('discriminator') for a in analyses)}")
    
    print("\nNext Steps:")
    print("  1. Review cpmm_pools_analysis.json")
    print("  2. Verify Pubkey offsets match Rust parser")
    print("  3. Update CPMM_POOL_ACCOUNT_SIZE if needed")
    print("  4. Test quote_exact_in with real reserves")

if __name__ == "__main__":
    main()
