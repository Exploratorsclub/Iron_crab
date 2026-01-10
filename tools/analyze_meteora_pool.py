#!/usr/bin/env python3
"""Analyze Meteora DLMM Pool Account Data"""

import json
import base64
import sys

def analyze_pool(pool_data):
    """Analyze a single DLMM pool account"""
    pubkey = pool_data['pubkey']
    data_b64 = pool_data['account']['data'][0]
    data = base64.b64decode(data_b64)
    
    print(f"\n=== Meteora DLMM Pool Analysis ===")
    print(f"Pubkey: {pubkey}")
    print(f"Data Size: {len(data)} bytes")
    print(f"Owner: {pool_data['account']['owner']}")
    print(f"Lamports: {pool_data['account']['lamports']}")
    print(f"\n--- First 256 bytes (hex) ---")
    print(' '.join(f'{b:02x}' for b in data[:256]))
    
    print(f"\n--- Attempting to parse fields ---")
    
    # Try to find pubkeys (32 bytes each)
    print("\n• Potential Pubkeys:")
    for i in range(0, min(len(data), 500), 32):
        chunk = data[i:i+32]
        if len(chunk) == 32:
            # Check if looks like a pubkey (non-zero, not all same)
            if chunk != b'\x00' * 32 and len(set(chunk)) > 5:
                print(f"  Offset {i:3d}: {base64.b58encode(chunk).decode()}")
    
    # Try to find u64 values (common for amounts, fees, etc)
    print("\n• Potential u64 values:")
    for i in range(0, min(len(data), 200), 8):
        val = int.from_bytes(data[i:i+8], 'little')
        if 0 < val < 2**63:  # Reasonable range
            print(f"  Offset {i:3d}: {val:20,d} (0x{val:016x})")
    
    # Try to find u16/u32 values
    print("\n• Potential u16 values (bin_step, fees in bps):")
    for i in range(0, min(len(data), 100), 2):
        val = int.from_bytes(data[i:i+2], 'little')
        if 1 <= val <= 10000:  # Reasonable for bps
            print(f"  Offset {i:3d}: {val:5d} ({val/100:.2f}%)")

if __name__ == "__main__":
    # Read from stdin or file
    if len(sys.argv) > 1:
        with open(sys.argv[1], 'r') as f:
            response = json.load(f)
    else:
        response = json.load(sys.stdin)
    
    if 'result' in response and isinstance(response['result'], list):
        for pool in response['result'][:3]:  # First 3 pools
            analyze_pool(pool)
            print("\n" + "="*60 + "\n")
    else:
        print("No pools found in response")
