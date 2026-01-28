#!/usr/bin/env python3
"""Check pump.fun bonding curve data."""
import urllib.request
import json
import sys
import base64

ACCOUNT = sys.argv[1] if len(sys.argv) > 1 else "56JpsjDnf9vZ5f2KGRZSyRCH2U2uChdpF2DwDA785bmW"
RPC = sys.argv[2] if len(sys.argv) > 2 else "http://localhost:8899"

ALPHABET = b'123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'

def b58encode(data):
    n = int.from_bytes(data, 'big')
    result = []
    while n > 0:
        n, r = divmod(n, 58)
        result.append(ALPHABET[r:r+1])
    for b in data:
        if b == 0:
            result.append(b'1')
        else:
            break
    return b''.join(reversed(result)).decode()

data = json.dumps({
    "jsonrpc": "2.0",
    "id": 1,
    "method": "getAccountInfo",
    "params": [ACCOUNT, {"encoding": "base64"}]
}).encode()

req = urllib.request.Request(RPC, data=data, headers={"Content-Type": "application/json"})
resp = json.loads(urllib.request.urlopen(req, timeout=10).read())

val = resp.get("result", {}).get("value")
if val:
    owner = val.get("owner")
    print(f"Account: {ACCOUNT}")
    print(f"Owner: {owner}")
    
    # Decode base64 data
    if val.get("data"):
        raw = base64.b64decode(val["data"][0])
        print(f"Data length: {len(raw)} bytes")
        
        # If owned by pump.fun program, parse bonding curve
        if owner == "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P" and len(raw) >= 81:
            # Bonding curve layout:
            # 0-8: discriminator (8 bytes)
            # 8-16: virtual_token_reserves (u64)
            # 16-24: virtual_sol_reserves (u64)
            # 24-32: real_token_reserves (u64)
            # 32-40: real_sol_reserves (u64)
            # 40-48: token_total_supply (u64)
            # 48: complete (bool)
            # 49-81: creator (32 bytes)
            import struct
            vtr, vsr, rtr, rsr, tts = struct.unpack('<QQQQQ', raw[8:48])
            complete = raw[48] != 0
            creator = b58encode(raw[49:81])
            print(f"Virtual Token Reserves: {vtr}")
            print(f"Virtual SOL Reserves: {vsr / 1e9:.4f} SOL")
            print(f"Real Token Reserves: {rtr}")
            print(f"Real SOL Reserves: {rsr / 1e9:.4f} SOL")
            print(f"Complete: {complete}")
            print(f"Creator: {creator}")
else:
    print(f"Account {ACCOUNT} does NOT exist")
