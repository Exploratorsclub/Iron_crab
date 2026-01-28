#!/usr/bin/env python3
"""Check pump.fun bonding curve creator."""
import urllib.request
import json
import sys
import base64

MINT = sys.argv[1] if len(sys.argv) > 1 else "HNMHAsH5YmjLgWooi8ALK3HLhpi2dawC3DTpJFNgpump"
RPC = sys.argv[2] if len(sys.argv) > 2 else "http://localhost:8899"

# Pump.fun bonding curve program
PUMP_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

# Get program accounts for the mint (bonding curve)
data = json.dumps({
    "jsonrpc": "2.0",
    "id": 1,
    "method": "getProgramAccounts",
    "params": [
        PUMP_PROGRAM,
        {
            "encoding": "base64",
            "filters": [
                {"memcmp": {"offset": 8, "bytes": MINT}}  # mint is at offset 8 in bonding curve
            ]
        }
    ]
}).encode()

req = urllib.request.Request(RPC, data=data, headers={"Content-Type": "application/json"})
resp = json.loads(urllib.request.urlopen(req, timeout=30).read())

accounts = resp.get("result", [])
if accounts:
    for acc in accounts:
        pubkey = acc["pubkey"]
        data_b64 = acc["account"]["data"][0]
        data = base64.b64decode(data_b64)
        
        # Bonding curve layout (simplified):
        # 0-8: discriminator
        # 8-40: mint (32 bytes)
        # 40-72: creator (32 bytes) - this is what we need
        
        if len(data) >= 72:
            creator_bytes = data[40:72]
            # Convert to base58
            import hashlib
            ALPHABET = b'123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'
            
            def b58encode(data):
                n = int.from_bytes(data, 'big')
                result = []
                while n > 0:
                    n, r = divmod(n, 58)
                    result.append(ALPHABET[r:r+1])
                # Add leading zeros
                for b in data:
                    if b == 0:
                        result.append(b'1')
                    else:
                        break
                return b''.join(reversed(result)).decode()
            
            creator = b58encode(creator_bytes)
            print(f"Bonding Curve: {pubkey}")
            print(f"Creator (from BC data): {creator}")
else:
    print(f"No bonding curve found for mint {MINT}")
