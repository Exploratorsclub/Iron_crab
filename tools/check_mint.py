#!/usr/bin/env python3
"""Check mint account info on local RPC."""
import urllib.request
import json
import sys

MINT = sys.argv[1] if len(sys.argv) > 1 else "9aLfKS3ovf3UVa9yJNPbjkxRLQH7DYS2xgTZh3fgpump"
RPC = sys.argv[2] if len(sys.argv) > 2 else "http://localhost:8899"

data = json.dumps({
    "jsonrpc": "2.0",
    "id": 1,
    "method": "getAccountInfo",
    "params": [MINT, {"encoding": "jsonParsed"}]
}).encode()

req = urllib.request.Request(RPC, data=data, headers={"Content-Type": "application/json"})
resp = json.loads(urllib.request.urlopen(req, timeout=10).read())

val = resp.get("result", {}).get("value")
if val:
    print(f"Mint: {MINT}")
    print(f"Owner: {val.get('owner')}")
    if val.get("data", {}).get("parsed"):
        parsed = val["data"]["parsed"]
        print(f"Type: {parsed.get('type')}")
        if parsed.get("info"):
            info = parsed["info"]
            print(f"Decimals: {info.get('decimals')}")
            print(f"Supply: {info.get('supply')}")
            print(f"MintAuthority: {info.get('mintAuthority')}")
else:
    print(f"Mint {MINT} does NOT exist (value=null)")
