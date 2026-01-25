#!/usr/bin/env python3
"""Check transaction status."""
import urllib.request
import json
import sys

SIG = sys.argv[1] if len(sys.argv) > 1 else "qSqYHfjySxZ83ir5pgqPDc7UWhP3BYKJzhTWA5Uf5NidbJZduR5YTEw6L8tTNXACY291uJHmPnrE7E6RQTvwVQV"
RPC_URL = "https://mainnet.helius-rpc.com/?api-key=96755862-7b83-484a-9f7a-2c0620253cc1"

payload = {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "getTransaction",
    "params": [SIG, {"encoding": "json", "maxSupportedTransactionVersion": 0}]
}

req = urllib.request.Request(
    RPC_URL,
    data=json.dumps(payload).encode(),
    headers={"Content-Type": "application/json"}
)

try:
    resp = json.load(urllib.request.urlopen(req, timeout=10))
    result = resp.get("result")
    error = resp.get("error")
    
    if error:
        print(f"Error: {error}")
    elif result is None:
        print("Transaction NOT FOUND (not confirmed / dropped)")
    else:
        meta = result.get("meta", {})
        err = meta.get("err")
        slot = result.get("slot")
        if err:
            print(f"Transaction FAILED in slot {slot}: {err}")
        else:
            print(f"Transaction SUCCESS in slot {slot}")
            print(f"  Fee: {meta.get('fee')} lamports")
            print(f"  Compute units: {meta.get('computeUnitsConsumed')}")
except Exception as e:
    print(f"Error: {e}")
    sys.exit(1)
