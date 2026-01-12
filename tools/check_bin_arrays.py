#!/usr/bin/env python3
"""Check bin arrays for a Meteora DLMM pool."""

import requests
import json
import sys
import base64

def main():
    lb_pair = sys.argv[1] if len(sys.argv) > 1 else "HNnd69zLdEVST1RMYyrwXkeKfUcQkNcLdUkU6wzsdwjL"
    rpc_url = "https://mainnet.helius-rpc.com/?api-key=96755862-7b83-484a-9f7a-2c0620253cc1"

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getProgramAccounts",
        "params": [
            "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo",
            {
                "encoding": "base64",
                "dataSlice": {"offset": 0, "length": 56},
                "filters": [{"memcmp": {"offset": 24, "bytes": lb_pair}}]
            }
        ]
    }

    resp = requests.post(rpc_url, json=payload)
    data = resp.json()

    if "result" not in data:
        print(f"Error: {data}")
        sys.exit(1)

    print(f"Found {len(data['result'])} bin arrays for {lb_pair}")
    for acc in data["result"]:
        raw = base64.b64decode(acc["account"]["data"][0])
        index = int.from_bytes(raw[8:16], "little", signed=True)
        print(f"  {acc['pubkey']}: index={index}")

if __name__ == "__main__":
    main()
