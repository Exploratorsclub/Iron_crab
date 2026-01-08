#!/usr/bin/env python3
import requests

HELIUS_RPC = "https://mainnet.helius-rpc.com/?api-key=96755862-7b83-484a-9f7a-2c0620253cc1"
account = "5PHirr8joyTMp9JMm6nW7hNDVyEYdkzDqazxPD7RaTjx"

payload = {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "getAccountInfo",
    "params": [account, {"encoding": "json"}]
}

resp = requests.post(HELIUS_RPC, json=payload)
data = resp.json()

if "result" in data and data["result"]:
    owner = data["result"]["value"]["owner"]
    print(f"Account: {account}")
    print(f"Owner: {owner}")
else:
    print("Account not found or error")
