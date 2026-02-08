#!/usr/bin/env python3
"""Check wallet token accounts and ATA derivation vs actual addresses."""
import json, urllib.request, hashlib, base64

wallet = "Ase7z1mRLps2cTNQnRHpLyQL4Q5FHwonjmZnYCTuUDZM"
rpc = "http://127.0.0.1:8899"

# Check Token-2022 accounts
for prog_name, prog_id in [("SPL Token", "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"), ("Token-2022", "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")]:
    data = json.dumps({
        "jsonrpc": "2.0", "id": 1,
        "method": "getTokenAccountsByOwner",
        "params": [wallet, {"programId": prog_id}, {"encoding": "jsonParsed"}]
    }).encode()
    req = urllib.request.Request(rpc, data=data, headers={"Content-Type": "application/json"})
    resp = json.load(urllib.request.urlopen(req))
    for a in resp["result"]["value"]:
        info = a["account"]["data"]["parsed"]["info"]
        bal = int(info["tokenAmount"]["amount"])
        if bal > 0:
            print(f"[{prog_name}] mint={info['mint']} balance_raw={bal} ata={a['pubkey']}")

# Now check account data lengths for the Token-2022 ATAs
print("\n--- Account data length check ---")
atas = ["74G7BsTRNeQpwGiPES9ffZXs595CZjn1eSoP1q3bwzE6", "BxL8hA8xfTbnYjzLiXsYw4bJTtioTeWyaCNQM1kkbMRS"]
data = json.dumps({
    "jsonrpc": "2.0", "id": 2,
    "method": "getMultipleAccounts",
    "params": [atas, {"encoding": "base64"}]
}).encode()
req = urllib.request.Request(rpc, data=data, headers={"Content-Type": "application/json"})
resp = json.load(urllib.request.urlopen(req))
for i, acc in enumerate(resp["result"]["value"]):
    if acc:
        raw = base64.b64decode(acc["data"][0])
        print(f"ATA={atas[i]} owner={acc['owner']} data_len={len(raw)} (Pack::unpack expects 165)")
    else:
        print(f"ATA={atas[i]} NOT FOUND")
