#!/usr/bin/env python3
"""Check the owner (token program) of a Solana mint account."""

import json
import sys
import urllib.request

def check_mint(mint_address: str) -> None:
    """Query Solana mainnet for mint account info."""
    req = urllib.request.Request(
        "https://api.mainnet-beta.solana.com",
        data=json.dumps({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getAccountInfo",
            "params": [mint_address, {"encoding": "jsonParsed"}]
        }).encode(),
        headers={"Content-Type": "application/json"}
    )
    
    with urllib.request.urlopen(req, timeout=10) as resp:
        data = json.loads(resp.read())
    
    value = data.get("result", {}).get("value")
    
    print(f"Mint: {mint_address}")
    print(f"Account exists: {value is not None}")
    
    if value:
        owner = value.get("owner", "N/A")
        print(f"Owner (Token Program): {owner}")
        
        # Identify the program
        if owner == "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA":
            print("Program: SPL Token (standard)")
        elif owner == "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb":
            print("Program: Token-2022 (extension)")
        else:
            print(f"Program: Unknown ({owner})")
    else:
        print("Owner: N/A (account does not exist)")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python check_mint_owner.py <mint_address>")
        sys.exit(1)
    
    check_mint(sys.argv[1])
