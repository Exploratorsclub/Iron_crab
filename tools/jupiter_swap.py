#!/usr/bin/env python3
"""
Jupiter API v6 Swap - Manual token sale
Uses Jupiter API with authentication
"""

import sys
import json
import base64
import requests
from pathlib import Path

# Jupiter API v6
JUPITER_API_KEY = "3c6bdf8a-d8c4-4e5b-afda-6922a836f793"
JUPITER_BASE = "https://api.jup.ag"

# Config
TOKEN_MINT = "9V7jznWgdN6tjMaJ6Bq11ZVQMkza6Zh45atgXbVmpump"
WSOL_MINT = "So11111111111111111111111111111111111111112"
AMOUNT = 1494544945  # Raw token amount
SLIPPAGE_BPS = 1000  # 10% slippage

def get_quote(input_mint, output_mint, amount, slippage_bps):
    """Get swap quote from Jupiter"""
    url = f"{JUPITER_BASE}/quote/v1/quote"
    
    params = {
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amount": str(amount),
        "slippageBps": str(slippage_bps),
    }
    
    headers = {
        "X-API-Key": JUPITER_API_KEY,
    }
    
    response = requests.get(url, params=params, headers=headers, timeout=30)
    
    if response.status_code != 200:
        raise Exception(f"Quote failed: {response.status_code} - {response.text}")
    
    return response.json()

def create_swap_transaction(quote, user_pubkey):
    """Create swap transaction from quote"""
    url = f"{JUPITER_BASE}/swap/v1/swap"
    
    headers = {
        "X-API-Key": JUPITER_API_KEY,
        "Content-Type": "application/json",
    }
    
    payload = {
        "quoteResponse": quote,
        "userPublicKey": user_pubkey,
        "wrapAndUnwrapSol": True,
        "computeUnitPriceMicroLamports": "auto",
    }
    
    print(f"📝 Creating swap transaction...")
    
    response = requests.post(url, json=payload, headers=headers, timeout=30)
    
    if response.status_code != 200:
        print(f"❌ Swap creation failed: {response.status_code}")
        print(response.text)
        sys.exit(1)
    
    return response.json()

def check_token_info(mint):
    """Check if token exists in Jupiter"""
    url = f"{JUPITER_BASE}/tokens/v1/{mint}"
    
    headers = {
        "X-API-Key": JUPITER_API_KEY,
    }
    
    print(f"🔍 Checking token info...")
    response = requests.get(url, headers=headers, timeout=30)
    
    if response.status_code == 200:
        data = response.json()
        print(f"✅ Token found: {data.get('symbol', 'Unknown')}")
        print(f"   Decimals: {data.get('decimals')}")
        print()
        return data
    else:
        print(f"⚠️  Token not in Jupiter registry (status: {response.status_code})")
        print()
        return None

def main():
    print("🪙 Jupiter Swap Tool")
    print(f"   Token: {TOKEN_MINT}")
    print(f"   Amount: {AMOUNT} raw units")
    print(f"   Slippage: {SLIPPAGE_BPS / 100}%")
    print()
    
    # Check token info first
    token_info = check_token_info(TOKEN_MINT)
    
    # Try different output mints
    output_mints = [
        ("WSOL", "So11111111111111111111111111111111111111112"),
        ("USDC", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"),
        ("USDT", "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"),
    ]
    
    for name, mint in output_mints:
        print(f"🔄 Trying {name}...")
        try:
            quote = get_quote(TOKEN_MINT, mint, AMOUNT, SLIPPAGE_BPS)
            
            # If we got here, quote succeeded
            in_amount = int(quote["inAmount"])
            out_amount = int(quote["outAmount"])
            
            route_info = quote.get("routePlan", [])
            route_labels = [step["swapInfo"]["label"] for step in route_info]
            route_str = " → ".join(route_labels) if route_labels else "Direct"
            
            print(f"✅ Route found via {name}!")
            print(f"   Route: {route_str}")
            print(f"   Output: {out_amount} {name} units")
            print()
            return
        except Exception as e:
            print(f"❌ No route via {name}")
            print()
    
    print("❌ No routes found on Jupiter for any output token")
    print()
    print("💡 This token is likely only on Pump.fun/PumpSwap")
    print("   Jupiter doesn't support Pump.fun pools yet")
    print()
    print("   Options:")
    print("   1. Export key to Phantom → swap on pump.fun UI")
    print("   2. Wait for Jupiter integration")
    print("   3. Use direct RPC to build Pump.fun swap (complex)")

if __name__ == "__main__":
    main()
