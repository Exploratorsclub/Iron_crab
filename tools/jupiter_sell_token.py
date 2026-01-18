#!/usr/bin/env python3
"""
Direct Jupiter swap - bypasses NATS entirely.

Finds best route across ALL DEXes and executes via Jupiter API.
"""

import sys
import json
import base64
import requests
from pathlib import Path

# Jupiter API V6
JUPITER_API = "https://quote-api.jup.ag/v6"


def get_best_route(input_mint: str, output_mint: str, amount: int, slippage_bps: int = 1000):
    """Get best swap route from Jupiter."""
    url = f"{JUPITER_API}/quote"
    params = {
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amount": amount,
        "slippageBps": slippage_bps,
        "onlyDirectRoutes": False,  # Allow multi-hop for best price
        "asLegacyTransaction": False,
    }
    
    print(f"🔍 Finding best route via Jupiter API...")
    print(f"   Input: {input_mint}")
    print(f"   Output: {output_mint}")
    print(f"   Amount: {amount}")
    
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    quote = resp.json()
    
    # Print route info
    in_amount = int(quote["inAmount"])
    out_amount = int(quote["outAmount"])
    price_impact_pct = float(quote.get("priceImpactPct", 0))
    
    route_plan = quote.get("routePlan", [])
    dexes_used = []
    for step in route_plan:
        swap_info = step.get("swapInfo", {})
        label = swap_info.get("label", "unknown")
        dexes_used.append(label)
    
    print(f"\n✅ Best route found:")
    print(f"   Route: {' → '.join(dexes_used)}")
    print(f"   Input: {in_amount / 1e6:.6f} tokens")
    print(f"   Output: {out_amount / 1e9:.8f} SOL")
    print(f"   Price Impact: {price_impact_pct:.2f}%")
    
    return quote


def create_swap_transaction(quote: dict, user_pubkey: str):
    """Create swap transaction from quote."""
    url = f"{JUPITER_API}/swap"
    
    payload = {
        "quoteResponse": quote,
        "userPublicKey": user_pubkey,
        "wrapAndUnwrapSol": True,
        "dynamicComputeUnitLimit": True,
        "prioritizationFeeLamports": "auto",  # Let Jupiter set priority fee
    }
    
    print(f"\n📝 Creating swap transaction...")
    
    resp = requests.post(url, json=payload, timeout=30)
    resp.raise_for_status()
    result = resp.json()
    
    swap_transaction = result["swapTransaction"]
    print(f"✅ Transaction created (base64 encoded)")
    
    return swap_transaction


def main():
    if len(sys.argv) < 2:
        print("Usage: python jupiter_sell_token.py <TOKEN_MINT> [AMOUNT]")
        print("\nExample:")
        print("  python jupiter_sell_token.py 9V7jznWgdN6tjMaJ6Bq11ZVQMkza6Zh45atgXbVmpump 1494544945")
        sys.exit(1)
    
    token_mint = sys.argv[1]
    
    # Get amount from args or try to detect from wallet
    if len(sys.argv) >= 3:
        amount = int(sys.argv[2])
    else:
        print("ERROR: Amount required")
        sys.exit(1)
    
    # SOL output
    output_mint = "So11111111111111111111111111111111111111112"
    
    # Get wallet pubkey from config or env
    config_path = Path(__file__).parent.parent / "my_config.server.toml"
    
    # Try to extract wallet from config
    wallet_pubkey = None
    if config_path.exists():
        with open(config_path) as f:
            for line in f:
                if "wallet_pubkey" in line:
                    parts = line.split("=")
                    if len(parts) == 2:
                        wallet_pubkey = parts[1].strip().strip('"\'')
                        break
    
    if not wallet_pubkey:
        print("ERROR: Could not find wallet_pubkey in config")
        print("Please provide wallet pubkey as environment variable:")
        print("  export WALLET_PUBKEY=<your_wallet_pubkey>")
        sys.exit(1)
    
    print(f"🪙 Wallet: {wallet_pubkey}\n")
    
    # Get best quote
    try:
        quote = get_best_route(
            input_mint=token_mint,
            output_mint=output_mint,
            amount=amount,
            slippage_bps=1000,  # 10% slippage for emergency exit
        )
    except Exception as e:
        print(f"❌ ERROR getting quote: {e}")
        sys.exit(1)
    
    # Create transaction
    try:
        swap_tx_base64 = create_swap_transaction(quote, wallet_pubkey)
    except Exception as e:
        print(f"❌ ERROR creating transaction: {e}")
        sys.exit(1)
    
    # Save to file for signing
    output_file = Path(__file__).parent.parent / "swap_tx_unsigned.txt"
    with open(output_file, 'w') as f:
        f.write(swap_tx_base64)
    
    print(f"\n💾 Unsigned transaction saved to: {output_file}")
    print(f"\n⚠️  NEXT STEPS:")
    print(f"   1. Sign and send this transaction using solana CLI:")
    print(f"      cat {output_file} | base64 -d > swap_tx.bin")
    print(f"      solana sign swap_tx.bin --keypair <path_to_keypair>")
    print(f"\n   OR use this script with --auto-sign (DANGEROUS - requires keypair access)")


if __name__ == "__main__":
    main()
