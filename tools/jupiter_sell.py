#!/usr/bin/env python3
"""
Quick sell via Jupiter - finds best price across ALL DEXes
No API key needed - Jupiter API is public
"""

import sys
import json
import subprocess
import base64
import requests
from pathlib import Path

JUPITER_API = "https://quote-api.jup.ag/v6"
WSOL = "So11111111111111111111111111111111111111112"

def get_wallet_pubkey(keypair_path):
    """Get wallet public key from keypair file"""
    result = subprocess.run(
        [f"{Path.home()}/.local/share/solana/install/active_release/bin/solana-keygen", 
         "pubkey", keypair_path],
        capture_output=True, text=True, check=True
    )
    return result.stdout.strip()

def get_quote(input_mint, output_mint, amount, slippage_bps=1000):
    """Get best swap quote from Jupiter"""
    url = f"{JUPITER_API}/quote"
    params = {
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amount": str(amount),
        "slippageBps": str(slippage_bps),
        "onlyDirectRoutes": "false",
    }
    
    print(f"📡 Getting best quote from Jupiter...")
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    
    quote = response.json()
    
    if "error" in quote:
        raise Exception(f"Jupiter API error: {quote['error']}")
    
    return quote

def create_swap_tx(quote, wallet_pubkey):
    """Create swap transaction from quote"""
    url = f"{JUPITER_API}/swap"
    
    payload = {
        "quoteResponse": quote,
        "userPublicKey": wallet_pubkey,
        "wrapAndUnwrapSol": True,
        "dynamicComputeUnitLimit": True,
        "prioritizationFeeLamports": "auto"
    }
    
    print(f"📝 Creating swap transaction...")
    response = requests.post(url, json=payload, timeout=30)
    response.raise_for_status()
    
    swap_data = response.json()
    
    if "swapTransaction" not in swap_data:
        raise Exception(f"No swap transaction in response: {swap_data}")
    
    return swap_data["swapTransaction"]

def sign_and_send_tx(tx_base64, keypair_path, rpc_url):
    """Sign and send transaction using Solana CLI"""
    solana_bin = f"{Path.home()}/.local/share/solana/install/active_release/bin/solana"
    
    # Decode and save
    tx_bytes = base64.b64decode(tx_base64)
    Path("/tmp/swap_tx.bin").write_bytes(tx_bytes)
    
    # Sign
    print(f"✍️  Signing transaction...")
    subprocess.run(
        [solana_bin, "sign", "/tmp/swap_tx.bin", 
         "--keypair", keypair_path, "-o", "/tmp/swap_tx_signed.bin"],
        check=True, capture_output=True
    )
    
    # Send
    print(f"📤 Sending transaction...")
    result = subprocess.run(
        [solana_bin, "send-transaction", "/tmp/swap_tx_signed.bin",
         "--url", rpc_url, "--commitment", "confirmed"],
        capture_output=True, text=True, check=True
    )
    
    # Extract signature from output
    signature = result.stdout.strip()
    
    # Cleanup
    Path("/tmp/swap_tx.bin").unlink(missing_ok=True)
    Path("/tmp/swap_tx_signed.bin").unlink(missing_ok=True)
    
    return signature

def main():
    if len(sys.argv) < 3:
        print("Usage: python3 jupiter_sell.py <TOKEN_MINT> <AMOUNT_RAW> [SLIPPAGE_BPS]")
        print("")
        print("Example:")
        print("  python3 jupiter_sell.py 9V7jznWgdN6tjMaJ6Bq11ZVQMkza6Zh45atgXbVmpump 1494544945")
        print("  python3 jupiter_sell.py <MINT> <AMOUNT> 2000  # 20% slippage")
        sys.exit(1)
    
    token_mint = sys.argv[1]
    amount = int(sys.argv[2])
    slippage_bps = int(sys.argv[3]) if len(sys.argv) > 3 else 1000  # Default 10%
    
    # Config
    keypair_path = Path.home() / ".config/solana/id.json"
    rpc_url = "https://mainnet.helius-rpc.com/?api-key=96755862-7b83-484a-9f7a-2c0620253cc1"
    
    if not keypair_path.exists():
        print(f"❌ ERROR: Keypair not found at {keypair_path}")
        sys.exit(1)
    
    # Get wallet
    wallet_pubkey = get_wallet_pubkey(str(keypair_path))
    print(f"🪙 Wallet: {wallet_pubkey}")
    print(f"🎯 Selling: {amount} tokens of {token_mint}")
    print(f"📊 Slippage: {slippage_bps / 100}%")
    print("")
    
    try:
        # Get quote
        quote = get_quote(token_mint, WSOL, amount, slippage_bps)
        
        # Display route
        in_amount = int(quote["inAmount"])
        out_amount = int(quote["outAmount"])
        out_sol = out_amount / 1e9
        
        route_steps = []
        for step in quote.get("routePlan", []):
            label = step["swapInfo"]["label"]
            route_steps.append(label)
        
        route = " → ".join(route_steps) if route_steps else "Direct"
        
        print(f"✅ Best route: {route}")
        print(f"💰 You will receive: {out_sol:.8f} SOL")
        print(f"📉 Price impact: {quote.get('priceImpactPct', 0):.4f}%")
        print("")
        
        # Confirm
        confirm = input("Proceed with swap? (yes/no): ").strip().lower()
        if confirm != "yes":
            print("❌ Cancelled")
            sys.exit(0)
        
        # Create swap transaction
        swap_tx = create_swap_tx(quote, wallet_pubkey)
        
        # Sign and send
        signature = sign_and_send_tx(swap_tx, str(keypair_path), rpc_url)
        
        print("")
        print("✅ TRANSACTION SENT!")
        print(f"📝 Signature: {signature}")
        print(f"🔗 Solscan: https://solscan.io/tx/{signature}")
        print("")
        print("⏳ Confirming...")
        
        # Wait for confirmation
        solana_bin = f"{Path.home()}/.local/share/solana/install/active_release/bin/solana"
        subprocess.run(
            [solana_bin, "confirm", signature, "--url", rpc_url],
            check=True
        )
        
        print("✅ Transaction confirmed!")
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
