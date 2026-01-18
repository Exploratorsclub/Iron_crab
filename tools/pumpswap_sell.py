#!/usr/bin/env python3
"""
Direct PumpSwap sell - builds raw Solana transaction
Bypasses Jupiter since token has no routes
"""

import sys
import json
import base64
import subprocess
from pathlib import Path

# Token details
TOKEN_MINT = "9V7jznWgdN6tjMaJ6Bq11ZVQMkza6Zh45atgXbVmpump"
TOKEN_BALANCE = 1494544945  # raw units (6 decimals)
PUMP_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"  # Pump.fun program

# Known pool from logs
POOL_ADDRESS = "8MyRftXMHEQ96n9kfsXbDWTE8nSRv6G4LnAS1nxPtdoo"

def get_wallet_keypair():
    """Read wallet keypair"""
    keypair_path = Path.home() / ".config" / "solana" / "id.json"
    if not keypair_path.exists():
        print(f"❌ Keypair not found: {keypair_path}")
        sys.exit(1)
    
    with open(keypair_path) as f:
        return json.load(f)

def get_wallet_pubkey():
    """Get wallet public key"""
    result = subprocess.run(
        ["solana-keygen", "pubkey"],
        capture_output=True,
        text=True,
        check=True
    )
    return result.stdout.strip()

def get_token_account(mint, owner):
    """Get associated token account address"""
    result = subprocess.run(
        ["spl-token", "address", "--token", mint, "--owner", owner],
        capture_output=True,
        text=True,
        check=True
    )
    return result.stdout.strip()

def main():
    print("🔴 PumpSwap Direct Sell")
    print(f"   Token: {TOKEN_MINT}")
    print(f"   Balance: {TOKEN_BALANCE / 1e6:.6f}")
    print(f"   Pool: {POOL_ADDRESS}")
    print()
    
    # This approach needs the Pump.fun swap instruction format
    # which requires reverse-engineering or using their SDK
    
    print("⚠️  Problem: Pump.fun swap requires program-specific instruction encoding")
    print()
    print("Options:")
    print("   1. Use Raydium if token migrated (check birdeye/dexscreener)")
    print("   2. Export private key → Phantom → manual swap on pump.fun")
    print("   3. Wait for liquidity on Jupiter-integrated DEX")
    print()
    print("Checking if token is on Raydium...")
    
    # Try to find token on Raydium/Orca via RPC
    # (would need to query program accounts, complex)
    
    print()
    print("💡 Simplest solution: Export key to Phantom")
    print("   1. cat ~/.config/solana/id.json")
    print("   2. Import to Phantom wallet")
    print("   3. Swap on pump.fun interface ($1.50 value)")

if __name__ == "__main__":
    main()
