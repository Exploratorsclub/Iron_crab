#!/usr/bin/env python3
"""
Test if Geyser provides complete account list by comparing with RPC.
For zero-amount trades, check if the missing token account exists in RPC.
"""

import json
import sys
import requests

# Zero-amount trade from logs
SIGNATURE = "4npFxcXcx8DBoYz8u39NAjcMEFnRJ8ABJZT8GZrk2cndUXrLz7HYpBv4jg634UEsXcU6dycRV7ttd8YeJL54Kiso"
MINT = "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm"
POOL = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TRADER = "BxbuS91tioycP35MbLnAdzqdQc6pvedcp9YN4ygfG2b7"

RPC_URL = "https://mainnet.helius-rpc.com/?api-key=96755862-7b83-484a-9f7a-2c0620253cc1"

def get_transaction(signature):
    """Fetch transaction from RPC."""
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTransaction",
        "params": [
            signature,
            {
                "encoding": "jsonParsed",
                "maxSupportedTransactionVersion": 0
            }
        ]
    }
    
    response = requests.post(RPC_URL, json=payload)
    result = response.json()
    
    if "error" in result:
        print(f"❌ RPC Error: {result['error']}")
        return None
    
    return result.get("result")

def analyze_accounts(tx):
    """Analyze accounts in transaction."""
    if not tx:
        return
    
    meta = tx.get("meta", {})
    message = tx.get("transaction", {}).get("message", {})
    
    # Get all account keys
    account_keys = message.get("accountKeys", [])
    
    print(f"\n📊 Transaction Analysis")
    print(f"Signature: {SIGNATURE}")
    print(f"Mint: {MINT}")
    print(f"Pool: {POOL}")
    print(f"Trader: {TRADER}")
    print(f"\n{'='*80}")
    
    # Find accounts in transaction
    print(f"\n🔍 Account Keys in Transaction ({len(account_keys)} total):")
    
    mint_found = False
    pool_found = False
    trader_ata_found = False
    
    for idx, acc in enumerate(account_keys):
        pubkey = acc.get("pubkey") if isinstance(acc, dict) else acc
        print(f"  [{idx:2d}] {pubkey}")
        
        if pubkey == MINT:
            mint_found = True
            print(f"       ⭐ MINT FOUND")
        elif pubkey == POOL:
            pool_found = True
            print(f"       ⭐ POOL FOUND")
    
    # Check token balances
    print(f"\n💰 Token Balances (from meta.postTokenBalances):")
    post_token_balances = meta.get("postTokenBalances", [])
    pre_token_balances = meta.get("preTokenBalances", [])
    
    if not post_token_balances:
        print("  ❌ NO token balances in transaction!")
    else:
        for tb in post_token_balances:
            account_index = tb.get("accountIndex")
            mint = tb.get("mint")
            owner = tb.get("owner")
            amount = tb.get("uiTokenAmount", {}).get("uiAmountString", "?")
            
            account_pubkey = account_keys[account_index].get("pubkey") if isinstance(account_keys[account_index], dict) else account_keys[account_index]
            
            print(f"  [{account_index:2d}] Account: {account_pubkey}")
            print(f"       Mint: {mint}")
            print(f"       Owner: {owner}")
            print(f"       Amount: {amount}")
            
            if mint == MINT and owner == TRADER:
                trader_ata_found = True
                print(f"       ⭐ TRADER'S TOKEN ACCOUNT FOUND!")
            
            # Calculate change
            pre_balance = next((p for p in pre_token_balances if p.get("accountIndex") == account_index), None)
            if pre_balance:
                pre_amount = float(pre_balance.get("uiTokenAmount", {}).get("uiAmountString", 0))
                post_amount = float(amount)
                change = post_amount - pre_amount
                print(f"       Change: {change:+.6f}")
    
    # Native SOL balances
    print(f"\n💵 Native SOL Balances:")
    pre_balances = meta.get("preBalances", [])
    post_balances = meta.get("postBalances", [])
    
    for idx, (pre, post) in enumerate(zip(pre_balances, post_balances)):
        change = post - pre
        if change != 0:
            account_pubkey = account_keys[idx].get("pubkey") if isinstance(account_keys[idx], dict) else account_keys[idx]
            print(f"  [{idx:2d}] {account_pubkey}")
            print(f"       Pre: {pre/1e9:.9f} SOL")
            print(f"       Post: {post/1e9:.9f} SOL")
            print(f"       Change: {change/1e9:+.9f} SOL")
    
    # Summary
    print(f"\n{'='*80}")
    print(f"\n✅ Summary:")
    print(f"  Mint in accountKeys: {mint_found}")
    print(f"  Pool in accountKeys: {pool_found}")
    print(f"  Trader's Token Account in postTokenBalances: {trader_ata_found}")
    
    if not trader_ata_found:
        print(f"\n❌ PROBLEM FOUND:")
        print(f"  The trader's token account for mint {MINT} is NOT in postTokenBalances!")
        print(f"  This explains why token_amount=0 in the parsed event.")
        print(f"\n  Possible reasons:")
        print(f"  1. Token account not included in Geyser's account list")
        print(f"  2. Account was closed during transaction")
        print(f"  3. Balance didn't change (wrapped/system program interaction)")
    else:
        print(f"\n✅ Token account IS present in RPC response.")
        print(f"  This suggests the parser might be looking at wrong account.")

if __name__ == "__main__":
    print("🔍 Testing Geyser Account Completeness")
    print("Comparing Geyser parsed event vs RPC transaction data")
    
    tx = get_transaction(SIGNATURE)
    if tx:
        analyze_accounts(tx)
    else:
        print("❌ Failed to fetch transaction from RPC")
        sys.exit(1)
