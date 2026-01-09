#!/usr/bin/env python3
"""
Fetch active CPMM pools from Raydium API
"""

import requests
import json

# Raydium API endpoints
POOL_LIST_API = "https://api-v3.raydium.io/pools/info/list"
POOL_INFO_API = "https://api-v3.raydium.io/pools/info/ids"

def get_cpmm_pools():
    """Fetch CPMM pools from Raydium API"""
    print("Fetching pool list from Raydium API...")
    
    # Request parameters
    params = {
        "poolType": "all",  # or try "Standard" for CPMM specifically
        "poolSortField": "liquidity",
        "sortType": "desc",
        "pageSize": 1000,
        "page": 1
    }
    
    response = requests.get(POOL_LIST_API, params=params)
    response.raise_for_status()
    
    data = response.json()
    
    if not data.get("success"):
        print(f"API Error: {data}")
        return []
    
    pools = data.get("data", {}).get("data", [])
    
    # Filter for CPMM pools (type: "Standard" + programId matches CPMM)
    cpmm_pools = [
        pool for pool in pools
        if pool.get("programId") == "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C"
    ]
    
    print(f"\n✅ Found {len(cpmm_pools)} CPMM pools")
    print(f"   Total pools in API: {len(pools)}")
    
    return cpmm_pools

def print_pool_info(pools):
    """Print pool information"""
    if not pools:
        print("\n❌ No CPMM pools found in API")
        return
    
    print("\n" + "="*100)
    print("Active CPMM Pools:")
    print("="*100)
    
    for i, pool in enumerate(pools[:10], 1):  # Show top 10
        mint_a = pool.get("mintA", {})
        mint_b = pool.get("mintB", {})
        
        print(f"\n{i}. Pool ID: {pool.get('id')}")
        print(f"   Type: {pool.get('type')} (Program: {pool.get('programId')[:8]}...)")
        print(f"   Pair: {mint_a.get('symbol', 'Unknown')} / {mint_b.get('symbol', 'Unknown')}")
        print(f"   Mint A: {mint_a.get('address', 'Unknown')}")
        print(f"   Mint B: {mint_b.get('address', 'Unknown')}")
        print(f"   TVL: ${pool.get('tvl', 0):,.2f}")
        print(f"   Volume 24h: ${pool.get('day', {}).get('volume', 0):,.2f}")
        print(f"   Fee Rate: {pool.get('feeRate', 0) / 10000}%")
    
    if len(pools) > 10:
        print(f"\n... and {len(pools) - 10} more pools")

def save_pool_addresses(pools):
    """Save pool addresses for testing"""
    if not pools:
        return
    
    pool_ids = [pool.get('id') for pool in pools if pool.get('id')]
    
    output = {
        "total_pools": len(pools),
        "pool_ids": pool_ids[:20],  # Top 20 by liquidity
        "sample_pool": pools[0] if pools else None
    }
    
    with open("cpmm_active_pools.json", "w") as f:
        json.dump(output, f, indent=2)
    
    print(f"\n✅ Saved {len(pool_ids[:20])} pool IDs to cpmm_active_pools.json")

if __name__ == "__main__":
    try:
        pools = get_cpmm_pools()
        print_pool_info(pools)
        save_pool_addresses(pools)
        
        if pools:
            print("\n" + "="*100)
            print("Next Steps:")
            print("="*100)
            print("1. Review cpmm_active_pools.json for pool IDs")
            print("2. Test quote_exact_in with real pool:")
            print(f"   Pool ID: {pools[0].get('id')}")
            print(f"   Mint A: {pools[0].get('mintA', {}).get('address')}")
            print(f"   Mint B: {pools[0].get('mintB', {}).get('address')}")
            print("\n3. Run integration test:")
            print("   cargo test --test cpmm_mainnet_integration --ignored -- --nocapture")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("\nAlternative: Check Raydium UI manually")
        print("→ https://raydium.io/liquidity/")
        print("→ Filter by 'Standard' pools (CPMM)")
        print("→ Copy pool addresses from URL or transaction details")
