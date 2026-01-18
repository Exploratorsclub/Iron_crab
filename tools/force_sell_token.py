#!/usr/bin/env python3
"""
Force SELL intent for a specific token.

Usage:
    python force_sell_token.py --mint <TOKEN_MINT> [--pool <POOL_ADDRESS>] [--dex <DEX_NAME>]

If pool/dex not specified, will try to auto-detect from market events or use provided values.
"""

import asyncio
import json
import sys
import argparse
import time
from pathlib import Path

# Add parent dir to path for nats import
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from nats.aio.client import Client as NATS
except ImportError:
    print("ERROR: nats-py not installed. Run: pip install nats-py")
    sys.exit(1)


async def publish_sell_intent(
    mint: str,
    token_amount: int,
    token_decimals: int,
    pool: str,
    dex: str,
    min_out_lamports: int = 100_000,  # 0.0001 SOL minimum (emergency fallback)
    accounts: list = None,
):
    """Publish a SELL intent to NATS."""
    
    nc = NATS()
    await nc.connect("nats://localhost:4222")
    
    intent_id = f"force-sell-{int(time.time() * 1000)}"
    
    # Build TradeIntent
    intent = {
        "schema_version": 1,
        "ts_unix_ms": int(time.time() * 1000),
        "component": "force-sell-script",
        "build": "manual",
        "run_id": "manual-force-sell",
        "intent_id": intent_id,
        "source": "manual-force-sell",
        "tier": "Tier1",
        "origin_type": "Manual",
        "ttl_ms": 10000,  # 10 second TTL
        "required_capital": {
            "raw": token_amount,
            "decimals": token_decimals,
            "ui": str(token_amount / (10 ** token_decimals))
        },
        "resources": {
            "input_mint": mint,  # Selling this token
            "output_mint": "So11111111111111111111111111111111111111112",  # For SOL
            "pools": [pool],
            "accounts": accounts or [],
        },
        "expected_roi_bps": 0,  # No profit expectation for force-sell
        "max_slippage_bps": 1000,  # 10% max slippage (emergency exit)
        "side": "Sell",
        "regime": "Early",
        "metadata": {
            "dex": dex,
            "reason_code": "MANUAL_FORCE_SELL",
            "reason_detail": "Manual force sell via script",
            "exit_type": "MANUAL",
            "min_out_raw": str(min_out_lamports),
        },
        "execution": {
            "min_out": {
                "raw": min_out_lamports,
                "decimals": 9,
                "ui": str(min_out_lamports / 1e9)
            }
        }
    }
    
    # Publish to trade intents topic
    topic = "ironcrab.v1.trade_intents"
    payload = json.dumps(intent).encode()
    
    print(f"📤 Publishing SELL intent to '{topic}'")
    print(f"   Intent ID: {intent_id}")
    print(f"   Token: {mint}")
    print(f"   Amount: {token_amount / (10 ** token_decimals):.6f} tokens")
    print(f"   Pool: {pool}")
    print(f"   DEX: {dex}")
    print(f"   Min Out: {min_out_lamports / 1e9:.8f} SOL")
    
    await nc.publish(topic, payload)
    await nc.flush()
    await nc.close()
    
    print("✅ Intent published successfully!")
    print(f"\nMonitor execution:")
    print(f"  journalctl -u execution-engine -f | grep '{intent_id}'")
    
    return intent_id


def find_pool_from_market_events(mint: str, log_dir: Path):
    """Find pool/dex from recent market events."""
    market_events_dir = log_dir / "market_events"
    
    if not market_events_dir.exists():
        return None, None
    
    # Check today's market events
    from datetime import datetime
    today = datetime.now().strftime("%Y%m%d")
    events_file = market_events_dir / f"market_events-{today}.jsonl"
    
    if not events_file.exists():
        return None, None
    
    # Search for events with this mint
    with open(events_file, 'r') as f:
        for line in f:
            try:
                event = json.loads(line)
                if event.get("mint") == mint:
                    # Found an event for this token
                    pool = event.get("pool")
                    dex = event.get("dex")
                    if pool and dex:
                        print(f"💡 Found pool from market events: {pool} ({dex})")
                        return pool, dex
            except json.JSONDecodeError:
                continue
    
    return None, None


def find_pool_from_intents(mint: str, log_dir: Path):
    """Find pool/dex from original BUY intent."""
    intents_dir = log_dir / "intents"
    
    if not intents_dir.exists():
        return None, None, None
    
    # Check today's and yesterday's intents
    from datetime import datetime, timedelta
    for days_ago in range(2):
        date = (datetime.now() - timedelta(days=days_ago)).strftime("%Y%m%d")
        intents_file = intents_dir / f"trade_intents-{date}.jsonl"
        
        if not intents_file.exists():
            continue
        
        # Search for BUY intents with this mint as output
        with open(intents_file, 'r') as f:
            for line in f:
                try:
                    intent = json.loads(line)
                    resources = intent.get("resources", {})
                    
                    # Check if this is a BUY for our token
                    if (intent.get("side") == "Buy" and 
                        resources.get("output_mint") == mint):
                        
                        pools = resources.get("pools", [])
                        accounts = resources.get("accounts", [])
                        dex = intent.get("metadata", {}).get("dex")
                        
                        if pools and dex:
                            pool = pools[0]
                            print(f"💡 Found pool from original BUY intent: {pool} ({dex})")
                            return pool, dex, accounts
                            
                except json.JSONDecodeError:
                    continue
    
    return None, None, None


async def main():
    parser = argparse.ArgumentParser(description="Force-sell a token via NATS intent")
    parser.add_argument("--mint", required=True, help="Token mint address to sell")
    parser.add_argument("--pool", help="Pool address (auto-detect if not provided)")
    parser.add_argument("--dex", help="DEX name (auto-detect if not provided)")
    parser.add_argument("--amount", type=float, help="Token amount to sell (all if not provided)")
    parser.add_argument("--decimals", type=int, default=6, help="Token decimals (default: 6)")
    parser.add_argument("--min-out", type=float, default=0.0001, help="Minimum SOL output (default: 0.0001)")
    
    args = parser.parse_args()
    
    # Try to auto-detect pool/dex if not provided
    pool = args.pool
    dex = args.dex
    accounts = []
    
    if not pool or not dex:
        log_dir = Path(__file__).parent.parent / "trade_logs"
        
        # Try market events first
        if not pool or not dex:
            found_pool, found_dex = find_pool_from_market_events(args.mint, log_dir)
            if found_pool:
                pool = pool or found_pool
                dex = dex or found_dex
        
        # Try intents as fallback
        if not pool or not dex:
            found_pool, found_dex, found_accounts = find_pool_from_intents(args.mint, log_dir)
            if found_pool:
                pool = pool or found_pool
                dex = dex or found_dex
                accounts = found_accounts or []
    
    if not pool:
        print("❌ ERROR: Pool address required (could not auto-detect)")
        print("   Use --pool <POOL_ADDRESS>")
        sys.exit(1)
    
    if not dex:
        print("❌ ERROR: DEX name required (could not auto-detect)")
        print("   Use --dex <DEX_NAME>")
        sys.exit(1)
    
    # Get token amount (from execution-engine position tracker or user input)
    if args.amount:
        token_amount = int(args.amount * (10 ** args.decimals))
    else:
        # Try to get from execution_results
        log_dir = Path(__file__).parent.parent / "trade_logs"
        executions_dir = log_dir / "executions"
        
        # Find latest BUY execution for this token
        from datetime import datetime, timedelta
        token_amount = None
        
        for days_ago in range(2):
            date = (datetime.now() - timedelta(days=days_ago)).strftime("%Y%m%d")
            exec_file = executions_dir / f"execution_results-{date}.jsonl"
            
            if not exec_file.exists():
                continue
            
            with open(exec_file, 'r') as f:
                for line in f:
                    try:
                        exec_result = json.loads(line)
                        
                        # Check if Confirmed BUY for our token
                        if (exec_result.get("status") == "Confirmed" and
                            exec_result.get("fill_out")):
                            
                            # Check if this is our token
                            token_mint_in_result = exec_result.get("token_mint")
                            if token_mint_in_result == args.mint:
                                fill_out = exec_result["fill_out"]
                                token_amount = fill_out["raw"]
                                args.decimals = fill_out["decimals"]
                                print(f"💡 Found token amount from execution: {token_amount / (10 ** args.decimals):.6f}")
                                break
                    except json.JSONDecodeError:
                        continue
                
                if token_amount:
                    break
        
        if not token_amount:
            print("❌ ERROR: Could not determine token amount")
            print("   Use --amount <AMOUNT>")
            sys.exit(1)
    
    # Calculate min_out in lamports
    min_out_lamports = int(args.min_out * 1e9)
    
    # Publish the intent
    intent_id = await publish_sell_intent(
        mint=args.mint,
        token_amount=token_amount,
        token_decimals=args.decimals,
        pool=pool,
        dex=dex,
        min_out_lamports=min_out_lamports,
        accounts=accounts,
    )
    
    print(f"\n✅ Force-sell intent published: {intent_id}")


if __name__ == "__main__":
    asyncio.run(main())
