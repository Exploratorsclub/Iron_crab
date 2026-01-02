import asyncio
import json
import os
import time
import uuid

from nats.aio.client import Client as NATS

TOPIC_MARKET_EVENTS = "ironcrab.v1.market_events"
SOL_MINT = "So11111111111111111111111111111111111111112"


def now_ms() -> int:
    return int(time.time() * 1000)


async def main() -> None:
    nats_url = os.environ.get("NATS_URL", "nats://localhost:4222")

    # Use a real pool/mint from the current server logs by default.
    pool_address = os.environ.get(
        "POOL_ADDRESS",
        "Dqb7bL7MZkuDgHrZZphRMRViJnepHxf9odx3RRwmifur",
    )
    mint = os.environ.get(
        "MINT",
        "921MoB1U7VprQfWw5D37a38LCBgB3nareT7rNffk66BG",
    )
    dex = os.environ.get("DEX", "raydium")

    publish_pool_created = os.environ.get("PUBLISH_POOL_CREATED", "1") not in (
        "0",
        "false",
        "False",
    )
    quote_mint = os.environ.get("QUOTE_MINT", SOL_MINT)
    initial_liquidity_sol = os.environ.get("INITIAL_LIQUIDITY_SOL", "50.0")

    # Optional: set a synthetic slot for all events.
    slot_env = os.environ.get("SLOT")
    slot_value = int(slot_env) if slot_env not in (None, "") else None

    trades = int(os.environ.get("TRADES", "25"))
    unique_buyers = int(os.environ.get("UNIQUE_BUYERS", "5"))
    sol_amount = int(os.environ.get("SOL_AMOUNT_LAMPORTS", "100000000"))  # 0.1 SOL
    token_amount = int(os.environ.get("TOKEN_AMOUNT_RAW", "1000000000"))

    run_id = os.environ.get("RUN_ID", str(uuid.uuid4()))
    build = os.environ.get("BUILD", "manual-test")

    nc = NATS()
    await nc.connect(servers=[nats_url])

    # Momentum-bot only creates trackers after seeing PoolCreated with sufficient liquidity.
    # Publishing this first makes the Trade burst fully deterministic for intent generation.
    if publish_pool_created:
        evt = {
            "schema_version": 1,
            "ts_unix_ms": now_ms(),
            "component": "manual-test",
            "build": build,
            "run_id": run_id,
            "event_id": f"evt-{run_id[:8]}-poolcreated",
            "source": "manual-test",
            "slot": slot_value,
            "kind": "PoolCreated",
            "pool_address": pool_address,
            "base_mint": mint,
            "quote_mint": quote_mint,
            "dex": dex,
            # rust_decimal serde expects a string
            "initial_liquidity_sol": initial_liquidity_sol,
        }
        await nc.publish(TOPIC_MARKET_EVENTS, json.dumps(evt).encode("utf-8"))
        await asyncio.sleep(0.05)

    for i in range(trades):
        trader = f"Buyer{(i % unique_buyers) + 1:02d}"
        evt = {
            "schema_version": 1,
            "ts_unix_ms": now_ms(),
            "component": "manual-test",
            "build": build,
            "run_id": run_id,
            "event_id": f"evt-{run_id[:8]}-{i:04d}",
            "source": "manual-test",
            "slot": slot_value,
            "kind": "Trade",
            "pool_address": pool_address,
            "mint": mint,
            "trader": trader,
            "is_buy": True,
            "sol_amount": sol_amount,
            "token_amount": token_amount,
            "signature": f"manualsig{i:04d}",
            "dex": dex,
        }

        await nc.publish(TOPIC_MARKET_EVENTS, json.dumps(evt).encode("utf-8"))
        await asyncio.sleep(0.02)

    await nc.flush(1)
    await nc.drain()


if __name__ == "__main__":
    asyncio.run(main())
