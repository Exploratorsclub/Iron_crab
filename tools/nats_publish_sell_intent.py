import asyncio
import json
import os
import time
import uuid
from typing import Optional

from nats.aio.client import Client as NATS

TOPIC = "ironcrab.v1.trade_intents"


def build_sell_intent(
    *,
    input_mint: str,
    amount_raw: int,
    decimals: int,
    pool_id: str,
    expected_roi_bps: int,
    max_slippage_bps: int,
    min_out_raw: int,
    min_out_decimals: int,
    dex: str,
    creator: Optional[str],
) -> dict:
    now_ms = int(time.time() * 1000)
    run_id = os.getenv("RUN_ID", "manual-publish")

    return {
        "schema_version": 1,
        "ts_unix_ms": now_ms,
        "component": "manual-publisher",
        "build": "manual",
        "run_id": run_id,
        "intent_id": f"manual-sell-{uuid.uuid4()}",
        "source": "manual-publisher",
        "tier": "Tier1",
        "origin_type": "StrategyA",
        "ttl_ms": 60_000,
        "required_capital": {
            "raw": int(amount_raw),
            "decimals": int(decimals),
        },
        "resources": {
            "input_mint": input_mint,
            "output_mint": "So11111111111111111111111111111111111111112",
            "pools": [pool_id],
        },
        "execution": {
            "min_out": {
                "raw": int(min_out_raw),
                "decimals": int(min_out_decimals),
            }
        },
        "expected_roi_bps": int(expected_roi_bps),
        "max_slippage_bps": int(max_slippage_bps),
        "side": "Sell",
        "regime": "Early",
        "metadata": {
            # Backward-compatibility for older engines (stringly-typed)
            "min_out_raw": str(int(min_out_raw)),

            # Used by tx planning to pick the DEX implementation
            "dex": dex,

            # Pump.fun tx planning requires creator; ignored for Orca.
            **({"creator": creator} if creator else {}),
        },
    }


async def main() -> None:
    nats_url = os.getenv("NATS_URL", "nats://localhost:4222")
    input_mint = os.getenv("INPUT_MINT", "So11111111111111111111111111111111111111112")
    pool_id = os.getenv("POOL_ID", "manual-test-pool")
    amount_raw = int(os.getenv("AMOUNT_RAW", "1000000"))
    decimals = int(os.getenv("DECIMALS", "9"))
    expected_roi_bps = int(os.getenv("EXPECTED_ROI_BPS", "2000"))
    max_slippage_bps = int(os.getenv("MAX_SLIPPAGE_BPS", "100"))

    # Required for deterministic tx planning.
    # For SELL intents (token->SOL), this is in lamports (decimals=9).
    min_out_raw = int(os.getenv("MIN_OUT_RAW", "1"))
    min_out_decimals = int(os.getenv("MIN_OUT_DECIMALS", "9"))

    dex = os.getenv("DEX", "pumpfun")
    creator = os.getenv("CREATOR")

    msg = build_sell_intent(
        input_mint=input_mint,
        amount_raw=amount_raw,
        decimals=decimals,
        pool_id=pool_id,
        expected_roi_bps=expected_roi_bps,
        max_slippage_bps=max_slippage_bps,
        min_out_raw=min_out_raw,
        min_out_decimals=min_out_decimals,
        dex=dex,
        creator=creator,
    )

    nc = NATS()
    await nc.connect(servers=[nats_url])
    await nc.publish(TOPIC, json.dumps(msg, separators=(",", ":")).encode("utf-8"))
    await nc.flush()
    await nc.close()

    print(f"Published SELL intent to {TOPIC} on {nats_url}")
    print(f"intent_id={msg['intent_id']}")


if __name__ == "__main__":
    asyncio.run(main())
