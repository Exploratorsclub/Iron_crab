#!/usr/bin/env python3
"""Republish a recent Raydium TradeIntent to NATS with a fresh intent_id.

This is a deterministic smoke-test for execution-engine tx planning when the
producer (momentum-bot) does not emit a new intent on demand.

Env vars:
  NATS_URL (default: nats://localhost:4222)
  INTENTS_JSONL (default: /home/ironcrab/Iron_crab/trade_logs/intents/trade_intents-YYYYMMDD.jsonl)
  TOPIC (default: ironcrab.v1.trade_intents)

Requires: nats-py
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from datetime import datetime
from typing import Any

import nats

SOL_MINT = "So11111111111111111111111111111111111111112"


def _default_intents_path() -> str:
    day = datetime.utcnow().strftime("%Y%m%d")
    return f"/home/ironcrab/Iron_crab/trade_logs/intents/trade_intents-{day}.jsonl"


def _pick_template_intent(path: str) -> dict[str, Any]:
    with open(path, "rb") as f:
        lines = f.read().splitlines()

    for raw in reversed(lines):
        try:
            obj: dict[str, Any] = json.loads(raw.decode("utf-8"))
        except Exception:
            continue

        md = obj.get("metadata")
        if not isinstance(md, dict) or md.get("dex") != "raydium":
            continue

        res = obj.get("resources")
        if not isinstance(res, dict):
            continue

        if res.get("input_mint") != SOL_MINT:
            continue
        out_mint = res.get("output_mint")
        if not isinstance(out_mint, str) or out_mint == SOL_MINT:
            continue

        return obj

    raise RuntimeError("no suitable raydium intent template found")


async def main() -> int:
    nats_url = os.environ.get("NATS_URL", "nats://localhost:4222")
    intents_path = os.environ.get("INTENTS_JSONL", _default_intents_path())
    topic = os.environ.get("TOPIC", "ironcrab.v1.trade_intents")

    template = _pick_template_intent(intents_path)

    now_ms = int(time.time() * 1000)
    new_intent = dict(template)
    new_intent["ts_unix_ms"] = now_ms
    new_intent["component"] = "test-publisher"
    new_intent["run_id"] = "manual"
    new_intent["intent_id"] = f"test-{now_ms}"

    nc = await nats.connect(nats_url)
    try:
        payload = json.dumps(new_intent, separators=(",", ":")).encode("utf-8")
        await nc.publish(topic, payload)
        await nc.flush(timeout=2)
    finally:
        await nc.close()

    print(new_intent["intent_id"])
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
