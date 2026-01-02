#!/usr/bin/env python3
"""Find the newest TradeIntent for Raydium with a non-SOL output mint.

This is used for server-side smoke tests to craft synthetic MarketEvents that
match what momentum-bot is actually producing/expecting.

Usage:
  python3 find_last_raydium_token_intent.py /path/to/trade_intents-YYYYMMDD.jsonl
"""

from __future__ import annotations

import argparse
import json
from typing import Any

SOL_MINT = "So11111111111111111111111111111111111111112"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("path")
    args = parser.parse_args()

    with open(args.path, "rb") as f:
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

        in_mint = res.get("input_mint")
        out_mint = res.get("output_mint")
        pools = res.get("pools")
        pool0 = pools[0] if isinstance(pools, list) and pools else None

        if in_mint != SOL_MINT:
            continue
        if not isinstance(out_mint, str) or out_mint == SOL_MINT:
            continue

        print("intent_id:", obj.get("intent_id"))
        print("input_mint:", in_mint)
        print("output_mint:", out_mint)
        print("pool0:", pool0)
        return 0

    raise SystemExit("no matching intent found")


if __name__ == "__main__":
    raise SystemExit(main())
