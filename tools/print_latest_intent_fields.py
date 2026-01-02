#!/usr/bin/env python3
"""Print key fields from the newest TradeIntent JSONL line.

Usage:
  python3 print_latest_intent_fields.py /path/to/trade_intents-YYYYMMDD.jsonl
"""

from __future__ import annotations

import argparse
import json
from typing import Any


def _get(d: dict[str, Any] | None, key: str) -> Any:
    if not isinstance(d, dict):
        return None
    return d.get(key)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("path", help="Path to JSONL file containing TradeIntent objects")
    args = parser.parse_args()

    with open(args.path, "rb") as f:
        lines = f.read().splitlines()

    if not lines:
        raise SystemExit("file is empty")

    obj = json.loads(lines[-1].decode("utf-8"))

    metadata = obj.get("metadata") if isinstance(obj, dict) else None
    execution = obj.get("execution") if isinstance(obj, dict) else None
    resources = obj.get("resources") if isinstance(obj, dict) else None

    print("intent_id:", _get(obj, "intent_id"))
    print("side:", _get(obj, "side"))
    if isinstance(resources, dict):
        pools = resources.get("pools")
        pool0 = pools[0] if isinstance(pools, list) and pools else None
        print("input_mint:", resources.get("input_mint"))
        print("output_mint:", resources.get("output_mint"))
        print("pool0:", pool0)
    print("dex:", _get(metadata, "dex"))
    print("creator:", _get(metadata, "creator"))
    print("min_out_raw:", _get(metadata, "min_out_raw"))
    print("execution:", execution)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
