#!/usr/bin/env python3
"""Print the tx_plan DecisionRecord check for a given intent_id.

Usage:
  python3 print_tx_plan_check.py /path/to/decision_records-YYYYMMDD.jsonl <intent_id>
"""

from __future__ import annotations

import argparse
import json
from typing import Any


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("path")
    parser.add_argument("intent_id")
    args = parser.parse_args()

    found: dict[str, Any] | None = None
    with open(args.path, "rb") as f:
        for raw in f:
            try:
                obj = json.loads(raw.decode("utf-8"))
            except Exception:
                continue
            if isinstance(obj, dict) and obj.get("intent_id") == args.intent_id:
                found = obj

    if not found:
        raise SystemExit("no matching decision record found")

    checks = found.get("checks")
    if not isinstance(checks, list):
        raise SystemExit("decision record has no checks[]")

    tx_plan = None
    for c in checks:
        if not isinstance(c, dict):
            continue
        name = c.get("check") or c.get("check_name")
        if name == "tx_plan":
            tx_plan = c
            break

    if not tx_plan:
        raise SystemExit("no tx_plan check found")

    print(json.dumps(tx_plan, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
