#!/usr/bin/env bash
set -u
cd ~/Iron_crab || exit 1

f=$(ls -1t trade_logs/decisions/decision_records-*.jsonl 2>/dev/null | head -n 1 || true)
echo "latest=$f"
if [ -z "${f}" ]; then
  echo "no_decision_records"
  exit 0
fi

echo "---arb_reject_counts_last_400---"
(
  tail -n 400 "$f" \
    | grep -F '"source":"arb-strategy"' \
    | sed -nE 's/.*"reason_code":"([^"]*)".*/\1/p' \
    | sort \
    | uniq -c \
    | sort -nr \
    | head -n 20
) || true

echo "---arb_examples_by_reason---"
for r in RISK_MAX_POSITION UNSUPPORTED_INTENT ARB_HANDLER_NOT_CONFIGURED; do
  echo "reason=$r"
  (tail -n 800 "$f" | grep -F '"source":"arb-strategy"' | grep -F "\"reason_code\":\"$r\"" | tail -n 1) || true
done

echo "---arb_samples_last_3---"
(
  tail -n 400 "$f" \
    | grep -F '"source":"arb-strategy"' \
    | tail -n 3
) || true
