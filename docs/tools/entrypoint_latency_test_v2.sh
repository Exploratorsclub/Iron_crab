#!/usr/bin/env bash
set -euo pipefail

# Entry point latency tester (v2)
# Measures TCP connect times to Gossip (8001) and optional RPC (8899).
# Optionally discovers additional low-latency peers via solana-gossip.
#
# Usage:
#   ./entrypoint_latency_test_v2.sh [--with-gossip] [--host host1,host2,...] [--rpc] [--limit N]
#
# Flags:
#   --with-gossip   Run solana-gossip spy to discover fast peers
#   --host list     Comma-separated host list (override defaults)
#   --rpc           Also measure RPC port (8899) TCP connect
#   --limit N       Limit number of default hosts used
#
# Output:
#   CSV: host,tcp8001_ms[,tcp8899_ms]
#   Sorted recommendation list
#   Optional gossip-derived --entrypoint lines
#
# Requirements: bash, date (millisecond capable), timeout. Optional: solana-gossip.

GOSSIP_PORT=8001
RPC_PORT=8899
TIMEOUT_SEC=3
WITH_GOSSIP=false
MEASURE_RPC=false
CUSTOM_HOSTS=""
LIMIT_DEFAULT=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --with-gossip) WITH_GOSSIP=true; shift ;;
    --host) CUSTOM_HOSTS="$2"; shift 2 ;;
    --rpc) MEASURE_RPC=true; shift ;;
    --limit) LIMIT_DEFAULT="$2"; shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

DEFAULT_HOSTS=(
  entrypoint.mainnet-beta.anza.xyz
  entrypoint2.mainnet-beta.anza.xyz
  entrypoint3.mainnet-beta.anza.xyz
  entrypoint4.mainnet-beta.anza.xyz
  entrypoint.mainnet-beta.solana.com
  entrypoint2.mainnet-beta.solana.com
  entrypoint3.mainnet-beta.solana.com
  entrypoint4.mainnet-beta.solana.com
)

if [[ $LIMIT_DEFAULT -gt 0 ]]; then
  DEFAULT_HOSTS=("${DEFAULT_HOSTS[@]:0:$LIMIT_DEFAULT}")
fi

if [[ -n "$CUSTOM_HOSTS" ]]; then
  IFS=',' read -r -a HOSTS <<< "$CUSTOM_HOSTS"
else
  HOSTS=("${DEFAULT_HOSTS[@]}")
fi

measure_tcp_ms() {
  local host=$1 port=$2
  local start end
  start=$(date +%s%3N)
  if timeout ${TIMEOUT_SEC} bash -c "</dev/tcp/${host}/${port}" 2>/dev/null; then
    end=$(date +%s%3N)
    echo $((end-start))
  else
    echo ""  # failed
  fi
}

CSV_HEADER="host,tcp${GOSSIP_PORT}_ms"
if $MEASURE_RPC; then
  CSV_HEADER="host,tcp${GOSSIP_PORT}_ms,tcp${RPC_PORT}_ms"
fi

CSV_FILE=/tmp/entrypoints_latency.csv
: > "$CSV_FILE"

echo "$CSV_HEADER" >> "$CSV_FILE"
for h in "${HOSTS[@]}"; do
  t_gossip=$(measure_tcp_ms "$h" "$GOSSIP_PORT")
  if $MEASURE_RPC; then
    t_rpc=$(measure_tcp_ms "$h" "$RPC_PORT")
    echo "$h,${t_gossip:-},${t_rpc:-}" >> "$CSV_FILE"
  else
    echo "$h,${t_gossip:-}" >> "$CSV_FILE"
  fi
  sleep 0.05
done

cat "$CSV_FILE"

echo
if command -v awk >/dev/null 2>&1; then
  echo "Sorted by tcp${GOSSIP_PORT}_ms (ascending):"
  awk -F, 'NR>1 && $2!="" {print $0}' "$CSV_FILE" | sort -t, -k2,2n | column -t -s,
fi

FASTEST=$(awk -F, 'NR>1 && $2!="" {print $1,$2}' "$CSV_FILE" | sort -k2,2n | head -1 | awk '{print $1}')
if [[ -z "$FASTEST" ]]; then FASTEST="entrypoint.mainnet-beta.anza.xyz"; fi

echo
echo "Recommended primary --entrypoint lines (top 4 by latency):"
awk -F, 'NR>1 && $2!="" {print $1,$2}' "$CSV_FILE" | sort -k2,2n | head -4 | awk -v gp="$GOSSIP_PORT" '{printf "--entrypoint %s:%s\n", $1, gp}'

echo
if $WITH_GOSSIP; then
  if command -v solana-gossip >/dev/null 2>&1; then
    echo "Discovering additional low-latency peers via solana-gossip (10s)..."
    solana-gossip spy --entrypoint "$FASTEST:${GOSSIP_PORT}" --num-nodes 200 --timeout 10 --extended 2>/dev/null \
      | awk '/:8001/ && /ms$/ { \
          host=""; ms=""; \
          for (i=1;i<=NF;i++){ \
            if ($i ~ /:8001$/) host=$i; \
            if ($i ~ /ms$/) ms=$i; \
          } \
          gsub(/ms$/, "", ms); \
          if (host != "" && ms != "") print host "," ms; \
        }' \
      | sort -t, -k2,2n | head -15 > /tmp/gossip_peers.csv || true
    if [[ -s /tmp/gossip_peers.csv ]]; then
      echo "Top gossip-derived peers:"; column -t -s, /tmp/gossip_peers.csv
      echo
      echo "Add lines (review trust & stability before using):"
      awk -F, '{printf "--entrypoint %s\n", $1}' /tmp/gossip_peers.csv
    else
      echo "No peers parsed (output format may have changed)."
    fi
  else
    echo "solana-gossip not installed; skipping gossip discovery."
  fi
else
  echo "(Skip gossip discovery; run with --with-gossip to enable)"
fi

echo "\nDone. CSV stored at $CSV_FILE"
