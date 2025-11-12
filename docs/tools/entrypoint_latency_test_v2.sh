#!/usr/bin/env bash
set -euo pipefail

# Entry point latency tester (v2)
# Measures TCP connect times to Gossip (8001) and optional RPC (8899).
# Optionally discovers additional low-latency peers via solana-gossip.
#
# Usage:
#   ./entrypoint_latency_test_v2.sh [--with-gossip] [--host host1,host2,...] [--rpc] [--limit N] [--gossip-cmd CMD] [--gossip-timeout SEC] [--debug]
#
# Flags:
#   --with-gossip   Run solana-gossip spy to discover fast peers
#   --host list     Comma-separated host list (override defaults)
#   --rpc           Also measure RPC port (8899) TCP connect
#   --limit N       Limit number of default hosts used
#   --gossip-cmd        Override gossip command (e.g. "sudo -u sol -H solana-gossip" or absolute path)
#   --gossip-timeout    Seconds to run gossip spy (default 10)
#   --debug             Keep raw gossip output & extra diagnostics (/tmp/gossip_raw.txt)
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
GOSSIP_CMD=${GOSSIP_CMD:-solana-gossip}
GOSSIP_TIMEOUT=10
DEBUG=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --with-gossip) WITH_GOSSIP=true; shift ;;
    --host) CUSTOM_HOSTS="$2"; shift 2 ;;
    --rpc) MEASURE_RPC=true; shift ;;
    --limit) LIMIT_DEFAULT="$2"; shift 2 ;;
  --gossip-cmd) GOSSIP_CMD="$2"; shift 2 ;;
  --gossip-timeout) GOSSIP_TIMEOUT="$2"; shift 2 ;;
  --debug) DEBUG=true; shift ;;
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
  gossip_available() { bash -lc "$GOSSIP_CMD --version" >/dev/null 2>&1; }
  run_gossip() { bash -lc "$GOSSIP_CMD $*"; }
  if gossip_available; then
    echo "Discovering additional low-latency peers via solana-gossip (${GOSSIP_TIMEOUT}s)..."
    RAW_GOSSIP=/tmp/gossip_raw.txt
    # Capture raw output for robust parsing and optional debugging
    if $DEBUG; then
      echo "[debug] Using gossip command: $GOSSIP_CMD" >&2
      echo "[debug] Writing raw gossip output to $RAW_GOSSIP" >&2
    fi
    run_gossip spy --entrypoint "$FASTEST:${GOSSIP_PORT}" --num-nodes 200 --timeout ${GOSSIP_TIMEOUT} --extended 2>/dev/null | tee "$RAW_GOSSIP" >/dev/null || true

    # Parse multiple possible formats:
    # - tokens like host:8001 ... 12ms or 12.3ms (anywhere in line)
    # - strip trailing commas/parentheses from tokens
    awk '
      {
        host=""; ms="";
        # find host token first
        for (i=1;i<=NF;i++) {
          if ($i ~ /:8001[),]*$/) {
            h=$i; gsub(/[),]$/, "", h); host=h; break;
          }
        }
        # find first ms token
        for (i=1;i<=NF && ms=="";i++) {
          if ($i ~ /^[0-9]+(\.[0-9]+)?ms[),]*$/) {
            m=$i; gsub(/[),]/, "", m); gsub(/ms$/, "", m); ms=m;
          }
        }
        if (host != "" && ms != "") print host "," ms;
      }
    ' "$RAW_GOSSIP" | sort -u | sort -t, -k2,2n | head -15 > /tmp/gossip_peers.csv || true

    # Fallback: try a more lenient grep-based extraction if nothing parsed
    if [[ ! -s /tmp/gossip_peers.csv ]]; then
      if $DEBUG; then echo "[debug] First 20 lines of raw gossip output:" >&2; head -20 "$RAW_GOSSIP" >&2; fi
      paste <(
        grep -Eo '([0-9]{1,3}\.){3}[0-9]{1,3}:8001|[A-Za-z0-9.-]+:8001' "$RAW_GOSSIP" | head -200
      ) <(
        grep -Eo '[0-9]+(\.[0-9]+)?ms' "$RAW_GOSSIP" | sed 's/ms$//' | head -200
      ) 2>/dev/null \
      | awk '{print $1","$2}' \
      | grep -E ":8001,([0-9]+(\.[0-9]+)?)$" \
      | sort -u | sort -t, -k2,2n | head -15 > /tmp/gossip_peers.csv || true
    fi
    if [[ -s /tmp/gossip_peers.csv ]]; then
      echo "Top gossip-derived peers:"; column -t -s, /tmp/gossip_peers.csv
      echo
      echo "Add lines (review trust & stability before using):"
      awk -F, '{printf "--entrypoint %s\n", $1}' /tmp/gossip_peers.csv
    else
      echo "No peers parsed (output format may have changed). Try --debug to inspect raw output at $RAW_GOSSIP."
    fi
  else
    echo "gossip command not available: $GOSSIP_CMD ; skipping gossip discovery."
  fi
else
  echo "(Skip gossip discovery; run with --with-gossip to enable)"
fi

echo
echo "Done. CSV stored at $CSV_FILE"
