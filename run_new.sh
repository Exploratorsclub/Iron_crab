#!/bin/bash
# IronCrab – New Architecture Run Scripts (WIP)
#
# These scripts start the new multi-process architecture binaries.
# Per docs/TARGET_ARCHITECTURE.md:
# - market-data: Geyser ingest, MarketEvents publisher
# - momentum-bot: Strategy plane, TradeIntents generator
# - execution-engine: Single signer, Tx pipeline
#
# IMPORTANT: This is the rebuild path (WIP). Does not replace legacy default.
#
# Usage:
#   ./run_new.sh market-data
#   ./run_new.sh momentum-bot
#   ./run_new.sh execution-engine
#   ./run_new.sh all --dry-run

set -e

COMPONENT="${1:-}"
CONFIG="${CONFIG:-config.toml}"
NATS_URL="${NATS_URL:-nats://localhost:4222}"
LOG_DIR="${LOG_DIR:-trade_logs}"
RELEASE="${RELEASE:-0}"
DRY_RUN=""

# Parse arguments
shift || true
while [[ $# -gt 0 ]]; do
    case $1 in
        --dry-run) DRY_RUN="--dry-run"; shift ;;
        --release) RELEASE=1; shift ;;
        --config) CONFIG="$2"; shift 2 ;;
        --nats-url) NATS_URL="$2"; shift 2 ;;
        --log-dir) LOG_DIR="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

if [[ -z "$COMPONENT" ]]; then
    echo "Usage: $0 <component> [--dry-run] [--release]"
    echo ""
    echo "Components:"
    echo "  market-data       - Geyser ingest, MarketEvents publisher"
    echo "  momentum-bot      - Strategy plane, TradeIntents generator"
    echo "  execution-engine  - Single signer, Tx pipeline"
    echo "  all               - Start all components"
    exit 1
fi

TARGET_DIR="target/debug"
if [[ "$RELEASE" == "1" ]]; then
    TARGET_DIR="target/release"
fi

COMMON_ARGS="--config $CONFIG --nats-url $NATS_URL --log-dir $LOG_DIR $DRY_RUN"

start_component() {
    local name="$1"
    local port="$2"
    local extra="${3:-}"

    local exe="$TARGET_DIR/$name"
    if [[ ! -x "$exe" ]]; then
        echo "Binary not found: $exe"
        echo "Run: cargo build --bin $name"
        exit 1
    fi

    echo "Starting $name on port $port..."
    echo "  $exe $COMMON_ARGS --metrics-port $port $extra"

    if [[ "$COMPONENT" == "all" ]]; then
        $exe $COMMON_ARGS --metrics-port $port $extra &
    else
        exec $exe $COMMON_ARGS --metrics-port $port $extra
    fi
}

case "$COMPONENT" in
    market-data)
        start_component "market-data" 9801
        ;;
    momentum-bot)
        start_component "momentum-bot" 9802
        ;;
    execution-engine)
        extra=""
        if [[ -n "$DRY_RUN" ]]; then
            extra="--simulate-only"
        fi
        start_component "execution-engine" 9803 "$extra"
        ;;
    all)
        echo "Starting all components (Ctrl+C to stop)..."
        echo ""
        echo "Components:"
        echo "  market-data      -> http://localhost:9801/metrics"
        echo "  momentum-bot     -> http://localhost:9802/metrics"
        echo "  execution-engine -> http://localhost:9803/metrics"
        echo ""

        start_component "market-data" 9801
        sleep 1
        start_component "momentum-bot" 9802
        sleep 1
        start_component "execution-engine" 9803 "--simulate-only"

        echo ""
        echo "All components started. Check logs in $LOG_DIR/"
        echo "Press Ctrl+C to stop all..."

        # Wait for all background jobs
        wait
        ;;
    *)
        echo "Unknown component: $COMPONENT"
        exit 1
        ;;
esac
