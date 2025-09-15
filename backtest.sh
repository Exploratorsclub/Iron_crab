#!/bin/bash
#
# IronCrab Backtest Script for Unix/Linux/macOS
# Runs backtesting with various options
#

set -e

RELEASE=""
CONFIG="config.example.toml"
BUILD=""
FEATURES=""
REPLAY_TRACE=""
REPLAY_START=""
REPLAY_END=""
REPLAY_SLOT_MS="400"
REPLAY_SEED=""
IMPACT="cpmm"
IMPACT_EXTRA_FEE_BPS="0"
IMPACT_NOISE_MEAN_BPS="0"
IMPACT_NOISE_STD_BPS="0"
PY_SCRIPT=""
VALIDATE_LIVE_CSV=""
HELP=""

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --release|-r)
            RELEASE="1"
            shift
            ;;
        --config|-c)
            CONFIG="$2"
            shift 2
            ;;
        --build|-b)
            BUILD="1"
            shift
            ;;
        --features|-f)
            FEATURES="$2"
            shift 2
            ;;
        --replay-trace)
            REPLAY_TRACE="$2"
            shift 2
            ;;
        --replay-start)
            REPLAY_START="$2"
            shift 2
            ;;
        --replay-end)
            REPLAY_END="$2"
            shift 2
            ;;
        --replay-slot-ms)
            REPLAY_SLOT_MS="$2"
            shift 2
            ;;
        --replay-seed)
            REPLAY_SEED="$2"
            shift 2
            ;;
        --impact)
            IMPACT="$2"
            shift 2
            ;;
        --impact-extra-fee-bps)
            IMPACT_EXTRA_FEE_BPS="$2"
            shift 2
            ;;
        --impact-noise-mean-bps)
            IMPACT_NOISE_MEAN_BPS="$2"
            shift 2
            ;;
        --impact-noise-std-bps)
            IMPACT_NOISE_STD_BPS="$2"
            shift 2
            ;;
        --py-script)
            PY_SCRIPT="$2"
            shift 2
            ;;
        --validate-live-csv)
            VALIDATE_LIVE_CSV="$2"
            shift 2
            ;;
        --help|-h)
            HELP="1"
            shift
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

if [[ "$HELP" == "1" ]]; then
    cat << 'EOF'
IronCrab Backtest Script

Usage: ./backtest.sh [options]

Options:
    --release, -r                   Use release build
    --config, -c <path>             Configuration file (default: config.example.toml)
    --build, -b                     Build before running
    --features, -f <list>           Features to build with (if --build is used)
    --replay-trace <path>           Path to replay trace file (.jsonl.gz)
    --replay-start <slot>           Start slot for replay
    --replay-end <slot>             End slot for replay  
    --replay-slot-ms <ms>           Milliseconds per slot (default: 400)
    --replay-seed <seed>            Random seed for deterministic replay
    --impact <model>                Impact model: cpmm|clmm|none (default: cpmm)
    --impact-extra-fee-bps <bps>    Additional fee in basis points
    --impact-noise-mean-bps <bps>   Noise mean in basis points
    --impact-noise-std-bps <bps>    Noise standard deviation in basis points
    --py-script <path>              Python strategy script to use
    --validate-live-csv <path>      Validate against live CSV trades
    --help, -h                      Show this help message

Examples:
    ./backtest.sh --replay-trace "data/trace.jsonl.gz" --replay-start 250000000 --replay-end 250001000
    ./backtest.sh --build --release --impact "clmm" --impact-extra-fee-bps 10
    ./backtest.sh --py-script "strategies/sample.py" --replay-trace "data/trace.jsonl.gz"
    ./backtest.sh --validate-live-csv "trades.csv" --replay-trace "data/trace.jsonl.gz"

Note: You need to have recorded trace data to run backtests.
Use the recorder binary to capture live data first.
EOF
    exit 0
fi

echo "=== IronCrab Backtest Script ==="

# Check if config file exists
if [[ ! -f "$CONFIG" ]]; then
    echo "Error: Configuration file '$CONFIG' not found!"
    echo "Please create a configuration file based on config.example.toml"
    exit 1
fi

# Build if requested
if [[ "$BUILD" == "1" ]]; then
    echo "Building IronCrab..."
    BUILD_ARGS=()
    if [[ "$RELEASE" == "1" ]]; then
        BUILD_ARGS+=(--release)
    fi
    if [[ -n "$FEATURES" ]]; then
        BUILD_ARGS+=(--features --feature-list "$FEATURES")
    fi
    
    ./build.sh "${BUILD_ARGS[@]}"
fi

# Determine binary path
if [[ "$RELEASE" == "1" ]]; then
    TARGET_DIR="target/release"
else
    TARGET_DIR="target/debug"
fi
BINARY_PATH="$TARGET_DIR/backtest_driver"

# Check if binary exists
if [[ ! -f "$BINARY_PATH" ]]; then
    echo "Error: Backtest binary not found at $BINARY_PATH"
    echo "Please run with --build flag or run ./build.sh first"
    exit 1
fi

# Build command arguments
CMD_ARGS=("--config" "$CONFIG")

if [[ -n "$REPLAY_TRACE" ]]; then
    if [[ ! -f "$REPLAY_TRACE" ]]; then
        echo "Error: Replay trace file '$REPLAY_TRACE' not found!"
        exit 1
    fi
    CMD_ARGS+=("--replay-trace" "$REPLAY_TRACE")
fi

if [[ -n "$REPLAY_START" ]]; then CMD_ARGS+=("--replay-start" "$REPLAY_START"); fi
if [[ -n "$REPLAY_END" ]]; then CMD_ARGS+=("--replay-end" "$REPLAY_END"); fi
if [[ "$REPLAY_SLOT_MS" != "400" ]]; then CMD_ARGS+=("--replay-slot-ms" "$REPLAY_SLOT_MS"); fi
if [[ -n "$REPLAY_SEED" ]]; then CMD_ARGS+=("--replay-seed" "$REPLAY_SEED"); fi
if [[ "$IMPACT" != "cpmm" ]]; then CMD_ARGS+=("--impact" "$IMPACT"); fi
if [[ "$IMPACT_EXTRA_FEE_BPS" != "0" ]]; then CMD_ARGS+=("--impact-extra-fee-bps" "$IMPACT_EXTRA_FEE_BPS"); fi
if [[ "$IMPACT_NOISE_MEAN_BPS" != "0" ]]; then CMD_ARGS+=("--impact-noise-mean-bps" "$IMPACT_NOISE_MEAN_BPS"); fi
if [[ "$IMPACT_NOISE_STD_BPS" != "0" ]]; then CMD_ARGS+=("--impact-noise-std-bps" "$IMPACT_NOISE_STD_BPS"); fi

if [[ -n "$PY_SCRIPT" ]]; then
    if [[ ! -f "$PY_SCRIPT" ]]; then
        echo "Error: Python script '$PY_SCRIPT' not found!"
        exit 1
    fi
    CMD_ARGS+=("--py-script" "$PY_SCRIPT")
fi

if [[ -n "$VALIDATE_LIVE_CSV" ]]; then
    if [[ ! -f "$VALIDATE_LIVE_CSV" ]]; then
        echo "Error: Live CSV file '$VALIDATE_LIVE_CSV' not found!"
        exit 1
    fi
    CMD_ARGS+=("--validate-live-csv" "$VALIDATE_LIVE_CSV")
fi

echo "Starting IronCrab Backtest..."
echo "Binary: $BINARY_PATH"
echo "Config: $CONFIG"
if [[ -n "$REPLAY_TRACE" ]]; then echo "Trace: $REPLAY_TRACE"; fi
if [[ -n "$PY_SCRIPT" ]]; then echo "Python Strategy: $PY_SCRIPT"; fi
echo "Impact Model: $IMPACT"
echo ""

# Run the backtest
if "$BINARY_PATH" "${CMD_ARGS[@]}"; then
    echo ""
    echo "✅ Backtest completed successfully!"
else
    echo ""
    echo "❌ Backtest failed with exit code $?"
    exit 1
fi