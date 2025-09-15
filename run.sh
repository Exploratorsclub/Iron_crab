#!/bin/bash
#
# IronCrab Run Script for Unix/Linux/macOS
# Starts the main IronCrab trading bot
#

set -e

RELEASE=""
CONFIG="config.example.toml"
BUILD=""
FEATURES=""
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
IronCrab Run Script

Usage: ./run.sh [options]

Options:
    --release, -r           Use release build (must be built first)
    --config, -c <path>     Path to configuration file (default: config.example.toml)
    --build, -b             Build before running
    --features, -f <list>   Features to build with (if --build is used)
    --help, -h              Show this help message

Examples:
    ./run.sh                                        # Run debug build with default config
    ./run.sh --release                              # Run release build
    ./run.sh --config "my_config.toml"              # Run with custom config
    ./run.sh --build --release                      # Build release and run
    ./run.sh --build --features "python,notify_watch"  # Build with features and run

Note: Make sure you have a valid configuration file before running.
See config.example.toml for reference.
EOF
    exit 0
fi

echo "=== IronCrab Run Script ==="

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
BINARY_PATH="$TARGET_DIR/ironcrab"

# Check if binary exists
if [[ ! -f "$BINARY_PATH" ]]; then
    echo "Error: Binary not found at $BINARY_PATH"
    echo "Please run with --build flag or run ./build.sh first"
    exit 1
fi

echo "Starting IronCrab Trading Bot..."
echo "Binary: $BINARY_PATH"
echo "Config: $CONFIG"
echo "Metrics will be available at: http://localhost:9898/metrics"
echo ""
echo "Press Ctrl+C to stop the bot"
echo "========================="

# Run the binary
"$BINARY_PATH" --config "$CONFIG"