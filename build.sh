#!/bin/bash
#
# IronCrab Build Script for Unix/Linux/macOS
# Builds the main binary and all additional binaries
#

set -e

RELEASE=""
FEATURES=""
FEATURE_LIST=""
HELP=""

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --release|-r)
            RELEASE="--release"
            shift
            ;;
        --features|-f)
            FEATURES="--features"
            shift
            ;;
        --feature-list)
            FEATURE_LIST="$2"
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
IronCrab Build Script

Usage: ./build.sh [options]

Options:
    --release, -r               Build in release mode (optimized)
    --features, -f              Enable additional features
    --feature-list <features>   Comma-separated list of features (python,python_ipc,jito,notify_watch,test_helpers)
    --help, -h                  Show this help message

Examples:
    ./build.sh                                      # Debug build
    ./build.sh --release                            # Release build  
    ./build.sh --features --feature-list "python,notify_watch"  # Build with features
    ./build.sh --release --features --feature-list "python"     # Release build with Python support

Available binaries after build:
    - ironcrab              (main trading bot)
    - backtest_driver       (backtesting engine)
    - recorder              (data recorder)
    - latency_stress        (performance testing)
    - raydium_pools         (pool analysis)
EOF
    exit 0
fi

echo "=== IronCrab Build Script ==="
echo "Building IronCrab Trading Bot..."

# Check if Cargo is available
if ! command -v cargo &> /dev/null; then
    echo "Error: Cargo not found. Please install Rust: https://rustup.rs/"
    exit 1
fi

# Build command construction
BUILD_CMD="cargo build"

if [[ -n "$RELEASE" ]]; then
    BUILD_CMD="$BUILD_CMD $RELEASE"
    echo "Building in RELEASE mode..."
else
    echo "Building in DEBUG mode..."
fi

if [[ -n "$FEATURES" && -n "$FEATURE_LIST" ]]; then
    BUILD_CMD="$BUILD_CMD $FEATURES $FEATURE_LIST"
    echo "Enabled features: $FEATURE_LIST"
fi

# Execute build
echo "Running: $BUILD_CMD"
if $BUILD_CMD; then
    echo ""
    echo "✅ Build completed successfully!"
    
    # Show built binaries
    if [[ -n "$RELEASE" ]]; then
        TARGET_DIR="target/release"
    else
        TARGET_DIR="target/debug"
    fi
    
    echo ""
    echo "Built binaries in $TARGET_DIR:"
    
    BINARIES=("ironcrab" "backtest_driver" "recorder" "latency_stress" "raydium_pools")
    for binary in "${BINARIES[@]}"; do
        binary_path="$TARGET_DIR/$binary"
        if [[ -f "$binary_path" ]]; then
            size=$(du -k "$binary_path" | cut -f1)
            echo "  ✓ $binary (${size} KB)"
        fi
    done
else
    echo ""
    echo "❌ Build failed"
    exit 1
fi