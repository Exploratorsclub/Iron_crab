#!/bin/bash
# Deploy script for IronCrab Multi-Process Architecture
# Usage: ./deploy_new.sh [--skip-build] [--component NAME]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

SKIP_BUILD=false
COMPONENT=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-build) SKIP_BUILD=true; shift ;;
        --component) COMPONENT="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# Components
COMPONENTS=("market-data" "momentum-bot" "arb-strategy" "execution-engine")
SYSTEMD_DIR="/etc/systemd/system"

# -----------------------------------------------------------------------------
# 1. Git Pull
# -----------------------------------------------------------------------------
log_info "Pulling latest changes from GitHub..."
git pull origin architecture-rebuild

# -----------------------------------------------------------------------------
# 2. Build Rust Binaries
# -----------------------------------------------------------------------------
if [ "$SKIP_BUILD" = false ]; then
    log_info "Building release binaries (this may take a few minutes)..."
    
    # Build all binaries with nats feature
    cargo build --release --features nats \
        --bin market-data \
        --bin momentum-bot \
            --bin arb-strategy \
        --bin execution-engine
    
    log_info "Build complete."
else
    log_warn "Skipping build (--skip-build flag set)"
fi

# -----------------------------------------------------------------------------
# 3. Setup Python venv for Control Plane
# -----------------------------------------------------------------------------
log_info "Setting up Python virtual environment for control-plane..."
if [ ! -d ".venv" ]; then
    python3 -m venv .venv
fi
.venv/bin/pip install -q -r control_plane/requirements.txt

# -----------------------------------------------------------------------------
# 4. Install systemd services
# -----------------------------------------------------------------------------
install_service() {
    local name=$1
    log_info "Installing ${name}.service..."
    sudo cp "docs/systemd/${name}.service" "$SYSTEMD_DIR/"
}

log_info "Installing systemd services..."
install_service "market-data"
install_service "momentum-bot"
install_service "arb-strategy"
install_service "execution-engine"
install_service "control-plane"
sudo cp "docs/systemd/ironcrab.target" "$SYSTEMD_DIR/"

sudo systemctl daemon-reload

# -----------------------------------------------------------------------------
# 5. Restart services (or specific component)
# -----------------------------------------------------------------------------
if [ -n "$COMPONENT" ]; then
    log_info "Restarting only: $COMPONENT"
    sudo systemctl restart "$COMPONENT"
else
    log_info "Restarting all IronCrab services..."
    sudo systemctl stop ironcrab.target 2>/dev/null || true
    sleep 1
    sudo systemctl start ironcrab.target
fi

# -----------------------------------------------------------------------------
# 6. Enable services for auto-start
# -----------------------------------------------------------------------------
log_info "Enabling services for auto-start..."
sudo systemctl enable ironcrab.target

# -----------------------------------------------------------------------------
# 7. Status check
# -----------------------------------------------------------------------------
sleep 2
log_info "Service status:"
echo ""
for svc in market-data momentum-bot execution-engine control-plane; do
    status=$(systemctl is-active "$svc" 2>/dev/null || echo "inactive")
    if [ "$status" = "active" ]; then
        echo -e "  ${GREEN}●${NC} $svc: $status"
    else
        echo -e "  ${RED}●${NC} $svc: $status"
    fi
done

status=$(systemctl is-active "arb-strategy" 2>/dev/null || echo "inactive")
if [ "$status" = "active" ]; then
    echo -e "  ${GREEN}●${NC} arb-strategy: $status"
else
    echo -e "  ${RED}●${NC} arb-strategy: $status"
fi

echo ""
log_info "Deployment complete!"
echo ""
echo "📊 Metrics endpoints:"
echo "   - market-data:      http://localhost:9801/metrics"
echo "   - momentum-bot:     http://localhost:9802/metrics"
echo "   - arb-strategy:     http://localhost:9803/metrics"
echo "   - execution-engine: http://localhost:9804/metrics"
echo ""
echo "🔧 Control Plane:      http://localhost:8080"
echo ""
echo "📜 View logs:"
echo "   journalctl -u market-data -f"
echo "   journalctl -u momentum-bot -f"
echo "   journalctl -u arb-strategy -f"
echo "   journalctl -u execution-engine -f"
echo "   journalctl -u control-plane -f"
echo ""
echo "🛑 Stop all:  sudo systemctl stop ironcrab.target"
echo "▶️  Start all: sudo systemctl start ironcrab.target"
