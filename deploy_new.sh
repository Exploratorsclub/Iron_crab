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

ensure_cargo() {
    if command -v cargo >/dev/null 2>&1; then
        return 0
    fi

    # Non-interactive shells often don't load Rust env. Try to load it.
    if [ -f "$HOME/.cargo/env" ]; then
        # shellcheck disable=SC1090
        source "$HOME/.cargo/env"
    fi

    export PATH="$HOME/.cargo/bin:$PATH"

    if ! command -v cargo >/dev/null 2>&1; then
        log_error "cargo not found in PATH. Install Rust or source ~/.cargo/env"
        exit 1
    fi
}

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
SYSTEMD_SRC_DIR="$SCRIPT_DIR/docs/systemd"

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

    ensure_cargo
    
    # Build all binaries (NATS is now required, not a feature)
    # Note: Geyser (yellowstone-grpc) is only available on Linux, enabled automatically
    log_info "Building market-data..."
    cargo build --release --bin market-data
    
    log_info "Building momentum-bot..."
    cargo build --release --bin momentum-bot
    
    log_info "Building arb-strategy..."
    cargo build --release --bin arb-strategy
    
    log_info "Building execution-engine..."
    cargo build --release --bin execution-engine

    log_info "Building position-manager (PA-6a shadow, optional service)..."
    cargo build --release --bin position-manager
    
    log_info "All binaries built successfully."
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
# 3.5. Configure Wallet Pubkey for Position Reconciliation
# -----------------------------------------------------------------------------
log_info "Configuring wallet pubkey for position reconciliation..."
# Prefer explicit keypair path if provided; otherwise try common locations.
KEYPAIR_PATH=""
if [ -n "${SOLANA_KEYPAIR_PATH:-}" ]; then
    KEYPAIR_PATH="$SOLANA_KEYPAIR_PATH"
elif [ -f "$HOME/.config/solana/id.json" ]; then
    KEYPAIR_PATH="$HOME/.config/solana/id.json"
elif [ -f "/home/sol/.config/solana/id.json" ]; then
    KEYPAIR_PATH="/home/sol/.config/solana/id.json"
fi

if [ -n "$KEYPAIR_PATH" ] && [ -f "$KEYPAIR_PATH" ]; then
    # Ensure solders is installed for keypair handling
    .venv/bin/pip install -q solders >/dev/null 2>&1 || true
    
    WALLET_PUBKEY=$(.venv/bin/python3 <<EOF 2>/dev/null || echo ""
import json
from solders.keypair import Keypair
with open('$KEYPAIR_PATH') as f:
    kp = Keypair.from_bytes(bytes(json.load(f)))
print(str(kp.pubkey()))
EOF
)
    
    if [ -n "$WALLET_PUBKEY" ]; then
        log_info "Wallet pubkey: $WALLET_PUBKEY"
        sudo mkdir -p /etc/systemd/system/market-data.service.d/
        sudo tee /etc/systemd/system/market-data.service.d/wallet.conf > /dev/null <<EOF
[Service]
Environment="IRONCRAB_WALLET_PUBKEY=$WALLET_PUBKEY"
EOF
        log_info "market-data will publish wallet balance snapshot at startup"
    else
        log_warn "Could not extract wallet pubkey (position reconciliation disabled)"
    fi
else
    log_warn "No wallet keypair found (position reconciliation disabled)"
fi

# -----------------------------------------------------------------------------
# 4. Install systemd services
# -----------------------------------------------------------------------------
install_service() {
    local name=$1
    log_info "Installing ${name}.service..."
    sudo cp "$SYSTEMD_SRC_DIR/${name}.service" "$SYSTEMD_DIR/"
}

log_info "Installing systemd services..."
install_service "market-data"
install_service "momentum-bot"
install_service "arb-strategy"
install_service "execution-engine"
install_service "control-plane"
install_service "trades-server"
sudo cp "$SYSTEMD_SRC_DIR/ironcrab.target" "$SYSTEMD_DIR/"

# Cleanup legacy services that conflict with new ones
if systemctl list-unit-files trades_server.service >/dev/null 2>&1; then
    log_warn "Removing legacy trades_server.service (replaced by trades-server.service)"
    sudo systemctl stop trades_server.service 2>/dev/null || true
    sudo systemctl disable trades_server.service 2>/dev/null || true
    sudo rm -f "$SYSTEMD_DIR/trades_server.service"
fi

# Kill any manually started trades_server processes (nohup, etc.) that block port 9899
if pgrep -f 'trades_server.py' >/dev/null 2>&1; then
    log_warn "Killing manually started trades_server.py processes..."
    pkill -f 'trades_server.py' || true
    sleep 1
fi

sudo systemctl daemon-reload

# -----------------------------------------------------------------------------
# 5. Restart services (or specific component)
# -----------------------------------------------------------------------------
if [ -n "$COMPONENT" ]; then
    log_info "Restarting only: $COMPONENT"
    sudo systemctl restart "$COMPONENT"
else
    # systemctl start ironcrab.target does NOT restart already-running Wants= units;
    # after cargo build replaces binaries, stale processes keep the old inode until restarted.
    log_info "Restarting all IronCrab services..."
    SERVICES=(market-data momentum-bot arb-strategy execution-engine control-plane trades-server)
    for svc in "${SERVICES[@]}"; do
        log_info "Restarting $svc..."
        sudo systemctl restart "$svc"
    done
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
for svc in market-data momentum-bot execution-engine control-plane trades-server; do
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
echo "📈 Trades API:         http://localhost:9899/trades (Grafana Infinity)"
echo ""
echo "📜 View logs:"
echo "   journalctl -u market-data -f"
echo "   journalctl -u momentum-bot -f"
echo "   journalctl -u arb-strategy -f"
echo "   journalctl -u execution-engine -f"
echo "   journalctl -u control-plane -f"
echo "   journalctl -u trades-server -f"
echo ""
echo "🛑 Stop all:  sudo systemctl stop ironcrab.target"
echo "▶️  Start all: sudo systemctl start ironcrab.target"
