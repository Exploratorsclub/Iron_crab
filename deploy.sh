#!/bin/bash
# Deployment script (updated): IronCrab Multi-Process Architecture
#
# Default behavior: deploy the new multi-process system via deploy_new.sh.
# Legacy monolith deploy is still available via: ./deploy.sh --legacy

set -e

if [[ "${1:-}" == "--legacy" ]]; then
    shift

    echo "Starting LEGACY (monolith) deployment..."

    echo "Pulling latest code from GitHub (main)..."
    git pull origin main

    if [ ! -f "my_config.server.toml" ]; then
        echo "Config file not found, copying from example..."
        cp config.example.toml my_config.server.toml
        echo "Please edit my_config.server.toml before restarting!"
        exit 1
    fi

    echo "Building legacy binaries..."
    cargo build --release --no-default-features --bin ironcrab
    cargo build --release --bin sell_all

    echo "Stopping ironcrab service..."
    sudo systemctl stop ironcrab

    echo "Installing legacy binary..."
    sudo cp target/release/ironcrab /usr/local/bin/ironcrab
    sudo chmod +x /usr/local/bin/ironcrab

    echo "Starting ironcrab service..."
    sudo systemctl start ironcrab

    sleep 2
    sudo systemctl status ironcrab --no-pager -l

    echo ""
    echo "Legacy deployment complete."
    echo "Logs:    journalctl -u ironcrab -f"
    echo "Metrics: http://localhost:9898/metrics"
    exit 0
fi

echo "Deploying NEW multi-process system..."
exec ./deploy_new.sh "$@"
