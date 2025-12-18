#!/bin/bash
# Deployment script for Iron_crab arbitrage bot
# Usage: ./deploy.sh

set -e  # Exit on error

echo "🚀 Starting deployment..."

# 1. Pull latest changes
echo "📥 Pulling latest code from GitHub..."
git pull origin solana3x_clean

# 2. Copy config if needed
if [ ! -f "my_config.server.toml" ]; then
    echo "⚠️  Config file not found, copying from example..."
    cp config.example.toml my_config.server.toml
    echo "⚠️  Please edit my_config.server.toml before restarting!"
    exit 1
fi

# 3. Build release binaries
echo "🔨 Building release binaries (this may take a few minutes)..."
# Build main bot
cargo build --release --no-default-features --bin ironcrab
# Build sell_all tool
echo "🔨 Building sell_all tool..."
cargo build --release --bin sell_all

# 4. Stop service
echo "⏸️  Stopping ironcrab service..."
sudo systemctl stop ironcrab

# 5. Copy binary to production location
echo "📦 Installing binary..."
sudo cp target/release/ironcrab /usr/local/bin/ironcrab
sudo chmod +x /usr/local/bin/ironcrab

# 6. Start service
echo "▶️  Starting ironcrab service..."
sudo systemctl start ironcrab

# 7. Check status
echo "✅ Checking service status..."
sleep 2
sudo systemctl status ironcrab --no-pager -l

echo ""
echo "🎉 Deployment complete!"
echo ""
echo "📊 View logs: journalctl -u ironcrab -f"
echo "📈 Metrics: http://localhost:9898/metrics"
echo "🔍 Status: systemctl status ironcrab"
