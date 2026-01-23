# Local Development Setup Guide (Windows)

This guide covers setting up IronCrab for local development on Windows.

> **⚠️ Windows Native Rust Build Not Fully Supported**  
> The `yellowstone-grpc-proto` dependency uses `protobuf-src` which may fail on Windows.
> **Use WSL2 for Rust builds** (see section 5). Windows works fine for UI development.

---

## Quick Start (Recommended)

For the fastest setup, use the helper scripts:

```powershell
# Start SSH tunnel to server + local UI
.\run_local.ps1 -Action start -Host ironcrab-prod

# Check status
.\run_local.ps1 -Action status

# Stop everything
.\run_local.ps1 -Action stop
```

This connects to the production server's control-plane and runs the UI locally.

---

## 1. Prerequisites

### Rust Toolchain (for building binaries)
```powershell
# Install rustup
winget install Rustlang.Rustup

# Project uses Rust 1.89.0 (see rust-toolchain.toml)
rustup show
```

### Node.js (for UI development)
```powershell
# Install Node.js 18+
winget install OpenJS.NodeJS.LTS

# Verify
node --version
npm --version
```

### Protobuf Compiler (required for Geyser gRPC)
```powershell
# Via Chocolatey (recommended)
choco install protoc -y
protoc --version
```

### Visual Studio Build Tools (for Rust on Windows)
- Download: [Visual Studio Build Tools](https://visualstudio.microsoft.com/visual-cpp-build-tools/)
- Select **"Desktop development with C++"** workload

---

## 2. UI Development (Windows Native)

The React/Vite UI runs natively on Windows:

```powershell
# Navigate to UI directory
cd ui

# Install dependencies
npm install

# Start dev server (http://localhost:5173)
npm run dev
```

Or use the helper script:
```powershell
.\run_ui.ps1
# or if PowerShell blocks npm.ps1:
.\run_ui.cmd
```

The UI connects to the control-plane API at `http://localhost:8080` by default.

---

## 3. Connecting to Server (SSH Tunnel)

To test the UI against the production control-plane:

```powershell
# Full tunnel (Control Plane + Prometheus + Grafana + Metrics)
.\run_local.ps1 -Action start -Host ironcrab-prod

# Tunnel only (no UI)
.\run_local.ps1 -Action start -Host ironcrab-prod -NoUi

# UI only (no tunnel, assumes tunnel already running)
.\run_local.ps1 -Action start -NoTunnel
```

**Manual SSH tunnel:**
```powershell
# Control Plane only
ssh -L 8080:127.0.0.1:8080 ironcrab-prod

# All ports (Control Plane + Prometheus + Grafana + Metrics)
ssh -L 8080:127.0.0.1:8080 -L 9090:127.0.0.1:9090 -L 3000:127.0.0.1:3000 -L 9801:127.0.0.1:9801 -L 9802:127.0.0.1:9802 -L 9803:127.0.0.1:9803 -L 9804:127.0.0.1:9804 ironcrab-prod
```

---

## 4. Building Rust (Windows/WSL2)

### Option A: WSL2 (Recommended)

```powershell
# Install WSL2 with Ubuntu
wsl --install -d Ubuntu
```

In WSL2:
```bash
# Install dependencies
sudo apt-get update
sudo apt-get install -y build-essential protobuf-compiler libprotobuf-dev pkg-config libssl-dev

# Navigate to project (Windows paths via /mnt/c/)
cd /mnt/c/Users/<YourUsername>/Desktop/Trading_bot/Iron_crab

# Build
cargo build --release

# Run tests
cargo test --features test_helpers

# Clippy
cargo clippy --all-targets -- -D warnings
```

### Option B: Windows Native (Limited Support)

```powershell
# May work for some targets, but Geyser gRPC often fails
cargo build --release --bin execution-engine
cargo test --features test_helpers
```

---

## 5. Local NATS (Optional)

For full local multi-process testing without server:

```powershell
# Install NATS via Chocolatey
choco install nats-server -y

# Start NATS with JetStream
nats-server -js -p 4222
```

Or via Docker:
```powershell
docker run -d --name nats -p 4222:4222 -p 8222:8222 nats:latest -js
```

---

## 6. IDE Setup

### VS Code with WSL (Recommended for Rust)

1. Install [Remote - WSL](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-wsl) extension
2. `Ctrl+Shift+P` → "WSL: Connect to WSL"
3. Open project folder from within WSL
4. Install rust-analyzer in WSL context

### VS Code Native (UI Development)

1. Install [ESLint](https://marketplace.visualstudio.com/items?itemName=dbaeumer.vscode-eslint) extension
2. Install [Prettier](https://marketplace.visualstudio.com/items?itemName=esbenp.prettier-vscode) extension
3. Open `ui/` folder for best TypeScript support

---

## 7. Common Issues

| Error | Solution |
|-------|----------|
| `protoc: command not found` | Install protobuf: `choco install protoc` |
| `ECONNREFUSED localhost:8080` | Start SSH tunnel or run control-plane locally |
| `sh is required to run configure` | Add `C:\Program Files\Git\bin` to PATH |
| WSL2 cargo slow | Use WSL2 filesystem (`~/projects/`) not `/mnt/c/` |
| `npm ERR! ERESOLVE` | Delete `node_modules` and `package-lock.json`, retry |

---

## 8. Environment Variables

For local testing, create `.env` in project root:

```env
# NATS (local or tunnel)
NATS_URL=nats://localhost:4222

# Metrics ports (match server)
METRICS_PORT_MARKET_DATA=9801
METRICS_PORT_MOMENTUM_BOT=9802
METRICS_PORT_ARB_STRATEGY=9803
METRICS_PORT_EXECUTION_ENGINE=9804
```

---

## See Also

- [RUNBOOK_PROD.md](RUNBOOK_PROD.md) - Production operations
- [CONFIG_SCHEMA.md](CONFIG_SCHEMA.md) - Hot-reload configuration
- [TARGET_ARCHITECTURE.md](TARGET_ARCHITECTURE.md) - System architecture
