# Local Development Setup Guide (Windows)

To run tests and build the project locally on Windows, you need to ensure your environment is correctly configured.

> **Note**: Production runs on Debian Linux (same server as the validator). 
> This guide is for local development/testing only.

## 1. Install Prerequisites

### Rust Toolchain
```powershell
# Install rustup if not already installed
winget install Rustlang.Rustup

# Project uses Rust 1.89.0 (see rust-toolchain.toml)
rustup show
```

### Visual Studio Build Tools
Ensure you have **Visual Studio Build Tools** installed with the **"Desktop development with C++"** workload.
- Download: [Visual Studio Build Tools](https://visualstudio.microsoft.com/visual-cpp-build-tools/)
- During installation, select "Desktop development with C++".

### Protobuf Compiler (REQUIRED for Geyser gRPC)
The `yellowstone-grpc-proto` dependency requires `protoc`:

**Option A: Via Chocolatey (recommended)**
```powershell
# Install Chocolatey if not installed: https://chocolatey.org/install
choco install protoc -y

# Verify
protoc --version
```

**Option B: Manual Installation**
1. Download latest release from [protobuf releases](https://github.com/protocolbuffers/protobuf/releases)
2. Download `protoc-XX.X-win64.zip`
3. Extract to `C:\protoc`
4. Add `C:\protoc\bin` to your PATH

### Python 3.x (Required for `pyo3` feature)
Only needed if building with `--features python`:
1. Download Python 3.10+ from [python.org](https://www.python.org/downloads/windows/)
2. **Important:** Check **"Add Python to PATH"** during installation
3. Verify: `python --version`

### Git Bash (Required for some build scripts)
1. Download Git for Windows from [git-scm.com](https://git-scm.com/download/win)
2. Add `C:\Program Files\Git\bin` to your PATH

## 2. Configure Environment Variables (PowerShell)

Add required tools to PATH (for current session):
```powershell
$env:PATH += ";C:\Program Files\Git\bin"
$env:PATH += ";C:\protoc\bin"  # If manual protoc install
```

To make permanent: System Properties → Environment Variables → Edit `Path`

## 3. Building & Testing

```powershell
# Build (debug)
cargo build

# Build (release)
cargo build --release

# Run tests
cargo test

# Run tests with test helpers
cargo test --features test_helpers

# Clippy (linter)
cargo clippy --all-targets -- -D warnings

# Format check
cargo fmt -- --check
```

## 4. Common Build Errors

| Error | Solution |
|-------|----------|
| `protoc: command not found` | Install protobuf compiler (see above) |
| `cannot find -lprotobuf` | Install protobuf via Chocolatey or manual |
| `sh is required to run configure` | Add `C:\Program Files\Git\bin` to PATH |
| `no Python 3.x interpreter found` | Install Python with PATH option |
| `linker link.exe not found` | Install VS Build Tools with C++ workload |
| `LINK : fatal error LNK1181` | Missing C++ libs, reinstall VS Build Tools |

## 5. Alternative: Build on Linux/WSL

If Windows build issues persist, use WSL2:

```bash
# In WSL2 (Ubuntu)
sudo apt-get update
sudo apt-get install -y build-essential protobuf-compiler libprotobuf-dev

# Clone and build
cd /mnt/c/Users/rober/Iron_crab
cargo build --release
```

This matches the CI environment exactly.
