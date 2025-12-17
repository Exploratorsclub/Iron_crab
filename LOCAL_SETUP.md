# Local Development Setup Guide

To run tests and build the project locally on Windows, you need to ensure your environment is correctly configured.

## 1. Install Prerequisites

### Visual Studio Build Tools
Ensure you have **Visual Studio Build Tools** installed with the **"Desktop development with C++"** workload.
- Download: [Visual Studio Build Tools](https://visualstudio.microsoft.com/visual-cpp-build-tools/)
- During installation, select "Desktop development with C++".

### Python 3.x (Required for `pyo3`)
The project uses `pyo3` which requires a Python interpreter.
1. Download Python 3.10 or newer from [python.org](https://www.python.org/downloads/windows/).
2. **Important:** During installation, check the box **"Add Python to PATH"**.
3. Verify installation in PowerShell:
   ```powershell
   python --version
   ```

### Git Bash (Required for `protobuf-src`)
The `protobuf-src` dependency requires a shell (`sh`) to build.
1. Download Git for Windows from [git-scm.com](https://git-scm.com/download/win).
2. During installation, you can use default settings.
3. You need to add the `bin` folder to your PATH so `sh.exe` is found.

## 2. Configure Environment Variables (PowerShell)

You need to add Git Bash to your PATH so the build script can find `sh`.

Run this in PowerShell to add it temporarily (for the current session):
```powershell
$env:PATH += ";C:\Program Files\Git\bin"
```

To make it permanent:
1. Search for "Edit the system environment variables" in Windows Search.
2. Click "Environment Variables".
3. Under "System variables", find `Path` and click "Edit".
4. Click "New" and add `C:\Program Files\Git\bin`.
5. Click OK.

## 3. Running Tests and Checks

Once the environment is set up, you can run the following commands in PowerShell:

### Run Tests
```powershell
# Add Git bin to path if not permanent
$env:PATH += ";C:\Program Files\Git\bin"

# Run all tests
cargo test
```

### Run Linter (Clippy)
```powershell
cargo clippy --all-targets --all-features -- -D warnings
```

### Check Formatting
```powershell
cargo fmt -- --check
```

## Troubleshooting

- **"sh is required to run configure"**: This means `sh.exe` is not in your PATH. Add `C:\Program Files\Git\bin` to your PATH.
- **"no Python 3.x interpreter found"**: Install Python and add it to your PATH.
- **"linker link.exe not found"**: Install Visual Studio Build Tools with C++ workload.
