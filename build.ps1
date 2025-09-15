#!/usr/bin/env pwsh
# 
# IronCrab Build Script for Windows
# Builds the main binary and all additional binaries
#

param(
    [switch]$Release,
    [switch]$Features,
    [string]$FeatureList = "",
    [switch]$Help
)

if ($Help) {
    Write-Host @"
IronCrab Build Script

Usage: .\build.ps1 [options]

Options:
    -Release        Build in release mode (optimized)
    -Features       Enable additional features
    -FeatureList    Comma-separated list of features (python,python_ipc,jito,notify_watch,test_helpers)
    -Help           Show this help message

Examples:
    .\build.ps1                              # Debug build
    .\build.ps1 -Release                     # Release build  
    .\build.ps1 -Features -FeatureList "python,notify_watch"  # Build with features
    .\build.ps1 -Release -Features -FeatureList "python"      # Release build with Python support

Available binaries after build:
    - ironcrab.exe          (main trading bot)
    - backtest_driver.exe   (backtesting engine)
    - recorder.exe          (data recorder)
    - latency_stress.exe    (performance testing)
    - raydium_pools.exe     (pool analysis)
"@
    exit 0
}

Write-Host "=== IronCrab Build Script ===" -ForegroundColor Cyan
Write-Host "Building IronCrab Trading Bot..." -ForegroundColor Yellow

# Check if Cargo is available
if (!(Get-Command cargo -ErrorAction SilentlyContinue)) {
    Write-Host "Error: Cargo not found. Please install Rust: https://rustup.rs/" -ForegroundColor Red
    exit 1
}

# Build command construction
$buildCmd = "cargo build"

if ($Release) {
    $buildCmd += " --release"
    Write-Host "Building in RELEASE mode..." -ForegroundColor Green
} else {
    Write-Host "Building in DEBUG mode..." -ForegroundColor Yellow
}

if ($Features -and $FeatureList) {
    $buildCmd += " --features $FeatureList"
    Write-Host "Enabled features: $FeatureList" -ForegroundColor Blue
}

# Execute build
Write-Host "Running: $buildCmd" -ForegroundColor Gray
try {
    Invoke-Expression $buildCmd
    if ($LASTEXITCODE -eq 0) {
        Write-Host "`n✅ Build completed successfully!" -ForegroundColor Green
        
        # Show built binaries
        $targetDir = if ($Release) { "target\release" } else { "target\debug" }
        Write-Host "`nBuilt binaries in $targetDir:" -ForegroundColor Cyan
        
        $binaries = @("ironcrab.exe", "backtest_driver.exe", "recorder.exe", "latency_stress.exe", "raydium_pools.exe")
        foreach ($binary in $binaries) {
            $path = Join-Path $targetDir $binary
            if (Test-Path $path) {
                $size = (Get-Item $path).Length
                $sizeKB = [math]::Round($size / 1024, 1)
                Write-Host "  ✓ $binary ($sizeKB KB)" -ForegroundColor Green
            }
        }
    } else {
        Write-Host "`n❌ Build failed with exit code $LASTEXITCODE" -ForegroundColor Red
        exit $LASTEXITCODE
    }
} catch {
    Write-Host "`n❌ Build failed: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}