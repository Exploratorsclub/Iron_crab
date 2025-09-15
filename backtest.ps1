#!/usr/bin/env pwsh
#
# IronCrab Backtest Script for Windows  
# Runs backtesting with various options
#

param(
    [switch]$Release,
    [string]$Config = "config.example.toml",
    [switch]$Build,
    [string]$Features = "",
    [string]$ReplayTrace = "",
    [string]$ReplayStart = "",
    [string]$ReplayEnd = "",
    [int]$ReplaySlotMs = 400,
    [string]$ReplaySeed = "",
    [string]$Impact = "cpmm",
    [int]$ImpactExtraFeeBps = 0,
    [int]$ImpactNoiseMeanBps = 0,
    [int]$ImpactNoiseStdBps = 0,
    [string]$PyScript = "",
    [string]$ValidateLiveCsv = "",
    [switch]$Help
)

if ($Help) {
    Write-Host @"
IronCrab Backtest Script

Usage: .\backtest.ps1 [options]

Options:
    -Release                    Use release build
    -Config <path>              Configuration file (default: config.example.toml)
    -Build                      Build before running
    -Features <list>            Features to build with (if -Build is used)
    -ReplayTrace <path>         Path to replay trace file (.jsonl.gz)
    -ReplayStart <slot>         Start slot for replay
    -ReplayEnd <slot>           End slot for replay  
    -ReplaySlotMs <ms>          Milliseconds per slot (default: 400)
    -ReplaySeed <seed>          Random seed for deterministic replay
    -Impact <model>             Impact model: cpmm|clmm|none (default: cpmm)
    -ImpactExtraFeeBps <bps>    Additional fee in basis points
    -ImpactNoiseMeanBps <bps>   Noise mean in basis points
    -ImpactNoiseStdBps <bps>    Noise standard deviation in basis points
    -PyScript <path>            Python strategy script to use
    -ValidateLiveCsv <path>     Validate against live CSV trades
    -Help                       Show this help message

Examples:
    .\backtest.ps1 -ReplayTrace "data\trace.jsonl.gz" -ReplayStart 250000000 -ReplayEnd 250001000
    .\backtest.ps1 -Build -Release -Impact "clmm" -ImpactExtraFeeBps 10
    .\backtest.ps1 -PyScript "strategies\sample.py" -ReplayTrace "data\trace.jsonl.gz"
    .\backtest.ps1 -ValidateLiveCsv "trades.csv" -ReplayTrace "data\trace.jsonl.gz"

Note: You need to have recorded trace data to run backtests.
Use the recorder binary to capture live data first.
"@
    exit 0
}

Write-Host "=== IronCrab Backtest Script ===" -ForegroundColor Cyan

# Check if config file exists
if (!(Test-Path $Config)) {
    Write-Host "Error: Configuration file '$Config' not found!" -ForegroundColor Red
    Write-Host "Please create a configuration file based on config.example.toml" -ForegroundColor Yellow
    exit 1
}

# Build if requested
if ($Build) {
    Write-Host "Building IronCrab..." -ForegroundColor Yellow
    $buildArgs = @()
    if ($Release) { $buildArgs += "-Release" }
    if ($Features) { $buildArgs += "-Features"; $buildArgs += "-FeatureList"; $buildArgs += $Features }
    
    & .\build.ps1 @buildArgs
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Build failed, cannot run backtest." -ForegroundColor Red
        exit $LASTEXITCODE
    }
}

# Determine binary path
$targetDir = if ($Release) { "target\release" } else { "target\debug" }
$binaryPath = Join-Path $targetDir "backtest_driver.exe"

# Check if binary exists
if (!(Test-Path $binaryPath)) {
    Write-Host "Error: Backtest binary not found at $binaryPath" -ForegroundColor Red
    Write-Host "Please run with -Build flag or run .\build.ps1 first" -ForegroundColor Yellow
    exit 1
}

# Build command arguments
$cmdArgs = @("--config", $Config)

if ($ReplayTrace) {
    if (!(Test-Path $ReplayTrace)) {
        Write-Host "Error: Replay trace file '$ReplayTrace' not found!" -ForegroundColor Red
        exit 1
    }
    $cmdArgs += @("--replay-trace", $ReplayTrace)
}

if ($ReplayStart) { $cmdArgs += @("--replay-start", $ReplayStart) }
if ($ReplayEnd) { $cmdArgs += @("--replay-end", $ReplayEnd) }
if ($ReplaySlotMs -ne 400) { $cmdArgs += @("--replay-slot-ms", $ReplaySlotMs) }
if ($ReplaySeed) { $cmdArgs += @("--replay-seed", $ReplaySeed) }
if ($Impact -ne "cpmm") { $cmdArgs += @("--impact", $Impact) }
if ($ImpactExtraFeeBps -gt 0) { $cmdArgs += @("--impact-extra-fee-bps", $ImpactExtraFeeBps) }
if ($ImpactNoiseMeanBps -gt 0) { $cmdArgs += @("--impact-noise-mean-bps", $ImpactNoiseMeanBps) }
if ($ImpactNoiseStdBps -gt 0) { $cmdArgs += @("--impact-noise-std-bps", $ImpactNoiseStdBps) }

if ($PyScript) {
    if (!(Test-Path $PyScript)) {
        Write-Host "Error: Python script '$PyScript' not found!" -ForegroundColor Red
        exit 1
    }
    $cmdArgs += @("--py-script", $PyScript)
}

if ($ValidateLiveCsv) {
    if (!(Test-Path $ValidateLiveCsv)) {
        Write-Host "Error: Live CSV file '$ValidateLiveCsv' not found!" -ForegroundColor Red
        exit 1
    }
    $cmdArgs += @("--validate-live-csv", $ValidateLiveCsv)
}

Write-Host "Starting IronCrab Backtest..." -ForegroundColor Green
Write-Host "Binary: $binaryPath" -ForegroundColor Gray
Write-Host "Config: $Config" -ForegroundColor Gray
if ($ReplayTrace) { Write-Host "Trace: $ReplayTrace" -ForegroundColor Gray }
if ($PyScript) { Write-Host "Python Strategy: $PyScript" -ForegroundColor Gray }
Write-Host "Impact Model: $Impact" -ForegroundColor Blue
Write-Host ""

# Run the backtest
try {
    & $binaryPath @cmdArgs
    if ($LASTEXITCODE -eq 0) {
        Write-Host "`n✅ Backtest completed successfully!" -ForegroundColor Green
    } else {
        Write-Host "`n❌ Backtest failed with exit code $LASTEXITCODE" -ForegroundColor Red
        exit $LASTEXITCODE
    }
} catch {
    Write-Host "`n❌ Failed to start backtest: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}