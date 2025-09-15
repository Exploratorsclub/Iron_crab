#!/usr/bin/env pwsh
#
# IronCrab Run Script for Windows
# Starts the main IronCrab trading bot
#

param(
    [switch]$Release,
    [string]$Config = "config.example.toml",
    [switch]$Build,
    [string]$Features = "",
    [switch]$Help
)

if ($Help) {
    Write-Host @"
IronCrab Run Script

Usage: .\run.ps1 [options]

Options:
    -Release        Use release build (must be built first)
    -Config <path>  Path to configuration file (default: config.example.toml)
    -Build          Build before running
    -Features       Features to build with (if -Build is used)
    -Help           Show this help message

Examples:
    .\run.ps1                                           # Run debug build with default config
    .\run.ps1 -Release                                  # Run release build
    .\run.ps1 -Config "my_config.toml"                  # Run with custom config
    .\run.ps1 -Build -Release                           # Build release and run
    .\run.ps1 -Build -Features "python,notify_watch"    # Build with features and run

Note: Make sure you have a valid configuration file before running.
See config.example.toml for reference.
"@
    exit 0
}

Write-Host "=== IronCrab Run Script ===" -ForegroundColor Cyan

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
        Write-Host "Build failed, cannot run." -ForegroundColor Red
        exit $LASTEXITCODE
    }
}

# Determine binary path
$targetDir = if ($Release) { "target\release" } else { "target\debug" }
$binaryPath = Join-Path $targetDir "ironcrab.exe"

# Check if binary exists
if (!(Test-Path $binaryPath)) {
    Write-Host "Error: Binary not found at $binaryPath" -ForegroundColor Red
    Write-Host "Please run with -Build flag or run .\build.ps1 first" -ForegroundColor Yellow
    exit 1
}

Write-Host "Starting IronCrab Trading Bot..." -ForegroundColor Green
Write-Host "Binary: $binaryPath" -ForegroundColor Gray
Write-Host "Config: $Config" -ForegroundColor Gray
Write-Host "Metrics will be available at: http://localhost:9898/metrics" -ForegroundColor Blue
Write-Host ""
Write-Host "Press Ctrl+C to stop the bot" -ForegroundColor Yellow
Write-Host "=========================" -ForegroundColor Cyan

# Run the binary
try {
    & $binaryPath --config $Config
} catch {
    Write-Host "`n❌ Failed to start IronCrab: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}