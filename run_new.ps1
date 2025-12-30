# IronCrab – New Architecture Run Scripts (WIP)
#
# These scripts start the new multi-process architecture binaries.
# Per docs/TARGET_ARCHITECTURE.md:
# - market-data: Geyser ingest, MarketEvents publisher
# - momentum-bot: Strategy plane, TradeIntents generator
# - execution-engine: Single signer, Tx pipeline
#
# IMPORTANT: This is the rebuild path (WIP). Does not replace legacy default.
#
# Usage:
#   .\run_new.ps1 -Component market-data
#   .\run_new.ps1 -Component momentum-bot
#   .\run_new.ps1 -Component execution-engine
#   .\run_new.ps1 -Component all -DryRun

param(
    [Parameter(Mandatory=$true)]
    [ValidateSet("market-data", "momentum-bot", "execution-engine", "all")]
    [string]$Component,

    [string]$Config = "config.toml",
    [string]$NatsUrl = "nats://localhost:4222",
    [string]$LogDir = "trade_logs",
    [switch]$DryRun,
    [switch]$Release,
    [switch]$Build
)

$ErrorActionPreference = "Stop"

# Build if requested
if ($Build) {
    Write-Host "Building all binaries..." -ForegroundColor Cyan
    if ($Release) {
        cargo build --release --bin market-data --bin momentum-bot --bin execution-engine
    } else {
        cargo build --bin market-data --bin momentum-bot --bin execution-engine
    }
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Build failed!" -ForegroundColor Red
        exit 1
    }
}

$targetDir = if ($Release) { "target/release" } else { "target/debug" }
$commonArgs = @("--config", $Config, "--nats-url", $NatsUrl, "--log-dir", $LogDir)
if ($DryRun) { $commonArgs += "--dry-run" }

function Start-Component {
    param([string]$Name, [int]$MetricsPort, [string[]]$ExtraArgs = @())

    $exe = Join-Path $targetDir "$Name.exe"
    if (-not (Test-Path $exe)) {
        $exe = Join-Path $targetDir $Name
    }

    if (-not (Test-Path $exe)) {
        Write-Host "Binary not found: $exe (run with -Build)" -ForegroundColor Red
        return
    }

    $args = $commonArgs + @("--metrics-port", $MetricsPort) + $ExtraArgs
    Write-Host "Starting $Name on port $MetricsPort..." -ForegroundColor Green
    Write-Host "  $exe $($args -join ' ')" -ForegroundColor Gray

    if ($Component -eq "all") {
        # Start in background
        Start-Process -FilePath $exe -ArgumentList $args -NoNewWindow
    } else {
        # Start in foreground
        & $exe @args
    }
}

switch ($Component) {
    "market-data" {
        Start-Component -Name "market-data" -MetricsPort 9801
    }
    "momentum-bot" {
        Start-Component -Name "momentum-bot" -MetricsPort 9802
    }
    "execution-engine" {
        $extraArgs = @()
        if ($DryRun) { $extraArgs += "--simulate-only" }
        Start-Component -Name "execution-engine" -MetricsPort 9803 -ExtraArgs $extraArgs
    }
    "all" {
        Write-Host "Starting all components (Ctrl+C to stop)..." -ForegroundColor Cyan
        Write-Host ""
        Write-Host "Components:" -ForegroundColor Yellow
        Write-Host "  market-data     -> http://localhost:9801/metrics"
        Write-Host "  momentum-bot    -> http://localhost:9802/metrics"
        Write-Host "  execution-engine -> http://localhost:9803/metrics"
        Write-Host ""

        Start-Component -Name "market-data" -MetricsPort 9801
        Start-Sleep -Seconds 1
        Start-Component -Name "momentum-bot" -MetricsPort 9802
        Start-Sleep -Seconds 1
        Start-Component -Name "execution-engine" -MetricsPort 9803

        Write-Host ""
        Write-Host "All components started. Check logs in $LogDir/" -ForegroundColor Green
        Write-Host "Press Ctrl+C to stop all..." -ForegroundColor Gray

        # Wait for interrupt
        while ($true) { Start-Sleep -Seconds 60 }
    }
}
