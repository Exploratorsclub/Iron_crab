# IronCrab UI runner (Windows)
# - Starts the Vite dev server for `ui/`
# - Assumes you access the server-side control-plane via SSH port forwarding

$ErrorActionPreference = 'Stop'

$RepoRoot = Split-Path -Parent $PSCommandPath
$UiDir = Join-Path $RepoRoot 'ui'

Write-Host ""
Write-Host "IronCrab UI (local)" -ForegroundColor Cyan
Write-Host "" 
Write-Host "Prerequisite: SSH tunnel to the server control-plane:" -ForegroundColor Yellow
Write-Host "  ssh -L 8080:127.0.0.1:8080 ironcrab@<server>" 
Write-Host "" 
Write-Host "Then the UI will call /api/* and Vite proxies to http://127.0.0.1:8080" -ForegroundColor DarkGray
Write-Host ""

if (-not (Test-Path $UiDir)) {
  throw "UI folder not found: $UiDir"
}

# Ensure Node.js is on PATH (some VS Code terminals don't inherit it)
$nodeDir = 'C:\Program Files\nodejs'
if (Test-Path (Join-Path $nodeDir 'node.exe')) {
  if (($env:Path -split ';') -notcontains $nodeDir) {
    $env:Path = "$nodeDir;" + $env:Path
  }
}

# Resolve npm (prefer npm.cmd to avoid PowerShell execution-policy blocking npm.ps1)
$npmCmd = (Get-Command npm.cmd -ErrorAction SilentlyContinue)?.Source
if (-not $npmCmd) {
  $fallbackNpm = 'C:\Program Files\nodejs\npm.cmd'
  if (Test-Path $fallbackNpm) { $npmCmd = $fallbackNpm }
}
if (-not $npmCmd) {
  $npmCmd = (Get-Command npm -ErrorAction SilentlyContinue)?.Source
}

if (-not $npmCmd) {
  Write-Host "npm not found. Install Node.js LTS (includes npm) and open a NEW terminal:" -ForegroundColor Red
  Write-Host "  winget install -e --id OpenJS.NodeJS.LTS" 
  exit 1
}

Push-Location $UiDir
try {
  Write-Host "Using npm: $npmCmd" -ForegroundColor DarkGray

  if (-not (Test-Path (Join-Path $UiDir 'node_modules'))) {
    Write-Host "Installing UI dependencies..." -ForegroundColor Cyan
    & $npmCmd install
  }

  Write-Host "Starting UI dev server..." -ForegroundColor Cyan
  & $npmCmd run dev
}
finally {
  Pop-Location
}
