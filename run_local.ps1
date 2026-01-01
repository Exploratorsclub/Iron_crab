<#
IronCrab local runner (Windows / PowerShell)

Goals:
- Keep the SSH tunnel stable (ServerAlive keepalives)
- Run the local UI (Vite) in a separate process, so debugging commands in your current terminal won't kill it

Usage:
  ./run_local.ps1 -Action start -Host 109.230.239.43 -User ironcrab -Port 2222
  ./run_local.ps1 -Action start -Host ironcrab-prod
  ./run_local.ps1 -Action status
  ./run_local.ps1 -Action stop

Notes:
- UI runs locally on http://localhost:5173
- Control-plane is accessed via SSH forward on http://127.0.0.1:8080
- Do NOT forward 5173 in SSH (that would conflict with local Vite)
#>

[CmdletBinding()]
param(
  [Parameter(Mandatory = $false)]
  [ValidateSet('start','stop','status')]
  [string]$Action = 'start',

  [Parameter(Mandatory = $false)]
  [Alias('Host')]
  [string]$SshHost = '109.230.239.43',

  [Parameter(Mandatory = $false)]
  [string]$User = 'ironcrab',

  [Parameter(Mandatory = $false)]
  [int]$Port = 2222,

  [Parameter(Mandatory = $false)]
  [string]$IdentityFile = '',

  [Parameter(Mandatory = $false)]
  [switch]$NoUi,

  [Parameter(Mandatory = $false)]
  [switch]$NoTunnel
)

$ErrorActionPreference = 'Stop'

$RepoRoot = Split-Path -Parent $PSCommandPath
$StateDir = Join-Path $RepoRoot '.local'
$StateFile = Join-Path $StateDir 'local_processes.json'

function Initialize-StateDir {
  if (-not (Test-Path $StateDir)) {
    New-Item -ItemType Directory -Path $StateDir | Out-Null
  }
}

function Get-LocalRunnerState {
  if (-not (Test-Path $StateFile)) { return $null }
  try {
    return Get-Content -Raw -Path $StateFile | ConvertFrom-Json
  } catch {
    return $null
  }
}

function Save-LocalRunnerState($obj) {
  Initialize-StateDir
  ($obj | ConvertTo-Json -Depth 10) | Set-Content -Path $StateFile -Encoding UTF8
}

function Test-ProcessAlive([int]$processId) {
  if ($processId -le 0) { return $false }
  try {
    Get-Process -Id $processId -ErrorAction Stop | Out-Null
    return $true
  } catch {
    return $false
  }
}

function Stop-ProcessTree([int]$processId) {
  if ($processId -le 0) { return }
  if (-not (Test-ProcessAlive $processId)) { return }
  # /T kills child processes too (important for cmd -> npm -> node)
  cmd /c "taskkill /PID $processId /T /F" | Out-Null
}

function Get-SshExe {
  $fallback = Join-Path $env:WINDIR 'System32\OpenSSH\ssh.exe'
  if (Test-Path $fallback) { return $fallback }
  $sshResolved = Get-Command ssh.exe -ErrorAction SilentlyContinue
  if ($sshResolved) { return $sshResolved.Source }
  throw 'ssh.exe not found (Windows OpenSSH).'
}

function Start-Tunnel {
  Initialize-StateDir
  $sshExe = Get-SshExe

  $hostIsIpv4 = $SshHost -match '^(?:\d{1,3}\.){3}\d{1,3}$'
  $hostLooksLikeAlias = (-not $hostIsIpv4) -and ($SshHost -notmatch '\.')

  # NOTE: $Host is a reserved automatic variable in PowerShell; use $SshHost.
  # We can only check which *script* parameters were provided here.
  $scriptBound = $script:__RunLocalBoundParameters
  $useSshConfigOnly = $hostLooksLikeAlias -and (-not $scriptBound.ContainsKey('User')) -and (-not $scriptBound.ContainsKey('Port')) -and (-not $scriptBound.ContainsKey('IdentityFile'))

  $forwards = @(
    '8080:127.0.0.1:8080',
    '3000:127.0.0.1:3000',
    '9090:127.0.0.1:9090',
    '9801:127.0.0.1:9801',
    '9802:127.0.0.1:9802',
    '9803:127.0.0.1:9803',
    '9804:127.0.0.1:9804'
  )

  $sshArgList = @(
    '-N',
    '-o', 'ServerAliveInterval=30',
    '-o', 'ServerAliveCountMax=3',
    '-o', 'ExitOnForwardFailure=yes'
  )

  if (-not $useSshConfigOnly) {
    $sshArgList += @('-p', [string]$Port)
  }

  if ((-not $useSshConfigOnly) -and $IdentityFile) {
    $sshArgList += @('-i', $IdentityFile)
  }

  foreach ($fwd in $forwards) {
    $sshArgList += @('-L', $fwd)
  }

  if ($useSshConfigOnly) {
    # Host is treated as an SSH config alias (e.g. "ironcrab-prod")
    $sshArgList += @($SshHost)
  } else {
    $sshArgList += @("$User@$SshHost")
  }

  $stdout = Join-Path $StateDir 'ssh_tunnel.stdout.log'
  $stderr = Join-Path $StateDir 'ssh_tunnel.stderr.log'

  $proc = Start-Process -FilePath $sshExe -ArgumentList $sshArgList -PassThru -NoNewWindow -RedirectStandardOutput $stdout -RedirectStandardError $stderr
  return $proc
}

function Start-Ui {
  $cmdPath = Join-Path $RepoRoot 'run_ui.cmd'
  if (-not (Test-Path $cmdPath)) {
    throw "run_ui.cmd not found at: $cmdPath"
  }

  $stdout = Join-Path $StateDir 'ui.stdout.log'
  $stderr = Join-Path $StateDir 'ui.stderr.log'

  # Start .cmd directly (spawns its own cmd.exe). Keeps UI lifetime independent.
  $proc = Start-Process -FilePath $cmdPath -WorkingDirectory $RepoRoot -PassThru -NoNewWindow -RedirectStandardOutput $stdout -RedirectStandardError $stderr
  return $proc
}

function Show-Status {
  $state = Get-LocalRunnerState
  if (-not $state) {
    Write-Host 'No local runner state found.' -ForegroundColor Yellow
    return
  }

  $sshOk = $false
  $uiOk = $false

  if ($state.ssh_pid) { $sshOk = Test-ProcessAlive ([int]$state.ssh_pid) }
  if ($state.ui_pid) { $uiOk = Test-ProcessAlive ([int]$state.ui_pid) }

  Write-Host "SSH tunnel PID: $($state.ssh_pid)  alive=$sshOk" -ForegroundColor Cyan
  Write-Host "UI (Vite) PID:   $($state.ui_pid)  alive=$uiOk" -ForegroundColor Cyan

  Write-Host "\nEndpoints:" -ForegroundColor Gray
  Write-Host "- UI:          http://localhost:5173" -ForegroundColor Gray
  Write-Host "- ControlPlane http://127.0.0.1:8080/health" -ForegroundColor Gray
}

switch ($Action) {
  'status' {
    Show-Status
    exit 0
  }
  'stop' {
    $state = Get-LocalRunnerState
    if ($state) {
      if ($state.ui_pid) { Stop-ProcessTree ([int]$state.ui_pid) }
      if ($state.ssh_pid) { Stop-ProcessTree ([int]$state.ssh_pid) }
      Remove-Item -ErrorAction SilentlyContinue $StateFile | Out-Null
    }
    Write-Host 'Stopped local tunnel + UI (if running).' -ForegroundColor Cyan
    exit 0
  }
  'start' {
    Initialize-StateDir

    # Capture which script parameters were explicitly provided by the user.
    # (Inside functions, $PSBoundParameters refers to the function parameters, not the script params.)
    $script:__RunLocalBoundParameters = $PSBoundParameters

    # Stop existing if present
    $state = Get-LocalRunnerState
    if ($state) {
      if ($state.ui_pid) { Stop-ProcessTree ([int]$state.ui_pid) }
      if ($state.ssh_pid) { Stop-ProcessTree ([int]$state.ssh_pid) }
    }

    $newState = [ordered]@{
      started_at = (Get-Date).ToString('o')
      host = $SshHost
      user = $User
      port = $Port
      identity_file = $IdentityFile
      ssh_pid = $null
      ui_pid = $null
    }

    if (-not $NoTunnel) {
      Write-Host 'Starting SSH tunnel…' -ForegroundColor Cyan
      $sshProc = Start-Tunnel
      $newState.ssh_pid = $sshProc.Id
      Start-Sleep -Milliseconds 300
    } else {
      Write-Host 'Skipping SSH tunnel (-NoTunnel).' -ForegroundColor Yellow
    }

    if (-not $NoUi) {
      Write-Host 'Starting local UI (Vite)…' -ForegroundColor Cyan
      $uiProc = Start-Ui
      $newState.ui_pid = $uiProc.Id
    } else {
      Write-Host 'Skipping UI (-NoUi).' -ForegroundColor Yellow
    }

    Save-LocalRunnerState $newState

    Write-Host "\nStarted." -ForegroundColor Green
    Show-Status
    exit 0
  }
}
