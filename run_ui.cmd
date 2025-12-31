@echo off
setlocal

REM Ensure Node.js is available on PATH for npm postinstall scripts.
set "PATH=C:\Program Files\nodejs;%PATH%"

echo.
echo IronCrab UI (local)
echo.
echo Prerequisite: SSH tunnel to the server control-plane:
echo   ssh -L 8080:127.0.0.1:8080 ironcrab@^<server^>
echo.
echo Then open the UI (Vite dev server) in your browser.
echo.

set "REPO_ROOT=%~dp0"
set "UI_DIR=%REPO_ROOT%ui"

if not exist "%UI_DIR%\package.json" (
  echo UI folder not found: %UI_DIR%
  exit /b 1
)

set "NPM_CMD=C:\Program Files\nodejs\npm.cmd"
if not exist "%NPM_CMD%" (
  echo npm.cmd not found at: %NPM_CMD%
  echo Install Node.js LTS ^(includes npm^), then retry:
  echo   winget install -e --id OpenJS.NodeJS.LTS
  exit /b 1
)

cd /d "%UI_DIR%"

if not exist "node_modules" (
  echo Installing UI dependencies...
  "%NPM_CMD%" install
)

echo Starting UI dev server...
"%NPM_CMD%" run dev
