# IronCrab UI (MVP)

Minimal React UI that reads data from the Control Plane.

## How it connects

This UI calls:
- `GET /api/health` → proxied to `http://127.0.0.1:8080/health`
- `GET /api/status` → proxied to `http://127.0.0.1:8080/status`

So you typically use SSH port forwarding to the server:

```bash
ssh -L 8080:127.0.0.1:8080 ironcrab@<server>
```

## Run locally

On Windows, if PowerShell execution policy blocks `npm` (e.g. `npm.ps1`), use the repo helper script:

```bat
..\run_ui.cmd
```

```bash
cd ui
npm install
npm run dev
```

Then open the printed URL (usually `http://localhost:5173`).
