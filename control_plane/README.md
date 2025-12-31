# IronCrab Control Plane

FastAPI service for system management, risk monitoring, and bot control.

## Quick Start

```bash
cd control_plane
pip install -r requirements.txt
python main.py
```

Or with uvicorn directly:
```bash
uvicorn control_plane.main:app --host 0.0.0.0 --port 8080 --reload
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `NATS_URL` | `nats://localhost:4222` | NATS server URL |
| `MARKET_DATA_URL` | `http://127.0.0.1:9801` | market-data metrics endpoint |
| `MOMENTUM_BOT_URL` | `http://127.0.0.1:9802` | momentum-bot metrics endpoint |
| `EXECUTION_ENGINE_URL` | `http://127.0.0.1:9804` | execution-engine metrics endpoint |
| `CONTROL_PLANE_PORT` | `8080` | Control plane HTTP port |

## Endpoints

### Health & Status
- `GET /health` - Health check
- `GET /status` - System status (all components)
- `GET /metrics` - Aggregated metrics from all components

### Trading
- `GET /positions` - Current open positions
- `POST /kill` - **Emergency kill switch**
- `POST /kill/reset` - Reset kill switch

### Management
- `POST /config` - Update component configuration
- `POST /command/{component}` - Send command to component via NATS
- `GET /logs/{component}` - Get recent logs

## Kill Switch

The kill switch is the emergency stop mechanism:

```bash
curl -X POST http://localhost:8080/kill \
  -H "Content-Type: application/json" \
  -d '{"reason": "Manual emergency stop", "liquidate_positions": true}'
```

This will:
1. Set `kill_switch_active = true`
2. Publish kill command to NATS topic `ironcrab.control.kill`
3. All components should subscribe and stop trading immediately
4. If `liquidate_positions=true`, execution-engine will close all positions

## NATS Integration

The control plane uses NATS for:
- **Publish**: Kill switch, config updates
- **Request/Reply**: Commands to specific components

Topics:
- `ironcrab.control.kill` - Kill switch commands
- `ironcrab.control.config.reload` - Configuration updates
- `ironcrab.{component}.commands` - Component-specific commands

## API Documentation

Once running, visit:
- Swagger UI: http://localhost:8080/docs
- ReDoc: http://localhost:8080/redoc
