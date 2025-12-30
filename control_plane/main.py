"""
IronCrab Control Plane – FastAPI Service

Source of Truth: docs/TARGET_ARCHITECTURE.md §2.4

Responsibilities:
- Start/Stop/Restart individual components
- Configuration management (hot reload)
- Risk monitoring and alerts
- Position overview and P&L dashboard
- Kill switch trigger
- NATS Request/Reply for commands to execution-engine

This service does NOT:
- Sign or send transactions
- Hold wallet keys
- Make trading decisions

Endpoints:
- GET /health - Health check
- GET /status - System status (all components)
- GET /positions - Current open positions
- GET /metrics - Aggregated metrics from all components
- POST /config - Update configuration
- POST /kill - Emergency kill switch
- POST /command/{component} - Send command to component via NATS
"""

import asyncio
import os
import json
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends, Header, Security
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, Field
import httpx
import hashlib
import secrets

# Optional NATS support
try:
    import nats
    from nats.aio.client import Client as NatsClient
    HAS_NATS = True
except ImportError:
    HAS_NATS = False
    NatsClient = None

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("control-plane")

# Audit logger for control actions (separate log stream)
audit_logger = logging.getLogger("control-plane.audit")
audit_handler = logging.FileHandler("control_plane_audit.log")
audit_handler.setFormatter(logging.Formatter(
    '%(asctime)s - AUDIT - %(message)s'
))
audit_logger.addHandler(audit_handler)
audit_logger.setLevel(logging.INFO)

# ============================================================================
# Configuration
# ============================================================================

class Config:
    """Control plane configuration from environment"""
    NATS_URL: str = os.getenv("NATS_URL", "nats://localhost:4222")
    MARKET_DATA_URL: str = os.getenv("MARKET_DATA_URL", "http://localhost:9801")
    MOMENTUM_BOT_URL: str = os.getenv("MOMENTUM_BOT_URL", "http://localhost:9802")
    EXECUTION_ENGINE_URL: str = os.getenv("EXECUTION_ENGINE_URL", "http://localhost:9803")
    
    # NATS topics
    TOPIC_COMMANDS: str = "ironcrab.control.commands"
    TOPIC_KILL_SWITCH: str = "ironcrab.control.kill"
    TOPIC_CONFIG_RELOAD: str = "ironcrab.control.config.reload"
    
    # RBAC: API Keys (in production, load from secure storage)
    # Format: {"hashed_key": {"role": "admin|viewer", "name": "description"}}
    # Generate keys with: python -c "import secrets; print(secrets.token_urlsafe(32))"
    ADMIN_API_KEY: str = os.getenv("CONTROL_PLANE_ADMIN_KEY", "")
    VIEWER_API_KEY: str = os.getenv("CONTROL_PLANE_VIEWER_KEY", "")
    
    # If no keys configured, allow unauthenticated access (dev mode)
    REQUIRE_AUTH: bool = os.getenv("CONTROL_PLANE_REQUIRE_AUTH", "false").lower() == "true"

config = Config()

# ============================================================================
# RBAC: Role-Based Access Control
# ============================================================================

class Role:
    """User roles with their permissions"""
    ADMIN = "admin"      # Full access: read + write + kill switch
    VIEWER = "viewer"    # Read-only: status, metrics, positions
    ANONYMOUS = "anonymous"  # No authentication (dev mode only)

class User(BaseModel):
    """Authenticated user context"""
    role: str
    name: str
    api_key_prefix: str  # First 8 chars for logging (never log full key)

# API Key security scheme
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

def hash_api_key(key: str) -> str:
    """Hash API key for secure comparison"""
    return hashlib.sha256(key.encode()).hexdigest()

def get_current_user(api_key: str = Security(api_key_header)) -> User:
    """
    Validate API key and return user context.
    
    Permissions:
    - ADMIN: All endpoints (read + write + kill)
    - VIEWER: Read-only endpoints (GET requests)
    - ANONYMOUS: Only if REQUIRE_AUTH=false (dev mode)
    """
    # Dev mode: no auth required
    if not config.REQUIRE_AUTH:
        return User(role=Role.ANONYMOUS, name="dev-mode", api_key_prefix="no-auth")
    
    if not api_key:
        raise HTTPException(
            status_code=401,
            detail="Missing API key. Provide X-API-Key header.",
            headers={"WWW-Authenticate": "ApiKey"}
        )
    
    key_prefix = api_key[:8] if len(api_key) >= 8 else api_key
    
    # Check admin key
    if config.ADMIN_API_KEY and secrets.compare_digest(api_key, config.ADMIN_API_KEY):
        audit_logger.info(f"AUTH_SUCCESS: role=admin, key_prefix={key_prefix}")
        return User(role=Role.ADMIN, name="admin", api_key_prefix=key_prefix)
    
    # Check viewer key
    if config.VIEWER_API_KEY and secrets.compare_digest(api_key, config.VIEWER_API_KEY):
        audit_logger.info(f"AUTH_SUCCESS: role=viewer, key_prefix={key_prefix}")
        return User(role=Role.VIEWER, name="viewer", api_key_prefix=key_prefix)
    
    # Invalid key
    audit_logger.warning(f"AUTH_FAILED: invalid key, prefix={key_prefix}")
    raise HTTPException(
        status_code=403,
        detail="Invalid API key",
    )

def require_admin(user: User = Depends(get_current_user)) -> User:
    """Dependency that requires admin role"""
    if user.role not in [Role.ADMIN, Role.ANONYMOUS]:
        audit_logger.warning(f"ACCESS_DENIED: user={user.name}, required=admin, has={user.role}")
        raise HTTPException(
            status_code=403,
            detail=f"Admin role required. Your role: {user.role}"
        )
    return user

def require_viewer(user: User = Depends(get_current_user)) -> User:
    """Dependency that requires at least viewer role (admin also allowed)"""
    # All authenticated users can view
    return user

# ============================================================================
# Models
# ============================================================================

class ComponentStatus(BaseModel):
    name: str
    healthy: bool
    metrics_url: str
    last_check: datetime
    details: Optional[Dict[str, Any]] = None

class SystemStatus(BaseModel):
    timestamp: datetime
    overall_healthy: bool
    components: List[ComponentStatus]
    kill_switch_active: bool = False

class Position(BaseModel):
    mint: str
    tokens: float
    entry_price_sol: float
    current_price_sol: Optional[float] = None
    unrealized_pnl_sol: Optional[float] = None
    regime: str  # "EARLY" or "ESTABLISHED"
    entry_slot: int
    age_slots: Optional[int] = None

class KillRequest(BaseModel):
    reason: str = Field(..., description="Reason for triggering kill switch")
    liquidate_positions: bool = Field(default=True, description="Whether to liquidate open positions")

class CommandRequest(BaseModel):
    command: str = Field(..., description="Command to send")
    params: Optional[Dict[str, Any]] = Field(default=None, description="Command parameters")
    timeout_ms: int = Field(default=5000, description="Timeout in milliseconds")

class ConfigUpdate(BaseModel):
    component: str = Field(..., description="Target component (market-data, momentum-bot, execution-engine)")
    config: Dict[str, Any] = Field(..., description="Configuration key-value pairs to update")

# ============================================================================
# Global State
# ============================================================================

class ControlPlaneState:
    def __init__(self):
        self.nats_client: Optional[NatsClient] = None
        self.kill_switch_active: bool = False
        self.kill_switch_reason: Optional[str] = None
        self.kill_switch_time: Optional[datetime] = None
        self.http_client: Optional[httpx.AsyncClient] = None
    
    async def connect_nats(self):
        if not HAS_NATS:
            logger.warning("NATS not available (install with: pip install nats-py)")
            return
        
        try:
            self.nats_client = await nats.connect(config.NATS_URL)
            logger.info(f"Connected to NATS at {config.NATS_URL}")
        except Exception as e:
            logger.error(f"Failed to connect to NATS: {e}")
    
    async def disconnect_nats(self):
        if self.nats_client:
            await self.nats_client.close()
            logger.info("Disconnected from NATS")
    
    async def publish(self, topic: str, data: dict):
        if not self.nats_client:
            logger.warning(f"NATS not connected, cannot publish to {topic}")
            return False
        
        try:
            payload = json.dumps(data).encode()
            await self.nats_client.publish(topic, payload)
            return True
        except Exception as e:
            logger.error(f"Failed to publish to {topic}: {e}")
            return False
    
    async def request(self, topic: str, data: dict, timeout: float = 5.0) -> Optional[dict]:
        if not self.nats_client:
            logger.warning(f"NATS not connected, cannot request {topic}")
            return None
        
        try:
            payload = json.dumps(data).encode()
            response = await self.nats_client.request(topic, payload, timeout=timeout)
            return json.loads(response.data.decode())
        except Exception as e:
            logger.error(f"Request to {topic} failed: {e}")
            return None

state = ControlPlaneState()

# ============================================================================
# Lifespan
# ============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Control plane starting...")
    
    # P0 Security Check: Control Plane must NEVER have access to wallet keys
    forbidden_vars = ["IRONCRAB_KEYPAIR_JSON", "IRONCRAB_KEYPAIR_B64", 
                      "IRONCRAB_KEYPAIR_PATH", "IRONCRAB_KEYPAIR_BASE58"]
    detected_keys = [v for v in forbidden_vars if os.getenv(v)]
    if detected_keys:
        logger.critical(f"SECURITY VIOLATION: Control Plane detected wallet key variables: {detected_keys}")
        logger.critical("Control Plane must be KEYLESS. Remove these variables immediately!")
        audit_logger.critical(f"STARTUP_BLOCKED: Wallet keys detected in environment: {detected_keys}")
        raise RuntimeError("Control Plane cannot start with wallet key environment variables")
    
    logger.info("Security check passed: No wallet keys in environment")
    audit_logger.info("STARTUP: Control Plane started (keyless mode verified)")
    
    state.http_client = httpx.AsyncClient(timeout=5.0)
    await state.connect_nats()
    logger.info("Control plane ready")
    
    yield
    
    # Shutdown
    logger.info("Control plane shutting down...")
    audit_logger.info("SHUTDOWN: Control Plane stopping")
    if state.http_client:
        await state.http_client.aclose()
    await state.disconnect_nats()

# ============================================================================
# FastAPI App
# ============================================================================

app = FastAPI(
    title="IronCrab Control Plane",
    description="System management and risk monitoring for IronCrab trading system",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================================
# Endpoints
# ============================================================================

@app.get("/health")
async def health():
    """Health check endpoint (no auth required)"""
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}

@app.get("/status", response_model=SystemStatus)
async def get_status(user: User = Depends(require_viewer)):
    """Get status of all system components (requires: viewer)"""
    audit_logger.info(f"STATUS_VIEW: user={user.name}, role={user.role}")
    components = []
    
    # Check each component's /live endpoint
    component_configs = [
        ("market-data", config.MARKET_DATA_URL),
        ("momentum-bot", config.MOMENTUM_BOT_URL),
        ("execution-engine", config.EXECUTION_ENGINE_URL),
    ]
    
    for name, base_url in component_configs:
        status = ComponentStatus(
            name=name,
            healthy=False,
            metrics_url=f"{base_url}/metrics",
            last_check=datetime.now(timezone.utc),
        )
        
        try:
            response = await state.http_client.get(f"{base_url}/live")
            status.healthy = response.status_code == 200
            status.details = {"response": response.text}
        except Exception as e:
            status.details = {"error": str(e)}
        
        components.append(status)
    
    overall_healthy = all(c.healthy for c in components) and not state.kill_switch_active
    
    return SystemStatus(
        timestamp=datetime.now(timezone.utc),
        overall_healthy=overall_healthy,
        components=components,
        kill_switch_active=state.kill_switch_active,
    )

@app.get("/positions")
async def get_positions(user: User = Depends(require_viewer)):
    """Get current open positions from execution-engine (requires: viewer)"""
    audit_logger.info(f"POSITIONS_VIEW: user={user.name}, role={user.role}")
    # In production: query execution-engine via NATS request/reply
    # For MVP: return mock data
    return {
        "positions": [],
        "total_value_sol": 0.0,
        "unrealized_pnl_sol": 0.0,
        "note": "Position tracking via NATS request/reply (not yet implemented)"
    }

@app.get("/metrics")
async def get_aggregated_metrics(user: User = Depends(require_viewer)):
    """Aggregate metrics from all components (requires: viewer)"""
    audit_logger.info(f"METRICS_VIEW: user={user.name}, role={user.role}")
    metrics = {}
    
    component_configs = [
        ("market-data", config.MARKET_DATA_URL),
        ("momentum-bot", config.MOMENTUM_BOT_URL),
        ("execution-engine", config.EXECUTION_ENGINE_URL),
    ]
    
    for name, base_url in component_configs:
        try:
            response = await state.http_client.get(f"{base_url}/metrics")
            if response.status_code == 200:
                metrics[name] = {
                    "status": "ok",
                    "raw_metrics": response.text[:1000] + "..." if len(response.text) > 1000 else response.text
                }
            else:
                metrics[name] = {"status": "error", "code": response.status_code}
        except Exception as e:
            metrics[name] = {"status": "unreachable", "error": str(e)}
    
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "components": metrics
    }

@app.post("/kill")
async def trigger_kill_switch(
    request: KillRequest, 
    background_tasks: BackgroundTasks,
    user: User = Depends(require_admin)
):
    """
    Trigger emergency kill switch (requires: admin).
    
    This will:
    1. Set kill_switch_active flag
    2. Publish kill command to NATS
    3. Optionally trigger position liquidation
    """
    logger.warning(f"KILL SWITCH TRIGGERED by {user.name}: {request.reason}")
    audit_logger.warning(f"KILL_SWITCH_ACTIVATED: user={user.name}, reason='{request.reason}', liquidate={request.liquidate_positions}")
    
    state.kill_switch_active = True
    state.kill_switch_reason = request.reason
    state.kill_switch_time = datetime.now(timezone.utc)
    
    # Publish kill command to NATS
    kill_msg = {
        "command": "kill",
        "reason": request.reason,
        "liquidate": request.liquidate_positions,
        "timestamp": state.kill_switch_time.isoformat(),
    }
    
    published = await state.publish(config.TOPIC_KILL_SWITCH, kill_msg)
    
    return {
        "status": "kill_switch_activated",
        "reason": request.reason,
        "liquidate_positions": request.liquidate_positions,
        "nats_published": published,
        "timestamp": state.kill_switch_time.isoformat(),
    }

@app.post("/kill/reset")
async def reset_kill_switch(user: User = Depends(require_admin)):
    """Reset kill switch (requires: admin)"""
    if not state.kill_switch_active:
        return {"status": "kill_switch_not_active"}
    
    logger.info(f"Kill switch reset requested by {user.name}")
    audit_logger.info(f"KILL_SWITCH_RESET: user={user.name}, previous_reason='{state.kill_switch_reason}'")
    state.kill_switch_active = False
    
    # Publish reset command
    await state.publish(config.TOPIC_KILL_SWITCH, {
        "command": "reset",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })
    
    return {
        "status": "kill_switch_reset",
        "previous_reason": state.kill_switch_reason,
        "previous_time": state.kill_switch_time.isoformat() if state.kill_switch_time else None,
    }

@app.post("/command/{component}")
async def send_command(
    component: str, 
    request: CommandRequest,
    user: User = Depends(require_admin)
):
    """Send command to a specific component via NATS request/reply (requires: admin)"""
    
    valid_components = ["market-data", "momentum-bot", "execution-engine"]
    if component not in valid_components:
        raise HTTPException(status_code=400, detail=f"Invalid component. Must be one of: {valid_components}")
    
    # Audit log the command (before execution)
    audit_logger.info(f"COMMAND: user={user.name}, component={component}, command={request.command}, params={request.params}")
    
    topic = f"ironcrab.{component.replace('-', '_')}.commands"
    
    command_msg = {
        "command": request.command,
        "params": request.params or {},
        "source": "control-plane",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    
    timeout = request.timeout_ms / 1000.0
    response = await state.request(topic, command_msg, timeout=timeout)
    
    if response is None:
        return {
            "status": "timeout_or_error",
            "component": component,
            "command": request.command,
            "note": "Component may not be running or NATS not connected"
        }
    
    return {
        "status": "ok",
        "component": component,
        "command": request.command,
        "response": response,
    }

@app.post("/config")
async def update_config(update: ConfigUpdate, user: User = Depends(require_admin)):
    """
    Update configuration for a component (requires: admin).
    
    Publishes config update to NATS for hot reload.
    """
    valid_components = ["market-data", "momentum-bot", "execution-engine"]
    if update.component not in valid_components:
        raise HTTPException(status_code=400, detail=f"Invalid component. Must be one of: {valid_components}")
    
    # Audit log the config change
    audit_logger.info(f"CONFIG_UPDATE: user={user.name}, component={update.component}, keys={list(update.config.keys())}")
    
    config_msg = {
        "command": "config_update",
        "component": update.component,
        "config": update.config,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    
    published = await state.publish(config.TOPIC_CONFIG_RELOAD, config_msg)
    
    return {
        "status": "config_update_published" if published else "nats_not_connected",
        "component": update.component,
        "config_keys": list(update.config.keys()),
    }

@app.get("/logs/{component}")
async def get_recent_logs(component: str, lines: int = 100, user: User = Depends(require_viewer)):
    """Get recent log entries for a component (requires: viewer)"""
    audit_logger.info(f"LOGS_VIEW: user={user.name}, component={component}, lines={lines}")
    # In production: read from log files or log aggregation service
    return {
        "component": component,
        "lines": lines,
        "logs": [],
        "note": "Log aggregation not yet implemented"
    }

@app.get("/whoami")
async def whoami(user: User = Depends(get_current_user)):
    """Get current authenticated user info"""
    return {
        "role": user.role,
        "name": user.name,
        "api_key_prefix": user.api_key_prefix,
        "permissions": {
            "can_view": True,
            "can_modify": user.role in [Role.ADMIN, Role.ANONYMOUS],
            "can_kill": user.role in [Role.ADMIN, Role.ANONYMOUS],
        }
    }

@app.get("/rbac/info")
async def rbac_info():
    """Get RBAC configuration info (no auth required)"""
    return {
        "auth_required": config.REQUIRE_AUTH,
        "roles": {
            Role.ADMIN: {
                "description": "Full access: read, write, kill switch",
                "endpoints": ["ALL"]
            },
            Role.VIEWER: {
                "description": "Read-only access",
                "endpoints": ["GET /status", "GET /positions", "GET /metrics", "GET /logs/*"]
            }
        },
        "note": "Set CONTROL_PLANE_REQUIRE_AUTH=true to enable authentication"
    }

# ============================================================================
# Main
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    
    port = int(os.getenv("CONTROL_PLANE_PORT", "8080"))
    host = os.getenv("CONTROL_PLANE_HOST", "0.0.0.0")
    
    logger.info(f"Starting Control Plane on {host}:{port}")
    uvicorn.run(app, host=host, port=port)
