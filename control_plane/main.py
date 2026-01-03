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
- Decision record display and statistics (P1 DoD)

This service does NOT:
- Sign or send transactions
- Hold wallet keys
- Make trading decisions

Endpoints:
- GET /health - Detailed health check with component status
- GET /live - Liveness probe (K8s/Systemd)
- GET /ready - Readiness probe (K8s/Systemd)
- GET /status - System status (all components)
- GET /positions - Current open positions
- GET /metrics - Aggregated metrics from all components
- POST /config - Update configuration
- POST /kill - Emergency kill switch
- POST /command/{component} - Send command to component via NATS
- GET /decisions - Get recent decision records (with filters)
- GET /decisions/stats - Get decision statistics
- GET /decisions/{decision_id} - Get specific decision by ID
- POST /decisions/query - Query decisions with complex filters
"""

import asyncio
import os
import json
import logging
from datetime import datetime, timezone
import uuid
from typing import Optional, Dict, Any, List
from contextlib import asynccontextmanager
import shlex

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
    MARKET_DATA_URL: str = os.getenv("MARKET_DATA_URL", "http://127.0.0.1:9801")
    MOMENTUM_BOT_URL: str = os.getenv("MOMENTUM_BOT_URL", "http://127.0.0.1:9802")
    ARB_STRATEGY_URL: str = os.getenv("ARB_STRATEGY_URL", "http://127.0.0.1:9803")
    EXECUTION_ENGINE_URL: str = os.getenv("EXECUTION_ENGINE_URL", "http://127.0.0.1:9804")
    
    # NATS topics
    TOPIC_COMMANDS: str = "ironcrab.control.commands"
    TOPIC_KILL_SWITCH: str = "ironcrab.control.kill"
    TOPIC_CONFIG_RELOAD: str = "ironcrab.control.config.reload"

    # Versioned control plane requests (preferred)
    TOPIC_CONTROL_REQUESTS: str = "ironcrab.v1.control_requests"

    # Legacy topic compatibility (can be disabled once all consumers are migrated)
    PUBLISH_LEGACY_KILL_TOPIC: bool = os.getenv(
        "CONTROL_PLANE_PUBLISH_LEGACY_KILL_TOPIC", "true"
    ).lower() in ("1", "true", "yes", "y")
    
    # RBAC: API Keys (in production, load from secure storage)
    # Format: {"hashed_key": {"role": "admin|viewer", "name": "description"}}
    # Generate keys with: python -c "import secrets; print(secrets.token_urlsafe(32))"
    ADMIN_API_KEY: str = os.getenv("CONTROL_PLANE_ADMIN_KEY", "")
    VIEWER_API_KEY: str = os.getenv("CONTROL_PLANE_VIEWER_KEY", "")
    
    # If no keys configured, allow unauthenticated access (dev mode)
    REQUIRE_AUTH: bool = os.getenv("CONTROL_PLANE_REQUIRE_AUTH", "false").lower() == "true"

config = Config()

CONTROL_PLANE_COMPONENT = "control-plane"
CONTROL_PLANE_BUILD = os.getenv("IRONCRAB_BUILD", "control-plane")
CONTROL_PLANE_RUN_ID = os.getenv("IRONCRAB_RUN_ID", str(uuid.uuid4()))


def _now_unix_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _control_request_header() -> Dict[str, Any]:
    return {
        "schema_version": 1,
        "ts_unix_ms": _now_unix_ms(),
        "component": CONTROL_PLANE_COMPONENT,
        "build": CONTROL_PLANE_BUILD,
        "run_id": CONTROL_PLANE_RUN_ID,
    }

SYSTEMD_COMPONENTS: Dict[str, str] = {
    "market-data": "market-data.service",
    "momentum-bot": "momentum-bot.service",
    "arb-strategy": "arb-strategy.service",
    "execution-engine": "execution-engine.service",
}

def _valid_component_list() -> List[str]:
    return list(SYSTEMD_COMPONENTS.keys())

async def _run_command(argv: List[str], timeout_s: float = 8.0) -> Dict[str, Any]:
    """Run a command and capture stdout/stderr without raising.

    Intended for operator actions (non hot-path).
    """
    try:
        proc = await asyncio.create_subprocess_exec(
            *argv,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout_b, stderr_b = await asyncio.wait_for(proc.communicate(), timeout=timeout_s)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.communicate()
            return {
                "ok": False,
                "argv": argv,
                "timeout_s": timeout_s,
                "error": "timeout",
            }

        stdout = stdout_b.decode(errors="replace") if stdout_b else ""
        stderr = stderr_b.decode(errors="replace") if stderr_b else ""
        return {
            "ok": proc.returncode == 0,
            "argv": argv,
            "returncode": proc.returncode,
            "stdout": stdout.strip(),
            "stderr": stderr.strip(),
        }
    except Exception as e:
        return {"ok": False, "argv": argv, "error": str(e)}

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

# ============================================================================
# Health Check Models (K8s / Systemd compatible)
# ============================================================================

class HealthStatus(BaseModel):
    """Detailed health status response"""
    status: str  # "ok", "degraded", "unhealthy"
    timestamp: str
    version: str = "1.0.0"
    uptime_seconds: float
    checks: Dict[str, bool] = Field(default_factory=dict)
    details: Optional[Dict[str, Any]] = None

class LivenessStatus(BaseModel):
    """Liveness probe response (is the process alive?)"""
    alive: bool
    timestamp: str
    pid: int

class ReadinessStatus(BaseModel):
    """Readiness probe response (is the service ready to accept traffic?)"""
    ready: bool
    timestamp: str
    checks: Dict[str, bool] = Field(default_factory=dict)
    reason: Optional[str] = None

# ============================================================================
# Decision Record Models (matching Rust IPC schema)
# ============================================================================

class CheckResult(BaseModel):
    """Single check result from decision pipeline"""
    check_name: str
    passed: bool
    reason_code: Optional[str] = None
    details: Optional[str] = None

class SimulationResult(BaseModel):
    """Simulation result from execution"""
    success: bool
    compute_units: Optional[int] = None
    error_code: Optional[str] = None
    logs: Optional[List[str]] = None

class SendResult(BaseModel):
    """Transaction send result"""
    signature: Optional[str] = None
    slot: Optional[int] = None
    error: Optional[str] = None

class DecisionRecord(BaseModel):
    """
    Decision record from execution-engine.
    Matches Rust DecisionRecord in src/ipc/schema.rs.
    """
    # Header fields
    ts: str  # ISO timestamp
    component: str
    build: Optional[str] = None
    run_id: Optional[str] = None
    
    # Core fields
    decision_id: str
    intent_id: str
    source: str  # P1: Strategy attribution
    origin_type: str  # "Manual", "Momentum", "Arbitrage", etc.
    regime: str  # "Early", "Established", etc.
    
    # Checks
    checks: List[CheckResult] = Field(default_factory=list)
    primary_reject_reason: Optional[str] = None
    
    # Simulation
    plan_hash: Optional[str] = None
    simulate: Optional[SimulationResult] = None
    
    # Send
    send: Optional[SendResult] = None
    
    # Outcome: "Executed", "Rejected", "SimFailed", "SendFailed"
    outcome: str
    
    # Replay support
    config_snapshot_id: Optional[str] = None
    input_snapshots: Dict[str, str] = Field(default_factory=dict)

class DecisionQuery(BaseModel):
    """Query parameters for decision records"""
    limit: int = Field(default=50, ge=1, le=500, description="Max records to return")
    source: Optional[str] = Field(default=None, description="Filter by source strategy")
    outcome: Optional[str] = Field(default=None, description="Filter by outcome (Executed, Rejected, SimFailed, SendFailed)")
    since: Optional[str] = Field(default=None, description="ISO timestamp to filter records after")
    intent_id: Optional[str] = Field(default=None, description="Filter by specific intent ID")

class DecisionStats(BaseModel):
    """Aggregated statistics for decision records"""
    total: int
    executed: int
    rejected: int
    sim_failed: int
    send_failed: int
    by_source: Dict[str, int]
    by_reject_reason: Dict[str, int]
    period_start: Optional[str] = None
    period_end: Optional[str] = None

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
        self.startup_time: Optional[datetime] = None  # P2: Track uptime
        # P1: Decision record cache (ring buffer for recent decisions)
        self.decisions: List[Dict[str, Any]] = []
        self.decisions_lock = asyncio.Lock()
        self.max_cached_decisions: int = 1000
        self.decision_subscriber_task: Optional[asyncio.Task] = None
    
    async def connect_nats(self):
        if not HAS_NATS:
            logger.warning("NATS not available (install with: pip install nats-py)")
            return
        
        try:
            self.nats_client = await nats.connect(config.NATS_URL)
            logger.info(f"Connected to NATS at {config.NATS_URL}")
            # Start decision record subscriber
            self.decision_subscriber_task = asyncio.create_task(self._subscribe_decisions())
        except Exception as e:
            logger.error(f"Failed to connect to NATS: {e}")
    
    async def _subscribe_decisions(self):
        """Subscribe to decision records topic for live updates"""
        if not self.nats_client:
            return
        
        try:
            # Subscribe to decision records topic
            sub = await self.nats_client.subscribe("ironcrab.v1.decision_records")
            logger.info("Subscribed to decision records topic")
            
            async for msg in sub.messages:
                try:
                    decision = json.loads(msg.data.decode())
                    async with self.decisions_lock:
                        self.decisions.append(decision)
                        # Keep only most recent decisions (ring buffer)
                        if len(self.decisions) > self.max_cached_decisions:
                            self.decisions = self.decisions[-self.max_cached_decisions:]
                    logger.debug(f"Cached decision: {decision.get('decision_id', 'unknown')}")
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse decision record: {e}")
        except Exception as e:
            if "cancelled" not in str(e).lower():
                logger.error(f"Decision subscriber error: {e}")
        except Exception as e:
            logger.error(f"Failed to connect to NATS: {e}")
    
    async def disconnect_nats(self):
        if self.decision_subscriber_task:
            self.decision_subscriber_task.cancel()
            try:
                await self.decision_subscriber_task
            except asyncio.CancelledError:
                pass
            logger.info("Decision subscriber stopped")
        if self.nats_client:
            await self.nats_client.close()
            logger.info("Disconnected from NATS")
    
    async def get_cached_decisions(
        self, 
        limit: int = 50,
        source: Optional[str] = None,
        outcome: Optional[str] = None,
        since: Optional[str] = None,
        intent_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Get cached decision records with optional filtering"""
        async with self.decisions_lock:
            filtered = self.decisions.copy()
        
        # Apply filters
        if source:
            filtered = [d for d in filtered if d.get("source") == source]
        if outcome:
            filtered = [d for d in filtered if d.get("outcome") == outcome]
        if since:
            try:
                since_dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
                filtered = [d for d in filtered 
                           if datetime.fromisoformat(d.get("ts", "").replace("Z", "+00:00")) > since_dt]
            except (ValueError, TypeError):
                pass  # Invalid date, skip filter
        if intent_id:
            filtered = [d for d in filtered if d.get("intent_id") == intent_id]
        
        # Return most recent first, limited
        return list(reversed(filtered))[:limit]
    
    def get_decision_stats(self) -> Dict[str, Any]:
        """Calculate statistics from cached decisions"""
        decisions = self.decisions  # snapshot
        
        stats = {
            "total": len(decisions),
            "executed": 0,
            "rejected": 0,
            "sim_failed": 0,
            "send_failed": 0,
            "by_source": {},
            "by_reject_reason": {},
            "period_start": None,
            "period_end": None,
        }
        
        for d in decisions:
            outcome = d.get("outcome", "").lower()
            if "executed" in outcome:
                stats["executed"] += 1
            elif "rejected" in outcome:
                stats["rejected"] += 1
            elif "simfailed" in outcome or "sim_failed" in outcome:
                stats["sim_failed"] += 1
            elif "sendfailed" in outcome or "send_failed" in outcome:
                stats["send_failed"] += 1
            
            # By source
            source = d.get("source", "unknown")
            stats["by_source"][source] = stats["by_source"].get(source, 0) + 1
            
            # By reject reason
            reason = d.get("primary_reject_reason")
            if reason:
                stats["by_reject_reason"][reason] = stats["by_reject_reason"].get(reason, 0) + 1
        
        # Period
        if decisions:
            timestamps = [d.get("ts") for d in decisions if d.get("ts")]
            if timestamps:
                stats["period_start"] = min(timestamps)
                stats["period_end"] = max(timestamps)
        
        return stats
    
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
    state.startup_time = datetime.now(timezone.utc)  # Track uptime
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

# ----------------------------------------------------------------------------
# Health / Liveness / Readiness Probes (P2: K8s/Systemd compatible)
# ----------------------------------------------------------------------------

@app.get("/health", response_model=HealthStatus)
async def health():
    """
    Detailed health check endpoint (no auth required).
    
    Returns comprehensive health status including:
    - Overall status (ok/degraded/unhealthy)
    - Uptime
    - Individual component checks
    
    Use for monitoring dashboards and alerting.
    """
    now = datetime.now(timezone.utc)
    uptime = (now - state.startup_time).total_seconds() if state.startup_time else 0.0
    
    checks = {
        "nats_connected": state.nats_client is not None and state.nats_client.is_connected if state.nats_client else False,
        "http_client_ready": state.http_client is not None,
        "kill_switch_inactive": not state.kill_switch_active,
    }
    
    # Determine overall status
    if all(checks.values()):
        status = "ok"
    elif checks["kill_switch_inactive"] and checks["http_client_ready"]:
        status = "degraded"  # Can operate without NATS
    else:
        status = "unhealthy"
    
    return HealthStatus(
        status=status,
        timestamp=now.isoformat(),
        version="1.0.0",
        uptime_seconds=uptime,
        checks=checks,
        details={
            "kill_switch_reason": state.kill_switch_reason,
            "cached_decisions": len(state.decisions),
        }
    )

@app.get("/live", response_model=LivenessStatus)
async def liveness():
    """
    Liveness probe (no auth required).
    
    K8s/Systemd uses this to determine if the process is alive.
    Returns 200 if the process is running, regardless of dependencies.
    
    If this fails, K8s will restart the pod.
    """
    import os
    return LivenessStatus(
        alive=True,
        timestamp=datetime.now(timezone.utc).isoformat(),
        pid=os.getpid(),
    )

@app.get("/ready", response_model=ReadinessStatus)
async def readiness():
    """
    Readiness probe (no auth required).
    
    K8s/Systemd uses this to determine if the service can accept traffic.
    Returns 200 only if core dependencies are available.
    
    If this fails, K8s will stop routing traffic to this pod.
    """
    checks = {
        "http_client": state.http_client is not None,
        "nats": state.nats_client is not None and (state.nats_client.is_connected if state.nats_client else False),
        "not_killed": not state.kill_switch_active,
    }
    
    # Ready if HTTP client is available and not in kill switch mode
    # NATS is optional (service can run in degraded mode without it)
    ready = checks["http_client"] and checks["not_killed"]
    
    reason = None
    if not ready:
        if state.kill_switch_active:
            reason = f"Kill switch active: {state.kill_switch_reason}"
        elif not checks["http_client"]:
            reason = "HTTP client not initialized"
    
    return ReadinessStatus(
        ready=ready,
        timestamp=datetime.now(timezone.utc).isoformat(),
        checks=checks,
        reason=reason,
    )

@app.get("/status", response_model=SystemStatus)
async def get_status(user: User = Depends(require_viewer)):
    """Get status of all system components (requires: viewer)"""
    audit_logger.info(f"STATUS_VIEW: user={user.name}, role={user.role}")
    components = []
    
    # Check each component's /live endpoint
    component_configs = [
        ("market-data", config.MARKET_DATA_URL),
        ("momentum-bot", config.MOMENTUM_BOT_URL),
        ("arb-strategy", config.ARB_STRATEGY_URL),
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
        ("arb-strategy", config.ARB_STRATEGY_URL),
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

    # Preferred: versioned control request
    control_req = {
        **_control_request_header(),
        "request_id": str(uuid.uuid4()),
        "target": "execution-engine",
        "kind": "kill_switch",
        "active": True,
        "reason": request.reason,
        "liquidate_positions": request.liquidate_positions,
    }

    published = await state.publish(config.TOPIC_CONTROL_REQUESTS, control_req)

    # Legacy topic (kept for compatibility, optional)
    if config.PUBLISH_LEGACY_KILL_TOPIC:
        await state.publish(config.TOPIC_KILL_SWITCH, kill_msg)
    
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

    # Preferred: versioned control request
    reset_req = {
        **_control_request_header(),
        "request_id": str(uuid.uuid4()),
        "target": "execution-engine",
        "kind": "reset_kill_switch",
    }
    await state.publish(config.TOPIC_CONTROL_REQUESTS, reset_req)

    # Legacy topic (kept for compatibility, optional)
    if config.PUBLISH_LEGACY_KILL_TOPIC:
        await state.publish(
            config.TOPIC_KILL_SWITCH,
            {
                "command": "reset",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        )
    
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
    
    valid_components = _valid_component_list()
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


@app.post("/systemd/{component}/{action}")
async def systemd_component_action(
    component: str,
    action: str,
    user: User = Depends(require_admin),
):
    """Start/stop/restart a component via systemd (requires: admin)."""
    valid_components = _valid_component_list()
    if component not in valid_components:
        raise HTTPException(status_code=400, detail=f"Invalid component. Must be one of: {valid_components}")

    valid_actions = {"start", "stop", "restart", "status"}
    if action not in valid_actions:
        raise HTTPException(status_code=400, detail=f"Invalid action. Must be one of: {sorted(valid_actions)}")

    unit = SYSTEMD_COMPONENTS[component]
    audit_logger.warning(f"SYSTEMD: user={user.name}, component={component}, action={action}, unit={unit}")

    # NOTE: This may require elevated privileges depending on the deployment.
    # Keep argv minimal so it matches tight sudoers allowlists.
    # Also use an absolute path for determinism.
    argv = ["/usr/bin/systemctl", action, unit]
    result = await _run_command(argv)

    # If we hit a permissions wall, try sudo -n (requires sudoers config and NoNewPrivileges disabled).
    stderr = (result.get("stderr") or "").lower()
    if (not result.get("ok")) and (
        "interactive authentication required" in stderr
        or "access denied" in stderr
        or "not authorized" in stderr
    ):
        sudo_argv = ["sudo", "-n", *argv]
        sudo_result = await _run_command(sudo_argv)
        if sudo_result.get("ok"):
            result = sudo_result
            argv = sudo_argv

    # Return minimal, operator-friendly payload.
    return {
        "component": component,
        "action": action,
        "unit": unit,
        "ok": result.get("ok", False),
        "stdout": result.get("stdout", ""),
        "stderr": result.get("stderr", ""),
        "returncode": result.get("returncode"),
        "argv": " ".join(shlex.quote(a) for a in argv),
    }

@app.post("/config")
async def update_config(update: ConfigUpdate, user: User = Depends(require_admin)):
    """
    Update configuration for a component (requires: admin).
    
    Publishes config update to NATS for hot reload.
    
    Supported keys for execution-engine:
    - max_position_size_lamports: u64 (must be > 0)
    - daily_loss_limit_lamports: u64 (must be > 0)
    - max_open_positions: u64 (1-100)
    - max_slippage_bps: u64 (1-10000)
    - simulation_timeout_ms: u64 (100-30000)
    - send_enabled: bool (can only enable if wallet configured)
    """
    valid_components = ["market-data", "momentum-bot", "execution-engine"]
    if update.component not in valid_components:
        raise HTTPException(status_code=400, detail=f"Invalid component. Must be one of: {valid_components}")
    
    # Audit log the config change
    audit_logger.info(f"CONFIG_UPDATE: user={user.name}, component={update.component}, keys={list(update.config.keys())}")
    
    # P1: Format matches Rust IPC schema (ConfigUpdate)
    config_msg = {
        "component": update.component,
        "config": update.config,
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

# ============================================================================
# Decision Display Endpoints (P1 DoD: UI shows decision records)
# ============================================================================

@app.get("/decisions", response_model=List[DecisionRecord])
async def get_decisions(
    limit: int = 50,
    source: Optional[str] = None,
    outcome: Optional[str] = None,
    since: Optional[str] = None,
    intent_id: Optional[str] = None,
    user: User = Depends(require_viewer)
):
    """
    Get recent decision records (requires: viewer).
    
    Returns decisions from the in-memory cache (populated via NATS subscription).
    
    Filters:
    - limit: Max number of records (1-500, default 50)
    - source: Filter by source strategy (e.g., "momentum-bot")
    - outcome: Filter by outcome ("Executed", "Rejected", "SimFailed", "SendFailed")
    - since: ISO timestamp to get records after
    - intent_id: Get specific intent's decision
    """
    audit_logger.info(f"DECISIONS_VIEW: user={user.name}, limit={limit}, source={source}, outcome={outcome}")
    
    limit = min(max(limit, 1), 500)
    decisions = await state.get_cached_decisions(
        limit=limit,
        source=source,
        outcome=outcome,
        since=since,
        intent_id=intent_id,
    )
    
    return decisions

@app.get("/decisions/stats", response_model=DecisionStats)
async def get_decision_stats(user: User = Depends(require_viewer)):
    """
    Get aggregated statistics for decision records (requires: viewer).
    
    Shows counts by outcome, source, and reject reason.
    """
    audit_logger.info(f"DECISIONS_STATS: user={user.name}")
    return state.get_decision_stats()

@app.get("/decisions/{decision_id}")
async def get_decision_by_id(decision_id: str, user: User = Depends(require_viewer)):
    """
    Get a specific decision record by ID (requires: viewer).
    """
    audit_logger.info(f"DECISION_VIEW: user={user.name}, decision_id={decision_id}")
    
    async with state.decisions_lock:
        for d in state.decisions:
            if d.get("decision_id") == decision_id:
                return d
    
    raise HTTPException(status_code=404, detail=f"Decision {decision_id} not found in cache")

@app.post("/decisions/query")
async def query_decisions(query: DecisionQuery, user: User = Depends(require_viewer)):
    """
    Query decision records with complex filters (requires: viewer).
    
    POST body allows for more complex queries than GET parameters.
    """
    audit_logger.info(f"DECISIONS_QUERY: user={user.name}, query={query.model_dump()}")
    
    decisions = await state.get_cached_decisions(
        limit=query.limit,
        source=query.source,
        outcome=query.outcome,
        since=query.since,
        intent_id=query.intent_id,
    )
    
    stats = state.get_decision_stats()
    
    return {
        "decisions": decisions,
        "count": len(decisions),
        "stats": stats,
        "query": query.model_dump(),
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
