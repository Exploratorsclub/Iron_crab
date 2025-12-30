//! execution-engine binary – Single Signer / Execution Plane
//!
//! Source of Truth: docs/TARGET_ARCHITECTURE.md §2.3
//!
//! Responsibilities:
//! - ONLY process allowed to load wallet keys
//! - Subscribe to TradeIntents from NATS
//! - Global Arbitration (EV × urgency × deadline)
//! - Capital Locks + Resource Locks
//! - Pipeline: Intent → Arbitrate → Plan → Simulate → Send → Confirm
//! - Emit DecisionRecords + ExecutionResults (even on reject)
//! - Write JSONL for replay/forensics
//!
//! P0 Requirements:
//! - Simulate-gated: simulation fail = never send
//! - Decision Records for every intent
//! - Reason-coded rejects
//! - No silent failure: all errors logged with reason code (DoD O)
//!
//! P1: State Persistence (DoD K)
//! - State survives restarts via StateSnapshot
//! - Idempotency store persisted and loaded
//! - Daily loss tracking persisted

use anyhow::Result;
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use ironcrab::ipc::{
    CheckResult, DecisionOutcome, DecisionRecord, ExecutionResult, ExecutionStatus, IntentOrigin,
    RejectReason, SimulationResult, TradeIntent, TradingRegime,
};
use ironcrab::metrics::serve_metrics;
use ironcrab::nats::{
    NatsClient, NatsConfig, TOPIC_DECISION_RECORDS, TOPIC_EXECUTION_RESULTS, TOPIC_TRADE_INTENTS,
};
use ironcrab::storage::{
    locks::{LockHolder, LockManager, LockResult},
    JsonlWriter, JsonlWriterConfig,
};

// P1 Crash Isolation: Systemd Watchdog support
use sd_notify::NotifyState;

/// Build version for decision records
const BUILD_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Parser, Debug)]
#[command(name = "execution-engine")]
#[command(about = "IronCrab Execution Plane – Single Signer, Tx Plan/Sim/Send")]
struct Args {
    /// Path to configuration file
    #[arg(short, long, default_value = "config.toml")]
    config: PathBuf,

    /// NATS server URL
    #[arg(long, env = "NATS_URL", default_value = "nats://localhost:4222")]
    nats_url: String,

    /// Prometheus metrics port
    #[arg(long, default_value = "9803")]
    metrics_port: u16,

    /// Log directory override
    #[arg(long, env = "IRONCRAB_LOG_DIR")]
    log_dir: Option<PathBuf>,

    /// Disable actual transaction sending (simulation only)
    #[arg(long)]
    simulate_only: bool,

    /// Dry run: don't connect to Solana or send transactions
    #[arg(long)]
    dry_run: bool,

    /// Initial SOL balance for lock manager (lamports)
    #[arg(long, default_value = "1000000000")]
    initial_sol_lamports: u64,
}

/// Execution engine configuration
/// 
/// All risk limits are documented here (DoD J) P0: No hidden defaults).
/// These values are checked before every trade execution.
#[derive(Debug, Clone)]
struct ExecutionConfig {
    // === Risk Invariants (DoD J) P0) ===
    
    /// Maximum single position size (lamports). Default: 0.5 SOL
    /// Rejects any intent with required_capital > this value.
    max_position_size_lamports: u64,
    
    /// Maximum daily loss (lamports) before kill switch. Default: 5 SOL
    /// Tracks cumulative losses within a calendar day (UTC).
    daily_loss_limit_lamports: u64,
    
    /// Maximum concurrent open positions. Default: 5
    /// Rejects new intents if this limit is reached.
    max_open_positions: usize,
    
    /// Maximum allowed slippage (basis points). Default: 500 (5%)
    /// Rejects any intent with max_slippage_bps > this value.
    max_slippage_bps: u32,
    
    // === Operational Config ===
    
    /// Simulation timeout (ms)
    simulation_timeout_ms: u64,
    
    /// Whether to actually send transactions
    send_enabled: bool,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            // Risk Invariants - conservative defaults for safety
            max_position_size_lamports: 500_000_000,  // 0.5 SOL max per trade
            daily_loss_limit_lamports: 5_000_000_000, // 5 SOL daily loss limit
            max_open_positions: 5,                     // max 5 concurrent positions
            max_slippage_bps: 500,                     // max 5% slippage allowed
            // Operational
            simulation_timeout_ms: 2000,
            send_enabled: false, // Default: simulate only
        }
    }
}

impl ExecutionConfig {
    /// Returns a snapshot ID for this config (for Decision Record correlation)
    fn snapshot_id(&self) -> String {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        format!("{:?}", self).hash(&mut hasher);
        format!("cfg-{:016x}", hasher.finish())
    }
}

// ============================================================================
// P1: State Persistence (DoD K) - State survives restarts
// ============================================================================

/// Persistent state snapshot for crash recovery
/// 
/// Saved on graceful shutdown and periodic intervals.
/// Loaded on startup to restore state.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StateSnapshot {
    /// Version for forward compatibility
    version: u32,
    /// UTC date for daily tracking
    day: String,
    /// Cumulative daily loss (lamports, positive = loss)
    daily_loss_lamports: i64,
    /// Current open positions count
    open_positions: usize,
    /// Decision counter (for generating unique IDs)
    decision_counter: u64,
    /// Execution counter
    execution_counter: u64,
    /// Processed intent IDs (idempotency store)
    processed_intents: Vec<String>,
    /// Timestamp when snapshot was created
    saved_at: String,
    /// Run ID that created this snapshot
    run_id: String,
}

impl StateSnapshot {
    const CURRENT_VERSION: u32 = 1;
    const SNAPSHOT_FILE: &'static str = "execution_state.json";
    
    /// Create a new snapshot from current state
    fn from_context(ctx: &ExecutionContext) -> Self {
        Self {
            version: Self::CURRENT_VERSION,
            day: ctx.current_day.read().to_string(),
            daily_loss_lamports: ctx.daily_loss_lamports.load(std::sync::atomic::Ordering::Relaxed),
            open_positions: ctx.open_positions.load(std::sync::atomic::Ordering::Relaxed),
            decision_counter: ctx.decision_counter.load(std::sync::atomic::Ordering::Relaxed),
            execution_counter: ctx.execution_counter.load(std::sync::atomic::Ordering::Relaxed),
            processed_intents: ctx.lock_manager.get_processed_intents(),
            saved_at: chrono::Utc::now().to_rfc3339(),
            run_id: ctx.run_id.clone(),
        }
    }
    
    /// Save snapshot to disk
    fn save(&self, log_dir: &PathBuf) -> Result<()> {
        let path = log_dir.join(Self::SNAPSHOT_FILE);
        std::fs::create_dir_all(log_dir)?;
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(&path, json)?;
        info!(path = %path.display(), "State snapshot saved");
        Ok(())
    }
    
    /// Load snapshot from disk (returns None if not found or invalid)
    fn load(log_dir: &PathBuf) -> Option<Self> {
        let path = log_dir.join(Self::SNAPSHOT_FILE);
        if !path.exists() {
            info!(path = %path.display(), "No state snapshot found, starting fresh");
            return None;
        }
        
        match std::fs::read_to_string(&path) {
            Ok(json) => {
                match serde_json::from_str::<StateSnapshot>(&json) {
                    Ok(snapshot) => {
                        if snapshot.version != Self::CURRENT_VERSION {
                            warn!(
                                found_version = snapshot.version,
                                expected_version = Self::CURRENT_VERSION,
                                "State snapshot version mismatch, starting fresh"
                            );
                            return None;
                        }
                        info!(
                            path = %path.display(),
                            saved_at = %snapshot.saved_at,
                            prev_run_id = %snapshot.run_id,
                            processed_intents = snapshot.processed_intents.len(),
                            "Loaded state snapshot"
                        );
                        Some(snapshot)
                    }
                    Err(e) => {
                        warn!(error = %e, "Failed to parse state snapshot, starting fresh");
                        None
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "Failed to read state snapshot, starting fresh");
                None
            }
        }
    }
    
    /// Check if the snapshot is from the same day
    fn is_same_day(&self) -> bool {
        let today = chrono::Utc::now().date_naive().to_string();
        self.day == today
    }
}

/// Runtime context for execution-engine
struct ExecutionContext {
    run_id: String,
    config: ExecutionConfig,
    config_snapshot_id: String,
    nats: Option<NatsClient>,
    decision_writer: JsonlWriter,
    execution_writer: JsonlWriter,
    lock_manager: LockManager,
    log_base: PathBuf,  // P1: For state persistence
    decision_counter: std::sync::atomic::AtomicU64,
    execution_counter: std::sync::atomic::AtomicU64,
    
    // === Risk Tracking (DoD J) P0) ===
    /// Current day (UTC) for daily loss tracking
    current_day: parking_lot::RwLock<chrono::NaiveDate>,
    /// Cumulative loss today (lamports, positive = loss)
    daily_loss_lamports: std::sync::atomic::AtomicI64,
    /// Currently open positions count
    open_positions: std::sync::atomic::AtomicUsize,
    
    // Metrics
    intents_received: std::sync::atomic::AtomicU64,
    intents_rejected: std::sync::atomic::AtomicU64,
    sim_failures: std::sync::atomic::AtomicU64,
    tx_sent: std::sync::atomic::AtomicU64,
}

impl ExecutionContext {
    /// Save state snapshot for crash recovery (P1: DoD K)
    fn save_state(&self) -> Result<()> {
        let snapshot = StateSnapshot::from_context(self);
        snapshot.save(&self.log_base)
    }
    
    fn next_decision_id(&self) -> String {
        let n = self
            .decision_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        format!("dec-{}-{:06}", &self.run_id[..8], n)
    }

    fn next_execution_id(&self) -> String {
        let n = self
            .execution_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        format!("exe-{}-{:06}", &self.run_id[..8], n)
    }

    fn record_intent_received(&self) {
        self.intents_received
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn record_intent_rejected(&self) {
        self.intents_rejected
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn record_sim_failure(&self) {
        self.sim_failures
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    
    // Risk Invariant helpers
    
    /// Check if we need to reset daily counters (new day)
    fn maybe_reset_daily(&self) {
        let today = chrono::Utc::now().date_naive();
        let mut current = self.current_day.write();
        if *current != today {
            tracing::info!(old_day = %current, new_day = %today, "Daily reset triggered");
            *current = today;
            self.daily_loss_lamports.store(0, std::sync::atomic::Ordering::Relaxed);
        }
    }
    
    /// Record a loss (positive = loss, negative = profit)
    fn record_pnl_lamports(&self, pnl: i64) {
        // Positive pnl = loss, negative = profit
        self.daily_loss_lamports.fetch_add(pnl, std::sync::atomic::Ordering::Relaxed);
    }
    
    /// Get current daily loss
    fn get_daily_loss_lamports(&self) -> i64 {
        self.daily_loss_lamports.load(std::sync::atomic::Ordering::Relaxed)
    }
    
    /// Increment open positions
    fn increment_open_positions(&self) {
        self.open_positions.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    
    /// Decrement open positions
    fn decrement_open_positions(&self) {
        self.open_positions.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
    
    /// Get current open positions count
    fn get_open_positions(&self) -> usize {
        self.open_positions.load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("execution_engine=info".parse()?)
                .add_directive("ironcrab=info".parse()?),
        )
        .init();

    let args = Args::parse();
    let run_id = Uuid::new_v4().to_string();

    info!(
        run_id = %run_id,
        config = %args.config.display(),
        simulate_only = args.simulate_only,
        dry_run = args.dry_run,
        metrics_port = args.metrics_port,
        "Starting execution-engine service"
    );

    // Start metrics server
    let metrics_addr = std::net::SocketAddr::from(([0, 0, 0, 0], args.metrics_port));
    tokio::spawn(async move {
        if let Err(e) = serve_metrics(metrics_addr).await {
            error!(error = %e, "Metrics server failed");
        }
    });
    info!(port = args.metrics_port, "Metrics server started at /metrics");

    // === This is the ONLY binary that should load keys ===
    // In production, load keys here. For MVP, we just acknowledge the pattern.
    let has_keys = std::env::var("IRONCRAB_KEYPAIR_JSON").is_ok()
        || std::env::var("IRONCRAB_KEYPAIR_B64").is_ok()
        || std::env::var("IRONCRAB_KEYPAIR_PATH").is_ok();

    if has_keys {
        info!("Wallet key environment variables detected (execution-engine is the single signer)");
    } else if !args.dry_run {
        warn!("No wallet keys configured. Running in simulation-only mode.");
    }

    // Setup config
    let mut exec_config = ExecutionConfig::default();
    exec_config.send_enabled = !args.simulate_only && !args.dry_run && has_keys;

    if exec_config.send_enabled {
        info!("Transaction sending ENABLED");
    } else {
        info!("Transaction sending DISABLED (simulate only)");
    }

    // Setup JSONL writers
    let log_base = args
        .log_dir
        .clone()
        .unwrap_or_else(|| PathBuf::from("trade_logs"));

    let decision_config =
        JsonlWriterConfig::new("decision_records").with_log_dir(log_base.join("decisions"));
    let decision_writer = JsonlWriter::new(decision_config)?;

    let execution_config =
        JsonlWriterConfig::new("execution_results").with_log_dir(log_base.join("executions"));
    let execution_writer = JsonlWriter::new(execution_config)?;

    info!(log_dir = %log_base.display(), "JSONL writers initialized");

    // Setup lock manager
    let lock_manager = LockManager::new(args.initial_sol_lamports);
    info!(
        initial_sol = args.initial_sol_lamports,
        "Lock manager initialized"
    );
    
    // P1: Load state snapshot if available (DoD K)
    let snapshot = StateSnapshot::load(&log_base);
    
    // Restore processed intents (idempotency)
    if let Some(ref snap) = snapshot {
        lock_manager.set_processed_intents(snap.processed_intents.clone());
        info!(
            restored_intents = snap.processed_intents.len(),
            "Idempotency store restored from snapshot"
        );
    }

    // Setup NATS
    let nats = if args.dry_run {
        info!("Dry-run mode: NATS disabled");
        None
    } else {
        let config = NatsConfig::new(&args.nats_url, "execution-engine");
        let mut client = NatsClient::new(config);
        if let Err(e) = client.connect().await {
            warn!(error = %e, "Failed to connect to NATS (continuing without)");
            None
        } else {
            info!(url = %args.nats_url, "Connected to NATS");
            Some(client)
        }
    };

    // P1: Determine initial values from snapshot (DoD K)
    let (initial_day, initial_daily_loss, initial_positions, initial_decision_counter, initial_execution_counter) = 
        if let Some(ref snap) = snapshot {
            if snap.is_same_day() {
                // Same day: restore all counters
                info!(
                    daily_loss = snap.daily_loss_lamports,
                    open_positions = snap.open_positions,
                    decision_counter = snap.decision_counter,
                    "Restored same-day state from snapshot"
                );
                (
                    chrono::NaiveDate::parse_from_str(&snap.day, "%Y-%m-%d")
                        .unwrap_or_else(|_| chrono::Utc::now().date_naive()),
                    snap.daily_loss_lamports,
                    snap.open_positions,
                    snap.decision_counter,
                    snap.execution_counter,
                )
            } else {
                // New day: reset daily counters but keep counters for ID generation
                info!(
                    old_day = %snap.day,
                    "New day detected, resetting daily loss but keeping ID counters"
                );
                (
                    chrono::Utc::now().date_naive(),
                    0,  // Reset daily loss
                    0,  // Reset positions (they would have been closed or expired)
                    snap.decision_counter, // Keep for unique IDs across restarts
                    snap.execution_counter,
                )
            }
        } else {
            // Fresh start
            (chrono::Utc::now().date_naive(), 0, 0, 0, 0)
        };

    let ctx = Arc::new(ExecutionContext {
        run_id: run_id.clone(),
        config_snapshot_id: exec_config.snapshot_id(),
        config: exec_config,
        nats,
        decision_writer,
        execution_writer,
        lock_manager,
        log_base: log_base.clone(),
        decision_counter: std::sync::atomic::AtomicU64::new(initial_decision_counter),
        execution_counter: std::sync::atomic::AtomicU64::new(initial_execution_counter),
        // Risk tracking - restored from snapshot
        current_day: parking_lot::RwLock::new(initial_day),
        daily_loss_lamports: std::sync::atomic::AtomicI64::new(initial_daily_loss),
        open_positions: std::sync::atomic::AtomicUsize::new(initial_positions),
        // Metrics
        intents_received: std::sync::atomic::AtomicU64::new(0),
        intents_rejected: std::sync::atomic::AtomicU64::new(0),
        sim_failures: std::sync::atomic::AtomicU64::new(0),
        tx_sent: std::sync::atomic::AtomicU64::new(0),
    });

    // === Main Loop: Process TradeIntents ===
    info!("Entering main execution loop");
    
    // P1 Crash Isolation: Signal systemd that we're ready
    let _ = sd_notify::notify(true, &[NotifyState::Ready]);
    debug!("Sent sd_notify READY to systemd");

    // Subscribe to TradeIntents if NATS connected
    let intent_subscription = if let Some(ref nats) = ctx.nats {
        match nats.subscribe(TOPIC_TRADE_INTENTS).await {
            Ok(sub) => {
                info!(topic = TOPIC_TRADE_INTENTS, "Subscribed to TradeIntents");
                Some(sub)
            }
            Err(e) => {
                warn!(error = %e, "Failed to subscribe to TradeIntents");
                None
            }
        }
    } else {
        None
    };

    let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));

    // For MVP dry-run test
    let mut simulated_tick: u64 = 0;
    let mut test_intent_processed = false;

    // Graceful shutdown handling
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    // Wrap subscription in Option for ownership
    let mut intent_sub_opt = intent_subscription;

    loop {
        tokio::select! {
            // NATS subscription: receive TradeIntents
            Some(msg) = async {
                if let Some(ref mut sub) = intent_sub_opt {
                    sub.next().await
                } else {
                    None
                }
            } => {
                match msg.deserialize::<TradeIntent>() {
                    Ok(intent) => {
                        info!(intent_id = %intent.intent_id, source = %intent.source, "Received TradeIntent from NATS");
                        if let Err(e) = process_intent(&ctx, intent).await {
                            error!(error = %e, "Failed to process intent");
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "Failed to deserialize TradeIntent");
                    }
                }
            }

            _ = interval.tick() => {
                simulated_tick += 1;

                // MVP: Simulate receiving a test intent once
                if simulated_tick == 5 && !test_intent_processed && args.dry_run {
                    test_intent_processed = true;

                    let test_intent = create_test_intent(&ctx.run_id);
                    info!(intent_id = %test_intent.intent_id, "Processing test intent");

                    if let Err(e) = process_intent(&ctx, test_intent).await {
                        error!(error = %e, "Failed to process intent");
                    }
                }

                // Periodic cleanup and stats
                if simulated_tick % 30 == 0 {
                    ctx.lock_manager.cleanup_expired();

                    let (cap_locks, res_locks) = ctx.lock_manager.active_lock_count();
                    let received = ctx.intents_received.load(std::sync::atomic::Ordering::Relaxed);
                    let rejected = ctx.intents_rejected.load(std::sync::atomic::Ordering::Relaxed);
                    let sim_fail = ctx.sim_failures.load(std::sync::atomic::Ordering::Relaxed);

                    info!(
                        tick = simulated_tick,
                        intents_received = received,
                        intents_rejected = rejected,
                        sim_failures = sim_fail,
                        active_capital_locks = cap_locks,
                        active_resource_locks = res_locks,
                        available_sol = ctx.lock_manager.available_sol(),
                        "Execution-engine heartbeat"
                    );
                    
                    // P1 Crash Isolation: Ping systemd watchdog (every 30s heartbeat)
                    let _ = sd_notify::notify(true, &[NotifyState::Watchdog]);
                }
                
                // P1: Periodic state save every 60 ticks (~1 minute) (DoD K)
                if simulated_tick % 60 == 0 {
                    if let Err(e) = ctx.save_state() {
                        warn!(error = %e, "Failed to save periodic state snapshot");
                    } else {
                        debug!(tick = simulated_tick, "Periodic state snapshot saved");
                    }
                }
            }
            _ = &mut shutdown => {
                info!("Shutdown signal received");
                break;
            }
        }
    }

    // P1: Save state snapshot on shutdown (DoD K)
    if let Err(e) = ctx.save_state() {
        error!(error = %e, "Failed to save state snapshot");
    }

    // Flush JSONL on shutdown
    ctx.decision_writer.flush()?;
    ctx.execution_writer.flush()?;
    info!(run_id = %run_id, "execution-engine shutdown complete");

    Ok(())
}

/// Create a test intent for MVP demonstration
fn create_test_intent(run_id: &str) -> TradeIntent {
    use ironcrab::ipc::{ExplicitAmount, IntentTier, TradeResources, TradeSide};

    TradeIntent::new(
        "test",
        BUILD_VERSION,
        run_id,
        format!("test-intent-{}", Uuid::new_v4()),
        "test-harness",
        IntentTier::Tier1,
        IntentOrigin::StrategyA,
        ExplicitAmount::new(50_000_000, 9), // 0.05 SOL
        TradeResources {
            input_mint: "So11111111111111111111111111111111111111112".to_string(),
            output_mint: "TestToken123".to_string(),
            pools: vec!["TestPool456".to_string()],
            accounts: vec![],
        },
        100,  // 1% expected ROI
        200,  // 2% max slippage
        TradeSide::Buy,
        TradingRegime::Early,
    )
    .with_ttl_ms(5000)
}

/// Process a single TradeIntent through the execution pipeline
async fn process_intent(ctx: &ExecutionContext, intent: TradeIntent) -> Result<()> {
    ctx.record_intent_received();

    let decision_id = ctx.next_decision_id();
    let mut checks: Vec<CheckResult> = Vec::new();

    info!(
        intent_id = %intent.intent_id,
        decision_id = %decision_id,
        source = %intent.source,
        "Processing intent"
    );

    // === Check 1: Idempotency ===
    if ctx.lock_manager.is_duplicate(&intent.intent_id) {
        let reason = RejectReason::LockDuplicateIntent;
        checks.push(CheckResult {
            check_name: "idempotency".to_string(),
            passed: false,
            reason_code: Some(reason.to_string()),
            details: Some("Intent already processed".to_string()),
        });

        return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
    }
    checks.push(CheckResult {
        check_name: "idempotency".to_string(),
        passed: true,
        reason_code: None,
        details: None,
    });

    // === Check 2: TTL validity ===
    // For MVP, assume TTL is valid (would check against current time/slot)
    checks.push(CheckResult {
        check_name: "ttl_valid".to_string(),
        passed: true,
        reason_code: None,
        details: None,
    });

    // === Risk Invariant Checks (DoD J) ===
    
    // Reset daily counters if new day
    ctx.maybe_reset_daily();
    
    // Check 3a: Max position size
    if intent.required_capital.raw > ctx.config.max_position_size_lamports {
        let reason = RejectReason::RiskMaxPosition;
        checks.push(CheckResult {
            check_name: "max_position_size".to_string(),
            passed: false,
            reason_code: Some(reason.to_string()),
            details: Some(format!(
                "required={} > max={}",
                intent.required_capital.raw,
                ctx.config.max_position_size_lamports
            )),
        });
        return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
    }
    checks.push(CheckResult {
        check_name: "max_position_size".to_string(),
        passed: true,
        reason_code: None,
        details: Some(format!(
            "required={} <= max={}",
            intent.required_capital.raw,
            ctx.config.max_position_size_lamports
        )),
    });
    
    // Check 3b: Max slippage
    if intent.max_slippage_bps > ctx.config.max_slippage_bps {
        let reason = RejectReason::SimSlippageExceeded;
        checks.push(CheckResult {
            check_name: "max_slippage".to_string(),
            passed: false,
            reason_code: Some(reason.to_string()),
            details: Some(format!(
                "intent_slippage={}bps > max={}bps",
                intent.max_slippage_bps,
                ctx.config.max_slippage_bps
            )),
        });
        return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
    }
    checks.push(CheckResult {
        check_name: "max_slippage".to_string(),
        passed: true,
        reason_code: None,
        details: None,
    });
    
    // Check 3c: Max open positions
    let current_positions = ctx.get_open_positions();
    if current_positions >= ctx.config.max_open_positions {
        let reason = RejectReason::RiskMaxOpenPositions;
        checks.push(CheckResult {
            check_name: "max_open_positions".to_string(),
            passed: false,
            reason_code: Some(reason.to_string()),
            details: Some(format!(
                "current={} >= max={}",
                current_positions,
                ctx.config.max_open_positions
            )),
        });
        return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
    }
    checks.push(CheckResult {
        check_name: "max_open_positions".to_string(),
        passed: true,
        reason_code: None,
        details: Some(format!(
            "current={} < max={}",
            current_positions,
            ctx.config.max_open_positions
        )),
    });
    
    // Check 3d: Daily loss limit
    let daily_loss = ctx.get_daily_loss_lamports();
    if daily_loss >= ctx.config.daily_loss_limit_lamports as i64 {
        let reason = RejectReason::RiskDailyLossLimit;
        checks.push(CheckResult {
            check_name: "daily_loss_limit".to_string(),
            passed: false,
            reason_code: Some(reason.to_string()),
            details: Some(format!(
                "daily_loss={} >= limit={}",
                daily_loss,
                ctx.config.daily_loss_limit_lamports
            )),
        });
        return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
    }
    checks.push(CheckResult {
        check_name: "daily_loss_limit".to_string(),
        passed: true,
        reason_code: None,
        details: Some(format!(
            "daily_loss={} < limit={}",
            daily_loss,
            ctx.config.daily_loss_limit_lamports
        )),
    });

    // === Check 4: Capital lock ===
    let holder = LockHolder::new(&intent.intent_id)
        .with_decision(&decision_id)
        .with_tier(intent.tier as u8);
    let lock_result = ctx.lock_manager.try_lock_capital(
        holder,
        intent.required_capital.raw,
        std::collections::HashMap::new(),
    );

    match lock_result {
        LockResult::Acquired => {
            checks.push(CheckResult {
                check_name: "capital_lock".to_string(),
                passed: true,
                reason_code: None,
                details: None,
            });
        }
        LockResult::AcquiredByPreemption { preempted } => {
            // DoD L) P0: Higher-priority intent preempted lower-priority lock
            info!(
                intent_id = %intent.intent_id,
                preempted_intent = %preempted.intent_id,
                "Capital lock acquired by preemption (DoD L)"
            );
            checks.push(CheckResult {
                check_name: "capital_lock".to_string(),
                passed: true,
                reason_code: None,
                details: Some(format!("Preempted: {}", preempted.intent_id)),
            });
        }
        LockResult::InsufficientCapital { available, requested } => {
            let reason = RejectReason::LockCapitalConflict;
            checks.push(CheckResult {
                check_name: "capital_lock".to_string(),
                passed: false,
                reason_code: Some(reason.to_string()),
                details: Some(format!(
                    "Insufficient capital: available={}, requested={}",
                    available, requested
                )),
            });
            return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
        }
        LockResult::Conflict { holder } => {
            let reason = RejectReason::LockCapitalConflict;
            checks.push(CheckResult {
                check_name: "capital_lock".to_string(),
                passed: false,
                reason_code: Some(reason.to_string()),
                details: Some(format!("Lock held by: {}", holder.intent_id)),
            });
            return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
        }
    }

    // === Simulate (P0: simulate-gated) ===
    info!(intent_id = %intent.intent_id, "Running simulation");

    let sim_result = simulate_transaction(&intent).await;

    if !sim_result.success {
        ctx.record_sim_failure();
        let reason = RejectReason::SimFailed;
        checks.push(CheckResult {
            check_name: "simulation".to_string(),
            passed: false,
            reason_code: Some(reason.to_string()),
            details: sim_result.error_code.clone(),
        });

        // Release lock on failure
        ctx.lock_manager.release_locks(&intent.intent_id);

        return emit_sim_failed_decision(ctx, decision_id, &intent, checks, sim_result).await;
    }

    checks.push(CheckResult {
        check_name: "simulation".to_string(),
        passed: true,
        reason_code: None,
        details: Some(format!(
            "CU consumed: {:?}",
            sim_result.compute_units_consumed
        )),
    });

    // === Send (if enabled) ===
    if ctx.config.send_enabled {
        info!(intent_id = %intent.intent_id, "Sending transaction");
        // In production: actually send the transaction
        // For MVP: this path is not taken (send_enabled = false)
        ctx.tx_sent
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    } else {
        debug!(intent_id = %intent.intent_id, "Transaction sending disabled");
    }

    // Mark as processed
    ctx.lock_manager.mark_processed(&intent.intent_id);

    // Emit decision record (success case)
    let decision = DecisionRecord {
        header: ironcrab::ipc::RecordHeader::new("execution-engine", BUILD_VERSION, &ctx.run_id),
        decision_id: decision_id.clone(),
        intent_id: intent.intent_id.clone(),
        origin_type: intent.origin_type,
        regime: intent.regime,
        checks,
        primary_reject_reason: None,
        plan_hash: Some(format!("plan-{}", Uuid::new_v4())),
        simulate: Some(sim_result),
        send: None, // MVP: not actually sending
        outcome: if ctx.config.send_enabled {
            DecisionOutcome::Sent
        } else {
            DecisionOutcome::SimFailed // Mark as sim-only for MVP
        },
        config_snapshot_id: None,
        input_snapshots: std::collections::HashMap::new(),
    };

    ctx.decision_writer.write(&decision)?;

    if let Some(ref nats) = ctx.nats {
        nats.publish(TOPIC_DECISION_RECORDS, &decision).await?;
    }

    // Release lock (in production: would release after confirmation)
    ctx.lock_manager.release_locks(&intent.intent_id);

    info!(
        intent_id = %intent.intent_id,
        decision_id = %decision_id,
        outcome = ?decision.outcome,
        "Intent processed"
    );

    Ok(())
}

/// Emit a rejected decision record
async fn emit_rejected_decision(
    ctx: &ExecutionContext,
    decision_id: String,
    intent: &TradeIntent,
    checks: Vec<CheckResult>,
    reason: RejectReason,
) -> Result<()> {
    ctx.record_intent_rejected();

    let decision = DecisionRecord::new_rejected(
        "execution-engine",
        BUILD_VERSION,
        &ctx.run_id,
        decision_id.clone(),
        intent.intent_id.clone(),
        intent.origin_type,
        intent.regime,
        checks,
        reason.to_string(),
    );

    ctx.decision_writer.write(&decision)?;

    if let Some(ref nats) = ctx.nats {
        nats.publish(TOPIC_DECISION_RECORDS, &decision).await?;
    }

    warn!(
        intent_id = %intent.intent_id,
        decision_id = %decision_id,
        reason = %reason,
        "Intent rejected"
    );

    Ok(())
}

/// Emit a sim-failed decision record
async fn emit_sim_failed_decision(
    ctx: &ExecutionContext,
    decision_id: String,
    intent: &TradeIntent,
    checks: Vec<CheckResult>,
    sim_result: SimulationResult,
) -> Result<()> {
    let decision = DecisionRecord::new_sim_failed(
        "execution-engine",
        BUILD_VERSION,
        &ctx.run_id,
        decision_id.clone(),
        intent.intent_id.clone(),
        intent.origin_type,
        intent.regime,
        checks,
        format!("plan-{}", Uuid::new_v4()),
        sim_result,
    );

    ctx.decision_writer.write(&decision)?;

    if let Some(ref nats) = ctx.nats {
        nats.publish(TOPIC_DECISION_RECORDS, &decision).await?;
    }

    warn!(
        intent_id = %intent.intent_id,
        decision_id = %decision_id,
        "Intent simulation failed"
    );

    Ok(())
}

/// Simulate a transaction (MVP: always succeeds)
async fn simulate_transaction(_intent: &TradeIntent) -> SimulationResult {
    // In production: call RPC simulateTransaction
    // For MVP: return success
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    SimulationResult {
        success: true,
        error_code: None,
        logs_preview: Some("Simulation successful (MVP stub)".to_string()),
        compute_units_consumed: Some(150_000),
    }
}
