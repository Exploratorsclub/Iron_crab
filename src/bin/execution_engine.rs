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

use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use ironcrab::ipc::{
    CheckResult, DecisionOutcome, DecisionRecord, ExecutionResult, ExecutionStatus, IntentOrigin,
    RejectReason, SimulationResult, TradeIntent, TradingRegime,
};
use ironcrab::nats::{
    NatsClient, NatsConfig, TOPIC_DECISION_RECORDS, TOPIC_EXECUTION_RESULTS, TOPIC_TRADE_INTENTS,
};
use ironcrab::storage::{
    locks::{LockHolder, LockManager, LockResult},
    JsonlWriter, JsonlWriterConfig,
};

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
#[derive(Debug, Clone)]
struct ExecutionConfig {
    /// Maximum daily loss (lamports) before kill switch
    daily_loss_limit_lamports: u64,
    /// Maximum concurrent open positions
    max_open_positions: usize,
    /// Simulation timeout (ms)
    simulation_timeout_ms: u64,
    /// Whether to actually send transactions
    send_enabled: bool,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            daily_loss_limit_lamports: 5_000_000_000, // 5 SOL
            max_open_positions: 5,
            simulation_timeout_ms: 2000,
            send_enabled: false, // Default: simulate only
        }
    }
}

/// Runtime context for execution-engine
struct ExecutionContext {
    run_id: String,
    config: ExecutionConfig,
    nats: Option<NatsClient>,
    decision_writer: JsonlWriter,
    execution_writer: JsonlWriter,
    lock_manager: LockManager,
    decision_counter: std::sync::atomic::AtomicU64,
    execution_counter: std::sync::atomic::AtomicU64,
    // Metrics
    intents_received: std::sync::atomic::AtomicU64,
    intents_rejected: std::sync::atomic::AtomicU64,
    sim_failures: std::sync::atomic::AtomicU64,
    tx_sent: std::sync::atomic::AtomicU64,
}

impl ExecutionContext {
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
        "Starting execution-engine service"
    );

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

    let ctx = Arc::new(ExecutionContext {
        run_id: run_id.clone(),
        config: exec_config,
        nats,
        decision_writer,
        execution_writer,
        lock_manager,
        decision_counter: std::sync::atomic::AtomicU64::new(0),
        execution_counter: std::sync::atomic::AtomicU64::new(0),
        intents_received: std::sync::atomic::AtomicU64::new(0),
        intents_rejected: std::sync::atomic::AtomicU64::new(0),
        sim_failures: std::sync::atomic::AtomicU64::new(0),
        tx_sent: std::sync::atomic::AtomicU64::new(0),
    });

    // === Main Loop: Process TradeIntents ===
    info!("Entering main execution loop");

    let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));

    // For MVP, simulate receiving an intent
    let mut simulated_tick: u64 = 0;
    let mut test_intent_processed = false;

    // Graceful shutdown handling
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
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
                }
            }
            _ = &mut shutdown => {
                info!("Shutdown signal received");
                break;
            }
        }
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

    // === Check 3: Capital lock ===
    let holder = LockHolder::new(&intent.intent_id).with_decision(&decision_id);
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
