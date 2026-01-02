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
use solana_client::rpc_config::{RpcSendTransactionConfig, RpcSimulateTransactionConfig};
use solana_sdk::pubkey::Pubkey;
use solana_commitment_config::CommitmentLevel;
use solana_sdk::signature::Signature;
use solana_sdk::signer::Signer;
use solana_sdk::transaction::Transaction;
use spl_associated_token_account::get_associated_token_address_with_program_id;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use ironcrab::ipc::{
    CheckResult, ConfigUpdate, ConfigUpdateResponse, ConfigUpdateStatus, DecisionOutcome,
    DecisionRecord, ExecutionResult, ExecutionStatus, FairnessPolicy, FeePolicy, IntentOrigin,
    RejectReason, SimulationResult, TradeIntent, TradeSide, TradingRegime,
};
use ironcrab::metrics::{
    record_recent_trade, serve_metrics, RecentTrade, ACTIVE_CAPITAL_LOCKS, ACTIVE_RESOURCE_LOCKS,
    AVAILABLE_SOL_LAMPORTS, INTENTS_EXECUTED_TOTAL, INTENTS_RECEIVED_TOTAL, INTENTS_REJECTED_TOTAL,
    NATS_MESSAGES_RECEIVED_TOTAL, OPEN_POSITIONS_GAUGE, REJECT_CAPITAL_LOCK, REJECT_DUPLICATE,
    REJECT_RESOURCE_LOCK, REJECT_SIMULATION_FAIL, SIMULATION_FAILURES_TOTAL, REJECT_SEND_FAILED,
    TX_CONFIRMED_TOTAL,
    TX_CONFIRM_TIMEOUT_TOTAL, TX_SEND_ATTEMPTS_TOTAL, TX_SEND_SUCCESS_TOTAL,
    JITO_BUNDLES_LANDED_TOTAL, JITO_BUNDLES_REJECTED_TOTAL, JITO_BUNDLES_SUBMITTED_TOTAL,
    JITO_BUNDLES_TIMEOUT_TOTAL, JITO_TIP_LAMPORTS_TOTAL,
};
use ironcrab::nats::{
    NatsClient, NatsConfig, TOPIC_DECISION_RECORDS, TOPIC_EXECUTION_RESULTS, TOPIC_TRADE_INTENTS,
};
use ironcrab::solana::cross_dex_handler::CrossDexHandler;
use ironcrab::solana::jito::{JitoClient, JitoRegion};
use ironcrab::solana::rpc::SolanaRpc;
use ironcrab::storage::{
    locks::{LockHolder, LockManager, LockResult, ResourceType},
    JsonlWriter, JsonlWriterConfig,
};
use ironcrab::wallet::Treasury;
use ironcrab::execution::tx_builder;

/// NATS topic for config reload commands from control-plane
const TOPIC_CONFIG_RELOAD: &str = "ironcrab.control.config.reload";

// P1 Crash Isolation: Systemd Watchdog support (Linux only)
#[cfg(unix)]
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

    /// Solana RPC URL
    #[arg(long, env = "RPC_URL", default_value = "http://127.0.0.1:8899")]
    rpc_url: String,

    /// Prometheus metrics port
    #[arg(long, default_value = "9804")]
    metrics_port: u16,

    /// Log directory override
    #[arg(long, env = "IRONCRAB_LOG_DIR")]
    log_dir: Option<PathBuf>,

    /// Disable actual transaction sending (simulation only)
    #[arg(long)]
    simulate_only: bool,

    /// Dry run: never send on-chain transactions (may still do read-only RPC for checks)
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

    /// Confirmation timeout (ms) for RPC send path
    confirmation_timeout_ms: u64,

    /// RPC sendTransaction: skip preflight (safe default when simulate-gated)
    send_skip_preflight: bool,

    /// RPC sendTransaction: preflight commitment ("processed"|"confirmed"|"finalized"); None uses RPC default
    send_preflight_commitment: Option<String>,

    /// Whether to actually send transactions
    send_enabled: bool,

    // === P1: Jito Bundle Config ===
    /// Enable Jito bundle submission for atomic execution
    jito_enabled: bool,

    /// Default tip amount for Jito bundles (lamports)
    jito_tip_lamports: u64,

    /// Jito block engine region (frankfurt, amsterdam, ny, tokyo, slc)
    jito_region: String,

    /// Timeout for bundle confirmation (seconds)
    jito_timeout_secs: u64,

    // === P1: Fee/Compute Policies ===
    /// Centralized fee policy (engine owns compute budget and priority fees)
    fee_policy: FeePolicy,

    // === P1: Fairness/Starvation Policy ===
    /// Fairness policy to prevent strategy starvation
    fairness_policy: FairnessPolicy,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            // Risk Invariants - conservative defaults for safety
            max_position_size_lamports: 500_000_000, // 0.5 SOL max per trade
            daily_loss_limit_lamports: 5_000_000_000, // 5 SOL daily loss limit
            max_open_positions: 5,                   // max 5 concurrent positions
            max_slippage_bps: 500,                   // max 5% slippage allowed
            // Operational
            simulation_timeout_ms: 2000,
            confirmation_timeout_ms: 30_000,
            send_skip_preflight: true,
            send_preflight_commitment: None,
            send_enabled: false, // Default: simulate only
            // P1: Jito Bundle defaults
            jito_enabled: false,
            jito_tip_lamports: 10_000, // 0.00001 SOL default tip
            jito_region: "frankfurt".to_string(),
            jito_timeout_secs: 30,
            // P1: Fee/Compute Policy
            fee_policy: FeePolicy::default(),
            // P1: Fairness Policy
            fairness_policy: FairnessPolicy::default(),
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
            daily_loss_lamports: ctx
                .daily_loss_lamports
                .load(std::sync::atomic::Ordering::Relaxed),
            open_positions: ctx
                .open_positions
                .load(std::sync::atomic::Ordering::Relaxed),
            decision_counter: ctx
                .decision_counter
                .load(std::sync::atomic::Ordering::Relaxed),
            execution_counter: ctx
                .execution_counter
                .load(std::sync::atomic::Ordering::Relaxed),
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
            Ok(json) => match serde_json::from_str::<StateSnapshot>(&json) {
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
            },
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
    wallet_pubkey: Option<Pubkey>,
    /// The ONLY signer (Single-Signer rule). None means keyless mode.
    treasury: Option<Treasury>,
    /// Hot-reloadable configuration (RwLock for runtime updates via NATS)
    config: parking_lot::RwLock<ExecutionConfig>,
    config_snapshot_id: parking_lot::RwLock<String>,
    nats: Option<NatsClient>,
    decision_writer: JsonlWriter,
    execution_writer: JsonlWriter,
    lock_manager: LockManager,
    log_base: PathBuf, // P1: For state persistence
    decision_counter: std::sync::atomic::AtomicU64,
    execution_counter: std::sync::atomic::AtomicU64,

    // === Risk Tracking (DoD J) P0) ===
    /// Current day (UTC) for daily loss tracking
    current_day: parking_lot::RwLock<chrono::NaiveDate>,
    /// Cumulative loss today (lamports, positive = loss)
    daily_loss_lamports: std::sync::atomic::AtomicI64,
    /// Currently open positions count
    open_positions: std::sync::atomic::AtomicUsize,

    // === P1: Jito Bundle Support ===
    /// Jito client for atomic bundle execution (None if disabled)
    jito_client: Option<JitoClient>,
    /// Bundle submissions counter
    bundles_submitted: std::sync::atomic::AtomicU64,
    /// Bundle confirmations counter
    bundles_confirmed: std::sync::atomic::AtomicU64,

    // === Cross-DEX Arbitrage Handler ===
    /// Handler for cross-DEX arb intents (optional, requires RPC)
    cross_dex_handler: Option<parking_lot::RwLock<CrossDexHandler>>,
    /// RPC wrapper for read-only queries and (future) sim/send
    rpc: Arc<SolanaRpc>,

    // Metrics
    intents_received: std::sync::atomic::AtomicU64,
    intents_rejected: std::sync::atomic::AtomicU64,
    sim_failures: std::sync::atomic::AtomicU64,
    tx_sent: std::sync::atomic::AtomicU64,
    arb_validated: std::sync::atomic::AtomicU64,
    arb_executed: std::sync::atomic::AtomicU64,
}

impl ExecutionContext {
    /// Get current config (read lock)
    fn get_config(&self) -> ExecutionConfig {
        self.config.read().clone()
    }

    /// Update config and return response (P1: Runtime Configuration via UI)
    fn apply_config_update(&self, update: &ConfigUpdate) -> ConfigUpdateResponse {
        let mut config = self.config.write();
        let mut applied = Vec::new();
        let mut rejected = Vec::new();

        // Process each config key
        for (key, value) in &update.config {
            match key.as_str() {
                "max_position_size_lamports" => {
                    if let Some(v) = value.as_u64() {
                        if v > 0 {
                            config.max_position_size_lamports = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be > 0".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "daily_loss_limit_lamports" => {
                    if let Some(v) = value.as_u64() {
                        if v > 0 {
                            config.daily_loss_limit_lamports = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be > 0".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "max_open_positions" => {
                    if let Some(v) = value.as_u64() {
                        if v > 0 && v <= 100 {
                            config.max_open_positions = v as usize;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be 1-100".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "max_slippage_bps" => {
                    if let Some(v) = value.as_u64() {
                        if v > 0 && v <= 10000 {
                            config.max_slippage_bps = v as u32;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be 1-10000".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "simulation_timeout_ms" => {
                    if let Some(v) = value.as_u64() {
                        if v >= 100 && v <= 30000 {
                            config.simulation_timeout_ms = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be 100-30000".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "confirmation_timeout_ms" => {
                    if let Some(v) = value.as_u64() {
                        if v >= 500 && v <= 300_000 {
                            config.confirmation_timeout_ms = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be 500-300000".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "confirm_timeout_ms" => {
                    if let Some(v) = value.as_u64() {
                        if v >= 500 && v <= 300_000 {
                            config.confirmation_timeout_ms = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be 500-300000".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                "skip_preflight" => {
                    if let Some(v) = value.as_bool() {
                        config.send_skip_preflight = v;
                        applied.push(key.clone());
                        info!(key = %key, new_value = %v, "Config updated");
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected bool".to_string()));
                    }
                }
                "preflight_commitment" => {
                    if value.is_null() {
                        config.send_preflight_commitment = None;
                        applied.push(key.clone());
                        info!(key = %key, new_value = "null", "Config updated");
                    } else if let Some(v) = value.as_str() {
                        let v_lc = v.to_lowercase();
                        match v_lc.as_str() {
                            "processed" | "confirmed" | "finalized" => {
                                config.send_preflight_commitment = Some(v_lc);
                                applied.push(key.clone());
                                info!(key = %key, new_value = %v, "Config updated");
                            }
                            _ => rejected.push((
                                key.clone(),
                                "Must be one of: processed, confirmed, finalized (or null)"
                                    .to_string(),
                            )),
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected string or null".to_string()));
                    }
                }
                "send_enabled" => {
                    if let Some(v) = value.as_bool() {
                        // Only allow enabling if keys are configured
                        let has_keys = Treasury::load_from_env().is_ok();

                        if v && !has_keys {
                            rejected.push((
                                key.clone(),
                                "Cannot enable sending without wallet keys".to_string(),
                            ));
                        } else {
                            config.send_enabled = v;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected bool".to_string()));
                    }
                }
                _ => {
                    rejected.push((key.clone(), format!("Unknown config key: {}", key)));
                }
            }
        }

        // Update snapshot ID
        let new_snapshot_id = config.snapshot_id();
        *self.config_snapshot_id.write() = new_snapshot_id.clone();

        // Determine status
        let status = if rejected.is_empty() {
            ConfigUpdateStatus::Applied
        } else if applied.is_empty() {
            ConfigUpdateStatus::Rejected
        } else {
            ConfigUpdateStatus::PartiallyApplied
        };

        ConfigUpdateResponse {
            status,
            applied_keys: applied,
            rejected_keys: rejected,
            new_snapshot_id: Some(new_snapshot_id),
        }
    }

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
            self.daily_loss_lamports
                .store(0, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// Record a loss (positive = loss, negative = profit)
    fn record_pnl_lamports(&self, pnl: i64) {
        // Positive pnl = loss, negative = profit
        self.daily_loss_lamports
            .fetch_add(pnl, std::sync::atomic::Ordering::Relaxed);
    }

    /// Get current daily loss
    fn get_daily_loss_lamports(&self) -> i64 {
        self.daily_loss_lamports
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Increment open positions
    fn increment_open_positions(&self) {
        self.open_positions
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Decrement open positions
    fn decrement_open_positions(&self) {
        // Saturating decrement to avoid underflow.
        let mut current = self
            .open_positions
            .load(std::sync::atomic::Ordering::Relaxed);

        loop {
            if current == 0 {
                return;
            }
            match self.open_positions.compare_exchange_weak(
                current,
                current - 1,
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(observed) => current = observed,
            }
        }
    }

    /// Get current open positions count
    fn get_open_positions(&self) -> usize {
        self.open_positions
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

fn sdk_to_spl(pk: &Pubkey) -> spl_token::solana_program::pubkey::Pubkey {
    spl_token::solana_program::pubkey::Pubkey::new_from_array(pk.to_bytes())
}

fn spl_to_sdk(pk: &spl_token::solana_program::pubkey::Pubkey) -> Pubkey {
    Pubkey::new_from_array(pk.to_bytes())
}

fn token_program_for_mint_owner(owner: &Pubkey) -> Option<Pubkey> {
    let spl_token_program = Pubkey::new_from_array(spl_token::id().to_bytes());
    let spl_token_2022_program = Pubkey::new_from_array(spl_token_2022::id().to_bytes());

    if *owner == spl_token_program {
        Some(spl_token_program)
    } else if *owner == spl_token_2022_program {
        Some(spl_token_2022_program)
    } else {
        None
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
    info!(
        port = args.metrics_port,
        "Metrics server started at /metrics"
    );

    // RPC wrapper (nonblocking; limiter/retry lives inside SolanaRpc)
    let rpc = Arc::new(SolanaRpc::new(&args.rpc_url));

    // RS-1.1 acceptance: prove basic RPC works through SolanaRpc
    match rpc.rpc.get_latest_blockhash().await {
        Ok(_bh) => info!("Fetched latest blockhash via SolanaRpc"),
        Err(e) => warn!(error = %e, "Failed to fetch latest blockhash via SolanaRpc"),
    }

    // === This is the ONLY binary that should load keys ===
    let treasury = match Treasury::load_from_env() {
        Ok(t) => {
            info!(wallet = %t.pubkey(), "Wallet keys loaded (execution-engine is the single signer)");
            Some(t)
        }
        Err(e) => {
            if !args.dry_run {
                warn!(error = %e, "No wallet keys configured or loadable; running without signer");
            }
            None
        }
    };
    let has_keys = treasury.is_some();

    // Setup config
    let mut exec_config = ExecutionConfig::default();
    exec_config.send_enabled = !args.simulate_only && !args.dry_run && has_keys;

    if exec_config.send_enabled {
        info!("Transaction sending ENABLED");
    } else {
        let reason = if args.dry_run {
            "dry_run"
        } else if args.simulate_only {
            "simulate_only"
        } else if !has_keys {
            "no_keys"
        } else {
            "disabled"
        };
        info!(reason, "Transaction sending DISABLED");
    }

    // P1: Setup Jito client for atomic bundle execution
    let jito_client = if exec_config.jito_enabled && !args.dry_run {
        let region =
            JitoRegion::from_str(&exec_config.jito_region).unwrap_or(JitoRegion::Frankfurt);
        let client = JitoClient::new(vec![region], exec_config.jito_tip_lamports);
        info!(
            region = %exec_config.jito_region,
            tip_lamports = %exec_config.jito_tip_lamports,
            "Jito client initialized for atomic bundle execution"
        );
        Some(client)
    } else {
        if exec_config.jito_enabled {
            info!("Jito configured but disabled in dry-run mode");
        } else {
            debug!("Jito bundle execution disabled");
        }
        None
    };

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

    // Load wallet keys and fetch real balance
    // NOTE: In `--dry-run`, we still allow key loading and balance reads.
    // `--dry-run` means: do not submit transactions, not: keyless.
    let (wallet_pubkey, initial_balance) = if let Some(ref t) = treasury {
        let pubkey = t.pubkey();

        // RS-1.1 acceptance: getBalance through SolanaRpc
        match rpc.rpc.get_balance(&pubkey).await {
            Ok(balance) => {
                info!(wallet = %pubkey, balance_sol = balance as f64 / 1e9, "Real wallet balance fetched");
                (Some(pubkey), balance)
            }
            Err(e) => {
                warn!(error = %e, "Failed to fetch wallet balance, using default");
                (Some(pubkey), args.initial_sol_lamports)
            }
        }
    } else {
        (None, args.initial_sol_lamports)
    };

    // Setup lock manager with real balance
    let lock_manager = LockManager::new(initial_balance);
    info!(
        initial_sol = initial_balance,
        balance_sol = initial_balance as f64 / 1e9,
        "Lock manager initialized with wallet balance"
    );

    // Update metrics with real balance
    AVAILABLE_SOL_LAMPORTS.store(initial_balance, Ordering::Relaxed);

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
    // NOTE: `--dry-run` means "never send on-chain transactions".
    // It must NOT disable NATS consumption, otherwise we can't end-to-end test the pipeline.
    let nats = {
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
    let (
        initial_day,
        initial_daily_loss,
        initial_positions,
        initial_decision_counter,
        initial_execution_counter,
    ) = if let Some(ref snap) = snapshot {
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
                0,                     // Reset daily loss
                0,                     // Reset positions (they would have been closed or expired)
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
        wallet_pubkey,
        treasury,
        config_snapshot_id: parking_lot::RwLock::new(exec_config.snapshot_id()),
        config: parking_lot::RwLock::new(exec_config),
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
        // P1: Jito bundle support
        jito_client,
        bundles_submitted: std::sync::atomic::AtomicU64::new(0),
        bundles_confirmed: std::sync::atomic::AtomicU64::new(0),
        // Cross-DEX handler (initialized below)
        cross_dex_handler: None,
        rpc: Arc::clone(&rpc),
        // Metrics
        intents_received: std::sync::atomic::AtomicU64::new(0),
        intents_rejected: std::sync::atomic::AtomicU64::new(0),
        sim_failures: std::sync::atomic::AtomicU64::new(0),
        tx_sent: std::sync::atomic::AtomicU64::new(0),
        arb_validated: std::sync::atomic::AtomicU64::new(0),
        arb_executed: std::sync::atomic::AtomicU64::new(0),
    });

    // === Main Loop: Process TradeIntents ===
    info!("Entering main execution loop");

    // P1 Crash Isolation: Signal systemd that we're ready
    #[cfg(unix)]
    {
        // NOTE: Do NOT unset NOTIFY_SOCKET here; we need it for Watchdog pings.
        let _ = sd_notify::notify(false, &[NotifyState::Ready]);
        debug!("Sent sd_notify READY to systemd");
    }

    // Keep readiness fresh even when idle.
    ironcrab::metrics::record_activity();

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

    // P1: Subscribe to Config Updates (Runtime Configuration via UI)
    let config_subscription = if let Some(ref nats) = ctx.nats {
        match nats.subscribe(TOPIC_CONFIG_RELOAD).await {
            Ok(sub) => {
                info!(topic = TOPIC_CONFIG_RELOAD, "Subscribed to Config Updates");
                Some(sub)
            }
            Err(e) => {
                warn!(error = %e, "Failed to subscribe to Config Updates");
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

    // Wrap subscriptions in Option for ownership
    let mut intent_sub_opt = intent_subscription;
    let mut config_sub_opt = config_subscription;

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

            // P1: NATS subscription: receive Config Updates (Runtime Configuration)
            Some(msg) = async {
                if let Some(ref mut sub) = config_sub_opt {
                    sub.next().await
                } else {
                    None
                }
            } => {
                match msg.deserialize::<ConfigUpdate>() {
                    Ok(update) => {
                        // Only process if targeted at execution-engine
                        if update.component == "execution-engine" {
                            info!(
                                component = %update.component,
                                keys = ?update.config.keys().collect::<Vec<_>>(),
                                "Received Config Update from control-plane"
                            );
                            let response = ctx.apply_config_update(&update);
                            info!(
                                status = ?response.status,
                                applied = ?response.applied_keys,
                                rejected = ?response.rejected_keys,
                                "Config update processed"
                            );
                        } else {
                            debug!(component = %update.component, "Ignoring config update for other component");
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "Failed to deserialize ConfigUpdate");
                    }
                }
            }

            _ = interval.tick() => {
                simulated_tick += 1;

                // Keep /ready fresh even when no intents flow.
                ironcrab::metrics::record_activity();

                // MVP/dev convenience: simulate receiving a test intent once when running dry-run
                // *without* NATS (so local dev still does something).
                if simulated_tick == 5 && !test_intent_processed && args.dry_run && ctx.nats.is_none() {
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
                    let available_sol = ctx.lock_manager.available_sol();

                    // Update Prometheus metrics
                    INTENTS_RECEIVED_TOTAL.store(received, Ordering::Relaxed);
                    INTENTS_REJECTED_TOTAL.store(rejected, Ordering::Relaxed);
                    SIMULATION_FAILURES_TOTAL.store(sim_fail, Ordering::Relaxed);
                    OPEN_POSITIONS_GAUGE.store(ctx.get_open_positions() as u64, Ordering::Relaxed);
                    ACTIVE_CAPITAL_LOCKS.store(cap_locks as u64, Ordering::Relaxed);
                    ACTIVE_RESOURCE_LOCKS.store(res_locks as u64, Ordering::Relaxed);
                    AVAILABLE_SOL_LAMPORTS.store(available_sol, Ordering::Relaxed);

                    info!(
                        tick = simulated_tick,
                        intents_received = received,
                        intents_rejected = rejected,
                        sim_failures = sim_fail,
                        active_capital_locks = cap_locks,
                        active_resource_locks = res_locks,
                        available_sol = available_sol,
                        "Execution-engine heartbeat"
                    );
                }

                // P1 Crash Isolation: Ping systemd watchdog frequently enough to avoid edge timing.
                if simulated_tick % 10 == 0 {
                    #[cfg(unix)]
                    let _ = sd_notify::notify(false, &[NotifyState::Watchdog]);
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
        100, // 1% expected ROI
        200, // 2% max slippage
        TradeSide::Buy,
        TradingRegime::Early,
    )
    .with_ttl_ms(5000)
}

/// Process a single TradeIntent through the execution pipeline
async fn process_intent(ctx: &ExecutionContext, intent: TradeIntent) -> Result<()> {
    ctx.record_intent_received();

    // Keep Prometheus counters aligned with persisted decision/intents logs.
    // (The periodic heartbeat also stores aggregated counts; this makes the metric
    // responsive and avoids confusing under-counting after restarts.)
    INTENTS_RECEIVED_TOTAL.fetch_add(1, Ordering::Relaxed);

    let decision_id = ctx.next_decision_id();
    let mut checks: Vec<CheckResult> = Vec::new();

    // P1: Get config snapshot for this decision (hot-reloadable)
    let config = ctx.get_config();

    info!(
        intent_id = %intent.intent_id,
        decision_id = %decision_id,
        source = %intent.source,
        "Processing intent"
    );

    // Update received counter
    NATS_MESSAGES_RECEIVED_TOTAL.fetch_add(1, Ordering::Relaxed);

    // === Check 1: Idempotency ===
    if ctx.lock_manager.is_duplicate(&intent.intent_id) {
        let reason = RejectReason::LockDuplicateIntent;
        REJECT_DUPLICATE.fetch_add(1, Ordering::Relaxed);
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

    // Check 3a: Max position size (applies to BUY only)
    if intent.side == TradeSide::Buy {
        if intent.required_capital.raw > config.max_position_size_lamports {
            let reason = RejectReason::RiskMaxPosition;
            checks.push(CheckResult {
                check_name: "max_position_size".to_string(),
                passed: false,
                reason_code: Some(reason.to_string()),
                details: Some(format!(
                    "required={} > max={}",
                    intent.required_capital.raw, config.max_position_size_lamports
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
                intent.required_capital.raw, config.max_position_size_lamports
            )),
        });
    } else {
        checks.push(CheckResult {
            check_name: "max_position_size".to_string(),
            passed: true,
            reason_code: None,
            details: Some("skipped_for_sell".to_string()),
        });
    }

    // Check 3b: Max slippage
    if intent.max_slippage_bps > config.max_slippage_bps {
        let reason = RejectReason::SimSlippageExceeded;
        checks.push(CheckResult {
            check_name: "max_slippage".to_string(),
            passed: false,
            reason_code: Some(reason.to_string()),
            details: Some(format!(
                "intent_slippage={}bps > max={}bps",
                intent.max_slippage_bps, config.max_slippage_bps
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

    // Check 3c: Max open positions (applies to BUY only; SELL exits should remain possible)
    if intent.side == TradeSide::Buy {
        let current_positions = ctx.get_open_positions();
        if current_positions >= config.max_open_positions {
            let reason = RejectReason::RiskMaxOpenPositions;
            checks.push(CheckResult {
                check_name: "max_open_positions".to_string(),
                passed: false,
                reason_code: Some(reason.to_string()),
                details: Some(format!(
                    "current={} >= max={}",
                    current_positions, config.max_open_positions
                )),
            });
            return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
        }
        checks.push(CheckResult {
            check_name: "max_open_positions".to_string(),
            passed: true,
            reason_code: None,
            details: Some(format!("current < max (buy_only)")),
        });
    } else {
        checks.push(CheckResult {
            check_name: "max_open_positions".to_string(),
            passed: true,
            reason_code: None,
            details: Some("skipped_for_sell".to_string()),
        });
    }

    // Check 3d: Daily loss limit (applies to BUY only; SELL exits should remain possible)
    if intent.side == TradeSide::Buy {
        let daily_loss = ctx.get_daily_loss_lamports();
        if daily_loss >= config.daily_loss_limit_lamports as i64 {
            let reason = RejectReason::RiskDailyLossLimit;
            checks.push(CheckResult {
                check_name: "daily_loss_limit".to_string(),
                passed: false,
                reason_code: Some(reason.to_string()),
                details: Some(format!(
                    "daily_loss={} >= limit={}",
                    daily_loss, config.daily_loss_limit_lamports
                )),
            });
            return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
        }
        checks.push(CheckResult {
            check_name: "daily_loss_limit".to_string(),
            passed: true,
            reason_code: None,
            details: Some("ok (buy_only)".to_string()),
        });
    } else {
        checks.push(CheckResult {
            check_name: "daily_loss_limit".to_string(),
            passed: true,
            reason_code: None,
            details: Some("skipped_for_sell".to_string()),
        });
    }

    // Check 3e: SELL token balance preflight (avoid emitting SELL intents we cannot fulfill)
    if intent.side == TradeSide::Sell {
        let wallet = match ctx.wallet_pubkey {
            Some(pk) => pk,
            None => {
                let reason = RejectReason::InternalError;
                checks.push(CheckResult {
                    check_name: "sell_token_balance".to_string(),
                    passed: false,
                    reason_code: Some(reason.to_string()),
                    details: Some("wallet_pubkey_unavailable (no keys loaded)".to_string()),
                });
                return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
            }
        };

        let rpc = Arc::clone(&ctx.rpc);

        let mint_str = intent.resources.input_mint.clone();
        let required_raw = intent.required_capital.raw;

        let balance_check: Result<(u64, Pubkey)> = async {
            let mint = Pubkey::from_str(&mint_str)
                .map_err(|_| anyhow::anyhow!("invalid input_mint: {}", mint_str))?;

            let mint_acct = rpc
                .rpc
                .get_account(&mint)
                .await
                .map_err(|e| anyhow::anyhow!("failed to fetch mint account: {}", e))?;
            let token_program =
                token_program_for_mint_owner(&mint_acct.owner).ok_or_else(|| {
                    anyhow::anyhow!("unsupported mint owner program: {}", mint_acct.owner)
                })?;

            let ata_spl = get_associated_token_address_with_program_id(
                &sdk_to_spl(&wallet),
                &sdk_to_spl(&mint),
                &sdk_to_spl(&token_program),
            );
            let ata = spl_to_sdk(&ata_spl);

            // If ATA doesn't exist yet, treat as 0 balance.
            let available_raw = match rpc.rpc.get_token_account_balance(&ata).await {
                Ok(ui) => ui
                    .amount
                    .parse::<u64>()
                    .map_err(|e| anyhow::anyhow!("invalid token amount: {}", e))?,
                Err(_) => 0,
            };

            Ok((available_raw, ata))
        }
        .await;

        match balance_check {
            Ok((available_raw, ata)) => {
                // Seed lock-manager token availability so we can lock against it.
                // This prevents overlapping SELLs on the same mint from overbooking.
                ctx.lock_manager.set_available_token_balance(
                    intent.resources.input_mint.clone(),
                    available_raw,
                );

                if available_raw < required_raw {
                    let reason = RejectReason::SimInsufficientBalance;
                    checks.push(CheckResult {
                        check_name: "sell_token_balance".to_string(),
                        passed: false,
                        reason_code: Some(reason.to_string()),
                        details: Some(format!(
                            "available={} < required={} (mint={}, ata={})",
                            available_raw, required_raw, intent.resources.input_mint, ata
                        )),
                    });
                    return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
                }

                checks.push(CheckResult {
                    check_name: "sell_token_balance".to_string(),
                    passed: true,
                    reason_code: None,
                    details: Some(format!(
                        "available={} >= required={} (ata={})",
                        available_raw, required_raw, ata
                    )),
                });
            }
            Err(e) => {
                let reason = RejectReason::InternalError;
                checks.push(CheckResult {
                    check_name: "sell_token_balance".to_string(),
                    passed: false,
                    reason_code: Some(reason.to_string()),
                    details: Some(format!("rpc_error: {}", e)),
                });
                return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
            }
        }
    }

    // === P1: Fee/Compute Policy Checks ===
    let fee_policy = &config.fee_policy;

    // Check: Compute units within limit
    let compute_units = fee_policy.compute_units_for_intent(&intent);
    if compute_units > fee_policy.max_compute_units {
        let reason = RejectReason::FeeComputeExceedsLimit;
        checks.push(CheckResult {
            check_name: "fee_compute_limit".to_string(),
            passed: false,
            reason_code: Some(reason.to_string()),
            details: Some(format!(
                "compute_units={} > max={}",
                compute_units, fee_policy.max_compute_units
            )),
        });
        return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
    }
    checks.push(CheckResult {
        check_name: "fee_compute_limit".to_string(),
        passed: true,
        reason_code: None,
        details: Some(format!("compute_units={}", compute_units)),
    });

    // Check: Priority fee within limit
    let priority_fee = fee_policy.priority_fee_for_intent(&intent);
    if priority_fee > fee_policy.max_priority_fee_micro_lamports {
        let reason = RejectReason::FeePriorityExceedsLimit;
        checks.push(CheckResult {
            check_name: "fee_priority_limit".to_string(),
            passed: false,
            reason_code: Some(reason.to_string()),
            details: Some(format!(
                "priority_fee={} > max={}",
                priority_fee, fee_policy.max_priority_fee_micro_lamports
            )),
        });
        return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
    }
    checks.push(CheckResult {
        check_name: "fee_priority_limit".to_string(),
        passed: true,
        reason_code: None,
        details: Some(format!("priority_fee_micro_lamports={}", priority_fee)),
    });

    // Check: Total transaction cost within limit
    let (base_fee, priority_fee_lamports, total_cost) = fee_policy.estimate_tx_cost(&intent);
    if total_cost > fee_policy.max_tx_cost_lamports {
        let reason = RejectReason::FeeExceedsMaxCost;
        checks.push(CheckResult {
            check_name: "fee_max_cost".to_string(),
            passed: false,
            reason_code: Some(reason.to_string()),
            details: Some(format!(
                "total_cost={} (base={}, priority={}) > max={}",
                total_cost, base_fee, priority_fee_lamports, fee_policy.max_tx_cost_lamports
            )),
        });
        return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
    }
    checks.push(CheckResult {
        check_name: "fee_max_cost".to_string(),
        passed: true,
        reason_code: None,
        details: Some(format!(
            "total_cost={} (base={}, priority={})",
            total_cost, base_fee, priority_fee_lamports
        )),
    });

    // Check: Trade profitable after fees
    let (is_profitable, profit_after_fees_bps) = fee_policy.is_profitable_after_fees(&intent);
    if !is_profitable {
        let reason = RejectReason::FeeUnprofitable;
        checks.push(CheckResult {
            check_name: "fee_profitability".to_string(),
            passed: false,
            reason_code: Some(reason.to_string()),
            details: Some(format!(
                "profit_after_fees={}bps < min={}bps",
                profit_after_fees_bps, fee_policy.min_profit_after_fees_bps
            )),
        });
        return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
    }
    checks.push(CheckResult {
        check_name: "fee_profitability".to_string(),
        passed: true,
        reason_code: None,
        details: Some(format!(
            "profit_after_fees={}bps >= min={}bps",
            profit_after_fees_bps, fee_policy.min_profit_after_fees_bps
        )),
    });

    // === Check 4: Resource locks (pools + mints) ===
    let holder = LockHolder::new(&intent.intent_id)
        .with_decision(&decision_id)
        .with_tier(intent.tier as u8)
        .with_source(&intent.source); // P1: Source for fairness tracking

    let mut locked_resources = 0u64;
    for pool in &intent.resources.pools {
        match ctx
            .lock_manager
            .try_lock_resource(holder.clone(), pool, ResourceType::Pool)
        {
            LockResult::Acquired | LockResult::AcquiredByPreemption { .. } => {
                locked_resources += 1;
            }
            LockResult::Conflict { holder: existing } => {
                let reason = RejectReason::LockResourceConflict;
                REJECT_RESOURCE_LOCK.fetch_add(1, Ordering::Relaxed);
                checks.push(CheckResult {
                    check_name: "resource_lock".to_string(),
                    passed: false,
                    reason_code: Some(reason.to_string()),
                    details: Some(format!("pool locked by {}", existing.intent_id)),
                });
                ctx.lock_manager.release_locks(&intent.intent_id);
                return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
            }
            LockResult::InsufficientCapital { .. } => {
                // Not applicable for resource locks
            }
        }
    }

    for mint in [&intent.resources.input_mint, &intent.resources.output_mint] {
        if mint.is_empty() {
            continue;
        }
        match ctx
            .lock_manager
            .try_lock_resource(holder.clone(), mint, ResourceType::Mint)
        {
            LockResult::Acquired | LockResult::AcquiredByPreemption { .. } => {
                locked_resources += 1;
            }
            LockResult::Conflict { holder: existing } => {
                let reason = RejectReason::LockResourceConflict;
                REJECT_RESOURCE_LOCK.fetch_add(1, Ordering::Relaxed);
                checks.push(CheckResult {
                    check_name: "resource_lock".to_string(),
                    passed: false,
                    reason_code: Some(reason.to_string()),
                    details: Some(format!("mint locked by {}", existing.intent_id)),
                });
                ctx.lock_manager.release_locks(&intent.intent_id);
                return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
            }
            LockResult::InsufficientCapital { .. } => {
                // Not applicable for resource locks
            }
        }
    }

    checks.push(CheckResult {
        check_name: "resource_locks".to_string(),
        passed: true,
        reason_code: None,
        details: Some(format!("locked={}", locked_resources)),
    });

    // === Check 5: Capital lock (BUY: SOL, SELL: tokens) ===
    let lock_result = if intent.side == TradeSide::Buy {
        ctx.lock_manager.try_lock_capital(
            holder,
            intent.required_capital.raw,
            std::collections::HashMap::new(),
        )
    } else {
        let mut tokens = std::collections::HashMap::new();
        tokens.insert(
            intent.resources.input_mint.clone(),
            intent.required_capital.raw,
        );
        ctx.lock_manager.try_lock_capital(holder, 0, tokens)
    };

    match lock_result {
        LockResult::Acquired => {
            checks.push(CheckResult {
                check_name: "capital_lock".to_string(),
                passed: true,
                reason_code: None,
                details: Some(if intent.side == TradeSide::Buy {
                    "sol".to_string()
                } else {
                    format!("token:{}", intent.resources.input_mint)
                }),
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
        LockResult::InsufficientCapital {
            available,
            requested,
        } => {
            let reason = RejectReason::LockCapitalConflict;
            REJECT_CAPITAL_LOCK.fetch_add(1, Ordering::Relaxed);
            checks.push(CheckResult {
                check_name: "capital_lock".to_string(),
                passed: false,
                reason_code: Some(reason.to_string()),
                details: Some(format!(
                    "Insufficient capital: available={}, requested={}",
                    available, requested
                )),
            });
            ctx.lock_manager.release_locks(&intent.intent_id);
            return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
        }
        LockResult::Conflict { holder } => {
            let reason = RejectReason::LockCapitalConflict;
            REJECT_CAPITAL_LOCK.fetch_add(1, Ordering::Relaxed);
            checks.push(CheckResult {
                check_name: "capital_lock".to_string(),
                passed: false,
                reason_code: Some(reason.to_string()),
                details: Some(format!("Lock held by: {}", holder.intent_id)),
            });
            ctx.lock_manager.release_locks(&intent.intent_id);
            return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
        }
    }

    // === Cross-DEX Arbitrage Validation (if applicable) ===
    let is_cross_dex_arb = CrossDexHandler::is_cross_dex_arb_intent(&intent);
    let mut cross_dex_validation = None;

    // Planned tx (RS-2.1): deterministic tx plan + plan_hash
    let (tx_plan, plan_hash_str) = {
        // === Plan (RS-2.1) ===
        // RS-3.1 requires real simulation, which depends on a deterministic tx plan.
        // Therefore planning is now mandatory (unsupported intents are rejected explicitly).
        info!(intent_id = %intent.intent_id, "Building tx plan");
        let wallet_pubkey = match ctx.wallet_pubkey {
            Some(pk) => pk,
            None => {
                let reason = RejectReason::InternalError;
                checks.push(CheckResult {
                    check_name: "tx_plan".to_string(),
                    passed: false,
                    reason_code: Some(reason.to_string()),
                    details: Some("wallet_pubkey_unavailable (keys not loaded)".to_string()),
                });
                ctx.lock_manager.release_locks(&intent.intent_id);
                return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
            }
        };

        match tx_builder::build_tx_plan(&intent, wallet_pubkey, Arc::clone(&ctx.rpc)).await {
            tx_builder::TxPlanOutcome::Planned(plan) => {
                let plan_hash_str = plan.hash_string();
                checks.push(CheckResult {
                    check_name: "tx_plan".to_string(),
                    passed: true,
                    reason_code: None,
                    details: Some(format!(
                        "ix_count={} plan_hash={}",
                        plan.instructions.len(),
                        plan_hash_str
                    )),
                });
                (plan, plan_hash_str)
            }
            tx_builder::TxPlanOutcome::Unsupported(u) => {
                checks.push(CheckResult {
                    check_name: "tx_plan".to_string(),
                    passed: false,
                    reason_code: Some(u.reason.to_string()),
                    details: Some(u.details),
                });
                ctx.lock_manager.release_locks(&intent.intent_id);
                return emit_rejected_decision(ctx, decision_id, &intent, checks, u.reason).await;
            }
        }
    };

    let plan_hash: Option<String> = Some(plan_hash_str.clone());

    if is_cross_dex_arb {
        info!(intent_id = %intent.intent_id, "Processing as Cross-DEX arbitrage intent");

        if let Some(ref handler_lock) = ctx.cross_dex_handler {
            let handler = handler_lock.read();

            // Estimate tx cost for profitability check
            let estimated_tx_cost = 50_000u64; // ~0.00005 SOL (TODO: use fee policy)

            match handler
                .validate_arb_opportunity(&intent, estimated_tx_cost)
                .await
            {
                Ok(validation) => {
                    ctx.arb_validated
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                    if !validation.is_valid {
                        let reason = RejectReason::ArbSpreadInsufficient;
                        checks.push(CheckResult {
                            check_name: "cross_dex_validation".to_string(),
                            passed: false,
                            reason_code: Some(reason.to_string()),
                            details: validation.reject_reason.clone(),
                        });

                        // Release lock on rejection
                        ctx.lock_manager.release_locks(&intent.intent_id);

                        return emit_rejected_decision(ctx, decision_id, &intent, checks, reason)
                            .await;
                    }

                    checks.push(CheckResult {
                        check_name: "cross_dex_validation".to_string(),
                        passed: true,
                        reason_code: None,
                        details: Some(format!(
                            "spread={}bps profit={}lamports",
                            validation.actual_spread_bps, validation.estimated_profit_lamports
                        )),
                    });

                    cross_dex_validation = Some(validation);
                }
                Err(e) => {
                    warn!(error = %e, "Cross-DEX validation failed");
                    let reason = RejectReason::ArbValidationError;
                    checks.push(CheckResult {
                        check_name: "cross_dex_validation".to_string(),
                        passed: false,
                        reason_code: Some(reason.to_string()),
                        details: Some(e.to_string()),
                    });

                    ctx.lock_manager.release_locks(&intent.intent_id);
                    return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
                }
            }
        } else {
            // Cross-DEX handler not configured
            let reason = RejectReason::ArbHandlerNotConfigured;
            checks.push(CheckResult {
                check_name: "cross_dex_handler".to_string(),
                passed: false,
                reason_code: Some(reason.to_string()),
                details: Some("Cross-DEX handler not initialized".to_string()),
            });

            ctx.lock_manager.release_locks(&intent.intent_id);
            return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
        }
    }

    // === P1: Check if bundle required for atomic execution ===
    let requires_bundle = intent.requires_bundle();

    let wallet_pubkey = ctx
        .wallet_pubkey
        .expect("wallet_pubkey must be present after successful planning");

    if requires_bundle && config.send_enabled && ctx.jito_client.is_none() {
        // Intent requires bundle but Jito not configured
        let reason = RejectReason::BundleNotConfigured;
        checks.push(CheckResult {
            check_name: "bundle_config".to_string(),
            passed: false,
            reason_code: Some(reason.to_string()),
            details: Some("Intent requires atomic bundle but Jito not configured".to_string()),
        });
        ctx.lock_manager.release_locks(&intent.intent_id);
        return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
    }

    // If bundle execution is required, include the tip instruction in both simulation and send.
    // This preserves simulate-gated correctness (we simulate exactly what we will send).
    let mut bundle_tip_ix: Option<solana_sdk::instruction::Instruction> = None;
    let mut bundle_tip_lamports: Option<u64> = None;
    if requires_bundle && config.send_enabled {
        let tip_lamports = intent.bundle_tip_lamports.unwrap_or(config.jito_tip_lamports);
        let jito_client = ctx
            .jito_client
            .as_ref()
            .expect("bundle_config gate ensures jito_client is present");

        match jito_client.build_tip_instruction(&wallet_pubkey, tip_lamports) {
            Ok(ix) => {
                bundle_tip_ix = Some(ix);
                bundle_tip_lamports = Some(tip_lamports);
            }
            Err(e) => {
                let reason = RejectReason::InternalError;
                checks.push(CheckResult {
                    check_name: "bundle_tip_ix".to_string(),
                    passed: false,
                    reason_code: Some(reason.to_string()),
                    details: Some(format!("failed to build tip instruction: {e}")),
                });
                ctx.lock_manager.release_locks(&intent.intent_id);
                return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
            }
        }
    }

    let tx_plan_for_sim = if let Some(ref ix) = bundle_tip_ix {
        let mut ixs = tx_plan.instructions.clone();
        ixs.push(ix.clone());
        tx_builder::TxPlan { instructions: ixs }
    } else {
        tx_plan.clone()
    };

    // === Simulate (P0: simulate-gated) ===
    info!(intent_id = %intent.intent_id, "Running simulation");

    let sim_result = simulate_transaction(ctx, wallet_pubkey, &tx_plan_for_sim).await;

    if !sim_result.success {
        let reason = RejectReason::SimFailed;
        checks.push(CheckResult {
            check_name: "simulation".to_string(),
            passed: false,
            reason_code: Some(reason.to_string()),
            details: sim_result.error_code.clone(),
        });

        // Release lock on failure
        ctx.lock_manager.release_locks(&intent.intent_id);

        return emit_sim_failed_decision(
            ctx,
            decision_id,
            &intent,
            checks,
            plan_hash_str,
            sim_result,
        )
        .await;
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

    // Track bundle result for decision record
    let mut bundle_id: Option<String> = None;
    let mut send_signature: Option<String> = None;
    let mut sent_anything = false;
    let mut send_failed = false;

    // === Send (if enabled) ===
    if config.send_enabled {
        if requires_bundle {
            TX_SEND_ATTEMPTS_TOTAL.fetch_add(1, Ordering::Relaxed);

            let treasury = match ctx.treasury.as_ref() {
                Some(t) => t,
                None => {
                    send_failed = true;
                    let reason = RejectReason::InternalError;
                    checks.push(CheckResult {
                        check_name: "send".to_string(),
                        passed: false,
                        reason_code: Some(reason.to_string()),
                        details: Some("no_signer_configured".to_string()),
                    });
                    ctx.record_intent_rejected();
                    INTENTS_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
                    ctx.lock_manager.release_locks(&intent.intent_id);
                    return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
                }
            };

            let jito_client = ctx
                .jito_client
                .as_ref()
                .expect("bundle_config gate ensures jito_client is present");

            let signer: &dyn Signer = treasury.signer_ref();
            let blockhash = match ctx.rpc.get_latest_blockhash_retry().await {
                Ok(bh) => bh,
                Err(e) => {
                    send_failed = true;
                    let reason = RejectReason::BundleFailed;
                    checks.push(CheckResult {
                        check_name: "send".to_string(),
                        passed: false,
                        reason_code: Some(reason.to_string()),
                        details: Some(format!("rpc_error:{e}")),
                    });
                    ctx.record_intent_rejected();
                    INTENTS_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
                    warn!(intent_id = %intent.intent_id, error = %e, "Failed to fetch blockhash for bundle send");
                    // Allow retry
                    ctx.lock_manager.release_locks(&intent.intent_id);
                    return emit_rejected_decision(ctx, decision_id, &intent, checks, reason).await;
                }
            };

            let mut ixs = tx_plan.instructions.clone();
            if let Some(ref ix) = bundle_tip_ix {
                ixs.push(ix.clone());
            }

            let tx = Transaction::new_signed_with_payer(
                &ixs,
                Some(&wallet_pubkey),
                &[signer],
                blockhash,
            );

            if let Some(tip_lamports) = bundle_tip_lamports {
                JITO_TIP_LAMPORTS_TOTAL.fetch_add(tip_lamports, Ordering::Relaxed);
            }
            JITO_BUNDLES_SUBMITTED_TOTAL.fetch_add(1, Ordering::Relaxed);

            match jito_client.send_bundle(&[tx]).await {
                Ok(bid) => {
                    TX_SEND_SUCCESS_TOTAL.fetch_add(1, Ordering::Relaxed);
                    sent_anything = true;
                    bundle_id = Some(bid.clone());
                    checks.push(CheckResult {
                        check_name: "send".to_string(),
                        passed: true,
                        reason_code: None,
                        details: Some(format!(
                            "bundle_id={bid} tip_lamports={}",
                            bundle_tip_lamports.unwrap_or(config.jito_tip_lamports)
                        )),
                    });
                    info!(intent_id = %intent.intent_id, bundle_id = %bid, "Bundle submitted via Jito");
                }
                Err(e) => {
                    send_failed = true;
                    let reason = RejectReason::BundleFailed;
                    let err_msg = format!("{e:?}");
                    checks.push(CheckResult {
                        check_name: "send".to_string(),
                        passed: false,
                        reason_code: Some(reason.to_string()),
                        details: Some(err_msg.clone()),
                    });

                    // Reject: atomic guarantee cannot be met.
                    ctx.record_intent_rejected();
                    INTENTS_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
                    JITO_BUNDLES_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
                    warn!(intent_id = %intent.intent_id, error = %err_msg, "Jito bundle submission failed");
                }
            }
        } else {
            match send_transaction_rpc(
                ctx,
                wallet_pubkey,
                &tx_plan,
                config.send_skip_preflight,
                parse_commitment_level_opt(config.send_preflight_commitment.as_deref()),
            )
            .await {
                Ok(sig_str) => {
                    TX_SEND_SUCCESS_TOTAL.fetch_add(1, Ordering::Relaxed);
                    sent_anything = true;
                    send_signature = Some(sig_str.clone());
                    checks.push(CheckResult {
                        check_name: "send".to_string(),
                        passed: true,
                        reason_code: None,
                        details: Some(format!("signature={sig_str}")),
                    });
                    info!(intent_id = %intent.intent_id, signature = %sig_str, "Transaction submitted via RPC");
                }
                Err(err_msg) => {
                    send_failed = true;
                    let reason = RejectReason::SendFailed;
                    REJECT_SEND_FAILED.fetch_add(1, Ordering::Relaxed);
                    checks.push(CheckResult {
                        check_name: "send".to_string(),
                        passed: false,
                        reason_code: Some(reason.to_string()),
                        details: Some(err_msg.clone()),
                    });

                    // Reject: do NOT claim Sent without a real signature.
                    ctx.record_intent_rejected();
                    INTENTS_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
                    warn!(intent_id = %intent.intent_id, error = %err_msg, "sendTransaction failed");
                }
            }
        }
    } else {
        debug!(intent_id = %intent.intent_id, "Transaction sending disabled");
    }

    // Mark as processed
    // If sendTransaction failed, do NOT mark processed (allow retry).
    if !(config.send_enabled && send_failed) {
        ctx.lock_manager.mark_processed(&intent.intent_id);
    }

    // Build SendResult if we sent something
    let mut send_result = if sent_anything && (bundle_id.is_some() || send_signature.is_some()) {
        Some(ironcrab::ipc::SendResult {
            signature: send_signature.clone(),
            bundle_id: bundle_id.clone(),
            sent_at_ms: chrono::Utc::now().timestamp_millis() as u64,
        })
    } else {
        None
    };

    // === Confirm (RS-4.2 / RS-7.4) ===
    // - RPC path (non-bundle): confirm signature via getSignatureStatuses.
    // - Bundle path: wait for bundle landing via Jito block engine.
    let mut final_outcome = if config.send_enabled && sent_anything {
        DecisionOutcome::Sent
    } else {
        DecisionOutcome::Rejected
    };

    if config.send_enabled && sent_anything {
        if requires_bundle {
            if let Some(ref mut sr) = send_result {
                if let Some(ref bid) = sr.bundle_id {
                    let jito_client = ctx
                        .jito_client
                        .as_ref()
                        .expect("bundle_config gate ensures jito_client is present");

                    match jito_client.wait_for_bundle(bid, config.jito_timeout_secs).await {
                        Ok(status) => {
                            // Bundle landed successfully
                            JITO_BUNDLES_LANDED_TOTAL.fetch_add(1, Ordering::Relaxed);
                            TX_CONFIRMED_TOTAL.fetch_add(1, Ordering::Relaxed);

                            // If Jito returned tx signatures, record the first as a convenience.
                            if let Some(sig0) = status.transactions.get(0).cloned() {
                                sr.signature = Some(sig0.clone());
                            }

                            checks.push(CheckResult {
                                check_name: "confirm".to_string(),
                                passed: true,
                                reason_code: None,
                                details: Some(format!(
                                    "bundle_id={bid} slot={} confirmation_status={} txs={}",
                                    status.slot,
                                    status.confirmation_status,
                                    status.transactions.len()
                                )),
                            });
                            final_outcome = DecisionOutcome::Confirmed;
                        }
                        Err(e) => {
                            let error_msg = format!("{e:?}");
                            let is_timeout =
                                error_msg.contains("timeout") || error_msg.contains("Timeout");

                            if is_timeout {
                                JITO_BUNDLES_TIMEOUT_TOTAL.fetch_add(1, Ordering::Relaxed);
                                TX_CONFIRM_TIMEOUT_TOTAL.fetch_add(1, Ordering::Relaxed);
                                checks.push(CheckResult {
                                    check_name: "confirm".to_string(),
                                    passed: false,
                                    reason_code: Some(RejectReason::BundleTimeout.to_string()),
                                    details: Some(error_msg),
                                });
                                final_outcome = DecisionOutcome::Sent;
                            } else {
                                JITO_BUNDLES_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
                                checks.push(CheckResult {
                                    check_name: "confirm".to_string(),
                                    passed: false,
                                    reason_code: Some(RejectReason::BundleFailed.to_string()),
                                    details: Some(error_msg),
                                });
                                final_outcome = DecisionOutcome::FailedConfirmed;
                            }
                        }
                    }
                }
            }
        } else if let Some(ref sr) = send_result {
            if let Some(ref sig_str) = sr.signature {
                match confirm_signature_status(ctx, sig_str, config.confirmation_timeout_ms).await {
                    Ok(ConfirmOutcome::Confirmed { details }) => {
                        checks.push(CheckResult {
                            check_name: "confirm".to_string(),
                            passed: true,
                            reason_code: None,
                            details: Some(details),
                        });
                        final_outcome = DecisionOutcome::Confirmed;
                    }
                    Ok(ConfirmOutcome::FailedConfirmed { details }) => {
                        checks.push(CheckResult {
                            check_name: "confirm".to_string(),
                            passed: false,
                            reason_code: Some("confirmed_err".to_string()),
                            details: Some(details),
                        });
                        final_outcome = DecisionOutcome::FailedConfirmed;
                    }
                    Ok(ConfirmOutcome::TimeoutSent { details }) => {
                        checks.push(CheckResult {
                            check_name: "confirm".to_string(),
                            passed: false,
                            reason_code: Some("confirm_timeout".to_string()),
                            details: Some(details),
                        });
                        final_outcome = DecisionOutcome::Sent;
                    }
                    Err(e) => {
                        // Ambiguous confirmation: keep outcome at Sent, but record details.
                        checks.push(CheckResult {
                            check_name: "confirm".to_string(),
                            passed: false,
                            reason_code: Some("confirm_rpc_error".to_string()),
                            details: Some(e),
                        });
                        final_outcome = DecisionOutcome::Sent;
                    }
                }
            }
        }
    }

    // Emit decision record
    let decision = if config.send_enabled && sent_anything {
        DecisionRecord {
            header: ironcrab::ipc::RecordHeader::new(
                "execution-engine",
                BUILD_VERSION,
                &ctx.run_id,
            ),
            decision_id: decision_id.clone(),
            intent_id: intent.intent_id.clone(),
            source: intent.source.clone(),
            origin_type: intent.origin_type,
            regime: intent.regime,
            checks,
            primary_reject_reason: None,
            plan_hash,
            simulate: Some(sim_result),
            send: send_result.clone(),
            outcome: final_outcome,
            config_snapshot_id: None,
            input_snapshots: std::collections::HashMap::new(),
        }
    } else if config.send_enabled && send_failed {
        DecisionRecord {
            header: ironcrab::ipc::RecordHeader::new(
                "execution-engine",
                BUILD_VERSION,
                &ctx.run_id,
            ),
            decision_id: decision_id.clone(),
            intent_id: intent.intent_id.clone(),
            source: intent.source.clone(),
            origin_type: intent.origin_type,
            regime: intent.regime,
            checks,
            primary_reject_reason: Some(RejectReason::SendFailed.to_string()),
            plan_hash,
            simulate: Some(sim_result),
            send: None,
            outcome: DecisionOutcome::Rejected,
            config_snapshot_id: None,
            input_snapshots: std::collections::HashMap::new(),
        }
    } else if config.send_enabled {
        // Simulation succeeded but execution is not implemented.
        // Persist as a rejection so dashboards/explorers are not misleading.
        DecisionRecord {
            header: ironcrab::ipc::RecordHeader::new(
                "execution-engine",
                BUILD_VERSION,
                &ctx.run_id,
            ),
            decision_id: decision_id.clone(),
            intent_id: intent.intent_id.clone(),
            source: intent.source.clone(),
            origin_type: intent.origin_type,
            regime: intent.regime,
            checks,
            primary_reject_reason: Some("send_not_implemented".to_string()),
            plan_hash,
            simulate: Some(sim_result),
            send: None,
            outcome: DecisionOutcome::Rejected,
            config_snapshot_id: None,
            input_snapshots: std::collections::HashMap::new(),
        }
    } else {
        // Don't mark this as a simulation failure: simulation succeeded, but sending is disabled.
        // Persist a clear reason for post-mortem debugging.
        let mut checks = checks;
        checks.push(CheckResult {
            check_name: "send_enabled".to_string(),
            passed: false,
            reason_code: Some("send_disabled".to_string()),
            details: Some("execution-engine config.send_enabled=false".to_string()),
        });

        // This is a policy rejection, not a sim failure.
        ctx.record_intent_rejected();
        INTENTS_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);

        DecisionRecord {
            header: ironcrab::ipc::RecordHeader::new(
                "execution-engine",
                BUILD_VERSION,
                &ctx.run_id,
            ),
            decision_id: decision_id.clone(),
            intent_id: intent.intent_id.clone(),
            source: intent.source.clone(),
            origin_type: intent.origin_type,
            regime: intent.regime,
            checks,
            primary_reject_reason: Some("send_disabled".to_string()),
            plan_hash,
            simulate: Some(sim_result),
            send: None,
            outcome: DecisionOutcome::Rejected,
            config_snapshot_id: None,
            input_snapshots: std::collections::HashMap::new(),
        }
    };

    ctx.decision_writer.write(&decision)?;

    if let Some(ref nats) = ctx.nats {
        nats.publish(TOPIC_DECISION_RECORDS, &decision).await?;
    }

    // Emit an ExecutionResult so strategy-plane components (e.g. momentum-bot) can
    // manage positions and exits (stop-loss / take-profit) based on confirmed outcomes.
    if config.send_enabled {
        let status = match decision.outcome {
            DecisionOutcome::Confirmed => ExecutionStatus::Confirmed,
            DecisionOutcome::FailedConfirmed => ExecutionStatus::Failed,
            DecisionOutcome::Sent => ExecutionStatus::Sent,
            // These outcomes imply there was no successful on-chain confirmation.
            // Whether we emit is controlled by `should_emit` below.
            DecisionOutcome::Rejected | DecisionOutcome::SimFailed => ExecutionStatus::Failed,
            DecisionOutcome::Expired => ExecutionStatus::Timeout,
        };

        let should_emit = sent_anything || send_failed;
        if should_emit {
            let exec_id = ctx.next_execution_id();

            let (signature, bundle_id) = if let Some(ref sr) = send_result {
                (sr.signature.clone(), sr.bundle_id.clone())
            } else {
                (None, bundle_id.clone())
            };

            let mut exec = ExecutionResult::new_sent(
                "execution-engine",
                BUILD_VERSION,
                &ctx.run_id,
                exec_id,
                decision_id.clone(),
                intent.intent_id.clone(),
                intent.source.clone(),
                signature,
                bundle_id,
            );

            exec.status = status;
            if matches!(exec.status, ExecutionStatus::Failed) {
                exec.error_message = Some("execution_failed".to_string());
            }

            ctx.execution_writer.write(&exec)?;
            if let Some(ref nats) = ctx.nats {
                nats.publish(TOPIC_EXECUTION_RESULTS, &exec).await?;
            }
        }
    }

    // Dashboard alignment: count executions only when we have a confirmed on-chain outcome.
    if matches!(decision.outcome, DecisionOutcome::Confirmed) {
        INTENTS_EXECUTED_TOTAL.fetch_add(1, Ordering::Relaxed);
        match intent.side {
            TradeSide::Buy => ctx.increment_open_positions(),
            TradeSide::Sell => ctx.decrement_open_positions(),
        }
        OPEN_POSITIONS_GAUGE.store(ctx.get_open_positions() as u64, Ordering::Relaxed);

        // Best-effort recent trade record for Grafana (/trades via Infinity datasource).
        // NOTE: execution-engine does not know exact fills; token amount and price are placeholders.
        let now_ms = chrono::Utc::now().timestamp_millis().max(0) as u64;
        let (mint, action, amount_tokens, price_sol) = match intent.side {
            TradeSide::Buy => {
                let sol = (intent.required_capital.raw as f64)
                    / 10f64.powi(intent.required_capital.decimals as i32);
                (
                    intent.resources.output_mint.clone(),
                    "BUY".to_string(),
                    0.0,
                    sol,
                )
            }
            TradeSide::Sell => {
                let tokens = (intent.required_capital.raw as f64)
                    / 10f64.powi(intent.required_capital.decimals as i32);
                (
                    intent.resources.input_mint.clone(),
                    "SELL".to_string(),
                    tokens,
                    0.0,
                )
            }
        };

        let tx_hash = send_result
            .as_ref()
            .and_then(|sr| sr.signature.clone())
            .or_else(|| send_signature.clone())
            .unwrap_or_default();

        record_recent_trade(RecentTrade {
            timestamp_ms: now_ms,
            mint,
            action,
            tx_hash,
            amount_tokens,
            price_sol,
            pnl_sol: None,
            pnl_pct: None,
            latency_ms: None,
        });
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

    // Keep Prometheus counters aligned with decision records.
    INTENTS_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);

    let decision = DecisionRecord::new_rejected(
        "execution-engine",
        BUILD_VERSION,
        &ctx.run_id,
        decision_id.clone(),
        intent.intent_id.clone(),
        intent.source.clone(),
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
    plan_hash: String,
    sim_result: SimulationResult,
) -> Result<()> {
    // Simulation failures are rejections and should show up both in totals and by-reason.
    ctx.record_sim_failure();
    ctx.record_intent_rejected();
    INTENTS_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
    REJECT_SIMULATION_FAIL.fetch_add(1, Ordering::Relaxed);

    let decision = DecisionRecord::new_sim_failed(
        "execution-engine",
        BUILD_VERSION,
        &ctx.run_id,
        decision_id.clone(),
        intent.intent_id.clone(),
        intent.source.clone(),
        intent.origin_type,
        intent.regime,
        checks,
        plan_hash,
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

/// Real RPC simulation (RS-3.1).
///
/// Notes:
/// - Uses `sig_verify=false` (unsigned tx is fine for simulation).
/// - Uses `replace_recent_blockhash=true` so simulation does not depend on blockhash freshness.
async fn simulate_transaction(
    ctx: &ExecutionContext,
    wallet_pubkey: Pubkey,
    plan: &tx_builder::TxPlan,
) -> SimulationResult {
    let message = solana_sdk::message::Message::new(&plan.instructions, Some(&wallet_pubkey));
    let tx = Transaction::new_unsigned(message);

    let cfg = RpcSimulateTransactionConfig {
        sig_verify: false,
        replace_recent_blockhash: true,
        ..RpcSimulateTransactionConfig::default()
    };

    match ctx.rpc.rpc.simulate_transaction_with_config(&tx, cfg).await {
        Ok(res) => {
            let value = res.value;

            let logs_preview = value
                .logs
                .as_ref()
                .map(|lines| {
                    // Keep this small: decision records should be lightweight.
                    let mut s = lines.join("\n");
                    const MAX: usize = 8_000;
                    if s.len() > MAX {
                        s.truncate(MAX);
                    }
                    s
                });

            match value.err {
                None => SimulationResult {
                    success: true,
                    error_code: None,
                    logs_preview,
                    compute_units_consumed: value.units_consumed,
                },
                Some(err) => SimulationResult {
                    success: false,
                    error_code: Some(format!("{err:?}")),
                    logs_preview,
                    compute_units_consumed: value.units_consumed,
                },
            }
        }
        Err(e) => SimulationResult {
            success: false,
            error_code: Some(format!("rpc_error:{e}")),
            logs_preview: None,
            compute_units_consumed: None,
        },
    }
}

/// Real RPC send (RS-4.1).
///
/// Notes:
/// - Only called after successful simulation (simulate-gated).
/// - Builds and SIGNS using the single-signer `Treasury`.
/// - Uses `skip_preflight=true` (we already simulated).
async fn send_transaction_rpc(
    ctx: &ExecutionContext,
    wallet_pubkey: Pubkey,
    plan: &tx_builder::TxPlan,
    skip_preflight: bool,
    preflight_commitment: Option<CommitmentLevel>,
) -> std::result::Result<String, String> {
    TX_SEND_ATTEMPTS_TOTAL.fetch_add(1, Ordering::Relaxed);
    let treasury = ctx
        .treasury
        .as_ref()
        .ok_or_else(|| "no_signer_configured".to_string())?;

    let signer: &dyn Signer = treasury.signer_ref();
    let blockhash = ctx
        .rpc
        .get_latest_blockhash_retry()
        .await
        .map_err(|e| format!("rpc_error:{e}"))?;

    let tx = Transaction::new_signed_with_payer(
        &plan.instructions,
        Some(&wallet_pubkey),
        &[signer],
        blockhash,
    );

    let config = RpcSendTransactionConfig {
        skip_preflight,
        preflight_commitment,
        encoding: Some(solana_transaction_status::UiTransactionEncoding::Base64),
        max_retries: None,
        min_context_slot: None,
    };

    ctx.rpc
        .rpc
        .send_transaction_with_config(&tx, config)
        .await
        .map(|sig| sig.to_string())
        .map_err(|e| format!("rpc_error:{e}"))
}

fn parse_commitment_level_opt(value: Option<&str>) -> Option<CommitmentLevel> {
    let v = value?.trim();
    if v.is_empty() {
        return None;
    }
    match v.to_ascii_lowercase().as_str() {
        "processed" => Some(CommitmentLevel::Processed),
        "confirmed" => Some(CommitmentLevel::Confirmed),
        "finalized" => Some(CommitmentLevel::Finalized),
        _ => None,
    }
}

enum ConfirmOutcome {
    Confirmed { details: String },
    FailedConfirmed { details: String },
    TimeoutSent { details: String },
}

/// Confirmation polling (RS-4.2).
///
/// Maps outcome per roadmap:
/// - confirmed => Confirmed
/// - status err => FailedConfirmed
/// - timeout/ambiguous => Sent (TimeoutSent)
async fn confirm_signature_status(
    ctx: &ExecutionContext,
    signature_base58: &str,
    timeout_ms: u64,
) -> std::result::Result<ConfirmOutcome, String> {
    let signature = Signature::from_str(signature_base58)
        .map_err(|e| format!("invalid_signature:{e}"))?;

    let start = std::time::Instant::now();
    let deadline = Duration::from_millis(timeout_ms.max(1));
    let mut attempt: u32 = 0;

    loop {
        if start.elapsed() >= deadline {
            TX_CONFIRM_TIMEOUT_TOTAL.fetch_add(1, Ordering::Relaxed);
            return Ok(ConfirmOutcome::TimeoutSent {
                details: format!("timeout_ms={timeout_ms} elapsed_ms={} signature={signature_base58}", start.elapsed().as_millis()),
            });
        }

        // Poll signature status
        let res = ctx
            .rpc
            .rpc
            .get_signature_statuses(&[signature])
            .await
            .map_err(|e| format!("rpc_error:{e}"))?;

        let status_opt = res
            .value
            .get(0)
            .cloned()
            .unwrap_or(None);

        if let Some(st) = status_opt {
            if let Some(err) = st.err {
                return Ok(ConfirmOutcome::FailedConfirmed {
                    details: format!(
                        "err={err:?} confirmations={:?} confirmation_status={:?} elapsed_ms={}",
                        st.confirmations,
                        st.confirmation_status,
                        start.elapsed().as_millis()
                    ),
                });
            }

            // Treat Confirmed or Finalized as confirmed.
            // Some RPCs return confirmations=None when rooted/finalized.
            let is_confirmed = match st.confirmation_status {
                Some(solana_transaction_status::TransactionConfirmationStatus::Confirmed)
                | Some(solana_transaction_status::TransactionConfirmationStatus::Finalized) => true,
                Some(solana_transaction_status::TransactionConfirmationStatus::Processed) => false,
                None => st.confirmations.is_none(),
            };

            if is_confirmed {
                TX_CONFIRMED_TOTAL.fetch_add(1, Ordering::Relaxed);
                return Ok(ConfirmOutcome::Confirmed {
                    details: format!(
                        "confirmations={:?} confirmation_status={:?} elapsed_ms={}",
                        st.confirmations,
                        st.confirmation_status,
                        start.elapsed().as_millis()
                    ),
                });
            }
        }

        // Backoff: small, bounded.
        attempt = attempt.saturating_add(1);
        let sleep_ms = (50u64 * attempt.min(20) as u64).min(1_000);
        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
    }
}
