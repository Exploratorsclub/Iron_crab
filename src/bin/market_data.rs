//! market-data binary – Data Plane (Geyser ingest + MarketEvents)
//!
//! Source of Truth: docs/TARGET_ARCHITECTURE.md §2.1
//!
//! Responsibilities:
//! - Geyser ingest (preferred), optional RPC/WS fallback
//! - Pool/Account cache (in-memory)
//! - Normalize and publish MarketEvents to NATS
//! - Discovery Worker: detect new mints/pools as events
//!
//! This binary does NOT:
//! - Load wallet keys
//! - Sign or send transactions
//! - Make trading decisions

// Allow holding locks across await - RwLock reads are fast and this simplifies the code.
// TODO: Refactor to use explicit clone-before-await pattern if this causes contention.
#![allow(clippy::await_holding_lock)]

use anyhow::Result;
use clap::Parser;
use solana_sdk::pubkey::Pubkey;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use ironcrab::config::WalletTrackerCfg;
use ironcrab::ipc::{
    BinData, ConfigUpdate, ConfigUpdateResponse, ConfigUpdateStatus, MarketEvent, MarketEventKind,
    PoolCacheUpdate,
};
use ironcrab::metrics::{
    serve_metrics, MARKET_EVENTS_PUBLISHED_TOTAL, MARKET_EVENTS_RECEIVED_TOTAL, NATS_ERRORS_TOTAL,
    NATS_MESSAGES_PUBLISHED_TOTAL, POOLS_TRACKED_GAUGE,
};
use ironcrab::nats::{NatsClient, NatsConfig, TOPIC_MARKET_EVENTS, TOPIC_POOL_CACHE_UPDATES, pool_subject, ensure_pool_cache_stream};
use ironcrab::solana::dex::meteora_bin_array_layout::BinArray;
use ironcrab::solana::dex::meteora_dlmm::METEORA_DLMM_PROGRAM;
use ironcrab::solana::dex::meteora_swap_builder::MeteoraDlmmSwapBuilder;
use ironcrab::solana::dex_parser::{
    parse_account_update, parse_transaction_update, DexType, ParsedDexEvent,
};
use ironcrab::solana::geyser_pool_discovery::{DexType as PoolDexType, GeyserPoolDiscovery};
use ironcrab::solana::rpc::SolanaRpc;
use ironcrab::solana::wallet_tracker::WalletTracker;
use spl_token::solana_program::program_option::COption;
use spl_token::solana_program::program_pack::Pack;
use spl_token_2022::extension::StateWithExtensions;

/// NATS topic for config reload (P1: Runtime Configuration via UI)
const TOPIC_CONFIG_RELOAD: &str = "ironcrab.control.config.reload";
use ironcrab::solana::geyser_listener::GeyserListener;
use ironcrab::storage::{JsonlWriter, JsonlWriterConfig};

// LivePoolCache - MASTER Cache (Single Source of Truth)
use ironcrab::execution::live_pool_cache::{parse_pool_account, CachedPoolState, LivePoolCache};

// P1 Crash Isolation: Systemd Watchdog support
#[cfg(unix)]
use sd_notify::NotifyState;

/// Build version for decision records
const BUILD_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Known DEX program IDs
const RAYDIUM_AMM_V4: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const RAYDIUM_CPMM: &str = "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C";
const ORCA_WHIRLPOOL: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";
const PUMPFUN_PROGRAM: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
const PUMPFUN_AMM_PROGRAM: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
const METEORA_DLMM: &str = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo";

/// Market data configuration (hot-reloadable via NATS)
#[allow(dead_code)]
#[derive(Debug, Clone)]
struct MarketDataConfig {
    /// Enable Raydium AMM V4 discovery. Default: true
    enable_raydium: bool,
    /// Enable Raydium CPMM discovery. Default: true
    enable_raydium_cpmm: bool,
    /// Enable Orca discovery. Default: true
    enable_orca: bool,
    /// Enable PumpFun discovery. Default: true
    enable_pumpfun: bool,
    /// Enable Meteora DLMM discovery. Default: true
    enable_meteora_dlmm: bool,
    /// Max events per second rate limit. Default: 10000
    max_events_per_sec: u32,
}

impl Default for MarketDataConfig {
    fn default() -> Self {
        Self {
            enable_raydium: true,
            enable_raydium_cpmm: true,
            enable_orca: true,
            enable_pumpfun: true,
            enable_meteora_dlmm: true,
            max_events_per_sec: 10_000,
        }
    }
}

#[derive(Parser, Debug)]
#[command(name = "market-data")]
#[command(about = "IronCrab Data Plane – Geyser ingest and MarketEvents publisher")]
struct Args {
    /// Path to configuration file
    #[arg(short, long, default_value = "config.toml")]
    config: PathBuf,

    /// NATS server URL
    #[arg(long, env = "NATS_URL", default_value = "nats://localhost:4222")]
    nats_url: String,

    /// Geyser gRPC endpoint
    #[arg(long, env = "GEYSER_URL", default_value = "http://127.0.0.1:10000")]
    geyser_url: String,

    /// Prometheus metrics port
    #[arg(long, default_value = "9801")]
    metrics_port: u16,

    /// Log directory override
    #[arg(long, env = "IRONCRAB_LOG_DIR")]
    log_dir: Option<PathBuf>,

    /// Dry run: don't publish to NATS
    #[arg(long)]
    dry_run: bool,

    /// Simulate mode: emit fake slot events instead of real Geyser connection
    #[arg(long)]
    simulate: bool,
}

/// Runtime context for market-data
struct MarketDataContext {
    run_id: String,
    /// P1: Config in RwLock for runtime hot-reload
    config: parking_lot::RwLock<MarketDataConfig>,
    nats: Option<NatsClient>,
    jsonl_writer: JsonlWriter,
    event_counter: std::sync::atomic::AtomicU64,
    /// P1: Wallet tracker for smart money / early buyer detection
    wallet_tracker: WalletTracker,

    /// Tracked token mints for mint-authority/freeze-authority metadata.
    tracked_mints: parking_lot::RwLock<std::collections::HashSet<Pubkey>>,
    tracked_mints_tx: watch::Sender<Vec<Pubkey>>,

    /// Known pump_amm pools (already seen first trade).
    /// We emit PoolCreated + DexPoolAccounts on FIRST trade, then just DexPoolAccounts on subsequent trades.
    /// Key: pool_address
    known_pump_amm_pools: parking_lot::RwLock<std::collections::HashSet<Pubkey>>,

    /// Vault account tracking for PoolStateUpdate events (Geyser-based reserve balances).
    /// Maps vault_address → VaultInfo (pool context).
    tracked_vaults: parking_lot::RwLock<std::collections::HashMap<Pubkey, VaultInfo>>,
    /// Channel to notify GeyserListener when tracked vaults change (triggers resubscribe).
    tracked_vaults_tx: watch::Sender<Vec<Pubkey>>,

    /// Meteora DLMM Bin Array tracking for BinArrayUpdate events (Geyser-based liquidity).
    /// Maps bin_array_pda → BinArrayInfo (pool context).
    tracked_bin_arrays: parking_lot::RwLock<std::collections::HashMap<Pubkey, BinArrayInfo>>,
    /// Channel to notify GeyserListener when tracked bin arrays change (triggers resubscribe).
    tracked_bin_arrays_tx: watch::Sender<Vec<Pubkey>>,

    /// MASTER LivePoolCache - Single Source of Truth for all pool state.
    /// Updated via Geyser events and propagated to execution-engine via NATS.
    live_pool_cache: Arc<LivePoolCache>,
}

/// Information about a tracked vault account
#[derive(Debug)]
struct VaultInfo {
    pool_address: Pubkey,
    dex: String,
    base_mint: Pubkey,
    quote_mint: Pubkey,
    /// true = this vault holds base token, false = quote token
    is_base_vault: bool,
    /// Last known balance (for delta detection)
    last_balance: std::sync::atomic::AtomicU64,
}

impl Clone for VaultInfo {
    fn clone(&self) -> Self {
        Self {
            pool_address: self.pool_address,
            dex: self.dex.clone(),
            base_mint: self.base_mint,
            quote_mint: self.quote_mint,
            is_base_vault: self.is_base_vault,
            last_balance: std::sync::atomic::AtomicU64::new(
                self.last_balance.load(std::sync::atomic::Ordering::Relaxed),
            ),
        }
    }
}

/// Information about a tracked Meteora DLMM Bin Array account
#[derive(Debug, Clone)]
struct BinArrayInfo {
    pool_address: Pubkey,
    /// Index of this bin array (determines which bins it contains)
    bin_array_index: i64,
    /// Bin step from pool (needed for price calculation)
    bin_step: u16,
}

impl MarketDataContext {
    fn next_event_id(&self) -> String {
        let n = self
            .event_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        format!("evt-{}-{:06}", &self.run_id[..8], n)
    }

    /// P1: Apply config update from control-plane (Runtime Configuration via UI)
    fn apply_config_update(&self, update: &ConfigUpdate) -> ConfigUpdateResponse {
        let mut config = self.config.write();
        let mut applied = Vec::new();
        let mut rejected = Vec::new();

        for (key, value) in &update.config {
            match key.as_str() {
                "enable_raydium" => {
                    if let Some(v) = value.as_bool() {
                        config.enable_raydium = v;
                        applied.push(key.clone());
                        info!(key = %key, new_value = %v, "Config updated");
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected bool".to_string()));
                    }
                }
                "enable_orca" => {
                    if let Some(v) = value.as_bool() {
                        config.enable_orca = v;
                        applied.push(key.clone());
                        info!(key = %key, new_value = %v, "Config updated");
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected bool".to_string()));
                    }
                }
                "enable_pumpfun" => {
                    if let Some(v) = value.as_bool() {
                        config.enable_pumpfun = v;
                        applied.push(key.clone());
                        info!(key = %key, new_value = %v, "Config updated");
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected bool".to_string()));
                    }
                }
                "max_events_per_sec" => {
                    if let Some(v) = value.as_u64() {
                        if v > 0 && v <= 1_000_000 {
                            config.max_events_per_sec = v as u32;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be 1-1000000".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                _ => {
                    rejected.push((key.clone(), format!("Unknown config key: {}", key)));
                }
            }
        }

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
            new_snapshot_id: None,
        }
    }
}

fn try_parse_mint_account(
    owner: &Pubkey,
    data: &[u8],
) -> Option<(u8, u64, Option<String>, Option<String>)> {
    if owner.to_bytes() == spl_token::ID.to_bytes() {
        let mint = spl_token::state::Mint::unpack(data).ok()?;
        let mint_authority = match mint.mint_authority {
            COption::Some(p) => Some(p.to_string()),
            COption::None => None,
        };
        let freeze_authority = match mint.freeze_authority {
            COption::Some(p) => Some(p.to_string()),
            COption::None => None,
        };
        Some((mint.decimals, mint.supply, mint_authority, freeze_authority))
    } else if owner.to_bytes() == spl_token_2022::ID.to_bytes() {
        let mint = StateWithExtensions::<spl_token_2022::state::Mint>::unpack(data).ok()?;
        let base = mint.base;
        let mint_authority = match base.mint_authority {
            COption::Some(p) => Some(p.to_string()),
            COption::None => None,
        };
        let freeze_authority = match base.freeze_authority {
            COption::Some(p) => Some(p.to_string()),
            COption::None => None,
        };
        Some((base.decimals, base.supply, mint_authority, freeze_authority))
    } else {
        None
    }
}

/// Parse a Token Account to extract the balance (amount).
/// Works with both spl-token and spl-token-2022 accounts.
fn try_parse_token_account_balance(data: &[u8]) -> Option<u64> {
    // SPL Token Account layout: 165 bytes
    // Offset 64: amount (u64, little-endian)
    if data.len() >= 72 {
        // Standard spl-token Account layout
        let amount_bytes: [u8; 8] = data[64..72].try_into().ok()?;
        Some(u64::from_le_bytes(amount_bytes))
    } else {
        None
    }
}

/// Publish wallet token balance snapshot for position reconciliation.
///
/// Called once at market-data startup to provide momentum-bot with current wallet state.
/// This allows momentum-bot to reconcile positions after restarts, detecting:
/// - Manual sales via Phantom/Jupiter (no ExecutionResult)
/// - Emergency liquidations via UI
/// - External transfers
///
/// NO periodic refresh - wallet changes come via ExecutionResults in normal operation.
async fn publish_wallet_snapshot(
    ctx: &MarketDataContext,
    rpc: &SolanaRpc,
    wallet: &Pubkey,
) -> Result<()> {
    use solana_client::rpc_request::TokenAccountsFilter;

    let token_program = Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
        .expect("valid token program");
    let token_2022_program = Pubkey::from_str("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")
        .expect("valid token-2022 program");
    let sol_mint = Pubkey::from_str("So11111111111111111111111111111111111111112")
        .expect("valid sol mint");

    let mut total_accounts = 0usize;
    let mut non_zero_accounts = 0usize;

    // Scan both SPL Token and Token-2022 programs
    for (program_id, program_name) in [
        (token_program, "SPL Token"),
        (token_2022_program, "Token-2022"),
    ] {
        let accounts = match rpc
            .rpc
            .get_token_accounts_by_owner(
                wallet,
                TokenAccountsFilter::ProgramId(program_id),
            )
            .await
        {
            Ok(accounts) => accounts,
            Err(e) => {
                warn!(error = %e, program = program_name, "Failed to fetch token accounts");
                continue;
            }
        };

        total_accounts += accounts.len();

        for account in accounts {
            // Parse JSON to extract mint and balance
            let parsed = match account.account.data {
                solana_account_decoder::UiAccountData::Json(parsed) => parsed,
                _ => continue,
            };

            let serde_json::Value::Object(root) = parsed.parsed else {
                continue;
            };

            let Some(info) = root.get("info").and_then(|v| v.as_object()) else {
                continue;
            };

            // Extract mint
            let Some(mint_str) = info.get("mint").and_then(|v| v.as_str()) else {
                continue;
            };
            let Ok(mint) = Pubkey::from_str(mint_str) else {
                continue;
            };

            // Extract balance
            let Some(token_amount) = info.get("tokenAmount").and_then(|v| v.as_object()) else {
                continue;
            };
            let Some(amount_str) = token_amount.get("amount").and_then(|v| v.as_str()) else {
                continue;
            };
            let Ok(balance_raw) = amount_str.parse::<u64>() else {
                continue;
            };
            let decimals = token_amount
                .get("decimals")
                .and_then(|v| v.as_u64())
                .unwrap_or(6) as u8;

            // Skip zero balances and SOL/WSOL
            if balance_raw == 0 || mint == sol_mint {
                continue;
            }

            non_zero_accounts += 1;

            let event_id = format!("wallet_snapshot_{}", mint);
            let event = MarketEvent::new(
                "market-data",
                BUILD_VERSION,
                &ctx.run_id,
                event_id,
                "wallet_scan",
                None, // No slot for RPC-based snapshot
                MarketEventKind::WalletBalanceSnapshot {
                    mint: mint.to_string(),
                    balance_raw,
                    decimals,
                    token_program: program_id.to_string(),
                },
            );

            // Publish to NATS
            if let Some(ref nats) = ctx.nats {
                if let Err(e) = nats.publish(TOPIC_MARKET_EVENTS, &event).await {
                    warn!(error = %e, mint = %mint, "Failed to publish WalletBalanceSnapshot");
                }
            }

            // Log to JSONL
            if let Err(e) = ctx.jsonl_writer.write(&event) {
                warn!(error = %e, "Failed to write WalletBalanceSnapshot to JSONL");
            }

            debug!(
                mint = %mint,
                balance_raw = balance_raw,
                program = program_name,
                "Published WalletBalanceSnapshot"
            );
        }
    }

    info!(
        total_accounts = total_accounts,
        non_zero_accounts = non_zero_accounts,
        "✅ Wallet snapshot published for position reconciliation"
    );

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("market_data=info".parse()?)
                .add_directive("ironcrab=info".parse()?),
        )
        .init();

    let args = Args::parse();
    let run_id = Uuid::new_v4().to_string();

    info!(
        run_id = %run_id,
        config = %args.config.display(),
        geyser_url = %args.geyser_url,
        metrics_port = args.metrics_port,
        "Starting market-data service"
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

    // === P0 Check: Ensure no wallet keys are loaded ===
    // market-data is KEYLESS per architecture – exit immediately if keys are detected
    if std::env::var("IRONCRAB_KEYPAIR_JSON").is_ok()
        || std::env::var("IRONCRAB_KEYPAIR_B64").is_ok()
        || std::env::var("IRONCRAB_KEYPAIR_PATH").is_ok()
    {
        error!("ERROR: Wallet key environment variables detected!");
        error!("market-data is KEYLESS per architecture. Remove key variables and restart.");
        error!("Only execution-engine should have access to wallet keys.");
        std::process::exit(1);
    }

    // Setup JSONL writer
    let log_dir = args
        .log_dir
        .unwrap_or_else(|| PathBuf::from("trade_logs/market_events"));
    let jsonl_config = JsonlWriterConfig::new("market_events").with_log_dir(&log_dir);
    let jsonl_writer = JsonlWriter::new(jsonl_config)?;

    info!(log_dir = %log_dir.display(), "JSONL writer initialized");

    // Setup NATS (optional in dry-run mode)
    let nats = if args.dry_run {
        info!("Dry-run mode: NATS publishing disabled");
        None
    } else {
        let config = NatsConfig::new(&args.nats_url, "market-data");
        let mut client = NatsClient::new(config);
        if let Err(e) = client.connect().await {
            warn!(error = %e, "Failed to connect to NATS (continuing without)");
            None
        } else {
            info!(url = %args.nats_url, "Connected to NATS");
            
            // Initialize JetStream stream for PoolCacheUpdates (persistent state)
            if let Err(e) = ensure_pool_cache_stream(client.client()).await {
                error!(error = %e, "Failed to create/update JetStream POOL_CACHE stream");
                error!("PoolCacheUpdates will not persist across restarts!");
                error!("Check that nats-server is running with -js flag");
            } else {
                info!("JetStream POOL_CACHE stream ready for persistent state recovery");
            }
            
            Some(client)
        }
    };

    // Initialize WalletTracker (P1: Smart Money / Insider Detection)
    // TODO: Load config from file for production
    let wallet_tracker_cfg = WalletTrackerCfg::default();
    let wallet_tracker = WalletTracker::new(wallet_tracker_cfg);
    info!(
        smart_money = wallet_tracker.stats().smart_money_count,
        bad_actors = wallet_tracker.stats().bad_actor_count,
        "WalletTracker initialized"
    );

    let (tracked_mints_tx, tracked_mints_rx) = watch::channel(Vec::<Pubkey>::new());
    let (tracked_vaults_tx, tracked_vaults_rx) = watch::channel(Vec::<Pubkey>::new());
    let (tracked_bin_arrays_tx, tracked_bin_arrays_rx) = watch::channel(Vec::<Pubkey>::new());

    let ctx = Arc::new(MarketDataContext {
        run_id: run_id.clone(),
        config: parking_lot::RwLock::new(MarketDataConfig::default()),
        nats,
        jsonl_writer,
        event_counter: std::sync::atomic::AtomicU64::new(0),
        wallet_tracker,
        tracked_mints: parking_lot::RwLock::new(std::collections::HashSet::new()),
        tracked_mints_tx,
        known_pump_amm_pools: parking_lot::RwLock::new(std::collections::HashSet::new()),
        tracked_vaults: parking_lot::RwLock::new(std::collections::HashMap::new()),
        tracked_vaults_tx,
        tracked_bin_arrays: parking_lot::RwLock::new(std::collections::HashMap::new()),
        tracked_bin_arrays_tx,
        live_pool_cache: Arc::new(LivePoolCache::new()),
    });

    // === Main Loop: Geyser subscription or simulation ===

    // P1 Crash Isolation: Signal systemd that we're ready
    #[cfg(unix)]
    {
        // NOTE: Do NOT unset NOTIFY_SOCKET here; we need it for Watchdog pings.
        let _ = sd_notify::notify(false, &[NotifyState::Ready]);
        debug!("Sent sd_notify READY to systemd");
    }

    // Keep readiness fresh even when idle.
    ironcrab::metrics::record_activity();

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

    if args.simulate {
        info!("Simulation mode: emitting fake slot events");
        run_simulation_loop(ctx.clone(), &run_id, config_subscription).await?;
    } else {
        info!(geyser_url = %args.geyser_url, "Starting Geyser integration");
        run_geyser_loop(
            ctx.clone(),
            &run_id,
            &args.geyser_url,
            config_subscription,
            tracked_mints_rx,
            tracked_vaults_rx,
            tracked_bin_arrays_rx,
        )
        .await?;
    }

    // Flush JSONL on shutdown
    ctx.jsonl_writer.flush()?;
    info!(run_id = %run_id, "market-data shutdown complete");

    Ok(())
}

/// Run with real Geyser connection
async fn run_geyser_loop(
    ctx: Arc<MarketDataContext>,
    run_id: &str,
    geyser_url: &str,
    mut config_subscription: Option<ironcrab::nats::NatsSubscription>,
    tracked_mints_rx: watch::Receiver<Vec<Pubkey>>,
    tracked_vaults_rx: watch::Receiver<Vec<Pubkey>>,
    tracked_bin_arrays_rx: watch::Receiver<Vec<Pubkey>>,
) -> Result<()> {
    // Initialize RPC client for fallback/metadata (prefer local RPC, fallback to Helius)
    let rpc_url =
        std::env::var("SOLANA_RPC_URL").unwrap_or_else(|_| "http://127.0.0.1:8899".to_string()); // Local validator/private RPC preferred
    let rpc = Arc::new(SolanaRpc::new(&rpc_url));
    info!(rpc_url = %rpc_url, "Initialized RPC client for metadata/fallback");

    // === P0: Wallet Balance Snapshot (Position Reconciliation) ===
    // Publish current wallet state once at startup to reconcile positions after restarts.
    // Handles: manual sales, emergency liquidations, external transfers.
    // NO periodic refresh - wallet changes come via ExecutionResults.
    if let Ok(wallet_pubkey_str) = std::env::var("IRONCRAB_WALLET_PUBKEY") {
        if let Ok(wallet_pubkey) = Pubkey::from_str(&wallet_pubkey_str) {
            info!(wallet = %wallet_pubkey, "📸 Publishing wallet balance snapshot for position reconciliation");
            if let Err(e) = publish_wallet_snapshot(&ctx, &rpc, &wallet_pubkey).await {
                warn!(error = %e, "Failed to publish wallet snapshot (continuing anyway)");
            }
        } else {
            warn!("IRONCRAB_WALLET_PUBKEY is set but not a valid pubkey");
        }
    } else {
        info!("IRONCRAB_WALLET_PUBKEY not set, skipping wallet snapshot");
    }

    // Mint metadata fetch pipeline:
    // - We add mints to `tracked_mints` when we see them via tx/pool discovery.
    // - Mint accounts often *never change*, so relying on a future Geyser account update
    //   means we may never emit TokenMintInfo (decimals/supply), which strategies need.
    // - Therefore we proactively fetch the mint account once via RPC and emit TokenMintInfo.
    let (mint_info_tx, mut mint_info_rx) = mpsc::unbounded_channel::<MarketEvent>();

    // DEX program IDs to monitor (must match validator account-index)
    let program_ids = vec![
        Pubkey::from_str(RAYDIUM_AMM_V4).expect("valid raydium pubkey"),
        Pubkey::from_str(RAYDIUM_CPMM).expect("valid raydium cpmm pubkey"),
        Pubkey::from_str(ORCA_WHIRLPOOL).expect("valid orca pubkey"),
        Pubkey::from_str(PUMPFUN_PROGRAM).expect("valid pumpfun pubkey"),
        Pubkey::from_str(PUMPFUN_AMM_PROGRAM).expect("valid pumpfun amm pubkey"),
        Pubkey::from_str(METEORA_DLMM).expect("valid meteora dlmm pubkey"),
    ];

    // Initialize Geyser-based pool discovery (PRIMARY method for pool discovery)
    let (pool_discovery, mut pool_discovery_rx) =
        GeyserPoolDiscovery::new(geyser_url.to_string(), program_ids.clone(), rpc.clone());

    // Spawn pool discovery task
    let _pool_discovery_handle = tokio::spawn(async move {
        if let Err(e) = pool_discovery.start().await {
            error!(error = %e, "GeyserPoolDiscovery crashed");
        }
    });

    // Merge tracked_mints, tracked_vaults, and tracked_bin_arrays into a single combined channel.
    // GeyserListener will subscribe to all accounts in the combined list.
    let (combined_tracked_tx, combined_tracked_rx) = watch::channel(Vec::<Pubkey>::new());
    {
        let mut mints_rx = tracked_mints_rx;
        let mut vaults_rx = tracked_vaults_rx;
        let mut bin_arrays_rx = tracked_bin_arrays_rx;
        let combined_tx = combined_tracked_tx;
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = mints_rx.changed() => {}
                    _ = vaults_rx.changed() => {}
                    _ = bin_arrays_rx.changed() => {}
                }
                // Merge all lists
                let mints: Vec<Pubkey> = mints_rx.borrow().clone();
                let vaults: Vec<Pubkey> = vaults_rx.borrow().clone();
                let bin_arrays: Vec<Pubkey> = bin_arrays_rx.borrow().clone();
                let mut combined: Vec<Pubkey> = mints;
                combined.extend(vaults);
                combined.extend(bin_arrays);
                combined.sort();
                combined.dedup();
                let _ = combined_tx.send(combined);
            }
        });
    }

    // Start legacy GeyserListener for transaction parsing (will be phased out in favor of pool discovery)
    let (listener, mut account_rx, mut transaction_rx) = GeyserListener::new_with_tracked_accounts(
        geyser_url.to_string(),
        program_ids,
        combined_tracked_rx,
    );

    // Spawn Geyser listener task
    let listener_handle = tokio::spawn(async move {
        if let Err(e) = listener.start().await {
            error!(error = %e, "Geyser listener crashed");
        }
    });

    // Graceful shutdown handling
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    let mut account_count = 0u64;
    let mut tx_count = 0u64;
    let mut last_heartbeat = std::time::Instant::now();
    let mut activity_interval = tokio::time::interval(std::time::Duration::from_secs(10));

    loop {
        tokio::select! {
            // Proactive mint metadata (decimals/supply) fetched via RPC.
            Some(mint_event) = mint_info_rx.recv() => {
                // Write to JSONL
                if let Err(e) = ctx.jsonl_writer.write(&mint_event) {
                    error!(error = %e, "Failed to write TokenMintInfo event to JSONL");
                }

                // Publish to NATS
                if let Some(ref nats) = ctx.nats {
                    if let Err(e) = nats.publish(TOPIC_MARKET_EVENTS, &mint_event).await {
                        warn!(error = %e, "Failed to publish TokenMintInfo event to NATS");
                        NATS_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                    } else {
                        NATS_MESSAGES_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                        MARKET_EVENTS_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }

            // Keep /ready fresh even if Geyser/NATS are quiet.
            _ = activity_interval.tick() => {
                ironcrab::metrics::record_activity();

                // P1 Crash Isolation: Ping systemd watchdog frequently enough.
                #[cfg(unix)]
                let _ = sd_notify::notify(false, &[NotifyState::Watchdog]);
            }

            // Account updates (pool state changes)
            Ok(account_update) = account_rx.recv() => {
                account_count += 1;
                ironcrab::metrics::record_activity();

                // Tracked mint updates (Token mint authority/freeze info)
                if (account_update.owner.to_bytes() == spl_token::ID.to_bytes()
                    || account_update.owner.to_bytes() == spl_token_2022::ID.to_bytes())
                    && ctx.tracked_mints.read().contains(&account_update.pubkey)
                {
                    if let Some((decimals, supply, mint_authority, freeze_authority)) =
                        try_parse_mint_account(&account_update.owner, &account_update.data)
                    {
                        let is_token_2022 = account_update.owner.to_bytes() == spl_token_2022::ID.to_bytes();
                        
                        let mint_event = MarketEvent::new(
                            "market-data",
                            BUILD_VERSION,
                            run_id,
                            ctx.next_event_id(),
                            "geyser",
                            Some(account_update.slot),
                            MarketEventKind::TokenMintInfo {
                                mint: account_update.pubkey.to_string(),
                                token_program: account_update.owner.to_string(),
                                decimals,
                                supply,
                                mint_authority,
                                freeze_authority,
                            },
                        );

                        // Log Token-2022 mints explicitly (debugging)
                        if is_token_2022 {
                            info!(
                                mint = %account_update.pubkey,
                                token_program = %account_update.owner,
                                decimals,
                                supply,
                                "TokenMintInfo: Token-2022 mint detected via Geyser"
                            );
                        } else {
                            debug!(
                                mint = %account_update.pubkey,
                                token_program = %account_update.owner,
                                decimals,
                                "TokenMintInfo: SPL Token mint via Geyser"
                            );
                        }

                        if let Err(e) = ctx.jsonl_writer.write(&mint_event) {
                            error!(error = %e, "Failed to write TokenMintInfo event to JSONL");
                        }

                        if let Some(ref nats) = ctx.nats {
                            if let Err(e) = nats.publish(TOPIC_MARKET_EVENTS, &mint_event).await {
                                warn!(error = %e, "Failed to publish TokenMintInfo event to NATS");
                                NATS_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                            } else {
                                NATS_MESSAGES_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                                MARKET_EVENTS_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                }

                // Vault account updates → emit PoolStateUpdate (Geyser-based reserve balances)
                // This eliminates the need for RPC calls to fetch vault balances.
                if account_update.owner.to_bytes() == spl_token::ID.to_bytes()
                    || account_update.owner.to_bytes() == spl_token_2022::ID.to_bytes()
                {
                    // Clone data from lock scope to avoid holding lock across await
                    let vault_info_opt = ctx.tracked_vaults.read().get(&account_update.pubkey).cloned();
                    if let Some(vault_info) = vault_info_opt {
                        // Parse token account to get balance
                        if let Some(balance) = try_parse_token_account_balance(&account_update.data) {
                            // Check if balance changed (avoid spamming unchanged updates)
                            let prev_balance = vault_info.last_balance.swap(balance, std::sync::atomic::Ordering::Relaxed);
                            if balance != prev_balance {
                                // We need both vault balances to emit a complete PoolStateUpdate.
                                // For now, emit partial updates - consumers should merge base+quote.
                                // Future: Track both vaults and emit only when both are known.
                                let (reserve_base, reserve_quote) = if vault_info.is_base_vault {
                                    (balance, 0u64) // Partial: only base known
                                } else {
                                    (0u64, balance) // Partial: only quote known
                                };

                                // Try to get the other vault's balance for a complete update
                                let vaults = ctx.tracked_vaults.read();
                                let complete_update = vaults.values()
                                    .find(|v| v.pool_address == vault_info.pool_address && v.is_base_vault != vault_info.is_base_vault)
                                    .map(|other| {
                                        let other_balance = other.last_balance.load(std::sync::atomic::Ordering::Relaxed);
                                        if vault_info.is_base_vault {
                                            (balance, other_balance)
                                        } else {
                                            (other_balance, balance)
                                        }
                                    });
                                drop(vaults);

                                let (final_base, final_quote) = complete_update.unwrap_or((reserve_base, reserve_quote));

                                // Only emit if we have at least one non-zero balance
                                if final_base > 0 || final_quote > 0 {
                                    // MASTER CACHE: Update vault balance in LivePoolCache
                                    ctx.live_pool_cache.update_vault_balance(&account_update.pubkey, balance, account_update.slot);

                                    // Publish PoolCacheUpdate::BalanceUpdated to JetStream (persistent state)
                                    if let Some(ref nats) = ctx.nats {
                                        let balance_update = PoolCacheUpdate::new_balance_updated(
                                            "market-data",
                                            BUILD_VERSION,
                                            run_id,
                                            vault_info.pool_address.to_string(),
                                            vault_info.dex.clone(),
                                            vault_info.base_mint.to_string(),
                                            vault_info.quote_mint.to_string(),
                                            final_base,
                                            final_quote,
                                            account_update.slot,
                                        );
                                        let subject = pool_subject(&vault_info.pool_address.to_string());
                                        if let Err(e) = nats.jetstream_publish(&subject, &balance_update).await {
                                            warn!(error = %e, "Failed to publish PoolCacheUpdate::BalanceUpdated to JetStream");
                                            NATS_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                                        } else {
                                            NATS_MESSAGES_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                                            info!(pool = %vault_info.pool_address, slot = account_update.slot, "MASTER CACHE: Published PoolCacheUpdate::BalanceUpdated to JetStream");
                                        }
                                    }

                                    // Publish MarketEvent::PoolStateUpdate (existing logic for strategies)
                                    let state_event = MarketEvent::new(
                                        "market-data",
                                        BUILD_VERSION,
                                        run_id,
                                        ctx.next_event_id(),
                                        "geyser_vault",
                                        Some(account_update.slot),
                                        MarketEventKind::PoolStateUpdate {
                                            pool_address: vault_info.pool_address.to_string(),
                                            dex: vault_info.dex.clone(),
                                            reserve_base: final_base,
                                            reserve_quote: final_quote,
                                            base_mint: vault_info.base_mint.to_string(),
                                            quote_mint: vault_info.quote_mint.to_string(),
                                            update_slot: account_update.slot,
                                        },
                                    );

                                    if let Err(e) = ctx.jsonl_writer.write(&state_event) {
                                        error!(error = %e, "Failed to write PoolStateUpdate event to JSONL");
                                    }

                                    if let Some(ref nats) = ctx.nats {
                                        if let Err(e) = nats.publish(TOPIC_MARKET_EVENTS, &state_event).await {
                                            warn!(error = %e, "Failed to publish PoolStateUpdate event to NATS");
                                            NATS_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                                        } else {
                                            NATS_MESSAGES_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                                            MARKET_EVENTS_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                // Bin Array account updates → emit BinArrayUpdate (Geyser-based liquidity distribution)
                // This eliminates the need for RPC calls to fetch Meteora DLMM bin arrays.
                let dlmm_program = Pubkey::from_str(METEORA_DLMM_PROGRAM)
                    .expect("Invalid METEORA_DLMM_PROGRAM constant");
                if account_update.owner == dlmm_program {
                    if let Some(bin_array_info) = ctx.tracked_bin_arrays.read().get(&account_update.pubkey).cloned() {
                        // Parse bin array to extract liquidity distribution
                        match BinArray::parse(&account_update.data, bin_array_info.bin_step) {
                            Ok(parsed_array) => {
                                // Convert to compact BinData (only bins with liquidity)
                                let bins: Vec<BinData> = parsed_array.bins
                                    .iter()
                                    .enumerate()
                                    .filter(|(_, bin)| bin.amount_x > 0 || bin.amount_y > 0)
                                    .map(|(offset, bin)| BinData {
                                        offset: offset as u8,
                                        amount_x: bin.amount_x,
                                        amount_y: bin.amount_y,
                                    })
                                    .collect();

                                // Only emit if there's any liquidity
                                if !bins.is_empty() {
                                    let bin_event = MarketEvent::new(
                                        "market-data",
                                        BUILD_VERSION,
                                        run_id,
                                        ctx.next_event_id(),
                                        "geyser_bin_array",
                                        Some(account_update.slot),
                                        MarketEventKind::BinArrayUpdate {
                                            pool_address: bin_array_info.pool_address.to_string(),
                                            bin_array_index: bin_array_info.bin_array_index,
                                            bins,
                                            update_slot: account_update.slot,
                                        },
                                    );

                                    if let Err(e) = ctx.jsonl_writer.write(&bin_event) {
                                        error!(error = %e, "Failed to write BinArrayUpdate event to JSONL");
                                    }

                                    if let Some(ref nats) = ctx.nats {
                                        if let Err(e) = nats.publish(TOPIC_MARKET_EVENTS, &bin_event).await {
                                            warn!(error = %e, "Failed to publish BinArrayUpdate event to NATS");
                                            NATS_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                                        } else {
                                            NATS_MESSAGES_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                                            MARKET_EVENTS_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                debug!(
                                    error = %e,
                                    pubkey = %account_update.pubkey,
                                    "Failed to parse bin array account"
                                );
                            }
                        }
                    }
                }

                // MASTER CACHE: Try to parse as DEX pool state and upsert into LivePoolCache
                if let Some(cached_state) = parse_pool_account(&account_update.owner, &account_update.data) {
                    // Update MASTER LivePoolCache (Single Source of Truth)
                    ctx.live_pool_cache.upsert(account_update.pubkey, cached_state.clone(), account_update.slot);

                    // Extract mint and reserve info from cached_state for PoolCacheUpdate
                    let (base_mint, quote_mint, base_reserve, quote_reserve) = match &cached_state {
                        CachedPoolState::Orca(s) => {
                            (s.token_mint_a, s.token_mint_b, s.vault_a_balance.unwrap_or(0), s.vault_b_balance.unwrap_or(0))
                        }
                        CachedPoolState::RaydiumAmm(s) => {
                            (s.base_mint, s.quote_mint, s.coin_reserve.unwrap_or(0), s.pc_reserve.unwrap_or(0))
                        }
                        CachedPoolState::RaydiumCpmm(s) => {
                            (s.token_0_mint, s.token_1_mint, s.reserve_0.unwrap_or(0), s.reserve_1.unwrap_or(0))
                        }
                        CachedPoolState::Meteora(s) => {
                            (s.token_x_mint, s.token_y_mint, s.reserve_x_balance.unwrap_or(0), s.reserve_y_balance.unwrap_or(0))
                        }
                        CachedPoolState::PumpAmm(s) => {
                            (s.base_mint, s.quote_mint, s.base_reserve.unwrap_or(0), s.quote_reserve.unwrap_or(0))
                        }
                        CachedPoolState::PumpFun(s) => {
                            (s.token_mint, Pubkey::default(), s.virtual_token_reserves, s.virtual_sol_reserves)
                        }
                    };

                    // Publish PoolCacheUpdate to JetStream (Single Source of Truth for pool state)
                    if let Some(ref nats) = ctx.nats {
                        let pool_update = PoolCacheUpdate::new_pool_discovered(
                            "market-data",
                            BUILD_VERSION,
                            run_id,
                            account_update.pubkey.to_string(),
                            cached_state.dex_name().to_string(),
                            base_mint.to_string(),
                            quote_mint.to_string(),
                            base_reserve,
                            quote_reserve,
                            Some(0), // liquidity_lamports not available from account data
                            account_update.slot,
                        );
                        let subject = pool_subject(&account_update.pubkey.to_string());
                        if let Err(e) = nats.jetstream_publish(&subject, &pool_update).await {
                            warn!(error = %e, "Failed to publish PoolCacheUpdate to JetStream");
                            NATS_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                        } else {
                            NATS_MESSAGES_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }

                // Try to parse as DEX pool event (for MarketEvents - existing logic)
                let event_kind = if let Some(parsed) = parse_account_update(&account_update) {
                    debug!(
                        slot = account_update.slot,
                        "Parsed DEX account update"
                    );
                    parsed.to_market_event_kind()
                } else {
                    // Fallback to raw event for unknown accounts
                    MarketEventKind::AccountUpdate {
                        pubkey: account_update.pubkey.to_string(),
                        owner: account_update.owner.to_string(),
                        data_len: account_update.data.len(),
                    }
                };

                let event = MarketEvent::new(
                    "market-data",
                    BUILD_VERSION,
                    run_id,
                    ctx.next_event_id(),
                    "geyser",
                    Some(account_update.slot),
                    event_kind,
                );

                // Write to JSONL
                if let Err(e) = ctx.jsonl_writer.write(&event) {
                    error!(error = %e, "Failed to write account event to JSONL");
                }

                // Publish to NATS
                if let Some(ref nats) = ctx.nats {
                    if let Err(e) = nats.publish(TOPIC_MARKET_EVENTS, &event).await {
                        warn!(error = %e, "Failed to publish account event to NATS");
                        NATS_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                    } else {
                        NATS_MESSAGES_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                        MARKET_EVENTS_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }

            // Transaction updates (pool creations, swaps)
            Ok(tx_update) = transaction_rx.recv() => {
                tx_count += 1;
                ironcrab::metrics::record_activity();

                // Try to parse as DEX event (PoolCreated, Trade)
                let parsed_event = parse_transaction_update(&tx_update);

                // Track mint pubkeys for mint-authority/freeze metadata.
                // GEYSER-FIRST: No RPC calls! For pump.fun we know decimals=6. For others, Geyser delivers.
                if let Some(parsed) = parsed_event.as_ref() {
                    let mint_and_dex: Option<(Pubkey, Option<DexType>)> = match parsed {
                        ParsedDexEvent::PoolCreated { base_mint, dex, .. } => Some((*base_mint, Some(*dex))),
                        ParsedDexEvent::Trade { mint, dex, .. } => Some((*mint, Some(*dex))),
                        ParsedDexEvent::LiquidityRemoved { mint, .. } => Some((*mint, None)),
                    };
                    if let Some((mint, dex_opt)) = mint_and_dex {
                        let mut tracked = ctx.tracked_mints.write();
                        if tracked.insert(mint) {
                            // Push updated list to geyser listener (resubscribe)
                            let updated: Vec<Pubkey> = tracked.iter().copied().collect();
                            let _ = ctx.tracked_mints_tx.send(updated);

                            // CRITICAL FIX (2026-01-18): DO NOT EMIT TokenMintInfo BEFORE GEYSER DELIVERS MINT ACCOUNT!
                            //
                            // Previous bug: market-data emitted TokenMintInfo for pump.fun with hard-coded:
                            //   token_program: spl_token::ID (SPL Token)
                            //   decimals: 6
                            //
                            // This assumption was FALSE for Token-2022 mints created via Pump.fun!
                            // Example: DMYNp65mub3i7LRpBdB66CgBAceLcQnv4gsWeCi6pump (Token-2022 mint)
                            //
                            // Root cause: Pump.fun now supports BOTH SPL Token AND Token-2022.
                            // The only source of truth is the mint account.owner (= token program).
                            //
                            // SOLUTION: Wait for Geyser to deliver the mint account (82 bytes, owner = token program).
                            // tracked_mints_tx already sent above → GeyserListener will resubscribe.
                            // When Geyser delivers the mint account, this code at line 779 will emit TokenMintInfo:
                            //   if ctx.tracked_mints.read().contains(&account_update.pubkey) { ... }
                            //
                            // NO RPC CALL NEEDED - Geyser delivers mint accounts automatically.
                            // NO EARLY EMIT - avoids sending wrong token_program.
                            debug!(
                                mint = %mint,
                                dex = ?dex_opt,
                                "New mint tracked, waiting for Geyser mint account delivery (Token-2022 safe)"
                            );
                            // For other DEXes: Same strategy - Geyser will deliver the mint account when subscribed.
                            // No RPC call needed - tracked_mints is already sent to Geyser subscriber.
                        }
                    }
                }

                // Pump.fun: propagate creator/dev wallet so strategy can build deterministic intents.
                // The PoolCreated MarketEventKind intentionally does not carry creator today, so emit
                // a separate DevWalletIdentified event when available.
                if let Some(ParsedDexEvent::PoolCreated {
                    base_mint,
                    dex: DexType::PumpFun,
                    creator: Some(creator),
                    ..
                }) = parsed_event.as_ref()
                {
                    let dev_event = MarketEvent::new(
                        "market-data",
                        BUILD_VERSION,
                        run_id,
                        ctx.next_event_id(),
                        "geyser",
                        Some(tx_update.slot),
                        MarketEventKind::DevWalletIdentified {
                            mint: base_mint.to_string(),
                            dev_wallet: creator.to_string(),
                            // Supply percentage is not computed here yet (would require extra on-chain reads).
                            // Momentum-bot treats this as an input for dev-risk filters; keep deterministic.
                            supply_percentage: 0.0,
                        },
                    );

                    if let Err(e) = ctx.jsonl_writer.write(&dev_event) {
                        error!(error = %e, "Failed to write dev wallet event to JSONL");
                    }

                    if let Some(ref nats) = ctx.nats {
                        if let Err(e) = nats.publish(TOPIC_MARKET_EVENTS, &dev_event).await {
                            warn!(error = %e, "Failed to publish dev wallet event to NATS");
                            NATS_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                        } else {
                            NATS_MESSAGES_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                            MARKET_EVENTS_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }

                // P1: Process wallet tracking events
                let wallet_events = if let Some(ref parsed) = parsed_event {
                    match parsed {
                        ParsedDexEvent::PoolCreated { base_mint, .. } => {
                            // Record pool creation for early buyer tracking
                            ctx.wallet_tracker.record_pool_created(&base_mint.to_string(), tx_update.slot);
                            Vec::new()
                        }
                        ParsedDexEvent::Trade { mint, trader, is_buy, sol_amount, token_amount, signature, slot, .. } => {
                            // Check for smart money, early buyers, insider activity
                            ctx.wallet_tracker.process_trade(
                                &mint.to_string(),
                                &trader.to_string(),
                                *is_buy,
                                *sol_amount,
                                *token_amount,
                                *slot,
                                signature,
                                &ctx.run_id,
                                "market-data",
                            )
                        }
                        ParsedDexEvent::LiquidityRemoved { .. } => Vec::new(),
                    }
                } else {
                    Vec::new()
                };

                // Publish wallet tracking events
                for wallet_event in wallet_events {
                    // Write to JSONL
                    if let Err(e) = ctx.jsonl_writer.write(&wallet_event) {
                        error!(error = %e, "Failed to write wallet event to JSONL");
                    }
                    // Publish to NATS
                    if let Some(ref nats) = ctx.nats {
                        if let Err(e) = nats.publish(TOPIC_MARKET_EVENTS, &wallet_event).await {
                            warn!(error = %e, "Failed to publish wallet event to NATS");
                        }
                    }
                }

                // Pump.fun AMM: emit static pool account metadata for intent-driven execution.
                // IMPORTANT: We emit PoolCreated + DexPoolAccounts TOGETHER on first trade.
                // This ensures arb-strategy has all required accounts before seeing the pool.
                if let Some(ParsedDexEvent::Trade {
                    pool_address,
                    mint: base_mint_pk,
                    dex: DexType::PumpFunAmm,
                    pool_accounts: Some(pool_accounts),
                    ..
                }) = parsed_event.as_ref()
                {
                    // v1 order (see MarketEventKind::DexPoolAccounts docs): base_mint at [2], quote_mint at [3]
                    let base_mint = pool_accounts.get(2).map(|p| p.to_string()).unwrap_or_default();
                    let quote_mint = pool_accounts.get(3).map(|p| p.to_string()).unwrap_or_default();

                    // Check if this is the FIRST trade for this pool (new pool discovery)
                    let is_first_trade = ctx.known_pump_amm_pools.write().insert(*pool_address);

                    // If first trade, emit PoolCreated FIRST (before DexPoolAccounts)
                    // This ensures arb-strategy sees PoolCreated + DexPoolAccounts together
                    if is_first_trade {
                        info!(
                            pool = %pool_address,
                            base_mint = %base_mint_pk,
                            "pump_amm pool discovered via first trade - emitting PoolCreated + DexPoolAccounts"
                        );

                        let pool_created_event = MarketEvent::new(
                            "market-data",
                            BUILD_VERSION,
                            run_id,
                            ctx.next_event_id(),
                            "geyser_first_trade",
                            Some(tx_update.slot),
                            MarketEventKind::PoolCreated {
                                pool_address: pool_address.to_string(),
                                base_mint: base_mint.clone(),
                                quote_mint: quote_mint.clone(),
                                dex: "pump_amm".to_string(),
                                initial_liquidity_sol: None, // Not available from trade
                            },
                        );

                        if let Err(e) = ctx.jsonl_writer.write(&pool_created_event) {
                            error!(error = %e, "Failed to write pump_amm PoolCreated event to JSONL");
                        }

                        if let Some(ref nats) = ctx.nats {
                            if let Err(e) = nats.publish(TOPIC_MARKET_EVENTS, &pool_created_event).await {
                                warn!(error = %e, "Failed to publish pump_amm PoolCreated event to NATS");
                                NATS_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                            } else {
                                NATS_MESSAGES_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                                MARKET_EVENTS_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }

                    // Always emit DexPoolAccounts on pump_amm trades
                    let accounts_event = MarketEvent::new(
                        "market-data",
                        BUILD_VERSION,
                        run_id,
                        ctx.next_event_id(),
                        "geyser",
                        Some(tx_update.slot),
                        MarketEventKind::DexPoolAccounts {
                            dex: DexType::PumpFunAmm.to_string(),
                            pool_address: pool_address.to_string(),
                            base_mint,
                            quote_mint,
                            accounts: pool_accounts.iter().map(|p| p.to_string()).collect(),
                        },
                    );

                    if let Err(e) = ctx.jsonl_writer.write(&accounts_event) {
                        error!(error = %e, "Failed to write DexPoolAccounts event to JSONL");
                    }

                    if let Some(ref nats) = ctx.nats {
                        if let Err(e) = nats.publish(TOPIC_MARKET_EVENTS, &accounts_event).await {
                            warn!(error = %e, "Failed to publish DexPoolAccounts event to NATS");
                            NATS_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                        } else {
                            NATS_MESSAGES_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                            MARKET_EVENTS_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }

                let event_kind = if let Some(parsed) = parsed_event {
                    info!(
                        slot = tx_update.slot,
                        sig = %tx_update.signature,
                        "Parsed DEX transaction"
                    );
                    parsed.to_market_event_kind()
                } else {
                    // Fallback to raw event for unknown transactions
                    MarketEventKind::TransactionDetected {
                        signature: tx_update.signature.clone(),
                        program: tx_update.account_keys.first().map(|k| k.to_string()).unwrap_or_default(),
                    }
                };

                let event = MarketEvent::new(
                    "market-data",
                    BUILD_VERSION,
                    run_id,
                    ctx.next_event_id(),
                    "geyser",
                    Some(tx_update.slot),
                    event_kind,
                );

                // Write to JSONL
                if let Err(e) = ctx.jsonl_writer.write(&event) {
                    error!(error = %e, "Failed to write tx event to JSONL");
                }

                // Publish to NATS
                if let Some(ref nats) = ctx.nats {
                    if let Err(e) = nats.publish(TOPIC_MARKET_EVENTS, &event).await {
                        warn!(error = %e, "Failed to publish tx event to NATS");
                        NATS_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                    } else {
                        NATS_MESSAGES_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                        MARKET_EVENTS_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }

            // Pool Discovery Events (Geyser-based pool creation events)
            Ok(pool_event) = pool_discovery_rx.recv() => {
                ironcrab::metrics::record_activity();

                info!(
                    dex = %pool_event.dex_type,
                    pool = %pool_event.pool_address,
                    base = %pool_event.base_mint,
                    quote = %pool_event.quote_mint,
                    liquidity_lamports = pool_event.liquidity_estimate_lamports,
                    "Pool discovered via Geyser"
                );

                // Track base mint for metadata fetching - GEYSER-FIRST: No RPC calls!
                let mut tracked = ctx.tracked_mints.write();
                if tracked.insert(pool_event.base_mint) {
                    let updated: Vec<Pubkey> = tracked.iter().copied().collect();
                    let _ = ctx.tracked_mints_tx.send(updated);

                    // For Pump.fun/PumpAMM: emit TokenMintInfo immediately with known decimals=6.
                    // For other DEXes: Geyser will deliver the mint account when subscribed.
                    // Note: PoolDexType only has PumpFun (legacy bonding), PumpAMM is detected via transaction parsing
                    let is_pump = matches!(pool_event.dex_type, PoolDexType::PumpFun);
                    if is_pump {
                        let mint_event = MarketEvent::new(
                            "market-data",
                            BUILD_VERSION,
                            run_id,
                            ctx.next_event_id(),
                            "geyser_known", // Not RPC - we know pump.fun uses decimals=6
                            Some(pool_event.slot),
                            MarketEventKind::TokenMintInfo {
                                mint: pool_event.base_mint.to_string(),
                                token_program: spl_token::ID.to_string(), // pump.fun always uses SPL Token
                                decimals: 6, // pump.fun tokens ALWAYS have 6 decimals
                                supply: 0,   // Unknown, but not critical for trading
                                mint_authority: None,
                                freeze_authority: None,
                            },
                        );
                        let _ = mint_info_tx.send(mint_event);
                        debug!(mint = %pool_event.base_mint, "Emitted TokenMintInfo for pump.fun token (decimals=6, no RPC)");
                    }
                    // For other DEXes: Geyser subscription handles it - no RPC call needed.
                }

                // Convert to MarketEvent::PoolCreated
                let event = MarketEvent::new(
                    "market-data",
                    BUILD_VERSION,
                    run_id,
                    ctx.next_event_id(),
                    "geyser_pool_discovery",
                    Some(pool_event.slot),
                    MarketEventKind::PoolCreated {
                        pool_address: pool_event.pool_address.to_string(),
                        base_mint: pool_event.base_mint.to_string(),
                        quote_mint: pool_event.quote_mint.to_string(),
                        dex: pool_event.dex_type.to_string(),
                        initial_liquidity_sol: Some(
                            rust_decimal::Decimal::from(pool_event.liquidity_estimate_lamports)
                                / rust_decimal::Decimal::from(1_000_000_000u64)
                        ),
                    },
                );

                // Write to JSONL
                if let Err(e) = ctx.jsonl_writer.write(&event) {
                    error!(error = %e, "Failed to write pool discovery event to JSONL");
                }

                // Publish to NATS
                if let Some(ref nats) = ctx.nats {
                    if let Err(e) = nats.publish(TOPIC_MARKET_EVENTS, &event).await {
                        warn!(error = %e, "Failed to publish pool discovery event to NATS");
                        NATS_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                    } else {
                        NATS_MESSAGES_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                        MARKET_EVENTS_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                    }
                }

                // Emit DevWalletIdentified for Pump.fun pools where we know the creator
                // This enables momentum-bot to populate metadata.creator for intent building
                if pool_event.dex_type == PoolDexType::PumpFun {
                    if let Some(creator) = pool_event.creator {
                        let dev_event = MarketEvent::new(
                            "market-data",
                            BUILD_VERSION,
                            run_id,
                            ctx.next_event_id(),
                            "geyser_pool_discovery",
                            Some(pool_event.slot),
                            MarketEventKind::DevWalletIdentified {
                                mint: pool_event.base_mint.to_string(),
                                dev_wallet: creator.to_string(),
                                // Supply percentage not computed (would need extra on-chain reads)
                                supply_percentage: 0.0,
                            },
                        );

                        if let Err(e) = ctx.jsonl_writer.write(&dev_event) {
                            error!(error = %e, "Failed to write dev wallet event to JSONL");
                        }

                        if let Some(ref nats) = ctx.nats {
                            if let Err(e) = nats.publish(TOPIC_MARKET_EVENTS, &dev_event).await {
                                warn!(error = %e, "Failed to publish dev wallet event to NATS");
                                NATS_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                            } else {
                                NATS_MESSAGES_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                                MARKET_EVENTS_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                                info!(
                                    mint = %pool_event.base_mint,
                                    creator = %creator,
                                    "✅ DevWalletIdentified emitted for pump.fun pool"
                                );
                            }
                        }
                    }
                }

                // Emit DexPoolAccounts for all DEXes that have vault information
                // This enables arb-strategy to have pool accounts BEFORE first trade
                if pool_event.coin_vault.is_some() || pool_event.pc_vault.is_some() {
                    let mut accounts = vec![
                        pool_event.pool_address.to_string(),
                        pool_event.base_mint.to_string(),
                        pool_event.quote_mint.to_string(),
                    ];

                    if let Some(coin_vault) = pool_event.coin_vault {
                        accounts.push(coin_vault.to_string());
                    }
                    if let Some(pc_vault) = pool_event.pc_vault {
                        accounts.push(pc_vault.to_string());
                    }
                    if let Some(creator) = pool_event.creator {
                        accounts.push(creator.to_string());
                    }
                    // Meteora DLMM: add active_id and bin_step as tagged values
                    // Format: "active_id:<value>" and "bin_step:<value>"
                    if let Some(active_id) = pool_event.active_id {
                        accounts.push(format!("active_id:{}", active_id));
                    }
                    if let Some(bin_step) = pool_event.bin_step {
                        accounts.push(format!("bin_step:{}", bin_step));
                    }
                    // Orca Whirlpool: add tick_current_index and tick_spacing as tagged values
                    // Format: "tick_current_index:<value>" and "tick_spacing:<value>"
                    if let Some(tick) = pool_event.tick_current_index {
                        accounts.push(format!("tick_current_index:{}", tick));
                    }
                    if let Some(spacing) = pool_event.tick_spacing {
                        accounts.push(format!("tick_spacing:{}", spacing));
                    }

                    let accounts_event = MarketEvent::new(
                        "market-data",
                        BUILD_VERSION,
                        run_id,
                        ctx.next_event_id(),
                        "geyser_pool_discovery",
                        Some(pool_event.slot),
                        MarketEventKind::DexPoolAccounts {
                            dex: pool_event.dex_type.to_string(),
                            pool_address: pool_event.pool_address.to_string(),
                            base_mint: pool_event.base_mint.to_string(),
                            quote_mint: pool_event.quote_mint.to_string(),
                            accounts,
                        },
                    );

                    if let Err(e) = ctx.jsonl_writer.write(&accounts_event) {
                        error!(error = %e, "Failed to write DexPoolAccounts event to JSONL");
                    }

                    if let Some(ref nats) = ctx.nats {
                        if let Err(e) = nats.publish(TOPIC_MARKET_EVENTS, &accounts_event).await {
                            warn!(error = %e, "Failed to publish DexPoolAccounts event to NATS");
                            NATS_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                        } else {
                            NATS_MESSAGES_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                            MARKET_EVENTS_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                            debug!(
                                dex = %pool_event.dex_type,
                                pool = %pool_event.pool_address,
                                "Emitted DexPoolAccounts for pool discovery"
                            );
                        }
                    }

                    // Track vault accounts for PoolStateUpdate events (Geyser-based reserve balances)
                    // This enables real-time reserve tracking without RPC calls.
                    let dex_str = pool_event.dex_type.to_string();
                    let mut vaults_changed = false;
                    {
                        let mut vaults = ctx.tracked_vaults.write();
                        if let Some(coin_vault) = pool_event.coin_vault {
                            vaults.entry(coin_vault).or_insert_with(|| {
                                vaults_changed = true;
                                VaultInfo {
                                    pool_address: pool_event.pool_address,
                                    dex: dex_str.clone(),
                                    base_mint: pool_event.base_mint,
                                    quote_mint: pool_event.quote_mint,
                                    is_base_vault: true,
                                    last_balance: std::sync::atomic::AtomicU64::new(0),
                                }
                            });
                        }
                        if let Some(pc_vault) = pool_event.pc_vault {
                            vaults.entry(pc_vault).or_insert_with(|| {
                                vaults_changed = true;
                                VaultInfo {
                                    pool_address: pool_event.pool_address,
                                    dex: dex_str.clone(),
                                    base_mint: pool_event.base_mint,
                                    quote_mint: pool_event.quote_mint,
                                    is_base_vault: false,
                                    last_balance: std::sync::atomic::AtomicU64::new(0),
                                }
                            });
                        }
                    }
                    // Notify GeyserListener to resubscribe with new vault accounts
                    if vaults_changed {
                        let vault_list: Vec<Pubkey> = ctx.tracked_vaults.read().keys().copied().collect();
                        let _ = ctx.tracked_vaults_tx.send(vault_list);
                        debug!(
                            pool = %pool_event.pool_address,
                            coin_vault = ?pool_event.coin_vault,
                            pc_vault = ?pool_event.pc_vault,
                            "Registered vault accounts for PoolStateUpdate tracking"
                        );
                    }

                    // Track Meteora DLMM Bin Array accounts for BinArrayUpdate events
                    // This enables real-time liquidity tracking without RPC calls.
                    if pool_event.dex_type == PoolDexType::MeteoraDlmm {
                        // For Meteora DLMM, we need to subscribe to bin array accounts.
                        // We derive PDAs for ±3 arrays around the active bin.
                        // Use actual active_id/bin_step from pool_event (parsed in geyser_pool_discovery)
                        let active_id = pool_event.active_id.unwrap_or(0);
                        let active_array_index = MeteoraDlmmSwapBuilder::bin_id_to_bin_array_index(active_id);
                        let bin_step = pool_event.bin_step.unwrap_or(1);

                        let mut bin_arrays_changed = false;
                        {
                            let mut bin_arrays = ctx.tracked_bin_arrays.write();
                            // Register ±3 bin arrays around active bin
                            for offset in -3i64..=3i64 {
                                let index = active_array_index + offset;
                                if let Ok(pda) = MeteoraDlmmSwapBuilder::derive_bin_array_pda(
                                    &pool_event.pool_address,
                                    index,
                                ) {
                                    bin_arrays.entry(pda).or_insert_with(|| {
                                        bin_arrays_changed = true;
                                        BinArrayInfo {
                                            pool_address: pool_event.pool_address,
                                            bin_array_index: index,
                                            bin_step,
                                        }
                                    });
                                }
                            }
                        }
                        // Notify GeyserListener to resubscribe with new bin array accounts
                        if bin_arrays_changed {
                            let bin_array_list: Vec<Pubkey> = ctx.tracked_bin_arrays.read().keys().copied().collect();
                            let num_arrays = bin_array_list.len();
                            let _ = ctx.tracked_bin_arrays_tx.send(bin_array_list);
                            debug!(
                                pool = %pool_event.pool_address,
                                arrays_tracked = num_arrays,
                                "Registered Meteora DLMM bin array accounts for BinArrayUpdate tracking"
                            );
                        }
                    }
                }
            }

            // P1: Handle Config Updates (Runtime Configuration via UI)
            msg = async {
                if let Some(ref mut sub) = config_subscription {
                    sub.next().await
                } else {
                    std::future::pending::<Option<ironcrab::nats::NatsMessage>>().await
                }
            } => {
                if let Some(nats_msg) = msg {
                    match serde_json::from_slice::<ConfigUpdate>(&nats_msg.payload) {
                        Ok(update) => {
                            if update.target_component == "market-data" {
                                info!(
                                    component = %update.target_component,
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
                                debug!(component = %update.target_component, "Ignoring config update for other component");
                            }
                        }
                        Err(e) => {
                            warn!(error = %e, "Failed to deserialize ConfigUpdate");
                        }
                    }
                }
            }

            // Periodic heartbeat
            _ = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                if last_heartbeat.elapsed().as_secs() >= 60 {
                    ironcrab::metrics::record_activity();
                    let (records, bytes) = ctx.jsonl_writer.stats();
                    let total_events = account_count + tx_count;

                    // Update Prometheus metrics
                    MARKET_EVENTS_RECEIVED_TOTAL.store(total_events, Ordering::Relaxed);
                    POOLS_TRACKED_GAUGE.store(account_count, Ordering::Relaxed);

                    info!(
                        accounts = account_count,
                        transactions = tx_count,
                        records_written = records,
                        bytes_written = bytes,
                        "market-data heartbeat (Geyser)"
                    );
                    last_heartbeat = std::time::Instant::now();
                }
            }

            _ = &mut shutdown => {
                info!("Shutdown signal received");
                listener_handle.abort();
                break;
            }
        }
    }

    Ok(())
}

/// Run simulation loop (for testing without Geyser)
async fn run_simulation_loop(
    ctx: Arc<MarketDataContext>,
    run_id: &str,
    mut config_subscription: Option<ironcrab::nats::NatsSubscription>,
) -> Result<()> {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
    let mut slot: u64 = 0;

    // Graceful shutdown handling
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = interval.tick() => {
                slot += 1; // Simulated slot progression

                // Keep /ready fresh even when only simulating.
                ironcrab::metrics::record_activity();

                let event = MarketEvent::new(
                    "market-data",
                    BUILD_VERSION,
                    run_id,
                    ctx.next_event_id(),
                    "simulated",
                    Some(slot),
                    MarketEventKind::SlotUpdate { current_slot: slot },
                );

                // Write to JSONL (P0 requirement)
                if let Err(e) = ctx.jsonl_writer.write(&event) {
                    error!(error = %e, "Failed to write event to JSONL");
                }

                // Publish to NATS
                if let Some(ref nats) = ctx.nats {
                    if let Err(e) = nats.publish(TOPIC_MARKET_EVENTS, &event).await {
                        warn!(error = %e, "Failed to publish to NATS");
                    }
                }

                // Periodic stats
                if slot % 60 == 0 {
                    let (records, bytes) = ctx.jsonl_writer.stats();
                    info!(
                        slot,
                        records_written = records,
                        bytes_written = bytes,
                        "market-data heartbeat (simulation)"
                    );
                }

                // P1 Crash Isolation: Ping systemd watchdog frequently enough.
                if slot % 10 == 0 {
                    #[cfg(unix)]
                    let _ = sd_notify::notify(false, &[NotifyState::Watchdog]);
                }
            }

            // P1: Handle Config Updates (Runtime Configuration via UI)
            msg = async {
                if let Some(ref mut sub) = config_subscription {
                    sub.next().await
                } else {
                    std::future::pending::<Option<ironcrab::nats::NatsMessage>>().await
                }
            } => {
                if let Some(nats_msg) = msg {
                    match serde_json::from_slice::<ConfigUpdate>(&nats_msg.payload) {
                        Ok(update) => {
                            if update.target_component == "market-data" {
                                info!(
                                    component = %update.target_component,
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
                                debug!(component = %update.target_component, "Ignoring config update for other component");
                            }
                        }
                        Err(e) => {
                            warn!(error = %e, "Failed to deserialize ConfigUpdate");
                        }
                    }
                }
            }

            _ = &mut shutdown => {
                info!("Shutdown signal received");
                break;
            }
        }
    }

    Ok(())
}
