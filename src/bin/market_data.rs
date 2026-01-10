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

use anyhow::Result;
use clap::Parser;
use solana_sdk::pubkey::Pubkey;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::watch;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use ironcrab::config::WalletTrackerCfg;
use ironcrab::ipc::{
    ConfigUpdate, ConfigUpdateResponse, ConfigUpdateStatus, MarketEvent, MarketEventKind,
};
use ironcrab::metrics::{
    serve_metrics, MARKET_EVENTS_PUBLISHED_TOTAL, MARKET_EVENTS_RECEIVED_TOTAL, NATS_ERRORS_TOTAL,
    NATS_MESSAGES_PUBLISHED_TOTAL, POOLS_TRACKED_GAUGE,
};
use ironcrab::nats::{NatsClient, NatsConfig, TOPIC_MARKET_EVENTS};
use ironcrab::solana::dex_parser::{
    parse_account_update, parse_transaction_update, DexType, ParsedDexEvent,
};
use ironcrab::solana::geyser_pool_discovery::GeyserPoolDiscovery;
use ironcrab::solana::rpc::SolanaRpc;
use ironcrab::solana::wallet_tracker::WalletTracker;
use spl_token::solana_program::program_option::COption;
use spl_token::solana_program::program_pack::Pack;
use spl_token_2022::extension::StateWithExtensions;

/// NATS topic for config reload (P1: Runtime Configuration via UI)
const TOPIC_CONFIG_RELOAD: &str = "ironcrab.control.config.reload";
use ironcrab::solana::geyser_listener::GeyserListener;
use ironcrab::storage::{JsonlWriter, JsonlWriterConfig};

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

    let ctx = Arc::new(MarketDataContext {
        run_id: run_id.clone(),
        config: parking_lot::RwLock::new(MarketDataConfig::default()),
        nats,
        jsonl_writer,
        event_counter: std::sync::atomic::AtomicU64::new(0),
        wallet_tracker,
        tracked_mints: parking_lot::RwLock::new(std::collections::HashSet::new()),
        tracked_mints_tx,
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
) -> Result<()> {
    // Initialize RPC client for fallback/metadata (prefer local RPC, fallback to Helius)
    let rpc_url = std::env::var("SOLANA_RPC_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:8899".to_string()); // Local validator/private RPC preferred
    let rpc = Arc::new(SolanaRpc::new(&rpc_url));
    info!(rpc_url = %rpc_url, "Initialized RPC client for metadata/fallback");

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
    let pool_discovery_handle = tokio::spawn(async move {
        if let Err(e) = pool_discovery.start().await {
            error!(error = %e, "GeyserPoolDiscovery crashed");
        }
    });

    // Start legacy GeyserListener for transaction parsing (will be phased out in favor of pool discovery)
    let (listener, mut account_rx, mut transaction_rx) = GeyserListener::new_with_tracked_accounts(
        geyser_url.to_string(),
        program_ids,
        tracked_mints_rx,
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

                // Try to parse as DEX pool event
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
                if let Some(parsed) = parsed_event.as_ref() {
                    let mint_opt: Option<Pubkey> = match parsed {
                        ParsedDexEvent::PoolCreated { base_mint, .. } => Some(*base_mint),
                        ParsedDexEvent::Trade { mint, .. } => Some(*mint),
                        ParsedDexEvent::LiquidityRemoved { mint, .. } => Some(*mint),
                    };
                    if let Some(mint) = mint_opt {
                        let mut tracked = ctx.tracked_mints.write();
                        if tracked.insert(mint) {
                            // push updated list to geyser listener (resubscribe)
                            let updated: Vec<Pubkey> = tracked.iter().copied().collect();
                            let _ = ctx.tracked_mints_tx.send(updated);
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
                if let Some(ParsedDexEvent::Trade {
                    pool_address,
                    dex: DexType::PumpFunAmm,
                    pool_accounts: Some(pool_accounts),
                    ..
                }) = parsed_event.as_ref()
                {
                    // v1 order (see MarketEventKind::DexPoolAccounts docs): base_mint at [2], quote_mint at [3]
                    let base_mint = pool_accounts.get(2).map(|p| p.to_string()).unwrap_or_default();
                    let quote_mint = pool_accounts.get(3).map(|p| p.to_string()).unwrap_or_default();

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

                // Track base mint for metadata fetching
                let mut tracked = ctx.tracked_mints.write();
                if tracked.insert(pool_event.base_mint) {
                    let updated: Vec<Pubkey> = tracked.iter().copied().collect();
                    let _ = ctx.tracked_mints_tx.send(updated);
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
