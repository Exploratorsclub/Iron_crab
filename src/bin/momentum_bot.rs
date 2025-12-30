//! momentum-bot binary – Strategy Plane (EARLY + ESTABLISHED policies)
//!
//! Source of Truth: docs/TARGET_ARCHITECTURE.md §2.2
//!
//! Responsibilities:
//! - Subscribe to MarketEvents from NATS
//! - Classify regime: EARLY vs ESTABLISHED
//! - Apply momentum policy to generate TradeIntents
//! - Publish TradeIntents to NATS
//! - Write trade_intents JSONL for replay
//!
//! This binary does NOT:
//! - Load wallet keys
//! - Sign or send transactions
//! - Directly call RPC/Geyser (gets data via MarketEvents)

use anyhow::Result;
use clap::Parser;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use ironcrab::ipc::{
    ExplicitAmount, IntentOrigin, IntentTier, MarketEvent, MarketEventKind, TradeIntent,
    TradeResources, TradeSide, TradingRegime,
};
use ironcrab::metrics::serve_metrics;
use ironcrab::nats::{NatsClient, NatsConfig, TOPIC_MARKET_EVENTS, TOPIC_TRADE_INTENTS};
use ironcrab::storage::{JsonlWriter, JsonlWriterConfig};

/// Build version for decision records
const BUILD_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Parser, Debug)]
#[command(name = "momentum-bot")]
#[command(about = "IronCrab Strategy Plane – Momentum policy and TradeIntent generation")]
struct Args {
    /// Path to configuration file
    #[arg(short, long, default_value = "config.toml")]
    config: PathBuf,

    /// NATS server URL
    #[arg(long, env = "NATS_URL", default_value = "nats://localhost:4222")]
    nats_url: String,

    /// Prometheus metrics port
    #[arg(long, default_value = "9802")]
    metrics_port: u16,

    /// Log directory override
    #[arg(long, env = "IRONCRAB_LOG_DIR")]
    log_dir: Option<PathBuf>,

    /// Test mode: generate intents for allowlisted mints only
    #[arg(long)]
    test_mode: bool,

    /// Dry run: don't publish to NATS
    #[arg(long)]
    dry_run: bool,
}

/// Momentum policy configuration
#[derive(Debug, Clone)]
struct MomentumConfig {
    /// Minimum liquidity (SOL) for EARLY regime
    early_min_liquidity_sol: f64,
    /// Minimum liquidity (SOL) for ESTABLISHED regime
    established_min_liquidity_sol: f64,
    /// Slot threshold for EARLY -> ESTABLISHED transition
    early_slot_threshold: u64,
    /// Max slippage BPS for EARLY trades
    early_max_slippage_bps: u32,
    /// Max slippage BPS for ESTABLISHED trades
    established_max_slippage_bps: u32,
    /// Default position size (SOL lamports)
    default_position_lamports: u64,
    /// Allowlist for test mode (mints that trigger intents)
    test_allowlist: HashSet<String>,
}

impl Default for MomentumConfig {
    fn default() -> Self {
        Self {
            early_min_liquidity_sol: 5.0,
            established_min_liquidity_sol: 20.0,
            early_slot_threshold: 1000,
            early_max_slippage_bps: 300,
            established_max_slippage_bps: 100,
            default_position_lamports: 100_000_000, // 0.1 SOL
            test_allowlist: HashSet::new(),
        }
    }
}

/// Runtime context for momentum-bot
struct MomentumContext {
    run_id: String,
    config: MomentumConfig,
    nats: Option<NatsClient>,
    jsonl_writer: JsonlWriter,
    intent_counter: std::sync::atomic::AtomicU64,
    /// Track known pools (pool_address -> first_seen_slot)
    pool_first_seen: parking_lot::RwLock<std::collections::HashMap<String, u64>>,
}

impl MomentumContext {
    fn next_intent_id(&self) -> String {
        let n = self
            .intent_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        format!("int-{}-{:06}", &self.run_id[..8], n)
    }

    /// Classify trading regime for a pool
    fn classify_regime(&self, pool_address: &str, current_slot: u64) -> TradingRegime {
        let first_seen = self.pool_first_seen.read().get(pool_address).copied();

        match first_seen {
            Some(first_slot) => {
                let age_slots = current_slot.saturating_sub(first_slot);
                if age_slots < self.config.early_slot_threshold {
                    TradingRegime::Early
                } else {
                    TradingRegime::Established
                }
            }
            None => TradingRegime::Early, // New pool = EARLY
        }
    }

    /// Record first-seen slot for a pool
    fn record_pool_seen(&self, pool_address: &str, slot: u64) {
        let mut pools = self.pool_first_seen.write();
        pools.entry(pool_address.to_string()).or_insert(slot);
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("momentum_bot=info".parse()?)
                .add_directive("ironcrab=info".parse()?),
        )
        .init();

    let args = Args::parse();
    let run_id = Uuid::new_v4().to_string();

    info!(
        run_id = %run_id,
        config = %args.config.display(),
        test_mode = args.test_mode,
        metrics_port = args.metrics_port,
        "Starting momentum-bot service"
    );

    // Start metrics server
    let metrics_addr = std::net::SocketAddr::from(([0, 0, 0, 0], args.metrics_port));
    tokio::spawn(async move {
        if let Err(e) = serve_metrics(metrics_addr).await {
            error!(error = %e, "Metrics server failed");
        }
    });
    info!(port = args.metrics_port, "Metrics server started at /metrics");

    // === P0 Check: Ensure no wallet keys are loaded ===
    // momentum-bot is KEYLESS per architecture
    if std::env::var("IRONCRAB_KEYPAIR_JSON").is_ok()
        || std::env::var("IRONCRAB_KEYPAIR_B64").is_ok()
        || std::env::var("IRONCRAB_KEYPAIR_PATH").is_ok()
    {
        error!("ERROR: Wallet key environment variables detected!");
        error!("momentum-bot is KEYLESS per architecture. Remove key variables and restart.");
        error!("Only execution-engine should have access to wallet keys.");
        std::process::exit(1);
    }

    // Setup config
    let mut momentum_config = MomentumConfig::default();
    if args.test_mode {
        // In test mode, add a test mint to allowlist
        momentum_config
            .test_allowlist
            .insert("TestMint111111111111111111111111111111111111".to_string());
        info!("Test mode enabled with allowlist");
    }

    // Setup JSONL writer
    let log_dir = args
        .log_dir
        .clone()
        .unwrap_or_else(|| PathBuf::from("trade_logs/intents"));
    let jsonl_config = JsonlWriterConfig::new("trade_intents").with_log_dir(&log_dir);
    let jsonl_writer = JsonlWriter::new(jsonl_config)?;

    info!(log_dir = %log_dir.display(), "JSONL writer initialized");

    // Setup NATS
    let nats = if args.dry_run {
        info!("Dry-run mode: NATS publishing disabled");
        None
    } else {
        let config = NatsConfig::new(&args.nats_url, "momentum-bot");
        let mut client = NatsClient::new(config);
        if let Err(e) = client.connect().await {
            warn!(error = %e, "Failed to connect to NATS (continuing without)");
            None
        } else {
            info!(url = %args.nats_url, "Connected to NATS");
            Some(client)
        }
    };

    let ctx = Arc::new(MomentumContext {
        run_id: run_id.clone(),
        config: momentum_config,
        nats,
        jsonl_writer,
        intent_counter: std::sync::atomic::AtomicU64::new(0),
        pool_first_seen: parking_lot::RwLock::new(std::collections::HashMap::new()),
    });

    // === Main Loop: Process MarketEvents ===
    info!("Entering main event loop");

    // For MVP without real NATS, simulate receiving events
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
    let mut simulated_slot: u64 = 0;

    // Graceful shutdown handling
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = interval.tick() => {
                simulated_slot += 5;

                // MVP: Simulate receiving a pool creation event in test mode
                if args.test_mode && simulated_slot == 10 {
                    let event = MarketEvent::new(
                        "market-data",
                        BUILD_VERSION,
                        &ctx.run_id,
                        format!("evt-sim-{}", simulated_slot),
                        "simulated",
                        Some(simulated_slot),
                        MarketEventKind::PoolCreated {
                            pool_address: "TestPool123".to_string(),
                            base_mint: "TestMint111111111111111111111111111111111111".to_string(),
                            quote_mint: "So11111111111111111111111111111111111111112".to_string(),
                            dex: "raydium".to_string(),
                            initial_liquidity_sol: Some(rust_decimal::Decimal::from(10)),
                        },
                    };

                    if let Err(e) = process_market_event(&ctx, &event).await {
                        error!(error = %e, "Failed to process market event");
                    }
                }

                // Periodic heartbeat
                if simulated_slot % 60 == 0 {
                    let (records, bytes) = ctx.jsonl_writer.stats();
                    let pools = ctx.pool_first_seen.read().len();
                    info!(
                        slot = simulated_slot,
                        intents_written = records,
                        bytes_written = bytes,
                        pools_tracked = pools,
                        "Momentum-bot heartbeat"
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
    ctx.jsonl_writer.flush()?;
    info!(run_id = %run_id, "momentum-bot shutdown complete");

    Ok(())
}

/// Process a MarketEvent and potentially generate TradeIntents
async fn process_market_event(ctx: &MomentumContext, event: &MarketEvent) -> Result<()> {
    match &event.kind {
        MarketEventKind::PoolCreated {
            pool_address,
            base_mint,
            quote_mint,
            dex,
            initial_liquidity_sol,
        } => {
            let slot = event.slot.unwrap_or(0);
            ctx.record_pool_seen(pool_address, slot);

            let regime = ctx.classify_regime(pool_address, slot);

            debug!(
                pool = %pool_address,
                base = %base_mint,
                dex = %dex,
                regime = ?regime,
                liquidity = ?initial_liquidity_sol,
                "Pool created event"
            );

            // Check if we should generate an intent
            let should_trade = match regime {
                TradingRegime::Early => {
                    // EARLY policy: strict filters
                    let liq = initial_liquidity_sol
                        .map(|d| d.to_string().parse::<f64>().unwrap_or(0.0))
                        .unwrap_or(0.0);

                    let in_allowlist = ctx.config.test_allowlist.contains(base_mint);

                    liq >= ctx.config.early_min_liquidity_sol || in_allowlist
                }
                TradingRegime::Established => {
                    // ESTABLISHED policy: more relaxed
                    let liq = initial_liquidity_sol
                        .map(|d| d.to_string().parse::<f64>().unwrap_or(0.0))
                        .unwrap_or(0.0);

                    liq >= ctx.config.established_min_liquidity_sol
                }
                TradingRegime::NotApplicable => false,
            };

            if should_trade {
                let max_slippage = match regime {
                    TradingRegime::Early => ctx.config.early_max_slippage_bps,
                    _ => ctx.config.established_max_slippage_bps,
                };

                let intent = TradeIntent::new(
                    "momentum-bot",
                    BUILD_VERSION,
                    &ctx.run_id,
                    ctx.next_intent_id(),
                    "momentum-bot",
                    IntentTier::Tier1,
                    IntentOrigin::StrategyA,
                    ExplicitAmount::new(ctx.config.default_position_lamports, 9),
                    TradeResources {
                        input_mint: quote_mint.clone(), // SOL
                        output_mint: base_mint.clone(),
                        pools: vec![pool_address.clone()],
                        accounts: vec![],
                    },
                    50, // Expected ROI: 0.5%
                    max_slippage,
                    TradeSide::Buy,
                    regime,
                )
                .with_ttl_ms(5000)
                .with_trigger(event.event_id.clone());

                info!(
                    intent_id = %intent.intent_id,
                    pool = %pool_address,
                    regime = ?regime,
                    "Generated TradeIntent"
                );

                // Write to JSONL (P0 requirement)
                ctx.jsonl_writer.write(&intent)?;

                // Publish to NATS
                if let Some(ref nats) = ctx.nats {
                    nats.publish(TOPIC_TRADE_INTENTS, &intent).await?;
                }
            }
        }
        MarketEventKind::SlotUpdate { current_slot } => {
            // Just track slot progression
            debug!(current_slot, "Slot update");
        }
        _ => {
            // Other event types: log for now
            debug!(event_id = %event.event_id, "Unhandled event type");
        }
    }

    Ok(())
}
