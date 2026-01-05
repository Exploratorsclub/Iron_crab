//! arb-strategy binary – Typ A Market-Driven Arbitrage Strategy
//!
//! Source of Truth: docs/TARGET_ARCHITECTURE.md §2.2.1
//!
//! Responsibilities:
//! - Consume MarketEvents from market-data
//! - Track pools across DEXes (same token pairs on different DEXes)
//! - Detect price spreads and calculate arbitrage opportunities
//! - Generate TradeIntents with origin_type: StrategyA
//!
//! This binary does NOT:
//! - Load wallet keys (keyless)
//! - Sign or send transactions
//! - React to specific parent transactions (that's Typ B MEV)

use anyhow::Result;
use clap::Parser;
use parking_lot::RwLock;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use ironcrab::ipc::{
    ExplicitAmount, IntentOrigin, IntentTier, MarketEvent, MarketEventKind, TradeIntent,
    TradeResources, TradeSide, TradingRegime,
};
use ironcrab::metrics::{
    serve_metrics, ARB_TRIANGLE_OPPORTUNITIES, INTENTS_GENERATED_TOTAL,
    MARKET_EVENTS_CONSUMED_TOTAL, NATS_MESSAGES_PUBLISHED_TOTAL, NATS_MESSAGES_RECEIVED_TOTAL,
    POOLS_TRACKED_GAUGE, TOKENS_TRACKED_GAUGE,
};
use ironcrab::nats::{NatsClient, NatsConfig};
use ironcrab::nats::{TOPIC_MARKET_EVENTS, TOPIC_TRADE_INTENTS};
use ironcrab::storage::{JsonlWriter, JsonlWriterConfig};

/// Build version for decision records
const BUILD_VERSION: &str = env!("CARGO_PKG_VERSION");

// ============================================================================
// Configuration
// ============================================================================

#[derive(Debug, Clone)]
struct ArbConfig {
    /// Minimum spread in bps to consider arbitrage. Default: 50 (0.5%)
    min_spread_bps: u32,
    /// Minimum profit in lamports after estimated fees. Default: 10_000_000 (0.01 SOL)
    min_profit_lamports: u64,
    /// Maximum position size in lamports. Default: 1_000_000_000 (1 SOL)
    max_position_lamports: u64,
    /// Estimated transaction cost in lamports. Default: 50_000 (0.00005 SOL)
    est_tx_cost_lamports: u64,
    /// Maximum slippage tolerance in bps. Default: 100 (1%)
    max_slippage_bps: u32,
    /// Cooldown between intents for same pair in ms. Default: 5000ms
    intent_cooldown_ms: u64,
    /// TTL for intents in ms. Default: 3000ms
    intent_ttl_ms: u64,
}

impl Default for ArbConfig {
    fn default() -> Self {
        Self {
            min_spread_bps: 50,                   // 0.5% minimum spread
            min_profit_lamports: 10_000_000,      // 0.01 SOL min profit
            max_position_lamports: 1_000_000_000, // 1 SOL max position
            est_tx_cost_lamports: 50_000,         // 0.00005 SOL tx cost
            max_slippage_bps: 100,                // 1% max slippage
            intent_cooldown_ms: 5000,             // 5s cooldown per pair
            intent_ttl_ms: 3000,                  // 3s TTL
        }
    }
}

#[derive(Parser, Debug)]
#[command(name = "arb-strategy")]
#[command(about = "IronCrab Typ A Arbitrage Strategy – Market-driven cross-DEX arbitrage")]
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

    /// Dry run: don't publish intents to NATS
    #[arg(long)]
    dry_run: bool,
}

// ============================================================================
// Pool Tracking for Cross-DEX Arbitrage
// ============================================================================

/// Tracks a pool's price/liquidity state
#[derive(Debug, Clone)]
struct PoolState {
    pool_address: String,
    dex: String,
    /// Last known price (quote per base, e.g., SOL per token)
    last_price: Option<Decimal>,
    /// Liquidity in SOL
    liquidity_sol: Decimal,
    /// Last update time
    last_update: Instant,
    /// Trade count for activity tracking
    trade_count: u64,
}

/// Tracks same token across multiple DEXes
#[derive(Debug)]
struct TokenArbTracker {
    base_mint: String,
    /// Pool states by DEX name
    pools_by_dex: HashMap<String, PoolState>,
    /// Last intent generated time
    last_intent_time: Option<Instant>,
}

impl TokenArbTracker {
    fn new(base_mint: &str) -> Self {
        Self {
            base_mint: base_mint.to_string(),
            pools_by_dex: HashMap::new(),
            last_intent_time: None,
        }
    }

    /// Add or update a pool for this token
    fn upsert_pool(&mut self, pool: PoolState) {
        self.pools_by_dex.insert(pool.dex.clone(), pool);
    }

    /// Check for arbitrage opportunity between DEXes
    /// Returns: Option<(buy_dex, sell_dex, spread_bps, estimated_profit_lamports)>
    fn check_arbitrage(&self, config: &ArbConfig) -> Option<ArbOpportunity> {
        // Need at least 2 DEXes with prices
        let pools_with_price: Vec<_> = self
            .pools_by_dex
            .values()
            .filter(|p| p.last_price.is_some())
            .collect();

        if pools_with_price.len() < 2 {
            debug!(
                mint = %self.base_mint,
                pools = pools_with_price.len(),
                "Arb check: insufficient pools with prices"
            );
            return None;
        }

        // Find best buy (lowest price) and best sell (highest price)
        let mut best_buy: Option<&PoolState> = None;
        let mut best_sell: Option<&PoolState> = None;

        for pool in &pools_with_price {
            let price = pool.last_price.unwrap();

            if best_buy.is_none() || price < best_buy.unwrap().last_price.unwrap() {
                best_buy = Some(pool);
            }
            if best_sell.is_none() || price > best_sell.unwrap().last_price.unwrap() {
                best_sell = Some(pool);
            }
        }

        let buy_pool = best_buy?;
        let sell_pool = best_sell?;

        // Don't arb same DEX
        if buy_pool.dex == sell_pool.dex {
            debug!(
                mint = %self.base_mint,
                dex = %buy_pool.dex,
                "Arb check rejected: same DEX for buy/sell"
            );
            return None;
        }

        let buy_price = buy_pool.last_price.unwrap();
        let sell_price = sell_pool.last_price.unwrap();

        // Calculate spread in bps
        // spread = (sell_price - buy_price) / buy_price * 10000
        if buy_price <= Decimal::ZERO {
            return None;
        }

        let spread = (sell_price - buy_price) / buy_price * Decimal::from(10000);
        let spread_bps = spread.to_string().parse::<i64>().unwrap_or(0);

        if spread_bps < config.min_spread_bps as i64 {
            debug!(
                mint = %self.base_mint,
                buy_dex = %buy_pool.dex,
                sell_dex = %sell_pool.dex,
                spread_bps = spread_bps,
                min_spread = config.min_spread_bps,
                "Arb check rejected: spread below minimum"
            );
            return None;
        }

        // Estimate profit
        // Use smaller liquidity pool as constraint
        let max_trade_sol = buy_pool
            .liquidity_sol
            .min(sell_pool.liquidity_sol)
            .min(Decimal::from(config.max_position_lamports) / Decimal::from(1_000_000_000u64));

        // Gross profit = trade_amount * spread_pct
        let gross_profit = max_trade_sol * (spread / Decimal::from(10000));
        let gross_profit_lamports = (gross_profit * Decimal::from(1_000_000_000u64))
            .to_string()
            .parse::<u64>()
            .unwrap_or(0);

        // Net profit after tx costs
        let net_profit = gross_profit_lamports.saturating_sub(config.est_tx_cost_lamports);

        if net_profit < config.min_profit_lamports {
            debug!(
                mint = %self.base_mint,
                buy_dex = %buy_pool.dex,
                sell_dex = %sell_pool.dex,
                spread_bps = spread_bps,
                gross_profit = gross_profit_lamports,
                tx_cost = config.est_tx_cost_lamports,
                net_profit = net_profit,
                min_profit = config.min_profit_lamports,
                "Arb check rejected: profit below minimum"
            );
            return None;
        }

        Some(ArbOpportunity {
            base_mint: self.base_mint.clone(),
            buy_dex: buy_pool.dex.clone(),
            buy_pool: buy_pool.pool_address.clone(),
            buy_price,
            sell_dex: sell_pool.dex.clone(),
            sell_pool: sell_pool.pool_address.clone(),
            sell_price,
            spread_bps: spread_bps as u32,
            trade_amount_lamports: (max_trade_sol * Decimal::from(1_000_000_000u64))
                .to_string()
                .parse::<u64>()
                .unwrap_or(config.max_position_lamports),
            estimated_profit_lamports: net_profit,
        })
    }
}

#[derive(Debug, Clone)]
struct ArbOpportunity {
    base_mint: String,
    buy_dex: String,
    buy_pool: String,
    buy_price: Decimal,
    sell_dex: String,
    sell_pool: String,
    sell_price: Decimal,
    spread_bps: u32,
    trade_amount_lamports: u64,
    estimated_profit_lamports: u64,
}

// ============================================================================
// Runtime Context
// ============================================================================

struct ArbContext {
    run_id: String,
    config: RwLock<ArbConfig>,
    nats: Option<NatsClient>,
    jsonl_writer: JsonlWriter,

    /// Token trackers for cross-DEX arbitrage
    trackers: RwLock<HashMap<String, TokenArbTracker>>,

    // Metrics
    events_received: AtomicU64,
    pools_tracked: AtomicU64,
    opportunities_found: AtomicU64,
    intents_generated: AtomicU64,
    intent_counter: AtomicU64,
}

impl ArbContext {
    fn next_intent_id(&self) -> String {
        let n = self.intent_counter.fetch_add(1, Ordering::Relaxed);
        format!("arb-{}-{:06}", &self.run_id[..8], n)
    }

    /// Update or create pool state from PoolCreated event
    fn handle_pool_created(
        &self,
        pool_address: &str,
        base_mint: &str,
        quote_mint: &str,
        dex: &str,
        liquidity_sol: Decimal,
    ) {
        // Only track SOL pairs for now
        const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
        if quote_mint != SOL_MINT {
            return;
        }

        let mut trackers = self.trackers.write();
        let tracker = trackers
            .entry(base_mint.to_string())
            .or_insert_with(|| TokenArbTracker::new(base_mint));

        let pool_state = PoolState {
            pool_address: pool_address.to_string(),
            dex: dex.to_string(),
            last_price: None,
            liquidity_sol,
            last_update: Instant::now(),
            trade_count: 0,
        };

        let is_new = !tracker.pools_by_dex.contains_key(dex);
        tracker.upsert_pool(pool_state);

        if is_new {
            self.pools_tracked.fetch_add(1, Ordering::Relaxed);
            debug!(
                mint = %base_mint,
                dex = %dex,
                pool = %pool_address,
                liquidity = %liquidity_sol,
                dexes = tracker.pools_by_dex.len(),
                "Pool added to arb tracker"
            );
        }
    }

    /// Update price from trade event
    fn handle_trade(
        &self,
        pool_address: &str,
        mint: &str,
        sol_amount: u64,
        token_amount: u64,
        _is_buy: bool,
    ) -> Option<ArbOpportunity> {
        if token_amount == 0 || sol_amount == 0 {
            return None;
        }

        // Calculate price: SOL per token
        let sol_dec = Decimal::from(sol_amount) / Decimal::from(1_000_000_000u64);
        let token_dec = Decimal::from(token_amount) / Decimal::from(1_000_000u64); // assume 6 decimals
        let price = sol_dec / token_dec;

        let config = self.config.read().clone();
        let mut trackers = self.trackers.write();

        if let Some(tracker) = trackers.get_mut(mint) {
            // Find which pool this is
            for pool in tracker.pools_by_dex.values_mut() {
                if pool.pool_address == pool_address {
                    pool.last_price = Some(price);
                    pool.trade_count += 1;
                    pool.last_update = Instant::now();
                    break;
                }
            }

            // Check for arbitrage opportunity
            if let Some(opp) = tracker.check_arbitrage(&config) {
                // Check cooldown
                let cooldown = Duration::from_millis(config.intent_cooldown_ms);
                if let Some(last_time) = tracker.last_intent_time {
                    if last_time.elapsed() < cooldown {
                        return None;
                    }
                }

                tracker.last_intent_time = Some(Instant::now());
                self.opportunities_found.fetch_add(1, Ordering::Relaxed);
                return Some(opp);
            }
        }

        None
    }
}

// ============================================================================
// Intent Generation
// ============================================================================

fn create_arb_intent(ctx: &ArbContext, opp: &ArbOpportunity) -> TradeIntent {
    let config = ctx.config.read();

    let resources = TradeResources {
        input_mint: "So11111111111111111111111111111111111111112".to_string(),
        output_mint: opp.base_mint.clone(),
        pools: vec![opp.buy_pool.clone(), opp.sell_pool.clone()],
        accounts: vec![],
    };

    let mut intent = TradeIntent::new(
        "arb-strategy",
        BUILD_VERSION,
        &ctx.run_id,
        ctx.next_intent_id(),
        "arb-strategy",
        IntentTier::Tier1,
        IntentOrigin::StrategyA, // Typ A - market-driven
        ExplicitAmount::new(opp.trade_amount_lamports, 9),
        resources,
        opp.spread_bps as i32,
        config.max_slippage_bps,
        TradeSide::Buy, // First leg: buy token
        TradingRegime::NotApplicable,
    );

    // Require atomic bundle execution
    intent = intent.with_bundle(Some(100_000)); // 0.0001 SOL tip

    // Add fee hints
    intent = intent.with_fee_hints(
        Some(400_000), // Cross-DEX arb needs more CU
        Some(100_000), // priority fee micro-lamports
        Some(1),       // elevated urgency
    );

    // Set TTL
    intent = intent.with_ttl_ms(config.intent_ttl_ms);

    // Add Cross-DEX metadata for execution-engine
    intent
        .metadata
        .insert("cross_dex_arb".to_string(), "true".to_string());
    intent
        .metadata
        .insert("buy_dex".to_string(), opp.buy_dex.clone());
    intent
        .metadata
        .insert("buy_pool".to_string(), opp.buy_pool.clone());
    intent
        .metadata
        .insert("buy_price".to_string(), opp.buy_price.to_string());
    intent
        .metadata
        .insert("sell_dex".to_string(), opp.sell_dex.clone());
    intent
        .metadata
        .insert("sell_pool".to_string(), opp.sell_pool.clone());
    intent
        .metadata
        .insert("sell_price".to_string(), opp.sell_price.to_string());
    intent
        .metadata
        .insert("spread_bps".to_string(), opp.spread_bps.to_string());
    intent.metadata.insert(
        "estimated_profit_lamports".to_string(),
        opp.estimated_profit_lamports.to_string(),
    );

    // Decision record: why this opportunity was chosen
    intent.metadata.insert("decision_reason".to_string(), format!(
        "Cross-DEX arb: Buy {} @ {} ({}), Sell @ {} ({}). Spread {}bps > min {}bps. Estimated profit {} lamports > min {}",
        opp.base_mint,
        opp.buy_price,
        opp.buy_dex,
        opp.sell_price,
        opp.sell_dex,
        opp.spread_bps,
        config.min_spread_bps,
        opp.estimated_profit_lamports,
        config.min_profit_lamports
    ));

    intent
}

// ============================================================================
// Main
// ============================================================================

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("arb_strategy=info".parse()?)
                .add_directive("ironcrab=info".parse()?),
        )
        .init();

    let args = Args::parse();
    let run_id = Uuid::new_v4().to_string();

    info!(
        run_id = %run_id,
        config = %args.config.display(),
        metrics_port = args.metrics_port,
        dry_run = args.dry_run,
        "Starting arb-strategy service (Typ A Market-Driven Arbitrage)"
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
    if std::env::var("IRONCRAB_KEYPAIR_JSON").is_ok()
        || std::env::var("IRONCRAB_KEYPAIR_B64").is_ok()
        || std::env::var("IRONCRAB_KEYPAIR_PATH").is_ok()
    {
        error!("ERROR: Wallet key environment variables detected!");
        error!("arb-strategy is KEYLESS per architecture. Remove key variables and restart.");
        std::process::exit(1);
    }

    // Setup JSONL writer
    let log_dir = args
        .log_dir
        .unwrap_or_else(|| PathBuf::from("trade_logs/arb_intents"));
    let jsonl_config = JsonlWriterConfig::new("arb_intents").with_log_dir(&log_dir);
    let jsonl_writer = JsonlWriter::new(jsonl_config)?;
    info!(log_dir = %log_dir.display(), "JSONL writer initialized");

    // Setup NATS
    let nats = if args.dry_run {
        info!("Dry-run mode: NATS publishing disabled");
        None
    } else {
        let config = NatsConfig::new(&args.nats_url, "arb-strategy");
        let mut client = NatsClient::new(config);
        if let Err(e) = client.connect().await {
            error!(error = %e, "Failed to connect to NATS");
            return Err(e);
        }
        info!(url = %args.nats_url, "Connected to NATS");
        Some(client)
    };

    let ctx = Arc::new(ArbContext {
        run_id: run_id.clone(),
        config: RwLock::new(ArbConfig::default()),
        nats,
        jsonl_writer,
        trackers: RwLock::new(HashMap::new()),
        events_received: AtomicU64::new(0),
        pools_tracked: AtomicU64::new(0),
        opportunities_found: AtomicU64::new(0),
        intents_generated: AtomicU64::new(0),
        intent_counter: AtomicU64::new(0),
    });

    // Subscribe to MarketEvents
    let market_subscription = if let Some(ref nats) = ctx.nats {
        match nats.subscribe(TOPIC_MARKET_EVENTS).await {
            Ok(sub) => {
                info!(topic = TOPIC_MARKET_EVENTS, "Subscribed to MarketEvents");
                Some(sub)
            }
            Err(e) => {
                error!(error = %e, "Failed to subscribe to MarketEvents");
                return Err(e);
            }
        }
    } else {
        None
    };

    // Main event loop
    info!("Entering main event loop");

    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    let mut market_sub = market_subscription;
    let mut heartbeat_interval = tokio::time::interval(Duration::from_secs(60));

    loop {
        tokio::select! {
            // MarketEvents
            msg = async {
                if let Some(ref mut sub) = market_sub {
                    sub.next().await
                } else {
                    std::future::pending::<Option<ironcrab::nats::NatsMessage>>().await
                }
            } => {
                if let Some(nats_msg) = msg {
                    // Prometheus: count inbound NATS messages for this process
                    NATS_MESSAGES_RECEIVED_TOTAL.fetch_add(1, Ordering::Relaxed);
                    ctx.events_received.fetch_add(1, Ordering::Relaxed);

                    match serde_json::from_slice::<MarketEvent>(&nats_msg.payload) {
                        Ok(event) => {
                            // Prometheus: count consumed MarketEvents for this process
                            MARKET_EVENTS_CONSUMED_TOTAL.fetch_add(1, Ordering::Relaxed);
                            if let Some(intent) = handle_market_event(&ctx, &event).await {
                                // Write to JSONL
                                if let Err(e) = ctx.jsonl_writer.write(&intent) {
                                    error!(error = %e, "Failed to write intent to JSONL");
                                }

                                // Publish to NATS
                                if let Some(ref nats) = ctx.nats {
                                    if let Err(e) = nats.publish(TOPIC_TRADE_INTENTS, &intent).await {
                                        warn!(error = %e, "Failed to publish intent to NATS");
                                    } else {
                                        // Prometheus: count outbound NATS messages and intents
                                        NATS_MESSAGES_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                                        INTENTS_GENERATED_TOTAL.fetch_add(1, Ordering::Relaxed);
                                        ctx.intents_generated.fetch_add(1, Ordering::Relaxed);
                                        info!(
                                            intent_id = %intent.intent_id,
                                            mint = %intent.resources.output_mint,
                                            spread_bps = intent.expected_roi_bps,
                                            "🎯 Arb intent published"
                                        );
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            warn!(error = %e, "Failed to deserialize MarketEvent");
                        }
                    }
                }
            }

            // Heartbeat
            _ = heartbeat_interval.tick() => {
                let (records, bytes) = ctx.jsonl_writer.stats();
                let trackers = ctx.trackers.read();
                let multi_dex_tokens = trackers.values()
                    .filter(|t| t.pools_by_dex.len() >= 2)
                    .count();

                // Prometheus: publish current gauges for this process
                POOLS_TRACKED_GAUGE.store(ctx.pools_tracked.load(Ordering::Relaxed), Ordering::Relaxed);
                TOKENS_TRACKED_GAUGE.store(trackers.len() as u64, Ordering::Relaxed);

                info!(
                    events_received = ctx.events_received.load(Ordering::Relaxed),
                    pools_tracked = ctx.pools_tracked.load(Ordering::Relaxed),
                    tokens_tracked = trackers.len(),
                    multi_dex_tokens = multi_dex_tokens,
                    opportunities_found = ctx.opportunities_found.load(Ordering::Relaxed),
                    intents_generated = ctx.intents_generated.load(Ordering::Relaxed),
                    intents_written = records,
                    bytes_written = bytes,
                    "arb-strategy heartbeat"
                );
            }

            _ = &mut shutdown => {
                info!("Shutdown signal received");
                break;
            }
        }
    }

    // Flush JSONL on shutdown
    ctx.jsonl_writer.flush()?;
    info!(run_id = %run_id, "arb-strategy shutdown complete");

    Ok(())
}

/// Handle a single MarketEvent
async fn handle_market_event(ctx: &ArbContext, event: &MarketEvent) -> Option<TradeIntent> {
    match &event.kind {
        MarketEventKind::PoolCreated {
            pool_address,
            base_mint,
            quote_mint,
            dex,
            initial_liquidity_sol,
        } => {
            let liquidity = initial_liquidity_sol.unwrap_or(Decimal::ZERO);
            ctx.handle_pool_created(pool_address, base_mint, quote_mint, dex, liquidity);
            None
        }

        MarketEventKind::Trade {
            pool_address,
            mint,
            sol_amount,
            token_amount,
            is_buy,
            ..
        } => {
            if let Some(opp) =
                ctx.handle_trade(pool_address, mint, *sol_amount, *token_amount, *is_buy)
            {
                // Prometheus: count arbitrage opportunities detected
                ARB_TRIANGLE_OPPORTUNITIES.fetch_add(1, Ordering::Relaxed);
                info!(
                    mint = %opp.base_mint,
                    buy_dex = %opp.buy_dex,
                    sell_dex = %opp.sell_dex,
                    spread_bps = opp.spread_bps,
                    profit_lamports = opp.estimated_profit_lamports,
                    "🔥 Arbitrage opportunity detected!"
                );
                Some(create_arb_intent(ctx, &opp))
            } else {
                None
            }
        }

        _ => None,
    }
}
