//! position-manager binary — PA-6b PositionAuthority reducer + sole KV writer.
//!
//! Keyless process that maintains PositionAuthority from JetStream
//! (ExecutionResult + WalletBalanceSnapshot) and publishes to the productive
//! `POSITION_AUTHORITY` KV bucket. EE keeps an in-process reducer for PA-3/4 gates only.

use anyhow::{Context, Result};
use clap::Parser;
use futures::StreamExt;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tracing::{debug, error, info, warn};

use ironcrab::config::Config;
use ironcrab::ipc::schema::{ExecutionResult, ExecutionStatus, MarketEvent, MarketEventKind};
use ironcrab::metrics::{
    record_activity, record_position_manager_event_applied,
    refresh_position_manager_authority_gauges, serve_metrics, set_readiness_nats_connected,
    MetricsComponent, NATS_MESSAGES_RECEIVED_TOTAL,
};
use ironcrab::nats::jetstream::{
    ensure_execution_results_stream, execution_results_consumer_config,
    wallet_snapshot_consumer_config, wallet_snapshot_live_consumer_config_position_manager,
    EXECUTION_RESULTS_STREAM_NAME, WALLET_SNAPSHOT_STREAM_NAME,
};
use ironcrab::nats::{NatsClient, NatsConfig, TOPIC_EXECUTION_RESULTS, TOPIC_MARKET_EVENTS};
use ironcrab::position_authority::{
    reconcile_position_authority_kv_after_restart, PositionAuthority,
    PositionAuthorityKvMetricsSink, PositionAuthorityKvPublisher,
};

type JetStreamPullConsumer =
    async_nats::jetstream::consumer::Consumer<async_nats::jetstream::consumer::pull::Config>;

const PM_JETSTREAM_CONSUMER_BATCH_MAX: usize = 15;
const PM_JETSTREAM_CONSUMER_FETCH_EXPIRES: Duration = Duration::from_millis(100);
const PM_EXECUTION_RESULT_FETCH_EXPIRES: Duration = Duration::from_millis(250);

#[derive(Parser, Debug)]
#[command(name = "position-manager")]
#[command(about = "IronCrab PositionAuthority reducer + POSITION_AUTHORITY KV writer (keyless)")]
struct Args {
    /// Path to configuration file (optional; reads [position_manager] when present).
    #[arg(short, long, default_value = "config.toml")]
    config: PathBuf,

    /// NATS server URL
    #[arg(long, env = "NATS_URL", default_value = "nats://localhost:4222")]
    nats_url: String,

    /// Prometheus metrics port
    #[arg(long, default_value = "9805")]
    metrics_port: u16,

    /// Trading wallet pubkey for wallet-snapshot JetStream filter
    #[arg(long, env = "IRONCRAB_WALLET_PUBKEY")]
    wallet_pubkey: Option<String>,
}

struct PositionManagerContext {
    position_authority: Mutex<PositionAuthority>,
    kv_publisher: PositionAuthorityKvPublisher,
}

impl PositionManagerContext {
    fn new(kv_publisher: PositionAuthorityKvPublisher) -> Self {
        Self {
            position_authority: Mutex::new(PositionAuthority::new()),
            kv_publisher,
        }
    }

    fn apply_execution_result(&self, exec: &ExecutionResult) {
        if exec.status != ExecutionStatus::Confirmed {
            return;
        }
        let changes = {
            let mut pa = self.position_authority.lock();
            pa.apply_from_confirmed_execution_result(exec)
        };
        self.kv_publisher.enqueue(&changes);
        record_position_manager_event_applied();
        self.refresh_metrics();
    }

    fn apply_wallet_event_kind(&self, kind: &MarketEventKind) {
        let changes = {
            let mut pa = self.position_authority.lock();
            pa.apply_from_wallet_market_event_kind(kind)
        };
        self.kv_publisher.enqueue(&changes);
        record_position_manager_event_applied();
        self.refresh_metrics();
    }

    fn refresh_metrics(&self) {
        let pa = self.position_authority.lock();
        refresh_position_manager_authority_gauges(
            pa.open_positions_count() as u64,
            pa.reconcile_needed_positions_count() as u64,
        );
    }
}

fn ensure_keyless_or_exit() {
    let key_vars = [
        "IRONCRAB_KEYPAIR_JSON",
        "IRONCRAB_KEYPAIR_B64",
        "IRONCRAB_KEYPAIR_PATH",
        "IRONCRAB_KEYPAIR_BASE58",
    ];

    if key_vars.iter().any(|v| std::env::var(v).is_ok()) {
        error!(
            "ERROR: Wallet key environment variables detected! position-manager is KEYLESS per architecture."
        );
        error!("Only execution-engine should have access to wallet keys.");
        std::process::exit(1);
    }
}

fn resolve_wallet_pubkey(args: &Args) -> Result<String> {
    if let Some(ref pk) = args.wallet_pubkey {
        return Ok(pk.clone());
    }

    if let Ok(pk) = std::env::var("IRONCRAB_WALLET_PUBKEY") {
        return Ok(pk);
    }

    if args.config.exists() {
        let cfg = Config::load(&args.config).context("load config for wallet_pubkey")?;
        if let Some(pm) = cfg.position_manager {
            if let Some(pk) = pm.wallet_pubkey {
                return Ok(pk);
            }
        }
    }

    anyhow::bail!(
        "wallet pubkey required: pass --wallet-pubkey, set IRONCRAB_WALLET_PUBKEY, or [position_manager].wallet_pubkey in config"
    );
}

fn resolve_metrics_port(args: &Args) -> u16 {
    if args.config.exists() {
        if let Ok(cfg) = Config::load(&args.config) {
            if let Some(pm) = cfg.position_manager {
                return pm.metrics_port;
            }
        }
    }
    args.metrics_port
}

fn is_enabled(args: &Args) -> bool {
    if args.config.exists() {
        if let Ok(cfg) = Config::load(&args.config) {
            if let Some(pm) = cfg.position_manager {
                return pm.enabled;
            }
        }
    }
    // CLI-only runs default to enabled so operators can smoke-test without config edits.
    true
}

/// Bootstrap wallet snapshots from JetStream (LastPerSubject per mint).
/// Returns observed count and event kinds for KV reconcile (apply deferred to reconcile).
async fn bootstrap_wallet_snapshot_kinds(
    nats: &NatsClient,
    wallet: &str,
) -> (usize, Vec<MarketEventKind>) {
    use async_nats::jetstream;

    let jetstream = jetstream::new(nats.client().clone());
    let stream = match jetstream.get_stream(WALLET_SNAPSHOT_STREAM_NAME).await {
        Ok(s) => s,
        Err(e) => {
            warn!(
                error = %e,
                stream = WALLET_SNAPSHOT_STREAM_NAME,
                "Wallet snapshot stream not found during bootstrap (market-data may not be running)"
            );
            return (0, Vec::new());
        }
    };

    let mut consumer_config = wallet_snapshot_consumer_config();
    consumer_config.filter_subject = format!("ironcrab.wallet_snapshot.{}.*", wallet);

    let consumer = match stream.create_consumer(consumer_config).await {
        Ok(c) => c,
        Err(e) => {
            warn!(error = %e, "Failed to create wallet snapshot bootstrap consumer");
            return (0, Vec::new());
        }
    };

    let batch_size = 1000;
    let mut observed = 0usize;
    let mut wallet_snapshot_kinds = Vec::new();

    loop {
        let mut messages = match consumer.fetch().max_messages(batch_size).messages().await {
            Ok(m) => m,
            Err(e) => {
                debug!(error = %e, "Wallet snapshot bootstrap fetch ended");
                break;
            }
        };

        let mut batch_count = 0;
        while let Some(msg_result) = messages.next().await {
            batch_count += 1;
            let msg = match msg_result {
                Ok(m) => m,
                Err(e) => {
                    debug!(error = %e, "Wallet snapshot bootstrap message error");
                    continue;
                }
            };

            match serde_json::from_slice::<MarketEvent>(&msg.payload) {
                Ok(event) => {
                    observed += 1;
                    if matches!(
                        event.kind,
                        MarketEventKind::WalletBalanceSnapshot { .. }
                            | MarketEventKind::WalletSnapshotComplete { .. }
                    ) {
                        wallet_snapshot_kinds.push(event.kind);
                    }
                }
                Err(e) => {
                    debug!(error = %e, "Failed to deserialize wallet snapshot MarketEvent");
                }
            }

            if let Err(e) = msg.ack().await {
                debug!(error = %e, "Failed to ack wallet snapshot bootstrap message");
            }
        }

        if batch_count < batch_size {
            break;
        }
    }

    info!(
        wallet = %wallet,
        snapshots = observed,
        kinds = wallet_snapshot_kinds.len(),
        "Wallet snapshot bootstrap kinds collected for PositionAuthority KV reconcile"
    );

    (observed, wallet_snapshot_kinds)
}

async fn create_wallet_snapshot_live_consumer(
    nats: &NatsClient,
    wallet: &str,
    bootstrap_observed: usize,
) -> Option<JetStreamPullConsumer> {
    use async_nats::jetstream;

    let jetstream = jetstream::new(nats.client().clone());
    let stream = match jetstream.get_stream(WALLET_SNAPSHOT_STREAM_NAME).await {
        Ok(s) => s,
        Err(e) => {
            warn!(
                error = %e,
                stream = WALLET_SNAPSHOT_STREAM_NAME,
                "JetStream wallet snapshot stream not found (market-data may not be running)"
            );
            return None;
        }
    };

    let mut cfg = wallet_snapshot_live_consumer_config_position_manager(wallet);
    if bootstrap_observed == 0 {
        cfg.deliver_policy = jetstream::consumer::DeliverPolicy::LastPerSubject;
    }

    match stream.create_consumer(cfg).await {
        Ok(consumer) => {
            info!(
                stream = WALLET_SNAPSHOT_STREAM_NAME,
                wallet = %wallet,
                deliver_policy = if bootstrap_observed == 0 {
                    "LastPerSubject"
                } else {
                    "New"
                },
                "Subscribed to JetStream WalletBalanceSnapshot (live)"
            );
            Some(consumer)
        }
        Err(e) => {
            warn!(error = %e, "Failed to create JetStream wallet snapshot live consumer");
            None
        }
    }
}

async fn run_wallet_snapshot_consumer_task(
    ctx: Arc<PositionManagerContext>,
    consumer: JetStreamPullConsumer,
    shutdown_rx: watch::Receiver<bool>,
) {
    loop {
        if *shutdown_rx.borrow() {
            info!("Wallet snapshot consumer task shutting down");
            break;
        }

        match consumer
            .fetch()
            .max_messages(PM_JETSTREAM_CONSUMER_BATCH_MAX)
            .expires(PM_JETSTREAM_CONSUMER_FETCH_EXPIRES)
            .messages()
            .await
        {
            Ok(mut messages) => {
                let mut batch: Vec<(async_nats::jetstream::Message, MarketEvent)> = Vec::new();
                while let Some(msg_result) = messages.next().await {
                    match msg_result {
                        Ok(msg) => match serde_json::from_slice::<MarketEvent>(&msg.payload) {
                            Ok(event) => batch.push((msg, event)),
                            Err(e) => {
                                debug!(error = %e, "Failed to deserialize wallet snapshot MarketEvent");
                                let _ = msg.ack().await;
                            }
                        },
                        Err(e) => {
                            debug!(error = %e, "JetStream wallet snapshot fetch returned error");
                        }
                    }
                }

                let mut last_index_by_mint: HashMap<String, usize> = HashMap::new();
                for (idx, (_, event)) in batch.iter().enumerate() {
                    if let MarketEventKind::WalletBalanceSnapshot { mint, .. } = &event.kind {
                        last_index_by_mint.insert(mint.clone(), idx);
                    }
                }

                for (idx, (msg, event)) in batch.into_iter().enumerate() {
                    let apply =
                        if let MarketEventKind::WalletBalanceSnapshot { mint, .. } = &event.kind {
                            last_index_by_mint.get(mint) == Some(&idx)
                        } else {
                            true
                        };
                    if apply {
                        record_activity();
                        NATS_MESSAGES_RECEIVED_TOTAL.fetch_add(1, Ordering::Relaxed);
                        ctx.apply_wallet_event_kind(&event.kind);
                    }
                    if let Err(e) = msg.ack().await {
                        debug!(error = %e, "Failed to ack wallet snapshot message");
                    }
                }
            }
            Err(e) => {
                debug!(error = %e, "No new wallet snapshot messages in JetStream");
            }
        }

        tokio::task::yield_now().await;
    }
}

async fn drain_execution_results(
    consumer: &JetStreamPullConsumer,
    ctx: &Arc<PositionManagerContext>,
    max_messages: usize,
    fetch_expires: Duration,
) -> u32 {
    let mut messages = match consumer
        .fetch()
        .max_messages(max_messages)
        .expires(fetch_expires)
        .messages()
        .await
    {
        Ok(m) => m,
        Err(e) => {
            debug!(error = %e, "ExecutionResult JetStream fetch failed (may be empty)");
            return 0;
        }
    };

    let mut processed: u32 = 0;
    while let Some(msg_res) = messages.next().await {
        match msg_res {
            Ok(msg) => {
                record_activity();
                NATS_MESSAGES_RECEIVED_TOTAL.fetch_add(1, Ordering::Relaxed);
                match serde_json::from_slice::<ExecutionResult>(&msg.payload) {
                    Ok(result) => {
                        ctx.apply_execution_result(&result);
                        processed = processed.saturating_add(1);
                    }
                    Err(e) => {
                        warn!(error = %e, "Failed to deserialize ExecutionResult");
                    }
                }
                if let Err(e) = msg.ack().await {
                    warn!(error = %e, "Failed to ack ExecutionResult");
                }
            }
            Err(e) => {
                warn!(error = %e, "ExecutionResult stream message error");
            }
        }
    }

    processed
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("position_manager=info".parse()?)
                .add_directive("ironcrab=info".parse()?),
        )
        .init();

    let args = Args::parse();
    ensure_keyless_or_exit();

    if !is_enabled(&args) {
        info!(
            config = %args.config.display(),
            "position-manager disabled via [position_manager].enabled=false — exiting"
        );
        return Ok(());
    }

    let wallet = resolve_wallet_pubkey(&args)?;
    let metrics_port = resolve_metrics_port(&args);

    info!(
        wallet = %wallet,
        metrics_port,
        nats_url = %args.nats_url,
        "Starting position-manager (PA-6b — sole POSITION_AUTHORITY KV writer)"
    );

    let metrics_addr = SocketAddr::from(([0, 0, 0, 0], metrics_port));
    tokio::spawn(async move {
        if let Err(e) = serve_metrics(metrics_addr, MetricsComponent::PositionManager).await {
            error!(error = %e, "Metrics server failed");
        }
    });

    let mut nats_config = NatsConfig::new(&args.nats_url, "position-manager");
    nats_config.request_timeout = NatsConfig::request_timeout_from_env(180);
    let mut nats = NatsClient::new(nats_config);
    nats.connect()
        .await
        .context("connect to NATS for position-manager")?;
    set_readiness_nats_connected(true);
    info!(url = %args.nats_url, "Connected to NATS");

    if let Err(e) = ensure_execution_results_stream(nats.client()).await {
        warn!(error = %e, "Failed to ensure EXECUTION_RESULTS stream (may already exist)");
    }

    let kv_publisher = PositionAuthorityKvPublisher::spawn(
        nats.clone_for_spawned_publish(),
        PositionAuthorityKvMetricsSink::PositionManager,
    );
    let ctx = Arc::new(PositionManagerContext::new(kv_publisher));

    let (bootstrap_observed, wallet_snapshot_kinds) =
        bootstrap_wallet_snapshot_kinds(&nats, &wallet).await;

    let position_authority_kv = tokio::sync::OnceCell::new();
    if let Err(e) = reconcile_position_authority_kv_after_restart(
        &nats,
        &ctx.position_authority,
        &position_authority_kv,
        &wallet_snapshot_kinds,
        PositionAuthorityKvMetricsSink::PositionManager,
    )
    .await
    {
        warn!(
            error = %e,
            "PositionAuthority KV startup reconcile failed (Momentum may see stale KV until next update)"
        );
    }
    ctx.refresh_metrics();

    let wallet_snapshot_consumer =
        create_wallet_snapshot_live_consumer(&nats, &wallet, bootstrap_observed).await;

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    if let Some(consumer) = wallet_snapshot_consumer {
        let wallet_ctx = Arc::clone(&ctx);
        let wallet_shutdown = shutdown_rx.clone();
        tokio::spawn(async move {
            run_wallet_snapshot_consumer_task(wallet_ctx, consumer, wallet_shutdown).await;
        });
    }

    let execution_js_consumer = {
        use async_nats::jetstream;

        let jetstream = jetstream::new(nats.client().clone());
        match jetstream.get_stream(EXECUTION_RESULTS_STREAM_NAME).await {
            Ok(stream) => match stream
                .create_consumer(execution_results_consumer_config("position-manager"))
                .await
            {
                Ok(consumer) => {
                    info!(
                        stream = EXECUTION_RESULTS_STREAM_NAME,
                        topic = TOPIC_EXECUTION_RESULTS,
                        "Subscribed to ExecutionResults via JetStream"
                    );
                    Some(consumer)
                }
                Err(e) => {
                    warn!(error = %e, "Failed to create execution results consumer");
                    None
                }
            },
            Err(e) => {
                warn!(
                    error = %e,
                    stream = EXECUTION_RESULTS_STREAM_NAME,
                    "Failed to get execution results stream"
                );
                None
            }
        }
    };

    let mut market_events_sub = match nats.subscribe(TOPIC_MARKET_EVENTS).await {
        Ok(sub) => {
            info!(
                topic = TOPIC_MARKET_EVENTS,
                "Subscribed to MarketEvents for WalletSnapshotComplete"
            );
            Some(sub)
        }
        Err(e) => {
            warn!(error = %e, topic = TOPIC_MARKET_EVENTS, "Failed to subscribe to MarketEvents");
            None
        }
    };

    loop {
        tokio::select! {
            biased;

            _ = tokio::signal::ctrl_c() => {
                info!("Shutdown signal received");
                let _ = shutdown_tx.send(true);
                break;
            }

            msg = async {
                match market_events_sub.as_mut() {
                    Some(sub) => sub.next().await,
                    None => std::future::pending().await,
                }
            } => {
                if let Some(msg) = msg {
                    record_activity();
                    NATS_MESSAGES_RECEIVED_TOTAL.fetch_add(1, Ordering::Relaxed);
                    match serde_json::from_slice::<MarketEvent>(&msg.payload) {
                        Ok(event) => {
                            if matches!(event.kind, MarketEventKind::WalletSnapshotComplete { .. }) {
                                ctx.apply_wallet_event_kind(&event.kind);
                            }
                        }
                        Err(e) => {
                            debug!(error = %e, "Failed to deserialize MarketEvent");
                        }
                    }
                }
            }

            _ = async {
                if let Some(ref consumer) = execution_js_consumer {
                    let _ = drain_execution_results(
                        consumer,
                        &ctx,
                        PM_JETSTREAM_CONSUMER_BATCH_MAX,
                        PM_EXECUTION_RESULT_FETCH_EXPIRES,
                    )
                    .await;
                } else {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            } => {}
        }
    }

    info!("position-manager shutdown complete");
    Ok(())
}
