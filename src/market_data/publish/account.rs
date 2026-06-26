//! Account-path NATS publish queue: dedicated `md-publish` runtime + worker pool.

use super::core::publish_market_event_core_and_momentum_ex;
use super::host::PublishHost;
use crate::ipc::{MarketEvent, PriorityFeePercentiles};
use crate::market_data::sidefx::host::{
    market_event_should_nats_core, MarketEventCorePublishTrace,
};
use crate::metrics::{
    dec_market_data_account_publish_queue_depth,
    inc_market_data_account_publish_enqueue_dropped_total,
    inc_market_data_account_publish_queue_depth,
    record_market_data_account_publish_worker_job_duration_us,
    record_market_data_account_publish_worker_reconnect,
    record_market_data_account_publish_worker_stall,
    set_market_data_account_publish_worker_last_success_unix_ms, wall_clock_unix_ms_now,
    MARKET_EVENTS_PUBLISHED_TOTAL, NATS_ERRORS_TOTAL, NATS_MESSAGES_PUBLISHED_TOTAL,
};
use crate::nats::{NatsClient, NatsConfig, TOPIC_PRIORITY_FEE_SAMPLES};
use serde::Serialize;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{error, warn};

/// Dedicated NATS publish queue (JetStream + core MarketEvent).
pub const MARKET_DATA_ACCOUNT_PUBLISH_QUEUE_CAP: usize = 16_384;
/// Dispatcher → per-worker queue capacity (PR160).
pub const MARKET_DATA_ACCOUNT_PUBLISH_WORKER_DISPATCH_QUEUE_CAP: usize = 4096;
/// Per publish job wall timeout before abort + worker reconnect.
pub const MARKET_DATA_PUBLISH_WORKER_JOB_TIMEOUT: Duration = Duration::from_secs(2);

/// Bounded enqueue handle for the `md-publish` runtime.
pub type AccountPublishSender = mpsc::Sender<AccountPathNatsJob>;

pub fn account_publish_worker_count_from_env() -> usize {
    const DEF: usize = 4;
    const MAX: usize = 32;
    std::env::var("MARKET_DATA_PUBLISH_WORKER_COUNT")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n > 0 && n <= MAX)
        .unwrap_or(DEF)
}

pub enum AccountPathNatsJob {
    JetStream {
        subject: String,
        payload: serde_json::Value,
        bump_market_events_published_total: bool,
    },
    CoreMarketEvent {
        event: Box<MarketEvent>,
        trace: Option<MarketEventCorePublishTrace>,
    },
    /// Serialized JSON to a core NATS topic (not a [`MarketEvent`], e.g. priority fee samples).
    CoreTopicJson {
        topic: String,
        payload: serde_json::Value,
    },
}

#[inline]
pub fn try_enqueue_account_path_nats_job(
    tx: &AccountPublishSender,
    job: AccountPathNatsJob,
    log_ctx: &'static str,
) -> bool {
    inc_market_data_account_publish_queue_depth();
    match tx.try_send(job) {
        Ok(()) => true,
        Err(mpsc::error::TrySendError::Full(_)) => {
            dec_market_data_account_publish_queue_depth();
            inc_market_data_account_publish_enqueue_dropped_total();
            warn!(
                msg = log_ctx,
                "account_path: publish queue full (PR160 try_send drop)"
            );
            false
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            dec_market_data_account_publish_queue_depth();
            warn!(msg = log_ctx, "account_path: publish queue closed");
            false
        }
    }
}

async fn md_account_publish_execute_one_job(
    nats: &NatsClient,
    host: &dyn PublishHost,
    job: AccountPathNatsJob,
) {
    match job {
        AccountPathNatsJob::JetStream {
            subject,
            payload,
            bump_market_events_published_total,
        } => {
            let is_wallet_tx_confirm = subject.starts_with("ironcrab.wallet_tx_confirm.");
            match nats.jetstream_publish(&subject, &payload).await {
                Ok(true) => {
                    NATS_MESSAGES_PUBLISHED_TOTAL
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if bump_market_events_published_total {
                        MARKET_EVENTS_PUBLISHED_TOTAL
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
                Ok(false) => {
                    if is_wallet_tx_confirm {
                        error!(
                            subject = %subject,
                            "account_path: WalletTxConfirmed JetStream publish failed (timeout or drop)"
                        );
                    } else {
                        warn!(
                            subject = %subject,
                            "account_path: JetStream publish failed (timeout or drop)"
                        );
                    }
                    NATS_ERRORS_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                Err(e) => {
                    if is_wallet_tx_confirm {
                        error!(
                            error = %e,
                            subject = %subject,
                            "account_path: WalletTxConfirmed JetStream publish failed"
                        );
                    } else {
                        warn!(
                            error = %e,
                            subject = %subject,
                            "account_path: JetStream publish failed"
                        );
                    }
                    NATS_ERRORS_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }
        AccountPathNatsJob::CoreMarketEvent { event, trace } => {
            let _ =
                publish_market_event_core_and_momentum_ex(nats, event.as_ref(), trace, Some(host))
                    .await;
        }
        AccountPathNatsJob::CoreTopicJson { topic, payload } => {
            match nats.publish(&topic, &payload).await {
                Ok(true) => {
                    NATS_MESSAGES_PUBLISHED_TOTAL
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                Ok(false) => {
                    NATS_ERRORS_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                Err(e) => {
                    warn!(error = %e, topic = %topic, "account_path: core topic publish failed");
                    NATS_ERRORS_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }
    }
}

async fn md_account_publish_worker_loop(
    worker_id: usize,
    mut rx: mpsc::Receiver<AccountPathNatsJob>,
    host: Arc<dyn PublishHost>,
    cfg: NatsConfig,
) {
    let mut had_stall = false;
    'reconnect: loop {
        let mut nats = NatsClient::new(cfg.clone());
        if let Err(e) = nats.connect().await {
            warn!(
                worker_id,
                error = %e,
                "md-publish worker: NATS connect failed; retrying in 1s"
            );
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue 'reconnect;
        }
        if std::mem::take(&mut had_stall) {
            record_market_data_account_publish_worker_reconnect();
        }
        set_market_data_account_publish_worker_last_success_unix_ms(
            worker_id,
            wall_clock_unix_ms_now(),
        );
        loop {
            let Some(job) = rx.recv().await else {
                return;
            };
            let job_start = Instant::now();
            match tokio::time::timeout(
                MARKET_DATA_PUBLISH_WORKER_JOB_TIMEOUT,
                md_account_publish_execute_one_job(&nats, host.as_ref(), job),
            )
            .await
            {
                Ok(()) => {
                    let us = job_start.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
                    record_market_data_account_publish_worker_job_duration_us(us);
                    set_market_data_account_publish_worker_last_success_unix_ms(
                        worker_id,
                        wall_clock_unix_ms_now(),
                    );
                    dec_market_data_account_publish_queue_depth();
                }
                Err(_elapsed) => {
                    warn!(
                        worker_id,
                        timeout_secs = MARKET_DATA_PUBLISH_WORKER_JOB_TIMEOUT.as_secs(),
                        "md-publish worker: job wall timeout; rotating dedicated NATS client"
                    );
                    record_market_data_account_publish_worker_stall();
                    dec_market_data_account_publish_queue_depth();
                    had_stall = true;
                    continue 'reconnect;
                }
            }
        }
    }
}

async fn md_account_publish_dispatcher(
    mut main_rx: mpsc::Receiver<AccountPathNatsJob>,
    worker_txs: Vec<mpsc::Sender<AccountPathNatsJob>>,
) {
    if worker_txs.is_empty() {
        return;
    }
    let n = worker_txs.len();
    let mut rr = 0usize;
    while let Some(job) = main_rx.recv().await {
        let i = rr % n;
        rr = rr.wrapping_add(1);
        if worker_txs[i].send(job).await.is_err() {
            warn!(worker = i, "md-publish: worker dispatch queue closed");
            break;
        }
    }
}

async fn md_publish_runtime_main(
    main_rx: mpsc::Receiver<AccountPathNatsJob>,
    host: Arc<dyn PublishHost>,
    template: NatsConfig,
    worker_count: usize,
) {
    let mut worker_txs = Vec::with_capacity(worker_count);
    let mut handles = Vec::with_capacity(worker_count);
    for wid in 0..worker_count {
        let (wtx, wrx) = mpsc::channel(MARKET_DATA_ACCOUNT_PUBLISH_WORKER_DISPATCH_QUEUE_CAP);
        worker_txs.push(wtx);
        let mut cfg = template.clone();
        cfg.name = format!("market-data-publish-{wid}");
        let host_w = Arc::clone(&host);
        handles.push(tokio::spawn(md_account_publish_worker_loop(
            wid, wrx, host_w, cfg,
        )));
    }
    md_account_publish_dispatcher(main_rx, worker_txs).await;
    for h in handles {
        let _ = h.await;
    }
}

/// PR160: isolated publish thread + multi-worker Tokio (no NATS await on main Geyser runtime).
pub fn spawn_md_account_publish_runtime(
    host: Arc<dyn PublishHost>,
    template: NatsConfig,
    worker_count: usize,
) -> AccountPublishSender {
    let worker_count = worker_count.clamp(1, 32);
    let (tx, rx) = mpsc::channel(MARKET_DATA_ACCOUNT_PUBLISH_QUEUE_CAP);
    std::thread::Builder::new()
        .name("md-publish".to_string())
        .spawn(move || {
            let runtime_threads = (worker_count + 2).clamp(2, 32);
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(runtime_threads)
                .thread_name("md-publish")
                .enable_all()
                .build()
                .expect("md-publish tokio runtime");
            rt.block_on(md_publish_runtime_main(rx, host, template, worker_count));
        })
        .expect("spawn md-publish thread");
    tx
}

pub async fn account_path_enqueue_priority_fee_sample(
    publish_tx: Option<&AccountPublishSender>,
    nats: Option<&NatsClient>,
    fee_msg: &PriorityFeePercentiles,
) {
    let payload = match serde_json::to_value(fee_msg) {
        Ok(v) => v,
        Err(e) => {
            warn!(
                error = %e,
                "account_path: failed to serialize PriorityFeePercentiles for NATS"
            );
            return;
        }
    };
    if let Some(tx) = publish_tx {
        let job = AccountPathNatsJob::CoreTopicJson {
            topic: TOPIC_PRIORITY_FEE_SAMPLES.to_string(),
            payload,
        };
        let _ = try_enqueue_account_path_nats_job(tx, job, "PriorityFeePercentiles");
    } else if let Some(nats) = nats {
        if let Err(e) = nats.publish(TOPIC_PRIORITY_FEE_SAMPLES, fee_msg).await {
            tracing::debug!(error = %e, "Failed to publish priority fee percentiles");
        }
    }
}

pub async fn account_path_enqueue_jetstream<T: Serialize>(
    publish_tx: Option<&AccountPublishSender>,
    nats: Option<&NatsClient>,
    subject: String,
    payload: &T,
    log_fail: &'static str,
    bump_market_events_published_total: bool,
) -> bool {
    if let Some(tx) = publish_tx {
        let payload = match serde_json::to_value(payload) {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    error = %e,
                    msg = log_fail,
                    "account_path: failed to serialize JetStream payload"
                );
                return false;
            }
        };
        let job = AccountPathNatsJob::JetStream {
            subject,
            payload,
            bump_market_events_published_total,
        };
        try_enqueue_account_path_nats_job(tx, job, log_fail)
    } else if let Some(nats) = nats {
        match nats.jetstream_publish(&subject, payload).await {
            Ok(true) => {
                NATS_MESSAGES_PUBLISHED_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if bump_market_events_published_total {
                    MARKET_EVENTS_PUBLISHED_TOTAL
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                true
            }
            Ok(false) => {
                warn!(
                    subject = %subject,
                    msg = log_fail,
                    "JetStream publish failed (timeout or drop)"
                );
                NATS_ERRORS_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                false
            }
            Err(e) => {
                warn!(error = %e, subject = %subject, msg = log_fail, "JetStream publish failed");
                NATS_ERRORS_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                false
            }
        }
    } else {
        false
    }
}

pub async fn account_path_enqueue_core_market_event(
    publish_tx: Option<&AccountPublishSender>,
    nats: Option<&NatsClient>,
    host: Option<&dyn PublishHost>,
    event: MarketEvent,
    trace: Option<MarketEventCorePublishTrace>,
) -> bool {
    if !market_event_should_nats_core(&event.kind) {
        return false;
    }
    if let Some(tx) = publish_tx {
        let event_id = event.event_id.clone();
        let job = AccountPathNatsJob::CoreMarketEvent {
            event: Box::new(event),
            trace,
        };
        let ok = try_enqueue_account_path_nats_job(tx, job, "core MarketEvent");
        if !ok {
            warn!(event_id = %event_id, "account_path: core MarketEvent enqueue failed");
        }
        ok
    } else if let Some(nats) = nats {
        let _ = publish_market_event_core_and_momentum_ex(nats, &event, trace, host).await;
        true
    } else {
        false
    }
}
