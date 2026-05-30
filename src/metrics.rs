use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Recent trade record for dashboard display
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct RecentTrade {
    pub timestamp_ms: u64,
    pub mint: String,
    pub action: String, // "BUY" or "SELL"
    pub tx_hash: String,
    pub amount_tokens: f64,
    #[serde(alias = "price_sol")]
    pub value_sol: f64,
    pub pnl_sol: Option<f64>,    // Only for SELL
    pub pnl_pct: Option<f64>,    // Only for SELL
    pub latency_ms: Option<u64>, // Discovery to TX landed
}

/// Ring buffer for last N trades
const MAX_RECENT_TRADES: usize = 20;
pub static RECENT_TRADES: Lazy<RwLock<VecDeque<RecentTrade>>> =
    Lazy::new(|| RwLock::new(VecDeque::with_capacity(MAX_RECENT_TRADES)));

/// Record a new trade (BUY or SELL) - persists to both in-memory buffer and JSONL file
pub fn record_recent_trade(trade: RecentTrade) {
    // Persist to JSONL file for recovery after restarts
    append_trade_to_jsonl(&trade);

    // Add to in-memory ring buffer
    let mut trades = RECENT_TRADES.write();
    if trades.len() >= MAX_RECENT_TRADES {
        trades.pop_front();
    }
    trades.push_back(trade);
}

/// Append a trade to today's JSONL file for persistence
fn append_trade_to_jsonl(trade: &RecentTrade) {
    use chrono::Utc;
    use std::io::Write;

    let dir_name =
        std::env::var("IRONCRAB_TRADE_LOG_DIR").unwrap_or_else(|_| "trade_logs".to_string());
    let date = Utc::now().format("%Y%m%d");
    let file_path = std::path::Path::new(&dir_name).join(format!("recent_trades-{}.jsonl", date));

    // Ensure directory exists
    if let Some(parent) = file_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }

    // Append to file (create if not exists)
    if let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&file_path)
    {
        if let Ok(json) = serde_json::to_string(trade) {
            let _ = writeln!(file, "{}", json);
        }
    }
}

/// Get recent trades as JSON - reads from JSONL/CSV file for persistence across restarts
pub fn get_recent_trades_json() -> String {
    // Priority 1: Try to read from today's JSONL file (new format)
    let trades = read_trades_from_jsonl(20);
    if !trades.is_empty() {
        return serde_json::to_string(&trades).unwrap_or_else(|_| "[]".to_string());
    }

    // Priority 2: Try to read from today's CSV file (legacy format)
    let csv_trades = read_trades_from_csv(20);
    if !csv_trades.is_empty() {
        return serde_json::to_string(&csv_trades).unwrap_or_else(|_| "[]".to_string());
    }

    // Fallback: in-memory buffer (empty after restart until first trade)
    let mem_trades = RECENT_TRADES.read();
    serde_json::to_string(&*mem_trades).unwrap_or_else(|_| "[]".to_string())
}

/// Read last N trades from today's JSONL file
fn read_trades_from_jsonl(limit: usize) -> Vec<RecentTrade> {
    use chrono::Utc;
    use std::io::BufRead;

    let dir_name =
        std::env::var("IRONCRAB_TRADE_LOG_DIR").unwrap_or_else(|_| "trade_logs".to_string());
    let date = Utc::now().format("%Y%m%d");
    let file_path = std::path::Path::new(&dir_name).join(format!("recent_trades-{}.jsonl", date));

    let file = match std::fs::File::open(&file_path) {
        Ok(f) => f,
        Err(_) => return Vec::new(),
    };

    let reader = std::io::BufReader::new(file);
    let lines: Vec<String> = reader.lines().map_while(Result::ok).collect();

    // Take last N lines, reverse to show newest first
    let start = lines.len().saturating_sub(limit);
    lines[start..]
        .iter()
        .rev()
        .filter_map(|line| serde_json::from_str::<RecentTrade>(line).ok())
        .collect()
}

/// Read last N trades from today's CSV file
fn read_trades_from_csv(limit: usize) -> Vec<RecentTrade> {
    use chrono::Utc;
    use std::io::BufRead;

    let dir_name =
        std::env::var("IRONCRAB_TRADE_LOG_DIR").unwrap_or_else(|_| "trade_logs".to_string());
    let date = Utc::now().format("%Y%m%d");
    let file_path = std::path::Path::new(&dir_name).join(format!("trades-{}.csv", date));

    let file = match std::fs::File::open(&file_path) {
        Ok(f) => f,
        Err(_) => return Vec::new(),
    };

    let reader = std::io::BufReader::new(file);
    let lines: Vec<String> = reader.lines().map_while(Result::ok).collect();

    // Skip header, take last N lines, reverse to show newest first
    let data_lines: Vec<&String> = lines.iter().skip(1).collect();
    let start = data_lines.len().saturating_sub(limit);

    data_lines[start..]
        .iter()
        .rev()
        .filter_map(|line| parse_csv_line(line))
        .collect()
}

/// Parse a CSV line into RecentTrade
fn parse_csv_line(line: &str) -> Option<RecentTrade> {
    // CSV format (sniper):
    // timestamp_utc,side,mint,dex,signature,lamports_in,lamports_out,tokens_in,tokens_out,
    // expected_tokens_out,expected_sol_out,shortfall_tokens,shortfall_sol,network_fee_lamports,
    // realized_pnl_sol,notes
    let parts: Vec<&str> = line.split(',').collect();
    if parts.len() < 10 {
        return None;
    }

    let timestamp_str = parts.first()?;
    let timestamp_ms = chrono::DateTime::parse_from_rfc3339(timestamp_str)
        .ok()?
        .timestamp_millis() as u64;

    let action = parts.get(1)?.to_string();
    let mint = parts.get(2)?.to_string();
    let tx_hash = parts.get(4).unwrap_or(&"").to_string();
    let lamports_in: u64 = parts.get(5).and_then(|s| s.parse().ok()).unwrap_or(0);
    let lamports_out: u64 = parts.get(6).and_then(|s| s.parse().ok()).unwrap_or(0);
    let tokens_in: f64 = parts.get(7).and_then(|s| s.parse().ok()).unwrap_or(0.0);
    let tokens_out: f64 = parts.get(8).and_then(|s| s.parse().ok()).unwrap_or(0.0);
    // Fallback to expected_tokens_out (index 9) for old CSV format compatibility
    let expected_tokens_out: f64 = parts.get(9).and_then(|s| s.parse().ok()).unwrap_or(0.0);

    // For BUY/FILL: lamports_in is spent, tokens_out received (use expected as fallback)
    // For SELL: tokens_in sold, lamports_out received
    let (amount_tokens, value_sol) = if action == "BUY" || action == "FILL" {
        let tokens = if tokens_out > 0.0 {
            tokens_out
        } else {
            expected_tokens_out
        };
        let sol = lamports_in as f64 / 1e9;
        let price = if tokens > 0.0 { sol / tokens } else { 0.0 };
        (tokens, price)
    } else {
        let tokens = tokens_in;
        let sol = lamports_out as f64 / 1e9;
        let price = if tokens > 0.0 { sol / tokens } else { 0.0 };
        (tokens, price)
    };

    // PnL from realized_pnl_sol column (index 14)
    let pnl_sol = if action == "SELL" {
        parts.get(14).and_then(|s| s.parse::<f64>().ok())
    } else {
        None
    };

    Some(RecentTrade {
        timestamp_ms,
        mint,
        action,
        tx_hash,
        amount_tokens,
        value_sol,
        pnl_sol,
        pnl_pct: None, // Not stored in CSV
        latency_ms: None,
    })
}

// =============================================================================
// Multi-Process Architecture Metrics (market-data, momentum-bot, execution-engine)
// =============================================================================

// --- market-data service metrics ---
pub static MARKET_EVENTS_RECEIVED_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static MARKET_EVENTS_PUBLISHED_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
/// Successful publishes to `ironcrab.v1.market_events.momentum` (market-data only).
pub static MARKET_EVENTS_MOMENTUM_FANOUT_PUBLISHED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static POOLS_DISCOVERED_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static POOLS_TRACKED_GAUGE: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static TOKENS_TRACKED_GAUGE: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
/// Reconnect attempts after `stream ended` (same gRPC client, resubscribe).
pub static GEYSER_RECONNECT_TOTAL_STREAM_ENDED: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
/// Reconnect after transport/stream error (new `connect()`).
pub static GEYSER_RECONNECT_TOTAL_STREAM_ERROR: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
/// Reconnect after subscription sink closed / send failure (new `connect()`).
pub static GEYSER_RECONNECT_TOTAL_SINK_GONE: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
/// gRPC stream payloads delivered into the listener (account / transaction / block_meta updates).
pub static GEYSER_LISTENER_STREAM_MESSAGES_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
/// gRPC stream `Err` deliveries (excludes graceful `None` / stream ended).
pub static GEYSER_STREAM_ERRORS_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
/// `CompressedAccountFilterSet::insert` failed: capacity below configured explicit-track ceiling.
pub static GEYSER_TRACKED_CUCKOO_TABLE_FULL_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// 1 while a Geyser gRPC connection is established; 0 while reconnecting.
/// PR164: `1` only when **both** TX and account Geyser sessions are connected.
pub static GEYSER_CONNECTED: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
/// PR164: TX-only Geyser gRPC session (transactions + blocks_meta).
pub static GEYSER_TX_SESSION_CONNECTED: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
/// PR164: Account-only Geyser gRPC session (owner filters + cuckoo pins; no transaction filters).
pub static GEYSER_ACCOUNT_SESSION_CONNECTED: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

/// PR164: `UpdateOneof::Transaction` received on the TX-only session.
pub static GEYSER_TX_LISTENER_TRANSACTIONS_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static GEYSER_TX_LISTENER_PAYLOAD_BROADCAST_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// PR164: Account updates received on the account-only session.
pub static GEYSER_ACCOUNT_LISTENER_ACCOUNT_UPDATES_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// PR164: In-place subscribe updates after initial connect (must stay **0** on TX session).
pub static GEYSER_TX_LISTENER_SUBSCRIBE_UPDATES_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// PR164: In-place subscribe updates (cuckoo / pin rebuild) on account session.
pub static GEYSER_ACCOUNT_LISTENER_SUBSCRIBE_UPDATES_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// PR164: Forced outer reconnect of TX session because transaction ingest stalled while chain advanced.
pub static GEYSER_TX_LISTENER_LIVENESS_RECONNECTS_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// Combined explicit Geyser account subscription size (mints + vaults + bin arrays + wallet).
pub static GEYSER_SUBSCRIPTION_ACCOUNTS: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

/// Pinned explicit accounts (never LRU-evicted).
pub static GEYSER_TRACKED_PINNED_ACCOUNTS: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

pub static GEYSER_TRACKED_ACCOUNTS_EVICTED_VAULT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static GEYSER_TRACKED_ACCOUNTS_EVICTED_BIN_ARRAY: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static GEYSER_TRACKED_ACCOUNTS_EVICTED_MINT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

/// momentum-bot published `MomentumActivePoolsUpdate` payloads (PR-D).
pub static MOMENTUM_ACTIVE_POOLS_MESSAGES_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
/// market-data applied `MomentumActivePoolsUpdate` from core NATS (PR-D).
pub static MARKET_DATA_MOMENTUM_ACTIVE_POOL_MESSAGES_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Count of `(mint, pool)` entries in market-data `ActivePoolSet` (PR-D gauge).
pub static MARKET_DATA_MOMENTUM_ACTIVE_POOL_PINS_GAUGE: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// NATS momentum updates absorbed by the pre-actor coalescer (PR169c).
pub static MARKET_DATA_MOMENTUM_COALESCED_MESSAGES_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Merged `ApplyMomentumActivePools` batches enqueued to the tracking actor (PR169c).
pub static MARKET_DATA_MOMENTUM_COALESCED_BATCHES_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// Geyser explicit-tracked subscription list syncs coalesced from the TX trade path (debounced flush).
pub static MARKET_DATA_GEYSER_SYNC_BATCH_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
/// Immediate `sync_geyser_tracked_accounts` (momentum pins, wallet tracks, config, mint metadata, etc.).
pub static MARKET_DATA_GEYSER_SYNC_IMMEDIATE_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// 1 while a debounced TX-path Geyser sync is scheduled and not yet flushed; 0 otherwise.
pub static MARKET_DATA_GEYSER_SYNC_PENDING: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

/// PR161: merge-task coalesced flush to `combined_tracked` (timer fired after `geyser_sync_batch_ms` quiet window).
pub static MARKET_DATA_GEYSER_MERGE_COALESCED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// PR161: optional urgent merge path (reserved; default coalesce-only).
pub static MARKET_DATA_GEYSER_MERGE_IMMEDIATE_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// PR161: 1 while a debounced merge flush is scheduled; 0 otherwise.
pub static MARKET_DATA_GEYSER_MERGE_PENDING: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

/// Reconnect after large explicit subscription set (PR-B: full client reconnect vs in-place churn).
pub static GEYSER_RECONNECT_TOTAL_SUBSCRIPTION_REBUILD: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// Geyser listener reconnect reason (PR-A stream resiliency).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GeyserReconnectReason {
    StreamEnded,
    StreamError,
    SinkGone,
    /// PR-B: intentional full reconnect for large `combined_tracked` updates.
    SubscriptionRebuild,
}

pub fn geyser_metrics_inc_reconnect(reason: GeyserReconnectReason) {
    match reason {
        GeyserReconnectReason::StreamEnded => {
            GEYSER_RECONNECT_TOTAL_STREAM_ENDED.fetch_add(1, Ordering::Relaxed);
        }
        GeyserReconnectReason::StreamError => {
            GEYSER_RECONNECT_TOTAL_STREAM_ERROR.fetch_add(1, Ordering::Relaxed);
        }
        GeyserReconnectReason::SinkGone => {
            GEYSER_RECONNECT_TOTAL_SINK_GONE.fetch_add(1, Ordering::Relaxed);
        }
        GeyserReconnectReason::SubscriptionRebuild => {
            GEYSER_RECONNECT_TOTAL_SUBSCRIPTION_REBUILD.fetch_add(1, Ordering::Relaxed);
        }
    }
}

pub fn geyser_metrics_set_subscription_accounts(n: usize) {
    GEYSER_SUBSCRIPTION_ACCOUNTS.store(n as u64, Ordering::Relaxed);
}

pub fn geyser_metrics_set_tracked_pinned_accounts(n: usize) {
    GEYSER_TRACKED_PINNED_ACCOUNTS.store(n as u64, Ordering::Relaxed);
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GeyserTrackedEvictKind {
    Vault,
    BinArray,
    Mint,
}

pub fn geyser_metrics_inc_tracked_evicted(kind: GeyserTrackedEvictKind) {
    match kind {
        GeyserTrackedEvictKind::Vault => {
            GEYSER_TRACKED_ACCOUNTS_EVICTED_VAULT.fetch_add(1, Ordering::Relaxed);
        }
        GeyserTrackedEvictKind::BinArray => {
            GEYSER_TRACKED_ACCOUNTS_EVICTED_BIN_ARRAY.fetch_add(1, Ordering::Relaxed);
        }
        GeyserTrackedEvictKind::Mint => {
            GEYSER_TRACKED_ACCOUNTS_EVICTED_MINT.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[inline]
pub fn record_momentum_active_pools_messages_total() {
    MOMENTUM_ACTIVE_POOLS_MESSAGES_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn record_market_data_momentum_active_pool_messages_total() {
    MARKET_DATA_MOMENTUM_ACTIVE_POOL_MESSAGES_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn set_market_data_momentum_active_pool_pins_gauge(n: usize) {
    MARKET_DATA_MOMENTUM_ACTIVE_POOL_PINS_GAUGE.store(n as u64, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_momentum_coalesced_messages_total() {
    MARKET_DATA_MOMENTUM_COALESCED_MESSAGES_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_momentum_coalesced_batches_total() {
    MARKET_DATA_MOMENTUM_COALESCED_BATCHES_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn record_market_data_geyser_sync_batch_total() {
    MARKET_DATA_GEYSER_SYNC_BATCH_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn record_market_data_geyser_sync_immediate_total() {
    MARKET_DATA_GEYSER_SYNC_IMMEDIATE_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn set_market_data_geyser_sync_pending(pending: u64) {
    MARKET_DATA_GEYSER_SYNC_PENDING.store(pending, Ordering::Relaxed);
}

#[inline]
pub fn record_market_data_geyser_merge_coalesced_total() {
    MARKET_DATA_GEYSER_MERGE_COALESCED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Reserved for an optional urgent merge path (PR161); coalesce-only mode does not increment this.
#[inline]
#[allow(dead_code)]
pub fn record_market_data_geyser_merge_immediate_total() {
    MARKET_DATA_GEYSER_MERGE_IMMEDIATE_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn set_market_data_geyser_merge_pending(pending: u64) {
    MARKET_DATA_GEYSER_MERGE_PENDING.store(pending, Ordering::Relaxed);
}

pub fn geyser_metrics_inc_stream_error() {
    GEYSER_STREAM_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn geyser_metrics_inc_listener_stream_payload() {
    GEYSER_LISTENER_STREAM_MESSAGES_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn geyser_metrics_inc_tracked_cuckoo_table_full() {
    GEYSER_TRACKED_CUCKOO_TABLE_FULL_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
fn geyser_metrics_refresh_aggregate_connected() {
    let both = GEYSER_TX_SESSION_CONNECTED.load(Ordering::Relaxed) == 1
        && GEYSER_ACCOUNT_SESSION_CONNECTED.load(Ordering::Relaxed) == 1;
    GEYSER_CONNECTED.store(if both { 1 } else { 0 }, Ordering::Relaxed);
}

/// PR164: TX Geyser session connected state (also refreshes aggregate `geyser_connected`).
pub fn geyser_metrics_set_tx_session_connected(connected: bool) {
    GEYSER_TX_SESSION_CONNECTED.store(if connected { 1 } else { 0 }, Ordering::Relaxed);
    geyser_metrics_refresh_aggregate_connected();
}

/// PR164: Account Geyser session connected state (also refreshes aggregate `geyser_connected`).
pub fn geyser_metrics_set_account_session_connected(connected: bool) {
    GEYSER_ACCOUNT_SESSION_CONNECTED.store(if connected { 1 } else { 0 }, Ordering::Relaxed);
    geyser_metrics_refresh_aggregate_connected();
}

#[inline]
pub fn geyser_metrics_inc_tx_listener_transactions_total() {
    GEYSER_TX_LISTENER_TRANSACTIONS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn geyser_metrics_inc_tx_listener_payload_broadcast_total() {
    GEYSER_TX_LISTENER_PAYLOAD_BROADCAST_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// PR166: `handle_geyser_transaction` entered (TX ingest progress; liveness / stall detection).
pub static MARKET_DATA_TX_HANDLER_PROCESSED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MARKET_DATA_TX_HANDLER_LAST_PROGRESS_UNIX_MS: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MARKET_DATA_TX_HANDLER_STALLS_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static MARKET_DATA_UNPARSED_TX_DROPPED_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static MARKET_DATA_UNPARSED_ACCOUNT_DROPPED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
static MARKET_DATA_TX_HANDLER_RECONNECT_REQUESTED: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
static MARKET_DATA_ACCOUNT_SESSION_RECONNECT_REQUESTED: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// PR167: all three ingest progress signals flat (TX handler, account listener, head slot).
pub static MARKET_DATA_GLOBAL_INGEST_STALLS_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MARKET_DATA_GLOBAL_INGEST_LAST_PROGRESS_UNIX_MS: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// PR167: subscription sink rate-limit skips (coalesced bursts during startup).
pub static GEYSER_ACCOUNT_LISTENER_SUBSCRIBE_SINK_THROTTLED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// PR167: subscription sink send timeout / backpressure → full reconnect.
pub static GEYSER_ACCOUNT_LISTENER_SUBSCRIBE_SINK_BACKPRESSURE_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Phase-R-R3: Yellowstone subscription sink `send` exceeded timeout (I-4b).
pub static MARKET_DATA_GEYSER_SUBSCRIPTION_SEND_TIMEOUT_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// PR167: account session reconnect requested by global ingest stall recovery.
pub static GEYSER_ACCOUNT_LISTENER_LIVENESS_RECONNECTS_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// PR167: deferred TX side-effect queue full (`try_send` drop).
pub static MARKET_DATA_TX_DEFERRED_DROPPED_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

/// PR169a: single-writer Geyser tracking actor queue depth (gauge).
pub static MARKET_DATA_GEYSER_TRACKING_QUEUE_DEPTH: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// PR169a: Geyser tracking actor queue full (`try_send` drop).
pub static MARKET_DATA_GEYSER_TRACKING_ENQUEUE_DROPPED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// PR169a: jobs processed by the Geyser tracking actor.
pub static MARKET_DATA_GEYSER_TRACKING_JOBS_PROCESSED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// Phase-R-R4: deferred side-effects queue depth (`md-sidefx` OS thread).
pub static MARKET_DATA_MD_SIDEFX_QUEUE_DEPTH: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
/// Phase-R-R4: `md-sidefx` queue full (`try_send` drop).
pub static MARKET_DATA_MD_SIDEFX_ENQUEUE_DROPPED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Phase-R-R4: jobs processed by the `md-sidefx` worker.
pub static MARKET_DATA_MD_SIDEFX_JOBS_PROCESSED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

#[inline]
pub fn record_market_data_tx_handler_processed() {
    MARKET_DATA_TX_HANDLER_PROCESSED_TOTAL.fetch_add(1, Ordering::Relaxed);
    MARKET_DATA_TX_HANDLER_LAST_PROGRESS_UNIX_MS.store(wall_clock_unix_ms_now(), Ordering::Relaxed);
    record_market_data_tokio_progress();
}

#[inline]
pub fn market_data_tx_handler_processed_value() -> u64 {
    MARKET_DATA_TX_HANDLER_PROCESSED_TOTAL.load(Ordering::Relaxed)
}

#[inline]
pub fn record_market_data_tx_handler_stall() {
    MARKET_DATA_TX_HANDLER_STALLS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_unparsed_tx_dropped_total() {
    MARKET_DATA_UNPARSED_TX_DROPPED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_unparsed_account_dropped_total() {
    MARKET_DATA_UNPARSED_ACCOUNT_DROPPED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn market_data_request_tx_session_reconnect() {
    MARKET_DATA_TX_HANDLER_RECONNECT_REQUESTED.store(1, Ordering::Relaxed);
}

#[inline]
pub fn market_data_take_tx_session_reconnect_request() -> bool {
    MARKET_DATA_TX_HANDLER_RECONNECT_REQUESTED
        .compare_exchange(1, 0, Ordering::Relaxed, Ordering::Relaxed)
        .is_ok()
}

#[inline]
pub fn market_data_request_account_session_reconnect() {
    MARKET_DATA_ACCOUNT_SESSION_RECONNECT_REQUESTED.store(1, Ordering::Relaxed);
}

#[inline]
pub fn market_data_take_account_session_reconnect_request() -> bool {
    MARKET_DATA_ACCOUNT_SESSION_RECONNECT_REQUESTED
        .compare_exchange(1, 0, Ordering::Relaxed, Ordering::Relaxed)
        .is_ok()
}

#[inline]
pub fn geyser_account_listener_account_updates_value() -> u64 {
    GEYSER_ACCOUNT_LISTENER_ACCOUNT_UPDATES_TOTAL.load(Ordering::Relaxed)
}

#[inline]
pub fn record_market_data_global_ingest_stall() {
    MARKET_DATA_GLOBAL_INGEST_STALLS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn touch_market_data_global_ingest_progress() {
    MARKET_DATA_GLOBAL_INGEST_LAST_PROGRESS_UNIX_MS
        .store(wall_clock_unix_ms_now(), Ordering::Relaxed);
}

#[inline]
pub fn geyser_metrics_inc_account_listener_subscribe_sink_throttled() {
    GEYSER_ACCOUNT_LISTENER_SUBSCRIBE_SINK_THROTTLED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn geyser_metrics_inc_account_listener_subscribe_sink_backpressure() {
    GEYSER_ACCOUNT_LISTENER_SUBSCRIBE_SINK_BACKPRESSURE_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn geyser_metrics_inc_subscription_send_timeout_total() {
    MARKET_DATA_GEYSER_SUBSCRIPTION_SEND_TIMEOUT_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn geyser_metrics_inc_account_listener_liveness_reconnect_total() {
    GEYSER_ACCOUNT_LISTENER_LIVENESS_RECONNECTS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_tx_deferred_dropped_total() {
    MARKET_DATA_TX_DEFERRED_DROPPED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn set_market_data_geyser_tracking_queue_depth(depth: usize) {
    MARKET_DATA_GEYSER_TRACKING_QUEUE_DEPTH.store(depth as u64, Ordering::Relaxed);
}

/// Phase-R-R2: alias of [`set_market_data_geyser_tracking_queue_depth`] (md-state channel depth).
#[inline]
pub fn set_market_data_md_state_queue_depth(depth: usize) {
    set_market_data_geyser_tracking_queue_depth(depth);
}

#[inline]
pub fn inc_market_data_geyser_tracking_enqueue_dropped_total() {
    MARKET_DATA_GEYSER_TRACKING_ENQUEUE_DROPPED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_geyser_tracking_jobs_processed_total() {
    MARKET_DATA_GEYSER_TRACKING_JOBS_PROCESSED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn set_market_data_md_sidefx_queue_depth(depth: usize) {
    MARKET_DATA_MD_SIDEFX_QUEUE_DEPTH.store(depth as u64, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_md_sidefx_enqueue_dropped_total() {
    MARKET_DATA_MD_SIDEFX_ENQUEUE_DROPPED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_md_sidefx_jobs_processed_total() {
    MARKET_DATA_MD_SIDEFX_JOBS_PROCESSED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn geyser_metrics_inc_account_listener_account_updates_total() {
    GEYSER_ACCOUNT_LISTENER_ACCOUNT_UPDATES_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn geyser_metrics_inc_account_listener_subscribe_updates_total() {
    GEYSER_ACCOUNT_LISTENER_SUBSCRIBE_UPDATES_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn geyser_metrics_inc_tx_listener_liveness_reconnect_total() {
    GEYSER_TX_LISTENER_LIVENESS_RECONNECTS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Monotonic Geyser head slot (read-only). Used by TX liveness gate (PR164).
#[inline]
pub fn market_data_geyser_head_slot_value() -> u64 {
    MARKET_DATA_GEYSER_HEAD_SLOT.load(Ordering::Relaxed)
}

// --- market-data: Geyser recv → Core NATS publish (hot path observability, no RPC) ---
/// Monotonic Geyser chain head (max slot seen on market-data ingest paths). I-16: not RPC `getSlot`.
pub static MARKET_DATA_GEYSER_HEAD_SLOT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static MARKET_DATA_LAST_TRADE_PUBLISH_TS_UNIX_MS: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MARKET_DATA_LAST_BONDING_CURVE_PUBLISH_TS_UNIX_MS: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

const MARKET_DATA_GEYSER_TO_PUBLISH_MS_BUCKETS: &[u64] = &[
    1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2000, 5000, 10_000, 30_000, 60_000,
];
const MARKET_DATA_LATENCY_MS_SUM_CAP: u64 = 600_000;

const MARKET_DATA_SLOT_LAG_AT_PUBLISH_BUCKETS: &[u64] = &[0, 1, 2, 3, 5, 10, 20, 50, 100, 200];

/// Segment for `market_data_geyser_to_publish_ms_*` / slot lag histogram families (no dynamic labels).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MarketDataLatencySegment {
    Trade,
    BondingCurve,
    PoolCreated,
    Other,
}

static MARKET_DATA_GEYSER_TO_PUBLISH_MS_TRADE_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> =
    Lazy::new(|| {
        MARKET_DATA_GEYSER_TO_PUBLISH_MS_BUCKETS
            .iter()
            .map(|_| AtomicU64::new(0))
            .collect()
    });
static MARKET_DATA_GEYSER_TO_PUBLISH_MS_TRADE_SUM: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
static MARKET_DATA_GEYSER_TO_PUBLISH_MS_TRADE_COUNT: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

static MARKET_DATA_GEYSER_TO_PUBLISH_MS_BONDING_CURVE_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> =
    Lazy::new(|| {
        MARKET_DATA_GEYSER_TO_PUBLISH_MS_BUCKETS
            .iter()
            .map(|_| AtomicU64::new(0))
            .collect()
    });
static MARKET_DATA_GEYSER_TO_PUBLISH_MS_BONDING_CURVE_SUM: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
static MARKET_DATA_GEYSER_TO_PUBLISH_MS_BONDING_CURVE_COUNT: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

static MARKET_DATA_GEYSER_TO_PUBLISH_MS_POOL_CREATED_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> =
    Lazy::new(|| {
        MARKET_DATA_GEYSER_TO_PUBLISH_MS_BUCKETS
            .iter()
            .map(|_| AtomicU64::new(0))
            .collect()
    });
static MARKET_DATA_GEYSER_TO_PUBLISH_MS_POOL_CREATED_SUM: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
static MARKET_DATA_GEYSER_TO_PUBLISH_MS_POOL_CREATED_COUNT: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

static MARKET_DATA_GEYSER_TO_PUBLISH_MS_OTHER_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> =
    Lazy::new(|| {
        MARKET_DATA_GEYSER_TO_PUBLISH_MS_BUCKETS
            .iter()
            .map(|_| AtomicU64::new(0))
            .collect()
    });
static MARKET_DATA_GEYSER_TO_PUBLISH_MS_OTHER_SUM: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
static MARKET_DATA_GEYSER_TO_PUBLISH_MS_OTHER_COUNT: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

static MARKET_DATA_SLOT_LAG_AT_PUBLISH_SLOTS_TRADE_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> =
    Lazy::new(|| {
        MARKET_DATA_SLOT_LAG_AT_PUBLISH_BUCKETS
            .iter()
            .map(|_| AtomicU64::new(0))
            .collect()
    });
static MARKET_DATA_SLOT_LAG_AT_PUBLISH_SLOTS_TRADE_SUM: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
static MARKET_DATA_SLOT_LAG_AT_PUBLISH_SLOTS_TRADE_COUNT: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

static MARKET_DATA_SLOT_LAG_AT_PUBLISH_SLOTS_BONDING_CURVE_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> =
    Lazy::new(|| {
        MARKET_DATA_SLOT_LAG_AT_PUBLISH_BUCKETS
            .iter()
            .map(|_| AtomicU64::new(0))
            .collect()
    });
static MARKET_DATA_SLOT_LAG_AT_PUBLISH_SLOTS_BONDING_CURVE_SUM: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
static MARKET_DATA_SLOT_LAG_AT_PUBLISH_SLOTS_BONDING_CURVE_COUNT: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

static MARKET_DATA_SLOT_LAG_AT_PUBLISH_SLOTS_POOL_CREATED_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> =
    Lazy::new(|| {
        MARKET_DATA_SLOT_LAG_AT_PUBLISH_BUCKETS
            .iter()
            .map(|_| AtomicU64::new(0))
            .collect()
    });
static MARKET_DATA_SLOT_LAG_AT_PUBLISH_SLOTS_POOL_CREATED_SUM: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
static MARKET_DATA_SLOT_LAG_AT_PUBLISH_SLOTS_POOL_CREATED_COUNT: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

static MARKET_DATA_SLOT_LAG_AT_PUBLISH_SLOTS_OTHER_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> =
    Lazy::new(|| {
        MARKET_DATA_SLOT_LAG_AT_PUBLISH_BUCKETS
            .iter()
            .map(|_| AtomicU64::new(0))
            .collect()
    });
static MARKET_DATA_SLOT_LAG_AT_PUBLISH_SLOTS_OTHER_SUM: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
static MARKET_DATA_SLOT_LAG_AT_PUBLISH_SLOTS_OTHER_COUNT: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// Pump.fun: wall ms between last `BondingCurveProgress` publish and next `Trade` publish (same bonding curve / pool).
static MARKET_DATA_TRADE_AFTER_BONDING_PUBLISH_MS_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> =
    Lazy::new(|| {
        MARKET_DATA_GEYSER_TO_PUBLISH_MS_BUCKETS
            .iter()
            .map(|_| AtomicU64::new(0))
            .collect()
    });
static MARKET_DATA_TRADE_AFTER_BONDING_PUBLISH_MS_SUM: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
static MARKET_DATA_TRADE_AFTER_BONDING_PUBLISH_MS_COUNT: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// Geyser listener `broadcast::send` → market-data `recv()` (queue + scheduling), milliseconds.
static MARKET_DATA_TX_CHANNEL_LAG_MS_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> = Lazy::new(|| {
    MARKET_DATA_GEYSER_TO_PUBLISH_MS_BUCKETS
        .iter()
        .map(|_| AtomicU64::new(0))
        .collect()
});
static MARKET_DATA_TX_CHANNEL_LAG_MS_SUM: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
static MARKET_DATA_TX_CHANNEL_LAG_MS_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

static MARKET_DATA_ACCOUNT_CHANNEL_LAG_MS_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> = Lazy::new(|| {
    MARKET_DATA_GEYSER_TO_PUBLISH_MS_BUCKETS
        .iter()
        .map(|_| AtomicU64::new(0))
        .collect()
});
static MARKET_DATA_ACCOUNT_CHANNEL_LAG_MS_SUM: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
static MARKET_DATA_ACCOUNT_CHANNEL_LAG_MS_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

/// Pump.fun: wall ms from Geyser `grpc_recv_at` on the TX update to `DevWalletIdentified` publish (TX fast-path after `pool_mint_map` insert).
static MARKET_DATA_POOL_MINT_MAP_TO_DEVWALLET_MS_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> =
    Lazy::new(|| {
        MARKET_DATA_GEYSER_TO_PUBLISH_MS_BUCKETS
            .iter()
            .map(|_| AtomicU64::new(0))
            .collect()
    });
static MARKET_DATA_POOL_MINT_MAP_TO_DEVWALLET_MS_SUM: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
static MARKET_DATA_POOL_MINT_MAP_TO_DEVWALLET_MS_COUNT: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// Pump.fun: wall ms from bonding-curve account `grpc_recv_at` to `DevWalletIdentified` publish (account path).
static MARKET_DATA_BONDING_CURVE_GRPC_TO_DEVWALLET_MS_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> =
    Lazy::new(|| {
        MARKET_DATA_GEYSER_TO_PUBLISH_MS_BUCKETS
            .iter()
            .map(|_| AtomicU64::new(0))
            .collect()
    });
static MARKET_DATA_BONDING_CURVE_GRPC_TO_DEVWALLET_MS_SUM: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
static MARKET_DATA_BONDING_CURVE_GRPC_TO_DEVWALLET_MS_COUNT: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// Pump.fun: slot delta from last published `BondingCurveProgress` (Geyser slot) to successful `Trade` publish (same pool/bonding curve).
static MARKET_DATA_BONDING_TO_TRADE_SLOT_DELTA_SLOTS_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> =
    Lazy::new(|| {
        MARKET_DATA_SLOT_LAG_AT_PUBLISH_BUCKETS
            .iter()
            .map(|_| AtomicU64::new(0))
            .collect()
    });
static MARKET_DATA_BONDING_TO_TRADE_SLOT_DELTA_SLOTS_SUM: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
static MARKET_DATA_BONDING_TO_TRADE_SLOT_DELTA_SLOTS_COUNT: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// `broadcast::RecvError::Lagged(n)` — skipped messages (cumulative `n` added per occurrence).
pub static MARKET_DATA_TX_BROADCAST_LAGGED_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

/// Remaining messages in the Geyser→market-data `broadcast` buffer after each successful `recv`
/// (ingest task only). Under healthy fairness this should stay near 0.
pub static MARKET_DATA_TX_BROADCAST_QUEUE_DEPTH: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
/// Remaining account updates in the Geyser→market-data `broadcast` buffer after each successful `recv`.
pub static MARKET_DATA_ACCOUNT_BROADCAST_QUEUE_DEPTH: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MARKET_DATA_ACCOUNT_BROADCAST_LAGGED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// Configured account worker pool size (Phase-R-R4b; const export for ops).
pub static MARKET_DATA_ACCOUNT_WORKER_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

/// Account ingest: messages accepted into per-worker `tokio::mpsc` queues (after recv, before worker `recv`).
pub static MARKET_DATA_ACCOUNT_WORKER_QUEUE_DEPTH: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// Account ingest: per-shard HIGH-priority `mpsc` depth (discovered pools, pinned, wallet-tracked curves).
pub static MARKET_DATA_ACCOUNT_HIGH_PRIORITY_QUEUE_DEPTH: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// Account ingest: per-shard LOW-priority `mpsc` depth (remaining relevant account updates).
pub static MARKET_DATA_ACCOUNT_LOW_PRIORITY_QUEUE_DEPTH: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// Account ingest: jobs waiting in the dedicated NATS publish `mpsc` (JetStream + core publish).
pub static MARKET_DATA_ACCOUNT_PUBLISH_QUEUE_DEPTH: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// Publish pipeline: `try_send` to main publish queue failed (channel full).
pub static MARKET_DATA_ACCOUNT_PUBLISH_ENQUEUE_DROPPED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// Dedicated publish worker: job exceeded per-job wall timeout (aborted + reconnect).
pub static MARKET_DATA_ACCOUNT_PUBLISH_WORKER_STALLS_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// Dedicated publish worker: reconnect after stall/timeout recovery.
pub static MARKET_DATA_ACCOUNT_PUBLISH_WORKER_RECONNECTS_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

const MARKET_DATA_ACCOUNT_PUBLISH_WORKER_LAST_SUCCESS_MAX: usize = 32;
static MARKET_DATA_ACCOUNT_PUBLISH_WORKER_LAST_SUCCESS_UNIX_MS: Lazy<Vec<AtomicU64>> =
    Lazy::new(|| {
        (0..MARKET_DATA_ACCOUNT_PUBLISH_WORKER_LAST_SUCCESS_MAX)
            .map(|_| AtomicU64::new(0))
            .collect()
    });

/// Per publish-worker job wall time (single NATS publish unit), microseconds.
const MARKET_DATA_ACCOUNT_PUBLISH_WORKER_JOB_DURATION_US_BUCKETS: &[u64] = &[
    50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000,
    1_000_000, 2_000_000,
];

static MARKET_DATA_ACCOUNT_PUBLISH_WORKER_JOB_DURATION_US_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> =
    Lazy::new(|| {
        MARKET_DATA_ACCOUNT_PUBLISH_WORKER_JOB_DURATION_US_BUCKETS
            .iter()
            .map(|_| AtomicU64::new(0))
            .collect()
    });
static MARKET_DATA_ACCOUNT_PUBLISH_WORKER_JOB_DURATION_US_SUM: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
static MARKET_DATA_ACCOUNT_PUBLISH_WORKER_JOB_DURATION_US_COUNT: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// Account ingest: cheap relevance filter discarded the update before `handle_geyser_account` body.
pub static MARKET_DATA_ACCOUNT_EARLY_DROP_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

/// Wall microseconds for `handle_geyser_account` body per worker (excludes queue wait).
const MARKET_DATA_ACCOUNT_HANDLER_DURATION_US_BUCKETS: &[u64] = &[
    50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000,
    1_000_000, 2_000_000,
];

static MARKET_DATA_ACCOUNT_HANDLER_DURATION_US_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> =
    Lazy::new(|| {
        MARKET_DATA_ACCOUNT_HANDLER_DURATION_US_BUCKETS
            .iter()
            .map(|_| AtomicU64::new(0))
            .collect()
    });
static MARKET_DATA_ACCOUNT_HANDLER_DURATION_US_SUM: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
static MARKET_DATA_ACCOUNT_HANDLER_DURATION_US_COUNT: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// Monotonic tick bumped on Geyser account/tx ingest (Tokio liveness vs OS-thread watchdog).
pub static MARKET_DATA_INGEST_PROGRESS_TICK: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

/// Wall ms of last Geyser ingest progress (account, tx, or head slot).
pub static MARKET_DATA_TOKIO_LAST_PROGRESS_UNIX_MS: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// Tokio runtime detected as stalled (no ingest progress); process exits for systemd restart.
pub static MARKET_DATA_TOKIO_LIVENESS_STALLS_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// JSONL off-hot-path queue full (`try_enqueue` drop).
pub static MARKET_DATA_JSONL_ENQUEUE_DROPPED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// PR Phase-R1: bounded JSONL writer queue depth (enqueue − dequeue).
pub static MARKET_DATA_JSONL_QUEUE_DEPTH: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

/// PR Phase-R1: records written by `jsonl-writer` OS thread.
pub static MARKET_DATA_JSONL_RECORDS_WRITTEN_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// Record Geyser ingest progress for Tokio liveness (cheap atomics only).
#[inline]
pub fn record_market_data_tokio_progress() {
    MARKET_DATA_TOKIO_LAST_PROGRESS_UNIX_MS.store(wall_clock_unix_ms_now(), Ordering::Relaxed);
    MARKET_DATA_INGEST_PROGRESS_TICK.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn record_market_data_tokio_liveness_stall() {
    MARKET_DATA_TOKIO_LIVENESS_STALLS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_jsonl_enqueue_dropped_total() {
    MARKET_DATA_JSONL_ENQUEUE_DROPPED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn set_market_data_jsonl_queue_depth(depth: usize) {
    MARKET_DATA_JSONL_QUEUE_DEPTH.store(depth as u64, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_jsonl_records_written_total() {
    MARKET_DATA_JSONL_RECORDS_WRITTEN_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Update monotonic Geyser head slot (max). Safe from any market-data ingest arm.
#[inline]
pub fn market_data_bump_geyser_head_slot(slot: u64) {
    if slot == 0 {
        return;
    }
    let _ = MARKET_DATA_GEYSER_HEAD_SLOT.fetch_max(slot, Ordering::Relaxed);
    record_market_data_tokio_progress();
}

/// Record wall ms delta for pump.fun trade after last bonding publish (bounded map hit only).
#[inline]
pub fn record_market_data_trade_after_bonding_publish_ms(delta_ms: u64) {
    record_histogram_u64_into(
        MARKET_DATA_GEYSER_TO_PUBLISH_MS_BUCKETS,
        &MARKET_DATA_TRADE_AFTER_BONDING_PUBLISH_MS_BUCKET_COUNTS,
        &MARKET_DATA_TRADE_AFTER_BONDING_PUBLISH_MS_SUM,
        &MARKET_DATA_TRADE_AFTER_BONDING_PUBLISH_MS_COUNT,
        delta_ms,
        MARKET_DATA_LATENCY_MS_SUM_CAP,
    );
}

/// Wall ms from `grpc_recv_at` (set in Geyser listener before `broadcast::send`) to market-data `recv()` return.
#[inline]
pub fn record_market_data_tx_channel_lag_ms(grpc_recv_at: Instant, recv_at: Instant) {
    let ms = recv_at
        .saturating_duration_since(grpc_recv_at)
        .as_millis()
        .min(u128::from(u64::MAX)) as u64;
    record_histogram_u64_into(
        MARKET_DATA_GEYSER_TO_PUBLISH_MS_BUCKETS,
        &MARKET_DATA_TX_CHANNEL_LAG_MS_BUCKET_COUNTS,
        &MARKET_DATA_TX_CHANNEL_LAG_MS_SUM,
        &MARKET_DATA_TX_CHANNEL_LAG_MS_COUNT,
        ms,
        MARKET_DATA_LATENCY_MS_SUM_CAP,
    );
}

/// Same as [`record_market_data_tx_channel_lag_ms`] for account updates.
#[inline]
pub fn record_market_data_account_channel_lag_ms(grpc_recv_at: Instant, recv_at: Instant) {
    let ms = recv_at
        .saturating_duration_since(grpc_recv_at)
        .as_millis()
        .min(u128::from(u64::MAX)) as u64;
    record_histogram_u64_into(
        MARKET_DATA_GEYSER_TO_PUBLISH_MS_BUCKETS,
        &MARKET_DATA_ACCOUNT_CHANNEL_LAG_MS_BUCKET_COUNTS,
        &MARKET_DATA_ACCOUNT_CHANNEL_LAG_MS_SUM,
        &MARKET_DATA_ACCOUNT_CHANNEL_LAG_MS_COUNT,
        ms,
        MARKET_DATA_LATENCY_MS_SUM_CAP,
    );
}

/// Pump.fun trade publish: chain slots since last bonding-curve progress publish for the same pool key.
#[inline]
pub fn record_market_data_bonding_to_trade_slot_delta_slots(delta_slots: u64) {
    record_histogram_u64_into(
        MARKET_DATA_SLOT_LAG_AT_PUBLISH_BUCKETS,
        &MARKET_DATA_BONDING_TO_TRADE_SLOT_DELTA_SLOTS_BUCKET_COUNTS,
        &MARKET_DATA_BONDING_TO_TRADE_SLOT_DELTA_SLOTS_SUM,
        &MARKET_DATA_BONDING_TO_TRADE_SLOT_DELTA_SLOTS_COUNT,
        delta_slots,
        u64::MAX,
    );
}

#[inline]
pub fn record_market_data_tx_broadcast_lagged(skipped_messages: u64) {
    if skipped_messages > 0 {
        MARKET_DATA_TX_BROADCAST_LAGGED_TOTAL.fetch_add(skipped_messages, Ordering::Relaxed);
    }
}

/// Update gauge: pending tx updates in the `broadcast` receiver (see `GeyserTxListener` tx channel).
#[inline]
pub fn set_market_data_tx_broadcast_queue_depth(depth: usize) {
    MARKET_DATA_TX_BROADCAST_QUEUE_DEPTH.store(depth as u64, Ordering::Relaxed);
}

/// Update gauge: pending account updates in the `broadcast` receiver (see `GeyserAccountListener` account channel).
#[inline]
pub fn set_market_data_account_broadcast_queue_depth(depth: usize) {
    MARKET_DATA_ACCOUNT_BROADCAST_QUEUE_DEPTH.store(depth as u64, Ordering::Relaxed);
}

#[inline]
pub fn record_market_data_account_broadcast_lagged(skipped_messages: u64) {
    if skipped_messages > 0 {
        MARKET_DATA_ACCOUNT_BROADCAST_LAGGED_TOTAL.fetch_add(skipped_messages, Ordering::Relaxed);
    }
}

#[inline]
pub fn set_market_data_account_worker_count(count: usize) {
    MARKET_DATA_ACCOUNT_WORKER_COUNT.store(count as u64, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_account_worker_queue_depth() {
    MARKET_DATA_ACCOUNT_WORKER_QUEUE_DEPTH.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn dec_market_data_account_worker_queue_depth() {
    MARKET_DATA_ACCOUNT_WORKER_QUEUE_DEPTH.fetch_sub(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_account_high_priority_queue_depth() {
    MARKET_DATA_ACCOUNT_HIGH_PRIORITY_QUEUE_DEPTH.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn dec_market_data_account_high_priority_queue_depth() {
    MARKET_DATA_ACCOUNT_HIGH_PRIORITY_QUEUE_DEPTH.fetch_sub(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_account_low_priority_queue_depth() {
    MARKET_DATA_ACCOUNT_LOW_PRIORITY_QUEUE_DEPTH.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn dec_market_data_account_low_priority_queue_depth() {
    MARKET_DATA_ACCOUNT_LOW_PRIORITY_QUEUE_DEPTH.fetch_sub(1, Ordering::Relaxed);
}

/// Wall ms from Geyser `grpc_recv_at` on the TX update to `DevWalletIdentified` publish (TX fast-path).
#[inline]
pub fn record_market_data_pool_mint_map_to_devwallet_ms(
    grpc_recv_at: Instant,
    publish_at: Instant,
) {
    let ms = publish_at
        .saturating_duration_since(grpc_recv_at)
        .as_millis()
        .min(u128::from(u64::MAX)) as u64;
    record_histogram_u64_into(
        MARKET_DATA_GEYSER_TO_PUBLISH_MS_BUCKETS,
        &MARKET_DATA_POOL_MINT_MAP_TO_DEVWALLET_MS_BUCKET_COUNTS,
        &MARKET_DATA_POOL_MINT_MAP_TO_DEVWALLET_MS_SUM,
        &MARKET_DATA_POOL_MINT_MAP_TO_DEVWALLET_MS_COUNT,
        ms,
        MARKET_DATA_LATENCY_MS_SUM_CAP,
    );
}

/// Wall ms from bonding-curve account `grpc_recv_at` to `DevWalletIdentified` publish (account path).
#[inline]
pub fn record_market_data_bonding_curve_grpc_to_devwallet_ms(
    grpc_recv_at: Instant,
    publish_at: Instant,
) {
    let ms = publish_at
        .saturating_duration_since(grpc_recv_at)
        .as_millis()
        .min(u128::from(u64::MAX)) as u64;
    record_histogram_u64_into(
        MARKET_DATA_GEYSER_TO_PUBLISH_MS_BUCKETS,
        &MARKET_DATA_BONDING_CURVE_GRPC_TO_DEVWALLET_MS_BUCKET_COUNTS,
        &MARKET_DATA_BONDING_CURVE_GRPC_TO_DEVWALLET_MS_SUM,
        &MARKET_DATA_BONDING_CURVE_GRPC_TO_DEVWALLET_MS_COUNT,
        ms,
        MARKET_DATA_LATENCY_MS_SUM_CAP,
    );
}

#[inline]
pub fn inc_market_data_account_publish_queue_depth() {
    MARKET_DATA_ACCOUNT_PUBLISH_QUEUE_DEPTH.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn dec_market_data_account_publish_queue_depth() {
    MARKET_DATA_ACCOUNT_PUBLISH_QUEUE_DEPTH.fetch_sub(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_account_publish_enqueue_dropped_total() {
    MARKET_DATA_ACCOUNT_PUBLISH_ENQUEUE_DROPPED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn record_market_data_account_publish_worker_stall() {
    MARKET_DATA_ACCOUNT_PUBLISH_WORKER_STALLS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn record_market_data_account_publish_worker_reconnect() {
    MARKET_DATA_ACCOUNT_PUBLISH_WORKER_RECONNECTS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn record_market_data_account_publish_worker_job_duration_us(us: u64) {
    record_histogram_u64_into(
        MARKET_DATA_ACCOUNT_PUBLISH_WORKER_JOB_DURATION_US_BUCKETS,
        &MARKET_DATA_ACCOUNT_PUBLISH_WORKER_JOB_DURATION_US_BUCKET_COUNTS,
        &MARKET_DATA_ACCOUNT_PUBLISH_WORKER_JOB_DURATION_US_SUM,
        &MARKET_DATA_ACCOUNT_PUBLISH_WORKER_JOB_DURATION_US_COUNT,
        us,
        u64::MAX,
    );
}

#[inline]
pub fn set_market_data_account_publish_worker_last_success_unix_ms(worker_id: usize, ms: u64) {
    if let Some(cell) = MARKET_DATA_ACCOUNT_PUBLISH_WORKER_LAST_SUCCESS_UNIX_MS.get(worker_id) {
        cell.store(ms, Ordering::Relaxed);
    }
}

#[inline]
pub fn record_market_data_account_early_drop_total() {
    MARKET_DATA_ACCOUNT_EARLY_DROP_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Per-account-update handler wall time (worker), microseconds.
#[inline]
pub fn record_market_data_account_handler_duration_us(us: u64) {
    record_histogram_u64_into(
        MARKET_DATA_ACCOUNT_HANDLER_DURATION_US_BUCKETS,
        &MARKET_DATA_ACCOUNT_HANDLER_DURATION_US_BUCKET_COUNTS,
        &MARKET_DATA_ACCOUNT_HANDLER_DURATION_US_SUM,
        &MARKET_DATA_ACCOUNT_HANDLER_DURATION_US_COUNT,
        us,
        u64::MAX,
    );
}

/// After successful core `TOPIC_MARKET_EVENTS` publish (`publish_market_event_core_and_momentum_ex` path).
#[inline]
pub fn record_market_data_geyser_to_publish_on_success(
    recv_at: Instant,
    segment: MarketDataLatencySegment,
    cold_path: bool,
    event_slot: Option<u64>,
) {
    if cold_path {
        return;
    }
    let elapsed_ms = recv_at.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
    match segment {
        MarketDataLatencySegment::Trade => record_histogram_u64_into(
            MARKET_DATA_GEYSER_TO_PUBLISH_MS_BUCKETS,
            &MARKET_DATA_GEYSER_TO_PUBLISH_MS_TRADE_BUCKET_COUNTS,
            &MARKET_DATA_GEYSER_TO_PUBLISH_MS_TRADE_SUM,
            &MARKET_DATA_GEYSER_TO_PUBLISH_MS_TRADE_COUNT,
            elapsed_ms,
            MARKET_DATA_LATENCY_MS_SUM_CAP,
        ),
        MarketDataLatencySegment::BondingCurve => record_histogram_u64_into(
            MARKET_DATA_GEYSER_TO_PUBLISH_MS_BUCKETS,
            &MARKET_DATA_GEYSER_TO_PUBLISH_MS_BONDING_CURVE_BUCKET_COUNTS,
            &MARKET_DATA_GEYSER_TO_PUBLISH_MS_BONDING_CURVE_SUM,
            &MARKET_DATA_GEYSER_TO_PUBLISH_MS_BONDING_CURVE_COUNT,
            elapsed_ms,
            MARKET_DATA_LATENCY_MS_SUM_CAP,
        ),
        MarketDataLatencySegment::PoolCreated => record_histogram_u64_into(
            MARKET_DATA_GEYSER_TO_PUBLISH_MS_BUCKETS,
            &MARKET_DATA_GEYSER_TO_PUBLISH_MS_POOL_CREATED_BUCKET_COUNTS,
            &MARKET_DATA_GEYSER_TO_PUBLISH_MS_POOL_CREATED_SUM,
            &MARKET_DATA_GEYSER_TO_PUBLISH_MS_POOL_CREATED_COUNT,
            elapsed_ms,
            MARKET_DATA_LATENCY_MS_SUM_CAP,
        ),
        MarketDataLatencySegment::Other => record_histogram_u64_into(
            MARKET_DATA_GEYSER_TO_PUBLISH_MS_BUCKETS,
            &MARKET_DATA_GEYSER_TO_PUBLISH_MS_OTHER_BUCKET_COUNTS,
            &MARKET_DATA_GEYSER_TO_PUBLISH_MS_OTHER_SUM,
            &MARKET_DATA_GEYSER_TO_PUBLISH_MS_OTHER_COUNT,
            elapsed_ms,
            MARKET_DATA_LATENCY_MS_SUM_CAP,
        ),
    };
    let es = match event_slot {
        Some(s) if s > 0 => s,
        _ => return,
    };
    let head = MARKET_DATA_GEYSER_HEAD_SLOT.load(Ordering::Relaxed);
    if head == 0 || head < es {
        return;
    }
    let lag = head.saturating_sub(es);
    match segment {
        MarketDataLatencySegment::Trade => record_histogram_u64_into(
            MARKET_DATA_SLOT_LAG_AT_PUBLISH_BUCKETS,
            &MARKET_DATA_SLOT_LAG_AT_PUBLISH_SLOTS_TRADE_BUCKET_COUNTS,
            &MARKET_DATA_SLOT_LAG_AT_PUBLISH_SLOTS_TRADE_SUM,
            &MARKET_DATA_SLOT_LAG_AT_PUBLISH_SLOTS_TRADE_COUNT,
            lag,
            u64::MAX,
        ),
        MarketDataLatencySegment::BondingCurve => record_histogram_u64_into(
            MARKET_DATA_SLOT_LAG_AT_PUBLISH_BUCKETS,
            &MARKET_DATA_SLOT_LAG_AT_PUBLISH_SLOTS_BONDING_CURVE_BUCKET_COUNTS,
            &MARKET_DATA_SLOT_LAG_AT_PUBLISH_SLOTS_BONDING_CURVE_SUM,
            &MARKET_DATA_SLOT_LAG_AT_PUBLISH_SLOTS_BONDING_CURVE_COUNT,
            lag,
            u64::MAX,
        ),
        MarketDataLatencySegment::PoolCreated => record_histogram_u64_into(
            MARKET_DATA_SLOT_LAG_AT_PUBLISH_BUCKETS,
            &MARKET_DATA_SLOT_LAG_AT_PUBLISH_SLOTS_POOL_CREATED_BUCKET_COUNTS,
            &MARKET_DATA_SLOT_LAG_AT_PUBLISH_SLOTS_POOL_CREATED_SUM,
            &MARKET_DATA_SLOT_LAG_AT_PUBLISH_SLOTS_POOL_CREATED_COUNT,
            lag,
            u64::MAX,
        ),
        MarketDataLatencySegment::Other => record_histogram_u64_into(
            MARKET_DATA_SLOT_LAG_AT_PUBLISH_BUCKETS,
            &MARKET_DATA_SLOT_LAG_AT_PUBLISH_SLOTS_OTHER_BUCKET_COUNTS,
            &MARKET_DATA_SLOT_LAG_AT_PUBLISH_SLOTS_OTHER_SUM,
            &MARKET_DATA_SLOT_LAG_AT_PUBLISH_SLOTS_OTHER_COUNT,
            lag,
            u64::MAX,
        ),
    };
}

// --- momentum-bot: intent header → JetStream publish (header.ts = TradeIntent::new wall time) ---
pub static MOMENTUM_INTENT_HEADER_TO_PUBLISH_MS_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> =
    Lazy::new(|| {
        MOMENTUM_EVENT_TO_LATENCY_MS_BUCKETS
            .iter()
            .map(|_| AtomicU64::new(0))
            .collect()
    });
pub static MOMENTUM_INTENT_HEADER_TO_PUBLISH_MS_SUM: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MOMENTUM_INTENT_HEADER_TO_PUBLISH_MS_COUNT: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// Causal `MarketEvent.header.ts_unix_ms` → `TradeIntent.header.ts_unix_ms` (entry path only).
pub static MOMENTUM_PUBLISH_TO_INTENT_MS_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> = Lazy::new(|| {
    MOMENTUM_EVENT_TO_LATENCY_MS_BUCKETS
        .iter()
        .map(|_| AtomicU64::new(0))
        .collect()
});
pub static MOMENTUM_PUBLISH_TO_INTENT_MS_SUM: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static MOMENTUM_PUBLISH_TO_INTENT_MS_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

#[inline]
pub fn try_record_momentum_intent_header_to_publish_ms(now_ms: u64, intent_header_ts_ms: u64) {
    if let Some(ms) = momentum_event_ts_latency_delta_ms(now_ms, intent_header_ts_ms) {
        record_histogram_u64_into(
            MOMENTUM_EVENT_TO_LATENCY_MS_BUCKETS,
            MOMENTUM_INTENT_HEADER_TO_PUBLISH_MS_BUCKET_COUNTS.as_slice(),
            &MOMENTUM_INTENT_HEADER_TO_PUBLISH_MS_SUM,
            &MOMENTUM_INTENT_HEADER_TO_PUBLISH_MS_COUNT,
            ms,
            MOMENTUM_LATENCY_MS_SUM_CAP,
        );
    }
}

/// Wall time from causal market-event publish timestamp to intent `RecordHeader` time.
#[inline]
pub fn try_record_momentum_publish_to_intent_ms(intent_header_ts_ms: u64, causal_event_ts_ms: u64) {
    if causal_event_ts_ms == 0 || causal_event_ts_ms > intent_header_ts_ms {
        MOMENTUM_LATENCY_EVENT_TS_INVALID_TOTAL.fetch_add(1, Ordering::Relaxed);
        return;
    }
    let ms = intent_header_ts_ms.saturating_sub(causal_event_ts_ms);
    record_histogram_u64_into(
        MOMENTUM_EVENT_TO_LATENCY_MS_BUCKETS,
        MOMENTUM_PUBLISH_TO_INTENT_MS_BUCKET_COUNTS.as_slice(),
        &MOMENTUM_PUBLISH_TO_INTENT_MS_SUM,
        &MOMENTUM_PUBLISH_TO_INTENT_MS_COUNT,
        ms,
        MOMENTUM_LATENCY_MS_SUM_CAP,
    );
}

// --- momentum-bot service metrics ---
pub static INTENTS_GENERATED_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
/// Exit/SELL intents generated by momentum-bot (separate from BUY intents)
pub static EXITS_GENERATED_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static FILTER_PASSED_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static FILTER_REJECTED_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static FILTER_REJECTED_LIQUIDITY: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static FILTER_REJECTED_VELOCITY: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static FILTER_REJECTED_BUYER_QUALITY: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static FILTER_REJECTED_INFLOW: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static FILTER_REJECTED_DEV_BEHAVIOR: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static MARKET_EVENTS_CONSUMED_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

// --- momentum-bot hot-path latency (Prometheus histograms; momentum-bot process only) ---
/// Producer `RecordHeader.ts_unix_ms` → momentum ingest (`now_ms`), milliseconds.
const MOMENTUM_EVENT_TO_LATENCY_MS_BUCKETS: &[u64] =
    &[1, 5, 10, 25, 50, 100, 250, 500, 1000, 2000, 5000];
pub static MOMENTUM_EVENT_TO_INGEST_MS_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> = Lazy::new(|| {
    MOMENTUM_EVENT_TO_LATENCY_MS_BUCKETS
        .iter()
        .map(|_| AtomicU64::new(0))
        .collect()
});
pub static MOMENTUM_EVENT_TO_INGEST_MS_SUM: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static MOMENTUM_EVENT_TO_INGEST_MS_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

/// JetStream `PoolCacheUpdate.header.ts_unix_ms` → momentum ingest (same buckets as Core NATS).
/// Separates replay / JetStream delivery skew from [`try_record_momentum_event_to_ingest_ms`], which
/// must remain **Core NATS live MarketEvents only** (see `docs/BUGS_FIXES.md`).
pub static MOMENTUM_JS_POOLCACHE_EVENT_TO_INGEST_MS_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> =
    Lazy::new(|| {
        MOMENTUM_EVENT_TO_LATENCY_MS_BUCKETS
            .iter()
            .map(|_| AtomicU64::new(0))
            .collect()
    });
pub static MOMENTUM_JS_POOLCACHE_EVENT_TO_INGEST_MS_SUM: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MOMENTUM_JS_POOLCACHE_EVENT_TO_INGEST_MS_COUNT: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// Same buckets: wall-clock from event `ts_unix_ms` to successful TradeIntent publish (only when
/// the causative `MarketEvent` timestamp is passed explicitly — no global `last_event_ts` guess).
pub static MOMENTUM_EVENT_TO_INTENT_PUBLISH_MS_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> =
    Lazy::new(|| {
        MOMENTUM_EVENT_TO_LATENCY_MS_BUCKETS
            .iter()
            .map(|_| AtomicU64::new(0))
            .collect()
    });
pub static MOMENTUM_EVENT_TO_INTENT_PUBLISH_MS_SUM: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MOMENTUM_EVENT_TO_INTENT_PUBLISH_MS_COUNT: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

const MOMENTUM_INTERNAL_US_BUCKETS: &[u64] = &[
    50, 100, 250, 500, 1000, 2500, 5000, 10000, 25000, 50000, 100000, 250000,
];
/// Full `process_intent` wall time (µs): includes simulation, send, and confirmation (seconds-scale).
const EXECUTION_PROCESS_INTENT_US_BUCKETS: &[u64] = &[
    50, 100, 250, 500, 1000, 2500, 5000, 10000, 25000, 50000, 100000, 250000, 500_000, 1_000_000,
    2_000_000, 5_000_000, 10_000_000, 20_000_000, 30_000_000, 45_000_000, 60_000_000,
];
/// Intent header → on-chain confirm (ms). Upper range matches [`EXECUTION_PROCESS_INTENT_US_BUCKETS`]
/// (60s) so `histogram_quantile` stays meaningful vs default confirmation timeouts (~15s+).
const EXECUTION_INTENT_TO_CONFIRM_MS_BUCKETS: &[u64] = &[
    1, 5, 10, 25, 50, 100, 250, 500, 1_000, 2_000, 5_000, 7_500, 10_000, 15_000, 20_000, 30_000,
    45_000, 60_000,
];
pub static MOMENTUM_INGEST_TO_PROCESS_US_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> = Lazy::new(|| {
    MOMENTUM_INTERNAL_US_BUCKETS
        .iter()
        .map(|_| AtomicU64::new(0))
        .collect()
});
pub static MOMENTUM_INGEST_TO_PROCESS_US_SUM: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static MOMENTUM_INGEST_TO_PROCESS_US_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

pub static MOMENTUM_PROCESS_MARKET_EVENT_US_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> = Lazy::new(|| {
    MOMENTUM_INTERNAL_US_BUCKETS
        .iter()
        .map(|_| AtomicU64::new(0))
        .collect()
});
pub static MOMENTUM_PROCESS_MARKET_EVENT_US_SUM: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static MOMENTUM_PROCESS_MARKET_EVENT_US_COUNT: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

pub static MOMENTUM_RECORD_TRADE_US_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> = Lazy::new(|| {
    MOMENTUM_INTERNAL_US_BUCKETS
        .iter()
        .map(|_| AtomicU64::new(0))
        .collect()
});
pub static MOMENTUM_RECORD_TRADE_US_SUM: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static MOMENTUM_RECORD_TRADE_US_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

pub static MOMENTUM_SIGNAL_EVAL_US_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> = Lazy::new(|| {
    MOMENTUM_INTERNAL_US_BUCKETS
        .iter()
        .map(|_| AtomicU64::new(0))
        .collect()
});
pub static MOMENTUM_SIGNAL_EVAL_US_SUM: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static MOMENTUM_SIGNAL_EVAL_US_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

pub static MOMENTUM_FULL_SCAN_SIGNAL_EVAL_US_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> =
    Lazy::new(|| {
        MOMENTUM_INTERNAL_US_BUCKETS
            .iter()
            .map(|_| AtomicU64::new(0))
            .collect()
    });
pub static MOMENTUM_FULL_SCAN_SIGNAL_EVAL_US_SUM: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static MOMENTUM_FULL_SCAN_SIGNAL_EVAL_US_COUNT: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// Deserialize + `flatten_market_events_for_ingest_ordered_batch` for one Core NATS activation.
pub static MOMENTUM_NATS_BATCH_PREPARE_US_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> = Lazy::new(|| {
    MOMENTUM_INTERNAL_US_BUCKETS
        .iter()
        .map(|_| AtomicU64::new(0))
        .collect()
});
pub static MOMENTUM_NATS_BATCH_PREPARE_US_SUM: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static MOMENTUM_NATS_BATCH_PREPARE_US_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

pub static MOMENTUM_LATENCY_EVENT_TS_INVALID_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

pub const MOMENTUM_LATENCY_MS_SUM_CAP: u64 = 1_000_000;
const MOMENTUM_LATENCY_US_SUM_CAP: u64 = 60_000_000;

// --- momentum-bot Core NATS ingest throughput / observability (bounded labels / scalar gauges) ---
/// Max `MarketEvent.slot` observed on **dequeued** Core NATS payloads in momentum-bot (subscription
/// buffer). This is **not** an independent live-chain head; see `momentum_market_events_internal_slot_delta_slots`.
pub static MOMENTUM_MARKET_EVENTS_SUBSCRIPTION_MAX_DEQUEUED_SLOT: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Last `MarketEvent.slot` from **Core NATS ingest** observed after entering `process_market_event` with
/// subscription-slot metrics enabled (mirrors strategy `last_event_slot` for that path; JetStream wallet
/// snapshots do not advance this gauge — see `momentum_market_events_internal_slot_delta_slots`).
pub static MOMENTUM_MARKET_EVENTS_LAST_APPLIED_SLOT: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Histogram samples where true `producer→ingest` ms exceeded [`MOMENTUM_LATENCY_MS_SUM_CAP`] (sum was clamped).
pub static MOMENTUM_EVENT_TO_INGEST_MS_SUM_CAPPED_SAMPLES_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Core NATS ingest: `tokio::select!` activations that pulled a MarketEvent batch.
pub static MOMENTUM_CORE_MARKET_EVENTS_INGEST_DRAIN_BATCHES_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Total Core NATS payloads drained across batches (after JSON decode batching, before flatten).
pub static MOMENTUM_CORE_MARKET_EVENTS_INGEST_DRAINED_MESSAGES_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Batches where `drained_messages >= effective_cap` (saturation signal).
pub static MOMENTUM_CORE_MARKET_EVENTS_INGEST_DRAIN_CAP_HIT_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Consecutive ingest batches that hit the drain cap (resets on a non-cap batch). Drives adaptive cap.
pub static MOMENTUM_CORE_MARKET_EVENTS_INGEST_CONSECUTIVE_CAP_HIT_STREAK: Lazy<AtomicU32> =
    Lazy::new(|| AtomicU32::new(0));

/// Max `producer_ts → ingest` wall lag (ms) observed in the **last completed** Core NATS ingest batch
/// (after flatten/coalesce). Complements `momentum_market_events_internal_slot_delta_slots`, which
/// can stay near zero while this gauge shows multi‑second/minute backlog.
pub static MOMENTUM_MARKET_EVENTS_INGEST_MAX_WALL_LAG_MS_LAST_BATCH: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// `UNIX_EPOCH` seconds when momentum-bot main set the gauge (0 before first set).
pub static MOMENTUM_BOT_PROCESS_START_UNIX_SEC: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

/// PumpFun BUY suppressed: missing creator/dev_wallet after bounded unwind (I‑12 / hot‑path fairness).
pub static MOMENTUM_ENTRY_BUY_SUPPRESSED_MISSING_CREATOR_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

#[inline]
pub fn set_momentum_market_events_ingest_max_wall_lag_ms_last_batch(ms: u64) {
    MOMENTUM_MARKET_EVENTS_INGEST_MAX_WALL_LAG_MS_LAST_BATCH.store(ms, Ordering::Relaxed);
}

#[inline]
pub fn set_momentum_bot_process_start_unix_sec(sec: u64) {
    MOMENTUM_BOT_PROCESS_START_UNIX_SEC.store(sec, Ordering::Relaxed);
}

#[inline]
pub fn record_momentum_entry_buy_suppressed_missing_creator() {
    MOMENTUM_ENTRY_BUY_SUPPRESSED_MISSING_CREATOR_TOTAL.fetch_add(1, Ordering::Relaxed);
}

// PR170: tracker trade ingest forensics (static metric names — no dynamic labels).
pub static MOMENTUM_TRACKER_TRADES_RECORDED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MOMENTUM_TRADES_RECEIVED_NO_TRACKER_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MOMENTUM_TRACKER_REJECTED_DEV_SELL_EARLY_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MOMENTUM_TRACKER_REJECTED_MICRO_BUY_SPAM_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MOMENTUM_TRACKER_REJECTED_BOT_CONCENTRATION_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MOMENTUM_TRACKER_REJECTED_LP_REMOVED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MOMENTUM_TRACKER_REJECTED_MINT_AUTHORITY_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MOMENTUM_TRACKER_REJECTED_PUMPFUN_BONDING_COMPLETE_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MOMENTUM_TRACKER_REJECTED_DEV_SUPPLY_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MOMENTUM_TRACKER_REJECTED_LARGE_DUMP_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MOMENTUM_TRACKER_REJECTED_OTHER_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

#[inline]
pub fn record_momentum_tracker_trades_recorded() {
    MOMENTUM_TRACKER_TRADES_RECORDED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn record_momentum_trades_received_no_tracker() {
    MOMENTUM_TRADES_RECEIVED_NO_TRACKER_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Maps `TokenTracker::reject` reason strings to low-cardinality Prometheus counters.
#[inline]
pub fn record_momentum_tracker_rejected(reason: &str) {
    if reason.starts_with("REJECT_DEV_SELL_EARLY") {
        MOMENTUM_TRACKER_REJECTED_DEV_SELL_EARLY_TOTAL.fetch_add(1, Ordering::Relaxed);
    } else if reason.starts_with("REJECT_MICRO_BUY_SPAM") {
        MOMENTUM_TRACKER_REJECTED_MICRO_BUY_SPAM_TOTAL.fetch_add(1, Ordering::Relaxed);
    } else if reason.starts_with("REJECT_BOT_CONCENTRATION") {
        MOMENTUM_TRACKER_REJECTED_BOT_CONCENTRATION_TOTAL.fetch_add(1, Ordering::Relaxed);
    } else if reason.starts_with("REJECT_LP_REMOVED") {
        MOMENTUM_TRACKER_REJECTED_LP_REMOVED_TOTAL.fetch_add(1, Ordering::Relaxed);
    } else if reason.starts_with("REJECT_MINT_AUTHORITY")
        || reason.starts_with("REJECT_FREEZE_AUTHORITY")
    {
        MOMENTUM_TRACKER_REJECTED_MINT_AUTHORITY_TOTAL.fetch_add(1, Ordering::Relaxed);
    } else if reason.starts_with("REJECT_PUMPFUN_BONDING_COMPLETE") {
        MOMENTUM_TRACKER_REJECTED_PUMPFUN_BONDING_COMPLETE_TOTAL.fetch_add(1, Ordering::Relaxed);
    } else if reason.starts_with("Dev holds too much") {
        MOMENTUM_TRACKER_REJECTED_DEV_SUPPLY_TOTAL.fetch_add(1, Ordering::Relaxed);
    } else if reason.starts_with("Large dump detected") {
        MOMENTUM_TRACKER_REJECTED_LARGE_DUMP_TOTAL.fetch_add(1, Ordering::Relaxed);
    } else {
        MOMENTUM_TRACKER_REJECTED_OTHER_TOTAL.fetch_add(1, Ordering::Relaxed);
    }
}

// Per-kind counters (static label keys via metric name — no dynamic labels).
pub static MOMENTUM_CORE_MARKET_EVENTS_RECV_TRADE_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MOMENTUM_CORE_MARKET_EVENTS_RECV_POOL_CREATED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MOMENTUM_CORE_MARKET_EVENTS_RECV_BONDING_CURVE_PROGRESS_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MOMENTUM_CORE_MARKET_EVENTS_RECV_DEX_POOL_ACCOUNTS_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MOMENTUM_CORE_MARKET_EVENTS_RECV_WALLET_BALANCE_SNAPSHOT_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MOMENTUM_CORE_MARKET_EVENTS_RECV_SLOT_UPDATE_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MOMENTUM_CORE_MARKET_EVENTS_RECV_POOL_STATE_UPDATE_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MOMENTUM_CORE_MARKET_EVENTS_RECV_TOKEN_MINT_INFO_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MOMENTUM_CORE_MARKET_EVENTS_RECV_OTHER_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

pub static MOMENTUM_CORE_MARKET_EVENTS_PROCESSED_TRADE_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MOMENTUM_CORE_MARKET_EVENTS_PROCESSED_POOL_CREATED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MOMENTUM_CORE_MARKET_EVENTS_PROCESSED_BONDING_CURVE_PROGRESS_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MOMENTUM_CORE_MARKET_EVENTS_PROCESSED_DEX_POOL_ACCOUNTS_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MOMENTUM_CORE_MARKET_EVENTS_PROCESSED_WALLET_BALANCE_SNAPSHOT_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MOMENTUM_CORE_MARKET_EVENTS_PROCESSED_SLOT_UPDATE_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MOMENTUM_CORE_MARKET_EVENTS_PROCESSED_POOL_STATE_UPDATE_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MOMENTUM_CORE_MARKET_EVENTS_PROCESSED_TOKEN_MINT_INFO_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MOMENTUM_CORE_MARKET_EVENTS_PROCESSED_OTHER_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// `max_dequeued_slot` / `last_applied_slot` are momentum-bot-local (subscription vs applied). **Not** a live NATS/chain head lag.
#[inline]
pub fn momentum_internal_subscription_slot_delta_saturating(
    max_dequeued_slot: u64,
    last_applied_slot: u64,
) -> u64 {
    max_dequeued_slot.saturating_sub(last_applied_slot)
}

#[inline]
pub fn record_momentum_market_events_subscription_max_dequeued_slot(slot: u64) {
    MOMENTUM_MARKET_EVENTS_SUBSCRIPTION_MAX_DEQUEUED_SLOT.fetch_max(slot, Ordering::Relaxed);
}

#[inline]
pub fn record_momentum_market_events_last_applied_slot(slot: u64) {
    MOMENTUM_MARKET_EVENTS_LAST_APPLIED_SLOT.fetch_max(slot, Ordering::Relaxed);
}

#[inline]
pub fn record_momentum_core_market_events_ingest_drain_batch(drained: usize, effective_cap: usize) {
    MOMENTUM_CORE_MARKET_EVENTS_INGEST_DRAIN_BATCHES_TOTAL.fetch_add(1, Ordering::Relaxed);
    MOMENTUM_CORE_MARKET_EVENTS_INGEST_DRAINED_MESSAGES_TOTAL
        .fetch_add(drained as u64, Ordering::Relaxed);
    if drained >= effective_cap {
        MOMENTUM_CORE_MARKET_EVENTS_INGEST_DRAIN_CAP_HIT_TOTAL.fetch_add(1, Ordering::Relaxed);
        let mut cur =
            MOMENTUM_CORE_MARKET_EVENTS_INGEST_CONSECUTIVE_CAP_HIT_STREAK.load(Ordering::Relaxed);
        loop {
            let next = cur.saturating_add(1).min(10_000);
            match MOMENTUM_CORE_MARKET_EVENTS_INGEST_CONSECUTIVE_CAP_HIT_STREAK
                .compare_exchange_weak(cur, next, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(c) => cur = c,
            }
        }
    } else {
        MOMENTUM_CORE_MARKET_EVENTS_INGEST_CONSECUTIVE_CAP_HIT_STREAK.store(0, Ordering::Relaxed);
    }
}

#[inline]
pub fn record_momentum_core_market_events_received_kind(kind: &'static str) {
    match kind {
        "Trade" => MOMENTUM_CORE_MARKET_EVENTS_RECV_TRADE_TOTAL.fetch_add(1, Ordering::Relaxed),
        "PoolCreated" => {
            MOMENTUM_CORE_MARKET_EVENTS_RECV_POOL_CREATED_TOTAL.fetch_add(1, Ordering::Relaxed)
        }
        "BondingCurveProgress" => MOMENTUM_CORE_MARKET_EVENTS_RECV_BONDING_CURVE_PROGRESS_TOTAL
            .fetch_add(1, Ordering::Relaxed),
        "DexPoolAccounts" => {
            MOMENTUM_CORE_MARKET_EVENTS_RECV_DEX_POOL_ACCOUNTS_TOTAL.fetch_add(1, Ordering::Relaxed)
        }
        "WalletBalanceSnapshot" => MOMENTUM_CORE_MARKET_EVENTS_RECV_WALLET_BALANCE_SNAPSHOT_TOTAL
            .fetch_add(1, Ordering::Relaxed),
        "SlotUpdate" => {
            MOMENTUM_CORE_MARKET_EVENTS_RECV_SLOT_UPDATE_TOTAL.fetch_add(1, Ordering::Relaxed)
        }
        "PoolStateUpdate" => {
            MOMENTUM_CORE_MARKET_EVENTS_RECV_POOL_STATE_UPDATE_TOTAL.fetch_add(1, Ordering::Relaxed)
        }
        "TokenMintInfo" => {
            MOMENTUM_CORE_MARKET_EVENTS_RECV_TOKEN_MINT_INFO_TOTAL.fetch_add(1, Ordering::Relaxed)
        }
        _ => MOMENTUM_CORE_MARKET_EVENTS_RECV_OTHER_TOTAL.fetch_add(1, Ordering::Relaxed),
    };
}

#[inline]
pub fn record_momentum_core_market_events_processed_kind(kind: &'static str) {
    match kind {
        "Trade" => {
            MOMENTUM_CORE_MARKET_EVENTS_PROCESSED_TRADE_TOTAL.fetch_add(1, Ordering::Relaxed)
        }
        "PoolCreated" => {
            MOMENTUM_CORE_MARKET_EVENTS_PROCESSED_POOL_CREATED_TOTAL.fetch_add(1, Ordering::Relaxed)
        }
        "BondingCurveProgress" => {
            MOMENTUM_CORE_MARKET_EVENTS_PROCESSED_BONDING_CURVE_PROGRESS_TOTAL
                .fetch_add(1, Ordering::Relaxed)
        }
        "DexPoolAccounts" => MOMENTUM_CORE_MARKET_EVENTS_PROCESSED_DEX_POOL_ACCOUNTS_TOTAL
            .fetch_add(1, Ordering::Relaxed),
        "WalletBalanceSnapshot" => {
            MOMENTUM_CORE_MARKET_EVENTS_PROCESSED_WALLET_BALANCE_SNAPSHOT_TOTAL
                .fetch_add(1, Ordering::Relaxed)
        }
        "SlotUpdate" => {
            MOMENTUM_CORE_MARKET_EVENTS_PROCESSED_SLOT_UPDATE_TOTAL.fetch_add(1, Ordering::Relaxed)
        }
        "PoolStateUpdate" => MOMENTUM_CORE_MARKET_EVENTS_PROCESSED_POOL_STATE_UPDATE_TOTAL
            .fetch_add(1, Ordering::Relaxed),
        "TokenMintInfo" => MOMENTUM_CORE_MARKET_EVENTS_PROCESSED_TOKEN_MINT_INFO_TOTAL
            .fetch_add(1, Ordering::Relaxed),
        _ => MOMENTUM_CORE_MARKET_EVENTS_PROCESSED_OTHER_TOTAL.fetch_add(1, Ordering::Relaxed),
    };
}

#[inline]
fn record_histogram_u64_into(
    buckets: &[u64],
    bucket_counts: &[AtomicU64],
    sum: &AtomicU64,
    count: &AtomicU64,
    value: u64,
    sum_cap: u64,
) {
    let v = value.min(sum_cap);
    sum.fetch_add(v, Ordering::Relaxed);
    count.fetch_add(1, Ordering::Relaxed);
    for (i, b) in buckets.iter().enumerate() {
        if v <= *b {
            bucket_counts[i].fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// `event_ts_ms` must be from the causative `MarketEvent` header. Returns `now_ms - event_ts_ms`
/// when the timestamp is usable (non-zero, not after `now_ms`). Otherwise increments
/// [`MOMENTUM_LATENCY_EVENT_TS_INVALID_TOTAL`] and returns `None` (no histogram sample).
pub fn momentum_event_ts_latency_delta_ms(now_ms: u64, event_ts_ms: u64) -> Option<u64> {
    if event_ts_ms == 0 || event_ts_ms > now_ms {
        MOMENTUM_LATENCY_EVENT_TS_INVALID_TOTAL.fetch_add(1, Ordering::Relaxed);
        return None;
    }
    Some(now_ms.saturating_sub(event_ts_ms))
}

/// Records producer→momentum wall latency for **live** Core NATS MarketEvent ingest.
/// Do **not** call from JetStream bootstrap/replay recovery (historical `ts_unix_ms` would skew p95/p99).
#[inline]
pub fn try_record_momentum_event_to_ingest_ms(now_ms: u64, event_ts_ms: u64) {
    if let Some(ms) = momentum_event_ts_latency_delta_ms(now_ms, event_ts_ms) {
        if ms > MOMENTUM_LATENCY_MS_SUM_CAP {
            MOMENTUM_EVENT_TO_INGEST_MS_SUM_CAPPED_SAMPLES_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        record_histogram_u64_into(
            MOMENTUM_EVENT_TO_LATENCY_MS_BUCKETS,
            &MOMENTUM_EVENT_TO_INGEST_MS_BUCKET_COUNTS,
            &MOMENTUM_EVENT_TO_INGEST_MS_SUM,
            &MOMENTUM_EVENT_TO_INGEST_MS_COUNT,
            ms,
            MOMENTUM_LATENCY_MS_SUM_CAP,
        );
    }
}

/// JetStream `PoolCacheUpdate` (or other JS pool-cache payloads with `RecordHeader.ts_unix_ms`).
#[inline]
pub fn try_record_momentum_jetstream_poolcache_event_to_ingest_ms(now_ms: u64, event_ts_ms: u64) {
    if let Some(ms) = momentum_event_ts_latency_delta_ms(now_ms, event_ts_ms) {
        record_histogram_u64_into(
            MOMENTUM_EVENT_TO_LATENCY_MS_BUCKETS,
            &MOMENTUM_JS_POOLCACHE_EVENT_TO_INGEST_MS_BUCKET_COUNTS,
            &MOMENTUM_JS_POOLCACHE_EVENT_TO_INGEST_MS_SUM,
            &MOMENTUM_JS_POOLCACHE_EVENT_TO_INGEST_MS_COUNT,
            ms,
            MOMENTUM_LATENCY_MS_SUM_CAP,
        );
    }
}

/// Call only after a successful JetStream publish when `event_ts_ms` is the **causal**
/// `MarketEvent.header.ts_unix_ms` for that intent (same mint/pool decision chain). Never pass a
/// timestamp from an unrelated event or a broad multi-position scan — use `None` at the call site
/// instead of calling this helper.
#[inline]
pub fn try_record_momentum_event_to_intent_publish_ms(now_ms: u64, event_ts_ms: u64) {
    if let Some(ms) = momentum_event_ts_latency_delta_ms(now_ms, event_ts_ms) {
        record_histogram_u64_into(
            MOMENTUM_EVENT_TO_LATENCY_MS_BUCKETS,
            &MOMENTUM_EVENT_TO_INTENT_PUBLISH_MS_BUCKET_COUNTS,
            &MOMENTUM_EVENT_TO_INTENT_PUBLISH_MS_SUM,
            &MOMENTUM_EVENT_TO_INTENT_PUBLISH_MS_COUNT,
            ms,
            MOMENTUM_LATENCY_MS_SUM_CAP,
        );
    }
}

#[inline]
pub fn record_momentum_ingest_to_process_us(us: u64) {
    record_histogram_u64_into(
        MOMENTUM_INTERNAL_US_BUCKETS,
        &MOMENTUM_INGEST_TO_PROCESS_US_BUCKET_COUNTS,
        &MOMENTUM_INGEST_TO_PROCESS_US_SUM,
        &MOMENTUM_INGEST_TO_PROCESS_US_COUNT,
        us,
        MOMENTUM_LATENCY_US_SUM_CAP,
    );
}

#[inline]
pub fn record_momentum_process_market_event_us(us: u64) {
    record_histogram_u64_into(
        MOMENTUM_INTERNAL_US_BUCKETS,
        &MOMENTUM_PROCESS_MARKET_EVENT_US_BUCKET_COUNTS,
        &MOMENTUM_PROCESS_MARKET_EVENT_US_SUM,
        &MOMENTUM_PROCESS_MARKET_EVENT_US_COUNT,
        us,
        MOMENTUM_LATENCY_US_SUM_CAP,
    );
}

#[inline]
pub fn record_momentum_record_trade_us(us: u64) {
    record_histogram_u64_into(
        MOMENTUM_INTERNAL_US_BUCKETS,
        &MOMENTUM_RECORD_TRADE_US_BUCKET_COUNTS,
        &MOMENTUM_RECORD_TRADE_US_SUM,
        &MOMENTUM_RECORD_TRADE_US_COUNT,
        us,
        MOMENTUM_LATENCY_US_SUM_CAP,
    );
}

#[inline]
pub fn record_momentum_signal_eval_us(us: u64) {
    record_histogram_u64_into(
        MOMENTUM_INTERNAL_US_BUCKETS,
        &MOMENTUM_SIGNAL_EVAL_US_BUCKET_COUNTS,
        &MOMENTUM_SIGNAL_EVAL_US_SUM,
        &MOMENTUM_SIGNAL_EVAL_US_COUNT,
        us,
        MOMENTUM_LATENCY_US_SUM_CAP,
    );
}

#[inline]
pub fn record_momentum_full_scan_signal_eval_us(us: u64) {
    record_histogram_u64_into(
        MOMENTUM_INTERNAL_US_BUCKETS,
        &MOMENTUM_FULL_SCAN_SIGNAL_EVAL_US_BUCKET_COUNTS,
        &MOMENTUM_FULL_SCAN_SIGNAL_EVAL_US_SUM,
        &MOMENTUM_FULL_SCAN_SIGNAL_EVAL_US_COUNT,
        us,
        MOMENTUM_LATENCY_US_SUM_CAP,
    );
}

#[inline]
pub fn record_momentum_nats_batch_prepare_us(us: u64) {
    record_histogram_u64_into(
        MOMENTUM_INTERNAL_US_BUCKETS,
        &MOMENTUM_NATS_BATCH_PREPARE_US_BUCKET_COUNTS,
        &MOMENTUM_NATS_BATCH_PREPARE_US_SUM,
        &MOMENTUM_NATS_BATCH_PREPARE_US_COUNT,
        us,
        MOMENTUM_LATENCY_US_SUM_CAP,
    );
}

/// Wall-clock milliseconds since UNIX epoch (for momentum E2E latency vs `RecordHeader.ts_unix_ms`).
#[inline]
pub fn wall_clock_unix_ms_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

#[cfg(test)]
fn reset_momentum_latency_metrics_for_test() {
    for c in MOMENTUM_EVENT_TO_INGEST_MS_BUCKET_COUNTS.iter() {
        c.store(0, Ordering::Relaxed);
    }
    MOMENTUM_EVENT_TO_INGEST_MS_SUM.store(0, Ordering::Relaxed);
    MOMENTUM_EVENT_TO_INGEST_MS_COUNT.store(0, Ordering::Relaxed);
    for c in MOMENTUM_JS_POOLCACHE_EVENT_TO_INGEST_MS_BUCKET_COUNTS.iter() {
        c.store(0, Ordering::Relaxed);
    }
    MOMENTUM_JS_POOLCACHE_EVENT_TO_INGEST_MS_SUM.store(0, Ordering::Relaxed);
    MOMENTUM_JS_POOLCACHE_EVENT_TO_INGEST_MS_COUNT.store(0, Ordering::Relaxed);
    for c in MOMENTUM_EVENT_TO_INTENT_PUBLISH_MS_BUCKET_COUNTS.iter() {
        c.store(0, Ordering::Relaxed);
    }
    MOMENTUM_EVENT_TO_INTENT_PUBLISH_MS_SUM.store(0, Ordering::Relaxed);
    MOMENTUM_EVENT_TO_INTENT_PUBLISH_MS_COUNT.store(0, Ordering::Relaxed);
    for c in MOMENTUM_INGEST_TO_PROCESS_US_BUCKET_COUNTS.iter() {
        c.store(0, Ordering::Relaxed);
    }
    MOMENTUM_INGEST_TO_PROCESS_US_SUM.store(0, Ordering::Relaxed);
    MOMENTUM_INGEST_TO_PROCESS_US_COUNT.store(0, Ordering::Relaxed);
    for c in MOMENTUM_PROCESS_MARKET_EVENT_US_BUCKET_COUNTS.iter() {
        c.store(0, Ordering::Relaxed);
    }
    MOMENTUM_PROCESS_MARKET_EVENT_US_SUM.store(0, Ordering::Relaxed);
    MOMENTUM_PROCESS_MARKET_EVENT_US_COUNT.store(0, Ordering::Relaxed);
    for c in MOMENTUM_RECORD_TRADE_US_BUCKET_COUNTS.iter() {
        c.store(0, Ordering::Relaxed);
    }
    MOMENTUM_RECORD_TRADE_US_SUM.store(0, Ordering::Relaxed);
    MOMENTUM_RECORD_TRADE_US_COUNT.store(0, Ordering::Relaxed);
    for c in MOMENTUM_SIGNAL_EVAL_US_BUCKET_COUNTS.iter() {
        c.store(0, Ordering::Relaxed);
    }
    MOMENTUM_SIGNAL_EVAL_US_SUM.store(0, Ordering::Relaxed);
    MOMENTUM_SIGNAL_EVAL_US_COUNT.store(0, Ordering::Relaxed);
    for c in MOMENTUM_FULL_SCAN_SIGNAL_EVAL_US_BUCKET_COUNTS.iter() {
        c.store(0, Ordering::Relaxed);
    }
    MOMENTUM_FULL_SCAN_SIGNAL_EVAL_US_SUM.store(0, Ordering::Relaxed);
    MOMENTUM_FULL_SCAN_SIGNAL_EVAL_US_COUNT.store(0, Ordering::Relaxed);
    for c in MOMENTUM_NATS_BATCH_PREPARE_US_BUCKET_COUNTS.iter() {
        c.store(0, Ordering::Relaxed);
    }
    MOMENTUM_NATS_BATCH_PREPARE_US_SUM.store(0, Ordering::Relaxed);
    MOMENTUM_NATS_BATCH_PREPARE_US_COUNT.store(0, Ordering::Relaxed);
    MOMENTUM_LATENCY_EVENT_TS_INVALID_TOTAL.store(0, Ordering::Relaxed);
    MOMENTUM_EVENT_TO_INGEST_MS_SUM_CAPPED_SAMPLES_TOTAL.store(0, Ordering::Relaxed);
    MOMENTUM_MARKET_EVENTS_SUBSCRIPTION_MAX_DEQUEUED_SLOT.store(0, Ordering::Relaxed);
    MOMENTUM_MARKET_EVENTS_LAST_APPLIED_SLOT.store(0, Ordering::Relaxed);
    MOMENTUM_CORE_MARKET_EVENTS_INGEST_DRAIN_BATCHES_TOTAL.store(0, Ordering::Relaxed);
    MOMENTUM_CORE_MARKET_EVENTS_INGEST_DRAINED_MESSAGES_TOTAL.store(0, Ordering::Relaxed);
    MOMENTUM_CORE_MARKET_EVENTS_INGEST_DRAIN_CAP_HIT_TOTAL.store(0, Ordering::Relaxed);
    MOMENTUM_CORE_MARKET_EVENTS_INGEST_CONSECUTIVE_CAP_HIT_STREAK.store(0, Ordering::Relaxed);
    MOMENTUM_CORE_MARKET_EVENTS_RECV_TRADE_TOTAL.store(0, Ordering::Relaxed);
    MOMENTUM_CORE_MARKET_EVENTS_RECV_POOL_CREATED_TOTAL.store(0, Ordering::Relaxed);
    MOMENTUM_CORE_MARKET_EVENTS_RECV_BONDING_CURVE_PROGRESS_TOTAL.store(0, Ordering::Relaxed);
    MOMENTUM_CORE_MARKET_EVENTS_RECV_DEX_POOL_ACCOUNTS_TOTAL.store(0, Ordering::Relaxed);
    MOMENTUM_CORE_MARKET_EVENTS_RECV_WALLET_BALANCE_SNAPSHOT_TOTAL.store(0, Ordering::Relaxed);
    MOMENTUM_CORE_MARKET_EVENTS_RECV_SLOT_UPDATE_TOTAL.store(0, Ordering::Relaxed);
    MOMENTUM_CORE_MARKET_EVENTS_RECV_POOL_STATE_UPDATE_TOTAL.store(0, Ordering::Relaxed);
    MOMENTUM_CORE_MARKET_EVENTS_RECV_TOKEN_MINT_INFO_TOTAL.store(0, Ordering::Relaxed);
    MOMENTUM_CORE_MARKET_EVENTS_RECV_OTHER_TOTAL.store(0, Ordering::Relaxed);
    MOMENTUM_CORE_MARKET_EVENTS_PROCESSED_TRADE_TOTAL.store(0, Ordering::Relaxed);
    MOMENTUM_CORE_MARKET_EVENTS_PROCESSED_POOL_CREATED_TOTAL.store(0, Ordering::Relaxed);
    MOMENTUM_CORE_MARKET_EVENTS_PROCESSED_BONDING_CURVE_PROGRESS_TOTAL.store(0, Ordering::Relaxed);
    MOMENTUM_CORE_MARKET_EVENTS_PROCESSED_DEX_POOL_ACCOUNTS_TOTAL.store(0, Ordering::Relaxed);
    MOMENTUM_CORE_MARKET_EVENTS_PROCESSED_WALLET_BALANCE_SNAPSHOT_TOTAL.store(0, Ordering::Relaxed);
    MOMENTUM_CORE_MARKET_EVENTS_PROCESSED_SLOT_UPDATE_TOTAL.store(0, Ordering::Relaxed);
    MOMENTUM_CORE_MARKET_EVENTS_PROCESSED_POOL_STATE_UPDATE_TOTAL.store(0, Ordering::Relaxed);
    MOMENTUM_CORE_MARKET_EVENTS_PROCESSED_TOKEN_MINT_INFO_TOTAL.store(0, Ordering::Relaxed);
    MOMENTUM_CORE_MARKET_EVENTS_PROCESSED_OTHER_TOTAL.store(0, Ordering::Relaxed);
}

// --- execution-engine service metrics ---
pub static INTENTS_RECEIVED_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static INTENTS_EXECUTED_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static INTENTS_REJECTED_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static SIMULATION_FAILURES_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
// RS-5.1: Real-send lifecycle counters (operator truth)
pub static TX_SEND_ATTEMPTS_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static TX_SEND_SUCCESS_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
// P2: Send method breakdown (TPU Direct vs Jito vs RPC)
pub static TX_SEND_TPU_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static TX_SEND_JITO_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static TX_SEND_RPC_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static TX_CONFIRMED_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static TX_CONFIRM_TIMEOUT_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
// FIX-32: Geyser-based TX confirmation breakdown
pub static TX_CONFIRM_GEYSER_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static TX_CONFIRM_RPC_FALLBACK_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static TX_CONFIRM_LATENCY_MS: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
// K Phase 1: Slot-to-Send Latency (Geyser event/slot → TX send)
const TX_SLOT_TO_SEND_MS_BUCKETS: &[u64] = &[10, 25, 50, 100, 200, 500, 1000, 2000];
pub static TX_SLOT_TO_SEND_MS_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> = Lazy::new(|| {
    TX_SLOT_TO_SEND_MS_BUCKETS
        .iter()
        .map(|_| AtomicU64::new(0))
        .collect()
});
pub static TX_SLOT_TO_SEND_MS_SUM_MS: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static TX_SLOT_TO_SEND_MS_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

// --- execution-engine pipeline latency (histograms; complements TX_CONFIRM_LATENCY_MS gauge) ---
pub static EXECUTION_INTENT_HEADER_TO_RECEIVE_MS_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> =
    Lazy::new(|| {
        EXECUTION_INTENT_TO_CONFIRM_MS_BUCKETS
            .iter()
            .map(|_| AtomicU64::new(0))
            .collect()
    });
pub static EXECUTION_INTENT_HEADER_TO_RECEIVE_MS_SUM: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static EXECUTION_INTENT_HEADER_TO_RECEIVE_MS_COUNT: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

pub static EXECUTION_INTENT_TO_CONFIRM_MS_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> = Lazy::new(|| {
    EXECUTION_INTENT_TO_CONFIRM_MS_BUCKETS
        .iter()
        .map(|_| AtomicU64::new(0))
        .collect()
});
pub static EXECUTION_INTENT_TO_CONFIRM_MS_SUM: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static EXECUTION_INTENT_TO_CONFIRM_MS_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

pub static EXECUTION_PROCESS_INTENT_US_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> = Lazy::new(|| {
    EXECUTION_PROCESS_INTENT_US_BUCKETS
        .iter()
        .map(|_| AtomicU64::new(0))
        .collect()
});
pub static EXECUTION_PROCESS_INTENT_US_SUM: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static EXECUTION_PROCESS_INTENT_US_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

pub static EXECUTION_SLOT_LAG_AT_SEND_SLOTS_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> = Lazy::new(|| {
    MARKET_DATA_SLOT_LAG_AT_PUBLISH_BUCKETS
        .iter()
        .map(|_| AtomicU64::new(0))
        .collect()
});
pub static EXECUTION_SLOT_LAG_AT_SEND_SLOTS_SUM: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static EXECUTION_SLOT_LAG_AT_SEND_SLOTS_COUNT: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

pub static TPU_RECONNECT_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static TPU_CACHE_STALE_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static GEYSER_TX_WATCHER_CONNECTED: Lazy<AtomicBool> = Lazy::new(|| AtomicBool::new(false));
pub static AVAILABLE_SOL_LAMPORTS: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ACTIVE_CAPITAL_LOCKS: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ACTIVE_RESOURCE_LOCKS: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
// Rejection reasons (labeled counters)
pub static REJECT_TTL_EXPIRED: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static REJECT_DUPLICATE: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static REJECT_CAPITAL_LOCK: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static REJECT_RESOURCE_LOCK: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static REJECT_RISK_LIMIT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static REJECT_SIMULATION_FAIL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static REJECT_SEND_FAILED: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

/// PumpSwap hot-path async healing (regular momentum SELL, structural sim error): eligible trigger
/// reached while **not** in per-base-mint cooldown (decision = publish path).
pub static PUMPSWAP_HOT_PATH_HEALING_TRIGGER_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Same path: trigger suppressed because per-base-mint cooldown still active.
pub static PUMPSWAP_HOT_PATH_HEALING_COOLDOWN_SUPPRESSED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Async `EnsurePumpAmmPoolAccounts` NATS publish returned `Ok(true)` (cooldown start in engine).
pub static PUMPSWAP_HOT_PATH_HEALING_ASYNC_PUBLISH_SUCCESS_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Async publish returned `Ok(false)` or `Err` (no cooldown advance).
pub static PUMPSWAP_HOT_PATH_HEALING_ASYNC_PUBLISH_FAIL_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Healing branch chose publish but NATS client missing (cannot spawn async publish).
pub static PUMPSWAP_HOT_PATH_HEALING_SKIPPED_NO_NATS_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

// --- NATS messaging metrics ---
pub static NATS_MESSAGES_PUBLISHED_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static NATS_MESSAGES_RECEIVED_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static NATS_RECONNECTS_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static NATS_ERRORS_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

// --- Wallet total (SOL + WSOL combined) ---
pub static WALLET_TOTAL_SOL_LAMPORTS: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

// --- Kill switch (execution-engine only; used for /status API) ---
/// When true: kill switch is active (BUYs blocked). Updated by execution-engine.
/// Control plane queries this via /status to sync UI display after restarts.
pub static KILL_SWITCH_ACTIVE: Lazy<AtomicBool> = Lazy::new(|| AtomicBool::new(false));

// =============================================================================
// E2E Readiness (market-data, execution-engine) – Blackbox-stable Status
// =============================================================================
// Process-local statics. Each binary sets its own values. Used by /status for
// structured readiness so Eval-E2E-Harness can verify contract readiness without
// reading Iron_crab/src, logs, or NATS connz.

/// NATS connected (set by binary when connection established)
pub static READINESS_NATS_CONNECTED: Lazy<AtomicBool> = Lazy::new(|| AtomicBool::new(false));
/// Control-Request subscription active (market-data) or ControlRequests+ControlResponses (execution-engine)
pub static READINESS_CONTROL_SUB_ACTIVE: Lazy<AtomicBool> = Lazy::new(|| AtomicBool::new(false));
/// Control-Response subscription active (execution-engine only)
pub static READINESS_CONTROL_RESPONSE_SUB_ACTIVE: Lazy<AtomicBool> =
    Lazy::new(|| AtomicBool::new(false));
/// JetStream currently usable (market-data: runtime get_stream check; not startup-latch)
pub static READINESS_JETSTREAM_READY: Lazy<AtomicBool> = Lazy::new(|| AtomicBool::new(false));
/// Consuming state paths initialized (execution-engine: LockManager, LivePoolCache bootstrap)
pub static READINESS_STATE_PATHS_INITIALIZED: Lazy<AtomicBool> =
    Lazy::new(|| AtomicBool::new(false));
/// Mode: 0=live, 1=dry_run, 2=simulate, 3=simulate_only
pub static READINESS_MODE: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

/// Component identifier for /status JSON (determines which readiness checks apply)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricsComponent {
    MarketData,
    ExecutionEngine,
    MomentumBot,
    ArbStrategy,
}

/// Set readiness: NATS connected
pub fn set_readiness_nats_connected(connected: bool) {
    READINESS_NATS_CONNECTED.store(connected, Ordering::Relaxed);
}

/// Set readiness: Control-Request subscription active
pub fn set_readiness_control_sub_active(active: bool) {
    READINESS_CONTROL_SUB_ACTIVE.store(active, Ordering::Relaxed);
}

/// Set readiness: Control-Response subscription active (execution-engine)
pub fn set_readiness_control_response_sub_active(active: bool) {
    READINESS_CONTROL_RESPONSE_SUB_ACTIVE.store(active, Ordering::Relaxed);
}

/// Set readiness: JetStream currently usable (runtime state, not startup-latch)
pub fn set_readiness_jetstream_ready(ready: bool) {
    READINESS_JETSTREAM_READY.store(ready, Ordering::Relaxed);
}

/// Set readiness: State paths initialized (execution-engine)
pub fn set_readiness_state_paths_initialized(initialized: bool) {
    READINESS_STATE_PATHS_INITIALIZED.store(initialized, Ordering::Relaxed);
}

/// Set readiness mode: 0=live, 1=dry_run, 2=simulate, 3=simulate_only
pub fn set_readiness_mode(mode: u8) {
    READINESS_MODE.store(mode as u64, Ordering::Relaxed);
}

/// Refresh readiness from current runtime state (not startup-latch).
/// Call periodically from main loop so /status reflects actual communication health.
pub fn update_readiness_market_data_current(
    nats_connected: bool,
    control_sub_active: bool,
    jetstream_ready: bool,
) {
    READINESS_NATS_CONNECTED.store(nats_connected, Ordering::Relaxed);
    READINESS_CONTROL_SUB_ACTIVE.store(control_sub_active, Ordering::Relaxed);
    READINESS_JETSTREAM_READY.store(jetstream_ready, Ordering::Relaxed);
}

/// Stale threshold for subscription liveness (seconds): if no heartbeat in this time, consider dead.
const SUBSCRIPTION_STALE_THRESHOLD_SECS: u64 = 15;

/// Refresh readiness from current runtime state (not startup-latch).
/// Control subs are active only when NATS connected AND subscription task has sent heartbeat recently.
pub fn update_readiness_execution_engine_current(
    nats_connected: bool,
    control_sub_last_activity_secs: u64,
    control_response_last_activity_secs: u64,
) {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    READINESS_NATS_CONNECTED.store(nats_connected, Ordering::Relaxed);
    READINESS_CONTROL_SUB_ACTIVE.store(
        nats_connected
            && now.saturating_sub(control_sub_last_activity_secs)
                <= SUBSCRIPTION_STALE_THRESHOLD_SECS,
        Ordering::Relaxed,
    );
    READINESS_CONTROL_RESPONSE_SUB_ACTIVE.store(
        nats_connected
            && now.saturating_sub(control_response_last_activity_secs)
                <= SUBSCRIPTION_STALE_THRESHOLD_SECS,
        Ordering::Relaxed,
    );
}

// --- WsolManager metrics ---
pub static WSOL_BALANCE_LAMPORTS: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static WSOL_WRAP_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static WSOL_UNWRAP_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static WSOL_WRAP_LAMPORTS_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static WSOL_UNWRAP_LAMPORTS_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

// --- AccountJanitor metrics ---
pub static JANITOR_CLOSE_ATA_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static JANITOR_SOL_RECOVERED_LAMPORTS: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static JANITOR_SWEEP_RUNS_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static JANITOR_ACCOUNTS_SCANNED_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static JANITOR_MERGE_DUST_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static JANITOR_TOKENS_MERGED_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static JANITOR_SWAP_DUST_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static JANITOR_SWAP_DUST_SOL_RECOVERED: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static JANITOR_SWAP_DUST_FAILED: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

// Global counters (simple, lock-free). For production consider Prometheus exporter.
pub static QUOTE_REQUESTS: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static QUOTE_SUCCESSES: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ROUTER_SINGLE_HOP: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ROUTER_HOPS2: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ROUTER_HOPS3: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TRIANGLE_ATTEMPTS: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TRIANGLE_PROFITABLE: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TRIANGLE_OPPORTUNITIES: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
/// Count of arb opportunities rejected due to missing DexPoolAccounts for pump_amm
pub static ARB_REJECTED_MISSING_ACCOUNTS: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static QUOTE_LATENCY_TOTAL_NS: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
// Generic cycle search metrics
pub static CYCLE_PARTIAL_EXAMINED: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static CYCLE_PRUNED_DOMINANCE: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static CYCLE_PRUNED_BOUND: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static CYCLE_COMPLETED: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
// Raydium refresh metrics
pub static RAYDIUM_POOLS_LOADED: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static RAYDIUM_POOLS_SKIPPED_SERUM: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static RAYDIUM_POOLS_SKIPPED_INVALID: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
// Total pools currently loaded in memory
pub static RAYDIUM_POOLS_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ORCA_POOLS_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
// Mint decimals resolution counters
pub static MINT_DECIMALS_SOURCE_CACHE: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static MINT_DECIMALS_SOURCE_SUPPLY: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static MINT_DECIMALS_SOURCE_ACCOUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static MINT_DECIMALS_FALLBACK_DEFAULT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

// --- New Prometheus style metrics (basic) ---
pub static TRADES_EXECUTED_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static TRADES_FAILED_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static RPC_ERRORS_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static RPC_RATE_LIMIT_HITS_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static RPC_TIMEOUTS_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static RPC_BACKOFF_MS_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static RPC_CONCURRENCY_ADJUSTMENTS_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static RPC_INFLIGHT_GAUGE: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static RPC_ALLOWED_CONCURRENCY: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static OPEN_POSITIONS_GAUGE: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
/// PA-2: PositionAuthority model open count (read-only; does not replace `open_positions`).
pub static POSITION_AUTHORITY_OPEN_GAUGE: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
/// Count of authority positions in `ReconcileNeeded` with non-zero balance.
pub static POSITION_AUTHORITY_RECONCILE_NEEDED_GAUGE: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// LockManager `count_non_zero_token_balances` mirrored for drift visibility (same instant as authority gauges when refreshed together).
pub static POSITION_AUTHORITY_LOCKMANAGER_OPEN_GAUGE: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// `authority_open - lockmanager_open` (signed; Prometheus scalar).
pub static POSITION_AUTHORITY_DRIFT_LOCKMANAGER: Lazy<AtomicI64> = Lazy::new(|| AtomicI64::new(0));
pub static CONCURRENT_INTENTS_GAUGE: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static DAILY_REALIZED_PNL_SOL_MICRO: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static LIQUIDITY_ESTIMATE_SOL_MICRO: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
// Histogram (swap latency) simplified: we keep bucket counters manually (ns)
const SWAP_LATENCY_BUCKETS: &[u64] = &[
    1_000_000,
    2_000_000,
    5_000_000,
    10_000_000,
    25_000_000,
    50_000_000,
    100_000_000,
    250_000_000,
    500_000_000,
    1_000_000_000,
];
pub static SWAP_LATENCY_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> = Lazy::new(|| {
    SWAP_LATENCY_BUCKETS
        .iter()
        .map(|_| AtomicU64::new(0))
        .collect()
});
pub static SWAP_LATENCY_SUM_NS: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static SWAP_LATENCY_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
// Quote latency histogram
const QUOTE_LATENCY_BUCKETS: &[u64] = &[
    200_000,
    500_000,
    1_000_000,
    2_000_000,
    5_000_000,
    10_000_000,
    25_000_000,
    50_000_000,
    100_000_000,
];
pub static QUOTE_LATENCY_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> = Lazy::new(|| {
    QUOTE_LATENCY_BUCKETS
        .iter()
        .map(|_| AtomicU64::new(0))
        .collect()
});
pub static QUOTE_LATENCY_SUM_NS: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static QUOTE_LATENCY_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
// Shortfall / Slippage aggregation
pub static SHORTFALL_TOKENS_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static SHORTFALL_SOL_MICRO_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static FILLS_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
// Network fees aggregation
pub static NETWORK_FEES_LAMPORTS_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static RPC_RETRY_ATTEMPTS_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
// Protocol / LP fee (aggregated lamports-equivalent or raw tokens? We aggregate lamports-equivalent for SOL side, plus raw token fee counts)
pub static PROTOCOL_FEE_TOKENS_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static PROTOCOL_FEE_SOL_MICRO_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
// Extended fee breakdown: DEX-specific protocol fees
pub static RAYDIUM_PROTOCOL_FEE_SOL_MICRO_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ORCA_PROTOCOL_FEE_SOL_MICRO_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
// Referrer fees (from transaction meta)
pub static REFERRER_FEE_SOL_MICRO_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
// Compute budget overhead (compute units * priority fee)
pub static COMPUTE_OVERHEAD_SOL_MICRO_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static PENDING_RECONCILIATIONS_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static PENDING_FAILED_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
// Partial exit metrics
pub static PARTIAL_EXIT_EVENTS_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static PARTIAL_EXIT_FRACTION_MICRO_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
// Jito bundle metrics
pub static JITO_BUNDLES_SUBMITTED_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static JITO_BUNDLES_LANDED_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static JITO_BUNDLES_REJECTED_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static JITO_BUNDLES_TIMEOUT_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static JITO_TIP_LAMPORTS_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static JITO_FALLBACK_RPC_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
// Re-quote metrics
pub static REQUOTE_EVENTS_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static REQUOTE_IMPROVED_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static REQUOTE_WORSENED_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
// Sum of (new_min_out - old_min_out)/old_min_out in micro (signed)
pub static REQUOTE_MIN_OUT_DELTA_RATIO_MICRO_SUM: Lazy<AtomicI64> = Lazy::new(|| AtomicI64::new(0));
// DEX selection (entry/exit) counters
pub static DEX_SELECTION_ENTRY_RAYDIUM_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static DEX_SELECTION_ENTRY_ORCA_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static DEX_SELECTION_EXIT_RAYDIUM_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static DEX_SELECTION_EXIT_ORCA_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
// Strategy sandboxing/IPC metrics
pub static STRATEGY_TICK_TIMEOUTS_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static STRATEGY_TICK_PANICS_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static STRATEGY_CIRCUIT_OPENS_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static STRATEGY_EXECUTIONS_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static STRATEGY_EXECUTION_SUCCESSES_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static STRATEGY_EXECUTION_FAILURES_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static PY_STRAT_TIMEOUTS_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static PY_STRAT_FAILS_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static PY_STRAT_CIRCUIT_OPENS_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static PY_STRAT_RESTARTS_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
// Gross vs Net realized PnL (session aggregates, SOL micro)
pub static GROSS_REALIZED_PNL_SOL_MICRO: Lazy<AtomicI64> = Lazy::new(|| AtomicI64::new(0));
pub static NET_REALIZED_PNL_SOL_MICRO: Lazy<AtomicI64> = Lazy::new(|| AtomicI64::new(0));
// Realized PnL (SOL) histogram (signed, absolute in SOL)
const REALIZED_PNL_SOL_BUCKETS: &[f64] = &[
    -1.0, -0.5, -0.25, -0.1, -0.05, -0.02, -0.01, 0.0, 0.01, 0.02, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0,
];
pub static REALIZED_PNL_SOL_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> = Lazy::new(|| {
    REALIZED_PNL_SOL_BUCKETS
        .iter()
        .map(|_| AtomicU64::new(0))
        .collect()
});
pub static REALIZED_PNL_SOL_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static REALIZED_PNL_SOL_SUM_MICRO: Lazy<AtomicI64> = Lazy::new(|| AtomicI64::new(0));
// Replay / Backtest driver metrics (populated by backtest driver/engine)
pub static REPLAY_MODE: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0)); // 1=replay, 0=live
pub static REPLAY_START_SLOT_GAUGE: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static REPLAY_END_SLOT_GAUGE: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static REPLAY_SLOT_MS_GAUGE: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static REPLAY_SEED_GAUGE: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static REPLAY_EVENTS_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static REPLAY_SLOTS_SEEN_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static REPLAY_NEW_POOLS_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static REPLAY_PRICE_UPDATES_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static REPLAY_RAYDIUM_POOLS_INGESTED: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static REPLAY_ORCA_POOLS_INGESTED: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static REPLAY_TRACE_POOLS_JSON_INGESTED: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
// Log management metrics
pub static LOG_FILES_CLEANED_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static LOG_CLEANUP_SIZE_BYTES_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static LOG_FILES_CURRENT_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static LOG_FILES_CURRENT_SIZE_BYTES: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
// Fee percent histogram (fee / notional), common percent buckets
const FEE_PCT_BUCKETS: &[f64] = &[0.0005, 0.001, 0.0025, 0.005, 0.01, 0.02, 0.05, 0.1];
pub static FEE_PCT_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> =
    Lazy::new(|| FEE_PCT_BUCKETS.iter().map(|_| AtomicU64::new(0)).collect());
pub static FEE_PCT_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
// Shortfall percent histogram (shortfall / expected_out)
const SHORTFALL_PCT_BUCKETS: &[f64] = &[
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5,
];
pub static SHORTFALL_PCT_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> = Lazy::new(|| {
    SHORTFALL_PCT_BUCKETS
        .iter()
        .map(|_| AtomicU64::new(0))
        .collect()
});
pub static SHORTFALL_PCT_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
// Realized trade return histogram (ratio realized_pnl / invested) buckets (cumulative style capture)
// Buckets chosen to capture deep losses to outsized wins.
const TRADE_RETURN_BUCKETS: &[f64] = &[
    -0.9, -0.5, -0.25, -0.1, -0.05, -0.02, -0.01, 0.0, 0.01, 0.02, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0,
];
pub static TRADE_RETURN_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> = Lazy::new(|| {
    TRADE_RETURN_BUCKETS
        .iter()
        .map(|_| AtomicU64::new(0))
        .collect()
});
pub static TRADE_RETURN_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static TRADE_RETURN_SUM_MICRO: Lazy<AtomicI64> = Lazy::new(|| AtomicI64::new(0)); // signed sum(ret * 1e6) for average
pub static SHARPE_RATIO_MICRO: Lazy<AtomicI64> = Lazy::new(|| AtomicI64::new(0));
pub static DRAWDOWN_PCT_MICRO: Lazy<AtomicI64> = Lazy::new(|| AtomicI64::new(0));
pub static LAST_ACTIVITY_TS: Lazy<AtomicU64> = Lazy::new(|| {
    // Default to "ready" on startup; it will go stale if the process stops updating.
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    AtomicU64::new(now)
});
const BUILD_VERSION: &str = env!("CARGO_PKG_VERSION");

pub struct LatencyTimer {
    start: Instant,
}
impl LatencyTimer {
    pub fn start() -> Self {
        Self {
            start: Instant::now(),
        }
    }
}
impl Drop for LatencyTimer {
    fn drop(&mut self) {
        let ns = self.start.elapsed().as_nanos() as u64;
        QUOTE_LATENCY_TOTAL_NS.fetch_add(ns, Ordering::Relaxed);
    }
}

pub fn record_quote_latency(ns: u64) {
    QUOTE_LATENCY_SUM_NS.fetch_add(ns, Ordering::Relaxed);
    QUOTE_LATENCY_COUNT.fetch_add(1, Ordering::Relaxed);
    for (i, b) in QUOTE_LATENCY_BUCKETS.iter().enumerate() {
        if ns <= *b {
            QUOTE_LATENCY_BUCKET_COUNTS[i].fetch_add(1, Ordering::Relaxed);
            break;
        }
    }
}

pub fn record_shortfall(tokens: u64, sol_ui: f64) {
    SHORTFALL_TOKENS_TOTAL.fetch_add(tokens, Ordering::Relaxed);
    SHORTFALL_SOL_MICRO_TOTAL.fetch_add((sol_ui * 1_000_000.0) as u64, Ordering::Relaxed);
    FILLS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

pub fn record_network_fee(lamports: u64) {
    NETWORK_FEES_LAMPORTS_TOTAL.fetch_add(lamports, Ordering::Relaxed);
}

pub fn record_trade_return(ret: f64) {
    // Bucket placement uses clamped value to keep distribution stable,
    // but the sum/average should reflect the actual (unclamped) return.
    let min_b = TRADE_RETURN_BUCKETS[0];
    let max_b = *TRADE_RETURN_BUCKETS.last().unwrap();
    let actual = if ret.is_finite() { ret } else { 0.0 };
    let bkt_val = actual.clamp(min_b, max_b);

    // Bucket placement (cumulative style)
    let mut placed = false;
    for (i, b) in TRADE_RETURN_BUCKETS.iter().enumerate() {
        if bkt_val <= *b {
            TRADE_RETURN_BUCKET_COUNTS[i].fetch_add(1, Ordering::Relaxed);
            placed = true;
            break;
        }
    }
    if !placed {
        // Should not happen due to clamp; kept for safety (+Inf via count only)
    }
    TRADE_RETURN_COUNT.fetch_add(1, Ordering::Relaxed);

    // Maintain running sum (micro) with saturation using the actual value
    let micro = (actual * 1_000_000.0).round();
    let micro_i64 = if micro > i64::MAX as f64 {
        i64::MAX
    } else if micro < i64::MIN as f64 {
        i64::MIN
    } else {
        micro as i64
    };
    TRADE_RETURN_SUM_MICRO.fetch_add(micro_i64, Ordering::Relaxed);
}

#[cfg(any(test, feature = "test_helpers"))]
pub fn reset_trade_return_metrics() {
    use std::sync::atomic::Ordering;
    for c in TRADE_RETURN_BUCKET_COUNTS.iter() {
        c.store(0, Ordering::Relaxed);
    }
    TRADE_RETURN_COUNT.store(0, Ordering::Relaxed);
    TRADE_RETURN_SUM_MICRO.store(0, Ordering::Relaxed);
}

pub fn record_fee_pct(pct: f64) {
    // Clamp to [0, 1] to avoid outliers; guard NaN/Inf
    let p = if pct.is_nan() || pct.is_infinite() || pct < 0.0 {
        0.0
    } else {
        pct.min(1.0)
    };
    for (i, b) in FEE_PCT_BUCKETS.iter().enumerate() {
        if p <= *b {
            FEE_PCT_BUCKET_COUNTS[i].fetch_add(1, Ordering::Relaxed);
            break;
        }
    }
    FEE_PCT_COUNT.fetch_add(1, Ordering::Relaxed);
}

pub fn record_shortfall_pct(pct: f64) {
    // Clamp to [0, 1] to avoid outliers; guard NaN/Inf
    let p = if pct.is_nan() || pct.is_infinite() || pct < 0.0 {
        0.0
    } else {
        pct.min(1.0)
    };
    for (i, b) in SHORTFALL_PCT_BUCKETS.iter().enumerate() {
        if p <= *b {
            SHORTFALL_PCT_BUCKET_COUNTS[i].fetch_add(1, Ordering::Relaxed);
            break;
        }
    }
    SHORTFALL_PCT_COUNT.fetch_add(1, Ordering::Relaxed);
}

pub fn record_realized_gross_net(gross_sol: f64, net_sol: f64) {
    let g = (gross_sol * 1_000_000.0).clamp(i64::MIN as f64, i64::MAX as f64) as i64;
    let n = (net_sol * 1_000_000.0).clamp(i64::MIN as f64, i64::MAX as f64) as i64;
    GROSS_REALIZED_PNL_SOL_MICRO.fetch_add(g, Ordering::Relaxed);
    NET_REALIZED_PNL_SOL_MICRO.fetch_add(n, Ordering::Relaxed);
}

pub fn record_realized_pnl_sol(value_sol: f64) {
    // Place in signed buckets; overflow goes to +Inf via count only
    let mut placed = false;
    for (i, b) in REALIZED_PNL_SOL_BUCKETS.iter().enumerate() {
        if value_sol <= *b {
            REALIZED_PNL_SOL_BUCKET_COUNTS[i].fetch_add(1, Ordering::Relaxed);
            placed = true;
            break;
        }
    }
    if !placed { /* +Inf implicit via count only */ }
    REALIZED_PNL_SOL_COUNT.fetch_add(1, Ordering::Relaxed);
    let micro = (value_sol * 1_000_000.0).round();
    let micro_i64 = if micro > i64::MAX as f64 {
        i64::MAX
    } else if micro < i64::MIN as f64 {
        i64::MIN
    } else {
        micro as i64
    };
    REALIZED_PNL_SOL_SUM_MICRO.fetch_add(micro_i64, Ordering::Relaxed);
}

pub fn snapshot() -> MetricsSnapshot {
    MetricsSnapshot {
        quote_requests: QUOTE_REQUESTS.load(Ordering::Relaxed),
        quote_successes: QUOTE_SUCCESSES.load(Ordering::Relaxed),
        router_single_hop: ROUTER_SINGLE_HOP.load(Ordering::Relaxed),
        router_hops2: ROUTER_HOPS2.load(Ordering::Relaxed),
        router_hops3: ROUTER_HOPS3.load(Ordering::Relaxed),
        arb_triangle_attempts: ARB_TRIANGLE_ATTEMPTS.load(Ordering::Relaxed),
        arb_triangle_profitable: ARB_TRIANGLE_PROFITABLE.load(Ordering::Relaxed),
        arb_triangle_opportunities: ARB_TRIANGLE_OPPORTUNITIES.load(Ordering::Relaxed),
        cycle_partial_examined: CYCLE_PARTIAL_EXAMINED.load(Ordering::Relaxed),
        cycle_pruned_dominance: CYCLE_PRUNED_DOMINANCE.load(Ordering::Relaxed),
        cycle_pruned_bound: CYCLE_PRUNED_BOUND.load(Ordering::Relaxed),
        cycle_completed: CYCLE_COMPLETED.load(Ordering::Relaxed),
        raydium_pools_loaded: RAYDIUM_POOLS_LOADED.load(Ordering::Relaxed),
        raydium_pools_skipped_serum: RAYDIUM_POOLS_SKIPPED_SERUM.load(Ordering::Relaxed),
        raydium_pools_skipped_invalid: RAYDIUM_POOLS_SKIPPED_INVALID.load(Ordering::Relaxed),
        raydium_pools_total: RAYDIUM_POOLS_TOTAL.load(Ordering::Relaxed),
        orca_pools_total: ORCA_POOLS_TOTAL.load(Ordering::Relaxed),
        avg_quote_latency_ms: {
            let reqs = QUOTE_REQUESTS.load(Ordering::Relaxed).max(1);
            (QUOTE_LATENCY_TOTAL_NS.load(Ordering::Relaxed) / reqs) as f64 / 1_000_000.0
        },
    }
}

#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub quote_requests: u64,
    pub quote_successes: u64,
    pub router_single_hop: u64,
    pub router_hops2: u64,
    pub router_hops3: u64,
    pub arb_triangle_attempts: u64,
    pub arb_triangle_profitable: u64,
    pub arb_triangle_opportunities: u64,
    pub cycle_partial_examined: u64,
    pub cycle_pruned_dominance: u64,
    pub cycle_pruned_bound: u64,
    pub cycle_completed: u64,
    pub avg_quote_latency_ms: f64,
    pub raydium_pools_loaded: u64,
    pub raydium_pools_skipped_serum: u64,
    pub raydium_pools_skipped_invalid: u64,
    pub raydium_pools_total: u64,
    pub orca_pools_total: u64,
}

/// Record one swap latency measurement (nanoseconds)
pub fn record_swap_latency(ns: u64) {
    SWAP_LATENCY_SUM_NS.fetch_add(ns, Ordering::Relaxed);
    SWAP_LATENCY_COUNT.fetch_add(1, Ordering::Relaxed);
    for (i, bucket) in SWAP_LATENCY_BUCKETS.iter().enumerate() {
        if ns <= *bucket {
            SWAP_LATENCY_BUCKET_COUNTS[i].fetch_add(1, Ordering::Relaxed);
            break;
        }
    }
}

/// Record swap latency from Duration
pub fn record_swap_latency_duration(duration: std::time::Duration) {
    record_swap_latency(duration.as_nanos() as u64);
}

/// Record slot-to-send latency (ms): time from Geyser event/slot observation to TX send.
/// Call after successful TX send when intent has slot_seen_at_ms in metadata.
/// If slot not available, do not call (no metric emitted).
pub fn record_tx_slot_to_send_ms(ms: u64) {
    TX_SLOT_TO_SEND_MS_SUM_MS.fetch_add(ms, Ordering::Relaxed);
    TX_SLOT_TO_SEND_MS_COUNT.fetch_add(1, Ordering::Relaxed);
    for (i, bucket) in TX_SLOT_TO_SEND_MS_BUCKETS.iter().enumerate() {
        if ms <= *bucket {
            TX_SLOT_TO_SEND_MS_BUCKET_COUNTS[i].fetch_add(1, Ordering::Relaxed);
            break;
        }
    }
}

/// `TradeIntent.header.ts_unix_ms` → first line of `process_intent` (JetStream / consumer skew).
#[inline]
pub fn try_record_execution_intent_header_to_receive_ms(now_ms: u64, intent_header_ts_ms: u64) {
    if let Some(ms) = momentum_event_ts_latency_delta_ms(now_ms, intent_header_ts_ms) {
        record_histogram_u64_into(
            EXECUTION_INTENT_TO_CONFIRM_MS_BUCKETS,
            EXECUTION_INTENT_HEADER_TO_RECEIVE_MS_BUCKET_COUNTS.as_slice(),
            &EXECUTION_INTENT_HEADER_TO_RECEIVE_MS_SUM,
            &EXECUTION_INTENT_HEADER_TO_RECEIVE_MS_COUNT,
            ms,
            MOMENTUM_LATENCY_MS_SUM_CAP,
        );
    }
}

/// Intent header time → on-chain confirm observation (wall), histogram for p99 (vs scalar gauge).
#[inline]
pub fn try_record_execution_intent_to_confirm_ms(now_ms: u64, intent_header_ts_ms: u64) {
    if let Some(ms) = momentum_event_ts_latency_delta_ms(now_ms, intent_header_ts_ms) {
        record_histogram_u64_into(
            EXECUTION_INTENT_TO_CONFIRM_MS_BUCKETS,
            EXECUTION_INTENT_TO_CONFIRM_MS_BUCKET_COUNTS.as_slice(),
            &EXECUTION_INTENT_TO_CONFIRM_MS_SUM,
            &EXECUTION_INTENT_TO_CONFIRM_MS_COUNT,
            ms,
            MOMENTUM_LATENCY_MS_SUM_CAP,
        );
    }
}

/// Total `process_intent` wall time (microseconds).
#[inline]
pub fn record_execution_process_intent_us(us: u64) {
    record_histogram_u64_into(
        EXECUTION_PROCESS_INTENT_US_BUCKETS,
        EXECUTION_PROCESS_INTENT_US_BUCKET_COUNTS.as_slice(),
        &EXECUTION_PROCESS_INTENT_US_SUM,
        &EXECUTION_PROCESS_INTENT_US_COUNT,
        us,
        MOMENTUM_LATENCY_US_SUM_CAP,
    );
}

/// `cached_blockhash.slot` (Geyser-fed) minus intent metadata `slot` at successful send.
#[inline]
pub fn record_execution_slot_lag_at_send_slots(lag_slots: u64) {
    record_histogram_u64_into(
        MARKET_DATA_SLOT_LAG_AT_PUBLISH_BUCKETS,
        EXECUTION_SLOT_LAG_AT_SEND_SLOTS_BUCKET_COUNTS.as_slice(),
        &EXECUTION_SLOT_LAG_AT_SEND_SLOTS_SUM,
        &EXECUTION_SLOT_LAG_AT_SEND_SLOTS_COUNT,
        lag_slots,
        u64::MAX,
    );
}

/// Record price impact measurement (basis points)
pub fn record_price_impact(_price_impact_bps: f64) {
    // For now, we'll just track in the trade success metrics
    // Could add a separate histogram for price impact if needed
}

/// Record slippage measurement (basis points)  
pub fn record_slippage(_slippage_bps: f64) {
    // For now, we'll just track in the trade success metrics
    // Could add a separate histogram for slippage if needed
}

/// Append `_bucket{le=...}`, `_sum`, `_count` lines (same layout as `tx_slot_to_send_ms`).
fn append_momentum_latency_histogram_prometheus(
    out: &mut String,
    metric: &str,
    buckets: &[u64],
    counts: &[AtomicU64],
    sum: &AtomicU64,
    count: &AtomicU64,
) {
    let c = count.load(Ordering::Relaxed);
    let s = sum.load(Ordering::Relaxed);
    for (i, b) in buckets.iter().enumerate() {
        let v = counts[i].load(Ordering::Relaxed);
        out.push_str(&format!("{}_bucket{{le=\"{}\"}} {}\n", metric, b, v));
    }
    out.push_str(&format!("{}_bucket{{le=\"+Inf\"}} {}\n", metric, c));
    out.push_str(&format!("{}_sum {}\n", metric, s));
    out.push_str(&format!("{}_count {}\n", metric, c));
}

async fn metrics_response() -> Response<Body> {
    // Build Prometheus exposition text
    let mut out = String::with_capacity(8192);
    macro_rules! line {
        ($name:expr, $val:expr) => {
            out.push_str($name);
            out.push(' ');
            out.push_str(&$val.to_string());
            out.push('\n');
        };
    }

    // =============================================================================
    // Multi-Process Architecture Metrics
    // =============================================================================

    // --- market-data service ---
    line!(
        "market_events_received_total",
        MARKET_EVENTS_RECEIVED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_events_published_total",
        MARKET_EVENTS_PUBLISHED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_events_momentum_fanout_published_total",
        MARKET_EVENTS_MOMENTUM_FANOUT_PUBLISHED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "pools_discovered_total",
        POOLS_DISCOVERED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "pools_tracked_total",
        POOLS_TRACKED_GAUGE.load(Ordering::Relaxed)
    );
    line!(
        "tokens_tracked_total",
        TOKENS_TRACKED_GAUGE.load(Ordering::Relaxed)
    );
    out.push_str("geyser_reconnect_total{reason=\"stream_ended\"} ");
    out.push_str(
        &GEYSER_RECONNECT_TOTAL_STREAM_ENDED
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("geyser_reconnect_total{reason=\"stream_error\"} ");
    out.push_str(
        &GEYSER_RECONNECT_TOTAL_STREAM_ERROR
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("geyser_reconnect_total{reason=\"sink_gone\"} ");
    out.push_str(
        &GEYSER_RECONNECT_TOTAL_SINK_GONE
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("geyser_reconnect_total{reason=\"subscription_rebuild\"} ");
    out.push_str(
        &GEYSER_RECONNECT_TOTAL_SUBSCRIPTION_REBUILD
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    line!(
        "geyser_stream_errors_total",
        GEYSER_STREAM_ERRORS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "geyser_listener_stream_messages_total",
        GEYSER_LISTENER_STREAM_MESSAGES_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "geyser_tracked_cuckoo_table_full_total",
        GEYSER_TRACKED_CUCKOO_TABLE_FULL_TOTAL.load(Ordering::Relaxed)
    );
    line!("geyser_connected", GEYSER_CONNECTED.load(Ordering::Relaxed));
    line!(
        "geyser_tx_session_connected",
        GEYSER_TX_SESSION_CONNECTED.load(Ordering::Relaxed)
    );
    line!(
        "geyser_account_session_connected",
        GEYSER_ACCOUNT_SESSION_CONNECTED.load(Ordering::Relaxed)
    );
    line!(
        "geyser_tx_listener_transactions_total",
        GEYSER_TX_LISTENER_TRANSACTIONS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "geyser_tx_listener_payload_broadcast_total",
        GEYSER_TX_LISTENER_PAYLOAD_BROADCAST_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_tx_handler_processed_total",
        MARKET_DATA_TX_HANDLER_PROCESSED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_tx_handler_last_progress_unix_ms",
        MARKET_DATA_TX_HANDLER_LAST_PROGRESS_UNIX_MS.load(Ordering::Relaxed)
    );
    line!(
        "market_data_tx_handler_stalls_total",
        MARKET_DATA_TX_HANDLER_STALLS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_global_ingest_stalls_total",
        MARKET_DATA_GLOBAL_INGEST_STALLS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_global_ingest_last_progress_unix_ms",
        MARKET_DATA_GLOBAL_INGEST_LAST_PROGRESS_UNIX_MS.load(Ordering::Relaxed)
    );
    line!(
        "market_data_tx_deferred_dropped_total",
        MARKET_DATA_TX_DEFERRED_DROPPED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_geyser_tracking_queue_depth",
        MARKET_DATA_GEYSER_TRACKING_QUEUE_DEPTH.load(Ordering::Relaxed)
    );
    line!(
        "market_data_md_state_queue_depth",
        MARKET_DATA_GEYSER_TRACKING_QUEUE_DEPTH.load(Ordering::Relaxed)
    );
    line!(
        "market_data_geyser_tracking_enqueue_dropped_total",
        MARKET_DATA_GEYSER_TRACKING_ENQUEUE_DROPPED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_geyser_tracking_jobs_processed_total",
        MARKET_DATA_GEYSER_TRACKING_JOBS_PROCESSED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_md_sidefx_queue_depth",
        MARKET_DATA_MD_SIDEFX_QUEUE_DEPTH.load(Ordering::Relaxed)
    );
    line!(
        "market_data_md_sidefx_enqueue_dropped_total",
        MARKET_DATA_MD_SIDEFX_ENQUEUE_DROPPED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_md_sidefx_jobs_processed_total",
        MARKET_DATA_MD_SIDEFX_JOBS_PROCESSED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_unparsed_tx_dropped_total",
        MARKET_DATA_UNPARSED_TX_DROPPED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_unparsed_account_dropped_total",
        MARKET_DATA_UNPARSED_ACCOUNT_DROPPED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "geyser_account_listener_account_updates_total",
        GEYSER_ACCOUNT_LISTENER_ACCOUNT_UPDATES_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "geyser_tx_listener_subscribe_updates_total",
        GEYSER_TX_LISTENER_SUBSCRIBE_UPDATES_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "geyser_account_listener_subscribe_updates_total",
        GEYSER_ACCOUNT_LISTENER_SUBSCRIBE_UPDATES_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "geyser_account_listener_subscribe_sink_throttled_total",
        GEYSER_ACCOUNT_LISTENER_SUBSCRIBE_SINK_THROTTLED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "geyser_account_listener_subscribe_sink_backpressure_total",
        GEYSER_ACCOUNT_LISTENER_SUBSCRIBE_SINK_BACKPRESSURE_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_geyser_subscription_send_timeout_total",
        MARKET_DATA_GEYSER_SUBSCRIPTION_SEND_TIMEOUT_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "geyser_account_listener_liveness_reconnects_total",
        GEYSER_ACCOUNT_LISTENER_LIVENESS_RECONNECTS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "geyser_tx_listener_liveness_reconnects_total",
        GEYSER_TX_LISTENER_LIVENESS_RECONNECTS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "geyser_subscription_accounts",
        GEYSER_SUBSCRIPTION_ACCOUNTS.load(Ordering::Relaxed)
    );
    line!(
        "geyser_tracked_pinned_accounts",
        GEYSER_TRACKED_PINNED_ACCOUNTS.load(Ordering::Relaxed)
    );
    line!(
        "momentum_active_pools_messages_total",
        MOMENTUM_ACTIVE_POOLS_MESSAGES_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_momentum_active_pool_messages_total",
        MARKET_DATA_MOMENTUM_ACTIVE_POOL_MESSAGES_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_momentum_active_pool_pins",
        MARKET_DATA_MOMENTUM_ACTIVE_POOL_PINS_GAUGE.load(Ordering::Relaxed)
    );
    line!(
        "market_data_momentum_coalesced_messages_total",
        MARKET_DATA_MOMENTUM_COALESCED_MESSAGES_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_momentum_coalesced_batches_total",
        MARKET_DATA_MOMENTUM_COALESCED_BATCHES_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_geyser_sync_batch_total",
        MARKET_DATA_GEYSER_SYNC_BATCH_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_geyser_sync_immediate_total",
        MARKET_DATA_GEYSER_SYNC_IMMEDIATE_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_geyser_sync_pending",
        MARKET_DATA_GEYSER_SYNC_PENDING.load(Ordering::Relaxed)
    );
    line!(
        "market_data_geyser_merge_coalesced_total",
        MARKET_DATA_GEYSER_MERGE_COALESCED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_geyser_merge_immediate_total",
        MARKET_DATA_GEYSER_MERGE_IMMEDIATE_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_geyser_merge_pending",
        MARKET_DATA_GEYSER_MERGE_PENDING.load(Ordering::Relaxed)
    );
    out.push_str("geyser_tracked_accounts_evicted_total{kind=\"vault\"} ");
    out.push_str(
        &GEYSER_TRACKED_ACCOUNTS_EVICTED_VAULT
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("geyser_tracked_accounts_evicted_total{kind=\"bin_array\"} ");
    out.push_str(
        &GEYSER_TRACKED_ACCOUNTS_EVICTED_BIN_ARRAY
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("geyser_tracked_accounts_evicted_total{kind=\"mint\"} ");
    out.push_str(
        &GEYSER_TRACKED_ACCOUNTS_EVICTED_MINT
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    line!(
        "market_data_geyser_head_slot",
        MARKET_DATA_GEYSER_HEAD_SLOT.load(Ordering::Relaxed)
    );
    line!(
        "market_data_tokio_last_progress_unix_ms",
        MARKET_DATA_TOKIO_LAST_PROGRESS_UNIX_MS.load(Ordering::Relaxed)
    );
    line!(
        "market_data_tokio_liveness_stalls_total",
        MARKET_DATA_TOKIO_LIVENESS_STALLS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_jsonl_enqueue_dropped_total",
        MARKET_DATA_JSONL_ENQUEUE_DROPPED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_jsonl_queue_depth",
        MARKET_DATA_JSONL_QUEUE_DEPTH.load(Ordering::Relaxed)
    );
    line!(
        "market_data_jsonl_records_written_total",
        MARKET_DATA_JSONL_RECORDS_WRITTEN_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_last_trade_publish_ts_unix_ms",
        MARKET_DATA_LAST_TRADE_PUBLISH_TS_UNIX_MS.load(Ordering::Relaxed)
    );
    line!(
        "market_data_last_bonding_curve_publish_ts_unix_ms",
        MARKET_DATA_LAST_BONDING_CURVE_PUBLISH_TS_UNIX_MS.load(Ordering::Relaxed)
    );
    append_momentum_latency_histogram_prometheus(
        &mut out,
        "market_data_geyser_to_publish_ms_trade",
        MARKET_DATA_GEYSER_TO_PUBLISH_MS_BUCKETS,
        &MARKET_DATA_GEYSER_TO_PUBLISH_MS_TRADE_BUCKET_COUNTS,
        &MARKET_DATA_GEYSER_TO_PUBLISH_MS_TRADE_SUM,
        &MARKET_DATA_GEYSER_TO_PUBLISH_MS_TRADE_COUNT,
    );
    append_momentum_latency_histogram_prometheus(
        &mut out,
        "market_data_geyser_to_publish_ms_bonding_curve",
        MARKET_DATA_GEYSER_TO_PUBLISH_MS_BUCKETS,
        &MARKET_DATA_GEYSER_TO_PUBLISH_MS_BONDING_CURVE_BUCKET_COUNTS,
        &MARKET_DATA_GEYSER_TO_PUBLISH_MS_BONDING_CURVE_SUM,
        &MARKET_DATA_GEYSER_TO_PUBLISH_MS_BONDING_CURVE_COUNT,
    );
    append_momentum_latency_histogram_prometheus(
        &mut out,
        "market_data_geyser_to_publish_ms_pool_created",
        MARKET_DATA_GEYSER_TO_PUBLISH_MS_BUCKETS,
        &MARKET_DATA_GEYSER_TO_PUBLISH_MS_POOL_CREATED_BUCKET_COUNTS,
        &MARKET_DATA_GEYSER_TO_PUBLISH_MS_POOL_CREATED_SUM,
        &MARKET_DATA_GEYSER_TO_PUBLISH_MS_POOL_CREATED_COUNT,
    );
    append_momentum_latency_histogram_prometheus(
        &mut out,
        "market_data_geyser_to_publish_ms_other",
        MARKET_DATA_GEYSER_TO_PUBLISH_MS_BUCKETS,
        &MARKET_DATA_GEYSER_TO_PUBLISH_MS_OTHER_BUCKET_COUNTS,
        &MARKET_DATA_GEYSER_TO_PUBLISH_MS_OTHER_SUM,
        &MARKET_DATA_GEYSER_TO_PUBLISH_MS_OTHER_COUNT,
    );
    append_momentum_latency_histogram_prometheus(
        &mut out,
        "market_data_slot_lag_at_publish_slots_trade",
        MARKET_DATA_SLOT_LAG_AT_PUBLISH_BUCKETS,
        &MARKET_DATA_SLOT_LAG_AT_PUBLISH_SLOTS_TRADE_BUCKET_COUNTS,
        &MARKET_DATA_SLOT_LAG_AT_PUBLISH_SLOTS_TRADE_SUM,
        &MARKET_DATA_SLOT_LAG_AT_PUBLISH_SLOTS_TRADE_COUNT,
    );
    append_momentum_latency_histogram_prometheus(
        &mut out,
        "market_data_slot_lag_at_publish_slots_bonding_curve",
        MARKET_DATA_SLOT_LAG_AT_PUBLISH_BUCKETS,
        &MARKET_DATA_SLOT_LAG_AT_PUBLISH_SLOTS_BONDING_CURVE_BUCKET_COUNTS,
        &MARKET_DATA_SLOT_LAG_AT_PUBLISH_SLOTS_BONDING_CURVE_SUM,
        &MARKET_DATA_SLOT_LAG_AT_PUBLISH_SLOTS_BONDING_CURVE_COUNT,
    );
    append_momentum_latency_histogram_prometheus(
        &mut out,
        "market_data_slot_lag_at_publish_slots_pool_created",
        MARKET_DATA_SLOT_LAG_AT_PUBLISH_BUCKETS,
        &MARKET_DATA_SLOT_LAG_AT_PUBLISH_SLOTS_POOL_CREATED_BUCKET_COUNTS,
        &MARKET_DATA_SLOT_LAG_AT_PUBLISH_SLOTS_POOL_CREATED_SUM,
        &MARKET_DATA_SLOT_LAG_AT_PUBLISH_SLOTS_POOL_CREATED_COUNT,
    );
    append_momentum_latency_histogram_prometheus(
        &mut out,
        "market_data_slot_lag_at_publish_slots_other",
        MARKET_DATA_SLOT_LAG_AT_PUBLISH_BUCKETS,
        &MARKET_DATA_SLOT_LAG_AT_PUBLISH_SLOTS_OTHER_BUCKET_COUNTS,
        &MARKET_DATA_SLOT_LAG_AT_PUBLISH_SLOTS_OTHER_SUM,
        &MARKET_DATA_SLOT_LAG_AT_PUBLISH_SLOTS_OTHER_COUNT,
    );
    append_momentum_latency_histogram_prometheus(
        &mut out,
        "market_data_trade_after_bonding_publish_ms",
        MARKET_DATA_GEYSER_TO_PUBLISH_MS_BUCKETS,
        &MARKET_DATA_TRADE_AFTER_BONDING_PUBLISH_MS_BUCKET_COUNTS,
        &MARKET_DATA_TRADE_AFTER_BONDING_PUBLISH_MS_SUM,
        &MARKET_DATA_TRADE_AFTER_BONDING_PUBLISH_MS_COUNT,
    );
    append_momentum_latency_histogram_prometheus(
        &mut out,
        "market_data_tx_channel_lag_ms",
        MARKET_DATA_GEYSER_TO_PUBLISH_MS_BUCKETS,
        &MARKET_DATA_TX_CHANNEL_LAG_MS_BUCKET_COUNTS,
        &MARKET_DATA_TX_CHANNEL_LAG_MS_SUM,
        &MARKET_DATA_TX_CHANNEL_LAG_MS_COUNT,
    );
    append_momentum_latency_histogram_prometheus(
        &mut out,
        "market_data_account_channel_lag_ms",
        MARKET_DATA_GEYSER_TO_PUBLISH_MS_BUCKETS,
        &MARKET_DATA_ACCOUNT_CHANNEL_LAG_MS_BUCKET_COUNTS,
        &MARKET_DATA_ACCOUNT_CHANNEL_LAG_MS_SUM,
        &MARKET_DATA_ACCOUNT_CHANNEL_LAG_MS_COUNT,
    );
    append_momentum_latency_histogram_prometheus(
        &mut out,
        "market_data_pool_mint_map_to_devwallet_ms",
        MARKET_DATA_GEYSER_TO_PUBLISH_MS_BUCKETS,
        &MARKET_DATA_POOL_MINT_MAP_TO_DEVWALLET_MS_BUCKET_COUNTS,
        &MARKET_DATA_POOL_MINT_MAP_TO_DEVWALLET_MS_SUM,
        &MARKET_DATA_POOL_MINT_MAP_TO_DEVWALLET_MS_COUNT,
    );
    append_momentum_latency_histogram_prometheus(
        &mut out,
        "market_data_bonding_curve_grpc_to_devwallet_ms",
        MARKET_DATA_GEYSER_TO_PUBLISH_MS_BUCKETS,
        &MARKET_DATA_BONDING_CURVE_GRPC_TO_DEVWALLET_MS_BUCKET_COUNTS,
        &MARKET_DATA_BONDING_CURVE_GRPC_TO_DEVWALLET_MS_SUM,
        &MARKET_DATA_BONDING_CURVE_GRPC_TO_DEVWALLET_MS_COUNT,
    );
    append_momentum_latency_histogram_prometheus(
        &mut out,
        "market_data_bonding_to_trade_slot_delta_slots",
        MARKET_DATA_SLOT_LAG_AT_PUBLISH_BUCKETS,
        &MARKET_DATA_BONDING_TO_TRADE_SLOT_DELTA_SLOTS_BUCKET_COUNTS,
        &MARKET_DATA_BONDING_TO_TRADE_SLOT_DELTA_SLOTS_SUM,
        &MARKET_DATA_BONDING_TO_TRADE_SLOT_DELTA_SLOTS_COUNT,
    );
    line!(
        "market_data_tx_broadcast_lagged_total",
        MARKET_DATA_TX_BROADCAST_LAGGED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_tx_broadcast_queue_depth",
        MARKET_DATA_TX_BROADCAST_QUEUE_DEPTH.load(Ordering::Relaxed)
    );
    line!(
        "market_data_account_broadcast_lagged_total",
        MARKET_DATA_ACCOUNT_BROADCAST_LAGGED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_account_broadcast_queue_depth",
        MARKET_DATA_ACCOUNT_BROADCAST_QUEUE_DEPTH.load(Ordering::Relaxed)
    );
    line!(
        "market_data_account_worker_count",
        MARKET_DATA_ACCOUNT_WORKER_COUNT.load(Ordering::Relaxed)
    );
    line!(
        "market_data_account_worker_queue_depth",
        MARKET_DATA_ACCOUNT_WORKER_QUEUE_DEPTH.load(Ordering::Relaxed)
    );
    line!(
        "market_data_account_high_priority_queue_depth",
        MARKET_DATA_ACCOUNT_HIGH_PRIORITY_QUEUE_DEPTH.load(Ordering::Relaxed)
    );
    line!(
        "market_data_account_low_priority_queue_depth",
        MARKET_DATA_ACCOUNT_LOW_PRIORITY_QUEUE_DEPTH.load(Ordering::Relaxed)
    );
    line!(
        "market_data_account_publish_queue_depth",
        MARKET_DATA_ACCOUNT_PUBLISH_QUEUE_DEPTH.load(Ordering::Relaxed)
    );
    line!(
        "market_data_account_publish_enqueue_dropped_total",
        MARKET_DATA_ACCOUNT_PUBLISH_ENQUEUE_DROPPED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_account_publish_worker_stalls_total",
        MARKET_DATA_ACCOUNT_PUBLISH_WORKER_STALLS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_account_publish_worker_reconnects_total",
        MARKET_DATA_ACCOUNT_PUBLISH_WORKER_RECONNECTS_TOTAL.load(Ordering::Relaxed)
    );
    for (i, cell) in MARKET_DATA_ACCOUNT_PUBLISH_WORKER_LAST_SUCCESS_UNIX_MS
        .iter()
        .enumerate()
    {
        out.push_str("market_data_account_publish_worker_last_success_unix_ms{worker=\"");
        out.push_str(&i.to_string());
        out.push_str("\"} ");
        out.push_str(&cell.load(Ordering::Relaxed).to_string());
        out.push('\n');
    }
    append_momentum_latency_histogram_prometheus(
        &mut out,
        "market_data_account_publish_worker_job_duration_us",
        MARKET_DATA_ACCOUNT_PUBLISH_WORKER_JOB_DURATION_US_BUCKETS,
        &MARKET_DATA_ACCOUNT_PUBLISH_WORKER_JOB_DURATION_US_BUCKET_COUNTS,
        &MARKET_DATA_ACCOUNT_PUBLISH_WORKER_JOB_DURATION_US_SUM,
        &MARKET_DATA_ACCOUNT_PUBLISH_WORKER_JOB_DURATION_US_COUNT,
    );
    line!(
        "market_data_account_early_drop_total",
        MARKET_DATA_ACCOUNT_EARLY_DROP_TOTAL.load(Ordering::Relaxed)
    );
    append_momentum_latency_histogram_prometheus(
        &mut out,
        "market_data_account_handler_duration_us",
        MARKET_DATA_ACCOUNT_HANDLER_DURATION_US_BUCKETS,
        &MARKET_DATA_ACCOUNT_HANDLER_DURATION_US_BUCKET_COUNTS,
        &MARKET_DATA_ACCOUNT_HANDLER_DURATION_US_SUM,
        &MARKET_DATA_ACCOUNT_HANDLER_DURATION_US_COUNT,
    );

    // --- momentum-bot service ---
    line!(
        "intents_generated_total",
        INTENTS_GENERATED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "exits_generated_total",
        EXITS_GENERATED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "filter_passed_total",
        FILTER_PASSED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "filter_rejected_total",
        FILTER_REJECTED_TOTAL.load(Ordering::Relaxed)
    );
    // Filter rejection breakdown
    out.push_str("filter_rejection_by_reason{reason=\"liquidity\"} ");
    out.push_str(
        &FILTER_REJECTED_LIQUIDITY
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("filter_rejection_by_reason{reason=\"velocity\"} ");
    out.push_str(&FILTER_REJECTED_VELOCITY.load(Ordering::Relaxed).to_string());
    out.push('\n');
    out.push_str("filter_rejection_by_reason{reason=\"buyer_quality\"} ");
    out.push_str(
        &FILTER_REJECTED_BUYER_QUALITY
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("filter_rejection_by_reason{reason=\"inflow\"} ");
    out.push_str(&FILTER_REJECTED_INFLOW.load(Ordering::Relaxed).to_string());
    out.push('\n');
    out.push_str("filter_rejection_by_reason{reason=\"dev_behavior\"} ");
    out.push_str(
        &FILTER_REJECTED_DEV_BEHAVIOR
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    line!(
        "market_events_consumed_total",
        MARKET_EVENTS_CONSUMED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "momentum_latency_event_ts_invalid_total",
        MOMENTUM_LATENCY_EVENT_TS_INVALID_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "momentum_event_to_ingest_ms_sum_capped_samples_total",
        MOMENTUM_EVENT_TO_INGEST_MS_SUM_CAPPED_SAMPLES_TOTAL.load(Ordering::Relaxed)
    );
    let max_dequeued_slot =
        MOMENTUM_MARKET_EVENTS_SUBSCRIPTION_MAX_DEQUEUED_SLOT.load(Ordering::Relaxed);
    let last_applied_slot = MOMENTUM_MARKET_EVENTS_LAST_APPLIED_SLOT.load(Ordering::Relaxed);
    line!(
        "momentum_market_events_subscription_max_dequeued_slot",
        max_dequeued_slot
    );
    line!(
        "momentum_market_events_last_applied_slot",
        last_applied_slot
    );
    line!(
        "momentum_market_events_internal_slot_delta_slots",
        momentum_internal_subscription_slot_delta_saturating(max_dequeued_slot, last_applied_slot)
    );
    line!(
        "momentum_market_events_ingest_max_wall_lag_ms_last_batch",
        MOMENTUM_MARKET_EVENTS_INGEST_MAX_WALL_LAG_MS_LAST_BATCH.load(Ordering::Relaxed)
    );
    line!(
        "momentum_bot_process_start_unix_seconds",
        MOMENTUM_BOT_PROCESS_START_UNIX_SEC.load(Ordering::Relaxed)
    );
    line!(
        "momentum_entry_buy_suppressed_missing_creator_total",
        MOMENTUM_ENTRY_BUY_SUPPRESSED_MISSING_CREATOR_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "momentum_tracker_trades_recorded_total",
        MOMENTUM_TRACKER_TRADES_RECORDED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "momentum_trades_received_no_tracker_total",
        MOMENTUM_TRADES_RECEIVED_NO_TRACKER_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "momentum_tracker_rejected_dev_sell_early_total",
        MOMENTUM_TRACKER_REJECTED_DEV_SELL_EARLY_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "momentum_tracker_rejected_micro_buy_spam_total",
        MOMENTUM_TRACKER_REJECTED_MICRO_BUY_SPAM_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "momentum_tracker_rejected_bot_concentration_total",
        MOMENTUM_TRACKER_REJECTED_BOT_CONCENTRATION_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "momentum_tracker_rejected_lp_removed_total",
        MOMENTUM_TRACKER_REJECTED_LP_REMOVED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "momentum_tracker_rejected_mint_authority_total",
        MOMENTUM_TRACKER_REJECTED_MINT_AUTHORITY_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "momentum_tracker_rejected_pumpfun_bonding_complete_total",
        MOMENTUM_TRACKER_REJECTED_PUMPFUN_BONDING_COMPLETE_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "momentum_tracker_rejected_dev_supply_total",
        MOMENTUM_TRACKER_REJECTED_DEV_SUPPLY_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "momentum_tracker_rejected_large_dump_total",
        MOMENTUM_TRACKER_REJECTED_LARGE_DUMP_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "momentum_tracker_rejected_other_total",
        MOMENTUM_TRACKER_REJECTED_OTHER_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "momentum_core_market_events_ingest_consecutive_cap_hit_streak",
        MOMENTUM_CORE_MARKET_EVENTS_INGEST_CONSECUTIVE_CAP_HIT_STREAK.load(Ordering::Relaxed)
    );
    line!(
        "momentum_core_market_events_ingest_drain_batches_total",
        MOMENTUM_CORE_MARKET_EVENTS_INGEST_DRAIN_BATCHES_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "momentum_core_market_events_ingest_drained_messages_total",
        MOMENTUM_CORE_MARKET_EVENTS_INGEST_DRAINED_MESSAGES_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "momentum_core_market_events_ingest_drain_cap_hit_total",
        MOMENTUM_CORE_MARKET_EVENTS_INGEST_DRAIN_CAP_HIT_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "momentum_core_market_events_received_trade_total",
        MOMENTUM_CORE_MARKET_EVENTS_RECV_TRADE_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "momentum_core_market_events_received_pool_created_total",
        MOMENTUM_CORE_MARKET_EVENTS_RECV_POOL_CREATED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "momentum_core_market_events_received_bonding_curve_progress_total",
        MOMENTUM_CORE_MARKET_EVENTS_RECV_BONDING_CURVE_PROGRESS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "momentum_core_market_events_received_dex_pool_accounts_total",
        MOMENTUM_CORE_MARKET_EVENTS_RECV_DEX_POOL_ACCOUNTS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "momentum_core_market_events_received_wallet_balance_snapshot_total",
        MOMENTUM_CORE_MARKET_EVENTS_RECV_WALLET_BALANCE_SNAPSHOT_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "momentum_core_market_events_received_slot_update_total",
        MOMENTUM_CORE_MARKET_EVENTS_RECV_SLOT_UPDATE_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "momentum_core_market_events_received_pool_state_update_total",
        MOMENTUM_CORE_MARKET_EVENTS_RECV_POOL_STATE_UPDATE_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "momentum_core_market_events_received_token_mint_info_total",
        MOMENTUM_CORE_MARKET_EVENTS_RECV_TOKEN_MINT_INFO_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "momentum_core_market_events_received_other_total",
        MOMENTUM_CORE_MARKET_EVENTS_RECV_OTHER_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "momentum_core_market_events_processed_trade_total",
        MOMENTUM_CORE_MARKET_EVENTS_PROCESSED_TRADE_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "momentum_core_market_events_processed_pool_created_total",
        MOMENTUM_CORE_MARKET_EVENTS_PROCESSED_POOL_CREATED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "momentum_core_market_events_processed_bonding_curve_progress_total",
        MOMENTUM_CORE_MARKET_EVENTS_PROCESSED_BONDING_CURVE_PROGRESS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "momentum_core_market_events_processed_dex_pool_accounts_total",
        MOMENTUM_CORE_MARKET_EVENTS_PROCESSED_DEX_POOL_ACCOUNTS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "momentum_core_market_events_processed_wallet_balance_snapshot_total",
        MOMENTUM_CORE_MARKET_EVENTS_PROCESSED_WALLET_BALANCE_SNAPSHOT_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "momentum_core_market_events_processed_slot_update_total",
        MOMENTUM_CORE_MARKET_EVENTS_PROCESSED_SLOT_UPDATE_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "momentum_core_market_events_processed_pool_state_update_total",
        MOMENTUM_CORE_MARKET_EVENTS_PROCESSED_POOL_STATE_UPDATE_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "momentum_core_market_events_processed_token_mint_info_total",
        MOMENTUM_CORE_MARKET_EVENTS_PROCESSED_TOKEN_MINT_INFO_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "momentum_core_market_events_processed_other_total",
        MOMENTUM_CORE_MARKET_EVENTS_PROCESSED_OTHER_TOTAL.load(Ordering::Relaxed)
    );
    append_momentum_latency_histogram_prometheus(
        &mut out,
        "momentum_event_to_ingest_ms",
        MOMENTUM_EVENT_TO_LATENCY_MS_BUCKETS,
        &MOMENTUM_EVENT_TO_INGEST_MS_BUCKET_COUNTS,
        &MOMENTUM_EVENT_TO_INGEST_MS_SUM,
        &MOMENTUM_EVENT_TO_INGEST_MS_COUNT,
    );
    append_momentum_latency_histogram_prometheus(
        &mut out,
        "momentum_jetstream_poolcache_event_to_ingest_ms",
        MOMENTUM_EVENT_TO_LATENCY_MS_BUCKETS,
        &MOMENTUM_JS_POOLCACHE_EVENT_TO_INGEST_MS_BUCKET_COUNTS,
        &MOMENTUM_JS_POOLCACHE_EVENT_TO_INGEST_MS_SUM,
        &MOMENTUM_JS_POOLCACHE_EVENT_TO_INGEST_MS_COUNT,
    );
    append_momentum_latency_histogram_prometheus(
        &mut out,
        "momentum_event_to_intent_publish_ms",
        MOMENTUM_EVENT_TO_LATENCY_MS_BUCKETS,
        &MOMENTUM_EVENT_TO_INTENT_PUBLISH_MS_BUCKET_COUNTS,
        &MOMENTUM_EVENT_TO_INTENT_PUBLISH_MS_SUM,
        &MOMENTUM_EVENT_TO_INTENT_PUBLISH_MS_COUNT,
    );
    append_momentum_latency_histogram_prometheus(
        &mut out,
        "momentum_intent_header_to_publish_ms",
        MOMENTUM_EVENT_TO_LATENCY_MS_BUCKETS,
        &MOMENTUM_INTENT_HEADER_TO_PUBLISH_MS_BUCKET_COUNTS,
        &MOMENTUM_INTENT_HEADER_TO_PUBLISH_MS_SUM,
        &MOMENTUM_INTENT_HEADER_TO_PUBLISH_MS_COUNT,
    );
    append_momentum_latency_histogram_prometheus(
        &mut out,
        "momentum_publish_to_intent_ms",
        MOMENTUM_EVENT_TO_LATENCY_MS_BUCKETS,
        &MOMENTUM_PUBLISH_TO_INTENT_MS_BUCKET_COUNTS,
        &MOMENTUM_PUBLISH_TO_INTENT_MS_SUM,
        &MOMENTUM_PUBLISH_TO_INTENT_MS_COUNT,
    );
    append_momentum_latency_histogram_prometheus(
        &mut out,
        "momentum_ingest_to_process_us",
        MOMENTUM_INTERNAL_US_BUCKETS,
        &MOMENTUM_INGEST_TO_PROCESS_US_BUCKET_COUNTS,
        &MOMENTUM_INGEST_TO_PROCESS_US_SUM,
        &MOMENTUM_INGEST_TO_PROCESS_US_COUNT,
    );
    append_momentum_latency_histogram_prometheus(
        &mut out,
        "momentum_process_market_event_us",
        MOMENTUM_INTERNAL_US_BUCKETS,
        &MOMENTUM_PROCESS_MARKET_EVENT_US_BUCKET_COUNTS,
        &MOMENTUM_PROCESS_MARKET_EVENT_US_SUM,
        &MOMENTUM_PROCESS_MARKET_EVENT_US_COUNT,
    );
    append_momentum_latency_histogram_prometheus(
        &mut out,
        "momentum_record_trade_us",
        MOMENTUM_INTERNAL_US_BUCKETS,
        &MOMENTUM_RECORD_TRADE_US_BUCKET_COUNTS,
        &MOMENTUM_RECORD_TRADE_US_SUM,
        &MOMENTUM_RECORD_TRADE_US_COUNT,
    );
    append_momentum_latency_histogram_prometheus(
        &mut out,
        "momentum_signal_eval_us",
        MOMENTUM_INTERNAL_US_BUCKETS,
        &MOMENTUM_SIGNAL_EVAL_US_BUCKET_COUNTS,
        &MOMENTUM_SIGNAL_EVAL_US_SUM,
        &MOMENTUM_SIGNAL_EVAL_US_COUNT,
    );
    append_momentum_latency_histogram_prometheus(
        &mut out,
        "momentum_full_scan_signal_eval_us",
        MOMENTUM_INTERNAL_US_BUCKETS,
        &MOMENTUM_FULL_SCAN_SIGNAL_EVAL_US_BUCKET_COUNTS,
        &MOMENTUM_FULL_SCAN_SIGNAL_EVAL_US_SUM,
        &MOMENTUM_FULL_SCAN_SIGNAL_EVAL_US_COUNT,
    );
    append_momentum_latency_histogram_prometheus(
        &mut out,
        "momentum_nats_batch_prepare_us",
        MOMENTUM_INTERNAL_US_BUCKETS,
        &MOMENTUM_NATS_BATCH_PREPARE_US_BUCKET_COUNTS,
        &MOMENTUM_NATS_BATCH_PREPARE_US_SUM,
        &MOMENTUM_NATS_BATCH_PREPARE_US_COUNT,
    );

    // --- execution-engine service ---
    line!(
        "intents_received_total",
        INTENTS_RECEIVED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "intents_executed_total",
        INTENTS_EXECUTED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "intents_rejected_total",
        INTENTS_REJECTED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "tx_send_attempts_total",
        TX_SEND_ATTEMPTS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "tx_send_success_total",
        TX_SEND_SUCCESS_TOTAL.load(Ordering::Relaxed)
    );
    // P2: Send method breakdown
    line!(
        "tx_send_tpu_total",
        TX_SEND_TPU_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "tx_send_jito_total",
        TX_SEND_JITO_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "tx_send_rpc_total",
        TX_SEND_RPC_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "tx_confirmed_total",
        TX_CONFIRMED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "tx_confirm_timeout_total",
        TX_CONFIRM_TIMEOUT_TOTAL.load(Ordering::Relaxed)
    );
    // FIX-32: Geyser-based TX confirmation breakdown
    line!(
        "tx_confirm_geyser_total",
        TX_CONFIRM_GEYSER_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "tx_confirm_rpc_fallback_total",
        TX_CONFIRM_RPC_FALLBACK_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "tx_confirm_latency_ms",
        TX_CONFIRM_LATENCY_MS.load(Ordering::Relaxed)
    );
    // K Phase 1: Slot-to-Send Latency histogram
    let sts_count = TX_SLOT_TO_SEND_MS_COUNT.load(Ordering::Relaxed);
    let sts_sum = TX_SLOT_TO_SEND_MS_SUM_MS.load(Ordering::Relaxed);
    for (i, b) in TX_SLOT_TO_SEND_MS_BUCKETS.iter().enumerate() {
        let cum = TX_SLOT_TO_SEND_MS_BUCKET_COUNTS[i].load(Ordering::Relaxed);
        out.push_str(&format!(
            "tx_slot_to_send_ms_bucket{{le=\"{}\"}} {}\n",
            b, cum
        ));
    }
    out.push_str(&format!(
        "tx_slot_to_send_ms_bucket{{le=\"+Inf\"}} {}\n",
        sts_count
    ));
    out.push_str(&format!("tx_slot_to_send_ms_sum {}\n", sts_sum));
    out.push_str(&format!("tx_slot_to_send_ms_count {}\n", sts_count));
    append_momentum_latency_histogram_prometheus(
        &mut out,
        "execution_intent_header_to_receive_ms",
        EXECUTION_INTENT_TO_CONFIRM_MS_BUCKETS,
        &EXECUTION_INTENT_HEADER_TO_RECEIVE_MS_BUCKET_COUNTS,
        &EXECUTION_INTENT_HEADER_TO_RECEIVE_MS_SUM,
        &EXECUTION_INTENT_HEADER_TO_RECEIVE_MS_COUNT,
    );
    append_momentum_latency_histogram_prometheus(
        &mut out,
        "execution_intent_to_confirm_ms",
        EXECUTION_INTENT_TO_CONFIRM_MS_BUCKETS,
        &EXECUTION_INTENT_TO_CONFIRM_MS_BUCKET_COUNTS,
        &EXECUTION_INTENT_TO_CONFIRM_MS_SUM,
        &EXECUTION_INTENT_TO_CONFIRM_MS_COUNT,
    );
    append_momentum_latency_histogram_prometheus(
        &mut out,
        "execution_process_intent_us",
        EXECUTION_PROCESS_INTENT_US_BUCKETS,
        &EXECUTION_PROCESS_INTENT_US_BUCKET_COUNTS,
        &EXECUTION_PROCESS_INTENT_US_SUM,
        &EXECUTION_PROCESS_INTENT_US_COUNT,
    );
    append_momentum_latency_histogram_prometheus(
        &mut out,
        "execution_slot_lag_at_send_slots",
        MARKET_DATA_SLOT_LAG_AT_PUBLISH_BUCKETS,
        &EXECUTION_SLOT_LAG_AT_SEND_SLOTS_BUCKET_COUNTS,
        &EXECUTION_SLOT_LAG_AT_SEND_SLOTS_SUM,
        &EXECUTION_SLOT_LAG_AT_SEND_SLOTS_COUNT,
    );
    line!(
        "tpu_reconnect_total",
        TPU_RECONNECT_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "tpu_cache_stale_total",
        TPU_CACHE_STALE_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "geyser_tx_watcher_connected",
        GEYSER_TX_WATCHER_CONNECTED.load(Ordering::Relaxed) as u64
    );
    line!(
        "simulation_failures_total",
        SIMULATION_FAILURES_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "available_sol_lamports",
        AVAILABLE_SOL_LAMPORTS.load(Ordering::Relaxed)
    );
    line!(
        "wallet_total_sol_lamports",
        WALLET_TOTAL_SOL_LAMPORTS.load(Ordering::Relaxed)
    );
    line!(
        "active_capital_locks",
        ACTIVE_CAPITAL_LOCKS.load(Ordering::Relaxed)
    );
    line!(
        "active_resource_locks",
        ACTIVE_RESOURCE_LOCKS.load(Ordering::Relaxed)
    );
    // Intent rejection reasons breakdown
    out.push_str("intent_rejection_by_reason{reason=\"ttl_expired\"} ");
    out.push_str(&REJECT_TTL_EXPIRED.load(Ordering::Relaxed).to_string());
    out.push('\n');
    out.push_str("intent_rejection_by_reason{reason=\"duplicate\"} ");
    out.push_str(&REJECT_DUPLICATE.load(Ordering::Relaxed).to_string());
    out.push('\n');
    out.push_str("intent_rejection_by_reason{reason=\"capital_lock\"} ");
    out.push_str(&REJECT_CAPITAL_LOCK.load(Ordering::Relaxed).to_string());
    out.push('\n');
    out.push_str("intent_rejection_by_reason{reason=\"resource_lock\"} ");
    out.push_str(&REJECT_RESOURCE_LOCK.load(Ordering::Relaxed).to_string());
    out.push('\n');
    out.push_str("intent_rejection_by_reason{reason=\"risk_limit\"} ");
    out.push_str(&REJECT_RISK_LIMIT.load(Ordering::Relaxed).to_string());
    out.push('\n');
    out.push_str("intent_rejection_by_reason{reason=\"simulation_fail\"} ");
    out.push_str(&REJECT_SIMULATION_FAIL.load(Ordering::Relaxed).to_string());
    out.push('\n');
    out.push_str("intent_rejection_by_reason{reason=\"send_failed\"} ");
    out.push_str(&REJECT_SEND_FAILED.load(Ordering::Relaxed).to_string());
    out.push('\n');
    line!(
        "pumpswap_hot_path_healing_trigger_total",
        PUMPSWAP_HOT_PATH_HEALING_TRIGGER_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "pumpswap_hot_path_healing_cooldown_suppressed_total",
        PUMPSWAP_HOT_PATH_HEALING_COOLDOWN_SUPPRESSED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "pumpswap_hot_path_healing_async_publish_success_total",
        PUMPSWAP_HOT_PATH_HEALING_ASYNC_PUBLISH_SUCCESS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "pumpswap_hot_path_healing_async_publish_fail_total",
        PUMPSWAP_HOT_PATH_HEALING_ASYNC_PUBLISH_FAIL_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "pumpswap_hot_path_healing_skipped_no_nats_total",
        PUMPSWAP_HOT_PATH_HEALING_SKIPPED_NO_NATS_TOTAL.load(Ordering::Relaxed)
    );

    // --- NATS messaging ---
    line!(
        "nats_messages_published_total",
        NATS_MESSAGES_PUBLISHED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "nats_messages_received_total",
        NATS_MESSAGES_RECEIVED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "nats_reconnects_total",
        NATS_RECONNECTS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "nats_errors_total",
        NATS_ERRORS_TOTAL.load(Ordering::Relaxed)
    );

    // --- WsolManager ---
    line!(
        "wsol_balance_lamports",
        WSOL_BALANCE_LAMPORTS.load(Ordering::Relaxed)
    );
    line!("wsol_wrap_total", WSOL_WRAP_TOTAL.load(Ordering::Relaxed));
    line!(
        "wsol_unwrap_total",
        WSOL_UNWRAP_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "wsol_wrap_lamports_total",
        WSOL_WRAP_LAMPORTS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "wsol_unwrap_lamports_total",
        WSOL_UNWRAP_LAMPORTS_TOTAL.load(Ordering::Relaxed)
    );

    // --- AccountJanitor ---
    line!(
        "janitor_close_ata_total",
        JANITOR_CLOSE_ATA_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "janitor_sol_recovered_lamports",
        JANITOR_SOL_RECOVERED_LAMPORTS.load(Ordering::Relaxed)
    );
    line!(
        "janitor_sweep_runs_total",
        JANITOR_SWEEP_RUNS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "janitor_accounts_scanned_total",
        JANITOR_ACCOUNTS_SCANNED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "janitor_merge_dust_total",
        JANITOR_MERGE_DUST_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "janitor_tokens_merged_total",
        JANITOR_TOKENS_MERGED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "janitor_swap_dust_total",
        JANITOR_SWAP_DUST_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "janitor_swap_dust_sol_recovered_lamports",
        JANITOR_SWAP_DUST_SOL_RECOVERED.load(Ordering::Relaxed)
    );
    line!(
        "janitor_swap_dust_failed_total",
        JANITOR_SWAP_DUST_FAILED.load(Ordering::Relaxed)
    );

    // =============================================================================
    // Legacy / Shared Metrics
    // =============================================================================

    line!(
        "quote_requests_total",
        QUOTE_REQUESTS.load(Ordering::Relaxed)
    );
    line!(
        "quote_successes_total",
        QUOTE_SUCCESSES.load(Ordering::Relaxed)
    );
    line!(
        "router_single_hop_total",
        ROUTER_SINGLE_HOP.load(Ordering::Relaxed)
    );
    line!("router_hops2_total", ROUTER_HOPS2.load(Ordering::Relaxed));
    line!("router_hops3_total", ROUTER_HOPS3.load(Ordering::Relaxed));
    line!(
        "arb_triangle_attempts_total",
        ARB_TRIANGLE_ATTEMPTS.load(Ordering::Relaxed)
    );
    line!(
        "arb_triangle_profitable_total",
        ARB_TRIANGLE_PROFITABLE.load(Ordering::Relaxed)
    );
    line!(
        "arb_triangle_opportunities_total",
        ARB_TRIANGLE_OPPORTUNITIES.load(Ordering::Relaxed)
    );
    line!(
        "arb_rejected_missing_accounts_total",
        ARB_REJECTED_MISSING_ACCOUNTS.load(Ordering::Relaxed)
    );
    line!(
        "cycle_partial_examined_total",
        CYCLE_PARTIAL_EXAMINED.load(Ordering::Relaxed)
    );
    line!(
        "cycle_pruned_dominance_total",
        CYCLE_PRUNED_DOMINANCE.load(Ordering::Relaxed)
    );
    line!(
        "cycle_pruned_bound_total",
        CYCLE_PRUNED_BOUND.load(Ordering::Relaxed)
    );
    line!(
        "cycle_completed_total",
        CYCLE_COMPLETED.load(Ordering::Relaxed)
    );
    line!(
        "raydium_pools_loaded_total",
        RAYDIUM_POOLS_LOADED.load(Ordering::Relaxed)
    );
    line!(
        "raydium_pools_skipped_serum_total",
        RAYDIUM_POOLS_SKIPPED_SERUM.load(Ordering::Relaxed)
    );
    line!(
        "raydium_pools_skipped_invalid_total",
        RAYDIUM_POOLS_SKIPPED_INVALID.load(Ordering::Relaxed)
    );
    line!(
        "raydium_pools_total",
        RAYDIUM_POOLS_TOTAL.load(Ordering::Relaxed)
    );
    line!("orca_pools_total", ORCA_POOLS_TOTAL.load(Ordering::Relaxed));
    line!(
        "mint_decimals_source_cache_total",
        MINT_DECIMALS_SOURCE_CACHE.load(Ordering::Relaxed)
    );
    line!(
        "mint_decimals_source_supply_total",
        MINT_DECIMALS_SOURCE_SUPPLY.load(Ordering::Relaxed)
    );
    line!(
        "mint_decimals_source_account_total",
        MINT_DECIMALS_SOURCE_ACCOUNT.load(Ordering::Relaxed)
    );
    line!(
        "mint_decimals_fallback_default_total",
        MINT_DECIMALS_FALLBACK_DEFAULT.load(Ordering::Relaxed)
    );
    line!(
        "trades_executed_total",
        TRADES_EXECUTED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "trades_failed_total",
        TRADES_FAILED_TOTAL.load(Ordering::Relaxed)
    );
    line!("rpc_errors_total", RPC_ERRORS_TOTAL.load(Ordering::Relaxed));
    line!(
        "rpc_rate_limit_hits_total",
        RPC_RATE_LIMIT_HITS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "rpc_timeouts_total",
        RPC_TIMEOUTS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "rpc_backoff_ms_total",
        RPC_BACKOFF_MS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "rpc_concurrency_adjustments_total",
        RPC_CONCURRENCY_ADJUSTMENTS_TOTAL.load(Ordering::Relaxed)
    );
    line!("rpc_inflight", RPC_INFLIGHT_GAUGE.load(Ordering::Relaxed));
    line!(
        "rpc_allowed_concurrency",
        RPC_ALLOWED_CONCURRENCY.load(Ordering::Relaxed)
    );
    line!(
        "open_positions",
        OPEN_POSITIONS_GAUGE.load(Ordering::Relaxed)
    );
    line!(
        "position_authority_open_positions",
        POSITION_AUTHORITY_OPEN_GAUGE.load(Ordering::Relaxed)
    );
    line!(
        "position_authority_reconcile_needed_positions",
        POSITION_AUTHORITY_RECONCILE_NEEDED_GAUGE.load(Ordering::Relaxed)
    );
    line!(
        "position_authority_lockmanager_open_positions",
        POSITION_AUTHORITY_LOCKMANAGER_OPEN_GAUGE.load(Ordering::Relaxed)
    );
    out.push_str(&format!(
        "position_authority_drift_lockmanager {}\n",
        POSITION_AUTHORITY_DRIFT_LOCKMANAGER.load(Ordering::Relaxed)
    ));
    line!(
        "concurrent_intents",
        CONCURRENT_INTENTS_GAUGE.load(Ordering::Relaxed)
    );
    line!(
        "daily_realized_pnl_sol",
        DAILY_REALIZED_PNL_SOL_MICRO.load(Ordering::Relaxed) as f64 / 1_000_000.0
    );
    line!(
        "liquidity_estimate_sol",
        LIQUIDITY_ESTIMATE_SOL_MICRO.load(Ordering::Relaxed) as f64 / 1_000_000.0
    );
    // Quote latency histogram exposition
    let q_count = QUOTE_LATENCY_COUNT.load(Ordering::Relaxed);
    let q_sum = QUOTE_LATENCY_SUM_NS.load(Ordering::Relaxed);
    for (i, b) in QUOTE_LATENCY_BUCKETS.iter().enumerate() {
        let cum = QUOTE_LATENCY_BUCKET_COUNTS[i].load(Ordering::Relaxed);
        out.push_str(&format!(
            "quote_latency_seconds_bucket{{le=\"{}\"}} {}\n",
            (*b as f64) / 1e9,
            cum
        ));
    }
    out.push_str(&format!(
        "quote_latency_seconds_sum {}\n",
        (q_sum as f64) / 1e9
    ));
    out.push_str(&format!("quote_latency_seconds_count {}\n", q_count));
    // Shortfall & fees aggregates
    line!(
        "shortfall_tokens_total",
        SHORTFALL_TOKENS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "shortfall_sol_total",
        SHORTFALL_SOL_MICRO_TOTAL.load(Ordering::Relaxed) as f64 / 1_000_000.0
    );
    line!("fills_total", FILLS_TOTAL.load(Ordering::Relaxed));
    line!(
        "network_fees_lamports_total",
        NETWORK_FEES_LAMPORTS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "rpc_retry_attempts_total",
        RPC_RETRY_ATTEMPTS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "protocol_fee_tokens_total",
        PROTOCOL_FEE_TOKENS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "protocol_fee_sol_total",
        PROTOCOL_FEE_SOL_MICRO_TOTAL.load(Ordering::Relaxed) as f64 / 1_000_000.0
    );
    // Extended fee breakdown
    line!(
        "raydium_protocol_fee_sol_total",
        RAYDIUM_PROTOCOL_FEE_SOL_MICRO_TOTAL.load(Ordering::Relaxed) as f64 / 1_000_000.0
    );
    line!(
        "orca_protocol_fee_sol_total",
        ORCA_PROTOCOL_FEE_SOL_MICRO_TOTAL.load(Ordering::Relaxed) as f64 / 1_000_000.0
    );
    line!(
        "referrer_fee_sol_total",
        REFERRER_FEE_SOL_MICRO_TOTAL.load(Ordering::Relaxed) as f64 / 1_000_000.0
    );
    line!(
        "compute_overhead_sol_total",
        COMPUTE_OVERHEAD_SOL_MICRO_TOTAL.load(Ordering::Relaxed) as f64 / 1_000_000.0
    );
    line!(
        "pending_reconciliations_total",
        PENDING_RECONCILIATIONS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "pending_failed_total",
        PENDING_FAILED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "partial_exit_events_total",
        PARTIAL_EXIT_EVENTS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "partial_exit_fraction_sum",
        PARTIAL_EXIT_FRACTION_MICRO_TOTAL.load(Ordering::Relaxed) as f64 / 1_000_000.0
    );
    // Jito bundle metrics
    line!(
        "jito_bundles_submitted_total",
        JITO_BUNDLES_SUBMITTED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "jito_bundles_landed_total",
        JITO_BUNDLES_LANDED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "jito_bundles_rejected_total",
        JITO_BUNDLES_REJECTED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "jito_bundles_timeout_total",
        JITO_BUNDLES_TIMEOUT_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "jito_tip_lamports_total",
        JITO_TIP_LAMPORTS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "jito_fallback_rpc_total",
        JITO_FALLBACK_RPC_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "requote_events_total",
        REQUOTE_EVENTS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "requote_improved_total",
        REQUOTE_IMPROVED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "requote_worsened_total",
        REQUOTE_WORSENED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "requote_min_out_delta_ratio_sum",
        REQUOTE_MIN_OUT_DELTA_RATIO_MICRO_SUM.load(Ordering::Relaxed) as f64 / 1_000_000.0
    );
    // DEX selection counters
    line!(
        "dex_selection_entry_raydium_total",
        DEX_SELECTION_ENTRY_RAYDIUM_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "dex_selection_entry_orca_total",
        DEX_SELECTION_ENTRY_ORCA_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "dex_selection_exit_raydium_total",
        DEX_SELECTION_EXIT_RAYDIUM_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "dex_selection_exit_orca_total",
        DEX_SELECTION_EXIT_ORCA_TOTAL.load(Ordering::Relaxed)
    );
    // Strategy sandboxing/IPC metrics
    line!(
        "strategy_tick_timeouts_total",
        STRATEGY_TICK_TIMEOUTS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "strategy_tick_panics_total",
        STRATEGY_TICK_PANICS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "strategy_circuit_opens_total",
        STRATEGY_CIRCUIT_OPENS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "strategy_executions_total",
        STRATEGY_EXECUTIONS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "strategy_execution_successes_total",
        STRATEGY_EXECUTION_SUCCESSES_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "strategy_execution_failures_total",
        STRATEGY_EXECUTION_FAILURES_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "py_strat_timeouts_total",
        PY_STRAT_TIMEOUTS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "py_strat_fails_total",
        PY_STRAT_FAILS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "py_strat_circuit_opens_total",
        PY_STRAT_CIRCUIT_OPENS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "py_strat_restarts_total",
        PY_STRAT_RESTARTS_TOTAL.load(Ordering::Relaxed)
    );
    // Gross/Net realized PnL (session aggregates)
    line!(
        "gross_realized_pnl_sol",
        GROSS_REALIZED_PNL_SOL_MICRO.load(Ordering::Relaxed) as f64 / 1_000_000.0
    );
    line!(
        "net_realized_pnl_sol",
        NET_REALIZED_PNL_SOL_MICRO.load(Ordering::Relaxed) as f64 / 1_000_000.0
    );
    // Replay/backtest metrics
    line!("replay_mode", REPLAY_MODE.load(Ordering::Relaxed));
    line!(
        "replay_start_slot",
        REPLAY_START_SLOT_GAUGE.load(Ordering::Relaxed)
    );
    line!(
        "replay_end_slot",
        REPLAY_END_SLOT_GAUGE.load(Ordering::Relaxed)
    );
    line!(
        "replay_slot_ms",
        REPLAY_SLOT_MS_GAUGE.load(Ordering::Relaxed)
    );
    line!("replay_seed", REPLAY_SEED_GAUGE.load(Ordering::Relaxed));
    line!(
        "replay_events_total",
        REPLAY_EVENTS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "replay_slots_seen_total",
        REPLAY_SLOTS_SEEN_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "replay_new_pools_total",
        REPLAY_NEW_POOLS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "replay_price_updates_total",
        REPLAY_PRICE_UPDATES_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "replay_raydium_pools_ingested",
        REPLAY_RAYDIUM_POOLS_INGESTED.load(Ordering::Relaxed)
    );
    line!(
        "replay_orca_pools_ingested",
        REPLAY_ORCA_POOLS_INGESTED.load(Ordering::Relaxed)
    );
    line!(
        "replay_trace_pools_json_ingested",
        REPLAY_TRACE_POOLS_JSON_INGESTED.load(Ordering::Relaxed)
    );
    // Log management metrics
    line!(
        "log_files_cleaned_total",
        LOG_FILES_CLEANED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "log_cleanup_size_bytes_total",
        LOG_CLEANUP_SIZE_BYTES_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "log_files_current_count",
        LOG_FILES_CURRENT_COUNT.load(Ordering::Relaxed)
    );
    line!(
        "log_files_current_size_bytes",
        LOG_FILES_CURRENT_SIZE_BYTES.load(Ordering::Relaxed)
    );
    // Fee percent histogram
    for (i, b) in FEE_PCT_BUCKETS.iter().enumerate() {
        let c = FEE_PCT_BUCKET_COUNTS[i].load(Ordering::Relaxed);
        out.push_str(&format!("fee_percent_bucket{{le=\"{}\"}} {}\n", b, c));
    }
    out.push_str(&format!(
        "fee_percent_bucket{{le=\"+Inf\"}} {}\n",
        FEE_PCT_COUNT.load(Ordering::Relaxed)
    ));
    // Shortfall percent histogram
    for (i, b) in SHORTFALL_PCT_BUCKETS.iter().enumerate() {
        let c = SHORTFALL_PCT_BUCKET_COUNTS[i].load(Ordering::Relaxed);
        out.push_str(&format!("shortfall_percent_bucket{{le=\"{}\"}} {}\n", b, c));
    }
    out.push_str(&format!(
        "shortfall_percent_bucket{{le=\"+Inf\"}} {}\n",
        SHORTFALL_PCT_COUNT.load(Ordering::Relaxed)
    ));
    // Trade return histogram (realized PnL / invested)
    let tr_count = TRADE_RETURN_COUNT.load(Ordering::Relaxed);
    let tr_sum_micro = TRADE_RETURN_SUM_MICRO.load(Ordering::Relaxed);
    for (i, b) in TRADE_RETURN_BUCKETS.iter().enumerate() {
        let c = TRADE_RETURN_BUCKET_COUNTS[i].load(Ordering::Relaxed);
        out.push_str(&format!("trade_return_bucket{{le=\"{}\"}} {}\n", b, c));
    }
    out.push_str(&format!(
        "trade_return_bucket{{le=\"+Inf\"}} {}\n",
        tr_count
    ));
    out.push_str(&format!(
        "trade_return_sum {}\n",
        tr_sum_micro as f64 / 1_000_000.0
    ));
    out.push_str(&format!("trade_return_count {}\n", tr_count));
    line!(
        "ironcrab_sharpe_ratio",
        SHARPE_RATIO_MICRO.load(Ordering::Relaxed) as f64 / 1_000_000.0
    );
    line!(
        "ironcrab_drawdown_pct",
        DRAWDOWN_PCT_MICRO.load(Ordering::Relaxed) as f64 / 1_000_000.0
    );
    out.push_str(&format!(
        "ironcrab_build_info{{version=\"{}\"}} 1\n",
        BUILD_VERSION
    ));
    // Realized PnL (SOL) histogram
    let r_count = REALIZED_PNL_SOL_COUNT.load(Ordering::Relaxed);
    let r_sum_micro = REALIZED_PNL_SOL_SUM_MICRO.load(Ordering::Relaxed);
    for (i, b) in REALIZED_PNL_SOL_BUCKETS.iter().enumerate() {
        let c = REALIZED_PNL_SOL_BUCKET_COUNTS[i].load(Ordering::Relaxed);
        out.push_str(&format!("realized_pnl_sol_bucket{{le=\"{}\"}} {}\n", b, c));
    }
    out.push_str(&format!(
        "realized_pnl_sol_bucket{{le=\"+Inf\"}} {}\n",
        r_count
    ));
    out.push_str(&format!(
        "realized_pnl_sol_sum {}\n",
        r_sum_micro as f64 / 1_000_000.0
    ));
    out.push_str(&format!("realized_pnl_sol_count {}\n", r_count));
    // Histogram exposition (Prometheus classic format)
    let swap_count = SWAP_LATENCY_COUNT.load(Ordering::Relaxed);
    let swap_sum = SWAP_LATENCY_SUM_NS.load(Ordering::Relaxed);
    for (i, bucket) in SWAP_LATENCY_BUCKETS.iter().enumerate() {
        let cum = SWAP_LATENCY_BUCKET_COUNTS[i].load(Ordering::Relaxed);
        out.push_str(&format!(
            "swap_latency_seconds_bucket{{le=\"{}\"}} {}\n",
            (*bucket as f64) / 1e9,
            cum
        ));
    }
    out.push_str(&format!(
        "swap_latency_seconds_sum {}\n",
        (swap_sum as f64) / 1e9
    ));
    out.push_str(&format!("swap_latency_seconds_count {}\n", swap_count));
    Response::builder()
        .status(200)
        .header("Content-Type", "text/plain; version=0.0.4")
        .body(Body::from(out))
        .unwrap()
}

/// Build structured /status JSON for E2E Readiness (market-data, execution-engine)
fn status_response(component: MetricsComponent) -> String {
    let nats = READINESS_NATS_CONNECTED.load(Ordering::Relaxed);
    let control_sub = READINESS_CONTROL_SUB_ACTIVE.load(Ordering::Relaxed);
    let control_resp = READINESS_CONTROL_RESPONSE_SUB_ACTIVE.load(Ordering::Relaxed);
    let jetstream = READINESS_JETSTREAM_READY.load(Ordering::Relaxed);
    let state_paths = READINESS_STATE_PATHS_INITIALIZED.load(Ordering::Relaxed);
    let mode_u64 = READINESS_MODE.load(Ordering::Relaxed);
    let mode_str = match mode_u64 {
        1 => "dry_run",
        2 => "simulate",
        3 => "simulate_only",
        _ => "live",
    };

    let (component_name, ready, missing_checks): (&str, bool, Vec<String>) = match component {
        MetricsComponent::MarketData => {
            // Mode-sensitive: dry_run/simulate intentionally disable parts – don't count as missing
            if mode_u64 == 1 {
                // dry_run: no NATS, no ControlRequests, no JetStream – ready if HTTP serving
                ("market-data", true, vec![])
            } else if mode_u64 == 2 {
                // simulate: fake Geyser, may have NATS for publish; no ControlRequests sub – require only nats
                let mut missing = Vec::new();
                if !nats {
                    missing.push("nats_connected".to_string());
                }
                let ready = nats;
                ("market-data", ready, missing)
            } else {
                let mut missing = Vec::new();
                if !nats {
                    missing.push("nats_connected".to_string());
                }
                if !control_sub {
                    missing.push("control_request_sub_active".to_string());
                }
                if !jetstream {
                    missing.push("jetstream_ready".to_string());
                }
                let ready = nats && control_sub && jetstream;
                ("market-data", ready, missing)
            }
        }
        MetricsComponent::ExecutionEngine => {
            // Mode-sensitive: dry_run/simulate_only don't disable NATS (still consume intents)
            // Only live-mode semantics; no intentionally disabled parts to skip
            let mut missing = Vec::new();
            if !nats {
                missing.push("nats_connected".to_string());
            }
            if !control_sub {
                missing.push("control_request_sub_active".to_string());
            }
            if !control_resp {
                missing.push("control_response_sub_active".to_string());
            }
            if !state_paths {
                missing.push("state_paths_initialized".to_string());
            }
            let ready = nats && control_sub && control_resp && state_paths;
            ("execution-engine", ready, missing)
        }
        MetricsComponent::MomentumBot | MetricsComponent::ArbStrategy => {
            let name = match component {
                MetricsComponent::MomentumBot => "momentum-bot",
                _ => "arb-strategy",
            };
            (
                name,
                nats,
                if nats {
                    vec![]
                } else {
                    vec!["nats_connected".to_string()]
                },
            )
        }
    };

    let reason_not_ready = if !ready && !missing_checks.is_empty() {
        Some(format!("missing: {}", missing_checks.join(", ")))
    } else {
        None
    };

    #[derive(serde::Serialize)]
    struct ReadinessSection {
        nats_connected: bool,
        public_http_ready: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        jetstream_ready: Option<bool>,
        mode: &'static str,
        #[serde(skip_serializing_if = "Option::is_none")]
        reason_not_ready: Option<String>,
        #[serde(skip_serializing_if = "Vec::is_empty")]
        missing_checks: Vec<String>,
    }

    #[derive(serde::Serialize)]
    struct StatusPayload<'a> {
        component: &'a str,
        ready: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        kill_switch_active: Option<bool>,
        readiness: ReadinessSection,
    }

    let kill_switch = match component {
        MetricsComponent::ExecutionEngine => Some(KILL_SWITCH_ACTIVE.load(Ordering::Relaxed)),
        _ => None,
    };

    let jetstream_ready_field = match component {
        MetricsComponent::MarketData => Some(jetstream),
        _ => None,
    };

    let payload = StatusPayload {
        component: component_name,
        ready,
        kill_switch_active: kill_switch,
        readiness: ReadinessSection {
            nats_connected: nats,
            public_http_ready: true,
            jetstream_ready: jetstream_ready_field,
            mode: mode_str,
            reason_not_ready,
            missing_checks,
        },
    };

    serde_json::to_string(&payload)
        .unwrap_or_else(|_| r#"{"component":"unknown","ready":false}"#.to_string())
}

pub async fn serve_metrics(addr: SocketAddr, component: MetricsComponent) -> anyhow::Result<()> {
    let make_svc = make_service_fn(move |_conn| {
        let component = component;
        async move {
            Ok::<_, hyper::Error>(service_fn(move |req: Request<Body>| {
                let component = component;
                async move {
                    let path = req.uri().path();
                    if path == "/metrics" {
                        record_activity();
                        return Ok::<_, hyper::Error>(metrics_response().await);
                    }
                    if path == "/trades" {
                        // Return recent trades as JSON for Grafana
                        let json = get_recent_trades_json();
                        return Ok::<_, hyper::Error>(
                            Response::builder()
                                .status(200)
                                .header("Content-Type", "application/json")
                                .header("Access-Control-Allow-Origin", "*")
                                .body(Body::from(json))
                                .unwrap(),
                        );
                    }
                    if path == "/live" {
                        record_activity();
                        return Ok::<_, hyper::Error>(Response::new(Body::from("ok")));
                    }
                    if path == "/status" {
                        // JSON status: structured readiness for E2E, backward-compat kill_switch_active
                        record_activity();
                        let json = status_response(component);
                        return Ok::<_, hyper::Error>(
                            Response::builder()
                                .status(200)
                                .header("Content-Type", "application/json")
                                .header("Access-Control-Allow-Origin", "*")
                                .body(Body::from(json))
                                .unwrap(),
                        );
                    }
                    if path == "/ready" {
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_secs();
                        let last = LAST_ACTIVITY_TS.load(Ordering::Relaxed);
                        if last > 0 && now.saturating_sub(last) <= 120 {
                            return Ok::<_, hyper::Error>(Response::new(Body::from("ready")));
                        } else {
                            return Ok::<_, hyper::Error>(
                                Response::builder()
                                    .status(503)
                                    .body(Body::from("stale"))
                                    .unwrap(),
                            );
                        }
                    }
                    Ok::<_, hyper::Error>(metrics_response().await)
                }
            }))
        }
    });
    Server::bind(&addr).serve(make_svc).await?;
    Ok(())
}

pub fn update_sharpe(sharpe: f64) {
    let micro = (sharpe * 1_000_000.0).clamp(i64::MIN as f64, i64::MAX as f64) as i64;
    SHARPE_RATIO_MICRO.store(micro, Ordering::Relaxed);
}

pub fn update_drawdown(drawdown_pct: f64) {
    let micro = (drawdown_pct * 1_000_000.0).clamp(0.0, i64::MAX as f64) as i64;
    DRAWDOWN_PCT_MICRO.store(micro, Ordering::Relaxed);
}

pub fn record_activity() {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    LAST_ACTIVITY_TS.store(now, Ordering::Relaxed);
}

#[cfg(test)]
mod momentum_latency_metrics_tests {
    use super::*;
    use serial_test::serial;

    #[test]
    #[serial]
    fn jetstream_poolcache_event_to_ingest_places_sample() {
        reset_momentum_latency_metrics_for_test();
        try_record_momentum_jetstream_poolcache_event_to_ingest_ms(200, 50);
        assert_eq!(
            MOMENTUM_JS_POOLCACHE_EVENT_TO_INGEST_MS_COUNT.load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            MOMENTUM_JS_POOLCACHE_EVENT_TO_INGEST_MS_SUM.load(Ordering::Relaxed),
            150
        );
    }

    #[test]
    #[serial]
    fn event_to_ingest_places_sample_in_expected_bucket() {
        reset_momentum_latency_metrics_for_test();
        try_record_momentum_event_to_ingest_ms(100, 40);
        assert_eq!(MOMENTUM_EVENT_TO_INGEST_MS_COUNT.load(Ordering::Relaxed), 1);
        assert_eq!(MOMENTUM_EVENT_TO_INGEST_MS_SUM.load(Ordering::Relaxed), 60);
        // 60 ms → first bucket with le >= 60 is 100 (index 5 in [1,5,10,25,50,100,...])
        assert_eq!(
            MOMENTUM_EVENT_TO_INGEST_MS_BUCKET_COUNTS[5].load(Ordering::Relaxed),
            1
        );
    }

    #[test]
    #[serial]
    fn invalid_event_ts_does_not_record_histogram_but_bumps_invalid() {
        reset_momentum_latency_metrics_for_test();
        try_record_momentum_event_to_ingest_ms(1_000, 0);
        try_record_momentum_event_to_ingest_ms(500, 600);
        assert_eq!(MOMENTUM_EVENT_TO_INGEST_MS_COUNT.load(Ordering::Relaxed), 0);
        assert_eq!(
            MOMENTUM_LATENCY_EVENT_TS_INVALID_TOTAL.load(Ordering::Relaxed),
            2
        );
    }

    #[test]
    #[serial]
    fn event_to_intent_publish_records_when_explicit_causal_ts_used() {
        reset_momentum_latency_metrics_for_test();
        try_record_momentum_event_to_intent_publish_ms(5_000, 4_000);
        assert_eq!(
            MOMENTUM_EVENT_TO_INTENT_PUBLISH_MS_COUNT.load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            MOMENTUM_EVENT_TO_INTENT_PUBLISH_MS_SUM.load(Ordering::Relaxed),
            1_000
        );
        assert_eq!(
            MOMENTUM_EVENT_TO_INTENT_PUBLISH_MS_BUCKET_COUNTS[8].load(Ordering::Relaxed),
            1
        );
    }

    #[test]
    #[serial]
    fn internal_us_histogram_records_sum_and_bucket() {
        reset_momentum_latency_metrics_for_test();
        record_momentum_signal_eval_us(800);
        assert_eq!(MOMENTUM_SIGNAL_EVAL_US_COUNT.load(Ordering::Relaxed), 1);
        assert_eq!(MOMENTUM_SIGNAL_EVAL_US_SUM.load(Ordering::Relaxed), 800);
        assert_eq!(
            MOMENTUM_SIGNAL_EVAL_US_BUCKET_COUNTS[4].load(Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn internal_subscription_slot_delta_saturates() {
        assert_eq!(
            momentum_internal_subscription_slot_delta_saturating(100, 50),
            50
        );
        assert_eq!(
            momentum_internal_subscription_slot_delta_saturating(50, 100),
            0
        );
        assert_eq!(
            momentum_internal_subscription_slot_delta_saturating(u64::MAX, u64::MAX - 10),
            10
        );
    }

    #[test]
    #[serial]
    fn event_to_ingest_counts_sum_capped_samples_when_latency_exceeds_histogram_sum_cap() {
        reset_momentum_latency_metrics_for_test();
        let now = MOMENTUM_LATENCY_MS_SUM_CAP + 500_000;
        let ts = 100u64;
        try_record_momentum_event_to_ingest_ms(now, ts);
        assert_eq!(MOMENTUM_EVENT_TO_INGEST_MS_COUNT.load(Ordering::Relaxed), 1);
        assert_eq!(
            MOMENTUM_EVENT_TO_INGEST_MS_SUM.load(Ordering::Relaxed),
            MOMENTUM_LATENCY_MS_SUM_CAP
        );
        assert_eq!(
            MOMENTUM_EVENT_TO_INGEST_MS_SUM_CAPPED_SAMPLES_TOTAL.load(Ordering::Relaxed),
            1
        );
    }

    #[test]
    #[serial]
    fn core_market_events_ingest_drain_batch_records_cap_hit_and_streak() {
        reset_momentum_latency_metrics_for_test();
        record_momentum_core_market_events_ingest_drain_batch(10, 48);
        assert_eq!(
            MOMENTUM_CORE_MARKET_EVENTS_INGEST_CONSECUTIVE_CAP_HIT_STREAK.load(Ordering::Relaxed),
            0
        );
        record_momentum_core_market_events_ingest_drain_batch(48, 48);
        record_momentum_core_market_events_ingest_drain_batch(48, 48);
        assert_eq!(
            MOMENTUM_CORE_MARKET_EVENTS_INGEST_DRAIN_CAP_HIT_TOTAL.load(Ordering::Relaxed),
            2
        );
        assert_eq!(
            MOMENTUM_CORE_MARKET_EVENTS_INGEST_CONSECUTIVE_CAP_HIT_STREAK.load(Ordering::Relaxed),
            2
        );
        record_momentum_core_market_events_ingest_drain_batch(10, 48);
        assert_eq!(
            MOMENTUM_CORE_MARKET_EVENTS_INGEST_CONSECUTIVE_CAP_HIT_STREAK.load(Ordering::Relaxed),
            0
        );
    }
}
