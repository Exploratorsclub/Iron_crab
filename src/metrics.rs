use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};
use once_cell::sync::Lazy;
use parking_lot::{Mutex, RwLock};
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Recent trade record for dashboard display
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct RecentTrade {
    pub timestamp_ms: u64,
    /// On-chain block time (UTC) when known; falls back to `timestamp_ms` in consumers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub block_time_unix_ms: Option<u64>,
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
        block_time_unix_ms: None,
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
/// arb-strategy published `ArbTrackRequestsUpdate` payloads (Phase 3).
pub static ARB_TRACK_REQUESTS_MESSAGES_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
/// arb-strategy NATS publish failures for `ArbTrackRequestsUpdate` chunks (Phase 3).
pub static ARB_TRACK_REQUESTS_PUBLISH_FAILED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// arb-strategy successfully published `ArbTrackRequestsUpdate` NATS chunks (Phase 3).
pub static ARB_TRACK_REQUESTS_PUBLISH_CHUNKS_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// market-data applied `ArbTrackRequestsUpdate` from core NATS (Phase 3).
pub static MARKET_DATA_ARB_TRACK_REQUESTS_MESSAGES_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// NATS arb track updates absorbed by the pre-track-worker coalescer (Phase 3).
pub static MARKET_DATA_ARB_TRACK_COALESCED_MESSAGES_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Merged `ApplyArbTrackRequests` batches enqueued to md-track-worker (Phase 3).
pub static MARKET_DATA_ARB_TRACK_COALESCED_BATCHES_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Vaults registered via arb multi-dex admission (no momentum pin).
pub static MARKET_DATA_ARB_REGISTERED_VAULTS_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Account worker dispatch: tracked vault pubkey classified HIGH.
pub static MARKET_DATA_VAULT_HIGH_PRIORITY_DISPATCH_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Arb-multi-dex pin tier evictions when pin budget is full.
pub static MARKET_DATA_ARB_PIN_EVICTIONS_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
/// Pools currently in arb-multi-dex pin tier (pool-level LRU unit).
pub static MARKET_DATA_ARB_PINNED_POOLS_GAUGE: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
/// Arb pin re-add attempts suppressed by post-eviction cooldown.
pub static MARKET_DATA_ARB_PIN_READD_COOLDOWN_SUPPRESSED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Whole-pool arb pin evictions (both vault legs demoted together).
pub static MARKET_DATA_ARB_PIN_POOL_EVICTIONS_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Arb pin evictions due to pin-budget pressure (`reason=budget`).
pub static MARKET_DATA_ARB_PIN_EVICTION_REASON_BUDGET: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Bounded arb-multi-dex coverage reconcile attempts (mint-level backfill).
pub static MARKET_DATA_ARB_RECONCILE_ATTEMPTS_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Pools registered/promoted during arb reconcile backfill.
pub static MARKET_DATA_ARB_RECONCILE_POOLS_REGISTERED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MARKET_DATA_ARB_RECONCILE_SKIPPED_NOT_MULTI_DEX_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MARKET_DATA_ARB_RECONCILE_SKIPPED_PARTIAL_STATE_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MARKET_DATA_ARB_RECONCILE_SKIPPED_NO_COMMON_QUOTE_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MARKET_DATA_ARB_RECONCILE_SKIPPED_COOLDOWN_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MARKET_DATA_ARB_RECONCILE_SKIPPED_BUDGET_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MARKET_DATA_ARB_RECONCILE_SKIPPED_ALREADY_PINNED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MARKET_DATA_ARB_COVERAGE_INDEX_UPDATES_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Current arb-multi-dex pin pubkey budget usage (vault + bin-array legs).
pub static MARKET_DATA_ARB_PIN_BUDGET_USED_GAUGE: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
/// Pools selected for bounded arb reconcile after ranking (per mint pass).
pub static MARKET_DATA_ARB_RECONCILE_SELECTED_POOLS_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Ranked reconcile candidates not selected because of `ARB_RECONCILE_MAX_POOLS_PER_MINT`.
pub static MARKET_DATA_ARB_RECONCILE_UNSELECTED_POOLS_DUE_TO_CAP_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MARKET_DATA_ARB_RECONCILE_SKIPPED_ACTIVE_BUDGET_PROTECTED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MARKET_DATA_ARB_RECONCILE_SKIPPED_OVERSIZED_POOL_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Arb pin evictions of stale (inactive-window) pools under budget pressure.
pub static MARKET_DATA_ARB_PIN_EVICTION_REASON_STALE_BUDGET: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Arb pin add skipped because only activity-protected pools would need eviction.
pub static MARKET_DATA_ARB_PIN_EVICTION_REASON_ACTIVE_PROTECTED: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Arb pin: Geyser reserve registration deferred because LivePoolCache has no row yet.
pub static MARKET_DATA_ARB_PIN_GEYSER_REGISTER_DEFERRED_LIVE_POOL_CACHE_MISS: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Arb pin: Geyser reserve registration deferred because vault/bin register was a no-op.
pub static MARKET_DATA_ARB_PIN_GEYSER_REGISTER_DEFERRED_VAULT_NO_CHANGE: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// Geyser explicit-tracked subscription list syncs coalesced from the TX trade path (debounced flush).
pub static MARKET_DATA_GEYSER_SYNC_BATCH_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
/// Immediate `sync_geyser_tracked_accounts` (momentum pins, wallet tracks, config, mint metadata, etc.).
pub static MARKET_DATA_GEYSER_SYNC_IMMEDIATE_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// 1 while a debounced TX-path Geyser sync is scheduled and not yet flushed; 0 otherwise.
pub static MARKET_DATA_GEYSER_SYNC_PENDING: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
/// md-state batch ended with tracked mutations but no net-new explicit subscription pubkeys.
pub static MARKET_DATA_GEYSER_SYNC_SKIPPED_NO_DELTA_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Debounced Geyser sync flush skipped due to per-second rate cap (startup burst protection).
pub static MARKET_DATA_GEYSER_SYNC_SKIPPED_RATE_LIMIT_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Legacy alias: admitted explicit Geyser pubkey count (mirrors `MARKET_DATA_GEYSER_EXPLICIT_ADMITTED_ACCOUNTS`).
pub static MARKET_DATA_GEYSER_EXPLICIT_SET_SIZE: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
/// PR3: physically admitted explicit pubkey count (FixedCapAdmission SSOT).
pub static MARKET_DATA_GEYSER_EXPLICIT_ADMITTED_ACCOUNTS: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// PR3: 1 when admitted set exceeds cap or convergence failed (fail-closed signal).
pub static MARKET_DATA_GEYSER_EXPLICIT_CAP_OVERFLOW: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Phase 2a: pubkey count in one delta-only Geyser subscribe push.
pub static MARKET_DATA_GEYSER_SUBSCRIBE_DELTA_PUBKEYS: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Phase 2a: track-worker coalesced Geyser push batches completed (500 ms window).
pub static MARKET_DATA_TRACK_REQUEST_COALESCE_BATCHES_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Phase 3 P3: explicit-set snapshot writes completed.
pub static MARKET_DATA_EXPLICIT_SET_SNAPSHOT_WRITE_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Phase 3 P3: explicit-set snapshot write failures (graceful degrade).
pub static MARKET_DATA_EXPLICIT_SET_SNAPSHOT_WRITE_ERRORS_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Phase 3 P3: pubkeys restored from explicit-set snapshot on startup.
pub static MARKET_DATA_EXPLICIT_SET_SNAPSHOT_RESTORE_PUBKEYS: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Phase 3 P3: wall ms for explicit-set snapshot restore on startup.
pub static MARKET_DATA_EXPLICIT_SET_SNAPSHOT_RESTORE_DURATION_MS: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// UnifiedHotPoolRegistry pool count (momentum-only hot pools).
pub static MARKET_DATA_HOT_POOL_REGISTRY_POOLS_MOMENTUM: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// UnifiedHotPoolRegistry pool count (arb-only hot pools).
pub static MARKET_DATA_HOT_POOL_REGISTRY_POOLS_ARB: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// UnifiedHotPoolRegistry pool count (momentum ∩ arb).
pub static MARKET_DATA_HOT_POOL_REGISTRY_POOLS_BOTH: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// BalanceUpdated published from MASTER cache without vault Geyser subscription.
pub static MARKET_DATA_BALANCE_UPDATED_FROM_CACHE_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// P2: JetStream BalanceUpdated from enrichment cache upsert path.
pub static MARKET_DATA_ENRICHMENT_BALANCE_UPDATED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// P2: Core NATS PoolStateUpdate from enrichment cache upsert path.
pub static MARKET_DATA_ENRICHMENT_POOL_STATE_PUBLISH_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// P2: account relevance filter hit via EnrichmentRegistry membership.
pub static MARKET_DATA_ACCOUNT_RELEVANCE_ENRICHMENT_HIT_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// P2: EnrichmentRegistry pool count gauge (deduplicated union).
pub static MARKET_DATA_ENRICHMENT_REGISTRY_POOLS_GAUGE: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// PoolStateUpdate published to Core NATS (bounded `dex` label).
pub static MARKET_DATA_POOL_STATE_PUBLISH_ORCA: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static MARKET_DATA_POOL_STATE_PUBLISH_METEORA_DLMM: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MARKET_DATA_POOL_STATE_PUBLISH_PUMP_AMM: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MARKET_DATA_POOL_STATE_PUBLISH_RAYDIUM: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MARKET_DATA_POOL_STATE_PUBLISH_RAYDIUM_CPMM: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MARKET_DATA_POOL_STATE_PUBLISH_METEORA_CPMM: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MARKET_DATA_POOL_STATE_PUBLISH_PUMPFUN: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MARKET_DATA_POOL_STATE_PUBLISH_OTHER: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
/// VaultBalanceTick skipped because on-chain balance unchanged (H1 evidence).
pub static MARKET_DATA_POOL_STATE_PUBLISH_SKIPPED_BALANCE_UNCHANGED: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// BinArrayUpdate published to Core NATS (Meteora DLMM only).
pub static MARKET_DATA_BIN_ARRAY_PUBLISH_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

/// PR161: merge-task coalesced flush to `combined_tracked` (timer fired after `geyser_sync_batch_ms` quiet window).
pub static MARKET_DATA_GEYSER_MERGE_COALESCED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// PR161: optional urgent merge path (reserved; default coalesce-only).
pub static MARKET_DATA_GEYSER_MERGE_IMMEDIATE_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// PR161: 1 while a debounced merge flush is scheduled; 0 otherwise.
pub static MARKET_DATA_GEYSER_MERGE_PENDING: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

/// PumpSwap `sell` successfully parsed from Geyser (dex_parser → market-data sidefx).
pub static PUMP_AMM_GEYSER_SELL_PARSED_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
/// PumpSwap Geyser-observed SELL that set `sell_layout_ready=true` in LivePoolCache.
pub static PUMP_AMM_GEYSER_SELL_LAYOUT_READY_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

pub fn record_pump_amm_geyser_sell_parsed() {
    PUMP_AMM_GEYSER_SELL_PARSED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

pub fn record_pump_amm_geyser_sell_layout_ready() {
    PUMP_AMM_GEYSER_SELL_LAYOUT_READY_TOTAL.fetch_add(1, Ordering::Relaxed);
}

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
pub fn record_arb_track_requests_messages_total() {
    ARB_TRACK_REQUESTS_MESSAGES_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn record_arb_track_requests_publish_failed_total() {
    ARB_TRACK_REQUESTS_PUBLISH_FAILED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn record_arb_track_requests_publish_chunks_total() {
    ARB_TRACK_REQUESTS_PUBLISH_CHUNKS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn record_market_data_arb_track_requests_messages_total() {
    MARKET_DATA_ARB_TRACK_REQUESTS_MESSAGES_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_arb_track_coalesced_messages_total() {
    MARKET_DATA_ARB_TRACK_COALESCED_MESSAGES_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_arb_track_coalesced_batches_total() {
    MARKET_DATA_ARB_TRACK_COALESCED_BATCHES_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_arb_track_worker_enqueue_dropped_total() {
    MARKET_DATA_ARB_TRACK_WORKER_ENQUEUE_DROPPED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_arb_registered_vaults_total() {
    MARKET_DATA_ARB_REGISTERED_VAULTS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_vault_high_priority_dispatch_total() {
    MARKET_DATA_VAULT_HIGH_PRIORITY_DISPATCH_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_arb_pin_evictions_total() {
    MARKET_DATA_ARB_PIN_EVICTIONS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn set_market_data_arb_pinned_pools_gauge(n: usize) {
    MARKET_DATA_ARB_PINNED_POOLS_GAUGE.store(n as u64, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_arb_pin_readd_cooldown_suppressed_total() {
    MARKET_DATA_ARB_PIN_READD_COOLDOWN_SUPPRESSED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_arb_pin_pool_evictions_total() {
    MARKET_DATA_ARB_PIN_POOL_EVICTIONS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_arb_pin_eviction_reason_budget() {
    MARKET_DATA_ARB_PIN_EVICTION_REASON_BUDGET.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_arb_reconcile_attempts_total() {
    MARKET_DATA_ARB_RECONCILE_ATTEMPTS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_arb_reconcile_pools_registered_total() {
    MARKET_DATA_ARB_RECONCILE_POOLS_REGISTERED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_arb_reconcile_skipped_not_multi_dex_total() {
    MARKET_DATA_ARB_RECONCILE_SKIPPED_NOT_MULTI_DEX_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_arb_reconcile_skipped_partial_state_total() {
    MARKET_DATA_ARB_RECONCILE_SKIPPED_PARTIAL_STATE_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_arb_reconcile_skipped_no_common_quote_total() {
    MARKET_DATA_ARB_RECONCILE_SKIPPED_NO_COMMON_QUOTE_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_arb_reconcile_skipped_cooldown_total() {
    MARKET_DATA_ARB_RECONCILE_SKIPPED_COOLDOWN_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_arb_reconcile_skipped_budget_total() {
    MARKET_DATA_ARB_RECONCILE_SKIPPED_BUDGET_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_arb_reconcile_skipped_already_pinned_total() {
    MARKET_DATA_ARB_RECONCILE_SKIPPED_ALREADY_PINNED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_arb_coverage_index_updates_total() {
    MARKET_DATA_ARB_COVERAGE_INDEX_UPDATES_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn set_market_data_arb_pin_budget_used(n: usize) {
    MARKET_DATA_ARB_PIN_BUDGET_USED_GAUGE.store(n as u64, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_arb_reconcile_selected_pools_total() {
    MARKET_DATA_ARB_RECONCILE_SELECTED_POOLS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn add_market_data_arb_reconcile_unselected_pools_due_to_cap_total(n: u64) {
    if n > 0 {
        MARKET_DATA_ARB_RECONCILE_UNSELECTED_POOLS_DUE_TO_CAP_TOTAL.fetch_add(n, Ordering::Relaxed);
    }
}

#[inline]
pub fn inc_market_data_arb_reconcile_skipped_active_budget_protected_total() {
    MARKET_DATA_ARB_RECONCILE_SKIPPED_ACTIVE_BUDGET_PROTECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_arb_reconcile_skipped_oversized_pool_total() {
    MARKET_DATA_ARB_RECONCILE_SKIPPED_OVERSIZED_POOL_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_arb_pin_eviction_reason_stale_budget() {
    MARKET_DATA_ARB_PIN_EVICTION_REASON_STALE_BUDGET.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_arb_pin_eviction_reason_active_protected() {
    MARKET_DATA_ARB_PIN_EVICTION_REASON_ACTIVE_PROTECTED.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_arb_pin_geyser_register_deferred_total(reason: &str) {
    match reason {
        "live_pool_cache_miss" => {
            MARKET_DATA_ARB_PIN_GEYSER_REGISTER_DEFERRED_LIVE_POOL_CACHE_MISS
                .fetch_add(1, Ordering::Relaxed);
        }
        "vault_register_no_change" => {
            MARKET_DATA_ARB_PIN_GEYSER_REGISTER_DEFERRED_VAULT_NO_CHANGE
                .fetch_add(1, Ordering::Relaxed);
        }
        _ => {}
    }
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
pub fn inc_market_data_geyser_sync_skipped_no_delta_total() {
    MARKET_DATA_GEYSER_SYNC_SKIPPED_NO_DELTA_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_geyser_sync_skipped_rate_limit_total() {
    MARKET_DATA_GEYSER_SYNC_SKIPPED_RATE_LIMIT_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn set_market_data_geyser_explicit_set_size(n: usize) {
    MARKET_DATA_GEYSER_EXPLICIT_SET_SIZE.store(n as u64, Ordering::Relaxed);
}

#[inline]
pub fn set_market_data_geyser_explicit_admitted_accounts(n: usize) {
    MARKET_DATA_GEYSER_EXPLICIT_ADMITTED_ACCOUNTS.store(n as u64, Ordering::Relaxed);
}

#[inline]
pub fn set_market_data_geyser_explicit_cap_overflow(n: usize) {
    MARKET_DATA_GEYSER_EXPLICIT_CAP_OVERFLOW.store(n as u64, Ordering::Relaxed);
}

#[inline]
pub fn record_market_data_geyser_subscribe_delta_pubkeys(n: u64) {
    MARKET_DATA_GEYSER_SUBSCRIBE_DELTA_PUBKEYS.fetch_add(n, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_track_request_coalesce_batches_total() {
    MARKET_DATA_TRACK_REQUEST_COALESCE_BATCHES_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_explicit_set_snapshot_write_total() {
    MARKET_DATA_EXPLICIT_SET_SNAPSHOT_WRITE_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_explicit_set_snapshot_write_errors_total() {
    MARKET_DATA_EXPLICIT_SET_SNAPSHOT_WRITE_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn set_market_data_explicit_set_snapshot_restore_pubkeys(n: u64) {
    MARKET_DATA_EXPLICIT_SET_SNAPSHOT_RESTORE_PUBKEYS.store(n, Ordering::Relaxed);
}

#[inline]
pub fn set_market_data_explicit_set_snapshot_restore_duration_ms(ms: u64) {
    MARKET_DATA_EXPLICIT_SET_SNAPSHOT_RESTORE_DURATION_MS.store(ms, Ordering::Relaxed);
}

#[inline]
pub fn market_data_geyser_explicit_set_size_value() -> u64 {
    MARKET_DATA_GEYSER_EXPLICIT_SET_SIZE.load(Ordering::Relaxed)
}

#[inline]
pub fn set_market_data_hot_pool_registry_pools_gauge(reason: &str, n: usize) {
    let v = n as u64;
    match reason {
        "momentum" => MARKET_DATA_HOT_POOL_REGISTRY_POOLS_MOMENTUM.store(v, Ordering::Relaxed),
        "arb" => MARKET_DATA_HOT_POOL_REGISTRY_POOLS_ARB.store(v, Ordering::Relaxed),
        "both" => MARKET_DATA_HOT_POOL_REGISTRY_POOLS_BOTH.store(v, Ordering::Relaxed),
        _ => {}
    }
}

#[inline]
pub fn inc_market_data_balance_updated_from_cache_total() {
    MARKET_DATA_BALANCE_UPDATED_FROM_CACHE_TOTAL.fetch_add(1, Ordering::Relaxed);
}

pub fn inc_market_data_enrichment_balance_updated_total() {
    MARKET_DATA_ENRICHMENT_BALANCE_UPDATED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

pub fn inc_market_data_enrichment_pool_state_publish_total() {
    MARKET_DATA_ENRICHMENT_POOL_STATE_PUBLISH_TOTAL.fetch_add(1, Ordering::Relaxed);
}

pub fn inc_market_data_account_relevance_enrichment_hit_total() {
    MARKET_DATA_ACCOUNT_RELEVANCE_ENRICHMENT_HIT_TOTAL.fetch_add(1, Ordering::Relaxed);
}

pub fn set_market_data_enrichment_registry_pools_gauge(n: u64) {
    MARKET_DATA_ENRICHMENT_REGISTRY_POOLS_GAUGE.store(n, Ordering::Relaxed);
}

/// Increment bounded `market_data_pool_state_publish_total{dex}`.
pub fn market_data_pool_state_publish_inc(dex: &str) {
    let counter = match dex {
        "orca" => &*MARKET_DATA_POOL_STATE_PUBLISH_ORCA,
        "meteora_dlmm" => &*MARKET_DATA_POOL_STATE_PUBLISH_METEORA_DLMM,
        "pump_amm" => &*MARKET_DATA_POOL_STATE_PUBLISH_PUMP_AMM,
        "raydium" => &*MARKET_DATA_POOL_STATE_PUBLISH_RAYDIUM,
        "raydium_cpmm" => &*MARKET_DATA_POOL_STATE_PUBLISH_RAYDIUM_CPMM,
        "meteora_cpmm" => &*MARKET_DATA_POOL_STATE_PUBLISH_METEORA_CPMM,
        "pumpfun" => &*MARKET_DATA_POOL_STATE_PUBLISH_PUMPFUN,
        _ => &*MARKET_DATA_POOL_STATE_PUBLISH_OTHER,
    };
    counter.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_pool_state_publish_skipped_balance_unchanged_total() {
    MARKET_DATA_POOL_STATE_PUBLISH_SKIPPED_BALANCE_UNCHANGED.fetch_add(1, Ordering::Relaxed);
}

pub fn market_data_bin_array_publish_inc() {
    MARKET_DATA_BIN_ARRAY_PUBLISH_TOTAL.fetch_add(1, Ordering::Relaxed);
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
/// Labeled: `market_data_unparsed_account_dropped_total{reason="legacy_dex_parse_miss"}`.
pub static MARKET_DATA_UNPARSED_ACCOUNT_DROPPED_LEGACY_DEX_PARSE_MISS: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Labeled: `market_data_unparsed_tx_dropped_total{reason="non_dex_transaction"}`.
pub static MARKET_DATA_UNPARSED_TX_DROPPED_NON_DEX_TRANSACTION: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Labeled: `market_data_unparsed_tx_dropped_total{reason="dex_parse_miss"}`.
pub static MARKET_DATA_UNPARSED_TX_DROPPED_DEX_PARSE_MISS: Lazy<AtomicU64> =
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

/// Phase-2b: md-track-worker queue depth (gauge).
pub static MARKET_DATA_TRACK_WORKER_QUEUE_DEPTH: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
/// Phase-2b: momentum active pools enqueue dropped on full track-worker queue.
pub static MARKET_DATA_MOMENTUM_TRACK_WORKER_ENQUEUE_DROPPED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Phase 3: arb track requests enqueue dropped on full track-worker queue.
pub static MARKET_DATA_ARB_TRACK_WORKER_ENQUEUE_DROPPED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// PR4a: momentum pool groups admitted before tracked-map mutation.
pub static MARKET_DATA_MOMENTUM_ADMISSION_ADMITTED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// PR4a: momentum pool groups rejected at admission (no tracked-map mutation).
pub static MARKET_DATA_MOMENTUM_ADMISSION_REJECTED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Scope C: open-position pool pins applied (vault/bin registration succeeded).
pub static MARKET_DATA_OPEN_POSITION_PIN_APPLIED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Scope C: open-position pool pins deferred (LivePoolCache miss; registry row kept).
pub static MARKET_DATA_OPEN_POSITION_PIN_DEFERRED_CACHE_MISS_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// PR4a: arb pool groups admitted before tracked-map mutation.
pub static MARKET_DATA_ARB_ADMISSION_ADMITTED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// PR4a: arb pool groups rejected at admission (no tracked-map mutation).
pub static MARKET_DATA_ARB_ADMISSION_REJECTED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// C1b: arb pins with incomplete vault/bin Geyser registration (gauge).
pub static MARKET_DATA_ARB_PIN_REGISTRATION_INCOMPLETE: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// C1b: arb shed skipped Must-hot (quote_ready / executable) owner groups.
pub static MARKET_DATA_ARB_SHED_SKIPPED_MUST_HOT_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// PR4b: wallet owner groups admitted before tracked-map mutation.
pub static MARKET_DATA_WALLET_ADMISSION_ADMITTED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// PR4b: wallet owner groups rejected at admission (no tracked-map mutation).
pub static MARKET_DATA_WALLET_ADMISSION_REJECTED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// PR4b: tracker mint groups admitted before tracked-map mutation.
pub static MARKET_DATA_TRACKER_ADMISSION_ADMITTED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// PR4b: tracker mint groups rejected at admission (no tracked-map mutation).
pub static MARKET_DATA_TRACKER_ADMISSION_REJECTED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// Bounded track-worker protocol: pending slot depth (gauge).
pub static MARKET_DATA_TRACK_PROTOCOL_PENDING_DEPTH: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Bounded track-worker protocol: in-flight command depth (gauge).
pub static MARKET_DATA_TRACK_PROTOCOL_INFLIGHT_DEPTH: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Queue-full overflow staged into pending for replay (I-MD-5).
pub static MARKET_DATA_TRACK_PROTOCOL_REPLAY_TRIGGERS_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Stale or duplicate revision ignored during drain (idempotent).
pub static MARKET_DATA_TRACK_PROTOCOL_SUPERSEDED_REVISIONS_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Oldest pending slot evicted when pending cap is full (bounded store).
pub static MARKET_DATA_TRACK_PROTOCOL_PENDING_EVICTED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Scope G: track-worker enqueue by coarse command kind (low cardinality).
pub static MARKET_DATA_TRACK_WORKER_ENQUEUE_BY_KIND_TOTAL: Lazy<[AtomicU64; 7]> =
    Lazy::new(|| std::array::from_fn(|_| AtomicU64::new(0)));
/// Scope G: protocol stage (queue-full replay) by coarse command kind.
pub static MARKET_DATA_TRACK_PROTOCOL_STAGE_BY_KIND_TOTAL: Lazy<[AtomicU64; 7]> =
    Lazy::new(|| std::array::from_fn(|_| AtomicU64::new(0)));
/// Scope G: enqueue deduped because equivalent intent already queued or pending.
pub static MARKET_DATA_TRACK_WORKER_ENQUEUE_DEDUPED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Scope H: pending supersede/coalesce avoided a new slot (no eviction).
pub static MARKET_DATA_TRACK_PROTOCOL_PENDING_COALESCED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Scope H: TX ingest skipped TrackMint because mint is already in tracked-membership snapshot.
pub static MARKET_DATA_TRACK_MINT_SKIPPED_ALREADY_TRACKED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Scope H: TrackMint messages absorbed by md-state burst coalesce (before dedupe).
pub static MARKET_DATA_MD_STATE_TRACK_MINT_COALESCE_MESSAGES_IN_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Scope H: TrackMints batches emitted by md-state burst coalesce.
pub static MARKET_DATA_MD_STATE_TRACK_MINT_COALESCE_BATCHES_OUT_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// PR169a: single-writer Geyser tracking actor queue depth (gauge).
pub static MARKET_DATA_GEYSER_TRACKING_QUEUE_DEPTH: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// PR169a: Geyser tracking actor queue full (`try_send` drop).
pub static MARKET_DATA_GEYSER_TRACKING_ENQUEUE_DROPPED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// PR169a: jobs dequeued by the md-state worker (not completion — see `market_data_md_state_bursts_completed_total`).
pub static MARKET_DATA_GEYSER_TRACKING_JOBS_PROCESSED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// PR234: md-state worker burst iterations completed (one per loop pass with work).
pub static MARKET_DATA_MD_STATE_BURSTS_COMPLETED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// PR234: md-state detected stalled while queue near cap (OS-thread liveness).
pub static MARKET_DATA_MD_STATE_STALLS_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
/// PR234: LRU eviction steps executed on md-state.
pub static MARKET_DATA_MD_STATE_EVICT_STEPS_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// PR234: eviction stopped before cap due to per-flush step/time budget.
pub static MARKET_DATA_MD_STATE_EVICT_STEPS_BUDGET_EXHAUSTED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// PR234: Geyser sync flush deferred because eviction still pending (no broadcast).
pub static MARKET_DATA_GEYSER_SYNC_PARTIAL_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
/// PR234: 1 when LRU eviction to cap is incomplete and will resume on next flush.
pub static MARKET_DATA_MD_STATE_EVICT_PENDING: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
/// PR235: md-state worker burst loop in progress (1 = busy).
pub static MARKET_DATA_MD_STATE_BURST_IN_PROGRESS: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// PR235: deferred md-state jobs waiting for next burst.
pub static MARKET_DATA_MD_STATE_DEFERRED_JOBS_LEN: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// PR235: RegisterPoolVaultsFromAccount skipped — vaults already tracked for hot pool.
pub static MARKET_DATA_MD_STATE_REGISTER_SKIPPED_IDEMPOTENT_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// PR235: cold-path discovery deferred while md-state queue under pressure.
pub static MARKET_DATA_DISCOVERY_DEFERRED_MD_STATE_PRESSURE_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

const MARKET_DATA_MD_STATE_SYNC_FLUSH_DURATION_US_BUCKETS: &[u64] = &[
    100, 500, 1_000, 5_000, 10_000, 50_000, 100_000, 500_000, 1_000_000, 5_000_000, 10_000_000,
];
static MARKET_DATA_MD_STATE_SYNC_FLUSH_DURATION_US_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> =
    Lazy::new(|| {
        MARKET_DATA_MD_STATE_SYNC_FLUSH_DURATION_US_BUCKETS
            .iter()
            .map(|_| AtomicU64::new(0))
            .collect()
    });
static MARKET_DATA_MD_STATE_SYNC_FLUSH_DURATION_US_SUM: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
static MARKET_DATA_MD_STATE_SYNC_FLUSH_DURATION_US_COUNT: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// PR237: md-state writer lock wait (tracked_vaults / tracked_bin_arrays / tracked_mints writes).
const MARKET_DATA_MD_STATE_WRITER_WAIT_US_BUCKETS: &[u64] = &[
    1, 10, 50, 100, 500, 1_000, 5_000, 10_000, 50_000, 100_000, 500_000, 1_000_000, 5_000_000,
    10_000_000,
];
static MARKET_DATA_MD_STATE_WRITER_WAIT_US_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> = Lazy::new(|| {
    MARKET_DATA_MD_STATE_WRITER_WAIT_US_BUCKETS
        .iter()
        .map(|_| AtomicU64::new(0))
        .collect()
});
static MARKET_DATA_MD_STATE_WRITER_WAIT_US_SUM: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
static MARKET_DATA_MD_STATE_WRITER_WAIT_US_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

/// PR237: wall unix ms when ingest membership snapshot was last refreshed by md-state.
static MARKET_DATA_TRACKED_MEMBERSHIP_SNAPSHOT_REFRESHED_UNIX_MS: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// PR237: ingest hot-path membership snapshot hits (sanity).
pub static MARKET_DATA_INGEST_MEMBERSHIP_SNAPSHOT_HITS_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// Phase-R-R4: deferred side-effects queue depth (`md-sidefx` OS thread).
pub static MARKET_DATA_MD_SIDEFX_QUEUE_DEPTH: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
/// Phase-R-R4: `md-sidefx` queue full (`try_send` drop).
pub static MARKET_DATA_MD_SIDEFX_ENQUEUE_DROPPED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Scope D: ENRICH sidefx enqueue dropped (cap / headroom).
pub static MARKET_DATA_MD_SIDEFX_ENRICH_ENQUEUE_DROPPED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Scope D: ENRICH JetStream publish skipped (redundant under vault feed / unchanged layout).
pub static MARKET_DATA_MD_SIDEFX_ENRICH_PUBLISH_SKIPPED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Phase-R-R4: jobs processed by the `md-sidefx` worker.
pub static MARKET_DATA_MD_SIDEFX_JOBS_PROCESSED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Phase 1 P1: `DevWalletIdentified` published from TX ingest (PoolCreated / trade fast-path).
pub static MARKET_DATA_DEVWALLET_TX_PUBLISHED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Phase 1 P2: `DevWalletIdentified` published from bonding-curve account path.
pub static MARKET_DATA_DEVWALLET_BONDING_PATH_TOTAL: Lazy<AtomicU64> =
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
#[allow(dead_code)]
pub fn inc_market_data_unparsed_tx_dropped_total() {
    MARKET_DATA_UNPARSED_TX_DROPPED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
#[allow(dead_code)]
pub fn inc_market_data_unparsed_account_dropped_total() {
    MARKET_DATA_UNPARSED_ACCOUNT_DROPPED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Unparsed account drop reason for `market_data_unparsed_account_dropped_total{reason=...}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketDataUnparsedAccountDropReason {
    LegacyDexParseMiss,
}

impl MarketDataUnparsedAccountDropReason {
    #[inline]
    pub fn as_prometheus_label(self) -> &'static str {
        match self {
            Self::LegacyDexParseMiss => "legacy_dex_parse_miss",
        }
    }
}

/// Unparsed TX drop reason for `market_data_unparsed_tx_dropped_total{reason=...}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketDataUnparsedTxDropReason {
    NonDexTransaction,
    DexParseMiss,
}

impl MarketDataUnparsedTxDropReason {
    #[inline]
    pub fn as_prometheus_label(self) -> &'static str {
        match self {
            Self::NonDexTransaction => "non_dex_transaction",
            Self::DexParseMiss => "dex_parse_miss",
        }
    }
}

#[inline]
pub fn record_market_data_unparsed_account_dropped(reason: MarketDataUnparsedAccountDropReason) {
    let counter = match reason {
        MarketDataUnparsedAccountDropReason::LegacyDexParseMiss => {
            &*MARKET_DATA_UNPARSED_ACCOUNT_DROPPED_LEGACY_DEX_PARSE_MISS
        }
    };
    counter.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn record_market_data_unparsed_tx_dropped(reason: MarketDataUnparsedTxDropReason) {
    let counter = match reason {
        MarketDataUnparsedTxDropReason::NonDexTransaction => {
            &*MARKET_DATA_UNPARSED_TX_DROPPED_NON_DEX_TRANSACTION
        }
        MarketDataUnparsedTxDropReason::DexParseMiss => {
            &*MARKET_DATA_UNPARSED_TX_DROPPED_DEX_PARSE_MISS
        }
    };
    counter.fetch_add(1, Ordering::Relaxed);
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
pub fn set_market_data_track_worker_queue_depth(depth: usize) {
    MARKET_DATA_TRACK_WORKER_QUEUE_DEPTH.store(depth as u64, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_momentum_track_worker_enqueue_dropped_total() {
    MARKET_DATA_MOMENTUM_TRACK_WORKER_ENQUEUE_DROPPED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_momentum_admission_admitted_total() {
    MARKET_DATA_MOMENTUM_ADMISSION_ADMITTED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_momentum_admission_rejected_total() {
    MARKET_DATA_MOMENTUM_ADMISSION_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_open_position_pin_applied_total() {
    MARKET_DATA_OPEN_POSITION_PIN_APPLIED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_open_position_pin_deferred_cache_miss_total() {
    MARKET_DATA_OPEN_POSITION_PIN_DEFERRED_CACHE_MISS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_arb_admission_admitted_total() {
    MARKET_DATA_ARB_ADMISSION_ADMITTED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_arb_admission_rejected_total() {
    MARKET_DATA_ARB_ADMISSION_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn set_market_data_arb_pin_registration_incomplete_gauge(n: usize) {
    MARKET_DATA_ARB_PIN_REGISTRATION_INCOMPLETE.store(n as u64, Ordering::Relaxed);
}

#[inline]
pub fn add_market_data_arb_shed_skipped_must_hot_total(n: u64) {
    if n > 0 {
        MARKET_DATA_ARB_SHED_SKIPPED_MUST_HOT_TOTAL.fetch_add(n, Ordering::Relaxed);
    }
}

#[inline]
pub fn inc_market_data_wallet_admission_admitted_total() {
    MARKET_DATA_WALLET_ADMISSION_ADMITTED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_wallet_admission_rejected_total() {
    MARKET_DATA_WALLET_ADMISSION_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_tracker_admission_admitted_total() {
    MARKET_DATA_TRACKER_ADMISSION_ADMITTED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_tracker_admission_rejected_total() {
    MARKET_DATA_TRACKER_ADMISSION_REJECTED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn set_market_data_track_protocol_pending_depth(depth: usize) {
    MARKET_DATA_TRACK_PROTOCOL_PENDING_DEPTH.store(depth as u64, Ordering::Relaxed);
}

#[inline]
pub fn set_market_data_track_protocol_inflight_depth(depth: usize) {
    MARKET_DATA_TRACK_PROTOCOL_INFLIGHT_DEPTH.store(depth as u64, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_track_protocol_replay_triggers_total() {
    MARKET_DATA_TRACK_PROTOCOL_REPLAY_TRIGGERS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_track_protocol_superseded_revisions_total() {
    MARKET_DATA_TRACK_PROTOCOL_SUPERSEDED_REVISIONS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_track_protocol_pending_evicted_total() {
    MARKET_DATA_TRACK_PROTOCOL_PENDING_EVICTED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_track_worker_enqueue_by_kind(kind_index: usize) {
    if kind_index < MARKET_DATA_TRACK_WORKER_ENQUEUE_BY_KIND_TOTAL.len() {
        MARKET_DATA_TRACK_WORKER_ENQUEUE_BY_KIND_TOTAL[kind_index].fetch_add(1, Ordering::Relaxed);
    }
}

#[inline]
pub fn inc_market_data_track_protocol_stage_by_kind(kind_index: usize) {
    if kind_index < MARKET_DATA_TRACK_PROTOCOL_STAGE_BY_KIND_TOTAL.len() {
        MARKET_DATA_TRACK_PROTOCOL_STAGE_BY_KIND_TOTAL[kind_index].fetch_add(1, Ordering::Relaxed);
    }
}

#[inline]
pub fn inc_market_data_track_worker_enqueue_deduped_total() {
    MARKET_DATA_TRACK_WORKER_ENQUEUE_DEDUPED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_track_protocol_pending_coalesced_total() {
    MARKET_DATA_TRACK_PROTOCOL_PENDING_COALESCED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_track_mint_skipped_already_tracked_total() {
    MARKET_DATA_TRACK_MINT_SKIPPED_ALREADY_TRACKED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_md_state_track_mint_coalesce_messages_in(n: u64) {
    MARKET_DATA_MD_STATE_TRACK_MINT_COALESCE_MESSAGES_IN_TOTAL.fetch_add(n, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_md_state_track_mint_coalesce_batches_out() {
    MARKET_DATA_MD_STATE_TRACK_MINT_COALESCE_BATCHES_OUT_TOTAL.fetch_add(1, Ordering::Relaxed);
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
pub fn market_data_geyser_tracking_jobs_processed_value() -> u64 {
    MARKET_DATA_GEYSER_TRACKING_JOBS_PROCESSED_TOTAL.load(Ordering::Relaxed)
}

#[inline]
pub fn market_data_geyser_tracking_enqueue_dropped_value() -> u64 {
    MARKET_DATA_GEYSER_TRACKING_ENQUEUE_DROPPED_TOTAL.load(Ordering::Relaxed)
}

#[inline]
pub fn market_data_md_state_bursts_completed_value() -> u64 {
    MARKET_DATA_MD_STATE_BURSTS_COMPLETED_TOTAL.load(Ordering::Relaxed)
}

#[inline]
pub fn inc_market_data_md_state_bursts_completed_total() {
    MARKET_DATA_MD_STATE_BURSTS_COMPLETED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn record_market_data_md_state_stall() {
    MARKET_DATA_MD_STATE_STALLS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_md_state_evict_steps_total(n: u64) {
    if n > 0 {
        MARKET_DATA_MD_STATE_EVICT_STEPS_TOTAL.fetch_add(n, Ordering::Relaxed);
    }
}

#[inline]
pub fn inc_market_data_md_state_evict_steps_budget_exhausted_total() {
    MARKET_DATA_MD_STATE_EVICT_STEPS_BUDGET_EXHAUSTED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_geyser_sync_partial_total() {
    MARKET_DATA_GEYSER_SYNC_PARTIAL_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn set_market_data_md_state_evict_pending(pending: bool) {
    MARKET_DATA_MD_STATE_EVICT_PENDING.store(u64::from(pending), Ordering::Relaxed);
}

#[inline]
pub fn set_market_data_md_state_burst_in_progress(in_progress: bool) {
    MARKET_DATA_MD_STATE_BURST_IN_PROGRESS.store(u64::from(in_progress), Ordering::Relaxed);
}

#[inline]
pub fn set_market_data_md_state_deferred_jobs_len(len: usize) {
    MARKET_DATA_MD_STATE_DEFERRED_JOBS_LEN.store(len as u64, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_md_state_register_skipped_idempotent_total() {
    MARKET_DATA_MD_STATE_REGISTER_SKIPPED_IDEMPOTENT_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_discovery_deferred_md_state_pressure_total() {
    MARKET_DATA_DISCOVERY_DEFERRED_MD_STATE_PRESSURE_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn record_market_data_md_state_sync_flush_duration_us(us: u64) {
    record_histogram_u64_into(
        MARKET_DATA_MD_STATE_SYNC_FLUSH_DURATION_US_BUCKETS,
        &MARKET_DATA_MD_STATE_SYNC_FLUSH_DURATION_US_BUCKET_COUNTS,
        &MARKET_DATA_MD_STATE_SYNC_FLUSH_DURATION_US_SUM,
        &MARKET_DATA_MD_STATE_SYNC_FLUSH_DURATION_US_COUNT,
        us,
        u64::MAX,
    );
}

#[inline]
pub fn record_market_data_md_state_writer_wait_us(us: u64) {
    record_histogram_u64_into(
        MARKET_DATA_MD_STATE_WRITER_WAIT_US_BUCKETS,
        &MARKET_DATA_MD_STATE_WRITER_WAIT_US_BUCKET_COUNTS,
        &MARKET_DATA_MD_STATE_WRITER_WAIT_US_SUM,
        &MARKET_DATA_MD_STATE_WRITER_WAIT_US_COUNT,
        us,
        u64::MAX,
    );
}

#[inline]
pub fn touch_market_data_tracked_membership_snapshot_refresh() {
    MARKET_DATA_TRACKED_MEMBERSHIP_SNAPSHOT_REFRESHED_UNIX_MS
        .store(wall_clock_unix_ms_now(), Ordering::Relaxed);
}

#[inline]
pub fn market_data_tracked_membership_snapshot_age_ms() -> u64 {
    let refreshed =
        MARKET_DATA_TRACKED_MEMBERSHIP_SNAPSHOT_REFRESHED_UNIX_MS.load(Ordering::Relaxed);
    if refreshed == 0 {
        return 0;
    }
    wall_clock_unix_ms_now().saturating_sub(refreshed)
}

#[inline]
pub fn inc_market_data_ingest_membership_snapshot_hits_total() {
    MARKET_DATA_INGEST_MEMBERSHIP_SNAPSHOT_HITS_TOTAL.fetch_add(1, Ordering::Relaxed);
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
pub fn inc_market_data_md_sidefx_enrich_enqueue_dropped_total() {
    MARKET_DATA_MD_SIDEFX_ENRICH_ENQUEUE_DROPPED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_md_sidefx_enrich_publish_skipped_total() {
    MARKET_DATA_MD_SIDEFX_ENRICH_PUBLISH_SKIPPED_TOTAL.fetch_add(1, Ordering::Relaxed);
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

static MARKET_DATA_ACCOUNT_CHANNEL_LAG_MS_EXEC_HOT_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> =
    Lazy::new(|| {
        MARKET_DATA_GEYSER_TO_PUBLISH_MS_BUCKETS
            .iter()
            .map(|_| AtomicU64::new(0))
            .collect()
    });
static MARKET_DATA_ACCOUNT_CHANNEL_LAG_MS_EXEC_HOT_SUM: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
static MARKET_DATA_ACCOUNT_CHANNEL_LAG_MS_EXEC_HOT_COUNT: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

static MARKET_DATA_ACCOUNT_CHANNEL_LAG_MS_ENRICH_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> =
    Lazy::new(|| {
        MARKET_DATA_GEYSER_TO_PUBLISH_MS_BUCKETS
            .iter()
            .map(|_| AtomicU64::new(0))
            .collect()
    });
static MARKET_DATA_ACCOUNT_CHANNEL_LAG_MS_ENRICH_SUM: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
static MARKET_DATA_ACCOUNT_CHANNEL_LAG_MS_ENRICH_COUNT: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// Account ingest class counter `market_data_account_updates_total{class=...}`.
pub static MARKET_DATA_ACCOUNT_UPDATES_TOTAL_EXEC_HOT: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MARKET_DATA_ACCOUNT_UPDATES_TOTAL_ENRICH: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MARKET_DATA_ACCOUNT_UPDATES_TOTAL_DROP: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

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
/// Legacy gauge: max of EXEC_HOT and ENRICH recv depths (Scope L1 split).
pub static MARKET_DATA_ACCOUNT_BROADCAST_QUEUE_DEPTH: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Scope L1: EXEC_HOT-dedicated broadcast receiver backlog.
pub static MARKET_DATA_ACCOUNT_BROADCAST_QUEUE_DEPTH_EXEC_HOT: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Scope L1: ENRICH-dedicated broadcast receiver backlog.
pub static MARKET_DATA_ACCOUNT_BROADCAST_QUEUE_DEPTH_ENRICH: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MARKET_DATA_ACCOUNT_BROADCAST_LAGGED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Scope L1: skipped messages on the EXEC_HOT broadcast receiver (`RecvError::Lagged`).
pub static MARKET_DATA_ACCOUNT_BROADCAST_LAGGED_TOTAL_EXEC_HOT: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Scope L1: skipped messages on the ENRICH broadcast receiver (`RecvError::Lagged`).
pub static MARKET_DATA_ACCOUNT_BROADCAST_LAGGED_TOTAL_ENRICH: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// Configured account worker pool size (total EXEC_HOT + ENRICH; backward-compat ops gauge).
pub static MARKET_DATA_ACCOUNT_WORKER_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
/// Scope J3: EXEC_HOT-only worker shards (HIGH queue handlers).
pub static MARKET_DATA_ACCOUNT_EXEC_HOT_WORKER_COUNT: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Scope J3: ENRICH-only worker shards (LOW queue + ingress/coalesce handlers).
pub static MARKET_DATA_ACCOUNT_ENRICH_WORKER_COUNT: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// Account ingest: messages accepted into per-worker `tokio::mpsc` queues (after recv, before worker `recv`).
pub static MARKET_DATA_ACCOUNT_WORKER_QUEUE_DEPTH: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// Account ingest: per-shard HIGH-priority `mpsc` depth (discovered pools, pinned, wallet-tracked curves).
pub static MARKET_DATA_ACCOUNT_HIGH_PRIORITY_QUEUE_DEPTH: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// Account ingest: per-shard LOW-priority `mpsc` depth (remaining relevant account updates).
pub static MARKET_DATA_ACCOUNT_LOW_PRIORITY_QUEUE_DEPTH: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// ENRICH coalesce: replaced an existing pending update for the same pubkey (latest-wins).
pub static MARKET_DATA_ACCOUNT_ENRICH_COALESCE_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// ENRICH enqueue dropped after coalesce map at cap (oldest evicted or try_send exhausted).
pub static MARKET_DATA_ACCOUNT_ENRICH_ENQUEUE_DROPPED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// ENRICH ingress `mpsc` depth (recv → enrich-dispatch task, per-shard sum).
pub static MARKET_DATA_ACCOUNT_ENRICH_INGRESS_QUEUE_DEPTH: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// ENRICH ingress `try_send` failed (channel full); recv did not block on coalesce mutex.
pub static MARKET_DATA_ACCOUNT_ENRICH_DISPATCH_CONTENDED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// EXEC_HOT HIGH `try_send` dropped (channel full); recv did not block on worker backlog.
pub static MARKET_DATA_ACCOUNT_HIGH_ENQUEUE_DROPPED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// Scope L2: soft enrich shed active under EXEC_HOT broadcast pressure (0/1).
pub static MARKET_DATA_EXEC_HOT_SHED_SOFT_ACTIVE: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
/// Scope L2: lock-free flag read from ENRICH broadcast recv (drain-only, no classify/enqueue).
pub static MARKET_DATA_ENRICH_SHED_ACTIVE: AtomicBool = AtomicBool::new(false);
/// Scope L2: ENRICH recv messages dropped while soft shed is active.
pub static MARKET_DATA_ACCOUNT_ENRICH_SHED_DROPPED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Scope L2: hard shed controller steps (tracker group eviction under EXEC_HOT pressure).
pub static MARKET_DATA_EXEC_HOT_HARD_SHED_STEPS_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// Scope L2: tracker owner groups evicted by hard shed.
pub static MARKET_DATA_EXEC_HOT_HARD_SHED_GROUPS_EVICTED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// Scope L2b: EXEC_HOT pressure shed tier (0=none, 1=tracker, 2=momentum, 3=arb).
pub static MARKET_DATA_EXEC_HOT_SHED_TIER: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

/// Scope L2b: per-tier hard shed steps (`tier` = tracker|momentum|arb).
pub static MARKET_DATA_EXEC_HOT_HARD_SHED_STEPS_TRACKER: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MARKET_DATA_EXEC_HOT_HARD_SHED_STEPS_MOMENTUM: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MARKET_DATA_EXEC_HOT_HARD_SHED_STEPS_ARB: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// Scope L2b: per-tier owner groups evicted by hard shed.
pub static MARKET_DATA_EXEC_HOT_HARD_SHED_GROUPS_TRACKER: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MARKET_DATA_EXEC_HOT_HARD_SHED_GROUPS_MOMENTUM: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MARKET_DATA_EXEC_HOT_HARD_SHED_GROUPS_ARB: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// Scope L2b: last shed result groups evicted (feedback for controller idle escalation).
pub static MARKET_DATA_EXEC_HOT_LAST_SHED_GROUPS_TRACKER: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MARKET_DATA_EXEC_HOT_LAST_SHED_GROUPS_MOMENTUM: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MARKET_DATA_EXEC_HOT_LAST_SHED_GROUPS_ARB: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// Scope L2b: lock-free admit suppress flags (controller sets, track-worker reads).
pub static MARKET_DATA_EXEC_HOT_TRACKER_ADMIT_SUPPRESS: AtomicBool = AtomicBool::new(false);
pub static MARKET_DATA_EXEC_HOT_MOMENTUM_ADMIT_SUPPRESS: AtomicBool = AtomicBool::new(false);
pub static MARKET_DATA_EXEC_HOT_ARB_ADMIT_SUPPRESS: AtomicBool = AtomicBool::new(false);

/// Scope L2b: admits rejected due to EXEC_HOT pressure suppress.
pub static MARKET_DATA_EXEC_HOT_PRESSURE_ADMIT_REJECTED_TRACKER: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MARKET_DATA_EXEC_HOT_PRESSURE_ADMIT_REJECTED_MOMENTUM: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MARKET_DATA_EXEC_HOT_PRESSURE_ADMIT_REJECTED_ARB: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// Scope L2c-MD: rolling EXEC_HOT channel lag estimates (ms), updated from recv samples.
pub static MARKET_DATA_EXEC_HOT_LAG_P50_EST_MS: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static MARKET_DATA_EXEC_HOT_LAG_P99_EST_MS: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
/// Scope L2c-MD: controller hysteresis output (0/1).
pub static MARKET_DATA_EXEC_HOT_LAG_ALARM: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

/// Scope L2c-MD: hard shed steps attributed to lag (vs depth) per tier.
pub static MARKET_DATA_EXEC_HOT_HARD_SHED_STEPS_TRACKER_LAG: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MARKET_DATA_EXEC_HOT_HARD_SHED_STEPS_MOMENTUM_LAG: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MARKET_DATA_EXEC_HOT_HARD_SHED_STEPS_ARB_LAG: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

const EXEC_HOT_LAG_RING_CAP: usize = 64;
static EXEC_HOT_LAG_RING: Lazy<[AtomicU64; EXEC_HOT_LAG_RING_CAP]> =
    Lazy::new(|| std::array::from_fn(|_| AtomicU64::new(0)));
static EXEC_HOT_LAG_RING_WRITE_IDX: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
static EXEC_HOT_LAG_SAMPLE_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
static EXEC_HOT_LAG_EWMA_P50_MS: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
static EXEC_HOT_LAG_EWMA_P99_MS: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

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

/// Early-drop reason for `market_data_account_early_drop_total{reason=...}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketDataAccountEarlyDropReason {
    NonDexNonMembership,
    DexPoolNotEnrichment,
}

impl MarketDataAccountEarlyDropReason {
    #[inline]
    pub fn as_prometheus_label(self) -> &'static str {
        match self {
            Self::NonDexNonMembership => "non_dex_non_membership",
            Self::DexPoolNotEnrichment => "dex_pool_not_enrichment",
        }
    }
}

/// Account ingest: cheap relevance filter discarded the update before `handle_geyser_account` body.
pub static MARKET_DATA_ACCOUNT_EARLY_DROP_NON_DEX_NON_MEMBERSHIP: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MARKET_DATA_ACCOUNT_EARLY_DROP_DEX_POOL_NOT_ENRICHMENT: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

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

/// Successful account broadcast `recv` iterations (drain rate via `rate(...)`).
pub static MARKET_DATA_ACCOUNT_RECV_ITERATIONS_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// Wall microseconds for `classify_account_geyser_update` in the account recv task.
const MARKET_DATA_ACCOUNT_RECV_CLASSIFY_DURATION_US_BUCKETS: &[u64] = &[
    1, 5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000,
    500_000,
];

static MARKET_DATA_ACCOUNT_RECV_CLASSIFY_DURATION_US_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> =
    Lazy::new(|| {
        MARKET_DATA_ACCOUNT_RECV_CLASSIFY_DURATION_US_BUCKETS
            .iter()
            .map(|_| AtomicU64::new(0))
            .collect()
    });
static MARKET_DATA_ACCOUNT_RECV_CLASSIFY_DURATION_US_SUM: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
static MARKET_DATA_ACCOUNT_RECV_CLASSIFY_DURATION_US_COUNT: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// Wall microseconds for HIGH-path `mpsc::send().await` in the account recv task.
const MARKET_DATA_ACCOUNT_RECV_HIGH_ENQUEUE_DURATION_US_BUCKETS: &[u64] = &[
    50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000,
    1_000_000, 2_000_000,
];

static MARKET_DATA_ACCOUNT_RECV_HIGH_ENQUEUE_DURATION_US_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> =
    Lazy::new(|| {
        MARKET_DATA_ACCOUNT_RECV_HIGH_ENQUEUE_DURATION_US_BUCKETS
            .iter()
            .map(|_| AtomicU64::new(0))
            .collect()
    });
static MARKET_DATA_ACCOUNT_RECV_HIGH_ENQUEUE_DURATION_US_SUM: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
static MARKET_DATA_ACCOUNT_RECV_HIGH_ENQUEUE_DURATION_US_COUNT: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// Wall microseconds for ENRICH-path `account_enrich_ingress_try_send` in the account recv task.
const MARKET_DATA_ACCOUNT_RECV_ENRICH_INGRESS_DURATION_US_BUCKETS: &[u64] = &[
    1, 5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000,
];

static MARKET_DATA_ACCOUNT_RECV_ENRICH_INGRESS_DURATION_US_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> =
    Lazy::new(|| {
        MARKET_DATA_ACCOUNT_RECV_ENRICH_INGRESS_DURATION_US_BUCKETS
            .iter()
            .map(|_| AtomicU64::new(0))
            .collect()
    });
static MARKET_DATA_ACCOUNT_RECV_ENRICH_INGRESS_DURATION_US_SUM: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
static MARKET_DATA_ACCOUNT_RECV_ENRICH_INGRESS_DURATION_US_COUNT: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// Wall microseconds per account recv loop iteration (`recv` → dispatch done).
const MARKET_DATA_ACCOUNT_RECV_ITERATION_DURATION_US_BUCKETS: &[u64] = &[
    50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000,
    1_000_000, 2_000_000,
];

static MARKET_DATA_ACCOUNT_RECV_ITERATION_DURATION_US_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> =
    Lazy::new(|| {
        MARKET_DATA_ACCOUNT_RECV_ITERATION_DURATION_US_BUCKETS
            .iter()
            .map(|_| AtomicU64::new(0))
            .collect()
    });
static MARKET_DATA_ACCOUNT_RECV_ITERATION_DURATION_US_SUM: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
static MARKET_DATA_ACCOUNT_RECV_ITERATION_DURATION_US_COUNT: Lazy<AtomicU64> =
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

/// Per-class account channel lag (`class="exec_hot"` | `class="enrich"`).
#[inline]
pub fn record_market_data_account_channel_lag_ms_for_class(
    class: &str,
    grpc_recv_at: Instant,
    recv_at: Instant,
) {
    let ms = recv_at
        .saturating_duration_since(grpc_recv_at)
        .as_millis()
        .min(u128::from(u64::MAX)) as u64;
    match class {
        "exec_hot" => {
            record_histogram_u64_into(
                MARKET_DATA_GEYSER_TO_PUBLISH_MS_BUCKETS,
                &MARKET_DATA_ACCOUNT_CHANNEL_LAG_MS_EXEC_HOT_BUCKET_COUNTS,
                &MARKET_DATA_ACCOUNT_CHANNEL_LAG_MS_EXEC_HOT_SUM,
                &MARKET_DATA_ACCOUNT_CHANNEL_LAG_MS_EXEC_HOT_COUNT,
                ms,
                MARKET_DATA_LATENCY_MS_SUM_CAP,
            );
            record_exec_hot_channel_lag_sample(ms);
        }
        "enrich" => record_histogram_u64_into(
            MARKET_DATA_GEYSER_TO_PUBLISH_MS_BUCKETS,
            &MARKET_DATA_ACCOUNT_CHANNEL_LAG_MS_ENRICH_BUCKET_COUNTS,
            &MARKET_DATA_ACCOUNT_CHANNEL_LAG_MS_ENRICH_SUM,
            &MARKET_DATA_ACCOUNT_CHANNEL_LAG_MS_ENRICH_COUNT,
            ms,
            MARKET_DATA_LATENCY_MS_SUM_CAP,
        ),
        _ => {}
    }
}

/// Increment `market_data_account_updates_total{class=...}`.
#[inline]
pub fn inc_market_data_account_updates_total(class: &str) {
    let counter = match class {
        "exec_hot" => &*MARKET_DATA_ACCOUNT_UPDATES_TOTAL_EXEC_HOT,
        "enrich" => &*MARKET_DATA_ACCOUNT_UPDATES_TOTAL_ENRICH,
        "drop" => &*MARKET_DATA_ACCOUNT_UPDATES_TOTAL_DROP,
        _ => return,
    };
    counter.fetch_add(1, Ordering::Relaxed);
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

/// Scope L1: per-recv-path broadcast backlog (`class` = `exec_hot` | `enrich`).
#[inline]
pub fn set_market_data_account_broadcast_queue_depth_for_class(class: &str, depth: usize) {
    match class {
        "exec_hot" => {
            MARKET_DATA_ACCOUNT_BROADCAST_QUEUE_DEPTH_EXEC_HOT
                .store(depth as u64, Ordering::Relaxed);
        }
        "enrich" => {
            MARKET_DATA_ACCOUNT_BROADCAST_QUEUE_DEPTH_ENRICH.store(depth as u64, Ordering::Relaxed);
        }
        _ => return,
    }
    refresh_market_data_account_broadcast_queue_depth_legacy();
}

#[inline]
fn refresh_market_data_account_broadcast_queue_depth_legacy() {
    let exec_hot = MARKET_DATA_ACCOUNT_BROADCAST_QUEUE_DEPTH_EXEC_HOT.load(Ordering::Relaxed);
    let enrich = MARKET_DATA_ACCOUNT_BROADCAST_QUEUE_DEPTH_ENRICH.load(Ordering::Relaxed);
    MARKET_DATA_ACCOUNT_BROADCAST_QUEUE_DEPTH.store(exec_hot.max(enrich), Ordering::Relaxed);
}

#[inline]
pub fn record_market_data_account_broadcast_lagged(skipped_messages: u64) {
    if skipped_messages > 0 {
        MARKET_DATA_ACCOUNT_BROADCAST_LAGGED_TOTAL.fetch_add(skipped_messages, Ordering::Relaxed);
    }
}

/// Scope L1: per-recv-path `RecvError::Lagged` (`class` = `exec_hot` | `enrich`).
#[inline]
pub fn record_market_data_account_broadcast_lagged_for_class(class: &str, skipped_messages: u64) {
    if skipped_messages == 0 {
        return;
    }
    match class {
        "exec_hot" => {
            MARKET_DATA_ACCOUNT_BROADCAST_LAGGED_TOTAL_EXEC_HOT
                .fetch_add(skipped_messages, Ordering::Relaxed);
        }
        "enrich" => {
            MARKET_DATA_ACCOUNT_BROADCAST_LAGGED_TOTAL_ENRICH
                .fetch_add(skipped_messages, Ordering::Relaxed);
        }
        _ => return,
    }
    MARKET_DATA_ACCOUNT_BROADCAST_LAGGED_TOTAL.fetch_add(skipped_messages, Ordering::Relaxed);
}

#[inline]
pub fn set_market_data_account_worker_count(count: usize) {
    MARKET_DATA_ACCOUNT_WORKER_COUNT.store(count as u64, Ordering::Relaxed);
}

#[inline]
pub fn set_market_data_account_exec_hot_worker_count(count: usize) {
    MARKET_DATA_ACCOUNT_EXEC_HOT_WORKER_COUNT.store(count as u64, Ordering::Relaxed);
}

#[inline]
pub fn set_market_data_account_enrich_worker_count(count: usize) {
    MARKET_DATA_ACCOUNT_ENRICH_WORKER_COUNT.store(count as u64, Ordering::Relaxed);
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

#[inline]
pub fn inc_market_data_account_enrich_coalesce_total() {
    MARKET_DATA_ACCOUNT_ENRICH_COALESCE_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_account_enrich_enqueue_dropped_total() {
    MARKET_DATA_ACCOUNT_ENRICH_ENQUEUE_DROPPED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_account_enrich_ingress_queue_depth() {
    MARKET_DATA_ACCOUNT_ENRICH_INGRESS_QUEUE_DEPTH.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn dec_market_data_account_enrich_ingress_queue_depth() {
    MARKET_DATA_ACCOUNT_ENRICH_INGRESS_QUEUE_DEPTH.fetch_sub(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_account_enrich_dispatch_contended_total() {
    MARKET_DATA_ACCOUNT_ENRICH_DISPATCH_CONTENDED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_account_high_enqueue_dropped_total() {
    MARKET_DATA_ACCOUNT_HIGH_ENQUEUE_DROPPED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Scope L2: true when ENRICH recv should drain-only (no classify/enqueue).
#[inline]
pub fn market_data_enrich_shed_active() -> bool {
    MARKET_DATA_ENRICH_SHED_ACTIVE.load(Ordering::Relaxed)
}

/// Scope L2: set soft enrich shed active (also updates prometheus gauge).
#[inline]
pub fn set_market_data_exec_hot_shed_soft_active(active: bool) {
    MARKET_DATA_ENRICH_SHED_ACTIVE.store(active, Ordering::Relaxed);
    MARKET_DATA_EXEC_HOT_SHED_SOFT_ACTIVE.store(u64::from(active), Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_account_enrich_shed_dropped_total() {
    MARKET_DATA_ACCOUNT_ENRICH_SHED_DROPPED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_exec_hot_hard_shed_steps_total() {
    MARKET_DATA_EXEC_HOT_HARD_SHED_STEPS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Scope L2b: EXEC_HOT hard shed tier for controller / worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecHotShedTier {
    None = 0,
    Tracker = 1,
    Momentum = 2,
    Arb = 3,
}

impl ExecHotShedTier {
    #[inline]
    pub fn as_u64(self) -> u64 {
        self as u64
    }

    #[inline]
    pub fn prometheus_label(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Tracker => "tracker",
            Self::Momentum => "momentum",
            Self::Arb => "arb",
        }
    }
}

#[inline]
pub fn set_market_data_exec_hot_shed_tier(tier: ExecHotShedTier) {
    MARKET_DATA_EXEC_HOT_SHED_TIER.store(tier.as_u64(), Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_exec_hot_hard_shed_steps_for_tier(tier: ExecHotShedTier) {
    inc_market_data_exec_hot_hard_shed_steps_total();
    let cell = match tier {
        ExecHotShedTier::Tracker => &*MARKET_DATA_EXEC_HOT_HARD_SHED_STEPS_TRACKER,
        ExecHotShedTier::Momentum => &*MARKET_DATA_EXEC_HOT_HARD_SHED_STEPS_MOMENTUM,
        ExecHotShedTier::Arb => &*MARKET_DATA_EXEC_HOT_HARD_SHED_STEPS_ARB,
        ExecHotShedTier::None => return,
    };
    cell.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn add_market_data_exec_hot_hard_shed_groups_evicted_total(n: u64) {
    if n > 0 {
        MARKET_DATA_EXEC_HOT_HARD_SHED_GROUPS_EVICTED_TOTAL.fetch_add(n, Ordering::Relaxed);
    }
}

#[inline]
pub fn add_market_data_exec_hot_hard_shed_groups_for_tier(tier: ExecHotShedTier, n: u64) {
    if n == 0 {
        return;
    }
    add_market_data_exec_hot_hard_shed_groups_evicted_total(n);
    let cell = match tier {
        ExecHotShedTier::Tracker => &*MARKET_DATA_EXEC_HOT_HARD_SHED_GROUPS_TRACKER,
        ExecHotShedTier::Momentum => &*MARKET_DATA_EXEC_HOT_HARD_SHED_GROUPS_MOMENTUM,
        ExecHotShedTier::Arb => &*MARKET_DATA_EXEC_HOT_HARD_SHED_GROUPS_ARB,
        ExecHotShedTier::None => return,
    };
    cell.fetch_add(n, Ordering::Relaxed);
}

#[inline]
pub fn set_market_data_exec_hot_last_shed_groups(tier: ExecHotShedTier, groups: u64) {
    let cell = match tier {
        ExecHotShedTier::Tracker => &*MARKET_DATA_EXEC_HOT_LAST_SHED_GROUPS_TRACKER,
        ExecHotShedTier::Momentum => &*MARKET_DATA_EXEC_HOT_LAST_SHED_GROUPS_MOMENTUM,
        ExecHotShedTier::Arb => &*MARKET_DATA_EXEC_HOT_LAST_SHED_GROUPS_ARB,
        ExecHotShedTier::None => return,
    };
    cell.store(groups, Ordering::Relaxed);
}

#[inline]
pub fn market_data_exec_hot_last_shed_groups(tier: ExecHotShedTier) -> u64 {
    let cell = match tier {
        ExecHotShedTier::Tracker => &*MARKET_DATA_EXEC_HOT_LAST_SHED_GROUPS_TRACKER,
        ExecHotShedTier::Momentum => &*MARKET_DATA_EXEC_HOT_LAST_SHED_GROUPS_MOMENTUM,
        ExecHotShedTier::Arb => &*MARKET_DATA_EXEC_HOT_LAST_SHED_GROUPS_ARB,
        ExecHotShedTier::None => return 0,
    };
    cell.load(Ordering::Relaxed)
}

#[inline]
pub fn set_market_data_exec_hot_admit_suppress(tracker: bool, momentum: bool, arb: bool) {
    MARKET_DATA_EXEC_HOT_TRACKER_ADMIT_SUPPRESS.store(tracker, Ordering::Relaxed);
    MARKET_DATA_EXEC_HOT_MOMENTUM_ADMIT_SUPPRESS.store(momentum, Ordering::Relaxed);
    MARKET_DATA_EXEC_HOT_ARB_ADMIT_SUPPRESS.store(arb, Ordering::Relaxed);
}

#[inline]
pub fn market_data_exec_hot_tracker_admit_suppress() -> bool {
    MARKET_DATA_EXEC_HOT_TRACKER_ADMIT_SUPPRESS.load(Ordering::Relaxed)
}

#[inline]
pub fn market_data_exec_hot_momentum_admit_suppress() -> bool {
    MARKET_DATA_EXEC_HOT_MOMENTUM_ADMIT_SUPPRESS.load(Ordering::Relaxed)
}

#[inline]
pub fn market_data_exec_hot_arb_admit_suppress() -> bool {
    MARKET_DATA_EXEC_HOT_ARB_ADMIT_SUPPRESS.load(Ordering::Relaxed)
}

#[inline]
pub fn inc_market_data_exec_hot_pressure_admit_rejected_total(tier: ExecHotShedTier) {
    let cell = match tier {
        ExecHotShedTier::Tracker => &*MARKET_DATA_EXEC_HOT_PRESSURE_ADMIT_REJECTED_TRACKER,
        ExecHotShedTier::Momentum => &*MARKET_DATA_EXEC_HOT_PRESSURE_ADMIT_REJECTED_MOMENTUM,
        ExecHotShedTier::Arb => &*MARKET_DATA_EXEC_HOT_PRESSURE_ADMIT_REJECTED_ARB,
        ExecHotShedTier::None => return,
    };
    cell.fetch_add(1, Ordering::Relaxed);
}

/// Scope L2c-MD: SLO breach thresholds for EXEC_HOT channel lag alarm (ms).
pub const EXEC_HOT_LAG_ALARM_P50_MS: u64 = 50;
pub const EXEC_HOT_LAG_ALARM_P99_MS: u64 = 200;
/// Hysteresis clear thresholds (ms).
pub const EXEC_HOT_LAG_CLEAR_P50_MS: u64 = 40;
pub const EXEC_HOT_LAG_CLEAR_P99_MS: u64 = 150;

/// Pure alarm predicate for unit tests and controller hysteresis input.
#[inline]
pub fn exec_hot_lag_raw_alarm(p50_ms: u64, p99_ms: u64) -> bool {
    p99_ms > EXEC_HOT_LAG_ALARM_P99_MS || p50_ms > EXEC_HOT_LAG_ALARM_P50_MS
}

/// Pure clear predicate for controller hysteresis.
#[inline]
pub fn exec_hot_lag_raw_clear(p50_ms: u64, p99_ms: u64) -> bool {
    p99_ms < EXEC_HOT_LAG_CLEAR_P99_MS && p50_ms < EXEC_HOT_LAG_CLEAR_P50_MS
}

/// Lock-free recv-path sample: ring slot + EWMA p50/p99 estimates.
#[inline]
pub fn record_exec_hot_channel_lag_sample(lag_ms: u64) {
    let idx = EXEC_HOT_LAG_RING_WRITE_IDX.fetch_add(1, Ordering::Relaxed) as usize
        % EXEC_HOT_LAG_RING_CAP;
    EXEC_HOT_LAG_RING[idx].store(lag_ms, Ordering::Relaxed);
    EXEC_HOT_LAG_SAMPLE_COUNT.fetch_add(1, Ordering::Relaxed);

    let old_p50 = EXEC_HOT_LAG_EWMA_P50_MS.load(Ordering::Relaxed);
    let new_p50 = if old_p50 == 0 {
        lag_ms
    } else {
        (lag_ms * 2 + old_p50 * 8) / 10
    };
    EXEC_HOT_LAG_EWMA_P50_MS.store(new_p50, Ordering::Relaxed);

    let old_p99 = EXEC_HOT_LAG_EWMA_P99_MS.load(Ordering::Relaxed);
    let new_p99 = if old_p99 == 0 {
        lag_ms
    } else if lag_ms >= old_p99 {
        (lag_ms * 3 + old_p99 * 7) / 10
    } else {
        (lag_ms + old_p99 * 19) / 20
    };
    EXEC_HOT_LAG_EWMA_P99_MS.store(new_p99, Ordering::Relaxed);

    MARKET_DATA_EXEC_HOT_LAG_P50_EST_MS.store(new_p50, Ordering::Relaxed);
    MARKET_DATA_EXEC_HOT_LAG_P99_EST_MS.store(new_p99, Ordering::Relaxed);
}

/// Recompute percentile estimates from the ring (controller tick; not on recv hot path).
pub fn refresh_exec_hot_lag_percentile_estimates() {
    let count = EXEC_HOT_LAG_SAMPLE_COUNT.load(Ordering::Relaxed);
    if count == 0 {
        return;
    }
    let take = (count as usize).min(EXEC_HOT_LAG_RING_CAP);
    let mut samples: Vec<u64> = (0..take)
        .map(|i| EXEC_HOT_LAG_RING[i].load(Ordering::Relaxed))
        .filter(|&v| v > 0)
        .collect();
    if samples.is_empty() {
        return;
    }
    samples.sort_unstable();
    let p50 = samples[samples.len() * 50 / 100];
    let p99 = samples[samples
        .len()
        .saturating_sub(1)
        .max(samples.len() * 99 / 100)];
    MARKET_DATA_EXEC_HOT_LAG_P50_EST_MS.store(p50, Ordering::Relaxed);
    MARKET_DATA_EXEC_HOT_LAG_P99_EST_MS.store(p99, Ordering::Relaxed);
}

#[inline]
pub fn market_data_exec_hot_lag_p50_est_ms() -> u64 {
    MARKET_DATA_EXEC_HOT_LAG_P50_EST_MS.load(Ordering::Relaxed)
}

#[inline]
pub fn market_data_exec_hot_lag_p99_est_ms() -> u64 {
    MARKET_DATA_EXEC_HOT_LAG_P99_EST_MS.load(Ordering::Relaxed)
}

#[inline]
pub fn set_market_data_exec_hot_lag_alarm(active: bool) {
    MARKET_DATA_EXEC_HOT_LAG_ALARM.store(u64::from(active), Ordering::Relaxed);
}

#[inline]
pub fn market_data_exec_hot_lag_alarm() -> bool {
    MARKET_DATA_EXEC_HOT_LAG_ALARM.load(Ordering::Relaxed) != 0
}

/// Hard shed step with optional lag attribution (reason label `depth|lag`).
#[inline]
pub fn inc_market_data_exec_hot_hard_shed_steps_for_tier_reason(
    tier: ExecHotShedTier,
    lag_triggered: bool,
) {
    inc_market_data_exec_hot_hard_shed_steps_for_tier(tier);
    if !lag_triggered {
        return;
    }
    let cell = match tier {
        ExecHotShedTier::Tracker => &*MARKET_DATA_EXEC_HOT_HARD_SHED_STEPS_TRACKER_LAG,
        ExecHotShedTier::Momentum => &*MARKET_DATA_EXEC_HOT_HARD_SHED_STEPS_MOMENTUM_LAG,
        ExecHotShedTier::Arb => &*MARKET_DATA_EXEC_HOT_HARD_SHED_STEPS_ARB_LAG,
        ExecHotShedTier::None => return,
    };
    cell.fetch_add(1, Ordering::Relaxed);
}

#[cfg(test)]
mod exec_hot_lag_tests {
    use super::*;

    #[test]
    fn exec_hot_lag_alarm_and_clear_thresholds() {
        assert!(!exec_hot_lag_raw_alarm(50, 200));
        assert!(exec_hot_lag_raw_alarm(51, 100));
        assert!(exec_hot_lag_raw_alarm(30, 201));
        assert!(exec_hot_lag_raw_clear(39, 149));
        assert!(!exec_hot_lag_raw_clear(40, 149));
        assert!(!exec_hot_lag_raw_clear(39, 150));
    }

    #[test]
    fn exec_hot_lag_samples_raise_alarm_estimate() {
        for ms in [10_u64, 20, 300, 400, 500] {
            record_exec_hot_channel_lag_sample(ms);
        }
        refresh_exec_hot_lag_percentile_estimates();
        let p50 = market_data_exec_hot_lag_p50_est_ms();
        let p99 = market_data_exec_hot_lag_p99_est_ms();
        assert!(exec_hot_lag_raw_alarm(p50, p99), "p50={p50} p99={p99}");
    }
}

#[inline]
pub fn inc_market_data_devwallet_tx_published_total() {
    MARKET_DATA_DEVWALLET_TX_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn inc_market_data_devwallet_bonding_path_total() {
    MARKET_DATA_DEVWALLET_BONDING_PATH_TOTAL.fetch_add(1, Ordering::Relaxed);
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
pub fn record_market_data_account_early_drop(reason: MarketDataAccountEarlyDropReason) {
    let counter = match reason {
        MarketDataAccountEarlyDropReason::NonDexNonMembership => {
            &*MARKET_DATA_ACCOUNT_EARLY_DROP_NON_DEX_NON_MEMBERSHIP
        }
        MarketDataAccountEarlyDropReason::DexPoolNotEnrichment => {
            &*MARKET_DATA_ACCOUNT_EARLY_DROP_DEX_POOL_NOT_ENRICHMENT
        }
    };
    counter.fetch_add(1, Ordering::Relaxed);
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

/// Successful account broadcast recv iteration (one `Ok(account_update)` handled).
#[inline]
pub fn inc_market_data_account_recv_iterations_total() {
    MARKET_DATA_ACCOUNT_RECV_ITERATIONS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Classify wall time in the account recv task, microseconds.
#[inline]
pub fn record_market_data_account_recv_classify_duration_us(us: u64) {
    record_histogram_u64_into(
        MARKET_DATA_ACCOUNT_RECV_CLASSIFY_DURATION_US_BUCKETS,
        &MARKET_DATA_ACCOUNT_RECV_CLASSIFY_DURATION_US_BUCKET_COUNTS,
        &MARKET_DATA_ACCOUNT_RECV_CLASSIFY_DURATION_US_SUM,
        &MARKET_DATA_ACCOUNT_RECV_CLASSIFY_DURATION_US_COUNT,
        us,
        u64::MAX,
    );
}

/// HIGH-path worker enqueue wall time in the account recv task, microseconds.
#[inline]
pub fn record_market_data_account_recv_high_enqueue_duration_us(us: u64) {
    record_histogram_u64_into(
        MARKET_DATA_ACCOUNT_RECV_HIGH_ENQUEUE_DURATION_US_BUCKETS,
        &MARKET_DATA_ACCOUNT_RECV_HIGH_ENQUEUE_DURATION_US_BUCKET_COUNTS,
        &MARKET_DATA_ACCOUNT_RECV_HIGH_ENQUEUE_DURATION_US_SUM,
        &MARKET_DATA_ACCOUNT_RECV_HIGH_ENQUEUE_DURATION_US_COUNT,
        us,
        u64::MAX,
    );
}

/// ENRICH ingress try-send wall time in the account recv task, microseconds.
#[inline]
pub fn record_market_data_account_recv_enrich_ingress_duration_us(us: u64) {
    record_histogram_u64_into(
        MARKET_DATA_ACCOUNT_RECV_ENRICH_INGRESS_DURATION_US_BUCKETS,
        &MARKET_DATA_ACCOUNT_RECV_ENRICH_INGRESS_DURATION_US_BUCKET_COUNTS,
        &MARKET_DATA_ACCOUNT_RECV_ENRICH_INGRESS_DURATION_US_SUM,
        &MARKET_DATA_ACCOUNT_RECV_ENRICH_INGRESS_DURATION_US_COUNT,
        us,
        u64::MAX,
    );
}

/// Total recv-loop iteration wall time (`recv` → dispatch done), microseconds.
#[inline]
pub fn record_market_data_account_recv_iteration_duration_us(us: u64) {
    record_histogram_u64_into(
        MARKET_DATA_ACCOUNT_RECV_ITERATION_DURATION_US_BUCKETS,
        &MARKET_DATA_ACCOUNT_RECV_ITERATION_DURATION_US_BUCKET_COUNTS,
        &MARKET_DATA_ACCOUNT_RECV_ITERATION_DURATION_US_SUM,
        &MARKET_DATA_ACCOUNT_RECV_ITERATION_DURATION_US_COUNT,
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
pub static FILTER_REJECTED_DOWNTREND: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static FILTER_REJECTED_TOKEN_AGE: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
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

// Scope 57: orphan BUY recovery tracker-state alignment + exit amount authority hint.
pub static MOMENTUM_ORPHAN_PROBE_RECOVERY_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static MOMENTUM_ORPHAN_SCALE_IN_RECOVERY_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MOMENTUM_EXIT_AMOUNT_OVERLAY_ONLY_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// PA-5.1: overlay closed because PositionAuthority signaled closed/absent/zero.
pub static MOMENTUM_OVERLAY_CLOSED_BY_AUTHORITY_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

pub static MOMENTUM_SCALE_IN_GATE_BLOCKED_MISSING_PROBE_STATE: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MOMENTUM_SCALE_IN_GATE_BLOCKED_PNL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static MOMENTUM_SCALE_IN_GATE_BLOCKED_WINDOW_EXPIRED: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MOMENTUM_SCALE_IN_GATE_BLOCKED_NO_QUOTE: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

#[inline]
pub fn record_momentum_orphan_probe_recovery_total() {
    MOMENTUM_ORPHAN_PROBE_RECOVERY_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn record_momentum_orphan_scale_in_recovery_total() {
    MOMENTUM_ORPHAN_SCALE_IN_RECOVERY_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn record_momentum_exit_amount_overlay_only_total() {
    MOMENTUM_EXIT_AMOUNT_OVERLAY_ONLY_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn record_momentum_overlay_closed_by_authority_total() {
    MOMENTUM_OVERLAY_CLOSED_BY_AUTHORITY_TOTAL.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn set_position_authority_drift_momentum(drift: i64) {
    POSITION_AUTHORITY_DRIFT_MOMENTUM.store(drift, Ordering::Relaxed);
}

/// Per-mint signed divergence: `position.token_amount_raw - wallet_snapshot.balance_raw`.
/// Cardinality bounded to open positions with non-zero drift (pruned on close / align).
pub static MOMENTUM_WALLET_BALANCE_DIVERGENCE_BY_MINT: Lazy<RwLock<HashMap<String, i64>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));
pub static MOMENTUM_WALLET_BALANCE_DIVERGENCE_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

#[inline]
pub fn set_momentum_wallet_balance_divergence_lamports(mint: &str, signed_raw_delta: i64) {
    let mut map = MOMENTUM_WALLET_BALANCE_DIVERGENCE_BY_MINT.write();
    if signed_raw_delta == 0 {
        map.remove(mint);
    } else {
        map.insert(mint.to_string(), signed_raw_delta);
    }
}

#[inline]
pub fn clear_momentum_wallet_balance_divergence_for_mint(mint: &str) {
    MOMENTUM_WALLET_BALANCE_DIVERGENCE_BY_MINT
        .write()
        .remove(mint);
}

#[inline]
pub fn record_momentum_wallet_balance_divergence_total() {
    MOMENTUM_WALLET_BALANCE_DIVERGENCE_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Scale-in gate blocked reason (Prometheus label `reason`).
#[derive(Debug, Clone, Copy)]
pub enum MomentumScaleInGateBlockedReason {
    MissingProbeState,
    Pnl,
    WindowExpired,
    NoQuote,
}

#[inline]
pub fn record_momentum_scale_in_gate_blocked_total(reason: MomentumScaleInGateBlockedReason) {
    let counter = match reason {
        MomentumScaleInGateBlockedReason::MissingProbeState => {
            &*MOMENTUM_SCALE_IN_GATE_BLOCKED_MISSING_PROBE_STATE
        }
        MomentumScaleInGateBlockedReason::Pnl => &*MOMENTUM_SCALE_IN_GATE_BLOCKED_PNL,
        MomentumScaleInGateBlockedReason::WindowExpired => {
            &*MOMENTUM_SCALE_IN_GATE_BLOCKED_WINDOW_EXPIRED
        }
        MomentumScaleInGateBlockedReason::NoQuote => &*MOMENTUM_SCALE_IN_GATE_BLOCKED_NO_QUOTE,
    };
    counter.fetch_add(1, Ordering::Relaxed);
}

/// I-MD-9 WaitHotSet observability: hot-set freshness at pre-entry filter pass.
#[inline]
pub fn record_momentum_filter_pass_hot_fresh(fresh: bool) {
    let counter = if fresh {
        &*MOMENTUM_FILTER_PASS_HOT_FRESH_TRUE
    } else {
        &*MOMENTUM_FILTER_PASS_HOT_FRESH_FALSE
    };
    counter.fetch_add(1, Ordering::Relaxed);
}

/// Why `entry_hot_set_fresh` failed (Prometheus labels `reason`, `dex`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MomentumEntryHotFreshFailReason {
    Missing,
    Age,
    Quote,
}

impl MomentumEntryHotFreshFailReason {
    fn as_label(self) -> &'static str {
        match self {
            Self::Missing => "missing",
            Self::Age => "age",
            Self::Quote => "quote",
        }
    }
}

fn normalize_momentum_entry_hot_fresh_fail_dex(dex: &str) -> String {
    match dex {
        "pumpswap" | "PumpFunAmm" | "PumpFun AMM" => "pump_amm".to_string(),
        "pumpfun" | "PumpFun" => "pumpfun".to_string(),
        "orca" => "orca".to_string(),
        "raydium" => "raydium".to_string(),
        "raydium_cpmm" => "raydium_cpmm".to_string(),
        "meteora_dlmm" => "meteora_dlmm".to_string(),
        "meteora_cpmm" => "meteora_cpmm".to_string(),
        _ => "other".to_string(),
    }
}

/// Increment `momentum_entry_hot_fresh_fail_total{reason,dex}`.
#[inline]
pub fn record_momentum_entry_hot_fresh_fail(reason: MomentumEntryHotFreshFailReason, dex: &str) {
    let key = format!(
        "{}|{}",
        reason.as_label(),
        normalize_momentum_entry_hot_fresh_fail_dex(dex)
    );
    let mut map = MOMENTUM_ENTRY_HOT_FRESH_FAIL_TOTAL.write();
    *map.entry(key).or_insert(0) += 1;
}

#[inline]
pub fn inc_momentum_wait_hot_set_enter_total() {
    MOMENTUM_WAIT_HOT_SET_ENTER_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// WaitHotSet exit reason (Prometheus label `reason`).
#[derive(Debug, Clone, Copy)]
pub enum MomentumWaitHotSetExitReason {
    Intent,
    Timeout,
    FilterFailed,
}

#[inline]
pub fn record_momentum_wait_hot_set_exit(reason: MomentumWaitHotSetExitReason, duration_ms: u64) {
    let counter = match reason {
        MomentumWaitHotSetExitReason::Intent => &*MOMENTUM_WAIT_HOT_SET_EXIT_INTENT,
        MomentumWaitHotSetExitReason::Timeout => &*MOMENTUM_WAIT_HOT_SET_EXIT_TIMEOUT,
        MomentumWaitHotSetExitReason::FilterFailed => &*MOMENTUM_WAIT_HOT_SET_EXIT_FILTER_FAILED,
    };
    counter.fetch_add(1, Ordering::Relaxed);
    record_histogram_u64_into(
        EXECUTION_INTENT_TO_CONFIRM_MS_BUCKETS,
        MOMENTUM_WAIT_HOT_SET_DURATION_MS_BUCKET_COUNTS.as_slice(),
        &MOMENTUM_WAIT_HOT_SET_DURATION_MS_SUM,
        &MOMENTUM_WAIT_HOT_SET_DURATION_MS_COUNT,
        duration_ms,
        MOMENTUM_LATENCY_MS_SUM_CAP,
    );
}

/// Probe-buy intent path after I-MD-9 hot-set gate (Prometheus label `path`).
#[derive(Debug, Clone, Copy)]
pub enum MomentumIntentPath {
    ImmediateHot,
    AfterWaitHot,
}

#[inline]
pub fn record_momentum_intent_path(path: MomentumIntentPath) {
    let counter = match path {
        MomentumIntentPath::ImmediateHot => &*MOMENTUM_INTENT_PATH_IMMEDIATE_HOT,
        MomentumIntentPath::AfterWaitHot => &*MOMENTUM_INTENT_PATH_AFTER_WAIT_HOT,
    };
    counter.fetch_add(1, Ordering::Relaxed);
}

/// `register_geyser_reserves_impl` / momentum-active pin registration outcome.
#[derive(Debug, Clone, Copy)]
pub enum MomentumPinVaultRegisterResult {
    Ok,
    CacheMiss,
    AdmissionRejected,
    AlreadySatisfied,
    Deferred,
}

#[inline]
pub fn inc_market_data_momentum_pin_vault_register_total(result: MomentumPinVaultRegisterResult) {
    let counter = match result {
        MomentumPinVaultRegisterResult::Ok => &*MARKET_DATA_MOMENTUM_PIN_VAULT_REGISTER_OK,
        MomentumPinVaultRegisterResult::CacheMiss => {
            &*MARKET_DATA_MOMENTUM_PIN_VAULT_REGISTER_CACHE_MISS
        }
        MomentumPinVaultRegisterResult::AdmissionRejected => {
            &*MARKET_DATA_MOMENTUM_PIN_VAULT_REGISTER_ADMISSION_REJECTED
        }
        MomentumPinVaultRegisterResult::AlreadySatisfied => {
            &*MARKET_DATA_MOMENTUM_PIN_VAULT_REGISTER_ALREADY_SATISFIED
        }
        MomentumPinVaultRegisterResult::Deferred => {
            &*MARKET_DATA_MOMENTUM_PIN_VAULT_REGISTER_DEFERRED
        }
    };
    counter.fetch_add(1, Ordering::Relaxed);
}

static MOMENTUM_FILTER_PASS_HOT_FRESH_TRUE: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
static MOMENTUM_FILTER_PASS_HOT_FRESH_FALSE: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
static MOMENTUM_ENTRY_HOT_FRESH_FAIL_TOTAL: Lazy<RwLock<HashMap<String, u64>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));
static MOMENTUM_WAIT_HOT_SET_ENTER_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
static MOMENTUM_WAIT_HOT_SET_EXIT_INTENT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
static MOMENTUM_WAIT_HOT_SET_EXIT_TIMEOUT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
static MOMENTUM_WAIT_HOT_SET_EXIT_FILTER_FAILED: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
static MOMENTUM_WAIT_HOT_SET_DURATION_MS_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> = Lazy::new(|| {
    EXECUTION_INTENT_TO_CONFIRM_MS_BUCKETS
        .iter()
        .map(|_| AtomicU64::new(0))
        .collect()
});
static MOMENTUM_WAIT_HOT_SET_DURATION_MS_SUM: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
static MOMENTUM_WAIT_HOT_SET_DURATION_MS_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
static MOMENTUM_INTENT_PATH_IMMEDIATE_HOT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
static MOMENTUM_INTENT_PATH_AFTER_WAIT_HOT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
static MARKET_DATA_MOMENTUM_PIN_VAULT_REGISTER_OK: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
static MARKET_DATA_MOMENTUM_PIN_VAULT_REGISTER_CACHE_MISS: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
static MARKET_DATA_MOMENTUM_PIN_VAULT_REGISTER_ADMISSION_REJECTED: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
static MARKET_DATA_MOMENTUM_PIN_VAULT_REGISTER_ALREADY_SATISFIED: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
static MARKET_DATA_MOMENTUM_PIN_VAULT_REGISTER_DEFERRED: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

pub mod wait_hot_set_test_counters {
    use super::*;
    use std::sync::atomic::Ordering;

    pub fn reset() {
        MOMENTUM_FILTER_PASS_HOT_FRESH_TRUE.store(0, Ordering::Relaxed);
        MOMENTUM_FILTER_PASS_HOT_FRESH_FALSE.store(0, Ordering::Relaxed);
        MOMENTUM_ENTRY_HOT_FRESH_FAIL_TOTAL.write().clear();
        MOMENTUM_WAIT_HOT_SET_ENTER_TOTAL.store(0, Ordering::Relaxed);
        MOMENTUM_WAIT_HOT_SET_EXIT_INTENT.store(0, Ordering::Relaxed);
        MOMENTUM_WAIT_HOT_SET_EXIT_TIMEOUT.store(0, Ordering::Relaxed);
        MOMENTUM_WAIT_HOT_SET_EXIT_FILTER_FAILED.store(0, Ordering::Relaxed);
        MOMENTUM_WAIT_HOT_SET_DURATION_MS_COUNT.store(0, Ordering::Relaxed);
        MOMENTUM_INTENT_PATH_IMMEDIATE_HOT.store(0, Ordering::Relaxed);
        MOMENTUM_INTENT_PATH_AFTER_WAIT_HOT.store(0, Ordering::Relaxed);
    }

    pub fn filter_pass_hot_fresh_true() -> u64 {
        MOMENTUM_FILTER_PASS_HOT_FRESH_TRUE.load(Ordering::Relaxed)
    }

    pub fn filter_pass_hot_fresh_false() -> u64 {
        MOMENTUM_FILTER_PASS_HOT_FRESH_FALSE.load(Ordering::Relaxed)
    }

    pub fn entry_hot_fresh_fail_total(reason: MomentumEntryHotFreshFailReason, dex: &str) -> u64 {
        let key = format!(
            "{}|{}",
            reason.as_label(),
            normalize_momentum_entry_hot_fresh_fail_dex(dex)
        );
        MOMENTUM_ENTRY_HOT_FRESH_FAIL_TOTAL
            .read()
            .get(&key)
            .copied()
            .unwrap_or(0)
    }

    pub fn wait_hot_set_enter_total() -> u64 {
        MOMENTUM_WAIT_HOT_SET_ENTER_TOTAL.load(Ordering::Relaxed)
    }

    pub fn wait_hot_set_exit_timeout_total() -> u64 {
        MOMENTUM_WAIT_HOT_SET_EXIT_TIMEOUT.load(Ordering::Relaxed)
    }

    pub fn wait_hot_set_duration_count() -> u64 {
        MOMENTUM_WAIT_HOT_SET_DURATION_MS_COUNT.load(Ordering::Relaxed)
    }

    pub fn intent_path_immediate_hot_total() -> u64 {
        MOMENTUM_INTENT_PATH_IMMEDIATE_HOT.load(Ordering::Relaxed)
    }

    pub fn intent_path_after_wait_hot_total() -> u64 {
        MOMENTUM_INTENT_PATH_AFTER_WAIT_HOT.load(Ordering::Relaxed)
    }
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
    } else if reason.starts_with("REJECT_DEV_SUPPLY_TOO_HIGH") {
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

#[cfg(test)]
fn reset_execution_intent_delivery_segment_metrics_for_test() {
    for c in EXECUTION_INTENT_JETSTREAM_TO_CHANNEL_MS_BUCKET_COUNTS.iter() {
        c.store(0, Ordering::Relaxed);
    }
    EXECUTION_INTENT_JETSTREAM_TO_CHANNEL_MS_SUM.store(0, Ordering::Relaxed);
    EXECUTION_INTENT_JETSTREAM_TO_CHANNEL_MS_COUNT.store(0, Ordering::Relaxed);
    for c in EXECUTION_INTENT_CHANNEL_WAIT_MS_BUCKET_COUNTS.iter() {
        c.store(0, Ordering::Relaxed);
    }
    EXECUTION_INTENT_CHANNEL_WAIT_MS_SUM.store(0, Ordering::Relaxed);
    EXECUTION_INTENT_CHANNEL_WAIT_MS_COUNT.store(0, Ordering::Relaxed);
    for c in EXECUTION_ENGINE_INTERVAL_TICK_DURATION_MS_BUCKET_COUNTS.iter() {
        c.store(0, Ordering::Relaxed);
    }
    EXECUTION_ENGINE_INTERVAL_TICK_DURATION_MS_SUM.store(0, Ordering::Relaxed);
    EXECUTION_ENGINE_INTERVAL_TICK_DURATION_MS_COUNT.store(0, Ordering::Relaxed);
}

// --- execution-engine service metrics ---
pub static INTENTS_RECEIVED_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static INTENTS_EXECUTED_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static INTENTS_REJECTED_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static SIMULATION_FAILURES_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
/// Simulation RPC calls that exceeded `simulation_timeout_ms` (execution-engine).
pub static SIM_TIMEOUT_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
/// Intents rejected with `DecisionOutcome::Expired` (TTL elapsed before processing).
pub static INTENTS_EXPIRED_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
// RS-5.1: Real-send lifecycle counters (operator truth)
pub static TX_SEND_ATTEMPTS_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static TX_SEND_SUCCESS_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
// P2: Send method breakdown (TPU Direct vs Jito vs RPC)
pub static TX_SEND_TPU_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static TX_SEND_JITO_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static TX_SEND_RPC_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static TX_CONFIRMED_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static TX_CONFIRM_TIMEOUT_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
// PR3: JetStream-based TX confirmation (market-data Geyser → JetStream → EE)
pub static TX_CONFIRM_JETSTREAM_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
// PR3.2: JetStream WalletTxConfirmed deserialize failures (duplicate slot serde, etc.)
pub static TX_CONFIRM_DESERIALIZE_ERRORS_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
// PR3.1: Orphan WalletTxConfirmed buffer (confirm arrived before waiter registration)
pub static TX_CONFIRM_JETSTREAM_ORPHAN_BUFFERED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static TX_CONFIRM_JETSTREAM_ORPHAN_HIT_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static TX_CONFIRM_JETSTREAM_ORPHAN_EVICTED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
#[allow(dead_code)]
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

// Send → Geyser/JetStream confirm (wall clock from confirm wait start to success).
const TX_SEND_TO_CONFIRM_MS_BUCKETS: &[u64] = EXECUTION_INTENT_TO_CONFIRM_MS_BUCKETS;
pub static TX_SEND_TO_CONFIRM_MS_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> = Lazy::new(|| {
    TX_SEND_TO_CONFIRM_MS_BUCKETS
        .iter()
        .map(|_| AtomicU64::new(0))
        .collect()
});
pub static TX_SEND_TO_CONFIRM_MS_SUM: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static TX_SEND_TO_CONFIRM_MS_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

// Confirmed slot minus blockhash slot at send (slots until on-chain landing).
const TX_CONFIRMED_SLOT_DELTA_SLOTS_BUCKETS: &[u64] = &[0, 1, 2, 3, 4, 5, 10, 20, 50];
pub static TX_CONFIRMED_SLOT_DELTA_SLOTS_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> = Lazy::new(|| {
    TX_CONFIRMED_SLOT_DELTA_SLOTS_BUCKETS
        .iter()
        .map(|_| AtomicU64::new(0))
        .collect()
});
pub static TX_CONFIRMED_SLOT_DELTA_SLOTS_SUM: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static TX_CONFIRMED_SLOT_DELTA_SLOTS_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

pub static TX_PRIORITY_FEE_SOURCE_STATIC_FLOOR_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static TX_PRIORITY_FEE_SOURCE_DYNAMIC_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

pub static TX_REBROADCAST_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static TX_REBROADCAST_METHOD_TPU_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static TX_REBROADCAST_METHOD_RPC_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
const TX_REBROADCAST_DURING_CONFIRM_MS_BUCKETS: &[u64] = TX_SEND_TO_CONFIRM_MS_BUCKETS;
pub static TX_REBROADCAST_DURING_CONFIRM_MS_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> = Lazy::new(|| {
    TX_REBROADCAST_DURING_CONFIRM_MS_BUCKETS
        .iter()
        .map(|_| AtomicU64::new(0))
        .collect()
});
pub static TX_REBROADCAST_DURING_CONFIRM_MS_SUM: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static TX_REBROADCAST_DURING_CONFIRM_MS_COUNT: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

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

/// JetStream fetch task: TradeIntent deserialize → successful `intent_tx.send` (ms).
pub static EXECUTION_INTENT_JETSTREAM_TO_CHANNEL_MS_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> =
    Lazy::new(|| {
        EXECUTION_INTENT_TO_CONFIRM_MS_BUCKETS
            .iter()
            .map(|_| AtomicU64::new(0))
            .collect()
    });
pub static EXECUTION_INTENT_JETSTREAM_TO_CHANNEL_MS_SUM: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static EXECUTION_INTENT_JETSTREAM_TO_CHANNEL_MS_COUNT: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// `intent_tx.send` enqueue → main-loop `intent_rx.recv` (channel + select! wait, ms).
pub static EXECUTION_INTENT_CHANNEL_WAIT_MS_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> = Lazy::new(|| {
    EXECUTION_INTENT_TO_CONFIRM_MS_BUCKETS
        .iter()
        .map(|_| AtomicU64::new(0))
        .collect()
});
pub static EXECUTION_INTENT_CHANNEL_WAIT_MS_SUM: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static EXECUTION_INTENT_CHANNEL_WAIT_MS_COUNT: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// Wall time of PoolCache + WalletSnapshot JetStream batch block in `interval.tick` arm (ms).
pub static EXECUTION_ENGINE_INTERVAL_TICK_DURATION_MS_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> =
    Lazy::new(|| {
        EXECUTION_INTENT_TO_CONFIRM_MS_BUCKETS
            .iter()
            .map(|_| AtomicU64::new(0))
            .collect()
    });
pub static EXECUTION_ENGINE_INTERVAL_TICK_DURATION_MS_SUM: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static EXECUTION_ENGINE_INTERVAL_TICK_DURATION_MS_COUNT: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

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
/// market-data wallet TX confirm Geyser listener connected (separate session from DEX ingest).
pub static WALLET_TX_CONFIRM_LISTENER_CONNECTED: Lazy<AtomicBool> =
    Lazy::new(|| AtomicBool::new(false));
pub static AVAILABLE_SOL_LAMPORTS: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ACTIVE_CAPITAL_LOCKS: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ACTIVE_RESOURCE_LOCKS: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
/// Post-send capital reservations (confirm pending; not subject to pre-send TTL).
pub static IN_FLIGHT_CAPITAL_RESERVATIONS: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
/// Pre-send capital locks released by TTL expiry (`cleanup_expired` only).
pub static CAPITAL_LOCK_EXPIRED_RELEASED_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
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

/// 2-hop cross-DEX opportunities that passed all filters in arb-strategy
pub static ARB_TWO_HOP_OPPORTUNITIES: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

/// Pools seeded into TokenArbTracker from SLAVE LivePoolCache (reserve-mid, no Trade event).
pub static ARB_TWO_HOP_TRACKER_SEEDED_POOLS: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

// --- pool_quote shadow (M1, default off) ---
pub static ARB_QUOTE_SHADOW_ROUND_TRIP_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_QUOTE_SHADOW_INCOMPATIBLE_KIND_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_QUOTE_SHADOW_ROUND_TRIP_PROFIT_SUM: Lazy<AtomicI64> =
    Lazy::new(|| AtomicI64::new(0));
pub static ARB_QUOTE_SHADOW_ROUND_TRIP_PROFIT_COUNT: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_QUOTE_SHADOW_LEGACY_SPREAD_BPS: Lazy<AtomicI64> = Lazy::new(|| AtomicI64::new(0));
pub static ARB_QUOTE_SHADOW_V2_PROFIT_LAMPORTS: Lazy<AtomicI64> = Lazy::new(|| AtomicI64::new(0));

const ARB_QUOTE_SHADOW_PROFIT_BUCKETS: [i64; 5] =
    [0, 1_000_000, 10_000_000, 100_000_000, 1_000_000_000];
static ARB_QUOTE_SHADOW_PROFIT_BUCKET_COUNTS: Lazy<[AtomicU64; 5]> =
    Lazy::new(|| std::array::from_fn(|_| AtomicU64::new(0)));

// --- 2-hop profit-first v2 (M2, default off) ---
pub static ARB_TWO_HOP_V2_SCREEN_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_V2_SCREEN_MULTI_DEX_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_V2_INCOMPATIBLE_KIND_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_V2_REJECTED_ROUND_TRIP_UNPROFITABLE: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_V2_REJECTED_QUOTE_STALE: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_V2_REJECTED_INCOMPATIBLE_QUOTE_KIND: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_V2_REJECTED_INSUFFICIENT_POOLS: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_V2_REJECTED_SLOT_DELTA_EXCEEDED: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_V2_INSUFFICIENT_CANDIDATES_LT2: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_V2_INSUFFICIENT_NO_FRESH_BUY_QUOTE: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_V2_INSUFFICIENT_NO_CROSS_DEX_SELL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_V2_INSUFFICIENT_SINGLE_DEX_CANDIDATES: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_V2_NO_CROSS_DEX_SELL_DETAIL_SELL_MISSING_VAULT: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_V2_NO_CROSS_DEX_SELL_DETAIL_SELL_MISSING_DLMM_BINS: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_V2_NO_CROSS_DEX_SELL_DETAIL_SELL_QUOTE_NONE: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_V2_NO_CROSS_DEX_SELL_DETAIL_SELL_NOT_FRESH: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_V2_NO_CROSS_DEX_SELL_DETAIL_SELL_ZERO_OUT: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_V2_SELL_QUOTE_NONE_DETAIL_STATE_STALE: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_V2_SELL_QUOTE_NONE_DETAIL_RESERVES_IMPLAUSIBLE: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_V2_SELL_QUOTE_NONE_DETAIL_DLMM_ACTIVE_BIN_MISSING: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_V2_SELL_QUOTE_NONE_DETAIL_DLMM_WALKER_ZERO: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_V2_SELL_QUOTE_NONE_DETAIL_DLMM_MARGINAL_REJECT: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_V2_SELL_QUOTE_NONE_DETAIL_CPMM_MATH_NONE: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_V2_SELL_QUOTE_NONE_DETAIL_UNSUPPORTED_DEX: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_V2_SELL_QUOTE_NONE_DETAIL_TRADE_FALLBACK_NONE: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_V2_SELL_QUOTE_NONE_DETAIL_MINT_DIRECTION_INVALID: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_PROACTIVE_TRACK_PUBLISH_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TRACK_SELECTED_POOLS_GAUGE: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TRACK_SELECTED_MINTS_GAUGE: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TRACK_SELECTED_PAIR_COMPLETE_MINTS_GAUGE: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TRACK_SELECTED_ORPHAN_POOLS_GAUGE: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TRACK_CANDIDATE_POOLS_EXECUTABLE: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TRACK_CANDIDATE_POOLS_QUOTE_READY: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TRACK_CANDIDATE_POOLS_WARMABLE: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TRACK_CANDIDATE_POOLS_REJECTED: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TRACK_SELECTED_POOL_READINESS_EXECUTABLE: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TRACK_SELECTED_POOL_READINESS_QUOTE_READY: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TRACK_SELECTED_POOL_READINESS_WARMABLE: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TRACK_SELECTED_POOL_READINESS_REJECTED: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_V2_SCREEN_SKIPPED_MINT_NOT_SELECTED: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_V2_ROUND_TRIP_FORMABLE_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TRACK_REMOVED_BUDGET_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TRACK_REMOVED_STALE_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TRACK_REMOVED_COOLDOWN_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TRACK_PUBLISH_SKIPPED_UNCHANGED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TRACK_SELECTION_RECOMPUTES_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TRACK_SELECTION_QUEUE_OVERFLOW_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TRACK_SELECTION_BLOCKING_JOIN_FAILED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

const ARB_QUOTE_PAIR_SLOT_DELTA_BUCKETS: &[u64] = &[0, 1, 2, 3, 4, 5, 8, 16, 32];
static ARB_QUOTE_PAIR_SLOT_DELTA_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> = Lazy::new(|| {
    ARB_QUOTE_PAIR_SLOT_DELTA_BUCKETS
        .iter()
        .map(|_| AtomicU64::new(0))
        .collect()
});
pub static ARB_QUOTE_PAIR_SLOT_DELTA_SUM: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_QUOTE_PAIR_SLOT_DELTA_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

const ARB_TRACK_PIN_BEFORE_FIRST_SCREEN_MS_BUCKETS: &[u64] = &[
    10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000, 60000,
];
static ARB_TRACK_PIN_BEFORE_FIRST_SCREEN_MS_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> = Lazy::new(|| {
    ARB_TRACK_PIN_BEFORE_FIRST_SCREEN_MS_BUCKETS
        .iter()
        .map(|_| AtomicU64::new(0))
        .collect()
});
pub static ARB_TRACK_PIN_BEFORE_FIRST_SCREEN_MS_SUM: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TRACK_PIN_BEFORE_FIRST_SCREEN_MS_COUNT: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

const ARB_PROACTIVE_PIN_FIRST_PUBLISH_MAP_CAP: usize = 4096;
static ARB_PROACTIVE_PIN_FIRST_PUBLISH_MS: Lazy<Mutex<HashMap<String, u64>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
static ARB_PROACTIVE_PIN_FIRST_PUBLISH_ORDER: Lazy<Mutex<VecDeque<String>>> =
    Lazy::new(|| Mutex::new(VecDeque::new()));

// --- arb-strategy bootstrap / incremental warmup (low-cardinality) ---
pub static ARB_STRATEGY_BOOTSTRAP_LIVE_POOL_CACHE_ROWS: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_STRATEGY_BOOTSTRAP_KNOWN_POOLS: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_STRATEGY_BOOTSTRAP_TRACKER_SEED_CANDIDATES: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_STRATEGY_BOOTSTRAP_TRACKER_SEEDED_POOLS: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_STRATEGY_BOOTSTRAP_SKIP_UNKNOWN_DEX: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_STRATEGY_BOOTSTRAP_SKIP_NON_ARB_QUOTE: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_STRATEGY_BOOTSTRAP_SKIP_MISSING_RESERVES: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_STRATEGY_BOOTSTRAP_SKIP_ZERO_RESERVES: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_STRATEGY_BOOTSTRAP_SKIP_NATIVE_TOKEN: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_STRATEGY_POOL_CACHE_UPDATES_SEEN: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_STRATEGY_POOL_CACHE_UPDATES_SEEDED: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_STRATEGY_POOL_CACHE_UPDATE_SKIP_NON_ARB_QUOTE: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_STRATEGY_POOL_CACHE_UPDATE_SKIP_NO_SEED: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_POOL_CACHE_UPDATES_APPLIED_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_POOL_CACHE_APPLY_BATCHES_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_POOL_CACHE_APPLY_BATCH_SIZE: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
/// JetStream fetch batches that returned zero messages (arb pool-cache live worker).
pub static ARB_POOL_CACHE_SYNC_FETCH_EMPTY_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
/// Messages pulled from JetStream before deserialize (arb pool-cache live worker).
pub static ARB_POOL_CACHE_SYNC_MESSAGES_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TRACKER_WRITE_ENQUEUE_DROPPED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TRACKER_WRITE_QUEUE_DEPTH: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TRACKER_WRITE_COALESCED_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TRACKER_WRITE_COALESCED_FLUSHED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

pub static ARB_TRACKER_WRITE_DROPPED_POOL_STATE_UPDATE: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TRACKER_WRITE_DROPPED_APPLY_TRADE: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TRACKER_WRITE_DROPPED_POOL_CREATED: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TRACKER_WRITE_DROPPED_DEX_POOL_ACCOUNTS: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TRACKER_WRITE_DROPPED_TOKEN_MINT_INFO: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TRACKER_WRITE_DROPPED_SEED_POOL_CACHE: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TRACKER_WRITE_DROPPED_FINALIZE_OPPORTUNITY: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

pub static ARB_TRACKER_WRITE_PROCESSED_POOL_STATE_UPDATE: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TRACKER_WRITE_PROCESSED_APPLY_TRADE: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TRACKER_WRITE_PROCESSED_POOL_CREATED: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TRACKER_WRITE_PROCESSED_DEX_POOL_ACCOUNTS: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TRACKER_WRITE_PROCESSED_TOKEN_MINT_INFO: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TRACKER_WRITE_PROCESSED_SEED_POOL_CACHE: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TRACKER_WRITE_PROCESSED_FINALIZE_OPPORTUNITY: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// Tracker-write job type for per-type drop/process metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArbTrackerWriteJobType {
    PoolStateUpdate,
    ApplyTrade,
    PoolCreated,
    DexPoolAccounts,
    TokenMintInfo,
    SeedPoolCache,
    FinalizeOpportunity,
}

impl ArbTrackerWriteJobType {
    pub const COUNT: usize = 7;

    pub fn index(self) -> usize {
        match self {
            Self::PoolStateUpdate => 0,
            Self::ApplyTrade => 1,
            Self::PoolCreated => 2,
            Self::DexPoolAccounts => 3,
            Self::TokenMintInfo => 4,
            Self::SeedPoolCache => 5,
            Self::FinalizeOpportunity => 6,
        }
    }

    pub fn as_numeric(self) -> u64 {
        (self.index() + 1) as u64
    }

    pub fn prometheus_label(self) -> &'static str {
        match self {
            Self::PoolStateUpdate => "pool_state_update",
            Self::ApplyTrade => "apply_trade",
            Self::PoolCreated => "pool_created",
            Self::DexPoolAccounts => "dex_pool_accounts",
            Self::TokenMintInfo => "token_mint_info",
            Self::SeedPoolCache => "seed_pool_cache",
            Self::FinalizeOpportunity => "finalize_opportunity",
        }
    }

    pub fn all() -> [Self; Self::COUNT] {
        [
            Self::PoolStateUpdate,
            Self::ApplyTrade,
            Self::PoolCreated,
            Self::DexPoolAccounts,
            Self::TokenMintInfo,
            Self::SeedPoolCache,
            Self::FinalizeOpportunity,
        ]
    }
}

/// Writer job duration histogram buckets (nanoseconds).
const ARB_TRACKER_WRITE_JOB_DURATION_NS_BUCKETS: &[u64] = &[
    1_000_000,
    5_000_000,
    10_000_000,
    25_000_000,
    50_000_000,
    100_000_000,
    250_000_000,
    500_000_000,
    1_000_000_000,
    2_500_000_000,
    5_000_000_000,
    10_000_000_000,
    30_000_000_000,
    60_000_000_000,
];

struct ArbTrackerWriteJobDurationHist {
    bucket_counts: Vec<AtomicU64>,
    sum_ns: AtomicU64,
    count: AtomicU64,
}

impl ArbTrackerWriteJobDurationHist {
    fn new() -> Self {
        Self {
            bucket_counts: ARB_TRACKER_WRITE_JOB_DURATION_NS_BUCKETS
                .iter()
                .map(|_| AtomicU64::new(0))
                .collect(),
            sum_ns: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    fn record(&self, duration_ns: u64) {
        record_histogram_u64_into(
            ARB_TRACKER_WRITE_JOB_DURATION_NS_BUCKETS,
            &self.bucket_counts,
            &self.sum_ns,
            &self.count,
            duration_ns,
            u64::MAX,
        );
    }
}

fn new_arb_tracker_write_job_duration_hists(
) -> [ArbTrackerWriteJobDurationHist; ArbTrackerWriteJobType::COUNT] {
    std::array::from_fn(|_| ArbTrackerWriteJobDurationHist::new())
}

static ARB_TRACKER_WRITE_JOB_STARTED: Lazy<[AtomicU64; ArbTrackerWriteJobType::COUNT]> =
    Lazy::new(|| std::array::from_fn(|_| AtomicU64::new(0)));
static ARB_TRACKER_WRITE_JOB_FINISHED: Lazy<[AtomicU64; ArbTrackerWriteJobType::COUNT]> =
    Lazy::new(|| std::array::from_fn(|_| AtomicU64::new(0)));
static ARB_TRACKER_WRITE_JOB_DURATION: Lazy<
    [ArbTrackerWriteJobDurationHist; ArbTrackerWriteJobType::COUNT],
> = Lazy::new(new_arb_tracker_write_job_duration_hists);

pub static ARB_TRACKER_WRITE_LAST_JOB_TYPE: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TRACKER_WRITE_SECONDS_SINCE_LAST_FINISH: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TRACKER_WRITE_CURRENT_JOB_TYPE: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TRACKER_WRITE_CURRENT_JOB_STARTED_UNIX_MS: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TRACKER_WRITE_LAST_FINISH_UNIX_MS: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

static ARB_TRACKER_WRITE_COALESCER_FLUSH_LOST: Lazy<[AtomicU64; ArbTrackerWriteJobType::COUNT]> =
    Lazy::new(|| std::array::from_fn(|_| AtomicU64::new(0)));
pub static ARB_TRACKER_WRITE_COALESCER_PENDING: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_BLOCKED_ON_APPLY_TRADE: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TRACKER_WRITE_STALL_WATCHDOG_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// Lock kinds observed during tracker-write jobs (Prometheus label `lock`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArbWriterLockKind {
    TrackersRead,
    TrackersWrite,
    VaultBalancesWrite,
}

impl ArbWriterLockKind {
    fn index(self) -> usize {
        match self {
            Self::TrackersRead => 0,
            Self::TrackersWrite => 1,
            Self::VaultBalancesWrite => 2,
        }
    }

    fn prometheus_label(self) -> &'static str {
        match self {
            Self::TrackersRead => "trackers_read",
            Self::TrackersWrite => "trackers_write",
            Self::VaultBalancesWrite => "vault_balances_write",
        }
    }

    const COUNT: usize = 3;
}

struct ArbWriterLockWaitHist {
    bucket_counts: Vec<AtomicU64>,
    sum_ns: AtomicU64,
    count: AtomicU64,
}

impl ArbWriterLockWaitHist {
    fn new() -> Self {
        Self {
            bucket_counts: ARB_TRACKER_WRITE_JOB_DURATION_NS_BUCKETS
                .iter()
                .map(|_| AtomicU64::new(0))
                .collect(),
            sum_ns: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    fn record(&self, wait_ns: u64) {
        record_histogram_u64_into(
            ARB_TRACKER_WRITE_JOB_DURATION_NS_BUCKETS,
            &self.bucket_counts,
            &self.sum_ns,
            &self.count,
            wait_ns,
            u64::MAX,
        );
    }
}

static ARB_TRACKER_WRITE_LOCK_WAIT: Lazy<[ArbWriterLockWaitHist; ArbWriterLockKind::COUNT]> =
    Lazy::new(|| std::array::from_fn(|_| ArbWriterLockWaitHist::new()));

/// Heartbeat sub-phases for stall forensics (Prometheus label `phase`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArbHeartbeatPhase {
    TrackersRead,
    MaybeEmit,
    SyncPools,
    Prune,
    InfoLog,
}

impl ArbHeartbeatPhase {
    fn index(self) -> usize {
        match self {
            Self::TrackersRead => 0,
            Self::MaybeEmit => 1,
            Self::SyncPools => 2,
            Self::Prune => 3,
            Self::InfoLog => 4,
        }
    }

    fn prometheus_label(self) -> &'static str {
        match self {
            Self::TrackersRead => "trackers_read",
            Self::MaybeEmit => "maybe_emit",
            Self::SyncPools => "sync_pools",
            Self::Prune => "prune",
            Self::InfoLog => "info_log",
        }
    }

    const COUNT: usize = 5;
}

struct ArbHeartbeatPhaseDurationHist {
    bucket_counts: Vec<AtomicU64>,
    sum_ns: AtomicU64,
    count: AtomicU64,
}

impl ArbHeartbeatPhaseDurationHist {
    fn new() -> Self {
        Self {
            bucket_counts: ARB_TRACKER_WRITE_JOB_DURATION_NS_BUCKETS
                .iter()
                .map(|_| AtomicU64::new(0))
                .collect(),
            sum_ns: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    fn record(&self, duration_ns: u64) {
        record_histogram_u64_into(
            ARB_TRACKER_WRITE_JOB_DURATION_NS_BUCKETS,
            &self.bucket_counts,
            &self.sum_ns,
            &self.count,
            duration_ns,
            u64::MAX,
        );
    }
}

static ARB_HEARTBEAT_PHASE_DURATION: Lazy<
    [ArbHeartbeatPhaseDurationHist; ArbHeartbeatPhase::COUNT],
> = Lazy::new(|| std::array::from_fn(|_| ArbHeartbeatPhaseDurationHist::new()));
pub static ARB_HEARTBEAT_LAST_FINISH_UNIX_MS: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_HEARTBEAT_SECONDS_SINCE_LAST_FINISH: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// 2-hop reject breakdown (arb-strategy `check_arbitrage`)
pub static ARB_TWO_HOP_REJECTED_SPREAD_TOO_LARGE: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_REJECTED_SPREAD_BELOW_MIN: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_REJECTED_PROFIT_BELOW_MIN: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_REJECTED_SAME_DEX: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_REJECTED_PUMPFUN: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_REJECTED_INSUFFICIENT_POOLS: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_REJECTED_STALE_PRICE: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_REJECTED_NO_COMPARABLE_PRICE: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_REJECTED_NATIVE_SOL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_REJECTED_DATA_QUALITY: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

/// Rejection reason for 2-hop cross-DEX arb checks (Prometheus label `reason`).
#[derive(Debug, Clone, Copy)]
pub enum ArbTwoHopRejectReason {
    SpreadTooLarge,
    SpreadBelowMin,
    ProfitBelowMin,
    SameDex,
    Pumpfun,
    InsufficientPools,
    StalePrice,
    NoComparablePrice,
    NativeSol,
    DataQuality,
}

/// Increment `arb_two_hop_rejected_total{reason=...}` for the given rejection.
pub fn arb_two_hop_rejected_inc(reason: ArbTwoHopRejectReason) {
    let counter = match reason {
        ArbTwoHopRejectReason::SpreadTooLarge => &*ARB_TWO_HOP_REJECTED_SPREAD_TOO_LARGE,
        ArbTwoHopRejectReason::SpreadBelowMin => &*ARB_TWO_HOP_REJECTED_SPREAD_BELOW_MIN,
        ArbTwoHopRejectReason::ProfitBelowMin => &*ARB_TWO_HOP_REJECTED_PROFIT_BELOW_MIN,
        ArbTwoHopRejectReason::SameDex => &*ARB_TWO_HOP_REJECTED_SAME_DEX,
        ArbTwoHopRejectReason::Pumpfun => &*ARB_TWO_HOP_REJECTED_PUMPFUN,
        ArbTwoHopRejectReason::InsufficientPools => &*ARB_TWO_HOP_REJECTED_INSUFFICIENT_POOLS,
        ArbTwoHopRejectReason::StalePrice => &*ARB_TWO_HOP_REJECTED_STALE_PRICE,
        ArbTwoHopRejectReason::NoComparablePrice => &*ARB_TWO_HOP_REJECTED_NO_COMPARABLE_PRICE,
        ArbTwoHopRejectReason::NativeSol => &*ARB_TWO_HOP_REJECTED_NATIVE_SOL,
        ArbTwoHopRejectReason::DataQuality => &*ARB_TWO_HOP_REJECTED_DATA_QUALITY,
    };
    counter.fetch_add(1, Ordering::Relaxed);
}

/// Increment `arb_two_hop_opportunities_total`.
pub fn arb_two_hop_opportunity_inc() {
    ARB_TWO_HOP_OPPORTUNITIES.fetch_add(1, Ordering::Relaxed);
}

/// Record pool_quote shadow round-trip observation (legacy path unaffected).
pub fn record_arb_quote_shadow_round_trip(
    profit_lamports: i64,
    legacy_spread_bps: Option<i64>,
    incompatible_kind: bool,
) {
    ARB_QUOTE_SHADOW_ROUND_TRIP_TOTAL.fetch_add(1, Ordering::Relaxed);
    if incompatible_kind {
        ARB_QUOTE_SHADOW_INCOMPATIBLE_KIND_TOTAL.fetch_add(1, Ordering::Relaxed);
    }
    ARB_QUOTE_SHADOW_ROUND_TRIP_PROFIT_SUM.fetch_add(profit_lamports, Ordering::Relaxed);
    ARB_QUOTE_SHADOW_ROUND_TRIP_PROFIT_COUNT.fetch_add(1, Ordering::Relaxed);
    ARB_QUOTE_SHADOW_V2_PROFIT_LAMPORTS.store(profit_lamports, Ordering::Relaxed);
    if let Some(spread) = legacy_spread_bps {
        ARB_QUOTE_SHADOW_LEGACY_SPREAD_BPS.store(spread, Ordering::Relaxed);
    }
    for (i, bucket) in ARB_QUOTE_SHADOW_PROFIT_BUCKETS.iter().enumerate() {
        if profit_lamports <= *bucket {
            ARB_QUOTE_SHADOW_PROFIT_BUCKET_COUNTS[i].fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Update legacy spread gauge when shadow mode compares v1 vs v2.
pub fn set_arb_quote_shadow_legacy_spread_bps(spread_bps: i64) {
    ARB_QUOTE_SHADOW_LEGACY_SPREAD_BPS.store(spread_bps, Ordering::Relaxed);
}

/// Rejection reason for `arb_two_hop_v2_rejected_total{reason=...}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArbTwoHopV2RejectReason {
    RoundTripUnprofitable,
    QuoteStale,
    IncompatibleQuoteKind,
    InsufficientPools,
    SlotDeltaExceeded,
}

/// Increment `arb_two_hop_v2_screen_total`.
pub fn arb_two_hop_v2_screen_inc() {
    ARB_TWO_HOP_V2_SCREEN_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Increment `arb_two_hop_v2_screen_multi_dex_total`.
pub fn arb_two_hop_v2_screen_multi_dex_inc() {
    ARB_TWO_HOP_V2_SCREEN_MULTI_DEX_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Subreason for `arb_two_hop_v2_insufficient_subreason_total{reason=...}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArbTwoHopV2InsufficientSubreason {
    CandidatesLt2,
    NoFreshBuyQuote,
    NoCrossDexSell,
    SingleDexCandidates,
}

/// Increment `arb_two_hop_v2_insufficient_subreason_total{reason=...}`.
pub fn arb_two_hop_v2_insufficient_subreason_inc(reason: ArbTwoHopV2InsufficientSubreason) {
    let counter = match reason {
        ArbTwoHopV2InsufficientSubreason::CandidatesLt2 => {
            &*ARB_TWO_HOP_V2_INSUFFICIENT_CANDIDATES_LT2
        }
        ArbTwoHopV2InsufficientSubreason::NoFreshBuyQuote => {
            &*ARB_TWO_HOP_V2_INSUFFICIENT_NO_FRESH_BUY_QUOTE
        }
        ArbTwoHopV2InsufficientSubreason::NoCrossDexSell => {
            &*ARB_TWO_HOP_V2_INSUFFICIENT_NO_CROSS_DEX_SELL
        }
        ArbTwoHopV2InsufficientSubreason::SingleDexCandidates => {
            &*ARB_TWO_HOP_V2_INSUFFICIENT_SINGLE_DEX_CANDIDATES
        }
    };
    counter.fetch_add(1, Ordering::Relaxed);
}

/// Drill-down reason for `arb_two_hop_v2_no_cross_dex_sell_detail_total{reason=...}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArbTwoHopV2NoCrossDexSellDetail {
    SellMissingVault,
    SellMissingDlmmBins,
    SellQuoteNone,
    SellNotFresh,
    SellZeroOut,
}

/// Increment `arb_two_hop_v2_no_cross_dex_sell_detail_total{reason=...}`.
pub fn arb_two_hop_v2_no_cross_dex_sell_detail_inc(reason: ArbTwoHopV2NoCrossDexSellDetail) {
    let counter = match reason {
        ArbTwoHopV2NoCrossDexSellDetail::SellMissingVault => {
            &*ARB_TWO_HOP_V2_NO_CROSS_DEX_SELL_DETAIL_SELL_MISSING_VAULT
        }
        ArbTwoHopV2NoCrossDexSellDetail::SellMissingDlmmBins => {
            &*ARB_TWO_HOP_V2_NO_CROSS_DEX_SELL_DETAIL_SELL_MISSING_DLMM_BINS
        }
        ArbTwoHopV2NoCrossDexSellDetail::SellQuoteNone => {
            &*ARB_TWO_HOP_V2_NO_CROSS_DEX_SELL_DETAIL_SELL_QUOTE_NONE
        }
        ArbTwoHopV2NoCrossDexSellDetail::SellNotFresh => {
            &*ARB_TWO_HOP_V2_NO_CROSS_DEX_SELL_DETAIL_SELL_NOT_FRESH
        }
        ArbTwoHopV2NoCrossDexSellDetail::SellZeroOut => {
            &*ARB_TWO_HOP_V2_NO_CROSS_DEX_SELL_DETAIL_SELL_ZERO_OUT
        }
    };
    counter.fetch_add(1, Ordering::Relaxed);
}

/// Drill-down reason for `arb_two_hop_v2_sell_quote_none_detail_total{reason=...}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArbTwoHopV2SellQuoteNoneDetail {
    StateStale,
    ReservesImplausible,
    DlmmActiveBinMissing,
    DlmmWalkerZero,
    DlmmMarginalReject,
    CpmmMathNone,
    UnsupportedDex,
    TradeFallbackNone,
    MintDirectionInvalid,
}

/// Increment `arb_two_hop_v2_sell_quote_none_detail_total{reason=...}`.
pub fn arb_two_hop_v2_sell_quote_none_detail_inc(reason: ArbTwoHopV2SellQuoteNoneDetail) {
    let counter = match reason {
        ArbTwoHopV2SellQuoteNoneDetail::StateStale => {
            &*ARB_TWO_HOP_V2_SELL_QUOTE_NONE_DETAIL_STATE_STALE
        }
        ArbTwoHopV2SellQuoteNoneDetail::ReservesImplausible => {
            &*ARB_TWO_HOP_V2_SELL_QUOTE_NONE_DETAIL_RESERVES_IMPLAUSIBLE
        }
        ArbTwoHopV2SellQuoteNoneDetail::DlmmActiveBinMissing => {
            &*ARB_TWO_HOP_V2_SELL_QUOTE_NONE_DETAIL_DLMM_ACTIVE_BIN_MISSING
        }
        ArbTwoHopV2SellQuoteNoneDetail::DlmmWalkerZero => {
            &*ARB_TWO_HOP_V2_SELL_QUOTE_NONE_DETAIL_DLMM_WALKER_ZERO
        }
        ArbTwoHopV2SellQuoteNoneDetail::DlmmMarginalReject => {
            &*ARB_TWO_HOP_V2_SELL_QUOTE_NONE_DETAIL_DLMM_MARGINAL_REJECT
        }
        ArbTwoHopV2SellQuoteNoneDetail::CpmmMathNone => {
            &*ARB_TWO_HOP_V2_SELL_QUOTE_NONE_DETAIL_CPMM_MATH_NONE
        }
        ArbTwoHopV2SellQuoteNoneDetail::UnsupportedDex => {
            &*ARB_TWO_HOP_V2_SELL_QUOTE_NONE_DETAIL_UNSUPPORTED_DEX
        }
        ArbTwoHopV2SellQuoteNoneDetail::TradeFallbackNone => {
            &*ARB_TWO_HOP_V2_SELL_QUOTE_NONE_DETAIL_TRADE_FALLBACK_NONE
        }
        ArbTwoHopV2SellQuoteNoneDetail::MintDirectionInvalid => {
            &*ARB_TWO_HOP_V2_SELL_QUOTE_NONE_DETAIL_MINT_DIRECTION_INVALID
        }
    };
    counter.fetch_add(1, Ordering::Relaxed);
}

/// Increment `arb_two_hop_v2_incompatible_kind_total`.
pub fn arb_two_hop_v2_incompatible_kind_inc() {
    ARB_TWO_HOP_V2_INCOMPATIBLE_KIND_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Increment `arb_two_hop_v2_rejected_total{reason=...}`.
pub fn arb_two_hop_v2_rejected_inc(reason: ArbTwoHopV2RejectReason) {
    let counter = match reason {
        ArbTwoHopV2RejectReason::RoundTripUnprofitable => {
            &*ARB_TWO_HOP_V2_REJECTED_ROUND_TRIP_UNPROFITABLE
        }
        ArbTwoHopV2RejectReason::QuoteStale => &*ARB_TWO_HOP_V2_REJECTED_QUOTE_STALE,
        ArbTwoHopV2RejectReason::IncompatibleQuoteKind => {
            &*ARB_TWO_HOP_V2_REJECTED_INCOMPATIBLE_QUOTE_KIND
        }
        ArbTwoHopV2RejectReason::InsufficientPools => &*ARB_TWO_HOP_V2_REJECTED_INSUFFICIENT_POOLS,
        ArbTwoHopV2RejectReason::SlotDeltaExceeded => &*ARB_TWO_HOP_V2_REJECTED_SLOT_DELTA_EXCEEDED,
    };
    counter.fetch_add(1, Ordering::Relaxed);
}

/// Record `|buy.as_of_slot - sell.as_of_slot|` for a v2 round-trip screen.
pub fn record_arb_quote_pair_slot_delta(delta_slots: u64) {
    record_histogram_u64_into(
        ARB_QUOTE_PAIR_SLOT_DELTA_BUCKETS,
        ARB_QUOTE_PAIR_SLOT_DELTA_BUCKET_COUNTS.as_slice(),
        &ARB_QUOTE_PAIR_SLOT_DELTA_SUM,
        &ARB_QUOTE_PAIR_SLOT_DELTA_COUNT,
        delta_slots,
        u64::MAX,
    );
}

/// Record first proactive multi-DEX pin publish for pin-before-first-screen latency.
pub fn record_arb_proactive_pin_first_publish(mint: &str) {
    let now = wall_clock_unix_ms_now();
    let mut map = ARB_PROACTIVE_PIN_FIRST_PUBLISH_MS.lock();
    if map.contains_key(mint) {
        return;
    }
    if map.len() >= ARB_PROACTIVE_PIN_FIRST_PUBLISH_MAP_CAP {
        let mut order = ARB_PROACTIVE_PIN_FIRST_PUBLISH_ORDER.lock();
        while map.len() >= ARB_PROACTIVE_PIN_FIRST_PUBLISH_MAP_CAP {
            if let Some(old) = order.pop_front() {
                map.remove(&old);
            } else {
                break;
            }
        }
    }
    map.insert(mint.to_string(), now);
    ARB_PROACTIVE_PIN_FIRST_PUBLISH_ORDER
        .lock()
        .push_back(mint.to_string());
}

/// On first v2 screen for a mint, record ms since first proactive pin publish (if tracked).
pub fn try_record_arb_track_pin_before_first_screen_ms(mint: &str) {
    let publish_ms = ARB_PROACTIVE_PIN_FIRST_PUBLISH_MS.lock().remove(mint);
    let Some(publish_ms) = publish_ms else {
        return;
    };
    ARB_PROACTIVE_PIN_FIRST_PUBLISH_ORDER
        .lock()
        .retain(|m| m != mint);
    let now = wall_clock_unix_ms_now();
    let delta_ms = now.saturating_sub(publish_ms);
    record_histogram_u64_into(
        ARB_TRACK_PIN_BEFORE_FIRST_SCREEN_MS_BUCKETS,
        ARB_TRACK_PIN_BEFORE_FIRST_SCREEN_MS_BUCKET_COUNTS.as_slice(),
        &ARB_TRACK_PIN_BEFORE_FIRST_SCREEN_MS_SUM,
        &ARB_TRACK_PIN_BEFORE_FIRST_SCREEN_MS_COUNT,
        delta_ms,
        3_600_000,
    );
}

/// Increment `arb_proactive_track_publish_total`.
pub fn record_arb_proactive_track_publish_total() {
    ARB_PROACTIVE_TRACK_PUBLISH_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Update bounded Arb track selection gauges and candidate counters.
pub fn set_arb_track_selection_metrics(
    selected_pools: usize,
    selected_mints: usize,
    pair_complete_mints: usize,
    orphan_pools: usize,
    candidate_counts: &crate::arbitrage::TrackCandidateCounts,
) {
    ARB_TRACK_SELECTED_POOLS_GAUGE.store(selected_pools as u64, Ordering::Relaxed);
    ARB_TRACK_SELECTED_MINTS_GAUGE.store(selected_mints as u64, Ordering::Relaxed);
    ARB_TRACK_SELECTED_PAIR_COMPLETE_MINTS_GAUGE
        .store(pair_complete_mints as u64, Ordering::Relaxed);
    ARB_TRACK_SELECTED_ORPHAN_POOLS_GAUGE.store(orphan_pools as u64, Ordering::Relaxed);
    ARB_TRACK_CANDIDATE_POOLS_EXECUTABLE.store(candidate_counts.executable, Ordering::Relaxed);
    ARB_TRACK_CANDIDATE_POOLS_QUOTE_READY.store(candidate_counts.quote_ready, Ordering::Relaxed);
    ARB_TRACK_CANDIDATE_POOLS_WARMABLE.store(candidate_counts.warmable, Ordering::Relaxed);
    ARB_TRACK_CANDIDATE_POOLS_REJECTED.store(candidate_counts.rejected, Ordering::Relaxed);
}

/// Update readiness histogram for pools in the authoritative selected pin set (I-ARB-10b).
pub fn set_arb_track_selected_pool_readiness_metrics(
    candidate_counts: &crate::arbitrage::TrackCandidateCounts,
) {
    ARB_TRACK_SELECTED_POOL_READINESS_EXECUTABLE
        .store(candidate_counts.executable, Ordering::Relaxed);
    ARB_TRACK_SELECTED_POOL_READINESS_QUOTE_READY
        .store(candidate_counts.quote_ready, Ordering::Relaxed);
    ARB_TRACK_SELECTED_POOL_READINESS_WARMABLE.store(candidate_counts.warmable, Ordering::Relaxed);
    ARB_TRACK_SELECTED_POOL_READINESS_REJECTED.store(candidate_counts.rejected, Ordering::Relaxed);
}

/// Skip reason for `arb_two_hop_v2_screen_skipped_total{reason=...}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArbTwoHopV2ScreenSkipReason {
    MintNotSelected,
}

/// Increment `arb_two_hop_v2_screen_skipped_total{reason=...}`.
pub fn arb_two_hop_v2_screen_skipped_inc(reason: ArbTwoHopV2ScreenSkipReason) {
    match reason {
        ArbTwoHopV2ScreenSkipReason::MintNotSelected => {
            ARB_TWO_HOP_V2_SCREEN_SKIPPED_MINT_NOT_SELECTED.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Increment `arb_two_hop_v2_round_trip_formable_total` when `select_round_trip_pools` succeeds.
pub fn arb_two_hop_v2_round_trip_formable_inc() {
    ARB_TWO_HOP_V2_ROUND_TRIP_FORMABLE_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Increment `arb_track_removed_total{reason=...}`.
pub fn record_arb_track_removed_total(reason: crate::nats::ArbTrackRemovedReason) {
    use crate::nats::ArbTrackRemovedReason;
    match reason {
        ArbTrackRemovedReason::Budget => {
            ARB_TRACK_REMOVED_BUDGET_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        ArbTrackRemovedReason::Stale => {
            ARB_TRACK_REMOVED_STALE_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        ArbTrackRemovedReason::Cooldown => {
            ARB_TRACK_REMOVED_COOLDOWN_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Increment `arb_track_publish_skipped_unchanged_total`.
pub fn record_arb_track_publish_skipped_unchanged_total() {
    ARB_TRACK_PUBLISH_SKIPPED_UNCHANGED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Increment `arb_track_selection_recomputes_total`.
pub fn record_arb_track_selection_recompute_total() {
    ARB_TRACK_SELECTION_RECOMPUTES_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Increment `arb_track_selection_queue_overflow_total`.
pub fn record_arb_track_selection_queue_overflow_total() {
    ARB_TRACK_SELECTION_QUEUE_OVERFLOW_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Increment `arb_track_selection_blocking_join_failed_total`.
pub fn record_arb_track_selection_blocking_join_failed_total() {
    ARB_TRACK_SELECTION_BLOCKING_JOIN_FAILED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Add to `arb_two_hop_tracker_seeded_pools_total` after SLAVE cache tracker seed.
pub fn arb_two_hop_tracker_seeded_pools_add(count: u64) {
    ARB_TWO_HOP_TRACKER_SEEDED_POOLS.fetch_add(count, Ordering::Relaxed);
}

/// Bootstrap skip reason for arb-strategy tracker warmup (Prometheus label `reason`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArbStrategyWarmupSkipReason {
    UnknownDex,
    NonArbQuote,
    MissingReserves,
    ZeroReserves,
    NativeTokenMint,
}

/// Record a bootstrap warmup skip (`arb_strategy_bootstrap_tracker_seed_skipped_total{reason=...}`).
pub fn arb_strategy_bootstrap_skip_inc(reason: ArbStrategyWarmupSkipReason) {
    let counter = match reason {
        ArbStrategyWarmupSkipReason::UnknownDex => &*ARB_STRATEGY_BOOTSTRAP_SKIP_UNKNOWN_DEX,
        ArbStrategyWarmupSkipReason::NonArbQuote => &*ARB_STRATEGY_BOOTSTRAP_SKIP_NON_ARB_QUOTE,
        ArbStrategyWarmupSkipReason::MissingReserves => {
            &*ARB_STRATEGY_BOOTSTRAP_SKIP_MISSING_RESERVES
        }
        ArbStrategyWarmupSkipReason::ZeroReserves => &*ARB_STRATEGY_BOOTSTRAP_SKIP_ZERO_RESERVES,
        ArbStrategyWarmupSkipReason::NativeTokenMint => &*ARB_STRATEGY_BOOTSTRAP_SKIP_NATIVE_TOKEN,
    };
    counter.fetch_add(1, Ordering::Relaxed);
}

/// Publish bootstrap warmup gauges after JetStream SLAVE recovery.
pub fn arb_strategy_bootstrap_warmup_set(
    live_pool_cache_rows: u64,
    known_pools: u64,
    tracker_seed_candidates: u64,
    tracker_seeded_pools: u64,
) {
    ARB_STRATEGY_BOOTSTRAP_LIVE_POOL_CACHE_ROWS.store(live_pool_cache_rows, Ordering::Relaxed);
    ARB_STRATEGY_BOOTSTRAP_KNOWN_POOLS.store(known_pools, Ordering::Relaxed);
    ARB_STRATEGY_BOOTSTRAP_TRACKER_SEED_CANDIDATES
        .store(tracker_seed_candidates, Ordering::Relaxed);
    ARB_STRATEGY_BOOTSTRAP_TRACKER_SEEDED_POOLS.store(tracker_seeded_pools, Ordering::Relaxed);
}

pub fn arb_strategy_pool_cache_update_seen_inc() {
    ARB_STRATEGY_POOL_CACHE_UPDATES_SEEN.fetch_add(1, Ordering::Relaxed);
}

pub fn arb_strategy_pool_cache_update_seeded_inc() {
    ARB_STRATEGY_POOL_CACHE_UPDATES_SEEDED.fetch_add(1, Ordering::Relaxed);
}

pub fn arb_strategy_pool_cache_update_skip_non_arb_quote_inc() {
    ARB_STRATEGY_POOL_CACHE_UPDATE_SKIP_NON_ARB_QUOTE.fetch_add(1, Ordering::Relaxed);
}

pub fn arb_strategy_pool_cache_update_skip_no_seed_inc() {
    ARB_STRATEGY_POOL_CACHE_UPDATE_SKIP_NO_SEED.fetch_add(1, Ordering::Relaxed);
}

pub fn arb_pool_cache_updates_applied_add(n: u64) {
    ARB_POOL_CACHE_UPDATES_APPLIED_TOTAL.fetch_add(n, Ordering::Relaxed);
}

pub fn arb_pool_cache_apply_batches_inc() {
    ARB_POOL_CACHE_APPLY_BATCHES_TOTAL.fetch_add(1, Ordering::Relaxed);
}

pub fn set_arb_pool_cache_apply_batch_size_gauge(n: u64) {
    ARB_POOL_CACHE_APPLY_BATCH_SIZE.store(n, Ordering::Relaxed);
}

pub fn arb_pool_cache_sync_messages_add(n: u64) {
    ARB_POOL_CACHE_SYNC_MESSAGES_TOTAL.fetch_add(n, Ordering::Relaxed);
}

pub fn arb_pool_cache_sync_fetch_empty_inc() {
    ARB_POOL_CACHE_SYNC_FETCH_EMPTY_TOTAL.fetch_add(1, Ordering::Relaxed);
}

const ARB_PRICE_FRESHNESS_AGE_MS_BUCKETS: &[u64] = &[
    100, 250, 500, 1_000, 2_000, 5_000, 10_000, 30_000, 60_000, 120_000,
];
const ARB_PRICE_FRESHNESS_AGE_MS_SUM_CAP: u64 = 600_000;
const ARB_PRICE_FRESHNESS_BUCKET_LEN: usize = 10;

fn arb_freshness_bucket_array() -> [AtomicU64; ARB_PRICE_FRESHNESS_BUCKET_LEN] {
    std::array::from_fn(|_| AtomicU64::new(0))
}

pub static ARB_PRICE_FRESHNESS_ORCA_TRADE_BUCKET_COUNTS: Lazy<
    [AtomicU64; ARB_PRICE_FRESHNESS_BUCKET_LEN],
> = Lazy::new(arb_freshness_bucket_array);
pub static ARB_PRICE_FRESHNESS_ORCA_TRADE_SUM: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_PRICE_FRESHNESS_ORCA_TRADE_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_PRICE_FRESHNESS_ORCA_VAULT_BUCKET_COUNTS: Lazy<
    [AtomicU64; ARB_PRICE_FRESHNESS_BUCKET_LEN],
> = Lazy::new(arb_freshness_bucket_array);
pub static ARB_PRICE_FRESHNESS_ORCA_VAULT_SUM: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_PRICE_FRESHNESS_ORCA_VAULT_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_PRICE_FRESHNESS_METEORA_DLMM_TRADE_BUCKET_COUNTS: Lazy<
    [AtomicU64; ARB_PRICE_FRESHNESS_BUCKET_LEN],
> = Lazy::new(arb_freshness_bucket_array);
pub static ARB_PRICE_FRESHNESS_METEORA_DLMM_TRADE_SUM: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_PRICE_FRESHNESS_METEORA_DLMM_TRADE_COUNT: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_PRICE_FRESHNESS_METEORA_DLMM_VAULT_BUCKET_COUNTS: Lazy<
    [AtomicU64; ARB_PRICE_FRESHNESS_BUCKET_LEN],
> = Lazy::new(arb_freshness_bucket_array);
pub static ARB_PRICE_FRESHNESS_METEORA_DLMM_VAULT_SUM: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_PRICE_FRESHNESS_METEORA_DLMM_VAULT_COUNT: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_PRICE_FRESHNESS_METEORA_DLMM_DLMM_META_BUCKET_COUNTS: Lazy<
    [AtomicU64; ARB_PRICE_FRESHNESS_BUCKET_LEN],
> = Lazy::new(arb_freshness_bucket_array);
pub static ARB_PRICE_FRESHNESS_METEORA_DLMM_DLMM_META_SUM: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_PRICE_FRESHNESS_METEORA_DLMM_DLMM_META_COUNT: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_PRICE_FRESHNESS_PUMP_AMM_TRADE_BUCKET_COUNTS: Lazy<
    [AtomicU64; ARB_PRICE_FRESHNESS_BUCKET_LEN],
> = Lazy::new(arb_freshness_bucket_array);
pub static ARB_PRICE_FRESHNESS_PUMP_AMM_TRADE_SUM: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_PRICE_FRESHNESS_PUMP_AMM_TRADE_COUNT: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_PRICE_FRESHNESS_PUMP_AMM_VAULT_BUCKET_COUNTS: Lazy<
    [AtomicU64; ARB_PRICE_FRESHNESS_BUCKET_LEN],
> = Lazy::new(arb_freshness_bucket_array);
pub static ARB_PRICE_FRESHNESS_PUMP_AMM_VAULT_SUM: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_PRICE_FRESHNESS_PUMP_AMM_VAULT_COUNT: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_PRICE_FRESHNESS_OTHER_TRADE_BUCKET_COUNTS: Lazy<
    [AtomicU64; ARB_PRICE_FRESHNESS_BUCKET_LEN],
> = Lazy::new(arb_freshness_bucket_array);
pub static ARB_PRICE_FRESHNESS_OTHER_TRADE_SUM: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_PRICE_FRESHNESS_OTHER_TRADE_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_PRICE_FRESHNESS_OTHER_VAULT_BUCKET_COUNTS: Lazy<
    [AtomicU64; ARB_PRICE_FRESHNESS_BUCKET_LEN],
> = Lazy::new(arb_freshness_bucket_array);
pub static ARB_PRICE_FRESHNESS_OTHER_VAULT_SUM: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_PRICE_FRESHNESS_OTHER_VAULT_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

fn arb_price_freshness_hist_parts(
    dex: &str,
    source: &str,
) -> (
    &'static [AtomicU64; ARB_PRICE_FRESHNESS_BUCKET_LEN],
    &'static AtomicU64,
    &'static AtomicU64,
    &'static str,
    &'static str,
) {
    let dex_label = match dex {
        "orca" => "orca",
        "meteora_dlmm" => "meteora_dlmm",
        "pump_amm" => "pump_amm",
        _ => "other",
    };
    let source_label = match source {
        "vault" => "vault",
        "dlmm_meta" => "dlmm_meta",
        _ => "trade",
    };
    match (dex_label, source_label) {
        ("orca", "trade") => (
            &*ARB_PRICE_FRESHNESS_ORCA_TRADE_BUCKET_COUNTS,
            &*ARB_PRICE_FRESHNESS_ORCA_TRADE_SUM,
            &*ARB_PRICE_FRESHNESS_ORCA_TRADE_COUNT,
            "orca",
            "trade",
        ),
        ("orca", "vault") => (
            &*ARB_PRICE_FRESHNESS_ORCA_VAULT_BUCKET_COUNTS,
            &*ARB_PRICE_FRESHNESS_ORCA_VAULT_SUM,
            &*ARB_PRICE_FRESHNESS_ORCA_VAULT_COUNT,
            "orca",
            "vault",
        ),
        ("meteora_dlmm", "trade") => (
            &*ARB_PRICE_FRESHNESS_METEORA_DLMM_TRADE_BUCKET_COUNTS,
            &*ARB_PRICE_FRESHNESS_METEORA_DLMM_TRADE_SUM,
            &*ARB_PRICE_FRESHNESS_METEORA_DLMM_TRADE_COUNT,
            "meteora_dlmm",
            "trade",
        ),
        ("meteora_dlmm", "vault") => (
            &*ARB_PRICE_FRESHNESS_METEORA_DLMM_VAULT_BUCKET_COUNTS,
            &*ARB_PRICE_FRESHNESS_METEORA_DLMM_VAULT_SUM,
            &*ARB_PRICE_FRESHNESS_METEORA_DLMM_VAULT_COUNT,
            "meteora_dlmm",
            "vault",
        ),
        ("meteora_dlmm", "dlmm_meta") => (
            &*ARB_PRICE_FRESHNESS_METEORA_DLMM_DLMM_META_BUCKET_COUNTS,
            &*ARB_PRICE_FRESHNESS_METEORA_DLMM_DLMM_META_SUM,
            &*ARB_PRICE_FRESHNESS_METEORA_DLMM_DLMM_META_COUNT,
            "meteora_dlmm",
            "dlmm_meta",
        ),
        ("pump_amm", "trade") => (
            &*ARB_PRICE_FRESHNESS_PUMP_AMM_TRADE_BUCKET_COUNTS,
            &*ARB_PRICE_FRESHNESS_PUMP_AMM_TRADE_SUM,
            &*ARB_PRICE_FRESHNESS_PUMP_AMM_TRADE_COUNT,
            "pump_amm",
            "trade",
        ),
        ("pump_amm", "vault") => (
            &*ARB_PRICE_FRESHNESS_PUMP_AMM_VAULT_BUCKET_COUNTS,
            &*ARB_PRICE_FRESHNESS_PUMP_AMM_VAULT_SUM,
            &*ARB_PRICE_FRESHNESS_PUMP_AMM_VAULT_COUNT,
            "pump_amm",
            "vault",
        ),
        (_, "vault") => (
            &*ARB_PRICE_FRESHNESS_OTHER_VAULT_BUCKET_COUNTS,
            &*ARB_PRICE_FRESHNESS_OTHER_VAULT_SUM,
            &*ARB_PRICE_FRESHNESS_OTHER_VAULT_COUNT,
            "other",
            "vault",
        ),
        _ => (
            &*ARB_PRICE_FRESHNESS_OTHER_TRADE_BUCKET_COUNTS,
            &*ARB_PRICE_FRESHNESS_OTHER_TRADE_SUM,
            &*ARB_PRICE_FRESHNESS_OTHER_TRADE_COUNT,
            "other",
            "trade",
        ),
    }
}

/// Record stale-price age at 2-hop reject (`arb_price_freshness_age_ms_bucket{dex,freshness_source}`).
pub fn record_arb_price_freshness_stale_age_ms(dex: &str, source: &str, age_ms: u64) {
    let (bucket_counts, sum, count, _, _) = arb_price_freshness_hist_parts(dex, source);
    record_histogram_u64_into(
        ARB_PRICE_FRESHNESS_AGE_MS_BUCKETS,
        bucket_counts,
        sum,
        count,
        age_ms,
        ARB_PRICE_FRESHNESS_AGE_MS_SUM_CAP,
    );
}

fn append_arb_price_freshness_labeled_histogram(
    out: &mut String,
    dex: &str,
    source: &str,
    bucket_counts: &[AtomicU64; ARB_PRICE_FRESHNESS_BUCKET_LEN],
    sum: &AtomicU64,
    count: &AtomicU64,
) {
    let c = count.load(Ordering::Relaxed);
    let s = sum.load(Ordering::Relaxed);
    for (i, b) in ARB_PRICE_FRESHNESS_AGE_MS_BUCKETS.iter().enumerate() {
        let v = bucket_counts[i].load(Ordering::Relaxed);
        out.push_str(&format!(
            "arb_price_freshness_age_ms_bucket{{dex=\"{dex}\",freshness_source=\"{source}\",le=\"{b}\"}} {v}\n"
        ));
    }
    out.push_str(&format!(
        "arb_price_freshness_age_ms_bucket{{dex=\"{dex}\",freshness_source=\"{source}\",le=\"+Inf\"}} {c}\n"
    ));
    out.push_str(&format!(
        "arb_price_freshness_age_ms_sum{{dex=\"{dex}\",freshness_source=\"{source}\"}} {s}\n"
    ));
    out.push_str(&format!(
        "arb_price_freshness_age_ms_count{{dex=\"{dex}\",freshness_source=\"{source}\"}} {c}\n"
    ));
}

fn append_arb_price_freshness_histograms(out: &mut String) {
    let series: [(
        &str,
        &str,
        &[AtomicU64; ARB_PRICE_FRESHNESS_BUCKET_LEN],
        &AtomicU64,
        &AtomicU64,
    ); 9] = [
        (
            "orca",
            "trade",
            &*ARB_PRICE_FRESHNESS_ORCA_TRADE_BUCKET_COUNTS,
            &*ARB_PRICE_FRESHNESS_ORCA_TRADE_SUM,
            &*ARB_PRICE_FRESHNESS_ORCA_TRADE_COUNT,
        ),
        (
            "orca",
            "vault",
            &*ARB_PRICE_FRESHNESS_ORCA_VAULT_BUCKET_COUNTS,
            &*ARB_PRICE_FRESHNESS_ORCA_VAULT_SUM,
            &*ARB_PRICE_FRESHNESS_ORCA_VAULT_COUNT,
        ),
        (
            "meteora_dlmm",
            "trade",
            &*ARB_PRICE_FRESHNESS_METEORA_DLMM_TRADE_BUCKET_COUNTS,
            &*ARB_PRICE_FRESHNESS_METEORA_DLMM_TRADE_SUM,
            &*ARB_PRICE_FRESHNESS_METEORA_DLMM_TRADE_COUNT,
        ),
        (
            "meteora_dlmm",
            "vault",
            &*ARB_PRICE_FRESHNESS_METEORA_DLMM_VAULT_BUCKET_COUNTS,
            &*ARB_PRICE_FRESHNESS_METEORA_DLMM_VAULT_SUM,
            &*ARB_PRICE_FRESHNESS_METEORA_DLMM_VAULT_COUNT,
        ),
        (
            "meteora_dlmm",
            "dlmm_meta",
            &*ARB_PRICE_FRESHNESS_METEORA_DLMM_DLMM_META_BUCKET_COUNTS,
            &*ARB_PRICE_FRESHNESS_METEORA_DLMM_DLMM_META_SUM,
            &*ARB_PRICE_FRESHNESS_METEORA_DLMM_DLMM_META_COUNT,
        ),
        (
            "pump_amm",
            "trade",
            &*ARB_PRICE_FRESHNESS_PUMP_AMM_TRADE_BUCKET_COUNTS,
            &*ARB_PRICE_FRESHNESS_PUMP_AMM_TRADE_SUM,
            &*ARB_PRICE_FRESHNESS_PUMP_AMM_TRADE_COUNT,
        ),
        (
            "pump_amm",
            "vault",
            &*ARB_PRICE_FRESHNESS_PUMP_AMM_VAULT_BUCKET_COUNTS,
            &*ARB_PRICE_FRESHNESS_PUMP_AMM_VAULT_SUM,
            &*ARB_PRICE_FRESHNESS_PUMP_AMM_VAULT_COUNT,
        ),
        (
            "other",
            "trade",
            &*ARB_PRICE_FRESHNESS_OTHER_TRADE_BUCKET_COUNTS,
            &*ARB_PRICE_FRESHNESS_OTHER_TRADE_SUM,
            &*ARB_PRICE_FRESHNESS_OTHER_TRADE_COUNT,
        ),
        (
            "other",
            "vault",
            &*ARB_PRICE_FRESHNESS_OTHER_VAULT_BUCKET_COUNTS,
            &*ARB_PRICE_FRESHNESS_OTHER_VAULT_SUM,
            &*ARB_PRICE_FRESHNESS_OTHER_VAULT_COUNT,
        ),
    ];
    for (dex, source, buckets, sum, count) in series {
        append_arb_price_freshness_labeled_histogram(out, dex, source, buckets, sum, count);
    }
}

pub fn inc_arb_tracker_write_enqueue_dropped_total() {
    ARB_TRACKER_WRITE_ENQUEUE_DROPPED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

pub fn arb_tracker_write_enqueue_dropped_inc(job_type: ArbTrackerWriteJobType) {
    ARB_TRACKER_WRITE_ENQUEUE_DROPPED_TOTAL.fetch_add(1, Ordering::Relaxed);
    let counter = match job_type {
        ArbTrackerWriteJobType::PoolStateUpdate => &*ARB_TRACKER_WRITE_DROPPED_POOL_STATE_UPDATE,
        ArbTrackerWriteJobType::ApplyTrade => &*ARB_TRACKER_WRITE_DROPPED_APPLY_TRADE,
        ArbTrackerWriteJobType::PoolCreated => &*ARB_TRACKER_WRITE_DROPPED_POOL_CREATED,
        ArbTrackerWriteJobType::DexPoolAccounts => &*ARB_TRACKER_WRITE_DROPPED_DEX_POOL_ACCOUNTS,
        ArbTrackerWriteJobType::TokenMintInfo => &*ARB_TRACKER_WRITE_DROPPED_TOKEN_MINT_INFO,
        ArbTrackerWriteJobType::SeedPoolCache => &*ARB_TRACKER_WRITE_DROPPED_SEED_POOL_CACHE,
        ArbTrackerWriteJobType::FinalizeOpportunity => {
            &*ARB_TRACKER_WRITE_DROPPED_FINALIZE_OPPORTUNITY
        }
    };
    counter.fetch_add(1, Ordering::Relaxed);
}

pub fn arb_tracker_write_job_processed_inc(job_type: ArbTrackerWriteJobType) {
    let counter = match job_type {
        ArbTrackerWriteJobType::PoolStateUpdate => &*ARB_TRACKER_WRITE_PROCESSED_POOL_STATE_UPDATE,
        ArbTrackerWriteJobType::ApplyTrade => &*ARB_TRACKER_WRITE_PROCESSED_APPLY_TRADE,
        ArbTrackerWriteJobType::PoolCreated => &*ARB_TRACKER_WRITE_PROCESSED_POOL_CREATED,
        ArbTrackerWriteJobType::DexPoolAccounts => &*ARB_TRACKER_WRITE_PROCESSED_DEX_POOL_ACCOUNTS,
        ArbTrackerWriteJobType::TokenMintInfo => &*ARB_TRACKER_WRITE_PROCESSED_TOKEN_MINT_INFO,
        ArbTrackerWriteJobType::SeedPoolCache => &*ARB_TRACKER_WRITE_PROCESSED_SEED_POOL_CACHE,
        ArbTrackerWriteJobType::FinalizeOpportunity => {
            &*ARB_TRACKER_WRITE_PROCESSED_FINALIZE_OPPORTUNITY
        }
    };
    counter.fetch_add(1, Ordering::Relaxed);
}

pub fn arb_tracker_write_coalesced_inc() {
    ARB_TRACKER_WRITE_COALESCED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

pub fn arb_tracker_write_coalesced_flushed_inc() {
    ARB_TRACKER_WRITE_COALESCED_FLUSHED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

pub fn set_arb_tracker_write_queue_depth(depth: u64) {
    ARB_TRACKER_WRITE_QUEUE_DEPTH.store(depth, Ordering::Relaxed);
}

pub fn arb_tracker_write_init_worker_state() {
    let now = wall_clock_unix_ms_now();
    ARB_TRACKER_WRITE_LAST_FINISH_UNIX_MS.store(now, Ordering::Relaxed);
    ARB_TRACKER_WRITE_SECONDS_SINCE_LAST_FINISH.store(0, Ordering::Relaxed);
}

pub fn arb_tracker_write_job_started(job_type: ArbTrackerWriteJobType) {
    ARB_TRACKER_WRITE_JOB_STARTED[job_type.index()].fetch_add(1, Ordering::Relaxed);
    ARB_TRACKER_WRITE_LAST_JOB_TYPE.store(job_type.as_numeric(), Ordering::Relaxed);
    ARB_TRACKER_WRITE_CURRENT_JOB_TYPE.store(job_type.as_numeric(), Ordering::Relaxed);
    ARB_TRACKER_WRITE_CURRENT_JOB_STARTED_UNIX_MS
        .store(wall_clock_unix_ms_now(), Ordering::Relaxed);
}

pub fn arb_tracker_write_job_finished(job_type: ArbTrackerWriteJobType, duration: Duration) {
    let idx = job_type.index();
    ARB_TRACKER_WRITE_JOB_FINISHED[idx].fetch_add(1, Ordering::Relaxed);
    ARB_TRACKER_WRITE_JOB_DURATION[idx].record(duration.as_nanos() as u64);
    let now = wall_clock_unix_ms_now();
    ARB_TRACKER_WRITE_LAST_FINISH_UNIX_MS.store(now, Ordering::Relaxed);
    ARB_TRACKER_WRITE_SECONDS_SINCE_LAST_FINISH.store(0, Ordering::Relaxed);
    ARB_TRACKER_WRITE_CURRENT_JOB_TYPE.store(0, Ordering::Relaxed);
    ARB_TRACKER_WRITE_CURRENT_JOB_STARTED_UNIX_MS.store(0, Ordering::Relaxed);
}

pub fn tick_arb_tracker_write_seconds_since_last_finish() {
    let last = ARB_TRACKER_WRITE_LAST_FINISH_UNIX_MS.load(Ordering::Relaxed);
    if last == 0 {
        return;
    }
    let now = wall_clock_unix_ms_now();
    let secs = now.saturating_sub(last) / 1000;
    ARB_TRACKER_WRITE_SECONDS_SINCE_LAST_FINISH.store(secs, Ordering::Relaxed);
}

pub fn arb_tracker_write_job_started_total(job_type: ArbTrackerWriteJobType) -> u64 {
    ARB_TRACKER_WRITE_JOB_STARTED[job_type.index()].load(Ordering::Relaxed)
}

pub fn arb_tracker_write_job_finished_total(job_type: ArbTrackerWriteJobType) -> u64 {
    ARB_TRACKER_WRITE_JOB_FINISHED[job_type.index()].load(Ordering::Relaxed)
}

pub fn arb_tracker_write_job_duration_count(job_type: ArbTrackerWriteJobType) -> u64 {
    ARB_TRACKER_WRITE_JOB_DURATION[job_type.index()]
        .count
        .load(Ordering::Relaxed)
}

pub fn arb_tracker_write_coalescer_flush_lost_inc(job_type: ArbTrackerWriteJobType) {
    ARB_TRACKER_WRITE_COALESCER_FLUSH_LOST[job_type.index()].fetch_add(1, Ordering::Relaxed);
}

pub fn arb_tracker_write_coalescer_flush_lost_total(job_type: ArbTrackerWriteJobType) -> u64 {
    ARB_TRACKER_WRITE_COALESCER_FLUSH_LOST[job_type.index()].load(Ordering::Relaxed)
}

pub fn set_arb_tracker_write_coalescer_pending(pending: u64) {
    ARB_TRACKER_WRITE_COALESCER_PENDING.store(pending, Ordering::Relaxed);
}

pub fn set_arb_two_hop_blocked_on_apply_trade(blocked: bool) {
    ARB_TWO_HOP_BLOCKED_ON_APPLY_TRADE.store(u64::from(blocked), Ordering::Relaxed);
}

pub fn arb_tracker_write_stall_watchdog_inc() {
    ARB_TRACKER_WRITE_STALL_WATCHDOG_TOTAL.fetch_add(1, Ordering::Relaxed);
}

pub fn record_arb_writer_lock_wait(lock: ArbWriterLockKind, wait: Duration) {
    ARB_TRACKER_WRITE_LOCK_WAIT[lock.index()].record(wait.as_nanos() as u64);
}

pub fn arb_writer_lock_wait_count(lock: ArbWriterLockKind) -> u64 {
    ARB_TRACKER_WRITE_LOCK_WAIT[lock.index()]
        .count
        .load(Ordering::Relaxed)
}

pub fn record_arb_heartbeat_phase(phase: ArbHeartbeatPhase, duration: Duration) {
    ARB_HEARTBEAT_PHASE_DURATION[phase.index()].record(duration.as_nanos() as u64);
}

pub fn arb_heartbeat_finished() {
    let now = wall_clock_unix_ms_now();
    ARB_HEARTBEAT_LAST_FINISH_UNIX_MS.store(now, Ordering::Relaxed);
    ARB_HEARTBEAT_SECONDS_SINCE_LAST_FINISH.store(0, Ordering::Relaxed);
}

pub fn tick_arb_heartbeat_seconds_since_last_finish() {
    let last = ARB_HEARTBEAT_LAST_FINISH_UNIX_MS.load(Ordering::Relaxed);
    if last == 0 {
        return;
    }
    let now = wall_clock_unix_ms_now();
    let secs = now.saturating_sub(last) / 1000;
    ARB_HEARTBEAT_SECONDS_SINCE_LAST_FINISH.store(secs, Ordering::Relaxed);
}

/// Sub-reason when a mint hits the `insufficient_pools` gate (low-cardinality `reason` label).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArbTwoHopInsufficientSubreason {
    NotKnownPool,
    MissingReserves,
    MissingTradePrice,
    NoComparablePrice,
    OnlyOneEligiblePool,
    OnlyOneEligibleDex,
}

/// Diagnostic sub-reason for any 2-hop reject path (low-cardinality `reason` label).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArbTwoHopRejectSubreason {
    NotKnownPool,
    MissingDecimals,
    MissingReserves,
    MissingTradePrice,
    NoComparablePrice,
    StalePrice,
    SameDexOnly,
    ImplausiblePrice,
    OnlyOneEligiblePool,
    OnlyOneEligibleDex,
}

impl From<ArbTwoHopInsufficientSubreason> for ArbTwoHopRejectSubreason {
    fn from(reason: ArbTwoHopInsufficientSubreason) -> Self {
        match reason {
            ArbTwoHopInsufficientSubreason::NotKnownPool => Self::NotKnownPool,
            ArbTwoHopInsufficientSubreason::MissingReserves => Self::MissingReserves,
            ArbTwoHopInsufficientSubreason::MissingTradePrice => Self::MissingTradePrice,
            ArbTwoHopInsufficientSubreason::NoComparablePrice => Self::NoComparablePrice,
            ArbTwoHopInsufficientSubreason::OnlyOneEligiblePool => Self::OnlyOneEligiblePool,
            ArbTwoHopInsufficientSubreason::OnlyOneEligibleDex => Self::OnlyOneEligibleDex,
        }
    }
}

pub static ARB_TWO_HOP_INSUFFICIENT_NOT_KNOWN_POOL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_INSUFFICIENT_MISSING_RESERVES: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_INSUFFICIENT_MISSING_TRADE_PRICE: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_INSUFFICIENT_NO_COMPARABLE_PRICE: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_INSUFFICIENT_ONLY_ONE_ELIGIBLE_POOL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_INSUFFICIENT_ONLY_ONE_ELIGIBLE_DEX: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

pub static ARB_TWO_HOP_REJECT_NOT_KNOWN_POOL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_REJECT_MISSING_DECIMALS: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_REJECT_MISSING_RESERVES: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_REJECT_MISSING_TRADE_PRICE: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_REJECT_NO_COMPARABLE_PRICE: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_REJECT_STALE_PRICE: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_REJECT_SAME_DEX_ONLY: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_REJECT_IMPLAUSIBLE_PRICE: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_REJECT_ONLY_ONE_ELIGIBLE_POOL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_REJECT_ONLY_ONE_ELIGIBLE_DEX: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

/// Increment `arb_two_hop_insufficient_subreason_total{reason=...}` for insufficient_pools only.
pub fn arb_two_hop_insufficient_subreason_inc(reason: ArbTwoHopInsufficientSubreason) {
    let counter = match reason {
        ArbTwoHopInsufficientSubreason::NotKnownPool => &*ARB_TWO_HOP_INSUFFICIENT_NOT_KNOWN_POOL,
        ArbTwoHopInsufficientSubreason::MissingReserves => {
            &*ARB_TWO_HOP_INSUFFICIENT_MISSING_RESERVES
        }
        ArbTwoHopInsufficientSubreason::MissingTradePrice => {
            &*ARB_TWO_HOP_INSUFFICIENT_MISSING_TRADE_PRICE
        }
        ArbTwoHopInsufficientSubreason::NoComparablePrice => {
            &*ARB_TWO_HOP_INSUFFICIENT_NO_COMPARABLE_PRICE
        }
        ArbTwoHopInsufficientSubreason::OnlyOneEligiblePool => {
            &*ARB_TWO_HOP_INSUFFICIENT_ONLY_ONE_ELIGIBLE_POOL
        }
        ArbTwoHopInsufficientSubreason::OnlyOneEligibleDex => {
            &*ARB_TWO_HOP_INSUFFICIENT_ONLY_ONE_ELIGIBLE_DEX
        }
    };
    counter.fetch_add(1, Ordering::Relaxed);
    arb_two_hop_reject_subreason_inc(reason.into());
}

fn append_arb_two_hop_insufficient_subreason_total(out: &mut String) {
    out.push_str("arb_two_hop_insufficient_subreason_total{reason=\"not_known_pool\"} ");
    out.push_str(
        &ARB_TWO_HOP_INSUFFICIENT_NOT_KNOWN_POOL
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_insufficient_subreason_total{reason=\"missing_reserves\"} ");
    out.push_str(
        &ARB_TWO_HOP_INSUFFICIENT_MISSING_RESERVES
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_insufficient_subreason_total{reason=\"missing_trade_price\"} ");
    out.push_str(
        &ARB_TWO_HOP_INSUFFICIENT_MISSING_TRADE_PRICE
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_insufficient_subreason_total{reason=\"no_comparable_price\"} ");
    out.push_str(
        &ARB_TWO_HOP_INSUFFICIENT_NO_COMPARABLE_PRICE
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_insufficient_subreason_total{reason=\"only_one_eligible_pool\"} ");
    out.push_str(
        &ARB_TWO_HOP_INSUFFICIENT_ONLY_ONE_ELIGIBLE_POOL
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_insufficient_subreason_total{reason=\"only_one_eligible_dex\"} ");
    out.push_str(
        &ARB_TWO_HOP_INSUFFICIENT_ONLY_ONE_ELIGIBLE_DEX
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
}

fn append_arb_two_hop_v2_insufficient_subreason_total(out: &mut String) {
    out.push_str("arb_two_hop_v2_insufficient_subreason_total{reason=\"candidates_lt_2\"} ");
    out.push_str(
        &ARB_TWO_HOP_V2_INSUFFICIENT_CANDIDATES_LT2
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_v2_insufficient_subreason_total{reason=\"no_fresh_buy_quote\"} ");
    out.push_str(
        &ARB_TWO_HOP_V2_INSUFFICIENT_NO_FRESH_BUY_QUOTE
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_v2_insufficient_subreason_total{reason=\"no_cross_dex_sell\"} ");
    out.push_str(
        &ARB_TWO_HOP_V2_INSUFFICIENT_NO_CROSS_DEX_SELL
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_v2_insufficient_subreason_total{reason=\"single_dex_candidates\"} ");
    out.push_str(
        &ARB_TWO_HOP_V2_INSUFFICIENT_SINGLE_DEX_CANDIDATES
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
}

fn append_arb_two_hop_v2_no_cross_dex_sell_detail_total(out: &mut String) {
    out.push_str("arb_two_hop_v2_no_cross_dex_sell_detail_total{reason=\"sell_missing_vault\"} ");
    out.push_str(
        &ARB_TWO_HOP_V2_NO_CROSS_DEX_SELL_DETAIL_SELL_MISSING_VAULT
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str(
        "arb_two_hop_v2_no_cross_dex_sell_detail_total{reason=\"sell_missing_dlmm_bins\"} ",
    );
    out.push_str(
        &ARB_TWO_HOP_V2_NO_CROSS_DEX_SELL_DETAIL_SELL_MISSING_DLMM_BINS
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_v2_no_cross_dex_sell_detail_total{reason=\"sell_quote_none\"} ");
    out.push_str(
        &ARB_TWO_HOP_V2_NO_CROSS_DEX_SELL_DETAIL_SELL_QUOTE_NONE
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_v2_no_cross_dex_sell_detail_total{reason=\"sell_not_fresh\"} ");
    out.push_str(
        &ARB_TWO_HOP_V2_NO_CROSS_DEX_SELL_DETAIL_SELL_NOT_FRESH
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_v2_no_cross_dex_sell_detail_total{reason=\"sell_zero_out\"} ");
    out.push_str(
        &ARB_TWO_HOP_V2_NO_CROSS_DEX_SELL_DETAIL_SELL_ZERO_OUT
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
}

fn append_arb_two_hop_v2_sell_quote_none_detail_total(out: &mut String) {
    out.push_str("arb_two_hop_v2_sell_quote_none_detail_total{reason=\"state_stale\"} ");
    out.push_str(
        &ARB_TWO_HOP_V2_SELL_QUOTE_NONE_DETAIL_STATE_STALE
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_v2_sell_quote_none_detail_total{reason=\"reserves_implausible\"} ");
    out.push_str(
        &ARB_TWO_HOP_V2_SELL_QUOTE_NONE_DETAIL_RESERVES_IMPLAUSIBLE
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str(
        "arb_two_hop_v2_sell_quote_none_detail_total{reason=\"dlmm_active_bin_missing\"} ",
    );
    out.push_str(
        &ARB_TWO_HOP_V2_SELL_QUOTE_NONE_DETAIL_DLMM_ACTIVE_BIN_MISSING
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_v2_sell_quote_none_detail_total{reason=\"dlmm_walker_zero\"} ");
    out.push_str(
        &ARB_TWO_HOP_V2_SELL_QUOTE_NONE_DETAIL_DLMM_WALKER_ZERO
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_v2_sell_quote_none_detail_total{reason=\"dlmm_marginal_reject\"} ");
    out.push_str(
        &ARB_TWO_HOP_V2_SELL_QUOTE_NONE_DETAIL_DLMM_MARGINAL_REJECT
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_v2_sell_quote_none_detail_total{reason=\"cpmm_math_none\"} ");
    out.push_str(
        &ARB_TWO_HOP_V2_SELL_QUOTE_NONE_DETAIL_CPMM_MATH_NONE
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_v2_sell_quote_none_detail_total{reason=\"unsupported_dex\"} ");
    out.push_str(
        &ARB_TWO_HOP_V2_SELL_QUOTE_NONE_DETAIL_UNSUPPORTED_DEX
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_v2_sell_quote_none_detail_total{reason=\"trade_fallback_none\"} ");
    out.push_str(
        &ARB_TWO_HOP_V2_SELL_QUOTE_NONE_DETAIL_TRADE_FALLBACK_NONE
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_v2_sell_quote_none_detail_total{reason=\"mint_direction_invalid\"} ");
    out.push_str(
        &ARB_TWO_HOP_V2_SELL_QUOTE_NONE_DETAIL_MINT_DIRECTION_INVALID
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
}

fn append_arb_two_hop_reject_subreason_total(out: &mut String) {
    out.push_str("arb_two_hop_reject_subreason_total{reason=\"not_known_pool\"} ");
    out.push_str(
        &ARB_TWO_HOP_REJECT_NOT_KNOWN_POOL
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_reject_subreason_total{reason=\"missing_decimals\"} ");
    out.push_str(
        &ARB_TWO_HOP_REJECT_MISSING_DECIMALS
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_reject_subreason_total{reason=\"missing_reserves\"} ");
    out.push_str(
        &ARB_TWO_HOP_REJECT_MISSING_RESERVES
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_reject_subreason_total{reason=\"missing_trade_price\"} ");
    out.push_str(
        &ARB_TWO_HOP_REJECT_MISSING_TRADE_PRICE
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_reject_subreason_total{reason=\"no_comparable_price\"} ");
    out.push_str(
        &ARB_TWO_HOP_REJECT_NO_COMPARABLE_PRICE
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_reject_subreason_total{reason=\"stale_price\"} ");
    out.push_str(
        &ARB_TWO_HOP_REJECT_STALE_PRICE
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_reject_subreason_total{reason=\"same_dex_only\"} ");
    out.push_str(
        &ARB_TWO_HOP_REJECT_SAME_DEX_ONLY
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_reject_subreason_total{reason=\"implausible_price\"} ");
    out.push_str(
        &ARB_TWO_HOP_REJECT_IMPLAUSIBLE_PRICE
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_reject_subreason_total{reason=\"only_one_eligible_pool\"} ");
    out.push_str(
        &ARB_TWO_HOP_REJECT_ONLY_ONE_ELIGIBLE_POOL
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_reject_subreason_total{reason=\"only_one_eligible_dex\"} ");
    out.push_str(
        &ARB_TWO_HOP_REJECT_ONLY_ONE_ELIGIBLE_DEX
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
}

/// Increment `arb_two_hop_reject_subreason_total{reason=...}` for any documented reject subreason.
pub fn arb_two_hop_reject_subreason_inc(reason: ArbTwoHopRejectSubreason) {
    let counter = match reason {
        ArbTwoHopRejectSubreason::NotKnownPool => &*ARB_TWO_HOP_REJECT_NOT_KNOWN_POOL,
        ArbTwoHopRejectSubreason::MissingDecimals => &*ARB_TWO_HOP_REJECT_MISSING_DECIMALS,
        ArbTwoHopRejectSubreason::MissingReserves => &*ARB_TWO_HOP_REJECT_MISSING_RESERVES,
        ArbTwoHopRejectSubreason::MissingTradePrice => &*ARB_TWO_HOP_REJECT_MISSING_TRADE_PRICE,
        ArbTwoHopRejectSubreason::NoComparablePrice => &*ARB_TWO_HOP_REJECT_NO_COMPARABLE_PRICE,
        ArbTwoHopRejectSubreason::StalePrice => &*ARB_TWO_HOP_REJECT_STALE_PRICE,
        ArbTwoHopRejectSubreason::SameDexOnly => &*ARB_TWO_HOP_REJECT_SAME_DEX_ONLY,
        ArbTwoHopRejectSubreason::ImplausiblePrice => &*ARB_TWO_HOP_REJECT_IMPLAUSIBLE_PRICE,
        ArbTwoHopRejectSubreason::OnlyOneEligiblePool => {
            &*ARB_TWO_HOP_REJECT_ONLY_ONE_ELIGIBLE_POOL
        }
        ArbTwoHopRejectSubreason::OnlyOneEligibleDex => &*ARB_TWO_HOP_REJECT_ONLY_ONE_ELIGIBLE_DEX,
    };
    counter.fetch_add(1, Ordering::Relaxed);
}

/// Pool-gate stages aggregated across 2-hop eligibility checks (`gate` label).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArbTwoHopPoolGate {
    CandidatePools,
    InKnownPools,
    FreshPrice,
    HasReserveData,
    HasTradeMid,
    HasDecimals,
    ComparablePricePresent,
    ComparablePricePlausible,
    EligiblePools,
}

pub static ARB_TWO_HOP_GATE_CANDIDATE_POOLS: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_GATE_IN_KNOWN_POOLS: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_GATE_FRESH_PRICE: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_GATE_HAS_RESERVE_DATA: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_GATE_HAS_TRADE_MID: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_GATE_HAS_DECIMALS: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_GATE_COMPARABLE_PRICE_PRESENT: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_GATE_COMPARABLE_PRICE_PLAUSIBLE: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_GATE_ELIGIBLE_POOLS: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_ELIGIBLE_DEXES_CHECKS_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_ELIGIBLE_ORCA: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_ELIGIBLE_METEORA_DLMM: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_ELIGIBLE_PUMP_AMM: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_ELIGIBLE_RAYDIUM: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_ELIGIBLE_RAYDIUM_CPMM: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TWO_HOP_ELIGIBLE_PUMPFUN: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

/// Add pool counts from one mint eligibility check to gate aggregate counters.
pub fn arb_two_hop_pool_gate_add(gate: ArbTwoHopPoolGate, count: u64) {
    if count == 0 {
        return;
    }
    let counter = match gate {
        ArbTwoHopPoolGate::CandidatePools => &*ARB_TWO_HOP_GATE_CANDIDATE_POOLS,
        ArbTwoHopPoolGate::InKnownPools => &*ARB_TWO_HOP_GATE_IN_KNOWN_POOLS,
        ArbTwoHopPoolGate::FreshPrice => &*ARB_TWO_HOP_GATE_FRESH_PRICE,
        ArbTwoHopPoolGate::HasReserveData => &*ARB_TWO_HOP_GATE_HAS_RESERVE_DATA,
        ArbTwoHopPoolGate::HasTradeMid => &*ARB_TWO_HOP_GATE_HAS_TRADE_MID,
        ArbTwoHopPoolGate::HasDecimals => &*ARB_TWO_HOP_GATE_HAS_DECIMALS,
        ArbTwoHopPoolGate::ComparablePricePresent => &*ARB_TWO_HOP_GATE_COMPARABLE_PRICE_PRESENT,
        ArbTwoHopPoolGate::ComparablePricePlausible => {
            &*ARB_TWO_HOP_GATE_COMPARABLE_PRICE_PLAUSIBLE
        }
        ArbTwoHopPoolGate::EligiblePools => &*ARB_TWO_HOP_GATE_ELIGIBLE_POOLS,
    };
    counter.fetch_add(count, Ordering::Relaxed);
}

/// Add distinct eligible DEX count from one mint check.
pub fn arb_two_hop_eligible_dexes_add(count: u64) {
    if count > 0 {
        ARB_TWO_HOP_ELIGIBLE_DEXES_CHECKS_TOTAL.fetch_add(count, Ordering::Relaxed);
    }
}

/// Add eligible pool count per DEX from one mint check (fixed DEX labels only).
pub fn arb_two_hop_eligible_pools_by_dex_add(dex: &str, count: u64) {
    if count == 0 {
        return;
    }
    let counter = match dex {
        "orca" => &*ARB_TWO_HOP_ELIGIBLE_ORCA,
        "meteora_dlmm" => &*ARB_TWO_HOP_ELIGIBLE_METEORA_DLMM,
        "pump_amm" => &*ARB_TWO_HOP_ELIGIBLE_PUMP_AMM,
        "raydium" => &*ARB_TWO_HOP_ELIGIBLE_RAYDIUM,
        "raydium_cpmm" => &*ARB_TWO_HOP_ELIGIBLE_RAYDIUM_CPMM,
        "pumpfun" => &*ARB_TWO_HOP_ELIGIBLE_PUMPFUN,
        _ => return,
    };
    counter.fetch_add(count, Ordering::Relaxed);
}

// --- Arb-strategy MarketEvent subscriber pipeline ---
pub static ARB_SUBSCRIBER_HIGH_QUEUE_DEPTH: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_SUBSCRIBER_LOW_QUEUE_DEPTH: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_SUBSCRIBER_HIGH_PROCESSED_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_SUBSCRIBER_LOW_PROCESSED_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_SUBSCRIBER_LOW_COALESCED_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_SUBSCRIBER_LOW_DROPPED_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_SUBSCRIBER_POOL_CREATED_SKIPPED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static ARB_SUBSCRIBER_HIGH_DROPPED_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_EVENT_WORKER_STALL_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

pub fn arb_subscriber_high_queue_depth_set(depth: u64) {
    ARB_SUBSCRIBER_HIGH_QUEUE_DEPTH.store(depth, Ordering::Relaxed);
}

pub fn arb_subscriber_low_queue_depth_set(depth: u64) {
    ARB_SUBSCRIBER_LOW_QUEUE_DEPTH.store(depth, Ordering::Relaxed);
}

pub fn arb_subscriber_high_processed_inc() {
    ARB_SUBSCRIBER_HIGH_PROCESSED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

pub fn arb_subscriber_low_processed_inc() {
    ARB_SUBSCRIBER_LOW_PROCESSED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

pub fn arb_subscriber_low_coalesced_inc() {
    ARB_SUBSCRIBER_LOW_COALESCED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

pub fn arb_subscriber_low_dropped_inc() {
    ARB_SUBSCRIBER_LOW_DROPPED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

pub fn arb_subscriber_pool_created_skipped_inc() {
    ARB_SUBSCRIBER_POOL_CREATED_SKIPPED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

pub fn arb_subscriber_high_dropped_inc() {
    ARB_SUBSCRIBER_HIGH_DROPPED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

pub fn arb_event_worker_stall_inc() {
    ARB_EVENT_WORKER_STALL_TOTAL.fetch_add(1, Ordering::Relaxed);
}

pub static MARKET_DATA_WALLET_SNAPSHOT_PERIODIC_PUBLISHED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

pub fn market_data_wallet_snapshot_periodic_published_inc() {
    MARKET_DATA_WALLET_SNAPSHOT_PERIODIC_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

// --- Multi-hop shadow / cycle sanity (arb-strategy) ---
pub static MULTI_HOP_RETURN_BPS_SATURATED_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static MULTI_HOP_SHADOW_LOGGED_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static MULTI_HOP_HOP_MISSING_QUOTE_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static MULTI_HOP_SEARCH_WORKER_QUEUE_DEPTH: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static MULTI_HOP_SEARCHES_COALESCED_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static MULTI_HOP_QUOTE_FROM_CACHE_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static MULTI_HOP_QUOTE_FROM_TRADE_CACHE_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MULTI_HOP_QUOTE_FROM_POOL_QUOTE_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static MULTI_HOP_CYCLE_REJECTED_SANITY_EDGE_RATIO: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MULTI_HOP_CYCLE_REJECTED_SANITY_PROFIT_CAP: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MULTI_HOP_CYCLE_REJECTED_SANITY_RETURN_BPS_CAP: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
pub static MULTI_HOP_QUOTE_READY_POOLS: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static MULTI_HOP_QUOTE_READY_WSOL_EDGE_POOLS: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static MULTI_HOP_SEARCH_NO_QUOTE_NEIGHBORS_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));

pub fn multi_hop_quote_ready_pools_set(count: u64) {
    MULTI_HOP_QUOTE_READY_POOLS.store(count, Ordering::Relaxed);
}

pub fn multi_hop_quote_ready_wsol_edge_pools_set(count: u64) {
    MULTI_HOP_QUOTE_READY_WSOL_EDGE_POOLS.store(count, Ordering::Relaxed);
}

pub fn multi_hop_search_no_quote_neighbors_inc() {
    MULTI_HOP_SEARCH_NO_QUOTE_NEIGHBORS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Sanity rejection reason for multi-hop cycles (Prometheus label `reason`).
#[derive(Debug, Clone, Copy)]
pub enum MultiHopSanityRejectReason {
    EdgeRatio,
    ProfitCap,
    ReturnBpsCap,
}

pub fn multi_hop_return_bps_saturated_inc() {
    MULTI_HOP_RETURN_BPS_SATURATED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

pub fn multi_hop_shadow_logged_inc() {
    MULTI_HOP_SHADOW_LOGGED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

pub fn multi_hop_hop_missing_quote_inc() {
    MULTI_HOP_HOP_MISSING_QUOTE_TOTAL.fetch_add(1, Ordering::Relaxed);
}

pub fn multi_hop_search_worker_queue_depth_set(depth: u64) {
    MULTI_HOP_SEARCH_WORKER_QUEUE_DEPTH.store(depth, Ordering::Relaxed);
}

pub fn multi_hop_searches_coalesced_inc() {
    MULTI_HOP_SEARCHES_COALESCED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

pub fn multi_hop_quote_from_cache_inc() {
    MULTI_HOP_QUOTE_FROM_CACHE_TOTAL.fetch_add(1, Ordering::Relaxed);
}

pub fn multi_hop_quote_from_trade_cache_inc() {
    MULTI_HOP_QUOTE_FROM_TRADE_CACHE_TOTAL.fetch_add(1, Ordering::Relaxed);
}

pub fn multi_hop_quote_from_pool_quote_inc() {
    MULTI_HOP_QUOTE_FROM_POOL_QUOTE_TOTAL.fetch_add(1, Ordering::Relaxed);
}

pub fn multi_hop_cycle_rejected_sanity_inc(reason: MultiHopSanityRejectReason) {
    let counter = match reason {
        MultiHopSanityRejectReason::EdgeRatio => &*MULTI_HOP_CYCLE_REJECTED_SANITY_EDGE_RATIO,
        MultiHopSanityRejectReason::ProfitCap => &*MULTI_HOP_CYCLE_REJECTED_SANITY_PROFIT_CAP,
        MultiHopSanityRejectReason::ReturnBpsCap => {
            &*MULTI_HOP_CYCLE_REJECTED_SANITY_RETURN_BPS_CAP
        }
    };
    counter.fetch_add(1, Ordering::Relaxed);
}
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
/// PA-5.1: `authority_open - momentum_overlay_count` (signed; Prometheus scalar).
pub static POSITION_AUTHORITY_DRIFT_MOMENTUM: Lazy<AtomicI64> = Lazy::new(|| AtomicI64::new(0));
pub static CONCURRENT_INTENTS_GAUGE: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
/// Pending TradeIntents in `intent_rx` between JetStream enqueue and dispatcher recv.
pub static EXECUTION_INTENT_RX_QUEUE_DEPTH: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
/// JetStream `num_pending` for execution-engine pool-cache live consumer (cold-path sync).
pub static EXECUTION_POOL_CACHE_CONSUMER_PENDING: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
/// JetStream `num_pending` for execution-engine wallet-snapshot live consumer.
pub static EXECUTION_WALLET_SNAPSHOT_CONSUMER_PENDING: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
/// PoolCacheUpdate messages applied by the EE live consumer task.
pub static EXECUTION_POOL_CACHE_MESSAGES_PROCESSED_TOTAL: Lazy<AtomicU64> =
    Lazy::new(|| AtomicU64::new(0));
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

/// Wall-clock send→confirm latency (ms). Canonical histogram vs `TX_CONFIRM_LATENCY_MS` gauge.
#[inline]
pub fn record_tx_send_to_confirm_ms(ms: u64) {
    record_histogram_u64_into(
        TX_SEND_TO_CONFIRM_MS_BUCKETS,
        TX_SEND_TO_CONFIRM_MS_BUCKET_COUNTS.as_slice(),
        &TX_SEND_TO_CONFIRM_MS_SUM,
        &TX_SEND_TO_CONFIRM_MS_COUNT,
        ms,
        MOMENTUM_LATENCY_MS_SUM_CAP,
    );
}

/// On-chain slot delta: `confirmed_slot.saturating_sub(slot_at_send)` (0 when `slot_at_send==0`).
#[inline]
pub fn record_tx_confirmed_slot_delta_slots(delta_slots: u64) {
    record_histogram_u64_into(
        TX_CONFIRMED_SLOT_DELTA_SLOTS_BUCKETS,
        TX_CONFIRMED_SLOT_DELTA_SLOTS_BUCKET_COUNTS.as_slice(),
        &TX_CONFIRMED_SLOT_DELTA_SLOTS_SUM,
        &TX_CONFIRMED_SLOT_DELTA_SLOTS_COUNT,
        delta_slots,
        u64::MAX,
    );
}

/// `source` must be `"static_floor"` or `"dynamic"`.
#[inline]
pub fn record_tx_priority_fee_source(source: &str) {
    match source {
        "static_floor" => {
            TX_PRIORITY_FEE_SOURCE_STATIC_FLOOR_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        "dynamic" => {
            TX_PRIORITY_FEE_SOURCE_DYNAMIC_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        _ => {}
    }
}

#[inline]
pub fn record_tx_rebroadcast() {
    TX_REBROADCAST_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// `method` must be `"tpu"` or `"rpc"`.
#[inline]
pub fn record_tx_rebroadcast_method(method: &str) {
    match method {
        "tpu" => {
            TX_REBROADCAST_METHOD_TPU_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        "rpc" => {
            TX_REBROADCAST_METHOD_RPC_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        _ => {}
    }
}

#[inline]
pub fn record_tx_rebroadcast_during_confirm_ms(ms: u64) {
    record_histogram_u64_into(
        TX_REBROADCAST_DURING_CONFIRM_MS_BUCKETS,
        TX_REBROADCAST_DURING_CONFIRM_MS_BUCKET_COUNTS.as_slice(),
        &TX_REBROADCAST_DURING_CONFIRM_MS_SUM,
        &TX_REBROADCAST_DURING_CONFIRM_MS_COUNT,
        ms,
        MOMENTUM_LATENCY_MS_SUM_CAP,
    );
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

/// JetStream fetch task: TradeIntent deserialize → successful channel enqueue (ms).
#[inline]
pub fn record_execution_intent_jetstream_to_channel_ms(ms: u64) {
    record_histogram_u64_into(
        EXECUTION_INTENT_TO_CONFIRM_MS_BUCKETS,
        EXECUTION_INTENT_JETSTREAM_TO_CHANNEL_MS_BUCKET_COUNTS.as_slice(),
        &EXECUTION_INTENT_JETSTREAM_TO_CHANNEL_MS_SUM,
        &EXECUTION_INTENT_JETSTREAM_TO_CHANNEL_MS_COUNT,
        ms,
        MOMENTUM_LATENCY_MS_SUM_CAP,
    );
}

/// JetStream consumer successfully enqueued a TradeIntent into `intent_rx`.
#[inline]
pub fn inc_execution_intent_rx_queue_depth() -> u64 {
    EXECUTION_INTENT_RX_QUEUE_DEPTH.fetch_add(1, Ordering::Relaxed) + 1
}

/// Intent dispatcher dequeued a TradeIntent from `intent_rx`.
#[inline]
pub fn dec_execution_intent_rx_queue_depth() -> u64 {
    let mut depth = EXECUTION_INTENT_RX_QUEUE_DEPTH.load(Ordering::Relaxed);
    loop {
        let new_depth = depth.saturating_sub(1);
        match EXECUTION_INTENT_RX_QUEUE_DEPTH.compare_exchange_weak(
            depth,
            new_depth,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => return new_depth,
            Err(actual) => depth = actual,
        }
    }
}

#[cfg(test)]
pub fn reset_execution_intent_rx_queue_depth_for_test() {
    EXECUTION_INTENT_RX_QUEUE_DEPTH.store(0, Ordering::Relaxed);
}

/// Update JetStream pending count for the EE pool-cache live consumer.
#[inline]
pub fn set_execution_pool_cache_consumer_pending(pending: u64) {
    EXECUTION_POOL_CACHE_CONSUMER_PENDING.store(pending, Ordering::Relaxed);
}

/// Update JetStream pending count for the EE wallet-snapshot live consumer.
#[inline]
pub fn set_execution_wallet_snapshot_consumer_pending(pending: u64) {
    EXECUTION_WALLET_SNAPSHOT_CONSUMER_PENDING.store(pending, Ordering::Relaxed);
}

/// Increment when the EE pool-cache consumer applies updates.
#[inline]
pub fn inc_execution_pool_cache_messages_processed(count: u64) {
    EXECUTION_POOL_CACHE_MESSAGES_PROCESSED_TOTAL.fetch_add(count, Ordering::Relaxed);
}

#[cfg(test)]
pub fn reset_execution_consumer_metrics_for_test() {
    EXECUTION_POOL_CACHE_CONSUMER_PENDING.store(0, Ordering::Relaxed);
    EXECUTION_WALLET_SNAPSHOT_CONSUMER_PENDING.store(0, Ordering::Relaxed);
    EXECUTION_POOL_CACHE_MESSAGES_PROCESSED_TOTAL.store(0, Ordering::Relaxed);
}

/// Channel enqueue → intent-dispatcher `intent_rx.recv` before `process_intent` spawn (ms).
#[inline]
pub fn record_execution_intent_channel_wait_ms(ms: u64) {
    record_histogram_u64_into(
        EXECUTION_INTENT_TO_CONFIRM_MS_BUCKETS,
        EXECUTION_INTENT_CHANNEL_WAIT_MS_BUCKET_COUNTS.as_slice(),
        &EXECUTION_INTENT_CHANNEL_WAIT_MS_SUM,
        &EXECUTION_INTENT_CHANNEL_WAIT_MS_COUNT,
        ms,
        MOMENTUM_LATENCY_MS_SUM_CAP,
    );
}

/// PoolCache + WalletSnapshot JetStream batch work inside main-loop heartbeat (ms).
/// After cold-path consumer isolation, this tracks heartbeat/control work only.
#[inline]
pub fn record_execution_engine_interval_tick_duration_ms(ms: u64) {
    record_histogram_u64_into(
        EXECUTION_INTENT_TO_CONFIRM_MS_BUCKETS,
        EXECUTION_ENGINE_INTERVAL_TICK_DURATION_MS_BUCKET_COUNTS.as_slice(),
        &EXECUTION_ENGINE_INTERVAL_TICK_DURATION_MS_SUM,
        &EXECUTION_ENGINE_INTERVAL_TICK_DURATION_MS_COUNT,
        ms,
        MOMENTUM_LATENCY_MS_SUM_CAP,
    );
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

fn append_account_channel_lag_ms_labeled_histogram(
    out: &mut String,
    class: &str,
    bucket_counts: &[AtomicU64],
    sum: &AtomicU64,
    count: &AtomicU64,
) {
    let c = count.load(Ordering::Relaxed);
    let s = sum.load(Ordering::Relaxed);
    for (i, b) in MARKET_DATA_GEYSER_TO_PUBLISH_MS_BUCKETS.iter().enumerate() {
        let v = bucket_counts[i].load(Ordering::Relaxed);
        out.push_str(&format!(
            "market_data_account_channel_lag_ms_bucket{{class=\"{class}\",le=\"{b}\"}} {v}\n"
        ));
    }
    out.push_str(&format!(
        "market_data_account_channel_lag_ms_bucket{{class=\"{class}\",le=\"+Inf\"}} {c}\n"
    ));
    out.push_str(&format!(
        "market_data_account_channel_lag_ms_sum{{class=\"{class}\"}} {s}\n"
    ));
    out.push_str(&format!(
        "market_data_account_channel_lag_ms_count{{class=\"{class}\"}} {c}\n"
    ));
}

/// Append `_bucket{le=...}`, `_sum`, `_count` lines (same layout as `tx_slot_to_send_ms`).
#[allow(clippy::too_many_arguments)]
fn append_labeled_duration_seconds_histogram(
    out: &mut String,
    metric: &str,
    label_key: &str,
    label_value: &str,
    buckets_ns: &[u64],
    bucket_counts: &[AtomicU64],
    sum: &AtomicU64,
    count: &AtomicU64,
) {
    let c = count.load(Ordering::Relaxed);
    let s = sum.load(Ordering::Relaxed);
    for (i, b) in buckets_ns.iter().enumerate() {
        let v = bucket_counts[i].load(Ordering::Relaxed);
        out.push_str(&format!(
            "{metric}_bucket{{{label_key}=\"{label_value}\",le=\"{}\"}} {v}\n",
            (*b as f64) / 1e9
        ));
    }
    out.push_str(&format!(
        "{metric}_bucket{{{label_key}=\"{label_value}\",le=\"+Inf\"}} {c}\n"
    ));
    out.push_str(&format!(
        "{metric}_sum{{{label_key}=\"{label_value}\"}} {}\n",
        (s as f64) / 1e9
    ));
    out.push_str(&format!(
        "{metric}_count{{{label_key}=\"{label_value}\"}} {c}\n"
    ));
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
        "market_data_track_worker_queue_depth",
        MARKET_DATA_TRACK_WORKER_QUEUE_DEPTH.load(Ordering::Relaxed)
    );
    line!(
        "market_data_momentum_track_worker_enqueue_dropped_total",
        MARKET_DATA_MOMENTUM_TRACK_WORKER_ENQUEUE_DROPPED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_momentum_admission_admitted_total",
        MARKET_DATA_MOMENTUM_ADMISSION_ADMITTED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_momentum_admission_rejected_total",
        MARKET_DATA_MOMENTUM_ADMISSION_REJECTED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_open_position_pin_applied_total",
        MARKET_DATA_OPEN_POSITION_PIN_APPLIED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_open_position_pin_deferred_cache_miss_total",
        MARKET_DATA_OPEN_POSITION_PIN_DEFERRED_CACHE_MISS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_arb_admission_admitted_total",
        MARKET_DATA_ARB_ADMISSION_ADMITTED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_arb_admission_rejected_total",
        MARKET_DATA_ARB_ADMISSION_REJECTED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_arb_pin_registration_incomplete",
        MARKET_DATA_ARB_PIN_REGISTRATION_INCOMPLETE.load(Ordering::Relaxed)
    );
    line!(
        "market_data_arb_shed_skipped_must_hot_total{reason=\"must_hot\"}",
        MARKET_DATA_ARB_SHED_SKIPPED_MUST_HOT_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_wallet_admission_admitted_total",
        MARKET_DATA_WALLET_ADMISSION_ADMITTED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_wallet_admission_rejected_total",
        MARKET_DATA_WALLET_ADMISSION_REJECTED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_tracker_admission_admitted_total",
        MARKET_DATA_TRACKER_ADMISSION_ADMITTED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_tracker_admission_rejected_total",
        MARKET_DATA_TRACKER_ADMISSION_REJECTED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_track_protocol_pending_depth",
        MARKET_DATA_TRACK_PROTOCOL_PENDING_DEPTH.load(Ordering::Relaxed)
    );
    line!(
        "market_data_track_protocol_inflight_depth",
        MARKET_DATA_TRACK_PROTOCOL_INFLIGHT_DEPTH.load(Ordering::Relaxed)
    );
    line!(
        "market_data_track_protocol_replay_triggers_total",
        MARKET_DATA_TRACK_PROTOCOL_REPLAY_TRIGGERS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_track_protocol_superseded_revisions_total",
        MARKET_DATA_TRACK_PROTOCOL_SUPERSEDED_REVISIONS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_track_protocol_pending_evicted_total",
        MARKET_DATA_TRACK_PROTOCOL_PENDING_EVICTED_TOTAL.load(Ordering::Relaxed)
    );
    for (name, idx) in [
        ("momentum", 0usize),
        ("arb", 1),
        ("wallet", 2),
        ("tracker", 3),
        ("sync", 4),
        ("continue", 5),
        ("other", 6),
    ] {
        line!(
            &format!("market_data_track_worker_enqueue_{name}_total"),
            MARKET_DATA_TRACK_WORKER_ENQUEUE_BY_KIND_TOTAL[idx].load(Ordering::Relaxed)
        );
        line!(
            &format!("market_data_track_protocol_stage_{name}_total"),
            MARKET_DATA_TRACK_PROTOCOL_STAGE_BY_KIND_TOTAL[idx].load(Ordering::Relaxed)
        );
    }
    line!(
        "market_data_track_worker_enqueue_deduped_total",
        MARKET_DATA_TRACK_WORKER_ENQUEUE_DEDUPED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_track_protocol_pending_coalesced_total",
        MARKET_DATA_TRACK_PROTOCOL_PENDING_COALESCED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_track_mint_skipped_already_tracked_total",
        MARKET_DATA_TRACK_MINT_SKIPPED_ALREADY_TRACKED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_md_state_track_mint_coalesce_messages_in_total",
        MARKET_DATA_MD_STATE_TRACK_MINT_COALESCE_MESSAGES_IN_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_md_state_track_mint_coalesce_batches_out_total",
        MARKET_DATA_MD_STATE_TRACK_MINT_COALESCE_BATCHES_OUT_TOTAL.load(Ordering::Relaxed)
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
        "market_data_md_state_bursts_completed_total",
        MARKET_DATA_MD_STATE_BURSTS_COMPLETED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_md_state_stalls_total",
        MARKET_DATA_MD_STATE_STALLS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_md_state_evict_steps_total",
        MARKET_DATA_MD_STATE_EVICT_STEPS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_md_state_evict_steps_budget_exhausted_total",
        MARKET_DATA_MD_STATE_EVICT_STEPS_BUDGET_EXHAUSTED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_geyser_sync_partial_total",
        MARKET_DATA_GEYSER_SYNC_PARTIAL_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_md_state_evict_pending",
        MARKET_DATA_MD_STATE_EVICT_PENDING.load(Ordering::Relaxed)
    );
    line!(
        "market_data_md_state_burst_in_progress",
        MARKET_DATA_MD_STATE_BURST_IN_PROGRESS.load(Ordering::Relaxed)
    );
    line!(
        "market_data_md_state_deferred_jobs_len",
        MARKET_DATA_MD_STATE_DEFERRED_JOBS_LEN.load(Ordering::Relaxed)
    );
    line!(
        "market_data_md_state_register_skipped_idempotent_total",
        MARKET_DATA_MD_STATE_REGISTER_SKIPPED_IDEMPOTENT_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_discovery_deferred_md_state_pressure_total",
        MARKET_DATA_DISCOVERY_DEFERRED_MD_STATE_PRESSURE_TOTAL.load(Ordering::Relaxed)
    );
    append_momentum_latency_histogram_prometheus(
        &mut out,
        "market_data_md_state_sync_flush_duration_us",
        MARKET_DATA_MD_STATE_SYNC_FLUSH_DURATION_US_BUCKETS,
        &MARKET_DATA_MD_STATE_SYNC_FLUSH_DURATION_US_BUCKET_COUNTS,
        &MARKET_DATA_MD_STATE_SYNC_FLUSH_DURATION_US_SUM,
        &MARKET_DATA_MD_STATE_SYNC_FLUSH_DURATION_US_COUNT,
    );
    append_momentum_latency_histogram_prometheus(
        &mut out,
        "market_data_md_state_writer_wait_us",
        MARKET_DATA_MD_STATE_WRITER_WAIT_US_BUCKETS,
        &MARKET_DATA_MD_STATE_WRITER_WAIT_US_BUCKET_COUNTS,
        &MARKET_DATA_MD_STATE_WRITER_WAIT_US_SUM,
        &MARKET_DATA_MD_STATE_WRITER_WAIT_US_COUNT,
    );
    line!(
        "market_data_tracked_membership_snapshot_age_ms",
        market_data_tracked_membership_snapshot_age_ms()
    );
    line!(
        "market_data_ingest_membership_snapshot_hits_total",
        MARKET_DATA_INGEST_MEMBERSHIP_SNAPSHOT_HITS_TOTAL.load(Ordering::Relaxed)
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
        "market_data_md_sidefx_enrich_enqueue_dropped_total",
        MARKET_DATA_MD_SIDEFX_ENRICH_ENQUEUE_DROPPED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_md_sidefx_enrich_publish_skipped_total",
        MARKET_DATA_MD_SIDEFX_ENRICH_PUBLISH_SKIPPED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_md_sidefx_jobs_processed_total",
        MARKET_DATA_MD_SIDEFX_JOBS_PROCESSED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_devwallet_tx_published_total",
        MARKET_DATA_DEVWALLET_TX_PUBLISHED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_devwallet_bonding_path_total",
        MARKET_DATA_DEVWALLET_BONDING_PATH_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_unparsed_tx_dropped_total",
        MARKET_DATA_UNPARSED_TX_DROPPED_TOTAL.load(Ordering::Relaxed)
    );
    out.push_str("market_data_unparsed_tx_dropped_total{reason=\"non_dex_transaction\"} ");
    out.push_str(
        &MARKET_DATA_UNPARSED_TX_DROPPED_NON_DEX_TRANSACTION
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("market_data_unparsed_tx_dropped_total{reason=\"dex_parse_miss\"} ");
    out.push_str(
        &MARKET_DATA_UNPARSED_TX_DROPPED_DEX_PARSE_MISS
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    line!(
        "market_data_unparsed_account_dropped_total",
        MARKET_DATA_UNPARSED_ACCOUNT_DROPPED_TOTAL.load(Ordering::Relaxed)
    );
    out.push_str("market_data_unparsed_account_dropped_total{reason=\"legacy_dex_parse_miss\"} ");
    out.push_str(
        &MARKET_DATA_UNPARSED_ACCOUNT_DROPPED_LEGACY_DEX_PARSE_MISS
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
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
        "market_data_wallet_snapshot_periodic_published_total",
        MARKET_DATA_WALLET_SNAPSHOT_PERIODIC_PUBLISHED_TOTAL.load(Ordering::Relaxed)
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
        "arb_track_requests_messages_total",
        ARB_TRACK_REQUESTS_MESSAGES_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "arb_track_requests_publish_failed_total",
        ARB_TRACK_REQUESTS_PUBLISH_FAILED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "arb_track_requests_publish_chunks_total",
        ARB_TRACK_REQUESTS_PUBLISH_CHUNKS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_arb_track_requests_messages_total",
        MARKET_DATA_ARB_TRACK_REQUESTS_MESSAGES_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_arb_track_coalesced_messages_total",
        MARKET_DATA_ARB_TRACK_COALESCED_MESSAGES_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_arb_track_coalesced_batches_total",
        MARKET_DATA_ARB_TRACK_COALESCED_BATCHES_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_arb_registered_vaults_total",
        MARKET_DATA_ARB_REGISTERED_VAULTS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_vault_high_priority_dispatch_total",
        MARKET_DATA_VAULT_HIGH_PRIORITY_DISPATCH_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_arb_pin_evictions_total",
        MARKET_DATA_ARB_PIN_EVICTIONS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_arb_pinned_pools",
        MARKET_DATA_ARB_PINNED_POOLS_GAUGE.load(Ordering::Relaxed)
    );
    line!(
        "market_data_arb_pin_readd_cooldown_suppressed_total",
        MARKET_DATA_ARB_PIN_READD_COOLDOWN_SUPPRESSED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_arb_pin_pool_evictions_total",
        MARKET_DATA_ARB_PIN_POOL_EVICTIONS_TOTAL.load(Ordering::Relaxed)
    );
    out.push_str("market_data_arb_pin_eviction_reason{reason=\"budget\"} ");
    out.push_str(
        &MARKET_DATA_ARB_PIN_EVICTION_REASON_BUDGET
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("market_data_arb_pin_eviction_reason{reason=\"stale_budget\"} ");
    out.push_str(
        &MARKET_DATA_ARB_PIN_EVICTION_REASON_STALE_BUDGET
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("market_data_arb_pin_eviction_reason{reason=\"active_protected\"} ");
    out.push_str(
        &MARKET_DATA_ARB_PIN_EVICTION_REASON_ACTIVE_PROTECTED
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str(
        "market_data_arb_pin_geyser_register_deferred_total{reason=\"live_pool_cache_miss\"} ",
    );
    out.push_str(
        &MARKET_DATA_ARB_PIN_GEYSER_REGISTER_DEFERRED_LIVE_POOL_CACHE_MISS
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str(
        "market_data_arb_pin_geyser_register_deferred_total{reason=\"vault_register_no_change\"} ",
    );
    out.push_str(
        &MARKET_DATA_ARB_PIN_GEYSER_REGISTER_DEFERRED_VAULT_NO_CHANGE
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    line!(
        "market_data_arb_pin_budget_used",
        MARKET_DATA_ARB_PIN_BUDGET_USED_GAUGE.load(Ordering::Relaxed)
    );
    line!(
        "market_data_arb_reconcile_attempts_total",
        MARKET_DATA_ARB_RECONCILE_ATTEMPTS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_arb_reconcile_pools_registered_total",
        MARKET_DATA_ARB_RECONCILE_POOLS_REGISTERED_TOTAL.load(Ordering::Relaxed)
    );
    out.push_str("market_data_arb_reconcile_skipped_total{reason=\"not_multi_dex\"} ");
    out.push_str(
        &MARKET_DATA_ARB_RECONCILE_SKIPPED_NOT_MULTI_DEX_TOTAL
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("market_data_arb_reconcile_skipped_total{reason=\"partial_state\"} ");
    out.push_str(
        &MARKET_DATA_ARB_RECONCILE_SKIPPED_PARTIAL_STATE_TOTAL
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("market_data_arb_reconcile_skipped_total{reason=\"no_common_quote\"} ");
    out.push_str(
        &MARKET_DATA_ARB_RECONCILE_SKIPPED_NO_COMMON_QUOTE_TOTAL
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("market_data_arb_reconcile_skipped_total{reason=\"cooldown\"} ");
    out.push_str(
        &MARKET_DATA_ARB_RECONCILE_SKIPPED_COOLDOWN_TOTAL
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("market_data_arb_reconcile_skipped_total{reason=\"budget\"} ");
    out.push_str(
        &MARKET_DATA_ARB_RECONCILE_SKIPPED_BUDGET_TOTAL
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("market_data_arb_reconcile_skipped_total{reason=\"already_pinned\"} ");
    out.push_str(
        &MARKET_DATA_ARB_RECONCILE_SKIPPED_ALREADY_PINNED_TOTAL
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    line!(
        "market_data_arb_reconcile_selected_pools_total",
        MARKET_DATA_ARB_RECONCILE_SELECTED_POOLS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_arb_reconcile_unselected_pools_due_to_cap_total",
        MARKET_DATA_ARB_RECONCILE_UNSELECTED_POOLS_DUE_TO_CAP_TOTAL.load(Ordering::Relaxed)
    );
    out.push_str("market_data_arb_reconcile_skipped_total{reason=\"active_budget_protected\"} ");
    out.push_str(
        &MARKET_DATA_ARB_RECONCILE_SKIPPED_ACTIVE_BUDGET_PROTECTED_TOTAL
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("market_data_arb_reconcile_skipped_total{reason=\"oversized_pool\"} ");
    out.push_str(
        &MARKET_DATA_ARB_RECONCILE_SKIPPED_OVERSIZED_POOL_TOTAL
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    line!(
        "market_data_arb_coverage_index_updates_total",
        MARKET_DATA_ARB_COVERAGE_INDEX_UPDATES_TOTAL.load(Ordering::Relaxed)
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
        "market_data_geyser_sync_skipped_no_delta_total",
        MARKET_DATA_GEYSER_SYNC_SKIPPED_NO_DELTA_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_geyser_sync_skipped_rate_limit_total",
        MARKET_DATA_GEYSER_SYNC_SKIPPED_RATE_LIMIT_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_geyser_explicit_set_size",
        MARKET_DATA_GEYSER_EXPLICIT_SET_SIZE.load(Ordering::Relaxed)
    );
    line!(
        "market_data_geyser_explicit_admitted_accounts",
        MARKET_DATA_GEYSER_EXPLICIT_ADMITTED_ACCOUNTS.load(Ordering::Relaxed)
    );
    line!(
        "market_data_geyser_explicit_cap_overflow",
        MARKET_DATA_GEYSER_EXPLICIT_CAP_OVERFLOW.load(Ordering::Relaxed)
    );
    line!(
        "market_data_geyser_subscribe_delta_pubkeys",
        MARKET_DATA_GEYSER_SUBSCRIBE_DELTA_PUBKEYS.load(Ordering::Relaxed)
    );
    line!(
        "market_data_track_request_coalesce_batches_total",
        MARKET_DATA_TRACK_REQUEST_COALESCE_BATCHES_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_explicit_set_snapshot_write_total",
        MARKET_DATA_EXPLICIT_SET_SNAPSHOT_WRITE_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_explicit_set_snapshot_write_errors_total",
        MARKET_DATA_EXPLICIT_SET_SNAPSHOT_WRITE_ERRORS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_explicit_set_snapshot_restore_pubkeys",
        MARKET_DATA_EXPLICIT_SET_SNAPSHOT_RESTORE_PUBKEYS.load(Ordering::Relaxed)
    );
    line!(
        "market_data_explicit_set_snapshot_restore_duration_ms",
        MARKET_DATA_EXPLICIT_SET_SNAPSHOT_RESTORE_DURATION_MS.load(Ordering::Relaxed)
    );
    out.push_str("market_data_hot_pool_registry_pools{reason=\"momentum\"} ");
    out.push_str(
        &MARKET_DATA_HOT_POOL_REGISTRY_POOLS_MOMENTUM
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("market_data_hot_pool_registry_pools{reason=\"arb\"} ");
    out.push_str(
        &MARKET_DATA_HOT_POOL_REGISTRY_POOLS_ARB
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("market_data_hot_pool_registry_pools{reason=\"both\"} ");
    out.push_str(
        &MARKET_DATA_HOT_POOL_REGISTRY_POOLS_BOTH
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    line!(
        "market_data_balance_updated_from_cache_total",
        MARKET_DATA_BALANCE_UPDATED_FROM_CACHE_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_enrichment_balance_updated_total",
        MARKET_DATA_ENRICHMENT_BALANCE_UPDATED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_enrichment_pool_state_publish_total",
        MARKET_DATA_ENRICHMENT_POOL_STATE_PUBLISH_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_account_relevance_enrichment_hit_total",
        MARKET_DATA_ACCOUNT_RELEVANCE_ENRICHMENT_HIT_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_enrichment_registry_pools_gauge",
        MARKET_DATA_ENRICHMENT_REGISTRY_POOLS_GAUGE.load(Ordering::Relaxed)
    );
    line!(
        "market_data_pool_state_publish_skipped_total{reason=\"balance_unchanged\"}",
        MARKET_DATA_POOL_STATE_PUBLISH_SKIPPED_BALANCE_UNCHANGED.load(Ordering::Relaxed)
    );
    out.push_str("market_data_pool_state_publish_total{dex=\"orca\"} ");
    out.push_str(
        &MARKET_DATA_POOL_STATE_PUBLISH_ORCA
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("market_data_pool_state_publish_total{dex=\"meteora_dlmm\"} ");
    out.push_str(
        &MARKET_DATA_POOL_STATE_PUBLISH_METEORA_DLMM
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("market_data_pool_state_publish_total{dex=\"pump_amm\"} ");
    out.push_str(
        &MARKET_DATA_POOL_STATE_PUBLISH_PUMP_AMM
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("market_data_pool_state_publish_total{dex=\"raydium\"} ");
    out.push_str(
        &MARKET_DATA_POOL_STATE_PUBLISH_RAYDIUM
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("market_data_pool_state_publish_total{dex=\"raydium_cpmm\"} ");
    out.push_str(
        &MARKET_DATA_POOL_STATE_PUBLISH_RAYDIUM_CPMM
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("market_data_pool_state_publish_total{dex=\"meteora_cpmm\"} ");
    out.push_str(
        &MARKET_DATA_POOL_STATE_PUBLISH_METEORA_CPMM
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("market_data_pool_state_publish_total{dex=\"pumpfun\"} ");
    out.push_str(
        &MARKET_DATA_POOL_STATE_PUBLISH_PUMPFUN
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("market_data_pool_state_publish_total{dex=\"other\"} ");
    out.push_str(
        &MARKET_DATA_POOL_STATE_PUBLISH_OTHER
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    line!(
        "market_data_bin_array_publish_total",
        MARKET_DATA_BIN_ARRAY_PUBLISH_TOTAL.load(Ordering::Relaxed)
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
    append_account_channel_lag_ms_labeled_histogram(
        &mut out,
        "exec_hot",
        &MARKET_DATA_ACCOUNT_CHANNEL_LAG_MS_EXEC_HOT_BUCKET_COUNTS,
        &MARKET_DATA_ACCOUNT_CHANNEL_LAG_MS_EXEC_HOT_SUM,
        &MARKET_DATA_ACCOUNT_CHANNEL_LAG_MS_EXEC_HOT_COUNT,
    );
    append_account_channel_lag_ms_labeled_histogram(
        &mut out,
        "enrich",
        &MARKET_DATA_ACCOUNT_CHANNEL_LAG_MS_ENRICH_BUCKET_COUNTS,
        &MARKET_DATA_ACCOUNT_CHANNEL_LAG_MS_ENRICH_SUM,
        &MARKET_DATA_ACCOUNT_CHANNEL_LAG_MS_ENRICH_COUNT,
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
        "market_data_account_broadcast_lagged_total{class=\"exec_hot\"}",
        MARKET_DATA_ACCOUNT_BROADCAST_LAGGED_TOTAL_EXEC_HOT.load(Ordering::Relaxed)
    );
    line!(
        "market_data_account_broadcast_lagged_total{class=\"enrich\"}",
        MARKET_DATA_ACCOUNT_BROADCAST_LAGGED_TOTAL_ENRICH.load(Ordering::Relaxed)
    );
    line!(
        "market_data_account_broadcast_queue_depth",
        MARKET_DATA_ACCOUNT_BROADCAST_QUEUE_DEPTH.load(Ordering::Relaxed)
    );
    line!(
        "market_data_account_broadcast_queue_depth{class=\"exec_hot\"}",
        MARKET_DATA_ACCOUNT_BROADCAST_QUEUE_DEPTH_EXEC_HOT.load(Ordering::Relaxed)
    );
    line!(
        "market_data_account_broadcast_queue_depth{class=\"enrich\"}",
        MARKET_DATA_ACCOUNT_BROADCAST_QUEUE_DEPTH_ENRICH.load(Ordering::Relaxed)
    );
    line!(
        "market_data_account_worker_count",
        MARKET_DATA_ACCOUNT_WORKER_COUNT.load(Ordering::Relaxed)
    );
    line!(
        "market_data_account_exec_hot_worker_count",
        MARKET_DATA_ACCOUNT_EXEC_HOT_WORKER_COUNT.load(Ordering::Relaxed)
    );
    line!(
        "market_data_account_enrich_worker_count",
        MARKET_DATA_ACCOUNT_ENRICH_WORKER_COUNT.load(Ordering::Relaxed)
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
        "market_data_account_enrich_coalesce_total",
        MARKET_DATA_ACCOUNT_ENRICH_COALESCE_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_account_enrich_enqueue_dropped_total",
        MARKET_DATA_ACCOUNT_ENRICH_ENQUEUE_DROPPED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_account_enrich_ingress_queue_depth",
        MARKET_DATA_ACCOUNT_ENRICH_INGRESS_QUEUE_DEPTH.load(Ordering::Relaxed)
    );
    line!(
        "market_data_account_enrich_dispatch_contended_total",
        MARKET_DATA_ACCOUNT_ENRICH_DISPATCH_CONTENDED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_account_high_enqueue_dropped_total",
        MARKET_DATA_ACCOUNT_HIGH_ENQUEUE_DROPPED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_exec_hot_shed_soft_active",
        MARKET_DATA_EXEC_HOT_SHED_SOFT_ACTIVE.load(Ordering::Relaxed)
    );
    line!(
        "market_data_account_enrich_shed_dropped_total",
        MARKET_DATA_ACCOUNT_ENRICH_SHED_DROPPED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_exec_hot_hard_shed_steps_total",
        MARKET_DATA_EXEC_HOT_HARD_SHED_STEPS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_exec_hot_hard_shed_groups_evicted_total",
        MARKET_DATA_EXEC_HOT_HARD_SHED_GROUPS_EVICTED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "market_data_exec_hot_shed_tier",
        MARKET_DATA_EXEC_HOT_SHED_TIER.load(Ordering::Relaxed)
    );
    line!(
        "market_data_exec_hot_hard_shed_steps_total{tier=\"tracker\"}",
        MARKET_DATA_EXEC_HOT_HARD_SHED_STEPS_TRACKER.load(Ordering::Relaxed)
    );
    line!(
        "market_data_exec_hot_hard_shed_steps_total{tier=\"momentum\"}",
        MARKET_DATA_EXEC_HOT_HARD_SHED_STEPS_MOMENTUM.load(Ordering::Relaxed)
    );
    line!(
        "market_data_exec_hot_hard_shed_steps_total{tier=\"arb\"}",
        MARKET_DATA_EXEC_HOT_HARD_SHED_STEPS_ARB.load(Ordering::Relaxed)
    );
    line!(
        "market_data_exec_hot_hard_shed_groups_evicted_total{tier=\"tracker\"}",
        MARKET_DATA_EXEC_HOT_HARD_SHED_GROUPS_TRACKER.load(Ordering::Relaxed)
    );
    line!(
        "market_data_exec_hot_hard_shed_groups_evicted_total{tier=\"momentum\"}",
        MARKET_DATA_EXEC_HOT_HARD_SHED_GROUPS_MOMENTUM.load(Ordering::Relaxed)
    );
    line!(
        "market_data_exec_hot_hard_shed_groups_evicted_total{tier=\"arb\"}",
        MARKET_DATA_EXEC_HOT_HARD_SHED_GROUPS_ARB.load(Ordering::Relaxed)
    );
    line!(
        "market_data_exec_hot_pressure_admit_rejected_total{tier=\"tracker\"}",
        MARKET_DATA_EXEC_HOT_PRESSURE_ADMIT_REJECTED_TRACKER.load(Ordering::Relaxed)
    );
    line!(
        "market_data_exec_hot_pressure_admit_rejected_total{tier=\"momentum\"}",
        MARKET_DATA_EXEC_HOT_PRESSURE_ADMIT_REJECTED_MOMENTUM.load(Ordering::Relaxed)
    );
    line!(
        "market_data_exec_hot_pressure_admit_rejected_total{tier=\"arb\"}",
        MARKET_DATA_EXEC_HOT_PRESSURE_ADMIT_REJECTED_ARB.load(Ordering::Relaxed)
    );
    line!(
        "market_data_exec_hot_lag_p50_est_ms",
        MARKET_DATA_EXEC_HOT_LAG_P50_EST_MS.load(Ordering::Relaxed)
    );
    line!(
        "market_data_exec_hot_lag_p99_est_ms",
        MARKET_DATA_EXEC_HOT_LAG_P99_EST_MS.load(Ordering::Relaxed)
    );
    line!(
        "market_data_exec_hot_lag_alarm",
        MARKET_DATA_EXEC_HOT_LAG_ALARM.load(Ordering::Relaxed)
    );
    line!(
        "market_data_exec_hot_hard_shed_steps_total{tier=\"tracker\",reason=\"lag\"}",
        MARKET_DATA_EXEC_HOT_HARD_SHED_STEPS_TRACKER_LAG.load(Ordering::Relaxed)
    );
    line!(
        "market_data_exec_hot_hard_shed_steps_total{tier=\"momentum\",reason=\"lag\"}",
        MARKET_DATA_EXEC_HOT_HARD_SHED_STEPS_MOMENTUM_LAG.load(Ordering::Relaxed)
    );
    line!(
        "market_data_exec_hot_hard_shed_steps_total{tier=\"arb\",reason=\"lag\"}",
        MARKET_DATA_EXEC_HOT_HARD_SHED_STEPS_ARB_LAG.load(Ordering::Relaxed)
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
    out.push_str("market_data_account_early_drop_total{reason=\"non_dex_non_membership\"} ");
    out.push_str(
        &MARKET_DATA_ACCOUNT_EARLY_DROP_NON_DEX_NON_MEMBERSHIP
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("market_data_account_early_drop_total{reason=\"dex_pool_not_enrichment\"} ");
    out.push_str(
        &MARKET_DATA_ACCOUNT_EARLY_DROP_DEX_POOL_NOT_ENRICHMENT
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("market_data_account_updates_total{class=\"exec_hot\"} ");
    out.push_str(
        &MARKET_DATA_ACCOUNT_UPDATES_TOTAL_EXEC_HOT
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("market_data_account_updates_total{class=\"enrich\"} ");
    out.push_str(
        &MARKET_DATA_ACCOUNT_UPDATES_TOTAL_ENRICH
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("market_data_account_updates_total{class=\"drop\"} ");
    out.push_str(
        &MARKET_DATA_ACCOUNT_UPDATES_TOTAL_DROP
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    append_momentum_latency_histogram_prometheus(
        &mut out,
        "market_data_account_handler_duration_us",
        MARKET_DATA_ACCOUNT_HANDLER_DURATION_US_BUCKETS,
        &MARKET_DATA_ACCOUNT_HANDLER_DURATION_US_BUCKET_COUNTS,
        &MARKET_DATA_ACCOUNT_HANDLER_DURATION_US_SUM,
        &MARKET_DATA_ACCOUNT_HANDLER_DURATION_US_COUNT,
    );
    line!(
        "market_data_account_recv_iterations_total",
        MARKET_DATA_ACCOUNT_RECV_ITERATIONS_TOTAL.load(Ordering::Relaxed)
    );
    append_momentum_latency_histogram_prometheus(
        &mut out,
        "market_data_account_recv_classify_duration_us",
        MARKET_DATA_ACCOUNT_RECV_CLASSIFY_DURATION_US_BUCKETS,
        &MARKET_DATA_ACCOUNT_RECV_CLASSIFY_DURATION_US_BUCKET_COUNTS,
        &MARKET_DATA_ACCOUNT_RECV_CLASSIFY_DURATION_US_SUM,
        &MARKET_DATA_ACCOUNT_RECV_CLASSIFY_DURATION_US_COUNT,
    );
    append_momentum_latency_histogram_prometheus(
        &mut out,
        "market_data_account_recv_high_enqueue_duration_us",
        MARKET_DATA_ACCOUNT_RECV_HIGH_ENQUEUE_DURATION_US_BUCKETS,
        &MARKET_DATA_ACCOUNT_RECV_HIGH_ENQUEUE_DURATION_US_BUCKET_COUNTS,
        &MARKET_DATA_ACCOUNT_RECV_HIGH_ENQUEUE_DURATION_US_SUM,
        &MARKET_DATA_ACCOUNT_RECV_HIGH_ENQUEUE_DURATION_US_COUNT,
    );
    append_momentum_latency_histogram_prometheus(
        &mut out,
        "market_data_account_recv_enrich_ingress_duration_us",
        MARKET_DATA_ACCOUNT_RECV_ENRICH_INGRESS_DURATION_US_BUCKETS,
        &MARKET_DATA_ACCOUNT_RECV_ENRICH_INGRESS_DURATION_US_BUCKET_COUNTS,
        &MARKET_DATA_ACCOUNT_RECV_ENRICH_INGRESS_DURATION_US_SUM,
        &MARKET_DATA_ACCOUNT_RECV_ENRICH_INGRESS_DURATION_US_COUNT,
    );
    append_momentum_latency_histogram_prometheus(
        &mut out,
        "market_data_account_recv_iteration_duration_us",
        MARKET_DATA_ACCOUNT_RECV_ITERATION_DURATION_US_BUCKETS,
        &MARKET_DATA_ACCOUNT_RECV_ITERATION_DURATION_US_BUCKET_COUNTS,
        &MARKET_DATA_ACCOUNT_RECV_ITERATION_DURATION_US_SUM,
        &MARKET_DATA_ACCOUNT_RECV_ITERATION_DURATION_US_COUNT,
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
    out.push_str("filter_rejection_by_reason{reason=\"downtrend\"} ");
    out.push_str(
        &FILTER_REJECTED_DOWNTREND
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("filter_rejection_by_reason{reason=\"token_age\"} ");
    out.push_str(
        &FILTER_REJECTED_TOKEN_AGE
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
        "momentum_orphan_probe_recovery_total",
        MOMENTUM_ORPHAN_PROBE_RECOVERY_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "momentum_orphan_scale_in_recovery_total",
        MOMENTUM_ORPHAN_SCALE_IN_RECOVERY_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "momentum_exit_amount_overlay_only_total",
        MOMENTUM_EXIT_AMOUNT_OVERLAY_ONLY_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "momentum_overlay_closed_by_authority_total",
        MOMENTUM_OVERLAY_CLOSED_BY_AUTHORITY_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "momentum_wallet_balance_divergence_total",
        MOMENTUM_WALLET_BALANCE_DIVERGENCE_TOTAL.load(Ordering::Relaxed)
    );
    for (mint, delta) in MOMENTUM_WALLET_BALANCE_DIVERGENCE_BY_MINT.read().iter() {
        out.push_str("momentum_wallet_balance_divergence_lamports{mint=\"");
        out.push_str(mint);
        out.push_str("\"} ");
        out.push_str(&delta.to_string());
        out.push('\n');
    }
    out.push_str("momentum_scale_in_gate_blocked_total{reason=\"missing_probe_state\"} ");
    out.push_str(
        &MOMENTUM_SCALE_IN_GATE_BLOCKED_MISSING_PROBE_STATE
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("momentum_scale_in_gate_blocked_total{reason=\"pnl\"} ");
    out.push_str(
        &MOMENTUM_SCALE_IN_GATE_BLOCKED_PNL
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("momentum_scale_in_gate_blocked_total{reason=\"window_expired\"} ");
    out.push_str(
        &MOMENTUM_SCALE_IN_GATE_BLOCKED_WINDOW_EXPIRED
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("momentum_scale_in_gate_blocked_total{reason=\"no_quote\"} ");
    out.push_str(
        &MOMENTUM_SCALE_IN_GATE_BLOCKED_NO_QUOTE
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("momentum_filter_pass_hot_fresh_total{fresh=\"true\"} ");
    out.push_str(
        &MOMENTUM_FILTER_PASS_HOT_FRESH_TRUE
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("momentum_filter_pass_hot_fresh_total{fresh=\"false\"} ");
    out.push_str(
        &MOMENTUM_FILTER_PASS_HOT_FRESH_FALSE
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    for (key, count) in MOMENTUM_ENTRY_HOT_FRESH_FAIL_TOTAL.read().iter() {
        let (reason, dex) = key.split_once('|').unwrap_or((key.as_str(), "other"));
        out.push_str("momentum_entry_hot_fresh_fail_total{reason=\"");
        out.push_str(reason);
        out.push_str("\",dex=\"");
        out.push_str(dex);
        out.push_str("\"} ");
        out.push_str(&count.to_string());
        out.push('\n');
    }
    line!(
        "momentum_wait_hot_set_enter_total",
        MOMENTUM_WAIT_HOT_SET_ENTER_TOTAL.load(Ordering::Relaxed)
    );
    out.push_str("momentum_wait_hot_set_exit_total{reason=\"intent\"} ");
    out.push_str(
        &MOMENTUM_WAIT_HOT_SET_EXIT_INTENT
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("momentum_wait_hot_set_exit_total{reason=\"timeout\"} ");
    out.push_str(
        &MOMENTUM_WAIT_HOT_SET_EXIT_TIMEOUT
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("momentum_wait_hot_set_exit_total{reason=\"filter_failed\"} ");
    out.push_str(
        &MOMENTUM_WAIT_HOT_SET_EXIT_FILTER_FAILED
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    append_momentum_latency_histogram_prometheus(
        &mut out,
        "momentum_wait_hot_set_duration_ms",
        EXECUTION_INTENT_TO_CONFIRM_MS_BUCKETS,
        &MOMENTUM_WAIT_HOT_SET_DURATION_MS_BUCKET_COUNTS,
        &MOMENTUM_WAIT_HOT_SET_DURATION_MS_SUM,
        &MOMENTUM_WAIT_HOT_SET_DURATION_MS_COUNT,
    );
    out.push_str("momentum_intent_path_total{path=\"immediate_hot\"} ");
    out.push_str(
        &MOMENTUM_INTENT_PATH_IMMEDIATE_HOT
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("momentum_intent_path_total{path=\"after_wait_hot\"} ");
    out.push_str(
        &MOMENTUM_INTENT_PATH_AFTER_WAIT_HOT
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("market_data_momentum_pin_vault_register_total{result=\"ok\"} ");
    out.push_str(
        &MARKET_DATA_MOMENTUM_PIN_VAULT_REGISTER_OK
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("market_data_momentum_pin_vault_register_total{result=\"cache_miss\"} ");
    out.push_str(
        &MARKET_DATA_MOMENTUM_PIN_VAULT_REGISTER_CACHE_MISS
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("market_data_momentum_pin_vault_register_total{result=\"admission_rejected\"} ");
    out.push_str(
        &MARKET_DATA_MOMENTUM_PIN_VAULT_REGISTER_ADMISSION_REJECTED
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("market_data_momentum_pin_vault_register_total{result=\"already_satisfied\"} ");
    out.push_str(
        &MARKET_DATA_MOMENTUM_PIN_VAULT_REGISTER_ALREADY_SATISFIED
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("market_data_momentum_pin_vault_register_total{result=\"deferred\"} ");
    out.push_str(
        &MARKET_DATA_MOMENTUM_PIN_VAULT_REGISTER_DEFERRED
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
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
    // PR3: JetStream-based TX confirmation breakdown
    line!(
        "tx_confirm_jetstream_total",
        TX_CONFIRM_JETSTREAM_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "tx_confirm_deserialize_errors_total",
        TX_CONFIRM_DESERIALIZE_ERRORS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "tx_confirm_jetstream_orphan_buffered_total",
        TX_CONFIRM_JETSTREAM_ORPHAN_BUFFERED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "tx_confirm_jetstream_orphan_hit_total",
        TX_CONFIRM_JETSTREAM_ORPHAN_HIT_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "tx_confirm_jetstream_orphan_evicted_total",
        TX_CONFIRM_JETSTREAM_ORPHAN_EVICTED_TOTAL.load(Ordering::Relaxed)
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
        "tx_send_to_confirm_ms",
        TX_SEND_TO_CONFIRM_MS_BUCKETS,
        &TX_SEND_TO_CONFIRM_MS_BUCKET_COUNTS,
        &TX_SEND_TO_CONFIRM_MS_SUM,
        &TX_SEND_TO_CONFIRM_MS_COUNT,
    );
    append_momentum_latency_histogram_prometheus(
        &mut out,
        "tx_confirmed_slot_delta_slots",
        TX_CONFIRMED_SLOT_DELTA_SLOTS_BUCKETS,
        &TX_CONFIRMED_SLOT_DELTA_SLOTS_BUCKET_COUNTS,
        &TX_CONFIRMED_SLOT_DELTA_SLOTS_SUM,
        &TX_CONFIRMED_SLOT_DELTA_SLOTS_COUNT,
    );
    out.push_str("tx_priority_fee_source_total{source=\"static_floor\"} ");
    out.push_str(
        &TX_PRIORITY_FEE_SOURCE_STATIC_FLOOR_TOTAL
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("tx_priority_fee_source_total{source=\"dynamic\"} ");
    out.push_str(
        &TX_PRIORITY_FEE_SOURCE_DYNAMIC_TOTAL
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    line!(
        "tx_rebroadcast_total",
        TX_REBROADCAST_TOTAL.load(Ordering::Relaxed)
    );
    out.push_str("tx_rebroadcast_method_total{method=\"tpu\"} ");
    out.push_str(
        &TX_REBROADCAST_METHOD_TPU_TOTAL
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("tx_rebroadcast_method_total{method=\"rpc\"} ");
    out.push_str(
        &TX_REBROADCAST_METHOD_RPC_TOTAL
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    append_momentum_latency_histogram_prometheus(
        &mut out,
        "tx_rebroadcast_during_confirm_ms",
        TX_REBROADCAST_DURING_CONFIRM_MS_BUCKETS,
        &TX_REBROADCAST_DURING_CONFIRM_MS_BUCKET_COUNTS,
        &TX_REBROADCAST_DURING_CONFIRM_MS_SUM,
        &TX_REBROADCAST_DURING_CONFIRM_MS_COUNT,
    );
    append_momentum_latency_histogram_prometheus(
        &mut out,
        "execution_intent_jetstream_to_channel_ms",
        EXECUTION_INTENT_TO_CONFIRM_MS_BUCKETS,
        &EXECUTION_INTENT_JETSTREAM_TO_CHANNEL_MS_BUCKET_COUNTS,
        &EXECUTION_INTENT_JETSTREAM_TO_CHANNEL_MS_SUM,
        &EXECUTION_INTENT_JETSTREAM_TO_CHANNEL_MS_COUNT,
    );
    append_momentum_latency_histogram_prometheus(
        &mut out,
        "execution_intent_channel_wait_ms",
        EXECUTION_INTENT_TO_CONFIRM_MS_BUCKETS,
        &EXECUTION_INTENT_CHANNEL_WAIT_MS_BUCKET_COUNTS,
        &EXECUTION_INTENT_CHANNEL_WAIT_MS_SUM,
        &EXECUTION_INTENT_CHANNEL_WAIT_MS_COUNT,
    );
    append_momentum_latency_histogram_prometheus(
        &mut out,
        "execution_engine_interval_tick_duration_ms",
        EXECUTION_INTENT_TO_CONFIRM_MS_BUCKETS,
        &EXECUTION_ENGINE_INTERVAL_TICK_DURATION_MS_BUCKET_COUNTS,
        &EXECUTION_ENGINE_INTERVAL_TICK_DURATION_MS_SUM,
        &EXECUTION_ENGINE_INTERVAL_TICK_DURATION_MS_COUNT,
    );
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
        "wallet_tx_confirm_listener_connected",
        WALLET_TX_CONFIRM_LISTENER_CONNECTED.load(Ordering::Relaxed) as u64
    );
    line!(
        "simulation_failures_total",
        SIMULATION_FAILURES_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "sim_timeout_total",
        SIM_TIMEOUT_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "intents_expired_total",
        INTENTS_EXPIRED_TOTAL.load(Ordering::Relaxed)
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
    line!(
        "in_flight_capital_reservations",
        IN_FLIGHT_CAPITAL_RESERVATIONS.load(Ordering::Relaxed)
    );
    line!(
        "capital_lock_expired_released_total",
        CAPITAL_LOCK_EXPIRED_RELEASED_TOTAL.load(Ordering::Relaxed)
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
        "arb_two_hop_opportunities_total",
        ARB_TWO_HOP_OPPORTUNITIES.load(Ordering::Relaxed)
    );
    line!(
        "arb_two_hop_tracker_seeded_pools_total",
        ARB_TWO_HOP_TRACKER_SEEDED_POOLS.load(Ordering::Relaxed)
    );
    line!(
        "arb_quote_shadow_round_trip_total",
        ARB_QUOTE_SHADOW_ROUND_TRIP_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "arb_quote_shadow_incompatible_kind_total",
        ARB_QUOTE_SHADOW_INCOMPATIBLE_KIND_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "arb_quote_shadow_round_trip_profit_sum_lamports",
        ARB_QUOTE_SHADOW_ROUND_TRIP_PROFIT_SUM.load(Ordering::Relaxed)
    );
    line!(
        "arb_quote_shadow_round_trip_profit_count",
        ARB_QUOTE_SHADOW_ROUND_TRIP_PROFIT_COUNT.load(Ordering::Relaxed)
    );
    line!(
        "arb_quote_shadow_legacy_spread_bps",
        ARB_QUOTE_SHADOW_LEGACY_SPREAD_BPS.load(Ordering::Relaxed)
    );
    line!(
        "arb_quote_shadow_v2_profit_lamports",
        ARB_QUOTE_SHADOW_V2_PROFIT_LAMPORTS.load(Ordering::Relaxed)
    );
    for (i, bucket) in ARB_QUOTE_SHADOW_PROFIT_BUCKETS.iter().enumerate() {
        out.push_str(&format!(
            "arb_quote_shadow_round_trip_profit_lamports_bucket{{le=\"{bucket}\"}} {}\n",
            ARB_QUOTE_SHADOW_PROFIT_BUCKET_COUNTS[i].load(Ordering::Relaxed)
        ));
    }
    out.push_str("arb_quote_shadow_round_trip_profit_lamports_bucket{le=\"+Inf\"} ");
    out.push_str(
        &ARB_QUOTE_SHADOW_ROUND_TRIP_PROFIT_COUNT
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    line!(
        "arb_two_hop_v2_screen_total",
        ARB_TWO_HOP_V2_SCREEN_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "arb_two_hop_v2_incompatible_kind_total",
        ARB_TWO_HOP_V2_INCOMPATIBLE_KIND_TOTAL.load(Ordering::Relaxed)
    );
    out.push_str("arb_two_hop_v2_rejected_total{reason=\"round_trip_unprofitable\"} ");
    out.push_str(
        &ARB_TWO_HOP_V2_REJECTED_ROUND_TRIP_UNPROFITABLE
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_v2_rejected_total{reason=\"quote_stale\"} ");
    out.push_str(
        &ARB_TWO_HOP_V2_REJECTED_QUOTE_STALE
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_v2_rejected_total{reason=\"incompatible_quote_kind\"} ");
    out.push_str(
        &ARB_TWO_HOP_V2_REJECTED_INCOMPATIBLE_QUOTE_KIND
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_v2_rejected_total{reason=\"insufficient_pools\"} ");
    out.push_str(
        &ARB_TWO_HOP_V2_REJECTED_INSUFFICIENT_POOLS
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_v2_rejected_total{reason=\"slot_delta_exceeded\"} ");
    out.push_str(
        &ARB_TWO_HOP_V2_REJECTED_SLOT_DELTA_EXCEEDED
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    line!(
        "arb_two_hop_v2_screen_multi_dex_total",
        ARB_TWO_HOP_V2_SCREEN_MULTI_DEX_TOTAL.load(Ordering::Relaxed)
    );
    out.push_str("arb_two_hop_v2_screen_skipped_total{reason=\"mint_not_selected\"} ");
    out.push_str(
        &ARB_TWO_HOP_V2_SCREEN_SKIPPED_MINT_NOT_SELECTED
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    line!(
        "arb_two_hop_v2_round_trip_formable_total",
        ARB_TWO_HOP_V2_ROUND_TRIP_FORMABLE_TOTAL.load(Ordering::Relaxed)
    );
    append_arb_two_hop_v2_insufficient_subreason_total(&mut out);
    append_arb_two_hop_v2_no_cross_dex_sell_detail_total(&mut out);
    append_arb_two_hop_v2_sell_quote_none_detail_total(&mut out);
    append_momentum_latency_histogram_prometheus(
        &mut out,
        "arb_quote_pair_slot_delta",
        ARB_QUOTE_PAIR_SLOT_DELTA_BUCKETS,
        ARB_QUOTE_PAIR_SLOT_DELTA_BUCKET_COUNTS.as_slice(),
        &ARB_QUOTE_PAIR_SLOT_DELTA_SUM,
        &ARB_QUOTE_PAIR_SLOT_DELTA_COUNT,
    );
    append_momentum_latency_histogram_prometheus(
        &mut out,
        "arb_track_pin_before_first_screen_ms",
        ARB_TRACK_PIN_BEFORE_FIRST_SCREEN_MS_BUCKETS,
        ARB_TRACK_PIN_BEFORE_FIRST_SCREEN_MS_BUCKET_COUNTS.as_slice(),
        &ARB_TRACK_PIN_BEFORE_FIRST_SCREEN_MS_SUM,
        &ARB_TRACK_PIN_BEFORE_FIRST_SCREEN_MS_COUNT,
    );
    line!(
        "arb_proactive_track_publish_total",
        ARB_PROACTIVE_TRACK_PUBLISH_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "arb_track_selected_pools",
        ARB_TRACK_SELECTED_POOLS_GAUGE.load(Ordering::Relaxed)
    );
    line!(
        "arb_track_selected_mints",
        ARB_TRACK_SELECTED_MINTS_GAUGE.load(Ordering::Relaxed)
    );
    line!(
        "arb_track_selected_pair_complete_mints",
        ARB_TRACK_SELECTED_PAIR_COMPLETE_MINTS_GAUGE.load(Ordering::Relaxed)
    );
    line!(
        "arb_track_selected_orphan_pools",
        ARB_TRACK_SELECTED_ORPHAN_POOLS_GAUGE.load(Ordering::Relaxed)
    );
    out.push_str("arb_track_candidate_pools_total{readiness=\"executable\"} ");
    out.push_str(
        &ARB_TRACK_CANDIDATE_POOLS_EXECUTABLE
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_track_candidate_pools_total{readiness=\"quote_ready\"} ");
    out.push_str(
        &ARB_TRACK_CANDIDATE_POOLS_QUOTE_READY
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_track_candidate_pools_total{readiness=\"warmable\"} ");
    out.push_str(
        &ARB_TRACK_CANDIDATE_POOLS_WARMABLE
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_track_candidate_pools_total{readiness=\"rejected\"} ");
    out.push_str(
        &ARB_TRACK_CANDIDATE_POOLS_REJECTED
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_track_selected_pool_readiness_total{readiness=\"executable\"} ");
    out.push_str(
        &ARB_TRACK_SELECTED_POOL_READINESS_EXECUTABLE
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_track_selected_pool_readiness_total{readiness=\"quote_ready\"} ");
    out.push_str(
        &ARB_TRACK_SELECTED_POOL_READINESS_QUOTE_READY
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_track_selected_pool_readiness_total{readiness=\"warmable\"} ");
    out.push_str(
        &ARB_TRACK_SELECTED_POOL_READINESS_WARMABLE
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_track_selected_pool_readiness_total{readiness=\"rejected\"} ");
    out.push_str(
        &ARB_TRACK_SELECTED_POOL_READINESS_REJECTED
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_track_removed_total{reason=\"budget\"} ");
    out.push_str(
        &ARB_TRACK_REMOVED_BUDGET_TOTAL
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_track_removed_total{reason=\"stale\"} ");
    out.push_str(
        &ARB_TRACK_REMOVED_STALE_TOTAL
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_track_removed_total{reason=\"cooldown\"} ");
    out.push_str(
        &ARB_TRACK_REMOVED_COOLDOWN_TOTAL
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    line!(
        "arb_track_publish_skipped_unchanged_total",
        ARB_TRACK_PUBLISH_SKIPPED_UNCHANGED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "arb_track_selection_recomputes_total",
        ARB_TRACK_SELECTION_RECOMPUTES_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "arb_track_selection_queue_overflow_total",
        ARB_TRACK_SELECTION_QUEUE_OVERFLOW_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "arb_track_selection_blocking_join_failed_total",
        ARB_TRACK_SELECTION_BLOCKING_JOIN_FAILED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "arb_strategy_bootstrap_live_pool_cache_rows",
        ARB_STRATEGY_BOOTSTRAP_LIVE_POOL_CACHE_ROWS.load(Ordering::Relaxed)
    );
    line!(
        "arb_strategy_bootstrap_known_pools_seeded",
        ARB_STRATEGY_BOOTSTRAP_KNOWN_POOLS.load(Ordering::Relaxed)
    );
    line!(
        "arb_strategy_bootstrap_tracker_seed_candidates",
        ARB_STRATEGY_BOOTSTRAP_TRACKER_SEED_CANDIDATES.load(Ordering::Relaxed)
    );
    line!(
        "arb_strategy_bootstrap_tracker_seeded_pools",
        ARB_STRATEGY_BOOTSTRAP_TRACKER_SEEDED_POOLS.load(Ordering::Relaxed)
    );
    out.push_str("arb_strategy_bootstrap_tracker_seed_skipped_total{reason=\"unknown_dex\"} ");
    out.push_str(
        &ARB_STRATEGY_BOOTSTRAP_SKIP_UNKNOWN_DEX
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_strategy_bootstrap_tracker_seed_skipped_total{reason=\"non_arb_quote\"} ");
    out.push_str(
        &ARB_STRATEGY_BOOTSTRAP_SKIP_NON_ARB_QUOTE
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_strategy_bootstrap_tracker_seed_skipped_total{reason=\"missing_reserves\"} ");
    out.push_str(
        &ARB_STRATEGY_BOOTSTRAP_SKIP_MISSING_RESERVES
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_strategy_bootstrap_tracker_seed_skipped_total{reason=\"zero_reserves\"} ");
    out.push_str(
        &ARB_STRATEGY_BOOTSTRAP_SKIP_ZERO_RESERVES
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str(
        "arb_strategy_bootstrap_tracker_seed_skipped_total{reason=\"native_token_mint\"} ",
    );
    out.push_str(
        &ARB_STRATEGY_BOOTSTRAP_SKIP_NATIVE_TOKEN
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    line!(
        "arb_strategy_pool_cache_updates_seen_total",
        ARB_STRATEGY_POOL_CACHE_UPDATES_SEEN.load(Ordering::Relaxed)
    );
    line!(
        "arb_strategy_pool_cache_updates_seeded_total",
        ARB_STRATEGY_POOL_CACHE_UPDATES_SEEDED.load(Ordering::Relaxed)
    );
    out.push_str("arb_strategy_pool_cache_updates_skipped_total{reason=\"non_arb_quote\"} ");
    out.push_str(
        &ARB_STRATEGY_POOL_CACHE_UPDATE_SKIP_NON_ARB_QUOTE
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_strategy_pool_cache_updates_skipped_total{reason=\"no_seed\"} ");
    out.push_str(
        &ARB_STRATEGY_POOL_CACHE_UPDATE_SKIP_NO_SEED
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    line!(
        "arb_pool_cache_updates_applied_total",
        ARB_POOL_CACHE_UPDATES_APPLIED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "arb_pool_cache_apply_batches_total",
        ARB_POOL_CACHE_APPLY_BATCHES_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "arb_pool_cache_apply_batch_size",
        ARB_POOL_CACHE_APPLY_BATCH_SIZE.load(Ordering::Relaxed)
    );
    line!(
        "arb_pool_cache_sync_messages_total",
        ARB_POOL_CACHE_SYNC_MESSAGES_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "arb_pool_cache_sync_fetch_empty_total",
        ARB_POOL_CACHE_SYNC_FETCH_EMPTY_TOTAL.load(Ordering::Relaxed)
    );
    append_arb_price_freshness_histograms(&mut out);
    line!(
        "arb_tracker_write_enqueue_dropped_total",
        ARB_TRACKER_WRITE_ENQUEUE_DROPPED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "arb_tracker_write_queue_depth",
        ARB_TRACKER_WRITE_QUEUE_DEPTH.load(Ordering::Relaxed)
    );
    line!(
        "arb_tracker_write_coalesced_total",
        ARB_TRACKER_WRITE_COALESCED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "arb_tracker_write_coalesced_flushed_total",
        ARB_TRACKER_WRITE_COALESCED_FLUSHED_TOTAL.load(Ordering::Relaxed)
    );
    out.push_str("arb_tracker_write_enqueue_dropped_total{job_type=\"pool_state_update\"} ");
    out.push_str(
        &ARB_TRACKER_WRITE_DROPPED_POOL_STATE_UPDATE
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_tracker_write_enqueue_dropped_total{job_type=\"apply_trade\"} ");
    out.push_str(
        &ARB_TRACKER_WRITE_DROPPED_APPLY_TRADE
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_tracker_write_enqueue_dropped_total{job_type=\"pool_created\"} ");
    out.push_str(
        &ARB_TRACKER_WRITE_DROPPED_POOL_CREATED
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_tracker_write_enqueue_dropped_total{job_type=\"dex_pool_accounts\"} ");
    out.push_str(
        &ARB_TRACKER_WRITE_DROPPED_DEX_POOL_ACCOUNTS
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_tracker_write_enqueue_dropped_total{job_type=\"token_mint_info\"} ");
    out.push_str(
        &ARB_TRACKER_WRITE_DROPPED_TOKEN_MINT_INFO
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_tracker_write_enqueue_dropped_total{job_type=\"seed_pool_cache\"} ");
    out.push_str(
        &ARB_TRACKER_WRITE_DROPPED_SEED_POOL_CACHE
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_tracker_write_enqueue_dropped_total{job_type=\"finalize_opportunity\"} ");
    out.push_str(
        &ARB_TRACKER_WRITE_DROPPED_FINALIZE_OPPORTUNITY
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_tracker_write_jobs_processed_total{job_type=\"pool_state_update\"} ");
    out.push_str(
        &ARB_TRACKER_WRITE_PROCESSED_POOL_STATE_UPDATE
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_tracker_write_jobs_processed_total{job_type=\"apply_trade\"} ");
    out.push_str(
        &ARB_TRACKER_WRITE_PROCESSED_APPLY_TRADE
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_tracker_write_jobs_processed_total{job_type=\"pool_created\"} ");
    out.push_str(
        &ARB_TRACKER_WRITE_PROCESSED_POOL_CREATED
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_tracker_write_jobs_processed_total{job_type=\"dex_pool_accounts\"} ");
    out.push_str(
        &ARB_TRACKER_WRITE_PROCESSED_DEX_POOL_ACCOUNTS
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_tracker_write_jobs_processed_total{job_type=\"token_mint_info\"} ");
    out.push_str(
        &ARB_TRACKER_WRITE_PROCESSED_TOKEN_MINT_INFO
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_tracker_write_jobs_processed_total{job_type=\"seed_pool_cache\"} ");
    out.push_str(
        &ARB_TRACKER_WRITE_PROCESSED_SEED_POOL_CACHE
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_tracker_write_jobs_processed_total{job_type=\"finalize_opportunity\"} ");
    out.push_str(
        &ARB_TRACKER_WRITE_PROCESSED_FINALIZE_OPPORTUNITY
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    for job_type in ArbTrackerWriteJobType::all() {
        out.push_str(&format!(
            "arb_tracker_write_job_started_total{{job_type=\"{}\"}} {}\n",
            job_type.prometheus_label(),
            ARB_TRACKER_WRITE_JOB_STARTED[job_type.index()].load(Ordering::Relaxed)
        ));
    }
    for job_type in ArbTrackerWriteJobType::all() {
        out.push_str(&format!(
            "arb_tracker_write_job_finished_total{{job_type=\"{}\"}} {}\n",
            job_type.prometheus_label(),
            ARB_TRACKER_WRITE_JOB_FINISHED[job_type.index()].load(Ordering::Relaxed)
        ));
    }
    for job_type in ArbTrackerWriteJobType::all() {
        append_labeled_duration_seconds_histogram(
            &mut out,
            "arb_tracker_write_job_duration_seconds",
            "job_type",
            job_type.prometheus_label(),
            ARB_TRACKER_WRITE_JOB_DURATION_NS_BUCKETS,
            &ARB_TRACKER_WRITE_JOB_DURATION[job_type.index()].bucket_counts,
            &ARB_TRACKER_WRITE_JOB_DURATION[job_type.index()].sum_ns,
            &ARB_TRACKER_WRITE_JOB_DURATION[job_type.index()].count,
        );
    }
    line!(
        "arb_tracker_write_last_job_type",
        ARB_TRACKER_WRITE_LAST_JOB_TYPE.load(Ordering::Relaxed)
    );
    line!(
        "arb_tracker_write_seconds_since_last_finish",
        ARB_TRACKER_WRITE_SECONDS_SINCE_LAST_FINISH.load(Ordering::Relaxed)
    );
    line!(
        "arb_tracker_write_current_job_type",
        ARB_TRACKER_WRITE_CURRENT_JOB_TYPE.load(Ordering::Relaxed)
    );
    for job_type in ArbTrackerWriteJobType::all() {
        out.push_str(&format!(
            "arb_tracker_write_coalescer_flush_lost_total{{job_type=\"{}\"}} {}\n",
            job_type.prometheus_label(),
            ARB_TRACKER_WRITE_COALESCER_FLUSH_LOST[job_type.index()].load(Ordering::Relaxed)
        ));
    }
    line!(
        "arb_tracker_write_coalescer_pending",
        ARB_TRACKER_WRITE_COALESCER_PENDING.load(Ordering::Relaxed)
    );
    line!(
        "arb_two_hop_blocked_on_apply_trade",
        ARB_TWO_HOP_BLOCKED_ON_APPLY_TRADE.load(Ordering::Relaxed)
    );
    line!(
        "arb_tracker_write_stall_watchdog_total",
        ARB_TRACKER_WRITE_STALL_WATCHDOG_TOTAL.load(Ordering::Relaxed)
    );
    for lock in [
        ArbWriterLockKind::TrackersRead,
        ArbWriterLockKind::TrackersWrite,
        ArbWriterLockKind::VaultBalancesWrite,
    ] {
        append_labeled_duration_seconds_histogram(
            &mut out,
            "arb_tracker_write_lock_wait_seconds",
            "lock",
            lock.prometheus_label(),
            ARB_TRACKER_WRITE_JOB_DURATION_NS_BUCKETS,
            &ARB_TRACKER_WRITE_LOCK_WAIT[lock.index()].bucket_counts,
            &ARB_TRACKER_WRITE_LOCK_WAIT[lock.index()].sum_ns,
            &ARB_TRACKER_WRITE_LOCK_WAIT[lock.index()].count,
        );
    }
    for phase in [
        ArbHeartbeatPhase::TrackersRead,
        ArbHeartbeatPhase::MaybeEmit,
        ArbHeartbeatPhase::SyncPools,
        ArbHeartbeatPhase::Prune,
        ArbHeartbeatPhase::InfoLog,
    ] {
        append_labeled_duration_seconds_histogram(
            &mut out,
            "arb_heartbeat_phase_duration_seconds",
            "phase",
            phase.prometheus_label(),
            ARB_TRACKER_WRITE_JOB_DURATION_NS_BUCKETS,
            &ARB_HEARTBEAT_PHASE_DURATION[phase.index()].bucket_counts,
            &ARB_HEARTBEAT_PHASE_DURATION[phase.index()].sum_ns,
            &ARB_HEARTBEAT_PHASE_DURATION[phase.index()].count,
        );
    }
    line!(
        "arb_heartbeat_seconds_since_last_finish",
        ARB_HEARTBEAT_SECONDS_SINCE_LAST_FINISH.load(Ordering::Relaxed)
    );
    line!(
        "arb_subscriber_high_queue_depth",
        ARB_SUBSCRIBER_HIGH_QUEUE_DEPTH.load(Ordering::Relaxed)
    );
    line!(
        "arb_subscriber_low_queue_depth",
        ARB_SUBSCRIBER_LOW_QUEUE_DEPTH.load(Ordering::Relaxed)
    );
    line!(
        "arb_subscriber_high_processed_total",
        ARB_SUBSCRIBER_HIGH_PROCESSED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "arb_subscriber_low_processed_total",
        ARB_SUBSCRIBER_LOW_PROCESSED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "arb_subscriber_low_coalesced_total",
        ARB_SUBSCRIBER_LOW_COALESCED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "arb_subscriber_low_dropped_total",
        ARB_SUBSCRIBER_LOW_DROPPED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "arb_subscriber_pool_created_skipped_total",
        ARB_SUBSCRIBER_POOL_CREATED_SKIPPED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "arb_subscriber_high_dropped_total",
        ARB_SUBSCRIBER_HIGH_DROPPED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "arb_event_worker_stall_total",
        ARB_EVENT_WORKER_STALL_TOTAL.load(Ordering::Relaxed)
    );
    out.push_str("arb_two_hop_rejected_total{reason=\"spread_too_large\"} ");
    out.push_str(
        &ARB_TWO_HOP_REJECTED_SPREAD_TOO_LARGE
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_rejected_total{reason=\"spread_below_min\"} ");
    out.push_str(
        &ARB_TWO_HOP_REJECTED_SPREAD_BELOW_MIN
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_rejected_total{reason=\"profit_below_min\"} ");
    out.push_str(
        &ARB_TWO_HOP_REJECTED_PROFIT_BELOW_MIN
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_rejected_total{reason=\"same_dex\"} ");
    out.push_str(
        &ARB_TWO_HOP_REJECTED_SAME_DEX
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_rejected_total{reason=\"pumpfun\"} ");
    out.push_str(
        &ARB_TWO_HOP_REJECTED_PUMPFUN
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_rejected_total{reason=\"insufficient_pools\"} ");
    out.push_str(
        &ARB_TWO_HOP_REJECTED_INSUFFICIENT_POOLS
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_rejected_total{reason=\"stale_price\"} ");
    out.push_str(
        &ARB_TWO_HOP_REJECTED_STALE_PRICE
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_rejected_total{reason=\"no_comparable_price\"} ");
    out.push_str(
        &ARB_TWO_HOP_REJECTED_NO_COMPARABLE_PRICE
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_rejected_total{reason=\"native_sol\"} ");
    out.push_str(
        &ARB_TWO_HOP_REJECTED_NATIVE_SOL
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_rejected_total{reason=\"data_quality\"} ");
    out.push_str(
        &ARB_TWO_HOP_REJECTED_DATA_QUALITY
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    append_arb_two_hop_insufficient_subreason_total(&mut out);
    append_arb_two_hop_reject_subreason_total(&mut out);
    out.push_str("arb_two_hop_pool_gate_pools_total{gate=\"candidate_pools\"} ");
    out.push_str(
        &ARB_TWO_HOP_GATE_CANDIDATE_POOLS
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_pool_gate_pools_total{gate=\"known_pools\"} ");
    out.push_str(
        &ARB_TWO_HOP_GATE_IN_KNOWN_POOLS
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_pool_gate_pools_total{gate=\"fresh_price\"} ");
    out.push_str(
        &ARB_TWO_HOP_GATE_FRESH_PRICE
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_pool_gate_pools_total{gate=\"has_reserve_data\"} ");
    out.push_str(
        &ARB_TWO_HOP_GATE_HAS_RESERVE_DATA
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_pool_gate_pools_total{gate=\"has_trade_mid\"} ");
    out.push_str(
        &ARB_TWO_HOP_GATE_HAS_TRADE_MID
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_pool_gate_pools_total{gate=\"has_decimals\"} ");
    out.push_str(
        &ARB_TWO_HOP_GATE_HAS_DECIMALS
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_pool_gate_pools_total{gate=\"comparable_price_present\"} ");
    out.push_str(
        &ARB_TWO_HOP_GATE_COMPARABLE_PRICE_PRESENT
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_pool_gate_pools_total{gate=\"comparable_price_plausible\"} ");
    out.push_str(
        &ARB_TWO_HOP_GATE_COMPARABLE_PRICE_PLAUSIBLE
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_pool_gate_pools_total{gate=\"eligible_pools\"} ");
    out.push_str(
        &ARB_TWO_HOP_GATE_ELIGIBLE_POOLS
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    line!(
        "arb_two_hop_eligible_dexes_checks_total",
        ARB_TWO_HOP_ELIGIBLE_DEXES_CHECKS_TOTAL.load(Ordering::Relaxed)
    );
    out.push_str("arb_two_hop_eligible_pools_by_dex_total{dex=\"orca\"} ");
    out.push_str(
        &ARB_TWO_HOP_ELIGIBLE_ORCA
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_eligible_pools_by_dex_total{dex=\"meteora_dlmm\"} ");
    out.push_str(
        &ARB_TWO_HOP_ELIGIBLE_METEORA_DLMM
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_eligible_pools_by_dex_total{dex=\"pump_amm\"} ");
    out.push_str(
        &ARB_TWO_HOP_ELIGIBLE_PUMP_AMM
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_eligible_pools_by_dex_total{dex=\"raydium\"} ");
    out.push_str(
        &ARB_TWO_HOP_ELIGIBLE_RAYDIUM
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_eligible_pools_by_dex_total{dex=\"raydium_cpmm\"} ");
    out.push_str(
        &ARB_TWO_HOP_ELIGIBLE_RAYDIUM_CPMM
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("arb_two_hop_eligible_pools_by_dex_total{dex=\"pumpfun\"} ");
    out.push_str(
        &ARB_TWO_HOP_ELIGIBLE_PUMPFUN
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    line!(
        "multi_hop_return_bps_saturated_total",
        MULTI_HOP_RETURN_BPS_SATURATED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "multi_hop_shadow_logged_total",
        MULTI_HOP_SHADOW_LOGGED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "multi_hop_hop_missing_quote_total",
        MULTI_HOP_HOP_MISSING_QUOTE_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "multi_hop_search_worker_queue_depth",
        MULTI_HOP_SEARCH_WORKER_QUEUE_DEPTH.load(Ordering::Relaxed)
    );
    line!(
        "multi_hop_searches_coalesced_total",
        MULTI_HOP_SEARCHES_COALESCED_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "multi_hop_quote_from_cache_total",
        MULTI_HOP_QUOTE_FROM_CACHE_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "multi_hop_quote_from_trade_cache_total",
        MULTI_HOP_QUOTE_FROM_TRADE_CACHE_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "multi_hop_quote_from_pool_quote_total",
        MULTI_HOP_QUOTE_FROM_POOL_QUOTE_TOTAL.load(Ordering::Relaxed)
    );
    out.push_str("multi_hop_cycle_rejected_sanity_total{reason=\"edge_ratio\"} ");
    out.push_str(
        &MULTI_HOP_CYCLE_REJECTED_SANITY_EDGE_RATIO
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("multi_hop_cycle_rejected_sanity_total{reason=\"profit_cap\"} ");
    out.push_str(
        &MULTI_HOP_CYCLE_REJECTED_SANITY_PROFIT_CAP
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    out.push_str("multi_hop_cycle_rejected_sanity_total{reason=\"return_bps_cap\"} ");
    out.push_str(
        &MULTI_HOP_CYCLE_REJECTED_SANITY_RETURN_BPS_CAP
            .load(Ordering::Relaxed)
            .to_string(),
    );
    out.push('\n');
    line!(
        "multi_hop_quote_ready_pools_total",
        MULTI_HOP_QUOTE_READY_POOLS.load(Ordering::Relaxed)
    );
    line!(
        "multi_hop_quote_ready_wsol_edge_pools_total",
        MULTI_HOP_QUOTE_READY_WSOL_EDGE_POOLS.load(Ordering::Relaxed)
    );
    line!(
        "multi_hop_search_no_quote_neighbors_total",
        MULTI_HOP_SEARCH_NO_QUOTE_NEIGHBORS_TOTAL.load(Ordering::Relaxed)
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
    out.push_str(&format!(
        "position_authority_drift_momentum {}\n",
        POSITION_AUTHORITY_DRIFT_MOMENTUM.load(Ordering::Relaxed)
    ));
    line!(
        "concurrent_intents",
        CONCURRENT_INTENTS_GAUGE.load(Ordering::Relaxed)
    );
    line!(
        "execution_intent_rx_queue_depth",
        EXECUTION_INTENT_RX_QUEUE_DEPTH.load(Ordering::Relaxed)
    );
    line!(
        "execution_pool_cache_consumer_pending",
        EXECUTION_POOL_CACHE_CONSUMER_PENDING.load(Ordering::Relaxed)
    );
    line!(
        "execution_wallet_snapshot_consumer_pending",
        EXECUTION_WALLET_SNAPSHOT_CONSUMER_PENDING.load(Ordering::Relaxed)
    );
    line!(
        "execution_pool_cache_messages_processed_total",
        EXECUTION_POOL_CACHE_MESSAGES_PROCESSED_TOTAL.load(Ordering::Relaxed)
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

    #[test]
    #[serial]
    fn execution_intent_delivery_segment_histograms_record_known_delta() {
        reset_execution_intent_delivery_segment_metrics_for_test();
        record_execution_intent_jetstream_to_channel_ms(42);
        record_execution_intent_channel_wait_ms(1_500);
        record_execution_engine_interval_tick_duration_ms(250);

        assert_eq!(
            EXECUTION_INTENT_JETSTREAM_TO_CHANNEL_MS_COUNT.load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            EXECUTION_INTENT_JETSTREAM_TO_CHANNEL_MS_SUM.load(Ordering::Relaxed),
            42
        );
        assert_eq!(
            EXECUTION_INTENT_CHANNEL_WAIT_MS_COUNT.load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            EXECUTION_INTENT_CHANNEL_WAIT_MS_SUM.load(Ordering::Relaxed),
            1_500
        );
        assert_eq!(
            EXECUTION_ENGINE_INTERVAL_TICK_DURATION_MS_COUNT.load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            EXECUTION_ENGINE_INTERVAL_TICK_DURATION_MS_SUM.load(Ordering::Relaxed),
            250
        );
        assert_eq!(
            EXECUTION_INTENT_CHANNEL_WAIT_MS_BUCKET_COUNTS[9].load(Ordering::Relaxed),
            1
        );
    }
}

#[cfg(test)]
mod arb_two_hop_subreason_metrics_tests {
    use super::*;

    #[test]
    fn insufficient_subreason_prometheus_exposes_only_incrementable_labels() {
        let mut out = String::new();
        append_arb_two_hop_insufficient_subreason_total(&mut out);

        for label in [
            "not_known_pool",
            "missing_reserves",
            "missing_trade_price",
            "no_comparable_price",
            "only_one_eligible_pool",
            "only_one_eligible_dex",
        ] {
            assert!(
                out.contains(&format!("reason=\"{label}\"")),
                "missing insufficient label {label}"
            );
        }
        for label in [
            "missing_decimals",
            "stale_price",
            "same_dex_only",
            "implausible_price",
        ] {
            assert!(
                !out.contains(&format!("reason=\"{label}\"")),
                "insufficient metric must not expose {label}"
            );
        }
    }

    #[test]
    fn reject_subreason_prometheus_exposes_generic_reject_labels() {
        let mut out = String::new();
        append_arb_two_hop_reject_subreason_total(&mut out);

        for label in [
            "missing_decimals",
            "stale_price",
            "same_dex_only",
            "implausible_price",
            "not_known_pool",
            "missing_reserves",
        ] {
            assert!(
                out.contains(&format!("reason=\"{label}\"")),
                "missing reject label {label}"
            );
        }
    }
}

#[cfg(test)]
mod arb_two_hop_v2_sell_quote_none_detail_metrics_tests {
    use super::*;

    #[test]
    fn sell_quote_none_detail_inc_and_prometheus_labels() {
        let before = ARB_TWO_HOP_V2_SELL_QUOTE_NONE_DETAIL_STATE_STALE.load(Ordering::Relaxed);
        arb_two_hop_v2_sell_quote_none_detail_inc(ArbTwoHopV2SellQuoteNoneDetail::StateStale);
        let after = ARB_TWO_HOP_V2_SELL_QUOTE_NONE_DETAIL_STATE_STALE.load(Ordering::Relaxed);
        assert_eq!(after, before + 1);

        let mut out = String::new();
        append_arb_two_hop_v2_sell_quote_none_detail_total(&mut out);
        for label in [
            "state_stale",
            "reserves_implausible",
            "dlmm_active_bin_missing",
            "dlmm_walker_zero",
            "dlmm_marginal_reject",
            "cpmm_math_none",
            "unsupported_dex",
            "trade_fallback_none",
            "mint_direction_invalid",
        ] {
            assert!(
                out.contains(&format!("reason=\"{label}\"")),
                "missing sell_quote_none detail label {label}"
            );
        }
    }
}

#[cfg(test)]
mod market_data_account_recv_metrics_tests {
    use super::*;

    #[test]
    fn recv_record_functions_increment_histogram_counts() {
        let classify_before =
            MARKET_DATA_ACCOUNT_RECV_CLASSIFY_DURATION_US_COUNT.load(Ordering::Relaxed);
        record_market_data_account_recv_classify_duration_us(42);
        assert_eq!(
            MARKET_DATA_ACCOUNT_RECV_CLASSIFY_DURATION_US_COUNT.load(Ordering::Relaxed),
            classify_before + 1
        );

        let high_before =
            MARKET_DATA_ACCOUNT_RECV_HIGH_ENQUEUE_DURATION_US_COUNT.load(Ordering::Relaxed);
        record_market_data_account_recv_high_enqueue_duration_us(100);
        assert_eq!(
            MARKET_DATA_ACCOUNT_RECV_HIGH_ENQUEUE_DURATION_US_COUNT.load(Ordering::Relaxed),
            high_before + 1
        );

        let enrich_before =
            MARKET_DATA_ACCOUNT_RECV_ENRICH_INGRESS_DURATION_US_COUNT.load(Ordering::Relaxed);
        record_market_data_account_recv_enrich_ingress_duration_us(5);
        assert_eq!(
            MARKET_DATA_ACCOUNT_RECV_ENRICH_INGRESS_DURATION_US_COUNT.load(Ordering::Relaxed),
            enrich_before + 1
        );

        let iter_before =
            MARKET_DATA_ACCOUNT_RECV_ITERATION_DURATION_US_COUNT.load(Ordering::Relaxed);
        record_market_data_account_recv_iteration_duration_us(250);
        assert_eq!(
            MARKET_DATA_ACCOUNT_RECV_ITERATION_DURATION_US_COUNT.load(Ordering::Relaxed),
            iter_before + 1
        );

        let recv_iters_before = MARKET_DATA_ACCOUNT_RECV_ITERATIONS_TOTAL.load(Ordering::Relaxed);
        inc_market_data_account_recv_iterations_total();
        assert_eq!(
            MARKET_DATA_ACCOUNT_RECV_ITERATIONS_TOTAL.load(Ordering::Relaxed),
            recv_iters_before + 1
        );
    }
}
