use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};
use once_cell::sync::Lazy;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

// Global counters (simple, lock-free). For production consider Prometheus exporter.
pub static QUOTE_REQUESTS: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static QUOTE_SUCCESSES: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ROUTER_SINGLE_HOP: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ROUTER_HOPS2: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ROUTER_HOPS3: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TRIANGLE_ATTEMPTS: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TRIANGLE_PROFITABLE: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TRIANGLE_OPPORTUNITIES: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
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
pub static WS_RECONNECTS_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static RPC_RETRY_ATTEMPTS_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
// WebSocket stability metrics
pub static WS_MESSAGES_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static WS_HEARTBEAT_MISSES_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static WS_ACTIVE_CONNECTIONS: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
// Protocol / LP fee (aggregated lamports-equivalent or raw tokens? We aggregate lamports-equivalent for SOL side, plus raw token fee counts)
pub static PROTOCOL_FEE_TOKENS_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static PROTOCOL_FEE_SOL_MICRO_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static PENDING_RECONCILIATIONS_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static PENDING_FAILED_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
// Partial exit metrics
pub static PARTIAL_EXIT_EVENTS_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static PARTIAL_EXIT_FRACTION_MICRO_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
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
pub static LAST_ACTIVITY_TS: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
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

async fn metrics_response() -> Response<Body> {
    // Build Prometheus exposition text
    let mut out = String::with_capacity(4096);
    macro_rules! line {
        ($name:expr, $val:expr) => {
            out.push_str($name);
            out.push(' ');
            out.push_str(&$val.to_string());
            out.push('\n');
        };
    }
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
        "ws_reconnects_total",
        WS_RECONNECTS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "rpc_retry_attempts_total",
        RPC_RETRY_ATTEMPTS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "ws_messages_total",
        WS_MESSAGES_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "ws_heartbeat_misses_total",
        WS_HEARTBEAT_MISSES_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "ws_active_connections",
        WS_ACTIVE_CONNECTIONS.load(Ordering::Relaxed)
    );
    line!(
        "protocol_fee_tokens_total",
        PROTOCOL_FEE_TOKENS_TOTAL.load(Ordering::Relaxed)
    );
    line!(
        "protocol_fee_sol_total",
        PROTOCOL_FEE_SOL_MICRO_TOTAL.load(Ordering::Relaxed) as f64 / 1_000_000.0
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

pub async fn serve_metrics(addr: SocketAddr) -> anyhow::Result<()> {
    let make_svc = make_service_fn(|_conn| async {
        Ok::<_, hyper::Error>(service_fn(|req: Request<Body>| async move {
            let path = req.uri().path();
            if path == "/metrics" {
                return Ok::<_, hyper::Error>(metrics_response().await);
            }
            if path == "/live" {
                return Ok::<_, hyper::Error>(Response::new(Body::from("ok")));
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
        }))
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
