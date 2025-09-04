use std::sync::atomic::{AtomicU64, AtomicI64, Ordering};
use once_cell::sync::Lazy;
use std::time::{Instant};
use std::net::SocketAddr;
use hyper::{Body, Request, Response, Server};
use hyper::service::{make_service_fn, service_fn};

// Global counters (simple, lock-free). For production consider Prometheus exporter.
pub static QUOTE_REQUESTS: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static QUOTE_SUCCESSES: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ROUTER_SINGLE_HOP: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ROUTER_HOPS2: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ROUTER_HOPS3: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TRIANGLE_ATTEMPTS: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static ARB_TRIANGLE_PROFITABLE: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
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

// --- New Prometheus style metrics (basic) ---
pub static TRADES_EXECUTED_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static TRADES_FAILED_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static RPC_ERRORS_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static OPEN_POSITIONS_GAUGE: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static DAILY_REALIZED_PNL_SOL_MICRO: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static LIQUIDITY_ESTIMATE_SOL_MICRO: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
// Histogram (swap latency) simplified: we keep bucket counters manually (ns)
const SWAP_LATENCY_BUCKETS: &[u64] = &[1_000_000, 2_000_000, 5_000_000, 10_000_000, 25_000_000, 50_000_000, 100_000_000, 250_000_000, 500_000_000, 1_000_000_000];
pub static SWAP_LATENCY_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> = Lazy::new(|| SWAP_LATENCY_BUCKETS.iter().map(|_| AtomicU64::new(0)).collect());
pub static SWAP_LATENCY_SUM_NS: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static SWAP_LATENCY_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
// Quote latency histogram
const QUOTE_LATENCY_BUCKETS: &[u64] = &[200_000, 500_000, 1_000_000, 2_000_000, 5_000_000, 10_000_000, 25_000_000, 50_000_000, 100_000_000];
pub static QUOTE_LATENCY_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> = Lazy::new(|| QUOTE_LATENCY_BUCKETS.iter().map(|_| AtomicU64::new(0)).collect());
pub static QUOTE_LATENCY_SUM_NS: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static QUOTE_LATENCY_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
// Shortfall / Slippage aggregation
pub static SHORTFALL_TOKENS_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static SHORTFALL_SOL_MICRO_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static FILLS_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
// Network fees aggregation
pub static NETWORK_FEES_LAMPORTS_TOTAL: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
// Realized trade return histogram (ratio realized_pnl / invested) buckets (cumulative style capture)
// Buckets chosen to capture deep losses to outsized wins.
const TRADE_RETURN_BUCKETS: &[f64] = &[
    -0.9, -0.5, -0.25, -0.1, -0.05, -0.02, -0.01, 0.0,
     0.01, 0.02, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0
];
pub static TRADE_RETURN_BUCKET_COUNTS: Lazy<Vec<AtomicU64>> = Lazy::new(|| TRADE_RETURN_BUCKETS.iter().map(|_| AtomicU64::new(0)).collect());
pub static TRADE_RETURN_COUNT: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
pub static TRADE_RETURN_SUM_MICRO: Lazy<AtomicI64> = Lazy::new(|| AtomicI64::new(0)); // signed sum(ret * 1e6) for average

pub struct LatencyTimer { start: Instant }
impl LatencyTimer { pub fn start() -> Self { Self { start: Instant::now() } } }
impl Drop for LatencyTimer { fn drop(&mut self) { let ns = self.start.elapsed().as_nanos() as u64; QUOTE_LATENCY_TOTAL_NS.fetch_add(ns, Ordering::Relaxed); } }

pub fn record_quote_latency(ns: u64) {
    QUOTE_LATENCY_SUM_NS.fetch_add(ns, Ordering::Relaxed);
    QUOTE_LATENCY_COUNT.fetch_add(1, Ordering::Relaxed);
    for (i,b) in QUOTE_LATENCY_BUCKETS.iter().enumerate() { if ns <= *b { QUOTE_LATENCY_BUCKET_COUNTS[i].fetch_add(1, Ordering::Relaxed); break; } }
}

pub fn record_shortfall(tokens: u64, sol_ui: f64) {
    SHORTFALL_TOKENS_TOTAL.fetch_add(tokens, Ordering::Relaxed);
    SHORTFALL_SOL_MICRO_TOTAL.fetch_add((sol_ui * 1_000_000.0) as u64, Ordering::Relaxed);
    FILLS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

pub fn record_network_fee(lamports: u64) { NETWORK_FEES_LAMPORTS_TOTAL.fetch_add(lamports, Ordering::Relaxed); }

pub fn record_trade_return(ret: f64) {
    // Clamp extreme outliers to last bucket range for stability
    let mut placed = false;
    for (i,b) in TRADE_RETURN_BUCKETS.iter().enumerate() {
        if ret <= *b { TRADE_RETURN_BUCKET_COUNTS[i].fetch_add(1, Ordering::Relaxed); placed = true; break; }
    }
    if !placed {
        // Overflow (> last bucket) counts only in an implicit +Inf bucket via count (Prometheus expectation). We don't keep explicit here.
    }
    TRADE_RETURN_COUNT.fetch_add(1, Ordering::Relaxed);
    let micro = (ret * 1_000_000.0).round();
    // Bound to i64 range
    let micro_i64 = if micro > i64::MAX as f64 { i64::MAX } else if micro < i64::MIN as f64 { i64::MIN } else { micro as i64 };
    TRADE_RETURN_SUM_MICRO.fetch_add(micro_i64, Ordering::Relaxed);
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
    cycle_partial_examined: CYCLE_PARTIAL_EXAMINED.load(Ordering::Relaxed),
    cycle_pruned_dominance: CYCLE_PRUNED_DOMINANCE.load(Ordering::Relaxed),
    cycle_pruned_bound: CYCLE_PRUNED_BOUND.load(Ordering::Relaxed),
    cycle_completed: CYCLE_COMPLETED.load(Ordering::Relaxed),
    raydium_pools_loaded: RAYDIUM_POOLS_LOADED.load(Ordering::Relaxed),
    raydium_pools_skipped_serum: RAYDIUM_POOLS_SKIPPED_SERUM.load(Ordering::Relaxed),
    raydium_pools_skipped_invalid: RAYDIUM_POOLS_SKIPPED_INVALID.load(Ordering::Relaxed),
        avg_quote_latency_ms: {
            let reqs = QUOTE_REQUESTS.load(Ordering::Relaxed).max(1);
            (QUOTE_LATENCY_TOTAL_NS.load(Ordering::Relaxed) / reqs) as f64 / 1_000_000.0
        }
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
    pub cycle_partial_examined: u64,
    pub cycle_pruned_dominance: u64,
    pub cycle_pruned_bound: u64,
    pub cycle_completed: u64,
    pub avg_quote_latency_ms: f64,
    pub raydium_pools_loaded: u64,
    pub raydium_pools_skipped_serum: u64,
    pub raydium_pools_skipped_invalid: u64,
}

/// Record one swap latency measurement (nanoseconds)
pub fn record_swap_latency(ns: u64) {
    SWAP_LATENCY_SUM_NS.fetch_add(ns, Ordering::Relaxed);
    SWAP_LATENCY_COUNT.fetch_add(1, Ordering::Relaxed);
    for (i, bucket) in SWAP_LATENCY_BUCKETS.iter().enumerate() {
        if ns <= *bucket { SWAP_LATENCY_BUCKET_COUNTS[i].fetch_add(1, Ordering::Relaxed); break; }
    }
}

async fn metrics_response() -> Response<Body> {
    // Build Prometheus exposition text
    let mut out = String::with_capacity(4096);
    macro_rules! line { ($name:expr, $val:expr) => { out.push_str($name); out.push(' '); out.push_str(&$val.to_string()); out.push('\n'); }; }
    line!("quote_requests_total", QUOTE_REQUESTS.load(Ordering::Relaxed));
    line!("quote_successes_total", QUOTE_SUCCESSES.load(Ordering::Relaxed));
    line!("router_single_hop_total", ROUTER_SINGLE_HOP.load(Ordering::Relaxed));
    line!("router_hops2_total", ROUTER_HOPS2.load(Ordering::Relaxed));
    line!("router_hops3_total", ROUTER_HOPS3.load(Ordering::Relaxed));
    line!("arb_triangle_attempts_total", ARB_TRIANGLE_ATTEMPTS.load(Ordering::Relaxed));
    line!("arb_triangle_profitable_total", ARB_TRIANGLE_PROFITABLE.load(Ordering::Relaxed));
    line!("cycle_partial_examined_total", CYCLE_PARTIAL_EXAMINED.load(Ordering::Relaxed));
    line!("cycle_pruned_dominance_total", CYCLE_PRUNED_DOMINANCE.load(Ordering::Relaxed));
    line!("cycle_pruned_bound_total", CYCLE_PRUNED_BOUND.load(Ordering::Relaxed));
    line!("cycle_completed_total", CYCLE_COMPLETED.load(Ordering::Relaxed));
    line!("raydium_pools_loaded_total", RAYDIUM_POOLS_LOADED.load(Ordering::Relaxed));
    line!("raydium_pools_skipped_serum_total", RAYDIUM_POOLS_SKIPPED_SERUM.load(Ordering::Relaxed));
    line!("raydium_pools_skipped_invalid_total", RAYDIUM_POOLS_SKIPPED_INVALID.load(Ordering::Relaxed));
    line!("trades_executed_total", TRADES_EXECUTED_TOTAL.load(Ordering::Relaxed));
    line!("trades_failed_total", TRADES_FAILED_TOTAL.load(Ordering::Relaxed));
    line!("rpc_errors_total", RPC_ERRORS_TOTAL.load(Ordering::Relaxed));
    line!("open_positions", OPEN_POSITIONS_GAUGE.load(Ordering::Relaxed));
    line!("daily_realized_pnl_sol", DAILY_REALIZED_PNL_SOL_MICRO.load(Ordering::Relaxed) as f64 / 1_000_000.0);
    line!("liquidity_estimate_sol", LIQUIDITY_ESTIMATE_SOL_MICRO.load(Ordering::Relaxed) as f64 / 1_000_000.0);
    // Quote latency histogram exposition
    let q_count = QUOTE_LATENCY_COUNT.load(Ordering::Relaxed);
    let q_sum = QUOTE_LATENCY_SUM_NS.load(Ordering::Relaxed);
    for (i,b) in QUOTE_LATENCY_BUCKETS.iter().enumerate() { let cum = QUOTE_LATENCY_BUCKET_COUNTS[i].load(Ordering::Relaxed); out.push_str(&format!("quote_latency_seconds_bucket{{le=\"{}\"}} {}\n", (*b as f64)/1e9, cum)); }
    out.push_str(&format!("quote_latency_seconds_sum {}\n", (q_sum as f64)/1e9));
    out.push_str(&format!("quote_latency_seconds_count {}\n", q_count));
    // Shortfall & fees aggregates
    line!("shortfall_tokens_total", SHORTFALL_TOKENS_TOTAL.load(Ordering::Relaxed));
    line!("shortfall_sol_total", SHORTFALL_SOL_MICRO_TOTAL.load(Ordering::Relaxed) as f64 / 1_000_000.0);
    line!("fills_total", FILLS_TOTAL.load(Ordering::Relaxed));
    line!("network_fees_lamports_total", NETWORK_FEES_LAMPORTS_TOTAL.load(Ordering::Relaxed));
    // Trade return histogram (realized PnL / invested)
    let tr_count = TRADE_RETURN_COUNT.load(Ordering::Relaxed);
    let tr_sum_micro = TRADE_RETURN_SUM_MICRO.load(Ordering::Relaxed);
    for (i,b) in TRADE_RETURN_BUCKETS.iter().enumerate() { let c = TRADE_RETURN_BUCKET_COUNTS[i].load(Ordering::Relaxed); out.push_str(&format!("trade_return_bucket{{le=\"{}\"}} {}\n", b, c)); }
    out.push_str(&format!("trade_return_sum {}\n", tr_sum_micro as f64 / 1_000_000.0));
    out.push_str(&format!("trade_return_count {}\n", tr_count));
    // Histogram exposition (Prometheus classic format)
    let swap_count = SWAP_LATENCY_COUNT.load(Ordering::Relaxed);
    let swap_sum = SWAP_LATENCY_SUM_NS.load(Ordering::Relaxed);
    for (i, bucket) in SWAP_LATENCY_BUCKETS.iter().enumerate() {
        let cum = SWAP_LATENCY_BUCKET_COUNTS[i].load(Ordering::Relaxed);
        out.push_str(&format!("swap_latency_seconds_bucket{{le=\"{}\"}} {}\n", (*bucket as f64)/1e9, cum));
    }
    out.push_str(&format!("swap_latency_seconds_sum {}\n", (swap_sum as f64)/1e9));
    out.push_str(&format!("swap_latency_seconds_count {}\n", swap_count));
    Response::builder().status(200).header("Content-Type","text/plain; version=0.0.4").body(Body::from(out)).unwrap()
}

pub async fn serve_metrics(addr: SocketAddr) -> anyhow::Result<()> {
    let make_svc = make_service_fn(|_conn| async {
        Ok::<_, hyper::Error>(service_fn(|_req: Request<Body>| async move { Ok::<_, hyper::Error>(metrics_response().await) }))
    });
    Server::bind(&addr).serve(make_svc).await?;
    Ok(())
}