use std::sync::atomic::{AtomicU64, Ordering};
use once_cell::sync::Lazy;
use std::time::{Instant};

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

pub struct LatencyTimer { start: Instant }
impl LatencyTimer { pub fn start() -> Self { Self { start: Instant::now() } } }
impl Drop for LatencyTimer { fn drop(&mut self) { let ns = self.start.elapsed().as_nanos() as u64; QUOTE_LATENCY_TOTAL_NS.fetch_add(ns, Ordering::Relaxed); } }

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
}