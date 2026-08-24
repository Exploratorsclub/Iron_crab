//! Bounded, deterministic end-to-end observability for arb-pinned pools.
//!
//! The cohort is selected from the pool address alone, so arb-strategy and market-data observe
//! the same pools without another topic or RPC call. Exact addresses are intentionally excluded
//! from Prometheus; callers may include them in throttled structured logs.

use once_cell::sync::Lazy;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

pub const ARB_PIN_QUALITY_COHORT_DENOMINATOR: u64 = 16;
const ARB_PIN_QUALITY_MAX_POOLS_PER_PROCESS: usize = 4_096;

const STAGES: [&str; 7] = [
    "pin_published",
    "pin_received",
    "subscription",
    "master_update",
    "slave_update",
    "tracker_seeded",
    "quote_ready",
];
const OUTCOMES: [&str; 8] = [
    "observed",
    "complete",
    "missing_cache",
    "missing_vault",
    "missing_bins",
    "identity_mismatch",
    "queue_drop",
    "other",
];
const SLOT_OUTCOMES: [&str; 3] = ["forward", "duplicate", "regression"];
const DEXES: [&str; 6] = [
    "orca",
    "meteora_dlmm",
    "meteora_cpmm",
    "pump_amm",
    "raydium",
    "other",
];
const COMPLETENESS_OUTCOMES: [&str; 6] = [
    "complete",
    "missing_cache",
    "missing_vault",
    "missing_bins",
    "identity_mismatch",
    "other",
];
const LATENCY_BUCKETS_MS: [u64; 10] = [10, 25, 50, 100, 250, 500, 1_000, 5_000, 30_000, 300_000];

static STAGE_POOL_COUNTS: Lazy<Vec<AtomicU64>> = Lazy::new(|| {
    (0..STAGES.len() * OUTCOMES.len())
        .map(|_| AtomicU64::new(0))
        .collect()
});
static STAGE_LATENCY_BUCKETS: Lazy<Vec<AtomicU64>> = Lazy::new(|| {
    (0..STAGES.len() * LATENCY_BUCKETS_MS.len())
        .map(|_| AtomicU64::new(0))
        .collect()
});
static STAGE_LATENCY_SUM: Lazy<Vec<AtomicU64>> =
    Lazy::new(|| (0..STAGES.len()).map(|_| AtomicU64::new(0)).collect());
static STAGE_LATENCY_COUNT: Lazy<Vec<AtomicU64>> =
    Lazy::new(|| (0..STAGES.len()).map(|_| AtomicU64::new(0)).collect());
static SLOT_UPDATE_COUNTS: Lazy<Vec<AtomicU64>> = Lazy::new(|| {
    (0..SLOT_OUTCOMES.len())
        .map(|_| AtomicU64::new(0))
        .collect()
});
static COMPLETENESS_POOL_COUNTS: Lazy<Vec<AtomicU64>> = Lazy::new(|| {
    (0..DEXES.len() * COMPLETENESS_OUTCOMES.len())
        .map(|_| AtomicU64::new(0))
        .collect()
});

#[derive(Default)]
struct CohortState {
    first_seen: HashMap<u64, u128>,
    pin_ts_ms: HashMap<u64, u64>,
    last_slot: HashMap<u64, u64>,
    completeness_seen: HashMap<u64, u64>,
}

static COHORT_STATE: Lazy<Mutex<CohortState>> = Lazy::new(|| Mutex::new(CohortState::default()));

fn admit_key(state: &mut CohortState, key: u64, pin_ts_ms: u64) -> bool {
    if state.pin_ts_ms.contains_key(&key) {
        return true;
    }
    if state.pin_ts_ms.len() >= ARB_PIN_QUALITY_MAX_POOLS_PER_PROCESS {
        return false;
    }
    state.pin_ts_ms.insert(key, pin_ts_ms);
    true
}

fn stable_hash(value: &str) -> u64 {
    // FNV-1a: stable across processes and Rust versions, unlike DefaultHasher.
    let mut hash = 0xcbf29ce484222325u64;
    for byte in value.trim().as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

pub fn arb_pin_quality_cohort_member(pool: &str) -> bool {
    stable_hash(pool) % ARB_PIN_QUALITY_COHORT_DENOMINATOR == 0
}

fn label_index(labels: &[&str], value: &str) -> usize {
    labels
        .iter()
        .position(|candidate| *candidate == value)
        .unwrap_or(labels.len() - 1)
}

fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

/// Register the start of a cohort trace. Repeated reconcile snapshots preserve the first pin time.
pub fn record_arb_pin_quality_anchor(pool: &str, pin_ts_ms: u64) {
    if !arb_pin_quality_cohort_member(pool) {
        return;
    }
    let key = stable_hash(pool);
    if !admit_key(&mut COHORT_STATE.lock(), key, pin_ts_ms) {
        return;
    }
}

/// Register a published pin and its latency anchor in the publishing process.
pub fn record_arb_pin_quality_pin(pool: &str, pin_ts_ms: u64) {
    record_arb_pin_quality_anchor(pool, pin_ts_ms);
    record_arb_pin_quality_stage(pool, "pin_published", "observed", Some(pin_ts_ms));
}

/// Record a stage/outcome once per cohort pool and process. Optional source time feeds latency.
pub fn record_arb_pin_quality_stage(
    pool: &str,
    stage: &str,
    outcome: &str,
    source_ts_ms: Option<u64>,
) {
    if !arb_pin_quality_cohort_member(pool) {
        return;
    }
    let stage_idx = label_index(&STAGES, stage);
    let outcome_idx = label_index(&OUTCOMES, outcome);
    let bit_idx = stage_idx * OUTCOMES.len() + outcome_idx;
    let key = stable_hash(pool);
    let (first, latency_ms) = {
        let mut state = COHORT_STATE.lock();
        if !admit_key(&mut state, key, source_ts_ms.unwrap_or_else(now_unix_ms)) {
            return;
        }
        let bits = state.first_seen.entry(key).or_insert(0);
        let mask = 1u128 << bit_idx;
        let first = *bits & mask == 0;
        if first {
            *bits |= mask;
        }
        let latency = state
            .pin_ts_ms
            .get(&key)
            .map(|ts| now_unix_ms().saturating_sub(*ts));
        (first, latency)
    };
    if !first {
        return;
    }
    STAGE_POOL_COUNTS[bit_idx].fetch_add(1, Ordering::Relaxed);
    if let Some(latency_ms) = latency_ms {
        STAGE_LATENCY_SUM[stage_idx].fetch_add(latency_ms, Ordering::Relaxed);
        STAGE_LATENCY_COUNT[stage_idx].fetch_add(1, Ordering::Relaxed);
        for (bucket_idx, bound) in LATENCY_BUCKETS_MS.iter().enumerate() {
            if latency_ms <= *bound {
                STAGE_LATENCY_BUCKETS[stage_idx * LATENCY_BUCKETS_MS.len() + bucket_idx]
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

/// Record slot monotonicity for every cohort PoolCacheUpdate.
pub fn record_arb_pin_quality_slot(pool: &str, slot: u64) -> &'static str {
    if !arb_pin_quality_cohort_member(pool) || slot == 0 {
        return "ignored";
    }
    let key = stable_hash(pool);
    let outcome = {
        let mut state = COHORT_STATE.lock();
        if !admit_key(&mut state, key, now_unix_ms()) {
            return "ignored";
        }
        match state.last_slot.insert(key, slot) {
            None => "forward",
            Some(previous) if slot > previous => "forward",
            Some(previous) if slot == previous => "duplicate",
            Some(_) => "regression",
        }
    };
    let idx = label_index(&SLOT_OUTCOMES, outcome);
    SLOT_UPDATE_COUNTS[idx].fetch_add(1, Ordering::Relaxed);
    outcome
}

/// Record one DEX-specific completeness classification per pool and outcome.
pub fn record_arb_pin_quality_completeness(pool: &str, dex: &str, outcome: &str) {
    if !arb_pin_quality_cohort_member(pool) {
        return;
    }
    let dex_idx = label_index(&DEXES, dex);
    let outcome_idx = label_index(&COMPLETENESS_OUTCOMES, outcome);
    let bit_idx = dex_idx * COMPLETENESS_OUTCOMES.len() + outcome_idx;
    let key = stable_hash(pool);
    let first = {
        let mut state = COHORT_STATE.lock();
        if !admit_key(&mut state, key, now_unix_ms()) {
            return;
        }
        let bits = state.completeness_seen.entry(key).or_insert(0);
        let mask = 1u64 << bit_idx;
        let first = *bits & mask == 0;
        if first {
            *bits |= mask;
        }
        first
    };
    if first {
        COMPLETENESS_POOL_COUNTS[bit_idx].fetch_add(1, Ordering::Relaxed);
    }
}

pub fn append_arb_pin_quality_metrics(out: &mut String) {
    out.push_str(&format!(
        "arb_pin_quality_cohort_info{{denominator=\"{}\"}} 1\n",
        ARB_PIN_QUALITY_COHORT_DENOMINATOR
    ));
    for (stage_idx, stage) in STAGES.iter().enumerate() {
        for (outcome_idx, outcome) in OUTCOMES.iter().enumerate() {
            let idx = stage_idx * OUTCOMES.len() + outcome_idx;
            out.push_str(&format!(
                "arb_pin_quality_cohort_stage_pools{{stage=\"{stage}\",outcome=\"{outcome}\"}} {}\n",
                STAGE_POOL_COUNTS[idx].load(Ordering::Relaxed)
            ));
        }
        let count = STAGE_LATENCY_COUNT[stage_idx].load(Ordering::Relaxed);
        for (bucket_idx, bound) in LATENCY_BUCKETS_MS.iter().enumerate() {
            let value = STAGE_LATENCY_BUCKETS[stage_idx * LATENCY_BUCKETS_MS.len() + bucket_idx]
                .load(Ordering::Relaxed);
            out.push_str(&format!(
                "arb_pin_quality_stage_latency_ms_bucket{{stage=\"{stage}\",le=\"{bound}\"}} {value}\n"
            ));
        }
        out.push_str(&format!(
            "arb_pin_quality_stage_latency_ms_bucket{{stage=\"{stage}\",le=\"+Inf\"}} {count}\n"
        ));
        out.push_str(&format!(
            "arb_pin_quality_stage_latency_ms_sum{{stage=\"{stage}\"}} {}\n",
            STAGE_LATENCY_SUM[stage_idx].load(Ordering::Relaxed)
        ));
        out.push_str(&format!(
            "arb_pin_quality_stage_latency_ms_count{{stage=\"{stage}\"}} {count}\n"
        ));
    }
    for (idx, outcome) in SLOT_OUTCOMES.iter().enumerate() {
        out.push_str(&format!(
            "arb_pin_quality_slot_updates_total{{outcome=\"{outcome}\"}} {}\n",
            SLOT_UPDATE_COUNTS[idx].load(Ordering::Relaxed)
        ));
    }
    for (dex_idx, dex) in DEXES.iter().enumerate() {
        for (outcome_idx, outcome) in COMPLETENESS_OUTCOMES.iter().enumerate() {
            let idx = dex_idx * COMPLETENESS_OUTCOMES.len() + outcome_idx;
            out.push_str(&format!(
                "arb_pin_quality_completeness_pools{{dex=\"{dex}\",outcome=\"{outcome}\"}} {}\n",
                COMPLETENESS_POOL_COUNTS[idx].load(Ordering::Relaxed)
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cohort_selection_is_stable_and_bounded() {
        let selected = (0..1_600)
            .filter(|i| arb_pin_quality_cohort_member(&format!("pool-{i}")))
            .count();
        assert!((60..140).contains(&selected), "selected={selected}");
        assert_eq!(
            arb_pin_quality_cohort_member("same-pool"),
            arb_pin_quality_cohort_member("same-pool")
        );
    }

    #[test]
    fn stage_is_counted_once_and_slot_regression_is_visible() {
        let pool = (0..10_000)
            .map(|i| format!("quality-test-pool-{i}"))
            .find(|pool| arb_pin_quality_cohort_member(pool))
            .expect("cohort member");
        record_arb_pin_quality_pin(&pool, now_unix_ms());
        record_arb_pin_quality_stage(&pool, "slave_update", "complete", None);
        record_arb_pin_quality_stage(&pool, "slave_update", "complete", None);
        assert_eq!(record_arb_pin_quality_slot(&pool, 20), "forward");
        assert_eq!(record_arb_pin_quality_slot(&pool, 20), "duplicate");
        assert_eq!(record_arb_pin_quality_slot(&pool, 19), "regression");
        let mut rendered = String::new();
        append_arb_pin_quality_metrics(&mut rendered);
        assert!(rendered.contains("arb_pin_quality_cohort_stage_pools"));
        assert!(rendered.contains("outcome=\"regression\""));
    }
}
