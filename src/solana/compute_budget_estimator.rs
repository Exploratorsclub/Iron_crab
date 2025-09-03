//! Heuristic Compute Budget & Priority Fee Estimator
//!
//! Provides a lightweight initial approach for sizing compute unit limits and
//! priority fees before full simulation-driven or historical adaptive tuning is
//! implemented. The aim is to prevent unnecessary over-provisioning while
//! avoiding under-estimation that would cause transaction failure.
use solana_sdk::instruction::Instruction;

#[derive(Debug, Clone, Copy)]
pub struct ComputeEstimate {
    pub compute_unit_limit: u32,
    pub compute_unit_price_micro_lamports: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct EstimatorConfig {
    pub base_single_swap_cu: u32,
    pub per_extra_ix_cu: u32,
    pub per_hop_increment_cu: u32,
    pub min_limit: u32,
    pub max_limit: u32,
    pub default_cu_price_micro_lamports: u64,
    pub large_notional_threshold: u64,
    pub large_notional_multiplier: u64,
}

impl Default for EstimatorConfig {
    fn default() -> Self {
        Self {
            base_single_swap_cu: 120_000,
            per_extra_ix_cu: 10_000,
            per_hop_increment_cu: 40_000,
            min_limit: 80_000,
            max_limit: 400_000,
            default_cu_price_micro_lamports: 1,
            large_notional_threshold: 1_000_000_000, // example threshold (raw units)
            large_notional_multiplier: 3,
        }
    }
}

pub fn estimate_from_instructions(ixs: &[Instruction], hops: usize, notional_in: u64, cfg: EstimatorConfig) -> ComputeEstimate {
    let mut limit = cfg.base_single_swap_cu;
    if hops > 1 { limit += cfg.per_hop_increment_cu * (hops as u32 - 1); }
    let extra_ix = ixs.len().saturating_sub(1) as u32; // beyond first
    limit = limit.saturating_add(extra_ix * cfg.per_extra_ix_cu);
    if limit < cfg.min_limit { limit = cfg.min_limit; }
    if limit > cfg.max_limit { limit = cfg.max_limit; }
    let mut price = cfg.default_cu_price_micro_lamports;
    if notional_in >= cfg.large_notional_threshold { price = price.saturating_mul(cfg.large_notional_multiplier); }
    ComputeEstimate { compute_unit_limit: limit, compute_unit_price_micro_lamports: price }
}

pub fn estimate_single_swap(notional_in: u64) -> ComputeEstimate {
    estimate_from_instructions(&[], 1, notional_in, EstimatorConfig::default())
}
