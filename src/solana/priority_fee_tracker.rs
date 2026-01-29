//! Dynamic Priority Fee Tracker
//!
//! Tracks recent transaction fees from Geyser and calculates percentiles
//! for dynamic fee estimation. NO RPC calls - purely Geyser-driven.
//!
//! # Architecture
//! - Receives fee samples from GeyserTransactionUpdate
//! - Maintains rolling window of recent samples (default: 50)
//! - Calculates P25, P50, P75, P90 percentiles
//! - Provides tier-based fee recommendations
//!
//! # Fee Calculation
//! Priority fee per CU = (total_fee - base_fee) / compute_units_consumed
//! Base fee ≈ 5000 lamports (signature verification)

use std::collections::VecDeque;
use std::sync::Arc;
use parking_lot::RwLock;
use tracing::{debug, info};

use crate::ipc::IntentTier;

/// Sample of a transaction's priority fee
#[derive(Debug, Clone, Copy)]
pub struct FeeSample {
    /// Slot when transaction was processed
    pub slot: u64,
    /// Total fee in lamports
    pub fee_lamports: u64,
    /// Compute units consumed
    pub compute_units: u64,
    /// Calculated priority fee in micro-lamports per CU
    pub priority_fee_micro_lamports: u64,
}

/// Fee percentiles from recent samples
#[derive(Debug, Clone, Copy, Default)]
pub struct FeePercentiles {
    pub p25: u64,
    pub p50: u64,
    pub p75: u64,
    pub p90: u64,
    pub sample_count: usize,
    pub last_slot: u64,
}

/// Configuration for the priority fee tracker
#[derive(Debug, Clone)]
pub struct PriorityFeeConfig {
    /// Number of samples to keep in rolling window
    pub window_size: usize,
    /// Minimum priority fee (floor)
    pub min_priority_fee_micro_lamports: u64,
    /// Maximum priority fee (ceiling)
    pub max_priority_fee_micro_lamports: u64,
    /// Tier0 multiplier (applied to P90)
    pub tier0_multiplier: f64,
    /// Tier1 multiplier (applied to P50)
    pub tier1_multiplier: f64,
    /// Arb multiplier (applied to P75)
    pub arb_multiplier: f64,
    /// Base fee in lamports (signature verification ~5000)
    pub base_fee_lamports: u64,
}

impl Default for PriorityFeeConfig {
    fn default() -> Self {
        Self {
            window_size: 50,
            min_priority_fee_micro_lamports: 10_000,      // 0.01 lamports/CU
            max_priority_fee_micro_lamports: 2_000_000,   // 2 lamports/CU
            tier0_multiplier: 1.5,
            tier1_multiplier: 1.2,
            arb_multiplier: 1.3,
            base_fee_lamports: 5_000,
        }
    }
}

/// Thread-safe priority fee tracker
pub struct PriorityFeeTracker {
    config: PriorityFeeConfig,
    samples: Arc<RwLock<VecDeque<FeeSample>>>,
    cached_percentiles: Arc<RwLock<FeePercentiles>>,
}

impl PriorityFeeTracker {
    /// Create a new tracker with default config
    pub fn new() -> Self {
        Self::with_config(PriorityFeeConfig::default())
    }

    /// Create a new tracker with custom config
    pub fn with_config(config: PriorityFeeConfig) -> Self {
        Self {
            samples: Arc::new(RwLock::new(VecDeque::with_capacity(config.window_size + 10))),
            cached_percentiles: Arc::new(RwLock::new(FeePercentiles::default())),
            config,
        }
    }

    /// Add a fee sample from a Geyser transaction update
    ///
    /// Returns the calculated priority fee in micro-lamports per CU, or None if invalid
    pub fn add_sample(&self, slot: u64, fee_lamports: u64, compute_units: Option<u64>) -> Option<u64> {
        // Need compute units to calculate priority fee per CU
        let compute_units = compute_units?;
        
        // Skip transactions with zero or very low compute (likely failed or trivial)
        if compute_units < 1000 {
            return None;
        }

        // Calculate priority fee:
        // priority_fee = (total_fee - base_fee) / compute_units * 1_000_000 (to get micro-lamports)
        let base_fee = self.config.base_fee_lamports;
        let priority_portion = fee_lamports.saturating_sub(base_fee);
        
        // Convert to micro-lamports per CU: (priority_portion * 1_000_000) / compute_units
        let priority_fee_micro = priority_portion
            .saturating_mul(1_000_000)
            .checked_div(compute_units)
            .unwrap_or(0);

        // Skip zero or negative priority fees
        if priority_fee_micro == 0 {
            return None;
        }

        let sample = FeeSample {
            slot,
            fee_lamports,
            compute_units,
            priority_fee_micro_lamports: priority_fee_micro,
        };

        // Add to samples
        {
            let mut samples = self.samples.write();
            samples.push_back(sample);

            // Trim to window size
            while samples.len() > self.config.window_size {
                samples.pop_front();
            }
        }

        // Recalculate percentiles periodically (every 10 samples to reduce compute)
        let sample_count = self.samples.read().len();
        if sample_count % 10 == 0 || sample_count <= 10 {
            self.recalculate_percentiles();
        }

        Some(priority_fee_micro)
    }

    /// Recalculate percentiles from current samples
    fn recalculate_percentiles(&self) {
        let samples = self.samples.read();
        
        if samples.is_empty() {
            return;
        }

        // Collect and sort priority fees
        let mut fees: Vec<u64> = samples.iter().map(|s| s.priority_fee_micro_lamports).collect();
        fees.sort_unstable();

        let len = fees.len();
        let last_slot = samples.back().map(|s| s.slot).unwrap_or(0);

        let percentiles = FeePercentiles {
            p25: Self::percentile(&fees, 25),
            p50: Self::percentile(&fees, 50),
            p75: Self::percentile(&fees, 75),
            p90: Self::percentile(&fees, 90),
            sample_count: len,
            last_slot,
        };

        drop(samples); // Release read lock before writing

        *self.cached_percentiles.write() = percentiles;

        debug!(
            p25 = percentiles.p25,
            p50 = percentiles.p50,
            p75 = percentiles.p75,
            p90 = percentiles.p90,
            samples = len,
            "priority_fee_tracker: updated percentiles"
        );
    }

    /// Calculate percentile from sorted array
    fn percentile(sorted: &[u64], p: usize) -> u64 {
        if sorted.is_empty() {
            return 0;
        }
        let idx = (sorted.len() * p / 100).min(sorted.len() - 1);
        sorted[idx]
    }

    /// Get current fee percentiles
    pub fn get_percentiles(&self) -> FeePercentiles {
        *self.cached_percentiles.read()
    }

    /// Get recommended priority fee for an intent tier
    ///
    /// Returns fee in micro-lamports per CU, clamped to config bounds
    pub fn get_fee_for_tier(&self, tier: IntentTier) -> u64 {
        let percentiles = self.get_percentiles();

        // If we don't have enough samples, return a safe default
        if percentiles.sample_count < 5 {
            return self.config.min_priority_fee_micro_lamports.max(100_000); // 0.1 lamports/CU default
        }

        let (base_percentile, multiplier) = match tier {
            IntentTier::Tier0 => (percentiles.p90, self.config.tier0_multiplier),
            IntentTier::Tier1 => (percentiles.p50, self.config.tier1_multiplier),
            IntentTier::Arb => (percentiles.p75, self.config.arb_multiplier),
        };

        let fee = (base_percentile as f64 * multiplier) as u64;

        // Clamp to bounds
        fee.clamp(
            self.config.min_priority_fee_micro_lamports,
            self.config.max_priority_fee_micro_lamports,
        )
    }

    /// Get sample count
    pub fn sample_count(&self) -> usize {
        self.samples.read().len()
    }

    /// Check if tracker has sufficient samples for reliable estimates
    pub fn is_ready(&self) -> bool {
        self.sample_count() >= 10
    }

    /// Log current status
    pub fn log_status(&self) {
        let percentiles = self.get_percentiles();
        info!(
            samples = percentiles.sample_count,
            p25_micro = percentiles.p25,
            p50_micro = percentiles.p50,
            p75_micro = percentiles.p75,
            p90_micro = percentiles.p90,
            last_slot = percentiles.last_slot,
            tier0_fee = self.get_fee_for_tier(IntentTier::Tier0),
            tier1_fee = self.get_fee_for_tier(IntentTier::Tier1),
            arb_fee = self.get_fee_for_tier(IntentTier::Arb),
            "priority_fee_tracker: status"
        );
    }
}

impl Default for PriorityFeeTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_sample_calculates_priority_fee() {
        let tracker = PriorityFeeTracker::new();
        
        // 10000 lamports fee, 100000 CU, base fee 5000
        // priority = (10000 - 5000) / 100000 * 1_000_000 = 50_000 micro-lamports/CU
        let result = tracker.add_sample(100, 10_000, Some(100_000));
        assert_eq!(result, Some(50_000));
    }

    #[test]
    fn test_percentiles_calculation() {
        let tracker = PriorityFeeTracker::new();
        
        // Add samples with varying fees
        for i in 1..=20 {
            let fee = 5000 + (i * 1000); // 6000, 7000, ..., 25000
            tracker.add_sample(i as u64, fee, Some(100_000));
        }

        let percentiles = tracker.get_percentiles();
        assert_eq!(percentiles.sample_count, 20);
        assert!(percentiles.p25 < percentiles.p50);
        assert!(percentiles.p50 < percentiles.p75);
        assert!(percentiles.p75 < percentiles.p90);
    }

    #[test]
    fn test_fee_clamping() {
        let mut config = PriorityFeeConfig::default();
        config.min_priority_fee_micro_lamports = 50_000;
        config.max_priority_fee_micro_lamports = 500_000;
        
        let tracker = PriorityFeeTracker::with_config(config);
        
        // Add very low fee samples
        for i in 1..=20 {
            tracker.add_sample(i as u64, 5100, Some(100_000)); // ~1000 micro-lamports
        }

        // Should be clamped to minimum
        let fee = tracker.get_fee_for_tier(IntentTier::Tier1);
        assert!(fee >= 50_000);
    }

    #[test]
    fn test_skip_invalid_samples() {
        let tracker = PriorityFeeTracker::new();
        
        // Zero compute units
        assert!(tracker.add_sample(1, 10_000, Some(0)).is_none());
        
        // No compute units
        assert!(tracker.add_sample(1, 10_000, None).is_none());
        
        // Very low compute units
        assert!(tracker.add_sample(1, 10_000, Some(500)).is_none());
    }
}
