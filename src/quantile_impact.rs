//! Quantile-based impact calculator for min_out computation
//!
//! Uses historical fill data to compute statistically-informed slippage estimates
//! based on percentiles (P95, P99) rather than fixed percentages.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};

/// Historical fill observation for a specific pool/pair
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FillObservation {
    /// Pool address or pair identifier
    pub pool_id: String,
    /// Expected output amount (from quote)
    pub expected_out: u64,
    /// Actual output amount (from transaction)
    pub actual_out: u64,
    /// Shortfall percentage (expected - actual) / expected
    pub shortfall_pct: f64,
    /// Timestamp of observation
    pub timestamp_ms: u64,
    /// Trade size category (small/medium/large)
    pub size_category: SizeCategory,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SizeCategory {
    Small,  // < 1% of pool liquidity
    Medium, // 1-5% of pool liquidity
    Large,  // > 5% of pool liquidity
}

/// Configuration for quantile-based slippage calculation
///
/// All defaults documented (DoD K) P0: No hidden defaults).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuantileConfig {
    /// Confidence level (0.95 = P95, 0.99 = P99). Default: 0.95
    pub confidence_level: f64,
    /// Minimum historical samples required before using quantile. Default: 20
    pub min_samples: usize,
    /// Maximum age of samples in seconds. Default: 86400 (24h)
    pub max_sample_age_secs: u64,
    /// Maximum samples to retain per pool. Default: 500
    pub max_samples_per_pool: usize,
    /// Fallback slippage if insufficient data (basis points). Default: 100 (1%)
    pub fallback_slippage_bps: u32,
}

impl Default for QuantileConfig {
    fn default() -> Self {
        Self {
            confidence_level: 0.95,     // P95 confidence
            min_samples: 20,            // need 20 samples for quantile
            max_sample_age_secs: 86400, // 24 hours max age
            max_samples_per_pool: 500,  // cap at 500 per pool
            fallback_slippage_bps: 100, // 1% if insufficient data
        }
    }
}

/// Quantile-based impact calculator
pub struct QuantileImpactCalculator {
    /// Historical fill observations per pool
    observations: Arc<RwLock<HashMap<String, VecDeque<FillObservation>>>>,
    /// Configuration
    config: QuantileConfig,
}

impl QuantileImpactCalculator {
    pub fn new(config: QuantileConfig) -> Self {
        Self {
            observations: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    /// Record a fill observation
    pub fn record_fill(
        &self,
        pool_id: String,
        expected_out: u64,
        actual_out: u64,
        size_category: SizeCategory,
    ) {
        let shortfall_pct = if expected_out > 0 {
            let shortfall = expected_out.saturating_sub(actual_out) as f64;
            (shortfall / expected_out as f64).clamp(0.0, 1.0)
        } else {
            0.0
        };

        let timestamp_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let observation = FillObservation {
            pool_id: pool_id.clone(),
            expected_out,
            actual_out,
            shortfall_pct,
            timestamp_ms,
            size_category,
        };

        let mut obs = self.observations.write().unwrap();
        let pool_obs = obs.entry(pool_id).or_default();

        // Add new observation
        pool_obs.push_back(observation);

        // Trim to max samples
        while pool_obs.len() > self.config.max_samples_per_pool {
            pool_obs.pop_front();
        }
    }

    /// Compute quantile-based min_out for a pool/pair
    pub fn compute_min_out(
        &self,
        pool_id: &str,
        expected_out: u64,
        size_category: SizeCategory,
    ) -> Result<u64> {
        let obs = self.observations.read().unwrap();
        let pool_obs = match obs.get(pool_id) {
            Some(o) => o,
            None => {
                // No historical data, use fallback
                return Ok(self.apply_fallback_slippage(expected_out));
            }
        };

        // Filter observations by age and size category
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let cutoff_ms = now_ms.saturating_sub(self.config.max_sample_age_secs * 1000);

        let mut relevant_shortfalls: Vec<f64> = pool_obs
            .iter()
            .filter(|o| {
                o.timestamp_ms >= cutoff_ms
                    && (o.size_category == size_category || size_category == SizeCategory::Small)
                // Small trades can use all data
            })
            .map(|o| o.shortfall_pct)
            .collect();

        // Check if we have enough samples
        if relevant_shortfalls.len() < self.config.min_samples {
            return Ok(self.apply_fallback_slippage(expected_out));
        }

        // Compute percentile
        let quantile_shortfall = self.compute_percentile(&mut relevant_shortfalls);

        // Apply quantile shortfall with safety margin (add 20% buffer)
        let adjusted_shortfall = (quantile_shortfall * 1.2).min(0.5); // Cap at 50%

        let min_out = (expected_out as f64 * (1.0 - adjusted_shortfall)) as u64;

        Ok(min_out)
    }

    /// Compute percentile from samples
    fn compute_percentile(&self, samples: &mut [f64]) -> f64 {
        if samples.is_empty() {
            return 0.0;
        }

        // Sort samples
        samples.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        // Compute percentile index
        let n = samples.len();
        let index = (self.config.confidence_level * (n as f64 - 1.0)) as usize;

        samples[index.min(n - 1)]
    }

    /// Apply fallback slippage
    fn apply_fallback_slippage(&self, expected_out: u64) -> u64 {
        let keep = 10_000u64.saturating_sub(self.config.fallback_slippage_bps as u64);
        (expected_out as u128 * keep as u128 / 10_000u128) as u64
    }

    /// Get statistics for a pool
    pub fn get_pool_stats(&self, pool_id: &str) -> Option<PoolStats> {
        let obs = self.observations.read().unwrap();
        let pool_obs = obs.get(pool_id)?;

        if pool_obs.is_empty() {
            return None;
        }

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let cutoff_ms = now_ms.saturating_sub(self.config.max_sample_age_secs * 1000);

        let recent_obs: Vec<&FillObservation> = pool_obs
            .iter()
            .filter(|o| o.timestamp_ms >= cutoff_ms)
            .collect();

        if recent_obs.is_empty() {
            return None;
        }

        let mean_shortfall =
            recent_obs.iter().map(|o| o.shortfall_pct).sum::<f64>() / recent_obs.len() as f64;

        let mut shortfalls: Vec<f64> = recent_obs.iter().map(|o| o.shortfall_pct).collect();
        let p50 = self.compute_percentile_at(&mut shortfalls, 0.50);
        let p95 = self.compute_percentile_at(&mut shortfalls, 0.95);
        let p99 = self.compute_percentile_at(&mut shortfalls, 0.99);

        Some(PoolStats {
            sample_count: recent_obs.len(),
            mean_shortfall_pct: mean_shortfall,
            p50_shortfall_pct: p50,
            p95_shortfall_pct: p95,
            p99_shortfall_pct: p99,
        })
    }

    fn compute_percentile_at(&self, samples: &mut [f64], percentile: f64) -> f64 {
        if samples.is_empty() {
            return 0.0;
        }

        samples.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let n = samples.len();
        let index = (percentile * (n as f64 - 1.0)) as usize;

        samples[index.min(n - 1)]
    }

    /// Clear old observations
    pub fn cleanup_old_observations(&self) {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let cutoff_ms = now_ms.saturating_sub(self.config.max_sample_age_secs * 1000);

        let mut obs = self.observations.write().unwrap();

        for pool_obs in obs.values_mut() {
            pool_obs.retain(|o| o.timestamp_ms >= cutoff_ms);
        }

        // Remove pools with no observations
        obs.retain(|_, v| !v.is_empty());
    }

    /// Export observations to JSON for persistence
    pub fn export_observations(&self) -> Result<String> {
        let obs = self.observations.read().unwrap();
        Ok(serde_json::to_string(&*obs)?)
    }

    /// Import observations from JSON
    pub fn import_observations(&self, json: &str) -> Result<()> {
        let imported: HashMap<String, VecDeque<FillObservation>> = serde_json::from_str(json)?;
        let mut obs = self.observations.write().unwrap();
        *obs = imported;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolStats {
    pub sample_count: usize,
    pub mean_shortfall_pct: f64,
    pub p50_shortfall_pct: f64,
    pub p95_shortfall_pct: f64,
    pub p99_shortfall_pct: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_percentile_calculation() {
        let config = QuantileConfig {
            confidence_level: 0.95,
            min_samples: 5,
            ..Default::default()
        };

        let calc = QuantileImpactCalculator::new(config);

        // Add 100 observations with increasing shortfall
        for i in 0..100 {
            let shortfall_pct = i as f64 / 1000.0; // 0% to 10%
            let expected = 100_000u64;
            let actual = (expected as f64 * (1.0 - shortfall_pct)) as u64;

            calc.record_fill(
                "test_pool".to_string(),
                expected,
                actual,
                SizeCategory::Medium,
            );
        }

        // Compute min_out using P95
        let result = calc
            .compute_min_out("test_pool", 100_000, SizeCategory::Medium)
            .unwrap();

        // P95 of 0-10% range should be around 9.5%
        // With 20% buffer: 9.5% * 1.2 = 11.4%
        // min_out ≈ 88,600
        assert!(result < 90_000);
        assert!(result > 85_000);
    }

    #[test]
    fn test_fallback_when_insufficient_data() {
        let config = QuantileConfig {
            min_samples: 20,
            fallback_slippage_bps: 100,
            ..Default::default()
        };

        let calc = QuantileImpactCalculator::new(config);

        // Only 5 samples (< min_samples)
        for _ in 0..5 {
            calc.record_fill(
                "test_pool".to_string(),
                100_000,
                99_000,
                SizeCategory::Small,
            );
        }

        let result = calc
            .compute_min_out("test_pool", 100_000, SizeCategory::Small)
            .unwrap();

        // Should use fallback 1% slippage
        assert_eq!(result, 99_000);
    }

    #[test]
    fn test_pool_stats() {
        let calc = QuantileImpactCalculator::new(QuantileConfig::default());

        // Add varied observations
        calc.record_fill("pool1".to_string(), 100_000, 99_500, SizeCategory::Small);
        calc.record_fill("pool1".to_string(), 100_000, 99_000, SizeCategory::Small);
        calc.record_fill("pool1".to_string(), 100_000, 98_500, SizeCategory::Small);

        let stats = calc.get_pool_stats("pool1").unwrap();

        assert_eq!(stats.sample_count, 3);
        assert!(stats.mean_shortfall_pct > 0.0);
        assert!(stats.mean_shortfall_pct < 0.02); // < 2%
    }
}
