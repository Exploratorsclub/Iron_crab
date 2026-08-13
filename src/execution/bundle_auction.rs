//! Shared Jito bundle auction budget math for cross-DEX arb (Arb pre-filter + EE tip gate).

use crate::config::FeePolicyCfg;

/// Minimum Jito bundle tip for cross-DEX arb (lamports).
pub const BUNDLE_TIP_MIN_LAMPORTS_DEFAULT: u64 = 100_000;
/// Maximum Jito bundle tip for cross-DEX arb (lamports).
pub const BUNDLE_TIP_MAX_LAMPORTS_DEFAULT: u64 = 2_000_000;
/// Default tip share of estimated arb profit when metadata has no explicit bps (10%).
pub const BUNDLE_TIP_DEFAULT_BPS_DEFAULT: u64 = 1000;
/// Maximum total auction spend (tip + priority fee) as share of estimated profit.
pub const BUNDLE_AUCTION_MAX_SPEND_BPS_DEFAULT: u64 = 1500;
/// Jito protocol minimum tip (bundles with tip=0 are rejected with -32602).
pub const JITO_BUNDLE_TIP_FLOOR_LAMPORTS_DEFAULT: u64 = 1_000;

/// Configurable bundle auction parameters shared between arb-strategy and execution-engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BundleAuctionParams {
    pub tip_min_lamports: u64,
    pub tip_max_lamports: u64,
    pub tip_default_bps: u64,
    pub auction_max_spend_bps: u64,
    pub jito_tip_floor_lamports: u64,
    /// When > 0, overrides formula-based minimum profit for arb pre-filter.
    pub bundle_min_profit_override: u64,
    pub arb_compute_units: u32,
    pub default_priority_fee_micro_lamports: u64,
    pub config_jito_tip_lamports: u64,
}

impl Default for BundleAuctionParams {
    fn default() -> Self {
        Self::from_fee_policy_cfg(None, 10_000)
    }
}

impl BundleAuctionParams {
    pub fn from_fee_policy_cfg(fp: Option<&FeePolicyCfg>, jito_tip_lamports: u64) -> Self {
        let fp = fp.cloned().unwrap_or_default();
        Self {
            tip_min_lamports: fp.bundle_tip_min_lamports,
            tip_max_lamports: fp.bundle_tip_max_lamports,
            tip_default_bps: fp.bundle_tip_default_bps,
            auction_max_spend_bps: fp.bundle_auction_max_spend_bps,
            jito_tip_floor_lamports: fp.jito_bundle_tip_floor_lamports,
            bundle_min_profit_override: fp.bundle_min_profit_lamports,
            arb_compute_units: fp.arb_compute_units,
            default_priority_fee_micro_lamports: fp.default_priority_fee_micro_lamports,
            config_jito_tip_lamports: jito_tip_lamports,
        }
    }

    /// Conservative min tip for arb gate: config min tip, configured jito tip, and Jito floor.
    pub fn assumed_min_tip_lamports(&self) -> u64 {
        self.tip_min_lamports
            .max(self.config_jito_tip_lamports)
            .max(self.jito_tip_floor_lamports)
    }

    /// Assumed priority cost for arb pre-filter (static config fee, not dynamic network fee).
    pub fn assumed_priority_lamports(&self) -> u64 {
        estimate_priority_fee_lamports(
            self.default_priority_fee_micro_lamports,
            self.arb_compute_units,
        )
    }

    /// Minimum estimated profit so max_auction_spend can cover min tip + assumed priority.
    pub fn min_profit_for_landing(&self) -> u64 {
        if self.bundle_min_profit_override > 0 {
            return self.bundle_min_profit_override;
        }
        min_profit_for_bundle_landing(
            self.assumed_min_tip_lamports(),
            self.assumed_priority_lamports(),
            self.auction_max_spend_bps,
        )
    }
}

pub fn estimate_priority_fee_lamports(micro_lamports_per_cu: u64, compute_units: u32) -> u64 {
    (micro_lamports_per_cu as u128 * compute_units as u128 / 1_000_000) as u64
}

/// Minimum estimated profit so that `max_auction_spend` can cover `min_tip + assumed_priority`.
pub fn min_profit_for_bundle_landing(
    min_tip_lamports: u64,
    assumed_priority_lamports: u64,
    auction_max_spend_bps: u64,
) -> u64 {
    if auction_max_spend_bps == 0 {
        return u64::MAX;
    }
    let required_spend = min_tip_lamports.saturating_add(assumed_priority_lamports);
    let numerator = required_spend as u128 * 10_000;
    let denominator = auction_max_spend_bps as u128;
    numerator.div_ceil(denominator) as u64
}

pub fn max_auction_spend_lamports(estimated_profit: u64, auction_max_spend_bps: u64) -> u64 {
    (estimated_profit as u128 * auction_max_spend_bps as u128 / 10_000) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn min_profit_for_bundle_967k_at_default_constants() {
        let min = min_profit_for_bundle_landing(100_000, 45_000, 1500);
        assert!(min >= 966_000, "expected >= 966k, got {min}");
    }

    #[test]
    fn prod_case_423k_fails_arb_gate() {
        let min = min_profit_for_bundle_landing(100_000, 45_000, 1500);
        assert!(423_802 < min);
    }

    #[test]
    fn min_profit_zero_bps_is_max() {
        assert_eq!(min_profit_for_bundle_landing(100_000, 45_000, 0), u64::MAX);
    }

    #[test]
    fn assumed_min_tip_uses_config_floor() {
        let params = BundleAuctionParams {
            tip_min_lamports: 50_000,
            tip_max_lamports: BUNDLE_TIP_MAX_LAMPORTS_DEFAULT,
            tip_default_bps: BUNDLE_TIP_DEFAULT_BPS_DEFAULT,
            auction_max_spend_bps: BUNDLE_AUCTION_MAX_SPEND_BPS_DEFAULT,
            jito_tip_floor_lamports: 1_000,
            bundle_min_profit_override: 0,
            arb_compute_units: 400_000,
            default_priority_fee_micro_lamports: 100_000,
            config_jito_tip_lamports: 100_000,
        };
        assert_eq!(params.assumed_min_tip_lamports(), 100_000);
    }
}
