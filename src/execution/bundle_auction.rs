//! Shared Jito bundle auction budget math for cross-DEX arb (Arb pre-filter + EE tip gate).

use crate::config::FeePolicyCfg;

/// Minimum Jito bundle tip for cross-DEX arb (lamports).
pub const BUNDLE_TIP_MIN_LAMPORTS_DEFAULT: u64 = 100_000;
/// Maximum Jito bundle tip for cross-DEX arb (lamports).
pub const BUNDLE_TIP_MAX_LAMPORTS_DEFAULT: u64 = 2_000_000;
/// Default tip share of estimated arb profit when metadata has no explicit bps (10%).
pub const BUNDLE_TIP_DEFAULT_BPS_DEFAULT: u64 = 1000;
/// Maximum Jito tip spend as share of estimated profit (priority fee is separate).
pub const BUNDLE_AUCTION_MAX_SPEND_BPS_DEFAULT: u64 = 1500;
/// Jito protocol minimum tip (bundles with tip=0 are rejected with -32602).
pub const JITO_BUNDLE_TIP_FLOOR_LAMPORTS_DEFAULT: u64 = 1_000;
/// Typical Solana base fee per signature (lamports).
pub const SOLANA_TX_BASE_FEE_LAMPORTS: u64 = 5_000;

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

    /// Minimum estimated profit so cross-DEX bundle tip + assumed priority + base fee fit in profit.
    pub fn min_profit_for_landing(&self) -> u64 {
        if self.bundle_min_profit_override > 0 {
            return self.bundle_min_profit_override;
        }
        min_profit_for_bundle_landing(
            self.jito_tip_floor_lamports,
            self.assumed_priority_lamports(),
            self.tip_default_bps,
            self.auction_max_spend_bps,
            self.tip_max_lamports,
        )
    }
}

/// Outcome of cross-DEX dynamic bundle tip (arb pre-filter + execution-engine).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrossDexBundleTipOutcome {
    Ok(u64),
    Rejected {
        estimated_profit: u64,
        priority_cost: u64,
        max_auction_spend: u64,
        computed_tip: u64,
    },
}

/// Cross-DEX arb tip: `auction_max_spend_bps` caps tip only; priority is not subtracted from that cap.
pub fn cross_dex_effective_bundle_tip_lamports(
    estimated_profit: u64,
    tip_bps: u64,
    priority_cost_lamports: u64,
    params: &BundleAuctionParams,
) -> CrossDexBundleTipOutcome {
    let max_auction_spend =
        max_auction_spend_lamports(estimated_profit, params.auction_max_spend_bps);

    if params.jito_tip_floor_lamports > max_auction_spend {
        return CrossDexBundleTipOutcome::Rejected {
            estimated_profit,
            priority_cost: priority_cost_lamports,
            max_auction_spend,
            computed_tip: 0,
        };
    }

    let profit_tip = (estimated_profit as u128 * tip_bps as u128 / 10_000) as u64;
    let upper = params.tip_max_lamports.min(max_auction_spend);
    let tip = profit_tip.clamp(params.jito_tip_floor_lamports, upper);

    let total_cost = SOLANA_TX_BASE_FEE_LAMPORTS
        .saturating_add(priority_cost_lamports)
        .saturating_add(tip);
    if total_cost > estimated_profit {
        return CrossDexBundleTipOutcome::Rejected {
            estimated_profit,
            priority_cost: priority_cost_lamports,
            max_auction_spend,
            computed_tip: tip,
        };
    }

    CrossDexBundleTipOutcome::Ok(tip)
}

pub fn estimate_priority_fee_lamports(micro_lamports_per_cu: u64, compute_units: u32) -> u64 {
    (micro_lamports_per_cu as u128 * compute_units as u128 / 1_000_000) as u64
}

/// Minimum estimated profit so [`cross_dex_effective_bundle_tip_lamports`] succeeds at `tip_default_bps`.
pub fn min_profit_for_bundle_landing(
    jito_tip_floor_lamports: u64,
    assumed_priority_lamports: u64,
    tip_default_bps: u64,
    auction_max_spend_bps: u64,
    tip_max_lamports: u64,
) -> u64 {
    if auction_max_spend_bps == 0 {
        return u64::MAX;
    }

    let params = BundleAuctionParams {
        tip_min_lamports: BUNDLE_TIP_MIN_LAMPORTS_DEFAULT,
        tip_max_lamports,
        tip_default_bps,
        auction_max_spend_bps,
        jito_tip_floor_lamports,
        bundle_min_profit_override: 0,
        arb_compute_units: 400_000,
        default_priority_fee_micro_lamports: 100_000,
        config_jito_tip_lamports: 0,
    };

    let floor_min_profit = jito_tip_floor_lamports
        .saturating_mul(10_000)
        .div_ceil(auction_max_spend_bps);
    let cost_floor = SOLANA_TX_BASE_FEE_LAMPORTS
        .saturating_add(assumed_priority_lamports)
        .saturating_add(jito_tip_floor_lamports);
    let mut lo = floor_min_profit.max(cost_floor);
    if matches!(
        cross_dex_effective_bundle_tip_lamports(
            lo,
            tip_default_bps,
            assumed_priority_lamports,
            &params,
        ),
        CrossDexBundleTipOutcome::Ok(_)
    ) {
        return lo;
    }

    let mut hi = lo.saturating_mul(2).max(lo + 1);
    while hi < u64::MAX / 4
        && matches!(
            cross_dex_effective_bundle_tip_lamports(
                hi,
                tip_default_bps,
                assumed_priority_lamports,
                &params,
            ),
            CrossDexBundleTipOutcome::Rejected { .. }
        )
    {
        hi = hi.saturating_mul(2);
    }
    if matches!(
        cross_dex_effective_bundle_tip_lamports(
            hi,
            tip_default_bps,
            assumed_priority_lamports,
            &params,
        ),
        CrossDexBundleTipOutcome::Rejected { .. }
    ) {
        return u64::MAX;
    }

    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if matches!(
            cross_dex_effective_bundle_tip_lamports(
                mid,
                tip_default_bps,
                assumed_priority_lamports,
                &params,
            ),
            CrossDexBundleTipOutcome::Ok(_)
        ) {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    lo
}

pub fn max_auction_spend_lamports(estimated_profit: u64, auction_max_spend_bps: u64) -> u64 {
    (estimated_profit as u128 * auction_max_spend_bps as u128 / 10_000) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn min_profit_for_bundle_at_default_constants() {
        let min = min_profit_for_bundle_landing(1_000, 45_000, 1000, 1500, 2_000_000);
        assert!(
            min < 100_000,
            "tip budget no longer requires ~967k, got {min}"
        );
        assert!(min >= 55_000, "expected ~56k floor, got {min}");
    }

    #[test]
    fn prod_case_low_profit_fails_arb_gate() {
        let min = min_profit_for_bundle_landing(1_000, 45_000, 1000, 1500, 2_000_000);
        assert!(49_000 < min);
    }

    #[test]
    fn min_profit_zero_bps_is_max() {
        assert_eq!(
            min_profit_for_bundle_landing(1_000, 45_000, 1000, 0, 2_000_000),
            u64::MAX
        );
    }

    #[test]
    fn prod_case_959k_tip_matches_expected() {
        let params = BundleAuctionParams {
            tip_min_lamports: BUNDLE_TIP_MIN_LAMPORTS_DEFAULT,
            tip_max_lamports: 2_000_000,
            tip_default_bps: 1000,
            auction_max_spend_bps: 1500,
            jito_tip_floor_lamports: 1_000,
            bundle_min_profit_override: 0,
            arb_compute_units: 450_000,
            default_priority_fee_micro_lamports: 100_000,
            config_jito_tip_lamports: 0,
        };
        let outcome = cross_dex_effective_bundle_tip_lamports(959_889, 1000, 279_985, &params);
        assert_eq!(outcome, CrossDexBundleTipOutcome::Ok(95_988));
    }

    #[test]
    fn tip_positive_when_priority_exceeds_fifteen_percent_of_profit() {
        let params = BundleAuctionParams::default();
        let outcome = cross_dex_effective_bundle_tip_lamports(605_000, 1000, 137_000, &params);
        assert!(matches!(outcome, CrossDexBundleTipOutcome::Ok(t) if t > 0));
    }

    #[test]
    fn rejects_when_floor_exceeds_auction_tip_budget() {
        let params = BundleAuctionParams::default();
        let outcome = cross_dex_effective_bundle_tip_lamports(5_000, 1000, 0, &params);
        assert!(matches!(
            outcome,
            CrossDexBundleTipOutcome::Rejected {
                computed_tip: 0,
                ..
            }
        ));
    }

    #[test]
    fn rejects_when_profit_below_base_priority_and_floor() {
        let params = BundleAuctionParams::default();
        let priority = params.assumed_priority_lamports();
        let profit = SOLANA_TX_BASE_FEE_LAMPORTS
            .saturating_add(priority)
            .saturating_add(params.jito_tip_floor_lamports)
            .saturating_sub(1);
        let outcome = cross_dex_effective_bundle_tip_lamports(
            profit,
            params.tip_default_bps,
            priority,
            &params,
        );
        assert!(matches!(outcome, CrossDexBundleTipOutcome::Rejected { .. }));
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
