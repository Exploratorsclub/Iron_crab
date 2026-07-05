//! P2 EnrichmentRegistry — membership superset for account relevance + cache publish.
//!
//! Enrichment = `pool_mint_map` ∪ high-priority bonding curves ∪ hot-pool pins (momentum ∪ arb).
//! Broader than execution hot-set for publish; does not add explicit Geyser subs (I-MD-5).

use solana_sdk::pubkey::Pubkey;

/// Pure membership predicate shared by ingest and sidefx hosts.
#[inline]
pub fn pool_is_enrichment_member(
    pool_mint_map: bool,
    high_priority_bonding_curve: bool,
    is_hot_pool: bool,
) -> bool {
    pool_mint_map || high_priority_bonding_curve || is_hot_pool
}

/// Inputs for enrichment membership checks on [`crate::market_data::ingest::IngestHost`].
pub trait EnrichmentMembershipInputs {
    fn enrichment_pool_mint_map_contains(&self, pool: &Pubkey) -> bool;
    fn enrichment_high_priority_bonding_curve_contains(&self, pool: &Pubkey) -> bool;
    fn enrichment_is_hot_pool(&self, pool: &Pubkey) -> bool;
}

#[inline]
pub fn is_enrichment_member_from_inputs(
    inputs: &dyn EnrichmentMembershipInputs,
    pool: &Pubkey,
) -> bool {
    pool_is_enrichment_member(
        inputs.enrichment_pool_mint_map_contains(pool),
        inputs.enrichment_high_priority_bonding_curve_contains(pool),
        inputs.enrichment_is_hot_pool(pool),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestInputs {
        mint_map: bool,
        bonding: bool,
        hot: bool,
    }

    impl EnrichmentMembershipInputs for TestInputs {
        fn enrichment_pool_mint_map_contains(&self, _pool: &Pubkey) -> bool {
            self.mint_map
        }

        fn enrichment_high_priority_bonding_curve_contains(&self, _pool: &Pubkey) -> bool {
            self.bonding
        }

        fn enrichment_is_hot_pool(&self, _pool: &Pubkey) -> bool {
            self.hot
        }
    }

    #[test]
    fn pool_is_enrichment_member_union() {
        let pool = Pubkey::new_unique();
        assert!(!pool_is_enrichment_member(false, false, false));
        assert!(pool_is_enrichment_member(true, false, false));
        assert!(pool_is_enrichment_member(false, true, false));
        assert!(pool_is_enrichment_member(false, false, true));
        let inputs = TestInputs {
            mint_map: false,
            bonding: false,
            hot: true,
        };
        assert!(is_enrichment_member_from_inputs(&inputs, &pool));
    }
}
