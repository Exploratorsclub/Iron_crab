//! Geyser account update relevance + dispatch-priority filters.

use super::host::IngestHost;
use crate::metrics::{
    inc_market_data_account_relevance_enrichment_hit_total, MarketDataAccountEarlyDropReason,
};
use crate::solana::geyser_listener::GeyserAccountUpdate;
use solana_sdk::pubkey::Pubkey;

/// Pre-decoded DEX program owners (no base58 / heap per update).
const RAYDIUM_AMM_V4_OWNER: Pubkey =
    solana_sdk::pubkey!("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8");
const RAYDIUM_CPMM_OWNER: Pubkey =
    solana_sdk::pubkey!("CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C");
const ORCA_WHIRLPOOL_OWNER: Pubkey =
    solana_sdk::pubkey!("whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc");
const PUMPFUN_PROGRAM_OWNER: Pubkey =
    solana_sdk::pubkey!("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P");
const PUMPFUN_AMM_PROGRAM_OWNER: Pubkey =
    solana_sdk::pubkey!("pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA");
const METEORA_DLMM_OWNER: Pubkey =
    solana_sdk::pubkey!("LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo");
const METEORA_CPMM_OWNER: Pubkey =
    solana_sdk::pubkey!("cpmmpPFsKiR4eeYnGSuXgkhLLgGL1j5FUZoJBJU9t9D");

#[inline]
pub fn account_geyser_update_is_dex_pool_owner(owner: &Pubkey) -> bool {
    let o = *owner;
    o == RAYDIUM_AMM_V4_OWNER
        || o == RAYDIUM_CPMM_OWNER
        || o == ORCA_WHIRLPOOL_OWNER
        || o == METEORA_CPMM_OWNER
        || o == METEORA_DLMM_OWNER
        || o == PUMPFUN_PROGRAM_OWNER
        || o == PUMPFUN_AMM_PROGRAM_OWNER
}

/// Result of the cheap account relevance filter (before DEX parse / heavy locks).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccountGeyserRelevance {
    Relevant,
    EarlyDrop(MarketDataAccountEarlyDropReason),
}

/// Cheap filter before DEX parse / heavy locks: drop clearly irrelevant account updates.
/// Conservative: any DEX program owner we parse in `parse_pool_account` / `parse_account_update` stays in.
pub fn account_geyser_update_relevance<H: IngestHost>(
    host: &H,
    u: &GeyserAccountUpdate,
) -> AccountGeyserRelevance {
    if let Some((wallet, wsol_ata)) = host.ingest_tracked_wallet_pubkeys() {
        if u.pubkey == wallet || u.pubkey == wsol_ata {
            return AccountGeyserRelevance::Relevant;
        }
        if host.ingest_tracked_wallet_token_account_contains(&u.pubkey) {
            return AccountGeyserRelevance::Relevant;
        }
    }
    if host.ingest_membership_contains(&u.pubkey) {
        return AccountGeyserRelevance::Relevant;
    }

    if !account_geyser_update_is_dex_pool_owner(&u.owner) {
        return AccountGeyserRelevance::EarlyDrop(
            MarketDataAccountEarlyDropReason::NonDexNonMembership,
        );
    }

    let pool_pk = u.pubkey;
    if host.ingest_is_enrichment_member(&pool_pk) {
        inc_market_data_account_relevance_enrichment_hit_total();
        return AccountGeyserRelevance::Relevant;
    }
    if u.owner == PUMPFUN_PROGRAM_OWNER && host.ingest_pumpfun_bonding_curve_tracks_wallet(&pool_pk)
    {
        return AccountGeyserRelevance::Relevant;
    }
    AccountGeyserRelevance::EarlyDrop(MarketDataAccountEarlyDropReason::DexPoolNotEnrichment)
}

/// Cheap filter before DEX parse / heavy locks: drop clearly irrelevant account updates.
/// Conservative: any DEX program owner we parse in `parse_pool_account` / `parse_account_update` stays in.
pub fn account_geyser_update_might_be_relevant<H: IngestHost>(
    host: &H,
    u: &GeyserAccountUpdate,
) -> bool {
    matches!(
        account_geyser_update_relevance(host, u),
        AccountGeyserRelevance::Relevant
    )
}

/// Strategic HIGH admission for account worker sharding (ACCOUNT-PATH-TX-PARITY-CREATOR).
pub fn account_geyser_dispatch_priority_high<H: IngestHost>(
    host: &H,
    u: &GeyserAccountUpdate,
) -> bool {
    let pool_pk = u.pubkey;
    if host.ingest_membership_vault_contains(&pool_pk) {
        host.ingest_record_vault_high_priority_dispatch();
        return true;
    }
    if host.ingest_membership_bin_array_contains(&pool_pk) {
        host.ingest_record_membership_snapshot_hit();
        return true;
    }
    if host.ingest_high_priority_bonding_curve_contains(&pool_pk) {
        return true;
    }
    if host.ingest_pool_mint_map_contains(&pool_pk) {
        return true;
    }
    if host.ingest_is_enrichment_member(&pool_pk) {
        return true;
    }
    if host.ingest_wallet_tracks_mint(&pool_pk) {
        return true;
    }
    if u.owner == PUMPFUN_PROGRAM_OWNER && host.ingest_pumpfun_wallet_tracks_pool_mint(&pool_pk) {
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::MarketDataAccountEarlyDropReason;
    use std::collections::HashSet;
    use std::time::Instant;

    struct MockIngestHost {
        membership: HashSet<Pubkey>,
        enrichment_members: HashSet<Pubkey>,
        pumpfun_wallet_track: HashSet<Pubkey>,
    }

    impl IngestHost for MockIngestHost {
        fn ingest_tracked_wallet_pubkeys(&self) -> Option<(Pubkey, Pubkey)> {
            None
        }

        fn ingest_tracked_wallet_token_account_contains(&self, _pubkey: &Pubkey) -> bool {
            false
        }

        fn ingest_membership_contains(&self, pubkey: &Pubkey) -> bool {
            self.membership.contains(pubkey)
        }

        fn ingest_membership_vault_contains(&self, _pubkey: &Pubkey) -> bool {
            false
        }

        fn ingest_membership_bin_array_contains(&self, _pubkey: &Pubkey) -> bool {
            false
        }

        fn ingest_is_hot_pool(&self, _pool: &Pubkey) -> bool {
            false
        }

        fn ingest_is_enrichment_member(&self, pool: &Pubkey) -> bool {
            self.enrichment_members.contains(pool)
        }

        fn ingest_pool_mint_map_contains(&self, pool: &Pubkey) -> bool {
            self.enrichment_members.contains(pool)
        }

        fn ingest_high_priority_bonding_curve_contains(&self, _pool: &Pubkey) -> bool {
            false
        }

        fn ingest_wallet_tracks_mint(&self, _mint: &Pubkey) -> bool {
            false
        }

        fn ingest_pumpfun_bonding_curve_tracks_wallet(&self, pool: &Pubkey) -> bool {
            self.pumpfun_wallet_track.contains(pool)
        }

        fn ingest_pumpfun_wallet_tracks_pool_mint(&self, _pool: &Pubkey) -> bool {
            false
        }

        fn ingest_record_membership_snapshot_hit(&self) {}

        fn ingest_record_vault_high_priority_dispatch(&self) {}

        fn ingest_record_enrichment_relevance_hit(&self) {}
    }

    fn sample_update(pubkey: Pubkey, owner: Pubkey) -> GeyserAccountUpdate {
        GeyserAccountUpdate {
            pubkey,
            slot: 1,
            owner,
            data: vec![],
            lamports: 0,
            grpc_recv_at: Instant::now(),
        }
    }

    #[test]
    fn account_geyser_update_relevance_non_enrichment_dex_pool() {
        let host = MockIngestHost {
            membership: HashSet::new(),
            enrichment_members: HashSet::new(),
            pumpfun_wallet_track: HashSet::new(),
        };
        let pool = Pubkey::new_unique();
        let u = sample_update(pool, RAYDIUM_CPMM_OWNER);
        assert_eq!(
            account_geyser_update_relevance(&host, &u),
            AccountGeyserRelevance::EarlyDrop(
                MarketDataAccountEarlyDropReason::DexPoolNotEnrichment
            )
        );
        assert!(!account_geyser_update_might_be_relevant(&host, &u));
    }

    #[test]
    fn account_geyser_update_relevance_random_non_dex_pubkey() {
        let host = MockIngestHost {
            membership: HashSet::new(),
            enrichment_members: HashSet::new(),
            pumpfun_wallet_track: HashSet::new(),
        };
        let pubkey = Pubkey::new_unique();
        let u = sample_update(pubkey, Pubkey::new_unique());
        assert_eq!(
            account_geyser_update_relevance(&host, &u),
            AccountGeyserRelevance::EarlyDrop(
                MarketDataAccountEarlyDropReason::NonDexNonMembership
            )
        );
        assert!(!account_geyser_update_might_be_relevant(&host, &u));
    }

    #[test]
    fn account_geyser_update_relevance_membership_explicit_vault_no_early_drop() {
        let vault = Pubkey::new_unique();
        let host = MockIngestHost {
            membership: HashSet::from([vault]),
            enrichment_members: HashSet::new(),
            pumpfun_wallet_track: HashSet::new(),
        };
        let u = sample_update(vault, Pubkey::new_unique());
        assert_eq!(
            account_geyser_update_relevance(&host, &u),
            AccountGeyserRelevance::Relevant
        );
        assert!(account_geyser_update_might_be_relevant(&host, &u));
    }
}
