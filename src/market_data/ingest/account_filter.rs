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

/// DEX pool owners handled exclusively via `md-sidefx` (`LivePoolCacheAccountUpdate`).
/// Legacy `parse_account_update` remains for Raydium AMM v4, Orca, and PumpFun bonding.
#[inline]
pub fn account_geyser_update_is_sidefx_only_pool_owner(owner: &Pubkey) -> bool {
    let o = *owner;
    o == RAYDIUM_CPMM_OWNER
        || o == METEORA_CPMM_OWNER
        || o == METEORA_DLMM_OWNER
        || o == PUMPFUN_AMM_PROGRAM_OWNER
}

/// Result of the cheap account relevance filter (before DEX parse / heavy locks).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccountGeyserRelevance {
    Relevant,
    EarlyDrop(MarketDataAccountEarlyDropReason),
}

/// Observability class for each Geyser account update at market-data recv (Scope A).
///
/// Maps to existing HIGH/LOW worker dispatch without changing queue semantics:
/// - `Drop` — `account_geyser_update_relevance` early drop (no worker enqueue)
/// - `ExecHot` — `account_geyser_dispatch_priority_high` (hot-pool vault/bin snapshot + Scope C wallet/bonding)
/// - `Enrich` — relevant explicit membership / discovery without EXEC_HOT admission
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccountUpdateClass {
    ExecHot,
    Enrich,
    Drop,
}

impl AccountUpdateClass {
    #[inline]
    pub fn as_prometheus_label(self) -> &'static str {
        match self {
            Self::ExecHot => "exec_hot",
            Self::Enrich => "enrich",
            Self::Drop => "drop",
        }
    }
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
    if u.owner == PUMPFUN_PROGRAM_OWNER && host.ingest_is_open_position_pumpfun_pin(&pool_pk) {
        return AccountGeyserRelevance::Relevant;
    }
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

/// L2d: ENRICH recv cheap prefilter — skip full classify when class cannot be `Enrich`.
///
/// Uses only O(1) snapshot lookups (no RwLock holds, no string keys, no pin scans).
#[inline]
pub fn account_geyser_enrich_path_needs_classify<H: IngestHost>(
    host: &H,
    u: &GeyserAccountUpdate,
) -> bool {
    if let Some((wallet, wsol_ata)) = host.ingest_tracked_wallet_pubkeys() {
        if u.pubkey == wallet || u.pubkey == wsol_ata {
            return true;
        }
        if host.ingest_tracked_wallet_token_account_contains(&u.pubkey) {
            return true;
        }
    }
    if host.ingest_membership_contains(&u.pubkey) {
        if host.ingest_exec_hot_vault_contains(&u.pubkey)
            || host.ingest_exec_hot_bin_array_contains(&u.pubkey)
        {
            return false;
        }
        return true;
    }
    if !account_geyser_update_is_dex_pool_owner(&u.owner) {
        return false;
    }
    let pool_pk = u.pubkey;
    if host.ingest_is_hot_pool(&pool_pk) {
        return false;
    }
    if host.ingest_high_priority_bonding_curve_contains(&pool_pk) {
        return false;
    }
    if host.ingest_wallet_tracks_mint(&pool_pk) {
        return false;
    }
    if u.owner == PUMPFUN_PROGRAM_OWNER {
        if host.ingest_pumpfun_bonding_curve_tracks_wallet(&pool_pk) {
            return true;
        }
        if host.ingest_pumpfun_wallet_tracks_pool_mint(&pool_pk) {
            return false;
        }
    }
    if host.ingest_pool_mint_map_contains(&pool_pk) {
        return true;
    }
    false
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

/// Classify a Geyser account update for lag/throughput metrics (reuses relevance + HIGH logic).
///
/// Returns `(class, early_drop_reason)` from a single relevance evaluation so recv-loop
/// metrics cannot diverge from enqueue decisions when ingest membership changes concurrently.
pub fn classify_account_geyser_update<H: IngestHost>(
    host: &H,
    u: &GeyserAccountUpdate,
) -> (AccountUpdateClass, Option<MarketDataAccountEarlyDropReason>) {
    match account_geyser_update_relevance(host, u) {
        AccountGeyserRelevance::EarlyDrop(reason) => (AccountUpdateClass::Drop, Some(reason)),
        AccountGeyserRelevance::Relevant => {
            let class = if account_geyser_dispatch_priority_high(host, u) {
                AccountUpdateClass::ExecHot
            } else {
                AccountUpdateClass::Enrich
            };
            (class, None)
        }
    }
}

/// Strategic HIGH admission for account worker sharding (Scope F: hot-pool legs + Scope C wallet/bonding).
pub fn account_geyser_dispatch_priority_high<H: IngestHost>(
    host: &H,
    u: &GeyserAccountUpdate,
) -> bool {
    let pool_pk = u.pubkey;
    if host.ingest_is_open_position_pumpfun_pin(&pool_pk) {
        return true;
    }
    if account_geyser_update_is_dex_pool_owner(&u.owner) && host.ingest_is_hot_pool(&pool_pk) {
        return true;
    }
    if host.ingest_exec_hot_vault_contains(&pool_pk) {
        host.ingest_record_vault_high_priority_dispatch();
        return true;
    }
    if host.ingest_exec_hot_bin_array_contains(&pool_pk) {
        host.ingest_record_membership_snapshot_hit();
        return true;
    }
    if host.ingest_high_priority_bonding_curve_contains(&pool_pk) {
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
        hot_pools: HashSet<Pubkey>,
        exec_hot_vaults: HashSet<Pubkey>,
        exec_hot_bin_arrays: HashSet<Pubkey>,
        tracked_wallet_token_accounts: HashSet<Pubkey>,
        pumpfun_wallet_track: HashSet<Pubkey>,
    }

    impl MockIngestHost {
        fn new() -> Self {
            Self {
                membership: HashSet::new(),
                enrichment_members: HashSet::new(),
                hot_pools: HashSet::new(),
                exec_hot_vaults: HashSet::new(),
                exec_hot_bin_arrays: HashSet::new(),
                tracked_wallet_token_accounts: HashSet::new(),
                pumpfun_wallet_track: HashSet::new(),
            }
        }
    }

    impl IngestHost for MockIngestHost {
        fn ingest_tracked_wallet_pubkeys(&self) -> Option<(Pubkey, Pubkey)> {
            None
        }

        fn ingest_tracked_wallet_token_account_contains(&self, pubkey: &Pubkey) -> bool {
            self.tracked_wallet_token_accounts.contains(pubkey)
        }

        fn ingest_membership_contains(&self, pubkey: &Pubkey) -> bool {
            self.membership.contains(pubkey)
        }

        fn ingest_membership_vault_contains(&self, _pubkey: &Pubkey) -> bool {
            false
        }

        fn ingest_membership_mint_contains(&self, pubkey: &Pubkey) -> bool {
            self.membership.contains(pubkey)
        }

        fn ingest_membership_bin_array_contains(&self, _pubkey: &Pubkey) -> bool {
            false
        }

        fn ingest_exec_hot_vault_contains(&self, pubkey: &Pubkey) -> bool {
            self.exec_hot_vaults.contains(pubkey)
        }

        fn ingest_exec_hot_bin_array_contains(&self, pubkey: &Pubkey) -> bool {
            self.exec_hot_bin_arrays.contains(pubkey)
        }

        fn ingest_is_hot_pool(&self, pool: &Pubkey) -> bool {
            self.hot_pools.contains(pool)
        }

        fn ingest_is_open_position_pumpfun_pin(&self, _pool: &Pubkey) -> bool {
            false
        }

        fn ingest_is_enrichment_member(&self, pool: &Pubkey) -> bool {
            self.enrichment_members.contains(pool) || self.hot_pools.contains(pool)
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
        let host = MockIngestHost::new();
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
        let host = MockIngestHost::new();
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
        let mut host = MockIngestHost::new();
        host.membership.insert(vault);
        let u = sample_update(vault, Pubkey::new_unique());
        assert_eq!(
            account_geyser_update_relevance(&host, &u),
            AccountGeyserRelevance::Relevant
        );
        assert!(account_geyser_update_might_be_relevant(&host, &u));
    }

    #[test]
    fn classify_account_geyser_update_enrich_membership_vault_without_hot_pool() {
        let vault = Pubkey::new_unique();
        let mut host = MockIngestHost::new();
        host.membership.insert(vault);
        let u = sample_update(vault, Pubkey::new_unique());
        let (class, early_drop_reason) = classify_account_geyser_update(&host, &u);
        assert_eq!(class, AccountUpdateClass::Enrich);
        assert_eq!(early_drop_reason, None);
    }

    #[test]
    fn classify_account_geyser_update_exec_hot_via_hot_pool_vault_snapshot() {
        let vault = Pubkey::new_unique();
        let mut host = MockIngestHost::new();
        host.membership.insert(vault);
        host.exec_hot_vaults.insert(vault);
        let u = sample_update(vault, Pubkey::new_unique());
        let (class, early_drop_reason) = classify_account_geyser_update(&host, &u);
        assert_eq!(class, AccountUpdateClass::ExecHot);
        assert_eq!(early_drop_reason, None);
    }

    #[test]
    fn classify_account_geyser_update_exec_hot_via_hot_pool_bin_snapshot() {
        let bin = Pubkey::new_unique();
        let mut host = MockIngestHost::new();
        host.membership.insert(bin);
        host.exec_hot_bin_arrays.insert(bin);
        let u = sample_update(bin, Pubkey::new_unique());
        let (class, early_drop_reason) = classify_account_geyser_update(&host, &u);
        assert_eq!(class, AccountUpdateClass::ExecHot);
        assert_eq!(early_drop_reason, None);
    }

    #[test]
    fn classify_account_geyser_update_enrich_enrichment_only_pool() {
        let pool = Pubkey::new_unique();
        let mut host = MockIngestHost::new();
        host.enrichment_members.insert(pool);
        let u = sample_update(pool, RAYDIUM_CPMM_OWNER);
        let (class, early_drop_reason) = classify_account_geyser_update(&host, &u);
        assert_eq!(class, AccountUpdateClass::Enrich);
        assert_eq!(early_drop_reason, None);
    }

    #[test]
    fn classify_account_geyser_update_exec_hot_via_hot_pool_pin() {
        let pool = Pubkey::new_unique();
        let mut host = MockIngestHost::new();
        host.hot_pools.insert(pool);
        let u = sample_update(pool, RAYDIUM_CPMM_OWNER);
        let (class, early_drop_reason) = classify_account_geyser_update(&host, &u);
        assert_eq!(class, AccountUpdateClass::ExecHot);
        assert_eq!(early_drop_reason, None);
    }

    #[test]
    fn classify_account_geyser_update_exec_hot_via_arb_hot_pool_equivalent() {
        let pool = Pubkey::new_unique();
        let mut host = MockIngestHost::new();
        host.hot_pools.insert(pool);
        let u = sample_update(pool, ORCA_WHIRLPOOL_OWNER);
        let (class, early_drop_reason) = classify_account_geyser_update(&host, &u);
        assert_eq!(class, AccountUpdateClass::ExecHot);
        assert_eq!(early_drop_reason, None);
    }

    #[test]
    fn classify_account_geyser_update_enrich_membership_mint_without_high_dispatch() {
        let mint = Pubkey::new_unique();
        let mut host = MockIngestHost::new();
        host.membership.insert(mint);
        let u = sample_update(mint, Pubkey::new_unique());
        let (class, early_drop_reason) = classify_account_geyser_update(&host, &u);
        assert_eq!(class, AccountUpdateClass::Enrich);
        assert_eq!(early_drop_reason, None);
    }

    #[test]
    fn classify_account_geyser_update_drop_non_enrichment_dex_pool() {
        let host = MockIngestHost::new();
        let pool = Pubkey::new_unique();
        let u = sample_update(pool, RAYDIUM_CPMM_OWNER);
        let (class, early_drop_reason) = classify_account_geyser_update(&host, &u);
        assert_eq!(class, AccountUpdateClass::Drop);
        assert_eq!(
            early_drop_reason,
            Some(MarketDataAccountEarlyDropReason::DexPoolNotEnrichment)
        );
    }

    #[test]
    fn account_geyser_enrich_path_skips_exec_hot_hot_pool() {
        let pool = Pubkey::new_unique();
        let mut host = MockIngestHost::new();
        host.hot_pools.insert(pool);
        let u = sample_update(pool, RAYDIUM_CPMM_OWNER);
        assert!(!account_geyser_enrich_path_needs_classify(&host, &u));
        let (class, _) = classify_account_geyser_update(&host, &u);
        assert_eq!(class, AccountUpdateClass::ExecHot);
    }

    #[test]
    fn account_geyser_enrich_path_needs_classify_for_enrichment_only_pool() {
        let pool = Pubkey::new_unique();
        let mut host = MockIngestHost::new();
        host.enrichment_members.insert(pool);
        let u = sample_update(pool, RAYDIUM_CPMM_OWNER);
        assert!(account_geyser_enrich_path_needs_classify(&host, &u));
        let (class, _) = classify_account_geyser_update(&host, &u);
        assert_eq!(class, AccountUpdateClass::Enrich);
    }
}
