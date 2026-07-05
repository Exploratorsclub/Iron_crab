//! Geyser account update relevance + dispatch-priority filters.

use super::host::IngestHost;
use crate::metrics::inc_market_data_account_relevance_enrichment_hit_total;
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

/// Cheap filter before DEX parse / heavy locks: drop clearly irrelevant account updates.
/// Conservative: any DEX program owner we parse in `parse_pool_account` / `parse_account_update` stays in.
pub fn account_geyser_update_might_be_relevant<H: IngestHost>(
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
        return true;
    }

    if !account_geyser_update_is_dex_pool_owner(&u.owner) {
        return false;
    }

    let pool_pk = u.pubkey;
    if host.ingest_is_enrichment_member(&pool_pk) {
        inc_market_data_account_relevance_enrichment_hit_total();
        return true;
    }
    if u.owner == PUMPFUN_PROGRAM_OWNER && host.ingest_pumpfun_bonding_curve_tracks_wallet(&pool_pk)
    {
        return true;
    }
    false
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
