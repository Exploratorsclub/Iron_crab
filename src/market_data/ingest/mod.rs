pub mod account_filter;
pub mod host;
pub mod tx_filter;

pub use account_filter::{
    account_geyser_dispatch_priority_high, account_geyser_update_is_dex_pool_owner,
    account_geyser_update_might_be_relevant,
};
pub use host::IngestHost;
pub use tx_filter::geyser_tx_involves_wallet;

use std::sync::Arc;

impl<T: IngestHost + ?Sized> IngestHost for Arc<T> {
    fn ingest_tracked_wallet_pubkeys(
        &self,
    ) -> Option<(solana_sdk::pubkey::Pubkey, solana_sdk::pubkey::Pubkey)> {
        (**self).ingest_tracked_wallet_pubkeys()
    }

    fn ingest_tracked_wallet_token_account_contains(
        &self,
        pubkey: &solana_sdk::pubkey::Pubkey,
    ) -> bool {
        (**self).ingest_tracked_wallet_token_account_contains(pubkey)
    }

    fn ingest_membership_contains(&self, pubkey: &solana_sdk::pubkey::Pubkey) -> bool {
        (**self).ingest_membership_contains(pubkey)
    }

    fn ingest_membership_vault_contains(&self, pubkey: &solana_sdk::pubkey::Pubkey) -> bool {
        (**self).ingest_membership_vault_contains(pubkey)
    }

    fn ingest_membership_bin_array_contains(&self, pubkey: &solana_sdk::pubkey::Pubkey) -> bool {
        (**self).ingest_membership_bin_array_contains(pubkey)
    }

    fn ingest_is_hot_pool(&self, pool: &solana_sdk::pubkey::Pubkey) -> bool {
        (**self).ingest_is_hot_pool(pool)
    }

    fn ingest_pool_mint_map_contains(&self, pool: &solana_sdk::pubkey::Pubkey) -> bool {
        (**self).ingest_pool_mint_map_contains(pool)
    }

    fn ingest_high_priority_bonding_curve_contains(
        &self,
        pool: &solana_sdk::pubkey::Pubkey,
    ) -> bool {
        (**self).ingest_high_priority_bonding_curve_contains(pool)
    }

    fn ingest_wallet_tracks_mint(&self, mint: &solana_sdk::pubkey::Pubkey) -> bool {
        (**self).ingest_wallet_tracks_mint(mint)
    }

    fn ingest_pumpfun_bonding_curve_tracks_wallet(
        &self,
        pool: &solana_sdk::pubkey::Pubkey,
    ) -> bool {
        (**self).ingest_pumpfun_bonding_curve_tracks_wallet(pool)
    }

    fn ingest_pumpfun_wallet_tracks_pool_mint(&self, pool: &solana_sdk::pubkey::Pubkey) -> bool {
        (**self).ingest_pumpfun_wallet_tracks_pool_mint(pool)
    }

    fn ingest_record_membership_snapshot_hit(&self) {
        (**self).ingest_record_membership_snapshot_hit()
    }

    fn ingest_record_vault_high_priority_dispatch(&self) {
        (**self).ingest_record_vault_high_priority_dispatch()
    }
}
