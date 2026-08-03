pub mod account_filter;
pub mod account_handler;
pub mod account_host;
pub mod account_parse;
pub mod host;
pub mod tx_filter;
pub mod tx_handler;
pub mod tx_host;
pub mod tx_parse;

pub use account_filter::{
    account_geyser_dispatch_priority_high, account_geyser_enrich_path_needs_classify,
    account_geyser_update_is_dex_pool_owner, account_geyser_update_might_be_relevant,
    account_geyser_update_relevance, classify_account_geyser_update,
    geyser_account_data_looks_like_meteora_bin_array, geyser_update_looks_like_meteora_bin_array,
    AccountGeyserRelevance, AccountUpdateClass,
};
pub use account_handler::{
    handle_geyser_account_update, publish_meteora_dlmm_bin_array_from_geyser,
    DlmmBinArrayPublishOutcome,
};
pub use account_host::{AccountBinArrayView, AccountIngestHost, AccountTrackedWalletView};
pub use account_parse::{
    try_parse_mint_account, try_parse_token_account_balance, wallet_geyser_snapshots_to_publish,
    wsol_ata_balance_lamports_from_geyser_data, WalletGeyserSnapshotMint,
    WalletGeyserSnapshotToPublish, WalletGeyserUpdateSource,
};
pub use host::IngestHost;
pub use tx_filter::geyser_tx_involves_wallet;
pub use tx_handler::handle_geyser_transaction_update;
pub use tx_host::{TxIngestHost, TxTrackedWalletView};
pub use tx_parse::{
    maybe_emit_dev_wallet_after_pool_mint_map, process_wallet_balance_snapshots_from_tx_meta,
    resolve_pumpfun_creator_tx_path, wallet_tx_meta_has_wsol_post_balance,
    wallet_tx_meta_native_sol_post_lamports, wallet_tx_meta_pin_ata_from_balance,
};

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

    fn ingest_membership_mint_contains(&self, pubkey: &solana_sdk::pubkey::Pubkey) -> bool {
        (**self).ingest_membership_mint_contains(pubkey)
    }

    fn ingest_membership_bin_array_contains(&self, pubkey: &solana_sdk::pubkey::Pubkey) -> bool {
        (**self).ingest_membership_bin_array_contains(pubkey)
    }

    fn ingest_exec_hot_vault_contains(&self, pubkey: &solana_sdk::pubkey::Pubkey) -> bool {
        (**self).ingest_exec_hot_vault_contains(pubkey)
    }

    fn ingest_exec_hot_bin_array_contains(&self, pubkey: &solana_sdk::pubkey::Pubkey) -> bool {
        (**self).ingest_exec_hot_bin_array_contains(pubkey)
    }

    fn ingest_is_hot_pool(&self, pool: &solana_sdk::pubkey::Pubkey) -> bool {
        (**self).ingest_is_hot_pool(pool)
    }

    fn ingest_is_open_position_pumpfun_pin(&self, pool: &solana_sdk::pubkey::Pubkey) -> bool {
        (**self).ingest_is_open_position_pumpfun_pin(pool)
    }

    fn ingest_is_enrichment_member(&self, pool: &solana_sdk::pubkey::Pubkey) -> bool {
        (**self).ingest_is_enrichment_member(pool)
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

    fn ingest_record_enrichment_relevance_hit(&self) {
        (**self).ingest_record_enrichment_relevance_hit()
    }
}
