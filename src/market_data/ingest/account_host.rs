//! Account ingest host trait — bin implements via `MarketDataContext` (`impl AccountIngestHost`).

use super::host::IngestHost;
use crate::ipc::MarketEvent;
use crate::market_data::publish::PublishHost;
use crate::nats::NatsClient;
use solana_sdk::pubkey::Pubkey;

/// Tracked wallet surface for Geyser account balance snapshots (no bin-internal types).
#[derive(Debug, Clone, Copy)]
pub struct AccountTrackedWalletView {
    pub wallet: Pubkey,
    pub wsol_ata: Pubkey,
}

/// Bin-array membership view for Meteora DLMM account ingest (no bin-internal types).
#[derive(Debug, Clone, Copy)]
pub struct AccountBinArrayView {
    pub pool_address: Pubkey,
    pub bin_array_index: i64,
    pub bin_step: u16,
}

/// Context surface for Geyser account ingest (`handle_geyser_account_update`).
pub trait AccountIngestHost: IngestHost + Send + Sync {
    fn account_build_version(&self) -> &'static str;
    fn account_run_id(&self) -> &str;
    fn account_next_event_id(&self) -> String;
    fn account_write_market_event_jsonl(&self, event: &MarketEvent);
    fn account_nats(&self) -> Option<&NatsClient>;
    fn account_publish_host(&self) -> Option<&dyn PublishHost>;

    fn account_tracked_wallet_view(&self) -> Option<AccountTrackedWalletView>;
    fn account_wallet_native_sol_swap(&self, lamports: u64) -> u64;
    fn account_wallet_wsol_swap(&self, lamports: u64) -> u64;
    fn account_wallet_wsol_seen_set(&self);
    fn account_wallet_mint_decimals_get(&self, mint: &Pubkey) -> Option<u8>;
    fn account_wallet_mint_decimals_insert(&self, mint: Pubkey, decimals: u8);

    fn account_membership_mint_contains(&self, pubkey: &Pubkey) -> bool;
    fn account_membership_bin_array_info(&self, pubkey: &Pubkey) -> Option<AccountBinArrayView>;
}

impl<T: AccountIngestHost + ?Sized> AccountIngestHost for std::sync::Arc<T> {
    fn account_build_version(&self) -> &'static str {
        (**self).account_build_version()
    }

    fn account_run_id(&self) -> &str {
        (**self).account_run_id()
    }

    fn account_next_event_id(&self) -> String {
        (**self).account_next_event_id()
    }

    fn account_write_market_event_jsonl(&self, event: &MarketEvent) {
        (**self).account_write_market_event_jsonl(event)
    }

    fn account_nats(&self) -> Option<&NatsClient> {
        (**self).account_nats()
    }

    fn account_publish_host(&self) -> Option<&dyn PublishHost> {
        (**self).account_publish_host()
    }

    fn account_tracked_wallet_view(&self) -> Option<AccountTrackedWalletView> {
        (**self).account_tracked_wallet_view()
    }

    fn account_wallet_native_sol_swap(&self, lamports: u64) -> u64 {
        (**self).account_wallet_native_sol_swap(lamports)
    }

    fn account_wallet_wsol_swap(&self, lamports: u64) -> u64 {
        (**self).account_wallet_wsol_swap(lamports)
    }

    fn account_wallet_wsol_seen_set(&self) {
        (**self).account_wallet_wsol_seen_set()
    }

    fn account_wallet_mint_decimals_get(&self, mint: &Pubkey) -> Option<u8> {
        (**self).account_wallet_mint_decimals_get(mint)
    }

    fn account_wallet_mint_decimals_insert(&self, mint: Pubkey, decimals: u8) {
        (**self).account_wallet_mint_decimals_insert(mint, decimals)
    }

    fn account_membership_mint_contains(&self, pubkey: &Pubkey) -> bool {
        (**self).account_membership_mint_contains(pubkey)
    }

    fn account_membership_bin_array_info(&self, pubkey: &Pubkey) -> Option<AccountBinArrayView> {
        (**self).account_membership_bin_array_info(pubkey)
    }
}
