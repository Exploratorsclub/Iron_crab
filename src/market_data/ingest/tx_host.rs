//! TX ingest host trait — bin implements via `MarketDataContext` (`impl TxIngestHost`).

use super::host::IngestHost;
use crate::execution::live_pool_cache::LivePoolCache;
use crate::ipc::{IntentTier, MarketEvent};
use crate::market_data::publish::PublishHost;
use crate::nats::NatsClient;
use crate::solana::dex_parser::OrcaPoolInfo;
use crate::solana::priority_fee_tracker::FeePercentiles;
use solana_sdk::pubkey::Pubkey;

/// Tracked wallet surface for TX meta balance snapshots (no bin-internal types).
#[derive(Debug, Clone, Copy)]
pub struct TxTrackedWalletView {
    pub wallet: Pubkey,
    pub wsol_ata: Pubkey,
}

/// Context surface for Geyser TX ingest (`handle_geyser_transaction_update`).
pub trait TxIngestHost: IngestHost + Send + Sync {
    fn tx_build_version(&self) -> &'static str;
    fn tx_run_id(&self) -> &str;
    fn tx_next_event_id(&self) -> String;
    fn tx_write_market_event_jsonl(&self, event: &MarketEvent);
    fn tx_nats(&self) -> Option<&NatsClient>;
    fn tx_publish_host(&self) -> Option<&dyn PublishHost>;

    fn tx_priority_fee_add_sample(
        &self,
        slot: u64,
        fee_lamports: u64,
        compute_units: Option<u64>,
    ) -> Option<u64>;
    fn tx_priority_fee_sample_count(&self) -> usize;
    fn tx_priority_fee_percentiles(&self) -> FeePercentiles;
    fn tx_priority_fee_for_tier(&self, tier: IntentTier) -> u64;

    fn tx_orca_pool_lookup(&self, pool: &Pubkey) -> Option<OrcaPoolInfo>;

    fn tx_record_pool_created(&self, mint: &str, slot: u64);
    #[allow(clippy::too_many_arguments)]
    fn tx_wallet_tracker_process_trade(
        &self,
        mint: &str,
        trader: &str,
        is_buy: bool,
        sol_amount: u64,
        token_amount: u64,
        slot: u64,
        signature: &str,
    ) -> Vec<MarketEvent>;

    fn tx_creator_cache_get(&self, mint: &str) -> Option<String>;
    fn tx_pool_creator_cache_get(&self, pool: &str) -> Option<String>;
    fn tx_pool_creator_cache_insert(&self, pool: String, creator: String);
    fn tx_creator_cache_insert(&self, mint: String, creator: String);
    fn tx_creator_cache_insert_returning_old(
        &self,
        mint: String,
        creator: String,
    ) -> Option<String>;
    fn tx_live_pool_pumpfun_creator(&self, pool: &Pubkey) -> Option<Pubkey>;

    fn tx_tracked_wallet_view(&self) -> Option<TxTrackedWalletView>;
    fn tx_wallet_native_sol_swap(&self, lamports: u64) -> u64;
    fn tx_wallet_wsol_store(&self, lamports: u64);
    fn tx_wallet_wsol_seen_set(&self);
    /// True when wallet-scoped mint should be enqueued via `TrackWalletMint` (no `tracked_mints` read in ingest).
    fn tx_wallet_mint_needs_track(&self, mint: &Pubkey) -> bool;
    fn tx_wallet_token_account_insert(&self, ata: Pubkey) -> bool;
    fn tx_wallet_token_account_remove(&self, ata: Pubkey) -> bool;
    fn tx_wallet_mint_decimals_insert(&self, mint: Pubkey, decimals: u8);
    /// Refresh Geyser wallet subscribe list after ATA pin (wallet + WSOL ATA + tracked token ATAs).
    fn tx_wallet_notify_geyser_subscribe_accounts_changed(&self);

    fn tx_live_pool_cache(&self) -> &LivePoolCache;
}

impl<T: TxIngestHost + ?Sized> TxIngestHost for std::sync::Arc<T> {
    fn tx_build_version(&self) -> &'static str {
        (**self).tx_build_version()
    }

    fn tx_run_id(&self) -> &str {
        (**self).tx_run_id()
    }

    fn tx_next_event_id(&self) -> String {
        (**self).tx_next_event_id()
    }

    fn tx_write_market_event_jsonl(&self, event: &MarketEvent) {
        (**self).tx_write_market_event_jsonl(event)
    }

    fn tx_nats(&self) -> Option<&NatsClient> {
        (**self).tx_nats()
    }

    fn tx_publish_host(&self) -> Option<&dyn PublishHost> {
        (**self).tx_publish_host()
    }

    fn tx_priority_fee_add_sample(
        &self,
        slot: u64,
        fee_lamports: u64,
        compute_units: Option<u64>,
    ) -> Option<u64> {
        (**self).tx_priority_fee_add_sample(slot, fee_lamports, compute_units)
    }

    fn tx_priority_fee_sample_count(&self) -> usize {
        (**self).tx_priority_fee_sample_count()
    }

    fn tx_priority_fee_percentiles(&self) -> FeePercentiles {
        (**self).tx_priority_fee_percentiles()
    }

    fn tx_priority_fee_for_tier(&self, tier: IntentTier) -> u64 {
        (**self).tx_priority_fee_for_tier(tier)
    }

    fn tx_orca_pool_lookup(&self, pool: &Pubkey) -> Option<OrcaPoolInfo> {
        (**self).tx_orca_pool_lookup(pool)
    }

    fn tx_record_pool_created(&self, mint: &str, slot: u64) {
        (**self).tx_record_pool_created(mint, slot)
    }

    fn tx_wallet_tracker_process_trade(
        &self,
        mint: &str,
        trader: &str,
        is_buy: bool,
        sol_amount: u64,
        token_amount: u64,
        slot: u64,
        signature: &str,
    ) -> Vec<MarketEvent> {
        (**self).tx_wallet_tracker_process_trade(
            mint,
            trader,
            is_buy,
            sol_amount,
            token_amount,
            slot,
            signature,
        )
    }

    fn tx_creator_cache_get(&self, mint: &str) -> Option<String> {
        (**self).tx_creator_cache_get(mint)
    }

    fn tx_pool_creator_cache_get(&self, pool: &str) -> Option<String> {
        (**self).tx_pool_creator_cache_get(pool)
    }

    fn tx_pool_creator_cache_insert(&self, pool: String, creator: String) {
        (**self).tx_pool_creator_cache_insert(pool, creator)
    }

    fn tx_creator_cache_insert(&self, mint: String, creator: String) {
        (**self).tx_creator_cache_insert(mint, creator)
    }

    fn tx_creator_cache_insert_returning_old(
        &self,
        mint: String,
        creator: String,
    ) -> Option<String> {
        (**self).tx_creator_cache_insert_returning_old(mint, creator)
    }

    fn tx_live_pool_pumpfun_creator(&self, pool: &Pubkey) -> Option<Pubkey> {
        (**self).tx_live_pool_pumpfun_creator(pool)
    }

    fn tx_tracked_wallet_view(&self) -> Option<TxTrackedWalletView> {
        (**self).tx_tracked_wallet_view()
    }

    fn tx_wallet_native_sol_swap(&self, lamports: u64) -> u64 {
        (**self).tx_wallet_native_sol_swap(lamports)
    }

    fn tx_wallet_wsol_store(&self, lamports: u64) {
        (**self).tx_wallet_wsol_store(lamports)
    }

    fn tx_wallet_wsol_seen_set(&self) {
        (**self).tx_wallet_wsol_seen_set()
    }

    fn tx_wallet_mint_needs_track(&self, mint: &Pubkey) -> bool {
        (**self).tx_wallet_mint_needs_track(mint)
    }

    fn tx_wallet_token_account_insert(&self, ata: Pubkey) -> bool {
        (**self).tx_wallet_token_account_insert(ata)
    }

    fn tx_wallet_token_account_remove(&self, ata: Pubkey) -> bool {
        (**self).tx_wallet_token_account_remove(ata)
    }

    fn tx_wallet_mint_decimals_insert(&self, mint: Pubkey, decimals: u8) {
        (**self).tx_wallet_mint_decimals_insert(mint, decimals)
    }

    fn tx_wallet_notify_geyser_subscribe_accounts_changed(&self) {
        (**self).tx_wallet_notify_geyser_subscribe_accounts_changed()
    }

    fn tx_live_pool_cache(&self) -> &LivePoolCache {
        (**self).tx_live_pool_cache()
    }
}
