//! md-state host trait — bin implements via `MarketDataContext` (`impl MdStateContext`).

use super::worker::MdStateSender;
use solana_sdk::pubkey::Pubkey;
use std::collections::HashSet;
use std::sync::Arc;

/// Context surface for the `md-state` OS thread worker loop.
pub trait MdStateContext: Send + Sync {
    fn snapshot_explicit_demand_pubkeys(&self) -> HashSet<Pubkey>;
    fn schedule_geyser_sync_batch_debounced(ctx: &Arc<Self>, md_state: &MdStateSender);
    fn refresh_hot_pool_registry_gauges(&self);
    fn refresh_tracked_membership_snapshot(&self);
    fn touch_tracked_vault_pubkey(&self, vault: &Pubkey);
    fn touch_tracked_bin_array_pubkey(&self, pda: &Pubkey);
    fn touch_tracked_pool_vaults_and_bins_if_tracked(&self, pool: Pubkey);
    fn set_ingest_tokio_handle(&self, handle: tokio::runtime::Handle);
}
