//! Sidefx worker host trait — bin implements via `MarketDataSidefxHost`.

use super::worker::MdSidefxBurstScratch;
use crate::execution::live_pool_cache::LivePoolCache;
use crate::ipc::{MarketEvent, MarketEventKind};
use crate::metrics::MarketDataLatencySegment;
use solana_sdk::pubkey::Pubkey;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Instant;

/// Optional Geyser→core-publish latency trace.
#[derive(Clone, Copy)]
pub struct MarketEventCorePublishTrace {
    pub recv_at: Instant,
    pub cold_path: bool,
    pub segment: MarketDataLatencySegment,
}

/// Lock-free vault view for md-sidefx vault balance ticks.
pub struct SidefxVaultMembershipView {
    pub pool_address: Pubkey,
    pub dex: String,
    pub base_mint: Pubkey,
    pub quote_mint: Pubkey,
    pub active_id: Option<i32>,
    pub bin_step: Option<u16>,
    pub last_balance: Arc<AtomicU64>,
}

/// Context surface for md-sidefx handlers (implemented by bin `MarketDataSidefxHost`).
pub trait SidefxWorkerHost: Send + Sync {
    fn build_version(&self) -> &'static str;
    fn next_event_id(&self) -> String;
    fn write_market_event_jsonl(&self, event: &MarketEvent);
    fn nats_enabled(&self) -> bool;
    fn enqueue_core_market_event(
        &self,
        event: MarketEvent,
        trace: Option<MarketEventCorePublishTrace>,
    ) -> bool;
    fn enqueue_jetstream(
        &self,
        subject: String,
        payload: serde_json::Value,
        log_fail: &'static str,
        bump_market_events_published_total: bool,
    );
    fn flush_lru_touches(&self, scratch: &mut MdSidefxBurstScratch);

    fn live_pool_cache(&self) -> &LivePoolCache;

    fn pool_mint_map_insert(&self, pool: String, mint: String);
    fn pool_mint_map_get(&self, pool: &str) -> Option<String>;

    fn pool_creator_cache_get(&self, pool: &str) -> Option<String>;
    fn pool_creator_cache_insert(&self, pool: String, creator: String);
    fn pool_creator_cache_insert_if_absent(&self, pool: String, creator: String) -> bool;

    fn creator_cache_set(&self, mint: String, creator: String);
    fn creator_cache_insert_if_absent(&self, mint: String, creator: String) -> bool;
    fn creator_cache_insert_returning_old(&self, mint: String, creator: String) -> Option<String>;

    fn high_priority_bonding_curves_insert(&self, pool: Pubkey);
    fn known_pump_amm_pools_insert(&self, pool: Pubkey) -> bool;
    fn known_trade_dex_pools_insert(&self, pool: Pubkey) -> bool;

    fn should_emit_curve_progress(&self, pool: &Pubkey, progress_bps: u32, complete: bool) -> bool;
    fn record_curve_progress_emitted(&self, pool: Pubkey, progress_bps: u32, complete: bool);

    fn vault_membership_view(&self, vault: &Pubkey) -> Option<SidefxVaultMembershipView>;
    fn snapshot_vault_pair_balances(&self, vault: &Pubkey, new_balance: u64) -> Option<(u64, u64)>;
    fn note_trade_pool_lru_touches(&self, pool: Pubkey, scratch: &mut MdSidefxBurstScratch);
}

#[inline]
pub fn market_event_should_nats_core(kind: &MarketEventKind) -> bool {
    !matches!(
        kind,
        MarketEventKind::AccountUpdate { .. } | MarketEventKind::TransactionDetected { .. }
    )
}
