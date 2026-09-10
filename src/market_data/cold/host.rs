//! Cold-path host trait — bin implements via `MarketDataContext` (`impl ColdHost`).

use crate::execution::live_pool_cache::LivePoolCache;
use crate::ipc::{ControlResponse, ControlResponseStatus};
use crate::nats::topics::TOPIC_CONTROL_RESPONSES;
use crate::nats::NatsClient;
use crate::solana::rpc::SolanaRpc;
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;
use tracing::{info, warn};

const BUILD_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Context surface for I-24d Ensure* cold-path handlers.
pub trait ColdHost: Send + Sync {
    fn run_id(&self) -> &str;
    fn nats(&self) -> Option<&NatsClient>;
    fn live_pool_cache(&self) -> &LivePoolCache;
    fn live_pool_cache_arc(&self) -> Arc<LivePoolCache>;
    fn raydium_serum_fetched_insert(&self, pool_addr: Pubkey);
    /// True when a prior backfill completed with full static Serum layout in cache.
    fn raydium_serum_fetched_contains(&self, pool_addr: Pubkey) -> bool;
    /// Claim one in-flight serum backfill for `pool_addr` (false if already claimed or complete).
    fn raydium_serum_fetched_try_claim(&self, pool_addr: Pubkey) -> bool;
    /// Release claim after incomplete backfill so a later upsert can retry.
    fn raydium_serum_fetched_remove(&self, pool_addr: Pubkey);
    fn cold_path_rpc(&self) -> Option<Arc<SolanaRpc>>;
}

pub(crate) async fn publish_control_response(
    nats: &NatsClient,
    run_id: &str,
    request_id: &str,
    status: ControlResponseStatus,
    pool_address: Option<String>,
    message: Option<String>,
) {
    let mut resp = ControlResponse::new(
        "market-data",
        BUILD_VERSION,
        run_id,
        request_id.to_string(),
        "market-data",
        status,
    );
    let pool_for_log = pool_address.clone();
    if let Some(pa) = pool_address {
        resp = resp.with_pool_address(pa);
    }
    if let Some(m) = message {
        resp = resp.with_message(m);
    }
    let status_str = format!("{:?}", status);
    if let Err(e) = nats.publish(TOPIC_CONTROL_RESPONSES, &resp).await {
        warn!(
            request_id = %request_id,
            status = %status_str,
            pool_address = ?pool_for_log,
            error = %e,
            "I-24d Discovery: Failed to publish ControlResponse"
        );
    } else {
        info!(
            request_id = %request_id,
            status = %status_str,
            pool_address = ?pool_for_log,
            "I-24d Discovery: ControlResponse published"
        );
    }
}
