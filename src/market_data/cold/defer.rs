//! PR235: defer cold-path discovery when md-state queue is under pressure.

use super::host::{publish_control_response, ColdHost};
use crate::ipc::ControlResponseStatus;
use crate::metrics::inc_market_data_discovery_deferred_md_state_pressure_total;
use std::sync::atomic::Ordering;
use tracing::warn;

/// md-state queue cap (mirrors `market_data` bin constant).
const MARKET_DATA_GEYSER_TRACKING_QUEUE_CAP: usize = 8192;
const MARKET_DATA_MD_STATE_DISCOVERY_DEFER_QUEUE_FRAC: f64 = 0.75;

/// PR235: true when md-state queue is saturated enough to defer cold-path discovery RPC.
fn market_data_md_state_queue_pressure_defer_discovery() -> bool {
    let depth =
        crate::metrics::MARKET_DATA_GEYSER_TRACKING_QUEUE_DEPTH.load(Ordering::Relaxed) as f64;
    depth
        >= (MARKET_DATA_GEYSER_TRACKING_QUEUE_CAP as f64)
            * MARKET_DATA_MD_STATE_DISCOVERY_DEFER_QUEUE_FRAC
}

pub async fn defer_discovery_if_md_state_pressure(host: &impl ColdHost, request_id: &str) -> bool {
    if !market_data_md_state_queue_pressure_defer_discovery() {
        return false;
    }
    inc_market_data_discovery_deferred_md_state_pressure_total();
    let depth = crate::metrics::MARKET_DATA_GEYSER_TRACKING_QUEUE_DEPTH.load(Ordering::Relaxed);
    warn!(
        request_id = %request_id,
        queue_depth = depth,
        queue_cap = MARKET_DATA_GEYSER_TRACKING_QUEUE_CAP,
        "PR235: deferring cold-path discovery — md-state queue under pressure"
    );
    if let Some(nats) = host.nats() {
        publish_control_response(
            nats,
            host.run_id(),
            request_id,
            ControlResponseStatus::Busy,
            None,
            Some("md_state_queue_pressure".into()),
        )
        .await;
    }
    true
}
