//! Cold-path JetStream publish slot resolution (I-16).

use tracing::warn;

/// Minimum Geyser-head lead over RPC `context.slot` before we WARN (slots, not wall-clock).
const RPC_CONTEXT_LAG_WARN_THRESHOLD_SLOTS: u64 = 32;

/// Watermark for cold-path `LivePoolCache::upsert` and `PoolCacheUpdate::geyser_slot`.
///
/// I-16: When `rpc_context_slot > 0`, the publish watermark is **exactly** that slot — the bytes
/// came from that RPC context and must not be labeled with a higher Geyser head (slot spoofing).
/// When zero, falls back to the monotonic Geyser head tracked by market-data ingest.
pub fn resolve_cold_path_publish_slot(rpc_context_slot: u64) -> u64 {
    if rpc_context_slot > 0 {
        return rpc_context_slot;
    }
    crate::metrics::market_data_geyser_head_slot_value()
}

/// Observability when RPC `context.slot` lags Geyser head. Does **not** change the publish watermark.
pub fn observe_cold_path_rpc_context_slot_lag(rpc_context_slot: u64) {
    if rpc_context_slot == 0 {
        return;
    }
    let head = crate::metrics::market_data_geyser_head_slot_value();
    if head <= rpc_context_slot {
        return;
    }
    let delta = head - rpc_context_slot;
    if delta < RPC_CONTEXT_LAG_WARN_THRESHOLD_SLOTS {
        return;
    }
    crate::metrics::inc_market_data_cold_path_rpc_context_lags_geyser_head_total();
    warn!(
        rpc_context_slot,
        geyser_head = head,
        delta,
        "Cold-path RPC context.slot lags Geyser head — publish watermark stays rpc_context_slot (I-16 honest slot)"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    #[test]
    fn resolve_prefers_rpc_context_slot_when_positive() {
        crate::metrics::MARKET_DATA_GEYSER_HEAD_SLOT.store(436_771_131, Ordering::Relaxed);
        assert_eq!(
            resolve_cold_path_publish_slot(436_375_700),
            436_375_700,
            "positive RPC context must win over Geyser head (no max spoofing)"
        );
    }

    #[test]
    fn resolve_falls_back_to_geyser_head_when_rpc_context_zero() {
        let head = 99_001_234u64;
        crate::metrics::MARKET_DATA_GEYSER_HEAD_SLOT.store(head, Ordering::Relaxed);
        assert_eq!(resolve_cold_path_publish_slot(0), head);
    }

    #[test]
    fn resolve_does_not_elevate_stale_rpc_context_to_head() {
        crate::metrics::MARKET_DATA_GEYSER_HEAD_SLOT.store(436_771_131, Ordering::Relaxed);
        assert_eq!(
            resolve_cold_path_publish_slot(436_771_116),
            436_771_116,
            "stale-but-honest RPC context must not be elevated to geyser_head"
        );
    }
}
