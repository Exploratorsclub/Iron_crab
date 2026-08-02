//! Cold-path JetStream publish slot resolution (I-16).

/// Watermark for cold-path `LivePoolCache::upsert` and `PoolCacheUpdate::geyser_slot`.
///
/// Uses `max(rpc_context_slot, geyser_head)` so a stale RPC `context.slot` cannot publish a slot
/// below the live Geyser head (Mom SLAVE monotonic reject would drop the JetStream update).
/// When both are zero, returns `0` (callers may fall back to `getSlot`).
pub fn resolve_cold_path_publish_slot(rpc_context_slot: u64) -> u64 {
    let head = crate::metrics::market_data_geyser_head_slot_value();
    rpc_context_slot.max(head)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    #[test]
    fn resolve_prefers_rpc_context_slot_when_positive() {
        crate::metrics::MARKET_DATA_GEYSER_HEAD_SLOT.store(0, Ordering::Relaxed);
        assert_eq!(resolve_cold_path_publish_slot(436_375_700), 436_375_700);
    }

    #[test]
    fn resolve_falls_back_to_geyser_head_when_rpc_context_zero() {
        let head = 99_001_234u64;
        crate::metrics::MARKET_DATA_GEYSER_HEAD_SLOT.store(head, Ordering::Relaxed);
        assert_eq!(resolve_cold_path_publish_slot(0), head);
    }

    #[test]
    fn resolve_merges_rpc_context_with_geyser_head_monotonic() {
        crate::metrics::MARKET_DATA_GEYSER_HEAD_SLOT.store(436_771_131, Ordering::Relaxed);
        assert_eq!(
            resolve_cold_path_publish_slot(436_771_116),
            436_771_131,
            "stale RPC context must not publish below Geyser head"
        );
        assert_eq!(
            resolve_cold_path_publish_slot(436_771_200),
            436_771_200,
            "fresh RPC context above head must win"
        );
    }
}
