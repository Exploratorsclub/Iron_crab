//! Cold-path JetStream publish slot resolution (I-16).

/// Watermark for cold-path `LivePoolCache::upsert` and `PoolCacheUpdate::geyser_slot`.
///
/// Prefers the RPC account response `context.slot`. When that is zero (unknown), falls back to
/// the monotonic Geyser head tracked by market-data ingest — never silently treats `0` as a
/// valid exit-quote watermark when a head is available.
pub fn resolve_cold_path_publish_slot(rpc_context_slot: u64) -> u64 {
    if rpc_context_slot > 0 {
        return rpc_context_slot;
    }
    crate::metrics::market_data_geyser_head_slot_value()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    #[test]
    fn resolve_prefers_rpc_context_slot_when_positive() {
        assert_eq!(resolve_cold_path_publish_slot(436_375_700), 436_375_700);
    }

    #[test]
    fn resolve_falls_back_to_geyser_head_when_rpc_context_zero() {
        let head = 99_001_234u64;
        crate::metrics::MARKET_DATA_GEYSER_HEAD_SLOT.store(head, Ordering::Relaxed);
        assert_eq!(resolve_cold_path_publish_slot(0), head);
    }
}
