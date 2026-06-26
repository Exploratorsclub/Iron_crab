//! JSONL host trait — bin implements via `MarketDataContext` (`impl JsonlHost`).

use crate::ipc::MarketEvent;

/// Non-blocking JSONL enqueue surface (I-4b: ingest must not block on disk I/O).
pub trait JsonlHost: Send + Sync {
    fn try_enqueue_market_event(&self, event: &MarketEvent) -> bool;

    fn on_jsonl_enqueue_dropped(&self, event: &MarketEvent);
}
