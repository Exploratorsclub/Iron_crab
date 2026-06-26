//! JSONL kind filter — excludes high-volume noise from disk logging.

use crate::ipc::MarketEventKind;

/// PR165: high-volume noise kinds excluded from JSONL (NATS core may still receive them).
pub fn market_event_should_jsonl(kind: &MarketEventKind) -> bool {
    !matches!(
        kind,
        MarketEventKind::AccountUpdate { .. } | MarketEventKind::TransactionDetected { .. }
    )
}
