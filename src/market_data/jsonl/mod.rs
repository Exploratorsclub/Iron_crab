pub mod filter;
pub mod host;
pub mod writer;

pub use filter::market_event_should_jsonl;
pub use host::JsonlHost;
pub use writer::{spawn_market_data_jsonl_writer, spawn_market_data_jsonl_writer_in_dir};

use crate::ipc::MarketEvent;

/// Filter + bounded enqueue via [`JsonlHost`] (ingest/sidefx call sites).
pub fn write_market_event_jsonl<H: JsonlHost>(host: &H, event: &MarketEvent) {
    if !market_event_should_jsonl(&event.kind) {
        return;
    }
    if !host.try_enqueue_market_event(event) {
        host.on_jsonl_enqueue_dropped(event);
    }
}
