//! In-flight cross-DEX arb dedup: one intent per mint+route until EE terminal outcome.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::ipc::{ExecutionResult, ExecutionStatus};

/// Route identity for 2-hop cross-DEX arb dedup.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct InFlightArbKey {
    pub mint: String,
    pub buy_dex: String,
    pub sell_dex: String,
    pub buy_pool: String,
    pub sell_pool: String,
}

impl InFlightArbKey {
    pub fn from_metadata(
        mint: &str,
        buy_dex: &str,
        sell_dex: &str,
        buy_pool: &str,
        sell_pool: &str,
    ) -> Self {
        Self {
            mint: mint.to_string(),
            buy_dex: buy_dex.to_string(),
            sell_dex: sell_dex.to_string(),
            buy_pool: buy_pool.to_string(),
            sell_pool: sell_pool.to_string(),
        }
    }
}

#[derive(Debug, Clone)]
struct InFlightArbEntry {
    intent_id: String,
    published_at: Instant,
    ttl_ms: u64,
}

/// Tracks published arb intents until terminal EE outcome or TTL expiry.
#[derive(Debug, Default)]
pub struct InFlightArbRegistry {
    entries: HashMap<InFlightArbKey, InFlightArbEntry>,
}

impl InFlightArbRegistry {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    /// Returns the blocking in-flight intent id when the route is occupied.
    pub fn blocking_intent_id(&self, key: &InFlightArbKey) -> Option<&str> {
        self.entries.get(key).map(|e| e.intent_id.as_str())
    }

    pub fn register(&mut self, key: InFlightArbKey, intent_id: String, ttl_ms: u64) {
        self.entries.insert(
            key,
            InFlightArbEntry {
                intent_id,
                published_at: Instant::now(),
                ttl_ms,
            },
        );
    }

    pub fn clear_by_intent_id(&mut self, intent_id: &str) -> bool {
        let key = self
            .entries
            .iter()
            .find(|(_, e)| e.intent_id == intent_id)
            .map(|(k, _)| k.clone());
        if let Some(k) = key {
            self.entries.remove(&k);
            return true;
        }
        false
    }

    /// Drop entries whose intent TTL (+ grace) expired without a terminal EE event.
    pub fn expire_stale(&mut self) {
        const TTL_GRACE_MS: u64 = 5_000;
        self.entries.retain(|_, e| {
            e.published_at.elapsed() < Duration::from_millis(e.ttl_ms.saturating_add(TTL_GRACE_MS))
        });
    }

    pub fn handle_execution_result(&mut self, exec: &ExecutionResult) {
        if exec.source != "arb-strategy" {
            return;
        }
        if is_terminal_execution_status(exec.status) {
            self.clear_by_intent_id(&exec.intent_id);
        }
    }
}

pub fn is_terminal_execution_status(status: ExecutionStatus) -> bool {
    matches!(
        status,
        ExecutionStatus::Confirmed | ExecutionStatus::Failed | ExecutionStatus::Timeout
    )
}

pub fn in_flight_key_from_intent_metadata(
    metadata: &std::collections::HashMap<String, String>,
    mint: &str,
) -> Option<InFlightArbKey> {
    if metadata.get("cross_dex_arb") != Some(&"true".to_string()) {
        return None;
    }
    let buy_dex = metadata.get("buy_dex")?;
    let sell_dex = metadata.get("sell_dex")?;
    let buy_pool = metadata.get("buy_pool")?;
    let sell_pool = metadata.get("sell_pool")?;
    Some(InFlightArbKey::from_metadata(
        mint, buy_dex, sell_dex, buy_pool, sell_pool,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ipc::RecordHeader;

    fn arb_exec(intent_id: &str, status: ExecutionStatus) -> ExecutionResult {
        ExecutionResult {
            header: RecordHeader::new("execution-engine", "test", "run"),
            execution_id: format!("exec-{intent_id}"),
            decision_id: format!("dec-{intent_id}"),
            intent_id: intent_id.to_string(),
            source: "arb-strategy".to_string(),
            token_mint: None,
            signature: None,
            bundle_id: None,
            status,
            fill_in: None,
            fill_out: None,
            fill_status: None,
            fill_unavailable_reason: None,
            confirmed_slot: None,
            block_time_unix_ms: None,
            fees: None,
            pnl: None,
            wallet_sol_delta_lamports: None,
            error_message: None,
            error_code: None,
            latency_ms: None,
            metadata: std::collections::HashMap::new(),
        }
    }

    #[test]
    fn publish_blocks_second_route_until_terminal() {
        let mut reg = InFlightArbRegistry::new();
        let key =
            InFlightArbKey::from_metadata("MintA", "meteora_dlmm", "orca", "buy-pool", "sell-pool");
        reg.register(key.clone(), "arb-000019".to_string(), 30_000);
        assert_eq!(reg.blocking_intent_id(&key), Some("arb-000019"));

        reg.handle_execution_result(&arb_exec("arb-000019", ExecutionStatus::Confirmed));
        assert!(reg.blocking_intent_id(&key).is_none());

        reg.register(key.clone(), "arb-000021".to_string(), 30_000);
        assert_eq!(reg.blocking_intent_id(&key), Some("arb-000021"));
    }

    #[test]
    fn sent_status_does_not_release_dedup() {
        let mut reg = InFlightArbRegistry::new();
        let key = InFlightArbKey::from_metadata("M", "a", "b", "p1", "p2");
        reg.register(key.clone(), "arb-sent".to_string(), 30_000);
        reg.handle_execution_result(&arb_exec("arb-sent", ExecutionStatus::Sent));
        assert_eq!(reg.blocking_intent_id(&key), Some("arb-sent"));
    }

    #[test]
    fn failed_status_releases_dedup() {
        let mut reg = InFlightArbRegistry::new();
        let key = InFlightArbKey::from_metadata("M", "a", "b", "p1", "p2");
        reg.register(key.clone(), "arb-fail".to_string(), 30_000);
        reg.handle_execution_result(&arb_exec("arb-fail", ExecutionStatus::Failed));
        assert!(reg.blocking_intent_id(&key).is_none());
    }
}
