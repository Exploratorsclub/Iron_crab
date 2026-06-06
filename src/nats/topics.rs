//! NATS Topic Constants
//!
//! Per docs/TARGET_ARCHITECTURE.md: Topics are versioned and documented.

/// Topic prefix for all IronCrab messages
pub const TOPIC_PREFIX: &str = "ironcrab";

/// Version suffix for topic compatibility
pub const TOPIC_VERSION: &str = "v1";

/// Market events from market-data service
pub const TOPIC_MARKET_EVENTS: &str = "ironcrab.v1.market_events";

/// Momentum-filtered market events (subset of [`TOPIC_MARKET_EVENTS`] payloads).
/// Other consumers keep using the core topic; momentum-bot prefers this subject to reduce fan-in.
pub const TOPIC_MOMENTUM_MARKET_EVENTS: &str = "ironcrab.v1.market_events.momentum";

/// Momentum active pool lifecycle (mint+pool pins for Geyser reserve subscription; PR-D).
pub const TOPIC_MOMENTUM_ACTIVE_POOLS: &str = "ironcrab.v1.momentum.active_pools";

/// Trade intents from strategy bots
pub const TOPIC_TRADE_INTENTS: &str = "ironcrab.v1.trade_intents";

/// Execution results from execution-engine
pub const TOPIC_EXECUTION_RESULTS: &str = "ironcrab.v1.execution_results";

/// Control requests (bidirectional)
pub const TOPIC_CONTROL_REQUESTS: &str = "ironcrab.v1.control_requests";

/// Control responses (for request/reply pattern)
pub const TOPIC_CONTROL_RESPONSES: &str = "ironcrab.v1.control_responses";

/// Decision records (for audit/debug)
pub const TOPIC_DECISION_RECORDS: &str = "ironcrab.v1.decision_records";

/// Priority fee samples from market-data (for dynamic fee estimation)
/// Published by market-data, consumed by execution-engine
pub const TOPIC_PRIORITY_FEE_SAMPLES: &str = "ironcrab.v1.priority_fee_samples";

/// Pool cache updates from market-data (Single Source of Truth)
/// NOTE: For JetStream persistence, use subject-per-pool pattern from jetstream.rs
pub const TOPIC_POOL_CACHE_UPDATES: &str = "ironcrab.v1.pool_cache_updates";

/// Wallet balance updates — DEPRECATED: JetStream is now SSOT for bot state.
/// WalletBalanceSnapshot in ironcrab.wallet_snapshot.{wallet}.{mint} replaced this.
/// Subject per wallet: ironcrab.v1.wallet_balance.{wallet_address}
#[deprecated(note = "JetStream wallet_snapshot is SSOT; use wallet_snapshot_subject instead")]
pub const TOPIC_WALLET_BALANCE_PREFIX: &str = "ironcrab.v1.wallet_balance";

/// Helper function to build wallet balance topic — DEPRECATED.
#[allow(deprecated)]
#[deprecated(note = "JetStream wallet_snapshot is SSOT; use wallet_snapshot_subject instead")]
pub fn wallet_balance_topic(wallet: &str) -> String {
    format!("{}.{}", TOPIC_WALLET_BALANCE_PREFIX, wallet)
}

/// Wallet balance snapshot (JetStream persisted for strategy reconciliation)
/// Subject per wallet+mint: ironcrab.wallet_snapshot.{wallet}.{mint}
pub const TOPIC_WALLET_SNAPSHOT_PREFIX: &str = "ironcrab.wallet_snapshot";

/// Subject pattern for wallet snapshots (for JetStream consumers)
pub const TOPIC_WALLET_SNAPSHOT_PATTERN: &str = "ironcrab.wallet_snapshot.*.*";

/// Helper function to build wallet snapshot subject
pub fn wallet_snapshot_subject(wallet: &str, mint: &str) -> String {
    format!("{}.{}.{}", TOPIC_WALLET_SNAPSHOT_PREFIX, wallet, mint)
}

/// Wallet TX confirmation (JetStream; market-data → execution-engine)
/// Subject per wallet+signature: ironcrab.wallet_tx_confirm.{wallet}.{signature}
pub const TOPIC_WALLET_TX_CONFIRM_PREFIX: &str = "ironcrab.wallet_tx_confirm";

/// Subject pattern for wallet TX confirmations (for JetStream consumers)
pub const TOPIC_WALLET_TX_CONFIRM_PATTERN: &str = "ironcrab.wallet_tx_confirm.*.*";

/// Build JetStream subject for a wallet TX confirmation event.
pub fn wallet_tx_confirm_subject(wallet: &str, signature: &str) -> String {
    format!(
        "{}.{}.{}",
        TOPIC_WALLET_TX_CONFIRM_PREFIX, wallet, signature
    )
}

/// JetStream subject pattern for pool cache (subject-per-pool for automatic compaction)
/// Each pool gets its own subject: ironcrab.pool_cache.{pool_address}
/// This allows JetStream to keep only the latest state per pool (max_messages_per_subject=1)
pub const TOPIC_POOL_CACHE_PATTERN: &str = "ironcrab.pool_cache.*";

/// JetStream subject pattern for config updates (subject-per-component)
/// Each component gets its own subject: ironcrab.config.{component}
/// JetStream keeps the last config per component (max_messages_per_subject=1)
/// This ensures components get the latest config even if they start after the broadcast.
pub const TOPIC_CONFIG_PATTERN: &str = "ironcrab.config.*";

/// Helper function to build config topic for a specific component
pub fn config_topic(component: &str) -> String {
    format!("ironcrab.config.{}", component)
}

/// Get all topics for subscription
pub fn all_topics() -> Vec<&'static str> {
    vec![
        TOPIC_MARKET_EVENTS,
        TOPIC_MOMENTUM_MARKET_EVENTS,
        TOPIC_TRADE_INTENTS,
        TOPIC_EXECUTION_RESULTS,
        TOPIC_CONTROL_REQUESTS,
        TOPIC_CONTROL_RESPONSES,
        TOPIC_DECISION_RECORDS,
        TOPIC_POOL_CACHE_UPDATES,
    ]
}

/// Subtopic for specific event kinds
pub fn market_events_subtopic(kind: &str) -> String {
    format!("{}.{}", TOPIC_MARKET_EVENTS, kind)
}

/// Subtopic for specific intent sources
pub fn trade_intents_subtopic(source: &str) -> String {
    format!("{}.{}", TOPIC_TRADE_INTENTS, source)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_format() {
        assert!(TOPIC_MARKET_EVENTS.starts_with(TOPIC_PREFIX));
        assert!(TOPIC_MARKET_EVENTS.contains(TOPIC_VERSION));
    }

    #[test]
    fn test_momentum_market_events_topic_versioned() {
        assert!(TOPIC_MOMENTUM_MARKET_EVENTS.starts_with(TOPIC_PREFIX));
        assert!(TOPIC_MOMENTUM_MARKET_EVENTS.contains(TOPIC_VERSION));
        assert!(TOPIC_MOMENTUM_MARKET_EVENTS.starts_with(TOPIC_MARKET_EVENTS));
        assert!(TOPIC_MOMENTUM_MARKET_EVENTS.len() > TOPIC_MARKET_EVENTS.len());
    }

    #[test]
    fn test_momentum_active_pools_topic_versioned() {
        assert!(TOPIC_MOMENTUM_ACTIVE_POOLS.starts_with(TOPIC_PREFIX));
        assert!(TOPIC_MOMENTUM_ACTIVE_POOLS.contains(TOPIC_VERSION));
        assert!(TOPIC_MOMENTUM_ACTIVE_POOLS.contains("momentum"));
        assert!(TOPIC_MOMENTUM_ACTIVE_POOLS.contains("active_pools"));
    }

    #[test]
    fn test_subtopics() {
        let subtopic = market_events_subtopic("pool_created");
        assert_eq!(subtopic, "ironcrab.v1.market_events.pool_created");
    }
}
