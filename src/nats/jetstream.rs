//! JetStream utilities for persistent state recovery
//!
//! This module provides utilities for creating and managing NATS JetStream streams,
//! particularly for the PoolCacheUpdates Single Source of Truth (SSOT) pattern.
//!
//! ## Architecture
//!
//! - market-data: MASTER cache, publishes to JetStream (one subject per pool)
//! - execution-engine: SLAVE cache, consumes with deliver_last() for state recovery
//! - arb-strategy: SLAVE cache, consumes with deliver_last() for state recovery
//!
//! ## Stream Configuration
//!
//! - Stream Name: POOL_CACHE
//! - Subjects: ironcrab.pool_cache.{pool_address}
//! - Retention: 7 days (debugging/recovery window)
//! - Max messages per subject: 1 (automatic compaction, keeps only latest state)
//! - Storage: File (persistent across restarts)
//! - Rollup: Enabled (for future snapshot support)

use anyhow::{Context, Result};
use async_nats::jetstream;
use tracing::{info, warn};

use super::{
    TOPIC_EXECUTION_RESULTS, TOPIC_TRADE_INTENTS, TOPIC_WALLET_SNAPSHOT_PATTERN,
    TOPIC_WALLET_TX_CONFIRM_PATTERN,
};

/// JetStream stream name for pool cache updates
pub const STREAM_NAME: &str = "POOL_CACHE";

/// JetStream stream name for config updates
pub const CONFIG_STREAM_NAME: &str = "CONFIG_UPDATES";

/// JetStream stream name for wallet balance snapshots
pub const WALLET_SNAPSHOT_STREAM_NAME: &str = "WALLET_SNAPSHOT";

/// JetStream stream name for wallet TX confirmations (separate from WALLET_SNAPSHOT for clear lifecycle)
pub const WALLET_TX_CONFIRM_STREAM_NAME: &str = "WALLET_TX_CONFIRM";

/// JetStream stream name for trade intents (persistent, avoids startup race with Core NATS)
pub const TRADE_INTENTS_STREAM_NAME: &str = "TRADE_INTENTS";

/// JetStream stream name for execution results (persistent, enables replay/audit)
pub const EXECUTION_RESULTS_STREAM_NAME: &str = "EXECUTION_RESULTS";

/// Subject pattern for pool cache (subject-per-pool for compaction)
pub const SUBJECT_PATTERN: &str = "ironcrab.pool_cache.*";

/// Get subject for a specific pool address
pub fn pool_subject(pool_address: &str) -> String {
    format!("ironcrab.pool_cache.{}", pool_address)
}

/// Create or update the POOL_CACHE stream with proper configuration
///
/// This function is idempotent - it creates the stream if missing, or updates
/// the configuration if it already exists.
///
/// # Arguments
///
/// * `client` - NATS client with JetStream access
///
/// # Returns
///
/// Ok(()) if stream created/updated successfully
///
/// # Errors
///
/// Returns error if:
/// - JetStream not enabled on server
/// - Insufficient permissions
/// - Network/connection issues
pub async fn ensure_pool_cache_stream(client: &async_nats::Client) -> Result<()> {
    let jetstream = jetstream::new(client.clone());

    // Build stream configuration
    let stream_config = jetstream::stream::Config {
        name: STREAM_NAME.to_string(),
        subjects: vec![SUBJECT_PATTERN.to_string()],
        retention: jetstream::stream::RetentionPolicy::Limits,
        max_age: std::time::Duration::from_secs(7 * 24 * 60 * 60), // 7 days
        storage: jetstream::stream::StorageType::File,
        num_replicas: 1,
        discard: jetstream::stream::DiscardPolicy::Old,
        max_messages_per_subject: 1, // Keep only latest per pool (automatic compaction)
        allow_rollup: true,          // Enable rollup headers for snapshots
        ..Default::default()
    };

    // Try to create or update stream
    match jetstream.get_or_create_stream(stream_config).await {
        Ok(mut stream) => {
            let info = stream.info().await?;
            info!(
                stream_name = %STREAM_NAME,
                subjects = %SUBJECT_PATTERN,
                retention_days = 7,
                max_msgs_per_subject = 1,
                num_messages = info.state.messages,
                storage = ?info.config.storage,
                "JetStream stream ready (created or updated)"
            );
            Ok(())
        }
        Err(e) => {
            warn!(
                stream_name = %STREAM_NAME,
                error = %e,
                "Failed to create/update JetStream stream - check JetStream enabled with -js flag"
            );
            Err(e).context("JetStream stream creation/update failed")
        }
    }
}

/// Create or update the WALLET_SNAPSHOT stream for wallet reconciliation
///
/// Keeps the latest snapshot per wallet+mint (subject-per-wallet+mint).
pub async fn ensure_wallet_snapshot_stream(client: &async_nats::Client) -> Result<()> {
    let jetstream = jetstream::new(client.clone());

    let stream_config = jetstream::stream::Config {
        name: WALLET_SNAPSHOT_STREAM_NAME.to_string(),
        subjects: vec![TOPIC_WALLET_SNAPSHOT_PATTERN.to_string()],
        retention: jetstream::stream::RetentionPolicy::Limits,
        max_age: std::time::Duration::from_secs(7 * 24 * 60 * 60), // 7 days
        storage: jetstream::stream::StorageType::File,
        num_replicas: 1,
        discard: jetstream::stream::DiscardPolicy::Old,
        max_messages_per_subject: 1, // Keep only latest per wallet+mint
        allow_rollup: true,
        ..Default::default()
    };

    match jetstream.get_or_create_stream(stream_config).await {
        Ok(mut stream) => {
            let info = stream.info().await?;
            info!(
                stream_name = %WALLET_SNAPSHOT_STREAM_NAME,
                subjects = %TOPIC_WALLET_SNAPSHOT_PATTERN,
                retention_days = 7,
                max_msgs_per_subject = 1,
                num_messages = info.state.messages,
                storage = ?info.config.storage,
                "JetStream WALLET_SNAPSHOT stream ready"
            );
            Ok(())
        }
        Err(e) => {
            warn!(
                stream_name = %WALLET_SNAPSHOT_STREAM_NAME,
                error = %e,
                "Failed to create/update WALLET_SNAPSHOT stream"
            );
            Err(e).context("WALLET_SNAPSHOT stream creation/update failed")
        }
    }
}

/// Create or update the WALLET_TX_CONFIRM stream for execution-engine TX confirmation.
///
/// Separate from `WALLET_SNAPSHOT`: confirmations are append-only per signature (not compacted
/// per-subject like balance snapshots).
pub async fn ensure_wallet_tx_confirm_stream(client: &async_nats::Client) -> Result<()> {
    let jetstream = jetstream::new(client.clone());

    let stream_config = jetstream::stream::Config {
        name: WALLET_TX_CONFIRM_STREAM_NAME.to_string(),
        subjects: vec![TOPIC_WALLET_TX_CONFIRM_PATTERN.to_string()],
        retention: jetstream::stream::RetentionPolicy::Limits,
        max_age: std::time::Duration::from_secs(24 * 60 * 60), // 24h
        storage: jetstream::stream::StorageType::File,
        num_replicas: 1,
        discard: jetstream::stream::DiscardPolicy::Old,
        ..Default::default()
    };

    match jetstream.get_or_create_stream(stream_config).await {
        Ok(mut stream) => {
            let info = stream.info().await?;
            info!(
                stream_name = %WALLET_TX_CONFIRM_STREAM_NAME,
                subjects = %TOPIC_WALLET_TX_CONFIRM_PATTERN,
                retention_hours = 24,
                num_messages = info.state.messages,
                storage = ?info.config.storage,
                "JetStream WALLET_TX_CONFIRM stream ready"
            );
            Ok(())
        }
        Err(e) => {
            warn!(
                stream_name = %WALLET_TX_CONFIRM_STREAM_NAME,
                error = %e,
                "Failed to create/update WALLET_TX_CONFIRM stream"
            );
            Err(e).context("WALLET_TX_CONFIRM stream creation/update failed")
        }
    }
}

/// Create or update the CONFIG_UPDATES stream for config hot-reload
///
/// This stream ensures components receive config updates even if they start
/// after the control-plane broadcasts (solves race condition).
///
/// # Configuration
///
/// - Stream Name: CONFIG_UPDATES
/// - Subjects: ironcrab.config.*
/// - Retention: 1 day (short, configs are re-published on change)
/// - Max messages per subject: 1 (keeps only latest config per component)
pub async fn ensure_config_stream(client: &async_nats::Client) -> Result<()> {
    let jetstream = jetstream::new(client.clone());

    let stream_config = jetstream::stream::Config {
        name: CONFIG_STREAM_NAME.to_string(),
        subjects: vec!["ironcrab.config.*".to_string()],
        retention: jetstream::stream::RetentionPolicy::Limits,
        max_age: std::time::Duration::from_secs(24 * 60 * 60), // 1 day
        storage: jetstream::stream::StorageType::File,
        num_replicas: 1,
        discard: jetstream::stream::DiscardPolicy::Old,
        max_messages_per_subject: 1, // Keep only latest config per component
        ..Default::default()
    };

    match jetstream.get_or_create_stream(stream_config).await {
        Ok(mut stream) => {
            let info = stream.info().await?;
            info!(
                stream_name = %CONFIG_STREAM_NAME,
                subjects = "ironcrab.config.*",
                max_msgs_per_subject = 1,
                num_messages = info.state.messages,
                "JetStream CONFIG_UPDATES stream ready"
            );
            Ok(())
        }
        Err(e) => {
            warn!(
                stream_name = %CONFIG_STREAM_NAME,
                error = %e,
                "Failed to create CONFIG_UPDATES stream"
            );
            Err(e).context("CONFIG_UPDATES stream creation failed")
        }
    }
}

/// Create or update the TRADE_INTENTS stream
///
/// Persists intents so execution-engine can consume them even when it starts
/// after momentum_bot (fixes Core NATS fire-and-forget race).
pub async fn ensure_trade_intents_stream(client: &async_nats::Client) -> Result<()> {
    let jetstream = jetstream::new(client.clone());

    let stream_config = jetstream::stream::Config {
        name: TRADE_INTENTS_STREAM_NAME.to_string(),
        subjects: vec![TOPIC_TRADE_INTENTS.to_string()],
        retention: jetstream::stream::RetentionPolicy::Limits,
        max_age: std::time::Duration::from_secs(24 * 60 * 60), // 24h
        storage: jetstream::stream::StorageType::File,
        num_replicas: 1,
        discard: jetstream::stream::DiscardPolicy::Old,
        ..Default::default() // no max_messages_per_subject — keep all intents
    };

    match jetstream.get_or_create_stream(stream_config).await {
        Ok(mut stream) => {
            let info = stream.info().await?;
            info!(
                stream_name = %TRADE_INTENTS_STREAM_NAME,
                subject = TOPIC_TRADE_INTENTS,
                num_messages = info.state.messages,
                "JetStream TRADE_INTENTS stream ready"
            );
            Ok(())
        }
        Err(e) => {
            warn!(
                stream_name = %TRADE_INTENTS_STREAM_NAME,
                error = %e,
                "Failed to create/update TRADE_INTENTS stream"
            );
            Err(e).context("TRADE_INTENTS stream creation failed")
        }
    }
}

/// Create or update the EXECUTION_RESULTS stream
///
/// Persists execution results so momentum_bot and market_data can consume them
/// even after restarts (fixes Core NATS fire-and-forget race). Enables replay and audit.
pub async fn ensure_execution_results_stream(client: &async_nats::Client) -> Result<()> {
    let jetstream = jetstream::new(client.clone());

    let stream_config = jetstream::stream::Config {
        name: EXECUTION_RESULTS_STREAM_NAME.to_string(),
        subjects: vec![TOPIC_EXECUTION_RESULTS.to_string()],
        retention: jetstream::stream::RetentionPolicy::Limits,
        max_age: std::time::Duration::from_secs(7 * 24 * 60 * 60), // 7 days
        storage: jetstream::stream::StorageType::File,
        num_replicas: 1,
        discard: jetstream::stream::DiscardPolicy::Old,
        ..Default::default()
    };

    match jetstream.get_or_create_stream(stream_config).await {
        Ok(mut stream) => {
            let info = stream.info().await?;
            info!(
                stream_name = %EXECUTION_RESULTS_STREAM_NAME,
                subject = TOPIC_EXECUTION_RESULTS,
                num_messages = info.state.messages,
                "JetStream EXECUTION_RESULTS stream ready"
            );
            Ok(())
        }
        Err(e) => {
            warn!(
                stream_name = %EXECUTION_RESULTS_STREAM_NAME,
                error = %e,
                "Failed to create/update EXECUTION_RESULTS stream"
            );
            Err(e).context("EXECUTION_RESULTS stream creation failed")
        }
    }
}

/// Consumer config for execution results (All = includes results published before we subscribed)
pub fn execution_results_consumer_config(durable_name: &str) -> jetstream::consumer::pull::Config {
    jetstream::consumer::pull::Config {
        deliver_policy: jetstream::consumer::DeliverPolicy::All,
        ack_policy: jetstream::consumer::AckPolicy::Explicit,
        durable_name: Some(durable_name.to_string()),
        max_ack_pending: 1000,
        filter_subject: TOPIC_EXECUTION_RESULTS.to_string(),
        ..Default::default()
    }
}

/// Get config subject for a specific component
pub fn config_subject(component: &str) -> String {
    format!("ironcrab.config.{}", component)
}

/// Consumer configuration for config updates (gets last config per component)
pub fn config_consumer_config(component: &str) -> jetstream::consumer::pull::Config {
    jetstream::consumer::pull::Config {
        deliver_policy: jetstream::consumer::DeliverPolicy::Last,
        ack_policy: jetstream::consumer::AckPolicy::Explicit,
        filter_subject: format!("ironcrab.config.{}", component),
        ..Default::default()
    }
}

/// Consumer configuration builder for SLAVE cache state recovery
///
/// Use this to create consumers that:
/// 1. Get the last message for each pool (deliver_last())
/// 2. Then receive incremental updates
///
/// # Example
///
/// ```no_run
/// use async_nats::jetstream;
/// use futures::StreamExt;
/// # async fn example(client: async_nats::Client) -> anyhow::Result<()> {
/// let jetstream = jetstream::new(client);
/// let stream = jetstream.get_stream("POOL_CACHE").await?;
///
/// let consumer = stream
///     .create_consumer(jetstream::consumer::pull::Config {
///         deliver_policy: jetstream::consumer::DeliverPolicy::LastPerSubject,
///         ack_policy: jetstream::consumer::AckPolicy::Explicit,
///         ..Default::default()
///     })
///     .await?;
///
/// // Pull all last messages (one per pool) for bootstrap
/// let mut messages = consumer.messages().await?;
/// while let Some(msg) = messages.next().await {
///     let msg = msg?;
///     // Process pool state...
///     let _ = msg.ack().await;
/// }
/// # Ok(())
/// # }
/// ```
pub fn slave_consumer_config() -> jetstream::consumer::pull::Config {
    jetstream::consumer::pull::Config {
        deliver_policy: jetstream::consumer::DeliverPolicy::LastPerSubject,
        ack_policy: jetstream::consumer::AckPolicy::Explicit,
        max_ack_pending: 1000,
        filter_subject: "ironcrab.pool_cache.>".to_string(), // Required for LastPerSubject
        ..Default::default()
    }
}

/// Ephemeral fallback when JetStream bootstrap did not run (empty SLAVE cache).
///
/// `DeliverPolicy::New` — no replay of per-subject historical snapshots. Used by execution-engine
/// and arb-strategy when bootstrap failed or was skipped (FIX-12).
pub fn pool_cache_live_fallback_consumer_config() -> jetstream::consumer::pull::Config {
    jetstream::consumer::pull::Config {
        deliver_policy: jetstream::consumer::DeliverPolicy::New,
        ack_policy: jetstream::consumer::AckPolicy::Explicit,
        max_ack_pending: 1000,
        filter_subject: "ironcrab.pool_cache.>".to_string(),
        ..Default::default()
    }
}

/// Consumer config for live `POOL_CACHE` updates only (`DeliverPolicy::New`).
///
/// Used by **momentum-bot** so the runtime does not replay the per-subject last snapshot for every
/// pool in the stream (hundreds of thousands of messages). Cold-path bootstrap (execution-engine,
/// arb-strategy, `sell_all_keyless`) uses `slave_consumer_config` via
/// `pool_cache_sync::bootstrap_pool_cache_from_jetstream`; live fallback uses
/// [`pool_cache_live_fallback_consumer_config`].
pub fn pool_cache_live_consumer_config() -> jetstream::consumer::pull::Config {
    jetstream::consumer::pull::Config {
        deliver_policy: jetstream::consumer::DeliverPolicy::New,
        ack_policy: jetstream::consumer::AckPolicy::Explicit,
        durable_name: Some("momentum-bot-pool-cache-live".to_string()),
        max_ack_pending: 1000,
        filter_subject: "ironcrab.pool_cache.>".to_string(),
        ..Default::default()
    }
}

/// Consumer config for wallet snapshot recovery (LastPerSubject)
pub fn wallet_snapshot_consumer_config() -> jetstream::consumer::pull::Config {
    jetstream::consumer::pull::Config {
        deliver_policy: jetstream::consumer::DeliverPolicy::LastPerSubject,
        ack_policy: jetstream::consumer::AckPolicy::Explicit,
        max_ack_pending: 1000,
        filter_subject: "ironcrab.wallet_snapshot.>".to_string(),
        ..Default::default()
    }
}

/// Consumer config for live WalletBalanceSnapshot updates (New only, no replay).
/// Used by momentum-bot for real-time position reconciliation.
pub fn wallet_snapshot_live_consumer_config() -> jetstream::consumer::pull::Config {
    jetstream::consumer::pull::Config {
        deliver_policy: jetstream::consumer::DeliverPolicy::New,
        ack_policy: jetstream::consumer::AckPolicy::Explicit,
        durable_name: Some("momentum-bot-wallet-snapshot".to_string()),
        max_ack_pending: 1000,
        filter_subject: "ironcrab.wallet_snapshot.>".to_string(),
        ..Default::default()
    }
}

/// Consumer config for live WalletBalanceSnapshot updates in execution-engine.
/// Durable consumer scoped per wallet; DeliverPolicy may be overridden at create time.
pub fn wallet_snapshot_live_consumer_config_execution_engine(
    wallet: &str,
) -> jetstream::consumer::pull::Config {
    jetstream::consumer::pull::Config {
        deliver_policy: jetstream::consumer::DeliverPolicy::New,
        ack_policy: jetstream::consumer::AckPolicy::Explicit,
        durable_name: Some(format!("execution-engine-wallet-snapshot-{}", wallet)),
        max_ack_pending: 1000,
        filter_subject: format!("ironcrab.wallet_snapshot.{}.*", wallet),
        ..Default::default()
    }
}

/// Consumer config for live WalletTxConfirmed updates in execution-engine.
/// Durable consumer scoped per wallet; `DeliverPolicy::All` includes confirms published
/// before the consumer exists (e.g. stream/EE startup ordering gap).
pub fn wallet_tx_confirm_live_consumer_config_execution_engine(
    wallet: &str,
) -> jetstream::consumer::pull::Config {
    jetstream::consumer::pull::Config {
        deliver_policy: jetstream::consumer::DeliverPolicy::All,
        ack_policy: jetstream::consumer::AckPolicy::Explicit,
        durable_name: Some(format!("execution-engine-wallet-tx-confirm-{}", wallet)),
        max_ack_pending: 1000,
        filter_subject: format!("ironcrab.wallet_tx_confirm.{}.*", wallet),
        ..Default::default()
    }
}

/// Consumer config for trade intents (All = includes intents published before we subscribed)
pub fn trade_intents_consumer_config() -> jetstream::consumer::pull::Config {
    jetstream::consumer::pull::Config {
        deliver_policy: jetstream::consumer::DeliverPolicy::All,
        ack_policy: jetstream::consumer::AckPolicy::Explicit,
        durable_name: Some("execution-engine".to_string()),
        max_ack_pending: 1000,
        filter_subject: TOPIC_TRADE_INTENTS.to_string(),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_subject() {
        let subject = pool_subject("14Nx7vjtSeMVWugP4zUq5EJkD97ZXKRFUCAPhJJ1pump");
        assert_eq!(
            subject,
            "ironcrab.pool_cache.14Nx7vjtSeMVWugP4zUq5EJkD97ZXKRFUCAPhJJ1pump"
        );
    }

    #[test]
    fn test_subject_pattern() {
        assert_eq!(SUBJECT_PATTERN, "ironcrab.pool_cache.*");
    }

    #[test]
    fn test_stream_name() {
        assert_eq!(STREAM_NAME, "POOL_CACHE");
    }

    #[test]
    fn pool_cache_live_consumer_uses_new_deliver_policy() {
        let live = pool_cache_live_consumer_config();
        assert!(matches!(
            live.deliver_policy,
            jetstream::consumer::DeliverPolicy::New
        ));
        assert_eq!(
            live.durable_name.as_deref(),
            Some("momentum-bot-pool-cache-live")
        );
        assert_eq!(live.filter_subject, "ironcrab.pool_cache.>");

        let slave = slave_consumer_config();
        assert!(matches!(
            slave.deliver_policy,
            jetstream::consumer::DeliverPolicy::LastPerSubject
        ));
    }

    #[test]
    fn wallet_snapshot_live_consumer_execution_engine_uses_new_deliver_policy() {
        let wallet = "WALLET123";
        let live = wallet_snapshot_live_consumer_config_execution_engine(wallet);
        assert!(matches!(
            live.deliver_policy,
            jetstream::consumer::DeliverPolicy::New
        ));
        assert_eq!(
            live.durable_name.as_deref(),
            Some("execution-engine-wallet-snapshot-WALLET123")
        );
        assert_eq!(live.filter_subject, "ironcrab.wallet_snapshot.WALLET123.*");
    }

    #[test]
    fn wallet_tx_confirm_live_consumer_execution_engine_uses_all_deliver_policy() {
        let wallet = "WALLET123";
        let live = wallet_tx_confirm_live_consumer_config_execution_engine(wallet);
        assert!(matches!(
            live.deliver_policy,
            jetstream::consumer::DeliverPolicy::All
        ));
        assert_eq!(
            live.durable_name.as_deref(),
            Some("execution-engine-wallet-tx-confirm-WALLET123")
        );
        assert_eq!(
            live.filter_subject,
            "ironcrab.wallet_tx_confirm.WALLET123.*"
        );
    }
}
