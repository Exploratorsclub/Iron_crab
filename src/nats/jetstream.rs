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

/// JetStream stream name for pool cache updates
pub const STREAM_NAME: &str = "POOL_CACHE";

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
        allow_rollup: true,           // Enable rollup headers for snapshots
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
///     msg.ack().await?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_subject() {
        let subject = pool_subject("14Nx7vjtSeMVWugP4zUq5EJkD97ZXKRFUCAPhJJ1pump");
        assert_eq!(subject, "ironcrab.pool_cache.14Nx7vjtSeMVWugP4zUq5EJkD97ZXKRFUCAPhJJ1pump");
    }

    #[test]
    fn test_subject_pattern() {
        assert_eq!(SUBJECT_PATTERN, "ironcrab.pool_cache.*");
    }

    #[test]
    fn test_stream_name() {
        assert_eq!(STREAM_NAME, "POOL_CACHE");
    }
}
