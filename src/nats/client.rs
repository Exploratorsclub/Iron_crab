//! NATS Client Wrapper
//!
//! Thin wrapper around async-nats with:
//! - Timeout handling
//! - Backpressure policy (drop with metrics)
//! - Reconnection handling
//!
//! Compile with `--features nats` to enable real NATS connection.
//! Without the feature, a stub implementation is used that logs messages.

use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use tracing::warn;

use tracing::{error, info};

use async_nats;

/// NATS client configuration
///
/// All defaults documented (DoD K) P0: No hidden defaults).
#[derive(Debug, Clone)]
pub struct NatsConfig {
    /// NATS server URL. Default: $NATS_URL or "nats://localhost:4222"
    pub url: String,
    /// Connection name for debugging. Default: "ironcrab"
    pub name: String,
    /// Reconnect attempts (-1 for infinite). Default: -1
    pub max_reconnects: i32,
    /// Request timeout. Default: 5s
    pub request_timeout: Duration,
    /// Publish timeout (for backpressure). Default: 100ms
    pub publish_timeout: Duration,
}

impl Default for NatsConfig {
    fn default() -> Self {
        Self {
            url: std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string()),
            name: "ironcrab".to_string(),
            max_reconnects: -1,                          // infinite reconnects
            request_timeout: Duration::from_secs(5),     // 5s request timeout
            publish_timeout: Duration::from_millis(100), // 100ms publish timeout
        }
    }
}

impl NatsConfig {
    pub fn new(url: &str, name: &str) -> Self {
        Self {
            url: url.to_string(),
            name: name.to_string(),
            ..Default::default()
        }
    }
}

// ============================================================================
// Real NATS implementation (feature-gated)
// ============================================================================

pub struct NatsClient {
    config: NatsConfig,
    client: Option<async_nats::Client>,
    messages_published: std::sync::atomic::AtomicU64,
    messages_dropped: std::sync::atomic::AtomicU64,
}

impl NatsClient {
    pub fn new(config: NatsConfig) -> Self {
        Self {
            config,
            client: None,
            messages_published: std::sync::atomic::AtomicU64::new(0),
            messages_dropped: std::sync::atomic::AtomicU64::new(0),
        }
    }

    pub async fn connect(&mut self) -> anyhow::Result<()> {
        info!(url = %self.config.url, name = %self.config.name, "Connecting to NATS");

        let client = async_nats::ConnectOptions::new()
            .name(&self.config.name)
            .request_timeout(Some(self.config.request_timeout))
            .connect(&self.config.url)
            .await?;

        info!(url = %self.config.url, "Connected to NATS");
        self.client = Some(client);
        Ok(())
    }

    pub fn is_connected(&self) -> bool {
        self.client.is_some()
    }

    pub async fn publish<T: Serialize>(&self, topic: &str, msg: &T) -> anyhow::Result<bool> {
        let Some(ref client) = self.client else {
            self.messages_dropped
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Ok(false);
        };

        let json = serde_json::to_vec(msg)?;

        match tokio::time::timeout(
            self.config.publish_timeout,
            client.publish(topic.to_string(), json.into()),
        )
        .await
        {
            Ok(Ok(())) => {
                self.messages_published
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok(true)
            }
            Ok(Err(e)) => {
                error!(error = %e, topic, "NATS publish error");
                self.messages_dropped
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok(false)
            }
            Err(_) => {
                warn!(topic, "NATS publish timeout (backpressure)");
                self.messages_dropped
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok(false)
            }
        }
    }

    pub async fn request<T: Serialize, R: DeserializeOwned>(
        &self,
        topic: &str,
        msg: &T,
    ) -> anyhow::Result<R> {
        let Some(ref client) = self.client else {
            anyhow::bail!("NATS not connected");
        };

        let json = serde_json::to_vec(msg)?;
        let response = client.request(topic.to_string(), json.into()).await?;
        Ok(serde_json::from_slice(&response.payload)?)
    }

    pub async fn subscribe(&self, topic: &str) -> anyhow::Result<NatsSubscription> {
        let Some(ref client) = self.client else {
            anyhow::bail!("NATS not connected");
        };

        let subscriber = client.subscribe(topic.to_string()).await?;
        Ok(NatsSubscription { subscriber })
    }

    /// Get reference to underlying NATS client for JetStream operations
    pub fn client(&self) -> &async_nats::Client {
        self.client
            .as_ref()
            .expect("client() called on disconnected NatsClient")
    }

    /// Publish to JetStream with subject-per-pool pattern
    ///
    /// This uses JetStream for persistent state recovery. Messages are published
    /// with a timeout to prevent blocking on backpressure.
    ///
    /// # Arguments
    ///
    /// * `subject` - JetStream subject (e.g., `ironcrab.pool_cache.{pool_address}`)
    /// * `msg` - Message to serialize and publish
    ///
    /// # Returns
    ///
    /// Ok(true) if published successfully, Ok(false) if dropped due to timeout
    pub async fn jetstream_publish<T: Serialize>(
        &self,
        subject: &str,
        msg: &T,
    ) -> anyhow::Result<bool> {
        let Some(ref client) = self.client else {
            self.messages_dropped
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Ok(false);
        };

        let json = serde_json::to_vec(msg)?;
        let jetstream = async_nats::jetstream::new(client.clone());

        match tokio::time::timeout(
            self.config.publish_timeout,
            jetstream.publish(subject.to_string(), json.into()),
        )
        .await
        {
            Ok(Ok(_ack)) => {
                self.messages_published
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok(true)
            }
            Ok(Err(e)) => {
                error!(error = %e, subject, "JetStream publish error");
                self.messages_dropped
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok(false)
            }
            Err(_) => {
                warn!(subject, "JetStream publish timeout (backpressure)");
                self.messages_dropped
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok(false)
            }
        }
    }

    /// Get or create a JetStream KV bucket
    ///
    /// Creates the bucket if it doesn't exist, otherwise returns the existing one.
    pub async fn get_or_create_kv_bucket(
        &self,
        bucket_name: &str,
    ) -> anyhow::Result<async_nats::jetstream::kv::Store> {
        let Some(ref client) = self.client else {
            anyhow::bail!("NATS not connected");
        };

        let jetstream = async_nats::jetstream::new(client.clone());

        // Try to get existing bucket first
        match jetstream.get_key_value(bucket_name).await {
            Ok(store) => {
                info!(bucket = bucket_name, "Using existing JetStream KV bucket");
                Ok(store)
            }
            Err(_) => {
                // Create new bucket
                info!(bucket = bucket_name, "Creating JetStream KV bucket");
                let store = jetstream
                    .create_key_value(async_nats::jetstream::kv::Config {
                        bucket: bucket_name.to_string(),
                        description: format!("IronCrab {} state", bucket_name),
                        max_value_size: 1024 * 64, // 64KB max per position (should be plenty)
                        history: 1,                // Keep only latest value
                        max_age: std::time::Duration::from_secs(86400 * 7), // 7 day TTL
                        ..Default::default()
                    })
                    .await?;
                Ok(store)
            }
        }
    }

    /// Put a value into a KV bucket
    pub async fn kv_put<T: Serialize>(
        &self,
        store: &async_nats::jetstream::kv::Store,
        key: &str,
        value: &T,
    ) -> anyhow::Result<u64> {
        let json = serde_json::to_vec(value)?;
        let revision = store.put(key, json.into()).await?;
        Ok(revision)
    }

    /// Get a value from a KV bucket
    pub async fn kv_get<T: DeserializeOwned>(
        &self,
        store: &async_nats::jetstream::kv::Store,
        key: &str,
    ) -> anyhow::Result<Option<T>> {
        match store.entry(key).await {
            Ok(Some(entry)) => {
                let value: T = serde_json::from_slice(&entry.value)?;
                Ok(Some(value))
            }
            Ok(None) => Ok(None),
            Err(e) => {
                // Check if it's a "not found" error
                let err_str = e.to_string();
                if err_str.contains("no message found") || err_str.contains("key not found") {
                    Ok(None)
                } else {
                    Err(e.into())
                }
            }
        }
    }

    /// Delete a key from a KV bucket
    pub async fn kv_delete(
        &self,
        store: &async_nats::jetstream::kv::Store,
        key: &str,
    ) -> anyhow::Result<()> {
        store.delete(key).await?;
        Ok(())
    }

    /// List all keys in a KV bucket
    pub async fn kv_keys(
        &self,
        store: &async_nats::jetstream::kv::Store,
    ) -> anyhow::Result<Vec<String>> {
        use futures::TryStreamExt;
        let keys: Vec<String> = store.keys().await?.try_collect().await?;
        Ok(keys)
    }

    /// Get all key-value pairs from a KV bucket
    pub async fn kv_get_all<T: DeserializeOwned>(
        &self,
        store: &async_nats::jetstream::kv::Store,
    ) -> anyhow::Result<HashMap<String, T>> {
        let keys = self.kv_keys(store).await?;
        let mut result = HashMap::new();

        for key in keys {
            if let Some(value) = self.kv_get::<T>(store, &key).await? {
                result.insert(key, value);
            }
        }

        Ok(result)
    }

    pub fn stats(&self) -> (u64, u64) {
        (
            self.messages_published
                .load(std::sync::atomic::Ordering::Relaxed),
            self.messages_dropped
                .load(std::sync::atomic::Ordering::Relaxed),
        )
    }
}

pub struct NatsSubscription {
    subscriber: async_nats::Subscriber,
}

impl NatsSubscription {
    pub async fn next(&mut self) -> Option<NatsMessage> {
        self.subscriber.next().await.map(|msg| NatsMessage {
            topic: msg.subject.to_string(),
            payload: msg.payload.to_vec(),
        })
    }
}

use futures::StreamExt;

// ============================================================================
// Common types
// ============================================================================

/// NATS message wrapper
pub struct NatsMessage {
    pub topic: String,
    pub payload: Vec<u8>,
}

impl NatsMessage {
    /// Deserialize payload
    pub fn deserialize<T: DeserializeOwned>(&self) -> anyhow::Result<T> {
        Ok(serde_json::from_slice(&self.payload)?)
    }
}
