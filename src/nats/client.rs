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
use std::time::Duration;
use tracing::{debug, warn};

#[cfg(feature = "nats")]
use tracing::{error, info};

#[cfg(feature = "nats")]
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
            max_reconnects: -1,                        // infinite reconnects
            request_timeout: Duration::from_secs(5),   // 5s request timeout
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

#[cfg(feature = "nats")]
pub struct NatsClient {
    config: NatsConfig,
    client: Option<async_nats::Client>,
    messages_published: std::sync::atomic::AtomicU64,
    messages_dropped: std::sync::atomic::AtomicU64,
}

#[cfg(feature = "nats")]
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
            self.messages_dropped.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
                self.messages_published.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok(true)
            }
            Ok(Err(e)) => {
                error!(error = %e, topic, "NATS publish error");
                self.messages_dropped.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok(false)
            }
            Err(_) => {
                warn!(topic, "NATS publish timeout (backpressure)");
                self.messages_dropped.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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

    pub fn stats(&self) -> (u64, u64) {
        (
            self.messages_published.load(std::sync::atomic::Ordering::Relaxed),
            self.messages_dropped.load(std::sync::atomic::Ordering::Relaxed),
        )
    }
}

#[cfg(feature = "nats")]
pub struct NatsSubscription {
    subscriber: async_nats::Subscriber,
}

#[cfg(feature = "nats")]
impl NatsSubscription {
    pub async fn next(&mut self) -> Option<NatsMessage> {
        self.subscriber.next().await.map(|msg| NatsMessage {
            topic: msg.subject.to_string(),
            payload: msg.payload.to_vec(),
        })
    }
}

#[cfg(feature = "nats")]
use futures::StreamExt;

// ============================================================================
// Stub NATS implementation (no feature)
// ============================================================================

#[cfg(not(feature = "nats"))]
pub struct NatsClient {
    config: NatsConfig,
    connected: bool,
    messages_published: std::sync::atomic::AtomicU64,
    messages_dropped: std::sync::atomic::AtomicU64,
}

#[cfg(not(feature = "nats"))]
impl NatsClient {
    /// Create a new NATS client (stub - does not actually connect)
    pub fn new(config: NatsConfig) -> Self {
        Self {
            config,
            connected: false,
            messages_published: std::sync::atomic::AtomicU64::new(0),
            messages_dropped: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Connect to NATS server
    ///
    /// Note: This is a stub. Compile with --features nats for real NATS.
    pub async fn connect(&mut self) -> anyhow::Result<()> {
        warn!(
            url = %self.config.url,
            "NATS connect (stub) - compile with --features nats for real NATS"
        );
        self.connected = true;
        Ok(())
    }

    /// Check if connected
    pub fn is_connected(&self) -> bool {
        self.connected
    }

    /// Publish a message (JSON serialized)
    ///
    /// Returns Ok(true) if published, Ok(false) if dropped due to backpressure.
    pub async fn publish<T: Serialize>(&self, topic: &str, msg: &T) -> anyhow::Result<bool> {
        if !self.connected {
            self.messages_dropped
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Ok(false);
        }

        let json = serde_json::to_vec(msg)?;

        // Stub: just log and count
        debug!(
            topic,
            bytes = json.len(),
            "NATS publish (stub)"
        );

        self.messages_published
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(true)
    }

    /// Request/reply pattern
    pub async fn request<T: Serialize, R: DeserializeOwned>(
        &self,
        _topic: &str,
        _msg: &T,
    ) -> anyhow::Result<R> {
        if !self.connected {
            anyhow::bail!("NATS not connected");
        }

        // Stub: cannot actually do request/reply without real NATS
        anyhow::bail!("NATS request/reply requires --features nats")
    }

    /// Subscribe to a topic (returns a stub receiver)
    pub async fn subscribe(&self, topic: &str) -> anyhow::Result<NatsSubscription> {
        if !self.connected {
            anyhow::bail!("NATS not connected");
        }

        debug!(topic, "NATS subscribe (stub)");
        Ok(NatsSubscription {
            topic: topic.to_string(),
        })
    }

    /// Get publish statistics
    pub fn stats(&self) -> (u64, u64) {
        (
            self.messages_published
                .load(std::sync::atomic::Ordering::Relaxed),
            self.messages_dropped
                .load(std::sync::atomic::Ordering::Relaxed),
        )
    }
}

#[cfg(not(feature = "nats"))]
pub struct NatsSubscription {
    pub topic: String,
}

#[cfg(not(feature = "nats"))]
impl NatsSubscription {
    /// Receive next message (stub - always returns None after timeout)
    pub async fn next(&mut self) -> Option<NatsMessage> {
        tokio::time::sleep(Duration::from_millis(100)).await;
        None
    }
}

// ============================================================================
// Common types (both implementations)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_nats_client_stub() {
        let config = NatsConfig::default();
        let mut client = NatsClient::new(config);

        // Initially not connected
        assert!(!client.is_connected());

        // Connect (stub)
        client.connect().await.unwrap();
        assert!(client.is_connected());

        // Publish (stub)
        let result = client
            .publish("test.topic", &serde_json::json!({"key": "value"}))
            .await
            .unwrap();
        assert!(result);

        let (published, dropped) = client.stats();
        assert_eq!(published, 1);
        assert_eq!(dropped, 0);
    }
}
