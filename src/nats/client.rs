//! NATS Client Wrapper
//!
//! Thin wrapper around async-nats with:
//! - Timeout handling
//! - Backpressure policy (drop with metrics)
//! - Reconnection handling
//!
//! Note: This module compiles without async-nats dependency for now.
//! When NATS is enabled, add `async-nats = "0.35"` to Cargo.toml.

use serde::{de::DeserializeOwned, Serialize};
use std::time::Duration;
use tracing::{debug, error, warn};

/// NATS client configuration
#[derive(Debug, Clone)]
pub struct NatsConfig {
    /// NATS server URL (e.g., "nats://localhost:4222")
    pub url: String,
    /// Connection name for debugging
    pub name: String,
    /// Reconnect attempts (-1 for infinite)
    pub max_reconnects: i32,
    /// Request timeout
    pub request_timeout: Duration,
    /// Publish timeout (for backpressure)
    pub publish_timeout: Duration,
}

impl Default for NatsConfig {
    fn default() -> Self {
        Self {
            url: std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string()),
            name: "ironcrab".to_string(),
            max_reconnects: -1,
            request_timeout: Duration::from_secs(5),
            publish_timeout: Duration::from_millis(100),
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

/// Placeholder NATS client (compile-time stub)
///
/// When NATS feature is enabled, replace with real async-nats client.
pub struct NatsClient {
    config: NatsConfig,
    connected: bool,
    messages_published: std::sync::atomic::AtomicU64,
    messages_dropped: std::sync::atomic::AtomicU64,
}

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
    /// Note: This is a stub. Real implementation requires async-nats.
    pub async fn connect(&mut self) -> anyhow::Result<()> {
        // Stub: just mark as "connected" for testing
        warn!(
            url = %self.config.url,
            "NATS connect called (stub implementation - add async-nats dependency for real NATS)"
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
        topic: &str,
        msg: &T,
    ) -> anyhow::Result<R> {
        if !self.connected {
            anyhow::bail!("NATS not connected");
        }

        let _json = serde_json::to_vec(msg)?;

        // Stub: cannot actually do request/reply without real NATS
        anyhow::bail!("NATS request/reply not implemented (stub)")
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

/// Stub subscription
pub struct NatsSubscription {
    pub topic: String,
}

impl NatsSubscription {
    /// Receive next message (stub - always returns None after timeout)
    pub async fn next(&mut self) -> Option<NatsMessage> {
        // Stub: sleep briefly then return None
        tokio::time::sleep(Duration::from_millis(100)).await;
        None
    }
}

/// Stub message
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

// ============================================================================
// Feature-gated real NATS implementation
// ============================================================================

/// When async-nats is available, this module provides the real implementation.
/// Add to Cargo.toml: async-nats = { version = "0.35", optional = true }
/// Add feature: nats = ["async-nats"]
#[cfg(feature = "nats")]
pub mod real {
    // Real implementation would go here
    // using async_nats::Client, etc.
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
