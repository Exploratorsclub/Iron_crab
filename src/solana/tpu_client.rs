//! TPU Direct Client for low-latency transaction submission.
//!
//! TPU (Transaction Processing Unit) Direct sends transactions directly to the
//! current and upcoming slot leaders via QUIC, bypassing RPC node queues.
//!
//! Benefits:
//! - ~50-100ms latency vs ~200-400ms via RPC sendTransaction
//! - Fan-out to multiple leaders for redundancy
//! - No RPC rate limits
//!
//! Requirements:
//! - Access to an RPC node (for leader schedule + cluster info)
//! - WebSocket connection (for leader schedule updates)
//! - Network connectivity to validator TPU ports (QUIC)
//!
//! Note: Works without stake, but stake provides priority during congestion.
//!
//! Platform Support:
//! - Linux: Full TPU Direct via QUIC (solana-quic-client)
//! - Windows: Stub implementation (falls back to RPC in tx_sender)

use anyhow::Result;
use solana_sdk::{signature::Signature, transaction::Transaction};
use std::sync::Arc;
use tracing::warn;

// info! is used on Linux only
#[cfg(not(windows))]
use tracing::info;

/// Error types specific to TPU submission
#[derive(Debug, thiserror::Error)]
pub enum TpuError {
    #[error("TPU client not initialized")]
    NotInitialized,
    #[error("TPU not supported on this platform")]
    NotSupported,
    #[error("Failed to create TPU client: {0}")]
    ClientCreation(String),
    #[error("Failed to send transaction: {0}")]
    SendFailed(String),
    #[error("Reconnection failed: {0}")]
    ReconnectFailed(String),
}

/// Configuration for the TPU submitter
#[derive(Debug, Clone)]
pub struct TpuSubmitterConfig {
    /// Number of leader slots to fan out to (send to current + next N leaders)
    pub fanout_slots: u64,
    /// Number of times to forward TX to leaders
    pub leader_forward_count: u64,
}

impl Default for TpuSubmitterConfig {
    fn default() -> Self {
        Self {
            fanout_slots: 2,
            leader_forward_count: 4,
        }
    }
}

// ============================================================================
// Platform-specific implementations
// ============================================================================

/// TPU Direct transaction submitter.
///
/// On Linux: Uses solana-quic-client for direct QUIC connections to leaders.
/// On Windows: Stub that returns NotSupported (TxSender falls back to RPC).
#[cfg(not(windows))]
pub struct TpuSubmitter {
    /// The underlying TPU client
    tpu_client: Arc<tokio::sync::RwLock<Option<TpuClientInner>>>,
    /// RPC client for leader schedule queries
    rpc_client: Arc<solana_client::rpc_client::RpcClient>,
    /// WebSocket URL for leader schedule updates
    ws_url: String,
    /// Configuration
    config: TpuSubmitterConfig,
    /// Track consecutive failures for backoff
    consecutive_failures: Arc<std::sync::atomic::AtomicU32>,
}

#[cfg(not(windows))]
type TpuClientInner = solana_tpu_client::tpu_client::TpuClient<
    solana_quic_client::QuicPool,
    solana_quic_client::QuicConnectionManager,
    solana_quic_client::QuicConfig,
>;

#[cfg(not(windows))]
impl TpuSubmitter {
    /// Create a new TPU submitter.
    pub async fn new(
        rpc_client: Arc<solana_client::rpc_client::RpcClient>,
        ws_url: &str,
        config: TpuSubmitterConfig,
    ) -> Result<Self> {
        use solana_connection_cache::connection_cache::ConnectionCache;
        use solana_quic_client::{QuicConfig, QuicConnectionManager, QuicPool};
        use solana_tpu_client::tpu_client::{TpuClient, TpuClientConfig};

        info!(
            ws_url = %ws_url,
            fanout_slots = config.fanout_slots,
            "Creating TPU client with QUIC"
        );

        let tpu_config = TpuClientConfig {
            fanout_slots: config.fanout_slots,
            ..TpuClientConfig::default()
        };

        // Create QUIC connection manager
        let connection_cache = ConnectionCache::<QuicPool, QuicConnectionManager, QuicConfig>::new(
            "ironcrab-tpu",
            solana_connection_cache::connection_cache::DEFAULT_CONNECTION_POOL_SIZE,
            solana_quic_client::QuicConfig::default(),
        )?;

        // Create TPU client with connection cache
        let tpu_client = TpuClient::new_with_connection_cache(
            Arc::clone(&rpc_client),
            ws_url,
            tpu_config,
            Arc::new(connection_cache),
        )
        .map_err(|e| anyhow::anyhow!("Failed to create TPU client: {:?}", e))?;

        info!("TPU client initialized successfully");

        Ok(Self {
            tpu_client: Arc::new(tokio::sync::RwLock::new(Some(tpu_client))),
            rpc_client,
            ws_url: ws_url.to_string(),
            config,
            consecutive_failures: Arc::new(std::sync::atomic::AtomicU32::new(0)),
        })
    }

    /// Send a transaction via TPU Direct.
    pub async fn send_transaction(&self, tx: &Transaction) -> Result<Signature, TpuError> {
        let guard = self.tpu_client.read().await;
        let client = guard.as_ref().ok_or(TpuError::NotInitialized)?;

        let signature = tx.signatures.first().copied().unwrap_or_default();

        // try_send_transaction returns TransportResult
        match client.try_send_transaction(tx) {
            Ok(()) => {
                self.consecutive_failures
                    .store(0, std::sync::atomic::Ordering::Relaxed);
                Ok(signature)
            }
            Err(e) => {
                self.consecutive_failures
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Err(TpuError::SendFailed(format!("{:?}", e)))
            }
        }
    }

    /// Check if the TPU client is initialized and ready.
    pub async fn is_ready(&self) -> bool {
        self.tpu_client.read().await.is_some()
    }

    /// Get the number of consecutive send failures.
    pub fn consecutive_failures(&self) -> u32 {
        self.consecutive_failures
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Suggest reconnection if failures exceed threshold.
    pub fn should_reconnect(&self, threshold: u32) -> bool {
        self.consecutive_failures() >= threshold
    }

    /// Reconnect the TPU client.
    pub async fn reconnect(&self) -> Result<(), TpuError> {
        use solana_connection_cache::connection_cache::ConnectionCache;
        use solana_quic_client::{QuicConfig, QuicConnectionManager, QuicPool};
        use solana_tpu_client::tpu_client::{TpuClient, TpuClientConfig};

        info!("Reconnecting TPU client");

        let tpu_config = TpuClientConfig {
            fanout_slots: self.config.fanout_slots,
            ..TpuClientConfig::default()
        };

        let connection_cache = ConnectionCache::<QuicPool, QuicConnectionManager, QuicConfig>::new(
            "ironcrab-tpu",
            solana_connection_cache::connection_cache::DEFAULT_CONNECTION_POOL_SIZE,
            solana_quic_client::QuicConfig::default(),
        )
        .map_err(|e| TpuError::ReconnectFailed(format!("{:?}", e)))?;

        let new_client = TpuClient::new_with_connection_cache(
            Arc::clone(&self.rpc_client),
            &self.ws_url,
            tpu_config,
            Arc::new(connection_cache),
        )
        .map_err(|e| TpuError::ReconnectFailed(format!("{:?}", e)))?;

        let mut guard = self.tpu_client.write().await;
        *guard = Some(new_client);

        self.consecutive_failures
            .store(0, std::sync::atomic::Ordering::Relaxed);

        info!("TPU client reconnected successfully");
        Ok(())
    }
}

// ============================================================================
// Windows stub implementation
// ============================================================================

#[cfg(windows)]
pub struct TpuSubmitter {
    _marker: std::marker::PhantomData<()>,
}

#[cfg(windows)]
impl TpuSubmitter {
    /// Create a new TPU submitter (stub on Windows).
    pub async fn new(
        _rpc_client: Arc<solana_client::rpc_client::RpcClient>,
        _ws_url: &str,
        _config: TpuSubmitterConfig,
    ) -> Result<Self> {
        warn!("TPU Direct is not supported on Windows, will use RPC fallback");
        Err(anyhow::anyhow!("TPU not supported on Windows"))
    }

    pub async fn send_transaction(&self, _tx: &Transaction) -> Result<Signature, TpuError> {
        Err(TpuError::NotSupported)
    }

    pub async fn is_ready(&self) -> bool {
        false
    }

    pub fn consecutive_failures(&self) -> u32 {
        0
    }

    pub fn should_reconnect(&self, _threshold: u32) -> bool {
        false
    }

    pub async fn reconnect(&self) -> Result<(), TpuError> {
        Err(TpuError::NotSupported)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tpu_submitter_config_defaults() {
        let config = TpuSubmitterConfig::default();
        assert_eq!(config.fanout_slots, 2);
        assert_eq!(config.leader_forward_count, 4);
    }

    #[test]
    fn test_tpu_error_display() {
        let err = TpuError::NotInitialized;
        assert_eq!(err.to_string(), "TPU client not initialized");

        let err = TpuError::NotSupported;
        assert_eq!(err.to_string(), "TPU not supported on this platform");

        let err = TpuError::SendFailed("test error".into());
        assert_eq!(err.to_string(), "Failed to send transaction: test error");
    }
}
