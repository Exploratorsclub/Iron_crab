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
//!
//! Leader Cache Health:
//! - The TPU client maintains a leader schedule cache updated via WebSocket
//! - If the WebSocket connection drops, the cache becomes stale
//! - This module provides health checks and automatic reconnection

use anyhow::Result;
use solana_sdk::{
    signature::Signature,
    transaction::{Transaction, VersionedTransaction},
};
use std::sync::Arc;

#[cfg(not(windows))]
use tracing::{debug, info, warn};

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
    #[error("Leader cache stale: cache_slot={0}, current_slot={1}")]
    LeaderCacheStale(u64, u64),
}

/// Configuration for the TPU submitter
#[derive(Debug, Clone)]
pub struct TpuSubmitterConfig {
    /// Number of leader slots to fan out to (send to current + next N leaders)
    pub fanout_slots: u64,
    /// Number of times to forward TX to leaders
    pub leader_forward_count: u64,
    /// Max slots the leader cache can be stale before triggering reconnect
    pub cache_stale_threshold: u64,
    /// Consecutive failures before suggesting reconnect
    pub reconnect_failure_threshold: u32,
}

impl Default for TpuSubmitterConfig {
    fn default() -> Self {
        Self {
            fanout_slots: 4,
            leader_forward_count: 4,
            cache_stale_threshold: 50,
            reconnect_failure_threshold: 3,
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
    /// Track last known good slot (for staleness detection)
    last_known_slot: Arc<std::sync::atomic::AtomicU64>,
    /// Track last reconnect time to avoid rapid reconnects
    last_reconnect_ms: Arc<std::sync::atomic::AtomicU64>,
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
        use solana_connection_cache::connection_cache::{ConnectionCache, NewConnectionConfig};
        use solana_quic_client::{QuicConfig, QuicConnectionManager, QuicPool};
        use solana_tpu_client::tpu_client::{TpuClient, TpuClientConfig};

        info!(
            ws_url = %ws_url,
            fanout_slots = config.fanout_slots,
            cache_stale_threshold = config.cache_stale_threshold,
            "Creating TPU client with QUIC"
        );

        let tpu_config = TpuClientConfig {
            fanout_slots: config.fanout_slots,
        };

        // Create QUIC connection manager (Solana 3.0 API)
        let quic_config = QuicConfig::new()?;
        let connection_manager = QuicConnectionManager::new_with_connection_config(quic_config);
        let connection_cache = ConnectionCache::<QuicPool, QuicConnectionManager, QuicConfig>::new(
            "ironcrab-tpu",
            connection_manager,
            solana_connection_cache::connection_cache::DEFAULT_CONNECTION_POOL_SIZE,
        )?;

        // Create TPU client with connection cache
        let tpu_client = TpuClient::new_with_connection_cache(
            Arc::clone(&rpc_client),
            ws_url,
            tpu_config,
            Arc::new(connection_cache),
        )
        .map_err(|e| anyhow::anyhow!("Failed to create TPU client: {:?}", e))?;

        // Get initial slot for staleness tracking
        let initial_slot = rpc_client.get_slot().unwrap_or(0);

        info!(
            initial_slot = initial_slot,
            "TPU client initialized successfully"
        );

        Ok(Self {
            tpu_client: Arc::new(tokio::sync::RwLock::new(Some(tpu_client))),
            rpc_client,
            ws_url: ws_url.to_string(),
            config,
            consecutive_failures: Arc::new(std::sync::atomic::AtomicU32::new(0)),
            last_known_slot: Arc::new(std::sync::atomic::AtomicU64::new(initial_slot)),
            last_reconnect_ms: Arc::new(std::sync::atomic::AtomicU64::new(0)),
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

    /// Send a versioned transaction (v0+ALT or legacy) via TPU Direct wire format.
    pub async fn send_versioned_transaction(
        &self,
        tx: &VersionedTransaction,
    ) -> Result<Signature, TpuError> {
        let guard = self.tpu_client.read().await;
        let client = guard.as_ref().ok_or(TpuError::NotInitialized)?;

        let signature = tx.signatures.first().copied().unwrap_or_default();
        let wire_transaction =
            bincode::serialize(tx).map_err(|e| TpuError::SendFailed(format!("serialize: {e}")))?;

        match client.try_send_wire_transaction(wire_transaction) {
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

    /// Check if the leader cache is stale by comparing current slot with last known slot.
    /// Returns (is_stale, current_slot, cached_slot) if stale.
    pub fn check_leader_cache_health(&self) -> Result<(), TpuError> {
        // Get current slot from RPC
        let current_slot = self.rpc_client.get_slot().unwrap_or(0);
        let cached_slot = self
            .last_known_slot
            .load(std::sync::atomic::Ordering::Relaxed);

        // Update last known slot
        self.last_known_slot
            .store(current_slot, std::sync::atomic::Ordering::Relaxed);

        // Check staleness
        if cached_slot > 0 && current_slot > cached_slot + self.config.cache_stale_threshold {
            crate::metrics::TPU_CACHE_STALE_TOTAL
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            warn!(
                current_slot = current_slot,
                cached_slot = cached_slot,
                threshold = self.config.cache_stale_threshold,
                "Leader cache is stale, WebSocket may have disconnected"
            );
            return Err(TpuError::LeaderCacheStale(cached_slot, current_slot));
        }

        debug!(
            current_slot = current_slot,
            cached_slot = cached_slot,
            "Leader cache health OK"
        );
        Ok(())
    }

    /// Check if enough time has passed since last reconnect to avoid rapid reconnects.
    /// Returns true if reconnect is allowed.
    pub fn can_reconnect(&self, min_interval_ms: u64) -> bool {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        let last_reconnect = self
            .last_reconnect_ms
            .load(std::sync::atomic::Ordering::Relaxed);

        now_ms.saturating_sub(last_reconnect) >= min_interval_ms
    }

    /// Reconnect the TPU client.
    pub async fn reconnect(&self) -> Result<(), TpuError> {
        use solana_connection_cache::connection_cache::{ConnectionCache, NewConnectionConfig};
        use solana_quic_client::{QuicConfig, QuicConnectionManager, QuicPool};
        use solana_tpu_client::tpu_client::{TpuClient, TpuClientConfig};

        // Record reconnect time
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        self.last_reconnect_ms
            .store(now_ms, std::sync::atomic::Ordering::Relaxed);

        info!("Reconnecting TPU client (WebSocket + QUIC)");

        let tpu_config = TpuClientConfig {
            fanout_slots: self.config.fanout_slots,
        };

        // Create QUIC connection manager (Solana 3.0 API)
        let quic_config =
            QuicConfig::new().map_err(|e| TpuError::ReconnectFailed(format!("{:?}", e)))?;
        let connection_manager = QuicConnectionManager::new_with_connection_config(quic_config);
        let connection_cache = ConnectionCache::<QuicPool, QuicConnectionManager, QuicConfig>::new(
            "ironcrab-tpu",
            connection_manager,
            solana_connection_cache::connection_cache::DEFAULT_CONNECTION_POOL_SIZE,
        )
        .map_err(|e| TpuError::ReconnectFailed(format!("{:?}", e)))?;

        let new_client = TpuClient::new_with_connection_cache(
            Arc::clone(&self.rpc_client),
            &self.ws_url,
            tpu_config,
            Arc::new(connection_cache),
        )
        .map_err(|e| TpuError::ReconnectFailed(format!("{:?}", e)))?;

        // Update slot tracking
        let current_slot = self.rpc_client.get_slot().unwrap_or(0);
        self.last_known_slot
            .store(current_slot, std::sync::atomic::Ordering::Relaxed);

        let mut guard = self.tpu_client.write().await;
        *guard = Some(new_client);

        self.consecutive_failures
            .store(0, std::sync::atomic::Ordering::Relaxed);

        crate::metrics::TPU_RECONNECT_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        info!(
            current_slot = current_slot,
            "TPU client reconnected successfully"
        );
        Ok(())
    }
}

// ============================================================================
// Windows stub implementation
// ============================================================================

#[cfg(windows)]
use tracing::warn;

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

    pub async fn send_versioned_transaction(
        &self,
        _tx: &VersionedTransaction,
    ) -> Result<Signature, TpuError> {
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

    pub fn check_leader_cache_health(&self) -> Result<(), TpuError> {
        Err(TpuError::NotSupported)
    }

    pub fn can_reconnect(&self, _min_interval_ms: u64) -> bool {
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
        assert_eq!(config.fanout_slots, 4);
        assert_eq!(config.leader_forward_count, 4);
        assert_eq!(config.cache_stale_threshold, 50);
        assert_eq!(config.reconnect_failure_threshold, 3);
    }

    #[test]
    fn test_tpu_error_display() {
        let err = TpuError::NotInitialized;
        assert_eq!(err.to_string(), "TPU client not initialized");

        let err = TpuError::NotSupported;
        assert_eq!(err.to_string(), "TPU not supported on this platform");

        let err = TpuError::SendFailed("test error".into());
        assert_eq!(err.to_string(), "Failed to send transaction: test error");

        let err = TpuError::LeaderCacheStale(100, 200);
        assert_eq!(
            err.to_string(),
            "Leader cache stale: cache_slot=100, current_slot=200"
        );
    }
}
