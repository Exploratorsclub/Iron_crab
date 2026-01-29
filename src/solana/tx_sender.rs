//! Unified Transaction Sender with automatic fallback.
//!
//! This module provides a unified interface for sending transactions
//! with automatic fallback between different submission methods:
//!
//! 1. **TPU Direct** - Fastest (~50-100ms), sends directly to slot leaders via QUIC
//! 2. **Jito Bundles** - MEV protection, atomic execution for arbitrage
//! 3. **RPC sendTransaction** - Standard fallback (~200-400ms)
//!
//! The fallback chain is configurable and automatically handles:
//! - Method-specific timeouts
//! - Retries per method
//! - Graceful degradation
//! - Metrics for monitoring

use crate::config::TxSubmissionCfg;
use crate::solana::jito::JitoClient;
use crate::solana::tpu_client::{TpuError, TpuSubmitter, TpuSubmitterConfig};
use anyhow::Result;
use solana_client::rpc_client::RpcClient;
use solana_commitment_config::CommitmentConfig;
use solana_sdk::{signature::Signature, transaction::Transaction};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use tracing::{debug, error, info, warn};

/// Send method used for a transaction
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendMethod {
    /// Sent via TPU Direct (QUIC to leader)
    TpuDirect,
    /// Sent via Jito bundle
    JitoBundle,
    /// Sent via RPC sendTransaction
    Rpc,
}

impl std::fmt::Display for SendMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SendMethod::TpuDirect => write!(f, "tpu"),
            SendMethod::JitoBundle => write!(f, "jito"),
            SendMethod::Rpc => write!(f, "rpc"),
        }
    }
}

/// Error types for transaction sending
#[derive(Debug, thiserror::Error)]
pub enum SendError {
    #[error("Method not configured: {0}")]
    MethodNotConfigured(&'static str),
    #[error("Timeout waiting for {0}")]
    Timeout(&'static str),
    #[error("TPU error: {0}")]
    TpuError(String),
    #[error("Jito error: {0}")]
    JitoError(String),
    #[error("RPC error: {0}")]
    RpcError(String),
    #[error("No send method available")]
    NoMethodAvailable,
    #[error("All methods failed")]
    AllMethodsFailed,
    #[error("Bundle required but Jito not configured")]
    BundleRequiredButNoJito,
}

/// Result of a successful send
#[derive(Debug, Clone)]
pub struct SendResult {
    /// Transaction signature
    pub signature: Signature,
    /// Method that succeeded
    pub method: SendMethod,
    /// Bundle ID if sent via Jito
    pub bundle_id: Option<String>,
}

/// Unified transaction sender with fallback chain.
pub struct TxSender {
    /// TPU Direct submitter (optional)
    tpu: Option<TpuSubmitter>,
    /// Jito bundle client (optional)
    jito: Option<Arc<JitoClient>>,
    /// RPC client for fallback
    rpc: Arc<RpcClient>,
    /// Configuration
    config: TxSubmissionCfg,
}

impl TxSender {
    /// Create a new TxSender.
    ///
    /// # Arguments
    /// * `rpc_client` - RPC client for leader schedule and fallback sends
    /// * `ws_url` - WebSocket URL for TPU client
    /// * `config` - TX submission configuration
    /// * `jito_client` - Optional Jito client for bundle submission
    pub async fn new(
        rpc_client: Arc<RpcClient>,
        ws_url: &str,
        config: TxSubmissionCfg,
        jito_client: Option<Arc<JitoClient>>,
    ) -> Result<Self> {
        let tpu = if config.tpu_enabled {
            info!("Initializing TPU Direct client");
            match TpuSubmitter::new(
                Arc::clone(&rpc_client),
                ws_url,
                TpuSubmitterConfig {
                    fanout_slots: config.tpu_fanout_slots,
                    leader_forward_count: config.tpu_leader_forward_count,
                    cache_stale_threshold: config.tpu_cache_stale_threshold,
                    reconnect_failure_threshold: config.tpu_reconnect_failure_threshold,
                },
            )
            .await
            {
                Ok(submitter) => {
                    info!(
                        fanout_slots = config.tpu_fanout_slots,
                        cache_stale_threshold = config.tpu_cache_stale_threshold,
                        "TPU Direct client initialized successfully"
                    );
                    Some(submitter)
                }
                Err(e) => {
                    warn!(error = %e, "Failed to initialize TPU client, will use fallback methods");
                    None
                }
            }
        } else {
            info!("TPU Direct disabled in config");
            None
        };

        let fallback_desc = if config.parallel_send && tpu.is_some() {
            "TPU+RPC parallel"
        } else {
            &config.primary_method
        };

        info!(
            primary = %fallback_desc,
            fallback = ?config.fallback_chain,
            tpu_enabled = config.tpu_enabled,
            "TxSender initialized with fallback chain (TPU → RPC)"
        );

        Ok(Self {
            tpu,
            jito: jito_client,
            rpc: rpc_client,
            config,
        })
    }

    /// Create a TxSender with only RPC (for testing or simple use cases)
    pub fn rpc_only(rpc_client: Arc<RpcClient>) -> Self {
        Self {
            tpu: None,
            jito: None,
            rpc: rpc_client,
            config: TxSubmissionCfg {
                primary_method: "rpc".into(),
                fallback_chain: vec![],
                tpu_enabled: false,
                parallel_send: false, // No TPU, so no parallel send
                ..Default::default()
            },
        }
    }

    /// Send a transaction with automatic fallback.
    ///
    /// Tries methods in order: primary_method → fallback_chain
    /// For bundle-required transactions, only Jito is used.
    /// If parallel_send is enabled and TPU is available, sends via TPU + RPC simultaneously.
    ///
    /// # Arguments
    /// * `tx` - Signed transaction to send
    /// * `require_bundle` - If true, only Jito bundle submission is allowed
    pub async fn send_with_fallback(
        &self,
        tx: &Transaction,
        require_bundle: bool,
    ) -> Result<SendResult, SendError> {
        // For bundle-required transactions (e.g., arbitrage), use Jito only
        if require_bundle && (self.config.skip_tpu_for_bundles || self.jito.is_none()) {
            return self
                .send_via_jito(tx)
                .await
                .map(|(sig, bundle_id)| SendResult {
                    signature: sig,
                    method: SendMethod::JitoBundle,
                    bundle_id: Some(bundle_id),
                });
        }

        // Parallel send: TPU + RPC simultaneously for maximum reliability
        // Solana deduplicates by signature, so only one TX lands and fees are paid once.
        if self.config.parallel_send && self.tpu.is_some() {
            return self.send_parallel(tx).await;
        }

        // Sequential fallback chain (legacy behavior)
        self.send_sequential(tx).await
    }

    /// Send via TPU and RPC in parallel. First success wins.
    /// Both methods send the same signed TX, so Solana deduplicates by signature.
    /// This ensures maximum reliability without paying double fees.
    async fn send_parallel(&self, tx: &Transaction) -> Result<SendResult, SendError> {
        debug!("Parallel send: TPU + RPC simultaneously");

        let tx_clone = tx.clone();
        let tpu_future = async {
            match self.send_via_tpu(tx).await {
                Ok(sig) => Some((sig, SendMethod::TpuDirect)),
                Err(e) => {
                    warn!(error = %e, "Parallel send: TPU failed");
                    None
                }
            }
        };

        let rpc_future = async {
            match self.send_via_rpc(&tx_clone).await {
                Ok(sig) => Some((sig, SendMethod::Rpc)),
                Err(e) => {
                    warn!(error = %e, "Parallel send: RPC failed");
                    None
                }
            }
        };

        // Run both in parallel, collect results
        let (tpu_result, rpc_result) = tokio::join!(tpu_future, rpc_future);

        // Prefer TPU result if available (it was sent first/faster typically)
        // But either result is valid since it's the same TX
        match (tpu_result, rpc_result) {
            (Some((sig, method)), _) => {
                info!(
                    signature = %sig,
                    method = %method,
                    parallel = true,
                    "TX sent via parallel send (TPU succeeded)"
                );
                Ok(SendResult {
                    signature: sig,
                    method,
                    bundle_id: None,
                })
            }
            (None, Some((sig, method))) => {
                info!(
                    signature = %sig,
                    method = %method,
                    parallel = true,
                    "TX sent via parallel send (RPC succeeded, TPU failed)"
                );
                Ok(SendResult {
                    signature: sig,
                    method,
                    bundle_id: None,
                })
            }
            (None, None) => {
                error!("Parallel send: Both TPU and RPC failed");
                Err(SendError::AllMethodsFailed)
            }
        }
    }

    /// Sequential send with fallback chain (legacy behavior)
    async fn send_sequential(&self, tx: &Transaction) -> Result<SendResult, SendError> {
        // Build method chain: primary + fallbacks
        let methods: Vec<&str> = std::iter::once(self.config.primary_method.as_str())
            .chain(self.config.fallback_chain.iter().map(|s| s.as_str()))
            .collect();

        debug!(methods = ?methods, "TX send method chain (sequential)");

        let mut last_error: Option<SendError> = None;

        for method in methods {
            let result = match method {
                "tpu" => self.send_via_tpu(tx).await.map(|sig| SendResult {
                    signature: sig,
                    method: SendMethod::TpuDirect,
                    bundle_id: None,
                }),
                "jito" => self
                    .send_via_jito(tx)
                    .await
                    .map(|(sig, bundle_id)| SendResult {
                        signature: sig,
                        method: SendMethod::JitoBundle,
                        bundle_id: Some(bundle_id),
                    }),
                "rpc" => self.send_via_rpc(tx).await.map(|sig| SendResult {
                    signature: sig,
                    method: SendMethod::Rpc,
                    bundle_id: None,
                }),
                _ => {
                    warn!(method = method, "Unknown send method, skipping");
                    continue;
                }
            };

            match result {
                Ok(send_result) => {
                    info!(
                        signature = %send_result.signature,
                        method = %send_result.method,
                        "TX sent successfully"
                    );
                    return Ok(send_result);
                }
                Err(e) => {
                    warn!(
                        method = method,
                        error = %e,
                        "Send method failed, trying next"
                    );
                    last_error = Some(e);
                }
            }
        }

        Err(last_error.unwrap_or(SendError::NoMethodAvailable))
    }

    /// Send via TPU Direct
    async fn send_via_tpu(&self, tx: &Transaction) -> Result<Signature, SendError> {
        let tpu = self
            .tpu
            .as_ref()
            .ok_or(SendError::MethodNotConfigured("tpu"))?;

        // Health check: check leader cache staleness and reconnect if needed
        if let Err(e) = tpu.check_leader_cache_health() {
            warn!(error = %e, "Leader cache stale, attempting reconnect");
            // Only reconnect if enough time has passed (30s minimum between reconnects)
            if tpu.can_reconnect(30_000) {
                if let Err(reconnect_err) = tpu.reconnect().await {
                    warn!(error = %reconnect_err, "TPU reconnect failed, continuing with stale cache");
                }
            }
        }

        // Check if we should reconnect due to consecutive failures
        if tpu.should_reconnect(self.config.tpu_reconnect_failure_threshold) {
            warn!(
                consecutive_failures = tpu.consecutive_failures(),
                threshold = self.config.tpu_reconnect_failure_threshold,
                "TPU client has many failures, attempting reconnect"
            );
            if tpu.can_reconnect(30_000) {
                if let Err(e) = tpu.reconnect().await {
                    warn!(error = %e, "TPU reconnect failed");
                }
            }
        }

        let timeout_duration = Duration::from_millis(self.config.method_timeout_ms);

        for attempt in 0..self.config.retries_per_method {
            let result = timeout(timeout_duration, tpu.send_transaction(tx)).await;

            match result {
                Ok(Ok(signature)) => {
                    return Ok(signature);
                }
                Ok(Err(e)) => {
                    if attempt + 1 < self.config.retries_per_method {
                        debug!(attempt = attempt, error = %e, "TPU send failed, retrying");
                    } else {
                        return Err(SendError::TpuError(e.to_string()));
                    }
                }
                Err(_) => {
                    if attempt + 1 < self.config.retries_per_method {
                        debug!(attempt = attempt, "TPU send timed out, retrying");
                    } else {
                        return Err(SendError::Timeout("tpu"));
                    }
                }
            }
        }

        Err(SendError::TpuError("Max retries exceeded".into()))
    }

    /// Send via Jito bundle
    async fn send_via_jito(&self, tx: &Transaction) -> Result<(Signature, String), SendError> {
        let jito = self
            .jito
            .as_ref()
            .ok_or(SendError::MethodNotConfigured("jito"))?;

        let signature = tx.signatures.first().copied().unwrap_or_default();

        let timeout_duration = Duration::from_millis(self.config.method_timeout_ms);

        for attempt in 0..self.config.retries_per_method {
            let result =
                timeout(timeout_duration, jito.send_bundle(std::slice::from_ref(tx))).await;

            match result {
                Ok(Ok(bundle_id)) => {
                    info!(bundle_id = %bundle_id, "Jito bundle submitted");
                    return Ok((signature, bundle_id));
                }
                Ok(Err(e)) => {
                    if attempt + 1 < self.config.retries_per_method {
                        debug!(attempt = attempt, error = %e, "Jito send failed, retrying");
                    } else {
                        return Err(SendError::JitoError(e.to_string()));
                    }
                }
                Err(_) => {
                    if attempt + 1 < self.config.retries_per_method {
                        debug!(attempt = attempt, "Jito send timed out, retrying");
                    } else {
                        return Err(SendError::Timeout("jito"));
                    }
                }
            }
        }

        Err(SendError::JitoError("Max retries exceeded".into()))
    }

    /// Send via RPC sendTransaction
    async fn send_via_rpc(&self, tx: &Transaction) -> Result<Signature, SendError> {
        let timeout_duration = Duration::from_millis(self.config.method_timeout_ms);

        for attempt in 0..self.config.retries_per_method {
            let rpc = Arc::clone(&self.rpc);
            let tx_clone = tx.clone();

            // Run RPC call in blocking task (RpcClient is sync)
            let result = timeout(
                timeout_duration,
                tokio::task::spawn_blocking(move || {
                    rpc.send_transaction_with_config(
                        &tx_clone,
                        solana_client::rpc_config::RpcSendTransactionConfig {
                            skip_preflight: true,
                            preflight_commitment: Some(CommitmentConfig::confirmed().commitment),
                            max_retries: Some(0), // We handle retries ourselves
                            ..Default::default()
                        },
                    )
                }),
            )
            .await;

            match result {
                Ok(Ok(Ok(signature))) => {
                    return Ok(signature);
                }
                Ok(Ok(Err(e))) => {
                    if attempt + 1 < self.config.retries_per_method {
                        debug!(attempt = attempt, error = %e, "RPC send failed, retrying");
                    } else {
                        return Err(SendError::RpcError(e.to_string()));
                    }
                }
                Ok(Err(e)) => {
                    return Err(SendError::RpcError(format!("Task join error: {}", e)));
                }
                Err(_) => {
                    if attempt + 1 < self.config.retries_per_method {
                        debug!(attempt = attempt, "RPC send timed out, retrying");
                    } else {
                        return Err(SendError::Timeout("rpc"));
                    }
                }
            }
        }

        Err(SendError::RpcError("Max retries exceeded".into()))
    }

    /// Check if TPU Direct is available and ready
    pub async fn is_tpu_ready(&self) -> bool {
        if let Some(tpu) = &self.tpu {
            tpu.is_ready().await
        } else {
            false
        }
    }

    /// Check if Jito is configured
    pub fn has_jito(&self) -> bool {
        self.jito.is_some()
    }

    /// Get the primary send method
    pub fn primary_method(&self) -> &str {
        &self.config.primary_method
    }

    /// Trigger TPU reconnection
    pub async fn reconnect_tpu(&self) -> Result<(), TpuError> {
        if let Some(tpu) = &self.tpu {
            tpu.reconnect().await
        } else {
            Err(TpuError::NotInitialized)
        }
    }

    /// Check TPU leader cache health. Returns Ok if healthy, Err if stale.
    pub fn check_tpu_health(&self) -> Result<(), TpuError> {
        if let Some(tpu) = &self.tpu {
            tpu.check_leader_cache_health()
        } else {
            Err(TpuError::NotInitialized)
        }
    }

    /// Spawn a background task that periodically checks TPU health and reconnects if needed.
    /// Returns a JoinHandle that can be used to await or abort the task.
    pub fn spawn_health_check_task(
        self: &Arc<Self>,
        interval_secs: u64,
    ) -> tokio::task::JoinHandle<()> {
        let sender = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                interval.tick().await;

                if let Some(tpu) = &sender.tpu {
                    // Check leader cache health
                    if let Err(e) = tpu.check_leader_cache_health() {
                        warn!(error = %e, "TPU health check: leader cache stale");
                        if tpu.can_reconnect(30_000) {
                            info!("TPU health check: triggering reconnect");
                            if let Err(reconnect_err) = tpu.reconnect().await {
                                error!(error = %reconnect_err, "TPU health check: reconnect failed");
                            } else {
                                info!("TPU health check: reconnected successfully");
                            }
                        }
                    } else {
                        debug!("TPU health check: OK");
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_send_method_display() {
        assert_eq!(SendMethod::TpuDirect.to_string(), "tpu");
        assert_eq!(SendMethod::JitoBundle.to_string(), "jito");
        assert_eq!(SendMethod::Rpc.to_string(), "rpc");
    }

    #[test]
    fn test_send_error_display() {
        let err = SendError::MethodNotConfigured("tpu");
        assert!(err.to_string().contains("tpu"));

        let err = SendError::Timeout("jito");
        assert!(err.to_string().contains("jito"));
    }
}
