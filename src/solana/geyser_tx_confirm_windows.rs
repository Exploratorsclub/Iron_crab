//! Windows stub for Geyser-based transaction/ATA confirmation.
//!
//! The real implementation depends on `yellowstone-grpc-*`, which currently
//! pulls in `protobuf-src` and fails to build on Windows in this repo.
//!
//! On Windows we expose the same public API surface used by the codebase,
//! but always report that Geyser is disabled.

use solana_sdk::pubkey::Pubkey;
use tokio::sync::oneshot;

/// Result of transaction confirmation
#[derive(Debug, Clone)]
pub struct TxConfirmationResult {
    pub signature: String,
    pub slot: u64,
    pub confirmed: bool,
}

/// Result of ATA balance check
#[derive(Debug, Clone)]
pub enum AtaBalanceResult {
    /// Tokens received with balance
    Received(u64),
    /// Timeout waiting for tokens
    Timeout,
    /// Error during confirmation
    Error(String),
}

/// Stub tracker that always reports Geyser as disabled.
#[derive(Debug, Default)]
pub struct GeyserTxConfirm {
    timeout_secs: u64,
}

impl GeyserTxConfirm {
    /// Create new tracker (RPC polling fallback in non-Windows builds).
    pub fn new(timeout_secs: u64) -> Self {
        Self { timeout_secs }
    }

    /// Create new tracker with Geyser support.
    ///
    /// On Windows this is a stub and behaves like `new()`.
    pub fn with_geyser(timeout_secs: u64, _geyser_url: String) -> Self {
        Self::new(timeout_secs)
    }

    /// Returns whether Geyser is enabled.
    pub fn is_geyser_enabled(&self) -> bool {
        false
    }

    /// Register an ATA to watch.
    ///
    /// On Windows this immediately returns an error result.
    pub fn register_ata(&self, _ata: Pubkey) -> oneshot::Receiver<AtaBalanceResult> {
        let (tx, rx) = oneshot::channel();
        let _ = tx.send(AtaBalanceResult::Error(
            "Geyser is not supported on Windows in this build".to_string(),
        ));
        rx
    }

    /// Expose timeout for callers that log it.
    pub fn timeout_secs(&self) -> u64 {
        self.timeout_secs
    }
}
