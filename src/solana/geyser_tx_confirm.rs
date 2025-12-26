//! Geyser-based transaction confirmation
//!
//! Instead of polling RPC for transaction status, this module uses Geyser gRPC
//! to receive real-time notifications when transactions are confirmed.
//! This is much more efficient and reduces RPC load.

use anyhow::Result;
use parking_lot::RwLock;
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::oneshot;
use tracing::{debug, info, warn};

/// Pending transaction waiting for confirmation
struct PendingTx {
    /// Channel to notify when confirmed
    notify: oneshot::Sender<TxConfirmationResult>,
    /// When this TX was submitted (for timeout)
    submitted_at: std::time::Instant,
    /// Optional: mint address for logging
    mint: Option<Pubkey>,
}

/// Pending ATA balance check - waits for tokens to arrive
struct PendingAtaBalance {
    /// Channel to notify when balance > 0
    notify: oneshot::Sender<AtaBalanceResult>,
    /// When this was registered (for timeout)
    registered_at: std::time::Instant,
    /// The mint (for logging)
    mint: Pubkey,
    /// Expected minimum balance (usually 0 = any tokens)
    min_balance: u64,
}

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

/// Geyser-based transaction confirmation tracker
pub struct GeyserTxConfirm {
    /// Map of signature -> pending confirmation
    pending_txs: Arc<RwLock<HashMap<String, PendingTx>>>,
    /// Map of ATA address -> pending balance confirmation
    pending_atas: Arc<RwLock<HashMap<Pubkey, PendingAtaBalance>>>,
    /// Timeout for confirmations (default 30s)
    timeout_secs: u64,
}

impl GeyserTxConfirm {
    /// Create new tracker
    pub fn new(timeout_secs: u64) -> Self {
        Self {
            pending_txs: Arc::new(RwLock::new(HashMap::new())),
            pending_atas: Arc::new(RwLock::new(HashMap::new())),
            timeout_secs,
        }
    }

    /// Register a transaction to wait for confirmation
    /// Returns a receiver that will get the result when confirmed
    pub fn register_tx(
        &self,
        signature: String,
        mint: Option<Pubkey>,
    ) -> oneshot::Receiver<TxConfirmationResult> {
        let (tx, rx) = oneshot::channel();

        let pending_tx = PendingTx {
            notify: tx,
            submitted_at: std::time::Instant::now(),
            mint,
        };

        self.pending_txs
            .write()
            .insert(signature.clone(), pending_tx);
        debug!(sig=%signature, "geyser_tx_confirm: registered TX for confirmation");

        rx
    }

    /// Register an ATA to wait for token balance
    /// This is used instead of polling RPC to check if tokens arrived
    pub fn register_ata(&self, ata: Pubkey) -> oneshot::Receiver<AtaBalanceResult> {
        let (tx, rx) = oneshot::channel();

        let pending = PendingAtaBalance {
            notify: tx,
            registered_at: std::time::Instant::now(),
            mint: ata,      // Use ATA as identifier (mint not needed for simplicity)
            min_balance: 1, // Any tokens > 0
        };

        self.pending_atas.write().insert(ata, pending);
        info!(ata=%ata, "geyser_tx_confirm: registered ATA for balance confirmation");

        rx
    }

    /// Called when a transaction is seen in Geyser stream
    /// This should be called from the Geyser listener when processing transactions
    pub fn on_transaction(&self, signature: &str, slot: u64) {
        if let Some(pending) = self.pending_txs.write().remove(signature) {
            let elapsed = pending.submitted_at.elapsed();

            info!(
                sig=%signature,
                slot=slot,
                elapsed_ms=elapsed.as_millis(),
                mint=?pending.mint,
                "geyser_tx_confirm: TX confirmed via Geyser"
            );

            // Notify the waiter
            let _ = pending.notify.send(TxConfirmationResult {
                signature: signature.to_string(),
                slot,
                confirmed: true,
            });
        }
    }

    /// Called when an account update is received via Geyser
    /// Checks if this is a watched ATA with tokens
    pub fn on_account_update(&self, pubkey: &Pubkey, data: &[u8], slot: u64) {
        // Check if we're waiting for this ATA
        let pending = {
            let pending_atas = self.pending_atas.read();
            if !pending_atas.contains_key(pubkey) {
                return;
            }
            drop(pending_atas);
            self.pending_atas.write().remove(pubkey)
        };

        if let Some(pending) = pending {
            // Parse SPL Token account balance (offset 64, 8 bytes)
            let balance = if data.len() >= 72 {
                u64::from_le_bytes(data[64..72].try_into().unwrap_or([0; 8]))
            } else {
                0
            };

            if balance >= pending.min_balance {
                let elapsed = pending.registered_at.elapsed();

                info!(
                    ata=%pubkey,
                    balance=balance,
                    slot=slot,
                    elapsed_ms=elapsed.as_millis(),
                    "geyser_tx_confirm: ATA balance confirmed via Geyser!"
                );

                let _ = pending.notify.send(AtaBalanceResult::Received(balance));
            } else {
                // Balance still 0, re-register
                debug!(ata=%pubkey, balance=balance, "geyser_tx_confirm: ATA balance still 0, waiting...");
                self.pending_atas.write().insert(*pubkey, pending);
            }
        }
    }

    /// Clean up timed-out transactions and ATAs
    /// Should be called periodically (e.g., every 5 seconds)
    pub fn cleanup_timeouts(&self) {
        let timeout = std::time::Duration::from_secs(self.timeout_secs);

        // Cleanup TX timeouts
        {
            let mut pending = self.pending_txs.write();
            let timed_out: Vec<String> = pending
                .iter()
                .filter(|(_, v)| v.submitted_at.elapsed() > timeout)
                .map(|(k, _)| k.clone())
                .collect();

            for sig in timed_out {
                if let Some(pending_tx) = pending.remove(&sig) {
                    warn!(
                        sig=%sig,
                        timeout_secs=self.timeout_secs,
                        mint=?pending_tx.mint,
                        "geyser_tx_confirm: TX confirmation timed out"
                    );

                    let _ = pending_tx.notify.send(TxConfirmationResult {
                        signature: sig,
                        slot: 0,
                        confirmed: false,
                    });
                }
            }
        }

        // Cleanup ATA timeouts
        {
            let mut pending = self.pending_atas.write();
            let timed_out: Vec<Pubkey> = pending
                .iter()
                .filter(|(_, v)| v.registered_at.elapsed() > timeout)
                .map(|(k, _)| *k)
                .collect();

            for ata in timed_out {
                if let Some(pending_ata) = pending.remove(&ata) {
                    warn!(
                        ata=%ata,
                        timeout_secs=self.timeout_secs,
                        "geyser_tx_confirm: ATA balance confirmation timed out"
                    );

                    let _ = pending_ata.notify.send(AtaBalanceResult::Timeout);
                }
            }
        }
    }

    /// Get number of pending TX confirmations
    pub fn pending_tx_count(&self) -> usize {
        self.pending_txs.read().len()
    }

    /// Get number of pending ATA balance confirmations
    pub fn pending_ata_count(&self) -> usize {
        self.pending_atas.read().len()
    }

    /// Check if an ATA is being watched
    pub fn is_watching_ata(&self, ata: &Pubkey) -> bool {
        self.pending_atas.read().contains_key(ata)
    }
}

impl Default for GeyserTxConfirm {
    fn default() -> Self {
        Self::new(30)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_geyser_tx_confirm() {
        let tracker = GeyserTxConfirm::new(5);

        // Register a TX
        let sig = "test_signature_123".to_string();
        let rx = tracker.register_tx(sig.clone(), None);

        assert_eq!(tracker.pending_tx_count(), 1);

        // Simulate Geyser notification
        tracker.on_transaction(&sig, 12345);

        // Should receive confirmation
        let result = rx.await.unwrap();
        assert!(result.confirmed);
        assert_eq!(result.slot, 12345);
        assert_eq!(tracker.pending_tx_count(), 0);
    }

    #[tokio::test]
    async fn test_ata_balance_confirm() {
        let tracker = GeyserTxConfirm::new(5);

        let ata = Pubkey::new_unique();
        let rx = tracker.register_ata(ata);

        assert_eq!(tracker.pending_ata_count(), 1);

        // Simulate account update with balance
        // SPL Token account: ... [64..72] = balance
        let mut data = vec![0u8; 165]; // Standard SPL Token account size
        data[64..72].copy_from_slice(&1000u64.to_le_bytes());

        tracker.on_account_update(&ata, &data, 12345);

        // Should receive confirmation
        let result = rx.await.unwrap();
        match result {
            AtaBalanceResult::Received(balance) => assert_eq!(balance, 1000),
            _ => panic!("Expected Received result"),
        }
        assert_eq!(tracker.pending_ata_count(), 0);
    }

    #[tokio::test]
    async fn test_timeout() {
        let tracker = GeyserTxConfirm::new(0); // Immediate timeout

        let sig = "timeout_test".to_string();
        let rx = tracker.register_tx(sig.clone(), None);

        // Wait a bit then cleanup
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        tracker.cleanup_timeouts();

        // Should receive timeout result
        let result = rx.await.unwrap();
        assert!(!result.confirmed);
    }
}
