//! Geyser-based transaction confirmation
//!
//! Instead of polling RPC for transaction status, this module uses Geyser gRPC
//! to receive real-time notifications when transactions are confirmed.
//! This is much more efficient and reduces RPC load.
//!
//! ## Efficient Design
//! - Uses a **separate lightweight Geyser stream** just for ATA watching
//! - Subscribes only to **specific ATA addresses** (not entire Token Program!)
//! - HashMap lookup is O(1) - even 100k updates/sec are handled efficiently
//! - Auto-unsubscribes when ATA confirmation received or timed out

use futures::StreamExt;
use parking_lot::RwLock;
use solana_sdk::pubkey::Pubkey;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info, warn};
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::prelude::{
    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
    SubscribeRequestFilterAccounts,
};

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

/// Command to the background Geyser watcher
enum WatcherCommand {
    /// Add an ATA to watch
    Watch(Pubkey),
    /// Remove an ATA from watch list
    Unwatch(Pubkey),
    /// Shutdown the watcher
    Shutdown,
}

/// Geyser-based transaction confirmation tracker
///
/// ## Efficiency Design
/// - Maintains a **separate Geyser stream** dedicated to ATA watching
/// - Only subscribes to specific ATA addresses we're interested in
/// - HashMap lookup for incoming updates is O(1)
/// - Automatically resubscribes when watch list changes
pub struct GeyserTxConfirm {
    /// Map of signature -> pending confirmation
    pending_txs: Arc<RwLock<HashMap<String, PendingTx>>>,
    /// Map of ATA address -> pending balance confirmation
    pending_atas: Arc<RwLock<HashMap<Pubkey, PendingAtaBalance>>>,
    /// Currently watched ATAs (for quick membership check)
    watched_atas: Arc<RwLock<HashSet<Pubkey>>>,
    /// Timeout for confirmations (default 30s)
    timeout_secs: u64,
    /// Channel to send commands to the background watcher
    watcher_tx: Option<mpsc::Sender<WatcherCommand>>,
    /// Geyser endpoint URL
    geyser_url: Option<String>,
}

impl GeyserTxConfirm {
    /// Create new tracker (without Geyser - uses RPC polling fallback)
    pub fn new(timeout_secs: u64) -> Self {
        Self {
            pending_txs: Arc::new(RwLock::new(HashMap::new())),
            pending_atas: Arc::new(RwLock::new(HashMap::new())),
            watched_atas: Arc::new(RwLock::new(HashSet::new())),
            timeout_secs,
            watcher_tx: None,
            geyser_url: None,
        }
    }

    /// Create new tracker with Geyser support for efficient ATA watching
    ///
    /// This spawns a background task that maintains a Geyser subscription
    /// for all watched ATAs. Much more efficient than RPC polling!
    pub fn with_geyser(timeout_secs: u64, geyser_url: String) -> Self {
        let (watcher_tx, watcher_rx) = mpsc::channel(100);

        let pending_atas = Arc::new(RwLock::new(HashMap::new()));
        let watched_atas = Arc::new(RwLock::new(HashSet::new()));

        // Spawn background watcher
        let pending_atas_clone = pending_atas.clone();
        let watched_atas_clone = watched_atas.clone();
        let geyser_url_clone = geyser_url.clone();

        tokio::spawn(async move {
            Self::run_ata_watcher(
                geyser_url_clone,
                watcher_rx,
                pending_atas_clone,
                watched_atas_clone,
            )
            .await;
        });

        Self {
            pending_txs: Arc::new(RwLock::new(HashMap::new())),
            pending_atas,
            watched_atas,
            timeout_secs,
            watcher_tx: Some(watcher_tx),
            geyser_url: Some(geyser_url),
        }
    }

    /// Background task that maintains Geyser subscription for ATAs
    ///
    /// ## Efficiency
    /// - Only subscribes to specific ATA addresses (not entire Token Program!)
    /// - Resubscribes when watch list changes
    /// - HashMap lookup for incoming updates is O(1)
    async fn run_ata_watcher(
        geyser_url: String,
        mut cmd_rx: mpsc::Receiver<WatcherCommand>,
        pending_atas: Arc<RwLock<HashMap<Pubkey, PendingAtaBalance>>>,
        watched_atas: Arc<RwLock<HashSet<Pubkey>>>,
    ) {
        info!("geyser_tx_confirm: ATA watcher starting");

        loop {
            // Get current watch list
            let current_atas: Vec<Pubkey> = watched_atas.read().iter().copied().collect();

            if current_atas.is_empty() {
                // No ATAs to watch - wait for commands
                tokio::select! {
                    cmd = cmd_rx.recv() => {
                        match cmd {
                            Some(WatcherCommand::Watch(ata)) => {
                                watched_atas.write().insert(ata);
                                debug!(ata=%ata, "geyser_tx_confirm: added ATA to watch list");
                            }
                            Some(WatcherCommand::Unwatch(ata)) => {
                                watched_atas.write().remove(&ata);
                            }
                            Some(WatcherCommand::Shutdown) | None => {
                                info!("geyser_tx_confirm: ATA watcher shutting down");
                                return;
                            }
                        }
                    }
                    _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {}
                }
                continue;
            }

            // Connect to Geyser and subscribe to current ATAs
            let result = Self::subscribe_and_watch(
                &geyser_url,
                &current_atas,
                &mut cmd_rx,
                &pending_atas,
                &watched_atas,
            )
            .await;

            if let Err(e) = result {
                warn!(error=%e, "geyser_tx_confirm: Geyser subscription failed, retrying in 1s");
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        }
    }

    /// Subscribe to specific ATAs and process updates
    async fn subscribe_and_watch(
        geyser_url: &str,
        atas: &[Pubkey],
        cmd_rx: &mut mpsc::Receiver<WatcherCommand>,
        pending_atas: &Arc<RwLock<HashMap<Pubkey, PendingAtaBalance>>>,
        watched_atas: &Arc<RwLock<HashSet<Pubkey>>>,
    ) -> anyhow::Result<()> {
        // Connect to Geyser
        let mut client = GeyserGrpcClient::build_from_shared(geyser_url.to_string())
            .map_err(|e| anyhow::anyhow!("Failed to build Geyser client: {}", e))?
            .connect()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to connect to Geyser: {}", e))?;

        // Build subscription for specific ATA addresses
        // This is the KEY efficiency: we only subscribe to the exact accounts we care about!
        let mut accounts_filter = HashMap::new();
        accounts_filter.insert(
            "ata_watch".to_string(),
            SubscribeRequestFilterAccounts {
                account: atas.iter().map(|a| a.to_string()).collect(),
                owner: vec![], // Don't filter by owner - we want specific accounts
                filters: vec![],
                nonempty_txn_signature: None,
            },
        );

        let request = SubscribeRequest {
            accounts: accounts_filter,
            slots: HashMap::new(),
            transactions: HashMap::new(),
            transactions_status: HashMap::new(),
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            entry: HashMap::new(),
            commitment: Some(CommitmentLevel::Confirmed as i32),
            accounts_data_slice: vec![],
            ping: None,
            from_slot: None,
        };

        let mut stream = client
            .subscribe_once(request)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to subscribe: {}", e))?;

        info!(
            ata_count = atas.len(),
            "geyser_tx_confirm: subscribed to ATA updates"
        );

        // Process updates until watch list changes or shutdown
        loop {
            tokio::select! {
                // Check for commands (add/remove ATA, shutdown)
                cmd = cmd_rx.recv() => {
                    match cmd {
                        Some(WatcherCommand::Watch(ata)) => {
                            watched_atas.write().insert(ata);
                            // Resubscribe with new ATA list
                            return Ok(());
                        }
                        Some(WatcherCommand::Unwatch(ata)) => {
                            watched_atas.write().remove(&ata);
                            // Continue with current subscription
                        }
                        Some(WatcherCommand::Shutdown) | None => {
                            return Err(anyhow::anyhow!("Shutdown requested"));
                        }
                    }
                }

                // Process Geyser updates
                msg = stream.next() => {
                    match msg {
                        Some(Ok(update)) => {
                            if let Some(UpdateOneof::Account(account_update)) = update.update_oneof {
                                if let Some(account_info) = account_update.account {
                                    // Parse pubkey
                                    if let Ok(bytes) = account_info.pubkey.as_slice().try_into() {
                                        let pubkey = Pubkey::new_from_array(bytes);

                                        // FAST PATH: Check if we're watching this ATA (O(1) lookup!)
                                        if watched_atas.read().contains(&pubkey) {
                                            // Check if there's a pending request for this ATA
                                            let pending = pending_atas.write().remove(&pubkey);

                                            if let Some(pending) = pending {
                                                // Parse balance from SPL Token account data
                                                let balance = if account_info.data.len() >= 72 {
                                                    u64::from_le_bytes(
                                                        account_info.data[64..72].try_into().unwrap_or([0; 8])
                                                    )
                                                } else {
                                                    0
                                                };

                                                if balance >= pending.min_balance {
                                                    let elapsed = pending.registered_at.elapsed();
                                                    info!(
                                                        ata=%pubkey,
                                                        balance=balance,
                                                        slot=account_update.slot,
                                                        elapsed_ms=elapsed.as_millis(),
                                                        "geyser_tx_confirm: ATA balance confirmed via Geyser!"
                                                    );

                                                    // Remove from watch list
                                                    watched_atas.write().remove(&pubkey);

                                                    let _ = pending.notify.send(AtaBalanceResult::Received(balance));
                                                } else {
                                                    // Balance still 0, keep watching
                                                    pending_atas.write().insert(pubkey, pending);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Some(Err(e)) => {
                            return Err(anyhow::anyhow!("Geyser stream error: {}", e));
                        }
                        None => {
                            return Err(anyhow::anyhow!("Geyser stream ended"));
                        }
                    }
                }

                // Periodic cleanup check
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => {
                    // Check if watch list is now empty
                    if watched_atas.read().is_empty() {
                        return Ok(());
                    }
                }
            }
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
    ///
    /// If Geyser is enabled, this will dynamically subscribe to the ATA's account updates.
    /// This is much more efficient than polling RPC!
    pub fn register_ata(&self, ata: Pubkey) -> oneshot::Receiver<AtaBalanceResult> {
        let (tx, rx) = oneshot::channel();

        let pending = PendingAtaBalance {
            notify: tx,
            registered_at: std::time::Instant::now(),
            mint: ata,      // Use ATA as identifier (mint not needed for simplicity)
            min_balance: 1, // Any tokens > 0
        };

        // Add to pending ATAs
        self.pending_atas.write().insert(ata, pending);

        // If Geyser watcher is running, tell it to watch this ATA
        if let Some(watcher_tx) = &self.watcher_tx {
            self.watched_atas.write().insert(ata);
            let _ = watcher_tx.try_send(WatcherCommand::Watch(ata));
            info!(ata=%ata, "geyser_tx_confirm: registered ATA for Geyser-based confirmation");
        } else {
            info!(ata=%ata, "geyser_tx_confirm: registered ATA (no Geyser, will use RPC fallback)");
        }

        rx
    }

    /// Check if Geyser-based watching is enabled
    pub fn is_geyser_enabled(&self) -> bool {
        self.watcher_tx.is_some()
    }

    /// Get the Geyser endpoint URL (if configured)
    pub fn geyser_url(&self) -> Option<&str> {
        self.geyser_url.as_deref()
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

                    // Also remove from watch list
                    self.watched_atas.write().remove(&ata);

                    // Tell watcher to stop watching this ATA
                    if let Some(watcher_tx) = &self.watcher_tx {
                        let _ = watcher_tx.try_send(WatcherCommand::Unwatch(ata));
                    }

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

    /// Get number of currently watched ATAs
    pub fn watched_ata_count(&self) -> usize {
        self.watched_atas.read().len()
    }

    /// Check if an ATA is being watched
    pub fn is_watching_ata(&self, ata: &Pubkey) -> bool {
        self.pending_atas.read().contains_key(ata)
    }

    /// Shutdown the background watcher gracefully
    pub fn shutdown(&self) {
        if let Some(watcher_tx) = &self.watcher_tx {
            let _ = watcher_tx.try_send(WatcherCommand::Shutdown);
        }
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
