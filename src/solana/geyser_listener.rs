//! Geyser gRPC listener for real-time account updates
//! Uses Agave 3.0's native Geyser integration for <10ms latency

use anyhow::{anyhow, Result};
use futures::StreamExt;
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use tokio::sync::broadcast;
use tracing::{error, info, warn};
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::prelude::{
    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
    SubscribeRequestFilterAccounts, SubscribeRequestFilterTransactions,
};

/// Event emitted when an account changes via Geyser
#[derive(Debug, Clone)]
pub struct GeyserAccountUpdate {
    pub pubkey: Pubkey,
    pub slot: u64,
    pub owner: Pubkey,
    pub data: Vec<u8>,
    pub lamports: u64,
}

/// Event emitted when a transaction is processed via Geyser
#[derive(Debug, Clone)]
pub struct GeyserTransactionUpdate {
    pub signature: String,
    pub slot: u64,
    pub account_keys: Vec<Pubkey>,
}

pub struct GeyserListener {
    endpoint: String,
    program_ids: Vec<Pubkey>,
    account_tx: broadcast::Sender<GeyserAccountUpdate>,
    transaction_tx: broadcast::Sender<GeyserTransactionUpdate>,
}

impl GeyserListener {
    /// Create new Geyser listener
    ///
    /// `endpoint`: Geyser gRPC endpoint (e.g., "http://127.0.0.1:10000")
    /// `program_ids`: DEX program IDs to monitor (Raydium, Orca, Pump.fun)
    pub fn new(
        endpoint: String,
        program_ids: Vec<Pubkey>,
    ) -> (
        Self,
        broadcast::Receiver<GeyserAccountUpdate>,
        broadcast::Receiver<GeyserTransactionUpdate>,
    ) {
        // Large buffer for high-frequency updates
        let (account_tx, account_rx) = broadcast::channel(200000);
        let (transaction_tx, transaction_rx) = broadcast::channel(50000);

        (
            Self {
                endpoint,
                program_ids,
                account_tx,
                transaction_tx,
            },
            account_rx,
            transaction_rx,
        )
    }

    /// Start listening to Geyser stream
    pub async fn start(self) -> Result<()> {
        info!(
            endpoint = %self.endpoint,
            programs = self.program_ids.len(),
            "geyser_listener: connecting to Geyser gRPC"
        );

        // Connect to Geyser gRPC
        let mut client = GeyserGrpcClient::build_from_shared(self.endpoint.clone())
            .map_err(|e| anyhow!("Failed to build Geyser client: {}", e))?
            .connect()
            .await
            .map_err(|e| anyhow!("Failed to connect to Geyser: {}", e))?;

        info!("geyser_listener: connected successfully");

        // Build subscription request
        let mut accounts_filter = HashMap::new();
        let mut transactions_filter = HashMap::new();

        for (idx, program_id) in self.program_ids.iter().enumerate() {
            // Account updates for pool state changes
            accounts_filter.insert(
                format!("dex_accounts_{}", idx),
                SubscribeRequestFilterAccounts {
                    account: vec![],
                    owner: vec![program_id.to_string()],
                    filters: vec![],
                    nonempty_txn_signature: None,
                },
            );

            // Transactions for pool creation (token mint in instruction accounts)
            transactions_filter.insert(
                format!("dex_transactions_{}", idx),
                SubscribeRequestFilterTransactions {
                    vote: Some(false),
                    failed: Some(false),
                    signature: None,
                    account_include: vec![program_id.to_string()],
                    account_exclude: vec![],
                    account_required: vec![],
                },
            );
        }

        let request = SubscribeRequest {
            accounts: accounts_filter,
            slots: HashMap::new(),
            transactions: transactions_filter,
            transactions_status: HashMap::new(),
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            entry: HashMap::new(),
            commitment: Some(CommitmentLevel::Confirmed as i32),
            accounts_data_slice: vec![],
            ping: None,
            from_slot: None,
        };

        // Subscribe and process stream
        let mut stream = client
            .subscribe_once(request)
            .await
            .map_err(|e| anyhow!("Failed to subscribe: {}", e))?;

        info!(
            programs = ?self.program_ids,
            "geyser_listener: subscribed to accounts and transactions"
        );

        let mut account_update_count = 0u64;
        let mut transaction_count = 0u64;

        // Process incoming updates
        while let Some(message) = stream.next().await {
            match message {
                Ok(msg) => {
                    if let Some(update) = msg.update_oneof {
                        match update {
                            UpdateOneof::Account(account_update) => {
                                account_update_count += 1;

                                if let Some(account_info) = account_update.account {
                                    // Parse pubkey
                                    let pubkey = match account_info.pubkey.as_slice().try_into() {
                                        Ok(bytes) => Pubkey::new_from_array(bytes),
                                        Err(_) => {
                                            warn!("geyser: invalid pubkey length");
                                            continue;
                                        }
                                    };

                                    // Parse owner
                                    let owner = match account_info.owner.as_slice().try_into() {
                                        Ok(bytes) => Pubkey::new_from_array(bytes),
                                        Err(_) => {
                                            warn!("geyser: invalid owner length");
                                            continue;
                                        }
                                    };

                                    let event = GeyserAccountUpdate {
                                        pubkey,
                                        slot: account_update.slot,
                                        owner,
                                        data: account_info.data,
                                        lamports: account_info.lamports,
                                    };

                                    // Broadcast to subscribers
                                    let _ = self.account_tx.send(event);

                                    if account_update_count % 1000 == 0 {
                                        info!(
                                            total_updates = account_update_count,
                                            slot = account_update.slot,
                                            "geyser_listener: processing account updates"
                                        );
                                    }
                                }
                            }
                            UpdateOneof::Transaction(tx_update) => {
                                transaction_count += 1;

                                if let Some(tx) = tx_update.transaction {
                                    // Extract signature from first signature in transaction
                                    let signature = tx
                                        .signatures
                                        .first()
                                        .map(|s| bs58::encode(s).into_string())
                                        .unwrap_or_else(|| "unknown".to_string());

                                    // Extract account keys from transaction message
                                    let mut account_keys = Vec::new();
                                    if let Some(transaction) = &tx.transaction {
                                        if let Some(message) = &transaction.message {
                                            for key in &message.account_keys {
                                                if key.len() == 32 {
                                                    if let Ok(bytes) = key.as_slice().try_into() {
                                                        account_keys
                                                            .push(Pubkey::new_from_array(bytes));
                                                    }
                                                }
                                            }
                                        }
                                    }

                                    let event = GeyserTransactionUpdate {
                                        signature,
                                        slot: tx_update.slot,
                                        account_keys,
                                    };

                                    // Broadcast to subscribers
                                    let _ = self.transaction_tx.send(event);

                                    if transaction_count % 100 == 0 {
                                        info!(
                                            total_transactions = transaction_count,
                                            slot = tx_update.slot,
                                            "geyser_listener: processing transactions"
                                        );
                                    }
                                }
                            }
                            UpdateOneof::Ping(_) => {
                                // Keep-alive ping, ignore
                            }
                            _ => {
                                // Slots, etc. - ignore
                            }
                        }
                    }
                }
                Err(e) => {
                    error!(error = %e, "geyser_listener: stream error");
                    return Err(anyhow!("Geyser stream error: {}", e));
                }
            }
        }

        warn!("geyser_listener: stream ended");
        Ok(())
    }
}
