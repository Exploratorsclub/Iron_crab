//! Geyser gRPC listener for real-time account updates
//! Uses Agave 3.0's native Geyser integration for <10ms latency

use anyhow::{anyhow, Result};
use futures::StreamExt;
use solana_sdk::{bs58, pubkey::Pubkey};
use std::collections::HashMap;
use tokio::sync::broadcast;
use tracing::debug;
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
    /// First instruction's account indices (for Pump.fun Create)
    pub instruction_accounts: Vec<Pubkey>,
    /// Instruction data (first 8 bytes = discriminator)
    pub instruction_data: Vec<u8>,
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
        let mut last_log = std::time::Instant::now();
        while let Some(message) = stream.next().await {
            if last_log.elapsed().as_secs() >= 60 {
                info!(
                    accounts = account_update_count,
                    transactions = transaction_count,
                    "geyser_listener: heartbeat - still receiving updates"
                );
                last_log = std::time::Instant::now();
            }
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
                                    // Extract signature from transaction
                                    let signature = if !tx.signature.is_empty() {
                                        bs58::encode(&tx.signature).into_string()
                                    } else {
                                        "unknown".to_string()
                                    };

                                    // Extract account keys from transaction message
                                    let mut account_keys = Vec::new();
                                    let mut instruction_accounts = Vec::new();
                                    let mut instruction_data = Vec::new();

                                    if let Some(transaction) = &tx.transaction {
                                        if let Some(message) = &transaction.message {
                                            // Extract all account keys
                                            for key in &message.account_keys {
                                                if key.len() == 32 {
                                                    if let Ok(bytes) = key.as_slice().try_into() {
                                                        account_keys
                                                            .push(Pubkey::new_from_array(bytes));
                                                    }
                                                }
                                            }

                                            // Append loaded addresses from meta (for V0 transactions)
                                            // Order matters: Static -> Loaded Writable -> Loaded Readonly
                                            if let Some(meta) = &tx.meta {
                                                for key in &meta.loaded_writable_addresses {
                                                    if let Ok(bytes) = key.as_slice().try_into() {
                                                        account_keys
                                                            .push(Pubkey::new_from_array(bytes));
                                                    }
                                                }
                                                for key in &meta.loaded_readonly_addresses {
                                                    if let Ok(bytes) = key.as_slice().try_into() {
                                                        account_keys
                                                            .push(Pubkey::new_from_array(bytes));
                                                    }
                                                }
                                            }

                                            // DEBUG: Log instruction extraction attempt
                                            debug!(
                                                signature = %signature,
                                                instruction_count = message.instructions.len(),
                                                account_keys_len = account_keys.len(),
                                                "geyser_listener: Processing transaction"
                                            );

                                            // Find the Pump.fun/Raydium/Orca instruction (not first, could be 2nd or 3rd)
                                            // Look for instruction that uses our monitored program
                                            for (idx, ix) in message.instructions.iter().enumerate()
                                            {
                                                if let Some(program_pubkey) =
                                                    account_keys.get(ix.program_id_index as usize)
                                                {
                                                    // DEBUG: Log each instruction
                                                    debug!(
                                                        signature = %signature,
                                                        instruction_idx = idx,
                                                        program_id = %program_pubkey,
                                                        accounts_len = ix.accounts.len(),
                                                        data_len = ix.data.len(),
                                                        "geyser_listener: Found instruction"
                                                    );

                                                    // Check if this instruction is for one of our monitored programs
                                                    if self.program_ids.contains(program_pubkey) {
                                                        // Extract accounts for this instruction
                                                        for &account_idx in &ix.accounts {
                                                            if let Some(pubkey) = account_keys
                                                                .get(account_idx as usize)
                                                            {
                                                                instruction_accounts.push(*pubkey);
                                                            }
                                                        }
                                                        // Extract instruction data
                                                        instruction_data = ix.data.clone();

                                                        debug!(
                                                            signature = %signature,
                                                            instruction_idx = idx,
                                                            "geyser: extracted DEX instruction"
                                                        );

                                                        break; // Found our instruction, stop searching
                                                    }
                                                }
                                            }
                                        }
                                    }

                                    let event = GeyserTransactionUpdate {
                                        signature,
                                        slot: tx_update.slot,
                                        account_keys,
                                        instruction_accounts,
                                        instruction_data,
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
