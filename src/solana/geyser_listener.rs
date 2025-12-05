//! Geyser gRPC listener for real-time account updates
//! Uses Agave 3.0's native Geyser integration for <10ms latency

use anyhow::{anyhow, Result};
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use tokio::sync::broadcast;
use tracing::{error, info, warn};
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::prelude::*;

/// Event emitted when an account changes via Geyser
#[derive(Debug, Clone)]
pub struct GeyserAccountUpdate {
    pub pubkey: Pubkey,
    pub slot: u64,
    pub owner: Pubkey,
    pub data: Vec<u8>,
    pub lamports: u64,
}

pub struct GeyserListener {
    endpoint: String,
    program_ids: Vec<Pubkey>,
    tx: broadcast::Sender<GeyserAccountUpdate>,
}

impl GeyserListener {
    /// Create new Geyser listener
    /// 
    /// `endpoint`: Geyser gRPC endpoint (e.g., "http://127.0.0.1:10000")
    /// `program_ids`: DEX program IDs to monitor (Raydium, Orca)
    pub fn new(
        endpoint: String,
        program_ids: Vec<Pubkey>,
    ) -> (Self, broadcast::Receiver<GeyserAccountUpdate>) {
        // Large buffer for high-frequency updates
        let (tx, rx) = broadcast::channel(50000);

        (
            Self {
                endpoint,
                program_ids,
                tx,
            },
            rx,
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
        let mut client = GeyserGrpcClient::connect(
            self.endpoint.clone(),
            None, // No auth token
            None, // No custom headers
        )
        .await
        .map_err(|e| anyhow!("Failed to connect to Geyser: {}", e))?;

        info!("geyser_listener: connected successfully");

        // Build subscription request
        let mut accounts_filter = HashMap::new();
        
        for (idx, program_id) in self.program_ids.iter().enumerate() {
            accounts_filter.insert(
                format!("dex_{}", idx),
                SubscribeRequestFilterAccounts {
                    account: vec![],
                    owner: vec![program_id.to_string()],
                    filters: vec![],
                },
            );
        }

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
        };

        // Subscribe and process stream
        let (_, mut stream) = client
            .subscribe_once(request)
            .await
            .map_err(|e| anyhow!("Failed to subscribe: {}", e))?;

        info!(
            programs = ?self.program_ids,
            "geyser_listener: subscribed to account updates"
        );

        let mut update_count = 0u64;

        // Process incoming updates
        while let Some(message) = stream.recv().await {
            match message {
                Ok(msg) => {
                    if let Some(update) = msg.update_oneof {
                        match update {
                            UpdateOneof::Account(account_update) => {
                                update_count += 1;

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
                                    let _ = self.tx.send(event);

                                    if update_count % 1000 == 0 {
                                        info!(
                                            total_updates = update_count,
                                            slot = account_update.slot,
                                            "geyser_listener: processing account updates"
                                        );
                                    }
                                }
                            }
                            UpdateOneof::Ping(_) => {
                                // Keep-alive ping, ignore
                            }
                            _ => {
                                // Slots, transactions, etc. - ignore
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
