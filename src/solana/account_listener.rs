//! WebSocket-based account listener for real-time pool updates
//! Subscribes to DEX program accounts and emits events on changes

use anyhow::{anyhow, Result};
use futures::{SinkExt, StreamExt};
use serde_json::json;
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

/// Event emitted when a pool account changes
#[derive(Debug, Clone)]
pub struct PoolUpdateEvent {
    pub program_id: Pubkey,
    pub account_pubkey: Pubkey,
    pub slot: u64,
}

/// WebSocket listener for Solana account updates
pub struct AccountListener {
    ws_url: String,
    program_ids: Vec<Pubkey>,
    tx: broadcast::Sender<PoolUpdateEvent>,
}

impl AccountListener {
    /// Create a new account listener
    /// `ws_url`: WebSocket endpoint (e.g., "ws://127.0.0.1:8900")
    /// `program_ids`: DEX program IDs to monitor (Raydium, Orca Whirlpool)
    pub fn new(
        ws_url: String,
        program_ids: Vec<Pubkey>,
    ) -> (Self, broadcast::Receiver<PoolUpdateEvent>) {
        // Increased buffer for high-volume trading (700k+ pools)
        // At 1000 events/sec, 10k buffer = 10 second tolerance before overflow
        let (tx, rx) = broadcast::channel(10000); // Buffer 10k events (was 1000)

        (
            Self {
                ws_url,
                program_ids,
                tx,
            },
            rx,
        )
    }

    /// Start listening for account updates (runs indefinitely)
    pub async fn run(self: Arc<Self>) -> Result<()> {
        info!(
            ws_url = %self.ws_url,
            program_count = self.program_ids.len(),
            "account_listener: starting WebSocket connection"
        );

        loop {
            match self.connect_and_subscribe().await {
                Ok(_) => {
                    warn!("account_listener: WebSocket connection closed, reconnecting in 5s");
                }
                Err(e) => {
                    error!(error = %e, "account_listener: WebSocket error, reconnecting in 5s");
                }
            }

            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        }
    }

    async fn connect_and_subscribe(&self) -> Result<()> {
        let (ws_stream, _) = connect_async(&self.ws_url)
            .await
            .map_err(|e| anyhow!("Failed to connect to WebSocket: {}", e))?;

        info!("account_listener: WebSocket connected");

        let (mut write, mut read) = ws_stream.split();

        // Subscribe to program accounts for each DEX
        for (idx, program_id) in self.program_ids.iter().enumerate() {
            let subscribe_msg = json!({
                "jsonrpc": "2.0",
                "id": idx + 1,
                "method": "programSubscribe",
                "params": [
                    program_id.to_string(),
                    {
                        "encoding": "base64",
                        "commitment": "confirmed"
                    }
                ]
            });

            write
                .send(Message::Text(subscribe_msg.to_string()))
                .await
                .map_err(|e| anyhow!("Failed to send subscription: {}", e))?;

            info!(
                program_id = %program_id,
                "account_listener: subscribed to program account updates"
            );
        }

        // Process incoming messages
        let mut update_count = 0u64;

        while let Some(msg_result) = read.next().await {
            match msg_result {
                Ok(Message::Text(text)) => {
                    if let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) {
                        // Check if it's a notification (not a subscription response)
                        if value.get("method").and_then(|m| m.as_str())
                            == Some("programNotification")
                        {
                            update_count += 1;

                            if let Some(params) = value.get("params") {
                                if let Some(result) = params.get("result") {
                                    let slot = result
                                        .get("context")
                                        .and_then(|c| c.get("slot"))
                                        .and_then(|s| s.as_u64())
                                        .unwrap_or(0);

                                    let account_key = result
                                        .get("value")
                                        .and_then(|v| v.get("pubkey"))
                                        .and_then(|p| p.as_str())
                                        .and_then(|s| s.parse::<Pubkey>().ok());

                                    if let Some(account_pubkey) = account_key {
                                        // Determine which program this belongs to (simplified)
                                        // In production, track subscription IDs properly
                                        let program_id = self.program_ids[0]; // Simplified for now

                                        let event = PoolUpdateEvent {
                                            program_id,
                                            account_pubkey,
                                            slot,
                                        };

                                        // Broadcast event (non-blocking)
                                        let _ = self.tx.send(event);

                                        if update_count % 100 == 0 {
                                            debug!(
                                                total_updates = update_count,
                                                slot = slot,
                                                "account_listener: processing pool updates"
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                Ok(Message::Ping(data)) => {
                    write.send(Message::Pong(data)).await?;
                }
                Ok(Message::Close(_)) => {
                    warn!("account_listener: WebSocket closed by server");
                    break;
                }
                Err(e) => {
                    error!(error = %e, "account_listener: WebSocket read error");
                    break;
                }
                _ => {}
            }
        }

        Ok(())
    }
}
