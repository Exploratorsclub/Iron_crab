//! Market-data Geyser `transactions_status` listener for wallet TX confirmation.
//!
//! Publishes all wallet-scoped transaction status updates to a channel for JetStream enqueue.
//! Separate Geyser session from the sacred DEX TX listener (PR164 / PR3).

use crate::metrics::WALLET_TX_CONFIRM_LISTENER_CONNECTED;
use futures::StreamExt;
use solana_sdk::bs58;
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::prelude::{
    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
    SubscribeRequestFilterTransactions,
};

/// On-chain wallet transaction status observed via Geyser.
#[derive(Debug, Clone)]
pub struct WalletTxConfirmUpdate {
    pub wallet: Pubkey,
    pub signature: String,
    pub slot: u64,
    /// `None` = success; `Some` = on-chain error string
    pub err: Option<String>,
}

fn commitment_from_str(s: &str) -> i32 {
    if s.eq_ignore_ascii_case("finalized") {
        CommitmentLevel::Finalized as i32
    } else {
        CommitmentLevel::Confirmed as i32
    }
}

/// Build a SubscribeRequest for TX status confirmation (wallet-filtered).
pub fn build_tx_status_subscribe_request(
    wallet_pubkey: &Pubkey,
    confirm_commitment: &str,
) -> SubscribeRequest {
    let mut tx_status_filter = HashMap::new();
    tx_status_filter.insert(
        "wallet_tx_confirm".to_string(),
        SubscribeRequestFilterTransactions {
            vote: Some(false),
            failed: None,
            signature: None,
            account_include: vec![wallet_pubkey.to_string()],
            account_exclude: vec![],
            account_required: vec![],
        },
    );

    SubscribeRequest {
        accounts: HashMap::new(),
        slots: HashMap::new(),
        transactions: HashMap::new(),
        transactions_status: tx_status_filter,
        blocks: HashMap::new(),
        blocks_meta: HashMap::new(),
        entry: HashMap::new(),
        commitment: Some(commitment_from_str(confirm_commitment)),
        accounts_data_slice: vec![],
        ping: None,
        from_slot: None,
    }
}

/// Spawn a dedicated Geyser `transactions_status` listener for the tracked wallet.
///
/// Reconnects automatically on stream errors. Updates are sent via `update_tx` (bounded channel).
pub fn spawn_wallet_tx_confirm_listener(
    geyser_url: String,
    wallet_pubkey: Pubkey,
    confirm_commitment: String,
    update_tx: mpsc::Sender<WalletTxConfirmUpdate>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        info!(
            wallet = %wallet_pubkey,
            commitment = %confirm_commitment,
            "wallet_tx_confirm_listener: starting"
        );

        loop {
            let mut client = match GeyserGrpcClient::build_from_shared(geyser_url.clone()) {
                Ok(builder) => match builder.connect().await {
                    Ok(c) => c,
                    Err(e) => {
                        error!(error = %e, "wallet_tx_confirm_listener: connect failed, retrying in 2s");
                        WALLET_TX_CONFIRM_LISTENER_CONNECTED.store(false, Ordering::Relaxed);
                        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                        continue;
                    }
                },
                Err(e) => {
                    error!(error = %e, "wallet_tx_confirm_listener: build failed, retrying in 2s");
                    WALLET_TX_CONFIRM_LISTENER_CONNECTED.store(false, Ordering::Relaxed);
                    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                    continue;
                }
            };

            let request = build_tx_status_subscribe_request(&wallet_pubkey, &confirm_commitment);

            let (_, mut stream) = match client.subscribe_with_request(Some(request)).await {
                Ok(pair) => pair,
                Err(e) => {
                    error!(error = %e, "wallet_tx_confirm_listener: subscribe failed, retrying in 2s");
                    WALLET_TX_CONFIRM_LISTENER_CONNECTED.store(false, Ordering::Relaxed);
                    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                    continue;
                }
            };

            info!(wallet = %wallet_pubkey, "wallet_tx_confirm_listener: connected and subscribed");
            WALLET_TX_CONFIRM_LISTENER_CONNECTED.store(true, Ordering::Relaxed);

            let stream_err = loop {
                match stream.next().await {
                    Some(Ok(update)) => {
                        let (sig_bytes, slot, err) = match &update.update_oneof {
                            Some(UpdateOneof::TransactionStatus(ts)) => {
                                let err = ts
                                    .err
                                    .as_ref()
                                    .map(|e| String::from_utf8_lossy(&e.err).to_string());
                                (ts.signature.clone(), ts.slot, err)
                            }
                            Some(UpdateOneof::Transaction(tx_update)) => {
                                if let Some(ref tx_info) = tx_update.transaction {
                                    let err_str = tx_info.meta.as_ref().and_then(|m| {
                                        m.err
                                            .as_ref()
                                            .map(|e| String::from_utf8_lossy(&e.err).to_string())
                                    });
                                    (tx_info.signature.clone(), tx_update.slot, err_str)
                                } else {
                                    continue;
                                }
                            }
                            _ => continue,
                        };

                        if sig_bytes.is_empty() {
                            continue;
                        }
                        let sig_str = bs58::encode(&sig_bytes).into_string();

                        let update = WalletTxConfirmUpdate {
                            wallet: wallet_pubkey,
                            signature: sig_str.clone(),
                            slot,
                            err: err.clone(),
                        };

                        match update_tx.try_send(update) {
                            Ok(()) => {
                                debug!(
                                    sig = %sig_str,
                                    slot = slot,
                                    err = ?err,
                                    "wallet_tx_confirm_listener: update enqueued"
                                );
                            }
                            Err(mpsc::error::TrySendError::Full(_)) => {
                                warn!(
                                    sig = %sig_str,
                                    "wallet_tx_confirm_listener: update channel full, dropping confirm"
                                );
                            }
                            Err(mpsc::error::TrySendError::Closed(_)) => {
                                info!("wallet_tx_confirm_listener: update channel closed, shutting down");
                                WALLET_TX_CONFIRM_LISTENER_CONNECTED
                                    .store(false, Ordering::Relaxed);
                                return;
                            }
                        }
                    }
                    Some(Err(e)) => {
                        break format!("stream error: {e}");
                    }
                    None => {
                        break "stream ended".to_string();
                    }
                }
            };

            warn!(
                error = %stream_err,
                wallet = %wallet_pubkey,
                "wallet_tx_confirm_listener: disconnected, reconnecting in 1s"
            );
            WALLET_TX_CONFIRM_LISTENER_CONNECTED.store(false, Ordering::Relaxed);
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }
    })
}
