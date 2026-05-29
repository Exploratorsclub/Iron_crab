//! Geyser gRPC listener for real-time account updates
//! Uses Agave 3.0's native Geyser integration for <10ms latency

use anyhow::{anyhow, Result};
use solana_sdk::pubkey::Pubkey;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::broadcast;
use tokio::sync::{watch, Notify};

#[cfg(not(windows))]
use futures::{SinkExt, StreamExt};
#[cfg(not(windows))]
use solana_pubkey::Pubkey as CuckooPubkey;
#[cfg(not(windows))]
use solana_sdk::bs58;
#[cfg(not(windows))]
use std::collections::HashMap;
#[cfg(not(windows))]
use std::sync::Mutex;
#[cfg(not(windows))]
use std::time::Duration;
#[cfg(not(windows))]
use tracing::debug;
#[cfg(not(windows))]
use tracing::{error, info, warn};
#[cfg(not(windows))]
use yellowstone_grpc_client::GeyserGrpcClient;
#[cfg(not(windows))]
use yellowstone_grpc_proto::cuckoo::CompressedAccountFilterSet;
#[cfg(not(windows))]
use yellowstone_grpc_proto::prelude::{
    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
    SubscribeRequestFilterAccounts, SubscribeRequestFilterBlocksMeta,
    SubscribeRequestFilterTransactions,
};

#[cfg(not(windows))]
use crate::metrics::{
    geyser_metrics_inc_account_listener_account_updates_total,
    geyser_metrics_inc_account_listener_liveness_reconnect_total,
    geyser_metrics_inc_account_listener_subscribe_sink_backpressure,
    geyser_metrics_inc_account_listener_subscribe_sink_throttled,
    geyser_metrics_inc_account_listener_subscribe_updates_total,
    geyser_metrics_inc_listener_stream_payload, geyser_metrics_inc_reconnect,
    geyser_metrics_inc_stream_error, geyser_metrics_inc_subscription_send_timeout_total,
    geyser_metrics_inc_tracked_cuckoo_table_full,
    geyser_metrics_inc_tx_listener_liveness_reconnect_total,
    geyser_metrics_inc_tx_listener_payload_broadcast_total,
    geyser_metrics_inc_tx_listener_transactions_total,
    geyser_metrics_set_account_session_connected, geyser_metrics_set_tx_session_connected,
    market_data_geyser_head_slot_value, market_data_take_account_session_reconnect_request,
    market_data_take_tx_session_reconnect_request, market_data_tx_handler_processed_value,
    GeyserReconnectReason,
};
#[cfg(not(windows))]
use rand::Rng;
#[cfg(not(windows))]
use tokio::time::sleep;

/// Event emitted when an account changes via Geyser
#[derive(Debug, Clone)]
pub struct GeyserAccountUpdate {
    pub pubkey: Pubkey,
    pub slot: u64,
    pub owner: Pubkey,
    pub data: Vec<u8>,
    pub lamports: u64,
    /// Wall-clock instant when the gRPC stream delivered this update into the listener (immediately before broadcast send).
    pub grpc_recv_at: Instant,
}

/// Inner instruction from transaction meta (for parsing token transfers)
#[derive(Debug, Clone)]
pub struct InnerInstruction {
    pub program_id_index: u8,
    pub accounts: Vec<u8>,
    pub data: Vec<u8>,
}

/// Token balance change from transaction meta
#[derive(Debug, Clone)]
pub struct TokenBalance {
    pub account_index: u8,
    pub mint: String,
    pub ui_token_amount: TokenAmount,
    /// Token program ID (SPL Token or Token-2022) - authoritative source for token type
    pub program_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TokenAmount {
    pub ui_amount: Option<f64>,
    pub decimals: u8,
    pub amount: String,
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
    /// Inner instructions (CPI calls, including token transfers)
    pub inner_instructions: Vec<InnerInstruction>,
    /// Token balance changes (pre/post balances)
    pub pre_token_balances: Vec<TokenBalance>,
    pub post_token_balances: Vec<TokenBalance>,
    /// Native SOL balances (lamports) for all accounts
    pub pre_balances: Vec<u64>,
    pub post_balances: Vec<u64>,
    /// Transaction fee in lamports (for priority fee tracking)
    pub fee_lamports: u64,
    /// Compute units consumed (for priority fee calculation)
    pub compute_units_consumed: Option<u64>,
    /// Wall-clock instant when the gRPC stream delivered this update into the listener (immediately before broadcast send).
    pub grpc_recv_at: Instant,
}

/// Event emitted when a new confirmed block is produced (from blocks_meta)
#[derive(Debug, Clone)]
pub struct GeyserBlockhashUpdate {
    pub blockhash: String,
    pub slot: u64,
    pub block_height: u64,
}

/// PR-A: exponential reconnect sleep with small jitter (cold path only).
#[cfg(not(windows))]
fn geyser_reconnect_sleep_ms(backoff_ms: u64) -> u64 {
    let jitter_cap = backoff_ms.min(150);
    let jitter = if jitter_cap == 0 {
        0u64
    } else {
        rand::thread_rng().gen_range(0..=jitter_cap)
    };
    backoff_ms.saturating_add(jitter)
}

/// Yellowstone `cuckoo_accounts_filter` entry name for explicit tracked accounts.
#[cfg(not(windows))]
pub(crate) const TRACKED_CUCKOO_SUBSCRIBE_NAME: &str = "tracked_accounts_cuckoo";

#[cfg(not(windows))]
pub(crate) fn build_tx_subscribe_request(program_ids: &[Pubkey]) -> SubscribeRequest {
    let mut transactions_filter = HashMap::new();
    for (idx, program_id) in program_ids.iter().enumerate() {
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
    let blocks_meta_filter =
        HashMap::from([("blockhash".to_string(), SubscribeRequestFilterBlocksMeta {})]);
    SubscribeRequest {
        accounts: HashMap::new(),
        slots: HashMap::new(),
        transactions: transactions_filter,
        transactions_status: HashMap::new(),
        blocks: HashMap::new(),
        blocks_meta: blocks_meta_filter,
        entry: HashMap::new(),
        commitment: Some(CommitmentLevel::Confirmed as i32),
        accounts_data_slice: vec![],
        ping: None,
        from_slot: None,
    }
}

#[cfg(not(windows))]
pub(crate) fn build_account_subscribe_request(
    program_ids: &[Pubkey],
    tracked_cuckoo: Option<&mut CompressedAccountFilterSet>,
) -> SubscribeRequest {
    let mut accounts_filter = HashMap::new();

    for (idx, program_id) in program_ids.iter().enumerate() {
        accounts_filter.insert(
            format!("dex_accounts_{}", idx),
            SubscribeRequestFilterAccounts {
                account: vec![],
                owner: vec![program_id.to_string()],
                filters: vec![],
                nonempty_txn_signature: None,
                cuckoo_accounts_filter: None,
            },
        );
    }

    let mut req = SubscribeRequest {
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

    if let Some(cuck) = tracked_cuckoo {
        cuck.insert_into_subscribe_request(&mut req, TRACKED_CUCKOO_SUBSCRIBE_NAME);
    }

    req
}

/// PR164: TX-only Geyser gRPC session — one `SubscribeRequest` at connect, **no** in-place
/// subscription updates (sacred TX stream + `blocks_meta` blockhash).
pub struct GeyserTxListener {
    endpoint: String,
    program_ids: Vec<Pubkey>,
    #[cfg_attr(windows, allow(dead_code))]
    transaction_tx: broadcast::Sender<GeyserTransactionUpdate>,
    #[cfg_attr(windows, allow(dead_code))]
    blockhash_tx: broadcast::Sender<GeyserBlockhashUpdate>,
}

/// Outcome of pushing a coalesced `SubscribeRequest` to the Yellowstone subscription sink (Phase-R-R3).
#[cfg(not(windows))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SubscriptionSinkPushOutcome {
    Sent,
    SinkClosed,
    TimedOut,
}

/// Phase-R-R3 (plan I-4b): max one Yellowstone subscription push per 500 ms window.
#[cfg(not(windows))]
pub(crate) const SUBSCRIBE_SINK_MIN_INTERVAL: Duration = Duration::from_millis(500);
/// Phase-R-R3: never block indefinitely on a full subscription sink.
#[cfg(not(windows))]
pub(crate) const SUBSCRIBE_SINK_SEND_TIMEOUT: Duration = Duration::from_secs(2);

/// PR164: Account + cuckoo Geyser session — owner filters + dynamic pins; **no** transaction filters.
pub struct GeyserAccountListener {
    endpoint: String,
    program_ids: Vec<Pubkey>,
    tracked_accounts_rx: watch::Receiver<Vec<Pubkey>>,
    full_reconnect_tracked_threshold: Arc<AtomicUsize>,
    tracked_cuckoo_max_capacity: usize,
    #[cfg_attr(windows, allow(dead_code))]
    account_tx: broadcast::Sender<GeyserAccountUpdate>,
}

impl GeyserTxListener {
    pub fn new(
        endpoint: String,
        program_ids: Vec<Pubkey>,
    ) -> (
        Self,
        broadcast::Receiver<GeyserTransactionUpdate>,
        broadcast::Receiver<GeyserBlockhashUpdate>,
    ) {
        let (transaction_tx, transaction_rx) = broadcast::channel(50000);
        let (blockhash_tx, blockhash_rx) = broadcast::channel(100);
        (
            Self {
                endpoint,
                program_ids,
                transaction_tx,
                blockhash_tx,
            },
            transaction_rx,
            blockhash_rx,
        )
    }

    pub fn subscribe_transaction_updates(&self) -> broadcast::Receiver<GeyserTransactionUpdate> {
        self.transaction_tx.subscribe()
    }

    pub fn subscribe_blockhash_updates(&self) -> broadcast::Receiver<GeyserBlockhashUpdate> {
        self.blockhash_tx.subscribe()
    }

    pub async fn start(self) -> Result<()> {
        #[cfg(windows)]
        {
            let _ = (self.endpoint, self.program_ids);
            Err(anyhow!(
                "Geyser gRPC is not supported on Windows in this repo build. \
                 Build on Linux/macOS for Geyser support."
            ))
        }

        #[cfg(not(windows))]
        {
            self.start_tx_impl().await
        }
    }

    #[cfg(not(windows))]
    async fn start_tx_impl(self) -> Result<()> {
        enum SessionExit {
            StreamEnded,
            HardReconnect,
            TxLivenessStale,
        }

        info!(
            endpoint = %self.endpoint,
            programs = self.program_ids.len(),
            "geyser_tx_listener: connecting to Geyser gRPC (TX + blocks_meta only; PR164)"
        );

        let Self {
            endpoint,
            program_ids,
            transaction_tx,
            blockhash_tx,
        } = self;

        let mut reconnect_backoff_ms: u64 = rand::thread_rng().gen_range(100..=250);
        const RECONNECT_BACKOFF_CAP_MS: u64 = 60_000;
        let mut last_stream_ended_log =
            std::time::Instant::now() - std::time::Duration::from_secs(10);
        let mut last_log = std::time::Instant::now();
        let mut transaction_count: u64 = 0;
        let mut seen_tx_since_connect = false;
        let mut last_liveness_tx_handler_total: u64 = 0;
        let mut last_liveness_head_slot: u64 = 0;
        let mut first_liveness_window = true;

        'outer: loop {
            let mut client = loop {
                match GeyserGrpcClient::build_from_shared(endpoint.clone())
                    .map_err(|e| anyhow!("Failed to build Geyser client: {}", e))?
                    .connect()
                    .await
                {
                    Ok(c) => {
                        info!("geyser_tx_listener: connected successfully");
                        geyser_metrics_set_tx_session_connected(true);
                        break c;
                    }
                    Err(e) => {
                        error!(error = %e, "geyser_tx_listener: connect failed, retrying");
                        geyser_metrics_set_tx_session_connected(false);
                        let sleep_ms = geyser_reconnect_sleep_ms(reconnect_backoff_ms);
                        sleep(Duration::from_millis(sleep_ms)).await;
                        reconnect_backoff_ms =
                            (reconnect_backoff_ms.saturating_mul(2)).min(RECONNECT_BACKOFF_CAP_MS);
                    }
                }
            };

            'same_client: loop {
                let request = build_tx_subscribe_request(&program_ids);
                let (mut _subscribe_tx, mut stream) =
                    match client.subscribe_with_request(Some(request)).await {
                        Ok(pair) => pair,
                        Err(e) => {
                            warn!(
                                error = %e,
                                "geyser_tx_listener: subscribe failed; reconnecting with new client"
                            );
                            geyser_metrics_inc_reconnect(GeyserReconnectReason::StreamError);
                            geyser_metrics_set_tx_session_connected(false);
                            let sleep_ms = geyser_reconnect_sleep_ms(reconnect_backoff_ms);
                            sleep(Duration::from_millis(sleep_ms)).await;
                            reconnect_backoff_ms = (reconnect_backoff_ms.saturating_mul(2))
                                .min(RECONNECT_BACKOFF_CAP_MS);
                            continue 'outer;
                        }
                    };

                info!(
                    programs = ?program_ids,
                    "geyser_tx_listener: subscribed (TX sacred — no further subscribe updates)"
                );

                let mut got_payload_since_subscribe = false;
                let mut liveness = tokio::time::interval(Duration::from_secs(60));
                liveness.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

                let session_exit = 'read: loop {
                    if last_log.elapsed().as_secs() >= 60 {
                        info!(
                            transactions = transaction_count,
                            "geyser_tx_listener: heartbeat - still receiving updates"
                        );
                        last_log = std::time::Instant::now();
                    }

                    tokio::select! {
                        biased;
                        _ = liveness.tick() => {
                            if market_data_take_tx_session_reconnect_request() {
                                warn!(
                                    "geyser_tx_listener: TX handler stall watchdog requested reconnect"
                                );
                                geyser_metrics_inc_tx_listener_liveness_reconnect_total();
                                break 'read SessionExit::TxLivenessStale;
                            }
                            let head = market_data_geyser_head_slot_value();
                            let tx_handler_total = market_data_tx_handler_processed_value();
                            if first_liveness_window {
                                last_liveness_head_slot = head;
                                last_liveness_tx_handler_total = tx_handler_total;
                                first_liveness_window = false;
                                continue;
                            }
                            if seen_tx_since_connect
                                && tx_handler_total == last_liveness_tx_handler_total
                                && head > last_liveness_head_slot
                            {
                                warn!(
                                    tx_handler_total,
                                    head_slot = head,
                                    prev_head_slot = last_liveness_head_slot,
                                    "geyser_tx_listener: TX handler stale while chain advanced — forcing reconnect"
                                );
                                geyser_metrics_inc_tx_listener_liveness_reconnect_total();
                                break 'read SessionExit::TxLivenessStale;
                            }
                            last_liveness_head_slot = head;
                            last_liveness_tx_handler_total = tx_handler_total;
                        }
                        maybe_message = stream.next() => {
                            match maybe_message {
                                None => {
                                    geyser_metrics_inc_reconnect(GeyserReconnectReason::StreamEnded);
                                    let now = std::time::Instant::now();
                                    if now.duration_since(last_stream_ended_log)
                                        >= Duration::from_secs(1)
                                    {
                                        warn!(
                                            "geyser_tx_listener: stream ended; resubscribing after short backoff"
                                        );
                                        last_stream_ended_log = now;
                                    }
                                    let sleep_ms = rand::thread_rng().gen_range(50..=200);
                                    sleep(Duration::from_millis(sleep_ms)).await;
                                    break 'read SessionExit::StreamEnded;
                                }
                                Some(Err(e)) => {
                                    error!(error = %e, "geyser_tx_listener: stream error");
                                    geyser_metrics_inc_stream_error();
                                    geyser_metrics_inc_reconnect(GeyserReconnectReason::StreamError);
                                    break 'read SessionExit::HardReconnect;
                                }
                                Some(Ok(msg)) => {
                                    geyser_metrics_inc_listener_stream_payload();
                                    if let Some(update) = msg.update_oneof {
                                        match update {
                                            UpdateOneof::Transaction(tx_update) => {
                                                geyser_metrics_inc_tx_listener_transactions_total();
                                                transaction_count = transaction_count.saturating_add(1);
                                                if let Some(tx) = tx_update.transaction {
                                                    seen_tx_since_connect = true;
                                                    let signature = if !tx.signature.is_empty() {
                                                        bs58::encode(&tx.signature).into_string()
                                                    } else {
                                                        "unknown".to_string()
                                                    };
                                                    let mut account_keys = Vec::new();
                                                    let mut instruction_accounts = Vec::new();
                                                    let mut instruction_data = Vec::new();
                                                    if let Some(transaction) = &tx.transaction {
                                                        if let Some(message) = &transaction.message {
                                                            for key in &message.account_keys {
                                                                if key.len() == 32 {
                                                                    if let Ok(bytes) = key.as_slice().try_into() {
                                                                        account_keys.push(Pubkey::new_from_array(bytes));
                                                                    }
                                                                }
                                                            }
                                                            if let Some(meta) = &tx.meta {
                                                                for key in &meta.loaded_writable_addresses {
                                                                    if let Ok(bytes) = key.as_slice().try_into() {
                                                                        account_keys.push(Pubkey::new_from_array(bytes));
                                                                    }
                                                                }
                                                                for key in &meta.loaded_readonly_addresses {
                                                                    if let Ok(bytes) = key.as_slice().try_into() {
                                                                        account_keys.push(Pubkey::new_from_array(bytes));
                                                                    }
                                                                }
                                                            }
                                                            debug!(
                                                                signature = %signature,
                                                                instruction_count = message.instructions.len(),
                                                                account_keys_len = account_keys.len(),
                                                                "geyser_tx_listener: Processing transaction"
                                                            );
                                                            for (idx, ix) in message.instructions.iter().enumerate() {
                                                                if let Some(program_pubkey) =
                                                                    account_keys.get(ix.program_id_index as usize)
                                                                {
                                                                    debug!(
                                                                        signature = %signature,
                                                                        instruction_idx = idx,
                                                                        program_id = %program_pubkey,
                                                                        accounts_len = ix.accounts.len(),
                                                                        data_len = ix.data.len(),
                                                                        "geyser_tx_listener: Found instruction"
                                                                    );
                                                                    if program_ids.contains(program_pubkey) {
                                                                        for &account_idx in &ix.accounts {
                                                                            if let Some(pubkey) =
                                                                                account_keys.get(account_idx as usize)
                                                                            {
                                                                                instruction_accounts.push(*pubkey);
                                                                            }
                                                                        }
                                                                        instruction_data = ix.data.clone();
                                                                        debug!(
                                                                            signature = %signature,
                                                                            instruction_idx = idx,
                                                                            "geyser: extracted DEX instruction"
                                                                        );
                                                                        break;
                                                                    }
                                                                }
                                                            }
                                                            if instruction_accounts.is_empty() {
                                                                if let Some(meta) = &tx.meta {
                                                                    for inner_ix_group in &meta.inner_instructions {
                                                                        for inner_ix in &inner_ix_group.instructions {
                                                                            if let Some(program_pubkey) = account_keys.get(
                                                                                inner_ix.program_id_index as usize,
                                                                            ) {
                                                                                if program_ids.contains(program_pubkey) {
                                                                                    for &account_idx in &inner_ix.accounts {
                                                                                        if let Some(pubkey) = account_keys.get(
                                                                                            account_idx as usize,
                                                                                        ) {
                                                                                            instruction_accounts.push(*pubkey);
                                                                                        }
                                                                                    }
                                                                                    instruction_data = inner_ix.data.clone();
                                                                                    debug!(
                                                                                        signature = %signature,
                                                                                        inner_ix_index = inner_ix_group.index,
                                                                                        program_id = %program_pubkey,
                                                                                        "geyser: extracted DEX instruction from INNER instruction (CPI)"
                                                                                    );
                                                                                    break;
                                                                                }
                                                                            }
                                                                        }
                                                                        if !instruction_accounts.is_empty() {
                                                                            break;
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                    let mut inner_instructions = Vec::new();
                                                    if let Some(meta) = &tx.meta {
                                                        for inner_ix_group in &meta.inner_instructions {
                                                            for inner_ix in &inner_ix_group.instructions {
                                                                inner_instructions.push(InnerInstruction {
                                                                    program_id_index: inner_ix.program_id_index as u8,
                                                                    accounts: inner_ix.accounts.to_vec(),
                                                                    data: inner_ix.data.clone(),
                                                                });
                                                            }
                                                        }
                                                    }
                                                    let mut pre_token_balances = Vec::new();
                                                    let mut post_token_balances = Vec::new();
                                                    let mut pre_balances = Vec::new();
                                                    let mut post_balances = Vec::new();
                                                    let mut fee_lamports = 0u64;
                                                    let mut compute_units_consumed = None;
                                                    if let Some(meta) = &tx.meta {
                                                        fee_lamports = meta.fee;
                                                        compute_units_consumed = meta.compute_units_consumed;
                                                        pre_balances = meta.pre_balances.clone();
                                                        post_balances = meta.post_balances.clone();
                                                        for balance in &meta.pre_token_balances {
                                                            if let Some(ui_amount) = &balance.ui_token_amount {
                                                                pre_token_balances.push(TokenBalance {
                                                                    account_index: balance.account_index as u8,
                                                                    mint: balance.mint.clone(),
                                                                    ui_token_amount: TokenAmount {
                                                                        ui_amount: Some(ui_amount.ui_amount),
                                                                        decimals: ui_amount.decimals as u8,
                                                                        amount: ui_amount.amount.clone(),
                                                                    },
                                                                    program_id: if balance.program_id.is_empty() {
                                                                        None
                                                                    } else {
                                                                        Some(balance.program_id.clone())
                                                                    },
                                                                });
                                                            }
                                                        }
                                                        for balance in &meta.post_token_balances {
                                                            if let Some(ui_amount) = &balance.ui_token_amount {
                                                                post_token_balances.push(TokenBalance {
                                                                    account_index: balance.account_index as u8,
                                                                    mint: balance.mint.clone(),
                                                                    ui_token_amount: TokenAmount {
                                                                        ui_amount: Some(ui_amount.ui_amount),
                                                                        decimals: ui_amount.decimals as u8,
                                                                        amount: ui_amount.amount.clone(),
                                                                    },
                                                                    program_id: if balance.program_id.is_empty() {
                                                                        None
                                                                    } else {
                                                                        Some(balance.program_id.clone())
                                                                    },
                                                                });
                                                            }
                                                        }
                                                    }
                                                    let grpc_recv_at = Instant::now();
                                                    let event = GeyserTransactionUpdate {
                                                        signature,
                                                        slot: tx_update.slot,
                                                        account_keys,
                                                        instruction_accounts,
                                                        instruction_data,
                                                        inner_instructions,
                                                        pre_token_balances,
                                                        post_token_balances,
                                                        pre_balances,
                                                        post_balances,
                                                        fee_lamports,
                                                        compute_units_consumed,
                                                        grpc_recv_at,
                                                    };
                                                    if transaction_tx.send(event).is_ok() {
                                                        geyser_metrics_inc_tx_listener_payload_broadcast_total();
                                                    }
                                                    if !got_payload_since_subscribe {
                                                        got_payload_since_subscribe = true;
                                                        reconnect_backoff_ms =
                                                            rand::thread_rng().gen_range(100..=250);
                                                    }
                                                    if transaction_count % 100 == 0 {
                                                        info!(
                                                            total_transactions = transaction_count,
                                                            slot = tx_update.slot,
                                                            "geyser_tx_listener: processing transactions"
                                                        );
                                                    }
                                                }
                                            }
                                            UpdateOneof::BlockMeta(block_meta) => {
                                                let event = GeyserBlockhashUpdate {
                                                    blockhash: block_meta.blockhash,
                                                    slot: block_meta.slot,
                                                    block_height: block_meta
                                                        .block_height
                                                        .map(|h| h.block_height)
                                                        .unwrap_or(0),
                                                };
                                                let _ = blockhash_tx.send(event);
                                                if !got_payload_since_subscribe {
                                                    got_payload_since_subscribe = true;
                                                    reconnect_backoff_ms =
                                                        rand::thread_rng().gen_range(100..=250);
                                                }
                                            }
                                            UpdateOneof::Ping(_) => {}
                                            _ => {}
                                        }
                                    }
                                }
                            }
                        }
                    }
                };

                match session_exit {
                    SessionExit::StreamEnded => {
                        continue 'same_client;
                    }
                    SessionExit::HardReconnect | SessionExit::TxLivenessStale => {
                        geyser_metrics_set_tx_session_connected(false);
                        let sleep_ms = geyser_reconnect_sleep_ms(reconnect_backoff_ms);
                        sleep(Duration::from_millis(sleep_ms)).await;
                        reconnect_backoff_ms =
                            (reconnect_backoff_ms.saturating_mul(2)).min(RECONNECT_BACKOFF_CAP_MS);
                        continue 'outer;
                    }
                }
            }
        }
    }
}

impl GeyserAccountListener {
    pub fn new_with_tracked_accounts(
        endpoint: String,
        program_ids: Vec<Pubkey>,
        tracked_accounts_rx: watch::Receiver<Vec<Pubkey>>,
        full_reconnect_tracked_threshold: Arc<AtomicUsize>,
        tracked_cuckoo_max_capacity: usize,
    ) -> (Self, broadcast::Receiver<GeyserAccountUpdate>) {
        let (account_tx, account_rx) = broadcast::channel(200000);
        (
            Self {
                endpoint,
                program_ids,
                tracked_accounts_rx,
                full_reconnect_tracked_threshold,
                tracked_cuckoo_max_capacity,
                account_tx,
            },
            account_rx,
        )
    }

    pub fn subscribe_account_updates(&self) -> broadcast::Receiver<GeyserAccountUpdate> {
        self.account_tx.subscribe()
    }

    pub async fn start(self) -> Result<()> {
        #[cfg(windows)]
        {
            let _ = (
                self.endpoint,
                self.program_ids,
                self.tracked_accounts_rx,
                self.full_reconnect_tracked_threshold,
                self.tracked_cuckoo_max_capacity,
            );
            Err(anyhow!(
                "Geyser gRPC is not supported on Windows in this repo build. \
                 Build on Linux/macOS for Geyser support."
            ))
        }

        #[cfg(not(windows))]
        {
            self.start_account_impl().await
        }
    }

    #[cfg(not(windows))]
    #[inline]
    fn cuckoo_pubkey(pk: Pubkey) -> CuckooPubkey {
        CuckooPubkey::new_from_array(pk.to_bytes())
    }

    /// Rebuild the persistent cuckoo filter from the full explicit tracked list.
    #[cfg(not(windows))]
    fn sync_tracked_cuckoo_filter(
        filter: &mut CompressedAccountFilterSet,
        max_capacity: usize,
        new_list: &[Pubkey],
    ) -> Result<()> {
        *filter = Self::tracked_cuckoo_from_full_list(max_capacity, new_list)?;
        Ok(())
    }

    #[cfg(not(windows))]
    fn tracked_cuckoo_from_full_list(
        max_capacity: usize,
        list: &[Pubkey],
    ) -> Result<CompressedAccountFilterSet> {
        let mut f = CompressedAccountFilterSet::with_capacity(max_capacity).map_err(|e| {
            anyhow!("geyser_listener: CompressedAccountFilterSet with_capacity failed: {e}")
        })?;
        for pk in list {
            f.insert(Self::cuckoo_pubkey(*pk)).map_err(|e| {
                geyser_metrics_inc_tracked_cuckoo_table_full();
                anyhow!("geyser_listener: CompressedAccountFilterSet insert / TableFull: {e}")
            })?;
        }
        Ok(f)
    }

    /// Phase-R-R3 / PR167: latest full subscribe snapshot wins — coalesce bursts into one sink send.
    #[cfg(not(windows))]
    fn coalesce_pending_subscription(
        pending: &Mutex<Option<SubscribeRequest>>,
        req: SubscribeRequest,
    ) {
        if let Ok(mut g) = pending.lock() {
            *g = Some(req);
        }
    }

    /// Push one coalesced `SubscribeRequest` to the Yellowstone sink with a hard timeout (I-4b).
    #[cfg(not(windows))]
    pub(crate) async fn push_subscribe_request_to_sink<S>(
        subscribe_tx: &mut S,
        req: SubscribeRequest,
        timeout: Duration,
    ) -> SubscriptionSinkPushOutcome
    where
        S: futures::Sink<SubscribeRequest> + Unpin,
        S::Error: std::fmt::Display,
    {
        match tokio::time::timeout(timeout, subscribe_tx.send(req)).await {
            Ok(Ok(())) => SubscriptionSinkPushOutcome::Sent,
            Ok(Err(_)) => SubscriptionSinkPushOutcome::SinkClosed,
            Err(_) => SubscriptionSinkPushOutcome::TimedOut,
        }
    }
    #[cfg(not(windows))]
    const TRACKED_SET_JUMP_REBUILD_THRESHOLD: usize = 50;

    #[cfg(not(windows))]
    async fn start_account_impl(self) -> Result<()> {
        enum SessionExit {
            StreamEnded,
            HardReconnect,
            /// PR-B: large explicit subscription set — reconnect with a fresh client.
            SubscriptionRebuild,
        }

        let Self {
            endpoint,
            program_ids,
            tracked_accounts_rx,
            full_reconnect_tracked_threshold,
            tracked_cuckoo_max_capacity,
            account_tx,
        } = self;

        info!(
            endpoint = %endpoint,
            programs = program_ids.len(),
            "geyser_account_listener: connecting to Geyser gRPC (accounts + cuckoo; PR164)"
        );

        let mut tracked_accounts_rx = tracked_accounts_rx;
        let mut tracked_accounts_current: Vec<Pubkey> = tracked_accounts_rx.borrow().clone();

        let cap = tracked_cuckoo_max_capacity.max(1);
        let mut tracked_cuckoo_filter: Option<CompressedAccountFilterSet> =
            match Self::tracked_cuckoo_from_full_list(cap, &tracked_accounts_current) {
                Ok(f) => Some(f),
                Err(e) => {
                    error!(
                        error = %e,
                        capacity = cap,
                        n = tracked_accounts_current.len(),
                        "geyser_account_listener: initial CompressedAccountFilterSet build failed"
                    );
                    return Err(anyhow!(
                            "geyser_account_listener: cannot build CompressedAccountFilterSet: {e}; increase max_tracked_accounts"
                        ));
                }
            };

        let mut account_update_count = 0u64;
        let mut last_log = std::time::Instant::now();

        let mut reconnect_backoff_ms: u64 = rand::thread_rng().gen_range(100..=250);
        const RECONNECT_BACKOFF_CAP_MS: u64 = 60_000;
        let mut last_stream_ended_log =
            std::time::Instant::now() - std::time::Duration::from_secs(10);
        // When over `full_reconnect_tracked_threshold`, subscription list deltas used to trigger a
        // full reconnect on every change — thrashing the stream. Coalesce to at most one such
        // rebuild per interval; other updates use in-place subscribe (same as small sets).
        const LARGE_TRACKED_SUBSCRIBE_REBUILD_MIN_INTERVAL: Duration = Duration::from_secs(5);
        let mut last_large_set_subscription_rebuild: Option<Instant> = None;
        let mut last_tracked_subscription_change: Option<Instant> = None;

        geyser_metrics_set_account_session_connected(false);

        'outer: loop {
            let mut client = loop {
                match GeyserGrpcClient::build_from_shared(endpoint.clone())
                    .map_err(|e| anyhow!("Failed to build Geyser client: {}", e))?
                    .connect()
                    .await
                {
                    Ok(c) => {
                        info!("geyser_listener: connected successfully");
                        geyser_metrics_set_account_session_connected(true);
                        break c;
                    }
                    Err(e) => {
                        error!(error = %e, "geyser_listener: connect failed, retrying");
                        geyser_metrics_set_account_session_connected(false);
                        let sleep_ms = geyser_reconnect_sleep_ms(reconnect_backoff_ms);
                        sleep(Duration::from_millis(sleep_ms)).await;
                        reconnect_backoff_ms =
                            (reconnect_backoff_ms.saturating_mul(2)).min(RECONNECT_BACKOFF_CAP_MS);
                    }
                }
            };

            'same_client: loop {
                let request =
                    build_account_subscribe_request(&program_ids, tracked_cuckoo_filter.as_mut());

                let (mut subscribe_tx, mut stream) =
                    match client.subscribe_with_request(Some(request)).await {
                        Ok(pair) => pair,
                        Err(e) => {
                            warn!(
                                error = %e,
                                "geyser_listener: subscribe failed; reconnecting with new client"
                            );
                            geyser_metrics_inc_reconnect(GeyserReconnectReason::StreamError);
                            geyser_metrics_set_account_session_connected(false);
                            let sleep_ms = geyser_reconnect_sleep_ms(reconnect_backoff_ms);
                            sleep(Duration::from_millis(sleep_ms)).await;
                            reconnect_backoff_ms = (reconnect_backoff_ms.saturating_mul(2))
                                .min(RECONNECT_BACKOFF_CAP_MS);
                            continue 'outer;
                        }
                    };

                // PR162: never `await` Yellowstone `subscribe_tx.send` in the stream read task — a slow
                // sink starves `stream.next()`, heartbeats, and head_slot. Coalesce bursts into one pending
                // request; a dedicated task performs the sink send.
                let pending_subscription_request: Arc<Mutex<Option<SubscribeRequest>>> =
                    Arc::new(Mutex::new(None));
                let subscription_notify = Arc::new(Notify::new());
                let pending_for_updater = Arc::clone(&pending_subscription_request);
                let notify_for_updater = Arc::clone(&subscription_notify);
                let sink_fail_notify = Arc::new(Notify::new());
                let sink_fail_reader = Arc::clone(&sink_fail_notify);
                let sink_fail_updater = Arc::clone(&sink_fail_notify);
                let subscription_updater_jh = tokio::spawn(async move {
                    let mut last_send = Instant::now() - SUBSCRIBE_SINK_MIN_INTERVAL;
                    loop {
                        notify_for_updater.notified().await;
                        loop {
                            let elapsed = last_send.elapsed();
                            if elapsed < SUBSCRIBE_SINK_MIN_INTERVAL {
                                geyser_metrics_inc_account_listener_subscribe_sink_throttled();
                                sleep(SUBSCRIBE_SINK_MIN_INTERVAL - elapsed).await;
                            }
                            let req = match pending_for_updater.lock() {
                                Ok(mut g) => g.take(),
                                Err(_) => continue,
                            };
                            let Some(req) = req else {
                                break;
                            };
                            match Self::push_subscribe_request_to_sink(
                                &mut subscribe_tx,
                                req,
                                SUBSCRIBE_SINK_SEND_TIMEOUT,
                            )
                            .await
                            {
                                SubscriptionSinkPushOutcome::Sent => {
                                    last_send = Instant::now();
                                }
                                SubscriptionSinkPushOutcome::SinkClosed => {
                                    warn!(
                                        "geyser_listener: subscription updater failed to push subscribe request (sink gone)"
                                    );
                                    geyser_metrics_inc_reconnect(GeyserReconnectReason::SinkGone);
                                    return;
                                }
                                SubscriptionSinkPushOutcome::TimedOut => {
                                    geyser_metrics_inc_subscription_send_timeout_total();
                                    geyser_metrics_inc_account_listener_subscribe_sink_backpressure(
                                    );
                                    warn!(
                                        "geyser_listener: subscription sink backpressure (send timeout) — requesting full reconnect"
                                    );
                                    sink_fail_updater.notify_one();
                                    return;
                                }
                            }
                            let has_more = pending_for_updater
                                .lock()
                                .map(|g| g.is_some())
                                .unwrap_or(false);
                            if !has_more {
                                break;
                            }
                        }
                    }
                });

                info!(
                    programs = ?program_ids,
                    tracked_accounts = tracked_accounts_current.len(),
                    "geyser_account_listener: subscribed"
                );

                let mut got_payload_since_subscribe = false;
                let mut account_liveness = tokio::time::interval(Duration::from_secs(5));
                account_liveness.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

                let session_exit = 'read: loop {
                    if last_log.elapsed().as_secs() >= 60 {
                        info!(
                            accounts = account_update_count,
                            tracked_accounts = tracked_accounts_current.len(),
                            "geyser_account_listener: heartbeat - still receiving updates"
                        );
                        last_log = std::time::Instant::now();
                    }

                    let tracked_changed_fut = async {
                        let _ = tracked_accounts_rx.changed().await;
                        tracked_accounts_rx.borrow().clone()
                    };

                    tokio::select! {
                        biased;
                        _ = sink_fail_reader.notified() => {
                            warn!(
                                tracked_accounts = tracked_accounts_current.len(),
                                "geyser_listener: subscription sink backpressure — forcing full reconnect"
                            );
                            break 'read SessionExit::HardReconnect;
                        }
                        _ = account_liveness.tick() => {
                            if market_data_take_account_session_reconnect_request() {
                                warn!(
                                    "geyser_account_listener: global ingest stall watchdog requested reconnect"
                                );
                                geyser_metrics_inc_account_listener_liveness_reconnect_total();
                                break 'read SessionExit::HardReconnect;
                            }
                        }
                        maybe_message = stream.next() => {
                            match maybe_message {
                                None => {
                                    geyser_metrics_inc_reconnect(GeyserReconnectReason::StreamEnded);
                                    let now = std::time::Instant::now();
                                    if now.duration_since(last_stream_ended_log)
                                        >= Duration::from_secs(1)
                                    {
                                        warn!(
                                            "geyser_listener: stream ended; resubscribing after short backoff"
                                        );
                                        last_stream_ended_log = now;
                                    }
                                    let sleep_ms = rand::thread_rng().gen_range(50..=200);
                                    sleep(Duration::from_millis(sleep_ms)).await;
                                    break 'read SessionExit::StreamEnded;
                                }
                                Some(Err(e)) => {
                                    error!(error = %e, "geyser_listener: stream error");
                                    geyser_metrics_inc_stream_error();
                                    geyser_metrics_inc_reconnect(GeyserReconnectReason::StreamError);
                                    break 'read SessionExit::HardReconnect;
                                }
                                Some(Ok(msg)) => {
                                    geyser_metrics_inc_listener_stream_payload();
                                    if let Some(update) = msg.update_oneof {
                                        match update {
                                UpdateOneof::Account(account_update) => {
                                    account_update_count += 1;
                                    geyser_metrics_inc_account_listener_account_updates_total();

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

                                        if let Some(ref cf) = tracked_cuckoo_filter {
                                            let passes_owner = program_ids.contains(&owner);
                                            let passes_tracked =
                                                cf.contains(Self::cuckoo_pubkey(pubkey));
                                            if !(passes_owner || passes_tracked) {
                                                continue;
                                            }
                                        }

                                        let grpc_recv_at = Instant::now();
                                        let event = GeyserAccountUpdate {
                                            pubkey,
                                            slot: account_update.slot,
                                            owner,
                                            data: account_info.data,
                                            lamports: account_info.lamports,
                                            grpc_recv_at,
                                        };

                                        // Broadcast to subscribers
                                        let _ = account_tx.send(event);
                                        if !got_payload_since_subscribe {
                                            got_payload_since_subscribe = true;
                                            reconnect_backoff_ms =
                                                rand::thread_rng().gen_range(100..=250);
                                        }

                                        if account_update_count % 1000 == 0 {
                                            info!(
                                                total_updates = account_update_count,
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
                                    // Slots, etc. - ignore
                                }
                                        }
                                    }
                                }
                            }
                        },
                        new_list = tracked_changed_fut => {
                            if new_list != tracked_accounts_current {
                                    let prev_len = tracked_accounts_current.len();
                                    let _old_list =
                                        std::mem::replace(&mut tracked_accounts_current, new_list);
                                    if let Some(ref mut cf) = tracked_cuckoo_filter {
                                        if let Err(e) = Self::sync_tracked_cuckoo_filter(
                                            cf,
                                            tracked_cuckoo_max_capacity.max(1),
                                            &tracked_accounts_current,
                                        ) {
                                            error!(
                                                error = %e,
                                                "geyser_listener: failed to rebuild CompressedAccountFilterSet"
                                            );
                                            return Err(e);
                                        }
                                    }
                                    let threshold = full_reconnect_tracked_threshold
                                        .load(Ordering::Relaxed);
                                    let now = Instant::now();
                                    let new_len = tracked_accounts_current.len();
                                    let jump = new_len.saturating_sub(prev_len);
                                    if jump > Self::TRACKED_SET_JUMP_REBUILD_THRESHOLD
                                        && last_tracked_subscription_change
                                            .map(|t| {
                                                now.saturating_duration_since(t)
                                                    < Duration::from_secs(1)
                                            })
                                            .unwrap_or(true)
                                    {
                                        last_tracked_subscription_change = Some(now);
                                        info!(
                                            tracked_accounts = new_len,
                                            jump,
                                            "geyser_listener: tracked set jump — forcing full reconnect (skip in-place subscribe)"
                                        );
                                        break 'read SessionExit::SubscriptionRebuild;
                                    }
                                    last_tracked_subscription_change = Some(now);
                                    let over_threshold =
                                        tracked_accounts_current.len() > threshold;
                                    let allow_full_rebuild = over_threshold
                                        && last_large_set_subscription_rebuild
                                            .map(|t| {
                                                now.saturating_duration_since(t)
                                                    >= LARGE_TRACKED_SUBSCRIBE_REBUILD_MIN_INTERVAL
                                            })
                                            .unwrap_or(true);
                                    if allow_full_rebuild {
                                        last_large_set_subscription_rebuild = Some(now);
                                        info!(
                                            tracked_accounts = tracked_accounts_current.len(),
                                            threshold,
                                            "geyser_listener: large tracked set — forcing full reconnect (skip in-place subscribe update)"
                                        );
                                        break 'read SessionExit::SubscriptionRebuild;
                                    }
                                    let updated_request = build_account_subscribe_request(
                                        &program_ids,
                                        tracked_cuckoo_filter.as_mut(),
                                    );
                                    Self::coalesce_pending_subscription(
                                        &pending_subscription_request,
                                        updated_request,
                                    );
                                    subscription_notify.notify_one();
                                    geyser_metrics_inc_account_listener_subscribe_updates_total();
                                    warn!(
                                        tracked_accounts = tracked_accounts_current.len(),
                                        "geyser_listener: subscription updated (NO reconnect; async sink)"
                                    );
                                }
                        }
                    };
                };

                subscription_updater_jh.abort();

                match session_exit {
                    SessionExit::StreamEnded => {
                        continue 'same_client;
                    }
                    SessionExit::HardReconnect => {
                        geyser_metrics_set_account_session_connected(false);
                        let sleep_ms = geyser_reconnect_sleep_ms(reconnect_backoff_ms);
                        sleep(Duration::from_millis(sleep_ms)).await;
                        reconnect_backoff_ms =
                            (reconnect_backoff_ms.saturating_mul(2)).min(RECONNECT_BACKOFF_CAP_MS);
                        continue 'outer;
                    }
                    SessionExit::SubscriptionRebuild => {
                        geyser_metrics_inc_reconnect(GeyserReconnectReason::SubscriptionRebuild);
                        geyser_metrics_set_account_session_connected(false);
                        // Intentional full reconnect for large subscription churn — no error backoff sleep
                        // in this arm. Still reset reconnect jitter so a *prior* HardReconnect cannot leave a
                        // stale 60s-class backoff applied to the next outer connect attempt.
                        reconnect_backoff_ms = rand::thread_rng().gen_range(100..=250);
                        continue 'outer;
                    }
                }
            }
        }
    }
}

#[cfg(all(test, not(windows)))]
mod geyser_resilience_tests {
    use super::{
        build_account_subscribe_request, build_tx_subscribe_request, geyser_reconnect_sleep_ms,
        GeyserAccountListener, SubscriptionSinkPushOutcome, SUBSCRIBE_SINK_MIN_INTERVAL,
        SUBSCRIBE_SINK_SEND_TIMEOUT,
    };
    use futures::SinkExt;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};
    use tokio::sync::Notify;
    use tokio::time::sleep;
    use yellowstone_grpc_proto::cuckoo::CompressedAccountFilterSet;
    use yellowstone_grpc_proto::prelude::SubscribeRequest;

    #[test]
    fn build_tx_subscribe_has_dex_transactions_empty_accounts() {
        let pk = solana_sdk::pubkey::Pubkey::new_from_array([7u8; 32]);
        let req = build_tx_subscribe_request(std::slice::from_ref(&pk));
        assert!(!req.transactions.is_empty());
        assert!(req.accounts.is_empty());
        assert!(!req
            .accounts
            .contains_key(super::TRACKED_CUCKOO_SUBSCRIBE_NAME));
    }

    #[test]
    fn build_account_subscribe_has_accounts_empty_transactions() {
        let pk = solana_sdk::pubkey::Pubkey::new_from_array([7u8; 32]);
        let req = build_account_subscribe_request(std::slice::from_ref(&pk), None);
        assert!(req.transactions.is_empty());
        assert!(!req.accounts.is_empty());
    }

    #[test]
    fn subscribe_request_coalesce_latest_wins() {
        use yellowstone_grpc_proto::prelude::SubscribeRequest;
        let pending = Mutex::new(None::<SubscribeRequest>);
        let pk = solana_sdk::pubkey::Pubkey::new_from_array([1u8; 32]);
        let req_a = build_account_subscribe_request(std::slice::from_ref(&pk), None);
        let req_b = build_account_subscribe_request(&[], None);
        GeyserAccountListener::coalesce_pending_subscription(&pending, req_a);
        GeyserAccountListener::coalesce_pending_subscription(&pending, req_b);
        let taken = pending.lock().unwrap().take().expect("pending req");
        assert!(taken.accounts.is_empty());
    }

    #[test]
    fn reconnect_sleep_ms_adds_bounded_jitter() {
        for _ in 0..20 {
            let s = geyser_reconnect_sleep_ms(500);
            assert!((500..=650).contains(&s), "s={s}");
        }
    }

    #[tokio::test]
    async fn r3_ten_subscription_notifies_coalesce_to_one_send() {
        let pending: Arc<Mutex<Option<SubscribeRequest>>> = Arc::new(Mutex::new(None));
        let notify = Arc::new(Notify::new());
        let send_count = Arc::new(AtomicUsize::new(0));
        let (mut sink_tx, _sink_rx) = futures::channel::mpsc::channel::<SubscribeRequest>(64);

        let pending_u = Arc::clone(&pending);
        let notify_u = Arc::clone(&notify);
        let send_count_u = Arc::clone(&send_count);
        let updater = tokio::spawn(async move {
            let mut last_send = Instant::now() - SUBSCRIBE_SINK_MIN_INTERVAL;
            loop {
                notify_u.notified().await;
                loop {
                    let elapsed = last_send.elapsed();
                    let min_interval = SUBSCRIBE_SINK_MIN_INTERVAL;
                    if elapsed < min_interval {
                        sleep(min_interval - elapsed).await;
                    }
                    let req = match pending_u.lock() {
                        Ok(mut g) => g.take(),
                        Err(_) => continue,
                    };
                    let Some(req) = req else {
                        break;
                    };
                    if sink_tx.send(req).await.is_ok() {
                        send_count_u.fetch_add(1, Ordering::Relaxed);
                        last_send = Instant::now();
                    }
                    let has_more = pending_u.lock().map(|g| g.is_some()).unwrap_or(false);
                    if !has_more {
                        break;
                    }
                }
            }
        });

        let pk = solana_sdk::pubkey::Pubkey::new_from_array([3u8; 32]);
        for _ in 0..10 {
            let req = build_account_subscribe_request(std::slice::from_ref(&pk), None);
            GeyserAccountListener::coalesce_pending_subscription(&pending, req);
            notify.notify_one();
        }

        sleep(Duration::from_millis(700)).await;
        let sends = send_count.load(Ordering::Relaxed);
        assert_eq!(
            sends, 1,
            "expected exactly one sink send for 10 coalesced notifies within 500ms window, got {sends}"
        );
        updater.abort();
    }

    #[tokio::test]
    async fn r3_blocked_sink_send_times_out_after_two_seconds() {
        let (mut sink_tx, _sink_rx) = futures::channel::mpsc::channel::<SubscribeRequest>(1);
        let req = build_account_subscribe_request(&[], None);
        sink_tx.send(req.clone()).await.expect("prime channel");
        let started = Instant::now();
        let outcome = GeyserAccountListener::push_subscribe_request_to_sink(
            &mut sink_tx,
            req,
            SUBSCRIBE_SINK_SEND_TIMEOUT,
        )
        .await;
        assert!(matches!(outcome, SubscriptionSinkPushOutcome::TimedOut));
        assert!(
            started.elapsed() >= SUBSCRIBE_SINK_SEND_TIMEOUT,
            "timeout should not return early"
        );
    }

    #[tokio::test]
    async fn r3_blocked_sink_send_notifies_reconnect_path() {
        let sink_fail = Arc::new(Notify::new());
        let sink_fail_updater = Arc::clone(&sink_fail);
        let (mut sink_tx, _sink_rx) = futures::channel::mpsc::channel::<SubscribeRequest>(1);
        let req = build_account_subscribe_request(&[], None);
        sink_tx.send(req.clone()).await.expect("prime channel");

        let jh = tokio::spawn(async move {
            let outcome = GeyserAccountListener::push_subscribe_request_to_sink(
                &mut sink_tx,
                req,
                SUBSCRIBE_SINK_SEND_TIMEOUT,
            )
            .await;
            if matches!(outcome, SubscriptionSinkPushOutcome::TimedOut) {
                sink_fail_updater.notify_one();
            }
        });

        let notified = tokio::time::timeout(Duration::from_secs(3), sink_fail.notified()).await;
        assert!(notified.is_ok(), "TimedOut should notify reconnect");
        jh.await.expect("updater task");
    }

    #[test]
    fn subscribe_request_includes_single_tracked_cuckoo_filter() {
        let mut cuckoo = CompressedAccountFilterSet::with_capacity(16).unwrap();
        let pk = solana_pubkey::Pubkey::new_from_array([9u8; 32]);
        cuckoo.insert(pk).unwrap();
        let req = build_account_subscribe_request(&[], Some(&mut cuckoo));
        let f = req
            .accounts
            .get("tracked_accounts_cuckoo")
            .expect("tracked cuckoo filter");
        assert!(f.cuckoo_accounts_filter.is_some());
        assert!(f.account.is_empty());
        assert!(f.owner.is_empty());
    }
}
