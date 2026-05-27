//! Geyser gRPC listener for real-time account updates
//! Uses Agave 3.0's native Geyser integration for <10ms latency

use anyhow::{anyhow, Result};
use solana_sdk::pubkey::Pubkey;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::broadcast;
use tokio::sync::watch;

#[cfg(not(windows))]
use futures::{SinkExt, StreamExt};
#[cfg(not(windows))]
use solana_pubkey::Pubkey as CuckooPubkey;
#[cfg(not(windows))]
use solana_sdk::bs58;
#[cfg(not(windows))]
use std::collections::HashMap;
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
    geyser_metrics_inc_reconnect, geyser_metrics_inc_stream_error,
    geyser_metrics_inc_tracked_cuckoo_table_full, geyser_metrics_set_connected,
    GeyserReconnectReason,
};
#[cfg(not(windows))]
use rand::Rng;
#[cfg(not(windows))]
use std::time::Duration;
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

pub struct GeyserListener {
    endpoint: String,
    program_ids: Vec<Pubkey>,
    /// Optional dynamic list of additional accounts (e.g. SPL mint pubkeys) to subscribe to.
    ///
    /// This is implemented via explicit `account` filters (not owner-wide token program
    /// subscriptions), so it scales with tracked tokens rather than chain-wide token activity.
    tracked_accounts_rx: Option<watch::Receiver<Vec<Pubkey>>>,
    /// When `tracked_accounts.len()` exceeds this value, account-list updates use a full outer
    /// gRPC reconnect instead of in-place `subscribe_tx.send` (PR-B Yellowstone churn control).
    /// Use `usize::MAX` to always prefer in-place updates. Shared so market-data can hot-reload.
    full_reconnect_tracked_threshold: Arc<AtomicUsize>,
    /// Max capacity for [`CompressedAccountFilterSet`] (Yellowstone cuckoo wire filter). Must be
    /// at least the configured `max_tracked_accounts` peak. `0` disables cuckoo (legacy explicit
    /// chunks — not used when `new_with_tracked_accounts` passes a positive cap from market-data).
    tracked_cuckoo_max_capacity: usize,
    #[cfg_attr(windows, allow(dead_code))]
    account_tx: broadcast::Sender<GeyserAccountUpdate>,
    #[cfg_attr(windows, allow(dead_code))]
    transaction_tx: broadcast::Sender<GeyserTransactionUpdate>,
    #[cfg_attr(windows, allow(dead_code))]
    blockhash_tx: broadcast::Sender<GeyserBlockhashUpdate>,
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
        broadcast::Receiver<GeyserBlockhashUpdate>,
    ) {
        // Large buffer for high-frequency updates
        let (account_tx, account_rx) = broadcast::channel(200000);
        let (transaction_tx, transaction_rx) = broadcast::channel(50000);
        let (blockhash_tx, blockhash_rx) = broadcast::channel(100);

        (
            Self {
                endpoint,
                program_ids,
                tracked_accounts_rx: None,
                full_reconnect_tracked_threshold: Arc::new(AtomicUsize::new(usize::MAX)),
                tracked_cuckoo_max_capacity: 0,
                account_tx,
                transaction_tx,
                blockhash_tx,
            },
            account_rx,
            transaction_rx,
            blockhash_rx,
        )
    }

    /// Create a new Geyser listener with an additional dynamic tracked-account subscription.
    ///
    /// When the provided watch channel updates, the listener will resubscribe to include the
    /// new account list.
    pub fn new_with_tracked_accounts(
        endpoint: String,
        program_ids: Vec<Pubkey>,
        tracked_accounts_rx: watch::Receiver<Vec<Pubkey>>,
        full_reconnect_tracked_threshold: Arc<AtomicUsize>,
        tracked_cuckoo_max_capacity: usize,
    ) -> (
        Self,
        broadcast::Receiver<GeyserAccountUpdate>,
        broadcast::Receiver<GeyserTransactionUpdate>,
        broadcast::Receiver<GeyserBlockhashUpdate>,
    ) {
        let (mut listener, account_rx, transaction_rx, blockhash_rx) =
            Self::new(endpoint, program_ids);
        listener.tracked_accounts_rx = Some(tracked_accounts_rx);
        listener.full_reconnect_tracked_threshold = full_reconnect_tracked_threshold;
        listener.tracked_cuckoo_max_capacity = tracked_cuckoo_max_capacity;
        (listener, account_rx, transaction_rx, blockhash_rx)
    }

    /// Start listening to Geyser stream
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
            self.start_impl().await
        }
    }

    /// Name of the single Yellowstone `cuckoo_accounts_filter` entry for explicit tracked accounts.
    #[cfg(not(windows))]
    const TRACKED_CUCKOO_SUBSCRIBE_NAME: &'static str = "tracked_accounts_cuckoo";

    #[cfg(not(windows))]
    #[inline]
    fn cuckoo_pubkey(pk: Pubkey) -> CuckooPubkey {
        CuckooPubkey::new_from_array(pk.to_bytes())
    }

    /// Rebuild the persistent cuckoo filter from the full explicit tracked list.
    ///
    /// Server-side updates stay compact (single cuckoo blob); rebuilding here is O(n) on the
    /// client but avoids partial-failure corruption from incremental cuckoo mutations.
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

    /// Build a SubscribeRequest for the given programs and optional tracked-account cuckoo filter.
    /// Extracted so it can be reused for initial subscription and incremental updates.
    #[cfg(not(windows))]
    pub(crate) fn build_subscribe_request(
        program_ids: &[Pubkey],
        tracked_cuckoo: Option<&mut CompressedAccountFilterSet>,
    ) -> SubscribeRequest {
        let mut accounts_filter = HashMap::new();
        let mut transactions_filter = HashMap::new();

        for (idx, program_id) in program_ids.iter().enumerate() {
            // Account updates for pool state changes
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

            // Transactions for swaps / pool creation
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

        // Subscribe to blocks_meta for confirmed blockhash streaming
        let blocks_meta_filter =
            HashMap::from([("blockhash".to_string(), SubscribeRequestFilterBlocksMeta {})]);

        let mut req = SubscribeRequest {
            accounts: accounts_filter,
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
        };

        if let Some(cuck) = tracked_cuckoo {
            cuck.insert_into_subscribe_request(&mut req, Self::TRACKED_CUCKOO_SUBSCRIBE_NAME);
        }

        req
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

    #[cfg(not(windows))]
    async fn start_impl(self) -> Result<()> {
        enum SessionExit {
            StreamEnded,
            HardReconnect,
            /// PR-B: large explicit subscription set — reconnect with a fresh client.
            SubscriptionRebuild,
        }

        info!(
            endpoint = %self.endpoint,
            programs = self.program_ids.len(),
            "geyser_listener: connecting to Geyser gRPC (resilient mode)"
        );

        let mut tracked_accounts_rx = self.tracked_accounts_rx;
        let mut tracked_accounts_current: Vec<Pubkey> = tracked_accounts_rx
            .as_ref()
            .map(|rx| rx.borrow().clone())
            .unwrap_or_default();

        let mut tracked_cuckoo_filter: Option<CompressedAccountFilterSet> = if tracked_accounts_rx
            .is_some()
        {
            let cap = self.tracked_cuckoo_max_capacity.max(1);
            match Self::tracked_cuckoo_from_full_list(cap, &tracked_accounts_current) {
                Ok(f) => Some(f),
                Err(e) => {
                    error!(
                        error = %e,
                        capacity = cap,
                        n = tracked_accounts_current.len(),
                        "geyser_listener: initial CompressedAccountFilterSet build failed"
                    );
                    return Err(anyhow!(
                            "geyser_listener: cannot build CompressedAccountFilterSet: {e}; increase max_tracked_accounts"
                        ));
                }
            }
        } else {
            None
        };

        let mut account_update_count = 0u64;
        let mut transaction_count = 0u64;
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

        geyser_metrics_set_connected(false);

        'outer: loop {
            let mut client = loop {
                match GeyserGrpcClient::build_from_shared(self.endpoint.clone())
                    .map_err(|e| anyhow!("Failed to build Geyser client: {}", e))?
                    .connect()
                    .await
                {
                    Ok(c) => {
                        info!("geyser_listener: connected successfully");
                        geyser_metrics_set_connected(true);
                        break c;
                    }
                    Err(e) => {
                        error!(error = %e, "geyser_listener: connect failed, retrying");
                        geyser_metrics_set_connected(false);
                        let sleep_ms = Self::geyser_reconnect_sleep_ms(reconnect_backoff_ms);
                        sleep(Duration::from_millis(sleep_ms)).await;
                        reconnect_backoff_ms =
                            (reconnect_backoff_ms.saturating_mul(2)).min(RECONNECT_BACKOFF_CAP_MS);
                    }
                }
            };

            'same_client: loop {
                let request = Self::build_subscribe_request(
                    &self.program_ids,
                    tracked_cuckoo_filter.as_mut(),
                );

                let (mut subscribe_tx, mut stream) =
                    match client.subscribe_with_request(Some(request)).await {
                        Ok(pair) => pair,
                        Err(e) => {
                            warn!(
                                error = %e,
                                "geyser_listener: subscribe failed; reconnecting with new client"
                            );
                            geyser_metrics_inc_reconnect(GeyserReconnectReason::StreamError);
                            geyser_metrics_set_connected(false);
                            let sleep_ms = Self::geyser_reconnect_sleep_ms(reconnect_backoff_ms);
                            sleep(Duration::from_millis(sleep_ms)).await;
                            reconnect_backoff_ms = (reconnect_backoff_ms.saturating_mul(2))
                                .min(RECONNECT_BACKOFF_CAP_MS);
                            continue 'outer;
                        }
                    };

                info!(
                    programs = ?self.program_ids,
                    tracked_accounts = tracked_accounts_current.len(),
                    "geyser_listener: subscribed"
                );

                let mut got_payload_since_subscribe = false;

                let session_exit = 'read: loop {
                    if last_log.elapsed().as_secs() >= 60 {
                        info!(
                            accounts = account_update_count,
                            transactions = transaction_count,
                            tracked_accounts = tracked_accounts_current.len(),
                            "geyser_listener: heartbeat - still receiving updates"
                        );
                        last_log = std::time::Instant::now();
                    }

                    let tracked_changed_fut = async {
                        if let Some(rx) = &mut tracked_accounts_rx {
                            let _ = rx.changed().await;
                            Some(rx.borrow().clone())
                        } else {
                            std::future::pending::<Option<Vec<Pubkey>>>().await
                        }
                    };

                    tokio::select! {
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

                                        if let Some(ref cf) = tracked_cuckoo_filter {
                                            let passes_owner = self.program_ids.contains(&owner);
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
                                        let _ = self.account_tx.send(event);
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
                                                // FIRST: Check top-level instructions
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

                                                // SECOND: If not found in top-level, check inner instructions (CPIs)
                                                // Pump.fun CREATE is often called via CPI from another program
                                                if instruction_accounts.is_empty() {
                                                    if let Some(meta) = &tx.meta {
                                                        for inner_ix_group in &meta.inner_instructions {
                                                            for inner_ix in &inner_ix_group.instructions
                                                            {
                                                                if let Some(program_pubkey) =
                                                                    account_keys.get(
                                                                        inner_ix.program_id_index
                                                                            as usize,
                                                                    )
                                                                {
                                                                    if self
                                                                        .program_ids
                                                                        .contains(program_pubkey)
                                                                    {
                                                                        // Extract accounts for this inner instruction
                                                                        for &account_idx in
                                                                            &inner_ix.accounts
                                                                        {
                                                                            if let Some(pubkey) =
                                                                                account_keys.get(
                                                                                    account_idx
                                                                                        as usize,
                                                                                )
                                                                            {
                                                                                instruction_accounts
                                                                                    .push(*pubkey);
                                                                            }
                                                                        }
                                                                        // Extract instruction data
                                                                        instruction_data =
                                                                            inner_ix.data.clone();

                                                                        debug!(
                                                                            signature = %signature,
                                                                            inner_ix_index = inner_ix_group.index,
                                                                            program_id = %program_pubkey,
                                                                            "geyser: extracted DEX instruction from INNER instruction (CPI)"
                                                                        );

                                                                        break; // Found our instruction
                                                                    }
                                                                }
                                                            }
                                                            if !instruction_accounts.is_empty() {
                                                                break; // Already found, exit outer loop
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }

                                        // Extract inner instructions for token transfer parsing
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

                                        // Extract token balances for amount calculation
                                        let mut pre_token_balances = Vec::new();
                                        let mut post_token_balances = Vec::new();
                                        let mut pre_balances = Vec::new();
                                        let mut post_balances = Vec::new();
                                        let mut fee_lamports = 0u64;
                                        let mut compute_units_consumed = None;

                                        if let Some(meta) = &tx.meta {
                                            // Extract fee and compute units for priority fee tracking
                                            fee_lamports = meta.fee;
                                            compute_units_consumed = meta.compute_units_consumed;

                                            // Extract native SOL balances (lamports)
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
                                                        // Extract program_id (SPL Token or Token-2022)
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
                                                        // Extract program_id (SPL Token or Token-2022)
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

                                        // Broadcast to subscribers
                                        let _ = self.transaction_tx.send(event);
                                        if !got_payload_since_subscribe {
                                            got_payload_since_subscribe = true;
                                            reconnect_backoff_ms =
                                                rand::thread_rng().gen_range(100..=250);
                                        }

                                        if transaction_count % 100 == 0 {
                                            info!(
                                                total_transactions = transaction_count,
                                                slot = tx_update.slot,
                                                "geyser_listener: processing transactions"
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
                                    let _ = self.blockhash_tx.send(event);
                                    if !got_payload_since_subscribe {
                                        got_payload_since_subscribe = true;
                                        reconnect_backoff_ms = rand::thread_rng().gen_range(100..=250);
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
                        maybe_new = tracked_changed_fut => {
                            if let Some(new_list) = maybe_new {
                                if new_list != tracked_accounts_current {
                                    let _old_list =
                                        std::mem::replace(&mut tracked_accounts_current, new_list);
                                    if let Some(ref mut cf) = tracked_cuckoo_filter {
                                        if let Err(e) = Self::sync_tracked_cuckoo_filter(
                                            cf,
                                            self.tracked_cuckoo_max_capacity.max(1),
                                            &tracked_accounts_current,
                                        ) {
                                            error!(
                                                error = %e,
                                                "geyser_listener: failed to rebuild CompressedAccountFilterSet"
                                            );
                                            return Err(e);
                                        }
                                    }
                                    let threshold = self
                                        .full_reconnect_tracked_threshold
                                        .load(Ordering::Relaxed);
                                    let now = Instant::now();
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
                                    let updated_request = Self::build_subscribe_request(
                                        &self.program_ids,
                                        tracked_cuckoo_filter.as_mut(),
                                    );
                                    if let Err(e) = subscribe_tx.send(updated_request).await {
                                        warn!(
                                            "geyser_listener: failed to send subscription update: {e}; reconnecting with new client"
                                        );
                                        geyser_metrics_inc_reconnect(GeyserReconnectReason::SinkGone);
                                        break 'read SessionExit::HardReconnect;
                                    }
                                    warn!(
                                        tracked_accounts = tracked_accounts_current.len(),
                                        "geyser_listener: subscription updated (NO reconnect)"
                                    );
                                }
                            }
                        }
                    }
                };

                match session_exit {
                    SessionExit::StreamEnded => {
                        continue 'same_client;
                    }
                    SessionExit::HardReconnect => {
                        geyser_metrics_set_connected(false);
                        let sleep_ms = Self::geyser_reconnect_sleep_ms(reconnect_backoff_ms);
                        sleep(Duration::from_millis(sleep_ms)).await;
                        reconnect_backoff_ms =
                            (reconnect_backoff_ms.saturating_mul(2)).min(RECONNECT_BACKOFF_CAP_MS);
                        continue 'outer;
                    }
                    SessionExit::SubscriptionRebuild => {
                        geyser_metrics_inc_reconnect(GeyserReconnectReason::SubscriptionRebuild);
                        geyser_metrics_set_connected(false);
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
    use super::GeyserListener;
    use yellowstone_grpc_proto::cuckoo::CompressedAccountFilterSet;

    #[test]
    fn reconnect_sleep_ms_adds_bounded_jitter() {
        for _ in 0..20 {
            let s = GeyserListener::geyser_reconnect_sleep_ms(500);
            assert!((500..=650).contains(&s), "s={s}");
        }
    }

    #[test]
    fn subscribe_request_includes_single_tracked_cuckoo_filter() {
        let mut cuckoo = CompressedAccountFilterSet::with_capacity(16).unwrap();
        let pk = solana_pubkey::Pubkey::new_from_array([9u8; 32]);
        cuckoo.insert(pk).unwrap();
        let req = GeyserListener::build_subscribe_request(&[], Some(&mut cuckoo));
        let f = req
            .accounts
            .get("tracked_accounts_cuckoo")
            .expect("tracked cuckoo filter");
        assert!(f.cuckoo_accounts_filter.is_some());
        assert!(f.account.is_empty());
        assert!(f.owner.is_empty());
    }
}
