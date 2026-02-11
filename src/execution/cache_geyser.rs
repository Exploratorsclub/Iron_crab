//! Geyser subscription for LivePoolCache
//!
//! This module provides the Geyser subscription task that feeds the LivePoolCache
//! with real-time pool state updates from all supported DEXes.
//!
//! # Usage
//!
//! ```ignore
//! let cache = create_shared_cache();
//! let handle = spawn_cache_geyser_task(geyser_url, cache.clone());
//!
//! // Use cache in TX builder
//! let state = cache.get(&pool_address)?;
//! ```

#[cfg(not(windows))]
use super::live_pool_cache::parse_pool_account;
use super::live_pool_cache::SharedLivePoolCache;
use anyhow::{anyhow, Result};
use solana_sdk::pubkey::Pubkey;
#[cfg(not(windows))]
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::watch;
#[cfg(not(windows))]
use tracing::{debug, warn};
use tracing::{error, info};

#[cfg(not(windows))]
use futures::{SinkExt, StreamExt};
#[cfg(not(windows))]
use std::collections::HashMap;
#[cfg(not(windows))]
use yellowstone_grpc_client::GeyserGrpcClient;
#[cfg(not(windows))]
use yellowstone_grpc_proto::prelude::{
    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
    SubscribeRequestFilterAccounts,
};

// DEX Program IDs (same as in geyser_listener.rs and geyser_pool_discovery.rs)
#[cfg(not(windows))]
const RAYDIUM_AMM_V4: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
#[cfg(not(windows))]
const RAYDIUM_CPMM: &str = "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C";
#[cfg(not(windows))]
const ORCA_WHIRLPOOL: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";
#[cfg(not(windows))]
const METEORA_DLMM: &str = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo";
#[cfg(not(windows))]
const PUMPFUN_PROGRAM: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
#[cfg(not(windows))]
const PUMPFUN_AMM: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
#[cfg(not(windows))]
const TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
#[cfg(not(windows))]
const TOKEN_2022_PROGRAM: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";

/// Configuration for the cache Geyser subscription
#[derive(Clone)]
pub struct CacheGeyserConfig {
    /// Geyser gRPC endpoint
    pub geyser_url: String,

    /// Whether to subscribe to vault accounts for reserve updates
    pub subscribe_vaults: bool,

    /// Maximum chunk size for explicit account subscriptions
    pub vault_chunk_size: usize,
}

impl Default for CacheGeyserConfig {
    fn default() -> Self {
        Self {
            geyser_url: "http://127.0.0.1:10000".to_string(),
            subscribe_vaults: true,
            vault_chunk_size: 500,
        }
    }
}

/// Handle for the Geyser subscription task
pub struct CacheGeyserHandle {
    /// Shutdown signal
    shutdown: Arc<AtomicBool>,

    /// Task handle
    #[allow(dead_code)]
    handle: tokio::task::JoinHandle<()>,
}

impl CacheGeyserHandle {
    /// Request shutdown
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }

    /// Check if running
    pub fn is_running(&self) -> bool {
        !self.handle.is_finished()
    }
}

/// Spawn the Geyser subscription task for cache updates
pub fn spawn_cache_geyser_task(
    config: CacheGeyserConfig,
    cache: SharedLivePoolCache,
    vault_updates_rx: Option<watch::Receiver<Vec<Pubkey>>>,
) -> CacheGeyserHandle {
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = shutdown.clone();

    let handle = tokio::spawn(async move {
        loop {
            if shutdown_clone.load(Ordering::SeqCst) {
                info!("cache_geyser: shutdown requested");
                break;
            }

            match run_cache_subscription(&config, cache.clone(), vault_updates_rx.clone()).await {
                Ok(()) => {
                    info!("cache_geyser: subscription ended normally");
                    break;
                }
                Err(e) => {
                    error!(error = %e, "cache_geyser: subscription failed, reconnecting in 5s");
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                }
            }
        }
    });

    CacheGeyserHandle { shutdown, handle }
}

/// Run the Geyser subscription (internal, reconnects on failure)
#[cfg(not(windows))]
async fn run_cache_subscription(
    config: &CacheGeyserConfig,
    cache: SharedLivePoolCache,
    mut vault_updates_rx: Option<watch::Receiver<Vec<Pubkey>>>,
) -> Result<()> {
    info!(endpoint = %config.geyser_url, "cache_geyser: connecting to Geyser");

    // Connect to Geyser
    let mut client = GeyserGrpcClient::build_from_shared(config.geyser_url.clone())
        .map_err(|e| anyhow!("Failed to build Geyser client: {}", e))?
        .connect()
        .await
        .map_err(|e| anyhow!("Failed to connect to Geyser: {}", e))?;

    info!("cache_geyser: connected successfully");

    // DEX program IDs for pool account subscriptions
    let dex_programs = vec![
        Pubkey::from_str(RAYDIUM_AMM_V4).unwrap(),
        Pubkey::from_str(RAYDIUM_CPMM).unwrap(),
        Pubkey::from_str(ORCA_WHIRLPOOL).unwrap(),
        Pubkey::from_str(METEORA_DLMM).unwrap(),
        Pubkey::from_str(PUMPFUN_PROGRAM).unwrap(),
        Pubkey::from_str(PUMPFUN_AMM).unwrap(),
    ];

    let token_program = Pubkey::from_str(TOKEN_PROGRAM).unwrap();
    let token_2022_program = Pubkey::from_str(TOKEN_2022_PROGRAM).unwrap();

    // Get initial vault list from cache
    let mut current_vaults: Vec<Pubkey> = if config.subscribe_vaults {
        cache.get_tracked_vaults()
    } else {
        vec![]
    };

    // Get initial mint list from cache (for token program detection)
    let mut current_mints: Vec<Pubkey> = cache.get_tracked_mints();

    let mut pool_update_count = 0u64;
    let mut vault_update_count = 0u64;
    let mut mint_update_count = 0u64;
    let mut last_stats_log = std::time::Instant::now();

    loop {
        let request = build_cache_subscribe_request(
            &dex_programs,
            &current_vaults,
            &current_mints,
            config.subscribe_vaults,
            config.vault_chunk_size,
        );

        // Subscribe with bidirectional channel: Sink for updates, Stream for data
        let (mut subscribe_tx, mut stream) = client
            .subscribe_with_request(Some(request))
            .await
            .map_err(|e| anyhow!("Failed to subscribe: {}", e))?;

        info!(
            dex_count = dex_programs.len(),
            vault_count = current_vaults.len(),
            mint_count = current_mints.len(),
            "cache_geyser: subscribed to Geyser (pools + vaults + mints)"
        );

        loop {
            // Log stats every 60 seconds
            if last_stats_log.elapsed().as_secs() >= 60 {
                let stats = cache.stats();
                info!(
                    pools = stats.pool_count,
                    vaults = stats.vault_mappings,
                    pool_updates = pool_update_count,
                    vault_updates = vault_update_count,
                    mint_updates = mint_update_count,
                    hit_rate = format!("{:.1}%", stats.hit_rate() * 100.0),
                    "cache_geyser: stats"
                );
                last_stats_log = std::time::Instant::now();
            }

            // Check for vault list updates (new pools added to cache)
            let vault_changed_fut = async {
                if let Some(rx) = &mut vault_updates_rx {
                    let _ = rx.changed().await;
                    Some(rx.borrow().clone())
                } else {
                    std::future::pending::<Option<Vec<Pubkey>>>().await
                }
            };

            tokio::select! {
                maybe_message = stream.next() => {
                    let Some(message) = maybe_message else {
                        warn!("cache_geyser: stream ended, reconnecting");
                        break;
                    };

                    match message {
                        Ok(msg) => {
                            if let Some(update) = msg.update_oneof {
                                if let UpdateOneof::Account(account_update) = update {
                                    if let Some(account_info) = account_update.account {
                                        // Parse pubkey and owner
                                        let pubkey = match account_info.pubkey.as_slice().try_into() {
                                            Ok(bytes) => Pubkey::new_from_array(bytes),
                                            Err(_) => continue,
                                        };
                                        let owner = match account_info.owner.as_slice().try_into() {
                                            Ok(bytes) => Pubkey::new_from_array(bytes),
                                            Err(_) => continue,
                                        };
                                        let slot = account_update.slot;
                                        let data_len = account_info.data.len();

                                        // Is this a MINT account? (82 bytes, owned by Token Program or Token-2022)
                                        // The owner of the mint account IS the token program!
                                        if data_len == 82 && (owner == token_program || owner == token_2022_program) {
                                            // This mint uses the token program indicated by `owner`
                                            cache.update_mint_program(&pubkey, owner);
                                            mint_update_count += 1;

                                            if mint_update_count % 100 == 0 || owner == token_2022_program {
                                                debug!(
                                                    mint = %pubkey,
                                                    token_program = %owner,
                                                    is_token_2022 = (owner == token_2022_program),
                                                    total = mint_update_count,
                                                    "cache_geyser: mint token program detected"
                                                );
                                            }
                                        }
                                        // Is this a token account (vault balance update)? 165 bytes
                                        else if data_len == 165 && (owner == token_program || owner == token_2022_program) {
                                            // SPL Token account: parse balance at offset 64
                                            if let Ok(balance_bytes) = account_info.data[64..72].try_into() {
                                                let balance = u64::from_le_bytes(balance_bytes);
                                                cache.update_vault_balance(&pubkey, balance, slot);
                                                vault_update_count += 1;

                                                if vault_update_count % 1000 == 0 {
                                                    debug!(
                                                        vault = %pubkey,
                                                        balance,
                                                        total = vault_update_count,
                                                        "cache_geyser: vault balance update"
                                                    );
                                                }
                                            }
                                        } else {
                                            // Try to parse as pool account
                                            if let Some(state) = parse_pool_account(&owner, &account_info.data) {
                                                cache.upsert(pubkey, state, slot);
                                                pool_update_count += 1;

                                                if pool_update_count % 100 == 0 {
                                                    debug!(
                                                        pool = %pubkey,
                                                        slot,
                                                        total = pool_update_count,
                                                        "cache_geyser: pool state update"
                                                    );
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            error!(error = %e, "cache_geyser: stream error");
                            return Err(anyhow!("Stream error: {}", e));
                        }
                    }
                }

                // Vault list updated - send incremental update via Sink
                new_vaults = vault_changed_fut => {
                    if let Some(vaults) = new_vaults {
                        if vaults != current_vaults {
                            info!(
                                old_count = current_vaults.len(),
                                new_count = vaults.len(),
                                "cache_geyser: vault list changed, updating subscription"
                            );
                            current_vaults = vaults;

                            // Also refresh mint list when vault list changes
                            // (new pools discovered = new mints to track)
                            let new_mints = cache.get_tracked_mints();
                            if new_mints.len() != current_mints.len() {
                                info!(
                                    old_mint_count = current_mints.len(),
                                    new_mint_count = new_mints.len(),
                                    "cache_geyser: mint list also changed"
                                );
                                current_mints = new_mints;
                            }

                            let updated_request = build_cache_subscribe_request(
                                &dex_programs,
                                &current_vaults,
                                &current_mints,
                                config.subscribe_vaults,
                                config.vault_chunk_size,
                            );
                            if let Err(e) = subscribe_tx.send(updated_request).await {
                                warn!("cache_geyser: failed to send subscription update: {e}, reconnecting");
                                break; // Fallback: reconnect on Sink send error
                            }
                            info!(
                                vaults = current_vaults.len(),
                                mints = current_mints.len(),
                                "cache_geyser: subscription updated (NO reconnect)"
                            );
                        }
                    }
                }
            }
        }
    }
}

/// Build a SubscribeRequest for cache Geyser subscriptions.
/// Extracted so it can be reused for initial subscription and incremental updates.
#[cfg(not(windows))]
fn build_cache_subscribe_request(
    dex_programs: &[Pubkey],
    current_vaults: &[Pubkey],
    current_mints: &[Pubkey],
    subscribe_vaults: bool,
    vault_chunk_size: usize,
) -> SubscribeRequest {
    let mut accounts_filter = HashMap::new();

    // Subscribe to all DEX program accounts (pool state updates)
    for (idx, program_id) in dex_programs.iter().enumerate() {
        accounts_filter.insert(
            format!("dex_pools_{}", idx),
            SubscribeRequestFilterAccounts {
                account: vec![],
                owner: vec![program_id.to_string()],
                filters: vec![],
                nonempty_txn_signature: None,
            },
        );
    }

    // Subscribe to specific vault accounts (for reserve balance updates)
    if subscribe_vaults && !current_vaults.is_empty() {
        for (chunk_idx, chunk) in current_vaults.chunks(vault_chunk_size).enumerate() {
            accounts_filter.insert(
                format!("vaults_{}", chunk_idx),
                SubscribeRequestFilterAccounts {
                    account: chunk.iter().map(|p| p.to_string()).collect(),
                    owner: vec![],
                    filters: vec![],
                    nonempty_txn_signature: None,
                },
            );
        }
    }

    // Subscribe to specific mint accounts (for token program detection)
    if !current_mints.is_empty() {
        for (chunk_idx, chunk) in current_mints.chunks(vault_chunk_size).enumerate() {
            accounts_filter.insert(
                format!("mints_{}", chunk_idx),
                SubscribeRequestFilterAccounts {
                    account: chunk.iter().map(|p| p.to_string()).collect(),
                    owner: vec![],
                    filters: vec![],
                    nonempty_txn_signature: None,
                },
            );
        }
    }

    SubscribeRequest {
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
    }
}

#[cfg(windows)]
async fn run_cache_subscription(
    _config: &CacheGeyserConfig,
    _cache: SharedLivePoolCache,
    _vault_updates_rx: Option<watch::Receiver<Vec<Pubkey>>>,
) -> Result<()> {
    Err(anyhow!(
        "Geyser gRPC is not supported on Windows. Build on Linux/macOS for Geyser support."
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::live_pool_cache::create_shared_cache;

    #[test]
    fn test_config_default() {
        let config = CacheGeyserConfig::default();
        assert_eq!(config.geyser_url, "http://127.0.0.1:10000");
        assert!(config.subscribe_vaults);
        assert_eq!(config.vault_chunk_size, 500);
    }

    #[tokio::test]
    async fn test_handle_shutdown() {
        let cache = create_shared_cache();
        let config = CacheGeyserConfig {
            geyser_url: "http://invalid:9999".to_string(),
            ..Default::default()
        };

        let handle = spawn_cache_geyser_task(config, cache, None);

        // Should be running initially (even if failing to connect)
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Request shutdown
        handle.shutdown();

        // Give it time to shut down
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}
