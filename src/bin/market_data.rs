//! market-data binary – Data Plane (Geyser ingest + MarketEvents)
//!
//! Source of Truth: docs/TARGET_ARCHITECTURE.md §2.1
//!
//! Responsibilities:
//! - Geyser ingest (preferred), optional RPC/WS fallback
//! - Pool/Account cache (in-memory)
//! - Normalize and publish MarketEvents to NATS
//! - Discovery Worker: detect new mints/pools as events
//!
//! This binary does NOT:
//! - Load wallet keys
//! - Sign or send transactions
//! - Make trading decisions

// Allow holding locks across await - RwLock reads are fast and this simplifies the code.
// TODO: Refactor to use explicit clone-before-await pattern if this causes contention.
#![allow(clippy::await_holding_lock)]

use anyhow::Result;
use clap::Parser;
use solana_sdk::pubkey::Pubkey;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use ironcrab::config::WalletTrackerCfg;
use ironcrab::ipc::{
    BinData, ConfigUpdate, ConfigUpdateResponse, ConfigUpdateStatus, ControlRequest,
    ControlRequestKind, ControlResponse, ControlResponseStatus, ExecutionResult, ExecutionStatus,
    IntentTier, MarketEvent, MarketEventKind, PoolCacheUpdate, PriorityFeePercentiles,
    NATIVE_SOL_MINT,
};
use ironcrab::metrics::{
    serve_metrics, MARKET_EVENTS_PUBLISHED_TOTAL, MARKET_EVENTS_RECEIVED_TOTAL, NATS_ERRORS_TOTAL,
    NATS_MESSAGES_PUBLISHED_TOTAL, POOLS_TRACKED_GAUGE,
};
use ironcrab::nats::{
    config_consumer_config, config_subject, ensure_execution_results_stream,
    ensure_pool_cache_stream, ensure_wallet_snapshot_stream, execution_results_consumer_config,
    pool_subject, wallet_snapshot_consumer_config, wallet_snapshot_subject, NatsClient, NatsConfig,
    CONFIG_STREAM_NAME, EXECUTION_RESULTS_STREAM_NAME, TOPIC_CONTROL_REQUESTS,
    TOPIC_CONTROL_RESPONSES, TOPIC_EXECUTION_RESULTS, TOPIC_MARKET_EVENTS,
    TOPIC_PRIORITY_FEE_SAMPLES, WALLET_SNAPSHOT_STREAM_NAME,
};
use ironcrab::solana::dex::meteora_bin_array_layout::BinArray;
use ironcrab::solana::dex::meteora_dlmm::METEORA_DLMM_PROGRAM;
use ironcrab::solana::dex::meteora_swap_builder::MeteoraDlmmSwapBuilder;
use ironcrab::solana::dex::pumpfun_amm::PumpFunAmmDex;
use ironcrab::solana::dex_parser::{
    parse_account_update, parse_transaction_update_with_pool_lookup, DexType, OrcaPoolInfo,
    ParsedDexEvent,
};
use ironcrab::solana::geyser_pool_discovery::{DexType as PoolDexType, GeyserPoolDiscovery};
use ironcrab::solana::priority_fee_tracker::PriorityFeeTracker;
use ironcrab::solana::rpc::SolanaRpc;
use ironcrab::solana::wallet_tracker::WalletTracker;
use spl_token::solana_program::program_option::COption;
use spl_token::solana_program::program_pack::Pack;
use spl_token_2022::extension::StateWithExtensions;

/// NATS topic for config reload (P1: Runtime Configuration via UI)
const TOPIC_CONFIG_RELOAD: &str = "ironcrab.control.config.reload";
use ironcrab::solana::geyser_listener::GeyserListener;
use ironcrab::storage::{JsonlWriter, JsonlWriterConfig};

/// ExecutionResult dedup: prevents replay storms from re-tracking the same ATA/mint over and over.
///
/// We keep this intentionally simple and bounded (no extra deps).
const EXECUTION_RESULT_DEDUP_CAPACITY: usize = 4096;

// LivePoolCache - MASTER Cache (Single Source of Truth)
use ironcrab::execution::live_pool_cache::{
    parse_pool_account, CachedPoolState, LivePoolCache, PumpAmmState, PumpFunState,
};

// P1 Crash Isolation: Systemd Watchdog support
#[cfg(unix)]
use sd_notify::NotifyState;

/// Build version for decision records
const BUILD_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Known DEX program IDs
const RAYDIUM_AMM_V4: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const RAYDIUM_CPMM: &str = "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C";
const ORCA_WHIRLPOOL: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";
const PUMPFUN_PROGRAM: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
const PUMPFUN_AMM_PROGRAM: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
const METEORA_DLMM: &str = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo";
const METEORA_CPMM: &str = "cpmmpPFsKiR4eeYnGSuXgkhLLgGL1j5FUZoJBJU9t9D";

/// Market data configuration (hot-reloadable via NATS)
#[allow(dead_code)]
#[derive(Debug, Clone)]
struct MarketDataConfig {
    /// Enable Raydium AMM V4 discovery. Default: true
    enable_raydium: bool,
    /// Enable Raydium CPMM discovery. Default: true
    enable_raydium_cpmm: bool,
    /// Enable Orca discovery. Default: true
    enable_orca: bool,
    /// Enable PumpFun bonding curve discovery. Default: true
    enable_pumpfun: bool,
    /// Enable PumpSwap AMM (post-bonding) discovery. Default: true
    enable_pumpswap: bool,
    /// Enable Meteora DLMM discovery. Default: true
    enable_meteora_dlmm: bool,
    /// Enable Meteora CPMM discovery. Default: true
    enable_meteora_cpmm: bool,
    /// Max events per second rate limit. Default: 10000
    max_events_per_sec: u32,
}

impl Default for MarketDataConfig {
    fn default() -> Self {
        Self {
            enable_raydium: true,
            enable_raydium_cpmm: true,
            enable_orca: true,
            enable_pumpfun: true,
            enable_pumpswap: true,
            enable_meteora_dlmm: true,
            enable_meteora_cpmm: true,
            max_events_per_sec: 10_000,
        }
    }
}

#[derive(Parser, Debug)]
#[command(name = "market-data")]
#[command(about = "IronCrab Data Plane – Geyser ingest and MarketEvents publisher")]
struct Args {
    /// Path to configuration file
    #[arg(short, long, default_value = "config.toml")]
    config: PathBuf,

    /// NATS server URL
    #[arg(long, env = "NATS_URL", default_value = "nats://localhost:4222")]
    nats_url: String,

    /// Geyser gRPC endpoint
    #[arg(long, env = "GEYSER_URL", default_value = "http://127.0.0.1:10000")]
    geyser_url: String,

    /// Prometheus metrics port
    #[arg(long, default_value = "9801")]
    metrics_port: u16,

    /// Log directory override
    #[arg(long, env = "IRONCRAB_LOG_DIR")]
    log_dir: Option<PathBuf>,

    /// Dry run: don't publish to NATS
    #[arg(long)]
    dry_run: bool,

    /// Simulate mode: emit fake slot events instead of real Geyser connection
    #[arg(long)]
    simulate: bool,

    /// Publish wallet snapshot once and exit (debug)
    #[arg(long, env = "IRONCRAB_WALLET_SNAPSHOT_ONLY")]
    wallet_snapshot_only: bool,
}

/// Runtime context for market-data
struct MarketDataContext {
    run_id: String,
    /// P1: Config in RwLock for runtime hot-reload
    config: parking_lot::RwLock<MarketDataConfig>,
    nats: Option<NatsClient>,
    jsonl_writer: JsonlWriter,
    event_counter: std::sync::atomic::AtomicU64,
    /// P1: Wallet tracker for smart money / early buyer detection
    wallet_tracker: WalletTracker,

    /// P2: Dynamic Priority Fee Tracker (Geyser-based, NO RPC)
    priority_fee_tracker: Arc<PriorityFeeTracker>,

    /// Tracked token mints for mint-authority/freeze-authority metadata.
    tracked_mints: parking_lot::RwLock<std::collections::HashSet<Pubkey>>,
    tracked_mints_tx: watch::Sender<Vec<Pubkey>>,

    /// Known pump_amm pools (already seen first trade).
    /// We emit PoolCreated + DexPoolAccounts on FIRST trade, then just DexPoolAccounts on subsequent trades.
    /// Key: pool_address
    known_pump_amm_pools: parking_lot::RwLock<std::collections::HashSet<Pubkey>>,

    /// Pools for which we've already emitted DexPoolAccounts from trade parsing.
    known_trade_dex_pools: parking_lot::RwLock<std::collections::HashSet<Pubkey>>,

    /// Vault account tracking for PoolStateUpdate events (Geyser-based reserve balances).
    /// Maps vault_address → VaultInfo (pool context).
    tracked_vaults: parking_lot::RwLock<std::collections::HashMap<Pubkey, VaultInfo>>,
    /// Channel to notify GeyserListener when tracked vaults change (triggers resubscribe).
    tracked_vaults_tx: watch::Sender<Vec<Pubkey>>,

    /// Meteora DLMM Bin Array tracking for BinArrayUpdate events (Geyser-based liquidity).
    /// Maps bin_array_pda → BinArrayInfo (pool context).
    tracked_bin_arrays: parking_lot::RwLock<std::collections::HashMap<Pubkey, BinArrayInfo>>,
    /// Channel to notify GeyserListener when tracked bin arrays change (triggers resubscribe).
    tracked_bin_arrays_tx: watch::Sender<Vec<Pubkey>>,

    /// MASTER LivePoolCache - Single Source of Truth for all pool state.
    /// Updated via Geyser events and propagated to execution-engine via NATS.
    live_pool_cache: Arc<LivePoolCache>,

    /// Creator cache for PumpFun tokens: mint -> creator pubkey.
    /// Populated from PoolCreated events, used to enrich Trade events.
    /// This enables momentum-bot to build intents without RPC calls.
    creator_cache: parking_lot::RwLock<std::collections::HashMap<String, String>>,

    /// Pool to mint mapping for PumpFun bonding curves.
    /// Maps pool_address -> mint. Populated from Trade events and PoolCreated.
    /// Used to look up mint when we receive BondingCurveUpdate (which only has pool_address).
    pool_mint_map: parking_lot::RwLock<std::collections::HashMap<String, String>>,

    /// Pool to creator mapping for PumpFun bonding curves.
    /// Maps pool_address -> creator. Populated from BondingCurveUpdate account events.
    /// Used as secondary lookup when creator_cache (mint -> creator) misses.
    pool_creator_cache: parking_lot::RwLock<std::collections::HashMap<String, String>>,

    /// FIX-29: Raydium pools for which Serum accounts have already been fetched.
    /// Serum accounts are static — one RPC call per pool lifetime is sufficient.
    raydium_serum_fetched: parking_lot::RwLock<std::collections::HashSet<Pubkey>>,

    /// === WsolManager Support: Wallet Balance Tracking ===
    /// Wallet pubkey to track for balance updates (for WsolManager in execution-engine).
    /// Set via IRONCRAB_WALLET_PUBKEY env var.
    tracked_wallet: Option<TrackedWallet>,
    /// Channel to notify GeyserListener when tracked wallet accounts change.
    /// NOTE: We keep the Sender alive even though we don't use it after initial send,
    /// because dropping it would close the Receiver used by the merge task.
    #[allow(dead_code)]
    tracked_wallet_tx: watch::Sender<Vec<Pubkey>>,
    /// Token ATA accounts for the tracked wallet (Geyser subscription list).
    tracked_wallet_token_accounts: parking_lot::RwLock<std::collections::HashSet<Pubkey>>,
    /// Cached mint decimals for tracked wallet tokens.
    tracked_wallet_mint_decimals: parking_lot::RwLock<std::collections::HashMap<Pubkey, u8>>,

    /// Dedup execution results we already processed (in-memory, bounded).
    execution_results_deduper: parking_lot::Mutex<ExecutionResultDeduper>,

    /// Throttling for BondingCurveProgress events: last emitted progress_bps per bonding curve.
    /// Only emit when progress changes by >= 50 bps or complete flag changes.
    last_emitted_curve_progress:
        parking_lot::RwLock<std::collections::HashMap<Pubkey, (u32, bool)>>,
}

#[derive(Debug, Default)]
struct ExecutionResultDeduper {
    order: std::collections::VecDeque<String>,
    seen: std::collections::HashSet<String>,
}

impl ExecutionResultDeduper {
    fn should_process(&mut self, key: &str) -> bool {
        if self.seen.contains(key) {
            return false;
        }
        self.seen.insert(key.to_string());
        self.order.push_back(key.to_string());
        while self.order.len() > EXECUTION_RESULT_DEDUP_CAPACITY {
            if let Some(evicted) = self.order.pop_front() {
                self.seen.remove(&evicted);
            }
        }
        true
    }
}

/// Tracked wallet info for WsolManager balance updates
#[derive(Debug)]
struct TrackedWallet {
    /// The wallet pubkey (owner)
    wallet: Pubkey,
    /// WSOL ATA address
    wsol_ata: Pubkey,
    /// Last known SOL balance (lamports)
    last_sol_balance: std::sync::atomic::AtomicU64,
    /// Last known WSOL balance (lamports)
    last_wsol_balance: std::sync::atomic::AtomicU64,
    /// Whether we've seen a WSOL ATA balance update yet
    wsol_seen: std::sync::atomic::AtomicBool,
}

/// WSOL Mint address constant
const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";

/// Associated Token Program ID
const ASSOCIATED_TOKEN_PROGRAM_ID: &str = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";

impl TrackedWallet {
    fn new(wallet: Pubkey) -> Self {
        // Compute WSOL ATA using known derivation
        let wsol_mint = Pubkey::from_str(WSOL_MINT).expect("valid wsol mint");
        let ata_program =
            Pubkey::from_str(ASSOCIATED_TOKEN_PROGRAM_ID).expect("valid ata program id");
        // Manual ATA derivation to avoid Pubkey type mismatch with spl_associated_token_account
        let (ata, _bump) = Pubkey::find_program_address(
            &[wallet.as_ref(), spl_token::ID.as_ref(), wsol_mint.as_ref()],
            &ata_program,
        );
        Self {
            wallet,
            wsol_ata: ata,
            last_sol_balance: std::sync::atomic::AtomicU64::new(0),
            last_wsol_balance: std::sync::atomic::AtomicU64::new(0),
            wsol_seen: std::sync::atomic::AtomicBool::new(false),
        }
    }
}

/// Information about a tracked vault account
#[derive(Debug)]
struct VaultInfo {
    pool_address: Pubkey,
    dex: String,
    base_mint: Pubkey,
    quote_mint: Pubkey,
    /// true = this vault holds base token, false = quote token
    is_base_vault: bool,
    /// Last known balance (for delta detection)
    last_balance: std::sync::atomic::AtomicU64,
    // =========================================================================
    // DLMM-specific fields (Option D: Bin Array Traversierung)
    // =========================================================================
    /// Meteora DLMM: Active bin index (where current price is)
    active_id: Option<i32>,
    /// Meteora DLMM: Bin step (price increment per bin in bps)
    bin_step: Option<u16>,
}

impl Clone for VaultInfo {
    fn clone(&self) -> Self {
        Self {
            pool_address: self.pool_address,
            dex: self.dex.clone(),
            base_mint: self.base_mint,
            quote_mint: self.quote_mint,
            is_base_vault: self.is_base_vault,
            last_balance: std::sync::atomic::AtomicU64::new(
                self.last_balance.load(std::sync::atomic::Ordering::Relaxed),
            ),
            active_id: self.active_id,
            bin_step: self.bin_step,
        }
    }
}

/// Information about a tracked Meteora DLMM Bin Array account
#[derive(Debug, Clone)]
struct BinArrayInfo {
    pool_address: Pubkey,
    /// Index of this bin array (determines which bins it contains)
    bin_array_index: i64,
    /// Bin step from pool (needed for price calculation)
    bin_step: u16,
}

impl MarketDataContext {
    fn next_event_id(&self) -> String {
        let n = self
            .event_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        format!("evt-{}-{:06}", &self.run_id[..8], n)
    }

    /// P1: Apply config update from control-plane (Runtime Configuration via UI)
    fn apply_config_update(&self, update: &ConfigUpdate) -> ConfigUpdateResponse {
        let mut config = self.config.write();
        let mut applied = Vec::new();
        let mut rejected = Vec::new();

        for (key, value) in &update.config {
            match key.as_str() {
                "enable_raydium" => {
                    if let Some(v) = value.as_bool() {
                        config.enable_raydium = v;
                        applied.push(key.clone());
                        info!(key = %key, new_value = %v, "Config updated");
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected bool".to_string()));
                    }
                }
                "enable_raydium_cpmm" => {
                    if let Some(v) = value.as_bool() {
                        config.enable_raydium_cpmm = v;
                        applied.push(key.clone());
                        info!(key = %key, new_value = %v, "Config updated");
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected bool".to_string()));
                    }
                }
                "enable_orca" => {
                    if let Some(v) = value.as_bool() {
                        config.enable_orca = v;
                        applied.push(key.clone());
                        info!(key = %key, new_value = %v, "Config updated");
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected bool".to_string()));
                    }
                }
                "enable_pumpfun" => {
                    if let Some(v) = value.as_bool() {
                        config.enable_pumpfun = v;
                        applied.push(key.clone());
                        info!(key = %key, new_value = %v, "Config updated");
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected bool".to_string()));
                    }
                }
                "enable_pumpswap" => {
                    if let Some(v) = value.as_bool() {
                        config.enable_pumpswap = v;
                        applied.push(key.clone());
                        info!(key = %key, new_value = %v, "Config updated");
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected bool".to_string()));
                    }
                }
                "enable_meteora_dlmm" => {
                    if let Some(v) = value.as_bool() {
                        config.enable_meteora_dlmm = v;
                        applied.push(key.clone());
                        info!(key = %key, new_value = %v, "Config updated");
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected bool".to_string()));
                    }
                }
                "enable_meteora_cpmm" => {
                    if let Some(v) = value.as_bool() {
                        config.enable_meteora_cpmm = v;
                        applied.push(key.clone());
                        info!(key = %key, new_value = %v, "Config updated");
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected bool".to_string()));
                    }
                }
                "max_events_per_sec" => {
                    if let Some(v) = value.as_u64() {
                        if v > 0 && v <= 1_000_000 {
                            config.max_events_per_sec = v as u32;
                            applied.push(key.clone());
                            info!(key = %key, new_value = %v, "Config updated");
                        } else {
                            rejected.push((key.clone(), "Must be 1-1000000".to_string()));
                        }
                    } else {
                        rejected.push((key.clone(), "Invalid type, expected u64".to_string()));
                    }
                }
                _ => {
                    rejected.push((key.clone(), format!("Unknown config key: {}", key)));
                }
            }
        }

        let status = if rejected.is_empty() {
            ConfigUpdateStatus::Applied
        } else if applied.is_empty() {
            ConfigUpdateStatus::Rejected
        } else {
            ConfigUpdateStatus::PartiallyApplied
        };

        ConfigUpdateResponse {
            status,
            applied_keys: applied,
            rejected_keys: rejected,
            new_snapshot_id: None,
        }
    }
}

fn try_parse_mint_account(
    owner: &Pubkey,
    data: &[u8],
) -> Option<(u8, u64, Option<String>, Option<String>)> {
    if owner.to_bytes() == spl_token::ID.to_bytes() {
        let mint = spl_token::state::Mint::unpack(data).ok()?;
        let mint_authority = match mint.mint_authority {
            COption::Some(p) => Some(p.to_string()),
            COption::None => None,
        };
        let freeze_authority = match mint.freeze_authority {
            COption::Some(p) => Some(p.to_string()),
            COption::None => None,
        };
        Some((mint.decimals, mint.supply, mint_authority, freeze_authority))
    } else if owner.to_bytes() == spl_token_2022::ID.to_bytes() {
        let mint = StateWithExtensions::<spl_token_2022::state::Mint>::unpack(data).ok()?;
        let base = mint.base;
        let mint_authority = match base.mint_authority {
            COption::Some(p) => Some(p.to_string()),
            COption::None => None,
        };
        let freeze_authority = match base.freeze_authority {
            COption::Some(p) => Some(p.to_string()),
            COption::None => None,
        };
        Some((base.decimals, base.supply, mint_authority, freeze_authority))
    } else {
        None
    }
}

/// Parse a Token Account to extract the balance (amount).
/// Works with both spl-token and spl-token-2022 accounts.
fn try_parse_token_account_balance(data: &[u8]) -> Option<u64> {
    // SPL Token Account layout: 165 bytes
    // Offset 64: amount (u64, little-endian)
    if data.len() >= 72 {
        // Standard spl-token Account layout
        let amount_bytes: [u8; 8] = data[64..72].try_into().ok()?;
        Some(u64::from_le_bytes(amount_bytes))
    } else {
        None
    }
}

/// Publish wallet token balance snapshot for position reconciliation.
///
/// Called at market-data startup to provide momentum-bot with current wallet state.
/// This allows momentum-bot to reconcile positions after restarts, detecting:
/// - Manual sales via Phantom/Jupiter (no ExecutionResult)
/// - Emergency liquidations via UI
/// - External transfers
/// - Closed ATAs (Geyser doesn't report deleted accounts)
/// - Tokens bought externally or with broken ATA tracking (owner-scan discovery)
///
/// **Startup RPC calls** (legitimate, NOT in hot-path):
/// 1. `getTokenAccountsByOwner` x2 (SPL Token + Token-2022) — discover unknown tokens
/// 2. `getMultipleAccounts` x1 — verify balances + decimals for all known mints
///
/// After startup, live balance updates are handled by Geyser (tracked ATA subscriptions).
/// No RPC calls are made in the runtime hot-path.
async fn publish_wallet_snapshot(
    ctx: &MarketDataContext,
    rpc: &SolanaRpc,
    wallet: &Pubkey,
    is_periodic: bool,
) -> Result<()> {
    use async_nats::jetstream;
    use futures::StreamExt;
    use std::collections::{HashMap, HashSet};

    // Constraint: At most one RPC roundtrip on restart.
    // In practice this also bounds the tracked accounts we add for wallet tracking.
    const MAX_BOOTSTRAP_MINTS: usize = 30;

    let token_program = Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
        .expect("valid token program");
    let token_2022_program = Pubkey::from_str("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")
        .expect("valid token-2022 program");
    let ata_program =
        Pubkey::from_str(ASSOCIATED_TOKEN_PROGRAM_ID).expect("valid associated token program");

    let wallet_str = wallet.to_string();

    // 1) Discover known mints from JetStream (LastPerSubject) for this wallet.
    //    This avoids any RPC owner-scan and gives us stable coverage over restarts.
    let mut known_mints: Vec<Pubkey> = Vec::new();
    // mint -> (decimals, token_program, last_balance_raw)
    // IMPORTANT: if bootstrap cannot resolve a token account deterministically (non-ATA),
    // we must NOT overwrite a previously correct balance with 0.
    let mut cached_mint_meta: HashMap<Pubkey, (u8, Pubkey, u64)> = HashMap::new();

    if let Some(ref nats) = ctx.nats {
        let js = jetstream::new(nats.client().clone());
        match js.get_stream(WALLET_SNAPSHOT_STREAM_NAME).await {
            Ok(stream) => {
                let mut consumer_config = wallet_snapshot_consumer_config();
                consumer_config.filter_subject =
                    format!("ironcrab.wallet_snapshot.{}.*", wallet_str);
                match stream.create_consumer(consumer_config).await {
                    Ok(consumer) => {
                        // Pull up to N wallet snapshot subjects (bounded).
                        // If there are more, we cap to keep bootstrap to 1 RPC call.
                        let mut messages = consumer
                            .fetch()
                            .max_messages(MAX_BOOTSTRAP_MINTS.saturating_mul(2))
                            .messages()
                            .await?;

                        while let Some(msg) = messages.next().await {
                            let msg = match msg {
                                Ok(m) => m,
                                Err(_) => continue,
                            };
                            let event: MarketEvent = match serde_json::from_slice(&msg.payload) {
                                Ok(e) => e,
                                Err(e) => {
                                    debug!(error = %e, "Wallet snapshot bootstrap: failed to deserialize MarketEvent");
                                    let _ = msg.ack().await;
                                    continue;
                                }
                            };

                            if let MarketEventKind::WalletBalanceSnapshot {
                                mint,
                                balance_raw,
                                decimals,
                                token_program,
                                ..
                            } = &event.kind
                            {
                                if mint.as_str() == WSOL_MINT {
                                    let _ = msg.ack().await;
                                    continue;
                                }
                                if let (Ok(mint_pk), Ok(token_prog_pk)) =
                                    (Pubkey::from_str(mint), Pubkey::from_str(token_program))
                                {
                                    cached_mint_meta.entry(mint_pk).or_insert_with(|| {
                                        known_mints.push(mint_pk);
                                        (*decimals, token_prog_pk, *balance_raw)
                                    });
                                }
                            }
                            let _ = msg.ack().await;
                            if known_mints.len() >= MAX_BOOTSTRAP_MINTS {
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        debug!(error = %e, "Wallet snapshot bootstrap: failed to create consumer");
                    }
                }
            }
            Err(e) => {
                debug!(error = %e, stream = WALLET_SNAPSHOT_STREAM_NAME, "Wallet snapshot bootstrap: stream not found");
            }
        }
    }

    // If JetStream has no prior snapshots (first-ever startup), allow an operator-provided
    // mint list to break the circular dependency ("no known mints" -> no bootstrap).
    if known_mints.is_empty() {
        if let Ok(v) = std::env::var("IRONCRAB_BOOTSTRAP_MINTS") {
            for s in v.split(',').map(|x| x.trim()).filter(|x| !x.is_empty()) {
                match Pubkey::from_str(s) {
                    Ok(m) => known_mints.push(m),
                    Err(e) => {
                        warn!(mint = %s, error = %e, "Invalid IRONCRAB_BOOTSTRAP_MINTS entry")
                    }
                }
            }
            known_mints.sort();
            known_mints.dedup();
        }
    }

    if known_mints.is_empty() {
        info!(
            wallet = %wallet_str,
            "Wallet snapshot bootstrap: no known mints (JetStream empty; no IRONCRAB_BOOTSTRAP_MINTS); publishing empty snapshot complete"
        );
        // Still publish WalletSnapshotComplete and keep Geyser subscriptions warm for wallet + WSOL.
        // Token holdings will be learned event-driven (ExecutionResults + Geyser).
    }

    // 1.5) Owner-Scan Discovery: getTokenAccountsByOwner (startup only)
    //
    // JetStream only knows mints that were previously tracked. If the bot was offline and
    // tokens were bought externally (Phantom, Jupiter) or a previous run had broken ATA
    // tracking, those mints won't be in JetStream.
    //
    // This owner-scan discovers ALL token accounts in the wallet, merges them with known
    // mints, and ensures the startup snapshot reflects the true on-chain wallet state.
    // This is a legitimate startup-only RPC call (2 calls: SPL Token + Token-2022).
    // It runs at every startup but NOT for periodic refreshes.
    // Capture WSOL balance from owner-scan for JetStream bootstrap (set inside if !is_periodic)
    let mut bootstrap_wsol_balance: Option<u64> = None;

    if !is_periodic {
        use solana_client::rpc_request::TokenAccountsFilter;

        let mut discovered_from_owner_scan: Vec<(Pubkey, u64, Pubkey)> = Vec::new(); // (mint, balance, token_program)
        let mut spl_non_zero_count: usize = 0;
        let mut t22_non_zero_count: usize = 0;

        // SPL Token accounts
        match rpc
            .rpc
            .get_token_accounts_by_owner(wallet, TokenAccountsFilter::ProgramId(token_program))
            .await
        {
            Ok(accounts) => {
                for keyed in &accounts {
                    if let solana_account_decoder::UiAccountData::Json(parsed) = &keyed.account.data
                    {
                        if let Some(info) = parsed.parsed.get("info") {
                            let mint_str = info.get("mint").and_then(|v| v.as_str()).unwrap_or("");
                            let balance_str = info
                                .get("tokenAmount")
                                .and_then(|v| v.get("amount"))
                                .and_then(|v| v.as_str())
                                .unwrap_or("0");
                            let decimals_val = info
                                .get("tokenAmount")
                                .and_then(|v| v.get("decimals"))
                                .and_then(|v| v.as_u64())
                                .unwrap_or(6) as u8;
                            if let Ok(mint_pk) = Pubkey::from_str(mint_str) {
                                let balance: u64 = balance_str.parse().unwrap_or(0);
                                if mint_str == WSOL_MINT {
                                    // Capture WSOL balance for JetStream bootstrap
                                    bootstrap_wsol_balance = Some(balance);
                                } else if balance > 0 {
                                    spl_non_zero_count += 1;
                                    discovered_from_owner_scan.push((
                                        mint_pk,
                                        balance,
                                        token_program,
                                    ));
                                    // Also cache decimals for later
                                    cached_mint_meta.entry(mint_pk).or_insert((
                                        decimals_val,
                                        token_program,
                                        0,
                                    ));
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => {
                warn!(program = "spl_token", error = %e, "Wallet snapshot bootstrap: getTokenAccountsByOwner failed");
            }
        }

        // Token-2022 accounts
        match rpc
            .rpc
            .get_token_accounts_by_owner(wallet, TokenAccountsFilter::ProgramId(token_2022_program))
            .await
        {
            Ok(accounts) => {
                for keyed in &accounts {
                    if let solana_account_decoder::UiAccountData::Json(parsed) = &keyed.account.data
                    {
                        if let Some(info) = parsed.parsed.get("info") {
                            let mint_str = info.get("mint").and_then(|v| v.as_str()).unwrap_or("");
                            let balance_str = info
                                .get("tokenAmount")
                                .and_then(|v| v.get("amount"))
                                .and_then(|v| v.as_str())
                                .unwrap_or("0");
                            let decimals_val = info
                                .get("tokenAmount")
                                .and_then(|v| v.get("decimals"))
                                .and_then(|v| v.as_u64())
                                .unwrap_or(6) as u8;
                            if let Ok(mint_pk) = Pubkey::from_str(mint_str) {
                                let balance: u64 = balance_str.parse().unwrap_or(0);
                                if balance > 0 && mint_str != WSOL_MINT {
                                    t22_non_zero_count += 1;
                                    discovered_from_owner_scan.push((
                                        mint_pk,
                                        balance,
                                        token_2022_program,
                                    ));
                                    cached_mint_meta.entry(mint_pk).or_insert((
                                        decimals_val,
                                        token_2022_program,
                                        0,
                                    ));
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => {
                warn!(program = "token_2022", error = %e, "Wallet snapshot bootstrap: getTokenAccountsByOwner failed");
            }
        }

        // A.3: Bootstrap owner-scan diagnostics (non-zero counts)
        info!(
            wallet = %wallet_str,
            spl_non_zero = spl_non_zero_count,
            t22_non_zero = t22_non_zero_count,
            total_discovered = discovered_from_owner_scan.len(),
            known_mints = known_mints.len(),
            cap = MAX_BOOTSTRAP_MINTS,
            "Bootstrap owner-scan: token counts"
        );

        // FIX-37: Owner-scan mints with real balance ALWAYS take priority over stale
        // JetStream entries. Previously, MAX_BOOTSTRAP_MINTS could be filled entirely
        // by stale JetStream snapshots, causing real wallet tokens to be ignored.
        let known_set: HashSet<Pubkey> = known_mints.iter().copied().collect();
        let mut newly_discovered = 0usize;
        for (mint_pk, _balance, _token_prog) in &discovered_from_owner_scan {
            if !known_set.contains(mint_pk) {
                known_mints.push(*mint_pk);
                newly_discovered += 1;
            }
        }

        if newly_discovered > 0 {
            info!(
                wallet = %wallet_str,
                newly_discovered,
                total_known = known_mints.len(),
                cap = MAX_BOOTSTRAP_MINTS,
                "Wallet snapshot bootstrap: owner-scan discovered unknown tokens (bypassing cap for real wallet tokens)"
            );
        } else if !discovered_from_owner_scan.is_empty() {
            debug!(
                wallet = %wallet_str,
                owner_scan_tokens = discovered_from_owner_scan.len(),
                "Wallet snapshot bootstrap: owner-scan found tokens (all already known)"
            );
        }
    }

    // 2) Single RPC roundtrip: fetch mint accounts + derived SPL/2022 ATAs via getMultipleAccounts.
    //
    // Reconciles all known mints (from JetStream + owner-scan discovery) via a single
    // getMultipleAccounts call. This gives us authoritative balance + decimals for every mint.
    fn derive_ata(owner: &Pubkey, mint: &Pubkey, token_prog: &Pubkey, ata_prog: &Pubkey) -> Pubkey {
        let (ata, _bump) = Pubkey::find_program_address(
            &[owner.as_ref(), token_prog.as_ref(), mint.as_ref()],
            ata_prog,
        );
        ata
    }

    let mut accounts_by_key: HashMap<Pubkey, solana_sdk::account::Account> = HashMap::new();
    if !known_mints.is_empty() {
        let mut keys: Vec<Pubkey> = Vec::with_capacity(known_mints.len() * 3);
        for mint in &known_mints {
            keys.push(*mint);
            keys.push(derive_ata(wallet, mint, &token_program, &ata_program));
            keys.push(derive_ata(wallet, mint, &token_2022_program, &ata_program));
        }
        keys.sort();
        keys.dedup();

        let fetched = match rpc.rpc.get_multiple_accounts(&keys).await {
            Ok(v) => v,
            Err(e) => {
                warn!(error = %e, wallet = %wallet_str, "Wallet snapshot bootstrap: getMultipleAccounts failed");
                return Ok(());
            }
        };

        for (idx, maybe_acc) in fetched.into_iter().enumerate() {
            if let Some(acc) = maybe_acc {
                if let Some(pk) = keys.get(idx) {
                    accounts_by_key.insert(*pk, acc);
                }
            }
        }
    }

    // Publish per-mint snapshots (truth from RPC bootstrap).
    let mut mints_in_wallet: Vec<String> = Vec::new();
    let mut wallet_token_accounts: HashSet<Pubkey> = HashSet::new();

    for mint in &known_mints {
        let last_meta = cached_mint_meta.get(mint).copied();

        // Resolve decimals: prefer mint account from RPC; fall back to last persisted snapshot.
        let decimals = accounts_by_key
            .get(mint)
            .and_then(|acc| try_parse_mint_account(&acc.owner, &acc.data).map(|(d, _, _, _)| d))
            .or_else(|| last_meta.map(|(d, _, _)| d))
            .or_else(|| ctx.tracked_wallet_mint_decimals.read().get(mint).copied())
            .unwrap_or_else(|| {
                warn!(
                    mint = %mint,
                    "Bootstrap: decimals unknown for mint; defaulting to 6"
                );
                6
            });
        ctx.tracked_wallet_mint_decimals
            .write()
            .insert(*mint, decimals);

        // Resolve balance via ATA accounts only (no scanning).
        let ata_spl = derive_ata(wallet, mint, &token_program, &ata_program);
        let ata_2022 = derive_ata(wallet, mint, &token_2022_program, &ata_program);

        let mut observed: Option<(u64, Pubkey, Pubkey)> = None; // (amount_raw, ata, token_program)

        if let Some(acc) = accounts_by_key.get(&ata_spl) {
            if acc.owner.to_bytes() == spl_token::ID.to_bytes() {
                if let Ok(ta) = spl_token::state::Account::unpack(&acc.data) {
                    let mint_pk = Pubkey::new_from_array(ta.mint.to_bytes());
                    let owner_pk = Pubkey::new_from_array(ta.owner.to_bytes());
                    if mint_pk == *mint && owner_pk == *wallet {
                        observed = Some((ta.amount, ata_spl, token_program));
                    }
                }
            }
        }
        if observed.is_none() {
            if let Some(acc) = accounts_by_key.get(&ata_2022) {
                if acc.owner.to_bytes() == spl_token_2022::ID.to_bytes() {
                    // Token-2022 accounts may have extensions (data > 165 bytes).
                    // Use StateWithExtensions instead of Pack::unpack to handle this.
                    if let Ok(state) =
                        StateWithExtensions::<spl_token_2022::state::Account>::unpack(&acc.data)
                    {
                        let ta = &state.base;
                        let mint_pk = Pubkey::new_from_array(ta.mint.to_bytes());
                        let owner_pk = Pubkey::new_from_array(ta.owner.to_bytes());
                        if mint_pk == *mint && owner_pk == *wallet {
                            observed = Some((ta.amount, ata_2022, token_2022_program));
                        }
                    }
                }
            }
        }

        let (balance_raw, token_program_used, maybe_ata) = match observed {
            Some((amt, ata, prog)) => (amt, prog, Some(ata)),
            None => {
                // ATA not found on-chain → balance is definitively 0.
                // The bot exclusively uses ATAs (derived via Associated Token Program).
                // If the ATA doesn't exist, the token was sold and the ATA was closed.
                // Previous logic incorrectly preserved stale non-zero balances here,
                // creating permanent ghost positions that could never be cleaned up.
                let prev_balance = last_meta.map(|(_, _, b)| b).unwrap_or(0);
                if prev_balance > 0 {
                    info!(
                        mint = %mint,
                        previous_balance = prev_balance,
                        "Wallet snapshot: ATA not found on-chain, clearing stale balance → 0 (token was sold/transferred)"
                    );
                }
                (
                    0u64,
                    last_meta.map(|(_, p, _)| p).unwrap_or(token_program),
                    None,
                )
            }
        };

        if let Some(ata) = maybe_ata {
            wallet_token_accounts.insert(ata);
        }
        if balance_raw > 0 {
            mints_in_wallet.push(mint.to_string());
        }

        // Ensure mint is tracked so Geyser can publish TokenMintInfo later (no RPC here).
        {
            let mut tracked = ctx.tracked_mints.write();
            if tracked.insert(*mint) {
                let updated: Vec<Pubkey> = tracked.iter().copied().collect();
                let _ = ctx.tracked_mints_tx.send(updated);
            }
        }

        let mint_str = mint.to_string();
        let event = MarketEvent::new(
            "market-data",
            BUILD_VERSION,
            &ctx.run_id,
            format!("wallet_snapshot_bootstrap_{}", mint_str),
            "wallet_bootstrap",
            None, // No slot for RPC bootstrap
            MarketEventKind::WalletBalanceSnapshot {
                mint: mint_str.clone(),
                balance_raw,
                decimals,
                token_program: token_program_used.to_string(),
            },
        );

        // Publish to JetStream only (SSOT for bot state)
        if let Some(ref nats) = ctx.nats {
            let subject = wallet_snapshot_subject(&wallet_str, &mint_str);
            if let Err(e) = nats.jetstream_publish(&subject, &event).await {
                warn!(error = %e, mint = %mint_str, "Failed to publish WalletBalanceSnapshot to JetStream (bootstrap)");
            }
        }

        if let Err(e) = ctx.jsonl_writer.write(&event) {
            warn!(error = %e, "Failed to write WalletBalanceSnapshot (bootstrap) to JSONL");
        }
    }

    // 2.5) Stale JetStream Cleanup: Override ghost entries not covered by bootstrap.
    //
    // MAX_BOOTSTRAP_MINTS limits how many mints are processed above.
    // JetStream may contain entries for mints that were sold/closed in previous runs,
    // but never got their zero-balance override because they exceeded the cap.
    // This step reads ALL remaining JetStream entries and publishes zero-balance
    // overrides for any non-zero mints not already covered. No additional RPC calls.
    if !is_periodic {
        if let Some(ref nats) = ctx.nats {
            let published_mint_set: HashSet<String> =
                known_mints.iter().map(|m| m.to_string()).collect();

            let js = jetstream::new(nats.client().clone());
            match js.get_stream(WALLET_SNAPSHOT_STREAM_NAME).await {
                Ok(stream) => {
                    let mut cleanup_consumer_config = wallet_snapshot_consumer_config();
                    cleanup_consumer_config.filter_subject =
                        format!("ironcrab.wallet_snapshot.{}.*", wallet_str);
                    match stream.create_consumer(cleanup_consumer_config).await {
                        Ok(consumer) => {
                            let mut stale_cleaned = 0usize;
                            let mut total_checked = 0usize;

                            loop {
                                let mut messages =
                                    match consumer.fetch().max_messages(500).messages().await {
                                        Ok(m) => m,
                                        Err(e) => {
                                            warn!(error = %e, "Stale cleanup: fetch failed");
                                            break;
                                        }
                                    };

                                let mut batch_count = 0usize;

                                while let Some(msg) = messages.next().await {
                                    let msg = match msg {
                                        Ok(m) => m,
                                        Err(e) => {
                                            warn!(error = %e, "Stale cleanup: error fetching message");
                                            continue;
                                        }
                                    };

                                    batch_count += 1;
                                    total_checked += 1;

                                    let event: MarketEvent =
                                        match serde_json::from_slice(&msg.payload) {
                                            Ok(e) => e,
                                            Err(_) => {
                                                let _ = msg.ack().await;
                                                continue;
                                            }
                                        };

                                    if let MarketEventKind::WalletBalanceSnapshot {
                                        mint,
                                        balance_raw,
                                        decimals,
                                        token_program: tp,
                                    } = &event.kind
                                    {
                                        if mint != WSOL_MINT
                                            && *balance_raw > 0
                                            && !published_mint_set.contains(mint)
                                        {
                                            // Stale entry: publish zero-balance override
                                            let override_event = MarketEvent::new(
                                                "market-data",
                                                BUILD_VERSION,
                                                &ctx.run_id,
                                                format!("wallet_snapshot_stale_cleanup_{}", mint),
                                                "wallet_bootstrap_stale_cleanup",
                                                None,
                                                MarketEventKind::WalletBalanceSnapshot {
                                                    mint: mint.clone(),
                                                    balance_raw: 0,
                                                    decimals: *decimals,
                                                    token_program: tp.clone(),
                                                },
                                            );

                                            let subject =
                                                wallet_snapshot_subject(&wallet_str, mint);
                                            if let Err(e) = nats
                                                .jetstream_publish(&subject, &override_event)
                                                .await
                                            {
                                                warn!(error = %e, mint = %mint, "Stale cleanup: failed to publish zero-balance override to JetStream");
                                            }

                                            stale_cleaned += 1;
                                            info!(
                                                mint = %mint,
                                                old_balance = *balance_raw,
                                                "Stale cleanup: cleared ghost position (ATA no longer exists, balance → 0)"
                                            );
                                        }
                                    }

                                    let _ = msg.ack().await;
                                }

                                if batch_count < 500 {
                                    break;
                                }
                            }

                            if stale_cleaned > 0 {
                                info!(
                                    stale_cleaned,
                                    total_checked,
                                    "✅ Stale JetStream cleanup: cleared ghost positions from previous runs"
                                );
                            } else if total_checked > 0 {
                                debug!(
                                    total_checked,
                                    published = published_mint_set.len(),
                                    "Stale cleanup: no ghost positions found (all entries are fresh)"
                                );
                            }
                        }
                        Err(e) => {
                            warn!(error = %e, "Stale cleanup: failed to create consumer");
                        }
                    }
                }
                Err(e) => {
                    debug!(error = %e, "Stale cleanup: WALLET_SNAPSHOT stream not found");
                }
            }
        }
    }

    // 3) Update tracked wallet accounts for Geyser subscription (wallet + WSOL + token ATAs).
    if let Some(ref tracked_wallet) = ctx.tracked_wallet {
        let mut accounts: Vec<Pubkey> = Vec::new();
        accounts.push(tracked_wallet.wallet);
        accounts.push(tracked_wallet.wsol_ata);
        // Keep any existing tracked token accounts and add what we learned here.
        {
            let existing = ctx.tracked_wallet_token_accounts.read().clone();
            wallet_token_accounts.extend(existing);
        }
        accounts.extend(wallet_token_accounts.iter().copied());
        accounts.sort();
        accounts.dedup();
        let _ = ctx.tracked_wallet_tx.send(accounts);
        *ctx.tracked_wallet_token_accounts.write() = wallet_token_accounts;
    }

    // 4) WalletSnapshotComplete helps momentum-bot close ghost positions.
    let complete_event = MarketEvent::new(
        "market-data",
        BUILD_VERSION,
        &ctx.run_id,
        format!(
            "wallet_snapshot_complete_bootstrap_{}",
            if is_periodic { "periodic" } else { "startup" }
        ),
        "wallet_bootstrap_complete",
        None,
        MarketEventKind::WalletSnapshotComplete {
            mints_in_wallet: mints_in_wallet.clone(),
            wallet: wallet_str.clone(),
            is_periodic,
        },
    );

    if let Some(ref nats) = ctx.nats {
        if let Err(e) = nats.publish(TOPIC_MARKET_EVENTS, &complete_event).await {
            warn!(error = %e, "Failed to publish WalletSnapshotComplete (bootstrap)");
        }
    }
    if let Err(e) = ctx.jsonl_writer.write(&complete_event) {
        warn!(error = %e, "Failed to write WalletSnapshotComplete (bootstrap) to JSONL");
    }

    // 5) Publish SOL + WSOL as WalletBalanceSnapshot to JetStream (SSOT for bot state).
    //    execution-engine/WsolManager bootstrap from JetStream on startup.
    if !is_periodic {
        if let Some(ref tracked_wallet) = ctx.tracked_wallet {
            if let Some(ref nats) = ctx.nats {
                // Fetch native SOL balance (1 lightweight RPC call during bootstrap)
                match rpc.rpc.get_balance(wallet).await {
                    Ok(sol_lamports) => {
                        // Always send explicit WSOL: Some(0) when no ATA exists, so WsolManager
                        // knows to wrap and JetStream doesn't retain stale WSOL from previous runs.
                        let wsol_balance = bootstrap_wsol_balance.unwrap_or(0);

                        // Seed TrackedWallet so subsequent Geyser events correctly detect changes
                        tracked_wallet
                            .last_sol_balance
                            .store(sol_lamports, Ordering::Relaxed);
                        tracked_wallet
                            .last_wsol_balance
                            .store(wsol_balance, Ordering::Relaxed);
                        tracked_wallet.wsol_seen.store(true, Ordering::Relaxed);

                        // Publish SOL + WSOL as WalletBalanceSnapshot to JetStream (SSOT).
                        // NOTE: Native SOL uses sentinel "NATIVE_SOL" as mint key because
                        // SOL_MINT == WSOL_MINT (same address). Without this, a single
                        // JetStream subject would be shared and one would overwrite the other.
                        {
                            let sol_snapshot = MarketEvent::new(
                                "market-data",
                                BUILD_VERSION,
                                &ctx.run_id,
                                "wallet_snapshot_bootstrap_NATIVE_SOL".to_string(),
                                "wallet_bootstrap",
                                None,
                                MarketEventKind::WalletBalanceSnapshot {
                                    mint: "NATIVE_SOL".to_string(),
                                    balance_raw: sol_lamports,
                                    decimals: 9,
                                    token_program: "system".to_string(),
                                },
                            );
                            let sol_subject = wallet_snapshot_subject(&wallet_str, "NATIVE_SOL");
                            if let Err(e) =
                                nats.jetstream_publish(&sol_subject, &sol_snapshot).await
                            {
                                warn!(error = %e, "Failed to publish native SOL WalletBalanceSnapshot to JetStream");
                            }

                            // Always publish WSOL (including 0) so JetStream has authoritative
                            // state; otherwise LastPerSubject returns stale WSOL from previous runs.
                            let wsol_bal = wsol_balance;
                            let wsol_snapshot = MarketEvent::new(
                                "market-data",
                                BUILD_VERSION,
                                &ctx.run_id,
                                "wallet_snapshot_bootstrap_WSOL".to_string(),
                                "wallet_bootstrap",
                                None,
                                MarketEventKind::WalletBalanceSnapshot {
                                    mint: WSOL_MINT.to_string(),
                                    balance_raw: wsol_bal,
                                    decimals: 9,
                                    token_program: "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
                                        .to_string(),
                                },
                            );
                            let wsol_subject = wallet_snapshot_subject(&wallet_str, WSOL_MINT);
                            if let Err(e) =
                                nats.jetstream_publish(&wsol_subject, &wsol_snapshot).await
                            {
                                warn!(error = %e, "Failed to publish WSOL WalletBalanceSnapshot to JetStream");
                            }

                            info!(
                                wallet = %wallet_str,
                                sol_lamports,
                                wsol_balance,
                                "SOL/WSOL WalletBalanceSnapshot published to JetStream (bootstrap)"
                            );
                        }
                    }
                    Err(e) => {
                        warn!(
                            error = %e,
                            "Failed to fetch SOL balance for bootstrap (will rely on Geyser events)"
                        );
                    }
                }
            }
        }
    }

    info!(
        wallet = %wallet_str,
        known_mints = known_mints.len(),
        mints_in_wallet = mints_in_wallet.len(),
        is_periodic,
        "✅ Wallet snapshot bootstrap published (RPC: getMultipleAccounts + startup owner-scan)"
    );

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("market_data=info".parse()?)
                .add_directive("ironcrab=info".parse()?),
        )
        .init();

    let args = Args::parse();
    let run_id = Uuid::new_v4().to_string();

    let wallet_env = std::env::var("IRONCRAB_WALLET_PUBKEY").ok();
    info!(
        run_id = %run_id,
        config = %args.config.display(),
        geyser_url = %args.geyser_url,
        metrics_port = args.metrics_port,
        wallet_pubkey = ?wallet_env,
        wallet_snapshot_only = args.wallet_snapshot_only,
        "Starting market-data service"
    );

    // Start metrics server
    let metrics_addr = std::net::SocketAddr::from(([0, 0, 0, 0], args.metrics_port));
    tokio::spawn(async move {
        if let Err(e) = serve_metrics(metrics_addr).await {
            error!(error = %e, "Metrics server failed");
        }
    });
    info!(
        port = args.metrics_port,
        "Metrics server started at /metrics"
    );

    // === P0 Check: Ensure no wallet keys are loaded ===
    // market-data is KEYLESS per architecture – exit immediately if keys are detected
    if std::env::var("IRONCRAB_KEYPAIR_JSON").is_ok()
        || std::env::var("IRONCRAB_KEYPAIR_B64").is_ok()
        || std::env::var("IRONCRAB_KEYPAIR_PATH").is_ok()
    {
        error!("ERROR: Wallet key environment variables detected!");
        error!("market-data is KEYLESS per architecture. Remove key variables and restart.");
        error!("Only execution-engine should have access to wallet keys.");
        std::process::exit(1);
    }

    // Setup JSONL writer
    let log_dir = args
        .log_dir
        .unwrap_or_else(|| PathBuf::from("trade_logs/market_events"));
    let jsonl_config = JsonlWriterConfig::new("market_events").with_log_dir(&log_dir);
    let jsonl_writer = JsonlWriter::new(jsonl_config)?;

    info!(log_dir = %log_dir.display(), "JSONL writer initialized");

    // Setup NATS (optional in dry-run mode)
    let nats = if args.dry_run {
        info!("Dry-run mode: NATS publishing disabled");
        None
    } else {
        let config = NatsConfig::new(&args.nats_url, "market-data");
        let mut client = NatsClient::new(config);
        if let Err(e) = client.connect().await {
            warn!(error = %e, "Failed to connect to NATS (continuing without)");
            None
        } else {
            info!(url = %args.nats_url, "Connected to NATS");

            // Initialize JetStream stream for PoolCacheUpdates (persistent state)
            if let Err(e) = ensure_pool_cache_stream(client.client()).await {
                error!(error = %e, "Failed to create/update JetStream POOL_CACHE stream");
                error!("PoolCacheUpdates will not persist across restarts!");
                error!("Check that nats-server is running with -js flag");
            } else {
                info!("JetStream POOL_CACHE stream ready for persistent state recovery");
            }

            // Initialize JetStream stream for WalletBalanceSnapshot (position reconciliation)
            if let Err(e) = ensure_wallet_snapshot_stream(client.client()).await {
                error!(error = %e, "Failed to create/update JetStream WALLET_SNAPSHOT stream");
                error!("WalletBalanceSnapshot persistence disabled!");
                error!("Check that nats-server is running with -js flag");
            } else {
                info!("JetStream WALLET_SNAPSHOT stream ready for position reconciliation");
            }

            // Initialize JetStream stream for ExecutionResults (wallet ATA tracking)
            if let Err(e) = ensure_execution_results_stream(client.client()).await {
                warn!(error = %e, "Failed to create/update JetStream EXECUTION_RESULTS stream");
            } else {
                info!("JetStream EXECUTION_RESULTS stream ready for wallet ATA tracking");
            }

            Some(client)
        }
    };

    // Initialize WalletTracker (P1: Smart Money / Insider Detection)
    // TODO: Load config from file for production
    let wallet_tracker_cfg = WalletTrackerCfg::default();
    let wallet_tracker = WalletTracker::new(wallet_tracker_cfg);
    info!(
        smart_money = wallet_tracker.stats().smart_money_count,
        bad_actors = wallet_tracker.stats().bad_actor_count,
        "WalletTracker initialized"
    );

    let (tracked_mints_tx, tracked_mints_rx) = watch::channel(Vec::<Pubkey>::new());
    let (tracked_vaults_tx, tracked_vaults_rx) = watch::channel(Vec::<Pubkey>::new());
    let (tracked_bin_arrays_tx, tracked_bin_arrays_rx) = watch::channel(Vec::<Pubkey>::new());
    let (tracked_wallet_tx, tracked_wallet_rx) = watch::channel(Vec::<Pubkey>::new());

    // === WsolManager Support: Setup wallet balance tracking ===
    let tracked_wallet = if let Ok(wallet_pubkey_str) = std::env::var("IRONCRAB_WALLET_PUBKEY") {
        match Pubkey::from_str(&wallet_pubkey_str) {
            Ok(wallet_pubkey) => {
                let tracked = TrackedWallet::new(wallet_pubkey);
                info!(
                    wallet = %wallet_pubkey,
                    wsol_ata = %tracked.wsol_ata,
                    "WalletBalance tracking enabled for WsolManager"
                );
                // Send initial tracked accounts (wallet + WSOL ATA)
                let _ = tracked_wallet_tx.send(vec![wallet_pubkey, tracked.wsol_ata]);
                Some(tracked)
            }
            Err(_) => {
                warn!("IRONCRAB_WALLET_PUBKEY is set but not a valid pubkey");
                None
            }
        }
    } else {
        debug!("IRONCRAB_WALLET_PUBKEY not set, WalletBalance tracking disabled");
        None
    };

    let ctx = Arc::new(MarketDataContext {
        run_id: run_id.clone(),
        config: parking_lot::RwLock::new(MarketDataConfig::default()),
        nats,
        jsonl_writer,
        event_counter: std::sync::atomic::AtomicU64::new(0),
        wallet_tracker,
        priority_fee_tracker: Arc::new(PriorityFeeTracker::new()),
        tracked_mints: parking_lot::RwLock::new(std::collections::HashSet::new()),
        tracked_mints_tx,
        known_pump_amm_pools: parking_lot::RwLock::new(std::collections::HashSet::new()),
        known_trade_dex_pools: parking_lot::RwLock::new(std::collections::HashSet::new()),
        tracked_vaults: parking_lot::RwLock::new(std::collections::HashMap::new()),
        tracked_vaults_tx,
        tracked_bin_arrays: parking_lot::RwLock::new(std::collections::HashMap::new()),
        tracked_bin_arrays_tx,
        live_pool_cache: Arc::new(LivePoolCache::new()),
        creator_cache: parking_lot::RwLock::new(std::collections::HashMap::new()),
        pool_mint_map: parking_lot::RwLock::new(std::collections::HashMap::new()),
        pool_creator_cache: parking_lot::RwLock::new(std::collections::HashMap::new()),
        raydium_serum_fetched: parking_lot::RwLock::new(std::collections::HashSet::new()),
        tracked_wallet,
        tracked_wallet_tx,
        tracked_wallet_token_accounts: parking_lot::RwLock::new(std::collections::HashSet::new()),
        tracked_wallet_mint_decimals: parking_lot::RwLock::new(std::collections::HashMap::new()),
        execution_results_deduper: parking_lot::Mutex::new(ExecutionResultDeduper::default()),
        last_emitted_curve_progress: parking_lot::RwLock::new(std::collections::HashMap::new()),
    });

    // === Main Loop: Geyser subscription or simulation ===

    // P1 Crash Isolation: Signal systemd that we're ready
    #[cfg(unix)]
    {
        // NOTE: Do NOT unset NOTIFY_SOCKET here; we need it for Watchdog pings.
        let _ = sd_notify::notify(false, &[NotifyState::Ready]);
        debug!("Sent sd_notify READY to systemd");
    }

    // Keep readiness fresh even when idle.
    ironcrab::metrics::record_activity();

    // P1: Subscribe to Config Updates (Runtime Configuration via UI)
    // Core NATS subscription (for backward compatibility)
    let config_subscription = if let Some(ref nats) = ctx.nats {
        match nats.subscribe(TOPIC_CONFIG_RELOAD).await {
            Ok(sub) => {
                info!(
                    topic = TOPIC_CONFIG_RELOAD,
                    "Subscribed to Config Updates (Core NATS fallback)"
                );
                Some(sub)
            }
            Err(e) => {
                warn!(error = %e, "Failed to subscribe to Config Updates");
                None
            }
        }
    } else {
        None
    };

    // P1: JetStream Config Bootstrap (persisted, solves race condition)
    // Fetch and apply the last config from JetStream before starting the main loop.
    if let Some(ref nats) = ctx.nats {
        use async_nats::jetstream;
        use futures::StreamExt;

        let jetstream = jetstream::new(nats.client().clone());

        match jetstream.get_stream(CONFIG_STREAM_NAME).await {
            Ok(stream) => {
                match stream
                    .create_consumer(config_consumer_config("market-data"))
                    .await
                {
                    Ok(consumer) => {
                        info!(
                            stream = CONFIG_STREAM_NAME,
                            subject = %config_subject("market-data"),
                            "Connected to JetStream Config Updates (persisted)"
                        );

                        // Bootstrap: Try to get the last config from JetStream
                        match consumer.fetch().max_messages(1).messages().await {
                            Ok(mut messages) => {
                                if let Some(Ok(msg)) = messages.next().await {
                                    match serde_json::from_slice::<ConfigUpdate>(&msg.payload) {
                                        Ok(update) => {
                                            info!(
                                                component = %update.target_component,
                                                keys = ?update.config.keys().collect::<Vec<_>>(),
                                                "Bootstrap: Applying config from JetStream"
                                            );
                                            let response = ctx.apply_config_update(&update);
                                            info!(
                                                status = ?response.status,
                                                applied = ?response.applied_keys,
                                                "Bootstrap config applied"
                                            );
                                            if let Err(e) = msg.ack().await {
                                                warn!(error = %e, "Failed to ack bootstrap config");
                                            }
                                        }
                                        Err(e) => {
                                            warn!(error = %e, "Failed to deserialize bootstrap config");
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                debug!(error = %e, "No bootstrap config in JetStream (first run or empty)");
                            }
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "Failed to create JetStream config consumer");
                    }
                }
            }
            Err(e) => {
                debug!(error = %e, stream = CONFIG_STREAM_NAME, "JetStream CONFIG_UPDATES stream not found (control-plane may not be running)");
            }
        }
    }

    if args.simulate {
        info!("Simulation mode: emitting fake slot events");
        run_simulation_loop(ctx.clone(), &run_id, config_subscription).await?;
    } else {
        info!(geyser_url = %args.geyser_url, "Starting Geyser integration");
        run_geyser_loop(
            ctx.clone(),
            &run_id,
            &args.geyser_url,
            config_subscription,
            tracked_mints_rx,
            tracked_vaults_rx,
            tracked_bin_arrays_rx,
            tracked_wallet_rx,
            args.wallet_snapshot_only,
        )
        .await?;
    }

    // Flush JSONL on shutdown
    ctx.jsonl_writer.flush()?;
    info!(run_id = %run_id, "market-data shutdown complete");

    Ok(())
}

/// I-24d: Handle EnsurePumpAmmPoolAccounts Discovery Request.
///
/// Performs RPC-based discovery (Cold Path), updates MASTER cache, publishes
/// JetStream PoolCacheUpdate (SSOT), and ControlResponse (correlation only).
async fn handle_ensure_pump_amm_pool_accounts(
    ctx: &MarketDataContext,
    rpc: &SolanaRpc,
    run_id: &str,
    request_id: &str,
    base_mint_str: &str,
) {
    let base_mint = match Pubkey::from_str(base_mint_str) {
        Ok(p) => p,
        Err(e) => {
            warn!(base_mint = %base_mint_str, error = %e, "EnsurePumpAmmPoolAccounts: invalid base_mint");
            if let Some(ref nats) = ctx.nats {
                publish_control_response(
                    nats,
                    &ctx.run_id,
                    request_id,
                    ControlResponseStatus::Error,
                    None,
                    Some(e.to_string()),
                )
                .await;
            }
            return;
        }
    };

    let dex = PumpFunAmmDex::new_with_cache(
        Arc::new(rpc.clone()),
        ctx.live_pool_cache.clone(),
        true, // allow_rpc_on_miss: Cold Path Discovery
    );

    match dex.pool_accounts_v1_for_base_mint(base_mint).await {
        Ok(Some(accounts)) if accounts.len() >= 14 => {
            let pool_address = accounts[0];
            let pool_address_str = pool_address.to_string();
            let base_mint_str = base_mint.to_string();
            let quote_mint = accounts[3];
            let quote_mint_str = quote_mint.to_string();

            // Update MASTER cache (upsert if not present, then set pool_accounts)
            let state = CachedPoolState::PumpAmm(PumpAmmState {
                base_mint,
                quote_mint,
                pool_base_token_account: accounts[4],
                pool_quote_token_account: accounts[5],
                base_reserve: None,
                quote_reserve: None,
                pool_accounts: accounts.clone(),
                creator: None,
            });
            ctx.live_pool_cache.upsert(pool_address, state, 0);

            // Publish JetStream PoolCacheUpdate (authoritative SSOT)
            if let Some(ref nats) = ctx.nats {
                let mut pool_update = PoolCacheUpdate::new_pool_discovered(
                    "market-data",
                    BUILD_VERSION,
                    run_id,
                    pool_address_str.clone(),
                    "pump_amm".to_string(),
                    base_mint_str.clone(),
                    quote_mint_str,
                    0,
                    0,
                    None,
                    0,
                );
                let mut meta = std::collections::HashMap::new();
                let accounts_str: Vec<String> = accounts.iter().map(|p| p.to_string()).collect();
                meta.insert("pool_accounts".to_string(), accounts_str.join(","));
                pool_update.metadata = Some(meta);
                let subject = pool_subject(&pool_address_str);
                if let Err(e) = nats.jetstream_publish(&subject, &pool_update).await {
                    warn!(error = %e, "EnsurePumpAmmPoolAccounts: Failed to publish PoolCacheUpdate to JetStream");
                } else {
                    info!(
                        pool = %pool_address_str,
                        base_mint = %base_mint_str,
                        "EnsurePumpAmmPoolAccounts: Published PoolCacheUpdate to JetStream"
                    );
                }
            }

            if let Some(ref nats) = ctx.nats {
                publish_control_response(
                    nats,
                    &ctx.run_id,
                    request_id,
                    ControlResponseStatus::Ok,
                    Some(pool_address_str),
                    None,
                )
                .await;
            }
        }
        Ok(Some(_)) => {
            warn!(base_mint = %base_mint_str, "EnsurePumpAmmPoolAccounts: pool_accounts incomplete (<14)");
            if let Some(ref nats) = ctx.nats {
                publish_control_response(
                    nats,
                    &ctx.run_id,
                    request_id,
                    ControlResponseStatus::Error,
                    None,
                    Some("pool_accounts incomplete".to_string()),
                )
                .await;
            }
        }
        Ok(None) => {
            info!(base_mint = %base_mint_str, "EnsurePumpAmmPoolAccounts: pool not found");
            if let Some(ref nats) = ctx.nats {
                publish_control_response(
                    nats,
                    &ctx.run_id,
                    request_id,
                    ControlResponseStatus::NotFound,
                    None,
                    None,
                )
                .await;
            }
        }
        Err(e) => {
            warn!(base_mint = %base_mint_str, error = %e, "EnsurePumpAmmPoolAccounts: discovery failed");
            if let Some(ref nats) = ctx.nats {
                publish_control_response(
                    nats,
                    &ctx.run_id,
                    request_id,
                    ControlResponseStatus::Error,
                    None,
                    Some(e.to_string()),
                )
                .await;
            }
        }
    }
}

async fn publish_control_response(
    nats: &NatsClient,
    run_id: &str,
    request_id: &str,
    status: ControlResponseStatus,
    pool_address: Option<String>,
    message: Option<String>,
) {
    let mut resp = ControlResponse::new(
        "market-data",
        BUILD_VERSION,
        run_id,
        request_id.to_string(),
        "market-data",
        status,
    );
    if let Some(pa) = pool_address {
        resp = resp.with_pool_address(pa);
    }
    if let Some(m) = message {
        resp = resp.with_message(m);
    }
    if let Err(e) = nats.publish(TOPIC_CONTROL_RESPONSES, &resp).await {
        warn!(error = %e, "Failed to publish ControlResponse");
    }
}

/// Run with real Geyser connection
#[allow(clippy::too_many_arguments)]
async fn run_geyser_loop(
    ctx: Arc<MarketDataContext>,
    run_id: &str,
    geyser_url: &str,
    mut config_subscription: Option<ironcrab::nats::NatsSubscription>,
    tracked_mints_rx: watch::Receiver<Vec<Pubkey>>,
    tracked_vaults_rx: watch::Receiver<Vec<Pubkey>>,
    tracked_bin_arrays_rx: watch::Receiver<Vec<Pubkey>>,
    tracked_wallet_rx: watch::Receiver<Vec<Pubkey>>,
    wallet_snapshot_only: bool,
) -> Result<()> {
    // Initialize RPC client for fallback/metadata (prefer local RPC, fallback to Helius)
    let rpc_url =
        std::env::var("SOLANA_RPC_URL").unwrap_or_else(|_| "http://127.0.0.1:8899".to_string()); // Local validator/private RPC preferred
    let rpc = Arc::new(SolanaRpc::new(&rpc_url));
    info!(rpc_url = %rpc_url, "Initialized RPC client for metadata/fallback");

    // === P0: Wallet Balance Snapshot (Position Reconciliation) ===
    // Bootstrap wallet state at startup (max 1 RPC roundtrip) using known mints from JetStream.
    //
    // Runtime tracking is event-driven (Geyser updates + ExecutionResults-triggered ATA tracking).
    // Note: Geyser does NOT send updates for closed/deleted token accounts (Phase 2 addresses this).
    let _wallet_for_reconciliation: Option<Pubkey> = if let Ok(wallet_pubkey_str) =
        std::env::var("IRONCRAB_WALLET_PUBKEY")
    {
        if let Ok(wallet_pubkey) = Pubkey::from_str(&wallet_pubkey_str) {
            info!(wallet = %wallet_pubkey, "📸 Publishing wallet balance snapshot for position reconciliation");
            if let Err(e) = publish_wallet_snapshot(&ctx, &rpc, &wallet_pubkey, false).await {
                warn!(error = %e, "Failed to publish wallet snapshot (continuing anyway)");
            }
            Some(wallet_pubkey)
        } else {
            warn!("IRONCRAB_WALLET_PUBKEY is set but not a valid pubkey");
            None
        }
    } else {
        info!("IRONCRAB_WALLET_PUBKEY not set, skipping wallet snapshot");
        None
    };

    if wallet_snapshot_only {
        info!("Wallet snapshot only mode enabled, exiting after snapshot");
        return Ok(());
    }

    // Mint metadata fetch pipeline:
    // - We add mints to `tracked_mints` when we see them via tx/pool discovery.
    // - Mint accounts often *never change*, so relying on a future Geyser account update
    //   means we may never emit TokenMintInfo (decimals/supply), which strategies need.
    // - Therefore we proactively fetch the mint account once via RPC and emit TokenMintInfo.
    let (mint_info_tx, mut mint_info_rx) = mpsc::unbounded_channel::<MarketEvent>();

    // DEX program IDs to monitor (must match validator account-index)
    let program_ids = vec![
        Pubkey::from_str(RAYDIUM_AMM_V4).expect("valid raydium pubkey"),
        Pubkey::from_str(RAYDIUM_CPMM).expect("valid raydium cpmm pubkey"),
        Pubkey::from_str(ORCA_WHIRLPOOL).expect("valid orca pubkey"),
        Pubkey::from_str(PUMPFUN_PROGRAM).expect("valid pumpfun pubkey"),
        Pubkey::from_str(PUMPFUN_AMM_PROGRAM).expect("valid pumpfun amm pubkey"),
        Pubkey::from_str(METEORA_DLMM).expect("valid meteora dlmm pubkey"),
        Pubkey::from_str(METEORA_CPMM).expect("valid meteora cpmm pubkey"),
    ];

    // Initialize Geyser-based pool discovery (PRIMARY method for pool discovery)
    let (pool_discovery, mut pool_discovery_rx) =
        GeyserPoolDiscovery::new(geyser_url.to_string(), program_ids.clone(), rpc.clone());

    // Spawn pool discovery task
    let _pool_discovery_handle = tokio::spawn(async move {
        if let Err(e) = pool_discovery.start().await {
            error!(error = %e, "GeyserPoolDiscovery crashed");
        }
    });

    // Merge tracked_mints, tracked_vaults, tracked_bin_arrays, and tracked_wallet into a single combined channel.
    // GeyserListener will subscribe to all accounts in the combined list.
    let (combined_tracked_tx, combined_tracked_rx) = watch::channel(Vec::<Pubkey>::new());
    {
        let mut mints_rx = tracked_mints_rx;
        let mut vaults_rx = tracked_vaults_rx;
        let mut bin_arrays_rx = tracked_bin_arrays_rx;
        let mut wallet_rx = tracked_wallet_rx;
        let combined_tx = combined_tracked_tx;
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = mints_rx.changed() => {}
                    _ = vaults_rx.changed() => {}
                    _ = bin_arrays_rx.changed() => {}
                    _ = wallet_rx.changed() => {}
                }
                // Merge all lists
                let mints: Vec<Pubkey> = mints_rx.borrow().clone();
                let vaults: Vec<Pubkey> = vaults_rx.borrow().clone();
                let bin_arrays: Vec<Pubkey> = bin_arrays_rx.borrow().clone();
                let wallet_accounts: Vec<Pubkey> = wallet_rx.borrow().clone();
                let mut combined: Vec<Pubkey> = mints;
                combined.extend(vaults);
                combined.extend(bin_arrays);
                combined.extend(wallet_accounts);
                combined.sort();
                combined.dedup();
                let _ = combined_tx.send(combined);
            }
        });
    }

    // Start legacy GeyserListener for transaction parsing (will be phased out in favor of pool discovery)
    let (listener, mut account_rx, mut transaction_rx, mut blockhash_rx) =
        GeyserListener::new_with_tracked_accounts(
            geyser_url.to_string(),
            program_ids,
            combined_tracked_rx,
        );

    // Spawn Geyser listener task
    let listener_handle = tokio::spawn(async move {
        if let Err(e) = listener.start().await {
            error!(error = %e, "Geyser listener crashed");
        }
    });

    // Graceful shutdown handling
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    let mut account_count = 0u64;
    let mut tx_count = 0u64;
    let mut last_heartbeat = std::time::Instant::now();
    let mut activity_interval = tokio::time::interval(std::time::Duration::from_secs(10));

    // JetStream consumer for execution results (wallet ATA tracking)
    //
    // This is the trigger that makes wallet tracking "just work" after a BUY:
    // execution-engine already knows token_account/token_program/mint_decimals deterministically.
    let execution_js_consumer = if let Some(ref nats) = ctx.nats {
        use async_nats::jetstream;

        let jetstream = jetstream::new(nats.client().clone());
        match jetstream.get_stream(EXECUTION_RESULTS_STREAM_NAME).await {
            Ok(stream) => match stream
                .create_consumer(execution_results_consumer_config("market-data"))
                .await
            {
                Ok(consumer) => {
                    info!(
                        stream = EXECUTION_RESULTS_STREAM_NAME,
                        topic = TOPIC_EXECUTION_RESULTS,
                        "Subscribed to ExecutionResults via JetStream for wallet ATA tracking"
                    );
                    Some(consumer)
                }
                Err(e) => {
                    warn!(error = %e, "Failed to create execution results consumer (wallet ATA auto-tracking disabled)");
                    None
                }
            },
            Err(e) => {
                warn!(
                    error = %e,
                    stream = EXECUTION_RESULTS_STREAM_NAME,
                    "Failed to get execution results stream"
                );
                None
            }
        }
    } else {
        None
    };

    // I-24d: Subscribe to ControlRequests for Discovery Request/Reply (PumpSwap pool_accounts).
    // Only process requests with target = "market-data".
    let mut control_subscription = if let Some(ref nats) = ctx.nats {
        match nats.subscribe(TOPIC_CONTROL_REQUESTS).await {
            Ok(sub) => {
                info!(
                    topic = TOPIC_CONTROL_REQUESTS,
                    "Subscribed to ControlRequests (Discovery)"
                );
                Some(sub)
            }
            Err(e) => {
                warn!(error = %e, "Failed to subscribe to ControlRequests");
                None
            }
        }
    } else {
        None
    };

    loop {
        tokio::select! {
            // I-24d: ControlRequests (EnsurePumpAmmPoolAccounts Discovery)
            msg = async {
                if let Some(ref mut sub) = control_subscription {
                    sub.next().await
                } else {
                    std::future::pending::<Option<ironcrab::nats::NatsMessage>>().await
                }
            } => {
                if let Some(nats_msg) = msg {
                    match nats_msg.deserialize::<ControlRequest>() {
                        Ok(req) => {
                            if req.target != "market-data" {
                                debug!(target = %req.target, "Ignoring ControlRequest for other target");
                            } else if let ControlRequestKind::EnsurePumpAmmPoolAccounts { base_mint } = req.kind {
                                let run_id = ctx.run_id.clone();
                                let request_id = req.request_id.clone();
                                let ctx_clone = ctx.clone();
                                let rpc_clone = rpc.clone();
                                tokio::spawn(async move {
                                    handle_ensure_pump_amm_pool_accounts(
                                        &ctx_clone,
                                        &rpc_clone,
                                        run_id.as_str(),
                                        &request_id,
                                        &base_mint,
                                    )
                                    .await;
                                });
                            }
                            // Other ControlRequestKind variants are for execution-engine, ignore.
                        }
                        Err(e) => {
                            warn!(error = %e, "Failed to deserialize ControlRequest");
                        }
                    }
                }
            }

            // Proactive mint metadata (decimals/supply) fetched via RPC.
            Some(mint_event) = mint_info_rx.recv() => {
                // Write to JSONL
                if let Err(e) = ctx.jsonl_writer.write(&mint_event) {
                    error!(error = %e, "Failed to write TokenMintInfo event to JSONL");
                }

                // Publish to NATS
                if let Some(ref nats) = ctx.nats {
                    if let Err(e) = nats.publish(TOPIC_MARKET_EVENTS, &mint_event).await {
                        warn!(error = %e, "Failed to publish TokenMintInfo event to NATS");
                        NATS_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                    } else {
                        NATS_MESSAGES_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                        MARKET_EVENTS_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }

            // Keep /ready fresh even if Geyser/NATS are quiet.
            _ = activity_interval.tick() => {
                ironcrab::metrics::record_activity();

                // P1 Crash Isolation: Ping systemd watchdog frequently enough.
                #[cfg(unix)]
                let _ = sd_notify::notify(false, &[NotifyState::Watchdog]);
            }

            // Track wallet ATAs/mints from execution-engine results via JetStream (no RPC).
            _ = async {
                use futures::StreamExt;
                if let Some(ref consumer) = execution_js_consumer {
                    match consumer
                        .fetch()
                        .max_messages(50)
                        .expires(std::time::Duration::from_millis(100))
                        .messages()
                        .await
                    {
                        Ok(mut messages) => {
                            while let Some(msg_result) = messages.next().await {
                                match msg_result {
                                    Ok(msg) => {
                                        let exec: ExecutionResult = match serde_json::from_slice(&msg.payload) {
                                            Ok(e) => e,
                                            Err(e) => {
                                                debug!(error = %e, "Failed to deserialize ExecutionResult");
                                                let _ = msg.ack().await;
                                                continue;
                                            }
                                        };

                                        // Dedup on execution_id (fallback: signature)
                                        let dedup_key = if !exec.execution_id.is_empty() {
                        exec.execution_id.clone()
                    } else {
                        exec.signature.clone().unwrap_or_else(|| exec.decision_id.clone())
                    };
                                        {
                                            let mut deduper = ctx.execution_results_deduper.lock();
                                            if !deduper.should_process(&dedup_key) {
                                                let _ = msg.ack().await;
                                                continue;
                                            }
                                        }

                                        let Some(ref tracked_wallet) = ctx.tracked_wallet else {
                                            let _ = msg.ack().await;
                                            continue;
                                        };

                                        let mint_str = match exec.token_mint.as_deref() {
                                            Some(s) => s,
                                            None => {
                                                let _ = msg.ack().await;
                                                continue;
                                            }
                                        };
                                        let mint = match Pubkey::from_str(mint_str) {
                                            Ok(m) => m,
                                            Err(_) => {
                                                let _ = msg.ack().await;
                                                continue;
                                            }
                                        };

                                        let ata_str = match exec.metadata.get("token_account") {
                                            Some(s) => s,
                                            None => {
                                                warn!(
                                                    execution_id = %exec.execution_id,
                                                    wallet = %tracked_wallet.wallet,
                                                    mint = ?exec.token_mint,
                                                    intent_id = %exec.intent_id,
                                                    side = ?exec.metadata.get("side"),
                                                    "ExecutionResult missing metadata.token_account — cannot track ATA"
                                                );
                                                let _ = msg.ack().await;
                                                continue;
                                            }
                                        };
                                        let ata = match Pubkey::from_str(ata_str) {
                                            Ok(a) => a,
                                            Err(_) => {
                                                warn!(
                                                    execution_id = %exec.execution_id,
                                                    wallet = %tracked_wallet.wallet,
                                                    ata = %ata_str,
                                                    mint = ?exec.token_mint,
                                                    intent_id = %exec.intent_id,
                                                    "ExecutionResult metadata.token_account is not a valid Pubkey"
                                                );
                                                let _ = msg.ack().await;
                                                continue;
                                            }
                                        };

                                        let token_program_str = match exec.metadata.get("token_program") {
                                            Some(s) => s,
                                            None => {
                                                warn!(
                                                    execution_id = %exec.execution_id,
                                                    wallet = %tracked_wallet.wallet,
                                                    mint = ?exec.token_mint,
                                                    intent_id = %exec.intent_id,
                                                    side = ?exec.metadata.get("side"),
                                                    "ExecutionResult missing metadata.token_program — cannot track ATA"
                                                );
                                                let _ = msg.ack().await;
                                                continue;
                                            }
                                        };
                                        let token_program = match Pubkey::from_str(token_program_str) {
                                            Ok(p) => p,
                                            Err(_) => {
                                                warn!(
                                                    execution_id = %exec.execution_id,
                                                    wallet = %tracked_wallet.wallet,
                                                    token_program = %token_program_str,
                                                    mint = ?exec.token_mint,
                                                    "ExecutionResult metadata.token_program is not a valid Pubkey"
                                                );
                                                let _ = msg.ack().await;
                                                continue;
                                            }
                                        };
                                        // Only support SPL Token + Token-2022 for wallet ATA tracking.
                                        if token_program.to_bytes() != spl_token::ID.to_bytes()
                                            && token_program.to_bytes() != spl_token_2022::ID.to_bytes()
                                        {
                                            let _ = msg.ack().await;
                                            continue;
                                        }

                                                        let mint_decimals: Option<u8> = exec
                                            .metadata
                                            .get("mint_decimals")
                                            .and_then(|s| s.parse::<u8>().ok());

                                        // 1) Track ATA for wallet updates (Geyser subscription list)
                                        let mut added_ata = false;
                                        {
                                            let mut set = ctx.tracked_wallet_token_accounts.write();
                                            if set.insert(ata) {
                                                added_ata = true;
                                            }
                                        }

                                        // 2) Cache decimals if provided
                                        if let Some(d) = mint_decimals {
                                            ctx.tracked_wallet_mint_decimals.write().insert(mint, d);
                                            ctx.live_pool_cache.set_mint_decimals(mint, d);
                                        }

                                        // 3) Track mint so Geyser will deliver the mint account
                                        let mut added_mint = false;
                                        {
                                            let mut tracked = ctx.tracked_mints.write();
                                            if tracked.insert(mint) {
                                                added_mint = true;
                                                let updated: Vec<Pubkey> = tracked.iter().copied().collect();
                                                let _ = ctx.tracked_mints_tx.send(updated);
                                            }
                                        }

                                        // 4) Recompute tracked wallet accounts and notify listener
                                        if added_ata {
                                            let mut accounts: Vec<Pubkey> = Vec::new();
                                            accounts.push(tracked_wallet.wallet);
                                            accounts.push(tracked_wallet.wsol_ata);
                                            accounts.extend(ctx.tracked_wallet_token_accounts.read().iter().copied());
                                            accounts.sort();
                                            accounts.dedup();
                                            let _ = ctx.tracked_wallet_tx.send(accounts);
                                        }

                                        let is_confirmed_sell = exec.status == ExecutionStatus::Confirmed
                                            && exec.metadata.get("side").map(|s| s.as_str()) == Some("SELL");

                                        info!(
                                            execution_id = %exec.execution_id,
                                            mint = %mint,
                                            ata = %ata,
                                            token_program = %token_program,
                                            mint_decimals = ?mint_decimals,
                                            added_ata,
                                            added_mint,
                                            is_confirmed_sell,
                                            "ExecutionResult: tracked wallet ATA/mint"
                                        );

                                        // 5) For confirmed SELLs: write zero-balance snapshot and untrack ATA
                                        if is_confirmed_sell {
                                            let wallet_str = tracked_wallet.wallet.to_string();
                                            let snapshot = MarketEvent::new(
                                                "market-data",
                                                BUILD_VERSION,
                                                &ctx.run_id,
                                                format!("wallet_snapshot_sell_{}", exec.execution_id),
                                                "execution_result_sell",
                                                None,
                                                MarketEventKind::WalletBalanceSnapshot {
                                                    mint: mint_str.to_string(),
                                                    balance_raw: 0,
                                                    decimals: mint_decimals.unwrap_or(9),
                                                    token_program: token_program_str.clone(),
                                                },
                                            );
                                            if let Some(ref nats) = ctx.nats {
                                                let subject = wallet_snapshot_subject(&wallet_str, mint_str);
                                                if let Err(e) = nats.jetstream_publish(&subject, &snapshot).await {
                                                    warn!(error = %e, mint = %mint_str, "Failed to publish zero-balance snapshot to JetStream after SELL");
                                                }
                                            }

                                            if mint_str == WSOL_MINT {
                                                tracked_wallet.last_wsol_balance.store(0, Ordering::Relaxed);
                                            }

                                            let mut set = ctx.tracked_wallet_token_accounts.write();
                                            if set.remove(&ata) {
                                                let mut accounts: Vec<Pubkey> = Vec::new();
                                                accounts.push(tracked_wallet.wallet);
                                                accounts.push(tracked_wallet.wsol_ata);
                                                accounts.extend(set.iter().copied());
                                                accounts.sort();
                                                accounts.dedup();
                                                let _ = ctx.tracked_wallet_tx.send(accounts);
                                                info!(
                                                    mint = %mint_str,
                                                    ata = %ata,
                                                    remaining_tracked = set.len(),
                                                    "Untracked ATA after confirmed SELL"
                                                );
                                            }
                                        }

                                        if let Err(e) = msg.ack().await {
                                            warn!(error = %e, "Failed to ack ExecutionResult");
                                        }
                                    }
                                    Err(e) => {
                                        warn!(error = %e, "ExecutionResult fetch error");
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            debug!(error = %e, "ExecutionResult stream fetch failed (may be empty)");
                        }
                    }
                } else {
                    std::future::pending::<()>().await
                }
            } => {}

            // Account updates (pool state changes)
            Ok(account_update) = account_rx.recv() => {
                account_count += 1;
                ironcrab::metrics::record_activity();

                // === WsolManager Support: Wallet Balance Updates ===
                // Track SOL (native) and WSOL (ATA) balance changes for WsolManager
                if let Some(ref tracked_wallet) = ctx.tracked_wallet {
                    let is_wallet_account = account_update.pubkey == tracked_wallet.wallet;
                    let is_wsol_ata = account_update.pubkey == tracked_wallet.wsol_ata;
                    let is_token_ata = ctx
                        .tracked_wallet_token_accounts
                        .read()
                        .contains(&account_update.pubkey);

                    if is_wallet_account || is_wsol_ata {
                        // Parse balance based on account type
                        let (new_sol, new_wsol, balance_changed) = if is_wallet_account {
                            // Native SOL account - balance is in lamports field
                            let lamports = account_update.lamports;
                            let prev = tracked_wallet.last_sol_balance.swap(lamports, Ordering::Relaxed);
                            let wsol = tracked_wallet.last_wsol_balance.load(Ordering::Relaxed);
                            let has_wsol = tracked_wallet.wsol_seen.load(Ordering::Relaxed);
                            let wsol_value = if has_wsol { Some(wsol) } else { None };
                            (lamports, wsol_value, lamports != prev)
                        } else {
                            // WSOL ATA - parse token account balance
                            if let Some(balance) = try_parse_token_account_balance(&account_update.data) {
                                let prev = tracked_wallet.last_wsol_balance.swap(balance, Ordering::Relaxed);
                                tracked_wallet.wsol_seen.store(true, Ordering::Relaxed);
                                let sol = tracked_wallet.last_sol_balance.load(Ordering::Relaxed);
                                (sol, Some(balance), balance != prev)
                            } else {
                                continue; // Failed to parse, skip
                            }
                        };

                        if balance_changed {
                            let wallet_str = tracked_wallet.wallet.to_string();

                            // Publish SOL + WSOL as WalletBalanceSnapshot to JetStream (SSOT)
                            if let Some(ref nats) = ctx.nats {
                                let sol_snapshot = MarketEvent::new(
                                    "market-data",
                                    BUILD_VERSION,
                                    run_id,
                                    format!("geyser_wallet_sol_{}", account_update.slot),
                                    "geyser_wallet_update",
                                    Some(account_update.slot),
                                    MarketEventKind::WalletBalanceSnapshot {
                                        mint: "NATIVE_SOL".to_string(),
                                        balance_raw: new_sol,
                                        decimals: 9,
                                        token_program: "system".to_string(),
                                    },
                                );
                                let sol_subject = wallet_snapshot_subject(&wallet_str, "NATIVE_SOL");
                                if let Err(e) = nats.jetstream_publish(&sol_subject, &sol_snapshot).await {
                                    warn!(error = %e, "Failed to publish native SOL WalletBalanceSnapshot to JetStream");
                                    NATS_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                                } else {
                                    NATS_MESSAGES_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                                }

                                if let Some(wsol) = new_wsol {
                                    let wsol_snapshot = MarketEvent::new(
                                        "market-data",
                                        BUILD_VERSION,
                                        run_id,
                                        format!("geyser_wallet_wsol_{}", account_update.slot),
                                        "geyser_wallet_update",
                                        Some(account_update.slot),
                                        MarketEventKind::WalletBalanceSnapshot {
                                            mint: WSOL_MINT.to_string(),
                                            balance_raw: wsol,
                                            decimals: 9,
                                            token_program: "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_string(),
                                        },
                                    );
                                    let wsol_subject = wallet_snapshot_subject(&wallet_str, WSOL_MINT);
                                    if let Err(e) = nats.jetstream_publish(&wsol_subject, &wsol_snapshot).await {
                                        warn!(error = %e, "Failed to publish WSOL WalletBalanceSnapshot to JetStream");
                                        NATS_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                                    } else {
                                        NATS_MESSAGES_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                                    }
                                }

                                info!(
                                    wallet = %wallet_str,
                                    sol_lamports = new_sol,
                                    wsol_lamports = ?new_wsol,
                                    slot = account_update.slot,
                                    "WalletBalanceSnapshot (SOL/WSOL) published to JetStream"
                                );
                            }
                        }
                    } else if is_token_ata
                        && (account_update.owner.to_bytes() == spl_token::ID.to_bytes()
                            || account_update.owner.to_bytes()
                                == spl_token_2022::ID.to_bytes())
                    {
                        let (mint, balance_raw) = if account_update.owner.to_bytes()
                            == spl_token::ID.to_bytes()
                        {
                            match spl_token::state::Account::unpack(&account_update.data) {
                                Ok(acc) => (Pubkey::new_from_array(acc.mint.to_bytes()), acc.amount),
                                Err(_) => continue,
                            }
                        } else {
                            // Token-2022 accounts may have extensions (data > 165 bytes).
                            // Use StateWithExtensions instead of Pack::unpack.
                            match StateWithExtensions::<spl_token_2022::state::Account>::unpack(&account_update.data) {
                                Ok(state) => (Pubkey::new_from_array(state.base.mint.to_bytes()), state.base.amount),
                                Err(_) => continue,
                            }
                        };

                        let decimals = ctx
                            .tracked_wallet_mint_decimals
                            .read()
                            .get(&mint)
                            .copied()
                            .unwrap_or_else(|| {
                                // This should only happen for accounts created after initial scan.
                                // Log a warning so we can track if this becomes a problem.
                                warn!(
                                    mint = %mint,
                                    account = %account_update.pubkey,
                                    "Decimals not cached for token account, using default 6"
                                );
                                6
                            });

                        let mint_str = mint.to_string();
                        let event = MarketEvent::new(
                            "market-data",
                            BUILD_VERSION,
                            run_id,
                            ctx.next_event_id(),
                            "geyser_wallet_update",
                            Some(account_update.slot),
                            MarketEventKind::WalletBalanceSnapshot {
                                mint: mint_str.clone(),
                                balance_raw,
                                decimals,
                                token_program: account_update.owner.to_string(),
                            },
                        );

                        if let Some(ref nats) = ctx.nats {
                            let subject = wallet_snapshot_subject(
                                &tracked_wallet.wallet.to_string(),
                                &mint_str,
                            );
                            if let Err(e) = nats.jetstream_publish(&subject, &event).await {
                                warn!(
                                    error = %e,
                                    mint = %mint_str,
                                    "Failed to publish WalletBalanceSnapshot to JetStream"
                                );
                                NATS_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                            } else {
                                NATS_MESSAGES_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                                MARKET_EVENTS_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                            }
                        }

                        if let Err(e) = ctx.jsonl_writer.write(&event) {
                            warn!(error = %e, "Failed to write WalletBalanceSnapshot to JSONL");
                        }
                    }
                }

                // Tracked mint updates (Token mint authority/freeze info)
                if (account_update.owner.to_bytes() == spl_token::ID.to_bytes()
                    || account_update.owner.to_bytes() == spl_token_2022::ID.to_bytes())
                    && ctx.tracked_mints.read().contains(&account_update.pubkey)
                {
                    if let Some((decimals, supply, mint_authority, freeze_authority)) =
                        try_parse_mint_account(&account_update.owner, &account_update.data)
                    {
                        // Decimals-Policy: TokenMintInfo from Geyser is authoritative.
                        // Keep wallet decimals cache warm so WalletBalanceSnapshot never needs the 6-decimal fallback.
                        ctx.tracked_wallet_mint_decimals
                            .write()
                            .insert(account_update.pubkey, decimals);

                        // PR1: Also populate MASTER LivePoolCache mint_decimals (Single Source of Truth).
                        // This keeps LivePoolCache consistent with tracked_wallet_mint_decimals.
                        ctx.live_pool_cache.set_mint_decimals(account_update.pubkey, decimals);

                        let is_token_2022 = account_update.owner.to_bytes() == spl_token_2022::ID.to_bytes();

                        let mint_event = MarketEvent::new(
                            "market-data",
                            BUILD_VERSION,
                            run_id,
                            ctx.next_event_id(),
                            "geyser",
                            Some(account_update.slot),
                            MarketEventKind::TokenMintInfo {
                                mint: account_update.pubkey.to_string(),
                                token_program: account_update.owner.to_string(),
                                decimals,
                                supply,
                                mint_authority,
                                freeze_authority,
                            },
                        );

                        // Log Token-2022 mints explicitly (debugging)
                        if is_token_2022 {
                            info!(
                                mint = %account_update.pubkey,
                                token_program = %account_update.owner,
                                decimals,
                                supply,
                                "TokenMintInfo: Token-2022 mint detected via Geyser"
                            );
                        } else {
                            debug!(
                                mint = %account_update.pubkey,
                                token_program = %account_update.owner,
                                decimals,
                                "TokenMintInfo: SPL Token mint via Geyser"
                            );
                        }

                        if let Err(e) = ctx.jsonl_writer.write(&mint_event) {
                            error!(error = %e, "Failed to write TokenMintInfo event to JSONL");
                        }

                        if let Some(ref nats) = ctx.nats {
                            if let Err(e) = nats.publish(TOPIC_MARKET_EVENTS, &mint_event).await {
                                warn!(error = %e, "Failed to publish TokenMintInfo event to NATS");
                                NATS_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                            } else {
                                NATS_MESSAGES_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                                MARKET_EVENTS_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                }

                // Vault account updates → emit PoolStateUpdate (Geyser-based reserve balances)
                // This eliminates the need for RPC calls to fetch vault balances.
                if account_update.owner.to_bytes() == spl_token::ID.to_bytes()
                    || account_update.owner.to_bytes() == spl_token_2022::ID.to_bytes()
                {
                    // Clone data from lock scope to avoid holding lock across await
                    let vault_info_opt = ctx.tracked_vaults.read().get(&account_update.pubkey).cloned();
                    if let Some(vault_info) = vault_info_opt {
                        // Parse token account to get balance
                        if let Some(balance) = try_parse_token_account_balance(&account_update.data) {
                            // Check if balance changed (avoid spamming unchanged updates)
                            let prev_balance = vault_info.last_balance.swap(balance, std::sync::atomic::Ordering::Relaxed);
                            if balance != prev_balance {
                                // We need both vault balances to emit a complete PoolStateUpdate.
                                // For now, emit partial updates - consumers should merge base+quote.
                                // Future: Track both vaults and emit only when both are known.
                                let (reserve_base, reserve_quote) = if vault_info.is_base_vault {
                                    (balance, 0u64) // Partial: only base known
                                } else {
                                    (0u64, balance) // Partial: only quote known
                                };

                                // Try to get the other vault's balance for a complete update
                                let vaults = ctx.tracked_vaults.read();
                                let complete_update = vaults.values()
                                    .find(|v| v.pool_address == vault_info.pool_address && v.is_base_vault != vault_info.is_base_vault)
                                    .map(|other| {
                                        let other_balance = other.last_balance.load(std::sync::atomic::Ordering::Relaxed);
                                        if vault_info.is_base_vault {
                                            (balance, other_balance)
                                        } else {
                                            (other_balance, balance)
                                        }
                                    });
                                drop(vaults);

                                let (final_base, final_quote) = complete_update.unwrap_or((reserve_base, reserve_quote));

                                // Only emit if we have at least one non-zero balance
                                if final_base > 0 || final_quote > 0 {
                                    // MASTER CACHE: Update vault balance in LivePoolCache
                                    ctx.live_pool_cache.update_vault_balance(&account_update.pubkey, balance, account_update.slot);

                                    // Publish PoolCacheUpdate::BalanceUpdated to JetStream (persistent state)
                                    if let Some(ref nats) = ctx.nats {
                                        let balance_update = PoolCacheUpdate::new_balance_updated(
                                            "market-data",
                                            BUILD_VERSION,
                                            run_id,
                                            vault_info.pool_address.to_string(),
                                            vault_info.dex.clone(),
                                            vault_info.base_mint.to_string(),
                                            vault_info.quote_mint.to_string(),
                                            final_base,
                                            final_quote,
                                            account_update.slot,
                                        );
                                        let subject = pool_subject(&vault_info.pool_address.to_string());
                                        if let Err(e) = nats.jetstream_publish(&subject, &balance_update).await {
                                            warn!(error = %e, "Failed to publish PoolCacheUpdate::BalanceUpdated to JetStream");
                                            NATS_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                                        } else {
                                            NATS_MESSAGES_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                                            info!(pool = %vault_info.pool_address, slot = account_update.slot, "MASTER CACHE: Published PoolCacheUpdate::BalanceUpdated to JetStream");
                                        }
                                    }

                                    // Publish MarketEvent::PoolStateUpdate (existing logic for strategies)
                                    let state_event = MarketEvent::new(
                                        "market-data",
                                        BUILD_VERSION,
                                        run_id,
                                        ctx.next_event_id(),
                                        "geyser_vault",
                                        Some(account_update.slot),
                                        MarketEventKind::PoolStateUpdate {
                                            pool_address: vault_info.pool_address.to_string(),
                                            dex: vault_info.dex.clone(),
                                            reserve_base: final_base,
                                            reserve_quote: final_quote,
                                            base_mint: vault_info.base_mint.to_string(),
                                            quote_mint: vault_info.quote_mint.to_string(),
                                            update_slot: account_update.slot,
                                            // DLMM-specific fields (Option D)
                                            active_id: vault_info.active_id,
                                            bin_step: vault_info.bin_step,
                                        },
                                    );

                                    if let Err(e) = ctx.jsonl_writer.write(&state_event) {
                                        error!(error = %e, "Failed to write PoolStateUpdate event to JSONL");
                                    }

                                    if let Some(ref nats) = ctx.nats {
                                        if let Err(e) = nats.publish(TOPIC_MARKET_EVENTS, &state_event).await {
                                            warn!(error = %e, "Failed to publish PoolStateUpdate event to NATS");
                                            NATS_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                                        } else {
                                            NATS_MESSAGES_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                                            MARKET_EVENTS_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                // Bin Array account updates → emit BinArrayUpdate (Geyser-based liquidity distribution)
                // This eliminates the need for RPC calls to fetch Meteora DLMM bin arrays.
                let dlmm_program = Pubkey::from_str(METEORA_DLMM_PROGRAM)
                    .expect("Invalid METEORA_DLMM_PROGRAM constant");
                if account_update.owner == dlmm_program {
                    if let Some(bin_array_info) = ctx.tracked_bin_arrays.read().get(&account_update.pubkey).cloned() {
                        // Parse bin array to extract liquidity distribution
                        match BinArray::parse(&account_update.data, bin_array_info.bin_step) {
                            Ok(parsed_array) => {
                                // Convert to compact BinData (only bins with liquidity)
                                let bins: Vec<BinData> = parsed_array.bins
                                    .iter()
                                    .enumerate()
                                    .filter(|(_, bin)| bin.amount_x > 0 || bin.amount_y > 0)
                                    .map(|(offset, bin)| BinData {
                                        offset: offset as u8,
                                        amount_x: bin.amount_x,
                                        amount_y: bin.amount_y,
                                    })
                                    .collect();

                                // Only emit if there's any liquidity
                                if !bins.is_empty() {
                                    let bin_event = MarketEvent::new(
                                        "market-data",
                                        BUILD_VERSION,
                                        run_id,
                                        ctx.next_event_id(),
                                        "geyser_bin_array",
                                        Some(account_update.slot),
                                        MarketEventKind::BinArrayUpdate {
                                            pool_address: bin_array_info.pool_address.to_string(),
                                            bin_array_index: bin_array_info.bin_array_index,
                                            bins,
                                            update_slot: account_update.slot,
                                        },
                                    );

                                    if let Err(e) = ctx.jsonl_writer.write(&bin_event) {
                                        error!(error = %e, "Failed to write BinArrayUpdate event to JSONL");
                                    }

                                    if let Some(ref nats) = ctx.nats {
                                        if let Err(e) = nats.publish(TOPIC_MARKET_EVENTS, &bin_event).await {
                                            warn!(error = %e, "Failed to publish BinArrayUpdate event to NATS");
                                            NATS_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                                        } else {
                                            NATS_MESSAGES_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                                            MARKET_EVENTS_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                debug!(
                                    error = %e,
                                    pubkey = %account_update.pubkey,
                                    "Failed to parse bin array account"
                                );
                            }
                        }
                    }
                }

                // MASTER CACHE: Try to parse as DEX pool state and upsert into LivePoolCache
                if let Some(mut cached_state) = parse_pool_account(&account_update.owner, &account_update.data) {
                    // P2#7: Enrich PumpFun token_mint from pool_mint_map when parse returns default
                    if let CachedPoolState::PumpFun(ref mut s) = &mut cached_state {
                        if s.token_mint == Pubkey::default() {
                            let pool_str = account_update.pubkey.to_string();
                            if let Some(mint_str) = ctx.pool_mint_map.read().get(&pool_str).cloned() {
                                if let Ok(mint_pk) = Pubkey::from_str(&mint_str) {
                                    s.token_mint = mint_pk;
                                    debug!(
                                        pool = %pool_str,
                                        mint = %mint_str,
                                        "P2#7: Enriched PumpFun token_mint from pool_mint_map"
                                    );
                                }
                            }
                        }
                    }

                    // Update MASTER LivePoolCache (Single Source of Truth)
                    ctx.live_pool_cache.upsert(account_update.pubkey, cached_state.clone(), account_update.slot);

                    // FIX-29: One-time Cold Path RPC to fetch Serum/OpenBook accounts for Raydium AMM.
                    // These are static (never change) — fetch once, cache forever.
                    if let CachedPoolState::RaydiumAmm(ref s) = cached_state {
                        if s.serum_bids.is_none() && s.market_id != Pubkey::default() {
                            let pool_pk = account_update.pubkey;
                            let already_fetched = ctx.raydium_serum_fetched.read().contains(&pool_pk);
                            if !already_fetched {
                                ctx.raydium_serum_fetched.write().insert(pool_pk);
                                let rpc = Arc::clone(&rpc);
                                let cache = Arc::clone(&ctx.live_pool_cache);
                                let market_id = s.market_id;
                                tokio::spawn(async move {
                                    match rpc.get_account_retry(&market_id).await {
                                        Ok(account) => {
                                            if let Some((bids, asks, eq, _bv, _qv)) =
                                                ironcrab::solana::dex::raydium::Raydium::parse_serum_market_accounts(&account.data)
                                            {
                                                if let (Some(b), Some(a), Some(e)) = (bids, asks, eq) {
                                                    cache.set_raydium_serum_accounts(&pool_pk, b, a, e);
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            tracing::debug!(
                                                pool = %pool_pk,
                                                market_id = %market_id,
                                                error = %e,
                                                "FIX-29: Failed to fetch serum market account (will retry on next trade)"
                                            );
                                        }
                                    }
                                });
                            }
                        }
                    }

                    // Extract mint and reserve info from cached_state for PoolCacheUpdate
                    let (base_mint, quote_mint, base_reserve, quote_reserve) = match &cached_state {
                        CachedPoolState::Orca(s) => {
                            (s.token_mint_a, s.token_mint_b, s.vault_a_balance.unwrap_or(0), s.vault_b_balance.unwrap_or(0))
                        }
                        CachedPoolState::RaydiumAmm(s) => {
                            (s.base_mint, s.quote_mint, s.coin_reserve.unwrap_or(0), s.pc_reserve.unwrap_or(0))
                        }
                        CachedPoolState::RaydiumCpmm(s) => {
                            (s.token_0_mint, s.token_1_mint, s.reserve_0.unwrap_or(0), s.reserve_1.unwrap_or(0))
                        }
                        CachedPoolState::Meteora(s) => {
                            (s.token_x_mint, s.token_y_mint, s.reserve_x_balance.unwrap_or(0), s.reserve_y_balance.unwrap_or(0))
                        }
                        CachedPoolState::PumpAmm(s) => {
                            // Cache creator for migrated PumpFun tokens (from AMM pool account)
                            if let Some(creator) = s.creator {
                                let mint_str = s.base_mint.to_string();
                                let pool_str = account_update.pubkey.to_string();
                                let creator_str = creator.to_string();

                                // Cache in pool_creator_cache (pool -> creator)
                                {
                                    let mut pool_creator = ctx.pool_creator_cache.write();
                                    if !pool_creator.contains_key(&pool_str) {
                                        pool_creator.insert(pool_str.clone(), creator_str.clone());
                                        debug!(
                                            pool = %pool_str,
                                            creator = %creator_str,
                                            "Cached creator from PumpAmm pool account (pool_creator_cache)"
                                        );
                                    }
                                }

                                // Also cache in creator_cache (mint -> creator)
                                {
                                    let mut creator_cache = ctx.creator_cache.write();
                                    if !creator_cache.contains_key(&mint_str) {
                                        creator_cache.insert(mint_str.clone(), creator_str.clone());
                                        info!(
                                            mint = %mint_str,
                                            pool = %pool_str,
                                            creator = %creator_str,
                                            "Cached creator from PumpAmm pool account (migrated token)"
                                        );
                                    }
                                }
                            }
                            // A.1 Phase 2.1: Register PumpAmm vaults for Geyser subscription (base/quote reserve updates)
                            {
                                let dex_str = "pump_amm".to_string();
                                let mut vaults_changed = false;
                                {
                                    let mut vaults = ctx.tracked_vaults.write();
                                    vaults
                                        .entry(s.pool_base_token_account)
                                        .or_insert_with(|| {
                                            vaults_changed = true;
                                            VaultInfo {
                                                pool_address: account_update.pubkey,
                                                dex: dex_str.clone(),
                                                base_mint: s.base_mint,
                                                quote_mint: s.quote_mint,
                                                is_base_vault: true,
                                                last_balance: std::sync::atomic::AtomicU64::new(0),
                                                active_id: None,
                                                bin_step: None,
                                            }
                                        });
                                    vaults
                                        .entry(s.pool_quote_token_account)
                                        .or_insert_with(|| {
                                            vaults_changed = true;
                                            VaultInfo {
                                                pool_address: account_update.pubkey,
                                                dex: dex_str,
                                                base_mint: s.base_mint,
                                                quote_mint: s.quote_mint,
                                                is_base_vault: false,
                                                last_balance: std::sync::atomic::AtomicU64::new(0),
                                                active_id: None,
                                                bin_step: None,
                                            }
                                        });
                                }
                                if vaults_changed {
                                    let vault_list: Vec<Pubkey> = ctx.tracked_vaults.read().keys().copied().collect();
                                    let _ = ctx.tracked_vaults_tx.send(vault_list);
                                    debug!(
                                        pool = %account_update.pubkey,
                                        base_vault = %s.pool_base_token_account,
                                        quote_vault = %s.pool_quote_token_account,
                                        "A.1: Registered PumpAmm vaults for Geyser reserve subscription"
                                    );
                                }
                            }
                            {
                                let (base_r, quote_r) = if s.base_reserve.is_none() || s.quote_reserve.is_none() {
                                    let base_vault = s.pool_base_token_account;
                                    let quote_vault = s.pool_quote_token_account;
                                    let base_bal = match rpc.get_account_opt_retry(&base_vault).await {
                                        Ok(Some(acc)) => try_parse_token_account_balance(&acc.data).unwrap_or(0),
                                        _ => 0,
                                    };
                                    let quote_bal = match rpc.get_account_opt_retry(&quote_vault).await {
                                        Ok(Some(acc)) => try_parse_token_account_balance(&acc.data).unwrap_or(0),
                                        _ => 0,
                                    };
                                    if base_bal > 0 || quote_bal > 0 {
                                        info!(
                                            pool = %account_update.pubkey,
                                            base_vault = %base_vault,
                                            quote_vault = %quote_vault,
                                            base_bal,
                                            quote_bal,
                                            "pump_amm: pre-loaded vault balances via RPC (Cold Start Bootstrap)"
                                        );
                                    }
                                    (base_bal, quote_bal)
                                } else {
                                    (s.base_reserve.unwrap_or(0), s.quote_reserve.unwrap_or(0))
                                };
                                (s.base_mint, s.quote_mint, base_r, quote_r)
                            }
                        }
                        CachedPoolState::PumpFun(s) => {
                            (s.token_mint, Pubkey::default(), s.virtual_token_reserves, s.virtual_sol_reserves)
                        }
                        CachedPoolState::MeteoraCpmm(s) => {
                            (s.token_0_mint, s.token_1_mint, s.reserve_0, s.reserve_1)
                        }
                    };

                    // Publish PoolCacheUpdate to JetStream (Single Source of Truth for pool state)
                    if let Some(ref nats) = ctx.nats {
                        let mut pool_update = PoolCacheUpdate::new_pool_discovered(
                            "market-data",
                            BUILD_VERSION,
                            run_id,
                            account_update.pubkey.to_string(),
                            cached_state.dex_name().to_string(),
                            base_mint.to_string(),
                            quote_mint.to_string(),
                            base_reserve,
                            quote_reserve,
                            Some(0), // liquidity_lamports not available from account data
                            account_update.slot,
                        );

                        // Propagate DEX-specific metadata to SLAVE caches via PoolCacheUpdate.metadata.
                        // This ensures execution-engine receives creator, pool accounts, etc. from Geyser
                        // without needing RPC fallbacks.
                        match &cached_state {
                            CachedPoolState::PumpFun(s) => {
                                // Always propagate real_reserves + complete for SELL validation
                                // in execution-engine's SLAVE cache.
                                let mut meta = std::collections::HashMap::new();
                                if s.creator != Pubkey::default() {
                                    meta.insert("creator".to_string(), s.creator.to_string());
                                }
                                meta.insert("associated_bonding_curve".to_string(), s.associated_bonding_curve.to_string());
                                meta.insert("complete".to_string(), s.complete.to_string());
                                meta.insert("real_token_reserves".to_string(), s.real_token_reserves.to_string());
                                meta.insert("real_sol_reserves".to_string(), s.real_sol_reserves.to_string());
                                meta.insert("cashback_enabled".to_string(), s.cashback_enabled.to_string());
                                pool_update.metadata = Some(meta);

                                // === BondingCurveProgress event for momentum-bot exit signal ===
                                // PumpFun initial real_token_reserves = 793_100_000_000_000
                                const INITIAL_REAL_TOKEN_RESERVES: u64 = 793_100_000_000_000;
                                let tokens_sold = INITIAL_REAL_TOKEN_RESERVES.saturating_sub(s.real_token_reserves);
                                let progress_bps = ((tokens_sold as u128 * 10_000) / INITIAL_REAL_TOKEN_RESERVES as u128) as u32;
                                let progress_bps = progress_bps.min(10_000);

                                // Throttle: only emit when progress changes by >= 50 bps or complete changes
                                let should_emit = {
                                    let cache = ctx.last_emitted_curve_progress.read();
                                    match cache.get(&account_update.pubkey) {
                                        Some(&(last_bps, last_complete)) => {
                                            progress_bps.abs_diff(last_bps) >= 50 || s.complete != last_complete
                                        }
                                        None => true,
                                    }
                                };

                                if should_emit {
                                    ctx.last_emitted_curve_progress.write()
                                        .insert(account_update.pubkey, (progress_bps, s.complete));

                                    let curve_event = MarketEvent::new(
                                        "market-data",
                                        BUILD_VERSION,
                                        run_id,
                                        ctx.next_event_id(),
                                        "geyser_bonding_curve",
                                        Some(account_update.slot),
                                        MarketEventKind::BondingCurveProgress {
                                            mint: s.token_mint.to_string(),
                                            bonding_curve: account_update.pubkey.to_string(),
                                            progress_bps,
                                            complete: s.complete,
                                        },
                                    );

                                    if let Err(e) = ctx.jsonl_writer.write(&curve_event) {
                                        error!(error = %e, "Failed to write BondingCurveProgress event to JSONL");
                                    }

                                    if let Err(e) = nats.publish(TOPIC_MARKET_EVENTS, &curve_event).await {
                                        warn!(error = %e, "Failed to publish BondingCurveProgress to NATS");
                                    } else {
                                        NATS_MESSAGES_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                                        MARKET_EVENTS_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                                    }
                                }
                            }
                            CachedPoolState::PumpAmm(s) => {
                                let mut meta = std::collections::HashMap::new();
                                if let Some(creator) = s.creator {
                                    meta.insert("creator".to_string(), creator.to_string());
                                }
                                // FIX-26: pool_accounts from Geyser parse, or fallback to MASTER cache
                                let effective_pool_accounts = if !s.pool_accounts.is_empty() {
                                    s.pool_accounts.clone()
                                } else {
                                    ctx.live_pool_cache
                                        .get_pump_amm_pool_accounts(&account_update.pubkey)
                                        .unwrap_or_default()
                                };
                                if !effective_pool_accounts.is_empty() {
                                    let accounts_str: Vec<String> = effective_pool_accounts.iter().map(|p| p.to_string()).collect();
                                    meta.insert("pool_accounts".to_string(), accounts_str.join(","));
                                }
                                if !meta.is_empty() {
                                    pool_update.metadata = Some(meta);
                                }
                            }
                            CachedPoolState::RaydiumAmm(s) => {
                                // FIX-29: Always propagate market_id (from Geyser parse),
                                // plus serum accounts when available (from async RPC fetch)
                                let mut meta = std::collections::HashMap::new();
                                if s.market_id != Pubkey::default() {
                                    meta.insert("market_id".to_string(), s.market_id.to_string());
                                }
                                if let (Some(bids), Some(asks), Some(eq)) =
                                    (s.serum_bids, s.serum_asks, s.serum_event_queue)
                                {
                                    meta.insert("serum_bids".to_string(), bids.to_string());
                                    meta.insert("serum_asks".to_string(), asks.to_string());
                                    meta.insert("serum_event_queue".to_string(), eq.to_string());
                                }
                                if !meta.is_empty() {
                                    pool_update.metadata = Some(meta);
                                }
                            }
                            _ => {}
                        }
                        // P3 #13: Propagate base_decimals and quote_decimals to SLAVE caches (all DEX types)
                        {
                            let mut meta = pool_update.metadata.as_ref().cloned().unwrap_or_default();
                            if let Some(d) = ctx.live_pool_cache.get_mint_decimals(&base_mint) {
                                meta.insert("base_decimals".to_string(), d.to_string());
                            }
                            // For quote: use quote_mint, or when default (PumpFun) use SOL
                            let quote_for_decimals = if quote_mint == Pubkey::default() {
                                Pubkey::from_str(NATIVE_SOL_MINT).ok()
                            } else {
                                Some(quote_mint)
                            };
                            if let Some(q) = quote_for_decimals {
                                if let Some(d) = ctx.live_pool_cache.get_mint_decimals(&q) {
                                    meta.insert("quote_decimals".to_string(), d.to_string());
                                } else if q == Pubkey::from_str(NATIVE_SOL_MINT).unwrap_or_default() {
                                    meta.insert("quote_decimals".to_string(), "9".to_string());
                                }
                            }
                            if !meta.is_empty() {
                                pool_update.metadata = Some(meta);
                            }
                        }
                        let subject = pool_subject(&account_update.pubkey.to_string());
                        if let Err(e) = nats.jetstream_publish(&subject, &pool_update).await {
                            warn!(error = %e, "Failed to publish PoolCacheUpdate to JetStream");
                            NATS_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                        } else {
                            NATS_MESSAGES_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }

                // Try to parse as DEX pool event (for MarketEvents - existing logic)
                let parsed = parse_account_update(&account_update);

                // Special handling for PumpFun BondingCurveUpdate: cache creator
                if let Some(ParsedDexEvent::BondingCurveUpdate {
                    pool_address,
                    creator,
                    virtual_token_reserves,
                    virtual_sol_reserves,
                    real_token_reserves,
                    real_sol_reserves,
                    complete,
                    cashback_enabled,
                    slot,
                }) = &parsed {
                    let pool_str = pool_address.to_string();
                    let creator_str = creator.to_string();

                    // Cache pool -> creator mapping (AUTHORITATIVE: bonding curve account data)
                    // FIX-22: Always overwrite — on-chain account data is the source of truth.
                    {
                        let mut pool_creator = ctx.pool_creator_cache.write();
                        pool_creator.insert(pool_str.clone(), creator_str.clone());
                    }

                    // If we know the mint for this pool, also update creator_cache (mint -> creator)
                    // and emit DevWalletIdentified event.
                    // FIX-22: Always overwrite cache with authoritative bonding curve data.
                    // PoolCreated events may set a wrong creator (instruction_accounts[7] can differ
                    // from on-chain creator for CPI/bundler creates). BondingCurveUpdate is authoritative.
                    if let Some(mint) = ctx.pool_mint_map.read().get(&pool_str).cloned() {
                        let mut creator_cache = ctx.creator_cache.write();
                        let existing = creator_cache.get(&mint).cloned();
                        creator_cache.insert(mint.clone(), creator_str.clone());
                        drop(creator_cache); // Release lock before async operations

                        // Emit DevWalletIdentified if creator is new or CHANGED (correction)
                        let should_emit = match &existing {
                            None => true,
                            Some(old) if old != &creator_str => {
                                warn!(
                                    mint = %mint,
                                    pool = %pool_str,
                                    old_creator = %old,
                                    new_creator = %creator_str,
                                    "FIX-22: Creator mismatch detected — BondingCurve account data overwrites stale cache value"
                                );
                                true
                            }
                            _ => false,
                        };

                        if should_emit {
                            info!(
                                mint = %mint,
                                pool = %pool_str,
                                creator = %creator_str,
                                corrected = existing.is_some(),
                                "Creator cached from BondingCurve account update (authoritative)"
                            );

                            // Emit DevWalletIdentified event
                            let dev_event = MarketEvent::new(
                                "market-data",
                                BUILD_VERSION,
                                run_id,
                                ctx.next_event_id(),
                                "geyser",
                                Some(*slot),
                                MarketEventKind::DevWalletIdentified {
                                    mint: mint.clone(),
                                    dev_wallet: creator_str.clone(),
                                    supply_percentage: 0.0, // Not computed from account data
                                },
                            );

                            if let Err(e) = ctx.jsonl_writer.write(&dev_event) {
                                error!(error = %e, "Failed to write DevWalletIdentified to JSONL");
                            }

                            if let Some(ref nats) = ctx.nats {
                                if let Err(e) = nats.publish(TOPIC_MARKET_EVENTS, &dev_event).await {
                                    warn!(error = %e, "Failed to publish DevWalletIdentified to NATS");
                                    NATS_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                                } else {
                                    NATS_MESSAGES_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                                    MARKET_EVENTS_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                    }

                    // P2#7: BondingCurveUpdate fallback when parse_pool_account failed.
                    // Ensure LivePoolCache/SLAVE receives creator so Liquidation/PumpFunDex skip RPC.
                    let needs_fallback = ctx
                        .live_pool_cache
                        .get(pool_address)
                        .is_none_or(|s| !matches!(s, CachedPoolState::PumpFun(_)));
                    if needs_fallback {
                        let base_mint_pk = ctx
                            .pool_mint_map
                            .read()
                            .get(&pool_str)
                            .and_then(|m| Pubkey::from_str(m).ok())
                            .unwrap_or_default();
                        let base_mint = base_mint_pk.to_string();
                        let minimal_state = CachedPoolState::PumpFun(PumpFunState {
                            token_mint: base_mint_pk,
                            bonding_curve: *pool_address,
                            associated_bonding_curve: Pubkey::default(),
                            virtual_token_reserves: *virtual_token_reserves,
                            virtual_sol_reserves: *virtual_sol_reserves,
                            real_token_reserves: *real_token_reserves,
                            real_sol_reserves: *real_sol_reserves,
                            complete: *complete,
                            creator: *creator,
                            cashback_enabled: *cashback_enabled,
                        });
                        ctx.live_pool_cache
                            .upsert(*pool_address, minimal_state, *slot);

                        let mut pool_update = PoolCacheUpdate::new_pool_discovered(
                            "market-data",
                            BUILD_VERSION,
                            run_id,
                            pool_str.clone(),
                            "pumpfun".to_string(),
                            base_mint.clone(),
                            NATIVE_SOL_MINT.to_string(),
                            *virtual_token_reserves,
                            *virtual_sol_reserves,
                            Some(0),
                            *slot,
                        );
                        let mut meta = std::collections::HashMap::new();
                        meta.insert("creator".to_string(), creator_str.clone());
                        meta.insert("complete".to_string(), complete.to_string());
                        meta.insert("real_token_reserves".to_string(), real_token_reserves.to_string());
                        meta.insert("real_sol_reserves".to_string(), real_sol_reserves.to_string());
                        meta.insert("cashback_enabled".to_string(), cashback_enabled.to_string());
                        // P3 #13: base_decimals and quote_decimals (PumpFun quote = SOL)
                        if let Some(d) = ctx.live_pool_cache.get_mint_decimals(&base_mint_pk) {
                            meta.insert("base_decimals".to_string(), d.to_string());
                        }
                        if let Ok(sol_pk) = Pubkey::from_str(NATIVE_SOL_MINT) {
                            if let Some(d) = ctx.live_pool_cache.get_mint_decimals(&sol_pk) {
                                meta.insert("quote_decimals".to_string(), d.to_string());
                            } else {
                                meta.insert("quote_decimals".to_string(), "9".to_string());
                            }
                        }
                        pool_update.metadata = Some(meta);

                        if let Some(ref nats) = ctx.nats {
                            let subject = pool_subject(&pool_str);
                            if let Err(e) = nats.jetstream_publish(&subject, &pool_update).await {
                                warn!(error = %e, pool = %pool_str, "P2#7: Failed to publish BondingCurveUpdate fallback PoolCacheUpdate");
                                NATS_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                            } else {
                                NATS_MESSAGES_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                                debug!(
                                    pool = %pool_str,
                                    creator = %creator_str,
                                    "P2#7: Published BondingCurveUpdate fallback PoolCacheUpdate"
                                );
                            }
                        }
                    }

                    // Don't emit the BondingCurveUpdate as a MarketEvent - it's internal
                    continue;
                }

                let event_kind = if let Some(parsed) = parsed {
                    debug!(
                        slot = account_update.slot,
                        "Parsed DEX account update"
                    );
                    parsed.to_market_event_kind()
                } else {
                    // Fallback to raw event for unknown accounts
                    MarketEventKind::AccountUpdate {
                        pubkey: account_update.pubkey.to_string(),
                        owner: account_update.owner.to_string(),
                        data_len: account_update.data.len(),
                    }
                };

                let event = MarketEvent::new(
                    "market-data",
                    BUILD_VERSION,
                    run_id,
                    ctx.next_event_id(),
                    "geyser",
                    Some(account_update.slot),
                    event_kind,
                );

                // Write to JSONL
                if let Err(e) = ctx.jsonl_writer.write(&event) {
                    error!(error = %e, "Failed to write account event to JSONL");
                }

                // Publish to NATS
                if let Some(ref nats) = ctx.nats {
                    if let Err(e) = nats.publish(TOPIC_MARKET_EVENTS, &event).await {
                        warn!(error = %e, "Failed to publish account event to NATS");
                        NATS_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                    } else {
                        NATS_MESSAGES_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                        MARKET_EVENTS_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }

            // Blockhash updates from Geyser blocks_meta
            Ok(bh_update) = blockhash_rx.recv() => {
                let event = MarketEvent::new(
                    "market-data",
                    BUILD_VERSION,
                    &ctx.run_id,
                    format!("blockhash-{}", bh_update.slot),
                    "geyser",
                    Some(bh_update.slot),
                    MarketEventKind::LatestBlockhash {
                        blockhash: bh_update.blockhash,
                        slot: bh_update.slot,
                        block_height: bh_update.block_height,
                    },
                );
                if let Some(ref nats) = ctx.nats {
                    if let Err(e) = nats.publish(TOPIC_MARKET_EVENTS, &event).await {
                        warn!(error = %e, "Failed to publish LatestBlockhash to NATS");
                    }
                }
            }

            // Transaction updates (pool creations, swaps)
            Ok(tx_update) = transaction_rx.recv() => {
                tx_count += 1;
                ironcrab::metrics::record_activity();

                // Phase 2 (Roadmap, Option C):
                // Detect Associated Token Program create/close instructions for the tracked wallet
                // and feed them into the same wallet-ATA tracking path (no RPC, no scanning).
                // This is needed to catch manual wallet actions (Phantom/Jupiter) that do not
                // produce ExecutionResults.

                // P2: Track priority fees from Geyser transactions (NO RPC calls!)
                if let Some(priority_fee) = ctx.priority_fee_tracker.add_sample(
                    tx_update.slot,
                    tx_update.fee_lamports,
                    tx_update.compute_units_consumed,
                ) {
                    // Publish percentiles every 50 samples (rate limited)
                    let sample_count = ctx.priority_fee_tracker.sample_count();
                    if sample_count % 50 == 0 && sample_count >= 10 {
                        let percentiles = ctx.priority_fee_tracker.get_percentiles();
                        let fee_msg = PriorityFeePercentiles::new(
                            "market-data",
                            BUILD_VERSION,
                            &ctx.run_id,
                            percentiles.sample_count,
                            percentiles.last_slot,
                            percentiles.p25,
                            percentiles.p50,
                            percentiles.p75,
                            percentiles.p90,
                            ctx.priority_fee_tracker.get_fee_for_tier(IntentTier::Tier0),
                            ctx.priority_fee_tracker.get_fee_for_tier(IntentTier::Tier1),
                            ctx.priority_fee_tracker.get_fee_for_tier(IntentTier::Arb),
                        );
                        if let Some(ref nats) = ctx.nats {
                            // NOTE: nats.publish() already serializes - don't double-serialize!
                            if let Err(e) = nats.publish(
                                TOPIC_PRIORITY_FEE_SAMPLES,
                                &fee_msg,
                            ).await {
                                debug!(error = %e, "Failed to publish priority fee percentiles");
                            }
                        }
                        debug!(
                            samples = sample_count,
                            p50 = percentiles.p50,
                            p90 = percentiles.p90,
                            last_fee = priority_fee,
                            "priority_fee: published percentiles"
                        );
                    }
                }

                // Try to parse as DEX event (PoolCreated, Trade)
                let pool_lookup = |pool: &Pubkey| -> Option<OrcaPoolInfo> {
                    match ctx.live_pool_cache.get(pool) {
                        Some(CachedPoolState::Orca(state)) => {
                            Some(OrcaPoolInfo {
                                token_mint_a: state.token_mint_a,
                                token_mint_b: state.token_mint_b,
                                token_vault_a: state.token_vault_a,
                                token_vault_b: state.token_vault_b,
                                tick_current_index: Some(state.tick_current_index),
                                tick_spacing: Some(state.tick_spacing),
                                token_a_program: state.token_a_program,
                                token_b_program: state.token_b_program,
                            })
                        }
                        _ => None,
                    }
                };
                let parsed_event =
                    parse_transaction_update_with_pool_lookup(&tx_update, Some(&pool_lookup));

                // Track mint pubkeys for mint-authority/freeze metadata.
                // GEYSER-FIRST: No RPC calls! For pump.fun we know decimals=6. For others, Geyser delivers.
                if let Some(parsed) = parsed_event.as_ref() {
                    let mint_and_dex: Option<(Pubkey, Option<DexType>)> = match parsed {
                        ParsedDexEvent::PoolCreated { base_mint, dex, .. } => Some((*base_mint, Some(*dex))),
                        ParsedDexEvent::Trade { mint, dex, .. } => Some((*mint, Some(*dex))),
                        ParsedDexEvent::LiquidityRemoved { mint, .. } => Some((*mint, None)),
                        ParsedDexEvent::BondingCurveUpdate { .. } => None, // Handled separately in account update
                    };
                    if let Some((mint, dex_opt)) = mint_and_dex {
                        let mut tracked = ctx.tracked_mints.write();
                        if tracked.insert(mint) {
                            // Push updated list to geyser listener (resubscribe)
                            let updated: Vec<Pubkey> = tracked.iter().copied().collect();
                            let _ = ctx.tracked_mints_tx.send(updated);

                            // Geyser delivers mint accounts via AccountsDB snapshot when subscribed.
                            // tracked_mints_tx already sent above → GeyserListener will resubscribe.
                            debug!(
                                mint = %mint,
                                dex = ?dex_opt,
                                "New mint tracked, waiting for Geyser mint account delivery"
                            );
                        }
                    }

                    // Build pool_mint_map for PumpFun (needed for BondingCurveUpdate -> creator lookup)
                    match parsed {
                        ParsedDexEvent::PoolCreated {
                            pool_address,
                            base_mint,
                            dex: DexType::PumpFun,
                            ..
                        } => {
                            ctx.pool_mint_map.write().insert(
                                pool_address.to_string(),
                                base_mint.to_string()
                            );
                        }
                        ParsedDexEvent::Trade {
                            pool_address,
                            mint,
                            dex: DexType::PumpFun,
                            ..
                        } => {
                            ctx.pool_mint_map.write().insert(
                                pool_address.to_string(),
                                mint.to_string()
                            );
                        }
                        _ => {}
                    }
                }

                // Pump.fun: propagate creator/dev wallet so strategy can build deterministic intents.
                // The PoolCreated MarketEventKind intentionally does not carry creator today, so emit
                // a separate DevWalletIdentified event when available.
                // Also cache the creator for later Trade events.
                if let Some(ParsedDexEvent::PoolCreated {
                    base_mint,
                    dex: DexType::PumpFun,
                    creator: Some(creator),
                    ..
                }) = parsed_event.as_ref()
                {
                    // Cache creator for later Trade events (P0: avoid RPC in momentum-bot)
                    {
                        let mut cache = ctx.creator_cache.write();
                        cache.insert(base_mint.to_string(), creator.to_string());
                        debug!(
                            mint = %base_mint,
                            creator = %creator,
                            cache_size = cache.len(),
                            "Cached PumpFun creator for Trade enrichment"
                        );
                    }

                    let dev_event = MarketEvent::new(
                        "market-data",
                        BUILD_VERSION,
                        run_id,
                        ctx.next_event_id(),
                        "geyser",
                        Some(tx_update.slot),
                        MarketEventKind::DevWalletIdentified {
                            mint: base_mint.to_string(),
                            dev_wallet: creator.to_string(),
                            // Supply percentage is not computed here yet (would require extra on-chain reads).
                            // Momentum-bot treats this as an input for dev-risk filters; keep deterministic.
                            supply_percentage: 0.0,
                        },
                    );

                    if let Err(e) = ctx.jsonl_writer.write(&dev_event) {
                        error!(error = %e, "Failed to write dev wallet event to JSONL");
                    }

                    if let Some(ref nats) = ctx.nats {
                        if let Err(e) = nats.publish(TOPIC_MARKET_EVENTS, &dev_event).await {
                            warn!(error = %e, "Failed to publish dev wallet event to NATS");
                            NATS_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                        } else {
                            NATS_MESSAGES_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                            MARKET_EVENTS_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }

                // P1: Process wallet tracking events
                let wallet_events = if let Some(ref parsed) = parsed_event {
                    match parsed {
                        ParsedDexEvent::PoolCreated { base_mint, .. } => {
                            // Record pool creation for early buyer tracking
                            ctx.wallet_tracker.record_pool_created(&base_mint.to_string(), tx_update.slot);
                            Vec::new()
                        }
                        ParsedDexEvent::Trade { mint, trader, is_buy, sol_amount, token_amount, signature, slot, .. } => {
                            // Check for smart money, early buyers, insider activity
                            ctx.wallet_tracker.process_trade(
                                &mint.to_string(),
                                &trader.to_string(),
                                *is_buy,
                                *sol_amount,
                                *token_amount,
                                *slot,
                                signature,
                                &ctx.run_id,
                                "market-data",
                            )
                        }
                        ParsedDexEvent::LiquidityRemoved { .. } => Vec::new(),
                        // BondingCurveUpdate is handled separately (account updates, not TX)
                        ParsedDexEvent::BondingCurveUpdate { .. } => Vec::new(),
                    }
                } else {
                    Vec::new()
                };

                // Publish wallet tracking events
                for wallet_event in wallet_events {
                    // Write to JSONL
                    if let Err(e) = ctx.jsonl_writer.write(&wallet_event) {
                        error!(error = %e, "Failed to write wallet event to JSONL");
                    }
                    // Publish to NATS
                    if let Some(ref nats) = ctx.nats {
                        if let Err(e) = nats.publish(TOPIC_MARKET_EVENTS, &wallet_event).await {
                            warn!(error = %e, "Failed to publish wallet event to NATS");
                        }
                    }
                }

                // P12 Option A: PumpSwap create_pool - pool_accounts available at creation time.
                // Emit PoolCreated + DexPoolAccounts immediately (no need to wait for first trade).
                if let Some(ParsedDexEvent::PoolCreated {
                    pool_address,
                    base_mint: base_mint_pk,
                    quote_mint: quote_mint_pk,
                    dex: DexType::PumpFunAmm,
                    pool_accounts: Some(pool_accounts),
                    ..
                }) = parsed_event.as_ref()
                {
                    let is_new_pool = ctx.known_pump_amm_pools.write().insert(*pool_address);
                    if is_new_pool {
                        let base_mint = base_mint_pk.to_string();
                        let quote_mint = quote_mint_pk.to_string();
                        info!(
                            pool = %pool_address,
                            base_mint = %base_mint_pk,
                            "pump_amm pool discovered via create_pool - emitting PoolCreated + DexPoolAccounts"
                        );
                        let pool_created_event = MarketEvent::new(
                            "market-data",
                            BUILD_VERSION,
                            run_id,
                            ctx.next_event_id(),
                            "geyser_create_pool",
                            Some(tx_update.slot),
                            MarketEventKind::PoolCreated {
                                pool_address: pool_address.to_string(),
                                base_mint: base_mint.clone(),
                                quote_mint: quote_mint.clone(),
                                dex: DexType::PumpFunAmm.to_string(),
                                initial_liquidity_sol: None,
                            },
                        );
                        if let Err(e) = ctx.jsonl_writer.write(&pool_created_event) {
                            error!(error = %e, "Failed to write pump_amm PoolCreated (create_pool) to JSONL");
                        }
                        if let Some(ref nats) = ctx.nats {
                            if let Err(e) = nats.publish(TOPIC_MARKET_EVENTS, &pool_created_event).await {
                                warn!(error = %e, "Failed to publish pump_amm PoolCreated (create_pool) to NATS");
                                NATS_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                            } else {
                                NATS_MESSAGES_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                                MARKET_EVENTS_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        let accounts_event = MarketEvent::new(
                            "market-data",
                            BUILD_VERSION,
                            run_id,
                            ctx.next_event_id(),
                            "geyser_create_pool",
                            Some(tx_update.slot),
                            MarketEventKind::DexPoolAccounts {
                                dex: DexType::PumpFunAmm.to_string(),
                                pool_address: pool_address.to_string(),
                                base_mint: base_mint.clone(),
                                quote_mint: quote_mint.clone(),
                                accounts: pool_accounts.iter().map(|p| p.to_string()).collect(),
                            },
                        );
                        if let Err(e) = ctx.jsonl_writer.write(&accounts_event) {
                            error!(error = %e, "Failed to write DexPoolAccounts (create_pool) to JSONL");
                        }
                        if let Some(ref nats) = ctx.nats {
                            if let Err(e) = nats.publish(TOPIC_MARKET_EVENTS, &accounts_event).await {
                                warn!(error = %e, "Failed to publish DexPoolAccounts (create_pool) to NATS");
                                NATS_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                            } else {
                                NATS_MESSAGES_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                                MARKET_EVENTS_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        if pool_accounts.len() >= 14 {
                            ctx.live_pool_cache.set_pump_amm_pool_accounts(pool_address, pool_accounts.clone());

                            // FIX-33: Persist pool_accounts to JetStream so bootstrap recovers them after restart.
                            if let Some(ref nats) = ctx.nats {
                                let mut pool_update = PoolCacheUpdate::new_pool_discovered(
                                    "market-data",
                                    BUILD_VERSION,
                                    run_id,
                                    pool_address.to_string(),
                                    "pump_amm".to_string(),
                                    base_mint.clone(),
                                    quote_mint.clone(),
                                    0,
                                    0,
                                    None,
                                    tx_update.slot,
                                );
                                let mut meta = std::collections::HashMap::new();
                                let accounts_str: Vec<String> = pool_accounts.iter().map(|p| p.to_string()).collect();
                                meta.insert("pool_accounts".to_string(), accounts_str.join(","));
                                pool_update.metadata = Some(meta);
                                let subject = pool_subject(&pool_address.to_string());
                                if let Err(e) = nats.jetstream_publish(&subject, &pool_update).await {
                                    warn!(error = %e, "FIX-33: Failed to publish pump_amm pool_accounts PoolCacheUpdate to JetStream (create_pool)");
                                    NATS_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                                } else {
                                    NATS_MESSAGES_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                        ctx.pool_mint_map.write().insert(pool_address.to_string(), base_mint.clone());
                    }
                }

                // Pump.fun AMM: emit static pool account metadata for intent-driven execution.
                // IMPORTANT: We emit PoolCreated + DexPoolAccounts TOGETHER on first trade.
                // This ensures arb-strategy has all required accounts before seeing the pool.
                if let Some(ParsedDexEvent::Trade {
                    pool_address,
                    mint: base_mint_pk,
                    dex: DexType::PumpFunAmm,
                    pool_accounts: Some(pool_accounts),
                    ..
                }) = parsed_event.as_ref()
                {
                    // v1 order (see MarketEventKind::DexPoolAccounts docs): base_mint at [2], quote_mint at [3]
                    let base_mint = pool_accounts.get(2).map(|p| p.to_string()).unwrap_or_default();
                    let quote_mint = pool_accounts.get(3).map(|p| p.to_string()).unwrap_or_default();

                    // Check if this is the FIRST trade for this pool (new pool discovery)
                    let is_first_trade = ctx.known_pump_amm_pools.write().insert(*pool_address);

                    // If first trade, emit PoolCreated FIRST (before DexPoolAccounts)
                    // This ensures arb-strategy sees PoolCreated + DexPoolAccounts together
                    if is_first_trade {
                        info!(
                            pool = %pool_address,
                            base_mint = %base_mint_pk,
                            "pump_amm pool discovered via first trade - emitting PoolCreated + DexPoolAccounts"
                        );

                        let pool_created_event = MarketEvent::new(
                            "market-data",
                            BUILD_VERSION,
                            run_id,
                            ctx.next_event_id(),
                            "geyser_first_trade",
                            Some(tx_update.slot),
                            MarketEventKind::PoolCreated {
                                pool_address: pool_address.to_string(),
                                base_mint: base_mint.clone(),
                                quote_mint: quote_mint.clone(),
                                dex: DexType::PumpFunAmm.to_string(),
                                initial_liquidity_sol: None, // Not available from trade
                            },
                        );

                        if let Err(e) = ctx.jsonl_writer.write(&pool_created_event) {
                            error!(error = %e, "Failed to write pump_amm PoolCreated event to JSONL");
                        }

                        if let Some(ref nats) = ctx.nats {
                            if let Err(e) = nats.publish(TOPIC_MARKET_EVENTS, &pool_created_event).await {
                                warn!(error = %e, "Failed to publish pump_amm PoolCreated event to NATS");
                                NATS_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                            } else {
                                NATS_MESSAGES_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                                MARKET_EVENTS_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }

                    // Always emit DexPoolAccounts on pump_amm trades
                    let accounts_event = MarketEvent::new(
                        "market-data",
                        BUILD_VERSION,
                        run_id,
                        ctx.next_event_id(),
                        "geyser",
                        Some(tx_update.slot),
                        MarketEventKind::DexPoolAccounts {
                            dex: DexType::PumpFunAmm.to_string(),
                            pool_address: pool_address.to_string(),
                            base_mint: base_mint.clone(),
                            quote_mint: quote_mint.clone(),
                            accounts: pool_accounts.iter().map(|p| p.to_string()).collect(),
                        },
                    );

                    if let Err(e) = ctx.jsonl_writer.write(&accounts_event) {
                        error!(error = %e, "Failed to write DexPoolAccounts event to JSONL");
                    }

                    if let Some(ref nats) = ctx.nats {
                        if let Err(e) = nats.publish(TOPIC_MARKET_EVENTS, &accounts_event).await {
                            warn!(error = %e, "Failed to publish DexPoolAccounts event to NATS");
                            NATS_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                        } else {
                            NATS_MESSAGES_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                            MARKET_EVENTS_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                        }
                    }

                    // FIX-26: Also populate MASTER LivePoolCache with pool_accounts.
                    // Ensures the Geyser PoolCacheUpdate builder can use these as fallback
                    // when the parsed Geyser state has empty pool_accounts.
                    if pool_accounts.len() >= 14 {
                        ctx.live_pool_cache.set_pump_amm_pool_accounts(pool_address, pool_accounts.clone());

                        // FIX-33: Persist pool_accounts to JetStream so bootstrap recovers them after restart.
                        if let Some(ref nats) = ctx.nats {
                            let mut pool_update = PoolCacheUpdate::new_pool_discovered(
                                "market-data",
                                BUILD_VERSION,
                                run_id,
                                pool_address.to_string(),
                                "pump_amm".to_string(),
                                base_mint.clone(),
                                quote_mint.clone(),
                                0,
                                0,
                                None,
                                tx_update.slot,
                            );
                            let mut meta = std::collections::HashMap::new();
                            let accounts_str: Vec<String> = pool_accounts.iter().map(|p| p.to_string()).collect();
                            meta.insert("pool_accounts".to_string(), accounts_str.join(","));
                            pool_update.metadata = Some(meta);
                            let subject = pool_subject(&pool_address.to_string());
                            if let Err(e) = nats.jetstream_publish(&subject, &pool_update).await {
                                warn!(error = %e, "FIX-33: Failed to publish pump_amm pool_accounts PoolCacheUpdate to JetStream (trade)");
                                NATS_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                            } else {
                                NATS_MESSAGES_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                }

                // For non-pump_amm DEXes: emit DexPoolAccounts on first trade if pool_accounts present.
                if let Some(ParsedDexEvent::Trade {
                    pool_address,
                    mint,
                    quote_mint,
                    dex,
                    pool_accounts: Some(pool_accounts),
                    ..
                }) = parsed_event.as_ref()
                {
                    if !matches!(dex, DexType::PumpFunAmm) {
                        let is_first_trade = ctx.known_trade_dex_pools.write().insert(*pool_address);
                        if is_first_trade {
                            let accounts_event = MarketEvent::new(
                                "market-data",
                                BUILD_VERSION,
                                run_id,
                                ctx.next_event_id(),
                                "geyser_first_trade",
                                Some(tx_update.slot),
                                MarketEventKind::DexPoolAccounts {
                                    dex: dex.to_string(),
                                    pool_address: pool_address.to_string(),
                                    base_mint: mint.to_string(),
                                    quote_mint: quote_mint.to_string(),
                                    accounts: pool_accounts.iter().map(|p| p.to_string()).collect(),
                                },
                            );

                            if let Err(e) = ctx.jsonl_writer.write(&accounts_event) {
                                error!(error = %e, "Failed to write DexPoolAccounts event to JSONL");
                            }

                            if let Some(ref nats) = ctx.nats {
                                if let Err(e) = nats.publish(TOPIC_MARKET_EVENTS, &accounts_event).await
                                {
                                    warn!(error = %e, "Failed to publish DexPoolAccounts event to NATS");
                                    NATS_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                                } else {
                                    NATS_MESSAGES_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                                    MARKET_EVENTS_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                                    debug!(
                                        dex = %dex,
                                        pool = %pool_address,
                                        "Emitted DexPoolAccounts from first trade"
                                    );
                                }
                            }
                        }
                    }
                }

                let event_kind = if let Some(parsed) = parsed_event {
                    info!(
                        slot = tx_update.slot,
                        sig = %tx_update.signature,
                        "Parsed DEX transaction"
                    );
                    let mut kind = parsed.to_market_event_kind();

                    // Enrich Trade events with creator from cache (P0: PumpFun intent building)
                    if let MarketEventKind::Trade { ref pool_address, ref mint, ref dex, ref mut creator, .. } = kind {
                        if dex == "pumpfun" || dex == "pump_amm" {
                            // Try creator_cache first (mint -> creator)
                            let mut found_creator = None;
                            {
                                let cache = ctx.creator_cache.read();
                                if let Some(cached_creator) = cache.get(mint) {
                                    found_creator = Some(cached_creator.clone());
                                }
                            }

                            // Fallback to pool_creator_cache (pool -> creator) from BondingCurveUpdate
                            if found_creator.is_none() {
                                let pool_cache = ctx.pool_creator_cache.read();
                                if let Some(pool_creator) = pool_cache.get(pool_address) {
                                    found_creator = Some(pool_creator.clone());

                                    // Also populate creator_cache for future lookups
                                    drop(pool_cache);
                                    ctx.creator_cache.write().insert(mint.clone(), found_creator.clone().unwrap());
                                    debug!(
                                        mint = %mint,
                                        pool = %pool_address,
                                        creator = %found_creator.as_ref().unwrap(),
                                        "Populated creator_cache from pool_creator_cache"
                                    );
                                }
                            }

                            if let Some(cached_creator) = found_creator {
                                *creator = Some(cached_creator.clone());
                                debug!(
                                    mint = %mint,
                                    dex = %dex,
                                    creator = %cached_creator,
                                    "Enriched Trade event with cached creator"
                                );
                            }
                        }
                    }
                    kind
                } else {
                    // Fallback to raw event for unknown transactions
                    MarketEventKind::TransactionDetected {
                        signature: tx_update.signature.clone(),
                        program: tx_update.account_keys.first().map(|k| k.to_string()).unwrap_or_default(),
                    }
                };

                let event = MarketEvent::new(
                    "market-data",
                    BUILD_VERSION,
                    run_id,
                    ctx.next_event_id(),
                    "geyser",
                    Some(tx_update.slot),
                    event_kind,
                );

                // Write to JSONL
                if let Err(e) = ctx.jsonl_writer.write(&event) {
                    error!(error = %e, "Failed to write tx event to JSONL");
                }

                // Publish to NATS
                if let Some(ref nats) = ctx.nats {
                    if let Err(e) = nats.publish(TOPIC_MARKET_EVENTS, &event).await {
                        warn!(error = %e, "Failed to publish tx event to NATS");
                        NATS_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                    } else {
                        NATS_MESSAGES_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                        MARKET_EVENTS_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }

            // Pool Discovery Events (Geyser-based pool creation events)
            Ok(pool_event) = pool_discovery_rx.recv() => {
                ironcrab::metrics::record_activity();

                info!(
                    dex = %pool_event.dex_type,
                    pool = %pool_event.pool_address,
                    base = %pool_event.base_mint,
                    quote = %pool_event.quote_mint,
                    liquidity_lamports = pool_event.liquidity_estimate_lamports,
                    "Pool discovered via Geyser"
                );

                // Track base mint for metadata fetching - GEYSER-FIRST: No RPC calls!
                let mut tracked = ctx.tracked_mints.write();
                if tracked.insert(pool_event.base_mint) {
                    let updated: Vec<Pubkey> = tracked.iter().copied().collect();
                    let _ = ctx.tracked_mints_tx.send(updated);

                    // For Pump.fun/PumpAMM: emit TokenMintInfo immediately with known decimals=6.
                    // For other DEXes: Geyser will deliver the mint account when subscribed.
                    // Note: PoolDexType only has PumpFun (legacy bonding), PumpAMM is detected via transaction parsing
                    let is_pump = matches!(pool_event.dex_type, PoolDexType::PumpFun);
                    if is_pump {
                        let mint_event = MarketEvent::new(
                            "market-data",
                            BUILD_VERSION,
                            run_id,
                            ctx.next_event_id(),
                            "geyser_known", // Not RPC - we know pump.fun uses decimals=6
                            Some(pool_event.slot),
                            MarketEventKind::TokenMintInfo {
                                mint: pool_event.base_mint.to_string(),
                                token_program: spl_token::ID.to_string(), // pump.fun always uses SPL Token
                                decimals: 6, // pump.fun tokens ALWAYS have 6 decimals
                                supply: 0,   // Unknown, but not critical for trading
                                mint_authority: None,
                                freeze_authority: None,
                            },
                        );
                        let _ = mint_info_tx.send(mint_event);
                        debug!(mint = %pool_event.base_mint, "Emitted TokenMintInfo for pump.fun token (decimals=6, no RPC)");
                    }
                    // For other DEXes: Geyser subscription handles it - no RPC call needed.
                }

                // Convert to MarketEvent::PoolCreated
                let event = MarketEvent::new(
                    "market-data",
                    BUILD_VERSION,
                    run_id,
                    ctx.next_event_id(),
                    "geyser_pool_discovery",
                    Some(pool_event.slot),
                    MarketEventKind::PoolCreated {
                        pool_address: pool_event.pool_address.to_string(),
                        base_mint: pool_event.base_mint.to_string(),
                        quote_mint: pool_event.quote_mint.to_string(),
                        dex: pool_event.dex_type.to_string(),
                        initial_liquidity_sol: Some(
                            rust_decimal::Decimal::from(pool_event.liquidity_estimate_lamports)
                                / rust_decimal::Decimal::from(1_000_000_000u64)
                        ),
                    },
                );

                // Write to JSONL
                if let Err(e) = ctx.jsonl_writer.write(&event) {
                    error!(error = %e, "Failed to write pool discovery event to JSONL");
                }

                // Publish to NATS
                if let Some(ref nats) = ctx.nats {
                    if let Err(e) = nats.publish(TOPIC_MARKET_EVENTS, &event).await {
                        warn!(error = %e, "Failed to publish pool discovery event to NATS");
                        NATS_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                    } else {
                        NATS_MESSAGES_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                        MARKET_EVENTS_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                    }
                }

                // Emit DevWalletIdentified for Pump.fun pools where we know the creator
                // This enables momentum-bot to populate metadata.creator for intent building
                if pool_event.dex_type == PoolDexType::PumpFun {
                    if let Some(creator) = pool_event.creator {
                        let dev_event = MarketEvent::new(
                            "market-data",
                            BUILD_VERSION,
                            run_id,
                            ctx.next_event_id(),
                            "geyser_pool_discovery",
                            Some(pool_event.slot),
                            MarketEventKind::DevWalletIdentified {
                                mint: pool_event.base_mint.to_string(),
                                dev_wallet: creator.to_string(),
                                // Supply percentage not computed (would need extra on-chain reads)
                                supply_percentage: 0.0,
                            },
                        );

                        if let Err(e) = ctx.jsonl_writer.write(&dev_event) {
                            error!(error = %e, "Failed to write dev wallet event to JSONL");
                        }

                        if let Some(ref nats) = ctx.nats {
                            if let Err(e) = nats.publish(TOPIC_MARKET_EVENTS, &dev_event).await {
                                warn!(error = %e, "Failed to publish dev wallet event to NATS");
                                NATS_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                            } else {
                                NATS_MESSAGES_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                                MARKET_EVENTS_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                                info!(
                                    mint = %pool_event.base_mint,
                                    creator = %creator,
                                    "✅ DevWalletIdentified emitted for pump.fun pool"
                                );
                            }
                        }
                    }
                }

                // Emit DexPoolAccounts for all DEXes that have vault information
                // This enables arb-strategy to have pool accounts BEFORE first trade
                if pool_event.coin_vault.is_some() || pool_event.pc_vault.is_some() {
                    let mut accounts = vec![
                        pool_event.pool_address.to_string(),
                        pool_event.base_mint.to_string(),
                        pool_event.quote_mint.to_string(),
                    ];

                    if let Some(coin_vault) = pool_event.coin_vault {
                        accounts.push(coin_vault.to_string());
                    }
                    if let Some(pc_vault) = pool_event.pc_vault {
                        accounts.push(pc_vault.to_string());
                    }
                    if let Some(creator) = pool_event.creator {
                        accounts.push(creator.to_string());
                    }
                    // Meteora DLMM: add active_id and bin_step as tagged values
                    // Format: "active_id:<value>" and "bin_step:<value>"
                    if let Some(active_id) = pool_event.active_id {
                        accounts.push(format!("active_id:{}", active_id));
                    }
                    if let Some(bin_step) = pool_event.bin_step {
                        accounts.push(format!("bin_step:{}", bin_step));
                    }
                    // Orca Whirlpool: add tick_current_index and tick_spacing as tagged values
                    // Format: "tick_current_index:<value>" and "tick_spacing:<value>"
                    if let Some(tick) = pool_event.tick_current_index {
                        accounts.push(format!("tick_current_index:{}", tick));
                    }
                    if let Some(spacing) = pool_event.tick_spacing {
                        accounts.push(format!("tick_spacing:{}", spacing));
                    }

                    let accounts_event = MarketEvent::new(
                        "market-data",
                        BUILD_VERSION,
                        run_id,
                        ctx.next_event_id(),
                        "geyser_pool_discovery",
                        Some(pool_event.slot),
                        MarketEventKind::DexPoolAccounts {
                            dex: pool_event.dex_type.to_string(),
                            pool_address: pool_event.pool_address.to_string(),
                            base_mint: pool_event.base_mint.to_string(),
                            quote_mint: pool_event.quote_mint.to_string(),
                            accounts,
                        },
                    );

                    if let Err(e) = ctx.jsonl_writer.write(&accounts_event) {
                        error!(error = %e, "Failed to write DexPoolAccounts event to JSONL");
                    }

                    if let Some(ref nats) = ctx.nats {
                        if let Err(e) = nats.publish(TOPIC_MARKET_EVENTS, &accounts_event).await {
                            warn!(error = %e, "Failed to publish DexPoolAccounts event to NATS");
                            NATS_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
                        } else {
                            NATS_MESSAGES_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                            MARKET_EVENTS_PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
                            debug!(
                                dex = %pool_event.dex_type,
                                pool = %pool_event.pool_address,
                                "Emitted DexPoolAccounts for pool discovery"
                            );
                        }
                    }

                    // Track vault accounts for PoolStateUpdate events (Geyser-based reserve balances)
                    // This enables real-time reserve tracking without RPC calls.
                    let dex_str = pool_event.dex_type.to_string();
                    // DLMM-specific: pass active_id/bin_step for Option D (Bin Array Traversierung)
                    let dlmm_active_id = pool_event.active_id;
                    let dlmm_bin_step = pool_event.bin_step;
                    let mut vaults_changed = false;
                    {
                        let mut vaults = ctx.tracked_vaults.write();
                        if let Some(coin_vault) = pool_event.coin_vault {
                            vaults.entry(coin_vault).or_insert_with(|| {
                                vaults_changed = true;
                                VaultInfo {
                                    pool_address: pool_event.pool_address,
                                    dex: dex_str.clone(),
                                    base_mint: pool_event.base_mint,
                                    quote_mint: pool_event.quote_mint,
                                    is_base_vault: true,
                                    last_balance: std::sync::atomic::AtomicU64::new(0),
                                    active_id: dlmm_active_id,
                                    bin_step: dlmm_bin_step,
                                }
                            });
                        }
                        if let Some(pc_vault) = pool_event.pc_vault {
                            vaults.entry(pc_vault).or_insert_with(|| {
                                vaults_changed = true;
                                VaultInfo {
                                    pool_address: pool_event.pool_address,
                                    dex: dex_str.clone(),
                                    base_mint: pool_event.base_mint,
                                    quote_mint: pool_event.quote_mint,
                                    is_base_vault: false,
                                    last_balance: std::sync::atomic::AtomicU64::new(0),
                                    active_id: dlmm_active_id,
                                    bin_step: dlmm_bin_step,
                                }
                            });
                        }
                    }
                    // Notify GeyserListener to resubscribe with new vault accounts
                    if vaults_changed {
                        let vault_list: Vec<Pubkey> = ctx.tracked_vaults.read().keys().copied().collect();
                        let _ = ctx.tracked_vaults_tx.send(vault_list);
                        debug!(
                            pool = %pool_event.pool_address,
                            coin_vault = ?pool_event.coin_vault,
                            pc_vault = ?pool_event.pc_vault,
                            "Registered vault accounts for PoolStateUpdate tracking"
                        );
                    }

                    // Track Meteora DLMM Bin Array accounts for BinArrayUpdate events
                    // This enables real-time liquidity tracking without RPC calls.
                    if pool_event.dex_type == PoolDexType::MeteoraDlmm {
                        // For Meteora DLMM, we need to subscribe to bin array accounts.
                        // We derive PDAs for ±3 arrays around the active bin.
                        // Use actual active_id/bin_step from pool_event (parsed in geyser_pool_discovery)
                        let active_id = pool_event.active_id.unwrap_or(0);
                        let active_array_index = MeteoraDlmmSwapBuilder::bin_id_to_bin_array_index(active_id);
                        let bin_step = pool_event.bin_step.unwrap_or(1);

                        let mut bin_arrays_changed = false;
                        {
                            let mut bin_arrays = ctx.tracked_bin_arrays.write();
                            // Register ±3 bin arrays around active bin
                            for offset in -3i64..=3i64 {
                                let index = active_array_index + offset;
                                if let Ok(pda) = MeteoraDlmmSwapBuilder::derive_bin_array_pda(
                                    &pool_event.pool_address,
                                    index,
                                ) {
                                    bin_arrays.entry(pda).or_insert_with(|| {
                                        bin_arrays_changed = true;
                                        BinArrayInfo {
                                            pool_address: pool_event.pool_address,
                                            bin_array_index: index,
                                            bin_step,
                                        }
                                    });
                                }
                            }
                        }
                        // Notify GeyserListener to resubscribe with new bin array accounts
                        if bin_arrays_changed {
                            let bin_array_list: Vec<Pubkey> = ctx.tracked_bin_arrays.read().keys().copied().collect();
                            let num_arrays = bin_array_list.len();
                            let _ = ctx.tracked_bin_arrays_tx.send(bin_array_list);
                            debug!(
                                pool = %pool_event.pool_address,
                                arrays_tracked = num_arrays,
                                "Registered Meteora DLMM bin array accounts for BinArrayUpdate tracking"
                            );
                        }
                    }
                }
            }

            // P1: Handle Config Updates (Runtime Configuration via UI)
            msg = async {
                if let Some(ref mut sub) = config_subscription {
                    sub.next().await
                } else {
                    std::future::pending::<Option<ironcrab::nats::NatsMessage>>().await
                }
            } => {
                if let Some(nats_msg) = msg {
                    match serde_json::from_slice::<ConfigUpdate>(&nats_msg.payload) {
                        Ok(update) => {
                            if update.target_component == "market-data" {
                                info!(
                                    component = %update.target_component,
                                    keys = ?update.config.keys().collect::<Vec<_>>(),
                                    "Received Config Update from control-plane"
                                );
                                let response = ctx.apply_config_update(&update);
                                info!(
                                    status = ?response.status,
                                    applied = ?response.applied_keys,
                                    rejected = ?response.rejected_keys,
                                    "Config update processed"
                                );
                            } else {
                                debug!(component = %update.target_component, "Ignoring config update for other component");
                            }
                        }
                        Err(e) => {
                            warn!(error = %e, "Failed to deserialize ConfigUpdate");
                        }
                    }
                }
            }

            // Periodic heartbeat
            _ = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                if last_heartbeat.elapsed().as_secs() >= 60 {
                    ironcrab::metrics::record_activity();
                    let (records, bytes) = ctx.jsonl_writer.stats();
                    let total_events = account_count + tx_count;

                    // Update Prometheus metrics
                    MARKET_EVENTS_RECEIVED_TOTAL.store(total_events, Ordering::Relaxed);
                    POOLS_TRACKED_GAUGE.store(account_count, Ordering::Relaxed);

                    info!(
                        accounts = account_count,
                        transactions = tx_count,
                        records_written = records,
                        bytes_written = bytes,
                        "market-data heartbeat (Geyser)"
                    );
                    last_heartbeat = std::time::Instant::now();
                }
            }

            _ = &mut shutdown => {
                info!("Shutdown signal received");
                listener_handle.abort();
                break;
            }
        }
    }

    Ok(())
}

/// Run simulation loop (for testing without Geyser)
async fn run_simulation_loop(
    ctx: Arc<MarketDataContext>,
    run_id: &str,
    mut config_subscription: Option<ironcrab::nats::NatsSubscription>,
) -> Result<()> {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
    let mut slot: u64 = 0;

    // Graceful shutdown handling
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = interval.tick() => {
                slot += 1; // Simulated slot progression

                // Keep /ready fresh even when only simulating.
                ironcrab::metrics::record_activity();

                let event = MarketEvent::new(
                    "market-data",
                    BUILD_VERSION,
                    run_id,
                    ctx.next_event_id(),
                    "simulated",
                    Some(slot),
                    MarketEventKind::SlotUpdate { current_slot: slot },
                );

                // Write to JSONL (P0 requirement)
                if let Err(e) = ctx.jsonl_writer.write(&event) {
                    error!(error = %e, "Failed to write event to JSONL");
                }

                // Publish to NATS
                if let Some(ref nats) = ctx.nats {
                    if let Err(e) = nats.publish(TOPIC_MARKET_EVENTS, &event).await {
                        warn!(error = %e, "Failed to publish to NATS");
                    }
                }

                // Periodic stats
                if slot % 60 == 0 {
                    let (records, bytes) = ctx.jsonl_writer.stats();
                    info!(
                        slot,
                        records_written = records,
                        bytes_written = bytes,
                        "market-data heartbeat (simulation)"
                    );
                }

                // P1 Crash Isolation: Ping systemd watchdog frequently enough.
                if slot % 10 == 0 {
                    #[cfg(unix)]
                    let _ = sd_notify::notify(false, &[NotifyState::Watchdog]);
                }
            }

            // P1: Handle Config Updates (Runtime Configuration via UI)
            msg = async {
                if let Some(ref mut sub) = config_subscription {
                    sub.next().await
                } else {
                    std::future::pending::<Option<ironcrab::nats::NatsMessage>>().await
                }
            } => {
                if let Some(nats_msg) = msg {
                    match serde_json::from_slice::<ConfigUpdate>(&nats_msg.payload) {
                        Ok(update) => {
                            if update.target_component == "market-data" {
                                info!(
                                    component = %update.target_component,
                                    keys = ?update.config.keys().collect::<Vec<_>>(),
                                    "Received Config Update from control-plane"
                                );
                                let response = ctx.apply_config_update(&update);
                                info!(
                                    status = ?response.status,
                                    applied = ?response.applied_keys,
                                    rejected = ?response.rejected_keys,
                                    "Config update processed"
                                );
                            } else {
                                debug!(component = %update.target_component, "Ignoring config update for other component");
                            }
                        }
                        Err(e) => {
                            warn!(error = %e, "Failed to deserialize ConfigUpdate");
                        }
                    }
                }
            }

            _ = &mut shutdown => {
                info!("Shutdown signal received");
                break;
            }
        }
    }

    Ok(())
}
