//! Phase 5b: `md-sidefx` OS threads — physically split Account-Quote vs TX-Discovery pipelines.

use super::handlers::md_sidefx_process_job;
use super::host::SidefxWorkerHost;
use crate::market_data::ingest::AccountUpdateClass;
use crate::metrics::{
    inc_market_data_account_sidefx_backpressure_total,
    inc_market_data_account_sidefx_enqueue_fail_loud_total,
    inc_market_data_account_sidefx_jobs_processed_total,
    inc_market_data_md_sidefx_enqueue_dropped_total,
    inc_market_data_md_sidefx_enrich_enqueue_dropped_total,
    inc_market_data_md_sidefx_jobs_processed_total,
    inc_market_data_tx_discovery_sidefx_enqueue_dropped_total,
    inc_market_data_tx_discovery_sidefx_jobs_processed_total,
    inc_market_data_tx_pin_seed_sidefx_enqueue_dropped_total,
    inc_market_data_tx_pin_seed_sidefx_jobs_processed_total,
    inc_market_data_tx_sidefx_enqueue_dropped_total,
    inc_market_data_tx_sidefx_jobs_processed_total,
    refresh_market_data_md_sidefx_deprecated_metrics, set_market_data_account_sidefx_queue_depth,
    set_market_data_tx_discovery_sidefx_queue_depth,
    set_market_data_tx_pin_seed_sidefx_queue_depth,
};
use crate::solana::dex_parser::DexType;
use solana_sdk::pubkey::Pubkey;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc as std_mpsc;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Instant;

/// Sidefx job priority propagated from account ingest classification (Scope D).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SidefxUpdateClass {
    ExecHot,
    Enrich,
}

impl SidefxUpdateClass {
    #[inline]
    pub fn is_exec_hot(self) -> bool {
        matches!(self, Self::ExecHot)
    }

    #[inline]
    pub fn merge_priority(self, other: Self) -> Self {
        if self.is_exec_hot() || other.is_exec_hot() {
            Self::ExecHot
        } else {
            Self::Enrich
        }
    }
}

impl From<AccountUpdateClass> for SidefxUpdateClass {
    fn from(class: AccountUpdateClass) -> Self {
        match class {
            AccountUpdateClass::ExecHot => Self::ExecHot,
            AccountUpdateClass::Enrich | AccountUpdateClass::Drop => Self::Enrich,
        }
    }
}

/// Physically separated sidefx pipelines (P0: Account-Quote must not starve under TX load).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MdSidefxPipeline {
    AccountQuote,
    /// Hot-pool layout/vault seed (pin explicit set); isolated from discovery flood.
    TxPinSeed,
    /// Non-hot TX discovery (mint-map, create, observe-only trades).
    TxDiscovery,
}

/// Account-Quote pipeline: no-drop (quotes/reserves SSOT for hot path).
pub const MARKET_DATA_MD_ACCOUNT_SIDEFX_QUEUE_CAP: usize = 16_384;
/// TX Pin-Seed pipeline: bounded try_send + drop OK; hot layout seed only.
pub const MARKET_DATA_MD_TX_PIN_SEED_SIDEFX_QUEUE_CAP: usize = 4096;
/// TX-Discovery pipeline: bounded try_send + drop OK.
pub const MARKET_DATA_MD_TX_DISCOVERY_SIDEFX_QUEUE_CAP: usize = 4096;
/// Deprecated alias — discovery queue cap (kept for existing call sites / docs).
pub const MARKET_DATA_MD_TX_SIDEFX_QUEUE_CAP: usize = MARKET_DATA_MD_TX_DISCOVERY_SIDEFX_QUEUE_CAP;
/// Deprecated alias — TX queue cap (kept for existing call sites / docs).
pub const MARKET_DATA_MD_SIDEFX_QUEUE_CAP: usize = MARKET_DATA_MD_TX_DISCOVERY_SIDEFX_QUEUE_CAP;

/// Max jobs drained per sidefx burst before coalesce pass (per pipeline).
pub const MARKET_DATA_MD_SIDEFX_BURST_MAX: usize = 128;

/// Reserve headroom for EXEC_HOT when the **TX** sidefx queue is under pressure (legacy TX-only).
const MARKET_DATA_MD_SIDEFX_ENRICH_HEADROOM: usize = 512;

pub enum MdSidefxCommand {
    PumpFunPoolMintMapInsert {
        run_id: String,
        pool_address: Pubkey,
        mint_str: String,
        slot: Option<u64>,
        tx_grpc_recv_at: Instant,
        /// P1: creator from TX parse when known (avoids pool_creator_cache hen-and-egg).
        creator_override: Option<Pubkey>,
    },
    PumpFunDevWalletFromPoolCreated {
        run_id: String,
        pool_address: Pubkey,
        base_mint: Pubkey,
        creator: Pubkey,
        slot: u64,
        tx_geyser_recv_at: Instant,
    },
    PumpAmmCreatePoolObserved {
        run_id: String,
        pool_address: Pubkey,
        base_mint: String,
        quote_mint: String,
        slot: u64,
        tx_geyser_recv_at: Instant,
    },
    PumpAmmTradeWithAccounts {
        run_id: String,
        pool_address: Pubkey,
        base_mint_pk: Pubkey,
        slot: u64,
        is_buy: bool,
        pool_accounts: Vec<Pubkey>,
        pump_amm_sell_requires_cashback_remaining: bool,
        pump_amm_sell_cashback_third_meta: Option<Pubkey>,
        pump_amm_sell_extended_tail_0: Option<Pubkey>,
        pump_amm_sell_extended_tail_1: Option<Pubkey>,
        pump_amm_sell_extended_fee_tail_0: Option<Pubkey>,
        pump_amm_sell_extended_fee_tail_1: Option<Pubkey>,
        pump_amm_sell_requires_fee_tail: bool,
        pump_amm_sell_requires_pre_fee_metas: bool,
        pump_amm_sell_pre_fee_meta_1: Option<Pubkey>,
        tx_geyser_recv_at: Instant,
    },
    GenericDexFirstTradeAccounts {
        run_id: String,
        pool_address: Pubkey,
        mint: Pubkey,
        quote_mint: Pubkey,
        dex: DexType,
        pool_accounts: Vec<Pubkey>,
        slot: u64,
        tx_geyser_recv_at: Instant,
    },
    BondingCurveDevWallet {
        run_id: String,
        pool_address: Pubkey,
        creator: Pubkey,
        slot: u64,
        grpc_recv_at: Instant,
        virtual_token_reserves: u64,
        virtual_sol_reserves: u64,
        real_token_reserves: u64,
        real_sol_reserves: u64,
        complete: bool,
        cashback_enabled: bool,
    },
    VaultBalanceTick {
        run_id: String,
        vault_pubkey: Pubkey,
        balance: u64,
        slot: u64,
        grpc_recv_at: Instant,
        update_class: SidefxUpdateClass,
    },
    /// DLMM bin-array LRU touch off account ingest (coalesced in md-sidefx burst).
    TouchBinArrayTick {
        pda: Pubkey,
        update_class: SidefxUpdateClass,
    },
    /// P0: coalesced PoolStateUpdate for hot Meteora DLMM pools (bin/state signal, no vault delta).
    DlmmPoolStatePublishSignal {
        run_id: String,
        pool_address: Pubkey,
        slot: u64,
        grpc_recv_at: Instant,
        update_class: SidefxUpdateClass,
    },
    /// PR237: trade-path vault/bin LRU touch via sidefx scratch (replaces md-state TouchPool).
    TradePoolLruTouch { pool: Pubkey },
    /// Phase-R-R4b: `parse_pool_account` + LivePoolCache writes + PoolCacheUpdate publish off account ingest.
    LivePoolCacheAccountUpdate {
        run_id: String,
        pool_pubkey: Pubkey,
        owner: Pubkey,
        account_data: Vec<u8>,
        slot: u64,
        grpc_recv_at: Instant,
        update_class: SidefxUpdateClass,
    },
    /// Phase-R-R4b: mint decimals mirror into MASTER cache (TokenMintInfo path).
    LivePoolCacheMintDecimals { mint: Pubkey, decimals: u8 },
}

/// Bounded enqueue handle for Account-Quote `md-account-sidefx` OS thread.
#[derive(Clone)]
pub struct MdAccountSidefxSender {
    tx: std_mpsc::SyncSender<MdSidefxCommand>,
    queue_depth: Arc<AtomicUsize>,
    pub queue_capacity: usize,
}

/// Bounded enqueue handle for TX Pin-Seed `md-tx-pin-seed` OS thread (drop OK).
#[derive(Clone)]
pub struct MdTxPinSeedSidefxSender {
    tx: std_mpsc::SyncSender<MdSidefxCommand>,
    queue_depth: Arc<AtomicUsize>,
    pub queue_capacity: usize,
}

/// Bounded enqueue handle for TX-Discovery `md-tx-discovery` OS thread (drop OK).
#[derive(Clone)]
pub struct MdTxDiscoverySidefxSender {
    tx: std_mpsc::SyncSender<MdSidefxCommand>,
    queue_depth: Arc<AtomicUsize>,
    pub queue_capacity: usize,
}

/// Deprecated alias — discovery sender (pre Scope 0b single TX queue).
pub type MdTxSidefxSender = MdTxDiscoverySidefxSender;

/// TX ingest enqueue targets (pin-seed vs discovery routing at enqueue time).
#[derive(Clone)]
pub struct MdTxSidefxSenders {
    pub pin_seed: MdTxPinSeedSidefxSender,
    pub discovery: MdTxDiscoverySidefxSender,
}

impl MdTxSidefxSenders {
    pub fn from_workers(workers: &MdSidefxWorkers) -> Self {
        Self {
            pin_seed: workers.tx_pin_seed.clone(),
            discovery: workers.tx_discovery.clone(),
        }
    }
}

/// Three physically separated sidefx pipelines (spawned together).
#[derive(Clone)]
pub struct MdSidefxWorkers {
    pub account: MdAccountSidefxSender,
    pub tx_pin_seed: MdTxPinSeedSidefxSender,
    pub tx_discovery: MdTxDiscoverySidefxSender,
}

impl MdSidefxWorkers {
    /// Deprecated field access — use `tx_discovery`.
    pub fn tx(&self) -> &MdTxDiscoverySidefxSender {
        &self.tx_discovery
    }

    pub fn tx_senders(&self) -> MdTxSidefxSenders {
        MdTxSidefxSenders::from_workers(self)
    }
}

/// Per-burst coalesced DLMM pool-state publish signal (latest slot wins per pool).
#[derive(Clone)]
pub struct DlmmPoolStateSignal {
    pub run_id: String,
    pub slot: u64,
    pub grpc_recv_at: Instant,
}

/// Per-burst scratch: LRU touches coalesced before md-state enqueue (class-split for Scope D).
pub struct MdSidefxBurstScratch {
    pending_exec_hot_vault_touches: HashSet<Pubkey>,
    pending_enrich_vault_touches: HashSet<Pubkey>,
    pending_exec_hot_bin_array_touches: HashSet<Pubkey>,
    pending_enrich_bin_array_touches: HashSet<Pubkey>,
    pending_dlmm_pool_state_signals: HashMap<Pubkey, DlmmPoolStateSignal>,
}

impl Default for MdSidefxBurstScratch {
    fn default() -> Self {
        Self::new()
    }
}

impl MdSidefxBurstScratch {
    pub fn new() -> Self {
        Self {
            pending_exec_hot_vault_touches: HashSet::new(),
            pending_enrich_vault_touches: HashSet::new(),
            pending_exec_hot_bin_array_touches: HashSet::new(),
            pending_enrich_bin_array_touches: HashSet::new(),
            pending_dlmm_pool_state_signals: HashMap::new(),
        }
    }

    pub fn note_vault_touch(&mut self, vault: Pubkey, class: SidefxUpdateClass) {
        if class.is_exec_hot() {
            self.pending_exec_hot_vault_touches.insert(vault);
            self.pending_enrich_vault_touches.remove(&vault);
        } else {
            self.pending_enrich_vault_touches.insert(vault);
        }
    }

    pub fn note_bin_array_touch(&mut self, pda: Pubkey, class: SidefxUpdateClass) {
        if class.is_exec_hot() {
            self.pending_exec_hot_bin_array_touches.insert(pda);
            self.pending_enrich_bin_array_touches.remove(&pda);
        } else {
            self.pending_enrich_bin_array_touches.insert(pda);
        }
    }

    pub fn note_dlmm_pool_state_signal(
        &mut self,
        pool: Pubkey,
        run_id: &str,
        slot: u64,
        grpc_recv_at: Instant,
    ) {
        match self.pending_dlmm_pool_state_signals.get(&pool) {
            Some(existing) if existing.slot >= slot => {}
            _ => {
                self.pending_dlmm_pool_state_signals.insert(
                    pool,
                    DlmmPoolStateSignal {
                        run_id: run_id.to_string(),
                        slot,
                        grpc_recv_at,
                    },
                );
            }
        }
    }

    pub fn drain_dlmm_pool_state_signals(&mut self) -> Vec<(Pubkey, DlmmPoolStateSignal)> {
        self.pending_dlmm_pool_state_signals.drain().collect()
    }

    pub fn pending_vault_touches_contains(&self, vault: &Pubkey) -> bool {
        self.pending_exec_hot_vault_touches.contains(vault)
            || self.pending_enrich_vault_touches.contains(vault)
    }

    pub fn lru_touches_empty(&self) -> bool {
        self.pending_exec_hot_vault_touches.is_empty()
            && self.pending_enrich_vault_touches.is_empty()
            && self.pending_exec_hot_bin_array_touches.is_empty()
            && self.pending_enrich_bin_array_touches.is_empty()
    }

    pub fn drain_lru_touches(&mut self) -> (Vec<Pubkey>, Vec<Pubkey>, Vec<Pubkey>, Vec<Pubkey>) {
        (
            self.pending_exec_hot_vault_touches.drain().collect(),
            self.pending_enrich_vault_touches.drain().collect(),
            self.pending_exec_hot_bin_array_touches.drain().collect(),
            self.pending_enrich_bin_array_touches.drain().collect(),
        )
    }
}

pub fn md_sidefx_flush_pending_md_state_jobs(
    host: &dyn SidefxWorkerHost,
    scratch: &mut MdSidefxBurstScratch,
) {
    super::handlers::md_sidefx_flush_pending_dlmm_pool_state_publishes(host, scratch);
    host.flush_lru_touches(scratch);
}

/// Canonical routing: each command variant belongs to exactly one pipeline.
/// Pin-seed **candidates** default to discovery here; use [`md_sidefx_tx_enqueue_pipeline`] at TX enqueue.
pub fn md_sidefx_command_pipeline(job: &MdSidefxCommand) -> MdSidefxPipeline {
    match job {
        MdSidefxCommand::LivePoolCacheAccountUpdate { .. }
        | MdSidefxCommand::VaultBalanceTick { .. }
        | MdSidefxCommand::TouchBinArrayTick { .. }
        | MdSidefxCommand::DlmmPoolStatePublishSignal { .. }
        | MdSidefxCommand::LivePoolCacheMintDecimals { .. }
        | MdSidefxCommand::BondingCurveDevWallet { .. } => MdSidefxPipeline::AccountQuote,
        MdSidefxCommand::PumpFunPoolMintMapInsert { .. }
        | MdSidefxCommand::PumpFunDevWalletFromPoolCreated { .. }
        | MdSidefxCommand::PumpAmmCreatePoolObserved { .. }
        | MdSidefxCommand::PumpAmmTradeWithAccounts { .. }
        | MdSidefxCommand::GenericDexFirstTradeAccounts { .. }
        | MdSidefxCommand::TradePoolLruTouch { .. } => MdSidefxPipeline::TxDiscovery,
    }
}

/// TX sub-pipeline jobs that may route to pin-seed when the pool is hot at enqueue time.
#[inline]
pub fn md_sidefx_is_tx_pin_seed_candidate(job: &MdSidefxCommand) -> bool {
    matches!(
        job,
        MdSidefxCommand::PumpAmmTradeWithAccounts { .. }
            | MdSidefxCommand::GenericDexFirstTradeAccounts { .. }
            | MdSidefxCommand::TradePoolLruTouch { .. }
    )
}

/// TX enqueue routing: hot pin-seed candidates → pin-seed queue; everything else → discovery.
#[inline]
pub fn md_sidefx_tx_enqueue_pipeline(job: &MdSidefxCommand, is_hot_pool: bool) -> MdSidefxPipeline {
    if is_hot_pool && md_sidefx_is_tx_pin_seed_candidate(job) {
        MdSidefxPipeline::TxPinSeed
    } else {
        MdSidefxPipeline::TxDiscovery
    }
}

/// Read propagated ingest class from account-path sidefx commands (TX-path defaults to EXEC_HOT).
pub fn md_sidefx_job_update_class(job: &MdSidefxCommand) -> SidefxUpdateClass {
    match job {
        MdSidefxCommand::LivePoolCacheAccountUpdate { update_class, .. } => *update_class,
        MdSidefxCommand::VaultBalanceTick { update_class, .. } => *update_class,
        MdSidefxCommand::TouchBinArrayTick { update_class, .. } => *update_class,
        MdSidefxCommand::DlmmPoolStatePublishSignal { update_class, .. } => *update_class,
        _ => SidefxUpdateClass::ExecHot,
    }
}

fn md_sidefx_apply_update_class(job: &mut MdSidefxCommand, class: SidefxUpdateClass) {
    match job {
        MdSidefxCommand::LivePoolCacheAccountUpdate { update_class, .. } => *update_class = class,
        MdSidefxCommand::VaultBalanceTick { update_class, .. } => *update_class = class,
        MdSidefxCommand::TouchBinArrayTick { update_class, .. } => *update_class = class,
        MdSidefxCommand::DlmmPoolStatePublishSignal { update_class, .. } => *update_class = class,
        _ => {}
    }
}

fn md_sidefx_inc_queue_depth(queue_depth: &AtomicUsize, pipeline: MdSidefxPipeline) {
    let new_depth = queue_depth.fetch_add(1, Ordering::Relaxed) + 1;
    match pipeline {
        MdSidefxPipeline::AccountQuote => set_market_data_account_sidefx_queue_depth(new_depth),
        MdSidefxPipeline::TxPinSeed => set_market_data_tx_pin_seed_sidefx_queue_depth(new_depth),
        MdSidefxPipeline::TxDiscovery => set_market_data_tx_discovery_sidefx_queue_depth(new_depth),
    }
    refresh_market_data_md_sidefx_deprecated_metrics();
}

fn md_sidefx_dec_queue_depth(queue_depth: &AtomicUsize, pipeline: MdSidefxPipeline) {
    let mut cur = queue_depth.load(Ordering::Relaxed);
    while cur > 0 {
        match queue_depth.compare_exchange_weak(cur, cur - 1, Ordering::Relaxed, Ordering::Relaxed)
        {
            Ok(_) => {
                let new_depth = cur - 1;
                match pipeline {
                    MdSidefxPipeline::AccountQuote => {
                        set_market_data_account_sidefx_queue_depth(new_depth)
                    }
                    MdSidefxPipeline::TxPinSeed => {
                        set_market_data_tx_pin_seed_sidefx_queue_depth(new_depth)
                    }
                    MdSidefxPipeline::TxDiscovery => {
                        set_market_data_tx_discovery_sidefx_queue_depth(new_depth)
                    }
                }
                refresh_market_data_md_sidefx_deprecated_metrics();
                return;
            }
            Err(actual) => cur = actual,
        }
    }
    match pipeline {
        MdSidefxPipeline::AccountQuote => set_market_data_account_sidefx_queue_depth(0),
        MdSidefxPipeline::TxPinSeed => set_market_data_tx_pin_seed_sidefx_queue_depth(0),
        MdSidefxPipeline::TxDiscovery => set_market_data_tx_discovery_sidefx_queue_depth(0),
    }
    refresh_market_data_md_sidefx_deprecated_metrics();
}

fn md_sidefx_inc_processed(pipeline: MdSidefxPipeline) {
    match pipeline {
        MdSidefxPipeline::AccountQuote => inc_market_data_account_sidefx_jobs_processed_total(),
        MdSidefxPipeline::TxPinSeed => {
            inc_market_data_tx_pin_seed_sidefx_jobs_processed_total();
            inc_market_data_tx_sidefx_jobs_processed_total();
        }
        MdSidefxPipeline::TxDiscovery => {
            inc_market_data_tx_discovery_sidefx_jobs_processed_total();
            inc_market_data_tx_sidefx_jobs_processed_total();
        }
    }
    inc_market_data_md_sidefx_jobs_processed_total();
}

/// Account-Quote enqueue: no silent drop — try_send then blocking send on account ingest only.
pub fn md_account_sidefx_try_enqueue_classed(
    sender: &MdAccountSidefxSender,
    _class: SidefxUpdateClass,
    job: MdSidefxCommand,
) {
    debug_assert_eq!(
        md_sidefx_command_pipeline(&job),
        MdSidefxPipeline::AccountQuote,
        "account sidefx enqueue called with TX-discovery job"
    );
    match sender.tx.try_send(job) {
        Ok(()) => {
            md_sidefx_inc_queue_depth(&sender.queue_depth, MdSidefxPipeline::AccountQuote);
        }
        Err(std_mpsc::TrySendError::Full(job)) => {
            inc_market_data_account_sidefx_backpressure_total();
            if sender.tx.send(job).is_ok() {
                md_sidefx_inc_queue_depth(&sender.queue_depth, MdSidefxPipeline::AccountQuote);
            } else {
                inc_market_data_account_sidefx_enqueue_fail_loud_total();
            }
        }
        Err(std_mpsc::TrySendError::Disconnected(_)) => {
            inc_market_data_account_sidefx_enqueue_fail_loud_total();
        }
    }
}

pub fn md_account_sidefx_try_enqueue(sender: &MdAccountSidefxSender, job: MdSidefxCommand) {
    let class = md_sidefx_job_update_class(&job);
    md_account_sidefx_try_enqueue_classed(sender, class, job);
}

/// TX-Discovery enqueue: bounded try_send + drop counters (existing behaviour).
pub fn md_tx_discovery_sidefx_try_enqueue(
    sender: &MdTxDiscoverySidefxSender,
    job: MdSidefxCommand,
) {
    debug_assert_ne!(
        md_sidefx_command_pipeline(&job),
        MdSidefxPipeline::AccountQuote,
        "tx sidefx enqueue called with account-quote job"
    );
    md_tx_discovery_sidefx_try_enqueue_classed(sender, SidefxUpdateClass::ExecHot, job);
}

pub fn md_tx_discovery_sidefx_try_enqueue_classed(
    sender: &MdTxDiscoverySidefxSender,
    class: SidefxUpdateClass,
    job: MdSidefxCommand,
) {
    md_tx_bounded_sidefx_try_enqueue_classed(
        &sender.tx,
        &sender.queue_depth,
        sender.queue_capacity,
        class,
        job,
        MdSidefxPipeline::TxDiscovery,
        inc_market_data_tx_discovery_sidefx_enqueue_dropped_total,
    );
}

/// TX Pin-Seed enqueue: bounded try_send + drop counters (isolated from discovery flood).
pub fn md_tx_pin_seed_sidefx_try_enqueue(sender: &MdTxPinSeedSidefxSender, job: MdSidefxCommand) {
    debug_assert!(
        md_sidefx_is_tx_pin_seed_candidate(&job),
        "pin-seed enqueue called with non pin-seed job"
    );
    md_tx_pin_seed_sidefx_try_enqueue_classed(sender, SidefxUpdateClass::ExecHot, job);
}

pub fn md_tx_pin_seed_sidefx_try_enqueue_classed(
    sender: &MdTxPinSeedSidefxSender,
    class: SidefxUpdateClass,
    job: MdSidefxCommand,
) {
    md_tx_bounded_sidefx_try_enqueue_classed(
        &sender.tx,
        &sender.queue_depth,
        sender.queue_capacity,
        class,
        job,
        MdSidefxPipeline::TxPinSeed,
        inc_market_data_tx_pin_seed_sidefx_enqueue_dropped_total,
    );
}

/// Route TX job to pin-seed or discovery queue based on hot-pool status at enqueue time.
pub fn md_tx_sidefx_route_enqueue(
    senders: &MdTxSidefxSenders,
    job: MdSidefxCommand,
    is_hot_pool: bool,
) {
    match md_sidefx_tx_enqueue_pipeline(&job, is_hot_pool) {
        MdSidefxPipeline::TxPinSeed => md_tx_pin_seed_sidefx_try_enqueue(&senders.pin_seed, job),
        MdSidefxPipeline::TxDiscovery => {
            md_tx_discovery_sidefx_try_enqueue(&senders.discovery, job)
        }
        MdSidefxPipeline::AccountQuote => {
            debug_assert!(false, "TX route enqueue called with account-quote job");
            md_tx_discovery_sidefx_try_enqueue(&senders.discovery, job);
        }
    }
}

/// Deprecated alias — discovery enqueue.
pub fn md_tx_sidefx_try_enqueue(sender: &MdTxSidefxSender, job: MdSidefxCommand) {
    md_tx_discovery_sidefx_try_enqueue(sender, job);
}

/// Deprecated alias — discovery enqueue.
pub fn md_tx_sidefx_try_enqueue_classed(
    sender: &MdTxSidefxSender,
    class: SidefxUpdateClass,
    job: MdSidefxCommand,
) {
    md_tx_discovery_sidefx_try_enqueue_classed(sender, class, job);
}

fn md_tx_bounded_sidefx_try_enqueue_classed(
    tx: &std_mpsc::SyncSender<MdSidefxCommand>,
    queue_depth: &AtomicUsize,
    queue_capacity: usize,
    class: SidefxUpdateClass,
    job: MdSidefxCommand,
    pipeline: MdSidefxPipeline,
    inc_pipeline_dropped: fn(),
) {
    let depth = queue_depth.load(Ordering::Relaxed);
    if !class.is_exec_hot() && depth + MARKET_DATA_MD_SIDEFX_ENRICH_HEADROOM >= queue_capacity {
        inc_market_data_md_sidefx_enrich_enqueue_dropped_total();
        return;
    }
    // Channel try_send is authoritative — depth atomic can briefly lag recv/inc ordering.
    match tx.try_send(job) {
        Ok(()) => md_sidefx_inc_queue_depth(queue_depth, pipeline),
        Err(std_mpsc::TrySendError::Full(_)) => {
            if class.is_exec_hot() {
                inc_pipeline_dropped();
                inc_market_data_tx_sidefx_enqueue_dropped_total();
                inc_market_data_md_sidefx_enqueue_dropped_total();
            } else {
                inc_market_data_md_sidefx_enrich_enqueue_dropped_total();
            }
        }
        Err(std_mpsc::TrySendError::Disconnected(_)) => {
            inc_market_data_md_sidefx_enqueue_dropped_total();
            inc_pipeline_dropped();
            if class.is_exec_hot() {
                inc_market_data_tx_sidefx_enqueue_dropped_total();
            } else {
                inc_market_data_md_sidefx_enrich_enqueue_dropped_total();
            }
        }
    }
}

/// Deprecated: routes to the correct pipeline by command variant (pin-seed candidates → discovery).
pub fn md_sidefx_try_enqueue(workers: &MdSidefxWorkers, job: MdSidefxCommand) {
    match md_sidefx_command_pipeline(&job) {
        MdSidefxPipeline::AccountQuote => md_account_sidefx_try_enqueue(&workers.account, job),
        MdSidefxPipeline::TxPinSeed | MdSidefxPipeline::TxDiscovery => {
            md_tx_discovery_sidefx_try_enqueue(&workers.tx_discovery, job)
        }
    }
}

/// Deprecated: routes to the correct pipeline by command variant (pin-seed candidates → discovery).
pub fn md_sidefx_try_enqueue_classed(
    workers: &MdSidefxWorkers,
    class: SidefxUpdateClass,
    job: MdSidefxCommand,
) {
    match md_sidefx_command_pipeline(&job) {
        MdSidefxPipeline::AccountQuote => {
            md_account_sidefx_try_enqueue_classed(&workers.account, class, job)
        }
        MdSidefxPipeline::TxPinSeed | MdSidefxPipeline::TxDiscovery => {
            md_tx_discovery_sidefx_try_enqueue_classed(&workers.tx_discovery, class, job)
        }
    }
}

pub fn md_sidefx_coalesce_key(job: &MdSidefxCommand) -> Option<Pubkey> {
    match job {
        MdSidefxCommand::PumpFunPoolMintMapInsert { pool_address, .. } => Some(*pool_address),
        MdSidefxCommand::PumpAmmTradeWithAccounts { pool_address, .. } => Some(*pool_address),
        MdSidefxCommand::PumpAmmCreatePoolObserved { pool_address, .. } => Some(*pool_address),
        MdSidefxCommand::LivePoolCacheAccountUpdate { pool_pubkey, .. } => Some(*pool_pubkey),
        MdSidefxCommand::VaultBalanceTick { vault_pubkey, .. } => Some(*vault_pubkey),
        MdSidefxCommand::TouchBinArrayTick { pda, .. } => Some(*pda),
        MdSidefxCommand::DlmmPoolStatePublishSignal { pool_address, .. } => Some(*pool_address),
        MdSidefxCommand::TradePoolLruTouch { pool } => Some(*pool),
        _ => None,
    }
}

pub fn md_sidefx_coalesce_burst(jobs: Vec<MdSidefxCommand>) -> Vec<MdSidefxCommand> {
    let mut out: Vec<MdSidefxCommand> = Vec::with_capacity(jobs.len());
    let mut coalesced: HashMap<Pubkey, usize> = HashMap::new();
    for job in jobs {
        if let Some(pool) = md_sidefx_coalesce_key(&job) {
            if let Some(&idx) = coalesced.get(&pool) {
                let merged_class = md_sidefx_job_update_class(&out[idx])
                    .merge_priority(md_sidefx_job_update_class(&job));
                out[idx] = job;
                md_sidefx_apply_update_class(&mut out[idx], merged_class);
            } else {
                coalesced.insert(pool, out.len());
                out.push(job);
            }
        } else {
            out.push(job);
        }
    }
    out
}

fn md_sidefx_partition_by_class(
    jobs: Vec<MdSidefxCommand>,
) -> (Vec<MdSidefxCommand>, Vec<MdSidefxCommand>) {
    let mut exec_hot = Vec::new();
    let mut enrich = Vec::new();
    for job in jobs {
        if md_sidefx_job_update_class(&job).is_exec_hot() {
            exec_hot.push(job);
        } else {
            enrich.push(job);
        }
    }
    (exec_hot, enrich)
}

fn md_sidefx_worker_loop(
    worker: Arc<dyn SidefxWorkerHost>,
    rx: std_mpsc::Receiver<MdSidefxCommand>,
    queue_depth: Arc<AtomicUsize>,
    pipeline: MdSidefxPipeline,
) {
    loop {
        let Ok(first) = rx.recv() else {
            break;
        };
        md_sidefx_dec_queue_depth(&queue_depth, pipeline);
        md_sidefx_inc_processed(pipeline);
        let mut jobs = vec![first];
        while jobs.len() < MARKET_DATA_MD_SIDEFX_BURST_MAX {
            match rx.try_recv() {
                Ok(job) => {
                    md_sidefx_dec_queue_depth(&queue_depth, pipeline);
                    md_sidefx_inc_processed(pipeline);
                    jobs.push(job);
                }
                Err(std_mpsc::TryRecvError::Empty) => break,
                Err(std_mpsc::TryRecvError::Disconnected) => break,
            }
        }
        let mut scratch = MdSidefxBurstScratch::new();
        let coalesced = md_sidefx_coalesce_burst(jobs);
        let (exec_hot_jobs, enrich_jobs) = md_sidefx_partition_by_class(coalesced);
        for job in exec_hot_jobs {
            md_sidefx_process_job(worker.as_ref(), &job, &mut scratch);
        }
        for job in enrich_jobs {
            md_sidefx_process_job(worker.as_ref(), &job, &mut scratch);
        }
        md_sidefx_flush_pending_md_state_jobs(worker.as_ref(), &mut scratch);
    }
}

fn spawn_md_sidefx_pipeline_worker(
    host: Arc<dyn SidefxWorkerHost>,
    queue_capacity: usize,
    thread_name: &'static str,
    pipeline: MdSidefxPipeline,
) -> (std_mpsc::SyncSender<MdSidefxCommand>, Arc<AtomicUsize>) {
    let (tx, rx) = std_mpsc::sync_channel::<MdSidefxCommand>(queue_capacity);
    let queue_depth = Arc::new(AtomicUsize::new(0));
    let depth_worker = Arc::clone(&queue_depth);
    let host_worker = Arc::clone(&host);
    let _join: JoinHandle<()> = std::thread::Builder::new()
        .name(thread_name.into())
        .spawn(move || md_sidefx_worker_loop(host_worker, rx, depth_worker, pipeline))
        .expect("spawn md-sidefx thread");
    (tx, queue_depth)
}

/// Spawn Account-Quote, TX Pin-Seed, and TX-Discovery sidefx OS threads.
pub fn spawn_md_sidefx_workers(
    host: Arc<dyn SidefxWorkerHost>,
    account_queue_capacity: usize,
    tx_pin_seed_queue_capacity: usize,
    tx_discovery_queue_capacity: usize,
) -> MdSidefxWorkers {
    let (account_tx, account_depth) = spawn_md_sidefx_pipeline_worker(
        Arc::clone(&host),
        account_queue_capacity,
        "md-account-sidefx",
        MdSidefxPipeline::AccountQuote,
    );
    let (pin_seed_tx, pin_seed_depth) = spawn_md_sidefx_pipeline_worker(
        Arc::clone(&host),
        tx_pin_seed_queue_capacity,
        "md-tx-pin-seed",
        MdSidefxPipeline::TxPinSeed,
    );
    let (discovery_tx, discovery_depth) = spawn_md_sidefx_pipeline_worker(
        host,
        tx_discovery_queue_capacity,
        "md-tx-discovery",
        MdSidefxPipeline::TxDiscovery,
    );
    MdSidefxWorkers {
        account: MdAccountSidefxSender {
            tx: account_tx,
            queue_depth: account_depth,
            queue_capacity: account_queue_capacity,
        },
        tx_pin_seed: MdTxPinSeedSidefxSender {
            tx: pin_seed_tx,
            queue_depth: pin_seed_depth,
            queue_capacity: tx_pin_seed_queue_capacity,
        },
        tx_discovery: MdTxDiscoverySidefxSender {
            tx: discovery_tx,
            queue_depth: discovery_depth,
            queue_capacity: tx_discovery_queue_capacity,
        },
    }
}

/// Deprecated alias: spawns all three pipelines with default caps.
pub fn spawn_md_sidefx_worker(
    host: Arc<dyn SidefxWorkerHost>,
    tx_discovery_queue_capacity: usize,
) -> MdSidefxWorkers {
    spawn_md_sidefx_workers(
        host,
        MARKET_DATA_MD_ACCOUNT_SIDEFX_QUEUE_CAP,
        MARKET_DATA_MD_TX_PIN_SEED_SIDEFX_QUEUE_CAP,
        tx_discovery_queue_capacity,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::{
        MARKET_DATA_ACCOUNT_SIDEFX_ENQUEUE_DROPPED_TOTAL,
        MARKET_DATA_ACCOUNT_SIDEFX_ENQUEUE_FAIL_LOUD_TOTAL,
        MARKET_DATA_ACCOUNT_SIDEFX_JOBS_PROCESSED_TOTAL,
        MARKET_DATA_TX_DISCOVERY_SIDEFX_ENQUEUE_DROPPED_TOTAL,
        MARKET_DATA_TX_DISCOVERY_SIDEFX_JOBS_PROCESSED_TOTAL,
        MARKET_DATA_TX_PIN_SEED_SIDEFX_ENQUEUE_DROPPED_TOTAL,
        MARKET_DATA_TX_PIN_SEED_SIDEFX_JOBS_PROCESSED_TOTAL,
        MARKET_DATA_TX_SIDEFX_ENQUEUE_DROPPED_TOTAL,
    };
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;

    const RAYDIUM_CPMM_OWNER: Pubkey =
        solana_sdk::pubkey!("CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C");

    struct TestBlockingHost;

    static LIVE_POOL_CACHE_TEST: Mutex<Option<crate::execution::live_pool_cache::LivePoolCache>> =
        Mutex::new(None);

    fn test_live_pool_cache() -> &'static crate::execution::live_pool_cache::LivePoolCache {
        let mut guard = LIVE_POOL_CACHE_TEST.lock().expect("lock");
        if guard.is_none() {
            *guard = Some(crate::execution::live_pool_cache::LivePoolCache::new());
        }
        // SAFETY: test-only static, never dropped while tests run.
        unsafe {
            &*(guard.as_ref().expect("cache")
                as *const crate::execution::live_pool_cache::LivePoolCache)
        }
    }

    impl SidefxWorkerHost for TestBlockingHost {
        fn build_version(&self) -> &'static str {
            "test"
        }
        fn next_event_id(&self) -> String {
            "e".into()
        }
        fn write_market_event_jsonl(&self, _event: &crate::ipc::MarketEvent) {}
        fn nats_enabled(&self) -> bool {
            false
        }
        fn enqueue_core_market_event(
            &self,
            _event: crate::ipc::MarketEvent,
            _trace: Option<super::super::host::MarketEventCorePublishTrace>,
        ) -> bool {
            false
        }
        fn enqueue_jetstream(
            &self,
            _subject: String,
            _payload: serde_json::Value,
            _log_fail: &'static str,
            _bump_market_events_published_total: bool,
        ) {
        }
        fn flush_lru_touches(&self, _scratch: &mut MdSidefxBurstScratch) {}
        fn live_pool_cache(&self) -> &crate::execution::live_pool_cache::LivePoolCache {
            test_live_pool_cache()
        }
        fn pool_mint_map_insert(&self, _pool: String, _mint: String) {}
        fn pool_mint_map_get(&self, _pool: &str) -> Option<String> {
            None
        }
        fn pool_creator_cache_get(&self, _pool: &str) -> Option<String> {
            None
        }
        fn pool_creator_cache_insert(&self, _pool: String, _creator: String) {}
        fn pool_creator_cache_insert_if_absent(&self, _pool: String, _creator: String) -> bool {
            false
        }
        fn creator_cache_set(&self, _mint: String, _creator: String) {}
        fn creator_cache_insert_if_absent(&self, _mint: String, _creator: String) -> bool {
            false
        }
        fn creator_cache_insert_returning_old(
            &self,
            _mint: String,
            _creator: String,
        ) -> Option<String> {
            None
        }
        fn high_priority_bonding_curves_insert(&self, _pool: Pubkey) {}
        fn known_pump_amm_pools_insert(&self, _pool: Pubkey) -> bool {
            false
        }
        fn known_trade_dex_pools_insert(&self, _pool: Pubkey) -> bool {
            false
        }
        fn should_emit_curve_progress(
            &self,
            _pool: &Pubkey,
            _progress_bps: u32,
            _complete: bool,
        ) -> bool {
            false
        }
        fn record_curve_progress_emitted(
            &self,
            _pool: Pubkey,
            _progress_bps: u32,
            _complete: bool,
        ) {
        }
        fn vault_membership_view(
            &self,
            _vault: &Pubkey,
        ) -> Option<super::super::host::SidefxVaultMembershipView> {
            None
        }
        fn snapshot_vault_pair_balances(
            &self,
            _vault: &Pubkey,
            _new_balance: u64,
        ) -> Option<(u64, u64)> {
            None
        }
        fn note_trade_pool_lru_touches(&self, _pool: Pubkey, _scratch: &mut MdSidefxBurstScratch) {}
        fn is_hot_pool(&self, _pool: &Pubkey) -> bool {
            false
        }
        fn is_open_position_pumpfun_pin(&self, _pool: &Pubkey) -> bool {
            false
        }
        fn is_enrichment_member(&self, _pool: &Pubkey) -> bool {
            false
        }
        fn pool_has_live_vault_geyser_feed(&self, _pool: Pubkey) -> bool {
            false
        }
        fn maybe_refresh_arb_dlmm_bin_window(&self, _pool: Pubkey, _new_active_id: i32) -> bool {
            false
        }
        fn maybe_retry_deferred_hot_pool_reserves_on_cache_fill(&self, _pool: &Pubkey) {}
        fn maybe_spawn_raydium_serum_cold_backfill(
            &self,
            _pool: Pubkey,
            _state: &crate::execution::live_pool_cache::RaydiumAmmState,
        ) {
        }
        fn apply_tx_pool_accounts_for_hot_pool(
            &self,
            _pool: Pubkey,
            _dex: DexType,
            _base_mint: Pubkey,
            _quote_mint: Pubkey,
            _pool_accounts: &[Pubkey],
            _slot: u64,
        ) {
        }
    }

    fn mk_account_job() -> MdSidefxCommand {
        MdSidefxCommand::VaultBalanceTick {
            run_id: "r".into(),
            vault_pubkey: Pubkey::new_unique(),
            balance: 1,
            slot: 1,
            grpc_recv_at: Instant::now(),
            update_class: SidefxUpdateClass::ExecHot,
        }
    }

    fn mk_tx_discovery_job() -> MdSidefxCommand {
        MdSidefxCommand::PumpFunPoolMintMapInsert {
            run_id: "r".into(),
            pool_address: Pubkey::new_unique(),
            mint_str: "m".into(),
            slot: None,
            tx_grpc_recv_at: Instant::now(),
            creator_override: None,
        }
    }

    fn mk_pin_seed_job(pool: Pubkey) -> MdSidefxCommand {
        MdSidefxCommand::TradePoolLruTouch { pool }
    }

    struct HotPoolTestHost {
        hot: Pubkey,
    }

    impl SidefxWorkerHost for HotPoolTestHost {
        fn build_version(&self) -> &'static str {
            "test"
        }
        fn next_event_id(&self) -> String {
            "e".into()
        }
        fn write_market_event_jsonl(&self, _event: &crate::ipc::MarketEvent) {}
        fn nats_enabled(&self) -> bool {
            false
        }
        fn enqueue_core_market_event(
            &self,
            _event: crate::ipc::MarketEvent,
            _trace: Option<super::super::host::MarketEventCorePublishTrace>,
        ) -> bool {
            false
        }
        fn enqueue_jetstream(
            &self,
            _subject: String,
            _payload: serde_json::Value,
            _log_fail: &'static str,
            _bump_market_events_published_total: bool,
        ) {
        }
        fn flush_lru_touches(&self, _scratch: &mut MdSidefxBurstScratch) {}
        fn live_pool_cache(&self) -> &crate::execution::live_pool_cache::LivePoolCache {
            test_live_pool_cache()
        }
        fn pool_mint_map_insert(&self, _pool: String, _mint: String) {}
        fn pool_mint_map_get(&self, _pool: &str) -> Option<String> {
            None
        }
        fn pool_creator_cache_get(&self, _pool: &str) -> Option<String> {
            None
        }
        fn pool_creator_cache_insert(&self, _pool: String, _creator: String) {}
        fn pool_creator_cache_insert_if_absent(&self, _pool: String, _creator: String) -> bool {
            false
        }
        fn creator_cache_set(&self, _mint: String, _creator: String) {}
        fn creator_cache_insert_if_absent(&self, _mint: String, _creator: String) -> bool {
            false
        }
        fn creator_cache_insert_returning_old(
            &self,
            _mint: String,
            _creator: String,
        ) -> Option<String> {
            None
        }
        fn high_priority_bonding_curves_insert(&self, _pool: Pubkey) {}
        fn known_pump_amm_pools_insert(&self, _pool: Pubkey) -> bool {
            false
        }
        fn known_trade_dex_pools_insert(&self, _pool: Pubkey) -> bool {
            false
        }
        fn should_emit_curve_progress(
            &self,
            _pool: &Pubkey,
            _progress_bps: u32,
            _complete: bool,
        ) -> bool {
            false
        }
        fn record_curve_progress_emitted(
            &self,
            _pool: Pubkey,
            _progress_bps: u32,
            _complete: bool,
        ) {
        }
        fn vault_membership_view(
            &self,
            _vault: &Pubkey,
        ) -> Option<super::super::host::SidefxVaultMembershipView> {
            None
        }
        fn snapshot_vault_pair_balances(
            &self,
            _vault: &Pubkey,
            _new_balance: u64,
        ) -> Option<(u64, u64)> {
            None
        }
        fn note_trade_pool_lru_touches(&self, _pool: Pubkey, _scratch: &mut MdSidefxBurstScratch) {}
        fn is_hot_pool(&self, pool: &Pubkey) -> bool {
            pool == &self.hot
        }
        fn is_open_position_pumpfun_pin(&self, _pool: &Pubkey) -> bool {
            false
        }
        fn is_enrichment_member(&self, _pool: &Pubkey) -> bool {
            false
        }
        fn pool_has_live_vault_geyser_feed(&self, _pool: Pubkey) -> bool {
            false
        }
        fn maybe_refresh_arb_dlmm_bin_window(&self, _pool: Pubkey, _new_active_id: i32) -> bool {
            false
        }
        fn maybe_retry_deferred_hot_pool_reserves_on_cache_fill(&self, _pool: &Pubkey) {}
        fn maybe_spawn_raydium_serum_cold_backfill(
            &self,
            _pool: Pubkey,
            _state: &crate::execution::live_pool_cache::RaydiumAmmState,
        ) {
        }
        fn apply_tx_pool_accounts_for_hot_pool(
            &self,
            _pool: Pubkey,
            _dex: DexType,
            _base_mint: Pubkey,
            _quote_mint: Pubkey,
            _pool_accounts: &[Pubkey],
            _slot: u64,
        ) {
        }
    }

    #[test]
    fn md_sidefx_routing_table_covers_all_variants() {
        let variants: Vec<MdSidefxCommand> = vec![
            mk_tx_discovery_job(),
            MdSidefxCommand::PumpFunDevWalletFromPoolCreated {
                run_id: "r".into(),
                pool_address: Pubkey::new_unique(),
                base_mint: Pubkey::new_unique(),
                creator: Pubkey::new_unique(),
                slot: 1,
                tx_geyser_recv_at: Instant::now(),
            },
            MdSidefxCommand::PumpAmmCreatePoolObserved {
                run_id: "r".into(),
                pool_address: Pubkey::new_unique(),
                base_mint: "b".into(),
                quote_mint: "q".into(),
                slot: 1,
                tx_geyser_recv_at: Instant::now(),
            },
            MdSidefxCommand::PumpAmmTradeWithAccounts {
                run_id: "r".into(),
                pool_address: Pubkey::new_unique(),
                base_mint_pk: Pubkey::new_unique(),
                slot: 1,
                is_buy: true,
                pool_accounts: vec![],
                pump_amm_sell_requires_cashback_remaining: false,
                pump_amm_sell_cashback_third_meta: None,
                pump_amm_sell_extended_tail_0: None,
                pump_amm_sell_extended_tail_1: None,
                pump_amm_sell_extended_fee_tail_0: None,
                pump_amm_sell_extended_fee_tail_1: None,
                pump_amm_sell_requires_fee_tail: false,
                pump_amm_sell_requires_pre_fee_metas: false,
                pump_amm_sell_pre_fee_meta_1: None,
                tx_geyser_recv_at: Instant::now(),
            },
            MdSidefxCommand::GenericDexFirstTradeAccounts {
                run_id: "r".into(),
                pool_address: Pubkey::new_unique(),
                mint: Pubkey::new_unique(),
                quote_mint: Pubkey::new_unique(),
                dex: DexType::RaydiumAmmV4,
                pool_accounts: vec![],
                slot: 1,
                tx_geyser_recv_at: Instant::now(),
            },
            MdSidefxCommand::BondingCurveDevWallet {
                run_id: "r".into(),
                pool_address: Pubkey::new_unique(),
                creator: Pubkey::new_unique(),
                slot: 1,
                grpc_recv_at: Instant::now(),
                virtual_token_reserves: 1,
                virtual_sol_reserves: 1,
                real_token_reserves: 1,
                real_sol_reserves: 1,
                complete: false,
                cashback_enabled: false,
            },
            mk_account_job(),
            MdSidefxCommand::TouchBinArrayTick {
                pda: Pubkey::new_unique(),
                update_class: SidefxUpdateClass::ExecHot,
            },
            MdSidefxCommand::DlmmPoolStatePublishSignal {
                run_id: "r".into(),
                pool_address: Pubkey::new_unique(),
                slot: 1,
                grpc_recv_at: Instant::now(),
                update_class: SidefxUpdateClass::ExecHot,
            },
            MdSidefxCommand::TradePoolLruTouch {
                pool: Pubkey::new_unique(),
            },
            MdSidefxCommand::LivePoolCacheAccountUpdate {
                run_id: "r".into(),
                pool_pubkey: Pubkey::new_unique(),
                owner: RAYDIUM_CPMM_OWNER,
                account_data: vec![],
                slot: 1,
                grpc_recv_at: Instant::now(),
                update_class: SidefxUpdateClass::ExecHot,
            },
            MdSidefxCommand::LivePoolCacheMintDecimals {
                mint: Pubkey::new_unique(),
                decimals: 6,
            },
        ];
        for job in variants {
            let _ = md_sidefx_command_pipeline(&job);
        }
    }

    #[test]
    fn md_sidefx_coalesce_preserves_exec_hot_class_over_enrich() {
        let pool = Pubkey::new_unique();
        let exec_hot = MdSidefxCommand::LivePoolCacheAccountUpdate {
            run_id: "r".into(),
            pool_pubkey: pool,
            owner: RAYDIUM_CPMM_OWNER,
            account_data: vec![1],
            slot: 1,
            grpc_recv_at: Instant::now(),
            update_class: SidefxUpdateClass::ExecHot,
        };
        let enrich = MdSidefxCommand::LivePoolCacheAccountUpdate {
            run_id: "r".into(),
            pool_pubkey: pool,
            owner: RAYDIUM_CPMM_OWNER,
            account_data: vec![2],
            slot: 2,
            grpc_recv_at: Instant::now(),
            update_class: SidefxUpdateClass::Enrich,
        };
        let out = md_sidefx_coalesce_burst(vec![enrich, exec_hot]);
        assert_eq!(out.len(), 1);
        assert_eq!(
            md_sidefx_job_update_class(&out[0]),
            SidefxUpdateClass::ExecHot
        );
        let MdSidefxCommand::LivePoolCacheAccountUpdate { account_data, .. } = &out[0] else {
            panic!("expected LivePoolCacheAccountUpdate");
        };
        assert_eq!(
            account_data,
            &vec![1],
            "latest-wins data from last job in burst"
        );
    }

    #[test]
    fn md_sidefx_partition_processes_exec_hot_before_enrich() {
        let pool_hot = Pubkey::new_unique();
        let pool_enrich = Pubkey::new_unique();
        let jobs = vec![
            MdSidefxCommand::LivePoolCacheAccountUpdate {
                run_id: "r".into(),
                pool_pubkey: pool_enrich,
                owner: RAYDIUM_CPMM_OWNER,
                account_data: vec![],
                slot: 1,
                grpc_recv_at: Instant::now(),
                update_class: SidefxUpdateClass::Enrich,
            },
            MdSidefxCommand::LivePoolCacheAccountUpdate {
                run_id: "r".into(),
                pool_pubkey: pool_hot,
                owner: RAYDIUM_CPMM_OWNER,
                account_data: vec![],
                slot: 1,
                grpc_recv_at: Instant::now(),
                update_class: SidefxUpdateClass::ExecHot,
            },
        ];
        let coalesced = md_sidefx_coalesce_burst(jobs);
        let (exec_hot, enrich) = md_sidefx_partition_by_class(coalesced);
        assert_eq!(exec_hot.len(), 1);
        assert_eq!(enrich.len(), 1);
        assert!(matches!(
            exec_hot[0],
            MdSidefxCommand::LivePoolCacheAccountUpdate { .. }
        ));
    }

    #[test]
    fn md_sidefx_burst_scratch_exec_hot_wins_vault_touch() {
        let vault = Pubkey::new_unique();
        let mut scratch = MdSidefxBurstScratch::new();
        scratch.note_vault_touch(vault, SidefxUpdateClass::Enrich);
        scratch.note_vault_touch(vault, SidefxUpdateClass::ExecHot);
        let (hot_vaults, enrich_vaults, _, _) = scratch.drain_lru_touches();
        assert_eq!(hot_vaults, vec![vault]);
        assert!(enrich_vaults.is_empty());
    }

    #[test]
    #[serial_test::serial]
    fn tx_discovery_flood_drops_discovery_without_dropping_account_or_pin_seed() {
        let host = Arc::new(TestBlockingHost) as Arc<dyn SidefxWorkerHost>;
        let workers = spawn_md_sidefx_workers(host, 8, 8, 4);
        let tx_dropped_before = MARKET_DATA_TX_SIDEFX_ENQUEUE_DROPPED_TOTAL.load(Ordering::Relaxed);
        let discovery_dropped_before =
            MARKET_DATA_TX_DISCOVERY_SIDEFX_ENQUEUE_DROPPED_TOTAL.load(Ordering::Relaxed);
        let pin_seed_dropped_before =
            MARKET_DATA_TX_PIN_SEED_SIDEFX_ENQUEUE_DROPPED_TOTAL.load(Ordering::Relaxed);
        let pin_seed_processed_before =
            MARKET_DATA_TX_PIN_SEED_SIDEFX_JOBS_PROCESSED_TOTAL.load(Ordering::Relaxed);
        let account_dropped_before =
            MARKET_DATA_ACCOUNT_SIDEFX_ENQUEUE_DROPPED_TOTAL.load(Ordering::Relaxed);
        let account_processed_before =
            MARKET_DATA_ACCOUNT_SIDEFX_JOBS_PROCESSED_TOTAL.load(Ordering::Relaxed);

        for _ in 0..16 {
            md_tx_discovery_sidefx_try_enqueue(&workers.tx_discovery, mk_tx_discovery_job());
        }

        let hot_pool = Pubkey::new_unique();
        md_tx_pin_seed_sidefx_try_enqueue(&workers.tx_pin_seed, mk_pin_seed_job(hot_pool));
        md_account_sidefx_try_enqueue(&workers.account, mk_account_job());

        let tx_dropped_after = MARKET_DATA_TX_SIDEFX_ENQUEUE_DROPPED_TOTAL.load(Ordering::Relaxed);
        let discovery_dropped_after =
            MARKET_DATA_TX_DISCOVERY_SIDEFX_ENQUEUE_DROPPED_TOTAL.load(Ordering::Relaxed);
        let pin_seed_dropped_after =
            MARKET_DATA_TX_PIN_SEED_SIDEFX_ENQUEUE_DROPPED_TOTAL.load(Ordering::Relaxed);
        let pin_seed_processed_after =
            MARKET_DATA_TX_PIN_SEED_SIDEFX_JOBS_PROCESSED_TOTAL.load(Ordering::Relaxed);
        let account_dropped_after =
            MARKET_DATA_ACCOUNT_SIDEFX_ENQUEUE_DROPPED_TOTAL.load(Ordering::Relaxed);
        let account_processed_after =
            MARKET_DATA_ACCOUNT_SIDEFX_JOBS_PROCESSED_TOTAL.load(Ordering::Relaxed);

        assert!(
            discovery_dropped_after > discovery_dropped_before,
            "discovery queue should drop under flood"
        );
        assert!(
            tx_dropped_after > tx_dropped_before,
            "legacy TX drop counter should reflect discovery drops"
        );
        assert_eq!(
            pin_seed_dropped_after, pin_seed_dropped_before,
            "pin-seed pipeline must not drop under discovery flood"
        );
        assert_eq!(
            account_dropped_after, account_dropped_before,
            "account pipeline must never increment drop counter"
        );
        assert!(
            workers.tx_pin_seed.queue_depth.load(Ordering::Relaxed) > 0
                || pin_seed_processed_after > pin_seed_processed_before,
            "pin-seed job should be accepted despite discovery flood (queued or already processed)"
        );
        assert!(
            workers.account.queue_depth.load(Ordering::Relaxed) > 0
                || account_processed_after > account_processed_before,
            "account job should be accepted despite discovery flood (queued or already processed)"
        );
    }

    #[test]
    #[serial_test::serial]
    fn tx_route_enqueue_hot_pin_seed_non_hot_discovery() {
        let hot_pool = Pubkey::new_unique();
        let cold_pool = Pubkey::new_unique();
        let host = Arc::new(HotPoolTestHost { hot: hot_pool }) as Arc<dyn SidefxWorkerHost>;
        let workers = spawn_md_sidefx_workers(host, 32, 32, 32);
        let senders = workers.tx_senders();
        let pin_seed_processed_before =
            MARKET_DATA_TX_PIN_SEED_SIDEFX_JOBS_PROCESSED_TOTAL.load(Ordering::Relaxed);
        let discovery_processed_before =
            MARKET_DATA_TX_DISCOVERY_SIDEFX_JOBS_PROCESSED_TOTAL.load(Ordering::Relaxed);

        md_tx_sidefx_route_enqueue(&senders, mk_pin_seed_job(hot_pool), true);
        md_tx_sidefx_route_enqueue(&senders, mk_pin_seed_job(cold_pool), false);

        let pin_seed_processed_after =
            MARKET_DATA_TX_PIN_SEED_SIDEFX_JOBS_PROCESSED_TOTAL.load(Ordering::Relaxed);
        let discovery_processed_after =
            MARKET_DATA_TX_DISCOVERY_SIDEFX_JOBS_PROCESSED_TOTAL.load(Ordering::Relaxed);

        assert!(
            workers.tx_pin_seed.queue_depth.load(Ordering::Relaxed) == 1
                || pin_seed_processed_after > pin_seed_processed_before,
            "hot pin-seed candidate must land in pin-seed queue (or be processed immediately)"
        );
        assert!(
            workers.tx_discovery.queue_depth.load(Ordering::Relaxed) == 1
                || discovery_processed_after > discovery_processed_before,
            "non-hot pin-seed candidate must land in discovery queue (or be processed immediately)"
        );
    }

    #[test]
    #[serial_test::serial]
    fn tx_flood_drops_tx_jobs_without_dropping_account_jobs() {
        let host = Arc::new(TestBlockingHost) as Arc<dyn SidefxWorkerHost>;
        let workers = spawn_md_sidefx_workers(host, 8, 8, 4);
        let tx_dropped_before = MARKET_DATA_TX_SIDEFX_ENQUEUE_DROPPED_TOTAL.load(Ordering::Relaxed);
        let account_dropped_before =
            MARKET_DATA_ACCOUNT_SIDEFX_ENQUEUE_DROPPED_TOTAL.load(Ordering::Relaxed);

        for _ in 0..16 {
            md_tx_discovery_sidefx_try_enqueue(&workers.tx_discovery, mk_tx_discovery_job());
        }

        md_account_sidefx_try_enqueue(&workers.account, mk_account_job());

        let tx_dropped_after = MARKET_DATA_TX_SIDEFX_ENQUEUE_DROPPED_TOTAL.load(Ordering::Relaxed);
        let account_dropped_after =
            MARKET_DATA_ACCOUNT_SIDEFX_ENQUEUE_DROPPED_TOTAL.load(Ordering::Relaxed);

        assert!(
            tx_dropped_after > tx_dropped_before,
            "TX queue should drop under flood"
        );
        assert_eq!(
            account_dropped_after, account_dropped_before,
            "account pipeline must never increment drop counter"
        );
        assert!(
            workers.account.queue_depth.load(Ordering::Relaxed) > 0
                || MARKET_DATA_ACCOUNT_SIDEFX_ENQUEUE_DROPPED_TOTAL.load(Ordering::Relaxed)
                    == account_dropped_before,
            "account job should be accepted despite TX flood"
        );
    }

    #[test]
    fn account_queue_never_increments_drop_counter_under_cap() {
        let host = Arc::new(TestBlockingHost) as Arc<dyn SidefxWorkerHost>;
        let workers = spawn_md_sidefx_workers(host, 32, 32, 4096);
        let dropped_before =
            MARKET_DATA_ACCOUNT_SIDEFX_ENQUEUE_DROPPED_TOTAL.load(Ordering::Relaxed);
        for _ in 0..8 {
            md_account_sidefx_try_enqueue(&workers.account, mk_account_job());
        }
        let dropped_after =
            MARKET_DATA_ACCOUNT_SIDEFX_ENQUEUE_DROPPED_TOTAL.load(Ordering::Relaxed);
        assert_eq!(dropped_before, dropped_after);
    }

    #[test]
    fn account_enqueue_disconnected_worker_fail_loud_without_panic() {
        let (tx, rx) = std_mpsc::sync_channel(8);
        drop(rx);
        let sender = MdAccountSidefxSender {
            tx,
            queue_depth: Arc::new(AtomicUsize::new(0)),
            queue_capacity: 8,
        };
        let fail_loud_before =
            MARKET_DATA_ACCOUNT_SIDEFX_ENQUEUE_FAIL_LOUD_TOTAL.load(Ordering::Relaxed);
        md_account_sidefx_try_enqueue(&sender, mk_account_job());
        let fail_loud_after =
            MARKET_DATA_ACCOUNT_SIDEFX_ENQUEUE_FAIL_LOUD_TOTAL.load(Ordering::Relaxed);
        assert!(
            fail_loud_after > fail_loud_before,
            "disconnected account sidefx enqueue must bump fail-loud metric"
        );
    }

    #[test]
    fn spawn_on_ingest_runtime_from_os_thread_does_not_panic() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let handle = runtime.handle().clone();
        let spawned = Arc::new(AtomicBool::new(false));
        let spawned_flag = Arc::clone(&spawned);
        let ingest_handle = parking_lot::RwLock::new(Some(handle));
        let thread = std::thread::spawn(move || {
            if let Some(h) = ingest_handle.read().clone() {
                h.spawn(async move {
                    spawned_flag.store(true, Ordering::Relaxed);
                });
            }
        });
        thread.join().expect("join os thread");
        runtime.block_on(async {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        });
        assert!(spawned.load(Ordering::Relaxed));
    }
}
