//! Phase 5b: `md-sidefx` OS thread — bounded enqueue, burst coalesce, deferred publish.

use super::handlers::md_sidefx_process_job;
use super::host::SidefxWorkerHost;
use crate::metrics::{
    inc_market_data_md_sidefx_enqueue_dropped_total,
    inc_market_data_md_sidefx_jobs_processed_total, set_market_data_md_sidefx_queue_depth,
};
use crate::solana::dex_parser::DexType;
use solana_sdk::pubkey::Pubkey;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc as std_mpsc;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Instant;

/// Phase-R-R4: bounded queue for deferred pool_mint_map / publish / live_pool_cache side-effects.
pub const MARKET_DATA_MD_SIDEFX_QUEUE_CAP: usize = 4096;
/// Phase-R-R4: max jobs drained per `md-sidefx` burst before coalesce pass.
pub const MARKET_DATA_MD_SIDEFX_BURST_MAX: usize = 128;

pub enum MdSidefxCommand {
    PumpFunPoolMintMapInsert {
        run_id: String,
        pool_address: Pubkey,
        mint_str: String,
        slot: Option<u64>,
        tx_grpc_recv_at: Instant,
    },
    PumpFunDevWalletFromPoolCreated {
        run_id: String,
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
    },
    /// DLMM bin-array LRU touch off account ingest (coalesced in md-sidefx burst).
    TouchBinArrayTick { pda: Pubkey },
    /// P0: coalesced PoolStateUpdate for hot Meteora DLMM pools (bin/state signal, no vault delta).
    DlmmPoolStatePublishSignal {
        run_id: String,
        pool_address: Pubkey,
        slot: u64,
        grpc_recv_at: Instant,
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
    },
    /// Phase-R-R4b: mint decimals mirror into MASTER cache (TokenMintInfo path).
    LivePoolCacheMintDecimals { mint: Pubkey, decimals: u8 },
}

/// Bounded enqueue handle for the `md-sidefx` OS thread (non-Tokio).
#[derive(Clone)]
pub struct MdSidefxSender {
    tx: std_mpsc::SyncSender<MdSidefxCommand>,
    queue_depth: Arc<AtomicUsize>,
    pub queue_capacity: usize,
}

/// Per-burst coalesced DLMM pool-state publish signal (latest slot wins per pool).
#[derive(Clone)]
pub struct DlmmPoolStateSignal {
    pub run_id: String,
    pub slot: u64,
    pub grpc_recv_at: Instant,
}

/// Per-burst scratch: LRU touches coalesced before md-state enqueue.
pub struct MdSidefxBurstScratch {
    pending_vault_touches: HashSet<Pubkey>,
    pending_bin_array_touches: HashSet<Pubkey>,
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
            pending_vault_touches: HashSet::new(),
            pending_bin_array_touches: HashSet::new(),
            pending_dlmm_pool_state_signals: HashMap::new(),
        }
    }

    pub fn note_vault_touch(&mut self, vault: Pubkey) {
        self.pending_vault_touches.insert(vault);
    }

    pub fn note_bin_array_touch(&mut self, pda: Pubkey) {
        self.pending_bin_array_touches.insert(pda);
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
        self.pending_vault_touches.contains(vault)
    }

    pub fn lru_touches_empty(&self) -> bool {
        self.pending_vault_touches.is_empty() && self.pending_bin_array_touches.is_empty()
    }

    pub fn drain_lru_touches(&mut self) -> (Vec<Pubkey>, Vec<Pubkey>) {
        (
            self.pending_vault_touches.drain().collect(),
            self.pending_bin_array_touches.drain().collect(),
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

fn md_sidefx_dec_queue_depth(queue_depth: &AtomicUsize) {
    let mut cur = queue_depth.load(Ordering::Relaxed);
    while cur > 0 {
        match queue_depth.compare_exchange_weak(cur, cur - 1, Ordering::Relaxed, Ordering::Relaxed)
        {
            Ok(_) => {
                set_market_data_md_sidefx_queue_depth(cur - 1);
                return;
            }
            Err(actual) => cur = actual,
        }
    }
    set_market_data_md_sidefx_queue_depth(0);
}

pub fn md_sidefx_try_enqueue(sender: &MdSidefxSender, job: MdSidefxCommand) {
    if sender.queue_depth.load(Ordering::Relaxed) >= sender.queue_capacity {
        inc_market_data_md_sidefx_enqueue_dropped_total();
        return;
    }
    if sender.tx.try_send(job).is_ok() {
        let depth = sender.queue_depth.fetch_add(1, Ordering::Relaxed) + 1;
        set_market_data_md_sidefx_queue_depth(depth);
    } else {
        inc_market_data_md_sidefx_enqueue_dropped_total();
    }
}

pub fn md_sidefx_coalesce_key(job: &MdSidefxCommand) -> Option<Pubkey> {
    match job {
        MdSidefxCommand::PumpFunPoolMintMapInsert { pool_address, .. } => Some(*pool_address),
        MdSidefxCommand::PumpAmmTradeWithAccounts { pool_address, .. } => Some(*pool_address),
        MdSidefxCommand::PumpAmmCreatePoolObserved { pool_address, .. } => Some(*pool_address),
        MdSidefxCommand::LivePoolCacheAccountUpdate { pool_pubkey, .. } => Some(*pool_pubkey),
        MdSidefxCommand::VaultBalanceTick { vault_pubkey, .. } => Some(*vault_pubkey),
        MdSidefxCommand::TouchBinArrayTick { pda } => Some(*pda),
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
                out[idx] = job;
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

fn md_sidefx_worker_loop(
    worker: Arc<dyn SidefxWorkerHost>,
    rx: std_mpsc::Receiver<MdSidefxCommand>,
    queue_depth: Arc<AtomicUsize>,
) {
    loop {
        let Ok(first) = rx.recv() else {
            break;
        };
        md_sidefx_dec_queue_depth(&queue_depth);
        inc_market_data_md_sidefx_jobs_processed_total();
        let mut jobs = vec![first];
        while jobs.len() < MARKET_DATA_MD_SIDEFX_BURST_MAX {
            match rx.try_recv() {
                Ok(job) => {
                    md_sidefx_dec_queue_depth(&queue_depth);
                    inc_market_data_md_sidefx_jobs_processed_total();
                    jobs.push(job);
                }
                Err(std_mpsc::TryRecvError::Empty) => break,
                Err(std_mpsc::TryRecvError::Disconnected) => break,
            }
        }
        let mut scratch = MdSidefxBurstScratch::new();
        for job in md_sidefx_coalesce_burst(jobs) {
            md_sidefx_process_job(worker.as_ref(), &job, &mut scratch);
        }
        md_sidefx_flush_pending_md_state_jobs(worker.as_ref(), &mut scratch);
    }
}

pub fn spawn_md_sidefx_worker(
    host: Arc<dyn SidefxWorkerHost>,
    queue_capacity: usize,
) -> MdSidefxSender {
    let (tx, rx) = std_mpsc::sync_channel::<MdSidefxCommand>(queue_capacity);
    let queue_depth = Arc::new(AtomicUsize::new(0));
    let depth_worker = Arc::clone(&queue_depth);
    let _join: JoinHandle<()> = std::thread::Builder::new()
        .name("md-sidefx".into())
        .spawn(move || md_sidefx_worker_loop(host, rx, depth_worker))
        .expect("spawn md-sidefx thread");
    MdSidefxSender {
        tx,
        queue_depth,
        queue_capacity,
    }
}
