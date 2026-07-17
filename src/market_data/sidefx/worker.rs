//! Phase 5b: `md-sidefx` OS thread — bounded enqueue, burst coalesce, deferred publish.

use super::handlers::md_sidefx_process_job;
use super::host::SidefxWorkerHost;
use crate::market_data::ingest::AccountUpdateClass;
use crate::metrics::{
    inc_market_data_md_sidefx_enqueue_dropped_total,
    inc_market_data_md_sidefx_enrich_enqueue_dropped_total,
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

/// Reserve headroom for EXEC_HOT when the sidefx queue is under pressure.
const MARKET_DATA_MD_SIDEFX_ENRICH_HEADROOM: usize = 512;

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
    let class = md_sidefx_job_update_class(&job);
    md_sidefx_try_enqueue_classed(sender, class, job);
}

pub fn md_sidefx_try_enqueue_classed(
    sender: &MdSidefxSender,
    class: SidefxUpdateClass,
    job: MdSidefxCommand,
) {
    let depth = sender.queue_depth.load(Ordering::Relaxed);
    if depth >= sender.queue_capacity {
        if class.is_exec_hot() {
            inc_market_data_md_sidefx_enqueue_dropped_total();
        } else {
            inc_market_data_md_sidefx_enrich_enqueue_dropped_total();
        }
        return;
    }
    if !class.is_exec_hot()
        && depth + MARKET_DATA_MD_SIDEFX_ENRICH_HEADROOM >= sender.queue_capacity
    {
        inc_market_data_md_sidefx_enrich_enqueue_dropped_total();
        return;
    }
    if sender.tx.try_send(job).is_ok() {
        let new_depth = sender.queue_depth.fetch_add(1, Ordering::Relaxed) + 1;
        set_market_data_md_sidefx_queue_depth(new_depth);
    } else if class.is_exec_hot() {
        inc_market_data_md_sidefx_enqueue_dropped_total();
    } else {
        inc_market_data_md_sidefx_enrich_enqueue_dropped_total();
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

#[cfg(test)]
mod tests {
    use super::*;

    const RAYDIUM_CPMM_OWNER: Pubkey =
        solana_sdk::pubkey!("CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C");

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
        assert_eq!(account_data, &vec![1], "latest-wins data from last job in burst");
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
}
