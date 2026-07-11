//! Bounded durable pending state for track-worker commands lost on full queue.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use solana_sdk::pubkey::Pubkey;

use crate::market_data::track::desired_set::ConsumerId;
use crate::market_data::track::worker_commands::{PoolExplicitSnapshot, TrackWorkerCommand};

/// Coalesced pool command awaiting worker replay after queue loss.
#[derive(Debug, Clone)]
pub enum PendingPoolCommand {
    RegisterReserves(PoolExplicitSnapshot),
    VaultsFromAccount(PoolExplicitSnapshot),
    AfterTrade(PoolExplicitSnapshot),
    RefreshDlmm {
        snapshot: PoolExplicitSnapshot,
        new_active_id: i32,
    },
}

impl PendingPoolCommand {
    pub fn pool(&self) -> Pubkey {
        match self {
            Self::RegisterReserves(s)
            | Self::VaultsFromAccount(s)
            | Self::AfterTrade(s)
            | Self::RefreshDlmm { snapshot: s, .. } => s.pool,
        }
    }

    pub fn consumer(&self) -> ConsumerId {
        match self {
            Self::RegisterReserves(s)
            | Self::VaultsFromAccount(s)
            | Self::AfterTrade(s)
            | Self::RefreshDlmm { snapshot: s, .. } => s.consumer,
        }
    }

    pub fn into_track_command(self) -> TrackWorkerCommand {
        match self {
            Self::RegisterReserves(snapshot) => {
                TrackWorkerCommand::RegisterPoolGeyserReserves { snapshot }
            }
            Self::VaultsFromAccount(snapshot) => {
                TrackWorkerCommand::RegisterPoolVaultsFromAccount { snapshot }
            }
            Self::AfterTrade(snapshot) => {
                TrackWorkerCommand::RegisterGeyserReservesAfterTrade { snapshot }
            }
            Self::RefreshDlmm {
                snapshot,
                new_active_id,
            } => TrackWorkerCommand::RefreshDlmmBinWindow {
                snapshot,
                new_active_id,
            },
        }
    }
}

/// Result of stashing a pool command in durable pending state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PendingPoolUpsertResult {
    Stored,
    Coalesced,
    Overflow,
}

/// Per-pool coalesced pending commands (one slot per pool+consumer, merged by kind).
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct CoalescedPoolPending {
    pool: Pubkey,
    consumer: ConsumerId,
    reserves: Option<PoolExplicitSnapshot>,
    vaults: Option<PoolExplicitSnapshot>,
    after_trade: Option<PoolExplicitSnapshot>,
    refresh_dlmm: Option<(PoolExplicitSnapshot, i32)>,
}

impl CoalescedPoolPending {
    fn merge(&mut self, command: PendingPoolCommand) {
        match command {
            PendingPoolCommand::RegisterReserves(snapshot) => {
                self.reserves = Some(snapshot);
            }
            PendingPoolCommand::VaultsFromAccount(snapshot) => {
                self.vaults = Some(snapshot);
            }
            PendingPoolCommand::AfterTrade(snapshot) => {
                self.after_trade = Some(snapshot);
            }
            PendingPoolCommand::RefreshDlmm {
                snapshot,
                new_active_id,
            } => {
                self.refresh_dlmm = Some((snapshot, new_active_id));
            }
        }
    }

    fn into_commands(self) -> Vec<PendingPoolCommand> {
        let mut out = Vec::new();
        if let Some(snapshot) = self.reserves {
            out.push(PendingPoolCommand::RegisterReserves(snapshot));
        }
        if let Some(snapshot) = self.vaults {
            out.push(PendingPoolCommand::VaultsFromAccount(snapshot));
        }
        if let Some(snapshot) = self.after_trade {
            out.push(PendingPoolCommand::AfterTrade(snapshot));
        }
        if let Some((snapshot, new_active_id)) = self.refresh_dlmm {
            out.push(PendingPoolCommand::RefreshDlmm {
                snapshot,
                new_active_id,
            });
        }
        out
    }
}

/// Authoritative wallet explicit demand merged under lock (no lost-update on burst ATA).
#[derive(Debug, Default)]
pub struct WalletExplicitPending {
    inner: Mutex<WalletExplicitState>,
    revision: AtomicU64,
}

#[derive(Debug, Default, Clone)]
struct WalletExplicitState {
    demand: HashSet<Pubkey>,
    token_accounts: HashSet<Pubkey>,
}

impl WalletExplicitPending {
    pub fn insert_ata(&self, ata: Pubkey) -> u64 {
        let mut g = self.inner.lock().expect("wallet pending lock");
        g.demand.insert(ata);
        g.token_accounts.insert(ata);
        self.bump_revision()
    }

    pub fn remove_ata(&self, ata: Pubkey) -> u64 {
        let mut g = self.inner.lock().expect("wallet pending lock");
        g.demand.remove(&ata);
        g.token_accounts.remove(&ata);
        self.bump_revision()
    }

    pub fn replace_token_accounts(&self, accounts: HashSet<Pubkey>) -> u64 {
        let mut g = self.inner.lock().expect("wallet pending lock");
        g.token_accounts = accounts;
        self.bump_revision()
    }

    pub fn snapshot(&self) -> (HashSet<Pubkey>, HashSet<Pubkey>, u64) {
        let g = self.inner.lock().expect("wallet pending lock");
        (
            g.demand.clone(),
            g.token_accounts.clone(),
            self.revision.load(Ordering::Acquire),
        )
    }

    pub fn ensure_wallet_base(&self, wallet: Pubkey, wsol_ata: Pubkey) -> u64 {
        let mut g = self.inner.lock().expect("wallet pending lock");
        let mut changed = false;
        if g.demand.insert(wallet) {
            changed = true;
        }
        if g.demand.insert(wsol_ata) {
            changed = true;
        }
        drop(g);
        if changed {
            self.bump_revision()
        } else {
            self.current_revision()
        }
    }

    pub fn contains_demand(&self, pk: Pubkey) -> bool {
        self.inner
            .lock()
            .expect("wallet pending lock")
            .demand
            .contains(&pk)
    }

    pub fn current_revision(&self) -> u64 {
        self.revision.load(Ordering::Acquire)
    }

    fn bump_revision(&self) -> u64 {
        self.revision.fetch_add(1, Ordering::AcqRel) + 1
    }
}

/// Bounded per-pool coalesced pending (one entry per pool+consumer; overflow is fail-closed).
#[derive(Debug)]
pub struct PendingPoolRegistrations {
    max_pools: usize,
    entries: Mutex<HashMap<(Pubkey, ConsumerId), CoalescedPoolPending>>,
    order: Mutex<VecDeque<(Pubkey, ConsumerId)>>,
    overflow: AtomicBool,
}

impl PendingPoolRegistrations {
    pub fn new(max_pools: usize) -> Self {
        Self {
            max_pools: max_pools.max(1),
            entries: Mutex::new(HashMap::new()),
            order: Mutex::new(VecDeque::new()),
            overflow: AtomicBool::new(false),
        }
    }

    pub fn upsert(&self, command: PendingPoolCommand) -> PendingPoolUpsertResult {
        let key = (command.pool(), command.consumer());
        let mut entries = self.entries.lock().expect("pending pool lock");
        let mut order = self.order.lock().expect("pending pool order lock");
        if entries.contains_key(&key) {
            entries.get_mut(&key).expect("pending key").merge(command);
            return PendingPoolUpsertResult::Coalesced;
        }
        if order.len() >= self.max_pools {
            self.overflow.store(true, Ordering::Release);
            return PendingPoolUpsertResult::Overflow;
        }
        let mut coalesced = CoalescedPoolPending {
            pool: key.0,
            consumer: key.1,
            reserves: None,
            vaults: None,
            after_trade: None,
            refresh_dlmm: None,
        };
        coalesced.merge(command);
        entries.insert(key, coalesced);
        order.push_back(key);
        PendingPoolUpsertResult::Stored
    }

    pub fn drain_all(&self) -> Vec<PendingPoolCommand> {
        let mut entries = self.entries.lock().expect("pending pool lock");
        let mut order = self.order.lock().expect("pending pool order lock");
        let keys: Vec<_> = order.drain(..).collect();
        keys.into_iter()
            .filter_map(|k| entries.remove(&k))
            .flat_map(|coalesced| coalesced.into_commands())
            .collect()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.lock().expect("pending pool lock").is_empty()
    }

    pub fn overflowed(&self) -> bool {
        self.overflow.load(Ordering::Acquire)
    }

    pub fn clear_overflow(&self) {
        self.overflow.store(false, Ordering::Release);
    }

    pub fn pool_count(&self) -> usize {
        self.entries.lock().expect("pending pool lock").len()
    }
}

/// Startup Geyser connect barrier: worker signals ready/failed after restore+convergence.
#[derive(Debug)]
pub struct GeyserConnectBarrier {
    state: AtomicU8,
}

const BARRIER_PENDING: u8 = 0;
const BARRIER_READY: u8 = 1;
const BARRIER_FAILED: u8 = 2;

impl Default for GeyserConnectBarrier {
    fn default() -> Self {
        Self::new()
    }
}

impl GeyserConnectBarrier {
    pub fn new() -> Self {
        Self {
            state: AtomicU8::new(BARRIER_PENDING),
        }
    }

    pub fn mark_ready(&self) {
        self.state.store(BARRIER_READY, Ordering::Release);
    }

    pub fn mark_failed(&self) {
        self.state.store(BARRIER_FAILED, Ordering::Release);
    }

    pub fn mark_pending(&self) {
        self.state.store(BARRIER_PENDING, Ordering::Release);
    }

    pub fn is_ready(&self) -> bool {
        self.state.load(Ordering::Acquire) == BARRIER_READY
    }

    pub fn wait_ready(&self, timeout: Duration) -> Result<(), &'static str> {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            match self.state.load(Ordering::Acquire) {
                BARRIER_READY => return Ok(()),
                BARRIER_FAILED => return Err("geyser_explicit_barrier_failed"),
                _ => std::thread::sleep(Duration::from_millis(5)),
            }
        }
        Err("geyser_explicit_barrier_timeout")
    }
}

/// Bounded diagnostic for protected wallet overflow (no unbounded clone on fail-closed).
#[derive(Debug, Clone)]
pub struct ProtectedOverflowDiagnostic {
    pub configured_cap: usize,
    pub wallet_demand_len: usize,
    pub sample_wallet_pubkeys: Vec<Pubkey>,
}

impl ProtectedOverflowDiagnostic {
    pub fn from_demand(cap: usize, demand: &HashSet<Pubkey>) -> Self {
        Self {
            configured_cap: cap,
            wallet_demand_len: demand.len(),
            sample_wallet_pubkeys: demand.iter().copied().take(8).collect(),
        }
    }
}
