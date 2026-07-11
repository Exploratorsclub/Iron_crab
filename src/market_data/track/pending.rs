//! Bounded durable pending state for track-worker commands lost on full queue.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use solana_sdk::pubkey::Pubkey;

use crate::market_data::track::desired_set::ConsumerId;
use crate::market_data::track::worker_commands::{PoolExplicitSnapshot, TrackWorkerCommand};

const REVISION_SEQ_MASK: u64 = 0xFFFF_FFFF;
const REVISION_LIFECYCLE_SHIFT: u32 = 32;
const DEFAULT_MAX_REVISION_SLOTS: usize = 4096;

/// Result of reserving a revision-registry slot for a pool+consumer key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RevisionAcquireResult {
    Acquired,
    RegistryFull,
}

/// Result of issuing the next revision for a registered key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RevisionAssignResult {
    Assigned(u64),
    RegistryFull,
    KeyNotRegistered,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct RevisionRefCounts {
    pub active: u32,
    pub pending: u32,
    pub inflight: u32,
}

impl RevisionRefCounts {
    fn total(self) -> u32 {
        self.active
            .saturating_add(self.pending)
            .saturating_add(self.inflight)
    }
}

#[derive(Debug, Clone)]
struct RevisionSlot {
    lifecycle_id: u32,
    issued_seq: u32,
    applied_seq: u32,
    refs: RevisionRefCounts,
    touch_stamp: u64,
}

/// Bounded per-(pool,consumer) revision registry with refcounted lifecycle.
///
/// Revisions pack `(lifecycle_id << 32) | sequence`. Slots are recycled only when all
/// refcounts are zero. Active / pending / in-flight keys are never evicted.
#[derive(Debug)]
pub struct PoolSnapshotRevisionSequencer {
    max_keys: usize,
    slots: Mutex<HashMap<(Pubkey, ConsumerId), RevisionSlot>>,
    next_lifecycle_id: AtomicU32,
    touch_seq: AtomicU64,
}

impl Default for PoolSnapshotRevisionSequencer {
    fn default() -> Self {
        Self::new()
    }
}

impl PoolSnapshotRevisionSequencer {
    pub fn new() -> Self {
        Self::with_max_keys(DEFAULT_MAX_REVISION_SLOTS)
    }

    pub fn with_max_keys(max_keys: usize) -> Self {
        Self {
            max_keys: max_keys.max(1),
            slots: Mutex::new(HashMap::new()),
            next_lifecycle_id: AtomicU32::new(1),
            touch_seq: AtomicU64::new(0),
        }
    }

    pub fn pack_revision(lifecycle_id: u32, sequence: u32) -> u64 {
        ((lifecycle_id as u64) << REVISION_LIFECYCLE_SHIFT) | (sequence as u64)
    }

    pub fn unpack_revision(revision: u64) -> (u32, u32) {
        (
            (revision >> REVISION_LIFECYCLE_SHIFT) as u32,
            (revision & REVISION_SEQ_MASK) as u32,
        )
    }

    pub fn revision_sequence(revision: u64) -> u32 {
        Self::unpack_revision(revision).1
    }

    pub fn revision_lifecycle_id(revision: u64) -> u32 {
        Self::unpack_revision(revision).0
    }

    #[deprecated(note = "use revision_lifecycle_id")]
    pub fn revision_generation(revision: u64) -> u32 {
        Self::revision_lifecycle_id(revision)
    }

    pub fn max_keys(&self) -> usize {
        self.max_keys
    }

    pub fn active_key_count(&self) -> usize {
        self.slots.lock().expect("pool revision slots lock").len()
    }

    pub fn total_memory_slots(&self) -> usize {
        self.active_key_count()
    }

    fn next_touch_stamp(&self) -> u64 {
        self.touch_seq
            .fetch_add(1, Ordering::AcqRel)
            .saturating_add(1)
    }

    fn fresh_lifecycle_id(&self) -> u32 {
        self.next_lifecycle_id.fetch_add(1, Ordering::AcqRel).max(1)
    }

    fn new_slot(&self) -> RevisionSlot {
        RevisionSlot {
            lifecycle_id: self.fresh_lifecycle_id(),
            issued_seq: 0,
            applied_seq: 0,
            refs: RevisionRefCounts::default(),
            touch_stamp: self.next_touch_stamp(),
        }
    }

    fn ensure_slot(&self, key: (Pubkey, ConsumerId)) -> Result<(), RevisionAcquireResult> {
        let mut slots = self.slots.lock().expect("pool revision slots lock");
        if slots.contains_key(&key) {
            return Ok(());
        }
        if slots.len() < self.max_keys {
            slots.insert(key, self.new_slot());
            return Ok(());
        }
        let recyclable = slots
            .iter()
            .filter(|(_, slot)| slot.refs.total() == 0)
            .min_by_key(|(_, slot)| slot.touch_stamp)
            .map(|(k, _)| *k);
        let Some(victim_key) = recyclable else {
            return Err(RevisionAcquireResult::RegistryFull);
        };
        slots.remove(&victim_key);
        slots.insert(key, self.new_slot());
        Ok(())
    }

    pub fn try_acquire_active(&self, pool: Pubkey, consumer: ConsumerId) -> RevisionAcquireResult {
        let key = (pool, consumer);
        if let Err(result) = self.ensure_slot(key) {
            return result;
        }
        let mut slots = self.slots.lock().expect("pool revision slots lock");
        let Some(slot) = slots.get_mut(&key) else {
            return RevisionAcquireResult::RegistryFull;
        };
        slot.refs.active = slot.refs.active.saturating_add(1);
        slot.touch_stamp = self.next_touch_stamp();
        RevisionAcquireResult::Acquired
    }

    pub fn release_active_if_idle(&self, pool: Pubkey, consumer: ConsumerId) -> bool {
        let key = (pool, consumer);
        let mut slots = self.slots.lock().expect("pool revision slots lock");
        let Some(slot) = slots.get_mut(&key) else {
            return false;
        };
        slot.refs.active = slot.refs.active.saturating_sub(1);
        if slot.refs.total() == 0 {
            slots.remove(&key);
            return true;
        }
        false
    }

    pub fn inc_pending_ref(&self, pool: Pubkey, consumer: ConsumerId) {
        let key = (pool, consumer);
        let mut slots = self.slots.lock().expect("pool revision slots lock");
        if let Some(slot) = slots.get_mut(&key) {
            slot.refs.pending = slot.refs.pending.saturating_add(1);
            slot.touch_stamp = self.next_touch_stamp();
        }
    }

    pub fn dec_pending_ref(&self, pool: Pubkey, consumer: ConsumerId) {
        let key = (pool, consumer);
        let mut slots = self.slots.lock().expect("pool revision slots lock");
        let Some(slot) = slots.get_mut(&key) else {
            return;
        };
        slot.refs.pending = slot.refs.pending.saturating_sub(1);
        if slot.refs.total() == 0 {
            slots.remove(&key);
        }
    }

    pub fn inc_inflight_ref(&self, pool: Pubkey, consumer: ConsumerId) {
        let key = (pool, consumer);
        let mut slots = self.slots.lock().expect("pool revision slots lock");
        if let Some(slot) = slots.get_mut(&key) {
            slot.refs.inflight = slot.refs.inflight.saturating_add(1);
            slot.touch_stamp = self.next_touch_stamp();
        }
    }

    pub fn dec_inflight_ref(&self, pool: Pubkey, consumer: ConsumerId) {
        let key = (pool, consumer);
        let mut slots = self.slots.lock().expect("pool revision slots lock");
        let Some(slot) = slots.get_mut(&key) else {
            return;
        };
        slot.refs.inflight = slot.refs.inflight.saturating_sub(1);
        if slot.refs.total() == 0 {
            slots.remove(&key);
        }
    }

    pub fn key_refs(&self, pool: Pubkey, consumer: ConsumerId) -> RevisionRefCounts {
        self.slots
            .lock()
            .expect("pool revision slots lock")
            .get(&(pool, consumer))
            .map(|slot| slot.refs)
            .unwrap_or_default()
    }

    pub fn assign_next(&self, snapshot: &mut PoolExplicitSnapshot) -> RevisionAssignResult {
        let key = (snapshot.pool, snapshot.consumer);
        let mut slots = self.slots.lock().expect("pool revision slots lock");
        let Some(slot) = slots.get_mut(&key) else {
            return RevisionAssignResult::KeyNotRegistered;
        };
        slot.issued_seq = slot.issued_seq.saturating_add(1);
        if slot.issued_seq == 0 {
            slot.lifecycle_id = self.fresh_lifecycle_id();
            slot.issued_seq = 1;
        }
        slot.touch_stamp = self.next_touch_stamp();
        snapshot.revision = Self::pack_revision(slot.lifecycle_id, slot.issued_seq);
        RevisionAssignResult::Assigned(snapshot.revision)
    }

    pub fn should_apply_and_record(&self, snapshot: &PoolExplicitSnapshot) -> bool {
        if snapshot.revision == 0 {
            return false;
        }
        let (lifecycle_id, seq) = Self::unpack_revision(snapshot.revision);
        let key = (snapshot.pool, snapshot.consumer);
        let mut slots = self.slots.lock().expect("pool revision slots lock");
        let Some(slot) = slots.get_mut(&key) else {
            return false;
        };
        if lifecycle_id != slot.lifecycle_id || seq <= slot.applied_seq {
            return false;
        }
        slot.applied_seq = seq;
        slot.refs.inflight = slot.refs.inflight.saturating_sub(1);
        slot.touch_stamp = self.next_touch_stamp();
        if slot.refs.total() == 0 {
            slots.remove(&key);
        }
        true
    }

    pub fn retire_key(&self, pool: Pubkey, consumer: ConsumerId) {
        let key = (pool, consumer);
        let mut slots = self.slots.lock().expect("pool revision slots lock");
        let Some(slot) = slots.get(&key) else {
            return;
        };
        if slot.refs.total() > 0 {
            return;
        }
        slots.remove(&key);
    }

    pub fn maybe_retire_key(
        &self,
        pool: Pubkey,
        consumer: ConsumerId,
        has_pending: bool,
        has_inflight: bool,
    ) {
        if has_pending || has_inflight {
            return;
        }
        self.retire_key(pool, consumer);
    }

    pub fn current_issued(&self, pool: Pubkey, consumer: ConsumerId) -> u64 {
        self.slots
            .lock()
            .expect("pool revision slots lock")
            .get(&(pool, consumer))
            .map(|slot| Self::pack_revision(slot.lifecycle_id, slot.issued_seq))
            .unwrap_or(0)
    }

    pub fn current_applied(&self, pool: Pubkey, consumer: ConsumerId) -> u64 {
        self.slots
            .lock()
            .expect("pool revision slots lock")
            .get(&(pool, consumer))
            .map(|slot| Self::pack_revision(slot.lifecycle_id, slot.applied_seq))
            .unwrap_or(0)
    }
}

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

    pub fn revision(&self) -> u64 {
        match self {
            Self::RegisterReserves(s)
            | Self::VaultsFromAccount(s)
            | Self::AfterTrade(s)
            | Self::RefreshDlmm { snapshot: s, .. } => s.revision,
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
    StaleNoOp,
    Overflow,
}

/// Per-pool latest authoritative pending command (one slot per pool+consumer, revision wins).
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct CoalescedPoolPending {
    pool: Pubkey,
    consumer: ConsumerId,
    latest_revision: u64,
    latest_command: PendingPoolCommand,
}

impl CoalescedPoolPending {
    fn try_merge(&mut self, revision: u64, command: PendingPoolCommand) -> bool {
        if revision > self.latest_revision {
            self.latest_revision = revision;
            self.latest_command = command;
            true
        } else {
            false
        }
    }

    fn into_command(self) -> PendingPoolCommand {
        self.latest_command
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
        g.demand.insert(wallet);
        g.demand.insert(wsol_ata);
        self.bump_revision()
    }

    pub fn current_revision(&self) -> u64 {
        self.revision.load(Ordering::Acquire)
    }

    pub fn contains_demand(&self, pk: Pubkey) -> bool {
        self.inner
            .lock()
            .expect("wallet pending lock")
            .demand
            .contains(&pk)
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
    revisions: std::sync::Arc<PoolSnapshotRevisionSequencer>,
}

impl PendingPoolRegistrations {
    pub fn new(max_pools: usize, revisions: std::sync::Arc<PoolSnapshotRevisionSequencer>) -> Self {
        Self {
            max_pools: max_pools.max(1),
            entries: Mutex::new(HashMap::new()),
            order: Mutex::new(VecDeque::new()),
            overflow: AtomicBool::new(false),
            revisions,
        }
    }

    pub fn upsert(&self, mut command: PendingPoolCommand) -> PendingPoolUpsertResult {
        let revision = match &mut command {
            PendingPoolCommand::RegisterReserves(s)
            | PendingPoolCommand::VaultsFromAccount(s)
            | PendingPoolCommand::AfterTrade(s) => {
                if s.revision == 0 {
                    match self.revisions.assign_next(s) {
                        RevisionAssignResult::Assigned(rev) => rev,
                        RevisionAssignResult::RegistryFull => {
                            self.overflow.store(true, Ordering::Release);
                            return PendingPoolUpsertResult::Overflow;
                        }
                        RevisionAssignResult::KeyNotRegistered => {
                            self.overflow.store(true, Ordering::Release);
                            return PendingPoolUpsertResult::Overflow;
                        }
                    }
                } else {
                    s.revision
                }
            }
            PendingPoolCommand::RefreshDlmm { snapshot, .. } => {
                if snapshot.revision == 0 {
                    match self.revisions.assign_next(snapshot) {
                        RevisionAssignResult::Assigned(rev) => rev,
                        RevisionAssignResult::RegistryFull => {
                            self.overflow.store(true, Ordering::Release);
                            return PendingPoolUpsertResult::Overflow;
                        }
                        RevisionAssignResult::KeyNotRegistered => {
                            self.overflow.store(true, Ordering::Release);
                            return PendingPoolUpsertResult::Overflow;
                        }
                    }
                } else {
                    snapshot.revision
                }
            }
        };
        let key = (command.pool(), command.consumer());
        let mut entries = self.entries.lock().expect("pending pool lock");
        let mut order = self.order.lock().expect("pending pool order lock");
        if let Some(entry) = entries.get_mut(&key) {
            if entry.try_merge(revision, command) {
                return PendingPoolUpsertResult::Coalesced;
            }
            return PendingPoolUpsertResult::StaleNoOp;
        }
        if order.len() >= self.max_pools {
            self.overflow.store(true, Ordering::Release);
            return PendingPoolUpsertResult::Overflow;
        }
        self.revisions.inc_pending_ref(key.0, key.1);
        let coalesced = CoalescedPoolPending {
            pool: key.0,
            consumer: key.1,
            latest_revision: revision,
            latest_command: command,
        };
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
            .map(|coalesced| {
                self.revisions
                    .dec_pending_ref(coalesced.pool, coalesced.consumer);
                coalesced.into_command()
            })
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

    pub fn latest_revision_for(&self, pool: Pubkey, consumer: ConsumerId) -> Option<u64> {
        self.entries
            .lock()
            .expect("pending pool lock")
            .get(&(pool, consumer))
            .map(|e| e.latest_revision)
    }

    pub fn has_pending(&self, pool: Pubkey, consumer: ConsumerId) -> bool {
        self.entries
            .lock()
            .expect("pending pool lock")
            .contains_key(&(pool, consumer))
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

    pub fn mark_pending(&self) {
        self.state.store(BARRIER_PENDING, Ordering::Release);
    }

    pub fn mark_failed(&self) {
        self.state.store(BARRIER_FAILED, Ordering::Release);
    }

    pub fn is_ready(&self) -> bool {
        self.state.load(Ordering::Acquire) == BARRIER_READY
    }

    pub fn is_failed(&self) -> bool {
        self.state.load(Ordering::Acquire) == BARRIER_FAILED
    }

    pub fn wait_ready(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            match self.state.load(Ordering::Acquire) {
                BARRIER_READY => return true,
                BARRIER_FAILED => return false,
                _ => std::thread::sleep(Duration::from_millis(10)),
            }
        }
        false
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
    pub fn from_demand(configured_cap: usize, demand: &HashSet<Pubkey>) -> Self {
        Self {
            configured_cap,
            wallet_demand_len: demand.len(),
            sample_wallet_pubkeys: demand.iter().copied().take(8).collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market_data::track::worker_commands::{
        BinArrayExplicitRow, MintExplicitRow, VaultExplicitRow,
    };

    fn mk_snapshot(pool: Pubkey, vault: Pubkey) -> PoolExplicitSnapshot {
        PoolExplicitSnapshot {
            pool,
            vaults: vec![VaultExplicitRow {
                pubkey: vault,
                dex: "raydium".into(),
                base_mint: Pubkey::new_unique(),
                quote_mint: Pubkey::new_unique(),
                is_base_vault: true,
                sibling_vault: None,
                active_id: None,
                bin_step: None,
            }],
            bin_arrays: vec![BinArrayExplicitRow {
                pubkey: Pubkey::new_unique(),
                bin_array_index: 0,
                bin_step: 1,
            }],
            mints: vec![MintExplicitRow {
                pubkey: Pubkey::new_unique(),
            }],
            consumer: ConsumerId::Momentum,
            owner: OwnerKey::Pool(pool),
            pin: GeyserPinReason::MomentumActive,
            revision: 0,
        }
    }

    use crate::market_data::track::desired_set::OwnerKey;
    use crate::market_data::track::worker_commands::GeyserPinReason;

    use std::sync::Arc;

    fn mk_pending(max: usize) -> (PendingPoolRegistrations, Arc<PoolSnapshotRevisionSequencer>) {
        let revisions = Arc::new(PoolSnapshotRevisionSequencer::with_max_keys(max));
        (
            PendingPoolRegistrations::new(max, Arc::clone(&revisions)),
            revisions,
        )
    }

    fn register_and_assign(
        revisions: &PoolSnapshotRevisionSequencer,
        pool: Pubkey,
        consumer: ConsumerId,
    ) -> u64 {
        assert_eq!(
            revisions.try_acquire_active(pool, consumer),
            RevisionAcquireResult::Acquired
        );
        let mut snapshot = mk_snapshot(pool, Pubkey::new_unique());
        match revisions.assign_next(&mut snapshot) {
            RevisionAssignResult::Assigned(rev) => rev,
            other => panic!("expected Assigned, got {other:?}"),
        }
    }

    #[test]
    fn pending_replays_latest_revision_not_kind_order() {
        let (pending, revisions) = mk_pending(8);
        let pool = Pubkey::new_unique();
        assert_eq!(
            revisions.try_acquire_active(pool, ConsumerId::Momentum),
            RevisionAcquireResult::Acquired
        );
        let old_vault = Pubkey::new_unique();
        let new_vault = Pubkey::new_unique();

        assert_eq!(
            pending.upsert(PendingPoolCommand::RegisterReserves(mk_snapshot(
                pool, old_vault
            ))),
            PendingPoolUpsertResult::Stored
        );
        assert_eq!(
            pending.upsert(PendingPoolCommand::AfterTrade(mk_snapshot(pool, new_vault))),
            PendingPoolUpsertResult::Coalesced
        );
        let drained = pending.drain_all();
        assert_eq!(drained.len(), 1);
        match &drained[0] {
            PendingPoolCommand::AfterTrade(s) => {
                assert_eq!(
                    PoolSnapshotRevisionSequencer::revision_sequence(s.revision),
                    2
                );
                assert_eq!(s.vaults[0].pubkey, new_vault);
            }
            other => panic!("expected AfterTrade latest, got {other:?}"),
        }
    }

    #[test]
    fn pending_stale_out_of_order_across_all_kinds_no_ops() {
        let (pending, revisions) = mk_pending(8);
        let pool = Pubkey::new_unique();
        assert_eq!(
            revisions.try_acquire_active(pool, ConsumerId::Momentum),
            RevisionAcquireResult::Acquired
        );
        let v2 = Pubkey::new_unique();
        let v3 = Pubkey::new_unique();
        let v4 = Pubkey::new_unique();

        assert_eq!(
            pending.upsert(PendingPoolCommand::VaultsFromAccount(mk_snapshot(pool, v2))),
            PendingPoolUpsertResult::Stored
        );
        let rev_vault = pending
            .latest_revision_for(pool, ConsumerId::Momentum)
            .expect("vault stored");

        assert_eq!(
            pending.upsert(PendingPoolCommand::RegisterReserves(mk_snapshot(pool, v4))),
            PendingPoolUpsertResult::Coalesced
        );
        let rev_reserve = pending
            .latest_revision_for(pool, ConsumerId::Momentum)
            .expect("reserve newer");

        assert_eq!(
            pending.upsert(PendingPoolCommand::RefreshDlmm {
                snapshot: mk_snapshot(pool, v3),
                new_active_id: 9,
            }),
            PendingPoolUpsertResult::Coalesced
        );
        let rev_dlmm = pending
            .latest_revision_for(pool, ConsumerId::Momentum)
            .expect("dlmm newest");

        assert!(rev_dlmm > rev_reserve);
        assert!(rev_reserve > rev_vault);

        let drained = pending.drain_all();
        assert_eq!(drained.len(), 1);
        match &drained[0] {
            PendingPoolCommand::RefreshDlmm {
                snapshot,
                new_active_id,
            } => {
                assert_eq!(snapshot.revision, rev_dlmm);
                assert_eq!(snapshot.vaults[0].pubkey, v3);
                assert_eq!(*new_active_id, 9);
            }
            other => panic!("expected RefreshDlmm latest, got {other:?}"),
        }
    }

    #[test]
    fn sequencer_rejects_stale_apply_after_newer() {
        let revisions = PoolSnapshotRevisionSequencer::with_max_keys(8);
        let pool = Pubkey::new_unique();
        assert_eq!(
            revisions.try_acquire_active(pool, ConsumerId::Momentum),
            RevisionAcquireResult::Acquired
        );
        let mut newer = mk_snapshot(pool, Pubkey::new_unique());
        let mut older = mk_snapshot(pool, Pubkey::new_unique());
        let rev_new = match revisions.assign_next(&mut newer) {
            RevisionAssignResult::Assigned(rev) => rev,
            other => panic!("{other:?}"),
        };
        revisions.inc_inflight_ref(pool, ConsumerId::Momentum);
        let rev_latest = match revisions.assign_next(&mut older) {
            RevisionAssignResult::Assigned(rev) => rev,
            other => panic!("{other:?}"),
        };
        revisions.inc_inflight_ref(pool, ConsumerId::Momentum);
        assert!(revisions.should_apply_and_record(&older));
        assert!(!revisions.should_apply_and_record(&newer));
        assert_eq!(
            revisions.current_applied(pool, ConsumerId::Momentum),
            rev_latest
        );
        assert!(rev_new < rev_latest);
    }

    #[test]
    fn revision_registry_bounded_after_churn_beyond_cap() {
        let revisions = PoolSnapshotRevisionSequencer::with_max_keys(8);
        for _ in 0..128 {
            let pool = Pubkey::new_unique();
            let revision = register_and_assign(&revisions, pool, ConsumerId::Momentum);
            revisions.inc_inflight_ref(pool, ConsumerId::Momentum);
            let mut snapshot = mk_snapshot(pool, Pubkey::new_unique());
            snapshot.revision = revision;
            assert!(revisions.should_apply_and_record(&snapshot));
            revisions.release_active_if_idle(pool, ConsumerId::Momentum);
        }
        assert!(
            revisions.active_key_count() <= 8,
            "revision registry must stay bounded"
        );
        assert_eq!(revisions.total_memory_slots(), revisions.active_key_count());
    }

    #[test]
    fn active_keys_never_evicted_above_cap() {
        let revisions = PoolSnapshotRevisionSequencer::with_max_keys(4);
        let pools: Vec<Pubkey> = (0..6).map(|_| Pubkey::new_unique()).collect();
        for pool in &pools[..4] {
            assert_eq!(
                revisions.try_acquire_active(*pool, ConsumerId::Momentum),
                RevisionAcquireResult::Acquired
            );
        }
        for pool in &pools[..4] {
            let _ = register_and_assign(&revisions, *pool, ConsumerId::Momentum);
        }
        assert_eq!(revisions.active_key_count(), 4);
        assert_eq!(
            revisions.try_acquire_active(pools[4], ConsumerId::Momentum),
            RevisionAcquireResult::RegistryFull,
            "must fail closed when all slots are active/in-flight"
        );
        let refs = revisions.key_refs(pools[0], ConsumerId::Momentum);
        assert!(refs.active > 0);
        assert_eq!(
            revisions.current_issued(pools[0], ConsumerId::Momentum),
            revisions.current_issued(pools[0], ConsumerId::Momentum)
        );
    }

    #[test]
    fn retired_lifecycle_rejects_delayed_stale_command_after_repin() {
        let revisions = PoolSnapshotRevisionSequencer::with_max_keys(8);
        let pool = Pubkey::new_unique();
        assert_eq!(
            revisions.try_acquire_active(pool, ConsumerId::Momentum),
            RevisionAcquireResult::Acquired
        );
        let mut stale = mk_snapshot(pool, Pubkey::new_unique());
        let stale_rev = match revisions.assign_next(&mut stale) {
            RevisionAssignResult::Assigned(rev) => rev,
            other => panic!("{other:?}"),
        };
        revisions.inc_inflight_ref(pool, ConsumerId::Momentum);
        assert!(revisions.should_apply_and_record(&stale));
        revisions.release_active_if_idle(pool, ConsumerId::Momentum);

        assert_eq!(
            revisions.try_acquire_active(pool, ConsumerId::Momentum),
            RevisionAcquireResult::Acquired
        );
        let mut fresh = mk_snapshot(pool, Pubkey::new_unique());
        let fresh_rev = match revisions.assign_next(&mut fresh) {
            RevisionAssignResult::Assigned(rev) => rev,
            other => panic!("{other:?}"),
        };
        assert!(
            PoolSnapshotRevisionSequencer::revision_lifecycle_id(fresh_rev)
                > PoolSnapshotRevisionSequencer::revision_lifecycle_id(stale_rev)
        );
        revisions.inc_inflight_ref(pool, ConsumerId::Momentum);
        assert!(!revisions.should_apply_and_record(&stale));
        assert!(revisions.should_apply_and_record(&fresh));
        assert_eq!(
            revisions.current_applied(pool, ConsumerId::Momentum),
            fresh_rev
        );
    }

    #[test]
    fn long_churn_memory_and_stale_rejection_bounded() {
        let revisions = PoolSnapshotRevisionSequencer::with_max_keys(16);
        let mut issued: Vec<(Pubkey, u64)> = Vec::new();
        for _ in 0..256 {
            let pool = Pubkey::new_unique();
            let rev = register_and_assign(&revisions, pool, ConsumerId::Momentum);
            revisions.inc_inflight_ref(pool, ConsumerId::Momentum);
            let mut snapshot = mk_snapshot(pool, Pubkey::new_unique());
            snapshot.revision = rev;
            assert!(revisions.should_apply_and_record(&snapshot));
            issued.push((pool, rev));
            revisions.release_active_if_idle(pool, ConsumerId::Momentum);
        }
        assert!(revisions.active_key_count() <= 16);
        for (pool, stale_rev) in issued.into_iter().take(32) {
            let mut stale = mk_snapshot(pool, Pubkey::new_unique());
            stale.revision = stale_rev;
            assert!(!revisions.should_apply_and_record(&stale));
        }
    }
}
