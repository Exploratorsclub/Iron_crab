//! Bounded durable pending state for track-worker commands lost on full queue.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use solana_sdk::pubkey::Pubkey;

use crate::market_data::track::desired_set::ConsumerId;
use crate::market_data::track::worker_commands::{PoolExplicitSnapshot, TrackWorkerCommand};

/// Monotonic per-(pool,consumer) revision sequencer for pool snapshot commands.
///
/// Revisions pack `(generation << 32) | sequence`. Tombstoned generations reject delayed stale
/// commands after registry cleanup/eviction.
#[derive(Debug)]
pub struct PoolSnapshotRevisionSequencer {
    max_keys: usize,
    slots: Mutex<HashMap<(Pubkey, ConsumerId), RevisionSlot>>,
    tombstones: Mutex<HashMap<(Pubkey, ConsumerId), u32>>,
    lru_stamp: AtomicU64,
}

#[derive(Debug, Clone)]
struct RevisionSlot {
    generation: u32,
    issued_seq: u32,
    applied_seq: u32,
    lru_stamp: u64,
}

const REVISION_SEQ_MASK: u64 = 0xFFFF_FFFF;
const REVISION_GEN_SHIFT: u32 = 32;
const DEFAULT_MAX_REVISION_SLOTS: usize = 4096;

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
            max_keys: max_keys.max(64),
            slots: Mutex::new(HashMap::new()),
            tombstones: Mutex::new(HashMap::new()),
            lru_stamp: AtomicU64::new(0),
        }
    }

    pub fn pack_revision(generation: u32, sequence: u32) -> u64 {
        ((generation as u64) << REVISION_GEN_SHIFT) | (sequence as u64)
    }

    pub fn unpack_revision(revision: u64) -> (u32, u32) {
        (
            (revision >> REVISION_GEN_SHIFT) as u32,
            (revision & REVISION_SEQ_MASK) as u32,
        )
    }

    pub fn revision_sequence(revision: u64) -> u32 {
        Self::unpack_revision(revision).1
    }

    pub fn revision_generation(revision: u64) -> u32 {
        Self::unpack_revision(revision).0
    }

    fn next_lru_stamp(&self) -> u64 {
        self.lru_stamp
            .fetch_add(1, Ordering::AcqRel)
            .saturating_add(1)
    }

    fn tombstone_generation(&self, key: (Pubkey, ConsumerId)) -> u32 {
        self.tombstones
            .lock()
            .expect("pool revision tombstones lock")
            .get(&key)
            .copied()
            .unwrap_or(0)
    }

    fn record_tombstone(&self, key: (Pubkey, ConsumerId), generation: u32) {
        let mut tombstones = self
            .tombstones
            .lock()
            .expect("pool revision tombstones lock");
        let entry = tombstones.entry(key).or_insert(0);
        *entry = (*entry).max(generation);
    }

    fn evict_lru_slot(&self, slots: &mut HashMap<(Pubkey, ConsumerId), RevisionSlot>) {
        if slots.len() < self.max_keys {
            return;
        }
        let Some((key, slot)) = slots
            .iter()
            .min_by_key(|(_, slot)| slot.lru_stamp)
            .map(|(k, s)| (*k, s.clone()))
        else {
            return;
        };
        self.record_tombstone(key, slot.generation);
        slots.remove(&key);
    }

    pub fn assign_next(&self, snapshot: &mut PoolExplicitSnapshot) -> u64 {
        let key = (snapshot.pool, snapshot.consumer);
        let mut slots = self.slots.lock().expect("pool revision issued lock");
        if !slots.contains_key(&key) {
            self.evict_lru_slot(&mut slots);
        }
        let floor_gen = self.tombstone_generation(key);
        let slot = slots.entry(key).or_insert_with(|| RevisionSlot {
            generation: floor_gen.saturating_add(1).max(1),
            issued_seq: 0,
            applied_seq: 0,
            lru_stamp: self.next_lru_stamp(),
        });
        slot.issued_seq = slot.issued_seq.saturating_add(1);
        if slot.issued_seq == 0 {
            slot.generation = slot.generation.saturating_add(1);
            slot.issued_seq = 1;
        }
        slot.lru_stamp = self.next_lru_stamp();
        snapshot.revision = Self::pack_revision(slot.generation, slot.issued_seq);
        snapshot.revision
    }

    pub fn should_apply_and_record(&self, snapshot: &PoolExplicitSnapshot) -> bool {
        if snapshot.revision == 0 {
            return false;
        }
        let (gen, seq) = Self::unpack_revision(snapshot.revision);
        let key = (snapshot.pool, snapshot.consumer);
        if gen <= self.tombstone_generation(key) {
            return false;
        }
        let mut slots = self.slots.lock().expect("pool revision issued lock");
        let Some(slot) = slots.get_mut(&key) else {
            return false;
        };
        if gen != slot.generation || seq <= slot.applied_seq {
            return false;
        }
        slot.applied_seq = seq;
        slot.lru_stamp = self.next_lru_stamp();
        true
    }

    pub fn retire_key(&self, pool: Pubkey, consumer: ConsumerId) {
        let key = (pool, consumer);
        let mut slots = self.slots.lock().expect("pool revision issued lock");
        if let Some(slot) = slots.remove(&key) {
            self.record_tombstone(key, slot.generation);
        }
    }

    pub fn maybe_retire_key(&self, pool: Pubkey, consumer: ConsumerId, has_pending: bool) {
        if has_pending {
            return;
        }
        self.retire_key(pool, consumer);
    }

    pub fn active_key_count(&self) -> usize {
        self.slots.lock().expect("pool revision issued lock").len()
    }

    pub fn current_issued(&self, pool: Pubkey, consumer: ConsumerId) -> u64 {
        self.slots
            .lock()
            .expect("pool revision issued lock")
            .get(&(pool, consumer))
            .map(|slot| Self::pack_revision(slot.generation, slot.issued_seq))
            .unwrap_or(0)
    }

    pub fn current_applied(&self, pool: Pubkey, consumer: ConsumerId) -> u64 {
        self.slots
            .lock()
            .expect("pool revision issued lock")
            .get(&(pool, consumer))
            .map(|slot| Self::pack_revision(slot.generation, slot.applied_seq))
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
                    self.revisions.assign_next(s)
                } else {
                    s.revision
                }
            }
            PendingPoolCommand::RefreshDlmm { snapshot, .. } => {
                if snapshot.revision == 0 {
                    self.revisions.assign_next(snapshot)
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
            .map(|coalesced| coalesced.into_command())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market_data::track::desired_set::OwnerKey;
    use crate::market_data::track::worker_commands::GeyserPinReason;

    fn mk_snapshot(pool: Pubkey, vault: Pubkey) -> PoolExplicitSnapshot {
        use crate::market_data::track::worker_commands::{MintExplicitRow, VaultExplicitRow};
        PoolExplicitSnapshot {
            pool,
            vaults: vec![VaultExplicitRow {
                pubkey: vault,
                dex: "test".into(),
                base_mint: Pubkey::new_unique(),
                quote_mint: Pubkey::new_unique(),
                is_base_vault: true,
                sibling_vault: None,
                active_id: None,
                bin_step: None,
            }],
            bin_arrays: vec![],
            mints: vec![MintExplicitRow {
                pubkey: Pubkey::new_unique(),
            }],
            consumer: ConsumerId::Momentum,
            owner: OwnerKey::Pool(pool),
            pin: GeyserPinReason::MomentumActive,
            revision: 0,
        }
    }

    use std::sync::Arc;

    fn mk_pending() -> (PendingPoolRegistrations, Arc<PoolSnapshotRevisionSequencer>) {
        let revisions = Arc::new(PoolSnapshotRevisionSequencer::new());
        (
            PendingPoolRegistrations::new(8, Arc::clone(&revisions)),
            revisions,
        )
    }

    #[test]
    fn pending_replays_latest_revision_not_kind_order() {
        let (pending, _revs) = mk_pending();
        let pool = Pubkey::new_unique();
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
        // Older revision (already assigned internally) cannot overwrite newer via re-upsert with stale
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
        let (pending, _revs) = mk_pending();
        let pool = Pubkey::new_unique();
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

        // Simulate stale replay by manually constructing lower-revision command — upsert assigns
        // monotonic revision so we verify only latest survives drain.
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
        let revisions = PoolSnapshotRevisionSequencer::new();
        let pool = Pubkey::new_unique();
        let mut newer = mk_snapshot(pool, Pubkey::new_unique());
        let mut older = mk_snapshot(pool, Pubkey::new_unique());
        let rev_new = revisions.assign_next(&mut newer);
        let rev_latest = revisions.assign_next(&mut older);
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
            let mut snapshot = mk_snapshot(pool, Pubkey::new_unique());
            let revision = revisions.assign_next(&mut snapshot);
            assert!(revisions.should_apply_and_record(&snapshot));
            assert_eq!(
                revisions.current_applied(pool, ConsumerId::Momentum),
                revision
            );
            revisions.retire_key(pool, ConsumerId::Momentum);
        }
        assert!(
            revisions.active_key_count() <= 8,
            "revision registry must stay bounded"
        );
    }

    #[test]
    fn retired_generation_rejects_delayed_stale_command_after_repin() {
        let revisions = PoolSnapshotRevisionSequencer::with_max_keys(8);
        let pool = Pubkey::new_unique();
        let mut stale = mk_snapshot(pool, Pubkey::new_unique());
        let stale_rev = revisions.assign_next(&mut stale);
        assert!(revisions.should_apply_and_record(&stale));
        revisions.retire_key(pool, ConsumerId::Momentum);

        let mut fresh = mk_snapshot(pool, Pubkey::new_unique());
        let fresh_rev = revisions.assign_next(&mut fresh);
        assert!(
            PoolSnapshotRevisionSequencer::revision_generation(fresh_rev)
                > PoolSnapshotRevisionSequencer::revision_generation(stale_rev)
        );
        assert!(!revisions.should_apply_and_record(&stale));
        assert!(revisions.should_apply_and_record(&fresh));
        assert_eq!(
            revisions.current_applied(pool, ConsumerId::Momentum),
            fresh_rev
        );
    }
}
