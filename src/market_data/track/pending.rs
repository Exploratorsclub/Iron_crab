//! Bounded durable pending state for track-worker commands lost on full queue.

use std::collections::{HashMap, HashSet, VecDeque};
#[cfg(test)]
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use solana_sdk::pubkey::Pubkey;

use crate::market_data::track::desired_set::ConsumerId;
use crate::market_data::track::worker_commands::{PoolExplicitSnapshot, TrackWorkerCommand};

const DEFAULT_MAX_REVISION_SLOTS: usize = 4096;

/// Idempotent logical owner identity for revision-registry active slots.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RevisionActiveOwner {
    MomentumMintPool { mint: Pubkey, pool: Pubkey },
    ArbPool { pool: Pubkey },
}

impl RevisionActiveOwner {
    pub fn pool(self) -> Pubkey {
        match self {
            Self::MomentumMintPool { pool, .. } | Self::ArbPool { pool } => pool,
        }
    }

    pub fn consumer(self) -> ConsumerId {
        match self {
            Self::MomentumMintPool { .. } => ConsumerId::Momentum,
            Self::ArbPool { .. } => ConsumerId::Arb,
        }
    }

    pub fn registry_key(self) -> (Pubkey, ConsumerId) {
        (self.pool(), self.consumer())
    }
}

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

/// Result of reserving an in-flight command slot (ephemeral; no active owner).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InflightReserveResult {
    Reserved,
    RegistryFull,
}

/// Which refcount to release when finishing a pool command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolCommandRefRelease {
    Inflight,
    Pending,
}

/// Terminal outcome for a pool snapshot command after worker processing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolCommandTerminal {
    Applied,
    StaleRevision,
    UnpinnedRejected,
    AdmissionRejected,
    InvalidRevision,
}

/// Phase returned by [`PoolSnapshotRevisionSequencer::begin_pool_command`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolCommandAcceptPhase {
    Stale,
    Ready,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct RevisionRefCounts {
    pub pending: u32,
    pub inflight: u32,
}

impl RevisionRefCounts {
    pub fn total(self) -> u32 {
        self.pending.saturating_add(self.inflight)
    }
}

#[derive(Debug, Clone)]
struct RevisionSlot {
    last_issued: u64,
    applied_revision: u64,
    pending: u32,
    inflight: u32,
    touch_stamp: u64,
}

impl RevisionSlot {
    fn recyclable(&self) -> bool {
        self.pending == 0 && self.inflight == 0
    }

    fn empty_slot(&self) -> bool {
        self.recyclable() && self.applied_revision == 0
    }
}

/// Coalesced pool command awaiting worker replay after queue loss.
#[derive(Debug, Clone)]
struct CoalescedPoolPending {
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

#[derive(Debug)]
struct RevisionRegistryInner {
    max_keys: usize,
    max_pending_pools: usize,
    slots: HashMap<(Pubkey, ConsumerId), RevisionSlot>,
    pending_entries: HashMap<(Pubkey, ConsumerId), CoalescedPoolPending>,
    pending_order: VecDeque<(Pubkey, ConsumerId)>,
    pending_overflow: bool,
}

impl RevisionRegistryInner {
    fn new(max_keys: usize) -> Self {
        Self {
            max_keys: max_keys.max(1),
            max_pending_pools: 1,
            slots: HashMap::new(),
            pending_entries: HashMap::new(),
            pending_order: VecDeque::new(),
            pending_overflow: false,
        }
    }

    fn maybe_remove_slot(&mut self, key: (Pubkey, ConsumerId)) {
        if self.slots.get(&key).is_some_and(|slot| slot.empty_slot()) {
            self.slots.remove(&key);
        }
    }
}

/// Bounded per-(pool,consumer) revision registry with refcounted lifecycle.
///
/// Revision slots and durable pending entries share one lock so pending refs and
/// visible entries cannot diverge.
#[derive(Debug)]
pub struct PoolSnapshotRevisionSequencer {
    inner: Mutex<RevisionRegistryInner>,
    touch_seq: AtomicU64,
    #[cfg(test)]
    stash_hold_before_visible: AtomicBool,
    #[cfg(test)]
    drain_hold_before_remove: AtomicBool,
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
            inner: Mutex::new(RevisionRegistryInner::new(max_keys)),
            touch_seq: AtomicU64::new(0),
            #[cfg(test)]
            stash_hold_before_visible: AtomicBool::new(false),
            #[cfg(test)]
            drain_hold_before_remove: AtomicBool::new(false),
        }
    }

    pub(crate) fn set_max_pending_pools(&self, max_pools: usize) {
        self.inner
            .lock()
            .expect("revision registry lock")
            .max_pending_pools = max_pools.max(1);
    }

    pub fn revision_newer_than_applied(revision: u64, applied_revision: u64) -> bool {
        revision != 0 && revision > applied_revision
    }

    pub fn max_keys(&self) -> usize {
        self.inner.lock().expect("revision registry lock").max_keys
    }

    pub fn active_key_count(&self) -> usize {
        self.inner
            .lock()
            .expect("revision registry lock")
            .slots
            .len()
    }

    pub fn total_memory_slots(&self) -> usize {
        self.active_key_count()
    }

    fn next_touch_stamp(&self) -> u64 {
        self.touch_seq
            .fetch_add(1, Ordering::AcqRel)
            .saturating_add(1)
    }

    fn new_slot(&self) -> RevisionSlot {
        RevisionSlot {
            last_issued: 0,
            applied_revision: 0,
            pending: 0,
            inflight: 0,
            touch_stamp: self.next_touch_stamp(),
        }
    }

    fn ensure_slot_locked(
        &self,
        inner: &mut RevisionRegistryInner,
        key: (Pubkey, ConsumerId),
    ) -> Result<(), RevisionAcquireResult> {
        if inner.slots.contains_key(&key) {
            return Ok(());
        }
        if inner.slots.len() < inner.max_keys {
            inner.slots.insert(key, self.new_slot());
            return Ok(());
        }
        let recyclable = inner
            .slots
            .iter()
            .filter(|(_, slot)| slot.recyclable())
            .min_by_key(|(_, slot)| slot.touch_stamp)
            .map(|(k, _)| *k);
        let Some(victim_key) = recyclable else {
            return Err(RevisionAcquireResult::RegistryFull);
        };
        inner.slots.remove(&victim_key);
        inner.slots.insert(key, self.new_slot());
        Ok(())
    }

    /// Ensure a revision-registry key exists (ownership lives in bounded pin maps).
    pub fn ensure_revision_key(&self, pool: Pubkey, consumer: ConsumerId) -> RevisionAcquireResult {
        let mut inner = self.inner.lock().expect("revision registry lock");
        self.ensure_slot_locked(&mut inner, (pool, consumer))
            .map(|()| RevisionAcquireResult::Acquired)
            .unwrap_or(RevisionAcquireResult::RegistryFull)
    }

    #[deprecated(note = "ownership is tracked in hot_pool_registry pin maps")]
    pub fn acquire_active_owner(&self, owner: RevisionActiveOwner) -> RevisionAcquireResult {
        self.ensure_revision_key(owner.pool(), owner.consumer())
    }

    #[deprecated(note = "ownership is tracked in hot_pool_registry pin maps")]
    pub fn release_active_owner(&self, _owner: RevisionActiveOwner) -> bool {
        false
    }

    #[deprecated(note = "use ensure_revision_key")]
    pub fn try_acquire_active(&self, pool: Pubkey, consumer: ConsumerId) -> RevisionAcquireResult {
        self.ensure_revision_key(pool, consumer)
    }

    #[deprecated(note = "ownership is tracked in hot_pool_registry pin maps")]
    pub fn release_active_if_idle(&self, _pool: Pubkey, _consumer: ConsumerId) -> bool {
        false
    }

    pub fn inc_pending_ref(&self, pool: Pubkey, consumer: ConsumerId) {
        let key = (pool, consumer);
        let mut inner = self.inner.lock().expect("revision registry lock");
        if let Some(slot) = inner.slots.get_mut(&key) {
            slot.pending = slot.pending.saturating_add(1);
            slot.touch_stamp = self.next_touch_stamp();
        }
    }

    pub fn dec_pending_ref(&self, pool: Pubkey, consumer: ConsumerId) {
        let key = (pool, consumer);
        let mut inner = self.inner.lock().expect("revision registry lock");
        let Some(slot) = inner.slots.get_mut(&key) else {
            return;
        };
        slot.pending = slot.pending.saturating_sub(1);
    }

    /// Reserve one in-flight command ref before queue send (ephemeral; no active owner).
    pub fn reserve_inflight_command(
        &self,
        pool: Pubkey,
        consumer: ConsumerId,
    ) -> InflightReserveResult {
        let key = (pool, consumer);
        let mut inner = self.inner.lock().expect("revision registry lock");
        if let Err(RevisionAcquireResult::RegistryFull) = self.ensure_slot_locked(&mut inner, key) {
            return InflightReserveResult::RegistryFull;
        }
        let Some(slot) = inner.slots.get_mut(&key) else {
            return InflightReserveResult::RegistryFull;
        };
        slot.inflight = slot.inflight.saturating_add(1);
        slot.touch_stamp = self.next_touch_stamp();
        InflightReserveResult::Reserved
    }

    #[deprecated(note = "use reserve_inflight_command")]
    pub fn inc_inflight_ref(&self, pool: Pubkey, consumer: ConsumerId) {
        let _ = self.reserve_inflight_command(pool, consumer);
    }

    /// Atomically move one in-flight ref to pending (enqueue loss path).
    pub fn transfer_inflight_to_pending(&self, pool: Pubkey, consumer: ConsumerId) -> bool {
        let key = (pool, consumer);
        let mut inner = self.inner.lock().expect("revision registry lock");
        let Some(slot) = inner.slots.get_mut(&key) else {
            return false;
        };
        if slot.inflight == 0 {
            return false;
        }
        slot.inflight = slot.inflight.saturating_sub(1);
        slot.pending = slot.pending.saturating_add(1);
        slot.touch_stamp = self.next_touch_stamp();
        true
    }

    pub fn release_inflight_command(&self, pool: Pubkey, consumer: ConsumerId) {
        let key = (pool, consumer);
        let mut inner = self.inner.lock().expect("revision registry lock");
        let Some(slot) = inner.slots.get_mut(&key) else {
            return;
        };
        slot.inflight = slot.inflight.saturating_sub(1);
    }

    #[deprecated(note = "use release_inflight_command")]
    pub fn dec_inflight_ref(&self, pool: Pubkey, consumer: ConsumerId) {
        self.release_inflight_command(pool, consumer);
    }

    pub fn key_refs(&self, pool: Pubkey, consumer: ConsumerId) -> RevisionRefCounts {
        self.inner
            .lock()
            .expect("revision registry lock")
            .slots
            .get(&(pool, consumer))
            .map(|slot| RevisionRefCounts {
                pending: slot.pending,
                inflight: slot.inflight,
            })
            .unwrap_or_default()
    }

    #[deprecated(note = "ownership is tracked in hot_pool_registry pin maps")]
    pub fn has_active_owner(&self, _owner: RevisionActiveOwner) -> bool {
        false
    }

    pub fn assign_next(&self, snapshot: &mut PoolExplicitSnapshot) -> RevisionAssignResult {
        let key = (snapshot.pool, snapshot.consumer);
        let mut inner = self.inner.lock().expect("revision registry lock");
        let Some(slot) = inner.slots.get_mut(&key) else {
            return RevisionAssignResult::KeyNotRegistered;
        };
        let Some(next) = slot.last_issued.checked_add(1) else {
            return RevisionAssignResult::RegistryFull;
        };
        if next == 0 {
            return RevisionAssignResult::RegistryFull;
        }
        slot.last_issued = next;
        slot.touch_stamp = self.next_touch_stamp();
        snapshot.revision = next;
        RevisionAssignResult::Assigned(next)
    }

    pub fn revision_acceptable(&self, snapshot: &PoolExplicitSnapshot) -> bool {
        matches!(
            self.begin_pool_command(snapshot),
            PoolCommandAcceptPhase::Ready
        )
    }

    /// Begin processing under the registry lock — stale commands never advance watermark.
    pub fn begin_pool_command(&self, snapshot: &PoolExplicitSnapshot) -> PoolCommandAcceptPhase {
        if snapshot.revision == 0 {
            return PoolCommandAcceptPhase::Stale;
        }
        let ready = self
            .inner
            .lock()
            .expect("revision registry lock")
            .slots
            .get(&(snapshot.pool, snapshot.consumer))
            .is_some_and(|slot| {
                Self::revision_newer_than_applied(snapshot.revision, slot.applied_revision)
            });
        if ready {
            PoolCommandAcceptPhase::Ready
        } else {
            PoolCommandAcceptPhase::Stale
        }
    }

    /// Finish a pool command — releases exactly one ref when requested and may
    /// advance the authoritative applied-revision watermark after demand validation.
    pub fn finish_pool_command(
        &self,
        snapshot: &PoolExplicitSnapshot,
        terminal: PoolCommandTerminal,
        release_ref: Option<PoolCommandRefRelease>,
        record_watermark: bool,
    ) {
        if snapshot.revision == 0 {
            if let Some(release) = release_ref {
                match release {
                    PoolCommandRefRelease::Inflight => {
                        self.release_inflight_command(snapshot.pool, snapshot.consumer);
                    }
                    PoolCommandRefRelease::Pending => {
                        self.dec_pending_ref(snapshot.pool, snapshot.consumer);
                    }
                }
            }
            return;
        }
        let key = (snapshot.pool, snapshot.consumer);
        let mut inner = self.inner.lock().expect("revision registry lock");
        let Some(slot) = inner.slots.get_mut(&key) else {
            drop(inner);
            if let Some(release) = release_ref {
                match release {
                    PoolCommandRefRelease::Inflight => {
                        self.release_inflight_command(snapshot.pool, snapshot.consumer);
                    }
                    PoolCommandRefRelease::Pending => {
                        self.dec_pending_ref(snapshot.pool, snapshot.consumer);
                    }
                }
            }
            return;
        };
        if record_watermark
            && matches!(
                terminal,
                PoolCommandTerminal::Applied
                    | PoolCommandTerminal::AdmissionRejected
                    | PoolCommandTerminal::UnpinnedRejected
            )
            && Self::revision_newer_than_applied(snapshot.revision, slot.applied_revision)
        {
            slot.applied_revision = snapshot.revision;
        }
        if let Some(release) = release_ref {
            match release {
                PoolCommandRefRelease::Inflight => {
                    slot.inflight = slot.inflight.saturating_sub(1);
                }
                PoolCommandRefRelease::Pending => {
                    slot.pending = slot.pending.saturating_sub(1);
                }
            }
        }
        slot.touch_stamp = self.next_touch_stamp();
        inner.maybe_remove_slot(key);
    }

    #[deprecated(note = "use begin_pool_command + finish_pool_command")]
    pub fn should_apply_and_record(&self, snapshot: &PoolExplicitSnapshot) -> bool {
        let phase = self.begin_pool_command(snapshot);
        let terminal = if phase == PoolCommandAcceptPhase::Ready {
            PoolCommandTerminal::Applied
        } else {
            PoolCommandTerminal::StaleRevision
        };
        self.finish_pool_command(
            snapshot,
            terminal,
            Some(PoolCommandRefRelease::Inflight),
            phase == PoolCommandAcceptPhase::Ready,
        );
        phase == PoolCommandAcceptPhase::Ready
    }

    pub fn retire_key(&self, pool: Pubkey, consumer: ConsumerId) {
        let key = (pool, consumer);
        let mut inner = self.inner.lock().expect("revision registry lock");
        let Some(slot) = inner.slots.get(&key) else {
            return;
        };
        if slot.inflight > 0 || slot.pending > 0 {
            return;
        }
        inner.slots.remove(&key);
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
        self.inner
            .lock()
            .expect("revision registry lock")
            .slots
            .get(&(pool, consumer))
            .map(|slot| slot.last_issued)
            .unwrap_or(0)
    }

    pub fn current_applied(&self, pool: Pubkey, consumer: ConsumerId) -> u64 {
        self.inner
            .lock()
            .expect("revision registry lock")
            .slots
            .get(&(pool, consumer))
            .map(|slot| slot.applied_revision)
            .unwrap_or(0)
    }

    #[cfg(test)]
    pub fn test_seed_slot_revision_state(
        &self,
        pool: Pubkey,
        consumer: ConsumerId,
        last_issued: u64,
        applied_revision: u64,
    ) {
        let key = (pool, consumer);
        let mut inner = self.inner.lock().expect("revision registry lock");
        if let Err(RevisionAcquireResult::RegistryFull) = self.ensure_slot_locked(&mut inner, key) {
            panic!("revision slot seed failed: registry full");
        }
        let slot = inner.slots.get_mut(&key).expect("seeded revision slot");
        slot.last_issued = last_issued;
        slot.applied_revision = applied_revision;
    }

    #[cfg(test)]
    pub fn test_set_stash_hold_before_visible(&self, hold: bool) {
        self.stash_hold_before_visible
            .store(hold, Ordering::Release);
    }

    #[cfg(test)]
    pub fn test_set_drain_hold_before_remove(&self, hold: bool) {
        self.drain_hold_before_remove.store(hold, Ordering::Release);
    }

    #[cfg(test)]
    fn test_stash_hold_active(&self) -> bool {
        self.stash_hold_before_visible.load(Ordering::Acquire)
    }

    #[cfg(test)]
    fn test_drain_hold_active(&self) -> bool {
        self.drain_hold_before_remove.load(Ordering::Acquire)
    }

    fn assign_revision_for_command(
        &self,
        inner: &mut RevisionRegistryInner,
        command: &mut PendingPoolCommand,
    ) -> Result<u64, PendingPoolUpsertResult> {
        let snapshot = match command {
            PendingPoolCommand::RegisterReserves(s)
            | PendingPoolCommand::VaultsFromAccount(s)
            | PendingPoolCommand::AfterTrade(s) => s,
            PendingPoolCommand::RefreshDlmm { snapshot, .. } => snapshot,
        };
        if snapshot.revision != 0 {
            return Ok(snapshot.revision);
        }
        let key = (snapshot.pool, snapshot.consumer);
        if let Err(RevisionAcquireResult::RegistryFull) = self.ensure_slot_locked(inner, key) {
            inner.pending_overflow = true;
            return Err(PendingPoolUpsertResult::Overflow);
        }
        let Some(slot) = inner.slots.get_mut(&key) else {
            inner.pending_overflow = true;
            return Err(PendingPoolUpsertResult::Overflow);
        };
        let Some(next) = slot.last_issued.checked_add(1) else {
            inner.pending_overflow = true;
            return Err(PendingPoolUpsertResult::Overflow);
        };
        if next == 0 {
            inner.pending_overflow = true;
            return Err(PendingPoolUpsertResult::Overflow);
        }
        slot.last_issued = next;
        slot.touch_stamp = self.next_touch_stamp();
        snapshot.revision = next;
        Ok(next)
    }

    fn stash_hold_spin(&self) {
        #[cfg(test)]
        {
            while self.stash_hold_before_visible.load(Ordering::Acquire) {
                std::thread::yield_now();
            }
        }
    }

    fn drain_hold_spin(&self) {
        #[cfg(test)]
        {
            while self.drain_hold_before_remove.load(Ordering::Acquire) {
                std::thread::yield_now();
            }
        }
    }

    pub(crate) fn pending_upsert(
        &self,
        mut command: PendingPoolCommand,
    ) -> PendingPoolUpsertResult {
        let mut inner = self.inner.lock().expect("revision registry lock");
        let revision = match self.assign_revision_for_command(&mut inner, &mut command) {
            Ok(rev) => rev,
            Err(result) => return result,
        };
        let key = (command.pool(), command.consumer());
        if let Some(entry) = inner.pending_entries.get_mut(&key) {
            if entry.try_merge(revision, command) {
                return PendingPoolUpsertResult::Coalesced;
            }
            return PendingPoolUpsertResult::StaleNoOp;
        }
        if inner.pending_order.len() >= inner.max_pending_pools {
            inner.pending_overflow = true;
            return PendingPoolUpsertResult::Overflow;
        }

        if let Some(slot) = inner.slots.get_mut(&key) {
            slot.pending = slot.pending.saturating_add(1);
            slot.touch_stamp = self.next_touch_stamp();
        }

        let coalesced = CoalescedPoolPending {
            latest_revision: revision,
            latest_command: command,
        };
        inner.pending_entries.insert(key, coalesced);
        inner.pending_order.push_back(key);
        PendingPoolUpsertResult::Stored
    }

    pub(crate) fn pending_upsert_after_inflight_send_failure(
        &self,
        pool: Pubkey,
        consumer: ConsumerId,
        command: PendingPoolCommand,
    ) -> PendingPoolUpsertResult {
        let mut inner = self.inner.lock().expect("revision registry lock");
        let mut command = command;
        let revision = match self.assign_revision_for_command(&mut inner, &mut command) {
            Ok(rev) => rev,
            Err(result) => {
                if let Some(slot) = inner.slots.get_mut(&(pool, consumer)) {
                    slot.inflight = slot.inflight.saturating_sub(1);
                }
                return result;
            }
        };
        let key = (pool, consumer);
        if let Some(entry) = inner.pending_entries.get_mut(&key) {
            if entry.try_merge(revision, command) {
                if let Some(slot) = inner.slots.get_mut(&key) {
                    slot.inflight = slot.inflight.saturating_sub(1);
                }
                return PendingPoolUpsertResult::Coalesced;
            }
            if let Some(slot) = inner.slots.get_mut(&key) {
                slot.inflight = slot.inflight.saturating_sub(1);
            }
            return PendingPoolUpsertResult::StaleNoOp;
        }
        if inner.pending_order.len() >= inner.max_pending_pools {
            if let Some(slot) = inner.slots.get_mut(&key) {
                slot.inflight = slot.inflight.saturating_sub(1);
            }
            inner.pending_overflow = true;
            return PendingPoolUpsertResult::Overflow;
        }

        let Some(slot) = inner.slots.get_mut(&key) else {
            inner.pending_overflow = true;
            return PendingPoolUpsertResult::Overflow;
        };
        if slot.inflight == 0 {
            inner.pending_overflow = true;
            return PendingPoolUpsertResult::Overflow;
        }
        slot.inflight = slot.inflight.saturating_sub(1);
        slot.pending = slot.pending.saturating_add(1);
        slot.touch_stamp = self.next_touch_stamp();

        let coalesced = CoalescedPoolPending {
            latest_revision: revision,
            latest_command: command,
        };
        drop(inner);
        self.stash_hold_spin();
        let mut inner = self.inner.lock().expect("revision registry lock");
        inner.pending_entries.insert(key, coalesced);
        inner.pending_order.push_back(key);
        PendingPoolUpsertResult::Stored
    }

    pub(crate) fn pending_drain_all(&self) -> Vec<PendingPoolCommand> {
        let mut inner = self.inner.lock().expect("revision registry lock");
        let keys: Vec<_> = inner.pending_order.drain(..).collect();
        drop(inner);
        let mut drained = Vec::with_capacity(keys.len());
        for k in keys {
            self.drain_hold_spin();
            let mut inner = self.inner.lock().expect("revision registry lock");
            let Some(coalesced) = inner.pending_entries.remove(&k) else {
                continue;
            };
            if let Some(slot) = inner.slots.get_mut(&k) {
                slot.pending = slot.pending.saturating_sub(1);
            }
            drained.push(coalesced.into_command());
        }
        drained
    }

    pub(crate) fn pending_is_empty(&self) -> bool {
        self.inner
            .lock()
            .expect("revision registry lock")
            .pending_entries
            .is_empty()
    }

    pub(crate) fn pending_overflowed(&self) -> bool {
        self.inner
            .lock()
            .expect("revision registry lock")
            .pending_overflow
    }

    pub(crate) fn pending_clear_overflow(&self) {
        self.inner
            .lock()
            .expect("revision registry lock")
            .pending_overflow = false;
    }

    pub(crate) fn pending_pool_count(&self) -> usize {
        self.inner
            .lock()
            .expect("revision registry lock")
            .pending_entries
            .len()
    }

    pub(crate) fn pending_latest_revision_for(
        &self,
        pool: Pubkey,
        consumer: ConsumerId,
    ) -> Option<u64> {
        self.inner
            .lock()
            .expect("revision registry lock")
            .pending_entries
            .get(&(pool, consumer))
            .map(|e| e.latest_revision)
    }

    pub(crate) fn pending_has_entry(&self, pool: Pubkey, consumer: ConsumerId) -> bool {
        self.inner
            .lock()
            .expect("revision registry lock")
            .pending_entries
            .contains_key(&(pool, consumer))
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

    /// Monotonic wallet revision; returns `u64::MAX` fail-closed when exhausted.
    #[allow(clippy::manual_saturating_arithmetic)]
    fn bump_revision(&self) -> u64 {
        self.revision
            .fetch_add(1, Ordering::AcqRel)
            .checked_add(1)
            .unwrap_or(u64::MAX)
    }
}

/// Bounded per-pool coalesced pending (one entry per pool+consumer; overflow is fail-closed).
#[derive(Debug)]
pub struct PendingPoolRegistrations {
    revisions: std::sync::Arc<PoolSnapshotRevisionSequencer>,
}

impl PendingPoolRegistrations {
    pub fn new(max_pools: usize, revisions: std::sync::Arc<PoolSnapshotRevisionSequencer>) -> Self {
        revisions.set_max_pending_pools(max_pools);
        Self { revisions }
    }

    pub fn upsert(&self, command: PendingPoolCommand) -> PendingPoolUpsertResult {
        self.revisions.pending_upsert(command)
    }

    /// Transactional stash after queue send failure while holding one in-flight reservation.
    pub fn upsert_after_inflight_send_failure(
        &self,
        pool: Pubkey,
        consumer: ConsumerId,
        command: PendingPoolCommand,
    ) -> PendingPoolUpsertResult {
        self.revisions
            .pending_upsert_after_inflight_send_failure(pool, consumer, command)
    }

    #[deprecated(note = "use upsert_after_inflight_send_failure")]
    pub fn upsert_transferred(&self, command: PendingPoolCommand) -> PendingPoolUpsertResult {
        self.upsert(command)
    }

    pub fn drain_all(&self) -> Vec<PendingPoolCommand> {
        self.revisions.pending_drain_all()
    }

    pub fn is_empty(&self) -> bool {
        self.revisions.pending_is_empty()
    }

    pub fn overflowed(&self) -> bool {
        self.revisions.pending_overflowed()
    }

    pub fn clear_overflow(&self) {
        self.revisions.pending_clear_overflow();
    }

    pub fn pool_count(&self) -> usize {
        self.revisions.pending_pool_count()
    }

    pub fn latest_revision_for(&self, pool: Pubkey, consumer: ConsumerId) -> Option<u64> {
        self.revisions.pending_latest_revision_for(pool, consumer)
    }

    pub fn has_pending(&self, pool: Pubkey, consumer: ConsumerId) -> bool {
        self.revisions.pending_has_entry(pool, consumer)
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

    fn reserve_assign(
        revisions: &PoolSnapshotRevisionSequencer,
        pool: Pubkey,
        consumer: ConsumerId,
    ) -> u64 {
        assert_eq!(
            revisions.reserve_inflight_command(pool, consumer),
            InflightReserveResult::Reserved
        );
        let mut snapshot = mk_snapshot(pool, Pubkey::new_unique());
        match revisions.assign_next(&mut snapshot) {
            RevisionAssignResult::Assigned(rev) => rev,
            other => panic!("expected Assigned, got {other:?}"),
        }
    }

    fn ensure_key(revisions: &PoolSnapshotRevisionSequencer, pool: Pubkey, consumer: ConsumerId) {
        assert_eq!(
            revisions.ensure_revision_key(pool, consumer),
            RevisionAcquireResult::Acquired
        );
    }

    fn register_and_assign(
        revisions: &PoolSnapshotRevisionSequencer,
        pool: Pubkey,
        consumer: ConsumerId,
    ) -> u64 {
        ensure_key(revisions, pool, consumer);
        reserve_assign(revisions, pool, consumer)
    }

    fn assert_reserved_inflight(
        revisions: &PoolSnapshotRevisionSequencer,
        pool: Pubkey,
        consumer: ConsumerId,
    ) {
        assert_eq!(
            revisions.reserve_inflight_command(pool, consumer),
            InflightReserveResult::Reserved
        );
    }

    fn finish_inflight(
        revisions: &PoolSnapshotRevisionSequencer,
        snapshot: &PoolExplicitSnapshot,
        terminal: PoolCommandTerminal,
        record_watermark: bool,
    ) {
        revisions.finish_pool_command(
            snapshot,
            terminal,
            Some(PoolCommandRefRelease::Inflight),
            record_watermark,
        );
    }

    #[test]
    fn revision_key_ensure_is_idempotent() {
        let revisions = PoolSnapshotRevisionSequencer::with_max_keys(8);
        let pool = Pubkey::new_unique();
        ensure_key(&revisions, pool, ConsumerId::Momentum);
        ensure_key(&revisions, pool, ConsumerId::Momentum);
        assert_eq!(revisions.active_key_count(), 1);
        assert_eq!(revisions.key_refs(pool, ConsumerId::Momentum).total(), 0);
    }

    #[test]
    fn revision_slot_retires_when_refs_return_to_zero() {
        let revisions = PoolSnapshotRevisionSequencer::with_max_keys(8);
        let pool = Pubkey::new_unique();
        let rev = register_and_assign(&revisions, pool, ConsumerId::Momentum);
        let mut snapshot = mk_snapshot(pool, Pubkey::new_unique());
        snapshot.revision = rev;
        finish_inflight(&revisions, &snapshot, PoolCommandTerminal::Applied, true);
        assert_eq!(revisions.key_refs(pool, ConsumerId::Momentum).total(), 0);
        assert_eq!(revisions.current_applied(pool, ConsumerId::Momentum), rev);
    }

    #[test]
    fn stale_noop_releases_inflight_without_apply() {
        let revisions = PoolSnapshotRevisionSequencer::with_max_keys(8);
        let pool = Pubkey::new_unique();
        ensure_key(&revisions, pool, ConsumerId::Momentum);
        let rev = reserve_assign(&revisions, pool, ConsumerId::Momentum);
        let mut stale = mk_snapshot(pool, Pubkey::new_unique());
        stale.revision = rev;
        finish_inflight(&revisions, &stale, PoolCommandTerminal::Applied, true);
        let mut older = mk_snapshot(pool, Pubkey::new_unique());
        older.revision = rev.saturating_sub(1);
        assert!(!revisions.revision_acceptable(&older));
        assert_eq!(
            revisions.reserve_inflight_command(pool, ConsumerId::Momentum),
            InflightReserveResult::Reserved
        );
        older.revision = rev;
        finish_inflight(
            &revisions,
            &older,
            PoolCommandTerminal::StaleRevision,
            false,
        );
        assert_eq!(revisions.key_refs(pool, ConsumerId::Momentum).inflight, 0);
    }

    #[test]
    fn inflight_transfer_to_pending_no_zero_ref_gap() {
        let revisions = PoolSnapshotRevisionSequencer::with_max_keys(8);
        let pool = Pubkey::new_unique();
        ensure_key(&revisions, pool, ConsumerId::Momentum);
        let _rev = reserve_assign(&revisions, pool, ConsumerId::Momentum);
        let refs_before = revisions.key_refs(pool, ConsumerId::Momentum);
        assert_eq!(refs_before.inflight, 1);
        assert!(revisions.transfer_inflight_to_pending(pool, ConsumerId::Momentum));
        let refs_after = revisions.key_refs(pool, ConsumerId::Momentum);
        assert_eq!(refs_after.inflight, 0);
        assert_eq!(refs_after.pending, 1);
        assert_eq!(refs_after.total(), 1);
    }

    #[test]
    fn delayed_after_unpin_churn_keeps_registry_bounded() {
        let revisions = PoolSnapshotRevisionSequencer::with_max_keys(4);
        let pool = Pubkey::new_unique();
        ensure_key(&revisions, pool, ConsumerId::Momentum);
        let rev = reserve_assign(&revisions, pool, ConsumerId::Momentum);
        let mut snapshot = mk_snapshot(pool, Pubkey::new_unique());
        snapshot.revision = rev;
        finish_inflight(
            &revisions,
            &snapshot,
            PoolCommandTerminal::UnpinnedRejected,
            false,
        );
        assert_eq!(revisions.active_key_count(), 0);
        for _ in 0..64 {
            let ephemeral_pool = Pubkey::new_unique();
            let rev = reserve_assign(&revisions, ephemeral_pool, ConsumerId::Momentum);
            let mut snap = mk_snapshot(ephemeral_pool, Pubkey::new_unique());
            snap.revision = rev;
            finish_inflight(
                &revisions,
                &snap,
                PoolCommandTerminal::UnpinnedRejected,
                false,
            );
        }
        assert!(revisions.active_key_count() <= 4);
    }

    #[test]
    fn revision_u64_max_exhaustion_fails_closed() {
        let revisions = PoolSnapshotRevisionSequencer::with_max_keys(8);
        let pool = Pubkey::new_unique();
        ensure_key(&revisions, pool, ConsumerId::Momentum);
        revisions.test_seed_slot_revision_state(pool, ConsumerId::Momentum, u64::MAX, u64::MAX - 1);
        let mut snap = mk_snapshot(pool, Pubkey::new_unique());
        assert_eq!(
            revisions.assign_next(&mut snap),
            RevisionAssignResult::RegistryFull
        );
    }

    #[test]
    fn pending_replays_latest_revision_not_kind_order() {
        let (pending, _revisions) = mk_pending(8);
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
        let drained = pending.drain_all();
        assert_eq!(drained.len(), 1);
        match &drained[0] {
            PendingPoolCommand::AfterTrade(s) => {
                assert_eq!(s.revision, 2);
                assert_eq!(s.vaults[0].pubkey, new_vault);
            }
            other => panic!("expected AfterTrade latest, got {other:?}"),
        }
    }

    #[test]
    fn pending_stale_out_of_order_across_all_kinds_no_ops() {
        let (pending, revisions) = mk_pending(8);
        let pool = Pubkey::new_unique();
        ensure_key(&revisions, pool, ConsumerId::Momentum);
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
        ensure_key(&revisions, pool, ConsumerId::Momentum);
        let mut newer = mk_snapshot(pool, Pubkey::new_unique());
        let mut older = mk_snapshot(pool, Pubkey::new_unique());
        let rev_new = reserve_assign(&revisions, pool, ConsumerId::Momentum);
        newer.revision = rev_new;
        let rev_latest = {
            assert_reserved_inflight(&revisions, pool, ConsumerId::Momentum);
            match revisions.assign_next(&mut older) {
                RevisionAssignResult::Assigned(rev) => rev,
                other => panic!("{other:?}"),
            }
        };
        finish_inflight(&revisions, &older, PoolCommandTerminal::Applied, true);
        assert_reserved_inflight(&revisions, pool, ConsumerId::Momentum);
        newer.revision = rev_new;
        finish_inflight(
            &revisions,
            &newer,
            PoolCommandTerminal::StaleRevision,
            false,
        );
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
            let mut snapshot = mk_snapshot(pool, Pubkey::new_unique());
            snapshot.revision = revision;
            finish_inflight(&revisions, &snapshot, PoolCommandTerminal::Applied, true);
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
        let pools: Vec<Pubkey> = (0..5).map(|_| Pubkey::new_unique()).collect();
        for pool in &pools[..4] {
            let _ = register_and_assign(&revisions, *pool, ConsumerId::Momentum);
        }
        assert_eq!(revisions.active_key_count(), 4);
        assert_eq!(
            revisions.ensure_revision_key(pools[4], ConsumerId::Momentum),
            RevisionAcquireResult::RegistryFull,
            "must fail closed when all slots are in use"
        );
        let refs = revisions.key_refs(pools[0], ConsumerId::Momentum);
        assert_eq!(refs.total(), 1);
    }

    #[test]
    fn repinned_slot_rejects_stale_after_newer_applied() {
        let revisions = PoolSnapshotRevisionSequencer::with_max_keys(8);
        let pool = Pubkey::new_unique();
        ensure_key(&revisions, pool, ConsumerId::Momentum);
        let mut stale = mk_snapshot(pool, Pubkey::new_unique());
        let stale_rev = reserve_assign(&revisions, pool, ConsumerId::Momentum);
        stale.revision = stale_rev;
        finish_inflight(&revisions, &stale, PoolCommandTerminal::Applied, true);
        revisions.retire_key(pool, ConsumerId::Momentum);
        assert_eq!(revisions.active_key_count(), 0);

        ensure_key(&revisions, pool, ConsumerId::Momentum);
        let mut fresh = mk_snapshot(pool, Pubkey::new_unique());
        let fresh_rev = reserve_assign(&revisions, pool, ConsumerId::Momentum);
        fresh.revision = fresh_rev;
        assert_reserved_inflight(&revisions, pool, ConsumerId::Momentum);
        let mut newer = mk_snapshot(pool, Pubkey::new_unique());
        let newer_rev = {
            assert_reserved_inflight(&revisions, pool, ConsumerId::Momentum);
            match revisions.assign_next(&mut newer) {
                RevisionAssignResult::Assigned(rev) => rev,
                other => panic!("{other:?}"),
            }
        };
        finish_inflight(&revisions, &newer, PoolCommandTerminal::Applied, true);
        stale.revision = stale_rev;
        assert_eq!(
            revisions.begin_pool_command(&stale),
            PoolCommandAcceptPhase::Stale
        );
        finish_inflight(&revisions, &fresh, PoolCommandTerminal::Applied, true);
        assert_eq!(
            revisions.current_applied(pool, ConsumerId::Momentum),
            newer_rev
        );
    }

    #[test]
    fn long_churn_memory_and_stale_rejection_bounded() {
        let revisions = PoolSnapshotRevisionSequencer::with_max_keys(16);
        let mut issued: Vec<(Pubkey, u64)> = Vec::new();
        for _ in 0..256 {
            let pool = Pubkey::new_unique();
            let rev = register_and_assign(&revisions, pool, ConsumerId::Momentum);
            let mut snapshot = mk_snapshot(pool, Pubkey::new_unique());
            snapshot.revision = rev;
            finish_inflight(&revisions, &snapshot, PoolCommandTerminal::Applied, true);
            issued.push((pool, rev));
        }
        assert!(revisions.active_key_count() <= 16);
        for (pool, stale_rev) in issued.into_iter().take(32) {
            let mut stale = mk_snapshot(pool, Pubkey::new_unique());
            stale.revision = stale_rev;
            assert_reserved_inflight(&revisions, pool, ConsumerId::Momentum);
            finish_inflight(
                &revisions,
                &stale,
                PoolCommandTerminal::StaleRevision,
                false,
            );
        }
    }

    #[test]
    fn concurrent_inflight_reserve_and_transfer_no_zero_ref_gap() {
        use std::sync::Arc;
        use std::thread;

        let revisions = Arc::new(PoolSnapshotRevisionSequencer::with_max_keys(8));
        let pool = Pubkey::new_unique();
        ensure_key(&revisions, pool, ConsumerId::Momentum);
        let revisions_a = Arc::clone(&revisions);
        let revisions_b = Arc::clone(&revisions);
        let handle = thread::spawn(move || {
            for _ in 0..64 {
                assert_eq!(
                    revisions_a.reserve_inflight_command(pool, ConsumerId::Momentum),
                    InflightReserveResult::Reserved
                );
                revisions_a.release_inflight_command(pool, ConsumerId::Momentum);
            }
        });
        for _ in 0..64 {
            assert_eq!(
                revisions_b.reserve_inflight_command(pool, ConsumerId::Momentum),
                InflightReserveResult::Reserved
            );
            assert!(revisions_b.transfer_inflight_to_pending(pool, ConsumerId::Momentum));
            revisions_b.dec_pending_ref(pool, ConsumerId::Momentum);
        }
        handle.join().expect("join");
        let refs = revisions.key_refs(pool, ConsumerId::Momentum);
        assert_eq!(refs.inflight, 0);
        assert_eq!(refs.pending, 0);
        assert_eq!(refs.total(), 0);
    }

    #[test]
    fn upsert_after_send_failure_releases_inflight_on_coalesce_and_stale() {
        let revisions = Arc::new(PoolSnapshotRevisionSequencer::with_max_keys(8));
        let pending = PendingPoolRegistrations::new(8, Arc::clone(&revisions));
        let pool = Pubkey::new_unique();
        ensure_key(&revisions, pool, ConsumerId::Momentum);
        let rev = register_and_assign(&revisions, pool, ConsumerId::Momentum);
        revisions.release_inflight_command(pool, ConsumerId::Momentum);

        let mut first = mk_snapshot(pool, Pubkey::new_unique());
        first.revision = rev;
        assert_reserved_inflight(&revisions, pool, ConsumerId::Momentum);
        assert_eq!(
            pending.upsert_after_inflight_send_failure(
                pool,
                ConsumerId::Momentum,
                PendingPoolCommand::RegisterReserves(first),
            ),
            PendingPoolUpsertResult::Stored
        );
        assert_eq!(revisions.key_refs(pool, ConsumerId::Momentum).pending, 1);
        assert_eq!(revisions.key_refs(pool, ConsumerId::Momentum).inflight, 0);

        let mut newer = mk_snapshot(pool, Pubkey::new_unique());
        newer.revision = rev.saturating_add(1);
        assert_reserved_inflight(&revisions, pool, ConsumerId::Momentum);
        assert_eq!(
            pending.upsert_after_inflight_send_failure(
                pool,
                ConsumerId::Momentum,
                PendingPoolCommand::AfterTrade(newer),
            ),
            PendingPoolUpsertResult::Coalesced
        );
        assert_eq!(revisions.key_refs(pool, ConsumerId::Momentum).pending, 1);
        assert_eq!(revisions.key_refs(pool, ConsumerId::Momentum).inflight, 0);

        let mut stale = mk_snapshot(pool, Pubkey::new_unique());
        stale.revision = rev;
        assert_reserved_inflight(&revisions, pool, ConsumerId::Momentum);
        assert_eq!(
            pending.upsert_after_inflight_send_failure(
                pool,
                ConsumerId::Momentum,
                PendingPoolCommand::VaultsFromAccount(stale),
            ),
            PendingPoolUpsertResult::StaleNoOp
        );
        assert_eq!(revisions.key_refs(pool, ConsumerId::Momentum).total(), 1);

        let drained = pending.drain_all();
        assert_eq!(drained.len(), 1);
        assert_eq!(revisions.key_refs(pool, ConsumerId::Momentum).total(), 0);
    }

    #[test]
    fn accepted_noop_advances_applied_revision_watermark() {
        let revisions = PoolSnapshotRevisionSequencer::with_max_keys(8);
        let pool = Pubkey::new_unique();
        ensure_key(&revisions, pool, ConsumerId::Momentum);
        let rev = register_and_assign(&revisions, pool, ConsumerId::Momentum);
        let mut snapshot = mk_snapshot(pool, Pubkey::new_unique());
        snapshot.revision = rev;
        finish_inflight(&revisions, &snapshot, PoolCommandTerminal::Applied, true);
        let mut stale = mk_snapshot(pool, Pubkey::new_unique());
        stale.revision = rev.saturating_sub(1);
        assert_eq!(
            revisions.begin_pool_command(&stale),
            PoolCommandAcceptPhase::Stale
        );
        finish_inflight(
            &revisions,
            &stale,
            PoolCommandTerminal::StaleRevision,
            false,
        );
        assert_eq!(revisions.current_applied(pool, ConsumerId::Momentum), rev);
    }

    #[test]
    fn pending_replay_releases_pending_ref_on_stale_terminal() {
        let revisions = PoolSnapshotRevisionSequencer::with_max_keys(8);
        let pool = Pubkey::new_unique();
        ensure_key(&revisions, pool, ConsumerId::Momentum);
        revisions.inc_pending_ref(pool, ConsumerId::Momentum);
        let mut snapshot = mk_snapshot(pool, Pubkey::new_unique());
        snapshot.revision = 1;
        revisions.finish_pool_command(
            &snapshot,
            PoolCommandTerminal::StaleRevision,
            Some(PoolCommandRefRelease::Pending),
            true,
        );
        assert_eq!(revisions.key_refs(pool, ConsumerId::Momentum).pending, 0);
    }

    #[test]
    fn atomic_stash_visible_only_with_pending_ref() {
        use std::sync::Arc;
        use std::thread;

        let revisions = Arc::new(PoolSnapshotRevisionSequencer::with_max_keys(8));
        let pending = Arc::new(PendingPoolRegistrations::new(8, Arc::clone(&revisions)));
        let pool = Pubkey::new_unique();
        ensure_key(&revisions, pool, ConsumerId::Momentum);
        let rev = register_and_assign(&revisions, pool, ConsumerId::Momentum);
        revisions.release_inflight_command(pool, ConsumerId::Momentum);

        revisions.test_set_stash_hold_before_visible(true);

        let revisions_producer = Arc::clone(&revisions);
        let pending_producer = Arc::clone(&pending);
        let producer = thread::spawn(move || {
            let mut snap = mk_snapshot(pool, Pubkey::new_unique());
            snap.revision = rev;
            assert_reserved_inflight(&revisions_producer, pool, ConsumerId::Momentum);
            pending_producer.upsert_after_inflight_send_failure(
                pool,
                ConsumerId::Momentum,
                PendingPoolCommand::RegisterReserves(snap),
            )
        });

        for _ in 0..200 {
            let has_entry = pending.has_pending(pool, ConsumerId::Momentum);
            let refs = revisions.key_refs(pool, ConsumerId::Momentum);
            if has_entry {
                assert!(
                    refs.pending > 0,
                    "visible pending entry must have pending ref"
                );
            }
            if refs.pending > 0 && !has_entry {
                assert!(
                    revisions.test_stash_hold_active(),
                    "pending ref without visible entry only during stash hold"
                );
            }
            thread::yield_now();
        }

        revisions.test_set_stash_hold_before_visible(false);
        assert_eq!(
            producer.join().expect("producer"),
            PendingPoolUpsertResult::Stored
        );
        assert!(pending.has_pending(pool, ConsumerId::Momentum));
        assert_eq!(revisions.key_refs(pool, ConsumerId::Momentum).pending, 1);

        revisions.test_set_drain_hold_before_remove(true);
        let pending_drain = Arc::clone(&pending);
        let drainer = thread::spawn(move || pending_drain.drain_all());
        for _ in 0..200 {
            let has_entry = pending.has_pending(pool, ConsumerId::Momentum);
            let refs = revisions.key_refs(pool, ConsumerId::Momentum);
            if !has_entry && refs.pending > 0 {
                assert!(
                    revisions.test_drain_hold_active(),
                    "pending ref after entry removed only during drain hold"
                );
            }
            thread::yield_now();
        }
        revisions.test_set_drain_hold_before_remove(false);
        let drained = drainer.join().expect("drainer");
        assert_eq!(drained.len(), 1);
        assert_eq!(revisions.key_refs(pool, ConsumerId::Momentum).pending, 0);
        assert!(!pending.has_pending(pool, ConsumerId::Momentum));
    }
}
