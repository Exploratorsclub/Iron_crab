//! Pure explicit-account admission state machine (foundation slice).
//!
//! Deterministic cap, group atomicity, consumer priority, shared-pubkey refcounts,
//! and LRU eviction — no RPC, async, or production wiring.

use solana_sdk::pubkey::Pubkey;
#[cfg(test)]
use std::cell::Cell;
use std::collections::{BTreeSet, HashMap, HashSet};

/// Consumer tag for explicit Geyser pubkey ownership.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ConsumerId {
    Wallet,
    Momentum,
    Arb,
    Tracker,
}

/// Pin priority for cap eviction — lower ordinal = higher protection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PinPriority {
    Wallet = 0,
    Momentum = 1,
    Arb = 2,
    Tracker = 3,
}

/// Stable logical owner identity (consumer + owner pubkey).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct OwnerKey {
    pub consumer: ConsumerId,
    pub owner: Pubkey,
}

impl OwnerKey {
    pub fn new(consumer: ConsumerId, owner: Pubkey) -> Self {
        Self { consumer, owner }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerGroupSnapshot {
    pub owner: OwnerKey,
    pub pubkeys: HashSet<Pubkey>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdmissionResult {
    Inserted {
        added_pubkeys: Vec<Pubkey>,
        evicted_owners: Vec<OwnerKey>,
    },
    OwnerAddedNoNewPubkey,
    RejectedCap,
    RejectedProtected,
    RejectedInvalidGroup,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CapConvergeResult {
    Converged { evicted_owners: Vec<OwnerKey> },
    ProtectedOverflow,
    Unconverged,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreResult {
    pub admitted: Vec<OwnerKey>,
    pub rejected: Vec<OwnerKey>,
}

/// Optional observer for eviction side-effects (no metrics dependency).
pub trait AdmissionObserver {
    fn on_group_evicted(&mut self, owner: OwnerKey, pubkeys: &[Pubkey]);
}

#[derive(Debug, Default)]
pub struct NoopAdmissionObserver;

impl AdmissionObserver for NoopAdmissionObserver {
    fn on_group_evicted(&mut self, _owner: OwnerKey, _pubkeys: &[Pubkey]) {}
}

#[derive(Debug, Clone)]
struct PubkeyEntry {
    owners: BTreeSet<OwnerKey>,
}

#[derive(Debug, Clone)]
struct OwnerGroupState {
    pubkeys: HashSet<Pubkey>,
    lru_stamp: u64,
}

#[derive(Debug, Clone)]
struct AdmitPlan {
    added_pubkeys: HashSet<Pubkey>,
    evict_owners: Vec<OwnerKey>,
    replace_old_pubkeys: Option<HashSet<Pubkey>>,
    touch_existing: bool,
}

/// Test-only planning instrumentation.
#[cfg(test)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PlanningStats {
    pub full_graph_copies: usize,
    pub owner_edge_visits: usize,
    pub candidate_pops: usize,
}

#[cfg(test)]
thread_local! {
    static PLANNING_STATS: Cell<PlanningStats> = const { Cell::new(PlanningStats {
        full_graph_copies: 0,
        owner_edge_visits: 0,
        candidate_pops: 0,
    }) };
}

#[cfg(test)]
fn reset_planning_stats() {
    PLANNING_STATS.with(|s| s.set(PlanningStats::default()));
}

#[cfg(test)]
fn take_planning_stats() -> PlanningStats {
    PLANNING_STATS.with(|s| {
        let v = s.get();
        s.set(PlanningStats::default());
        v
    })
}

#[cfg(test)]
fn record_owner_edge_visit() {
    PLANNING_STATS.with(|s| {
        let mut v = s.get();
        v.owner_edge_visits += 1;
        s.set(v);
    });
}

#[cfg(test)]
fn record_candidate_pop() {
    PLANNING_STATS.with(|s| {
        let mut v = s.get();
        v.candidate_pops += 1;
        s.set(v);
    });
}

#[cfg(not(test))]
fn record_owner_edge_visit() {}

#[cfg(not(test))]
fn record_candidate_pop() {}

/// Admission-managed explicit pubkey set — distinct from legacy `DesiredExplicitSet`.
#[derive(Debug, Clone)]
pub struct AdmissionDesiredExplicitSet {
    cap: usize,
    pubkeys: HashMap<Pubkey, PubkeyEntry>,
    owner_groups: HashMap<OwnerKey, OwnerGroupState>,
    lru_counter: u64,
}

/// Fail-closed LRU stamp exhaustion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LruStampError;

const LRU_RENORMALIZE_THRESHOLD: u64 = u64::MAX - 1024;

impl Default for AdmissionDesiredExplicitSet {
    fn default() -> Self {
        Self::new(25_000)
    }
}

impl AdmissionDesiredExplicitSet {
    pub fn new(cap: usize) -> Self {
        Self {
            cap: cap.max(1),
            pubkeys: HashMap::new(),
            owner_groups: HashMap::new(),
            lru_counter: 0,
        }
    }

    #[cfg(test)]
    pub fn new_for_test(cap: usize, lru_counter: u64) -> Self {
        Self {
            cap: cap.max(1),
            pubkeys: HashMap::new(),
            owner_groups: HashMap::new(),
            lru_counter,
        }
    }

    pub fn len(&self) -> usize {
        self.pubkeys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pubkeys.is_empty()
    }

    pub fn max_explicit_pubkeys(&self) -> usize {
        self.cap
    }

    /// Sole public cap mutation API — always converges or returns fail-closed status.
    pub fn set_max_explicit_pubkeys(&mut self, cap: usize) -> CapConvergeResult {
        let snapshot = self.clone();
        let previous_cap = self.cap;
        self.cap = cap.max(1);
        let result = self.converge_to_cap_observed(&mut NoopAdmissionObserver);
        match &result {
            CapConvergeResult::Converged { .. } => {
                debug_assert!(self.in_cap_invariant_ok(&result));
                result
            }
            CapConvergeResult::ProtectedOverflow => result,
            CapConvergeResult::Unconverged => {
                *self = snapshot;
                self.cap = previous_cap;
                CapConvergeResult::Unconverged
            }
        }
    }

    pub fn contains(&self, pubkey: &Pubkey) -> bool {
        self.pubkeys.contains_key(pubkey)
    }

    pub fn snapshot_pubkeys(&self) -> HashSet<Pubkey> {
        self.pubkeys.keys().copied().collect()
    }

    pub fn snapshot_owner_groups(&self) -> Vec<OwnerGroupSnapshot> {
        let mut groups: Vec<_> = self
            .owner_groups
            .iter()
            .map(|(owner, state)| OwnerGroupSnapshot {
                owner: *owner,
                pubkeys: state.pubkeys.clone(),
            })
            .collect();
        groups.sort_by_key(|g| g.owner);
        groups
    }

    pub fn wallet_demand_exceeds_cap(&self, wallet_pubkeys: &[Pubkey]) -> bool {
        dedupe_pubkeys(wallet_pubkeys).len() > self.cap
    }

    pub fn cap_overflow(&self) -> i64 {
        let len = self.len();
        let cap = self.cap;
        if len <= cap {
            return 0;
        }
        let overflow = len - cap;
        if overflow > i64::MAX as usize {
            i64::MAX
        } else {
            overflow as i64
        }
    }

    pub fn try_admit_group(
        &mut self,
        consumer: ConsumerId,
        owner: Pubkey,
        pubkeys: &[Pubkey],
    ) -> AdmissionResult {
        self.try_admit_group_observed(consumer, owner, pubkeys, &mut NoopAdmissionObserver)
    }

    pub fn try_admit_group_observed(
        &mut self,
        consumer: ConsumerId,
        owner: Pubkey,
        pubkeys: &[Pubkey],
        observer: &mut dyn AdmissionObserver,
    ) -> AdmissionResult {
        let deduped = dedupe_pubkeys(pubkeys);
        if deduped.is_empty() {
            return AdmissionResult::RejectedInvalidGroup;
        }

        let key = OwnerKey::new(consumer, owner);
        let plan = match self.plan_admit(key, &deduped) {
            Ok(plan) => plan,
            Err(reject) => return reject,
        };

        if plan.touch_existing {
            if self.bump_lru_stamp(key).is_err() {
                return AdmissionResult::RejectedCap;
            }
            return AdmissionResult::OwnerAddedNoNewPubkey;
        }

        for victim in &plan.evict_owners {
            let victim_pubkeys = self
                .owner_groups
                .get(victim)
                .map(|g| g.pubkeys.iter().copied().collect::<Vec<_>>())
                .unwrap_or_default();
            self.remove_group_internal(*victim);
            observer.on_group_evicted(*victim, &victim_pubkeys);
        }

        if let Some(old) = plan.replace_old_pubkeys {
            self.remove_owner_pubkeys(key, &old);
        }

        if self.insert_owner_pubkeys(key, &deduped).is_err() {
            return AdmissionResult::RejectedCap;
        }

        let mut added: Vec<_> = plan.added_pubkeys.into_iter().collect();
        added.sort();
        let mut evicted = plan.evict_owners;
        evicted.sort();

        debug_assert!(self.len() <= self.cap || self.wallet_only_overflow());

        AdmissionResult::Inserted {
            added_pubkeys: added,
            evicted_owners: evicted,
        }
    }

    pub fn remove_group(&mut self, consumer: ConsumerId, owner: Pubkey) -> bool {
        let key = OwnerKey::new(consumer, owner);
        if self.owner_groups.contains_key(&key) {
            self.remove_group_internal(key);
            if self.len() > self.cap && !self.wallet_only_overflow() {
                let _ = self.converge_to_cap_observed(&mut NoopAdmissionObserver);
            }
            true
        } else {
            false
        }
    }

    pub fn touch_group(&mut self, consumer: ConsumerId, owner: Pubkey) -> bool {
        let key = OwnerKey::new(consumer, owner);
        if self.owner_groups.contains_key(&key) {
            self.bump_lru_stamp(key).is_ok()
        } else {
            false
        }
    }

    pub fn converge_to_cap(&mut self) -> CapConvergeResult {
        self.converge_to_cap_observed(&mut NoopAdmissionObserver)
    }

    pub fn converge_to_cap_observed(
        &mut self,
        observer: &mut dyn AdmissionObserver,
    ) -> CapConvergeResult {
        if self.wallet_only_overflow() {
            return CapConvergeResult::ProtectedOverflow;
        }

        let mut evicted = Vec::new();
        while self.len() > self.cap {
            let Some(victim) = self.pick_shrink_victim_marginal() else {
                if self.wallet_only_overflow() {
                    return CapConvergeResult::ProtectedOverflow;
                }
                return CapConvergeResult::Unconverged;
            };
            let victim_pubkeys = self
                .owner_groups
                .get(&victim)
                .map(|g| g.pubkeys.iter().copied().collect::<Vec<_>>())
                .unwrap_or_default();
            self.remove_group_internal(victim);
            observer.on_group_evicted(victim, &victim_pubkeys);
            evicted.push(victim);
        }
        evicted.sort();
        CapConvergeResult::Converged {
            evicted_owners: evicted,
        }
    }

    pub fn restore_owner_groups(&mut self, groups: &[OwnerGroupSnapshot]) -> RestoreResult {
        let mut sorted = groups.to_vec();
        sorted.sort_by_key(|g| g.owner);

        let mut admitted = Vec::new();
        let mut rejected = Vec::new();
        for group in sorted {
            let result = self.try_admit_group(
                group.owner.consumer,
                group.owner.owner,
                &group.pubkeys.iter().copied().collect::<Vec<_>>(),
            );
            match result {
                AdmissionResult::Inserted { .. } | AdmissionResult::OwnerAddedNoNewPubkey => {
                    admitted.push(group.owner);
                }
                AdmissionResult::RejectedCap
                | AdmissionResult::RejectedProtected
                | AdmissionResult::RejectedInvalidGroup => {
                    rejected.push(group.owner);
                }
            }
        }
        RestoreResult { admitted, rejected }
    }

    pub fn reconcile_from_owner_groups(&mut self, groups: &[OwnerGroupSnapshot]) -> RestoreResult {
        self.pubkeys.clear();
        self.owner_groups.clear();
        self.lru_counter = 0;
        self.restore_owner_groups(groups)
    }

    pub fn owner_refcount(&self, pubkey: &Pubkey) -> usize {
        self.pubkeys
            .get(pubkey)
            .map(|e| e.owners.len())
            .unwrap_or(0)
    }

    // --- internal ---

    fn in_cap_invariant_ok(&self, converge: &CapConvergeResult) -> bool {
        self.len() <= self.cap
            || self.wallet_only_overflow()
            || matches!(
                converge,
                CapConvergeResult::ProtectedOverflow | CapConvergeResult::Unconverged
            )
    }

    fn pin_priority(consumer: ConsumerId) -> PinPriority {
        match consumer {
            ConsumerId::Wallet => PinPriority::Wallet,
            ConsumerId::Momentum => PinPriority::Momentum,
            ConsumerId::Arb => PinPriority::Arb,
            ConsumerId::Tracker => PinPriority::Tracker,
        }
    }

    fn can_evict_for_incoming(victim: ConsumerId, incoming: ConsumerId) -> bool {
        if victim == ConsumerId::Wallet {
            return false;
        }
        let victim_p = Self::pin_priority(victim);
        let incoming_p = Self::pin_priority(incoming);
        if victim_p > incoming_p {
            return true;
        }
        victim_p == incoming_p && incoming_p != PinPriority::Wallet
    }

    fn plan_admit(
        &self,
        key: OwnerKey,
        new_pubkeys: &HashSet<Pubkey>,
    ) -> Result<AdmitPlan, AdmissionResult> {
        let old_pubkeys = self.owner_groups.get(&key).map(|g| &g.pubkeys);

        if old_pubkeys == Some(new_pubkeys) {
            return Ok(AdmitPlan {
                added_pubkeys: HashSet::new(),
                evict_owners: Vec::new(),
                replace_old_pubkeys: None,
                touch_existing: true,
            });
        }

        let added_pubkeys = self.added_pubkeys_after(key, new_pubkeys, old_pubkeys, &[]);
        let projected_len = self.projected_len(key, new_pubkeys, old_pubkeys, &[]);
        let deficit = projected_len.saturating_sub(self.cap);

        if deficit == 0 {
            return Ok(AdmitPlan {
                added_pubkeys,
                evict_owners: Vec::new(),
                replace_old_pubkeys: old_pubkeys.cloned(),
                touch_existing: false,
            });
        }

        if key.consumer == ConsumerId::Wallet
            && self.wallet_demand_after(key, new_pubkeys) > self.cap
        {
            return Err(AdmissionResult::RejectedProtected);
        }

        let evict_owners = self.plan_evictions(key, new_pubkeys, old_pubkeys, deficit)?;
        let after_len = self.projected_len(key, new_pubkeys, old_pubkeys, &evict_owners);
        if after_len > self.cap {
            return Err(if key.consumer == ConsumerId::Wallet {
                AdmissionResult::RejectedProtected
            } else {
                AdmissionResult::RejectedCap
            });
        }

        Ok(AdmitPlan {
            added_pubkeys,
            evict_owners,
            replace_old_pubkeys: old_pubkeys.cloned(),
            touch_existing: false,
        })
    }

    fn wallet_demand_after(&self, incoming: OwnerKey, new_pubkeys: &HashSet<Pubkey>) -> usize {
        self.owner_groups
            .iter()
            .filter(|(owner, _)| owner.consumer == ConsumerId::Wallet && **owner != incoming)
            .flat_map(|(_, g)| g.pubkeys.iter().copied())
            .chain(new_pubkeys.iter().copied())
            .collect::<HashSet<_>>()
            .len()
    }

    fn projected_refcount(
        &self,
        pk: &Pubkey,
        incoming: OwnerKey,
        new_pubkeys: &HashSet<Pubkey>,
        old_pubkeys: Option<&HashSet<Pubkey>>,
        evicted: &[OwnerKey],
    ) -> usize {
        record_owner_edge_visit();
        let mut count = self.owner_refcount(pk);

        if let Some(old) = old_pubkeys {
            if old.contains(pk) && !new_pubkeys.contains(pk) {
                count = count.saturating_sub(1);
            }
        }

        for victim in evicted {
            if self
                .owner_groups
                .get(victim)
                .is_some_and(|g| g.pubkeys.contains(pk))
            {
                count = count.saturating_sub(1);
            }
        }

        if new_pubkeys.contains(pk) {
            let already_present = self.pubkeys.contains_key(pk);
            let retained_from_incoming = old_pubkeys.is_some_and(|old| old.contains(pk));
            if !already_present || !retained_from_incoming {
                count += 1;
            }
        }

        let _ = incoming;
        count
    }

    fn projected_len(
        &self,
        incoming: OwnerKey,
        new_pubkeys: &HashSet<Pubkey>,
        old_pubkeys: Option<&HashSet<Pubkey>>,
        evicted: &[OwnerKey],
    ) -> usize {
        let mut touched = HashSet::new();
        touched.extend(new_pubkeys.iter().copied());
        if let Some(old) = old_pubkeys {
            touched.extend(old.iter().copied());
        }
        for victim in evicted {
            if let Some(group) = self.owner_groups.get(victim) {
                touched.extend(group.pubkeys.iter().copied());
            }
        }

        let unchanged = self
            .pubkeys
            .keys()
            .filter(|pk| !touched.contains(pk))
            .count();

        let mut touched_present = 0usize;
        for pk in touched {
            if self.projected_refcount(&pk, incoming, new_pubkeys, old_pubkeys, evicted) > 0 {
                touched_present += 1;
            }
        }
        unchanged + touched_present
    }

    fn added_pubkeys_after(
        &self,
        incoming: OwnerKey,
        new_pubkeys: &HashSet<Pubkey>,
        old_pubkeys: Option<&HashSet<Pubkey>>,
        evicted: &[OwnerKey],
    ) -> HashSet<Pubkey> {
        new_pubkeys
            .iter()
            .filter(|pk| {
                self.projected_refcount(pk, incoming, new_pubkeys, old_pubkeys, evicted) > 0
                    && self.projected_refcount(pk, incoming, new_pubkeys, old_pubkeys, evicted) == 1
                    && (!self.pubkeys.contains_key(pk)
                        || old_pubkeys.is_some_and(|old| !old.contains(pk)))
            })
            .copied()
            .collect()
    }

    fn marginal_free_for_evicting(
        &self,
        victim: OwnerKey,
        incoming: OwnerKey,
        new_pubkeys: &HashSet<Pubkey>,
        old_pubkeys: Option<&HashSet<Pubkey>>,
        evicted_so_far: &[OwnerKey],
    ) -> usize {
        if !self.owner_groups.contains_key(&victim) {
            return 0;
        }
        let mut evicted = evicted_so_far.to_vec();
        evicted.push(victim);
        let before = self.projected_len(incoming, new_pubkeys, old_pubkeys, evicted_so_far);
        let after = self.projected_len(incoming, new_pubkeys, old_pubkeys, &evicted);
        before.saturating_sub(after)
    }

    fn plan_evictions(
        &self,
        incoming: OwnerKey,
        new_pubkeys: &HashSet<Pubkey>,
        old_pubkeys: Option<&HashSet<Pubkey>>,
        deficit: usize,
    ) -> Result<Vec<OwnerKey>, AdmissionResult> {
        let mut victims = Vec::new();
        let mut candidates: Vec<(OwnerKey, PinPriority, u64)> = self
            .owner_groups
            .iter()
            .filter(|(owner, _)| {
                **owner != incoming
                    && Self::can_evict_for_incoming(owner.consumer, incoming.consumer)
            })
            .map(|(owner, state)| (*owner, Self::pin_priority(owner.consumer), state.lru_stamp))
            .collect();

        candidates.sort_by(|a, b| {
            b.1.cmp(&a.1)
                .then_with(|| a.2.cmp(&b.2))
                .then_with(|| a.0.cmp(&b.0))
        });

        let mut remaining_deficit = deficit;
        let mut idx = 0usize;
        while remaining_deficit > 0 && idx < candidates.len() {
            record_candidate_pop();
            let (victim, _, _) = candidates[idx];
            idx += 1;

            let marginal = self.marginal_free_for_evicting(
                victim,
                incoming,
                new_pubkeys,
                old_pubkeys,
                &victims,
            );
            if marginal == 0 {
                continue;
            }
            victims.push(victim);
            remaining_deficit = remaining_deficit.saturating_sub(marginal);
        }

        if remaining_deficit > 0 {
            let mut joint_candidates = candidates;
            joint_candidates.sort_by(|a, b| {
                b.1.cmp(&a.1)
                    .then_with(|| a.2.cmp(&b.2))
                    .then_with(|| a.0.cmp(&b.0))
            });
            for (victim, _, _) in joint_candidates {
                if victims.contains(&victim) {
                    continue;
                }
                record_candidate_pop();
                let marginal = self.marginal_free_for_evicting(
                    victim,
                    incoming,
                    new_pubkeys,
                    old_pubkeys,
                    &victims,
                );
                if marginal == 0 {
                    continue;
                }
                victims.push(victim);
                remaining_deficit = remaining_deficit.saturating_sub(marginal);
                if remaining_deficit == 0 {
                    break;
                }
            }
        }

        if remaining_deficit > 0 {
            return Err(AdmissionResult::RejectedCap);
        }

        let projected = self.projected_len(incoming, new_pubkeys, old_pubkeys, &victims);
        if projected > self.cap {
            return Err(AdmissionResult::RejectedCap);
        }
        let _ = deficit;
        Ok(victims)
    }

    fn pick_shrink_victim_marginal(&self) -> Option<OwnerKey> {
        let deficit = self.len().saturating_sub(self.cap);
        if deficit == 0 {
            return None;
        }
        let mut candidates: Vec<(OwnerKey, PinPriority, u64)> = self
            .owner_groups
            .iter()
            .filter(|(owner, _)| owner.consumer != ConsumerId::Wallet)
            .map(|(owner, state)| (*owner, Self::pin_priority(owner.consumer), state.lru_stamp))
            .collect();
        if candidates.is_empty() {
            return None;
        }
        candidates.sort_by(|a, b| {
            b.1.cmp(&a.1)
                .then_with(|| a.2.cmp(&b.2))
                .then_with(|| a.0.cmp(&b.0))
        });

        let mut best: Option<(OwnerKey, usize)> = None;
        for (victim, _, _) in &candidates {
            record_candidate_pop();
            let marginal = self.marginal_free_for_shrink(*victim);
            if marginal == 0 {
                continue;
            }
            let fits = marginal >= deficit;
            let better = match best {
                None => true,
                Some((_, best_marginal)) => {
                    if fits && best_marginal < deficit {
                        true
                    } else if fits == (best_marginal >= deficit) {
                        marginal < best_marginal
                    } else {
                        fits
                    }
                }
            };
            if better {
                best = Some((*victim, marginal));
            }
        }

        best.map(|(victim, _)| victim)
    }

    fn marginal_free_for_shrink(&self, victim: OwnerKey) -> usize {
        let Some(group) = self.owner_groups.get(&victim) else {
            return 0;
        };
        group
            .pubkeys
            .iter()
            .filter(|pk| self.owner_refcount(pk) == 1)
            .count()
    }

    fn wallet_only_overflow(&self) -> bool {
        let wallet_count: HashSet<Pubkey> = self
            .owner_groups
            .iter()
            .filter(|(o, _)| o.consumer == ConsumerId::Wallet)
            .flat_map(|(_, g)| g.pubkeys.iter().copied())
            .collect();
        wallet_count.len() > self.cap
    }

    fn bump_lru_stamp(&mut self, key: OwnerKey) -> Result<(), LruStampError> {
        let stamp = self.advance_lru_counter()?;
        if let Some(group) = self.owner_groups.get_mut(&key) {
            group.lru_stamp = stamp;
        }
        Ok(())
    }

    fn advance_lru_counter(&mut self) -> Result<u64, LruStampError> {
        if self.lru_counter >= LRU_RENORMALIZE_THRESHOLD {
            self.renormalize_lru_stamps()?;
        }
        self.lru_counter = self.lru_counter.checked_add(1).ok_or(LruStampError)?;
        Ok(self.lru_counter)
    }

    fn renormalize_lru_stamps(&mut self) -> Result<(), LruStampError> {
        let mut ordered: Vec<(OwnerKey, u64)> = self
            .owner_groups
            .iter()
            .map(|(k, g)| (*k, g.lru_stamp))
            .collect();
        ordered.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
        for (idx, (key, _)) in ordered.iter().enumerate() {
            let stamp = (idx as u64).checked_add(1).ok_or(LruStampError)?;
            if let Some(group) = self.owner_groups.get_mut(key) {
                group.lru_stamp = stamp;
            }
        }
        self.lru_counter = ordered.len().try_into().map_err(|_| LruStampError)?;
        Ok(())
    }

    fn insert_owner_pubkeys(
        &mut self,
        key: OwnerKey,
        pubkeys: &HashSet<Pubkey>,
    ) -> Result<(), LruStampError> {
        let stamp = self.advance_lru_counter()?;
        self.owner_groups
            .entry(key)
            .and_modify(|g| {
                g.pubkeys = pubkeys.clone();
                g.lru_stamp = stamp;
            })
            .or_insert_with(|| OwnerGroupState {
                pubkeys: pubkeys.clone(),
                lru_stamp: stamp,
            });

        for pk in pubkeys {
            self.pubkeys
                .entry(*pk)
                .or_insert_with(|| PubkeyEntry {
                    owners: BTreeSet::new(),
                })
                .owners
                .insert(key);
        }
        Ok(())
    }

    fn remove_owner_pubkeys(&mut self, key: OwnerKey, pubkeys: &HashSet<Pubkey>) {
        for pk in pubkeys {
            if let Some(entry) = self.pubkeys.get_mut(pk) {
                entry.owners.remove(&key);
                if entry.owners.is_empty() {
                    self.pubkeys.remove(pk);
                }
            }
        }
    }

    fn remove_group_internal(&mut self, key: OwnerKey) {
        if let Some(group) = self.owner_groups.remove(&key) {
            self.remove_owner_pubkeys(key, &group.pubkeys);
        }
    }
}

fn dedupe_pubkeys(pubkeys: &[Pubkey]) -> HashSet<Pubkey> {
    pubkeys.iter().copied().collect()
}

pub fn pin_priority_from_consumer(consumer: ConsumerId) -> PinPriority {
    match consumer {
        ConsumerId::Wallet => PinPriority::Wallet,
        ConsumerId::Momentum => PinPriority::Momentum,
        ConsumerId::Arb => PinPriority::Arb,
        ConsumerId::Tracker => PinPriority::Tracker,
    }
}

#[cfg(test)]
fn admission_result_class(result: &AdmissionResult) -> &'static str {
    match result {
        AdmissionResult::Inserted { .. } => "inserted",
        AdmissionResult::OwnerAddedNoNewPubkey => "touch",
        AdmissionResult::RejectedCap => "rejected_cap",
        AdmissionResult::RejectedProtected => "rejected_protected",
        AdmissionResult::RejectedInvalidGroup => "rejected_invalid",
    }
}

#[cfg(test)]
mod reference {
    use super::*;
    use std::collections::BTreeMap;

    /// Slow, naive reference model for deterministic equivalence checking.
    #[derive(Clone)]
    pub struct NaiveRefModel {
        cap: usize,
        owner_groups: BTreeMap<OwnerKey, HashSet<Pubkey>>,
        lru: BTreeMap<OwnerKey, u64>,
        lru_counter: u64,
    }

    impl NaiveRefModel {
        pub fn new(cap: usize) -> Self {
            Self {
                cap: cap.max(1),
                owner_groups: BTreeMap::new(),
                lru: BTreeMap::new(),
                lru_counter: 0,
            }
        }

        pub fn max_explicit_pubkeys(&self) -> usize {
            self.cap
        }

        pub fn len(&self) -> usize {
            self.physical().len()
        }

        fn physical(&self) -> HashSet<Pubkey> {
            let mut keys = HashSet::new();
            for pks in self.owner_groups.values() {
                keys.extend(pks.iter().copied());
            }
            keys
        }

        fn owner_refcount(&self, pk: &Pubkey) -> usize {
            self.owner_groups
                .values()
                .filter(|pks| pks.contains(pk))
                .count()
        }

        pub fn snapshot_pubkeys(&self) -> HashSet<Pubkey> {
            self.physical()
        }

        pub fn snapshot_owner_groups(&self) -> Vec<OwnerGroupSnapshot> {
            self.owner_groups
                .iter()
                .map(|(owner, pubkeys)| OwnerGroupSnapshot {
                    owner: *owner,
                    pubkeys: pubkeys.clone(),
                })
                .collect()
        }

        fn wallet_only_overflow(&self) -> bool {
            let wallet: HashSet<Pubkey> = self
                .owner_groups
                .iter()
                .filter(|(o, _)| o.consumer == ConsumerId::Wallet)
                .flat_map(|(_, pks)| pks.iter().copied())
                .collect();
            wallet.len() > self.cap
        }

        fn simulate_physical(
            &self,
            evicted: &[OwnerKey],
            incoming: OwnerKey,
            new_pubkeys: &HashSet<Pubkey>,
        ) -> HashSet<Pubkey> {
            let mut groups = self.owner_groups.clone();
            for v in evicted {
                groups.remove(v);
            }
            groups.insert(incoming, new_pubkeys.clone());
            let mut physical = HashSet::new();
            for pks in groups.values() {
                physical.extend(pks.iter().copied());
            }
            physical
        }

        fn plan_evictions(
            &self,
            incoming: OwnerKey,
            new_pubkeys: &HashSet<Pubkey>,
        ) -> Option<Vec<OwnerKey>> {
            let old_pubkeys = self.owner_groups.get(&incoming);
            let projected_len = |evicted: &[OwnerKey]| -> usize {
                let mut touched = HashSet::new();
                touched.extend(new_pubkeys.iter().copied());
                if let Some(old) = old_pubkeys {
                    touched.extend(old.iter().copied());
                }
                for victim in evicted {
                    if let Some(pks) = self.owner_groups.get(victim) {
                        touched.extend(pks.iter().copied());
                    }
                }
                let unchanged = self
                    .physical()
                    .iter()
                    .filter(|pk| !touched.contains(pk))
                    .count();
                let mut present = 0usize;
                for pk in touched {
                    let mut count = self.owner_refcount(&pk);
                    if let Some(old) = old_pubkeys {
                        if old.contains(&pk) && !new_pubkeys.contains(&pk) {
                            count = count.saturating_sub(1);
                        }
                    }
                    for victim in evicted {
                        if self
                            .owner_groups
                            .get(victim)
                            .is_some_and(|pks| pks.contains(&pk))
                        {
                            count = count.saturating_sub(1);
                        }
                    }
                    if new_pubkeys.contains(&pk) {
                        let already = self.physical().contains(&pk);
                        let retained = old_pubkeys.is_some_and(|old| old.contains(&pk));
                        if !already || !retained {
                            count += 1;
                        }
                    }
                    if count > 0 {
                        present += 1;
                    }
                }
                unchanged + present
            };

            let deficit = projected_len(&[]).saturating_sub(self.cap);
            if deficit == 0 {
                return Some(Vec::new());
            }

            let mut victims = Vec::new();
            let mut candidates: Vec<(OwnerKey, PinPriority, u64)> = self
                .owner_groups
                .keys()
                .copied()
                .filter(|owner| {
                    *owner != incoming
                        && AdmissionDesiredExplicitSet::can_evict_for_incoming(
                            owner.consumer,
                            incoming.consumer,
                        )
                })
                .map(|owner| {
                    (
                        owner,
                        pin_priority_from_consumer(owner.consumer),
                        self.lru.get(&owner).copied().unwrap_or(0),
                    )
                })
                .collect();
            candidates.sort_by(|a, b| {
                b.1.cmp(&a.1)
                    .then_with(|| a.2.cmp(&b.2))
                    .then_with(|| a.0.cmp(&b.0))
            });

            let mut remaining_deficit = deficit;
            let mut idx = 0usize;
            while remaining_deficit > 0 && idx < candidates.len() {
                let victim = candidates[idx].0;
                idx += 1;
                let mut trial = victims.clone();
                trial.push(victim);
                let before = projected_len(&victims);
                let after = projected_len(&trial);
                if after >= before {
                    continue;
                }
                let marginal = before.saturating_sub(after);
                victims.push(victim);
                remaining_deficit = remaining_deficit.saturating_sub(marginal);
            }

            if remaining_deficit > 0 {
                for (victim, _, _) in candidates {
                    if victims.contains(&victim) {
                        continue;
                    }
                    let mut trial = victims.clone();
                    trial.push(victim);
                    let before = projected_len(&victims);
                    let after = projected_len(&trial);
                    if after >= before {
                        continue;
                    }
                    let marginal = before.saturating_sub(after);
                    victims.push(victim);
                    remaining_deficit = remaining_deficit.saturating_sub(marginal);
                    if remaining_deficit == 0 {
                        break;
                    }
                }
            }

            if projected_len(&victims) <= self.cap {
                Some(victims)
            } else {
                None
            }
        }

        pub fn try_admit(
            &mut self,
            consumer: ConsumerId,
            owner: Pubkey,
            pubkeys: &[Pubkey],
        ) -> AdmissionResult {
            let deduped = dedupe_pubkeys(pubkeys);
            if deduped.is_empty() {
                return AdmissionResult::RejectedInvalidGroup;
            }
            let key = OwnerKey::new(consumer, owner);
            let snap = self.clone();

            if self.owner_groups.get(&key) == Some(&deduped) {
                self.lru_counter += 1;
                self.lru.insert(key, self.lru_counter);
                return AdmissionResult::OwnerAddedNoNewPubkey;
            }

            if consumer == ConsumerId::Wallet {
                let wallet_demand: HashSet<Pubkey> = self
                    .owner_groups
                    .iter()
                    .filter(|(o, _)| o.consumer == ConsumerId::Wallet && **o != key)
                    .flat_map(|(_, pks)| pks.iter().copied())
                    .chain(deduped.iter().copied())
                    .collect();
                if wallet_demand.len() > self.cap {
                    return AdmissionResult::RejectedProtected;
                }
            }

            let Some(victims) = self.plan_evictions(key, &deduped) else {
                return if consumer == ConsumerId::Wallet {
                    AdmissionResult::RejectedProtected
                } else {
                    AdmissionResult::RejectedCap
                };
            };

            if self.simulate_physical(&victims, key, &deduped).len() > self.cap {
                *self = snap;
                return if consumer == ConsumerId::Wallet {
                    AdmissionResult::RejectedProtected
                } else {
                    AdmissionResult::RejectedCap
                };
            }

            for victim in victims {
                self.owner_groups.remove(&victim);
                self.lru.remove(&victim);
            }
            self.owner_groups.insert(key, deduped);
            self.lru_counter += 1;
            self.lru.insert(key, self.lru_counter);
            AdmissionResult::Inserted {
                added_pubkeys: Vec::new(),
                evicted_owners: Vec::new(),
            }
        }

        pub fn remove_group(&mut self, consumer: ConsumerId, owner: Pubkey) -> bool {
            let key = OwnerKey::new(consumer, owner);
            if self.owner_groups.remove(&key).is_some() {
                self.lru.remove(&key);
                if self.len() > self.cap && !self.wallet_only_overflow() {
                    let _ = self.set_max_explicit_pubkeys(self.cap);
                }
                true
            } else {
                false
            }
        }

        pub fn touch_group(&mut self, consumer: ConsumerId, owner: Pubkey) -> bool {
            let key = OwnerKey::new(consumer, owner);
            if self.owner_groups.contains_key(&key) {
                self.lru_counter += 1;
                self.lru.insert(key, self.lru_counter);
                true
            } else {
                false
            }
        }

        fn marginal_free_for_shrink(&self, victim: OwnerKey) -> usize {
            let Some(pks) = self.owner_groups.get(&victim) else {
                return 0;
            };
            pks.iter().filter(|pk| self.owner_refcount(pk) == 1).count()
        }

        fn pick_shrink_victim(&self, deficit: usize) -> Option<OwnerKey> {
            let mut candidates: Vec<(OwnerKey, PinPriority, u64)> = self
                .owner_groups
                .keys()
                .copied()
                .filter(|o| o.consumer != ConsumerId::Wallet)
                .map(|o| {
                    (
                        o,
                        pin_priority_from_consumer(o.consumer),
                        self.lru.get(&o).copied().unwrap_or(0),
                    )
                })
                .collect();
            if candidates.is_empty() {
                return None;
            }
            candidates.sort_by(|a, b| {
                b.1.cmp(&a.1)
                    .then_with(|| a.2.cmp(&b.2))
                    .then_with(|| a.0.cmp(&b.0))
            });
            let mut best: Option<(OwnerKey, usize)> = None;
            for (victim, _, _) in candidates {
                let marginal = self.marginal_free_for_shrink(victim);
                if marginal == 0 {
                    continue;
                }
                let fits = marginal >= deficit;
                let better = match best {
                    None => true,
                    Some((_, best_marginal)) => {
                        if fits && best_marginal < deficit {
                            true
                        } else if fits == (best_marginal >= deficit) {
                            marginal < best_marginal
                        } else {
                            fits
                        }
                    }
                };
                if better {
                    best = Some((victim, marginal));
                }
            }
            best.map(|(victim, _)| victim)
        }

        pub fn set_max_explicit_pubkeys(&mut self, cap: usize) -> CapConvergeResult {
            let snapshot = self.clone();
            let previous_cap = self.cap;
            self.cap = cap.max(1);
            if self.wallet_only_overflow() {
                return CapConvergeResult::ProtectedOverflow;
            }
            let mut evicted = Vec::new();
            while self.len() > self.cap {
                let deficit = self.len().saturating_sub(self.cap);
                let Some(victim) = self.pick_shrink_victim(deficit) else {
                    if self.wallet_only_overflow() {
                        return CapConvergeResult::ProtectedOverflow;
                    }
                    *self = snapshot;
                    self.cap = previous_cap;
                    return CapConvergeResult::Unconverged;
                };
                self.owner_groups.remove(&victim);
                self.lru.remove(&victim);
                evicted.push(victim);
            }
            evicted.sort();
            CapConvergeResult::Converged {
                evicted_owners: evicted,
            }
        }
    }

    pub fn assert_equivalent(
        set: &AdmissionDesiredExplicitSet,
        reference: &NaiveRefModel,
        result: &AdmissionResult,
        ref_result: &AdmissionResult,
    ) {
        assert_eq!(
            admission_result_class(result),
            admission_result_class(ref_result),
            "result class mismatch: {result:?} vs {ref_result:?}"
        );

        if matches!(
            result,
            AdmissionResult::RejectedCap
                | AdmissionResult::RejectedProtected
                | AdmissionResult::RejectedInvalidGroup
        ) {
            return;
        }

        assert_eq!(set.snapshot_pubkeys(), reference.snapshot_pubkeys());
        assert_eq!(
            set.snapshot_owner_groups(),
            reference.snapshot_owner_groups()
        );
        assert_eq!(set.len(), reference.len());
        assert_eq!(set.max_explicit_pubkeys(), reference.max_explicit_pubkeys());
        for pk in set.snapshot_pubkeys() {
            assert_eq!(set.owner_refcount(&pk), reference.owner_refcount(&pk));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::reference::{assert_equivalent, NaiveRefModel};
    use super::*;
    use rand::seq::SliceRandom;
    use rand::SeedableRng;
    use rand::{rngs::StdRng, Rng};

    fn admit(
        set: &mut AdmissionDesiredExplicitSet,
        consumer: ConsumerId,
        owner: Pubkey,
        pubkeys: &[Pubkey],
    ) -> AdmissionResult {
        reset_planning_stats();
        set.try_admit_group(consumer, owner, pubkeys)
    }

    #[test]
    fn deduped_pubkeys_stay_within_cap() {
        let pk = Pubkey::new_unique();
        let mut set = AdmissionDesiredExplicitSet::new(2);
        let owner = Pubkey::new_unique();
        let result = admit(&mut set, ConsumerId::Tracker, owner, &[pk, pk, pk]);
        assert!(matches!(result, AdmissionResult::Inserted { .. }));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn rejected_group_does_not_mutate_state() {
        let mut set = AdmissionDesiredExplicitSet::new(10);
        let a = Pubkey::new_unique();
        let owner = Pubkey::new_unique();
        admit(&mut set, ConsumerId::Tracker, owner, &[a]);
        let before = set.snapshot_owner_groups();
        let before_len = set.len();

        let result = admit(&mut set, ConsumerId::Tracker, Pubkey::new_unique(), &[]);
        assert_eq!(result, AdmissionResult::RejectedInvalidGroup);
        assert_eq!(set.len(), before_len);
        assert_eq!(set.snapshot_owner_groups(), before);
    }

    #[test]
    fn insufficient_evictable_space_leaves_state_unchanged() {
        let mut set = AdmissionDesiredExplicitSet::new(2);
        let wallet_owner = Pubkey::new_unique();
        let w1 = Pubkey::new_unique();
        let w2 = Pubkey::new_unique();
        admit(&mut set, ConsumerId::Wallet, wallet_owner, &[w1, w2]);

        let tracker_owner = Pubkey::new_unique();
        let t1 = Pubkey::new_unique();
        let t2 = Pubkey::new_unique();
        let snap = set.clone();
        let result = admit(&mut set, ConsumerId::Tracker, tracker_owner, &[t1, t2]);
        assert_eq!(result, AdmissionResult::RejectedCap);
        assert_eq!(set.len(), snap.len());
        assert_eq!(set.snapshot_pubkeys(), snap.snapshot_pubkeys());
    }

    #[test]
    fn two_vault_group_admitted_or_rejected_atomically() {
        let mut set = AdmissionDesiredExplicitSet::new(1);
        let owner = Pubkey::new_unique();
        let v1 = Pubkey::new_unique();
        let v2 = Pubkey::new_unique();
        let result = admit(&mut set, ConsumerId::Arb, owner, &[v1, v2]);
        assert_eq!(result, AdmissionResult::RejectedCap);
        assert!(set.is_empty());

        let mut set = AdmissionDesiredExplicitSet::new(3);
        admit(
            &mut set,
            ConsumerId::Arb,
            Pubkey::new_unique(),
            &[Pubkey::new_unique()],
        );
        let result = admit(&mut set, ConsumerId::Arb, owner, &[v1, v2]);
        assert!(matches!(result, AdmissionResult::Inserted { .. }));
        assert!(set.contains(&v1));
        assert!(set.contains(&v2));
    }

    #[test]
    fn shared_pubkey_survives_single_owner_removal() {
        let mut set = AdmissionDesiredExplicitSet::new(10);
        let shared = Pubkey::new_unique();
        let o1 = Pubkey::new_unique();
        let o2 = Pubkey::new_unique();
        admit(&mut set, ConsumerId::Wallet, o1, &[shared]);
        admit(&mut set, ConsumerId::Momentum, o2, &[shared]);
        assert_eq!(set.len(), 1);
        assert!(set.remove_group(ConsumerId::Momentum, o2));
        assert!(set.contains(&shared));
        assert_eq!(set.owner_refcount(&shared), 1);
    }

    #[test]
    fn wallet_never_evicted() {
        let wallet_pk = Pubkey::new_unique();
        let mut set = AdmissionDesiredExplicitSet::new(2);
        admit(
            &mut set,
            ConsumerId::Wallet,
            Pubkey::new_unique(),
            &[wallet_pk],
        );
        for i in 0..5 {
            admit(
                &mut set,
                ConsumerId::Momentum,
                Pubkey::new_from_array([i; 32]),
                &[Pubkey::new_from_array([i + 10; 32])],
            );
        }
        assert!(set.contains(&wallet_pk));
    }

    #[test]
    fn momentum_evicts_tracker_before_arb() {
        let mut set = AdmissionDesiredExplicitSet::new(2);
        let tracker_pk = Pubkey::new_unique();
        let arb_pk = Pubkey::new_unique();
        admit(
            &mut set,
            ConsumerId::Tracker,
            Pubkey::new_unique(),
            &[tracker_pk],
        );
        admit(&mut set, ConsumerId::Arb, Pubkey::new_unique(), &[arb_pk]);
        let mom_pk = Pubkey::new_unique();
        admit(
            &mut set,
            ConsumerId::Momentum,
            Pubkey::new_unique(),
            &[mom_pk],
        );
        assert!(!set.contains(&tracker_pk));
        assert!(set.contains(&arb_pk));
        assert!(set.contains(&mom_pk));
    }

    #[test]
    fn arb_evicts_tracker_not_momentum_or_wallet() {
        let mut set = AdmissionDesiredExplicitSet::new(2);
        let wallet_pk = Pubkey::new_unique();
        let mom_pk = Pubkey::new_unique();
        let tracker_pk = Pubkey::new_unique();
        admit(
            &mut set,
            ConsumerId::Wallet,
            Pubkey::new_unique(),
            &[wallet_pk],
        );
        admit(
            &mut set,
            ConsumerId::Momentum,
            Pubkey::new_unique(),
            &[mom_pk],
        );
        let result = admit(
            &mut set,
            ConsumerId::Arb,
            Pubkey::new_unique(),
            &[tracker_pk],
        );
        assert_eq!(result, AdmissionResult::RejectedCap);
        assert!(set.contains(&wallet_pk));
        assert!(set.contains(&mom_pk));

        let mut set = AdmissionDesiredExplicitSet::new(2);
        admit(
            &mut set,
            ConsumerId::Tracker,
            Pubkey::new_unique(),
            &[tracker_pk],
        );
        admit(
            &mut set,
            ConsumerId::Momentum,
            Pubkey::new_unique(),
            &[mom_pk],
        );
        let arb_pk = Pubkey::new_unique();
        admit(&mut set, ConsumerId::Arb, Pubkey::new_unique(), &[arb_pk]);
        assert!(!set.contains(&tracker_pk));
        assert!(set.contains(&mom_pk));
        assert!(set.contains(&arb_pk));
    }

    #[test]
    fn tracker_only_evicts_tracker_or_rejected() {
        let mut set = AdmissionDesiredExplicitSet::new(2);
        let mom_pk = Pubkey::new_unique();
        let arb_pk = Pubkey::new_unique();
        admit(
            &mut set,
            ConsumerId::Momentum,
            Pubkey::new_unique(),
            &[mom_pk],
        );
        admit(&mut set, ConsumerId::Arb, Pubkey::new_unique(), &[arb_pk]);
        let result = admit(
            &mut set,
            ConsumerId::Tracker,
            Pubkey::new_unique(),
            &[Pubkey::new_unique()],
        );
        assert_eq!(result, AdmissionResult::RejectedCap);
        assert!(set.contains(&mom_pk));
        assert!(set.contains(&arb_pk));

        let mut set = AdmissionDesiredExplicitSet::new(1);
        let old_tracker = Pubkey::new_unique();
        admit(
            &mut set,
            ConsumerId::Tracker,
            Pubkey::new_unique(),
            &[old_tracker],
        );
        let new_tracker = Pubkey::new_unique();
        admit(
            &mut set,
            ConsumerId::Tracker,
            Pubkey::new_unique(),
            &[new_tracker],
        );
        assert!(!set.contains(&old_tracker));
        assert!(set.contains(&new_tracker));
    }

    #[test]
    fn same_priority_uses_lru_order() {
        let mut set = AdmissionDesiredExplicitSet::new(2);
        let first_pk = Pubkey::new_unique();
        let second_pk = Pubkey::new_unique();
        let first_owner = Pubkey::new_unique();
        let second_owner = Pubkey::new_unique();
        admit(&mut set, ConsumerId::Tracker, first_owner, &[first_pk]);
        admit(&mut set, ConsumerId::Tracker, second_owner, &[second_pk]);
        set.touch_group(ConsumerId::Tracker, first_owner);
        let new_pk = Pubkey::new_unique();
        admit(
            &mut set,
            ConsumerId::Tracker,
            Pubkey::new_unique(),
            &[new_pk],
        );
        assert!(set.contains(&first_pk));
        assert!(!set.contains(&second_pk));
        assert!(set.contains(&new_pk));
    }

    #[test]
    fn wallet_only_over_cap_protected_overflow() {
        let mut set = AdmissionDesiredExplicitSet::new(2);
        let w1 = Pubkey::new_unique();
        let w2 = Pubkey::new_unique();
        let w3 = Pubkey::new_unique();
        assert!(set.wallet_demand_exceeds_cap(&[w1, w2, w3]));
        let result = admit(
            &mut set,
            ConsumerId::Wallet,
            Pubkey::new_unique(),
            &[w1, w2, w3],
        );
        assert_eq!(result, AdmissionResult::RejectedProtected);
        assert!(set.is_empty());

        let mut set = AdmissionDesiredExplicitSet::new(3);
        admit(
            &mut set,
            ConsumerId::Wallet,
            Pubkey::new_unique(),
            &[w1, w2, w3],
        );
        assert_eq!(
            set.set_max_explicit_pubkeys(2),
            CapConvergeResult::ProtectedOverflow
        );
    }

    #[test]
    fn empty_group_rejected_invalid() {
        let mut set = AdmissionDesiredExplicitSet::new(5);
        assert_eq!(
            admit(&mut set, ConsumerId::Arb, Pubkey::new_unique(), &[]),
            AdmissionResult::RejectedInvalidGroup
        );
    }

    #[test]
    fn cap_shrink_converges_deterministically() {
        let mut set = AdmissionDesiredExplicitSet::new(5);
        for i in 0..5u8 {
            admit(
                &mut set,
                ConsumerId::Tracker,
                Pubkey::new_from_array([i; 32]),
                &[Pubkey::new_from_array([i + 50; 32])],
            );
        }
        let r1 = {
            let mut a = set.clone();
            a.set_max_explicit_pubkeys(2)
        };
        let r2 = {
            let mut b = set.clone();
            b.set_max_explicit_pubkeys(2)
        };
        assert_eq!(r1, r2);
        if let CapConvergeResult::Converged { evicted_owners } = r1 {
            assert_eq!(evicted_owners.len(), 3);
        } else {
            panic!("expected converge");
        }
    }

    #[test]
    fn set_max_shrink_mixed_priorities_enforces_cap_or_protected() {
        let mut set = AdmissionDesiredExplicitSet::new(6);
        admit(
            &mut set,
            ConsumerId::Wallet,
            Pubkey::new_unique(),
            &[Pubkey::new_unique()],
        );
        admit(
            &mut set,
            ConsumerId::Momentum,
            Pubkey::new_unique(),
            &[Pubkey::new_unique()],
        );
        admit(
            &mut set,
            ConsumerId::Arb,
            Pubkey::new_unique(),
            &[Pubkey::new_unique(), Pubkey::new_unique()],
        );
        admit(
            &mut set,
            ConsumerId::Tracker,
            Pubkey::new_unique(),
            &[Pubkey::new_unique(), Pubkey::new_unique()],
        );
        assert_eq!(set.len(), 6);

        let result = set.set_max_explicit_pubkeys(3);
        assert!(matches!(
            result,
            CapConvergeResult::Converged { .. } | CapConvergeResult::ProtectedOverflow
        ));
        assert!(set.len() <= set.max_explicit_pubkeys() || set.wallet_only_overflow());
        assert!(set.contains(&set.snapshot_pubkeys().iter().next().copied().unwrap()));
    }

    #[test]
    fn restore_preserves_shared_owner_refcounts() {
        let mut set = AdmissionDesiredExplicitSet::new(10);
        let shared = Pubkey::new_unique();
        let o1 = Pubkey::new_unique();
        let o2 = Pubkey::new_unique();
        admit(&mut set, ConsumerId::Wallet, o1, &[shared]);
        admit(&mut set, ConsumerId::Arb, o2, &[shared]);
        let snap = set.snapshot_owner_groups();
        let mut restored = AdmissionDesiredExplicitSet::new(10);
        restored.reconcile_from_owner_groups(&snap);
        assert_eq!(restored.owner_refcount(&shared), 2);
        assert_eq!(restored.len(), 1);
    }

    #[test]
    fn replacement_at_cap_drops_exclusive_outgoing_keys() {
        let mut set = AdmissionDesiredExplicitSet::new(2);
        let owner = Pubkey::new_unique();
        let a = Pubkey::new_unique();
        let b = Pubkey::new_unique();
        let c = Pubkey::new_unique();
        admit(&mut set, ConsumerId::Arb, owner, &[a, b]);
        admit(&mut set, ConsumerId::Arb, owner, &[b, c]);
        assert!(!set.contains(&a));
        assert!(set.contains(&b));
        assert!(set.contains(&c));
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn partial_overlap_replacement_and_victim_exact_end_capacity() {
        let mut set = AdmissionDesiredExplicitSet::new(3);
        let shared = Pubkey::new_unique();
        let owner_a = Pubkey::new_unique();
        let owner_b = Pubkey::new_unique();
        let x = Pubkey::new_unique();
        let y = Pubkey::new_unique();
        admit(&mut set, ConsumerId::Tracker, owner_a, &[shared, x]);
        admit(&mut set, ConsumerId::Tracker, owner_b, &[shared, y]);
        assert_eq!(set.len(), 3);
        let z = Pubkey::new_unique();
        admit(&mut set, ConsumerId::Arb, owner_a, &[shared, z]);
        assert_eq!(set.len(), 3);
        assert!(!set.contains(&x));
        assert!(set.contains(&shared));
        assert!(set.contains(&y));
        assert!(set.contains(&z));
    }

    #[test]
    fn priority_not_overridden_by_marginal_free_space() {
        let mut set = AdmissionDesiredExplicitSet::new(3);
        let big_tracker = (0..2)
            .map(|i| Pubkey::new_from_array([100 + i; 32]))
            .collect::<Vec<_>>();
        let small_arb = [Pubkey::new_unique()];
        admit(
            &mut set,
            ConsumerId::Tracker,
            Pubkey::new_unique(),
            &big_tracker,
        );
        admit(&mut set, ConsumerId::Arb, Pubkey::new_unique(), &small_arb);
        let mom = [Pubkey::new_unique()];
        admit(&mut set, ConsumerId::Momentum, Pubkey::new_unique(), &mom);
        for pk in &big_tracker {
            assert!(!set.contains(pk));
        }
        assert!(set.contains(&small_arb[0]));
        assert!(set.contains(&mom[0]));
    }

    #[test]
    fn no_eviction_fast_path_zero_full_graph_copies() {
        let mut set = AdmissionDesiredExplicitSet::new(100);
        for i in 0..90u8 {
            admit(
                &mut set,
                ConsumerId::Tracker,
                Pubkey::new_from_array([i; 32]),
                &[Pubkey::new_from_array([i + 1; 32])],
            );
        }
        reset_planning_stats();
        let owner = Pubkey::new_unique();
        let pks: Vec<_> = (0..3).map(|_| Pubkey::new_unique()).collect();
        let result = set.try_admit_group(ConsumerId::Tracker, owner, &pks);
        let stats = take_planning_stats();
        assert!(matches!(result, AdmissionResult::Inserted { .. }));
        assert_eq!(stats.full_graph_copies, 0);
        assert!(stats.owner_edge_visits <= pks.len() * 4 + 8);
        assert!(set.len() <= set.max_explicit_pubkeys());
    }

    #[test]
    fn shared_momentum_evicts_only_zero_marginal_tracker_b() {
        let shared = Pubkey::new_unique();
        let x = Pubkey::new_unique();
        let y = Pubkey::new_unique();
        let tracker_a = Pubkey::new_unique();
        let tracker_b = Pubkey::new_unique();
        let momentum_owner = Pubkey::new_unique();

        let mut set = AdmissionDesiredExplicitSet::new(2);
        admit(&mut set, ConsumerId::Tracker, tracker_a, &[shared]);
        admit(&mut set, ConsumerId::Tracker, tracker_b, &[x]);
        assert_eq!(set.len(), 2);

        let result = admit(&mut set, ConsumerId::Momentum, momentum_owner, &[shared, y]);
        assert!(matches!(result, AdmissionResult::Inserted { .. }));
        assert_eq!(set.len(), 2);
        assert!(set.contains(&shared));
        assert!(set.contains(&y));
        assert!(!set.contains(&x));
        assert!(set
            .owner_groups
            .contains_key(&OwnerKey::new(ConsumerId::Tracker, tracker_a)));
        assert!(!set
            .owner_groups
            .contains_key(&OwnerKey::new(ConsumerId::Tracker, tracker_b)));
    }

    #[test]
    fn fully_shared_cap_shrink_requires_joint_group_removal() {
        let shared = Pubkey::new_unique();
        let a = Pubkey::new_unique();
        let owner_a = Pubkey::new_unique();
        let owner_b = Pubkey::new_unique();
        let mut set = AdmissionDesiredExplicitSet::new(2);
        admit(&mut set, ConsumerId::Tracker, owner_a, &[shared, a]);
        admit(&mut set, ConsumerId::Tracker, owner_b, &[shared]);
        assert_eq!(set.len(), 2);

        let result = set.set_max_explicit_pubkeys(1);
        assert!(matches!(result, CapConvergeResult::Converged { .. }));
        assert_eq!(set.len(), 1);
        assert!(set.contains(&shared));
        assert_eq!(set.owner_refcount(&shared), 1);
        assert_eq!(set.snapshot_owner_groups().len(), 1);
        assert!(!set
            .owner_groups
            .contains_key(&OwnerKey::new(ConsumerId::Tracker, owner_a)));
    }

    #[test]
    fn lru_renormalizes_before_wrap_preserving_order() {
        let mut set = AdmissionDesiredExplicitSet::new(10);
        let o1 = Pubkey::new_unique();
        let o2 = Pubkey::new_unique();
        let p1 = Pubkey::new_unique();
        let p2 = Pubkey::new_unique();
        admit(&mut set, ConsumerId::Tracker, o1, &[p1]);
        admit(&mut set, ConsumerId::Tracker, o2, &[p2]);
        set.touch_group(ConsumerId::Tracker, o1);

        let mut near = AdmissionDesiredExplicitSet::new_for_test(10, LRU_RENORMALIZE_THRESHOLD);
        admit(&mut near, ConsumerId::Tracker, o1, &[p1]);
        admit(&mut near, ConsumerId::Tracker, o2, &[p2]);
        near.touch_group(ConsumerId::Tracker, o1);
        assert!(near.touch_group(ConsumerId::Tracker, o2));

        let mut set = AdmissionDesiredExplicitSet::new(2);
        admit(&mut set, ConsumerId::Tracker, o1, &[p1]);
        admit(&mut set, ConsumerId::Tracker, o2, &[p2]);
        set.touch_group(ConsumerId::Tracker, o1);
        let victim_pk = Pubkey::new_unique();
        admit(
            &mut set,
            ConsumerId::Tracker,
            Pubkey::new_unique(),
            &[victim_pk],
        );
        assert!(set.contains(&p1));
        assert!(!set.contains(&p2));
    }

    #[test]
    fn cap_overflow_saturates_at_i64_max() {
        let mut set = AdmissionDesiredExplicitSet::new(1);
        let huge_len = i64::MAX as usize + 10;
        set.cap = 1;
        for i in 0..huge_len.min(1000) {
            admit(
                &mut set,
                ConsumerId::Tracker,
                Pubkey::new_from_array([(i % 255) as u8; 32]),
                &[Pubkey::new_from_array([((i + 1) % 255) as u8; 32])],
            );
        }
        let overflow = set.cap_overflow();
        assert!(overflow >= 0);
        if set.len() > set.cap && (set.len() - set.cap) > i64::MAX as usize {
            assert_eq!(overflow, i64::MAX);
        }
    }

    #[test]
    fn cap_overflow_usize_max_edge() {
        let mut set = AdmissionDesiredExplicitSet::new(usize::MAX);
        let pk = Pubkey::new_unique();
        admit(&mut set, ConsumerId::Tracker, Pubkey::new_unique(), &[pk]);
        assert_eq!(set.cap_overflow(), 0);
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn deterministic_reference_equivalence_seeded_sequences() {
        for seed in [1_u64, 42, 2_915_510_010] {
            run_reference_sequence(seed);
        }
    }

    fn run_reference_sequence(seed: u64) {
        let mut rng = StdRng::seed_from_u64(seed);
        let mut set = AdmissionDesiredExplicitSet::new(8);
        let mut reference = NaiveRefModel::new(8);
        let mut pool: Vec<Pubkey> = (0..16)
            .map(|i| Pubkey::new_from_array([i as u8; 32]))
            .collect();

        for step in 0..300 {
            let op = rng.gen_range(0..100);
            let mut last_op = "unknown";
            match op {
                0..=45 => {
                    last_op = "admit";
                    let consumer = match rng.gen_range(0..4) {
                        0 => ConsumerId::Wallet,
                        1 => ConsumerId::Momentum,
                        2 => ConsumerId::Arb,
                        _ => ConsumerId::Tracker,
                    };
                    let owner = Pubkey::new_unique();
                    let count = rng.gen_range(1..=4);
                    let mut pubkeys = Vec::new();
                    for _ in 0..count {
                        if rng.gen_bool(0.35) && !pool.is_empty() {
                            pubkeys.push(pool[rng.gen_range(0..pool.len())]);
                        } else {
                            let pk = Pubkey::new_unique();
                            pool.push(pk);
                            pubkeys.push(pk);
                        }
                    }
                    if rng.gen_bool(0.2) {
                        if let Some(g) = set.snapshot_owner_groups().choose(&mut rng) {
                            let owner = g.owner.owner;
                            let consumer = g.owner.consumer;
                            let snap = set.clone();
                            let ref_snap = reference.clone();
                            reset_planning_stats();
                            let result = set.try_admit_group(consumer, owner, &pubkeys);
                            let ref_result = reference.try_admit(consumer, owner, &pubkeys);
                            if matches!(
                                result,
                                AdmissionResult::RejectedCap
                                    | AdmissionResult::RejectedProtected
                                    | AdmissionResult::RejectedInvalidGroup
                            ) {
                                assert_eq!(set.snapshot_pubkeys(), snap.snapshot_pubkeys());
                                assert_eq!(
                                    set.snapshot_owner_groups(),
                                    snap.snapshot_owner_groups()
                                );
                                assert_eq!(
                                    reference.snapshot_pubkeys(),
                                    ref_snap.snapshot_pubkeys()
                                );
                            } else {
                                assert_equivalent(&set, &reference, &result, &ref_result);
                            }
                            continue;
                        }
                    }
                    let snap = set.clone();
                    let ref_snap = reference.clone();
                    reset_planning_stats();
                    let result = set.try_admit_group(consumer, owner, &pubkeys);
                    let ref_result = reference.try_admit(consumer, owner, &pubkeys);
                    if matches!(
                        result,
                        AdmissionResult::RejectedCap
                            | AdmissionResult::RejectedProtected
                            | AdmissionResult::RejectedInvalidGroup
                    ) {
                        assert_eq!(set.snapshot_pubkeys(), snap.snapshot_pubkeys());
                        assert_eq!(set.snapshot_owner_groups(), snap.snapshot_owner_groups());
                        assert_eq!(reference.snapshot_pubkeys(), ref_snap.snapshot_pubkeys());
                    } else {
                        assert_equivalent(&set, &reference, &result, &ref_result);
                    }
                }
                46..=60 => {
                    last_op = "remove";
                    if let Some(g) = set.snapshot_owner_groups().choose(&mut rng) {
                        let snap = set.clone();
                        let ref_snap = reference.clone();
                        let removed = set.remove_group(g.owner.consumer, g.owner.owner);
                        let ref_removed = reference.remove_group(g.owner.consumer, g.owner.owner);
                        assert_eq!(removed, ref_removed);
                        if removed {
                            assert_eq!(set.snapshot_pubkeys(), reference.snapshot_pubkeys());
                            assert_eq!(
                                set.snapshot_owner_groups(),
                                reference.snapshot_owner_groups()
                            );
                        } else {
                            assert_eq!(set.snapshot_pubkeys(), snap.snapshot_pubkeys());
                            assert_eq!(reference.snapshot_pubkeys(), ref_snap.snapshot_pubkeys());
                        }
                    }
                }
                61..=75 => {
                    last_op = "touch";
                    if let Some(g) = set.snapshot_owner_groups().choose(&mut rng) {
                        let touched = set.touch_group(g.owner.consumer, g.owner.owner);
                        let ref_touched = reference.touch_group(g.owner.consumer, g.owner.owner);
                        assert_eq!(touched, ref_touched);
                        assert_eq!(set.snapshot_pubkeys(), reference.snapshot_pubkeys());
                        assert_eq!(
                            set.snapshot_owner_groups(),
                            reference.snapshot_owner_groups()
                        );
                    }
                }
                _ => {
                    last_op = "set_max";
                    let new_cap = rng.gen_range(2..12);
                    let snap = set.clone();
                    let ref_snap = reference.clone();
                    let result = set.set_max_explicit_pubkeys(new_cap);
                    let ref_result = reference.set_max_explicit_pubkeys(new_cap);
                    assert_eq!(
                        std::mem::discriminant(&result),
                        std::mem::discriminant(&ref_result)
                    );
                    if matches!(result, CapConvergeResult::ProtectedOverflow) {
                        assert_eq!(set.max_explicit_pubkeys(), new_cap.max(1));
                        assert_eq!(set.snapshot_pubkeys(), snap.snapshot_pubkeys());
                        assert_eq!(reference.snapshot_pubkeys(), ref_snap.snapshot_pubkeys());
                    } else if matches!(result, CapConvergeResult::Unconverged) {
                        assert_eq!(set.max_explicit_pubkeys(), snap.max_explicit_pubkeys());
                        assert_eq!(set.snapshot_pubkeys(), snap.snapshot_pubkeys());
                        assert_eq!(reference.snapshot_pubkeys(), ref_snap.snapshot_pubkeys());
                    } else {
                        assert!(
                            set.len() <= set.max_explicit_pubkeys() || set.wallet_only_overflow()
                        );
                        assert_eq!(set.snapshot_pubkeys(), reference.snapshot_pubkeys());
                        assert_eq!(
                            set.snapshot_owner_groups(),
                            reference.snapshot_owner_groups()
                        );
                    }
                }
            }
            if set.len() > set.max_explicit_pubkeys() && !set.wallet_only_overflow() {
                panic!(
                    "seed={seed} step={step} op={last_op} len={} cap={} wallet_only={}",
                    set.len(),
                    set.max_explicit_pubkeys(),
                    set.wallet_only_overflow()
                );
            }
        }
    }

    #[test]
    fn partial_overlap_planning_stats_bounded_edges() {
        let mut set = AdmissionDesiredExplicitSet::new(4);
        let shared = Pubkey::new_unique();
        let a = Pubkey::new_unique();
        let b = Pubkey::new_unique();
        let c = Pubkey::new_unique();
        let d = Pubkey::new_unique();
        admit(
            &mut set,
            ConsumerId::Tracker,
            Pubkey::new_unique(),
            &[shared, a],
        );
        admit(
            &mut set,
            ConsumerId::Tracker,
            Pubkey::new_unique(),
            &[shared, b],
        );
        admit(&mut set, ConsumerId::Arb, Pubkey::new_unique(), &[c]);
        reset_planning_stats();
        let result = set.try_admit_group(ConsumerId::Momentum, Pubkey::new_unique(), &[shared, d]);
        let stats = take_planning_stats();
        assert!(matches!(result, AdmissionResult::Inserted { .. }));
        assert_eq!(stats.full_graph_copies, 0);
        assert!(stats.owner_edge_visits <= 24);
        assert!(stats.candidate_pops <= 8);
    }
}
