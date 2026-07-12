//! Pure explicit-account admission state machine (foundation slice).
//!
//! Deterministic cap, group atomicity, consumer priority, shared-pubkey refcounts,
//! and LRU eviction — no RPC, async, or production wiring.

use solana_sdk::pubkey::Pubkey;
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

/// Admission-managed explicit pubkey set — distinct from legacy `DesiredExplicitSet`.
#[derive(Debug, Clone)]
pub struct AdmissionDesiredExplicitSet {
    cap: usize,
    pubkeys: HashMap<Pubkey, PubkeyEntry>,
    owner_groups: HashMap<OwnerKey, OwnerGroupState>,
    lru_counter: u64,
}

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

    pub fn len(&self) -> usize {
        self.pubkeys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pubkeys.is_empty()
    }

    pub fn max_explicit_pubkeys(&self) -> usize {
        self.cap
    }

    pub fn set_max_explicit_pubkeys(&mut self, cap: usize) -> CapConvergeResult {
        self.cap = cap.max(1);
        let result = self.converge_to_cap_observed(&mut NoopAdmissionObserver);
        debug_assert!(
            self.len() <= self.cap
                || self.wallet_only_overflow()
                || matches!(
                    result,
                    CapConvergeResult::ProtectedOverflow | CapConvergeResult::Unconverged
                )
        );
        result
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
        let deduped = dedupe_pubkeys(wallet_pubkeys);
        deduped.len() > self.cap
    }

    pub fn cap_overflow(&self) -> i64 {
        self.len() as i64 - self.cap as i64
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
            self.touch_owner(key);
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

        self.insert_owner_pubkeys(key, &deduped);
        self.touch_owner(key);

        let mut added: Vec<_> = plan.added_pubkeys.into_iter().collect();
        added.sort();
        let mut evicted = plan.evict_owners;
        evicted.sort();

        AdmissionResult::Inserted {
            added_pubkeys: added,
            evicted_owners: evicted,
        }
    }

    pub fn remove_group(&mut self, consumer: ConsumerId, owner: Pubkey) -> bool {
        let key = OwnerKey::new(consumer, owner);
        if self.owner_groups.contains_key(&key) {
            self.remove_group_internal(key);
            true
        } else {
            false
        }
    }

    /// Re-admit / touch an existing owner group to refresh LRU without changing pubkeys.
    pub fn touch_group(&mut self, consumer: ConsumerId, owner: Pubkey) -> bool {
        let key = OwnerKey::new(consumer, owner);
        if self.owner_groups.contains_key(&key) {
            self.touch_owner(key);
            true
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
            let Some(victim) = self.pick_shrink_victim() else {
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

    /// Restore owner groups in deterministic order using the same admission semantics.
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

    /// Replace all state from owner groups (clear + restore).
    pub fn reconcile_from_owner_groups(&mut self, groups: &[OwnerGroupSnapshot]) -> RestoreResult {
        self.pubkeys.clear();
        self.owner_groups.clear();
        self.lru_counter = 0;
        self.restore_owner_groups(groups)
    }

    // --- internal ---

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

    pub fn owner_refcount(&self, pubkey: &Pubkey) -> usize {
        self.pubkeys
            .get(pubkey)
            .map(|e| e.owners.len())
            .unwrap_or(0)
    }

    fn plan_admit(
        &self,
        key: OwnerKey,
        new_pubkeys: &HashSet<Pubkey>,
    ) -> Result<AdmitPlan, AdmissionResult> {
        let old_pubkeys = self.owner_groups.get(&key).map(|g| g.pubkeys.clone());

        if old_pubkeys.as_ref() == Some(new_pubkeys) {
            return Ok(AdmitPlan {
                added_pubkeys: HashSet::new(),
                evict_owners: Vec::new(),
                replace_old_pubkeys: None,
                touch_existing: true,
            });
        }

        let current_physical = self.snapshot_pubkeys();
        let projected = self.simulate_physical(&[], key, new_pubkeys);
        let added_pubkeys: HashSet<Pubkey> =
            projected.difference(&current_physical).copied().collect();

        let deficit = projected.len().saturating_sub(self.cap);
        if deficit == 0 {
            return Ok(AdmitPlan {
                added_pubkeys,
                evict_owners: Vec::new(),
                replace_old_pubkeys: old_pubkeys,
                touch_existing: false,
            });
        }

        if key.consumer == ConsumerId::Wallet {
            let wallet_demand: HashSet<Pubkey> = self
                .owner_groups
                .iter()
                .filter(|(owner, _)| owner.consumer == ConsumerId::Wallet && **owner != key)
                .flat_map(|(_, g)| g.pubkeys.iter().copied())
                .chain(new_pubkeys.iter().copied())
                .collect();
            if wallet_demand.len() > self.cap {
                return Err(AdmissionResult::RejectedProtected);
            }
        }

        let evict_owners = self.plan_evictions(key.consumer, key, deficit, new_pubkeys)?;
        let after_evict = self.simulate_physical(&evict_owners, key, new_pubkeys);
        if after_evict.len() > self.cap {
            if key.consumer == ConsumerId::Wallet {
                return Err(AdmissionResult::RejectedProtected);
            }
            return Err(AdmissionResult::RejectedCap);
        }

        Ok(AdmitPlan {
            added_pubkeys,
            evict_owners,
            replace_old_pubkeys: old_pubkeys,
            touch_existing: false,
        })
    }

    pub fn set_cap(&mut self, cap: usize) {
        self.cap = cap.max(1);
    }

    fn simulate_physical(
        &self,
        evictions: &[OwnerKey],
        incoming: OwnerKey,
        new_pubkeys: &HashSet<Pubkey>,
    ) -> HashSet<Pubkey> {
        let mut groups: HashMap<OwnerKey, HashSet<Pubkey>> = self
            .owner_groups
            .iter()
            .map(|(owner, state)| (*owner, state.pubkeys.clone()))
            .collect();

        for victim in evictions {
            groups.remove(victim);
        }

        groups.insert(incoming, new_pubkeys.clone());

        let mut physical = HashSet::new();
        for pubkeys in groups.values() {
            physical.extend(pubkeys.iter().copied());
        }
        physical
    }

    fn plan_evictions(
        &self,
        incoming_consumer: ConsumerId,
        incoming_owner: OwnerKey,
        deficit: usize,
        new_pubkeys: &HashSet<Pubkey>,
    ) -> Result<Vec<OwnerKey>, AdmissionResult> {
        let mut victims = Vec::new();
        let mut candidates: Vec<(OwnerKey, PinPriority, u64)> = self
            .owner_groups
            .iter()
            .filter(|(owner, _)| {
                **owner != incoming_owner
                    && Self::can_evict_for_incoming(owner.consumer, incoming_consumer)
            })
            .map(|(owner, state)| (*owner, Self::pin_priority(owner.consumer), state.lru_stamp))
            .collect();

        candidates.sort_by(|a, b| {
            b.1.cmp(&a.1)
                .then_with(|| a.2.cmp(&b.2))
                .then_with(|| a.0.cmp(&b.0))
        });

        let mut current_len = self
            .simulate_physical(&[], incoming_owner, new_pubkeys)
            .len();
        for (victim, _, _) in candidates {
            if current_len <= self.cap {
                break;
            }
            victims.push(victim);
            current_len = self
                .simulate_physical(&victims, incoming_owner, new_pubkeys)
                .len();
        }

        if current_len > self.cap {
            return Err(AdmissionResult::RejectedCap);
        }
        let _ = deficit;
        Ok(victims)
    }

    fn pick_shrink_victim(&self) -> Option<OwnerKey> {
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
        candidates.first().map(|(k, _, _)| *k)
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

    fn touch_owner(&mut self, key: OwnerKey) {
        self.lru_counter += 1;
        if let Some(group) = self.owner_groups.get_mut(&key) {
            group.lru_stamp = self.lru_counter;
        }
    }

    fn insert_owner_pubkeys(&mut self, key: OwnerKey, pubkeys: &HashSet<Pubkey>) {
        self.lru_counter += 1;
        let stamp = self.lru_counter;
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

// --- reference model for stress tests ---

#[cfg(test)]
mod reference {
    use super::*;
    use std::collections::BTreeMap;

    #[derive(Clone)]
    pub struct RefSet {
        cap: usize,
        owners: BTreeMap<OwnerKey, HashSet<Pubkey>>,
        lru: BTreeMap<OwnerKey, u64>,
        lru_counter: u64,
    }

    impl RefSet {
        pub fn new(cap: usize) -> Self {
            Self {
                cap: cap.max(1),
                owners: BTreeMap::new(),
                lru: BTreeMap::new(),
                lru_counter: 0,
            }
        }

        pub fn len(&self) -> usize {
            let mut keys = HashSet::new();
            for pks in self.owners.values() {
                keys.extend(pks.iter().copied());
            }
            keys.len()
        }

        fn refcount(&self, pk: &Pubkey) -> usize {
            self.owners
                .iter()
                .filter(|(_, pks)| pks.contains(pk))
                .count()
        }

        fn touch(&mut self, key: OwnerKey) {
            self.lru_counter += 1;
            self.lru.insert(key, self.lru_counter);
        }

        pub fn set_cap(&mut self, cap: usize) {
            self.cap = cap.max(1);
        }

        fn simulate_physical(
            &self,
            evictions: &[OwnerKey],
            incoming: OwnerKey,
            new_pubkeys: &HashSet<Pubkey>,
        ) -> HashSet<Pubkey> {
            let mut groups: BTreeMap<OwnerKey, HashSet<Pubkey>> = self.owners.clone();
            for victim in evictions {
                groups.remove(victim);
            }
            groups.insert(incoming, new_pubkeys.clone());
            let mut physical = HashSet::new();
            for pubkeys in groups.values() {
                physical.extend(pubkeys.iter().copied());
            }
            physical
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
            let snapshot = self.clone();

            if self.owners.get(&key) == Some(&deduped) {
                self.touch(key);
                return AdmissionResult::OwnerAddedNoNewPubkey;
            }

            let projected = self.simulate_physical(&[], key, &deduped);
            if projected.len() > self.cap {
                if consumer == ConsumerId::Wallet {
                    let wallet_demand: HashSet<Pubkey> = self
                        .owners
                        .iter()
                        .filter(|(owner, _)| owner.consumer == ConsumerId::Wallet && **owner != key)
                        .flat_map(|(_, g)| g.iter().copied())
                        .chain(deduped.iter().copied())
                        .collect();
                    if wallet_demand.len() > self.cap {
                        return AdmissionResult::RejectedProtected;
                    }
                }

                let victims = self.plan_evictions(consumer, key, &deduped);
                if victims.is_empty() {
                    return if consumer == ConsumerId::Wallet {
                        AdmissionResult::RejectedProtected
                    } else {
                        AdmissionResult::RejectedCap
                    };
                }
                let after = self.simulate_physical(&victims, key, &deduped);
                if after.len() > self.cap {
                    *self = snapshot;
                    return if consumer == ConsumerId::Wallet {
                        AdmissionResult::RejectedProtected
                    } else {
                        AdmissionResult::RejectedCap
                    };
                }
                for victim in victims {
                    self.owners.remove(&victim);
                    self.lru.remove(&victim);
                }
            }

            self.owners.insert(key, deduped);
            self.touch(key);
            AdmissionResult::Inserted {
                added_pubkeys: Vec::new(),
                evicted_owners: Vec::new(),
            }
        }

        fn plan_evictions(
            &self,
            incoming_consumer: ConsumerId,
            incoming_owner: OwnerKey,
            new_pubkeys: &HashSet<Pubkey>,
        ) -> Vec<OwnerKey> {
            let mut victims = Vec::new();
            let mut candidates: Vec<(OwnerKey, PinPriority, u64)> = self
                .owners
                .keys()
                .copied()
                .filter(|owner| {
                    *owner != incoming_owner
                        && AdmissionDesiredExplicitSet::can_evict_for_incoming(
                            owner.consumer,
                            incoming_consumer,
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

            let mut current_len = self
                .simulate_physical(&[], incoming_owner, new_pubkeys)
                .len();
            for (victim, _, _) in candidates {
                if current_len <= self.cap {
                    break;
                }
                victims.push(victim);
                current_len = self
                    .simulate_physical(&victims, incoming_owner, new_pubkeys)
                    .len();
            }
            victims
        }

        pub fn remove_group(&mut self, consumer: ConsumerId, owner: Pubkey) -> bool {
            let key = OwnerKey::new(consumer, owner);
            if self.owners.remove(&key).is_some() {
                self.lru.remove(&key);
                true
            } else {
                false
            }
        }

        #[allow(dead_code)]
        fn refcounts_match(&self, other: &AdmissionDesiredExplicitSet) -> bool {
            for (pk, entry) in &other.pubkeys {
                if self.refcount(pk) != entry.owners.len() {
                    return false;
                }
            }
            for pk in other.snapshot_pubkeys() {
                if self.refcount(&pk) != other.owner_refcount(&pk) {
                    return false;
                }
            }
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::reference::RefSet;
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
    fn no_eviction_fast_path_bounded_work() {
        let mut set = AdmissionDesiredExplicitSet::new(100);
        for i in 0..90u8 {
            admit(
                &mut set,
                ConsumerId::Tracker,
                Pubkey::new_from_array([i; 32]),
                &[Pubkey::new_from_array([i + 1; 32])],
            );
        }
        let owner = Pubkey::new_unique();
        let pks: Vec<_> = (0..3).map(|_| Pubkey::new_unique()).collect();
        let result = admit(&mut set, ConsumerId::Tracker, owner, &pks);
        assert!(matches!(result, AdmissionResult::Inserted { .. }));
        assert!(set.len() <= set.max_explicit_pubkeys());
    }

    #[test]
    fn arb_rejected_when_wallet_fills_cap() {
        let mut set = AdmissionDesiredExplicitSet::new(4);
        let wallet_pks: Vec<_> = (0..4).map(|_| Pubkey::new_unique()).collect();
        admit(
            &mut set,
            ConsumerId::Wallet,
            Pubkey::new_unique(),
            &wallet_pks,
        );
        assert_eq!(set.len(), 4);
        let arb_pks: Vec<_> = (0..4).map(|_| Pubkey::new_unique()).collect();
        let result = admit(&mut set, ConsumerId::Arb, Pubkey::new_unique(), &arb_pks);
        assert_eq!(result, AdmissionResult::RejectedCap);
        assert_eq!(set.len(), 4);
    }

    #[test]
    fn cap_shrink_evicts_lower_priority_groups() {
        let mut set = AdmissionDesiredExplicitSet::new(8);
        let wallet_pks: Vec<_> = (0..4).map(|_| Pubkey::new_unique()).collect();
        admit(
            &mut set,
            ConsumerId::Wallet,
            Pubkey::new_unique(),
            &wallet_pks,
        );
        let arb_pks: Vec<_> = (0..4).map(|_| Pubkey::new_unique()).collect();
        admit(&mut set, ConsumerId::Arb, Pubkey::new_unique(), &arb_pks);
        assert_eq!(set.len(), 8);
        let result = set.set_max_explicit_pubkeys(4);
        assert!(matches!(result, CapConvergeResult::Converged { .. }));
        assert_eq!(set.len(), 4);
        assert!(!set
            .snapshot_owner_groups()
            .iter()
            .any(|g| g.owner.consumer == ConsumerId::Arb));
    }

    #[test]
    fn stress_random_ops_match_reference_and_invariants() {
        let mut rng = StdRng::seed_from_u64(2_915_510_010);
        let mut set = AdmissionDesiredExplicitSet::new(12);
        let mut reference = RefSet::new(12);

        for step in 0..400 {
            let op = rng.gen_range(0..100);
            let mut op_name = "unknown";
            match op {
                0..=55 => {
                    op_name = "admit";
                    let consumer = match rng.gen_range(0..4) {
                        0 => ConsumerId::Wallet,
                        1 => ConsumerId::Momentum,
                        2 => ConsumerId::Arb,
                        _ => ConsumerId::Tracker,
                    };
                    let owner = Pubkey::new_unique();
                    let count = rng.gen_range(1..=4);
                    let pubkeys: Vec<_> = (0..count).map(|_| Pubkey::new_unique()).collect();
                    let snap = set.clone();
                    let result = set.try_admit_group(consumer, owner, &pubkeys);
                    let ref_result = reference.try_admit(consumer, owner, &pubkeys);
                    match (&result, &ref_result) {
                        (
                            AdmissionResult::RejectedCap | AdmissionResult::RejectedProtected,
                            AdmissionResult::RejectedCap | AdmissionResult::RejectedProtected,
                        ) => {
                            assert_eq!(set.snapshot_pubkeys(), snap.snapshot_pubkeys());
                        }
                        _ => {}
                    }
                    if !matches!(
                        result,
                        AdmissionResult::RejectedCap
                            | AdmissionResult::RejectedProtected
                            | AdmissionResult::RejectedInvalidGroup
                    ) {
                        assert!(
                            set.len() <= set.max_explicit_pubkeys() || set.wallet_only_overflow(),
                            "admit left over-cap state: {result:?}"
                        );
                    }
                }
                56..=75 => {
                    op_name = "remove";
                    if let Some((consumer, owner)) = set
                        .snapshot_owner_groups()
                        .choose(&mut rng)
                        .map(|g| (g.owner.consumer, g.owner.owner))
                    {
                        set.remove_group(consumer, owner);
                        reference.remove_group(consumer, owner);
                    }
                }
                76..=85 => {
                    op_name = "set_max";
                    let new_cap = rng.gen_range(4..16);
                    let converge = set.set_max_explicit_pubkeys(new_cap);
                    reference.set_cap(set.max_explicit_pubkeys());
                    if set.len() > set.max_explicit_pubkeys() && !set.wallet_only_overflow() {
                        panic!(
                            "set_max failed to enforce cap: {:?} len={} cap={}",
                            converge,
                            set.len(),
                            set.max_explicit_pubkeys()
                        );
                    }
                    let _ = set.converge_to_cap();
                }
                _ => {
                    op_name = "touch";
                    if let Some(g) = set.snapshot_owner_groups().choose(&mut rng) {
                        set.touch_group(g.owner.consumer, g.owner.owner);
                    }
                }
            }

            if set.len() > set.max_explicit_pubkeys() && !set.wallet_only_overflow() {
                let _ = set.converge_to_cap();
            }

            if set.len() > set.max_explicit_pubkeys() && !set.wallet_only_overflow() {
                panic!(
                    "step={step} op={op_name} over cap without protected wallet overflow: len={} cap={} wallet_only={} groups={:?}",
                    set.len(),
                    set.max_explicit_pubkeys(),
                    set.wallet_only_overflow(),
                    set.snapshot_owner_groups()
                );
            }
            assert!(set.len() <= set.max_explicit_pubkeys() || set.wallet_only_overflow());
            assert!(set.cap_overflow() <= 0 || set.wallet_only_overflow());
            for group in set.snapshot_owner_groups() {
                for pk in &group.pubkeys {
                    assert!(set.contains(pk));
                    assert!(set.owner_refcount(pk) >= 1);
                }
            }
            let _ = step;
        }
    }
}
