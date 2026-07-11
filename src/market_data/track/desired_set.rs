//! Hard admission SSOT for explicit Geyser subscription pubkeys (I-MD-7 / I-MD-8).

use solana_sdk::pubkey::Pubkey;
use std::collections::{HashMap, HashSet};

/// Consumer tag for explicit Geyser pubkey ownership.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

/// Owner reference for shared-pubkey refcounting (pool / wallet / standalone mint).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum OwnerKey {
    Wallet,
    Pool(Pubkey),
    Mint(Pubkey),
}

/// Result of attempting to admit an owner group atomically.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionResult {
    Admitted { new_pubkeys: usize },
    OwnerAddedNoNewPubkey,
    RejectedCap,
    RejectedProtected,
}

/// Reason recorded when a group is evicted from the desired set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictReason {
    HigherPriority,
    SamePriorityLru,
    CapShrink,
    RestoreConverge,
}

#[derive(Debug, Clone)]
struct PubkeyEntry {
    owners: HashSet<(ConsumerId, OwnerKey)>,
}

#[derive(Debug, Clone)]
struct OwnerGroup {
    consumer: ConsumerId,
    owner: OwnerKey,
    pubkeys: HashSet<Pubkey>,
    admitted_seq: u64,
}

#[derive(Debug, Clone)]
pub struct DesiredExplicitSet {
    entries: HashMap<Pubkey, PubkeyEntry>,
    groups: HashMap<(ConsumerId, OwnerKey), OwnerGroup>,
    by_consumer: HashMap<ConsumerId, HashSet<Pubkey>>,
    max_explicit_pubkeys: usize,
    next_seq: u64,
}

impl Default for DesiredExplicitSet {
    fn default() -> Self {
        Self::new(25_000)
    }
}

impl DesiredExplicitSet {
    pub fn new(max_explicit_pubkeys: usize) -> Self {
        Self {
            entries: HashMap::new(),
            groups: HashMap::new(),
            by_consumer: HashMap::new(),
            max_explicit_pubkeys: max_explicit_pubkeys.max(1),
            next_seq: 0,
        }
    }

    pub fn max_explicit_pubkeys(&self) -> usize {
        self.max_explicit_pubkeys
    }

    pub fn set_max_explicit_pubkeys(&mut self, cap: usize) {
        self.max_explicit_pubkeys = cap.max(1);
        self.evict_until_within_cap(EvictReason::CapShrink);
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn contains(&self, pubkey: &Pubkey) -> bool {
        self.entries.contains_key(pubkey)
    }

    pub fn consumers_of(&self, pubkey: &Pubkey) -> Option<Vec<ConsumerId>> {
        self.entries.get(pubkey).map(|e| {
            e.owners
                .iter()
                .map(|(c, _)| *c)
                .collect::<HashSet<_>>()
                .into_iter()
                .collect()
        })
    }

    pub fn snapshot_pubkeys(&self) -> HashSet<Pubkey> {
        self.entries.keys().copied().collect()
    }

    pub fn admitted_pool_count(&self, consumer: ConsumerId) -> usize {
        self.groups
            .keys()
            .filter(|(c, owner)| *c == consumer && matches!(owner, OwnerKey::Pool(_)))
            .count()
    }

    pub fn cap_overflow(&self) -> usize {
        self.entries.len().saturating_sub(self.max_explicit_pubkeys)
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.groups.clear();
        self.by_consumer.clear();
        self.next_seq = 0;
    }

    /// Atomically admit a full owner group or reject without mutation.
    pub fn try_admit_group(
        &mut self,
        consumer: ConsumerId,
        owner: OwnerKey,
        pubkeys: HashSet<Pubkey>,
    ) -> AdmissionResult {
        if pubkeys.is_empty() {
            return AdmissionResult::RejectedProtected;
        }
        if let Some(existing) = self.groups.get(&(consumer, owner)) {
            if existing.pubkeys == pubkeys {
                return AdmissionResult::OwnerAddedNoNewPubkey;
            }
        }

        let new_unique: HashSet<Pubkey> = pubkeys
            .iter()
            .filter(|pk| !self.entries.contains_key(pk))
            .copied()
            .collect();
        let need = new_unique.len();
        let free = self.max_explicit_pubkeys.saturating_sub(self.entries.len());

        if need > free {
            let mut deficit = need - free;
            if !self.evict_for_incoming(consumer, &mut deficit) {
                return AdmissionResult::RejectedCap;
            }
            if deficit > 0 {
                return AdmissionResult::RejectedCap;
            }
        }

        self.commit_group(consumer, owner, pubkeys);
        if new_unique.is_empty() {
            AdmissionResult::OwnerAddedNoNewPubkey
        } else {
            AdmissionResult::Admitted {
                new_pubkeys: new_unique.len(),
            }
        }
    }

    /// Remove one owner group; pubkeys remain while other owners still reference them.
    pub fn remove_group(&mut self, consumer: ConsumerId, owner: OwnerKey) -> Vec<Pubkey> {
        let Some(group) = self.groups.remove(&(consumer, owner)) else {
            return Vec::new();
        };
        let mut removed_entirely = Vec::new();
        for pk in group.pubkeys {
            if let Some(entry) = self.entries.get_mut(&pk) {
                entry.owners.remove(&(consumer, owner));
                if entry.owners.is_empty() {
                    self.entries.remove(&pk);
                    removed_entirely.push(pk);
                }
            }
        }
        self.rebuild_by_consumer_index();
        removed_entirely
    }

    /// Deterministic convergence: admit rows in priority order, evict lower groups as needed.
    pub fn converge_from_rows(&mut self, rows: &[(Pubkey, ConsumerId, Option<Pubkey>)]) {
        let mut grouped: HashMap<(ConsumerId, OwnerKey), HashSet<Pubkey>> = HashMap::new();
        for (pk, consumer, pool) in rows {
            let owner = owner_key_from_row(*consumer, *pool, *pk);
            grouped.entry((*consumer, owner)).or_default().insert(*pk);
        }

        let mut owners: Vec<(ConsumerId, OwnerKey, HashSet<Pubkey>)> = grouped
            .into_iter()
            .map(|((c, o), pks)| (c, o, pks))
            .collect();
        owners.sort_by(|a, b| {
            pin_priority_from_consumer(a.0)
                .cmp(&pin_priority_from_consumer(b.0))
                .then_with(|| a.1.cmp(&b.1))
                .then_with(|| a.2.len().cmp(&b.2.len()))
        });

        self.clear();
        for (consumer, owner, pubkeys) in owners {
            let _ = self.try_admit_group(consumer, owner, pubkeys);
        }
        self.evict_until_within_cap(EvictReason::RestoreConverge);
    }

    fn commit_group(&mut self, consumer: ConsumerId, owner: OwnerKey, pubkeys: HashSet<Pubkey>) {
        self.next_seq = self.next_seq.saturating_add(1);
        let seq = self.next_seq;
        if let Some(prev) = self.groups.remove(&(consumer, owner)) {
            for pk in prev.pubkeys {
                if let Some(entry) = self.entries.get_mut(&pk) {
                    entry.owners.remove(&(consumer, owner));
                    if entry.owners.is_empty() {
                        self.entries.remove(&pk);
                    }
                }
            }
        }
        self.groups.insert(
            (consumer, owner),
            OwnerGroup {
                consumer,
                owner,
                pubkeys: pubkeys.clone(),
                admitted_seq: seq,
            },
        );
        for pk in pubkeys {
            let entry = self.entries.entry(pk).or_insert_with(|| PubkeyEntry {
                owners: HashSet::new(),
            });
            entry.owners.insert((consumer, owner));
            self.by_consumer.entry(consumer).or_default().insert(pk);
        }
    }

    fn evict_for_incoming(&mut self, incoming: ConsumerId, deficit: &mut usize) -> bool {
        let incoming_priority = pin_priority_from_consumer(incoming);
        let mut candidates: Vec<(PinPriority, u64, ConsumerId, OwnerKey, usize)> = self
            .groups
            .values()
            .filter(|g| {
                let gp = pin_priority_from_consumer(g.consumer);
                gp > incoming_priority
                    || (gp == incoming_priority
                        && incoming_priority != PinPriority::Wallet
                        && g.consumer == incoming)
            })
            .map(|g| {
                let removable = g
                    .pubkeys
                    .iter()
                    .filter(|pk| {
                        self.entries.get(pk).is_some_and(|e| {
                            e.owners.len() == 1 && e.owners.contains(&(g.consumer, g.owner))
                        })
                    })
                    .count();
                (
                    pin_priority_from_consumer(g.consumer),
                    g.admitted_seq,
                    g.consumer,
                    g.owner,
                    removable,
                )
            })
            .filter(|(_, _, _, _, removable)| *removable > 0)
            .collect();

        candidates.sort_by(|a, b| {
            b.0.cmp(&a.0)
                .then_with(|| a.1.cmp(&b.1))
                .then_with(|| a.4.cmp(&b.4))
        });

        for (_, _, consumer, owner, _removable) in candidates {
            if *deficit == 0 {
                return true;
            }
            let reason = if pin_priority_from_consumer(consumer) > incoming_priority {
                EvictReason::HigherPriority
            } else {
                EvictReason::SamePriorityLru
            };
            let freed = self.evict_group(consumer, owner, reason);
            *deficit = deficit.saturating_sub(freed);
        }
        *deficit == 0
    }

    fn evict_group(
        &mut self,
        consumer: ConsumerId,
        owner: OwnerKey,
        _reason: EvictReason,
    ) -> usize {
        let Some(group) = self.groups.remove(&(consumer, owner)) else {
            return 0;
        };
        let mut freed = 0usize;
        for pk in group.pubkeys {
            if let Some(entry) = self.entries.get_mut(&pk) {
                entry.owners.remove(&(consumer, owner));
                if entry.owners.is_empty() {
                    self.entries.remove(&pk);
                    freed += 1;
                }
            }
        }
        self.rebuild_by_consumer_index();
        freed
    }

    fn evict_until_within_cap(&mut self, reason: EvictReason) {
        while self.entries.len() > self.max_explicit_pubkeys {
            let victim = self
                .groups
                .values()
                .max_by_key(|g| {
                    (
                        pin_priority_from_consumer(g.consumer),
                        std::cmp::Reverse(g.admitted_seq),
                    )
                })
                .map(|g| (g.consumer, g.owner));
            let Some((consumer, owner)) = victim else {
                break;
            };
            if self.evict_group(consumer, owner, reason) == 0 {
                break;
            }
        }
    }

    fn rebuild_by_consumer_index(&mut self) {
        self.by_consumer.clear();
        for (pk, entry) in &self.entries {
            for (consumer, _) in &entry.owners {
                self.by_consumer.entry(*consumer).or_default().insert(*pk);
            }
        }
    }
}

fn owner_key_from_row(consumer: ConsumerId, pool: Option<Pubkey>, pk: Pubkey) -> OwnerKey {
    match consumer {
        ConsumerId::Wallet => OwnerKey::Wallet,
        ConsumerId::Tracker if pool.is_none() => OwnerKey::Mint(pk),
        _ => OwnerKey::Pool(pool.unwrap_or(pk)),
    }
}

pub fn pin_priority_from_consumer(consumer: ConsumerId) -> PinPriority {
    match consumer {
        ConsumerId::Wallet => PinPriority::Wallet,
        ConsumerId::Momentum => PinPriority::Momentum,
        ConsumerId::Arb => PinPriority::Arb,
        ConsumerId::Tracker => PinPriority::Tracker,
    }
}

/// Symmetric set difference for Geyser subscribe delta (|A Δ B|).
pub fn symmetric_diff(a: &HashSet<Pubkey>, b: &HashSet<Pubkey>) -> HashSet<Pubkey> {
    a.symmetric_difference(b).copied().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pool_owner() -> (ConsumerId, OwnerKey, Pubkey) {
        let pool = Pubkey::new_unique();
        (ConsumerId::Arb, OwnerKey::Pool(pool), pool)
    }

    #[test]
    fn deduplicated_pubkeys_stay_within_cap() {
        let mut set = DesiredExplicitSet::new(4);
        let wallet = Pubkey::new_unique();
        assert!(matches!(
            set.try_admit_group(
                ConsumerId::Wallet,
                OwnerKey::Wallet,
                HashSet::from([wallet]),
            ),
            AdmissionResult::Admitted { .. }
        ));
        for _ in 0..10 {
            let (c, o, pool) = pool_owner();
            let a = Pubkey::new_unique();
            let b = Pubkey::new_unique();
            let _ = set.try_admit_group(c, o, HashSet::from([a, b]));
            assert!(set.len() <= 4);
        }
    }

    #[test]
    fn rejected_group_does_not_mutate_desired_set() {
        let mut set = DesiredExplicitSet::new(2);
        let p1 = Pubkey::new_unique();
        let p2 = Pubkey::new_unique();
        let p3 = Pubkey::new_unique();
        let (_, o1, _) = pool_owner();
        let (c2, o2, _) = pool_owner();
        assert!(matches!(
            set.try_admit_group(ConsumerId::Wallet, OwnerKey::Wallet, HashSet::from([p1])),
            AdmissionResult::Admitted { .. }
        ));
        assert!(matches!(
            set.try_admit_group(ConsumerId::Momentum, o1, HashSet::from([p2])),
            AdmissionResult::Admitted { .. }
        ));
        let len_before = set.len();
        assert!(matches!(
            set.try_admit_group(c2, o2, HashSet::from([p3])),
            AdmissionResult::RejectedCap
        ));
        assert_eq!(set.len(), len_before);
        assert!(!set.contains(&p3));
    }

    #[test]
    fn two_vault_group_admitted_or_rejected_atomically() {
        let mut set = DesiredExplicitSet::new(3);
        let (_, o1, _) = pool_owner();
        let v1 = Pubkey::new_unique();
        let v2 = Pubkey::new_unique();
        assert!(matches!(
            set.try_admit_group(ConsumerId::Momentum, o1, HashSet::from([v1, v2])),
            AdmissionResult::Admitted { new_pubkeys: 2 }
        ));
        let wallet = Pubkey::new_unique();
        assert!(matches!(
            set.try_admit_group(
                ConsumerId::Wallet,
                OwnerKey::Wallet,
                HashSet::from([wallet])
            ),
            AdmissionResult::Admitted { .. }
        ));
        let (_, o2, _) = pool_owner();
        let v3 = Pubkey::new_unique();
        let v4 = Pubkey::new_unique();
        assert!(matches!(
            set.try_admit_group(ConsumerId::Arb, o2, HashSet::from([v3, v4])),
            AdmissionResult::RejectedCap
        ));
        assert!(!set.contains(&v3));
        assert!(!set.contains(&v4));
    }

    #[test]
    fn shared_pubkey_survives_single_owner_removal() {
        let mut set = DesiredExplicitSet::new(10);
        let shared = Pubkey::new_unique();
        let (_, o1, _) = pool_owner();
        let (_, o2, _) = pool_owner();
        assert!(matches!(
            set.try_admit_group(ConsumerId::Arb, o1, HashSet::from([shared])),
            AdmissionResult::Admitted { .. }
        ));
        assert!(matches!(
            set.try_admit_group(ConsumerId::Momentum, o2, HashSet::from([shared])),
            AdmissionResult::OwnerAddedNoNewPubkey
        ));
        set.remove_group(ConsumerId::Arb, o1);
        assert!(set.contains(&shared));
        set.remove_group(ConsumerId::Momentum, o2);
        assert!(!set.contains(&shared));
    }

    #[test]
    fn wallet_never_evicted_by_momentum() {
        let wallet = Pubkey::new_unique();
        let mut set = DesiredExplicitSet::new(2);
        assert!(matches!(
            set.try_admit_group(
                ConsumerId::Wallet,
                OwnerKey::Wallet,
                HashSet::from([wallet])
            ),
            AdmissionResult::Admitted { .. }
        ));
        let (_, o, _) = pool_owner();
        let m1 = Pubkey::new_unique();
        let m2 = Pubkey::new_unique();
        assert!(matches!(
            set.try_admit_group(ConsumerId::Momentum, o, HashSet::from([m1])),
            AdmissionResult::Admitted { .. }
        ));
        let (_, o2, _) = pool_owner();
        assert!(matches!(
            set.try_admit_group(ConsumerId::Momentum, o2, HashSet::from([m2])),
            AdmissionResult::Admitted { .. }
        ));
        assert!(set.contains(&wallet));
    }

    #[test]
    fn momentum_evicts_arb_before_momentum() {
        let mut set = DesiredExplicitSet::new(1);
        let arb_pk = Pubkey::new_unique();
        let (_, arb_owner, _) = pool_owner();
        assert!(matches!(
            set.try_admit_group(ConsumerId::Arb, arb_owner, HashSet::from([arb_pk])),
            AdmissionResult::Admitted { .. }
        ));
        let mom_pk = Pubkey::new_unique();
        let (_, mom_owner, _) = pool_owner();
        assert!(matches!(
            set.try_admit_group(ConsumerId::Momentum, mom_owner, HashSet::from([mom_pk])),
            AdmissionResult::Admitted { .. }
        ));
        assert!(!set.contains(&arb_pk));
        assert!(set.contains(&mom_pk));
    }

    #[test]
    fn arb_cannot_evict_momentum() {
        let mut set = DesiredExplicitSet::new(2);
        let mom_pk = Pubkey::new_unique();
        let (_, mom_owner, _) = pool_owner();
        assert!(matches!(
            set.try_admit_group(ConsumerId::Momentum, mom_owner, HashSet::from([mom_pk])),
            AdmissionResult::Admitted { .. }
        ));
        let wallet = Pubkey::new_unique();
        assert!(matches!(
            set.try_admit_group(
                ConsumerId::Wallet,
                OwnerKey::Wallet,
                HashSet::from([wallet])
            ),
            AdmissionResult::Admitted { .. }
        ));
        let arb_pk = Pubkey::new_unique();
        let (_, arb_owner, _) = pool_owner();
        assert!(matches!(
            set.try_admit_group(ConsumerId::Arb, arb_owner, HashSet::from([arb_pk])),
            AdmissionResult::RejectedCap
        ));
        assert!(set.contains(&mom_pk));
        assert!(!set.contains(&arb_pk));
    }

    #[test]
    fn none_pin_maps_to_tracker_consumer() {
        assert_eq!(
            pin_priority_from_consumer(ConsumerId::Tracker),
            PinPriority::Tracker
        );
    }

    #[test]
    fn cap_shrink_converges_deterministically() {
        let mut set = DesiredExplicitSet::new(5);
        for _ in 0..4 {
            let (_, o, _) = pool_owner();
            let pk = Pubkey::new_unique();
            let _ = set.try_admit_group(ConsumerId::Tracker, o, HashSet::from([pk]));
        }
        set.set_max_explicit_pubkeys(2);
        assert!(set.len() <= 2);
        assert_eq!(set.cap_overflow(), 0);
    }

    #[test]
    fn snapshot_over_cap_prioritized_reduce() {
        let rows: Vec<(Pubkey, ConsumerId, Option<Pubkey>)> = (0..6)
            .map(|i| {
                let pk = Pubkey::new_unique();
                let pool = Pubkey::new_unique();
                let consumer = if i == 0 {
                    ConsumerId::Wallet
                } else if i < 3 {
                    ConsumerId::Momentum
                } else {
                    ConsumerId::Arb
                };
                (pk, consumer, Some(pool))
            })
            .collect();
        let mut set = DesiredExplicitSet::new(3);
        set.converge_from_rows(&rows);
        assert!(set.len() <= 3);
        let wallet_row = rows[0].0;
        assert!(set.contains(&wallet_row));
    }

    #[test]
    fn symmetric_diff_detects_add_and_remove() {
        let a: HashSet<Pubkey> = [Pubkey::new_unique(), Pubkey::new_unique()]
            .into_iter()
            .collect();
        let mut b = a.clone();
        let added = Pubkey::new_unique();
        b.insert(added);
        let removed: Pubkey = *a.iter().next().unwrap();
        b.remove(&removed);
        let delta = symmetric_diff(&a, &b);
        assert_eq!(delta.len(), 2);
        assert!(delta.contains(&added));
        assert!(delta.contains(&removed));
    }
}
