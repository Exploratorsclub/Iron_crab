//! Explicit owner-group / pubkey-refcount model (PR 1a foundation).
//!
//! Pure local state: no cap, admission, priority, eviction, or runtime wiring.

use solana_sdk::pubkey::Pubkey;
use std::collections::{BTreeMap, HashMap, HashSet};

/// Consumer tag for explicit ownership (Plan §5.2; Tracker included for completeness).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ExplicitConsumer {
    Wallet,
    Momentum,
    Arb,
    Tracker,
}

/// Stable logical owner identity within a consumer.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ExplicitOwnerKey {
    Wallet,
    Pool(Pubkey),
    Mint(Pubkey),
    /// Deterministic test / tracker identity without runtime dependencies.
    Generic(u64),
}

impl PartialOrd for ExplicitOwnerKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ExplicitOwnerKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        match (self, other) {
            (Self::Wallet, Self::Wallet) => Ordering::Equal,
            (Self::Wallet, _) => Ordering::Less,
            (_, Self::Wallet) => Ordering::Greater,
            (Self::Pool(a), Self::Pool(b)) => a.cmp(b),
            (Self::Pool(_), _) => Ordering::Less,
            (_, Self::Pool(_)) => Ordering::Greater,
            (Self::Mint(a), Self::Mint(b)) => a.cmp(b),
            (Self::Mint(_), _) => Ordering::Less,
            (_, Self::Mint(_)) => Ordering::Greater,
            (Self::Generic(a), Self::Generic(b)) => a.cmp(b),
        }
    }
}

/// Full logical owner: consumer + owner key.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ExplicitOwner {
    pub consumer: ExplicitConsumer,
    pub owner_key: ExplicitOwnerKey,
}

/// Snapshot of one owner group at a point in time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerGroupSnapshot {
    pub consumer: ExplicitConsumer,
    pub owner_key: ExplicitOwnerKey,
    pub pubkeys: Vec<Pubkey>,
}

impl OwnerGroupSnapshot {
    fn from_owner(owner: &ExplicitOwner, pubkeys: &[Pubkey]) -> Self {
        Self {
            consumer: owner.consumer,
            owner_key: owner.owner_key.clone(),
            pubkeys: pubkeys.to_vec(),
        }
    }
}

/// Empty owner groups are invalid input and do not mutate state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmptyOwnerGroupError;

/// Result of an ownership mutation — only actual physical pubkey changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupChange {
    /// First insert for this owner; all pubkeys are new physical keys.
    NewGroup { physical_added: Vec<Pubkey> },
    /// Idempotent re-upsert of an identical group.
    Unchanged,
    /// Atomic replacement; physical deltas from refcount 0→1 / 1→0 transitions.
    Replaced {
        physical_added: Vec<Pubkey>,
        physical_removed: Vec<Pubkey>,
    },
}

/// Explicit ownership store: owner groups with shared-pubkey refcounts.
#[derive(Debug, Clone, Default)]
pub struct ExplicitOwnership {
    groups: BTreeMap<ExplicitOwner, Vec<Pubkey>>,
    pubkey_owners: HashMap<Pubkey, HashSet<ExplicitOwner>>,
}

impl ExplicitOwnership {
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of physically tracked pubkeys (refcount > 0).
    pub fn len(&self) -> usize {
        self.pubkey_owners
            .values()
            .filter(|owners| !owners.is_empty())
            .count()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn contains(&self, pubkey: &Pubkey) -> bool {
        self.pubkey_owners
            .get(pubkey)
            .is_some_and(|owners| !owners.is_empty())
    }

    /// Pubkeys owned by `owner`, or `None` if the owner is unknown.
    pub fn owner_group(&self, owner: &ExplicitOwner) -> Option<&[Pubkey]> {
        self.groups.get(owner).map(Vec::as_slice)
    }

    /// Number of distinct owners referencing `pubkey`.
    pub fn owner_refcount(&self, pubkey: &Pubkey) -> usize {
        self.pubkey_owners
            .get(pubkey)
            .map(HashSet::len)
            .unwrap_or(0)
    }

    /// All physically tracked pubkeys, sorted deterministically.
    pub fn snapshot_pubkeys(&self) -> Vec<Pubkey> {
        let mut keys: Vec<Pubkey> = self
            .pubkey_owners
            .iter()
            .filter_map(|(pubkey, owners)| (!owners.is_empty()).then_some(*pubkey))
            .collect();
        keys.sort();
        keys
    }

    /// All owner groups, sorted by `(consumer, owner_key)`.
    pub fn snapshot_owner_groups(&self) -> Vec<OwnerGroupSnapshot> {
        self.groups
            .iter()
            .map(|(owner, pubkeys)| OwnerGroupSnapshot::from_owner(owner, pubkeys))
            .collect()
    }

    pub fn clear(&mut self) {
        self.groups.clear();
        self.pubkey_owners.clear();
    }

    /// Insert or replace an owner group. Empty `pubkeys` is invalid and mutates nothing.
    pub fn upsert_group(
        &mut self,
        owner: ExplicitOwner,
        pubkeys: impl IntoIterator<Item = Pubkey>,
    ) -> Result<GroupChange, EmptyOwnerGroupError> {
        let normalized = normalize_pubkeys(pubkeys);
        if normalized.is_empty() {
            return Err(EmptyOwnerGroupError);
        }

        match self.groups.get(&owner) {
            None => {
                let physical_added =
                    self.attach_owner_pubkeys(&owner, &normalized, &HashSet::new());
                self.groups.insert(owner, normalized);
                Ok(GroupChange::NewGroup { physical_added })
            }
            Some(existing) if existing == &normalized => Ok(GroupChange::Unchanged),
            Some(old_pubkeys) => {
                let old_pubkeys = old_pubkeys.clone();
                let old_set: HashSet<Pubkey> = old_pubkeys.iter().copied().collect();
                let new_set: HashSet<Pubkey> = normalized.iter().copied().collect();
                self.detach_owner_pubkeys(&owner, &old_pubkeys);
                let mut physical_removed = Vec::new();
                for pubkey in old_set.difference(&new_set) {
                    if self
                        .pubkey_owners
                        .get(pubkey)
                        .is_some_and(|owners| owners.is_empty())
                    {
                        self.pubkey_owners.remove(pubkey);
                        physical_removed.push(*pubkey);
                    }
                }
                let physical_added = self.attach_owner_pubkeys(&owner, &normalized, &old_set);
                self.groups.insert(owner, normalized);
                physical_removed.sort();
                Ok(GroupChange::Replaced {
                    physical_added,
                    physical_removed,
                })
            }
        }
    }

    /// Remove an owner group. Returns the removed snapshot, or `None` if unknown.
    pub fn remove_group(&mut self, owner: &ExplicitOwner) -> Option<OwnerGroupSnapshot> {
        let pubkeys = self.groups.remove(owner)?;
        let snapshot = OwnerGroupSnapshot::from_owner(owner, &pubkeys);
        self.detach_owner_pubkeys(owner, &pubkeys);
        self.prune_empty_pubkeys();
        Some(snapshot)
    }

    fn attach_owner_pubkeys(
        &mut self,
        owner: &ExplicitOwner,
        pubkeys: &[Pubkey],
        previously_owned: &HashSet<Pubkey>,
    ) -> Vec<Pubkey> {
        let mut physical_added = Vec::new();
        for pubkey in pubkeys {
            let owners = self.pubkey_owners.entry(*pubkey).or_default();
            let was_physical = !owners.is_empty();
            owners.insert(owner.clone());
            if !was_physical && !previously_owned.contains(pubkey) {
                physical_added.push(*pubkey);
            }
        }
        physical_added
    }

    fn detach_owner_pubkeys(&mut self, owner: &ExplicitOwner, pubkeys: &[Pubkey]) {
        for pubkey in pubkeys {
            if let Some(owners) = self.pubkey_owners.get_mut(pubkey) {
                owners.remove(owner);
            }
        }
    }

    fn prune_empty_pubkeys(&mut self) -> Vec<Pubkey> {
        let mut physical_removed = Vec::new();
        self.pubkey_owners.retain(|pubkey, owners| {
            if owners.is_empty() {
                physical_removed.push(*pubkey);
                false
            } else {
                true
            }
        });
        physical_removed.sort();
        physical_removed
    }
}

fn normalize_pubkeys(pubkeys: impl IntoIterator<Item = Pubkey>) -> Vec<Pubkey> {
    let mut keys: Vec<Pubkey> = pubkeys.into_iter().collect();
    keys.sort();
    keys.dedup();
    keys
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::pubkey::Pubkey;

    fn pk(seed: u8) -> Pubkey {
        Pubkey::new_from_array([seed; 32])
    }

    fn pool_owner(consumer: ExplicitConsumer, seed: u8) -> ExplicitOwner {
        ExplicitOwner {
            consumer,
            owner_key: ExplicitOwnerKey::Pool(pk(seed)),
        }
    }

    fn wallet_owner() -> ExplicitOwner {
        ExplicitOwner {
            consumer: ExplicitConsumer::Wallet,
            owner_key: ExplicitOwnerKey::Wallet,
        }
    }

    #[test]
    fn deduplicates_pubkeys_within_group() {
        let mut ownership = ExplicitOwnership::new();
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        let a = pk(10);
        let change = ownership
            .upsert_group(owner.clone(), [a, a, pk(11)])
            .unwrap();
        assert!(matches!(change, GroupChange::NewGroup { .. }));
        assert_eq!(ownership.owner_group(&owner), Some([a, pk(11)].as_slice()));
    }

    #[test]
    fn shared_pubkey_survives_single_owner_removal() {
        let mut ownership = ExplicitOwnership::new();
        let shared = pk(1);
        let owner_a = pool_owner(ExplicitConsumer::Momentum, 1);
        let owner_b = pool_owner(ExplicitConsumer::Arb, 2);
        ownership.upsert_group(owner_a.clone(), [shared]).unwrap();
        ownership.upsert_group(owner_b.clone(), [shared]).unwrap();
        assert_eq!(ownership.owner_refcount(&shared), 2);

        ownership.remove_group(&owner_a);
        assert!(ownership.contains(&shared));
        assert_eq!(ownership.owner_refcount(&shared), 1);
        assert_eq!(ownership.len(), 1);
    }

    #[test]
    fn last_owner_removes_pubkey_physically() {
        let mut ownership = ExplicitOwnership::new();
        let shared = pk(1);
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        ownership.upsert_group(owner.clone(), [shared]).unwrap();
        ownership.remove_group(&owner);
        assert!(!ownership.contains(&shared));
        assert!(ownership.is_empty());
    }

    #[test]
    fn same_consumer_different_pools_are_distinct_owners() {
        let mut ownership = ExplicitOwnership::new();
        let pool_a = pool_owner(ExplicitConsumer::Momentum, 1);
        let pool_b = pool_owner(ExplicitConsumer::Momentum, 2);
        ownership.upsert_group(pool_a.clone(), [pk(10)]).unwrap();
        ownership.upsert_group(pool_b.clone(), [pk(11)]).unwrap();
        assert_eq!(ownership.snapshot_owner_groups().len(), 2);
        assert_ne!(pool_a, pool_b);
    }

    #[test]
    fn same_pool_different_consumers_are_distinct_owners() {
        let mut ownership = ExplicitOwnership::new();
        let pool = pk(5);
        let momentum = ExplicitOwner {
            consumer: ExplicitConsumer::Momentum,
            owner_key: ExplicitOwnerKey::Pool(pool),
        };
        let arb = ExplicitOwner {
            consumer: ExplicitConsumer::Arb,
            owner_key: ExplicitOwnerKey::Pool(pool),
        };
        ownership.upsert_group(momentum.clone(), [pk(10)]).unwrap();
        ownership.upsert_group(arb.clone(), [pk(11)]).unwrap();
        assert_eq!(ownership.snapshot_owner_groups().len(), 2);
    }

    #[test]
    fn idempotent_reupsert_is_unchanged() {
        let mut ownership = ExplicitOwnership::new();
        let owner = pool_owner(ExplicitConsumer::Tracker, 9);
        let keys = [pk(1), pk(2)];
        ownership.upsert_group(owner.clone(), keys).unwrap();
        let before_pubkeys = ownership.snapshot_pubkeys();
        let change = ownership.upsert_group(owner.clone(), keys).unwrap();
        assert_eq!(change, GroupChange::Unchanged);
        assert_eq!(ownership.snapshot_pubkeys(), before_pubkeys);
    }

    #[test]
    fn replacement_reports_exact_physical_deltas() {
        let mut ownership = ExplicitOwnership::new();
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        let k1 = pk(1);
        let k2 = pk(2);
        let k3 = pk(3);
        ownership.upsert_group(owner.clone(), [k1, k2]).unwrap();
        let change = ownership.upsert_group(owner.clone(), [k2, k3]).unwrap();
        assert_eq!(
            change,
            GroupChange::Replaced {
                physical_added: vec![k3],
                physical_removed: vec![k1],
            }
        );
        assert_eq!(ownership.snapshot_pubkeys(), vec![k2, k3]);
    }

    #[test]
    fn replacement_with_shared_pubkey_does_not_remove_physically() {
        let mut ownership = ExplicitOwnership::new();
        let shared = pk(1);
        let owner_a = pool_owner(ExplicitConsumer::Momentum, 1);
        let owner_b = pool_owner(ExplicitConsumer::Arb, 2);
        ownership
            .upsert_group(owner_a.clone(), [shared, pk(2)])
            .unwrap();
        ownership.upsert_group(owner_b.clone(), [shared]).unwrap();

        let change = ownership.upsert_group(owner_a.clone(), [pk(3)]).unwrap();
        assert_eq!(
            change,
            GroupChange::Replaced {
                physical_added: vec![pk(3)],
                physical_removed: vec![pk(2)],
            }
        );
        assert!(ownership.contains(&shared));
        assert_eq!(ownership.owner_refcount(&shared), 1);
    }

    #[test]
    fn empty_group_is_invalid_and_mutates_nothing() {
        let mut ownership = ExplicitOwnership::new();
        let owner = wallet_owner();
        assert!(matches!(
            ownership.upsert_group(owner.clone(), []),
            Err(EmptyOwnerGroupError)
        ));
        assert!(ownership.is_empty());
        ownership.upsert_group(owner.clone(), [pk(1)]).unwrap();
        assert!(matches!(
            ownership.upsert_group(owner.clone(), []),
            Err(EmptyOwnerGroupError)
        ));
        assert_eq!(ownership.len(), 1);
    }

    #[test]
    fn snapshot_order_is_deterministic() {
        let mut ownership = ExplicitOwnership::new();
        let o3 = pool_owner(ExplicitConsumer::Arb, 3);
        let o1 = pool_owner(ExplicitConsumer::Momentum, 1);
        let o2 = pool_owner(ExplicitConsumer::Momentum, 2);
        ownership.upsert_group(o3.clone(), [pk(30)]).unwrap();
        ownership.upsert_group(o1.clone(), [pk(10)]).unwrap();
        ownership.upsert_group(o2.clone(), [pk(20)]).unwrap();

        let groups = ownership.snapshot_owner_groups();
        let consumers: Vec<_> = groups.iter().map(|g| g.consumer).collect();
        assert_eq!(
            consumers,
            [
                ExplicitConsumer::Momentum,
                ExplicitConsumer::Momentum,
                ExplicitConsumer::Arb,
            ]
        );

        let mut ownership2 = ExplicitOwnership::new();
        ownership2.upsert_group(o1.clone(), [pk(10)]).unwrap();
        ownership2.upsert_group(o3.clone(), [pk(30)]).unwrap();
        ownership2.upsert_group(o2.clone(), [pk(20)]).unwrap();
        assert_eq!(
            ownership.snapshot_owner_groups(),
            ownership2.snapshot_owner_groups()
        );
        assert_eq!(ownership.snapshot_pubkeys(), vec![pk(10), pk(20), pk(30)]);
    }

    #[test]
    fn remove_unknown_owner_is_noop() {
        let mut ownership = ExplicitOwnership::new();
        let owner = pool_owner(ExplicitConsumer::Tracker, 7);
        assert!(ownership.remove_group(&owner).is_none());
        ownership.upsert_group(owner.clone(), [pk(1)]).unwrap();
        let other = pool_owner(ExplicitConsumer::Tracker, 8);
        assert!(ownership.remove_group(&other).is_none());
        assert_eq!(ownership.len(), 1);
    }

    /// Naive reference model for cross-checking every mutation.
    #[derive(Debug, Clone, Default)]
    struct RefModel {
        groups: BTreeMap<ExplicitOwner, Vec<Pubkey>>,
    }

    impl RefModel {
        fn upsert_group(
            &mut self,
            owner: ExplicitOwner,
            pubkeys: impl IntoIterator<Item = Pubkey>,
        ) -> Result<GroupChange, EmptyOwnerGroupError> {
            let normalized = normalize_pubkeys(pubkeys);
            if normalized.is_empty() {
                return Err(EmptyOwnerGroupError);
            }
            let ownership = self.to_explicit_ownership();
            let mut scratch = ownership.clone();
            let change = scratch.upsert_group(owner.clone(), normalized.clone())?;
            if !matches!(change, GroupChange::Unchanged) {
                self.groups.insert(owner, normalized);
            }
            Ok(change)
        }

        fn remove_group(&mut self, owner: &ExplicitOwner) -> Option<OwnerGroupSnapshot> {
            let ownership = self.to_explicit_ownership();
            let mut scratch = ownership.clone();
            let removed = scratch.remove_group(owner)?;
            self.groups.remove(owner);
            Some(removed)
        }

        fn to_explicit_ownership(&self) -> ExplicitOwnership {
            let mut ownership = ExplicitOwnership::new();
            for (owner, pubkeys) in &self.groups {
                ownership
                    .upsert_group(owner.clone(), pubkeys.clone())
                    .unwrap();
            }
            ownership
        }
    }

    fn assert_consistent(model: &RefModel, ownership: &ExplicitOwnership, change: &GroupChange) {
        let rebuilt = model.to_explicit_ownership();
        assert_eq!(
            ownership.snapshot_owner_groups(),
            rebuilt.snapshot_owner_groups()
        );
        assert_eq!(ownership.snapshot_pubkeys(), rebuilt.snapshot_pubkeys());
        for pubkey in ownership.snapshot_pubkeys() {
            assert_eq!(
                ownership.owner_refcount(&pubkey),
                rebuilt.owner_refcount(&pubkey)
            );
        }
        let expected_change = match change {
            GroupChange::NewGroup { physical_added } => GroupChange::NewGroup {
                physical_added: physical_added.clone(),
            },
            GroupChange::Unchanged => GroupChange::Unchanged,
            GroupChange::Replaced {
                physical_added,
                physical_removed,
            } => GroupChange::Replaced {
                physical_added: physical_added.clone(),
                physical_removed: physical_removed.clone(),
            },
        };
        assert_eq!(change, &expected_change);
    }

    #[test]
    fn bounded_reference_model_matches_explicit_ownership() {
        let mut ownership = ExplicitOwnership::new();
        let mut model = RefModel::default();

        let owners = [
            wallet_owner(),
            pool_owner(ExplicitConsumer::Momentum, 1),
            pool_owner(ExplicitConsumer::Momentum, 2),
            pool_owner(ExplicitConsumer::Arb, 1),
            ExplicitOwner {
                consumer: ExplicitConsumer::Tracker,
                owner_key: ExplicitOwnerKey::Generic(42),
            },
        ];

        let key_pool = [pk(1), pk(2), pk(3), pk(4), pk(5)];

        for (step, owner) in owners.iter().enumerate() {
            let keys: Vec<Pubkey> = key_pool
                .iter()
                .copied()
                .filter(|k| (k.to_bytes()[0] as usize + step) % 2 == 0)
                .collect();
            if keys.is_empty() {
                continue;
            }
            let change = ownership.upsert_group(owner.clone(), keys.clone()).unwrap();
            let ref_change = model.upsert_group(owner.clone(), keys).unwrap();
            assert_eq!(change, ref_change);
            assert_consistent(&model, &ownership, &change);
        }

        // Shared-key overlap upsert on momentum pool 1
        let shared_owner = pool_owner(ExplicitConsumer::Momentum, 1);
        let change = ownership
            .upsert_group(shared_owner.clone(), [pk(1), pk(5), pk(6)])
            .unwrap();
        let ref_change = model
            .upsert_group(shared_owner.clone(), [pk(1), pk(5), pk(6)])
            .unwrap();
        assert_eq!(change, ref_change);
        assert_consistent(&model, &ownership, &change);

        // Idempotent
        let change = ownership
            .upsert_group(shared_owner.clone(), [pk(1), pk(5), pk(6)])
            .unwrap();
        let ref_change = model
            .upsert_group(shared_owner.clone(), [pk(1), pk(5), pk(6)])
            .unwrap();
        assert_eq!(change, GroupChange::Unchanged);
        assert_eq!(ref_change, GroupChange::Unchanged);
        assert_consistent(&model, &ownership, &change);

        // Remove arb owner
        let arb_owner = pool_owner(ExplicitConsumer::Arb, 1);
        let removed = ownership.remove_group(&arb_owner);
        let ref_removed = model.remove_group(&arb_owner);
        assert_eq!(removed, ref_removed);
        assert_consistent(&model, &ownership, &GroupChange::Unchanged);

        // Remove wallet
        let wo = wallet_owner();
        ownership.remove_group(&wo);
        model.remove_group(&wo);
        assert_consistent(&model, &ownership, &GroupChange::Unchanged);
    }
}
