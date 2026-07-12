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
    /// First insert for this owner; pubkeys that transition refcount 0→1.
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
                let physical_removed: Vec<Pubkey> = self
                    .detach_owner_pubkeys(&owner, &old_pubkeys, None)
                    .into_iter()
                    .filter(|pubkey| !new_set.contains(pubkey))
                    .collect();
                let physical_added = self.attach_owner_pubkeys(&owner, &normalized, &old_set);
                self.groups.insert(owner, normalized);
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
        self.detach_owner_pubkeys(owner, &pubkeys, None);
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

    /// Unlink `owner` from `pubkeys`, removing reverse-index keys on refcount 1→0.
    ///
    /// Visits exactly `pubkeys.len()` map keys (group-local). When `keys_visited` is
    /// `Some`, increments it once per pubkey processed (test instrumentation).
    fn detach_owner_pubkeys(
        &mut self,
        owner: &ExplicitOwner,
        pubkeys: &[Pubkey],
        mut keys_visited: Option<&mut usize>,
    ) -> Vec<Pubkey> {
        let mut physical_removed = Vec::new();
        for pubkey in pubkeys {
            if let Some(counter) = keys_visited.as_deref_mut() {
                *counter += 1;
            }
            let became_empty = if let Some(owners) = self.pubkey_owners.get_mut(pubkey) {
                owners.remove(owner);
                owners.is_empty()
            } else {
                false
            };
            if became_empty {
                self.pubkey_owners.remove(pubkey);
                physical_removed.push(*pubkey);
            }
        }
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
    use std::collections::BTreeSet;

    fn pk(seed: u8) -> Pubkey {
        Pubkey::new_from_array([seed; 32])
    }

    /// Counts pubkey visits inside [`ExplicitOwnership::detach_owner_pubkeys`].
    #[derive(Debug, Default)]
    struct UnlinkStats {
        keys_visited: usize,
    }

    impl UnlinkStats {
        fn detach(
            ownership: &mut ExplicitOwnership,
            owner: &ExplicitOwner,
            pubkeys: &[Pubkey],
        ) -> (Self, Vec<Pubkey>) {
            let mut stats = Self::default();
            let removed =
                ownership.detach_owner_pubkeys(owner, pubkeys, Some(&mut stats.keys_visited));
            (stats, removed)
        }
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

    fn same_pool_owner(consumer: ExplicitConsumer, pool_seed: u8) -> ExplicitOwner {
        ExplicitOwner {
            consumer,
            owner_key: ExplicitOwnerKey::Pool(pk(pool_seed)),
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct IndexSnapshot {
        owner_groups: Vec<OwnerGroupSnapshot>,
        pubkeys: Vec<Pubkey>,
        pubkey_owner_sets: Vec<(Pubkey, Vec<ExplicitOwner>)>,
    }

    fn pubkey_owner_sets_from_groups(
        groups: &BTreeMap<ExplicitOwner, BTreeSet<Pubkey>>,
    ) -> BTreeMap<Pubkey, BTreeSet<ExplicitOwner>> {
        let mut map: BTreeMap<Pubkey, BTreeSet<ExplicitOwner>> = BTreeMap::new();
        for (owner, pubkeys) in groups {
            for pubkey in pubkeys {
                map.entry(*pubkey).or_default().insert(owner.clone());
            }
        }
        map
    }

    fn physical_pubkeys_from_groups(
        groups: &BTreeMap<ExplicitOwner, BTreeSet<Pubkey>>,
    ) -> BTreeSet<Pubkey> {
        let mut set = BTreeSet::new();
        for pubkeys in groups.values() {
            set.extend(pubkeys.iter().copied());
        }
        set
    }

    fn snapshot_owner_groups_from_map(
        groups: &BTreeMap<ExplicitOwner, BTreeSet<Pubkey>>,
    ) -> Vec<OwnerGroupSnapshot> {
        groups
            .iter()
            .map(|(owner, pubkeys)| OwnerGroupSnapshot {
                consumer: owner.consumer,
                owner_key: owner.owner_key.clone(),
                pubkeys: pubkeys.iter().copied().collect(),
            })
            .collect()
    }

    fn index_snapshot_from_groups(
        groups: &BTreeMap<ExplicitOwner, BTreeSet<Pubkey>>,
    ) -> IndexSnapshot {
        let owner_groups = snapshot_owner_groups_from_map(groups);
        let pubkeys: Vec<Pubkey> = physical_pubkeys_from_groups(groups).into_iter().collect();
        let pubkey_owner_sets: Vec<(Pubkey, Vec<ExplicitOwner>)> =
            pubkey_owner_sets_from_groups(groups)
                .into_iter()
                .map(|(pubkey, owners)| (pubkey, owners.into_iter().collect()))
                .collect();
        IndexSnapshot {
            owner_groups,
            pubkeys,
            pubkey_owner_sets,
        }
    }

    fn sut_owner_groups(
        ownership: &ExplicitOwnership,
    ) -> BTreeMap<ExplicitOwner, BTreeSet<Pubkey>> {
        ownership
            .groups
            .iter()
            .map(|(owner, pubkeys)| (owner.clone(), pubkeys.iter().copied().collect()))
            .collect()
    }

    fn sut_pubkey_owner_sets(
        ownership: &ExplicitOwnership,
    ) -> BTreeMap<Pubkey, BTreeSet<ExplicitOwner>> {
        ownership
            .pubkey_owners
            .iter()
            .map(|(pubkey, owners)| (*pubkey, owners.iter().cloned().collect()))
            .collect()
    }

    fn assert_no_stale_empty_reverse_index_keys(ownership: &ExplicitOwnership) {
        for (pubkey, owners) in &ownership.pubkey_owners {
            assert!(
                !owners.is_empty(),
                "stale empty reverse-index entry for {pubkey:?}"
            );
        }
    }

    fn normalized_pubkey_owner_vec(
        map: &BTreeMap<Pubkey, BTreeSet<ExplicitOwner>>,
    ) -> Vec<(Pubkey, Vec<ExplicitOwner>)> {
        map.iter()
            .map(|(pubkey, owners)| (*pubkey, owners.iter().cloned().collect()))
            .collect()
    }

    fn index_snapshot(ownership: &ExplicitOwnership) -> IndexSnapshot {
        let owner_groups = ownership.snapshot_owner_groups();
        let pubkeys = ownership.snapshot_pubkeys();
        let pubkey_owner_sets = normalized_pubkey_owner_vec(&sut_pubkey_owner_sets(ownership));
        IndexSnapshot {
            owner_groups,
            pubkeys,
            pubkey_owner_sets,
        }
    }

    /// Independent reference model — never calls [`ExplicitOwnership`].
    #[derive(Debug, Clone, Default)]
    struct RefModel {
        groups: BTreeMap<ExplicitOwner, BTreeSet<Pubkey>>,
    }

    impl RefModel {
        fn index_snapshot(&self) -> IndexSnapshot {
            index_snapshot_from_groups(&self.groups)
        }

        fn upsert_group(
            &mut self,
            owner: ExplicitOwner,
            pubkeys: impl IntoIterator<Item = Pubkey>,
        ) -> Result<GroupChange, EmptyOwnerGroupError> {
            let normalized: BTreeSet<Pubkey> = pubkeys.into_iter().collect();
            if normalized.is_empty() {
                return Err(EmptyOwnerGroupError);
            }

            match self.groups.get(&owner) {
                Some(existing) if *existing == normalized => Ok(GroupChange::Unchanged),
                Some(_) => {
                    let physical_before = physical_pubkeys_from_groups(&self.groups);
                    self.groups.insert(owner.clone(), normalized.clone());
                    let physical_after = physical_pubkeys_from_groups(&self.groups);
                    let physical_added: Vec<Pubkey> = physical_after
                        .difference(&physical_before)
                        .copied()
                        .collect();
                    let physical_removed: Vec<Pubkey> = physical_before
                        .difference(&physical_after)
                        .copied()
                        .collect();
                    Ok(GroupChange::Replaced {
                        physical_added,
                        physical_removed,
                    })
                }
                None => {
                    let physical_before = physical_pubkeys_from_groups(&self.groups);
                    self.groups.insert(owner, normalized.clone());
                    let physical_after = physical_pubkeys_from_groups(&self.groups);
                    let physical_added: Vec<Pubkey> = physical_after
                        .difference(&physical_before)
                        .copied()
                        .collect();
                    Ok(GroupChange::NewGroup { physical_added })
                }
            }
        }

        fn remove_group(&mut self, owner: &ExplicitOwner) -> Option<OwnerGroupSnapshot> {
            let pubkeys = self.groups.remove(owner)?;
            Some(OwnerGroupSnapshot {
                consumer: owner.consumer,
                owner_key: owner.owner_key.clone(),
                pubkeys: pubkeys.into_iter().collect(),
            })
        }
    }

    fn assert_indexes_match(ownership: &ExplicitOwnership, model: &RefModel) {
        assert_no_stale_empty_reverse_index_keys(ownership);

        let sut_groups = sut_owner_groups(ownership);
        assert_eq!(sut_groups, model.groups);

        let sut_reverse = sut_pubkey_owner_sets(ownership);
        let model_reverse = pubkey_owner_sets_from_groups(&model.groups);
        assert_eq!(sut_reverse, model_reverse);

        let all_pubkeys: BTreeSet<Pubkey> = sut_reverse
            .keys()
            .chain(model_reverse.keys())
            .copied()
            .collect();
        for pubkey in all_pubkeys {
            let sut_size = sut_reverse.get(&pubkey).map(BTreeSet::len).unwrap_or(0);
            let model_size = model_reverse.get(&pubkey).map(BTreeSet::len).unwrap_or(0);
            assert_eq!(ownership.owner_refcount(&pubkey), sut_size);
            assert_eq!(ownership.owner_refcount(&pubkey), model_size);
            assert_eq!(sut_size, model_size);
        }

        let actual = index_snapshot(ownership);
        let expected = model.index_snapshot();
        assert_eq!(actual.owner_groups, expected.owner_groups);
        assert_eq!(actual.pubkeys, expected.pubkeys);
        assert_eq!(actual.pubkey_owner_sets, expected.pubkey_owner_sets);
    }

    fn assert_operation(
        ownership: &ExplicitOwnership,
        model: &RefModel,
        change: &GroupChange,
        expected_change: &GroupChange,
    ) {
        assert_eq!(change, expected_change);
        assert_indexes_match(ownership, model);
    }

    #[test]
    fn deduplicates_unsorted_duplicate_pubkeys_within_group() {
        let mut ownership = ExplicitOwnership::new();
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        let a = pk(10);
        let b = pk(11);
        let change = ownership
            .upsert_group(owner.clone(), [b, a, b, a, b])
            .unwrap();
        assert_eq!(
            change,
            GroupChange::NewGroup {
                physical_added: vec![a, b],
            }
        );
        assert_eq!(ownership.owner_group(&owner), Some([a, b].as_slice()));
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
    fn two_pools_under_same_consumer_remain_distinct_owners() {
        let mut ownership = ExplicitOwnership::new();
        let pool_a = pool_owner(ExplicitConsumer::Momentum, 1);
        let pool_b = pool_owner(ExplicitConsumer::Momentum, 2);
        ownership.upsert_group(pool_a.clone(), [pk(10)]).unwrap();
        ownership.upsert_group(pool_b.clone(), [pk(11)]).unwrap();
        assert_eq!(ownership.snapshot_owner_groups().len(), 2);
        assert_ne!(pool_a, pool_b);
        assert_eq!(ownership.owner_refcount(&pk(10)), 1);
        assert_eq!(ownership.owner_refcount(&pk(11)), 1);
    }

    #[test]
    fn two_consumers_sharing_same_pool_owner_key_remain_distinct() {
        let mut ownership = ExplicitOwnership::new();
        let momentum = same_pool_owner(ExplicitConsumer::Momentum, 5);
        let arb = same_pool_owner(ExplicitConsumer::Arb, 5);
        let shared_pubkey = pk(20);
        ownership
            .upsert_group(momentum.clone(), [shared_pubkey, pk(21)])
            .unwrap();
        ownership
            .upsert_group(arb.clone(), [shared_pubkey, pk(22)])
            .unwrap();
        assert_eq!(ownership.snapshot_owner_groups().len(), 2);
        assert_eq!(ownership.owner_refcount(&shared_pubkey), 2);
        assert_eq!(ownership.owner_refcount(&pk(21)), 1);
        assert_eq!(ownership.owner_refcount(&pk(22)), 1);
    }

    #[test]
    fn idempotent_reupsert_with_reversed_order_and_duplicates_is_unchanged() {
        let mut ownership = ExplicitOwnership::new();
        let owner = pool_owner(ExplicitConsumer::Tracker, 9);
        let keys = [pk(1), pk(2), pk(3)];
        ownership.upsert_group(owner.clone(), keys).unwrap();
        let before = index_snapshot(&ownership);
        let change = ownership
            .upsert_group(owner.clone(), [pk(3), pk(1), pk(2), pk(2), pk(1)])
            .unwrap();
        assert_eq!(change, GroupChange::Unchanged);
        assert_eq!(index_snapshot(&ownership), before);
    }

    #[test]
    fn replacement_oracle_exclusive_case_matches_ref_model() {
        let mut ownership = ExplicitOwnership::new();
        let mut model = RefModel::default();
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        let k1 = pk(1);
        let k2 = pk(2);
        let k3 = pk(3);

        ownership.upsert_group(owner.clone(), [k1, k2]).unwrap();
        model.upsert_group(owner.clone(), [k1, k2]).unwrap();

        let change = ownership.upsert_group(owner.clone(), [k2, k3]).unwrap();
        let ref_change = model.upsert_group(owner.clone(), [k2, k3]).unwrap();
        assert_eq!(
            change,
            GroupChange::Replaced {
                physical_added: vec![k3],
                physical_removed: vec![k1],
            }
        );
        assert_eq!(change, ref_change);
        assert_indexes_match(&ownership, &model);
    }

    #[test]
    fn replacement_oracle_shared_key_case_matches_ref_model() {
        let mut ownership = ExplicitOwnership::new();
        let mut model = RefModel::default();
        let shared = pk(1);
        let owner_a = pool_owner(ExplicitConsumer::Momentum, 1);
        let owner_b = pool_owner(ExplicitConsumer::Arb, 2);

        ownership
            .upsert_group(owner_a.clone(), [shared, pk(2)])
            .unwrap();
        model
            .upsert_group(owner_a.clone(), [shared, pk(2)])
            .unwrap();
        ownership.upsert_group(owner_b.clone(), [shared]).unwrap();
        model.upsert_group(owner_b.clone(), [shared]).unwrap();

        let change = ownership.upsert_group(owner_a.clone(), [pk(3)]).unwrap();
        let ref_change = model.upsert_group(owner_a.clone(), [pk(3)]).unwrap();
        assert_eq!(
            change,
            GroupChange::Replaced {
                physical_added: vec![pk(3)],
                physical_removed: vec![pk(2)],
            }
        );
        assert_eq!(change, ref_change);
        assert_indexes_match(&ownership, &model);
        assert!(ownership.contains(&shared));
        assert_eq!(ownership.owner_refcount(&shared), 1);
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
    fn empty_group_preserves_full_index_snapshot() {
        let mut ownership = ExplicitOwnership::new();
        let mut model = RefModel::default();
        let owner = wallet_owner();
        let before_actual = index_snapshot(&ownership);
        let before_model = model.index_snapshot();
        assert!(matches!(
            ownership.upsert_group(owner.clone(), []),
            Err(EmptyOwnerGroupError)
        ));
        assert!(matches!(
            model.upsert_group(owner.clone(), []),
            Err(EmptyOwnerGroupError)
        ));
        assert_eq!(index_snapshot(&ownership), before_actual);
        assert_eq!(model.index_snapshot(), before_model);

        ownership
            .upsert_group(owner.clone(), [pk(1), pk(2)])
            .unwrap();
        model.upsert_group(owner.clone(), [pk(1), pk(2)]).unwrap();
        let mid_actual = index_snapshot(&ownership);
        let mid_model = model.index_snapshot();

        assert!(matches!(
            ownership.upsert_group(owner.clone(), []),
            Err(EmptyOwnerGroupError)
        ));
        assert!(matches!(
            model.upsert_group(owner.clone(), []),
            Err(EmptyOwnerGroupError)
        ));
        assert_eq!(index_snapshot(&ownership), mid_actual);
        assert_eq!(model.index_snapshot(), mid_model);
        assert_indexes_match(&ownership, &model);
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
    fn remove_unknown_owner_preserves_full_index_snapshot() {
        let mut ownership = ExplicitOwnership::new();
        let mut model = RefModel::default();
        let owner = pool_owner(ExplicitConsumer::Tracker, 7);
        let unknown = pool_owner(ExplicitConsumer::Tracker, 8);

        let before_actual = index_snapshot(&ownership);
        let before_model = model.index_snapshot();
        assert!(ownership.remove_group(&unknown).is_none());
        assert!(model.remove_group(&unknown).is_none());
        assert_eq!(index_snapshot(&ownership), before_actual);
        assert_eq!(model.index_snapshot(), before_model);

        ownership.upsert_group(owner.clone(), [pk(1)]).unwrap();
        model.upsert_group(owner.clone(), [pk(1)]).unwrap();
        let mid_actual = index_snapshot(&ownership);
        let mid_model = model.index_snapshot();

        assert!(ownership.remove_group(&unknown).is_none());
        assert!(model.remove_group(&unknown).is_none());
        assert_eq!(index_snapshot(&ownership), mid_actual);
        assert_eq!(model.index_snapshot(), mid_model);
        assert_indexes_match(&ownership, &model);
    }

    #[test]
    fn bounded_reference_model_matches_explicit_ownership() {
        let mut ownership = ExplicitOwnership::new();
        let mut model = RefModel::default();

        let owners = [
            wallet_owner(),
            pool_owner(ExplicitConsumer::Momentum, 1),
            pool_owner(ExplicitConsumer::Momentum, 2),
            same_pool_owner(ExplicitConsumer::Arb, 1),
            same_pool_owner(ExplicitConsumer::Momentum, 1),
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
            assert_operation(&ownership, &model, &change, &ref_change);
        }

        let shared_owner = pool_owner(ExplicitConsumer::Momentum, 1);
        let change = ownership
            .upsert_group(shared_owner.clone(), [pk(1), pk(5), pk(6)])
            .unwrap();
        let ref_change = model
            .upsert_group(shared_owner.clone(), [pk(1), pk(5), pk(6)])
            .unwrap();
        assert_operation(&ownership, &model, &change, &ref_change);

        let change = ownership
            .upsert_group(shared_owner.clone(), [pk(6), pk(5), pk(1), pk(1)])
            .unwrap();
        let ref_change = model
            .upsert_group(shared_owner.clone(), [pk(6), pk(5), pk(1), pk(1)])
            .unwrap();
        assert_operation(&ownership, &model, &change, &ref_change);
        assert_eq!(change, GroupChange::Unchanged);

        let arb_owner = pool_owner(ExplicitConsumer::Arb, 1);
        let removed = ownership.remove_group(&arb_owner);
        let ref_removed = model.remove_group(&arb_owner);
        assert_eq!(removed, ref_removed);
        assert_indexes_match(&ownership, &model);

        let wo = wallet_owner();
        ownership.remove_group(&wo);
        model.remove_group(&wo);
        assert_indexes_match(&ownership, &model);
    }

    #[test]
    fn removal_and_replacement_leave_no_empty_reverse_index_keys() {
        let mut ownership = ExplicitOwnership::new();
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        let other = pool_owner(ExplicitConsumer::Arb, 2);
        let shared = pk(1);
        let k2 = pk(2);
        let k3 = pk(3);

        ownership.upsert_group(owner.clone(), [shared, k2]).unwrap();
        assert_no_stale_empty_reverse_index_keys(&ownership);

        ownership.upsert_group(other.clone(), [shared]).unwrap();
        assert_no_stale_empty_reverse_index_keys(&ownership);

        ownership.upsert_group(owner.clone(), [k2, k3]).unwrap();
        assert_no_stale_empty_reverse_index_keys(&ownership);

        ownership.remove_group(&other);
        assert_no_stale_empty_reverse_index_keys(&ownership);

        ownership.remove_group(&owner);
        assert_no_stale_empty_reverse_index_keys(&ownership);
        assert!(ownership.pubkey_owners.is_empty());
    }

    #[test]
    fn new_group_physical_added_only_for_zero_to_one_transitions() {
        let mut ownership = ExplicitOwnership::new();
        let shared = pk(1);
        let first = pool_owner(ExplicitConsumer::Momentum, 1);
        let second = pool_owner(ExplicitConsumer::Arb, 2);

        let change = ownership
            .upsert_group(first.clone(), [shared, pk(2)])
            .unwrap();
        assert_eq!(
            change,
            GroupChange::NewGroup {
                physical_added: vec![shared, pk(2)],
            }
        );

        let change = ownership.upsert_group(second.clone(), [shared]).unwrap();
        assert_eq!(
            change,
            GroupChange::NewGroup {
                physical_added: vec![],
            }
        );
        assert_eq!(ownership.owner_refcount(&shared), 2);
    }

    #[test]
    fn exclusive_removal_clears_all_affected_reverse_index_keys() {
        let mut ownership = ExplicitOwnership::new();
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        let k1 = pk(1);
        let k2 = pk(2);
        ownership.upsert_group(owner.clone(), [k1, k2]).unwrap();

        ownership.remove_group(&owner);
        assert_no_stale_empty_reverse_index_keys(&ownership);
        assert!(ownership.pubkey_owners.is_empty());
        assert!(!ownership.contains(&k1));
        assert!(!ownership.contains(&k2));
    }

    #[test]
    fn shared_removal_preserves_shared_reverse_index_keys() {
        let mut ownership = ExplicitOwnership::new();
        let shared = pk(1);
        let exclusive = pk(2);
        let owner_a = pool_owner(ExplicitConsumer::Momentum, 1);
        let owner_b = pool_owner(ExplicitConsumer::Arb, 2);
        ownership
            .upsert_group(owner_a.clone(), [shared, exclusive])
            .unwrap();
        ownership.upsert_group(owner_b.clone(), [shared]).unwrap();

        ownership.remove_group(&owner_a);
        assert_no_stale_empty_reverse_index_keys(&ownership);
        assert!(ownership.contains(&shared));
        assert!(!ownership.contains(&exclusive));
        assert_eq!(ownership.owner_refcount(&shared), 1);
        assert_eq!(ownership.owner_group(&owner_b), Some([shared].as_slice()));
    }

    #[test]
    fn unlink_visits_only_removed_group_keys_not_full_index() {
        const BACKGROUND_KEYS: usize = 200;
        const REMOVED_GROUP_KEYS: usize = 3;

        let mut ownership = ExplicitOwnership::new();
        let background_owner = pool_owner(ExplicitConsumer::Momentum, 1);
        let background_keys: Vec<Pubkey> =
            (0..BACKGROUND_KEYS).map(|seed| pk(seed as u8)).collect();
        ownership
            .upsert_group(background_owner, background_keys)
            .unwrap();
        let total_physical = ownership.len();
        assert!(
            total_physical > REMOVED_GROUP_KEYS,
            "background index must dwarf removed group for visit-count proof"
        );

        let removed_owner = pool_owner(ExplicitConsumer::Arb, 2);
        let removed_keys: Vec<Pubkey> = (200..200 + REMOVED_GROUP_KEYS)
            .map(|seed| pk(seed as u8))
            .collect();
        ownership
            .upsert_group(removed_owner.clone(), removed_keys.clone())
            .unwrap();

        let (stats, physical_removed) =
            UnlinkStats::detach(&mut ownership, &removed_owner, &removed_keys);
        assert_eq!(
            stats.keys_visited, REMOVED_GROUP_KEYS,
            "unlink must visit exactly the old group's pubkeys"
        );
        assert!(
            stats.keys_visited < total_physical,
            "unlink must not scan the full reverse index (visited={}, total_physical={})",
            stats.keys_visited,
            total_physical
        );
        assert_eq!(physical_removed, removed_keys);

        ownership.groups.remove(&removed_owner);
        assert_no_stale_empty_reverse_index_keys(&ownership);
        assert_eq!(ownership.len(), total_physical);

        ownership
            .upsert_group(removed_owner.clone(), removed_keys.clone())
            .unwrap();
        let snapshot = ownership.remove_group(&removed_owner).unwrap();
        assert_eq!(snapshot.pubkeys, removed_keys);
        assert_no_stale_empty_reverse_index_keys(&ownership);
        assert_eq!(ownership.len(), total_physical);
    }
}
