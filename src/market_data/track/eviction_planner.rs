//! Validated eviction planning snapshot (PR 1c1a).
//!
//! Side-effect-free read model over [`ExplicitOwnership`] public snapshot APIs.
//! No victim selection, admission, cap-shrink, or runtime wiring.

use super::explicit_ownership::{
    ExplicitConsumer, ExplicitOwner, ExplicitOwnership, OwnerGroupSnapshot,
};
use solana_sdk::pubkey::Pubkey;
use std::collections::{BTreeMap, BTreeSet};

/// External LRU stamp for one logical owner (consumer identity lives on [`ExplicitOwner`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerLruEntry {
    pub owner: ExplicitOwner,
    pub last_touch: u64,
}

/// One owner group plus its LRU stamp in deterministic planning order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerPlanningRecord {
    pub owner: ExplicitOwner,
    pub last_touch: u64,
    pub pubkeys: Vec<Pubkey>,
}

impl OwnerPlanningRecord {
    pub fn consumer(&self) -> ExplicitConsumer {
        self.owner.consumer
    }
}

/// Exact owner set and refcount for one physically tracked pubkey.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubkeyOwnerIndex {
    pub pubkey: Pubkey,
    pub refcount: usize,
    pub owners: Vec<ExplicitOwner>,
}

/// Immutable validated snapshot for later pure eviction planning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvictionPlanningSnapshot {
    physical_len: usize,
    physical_pubkeys: Vec<Pubkey>,
    owners: Vec<OwnerPlanningRecord>,
    pubkey_index: Vec<PubkeyOwnerIndex>,
}

/// Snapshot construction failures — explicit, no panic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotBuildError {
    MissingLruEntry(ExplicitOwner),
    UnknownLruOwner(ExplicitOwner),
    DuplicateLruOwner(ExplicitOwner),
    PhysicalLenMismatch {
        derived: usize,
        canonical: usize,
    },
    PhysicalPubkeysMismatch,
    RefcountMismatch {
        pubkey: Pubkey,
        derived: usize,
        canonical: usize,
    },
}

/// Total protection order: lower ordinal = higher protection (`Wallet` most protected).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ConsumerProtectionRank(u8);

impl ConsumerProtectionRank {
    pub fn of(consumer: ExplicitConsumer) -> Self {
        Self(match consumer {
            ExplicitConsumer::Wallet => 0,
            ExplicitConsumer::Momentum => 1,
            ExplicitConsumer::Arb => 2,
            ExplicitConsumer::Tracker => 3,
        })
    }

    pub fn consumer(self) -> ExplicitConsumer {
        match self.0 {
            0 => ExplicitConsumer::Wallet,
            1 => ExplicitConsumer::Momentum,
            2 => ExplicitConsumer::Arb,
            _ => ExplicitConsumer::Tracker,
        }
    }
}

impl EvictionPlanningSnapshot {
    /// Build a validated snapshot from canonical ownership plus one LRU row per owner.
    pub fn from_ownership(
        ownership: &ExplicitOwnership,
        lru_entries: impl IntoIterator<Item = OwnerLruEntry>,
    ) -> Result<Self, SnapshotBuildError> {
        let owner_groups = ownership.snapshot_owner_groups();
        let owners_in_ownership = owner_set_from_groups(&owner_groups);

        let mut lru_by_owner: BTreeMap<ExplicitOwner, u64> = BTreeMap::new();
        for entry in lru_entries {
            if lru_by_owner
                .insert(entry.owner.clone(), entry.last_touch)
                .is_some()
            {
                return Err(SnapshotBuildError::DuplicateLruOwner(entry.owner));
            }
            if !owners_in_ownership.contains(&entry.owner) {
                return Err(SnapshotBuildError::UnknownLruOwner(entry.owner));
            }
        }

        if ownership.is_empty() {
            if lru_by_owner.is_empty() {
                return Ok(Self::empty());
            }
            let unknown = lru_by_owner.into_keys().next().expect("non-empty lru map");
            return Err(SnapshotBuildError::UnknownLruOwner(unknown));
        }

        for owner in &owners_in_ownership {
            if !lru_by_owner.contains_key(owner) {
                return Err(SnapshotBuildError::MissingLruEntry(owner.clone()));
            }
        }

        let mut owners = Vec::with_capacity(owner_groups.len());
        for group in owner_groups {
            let owner = owner_from_group(&group);
            let last_touch = lru_by_owner
                .get(&owner)
                .copied()
                .expect("LRU validated above");
            owners.push(OwnerPlanningRecord {
                owner: owner.clone(),
                last_touch,
                pubkeys: group.pubkeys,
            });
        }
        owners.sort_by(|a, b| a.owner.cmp(&b.owner));

        let pubkey_index = build_pubkey_index(&owners)?;
        validate_against_ownership(ownership, &pubkey_index)?;

        let physical_pubkeys: Vec<Pubkey> = pubkey_index.iter().map(|row| row.pubkey).collect();
        let physical_len = physical_pubkeys.len();

        Ok(Self {
            physical_len,
            physical_pubkeys,
            owners,
            pubkey_index,
        })
    }

    fn empty() -> Self {
        Self {
            physical_len: 0,
            physical_pubkeys: Vec::new(),
            owners: Vec::new(),
            pubkey_index: Vec::new(),
        }
    }

    pub fn physical_len(&self) -> usize {
        self.physical_len
    }

    pub fn physical_pubkeys(&self) -> &[Pubkey] {
        &self.physical_pubkeys
    }

    pub fn owners(&self) -> &[OwnerPlanningRecord] {
        &self.owners
    }

    pub fn pubkey_index(&self) -> &[PubkeyOwnerIndex] {
        &self.pubkey_index
    }

    pub fn owner_record(&self, owner: &ExplicitOwner) -> Option<&OwnerPlanningRecord> {
        self.owners
            .binary_search_by(|record| record.owner.cmp(owner))
            .ok()
            .map(|idx| &self.owners[idx])
    }

    pub fn owner_refcount(&self, pubkey: &Pubkey) -> usize {
        self.pubkey_index
            .binary_search_by_key(pubkey, |row| row.pubkey)
            .ok()
            .map(|idx| self.pubkey_index[idx].refcount)
            .unwrap_or(0)
    }

    pub fn pubkey_owners(&self, pubkey: &Pubkey) -> Option<&[ExplicitOwner]> {
        self.pubkey_index
            .binary_search_by_key(pubkey, |row| row.pubkey)
            .ok()
            .map(|idx| self.pubkey_index[idx].owners.as_slice())
    }

    pub fn last_touch(&self, owner: &ExplicitOwner) -> Option<u64> {
        self.owner_record(owner).map(|record| record.last_touch)
    }
}

fn owner_from_group(group: &OwnerGroupSnapshot) -> ExplicitOwner {
    ExplicitOwner {
        consumer: group.consumer,
        owner_key: group.owner_key.clone(),
    }
}

fn owner_set_from_groups(groups: &[OwnerGroupSnapshot]) -> BTreeSet<ExplicitOwner> {
    groups.iter().map(owner_from_group).collect()
}

fn build_pubkey_index(
    owners: &[OwnerPlanningRecord],
) -> Result<Vec<PubkeyOwnerIndex>, SnapshotBuildError> {
    let mut owners_by_pubkey: BTreeMap<Pubkey, BTreeSet<ExplicitOwner>> = BTreeMap::new();
    for record in owners {
        for pubkey in &record.pubkeys {
            owners_by_pubkey
                .entry(*pubkey)
                .or_default()
                .insert(record.owner.clone());
        }
    }

    Ok(owners_by_pubkey
        .into_iter()
        .map(|(pubkey, owner_set)| {
            let owners: Vec<ExplicitOwner> = owner_set.into_iter().collect();
            PubkeyOwnerIndex {
                pubkey,
                refcount: owners.len(),
                owners,
            }
        })
        .collect())
}

fn validate_against_ownership(
    ownership: &ExplicitOwnership,
    pubkey_index: &[PubkeyOwnerIndex],
) -> Result<(), SnapshotBuildError> {
    let derived_pubkeys: Vec<Pubkey> = pubkey_index.iter().map(|row| row.pubkey).collect();
    let canonical_pubkeys = ownership.snapshot_pubkeys();
    if derived_pubkeys != canonical_pubkeys {
        if derived_pubkeys.len() != canonical_pubkeys.len() {
            return Err(SnapshotBuildError::PhysicalLenMismatch {
                derived: derived_pubkeys.len(),
                canonical: canonical_pubkeys.len(),
            });
        }
        return Err(SnapshotBuildError::PhysicalPubkeysMismatch);
    }

    for row in pubkey_index {
        let canonical = ownership.owner_refcount(&row.pubkey);
        if row.refcount != canonical {
            return Err(SnapshotBuildError::RefcountMismatch {
                pubkey: row.pubkey,
                derived: row.refcount,
                canonical,
            });
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::explicit_ownership::ExplicitOwnerKey;
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

    fn lru(owner: ExplicitOwner, touch: u64) -> OwnerLruEntry {
        OwnerLruEntry {
            owner,
            last_touch: touch,
        }
    }

    fn upsert(
        ownership: &mut ExplicitOwnership,
        owner: ExplicitOwner,
        pubkeys: impl IntoIterator<Item = Pubkey>,
    ) {
        ownership.upsert_group(owner, pubkeys).unwrap();
    }

    #[derive(Debug, Default)]
    struct BuildStats {
        group_key_edges: usize,
    }

    fn build_with_edge_count(
        ownership: &ExplicitOwnership,
        lru_entries: impl IntoIterator<Item = OwnerLruEntry>,
    ) -> Result<(EvictionPlanningSnapshot, BuildStats), SnapshotBuildError> {
        let groups = ownership.snapshot_owner_groups();
        let mut stats = BuildStats::default();
        for group in &groups {
            stats.group_key_edges += group.pubkeys.len();
        }
        let snapshot = EvictionPlanningSnapshot::from_ownership(ownership, lru_entries)?;
        Ok((snapshot, stats))
    }

    /// Independent oracle — uses only [`ExplicitOwnership`] public snapshot APIs.
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct RefSnapshot {
        physical_pubkeys: Vec<Pubkey>,
        owner_groups: Vec<OwnerGroupSnapshot>,
        pubkey_refcounts: BTreeMap<Pubkey, usize>,
    }

    impl RefSnapshot {
        fn from_ownership(ownership: &ExplicitOwnership) -> Self {
            let owner_groups = ownership.snapshot_owner_groups();
            let physical_pubkeys = ownership.snapshot_pubkeys();
            let mut pubkey_refcounts = BTreeMap::new();
            for group in &owner_groups {
                for pubkey in &group.pubkeys {
                    *pubkey_refcounts.entry(*pubkey).or_default() += 1;
                }
            }
            Self {
                physical_pubkeys,
                owner_groups,
                pubkey_refcounts,
            }
        }

        fn assert_matches(&self, snapshot: &EvictionPlanningSnapshot) {
            assert_eq!(snapshot.physical_len(), self.physical_pubkeys.len());
            assert_eq!(
                snapshot.physical_pubkeys(),
                self.physical_pubkeys.as_slice()
            );

            let expected_owners: BTreeMap<ExplicitOwner, (u64, Vec<Pubkey>)> = snapshot
                .owners()
                .iter()
                .map(|record| {
                    (
                        record.owner.clone(),
                        (record.last_touch, record.pubkeys.clone()),
                    )
                })
                .collect();
            assert_eq!(expected_owners.len(), self.owner_groups.len());

            for group in &self.owner_groups {
                let owner = owner_from_group(group);
                let record = snapshot.owner_record(&owner).expect("missing owner record");
                assert_eq!(record.pubkeys, group.pubkeys);
            }

            for (pubkey, expected_rc) in &self.pubkey_refcounts {
                assert_eq!(snapshot.owner_refcount(pubkey), *expected_rc);
                let owners = snapshot
                    .pubkey_owners(pubkey)
                    .expect("missing pubkey index row");
                assert_eq!(owners.len(), *expected_rc);
            }
        }
    }

    fn capture_ownership(ownership: &ExplicitOwnership) -> RefSnapshot {
        RefSnapshot::from_ownership(ownership)
    }

    #[test]
    fn empty_ownership_accepts_empty_metadata() {
        let ownership = ExplicitOwnership::new();
        let snapshot = EvictionPlanningSnapshot::from_ownership(&ownership, []).unwrap();
        assert_eq!(snapshot.physical_len(), 0);
        assert!(snapshot.owners().is_empty());
        assert!(snapshot.pubkey_index().is_empty());
    }

    #[test]
    fn single_and_multiple_owner_snapshots() {
        let mut ownership = ExplicitOwnership::new();
        let a = pool_owner(ExplicitConsumer::Tracker, 1);
        let b = pool_owner(ExplicitConsumer::Arb, 2);
        upsert(&mut ownership, a.clone(), [pk(10)]);
        upsert(&mut ownership, b.clone(), [pk(11), pk(12)]);

        let snapshot = EvictionPlanningSnapshot::from_ownership(
            &ownership,
            [lru(a.clone(), 100), lru(b.clone(), 200)],
        )
        .unwrap();

        assert_eq!(snapshot.physical_len(), 3);
        assert_eq!(snapshot.owners().len(), 2);
        assert_eq!(snapshot.owner_record(&a).unwrap().last_touch, 100);
        capture_ownership(&ownership).assert_matches(&snapshot);
    }

    #[test]
    fn shared_keys_have_exact_refcounts_and_owner_sets() {
        let mut ownership = ExplicitOwnership::new();
        let shared = pk(1);
        let a = pool_owner(ExplicitConsumer::Momentum, 1);
        let b = pool_owner(ExplicitConsumer::Arb, 2);
        upsert(&mut ownership, a.clone(), [shared, pk(2)]);
        upsert(&mut ownership, b.clone(), [shared]);

        let snapshot =
            EvictionPlanningSnapshot::from_ownership(&ownership, [lru(a, 1), lru(b, 2)]).unwrap();

        let row = snapshot
            .pubkey_index()
            .iter()
            .find(|row| row.pubkey == shared)
            .unwrap();
        assert_eq!(row.refcount, 2);
        assert_eq!(row.owners.len(), 2);
        assert_eq!(snapshot.owner_refcount(&shared), 2);
        capture_ownership(&ownership).assert_matches(&snapshot);
    }

    #[test]
    fn same_owner_key_across_consumers_remain_distinct() {
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
        upsert(&mut ownership, momentum.clone(), [pk(10)]);
        upsert(&mut ownership, arb.clone(), [pk(11)]);

        let snapshot = EvictionPlanningSnapshot::from_ownership(
            &ownership,
            [lru(momentum.clone(), 7), lru(arb.clone(), 8)],
        )
        .unwrap();

        assert_ne!(momentum, arb);
        assert_eq!(snapshot.owners().len(), 2);
        assert!(snapshot.owner_record(&momentum).is_some());
        assert!(snapshot.owner_record(&arb).is_some());
        capture_ownership(&ownership).assert_matches(&snapshot);
    }

    #[test]
    fn missing_extra_and_duplicate_lru_are_rejected_without_mutation() {
        let mut ownership = ExplicitOwnership::new();
        let owner = pool_owner(ExplicitConsumer::Tracker, 1);
        upsert(&mut ownership, owner.clone(), [pk(1)]);
        let groups_before = ownership.snapshot_owner_groups();
        let pubkeys_before = ownership.snapshot_pubkeys();

        let missing = EvictionPlanningSnapshot::from_ownership(&ownership, []);
        assert_eq!(
            missing,
            Err(SnapshotBuildError::MissingLruEntry(owner.clone()))
        );
        assert_eq!(ownership.snapshot_owner_groups(), groups_before);
        assert_eq!(ownership.snapshot_pubkeys(), pubkeys_before);

        let extra = EvictionPlanningSnapshot::from_ownership(
            &ownership,
            [
                lru(owner.clone(), 1),
                lru(pool_owner(ExplicitConsumer::Tracker, 9), 2),
            ],
        );
        assert!(matches!(extra, Err(SnapshotBuildError::UnknownLruOwner(_))));

        let dup = EvictionPlanningSnapshot::from_ownership(
            &ownership,
            [lru(owner.clone(), 1), lru(owner.clone(), 2)],
        );
        assert_eq!(
            dup,
            Err(SnapshotBuildError::DuplicateLruOwner(owner.clone()))
        );
    }

    #[test]
    fn deterministic_under_permuted_metadata_order() {
        let mut ownership = ExplicitOwnership::new();
        let o1 = pool_owner(ExplicitConsumer::Tracker, 1);
        let o2 = pool_owner(ExplicitConsumer::Arb, 2);
        let o3 = pool_owner(ExplicitConsumer::Momentum, 3);
        upsert(&mut ownership, o1.clone(), [pk(1)]);
        upsert(&mut ownership, o2.clone(), [pk(2)]);
        upsert(&mut ownership, o3.clone(), [pk(3)]);

        let s1 = EvictionPlanningSnapshot::from_ownership(
            &ownership,
            [
                lru(o1.clone(), 10),
                lru(o2.clone(), 20),
                lru(o3.clone(), 30),
            ],
        )
        .unwrap();
        let s2 = EvictionPlanningSnapshot::from_ownership(
            &ownership,
            [
                lru(o3.clone(), 30),
                lru(o1.clone(), 10),
                lru(o2.clone(), 20),
            ],
        )
        .unwrap();
        assert_eq!(s1, s2);
    }

    #[test]
    fn physical_len_matches_ownership_len() {
        let mut ownership = ExplicitOwnership::new();
        let shared = pk(1);
        let a = pool_owner(ExplicitConsumer::Tracker, 1);
        let b = pool_owner(ExplicitConsumer::Arb, 2);
        upsert(&mut ownership, a.clone(), [shared, pk(2)]);
        upsert(&mut ownership, b.clone(), [shared]);

        let snapshot =
            EvictionPlanningSnapshot::from_ownership(&ownership, [lru(a, 1), lru(b, 2)]).unwrap();
        assert_eq!(snapshot.physical_len(), ownership.len());
        assert_eq!(snapshot.physical_len(), 2);
    }

    #[test]
    fn large_graph_build_visits_owner_key_edges_linearly() {
        const OWNER_COUNT: usize = 400;
        const KEYS_PER_OWNER: usize = 3;

        let mut ownership = ExplicitOwnership::new();
        let mut lru_entries = Vec::with_capacity(OWNER_COUNT);
        for seed in 0..OWNER_COUNT {
            let owner = ExplicitOwner {
                consumer: ExplicitConsumer::Tracker,
                owner_key: ExplicitOwnerKey::Generic(seed as u64),
            };
            let pubkeys: Vec<Pubkey> = (0..KEYS_PER_OWNER)
                .map(|k| pk(((seed * KEYS_PER_OWNER + k) % 250) as u8))
                .collect();
            upsert(&mut ownership, owner.clone(), pubkeys);
            lru_entries.push(lru(owner, seed as u64));
        }

        let (snapshot, stats) = build_with_edge_count(&ownership, lru_entries).unwrap();
        assert_eq!(stats.group_key_edges, OWNER_COUNT * KEYS_PER_OWNER);
        assert_eq!(snapshot.owners().len(), OWNER_COUNT);
        capture_ownership(&ownership).assert_matches(&snapshot);
    }

    #[test]
    fn checked_stamps_accept_usize_max_without_counter_arithmetic() {
        let mut ownership = ExplicitOwnership::new();
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        upsert(&mut ownership, owner.clone(), [pk(1)]);

        let snapshot =
            EvictionPlanningSnapshot::from_ownership(&ownership, [lru(owner, u64::MAX)]).unwrap();
        assert_eq!(
            snapshot.last_touch(&snapshot.owners()[0].owner),
            Some(u64::MAX)
        );
    }

    #[test]
    fn independent_oracle_matches_owner_groups_pubkeys_and_refcounts() {
        let mut ownership = ExplicitOwnership::new();
        let owners = [
            pool_owner(ExplicitConsumer::Wallet, 1),
            pool_owner(ExplicitConsumer::Momentum, 2),
            pool_owner(ExplicitConsumer::Arb, 3),
            pool_owner(ExplicitConsumer::Tracker, 4),
        ];
        let shared = pk(50);
        upsert(&mut ownership, owners[0].clone(), [pk(10)]);
        upsert(&mut ownership, owners[1].clone(), [shared, pk(11)]);
        upsert(&mut ownership, owners[2].clone(), [shared, pk(12)]);
        upsert(&mut ownership, owners[3].clone(), [pk(13)]);

        let lru_entries = owners
            .iter()
            .enumerate()
            .map(|(i, owner)| lru(owner.clone(), (i as u64) + 1))
            .collect::<Vec<_>>();

        let snapshot = EvictionPlanningSnapshot::from_ownership(&ownership, lru_entries).unwrap();
        RefSnapshot::from_ownership(&ownership).assert_matches(&snapshot);
    }

    #[test]
    fn consumer_protection_rank_orders_wallet_above_tracker() {
        assert!(
            ConsumerProtectionRank::of(ExplicitConsumer::Wallet)
                < ConsumerProtectionRank::of(ExplicitConsumer::Tracker)
        );
        assert!(
            ConsumerProtectionRank::of(ExplicitConsumer::Momentum)
                < ConsumerProtectionRank::of(ExplicitConsumer::Arb)
        );
    }
}
