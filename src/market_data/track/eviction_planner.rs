//! Validated eviction planning snapshot (PR 1c1a).
//!
//! Side-effect-free read model over [`ExplicitOwnership`] public snapshot APIs.
//! No victim selection, admission, cap-shrink, or runtime wiring.
//!
//! Construction is one pass over owner-group edges with `O(E log E)` deterministic
//! ordering (`BTreeMap` / `BTreeSet` inserts and final sorts); no repeated whole-graph
//! rebuild.

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

/// Test-only counters incremented inside the actual constructor loops.
#[cfg(test)]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BuildStats {
    pub owner_records_built: u64,
    pub group_key_edges: u64,
    pub reverse_index_inserts: u64,
    pub validation_reads: u64,
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

trait BuildStatsSink {
    fn record_owner_record(&mut self);
    fn record_group_key_edge(&mut self);
    fn record_reverse_index_insert(&mut self);
    fn record_validation_read(&mut self);
}

struct NoopBuildStats;

impl BuildStatsSink for NoopBuildStats {
    fn record_owner_record(&mut self) {}
    fn record_group_key_edge(&mut self) {}
    fn record_reverse_index_insert(&mut self) {}
    fn record_validation_read(&mut self) {}
}

#[cfg(test)]
impl BuildStatsSink for BuildStats {
    fn record_owner_record(&mut self) {
        self.owner_records_built += 1;
    }
    fn record_group_key_edge(&mut self) {
        self.group_key_edges += 1;
    }
    fn record_reverse_index_insert(&mut self) {
        self.reverse_index_inserts += 1;
    }
    fn record_validation_read(&mut self) {
        self.validation_reads += 1;
    }
}

impl EvictionPlanningSnapshot {
    /// Build a validated snapshot from canonical ownership plus one LRU row per owner.
    pub fn from_ownership(
        ownership: &ExplicitOwnership,
        lru_entries: impl IntoIterator<Item = OwnerLruEntry>,
    ) -> Result<Self, SnapshotBuildError> {
        from_ownership_inner(ownership, lru_entries, &mut NoopBuildStats)
    }

    #[cfg(test)]
    pub fn from_ownership_with_stats(
        ownership: &ExplicitOwnership,
        lru_entries: impl IntoIterator<Item = OwnerLruEntry>,
        stats: &mut BuildStats,
    ) -> Result<Self, SnapshotBuildError> {
        from_ownership_inner(ownership, lru_entries, stats)
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

fn from_ownership_inner<S: BuildStatsSink>(
    ownership: &ExplicitOwnership,
    lru_entries: impl IntoIterator<Item = OwnerLruEntry>,
    stats: &mut S,
) -> Result<EvictionPlanningSnapshot, SnapshotBuildError> {
    let owner_groups = ownership.snapshot_owner_groups();
    let canonical_pubkeys = ownership.snapshot_pubkeys();
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

    for owner in &owners_in_ownership {
        if !lru_by_owner.contains_key(owner) {
            return Err(SnapshotBuildError::MissingLruEntry(owner.clone()));
        }
    }

    let mut owners = Vec::with_capacity(owner_groups.len());
    for group in owner_groups {
        let owner = owner_from_group(&group);
        let Some(last_touch) = lru_by_owner.get(&owner).copied() else {
            return Err(SnapshotBuildError::MissingLruEntry(owner));
        };
        stats.record_owner_record();
        for _pubkey in &group.pubkeys {
            stats.record_group_key_edge();
        }
        owners.push(OwnerPlanningRecord {
            owner: owner.clone(),
            last_touch,
            pubkeys: group.pubkeys,
        });
    }
    owners.sort_by(|a, b| a.owner.cmp(&b.owner));

    let pubkey_index = build_pubkey_index(&owners, stats)?;
    validate_against_ownership(ownership, &canonical_pubkeys, &pubkey_index, stats)?;

    let physical_pubkeys: Vec<Pubkey> = pubkey_index.iter().map(|row| row.pubkey).collect();
    let physical_len = physical_pubkeys.len();

    Ok(EvictionPlanningSnapshot {
        physical_len,
        physical_pubkeys,
        owners,
        pubkey_index,
    })
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

fn build_pubkey_index<S: BuildStatsSink>(
    owners: &[OwnerPlanningRecord],
    stats: &mut S,
) -> Result<Vec<PubkeyOwnerIndex>, SnapshotBuildError> {
    let mut owners_by_pubkey: BTreeMap<Pubkey, BTreeSet<ExplicitOwner>> = BTreeMap::new();
    for record in owners {
        for pubkey in &record.pubkeys {
            owners_by_pubkey
                .entry(*pubkey)
                .or_default()
                .insert(record.owner.clone());
            stats.record_reverse_index_insert();
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

fn validate_against_ownership<S: BuildStatsSink>(
    ownership: &ExplicitOwnership,
    canonical_pubkeys: &[Pubkey],
    pubkey_index: &[PubkeyOwnerIndex],
    stats: &mut S,
) -> Result<(), SnapshotBuildError> {
    let derived_pubkeys: Vec<Pubkey> = pubkey_index.iter().map(|row| row.pubkey).collect();
    stats.record_validation_read();
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
        stats.record_validation_read();
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

    /// Fixture row: owner identity, LRU stamp, and normalized group pubkeys.
    type FixtureRow = (ExplicitOwner, u64, Vec<Pubkey>);

    /// Independent oracle — derives expected state solely from fixture rows before SUT.
    #[derive(Debug, Clone)]
    struct FixtureOracle {
        owner_records: BTreeMap<ExplicitOwner, (u64, Vec<Pubkey>)>,
        physical_pubkeys: Vec<Pubkey>,
        pubkey_owner_sets: BTreeMap<Pubkey, BTreeSet<ExplicitOwner>>,
    }

    impl FixtureOracle {
        fn from_fixtures(fixtures: &[FixtureRow]) -> Self {
            let mut owner_records = BTreeMap::new();
            let mut pubkey_owner_sets: BTreeMap<Pubkey, BTreeSet<ExplicitOwner>> = BTreeMap::new();

            for (owner, touch, pubkeys) in fixtures {
                let mut normalized = pubkeys.clone();
                normalized.sort();
                normalized.dedup();
                owner_records.insert(owner.clone(), (*touch, normalized.clone()));
                for pubkey in &normalized {
                    pubkey_owner_sets
                        .entry(*pubkey)
                        .or_default()
                        .insert(owner.clone());
                }
            }

            let physical_pubkeys: Vec<Pubkey> = pubkey_owner_sets.keys().copied().collect();
            Self {
                owner_records,
                physical_pubkeys,
                pubkey_owner_sets,
            }
        }

        fn physical_len(&self) -> usize {
            self.physical_pubkeys.len()
        }

        fn assert_matches(&self, snapshot: &EvictionPlanningSnapshot) {
            assert_eq!(snapshot.physical_len(), self.physical_len());
            assert_eq!(
                snapshot.physical_pubkeys(),
                self.physical_pubkeys.as_slice()
            );
            assert_eq!(snapshot.owners().len(), self.owner_records.len());

            for (owner, (expected_touch, expected_pubkeys)) in &self.owner_records {
                let record = snapshot
                    .owner_record(owner)
                    .unwrap_or_else(|| panic!("missing owner record for {owner:?}"));
                assert_eq!(record.last_touch, *expected_touch);
                assert_eq!(&record.pubkeys, expected_pubkeys);
            }

            for (pubkey, expected_owners) in &self.pubkey_owner_sets {
                let expected_refcount = expected_owners.len();
                assert_eq!(snapshot.owner_refcount(pubkey), expected_refcount);
                let owners = snapshot
                    .pubkey_owners(pubkey)
                    .unwrap_or_else(|| panic!("missing pubkey index row for {pubkey:?}"));
                assert_eq!(owners.len(), expected_refcount);
                assert_eq!(
                    owners.iter().cloned().collect::<BTreeSet<_>>(),
                    *expected_owners
                );
            }
        }
    }

    fn fixtures_to_lru(fixtures: &[FixtureRow]) -> Vec<OwnerLruEntry> {
        fixtures
            .iter()
            .map(|(owner, touch, _)| lru(owner.clone(), *touch))
            .collect()
    }

    fn fixtures_to_ownership(fixtures: &[FixtureRow]) -> ExplicitOwnership {
        let mut ownership = ExplicitOwnership::new();
        for (owner, _, pubkeys) in fixtures {
            upsert(&mut ownership, owner.clone(), pubkeys.iter().copied());
        }
        ownership
    }

    #[test]
    fn empty_ownership_accepts_empty_metadata() {
        let ownership = ExplicitOwnership::new();
        let oracle = FixtureOracle::from_fixtures(&[]);
        let snapshot = EvictionPlanningSnapshot::from_ownership(&ownership, []).unwrap();
        oracle.assert_matches(&snapshot);
        assert_eq!(snapshot.physical_len(), 0);
        assert!(snapshot.owners().is_empty());
        assert!(snapshot.pubkey_index().is_empty());
    }

    #[test]
    fn nonempty_ownership_rejects_missing_metadata_without_short_circuit() {
        let mut ownership = ExplicitOwnership::new();
        let owner = pool_owner(ExplicitConsumer::Tracker, 1);
        upsert(&mut ownership, owner.clone(), [pk(1)]);

        assert_eq!(
            EvictionPlanningSnapshot::from_ownership(&ownership, []),
            Err(SnapshotBuildError::MissingLruEntry(owner.clone()))
        );
    }

    #[test]
    fn single_and_multiple_owner_snapshots() {
        let a = pool_owner(ExplicitConsumer::Tracker, 1);
        let b = pool_owner(ExplicitConsumer::Arb, 2);
        let fixtures = [
            (a.clone(), 100, vec![pk(10)]),
            (b.clone(), 200, vec![pk(11), pk(12)]),
        ];
        let oracle = FixtureOracle::from_fixtures(&fixtures);
        let ownership = fixtures_to_ownership(&fixtures);

        let snapshot =
            EvictionPlanningSnapshot::from_ownership(&ownership, fixtures_to_lru(&fixtures))
                .unwrap();

        assert_eq!(snapshot.physical_len(), 3);
        assert_eq!(snapshot.owners().len(), 2);
        oracle.assert_matches(&snapshot);
    }

    #[test]
    fn shared_keys_have_exact_refcounts_and_owner_sets() {
        let shared = pk(1);
        let a = pool_owner(ExplicitConsumer::Momentum, 1);
        let b = pool_owner(ExplicitConsumer::Arb, 2);
        let fixtures = [
            (a.clone(), 1, vec![shared, pk(2)]),
            (b.clone(), 2, vec![shared]),
        ];
        let oracle = FixtureOracle::from_fixtures(&fixtures);
        let ownership = fixtures_to_ownership(&fixtures);

        let snapshot =
            EvictionPlanningSnapshot::from_ownership(&ownership, fixtures_to_lru(&fixtures))
                .unwrap();

        oracle.assert_matches(&snapshot);
        let row = snapshot
            .pubkey_index()
            .iter()
            .find(|row| row.pubkey == shared)
            .unwrap();
        assert_eq!(row.refcount, 2);
        assert_eq!(row.owners.len(), 2);
    }

    #[test]
    fn same_owner_key_across_consumers_remain_distinct() {
        let pool = pk(5);
        let momentum = ExplicitOwner {
            consumer: ExplicitConsumer::Momentum,
            owner_key: ExplicitOwnerKey::Pool(pool),
        };
        let arb = ExplicitOwner {
            consumer: ExplicitConsumer::Arb,
            owner_key: ExplicitOwnerKey::Pool(pool),
        };
        let fixtures = [
            (momentum.clone(), 7, vec![pk(10)]),
            (arb.clone(), 8, vec![pk(11)]),
        ];
        let oracle = FixtureOracle::from_fixtures(&fixtures);
        let ownership = fixtures_to_ownership(&fixtures);

        let snapshot =
            EvictionPlanningSnapshot::from_ownership(&ownership, fixtures_to_lru(&fixtures))
                .unwrap();

        assert_ne!(momentum, arb);
        oracle.assert_matches(&snapshot);
    }

    #[test]
    fn missing_extra_and_duplicate_lru_are_rejected_without_mutation() {
        let owner = pool_owner(ExplicitConsumer::Tracker, 1);
        let fixtures = [(owner.clone(), 1, vec![pk(1)])];
        let ownership = fixtures_to_ownership(&fixtures);
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
        let o1 = pool_owner(ExplicitConsumer::Tracker, 1);
        let o2 = pool_owner(ExplicitConsumer::Arb, 2);
        let o3 = pool_owner(ExplicitConsumer::Momentum, 3);
        let fixtures = [
            (o1.clone(), 10, vec![pk(1)]),
            (o2.clone(), 20, vec![pk(2)]),
            (o3.clone(), 30, vec![pk(3)]),
        ];
        let ownership = fixtures_to_ownership(&fixtures);

        let s1 = EvictionPlanningSnapshot::from_ownership(&ownership, fixtures_to_lru(&fixtures))
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
    fn physical_len_matches_fixture_physical_set() {
        let shared = pk(1);
        let a = pool_owner(ExplicitConsumer::Tracker, 1);
        let b = pool_owner(ExplicitConsumer::Arb, 2);
        let fixtures = [
            (a.clone(), 1, vec![shared, pk(2)]),
            (b.clone(), 2, vec![shared]),
        ];
        let oracle = FixtureOracle::from_fixtures(&fixtures);
        let ownership = fixtures_to_ownership(&fixtures);

        let snapshot =
            EvictionPlanningSnapshot::from_ownership(&ownership, fixtures_to_lru(&fixtures))
                .unwrap();
        assert_eq!(snapshot.physical_len(), oracle.physical_len());
        assert_eq!(snapshot.physical_len(), 2);
        oracle.assert_matches(&snapshot);
    }

    #[test]
    fn large_graph_build_stats_match_constructor_work() {
        const OWNER_COUNT: usize = 400;
        const KEYS_PER_OWNER: usize = 3;

        let mut fixtures = Vec::with_capacity(OWNER_COUNT);
        for seed in 0..OWNER_COUNT {
            let owner = ExplicitOwner {
                consumer: ExplicitConsumer::Tracker,
                owner_key: ExplicitOwnerKey::Generic(seed as u64),
            };
            let pubkeys: Vec<Pubkey> = (0..KEYS_PER_OWNER)
                .map(|k| pk(((seed * KEYS_PER_OWNER + k) % 250) as u8))
                .collect();
            fixtures.push((owner, seed as u64, pubkeys));
        }

        let oracle = FixtureOracle::from_fixtures(&fixtures);
        let ownership = fixtures_to_ownership(&fixtures);
        let mut stats = BuildStats::default();
        let snapshot = EvictionPlanningSnapshot::from_ownership_with_stats(
            &ownership,
            fixtures_to_lru(&fixtures),
            &mut stats,
        )
        .unwrap();

        let total_edges = OWNER_COUNT * KEYS_PER_OWNER;
        assert_eq!(stats.owner_records_built, OWNER_COUNT as u64);
        assert_eq!(stats.group_key_edges, total_edges as u64);
        assert_eq!(stats.reverse_index_inserts, total_edges as u64);
        assert_eq!(
            stats.validation_reads,
            1 + oracle.physical_len() as u64,
            "one pubkey-list compare plus one refcount read per physical pubkey"
        );
        assert_eq!(snapshot.owners().len(), OWNER_COUNT);
        oracle.assert_matches(&snapshot);
    }

    #[test]
    fn checked_stamps_accept_usize_max_without_counter_arithmetic() {
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        let fixtures = [(owner.clone(), u64::MAX, vec![pk(1)])];
        let oracle = FixtureOracle::from_fixtures(&fixtures);
        let ownership = fixtures_to_ownership(&fixtures);

        let snapshot =
            EvictionPlanningSnapshot::from_ownership(&ownership, fixtures_to_lru(&fixtures))
                .unwrap();
        oracle.assert_matches(&snapshot);
        assert_eq!(snapshot.last_touch(&owner), Some(u64::MAX));
    }

    #[test]
    fn independent_oracle_derives_expected_state_before_sut() {
        let owners = [
            pool_owner(ExplicitConsumer::Wallet, 1),
            pool_owner(ExplicitConsumer::Momentum, 2),
            pool_owner(ExplicitConsumer::Arb, 3),
            pool_owner(ExplicitConsumer::Tracker, 4),
        ];
        let shared = pk(50);
        let fixtures = [
            (owners[0].clone(), 1, vec![pk(10)]),
            (owners[1].clone(), 2, vec![shared, pk(11)]),
            (owners[2].clone(), 3, vec![shared, pk(12)]),
            (owners[3].clone(), 4, vec![pk(13)]),
        ];

        let oracle = FixtureOracle::from_fixtures(&fixtures);
        let ownership = fixtures_to_ownership(&fixtures);
        let snapshot =
            EvictionPlanningSnapshot::from_ownership(&ownership, fixtures_to_lru(&fixtures))
                .unwrap();

        oracle.assert_matches(&snapshot);
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
