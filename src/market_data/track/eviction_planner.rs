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

/// Cumulative eviction tier — explicit protection floor opened for feasibility.
///
/// `Tracker` is the least-protected tier; each higher variant cumulatively includes
/// all less-protected consumers below it (never `Wallet`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum EvictionTier {
    Tracker,
    Arb,
    Momentum,
}

impl EvictionTier {
    fn cumulative_consumers(self) -> &'static [ExplicitConsumer] {
        match self {
            Self::Tracker => &[ExplicitConsumer::Tracker],
            Self::Arb => &[ExplicitConsumer::Tracker, ExplicitConsumer::Arb],
            Self::Momentum => &[
                ExplicitConsumer::Tracker,
                ExplicitConsumer::Arb,
                ExplicitConsumer::Momentum,
            ],
        }
    }

    fn allowed_for_incoming(consumer: ExplicitConsumer) -> &'static [EvictionTier] {
        match consumer {
            ExplicitConsumer::Tracker => &[Self::Tracker],
            ExplicitConsumer::Arb => &[Self::Tracker, Self::Arb],
            ExplicitConsumer::Momentum | ExplicitConsumer::Wallet => {
                &[Self::Tracker, Self::Arb, Self::Momentum]
            }
        }
    }
}

/// Inputs for pure tier-opening feasibility (no victim selection).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TierFeasibilityRequest {
    pub incoming_owner: ExplicitOwner,
    pub incoming_pubkeys: Vec<Pubkey>,
    pub cap: usize,
}

/// Outcome of tier-opening feasibility analysis.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TierFeasibilityResult {
    NoEvictionNeeded {
        incoming_physical_added: usize,
        projected_final_len: usize,
    },
    Feasible {
        incoming_physical_added: usize,
        required_to_free: usize,
        opened_through: EvictionTier,
        maximally_freeable_pubkeys: Vec<Pubkey>,
    },
    RejectedProtected {
        incoming_physical_added: usize,
        required_to_free: usize,
        maximally_freeable_pubkeys: Vec<Pubkey>,
    },
    RejectedInvalidInput,
    InternalInvariantViolation,
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

    /// Pure tier-opening feasibility — no victim selection or mutation.
    pub fn analyze_tier_feasibility(
        &self,
        request: TierFeasibilityRequest,
    ) -> TierFeasibilityResult {
        analyze_tier_feasibility_inner(self, request)
    }

    #[cfg(test)]
    fn test_only(
        physical_len: usize,
        owners: Vec<OwnerPlanningRecord>,
        pubkey_index: Vec<PubkeyOwnerIndex>,
    ) -> Self {
        let physical_pubkeys: Vec<Pubkey> = pubkey_index.iter().map(|row| row.pubkey).collect();
        Self {
            physical_len,
            physical_pubkeys,
            owners,
            pubkey_index,
        }
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

fn analyze_tier_feasibility_inner(
    snapshot: &EvictionPlanningSnapshot,
    request: TierFeasibilityRequest,
) -> TierFeasibilityResult {
    let mut incoming_pubkeys = request.incoming_pubkeys;
    incoming_pubkeys.sort();
    incoming_pubkeys.dedup();

    if incoming_pubkeys.is_empty() {
        return TierFeasibilityResult::RejectedInvalidInput;
    }

    if snapshot.owner_record(&request.incoming_owner).is_some() {
        return TierFeasibilityResult::RejectedInvalidInput;
    }

    let incoming_set: BTreeSet<Pubkey> = incoming_pubkeys.iter().copied().collect();
    let incoming_physical_added = incoming_pubkeys
        .iter()
        .filter(|pubkey| snapshot.owner_refcount(pubkey) == 0)
        .count();

    let projected_final_len = match snapshot.physical_len().checked_add(incoming_physical_added) {
        Some(len) => len,
        None => return TierFeasibilityResult::InternalInvariantViolation,
    };

    if projected_final_len <= request.cap {
        return TierFeasibilityResult::NoEvictionNeeded {
            incoming_physical_added,
            projected_final_len,
        };
    }
    let required_to_free = match projected_final_len.checked_sub(request.cap) {
        Some(needed) => needed,
        None => return TierFeasibilityResult::InternalInvariantViolation,
    };

    let incoming_consumer = request.incoming_owner.consumer;
    let allowed_tiers = EvictionTier::allowed_for_incoming(incoming_consumer);

    let mut evictable_owners_by_tier: BTreeMap<EvictionTier, BTreeSet<ExplicitOwner>> =
        BTreeMap::new();
    for record in snapshot.owners() {
        let consumer = record.owner.consumer;
        if consumer == ExplicitConsumer::Wallet {
            continue;
        }
        for tier in allowed_tiers {
            if EvictionTier::cumulative_consumers(*tier).contains(&consumer) {
                evictable_owners_by_tier
                    .entry(*tier)
                    .or_default()
                    .insert(record.owner.clone());
            }
        }
    }

    let mut freeable_by_tier: BTreeMap<EvictionTier, Vec<Pubkey>> = BTreeMap::new();
    for row in snapshot.pubkey_index() {
        if incoming_set.contains(&row.pubkey) {
            continue;
        }
        for tier in allowed_tiers {
            let evictable = evictable_owners_by_tier
                .get(tier)
                .cloned()
                .unwrap_or_default();
            if row.owners.iter().all(|owner| evictable.contains(owner)) {
                freeable_by_tier.entry(*tier).or_default().push(row.pubkey);
            }
        }
    }

    for pubkeys in freeable_by_tier.values_mut() {
        pubkeys.sort();
        pubkeys.dedup();
    }

    let mut opened_through = None;
    for tier in allowed_tiers {
        let freeable = freeable_by_tier.get(tier).map(Vec::as_slice).unwrap_or(&[]);
        if freeable.len() >= required_to_free {
            opened_through = Some(*tier);
            break;
        }
    }

    match opened_through {
        Some(tier) => TierFeasibilityResult::Feasible {
            incoming_physical_added,
            required_to_free,
            opened_through: tier,
            maximally_freeable_pubkeys: freeable_by_tier.get(&tier).cloned().unwrap_or_default(),
        },
        None => {
            let max_tier = *allowed_tiers.last().unwrap_or(&EvictionTier::Tracker);
            TierFeasibilityResult::RejectedProtected {
                incoming_physical_added,
                required_to_free,
                maximally_freeable_pubkeys: freeable_by_tier
                    .get(&max_tier)
                    .cloned()
                    .unwrap_or_default(),
            }
        }
    }
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

    fn snapshot_from_fixtures(fixtures: &[FixtureRow]) -> EvictionPlanningSnapshot {
        let ownership = fixtures_to_ownership(fixtures);
        EvictionPlanningSnapshot::from_ownership(&ownership, fixtures_to_lru(fixtures)).unwrap()
    }

    fn feasibility_request(
        incoming_owner: ExplicitOwner,
        incoming_pubkeys: Vec<Pubkey>,
        cap: usize,
    ) -> TierFeasibilityRequest {
        TierFeasibilityRequest {
            incoming_owner,
            incoming_pubkeys,
            cap,
        }
    }

    fn assert_feasible(
        result: &TierFeasibilityResult,
        opened_through: EvictionTier,
        required_to_free: usize,
        incoming_physical_added: usize,
        maximally_freeable: &[Pubkey],
    ) {
        match result {
            TierFeasibilityResult::Feasible {
                incoming_physical_added: added,
                required_to_free: needed,
                opened_through: tier,
                maximally_freeable_pubkeys,
            } => {
                assert_eq!(*added, incoming_physical_added);
                assert_eq!(*needed, required_to_free);
                assert_eq!(*tier, opened_through);
                assert_eq!(maximally_freeable_pubkeys.as_slice(), maximally_freeable);
            }
            other => panic!("expected Feasible, got {other:?}"),
        }
    }

    /// Independent exhaustive oracle — does not call production feasibility helpers.
    struct TierFeasibilityOracle {
        physical_len: usize,
        pubkey_owner_sets: BTreeMap<Pubkey, BTreeSet<ExplicitOwner>>,
        evictable_owners_by_tier: BTreeMap<EvictionTier, BTreeSet<ExplicitOwner>>,
    }

    impl TierFeasibilityOracle {
        fn from_fixtures(fixtures: &[FixtureRow]) -> Self {
            let base = FixtureOracle::from_fixtures(fixtures);
            let mut evictable_owners_by_tier: BTreeMap<EvictionTier, BTreeSet<ExplicitOwner>> =
                BTreeMap::new();
            for (owner, _, _) in fixtures {
                if owner.consumer == ExplicitConsumer::Wallet {
                    continue;
                }
                for tier in [
                    EvictionTier::Tracker,
                    EvictionTier::Arb,
                    EvictionTier::Momentum,
                ] {
                    if EvictionTier::cumulative_consumers(tier).contains(&owner.consumer) {
                        evictable_owners_by_tier
                            .entry(tier)
                            .or_default()
                            .insert(owner.clone());
                    }
                }
            }
            Self {
                physical_len: base.physical_len(),
                pubkey_owner_sets: base.pubkey_owner_sets,
                evictable_owners_by_tier,
            }
        }

        fn incoming_physical_added(&self, incoming_pubkeys: &[Pubkey]) -> usize {
            incoming_pubkeys
                .iter()
                .filter(|pk| !self.pubkey_owner_sets.contains_key(pk))
                .count()
        }

        fn maximally_freeable_via_subset_enumeration(
            &self,
            tier: EvictionTier,
            incoming: &BTreeSet<Pubkey>,
        ) -> BTreeSet<Pubkey> {
            let evictable = self
                .evictable_owners_by_tier
                .get(&tier)
                .cloned()
                .unwrap_or_default();
            let owners: Vec<ExplicitOwner> = evictable.into_iter().collect();
            let n = owners.len();
            let mut best = BTreeSet::new();
            let limit = 1usize << n;
            for mask in 0..limit {
                let subset: BTreeSet<ExplicitOwner> = owners
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, owner)| (mask & (1 << idx) != 0).then(|| owner.clone()))
                    .collect();
                let freeable: BTreeSet<Pubkey> = self
                    .pubkey_owner_sets
                    .iter()
                    .filter(|(pubkey, owners)| {
                        !incoming.contains(pubkey) && owners.iter().all(|o| subset.contains(o))
                    })
                    .map(|(pubkey, _)| *pubkey)
                    .collect();
                if freeable.len() > best.len() {
                    best = freeable;
                }
            }
            best
        }

        fn analyze(
            &self,
            incoming_owner: &ExplicitOwner,
            incoming_pubkeys: &[Pubkey],
            cap: usize,
        ) -> TierFeasibilityResult {
            let mut normalized: Vec<Pubkey> = incoming_pubkeys.to_vec();
            normalized.sort();
            normalized.dedup();
            if normalized.is_empty() {
                return TierFeasibilityResult::RejectedInvalidInput;
            }
            let incoming_set: BTreeSet<Pubkey> = normalized.iter().copied().collect();
            let added = self.incoming_physical_added(&normalized);
            let projected = match self.physical_len.checked_add(added) {
                Some(v) => v,
                None => return TierFeasibilityResult::InternalInvariantViolation,
            };
            if projected <= cap {
                return TierFeasibilityResult::NoEvictionNeeded {
                    incoming_physical_added: added,
                    projected_final_len: projected,
                };
            }
            let required = match projected.checked_sub(cap) {
                Some(v) => v,
                None => return TierFeasibilityResult::InternalInvariantViolation,
            };
            let allowed = EvictionTier::allowed_for_incoming(incoming_owner.consumer);
            let mut opened = None;
            let mut freeable_at_opened = BTreeSet::new();
            for tier in allowed {
                let freeable = self.maximally_freeable_via_subset_enumeration(*tier, &incoming_set);
                if freeable.len() >= required {
                    opened = Some(*tier);
                    freeable_at_opened = freeable;
                    break;
                }
            }
            let mut maximally_freeable: Vec<Pubkey> = match opened {
                Some(_) => freeable_at_opened.into_iter().collect(),
                None => {
                    let max_tier = *allowed.last().unwrap_or(&EvictionTier::Tracker);
                    self.maximally_freeable_via_subset_enumeration(max_tier, &incoming_set)
                        .into_iter()
                        .collect()
                }
            };
            maximally_freeable.sort();
            match opened {
                Some(tier) => TierFeasibilityResult::Feasible {
                    incoming_physical_added: added,
                    required_to_free: required,
                    opened_through: tier,
                    maximally_freeable_pubkeys: maximally_freeable,
                },
                None => TierFeasibilityResult::RejectedProtected {
                    incoming_physical_added: added,
                    required_to_free: required,
                    maximally_freeable_pubkeys: maximally_freeable,
                },
            }
        }
    }

    #[test]
    fn tier_feasibility_no_eviction_needed() {
        let tracker = pool_owner(ExplicitConsumer::Tracker, 1);
        let fixtures = [(tracker.clone(), 1, vec![pk(1)])];
        let snapshot = snapshot_from_fixtures(&fixtures);
        let incoming = pool_owner(ExplicitConsumer::Tracker, 9);
        let result =
            snapshot.analyze_tier_feasibility(feasibility_request(incoming, vec![pk(2)], 5));
        assert_eq!(
            result,
            TierFeasibilityResult::NoEvictionNeeded {
                incoming_physical_added: 1,
                projected_final_len: 2,
            }
        );
    }

    #[test]
    fn tier_feasibility_tracker_tier_alone_suffices() {
        let t1 = pool_owner(ExplicitConsumer::Tracker, 1);
        let t2 = pool_owner(ExplicitConsumer::Tracker, 2);
        let fixtures = [(t1.clone(), 1, vec![pk(1)]), (t2.clone(), 2, vec![pk(2)])];
        let snapshot = snapshot_from_fixtures(&fixtures);
        let incoming = pool_owner(ExplicitConsumer::Momentum, 9);
        let result = snapshot.analyze_tier_feasibility(feasibility_request(
            incoming,
            vec![pk(10), pk(11)],
            2,
        ));
        assert_feasible(&result, EvictionTier::Tracker, 2, 2, &[pk(1), pk(2)]);
    }

    #[test]
    fn tier_feasibility_jointly_shared_tracker_pubkeys_suffice() {
        let shared = pk(50);
        let t1 = pool_owner(ExplicitConsumer::Tracker, 1);
        let t2 = pool_owner(ExplicitConsumer::Tracker, 2);
        let fixtures = [
            (t1.clone(), 1, vec![shared]),
            (t2.clone(), 2, vec![shared, pk(2)]),
        ];
        let snapshot = snapshot_from_fixtures(&fixtures);
        let incoming = pool_owner(ExplicitConsumer::Momentum, 9);
        let result =
            snapshot.analyze_tier_feasibility(feasibility_request(incoming, vec![pk(10)], 2));
        assert_feasible(&result, EvictionTier::Tracker, 1, 1, &[pk(2), shared]);
    }

    #[test]
    fn tier_feasibility_tracker_insufficient_arb_cumulative_suffices() {
        let shared = pk(50);
        let tracker = pool_owner(ExplicitConsumer::Tracker, 1);
        let arb = pool_owner(ExplicitConsumer::Arb, 2);
        let fixtures = [
            (tracker.clone(), 1, vec![pk(1), shared]),
            (arb.clone(), 2, vec![shared, pk(3)]),
        ];
        let snapshot = snapshot_from_fixtures(&fixtures);
        let incoming = pool_owner(ExplicitConsumer::Momentum, 9);
        let result = snapshot.analyze_tier_feasibility(feasibility_request(
            incoming,
            vec![pk(10), pk(11)],
            2,
        ));
        assert_feasible(&result, EvictionTier::Arb, 3, 2, &[pk(1), pk(3), shared]);
    }

    #[test]
    fn tier_feasibility_incoming_tracker_cannot_open_arb_or_momentum() {
        let tracker = pool_owner(ExplicitConsumer::Tracker, 1);
        let arb = pool_owner(ExplicitConsumer::Arb, 2);
        let momentum = pool_owner(ExplicitConsumer::Momentum, 3);
        let fixtures = [
            (tracker.clone(), 1, vec![pk(1)]),
            (arb.clone(), 2, vec![pk(2)]),
            (momentum.clone(), 3, vec![pk(3)]),
        ];
        let snapshot = snapshot_from_fixtures(&fixtures);
        let incoming = pool_owner(ExplicitConsumer::Tracker, 9);
        let result = snapshot.analyze_tier_feasibility(feasibility_request(
            incoming.clone(),
            vec![pk(10), pk(11)],
            2,
        ));
        match &result {
            TierFeasibilityResult::RejectedProtected {
                maximally_freeable_pubkeys,
                ..
            } => assert_eq!(maximally_freeable_pubkeys, &[pk(1)]),
            other => panic!("expected RejectedProtected, got {other:?}"),
        }
        let feasible_only_tracker =
            snapshot.analyze_tier_feasibility(feasibility_request(incoming, vec![pk(10)], 3));
        assert_feasible(
            &feasible_only_tracker,
            EvictionTier::Tracker,
            1,
            1,
            &[pk(1)],
        );
    }

    #[test]
    fn tier_feasibility_incoming_arb_cannot_open_momentum() {
        let tracker = pool_owner(ExplicitConsumer::Tracker, 1);
        let arb = pool_owner(ExplicitConsumer::Arb, 2);
        let momentum = pool_owner(ExplicitConsumer::Momentum, 3);
        let fixtures = [
            (tracker.clone(), 1, vec![pk(1)]),
            (arb.clone(), 2, vec![pk(2)]),
            (momentum.clone(), 3, vec![pk(3)]),
        ];
        let snapshot = snapshot_from_fixtures(&fixtures);
        let incoming = pool_owner(ExplicitConsumer::Arb, 9);
        let result = snapshot.analyze_tier_feasibility(feasibility_request(
            incoming,
            vec![pk(10), pk(11), pk(12)],
            2,
        ));
        match &result {
            TierFeasibilityResult::RejectedProtected {
                maximally_freeable_pubkeys,
                ..
            } => assert_eq!(maximally_freeable_pubkeys, &[pk(1), pk(2)]),
            other => panic!("expected RejectedProtected, got {other:?}"),
        }
    }

    #[test]
    fn tier_feasibility_wallet_never_maximally_freeable() {
        let wallet = pool_owner(ExplicitConsumer::Wallet, 1);
        let tracker = pool_owner(ExplicitConsumer::Tracker, 2);
        let shared = pk(50);
        let fixtures = [
            (wallet.clone(), 1, vec![shared, pk(1)]),
            (tracker.clone(), 2, vec![pk(2)]),
        ];
        let snapshot = snapshot_from_fixtures(&fixtures);
        let incoming = pool_owner(ExplicitConsumer::Momentum, 9);
        let result =
            snapshot.analyze_tier_feasibility(feasibility_request(incoming, vec![pk(10)], 3));
        match &result {
            TierFeasibilityResult::Feasible {
                opened_through,
                maximally_freeable_pubkeys,
                required_to_free,
                ..
            } => {
                assert_eq!(*opened_through, EvictionTier::Tracker);
                assert_eq!(*required_to_free, 1);
                assert_eq!(maximally_freeable_pubkeys, &[pk(2)]);
                assert!(!maximally_freeable_pubkeys.contains(&shared));
                assert!(!maximally_freeable_pubkeys.contains(&pk(1)));
            }
            other => panic!("expected Feasible, got {other:?}"),
        }
    }

    #[test]
    fn tier_feasibility_mixed_tracker_arb_pubkey_freeable_only_at_arb_tier() {
        let shared = pk(50);
        let tracker = pool_owner(ExplicitConsumer::Tracker, 1);
        let arb = pool_owner(ExplicitConsumer::Arb, 2);
        let fixtures = [
            (tracker.clone(), 1, vec![shared, pk(1)]),
            (arb.clone(), 2, vec![shared]),
        ];
        let snapshot = snapshot_from_fixtures(&fixtures);
        let incoming = pool_owner(ExplicitConsumer::Momentum, 9);

        let tracker_only = snapshot.analyze_tier_feasibility(feasibility_request(
            incoming.clone(),
            vec![pk(10)],
            2,
        ));
        match &tracker_only {
            TierFeasibilityResult::Feasible {
                opened_through,
                maximally_freeable_pubkeys,
                ..
            } => {
                assert_eq!(*opened_through, EvictionTier::Tracker);
                assert_eq!(maximally_freeable_pubkeys, &[pk(1)]);
                assert!(!maximally_freeable_pubkeys.contains(&shared));
            }
            other => panic!("expected Feasible at Tracker, got {other:?}"),
        }

        let needs_arb = snapshot.analyze_tier_feasibility(feasibility_request(
            incoming,
            vec![pk(10), pk(11)],
            2,
        ));
        assert_feasible(&needs_arb, EvictionTier::Arb, 2, 2, &[pk(1), shared]);
    }

    #[test]
    fn tier_feasibility_incoming_overlap_never_counts_as_free() {
        let tracker = pool_owner(ExplicitConsumer::Tracker, 1);
        let fixtures = [(tracker.clone(), 1, vec![pk(1), pk(2)])];
        let snapshot = snapshot_from_fixtures(&fixtures);
        let incoming = pool_owner(ExplicitConsumer::Momentum, 9);
        let overlap = pk(1);
        let result = snapshot.analyze_tier_feasibility(feasibility_request(
            incoming,
            vec![overlap, pk(10)],
            2,
        ));
        match &result {
            TierFeasibilityResult::Feasible {
                maximally_freeable_pubkeys,
                ..
            } => {
                assert!(!maximally_freeable_pubkeys.contains(&overlap));
                assert_eq!(maximally_freeable_pubkeys, &[pk(2)]);
            }
            other => panic!("expected Feasible, got {other:?}"),
        }
    }

    #[test]
    fn tier_feasibility_protected_rejection_reports_exact_maximal_pubkeys() {
        let wallet = pool_owner(ExplicitConsumer::Wallet, 1);
        let momentum = pool_owner(ExplicitConsumer::Momentum, 2);
        let fixtures = [
            (wallet.clone(), 1, vec![pk(1)]),
            (momentum.clone(), 2, vec![pk(2), pk(3)]),
        ];
        let snapshot = snapshot_from_fixtures(&fixtures);
        let incoming = pool_owner(ExplicitConsumer::Momentum, 9);
        let result = snapshot.analyze_tier_feasibility(feasibility_request(
            incoming,
            vec![pk(10), pk(11)],
            1,
        ));
        match &result {
            TierFeasibilityResult::RejectedProtected {
                required_to_free,
                maximally_freeable_pubkeys,
                ..
            } => {
                assert_eq!(*required_to_free, 4);
                assert_eq!(maximally_freeable_pubkeys, &[pk(2), pk(3)]);
            }
            other => panic!("expected RejectedProtected, got {other:?}"),
        }
    }

    #[test]
    fn tier_feasibility_deterministic_under_permuted_incoming_order() {
        let t1 = pool_owner(ExplicitConsumer::Tracker, 1);
        let t2 = pool_owner(ExplicitConsumer::Tracker, 2);
        let fixtures = [(t1.clone(), 1, vec![pk(1)]), (t2.clone(), 2, vec![pk(2)])];
        let snapshot = snapshot_from_fixtures(&fixtures);
        let incoming = pool_owner(ExplicitConsumer::Momentum, 9);
        let a = snapshot.analyze_tier_feasibility(feasibility_request(
            incoming.clone(),
            vec![pk(10), pk(11), pk(12)],
            1,
        ));
        let b = snapshot.analyze_tier_feasibility(feasibility_request(
            incoming,
            vec![pk(12), pk(10), pk(11)],
            1,
        ));
        assert_eq!(a, b);
    }

    #[test]
    fn tier_feasibility_rejects_duplicate_empty_and_existing_incoming() {
        let tracker = pool_owner(ExplicitConsumer::Tracker, 1);
        let fixtures = [(tracker.clone(), 1, vec![pk(1)])];
        let snapshot = snapshot_from_fixtures(&fixtures);
        let incoming = pool_owner(ExplicitConsumer::Momentum, 9);

        assert_eq!(
            snapshot.analyze_tier_feasibility(feasibility_request(incoming.clone(), vec![], 5)),
            TierFeasibilityResult::RejectedInvalidInput
        );
        assert_eq!(
            snapshot.analyze_tier_feasibility(feasibility_request(tracker.clone(), vec![pk(2)], 5)),
            TierFeasibilityResult::RejectedInvalidInput
        );

        let dup = snapshot.analyze_tier_feasibility(feasibility_request(
            incoming.clone(),
            vec![pk(2), pk(2)],
            5,
        ));
        assert!(matches!(
            dup,
            TierFeasibilityResult::NoEvictionNeeded { .. }
        ));
    }

    #[test]
    fn tier_feasibility_usize_max_checked_fail_closed() {
        let tracker = pool_owner(ExplicitConsumer::Tracker, 1);
        let snapshot = EvictionPlanningSnapshot::test_only(
            usize::MAX,
            vec![OwnerPlanningRecord {
                owner: tracker.clone(),
                last_touch: 1,
                pubkeys: vec![pk(1)],
            }],
            vec![PubkeyOwnerIndex {
                pubkey: pk(1),
                refcount: 1,
                owners: vec![tracker],
            }],
        );
        let incoming = pool_owner(ExplicitConsumer::Momentum, 9);
        let result =
            snapshot.analyze_tier_feasibility(feasibility_request(incoming, vec![pk(2)], 0));
        assert_eq!(result, TierFeasibilityResult::InternalInvariantViolation);
    }

    #[test]
    fn tier_feasibility_exhaustive_oracle_matches_small_fixture_graphs() {
        let graphs: Vec<Vec<FixtureRow>> = vec![
            vec![
                (pool_owner(ExplicitConsumer::Tracker, 1), 1, vec![pk(1)]),
                (pool_owner(ExplicitConsumer::Arb, 2), 2, vec![pk(2)]),
            ],
            vec![
                (
                    pool_owner(ExplicitConsumer::Tracker, 1),
                    1,
                    vec![pk(10), pk(11)],
                ),
                (
                    pool_owner(ExplicitConsumer::Arb, 2),
                    2,
                    vec![pk(11), pk(12)],
                ),
                (pool_owner(ExplicitConsumer::Momentum, 3), 3, vec![pk(12)]),
            ],
            vec![
                (pool_owner(ExplicitConsumer::Wallet, 1), 1, vec![pk(20)]),
                (pool_owner(ExplicitConsumer::Tracker, 2), 2, vec![pk(21)]),
            ],
        ];

        let incoming_owners = [
            pool_owner(ExplicitConsumer::Tracker, 50),
            pool_owner(ExplicitConsumer::Arb, 51),
            pool_owner(ExplicitConsumer::Momentum, 52),
            pool_owner(ExplicitConsumer::Wallet, 53),
        ];
        let incoming_keys = [vec![pk(100)], vec![pk(101), pk(102)], vec![pk(103)]];

        for fixtures in graphs {
            let snapshot = snapshot_from_fixtures(&fixtures);
            let oracle = TierFeasibilityOracle::from_fixtures(&fixtures);
            for incoming_owner in &incoming_owners {
                if fixtures.iter().any(|(o, _, _)| o == incoming_owner) {
                    continue;
                }
                for keys in &incoming_keys {
                    for cap in [0usize, 1, 2, 5] {
                        let request =
                            feasibility_request(incoming_owner.clone(), keys.clone(), cap);
                        let actual = snapshot.analyze_tier_feasibility(request);
                        let expected = oracle.analyze(incoming_owner, keys, cap);
                        assert_eq!(actual, expected, "fixtures={fixtures:?}");
                    }
                }
            }
        }
    }
}
