//! Validated eviction planning snapshot and pure eviction planning (PR 1c1a/1c1b).
//!
//! Side-effect-free read model over [`ExplicitOwnership`] public snapshot APIs:
//! validated [`EvictionPlanningSnapshot`] construction, tier-opening
//! [`EvictionPlanningSnapshot::analyze_tier_feasibility`], and priority/LRU
//! [`select_eviction_victims`]. No admission, cap-shrink, touch-state mutation,
//! or runtime wiring.
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

    let empty_evictable_owners = BTreeSet::new();
    let mut freeable_by_tier: BTreeMap<EvictionTier, Vec<Pubkey>> = BTreeMap::new();
    for row in snapshot.pubkey_index() {
        if incoming_set.contains(&row.pubkey) {
            continue;
        }
        for tier in allowed_tiers {
            let evictable = evictable_owners_by_tier
                .get(tier)
                .unwrap_or(&empty_evictable_owners);
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

/// Inputs for pure victim selection (same shape as tier feasibility).
pub type VictimSelectionRequest = TierFeasibilityRequest;

/// Concrete victim plan with exact pubkey deltas.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VictimSelectionPlan {
    pub victims: Vec<ExplicitOwner>,
    pub physical_freed: Vec<Pubkey>,
    pub incoming_physical_added: usize,
    pub projected_final_len: usize,
    pub opened_through: EvictionTier,
}

/// Outcome of priority/LRU victim selection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VictimSelectionResult {
    NoEvictionNeeded {
        incoming_physical_added: usize,
        projected_final_len: usize,
    },
    Planned(VictimSelectionPlan),
    RejectedProtected {
        incoming_physical_added: usize,
        required_to_free: usize,
    },
    RejectedInvalidInput,
    InternalInvariantViolation,
}

/// Concrete victim plan for cap shrink (no incoming group).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapShrinkSelectionPlan {
    pub victims: Vec<ExplicitOwner>,
    pub physical_freed: Vec<Pubkey>,
    pub projected_final_len: usize,
    pub opened_through: EvictionTier,
}

/// Outcome of priority/LRU victim selection for cap shrink.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CapShrinkSelectionResult {
    /// `physical_len <= target_cap`; no victims required.
    NoShrinkNeeded,
    Planned(CapShrinkSelectionPlan),
    RejectedProtected {
        required_to_free: usize,
    },
    InternalInvariantViolation,
}

/// Test-only counters for selector hot loops (not clone/tautology metrics).
#[cfg(test)]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SelectorStats {
    pub initial_edges: u64,
    pub candidate_evaluations: u64,
    pub incremental_refcount_updates: u64,
    pub package_evaluations: u64,
}

trait SelectorStatsSink {
    fn record_initial_edges(&mut self, count: u64);
    fn record_candidate_evaluation(&mut self);
    fn record_refcount_update(&mut self);
    fn record_package_evaluation(&mut self);
}

struct NoopSelectorStats;

impl SelectorStatsSink for NoopSelectorStats {
    fn record_initial_edges(&mut self, _count: u64) {}
    fn record_candidate_evaluation(&mut self) {}
    fn record_refcount_update(&mut self) {}
    fn record_package_evaluation(&mut self) {}
}

#[cfg(test)]
impl SelectorStatsSink for SelectorStats {
    fn record_initial_edges(&mut self, count: u64) {
        self.initial_edges += count;
    }
    fn record_candidate_evaluation(&mut self) {
        self.candidate_evaluations += 1;
    }
    fn record_refcount_update(&mut self) {
        self.incremental_refcount_updates += 1;
    }
    fn record_package_evaluation(&mut self) {
        self.package_evaluations += 1;
    }
}

/// Eviction priority ordinal: lower = evicted first (`Tracker` before `Arb` before `Momentum`).
fn consumer_eviction_priority(consumer: ExplicitConsumer) -> u8 {
    match consumer {
        ExplicitConsumer::Tracker => 0,
        ExplicitConsumer::Arb => 1,
        ExplicitConsumer::Momentum => 2,
        ExplicitConsumer::Wallet => u8::MAX,
    }
}

/// Side-effect-free victim selection — calls tier feasibility internally.
pub fn select_eviction_victims(
    snapshot: &EvictionPlanningSnapshot,
    request: VictimSelectionRequest,
) -> VictimSelectionResult {
    select_eviction_victims_inner(snapshot, request, &mut NoopSelectorStats)
}

#[cfg(test)]
pub fn select_eviction_victims_with_stats(
    snapshot: &EvictionPlanningSnapshot,
    request: VictimSelectionRequest,
    stats: &mut SelectorStats,
) -> VictimSelectionResult {
    select_eviction_victims_inner(snapshot, request, stats)
}

fn select_eviction_victims_inner<S: SelectorStatsSink>(
    snapshot: &EvictionPlanningSnapshot,
    request: VictimSelectionRequest,
    stats: &mut S,
) -> VictimSelectionResult {
    let feasibility = snapshot.analyze_tier_feasibility(request.clone());

    match feasibility {
        TierFeasibilityResult::NoEvictionNeeded {
            incoming_physical_added,
            projected_final_len,
        } => VictimSelectionResult::NoEvictionNeeded {
            incoming_physical_added,
            projected_final_len,
        },
        TierFeasibilityResult::RejectedInvalidInput => VictimSelectionResult::RejectedInvalidInput,
        TierFeasibilityResult::InternalInvariantViolation => {
            VictimSelectionResult::InternalInvariantViolation
        }
        TierFeasibilityResult::RejectedProtected {
            incoming_physical_added,
            required_to_free,
            ..
        } => VictimSelectionResult::RejectedProtected {
            incoming_physical_added,
            required_to_free,
        },
        TierFeasibilityResult::Feasible {
            incoming_physical_added,
            required_to_free,
            opened_through,
            ..
        } => run_victim_selection(
            snapshot,
            &request,
            incoming_physical_added,
            required_to_free,
            opened_through,
            stats,
        ),
    }
}

fn run_victim_selection<S: SelectorStatsSink>(
    snapshot: &EvictionPlanningSnapshot,
    request: &VictimSelectionRequest,
    incoming_physical_added: usize,
    required_to_free: usize,
    opened_through: EvictionTier,
    stats: &mut S,
) -> VictimSelectionResult {
    let mut incoming_pubkeys = request.incoming_pubkeys.clone();
    incoming_pubkeys.sort();
    incoming_pubkeys.dedup();
    let incoming_set: BTreeSet<Pubkey> = incoming_pubkeys.iter().copied().collect();

    let Some((victims, physical_freed)) = execute_victim_selection(
        snapshot,
        &incoming_set,
        required_to_free,
        opened_through,
        stats,
    ) else {
        return VictimSelectionResult::InternalInvariantViolation;
    };

    let projected_final_len = match snapshot
        .physical_len()
        .checked_add(incoming_physical_added)
        .and_then(|v| v.checked_sub(physical_freed.len()))
    {
        Some(v) => v,
        None => return VictimSelectionResult::InternalInvariantViolation,
    };

    VictimSelectionResult::Planned(VictimSelectionPlan {
        victims,
        physical_freed,
        incoming_physical_added,
        projected_final_len,
        opened_through,
    })
}

enum CapShrinkFeasibilityOutcome {
    NoShrinkNeeded,
    Feasible {
        required_to_free: usize,
        opened_through: EvictionTier,
    },
    RejectedProtected {
        required_to_free: usize,
    },
    InternalInvariantViolation,
}

const CAP_SHRINK_ALLOWED_TIERS: &[EvictionTier] = &[
    EvictionTier::Tracker,
    EvictionTier::Arb,
    EvictionTier::Momentum,
];

fn analyze_cap_shrink_feasibility(
    snapshot: &EvictionPlanningSnapshot,
    target_cap: usize,
) -> CapShrinkFeasibilityOutcome {
    let physical_len = snapshot.physical_len();
    if physical_len <= target_cap {
        return CapShrinkFeasibilityOutcome::NoShrinkNeeded;
    }
    let required_to_free = match physical_len.checked_sub(target_cap) {
        Some(needed) => needed,
        None => return CapShrinkFeasibilityOutcome::InternalInvariantViolation,
    };

    let mut evictable_owners_by_tier: BTreeMap<EvictionTier, BTreeSet<ExplicitOwner>> =
        BTreeMap::new();
    for record in snapshot.owners() {
        let consumer = record.owner.consumer;
        if consumer == ExplicitConsumer::Wallet {
            continue;
        }
        for tier in CAP_SHRINK_ALLOWED_TIERS {
            if EvictionTier::cumulative_consumers(*tier).contains(&consumer) {
                evictable_owners_by_tier
                    .entry(*tier)
                    .or_default()
                    .insert(record.owner.clone());
            }
        }
    }

    let empty_evictable_owners = BTreeSet::new();
    let mut freeable_by_tier: BTreeMap<EvictionTier, Vec<Pubkey>> = BTreeMap::new();
    for row in snapshot.pubkey_index() {
        for tier in CAP_SHRINK_ALLOWED_TIERS {
            let evictable = evictable_owners_by_tier
                .get(tier)
                .unwrap_or(&empty_evictable_owners);
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
    for tier in CAP_SHRINK_ALLOWED_TIERS {
        let freeable = freeable_by_tier.get(tier).map(Vec::as_slice).unwrap_or(&[]);
        if freeable.len() >= required_to_free {
            opened_through = Some(*tier);
            break;
        }
    }

    match opened_through {
        Some(tier) => CapShrinkFeasibilityOutcome::Feasible {
            required_to_free,
            opened_through: tier,
        },
        None => CapShrinkFeasibilityOutcome::RejectedProtected { required_to_free },
    }
}

/// Side-effect-free victim selection for cap shrink — no incoming group.
pub fn select_cap_shrink_victims(
    snapshot: &EvictionPlanningSnapshot,
    target_cap: usize,
) -> CapShrinkSelectionResult {
    select_cap_shrink_victims_inner(snapshot, target_cap, &mut NoopSelectorStats)
}

#[cfg(test)]
pub fn select_cap_shrink_victims_with_stats(
    snapshot: &EvictionPlanningSnapshot,
    target_cap: usize,
    stats: &mut SelectorStats,
) -> CapShrinkSelectionResult {
    select_cap_shrink_victims_inner(snapshot, target_cap, stats)
}

fn select_cap_shrink_victims_inner<S: SelectorStatsSink>(
    snapshot: &EvictionPlanningSnapshot,
    target_cap: usize,
    stats: &mut S,
) -> CapShrinkSelectionResult {
    match analyze_cap_shrink_feasibility(snapshot, target_cap) {
        CapShrinkFeasibilityOutcome::NoShrinkNeeded => CapShrinkSelectionResult::NoShrinkNeeded,
        CapShrinkFeasibilityOutcome::InternalInvariantViolation => {
            CapShrinkSelectionResult::InternalInvariantViolation
        }
        CapShrinkFeasibilityOutcome::RejectedProtected { required_to_free } => {
            CapShrinkSelectionResult::RejectedProtected { required_to_free }
        }
        CapShrinkFeasibilityOutcome::Feasible {
            required_to_free,
            opened_through,
        } => {
            let incoming_set = BTreeSet::new();
            let Some((victims, physical_freed)) = execute_victim_selection(
                snapshot,
                &incoming_set,
                required_to_free,
                opened_through,
                stats,
            ) else {
                return CapShrinkSelectionResult::InternalInvariantViolation;
            };

            let projected_final_len =
                match snapshot.physical_len().checked_sub(physical_freed.len()) {
                    Some(v) => v,
                    None => return CapShrinkSelectionResult::InternalInvariantViolation,
                };

            CapShrinkSelectionResult::Planned(CapShrinkSelectionPlan {
                victims,
                physical_freed,
                projected_final_len,
                opened_through,
            })
        }
    }
}

fn execute_victim_selection<S: SelectorStatsSink>(
    snapshot: &EvictionPlanningSnapshot,
    incoming_set: &BTreeSet<Pubkey>,
    required_to_free: usize,
    opened_through: EvictionTier,
    stats: &mut S,
) -> Option<(Vec<ExplicitOwner>, Vec<Pubkey>)> {
    let cumulative = EvictionTier::cumulative_consumers(opened_through);
    let allowed_owners: BTreeSet<ExplicitOwner> = snapshot
        .owners()
        .iter()
        .filter(|record| {
            record.owner.consumer != ExplicitConsumer::Wallet
                && cumulative.contains(&record.owner.consumer)
        })
        .map(|record| record.owner.clone())
        .collect();

    let candidates = build_sorted_candidates(snapshot, &allowed_owners);

    let pubkey_index = snapshot.pubkey_index();
    let projected_refcounts: Vec<usize> = pubkey_index.iter().map(|row| row.refcount).collect();
    let initial_edge_count: u64 = pubkey_index.iter().map(|row| row.owners.len() as u64).sum();
    stats.record_initial_edges(initial_edge_count);

    let mut owner_pubkey_indices: BTreeMap<ExplicitOwner, Vec<usize>> = BTreeMap::new();
    for (idx, row) in pubkey_index.iter().enumerate() {
        for owner in &row.owners {
            owner_pubkey_indices
                .entry(owner.clone())
                .or_default()
                .push(idx);
        }
    }
    for indices in owner_pubkey_indices.values_mut() {
        indices.sort_unstable();
        indices.dedup();
    }

    let mut workspace = SelectorWorkspace {
        pubkey_index,
        owner_pubkey_indices,
        projected_refcounts,
        incoming_set: incoming_set.clone(),
        allowed_owners,
        selected: BTreeSet::new(),
        victims: Vec::new(),
        freed_pubkeys: BTreeSet::new(),
    };

    while workspace.freed_pubkeys.len() < required_to_free {
        let mut picked_positive = false;
        for owner in &candidates {
            if workspace.selected.contains(owner) {
                continue;
            }
            stats.record_candidate_evaluation();
            if workspace.positive_marginal_count(owner) > 0 {
                workspace.select_owner(owner, stats);
                picked_positive = true;
                break;
            }
        }

        if picked_positive {
            continue;
        }

        let packages = workspace.collect_joint_packages(stats);

        let best_package = packages
            .into_iter()
            .min_by(|a, b| compare_joint_packages(a, b, snapshot))?;

        for owner in &candidates {
            if best_package.contains(owner) && !workspace.selected.contains(owner) {
                workspace.select_owner(owner, stats);
            }
        }
    }

    let physical_freed: Vec<Pubkey> = workspace.freed_pubkeys.into_iter().collect();
    Some((workspace.victims, physical_freed))
}

struct SelectorWorkspace<'a> {
    pubkey_index: &'a [PubkeyOwnerIndex],
    owner_pubkey_indices: BTreeMap<ExplicitOwner, Vec<usize>>,
    projected_refcounts: Vec<usize>,
    incoming_set: BTreeSet<Pubkey>,
    allowed_owners: BTreeSet<ExplicitOwner>,
    selected: BTreeSet<ExplicitOwner>,
    victims: Vec<ExplicitOwner>,
    freed_pubkeys: BTreeSet<Pubkey>,
}

impl SelectorWorkspace<'_> {
    fn positive_marginal_count(&self, owner: &ExplicitOwner) -> usize {
        let Some(indices) = self.owner_pubkey_indices.get(owner) else {
            return 0;
        };
        indices
            .iter()
            .filter(|&&idx| {
                let pubkey = self.pubkey_index[idx].pubkey;
                !self.incoming_set.contains(&pubkey) && self.projected_refcounts[idx] == 1
            })
            .count()
    }

    fn select_owner<S: SelectorStatsSink>(&mut self, owner: &ExplicitOwner, stats: &mut S) {
        self.selected.insert(owner.clone());
        self.victims.push(owner.clone());

        if let Some(indices) = self.owner_pubkey_indices.get(owner) {
            for &idx in indices {
                let prev = self.projected_refcounts[idx];
                if prev == 0 {
                    continue;
                }
                self.projected_refcounts[idx] = prev - 1;
                stats.record_refcount_update();
                if self.projected_refcounts[idx] == 0 {
                    let pubkey = self.pubkey_index[idx].pubkey;
                    if !self.incoming_set.contains(&pubkey) {
                        self.freed_pubkeys.insert(pubkey);
                    }
                }
            }
        }
    }

    fn collect_joint_packages<S: SelectorStatsSink>(
        &self,
        stats: &mut S,
    ) -> Vec<BTreeSet<ExplicitOwner>> {
        let mut packages: BTreeSet<BTreeSet<ExplicitOwner>> = BTreeSet::new();

        for (idx, row) in self.pubkey_index.iter().enumerate() {
            if self.incoming_set.contains(&row.pubkey) {
                continue;
            }
            if self.projected_refcounts[idx] == 0 {
                continue;
            }

            let remaining: BTreeSet<ExplicitOwner> = row
                .owners
                .iter()
                .filter(|owner| {
                    self.allowed_owners.contains(*owner) && !self.selected.contains(*owner)
                })
                .cloned()
                .collect();

            if remaining.is_empty() {
                continue;
            }
            if remaining.len() != self.projected_refcounts[idx] {
                continue;
            }

            stats.record_package_evaluation();
            packages.insert(remaining);
        }

        packages.into_iter().collect()
    }
}

fn build_sorted_candidates(
    snapshot: &EvictionPlanningSnapshot,
    allowed_owners: &BTreeSet<ExplicitOwner>,
) -> Vec<ExplicitOwner> {
    let mut records: Vec<&OwnerPlanningRecord> = snapshot
        .owners()
        .iter()
        .filter(|record| allowed_owners.contains(&record.owner))
        .collect();
    records.sort_by(|a, b| {
        consumer_eviction_priority(a.owner.consumer)
            .cmp(&consumer_eviction_priority(b.owner.consumer))
            .then(a.last_touch.cmp(&b.last_touch))
            .then(a.owner.cmp(&b.owner))
    });
    records
        .into_iter()
        .map(|record| record.owner.clone())
        .collect()
}

fn owner_sort_tuple(
    owner: &ExplicitOwner,
    snapshot: &EvictionPlanningSnapshot,
) -> (u8, u64, ExplicitOwner) {
    (
        consumer_eviction_priority(owner.consumer),
        snapshot.last_touch(owner).unwrap_or(0),
        owner.clone(),
    )
}

fn compare_joint_packages(
    a: &BTreeSet<ExplicitOwner>,
    b: &BTreeSet<ExplicitOwner>,
    snapshot: &EvictionPlanningSnapshot,
) -> std::cmp::Ordering {
    let max_a = a
        .iter()
        .map(|o| consumer_eviction_priority(o.consumer))
        .max()
        .unwrap_or(0);
    let max_b = b
        .iter()
        .map(|o| consumer_eviction_priority(o.consumer))
        .max()
        .unwrap_or(0);
    max_a
        .cmp(&max_b)
        .then_with(|| {
            let mut keys_a: Vec<_> = a.iter().map(|o| owner_sort_tuple(o, snapshot)).collect();
            let mut keys_b: Vec<_> = b.iter().map(|o| owner_sort_tuple(o, snapshot)).collect();
            keys_a.sort();
            keys_b.sort();
            keys_a.cmp(&keys_b)
        })
        .then_with(|| a.len().cmp(&b.len()))
        .then_with(|| {
            let mut owners_a: Vec<_> = a.iter().cloned().collect();
            let mut owners_b: Vec<_> = b.iter().cloned().collect();
            owners_a.sort();
            owners_b.sort();
            owners_a.cmp(&owners_b)
        })
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

    /// Independent exhaustive oracle — hard-coded policy only; no production feasibility helpers.
    struct TierFeasibilityOracle {
        physical_len: usize,
        pubkey_owner_sets: BTreeMap<Pubkey, BTreeSet<ExplicitOwner>>,
        evictable_owners_by_tier: BTreeMap<OracleTier, BTreeSet<ExplicitOwner>>,
    }

    /// Test-only tier label — policy is duplicated here, not delegated to production helpers.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    enum OracleTier {
        Tracker,
        Arb,
        Momentum,
    }

    impl OracleTier {
        fn to_eviction_tier(self) -> EvictionTier {
            match self {
                Self::Tracker => EvictionTier::Tracker,
                Self::Arb => EvictionTier::Arb,
                Self::Momentum => EvictionTier::Momentum,
            }
        }
    }

    fn oracle_allowed_tiers(incoming: ExplicitConsumer) -> &'static [OracleTier] {
        match incoming {
            ExplicitConsumer::Tracker => &[OracleTier::Tracker],
            ExplicitConsumer::Arb => &[OracleTier::Tracker, OracleTier::Arb],
            ExplicitConsumer::Momentum | ExplicitConsumer::Wallet => {
                &[OracleTier::Tracker, OracleTier::Arb, OracleTier::Momentum]
            }
        }
    }

    fn oracle_owner_in_cumulative_tier(owner: &ExplicitOwner, tier: OracleTier) -> bool {
        match owner.consumer {
            ExplicitConsumer::Wallet => false,
            ExplicitConsumer::Tracker => true,
            ExplicitConsumer::Arb => matches!(tier, OracleTier::Arb | OracleTier::Momentum),
            ExplicitConsumer::Momentum => tier == OracleTier::Momentum,
        }
    }

    impl TierFeasibilityOracle {
        fn from_fixtures(fixtures: &[FixtureRow]) -> Self {
            let mut pubkey_owner_sets: BTreeMap<Pubkey, BTreeSet<ExplicitOwner>> = BTreeMap::new();
            let mut all_owners = BTreeSet::new();
            for (owner, _, pubkeys) in fixtures {
                all_owners.insert(owner.clone());
                for pubkey in pubkeys {
                    pubkey_owner_sets
                        .entry(*pubkey)
                        .or_default()
                        .insert(owner.clone());
                }
            }

            let mut evictable_owners_by_tier: BTreeMap<OracleTier, BTreeSet<ExplicitOwner>> =
                BTreeMap::new();
            for tier in [OracleTier::Tracker, OracleTier::Arb, OracleTier::Momentum] {
                for owner in &all_owners {
                    if oracle_owner_in_cumulative_tier(owner, tier) {
                        evictable_owners_by_tier
                            .entry(tier)
                            .or_default()
                            .insert(owner.clone());
                    }
                }
            }

            Self {
                physical_len: pubkey_owner_sets.len(),
                pubkey_owner_sets,
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
            tier: OracleTier,
            incoming: &BTreeSet<Pubkey>,
        ) -> BTreeSet<Pubkey> {
            let empty = BTreeSet::new();
            let evictable = self.evictable_owners_by_tier.get(&tier).unwrap_or(&empty);
            let owners: Vec<&ExplicitOwner> = evictable.iter().collect();
            let n = owners.len();
            let mut best = BTreeSet::new();
            let limit = 1usize << n;
            for mask in 0..limit {
                let subset: BTreeSet<&ExplicitOwner> = owners
                    .iter()
                    .enumerate()
                    .filter(|(idx, _)| mask & (1 << idx) != 0)
                    .map(|(_, owner)| *owner)
                    .collect();
                let freeable: BTreeSet<Pubkey> = self
                    .pubkey_owner_sets
                    .iter()
                    .filter(|(pubkey, owner_set)| {
                        !incoming.contains(pubkey)
                            && owner_set.iter().all(|owner| subset.contains(owner))
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
            let allowed = oracle_allowed_tiers(incoming_owner.consumer);
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
                    let max_tier = *allowed.last().unwrap_or(&OracleTier::Tracker);
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
                    opened_through: tier.to_eviction_tier(),
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
        let shared_tracker_arb = pk(11);
        let shared_tracker_momentum = pk(50);
        let wallet_only = pk(20);
        let tracker_only = pk(1);

        let graphs: Vec<Vec<FixtureRow>> = vec![
            vec![
                (
                    pool_owner(ExplicitConsumer::Tracker, 1),
                    1,
                    vec![tracker_only],
                ),
                (pool_owner(ExplicitConsumer::Arb, 2), 2, vec![pk(2)]),
            ],
            vec![
                (
                    pool_owner(ExplicitConsumer::Tracker, 1),
                    1,
                    vec![pk(10), shared_tracker_arb],
                ),
                (
                    pool_owner(ExplicitConsumer::Arb, 2),
                    2,
                    vec![shared_tracker_arb, pk(12)],
                ),
                (
                    pool_owner(ExplicitConsumer::Momentum, 3),
                    3,
                    vec![pk(12), shared_tracker_momentum],
                ),
            ],
            vec![
                (
                    pool_owner(ExplicitConsumer::Wallet, 1),
                    1,
                    vec![wallet_only, pk(21)],
                ),
                (
                    pool_owner(ExplicitConsumer::Tracker, 2),
                    2,
                    vec![pk(21), shared_tracker_momentum],
                ),
                (
                    pool_owner(ExplicitConsumer::Arb, 3),
                    3,
                    vec![shared_tracker_momentum, shared_tracker_arb],
                ),
            ],
            vec![
                (
                    pool_owner(ExplicitConsumer::Tracker, 4),
                    4,
                    vec![shared_tracker_arb],
                ),
                (
                    pool_owner(ExplicitConsumer::Arb, 5),
                    5,
                    vec![shared_tracker_arb, pk(30)],
                ),
                (
                    pool_owner(ExplicitConsumer::Momentum, 6),
                    6,
                    vec![pk(30), pk(31)],
                ),
            ],
        ];

        let incoming_owners = [
            pool_owner(ExplicitConsumer::Tracker, 50),
            pool_owner(ExplicitConsumer::Arb, 51),
            pool_owner(ExplicitConsumer::Momentum, 52),
            pool_owner(ExplicitConsumer::Wallet, 53),
        ];

        let incoming_key_matrix = [
            vec![pk(100)],
            vec![pk(101), pk(102)],
            vec![pk(103)],
            vec![tracker_only],
            vec![shared_tracker_arb],
            vec![shared_tracker_momentum],
            vec![wallet_only],
            vec![pk(100), tracker_only],
            vec![pk(101), shared_tracker_arb],
            vec![pk(102), shared_tracker_momentum],
            vec![pk(103), wallet_only],
            vec![tracker_only, shared_tracker_arb, shared_tracker_momentum],
        ];

        for fixtures in graphs {
            let snapshot = snapshot_from_fixtures(&fixtures);
            let oracle = TierFeasibilityOracle::from_fixtures(&fixtures);
            for incoming_owner in &incoming_owners {
                if fixtures.iter().any(|(o, _, _)| o == incoming_owner) {
                    continue;
                }
                for keys in &incoming_key_matrix {
                    for cap in [0usize, 1, 2, 3, 5] {
                        let request =
                            feasibility_request(incoming_owner.clone(), keys.clone(), cap);
                        let actual = snapshot.analyze_tier_feasibility(request);
                        let expected = oracle.analyze(incoming_owner, keys, cap);
                        assert_eq!(
                            actual, expected,
                            "fixtures={fixtures:?} incoming={incoming_owner:?} keys={keys:?} cap={cap}"
                        );
                    }
                }
            }
        }
    }

    // --- Victim selection tests (PR 1c1b) ---

    fn victim_request(
        incoming_owner: ExplicitOwner,
        incoming_pubkeys: Vec<Pubkey>,
        cap: usize,
    ) -> VictimSelectionRequest {
        VictimSelectionRequest {
            incoming_owner,
            incoming_pubkeys,
            cap,
        }
    }

    fn assert_planned(
        result: &VictimSelectionResult,
        victims: &[ExplicitOwner],
        physical_freed: &[Pubkey],
        opened_through: EvictionTier,
        incoming_physical_added: usize,
        projected_final_len: usize,
    ) {
        match result {
            VictimSelectionResult::Planned(plan) => {
                assert_eq!(plan.victims.as_slice(), victims);
                assert_eq!(plan.physical_freed.as_slice(), physical_freed);
                assert_eq!(plan.opened_through, opened_through);
                assert_eq!(plan.incoming_physical_added, incoming_physical_added);
                assert_eq!(plan.projected_final_len, projected_final_len);
            }
            other => panic!("expected Planned, got {other:?}"),
        }
    }

    #[test]
    fn victim_selection_no_eviction_needed() {
        let tracker = pool_owner(ExplicitConsumer::Tracker, 1);
        let fixtures = [(tracker.clone(), 1, vec![pk(1)])];
        let snapshot = snapshot_from_fixtures(&fixtures);
        let incoming = pool_owner(ExplicitConsumer::Tracker, 9);
        let result = select_eviction_victims(&snapshot, victim_request(incoming, vec![pk(2)], 5));
        assert_eq!(
            result,
            VictimSelectionResult::NoEvictionNeeded {
                incoming_physical_added: 1,
                projected_final_len: 2,
            }
        );
    }

    #[test]
    fn victim_selection_tracker_before_arb_momentum() {
        let tracker = pool_owner(ExplicitConsumer::Tracker, 1);
        let arb = pool_owner(ExplicitConsumer::Arb, 2);
        let momentum = pool_owner(ExplicitConsumer::Momentum, 3);
        let fixtures = [
            (momentum.clone(), 30, vec![pk(3)]),
            (arb.clone(), 20, vec![pk(2)]),
            (tracker.clone(), 10, vec![pk(1)]),
        ];
        let snapshot = snapshot_from_fixtures(&fixtures);
        let incoming = pool_owner(ExplicitConsumer::Momentum, 9);
        let result = select_eviction_victims(&snapshot, victim_request(incoming, vec![pk(10)], 3));
        assert_planned(
            &result,
            std::slice::from_ref(&tracker),
            &[pk(1)],
            EvictionTier::Tracker,
            1,
            3,
        );
    }

    #[test]
    fn victim_selection_same_tier_true_lru_and_owner_tie_break() {
        let t1 = pool_owner(ExplicitConsumer::Tracker, 1);
        let t2 = pool_owner(ExplicitConsumer::Tracker, 2);
        let fixtures = [
            (t1.clone(), 100, vec![pk(1)]),
            (t2.clone(), 200, vec![pk(2)]),
        ];
        let snapshot = snapshot_from_fixtures(&fixtures);
        let incoming = pool_owner(ExplicitConsumer::Momentum, 9);
        let result = select_eviction_victims(&snapshot, victim_request(incoming, vec![pk(10)], 2));
        assert_planned(
            &result,
            std::slice::from_ref(&t1),
            &[pk(1)],
            EvictionTier::Tracker,
            1,
            2,
        );
    }

    #[test]
    fn victim_selection_wallet_never_victim() {
        let wallet = pool_owner(ExplicitConsumer::Wallet, 1);
        let tracker = pool_owner(ExplicitConsumer::Tracker, 2);
        let shared = pk(50);
        let fixtures = [
            (wallet.clone(), 1, vec![shared, pk(1)]),
            (tracker.clone(), 2, vec![pk(2)]),
        ];
        let snapshot = snapshot_from_fixtures(&fixtures);
        let incoming = pool_owner(ExplicitConsumer::Momentum, 9);
        let result = select_eviction_victims(&snapshot, victim_request(incoming, vec![pk(10)], 3));
        match &result {
            VictimSelectionResult::Planned(plan) => {
                assert!(!plan.victims.contains(&wallet));
                assert_eq!(plan.victims, vec![tracker.clone()]);
            }
            other => panic!("expected Planned, got {other:?}"),
        }
    }

    #[test]
    fn victim_selection_zero_marginal_preserve_shared_incoming() {
        let shared = pk(50);
        let a = pool_owner(ExplicitConsumer::Tracker, 1);
        let b = pool_owner(ExplicitConsumer::Tracker, 2);
        let fixtures = [(a.clone(), 1, vec![shared]), (b.clone(), 2, vec![pk(2)])];
        let snapshot = snapshot_from_fixtures(&fixtures);
        let incoming = pool_owner(ExplicitConsumer::Momentum, 9);
        let result =
            select_eviction_victims(&snapshot, victim_request(incoming, vec![shared, pk(10)], 2));
        assert_planned(
            &result,
            std::slice::from_ref(&b),
            &[pk(2)],
            EvictionTier::Tracker,
            1,
            2,
        );
        assert!(!matches!(&result, VictimSelectionResult::Planned(p) if p.victims.contains(&a)));
    }

    #[test]
    fn victim_selection_joint_shared_requires_both_owners() {
        let shared = pk(50);
        let a = pool_owner(ExplicitConsumer::Tracker, 1);
        let b = pool_owner(ExplicitConsumer::Tracker, 2);
        let fixtures = [(a.clone(), 1, vec![shared]), (b.clone(), 2, vec![shared])];
        let snapshot = snapshot_from_fixtures(&fixtures);
        let incoming = pool_owner(ExplicitConsumer::Momentum, 9);
        let result = select_eviction_victims(&snapshot, victim_request(incoming, vec![pk(10)], 1));
        assert_planned(
            &result,
            &[a.clone(), b.clone()],
            &[shared],
            EvictionTier::Tracker,
            1,
            1,
        );
    }

    #[test]
    fn victim_selection_lower_tier_joint_when_higher_tier_positive_exists() {
        let shared_low = pk(50);
        let shared_high = pk(51);
        let t_joint = pool_owner(ExplicitConsumer::Tracker, 1);
        let t_pos = pool_owner(ExplicitConsumer::Tracker, 2);
        let m_pos = pool_owner(ExplicitConsumer::Momentum, 3);
        let fixtures = [
            (t_joint.clone(), 1, vec![shared_low]),
            (
                pool_owner(ExplicitConsumer::Tracker, 4),
                4,
                vec![shared_low],
            ),
            (t_pos.clone(), 2, vec![pk(1)]),
            (m_pos.clone(), 3, vec![shared_high, pk(2)]),
            (
                pool_owner(ExplicitConsumer::Momentum, 5),
                5,
                vec![shared_high],
            ),
        ];
        let snapshot = snapshot_from_fixtures(&fixtures);
        let incoming = pool_owner(ExplicitConsumer::Momentum, 9);
        let result =
            select_eviction_victims(&snapshot, victim_request(incoming, vec![pk(10), pk(11)], 3));
        match &result {
            VictimSelectionResult::Planned(plan) => {
                assert_eq!(plan.victims[0], t_pos);
                assert!(!plan.victims.contains(&t_joint));
            }
            other => panic!("expected Planned, got {other:?}"),
        }
    }

    #[test]
    fn victim_selection_incoming_overlap_never_freed() {
        let tracker = pool_owner(ExplicitConsumer::Tracker, 1);
        let fixtures = [(tracker.clone(), 1, vec![pk(1), pk(2)])];
        let snapshot = snapshot_from_fixtures(&fixtures);
        let incoming = pool_owner(ExplicitConsumer::Momentum, 9);
        let overlap = pk(1);
        let result = select_eviction_victims(
            &snapshot,
            victim_request(incoming, vec![overlap, pk(10)], 2),
        );
        match &result {
            VictimSelectionResult::Planned(plan) => {
                assert!(!plan.physical_freed.contains(&overlap));
                assert_eq!(plan.physical_freed, vec![pk(2)]);
            }
            other => panic!("expected Planned, got {other:?}"),
        }
    }

    #[test]
    fn victim_selection_mixed_tracker_arb_shared_minimal_opened_tier() {
        let shared = pk(50);
        let tracker = pool_owner(ExplicitConsumer::Tracker, 1);
        let arb = pool_owner(ExplicitConsumer::Arb, 2);
        let fixtures = [
            (tracker.clone(), 1, vec![shared, pk(1)]),
            (arb.clone(), 2, vec![shared]),
        ];
        let snapshot = snapshot_from_fixtures(&fixtures);
        let incoming = pool_owner(ExplicitConsumer::Momentum, 9);
        let result =
            select_eviction_victims(&snapshot, victim_request(incoming, vec![pk(10), pk(11)], 2));
        assert_planned(
            &result,
            &[tracker.clone(), arb.clone()],
            &[pk(1), shared],
            EvictionTier::Arb,
            2,
            2,
        );
    }

    #[test]
    fn victim_selection_multiple_joint_packages_deterministic_tie_break() {
        let shared_a = pk(60);
        let shared_b = pk(61);
        let t1 = pool_owner(ExplicitConsumer::Tracker, 1);
        let t2 = pool_owner(ExplicitConsumer::Tracker, 2);
        let t3 = pool_owner(ExplicitConsumer::Tracker, 3);
        let t4 = pool_owner(ExplicitConsumer::Tracker, 4);
        let fixtures = [
            (t1.clone(), 10, vec![shared_a]),
            (t2.clone(), 20, vec![shared_a]),
            (t3.clone(), 30, vec![shared_b]),
            (t4.clone(), 40, vec![shared_b]),
        ];
        let snapshot = snapshot_from_fixtures(&fixtures);
        let incoming = pool_owner(ExplicitConsumer::Momentum, 9);
        let result =
            select_eviction_victims(&snapshot, victim_request(incoming, vec![pk(10), pk(11)], 3));
        assert_planned(
            &result,
            &[t1.clone(), t2.clone()],
            &[shared_a],
            EvictionTier::Tracker,
            2,
            3,
        );
    }

    #[test]
    fn victim_selection_snapshot_invariant_under_fixture_permutations() {
        let shared = pk(50);
        let t1 = pool_owner(ExplicitConsumer::Tracker, 1);
        let t2 = pool_owner(ExplicitConsumer::Tracker, 2);
        let arb = pool_owner(ExplicitConsumer::Arb, 3);
        let fixtures = [
            (t1.clone(), 10, vec![pk(1), shared]),
            (t2.clone(), 20, vec![pk(2)]),
            (arb.clone(), 30, vec![shared, pk(3)]),
        ];
        let incoming = pool_owner(ExplicitConsumer::Momentum, 9);
        let request = victim_request(incoming, vec![pk(10), pk(11)], 3);

        let baseline_snapshot = snapshot_from_fixtures(&fixtures);
        let baseline = select_eviction_victims(&baseline_snapshot, request.clone());

        let row_perms = all_permutations(&(0..fixtures.len()).collect::<Vec<_>>());
        for row_perm in &row_perms {
            let perm_fixtures: Vec<FixtureRow> = row_perm
                .iter()
                .map(|&idx| {
                    let (owner, touch, pubkeys) = &fixtures[idx];
                    (owner.clone(), *touch, pubkeys.clone())
                })
                .collect();

            let pubkey_orders: Vec<Vec<usize>> = perm_fixtures
                .iter()
                .map(|(_, _, pubkeys)| {
                    let mut order: Vec<usize> = (0..pubkeys.len()).collect();
                    order.reverse();
                    order
                })
                .collect();
            let permuted_pubkeys: Vec<FixtureRow> = perm_fixtures
                .iter()
                .enumerate()
                .map(|(row_idx, (owner, touch, pubkeys))| {
                    let ordered: Vec<Pubkey> = pubkey_orders[row_idx]
                        .iter()
                        .map(|&key_idx| pubkeys[key_idx])
                        .collect();
                    (owner.clone(), *touch, ordered)
                })
                .collect();

            for lru_perm in all_permutations(&(0..permuted_pubkeys.len()).collect::<Vec<_>>()) {
                let snapshot = snapshot_from_permuted(&permuted_pubkeys, &lru_perm).unwrap();
                let actual = select_eviction_victims(&snapshot, request.clone());
                assert_eq!(
                    actual, baseline,
                    "row_perm={row_perm:?} lru_perm={lru_perm:?}"
                );
            }
        }
    }

    fn all_permutations(items: &[usize]) -> Vec<Vec<usize>> {
        let n = items.len();
        if n == 0 {
            return vec![vec![]];
        }
        if n > 8 {
            return vec![items.to_vec(), items.iter().rev().copied().collect()];
        }
        let mut out = Vec::new();
        let mut used = vec![false; n];
        let mut current = Vec::with_capacity(n);
        permute_dfs(items, &mut used, &mut current, &mut out);
        out
    }

    fn permute_dfs(
        items: &[usize],
        used: &mut [bool],
        current: &mut Vec<usize>,
        out: &mut Vec<Vec<usize>>,
    ) {
        if current.len() == items.len() {
            out.push(current.clone());
            return;
        }
        for (idx, &item) in items.iter().enumerate() {
            if used[idx] {
                continue;
            }
            used[idx] = true;
            current.push(item);
            permute_dfs(items, used, current, out);
            current.pop();
            used[idx] = false;
        }
    }

    fn snapshot_from_permuted(
        fixtures: &[FixtureRow],
        lru_order: &[usize],
    ) -> Result<EvictionPlanningSnapshot, SnapshotBuildError> {
        let mut ownership = ExplicitOwnership::new();
        for (owner, _, pubkeys) in fixtures {
            upsert(&mut ownership, owner.clone(), pubkeys.iter().copied());
        }
        let lru_entries: Vec<OwnerLruEntry> = lru_order
            .iter()
            .map(|&idx| {
                let (owner, touch, _) = &fixtures[idx];
                lru(owner.clone(), *touch)
            })
            .collect();
        EvictionPlanningSnapshot::from_ownership(&ownership, lru_entries)
    }

    #[test]
    fn victim_selection_exact_deltas_after_multiple_rounds() {
        let shared = pk(50);
        let t1 = pool_owner(ExplicitConsumer::Tracker, 1);
        let t2 = pool_owner(ExplicitConsumer::Tracker, 2);
        let t3 = pool_owner(ExplicitConsumer::Tracker, 3);
        let fixtures = [
            (t1.clone(), 1, vec![pk(1)]),
            (t2.clone(), 2, vec![pk(2)]),
            (t3.clone(), 3, vec![shared]),
            (pool_owner(ExplicitConsumer::Tracker, 4), 4, vec![shared]),
        ];
        let snapshot = snapshot_from_fixtures(&fixtures);
        let incoming = pool_owner(ExplicitConsumer::Momentum, 9);
        let result =
            select_eviction_victims(&snapshot, victim_request(incoming, vec![pk(10), pk(11)], 3));
        match &result {
            VictimSelectionResult::Planned(plan) => {
                assert_eq!(plan.physical_freed.len(), 2);
                assert_eq!(plan.projected_final_len, 3);
                assert_eq!(
                    snapshot.physical_len() + plan.incoming_physical_added
                        - plan.physical_freed.len(),
                    plan.projected_final_len
                );
            }
            other => panic!("expected Planned, got {other:?}"),
        }
    }

    #[test]
    fn victim_selection_protected_and_invalid_unchanged() {
        let wallet = pool_owner(ExplicitConsumer::Wallet, 1);
        let momentum = pool_owner(ExplicitConsumer::Momentum, 2);
        let fixtures = [
            (wallet.clone(), 1, vec![pk(1)]),
            (momentum.clone(), 2, vec![pk(2), pk(3)]),
        ];
        let snapshot = snapshot_from_fixtures(&fixtures);
        let incoming = pool_owner(ExplicitConsumer::Momentum, 9);
        let protected = select_eviction_victims(
            &snapshot,
            victim_request(incoming.clone(), vec![pk(10), pk(11)], 1),
        );
        assert!(matches!(
            protected,
            VictimSelectionResult::RejectedProtected { .. }
        ));

        assert_eq!(
            select_eviction_victims(&snapshot, victim_request(incoming, vec![], 5)),
            VictimSelectionResult::RejectedInvalidInput
        );
    }

    #[test]
    fn victim_selection_adversarial_hypergraph_overlapping_packages() {
        let s1 = pk(1);
        let s2 = pk(2);
        let s3 = pk(3);
        let a = pool_owner(ExplicitConsumer::Tracker, 1);
        let b = pool_owner(ExplicitConsumer::Tracker, 2);
        let c = pool_owner(ExplicitConsumer::Tracker, 3);
        let fixtures = [
            (a.clone(), 1, vec![s1, s2]),
            (b.clone(), 2, vec![s2, s3]),
            (c.clone(), 3, vec![s1, s3]),
        ];
        let snapshot = snapshot_from_fixtures(&fixtures);
        let incoming = pool_owner(ExplicitConsumer::Momentum, 9);
        let result =
            select_eviction_victims(&snapshot, victim_request(incoming, vec![pk(10), pk(11)], 3));
        assert_planned(
            &result,
            &[a.clone(), b.clone(), c.clone()],
            &[s1, s2, s3],
            EvictionTier::Tracker,
            2,
            2,
        );
    }

    #[test]
    fn victim_selection_stats_track_real_work() {
        let shared = pk(50);
        let t1 = pool_owner(ExplicitConsumer::Tracker, 1);
        let t2 = pool_owner(ExplicitConsumer::Tracker, 2);
        let fixtures = [(t1.clone(), 1, vec![shared]), (t2.clone(), 2, vec![shared])];
        let snapshot = snapshot_from_fixtures(&fixtures);
        let incoming = pool_owner(ExplicitConsumer::Momentum, 9);
        let mut stats = SelectorStats::default();
        let _ = select_eviction_victims_with_stats(
            &snapshot,
            victim_request(incoming, vec![pk(10)], 1),
            &mut stats,
        );
        assert_eq!(stats.initial_edges, 2, "one shared edge per owner");
        assert!(stats.candidate_evaluations > 0);
        assert_eq!(stats.incremental_refcount_updates, 2);
        assert!(stats.package_evaluations > 0);
    }

    /// Hard-coded per-owner rank within a joint package (oracle only).
    type PolicyOwnerRankKey = (u8, u64, ExplicitOwner);

    /// Canonical joint-package tie-break key (oracle only).
    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
    struct PolicyPackageRankKey {
        max_tier: u8,
        member_keys: Vec<PolicyOwnerRankKey>,
        package_len: usize,
        owners: Vec<ExplicitOwner>,
    }

    /// Independent victim oracle — hard-coded policy + powerset (<=8 owners) + procedural replay.
    /// Never calls production selection/tier/package/order helpers.
    struct IndependentVictimOracle {
        physical_len: usize,
        owner_records: BTreeMap<ExplicitOwner, (u64, Vec<Pubkey>)>,
        pubkey_holders: BTreeMap<Pubkey, BTreeSet<ExplicitOwner>>,
    }

    /// Policy tier label duplicated for oracle isolation.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    enum PolicyTier {
        Tracker,
        Arb,
        Momentum,
    }

    impl PolicyTier {
        fn to_eviction_tier(self) -> EvictionTier {
            match self {
                Self::Tracker => EvictionTier::Tracker,
                Self::Arb => EvictionTier::Arb,
                Self::Momentum => EvictionTier::Momentum,
            }
        }

        fn allowed_for_incoming(consumer: ExplicitConsumer) -> &'static [PolicyTier] {
            match consumer {
                ExplicitConsumer::Tracker => &[Self::Tracker],
                ExplicitConsumer::Arb => &[Self::Tracker, Self::Arb],
                ExplicitConsumer::Momentum | ExplicitConsumer::Wallet => {
                    &[Self::Tracker, Self::Arb, Self::Momentum]
                }
            }
        }

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
    }

    struct VictimOracleCase {
        fixtures: Vec<FixtureRow>,
        incoming: ExplicitOwner,
        keys: Vec<Pubkey>,
        cap: usize,
    }

    impl IndependentVictimOracle {
        fn eviction_rank(consumer: ExplicitConsumer) -> u8 {
            match consumer {
                ExplicitConsumer::Tracker => 0,
                ExplicitConsumer::Arb => 1,
                ExplicitConsumer::Momentum => 2,
                ExplicitConsumer::Wallet => u8::MAX,
            }
        }

        fn from_fixtures(fixtures: &[FixtureRow]) -> Self {
            let mut owner_records = BTreeMap::new();
            let mut pubkey_holders: BTreeMap<Pubkey, BTreeSet<ExplicitOwner>> = BTreeMap::new();
            for (owner, touch, pubkeys) in fixtures {
                let mut normalized = pubkeys.clone();
                normalized.sort();
                normalized.dedup();
                owner_records.insert(owner.clone(), (*touch, normalized.clone()));
                for pubkey in &normalized {
                    pubkey_holders
                        .entry(*pubkey)
                        .or_default()
                        .insert(owner.clone());
                }
            }
            Self {
                physical_len: pubkey_holders.len(),
                owner_records,
                pubkey_holders,
            }
        }

        fn incoming_physical_added(&self, incoming: &[Pubkey]) -> usize {
            incoming
                .iter()
                .filter(|pk| !self.pubkey_holders.contains_key(pk))
                .count()
        }

        fn eligible_owners_at_tier(&self, tier: PolicyTier) -> BTreeSet<ExplicitOwner> {
            let cumulative = PolicyTier::cumulative_consumers(tier);
            self.owner_records
                .keys()
                .filter(|owner| {
                    owner.consumer != ExplicitConsumer::Wallet
                        && cumulative.contains(&owner.consumer)
                })
                .cloned()
                .collect()
        }

        fn freed_by_victim_subset(
            &self,
            victims: &BTreeSet<ExplicitOwner>,
            incoming: &BTreeSet<Pubkey>,
        ) -> BTreeSet<Pubkey> {
            self.pubkey_holders
                .iter()
                .filter(|(pubkey, holders)| {
                    !incoming.contains(pubkey)
                        && holders.iter().all(|owner| victims.contains(owner))
                })
                .map(|(pubkey, _)| *pubkey)
                .collect()
        }

        fn powerset_max_freeable(
            &self,
            allowed: &BTreeSet<ExplicitOwner>,
            incoming: &BTreeSet<Pubkey>,
        ) -> usize {
            let owners: Vec<ExplicitOwner> = allowed.iter().cloned().collect();
            assert!(owners.len() <= 8, "powerset oracle limited to <=8 owners");
            let n = owners.len();
            let mut best = 0usize;
            for mask in 0..(1usize << n) {
                let subset: BTreeSet<ExplicitOwner> = owners
                    .iter()
                    .enumerate()
                    .filter(|(idx, _)| mask & (1 << idx) != 0)
                    .map(|(_, owner)| owner.clone())
                    .collect();
                best = best.max(self.freed_by_victim_subset(&subset, incoming).len());
            }
            best
        }

        fn powerset_any_subset_frees_enough(
            &self,
            allowed: &BTreeSet<ExplicitOwner>,
            incoming: &BTreeSet<Pubkey>,
            required: usize,
        ) -> bool {
            self.powerset_max_freeable(allowed, incoming) >= required
        }

        fn pubkey_freeable_count_at_tier(
            &self,
            tier: PolicyTier,
            incoming: &BTreeSet<Pubkey>,
        ) -> usize {
            let eligible = self.eligible_owners_at_tier(tier);
            self.pubkey_holders
                .iter()
                .filter(|(pubkey, holders)| {
                    !incoming.contains(pubkey)
                        && holders.iter().all(|owner| eligible.contains(owner))
                })
                .count()
        }

        fn sorted_candidates(&self, allowed: &BTreeSet<ExplicitOwner>) -> Vec<ExplicitOwner> {
            let mut candidates: Vec<ExplicitOwner> = allowed.iter().cloned().collect();
            candidates.sort_by(|a, b| {
                let touch_a = self.owner_records.get(a).map(|(t, _)| *t).unwrap_or(0);
                let touch_b = self.owner_records.get(b).map(|(t, _)| *t).unwrap_or(0);
                Self::eviction_rank(a.consumer)
                    .cmp(&Self::eviction_rank(b.consumer))
                    .then(touch_a.cmp(&touch_b))
                    .then(a.cmp(b))
            });
            candidates
        }

        fn owner_pubkeys_map(&self) -> BTreeMap<ExplicitOwner, Vec<Pubkey>> {
            let mut map: BTreeMap<ExplicitOwner, Vec<Pubkey>> = BTreeMap::new();
            for (pubkey, holders) in &self.pubkey_holders {
                for owner in holders {
                    map.entry(owner.clone()).or_default().push(*pubkey);
                }
            }
            for pubkeys in map.values_mut() {
                pubkeys.sort();
                pubkeys.dedup();
            }
            map
        }

        fn package_rank_tuple(&self, package: &BTreeSet<ExplicitOwner>) -> PolicyPackageRankKey {
            let max_tier = package
                .iter()
                .map(|owner| Self::eviction_rank(owner.consumer))
                .max()
                .unwrap_or(0);
            let mut member_keys: Vec<PolicyOwnerRankKey> = package
                .iter()
                .map(|owner| {
                    let touch = self.owner_records.get(owner).map(|(t, _)| *t).unwrap_or(0);
                    (Self::eviction_rank(owner.consumer), touch, owner.clone())
                })
                .collect();
            member_keys.sort();
            let mut owners: Vec<_> = package.iter().cloned().collect();
            owners.sort();
            PolicyPackageRankKey {
                max_tier,
                member_keys,
                package_len: package.len(),
                owners,
            }
        }

        fn compare_packages(
            &self,
            a: &BTreeSet<ExplicitOwner>,
            b: &BTreeSet<ExplicitOwner>,
        ) -> std::cmp::Ordering {
            self.package_rank_tuple(a).cmp(&self.package_rank_tuple(b))
        }

        fn procedural_selection(
            &self,
            allowed: &BTreeSet<ExplicitOwner>,
            incoming: &BTreeSet<Pubkey>,
            required: usize,
        ) -> Option<(Vec<ExplicitOwner>, BTreeSet<Pubkey>)> {
            let candidates = self.sorted_candidates(allowed);
            let owner_pubkeys = self.owner_pubkeys_map();
            let mut projected: BTreeMap<Pubkey, usize> = self
                .pubkey_holders
                .iter()
                .map(|(pubkey, holders)| (*pubkey, holders.len()))
                .collect();
            let mut selected = BTreeSet::new();
            let mut victims = Vec::new();
            let mut freed = BTreeSet::new();

            while freed.len() < required {
                let mut picked_positive = false;
                for owner in &candidates {
                    if selected.contains(owner) {
                        continue;
                    }
                    let positive = owner_pubkeys
                        .get(owner)
                        .into_iter()
                        .flatten()
                        .any(|pubkey| {
                            !incoming.contains(pubkey)
                                && projected.get(pubkey).copied().unwrap_or(0) == 1
                        });
                    if positive {
                        selected.insert(owner.clone());
                        victims.push(owner.clone());
                        for pubkey in owner_pubkeys.get(owner).into_iter().flatten() {
                            let prev = projected.get_mut(pubkey).unwrap();
                            if *prev == 0 {
                                continue;
                            }
                            *prev -= 1;
                            if *prev == 0 && !incoming.contains(pubkey) {
                                freed.insert(*pubkey);
                            }
                        }
                        picked_positive = true;
                        break;
                    }
                }
                if picked_positive {
                    continue;
                }

                let mut packages = BTreeSet::new();
                for (pubkey, holders) in &self.pubkey_holders {
                    if incoming.contains(pubkey) {
                        continue;
                    }
                    let count = projected.get(pubkey).copied().unwrap_or(0);
                    if count == 0 {
                        continue;
                    }
                    let remaining: BTreeSet<ExplicitOwner> = holders
                        .iter()
                        .filter(|owner| allowed.contains(*owner) && !selected.contains(*owner))
                        .cloned()
                        .collect();
                    if remaining.len() == count && !remaining.is_empty() {
                        packages.insert(remaining);
                    }
                }

                let best = packages
                    .into_iter()
                    .min_by(|a, b| self.compare_packages(a, b))?;

                for owner in &candidates {
                    if best.contains(owner) && !selected.contains(owner) {
                        selected.insert(owner.clone());
                        victims.push(owner.clone());
                        for pubkey in owner_pubkeys.get(owner).into_iter().flatten() {
                            let prev = projected.get_mut(pubkey).unwrap();
                            if *prev == 0 {
                                continue;
                            }
                            *prev -= 1;
                            if *prev == 0 && !incoming.contains(pubkey) {
                                freed.insert(*pubkey);
                            }
                        }
                    }
                }
            }

            Some((victims, freed))
        }

        fn analyze(
            &self,
            incoming_owner: &ExplicitOwner,
            incoming_pubkeys: &[Pubkey],
            cap: usize,
        ) -> VictimSelectionResult {
            let mut normalized: Vec<Pubkey> = incoming_pubkeys.to_vec();
            normalized.sort();
            normalized.dedup();
            if normalized.is_empty() {
                return VictimSelectionResult::RejectedInvalidInput;
            }
            if self.owner_records.contains_key(incoming_owner) {
                return VictimSelectionResult::RejectedInvalidInput;
            }

            let incoming_set: BTreeSet<Pubkey> = normalized.iter().copied().collect();
            let incoming_added = self.incoming_physical_added(&normalized);
            let projected = match self.physical_len.checked_add(incoming_added) {
                Some(value) => value,
                None => return VictimSelectionResult::InternalInvariantViolation,
            };
            if projected <= cap {
                return VictimSelectionResult::NoEvictionNeeded {
                    incoming_physical_added: incoming_added,
                    projected_final_len: projected,
                };
            }
            let required = match projected.checked_sub(cap) {
                Some(value) => value,
                None => return VictimSelectionResult::InternalInvariantViolation,
            };

            let allowed_tiers = PolicyTier::allowed_for_incoming(incoming_owner.consumer);
            let mut opened = None;
            for tier in allowed_tiers {
                if self.pubkey_freeable_count_at_tier(*tier, &incoming_set) >= required {
                    opened = Some((*tier, self.eligible_owners_at_tier(*tier)));
                    break;
                }
            }

            let Some((tier, allowed)) = opened else {
                let max_tier = *allowed_tiers.last().unwrap_or(&PolicyTier::Tracker);
                let max_allowed = self.eligible_owners_at_tier(max_tier);
                assert!(
                    !self.powerset_any_subset_frees_enough(&max_allowed, &incoming_set, required),
                    "RejectedProtected but powerset found feasible eligible subset"
                );
                return VictimSelectionResult::RejectedProtected {
                    incoming_physical_added: incoming_added,
                    required_to_free: required,
                };
            };

            assert!(
                self.powerset_any_subset_frees_enough(&allowed, &incoming_set, required),
                "opened tier but powerset found no feasible eligible subset"
            );

            let Some((victims, freed)) =
                self.procedural_selection(&allowed, &incoming_set, required)
            else {
                return VictimSelectionResult::InternalInvariantViolation;
            };

            let mut physical_freed: Vec<Pubkey> = freed.into_iter().collect();
            physical_freed.sort();
            let projected_final_len = self
                .physical_len
                .checked_add(incoming_added)
                .and_then(|value| value.checked_sub(physical_freed.len()))
                .unwrap_or(0);

            VictimSelectionResult::Planned(VictimSelectionPlan {
                victims,
                physical_freed,
                incoming_physical_added: incoming_added,
                projected_final_len,
                opened_through: tier.to_eviction_tier(),
            })
        }
    }

    fn victim_oracle_test_cases() -> Vec<VictimOracleCase> {
        let shared = pk(50);
        let shared_ab = pk(51);
        let wallet = pool_owner(ExplicitConsumer::Wallet, 1);
        let tracker_a = pool_owner(ExplicitConsumer::Tracker, 1);
        let tracker_b = pool_owner(ExplicitConsumer::Tracker, 2);
        let tracker_c = pool_owner(ExplicitConsumer::Tracker, 3);
        let arb_a = pool_owner(ExplicitConsumer::Arb, 4);
        let momentum_a = pool_owner(ExplicitConsumer::Momentum, 5);
        let incoming_tracker = pool_owner(ExplicitConsumer::Tracker, 90);
        let incoming_arb = pool_owner(ExplicitConsumer::Arb, 91);
        let incoming_momentum = pool_owner(ExplicitConsumer::Momentum, 92);

        vec![
            VictimOracleCase {
                fixtures: vec![
                    (tracker_a.clone(), 1, vec![pk(1)]),
                    (arb_a.clone(), 2, vec![pk(2)]),
                ],
                incoming: incoming_momentum.clone(),
                keys: vec![pk(100)],
                cap: 5,
            },
            VictimOracleCase {
                fixtures: vec![
                    (tracker_a.clone(), 1, vec![pk(1)]),
                    (arb_a.clone(), 2, vec![pk(2)]),
                    (momentum_a.clone(), 3, vec![pk(3)]),
                ],
                incoming: incoming_momentum.clone(),
                keys: vec![pk(100)],
                cap: 2,
            },
            VictimOracleCase {
                fixtures: vec![
                    (tracker_a.clone(), 1, vec![shared]),
                    (tracker_b.clone(), 2, vec![shared]),
                ],
                incoming: incoming_momentum.clone(),
                keys: vec![pk(100)],
                cap: 1,
            },
            VictimOracleCase {
                fixtures: vec![
                    (tracker_a.clone(), 1, vec![shared, pk(2)]),
                    (arb_a.clone(), 3, vec![shared]),
                ],
                incoming: incoming_momentum.clone(),
                keys: vec![pk(100), pk(101)],
                cap: 2,
            },
            VictimOracleCase {
                fixtures: vec![
                    (wallet.clone(), 1, vec![pk(10)]),
                    (tracker_a.clone(), 2, vec![pk(11)]),
                ],
                incoming: incoming_momentum.clone(),
                keys: vec![pk(100)],
                cap: 2,
            },
            VictimOracleCase {
                fixtures: vec![(tracker_a.clone(), 1, vec![pk(1), pk(2)])],
                incoming: incoming_momentum.clone(),
                keys: vec![pk(1), pk(100)],
                cap: 2,
            },
            VictimOracleCase {
                fixtures: vec![
                    (tracker_a.clone(), 1, vec![pk(1), shared_ab]),
                    (arb_a.clone(), 2, vec![shared_ab]),
                ],
                incoming: incoming_momentum.clone(),
                keys: vec![pk(100), pk(101)],
                cap: 2,
            },
            VictimOracleCase {
                fixtures: vec![
                    (tracker_a.clone(), 1, vec![pk(1), pk(2)]),
                    (tracker_b.clone(), 2, vec![pk(2), pk(3)]),
                    (tracker_c.clone(), 3, vec![pk(1), pk(3)]),
                ],
                incoming: incoming_momentum.clone(),
                keys: vec![pk(100), pk(101)],
                cap: 3,
            },
            VictimOracleCase {
                fixtures: vec![
                    (tracker_a.clone(), 1, vec![shared]),
                    (tracker_b.clone(), 2, vec![pk(2)]),
                ],
                incoming: incoming_momentum.clone(),
                keys: vec![shared, pk(100)],
                cap: 2,
            },
            VictimOracleCase {
                fixtures: vec![
                    (wallet.clone(), 1, vec![pk(1)]),
                    (momentum_a.clone(), 2, vec![pk(2), pk(3)]),
                ],
                incoming: incoming_momentum.clone(),
                keys: vec![pk(10), pk(11)],
                cap: 1,
            },
            VictimOracleCase {
                fixtures: vec![
                    (tracker_a.clone(), 1, vec![pk(1)]),
                    (arb_a.clone(), 2, vec![pk(2)]),
                    (momentum_a.clone(), 3, vec![pk(3)]),
                ],
                incoming: incoming_tracker.clone(),
                keys: vec![pk(100), pk(101)],
                cap: 2,
            },
            VictimOracleCase {
                fixtures: vec![
                    (tracker_a.clone(), 1, vec![pk(1)]),
                    (arb_a.clone(), 2, vec![pk(2)]),
                    (momentum_a.clone(), 3, vec![pk(3)]),
                ],
                incoming: incoming_arb.clone(),
                keys: vec![pk(100), pk(101), pk(102)],
                cap: 2,
            },
            VictimOracleCase {
                fixtures: vec![
                    (tracker_a.clone(), 10, vec![pk(60)]),
                    (tracker_b.clone(), 20, vec![pk(60)]),
                    (tracker_c.clone(), 30, vec![pk(61)]),
                    (pool_owner(ExplicitConsumer::Tracker, 6), 40, vec![pk(61)]),
                ],
                incoming: incoming_momentum.clone(),
                keys: vec![pk(100), pk(101)],
                cap: 3,
            },
        ]
    }

    #[test]
    fn victim_selection_powerset_oracle_matches_production_on_deterministic_graphs() {
        for case in victim_oracle_test_cases() {
            assert!(
                case.fixtures.len() <= 8,
                "powerset oracle requires <=8 owners"
            );
            let snapshot = snapshot_from_fixtures(&case.fixtures);
            let oracle = IndependentVictimOracle::from_fixtures(&case.fixtures);
            let request = victim_request(case.incoming.clone(), case.keys.clone(), case.cap);
            let actual = select_eviction_victims(&snapshot, request);
            let expected = oracle.analyze(&case.incoming, &case.keys, case.cap);
            assert_eq!(
                actual, expected,
                "fixtures={:?} incoming={:?} keys={:?} cap={}",
                case.fixtures, case.incoming, case.keys, case.cap
            );
        }
    }

    fn assert_cap_shrink_planned(
        result: &CapShrinkSelectionResult,
        victims: &[ExplicitOwner],
        physical_freed: &[Pubkey],
        opened_through: EvictionTier,
        projected_final_len: usize,
    ) {
        match result {
            CapShrinkSelectionResult::Planned(plan) => {
                assert_eq!(plan.victims.as_slice(), victims);
                assert_eq!(plan.physical_freed.as_slice(), physical_freed);
                assert_eq!(plan.opened_through, opened_through);
                assert_eq!(plan.projected_final_len, projected_final_len);
            }
            other => panic!("expected planned cap shrink, got {other:?}"),
        }
    }

    #[test]
    fn cap_shrink_selector_no_shrink_needed_when_within_cap() {
        let tracker = pool_owner(ExplicitConsumer::Tracker, 1);
        let fixtures = [(tracker, 1, vec![pk(1)])];
        let snapshot = snapshot_from_fixtures(&fixtures);
        assert_eq!(
            select_cap_shrink_victims(&snapshot, 5),
            CapShrinkSelectionResult::NoShrinkNeeded
        );
    }

    #[test]
    fn cap_shrink_selector_evicts_tracker_before_arb() {
        let tracker = pool_owner(ExplicitConsumer::Tracker, 1);
        let arb = pool_owner(ExplicitConsumer::Arb, 2);
        let fixtures = [
            (tracker.clone(), 10, vec![pk(1)]),
            (arb.clone(), 20, vec![pk(2)]),
        ];
        let snapshot = snapshot_from_fixtures(&fixtures);
        assert_cap_shrink_planned(
            &select_cap_shrink_victims(&snapshot, 1),
            &[tracker],
            &[pk(1)],
            EvictionTier::Tracker,
            1,
        );
    }

    #[test]
    fn cap_shrink_selector_true_lru_within_tier() {
        let t_old = pool_owner(ExplicitConsumer::Tracker, 1);
        let t_new = pool_owner(ExplicitConsumer::Tracker, 2);
        let fixtures = [
            (t_old.clone(), 10, vec![pk(1)]),
            (t_new.clone(), 20, vec![pk(2)]),
        ];
        let snapshot = snapshot_from_fixtures(&fixtures);
        assert_cap_shrink_planned(
            &select_cap_shrink_victims(&snapshot, 1),
            &[t_old],
            &[pk(1)],
            EvictionTier::Tracker,
            1,
        );
    }

    #[test]
    fn cap_shrink_selector_joint_shared_victims() {
        let shared_a = pk(60);
        let shared_b = pk(61);
        let t1 = pool_owner(ExplicitConsumer::Tracker, 1);
        let t2 = pool_owner(ExplicitConsumer::Tracker, 2);
        let t3 = pool_owner(ExplicitConsumer::Tracker, 3);
        let t4 = pool_owner(ExplicitConsumer::Tracker, 4);
        let fixtures = [
            (t1.clone(), 10, vec![shared_a]),
            (t2.clone(), 20, vec![shared_a]),
            (t3.clone(), 30, vec![shared_b]),
            (t4.clone(), 40, vec![shared_b]),
        ];
        let snapshot = snapshot_from_fixtures(&fixtures);
        assert_cap_shrink_planned(
            &select_cap_shrink_victims(&snapshot, 1),
            &[t1.clone(), t2.clone()],
            &[shared_a],
            EvictionTier::Tracker,
            1,
        );
    }

    #[test]
    fn cap_shrink_selector_wallet_never_victim() {
        let wallet = pool_owner(ExplicitConsumer::Wallet, 1);
        let shared = pk(50);
        let tracker = pool_owner(ExplicitConsumer::Tracker, 2);
        let fixtures = [
            (wallet.clone(), 1, vec![shared, pk(1)]),
            (tracker.clone(), 10, vec![shared, pk(2)]),
        ];
        let snapshot = snapshot_from_fixtures(&fixtures);
        match select_cap_shrink_victims(&snapshot, 2) {
            CapShrinkSelectionResult::Planned(plan) => {
                assert!(!plan.victims.contains(&wallet));
            }
            other => panic!("expected planned shrink, got {other:?}"),
        }
    }

    #[test]
    fn cap_shrink_selector_protected_when_wallet_exceeds_target() {
        let wallet = pool_owner(ExplicitConsumer::Wallet, 1);
        let fixtures = [(wallet, 1, vec![pk(1), pk(2), pk(3)])];
        let snapshot = snapshot_from_fixtures(&fixtures);
        assert_eq!(
            select_cap_shrink_victims(&snapshot, 2),
            CapShrinkSelectionResult::RejectedProtected {
                required_to_free: 1
            }
        );
    }
}
