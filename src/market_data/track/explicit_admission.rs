//! Fixed-cap admission over [`ExplicitOwnership`] (PR 1b foundation).
//!
//! Pure local state: admits or rejects owner groups against an immutable cap.
//! No eviction, priority, LRU, cap shrink, or runtime wiring.
//!
//! Normal planning is O(old_group_size + new_group_size). On
//! [`FixedCapAdmissionResult::InternalInvariantViolation`] or remove-side equivalent,
//! `physical_len` is reconciled from [`ExplicitOwnership::len`] on a cold fail-closed path
//! (full scan permitted only for this exceptional recovery).

use super::explicit_ownership::{
    EmptyOwnerGroupError, ExplicitOwner, ExplicitOwnership, GroupChange, OwnerGroupSnapshot,
};
use solana_sdk::pubkey::Pubkey;
use std::collections::HashSet;

/// Result of a fixed-cap admission attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FixedCapAdmissionResult {
    /// First insert for this owner; pubkeys that transition refcount 0→1.
    Inserted { physical_added: Vec<Pubkey> },
    /// Atomic replacement; physical deltas from refcount 0→1 / 1→0 transitions.
    OwnerReplaced {
        physical_added: Vec<Pubkey>,
        physical_removed: Vec<Pubkey>,
    },
    /// New owner group whose pubkeys were all already physically tracked.
    OwnerAddedNoNewPubkey,
    /// Idempotent re-admit of an identical normalized group.
    Unchanged,
    /// Projected physical pubkey count would exceed the cap; no mutation.
    RejectedCap {
        required_unique: usize,
        available_unique: usize,
    },
    /// Empty owner groups are invalid and do not mutate state.
    RejectedInvalidGroup,
    /// Plan/commit mismatch or internal checked-arithmetic failure; ownership may have mutated
    /// but `physical_len` was reconciled from ownership on the cold recovery path.
    InternalInvariantViolation,
}

/// Outcome of removing an owner group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FixedCapRemoveOutcome {
    Removed(FixedCapRemoveResult),
    NotFound,
    InternalInvariantViolation,
}

/// Physical deltas from removing an owner group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FixedCapRemoveResult {
    pub snapshot: OwnerGroupSnapshot,
    pub physical_removed: Vec<Pubkey>,
}

/// Test-only counters for instrumented planning lookups (owner group + per-pubkey refcount).
#[cfg(test)]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlanningStats {
    pub refcount_lookups: u64,
    pub owner_group_lookups: u64,
}

#[cfg(test)]
impl PlanningStats {
    fn record_refcount_lookup(&mut self) {
        self.refcount_lookups += 1;
    }

    fn record_owner_group_lookup(&mut self) {
        self.owner_group_lookups += 1;
    }
}

/// Fixed-cap admission layer over explicit ownership.
#[derive(Debug, Clone)]
pub struct FixedCapAdmission {
    cap: usize,
    physical_len: usize,
    ownership: ExplicitOwnership,
    #[cfg(test)]
    planning_stats: PlanningStats,
    /// When true, commit validation expects inflated deltas so production repair runs.
    #[cfg(test)]
    test_force_commit_plan_mismatch: bool,
    /// When set, overrides cached `physical_len` before remove planning (fault injection).
    #[cfg(test)]
    test_physical_len_override: Option<usize>,
}

/// Precomputed admission outcome — checked arithmetic before any ownership mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AdmissionPlan {
    Unchanged,
    RejectCap {
        required_unique: usize,
        available_unique: usize,
    },
    Commit(CommitPlan),
    InternalInvariantViolation,
}

/// Expected physical deltas for post-commit production validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExpectedGroupChange {
    NewGroup {
        physical_added_len: usize,
    },
    Replaced {
        physical_added_len: usize,
        physical_removed_len: usize,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CommitPlan {
    pre_len: usize,
    projected_final_len: usize,
    expected: ExpectedGroupChange,
}

enum RemovePlanResult {
    NotFound,
    Ready(RemovePlan),
    InternalInvariantViolation,
}

/// Precomputed removal outcome — checked arithmetic before [`ExplicitOwnership::remove_group`].
#[derive(Debug, Clone, PartialEq, Eq)]
struct RemovePlan {
    pre_len: usize,
    projected_final_len: usize,
    physical_removed: Vec<Pubkey>,
}

impl FixedCapAdmission {
    pub fn new(cap: usize) -> Self {
        Self {
            cap,
            physical_len: 0,
            ownership: ExplicitOwnership::new(),
            #[cfg(test)]
            planning_stats: PlanningStats::default(),
            #[cfg(test)]
            test_force_commit_plan_mismatch: false,
            #[cfg(test)]
            test_physical_len_override: None,
        }
    }

    pub fn cap(&self) -> usize {
        self.cap
    }

    pub fn len(&self) -> usize {
        self.physical_len
    }

    pub fn is_empty(&self) -> bool {
        self.physical_len == 0
    }

    pub fn snapshot_pubkeys(&self) -> Vec<Pubkey> {
        self.ownership.snapshot_pubkeys()
    }

    pub fn snapshot_owner_groups(&self) -> Vec<OwnerGroupSnapshot> {
        self.ownership.snapshot_owner_groups()
    }

    pub fn owner_group(&self, owner: &ExplicitOwner) -> Option<&[Pubkey]> {
        self.ownership.owner_group(owner)
    }

    pub fn owner_refcount(&self, pubkey: &Pubkey) -> usize {
        self.ownership.owner_refcount(pubkey)
    }

    #[cfg(test)]
    pub fn planning_stats(&self) -> &PlanningStats {
        &self.planning_stats
    }

    #[cfg(test)]
    pub fn set_test_force_commit_plan_mismatch(&mut self, enabled: bool) {
        self.test_force_commit_plan_mismatch = enabled;
    }

    #[cfg(test)]
    pub fn set_test_physical_len_override(&mut self, len: Option<usize>) {
        self.test_physical_len_override = len;
    }

    /// Admit or replace an owner group when the projected physical pubkey count fits the cap.
    pub fn try_admit_group(
        &mut self,
        owner: ExplicitOwner,
        pubkeys: impl IntoIterator<Item = Pubkey>,
    ) -> FixedCapAdmissionResult {
        let normalized = normalize_pubkeys(pubkeys);
        if normalized.is_empty() {
            return FixedCapAdmissionResult::RejectedInvalidGroup;
        }

        let plan = self.plan_admission(&owner, &normalized);

        match plan {
            AdmissionPlan::Unchanged => FixedCapAdmissionResult::Unchanged,
            AdmissionPlan::RejectCap {
                required_unique,
                available_unique,
            } => FixedCapAdmissionResult::RejectedCap {
                required_unique,
                available_unique,
            },
            AdmissionPlan::InternalInvariantViolation => {
                FixedCapAdmissionResult::InternalInvariantViolation
            }
            AdmissionPlan::Commit(commit_plan) => {
                let commit_plan = apply_commit_plan_fault_injection(self, commit_plan);

                let _pre_len = commit_plan.pre_len;
                let change = match self.ownership.upsert_group(owner, normalized) {
                    Ok(change) => change,
                    Err(EmptyOwnerGroupError) => {
                        return FixedCapAdmissionResult::RejectedInvalidGroup;
                    }
                };

                if !group_change_matches_plan(&change, commit_plan.expected) {
                    self.reconcile_physical_len_cold();
                    return FixedCapAdmissionResult::InternalInvariantViolation;
                }

                self.physical_len = commit_plan.projected_final_len;
                map_group_change_to_result(change)
            }
        }
    }

    /// Remove an owner group and return exact physical pubkey deltas.
    pub fn remove_group(&mut self, owner: &ExplicitOwner) -> FixedCapRemoveOutcome {
        let plan_result = self.plan_remove(owner);
        let plan = match plan_result {
            RemovePlanResult::NotFound => return FixedCapRemoveOutcome::NotFound,
            RemovePlanResult::InternalInvariantViolation => {
                return FixedCapRemoveOutcome::InternalInvariantViolation;
            }
            RemovePlanResult::Ready(plan) => plan,
        };

        let snapshot = match self.ownership.remove_group(owner) {
            Some(snapshot) => snapshot,
            None => {
                self.reconcile_physical_len_cold();
                return FixedCapRemoveOutcome::InternalInvariantViolation;
            }
        };

        let mut actual_removed = Vec::new();
        for pubkey in &snapshot.pubkeys {
            let refcount_after = self.ownership.owner_refcount(pubkey);
            if refcount_after == 0 {
                actual_removed.push(*pubkey);
            }
        }
        actual_removed.sort();

        if actual_removed != plan.physical_removed {
            self.reconcile_physical_len_cold();
            return FixedCapRemoveOutcome::InternalInvariantViolation;
        }

        self.physical_len = plan.projected_final_len;
        if self.physical_len != self.ownership.len() {
            self.reconcile_physical_len_cold();
            return FixedCapRemoveOutcome::InternalInvariantViolation;
        }

        FixedCapRemoveOutcome::Removed(FixedCapRemoveResult {
            snapshot,
            physical_removed: plan.physical_removed,
        })
    }

    /// Cold fail-closed recovery: full scan of ownership to repair cached `physical_len`.
    fn reconcile_physical_len_cold(&mut self) {
        self.physical_len = self.ownership.len();
    }

    fn plan_admission(&mut self, owner: &ExplicitOwner, normalized: &[Pubkey]) -> AdmissionPlan {
        #[cfg(test)]
        self.planning_stats.record_owner_group_lookup();

        let pre_len = self.effective_physical_len_for_planning();

        match self.ownership.owner_group(owner) {
            None => {
                let mut physical_added = 0usize;
                for pubkey in normalized {
                    #[cfg(test)]
                    self.planning_stats.record_refcount_lookup();
                    if self.ownership.owner_refcount(pubkey) == 0 {
                        physical_added += 1;
                    }
                }

                let available_unique = self.cap.saturating_sub(pre_len);
                if physical_added > available_unique {
                    return AdmissionPlan::RejectCap {
                        required_unique: physical_added,
                        available_unique,
                    };
                }

                let Some(projected_final_len) = pre_len.checked_add(physical_added) else {
                    return AdmissionPlan::InternalInvariantViolation;
                };
                if projected_final_len > self.cap {
                    return AdmissionPlan::InternalInvariantViolation;
                }

                AdmissionPlan::Commit(CommitPlan {
                    pre_len,
                    projected_final_len,
                    expected: ExpectedGroupChange::NewGroup {
                        physical_added_len: physical_added,
                    },
                })
            }
            Some(existing) if existing == normalized => AdmissionPlan::Unchanged,
            Some(old_pubkeys) => {
                let old_set: HashSet<Pubkey> = old_pubkeys.iter().copied().collect();
                let new_set: HashSet<Pubkey> = normalized.iter().copied().collect();

                let mut physical_added = 0usize;
                for pubkey in new_set.difference(&old_set) {
                    #[cfg(test)]
                    self.planning_stats.record_refcount_lookup();
                    if self.ownership.owner_refcount(pubkey) == 0 {
                        physical_added += 1;
                    }
                }

                let mut physical_removed = 0usize;
                for pubkey in old_set.difference(&new_set) {
                    #[cfg(test)]
                    self.planning_stats.record_refcount_lookup();
                    if self.ownership.owner_refcount(pubkey) == 1 {
                        physical_removed += 1;
                    }
                }

                let available_unique = self
                    .cap
                    .saturating_sub(pre_len)
                    .saturating_add(physical_removed);
                if physical_added > available_unique {
                    return AdmissionPlan::RejectCap {
                        required_unique: physical_added,
                        available_unique,
                    };
                }

                let Some(projected_final_len) = pre_len
                    .checked_sub(physical_removed)
                    .and_then(|len| len.checked_add(physical_added))
                else {
                    return AdmissionPlan::InternalInvariantViolation;
                };
                if projected_final_len > self.cap {
                    return AdmissionPlan::InternalInvariantViolation;
                }

                AdmissionPlan::Commit(CommitPlan {
                    pre_len,
                    projected_final_len,
                    expected: ExpectedGroupChange::Replaced {
                        physical_added_len: physical_added,
                        physical_removed_len: physical_removed,
                    },
                })
            }
        }
    }

    fn plan_remove(&mut self, owner: &ExplicitOwner) -> RemovePlanResult {
        #[cfg(test)]
        self.planning_stats.record_owner_group_lookup();

        let pubkeys = match self.ownership.owner_group(owner) {
            Some(pubkeys) => pubkeys,
            None => return RemovePlanResult::NotFound,
        };

        let mut physical_removed = Vec::new();
        for pubkey in pubkeys {
            #[cfg(test)]
            self.planning_stats.record_refcount_lookup();
            if self.ownership.owner_refcount(pubkey) == 1 {
                physical_removed.push(*pubkey);
            }
        }
        physical_removed.sort();

        let pre_len = self.effective_physical_len_for_planning();
        let Some(projected_final_len) = pre_len.checked_sub(physical_removed.len()) else {
            return RemovePlanResult::InternalInvariantViolation;
        };

        RemovePlanResult::Ready(RemovePlan {
            pre_len,
            projected_final_len,
            physical_removed,
        })
    }

    fn effective_physical_len_for_planning(&self) -> usize {
        #[cfg(test)]
        if let Some(override_len) = self.test_physical_len_override {
            return override_len;
        }
        self.physical_len
    }
}

fn group_change_matches_plan(change: &GroupChange, expected: ExpectedGroupChange) -> bool {
    match (change, expected) {
        (
            GroupChange::NewGroup { physical_added },
            ExpectedGroupChange::NewGroup { physical_added_len },
        ) => physical_added.len() == physical_added_len,
        (
            GroupChange::Replaced {
                physical_added,
                physical_removed,
            },
            ExpectedGroupChange::Replaced {
                physical_added_len,
                physical_removed_len,
            },
        ) => {
            physical_added.len() == physical_added_len
                && physical_removed.len() == physical_removed_len
        }
        (GroupChange::Unchanged, _) => false,
        _ => false,
    }
}

#[cfg(test)]
fn apply_commit_plan_fault_injection(
    admission: &FixedCapAdmission,
    plan: CommitPlan,
) -> CommitPlan {
    if admission.test_force_commit_plan_mismatch {
        inflate_commit_plan_for_fault_injection(plan)
    } else {
        plan
    }
}

#[cfg(not(test))]
fn apply_commit_plan_fault_injection(
    _admission: &FixedCapAdmission,
    plan: CommitPlan,
) -> CommitPlan {
    plan
}

#[cfg(test)]
fn inflate_commit_plan_for_fault_injection(mut plan: CommitPlan) -> CommitPlan {
    match &mut plan.expected {
        ExpectedGroupChange::NewGroup { physical_added_len } => {
            *physical_added_len = physical_added_len.saturating_add(1)
        }
        ExpectedGroupChange::Replaced {
            physical_added_len, ..
        } => *physical_added_len = physical_added_len.saturating_add(1),
    }
    plan
}

fn map_group_change_to_result(change: GroupChange) -> FixedCapAdmissionResult {
    match change {
        GroupChange::NewGroup { physical_added } => {
            if physical_added.is_empty() {
                FixedCapAdmissionResult::OwnerAddedNoNewPubkey
            } else {
                FixedCapAdmissionResult::Inserted { physical_added }
            }
        }
        GroupChange::Unchanged => FixedCapAdmissionResult::Unchanged,
        GroupChange::Replaced {
            physical_added,
            physical_removed,
        } => FixedCapAdmissionResult::OwnerReplaced {
            physical_added,
            physical_removed,
        },
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
    use super::super::explicit_ownership::{ExplicitConsumer, ExplicitOwnerKey};
    use super::*;
    use solana_sdk::pubkey::Pubkey;
    use std::collections::{BTreeMap, BTreeSet};

    fn pk(seed: u8) -> Pubkey {
        Pubkey::new_from_array([seed; 32])
    }

    fn pool_owner(consumer: ExplicitConsumer, seed: u8) -> ExplicitOwner {
        ExplicitOwner {
            consumer,
            owner_key: ExplicitOwnerKey::Pool(pk(seed)),
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

    fn admission_index_snapshot(admission: &FixedCapAdmission) -> IndexSnapshot {
        let mut groups = BTreeMap::new();
        for group in admission.snapshot_owner_groups() {
            let owner = ExplicitOwner {
                consumer: group.consumer,
                owner_key: group.owner_key,
            };
            groups.insert(owner, group.pubkeys.into_iter().collect());
        }
        index_snapshot_from_groups(&groups)
    }

    fn result_class(result: &FixedCapAdmissionResult) -> &'static str {
        match result {
            FixedCapAdmissionResult::Inserted { .. } => "Inserted",
            FixedCapAdmissionResult::OwnerReplaced { .. } => "OwnerReplaced",
            FixedCapAdmissionResult::OwnerAddedNoNewPubkey => "OwnerAddedNoNewPubkey",
            FixedCapAdmissionResult::Unchanged => "Unchanged",
            FixedCapAdmissionResult::RejectedCap { .. } => "RejectedCap",
            FixedCapAdmissionResult::RejectedInvalidGroup => "RejectedInvalidGroup",
            FixedCapAdmissionResult::InternalInvariantViolation => "InternalInvariantViolation",
        }
    }

    fn expected_refcount_lookups(old: Option<&[Pubkey]>, new: &[Pubkey]) -> u64 {
        match old {
            None => u64::try_from(new.len()).unwrap_or(u64::MAX),
            Some(existing) if existing == new => 0,
            Some(old_pubkeys) => {
                let old_set: HashSet<Pubkey> = old_pubkeys.iter().copied().collect();
                let new_set: HashSet<Pubkey> = new.iter().copied().collect();
                u64::try_from(old_set.symmetric_difference(&new_set).count()).unwrap_or(u64::MAX)
            }
        }
    }

    fn assert_planning_stats_exact(
        stats: &PlanningStats,
        owner_group_lookups: u64,
        refcount_lookups: u64,
    ) {
        assert_eq!(stats.owner_group_lookups, owner_group_lookups);
        assert_eq!(stats.refcount_lookups, refcount_lookups);
    }

    /// Independent capped reference model — never calls [`FixedCapAdmission`].
    ///
    /// Admission uses a cloned candidate-state union algorithm, not production projection math.
    #[derive(Debug, Clone)]
    struct RefCapModel {
        cap: usize,
        groups: BTreeMap<ExplicitOwner, BTreeSet<Pubkey>>,
    }

    impl RefCapModel {
        fn new(cap: usize) -> Self {
            Self {
                cap,
                groups: BTreeMap::new(),
            }
        }

        fn len(&self) -> usize {
            physical_pubkeys_from_groups(&self.groups).len()
        }

        fn index_snapshot(&self) -> IndexSnapshot {
            index_snapshot_from_groups(&self.groups)
        }

        fn owner_refcount(&self, pubkey: &Pubkey) -> usize {
            self.groups
                .iter()
                .filter(|(_, pubkeys)| pubkeys.contains(pubkey))
                .count()
        }

        fn try_admit_group(
            &mut self,
            owner: ExplicitOwner,
            pubkeys: impl IntoIterator<Item = Pubkey>,
        ) -> FixedCapAdmissionResult {
            let normalized: BTreeSet<Pubkey> = pubkeys.into_iter().collect();
            if normalized.is_empty() {
                return FixedCapAdmissionResult::RejectedInvalidGroup;
            }

            let before_snapshot = self.index_snapshot();
            let old_group = self.groups.get(&owner).cloned();

            if old_group.as_ref() == Some(&normalized) {
                return FixedCapAdmissionResult::Unchanged;
            }

            let physical_before = physical_pubkeys_from_groups(&self.groups);
            let mut candidate_groups = self.groups.clone();
            candidate_groups.insert(owner.clone(), normalized.clone());
            let physical_after = physical_pubkeys_from_groups(&candidate_groups);

            let mut physical_added: Vec<Pubkey> = physical_after
                .difference(&physical_before)
                .copied()
                .collect();
            physical_added.sort();

            let mut physical_removed: Vec<Pubkey> = physical_before
                .difference(&physical_after)
                .copied()
                .collect();
            physical_removed.sort();

            if physical_after.len() > self.cap {
                assert_eq!(self.index_snapshot(), before_snapshot);
                let current_len = physical_before.len();
                return FixedCapAdmissionResult::RejectedCap {
                    required_unique: physical_added.len(),
                    available_unique: self
                        .cap
                        .saturating_sub(current_len)
                        .saturating_add(physical_removed.len()),
                };
            }

            self.groups = candidate_groups;

            match old_group {
                None => {
                    if physical_added.is_empty() {
                        FixedCapAdmissionResult::OwnerAddedNoNewPubkey
                    } else {
                        FixedCapAdmissionResult::Inserted { physical_added }
                    }
                }
                Some(_) => FixedCapAdmissionResult::OwnerReplaced {
                    physical_added,
                    physical_removed,
                },
            }
        }

        fn remove_group(&mut self, owner: &ExplicitOwner) -> Option<FixedCapRemoveResult> {
            let pubkeys = self.groups.get(owner)?.clone();
            let physical_before = physical_pubkeys_from_groups(&self.groups);
            let snapshot = OwnerGroupSnapshot {
                consumer: owner.consumer,
                owner_key: owner.owner_key.clone(),
                pubkeys: pubkeys.iter().copied().collect(),
            };
            self.groups.remove(owner)?;
            let physical_after = physical_pubkeys_from_groups(&self.groups);
            let mut physical_removed: Vec<Pubkey> = physical_before
                .difference(&physical_after)
                .copied()
                .collect();
            physical_removed.sort();
            Some(FixedCapRemoveResult {
                snapshot,
                physical_removed,
            })
        }
    }

    fn independent_physical_len(admission: &FixedCapAdmission) -> usize {
        admission_index_snapshot(admission).pubkeys.len()
    }

    fn assert_physical_len_invariant(admission: &FixedCapAdmission) {
        assert_eq!(admission.len(), independent_physical_len(admission));
        assert_eq!(admission.len(), admission.ownership.len());
    }

    fn assert_indexes_match(admission: &FixedCapAdmission, model: &RefCapModel) {
        assert_eq!(admission.len(), model.len());
        assert_eq!(admission_index_snapshot(admission), model.index_snapshot());

        let all_pubkeys: BTreeSet<Pubkey> = admission
            .snapshot_pubkeys()
            .into_iter()
            .chain(model.index_snapshot().pubkeys)
            .collect();
        for pubkey in all_pubkeys {
            assert_eq!(
                admission.owner_refcount(&pubkey),
                model.owner_refcount(&pubkey)
            );
        }
    }

    fn assert_operation(
        admission: &FixedCapAdmission,
        model: &RefCapModel,
        result: &FixedCapAdmissionResult,
        expected: &FixedCapAdmissionResult,
    ) {
        assert_eq!(result_class(result), result_class(expected));
        match (result, expected) {
            (
                FixedCapAdmissionResult::Inserted { physical_added },
                FixedCapAdmissionResult::Inserted {
                    physical_added: expected_added,
                },
            ) => assert_eq!(physical_added, expected_added),
            (
                FixedCapAdmissionResult::OwnerReplaced {
                    physical_added,
                    physical_removed,
                },
                FixedCapAdmissionResult::OwnerReplaced {
                    physical_added: expected_added,
                    physical_removed: expected_removed,
                },
            ) => {
                assert_eq!(physical_added, expected_added);
                assert_eq!(physical_removed, expected_removed);
            }
            (
                FixedCapAdmissionResult::RejectedCap {
                    required_unique,
                    available_unique,
                },
                FixedCapAdmissionResult::RejectedCap {
                    required_unique: expected_required,
                    available_unique: expected_available,
                },
            ) => {
                assert_eq!(required_unique, expected_required);
                assert_eq!(available_unique, expected_available);
            }
            (
                FixedCapAdmissionResult::OwnerAddedNoNewPubkey,
                FixedCapAdmissionResult::OwnerAddedNoNewPubkey,
            )
            | (FixedCapAdmissionResult::Unchanged, FixedCapAdmissionResult::Unchanged)
            | (
                FixedCapAdmissionResult::RejectedInvalidGroup,
                FixedCapAdmissionResult::RejectedInvalidGroup,
            ) => {}
            _ => panic!("result mismatch: {result:?} vs {expected:?}"),
        }
        assert_indexes_match(admission, model);
        assert_physical_len_invariant(admission);
    }

    fn assert_admitted(result: FixedCapAdmissionResult) {
        match result {
            FixedCapAdmissionResult::Inserted { .. }
            | FixedCapAdmissionResult::OwnerReplaced { .. }
            | FixedCapAdmissionResult::OwnerAddedNoNewPubkey
            | FixedCapAdmissionResult::Unchanged => {}
            other => panic!("expected successful admission, got {other:?}"),
        }
    }

    fn assert_removed(outcome: FixedCapRemoveOutcome) -> FixedCapRemoveResult {
        match outcome {
            FixedCapRemoveOutcome::Removed(result) => result,
            other => panic!("expected Removed, got {other:?}"),
        }
    }

    #[test]
    fn new_deduplicated_group_under_cap_is_admitted() {
        let mut admission = FixedCapAdmission::new(3);
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        let result = admission.try_admit_group(owner.clone(), [pk(1), pk(2)]);
        assert_eq!(
            result,
            FixedCapAdmissionResult::Inserted {
                physical_added: vec![pk(1), pk(2)],
            }
        );
        assert_eq!(admission.len(), 2);
    }

    #[test]
    fn group_exactly_at_cap_is_admitted() {
        let mut admission = FixedCapAdmission::new(2);
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        let result = admission.try_admit_group(owner.clone(), [pk(1), pk(2)]);
        assert_eq!(
            result,
            FixedCapAdmissionResult::Inserted {
                physical_added: vec![pk(1), pk(2)],
            }
        );
        assert_eq!(admission.len(), 2);
        assert_eq!(admission.cap(), 2);
    }

    #[test]
    fn group_over_cap_is_rejected_without_mutation() {
        let mut admission = FixedCapAdmission::new(2);
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        let before = admission_index_snapshot(&admission);
        let result = admission.try_admit_group(owner.clone(), [pk(1), pk(2), pk(3)]);
        assert_eq!(
            result,
            FixedCapAdmissionResult::RejectedCap {
                required_unique: 3,
                available_unique: 2,
            }
        );
        assert_eq!(admission_index_snapshot(&admission), before);
        assert!(admission.is_empty());
    }

    #[test]
    fn two_vault_group_is_fully_admitted_or_rejected() {
        let mut admission = FixedCapAdmission::new(2);
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        let vault_a = pk(10);
        let vault_b = pk(11);

        let result = admission.try_admit_group(owner.clone(), [vault_a, vault_b]);
        assert_eq!(
            result,
            FixedCapAdmissionResult::Inserted {
                physical_added: vec![vault_a, vault_b],
            }
        );

        let mut admission_full = FixedCapAdmission::new(1);
        let before = admission_index_snapshot(&admission_full);
        let reject = admission_full.try_admit_group(owner, [vault_a, vault_b]);
        assert!(matches!(
            reject,
            FixedCapAdmissionResult::RejectedCap { .. }
        ));
        assert_eq!(admission_index_snapshot(&admission_full), before);
    }

    #[test]
    fn shared_pubkey_at_full_cap_can_add_owner_without_new_pubkey() {
        let mut admission = FixedCapAdmission::new(1);
        let shared = pk(1);
        let owner_a = pool_owner(ExplicitConsumer::Momentum, 1);
        let owner_b = pool_owner(ExplicitConsumer::Arb, 2);

        assert_admitted(admission.try_admit_group(owner_a.clone(), [shared]));
        assert_eq!(admission.len(), 1);

        let result = admission.try_admit_group(owner_b.clone(), [shared]);
        assert_eq!(result, FixedCapAdmissionResult::OwnerAddedNoNewPubkey);
        assert_eq!(admission.len(), 1);
        assert_eq!(admission.owner_refcount(&shared), 2);
    }

    #[test]
    fn replacement_at_cap_uses_exclusive_outgoing_key() {
        let mut admission = FixedCapAdmission::new(2);
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        let k1 = pk(1);
        let k2 = pk(2);
        let k3 = pk(3);

        assert_admitted(admission.try_admit_group(owner.clone(), [k1, k2]));
        let result = admission.try_admit_group(owner.clone(), [k2, k3]);
        assert_eq!(
            result,
            FixedCapAdmissionResult::OwnerReplaced {
                physical_added: vec![k3],
                physical_removed: vec![k1],
            }
        );
        assert_eq!(admission.len(), 2);
    }

    #[test]
    fn replacement_with_shared_outgoing_key_does_not_count_as_free() {
        let mut admission = FixedCapAdmission::new(2);
        let shared = pk(1);
        let owner_a = pool_owner(ExplicitConsumer::Momentum, 1);
        let owner_b = pool_owner(ExplicitConsumer::Arb, 2);

        assert_admitted(admission.try_admit_group(owner_a.clone(), [shared, pk(2)]));
        assert_admitted(admission.try_admit_group(owner_b.clone(), [shared]));
        assert_eq!(admission.len(), 2);

        let before = admission_index_snapshot(&admission);
        let result = admission.try_admit_group(owner_a.clone(), [pk(3), pk(4)]);
        assert_eq!(
            result,
            FixedCapAdmissionResult::RejectedCap {
                required_unique: 2,
                available_unique: 1,
            }
        );
        assert_eq!(admission_index_snapshot(&admission), before);
    }

    #[test]
    fn replacement_over_cap_is_fully_rejected() {
        let mut admission = FixedCapAdmission::new(2);
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        let k1 = pk(1);
        let k2 = pk(2);

        assert_admitted(admission.try_admit_group(owner.clone(), [k1, k2]));
        let before = admission_index_snapshot(&admission);
        let result = admission.try_admit_group(owner.clone(), [k2, pk(3), pk(4)]);
        assert_eq!(
            result,
            FixedCapAdmissionResult::RejectedCap {
                required_unique: 2,
                available_unique: 1,
            }
        );
        assert_eq!(admission_index_snapshot(&admission), before);
    }

    #[test]
    fn idempotent_reordered_duplicate_readmit_is_unchanged() {
        let mut admission = FixedCapAdmission::new(3);
        let owner = pool_owner(ExplicitConsumer::Tracker, 9);
        let keys = [pk(1), pk(2), pk(3)];
        assert_admitted(admission.try_admit_group(owner.clone(), keys));
        let before = admission_index_snapshot(&admission);
        let result = admission.try_admit_group(owner.clone(), [pk(3), pk(1), pk(2), pk(2), pk(1)]);
        assert_eq!(result, FixedCapAdmissionResult::Unchanged);
        assert_eq!(admission_index_snapshot(&admission), before);
    }

    #[test]
    fn remove_reports_exact_physical_deltas() {
        let mut admission = FixedCapAdmission::new(3);
        let shared = pk(1);
        let owner_a = pool_owner(ExplicitConsumer::Momentum, 1);
        let owner_b = pool_owner(ExplicitConsumer::Arb, 2);

        assert_admitted(admission.try_admit_group(owner_a.clone(), [shared, pk(2)]));
        assert_admitted(admission.try_admit_group(owner_b.clone(), [shared]));

        let removed = assert_removed(admission.remove_group(&owner_a));
        assert_eq!(removed.physical_removed, vec![pk(2)]);
        assert_eq!(admission.len(), 1);
        assert!(admission.contains_pubkey(&shared));

        let removed_b = assert_removed(admission.remove_group(&owner_b));
        assert_eq!(removed_b.physical_removed, vec![shared]);
        assert!(admission.is_empty());
    }

    #[test]
    fn cap_zero_usize_max_and_overflow_near_projections_are_fail_closed() {
        let mut zero = FixedCapAdmission::new(0);
        assert_eq!(
            zero.try_admit_group(pool_owner(ExplicitConsumer::Momentum, 1), [pk(1)]),
            FixedCapAdmissionResult::RejectedCap {
                required_unique: 1,
                available_unique: 0,
            }
        );
        assert!(zero.is_empty());

        let mut max_cap = FixedCapAdmission::new(usize::MAX);
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        let result = max_cap.try_admit_group(owner.clone(), [pk(1), pk(2)]);
        assert!(matches!(result, FixedCapAdmissionResult::Inserted { .. }));
        assert_eq!(max_cap.len(), 2);

        let mut bounded = FixedCapAdmission::new(3);
        assert_admitted(bounded.try_admit_group(owner.clone(), [pk(1), pk(2), pk(3)]));
        let before = admission_index_snapshot(&bounded);
        let reject = bounded.try_admit_group(
            pool_owner(ExplicitConsumer::Arb, 2),
            (0u8..=255).map(pk).collect::<Vec<_>>(),
        );
        assert!(matches!(
            reject,
            FixedCapAdmissionResult::RejectedCap { .. }
        ));
        assert_eq!(admission_index_snapshot(&bounded), before);
    }

    #[test]
    fn planning_stats_match_exact_edge_refcount_lookups() {
        let mut admission = FixedCapAdmission::new(16);
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        let insert_keys = [pk(1), pk(2)];

        let _ = admission.try_admit_group(owner.clone(), insert_keys);
        assert_planning_stats_exact(admission.planning_stats(), 1, 2);

        let replace_small = [pk(2), pk(3)];
        let before = admission.planning_stats().clone();
        let _ = admission.try_admit_group(owner.clone(), replace_small);
        assert_eq!(
            admission.planning_stats().owner_group_lookups,
            before.owner_group_lookups + 1
        );
        assert_eq!(
            admission.planning_stats().refcount_lookups,
            before.refcount_lookups + expected_refcount_lookups(Some(&insert_keys), &replace_small)
        );

        let large_old: Vec<Pubkey> = (10u8..=19).map(pk).collect();
        let before = admission.planning_stats().clone();
        let _ = admission.try_admit_group(owner.clone(), large_old.clone());
        assert_eq!(
            admission.planning_stats().owner_group_lookups,
            before.owner_group_lookups + 1
        );
        assert_eq!(
            admission.planning_stats().refcount_lookups,
            before.refcount_lookups + expected_refcount_lookups(Some(&replace_small), &large_old)
        );
        assert_eq!(admission.owner_group(&owner), Some(large_old.as_slice()));

        let shrink_new = [pk(10), pk(11)];
        let before = admission.planning_stats().clone();
        let _ = admission.try_admit_group(owner.clone(), shrink_new);
        let shrink_edges = expected_refcount_lookups(Some(&large_old), &shrink_new);
        assert_eq!(shrink_edges, 8);
        assert_eq!(
            admission.planning_stats().owner_group_lookups,
            before.owner_group_lookups + 1
        );
        assert_eq!(
            admission.planning_stats().refcount_lookups,
            before.refcount_lookups + shrink_edges
        );

        let grow_new: Vec<Pubkey> = (20u8..=29).map(pk).collect();
        let before = admission.planning_stats().clone();
        let _ = admission.try_admit_group(owner.clone(), grow_new.clone());
        let grow_edges = expected_refcount_lookups(Some(&shrink_new), &grow_new);
        assert_eq!(grow_edges, 12);
        assert_eq!(
            admission.planning_stats().owner_group_lookups,
            before.owner_group_lookups + 1
        );
        assert_eq!(
            admission.planning_stats().refcount_lookups,
            before.refcount_lookups + grow_edges
        );

        let before = admission.planning_stats().clone();
        let _ = admission.try_admit_group(owner.clone(), grow_new);
        assert_eq!(
            admission.planning_stats().owner_group_lookups,
            before.owner_group_lookups + 1
        );
        assert_eq!(
            admission.planning_stats().refcount_lookups,
            before.refcount_lookups
        );
    }

    #[test]
    fn commit_plan_mismatch_fault_injection_repairs_cache_and_returns_internal_error() {
        let mut admission = FixedCapAdmission::new(4);
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        assert_admitted(admission.try_admit_group(owner.clone(), [pk(1), pk(2)]));

        admission.set_test_force_commit_plan_mismatch(true);
        let result = admission.try_admit_group(owner.clone(), [pk(2), pk(3), pk(4)]);
        assert_eq!(result, FixedCapAdmissionResult::InternalInvariantViolation);
        assert_eq!(
            admission.owner_group(&owner),
            Some([pk(2), pk(3), pk(4)].as_slice())
        );
        assert_physical_len_invariant(&admission);
        assert_eq!(admission.len(), 3);
    }

    #[test]
    fn admission_arithmetic_overflow_returns_internal_error_without_mutation() {
        let mut admission = FixedCapAdmission::new(8);
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        assert_admitted(admission.try_admit_group(owner.clone(), [pk(1), pk(2)]));
        let before = admission_index_snapshot(&admission);

        admission.set_test_physical_len_override(Some(0));
        let result = admission.try_admit_group(owner.clone(), [pk(3)]);
        assert_eq!(result, FixedCapAdmissionResult::InternalInvariantViolation);
        assert_eq!(admission_index_snapshot(&admission), before);
        assert_eq!(admission.len(), 2);
    }

    #[test]
    fn remove_underflow_plan_returns_internal_error_without_mutation() {
        let mut admission = FixedCapAdmission::new(4);
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        assert_admitted(admission.try_admit_group(owner.clone(), [pk(1), pk(2)]));
        let before = admission_index_snapshot(&admission);

        admission.set_test_physical_len_override(Some(0));
        let outcome = admission.remove_group(&owner);
        assert_eq!(outcome, FixedCapRemoveOutcome::InternalInvariantViolation);
        assert_eq!(admission_index_snapshot(&admission), before);
        assert_physical_len_invariant(&admission);
    }

    #[test]
    fn remove_unknown_owner_returns_not_found() {
        let mut admission = FixedCapAdmission::new(3);
        let unknown = pool_owner(ExplicitConsumer::Tracker, 99);
        assert_eq!(
            admission.remove_group(&unknown),
            FixedCapRemoveOutcome::NotFound
        );
    }

    #[test]
    fn remove_exclusive_pubkey_frees_physical_slot() {
        let mut admission = FixedCapAdmission::new(3);
        let mut model = RefCapModel::new(3);
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        let exclusive = pk(10);

        let result = admission.try_admit_group(owner.clone(), [exclusive, pk(11)]);
        let expected = model.try_admit_group(owner.clone(), [exclusive, pk(11)]);
        assert_operation(&admission, &model, &result, &expected);

        let removed = assert_removed(admission.remove_group(&owner));
        let ref_removed = model.remove_group(&owner).expect("owner present");
        assert_eq!(removed, ref_removed);
        assert_eq!(removed.physical_removed, vec![exclusive, pk(11)]);
        assert!(admission.is_empty());
        assert_physical_len_invariant(&admission);
        assert_indexes_match(&admission, &model);
    }

    #[test]
    fn remove_shared_pubkey_keeps_physical_tracking() {
        let mut admission = FixedCapAdmission::new(3);
        let mut model = RefCapModel::new(3);
        let shared = pk(1);
        let owner_a = pool_owner(ExplicitConsumer::Momentum, 1);
        let owner_b = pool_owner(ExplicitConsumer::Arb, 2);

        let result_a = admission.try_admit_group(owner_a.clone(), [shared, pk(2)]);
        let expected_a = model.try_admit_group(owner_a.clone(), [shared, pk(2)]);
        assert_operation(&admission, &model, &result_a, &expected_a);

        let result_b = admission.try_admit_group(owner_b.clone(), [shared]);
        let expected_b = model.try_admit_group(owner_b.clone(), [shared]);
        assert_operation(&admission, &model, &result_b, &expected_b);

        let removed = assert_removed(admission.remove_group(&owner_a));
        let ref_removed = model.remove_group(&owner_a).expect("owner_a present");
        assert_eq!(removed, ref_removed);
        assert_eq!(removed.physical_removed, vec![pk(2)]);
        assert_eq!(admission.len(), 1);
        assert!(admission.contains_pubkey(&shared));
        assert_eq!(admission.owner_refcount(&shared), 1);
        assert_physical_len_invariant(&admission);
        assert_indexes_match(&admission, &model);
    }

    #[test]
    fn cached_physical_len_matches_snapshot_through_long_sequence() {
        let mut admission = FixedCapAdmission::new(6);
        let mut model = RefCapModel::new(6);

        let owners = [
            pool_owner(ExplicitConsumer::Momentum, 1),
            pool_owner(ExplicitConsumer::Arb, 2),
            pool_owner(ExplicitConsumer::Tracker, 3),
        ];
        let keys: Vec<Pubkey> = (1u8..=12).map(pk).collect();

        let mut step = 0usize;
        for owner in owners.iter() {
            let group: Vec<Pubkey> = keys.iter().copied().skip(step).take(2).collect();
            step += 1;
            let before_len = admission.len();
            let result = admission.try_admit_group(owner.clone(), group.clone());
            let expected = model.try_admit_group(owner.clone(), group);
            assert_operation(&admission, &model, &result, &expected);
            if matches!(result, FixedCapAdmissionResult::RejectedCap { .. }) {
                assert_eq!(admission.len(), before_len);
            }
            assert_physical_len_invariant(&admission);
        }

        let shared_owner = owners[0].clone();
        let replace_keys = vec![keys[1], keys[5], keys[6]];
        let result = admission.try_admit_group(shared_owner.clone(), replace_keys.clone());
        let expected = model.try_admit_group(shared_owner.clone(), replace_keys);
        assert_operation(&admission, &model, &result, &expected);
        assert_physical_len_invariant(&admission);

        let reject = admission.try_admit_group(
            pool_owner(ExplicitConsumer::Wallet, 9),
            vec![keys[7], keys[8], keys[9], keys[10]],
        );
        assert!(matches!(
            reject,
            FixedCapAdmissionResult::RejectedCap { .. }
        ));
        assert_physical_len_invariant(&admission);

        let unchanged =
            admission.try_admit_group(shared_owner.clone(), [keys[5], keys[1], keys[6]]);
        assert_eq!(unchanged, FixedCapAdmissionResult::Unchanged);
        assert_physical_len_invariant(&admission);

        let invalid = admission.try_admit_group(shared_owner.clone(), []);
        assert_eq!(invalid, FixedCapAdmissionResult::RejectedInvalidGroup);
        assert_physical_len_invariant(&admission);

        for owner in owners.iter().rev() {
            let before_len = admission.len();
            let removed = admission.remove_group(owner);
            let ref_removed = model.remove_group(owner);
            match (&removed, &ref_removed) {
                (FixedCapRemoveOutcome::Removed(actual), Some(expected)) => {
                    assert_eq!(actual, expected);
                    let delta = actual.physical_removed.len();
                    assert_eq!(admission.len(), before_len.saturating_sub(delta));
                }
                (FixedCapRemoveOutcome::NotFound, None) => {}
                _ => panic!("remove mismatch: {removed:?} vs {ref_removed:?}"),
            }
            assert_physical_len_invariant(&admission);
            assert_indexes_match(&admission, &model);
        }
    }

    #[test]
    fn bounded_reference_model_matches_fixed_cap_admission() {
        let caps = [0usize, 1, 2, 3, 5];
        let seeds = [1u64, 7, 13, 42, 99, 255];

        caps.iter().copied().for_each(|cap| {
            seeds.iter().copied().for_each(|seed| {
                let mut admission = FixedCapAdmission::new(cap);
                let mut model = RefCapModel::new(cap);

                let owners = [
                    pool_owner(ExplicitConsumer::Momentum, seed as u8),
                    pool_owner(ExplicitConsumer::Arb, seed.wrapping_add(1) as u8),
                    pool_owner(ExplicitConsumer::Tracker, seed.wrapping_add(2) as u8),
                    ExplicitOwner {
                        consumer: ExplicitConsumer::Wallet,
                        owner_key: ExplicitOwnerKey::Wallet,
                    },
                ];

                let key_pool: Vec<Pubkey> =
                    (0u8..8).map(|b| pk(b.wrapping_add(seed as u8))).collect();

                owners.iter().enumerate().for_each(|(step, owner)| {
                    let keys: Vec<Pubkey> = key_pool
                        .iter()
                        .copied()
                        .filter(|k| (k.to_bytes()[0] as usize + step + seed as usize) % 3 != 0)
                        .take(2 + (step % 2))
                        .collect();
                    if keys.is_empty() {
                        return;
                    }

                    let before = admission_index_snapshot(&admission);
                    let result = admission.try_admit_group(owner.clone(), keys.clone());
                    let expected = model.try_admit_group(owner.clone(), keys);
                    if matches!(result, FixedCapAdmissionResult::RejectedCap { .. }) {
                        assert_eq!(admission_index_snapshot(&admission), before);
                    }
                    assert_operation(&admission, &model, &result, &expected);
                });

                let shared_owner = pool_owner(ExplicitConsumer::Momentum, seed as u8);
                let key_batches = [
                    vec![key_pool[0], key_pool[1], key_pool[2]],
                    vec![key_pool[2], key_pool[1], key_pool[0], key_pool[0]],
                    vec![key_pool[3], key_pool[4]],
                ];
                key_batches.iter().for_each(|keys| {
                    let before = admission_index_snapshot(&admission);
                    let result = admission.try_admit_group(shared_owner.clone(), keys.clone());
                    let expected = model.try_admit_group(shared_owner.clone(), keys.clone());
                    if matches!(result, FixedCapAdmissionResult::RejectedCap { .. }) {
                        assert_eq!(admission_index_snapshot(&admission), before);
                    }
                    assert_operation(&admission, &model, &result, &expected);
                });

                let remove_owner = pool_owner(ExplicitConsumer::Arb, seed.wrapping_add(1) as u8);
                let removed = admission.remove_group(&remove_owner);
                let ref_removed = model.remove_group(&remove_owner);
                match (&removed, &ref_removed) {
                    (FixedCapRemoveOutcome::Removed(actual), Some(expected)) => {
                        assert_eq!(actual, expected);
                    }
                    (FixedCapRemoveOutcome::NotFound, None) => {}
                    _ => panic!("remove mismatch: {removed:?} vs {ref_removed:?}"),
                }
                assert_physical_len_invariant(&admission);
                assert_indexes_match(&admission, &model);

                assert!(admission.len() <= cap);
            });
        });
    }

    impl FixedCapAdmission {
        fn contains_pubkey(&self, pubkey: &Pubkey) -> bool {
            self.ownership.contains(pubkey)
        }
    }
}
