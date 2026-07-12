//! Fixed-cap admission over [`ExplicitOwnership`] (PR 1b foundation).
//!
//! Pure local state: admits or rejects owner groups against an immutable cap.
//! No eviction, priority, LRU, cap shrink, or runtime wiring.

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
}

/// Precomputed admission outcome — all checked arithmetic happens before any ownership mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AdmissionPlan {
    Unchanged,
    RejectCap {
        required_unique: usize,
        available_unique: usize,
    },
    Commit {
        projected_final_len: usize,
        expected_added_len: usize,
        expected_removed_len: usize,
    },
}

/// Precomputed removal outcome — checked arithmetic before [`ExplicitOwnership::remove_group`].
#[derive(Debug, Clone, PartialEq, Eq)]
struct RemovePlan {
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
            AdmissionPlan::Commit {
                projected_final_len,
                expected_added_len,
                expected_removed_len,
            } => {
                let change = match self.ownership.upsert_group(owner, normalized) {
                    Ok(change) => change,
                    Err(EmptyOwnerGroupError) => {
                        return FixedCapAdmissionResult::RejectedInvalidGroup;
                    }
                };
                debug_assert_commit_matches_plan(&change, expected_added_len, expected_removed_len);
                self.physical_len = projected_final_len;
                map_group_change_to_result(change)
            }
        }
    }

    /// Remove an owner group and return exact physical pubkey deltas.
    pub fn remove_group(&mut self, owner: &ExplicitOwner) -> Option<FixedCapRemoveResult> {
        let plan = self.plan_remove(owner)?;
        let snapshot = self.ownership.remove_group(owner)?;
        self.physical_len = plan.projected_final_len;
        Some(FixedCapRemoveResult {
            snapshot,
            physical_removed: plan.physical_removed,
        })
    }

    fn plan_admission(&mut self, owner: &ExplicitOwner, normalized: &[Pubkey]) -> AdmissionPlan {
        #[cfg(test)]
        self.planning_stats.record_owner_group_lookup();

        let current_len = self.physical_len;

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

                let available_unique = self.cap.saturating_sub(current_len);
                if physical_added > available_unique {
                    return AdmissionPlan::RejectCap {
                        required_unique: physical_added,
                        available_unique,
                    };
                }

                let Some(projected_final_len) = current_len
                    .checked_add(physical_added)
                    .filter(|&len| len <= self.cap)
                else {
                    return AdmissionPlan::RejectCap {
                        required_unique: physical_added,
                        available_unique,
                    };
                };

                AdmissionPlan::Commit {
                    projected_final_len,
                    expected_added_len: physical_added,
                    expected_removed_len: 0,
                }
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
                    .saturating_sub(current_len)
                    .saturating_add(physical_removed);
                if physical_added > available_unique {
                    return AdmissionPlan::RejectCap {
                        required_unique: physical_added,
                        available_unique,
                    };
                }

                let Some(projected_final_len) = current_len
                    .checked_sub(physical_removed)
                    .and_then(|len| len.checked_add(physical_added))
                    .filter(|&len| len <= self.cap)
                else {
                    return AdmissionPlan::RejectCap {
                        required_unique: physical_added,
                        available_unique,
                    };
                };

                AdmissionPlan::Commit {
                    projected_final_len,
                    expected_added_len: physical_added,
                    expected_removed_len: physical_removed,
                }
            }
        }
    }

    fn plan_remove(&self, owner: &ExplicitOwner) -> Option<RemovePlan> {
        let pubkeys = self.ownership.owner_group(owner)?;
        let mut physical_removed = Vec::new();
        for pubkey in pubkeys {
            if self.ownership.owner_refcount(pubkey) == 1 {
                physical_removed.push(*pubkey);
            }
        }
        physical_removed.sort();
        let projected_final_len = self.physical_len.checked_sub(physical_removed.len())?;
        Some(RemovePlan {
            projected_final_len,
            physical_removed,
        })
    }
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

fn debug_assert_commit_matches_plan(
    change: &GroupChange,
    expected_added_len: usize,
    expected_removed_len: usize,
) {
    match change {
        GroupChange::Unchanged => {
            debug_assert_eq!(expected_added_len, 0);
            debug_assert_eq!(expected_removed_len, 0);
        }
        GroupChange::NewGroup { physical_added } => {
            debug_assert_eq!(physical_added.len(), expected_added_len);
            debug_assert_eq!(expected_removed_len, 0);
        }
        GroupChange::Replaced {
            physical_added,
            physical_removed,
        } => {
            debug_assert_eq!(physical_added.len(), expected_added_len);
            debug_assert_eq!(physical_removed.len(), expected_removed_len);
        }
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
        }
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

    fn planning_lookup_upper_bound(group_size: usize, is_replace: bool) -> u64 {
        let owner_group = 1;
        let refcount = if is_replace {
            u64::try_from(group_size.saturating_mul(2)).unwrap_or(u64::MAX)
        } else {
            u64::try_from(group_size).unwrap_or(u64::MAX)
        };
        owner_group + refcount
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

        let removed = admission.remove_group(&owner_a).expect("owner_a present");
        assert_eq!(removed.physical_removed, vec![pk(2)]);
        assert_eq!(admission.len(), 1);
        assert!(admission.contains_pubkey(&shared));

        let removed_b = admission.remove_group(&owner_b).expect("owner_b present");
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
    fn planning_stats_bounded_by_group_size_per_operation() {
        let mut admission = FixedCapAdmission::new(4);
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);

        let _ = admission.try_admit_group(owner.clone(), [pk(1), pk(2)]);
        let stats_after_insert = admission.planning_stats().clone();
        let insert_bound = planning_lookup_upper_bound(2, false);
        assert!(stats_after_insert.owner_group_lookups <= insert_bound);
        assert!(stats_after_insert.refcount_lookups <= insert_bound);
        assert!(stats_after_insert.refcount_lookups > 0);

        let _ = admission.try_admit_group(owner.clone(), [pk(2), pk(3)]);
        let stats_after_replace = admission.planning_stats().clone();
        let replace_bound =
            stats_after_insert.owner_group_lookups + planning_lookup_upper_bound(2, true);
        let replace_refcount_bound =
            stats_after_insert.refcount_lookups + planning_lookup_upper_bound(2, true);
        assert!(stats_after_replace.owner_group_lookups <= replace_bound);
        assert!(stats_after_replace.refcount_lookups <= replace_refcount_bound);

        let _ = admission.try_admit_group(owner.clone(), [pk(2), pk(3)]);
        let stats_after_unchanged = admission.planning_stats().clone();
        assert_eq!(
            stats_after_unchanged.refcount_lookups,
            stats_after_replace.refcount_lookups
        );
        assert_eq!(
            stats_after_unchanged.owner_group_lookups,
            stats_after_replace.owner_group_lookups + 1
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

        let removed = admission.remove_group(&owner).expect("owner present");
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

        let removed = admission.remove_group(&owner_a).expect("owner_a present");
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
        for owner in &owners {
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
            assert_eq!(removed, ref_removed);
            if let Some(removed) = removed.as_ref() {
                let delta = removed.physical_removed.len();
                assert_eq!(admission.len(), before_len.saturating_sub(delta));
            }
            assert_physical_len_invariant(&admission);
            assert_indexes_match(&admission, &model);
        }
    }

    #[test]
    fn bounded_reference_model_matches_fixed_cap_admission() {
        let caps = [0usize, 1, 2, 3, 5];
        let seeds = [1u64, 7, 13, 42, 99, 255];

        for &cap in &caps {
            for &seed in &seeds {
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

                for (step, owner) in owners.iter().enumerate() {
                    let keys: Vec<Pubkey> = key_pool
                        .iter()
                        .copied()
                        .filter(|k| (k.to_bytes()[0] as usize + step + seed as usize) % 3 != 0)
                        .take(2 + (step % 2))
                        .collect();
                    if keys.is_empty() {
                        continue;
                    }

                    let before = admission_index_snapshot(&admission);
                    let result = admission.try_admit_group(owner.clone(), keys.clone());
                    let expected = model.try_admit_group(owner.clone(), keys);
                    if matches!(result, FixedCapAdmissionResult::RejectedCap { .. }) {
                        assert_eq!(admission_index_snapshot(&admission), before);
                    }
                    assert_operation(&admission, &model, &result, &expected);
                }

                let shared_owner = pool_owner(ExplicitConsumer::Momentum, seed as u8);
                for keys in [
                    vec![key_pool[0], key_pool[1], key_pool[2]],
                    vec![key_pool[2], key_pool[1], key_pool[0], key_pool[0]],
                    vec![key_pool[3], key_pool[4]],
                ] {
                    let before = admission_index_snapshot(&admission);
                    let result = admission.try_admit_group(shared_owner.clone(), keys.clone());
                    let expected = model.try_admit_group(shared_owner.clone(), keys);
                    if matches!(result, FixedCapAdmissionResult::RejectedCap { .. }) {
                        assert_eq!(admission_index_snapshot(&admission), before);
                    }
                    assert_operation(&admission, &model, &result, &expected);
                }

                let remove_owner = pool_owner(ExplicitConsumer::Arb, seed.wrapping_add(1) as u8);
                let removed = admission.remove_group(&remove_owner);
                let ref_removed = model.remove_group(&remove_owner);
                assert_eq!(removed, ref_removed);
                assert_physical_len_invariant(&admission);
                assert_indexes_match(&admission, &model);

                assert!(admission.len() <= cap);
            }
        }
    }

    impl FixedCapAdmission {
        fn contains_pubkey(&self, pubkey: &Pubkey) -> bool {
            self.ownership.contains(pubkey)
        }
    }
}
