//! Insert-only fixed-cap admission over [`ExplicitOwnership`] (PR 1b1).
//!
//! Accepts only previously absent owners. No replacement, removal API, eviction,
//! cap mutation, or runtime wiring.
//!
//! Normal planning is O(new_group_size). [`FixedCapAdmission::reconcile_physical_len_cold`]
//! may scan ownership only on exceptional invariant/rollback failure.

use super::explicit_ownership::{
    EmptyOwnerGroupError, ExplicitOwner, ExplicitOwnership, GroupChange, OwnerGroupSnapshot,
};
use solana_sdk::pubkey::Pubkey;

/// Result of an insert-only admission attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FixedCapAdmissionResult {
    /// First insert for this owner; pubkeys that transition refcount 0→1.
    Inserted { physical_added: Vec<Pubkey> },
    /// New owner group whose pubkeys were all already physically tracked.
    OwnerAddedNoNewPubkey,
    /// Projected physical pubkey count would exceed the cap; no mutation.
    RejectedCap {
        required_unique: usize,
        available_unique: usize,
    },
    /// Empty owner groups are invalid and do not mutate state.
    RejectedInvalidGroup,
    /// Owner already present; fully mutation-free even for an identical group.
    RejectedExistingOwner,
    /// Plan/commit or rollback invariant failure (cold recovery may scan ownership).
    InternalInvariantViolation,
}

/// Test-only counters for instrumented planning lookups.
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

/// Insert-only fixed-cap admission layer over explicit ownership.
#[derive(Debug, Clone)]
pub struct FixedCapAdmission {
    cap: usize,
    physical_len: usize,
    ownership: ExplicitOwnership,
    #[cfg(test)]
    planning_stats: PlanningStats,
    #[cfg(test)]
    test_force_commit_plan_mismatch: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InsertPlan {
    pre_len: usize,
    projected_final_len: usize,
    physical_added: Vec<Pubkey>,
}

enum InsertPlanOutcome {
    Ready(InsertPlan),
    RejectCap {
        required_unique: usize,
        available_unique: usize,
    },
    InternalInvariantViolation,
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

    /// Admit a new owner group when the owner is absent and projected physical pubkeys fit the cap.
    pub fn try_admit_new_group(
        &mut self,
        owner: ExplicitOwner,
        pubkeys: impl IntoIterator<Item = Pubkey>,
    ) -> FixedCapAdmissionResult {
        let normalized = normalize_pubkeys(pubkeys);
        if normalized.is_empty() {
            return FixedCapAdmissionResult::RejectedInvalidGroup;
        }

        #[cfg(test)]
        self.planning_stats.record_owner_group_lookup();
        if self.ownership.owner_group(&owner).is_some() {
            return FixedCapAdmissionResult::RejectedExistingOwner;
        }

        let plan_outcome = self.plan_insert(&normalized);
        let plan = match plan_outcome {
            InsertPlanOutcome::RejectCap {
                required_unique,
                available_unique,
            } => {
                return FixedCapAdmissionResult::RejectedCap {
                    required_unique,
                    available_unique,
                };
            }
            InsertPlanOutcome::InternalInvariantViolation => {
                return FixedCapAdmissionResult::InternalInvariantViolation;
            }
            InsertPlanOutcome::Ready(plan) => plan,
        };

        let force_mismatch = {
            #[cfg(test)]
            {
                self.test_force_commit_plan_mismatch
            }
            #[cfg(not(test))]
            {
                false
            }
        };
        let expected_physical_added = expected_physical_added_at_commit(&plan, force_mismatch);

        let change = match self
            .ownership
            .upsert_group(owner.clone(), normalized.clone())
        {
            Ok(change) => change,
            Err(EmptyOwnerGroupError) => {
                return FixedCapAdmissionResult::RejectedInvalidGroup;
            }
        };

        if !new_group_matches_plan(&change, &expected_physical_added) {
            return self.rollback_failed_insert(&owner, plan.pre_len, &normalized);
        }

        self.physical_len = plan.projected_final_len;
        if plan.physical_added.is_empty() {
            FixedCapAdmissionResult::OwnerAddedNoNewPubkey
        } else {
            FixedCapAdmissionResult::Inserted {
                physical_added: plan.physical_added,
            }
        }
    }

    fn plan_insert(&mut self, normalized: &[Pubkey]) -> InsertPlanOutcome {
        let pre_len = self.physical_len;
        let mut physical_added = Vec::new();
        for pubkey in normalized {
            #[cfg(test)]
            self.planning_stats.record_refcount_lookup();
            if self.ownership.owner_refcount(pubkey) == 0 {
                physical_added.push(*pubkey);
            }
        }

        let required_unique = physical_added.len();
        let available_unique = self.cap.saturating_sub(pre_len);
        if required_unique > available_unique {
            return InsertPlanOutcome::RejectCap {
                required_unique,
                available_unique,
            };
        }

        let Some(projected_final_len) = pre_len.checked_add(required_unique) else {
            return InsertPlanOutcome::InternalInvariantViolation;
        };
        if projected_final_len > self.cap {
            return InsertPlanOutcome::InternalInvariantViolation;
        }

        InsertPlanOutcome::Ready(InsertPlan {
            pre_len,
            projected_final_len,
            physical_added,
        })
    }

    fn rollback_failed_insert(
        &mut self,
        owner: &ExplicitOwner,
        pre_len: usize,
        normalized: &[Pubkey],
    ) -> FixedCapAdmissionResult {
        let removed = self.ownership.remove_group(owner);
        match removed {
            Some(snapshot) if snapshot.pubkeys == normalized => {
                self.physical_len = pre_len;
                if self.ownership.owner_group(owner).is_some() {
                    self.reconcile_physical_len_cold();
                }
                FixedCapAdmissionResult::InternalInvariantViolation
            }
            _ => {
                self.reconcile_physical_len_cold();
                FixedCapAdmissionResult::InternalInvariantViolation
            }
        }
    }

    /// Cold fail-closed recovery: full scan of ownership to repair cached `physical_len`.
    fn reconcile_physical_len_cold(&mut self) {
        self.physical_len = self.ownership.len();
    }
}

fn expected_physical_added_at_commit(plan: &InsertPlan, force_mismatch: bool) -> Vec<Pubkey> {
    if force_mismatch && !plan.physical_added.is_empty() {
        let mut expected = plan.physical_added.clone();
        expected.push(expected[0]);
        expected
    } else {
        plan.physical_added.clone()
    }
}

fn new_group_matches_plan(change: &GroupChange, expected_physical_added: &[Pubkey]) -> bool {
    match change {
        GroupChange::NewGroup { physical_added } => physical_added == expected_physical_added,
        GroupChange::Unchanged | GroupChange::Replaced { .. } => false,
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

    fn admission_groups(
        admission: &FixedCapAdmission,
    ) -> BTreeMap<ExplicitOwner, BTreeSet<Pubkey>> {
        let mut groups = BTreeMap::new();
        for group in admission.snapshot_owner_groups() {
            let owner = ExplicitOwner {
                consumer: group.consumer,
                owner_key: group.owner_key,
            };
            groups.insert(owner, group.pubkeys.into_iter().collect());
        }
        groups
    }

    fn physical_len_from_groups(groups: &BTreeMap<ExplicitOwner, BTreeSet<Pubkey>>) -> usize {
        let mut set = BTreeSet::new();
        for pubkeys in groups.values() {
            set.extend(pubkeys.iter().copied());
        }
        set.len()
    }

    /// Independent insert-only candidate-state model — never calls [`FixedCapAdmission`].
    #[derive(Debug, Clone)]
    struct RefInsertModel {
        cap: usize,
        groups: BTreeMap<ExplicitOwner, BTreeSet<Pubkey>>,
    }

    impl RefInsertModel {
        fn new(cap: usize) -> Self {
            Self {
                cap,
                groups: BTreeMap::new(),
            }
        }

        fn try_admit_new_group(
            &mut self,
            owner: ExplicitOwner,
            pubkeys: impl IntoIterator<Item = Pubkey>,
        ) -> FixedCapAdmissionResult {
            let mut normalized: Vec<Pubkey> = pubkeys.into_iter().collect();
            normalized.sort();
            normalized.dedup();
            if normalized.is_empty() {
                return FixedCapAdmissionResult::RejectedInvalidGroup;
            }
            if self.groups.contains_key(&owner) {
                return FixedCapAdmissionResult::RejectedExistingOwner;
            }

            let physical_before: BTreeSet<Pubkey> = self
                .groups
                .values()
                .flat_map(|set| set.iter().copied())
                .collect();
            let candidate: BTreeSet<Pubkey> = normalized.iter().copied().collect();
            let mut candidate_groups = self.groups.clone();
            candidate_groups.insert(owner.clone(), candidate.clone());
            let physical_after: BTreeSet<Pubkey> = candidate_groups
                .values()
                .flat_map(|set| set.iter().copied())
                .collect();

            let physical_added: Vec<Pubkey> = physical_after
                .difference(&physical_before)
                .copied()
                .collect();

            if physical_after.len() > self.cap {
                return FixedCapAdmissionResult::RejectedCap {
                    required_unique: physical_added.len(),
                    available_unique: self.cap.saturating_sub(physical_before.len()),
                };
            }

            self.groups = candidate_groups;
            if physical_added.is_empty() {
                FixedCapAdmissionResult::OwnerAddedNoNewPubkey
            } else {
                FixedCapAdmissionResult::Inserted { physical_added }
            }
        }
    }

    fn assert_state_matches(admission: &FixedCapAdmission, model: &RefInsertModel) {
        assert_eq!(admission.len(), physical_len_from_groups(&model.groups));
        assert_eq!(admission.len(), admission.snapshot_pubkeys().len());
        assert_eq!(admission_groups(admission), model.groups);
    }

    #[test]
    fn new_group_under_cap_is_inserted() {
        let mut admission = FixedCapAdmission::new(3);
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        let result = admission.try_admit_new_group(owner.clone(), [pk(2), pk(1), pk(1)]);
        assert_eq!(
            result,
            FixedCapAdmissionResult::Inserted {
                physical_added: vec![pk(1), pk(2)],
            }
        );
        assert_eq!(admission.len(), 2);
        assert_eq!(
            admission.owner_group(&owner),
            Some([pk(1), pk(2)].as_slice())
        );
    }

    #[test]
    fn new_group_exactly_at_cap_is_inserted() {
        let mut admission = FixedCapAdmission::new(2);
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        let result = admission.try_admit_new_group(owner, [pk(1), pk(2)]);
        assert_eq!(
            result,
            FixedCapAdmissionResult::Inserted {
                physical_added: vec![pk(1), pk(2)],
            }
        );
        assert_eq!(admission.len(), 2);
    }

    #[test]
    fn new_group_over_cap_is_rejected_without_mutation() {
        let mut admission = FixedCapAdmission::new(2);
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        let before = admission_groups(&admission);
        let result = admission.try_admit_new_group(owner, [pk(1), pk(2), pk(3)]);
        assert_eq!(
            result,
            FixedCapAdmissionResult::RejectedCap {
                required_unique: 3,
                available_unique: 2,
            }
        );
        assert_eq!(admission_groups(&admission), before);
        assert!(admission.is_empty());
    }

    #[test]
    fn shared_only_new_owner_at_full_cap_is_owner_added_no_new_pubkey() {
        let mut admission = FixedCapAdmission::new(1);
        let shared = pk(1);
        let first = pool_owner(ExplicitConsumer::Momentum, 1);
        let second = pool_owner(ExplicitConsumer::Arb, 2);
        assert_eq!(
            admission.try_admit_new_group(first, [shared]),
            FixedCapAdmissionResult::Inserted {
                physical_added: vec![shared],
            }
        );
        assert_eq!(
            admission.try_admit_new_group(second.clone(), [shared]),
            FixedCapAdmissionResult::OwnerAddedNoNewPubkey
        );
        assert_eq!(admission.len(), 1);
        assert_eq!(admission.owner_refcount(&shared), 2);
        assert_eq!(admission.owner_group(&second), Some([shared].as_slice()));
    }

    #[test]
    fn mixed_shared_and_new_keys_at_cap() {
        let mut admission = FixedCapAdmission::new(2);
        let shared = pk(1);
        let first = pool_owner(ExplicitConsumer::Momentum, 1);
        assert_admitted(admission.try_admit_new_group(first, [shared]));
        let second = pool_owner(ExplicitConsumer::Arb, 2);
        assert_eq!(
            admission.try_admit_new_group(second, [shared, pk(2)]),
            FixedCapAdmissionResult::Inserted {
                physical_added: vec![pk(2)],
            }
        );
        assert_eq!(admission.len(), 2);
    }

    #[test]
    fn empty_group_is_rejected_invalid() {
        let mut admission = FixedCapAdmission::new(3);
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        assert_eq!(
            admission.try_admit_new_group(owner, []),
            FixedCapAdmissionResult::RejectedInvalidGroup
        );
    }

    #[test]
    fn existing_owner_is_rejected_without_mutation_even_for_same_group() {
        let mut admission = FixedCapAdmission::new(3);
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        assert_admitted(admission.try_admit_new_group(owner.clone(), [pk(1), pk(2)]));
        let before = admission_groups(&admission);
        assert_eq!(
            admission.try_admit_new_group(owner.clone(), [pk(2), pk(1)]),
            FixedCapAdmissionResult::RejectedExistingOwner
        );
        assert_eq!(admission_groups(&admission), before);
    }

    #[test]
    fn cap_zero_rejects_new_physical_keys() {
        let mut admission = FixedCapAdmission::new(0);
        assert_eq!(
            admission.try_admit_new_group(pool_owner(ExplicitConsumer::Momentum, 1), [pk(1)]),
            FixedCapAdmissionResult::RejectedCap {
                required_unique: 1,
                available_unique: 0,
            }
        );
    }

    #[test]
    fn usize_max_cap_accepts_large_insert() {
        let mut admission = FixedCapAdmission::new(usize::MAX);
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        let keys: Vec<Pubkey> = (0u8..=255).map(pk).collect();
        assert!(matches!(
            admission.try_admit_new_group(owner, keys),
            FixedCapAdmissionResult::Inserted { .. }
        ));
        assert_eq!(admission.len(), 256);
    }

    #[test]
    fn planning_stats_match_exact_refcount_lookups_per_insert() {
        let mut admission = FixedCapAdmission::new(4);
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        let keys = [pk(1), pk(2), pk(3)];

        let _ = admission.try_admit_new_group(owner.clone(), keys);
        assert_eq!(admission.planning_stats().owner_group_lookups, 1);
        assert_eq!(admission.planning_stats().refcount_lookups, 3);

        let other = pool_owner(ExplicitConsumer::Arb, 2);
        let _ = admission.try_admit_new_group(other, [pk(1)]);
        assert_eq!(admission.planning_stats().owner_group_lookups, 2);
        assert_eq!(admission.planning_stats().refcount_lookups, 4);
    }

    #[test]
    fn bounded_reference_insert_model_matches_admission() {
        let caps = [0usize, 1, 2, 3, 5, usize::MAX];
        let seeds = [1u8, 7, 42, 255];

        caps.iter().copied().for_each(|cap| {
            seeds.iter().copied().for_each(|seed| {
                let mut admission = FixedCapAdmission::new(cap);
                let mut model = RefInsertModel::new(cap);
                let owners = [
                    pool_owner(ExplicitConsumer::Momentum, seed),
                    pool_owner(ExplicitConsumer::Arb, seed.wrapping_add(1)),
                    pool_owner(ExplicitConsumer::Tracker, seed.wrapping_add(2)),
                ];
                let key_pool: Vec<Pubkey> = (0u8..6).map(|b| pk(b.wrapping_add(seed))).collect();

                owners.iter().enumerate().for_each(|(step, owner)| {
                    let keys: Vec<Pubkey> = key_pool
                        .iter()
                        .copied()
                        .filter(|k| (k.to_bytes()[0] as usize + step) % 2 == 0)
                        .take(1 + step % 2)
                        .collect();
                    if keys.is_empty() {
                        return;
                    }
                    let before = admission_groups(&admission);
                    let result = admission.try_admit_new_group(owner.clone(), keys.clone());
                    let expected = model.try_admit_new_group(owner.clone(), keys);
                    assert_eq!(result, expected);
                    if matches!(result, FixedCapAdmissionResult::RejectedCap { .. }) {
                        assert_eq!(admission_groups(&admission), before);
                    }
                    assert_state_matches(&admission, &model);
                });

                let dup_owner = owners[0].clone();
                let dup_result = admission.try_admit_new_group(dup_owner.clone(), [key_pool[0]]);
                let dup_expected = model.try_admit_new_group(dup_owner, [key_pool[0]]);
                assert_eq!(dup_result, dup_expected);
                assert_state_matches(&admission, &model);
            });
        });
    }

    #[test]
    fn forced_commit_mismatch_rolls_back_and_preserves_state() {
        let mut admission = FixedCapAdmission::new(4);
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        assert_admitted(admission.try_admit_new_group(owner.clone(), [pk(1)]));
        let before_groups = admission_groups(&admission);
        let before_len = admission.len();

        admission.set_test_force_commit_plan_mismatch(true);
        let result = admission.try_admit_new_group(pool_owner(ExplicitConsumer::Arb, 2), [pk(2)]);
        assert_eq!(result, FixedCapAdmissionResult::InternalInvariantViolation);
        assert_eq!(admission_groups(&admission), before_groups);
        assert_eq!(admission.len(), before_len);
        assert!(admission
            .owner_group(&pool_owner(ExplicitConsumer::Arb, 2))
            .is_none());
    }

    fn assert_admitted(result: FixedCapAdmissionResult) {
        match result {
            FixedCapAdmissionResult::Inserted { .. }
            | FixedCapAdmissionResult::OwnerAddedNoNewPubkey => {}
            other => panic!("expected successful admission, got {other:?}"),
        }
    }
}
