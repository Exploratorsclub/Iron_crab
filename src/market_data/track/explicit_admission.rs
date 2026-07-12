//! Insert, replacement, and owner-removal fixed-cap admission over [`ExplicitOwnership`]
//! (PR 1b1/1b2/1b3).
//!
//! Accepts only previously absent owners via insert. Replacement mutates only existing owners.
//! Removal is group-local and atomic. No eviction, cap mutation, or runtime wiring.
//!
//! Normal planning is O(group_size). [`FixedCapAdmission::reconcile_physical_len_cold`]
//! may scan ownership only on exceptional invariant/rollback failure.

use super::explicit_ownership::{
    EmptyOwnerGroupError, ExplicitOwner, ExplicitOwnership, GroupChange, OwnerGroupSnapshot,
};
use solana_sdk::pubkey::Pubkey;

/// Recovery path after an internal invariant violation during replacement rollback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvariantViolationRecovery {
    /// Commit mismatch: previous owner group was atomically restored.
    PreviousRestored,
    /// Restore validation failed: owner was fail-closed removed and `physical_len` reconciled.
    OwnerRemovedFailClosed,
}

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

/// Result of a fixed-cap owner-group removal attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FixedCapRemoveResult {
    /// Owner removed; physical deltas from refcount 1→0 transitions.
    Removed { physical_removed: Vec<Pubkey> },
    /// Owner absent; fully mutation-free.
    NotFound,
    /// Post-commit plan/rollback mismatch with a documented recovery path.
    InternalInvariantViolation {
        recovery: InvariantViolationRecovery,
    },
}

/// Result of a fixed-cap owner-group replacement attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FixedCapReplaceResult {
    /// Atomic replacement; physical deltas from refcount 0→1 / 1→0 transitions.
    Replaced {
        physical_added: Vec<Pubkey>,
        physical_removed: Vec<Pubkey>,
    },
    /// Idempotent replacement with an identical normalized group.
    Unchanged,
    /// Projected physical pubkey count would exceed the cap; no mutation.
    RejectedCap {
        required_unique: usize,
        available_unique: usize,
    },
    /// Empty owner groups are invalid and do not mutate state.
    RejectedInvalidGroup,
    /// Owner absent on replacement; no insert fallback.
    RejectedMissingOwner,
    /// Pre-commit planning invariant failure; no mutation.
    PlanningInvariantViolation,
    /// Post-commit plan/rollback mismatch with a documented recovery path.
    InternalInvariantViolation {
        recovery: InvariantViolationRecovery,
    },
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
    #[cfg(test)]
    test_force_restore_plan_mismatch: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InsertPlan {
    pre_len: usize,
    projected_final_len: usize,
    physical_added: Vec<Pubkey>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReplacePlan {
    pre_len: usize,
    projected_final_len: usize,
    old_normalized: Vec<Pubkey>,
    physical_added: Vec<Pubkey>,
    physical_removed: Vec<Pubkey>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RemovePlan {
    pre_len: usize,
    projected_final_len: usize,
    old_normalized: Vec<Pubkey>,
    physical_removed: Vec<Pubkey>,
}

enum InsertPlanOutcome {
    Ready(InsertPlan),
    RejectCap {
        required_unique: usize,
        available_unique: usize,
    },
    InternalInvariantViolation,
}

enum ReplacePlanOutcome {
    Ready(ReplacePlan),
    Unchanged,
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
            #[cfg(test)]
            test_force_restore_plan_mismatch: false,
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
    pub fn set_test_force_restore_plan_mismatch(&mut self, enabled: bool) {
        self.test_force_restore_plan_mismatch = enabled;
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

    /// Replace an existing owner group atomically when projected physical pubkeys fit the cap.
    pub fn try_replace_group(
        &mut self,
        existing_owner: ExplicitOwner,
        replacement_pubkeys: impl IntoIterator<Item = Pubkey>,
    ) -> FixedCapReplaceResult {
        let normalized = normalize_pubkeys(replacement_pubkeys);
        if normalized.is_empty() {
            return FixedCapReplaceResult::RejectedInvalidGroup;
        }

        #[cfg(test)]
        self.planning_stats.record_owner_group_lookup();
        let Some(old_group) = self.ownership.owner_group(&existing_owner) else {
            return FixedCapReplaceResult::RejectedMissingOwner;
        };
        let old_normalized = old_group.to_vec();
        if old_normalized == normalized {
            return FixedCapReplaceResult::Unchanged;
        }

        let plan_outcome = self.plan_replace(&old_normalized, &normalized);
        let plan = match plan_outcome {
            ReplacePlanOutcome::Unchanged => return FixedCapReplaceResult::Unchanged,
            ReplacePlanOutcome::RejectCap {
                required_unique,
                available_unique,
            } => {
                return FixedCapReplaceResult::RejectedCap {
                    required_unique,
                    available_unique,
                };
            }
            ReplacePlanOutcome::InternalInvariantViolation => {
                return FixedCapReplaceResult::PlanningInvariantViolation;
            }
            ReplacePlanOutcome::Ready(plan) => plan,
        };

        let force_commit_mismatch = {
            #[cfg(test)]
            {
                self.test_force_commit_plan_mismatch
            }
            #[cfg(not(test))]
            {
                false
            }
        };
        let expected_physical_added =
            expected_physical_added_at_replace_commit(&plan, force_commit_mismatch);
        let expected_physical_removed =
            expected_physical_removed_at_replace_commit(&plan, force_commit_mismatch);

        let change = match self
            .ownership
            .upsert_group(existing_owner.clone(), normalized.clone())
        {
            Ok(change) => change,
            Err(EmptyOwnerGroupError) => {
                return FixedCapReplaceResult::RejectedInvalidGroup;
            }
        };

        if !replaced_group_matches_plan(
            &change,
            &expected_physical_added,
            &expected_physical_removed,
        ) {
            return self.rollback_failed_replace(&existing_owner, &plan);
        }

        self.physical_len = plan.projected_final_len;
        FixedCapReplaceResult::Replaced {
            physical_added: plan.physical_added,
            physical_removed: plan.physical_removed,
        }
    }

    /// Remove an existing owner group atomically when projected physical pubkeys stay within the cap.
    pub fn remove_group(&mut self, owner: ExplicitOwner) -> FixedCapRemoveResult {
        #[cfg(test)]
        self.planning_stats.record_owner_group_lookup();
        let Some(old_group) = self.ownership.owner_group(&owner) else {
            return FixedCapRemoveResult::NotFound;
        };
        let old_normalized = old_group.to_vec();

        let plan = self.plan_remove(&old_normalized);

        let force_commit_mismatch = {
            #[cfg(test)]
            {
                self.test_force_commit_plan_mismatch
            }
            #[cfg(not(test))]
            {
                false
            }
        };
        let expected_physical_removed =
            expected_physical_removed_at_remove_commit(&plan, force_commit_mismatch);

        let snapshot = self.ownership.remove_group(&owner);

        if !removed_group_matches_plan(
            &snapshot,
            &owner,
            &plan.old_normalized,
            &expected_physical_removed,
            &self.ownership,
        ) {
            return self.rollback_failed_remove(&owner, &plan);
        }

        self.physical_len = plan.projected_final_len;
        FixedCapRemoveResult::Removed {
            physical_removed: plan.physical_removed,
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

    fn plan_replace(
        &mut self,
        old_normalized: &[Pubkey],
        new_normalized: &[Pubkey],
    ) -> ReplacePlanOutcome {
        if old_normalized == new_normalized {
            return ReplacePlanOutcome::Unchanged;
        }

        let pre_len = self.physical_len;
        let mut physical_removed = Vec::new();
        let mut physical_added = Vec::new();

        let mut old_idx = 0usize;
        let mut new_idx = 0usize;
        while old_idx < old_normalized.len() || new_idx < new_normalized.len() {
            let old_key = old_normalized.get(old_idx).copied();
            let new_key = new_normalized.get(new_idx).copied();
            match (old_key, new_key) {
                (None, None) => break,
                (Some(old), None) => {
                    #[cfg(test)]
                    self.planning_stats.record_refcount_lookup();
                    if self.ownership.owner_refcount(&old) == 1 {
                        physical_removed.push(old);
                    }
                    old_idx += 1;
                }
                (None, Some(new)) => {
                    #[cfg(test)]
                    self.planning_stats.record_refcount_lookup();
                    if self.ownership.owner_refcount(&new) == 0 {
                        physical_added.push(new);
                    }
                    new_idx += 1;
                }
                (Some(old), Some(new)) => {
                    if old == new {
                        old_idx += 1;
                        new_idx += 1;
                    } else if old < new {
                        #[cfg(test)]
                        self.planning_stats.record_refcount_lookup();
                        if self.ownership.owner_refcount(&old) == 1 {
                            physical_removed.push(old);
                        }
                        old_idx += 1;
                    } else {
                        #[cfg(test)]
                        self.planning_stats.record_refcount_lookup();
                        if self.ownership.owner_refcount(&new) == 0 {
                            physical_added.push(new);
                        }
                        new_idx += 1;
                    }
                }
            }
        }

        let required_unique = physical_added.len();
        let available_unique = self
            .cap
            .saturating_sub(pre_len)
            .saturating_add(physical_removed.len());
        if required_unique > available_unique {
            return ReplacePlanOutcome::RejectCap {
                required_unique,
                available_unique,
            };
        }

        let Some(after_remove) = pre_len.checked_sub(physical_removed.len()) else {
            return ReplacePlanOutcome::InternalInvariantViolation;
        };
        let Some(projected_final_len) = after_remove.checked_add(physical_added.len()) else {
            return ReplacePlanOutcome::InternalInvariantViolation;
        };
        if projected_final_len > self.cap {
            return ReplacePlanOutcome::InternalInvariantViolation;
        }

        ReplacePlanOutcome::Ready(ReplacePlan {
            pre_len,
            projected_final_len,
            old_normalized: old_normalized.to_vec(),
            physical_added,
            physical_removed,
        })
    }

    fn plan_remove(&mut self, old_normalized: &[Pubkey]) -> RemovePlan {
        let pre_len = self.physical_len;
        let mut physical_removed = Vec::new();
        for pubkey in old_normalized {
            #[cfg(test)]
            self.planning_stats.record_refcount_lookup();
            if self.ownership.owner_refcount(pubkey) == 1 {
                physical_removed.push(*pubkey);
            }
        }

        let Some(projected_final_len) = pre_len.checked_sub(physical_removed.len()) else {
            return RemovePlan {
                pre_len,
                projected_final_len: pre_len,
                old_normalized: old_normalized.to_vec(),
                physical_removed,
            };
        };

        RemovePlan {
            pre_len,
            projected_final_len,
            old_normalized: old_normalized.to_vec(),
            physical_removed,
        }
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

    fn rollback_failed_replace(
        &mut self,
        owner: &ExplicitOwner,
        plan: &ReplacePlan,
    ) -> FixedCapReplaceResult {
        let force_restore_mismatch = {
            #[cfg(test)]
            {
                self.test_force_restore_plan_mismatch
            }
            #[cfg(not(test))]
            {
                false
            }
        };
        let restore_pubkeys = if force_restore_mismatch && !plan.old_normalized.is_empty() {
            let mut corrupted = plan.old_normalized.clone();
            corrupted.push(corrupted[0]);
            corrupted
        } else {
            plan.old_normalized.clone()
        };

        let restore_change = match self
            .ownership
            .upsert_group(owner.clone(), restore_pubkeys.clone())
        {
            Ok(change) => change,
            Err(EmptyOwnerGroupError) => {
                return self.fail_closed_remove_owner_after_replace(owner);
            }
        };

        let restored = match restore_change {
            GroupChange::Replaced { .. } | GroupChange::Unchanged => {
                self.owner_group(owner) == Some(restore_pubkeys.as_slice())
            }
            GroupChange::NewGroup { .. } => false,
        };

        if restored {
            self.physical_len = plan.pre_len;
            if self.ownership.owner_group(owner).is_none() {
                self.reconcile_physical_len_cold();
            }
            FixedCapReplaceResult::InternalInvariantViolation {
                recovery: InvariantViolationRecovery::PreviousRestored,
            }
        } else {
            self.fail_closed_remove_owner_after_replace(owner)
        }
    }

    fn fail_closed_remove_owner_after_replace(
        &mut self,
        owner: &ExplicitOwner,
    ) -> FixedCapReplaceResult {
        self.fail_closed_remove_owner(owner);
        FixedCapReplaceResult::InternalInvariantViolation {
            recovery: InvariantViolationRecovery::OwnerRemovedFailClosed,
        }
    }

    fn rollback_failed_remove(
        &mut self,
        owner: &ExplicitOwner,
        plan: &RemovePlan,
    ) -> FixedCapRemoveResult {
        let force_restore_mismatch = {
            #[cfg(test)]
            {
                self.test_force_restore_plan_mismatch
            }
            #[cfg(not(test))]
            {
                false
            }
        };
        let restore_pubkeys = if force_restore_mismatch && !plan.old_normalized.is_empty() {
            let mut corrupted = plan.old_normalized.clone();
            corrupted.push(corrupted[0]);
            corrupted
        } else {
            plan.old_normalized.clone()
        };

        let restore_change = match self
            .ownership
            .upsert_group(owner.clone(), restore_pubkeys.clone())
        {
            Ok(change) => change,
            Err(EmptyOwnerGroupError) => {
                return self.fail_closed_remove_owner_after_remove(owner);
            }
        };

        let restored = match restore_change {
            GroupChange::NewGroup { .. } => {
                self.owner_group(owner) == Some(restore_pubkeys.as_slice())
            }
            GroupChange::Replaced { .. } | GroupChange::Unchanged => false,
        };

        if restored {
            self.physical_len = plan.pre_len;
            if self.ownership.owner_group(owner).is_none() {
                self.reconcile_physical_len_cold();
            }
            FixedCapRemoveResult::InternalInvariantViolation {
                recovery: InvariantViolationRecovery::PreviousRestored,
            }
        } else {
            self.fail_closed_remove_owner_after_remove(owner)
        }
    }

    fn fail_closed_remove_owner_after_remove(
        &mut self,
        owner: &ExplicitOwner,
    ) -> FixedCapRemoveResult {
        self.fail_closed_remove_owner(owner);
        FixedCapRemoveResult::InternalInvariantViolation {
            recovery: InvariantViolationRecovery::OwnerRemovedFailClosed,
        }
    }

    fn fail_closed_remove_owner(&mut self, owner: &ExplicitOwner) {
        let _ = self.ownership.remove_group(owner);
        const MAX_REMOVE_ATTEMPTS: usize = 8;
        for _ in 0..MAX_REMOVE_ATTEMPTS {
            if self.ownership.owner_group(owner).is_none() {
                break;
            }
            if self.ownership.remove_group(owner).is_none() {
                break;
            }
        }
        self.reconcile_physical_len_cold();
        debug_assert!(self.ownership.owner_group(owner).is_none());
        debug_assert!(self.physical_len <= self.cap);
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

fn expected_physical_added_at_replace_commit(
    plan: &ReplacePlan,
    force_mismatch: bool,
) -> Vec<Pubkey> {
    if force_mismatch && !plan.physical_added.is_empty() {
        let mut expected = plan.physical_added.clone();
        expected.push(expected[0]);
        expected
    } else {
        plan.physical_added.clone()
    }
}

fn expected_physical_removed_at_replace_commit(
    plan: &ReplacePlan,
    force_mismatch: bool,
) -> Vec<Pubkey> {
    if force_mismatch && !plan.physical_removed.is_empty() {
        let mut expected = plan.physical_removed.clone();
        expected.push(expected[0]);
        expected
    } else {
        plan.physical_removed.clone()
    }
}

fn expected_physical_removed_at_remove_commit(
    plan: &RemovePlan,
    force_mismatch: bool,
) -> Vec<Pubkey> {
    if force_mismatch && !plan.physical_removed.is_empty() {
        let mut expected = plan.physical_removed.clone();
        expected.push(expected[0]);
        expected
    } else {
        plan.physical_removed.clone()
    }
}

fn new_group_matches_plan(change: &GroupChange, expected_physical_added: &[Pubkey]) -> bool {
    match change {
        GroupChange::NewGroup { physical_added } => physical_added == expected_physical_added,
        GroupChange::Unchanged | GroupChange::Replaced { .. } => false,
    }
}

fn replaced_group_matches_plan(
    change: &GroupChange,
    expected_physical_added: &[Pubkey],
    expected_physical_removed: &[Pubkey],
) -> bool {
    match change {
        GroupChange::Replaced {
            physical_added,
            physical_removed,
        } => {
            physical_added == expected_physical_added
                && physical_removed == expected_physical_removed
        }
        GroupChange::NewGroup { .. } | GroupChange::Unchanged => false,
    }
}

fn removed_group_matches_plan(
    snapshot: &Option<OwnerGroupSnapshot>,
    owner: &ExplicitOwner,
    expected_old_normalized: &[Pubkey],
    expected_physical_removed: &[Pubkey],
    ownership: &ExplicitOwnership,
) -> bool {
    let Some(snapshot) = snapshot else {
        return false;
    };
    if snapshot.consumer != owner.consumer || snapshot.owner_key != owner.owner_key {
        return false;
    }
    if snapshot.pubkeys != expected_old_normalized {
        return false;
    }
    if ownership.owner_group(owner).is_some() {
        return false;
    }

    for pubkey in expected_old_normalized {
        let refcount = ownership.owner_refcount(pubkey);
        let should_be_removed = expected_physical_removed.binary_search(pubkey).is_ok();
        if should_be_removed {
            if refcount != 0 {
                return false;
            }
        } else if refcount == 0 {
            return false;
        }
    }
    true
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

    /// Independent replacement candidate-state model — never calls [`FixedCapAdmission`].
    #[derive(Debug, Clone)]
    struct RefReplaceModel {
        cap: usize,
        groups: BTreeMap<ExplicitOwner, BTreeSet<Pubkey>>,
    }

    impl RefReplaceModel {
        fn new(cap: usize) -> Self {
            Self {
                cap,
                groups: BTreeMap::new(),
            }
        }

        fn admit_for_setup(
            &mut self,
            owner: ExplicitOwner,
            pubkeys: impl IntoIterator<Item = Pubkey>,
        ) {
            let mut normalized: Vec<Pubkey> = pubkeys.into_iter().collect();
            normalized.sort();
            normalized.dedup();
            assert!(!normalized.is_empty());
            self.groups.insert(owner, normalized.into_iter().collect());
        }

        fn try_replace_group(
            &mut self,
            owner: ExplicitOwner,
            pubkeys: impl IntoIterator<Item = Pubkey>,
        ) -> FixedCapReplaceResult {
            let mut normalized: Vec<Pubkey> = pubkeys.into_iter().collect();
            normalized.sort();
            normalized.dedup();
            if normalized.is_empty() {
                return FixedCapReplaceResult::RejectedInvalidGroup;
            }
            let Some(old_group) = self.groups.get(&owner).cloned() else {
                return FixedCapReplaceResult::RejectedMissingOwner;
            };
            if old_group == normalized.iter().copied().collect::<BTreeSet<_>>() {
                return FixedCapReplaceResult::Unchanged;
            }

            let physical_before: BTreeSet<Pubkey> = self
                .groups
                .values()
                .flat_map(|set| set.iter().copied())
                .collect();
            let mut candidate_groups = self.groups.clone();
            candidate_groups.insert(owner.clone(), normalized.iter().copied().collect());
            let physical_after: BTreeSet<Pubkey> = candidate_groups
                .values()
                .flat_map(|set| set.iter().copied())
                .collect();

            let physical_added: Vec<Pubkey> = physical_after
                .difference(&physical_before)
                .copied()
                .collect();
            let physical_removed: Vec<Pubkey> = physical_before
                .difference(&physical_after)
                .copied()
                .collect();

            if physical_after.len() > self.cap {
                let available_unique = self
                    .cap
                    .saturating_sub(physical_before.len())
                    .saturating_add(physical_removed.len());
                return FixedCapReplaceResult::RejectedCap {
                    required_unique: physical_added.len(),
                    available_unique,
                };
            }

            self.groups = candidate_groups;
            FixedCapReplaceResult::Replaced {
                physical_added,
                physical_removed,
            }
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct AdmissionPublicSnapshot {
        pubkeys: Vec<Pubkey>,
        owner_groups: BTreeMap<ExplicitOwner, BTreeSet<Pubkey>>,
        pubkey_refcounts: BTreeMap<Pubkey, usize>,
    }

    fn pubkey_refcounts_from_groups(
        groups: &BTreeMap<ExplicitOwner, BTreeSet<Pubkey>>,
    ) -> BTreeMap<Pubkey, usize> {
        let mut counts: BTreeMap<Pubkey, usize> = BTreeMap::new();
        for (owner, pubkeys) in groups {
            for pubkey in pubkeys {
                *counts.entry(*pubkey).or_default() += 1;
                let _ = owner;
            }
        }
        counts
    }

    fn capture_admission_snapshot(admission: &FixedCapAdmission) -> AdmissionPublicSnapshot {
        let owner_groups = admission_groups(admission);
        let pubkeys = admission.snapshot_pubkeys();
        let expected_physical: BTreeSet<Pubkey> = owner_groups
            .values()
            .flat_map(|set| set.iter().copied())
            .collect();
        assert_eq!(pubkeys, expected_physical.into_iter().collect::<Vec<_>>());

        let pubkey_refcounts: BTreeMap<Pubkey, usize> = pubkeys
            .iter()
            .map(|pubkey| {
                let refcount = admission.owner_refcount(pubkey);
                assert!(refcount > 0, "stale zero refcount for {pubkey:?}");
                (*pubkey, refcount)
            })
            .collect();
        assert_eq!(
            pubkey_refcounts,
            pubkey_refcounts_from_groups(&owner_groups)
        );

        AdmissionPublicSnapshot {
            pubkeys,
            owner_groups,
            pubkey_refcounts,
        }
    }

    fn capture_model_snapshot(model: &RefReplaceModel) -> AdmissionPublicSnapshot {
        capture_groups_snapshot(&model.groups)
    }

    fn capture_remove_model_snapshot(model: &RefRemoveModel) -> AdmissionPublicSnapshot {
        capture_groups_snapshot(&model.groups)
    }

    fn capture_groups_snapshot(
        groups: &BTreeMap<ExplicitOwner, BTreeSet<Pubkey>>,
    ) -> AdmissionPublicSnapshot {
        let owner_groups = groups.clone();
        let mut pubkeys: Vec<Pubkey> = owner_groups
            .values()
            .flat_map(|set| set.iter().copied())
            .collect();
        pubkeys.sort();
        pubkeys.dedup();
        AdmissionPublicSnapshot {
            pubkey_refcounts: pubkey_refcounts_from_groups(&owner_groups),
            owner_groups,
            pubkeys,
        }
    }

    fn assert_public_snapshots_equal(
        actual: &AdmissionPublicSnapshot,
        expected: &AdmissionPublicSnapshot,
    ) {
        assert_eq!(actual.pubkeys, expected.pubkeys);
        assert_eq!(actual.owner_groups, expected.owner_groups);
        assert_eq!(actual.pubkey_refcounts, expected.pubkey_refcounts);
    }

    fn assert_no_extra_reverse_index_keys(admission: &FixedCapAdmission) {
        let snapshot = capture_admission_snapshot(admission);
        for pubkey in &snapshot.pubkeys {
            assert!(admission.owner_refcount(pubkey) > 0);
        }
    }

    fn assert_state_matches(admission: &FixedCapAdmission, model: &RefInsertModel) {
        let actual = capture_admission_snapshot(admission);
        let expected_pubkeys: Vec<Pubkey> = model
            .groups
            .values()
            .flat_map(|set| set.iter().copied())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let expected = AdmissionPublicSnapshot {
            owner_groups: model.groups.clone(),
            pubkeys: expected_pubkeys,
            pubkey_refcounts: pubkey_refcounts_from_groups(&model.groups),
        };
        assert_public_snapshots_equal(&actual, &expected);
        assert_eq!(admission.len(), actual.pubkeys.len());
        assert_no_extra_reverse_index_keys(admission);
    }

    fn assert_replace_state_matches(admission: &FixedCapAdmission, model: &RefReplaceModel) {
        let actual = capture_admission_snapshot(admission);
        let expected = capture_model_snapshot(model);
        assert_public_snapshots_equal(&actual, &expected);
        assert_eq!(admission.len(), actual.pubkeys.len());
        assert!(admission.len() <= admission.cap());
        assert_no_extra_reverse_index_keys(admission);
    }

    /// Independent removal candidate-state model — never calls [`FixedCapAdmission`].
    #[derive(Debug, Clone)]
    struct RefRemoveModel {
        cap: usize,
        groups: BTreeMap<ExplicitOwner, BTreeSet<Pubkey>>,
    }

    impl RefRemoveModel {
        fn new(cap: usize) -> Self {
            Self {
                cap,
                groups: BTreeMap::new(),
            }
        }

        fn admit_for_setup(
            &mut self,
            owner: ExplicitOwner,
            pubkeys: impl IntoIterator<Item = Pubkey>,
        ) {
            let mut normalized: Vec<Pubkey> = pubkeys.into_iter().collect();
            normalized.sort();
            normalized.dedup();
            assert!(!normalized.is_empty());
            self.groups.insert(owner, normalized.into_iter().collect());
        }

        fn remove_group(&mut self, owner: ExplicitOwner) -> FixedCapRemoveResult {
            if !self.groups.contains_key(&owner) {
                return FixedCapRemoveResult::NotFound;
            }

            let physical_before: BTreeSet<Pubkey> = self
                .groups
                .values()
                .flat_map(|set| set.iter().copied())
                .collect();
            let mut candidate_groups = self.groups.clone();
            candidate_groups.remove(&owner);
            let physical_after: BTreeSet<Pubkey> = candidate_groups
                .values()
                .flat_map(|set| set.iter().copied())
                .collect();

            let physical_removed: Vec<Pubkey> = physical_before
                .difference(&physical_after)
                .copied()
                .collect();

            debug_assert!(physical_after.len() <= self.cap);

            self.groups = candidate_groups;
            FixedCapRemoveResult::Removed { physical_removed }
        }
    }

    fn assert_remove_state_matches(admission: &FixedCapAdmission, model: &RefRemoveModel) {
        let actual = capture_admission_snapshot(admission);
        let expected = capture_remove_model_snapshot(model);
        assert_public_snapshots_equal(&actual, &expected);
        assert_eq!(admission.len(), actual.pubkeys.len());
        assert!(admission.len() <= admission.cap());
        assert_no_extra_reverse_index_keys(admission);
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

    // --- Replacement tests (PR 1b2) ---

    #[test]
    fn replace_k1_k2_to_k2_k3_at_cap_reports_exact_deltas() {
        let mut admission = FixedCapAdmission::new(2);
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        let k1 = pk(1);
        let k2 = pk(2);
        let k3 = pk(3);
        assert_admitted(admission.try_admit_new_group(owner.clone(), [k1, k2]));
        assert_eq!(admission.len(), 2);

        let result = admission.try_replace_group(owner.clone(), [k2, k3]);
        assert_eq!(
            result,
            FixedCapReplaceResult::Replaced {
                physical_added: vec![k3],
                physical_removed: vec![k1],
            }
        );
        assert_eq!(admission.len(), 2);
        assert_eq!(admission.owner_group(&owner), Some([k2, k3].as_slice()));
    }

    #[test]
    fn replace_does_not_remove_shared_outgoing_key() {
        let mut admission = FixedCapAdmission::new(4);
        let shared = pk(1);
        let owner_a = pool_owner(ExplicitConsumer::Momentum, 1);
        let owner_b = pool_owner(ExplicitConsumer::Arb, 2);
        assert_admitted(admission.try_admit_new_group(owner_a.clone(), [shared, pk(2)]));
        assert_admitted(admission.try_admit_new_group(owner_b.clone(), [shared]));
        let before_len = admission.len();

        let result = admission.try_replace_group(owner_a.clone(), [pk(3)]);
        assert_eq!(
            result,
            FixedCapReplaceResult::Replaced {
                physical_added: vec![pk(3)],
                physical_removed: vec![pk(2)],
            }
        );
        assert!(admission.owner_refcount(&shared) > 0);
        assert_eq!(admission.len(), before_len);
    }

    #[test]
    fn replace_does_not_add_shared_incoming_key() {
        let mut admission = FixedCapAdmission::new(4);
        let shared = pk(1);
        let owner_a = pool_owner(ExplicitConsumer::Momentum, 1);
        let owner_b = pool_owner(ExplicitConsumer::Arb, 2);
        assert_admitted(admission.try_admit_new_group(owner_a.clone(), [pk(2)]));
        assert_admitted(admission.try_admit_new_group(owner_b.clone(), [shared]));
        let before_len = admission.len();

        let result = admission.try_replace_group(owner_a.clone(), [shared, pk(3)]);
        assert_eq!(
            result,
            FixedCapReplaceResult::Replaced {
                physical_added: vec![pk(3)],
                physical_removed: vec![pk(2)],
            }
        );
        assert_eq!(admission.len(), before_len);
        assert_eq!(admission.owner_refcount(&shared), 2);
    }

    #[test]
    fn replace_grow_under_exact_and_over_cap() {
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        let k1 = pk(1);
        let k2 = pk(2);
        let k3 = pk(3);

        let mut under = FixedCapAdmission::new(3);
        assert_admitted(under.try_admit_new_group(owner.clone(), [k1]));
        assert_eq!(
            under.try_replace_group(owner.clone(), [k1, k2]),
            FixedCapReplaceResult::Replaced {
                physical_added: vec![k2],
                physical_removed: vec![],
            }
        );

        let mut exact = FixedCapAdmission::new(2);
        assert_admitted(exact.try_admit_new_group(owner.clone(), [k1]));
        assert_eq!(
            exact.try_replace_group(owner.clone(), [k1, k2]),
            FixedCapReplaceResult::Replaced {
                physical_added: vec![k2],
                physical_removed: vec![],
            }
        );
        assert_eq!(exact.len(), 2);

        let mut over = FixedCapAdmission::new(2);
        assert_admitted(over.try_admit_new_group(owner.clone(), [k1, k2]));
        let before = admission_groups(&over);
        assert_eq!(
            over.try_replace_group(owner.clone(), [k1, k2, k3]),
            FixedCapReplaceResult::RejectedCap {
                required_unique: 1,
                available_unique: 0,
            }
        );
        assert_eq!(admission_groups(&over), before);
    }

    #[test]
    fn replace_shrink_and_fully_shared_replacement() {
        let mut admission = FixedCapAdmission::new(4);
        let shared = pk(1);
        let owner_a = pool_owner(ExplicitConsumer::Momentum, 1);
        let owner_b = pool_owner(ExplicitConsumer::Arb, 2);
        assert_admitted(admission.try_admit_new_group(owner_a.clone(), [shared, pk(2), pk(3)]));
        assert_admitted(admission.try_admit_new_group(owner_b.clone(), [shared]));
        let before_len = admission.len();

        assert_eq!(
            admission.try_replace_group(owner_a.clone(), [shared]),
            FixedCapReplaceResult::Replaced {
                physical_added: vec![],
                physical_removed: vec![pk(2), pk(3)],
            }
        );
        assert_eq!(admission.len(), before_len - 2);
        assert_eq!(admission.owner_refcount(&shared), 2);

        let mut admission2 = FixedCapAdmission::new(2);
        assert_admitted(admission2.try_admit_new_group(owner_a.clone(), [shared, pk(2)]));
        assert_admitted(admission2.try_admit_new_group(owner_b.clone(), [shared]));
        assert_eq!(
            admission2.try_replace_group(owner_a.clone(), [shared]),
            FixedCapReplaceResult::Replaced {
                physical_added: vec![],
                physical_removed: vec![pk(2)],
            }
        );
        assert_eq!(admission2.len(), 1);
    }

    #[test]
    fn replace_identical_group_with_reordered_duplicates_is_unchanged() {
        let mut admission = FixedCapAdmission::new(3);
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        assert_admitted(admission.try_admit_new_group(owner.clone(), [pk(1), pk(2), pk(3)]));
        let before = admission_groups(&admission);
        let before_len = admission.len();

        assert_eq!(
            admission.try_replace_group(owner.clone(), [pk(3), pk(1), pk(2), pk(2)]),
            FixedCapReplaceResult::Unchanged
        );
        assert_eq!(admission_groups(&admission), before);
        assert_eq!(admission.len(), before_len);
    }

    #[test]
    fn replace_missing_invalid_and_cap_reject_are_mutation_free() {
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        let unknown = pool_owner(ExplicitConsumer::Arb, 9);

        let mut missing = FixedCapAdmission::new(3);
        let before = admission_groups(&missing);
        assert_eq!(
            missing.try_replace_group(unknown.clone(), [pk(1)]),
            FixedCapReplaceResult::RejectedMissingOwner
        );
        assert_eq!(admission_groups(&missing), before);

        let mut invalid = FixedCapAdmission::new(3);
        assert_admitted(invalid.try_admit_new_group(owner.clone(), [pk(1)]));
        let before = admission_groups(&invalid);
        assert_eq!(
            invalid.try_replace_group(owner.clone(), []),
            FixedCapReplaceResult::RejectedInvalidGroup
        );
        assert_eq!(admission_groups(&invalid), before);

        let mut cap_reject = FixedCapAdmission::new(2);
        assert_admitted(cap_reject.try_admit_new_group(owner.clone(), [pk(1), pk(2)]));
        let before = admission_groups(&cap_reject);
        assert_eq!(
            cap_reject.try_replace_group(owner.clone(), [pk(1), pk(3), pk(4)]),
            FixedCapReplaceResult::RejectedCap {
                required_unique: 2,
                available_unique: 1,
            }
        );
        assert_eq!(admission_groups(&cap_reject), before);
    }

    #[test]
    fn replace_cached_len_matches_snapshot_after_long_sequences() {
        let mut admission = FixedCapAdmission::new(6);
        let mut model = RefReplaceModel::new(6);
        let owners = [
            pool_owner(ExplicitConsumer::Momentum, 1),
            pool_owner(ExplicitConsumer::Arb, 2),
            pool_owner(ExplicitConsumer::Tracker, 3),
        ];
        let key_pool: Vec<Pubkey> = (1u8..=8).map(pk).collect();

        for (step, owner) in owners.iter().enumerate() {
            let keys: Vec<Pubkey> = key_pool
                .iter()
                .copied()
                .filter(|k| (k.to_bytes()[0] as usize + step) % 2 == 0)
                .take(2)
                .collect();
            assert_admitted(admission.try_admit_new_group(owner.clone(), keys.clone()));
            model.admit_for_setup(owner.clone(), keys);
            assert_replace_state_matches(&admission, &model);
        }

        for step in 0..12 {
            let owner = owners[step % owners.len()].clone();
            let keys: Vec<Pubkey> = key_pool
                .iter()
                .copied()
                .filter(|k| (k.to_bytes()[0] as usize + step) % 3 != 0)
                .take(1 + step % 3)
                .collect();
            if keys.is_empty() {
                continue;
            }
            let result = admission.try_replace_group(owner.clone(), keys.clone());
            let expected = model.try_replace_group(owner.clone(), keys);
            assert_eq!(result, expected);
            assert_replace_state_matches(&admission, &model);
        }
    }

    #[test]
    fn replace_planning_stats_match_asymmetric_group_lookups() {
        let mut admission = FixedCapAdmission::new(8);
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        let old_keys = [pk(1), pk(2), pk(3), pk(4), pk(5)];
        let new_keys = [pk(3), pk(4), pk(6), pk(7), pk(8), pk(9)];
        assert_admitted(admission.try_admit_new_group(owner.clone(), old_keys));
        admission.planning_stats.refcount_lookups = 0;
        admission.planning_stats.owner_group_lookups = 0;

        let _ = admission.try_replace_group(owner, new_keys);
        assert_eq!(admission.planning_stats().owner_group_lookups, 1);
        assert_eq!(admission.planning_stats().refcount_lookups, 7);
    }

    #[test]
    fn bounded_reference_replace_model_matches_admission() {
        let caps = [2usize, 3, 5, 8];
        let seeds = [3u8, 11, 77];

        caps.iter().copied().for_each(|cap| {
            seeds.iter().copied().for_each(|seed| {
                let mut admission = FixedCapAdmission::new(cap);
                let mut model = RefReplaceModel::new(cap);
                let owners = [
                    pool_owner(ExplicitConsumer::Momentum, seed),
                    pool_owner(ExplicitConsumer::Arb, seed.wrapping_add(1)),
                ];
                let key_pool: Vec<Pubkey> = (0u8..7).map(|b| pk(b.wrapping_add(seed))).collect();

                for (step, owner) in owners.iter().enumerate() {
                    let keys: Vec<Pubkey> = key_pool
                        .iter()
                        .copied()
                        .filter(|k| (k.to_bytes()[0] as usize + step) % 2 == 0)
                        .take(1)
                        .collect();
                    if keys.is_empty() {
                        continue;
                    }
                    let admit_result = admission.try_admit_new_group(owner.clone(), keys.clone());
                    if matches!(admit_result, FixedCapAdmissionResult::RejectedCap { .. }) {
                        continue;
                    }
                    assert_admitted(admit_result);
                    model.admit_for_setup(owner.clone(), keys);
                    assert_replace_state_matches(&admission, &model);
                }

                for step in 0..10 {
                    let owner = owners[step % owners.len()].clone();
                    if !model.groups.contains_key(&owner) {
                        continue;
                    }
                    let keys: Vec<Pubkey> = key_pool
                        .iter()
                        .copied()
                        .filter(|k| (k.to_bytes()[0] as usize + step) % 3 != 1)
                        .take(1 + step % 2)
                        .collect();
                    if keys.is_empty() {
                        continue;
                    }
                    let before = admission_groups(&admission);
                    let result = admission.try_replace_group(owner.clone(), keys.clone());
                    let expected = model.try_replace_group(owner.clone(), keys);
                    assert_eq!(result, expected);
                    if matches!(
                        result,
                        FixedCapReplaceResult::RejectedCap { .. }
                            | FixedCapReplaceResult::RejectedMissingOwner
                            | FixedCapReplaceResult::RejectedInvalidGroup
                    ) {
                        assert_eq!(admission_groups(&admission), before);
                    }
                    assert_replace_state_matches(&admission, &model);
                }
            });
        });
    }

    #[test]
    fn replace_forced_commit_mismatch_restores_previous_owner() {
        let mut admission = FixedCapAdmission::new(4);
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        assert_admitted(admission.try_admit_new_group(owner.clone(), [pk(1), pk(2)]));
        let before = capture_admission_snapshot(&admission);

        admission.set_test_force_commit_plan_mismatch(true);
        let result = admission.try_replace_group(owner.clone(), [pk(2), pk(3)]);
        assert_eq!(
            result,
            FixedCapReplaceResult::InternalInvariantViolation {
                recovery: InvariantViolationRecovery::PreviousRestored,
            }
        );
        let after = capture_admission_snapshot(&admission);
        assert_public_snapshots_equal(&after, &before);
        assert_no_extra_reverse_index_keys(&admission);
        assert_eq!(
            admission.owner_group(&owner),
            Some([pk(1), pk(2)].as_slice())
        );
    }

    #[test]
    fn replace_forced_restore_mismatch_removes_owner_and_reconciles_len() {
        let mut admission = FixedCapAdmission::new(4);
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        let other = pool_owner(ExplicitConsumer::Arb, 2);
        assert_admitted(admission.try_admit_new_group(owner.clone(), [pk(1), pk(2)]));
        assert_admitted(admission.try_admit_new_group(other.clone(), [pk(3)]));
        let before = capture_admission_snapshot(&admission);

        admission.set_test_force_commit_plan_mismatch(true);
        admission.set_test_force_restore_plan_mismatch(true);
        let result = admission.try_replace_group(owner.clone(), [pk(2), pk(4)]);
        assert_eq!(
            result,
            FixedCapReplaceResult::InternalInvariantViolation {
                recovery: InvariantViolationRecovery::OwnerRemovedFailClosed,
            }
        );
        assert!(admission.owner_group(&owner).is_none());

        let mut expected_groups = before.owner_groups.clone();
        expected_groups.remove(&owner);
        let expected_pubkeys: Vec<Pubkey> = expected_groups
            .values()
            .flat_map(|set| set.iter().copied())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let expected = AdmissionPublicSnapshot {
            owner_groups: expected_groups.clone(),
            pubkeys: expected_pubkeys,
            pubkey_refcounts: pubkey_refcounts_from_groups(&expected_groups),
        };
        let after = capture_admission_snapshot(&admission);
        assert_public_snapshots_equal(&after, &expected);
        assert_no_extra_reverse_index_keys(&admission);
        assert!(admission.len() <= admission.cap());
        assert_eq!(admission.len(), admission.snapshot_pubkeys().len());
    }

    // --- Removal tests (PR 1b3) ---

    #[test]
    fn remove_exclusive_owner_removes_all_physical_keys() {
        let mut admission = FixedCapAdmission::new(3);
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        assert_admitted(admission.try_admit_new_group(owner.clone(), [pk(1), pk(2)]));
        assert_eq!(admission.len(), 2);

        let result = admission.remove_group(owner.clone());
        assert_eq!(
            result,
            FixedCapRemoveResult::Removed {
                physical_removed: vec![pk(1), pk(2)],
            }
        );
        assert!(admission.is_empty());
        assert!(admission.owner_group(&owner).is_none());
    }

    #[test]
    fn remove_keeps_shared_key_and_removes_exclusive_key() {
        let mut admission = FixedCapAdmission::new(4);
        let shared = pk(1);
        let owner_a = pool_owner(ExplicitConsumer::Momentum, 1);
        let owner_b = pool_owner(ExplicitConsumer::Arb, 2);
        assert_admitted(admission.try_admit_new_group(owner_a.clone(), [shared, pk(2)]));
        assert_admitted(admission.try_admit_new_group(owner_b.clone(), [shared]));
        let before_len = admission.len();

        let result = admission.remove_group(owner_a.clone());
        assert_eq!(
            result,
            FixedCapRemoveResult::Removed {
                physical_removed: vec![pk(2)],
            }
        );
        assert_eq!(admission.len(), before_len - 1);
        assert!(admission.owner_refcount(&shared) > 0);
        assert_eq!(admission.owner_refcount(&pk(2)), 0);
        assert!(admission.owner_group(&owner_a).is_none());
        assert_eq!(admission.owner_group(&owner_b), Some([shared].as_slice()));
    }

    #[test]
    fn remove_fully_shared_owner_removes_no_physical_keys() {
        let mut admission = FixedCapAdmission::new(2);
        let shared = pk(1);
        let owner_a = pool_owner(ExplicitConsumer::Momentum, 1);
        let owner_b = pool_owner(ExplicitConsumer::Arb, 2);
        assert_admitted(admission.try_admit_new_group(owner_a.clone(), [shared]));
        assert_admitted(admission.try_admit_new_group(owner_b.clone(), [shared]));
        let before_len = admission.len();

        let result = admission.remove_group(owner_b.clone());
        assert_eq!(
            result,
            FixedCapRemoveResult::Removed {
                physical_removed: vec![],
            }
        );
        assert_eq!(admission.len(), before_len);
        assert_eq!(admission.owner_refcount(&shared), 1);
        assert!(admission.owner_group(&owner_b).is_none());
        assert_eq!(admission.owner_group(&owner_a), Some([shared].as_slice()));
    }

    #[test]
    fn remove_unknown_owner_is_mutation_free() {
        let mut admission = FixedCapAdmission::new(3);
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        let unknown = pool_owner(ExplicitConsumer::Arb, 9);
        assert_admitted(admission.try_admit_new_group(owner.clone(), [pk(1)]));
        let before = capture_admission_snapshot(&admission);

        assert_eq!(
            admission.remove_group(unknown),
            FixedCapRemoveResult::NotFound
        );
        assert_public_snapshots_equal(&capture_admission_snapshot(&admission), &before);
    }

    #[test]
    fn remove_cached_len_matches_snapshot_after_long_sequences() {
        let mut admission = FixedCapAdmission::new(6);
        let mut model = RefRemoveModel::new(6);
        let owners = [
            pool_owner(ExplicitConsumer::Momentum, 1),
            pool_owner(ExplicitConsumer::Arb, 2),
            pool_owner(ExplicitConsumer::Tracker, 3),
        ];
        let key_pool: Vec<Pubkey> = (1u8..=8).map(pk).collect();

        for (step, owner) in owners.iter().enumerate() {
            let keys: Vec<Pubkey> = key_pool
                .iter()
                .copied()
                .filter(|k| (k.to_bytes()[0] as usize + step) % 2 == 0)
                .take(2)
                .collect();
            assert_admitted(admission.try_admit_new_group(owner.clone(), keys.clone()));
            model.admit_for_setup(owner.clone(), keys);
            assert_remove_state_matches(&admission, &model);
        }

        for step in 0..8 {
            let owner = owners[step % owners.len()].clone();
            if admission.owner_group(&owner).is_none() {
                continue;
            }
            let result = admission.remove_group(owner.clone());
            let expected = model.remove_group(owner.clone());
            assert_eq!(result, expected);
            assert_remove_state_matches(&admission, &model);

            if step % 3 == 0 {
                let keys: Vec<Pubkey> = key_pool
                    .iter()
                    .copied()
                    .filter(|k| (k.to_bytes()[0] as usize + step) % 3 != 0)
                    .take(1 + step % 2)
                    .collect();
                if !keys.is_empty() && admission.owner_group(&owner).is_none() {
                    let admit = admission.try_admit_new_group(owner.clone(), keys.clone());
                    if matches!(
                        admit,
                        FixedCapAdmissionResult::Inserted { .. }
                            | FixedCapAdmissionResult::OwnerAddedNoNewPubkey
                    ) {
                        model.admit_for_setup(owner.clone(), keys);
                        assert_remove_state_matches(&admission, &model);
                    }
                }
            }
        }
    }

    #[test]
    fn remove_planning_stats_match_old_group_edges() {
        let mut admission = FixedCapAdmission::new(8);
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        let keys = [pk(1), pk(2), pk(3), pk(4), pk(5)];
        assert_admitted(admission.try_admit_new_group(owner.clone(), keys));
        admission.planning_stats.refcount_lookups = 0;
        admission.planning_stats.owner_group_lookups = 0;

        let _ = admission.remove_group(owner);
        assert_eq!(admission.planning_stats().owner_group_lookups, 1);
        assert_eq!(admission.planning_stats().refcount_lookups, 5);
    }

    #[test]
    fn bounded_reference_remove_model_matches_admission() {
        let caps = [2usize, 3, 5, 8];
        let seeds = [5u8, 13, 91];

        caps.iter().copied().for_each(|cap| {
            seeds.iter().copied().for_each(|seed| {
                let mut admission = FixedCapAdmission::new(cap);
                let mut model = RefRemoveModel::new(cap);
                let owners = [
                    pool_owner(ExplicitConsumer::Momentum, seed),
                    pool_owner(ExplicitConsumer::Arb, seed.wrapping_add(1)),
                    pool_owner(ExplicitConsumer::Tracker, seed.wrapping_add(2)),
                ];
                let key_pool: Vec<Pubkey> = (0u8..7).map(|b| pk(b.wrapping_add(seed))).collect();

                for (step, owner) in owners.iter().enumerate() {
                    let keys: Vec<Pubkey> = key_pool
                        .iter()
                        .copied()
                        .filter(|k| (k.to_bytes()[0] as usize + step) % 2 == 0)
                        .take(1 + step % 2)
                        .collect();
                    if keys.is_empty() {
                        continue;
                    }
                    let admit_result = admission.try_admit_new_group(owner.clone(), keys.clone());
                    if matches!(admit_result, FixedCapAdmissionResult::RejectedCap { .. }) {
                        continue;
                    }
                    assert_admitted(admit_result);
                    model.admit_for_setup(owner.clone(), keys);
                    assert_remove_state_matches(&admission, &model);
                }

                for step in 0..10 {
                    let owner = owners[step % owners.len()].clone();
                    if !model.groups.contains_key(&owner) {
                        let before = capture_admission_snapshot(&admission);
                        assert_eq!(
                            admission.remove_group(owner.clone()),
                            FixedCapRemoveResult::NotFound
                        );
                        assert_public_snapshots_equal(
                            &capture_admission_snapshot(&admission),
                            &before,
                        );
                        continue;
                    }
                    let before = admission_groups(&admission);
                    let result = admission.remove_group(owner.clone());
                    let expected = model.remove_group(owner.clone());
                    assert_eq!(result, expected);
                    if matches!(result, FixedCapRemoveResult::NotFound) {
                        assert_eq!(admission_groups(&admission), before);
                    }
                    assert_remove_state_matches(&admission, &model);
                }
            });
        });
    }

    #[test]
    fn remove_forced_commit_mismatch_restores_previous_owner() {
        let mut admission = FixedCapAdmission::new(4);
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        assert_admitted(admission.try_admit_new_group(owner.clone(), [pk(1), pk(2)]));
        let before = capture_admission_snapshot(&admission);

        admission.set_test_force_commit_plan_mismatch(true);
        let result = admission.remove_group(owner.clone());
        assert_eq!(
            result,
            FixedCapRemoveResult::InternalInvariantViolation {
                recovery: InvariantViolationRecovery::PreviousRestored,
            }
        );
        let after = capture_admission_snapshot(&admission);
        assert_public_snapshots_equal(&after, &before);
        assert_no_extra_reverse_index_keys(&admission);
        assert_eq!(
            admission.owner_group(&owner),
            Some([pk(1), pk(2)].as_slice())
        );
    }

    #[test]
    fn remove_forced_restore_mismatch_removes_owner_and_reconciles_len() {
        let mut admission = FixedCapAdmission::new(4);
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        let other = pool_owner(ExplicitConsumer::Arb, 2);
        assert_admitted(admission.try_admit_new_group(owner.clone(), [pk(1), pk(2)]));
        assert_admitted(admission.try_admit_new_group(other.clone(), [pk(3)]));
        let before = capture_admission_snapshot(&admission);

        admission.set_test_force_commit_plan_mismatch(true);
        admission.set_test_force_restore_plan_mismatch(true);
        let result = admission.remove_group(owner.clone());
        assert_eq!(
            result,
            FixedCapRemoveResult::InternalInvariantViolation {
                recovery: InvariantViolationRecovery::OwnerRemovedFailClosed,
            }
        );
        assert!(admission.owner_group(&owner).is_none());

        let mut expected_groups = before.owner_groups.clone();
        expected_groups.remove(&owner);
        let expected_pubkeys: Vec<Pubkey> = expected_groups
            .values()
            .flat_map(|set| set.iter().copied())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let expected = AdmissionPublicSnapshot {
            owner_groups: expected_groups.clone(),
            pubkeys: expected_pubkeys,
            pubkey_refcounts: pubkey_refcounts_from_groups(&expected_groups),
        };
        let after = capture_admission_snapshot(&admission);
        assert_public_snapshots_equal(&after, &expected);
        assert_no_extra_reverse_index_keys(&admission);
        assert!(admission.len() <= admission.cap());
        assert_eq!(admission.len(), admission.snapshot_pubkeys().len());
    }

    #[test]
    fn remove_does_not_leave_stale_reverse_index_keys() {
        let mut admission = FixedCapAdmission::new(4);
        let shared = pk(1);
        let owner_a = pool_owner(ExplicitConsumer::Momentum, 1);
        let owner_b = pool_owner(ExplicitConsumer::Arb, 2);
        assert_admitted(admission.try_admit_new_group(owner_a.clone(), [shared, pk(2)]));
        assert_admitted(admission.try_admit_new_group(owner_b.clone(), [shared]));

        assert_eq!(
            admission.remove_group(owner_a),
            FixedCapRemoveResult::Removed {
                physical_removed: vec![pk(2)],
            }
        );
        assert_no_extra_reverse_index_keys(&admission);
        assert_eq!(admission.owner_refcount(&shared), 1);
        assert_eq!(admission.owner_refcount(&pk(2)), 0);
    }

    fn assert_admitted(result: FixedCapAdmissionResult) {
        match result {
            FixedCapAdmissionResult::Inserted { .. }
            | FixedCapAdmissionResult::OwnerAddedNoNewPubkey => {}
            other => panic!("expected successful admission, got {other:?}"),
        }
    }
}
