//! Insert, replacement, owner-removal, and evicting fixed-cap admission over
//! [`ExplicitOwnership`] (PR 1b1/1b2/1b3/1c2).
//!
//! Accepts only previously absent owners via insert. Replacement mutates only existing owners.
//! Removal is group-local and atomic. Evicting admission commits via validated state swap.
//! No cap mutation or runtime wiring.
//!
//! Normal planning is O(group_size). [`FixedCapAdmission::reconcile_physical_len_cold`]
//! may scan ownership only on exceptional invariant/rollback failure.

use super::eviction_planner::{
    select_eviction_victims, EvictionPlanningSnapshot, EvictionTier, OwnerLruEntry,
    VictimSelectionPlan, VictimSelectionRequest, VictimSelectionResult,
};
use super::explicit_ownership::{
    EmptyOwnerGroupError, ExplicitOwner, ExplicitOwnership, GroupChange, OwnerGroupSnapshot,
};
use solana_sdk::pubkey::Pubkey;
use std::collections::{BTreeMap, BTreeSet};

/// Recovery path after an internal invariant violation during replacement rollback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvariantViolationRecovery {
    /// Commit mismatch: previous owner group was atomically restored.
    PreviousRestored,
    /// Restore validation failed: owner was fail-closed removed and `physical_len` reconciled.
    OwnerRemovedFailClosed,
}

/// Recovery path after an internal invariant violation during removal rollback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FixedCapRemoveRecovery {
    /// Commit mismatch: previous owner group was atomically restored.
    PreviousRestored,
    /// Restore validation failed: owner was fail-closed removed and `physical_len` reconciled.
    OwnerRemovedFailClosed,
    /// One-shot fail-closed removal did not leave the owner absent.
    RecoveryFailed,
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
    /// Pre-commit planning invariant failure; no mutation.
    PlanningInvariantViolation,
    /// Post-commit plan/rollback mismatch with a documented recovery path.
    InternalInvariantViolation { recovery: FixedCapRemoveRecovery },
}

/// Read-only eviction plan for a hypothetical new-owner admit (PR 1c2b1).
///
/// Alias of [`VictimSelectionResult`]: identical cap/delta semantics from the pure selector
/// over an internally built [`EvictionPlanningSnapshot`]. Not a commit token.
pub type AdmissionEvictionPlanResult = VictimSelectionResult;

/// Result of an atomic admit-with-eviction attempt (PR 1c2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EvictingAdmissionResult {
    /// Fast path: incoming fits without eviction.
    InsertedNoEviction { physical_added: Vec<Pubkey> },
    /// Eviction path: victims removed and incoming committed via validated state swap.
    InsertedWithEviction {
        physical_added: Vec<Pubkey>,
        physical_removed: Vec<Pubkey>,
        victims: Vec<ExplicitOwner>,
        opened_through: EvictionTier,
    },
    /// Owner already present; fully mutation-free.
    RejectedExistingOwner,
    /// Planner could not free enough space at allowed tiers.
    RejectedProtected {
        incoming_physical_added: usize,
        required_to_free: usize,
    },
    /// Empty incoming group.
    RejectedInvalidInput,
    /// Candidate build/validation or planner inconsistency; live state unchanged.
    InternalInvariantViolation,
}

/// Result of an explicit owner-group LRU touch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TouchResult {
    /// Owner present; stamp advanced.
    Touched,
    /// Owner absent; fully mutation-free.
    NotFound,
    /// Ownership/LRU owner sets diverged during internal recovery.
    InternalInvariantViolation,
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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct EvictingStatsDelta {
    owner_group_lookups: u64,
    refcount_lookups: u64,
    candidate_builds: u64,
    candidate_group_edges: u64,
}

/// Test-only counters for instrumented planning lookups.
///
/// `eviction_candidate_builds` / `eviction_candidate_group_edges` count read-only
/// candidate-graph rebuild work on the eviction commit path (one build per success).
#[cfg(test)]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlanningStats {
    pub refcount_lookups: u64,
    pub owner_group_lookups: u64,
    pub eviction_candidate_builds: u64,
    pub eviction_candidate_group_edges: u64,
}

#[cfg(test)]
impl PlanningStats {
    fn record_refcount_lookup(&mut self) {
        self.refcount_lookups += 1;
    }

    fn record_owner_group_lookup(&mut self) {
        self.owner_group_lookups += 1;
    }

    fn after_eviction_delta(&self, delta: EvictingStatsDelta) -> Option<Self> {
        Some(Self {
            owner_group_lookups: self
                .owner_group_lookups
                .checked_add(delta.owner_group_lookups)?,
            refcount_lookups: self.refcount_lookups.checked_add(delta.refcount_lookups)?,
            eviction_candidate_builds: self
                .eviction_candidate_builds
                .checked_add(delta.candidate_builds)?,
            eviction_candidate_group_edges: self
                .eviction_candidate_group_edges
                .checked_add(delta.candidate_group_edges)?,
        })
    }
}

/// Insert-only fixed-cap admission layer over explicit ownership.
#[derive(Debug, Clone)]
pub struct FixedCapAdmission {
    cap: usize,
    physical_len: usize,
    ownership: ExplicitOwnership,
    owner_lru: BTreeMap<ExplicitOwner, u64>,
    next_touch_stamp: u64,
    #[cfg(test)]
    planning_stats: PlanningStats,
    #[cfg(test)]
    test_force_commit_plan_mismatch: bool,
    #[cfg(test)]
    test_force_restore_plan_mismatch: bool,
    #[cfg(test)]
    test_force_remove_planning_underflow: bool,
    #[cfg(test)]
    test_force_remove_planning_zero_refcount: bool,
    #[cfg(test)]
    test_force_stamp_plan_failure: bool,
    #[cfg(test)]
    test_eviction_plan_extra_lru: Vec<OwnerLruEntry>,
    #[cfg(test)]
    test_force_evicting_omit_survivor: Option<ExplicitOwner>,
    #[cfg(test)]
    test_force_evicting_corrupt_physical_len: bool,
    #[cfg(test)]
    test_force_evicting_corrupt_survivor_stamp: Option<ExplicitOwner>,
    #[cfg(test)]
    test_force_evicting_extra_candidate_owner: Option<(ExplicitOwner, Vec<Pubkey>)>,
    #[cfg(test)]
    test_force_evicting_corrupt_next_touch_stamp: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StampPlan {
    owner_lru: BTreeMap<ExplicitOwner, u64>,
    next_touch_stamp: u64,
}

#[derive(Debug, Clone)]
struct EvictingCandidateState {
    ownership: ExplicitOwnership,
    owner_lru: BTreeMap<ExplicitOwner, u64>,
    next_touch_stamp: u64,
    physical_len: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StampPlanError {
    Overflow,
    MissingOwnerStamp,
    ForcedFailure,
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
    old_refcounts: Vec<(Pubkey, usize)>,
}

enum RemovePlanOutcome {
    Ready(RemovePlan),
    PlanningInvariantViolation,
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
            owner_lru: BTreeMap::new(),
            next_touch_stamp: 1,
            #[cfg(test)]
            planning_stats: PlanningStats::default(),
            #[cfg(test)]
            test_force_commit_plan_mismatch: false,
            #[cfg(test)]
            test_force_restore_plan_mismatch: false,
            #[cfg(test)]
            test_force_remove_planning_underflow: false,
            #[cfg(test)]
            test_force_remove_planning_zero_refcount: false,
            #[cfg(test)]
            test_force_stamp_plan_failure: false,
            #[cfg(test)]
            test_eviction_plan_extra_lru: Vec::new(),
            #[cfg(test)]
            test_force_evicting_omit_survivor: None,
            #[cfg(test)]
            test_force_evicting_corrupt_physical_len: false,
            #[cfg(test)]
            test_force_evicting_corrupt_survivor_stamp: None,
            #[cfg(test)]
            test_force_evicting_extra_candidate_owner: None,
            #[cfg(test)]
            test_force_evicting_corrupt_next_touch_stamp: false,
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

    /// Advance the LRU stamp for an existing owner group.
    pub fn touch_group(&mut self, owner: ExplicitOwner) -> TouchResult {
        if self.ownership.owner_group(&owner).is_none() {
            return TouchResult::NotFound;
        }
        let Some(_pre_stamp) = self.owner_lru.get(&owner).copied() else {
            self.fail_closed_remove_owner_stamp_mismatch(&owner);
            return TouchResult::InternalInvariantViolation;
        };
        let stamp_plan = match plan_stamp_assign(
            &self.owner_lru,
            self.next_touch_stamp,
            &owner,
            self.stamp_plan_force_failure(),
        ) {
            Ok(plan) => plan,
            Err(_) => return TouchResult::InternalInvariantViolation,
        };
        self.apply_stamp_plan(stamp_plan);
        TouchResult::Touched
    }

    /// One LRU row per current owner, sorted deterministically by owner.
    pub fn snapshot_lru_entries(&self) -> Vec<OwnerLruEntry> {
        self.owner_lru
            .iter()
            .map(|(owner, &last_touch)| OwnerLruEntry {
                owner: owner.clone(),
                last_touch,
            })
            .collect()
    }

    /// Current LRU stamp for an owner, if present.
    pub fn last_touch(&self, owner: &ExplicitOwner) -> Option<u64> {
        self.owner_lru.get(owner).copied()
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

    #[cfg(test)]
    pub fn set_test_force_remove_planning_underflow(&mut self, enabled: bool) {
        self.test_force_remove_planning_underflow = enabled;
    }

    #[cfg(test)]
    pub fn set_test_force_remove_planning_zero_refcount(&mut self, enabled: bool) {
        self.test_force_remove_planning_zero_refcount = enabled;
    }

    #[cfg(test)]
    pub fn set_test_force_stamp_plan_failure(&mut self, enabled: bool) {
        self.test_force_stamp_plan_failure = enabled;
    }

    #[cfg(test)]
    pub fn set_test_next_touch_stamp(&mut self, stamp: u64) {
        self.next_touch_stamp = stamp;
    }

    #[cfg(test)]
    pub fn set_test_owner_stamp(&mut self, owner: ExplicitOwner, stamp: u64) {
        self.owner_lru.insert(owner, stamp);
    }

    #[cfg(test)]
    pub fn test_owner_stamps(&self) -> BTreeMap<ExplicitOwner, u64> {
        self.owner_lru.clone()
    }

    #[cfg(test)]
    pub fn test_build_eviction_snapshot(
        &self,
    ) -> Result<
        super::eviction_planner::EvictionPlanningSnapshot,
        super::eviction_planner::SnapshotBuildError,
    > {
        super::eviction_planner::EvictionPlanningSnapshot::from_ownership(
            &self.ownership,
            self.snapshot_lru_entries(),
        )
    }

    #[cfg(test)]
    pub fn test_next_touch_stamp(&self) -> u64 {
        self.next_touch_stamp
    }

    #[cfg(test)]
    pub fn test_renormalize_lru_stamps(&mut self) {
        if let Ok(map) = renormalize_stamps_fallible(&self.owner_lru) {
            let len = map.len();
            if let Ok(base) = u64::try_from(len) {
                if let Some(next) = base.checked_add(1) {
                    self.owner_lru = map;
                    self.next_touch_stamp = next;
                }
            }
        }
    }

    #[cfg(test)]
    pub fn set_test_eviction_plan_extra_lru(&mut self, entries: Vec<OwnerLruEntry>) {
        self.test_eviction_plan_extra_lru = entries;
    }

    #[cfg(test)]
    pub fn set_test_force_evicting_omit_survivor(&mut self, owner: Option<ExplicitOwner>) {
        self.test_force_evicting_omit_survivor = owner;
    }

    #[cfg(test)]
    pub fn set_test_force_evicting_corrupt_physical_len(&mut self, enabled: bool) {
        self.test_force_evicting_corrupt_physical_len = enabled;
    }

    #[cfg(test)]
    pub fn set_test_force_evicting_corrupt_survivor_stamp(&mut self, owner: Option<ExplicitOwner>) {
        self.test_force_evicting_corrupt_survivor_stamp = owner;
    }

    #[cfg(test)]
    pub fn set_test_force_evicting_extra_candidate_owner(
        &mut self,
        owner: Option<(ExplicitOwner, Vec<Pubkey>)>,
    ) {
        self.test_force_evicting_extra_candidate_owner = owner;
    }

    #[cfg(test)]
    pub fn set_test_force_evicting_corrupt_next_touch_stamp(&mut self, enabled: bool) {
        self.test_force_evicting_corrupt_next_touch_stamp = enabled;
    }

    /// Plan eviction victims for a hypothetical new-owner admit using only internal state.
    ///
    /// Read-only: builds [`EvictionPlanningSnapshot`] from [`Self::ownership`] and
    /// [`Self::snapshot_lru_entries`], then delegates to [`select_eviction_victims`].
    /// No ownership/LRU mutation, stamp consumption, victim removal, or incoming commit.
    /// Snapshot construction failures map to [`VictimSelectionResult::InternalInvariantViolation`].
    pub fn plan_admit_with_eviction(
        &self,
        incoming_owner: ExplicitOwner,
        incoming_pubkeys: impl IntoIterator<Item = Pubkey>,
    ) -> AdmissionEvictionPlanResult {
        let incoming_pubkeys = normalize_pubkeys(incoming_pubkeys);
        let snapshot = match EvictionPlanningSnapshot::from_ownership(
            &self.ownership,
            self.eviction_planning_lru_entries(),
        ) {
            Ok(snapshot) => snapshot,
            Err(_) => return VictimSelectionResult::InternalInvariantViolation,
        };
        select_eviction_victims(
            &snapshot,
            VictimSelectionRequest {
                incoming_owner,
                incoming_pubkeys,
                cap: self.cap,
            },
        )
    }

    fn eviction_planning_lru_entries(&self) -> Vec<OwnerLruEntry> {
        #[cfg(test)]
        if !self.test_eviction_plan_extra_lru.is_empty() {
            let mut entries = self.snapshot_lru_entries();
            entries.extend(self.test_eviction_plan_extra_lru.iter().cloned());
            return entries;
        }
        self.snapshot_lru_entries()
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

        let stamp_plan = match plan_stamp_assign(
            &self.owner_lru,
            self.next_touch_stamp,
            &owner,
            self.stamp_plan_force_failure(),
        ) {
            Ok(plan) => plan,
            Err(_) => return FixedCapAdmissionResult::InternalInvariantViolation,
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
        self.apply_stamp_plan(stamp_plan);
        if plan.physical_added.is_empty() {
            FixedCapAdmissionResult::OwnerAddedNoNewPubkey
        } else {
            FixedCapAdmissionResult::Inserted {
                physical_added: plan.physical_added,
            }
        }
    }

    /// Admit a new owner group, evicting victims when cap pressure requires it.
    ///
    /// Fast path (no cap pressure) delegates to [`Self::try_admit_new_group`]. Eviction path builds
    /// and validates a full candidate ownership/LRU snapshot, then commits via infallible swap.
    pub fn try_admit_with_eviction(
        &mut self,
        owner: ExplicitOwner,
        pubkeys: impl IntoIterator<Item = Pubkey>,
    ) -> EvictingAdmissionResult {
        let normalized = normalize_pubkeys(pubkeys);
        if normalized.is_empty() {
            return EvictingAdmissionResult::RejectedInvalidInput;
        }

        if self.ownership.owner_group(&owner).is_some() {
            return EvictingAdmissionResult::RejectedExistingOwner;
        }

        let physical_added: Vec<Pubkey> = normalized
            .iter()
            .copied()
            .filter(|pubkey| self.ownership.owner_refcount(pubkey) == 0)
            .collect();

        match self.plan_insert_readonly(&normalized) {
            InsertPlanOutcome::Ready(_) => {
                return self.map_fast_path_admit(owner, normalized);
            }
            InsertPlanOutcome::RejectCap { .. } => {}
            InsertPlanOutcome::InternalInvariantViolation => {
                return EvictingAdmissionResult::InternalInvariantViolation;
            }
        }

        let plan_result = self.plan_admit_with_eviction(owner.clone(), normalized.clone());
        let plan = match plan_result {
            VictimSelectionResult::Planned(plan) => plan,
            VictimSelectionResult::NoEvictionNeeded { .. } => {
                return EvictingAdmissionResult::InternalInvariantViolation;
            }
            VictimSelectionResult::RejectedProtected {
                incoming_physical_added,
                required_to_free,
            } => {
                return EvictingAdmissionResult::RejectedProtected {
                    incoming_physical_added,
                    required_to_free,
                };
            }
            VictimSelectionResult::RejectedInvalidInput => {
                return EvictingAdmissionResult::RejectedInvalidInput;
            }
            VictimSelectionResult::InternalInvariantViolation => {
                return EvictingAdmissionResult::InternalInvariantViolation;
            }
        };

        let (candidate, stats_delta) =
            match self.build_evicting_candidate(&owner, &normalized, &plan) {
                Ok(state) => state,
                Err(()) => return EvictingAdmissionResult::InternalInvariantViolation,
            };

        if !self.validate_evicting_candidate(
            &candidate,
            &plan,
            &owner,
            &normalized,
            &physical_added,
        ) {
            return EvictingAdmissionResult::InternalInvariantViolation;
        }

        #[cfg(test)]
        let post_stats = match self.planning_stats.after_eviction_delta(stats_delta) {
            Some(stats) => stats,
            None => return EvictingAdmissionResult::InternalInvariantViolation,
        };
        #[cfg(not(test))]
        let _stats_delta = stats_delta;

        let result = EvictingAdmissionResult::InsertedWithEviction {
            physical_added,
            physical_removed: plan.physical_freed,
            victims: plan.victims,
            opened_through: plan.opened_through,
        };

        self.ownership = candidate.ownership;
        self.owner_lru = candidate.owner_lru;
        self.next_touch_stamp = candidate.next_touch_stamp;
        self.physical_len = candidate.physical_len;
        #[cfg(test)]
        {
            self.planning_stats = post_stats;
        }

        result
    }

    fn map_fast_path_admit(
        &mut self,
        owner: ExplicitOwner,
        normalized: Vec<Pubkey>,
    ) -> EvictingAdmissionResult {
        #[cfg(test)]
        let stats_before = self.planning_stats.clone();

        let mapped = match self.try_admit_new_group(owner, normalized) {
            FixedCapAdmissionResult::Inserted { physical_added } => {
                EvictingAdmissionResult::InsertedNoEviction { physical_added }
            }
            FixedCapAdmissionResult::OwnerAddedNoNewPubkey => {
                EvictingAdmissionResult::InsertedNoEviction {
                    physical_added: Vec::new(),
                }
            }
            FixedCapAdmissionResult::RejectedExistingOwner => {
                EvictingAdmissionResult::RejectedExistingOwner
            }
            FixedCapAdmissionResult::RejectedInvalidGroup => {
                EvictingAdmissionResult::RejectedInvalidInput
            }
            FixedCapAdmissionResult::RejectedCap { .. }
            | FixedCapAdmissionResult::InternalInvariantViolation => {
                EvictingAdmissionResult::InternalInvariantViolation
            }
        };

        #[cfg(test)]
        if !matches!(mapped, EvictingAdmissionResult::InsertedNoEviction { .. }) {
            self.planning_stats = stats_before;
        }

        mapped
    }

    fn build_evicting_candidate(
        &self,
        incoming_owner: &ExplicitOwner,
        incoming_pubkeys: &[Pubkey],
        plan: &VictimSelectionPlan,
    ) -> Result<(EvictingCandidateState, EvictingStatsDelta), ()> {
        let victims: BTreeSet<ExplicitOwner> = plan.victims.iter().cloned().collect();
        if victims.contains(incoming_owner) {
            return Err(());
        }

        let mut candidate_ownership = ExplicitOwnership::new();
        let mut group_edges = 0usize;
        for group in self.ownership.snapshot_owner_groups() {
            let owner = ExplicitOwner {
                consumer: group.consumer,
                owner_key: group.owner_key,
            };
            if victims.contains(&owner) {
                continue;
            }
            #[cfg(test)]
            if self.test_force_evicting_omit_survivor.as_ref() == Some(&owner) {
                return Err(());
            }
            group_edges = group_edges.saturating_add(group.pubkeys.len());
            candidate_ownership
                .upsert_group(owner, group.pubkeys.clone())
                .map_err(|EmptyOwnerGroupError| ())?;
        }
        candidate_ownership
            .upsert_group(incoming_owner.clone(), incoming_pubkeys.to_vec())
            .map_err(|EmptyOwnerGroupError| ())?;
        group_edges = group_edges.saturating_add(incoming_pubkeys.len());

        #[cfg(test)]
        if let Some((extra_owner, extra_pubkeys)) = &self.test_force_evicting_extra_candidate_owner
        {
            candidate_ownership
                .upsert_group(extra_owner.clone(), extra_pubkeys.clone())
                .map_err(|EmptyOwnerGroupError| ())?;
            group_edges = group_edges.saturating_add(extra_pubkeys.len());
        }

        let mut survivor_lru = self.owner_lru.clone();
        for victim in &plan.victims {
            survivor_lru.remove(victim);
        }
        let stamp_plan = plan_stamp_assign(
            &survivor_lru,
            self.next_touch_stamp,
            incoming_owner,
            self.stamp_plan_force_failure(),
        )
        .map_err(|_| ())?;

        let candidate_lru =
            finalize_evicting_candidate_lru(stamp_plan.owner_lru, self.evicting_corrupt_stamp());

        let next_touch_stamp = {
            #[cfg(test)]
            {
                if self.test_force_evicting_corrupt_next_touch_stamp {
                    candidate_lru.values().copied().min().unwrap_or(0)
                } else {
                    stamp_plan.next_touch_stamp
                }
            }
            #[cfg(not(test))]
            {
                stamp_plan.next_touch_stamp
            }
        };

        let physical_len = {
            let base = candidate_ownership.len();
            #[cfg(test)]
            {
                if self.test_force_evicting_corrupt_physical_len {
                    base.saturating_add(1)
                } else {
                    base
                }
            }
            #[cfg(not(test))]
            {
                base
            }
        };

        let stats_delta = EvictingStatsDelta {
            owner_group_lookups: 0,
            refcount_lookups: 0,
            candidate_builds: 1,
            candidate_group_edges: u64::try_from(group_edges).map_err(|_| ())?,
        };

        Ok((
            EvictingCandidateState {
                ownership: candidate_ownership,
                owner_lru: candidate_lru,
                next_touch_stamp,
                physical_len,
            },
            stats_delta,
        ))
    }

    fn validate_evicting_candidate(
        &self,
        candidate: &EvictingCandidateState,
        plan: &VictimSelectionPlan,
        incoming_owner: &ExplicitOwner,
        incoming_pubkeys: &[Pubkey],
        physical_added: &[Pubkey],
    ) -> bool {
        let victims: BTreeSet<ExplicitOwner> = plan.victims.iter().cloned().collect();

        let mut expected_owners: BTreeSet<ExplicitOwner> = self
            .ownership
            .snapshot_owner_groups()
            .into_iter()
            .map(|group| ExplicitOwner {
                consumer: group.consumer,
                owner_key: group.owner_key,
            })
            .filter(|owner| !victims.contains(owner))
            .collect();
        expected_owners.insert(incoming_owner.clone());

        let candidate_owners: BTreeSet<ExplicitOwner> = candidate
            .ownership
            .snapshot_owner_groups()
            .into_iter()
            .map(|group| ExplicitOwner {
                consumer: group.consumer,
                owner_key: group.owner_key,
            })
            .collect();
        if candidate_owners != expected_owners {
            return false;
        }

        for victim in &plan.victims {
            if candidate.ownership.owner_group(victim).is_some() {
                return false;
            }
            if candidate.owner_lru.contains_key(victim) {
                return false;
            }
        }

        if candidate.ownership.owner_group(incoming_owner) != Some(incoming_pubkeys) {
            return false;
        }

        for group in self.ownership.snapshot_owner_groups() {
            let owner = ExplicitOwner {
                consumer: group.consumer,
                owner_key: group.owner_key,
            };
            if victims.contains(&owner) {
                continue;
            }
            if owner == *incoming_owner {
                continue;
            }
            if self.ownership.owner_group(&owner) != candidate.ownership.owner_group(&owner) {
                return false;
            }
            if self.next_touch_stamp == u64::MAX {
                continue;
            }
            match (self.owner_lru.get(&owner), candidate.owner_lru.get(&owner)) {
                (Some(live), Some(cand)) if live == cand => {}
                _ => return false,
            }
        }

        if self.next_touch_stamp == u64::MAX {
            let survivor_order_live: Vec<ExplicitOwner> = self
                .owner_lru
                .iter()
                .filter(|(owner, _)| !victims.contains(owner))
                .map(|(owner, _)| owner.clone())
                .collect();
            let survivor_order_candidate: Vec<ExplicitOwner> = candidate
                .owner_lru
                .iter()
                .filter(|(owner, _)| *owner != incoming_owner)
                .map(|(owner, _)| owner.clone())
                .collect();
            if survivor_order_live.len() != survivor_order_candidate.len() {
                return false;
            }
            let mut live_sorted = survivor_order_live;
            let mut cand_sorted = survivor_order_candidate;
            live_sorted.sort_by(|a, b| {
                self.owner_lru[a]
                    .cmp(&self.owner_lru[b])
                    .then_with(|| a.cmp(b))
            });
            cand_sorted.sort_by(|a, b| {
                candidate.owner_lru[a]
                    .cmp(&candidate.owner_lru[b])
                    .then_with(|| a.cmp(b))
            });
            if live_sorted != cand_sorted {
                return false;
            }
        }

        let incoming_stamp = match candidate.owner_lru.get(incoming_owner) {
            Some(stamp) => *stamp,
            None => return false,
        };
        for (owner, stamp) in &candidate.owner_lru {
            if owner != incoming_owner && *stamp >= incoming_stamp {
                return false;
            }
        }

        let max_lru_stamp = candidate.owner_lru.values().copied().max().unwrap_or(0);
        if candidate.next_touch_stamp <= max_lru_stamp {
            return false;
        }
        if candidate.next_touch_stamp != incoming_stamp.saturating_add(1) {
            return false;
        }

        let live_keys: BTreeSet<Pubkey> = self.ownership.snapshot_pubkeys().into_iter().collect();
        let candidate_keys: BTreeSet<Pubkey> =
            candidate.ownership.snapshot_pubkeys().into_iter().collect();
        let mut removed: Vec<Pubkey> = live_keys.difference(&candidate_keys).copied().collect();
        removed.sort();
        let mut expected_freed = plan.physical_freed.clone();
        expected_freed.sort();
        if removed != expected_freed {
            return false;
        }

        let mut added: Vec<Pubkey> = candidate_keys.difference(&live_keys).copied().collect();
        added.sort();
        let mut expected_added = physical_added.to_vec();
        expected_added.sort();
        if added != expected_added {
            return false;
        }

        if physical_added.len() != plan.incoming_physical_added {
            return false;
        }
        if candidate.physical_len != plan.projected_final_len {
            return false;
        }
        if candidate.physical_len > self.cap {
            return false;
        }
        if candidate.physical_len != candidate.ownership.len() {
            return false;
        }

        let lru_entries: Vec<OwnerLruEntry> = candidate
            .owner_lru
            .iter()
            .map(|(owner, &last_touch)| OwnerLruEntry {
                owner: owner.clone(),
                last_touch,
            })
            .collect();
        EvictionPlanningSnapshot::from_ownership(&candidate.ownership, lru_entries).is_ok()
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
        let pre_touch_stamp = match required_existing_stamp(&self.owner_lru, &existing_owner) {
            Ok(stamp) => stamp,
            Err(StampPlanError::MissingOwnerStamp) => {
                self.fail_closed_remove_owner_stamp_mismatch(&existing_owner);
                return FixedCapReplaceResult::InternalInvariantViolation {
                    recovery: InvariantViolationRecovery::OwnerRemovedFailClosed,
                };
            }
            Err(_) => return FixedCapReplaceResult::PlanningInvariantViolation,
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

        let stamp_plan = match plan_stamp_assign(
            &self.owner_lru,
            self.next_touch_stamp,
            &existing_owner,
            self.stamp_plan_force_failure(),
        ) {
            Ok(plan) => plan,
            Err(_) => return FixedCapReplaceResult::PlanningInvariantViolation,
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
            return self.rollback_failed_replace(&existing_owner, &plan, pre_touch_stamp);
        }

        self.physical_len = plan.projected_final_len;
        self.apply_stamp_plan(stamp_plan);
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

        let pre_touch_stamp = match required_existing_stamp(&self.owner_lru, &owner) {
            Ok(stamp) => stamp,
            Err(StampPlanError::MissingOwnerStamp) => {
                self.fail_closed_remove_owner_stamp_mismatch(&owner);
                return FixedCapRemoveResult::InternalInvariantViolation {
                    recovery: FixedCapRemoveRecovery::OwnerRemovedFailClosed,
                };
            }
            Err(_) => return FixedCapRemoveResult::PlanningInvariantViolation,
        };

        let plan = match self.plan_remove(&old_normalized) {
            RemovePlanOutcome::PlanningInvariantViolation => {
                return FixedCapRemoveResult::PlanningInvariantViolation;
            }
            RemovePlanOutcome::Ready(plan) => plan,
        };

        let stamp_plan = plan_stamp_remove(&self.owner_lru, self.next_touch_stamp, &owner);

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
            &plan,
            &expected_physical_removed,
            &self.ownership,
        ) {
            return self.rollback_failed_remove(&owner, &plan, pre_touch_stamp);
        }

        self.physical_len = plan.projected_final_len;
        self.apply_stamp_plan(stamp_plan);
        FixedCapRemoveResult::Removed {
            physical_removed: plan.physical_removed,
        }
    }

    fn plan_insert(&mut self, normalized: &[Pubkey]) -> InsertPlanOutcome {
        let outcome = self.plan_insert_readonly(normalized);
        #[cfg(test)]
        if !matches!(outcome, InsertPlanOutcome::InternalInvariantViolation) {
            for _ in normalized {
                self.planning_stats.record_refcount_lookup();
            }
        }
        outcome
    }

    fn plan_insert_readonly(&self, normalized: &[Pubkey]) -> InsertPlanOutcome {
        let pre_len = self.physical_len;
        let mut physical_added = Vec::new();
        for pubkey in normalized {
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

    fn plan_remove(&mut self, old_normalized: &[Pubkey]) -> RemovePlanOutcome {
        let pre_len = self.physical_len;
        let mut physical_removed = Vec::new();
        let mut old_refcounts = Vec::with_capacity(old_normalized.len());
        for pubkey in old_normalized {
            #[cfg(test)]
            self.planning_stats.record_refcount_lookup();
            let refcount = self.ownership.owner_refcount(pubkey);
            #[cfg(test)]
            let refcount = if self.test_force_remove_planning_zero_refcount {
                0
            } else {
                refcount
            };
            if refcount == 0 {
                return RemovePlanOutcome::PlanningInvariantViolation;
            }
            old_refcounts.push((*pubkey, refcount));
            if refcount == 1 {
                physical_removed.push(*pubkey);
            }
        }

        let removed_count = {
            #[cfg(test)]
            if self.test_force_remove_planning_underflow {
                pre_len.saturating_add(1)
            } else {
                physical_removed.len()
            }
            #[cfg(not(test))]
            {
                physical_removed.len()
            }
        };

        let Some(projected_final_len) = pre_len.checked_sub(removed_count) else {
            return RemovePlanOutcome::PlanningInvariantViolation;
        };

        RemovePlanOutcome::Ready(RemovePlan {
            pre_len,
            projected_final_len,
            old_normalized: old_normalized.to_vec(),
            physical_removed,
            old_refcounts,
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

    fn rollback_failed_replace(
        &mut self,
        owner: &ExplicitOwner,
        plan: &ReplacePlan,
        pre_touch_stamp: u64,
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
            self.owner_lru.insert(owner.clone(), pre_touch_stamp);
            if self.owner_group(owner) == Some(restore_pubkeys.as_slice())
                && self.last_touch(owner) == Some(pre_touch_stamp)
            {
                self.physical_len = plan.pre_len;
                return FixedCapReplaceResult::InternalInvariantViolation {
                    recovery: InvariantViolationRecovery::PreviousRestored,
                };
            }
            self.fail_closed_remove_owner_after_replace(owner)
        } else {
            self.fail_closed_remove_owner_after_replace(owner)
        }
    }

    fn fail_closed_remove_owner_after_replace(
        &mut self,
        owner: &ExplicitOwner,
    ) -> FixedCapReplaceResult {
        let _ = self.ownership.remove_group(owner);
        while self.ownership.owner_group(owner).is_some() {
            if self.ownership.remove_group(owner).is_none() {
                break;
            }
        }
        self.owner_lru.remove(owner);
        self.reconcile_cold();
        FixedCapReplaceResult::InternalInvariantViolation {
            recovery: InvariantViolationRecovery::OwnerRemovedFailClosed,
        }
    }

    fn rollback_failed_remove(
        &mut self,
        owner: &ExplicitOwner,
        plan: &RemovePlan,
        pre_touch_stamp: u64,
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
            self.owner_lru.insert(owner.clone(), pre_touch_stamp);
            if self.owner_group(owner) == Some(restore_pubkeys.as_slice())
                && self.last_touch(owner) == Some(pre_touch_stamp)
            {
                self.physical_len = plan.pre_len;
                return FixedCapRemoveResult::InternalInvariantViolation {
                    recovery: FixedCapRemoveRecovery::PreviousRestored,
                };
            }
            self.fail_closed_remove_owner_after_remove(owner)
        } else {
            self.fail_closed_remove_owner_after_remove(owner)
        }
    }

    fn fail_closed_remove_owner_after_remove(
        &mut self,
        owner: &ExplicitOwner,
    ) -> FixedCapRemoveResult {
        let _ = self.ownership.remove_group(owner);
        if self.ownership.owner_group(owner).is_some() {
            self.physical_len = self.ownership.len();
            return FixedCapRemoveResult::InternalInvariantViolation {
                recovery: FixedCapRemoveRecovery::RecoveryFailed,
            };
        }
        self.owner_lru.remove(owner);
        self.reconcile_cold();
        FixedCapRemoveResult::InternalInvariantViolation {
            recovery: FixedCapRemoveRecovery::OwnerRemovedFailClosed,
        }
    }

    fn fail_closed_remove_owner_stamp_mismatch(&mut self, owner: &ExplicitOwner) {
        let _ = self.ownership.remove_group(owner);
        while self.ownership.owner_group(owner).is_some() {
            if self.ownership.remove_group(owner).is_none() {
                break;
            }
        }
        self.owner_lru.remove(owner);
        self.reconcile_cold();
    }

    fn apply_stamp_plan(&mut self, plan: StampPlan) {
        self.owner_lru = plan.owner_lru;
        self.next_touch_stamp = plan.next_touch_stamp;
    }

    fn stamp_plan_force_failure(&self) -> bool {
        #[cfg(test)]
        {
            self.test_force_stamp_plan_failure
        }
        #[cfg(not(test))]
        {
            false
        }
    }

    fn evicting_corrupt_stamp(&self) -> Option<&ExplicitOwner> {
        #[cfg(test)]
        {
            self.test_force_evicting_corrupt_survivor_stamp.as_ref()
        }
        #[cfg(not(test))]
        {
            None
        }
    }

    /// Cold fail-closed recovery: reconcile owner/stamp parity, then repair `physical_len`.
    fn reconcile_cold(&mut self) {
        self.reconcile_lru_cold();
        self.physical_len = self.ownership.len();
    }

    fn reconcile_physical_len_cold(&mut self) {
        self.reconcile_cold();
    }

    fn reconcile_lru_cold(&mut self) {
        let ownership_owners: BTreeSet<ExplicitOwner> = self
            .ownership
            .snapshot_owner_groups()
            .into_iter()
            .map(|group| ExplicitOwner {
                consumer: group.consumer,
                owner_key: group.owner_key,
            })
            .collect();

        self.owner_lru
            .retain(|owner, _| ownership_owners.contains(owner));

        let owners_missing_stamp: Vec<ExplicitOwner> = ownership_owners
            .iter()
            .filter(|owner| !self.owner_lru.contains_key(owner))
            .cloned()
            .collect();

        for owner in owners_missing_stamp {
            let _ = self.ownership.remove_group(&owner);
            while self.ownership.owner_group(&owner).is_some() {
                if self.ownership.remove_group(&owner).is_none() {
                    break;
                }
            }
        }
    }
}

fn required_existing_stamp(
    owner_lru: &BTreeMap<ExplicitOwner, u64>,
    owner: &ExplicitOwner,
) -> Result<u64, StampPlanError> {
    owner_lru
        .get(owner)
        .copied()
        .ok_or(StampPlanError::MissingOwnerStamp)
}

fn renormalize_stamps_fallible(
    owner_lru: &BTreeMap<ExplicitOwner, u64>,
) -> Result<BTreeMap<ExplicitOwner, u64>, StampPlanError> {
    if owner_lru.is_empty() {
        return Ok(BTreeMap::new());
    }
    let mut entries: Vec<(ExplicitOwner, u64)> = owner_lru
        .iter()
        .map(|(owner, &stamp)| (owner.clone(), stamp))
        .collect();
    entries.sort_by(|(owner_a, stamp_a), (owner_b, stamp_b)| {
        stamp_a.cmp(stamp_b).then_with(|| owner_a.cmp(owner_b))
    });
    let mut renormalized = BTreeMap::new();
    for (idx, (owner, _)) in entries.into_iter().enumerate() {
        let rank = u64::try_from(idx).map_err(|_| StampPlanError::Overflow)?;
        let stamp = rank.checked_add(1).ok_or(StampPlanError::Overflow)?;
        renormalized.insert(owner, stamp);
    }
    Ok(renormalized)
}

fn prepare_stamp_base(
    owner_lru: &BTreeMap<ExplicitOwner, u64>,
    next_touch_stamp: u64,
) -> Result<(BTreeMap<ExplicitOwner, u64>, u64), StampPlanError> {
    if next_touch_stamp == u64::MAX {
        let map = renormalize_stamps_fallible(owner_lru)?;
        let len = map.len();
        let base = u64::try_from(len).map_err(|_| StampPlanError::Overflow)?;
        let assign = base.checked_add(1).ok_or(StampPlanError::Overflow)?;
        Ok((map, assign))
    } else {
        Ok((owner_lru.clone(), next_touch_stamp))
    }
}

fn plan_stamp_assign(
    owner_lru: &BTreeMap<ExplicitOwner, u64>,
    next_touch_stamp: u64,
    owner: &ExplicitOwner,
    force_failure: bool,
) -> Result<StampPlan, StampPlanError> {
    if force_failure {
        return Err(StampPlanError::ForcedFailure);
    }
    let (mut map, assign_stamp) = prepare_stamp_base(owner_lru, next_touch_stamp)?;
    let new_next = assign_stamp
        .checked_add(1)
        .ok_or(StampPlanError::Overflow)?;
    map.insert(owner.clone(), assign_stamp);
    Ok(StampPlan {
        owner_lru: map,
        next_touch_stamp: new_next,
    })
}

fn plan_stamp_remove(
    owner_lru: &BTreeMap<ExplicitOwner, u64>,
    next_touch_stamp: u64,
    owner: &ExplicitOwner,
) -> StampPlan {
    let mut map = owner_lru.clone();
    map.remove(owner);
    StampPlan {
        owner_lru: map,
        next_touch_stamp,
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
    plan: &RemovePlan,
    expected_physical_removed: &[Pubkey],
    ownership: &ExplicitOwnership,
) -> bool {
    let Some(snapshot) = snapshot else {
        return false;
    };
    if snapshot.consumer != owner.consumer || snapshot.owner_key != owner.owner_key {
        return false;
    }
    if snapshot.pubkeys != plan.old_normalized {
        return false;
    }
    if ownership.owner_group(owner).is_some() {
        return false;
    }

    for &(pubkey, old_rc) in &plan.old_refcounts {
        let new_rc = ownership.owner_refcount(&pubkey);
        let Some(expected_rc) = old_rc.checked_sub(1) else {
            return false;
        };
        if new_rc != expected_rc {
            return false;
        }
    }

    let mut planned_physical_removed: Vec<Pubkey> = plan
        .old_refcounts
        .iter()
        .filter(|(_, old_rc)| *old_rc == 1)
        .map(|(pubkey, _)| *pubkey)
        .collect();
    planned_physical_removed.sort();
    let mut expected = expected_physical_removed.to_vec();
    expected.sort();
    planned_physical_removed == expected
}

fn normalize_pubkeys(pubkeys: impl IntoIterator<Item = Pubkey>) -> Vec<Pubkey> {
    let mut keys: Vec<Pubkey> = pubkeys.into_iter().collect();
    keys.sort();
    keys.dedup();
    keys
}

fn finalize_evicting_candidate_lru(
    mut owner_lru: BTreeMap<ExplicitOwner, u64>,
    corrupt_owner: Option<&ExplicitOwner>,
) -> BTreeMap<ExplicitOwner, u64> {
    if let Some(owner) = corrupt_owner {
        if let Some(stamp) = owner_lru.get_mut(owner) {
            *stamp = stamp.saturating_sub(1);
        }
    }
    owner_lru
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

        for step in 0..16 {
            let owner = owners[step % owners.len()].clone();
            match step % 3 {
                0 => {
                    if !model.groups.contains_key(&owner) {
                        continue;
                    }
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
                1 => {
                    if admission.owner_group(&owner).is_none() {
                        continue;
                    }
                    let result = admission.remove_group(owner.clone());
                    let expected = model.remove_group(owner.clone());
                    assert_eq!(result, expected);
                    assert_replace_state_matches(&admission, &model);
                }
                _ => {
                    if admission.owner_group(&owner).is_some() {
                        continue;
                    }
                    let keys: Vec<Pubkey> = key_pool
                        .iter()
                        .copied()
                        .filter(|k| (k.to_bytes()[0] as usize + step) % 4 == 0)
                        .take(1 + step % 2)
                        .collect();
                    if keys.is_empty() {
                        continue;
                    }
                    let before = capture_admission_snapshot(&admission);
                    let admit = admission.try_admit_new_group(owner.clone(), keys.clone());
                    if matches!(
                        admit,
                        FixedCapAdmissionResult::RejectedCap { .. }
                            | FixedCapAdmissionResult::RejectedInvalidGroup
                            | FixedCapAdmissionResult::RejectedExistingOwner
                    ) {
                        assert_public_snapshots_equal(
                            &capture_admission_snapshot(&admission),
                            &before,
                        );
                        continue;
                    }
                    assert_admitted(admit);
                    model.admit_for_setup(owner.clone(), keys);
                    assert_replace_state_matches(&admission, &model);
                }
            }
        }
    }

    #[test]
    fn remove_planning_underflow_aborts_without_mutation() {
        let mut admission = FixedCapAdmission::new(4);
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        assert_admitted(admission.try_admit_new_group(owner.clone(), [pk(1), pk(2)]));
        let before = capture_admission_snapshot(&admission);
        let before_len = admission.len();

        admission.set_test_force_remove_planning_underflow(true);
        assert_eq!(
            admission.remove_group(owner.clone()),
            FixedCapRemoveResult::PlanningInvariantViolation
        );
        assert_public_snapshots_equal(&capture_admission_snapshot(&admission), &before);
        assert_eq!(admission.len(), before_len);
        assert_eq!(
            admission.owner_group(&owner),
            Some([pk(1), pk(2)].as_slice())
        );
    }

    #[test]
    fn remove_planning_zero_refcount_aborts_without_mutation() {
        let mut admission = FixedCapAdmission::new(4);
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        assert_admitted(admission.try_admit_new_group(owner.clone(), [pk(1), pk(2)]));
        let before = capture_admission_snapshot(&admission);
        let before_len = admission.len();

        admission.set_test_force_remove_planning_zero_refcount(true);
        assert_eq!(
            admission.remove_group(owner.clone()),
            FixedCapRemoveResult::PlanningInvariantViolation
        );
        assert_public_snapshots_equal(&capture_admission_snapshot(&admission), &before);
        assert_eq!(admission.len(), before_len);
        assert_eq!(
            admission.owner_group(&owner),
            Some([pk(1), pk(2)].as_slice())
        );
    }

    #[test]
    fn remove_planning_stats_are_group_local_under_large_background() {
        let mut admission = FixedCapAdmission::new(100);
        for seed in 10u8..90 {
            let bg_owner = pool_owner(ExplicitConsumer::Tracker, seed);
            assert_admitted(admission.try_admit_new_group(bg_owner, [pk(seed)]));
        }
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        let target_keys = [pk(1), pk(2), pk(3)];
        assert_admitted(admission.try_admit_new_group(owner.clone(), target_keys));
        admission.planning_stats.refcount_lookups = 0;
        admission.planning_stats.owner_group_lookups = 0;

        let _ = admission.remove_group(owner);
        assert_eq!(admission.planning_stats().owner_group_lookups, 1);
        assert_eq!(admission.planning_stats().refcount_lookups, 3);
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
                recovery: FixedCapRemoveRecovery::PreviousRestored,
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
                recovery: FixedCapRemoveRecovery::OwnerRemovedFailClosed,
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

    fn assert_lru_matches_ownership(admission: &FixedCapAdmission) {
        let owners: BTreeSet<ExplicitOwner> = admission
            .snapshot_owner_groups()
            .into_iter()
            .map(|group| ExplicitOwner {
                consumer: group.consumer,
                owner_key: group.owner_key,
            })
            .collect();
        let lru_owners: BTreeSet<ExplicitOwner> = admission
            .snapshot_lru_entries()
            .into_iter()
            .map(|entry| entry.owner)
            .collect();
        assert_eq!(lru_owners, owners);
        admission
            .test_build_eviction_snapshot()
            .expect("LRU snapshot must build eviction planning snapshot");
    }

    /// Independent LRU stamp model — separate monotonic logic from production.
    #[derive(Debug, Clone)]
    struct RefLruModel {
        stamps: BTreeMap<ExplicitOwner, u64>,
        next_stamp: u64,
    }

    impl RefLruModel {
        fn new() -> Self {
            Self {
                stamps: BTreeMap::new(),
                next_stamp: 1,
            }
        }

        fn next_counter(&self) -> u64 {
            self.next_stamp
        }

        fn on_successful_insert(&mut self, owner: &ExplicitOwner) {
            self.assign_stamp(owner);
        }

        fn on_successful_replace(&mut self, owner: &ExplicitOwner) {
            self.assign_stamp(owner);
        }

        fn on_successful_remove(&mut self, owner: &ExplicitOwner) {
            self.stamps.remove(owner);
        }

        fn on_touch(&mut self, owner: &ExplicitOwner) {
            self.assign_stamp(owner);
        }

        fn restore_stamp(&mut self, owner: &ExplicitOwner, stamp: u64) {
            self.stamps.insert(owner.clone(), stamp);
            self.next_stamp = self.next_stamp.max(stamp.saturating_add(1));
        }

        fn assign_stamp(&mut self, owner: &ExplicitOwner) {
            if self.next_stamp == u64::MAX {
                self.compact_from_stamp_order();
            }
            let stamp = self.next_stamp;
            self.next_stamp = self.next_stamp.saturating_add(1);
            self.stamps.insert(owner.clone(), stamp);
        }

        fn compact_from_stamp_order(&mut self) {
            let mut ordered: Vec<(ExplicitOwner, u64)> = self
                .stamps
                .iter()
                .map(|(owner, stamp)| (owner.clone(), *stamp))
                .collect();
            ordered.sort_by(|(owner_a, stamp_a), (owner_b, stamp_b)| {
                stamp_a.cmp(stamp_b).then_with(|| owner_a.cmp(owner_b))
            });
            self.stamps.clear();
            for (idx, (owner, _)) in ordered.into_iter().enumerate() {
                if let Ok(rank) = u64::try_from(idx) {
                    if let Some(stamp) = rank.checked_add(1) {
                        self.stamps.insert(owner, stamp);
                    }
                }
            }
            self.next_stamp = self
                .stamps
                .values()
                .copied()
                .max()
                .and_then(|max| max.checked_add(1))
                .unwrap_or(1);
        }

        fn snapshot(&self) -> BTreeMap<ExplicitOwner, u64> {
            self.stamps.clone()
        }

        fn ordered_owner_stamps(&self) -> Vec<(u64, ExplicitOwner)> {
            let mut ordered: Vec<(u64, ExplicitOwner)> = self
                .stamps
                .iter()
                .map(|(owner, stamp)| (*stamp, owner.clone()))
                .collect();
            ordered.sort_by(|(stamp_a, owner_a), (stamp_b, owner_b)| {
                stamp_a.cmp(stamp_b).then_with(|| owner_a.cmp(owner_b))
            });
            ordered
        }
    }

    fn assert_lru_matches_model(admission: &FixedCapAdmission, model: &RefLruModel) {
        assert_eq!(admission.test_owner_stamps(), model.snapshot());
        assert_eq!(admission.test_next_touch_stamp(), model.next_counter());
        assert_lru_matches_ownership(admission);
    }

    // --- LRU metadata tests (PR 1c2a) ---

    #[test]
    fn lru_inserts_receive_monotone_stamps() {
        let mut admission = FixedCapAdmission::new(8);
        let mut model = RefLruModel::new();
        let owners = [
            pool_owner(ExplicitConsumer::Momentum, 1),
            pool_owner(ExplicitConsumer::Arb, 2),
            pool_owner(ExplicitConsumer::Tracker, 3),
        ];

        let mut prev = 0u64;
        for owner in &owners {
            assert_admitted(admission.try_admit_new_group(owner.clone(), [pk(1)]));
            model.on_successful_insert(owner);
            let stamp = admission.last_touch(owner).unwrap();
            assert!(stamp > prev);
            prev = stamp;
            assert_lru_matches_model(&admission, &model);
        }
    }

    #[test]
    fn lru_shared_owner_groups_remain_separate_owners() {
        let mut admission = FixedCapAdmission::new(4);
        let mut model = RefLruModel::new();
        let shared = pk(1);
        let first = pool_owner(ExplicitConsumer::Momentum, 1);
        let second = pool_owner(ExplicitConsumer::Arb, 2);
        assert_admitted(admission.try_admit_new_group(first.clone(), [shared]));
        model.on_successful_insert(&first);
        assert_admitted(admission.try_admit_new_group(second.clone(), [shared]));
        model.on_successful_insert(&second);

        let stamp_a = admission.last_touch(&first).unwrap();
        let stamp_b = admission.last_touch(&second).unwrap();
        assert_ne!(stamp_a, stamp_b);
        assert_eq!(admission.snapshot_lru_entries().len(), 2);
        assert_lru_matches_model(&admission, &model);
    }

    #[test]
    fn lru_replace_touches_unchanged_does_not() {
        let mut admission = FixedCapAdmission::new(4);
        let mut model = RefLruModel::new();
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        assert_admitted(admission.try_admit_new_group(owner.clone(), [pk(1), pk(2)]));
        model.on_successful_insert(&owner);
        let before = admission.last_touch(&owner).unwrap();

        assert_eq!(
            admission.try_replace_group(owner.clone(), [pk(2), pk(3)]),
            FixedCapReplaceResult::Replaced {
                physical_added: vec![pk(3)],
                physical_removed: vec![pk(1)],
            }
        );
        model.on_successful_replace(&owner);
        assert!(admission.last_touch(&owner).unwrap() > before);

        let after_replace = admission.last_touch(&owner).unwrap();
        assert_eq!(
            admission.try_replace_group(owner.clone(), [pk(2), pk(3)]),
            FixedCapReplaceResult::Unchanged
        );
        assert_eq!(admission.last_touch(&owner).unwrap(), after_replace);
        assert_lru_matches_model(&admission, &model);
    }

    #[test]
    fn lru_explicit_touch_changes_only_target_owner() {
        let mut admission = FixedCapAdmission::new(4);
        let mut model = RefLruModel::new();
        let owner_a = pool_owner(ExplicitConsumer::Momentum, 1);
        let owner_b = pool_owner(ExplicitConsumer::Arb, 2);
        assert_admitted(admission.try_admit_new_group(owner_a.clone(), [pk(1)]));
        model.on_successful_insert(&owner_a);
        assert_admitted(admission.try_admit_new_group(owner_b.clone(), [pk(2)]));
        model.on_successful_insert(&owner_b);
        let stamp_b_before = admission.last_touch(&owner_b).unwrap();

        assert_eq!(admission.touch_group(owner_a.clone()), TouchResult::Touched);
        model.on_touch(&owner_a);
        assert!(admission.last_touch(&owner_a).unwrap() > stamp_b_before);
        assert_eq!(admission.last_touch(&owner_b).unwrap(), stamp_b_before);
        assert_lru_matches_model(&admission, &model);
    }

    #[test]
    fn lru_unknown_touch_is_full_no_op() {
        let mut admission = FixedCapAdmission::new(4);
        let mut model = RefLruModel::new();
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        assert_admitted(admission.try_admit_new_group(owner.clone(), [pk(1)]));
        model.on_successful_insert(&owner);
        let before = capture_admission_snapshot(&admission);
        let lru_before = admission.snapshot_lru_entries();

        assert_eq!(
            admission.touch_group(pool_owner(ExplicitConsumer::Arb, 9)),
            TouchResult::NotFound
        );
        assert_public_snapshots_equal(&capture_admission_snapshot(&admission), &before);
        assert_eq!(admission.snapshot_lru_entries(), lru_before);
        assert_lru_matches_model(&admission, &model);
    }

    #[test]
    fn lru_remove_deletes_stamp() {
        let mut admission = FixedCapAdmission::new(4);
        let mut model = RefLruModel::new();
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        assert_admitted(admission.try_admit_new_group(owner.clone(), [pk(1)]));
        model.on_successful_insert(&owner);
        assert!(admission.last_touch(&owner).is_some());

        assert_matches_remove(admission.remove_group(owner.clone()));
        model.on_successful_remove(&owner);
        assert!(admission.last_touch(&owner).is_none());
        assert!(admission.snapshot_lru_entries().is_empty());
        assert_lru_matches_model(&admission, &model);
    }

    #[test]
    fn lru_reject_paths_consume_no_stamp() {
        let mut admission = FixedCapAdmission::new(2);
        let mut model = RefLruModel::new();
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        assert_admitted(admission.try_admit_new_group(owner.clone(), [pk(1)]));
        model.on_successful_insert(&owner);
        let stamp_before = admission.last_touch(&owner).unwrap();
        let counter_before = admission.test_next_touch_stamp();

        assert_eq!(
            admission.try_admit_new_group(owner.clone(), [pk(2)]),
            FixedCapAdmissionResult::RejectedExistingOwner
        );
        assert_eq!(
            admission.try_replace_group(owner.clone(), []),
            FixedCapReplaceResult::RejectedInvalidGroup
        );
        assert_eq!(
            admission.try_replace_group(pool_owner(ExplicitConsumer::Arb, 9), [pk(1)]),
            FixedCapReplaceResult::RejectedMissingOwner
        );
        assert_eq!(
            admission.try_replace_group(owner.clone(), [pk(1), pk(2), pk(3)]),
            FixedCapReplaceResult::RejectedCap {
                required_unique: 2,
                available_unique: 1,
            }
        );
        assert_eq!(admission.last_touch(&owner).unwrap(), stamp_before);
        assert_eq!(admission.snapshot_lru_entries().len(), 1);
        assert_eq!(admission.test_next_touch_stamp(), counter_before);

        let other = pool_owner(ExplicitConsumer::Arb, 2);
        assert_admitted(admission.try_admit_new_group(other.clone(), [pk(2)]));
        model.on_successful_insert(&other);
        assert_lru_matches_model(&admission, &model);

        assert_eq!(admission.touch_group(owner.clone()), TouchResult::Touched);
        model.on_touch(&owner);
        assert_lru_matches_model(&admission, &model);
    }

    #[test]
    fn lru_stamp_plan_failure_is_mutation_free() {
        let mut admission = FixedCapAdmission::new(4);
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        assert_admitted(admission.try_admit_new_group(owner.clone(), [pk(1)]));
        let before = capture_admission_snapshot(&admission);
        let counter_before = admission.test_next_touch_stamp();
        let stamps_before = admission.test_owner_stamps();

        admission.set_test_force_stamp_plan_failure(true);
        assert_eq!(
            admission.try_admit_new_group(pool_owner(ExplicitConsumer::Arb, 2), [pk(2)]),
            FixedCapAdmissionResult::InternalInvariantViolation
        );
        assert_eq!(
            admission.try_replace_group(owner.clone(), [pk(2)]),
            FixedCapReplaceResult::PlanningInvariantViolation
        );
        assert_eq!(
            admission.touch_group(owner.clone()),
            TouchResult::InternalInvariantViolation
        );
        admission.set_test_force_stamp_plan_failure(false);

        assert_public_snapshots_equal(&capture_admission_snapshot(&admission), &before);
        assert_eq!(admission.test_owner_stamps(), stamps_before);
        assert_eq!(admission.test_next_touch_stamp(), counter_before);
        assert_lru_matches_ownership(&admission);
    }

    #[test]
    fn lru_replace_missing_stamp_before_unchanged_fail_closed() {
        let mut admission = FixedCapAdmission::new(4);
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        assert_admitted(admission.try_admit_new_group(owner.clone(), [pk(1), pk(2)]));
        let stamp_before = admission.test_next_touch_stamp();
        admission.owner_lru.remove(&owner);

        assert_eq!(
            admission.try_replace_group(owner.clone(), [pk(1), pk(2)]),
            FixedCapReplaceResult::InternalInvariantViolation {
                recovery: InvariantViolationRecovery::OwnerRemovedFailClosed,
            }
        );
        assert!(admission.owner_group(&owner).is_none());
        assert_eq!(admission.test_next_touch_stamp(), stamp_before);
    }

    #[test]
    fn lru_replace_unchanged_with_valid_stamp_consumes_no_stamp() {
        let mut admission = FixedCapAdmission::new(4);
        let mut model = RefLruModel::new();
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        assert_admitted(admission.try_admit_new_group(owner.clone(), [pk(1), pk(2)]));
        model.on_successful_insert(&owner);
        let stamp = admission.last_touch(&owner).unwrap();
        let counter = admission.test_next_touch_stamp();

        assert_eq!(
            admission.try_replace_group(owner.clone(), [pk(2), pk(1)]),
            FixedCapReplaceResult::Unchanged
        );
        assert_eq!(admission.last_touch(&owner).unwrap(), stamp);
        assert_eq!(admission.test_next_touch_stamp(), counter);
        assert_lru_matches_model(&admission, &model);
    }

    #[test]
    fn lru_missing_stamp_fail_closed_before_replace_remove_touch() {
        let mut admission = FixedCapAdmission::new(4);
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        assert_admitted(admission.try_admit_new_group(owner.clone(), [pk(1), pk(2)]));
        admission.owner_lru.remove(&owner);

        assert_eq!(
            admission.try_replace_group(owner.clone(), [pk(2), pk(3)]),
            FixedCapReplaceResult::InternalInvariantViolation {
                recovery: InvariantViolationRecovery::OwnerRemovedFailClosed,
            }
        );
        assert!(admission.owner_group(&owner).is_none());
        assert!(admission.last_touch(&owner).is_none());
        assert_lru_matches_ownership(&admission);

        assert_admitted(admission.try_admit_new_group(owner.clone(), [pk(1)]));
        admission.owner_lru.remove(&owner);
        assert_eq!(
            admission.remove_group(owner.clone()),
            FixedCapRemoveResult::InternalInvariantViolation {
                recovery: FixedCapRemoveRecovery::OwnerRemovedFailClosed,
            }
        );
        assert!(admission.owner_group(&owner).is_none());
        assert!(admission.last_touch(&owner).is_none());
        assert_eq!(admission.len(), 0);
        assert!(admission.snapshot_lru_entries().is_empty());
        assert_lru_matches_ownership(&admission);

        assert_admitted(admission.try_admit_new_group(owner.clone(), [pk(1)]));
        admission.owner_lru.remove(&owner);
        assert_eq!(
            admission.touch_group(owner.clone()),
            TouchResult::InternalInvariantViolation
        );
        assert!(admission.owner_group(&owner).is_none());
        assert_lru_matches_ownership(&admission);
    }

    #[test]
    fn lru_recovery_branches_build_eviction_snapshot() {
        let mut admission = FixedCapAdmission::new(4);
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        let other = pool_owner(ExplicitConsumer::Arb, 2);
        assert_admitted(admission.try_admit_new_group(owner.clone(), [pk(1), pk(2)]));
        assert_admitted(admission.try_admit_new_group(other.clone(), [pk(3)]));

        admission.set_test_force_commit_plan_mismatch(true);
        assert_eq!(
            admission.try_replace_group(owner.clone(), [pk(2), pk(4)]),
            FixedCapReplaceResult::InternalInvariantViolation {
                recovery: InvariantViolationRecovery::PreviousRestored,
            }
        );
        assert_lru_matches_ownership(&admission);
        admission.set_test_force_commit_plan_mismatch(false);

        admission.set_test_force_commit_plan_mismatch(true);
        admission.set_test_force_restore_plan_mismatch(true);
        assert_eq!(
            admission.try_replace_group(owner.clone(), [pk(2), pk(5)]),
            FixedCapReplaceResult::InternalInvariantViolation {
                recovery: InvariantViolationRecovery::OwnerRemovedFailClosed,
            }
        );
        assert_lru_matches_ownership(&admission);
        admission.set_test_force_commit_plan_mismatch(false);
        admission.set_test_force_restore_plan_mismatch(false);

        assert_admitted(admission.try_admit_new_group(owner.clone(), [pk(1)]));
        admission.set_test_force_commit_plan_mismatch(true);
        assert_eq!(
            admission.remove_group(owner.clone()),
            FixedCapRemoveResult::InternalInvariantViolation {
                recovery: FixedCapRemoveRecovery::PreviousRestored,
            }
        );
        assert_lru_matches_ownership(&admission);
        admission.set_test_force_commit_plan_mismatch(false);
    }

    #[test]
    fn lru_replace_previous_restored_restores_old_stamp() {
        let mut admission = FixedCapAdmission::new(4);
        let mut model = RefLruModel::new();
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        assert_admitted(admission.try_admit_new_group(owner.clone(), [pk(1), pk(2)]));
        model.on_successful_insert(&owner);
        let old_stamp = admission.last_touch(&owner).unwrap();

        admission.set_test_force_commit_plan_mismatch(true);
        assert_eq!(
            admission.try_replace_group(owner.clone(), [pk(2), pk(3)]),
            FixedCapReplaceResult::InternalInvariantViolation {
                recovery: InvariantViolationRecovery::PreviousRestored,
            }
        );
        model.restore_stamp(&owner, old_stamp);
        assert_eq!(admission.last_touch(&owner).unwrap(), old_stamp);
        assert_lru_matches_model(&admission, &model);
        assert_lru_matches_ownership(&admission);
    }

    #[test]
    fn lru_fail_closed_removes_stamp() {
        let mut admission = FixedCapAdmission::new(4);
        let mut model = RefLruModel::new();
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        let other = pool_owner(ExplicitConsumer::Arb, 2);
        assert_admitted(admission.try_admit_new_group(owner.clone(), [pk(1), pk(2)]));
        model.on_successful_insert(&owner);
        assert_admitted(admission.try_admit_new_group(other.clone(), [pk(3)]));
        model.on_successful_insert(&other);

        admission.set_test_force_commit_plan_mismatch(true);
        admission.set_test_force_restore_plan_mismatch(true);
        assert_eq!(
            admission.try_replace_group(owner.clone(), [pk(2), pk(4)]),
            FixedCapReplaceResult::InternalInvariantViolation {
                recovery: InvariantViolationRecovery::OwnerRemovedFailClosed,
            }
        );
        model.on_successful_remove(&owner);
        assert!(admission.last_touch(&owner).is_none());
        assert_lru_matches_model(&admission, &model);

        admission.set_test_force_commit_plan_mismatch(true);
        admission.set_test_force_restore_plan_mismatch(true);
        let remove_owner = other.clone();
        assert_eq!(
            admission.remove_group(remove_owner.clone()),
            FixedCapRemoveResult::InternalInvariantViolation {
                recovery: FixedCapRemoveRecovery::OwnerRemovedFailClosed,
            }
        );
        model.on_successful_remove(&remove_owner);
        assert!(admission.last_touch(&remove_owner).is_none());
        assert_lru_matches_model(&admission, &model);
    }

    #[test]
    fn lru_snapshot_parity_after_long_sequences() {
        let mut admission = FixedCapAdmission::new(6);
        let mut model = RefLruModel::new();
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
            assert_admitted(admission.try_admit_new_group(owner.clone(), keys));
            model.on_successful_insert(owner);
            assert_lru_matches_model(&admission, &model);
        }

        for step in 0..24 {
            let owner = owners[step % owners.len()].clone();
            match step % 4 {
                0 => {
                    if admission.owner_group(&owner).is_none() {
                        continue;
                    }
                    let keys: Vec<Pubkey> = key_pool
                        .iter()
                        .copied()
                        .filter(|k| (k.to_bytes()[0] as usize + step) % 3 != 0)
                        .take(1 + step % 3)
                        .collect();
                    if keys.is_empty() {
                        continue;
                    }
                    let result = admission.try_replace_group(owner.clone(), keys);
                    if matches!(result, FixedCapReplaceResult::Replaced { .. }) {
                        model.on_successful_replace(&owner);
                    }
                }
                1 => {
                    if admission.owner_group(&owner).is_none() {
                        continue;
                    }
                    let result = admission.remove_group(owner.clone());
                    if matches!(result, FixedCapRemoveResult::Removed { .. }) {
                        model.on_successful_remove(&owner);
                    }
                }
                2 => {
                    if admission.owner_group(&owner).is_some()
                        && admission.touch_group(owner.clone()) == TouchResult::Touched
                    {
                        model.on_touch(&owner);
                    }
                }
                _ => {
                    if admission.owner_group(&owner).is_some() {
                        continue;
                    }
                    let keys: Vec<Pubkey> = key_pool
                        .iter()
                        .copied()
                        .filter(|k| (k.to_bytes()[0] as usize + step) % 4 == 0)
                        .take(1)
                        .collect();
                    if keys.is_empty() {
                        continue;
                    }
                    let result = admission.try_admit_new_group(owner.clone(), keys);
                    if matches!(
                        result,
                        FixedCapAdmissionResult::Inserted { .. }
                            | FixedCapAdmissionResult::OwnerAddedNoNewPubkey
                    ) {
                        model.on_successful_insert(&owner);
                    }
                }
            }
            assert_lru_matches_model(&admission, &model);
        }
    }

    #[test]
    fn lru_snapshot_sorted_by_owner() {
        let mut admission = FixedCapAdmission::new(8);
        let owners = [
            pool_owner(ExplicitConsumer::Tracker, 3),
            pool_owner(ExplicitConsumer::Momentum, 1),
            pool_owner(ExplicitConsumer::Arb, 2),
        ];
        for owner in &owners {
            assert_admitted(admission.try_admit_new_group(owner.clone(), [pk(1)]));
        }
        let snapshot = admission.snapshot_lru_entries();
        let mut sorted = snapshot.clone();
        sorted.sort_by(|a, b| a.owner.cmp(&b.owner));
        assert_eq!(snapshot, sorted);
    }

    #[test]
    fn lru_near_max_renormalization_preserves_stamp_owner_order() {
        let mut admission = FixedCapAdmission::new(16);
        let mut model = RefLruModel::new();
        let owners: Vec<ExplicitOwner> = (0u8..4)
            .map(|seed| pool_owner(ExplicitConsumer::Momentum, seed))
            .collect();
        for owner in &owners {
            assert_admitted(
                admission.try_admit_new_group(owner.clone(), [pk(owner_key_hash_seed(owner))]),
            );
            model.on_successful_insert(owner);
        }
        admission.set_test_next_touch_stamp(u64::MAX - 1);
        admission.set_test_owner_stamp(owners[0].clone(), u64::MAX - 2);
        admission.set_test_owner_stamp(owners[1].clone(), u64::MAX - 2);
        admission.set_test_owner_stamp(owners[2].clone(), u64::MAX - 1);
        admission.set_test_owner_stamp(owners[3].clone(), u64::MAX - 1);
        model.stamps = admission.test_owner_stamps();
        model.next_stamp = admission.test_next_touch_stamp();

        let before_sorted = model.ordered_owner_stamps();

        admission.test_renormalize_lru_stamps();
        model.compact_from_stamp_order();

        let after_sorted = model.ordered_owner_stamps();
        let before_owners: Vec<ExplicitOwner> =
            before_sorted.into_iter().map(|(_, owner)| owner).collect();
        let after_owners: Vec<ExplicitOwner> =
            after_sorted.into_iter().map(|(_, owner)| owner).collect();
        assert_eq!(before_owners, after_owners);
        assert_lru_matches_model(&admission, &model);

        assert_eq!(
            admission.touch_group(owners[0].clone()),
            TouchResult::Touched
        );
        model.on_touch(&owners[0]);
        assert_lru_matches_model(&admission, &model);

        assert_eq!(
            admission.touch_group(owners[2].clone()),
            TouchResult::Touched
        );
        model.on_touch(&owners[2]);
        assert_lru_matches_model(&admission, &model);
    }

    #[test]
    fn lru_touches_remain_monotone_after_renormalization() {
        let mut admission = FixedCapAdmission::new(8);
        let owner_a = pool_owner(ExplicitConsumer::Momentum, 1);
        let owner_b = pool_owner(ExplicitConsumer::Arb, 2);
        assert_admitted(admission.try_admit_new_group(owner_a.clone(), [pk(1)]));
        assert_admitted(admission.try_admit_new_group(owner_b.clone(), [pk(2)]));
        admission.set_test_next_touch_stamp(u64::MAX);
        admission.set_test_owner_stamp(owner_a.clone(), u64::MAX - 1);
        admission.set_test_owner_stamp(owner_b.clone(), u64::MAX);

        assert_eq!(admission.touch_group(owner_a.clone()), TouchResult::Touched);
        let first = admission.last_touch(&owner_a).unwrap();
        assert_eq!(admission.touch_group(owner_b.clone()), TouchResult::Touched);
        let second = admission.last_touch(&owner_b).unwrap();
        assert!(second > first);
        assert_eq!(admission.touch_group(owner_a.clone()), TouchResult::Touched);
        let third = admission.last_touch(&owner_a).unwrap();
        assert!(third > second);
    }

    fn owner_key_hash_seed(owner: &ExplicitOwner) -> u8 {
        match &owner.owner_key {
            ExplicitOwnerKey::Wallet => 0,
            ExplicitOwnerKey::Pool(pk) | ExplicitOwnerKey::Mint(pk) => pk.to_bytes()[0],
            ExplicitOwnerKey::Generic(id) => (*id & 0xFF) as u8,
        }
    }

    fn assert_matches_remove(result: FixedCapRemoveResult) {
        match result {
            FixedCapRemoveResult::Removed { .. } => {}
            other => panic!("expected successful removal, got {other:?}"),
        }
    }

    use super::super::eviction_planner::{
        select_eviction_victims, EvictionTier, TierFeasibilityRequest, TierFeasibilityResult,
        VictimSelectionPlan, VictimSelectionRequest,
    };

    type EvictionFixtureRow = (ExplicitOwner, u64, Vec<Pubkey>);

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct ReverseIndexRow {
        refcount: usize,
        owners: BTreeSet<ExplicitOwner>,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct AdmissionPlanStateSnapshot {
        cap: usize,
        physical_len: usize,
        next_touch_stamp: u64,
        owner_lru: BTreeMap<ExplicitOwner, u64>,
        ownership_groups: BTreeMap<ExplicitOwner, BTreeSet<Pubkey>>,
        snapshot_pubkeys: Vec<Pubkey>,
        reverse_index: BTreeMap<Pubkey, ReverseIndexRow>,
        planning_stats: PlanningStats,
        test_force_commit_plan_mismatch: bool,
        test_force_restore_plan_mismatch: bool,
        test_force_remove_planning_underflow: bool,
        test_force_remove_planning_zero_refcount: bool,
        test_force_stamp_plan_failure: bool,
        test_eviction_plan_extra_lru: Vec<OwnerLruEntry>,
        test_force_evicting_omit_survivor: Option<ExplicitOwner>,
        test_force_evicting_corrupt_physical_len: bool,
        test_force_evicting_corrupt_survivor_stamp: Option<ExplicitOwner>,
        test_force_evicting_extra_candidate_owner: Option<(ExplicitOwner, Vec<Pubkey>)>,
        test_force_evicting_corrupt_next_touch_stamp: bool,
    }

    fn reverse_index_from_groups(
        groups: &BTreeMap<ExplicitOwner, BTreeSet<Pubkey>>,
    ) -> BTreeMap<Pubkey, ReverseIndexRow> {
        let mut by_pubkey: BTreeMap<Pubkey, BTreeSet<ExplicitOwner>> = BTreeMap::new();
        for (owner, pubkeys) in groups {
            for pubkey in pubkeys {
                by_pubkey.entry(*pubkey).or_default().insert(owner.clone());
            }
        }
        by_pubkey
            .into_iter()
            .map(|(pubkey, owners)| {
                let refcount = owners.len();
                (pubkey, ReverseIndexRow { refcount, owners })
            })
            .collect()
    }

    fn snapshot_admission_plan_state(admission: &FixedCapAdmission) -> AdmissionPlanStateSnapshot {
        let ownership_groups = admission_groups(admission);
        let snapshot_pubkeys = admission.snapshot_pubkeys();
        let reverse_index = reverse_index_from_groups(&ownership_groups);
        for (pubkey, row) in &reverse_index {
            assert_eq!(row.refcount, admission.owner_refcount(pubkey));
        }
        assert_eq!(admission.physical_len, snapshot_pubkeys.len());
        AdmissionPlanStateSnapshot {
            cap: admission.cap,
            physical_len: admission.physical_len,
            next_touch_stamp: admission.next_touch_stamp,
            owner_lru: admission.owner_lru.clone(),
            ownership_groups,
            snapshot_pubkeys,
            reverse_index,
            planning_stats: admission.planning_stats.clone(),
            test_force_commit_plan_mismatch: admission.test_force_commit_plan_mismatch,
            test_force_restore_plan_mismatch: admission.test_force_restore_plan_mismatch,
            test_force_remove_planning_underflow: admission.test_force_remove_planning_underflow,
            test_force_remove_planning_zero_refcount: admission
                .test_force_remove_planning_zero_refcount,
            test_force_stamp_plan_failure: admission.test_force_stamp_plan_failure,
            test_eviction_plan_extra_lru: admission.test_eviction_plan_extra_lru.clone(),
            test_force_evicting_omit_survivor: admission.test_force_evicting_omit_survivor.clone(),
            test_force_evicting_corrupt_physical_len: admission
                .test_force_evicting_corrupt_physical_len,
            test_force_evicting_corrupt_survivor_stamp: admission
                .test_force_evicting_corrupt_survivor_stamp
                .clone(),
            test_force_evicting_extra_candidate_owner: admission
                .test_force_evicting_extra_candidate_owner
                .clone(),
            test_force_evicting_corrupt_next_touch_stamp: admission
                .test_force_evicting_corrupt_next_touch_stamp,
        }
    }

    fn assert_admission_plan_state_unchanged(
        admission: &FixedCapAdmission,
        before: &AdmissionPlanStateSnapshot,
    ) {
        assert_eq!(snapshot_admission_plan_state(admission), *before);
    }

    fn assert_admission_live_state_unchanged(
        admission: &FixedCapAdmission,
        before: &AdmissionPlanStateSnapshot,
    ) {
        assert_admission_plan_state_unchanged(admission, before);
    }

    fn plan_and_assert_state_unchanged(
        admission: &FixedCapAdmission,
        before: &AdmissionPlanStateSnapshot,
        incoming_owner: ExplicitOwner,
        incoming_pubkeys: Vec<Pubkey>,
    ) -> AdmissionEvictionPlanResult {
        let result = admission.plan_admit_with_eviction(incoming_owner, incoming_pubkeys);
        assert_admission_plan_state_unchanged(admission, before);
        result
    }

    fn admission_from_eviction_fixtures(
        cap: usize,
        fixtures: &[EvictionFixtureRow],
    ) -> FixedCapAdmission {
        let mut admission = FixedCapAdmission::new(cap);
        for (owner, _stamp, pubkeys) in fixtures {
            assert_admitted(admission.try_admit_new_group(owner.clone(), pubkeys.clone()));
        }
        for (owner, stamp, _) in fixtures {
            admission.set_test_owner_stamp(owner.clone(), *stamp);
        }
        if let Some(max_stamp) = fixtures.iter().map(|(_, stamp, _)| *stamp).max() {
            admission.set_test_next_touch_stamp(max_stamp.saturating_add(1));
        }
        admission
    }

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

    fn tier_request(
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

    fn assert_facade_matches_direct_selector(
        admission: &FixedCapAdmission,
        incoming_owner: ExplicitOwner,
        incoming_pubkeys: Vec<Pubkey>,
    ) {
        let snapshot = admission
            .test_build_eviction_snapshot()
            .expect("fixture admission must build eviction snapshot");
        let expected = select_eviction_victims(
            &snapshot,
            victim_request(
                incoming_owner.clone(),
                incoming_pubkeys.clone(),
                admission.cap(),
            ),
        );
        let before = snapshot_admission_plan_state(admission);
        let actual =
            plan_and_assert_state_unchanged(admission, &before, incoming_owner, incoming_pubkeys);
        assert_eq!(actual, expected);
    }

    #[test]
    fn eviction_plan_facade_result_no_eviction_needed_exact() {
        let tracker = pool_owner(ExplicitConsumer::Tracker, 1);
        let fixtures = [(tracker.clone(), 1, vec![pk(1)])];
        let admission = admission_from_eviction_fixtures(5, &fixtures);
        let incoming = pool_owner(ExplicitConsumer::Tracker, 9);
        let before = snapshot_admission_plan_state(&admission);
        let result =
            plan_and_assert_state_unchanged(&admission, &before, incoming.clone(), vec![pk(2)]);
        assert_eq!(
            result,
            VictimSelectionResult::NoEvictionNeeded {
                incoming_physical_added: 1,
                projected_final_len: 2,
            }
        );
        assert_facade_matches_direct_selector(&admission, incoming, vec![pk(2)]);
    }

    #[test]
    fn eviction_plan_facade_result_planned_exact() {
        let tracker = pool_owner(ExplicitConsumer::Tracker, 1);
        let arb = pool_owner(ExplicitConsumer::Arb, 2);
        let momentum = pool_owner(ExplicitConsumer::Momentum, 3);
        let fixtures = [
            (momentum.clone(), 30, vec![pk(3)]),
            (arb.clone(), 20, vec![pk(2)]),
            (tracker.clone(), 10, vec![pk(1)]),
        ];
        let admission = admission_from_eviction_fixtures(3, &fixtures);
        let incoming = pool_owner(ExplicitConsumer::Momentum, 9);
        let before = snapshot_admission_plan_state(&admission);
        let result =
            plan_and_assert_state_unchanged(&admission, &before, incoming.clone(), vec![pk(10)]);
        assert_eq!(
            result,
            VictimSelectionResult::Planned(VictimSelectionPlan {
                victims: vec![tracker.clone()],
                physical_freed: vec![pk(1)],
                incoming_physical_added: 1,
                projected_final_len: 3,
                opened_through: EvictionTier::Tracker,
            })
        );
        assert_facade_matches_direct_selector(&admission, incoming, vec![pk(10)]);
    }

    #[test]
    fn eviction_plan_facade_result_planned_wallet_never_victim() {
        let wallet = pool_owner(ExplicitConsumer::Wallet, 1);
        let tracker = pool_owner(ExplicitConsumer::Tracker, 2);
        let shared = pk(50);
        let fixtures = [
            (wallet.clone(), 1, vec![shared, pk(1)]),
            (tracker.clone(), 2, vec![pk(2)]),
        ];
        let admission = admission_from_eviction_fixtures(3, &fixtures);
        let incoming = pool_owner(ExplicitConsumer::Momentum, 9);
        let before = snapshot_admission_plan_state(&admission);
        let result =
            plan_and_assert_state_unchanged(&admission, &before, incoming.clone(), vec![pk(10)]);
        assert_eq!(
            result,
            VictimSelectionResult::Planned(VictimSelectionPlan {
                victims: vec![tracker.clone()],
                physical_freed: vec![pk(2)],
                incoming_physical_added: 1,
                projected_final_len: 3,
                opened_through: EvictionTier::Tracker,
            })
        );
        let planned = match &result {
            VictimSelectionResult::Planned(plan) => plan,
            other => panic!("expected Planned, got {other:?}"),
        };
        assert!(!planned.victims.contains(&wallet));
        assert_facade_matches_direct_selector(&admission, incoming, vec![pk(10)]);
    }

    #[test]
    fn eviction_plan_facade_result_planned_joint_shared() {
        let shared = pk(50);
        let a = pool_owner(ExplicitConsumer::Tracker, 1);
        let b = pool_owner(ExplicitConsumer::Tracker, 2);
        let fixtures = [(a.clone(), 1, vec![shared]), (b.clone(), 2, vec![shared])];
        let admission = admission_from_eviction_fixtures(1, &fixtures);
        let incoming = pool_owner(ExplicitConsumer::Momentum, 9);
        let before = snapshot_admission_plan_state(&admission);
        let result =
            plan_and_assert_state_unchanged(&admission, &before, incoming.clone(), vec![pk(10)]);
        assert_eq!(
            result,
            VictimSelectionResult::Planned(VictimSelectionPlan {
                victims: vec![a.clone(), b.clone()],
                physical_freed: vec![shared],
                incoming_physical_added: 1,
                projected_final_len: 1,
                opened_through: EvictionTier::Tracker,
            })
        );
        assert_facade_matches_direct_selector(&admission, incoming, vec![pk(10)]);
    }

    #[test]
    fn eviction_plan_facade_result_rejected_protected_exact() {
        let wallet = pool_owner(ExplicitConsumer::Wallet, 1);
        let momentum = pool_owner(ExplicitConsumer::Momentum, 2);
        let fixtures = [
            (wallet.clone(), 1, vec![pk(1)]),
            (momentum.clone(), 2, vec![pk(2)]),
        ];
        let admission = admission_from_eviction_fixtures(2, &fixtures);
        let incoming = pool_owner(ExplicitConsumer::Momentum, 9);
        let incoming_pubkeys = vec![pk(10), pk(11)];
        let before = snapshot_admission_plan_state(&admission);
        let result = plan_and_assert_state_unchanged(
            &admission,
            &before,
            incoming.clone(),
            incoming_pubkeys.clone(),
        );
        assert_eq!(
            result,
            VictimSelectionResult::RejectedProtected {
                incoming_physical_added: 2,
                required_to_free: 2,
            }
        );
        let snapshot = admission
            .test_build_eviction_snapshot()
            .expect("fixture admission must build eviction snapshot");
        assert_eq!(
            snapshot.analyze_tier_feasibility(tier_request(
                incoming,
                incoming_pubkeys,
                admission.cap(),
            )),
            TierFeasibilityResult::RejectedProtected {
                incoming_physical_added: 2,
                required_to_free: 2,
                maximally_freeable_pubkeys: vec![pk(2)],
            }
        );
        assert_facade_matches_direct_selector(
            &admission,
            pool_owner(ExplicitConsumer::Momentum, 9),
            vec![pk(10), pk(11)],
        );
    }

    #[test]
    fn eviction_plan_facade_result_rejected_invalid_input_exact() {
        let tracker = pool_owner(ExplicitConsumer::Tracker, 1);
        let fixtures = [(tracker.clone(), 1, vec![pk(1)])];
        let admission = admission_from_eviction_fixtures(5, &fixtures);
        let incoming = pool_owner(ExplicitConsumer::Momentum, 9);
        let before = snapshot_admission_plan_state(&admission);
        let result = plan_and_assert_state_unchanged(&admission, &before, incoming.clone(), vec![]);
        assert_eq!(result, VictimSelectionResult::RejectedInvalidInput);
        assert_facade_matches_direct_selector(&admission, incoming, vec![]);
    }

    #[test]
    fn eviction_plan_facade_result_internal_invariant_violation_exact() {
        let tracker = pool_owner(ExplicitConsumer::Tracker, 1);
        let fixtures = [(tracker.clone(), 1, vec![pk(1)])];
        let mut admission = admission_from_eviction_fixtures(5, &fixtures);
        let ghost = pool_owner(ExplicitConsumer::Tracker, 99);
        let overlay = vec![OwnerLruEntry {
            owner: ghost,
            last_touch: 999,
        }];
        admission.set_test_eviction_plan_extra_lru(overlay.clone());
        let before = snapshot_admission_plan_state(&admission);
        assert_eq!(before.test_eviction_plan_extra_lru, overlay);
        let result = plan_and_assert_state_unchanged(
            &admission,
            &before,
            pool_owner(ExplicitConsumer::Momentum, 9),
            vec![pk(2)],
        );
        assert_eq!(result, VictimSelectionResult::InternalInvariantViolation);
        assert_eq!(
            snapshot_admission_plan_state(&admission).test_eviction_plan_extra_lru,
            overlay
        );
    }

    #[test]
    fn eviction_plan_facade_touch_changes_plan_lru_deterministically() {
        let t1 = pool_owner(ExplicitConsumer::Tracker, 1);
        let t2 = pool_owner(ExplicitConsumer::Tracker, 2);
        let fixtures = [
            (t1.clone(), 100, vec![pk(1)]),
            (t2.clone(), 200, vec![pk(2)]),
        ];
        let mut admission = admission_from_eviction_fixtures(2, &fixtures);
        let incoming = pool_owner(ExplicitConsumer::Momentum, 9);
        let before_touch = snapshot_admission_plan_state(&admission);
        let before_plan = plan_and_assert_state_unchanged(
            &admission,
            &before_touch,
            incoming.clone(),
            vec![pk(10)],
        );
        assert_eq!(
            before_plan,
            VictimSelectionResult::Planned(VictimSelectionPlan {
                victims: vec![t1.clone()],
                physical_freed: vec![pk(1)],
                incoming_physical_added: 1,
                projected_final_len: 2,
                opened_through: EvictionTier::Tracker,
            })
        );

        assert_eq!(admission.touch_group(t1.clone()), TouchResult::Touched);
        let after_touch_state = snapshot_admission_plan_state(&admission);
        assert_ne!(after_touch_state.owner_lru, before_touch.owner_lru);
        let after_plan =
            plan_and_assert_state_unchanged(&admission, &after_touch_state, incoming, vec![pk(10)]);
        assert_eq!(
            after_plan,
            VictimSelectionResult::Planned(VictimSelectionPlan {
                victims: vec![t2.clone()],
                physical_freed: vec![pk(2)],
                incoming_physical_added: 1,
                projected_final_len: 2,
                opened_through: EvictionTier::Tracker,
            })
        );
    }

    #[test]
    fn eviction_plan_facade_matches_direct_selector_on_fixtures() {
        let shared = pk(50);
        let a = pool_owner(ExplicitConsumer::Tracker, 1);
        let b = pool_owner(ExplicitConsumer::Tracker, 2);
        let fixtures = [
            (a.clone(), 1, vec![shared]),
            (b.clone(), 2, vec![pk(2)]),
            (pool_owner(ExplicitConsumer::Arb, 3), 3, vec![pk(3)]),
        ];
        let admission = admission_from_eviction_fixtures(3, &fixtures);
        assert_facade_matches_direct_selector(
            &admission,
            pool_owner(ExplicitConsumer::Momentum, 9),
            vec![shared, pk(10)],
        );
        assert_facade_matches_direct_selector(
            &admission,
            pool_owner(ExplicitConsumer::Momentum, 9),
            vec![pk(10), pk(11)],
        );
    }

    #[test]
    fn eviction_plan_facade_repeated_planning_is_identical_and_stamp_free() {
        let tracker = pool_owner(ExplicitConsumer::Tracker, 1);
        let arb = pool_owner(ExplicitConsumer::Arb, 2);
        let fixtures = [
            (tracker.clone(), 10, vec![pk(1)]),
            (arb.clone(), 20, vec![pk(2)]),
        ];
        let admission = admission_from_eviction_fixtures(2, &fixtures);
        let incoming = pool_owner(ExplicitConsumer::Momentum, 9);
        let incoming_pubkeys = vec![pk(10)];
        let before = snapshot_admission_plan_state(&admission);
        let first = plan_and_assert_state_unchanged(
            &admission,
            &before,
            incoming.clone(),
            incoming_pubkeys.clone(),
        );
        let second =
            plan_and_assert_state_unchanged(&admission, &before, incoming, incoming_pubkeys);
        assert_eq!(first, second);
    }

    // --- Evicting admission commit tests (PR 1c2) ---

    fn assert_evicting_admitted(result: EvictingAdmissionResult) {
        match result {
            EvictingAdmissionResult::InsertedNoEviction { .. }
            | EvictingAdmissionResult::InsertedWithEviction { .. } => {}
            other => panic!("expected successful evicting admission, got {other:?}"),
        }
    }

    type RefOracleOwnerRankKey = (u8, u64, ExplicitOwner);

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
    struct RefOraclePackageRankKey {
        max_tier: u8,
        member_keys: Vec<RefOracleOwnerRankKey>,
        package_len: usize,
        owners: Vec<ExplicitOwner>,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    enum RefOraclePolicyTier {
        Tracker,
        Arb,
        Momentum,
    }

    impl RefOraclePolicyTier {
        fn to_eviction_tier(self) -> EvictionTier {
            match self {
                Self::Tracker => EvictionTier::Tracker,
                Self::Arb => EvictionTier::Arb,
                Self::Momentum => EvictionTier::Momentum,
            }
        }

        fn allowed_for_incoming(consumer: ExplicitConsumer) -> &'static [RefOraclePolicyTier] {
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

    /// Independent eviction oracle — hard-coded policy, powerset (<=8 owners), procedural replay.
    /// Never calls production planner/selector or [`FixedCapAdmission`].
    struct RefEvictingOracle {
        physical_len: usize,
        owner_records: BTreeMap<ExplicitOwner, (u64, Vec<Pubkey>)>,
        pubkey_holders: BTreeMap<Pubkey, BTreeSet<ExplicitOwner>>,
    }

    impl RefEvictingOracle {
        fn eviction_rank(consumer: ExplicitConsumer) -> u8 {
            match consumer {
                ExplicitConsumer::Tracker => 0,
                ExplicitConsumer::Arb => 1,
                ExplicitConsumer::Momentum => 2,
                ExplicitConsumer::Wallet => u8::MAX,
            }
        }

        fn from_state(
            groups: &BTreeMap<ExplicitOwner, BTreeSet<Pubkey>>,
            lru: &BTreeMap<ExplicitOwner, u64>,
        ) -> Self {
            let mut owner_records = BTreeMap::new();
            let mut pubkey_holders: BTreeMap<Pubkey, BTreeSet<ExplicitOwner>> = BTreeMap::new();
            for (owner, pubkeys) in groups {
                let mut normalized: Vec<Pubkey> = pubkeys.iter().copied().collect();
                normalized.sort();
                normalized.dedup();
                let touch = lru.get(owner).copied().unwrap_or(0);
                owner_records.insert(owner.clone(), (touch, normalized.clone()));
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

        fn eligible_owners_at_tier(&self, tier: RefOraclePolicyTier) -> BTreeSet<ExplicitOwner> {
            let cumulative = RefOraclePolicyTier::cumulative_consumers(tier);
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

        fn pubkey_freeable_count_at_tier(
            &self,
            tier: RefOraclePolicyTier,
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

        fn package_rank_tuple(&self, package: &BTreeSet<ExplicitOwner>) -> RefOraclePackageRankKey {
            let max_tier = package
                .iter()
                .map(|owner| Self::eviction_rank(owner.consumer))
                .max()
                .unwrap_or(0);
            let mut member_keys: Vec<RefOracleOwnerRankKey> = package
                .iter()
                .map(|owner| {
                    let touch = self.owner_records.get(owner).map(|(t, _)| *t).unwrap_or(0);
                    (Self::eviction_rank(owner.consumer), touch, owner.clone())
                })
                .collect();
            member_keys.sort();
            let mut owners: Vec<_> = package.iter().cloned().collect();
            owners.sort();
            RefOraclePackageRankKey {
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

        fn predict(
            &self,
            incoming_owner: &ExplicitOwner,
            incoming_pubkeys: &[Pubkey],
            cap: usize,
        ) -> EvictingAdmissionResult {
            let mut normalized: Vec<Pubkey> = incoming_pubkeys.to_vec();
            normalized.sort();
            normalized.dedup();
            if normalized.is_empty() {
                return EvictingAdmissionResult::RejectedInvalidInput;
            }
            if self.owner_records.contains_key(incoming_owner) {
                return EvictingAdmissionResult::RejectedExistingOwner;
            }

            let incoming_set: BTreeSet<Pubkey> = normalized.iter().copied().collect();
            let incoming_added = self.incoming_physical_added(&normalized);
            let projected = match self.physical_len.checked_add(incoming_added) {
                Some(value) => value,
                None => return EvictingAdmissionResult::InternalInvariantViolation,
            };
            if projected <= cap {
                let physical_added: Vec<Pubkey> = normalized
                    .iter()
                    .copied()
                    .filter(|pk| !self.pubkey_holders.contains_key(pk))
                    .collect();
                return EvictingAdmissionResult::InsertedNoEviction { physical_added };
            }
            let required = match projected.checked_sub(cap) {
                Some(value) => value,
                None => return EvictingAdmissionResult::InternalInvariantViolation,
            };

            let allowed_tiers = RefOraclePolicyTier::allowed_for_incoming(incoming_owner.consumer);
            let mut opened = None;
            for tier in allowed_tiers {
                if self.pubkey_freeable_count_at_tier(*tier, &incoming_set) >= required {
                    opened = Some((*tier, self.eligible_owners_at_tier(*tier)));
                    break;
                }
            }

            let Some((tier, allowed)) = opened else {
                return EvictingAdmissionResult::RejectedProtected {
                    incoming_physical_added: incoming_added,
                    required_to_free: required,
                };
            };

            if self.powerset_max_freeable(&allowed, &incoming_set) < required {
                return EvictingAdmissionResult::InternalInvariantViolation;
            }

            let Some((victims, freed)) =
                self.procedural_selection(&allowed, &incoming_set, required)
            else {
                return EvictingAdmissionResult::InternalInvariantViolation;
            };

            let mut physical_freed: Vec<Pubkey> = freed.into_iter().collect();
            physical_freed.sort();
            let physical_added: Vec<Pubkey> = normalized
                .iter()
                .copied()
                .filter(|pk| !self.pubkey_holders.contains_key(pk))
                .collect();

            EvictingAdmissionResult::InsertedWithEviction {
                physical_added,
                physical_removed: physical_freed,
                victims,
                opened_through: tier.to_eviction_tier(),
            }
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct RefEvictingModelState {
        groups: BTreeMap<ExplicitOwner, BTreeSet<Pubkey>>,
        lru: BTreeMap<ExplicitOwner, u64>,
        next_stamp: u64,
    }

    /// Independent admission model for mixed insert/replace/remove/touch/evicting ops.
    struct RefEvictingAdmissionModel {
        cap: usize,
        state: RefEvictingModelState,
    }

    impl RefEvictingAdmissionModel {
        fn new(cap: usize) -> Self {
            Self {
                cap,
                state: RefEvictingModelState {
                    groups: BTreeMap::new(),
                    lru: BTreeMap::new(),
                    next_stamp: 1,
                },
            }
        }

        fn from_fixtures(cap: usize, fixtures: &[EvictionFixtureRow]) -> Self {
            let mut model = Self::new(cap);
            for (owner, stamp, pubkeys) in fixtures {
                let _ = model.try_admit_new_group(owner.clone(), pubkeys.clone());
                model.state.lru.insert(owner.clone(), *stamp);
            }
            if let Some(max_stamp) = fixtures.iter().map(|(_, stamp, _)| *stamp).max() {
                model.state.next_stamp = max_stamp.saturating_add(1);
            }
            model
        }

        fn physical_len(&self) -> usize {
            self.state
                .groups
                .values()
                .flat_map(|set| set.iter().copied())
                .collect::<BTreeSet<_>>()
                .len()
        }

        fn assign_stamp(&mut self, owner: &ExplicitOwner) {
            if self.state.next_stamp == u64::MAX {
                let mut ordered: Vec<(ExplicitOwner, u64)> = self
                    .state
                    .lru
                    .iter()
                    .map(|(owner, stamp)| (owner.clone(), *stamp))
                    .collect();
                ordered.sort_by(|(a_owner, a_stamp), (b_owner, b_stamp)| {
                    a_stamp.cmp(b_stamp).then_with(|| a_owner.cmp(b_owner))
                });
                self.state.lru.clear();
                for (idx, (owner, _)) in ordered.into_iter().enumerate() {
                    if let Ok(rank) = u64::try_from(idx) {
                        if let Some(stamp) = rank.checked_add(1) {
                            self.state.lru.insert(owner, stamp);
                        }
                    }
                }
                self.state.next_stamp = self
                    .state
                    .lru
                    .values()
                    .copied()
                    .max()
                    .and_then(|max| max.checked_add(1))
                    .unwrap_or(1);
            }
            let stamp = self.state.next_stamp;
            self.state.next_stamp = self.state.next_stamp.saturating_add(1);
            self.state.lru.insert(owner.clone(), stamp);
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
            if self.state.groups.contains_key(&owner) {
                return FixedCapAdmissionResult::RejectedExistingOwner;
            }
            let physical_before: BTreeSet<Pubkey> = self
                .state
                .groups
                .values()
                .flat_map(|set| set.iter().copied())
                .collect();
            let candidate: BTreeSet<Pubkey> = normalized.iter().copied().collect();
            let mut candidate_groups = self.state.groups.clone();
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
            self.state.groups = candidate_groups;
            self.assign_stamp(&owner);
            if physical_added.is_empty() {
                FixedCapAdmissionResult::OwnerAddedNoNewPubkey
            } else {
                FixedCapAdmissionResult::Inserted { physical_added }
            }
        }

        fn commit_evicting_success(
            &mut self,
            owner: ExplicitOwner,
            normalized: &[Pubkey],
            victims: &[ExplicitOwner],
        ) {
            let victim_set: BTreeSet<ExplicitOwner> = victims.iter().cloned().collect();
            self.state.groups.retain(|o, _| !victim_set.contains(o));
            self.state.lru.retain(|o, _| !victim_set.contains(o));
            self.state
                .groups
                .insert(owner.clone(), normalized.iter().copied().collect());
            self.assign_stamp(&owner);
        }

        fn try_admit_with_eviction(
            &mut self,
            owner: ExplicitOwner,
            pubkeys: impl IntoIterator<Item = Pubkey>,
        ) -> EvictingAdmissionResult {
            let mut normalized: Vec<Pubkey> = pubkeys.into_iter().collect();
            normalized.sort();
            normalized.dedup();
            let oracle = RefEvictingOracle::from_state(&self.state.groups, &self.state.lru);
            let prediction = oracle.predict(&owner, &normalized, self.cap);
            match &prediction {
                EvictingAdmissionResult::InsertedNoEviction { .. } => {
                    match self.try_admit_new_group(owner, normalized) {
                        FixedCapAdmissionResult::Inserted { physical_added } => {
                            EvictingAdmissionResult::InsertedNoEviction { physical_added }
                        }
                        FixedCapAdmissionResult::OwnerAddedNoNewPubkey => {
                            EvictingAdmissionResult::InsertedNoEviction {
                                physical_added: Vec::new(),
                            }
                        }
                        FixedCapAdmissionResult::RejectedExistingOwner => {
                            EvictingAdmissionResult::RejectedExistingOwner
                        }
                        FixedCapAdmissionResult::RejectedInvalidGroup => {
                            EvictingAdmissionResult::RejectedInvalidInput
                        }
                        FixedCapAdmissionResult::RejectedCap { .. }
                        | FixedCapAdmissionResult::InternalInvariantViolation => {
                            EvictingAdmissionResult::InternalInvariantViolation
                        }
                    }
                }
                EvictingAdmissionResult::InsertedWithEviction {
                    victims,
                    physical_added,
                    physical_removed,
                    opened_through,
                } => {
                    self.commit_evicting_success(owner.clone(), &normalized, victims);
                    EvictingAdmissionResult::InsertedWithEviction {
                        physical_added: physical_added.clone(),
                        physical_removed: physical_removed.clone(),
                        victims: victims.clone(),
                        opened_through: *opened_through,
                    }
                }
                other => other.clone(),
            }
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
            let Some(old) = self.state.groups.get(&owner).cloned() else {
                return FixedCapReplaceResult::RejectedMissingOwner;
            };
            if old.iter().copied().collect::<Vec<_>>() == normalized {
                return FixedCapReplaceResult::Unchanged;
            }
            let physical_before: BTreeSet<Pubkey> = self
                .state
                .groups
                .values()
                .flat_map(|set| set.iter().copied())
                .collect();
            let mut candidate_groups = self.state.groups.clone();
            candidate_groups.insert(owner.clone(), normalized.iter().copied().collect());
            let physical_after: BTreeSet<Pubkey> = candidate_groups
                .values()
                .flat_map(|set| set.iter().copied())
                .collect();
            if physical_after.len() > self.cap {
                let added: Vec<Pubkey> = physical_after
                    .difference(&physical_before)
                    .copied()
                    .collect();
                return FixedCapReplaceResult::RejectedCap {
                    required_unique: added.len(),
                    available_unique: self
                        .cap
                        .saturating_sub(physical_before.len())
                        .saturating_add(physical_before.difference(&physical_after).count()),
                };
            }
            let physical_added: Vec<Pubkey> = physical_after
                .difference(&physical_before)
                .copied()
                .collect();
            let physical_removed: Vec<Pubkey> = physical_before
                .difference(&physical_after)
                .copied()
                .collect();
            self.state.groups = candidate_groups;
            self.assign_stamp(&owner);
            FixedCapReplaceResult::Replaced {
                physical_added,
                physical_removed,
            }
        }

        fn remove_group(&mut self, owner: ExplicitOwner) -> FixedCapRemoveResult {
            if !self.state.groups.contains_key(&owner) {
                return FixedCapRemoveResult::NotFound;
            }
            let physical_before: BTreeSet<Pubkey> = self
                .state
                .groups
                .values()
                .flat_map(|set| set.iter().copied())
                .collect();
            let mut candidate_groups = self.state.groups.clone();
            candidate_groups.remove(&owner);
            let physical_after: BTreeSet<Pubkey> = candidate_groups
                .values()
                .flat_map(|set| set.iter().copied())
                .collect();
            let physical_removed: Vec<Pubkey> = physical_before
                .difference(&physical_after)
                .copied()
                .collect();
            self.state.groups = candidate_groups;
            self.state.lru.remove(&owner);
            FixedCapRemoveResult::Removed { physical_removed }
        }

        fn touch_group(&mut self, owner: &ExplicitOwner) {
            if self.state.groups.contains_key(owner) {
                self.assign_stamp(owner);
            }
        }
    }

    fn assert_evicting_model_matches_admission(
        admission: &FixedCapAdmission,
        model: &RefEvictingAdmissionModel,
    ) {
        assert_eq!(admission_groups(admission), model.state.groups);
        assert_eq!(admission.test_owner_stamps(), model.state.lru);
        assert_eq!(admission.test_next_touch_stamp(), model.state.next_stamp);
        assert_eq!(admission.len(), model.physical_len());
        assert!(admission.len() <= admission.cap());
        assert_lru_matches_ownership(admission);
        assert_no_extra_reverse_index_keys(admission);
    }

    struct RefEvictOracleCase {
        fixtures: Vec<EvictionFixtureRow>,
        incoming: ExplicitOwner,
        keys: Vec<Pubkey>,
        cap: usize,
    }

    fn ref_evict_oracle_cases() -> Vec<RefEvictOracleCase> {
        let shared = pk(50);
        let wallet = pool_owner(ExplicitConsumer::Wallet, 1);
        let tracker_a = pool_owner(ExplicitConsumer::Tracker, 1);
        let tracker_b = pool_owner(ExplicitConsumer::Tracker, 2);
        let arb_a = pool_owner(ExplicitConsumer::Arb, 4);
        let momentum_a = pool_owner(ExplicitConsumer::Momentum, 5);
        let incoming_momentum = pool_owner(ExplicitConsumer::Momentum, 92);
        let incoming_arb = pool_owner(ExplicitConsumer::Arb, 91);
        vec![
            RefEvictOracleCase {
                fixtures: vec![
                    (tracker_a.clone(), 10, vec![pk(1)]),
                    (arb_a.clone(), 20, vec![pk(2)]),
                    (momentum_a.clone(), 30, vec![pk(3)]),
                ],
                incoming: incoming_momentum.clone(),
                keys: vec![pk(100)],
                cap: 3,
            },
            RefEvictOracleCase {
                fixtures: vec![
                    (wallet.clone(), 1, vec![shared, pk(1)]),
                    (tracker_a.clone(), 2, vec![pk(2)]),
                ],
                incoming: incoming_momentum.clone(),
                keys: vec![pk(10)],
                cap: 3,
            },
            RefEvictOracleCase {
                fixtures: vec![
                    (momentum_a.clone(), 20, vec![pk(2), pk(3)]),
                    (tracker_a.clone(), 10, vec![pk(1)]),
                ],
                incoming: incoming_arb.clone(),
                keys: vec![pk(10), pk(11)],
                cap: 3,
            },
            RefEvictOracleCase {
                fixtures: vec![
                    (tracker_a.clone(), 1, vec![shared]),
                    (tracker_b.clone(), 2, vec![shared]),
                ],
                incoming: incoming_momentum.clone(),
                keys: vec![pk(10)],
                cap: 1,
            },
        ]
    }

    #[test]
    fn evicting_admit_no_cap_pressure_uses_fast_path_without_candidate_build() {
        let tracker = pool_owner(ExplicitConsumer::Tracker, 1);
        let fixtures = [(tracker.clone(), 1, vec![pk(1)])];
        let mut admission = admission_from_eviction_fixtures(5, &fixtures);
        let incoming = pool_owner(ExplicitConsumer::Tracker, 9);
        let result = admission.try_admit_with_eviction(incoming.clone(), vec![pk(2)]);
        assert_eq!(
            result,
            EvictingAdmissionResult::InsertedNoEviction {
                physical_added: vec![pk(2)],
            }
        );
        assert_eq!(admission.planning_stats().eviction_candidate_builds, 0);
        assert_eq!(admission.planning_stats().eviction_candidate_group_edges, 0);
        assert_eq!(admission.len(), 2);
        assert_eq!(admission.owner_group(&incoming), Some([pk(2)].as_slice()));
        assert_lru_matches_ownership(&admission);
    }

    #[test]
    fn evicting_admit_fast_path_stamp_plan_failure_is_full_no_op() {
        let mut admission = FixedCapAdmission::new(5);
        let owner = pool_owner(ExplicitConsumer::Tracker, 1);
        assert_admitted(admission.try_admit_new_group(owner.clone(), [pk(1)]));

        admission.set_test_force_stamp_plan_failure(true);
        let before = snapshot_admission_plan_state(&admission);
        let result = admission
            .try_admit_with_eviction(pool_owner(ExplicitConsumer::Tracker, 9), vec![pk(2)]);
        assert_eq!(result, EvictingAdmissionResult::InternalInvariantViolation);
        assert_admission_plan_state_unchanged(&admission, &before);
    }

    #[test]
    fn evicting_admit_fast_path_commit_plan_mismatch_is_full_no_op() {
        let mut admission = FixedCapAdmission::new(5);
        let owner = pool_owner(ExplicitConsumer::Tracker, 1);
        assert_admitted(admission.try_admit_new_group(owner.clone(), [pk(1)]));

        admission.set_test_force_commit_plan_mismatch(true);
        let before = snapshot_admission_plan_state(&admission);
        let result = admission
            .try_admit_with_eviction(pool_owner(ExplicitConsumer::Tracker, 9), vec![pk(2)]);
        assert_eq!(result, EvictingAdmissionResult::InternalInvariantViolation);
        assert_admission_plan_state_unchanged(&admission, &before);
    }

    #[test]
    fn evicting_admit_tracker_evicted_for_momentum_incoming() {
        let tracker = pool_owner(ExplicitConsumer::Tracker, 1);
        let arb = pool_owner(ExplicitConsumer::Arb, 2);
        let momentum = pool_owner(ExplicitConsumer::Momentum, 3);
        let fixtures = [
            (momentum.clone(), 30, vec![pk(3)]),
            (arb.clone(), 20, vec![pk(2)]),
            (tracker.clone(), 10, vec![pk(1)]),
        ];
        let mut admission = admission_from_eviction_fixtures(3, &fixtures);
        let incoming = pool_owner(ExplicitConsumer::Momentum, 9);
        let result = admission.try_admit_with_eviction(incoming.clone(), vec![pk(10)]);
        assert_eq!(
            result,
            EvictingAdmissionResult::InsertedWithEviction {
                physical_added: vec![pk(10)],
                physical_removed: vec![pk(1)],
                victims: vec![tracker.clone()],
                opened_through: EvictionTier::Tracker,
            }
        );
        assert!(admission.owner_group(&tracker).is_none());
        assert_eq!(admission.owner_group(&incoming), Some([pk(10)].as_slice()));
        assert_eq!(admission.len(), 3);
    }

    #[test]
    fn evicting_admit_incoming_arb_cannot_evict_momentum() {
        let tracker = pool_owner(ExplicitConsumer::Tracker, 1);
        let momentum = pool_owner(ExplicitConsumer::Momentum, 2);
        let fixtures = [
            (momentum.clone(), 20, vec![pk(2), pk(3)]),
            (tracker.clone(), 10, vec![pk(1)]),
        ];
        let mut admission = admission_from_eviction_fixtures(3, &fixtures);
        let before = snapshot_admission_plan_state(&admission);
        let incoming = pool_owner(ExplicitConsumer::Arb, 9);
        let result = admission.try_admit_with_eviction(incoming, vec![pk(10), pk(11)]);
        assert_eq!(
            result,
            EvictingAdmissionResult::RejectedProtected {
                incoming_physical_added: 2,
                required_to_free: 2,
            }
        );
        assert_admission_live_state_unchanged(&admission, &before);
    }

    #[test]
    fn evicting_admit_wallet_never_victim() {
        let wallet = pool_owner(ExplicitConsumer::Wallet, 1);
        let tracker = pool_owner(ExplicitConsumer::Tracker, 2);
        let shared = pk(50);
        let fixtures = [
            (wallet.clone(), 1, vec![shared, pk(1)]),
            (tracker.clone(), 2, vec![pk(2)]),
        ];
        let mut admission = admission_from_eviction_fixtures(3, &fixtures);
        let incoming = pool_owner(ExplicitConsumer::Momentum, 9);
        let result = admission.try_admit_with_eviction(incoming.clone(), vec![pk(10)]);
        match &result {
            EvictingAdmissionResult::InsertedWithEviction { victims, .. } => {
                assert!(!victims.contains(&wallet));
                assert_eq!(victims, &vec![tracker.clone()]);
            }
            other => panic!("expected InsertedWithEviction, got {other:?}"),
        }
    }

    #[test]
    fn evicting_admit_true_lru_within_tier() {
        let t_old = pool_owner(ExplicitConsumer::Tracker, 1);
        let t_new = pool_owner(ExplicitConsumer::Tracker, 2);
        let fixtures = [
            (t_old.clone(), 10, vec![pk(1)]),
            (t_new.clone(), 20, vec![pk(2)]),
        ];
        let mut admission = admission_from_eviction_fixtures(2, &fixtures);
        let incoming = pool_owner(ExplicitConsumer::Momentum, 9);
        let result = admission.try_admit_with_eviction(incoming, vec![pk(10)]);
        match &result {
            EvictingAdmissionResult::InsertedWithEviction { victims, .. } => {
                assert_eq!(victims, &vec![t_old.clone()]);
            }
            other => panic!("expected InsertedWithEviction, got {other:?}"),
        }
    }

    #[test]
    fn evicting_admit_joint_shared_victims() {
        let shared = pk(50);
        let a = pool_owner(ExplicitConsumer::Tracker, 1);
        let b = pool_owner(ExplicitConsumer::Tracker, 2);
        let fixtures = [(a.clone(), 1, vec![shared]), (b.clone(), 2, vec![shared])];
        let mut admission = admission_from_eviction_fixtures(1, &fixtures);
        let incoming = pool_owner(ExplicitConsumer::Momentum, 9);
        let result = admission.try_admit_with_eviction(incoming, vec![pk(10)]);
        assert_eq!(
            result,
            EvictingAdmissionResult::InsertedWithEviction {
                physical_added: vec![pk(10)],
                physical_removed: vec![shared],
                victims: vec![a.clone(), b.clone()],
                opened_through: EvictionTier::Tracker,
            }
        );
    }

    #[test]
    fn evicting_admit_zero_marginal_preserve_shared_incoming() {
        let shared = pk(50);
        let a = pool_owner(ExplicitConsumer::Tracker, 1);
        let b = pool_owner(ExplicitConsumer::Tracker, 2);
        let fixtures = [(a.clone(), 1, vec![shared]), (b.clone(), 2, vec![pk(2)])];
        let mut admission = admission_from_eviction_fixtures(2, &fixtures);
        let incoming = pool_owner(ExplicitConsumer::Momentum, 9);
        let result = admission.try_admit_with_eviction(incoming, vec![shared, pk(10)]);
        match &result {
            EvictingAdmissionResult::InsertedWithEviction {
                physical_added,
                victims,
                ..
            } => {
                assert_eq!(physical_added, &vec![pk(10)]);
                assert_eq!(victims, &vec![b.clone()]);
                assert!(!victims.contains(&a));
            }
            other => panic!("expected InsertedWithEviction, got {other:?}"),
        }
    }

    #[test]
    fn evicting_admit_exact_physical_deltas_and_final_len() {
        let tracker = pool_owner(ExplicitConsumer::Tracker, 1);
        let fixtures = [(tracker.clone(), 10, vec![pk(1), pk(2)])];
        let mut admission = admission_from_eviction_fixtures(2, &fixtures);
        let incoming = pool_owner(ExplicitConsumer::Momentum, 9);
        let overlap = pk(1);
        let result = admission.try_admit_with_eviction(incoming, vec![overlap, pk(10)]);
        assert_eq!(
            result,
            EvictingAdmissionResult::InsertedWithEviction {
                physical_added: vec![pk(10)],
                physical_removed: vec![pk(2)],
                victims: vec![tracker.clone()],
                opened_through: EvictionTier::Tracker,
            }
        );
        assert_eq!(admission.len(), 2);
    }

    #[test]
    fn evicting_admit_preserves_survivor_stamps_and_incoming_newest() {
        let t1 = pool_owner(ExplicitConsumer::Tracker, 1);
        let arb = pool_owner(ExplicitConsumer::Arb, 2);
        let fixtures = [
            (t1.clone(), 100, vec![pk(1)]),
            (arb.clone(), 200, vec![pk(2)]),
        ];
        let mut admission = admission_from_eviction_fixtures(2, &fixtures);
        let arb_stamp = admission.last_touch(&arb).unwrap();
        let incoming = pool_owner(ExplicitConsumer::Momentum, 9);
        assert_evicting_admitted(admission.try_admit_with_eviction(incoming.clone(), vec![pk(10)]));
        assert!(admission.last_touch(&t1).is_none());
        assert_eq!(admission.last_touch(&arb), Some(arb_stamp));
        let incoming_stamp = admission.last_touch(&incoming).unwrap();
        assert!(incoming_stamp > arb_stamp);
    }

    #[test]
    fn evicting_admit_near_overflow_renormalization_preserves_order() {
        let t1 = pool_owner(ExplicitConsumer::Tracker, 1);
        let t2 = pool_owner(ExplicitConsumer::Tracker, 2);
        let fixtures = [
            (t1.clone(), u64::MAX - 1, vec![pk(1)]),
            (t2.clone(), u64::MAX, vec![pk(2)]),
        ];
        let mut admission = admission_from_eviction_fixtures(2, &fixtures);
        admission.set_test_next_touch_stamp(u64::MAX);
        let incoming = pool_owner(ExplicitConsumer::Momentum, 9);
        assert_evicting_admitted(admission.try_admit_with_eviction(incoming.clone(), vec![pk(10)]));
        let mut ordered: Vec<(u64, ExplicitOwner)> = admission
            .owner_lru
            .iter()
            .map(|(owner, &stamp)| (stamp, owner.clone()))
            .collect();
        ordered.sort_by(|(stamp_a, owner_a), (stamp_b, owner_b)| {
            stamp_a.cmp(stamp_b).then_with(|| owner_a.cmp(owner_b))
        });
        assert_eq!(ordered.len(), 2);
        assert_eq!(ordered[0].1, t2);
        assert_eq!(ordered[1].1, incoming);
        assert!(ordered[0].0 < ordered[1].0);
    }

    #[test]
    fn evicting_admit_rejections_are_full_no_ops() {
        let wallet = pool_owner(ExplicitConsumer::Wallet, 1);
        let momentum = pool_owner(ExplicitConsumer::Momentum, 2);
        let fixtures = [
            (wallet.clone(), 1, vec![pk(1)]),
            (momentum.clone(), 2, vec![pk(2)]),
        ];
        let mut admission = admission_from_eviction_fixtures(2, &fixtures);
        let before = snapshot_admission_plan_state(&admission);

        let protected = admission.try_admit_with_eviction(
            pool_owner(ExplicitConsumer::Momentum, 9),
            vec![pk(10), pk(11)],
        );
        assert!(matches!(
            protected,
            EvictingAdmissionResult::RejectedProtected { .. }
        ));
        assert_admission_live_state_unchanged(&admission, &before);

        let invalid =
            admission.try_admit_with_eviction(pool_owner(ExplicitConsumer::Momentum, 10), vec![]);
        assert_eq!(invalid, EvictingAdmissionResult::RejectedInvalidInput);
        assert_admission_live_state_unchanged(&admission, &before);

        let mut existing_admission = admission_from_eviction_fixtures(5, &fixtures);
        assert_admitted(
            existing_admission
                .try_admit_new_group(pool_owner(ExplicitConsumer::Tracker, 99), [pk(99)]),
        );
        let before_existing = snapshot_admission_plan_state(&existing_admission);
        let existing = existing_admission
            .try_admit_with_eviction(pool_owner(ExplicitConsumer::Tracker, 99), vec![pk(100)]);
        assert_eq!(existing, EvictingAdmissionResult::RejectedExistingOwner);
        assert_admission_live_state_unchanged(&existing_admission, &before_existing);
    }

    #[test]
    fn evicting_admit_candidate_build_fault_seams_are_no_ops() {
        let tracker = pool_owner(ExplicitConsumer::Tracker, 1);
        let arb = pool_owner(ExplicitConsumer::Arb, 2);
        let fixtures = [
            (tracker.clone(), 10, vec![pk(1)]),
            (arb.clone(), 20, vec![pk(2)]),
        ];
        let incoming = pool_owner(ExplicitConsumer::Momentum, 9);
        let incoming_keys = vec![pk(10)];

        let mut omit = admission_from_eviction_fixtures(2, &fixtures);
        omit.set_test_force_evicting_omit_survivor(Some(arb.clone()));
        let before_omit = snapshot_admission_plan_state(&omit);
        assert_eq!(
            omit.try_admit_with_eviction(incoming.clone(), incoming_keys.clone()),
            EvictingAdmissionResult::InternalInvariantViolation
        );
        assert_admission_live_state_unchanged(&omit, &before_omit);

        let mut corrupt_len = admission_from_eviction_fixtures(2, &fixtures);
        corrupt_len.set_test_force_evicting_corrupt_physical_len(true);
        let before_len = snapshot_admission_plan_state(&corrupt_len);
        assert_eq!(
            corrupt_len.try_admit_with_eviction(incoming.clone(), incoming_keys.clone()),
            EvictingAdmissionResult::InternalInvariantViolation
        );
        assert_admission_live_state_unchanged(&corrupt_len, &before_len);

        let mut corrupt_stamp = admission_from_eviction_fixtures(2, &fixtures);
        corrupt_stamp.set_test_force_evicting_corrupt_survivor_stamp(Some(arb.clone()));
        let before_stamp = snapshot_admission_plan_state(&corrupt_stamp);
        assert_eq!(
            corrupt_stamp.try_admit_with_eviction(incoming.clone(), incoming_keys.clone()),
            EvictingAdmissionResult::InternalInvariantViolation
        );
        assert_admission_live_state_unchanged(&corrupt_stamp, &before_stamp);

        let mut stamp_fail = admission_from_eviction_fixtures(2, &fixtures);
        stamp_fail.set_test_force_stamp_plan_failure(true);
        let before_stamp_fail = snapshot_admission_plan_state(&stamp_fail);
        assert_eq!(
            stamp_fail.try_admit_with_eviction(incoming, incoming_keys),
            EvictingAdmissionResult::InternalInvariantViolation
        );
        assert_admission_live_state_unchanged(&stamp_fail, &before_stamp_fail);

        let mut extra = admission_from_eviction_fixtures(2, &fixtures);
        let ghost = pool_owner(ExplicitConsumer::Tracker, 99);
        extra.set_test_force_evicting_extra_candidate_owner(Some((ghost, vec![pk(99)])));
        let before_extra = snapshot_admission_plan_state(&extra);
        assert_eq!(
            extra.try_admit_with_eviction(pool_owner(ExplicitConsumer::Momentum, 9), vec![pk(10)],),
            EvictingAdmissionResult::InternalInvariantViolation
        );
        assert_admission_live_state_unchanged(&extra, &before_extra);

        let mut bad_next = admission_from_eviction_fixtures(2, &fixtures);
        bad_next.set_test_force_evicting_corrupt_next_touch_stamp(true);
        let before_bad_next = snapshot_admission_plan_state(&bad_next);
        assert_eq!(
            bad_next
                .try_admit_with_eviction(pool_owner(ExplicitConsumer::Momentum, 9), vec![pk(10)],),
            EvictingAdmissionResult::InternalInvariantViolation
        );
        assert_admission_live_state_unchanged(&bad_next, &before_bad_next);
    }

    #[test]
    fn evicting_admit_independent_oracle_matches_result_and_post_state() {
        for case in ref_evict_oracle_cases() {
            let mut admission = admission_from_eviction_fixtures(case.cap, &case.fixtures);
            let mut model = RefEvictingAdmissionModel::from_fixtures(case.cap, &case.fixtures);
            let oracle = RefEvictingOracle::from_state(&model.state.groups, &model.state.lru);
            let expected = oracle.predict(&case.incoming, &case.keys, case.cap);
            let before = snapshot_admission_plan_state(&admission);
            let actual =
                admission.try_admit_with_eviction(case.incoming.clone(), case.keys.clone());
            let model_result = model.try_admit_with_eviction(case.incoming, case.keys);
            assert_eq!(actual, expected, "admission result must match oracle");
            assert_eq!(model_result, expected, "model result must match oracle");
            if matches!(
                actual,
                EvictingAdmissionResult::RejectedExistingOwner
                    | EvictingAdmissionResult::RejectedInvalidInput
                    | EvictingAdmissionResult::RejectedProtected { .. }
                    | EvictingAdmissionResult::InternalInvariantViolation
            ) {
                assert_admission_live_state_unchanged(&admission, &before);
            } else {
                assert_evicting_model_matches_admission(&admission, &model);
            }
        }
    }

    #[test]
    fn evicting_admit_property_sequence_mixed_operations_with_model_parity() {
        let caps = [1usize, 2, 3, 5];
        let seeds = [3u8, 11, 29];

        for cap in caps {
            for seed in seeds {
                let mut admission = FixedCapAdmission::new(cap);
                let mut model = RefEvictingAdmissionModel::new(cap);
                let owners = [
                    pool_owner(ExplicitConsumer::Tracker, seed),
                    pool_owner(ExplicitConsumer::Arb, seed.wrapping_add(1)),
                    pool_owner(ExplicitConsumer::Momentum, seed.wrapping_add(2)),
                ];
                let key_pool: Vec<Pubkey> = (0u8..8).map(|b| pk(b.wrapping_add(seed))).collect();

                for (step, owner) in owners.iter().enumerate() {
                    let keys: Vec<Pubkey> = key_pool
                        .iter()
                        .copied()
                        .filter(|k| (k.to_bytes()[0] as usize + step) % 3 != 0)
                        .take(1 + step % 2)
                        .collect();
                    if keys.is_empty() {
                        continue;
                    }

                    let before = snapshot_admission_plan_state(&admission);
                    let oracle =
                        RefEvictingOracle::from_state(&model.state.groups, &model.state.lru);
                    let expected = oracle.predict(owner, &keys, cap);
                    let actual = admission.try_admit_with_eviction(owner.clone(), keys.clone());
                    let model_result = model.try_admit_with_eviction(owner.clone(), keys);
                    assert_eq!(actual, expected);
                    assert_eq!(model_result, expected);
                    if matches!(
                        actual,
                        EvictingAdmissionResult::RejectedExistingOwner
                            | EvictingAdmissionResult::RejectedInvalidInput
                            | EvictingAdmissionResult::RejectedProtected { .. }
                            | EvictingAdmissionResult::InternalInvariantViolation
                    ) {
                        assert_admission_live_state_unchanged(&admission, &before);
                    } else {
                        assert_evicting_model_matches_admission(&admission, &model);
                    }
                }

                if let Some(owner) = owners.first() {
                    if admission.owner_group(owner).is_some() {
                        let _ = admission.touch_group(owner.clone());
                        model.touch_group(owner);
                        assert_evicting_model_matches_admission(&admission, &model);
                    }
                }
                if owners.len() >= 2 {
                    let owner = owners[1].clone();
                    if admission.owner_group(&owner).is_some() {
                        let replace_keys = key_pool.iter().copied().take(2).collect::<Vec<_>>();
                        let _ = admission.try_replace_group(owner.clone(), replace_keys.clone());
                        let _ = model.try_replace_group(owner, replace_keys);
                        assert_evicting_model_matches_admission(&admission, &model);
                    }
                }
                if owners.len() >= 3 {
                    let owner = owners[2].clone();
                    if admission.owner_group(&owner).is_some() {
                        let _ = admission.remove_group(owner.clone());
                        let _ = model.remove_group(owner);
                        assert_evicting_model_matches_admission(&admission, &model);
                    }
                }
            }
        }
    }
}
