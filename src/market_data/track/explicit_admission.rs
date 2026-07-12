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

/// Test-only counters to prove admission planning avoids full-graph scans/clones.
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
    ownership: ExplicitOwnership,
    #[cfg(test)]
    planning_stats: PlanningStats,
}

impl FixedCapAdmission {
    pub fn new(cap: usize) -> Self {
        Self {
            cap,
            ownership: ExplicitOwnership::new(),
            #[cfg(test)]
            planning_stats: PlanningStats::default(),
        }
    }

    pub fn cap(&self) -> usize {
        self.cap
    }

    pub fn len(&self) -> usize {
        self.ownership.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ownership.is_empty()
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

        let projection = self.project_admission(&owner, &normalized);

        match projection {
            AdmissionProjection::Unchanged => FixedCapAdmissionResult::Unchanged,
            AdmissionProjection::RejectCap {
                required_unique,
                available_unique,
            } => FixedCapAdmissionResult::RejectedCap {
                required_unique,
                available_unique,
            },
            AdmissionProjection::Admit => match self.ownership.upsert_group(owner, normalized) {
                Ok(GroupChange::NewGroup { physical_added }) => {
                    if physical_added.is_empty() {
                        FixedCapAdmissionResult::OwnerAddedNoNewPubkey
                    } else {
                        FixedCapAdmissionResult::Inserted { physical_added }
                    }
                }
                Ok(GroupChange::Unchanged) => FixedCapAdmissionResult::Unchanged,
                Ok(GroupChange::Replaced {
                    physical_added,
                    physical_removed,
                }) => FixedCapAdmissionResult::OwnerReplaced {
                    physical_added,
                    physical_removed,
                },
                Err(EmptyOwnerGroupError) => FixedCapAdmissionResult::RejectedInvalidGroup,
            },
        }
    }

    /// Remove an owner group and return exact physical pubkey deltas.
    pub fn remove_group(&mut self, owner: &ExplicitOwner) -> Option<FixedCapRemoveResult> {
        let pubkeys = self.ownership.owner_group(owner)?;
        let mut physical_removed = Vec::new();
        for pubkey in pubkeys {
            if self.ownership.owner_refcount(pubkey) == 1 {
                physical_removed.push(*pubkey);
            }
        }
        physical_removed.sort();
        let snapshot = self.ownership.remove_group(owner)?;
        Some(FixedCapRemoveResult {
            snapshot,
            physical_removed,
        })
    }

    fn project_admission(
        &mut self,
        owner: &ExplicitOwner,
        normalized: &[Pubkey],
    ) -> AdmissionProjection {
        #[cfg(test)]
        self.planning_stats.record_owner_group_lookup();

        let current_len = self.ownership.len();

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
                    return AdmissionProjection::RejectCap {
                        required_unique: physical_added,
                        available_unique,
                    };
                }

                let Some(projected_len) = current_len
                    .checked_add(physical_added)
                    .filter(|&len| len <= self.cap)
                else {
                    return AdmissionProjection::RejectCap {
                        required_unique: physical_added,
                        available_unique,
                    };
                };

                debug_assert_eq!(projected_len, current_len + physical_added);
                AdmissionProjection::Admit
            }
            Some(existing) if existing == normalized => AdmissionProjection::Unchanged,
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
                    return AdmissionProjection::RejectCap {
                        required_unique: physical_added,
                        available_unique,
                    };
                }

                let Some(after_free) = current_len.checked_sub(physical_removed) else {
                    return AdmissionProjection::RejectCap {
                        required_unique: physical_added,
                        available_unique,
                    };
                };
                let Some(projected_len) = after_free
                    .checked_add(physical_added)
                    .filter(|&len| len <= self.cap)
                else {
                    return AdmissionProjection::RejectCap {
                        required_unique: physical_added,
                        available_unique,
                    };
                };

                debug_assert_eq!(projected_len, after_free + physical_added);
                AdmissionProjection::Admit
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AdmissionProjection {
    Unchanged,
    Admit,
    RejectCap {
        required_unique: usize,
        available_unique: usize,
    },
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

        fn project(
            &self,
            owner: &ExplicitOwner,
            normalized: &BTreeSet<Pubkey>,
        ) -> Result<AdmissionProjection, ()> {
            let current_len = self.len();

            match self.groups.get(owner) {
                None => {
                    let physical_added = normalized
                        .iter()
                        .filter(|pk| self.owner_refcount(pk) == 0)
                        .count();
                    let available_unique = self.cap.saturating_sub(current_len);
                    if physical_added > available_unique {
                        return Ok(AdmissionProjection::RejectCap {
                            required_unique: physical_added,
                            available_unique,
                        });
                    }
                    let Some(projected_len) = current_len
                        .checked_add(physical_added)
                        .filter(|&len| len <= self.cap)
                    else {
                        return Ok(AdmissionProjection::RejectCap {
                            required_unique: physical_added,
                            available_unique,
                        });
                    };
                    debug_assert_eq!(projected_len, current_len + physical_added);
                    Ok(AdmissionProjection::Admit)
                }
                Some(existing) if existing == normalized => Ok(AdmissionProjection::Unchanged),
                Some(old_pubkeys) => {
                    let physical_added = normalized
                        .difference(old_pubkeys)
                        .filter(|pk| self.owner_refcount(pk) == 0)
                        .count();
                    let physical_removed = old_pubkeys
                        .difference(normalized)
                        .filter(|pk| self.owner_refcount(pk) == 1)
                        .count();
                    let available_unique = self
                        .cap
                        .saturating_sub(current_len)
                        .saturating_add(physical_removed);
                    if physical_added > available_unique {
                        return Ok(AdmissionProjection::RejectCap {
                            required_unique: physical_added,
                            available_unique,
                        });
                    }
                    let Some(after_free) = current_len.checked_sub(physical_removed) else {
                        return Ok(AdmissionProjection::RejectCap {
                            required_unique: physical_added,
                            available_unique,
                        });
                    };
                    let Some(projected_len) = after_free
                        .checked_add(physical_added)
                        .filter(|&len| len <= self.cap)
                    else {
                        return Ok(AdmissionProjection::RejectCap {
                            required_unique: physical_added,
                            available_unique,
                        });
                    };
                    debug_assert_eq!(projected_len, after_free + physical_added);
                    Ok(AdmissionProjection::Admit)
                }
            }
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

            let before = self.index_snapshot();
            let projection = self.project(&owner, &normalized).unwrap();

            match projection {
                AdmissionProjection::Unchanged => FixedCapAdmissionResult::Unchanged,
                AdmissionProjection::RejectCap {
                    required_unique,
                    available_unique,
                } => {
                    assert_eq!(self.index_snapshot(), before);
                    FixedCapAdmissionResult::RejectedCap {
                        required_unique,
                        available_unique,
                    }
                }
                AdmissionProjection::Admit => {
                    let physical_before = physical_pubkeys_from_groups(&self.groups);
                    match self.groups.get(&owner) {
                        None => {
                            self.groups.insert(owner, normalized.clone());
                            let physical_after = physical_pubkeys_from_groups(&self.groups);
                            let physical_added: Vec<Pubkey> = physical_after
                                .difference(&physical_before)
                                .copied()
                                .collect();
                            if physical_added.is_empty() {
                                FixedCapAdmissionResult::OwnerAddedNoNewPubkey
                            } else {
                                FixedCapAdmissionResult::Inserted { physical_added }
                            }
                        }
                        Some(existing) if existing == &normalized => {
                            FixedCapAdmissionResult::Unchanged
                        }
                        Some(_) => {
                            self.groups.insert(owner, normalized);
                            let physical_after = physical_pubkeys_from_groups(&self.groups);
                            let physical_added: Vec<Pubkey> = physical_after
                                .difference(&physical_before)
                                .copied()
                                .collect();
                            let physical_removed: Vec<Pubkey> = physical_before
                                .difference(&physical_after)
                                .copied()
                                .collect();
                            FixedCapAdmissionResult::OwnerReplaced {
                                physical_added,
                                physical_removed,
                            }
                        }
                    }
                }
            }
        }

        fn remove_group(&mut self, owner: &ExplicitOwner) -> Option<FixedCapRemoveResult> {
            let pubkeys = self.groups.remove(owner)?;
            let physical_before = physical_pubkeys_from_groups(&self.groups);
            let snapshot = OwnerGroupSnapshot {
                consumer: owner.consumer,
                owner_key: owner.owner_key.clone(),
                pubkeys: pubkeys.iter().copied().collect(),
            };
            let physical_after = physical_pubkeys_from_groups(&self.groups);
            let physical_removed: Vec<Pubkey> = physical_before
                .difference(&physical_after)
                .copied()
                .collect();
            Some(FixedCapRemoveResult {
                snapshot,
                physical_removed,
            })
        }
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

        let removed = admission.remove_group(&owner_a).unwrap();
        assert_eq!(removed.physical_removed, vec![pk(2)]);
        assert_eq!(admission.len(), 1);
        assert!(admission.contains_pubkey(&shared));

        let removed_b = admission.remove_group(&owner_b).unwrap();
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
    fn planning_stats_prove_no_full_graph_scan_in_normal_path() {
        let mut admission = FixedCapAdmission::new(4);
        let owner = pool_owner(ExplicitConsumer::Momentum, 1);
        let _ = admission.try_admit_group(owner.clone(), [pk(1), pk(2)]);
        let stats_after_insert = admission.planning_stats().clone();

        let _ = admission.try_admit_group(owner.clone(), [pk(2), pk(3)]);
        let stats_after_replace = admission.planning_stats().clone();

        let _ = admission.try_admit_group(owner.clone(), [pk(2), pk(3)]);
        let stats_after_unchanged = admission.planning_stats().clone();

        assert!(stats_after_insert.refcount_lookups > 0);
        assert_eq!(stats_after_insert.owner_group_lookups, 1);
        assert!(stats_after_replace.refcount_lookups > stats_after_insert.refcount_lookups);
        assert_eq!(stats_after_replace.owner_group_lookups, 2);
        assert_eq!(
            stats_after_unchanged.refcount_lookups,
            stats_after_replace.refcount_lookups
        );
        assert_eq!(stats_after_unchanged.owner_group_lookups, 3);
    }

    #[test]
    fn bounded_reference_model_matches_fixed_cap_admission() {
        let caps = [0usize, 1, 2, 3, 5];
        let seeds: [u64; 6] = [1, 7, 13, 42, 99, 255];

        for cap in caps {
            for seed in seeds {
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
                assert_eq!(
                    removed.map(|r| r.physical_removed),
                    ref_removed.map(|r| r.physical_removed)
                );
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
