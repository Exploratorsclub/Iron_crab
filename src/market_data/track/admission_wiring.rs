//! Runtime wiring: converge tracked demand into [`FixedCapAdmission`] (I-MD-7 / I-MD-8).

use super::desired_set::ConsumerId;
use super::explicit_admission::{
    CapShrinkResult, EvictingAdmissionResult, FixedCapAdmission, FixedCapAdmissionResult,
    FixedCapReplaceResult,
};
use super::explicit_ownership::{
    ExplicitConsumer, ExplicitOwner, ExplicitOwnerKey, OwnerGroupSnapshot,
};
use solana_sdk::pubkey::Pubkey;
use std::collections::{BTreeMap, BTreeSet, HashSet};

/// Result of reconciling authoritative owner demand into admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionConvergeResult {
    Converged,
    ProtectedOverflow,
    Unconverged,
}

/// Result of cold restore from snapshot owner groups.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionRestoreResult {
    Restored,
    ProtectedOverflow,
    Unconverged,
}

pub fn consumer_id_to_explicit(consumer: ConsumerId) -> ExplicitConsumer {
    match consumer {
        ConsumerId::Wallet => ExplicitConsumer::Wallet,
        ConsumerId::Momentum => ExplicitConsumer::Momentum,
        ConsumerId::Arb => ExplicitConsumer::Arb,
        ConsumerId::Tracker => ExplicitConsumer::Tracker,
    }
}

pub fn explicit_owner_from_row(
    consumer: ConsumerId,
    pool: Option<Pubkey>,
    pubkey: Pubkey,
) -> ExplicitOwner {
    let explicit_consumer = consumer_id_to_explicit(consumer);
    let owner_key = if explicit_consumer == ExplicitConsumer::Wallet {
        ExplicitOwnerKey::Wallet
    } else if let Some(pool_pk) = pool {
        ExplicitOwnerKey::Pool(pool_pk)
    } else {
        ExplicitOwnerKey::Mint(pubkey)
    };
    ExplicitOwner {
        consumer: explicit_consumer,
        owner_key,
    }
}

/// Group flat pubkey rows into owner groups for admission.
pub fn rows_to_owner_groups(
    rows: &[(Pubkey, ConsumerId, Option<Pubkey>)],
) -> Vec<(ExplicitOwner, Vec<Pubkey>)> {
    let mut groups: BTreeMap<ExplicitOwner, BTreeSet<Pubkey>> = BTreeMap::new();
    for (pk, consumer, pool) in rows {
        let owner = explicit_owner_from_row(*consumer, *pool, *pk);
        groups.entry(owner).or_default().insert(*pk);
    }
    groups
        .into_iter()
        .map(|(owner, set)| (owner, set.into_iter().collect()))
        .collect()
}

/// Preserve Tracker-labelled owner groups already in admission (e.g. cold restore) when ctx
/// rows omit them transiently during converge.
pub fn merge_admission_tracker_owner_groups(
    admission: &FixedCapAdmission,
    authoritative: &mut Vec<(ExplicitOwner, Vec<Pubkey>)>,
) {
    let mut present: BTreeSet<(ExplicitConsumer, ExplicitOwnerKey)> = authoritative
        .iter()
        .map(|(owner, _)| owner_group_key(owner))
        .collect();

    for group in admission.snapshot_owner_groups() {
        if group.consumer != ExplicitConsumer::Tracker {
            continue;
        }
        let owner = ExplicitOwner {
            consumer: group.consumer,
            owner_key: group.owner_key.clone(),
        };
        let key = owner_group_key(&owner);
        if present.insert(key) {
            authoritative.push((owner, group.pubkeys.clone()));
        }
    }
}

fn owner_group_key(owner: &ExplicitOwner) -> (ExplicitConsumer, ExplicitOwnerKey) {
    (owner.consumer, owner.owner_key.clone())
}

/// Pool pubkey for pool-scoped owner groups (`ExplicitOwnerKey::Pool`).
pub fn pool_pubkey_from_owner_group(group: &OwnerGroupSnapshot) -> Option<Pubkey> {
    match group.owner_key {
        ExplicitOwnerKey::Pool(pool) => Some(pool),
        _ => None,
    }
}

/// Derive bypass-gate pool sets from [`FixedCapAdmission`] owner groups (SSOT).
pub fn explicit_admitted_pool_sets_from_admission(
    admission: &FixedCapAdmission,
) -> (HashSet<Pubkey>, HashSet<Pubkey>) {
    let mut momentum = HashSet::new();
    let mut arb = HashSet::new();
    for group in admission.snapshot_owner_groups() {
        let Some(pool) = pool_pubkey_from_owner_group(&group) else {
            continue;
        };
        match group.consumer {
            ExplicitConsumer::Momentum => {
                momentum.insert(pool);
            }
            ExplicitConsumer::Arb => {
                arb.insert(pool);
            }
            ExplicitConsumer::Wallet | ExplicitConsumer::Tracker => {}
        }
    }
    (momentum, arb)
}

/// Admit or replace one immutable owner group (eviction when required).
pub fn try_admit_owner_group(
    admission: &mut FixedCapAdmission,
    owner: ExplicitOwner,
    pubkeys: Vec<Pubkey>,
) -> bool {
    if pubkeys.is_empty() {
        return true;
    }
    if admission.owner_group(&owner).is_some() {
        let current: BTreeSet<Pubkey> = admission
            .owner_group(&owner)
            .unwrap()
            .iter()
            .copied()
            .collect();
        let next: BTreeSet<Pubkey> = pubkeys.iter().copied().collect();
        if current == next {
            let _ = admission.touch_group(owner);
            return true;
        }
        match admission.try_replace_group(owner.clone(), pubkeys.clone()) {
            FixedCapReplaceResult::Replaced { .. } | FixedCapReplaceResult::Unchanged => true,
            FixedCapReplaceResult::RejectedCap { .. } => {
                let _ = admission.remove_group(owner.clone());
                matches!(
                    admission.try_admit_with_eviction(owner, pubkeys),
                    EvictingAdmissionResult::InsertedNoEviction { .. }
                        | EvictingAdmissionResult::InsertedWithEviction { .. }
                )
            }
            FixedCapReplaceResult::RejectedInvalidGroup
            | FixedCapReplaceResult::RejectedMissingOwner
            | FixedCapReplaceResult::PlanningInvariantViolation
            | FixedCapReplaceResult::InternalInvariantViolation { .. } => false,
        }
    } else {
        match admission.try_admit_new_group(owner.clone(), pubkeys.clone()) {
            FixedCapAdmissionResult::Inserted { .. }
            | FixedCapAdmissionResult::OwnerAddedNoNewPubkey => true,
            FixedCapAdmissionResult::RejectedCap { .. } => matches!(
                admission.try_admit_with_eviction(owner, pubkeys),
                EvictingAdmissionResult::InsertedNoEviction { .. }
                    | EvictingAdmissionResult::InsertedWithEviction { .. }
            ),
            FixedCapAdmissionResult::RejectedInvalidGroup
            | FixedCapAdmissionResult::RejectedExistingOwner
            | FixedCapAdmissionResult::InternalInvariantViolation => false,
        }
    }
}

/// Reconcile authoritative demand into admission (eviction when required).
pub fn converge_admission_from_groups(
    admission: &mut FixedCapAdmission,
    authoritative: &[(ExplicitOwner, Vec<Pubkey>)],
) -> AdmissionConvergeResult {
    let cap = admission.cap();
    if admission.wallet_demand_exceeds_cap(cap) {
        return AdmissionConvergeResult::ProtectedOverflow;
    }

    let auth_keys: BTreeSet<(ExplicitConsumer, ExplicitOwnerKey)> = authoritative
        .iter()
        .map(|(owner, _)| owner_group_key(owner))
        .collect();

    for group in admission.snapshot_owner_groups() {
        let owner = ExplicitOwner {
            consumer: group.consumer,
            owner_key: group.owner_key.clone(),
        };
        if !auth_keys.contains(&owner_group_key(&owner)) {
            let _ = admission.remove_group(owner);
        }
    }

    let mut ordered = authoritative.to_vec();
    ordered.sort_by(|(a, _), (b, _)| a.cmp(b));

    for (owner, pubkeys) in ordered {
        if !try_admit_owner_group(admission, owner, pubkeys) {
            if admission.wallet_demand_exceeds_cap(cap) {
                return AdmissionConvergeResult::ProtectedOverflow;
            }
            return AdmissionConvergeResult::Unconverged;
        }
    }

    if admission.len() > cap || admission.wallet_demand_exceeds_cap(cap) {
        if admission.wallet_demand_exceeds_cap(cap) {
            AdmissionConvergeResult::ProtectedOverflow
        } else {
            AdmissionConvergeResult::Unconverged
        }
    } else {
        AdmissionConvergeResult::Converged
    }
}

/// Cold restore: admit snapshot owner groups in deterministic order.
pub fn restore_admission_from_owner_groups(
    admission: &mut FixedCapAdmission,
    groups: &[OwnerGroupSnapshot],
) -> AdmissionRestoreResult {
    admission.clear_for_restore();

    let cap = admission.cap();
    let wallet_pubkeys: BTreeSet<Pubkey> = groups
        .iter()
        .filter(|g| g.consumer == ExplicitConsumer::Wallet)
        .flat_map(|g| g.pubkeys.iter().copied())
        .collect();
    if wallet_pubkeys.len() > cap {
        return AdmissionRestoreResult::ProtectedOverflow;
    }

    let mut ordered: Vec<&OwnerGroupSnapshot> = groups.iter().collect();
    ordered.sort_by(|a, b| {
        a.consumer
            .cmp(&b.consumer)
            .then_with(|| a.owner_key.cmp(&b.owner_key))
    });

    for group in ordered {
        let owner = ExplicitOwner {
            consumer: group.consumer,
            owner_key: group.owner_key.clone(),
        };
        if !try_admit_owner_group(admission, owner, group.pubkeys.clone()) {
            if admission.wallet_demand_exceeds_cap(cap) {
                return AdmissionRestoreResult::ProtectedOverflow;
            }
            return AdmissionRestoreResult::Unconverged;
        }
    }

    if admission.len() > cap {
        AdmissionRestoreResult::Unconverged
    } else if admission.wallet_demand_exceeds_cap(cap) {
        AdmissionRestoreResult::ProtectedOverflow
    } else {
        AdmissionRestoreResult::Restored
    }
}

pub fn admitted_pubkey_set(admission: &FixedCapAdmission) -> HashSet<Pubkey> {
    admission.snapshot_pubkeys().into_iter().collect()
}

/// True when admission hard cap or physical len exceeds the configured `max_tracked_accounts`.
pub fn admission_exceeds_configured_cap(
    admission: &FixedCapAdmission,
    configured_cap: usize,
) -> bool {
    admission.cap() > configured_cap || admission.len() > configured_cap
}

/// Partition admitted pubkeys into merge-task channels so their union equals `admitted`.
///
/// Tracked-map membership classifies known kinds; any admitted pubkey without a map row
/// (e.g. restored Tracker admission) is routed to `mints` so Geyser still receives it.
pub fn partition_admitted_pubkeys_for_geyser_channels(
    admitted: &HashSet<Pubkey>,
    mint_keys: &HashSet<Pubkey>,
    vault_keys: &HashSet<Pubkey>,
    bin_keys: &HashSet<Pubkey>,
    wallet_keys: &HashSet<Pubkey>,
) -> (Vec<Pubkey>, Vec<Pubkey>, Vec<Pubkey>, Vec<Pubkey>) {
    let mut mints = Vec::new();
    let mut vaults = Vec::new();
    let mut bins = Vec::new();
    let mut wallets = Vec::new();
    let mut classified = HashSet::new();

    for pk in admitted {
        if wallet_keys.contains(pk) {
            wallets.push(*pk);
            classified.insert(*pk);
        }
    }
    for pk in admitted {
        if classified.contains(pk) {
            continue;
        }
        if vault_keys.contains(pk) {
            vaults.push(*pk);
            classified.insert(*pk);
        }
    }
    for pk in admitted {
        if classified.contains(pk) {
            continue;
        }
        if bin_keys.contains(pk) {
            bins.push(*pk);
            classified.insert(*pk);
        }
    }
    for pk in admitted {
        if classified.contains(pk) {
            continue;
        }
        if mint_keys.contains(pk) {
            mints.push(*pk);
            classified.insert(*pk);
        }
    }
    for pk in admitted {
        if !classified.contains(pk) {
            mints.push(*pk);
        }
    }

    mints.sort();
    vaults.sort();
    bins.sort();
    wallets.sort();
    (mints, vaults, bins, wallets)
}

pub fn apply_cap_shrink(admission: &mut FixedCapAdmission, new_cap: usize) -> CapShrinkResult {
    admission.try_shrink_cap(new_cap)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pool_owner(consumer: ExplicitConsumer, pool: Pubkey) -> ExplicitOwner {
        ExplicitOwner {
            consumer,
            owner_key: ExplicitOwnerKey::Pool(pool),
        }
    }

    #[test]
    fn wallet_over_cap_restore_fail_closed() {
        let mut admission = FixedCapAdmission::new(2);
        let groups = vec![OwnerGroupSnapshot {
            consumer: ExplicitConsumer::Wallet,
            owner_key: ExplicitOwnerKey::Wallet,
            pubkeys: vec![
                Pubkey::new_unique(),
                Pubkey::new_unique(),
                Pubkey::new_unique(),
            ],
        }];
        assert_eq!(
            restore_admission_from_owner_groups(&mut admission, &groups),
            AdmissionRestoreResult::ProtectedOverflow
        );
    }

    #[test]
    fn converge_wallet_mint_row() {
        let mut admission = FixedCapAdmission::new(25_000);
        let mint = Pubkey::new_unique();
        let owner = explicit_owner_from_row(ConsumerId::Wallet, None, mint);
        assert_eq!(
            converge_admission_from_groups(&mut admission, &[(owner, vec![mint])]),
            AdmissionConvergeResult::Converged
        );
        assert_eq!(admission.len(), 1);
    }

    #[test]
    fn admission_exceeds_configured_cap_when_hard_cap_desynced() {
        let mut admission = FixedCapAdmission::new(10_000);
        assert!(!admission_exceeds_configured_cap(&admission, 10_000));
        assert!(admission_exceeds_configured_cap(&admission, 5_000));
        let owner = pool_owner(ExplicitConsumer::Momentum, Pubkey::new_unique());
        assert!(try_admit_owner_group(
            &mut admission,
            owner,
            vec![Pubkey::new_unique()]
        ));
        assert!(!admission_exceeds_configured_cap(&admission, 10_000));
        assert!(admission_exceeds_configured_cap(&admission, 0));
    }

    #[test]
    fn partition_admitted_includes_keys_without_tracked_map_rows() {
        let admitted: HashSet<Pubkey> = [
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
        ]
        .into_iter()
        .collect();
        let vault_pk = *admitted.iter().next().unwrap();
        let wallet_pk = admitted.iter().nth(1).copied().unwrap();
        let orphan_pk = admitted.iter().nth(2).copied().unwrap();
        let (mints, vaults, bins, wallets) = partition_admitted_pubkeys_for_geyser_channels(
            &admitted,
            &HashSet::new(),
            &HashSet::from([vault_pk]),
            &HashSet::new(),
            &HashSet::from([wallet_pk]),
        );
        assert_eq!(wallets, vec![wallet_pk]);
        assert_eq!(vaults, vec![vault_pk]);
        assert_eq!(bins, Vec::<Pubkey>::new());
        assert_eq!(mints, vec![orphan_pk]);
        let combined: HashSet<Pubkey> = mints
            .into_iter()
            .chain(vaults)
            .chain(bins)
            .chain(wallets)
            .collect();
        assert_eq!(combined, admitted);
    }

    #[test]
    fn converge_preserves_restored_tracker_when_ctx_has_arb_same_pool() {
        let mut admission = FixedCapAdmission::new(20);
        let pool = Pubkey::new_unique();
        let tracker_pk = Pubkey::new_unique();
        let arb_pk = Pubkey::new_unique();
        let tracker_owner = pool_owner(ExplicitConsumer::Tracker, pool);
        assert!(try_admit_owner_group(
            &mut admission,
            tracker_owner.clone(),
            vec![tracker_pk]
        ));

        let arb_owner = pool_owner(ExplicitConsumer::Arb, pool);
        let mut authoritative = vec![(arb_owner, vec![arb_pk])];
        merge_admission_tracker_owner_groups(&admission, &mut authoritative);
        assert_eq!(
            converge_admission_from_groups(&mut admission, &authoritative),
            AdmissionConvergeResult::Converged
        );
        assert!(admission.owner_group(&tracker_owner).is_some());
        assert!(admission.contains(&tracker_pk));
        assert!(admission.contains(&arb_pk));
    }

    #[test]
    fn converge_removes_stale_owner() {
        let mut admission = FixedCapAdmission::new(10);
        let stale_pool = Pubkey::new_unique();
        let stale_pk = Pubkey::new_unique();
        assert!(try_admit_owner_group(
            &mut admission,
            pool_owner(ExplicitConsumer::Momentum, stale_pool),
            vec![stale_pk]
        ));
        assert!(admission.contains(&stale_pk));
        assert_eq!(
            converge_admission_from_groups(&mut admission, &[]),
            AdmissionConvergeResult::Converged
        );
        assert!(!admission.contains(&stale_pk));
    }

    #[test]
    fn explicit_admitted_pool_sets_from_admission_maps_pool_owner_groups() {
        let mut admission = FixedCapAdmission::new(100);
        let mom_pool = Pubkey::new_unique();
        let arb_pool = Pubkey::new_unique();
        assert!(try_admit_owner_group(
            &mut admission,
            pool_owner(ExplicitConsumer::Momentum, mom_pool),
            vec![Pubkey::new_unique()]
        ));
        assert!(try_admit_owner_group(
            &mut admission,
            pool_owner(ExplicitConsumer::Arb, arb_pool),
            vec![Pubkey::new_unique()]
        ));
        let (momentum, arb) = explicit_admitted_pool_sets_from_admission(&admission);
        assert_eq!(momentum, std::collections::HashSet::from([mom_pool]));
        assert_eq!(arb, std::collections::HashSet::from([arb_pool]));
    }

    #[test]
    fn explicit_admitted_pool_sets_omit_wallet_and_tracker_pool_groups() {
        let mut admission = FixedCapAdmission::new(100);
        let pool = Pubkey::new_unique();
        assert!(try_admit_owner_group(
            &mut admission,
            pool_owner(ExplicitConsumer::Tracker, pool),
            vec![Pubkey::new_unique()]
        ));
        let (momentum, arb) = explicit_admitted_pool_sets_from_admission(&admission);
        assert!(momentum.is_empty());
        assert!(arb.is_empty());
    }
}
