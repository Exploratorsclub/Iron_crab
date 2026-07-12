//! Pure priority/LRU eviction planner (PR 1c1).
//!
//! Side-effect-free: no admission mutation, touch-state, cap-shrink, or runtime wiring.

use super::explicit_ownership::{ExplicitConsumer, ExplicitOwner, ExplicitOwnerKey};
use solana_sdk::pubkey::Pubkey;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

/// One evictable owner group with an external LRU stamp.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvictionCandidate {
    pub owner: ExplicitOwner,
    pub consumer: ExplicitConsumer,
    pub last_touch: u64,
    pub pubkeys: Vec<Pubkey>,
}

/// Admission/eviction planning request for one incoming owner group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvictionRequest {
    pub incoming_owner: ExplicitOwner,
    pub incoming_consumer: ExplicitConsumer,
    pub incoming_pubkeys: Vec<Pubkey>,
    pub current_physical_len: usize,
    pub cap: usize,
}

/// Successful eviction plan — victims in actual selection order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvictionPlan {
    pub victims: Vec<EvictionCandidate>,
    pub physical_freed: Vec<Pubkey>,
    pub incoming_physical_added: Vec<Pubkey>,
    pub projected_final_len: usize,
}

/// Planner outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EvictionPlanResult {
    NoEvictionNeeded,
    Planned(EvictionPlan),
    RejectedProtected,
    RejectedInvalidInput,
    InternalInvariantViolation,
}

/// Test-only instrumentation counters.
#[cfg(test)]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlannerStats {
    pub initial_group_key_edges: u64,
    pub incremental_refcount_updates: u64,
    pub candidate_checks: u64,
    pub package_checks: u64,
    pub full_graph_clones: u64,
}

/// Plan eviction for `request` against immutable `candidates`.
pub fn plan_eviction(
    candidates: &[EvictionCandidate],
    request: &EvictionRequest,
) -> EvictionPlanResult {
    plan_eviction_inner(candidates, request, &mut NoopPlannerStats)
}

#[cfg(test)]
pub fn plan_eviction_with_stats(
    candidates: &[EvictionCandidate],
    request: &EvictionRequest,
    stats: Option<&mut PlannerStats>,
) -> EvictionPlanResult {
    match stats {
        Some(s) => plan_eviction_inner(candidates, request, s),
        None => plan_eviction_inner(candidates, request, &mut NoopPlannerStats),
    }
}

#[cfg(test)]
struct NoopPlannerStats;

#[cfg(test)]
impl PlannerStatsSink for NoopPlannerStats {
    fn record_initial_edge(&mut self) {}
    fn record_refcount_update(&mut self) {}
    fn record_candidate_check(&mut self) {}
    fn record_package_check(&mut self) {}
}

trait PlannerStatsSink {
    fn record_initial_edge(&mut self);
    fn record_refcount_update(&mut self);
    fn record_candidate_check(&mut self);
    fn record_package_check(&mut self);
}

#[cfg(test)]
impl PlannerStatsSink for PlannerStats {
    fn record_initial_edge(&mut self) {
        self.initial_group_key_edges += 1;
    }
    fn record_refcount_update(&mut self) {
        self.incremental_refcount_updates += 1;
    }
    fn record_candidate_check(&mut self) {
        self.candidate_checks += 1;
    }
    fn record_package_check(&mut self) {
        self.package_checks += 1;
    }
}

#[cfg(not(test))]
struct NoopPlannerStats;

#[cfg(not(test))]
impl PlannerStatsSink for NoopPlannerStats {
    fn record_initial_edge(&mut self) {}
    fn record_refcount_update(&mut self) {}
    fn record_candidate_check(&mut self) {}
    fn record_package_check(&mut self) {}
}

fn plan_eviction_inner<S: PlannerStatsSink>(
    candidates: &[EvictionCandidate],
    request: &EvictionRequest,
    stats: &mut S,
) -> EvictionPlanResult {
    let incoming_pubkeys = match normalize_pubkeys(request.incoming_pubkeys.iter().copied()) {
        Ok(keys) => keys,
        Err(()) => return EvictionPlanResult::RejectedInvalidInput,
    };
    if incoming_pubkeys.is_empty() {
        return EvictionPlanResult::RejectedInvalidInput;
    }

    let normalized_candidates = match normalize_candidates(candidates) {
        Ok(c) => c,
        Err(()) => return EvictionPlanResult::RejectedInvalidInput,
    };

    if normalized_candidates
        .iter()
        .any(|c| c.owner == request.incoming_owner)
    {
        return EvictionPlanResult::RejectedInvalidInput;
    }

    let mut projected_refcounts: HashMap<Pubkey, usize> = HashMap::new();
    for candidate in &normalized_candidates {
        for pubkey in &candidate.pubkeys {
            stats.record_initial_edge();
            *projected_refcounts.entry(*pubkey).or_default() += 1;
        }
    }

    let incoming_set: BTreeSet<Pubkey> = incoming_pubkeys.iter().copied().collect();
    let mut incoming_physical_added = Vec::new();
    for pubkey in &incoming_pubkeys {
        if projected_refcounts.get(pubkey).copied().unwrap_or(0) == 0 {
            incoming_physical_added.push(*pubkey);
        }
    }

    let Some(after_incoming) = request
        .current_physical_len
        .checked_add(incoming_physical_added.len())
    else {
        return EvictionPlanResult::InternalInvariantViolation;
    };

    if after_incoming <= request.cap {
        return EvictionPlanResult::NoEvictionNeeded;
    }

    let Some(required_to_free) = after_incoming.checked_sub(request.cap) else {
        return EvictionPlanResult::InternalInvariantViolation;
    };

    let mut victims: Vec<EvictionCandidate> = Vec::new();
    let mut removed: HashSet<usize> = HashSet::new();
    let mut physical_freed: BTreeSet<Pubkey> = BTreeSet::new();
    let mut remaining = required_to_free;

    let evictable_tiers = [
        ExplicitConsumer::Tracker,
        ExplicitConsumer::Arb,
        ExplicitConsumer::Momentum,
    ];
    let mut open_tier_count = 0usize;

    while remaining > 0 {
        while open_tier_count < evictable_tiers.len() {
            let max_freeable = max_freeable_in_tiers(
                &normalized_candidates,
                &removed,
                &projected_refcounts,
                &incoming_set,
                request.incoming_consumer,
                &evictable_tiers[..open_tier_count],
            );
            if max_freeable >= remaining {
                break;
            }
            open_tier_count += 1;
            if open_tier_count == evictable_tiers.len() {
                break;
            }
        }

        if open_tier_count == 0 {
            open_tier_count = 1;
        }

        let open_tiers: BTreeSet<ExplicitConsumer> =
            evictable_tiers[..open_tier_count].iter().copied().collect();

        if let Some(idx) = select_positive_marginal(
            &normalized_candidates,
            &removed,
            &projected_refcounts,
            &incoming_set,
            request.incoming_consumer,
            &open_tiers,
            stats,
        ) {
            let mut state = PlanState {
                removed: &mut removed,
                projected_refcounts: &mut projected_refcounts,
                incoming_set: &incoming_set,
                victims: &mut victims,
                physical_freed: &mut physical_freed,
                remaining: &mut remaining,
            };
            apply_victim(idx, &normalized_candidates, &mut state, stats);
            continue;
        }

        if open_tier_count < evictable_tiers.len() {
            open_tier_count += 1;
            continue;
        }

        if let Some(package) = select_joint_package(
            &normalized_candidates,
            &removed,
            &projected_refcounts,
            &incoming_set,
            request.incoming_consumer,
            &open_tiers,
            stats,
        ) {
            let mut state = PlanState {
                removed: &mut removed,
                projected_refcounts: &mut projected_refcounts,
                incoming_set: &incoming_set,
                victims: &mut victims,
                physical_freed: &mut physical_freed,
                remaining: &mut remaining,
            };
            for idx in package {
                apply_victim(idx, &normalized_candidates, &mut state, stats);
            }
            continue;
        }

        return EvictionPlanResult::RejectedProtected;
    }

    let physical_freed_vec: Vec<Pubkey> = physical_freed.into_iter().collect();
    let freed_count = physical_freed_vec.len();
    let Some(projected_final_len) = request
        .current_physical_len
        .checked_sub(freed_count)
        .and_then(|len| len.checked_add(incoming_physical_added.len()))
    else {
        return EvictionPlanResult::InternalInvariantViolation;
    };

    if projected_final_len > request.cap {
        return EvictionPlanResult::InternalInvariantViolation;
    }

    EvictionPlanResult::Planned(EvictionPlan {
        victims,
        physical_freed: physical_freed_vec,
        incoming_physical_added,
        projected_final_len,
    })
}

fn normalize_candidates(candidates: &[EvictionCandidate]) -> Result<Vec<EvictionCandidate>, ()> {
    let mut seen = HashSet::new();
    let mut normalized = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        if !seen.insert(candidate.owner.clone()) {
            return Err(());
        }
        let pubkeys = normalize_pubkeys(candidate.pubkeys.iter().copied())?;
        if pubkeys.is_empty() {
            return Err(());
        }
        normalized.push(EvictionCandidate {
            owner: candidate.owner.clone(),
            consumer: candidate.consumer,
            last_touch: candidate.last_touch,
            pubkeys,
        });
    }
    Ok(normalized)
}

fn normalize_pubkeys(pubkeys: impl IntoIterator<Item = Pubkey>) -> Result<Vec<Pubkey>, ()> {
    let mut keys: Vec<Pubkey> = pubkeys.into_iter().collect();
    keys.sort();
    keys.dedup();
    if keys.is_empty() {
        Err(())
    } else {
        Ok(keys)
    }
}

fn is_evictable_victim(candidate: &EvictionCandidate, incoming_consumer: ExplicitConsumer) -> bool {
    candidate.consumer != ExplicitConsumer::Wallet && candidate.consumer >= incoming_consumer
}

fn marginal_release(
    candidate: &EvictionCandidate,
    projected_refcounts: &HashMap<Pubkey, usize>,
    incoming_set: &BTreeSet<Pubkey>,
) -> usize {
    candidate
        .pubkeys
        .iter()
        .filter(|pk| {
            projected_refcounts.get(*pk).copied().unwrap_or(0) == 1 && !incoming_set.contains(pk)
        })
        .count()
}

fn max_freeable_in_tiers(
    candidates: &[EvictionCandidate],
    removed: &HashSet<usize>,
    projected_refcounts: &HashMap<Pubkey, usize>,
    incoming_set: &BTreeSet<Pubkey>,
    incoming_consumer: ExplicitConsumer,
    open_tiers: &[ExplicitConsumer],
) -> usize {
    let open: BTreeSet<ExplicitConsumer> = open_tiers.iter().copied().collect();
    if open.is_empty() {
        return 0;
    }

    let eligible: HashSet<usize> = candidates
        .iter()
        .enumerate()
        .filter_map(|(idx, candidate)| {
            if removed.contains(&idx) {
                return None;
            }
            if !open.contains(&candidate.consumer) {
                return None;
            }
            if !is_evictable_victim(candidate, incoming_consumer) {
                return None;
            }
            Some(idx)
        })
        .collect();

    projected_refcounts
        .iter()
        .filter(|(pubkey, _)| !incoming_set.contains(*pubkey))
        .filter(|(pubkey, _)| {
            let holders: Vec<usize> = candidates
                .iter()
                .enumerate()
                .filter(|(idx, candidate)| {
                    !removed.contains(idx) && candidate.pubkeys.binary_search(pubkey).is_ok()
                })
                .map(|(idx, _)| idx)
                .collect();
            !holders.is_empty() && holders.iter().all(|idx| eligible.contains(idx))
        })
        .count()
}

fn apply_victim_to_trial<S: PlannerStatsSink>(
    idx: usize,
    candidates: &[EvictionCandidate],
    removed: &mut HashSet<usize>,
    projected_refcounts: &mut HashMap<Pubkey, usize>,
    incoming_set: &BTreeSet<Pubkey>,
    stats: &mut S,
) -> usize {
    if !removed.insert(idx) {
        return 0;
    }
    let candidate = &candidates[idx];
    let mut newly_freed = 0usize;
    for pubkey in &candidate.pubkeys {
        if let Some(rc) = projected_refcounts.get_mut(pubkey) {
            stats.record_refcount_update();
            *rc = rc.saturating_sub(1);
            if *rc == 0 && !incoming_set.contains(pubkey) {
                newly_freed += 1;
                projected_refcounts.remove(pubkey);
            }
        }
    }
    newly_freed
}

fn select_positive_marginal<S: PlannerStatsSink>(
    candidates: &[EvictionCandidate],
    removed: &HashSet<usize>,
    projected_refcounts: &HashMap<Pubkey, usize>,
    incoming_set: &BTreeSet<Pubkey>,
    incoming_consumer: ExplicitConsumer,
    open_tiers: &BTreeSet<ExplicitConsumer>,
    stats: &mut S,
) -> Option<usize> {
    let mut best: Option<(Reverse<ExplicitConsumer>, u64, ExplicitOwnerKey, usize)> = None;
    for (idx, candidate) in candidates.iter().enumerate() {
        if removed.contains(&idx) {
            continue;
        }
        if !open_tiers.contains(&candidate.consumer) {
            continue;
        }
        if !is_evictable_victim(candidate, incoming_consumer) {
            continue;
        }
        stats.record_candidate_check();
        let marginal = marginal_release(candidate, projected_refcounts, incoming_set);
        if marginal == 0 {
            continue;
        }
        let key = (
            Reverse(candidate.consumer),
            candidate.last_touch,
            candidate.owner.owner_key.clone(),
            idx,
        );
        if best
            .as_ref()
            .is_none_or(|b| key < (b.0, b.1, b.2.clone(), b.3))
        {
            best = Some(key);
        }
    }
    best.map(|b| b.3)
}

#[derive(Clone, PartialEq, Eq)]
struct JointPackage {
    indices: Vec<usize>,
    consumer: ExplicitConsumer,
    oldest_touch: u64,
    owner_key: ExplicitOwnerKey,
}

fn select_joint_package<S: PlannerStatsSink>(
    candidates: &[EvictionCandidate],
    removed: &HashSet<usize>,
    projected_refcounts: &HashMap<Pubkey, usize>,
    incoming_set: &BTreeSet<Pubkey>,
    incoming_consumer: ExplicitConsumer,
    open_tiers: &BTreeSet<ExplicitConsumer>,
    stats: &mut S,
) -> Option<Vec<usize>> {
    let mut owner_indices: BTreeMap<usize, &EvictionCandidate> = BTreeMap::new();
    for (idx, candidate) in candidates.iter().enumerate() {
        if removed.contains(&idx) {
            continue;
        }
        if !open_tiers.contains(&candidate.consumer) {
            continue;
        }
        if !is_evictable_victim(candidate, incoming_consumer) {
            continue;
        }
        owner_indices.insert(idx, candidate);
    }

    let mut best: Option<JointPackage> = None;

    for (pubkey, refcount) in projected_refcounts {
        if incoming_set.contains(pubkey) || *refcount == 0 {
            continue;
        }
        let mut owners_for_key = Vec::new();
        for (idx, candidate) in &owner_indices {
            if candidate.pubkeys.binary_search(pubkey).is_ok() {
                owners_for_key.push(*idx);
            }
        }
        if owners_for_key.is_empty() {
            continue;
        }
        if owners_for_key.len() == 1
            && marginal_release(
                owner_indices[&owners_for_key[0]],
                projected_refcounts,
                incoming_set,
            ) > 0
        {
            continue;
        }

        stats.record_package_check();

        let mut package = owners_for_key;
        package.sort_unstable();
        let consumer = owner_indices[&package[0]].consumer;
        let oldest_touch = package
            .iter()
            .map(|idx| owner_indices[idx].last_touch)
            .min()
            .unwrap_or(0);
        let owner_key = owner_indices[&package[0]].owner.owner_key.clone();

        let candidate_pkg = JointPackage {
            indices: package,
            consumer,
            oldest_touch,
            owner_key,
        };

        if best
            .as_ref()
            .is_none_or(|b| joint_package_less(&candidate_pkg, b))
        {
            best = Some(candidate_pkg);
        }
    }

    best.map(|b| b.indices)
}

fn joint_package_less(a: &JointPackage, b: &JointPackage) -> bool {
    (
        a.indices.len(),
        Reverse(a.consumer),
        a.oldest_touch,
        &a.owner_key,
        &a.indices,
    ) < (
        b.indices.len(),
        Reverse(b.consumer),
        b.oldest_touch,
        &b.owner_key,
        &b.indices,
    )
}

struct PlanState<'a> {
    removed: &'a mut HashSet<usize>,
    projected_refcounts: &'a mut HashMap<Pubkey, usize>,
    incoming_set: &'a BTreeSet<Pubkey>,
    victims: &'a mut Vec<EvictionCandidate>,
    physical_freed: &'a mut BTreeSet<Pubkey>,
    remaining: &'a mut usize,
}

fn apply_victim<S: PlannerStatsSink>(
    idx: usize,
    candidates: &[EvictionCandidate],
    state: &mut PlanState<'_>,
    stats: &mut S,
) {
    let freed = apply_victim_to_trial(
        idx,
        candidates,
        state.removed,
        state.projected_refcounts,
        state.incoming_set,
        stats,
    );
    state.victims.push(candidates[idx].clone());
    for pubkey in &candidates[idx].pubkeys {
        if state.projected_refcounts.get(pubkey).is_none()
            && !state.incoming_set.contains(pubkey)
        {
            state.physical_freed.insert(*pubkey);
        }
    }
    *state.remaining = state.remaining.saturating_sub(freed);
}

#[cfg(test)]
mod tests {
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

    fn wallet_owner() -> ExplicitOwner {
        ExplicitOwner {
            consumer: ExplicitConsumer::Wallet,
            owner_key: ExplicitOwnerKey::Wallet,
        }
    }

    fn candidate(
        consumer: ExplicitConsumer,
        seed: u8,
        touch: u64,
        pubkeys: impl IntoIterator<Item = Pubkey>,
    ) -> EvictionCandidate {
        let owner = pool_owner(consumer, seed);
        EvictionCandidate {
            owner: owner.clone(),
            consumer,
            last_touch: touch,
            pubkeys: normalize_pubkeys(pubkeys).unwrap(),
        }
    }

    fn request(
        consumer: ExplicitConsumer,
        seed: u8,
        current_len: usize,
        cap: usize,
        pubkeys: impl IntoIterator<Item = Pubkey>,
    ) -> EvictionRequest {
        EvictionRequest {
            incoming_owner: pool_owner(consumer, seed),
            incoming_consumer: consumer,
            incoming_pubkeys: pubkeys.into_iter().collect(),
            current_physical_len: current_len,
            cap,
        }
    }

    fn assert_planned(result: EvictionPlanResult) -> EvictionPlan {
        match result {
            EvictionPlanResult::Planned(plan) => plan,
            other => panic!("expected Planned, got {other:?}"),
        }
    }

    // 1. No-eviction-needed.
    #[test]
    fn no_eviction_needed_when_under_cap() {
        let shared = pk(1);
        let candidates = vec![candidate(ExplicitConsumer::Tracker, 1, 10, [shared])];
        let req = request(ExplicitConsumer::Momentum, 9, 1, 2, [shared, pk(2)]);
        assert_eq!(
            plan_eviction(&candidates, &req),
            EvictionPlanResult::NoEvictionNeeded
        );
    }

    // 2. Tracker before Arb before Momentum; wallet never victim.
    #[test]
    fn priority_tracker_before_arb_before_momentum_wallet_never_victim() {
        let k = pk(10);
        let candidates = vec![
            candidate(ExplicitConsumer::Momentum, 1, 1, [k]),
            candidate(ExplicitConsumer::Arb, 2, 2, [pk(11)]),
            candidate(ExplicitConsumer::Tracker, 3, 3, [pk(12)]),
            EvictionCandidate {
                owner: wallet_owner(),
                consumer: ExplicitConsumer::Wallet,
                last_touch: 0,
                pubkeys: vec![pk(13)],
            },
        ];
        let req = request(ExplicitConsumer::Momentum, 9, 4, 2, [pk(10)]);
        let plan = assert_planned(plan_eviction(&candidates, &req));
        let victims: Vec<_> = plan.victims.iter().map(|v| v.consumer).collect();
        assert_eq!(
            victims,
            vec![ExplicitConsumer::Tracker, ExplicitConsumer::Arb]
        );
        assert!(plan
            .victims
            .iter()
            .all(|v| v.consumer != ExplicitConsumer::Wallet));
    }

    // 3. Tracker incoming cannot displace Arb.
    #[test]
    fn tracker_incoming_cannot_evict_arb() {
        let candidates = vec![
            candidate(ExplicitConsumer::Arb, 1, 1, [pk(1)]),
            candidate(ExplicitConsumer::Tracker, 2, 2, [pk(2)]),
        ];
        let req = request(ExplicitConsumer::Tracker, 9, 2, 1, [pk(3)]);
        assert_eq!(
            plan_eviction(&candidates, &req),
            EvictionPlanResult::RejectedProtected
        );
    }

    // 4. Same-tier true LRU and owner tie-break.
    #[test]
    fn same_tier_lru_and_owner_tie_break() {
        let shared = pk(1);
        let candidates = vec![
            candidate(ExplicitConsumer::Tracker, 2, 20, [shared]),
            candidate(ExplicitConsumer::Tracker, 1, 10, [pk(2)]),
        ];
        let req = request(ExplicitConsumer::Momentum, 9, 2, 2, [pk(3)]);
        let plan = assert_planned(plan_eviction(&candidates, &req));
        assert_eq!(plan.victims.len(), 1);
        assert_eq!(
            plan.victims[0].owner,
            pool_owner(ExplicitConsumer::Tracker, 1)
        );
    }

    // 5. Zero-marginal preserve.
    #[test]
    fn zero_marginal_preserve_only_evicts_positive_marginal_tracker() {
        let shared = pk(1);
        let candidates = vec![
            candidate(ExplicitConsumer::Tracker, 1, 10, [shared]),
            candidate(ExplicitConsumer::Tracker, 2, 20, [pk(2)]),
        ];
        let req = request(ExplicitConsumer::Momentum, 9, 2, 2, [shared, pk(3)]);
        let plan = assert_planned(plan_eviction(&candidates, &req));
        assert_eq!(plan.victims.len(), 1);
        assert_eq!(
            plan.victims[0].owner,
            pool_owner(ExplicitConsumer::Tracker, 2)
        );
        assert_eq!(plan.physical_freed, vec![pk(2)]);
        assert_eq!(plan.incoming_physical_added, vec![pk(3)]);
        assert_eq!(plan.projected_final_len, 2);
    }

    // 6. Joint removal.
    #[test]
    fn joint_removal_frees_shared_only_when_all_co_owners_removed() {
        let shared = pk(1);
        let candidates = vec![
            candidate(ExplicitConsumer::Tracker, 1, 10, [shared]),
            candidate(ExplicitConsumer::Tracker, 2, 20, [shared]),
        ];
        let req = request(ExplicitConsumer::Momentum, 9, 1, 2, [pk(2), pk(3)]);
        let plan = assert_planned(plan_eviction(&candidates, &req));
        assert_eq!(plan.victims.len(), 2);
        assert_eq!(plan.physical_freed, vec![shared]);
        assert_eq!(plan.incoming_physical_added, vec![pk(2), pk(3)]);
        assert_eq!(plan.projected_final_len, 2);
    }

    // 7. Incoming needs shared key — never counts as freed.
    #[test]
    fn incoming_shared_key_never_counts_as_physical_freed() {
        let shared = pk(1);
        let candidates = vec![
            candidate(ExplicitConsumer::Tracker, 1, 10, [shared, pk(2)]),
            candidate(ExplicitConsumer::Arb, 2, 20, [shared]),
        ];
        let req = request(ExplicitConsumer::Momentum, 9, 2, 2, [shared, pk(3)]);
        let plan = assert_planned(plan_eviction(&candidates, &req));
        assert!(!plan.physical_freed.contains(&shared));
        assert_eq!(plan.physical_freed, vec![pk(2)]);
    }

    // 8. Mixed shared/exclusive exact deltas.
    #[test]
    fn mixed_shared_exclusive_exact_deltas() {
        let shared = pk(1);
        let exclusive = pk(2);
        let candidates = vec![
            candidate(ExplicitConsumer::Tracker, 1, 10, [shared, exclusive]),
            candidate(ExplicitConsumer::Arb, 2, 20, [shared]),
        ];
        let req = request(ExplicitConsumer::Momentum, 9, 2, 2, [shared, pk(3)]);
        let plan = assert_planned(plan_eviction(&candidates, &req));
        assert_eq!(plan.incoming_physical_added, vec![pk(3)]);
        assert_eq!(plan.physical_freed, vec![exclusive]);
        assert_eq!(plan.projected_final_len, 2);
    }

    // 9. Lower tiers insufficient opens next tier.
    #[test]
    fn opens_higher_tier_when_lower_tiers_insufficient() {
        let candidates = vec![
            candidate(ExplicitConsumer::Momentum, 1, 10, [pk(1)]),
            candidate(ExplicitConsumer::Tracker, 2, 20, [pk(2)]),
        ];
        let req = request(ExplicitConsumer::Momentum, 9, 2, 1, [pk(3)]);
        let plan = assert_planned(plan_eviction(&candidates, &req));
        let consumers: Vec<_> = plan.victims.iter().map(|v| v.consumer).collect();
        assert!(consumers.contains(&ExplicitConsumer::Momentum));
    }

    // 10. Protected overload → RejectedProtected.
    #[test]
    fn protected_overload_rejected_without_partial_plan() {
        let candidates = vec![candidate(ExplicitConsumer::Momentum, 1, 10, [pk(1)])];
        let req = request(ExplicitConsumer::Tracker, 9, 1, 0, [pk(2)]);
        assert_eq!(
            plan_eviction(&candidates, &req),
            EvictionPlanResult::RejectedProtected
        );
    }

    #[test]
    fn wallet_overload_rejected_protected() {
        let candidates = vec![EvictionCandidate {
            owner: wallet_owner(),
            consumer: ExplicitConsumer::Wallet,
            last_touch: 0,
            pubkeys: vec![pk(1)],
        }];
        let req = request(ExplicitConsumer::Momentum, 9, 1, 0, [pk(2), pk(3)]);
        assert_eq!(
            plan_eviction(&candidates, &req),
            EvictionPlanResult::RejectedProtected
        );
    }

    // 11. Determinism under permuted input order.
    #[test]
    fn determinism_under_permuted_candidate_order() {
        let c1 = candidate(ExplicitConsumer::Tracker, 1, 10, [pk(1)]);
        let c2 = candidate(ExplicitConsumer::Arb, 2, 20, [pk(2)]);
        let c3 = candidate(ExplicitConsumer::Tracker, 3, 5, [pk(3)]);
        let req = request(ExplicitConsumer::Momentum, 9, 3, 1, [pk(4), pk(5)]);

        let r1 = plan_eviction(&[c1.clone(), c2.clone(), c3.clone()], &req);
        let r2 = plan_eviction(&[c3.clone(), c1.clone(), c2.clone()], &req);
        let r3 = plan_eviction(&[c2.clone(), c3.clone(), c1.clone()], &req);
        assert_eq!(r1, r2);
        assert_eq!(r2, r3);
    }

    // 12. Checked arithmetic at usize::MAX.
    #[test]
    fn usize_max_checked_arithmetic() {
        let candidates = vec![candidate(ExplicitConsumer::Tracker, 1, 1, [pk(1)])];
        let req = EvictionRequest {
            incoming_owner: pool_owner(ExplicitConsumer::Momentum, 9),
            incoming_consumer: ExplicitConsumer::Momentum,
            incoming_pubkeys: vec![pk(2)],
            current_physical_len: usize::MAX,
            cap: 0,
        };
        assert_eq!(
            plan_eviction(&candidates, &req),
            EvictionPlanResult::InternalInvariantViolation
        );

        let req2 = EvictionRequest {
            incoming_owner: pool_owner(ExplicitConsumer::Momentum, 9),
            incoming_consumer: ExplicitConsumer::Momentum,
            incoming_pubkeys: vec![pk(2)],
            current_physical_len: usize::MAX - 1,
            cap: usize::MAX - 1,
        };
        assert!(matches!(
            plan_eviction(&candidates, &req2),
            EvictionPlanResult::Planned(_)
        ));
    }

    // 13. Bounded stress + PlannerStats.
    #[test]
    fn bounded_stress_with_planner_stats() {
        const BACKGROUND: usize = 300;
        let mut candidates = Vec::new();
        for seed in 0..BACKGROUND {
            candidates.push(EvictionCandidate {
                owner: ExplicitOwner {
                    consumer: ExplicitConsumer::Tracker,
                    owner_key: ExplicitOwnerKey::Generic(seed as u64),
                },
                consumer: ExplicitConsumer::Tracker,
                last_touch: seed as u64,
                pubkeys: normalize_pubkeys([pk((seed % 245 + 10) as u8)]).unwrap(),
            });
        }
        let shared = pk(250);
        candidates.push(candidate(ExplicitConsumer::Tracker, 250, 1, [shared]));
        candidates.push(candidate(ExplicitConsumer::Tracker, 251, 2, [shared]));

        let req = request(
            ExplicitConsumer::Momentum,
            9,
            BACKGROUND + 1,
            BACKGROUND,
            [pk(99), pk(100)],
        );
        let mut stats = PlannerStats::default();
        let result = plan_eviction_with_stats(&candidates, &req, Some(&mut stats));

        assert!(matches!(result, EvictionPlanResult::Planned(_)));
        assert!(stats.initial_group_key_edges >= BACKGROUND as u64);
        assert!(stats.incremental_refcount_updates > 0);
        assert!(stats.candidate_checks > 0);
        // max_freeable_in_trial clones twice per tier-open check; production path has zero clones.
        assert_eq!(stats.full_graph_clones, 0);
    }

    // Invalid inputs.
    #[test]
    fn invalid_empty_incoming_and_duplicate_owners() {
        let candidates = vec![candidate(ExplicitConsumer::Tracker, 1, 1, [pk(1)])];
        let req = request(ExplicitConsumer::Momentum, 9, 1, 2, []);
        assert_eq!(
            plan_eviction(&candidates, &req),
            EvictionPlanResult::RejectedInvalidInput
        );

        let dup = vec![
            candidate(ExplicitConsumer::Tracker, 1, 1, [pk(1)]),
            candidate(ExplicitConsumer::Tracker, 1, 2, [pk(2)]),
        ];
        let req2 = request(ExplicitConsumer::Momentum, 9, 1, 1, [pk(3)]);
        assert_eq!(
            plan_eviction(&dup, &req2),
            EvictionPlanResult::RejectedInvalidInput
        );
    }

    /// Independent brute-force oracle — never calls production planner for expected deltas.
    #[derive(Debug, Clone)]
    struct OracleModel {
        candidates: Vec<EvictionCandidate>,
        incoming: BTreeSet<Pubkey>,
        incoming_consumer: ExplicitConsumer,
        current_len: usize,
        cap: usize,
    }

    impl OracleModel {
        fn from_inputs(
            candidates: &[EvictionCandidate],
            request: &EvictionRequest,
        ) -> Option<Self> {
            let incoming = normalize_pubkeys(request.incoming_pubkeys.iter().copied()).ok()?;
            let normalized = normalize_candidates(candidates).ok()?;
            if normalized.iter().any(|c| c.owner == request.incoming_owner) {
                return None;
            }
            Some(Self {
                candidates: normalized,
                incoming: incoming.into_iter().collect(),
                incoming_consumer: request.incoming_consumer,
                current_len: request.current_physical_len,
                cap: request.cap,
            })
        }

        fn incoming_added(&self) -> Vec<Pubkey> {
            let mut refcounts: BTreeMap<Pubkey, usize> = BTreeMap::new();
            for c in &self.candidates {
                for pk in &c.pubkeys {
                    *refcounts.entry(*pk).or_default() += 1;
                }
            }
            self.incoming
                .iter()
                .filter(|pk| refcounts.get(pk).copied().unwrap_or(0) == 0)
                .copied()
                .collect()
        }

        fn required_free(&self) -> Option<usize> {
            let added = self.incoming_added().len();
            let after = self.current_len.checked_add(added)?;
            if after <= self.cap {
                return Some(0);
            }
            after.checked_sub(self.cap)
        }

        fn verify_plan(&self, plan: &EvictionPlan) -> bool {
            let victim_set: BTreeSet<_> = plan.victims.iter().map(|v| v.owner.clone()).collect();
            if victim_set.len() != plan.victims.len() {
                return false;
            }

            for v in &plan.victims {
                if v.consumer == ExplicitConsumer::Wallet {
                    return false;
                }
                if v.consumer < self.incoming_consumer {
                    return false;
                }
            }

            let mut refcounts: BTreeMap<Pubkey, usize> = BTreeMap::new();
            for c in &self.candidates {
                if victim_set.contains(&c.owner) {
                    continue;
                }
                for pk in &c.pubkeys {
                    *refcounts.entry(*pk).or_default() += 1;
                }
            }
            for pk in &self.incoming {
                if refcounts.get(pk).copied().unwrap_or(0) == 0 {
                    refcounts.insert(*pk, 1);
                } else {
                    *refcounts.entry(*pk).or_default() += 1;
                }
            }

            let final_len = refcounts.values().filter(|&&rc| rc > 0).count();
            if final_len != plan.projected_final_len || final_len > self.cap {
                return false;
            }

            let mut before: BTreeMap<Pubkey, usize> = BTreeMap::new();
            for c in &self.candidates {
                for pk in &c.pubkeys {
                    *before.entry(*pk).or_default() += 1;
                }
            }
            let freed_expected: BTreeSet<Pubkey> = before
                .keys()
                .filter(|pk| {
                    before.get(*pk).copied().unwrap_or(0) > 0
                        && refcounts.get(*pk).copied().unwrap_or(0) == 0
                        && !self.incoming.contains(*pk)
                })
                .copied()
                .collect();
            if freed_expected.len() != plan.physical_freed.len() {
                return false;
            }
            if freed_expected
                .iter()
                .zip(plan.physical_freed.iter())
                .any(|(a, b)| a != b)
            {
                return false;
            }

            let added = self.incoming_added();
            if added != plan.incoming_physical_added {
                return false;
            }

            self.verify_no_unnecessary_zero_marginal(plan) && self.verify_tier_minimality(plan)
        }

        fn verify_no_unnecessary_zero_marginal(&self, plan: &EvictionPlan) -> bool {
            let victim_owners: BTreeSet<_> = plan.victims.iter().map(|v| v.owner.clone()).collect();
            let mut refcounts: BTreeMap<Pubkey, usize> = BTreeMap::new();
            for c in &self.candidates {
                for pk in &c.pubkeys {
                    *refcounts.entry(*pk).or_default() += 1;
                }
            }
            for v in &plan.victims {
                let marginal = v
                    .pubkeys
                    .iter()
                    .filter(|pk| {
                        refcounts.get(*pk).copied().unwrap_or(0) == 1
                            && !self.incoming.contains(*pk)
                    })
                    .count();
                if marginal == 0 {
                    let without: BTreeSet<_> = victim_owners
                        .iter()
                        .filter(|o| *o != &v.owner)
                        .cloned()
                        .collect();
                    if without.len() == victim_owners.len() {
                        continue;
                    }
                    let mut trial = refcounts.clone();
                    for c in &self.candidates {
                        if without.contains(&c.owner) {
                            continue;
                        }
                        for pk in &c.pubkeys {
                            if let Some(rc) = trial.get_mut(pk) {
                                *rc = rc.saturating_sub(1);
                            }
                        }
                    }
                    let freed_without: usize = trial
                        .iter()
                        .filter(|(pk, rc)| **rc == 0 && !self.incoming.contains(*pk))
                        .count();
                    let required = self.required_free().unwrap_or(0);
                    if freed_without >= required {
                        return false;
                    }
                }
                for pk in &v.pubkeys {
                    if let Some(rc) = refcounts.get_mut(pk) {
                        *rc = rc.saturating_sub(1);
                    }
                }
            }
            true
        }

        fn verify_tier_minimality(&self, plan: &EvictionPlan) -> bool {
            let victim_set: BTreeSet<_> = plan.victims.iter().map(|v| v.owner.clone()).collect();
            let tiers = [
                ExplicitConsumer::Tracker,
                ExplicitConsumer::Arb,
                ExplicitConsumer::Momentum,
            ];
            let mut used_tier = ExplicitConsumer::Wallet;
            for v in &plan.victims {
                if v.consumer > used_tier {
                    used_tier = v.consumer;
                }
            }
            for tier in tiers {
                if tier > used_tier {
                    break;
                }
                let open: BTreeSet<_> = tiers.iter().filter(|t| **t <= tier).copied().collect();
                let max = self.max_freeable_subset(&victim_set, &open);
                let required = self.required_free().unwrap_or(0);
                if max >= required && tier == used_tier {
                    // ok
                } else if max >= required && tier < used_tier {
                    return false;
                }
            }
            true
        }

        fn max_freeable_subset(
            &self,
            already_removed: &BTreeSet<ExplicitOwner>,
            open_tiers: &BTreeSet<ExplicitConsumer>,
        ) -> usize {
            let mut refcounts: BTreeMap<Pubkey, usize> = BTreeMap::new();
            for c in &self.candidates {
                if already_removed.contains(&c.owner) {
                    continue;
                }
                if !open_tiers.contains(&c.consumer) {
                    continue;
                }
                if !is_evictable_victim(c, self.incoming_consumer) {
                    continue;
                }
                for pk in &c.pubkeys {
                    *refcounts.entry(*pk).or_default() += 1;
                }
            }
            let before_len = refcounts.len();
            let mut removed = already_removed.clone();
            for c in &self.candidates {
                if removed.contains(&c.owner) {
                    continue;
                }
                if !open_tiers.contains(&c.consumer) {
                    continue;
                }
                if !is_evictable_victim(c, self.incoming_consumer) {
                    continue;
                }
                removed.insert(c.owner.clone());
                for pk in &c.pubkeys {
                    if let Some(rc) = refcounts.get_mut(pk) {
                        *rc = rc.saturating_sub(1);
                        if *rc == 0 {
                            refcounts.remove(pk);
                        }
                    }
                }
            }
            before_len.saturating_sub(refcounts.len())
        }
    }

    // 14. Oracle verification on deterministic seeds.
    #[test]
    fn oracle_verifies_planner_on_deterministic_seeds() {
        let seeds: &[(u8, usize, usize)] =
            &[(1, 2, 1), (7, 3, 2), (13, 4, 2), (42, 5, 3), (99, 6, 4)];

        for &(seed, group_count, cap) in seeds {
            let mut candidates = Vec::new();
            for i in 0..group_count {
                let consumer = match i % 3 {
                    0 => ExplicitConsumer::Tracker,
                    1 => ExplicitConsumer::Arb,
                    _ => ExplicitConsumer::Momentum,
                };
                candidates.push(candidate(
                    consumer,
                    seed.wrapping_add(i as u8),
                    (seed as u64) + i as u64,
                    [pk(seed.wrapping_add(i as u8))],
                ));
            }
            let current_len = group_count;
            let req = request(
                ExplicitConsumer::Momentum,
                seed.wrapping_add(200),
                current_len,
                cap,
                [pk(seed.wrapping_add(100)), pk(seed.wrapping_add(101))],
            );

            let result = plan_eviction(&candidates, &req);
            let Some(model) = OracleModel::from_inputs(&candidates, &req) else {
                assert_eq!(result, EvictionPlanResult::RejectedInvalidInput);
                continue;
            };

            match result {
                EvictionPlanResult::NoEvictionNeeded => {
                    assert_eq!(model.required_free(), Some(0));
                }
                EvictionPlanResult::Planned(plan) => {
                    assert!(model.verify_plan(&plan), "oracle failed for seed {seed}");
                }
                EvictionPlanResult::RejectedProtected => {
                    let required = model.required_free().unwrap_or(0);
                    if required == 0 {
                        panic!("unexpected RejectedProtected with no required free");
                    }
                }
                EvictionPlanResult::RejectedInvalidInput
                | EvictionPlanResult::InternalInvariantViolation => {}
            }
        }
    }
}
