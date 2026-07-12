//! Hard admission SSOT for explicit Geyser subscription pubkeys (I-MD-7 / I-MD-8).

use crate::metrics::inc_market_data_geyser_explicit_evicted_total;
use solana_sdk::pubkey::Pubkey;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};

/// Consumer tag for explicit Geyser pubkey ownership.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConsumerId {
    Wallet,
    Momentum,
    Arb,
    Tracker,
}

/// Pin priority for cap eviction — lower ordinal = higher protection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PinPriority {
    Wallet = 0,
    Momentum = 1,
    Arb = 2,
    Tracker = 3,
}

/// Owner reference for shared-pubkey refcounting (pool / wallet / standalone mint).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum OwnerKey {
    Wallet,
    Pool(Pubkey),
    Mint(Pubkey),
}

/// Result of attempting to admit an owner group atomically.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionResult {
    Admitted {
        new_pubkeys: usize,
    },
    OwnerAddedNoNewPubkey,
    RejectedCap,
    RejectedProtected,
    RejectedInvalidGroup,
    /// Plan validated but post-commit invariant failed (fail-closed; no metrics as RejectedCap).
    RejectedInternal,
}

/// Result of cap shrink / restore convergence (release-enforced, never debug_assert).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapConvergeResult {
    Converged,
    ProtectedOverflow,
    Unconverged,
}

/// Reason recorded when a group is evicted from the desired set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictReason {
    HigherPriority,
    SamePriorityLru,
    CapShrink,
    RestoreConverge,
}

/// Serializable owner group for snapshot v2 restore.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerGroupSnapshot {
    pub consumer: ConsumerId,
    pub owner: OwnerKey,
    pub pubkeys: HashSet<Pubkey>,
    pub last_touched_gen: u64,
}

#[derive(Debug, Clone)]
struct PubkeyEntry {
    owners: HashSet<(ConsumerId, OwnerKey)>,
}

#[derive(Debug, Clone)]
struct OwnerGroup {
    consumer: ConsumerId,
    owner: OwnerKey,
    pubkeys: HashSet<Pubkey>,
    admitted_seq: u64,
    last_touched_gen: u64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct EvictCandidate {
    priority: PinPriority,
    touch_gen: u64,
    admitted_seq: u64,
    owner: OwnerKey,
    consumer: ConsumerId,
    removable: usize,
    stamp: u64,
}

impl Ord for EvictCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.priority
            .cmp(&other.priority)
            .then_with(|| other.touch_gen.cmp(&self.touch_gen))
            .then_with(|| other.admitted_seq.cmp(&self.admitted_seq))
            .then_with(|| self.owner.cmp(&other.owner))
    }
}

impl PartialOrd for EvictCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone)]
struct AdmissionPlan {
    evictions: Vec<(ConsumerId, OwnerKey, EvictReason)>,
    incoming: Option<(ConsumerId, OwnerKey, HashSet<Pubkey>)>,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct PlanningStats {
    pub candidate_pops: usize,
    pub stale_pops: usize,
    pub edge_updates: usize,
    pub refcount_checks: usize,
    pub projection_copies: usize,
    pub entries_copied: usize,
    pub owner_edge_iterations: usize,
    pub victim_owner_edge_scans: usize,
    pub victim_removals: usize,
}

/// Backward-compatible alias for eviction planner instrumentation.
pub type EvictionPlannerStats = PlanningStats;

/// Incremental admission projection overlay — touches only incoming/victim pubkey edges.
#[derive(Debug)]
struct PlanningOverlay {
    touched_pubkeys: HashSet<Pubkey>,
    suppressed_groups: HashSet<(ConsumerId, OwnerKey)>,
    incoming: Option<((ConsumerId, OwnerKey), HashSet<Pubkey>)>,
    projected_len: usize,
    projected_pubkey_refcount: HashMap<Pubkey, usize>,
}

fn pk_projection_contribution(live: bool, proj: bool) -> i64 {
    match (live, proj) {
        (false, true) => 1,
        (true, false) => -1,
        _ => 0,
    }
}

fn projected_len_delta(live: bool, old_proj: bool, new_proj: bool) -> i64 {
    pk_projection_contribution(live, new_proj) - pk_projection_contribution(live, old_proj)
}

fn apply_projected_len_delta(projected_len: usize, delta: i64) -> usize {
    (projected_len as i64 + delta).max(0) as usize
}

fn initial_projected_refcount(
    set: &DesiredExplicitSet,
    pk: Pubkey,
    suppressed: &HashSet<(ConsumerId, OwnerKey)>,
    incoming: Option<((ConsumerId, OwnerKey), &HashSet<Pubkey>)>,
    mut stats: Option<&mut PlanningStats>,
) -> usize {
    let mut count = 0usize;
    if let Some(entry) = set.entries.get(&pk) {
        for owner in &entry.owners {
            if let Some(s) = stats.as_mut() {
                s.owner_edge_iterations = s.owner_edge_iterations.saturating_add(1);
            }
            if !suppressed.contains(owner) {
                count = count.saturating_add(1);
            }
        }
    }
    if let Some((key, pubs)) = incoming {
        if pubs.contains(&pk) {
            if let Some(s) = stats.as_mut() {
                s.owner_edge_iterations = s.owner_edge_iterations.saturating_add(1);
            }
            let _ = key;
            count = count.saturating_add(1);
        }
    }
    count
}

impl PlanningOverlay {
    fn for_cap_shrink(set: &DesiredExplicitSet, mut stats: Option<&mut PlanningStats>) -> Self {
        let touched_pubkeys: HashSet<Pubkey> = set.entries.keys().copied().collect();
        let mut projected_pubkey_refcount = HashMap::with_capacity(touched_pubkeys.len());
        for pk in &touched_pubkeys {
            let count = set.entries.get(pk).map(|e| e.owners.len()).unwrap_or(0);
            if let Some(s) = stats.as_mut() {
                s.owner_edge_iterations = s.owner_edge_iterations.saturating_add(count);
            }
            projected_pubkey_refcount.insert(*pk, count);
        }
        Self {
            touched_pubkeys,
            suppressed_groups: HashSet::new(),
            incoming: None,
            projected_len: set.entries.len(),
            projected_pubkey_refcount,
        }
    }

    fn for_incoming_admission(
        set: &DesiredExplicitSet,
        consumer: ConsumerId,
        owner: OwnerKey,
        incoming: &HashSet<Pubkey>,
        mut stats: Option<&mut PlanningStats>,
    ) -> Self {
        let key = (consumer, owner);
        let mut touched = incoming.clone();
        let mut suppressed = HashSet::new();
        suppressed.insert(key);
        if let Some(existing) = set.groups.get(&key) {
            touched.extend(&existing.pubkeys);
        }
        let incoming_ref = Some((key, incoming));
        let mut projected_len = set.entries.len();
        let mut projected_pubkey_refcount = HashMap::with_capacity(touched.len());
        for pk in &touched {
            let live = set.entries.contains_key(pk);
            let count = initial_projected_refcount(
                set,
                *pk,
                &suppressed,
                incoming_ref,
                stats.as_deref_mut(),
            );
            projected_pubkey_refcount.insert(*pk, count);
            let proj_exists = count > 0;
            projected_len = apply_projected_len_delta(
                projected_len,
                projected_len_delta(live, live, proj_exists),
            );
        }
        Self {
            touched_pubkeys: touched,
            suppressed_groups: suppressed,
            incoming: Some((key, incoming.clone())),
            projected_len,
            projected_pubkey_refcount,
        }
    }

    fn add_victim(
        &mut self,
        set: &DesiredExplicitSet,
        victim: (ConsumerId, OwnerKey),
        mut stats: Option<&mut PlanningStats>,
    ) {
        if self.suppressed_groups.contains(&victim) {
            return;
        }
        let pubkeys: Vec<Pubkey> = set
            .groups
            .get(&victim)
            .map(|group| group.pubkeys.iter().copied().collect())
            .unwrap_or_default();
        for pk in &pubkeys {
            self.touched_pubkeys.insert(*pk);
        }
        let incoming_ref = self.incoming.as_ref().map(|(k, p)| (*k, p));
        let before_counts: Vec<_> = pubkeys
            .iter()
            .map(|pk| {
                (
                    *pk,
                    self.projected_pubkey_refcount
                        .get(pk)
                        .copied()
                        .unwrap_or_else(|| {
                            initial_projected_refcount(
                                set,
                                *pk,
                                &self.suppressed_groups,
                                incoming_ref,
                                None,
                            )
                        }),
                )
            })
            .collect();
        if !self.suppressed_groups.insert(victim) {
            return;
        }
        if let Some(stats) = stats.as_mut() {
            stats.victim_removals = stats.victim_removals.saturating_add(1);
        }
        for (pk, before) in before_counts {
            let after = before.saturating_sub(1);
            self.projected_pubkey_refcount.insert(pk, after);
            let live = set.entries.contains_key(&pk);
            let delta = projected_len_delta(live, before > 0, after > 0);
            self.projected_len = apply_projected_len_delta(self.projected_len, delta);
        }
    }
}

fn incoming_net_entry_delta(
    set: &DesiredExplicitSet,
    consumer: ConsumerId,
    owner: OwnerKey,
    incoming: &HashSet<Pubkey>,
    mut stats: Option<&mut PlanningStats>,
) -> i64 {
    let key = (consumer, owner);
    let mut delta = 0i64;
    if let Some(existing) = set.groups.get(&key) {
        for pk in &existing.pubkeys {
            if !incoming.contains(pk)
                && set
                    .entries
                    .get(pk)
                    .is_some_and(|e| e.owners.len() == 1 && e.owners.contains(&key))
            {
                delta -= 1;
                if let Some(stats) = stats.as_deref_mut() {
                    stats.owner_edge_iterations = stats.owner_edge_iterations.saturating_add(1);
                }
            }
        }
    }
    for pk in incoming {
        if !set.entries.contains_key(pk) {
            delta += 1;
            if let Some(stats) = stats.as_deref_mut() {
                stats.owner_edge_iterations = stats.owner_edge_iterations.saturating_add(1);
            }
        }
    }
    delta
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct PolicyEvictEntry {
    key: (ConsumerId, OwnerKey),
    priority: PinPriority,
    touch_gen: u64,
    admitted_seq: u64,
    owner: OwnerKey,
}

impl Ord for PolicyEvictEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.priority
            .cmp(&other.priority)
            .then_with(|| self.touch_gen.cmp(&other.touch_gen).reverse())
            .then_with(|| self.admitted_seq.cmp(&other.admitted_seq).reverse())
            .then_with(|| self.owner.cmp(&other.owner).reverse())
    }
}

impl PartialOrd for PolicyEvictEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone)]
pub struct DesiredExplicitSet {
    entries: HashMap<Pubkey, PubkeyEntry>,
    groups: HashMap<(ConsumerId, OwnerKey), OwnerGroup>,
    by_consumer: HashMap<ConsumerId, HashSet<Pubkey>>,
    max_explicit_pubkeys: usize,
    next_seq: u64,
    next_touch_gen: u64,
    evict_heap: BinaryHeap<EvictCandidate>,
    heap_stamp: HashMap<(ConsumerId, OwnerKey), u64>,
    global_heap_stamp: u64,
}

impl Default for DesiredExplicitSet {
    fn default() -> Self {
        Self::new(25_000)
    }
}

impl DesiredExplicitSet {
    pub fn new(max_explicit_pubkeys: usize) -> Self {
        Self {
            entries: HashMap::new(),
            groups: HashMap::new(),
            by_consumer: HashMap::new(),
            max_explicit_pubkeys: max_explicit_pubkeys.max(1),
            next_seq: 0,
            next_touch_gen: 0,
            evict_heap: BinaryHeap::new(),
            heap_stamp: HashMap::new(),
            global_heap_stamp: 0,
        }
    }

    pub fn max_explicit_pubkeys(&self) -> usize {
        self.max_explicit_pubkeys
    }

    pub fn set_max_explicit_pubkeys(&mut self, cap: usize) -> CapConvergeResult {
        self.max_explicit_pubkeys = cap.max(1);
        if self.wallet_group_exceeds_cap() {
            return CapConvergeResult::ProtectedOverflow;
        }
        self.evict_until_within_cap(EvictReason::CapShrink)
    }

    fn wallet_group_exceeds_cap(&self) -> bool {
        self.groups
            .get(&(ConsumerId::Wallet, OwnerKey::Wallet))
            .is_some_and(|g| g.pubkeys.len() > self.max_explicit_pubkeys)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn contains(&self, pubkey: &Pubkey) -> bool {
        self.entries.contains_key(pubkey)
    }

    pub fn consumers_of(&self, pubkey: &Pubkey) -> Option<Vec<ConsumerId>> {
        self.entries.get(pubkey).map(|e| {
            e.owners
                .iter()
                .map(|(c, _)| *c)
                .collect::<HashSet<_>>()
                .into_iter()
                .collect()
        })
    }

    pub fn snapshot_pubkeys(&self) -> HashSet<Pubkey> {
        self.entries.keys().copied().collect()
    }

    pub fn snapshot_owner_groups(&self) -> Vec<OwnerGroupSnapshot> {
        self.groups
            .values()
            .map(|g| OwnerGroupSnapshot {
                consumer: g.consumer,
                owner: g.owner,
                pubkeys: g.pubkeys.clone(),
                last_touched_gen: g.last_touched_gen,
            })
            .collect()
    }

    pub fn admitted_pool_count(&self, consumer: ConsumerId) -> usize {
        self.groups
            .keys()
            .filter(|(c, owner)| *c == consumer && matches!(owner, OwnerKey::Pool(_)))
            .count()
    }

    pub fn cap_overflow(&self) -> usize {
        self.entries.len().saturating_sub(self.max_explicit_pubkeys)
    }

    pub fn wallet_demand_count(&self, demand: &HashSet<Pubkey>) -> usize {
        demand.len()
    }

    pub fn wallet_demand_exceeds_cap(&self, demand: &HashSet<Pubkey>) -> bool {
        self.projected_wallet_protected_pubkeys(demand).len() > self.max_explicit_pubkeys
    }

    /// Union of incoming wallet demand and all Wallet-consumer pubkeys already admitted (wallet group + wallet-pinned mints).
    pub fn projected_wallet_protected_pubkeys(&self, demand: &HashSet<Pubkey>) -> HashSet<Pubkey> {
        let mut out = demand.clone();
        if let Some(existing) = self.by_consumer.get(&ConsumerId::Wallet) {
            out.extend(existing.iter().copied());
        }
        out
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.groups.clear();
        self.by_consumer.clear();
        self.next_seq = 0;
        self.next_touch_gen = 0;
        self.evict_heap.clear();
        self.heap_stamp.clear();
        self.global_heap_stamp = 0;
    }

    /// Legitimate demand refresh — updates LRU touch generation for one owner group.
    pub fn touch_group(&mut self, consumer: ConsumerId, owner: OwnerKey) {
        if self.groups.contains_key(&(consumer, owner)) {
            self.next_touch_gen = self.next_touch_gen.saturating_add(1);
            if let Some(g) = self.groups.get_mut(&(consumer, owner)) {
                g.last_touched_gen = self.next_touch_gen;
            }
            self.push_evict_candidate(consumer, owner);
        }
    }

    pub fn try_admit_group(
        &mut self,
        consumer: ConsumerId,
        owner: OwnerKey,
        pubkeys: HashSet<Pubkey>,
    ) -> AdmissionResult {
        if pubkeys.is_empty() {
            return AdmissionResult::RejectedInvalidGroup;
        }
        if let Some(existing) = self.groups.get(&(consumer, owner)) {
            if existing.pubkeys == pubkeys {
                self.touch_group(consumer, owner);
                return AdmissionResult::OwnerAddedNoNewPubkey;
            }
        }
        match self.plan_admit_group(consumer, owner, pubkeys) {
            Ok(plan) => self.apply_plan(plan),
            Err(result) => result,
        }
    }

    /// Admit wallet demand as a single atomic owner group; fails closed when demand alone exceeds cap.
    pub fn try_admit_wallet_demand(&mut self, demand: HashSet<Pubkey>) -> AdmissionResult {
        if demand.is_empty() {
            return AdmissionResult::RejectedInvalidGroup;
        }
        if self.wallet_demand_exceeds_cap(&demand) {
            return AdmissionResult::RejectedCap;
        }
        self.try_admit_group(ConsumerId::Wallet, OwnerKey::Wallet, demand)
    }

    /// Remove one owner group; pubkeys remain while other owners still reference them.
    pub fn remove_group(&mut self, consumer: ConsumerId, owner: OwnerKey) -> Vec<Pubkey> {
        let Some(group) = self.groups.remove(&(consumer, owner)) else {
            return Vec::new();
        };
        self.heap_stamp.remove(&(consumer, owner));
        let mut removed_entirely = Vec::new();
        for pk in group.pubkeys {
            if let Some(entry) = self.entries.get_mut(&pk) {
                entry.owners.remove(&(consumer, owner));
                if entry.owners.is_empty() {
                    self.entries.remove(&pk);
                    removed_entirely.push(pk);
                    self.remove_pubkey_from_consumer_index(consumer, pk);
                }
            }
        }
        removed_entirely
    }

    /// Restore complete owner groups without clearing resident state (backward-compatible convergence).
    pub fn restore_owner_groups(&mut self, groups: &[OwnerGroupSnapshot]) {
        let incoming_keys: HashSet<(ConsumerId, OwnerKey)> =
            groups.iter().map(|g| (g.consumer, g.owner)).collect();
        for key in self
            .groups
            .keys()
            .copied()
            .collect::<Vec<(ConsumerId, OwnerKey)>>()
        {
            if !incoming_keys.contains(&key) {
                self.remove_group(key.0, key.1);
            }
        }
        let mut ordered: Vec<&OwnerGroupSnapshot> = groups.iter().collect();
        ordered.sort_by(|a, b| {
            pin_priority_from_consumer(a.consumer)
                .cmp(&pin_priority_from_consumer(b.consumer))
                .then_with(|| a.owner.cmp(&b.owner))
        });
        for g in ordered {
            let _ = self.try_admit_group_with_touch(
                g.consumer,
                g.owner,
                g.pubkeys.clone(),
                g.last_touched_gen,
            );
        }
        let _ = self.evict_until_within_cap(EvictReason::RestoreConverge);
    }

    /// Reconcile authoritative owner groups in priority order without flatten/clear.
    pub fn reconcile_owner_groups(&mut self, groups: Vec<(ConsumerId, OwnerKey, HashSet<Pubkey>)>) {
        self.reconcile_owner_groups_with_preserve(groups, Vec::new());
    }

    /// Reconcile authoritative groups; preserve restored groups until an authoritative replacement exists.
    pub fn reconcile_owner_groups_with_preserve(
        &mut self,
        authoritative: Vec<(ConsumerId, OwnerKey, HashSet<Pubkey>)>,
        preserve: Vec<(ConsumerId, OwnerKey, HashSet<Pubkey>)>,
    ) {
        let auth_keys: HashSet<(ConsumerId, OwnerKey)> =
            authoritative.iter().map(|(c, o, _)| (*c, *o)).collect();
        let preserve_keys: HashSet<(ConsumerId, OwnerKey)> =
            preserve.iter().map(|(c, o, _)| (*c, *o)).collect();
        let keep_keys: HashSet<(ConsumerId, OwnerKey)> =
            auth_keys.union(&preserve_keys).copied().collect();
        for key in self
            .groups
            .keys()
            .copied()
            .collect::<Vec<(ConsumerId, OwnerKey)>>()
        {
            if !keep_keys.contains(&key) {
                self.remove_group(key.0, key.1);
            }
        }
        let mut ordered = authoritative;
        ordered.sort_by(|a, b| {
            pin_priority_from_consumer(a.0)
                .cmp(&pin_priority_from_consumer(b.0))
                .then_with(|| a.1.cmp(&b.1))
        });
        for (consumer, owner, pubkeys) in ordered {
            let _ = self.try_admit_group(consumer, owner, pubkeys);
        }
        let mut preserved = preserve;
        preserved.sort_by(|a, b| {
            pin_priority_from_consumer(a.0)
                .cmp(&pin_priority_from_consumer(b.0))
                .then_with(|| a.1.cmp(&b.1))
        });
        for (consumer, owner, pubkeys) in preserved {
            if auth_keys.contains(&(consumer, owner)) {
                continue;
            }
            let _ = self.try_admit_group(consumer, owner, pubkeys);
        }
        let _ = self.evict_until_within_cap(EvictReason::RestoreConverge);
    }

    /// Legacy row convergence — builds complete groups then reconciles (no clear).
    pub fn converge_from_rows(&mut self, rows: &[(Pubkey, ConsumerId, Option<Pubkey>)]) {
        let groups = build_owner_groups_from_rows(rows);
        self.reconcile_owner_groups(groups);
    }

    fn try_admit_group_with_touch(
        &mut self,
        consumer: ConsumerId,
        owner: OwnerKey,
        pubkeys: HashSet<Pubkey>,
        touch_gen: u64,
    ) -> AdmissionResult {
        let result = self.try_admit_group(consumer, owner, pubkeys);
        if self.groups.contains_key(&(consumer, owner)) {
            if let Some(g) = self.groups.get_mut(&(consumer, owner)) {
                g.last_touched_gen = touch_gen.max(g.last_touched_gen);
            }
            self.push_evict_candidate(consumer, owner);
        }
        result
    }

    fn plan_admit_group(
        &self,
        consumer: ConsumerId,
        owner: OwnerKey,
        pubkeys: HashSet<Pubkey>,
    ) -> Result<AdmissionPlan, AdmissionResult> {
        self.plan_admit_group_with_stats(consumer, owner, pubkeys, None)
    }

    fn plan_admit_group_with_stats(
        &self,
        consumer: ConsumerId,
        owner: OwnerKey,
        pubkeys: HashSet<Pubkey>,
        mut stats: Option<&mut PlanningStats>,
    ) -> Result<AdmissionPlan, AdmissionResult> {
        if pubkeys.is_empty() {
            return Err(AdmissionResult::RejectedInvalidGroup);
        }
        if let Some(existing) = self.groups.get(&(consumer, owner)) {
            if existing.pubkeys == pubkeys {
                return Err(AdmissionResult::OwnerAddedNoNewPubkey);
            }
        }

        let net_delta =
            incoming_net_entry_delta(self, consumer, owner, &pubkeys, stats.as_deref_mut());
        let projected_without_evict = (self.entries.len() as i64 + net_delta).max(0) as usize;

        let mut evictions = Vec::new();
        if projected_without_evict > self.max_explicit_pubkeys {
            let overlay = PlanningOverlay::for_incoming_admission(
                self,
                consumer,
                owner,
                &pubkeys,
                stats.as_deref_mut(),
            );
            match self.plan_evictions_with_overlay(
                overlay,
                Some((consumer, owner, &pubkeys)),
                self.max_explicit_pubkeys,
                stats.as_deref_mut(),
            ) {
                Some(victims) => evictions = victims,
                None => return Err(AdmissionResult::RejectedCap),
            }
        }

        let plan = AdmissionPlan {
            evictions,
            incoming: Some((consumer, owner, pubkeys)),
        };
        if !self.validate_plan_fits_cap(&plan, stats) {
            return Err(AdmissionResult::RejectedCap);
        }
        Ok(plan)
    }

    fn apply_plan(&mut self, plan: AdmissionPlan) -> AdmissionResult {
        debug_assert!(self.validate_plan_fits_cap(&plan, None));
        let Some((consumer, owner, pubkeys)) = plan.incoming else {
            return AdmissionResult::RejectedInvalidGroup;
        };
        let new_unique: HashSet<Pubkey> = pubkeys
            .iter()
            .filter(|pk| !self.entries.contains_key(pk))
            .copied()
            .collect();

        for (victim_consumer, victim_owner, reason) in plan.evictions {
            self.evict_group(victim_consumer, victim_owner, reason);
        }

        self.commit_group(consumer, owner, pubkeys);
        self.compact_evict_heap_if_needed();
        if self.len() > self.max_explicit_pubkeys {
            return AdmissionResult::RejectedInternal;
        }
        if new_unique.is_empty() {
            AdmissionResult::OwnerAddedNoNewPubkey
        } else {
            AdmissionResult::Admitted {
                new_pubkeys: new_unique.len(),
            }
        }
    }

    fn validate_plan_fits_cap(
        &self,
        plan: &AdmissionPlan,
        stats: Option<&mut PlanningStats>,
    ) -> bool {
        self.projected_entry_count_after_plan(plan, stats) <= self.max_explicit_pubkeys
    }

    fn projected_entry_count_after_plan(
        &self,
        plan: &AdmissionPlan,
        mut stats: Option<&mut PlanningStats>,
    ) -> usize {
        let Some((inc_c, inc_o, incoming)) = plan.incoming.as_ref() else {
            return self.entries.len();
        };
        if plan.evictions.is_empty() {
            let delta =
                incoming_net_entry_delta(self, *inc_c, *inc_o, incoming, stats.as_deref_mut());
            return (self.entries.len() as i64 + delta).max(0) as usize;
        }
        let mut overlay = PlanningOverlay::for_incoming_admission(
            self,
            *inc_c,
            *inc_o,
            incoming,
            stats.as_deref_mut(),
        );
        for (vc, vo, _) in &plan.evictions {
            overlay.add_victim(self, (*vc, *vo), stats.as_deref_mut());
        }
        overlay.projected_len
    }

    fn build_policy_evict_heap_from_live(
        set: &DesiredExplicitSet,
        skip: Option<(ConsumerId, OwnerKey)>,
        suppressed: &HashSet<(ConsumerId, OwnerKey)>,
    ) -> BinaryHeap<PolicyEvictEntry> {
        let mut heap = BinaryHeap::new();
        for (key, group) in &set.groups {
            if group.consumer == ConsumerId::Wallet {
                continue;
            }
            if suppressed.contains(key) {
                continue;
            }
            if skip.is_some_and(|(c, o)| *key == (c, o)) {
                continue;
            }
            heap.push(PolicyEvictEntry {
                key: *key,
                priority: pin_priority_from_consumer(group.consumer),
                touch_gen: group.last_touched_gen,
                admitted_seq: group.admitted_seq,
                owner: group.owner,
            });
        }
        heap
    }

    fn pop_next_policy_victim(
        heap: &mut BinaryHeap<PolicyEvictEntry>,
        planner: &EvictionPlanner<'_>,
        incoming: ConsumerId,
        incoming_owner: OwnerKey,
        incoming_priority: PinPriority,
        mut stats: Option<&mut PlanningStats>,
    ) -> Option<((ConsumerId, OwnerKey), EvictReason)> {
        while let Some(entry) = heap.pop() {
            if let Some(stats) = stats.as_deref_mut() {
                stats.candidate_pops = stats.candidate_pops.saturating_add(1);
            }
            let (consumer, owner) = entry.key;
            if consumer == incoming && owner == incoming_owner {
                if let Some(stats) = stats.as_deref_mut() {
                    stats.stale_pops = stats.stale_pops.saturating_add(1);
                }
                continue;
            }
            if planner.is_victim(consumer, owner) {
                if let Some(stats) = stats.as_deref_mut() {
                    stats.stale_pops = stats.stale_pops.saturating_add(1);
                }
                continue;
            }
            let gp = pin_priority_from_consumer(consumer);
            let evictable = gp > incoming_priority
                || (gp == incoming_priority
                    && incoming_priority != PinPriority::Wallet
                    && consumer == incoming);
            if !evictable {
                if let Some(stats) = stats.as_deref_mut() {
                    stats.stale_pops = stats.stale_pops.saturating_add(1);
                }
                continue;
            }
            let reason = if gp > incoming_priority {
                EvictReason::HigherPriority
            } else {
                EvictReason::SamePriorityLru
            };
            return Some(((consumer, owner), reason));
        }
        None
    }

    fn plan_evictions_with_overlay(
        &self,
        mut overlay: PlanningOverlay,
        incoming: Option<(ConsumerId, OwnerKey, &HashSet<Pubkey>)>,
        cap: usize,
        mut stats: Option<&mut PlanningStats>,
    ) -> Option<Vec<(ConsumerId, OwnerKey, EvictReason)>> {
        let (incoming_consumer, incoming_owner, incoming_priority) = match incoming {
            Some((c, o, _)) => (c, o, pin_priority_from_consumer(c)),
            None => (ConsumerId::Wallet, OwnerKey::Wallet, PinPriority::Wallet),
        };

        let skip_incoming = incoming.map(|(c, o, _)| (c, o));
        let mut planner = EvictionPlanner::new(self, &overlay, stats.as_deref_mut());
        let mut heap = Self::build_policy_evict_heap_from_live(
            self,
            skip_incoming,
            &overlay.suppressed_groups,
        );
        let mut victims: Vec<(ConsumerId, OwnerKey, EvictReason)> = Vec::new();

        loop {
            if overlay.projected_len <= cap {
                return Some(victims);
            }

            let next = Self::pop_next_policy_victim(
                &mut heap,
                &planner,
                incoming_consumer,
                incoming_owner,
                incoming_priority,
                stats.as_deref_mut(),
            )?;
            let ((consumer, owner), reason) = next;
            planner.add_victim((consumer, owner), &overlay, stats.as_deref_mut());
            overlay.add_victim(self, (consumer, owner), stats.as_deref_mut());
            victims.push((consumer, owner, reason));
        }
    }

    fn commit_group(&mut self, consumer: ConsumerId, owner: OwnerKey, pubkeys: HashSet<Pubkey>) {
        self.next_seq = self.next_seq.saturating_add(1);
        self.next_touch_gen = self.next_touch_gen.saturating_add(1);
        let seq = self.next_seq;
        let touch = self.next_touch_gen;
        if let Some(prev) = self.groups.remove(&(consumer, owner)) {
            for pk in prev.pubkeys {
                if let Some(entry) = self.entries.get_mut(&pk) {
                    entry.owners.remove(&(consumer, owner));
                    if entry.owners.is_empty() {
                        self.entries.remove(&pk);
                        self.remove_pubkey_from_consumer_index(consumer, pk);
                    }
                }
            }
        }
        self.groups.insert(
            (consumer, owner),
            OwnerGroup {
                consumer,
                owner,
                pubkeys: pubkeys.clone(),
                admitted_seq: seq,
                last_touched_gen: touch,
            },
        );
        for pk in pubkeys {
            let entry = self.entries.entry(pk).or_insert_with(|| PubkeyEntry {
                owners: HashSet::new(),
            });
            entry.owners.insert((consumer, owner));
            self.by_consumer.entry(consumer).or_default().insert(pk);
        }
        self.push_evict_candidate(consumer, owner);
    }

    fn evict_group(&mut self, consumer: ConsumerId, owner: OwnerKey, reason: EvictReason) -> usize {
        let Some(group) = self.groups.remove(&(consumer, owner)) else {
            return 0;
        };
        self.heap_stamp.remove(&(consumer, owner));
        let mut freed = 0usize;
        for pk in group.pubkeys {
            if let Some(entry) = self.entries.get_mut(&pk) {
                entry.owners.remove(&(consumer, owner));
                if entry.owners.is_empty() {
                    self.entries.remove(&pk);
                    freed += 1;
                    self.remove_pubkey_from_consumer_index(consumer, pk);
                }
            }
        }
        inc_market_data_geyser_explicit_evicted_total(
            consumer_id_label(consumer),
            evict_reason_label(reason),
        );
        freed
    }

    fn evict_until_within_cap(&mut self, reason: EvictReason) -> CapConvergeResult {
        while self.entries.len() > self.max_explicit_pubkeys {
            let deficit = self.entries.len().saturating_sub(self.max_explicit_pubkeys);
            let Some(victims) = self.plan_cap_shrink_victims(deficit) else {
                if self.wallet_group_exceeds_cap() {
                    return CapConvergeResult::ProtectedOverflow;
                }
                return CapConvergeResult::Unconverged;
            };
            if victims.is_empty() {
                return CapConvergeResult::Unconverged;
            }
            let before_len = self.entries.len();
            for (consumer, owner) in victims {
                if consumer == ConsumerId::Wallet {
                    return CapConvergeResult::ProtectedOverflow;
                }
                self.evict_group(consumer, owner, reason);
            }
            if self.entries.len() >= before_len {
                return CapConvergeResult::Unconverged;
            }
        }
        self.compact_evict_heap_if_needed();
        if self.entries.len() > self.max_explicit_pubkeys {
            CapConvergeResult::Unconverged
        } else {
            CapConvergeResult::Converged
        }
    }

    /// Set-aware cap-shrink victim planning: aggregate eviction across co-dependent groups.
    fn plan_cap_shrink_victims(&self, _deficit: usize) -> Option<Vec<(ConsumerId, OwnerKey)>> {
        let overlay = PlanningOverlay::for_cap_shrink(self, None);
        let victims_with_reason =
            self.plan_evictions_with_overlay(overlay, None, self.max_explicit_pubkeys, None)?;
        Some(
            victims_with_reason
                .into_iter()
                .map(|(c, o, _)| (c, o))
                .collect(),
        )
    }

    fn compact_evict_heap_if_needed(&mut self) {
        const HEAP_COMPACT_FACTOR: usize = 4;
        if self.evict_heap.len() <= self.groups.len().saturating_mul(HEAP_COMPACT_FACTOR) {
            return;
        }
        self.evict_heap.clear();
        let keys: Vec<(ConsumerId, OwnerKey)> = self.groups.keys().copied().collect();
        for (consumer, owner) in keys {
            self.push_evict_candidate(consumer, owner);
        }
    }

    fn removable_pubkeys_for_group(&self, consumer: ConsumerId, owner: OwnerKey) -> usize {
        self.groups
            .get(&(consumer, owner))
            .map(|g| {
                g.pubkeys
                    .iter()
                    .filter(|pk| {
                        self.entries.get(pk).is_some_and(|e| {
                            e.owners.len() == 1 && e.owners.contains(&(consumer, owner))
                        })
                    })
                    .count()
            })
            .unwrap_or(0)
    }

    fn push_evict_candidate(&mut self, consumer: ConsumerId, owner: OwnerKey) {
        let Some(group) = self.groups.get(&(consumer, owner)) else {
            return;
        };
        self.global_heap_stamp = self.global_heap_stamp.saturating_add(1);
        let stamp = self.global_heap_stamp;
        self.heap_stamp.insert((consumer, owner), stamp);
        let removable = self.removable_pubkeys_for_group(consumer, owner);
        self.evict_heap.push(EvictCandidate {
            priority: pin_priority_from_consumer(group.consumer),
            touch_gen: group.last_touched_gen,
            admitted_seq: group.admitted_seq,
            owner: group.owner,
            consumer: group.consumer,
            removable,
            stamp,
        });
    }

    fn remove_pubkey_from_consumer_index(&mut self, consumer: ConsumerId, pk: Pubkey) {
        if let Some(set) = self.by_consumer.get_mut(&consumer) {
            set.remove(&pk);
            if set.is_empty() {
                self.by_consumer.remove(&consumer);
            }
        }
    }

    #[cfg(test)]
    fn test_plan_evictions(
        &self,
        incoming: Option<(ConsumerId, OwnerKey, HashSet<Pubkey>)>,
        cap: usize,
        mut stats: Option<&mut PlanningStats>,
    ) -> Option<Vec<(ConsumerId, OwnerKey, EvictReason)>> {
        let overlay = match incoming.as_ref() {
            Some((c, o, p)) => {
                PlanningOverlay::for_incoming_admission(self, *c, *o, p, stats.as_deref_mut())
            }
            None => PlanningOverlay::for_cap_shrink(self, stats.as_deref_mut()),
        };
        let incoming_ref = incoming.as_ref().map(|(c, o, p)| (*c, *o, p));
        self.plan_evictions_with_overlay(overlay, incoming_ref, cap, stats)
    }

    #[cfg(test)]
    fn test_plan_admit_group_with_stats(
        &self,
        consumer: ConsumerId,
        owner: OwnerKey,
        pubkeys: HashSet<Pubkey>,
        stats: Option<&mut PlanningStats>,
    ) -> Result<AdmissionPlan, AdmissionResult> {
        self.plan_admit_group_with_stats(consumer, owner, pubkeys, stats)
    }
}

/// Incremental marginal-free eviction planner on live state + overlay (bounded edge updates).
struct EvictionPlanner<'a> {
    set: &'a DesiredExplicitSet,
    suppressed: HashSet<(ConsumerId, OwnerKey)>,
    victims: HashSet<(ConsumerId, OwnerKey)>,
    marginal: HashMap<(ConsumerId, OwnerKey), usize>,
    marginal_contributors: HashMap<(ConsumerId, OwnerKey), HashSet<Pubkey>>,
    group_pubkeys: HashMap<(ConsumerId, OwnerKey), Vec<Pubkey>>,
    pubkey_groups: HashMap<Pubkey, Vec<(ConsumerId, OwnerKey)>>,
    projected_pubkey_refcount: HashMap<Pubkey, usize>,
}

impl<'a> EvictionPlanner<'a> {
    fn new(
        set: &'a DesiredExplicitSet,
        overlay: &PlanningOverlay,
        mut stats: Option<&mut PlanningStats>,
    ) -> Self {
        let suppressed = overlay.suppressed_groups.clone();
        let mut marginal = HashMap::new();
        let mut marginal_contributors: HashMap<(ConsumerId, OwnerKey), HashSet<Pubkey>> =
            HashMap::new();
        let mut pubkey_groups: HashMap<Pubkey, Vec<(ConsumerId, OwnerKey)>> = HashMap::new();
        let mut tracked_groups = HashSet::new();
        for pk in &overlay.touched_pubkeys {
            Self::index_pubkey(
                set,
                overlay,
                *pk,
                &mut pubkey_groups,
                &mut tracked_groups,
                stats.as_deref_mut(),
            );
        }
        for key in &tracked_groups {
            marginal.insert(*key, 0);
            marginal_contributors.insert(*key, HashSet::new());
        }
        let mut group_pubkeys = HashMap::new();
        for key in &tracked_groups {
            if let Some(group) = set.groups.get(key) {
                group_pubkeys.insert(*key, group.pubkeys.iter().copied().collect());
            }
        }
        let mut planner = Self {
            set,
            suppressed,
            victims: HashSet::new(),
            marginal,
            marginal_contributors,
            group_pubkeys,
            pubkey_groups,
            projected_pubkey_refcount: overlay.projected_pubkey_refcount.clone(),
        };
        for key in &tracked_groups {
            if let Some(group) = set.groups.get(key) {
                planner
                    .group_pubkeys
                    .entry(*key)
                    .or_insert_with(|| group.pubkeys.iter().copied().collect());
            }
        }
        for key in planner.marginal.keys().copied().collect::<Vec<_>>() {
            planner.recompute_marginal_for_group(key, stats.as_deref_mut());
        }
        planner
    }

    fn sole_projected_owner(&self, pk: &Pubkey) -> Option<(ConsumerId, OwnerKey)> {
        if self.projected_pubkey_refcount.get(pk).copied().unwrap_or(0) != 1 {
            return None;
        }
        let mut owners = self
            .pubkey_groups
            .get(pk)?
            .iter()
            .copied()
            .filter(|owner| !self.suppressed.contains(owner) && !self.victims.contains(owner));
        owners.next()
    }

    fn index_pubkey(
        set: &DesiredExplicitSet,
        overlay: &PlanningOverlay,
        pk: Pubkey,
        pubkey_groups: &mut HashMap<Pubkey, Vec<(ConsumerId, OwnerKey)>>,
        tracked_groups: &mut HashSet<(ConsumerId, OwnerKey)>,
        mut stats: Option<&mut PlanningStats>,
    ) {
        if let Some(entry) = set.entries.get(&pk) {
            for owner in &entry.owners {
                if overlay.suppressed_groups.contains(owner) {
                    continue;
                }
                if let Some(s) = stats.as_mut() {
                    s.owner_edge_iterations = s.owner_edge_iterations.saturating_add(1);
                }
                tracked_groups.insert(*owner);
                pubkey_groups.entry(pk).or_default().push(*owner);
            }
        }
        if let Some((key, pubs)) = overlay.incoming.as_ref() {
            if pubs.contains(&pk) && !overlay.suppressed_groups.contains(key) {
                if let Some(s) = stats.as_mut() {
                    s.owner_edge_iterations = s.owner_edge_iterations.saturating_add(1);
                }
                tracked_groups.insert(*key);
                pubkey_groups.entry(pk).or_default().push(*key);
            }
        }
    }

    fn recompute_marginal_for_group(
        &mut self,
        group: (ConsumerId, OwnerKey),
        mut stats: Option<&mut PlanningStats>,
    ) {
        if self.victims.contains(&group) {
            self.marginal.insert(group, 0);
            self.marginal_contributors.insert(group, HashSet::new());
            return;
        }
        let Some(g) = self.set.groups.get(&group) else {
            self.marginal.insert(group, 0);
            self.marginal_contributors.insert(group, HashSet::new());
            return;
        };
        let mut contributors = HashSet::new();
        for pk in &g.pubkeys {
            if let Some(stats) = stats.as_deref_mut() {
                stats.refcount_checks = stats.refcount_checks.saturating_add(1);
            }
            if self.pk_counts_toward_marginal(pk, group) {
                contributors.insert(*pk);
            }
        }
        self.marginal.insert(group, contributors.len());
        self.marginal_contributors.insert(group, contributors);
    }

    fn pk_counts_toward_marginal(&self, pk: &Pubkey, group: (ConsumerId, OwnerKey)) -> bool {
        self.projected_pubkey_refcount.get(pk).copied().unwrap_or(0) == 1
            && self.sole_projected_owner(pk) == Some(group)
    }

    fn is_victim(&self, consumer: ConsumerId, owner: OwnerKey) -> bool {
        self.victims.contains(&(consumer, owner))
    }

    fn marginal_freed(&self, victim: (ConsumerId, OwnerKey)) -> usize {
        self.marginal.get(&victim).copied().unwrap_or(0)
    }

    fn add_victim(
        &mut self,
        victim: (ConsumerId, OwnerKey),
        _overlay: &PlanningOverlay,
        mut stats: Option<&mut PlanningStats>,
    ) -> usize {
        let victim_pubkeys: Vec<Pubkey> = self
            .set
            .groups
            .get(&victim)
            .map(|group| group.pubkeys.iter().copied().collect())
            .unwrap_or_default();
        if !victim_pubkeys.is_empty() {
            self.group_pubkeys.insert(victim, victim_pubkeys.clone());
        }
        for pk in &victim_pubkeys {
            if !self.pubkey_groups.contains_key(pk) {
                if let Some(entry) = self.set.entries.get(pk) {
                    for owner in &entry.owners {
                        if !self.suppressed.contains(owner) {
                            self.pubkey_groups.entry(*pk).or_default().push(*owner);
                        }
                    }
                }
            }
        }
        if !self.marginal.contains_key(&victim) {
            self.recompute_marginal_for_group(victim, stats.as_deref_mut());
        }

        let freed = self.marginal_freed(victim);
        let before_counts: Vec<_> = victim_pubkeys
            .iter()
            .map(|pk| {
                (
                    *pk,
                    self.projected_pubkey_refcount
                        .get(pk)
                        .copied()
                        .unwrap_or_else(|| {
                            self.pubkey_groups
                                .get(pk)
                                .map(|owners| {
                                    owners
                                        .iter()
                                        .filter(|owner| {
                                            !self.suppressed.contains(owner)
                                                && !self.victims.contains(owner)
                                        })
                                        .count()
                                })
                                .unwrap_or(0)
                        }),
                )
            })
            .collect();
        self.victims.insert(victim);

        for (pk, before) in before_counts {
            let after = before.saturating_sub(1);
            self.projected_pubkey_refcount.insert(pk, after);
            if before >= 2 && after == 1 {
                if let Some(sole) = self.sole_projected_owner(&pk) {
                    if let Some(contributors) = self.marginal_contributors.get_mut(&sole) {
                        if contributors.insert(pk) {
                            if let Some(m) = self.marginal.get_mut(&sole) {
                                *m = m.saturating_add(1);
                            }
                            if let Some(stats) = stats.as_deref_mut() {
                                stats.edge_updates = stats.edge_updates.saturating_add(1);
                            }
                        }
                    }
                }
            }
        }

        self.marginal.insert(victim, 0);
        self.marginal_contributors.insert(victim, HashSet::new());

        freed
    }
}

fn build_owner_groups_from_rows(
    rows: &[(Pubkey, ConsumerId, Option<Pubkey>)],
) -> Vec<(ConsumerId, OwnerKey, HashSet<Pubkey>)> {
    let mut grouped: HashMap<(ConsumerId, OwnerKey), HashSet<Pubkey>> = HashMap::new();
    for (pk, consumer, pool) in rows {
        let owner = owner_key_from_row(*consumer, *pool, *pk);
        grouped.entry((*consumer, owner)).or_default().insert(*pk);
    }
    grouped
        .into_iter()
        .map(|((consumer, owner), pubkeys)| (consumer, owner, pubkeys))
        .collect()
}

fn owner_key_from_row(consumer: ConsumerId, pool: Option<Pubkey>, pk: Pubkey) -> OwnerKey {
    match consumer {
        ConsumerId::Wallet => OwnerKey::Wallet,
        ConsumerId::Tracker if pool.is_none() => OwnerKey::Mint(pk),
        _ => OwnerKey::Pool(pool.unwrap_or(pk)),
    }
}

pub fn pin_priority_from_consumer(consumer: ConsumerId) -> PinPriority {
    match consumer {
        ConsumerId::Wallet => PinPriority::Wallet,
        ConsumerId::Momentum => PinPriority::Momentum,
        ConsumerId::Arb => PinPriority::Arb,
        ConsumerId::Tracker => PinPriority::Tracker,
    }
}

fn consumer_id_label(consumer: ConsumerId) -> &'static str {
    match consumer {
        ConsumerId::Wallet => "wallet",
        ConsumerId::Momentum => "momentum",
        ConsumerId::Arb => "arb",
        ConsumerId::Tracker => "tracker",
    }
}

fn evict_reason_label(reason: EvictReason) -> &'static str {
    match reason {
        EvictReason::HigherPriority => "higher_priority",
        EvictReason::SamePriorityLru => "same_priority_lru",
        EvictReason::CapShrink => "cap_shrink",
        EvictReason::RestoreConverge => "restore_converge",
    }
}

/// Symmetric set difference for Geyser subscribe delta (|A Δ B|).
pub fn symmetric_diff(a: &HashSet<Pubkey>, b: &HashSet<Pubkey>) -> HashSet<Pubkey> {
    a.symmetric_difference(b).copied().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pool_owner() -> (ConsumerId, OwnerKey, Pubkey) {
        let pool = Pubkey::new_unique();
        (ConsumerId::Arb, OwnerKey::Pool(pool), pool)
    }

    #[test]
    fn deduplicated_pubkeys_stay_within_cap() {
        let mut set = DesiredExplicitSet::new(4);
        let wallet = Pubkey::new_unique();
        assert!(matches!(
            set.try_admit_group(
                ConsumerId::Wallet,
                OwnerKey::Wallet,
                HashSet::from([wallet]),
            ),
            AdmissionResult::Admitted { .. }
        ));
        for _ in 0..10 {
            let (c, o, _) = pool_owner();
            let a = Pubkey::new_unique();
            let b = Pubkey::new_unique();
            let _ = set.try_admit_group(c, o, HashSet::from([a, b]));
            assert!(set.len() <= 4);
        }
    }

    #[test]
    fn rejected_group_does_not_mutate_desired_set() {
        let mut set = DesiredExplicitSet::new(2);
        let p1 = Pubkey::new_unique();
        let p2 = Pubkey::new_unique();
        let p3 = Pubkey::new_unique();
        let (_, o1, _) = pool_owner();
        let (c2, o2, _) = pool_owner();
        assert!(matches!(
            set.try_admit_group(ConsumerId::Wallet, OwnerKey::Wallet, HashSet::from([p1])),
            AdmissionResult::Admitted { .. }
        ));
        assert!(matches!(
            set.try_admit_group(ConsumerId::Momentum, o1, HashSet::from([p2])),
            AdmissionResult::Admitted { .. }
        ));
        let len_before = set.len();
        let admitted_before = set.snapshot_pubkeys();
        assert!(matches!(
            set.try_admit_group(c2, o2, HashSet::from([p3])),
            AdmissionResult::RejectedCap
        ));
        assert_eq!(set.len(), len_before);
        assert_eq!(set.snapshot_pubkeys(), admitted_before);
        assert!(!set.contains(&p3));
    }

    #[test]
    fn rejected_eviction_with_insufficient_removable_space_leaves_state_unchanged() {
        let mut set = DesiredExplicitSet::new(3);
        let shared = Pubkey::new_unique();
        let (_, o1, _) = pool_owner();
        let (_, o2, _) = pool_owner();
        let extra = Pubkey::new_unique();
        assert!(matches!(
            set.try_admit_group(ConsumerId::Arb, o1, HashSet::from([shared])),
            AdmissionResult::Admitted { .. }
        ));
        assert!(matches!(
            set.try_admit_group(ConsumerId::Momentum, o2, HashSet::from([shared, extra])),
            AdmissionResult::Admitted { .. }
        ));
        let before = set.snapshot_pubkeys();
        let (c3, o3, _) = pool_owner();
        let need_three = Pubkey::new_unique();
        let need_four = Pubkey::new_unique();
        let need_five = Pubkey::new_unique();
        assert!(matches!(
            set.try_admit_group(c3, o3, HashSet::from([need_three, need_four, need_five])),
            AdmissionResult::RejectedCap
        ));
        assert_eq!(set.snapshot_pubkeys(), before);
    }

    #[test]
    fn two_vault_group_admitted_or_rejected_atomically() {
        let mut set = DesiredExplicitSet::new(3);
        let (_, o1, _) = pool_owner();
        let v1 = Pubkey::new_unique();
        let v2 = Pubkey::new_unique();
        assert!(matches!(
            set.try_admit_group(ConsumerId::Momentum, o1, HashSet::from([v1, v2])),
            AdmissionResult::Admitted { new_pubkeys: 2 }
        ));
        let wallet = Pubkey::new_unique();
        assert!(matches!(
            set.try_admit_group(
                ConsumerId::Wallet,
                OwnerKey::Wallet,
                HashSet::from([wallet])
            ),
            AdmissionResult::Admitted { .. }
        ));
        let (_, o2, _) = pool_owner();
        let v3 = Pubkey::new_unique();
        let v4 = Pubkey::new_unique();
        assert!(matches!(
            set.try_admit_group(ConsumerId::Arb, o2, HashSet::from([v3, v4])),
            AdmissionResult::RejectedCap
        ));
        assert!(!set.contains(&v3));
        assert!(!set.contains(&v4));
    }

    #[test]
    fn shared_pubkey_survives_single_owner_removal() {
        let mut set = DesiredExplicitSet::new(10);
        let shared = Pubkey::new_unique();
        let (_, o1, _) = pool_owner();
        let (_, o2, _) = pool_owner();
        assert!(matches!(
            set.try_admit_group(ConsumerId::Arb, o1, HashSet::from([shared])),
            AdmissionResult::Admitted { .. }
        ));
        assert!(matches!(
            set.try_admit_group(ConsumerId::Momentum, o2, HashSet::from([shared])),
            AdmissionResult::OwnerAddedNoNewPubkey
        ));
        set.remove_group(ConsumerId::Arb, o1);
        assert!(set.contains(&shared));
        set.remove_group(ConsumerId::Momentum, o2);
        assert!(!set.contains(&shared));
    }

    #[test]
    fn wallet_never_evicted_by_momentum() {
        let wallet = Pubkey::new_unique();
        let mut set = DesiredExplicitSet::new(2);
        assert!(matches!(
            set.try_admit_group(
                ConsumerId::Wallet,
                OwnerKey::Wallet,
                HashSet::from([wallet])
            ),
            AdmissionResult::Admitted { .. }
        ));
        let (_, o, _) = pool_owner();
        let m1 = Pubkey::new_unique();
        let m2 = Pubkey::new_unique();
        assert!(matches!(
            set.try_admit_group(ConsumerId::Momentum, o, HashSet::from([m1])),
            AdmissionResult::Admitted { .. }
        ));
        let (_, o2, _) = pool_owner();
        assert!(matches!(
            set.try_admit_group(ConsumerId::Momentum, o2, HashSet::from([m2])),
            AdmissionResult::Admitted { .. }
        ));
        assert!(set.contains(&wallet));
    }

    #[test]
    fn momentum_evicts_arb_before_momentum() {
        let mut set = DesiredExplicitSet::new(1);
        let arb_pk = Pubkey::new_unique();
        let (_, arb_owner, _) = pool_owner();
        assert!(matches!(
            set.try_admit_group(ConsumerId::Arb, arb_owner, HashSet::from([arb_pk])),
            AdmissionResult::Admitted { .. }
        ));
        let mom_pk = Pubkey::new_unique();
        let (_, mom_owner, _) = pool_owner();
        assert!(matches!(
            set.try_admit_group(ConsumerId::Momentum, mom_owner, HashSet::from([mom_pk])),
            AdmissionResult::Admitted { .. }
        ));
        assert!(!set.contains(&arb_pk));
        assert!(set.contains(&mom_pk));
    }

    #[test]
    fn arb_cannot_evict_momentum() {
        let mut set = DesiredExplicitSet::new(2);
        let mom_pk = Pubkey::new_unique();
        let (_, mom_owner, _) = pool_owner();
        assert!(matches!(
            set.try_admit_group(ConsumerId::Momentum, mom_owner, HashSet::from([mom_pk])),
            AdmissionResult::Admitted { .. }
        ));
        let wallet = Pubkey::new_unique();
        assert!(matches!(
            set.try_admit_group(
                ConsumerId::Wallet,
                OwnerKey::Wallet,
                HashSet::from([wallet])
            ),
            AdmissionResult::Admitted { .. }
        ));
        let arb_pk = Pubkey::new_unique();
        let (_, arb_owner, _) = pool_owner();
        assert!(matches!(
            set.try_admit_group(ConsumerId::Arb, arb_owner, HashSet::from([arb_pk])),
            AdmissionResult::RejectedCap
        ));
        assert!(set.contains(&mom_pk));
        assert!(!set.contains(&arb_pk));
    }

    #[test]
    fn shared_pubkey_stale_heap_candidates_never_exceed_cap_in_release() {
        let mut set = DesiredExplicitSet::new(2);
        let shared = Pubkey::new_unique();
        let (_, o1, _) = pool_owner();
        let (_, o2, _) = pool_owner();
        let extra = Pubkey::new_unique();
        assert!(matches!(
            set.try_admit_group(ConsumerId::Arb, o1, HashSet::from([shared])),
            AdmissionResult::Admitted { .. }
        ));
        assert!(matches!(
            set.try_admit_group(ConsumerId::Momentum, o2, HashSet::from([shared, extra])),
            AdmissionResult::Admitted { .. }
        ));
        for _ in 0..8 {
            set.touch_group(ConsumerId::Arb, o1);
        }
        let len_before = set.len();
        assert_eq!(len_before, 2);
        let (_, o3, _) = pool_owner();
        let n1 = Pubkey::new_unique();
        let n2 = Pubkey::new_unique();
        assert!(matches!(
            set.try_admit_group(ConsumerId::Momentum, o3, HashSet::from([n1, n2])),
            AdmissionResult::Admitted { .. } | AdmissionResult::RejectedCap
        ));
        assert!(set.len() <= set.max_explicit_pubkeys());
    }

    #[test]
    fn same_priority_uses_lru_not_fifo_via_readmit() {
        let mut set = DesiredExplicitSet::new(2);
        let (_, old_owner, _) = pool_owner();
        let old_pk = Pubkey::new_unique();
        assert!(matches!(
            set.try_admit_group(ConsumerId::Momentum, old_owner, HashSet::from([old_pk])),
            AdmissionResult::Admitted { .. }
        ));
        let (_, new_owner, _) = pool_owner();
        let new_pk = Pubkey::new_unique();
        assert!(matches!(
            set.try_admit_group(ConsumerId::Momentum, new_owner, HashSet::from([new_pk])),
            AdmissionResult::Admitted { .. }
        ));
        assert!(matches!(
            set.try_admit_group(ConsumerId::Momentum, old_owner, HashSet::from([old_pk])),
            AdmissionResult::OwnerAddedNoNewPubkey
        ));
        let (_, incoming_owner, _) = pool_owner();
        let incoming_pk = Pubkey::new_unique();
        assert!(matches!(
            set.try_admit_group(
                ConsumerId::Momentum,
                incoming_owner,
                HashSet::from([incoming_pk])
            ),
            AdmissionResult::Admitted { .. }
        ));
        assert!(
            !set.contains(&new_pk),
            "LRU victim should be untouched newer group"
        );
        assert!(set.contains(&old_pk));
        assert!(set.contains(&incoming_pk));
    }

    #[test]
    fn wallet_over_cap_fails_closed() {
        let mut set = DesiredExplicitSet::new(2);
        let demand: HashSet<Pubkey> = (0..3).map(|_| Pubkey::new_unique()).collect();
        assert!(set.wallet_demand_exceeds_cap(&demand));
        assert!(matches!(
            set.try_admit_wallet_demand(demand),
            AdmissionResult::RejectedCap
        ));
        assert!(set.is_empty());
    }

    #[test]
    fn empty_group_is_invalid_not_protected() {
        let mut set = DesiredExplicitSet::new(10);
        assert!(matches!(
            set.try_admit_group(ConsumerId::Wallet, OwnerKey::Wallet, HashSet::new()),
            AdmissionResult::RejectedInvalidGroup
        ));
    }

    #[test]
    fn cap_shrink_converges_deterministically() {
        let mut set = DesiredExplicitSet::new(5);
        for _ in 0..4 {
            let (_, o, _) = pool_owner();
            let pk = Pubkey::new_unique();
            let _ = set.try_admit_group(ConsumerId::Tracker, o, HashSet::from([pk]));
        }
        set.set_max_explicit_pubkeys(2);
        assert!(set.len() <= 2);
        assert_eq!(set.cap_overflow(), 0);
    }

    #[test]
    fn fully_shared_groups_cap_shrink_converges_via_multi_group_eviction() {
        let mut set = DesiredExplicitSet::new(3);
        let shared_a = Pubkey::new_unique();
        let shared_b = Pubkey::new_unique();
        let (_, o1, _) = pool_owner();
        let (_, o2, _) = pool_owner();
        assert!(matches!(
            set.try_admit_group(ConsumerId::Arb, o1, HashSet::from([shared_a, shared_b])),
            AdmissionResult::Admitted { .. }
        ));
        assert!(matches!(
            set.try_admit_group(
                ConsumerId::Momentum,
                o2,
                HashSet::from([shared_a, shared_b])
            ),
            AdmissionResult::OwnerAddedNoNewPubkey
        ));
        assert_eq!(set.len(), 2);
        assert_eq!(
            set.set_max_explicit_pubkeys(1),
            CapConvergeResult::Converged
        );
        assert!(set.len() <= 1);
        assert_eq!(set.cap_overflow(), 0);
    }

    #[test]
    fn cap_shrink_never_evicts_wallet_group() {
        let wallet = Pubkey::new_unique();
        let mut set = DesiredExplicitSet::new(3);
        assert!(matches!(
            set.try_admit_group(
                ConsumerId::Wallet,
                OwnerKey::Wallet,
                HashSet::from([wallet])
            ),
            AdmissionResult::Admitted { .. }
        ));
        for _ in 0..3 {
            let (_, o, _) = pool_owner();
            let pk = Pubkey::new_unique();
            let _ = set.try_admit_group(ConsumerId::Tracker, o, HashSet::from([pk]));
        }
        assert_eq!(
            set.set_max_explicit_pubkeys(1),
            CapConvergeResult::Converged
        );
        assert!(set.contains(&wallet));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn protected_wallet_overflow_fails_cap_shrink_closed() {
        let mut set = DesiredExplicitSet::new(5);
        let demand: HashSet<Pubkey> = (0..4).map(|_| Pubkey::new_unique()).collect();
        assert!(matches!(
            set.try_admit_wallet_demand(demand),
            AdmissionResult::Admitted { .. }
        ));
        assert_eq!(
            set.set_max_explicit_pubkeys(2),
            CapConvergeResult::ProtectedOverflow
        );
        assert!(set.len() > 2);
    }

    #[test]
    fn restore_owner_groups_preserves_shared_pubkey() {
        let mut set = DesiredExplicitSet::new(10);
        let shared = Pubkey::new_unique();
        let pool_a = Pubkey::new_unique();
        let pool_b = Pubkey::new_unique();
        let only_a = Pubkey::new_unique();
        set.restore_owner_groups(&[
            OwnerGroupSnapshot {
                consumer: ConsumerId::Momentum,
                owner: OwnerKey::Pool(pool_a),
                pubkeys: HashSet::from([shared, only_a]),
                last_touched_gen: 1,
            },
            OwnerGroupSnapshot {
                consumer: ConsumerId::Arb,
                owner: OwnerKey::Pool(pool_b),
                pubkeys: HashSet::from([shared]),
                last_touched_gen: 2,
            },
        ]);
        set.remove_group(ConsumerId::Momentum, OwnerKey::Pool(pool_a));
        assert!(set.contains(&shared));
        set.restore_owner_groups(&[OwnerGroupSnapshot {
            consumer: ConsumerId::Arb,
            owner: OwnerKey::Pool(pool_b),
            pubkeys: HashSet::from([shared]),
            last_touched_gen: 2,
        }]);
        assert!(set.contains(&shared));
    }

    #[test]
    fn snapshot_over_cap_prioritized_reduce() {
        let rows: Vec<(Pubkey, ConsumerId, Option<Pubkey>)> = (0..6)
            .map(|i| {
                let pk = Pubkey::new_unique();
                let pool = Pubkey::new_unique();
                let consumer = if i == 0 {
                    ConsumerId::Wallet
                } else if i < 3 {
                    ConsumerId::Momentum
                } else {
                    ConsumerId::Arb
                };
                (pk, consumer, Some(pool))
            })
            .collect();
        let mut set = DesiredExplicitSet::new(3);
        set.converge_from_rows(&rows);
        assert!(set.len() <= 3);
        let wallet_row = rows[0].0;
        assert!(set.contains(&wallet_row));
    }

    #[test]
    fn admission_rejected_cap_does_not_mutate_before_plan_apply() {
        let mut set = DesiredExplicitSet::new(2);
        let pool_a = Pubkey::new_unique();
        let pool_b = Pubkey::new_unique();
        let k1 = Pubkey::new_unique();
        let k2 = Pubkey::new_unique();
        let k3 = Pubkey::new_unique();
        assert!(matches!(
            set.try_admit_group(
                ConsumerId::Momentum,
                OwnerKey::Pool(pool_a),
                HashSet::from([k1, k2])
            ),
            AdmissionResult::Admitted { .. }
        ));
        let before = set.snapshot_pubkeys();
        assert!(matches!(
            set.try_admit_group(ConsumerId::Arb, OwnerKey::Pool(pool_b), HashSet::from([k3])),
            AdmissionResult::RejectedCap
        ));
        assert_eq!(set.snapshot_pubkeys(), before);
    }

    #[test]
    fn symmetric_diff_detects_add_and_remove() {
        let a: HashSet<Pubkey> = [Pubkey::new_unique(), Pubkey::new_unique()]
            .into_iter()
            .collect();
        let mut b = a.clone();
        let added = Pubkey::new_unique();
        b.insert(added);
        let removed: Pubkey = *a.iter().next().unwrap();
        b.remove(&removed);
        let delta = symmetric_diff(&a, &b);
        assert_eq!(delta.len(), 2);
        assert!(delta.contains(&added));
        assert!(delta.contains(&removed));
    }

    #[test]
    fn exclusive_key_replacement_at_cap_succeeds_without_unrelated_eviction() {
        let mut set = DesiredExplicitSet::new(3);
        let wallet = Pubkey::new_unique();
        assert!(matches!(
            set.try_admit_group(
                ConsumerId::Wallet,
                OwnerKey::Wallet,
                HashSet::from([wallet])
            ),
            AdmissionResult::Admitted { .. }
        ));
        let (_, arb_owner, _) = pool_owner();
        let arb_pk = Pubkey::new_unique();
        assert!(matches!(
            set.try_admit_group(ConsumerId::Arb, arb_owner, HashSet::from([arb_pk])),
            AdmissionResult::Admitted { .. }
        ));
        let pool = Pubkey::new_unique();
        let owner = OwnerKey::Pool(pool);
        let old = Pubkey::new_unique();
        assert!(matches!(
            set.try_admit_group(ConsumerId::Momentum, owner, HashSet::from([old])),
            AdmissionResult::Admitted { .. }
        ));
        assert_eq!(set.len(), 3);

        let new_pk = Pubkey::new_unique();
        assert!(matches!(
            set.try_admit_group(ConsumerId::Momentum, owner, HashSet::from([new_pk])),
            AdmissionResult::Admitted { .. }
        ));
        assert!(set.contains(&arb_pk), "unrelated arb group must survive");
        assert!(set.contains(&wallet));
        assert!(!set.contains(&old));
        assert!(set.contains(&new_pk));
        assert_eq!(set.len(), 3);
    }

    #[test]
    fn shared_key_owner_group_replacement_at_cap_without_unrelated_eviction() {
        let mut set = DesiredExplicitSet::new(3);
        let shared = Pubkey::new_unique();
        let (_, arb_owner, _) = pool_owner();
        let arb_excl = Pubkey::new_unique();
        assert!(matches!(
            set.try_admit_group(
                ConsumerId::Arb,
                arb_owner,
                HashSet::from([shared, arb_excl])
            ),
            AdmissionResult::Admitted { .. }
        ));
        let pool = Pubkey::new_unique();
        let owner = OwnerKey::Pool(pool);
        let mom_excl_old = Pubkey::new_unique();
        assert!(matches!(
            set.try_admit_group(
                ConsumerId::Momentum,
                owner,
                HashSet::from([shared, mom_excl_old])
            ),
            AdmissionResult::Admitted { .. }
        ));
        assert_eq!(set.len(), 3);

        let mom_excl_new = Pubkey::new_unique();
        assert!(matches!(
            set.try_admit_group(
                ConsumerId::Momentum,
                owner,
                HashSet::from([shared, mom_excl_new])
            ),
            AdmissionResult::Admitted { .. } | AdmissionResult::OwnerAddedNoNewPubkey
        ));
        assert!(set.contains(&arb_excl));
        assert!(!set.contains(&mom_excl_old));
        assert!(set.contains(&mom_excl_new));
        assert!(set.contains(&shared));
        assert_eq!(set.len(), 3);
    }

    #[test]
    fn dlmm_window_shift_equal_bins_at_cap_succeeds_without_eviction() {
        let mut set = DesiredExplicitSet::new(4);
        let wallet = Pubkey::new_unique();
        let (_, arb_owner, _) = pool_owner();
        let arb_pk = Pubkey::new_unique();
        let pool = Pubkey::new_unique();
        let owner = OwnerKey::Pool(pool);
        let b1 = Pubkey::new_unique();
        let b2 = Pubkey::new_unique();
        assert!(matches!(
            set.try_admit_group(
                ConsumerId::Wallet,
                OwnerKey::Wallet,
                HashSet::from([wallet])
            ),
            AdmissionResult::Admitted { .. }
        ));
        assert!(matches!(
            set.try_admit_group(ConsumerId::Arb, arb_owner, HashSet::from([arb_pk])),
            AdmissionResult::Admitted { .. }
        ));
        assert!(matches!(
            set.try_admit_group(ConsumerId::Momentum, owner, HashSet::from([b1, b2])),
            AdmissionResult::Admitted { .. }
        ));
        assert_eq!(set.len(), 4);

        let b3 = Pubkey::new_unique();
        let b4 = Pubkey::new_unique();
        assert!(matches!(
            set.try_admit_group(ConsumerId::Momentum, owner, HashSet::from([b3, b4])),
            AdmissionResult::Admitted { .. }
        ));
        assert!(set.contains(&arb_pk));
        assert!(set.contains(&wallet));
        assert!(!set.contains(&b1));
        assert!(!set.contains(&b2));
        assert!(set.contains(&b3));
        assert!(set.contains(&b4));
        assert_eq!(set.len(), 4);
    }

    #[test]
    fn replacement_rejected_at_cap_leaves_state_unchanged() {
        let mut set = DesiredExplicitSet::new(2);
        let wallet = Pubkey::new_unique();
        let pool = Pubkey::new_unique();
        let owner = OwnerKey::Pool(pool);
        let k1 = Pubkey::new_unique();
        let k2 = Pubkey::new_unique();
        assert!(matches!(
            set.try_admit_group(
                ConsumerId::Wallet,
                OwnerKey::Wallet,
                HashSet::from([wallet])
            ),
            AdmissionResult::Admitted { .. }
        ));
        assert!(matches!(
            set.try_admit_group(ConsumerId::Momentum, owner, HashSet::from([k1])),
            AdmissionResult::Admitted { .. }
        ));
        let before = set.snapshot_pubkeys();
        assert!(matches!(
            set.try_admit_group(
                ConsumerId::Momentum,
                owner,
                HashSet::from([k2, Pubkey::new_unique()])
            ),
            AdmissionResult::RejectedCap
        ));
        assert_eq!(set.snapshot_pubkeys(), before);
    }

    #[test]
    fn replacement_victim_chain_partial_overlap_frees_exact_capacity() {
        let mut set = DesiredExplicitSet::new(3);
        let shared = Pubkey::new_unique();
        let pk_a = Pubkey::new_unique();
        let pk_b = Pubkey::new_unique();
        let pk_c = Pubkey::new_unique();
        let pk_d = Pubkey::new_unique();
        let pool_m = Pubkey::new_unique();
        let owner_m = OwnerKey::Pool(pool_m);
        let (_, owner_arb, _) = pool_owner();

        assert!(matches!(
            set.try_admit_group(ConsumerId::Momentum, owner_m, HashSet::from([shared, pk_b])),
            AdmissionResult::Admitted { .. }
        ));
        assert!(matches!(
            set.try_admit_group(ConsumerId::Arb, owner_arb, HashSet::from([shared, pk_a])),
            AdmissionResult::Admitted { .. }
        ));
        assert_eq!(set.len(), 3);

        assert!(matches!(
            set.try_admit_group(ConsumerId::Momentum, owner_m, HashSet::from([pk_c, pk_d])),
            AdmissionResult::Admitted { .. }
        ));
        assert!(!set.contains(&pk_a), "arb victim jointly frees shared+a");
        assert!(!set.contains(&shared));
        assert!(set.contains(&pk_c));
        assert!(set.contains(&pk_d));
        assert!(!set.contains(&pk_b));
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn eviction_prefers_lower_priority_over_higher_marginal() {
        let mut set = DesiredExplicitSet::new(2);
        let (_, arb_owner, _) = pool_owner();
        let (_, tracker_owner, _) = pool_owner();
        let arb_pk = Pubkey::new_unique();
        let t1 = Pubkey::new_unique();
        let mom_pk = Pubkey::new_unique();
        assert!(matches!(
            set.try_admit_group(ConsumerId::Arb, arb_owner, HashSet::from([arb_pk])),
            AdmissionResult::Admitted { .. }
        ));
        assert!(matches!(
            set.try_admit_group(ConsumerId::Tracker, tracker_owner, HashSet::from([t1])),
            AdmissionResult::Admitted { .. }
        ));
        assert_eq!(set.len(), 2);
        assert!(matches!(
            set.try_admit_group(
                ConsumerId::Momentum,
                OwnerKey::Pool(Pubkey::new_unique()),
                HashSet::from([mom_pk])
            ),
            AdmissionResult::Admitted { .. }
        ));
        assert!(
            !set.contains(&t1),
            "tracker is lower-priority victim before arb"
        );
        assert!(set.contains(&arb_pk));
        assert!(set.contains(&mom_pk));
    }

    #[test]
    fn eviction_same_priority_prefers_older_lru_over_higher_marginal() {
        let mut set = DesiredExplicitSet::new(4);
        let (_, old_owner, _) = pool_owner();
        let (_, mid_owner, _) = pool_owner();
        let old_pk = Pubkey::new_unique();
        let b = Pubkey::new_unique();
        let c = Pubkey::new_unique();
        let d = Pubkey::new_unique();
        assert!(matches!(
            set.try_admit_group(ConsumerId::Momentum, mid_owner, HashSet::from([b, c, d])),
            AdmissionResult::Admitted { .. }
        ));
        assert!(matches!(
            set.try_admit_group(ConsumerId::Momentum, old_owner, HashSet::from([old_pk])),
            AdmissionResult::Admitted { .. }
        ));
        assert_eq!(set.len(), 4);
        let (_, incoming_owner, _) = pool_owner();
        let incoming_pk = Pubkey::new_unique();
        assert!(matches!(
            set.try_admit_group(
                ConsumerId::Momentum,
                incoming_owner,
                HashSet::from([incoming_pk])
            ),
            AdmissionResult::Admitted { .. }
        ));
        assert!(
            set.contains(&old_pk),
            "newer momentum group is LRU victim, not older peer with lower marginal"
        );
        assert!(!set.contains(&b));
        assert!(!set.contains(&c));
        assert!(!set.contains(&d));
        assert!(set.contains(&incoming_pk));
    }

    #[test]
    fn fast_path_admission_avoids_projection_clone() {
        let set = DesiredExplicitSet::new(10);
        let (_, owner, pk) = pool_owner();
        let mut stats = PlanningStats::default();
        assert!(set
            .test_plan_admit_group_with_stats(
                ConsumerId::Momentum,
                owner,
                HashSet::from([pk]),
                Some(&mut stats),
            )
            .is_ok());
        assert_eq!(stats.projection_copies, 0);
        assert_eq!(stats.entries_copied, 0);
        assert_eq!(stats.owner_edge_iterations, 2);
    }

    #[test]
    fn eviction_planner_ops_bounded_on_partial_overlap_chains() {
        let mut set = DesiredExplicitSet::new(2);
        let hub_a = Pubkey::new_unique();
        let hub_b = Pubkey::new_unique();
        for _ in 0..20 {
            let (_, owner, _) = pool_owner();
            let _ = set.try_admit_group(ConsumerId::Tracker, owner, HashSet::from([hub_a, hub_b]));
        }
        assert_eq!(set.len(), 2);
        let edge_count = set
            .snapshot_owner_groups()
            .iter()
            .map(|g| g.pubkeys.len())
            .sum::<usize>();
        let group_count = set.snapshot_owner_groups().len();
        let mut stats = PlanningStats::default();
        let victims = set
            .test_plan_evictions(None, 1, Some(&mut stats))
            .expect("cap shrink plan");
        assert!(!victims.is_empty());
        assert_eq!(stats.projection_copies, 0);
        assert_eq!(stats.entries_copied, 0);
        let victim_edge_visits: usize = victims
            .iter()
            .map(|(consumer, owner, _)| {
                set.groups
                    .get(&(*consumer, *owner))
                    .map(|g| g.pubkeys.len())
                    .unwrap_or(0)
            })
            .sum();
        let log_g = (group_count + 1).ilog2() as usize + 1;
        assert!(stats.candidate_pops <= group_count.saturating_mul(log_g));
        assert!(stats.victim_removals <= group_count);
        let owner_edge_budget = edge_count
            .saturating_add(victim_edge_visits)
            .saturating_add(stats.candidate_pops);
        assert!(
            stats.owner_edge_iterations <= owner_edge_budget,
            "owner edge visits {}/{} must stay bounded by initial+touched edges + victim edges + heap pops",
            stats.owner_edge_iterations,
            owner_edge_budget
        );
        assert_eq!(
            stats.victim_owner_edge_scans, 0,
            "victim removal must not rescan owner edges"
        );
        assert!(
            stats.refcount_checks
                <= edge_count
                    .saturating_add(victim_edge_visits)
                    .saturating_add(stats.candidate_pops),
            "refcount checks must stay bounded by initial edges + victim edges + heap pops"
        );
        assert!(
            stats.edge_updates <= victim_edge_visits.saturating_add(stats.candidate_pops),
            "marginal edge updates must stay bounded under dense pubkey sharing"
        );
        assert!(
            stats.owner_edge_iterations
                < edge_count.saturating_mul(stats.victim_removals.saturating_add(1)),
            "must not scale with edge_count * victims"
        );
        assert!(stats.candidate_pops > 0);
        assert!(stats.victim_removals > 0);
    }
}
