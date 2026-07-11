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
    Admitted { new_pubkeys: usize },
    OwnerAddedNoNewPubkey,
    RejectedCap,
    RejectedProtected,
    RejectedInvalidGroup,
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
        if pubkeys.is_empty() {
            return Err(AdmissionResult::RejectedInvalidGroup);
        }
        if let Some(existing) = self.groups.get(&(consumer, owner)) {
            if existing.pubkeys == pubkeys {
                return Err(AdmissionResult::OwnerAddedNoNewPubkey);
            }
        }

        let new_unique: HashSet<Pubkey> = pubkeys
            .iter()
            .filter(|pk| !self.entries.contains_key(pk))
            .copied()
            .collect();
        let need = new_unique.len();
        let free = self.max_explicit_pubkeys.saturating_sub(self.entries.len());

        let mut evictions = Vec::new();
        if need > free {
            let deficit = need - free;
            match self.plan_evictions_for_incoming(consumer, owner, &new_unique, deficit) {
                Some(victims) => evictions = victims,
                None => return Err(AdmissionResult::RejectedCap),
            }
        }

        let plan = AdmissionPlan {
            evictions,
            incoming: Some((consumer, owner, pubkeys)),
        };
        if !self.validate_plan_fits_cap(&plan) {
            return Err(AdmissionResult::RejectedCap);
        }
        Ok(plan)
    }

    fn apply_plan(&mut self, plan: AdmissionPlan) -> AdmissionResult {
        if !self.validate_plan_fits_cap(&plan) {
            return AdmissionResult::RejectedCap;
        }
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
            return AdmissionResult::RejectedCap;
        }
        if new_unique.is_empty() {
            AdmissionResult::OwnerAddedNoNewPubkey
        } else {
            AdmissionResult::Admitted {
                new_pubkeys: new_unique.len(),
            }
        }
    }

    fn validate_plan_fits_cap(&self, plan: &AdmissionPlan) -> bool {
        self.projected_entry_count_after_plan(plan) <= self.max_explicit_pubkeys
    }

    fn projected_entry_count_after_plan(&self, plan: &AdmissionPlan) -> usize {
        let Some((inc_c, inc_o, incoming)) = plan.incoming.as_ref() else {
            return self.entries.len();
        };
        let victims: Vec<(ConsumerId, OwnerKey)> =
            plan.evictions.iter().map(|(c, o, _)| (*c, *o)).collect();
        let after_evict = self
            .entries
            .len()
            .saturating_sub(self.unique_pubkeys_freed_if_evicted(&victims).len());
        let mut incoming_unique = 0usize;
        for pk in incoming {
            if self.entries.get(pk).is_some_and(|entry| {
                let victim_set: HashSet<_> = victims.iter().copied().collect();
                entry.owners.iter().any(|owner| !victim_set.contains(owner))
            }) {
                continue;
            }
            if let Some(existing) = self.groups.get(&(*inc_c, *inc_o)) {
                if existing.pubkeys.contains(pk) {
                    continue;
                }
            }
            incoming_unique += 1;
        }
        after_evict + incoming_unique
    }

    fn unique_pubkeys_freed_if_evicted(
        &self,
        victims: &[(ConsumerId, OwnerKey)],
    ) -> HashSet<Pubkey> {
        let victim_set: HashSet<(ConsumerId, OwnerKey)> = victims.iter().copied().collect();
        let mut freed = HashSet::new();
        for (vc, vo) in victims {
            let Some(group) = self.groups.get(&(*vc, *vo)) else {
                continue;
            };
            for pk in &group.pubkeys {
                if freed.contains(pk) {
                    continue;
                }
                if self.entries.get(pk).is_some_and(|entry| {
                    entry.owners.iter().all(|owner| victim_set.contains(owner))
                }) {
                    freed.insert(*pk);
                }
            }
        }
        freed
    }

    fn marginal_unique_freed_by_evicting(
        &self,
        victim: (ConsumerId, OwnerKey),
        already: &[(ConsumerId, OwnerKey)],
    ) -> usize {
        let mut extended = already.to_vec();
        extended.push(victim);
        let before = self.unique_pubkeys_freed_if_evicted(already).len();
        let after = self.unique_pubkeys_freed_if_evicted(&extended).len();
        after.saturating_sub(before)
    }

    fn plan_evictions_for_incoming(
        &self,
        incoming: ConsumerId,
        incoming_owner: OwnerKey,
        _new_unique: &HashSet<Pubkey>,
        mut deficit: usize,
    ) -> Option<Vec<(ConsumerId, OwnerKey, EvictReason)>> {
        let incoming_priority = pin_priority_from_consumer(incoming);
        let mut victims = Vec::new();
        let mut victim_keys = Vec::new();

        while deficit > 0 {
            let mut best_positive: Option<((ConsumerId, OwnerKey), usize, EvictReason)> = None;
            let mut best_zero: Option<((ConsumerId, OwnerKey), EvictReason)> = None;
            for (consumer, owner) in self.cap_shrink_candidate_keys() {
                if consumer == incoming && owner == incoming_owner {
                    continue;
                }
                if victim_keys
                    .iter()
                    .any(|(c, o)| *c == consumer && *o == owner)
                {
                    continue;
                }
                let gp = pin_priority_from_consumer(consumer);
                let evictable = gp > incoming_priority
                    || (gp == incoming_priority
                        && incoming_priority != PinPriority::Wallet
                        && consumer == incoming);
                if !evictable {
                    continue;
                }
                let marginal =
                    self.marginal_unique_freed_by_evicting((consumer, owner), &victim_keys);
                let reason = if gp > incoming_priority {
                    EvictReason::HigherPriority
                } else {
                    EvictReason::SamePriorityLru
                };
                if marginal > 0 {
                    let replace = best_positive.as_ref().is_none_or(|(_, m, _)| marginal > *m);
                    if replace {
                        best_positive = Some(((consumer, owner), marginal, reason));
                    }
                } else if best_zero.is_none() {
                    best_zero = Some(((consumer, owner), reason));
                }
            }
            if let Some(((consumer, owner), marginal, reason)) = best_positive {
                victim_keys.push((consumer, owner));
                victims.push((consumer, owner, reason));
                deficit = deficit.saturating_sub(marginal);
            } else if let Some(((consumer, owner), reason)) = best_zero {
                victim_keys.push((consumer, owner));
                victims.push((consumer, owner, reason));
            } else {
                return None;
            }
        }

        Some(victims)
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
    fn plan_cap_shrink_victims(&self, deficit: usize) -> Option<Vec<(ConsumerId, OwnerKey)>> {
        let mut victim_keys = Vec::new();
        let mut freed = 0usize;
        while freed < deficit {
            let mut best_positive: Option<((ConsumerId, OwnerKey), usize)> = None;
            let mut best_zero: Option<(ConsumerId, OwnerKey)> = None;
            for (consumer, owner) in self.cap_shrink_candidate_keys() {
                if victim_keys
                    .iter()
                    .any(|(c, o)| *c == consumer && *o == owner)
                {
                    continue;
                }
                let marginal =
                    self.marginal_unique_freed_by_evicting((consumer, owner), &victim_keys);
                if marginal > 0 {
                    let replace = best_positive.as_ref().is_none_or(|(_, m)| marginal > *m);
                    if replace {
                        best_positive = Some(((consumer, owner), marginal));
                    }
                } else if best_zero.is_none() {
                    best_zero = Some((consumer, owner));
                }
            }
            if let Some(((consumer, owner), marginal)) = best_positive {
                victim_keys.push((consumer, owner));
                freed = freed.saturating_add(marginal);
            } else if let Some((consumer, owner)) = best_zero {
                victim_keys.push((consumer, owner));
            } else {
                return None;
            }
        }
        Some(victim_keys)
    }

    /// Cap-shrink candidates sorted by eviction priority; Wallet is never eligible.
    fn cap_shrink_candidate_keys(&self) -> Vec<(ConsumerId, OwnerKey)> {
        let mut keys: Vec<(ConsumerId, OwnerKey)> = self
            .groups
            .keys()
            .copied()
            .filter(|(c, _)| *c != ConsumerId::Wallet)
            .collect();
        keys.sort_by(|a, b| {
            let ga = &self.groups[a];
            let gb = &self.groups[b];
            pin_priority_from_consumer(ga.consumer)
                .cmp(&pin_priority_from_consumer(gb.consumer))
                .reverse()
                .then_with(|| ga.last_touched_gen.cmp(&gb.last_touched_gen))
                .then_with(|| ga.admitted_seq.cmp(&gb.admitted_seq))
                .then_with(|| a.1.cmp(&b.1))
        });
        keys
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
}
