//! Phase 2a: single source of truth for explicit Geyser subscription pubkeys (consumer refcount).

use solana_sdk::pubkey::Pubkey;
use std::collections::{HashMap, HashSet};

/// Consumer tag for explicit Geyser pubkey ownership (Plan §5.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConsumerId {
    Wallet,
    Momentum,
    Arb,
    /// Unpinned mint / recent-trade LRU demand (I-MD-5: lowest protection).
    Tracker,
}

/// Pin priority for cap eviction — lower ordinal = higher protection (Plan §3.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PinPriority {
    Wallet = 0,
    Momentum = 1,
    Arb = 2,
    Tracker = 3,
}

#[derive(Debug, Clone)]
pub struct ExplicitEntry {
    pub consumers: HashSet<ConsumerId>,
    pub pool: Option<Pubkey>,
    pub pin_priority: PinPriority,
}

impl ExplicitEntry {
    fn recompute_pin_priority(&mut self) {
        self.pin_priority = self
            .consumers
            .iter()
            .map(|c| pin_priority_from_consumer(*c))
            .min()
            .unwrap_or(PinPriority::Tracker);
    }
}

#[derive(Debug, Clone)]
pub struct DesiredExplicitSet {
    entries: HashMap<Pubkey, ExplicitEntry>,
    by_consumer: HashMap<ConsumerId, HashSet<Pubkey>>,
    max_explicit_pubkeys: usize,
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
            by_consumer: HashMap::new(),
            max_explicit_pubkeys: max_explicit_pubkeys.max(1),
        }
    }

    pub fn max_explicit_pubkeys(&self) -> usize {
        self.max_explicit_pubkeys
    }

    pub fn set_max_explicit_pubkeys(&mut self, cap: usize) {
        self.max_explicit_pubkeys = cap.max(1);
        self.evict_if_over_cap();
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

    pub fn consumers_of(&self, pubkey: &Pubkey) -> Option<&HashSet<ConsumerId>> {
        self.entries.get(pubkey).map(|e| &e.consumers)
    }

    pub fn snapshot_pubkeys(&self) -> HashSet<Pubkey> {
        self.entries.keys().copied().collect()
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.by_consumer.clear();
    }

    /// Insert or add `consumer` refcount. Returns true when the pubkey set changed (new key).
    pub fn insert(&mut self, pubkey: Pubkey, consumer: ConsumerId, pool: Option<Pubkey>) -> bool {
        let was_new = !self.entries.contains_key(&pubkey);
        if was_new
            && self.entries.len() >= self.max_explicit_pubkeys
            && !self.evict_one_for_insert(consumer)
        {
            return false;
        }
        let entry = self.entries.entry(pubkey).or_insert_with(|| ExplicitEntry {
            consumers: HashSet::new(),
            pool,
            pin_priority: PinPriority::Tracker,
        });
        if entry.pool.is_none() {
            entry.pool = pool;
        }
        entry.consumers.insert(consumer);
        entry.recompute_pin_priority();
        self.by_consumer.entry(consumer).or_default().insert(pubkey);
        was_new
    }

    /// Remove one consumer refcount. Returns true when the pubkey was removed entirely.
    pub fn remove(&mut self, pubkey: Pubkey, consumer: ConsumerId) -> bool {
        let Some(entry) = self.entries.get_mut(&pubkey) else {
            return false;
        };
        entry.consumers.remove(&consumer);
        if let Some(set) = self.by_consumer.get_mut(&consumer) {
            set.remove(&pubkey);
        }
        if entry.consumers.is_empty() {
            self.entries.remove(&pubkey);
            return true;
        }
        entry.recompute_pin_priority();
        false
    }

    fn evict_if_over_cap(&mut self) {
        while self.entries.len() > self.max_explicit_pubkeys {
            if !self.evict_one_least_protected() {
                break;
            }
        }
    }

    /// Evict one LRU candidate when over cap (lowest pin protection first).
    fn evict_one_least_protected(&mut self) -> bool {
        let Some(victim) = self
            .entries
            .iter()
            .max_by_key(|(_, e)| e.pin_priority)
            .map(|(pk, _)| *pk)
        else {
            return false;
        };
        self.remove_entry_all_consumers(victim);
        true
    }

    /// Evict one slot for a new insert — never evict entries with higher protection than incoming.
    fn evict_one_for_insert(&mut self, incoming: ConsumerId) -> bool {
        let incoming_priority = pin_priority_from_consumer(incoming);
        let victim = self
            .entries
            .iter()
            .filter(|(_, e)| {
                e.pin_priority > incoming_priority
                    || (e.pin_priority == incoming_priority
                        && incoming_priority != PinPriority::Wallet)
            })
            .max_by_key(|(_, e)| e.pin_priority)
            .map(|(pk, _)| *pk);
        if let Some(pk) = victim {
            self.remove_entry_all_consumers(pk);
            return true;
        }
        false
    }

    fn remove_entry_all_consumers(&mut self, pubkey: Pubkey) {
        if let Some(entry) = self.entries.remove(&pubkey) {
            for consumer in entry.consumers {
                if let Some(set) = self.by_consumer.get_mut(&consumer) {
                    set.remove(&pubkey);
                }
            }
        }
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

/// Symmetric set difference for Geyser subscribe delta (|A Δ B|).
pub fn symmetric_diff(a: &HashSet<Pubkey>, b: &HashSet<Pubkey>) -> HashSet<Pubkey> {
    a.symmetric_difference(b).copied().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn phase2a_desired_set_refcount_wallet_never_evicted_by_momentum() {
        let wallet_pk = Pubkey::new_unique();
        let momentum_pks: Vec<Pubkey> = (0..4).map(|_| Pubkey::new_unique()).collect();
        let mut set = DesiredExplicitSet::new(3);

        assert!(set.insert(wallet_pk, ConsumerId::Wallet, None));
        for pk in &momentum_pks[..2] {
            assert!(set.insert(*pk, ConsumerId::Momentum, None));
        }
        assert_eq!(set.len(), 3);

        // Third momentum insert must evict a momentum row, not the wallet pin.
        assert!(set.insert(momentum_pks[2], ConsumerId::Momentum, None));
        assert!(set.contains(&wallet_pk));
        assert!(!set.contains(&momentum_pks[0]) || !set.contains(&momentum_pks[1]));
        assert!(set.contains(&momentum_pks[2]));

        let mut refcount = DesiredExplicitSet::new(10);
        let shared = Pubkey::new_unique();
        assert!(refcount.insert(shared, ConsumerId::Wallet, None));
        assert!(!refcount.insert(shared, ConsumerId::Momentum, None));
        assert!(!refcount.remove(shared, ConsumerId::Momentum));
        assert!(refcount.contains(&shared));
        assert!(refcount.remove(shared, ConsumerId::Wallet));
        assert!(!refcount.contains(&shared));
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
