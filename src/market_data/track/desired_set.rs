//! Consumer tags and helpers for explicit Geyser subscription ownership (Plan §5.2).
//!
//! Physical admission and cap enforcement live in [`super::explicit_admission::FixedCapAdmission`]
//! (SSOT since PR 4c). This module retains shared types only.

use solana_sdk::pubkey::Pubkey;
use std::collections::HashSet;

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
