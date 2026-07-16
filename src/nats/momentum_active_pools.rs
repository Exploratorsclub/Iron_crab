//! Core NATS fire-and-forget messages: momentum-bot → market-data active pool pins (PR-D).
//!
//! Spec: `Iron_crab-eval/docs/spec/MOMENTUM_ACTIVE_POOLS.md` (eval repo; wire format stable here).

use serde::{Deserialize, Serialize};

/// Wire version for [`MomentumActivePoolsUpdate`] (stable across producers).
pub const MOMENTUM_ACTIVE_POOLS_WIRE_VERSION: u32 = 1;

/// Wire payload for `ironcrab.v1.momentum.active_pools` ([`crate::nats::topics::TOPIC_MOMENTUM_ACTIVE_POOLS`]).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MomentumActivePoolsUpdate {
    pub version: u32,
    pub ts_unix_ms: u64,
    pub active: Vec<MomentumActivePoolEntry>,
    pub removed: Vec<MomentumRemovedPoolEntry>,
    /// When true, `active` is the authoritative full set: market-data unpins any `(mint, pool)`
    /// it still holds that is not listed in `active` (reconcile / lost-incremental recovery).
    #[serde(default)]
    pub full_active_snapshot: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MomentumActivePoolEntry {
    pub mint: String,
    pub pool: String,
    pub pin_reason: MomentumActivePinReason,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MomentumActivePinReason {
    Tracker,
    Position,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MomentumRemovedPoolEntry {
    pub mint: String,
    pub pool: String,
    pub reason: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn momentum_active_pools_update_roundtrip_json() {
        let u = MomentumActivePoolsUpdate {
            version: 1,
            ts_unix_ms: 1_700_000_000,
            active: vec![MomentumActivePoolEntry {
                mint: "So11111111111111111111111111111111111111112".to_string(),
                pool: "Pool111111111111111111111111111111111111111".to_string(),
                pin_reason: MomentumActivePinReason::Tracker,
            }],
            removed: vec![MomentumRemovedPoolEntry {
                mint: "Mint222222222222222222222222222222222222222".to_string(),
                pool: "Pool333333333333333333333333333333333333333".to_string(),
                reason: "stale_discovery".to_string(),
            }],
            full_active_snapshot: false,
        };
        let json = serde_json::to_string(&u).expect("serialize");
        let back: MomentumActivePoolsUpdate = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(u, back);
        assert!(!back.full_active_snapshot);
        assert!(json.contains("\"pin_reason\":\"tracker\""));
        assert!(json.contains("\"reason\":\"stale_discovery\""));
    }

    #[test]
    fn momentum_active_pools_update_deserializes_without_full_snapshot_field() {
        let json = r#"{"version":1,"ts_unix_ms":1,"active":[],"removed":[]}"#;
        let u: MomentumActivePoolsUpdate = serde_json::from_str(json).expect("deserialize");
        assert!(!u.full_active_snapshot);
    }
}
