//! Core NATS fire-and-forget messages: arb-strategy → market-data Geyser pool pins (Phase 3).
//!
//! Spec: `Iron_crab-eval/docs/spec/ARB_TRACK_REQUESTS.md` (eval repo; wire format stable here).

use serde::{Deserialize, Serialize};

/// Wire payload for `ironcrab.v1.arb.track_requests` ([`crate::nats::topics::TOPIC_ARB_TRACK_REQUESTS`]).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArbTrackRequestsUpdate {
    pub version: u32,
    pub ts_unix_ms: u64,
    pub active: Vec<ArbTrackActiveEntry>,
    pub removed: Vec<ArbTrackRemovedEntry>,
    /// When true, `active` is the authoritative full Arb pool set: market-data unpins any pool
    /// it still holds under the Arb consumer that is not listed in `active`.
    #[serde(default)]
    pub reconcile: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArbTrackActiveEntry {
    pub pool: String,
    pub reason: ArbTrackActiveReason,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ArbTrackActiveReason {
    Baseline,
    MultiDex,
    TradeSignal,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArbTrackRemovedEntry {
    pub pool: String,
    pub reason: ArbTrackRemovedReason,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ArbTrackRemovedReason {
    Cooldown,
    Stale,
    Budget,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arb_track_requests_update_roundtrip_json() {
        let u = ArbTrackRequestsUpdate {
            version: 1,
            ts_unix_ms: 1_700_000_000,
            active: vec![ArbTrackActiveEntry {
                pool: "Pool111111111111111111111111111111111111111".to_string(),
                reason: ArbTrackActiveReason::MultiDex,
            }],
            removed: vec![ArbTrackRemovedEntry {
                pool: "Pool222222222222222222222222222222222222222".to_string(),
                reason: ArbTrackRemovedReason::Cooldown,
            }],
            reconcile: true,
        };
        let json = serde_json::to_string(&u).expect("serialize");
        let back: ArbTrackRequestsUpdate = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(u, back);
        assert!(back.reconcile);
        assert!(json.contains("\"reason\":\"multi_dex\""));
        assert!(json.contains("\"reason\":\"cooldown\""));
        assert!(json.contains("\"reconcile\":true"));
    }

    #[test]
    fn arb_track_requests_update_deserializes_without_reconcile_field() {
        let json = r#"{"version":1,"ts_unix_ms":1,"active":[],"removed":[]}"#;
        let u: ArbTrackRequestsUpdate = serde_json::from_str(json).expect("deserialize");
        assert!(!u.reconcile);
    }
}
