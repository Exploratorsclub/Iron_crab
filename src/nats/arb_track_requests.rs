//! Core NATS fire-and-forget messages: arb-strategy → market-data Geyser pool pins (Phase 3).
//!
//! Spec: `Iron_crab-eval/docs/spec/ARB_TRACK_REQUESTS.md` (eval repo; wire format stable here).

use serde::{Deserialize, Serialize};

/// Max serialized JSON size per NATS publish (server limit ~1 MiB; leave headroom).
pub const ARB_TRACK_PUBLISH_MAX_PAYLOAD_BYTES: usize = 900_000;

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

/// Serialized JSON byte length for a wire payload (used for NATS max-payload budgeting).
pub fn arb_track_payload_bytes(update: &ArbTrackRequestsUpdate) -> usize {
    serde_json::to_vec(update)
        .map(|v| v.len())
        .unwrap_or(usize::MAX)
}

/// Split an update into NATS-safe chunks. `reconcile: true` is never split (caller must handle oversize).
pub fn split_arb_track_requests_update(
    update: ArbTrackRequestsUpdate,
) -> Vec<ArbTrackRequestsUpdate> {
    if update.reconcile {
        return vec![update];
    }
    if arb_track_payload_bytes(&update) <= ARB_TRACK_PUBLISH_MAX_PAYLOAD_BYTES {
        return vec![update];
    }

    let version = update.version;
    let ts_unix_ms = update.ts_unix_ms;
    let mut chunks = chunk_removed_entries(version, ts_unix_ms, update.removed);
    chunks.extend(chunk_active_entries(version, ts_unix_ms, update.active));
    chunks
}

/// Trim `active` on a reconcile snapshot until it fits the publish budget (drops tail entries).
pub fn trim_reconcile_update_to_budget(
    mut update: ArbTrackRequestsUpdate,
) -> Option<ArbTrackRequestsUpdate> {
    debug_assert!(update.reconcile);
    while arb_track_payload_bytes(&update) > ARB_TRACK_PUBLISH_MAX_PAYLOAD_BYTES
        && !update.active.is_empty()
    {
        update.active.pop();
    }
    if arb_track_payload_bytes(&update) <= ARB_TRACK_PUBLISH_MAX_PAYLOAD_BYTES {
        Some(update)
    } else {
        None
    }
}

fn chunk_removed_entries(
    version: u32,
    ts_unix_ms: u64,
    removed: Vec<ArbTrackRemovedEntry>,
) -> Vec<ArbTrackRequestsUpdate> {
    chunk_entries(removed, |removed| ArbTrackRequestsUpdate {
        version,
        ts_unix_ms,
        active: Vec::new(),
        removed,
        reconcile: false,
    })
}

fn chunk_active_entries(
    version: u32,
    ts_unix_ms: u64,
    active: Vec<ArbTrackActiveEntry>,
) -> Vec<ArbTrackRequestsUpdate> {
    chunk_entries(active, |active| ArbTrackRequestsUpdate {
        version,
        ts_unix_ms,
        active,
        removed: Vec::new(),
        reconcile: false,
    })
}

fn chunk_entries<T: Clone>(
    entries: Vec<T>,
    build: impl Fn(Vec<T>) -> ArbTrackRequestsUpdate,
) -> Vec<ArbTrackRequestsUpdate> {
    if entries.is_empty() {
        return Vec::new();
    }

    let mut chunks = Vec::new();
    let mut idx = 0;
    while idx < entries.len() {
        let remaining = entries.len() - idx;
        let mut lo = 1usize;
        let mut hi = remaining;
        let mut best = 1;
        while lo <= hi {
            let mid = lo + (hi - lo) / 2;
            let trial = build(entries[idx..idx + mid].to_vec());
            if arb_track_payload_bytes(&trial) <= ARB_TRACK_PUBLISH_MAX_PAYLOAD_BYTES {
                best = mid;
                lo = mid + 1;
            } else {
                if mid == 0 {
                    break;
                }
                hi = mid - 1;
            }
        }
        let chunk = build(entries[idx..idx + best].to_vec());
        if arb_track_payload_bytes(&chunk) > ARB_TRACK_PUBLISH_MAX_PAYLOAD_BYTES {
            // Single entry exceeds budget (unexpected for pool pubkeys); publish anyway.
            chunks.push(chunk);
            idx += 1;
        } else {
            chunks.push(chunk);
            idx += best;
        }
    }

    chunks
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_pool_id(i: usize) -> String {
        format!("Pool{i:040}")
    }

    fn large_removed_entries(count: usize) -> Vec<ArbTrackRemovedEntry> {
        (0..count)
            .map(|i| ArbTrackRemovedEntry {
                pool: sample_pool_id(i),
                reason: ArbTrackRemovedReason::Stale,
            })
            .collect()
    }

    fn large_baseline_active(count: usize) -> Vec<ArbTrackActiveEntry> {
        (0..count)
            .map(|i| ArbTrackActiveEntry {
                pool: sample_pool_id(i),
                reason: ArbTrackActiveReason::Baseline,
            })
            .collect()
    }

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

    #[test]
    fn split_large_removed_produces_multiple_chunks_under_budget() {
        let update = ArbTrackRequestsUpdate {
            version: 1,
            ts_unix_ms: 1_700_000_000,
            active: Vec::new(),
            removed: large_removed_entries(60_000),
            reconcile: false,
        };
        assert!(
            arb_track_payload_bytes(&update) > ARB_TRACK_PUBLISH_MAX_PAYLOAD_BYTES,
            "fixture must exceed budget"
        );

        let chunks = split_arb_track_requests_update(update);
        assert!(
            chunks.len() > 1,
            "expected multiple chunks, got {}",
            chunks.len()
        );
        for chunk in &chunks {
            assert!(!chunk.reconcile);
            assert!(chunk.active.is_empty());
            assert!(!chunk.removed.is_empty());
            assert!(
                arb_track_payload_bytes(chunk) <= ARB_TRACK_PUBLISH_MAX_PAYLOAD_BYTES,
                "chunk payload {} exceeds budget",
                arb_track_payload_bytes(chunk)
            );
            let json = serde_json::to_string(chunk).expect("serialize chunk");
            let back: ArbTrackRequestsUpdate =
                serde_json::from_str(&json).expect("deserialize chunk");
            assert_eq!(*chunk, back);
        }
    }

    #[test]
    fn reconcile_baseline_500_stays_single_chunk() {
        let update = ArbTrackRequestsUpdate {
            version: 1,
            ts_unix_ms: 1_700_000_000,
            active: large_baseline_active(500),
            removed: Vec::new(),
            reconcile: true,
        };
        let chunks = split_arb_track_requests_update(update.clone());
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0], update);
        assert!(
            arb_track_payload_bytes(&chunks[0]) <= ARB_TRACK_PUBLISH_MAX_PAYLOAD_BYTES,
            "baseline reconcile should fit budget"
        );
    }

    #[test]
    fn reconcile_true_never_split_even_when_incremental_would() {
        let update = ArbTrackRequestsUpdate {
            version: 1,
            ts_unix_ms: 1_700_000_000,
            active: Vec::new(),
            removed: large_removed_entries(60_000),
            reconcile: true,
        };
        let chunks = split_arb_track_requests_update(update.clone());
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0], update);
    }
}
