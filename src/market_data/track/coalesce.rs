//! Phase 5a: NATS burst coalescers for momentum and arb track-worker enqueue.
//! Queue-full paths preserve demand via bounded pending replay (I-MD-5).

use super::worker::{
    track_worker_try_enqueue, TrackWorkerCommand, TrackWorkerContext, TrackWorkerSender,
};
use crate::metrics::{
    inc_market_data_arb_track_coalesced_batches_total,
    inc_market_data_arb_track_coalesced_messages_total,
    inc_market_data_arb_track_worker_enqueue_dropped_total,
    inc_market_data_momentum_coalesced_batches_total,
    inc_market_data_momentum_coalesced_messages_total,
    inc_market_data_momentum_track_worker_enqueue_dropped_total,
};
use crate::nats::{
    ArbTrackActiveEntry, ArbTrackRemovedEntry, ArbTrackRequestsUpdate, MomentumActivePinReason,
    MomentumActivePoolEntry, MomentumActivePoolsUpdate, MomentumRemovedPoolEntry,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::warn;

/// PR169c: bounded channel for momentum updates before actor coalesce.
pub const MARKET_DATA_MOMENTUM_COALESCE_CHANNEL_CAP: usize = 512;
/// Phase 3: bounded channel for arb track updates before track-worker coalesce.
pub const MARKET_DATA_ARB_COALESCE_CHANNEL_CAP: usize = 512;

type MomentumPoolKey = (String, String);

/// When coalescing bursts, Position pins must not be downgraded to Tracker for the same `(mint, pool)`.
fn merge_momentum_active_pin_reason(
    existing: MomentumActivePinReason,
    incoming: MomentumActivePinReason,
) -> MomentumActivePinReason {
    if existing == MomentumActivePinReason::Position
        || incoming == MomentumActivePinReason::Position
    {
        MomentumActivePinReason::Position
    } else {
        MomentumActivePinReason::Tracker
    }
}

/// PR169c: merge a burst of momentum updates into one payload equivalent to sequential applies.
pub fn merge_momentum_active_pools_updates(
    updates: &[MomentumActivePoolsUpdate],
) -> Option<MomentumActivePoolsUpdate> {
    if updates.is_empty() {
        return None;
    }
    let mut active_map: HashMap<MomentumPoolKey, MomentumActivePoolEntry> = HashMap::new();
    let mut removed_map: HashMap<MomentumPoolKey, MomentumRemovedPoolEntry> = HashMap::new();
    let mut saw_full_snapshot = false;
    let mut max_version = 0u32;
    let mut max_ts = 0u64;

    for update in updates {
        max_version = max_version.max(update.version);
        max_ts = max_ts.max(update.ts_unix_ms);
        if update.full_active_snapshot {
            saw_full_snapshot = true;
            let target: HashSet<MomentumPoolKey> = update
                .active
                .iter()
                .map(|a| (a.mint.clone(), a.pool.clone()))
                .collect();
            for key in active_map.keys().cloned().collect::<Vec<_>>() {
                if !target.contains(&key) {
                    active_map.remove(&key);
                    removed_map.remove(&key);
                }
            }
            active_map.clear();
            for r in &update.removed {
                let key = (r.mint.clone(), r.pool.clone());
                active_map.remove(&key);
                removed_map.insert(key, r.clone());
            }
            for a in &update.active {
                let key = (a.mint.clone(), a.pool.clone());
                removed_map.remove(&key);
                match active_map.get(&key) {
                    Some(existing) => {
                        let merged = MomentumActivePoolEntry {
                            pin_reason: merge_momentum_active_pin_reason(
                                existing.pin_reason,
                                a.pin_reason,
                            ),
                            ..a.clone()
                        };
                        active_map.insert(key, merged);
                    }
                    None => {
                        active_map.insert(key, a.clone());
                    }
                }
            }
        } else {
            for r in &update.removed {
                let key = (r.mint.clone(), r.pool.clone());
                active_map.remove(&key);
                removed_map.insert(key, r.clone());
            }
            for a in &update.active {
                let key = (a.mint.clone(), a.pool.clone());
                removed_map.remove(&key);
                match active_map.get(&key) {
                    Some(existing) => {
                        let merged = MomentumActivePoolEntry {
                            pin_reason: merge_momentum_active_pin_reason(
                                existing.pin_reason,
                                a.pin_reason,
                            ),
                            ..a.clone()
                        };
                        active_map.insert(key, merged);
                    }
                    None => {
                        active_map.insert(key, a.clone());
                    }
                }
            }
        }
    }

    for key in active_map.keys().cloned().collect::<Vec<_>>() {
        removed_map.remove(&key);
    }

    Some(MomentumActivePoolsUpdate {
        version: max_version,
        ts_unix_ms: max_ts,
        active: active_map.into_values().collect(),
        removed: removed_map.into_values().collect(),
        full_active_snapshot: saw_full_snapshot,
    })
}

/// Phase 3: merge a burst of arb track updates into one payload equivalent to sequential applies.
pub fn merge_arb_track_requests_updates(
    updates: &[ArbTrackRequestsUpdate],
) -> Option<ArbTrackRequestsUpdate> {
    if updates.is_empty() {
        return None;
    }
    let mut active_map: HashMap<String, ArbTrackActiveEntry> = HashMap::new();
    let mut removed_map: HashMap<String, ArbTrackRemovedEntry> = HashMap::new();
    let mut saw_reconcile = false;
    let mut max_version = 0u32;
    let mut max_ts = 0u64;

    for update in updates {
        max_version = max_version.max(update.version);
        max_ts = max_ts.max(update.ts_unix_ms);
        if update.reconcile {
            saw_reconcile = true;
            let target: HashSet<String> = update.active.iter().map(|a| a.pool.clone()).collect();
            for key in active_map.keys().cloned().collect::<Vec<_>>() {
                if !target.contains(&key) {
                    active_map.remove(&key);
                    removed_map.remove(&key);
                }
            }
            active_map.clear();
            for r in &update.removed {
                let key = r.pool.clone();
                active_map.remove(&key);
                removed_map.insert(key, r.clone());
            }
            for a in &update.active {
                let key = a.pool.clone();
                removed_map.remove(&key);
                active_map.insert(key, a.clone());
            }
        } else {
            for r in &update.removed {
                let key = r.pool.clone();
                active_map.remove(&key);
                removed_map.insert(key, r.clone());
            }
            for a in &update.active {
                let key = a.pool.clone();
                removed_map.remove(&key);
                active_map.insert(key, a.clone());
            }
        }
    }

    for key in active_map.keys().cloned().collect::<Vec<_>>() {
        removed_map.remove(&key);
    }

    Some(ArbTrackRequestsUpdate {
        version: max_version,
        ts_unix_ms: max_ts,
        active: active_map.into_values().collect(),
        removed: removed_map.into_values().collect(),
        reconcile: saw_reconcile,
    })
}

pub fn momentum_coalesce_try_send(
    tx: &mpsc::Sender<MomentumActivePoolsUpdate>,
    update: MomentumActivePoolsUpdate,
) {
    inc_market_data_momentum_coalesced_messages_total();
    if tx.try_send(update).is_err() {
        warn!("momentum coalesce channel full; dropping MomentumActivePoolsUpdate");
    }
}

pub fn spawn_momentum_tracking_coalescer<C: TrackWorkerContext + 'static>(
    ctx: Arc<C>,
    track_worker: TrackWorkerSender,
) -> mpsc::Sender<MomentumActivePoolsUpdate> {
    let (coalesce_tx, mut coalesce_rx) =
        mpsc::channel::<MomentumActivePoolsUpdate>(MARKET_DATA_MOMENTUM_COALESCE_CHANNEL_CAP);
    tokio::spawn(async move {
        while let Some(first) = coalesce_rx.recv().await {
            let mut pending = vec![first];
            while let Ok(more) = coalesce_rx.try_recv() {
                pending.push(more);
            }
            let debounce_ms = ctx.geyser_sync_batch_debounce_ms();
            tokio::time::sleep(Duration::from_millis(debounce_ms)).await;
            while let Ok(more) = coalesce_rx.try_recv() {
                pending.push(more);
            }
            if let Some(merged) = merge_momentum_active_pools_updates(&pending) {
                inc_market_data_momentum_coalesced_batches_total();
                if !track_worker_try_enqueue(
                    &track_worker,
                    TrackWorkerCommand::ApplyMomentumActivePools(merged),
                ) {
                    inc_market_data_momentum_track_worker_enqueue_dropped_total();
                }
            }
            pending.clear();
        }
    });
    coalesce_tx
}

pub fn arb_coalesce_try_send(
    tx: &mpsc::Sender<ArbTrackRequestsUpdate>,
    update: ArbTrackRequestsUpdate,
) {
    inc_market_data_arb_track_coalesced_messages_total();
    if tx.try_send(update).is_err() {
        warn!("arb track coalesce channel full; dropping ArbTrackRequestsUpdate");
    }
}

pub fn spawn_arb_tracking_coalescer<C: TrackWorkerContext + 'static>(
    ctx: Arc<C>,
    track_worker: TrackWorkerSender,
) -> mpsc::Sender<ArbTrackRequestsUpdate> {
    let (coalesce_tx, mut coalesce_rx) =
        mpsc::channel::<ArbTrackRequestsUpdate>(MARKET_DATA_ARB_COALESCE_CHANNEL_CAP);
    tokio::spawn(async move {
        while let Some(first) = coalesce_rx.recv().await {
            let mut pending = vec![first];
            while let Ok(more) = coalesce_rx.try_recv() {
                pending.push(more);
            }
            let debounce_ms = ctx.geyser_sync_batch_debounce_ms();
            tokio::time::sleep(Duration::from_millis(debounce_ms)).await;
            while let Ok(more) = coalesce_rx.try_recv() {
                pending.push(more);
            }
            if let Some(merged) = merge_arb_track_requests_updates(&pending) {
                inc_market_data_arb_track_coalesced_batches_total();
                if !track_worker_try_enqueue(
                    &track_worker,
                    TrackWorkerCommand::ApplyArbTrackRequests(merged),
                ) {
                    inc_market_data_arb_track_worker_enqueue_dropped_total();
                }
            }
            pending.clear();
        }
    });
    coalesce_tx
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_momentum_active_pools_prefers_position_over_tracker() {
        let merged = merge_momentum_active_pools_updates(&[
            MomentumActivePoolsUpdate {
                version: 1,
                ts_unix_ms: 1,
                active: vec![MomentumActivePoolEntry {
                    mint: "m".into(),
                    pool: "p".into(),
                    pin_reason: MomentumActivePinReason::Tracker,
                }],
                removed: vec![],
                full_active_snapshot: false,
            },
            MomentumActivePoolsUpdate {
                version: 1,
                ts_unix_ms: 2,
                active: vec![MomentumActivePoolEntry {
                    mint: "m".into(),
                    pool: "p".into(),
                    pin_reason: MomentumActivePinReason::Position,
                }],
                removed: vec![],
                full_active_snapshot: false,
            },
        ])
        .expect("merged");
        assert_eq!(merged.active.len(), 1);
        assert_eq!(
            merged.active[0].pin_reason,
            MomentumActivePinReason::Position
        );
    }
}
