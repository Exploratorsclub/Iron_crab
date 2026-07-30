//! JetStream KV publish helpers for PositionAuthority (PA-5.1 / PA-6b).
//!
//! Shared by `position-manager` (sole writer after PA-6b) and `execution-engine`
//! (optional rollback via `publish_position_authority_kv`).

use parking_lot::Mutex as ParkingMutex;
use std::collections::HashSet;
use tracing::{debug, info, warn};

use crate::ipc::schema::{
    MarketEventKind, PositionAuthoritySnapshot, POSITION_AUTHORITY_KV_BUCKET,
};
use crate::metrics::{
    record_position_manager_kv_publish_error, record_position_manager_kv_put,
    record_position_manager_kv_tombstone,
};
use crate::nats::NatsClient;
use crate::position_authority::{PositionAuthority, PositionAuthorityChange};

/// Who records Prometheus counters for KV publish operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PositionAuthorityKvMetricsSink {
    #[default]
    None,
    PositionManager,
}

/// FIFO worker for ordered PositionAuthority KV publishes (preserves reducer apply order).
pub struct PositionAuthorityKvPublisher {
    tx: Option<tokio::sync::mpsc::UnboundedSender<Vec<PositionAuthorityChange>>>,
}

impl PositionAuthorityKvPublisher {
    pub fn disabled() -> Self {
        Self { tx: None }
    }

    pub fn spawn(nats: NatsClient, metrics_sink: PositionAuthorityKvMetricsSink) -> Self {
        let kv_cell = tokio::sync::OnceCell::new();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Some(changes) = rx.recv().await {
                if let Err(e) =
                    publish_position_authority_changes_to_kv(&nats, &kv_cell, changes, metrics_sink)
                        .await
                {
                    warn!(error = %e, "PositionAuthority KV publish failed");
                    if metrics_sink == PositionAuthorityKvMetricsSink::PositionManager {
                        record_position_manager_kv_publish_error();
                    }
                }
            }
        });
        Self { tx: Some(tx) }
    }

    pub fn enqueue(&self, changes: &[PositionAuthorityChange]) {
        if changes.is_empty() {
            return;
        }
        if let Some(tx) = &self.tx {
            let _ = tx.send(changes.to_vec());
        }
    }
}

/// PA-5.1: write PositionAuthority deltas to JetStream KV (`POSITION_AUTHORITY` bucket).
pub async fn publish_position_authority_changes_to_kv(
    nats: &NatsClient,
    kv_cell: &tokio::sync::OnceCell<async_nats::jetstream::kv::Store>,
    changes: Vec<PositionAuthorityChange>,
    metrics_sink: PositionAuthorityKvMetricsSink,
) -> anyhow::Result<()> {
    if changes.is_empty() {
        return Ok(());
    }
    let store = if let Some(store) = kv_cell.get() {
        store.clone()
    } else {
        let store = nats
            .get_or_create_kv_bucket(POSITION_AUTHORITY_KV_BUCKET)
            .await?;
        let _ = kv_cell.set(store.clone());
        store
    };
    for change in changes {
        match change {
            PositionAuthorityChange::Put(snapshot) => {
                let mint = snapshot.mint.clone();
                nats.kv_put(&store, &mint, &snapshot).await?;
                if metrics_sink == PositionAuthorityKvMetricsSink::PositionManager {
                    record_position_manager_kv_put();
                }
            }
            PositionAuthorityChange::Tombstone { mint } => {
                if let Err(e) = nats.kv_delete(&store, &mint).await {
                    debug!(mint = %mint, error = %e, "PositionAuthority KV tombstone delete (may not exist)");
                } else if metrics_sink == PositionAuthorityKvMetricsSink::PositionManager {
                    record_position_manager_kv_tombstone();
                }
            }
        }
    }
    Ok(())
}

/// PA-5.1: tombstone sweep only when bootstrap included `WalletSnapshotComplete`
/// (partial balance snapshots alone are not a complete wallet picture).
pub fn wallet_bootstrap_allows_pa_kv_tombstone_sweep(
    wallet_snapshot_kinds: &[MarketEventKind],
) -> bool {
    wallet_snapshot_kinds
        .iter()
        .any(|k| matches!(k, MarketEventKind::WalletSnapshotComplete { .. }))
}

/// PA-5.1: after restart, seed PositionAuthority from wallet bootstrap and
/// tombstone JetStream KV keys that no longer exist in the authority model.
pub async fn reconcile_position_authority_kv_after_restart(
    nats: &NatsClient,
    position_authority: &ParkingMutex<PositionAuthority>,
    kv_cell: &tokio::sync::OnceCell<async_nats::jetstream::kv::Store>,
    wallet_snapshot_kinds: &[MarketEventKind],
    metrics_sink: PositionAuthorityKvMetricsSink,
) -> anyhow::Result<()> {
    let (changes, tracked) = {
        let mut pa = position_authority.lock();
        let mut changes = Vec::new();
        for kind in wallet_snapshot_kinds {
            changes.extend(pa.apply_from_wallet_market_event_kind(kind));
        }
        let tracked: HashSet<String> = pa.tracked_mints().into_iter().collect();
        (changes, tracked)
    };

    let mut changes = changes;

    let store = if let Some(store) = kv_cell.get() {
        store.clone()
    } else {
        let store = nats
            .get_or_create_kv_bucket(POSITION_AUTHORITY_KV_BUCKET)
            .await?;
        let _ = kv_cell.set(store.clone());
        store
    };

    let allow_tombstone_sweep =
        wallet_bootstrap_allows_pa_kv_tombstone_sweep(wallet_snapshot_kinds);

    if allow_tombstone_sweep {
        let kv_entries = nats
            .kv_get_all::<PositionAuthoritySnapshot>(&store)
            .await
            .unwrap_or_default();
        for mint in kv_entries.keys() {
            if !tracked.contains(mint) {
                changes.push(PositionAuthorityChange::Tombstone { mint: mint.clone() });
            }
        }
        if !changes.is_empty() {
            publish_position_authority_changes_to_kv(nats, kv_cell, changes, metrics_sink).await?;
            info!(
                wallet_snapshots = wallet_snapshot_kinds.len(),
                kv_keys = kv_entries.len(),
                tracked_mints = tracked.len(),
                "PositionAuthority KV reconciled after restart (tombstone sweep)"
            );
        }
    } else {
        if !changes.is_empty() {
            publish_position_authority_changes_to_kv(nats, kv_cell, changes, metrics_sink).await?;
        }
        warn!(
            wallet_snapshots = wallet_snapshot_kinds.len(),
            has_snapshot_complete = false,
            tracked_mints = tracked.len(),
            "PositionAuthority KV reconcile: tombstone sweep skipped (wallet bootstrap missing WalletSnapshotComplete or empty)"
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ipc::schema::PositionAuthorityStatus;

    fn sample_put(mint: &str) -> PositionAuthorityChange {
        PositionAuthorityChange::Put(PositionAuthoritySnapshot {
            mint: mint.to_string(),
            balance_raw: 100,
            decimals: 6,
            status: PositionAuthorityStatus::Open,
            last_update_source: crate::ipc::schema::PositionAuthorityUpdateSource::Execution,
            sold_raw_total: None,
        })
    }

    #[test]
    fn put_and_tombstone_change_variants_distinct() {
        let put = sample_put("mintA");
        let tombstone = PositionAuthorityChange::Tombstone {
            mint: "mintA".to_string(),
        };
        assert!(matches!(put, PositionAuthorityChange::Put(_)));
        assert!(matches!(
            tombstone,
            PositionAuthorityChange::Tombstone { .. }
        ));
    }

    #[test]
    fn wallet_bootstrap_tombstone_sweep_requires_snapshot_complete() {
        let balance_only = vec![MarketEventKind::WalletBalanceSnapshot {
            mint: "mintA".to_string(),
            balance_raw: 1,
            decimals: 6,
            token_program: "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_string(),
        }];
        assert!(
            !wallet_bootstrap_allows_pa_kv_tombstone_sweep(&balance_only),
            "partial balance snapshots must not authorize tombstone sweep"
        );
        assert!(
            !wallet_bootstrap_allows_pa_kv_tombstone_sweep(&[]),
            "empty bootstrap must not authorize tombstone sweep"
        );
        let with_complete = vec![MarketEventKind::WalletSnapshotComplete {
            wallet: "wallet".to_string(),
            mints_in_wallet: vec!["mintA".to_string()],
            is_periodic: true,
        }];
        assert!(
            wallet_bootstrap_allows_pa_kv_tombstone_sweep(&with_complete),
            "WalletSnapshotComplete authorizes tombstone sweep"
        );
    }

    #[test]
    fn kv_publisher_enqueue_noop_when_disabled() {
        let publisher = PositionAuthorityKvPublisher::disabled();
        publisher.enqueue(&[sample_put("mintA")]);
    }
}
