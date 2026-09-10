//! Phase 3 P3: persist/restore explicit Geyser subscription set + Tier-1 `pool_mint_map` (I-MD-6).

use super::desired_set::ConsumerId;
use super::explicit_ownership::{ExplicitConsumer, ExplicitOwnerKey, OwnerGroupSnapshot};
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

/// On-disk snapshot format version (v4 excludes Tracker consumer groups from persist/restore).
pub const EXPLICIT_SET_SNAPSHOT_VERSION: u32 = 4;
/// Legacy v1 snapshots remain readable for restore.
pub const EXPLICIT_SET_SNAPSHOT_VERSION_V1: u32 = 1;
/// v2 adds complete owner groups (MomentumPosition still collapsed to Momentum).
pub const EXPLICIT_SET_SNAPSHOT_VERSION_V2: u32 = 2;
/// v3 adds `MomentumPosition` consumer + position pool list.
pub const EXPLICIT_SET_SNAPSHOT_VERSION_V3: u32 = 3;
/// Default production snapshot path (I-MD-6).
pub const EXPLICIT_SET_SNAPSHOT_DEFAULT_PATH: &str = "/var/lib/ironcrab/explicit_set.snapshot";
/// Periodic snapshot write interval (5 min).
pub const MARKET_DATA_EXPLICIT_SET_SNAPSHOT_INTERVAL_SECS: u64 = 300;
/// Max `pool_mint_map` entries persisted for enrichment relevance after restart.
pub const EXPLICIT_SET_SNAPSHOT_POOL_MINT_MAP_CAP: usize = 50_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SnapshotConsumer {
    Wallet,
    MomentumPosition,
    Momentum,
    Arb,
    Tracker,
}

impl From<ConsumerId> for SnapshotConsumer {
    fn from(c: ConsumerId) -> Self {
        match c {
            ConsumerId::Wallet => SnapshotConsumer::Wallet,
            ConsumerId::MomentumPosition => SnapshotConsumer::MomentumPosition,
            ConsumerId::Momentum => SnapshotConsumer::Momentum,
            ConsumerId::Arb => SnapshotConsumer::Arb,
            ConsumerId::Tracker => SnapshotConsumer::Tracker,
        }
    }
}

impl From<ExplicitConsumer> for SnapshotConsumer {
    fn from(c: ExplicitConsumer) -> Self {
        match c {
            ExplicitConsumer::Wallet => SnapshotConsumer::Wallet,
            ExplicitConsumer::MomentumPosition => SnapshotConsumer::MomentumPosition,
            ExplicitConsumer::Momentum => SnapshotConsumer::Momentum,
            ExplicitConsumer::Arb => SnapshotConsumer::Arb,
            ExplicitConsumer::Tracker => SnapshotConsumer::Tracker,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExplicitAccountKind {
    Mint,
    Vault,
    BinArray,
    WalletToken,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExplicitSnapshotRow {
    pub pubkey: String,
    pub consumer: SnapshotConsumer,
    pub pool: Option<String>,
    pub kind: ExplicitAccountKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotOwnerKey {
    pub kind: String,
    pub pubkey: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotOwnerGroup {
    pub consumer: SnapshotConsumer,
    pub owner: SnapshotOwnerKey,
    pub pubkeys: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExplicitSetSnapshot {
    pub version: u32,
    pub saved_at_unix: u64,
    pub run_id: Option<String>,
    pub rows: Vec<ExplicitSnapshotRow>,
    #[serde(default)]
    pub owner_groups: Vec<SnapshotOwnerGroup>,
    pub pool_mint_map: Vec<(String, String)>,
    pub momentum_pools: Vec<String>,
    #[serde(default)]
    pub momentum_position_pools: Vec<String>,
    pub arb_pools: Vec<String>,
}

impl ExplicitSetSnapshot {
    pub fn new(run_id: Option<String>) -> Self {
        Self {
            version: EXPLICIT_SET_SNAPSHOT_VERSION,
            saved_at_unix: unix_now(),
            run_id,
            rows: Vec::new(),
            owner_groups: Vec::new(),
            pool_mint_map: Vec::new(),
            momentum_pools: Vec::new(),
            momentum_position_pools: Vec::new(),
            arb_pools: Vec::new(),
        }
    }

    pub fn is_compatible(&self) -> bool {
        self.version == EXPLICIT_SET_SNAPSHOT_VERSION_V1
            || self.version == EXPLICIT_SET_SNAPSHOT_VERSION_V2
            || self.version == EXPLICIT_SET_SNAPSHOT_VERSION_V3
            || self.version == EXPLICIT_SET_SNAPSHOT_VERSION
    }

    pub fn explicit_pubkey_count(&self) -> usize {
        if !self.owner_groups.is_empty() {
            self.owner_groups.iter().map(|g| g.pubkeys.len()).sum()
        } else {
            self.rows.len()
        }
    }

    pub fn to_owner_group_snapshots(&self) -> Vec<OwnerGroupSnapshot> {
        if !self.owner_groups.is_empty() {
            return self
                .owner_groups
                .iter()
                .filter_map(snapshot_owner_group_to_domain)
                .collect();
        }
        rows_to_owner_group_snapshots(&self.rows)
    }
}

pub fn explicit_owner_key_to_snapshot(key: &ExplicitOwnerKey) -> SnapshotOwnerKey {
    match key {
        ExplicitOwnerKey::Wallet => SnapshotOwnerKey {
            kind: "wallet".to_string(),
            pubkey: None,
        },
        ExplicitOwnerKey::Pool(pk) => SnapshotOwnerKey {
            kind: "pool".to_string(),
            pubkey: Some(pk.to_string()),
        },
        ExplicitOwnerKey::Mint(pk) => SnapshotOwnerKey {
            kind: "mint".to_string(),
            pubkey: Some(pk.to_string()),
        },
        ExplicitOwnerKey::Generic(id) => SnapshotOwnerKey {
            kind: "generic".to_string(),
            pubkey: Some(id.to_string()),
        },
    }
}

pub fn snapshot_owner_key_to_domain(key: &SnapshotOwnerKey) -> Option<ExplicitOwnerKey> {
    match key.kind.as_str() {
        "wallet" => Some(ExplicitOwnerKey::Wallet),
        "pool" => key
            .pubkey
            .as_deref()
            .and_then(|s| Pubkey::from_str(s).ok())
            .map(ExplicitOwnerKey::Pool),
        "mint" => key
            .pubkey
            .as_deref()
            .and_then(|s| Pubkey::from_str(s).ok())
            .map(ExplicitOwnerKey::Mint),
        "generic" => key
            .pubkey
            .as_deref()
            .and_then(|s| s.parse::<u64>().ok())
            .map(ExplicitOwnerKey::Generic),
        _ => None,
    }
}

fn snapshot_consumer_to_domain(c: SnapshotConsumer) -> ExplicitConsumer {
    match c {
        SnapshotConsumer::Wallet => ExplicitConsumer::Wallet,
        SnapshotConsumer::MomentumPosition => ExplicitConsumer::MomentumPosition,
        SnapshotConsumer::Momentum => ExplicitConsumer::Momentum,
        SnapshotConsumer::Arb => ExplicitConsumer::Arb,
        SnapshotConsumer::Tracker => ExplicitConsumer::Tracker,
    }
}

pub fn snapshot_owner_group_to_domain(group: &SnapshotOwnerGroup) -> Option<OwnerGroupSnapshot> {
    let owner_key = snapshot_owner_key_to_domain(&group.owner)?;
    let pubkeys: Vec<Pubkey> = group
        .pubkeys
        .iter()
        .filter_map(|s| Pubkey::from_str(s).ok())
        .collect();
    if pubkeys.is_empty() {
        return None;
    }
    Some(OwnerGroupSnapshot {
        consumer: snapshot_consumer_to_domain(group.consumer),
        owner_key,
        pubkeys,
    })
}

pub fn owner_group_snapshot_to_disk(group: &OwnerGroupSnapshot) -> SnapshotOwnerGroup {
    SnapshotOwnerGroup {
        consumer: group.consumer.into(),
        owner: explicit_owner_key_to_snapshot(&group.owner_key),
        pubkeys: group.pubkeys.iter().map(|pk| pk.to_string()).collect(),
    }
}

/// Group legacy v1 rows into owner groups (pool-centric + wallet + standalone mint).
pub fn rows_to_owner_group_snapshots(rows: &[ExplicitSnapshotRow]) -> Vec<OwnerGroupSnapshot> {
    let mut groups: BTreeMap<(ExplicitConsumer, ExplicitOwnerKey), BTreeSet<Pubkey>> =
        BTreeMap::new();
    for row in rows {
        let Ok(pk) = Pubkey::from_str(&row.pubkey) else {
            continue;
        };
        let consumer = match row.consumer {
            SnapshotConsumer::Wallet => ExplicitConsumer::Wallet,
            SnapshotConsumer::MomentumPosition => ExplicitConsumer::MomentumPosition,
            SnapshotConsumer::Momentum => ExplicitConsumer::Momentum,
            SnapshotConsumer::Arb => ExplicitConsumer::Arb,
            SnapshotConsumer::Tracker => ExplicitConsumer::Tracker,
        };
        let owner_key = if consumer == ExplicitConsumer::Wallet {
            ExplicitOwnerKey::Wallet
        } else if let Some(pool_str) = row.pool.as_deref() {
            if let Ok(pool) = Pubkey::from_str(pool_str) {
                ExplicitOwnerKey::Pool(pool)
            } else {
                continue;
            }
        } else {
            ExplicitOwnerKey::Mint(pk)
        };
        groups.entry((consumer, owner_key)).or_default().insert(pk);
    }
    groups
        .into_iter()
        .map(|((consumer, owner_key), pubkeys)| OwnerGroupSnapshot {
            consumer,
            owner_key,
            pubkeys: pubkeys.into_iter().collect(),
        })
        .collect()
}

pub fn explicit_set_snapshot_path() -> PathBuf {
    std::env::var("IRONCRAB_EXPLICIT_SET_SNAPSHOT_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(EXPLICIT_SET_SNAPSHOT_DEFAULT_PATH))
}

pub fn load_explicit_set_snapshot(path: &Path) -> Option<ExplicitSetSnapshot> {
    let bytes = std::fs::read(path).ok()?;
    let snapshot: ExplicitSetSnapshot = serde_json::from_slice(&bytes).ok()?;
    if !snapshot.is_compatible() {
        return None;
    }
    Some(snapshot)
}

pub fn write_explicit_set_snapshot(
    path: &Path,
    snapshot: &ExplicitSetSnapshot,
) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("tmp");
    let json = serde_json::to_vec(snapshot)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    std::fs::write(&tmp, json)?;
    std::fs::rename(tmp, path)?;
    Ok(())
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Whether an explicit consumer may be written to or restored from disk (I-MD-6).
pub fn snapshot_consumer_is_persistable(consumer: SnapshotConsumer) -> bool {
    !matches!(consumer, SnapshotConsumer::Tracker)
}

/// Filter owner groups before snapshot persist (Wallet/Momentum/MomentumPosition/Arb only).
pub fn snapshot_owner_groups_for_persist(groups: &[SnapshotOwnerGroup]) -> Vec<SnapshotOwnerGroup> {
    groups
        .iter()
        .filter(|g| snapshot_consumer_is_persistable(g.consumer))
        .cloned()
        .collect()
}

/// Filter legacy row-based snapshots before persist.
pub fn snapshot_rows_for_persist(rows: &[ExplicitSnapshotRow]) -> Vec<ExplicitSnapshotRow> {
    rows.iter()
        .filter(|r| snapshot_consumer_is_persistable(r.consumer))
        .cloned()
        .collect()
}

/// Strip Tracker consumer groups/rows from a snapshot before restore (legacy v1–v3).
pub fn filter_tracker_consumer_from_snapshot(snapshot: ExplicitSetSnapshot) -> ExplicitSetSnapshot {
    ExplicitSetSnapshot {
        owner_groups: snapshot_owner_groups_for_persist(&snapshot.owner_groups),
        rows: snapshot_rows_for_persist(&snapshot.rows),
        ..snapshot
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_set_snapshot_v2_roundtrip_json() {
        let mut snap = ExplicitSetSnapshot::new(Some("run-test".to_string()));
        snap.owner_groups.push(SnapshotOwnerGroup {
            consumer: SnapshotConsumer::Momentum,
            owner: SnapshotOwnerKey {
                kind: "pool".to_string(),
                pubkey: Some(Pubkey::new_unique().to_string()),
            },
            pubkeys: vec![Pubkey::new_unique().to_string()],
        });

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("explicit_set.snapshot");
        write_explicit_set_snapshot(&path, &snap).expect("write");
        let loaded = load_explicit_set_snapshot(&path).expect("load");
        assert_eq!(loaded, snap);
        assert!(loaded.is_compatible());
        assert_eq!(loaded.to_owner_group_snapshots().len(), 1);
    }

    #[test]
    fn explicit_set_snapshot_v3_momentum_position_roundtrip_json() {
        let mut snap = ExplicitSetSnapshot::new(Some("run-v3".to_string()));
        let pool = Pubkey::new_unique();
        snap.owner_groups.push(SnapshotOwnerGroup {
            consumer: SnapshotConsumer::MomentumPosition,
            owner: SnapshotOwnerKey {
                kind: "pool".to_string(),
                pubkey: Some(pool.to_string()),
            },
            pubkeys: vec![Pubkey::new_unique().to_string()],
        });
        snap.momentum_position_pools.push(pool.to_string());

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("explicit_set.snapshot");
        write_explicit_set_snapshot(&path, &snap).expect("write");
        let loaded = load_explicit_set_snapshot(&path).expect("load");
        assert_eq!(loaded.version, EXPLICIT_SET_SNAPSHOT_VERSION);
        assert_eq!(loaded.momentum_position_pools, snap.momentum_position_pools);
        let groups = loaded.to_owner_group_snapshots();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].consumer, ExplicitConsumer::MomentumPosition);
    }

    #[test]
    fn explicit_set_snapshot_v1_rows_restore_owner_groups() {
        let mut snap = ExplicitSetSnapshot::new(None);
        snap.version = EXPLICIT_SET_SNAPSHOT_VERSION_V1;
        let pool = Pubkey::new_unique();
        let vault = Pubkey::new_unique();
        snap.rows.push(ExplicitSnapshotRow {
            pubkey: vault.to_string(),
            consumer: SnapshotConsumer::Momentum,
            pool: Some(pool.to_string()),
            kind: ExplicitAccountKind::Vault,
        });
        let groups = snap.to_owner_group_snapshots();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].owner_key, ExplicitOwnerKey::Pool(pool));
    }

    #[test]
    fn explicit_set_snapshot_v4_excludes_tracker_on_persist() {
        let mut snap = ExplicitSetSnapshot::new(Some("run-v4".to_string()));
        let mint = Pubkey::new_unique();
        snap.owner_groups.push(SnapshotOwnerGroup {
            consumer: SnapshotConsumer::Tracker,
            owner: SnapshotOwnerKey {
                kind: "mint".to_string(),
                pubkey: Some(mint.to_string()),
            },
            pubkeys: vec![mint.to_string()],
        });
        snap.owner_groups.push(SnapshotOwnerGroup {
            consumer: SnapshotConsumer::Wallet,
            owner: SnapshotOwnerKey {
                kind: "wallet".to_string(),
                pubkey: None,
            },
            pubkeys: vec![Pubkey::new_unique().to_string()],
        });
        snap.rows.push(ExplicitSnapshotRow {
            pubkey: mint.to_string(),
            consumer: SnapshotConsumer::Tracker,
            pool: None,
            kind: ExplicitAccountKind::Mint,
        });

        let filtered = filter_tracker_consumer_from_snapshot(snap.clone());
        assert_eq!(filtered.version, EXPLICIT_SET_SNAPSHOT_VERSION);
        assert!(filtered
            .owner_groups
            .iter()
            .all(|g| { snapshot_consumer_is_persistable(g.consumer) }));
        assert!(filtered
            .rows
            .iter()
            .all(|r| snapshot_consumer_is_persistable(r.consumer)));
        assert_eq!(filtered.owner_groups.len(), 1);
        assert_eq!(filtered.rows.len(), 0);

        let persisted_groups = snapshot_owner_groups_for_persist(&snap.owner_groups);
        assert_eq!(persisted_groups.len(), 1);
        assert_eq!(persisted_groups[0].consumer, SnapshotConsumer::Wallet);
    }

    #[test]
    fn explicit_set_snapshot_legacy_v3_tracker_stripped_on_restore_groups() {
        let mut snap = ExplicitSetSnapshot::new(None);
        snap.version = EXPLICIT_SET_SNAPSHOT_VERSION_V3;
        let tracker_mint = Pubkey::new_unique();
        snap.owner_groups.push(SnapshotOwnerGroup {
            consumer: SnapshotConsumer::Tracker,
            owner: SnapshotOwnerKey {
                kind: "mint".to_string(),
                pubkey: Some(tracker_mint.to_string()),
            },
            pubkeys: vec![tracker_mint.to_string()],
        });
        let groups = filter_tracker_consumer_from_snapshot(snap).to_owner_group_snapshots();
        assert!(groups.is_empty());
    }

    #[test]
    fn explicit_set_snapshot_rejects_unknown_version() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("explicit_set.snapshot");
        let mut snap = ExplicitSetSnapshot::new(None);
        snap.version = 99;
        write_explicit_set_snapshot(&path, &snap).expect("write");
        assert!(load_explicit_set_snapshot(&path).is_none());
    }
}
