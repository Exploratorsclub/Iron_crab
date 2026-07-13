//! Phase 3 P3: persist/restore explicit Geyser subscription set + Tier-1 `pool_mint_map` (I-MD-6).

use super::desired_set::{ConsumerId, OwnerGroupSnapshot, OwnerKey};
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

/// On-disk snapshot format version (v2 adds complete owner groups).
pub const EXPLICIT_SET_SNAPSHOT_VERSION: u32 = 2;
/// Default production snapshot path (I-MD-6).
pub const EXPLICIT_SET_SNAPSHOT_DEFAULT_PATH: &str = "/var/lib/ironcrab/explicit_set.snapshot";
/// Periodic snapshot write interval (5 min).
pub const MARKET_DATA_EXPLICIT_SET_SNAPSHOT_INTERVAL_SECS: u64 = 300;
/// Max `pool_mint_map` entries persisted for enrichment relevance after restart.
pub const EXPLICIT_SET_SNAPSHOT_POOL_MINT_MAP_CAP: usize = 50_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SnapshotConsumer {
    Wallet,
    Momentum,
    Arb,
    Tracker,
}

impl From<ConsumerId> for SnapshotConsumer {
    fn from(c: ConsumerId) -> Self {
        match c {
            ConsumerId::Wallet => SnapshotConsumer::Wallet,
            ConsumerId::Momentum => SnapshotConsumer::Momentum,
            ConsumerId::Arb => SnapshotConsumer::Arb,
            ConsumerId::Tracker => SnapshotConsumer::Tracker,
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
    pub last_touched_gen: u64,
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
            arb_pools: Vec::new(),
        }
    }

    pub fn is_compatible(&self) -> bool {
        self.version == 1 || self.version == EXPLICIT_SET_SNAPSHOT_VERSION
    }

    pub fn explicit_pubkey_count(&self) -> usize {
        if !self.owner_groups.is_empty() {
            self.owner_groups
                .iter()
                .map(|g| g.pubkeys.len())
                .sum::<usize>()
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

pub fn owner_key_to_snapshot(owner: OwnerKey) -> SnapshotOwnerKey {
    match owner {
        OwnerKey::Wallet => SnapshotOwnerKey {
            kind: "wallet".to_string(),
            pubkey: None,
        },
        OwnerKey::Pool(pk) => SnapshotOwnerKey {
            kind: "pool".to_string(),
            pubkey: Some(pk.to_string()),
        },
        OwnerKey::Mint(pk) => SnapshotOwnerKey {
            kind: "mint".to_string(),
            pubkey: Some(pk.to_string()),
        },
    }
}

pub fn snapshot_owner_key_to_domain(key: &SnapshotOwnerKey) -> Option<OwnerKey> {
    match key.kind.as_str() {
        "wallet" => Some(OwnerKey::Wallet),
        "pool" => key
            .pubkey
            .as_deref()
            .and_then(|s| Pubkey::from_str(s).ok())
            .map(OwnerKey::Pool),
        "mint" => key
            .pubkey
            .as_deref()
            .and_then(|s| Pubkey::from_str(s).ok())
            .map(OwnerKey::Mint),
        _ => None,
    }
}

fn snapshot_owner_group_to_domain(group: &SnapshotOwnerGroup) -> Option<OwnerGroupSnapshot> {
    let consumer = match group.consumer {
        SnapshotConsumer::Wallet => ConsumerId::Wallet,
        SnapshotConsumer::Momentum => ConsumerId::Momentum,
        SnapshotConsumer::Arb => ConsumerId::Arb,
        SnapshotConsumer::Tracker => ConsumerId::Tracker,
    };
    let owner = snapshot_owner_key_to_domain(&group.owner)?;
    let pubkeys = group
        .pubkeys
        .iter()
        .filter_map(|s| Pubkey::from_str(s).ok())
        .collect::<HashSet<_>>();
    if pubkeys.is_empty() {
        return None;
    }
    Some(OwnerGroupSnapshot {
        consumer,
        owner,
        pubkeys,
        last_touched_gen: group.last_touched_gen,
    })
}

fn rows_to_owner_group_snapshots(rows: &[ExplicitSnapshotRow]) -> Vec<OwnerGroupSnapshot> {
    use std::collections::HashMap;
    let mut grouped: HashMap<(ConsumerId, OwnerKey), HashSet<Pubkey>> = HashMap::new();
    for row in rows {
        let Ok(pk) = Pubkey::from_str(&row.pubkey) else {
            continue;
        };
        let consumer = match row.consumer {
            SnapshotConsumer::Wallet => ConsumerId::Wallet,
            SnapshotConsumer::Momentum => ConsumerId::Momentum,
            SnapshotConsumer::Arb => ConsumerId::Arb,
            SnapshotConsumer::Tracker => ConsumerId::Tracker,
        };
        let pool = row.pool.as_deref().and_then(|s| Pubkey::from_str(s).ok());
        let owner = match consumer {
            ConsumerId::Wallet => OwnerKey::Wallet,
            ConsumerId::Tracker
                if pool.is_none() && matches!(row.kind, ExplicitAccountKind::Mint) =>
            {
                OwnerKey::Mint(pk)
            }
            _ => OwnerKey::Pool(pool.unwrap_or(pk)),
        };
        grouped.entry((consumer, owner)).or_default().insert(pk);
    }
    grouped
        .into_iter()
        .map(|((consumer, owner), pubkeys)| OwnerGroupSnapshot {
            consumer,
            owner,
            pubkeys,
            last_touched_gen: 0,
        })
        .collect()
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::pubkey::Pubkey;

    #[test]
    fn explicit_set_snapshot_roundtrip_json() {
        let mut snap = ExplicitSetSnapshot::new(Some("run-test".to_string()));
        snap.owner_groups.push(SnapshotOwnerGroup {
            consumer: SnapshotConsumer::Momentum,
            owner: SnapshotOwnerKey {
                kind: "pool".to_string(),
                pubkey: Some(Pubkey::new_unique().to_string()),
            },
            pubkeys: vec![Pubkey::new_unique().to_string()],
            last_touched_gen: 3,
        });
        snap.pool_mint_map.push((
            Pubkey::new_unique().to_string(),
            Pubkey::new_unique().to_string(),
        ));

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("explicit_set.snapshot");
        write_explicit_set_snapshot(&path, &snap).expect("write");
        let loaded = load_explicit_set_snapshot(&path).expect("load");
        assert_eq!(loaded, snap);
        assert!(loaded.is_compatible());
    }

    #[test]
    fn explicit_set_snapshot_v1_rows_restore_groups() {
        let mut snap = ExplicitSetSnapshot::new(None);
        snap.version = 1;
        let pool = Pubkey::new_unique();
        let mint = Pubkey::new_unique();
        snap.rows.push(ExplicitSnapshotRow {
            pubkey: mint.to_string(),
            consumer: SnapshotConsumer::Tracker,
            pool: None,
            kind: ExplicitAccountKind::Mint,
        });
        snap.rows.push(ExplicitSnapshotRow {
            pubkey: Pubkey::new_unique().to_string(),
            consumer: SnapshotConsumer::Momentum,
            pool: Some(pool.to_string()),
            kind: ExplicitAccountKind::Vault,
        });
        let groups = snap.to_owner_group_snapshots();
        assert_eq!(groups.len(), 2);
        assert!(groups.iter().any(|g| matches!(g.owner, OwnerKey::Mint(_))));
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
