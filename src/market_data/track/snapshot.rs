//! Phase 3 P3: persist/restore explicit Geyser subscription set + Tier-1 `pool_mint_map` (I-MD-6).

use super::desired_set::ConsumerId;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// On-disk snapshot format version.
pub const EXPLICIT_SET_SNAPSHOT_VERSION: u32 = 1;
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
pub struct ExplicitSetSnapshot {
    pub version: u32,
    pub saved_at_unix: u64,
    pub run_id: Option<String>,
    pub rows: Vec<ExplicitSnapshotRow>,
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
            pool_mint_map: Vec::new(),
            momentum_pools: Vec::new(),
            arb_pools: Vec::new(),
        }
    }

    pub fn is_compatible(&self) -> bool {
        self.version == EXPLICIT_SET_SNAPSHOT_VERSION
    }

    pub fn explicit_pubkey_count(&self) -> usize {
        self.rows.len()
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
        snap.rows.push(ExplicitSnapshotRow {
            pubkey: Pubkey::new_unique().to_string(),
            consumer: SnapshotConsumer::Momentum,
            pool: Some(Pubkey::new_unique().to_string()),
            kind: ExplicitAccountKind::Vault,
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
    fn explicit_set_snapshot_rejects_unknown_version() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("explicit_set.snapshot");
        let mut snap = ExplicitSetSnapshot::new(None);
        snap.version = 99;
        write_explicit_set_snapshot(&path, &snap).expect("write");
        assert!(load_explicit_set_snapshot(&path).is_none());
    }
}
