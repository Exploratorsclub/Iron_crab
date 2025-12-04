//! Persistent SQLite cache for Orca vault reserves.
//! Reduces RPC load by caching reserve balances across service restarts.

use anyhow::Result;
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use rusqlite::{params, Connection, OptionalExtension};
use solana_sdk::pubkey::Pubkey;
use std::path::Path;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct ReserveEntry {
    pub pool_address: Pubkey,
    pub reserve_base: u128,
    pub reserve_quote: u128,
    pub cached_at: DateTime<Utc>,
}

pub struct OrcaReserveCache {
    db: Arc<Mutex<Connection>>,
    cache_ttl_secs: u64,
}

impl OrcaReserveCache {
    /// Initialize cache with given SQLite path and TTL in seconds.
    pub fn new<P: AsRef<Path>>(db_path: P, cache_ttl_secs: u64) -> Result<Self> {
        let conn = Connection::open(db_path)?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS orca_reserves (
                pool_address TEXT PRIMARY KEY,
                reserve_base INTEGER NOT NULL,
                reserve_quote INTEGER NOT NULL,
                cached_at TEXT NOT NULL
            )",
        )?;
        // Add index for queries by cached_at
        let _ = conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_orca_cached_at ON orca_reserves(cached_at)",
            [],
        );
        Ok(Self {
            db: Arc::new(Mutex::new(conn)),
            cache_ttl_secs,
        })
    }

    /// Store reserve entry in cache.
    pub fn set(&self, entry: &ReserveEntry) -> Result<()> {
        let db = self.db.lock();
        db.execute(
            "INSERT OR REPLACE INTO orca_reserves (pool_address, reserve_base, reserve_quote, cached_at)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                entry.pool_address.to_string(),
                entry.reserve_base as i64,
                entry.reserve_quote as i64,
                entry.cached_at.to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    /// Retrieve reserve entry if cached and not expired.
    pub fn get(&self, pool_address: &Pubkey) -> Result<Option<ReserveEntry>> {
        let db = self.db.lock();
        let now = Utc::now();

        let result: Option<(String, i64, i64, String)> = db
            .query_row(
                "SELECT pool_address, reserve_base, reserve_quote, cached_at 
                 FROM orca_reserves WHERE pool_address = ?1",
                params![pool_address.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?;

        if let Some((addr_str, base, quote, cached_at_str)) = result {
            let cached_at = DateTime::parse_from_rfc3339(&cached_at_str)
                .ok()
                .map(|dt| dt.with_timezone(&Utc))
                .ok_or_else(|| anyhow::anyhow!("failed to parse cached_at timestamp"))?;

            let elapsed = now.signed_duration_since(cached_at);
            if elapsed.num_seconds() < self.cache_ttl_secs as i64 {
                return Ok(Some(ReserveEntry {
                    pool_address: addr_str.parse()?,
                    reserve_base: base as u128,
                    reserve_quote: quote as u128,
                    cached_at,
                }));
            }
        }
        Ok(None)
    }

    /// Get all pools sorted by recency (for prefetching).
    pub fn get_recent_pools(&self, limit: usize) -> Result<Vec<(Pubkey, DateTime<Utc>)>> {
        let db = self.db.lock();
        let mut stmt = db.prepare(
            "SELECT pool_address, cached_at FROM orca_reserves 
             ORDER BY cached_at DESC LIMIT ?1",
        )?;
        let pools = stmt
            .query_map(params![limit as i32], |row| {
                Ok((
                    row.get::<_, String>(0)?.parse::<Pubkey>().ok(),
                    row.get::<_, String>(1)?,
                ))
            })?
            .filter_map(|r| r.ok())
            .filter_map(|(addr_opt, time_str)| {
                addr_opt.and_then(|addr| {
                    DateTime::parse_from_rfc3339(&time_str)
                        .ok()
                        .map(|dt| (addr, dt.with_timezone(&Utc)))
                })
            })
            .collect();
        Ok(pools)
    }

    /// Clear expired entries (older than cache_ttl_secs).
    pub fn cleanup_expired(&self) -> Result<usize> {
        let db = self.db.lock();
        let cutoff = Utc::now()
            .checked_sub_signed(chrono::Duration::seconds(self.cache_ttl_secs as i64))
            .unwrap();
        let deleted = db.execute(
            "DELETE FROM orca_reserves WHERE cached_at < ?1",
            params![cutoff.to_rfc3339()],
        )?;
        Ok(deleted)
    }

    /// Statistics: count of cached entries.
    pub fn stats(&self) -> Result<(usize, DateTime<Utc>)> {
        let db = self.db.lock();
        let count: usize =
            db.query_row("SELECT COUNT(*) FROM orca_reserves", [], |row| row.get(0))?;
        let oldest: Option<String> = db
            .query_row("SELECT MIN(cached_at) FROM orca_reserves", [], |row| {
                row.get(0)
            })
            .optional()?
            .flatten();

        let oldest_time = oldest
            .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(Utc::now);

        Ok((count, oldest_time))
    }
}
