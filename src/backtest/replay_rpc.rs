//! Minimal RPC facade for deterministic replays backed by ReplayStore.
use std::collections::HashMap;
use std::sync::Arc;
use std::str::FromStr;
use solana_sdk::account::Account;
use solana_sdk::pubkey::Pubkey;

use super::replay::ReplayStore;

#[derive(Clone)]
pub struct ReplayRpc {
    store: Arc<ReplayStore>,
    // Cache the latest bytes per key for quick get_account
    latest: Arc<HashMap<String, Vec<u8>>>,
}

impl ReplayRpc {
    pub fn new(store: Arc<ReplayStore>) -> Self {
        let mut latest = HashMap::new();
        for (k, updates) in &store.accounts {
            if let Some(last) = updates.last() { latest.insert(k.clone(), last.clone()); }
        }
        Self { store, latest: Arc::new(latest) }
    }

    pub fn get_account(&self, key: &Pubkey) -> Option<Account> {
        let k = key.to_string();
        let bytes = self.latest.get(&k)?.clone();
        Some(Account { lamports: 0, data: bytes, owner: Pubkey::default(), executable: false, rent_epoch: 0 })
    }

    pub fn get_multiple_accounts(&self, keys: &[Pubkey]) -> Vec<Option<Account>> {
        keys.iter().map(|k| self.get_account(k)).collect()
    }

    /// Logs in the slot range (inclusive)
    pub fn logs_in_range(&self, start_slot: u64, end_slot: u64) -> Vec<(u64, String)> {
        self.store
            .logs
            .iter()
            .filter(|(s, _)| *s >= start_slot && *s <= end_slot)
            .cloned()
            .collect()
    }

    /// Convenience: get account by base58 string key (returns None if invalid or missing)
    pub fn get_account_str(&self, key_str: &str) -> Option<Account> {
        let key = Pubkey::from_str(key_str).ok()?;
        self.get_account(&key)
    }

    /// Return a cloned vector of (pubkey_string, latest_bytes) for all known accounts.
    pub fn all_latest(&self) -> Vec<(String, Vec<u8>)> {
        self.latest.iter().map(|(k,v)| (k.clone(), v.clone())).collect()
    }
}
