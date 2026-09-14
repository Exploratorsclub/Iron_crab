//! Unpinned pool layout keys (TX-derived addresses). Not quote SSOT; not Geyser subscriptions.
//!
//! Promoted to MASTER [`LivePoolCache`] on pin; demoted back on unpin when no pins remain.

use std::collections::HashMap;

use solana_sdk::pubkey::Pubkey;

use super::live_pool_cache::{
    CachedPoolState, MeteoraState, OrcaWhirlpoolState, PumpAmmState, RaydiumAmmState,
    RaydiumCpmmState,
};

pub const DEFAULT_TTL_MS: u64 = 120_000;
pub const DEFAULT_CAP: usize = 32_768;

/// Address/layout fields only — no reserve balances or quote numerics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PoolLayoutKeys {
    PumpAmm {
        base_mint: Pubkey,
        quote_mint: Pubkey,
        pool_base_token_account: Pubkey,
        pool_quote_token_account: Pubkey,
        pool_accounts: Vec<Pubkey>,
    },
    Orca {
        token_mint_a: Pubkey,
        token_mint_b: Pubkey,
        token_vault_a: Pubkey,
        token_vault_b: Pubkey,
    },
    RaydiumCpmm {
        token_0_mint: Pubkey,
        token_1_mint: Pubkey,
        token_0_vault: Pubkey,
        token_1_vault: Pubkey,
    },
    Meteora {
        token_x_mint: Pubkey,
        token_y_mint: Pubkey,
        reserve_x: Pubkey,
        reserve_y: Pubkey,
    },
    RaydiumAmm {
        base_mint: Pubkey,
        quote_mint: Pubkey,
        coin_vault: Pubkey,
        pc_vault: Pubkey,
    },
}

#[derive(Debug, Clone)]
struct BookEntry {
    keys: PoolLayoutKeys,
    last_seen_ms: u64,
    lru_stamp: u64,
}

/// In-memory unpinned TX layout store (TTL + LRU cap). Single-writer (md-state) assumed.
#[derive(Debug, Default)]
pub struct PoolAddressBook {
    entries: HashMap<Pubkey, BookEntry>,
    ttl_ms: u64,
    cap: usize,
    lru_clock: u64,
}

impl PoolAddressBook {
    pub fn new() -> Self {
        Self::with_ttl_and_cap(DEFAULT_TTL_MS, DEFAULT_CAP)
    }

    pub fn with_ttl_and_cap(ttl_ms: u64, cap: usize) -> Self {
        Self {
            entries: HashMap::new(),
            ttl_ms,
            cap,
            lru_clock: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn contains(&self, pool: &Pubkey) -> bool {
        self.entries.contains_key(pool)
    }

    fn next_lru_stamp(&mut self) -> u64 {
        self.lru_clock += 1;
        self.lru_clock
    }

    pub fn evict_stale(&mut self, now_ms: u64) -> usize {
        let ttl = self.ttl_ms;
        let before = self.entries.len();
        self.entries.retain(|pool, e| {
            pump_amm_v14_pin_payload(pool, &e.keys) || now_ms.saturating_sub(e.last_seen_ms) <= ttl
        });
        before - self.entries.len()
    }

    fn evict_over_cap(&mut self) -> usize {
        if self.entries.len() <= self.cap {
            return 0;
        }
        let to_remove = self.entries.len() - self.cap;
        let mut by_evict_priority: Vec<(Pubkey, u64, bool)> = self
            .entries
            .iter()
            .map(|(p, e)| {
                let v14 = pump_amm_v14_pin_payload(p, &e.keys);
                (*p, e.lru_stamp, v14)
            })
            .collect();
        // Drop incomplete book rows before Pump v14 pin-payload; then oldest LRU.
        by_evict_priority.sort_by(|a, b| {
            a.2.cmp(&b.2).then_with(|| a.1.cmp(&b.1))
        });
        for (pool, _, _) in by_evict_priority.into_iter().take(to_remove) {
            self.entries.remove(&pool);
        }
        to_remove
    }

    pub fn merge(&mut self, pool: Pubkey, incoming: PoolLayoutKeys, now_ms: u64) {
        self.evict_stale(now_ms);
        let merged = match self.entries.get(&pool) {
            Some(existing) => merge_layout_keys(&existing.keys, &incoming),
            None => incoming,
        };
        let stamp = self.next_lru_stamp();
        self.entries.insert(
            pool,
            BookEntry {
                keys: merged,
                last_seen_ms: now_ms,
                lru_stamp: stamp,
            },
        );
        self.evict_over_cap();
    }

    /// Remove and return layout keys for pin promotion.
    pub fn take(&mut self, pool: Pubkey) -> Option<PoolLayoutKeys> {
        self.entries.remove(&pool).map(|e| e.keys)
    }

    /// Re-insert after unpin demote (merge with any leftover book row).
    pub fn insert_from_demote(&mut self, pool: Pubkey, keys: PoolLayoutKeys, now_ms: u64) {
        self.merge(pool, keys, now_ms);
    }

    pub fn layout_keys_for_pool(&self, pool: &Pubkey) -> Option<&PoolLayoutKeys> {
        self.entries.get(pool).map(|e| &e.keys)
    }
}

fn non_default(pk: Pubkey) -> Option<Pubkey> {
    (pk != Pubkey::default()).then_some(pk)
}

/// Pump AMM layer C (v14 instruction accounts) ready for pin promotion — not subject to book TTL.
fn pump_amm_v14_pin_payload(pool: &Pubkey, keys: &PoolLayoutKeys) -> bool {
    match keys {
        PoolLayoutKeys::PumpAmm { pool_accounts, .. } => {
            pool_accounts.len() >= 14 && pool_accounts.first() == Some(pool)
        }
        _ => false,
    }
}

fn merge_layout_keys(existing: &PoolLayoutKeys, incoming: &PoolLayoutKeys) -> PoolLayoutKeys {
    match (existing, incoming) {
        (
            PoolLayoutKeys::PumpAmm {
                base_mint: ex_bm,
                quote_mint: ex_qm,
                pool_base_token_account: ex_b,
                pool_quote_token_account: ex_q,
                pool_accounts: ex_pa,
            },
            PoolLayoutKeys::PumpAmm {
                base_mint: in_bm,
                quote_mint: in_qm,
                pool_base_token_account: in_b,
                pool_quote_token_account: in_q,
                pool_accounts: in_pa,
            },
        ) => PoolLayoutKeys::PumpAmm {
            base_mint: if *ex_bm == Pubkey::default() {
                *in_bm
            } else {
                *ex_bm
            },
            quote_mint: if *ex_qm == Pubkey::default() {
                *in_qm
            } else {
                *ex_qm
            },
            pool_base_token_account: if *ex_b == Pubkey::default() {
                *in_b
            } else {
                *ex_b
            },
            pool_quote_token_account: if *ex_q == Pubkey::default() {
                *in_q
            } else {
                *ex_q
            },
            pool_accounts: if ex_pa.is_empty() && !in_pa.is_empty() {
                in_pa.clone()
            } else {
                ex_pa.clone()
            },
        },
        (
            PoolLayoutKeys::Orca {
                token_mint_a: ex_ma,
                token_mint_b: ex_mb,
                token_vault_a: ex_va,
                token_vault_b: ex_vb,
            },
            PoolLayoutKeys::Orca {
                token_mint_a: in_ma,
                token_mint_b: in_mb,
                token_vault_a: in_va,
                token_vault_b: in_vb,
            },
        ) => PoolLayoutKeys::Orca {
            token_mint_a: if *ex_ma == Pubkey::default() {
                *in_ma
            } else {
                *ex_ma
            },
            token_mint_b: if *ex_mb == Pubkey::default() {
                *in_mb
            } else {
                *ex_mb
            },
            token_vault_a: if *ex_va == Pubkey::default() {
                *in_va
            } else {
                *ex_va
            },
            token_vault_b: if *ex_vb == Pubkey::default() {
                *in_vb
            } else {
                *ex_vb
            },
        },
        (
            PoolLayoutKeys::RaydiumCpmm {
                token_0_mint: ex_m0,
                token_1_mint: ex_m1,
                token_0_vault: ex_v0,
                token_1_vault: ex_v1,
            },
            PoolLayoutKeys::RaydiumCpmm {
                token_0_mint: in_m0,
                token_1_mint: in_m1,
                token_0_vault: in_v0,
                token_1_vault: in_v1,
            },
        ) => PoolLayoutKeys::RaydiumCpmm {
            token_0_mint: if *ex_m0 == Pubkey::default() {
                *in_m0
            } else {
                *ex_m0
            },
            token_1_mint: if *ex_m1 == Pubkey::default() {
                *in_m1
            } else {
                *ex_m1
            },
            token_0_vault: if *ex_v0 == Pubkey::default() {
                *in_v0
            } else {
                *ex_v0
            },
            token_1_vault: if *ex_v1 == Pubkey::default() {
                *in_v1
            } else {
                *ex_v1
            },
        },
        (
            PoolLayoutKeys::Meteora {
                token_x_mint: ex_mx,
                token_y_mint: ex_my,
                reserve_x: ex_rx,
                reserve_y: ex_ry,
            },
            PoolLayoutKeys::Meteora {
                token_x_mint: in_mx,
                token_y_mint: in_my,
                reserve_x: in_rx,
                reserve_y: in_ry,
            },
        ) => PoolLayoutKeys::Meteora {
            token_x_mint: if *ex_mx == Pubkey::default() {
                *in_mx
            } else {
                *ex_mx
            },
            token_y_mint: if *ex_my == Pubkey::default() {
                *in_my
            } else {
                *ex_my
            },
            reserve_x: if *ex_rx == Pubkey::default() {
                *in_rx
            } else {
                *ex_rx
            },
            reserve_y: if *ex_ry == Pubkey::default() {
                *in_ry
            } else {
                *ex_ry
            },
        },
        (
            PoolLayoutKeys::RaydiumAmm {
                base_mint: ex_bm,
                quote_mint: ex_qm,
                coin_vault: ex_cv,
                pc_vault: ex_pv,
            },
            PoolLayoutKeys::RaydiumAmm {
                base_mint: in_bm,
                quote_mint: in_qm,
                coin_vault: in_cv,
                pc_vault: in_pv,
            },
        ) => PoolLayoutKeys::RaydiumAmm {
            base_mint: if *ex_bm == Pubkey::default() {
                *in_bm
            } else {
                *ex_bm
            },
            quote_mint: if *ex_qm == Pubkey::default() {
                *in_qm
            } else {
                *ex_qm
            },
            coin_vault: if *ex_cv == Pubkey::default() {
                *in_cv
            } else {
                *ex_cv
            },
            pc_vault: if *ex_pv == Pubkey::default() {
                *in_pv
            } else {
                *ex_pv
            },
        },
        _ => existing.clone(),
    }
}

impl PoolLayoutKeys {
    /// Extract layout keys from a TX-parsed or demoted MASTER row (skips PumpFun bonding curve).
    pub fn from_cached_layout_state(state: &CachedPoolState) -> Option<Self> {
        match state {
            CachedPoolState::PumpAmm(s) => Some(PoolLayoutKeys::PumpAmm {
                base_mint: s.base_mint,
                quote_mint: s.quote_mint,
                pool_base_token_account: s.pool_base_token_account,
                pool_quote_token_account: s.pool_quote_token_account,
                pool_accounts: s.pool_accounts.clone(),
            }),
            CachedPoolState::Orca(s) => Some(PoolLayoutKeys::Orca {
                token_mint_a: s.token_mint_a,
                token_mint_b: s.token_mint_b,
                token_vault_a: s.token_vault_a,
                token_vault_b: s.token_vault_b,
            }),
            CachedPoolState::RaydiumCpmm(s) => Some(PoolLayoutKeys::RaydiumCpmm {
                token_0_mint: s.token_0_mint,
                token_1_mint: s.token_1_mint,
                token_0_vault: s.token_0_vault,
                token_1_vault: s.token_1_vault,
            }),
            CachedPoolState::Meteora(s) => Some(PoolLayoutKeys::Meteora {
                token_x_mint: s.token_x_mint,
                token_y_mint: s.token_y_mint,
                reserve_x: s.reserve_x,
                reserve_y: s.reserve_y,
            }),
            CachedPoolState::RaydiumAmm(s) => Some(PoolLayoutKeys::RaydiumAmm {
                base_mint: s.base_mint,
                quote_mint: s.quote_mint,
                coin_vault: s.coin_vault,
                pc_vault: s.pc_vault,
            }),
            CachedPoolState::PumpFun(_) | CachedPoolState::MeteoraCpmm(_) => None,
        }
    }

    pub fn merge_from_cached_layout(&self, incoming: &CachedPoolState) -> Self {
        let Some(inc_keys) = Self::from_cached_layout_state(incoming) else {
            return self.clone();
        };
        merge_layout_keys(self, &inc_keys)
    }

    /// Layout-only MASTER row (reserves / quote numerics unset).
    pub fn into_layout_only_cached_state(self) -> CachedPoolState {
        match self {
            PoolLayoutKeys::PumpAmm {
                base_mint,
                quote_mint,
                pool_base_token_account,
                pool_quote_token_account,
                pool_accounts,
            } => CachedPoolState::PumpAmm(PumpAmmState {
                base_mint,
                quote_mint,
                pool_base_token_account,
                pool_quote_token_account,
                base_reserve: None,
                quote_reserve: None,
                pool_accounts,
                creator: None,
            }),
            PoolLayoutKeys::Orca {
                token_mint_a,
                token_mint_b,
                token_vault_a,
                token_vault_b,
            } => CachedPoolState::Orca(OrcaWhirlpoolState {
                token_mint_a,
                token_mint_b,
                token_vault_a,
                token_vault_b,
                tick_current_index: 0,
                sqrt_price: 0,
                liquidity: 0,
                fee_rate: 0,
                protocol_fee_rate: 0,
                tick_spacing: 0,
                vault_a_balance: None,
                vault_b_balance: None,
                token_a_program: None,
                token_b_program: None,
                whirlpool_quote_account_seeded: false,
            }),
            PoolLayoutKeys::RaydiumCpmm {
                token_0_mint,
                token_1_mint,
                token_0_vault,
                token_1_vault,
            } => CachedPoolState::RaydiumCpmm(RaydiumCpmmState {
                token_0_mint,
                token_1_mint,
                token_0_vault,
                token_1_vault,
                reserve_0: None,
                reserve_1: None,
            }),
            PoolLayoutKeys::Meteora {
                token_x_mint,
                token_y_mint,
                reserve_x,
                reserve_y,
            } => CachedPoolState::Meteora(MeteoraState {
                token_x_mint,
                token_y_mint,
                reserve_x,
                reserve_y,
                active_id: 0,
                bin_step: 0,
                reserve_x_balance: None,
                reserve_y_balance: None,
                dlmm_bin_params_account_seeded: false,
            }),
            PoolLayoutKeys::RaydiumAmm {
                base_mint,
                quote_mint,
                coin_vault,
                pc_vault,
            } => CachedPoolState::RaydiumAmm(RaydiumAmmState {
                base_mint,
                quote_mint,
                coin_vault,
                pc_vault,
                base_decimals: 0,
                quote_decimals: 0,
                coin_reserve: None,
                pc_reserve: None,
                market_id: Pubkey::default(),
                serum_bids: None,
                serum_asks: None,
                serum_event_queue: None,
                serum_base_vault: None,
                serum_quote_vault: None,
            }),
        }
    }

    pub fn has_any_vault_pubkey(&self) -> bool {
        match self {
            PoolLayoutKeys::PumpAmm {
                pool_base_token_account,
                pool_quote_token_account,
                ..
            } => {
                non_default(*pool_base_token_account).is_some()
                    && non_default(*pool_quote_token_account).is_some()
            }
            PoolLayoutKeys::Orca {
                token_vault_a,
                token_vault_b,
                ..
            } => non_default(*token_vault_a).is_some() && non_default(*token_vault_b).is_some(),
            PoolLayoutKeys::RaydiumCpmm {
                token_0_vault,
                token_1_vault,
                ..
            } => non_default(*token_0_vault).is_some() && non_default(*token_1_vault).is_some(),
            PoolLayoutKeys::Meteora {
                reserve_x,
                reserve_y,
                ..
            } => non_default(*reserve_x).is_some() && non_default(*reserve_y).is_some(),
            PoolLayoutKeys::RaydiumAmm {
                coin_vault,
                pc_vault,
                ..
            } => non_default(*coin_vault).is_some() && non_default(*pc_vault).is_some(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_prefers_existing_non_default() {
        let a = Pubkey::new_unique();
        let b = Pubkey::new_unique();
        let existing = PoolLayoutKeys::Orca {
            token_mint_a: a,
            token_mint_b: Pubkey::default(),
            token_vault_a: b,
            token_vault_b: Pubkey::default(),
        };
        let incoming = PoolLayoutKeys::Orca {
            token_mint_a: Pubkey::new_unique(),
            token_mint_b: Pubkey::new_unique(),
            token_vault_a: Pubkey::new_unique(),
            token_vault_b: Pubkey::new_unique(),
        };
        let merged = merge_layout_keys(&existing, &incoming);
        let PoolLayoutKeys::Orca {
            token_mint_a,
            token_vault_a,
            token_mint_b,
            token_vault_b,
        } = merged
        else {
            panic!("expected orca");
        };
        assert_eq!(token_mint_a, a);
        assert_eq!(token_vault_a, b);
        assert_ne!(token_mint_b, Pubkey::default());
        assert_ne!(token_vault_b, Pubkey::default());
    }

    #[test]
    fn take_removes_entry() {
        let mut book = PoolAddressBook::new();
        let pool = Pubkey::new_unique();
        let keys = PoolLayoutKeys::RaydiumCpmm {
            token_0_mint: Pubkey::new_unique(),
            token_1_mint: Pubkey::new_unique(),
            token_0_vault: Pubkey::new_unique(),
            token_1_vault: Pubkey::new_unique(),
        };
        book.merge(pool, keys.clone(), 1_000);
        assert!(book.contains(&pool));
        let taken = book.take(pool).expect("take");
        assert_eq!(taken, keys);
        assert!(!book.contains(&pool));
    }

    #[test]
    fn insert_after_take_unpin_roundtrip() {
        let mut book = PoolAddressBook::new();
        let pool = Pubkey::new_unique();
        let keys = PoolLayoutKeys::PumpAmm {
            base_mint: Pubkey::new_unique(),
            quote_mint: Pubkey::new_unique(),
            pool_base_token_account: Pubkey::new_unique(),
            pool_quote_token_account: Pubkey::new_unique(),
            pool_accounts: vec![],
        };
        book.merge(pool, keys.clone(), 100);
        book.take(pool);
        book.insert_from_demote(pool, keys.clone(), 200);
        assert!(book.contains(&pool));
    }

    #[test]
    fn ttl_evict_drops_stale() {
        let mut book = PoolAddressBook::with_ttl_and_cap(100, 10);
        let pool = Pubkey::new_unique();
        book.merge(
            pool,
            PoolLayoutKeys::Orca {
                token_mint_a: Pubkey::new_unique(),
                token_mint_b: Pubkey::new_unique(),
                token_vault_a: Pubkey::new_unique(),
                token_vault_b: Pubkey::new_unique(),
            },
            0,
        );
        assert_eq!(book.evict_stale(101), 1);
        assert!(!book.contains(&pool));
    }

    #[test]
    fn cap_evict_oldest_lru() {
        let mut book = PoolAddressBook::with_ttl_and_cap(1_000_000, 2);
        let p0 = Pubkey::new_unique();
        let p1 = Pubkey::new_unique();
        let p2 = Pubkey::new_unique();
        let mk = || PoolLayoutKeys::Orca {
            token_mint_a: Pubkey::new_unique(),
            token_mint_b: Pubkey::new_unique(),
            token_vault_a: Pubkey::new_unique(),
            token_vault_b: Pubkey::new_unique(),
        };
        book.merge(p0, mk(), 1);
        book.merge(p1, mk(), 2);
        book.merge(p2, mk(), 3);
        assert_eq!(book.len(), 2);
        assert!(!book.contains(&p0));
        assert!(book.contains(&p1));
        assert!(book.contains(&p2));
    }

    fn test_pump_v14_keys(pool: Pubkey) -> PoolLayoutKeys {
        let base = Pubkey::new_unique();
        let quote = Pubkey::new_unique();
        let mut pool_accounts = vec![
            pool,
            Pubkey::new_unique(),
            base,
            quote,
            Pubkey::new_unique(),
            Pubkey::new_unique(),
        ];
        pool_accounts.extend((0..8).map(|_| Pubkey::new_unique()));
        PoolLayoutKeys::PumpAmm {
            base_mint: base,
            quote_mint: quote,
            pool_base_token_account: pool_accounts[4],
            pool_quote_token_account: pool_accounts[5],
            pool_accounts,
        }
    }

    #[test]
    fn ttl_evict_retains_pump_v14_pin_payload() {
        let mut book = PoolAddressBook::with_ttl_and_cap(DEFAULT_TTL_MS, 10);
        let pool = Pubkey::new_unique();
        book.merge(pool, test_pump_v14_keys(pool), 0);
        assert_eq!(book.evict_stale(DEFAULT_TTL_MS + 1), 0);
        assert!(book.contains(&pool));
    }

    #[test]
    fn cap_evict_prefers_incomplete_over_pump_v14() {
        let mut book = PoolAddressBook::with_ttl_and_cap(1_000_000, 2);
        let pump_pool = Pubkey::new_unique();
        let orca_pool = Pubkey::new_unique();
        book.merge(pump_pool, test_pump_v14_keys(pump_pool), 1);
        let partial = Pubkey::new_unique();
        book.merge(
            partial,
            PoolLayoutKeys::PumpAmm {
                base_mint: Pubkey::new_unique(),
                quote_mint: Pubkey::new_unique(),
                pool_base_token_account: Pubkey::new_unique(),
                pool_quote_token_account: Pubkey::new_unique(),
                pool_accounts: vec![],
            },
            2,
        );
        book.merge(
            orca_pool,
            PoolLayoutKeys::Orca {
                token_mint_a: Pubkey::new_unique(),
                token_mint_b: Pubkey::new_unique(),
                token_vault_a: Pubkey::new_unique(),
                token_vault_b: Pubkey::new_unique(),
            },
            3,
        );
        assert_eq!(book.len(), 2);
        assert!(book.contains(&pump_pool));
        assert!(book.contains(&orca_pool));
        assert!(!book.contains(&partial));
    }
}
