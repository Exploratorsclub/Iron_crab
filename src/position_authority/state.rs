//! Durable position state (read-only PA-1 skeleton).
//!
//! This module is **not** wired to production. Pure reducer for tests and future
//! `position-manager` / JetStream consumption.

use std::collections::btree_map::Entry;
use std::collections::BTreeMap;

use crate::ipc::schema::{ExecutionResult, ExecutionStatus, MarketEventKind, NATIVE_SOL_MINT};

// ---------------------------------------------------------------------------
// Public domain types
// ---------------------------------------------------------------------------

/// Per-mint state maintained by [`PositionAuthority`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PositionState {
    pub mint: String,
    /// On-chain token balance in raw (smallest) units — derived in PA-1 from events.
    pub balance_raw: u64,
    pub decimals: u8,
    /// SPL Token or Token-2022 program id (base58).
    pub token_program: String,
    /// Associated token account, if known from events.
    pub ata: Option<String>,
    /// One entry per confirmed BUY fill (raw token units).
    pub buy_fills: Vec<u64>,
    /// Cumulative raw amount reported sold (from execution SELL events).
    pub sold_raw_total: u64,
    pub status: PositionStatus,
    pub last_update_source: UpdateSource,
}

/// High-level position lifecycle for PA-1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PositionStatus {
    Open,
    Closed,
    /// Internal inconsistency (e.g. SELL > computed balance) — needs chain reconciliation.
    ReconcileNeeded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateSource {
    Execution,
    WalletSnapshot,
}

/// Test-oriented input for the reducer. Production wiring may map
/// `ExecutionResult` / `MarketEventKind` into these.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PositionEvent {
    /// Confirmed BUY: `fill_raw` is token `fill_out` (tokens received).
    BuyConfirmed {
        mint: String,
        fill_raw: u64,
        decimals: u8,
        token_program: String,
        ata: Option<String>,
    },
    /// Confirmed SELL: `sold_raw` is token `fill_in` (tokens sold).
    SellConfirmed {
        mint: String,
        sold_raw: u64,
        decimals: u8,
        token_program: String,
    },
    /// `MarketEventKind::WalletBalanceSnapshot`-compatible.
    WalletBalanceSnapshot {
        mint: String,
        balance_raw: u64,
        decimals: u8,
        token_program: String,
    },
    /// `MarketEventKind::WalletSnapshotComplete` (no-op in PA-1; reserved for later ghost cleanup).
    WalletSnapshotComplete {
        wallet: String,
        mints_in_wallet: Vec<String>,
        is_periodic: bool,
    },
}

impl PositionEvent {
    /// From `ExecutionResult` when `status == Confirmed`, `token_mint` is set, `metadata["side"]`
    /// is `BUY`/`SELL`, and the corresponding fill is present (BUY → `fill_out`, SELL → `fill_in`).
    pub fn try_from_execution_result(result: &ExecutionResult) -> Option<Self> {
        if result.status != ExecutionStatus::Confirmed {
            return None;
        }
        let mint = result.token_mint.clone()?;
        if is_sol_or_wsol_mint(&mint) {
            return None;
        }
        let side = result.metadata.get("side")?.as_str();
        match side {
            "BUY" => {
                let fo = result.fill_out.as_ref()?;
                let token_program = result
                    .metadata
                    .get("token_program")
                    .cloned()
                    .unwrap_or_else(default_spl_token_program);
                let ata = result.metadata.get("token_account").cloned();
                Some(PositionEvent::BuyConfirmed {
                    mint,
                    fill_raw: fo.raw,
                    decimals: fo.decimals,
                    token_program,
                    ata,
                })
            }
            "SELL" => {
                let fi = result.fill_in.as_ref()?;
                let token_program = result
                    .metadata
                    .get("token_program")
                    .cloned()
                    .unwrap_or_else(default_spl_token_program);
                Some(PositionEvent::SellConfirmed {
                    mint,
                    sold_raw: fi.raw,
                    decimals: fi.decimals,
                    token_program,
                })
            }
            _ => None,
        }
    }

    /// Map a single [`MarketEventKind`] wallet variant into a [`PositionEvent`], if applicable.
    pub fn try_from_market_event_kind(kind: &MarketEventKind) -> Option<Self> {
        match kind {
            MarketEventKind::WalletBalanceSnapshot {
                mint,
                balance_raw,
                decimals,
                token_program,
            } => {
                if is_sol_or_wsol_mint(mint) {
                    return None;
                }
                Some(PositionEvent::WalletBalanceSnapshot {
                    mint: mint.clone(),
                    balance_raw: *balance_raw,
                    decimals: *decimals,
                    token_program: token_program.clone(),
                })
            }
            MarketEventKind::WalletSnapshotComplete {
                mints_in_wallet,
                wallet,
                is_periodic,
            } => Some(PositionEvent::WalletSnapshotComplete {
                wallet: wallet.clone(),
                mints_in_wallet: mints_in_wallet.clone(),
                is_periodic: *is_periodic,
            }),
            _ => None,
        }
    }
}

fn default_spl_token_program() -> String {
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_string()
}

/// Mints that must not be counted as open **trade** positions in PositionAuthority
/// (wrapped/native SOL; same canonical mint as [`NATIVE_SOL_MINT`], plus JetStream
/// `NATIVE_SOL` sentinel for lamports).
#[inline]
pub fn is_sol_or_wsol_mint(mint: &str) -> bool {
    mint == "NATIVE_SOL" || mint == NATIVE_SOL_MINT
}

/// `authority open count` minus `lock manager open count` (same notion as
/// `LockManager::count_non_zero_token_balances` in execution-engine).
#[inline]
pub fn position_authority_drift_lockmanager(authority_open: usize, lockmanager_open: usize) -> i64 {
    authority_open as i64 - lockmanager_open as i64
}

// ---------------------------------------------------------------------------
// Reducer
// ---------------------------------------------------------------------------

/// Read-only aggregate of per-mint [`PositionState`].
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PositionAuthority {
    by_mint: BTreeMap<String, PositionState>,
}

impl PositionAuthority {
    pub fn new() -> Self {
        Self::default()
    }

    /// PA-2: same mapping as execution-engine (confirmed-only; WSOL/SOL execution rows ignored).
    pub fn apply_from_confirmed_execution_result(&mut self, result: &ExecutionResult) {
        if let Some(ev) = PositionEvent::try_from_execution_result(result) {
            self.apply(&ev);
        }
    }

    /// PA-2: `MarketEventKind` → same filter as `try_from_market_event_kind` (skips SOL/WSOL for snapshots).
    pub fn apply_from_wallet_market_event_kind(&mut self, kind: &MarketEventKind) {
        if let Some(ev) = PositionEvent::try_from_market_event_kind(kind) {
            self.apply(&ev);
        }
    }

    pub fn apply(&mut self, event: &PositionEvent) {
        match event {
            PositionEvent::BuyConfirmed {
                mint,
                fill_raw,
                decimals,
                token_program,
                ata,
            } => {
                self.apply_buy(mint, *fill_raw, *decimals, token_program, ata.as_ref());
            }
            PositionEvent::SellConfirmed {
                mint,
                sold_raw,
                decimals,
                token_program,
            } => {
                self.apply_sell(mint, *sold_raw, *decimals, token_program);
            }
            PositionEvent::WalletBalanceSnapshot {
                mint,
                balance_raw,
                decimals,
                token_program,
            } => {
                self.apply_wallet_snapshot(mint, *balance_raw, *decimals, token_program);
            }
            PositionEvent::WalletSnapshotComplete { .. } => {}
        }
    }

    fn apply_buy(
        &mut self,
        mint: &str,
        fill_raw: u64,
        decimals: u8,
        token_program: &str,
        ata: Option<&String>,
    ) {
        let e = self
            .by_mint
            .entry(mint.to_string())
            .or_insert_with(|| PositionState {
                mint: mint.to_string(),
                balance_raw: 0,
                decimals,
                token_program: token_program.to_string(),
                ata: None,
                buy_fills: Vec::new(),
                sold_raw_total: 0,
                status: PositionStatus::Closed,
                last_update_source: UpdateSource::Execution,
            });
        e.decimals = decimals;
        e.token_program = token_program.to_string();
        if let Some(a) = ata {
            e.ata = Some(a.clone());
        }
        e.buy_fills.push(fill_raw);
        e.balance_raw = e.balance_raw.saturating_add(fill_raw);
        e.status = if e.balance_raw == 0 {
            PositionStatus::Closed
        } else {
            PositionStatus::Open
        };
        e.last_update_source = UpdateSource::Execution;
    }

    fn apply_sell(&mut self, mint: &str, sold_raw: u64, decimals: u8, token_program: &str) {
        let e = self
            .by_mint
            .entry(mint.to_string())
            .or_insert_with(|| PositionState {
                mint: mint.to_string(),
                balance_raw: 0,
                decimals,
                token_program: token_program.to_string(),
                ata: None,
                buy_fills: Vec::new(),
                sold_raw_total: 0,
                status: PositionStatus::Closed,
                last_update_source: UpdateSource::Execution,
            });
        e.decimals = decimals;
        e.token_program = token_program.to_string();
        e.sold_raw_total = e.sold_raw_total.saturating_add(sold_raw);
        let new_bal = e.balance_raw.saturating_sub(sold_raw);
        if sold_raw > e.balance_raw {
            e.status = PositionStatus::ReconcileNeeded;
            e.balance_raw = 0;
        } else {
            e.balance_raw = new_bal;
            e.status = if e.balance_raw == 0 {
                PositionStatus::Closed
            } else {
                PositionStatus::Open
            };
        }
        e.last_update_source = UpdateSource::Execution;
    }

    fn apply_wallet_snapshot(
        &mut self,
        mint: &str,
        balance_raw: u64,
        decimals: u8,
        token_program: &str,
    ) {
        if balance_raw == 0 {
            self.by_mint.remove(mint);
            return;
        }

        match self.by_mint.entry(mint.to_string()) {
            Entry::Occupied(mut occ) => {
                let e = occ.get_mut();
                let prev = e.balance_raw;
                e.decimals = decimals;
                e.token_program = token_program.to_string();
                e.last_update_source = UpdateSource::WalletSnapshot;
                if prev == balance_raw {
                    // Invariant: balance_raw > 0 (zero balances remove the entry at function start).
                    e.status = PositionStatus::Open;
                } else {
                    e.balance_raw = balance_raw;
                    e.status = PositionStatus::ReconcileNeeded;
                }
            }
            Entry::Vacant(v) => {
                // `balance_raw > 0` here: recovered from wallet with no prior execution state.
                v.insert(PositionState {
                    mint: mint.to_string(),
                    balance_raw,
                    decimals,
                    token_program: token_program.to_string(),
                    ata: None,
                    buy_fills: Vec::new(),
                    sold_raw_total: 0,
                    status: PositionStatus::ReconcileNeeded,
                    last_update_source: UpdateSource::WalletSnapshot,
                });
            }
        }
    }

    pub fn get(&self, mint: &str) -> Option<&PositionState> {
        self.by_mint.get(mint)
    }

    /// Count of mints with a non-zero balance in this model.
    pub fn open_positions_count(&self) -> usize {
        self.by_mint.values().filter(|p| p.balance_raw > 0).count()
    }

    /// Mints in [`PositionStatus::ReconcileNeeded`] with non-zero model balance.
    pub fn reconcile_needed_positions_count(&self) -> usize {
        self.by_mint
            .values()
            .filter(|p| p.balance_raw > 0 && p.status == PositionStatus::ReconcileNeeded)
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::ipc::schema::{ExplicitAmount, RecordHeader};

    fn mint() -> String {
        "SoMeMint1111111111111111111111111111111111".to_string()
    }

    fn token_2022() -> String {
        "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb".to_string()
    }

    fn buy(m: &str, raw: u64, dec: u8) -> PositionEvent {
        PositionEvent::BuyConfirmed {
            mint: m.to_string(),
            fill_raw: raw,
            decimals: dec,
            token_program: default_spl_token_program(),
            ata: None,
        }
    }

    fn sell(m: &str, raw: u64, dec: u8) -> PositionEvent {
        PositionEvent::SellConfirmed {
            mint: m.to_string(),
            sold_raw: raw,
            decimals: dec,
            token_program: default_spl_token_program(),
        }
    }

    fn snap(m: &str, bal: u64, dec: u8) -> PositionEvent {
        PositionEvent::WalletBalanceSnapshot {
            mint: m.to_string(),
            balance_raw: bal,
            decimals: dec,
            token_program: default_spl_token_program(),
        }
    }

    #[test]
    fn buy_then_scale_in_accumulates_balance() {
        let m = mint();
        let mut a = PositionAuthority::new();
        a.apply(&buy(&m, 100, 6));
        a.apply(&buy(&m, 300, 6));
        let p = a.get(&m).expect("position");
        assert_eq!(p.balance_raw, 400);
        assert_eq!(p.status, PositionStatus::Open);
        assert_eq!(p.buy_fills, vec![100, 300]);
    }

    #[test]
    fn partial_sell_keeps_position_open() {
        let m = mint();
        let mut a = PositionAuthority::new();
        a.apply(&buy(&m, 400, 6));
        a.apply(&sell(&m, 100, 6));
        let p = a.get(&m).expect("position");
        assert_eq!(p.balance_raw, 300);
        assert_eq!(p.sold_raw_total, 100);
        assert_eq!(p.status, PositionStatus::Open);
    }

    #[test]
    fn full_sell_closes_position() {
        let m = mint();
        let mut a = PositionAuthority::new();
        a.apply(&buy(&m, 400, 6));
        a.apply(&sell(&m, 400, 6));
        let p = a.get(&m).expect("position");
        assert_eq!(p.balance_raw, 0);
        assert_eq!(p.sold_raw_total, 400);
        assert_eq!(p.status, PositionStatus::Closed);
    }

    #[test]
    fn wallet_zero_snapshot_closes_position() {
        let m = mint();
        let mut a = PositionAuthority::new();
        a.apply(&buy(&m, 400, 6));
        a.apply(&snap(&m, 0, 6));
        assert!(a.get(&m).is_none());
        assert_eq!(a.open_positions_count(), 0);
    }

    #[test]
    fn wallet_nonzero_snapshot_recovers_missing_position() {
        let m = mint();
        let mut a = PositionAuthority::new();
        a.apply(&snap(&m, 250, 9));
        let p = a.get(&m).expect("position");
        assert_eq!(p.balance_raw, 250);
        assert_eq!(p.status, PositionStatus::ReconcileNeeded);
        assert_eq!(p.last_update_source, UpdateSource::WalletSnapshot);
    }

    #[test]
    fn wallet_snapshot_matching_existing_position_keeps_open_not_reconcile() {
        let m = mint();
        let mut a = PositionAuthority::new();
        a.apply(&buy(&m, 400, 6));
        a.apply(&snap(&m, 400, 6));
        let p = a.get(&m).expect("position");
        assert_eq!(p.balance_raw, 400);
        assert_eq!(p.status, PositionStatus::Open);
        assert_eq!(a.reconcile_needed_positions_count(), 0);
    }

    #[test]
    fn wallet_snapshot_different_existing_position_marks_reconcile_needed() {
        let m = mint();
        let mut a = PositionAuthority::new();
        a.apply(&buy(&m, 400, 6));
        a.apply(&snap(&m, 350, 6));
        let p = a.get(&m).expect("position");
        assert_eq!(p.balance_raw, 350);
        assert_eq!(p.status, PositionStatus::ReconcileNeeded);
        assert_eq!(a.reconcile_needed_positions_count(), 1);
    }

    #[test]
    fn token_2022_program_preserved() {
        let m = mint();
        let tp = token_2022();
        let mut a = PositionAuthority::new();
        a.apply(&PositionEvent::BuyConfirmed {
            mint: m.clone(),
            fill_raw: 1,
            decimals: 6,
            token_program: tp.clone(),
            ata: None,
        });
        assert_eq!(a.get(&m).unwrap().token_program, tp);
    }

    #[test]
    fn sell_more_than_balance_saturates_marks_reconcile_sell_150_bought_100() {
        let m = mint();
        let mut a = PositionAuthority::new();
        a.apply(&buy(&m, 100, 6));
        a.apply(&sell(&m, 150, 6));
        let p = a.get(&m).expect("position");
        assert_eq!(p.balance_raw, 0);
        assert_eq!(p.sold_raw_total, 150);
        assert_eq!(p.status, PositionStatus::ReconcileNeeded);
    }

    #[test]
    fn wallet_snapshot_ignores_wsol_mint() {
        let m = NATIVE_SOL_MINT.to_string();
        let k = MarketEventKind::WalletBalanceSnapshot {
            mint: m,
            balance_raw: 1_000_000,
            decimals: 9,
            token_program: default_spl_token_program(),
        };
        assert!(PositionEvent::try_from_market_event_kind(&k).is_none());
    }

    #[test]
    fn wallet_snapshot_ignores_native_sol_lamport_sentinel() {
        let k = MarketEventKind::WalletBalanceSnapshot {
            mint: "NATIVE_SOL".to_string(),
            balance_raw: 5_000_000_000,
            decimals: 9,
            token_program: default_spl_token_program(),
        };
        assert!(PositionEvent::try_from_market_event_kind(&k).is_none());
    }

    #[test]
    fn reconcile_needed_count_only_reconcile_status() {
        let m = mint();
        let mut a = PositionAuthority::new();
        a.apply(&snap(&m, 250, 6));
        assert_eq!(a.reconcile_needed_positions_count(), 1);
        a.apply(&buy(&m, 100, 6));
        assert_eq!(a.reconcile_needed_positions_count(), 0);
    }

    #[test]
    fn drift_helper_matches_lock_manager_style_counts() {
        use crate::storage::locks::LockManager;
        let lm = LockManager::new(0);
        lm.set_available_token_balance("mintA".to_string(), 1);
        lm.set_available_token_balance("mintB".to_string(), 1);
        let lock_open = lm.count_non_zero_token_balances();
        let mut a = PositionAuthority::new();
        a.apply_from_wallet_market_event_kind(&MarketEventKind::WalletBalanceSnapshot {
            mint: "mintA".to_string(),
            balance_raw: 1,
            decimals: 6,
            token_program: default_spl_token_program(),
        });
        a.apply_from_wallet_market_event_kind(&MarketEventKind::WalletBalanceSnapshot {
            mint: "mintB".to_string(),
            balance_raw: 1,
            decimals: 6,
            token_program: default_spl_token_program(),
        });
        a.apply_from_wallet_market_event_kind(&MarketEventKind::WalletBalanceSnapshot {
            mint: "mintC".to_string(),
            balance_raw: 1,
            decimals: 6,
            token_program: default_spl_token_program(),
        });
        let auth_open = a.open_positions_count();
        assert_eq!(lock_open, 2);
        assert_eq!(auth_open, 3);
        assert_eq!(
            position_authority_drift_lockmanager(auth_open, lock_open),
            1
        );
    }

    #[test]
    fn apply_from_confirmed_exec_buy_increments_open() {
        use std::collections::HashMap;

        let m = mint();
        let mut meta = HashMap::new();
        meta.insert("side".to_string(), "BUY".to_string());
        meta.insert("token_program".to_string(), default_spl_token_program());

        let r = ExecutionResult {
            header: RecordHeader::new("test", "0", "run"),
            execution_id: "e1".to_string(),
            decision_id: "d1".to_string(),
            intent_id: "i1".to_string(),
            source: "test".to_string(),
            token_mint: Some(m.clone()),
            signature: None,
            bundle_id: None,
            status: ExecutionStatus::Confirmed,
            fill_in: None,
            fill_out: Some(ExplicitAmount::new(10, 6)),
            fill_status: None,
            fill_unavailable_reason: None,
            confirmed_slot: None,
            block_time_unix_ms: None,
            fees: None,
            pnl: None,
            wallet_sol_delta_lamports: None,
            error_message: None,
            error_code: None,
            latency_ms: None,
            metadata: meta,
        };
        let mut a = PositionAuthority::new();
        a.apply_from_confirmed_execution_result(&r);
        assert_eq!(a.open_positions_count(), 1);
        assert_eq!(a.get(&m).unwrap().balance_raw, 10);
    }

    #[test]
    fn apply_from_confirmed_exec_sell_closes() {
        use std::collections::HashMap;

        let m = mint();
        let mut buy_meta = HashMap::new();
        buy_meta.insert("side".to_string(), "BUY".to_string());
        buy_meta.insert("token_program".to_string(), default_spl_token_program());
        let buy = ExecutionResult {
            header: RecordHeader::new("test", "0", "run"),
            execution_id: "e0".to_string(),
            decision_id: "d0".to_string(),
            intent_id: "i0".to_string(),
            source: "test".to_string(),
            token_mint: Some(m.clone()),
            signature: None,
            bundle_id: None,
            status: ExecutionStatus::Confirmed,
            fill_in: None,
            fill_out: Some(ExplicitAmount::new(100, 6)),
            fill_status: None,
            fill_unavailable_reason: None,
            confirmed_slot: None,
            block_time_unix_ms: None,
            fees: None,
            pnl: None,
            wallet_sol_delta_lamports: None,
            error_message: None,
            error_code: None,
            latency_ms: None,
            metadata: buy_meta,
        };
        let mut sell_meta = HashMap::new();
        sell_meta.insert("side".to_string(), "SELL".to_string());
        sell_meta.insert("token_program".to_string(), default_spl_token_program());
        let sell = ExecutionResult {
            header: RecordHeader::new("test", "0", "run"),
            execution_id: "e1".to_string(),
            decision_id: "d1".to_string(),
            intent_id: "i1".to_string(),
            source: "test".to_string(),
            token_mint: Some(m.clone()),
            signature: None,
            bundle_id: None,
            status: ExecutionStatus::Confirmed,
            fill_in: Some(ExplicitAmount::new(100, 6)),
            fill_out: None,
            fill_status: None,
            fill_unavailable_reason: None,
            confirmed_slot: None,
            block_time_unix_ms: None,
            fees: None,
            pnl: None,
            wallet_sol_delta_lamports: None,
            error_message: None,
            error_code: None,
            latency_ms: None,
            metadata: sell_meta,
        };
        let mut a = PositionAuthority::new();
        a.apply_from_confirmed_execution_result(&buy);
        a.apply_from_confirmed_execution_result(&sell);
        assert_eq!(a.open_positions_count(), 0);
    }

    #[test]
    fn apply_from_wallet_snapshot_zero_removes() {
        let m = mint();
        let mut a = PositionAuthority::new();
        a.apply_from_confirmed_execution_result(&ExecutionResult {
            header: RecordHeader::new("t", "0", "r"),
            execution_id: "e".to_string(),
            decision_id: "d".to_string(),
            intent_id: "i".to_string(),
            source: "s".to_string(),
            token_mint: Some(m.clone()),
            signature: None,
            bundle_id: None,
            status: ExecutionStatus::Confirmed,
            fill_in: None,
            fill_out: Some(ExplicitAmount::new(100, 6)),
            fill_status: None,
            fill_unavailable_reason: None,
            confirmed_slot: None,
            block_time_unix_ms: None,
            fees: None,
            pnl: None,
            wallet_sol_delta_lamports: None,
            error_message: None,
            error_code: None,
            latency_ms: None,
            metadata: {
                let mut h = std::collections::HashMap::new();
                h.insert("side".to_string(), "BUY".to_string());
                h.insert("token_program".to_string(), default_spl_token_program());
                h
            },
        });
        a.apply_from_wallet_market_event_kind(&MarketEventKind::WalletBalanceSnapshot {
            mint: m,
            balance_raw: 0,
            decimals: 6,
            token_program: default_spl_token_program(),
        });
        assert_eq!(a.open_positions_count(), 0);
    }

    #[test]
    fn execution_result_helper_buy_sell_roundtrip() {
        use std::collections::HashMap;

        use crate::ipc::schema::RecordHeader;

        let m = mint();
        let mut meta = HashMap::new();
        meta.insert("side".to_string(), "BUY".to_string());
        meta.insert("token_program".to_string(), token_2022());

        let r = ExecutionResult {
            header: RecordHeader::new("test", "0", "run"),
            execution_id: "e1".to_string(),
            decision_id: "d1".to_string(),
            intent_id: "i1".to_string(),
            source: "test".to_string(),
            token_mint: Some(m.clone()),
            signature: None,
            bundle_id: None,
            status: ExecutionStatus::Confirmed,
            fill_in: None,
            fill_out: Some(crate::ipc::schema::ExplicitAmount::new(50, 6)),
            fill_status: None,
            fill_unavailable_reason: None,
            confirmed_slot: None,
            block_time_unix_ms: None,
            fees: None,
            pnl: None,
            wallet_sol_delta_lamports: None,
            error_message: None,
            error_code: None,
            latency_ms: None,
            metadata: meta,
        };
        let ev = PositionEvent::try_from_execution_result(&r).expect("mapped");
        let mut a = PositionAuthority::new();
        a.apply(&ev);
        assert_eq!(a.get(&m).unwrap().balance_raw, 50);
        assert_eq!(a.get(&m).unwrap().token_program, token_2022());
    }
}
