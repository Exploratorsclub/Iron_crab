//! Durable position state (read-only PA-1 skeleton).
//!
//! This module is **not** wired to production. Pure reducer for tests and future
//! `position-manager` / JetStream consumption.

use std::collections::BTreeMap;

use crate::ipc::schema::{ExecutionResult, ExecutionStatus, MarketEventKind};

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
            } => Some(PositionEvent::WalletBalanceSnapshot {
                mint: mint.clone(),
                balance_raw: *balance_raw,
                decimals: *decimals,
                token_program: token_program.clone(),
            }),
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
                last_update_source: UpdateSource::WalletSnapshot,
            });
        e.decimals = decimals;
        e.token_program = token_program.to_string();
        e.balance_raw = balance_raw;
        e.status = PositionStatus::ReconcileNeeded;
        e.last_update_source = UpdateSource::WalletSnapshot;
    }

    pub fn get(&self, mint: &str) -> Option<&PositionState> {
        self.by_mint.get(mint)
    }

    /// Count of mints with a non-zero balance in this model.
    pub fn open_positions_count(&self) -> usize {
        self.by_mint.values().filter(|p| p.balance_raw > 0).count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
