//! tokens_per_sol-Konvention (INVARIANTS.md I-14).
//! LOWER tps = token wertvoller. pnl_pct = (entry/current - 1)*100.

/// `tokens_per_sol` aus UI-Tokenmenge (raw / 10^decimals) und UI-SOL (Lamports / 10^9).
/// I-15 / I-14: dieselbe Einheit wie Momentum-`entry_price` aus fill_out/fill_in-UI-Amounts.
/// Kein raw/raw-Mix: nicht `raw_tokens / raw_lamports` verwenden.
pub fn ui_tokens_per_sol(
    sell_token_amount_raw: u64,
    token_decimals: u8,
    sol_out_lamports: u64,
) -> f64 {
    if sol_out_lamports == 0 {
        return 0.0;
    }
    let d = 10f64.powi(i32::from(token_decimals));
    if d <= 0.0 {
        return 0.0;
    }
    let token_ui = sell_token_amount_raw as f64 / d;
    const LAMPORTS_PER_SOL: f64 = 1_000_000_000.0;
    let sol_ui = sol_out_lamports as f64 / LAMPORTS_PER_SOL;
    if sol_ui <= 0.0 {
        return 0.0;
    }
    let tps = token_ui / sol_ui;
    if tps.is_finite() && tps > 0.0 {
        tps
    } else {
        0.0
    }
}

/// PnL in Prozent. I-14: (entry/current - 1)*100.
/// Token wird günstiger (current UP) -> negativer PnL.
pub fn pnl_pct(entry_price: f64, current_price: f64) -> f64 {
    if entry_price <= 0.0 || current_price <= 0.0 {
        return 0.0;
    }
    ((entry_price / current_price) - 1.0) * 100.0
}

/// Aktualisiert highest_price. I-14: niedrigster tps = bester Preis.
/// Gibt min(current_highest, new_price) zurück.
/// Edge-Case: Bei <= 0 → 0.0 zurückgeben.
pub fn updated_highest_price(current_highest: f64, new_price: f64) -> f64 {
    if current_highest <= 0.0 || new_price <= 0.0 {
        return 0.0;
    }
    current_highest.min(new_price)
}

/// Drawdown vom ATH in Prozent. (current/highest - 1)*100.
/// Edge-Case: Bei <= 0 → 0.0 zurückgeben.
pub fn drawdown_from_ath_pct(highest_price: f64, current_price: f64) -> f64 {
    if highest_price <= 0.0 || current_price <= 0.0 {
        return 0.0;
    }
    ((current_price / highest_price) - 1.0) * 100.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pnl_pct_entry_equal_current_zero() {
        assert!((pnl_pct(100.0, 100.0) - 0.0).abs() < 1e-10);
    }

    #[test]
    fn pnl_pct_token_got_cheaper_negative() {
        // Token got cheaper: current_tps UP (e.g. 200) -> negative PnL
        // entry=100, current=200 -> (100/200 - 1)*100 = -50
        let pnl = pnl_pct(100.0, 200.0);
        assert!(pnl < 0.0, "expected negative pnl, got {}", pnl);
        assert!((pnl - (-50.0)).abs() < 1e-10);
    }

    #[test]
    fn pnl_pct_token_got_expensive_positive() {
        // Token got more expensive: current_tps DOWN (e.g. 50) -> positive PnL
        // entry=100, current=50 -> (100/50 - 1)*100 = 100
        let pnl = pnl_pct(100.0, 50.0);
        assert!(pnl > 0.0, "expected positive pnl, got {}", pnl);
        assert!((pnl - 100.0).abs() < 1e-10);
    }

    #[test]
    fn updated_highest_price_lower_is_better() {
        // Lower tps = better price. min = best.
        assert!((updated_highest_price(100.0, 80.0) - 80.0).abs() < 1e-10);
        assert!((updated_highest_price(80.0, 100.0) - 80.0).abs() < 1e-10);
    }

    #[test]
    fn drawdown_from_ath_pct_at_ath_zero() {
        // At ATH: current == highest -> drawdown 0
        assert!((drawdown_from_ath_pct(100.0, 100.0) - 0.0).abs() < 1e-10);
    }

    #[test]
    fn edge_case_zero_entry_pnl() {
        assert!((pnl_pct(0.0, 100.0) - 0.0).abs() < 1e-10);
        assert!((pnl_pct(100.0, 0.0) - 0.0).abs() < 1e-10);
    }

    #[test]
    fn edge_case_zero_updated_highest() {
        assert!((updated_highest_price(0.0, 100.0) - 0.0).abs() < 1e-10);
        assert!((updated_highest_price(100.0, 0.0) - 0.0).abs() < 1e-10);
    }

    #[test]
    fn edge_case_zero_drawdown() {
        assert!((drawdown_from_ath_pct(0.0, 100.0) - 0.0).abs() < 1e-10);
        assert!((drawdown_from_ath_pct(100.0, 0.0) - 0.0).abs() < 1e-10);
    }

    /// Scope 56 / prod: 9mR7...-class – executable tps must match UI convention (not raw/raw).
    #[test]
    fn ui_tps_9m_r7_like_sell_matched_entry_and_pnl() {
        let entry_tps = 9_876_538.033_6;
        let sell_raw = 12_345_672_542u64;
        let sol_lamports = 418_528u64;
        let decimals: u8 = 6;
        let exec = ui_tokens_per_sol(sell_raw, decimals, sol_lamports);
        assert!((exec - 29_497_841.343_9).abs() < 0.1, "exec tps {exec}");
        let pnl = pnl_pct(entry_tps, exec);
        assert!((pnl - (-66.5)).abs() < 0.2, "pnl {pnl}");
    }

    #[test]
    fn lower_exec_tps_than_entry_is_profit() {
        let p = pnl_pct(1_000.0, 500.0);
        assert!(p > 0.0, "lower tps = more valuable token = profit");
    }

    #[test]
    fn higher_exec_tps_than_entry_is_loss_stop_loss_range() {
        let p = pnl_pct(1_000.0, 2_000.0);
        assert!(p < 0.0, "higher tps = loss");
    }
}
