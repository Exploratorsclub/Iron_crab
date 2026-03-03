//! tokens_per_sol-Konvention (INVARIANTS.md I-14).
//! LOWER tps = token wertvoller. pnl_pct = (entry/current - 1)*100.

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
}
