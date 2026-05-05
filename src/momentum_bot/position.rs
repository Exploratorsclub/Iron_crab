use crate::trade_intent::types::{TradeIntent, TradeSide};

/// Position im Momentum-Bot.
#[derive(Clone, Debug)]
pub struct Position {
    pub intent: TradeIntent,
    pub entry_price: f64, // tokens_per_sol (höher = billiger)
    pub highest_price: f64, // minimaler beobachteter tokens_per_sol (ATH)
    pub tokens_held: f64,
    pub created_at: u64,
}

impl Position {
    /// Aktueller PnL in Prozent.
    pub fn pnl_pct(&self) -> f64 {
        if self.entry_price <= 0.0 || self.intent.current_price <= 0.0 {
            return 0.0;
        }
        // FIX-PNL: tokens_per_sol → entry/current - 1
        (self.entry_price / self.intent.current_price - 1.0) * 100.0
    }

    /// Drawdown vomATH in Prozent.
    pub fn drawdown_from_ath_pct(&self) -> f64 {
        if self.highest_price <= 0.0 || self.intent.current_price <= 0.0 {
            return 0.0;
        }
        // FIX-ATH: highest_price ist minimaler tokens_per_sol (bester Preis = ATH)
        // Drawdown = (current / highest - 1) * 100
        (self.intent.current_price / self.highest_price - 1.0) * 100.0
    }

    /// Aktualisiere Position mit neuem Preis.
    pub fn update_price(&mut self, price: f64) {
        if price <= 0.0 { return; }
        // highest_price = minimaler tokens_per_sol (billigster = ATH)
        if self.highest_price == 0.0 || price < self.highest_price {
            self.highest_price = price;
        }
    }
}