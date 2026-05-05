use crate::execution::tokens_per_sol;
use crate::trade_intent::types::TradeIntent;

/// Position im Momentum-Bot.
#[derive(Clone, Debug)]
pub struct Position {
    pub intent: TradeIntent,
    pub entry_price: f64,   // tokens_per_sol (höher = billiger)
    pub highest_price: f64, // minimaler beobachteter tokens_per_sol (ATH)
    pub tokens_held: f64,
    pub created_at: u64,
}

impl Position {
    /// Aktueller PnL in Prozent.
    pub fn pnl_pct(&self) -> f64 {
        tokens_per_sol::pnl_pct(self.entry_price, self.intent.current_price)
    }

    /// Drawdown vom ATH in Prozent.
    pub fn drawdown_from_ath_pct(&self) -> f64 {
        tokens_per_sol::drawdown_from_ath_pct(self.highest_price, self.intent.current_price)
    }

    /// Aktualisiere Position mit neuem Preis.
    pub fn update_price(&mut self, price: f64) {
        if price <= 0.0 {
            return;
        }
        self.intent.current_price = price;
        let prev = self.highest_price;
        let updated = tokens_per_sol::updated_highest_price(prev, price);
        if prev <= 0.0 && updated <= 0.0 {
            self.highest_price = price;
        } else {
            self.highest_price = updated;
        }
    }
}
