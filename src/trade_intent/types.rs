//! `current_price` is `tokens_per_sol` (UI), same convention as `execution::tokens_per_sol` (I-14).

#[derive(Clone, Debug, Default)]
pub struct PositionPriceState {
    pub current_price: f64,
}
