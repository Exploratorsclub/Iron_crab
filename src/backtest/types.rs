use std::collections::HashMap;
use rust_decimal::Decimal;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SimEventKind {
    SlotAdvance { slot: u64 },
    CfmPriceUpdate { pool: String, base_reserve: u128, quote_reserve: u128, fee_bps: u32 },
    NewPool { pool: String, base_mint: String, quote_mint: String, fee_bps: u32 },
    TradeFill { pool: String, input: u64, output: u64 },
    Log(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimEvent { pub ts_ms: u64, pub kind: SimEventKind }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position { pub amount: Decimal }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Portfolio { pub tokens: HashMap<String, Position> }

impl Portfolio {
    pub fn new() -> Self { Self { tokens: HashMap::new() } }
    pub fn add(&mut self, mint: &str, delta: Decimal) {
        self.tokens.entry(mint.to_string()).and_modify(|p| p.amount += delta).or_insert(Position{amount: delta});
    }
    pub fn apply_swap(&mut self, input: &str, output: &str, in_amount: u64, out_amount: u64) {
        let din = Decimal::from(in_amount as i128);
        let dout = Decimal::from(out_amount as i128);
        self.add(output, dout);
        self.add(input, -din);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionSwap { pub pool: String, pub input_mint: String, pub output_mint: String, pub amount_in: u64, pub max_slippage_bps: u32 }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StrategyAction { Swap(ActionSwap) }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyDecision { pub actions: Vec<StrategyAction> }

pub trait BacktestStrategy: Send + Sync {
    fn on_event(&self, ctx: &SimContext, event: &SimEvent) -> StrategyDecision;
}

pub struct SimContext<'a> {
    pub portfolio: &'a Portfolio,
    pub time_ms: u64,
}
