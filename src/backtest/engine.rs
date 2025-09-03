use anyhow::Result;
use super::types::{SimEvent,Portfolio,StrategyDecision,BacktestStrategy,StrategyAction,SimContext};
use super::market::MarketAdapter;

fn apply_slippage_min_out(quote_out: u64, slippage_bps: u32) -> u64 {
    if slippage_bps == 0 { return quote_out; }
    let keep = 10_000u64.saturating_sub(slippage_bps as u64);
    (quote_out as u128 * keep as u128 / 10_000u128) as u64
}

pub struct BacktestEngine<S: BacktestStrategy, M: MarketAdapter> {
    pub strategy: S,
    pub market: M,
    pub portfolio: Portfolio,
    pub events: Vec<SimEvent>,
    pub decisions: Vec<StrategyDecision>,
    pub slippage_rejections: Vec<String>,
}
impl<S: BacktestStrategy, M: MarketAdapter> BacktestEngine<S,M> {
    pub fn new(strategy:S, market:M, portfolio:Portfolio, events:Vec<SimEvent>) -> Self { Self { strategy, market, portfolio, events, decisions: vec![], slippage_rejections: vec![] } }
    pub fn run(&mut self) -> Result<()> {
        for ev in &self.events {
            let ctx = SimContext { portfolio: &self.portfolio, time_ms: ev.ts_ms };
            let decision = self.strategy.on_event(&ctx, ev);
            for act in &decision.actions {
                if let StrategyAction::Swap(a) = act {
                    // Pre-quote for slippage enforcement
                    let mut min_out: Option<u64> = None;
                    if a.max_slippage_bps > 0 {
                        if let Some(q) = self.market.quote(&a.input_mint, &a.output_mint, a.amount_in) {
                            let m = apply_slippage_min_out(q.amount_out, a.max_slippage_bps);
                            min_out = Some(m);
                        }
                    }
                    let (iin,out) = self.market.apply_swap(a)?;
                    if let Some(m) = min_out {
                        if out < m {
                            // Reject – record & skip portfolio mutation
                            self.slippage_rejections.push(format!("swap {}->{} amount_in={} out={} min_out={}", a.input_mint, a.output_mint, a.amount_in, out, m));
                            continue;
                        }
                    }
                    self.portfolio.apply_swap(&a.input_mint,&a.output_mint,iin,out);
                }
            }
            self.decisions.push(decision);
        }
        Ok(())
    }
}

pub struct NoopStrategy;
impl BacktestStrategy for NoopStrategy { fn on_event(&self, _ctx:&SimContext, _ev:&SimEvent) -> StrategyDecision { StrategyDecision { actions: vec![] } } }

pub fn make_swap(pool:&str, input_mint:&str, output_mint:&str, amount_in:u64, max_slippage_bps:u32) -> StrategyAction { use super::types::ActionSwap; StrategyAction::Swap(ActionSwap { pool: pool.into(), input_mint: input_mint.into(), output_mint: output_mint.into(), amount_in, max_slippage_bps }) }
