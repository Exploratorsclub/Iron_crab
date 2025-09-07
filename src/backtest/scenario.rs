use super::{engine::BacktestEngine, market::MarketAdapter, types::BacktestStrategy};

pub struct Scenario {
    pub sizes: Vec<u64>,
    pub slippages_bps: Vec<u32>,
}

impl Scenario {
    pub fn run<S: BacktestStrategy, M: MarketAdapter>(
        self,
        mut engine: BacktestEngine<S, M>,
    ) -> anyhow::Result<BacktestEngine<S, M>> {
        // Simple: engine already contains events & strategy that may inspect size/slippage via its own logic.
        // Advanced parameterization can be added later via callbacks.
        engine.run()?;
        Ok(engine)
    }
}
