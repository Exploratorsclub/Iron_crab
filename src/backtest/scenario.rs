use super::{
    engine::BacktestEngine,
    market::MarketAdapter,
    types::{BacktestStrategy, SimEvent, SimEventKind},
};

use crate::backtest::impact::ImpactSettings;

pub struct Scenario {
    pub name: String,
    pub sizes: Vec<u64>,
    pub slippages_bps: Vec<u32>,
    pub impact: Option<ImpactSettings>,
}

impl Scenario {
    pub fn run<S: BacktestStrategy, M: MarketAdapter>(
        self,
        mut engine: BacktestEngine<S, M>,
    ) -> anyhow::Result<BacktestEngine<S, M>> {
        // Inject a ScenarioMeta at the beginning for strategy awareness
        let size = self.sizes.first().cloned().unwrap_or(0);
        let slippage_bps = self.slippages_bps.first().cloned().unwrap_or(0);
        // Set optional impact knobs
        if let Some(impact) = self.impact.clone() {
            engine.set_impact_settings(impact);
        }
        if slippage_bps > 0 {
            engine.set_slippage_override_bps(Some(slippage_bps));
        }
        // Prepend meta event
        engine.events.insert(
            0,
            SimEvent {
                ts_ms: engine.events.first().map(|e| e.ts_ms.saturating_sub(1)).unwrap_or(0),
                kind: SimEventKind::ScenarioMeta {
                    name: self.name,
                    size,
                    slippage_bps,
                    latency_ms: engine
                        .impact_settings
                        .emulate_latency_ms
                        .unwrap_or(0),
                },
            },
        );
        engine.run()?;
        Ok(engine)
    }

    /// Run a parameter sweep over all (size, slippage_bps) pairs by creating a fresh engine via a user-provided factory.
    /// This avoids requiring BacktestEngine to be Clone.
    pub fn run_for_each<S, M, F>(self, mut make_engine: F) -> anyhow::Result<Vec<BacktestEngine<S, M>>>
    where
        S: BacktestStrategy,
        M: MarketAdapter,
        F: FnMut(u64, u32) -> BacktestEngine<S, M>,
    {
        let mut results = Vec::new();
        for sz in &self.sizes {
            for sl in &self.slippages_bps {
                let mut eng = make_engine(*sz, *sl);
                // announce scenario to the strategy
                let latency_ms = eng
                    .impact_settings
                    .emulate_latency_ms
                    .unwrap_or(0);
                eng.events.insert(
                    0,
                    SimEvent {
                        ts_ms: eng.events.first().map(|e| e.ts_ms.saturating_sub(1)).unwrap_or(0),
                        kind: SimEventKind::ScenarioMeta {
                            name: self.name.clone(),
                            size: *sz,
                            slippage_bps: *sl,
                            latency_ms,
                        },
                    },
                );
                if let Some(impact) = self.impact.clone() {
                    eng.set_impact_settings(impact);
                }
                if *sl > 0 {
                    eng.set_slippage_override_bps(Some(*sl));
                }
                eng.run()?;
                results.push(eng);
            }
        }
        Ok(results)
    }
}
