use ironcrab::backtest::{engine::BacktestEngine, market::{CfmAdapter, CfmPool}, impact::ImpactSettings};
use ironcrab::backtest::types::{Portfolio, SimEvent, SimEventKind, StrategyAction, ActionSwap, StrategyDecision, BacktestStrategy, SimContext};

struct OneShotStrategy;
impl BacktestStrategy for OneShotStrategy {
    fn on_tick(&self, _ctx: &SimContext) -> ironcrab::backtest::types::StrategyDecision {
        StrategyDecision { actions: vec![
            StrategyAction::Swap(ActionSwap { pool: "P".into(), input_mint: "A".into(), output_mint: "B".into(), amount_in: 1_000_000, max_slippage_bps: 50 })
        ] }
    }
}

#[test]
fn impact_noise_min_out_applies() {
    let mut adapter = CfmAdapter::new();
    adapter.upsert_pool(CfmPool { pool: "P".into(), base_mint: "A".into(), quote_mint: "B".into(), base_reserve: 1_000_000_000, quote_reserve: 1_000_000_000, fee_bps: 30, tick_spacing: None });
    let portfolio = Portfolio::new();
    let events = vec![SimEvent { ts_ms: 0, kind: SimEventKind::SlotAdvance { slot: 0 } }];
    let strategy = OneShotStrategy;
    let mut engine = BacktestEngine::new(strategy, adapter, portfolio, events);
    engine.set_impact_settings(ImpactSettings { seed: Some(42), noise_bps_mean: 100.0, noise_bps_std: 0.0, emulate_latency_ms: None, extra_fee_bps: 0, slot_ms: Some(400) });
    let _ = engine.run();
    // With 100 bps added noise on top of 50 bps slippage, min_out should be low enough to still execute given large reserves.
    assert_eq!(engine.slippage_rejections.len(), 0);
}
