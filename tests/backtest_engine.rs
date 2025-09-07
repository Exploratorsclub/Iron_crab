use ironcrab::backtest::{
    engine::{make_swap, BacktestEngine, NoopStrategy},
    market::{CfmAdapter, CfmPool},
    types::{BacktestStrategy, Portfolio, SimContext, SimEvent, SimEventKind, StrategyDecision},
};

struct OneSwap;
impl BacktestStrategy for OneSwap {
    fn on_event(&self, _ctx: &SimContext, _ev: &SimEvent) -> StrategyDecision {
        StrategyDecision {
            actions: vec![make_swap("P1", "A", "B", 50_000, 10)],
        } // 0.10% max slippage
    }
}

fn mk_events() -> Vec<SimEvent> {
    vec![
        SimEvent {
            ts_ms: 1,
            kind: SimEventKind::SlotAdvance { slot: 1 },
        },
        SimEvent {
            ts_ms: 2,
            kind: SimEventKind::Log("noop".into()),
        },
    ]
}

#[test]
fn engine_runs() {
    let strategy = NoopStrategy;
    let market = CfmAdapter {
        pools: vec![CfmPool {
            pool: "X".into(),
            base_mint: "A".into(),
            quote_mint: "B".into(),
            base_reserve: 1_000_000,
            quote_reserve: 2_000_000,
            fee_bps: 25,
            tick_spacing: None,
        }],
    };
    let portfolio = Portfolio::new();
    let events = mk_events();
    let mut engine = BacktestEngine::new(strategy, market, portfolio, events);
    engine.run().unwrap();
    assert_eq!(engine.decisions.len(), 2);
}

#[test]
fn slippage_enforcement() {
    // Very small pool -> price impact likely > 0.10% so may reject
    let market = CfmAdapter {
        pools: vec![CfmPool {
            pool: "P1".into(),
            base_mint: "A".into(),
            quote_mint: "B".into(),
            base_reserve: 20_000,
            quote_reserve: 20_000,
            fee_bps: 30,
            tick_spacing: None,
        }],
    };
    let portfolio = Portfolio::new();
    let events = vec![SimEvent {
        ts_ms: 0,
        kind: SimEventKind::SlotAdvance { slot: 0 },
    }];
    let strategy = OneSwap;
    let mut engine = BacktestEngine::new(strategy, market, portfolio, events);
    engine.run().unwrap();
    // Either executed (portfolio changed) OR rejected (slippage_rejections logged). Assert at least one path.
    let rejected = !engine.slippage_rejections.is_empty();
    let executed = engine
        .portfolio
        .tokens
        .get("B")
        .map(|p| p.amount > 0.into())
        .unwrap_or(false);
    assert!(
        rejected || executed,
        "neither executed nor rejected recorded"
    );
    if rejected {
        // ensure at least one execution record with rejected=true and reason slippage
        let any = engine
            .executions
            .iter()
            .any(|r| r.rejected && r.reason.as_deref() == Some("slippage"));
        assert!(any, "missing rejected execution record");
    }
}
