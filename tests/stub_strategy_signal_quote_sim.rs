use ironcrab::backtest::engine::make_swap;
use ironcrab::backtest::market::CfmPool;
use ironcrab::backtest::types::{
    BacktestStrategy, Portfolio, SimContext, SimEvent, SimEventKind, StrategyDecision,
};
use ironcrab::backtest::{engine::BacktestEngine, market::CfmAdapter};
use std::sync::Mutex;

// Deterministic stub strategy emitting a fixed sequence of swaps on first tick.
struct StubStrategy {
    fired: Mutex<bool>,
}

impl BacktestStrategy for StubStrategy {
    fn on_tick(&self, _ctx: &SimContext) -> StrategyDecision {
        let mut fired = self.fired.lock().unwrap();
        if !*fired {
            *fired = true;
            // Two-hop chain: A->B then B->C, with conservative slippage guards (100 bps)
            StrategyDecision {
                actions: vec![
                    make_swap("P1", "A", "B", 100_000, 100),
                    make_swap("P2", "B", "C", 50_000, 100),
                ],
            }
        } else {
            StrategyDecision { actions: vec![] }
        }
    }
}

fn mk_events(n: u64) -> Vec<SimEvent> {
    (0..n)
        .map(|i| SimEvent {
            ts_ms: i,
            kind: SimEventKind::SlotAdvance { slot: i },
        })
        .collect()
}

#[test]
fn stub_strategy_signal_quote_sim_chain_executes() {
    // Simple market with two pools to enable a chain A->B->C.
    let market = CfmAdapter {
        pools: vec![
            CfmPool {
                pool: "P1".into(),
                base_mint: "A".into(),
                quote_mint: "B".into(),
                base_reserve: 1_000_000,
                quote_reserve: 2_000_000,
                fee_bps: 30,
                tick_spacing: None,
            },
            CfmPool {
                pool: "P2".into(),
                base_mint: "B".into(),
                quote_mint: "C".into(),
                base_reserve: 2_000_000,
                quote_reserve: 3_000_000,
                fee_bps: 30,
                tick_spacing: None,
            },
        ],
    };
    let portfolio = Portfolio::new();
    let events = mk_events(2);
    let strategy = StubStrategy {
        fired: Mutex::new(false),
    };
    let mut engine = BacktestEngine::new(strategy, market, portfolio, events);

    engine.run().expect("engine should run");

    // We expect two actions to have been proposed; depending on reserves and slippage, both should execute.
    // Assert at least one non-rejected execution and no catastrophic failures.
    assert!(!engine.decisions.is_empty());
    let non_rejected: Vec<_> = engine.executions.iter().filter(|r| !r.rejected).collect();
    assert!(
        !non_rejected.is_empty(),
        "expected at least one executed swap"
    );

    // If both swaps executed, C position should be > 0; at minimum, ensure portfolio changed.
    let c_pos = engine
        .portfolio
        .tokens
        .get("C")
        .map(|p| p.amount)
        .unwrap_or_default();
    let a_pos = engine
        .portfolio
        .tokens
        .get("A")
        .map(|p| p.amount)
        .unwrap_or_default();
    // Portfolio should reflect some movement (A decreased or C increased)
    assert!(c_pos > 0.into() || a_pos < 0.into());

    // For all executed swaps with a min_out computed, the actual out must respect it.
    for rec in non_rejected {
        if let (Some(min_out), Some(amount_out)) = (rec.min_out, rec.amount_out) {
            assert!(amount_out >= min_out, "slippage guard violated");
        }
    }
}
