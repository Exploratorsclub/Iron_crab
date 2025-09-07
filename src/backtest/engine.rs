use anyhow::Result;
use super::types::{SimEvent,Portfolio,StrategyDecision,BacktestStrategy,StrategyAction,SimContext};
use super::market::{MarketAdapter, ImpactModel};

fn apply_slippage_min_out(quote_out: u64, slippage_bps: u32) -> u64 {
    if slippage_bps == 0 { return quote_out; }
    let keep = 10_000u64.saturating_sub(slippage_bps as u64);
    (quote_out as u128 * keep as u128 / 10_000u128) as u64
}

#[derive(Debug, Clone)]
pub struct SwapExecutionRecord {
    pub input_mint: String,
    pub output_mint: String,
    pub amount_in: u64,
    pub min_out: Option<u64>,
    pub amount_out: Option<u64>, // None if rejected
    pub rejected: bool,
    pub reason: Option<String>,
}

pub struct BacktestEngine<S: BacktestStrategy, M: MarketAdapter> {
    pub strategy: S,
    pub market: M,
    pub portfolio: Portfolio,
    pub events: Vec<SimEvent>,
    pub decisions: Vec<StrategyDecision>,
    pub slippage_rejections: Vec<String>,
    pub executions: Vec<SwapExecutionRecord>,
    pub impact_model: Option<Box<dyn ImpactModel + Send + Sync>>, // optional pluggable model
}
impl<S: BacktestStrategy, M: MarketAdapter> BacktestEngine<S,M> {
    pub fn new(strategy:S, market:M, portfolio:Portfolio, events:Vec<SimEvent>) -> Self { Self { strategy, market, portfolio, events, decisions: vec![], slippage_rejections: vec![], executions: vec![], impact_model: None } }
    pub fn set_impact_model(&mut self, model: Box<dyn ImpactModel + Send + Sync>) { self.impact_model = Some(model); }
    pub fn run(&mut self) -> Result<()> {
        for ev in &self.events {
            let ctx = SimContext { portfolio: &self.portfolio, time_ms: ev.ts_ms };
            let decision = self.strategy.on_event(&ctx, ev);
            for act in &decision.actions {
                match act {
                StrategyAction::Swap(a) => {
                    // Pre-quote for slippage enforcement
                    let mut min_out: Option<u64> = None;
                    if a.max_slippage_bps > 0 {
                        if let Some(q) = self.market.quote(&a.input_mint, &a.output_mint, a.amount_in) {
                            let base_out = if let Some(ref im) = self.impact_model {
                                // Use model on reserves for expected out
                                im.expected_out(q.in_reserve, q.out_reserve, a.amount_in, q.fee_bps)
                            } else { q.amount_out };
                            let m = apply_slippage_min_out(base_out, a.max_slippage_bps);
                            min_out = Some(m);
                        }
                    }
                    let (iin,out) = self.market.apply_swap(a)?;
                    if let Some(m) = min_out {
                        if out < m {
                            // Reject – record & skip portfolio mutation
                            self.slippage_rejections.push(format!("swap {}->{} amount_in={} out={} min_out={}", a.input_mint, a.output_mint, a.amount_in, out, m));
                            self.executions.push(SwapExecutionRecord { input_mint: a.input_mint.clone(), output_mint: a.output_mint.clone(), amount_in: a.amount_in, min_out: Some(m), amount_out: None, rejected: true, reason: Some("slippage".into()) });
                            continue;
                        }
                    }
                    self.portfolio.apply_swap(&a.input_mint,&a.output_mint,iin,out);
                    self.executions.push(SwapExecutionRecord { input_mint: a.input_mint.clone(), output_mint: a.output_mint.clone(), amount_in: a.amount_in, min_out, amount_out: Some(out), rejected: false, reason: None });
                }
                }
            }
            self.decisions.push(decision);
        }
        Ok(())
    }
}

// Optional: Python Strategy Adapter (IPC JSON) for backtests
#[cfg(feature = "python")]
pub mod py_strategy_adapter {
    use super::*;
    use anyhow::{Result, anyhow};
    use std::process::{Command, Stdio};
    use std::io::Write;

    pub struct PyProcStrategy {
        pub cmd: String,
        pub args: Vec<String>,
    }

    impl PyProcStrategy {
        pub fn new(cmd: impl Into<String>, args: Vec<String>) -> Self { Self { cmd: cmd.into(), args } }
        /// Convenience: run `python <script_path>`
        pub fn from_script(script_path: impl Into<String>) -> Self {
            Self { cmd: "python".into(), args: vec![script_path.into()] }
        }
        /// Convenience: custom python executable + script + extra args
        pub fn with_python(python_exe: impl Into<String>, script_path: impl Into<String>, extra_args: &[impl AsRef<str>]) -> Self {
            let mut args: Vec<String> = Vec::with_capacity(1 + extra_args.len());
            args.push(script_path.into());
            for a in extra_args { args.push(a.as_ref().to_string()); }
            Self { cmd: python_exe.into(), args }
        }
    }

    impl BacktestStrategy for PyProcStrategy {
        fn on_event(&self, _ctx: &SimContext, event: &SimEvent) -> StrategyDecision {
            // Send event JSON on stdin; read a single JSON line of StrategyDecision from stdout
            let mut child = match Command::new(&self.cmd)
                .args(&self.args)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .spawn() {
                Ok(c) => c,
                Err(_) => return StrategyDecision { actions: vec![] },
            };
            let ev_json = match serde_json::to_string(event) { Ok(s) => s, Err(_) => return StrategyDecision { actions: vec![] } };
            if let Some(mut stdin) = child.stdin.take() {
                let _ = writeln!(stdin, "{}", ev_json);
            }
            let out = match child.wait_with_output() { Ok(o) => o, Err(_) => return StrategyDecision { actions: vec![] } };
            if !out.status.success() { return StrategyDecision { actions: vec![] }; }
            match serde_json::from_slice::<StrategyDecision>(&out.stdout) { Ok(d) => d, Err(_) => StrategyDecision { actions: vec![] } }
        }
    }
}

pub struct NoopStrategy;
impl BacktestStrategy for NoopStrategy { fn on_event(&self, _ctx:&SimContext, _ev:&SimEvent) -> StrategyDecision { StrategyDecision { actions: vec![] } } }

pub fn make_swap(pool:&str, input_mint:&str, output_mint:&str, amount_in:u64, max_slippage_bps:u32) -> StrategyAction { use super::types::ActionSwap; StrategyAction::Swap(ActionSwap { pool: pool.into(), input_mint: input_mint.into(), output_mint: output_mint.into(), amount_in, max_slippage_bps }) }
