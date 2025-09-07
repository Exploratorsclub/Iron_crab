use super::impact::{ImpactSettings, NoiseSampler};
use super::market::{ImpactModel, MarketAdapter};
use super::types::{
    BacktestStrategy, FillInfo, Portfolio, SimContext, SimEvent, SimEventKind, StrategyAction,
    StrategyDecision,
};
use anyhow::Result;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::time::Instant;

fn apply_slippage_min_out(quote_out: u64, slippage_bps: u32) -> u64 {
    if slippage_bps == 0 {
        return quote_out;
    }
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
    pub impact_settings: ImpactSettings,
    noise_sampler: Option<NoiseSampler>,
    slippage_override_bps: Option<u32>,
    // sandbox: circuit breaker state
    pub failure_count: u32,
    pub breaker_open_until: Option<Instant>,
}
impl<S: BacktestStrategy, M: MarketAdapter> BacktestEngine<S, M> {
    pub fn new(strategy: S, market: M, portfolio: Portfolio, events: Vec<SimEvent>) -> Self {
        Self {
            strategy,
            market,
            portfolio,
            events,
            decisions: vec![],
            slippage_rejections: vec![],
            executions: vec![],
            impact_model: None,
            impact_settings: ImpactSettings::default(),
            noise_sampler: None,
            failure_count: 0,
            breaker_open_until: None,
            slippage_override_bps: None,
        }
    }
    pub fn set_impact_model(&mut self, model: Box<dyn ImpactModel + Send + Sync>) {
        self.impact_model = Some(model);
    }
    pub fn set_impact_settings(&mut self, settings: ImpactSettings) {
        self.noise_sampler = Some(NoiseSampler::new(
            settings.seed,
            settings.noise_bps_mean,
            settings.noise_bps_std,
        ));
        self.impact_settings = settings;
    }
    pub fn set_slippage_override_bps(&mut self, bps: Option<u32>) {
        self.slippage_override_bps = bps;
    }
    pub fn run(&mut self) -> Result<()> {
        // Lifecycle: init once at start
        if let Some(first) = self.events.first() {
            let ictx = SimContext {
                portfolio: &self.portfolio,
                time_ms: first.ts_ms,
            };
            self.strategy.init(&ictx);
        }
        for ev in &self.events {
            // Circuit breaker: skip while open
            if let Some(until) = self.breaker_open_until {
                if Instant::now() < until {
                    continue;
                }
                self.breaker_open_until = None; // re-close after cooldown
            }
            let time_ms = ev.ts_ms;
            // Apply market updates from replay events before strategy sees the tick/event
            match &ev.kind {
                SimEventKind::NewPool {
                    pool,
                    base_mint,
                    quote_mint,
                    fee_bps,
                } => {
                    self.market
                        .on_new_pool(pool, base_mint, quote_mint, *fee_bps);
                }
                SimEventKind::CfmPriceUpdate {
                    pool,
                    base_reserve,
                    quote_reserve,
                    fee_bps,
                } => {
                    self.market
                        .on_price_update(pool, *base_reserve, *quote_reserve, *fee_bps);
                }
                _ => {}
            }
            // limit immutable borrow of portfolio to this block
            let decision = {
                let ctx = SimContext {
                    portfolio: &self.portfolio,
                    time_ms,
                };
                // Panic-sandbox strategy calls
                let res = catch_unwind(AssertUnwindSafe(|| {
                    match &ev.kind {
                        SimEventKind::SlotAdvance { .. } => {
                            let d = self.strategy.on_tick(&ctx);
                            if d.actions.is_empty() {
                                // Fallback: if on_tick yields no actions, allow on_event to handle SlotAdvance
                                self.strategy.on_event(&ctx, ev)
                            } else {
                                d
                            }
                        }
                        _ => self.strategy.on_event(&ctx, ev),
                    }
                }));
                match res {
                    Ok(d) => {
                        self.failure_count = 0;
                        d
                    }
                    Err(_) => {
                        // record failure; open circuit if too many
                        self.failure_count = self.failure_count.saturating_add(1);
                        const FAIL_THRESHOLD: u32 = 5;
                        const OPEN_MS: u64 = 5_000;
                        if self.failure_count >= FAIL_THRESHOLD {
                            self.failure_count = 0;
                            self.breaker_open_until =
                                Some(Instant::now() + std::time::Duration::from_millis(OPEN_MS));
                        }
                        StrategyDecision { actions: vec![] }
                    }
                }
            };
            for act in &decision.actions {
                match act {
                    StrategyAction::Swap(a) => {
                        // Pre-quote for slippage enforcement
                        let mut min_out: Option<u64> = None;
                        if a.max_slippage_bps > 0 {
                            let allowed_slip =
                                self.slippage_override_bps.unwrap_or(a.max_slippage_bps);
                            if let Some(q) =
                                self.market
                                    .quote(&a.input_mint, &a.output_mint, a.amount_in)
                            {
                                let base_out = if let Some(ref im) = self.impact_model {
                                    // Use model on reserves for expected out
                                    im.expected_out(
                                        q.in_reserve,
                                        q.out_reserve,
                                        a.amount_in,
                                        q.fee_bps,
                                        q.tick_spacing,
                                    )
                                } else {
                                    q.amount_out
                                };
                                // Apply extra protocol/referral fee bps if configured
                                let extra_fee = self.impact_settings.extra_fee_bps;
                                let mut base_after_extra = base_out;
                                if extra_fee > 0 {
                                    let adj = (base_after_extra as u128)
                                        * (10_000u128 - extra_fee as u128)
                                        / 10_000u128;
                                    base_after_extra = adj as u64;
                                }
                                // Draw stochastic shortfall noise (bps) and add to slippage guard
                                let mut slippage_bps = allowed_slip;
                                if let Some(s) = self.noise_sampler.as_mut() {
                                    slippage_bps = slippage_bps.saturating_add(s.sample_bps());
                                }
                                // Latency penalty: translate emulate_latency_ms into additional bps
                                if let (Some(lat_ms), Some(slot_ms)) = (
                                    self.impact_settings.emulate_latency_ms,
                                    self.impact_settings.slot_ms,
                                ) {
                                    if slot_ms > 0 {
                                        let slots = ((lat_ms + slot_ms - 1) / slot_ms) as u32;
                                        let penalty = (slots.saturating_mul(10)).min(500); // 10 bps per slot, cap 500 bps
                                        slippage_bps = slippage_bps.saturating_add(penalty);
                                    }
                                }
                                let m = apply_slippage_min_out(base_after_extra, slippage_bps);
                                min_out = Some(m);
                                // Store sampled noise in the execution record by reusing min_out Option later (no direct field); we'll re-sample after swap if needed.
                            }
                        }
                        let (iin, mut out) = self.market.apply_swap(a)?;
                        // Apply extra fee and stochastic noise to realized out
                        if self.impact_settings.extra_fee_bps > 0 {
                            let adj = (out as u128)
                                * (10_000u128 - self.impact_settings.extra_fee_bps as u128)
                                / 10_000u128;
                            out = adj as u64;
                        }
                        if let Some(s) = self.noise_sampler.as_mut() {
                            let nb = s.sample_bps();
                            if nb > 0 {
                                let adj = (out as u128) * (10_000u128 - nb as u128) / 10_000u128;
                                out = adj as u64;
                            }
                        }
                        if let Some(m) = min_out {
                            if out < m {
                                // Reject – record & skip portfolio mutation
                                self.slippage_rejections.push(format!(
                                    "swap {}->{} amount_in={} out={} min_out={}",
                                    a.input_mint, a.output_mint, a.amount_in, out, m
                                ));
                                self.executions.push(SwapExecutionRecord {
                                    input_mint: a.input_mint.clone(),
                                    output_mint: a.output_mint.clone(),
                                    amount_in: a.amount_in,
                                    min_out: Some(m),
                                    amount_out: None,
                                    rejected: true,
                                    reason: Some("slippage".into()),
                                });
                                continue;
                            }
                        }
                        self.portfolio
                            .apply_swap(&a.input_mint, &a.output_mint, iin, out);
                        // notify fill to strategy
                        let finfo = FillInfo {
                            ts_ms: time_ms,
                            pool: a.pool.clone(),
                            input_mint: a.input_mint.clone(),
                            output_mint: a.output_mint.clone(),
                            amount_in: iin,
                            amount_out: out,
                        };
                        let ctx_fill = SimContext {
                            portfolio: &self.portfolio,
                            time_ms,
                        };
                        self.strategy.on_fill(&ctx_fill, &finfo);
                        self.executions.push(SwapExecutionRecord {
                            input_mint: a.input_mint.clone(),
                            output_mint: a.output_mint.clone(),
                            amount_in: a.amount_in,
                            min_out,
                            amount_out: Some(out),
                            rejected: false,
                            reason: None,
                        });
                    }
                }
            }
            self.decisions.push(decision);
        }
        // Lifecycle: exit once at end
        if let Some(last) = self.events.last() {
            let ectx = SimContext {
                portfolio: &self.portfolio,
                time_ms: last.ts_ms,
            };
            self.strategy.on_exit(&ectx);
        }
        // Graceful shutdown for persistent Python worker if used
        #[cfg(feature = "python_ipc")]
        {
            // best-effort: if strategy is PyProcStrategy, send Stop
            // We can't downcast generic S; users running via driver construct engine with PyProcStrategy directly.
            // For explicit shutdown, use the on_exit hook to notify the worker.
        }
        Ok(())
    }
}

// Optional: Python Strategy Adapter (IPC JSON) for backtests (no pyo3 needed)
#[cfg(feature = "python_ipc")]
pub mod py_strategy_adapter {
    use super::*;
    use std::io::{BufRead, BufReader, Write};
    use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
    use std::sync::{mpsc, Arc, Mutex};
    use std::time::{Duration, Instant};

    pub struct PyProcStrategy {
        cmd: String,
        args: Vec<String>,
        timeout_ms: u64,
        worker: Arc<PyWorker>,
        breaker: Mutex<(u32, Option<Instant>)>, // (failures, open_until)
    }

    enum WorkerMsg {
        Req {
            json: String,
            expect_reply: bool,
            tx: mpsc::Sender<Result<Option<String>, String>>,
        },
        Stop,
    }

    struct PyWorker {
        tx: mpsc::Sender<WorkerMsg>,
    }

    impl PyWorker {
        fn spawn(cmd: String, args: Vec<String>) -> Arc<Self> {
            let (tx, rx) = mpsc::channel::<WorkerMsg>();
            std::thread::spawn(move || {
                fn start_child(
                    cmd: &str,
                    args: &[String],
                ) -> std::io::Result<(Child, ChildStdin, BufReader<ChildStdout>)> {
                    let mut child = Command::new(cmd)
                        .args(args)
                        .stdin(Stdio::piped())
                        .stdout(Stdio::piped())
                        .spawn()?;
                    let stdin = child.stdin.take().ok_or_else(|| {
                        std::io::Error::new(std::io::ErrorKind::Other, "no stdin")
                    })?;
                    let stdout = child.stdout.take().ok_or_else(|| {
                        std::io::Error::new(std::io::ErrorKind::Other, "no stdout")
                    })?;
                    Ok((child, stdin, BufReader::new(stdout)))
                }
                let mut child_opt: Option<(Child, ChildStdin, BufReader<ChildStdout>)> =
                    start_child(&cmd, &args).ok();
                while let Ok(msg) = rx.recv() {
                    match msg {
                        WorkerMsg::Stop => {
                            if let Some((mut c, _in, _out)) = child_opt.take() {
                                let _ = c.kill();
                            }
                            break;
                        }
                        WorkerMsg::Req {
                            json,
                            expect_reply,
                            tx,
                        } => {
                            // Ensure child exists
                            if child_opt.is_none() {
                                child_opt = start_child(&cmd, &args).ok();
                                crate::metrics::PY_STRAT_RESTARTS_TOTAL
                                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            }
                            let mut ok = false;
                            if let Some((ref mut ch, ref mut stdin, ref mut stdout)) = child_opt {
                                // write line
                                if writeln!(stdin, "{}", json).is_ok() {
                                    let _ = stdin.flush();
                                } else {
                                    let _ = tx.send(Err("write_failed".into()));
                                    continue;
                                }
                                if expect_reply {
                                    let mut line = String::new();
                                    match stdout.read_line(&mut line) {
                                        Ok(0) => {
                                            let _ = tx.send(Err("eof".into()));
                                        }
                                        Ok(_) => {
                                            let _ = tx.send(Ok(Some(line)));
                                            ok = true;
                                        }
                                        Err(e) => {
                                            let _ = tx.send(Err(format!("read_err:{e}")));
                                        }
                                    }
                                } else {
                                    let _ = tx.send(Ok(None));
                                    ok = true;
                                }
                            }
                            if !ok {
                                // attempt restart once
                                if let Some((mut c, _, _)) = child_opt.take() {
                                    let _ = c.kill();
                                }
                                child_opt = start_child(&cmd, &args).ok();
                                crate::metrics::PY_STRAT_RESTARTS_TOTAL
                                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            }
                        }
                    }
                }
            });
            Arc::new(PyWorker { tx })
        }

        fn request(
            &self,
            json: String,
            expect_reply: bool,
            timeout: Duration,
        ) -> Result<Option<String>, ()> {
            let (tx, rx) = mpsc::channel();
            if self
                .tx
                .send(WorkerMsg::Req {
                    json,
                    expect_reply,
                    tx,
                })
                .is_err()
            {
                return Err(());
            }
            match rx.recv_timeout(timeout) {
                Ok(Ok(s)) => Ok(s),
                _ => Err(()),
            }
        }

        fn stop(&self) {
            let _ = self.tx.send(WorkerMsg::Stop);
        }
    }

    impl Drop for PyWorker {
        fn drop(&mut self) {
            let _ = self.tx.send(WorkerMsg::Stop);
        }
    }

    impl PyProcStrategy {
        pub fn new(cmd: impl Into<String>, args: Vec<String>) -> Self {
            let worker = PyWorker::spawn(cmd.into(), args.clone());
            Self {
                cmd: "python".into(),
                args,
                timeout_ms: 500,
                worker,
                breaker: Mutex::new((0, None)),
            }
        }
        /// Convenience: run `python <script_path>`
        pub fn from_script(script_path: impl Into<String>) -> Self {
            let args = vec![script_path.into()];
            let worker = PyWorker::spawn("python".into(), args.clone());
            Self {
                cmd: "python".into(),
                args,
                timeout_ms: 500,
                worker,
                breaker: Mutex::new((0, None)),
            }
        }
        /// Convenience: custom python executable + script + extra args
        pub fn with_python(
            python_exe: impl Into<String>,
            script_path: impl Into<String>,
            extra_args: &[impl AsRef<str>],
        ) -> Self {
            let mut args: Vec<String> = Vec::with_capacity(1 + extra_args.len());
            args.push(script_path.into());
            for a in extra_args {
                args.push(a.as_ref().to_string());
            }
            let cmd = python_exe.into();
            let worker = PyWorker::spawn(cmd.clone(), args.clone());
            Self {
                cmd,
                args,
                timeout_ms: 500,
                worker,
                breaker: Mutex::new((0, None)),
            }
        }
        /// Configure per-call timeout in milliseconds (default 500ms)
        pub fn with_timeout_ms(mut self, timeout_ms: u64) -> Self {
            self.timeout_ms = timeout_ms;
            self
        }

        /// Explicitly stop the worker (best-effort). Usually not needed due to Drop.
        pub fn shutdown(&self) {
            self.worker.stop();
        }
    }

    impl BacktestStrategy for PyProcStrategy {
        fn init(&self, _ctx: &SimContext) { /* no-op: stateless per-call process */
        }

        fn on_tick(&self, ctx: &SimContext) -> StrategyDecision {
            // Circuit breaker
            let mut br = self.breaker.lock().unwrap();
            if let Some(until) = br.1 {
                if Instant::now() < until {
                    return StrategyDecision { actions: vec![] };
                } else {
                    br.1 = None;
                }
            }
            drop(br);
            let req = serde_json::json!({"kind":"tick","time_ms": ctx.time_ms});
            let res = self.worker.request(
                req.to_string(),
                true,
                Duration::from_millis(self.timeout_ms),
            );
            match res {
                Ok(Some(s)) => {
                    if let Ok(dec) = serde_json::from_str::<StrategyDecision>(&s) {
                        self.breaker.lock().unwrap().0 = 0;
                        return dec;
                    }
                    let mut br = self.breaker.lock().unwrap();
                    br.0 = br.0.saturating_add(1);
                    crate::metrics::PY_STRAT_FAILS_TOTAL
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if br.0 >= 5 {
                        br.0 = 0;
                        br.1 = Some(Instant::now() + Duration::from_millis(5_000));
                        crate::metrics::PY_STRAT_CIRCUIT_OPENS_TOTAL
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    StrategyDecision { actions: vec![] }
                }
                _ => {
                    let mut br = self.breaker.lock().unwrap();
                    br.0 = br.0.saturating_add(1);
                    crate::metrics::PY_STRAT_TIMEOUTS_TOTAL
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if br.0 >= 5 {
                        br.0 = 0;
                        br.1 = Some(Instant::now() + Duration::from_millis(5_000));
                        crate::metrics::PY_STRAT_CIRCUIT_OPENS_TOTAL
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    StrategyDecision { actions: vec![] }
                }
            }
        }

        fn on_event(&self, _ctx: &SimContext, event: &SimEvent) -> StrategyDecision {
            // Circuit breaker
            let mut br = self.breaker.lock().unwrap();
            if let Some(until) = br.1 {
                if Instant::now() < until {
                    return StrategyDecision { actions: vec![] };
                } else {
                    br.1 = None;
                }
            }
            drop(br);
            let ev_json = match serde_json::to_string(event) {
                Ok(s) => s,
                Err(_) => return StrategyDecision { actions: vec![] },
            };
            let res = self
                .worker
                .request(ev_json, true, Duration::from_millis(self.timeout_ms));
            match res {
                Ok(Some(s)) => {
                    if let Ok(dec) = serde_json::from_str::<StrategyDecision>(&s) {
                        self.breaker.lock().unwrap().0 = 0;
                        return dec;
                    }
                    let mut br = self.breaker.lock().unwrap();
                    br.0 = br.0.saturating_add(1);
                    crate::metrics::PY_STRAT_FAILS_TOTAL
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if br.0 >= 5 {
                        br.0 = 0;
                        br.1 = Some(Instant::now() + Duration::from_millis(5_000));
                    }
                    StrategyDecision { actions: vec![] }
                }
                _ => {
                    let mut br = self.breaker.lock().unwrap();
                    br.0 = br.0.saturating_add(1);
                    crate::metrics::PY_STRAT_TIMEOUTS_TOTAL
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if br.0 >= 5 {
                        br.0 = 0;
                        br.1 = Some(Instant::now() + Duration::from_millis(5_000));
                    }
                    StrategyDecision { actions: vec![] }
                }
            }
        }

        fn on_fill(&self, ctx: &SimContext, fill: &FillInfo) {
            let req = serde_json::json!({"kind":"fill","time_ms": ctx.time_ms, "fill": fill});
            let _ = self.worker.request(
                req.to_string(),
                false,
                Duration::from_millis(self.timeout_ms),
            );
        }
        fn on_exit(&self, ctx: &SimContext) {
            let req = serde_json::json!({"kind":"exit","time_ms": ctx.time_ms});
            let _ = self.worker.request(
                req.to_string(),
                false,
                Duration::from_millis(self.timeout_ms),
            );
        }
    }

    impl Drop for PyProcStrategy {
        fn drop(&mut self) {
            self.worker.stop();
        }
    }
}

pub struct NoopStrategy;
impl BacktestStrategy for NoopStrategy {
    fn on_event(&self, _ctx: &SimContext, _ev: &SimEvent) -> StrategyDecision {
        StrategyDecision { actions: vec![] }
    }
}

pub fn make_swap(
    pool: &str,
    input_mint: &str,
    output_mint: &str,
    amount_in: u64,
    max_slippage_bps: u32,
) -> StrategyAction {
    use super::types::ActionSwap;
    StrategyAction::Swap(ActionSwap {
        pool: pool.into(),
        input_mint: input_mint.into(),
        output_mint: output_mint.into(),
        amount_in,
        max_slippage_bps,
    })
}
