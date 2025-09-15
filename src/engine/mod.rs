use anyhow::Result;
use rust_decimal::prelude::ToPrimitive;
use std::{io::Write, sync::Arc, time::Duration};
use tokio::time::{interval, Duration as TokioDuration};
use tracing::{debug, error, info, warn, Instrument as _};

use crate::config::{Config, StrategyDef};
use crate::solana::arbitrage::ArbitrageEngine;
use crate::solana::dex::orca::ORCA_WHIRLPOOL_PROGRAM;
use crate::solana::dex::raydium::RAYDIUM_AMM_V4;
use crate::solana::dex::router::Router;
use crate::solana::dex::{orca::Orca, raydium::Raydium, Dex};
use crate::solana::rpc::SolanaRpc;
use crate::solana::sniper::{run_sniper, SniperCfg};
use crate::types::{Side, TradeIntent};
use crate::wallet::Treasury;
use solana_sdk::transaction::Transaction;

pub mod allocator;
#[cfg(feature = "python")]
pub mod py_strategy;
pub mod strategy;

use allocator::Allocator;
use strategy::Strategy;

#[derive(Clone)]
pub struct EngineContext {
    pub cfg: Arc<Config>,
    pub rpc: Arc<SolanaRpc>,
    pub treasury: Treasury,
}

pub struct Engine {
    ctx: Arc<EngineContext>,
    allocator: Arc<Allocator>,
    strategies: Vec<Arc<dyn Strategy>>, // pro Markt konfigurierbar
    router: Router,                     // DEX router for best execution
}

impl Engine {
    pub async fn new(cfg: Arc<Config>, rpc: Arc<SolanaRpc>, treasury: Treasury) -> Result<Self> {
        let allocator = Arc::new(Allocator::new(cfg.allocator.clone()));
        let ctx = Arc::new(EngineContext {
            cfg: cfg.clone(),
            rpc: rpc.clone(),
            treasury,
        });

        // Initialize DEX connectors for router
        let raydium = Arc::new(Raydium::new(rpc.clone()));
        let orca = Arc::new(Orca::new(rpc.clone()));
        let router = Router::new(vec![raydium, orca]);

        Ok(Self {
            ctx,
            allocator,
            strategies: vec![],
            router,
        })
    }

    pub async fn build_strategies(&mut self) -> Result<()> {
        for m in &self.ctx.cfg.markets {
            let sdef: &StrategyDef = self
                .ctx
                .cfg
                .strategies
                .get(&m.strategy)
                .ok_or_else(|| anyhow::anyhow!("unknown strategy {}", m.strategy))?;
            match sdef.kind.as_str() {
                "rust" => {
                    // Platzhalter: dummy Strategie pro Markt
                    let s = Arc::new(DummyRustStrategy {
                        name: format!("{}-{}", m.name, m.strategy),
                    });
                    self.strategies.push(s);
                }
                "python" => {
                    #[cfg(feature = "python")]
                    {
                        use crate::engine::py_strategy::py::PyStrategy;
                        let module = sdef
                            .module
                            .clone()
                            .ok_or_else(|| anyhow::anyhow!("python strategy needs module"))?;
                        let class = sdef
                            .class
                            .clone()
                            .ok_or_else(|| anyhow::anyhow!("python strategy needs class"))?;
                        let params = serde_json::to_value(&sdef.params)?;
                        let s = PyStrategy::new(m.strategy.clone(), module, class, params).await?;
                        self.strategies.push(Arc::new(s));
                    }
                    #[cfg(not(feature = "python"))]
                    {
                        warn!("python feature not enabled – skipping {}", m.strategy);
                    }
                }
                other => warn!("unknown strategy kind: {}", other),
            }
        }
        Ok(())
    }

    pub async fn run(&self) -> Result<()> {
        // DEX program log
        tracing::debug!(raydium = %RAYDIUM_AMM_V4, orca = %ORCA_WHIRLPOOL_PROGRAM, "DEX program IDs loaded");

        // Strategy init hooks (best-effort)
        for s in &self.strategies {
            if let Err(e) = s.init(self.ctx.clone()) {
                tracing::warn!(?e, name = s.name(), "strategy init failed");
            }
        }

        // Rebalancing Loop (mit RPC-Referenz)
        let ctx = self.ctx.clone();
        let alloc = self.allocator.clone();
        let markets = ctx.cfg.markets.clone();
        tokio::spawn(async move {
            let mut iv = interval(Duration::from_secs(ctx.cfg.allocator.rebalance_secs));
            loop {
                iv.tick().await;
                if let Err(e) = alloc.rebalance(&ctx.treasury, &markets, &ctx.rpc).await {
                    tracing::error!(?e, "rebalance failed");
                }
            }
        });

        // DEX Connectors: Periodische Pool-Refreshs (Skeleton)
        {
            let rpc = self.ctx.rpc.clone();
            tokio::spawn(async move {
                let rdx = Raydium::new(rpc.clone());
                let orc = Orca::new(rpc.clone());
                let mut iv = interval(Duration::from_secs(10));
                loop {
                    iv.tick().await;
                    if let Err(e) = rdx.refresh_pools().await {
                        tracing::debug!(?e, "raydium refresh");
                    }
                    if let Err(e) = orc.refresh_pools().await {
                        tracing::debug!(?e, "orca refresh");
                    }
                }
            });
        }

        // Leichter Arbitrage-Scan-Loop (nur Logs; verwendet Struct & Methoden)
        {
            let rpc = self.ctx.rpc.clone();
            let cfg_pairs = self.ctx.cfg.arbitrage.clone();
            tokio::spawn(async move {
                let ray = Arc::new(Raydium::new(rpc.clone()));
                let orc = Arc::new(Orca::new(rpc.clone()));
                let arb = ArbitrageEngine::new(rpc, vec![ray.clone(), orc.clone()]);
                let interval_ms = cfg_pairs
                    .as_ref()
                    .and_then(|c| c.interval_ms)
                    .unwrap_or(2000);
                let pairs = cfg_pairs.map(|c| c.pairs).unwrap_or_default();
                let mut iv = interval(Duration::from_millis(interval_ms));
                loop {
                    iv.tick().await;
                    for p in &pairs {
                        if let Ok(Some(edge)) =
                            arb.best_edge(&p.in_mint, &p.out_mint, p.ui_amount).await
                        {
                            tracing::info!(?edge, pair = %format!("{}->{}", p.in_mint, p.out_mint), "arb candidate");
                        }
                    }
                }
            });
        }

        // Sniper-Loop (Stub) – nutzt SniperCfg/run_sniper
        if let Some(sn_cfg) = self.ctx.cfg.sniper.clone() {
            let rpc_clone = self.ctx.rpc.clone();
            // Instantiate shared DEX connectors for sniper (lightweight additional instances)
            let raydium_ref = Arc::new(Raydium::new(rpc_clone.clone()));
            let orca_ref = Arc::new(Orca::new(rpc_clone.clone()));
            let treasury_arc = Arc::new(self.ctx.treasury.clone());
            tokio::spawn(async move {
                let cfg: SniperCfg = (&sn_cfg).into();
                if let Err(e) = run_sniper(
                    rpc_clone,
                    cfg,
                    Some(raydium_ref),
                    Some(orca_ref),
                    treasury_arc,
                )
                .await
                {
                    tracing::warn!(?e, "sniper exited");
                }
            });
        }

        // Strategy Tick Loop
        let mut iv = interval(Duration::from_millis(600));
        // Simple per-strategy circuit breakers (local state)
        struct Circuit {
            failures: u32,
            opened_until: Option<std::time::Instant>,
        }
        let mut circuits: Vec<Circuit> = self
            .strategies
            .iter()
            .map(|_| Circuit {
                failures: 0,
                opened_until: None,
            })
            .collect();
        const FAIL_THRESHOLD: u32 = 5;
        const OPEN_MS: u64 = 5_000;
        loop {
            iv.tick().await;
            crate::metrics::record_activity();
            for (idx, s) in self.strategies.iter().enumerate() {
                // circuit open?
                if let Some(until) = circuits[idx].opened_until {
                    if std::time::Instant::now() < until {
                        continue;
                    }
                    circuits[idx].opened_until = None; // half-open reset
                }
                // per-tick timeout budget
                let span = tracing::info_span!("strategy_tick", name = s.name());
                let s_arc = s.clone();
                let ctx = self.ctx.clone();
                let handle = tokio::spawn(
                    async move { Strategy::on_tick(&*s_arc, ctx).await }.instrument(span),
                );
                match tokio::time::timeout(TokioDuration::from_millis(500), handle).await {
                    Err(_) => {
                        // timeout
                        tracing::warn!(name = s.name(), "strategy tick timed out");
                        crate::metrics::STRATEGY_TICK_TIMEOUTS_TOTAL
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        circuits[idx].failures += 1;
                    }
                    Ok(Err(join_err)) => {
                        // panic
                        tracing::warn!(name = s.name(), panic = %join_err, "strategy tick panicked");
                        crate::metrics::STRATEGY_TICK_PANICS_TOTAL
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        circuits[idx].failures += 1;
                    }
                    Ok(Ok(Err(e))) => {
                        // returned error
                        tracing::warn!(?e, name = s.name(), "strategy tick error");
                        circuits[idx].failures += 1;
                    }
                    Ok(Ok(Ok(intents))) => {
                        // success
                        circuits[idx].failures = 0;
                        for ti in intents {
                            self.execute(ti).await?;
                        }
                    }
                }
                if circuits[idx].failures >= FAIL_THRESHOLD {
                    circuits[idx].failures = 0;
                    circuits[idx].opened_until =
                        Some(std::time::Instant::now() + TokioDuration::from_millis(OPEN_MS));
                    crate::metrics::STRATEGY_CIRCUIT_OPENS_TOTAL
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    tracing::warn!(
                        name = s.name(),
                        open_ms = OPEN_MS,
                        "strategy circuit opened due to repeated failures"
                    );
                }
            }
        }
    }

    pub async fn execute(&self, intent: TradeIntent) -> Result<()> {
        let start_time = std::time::Instant::now();
        info!(?intent, "executing trade intent");

        // Track execution attempt
        crate::metrics::STRATEGY_EXECUTIONS_TOTAL
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Convert TradeIntent to routing parameters
        let (input_mint, output_mint, amount_in) = match intent.side {
            Side::Buy => {
                // Buy: spend quote token to get base token
                let amount_in_raw = self.convert_ui_amount_to_raw(
                    &intent.quote.mint,
                    intent.amount.ui,
                    intent.quote.decimals,
                )?;
                (
                    intent.quote.mint.clone(),
                    intent.base.mint.clone(),
                    amount_in_raw,
                )
            }
            Side::Sell => {
                // Sell: spend base token to get quote token
                let amount_in_raw = self.convert_ui_amount_to_raw(
                    &intent.base.mint,
                    intent.amount.ui,
                    intent.base.decimals,
                )?;
                (
                    intent.base.mint.clone(),
                    intent.quote.mint.clone(),
                    amount_in_raw,
                )
            }
        };

        debug!(
            input_mint = %input_mint,
            output_mint = %output_mint,
            amount_in = amount_in,
            slippage_bps = intent.max_slippage_bps,
            "routing trade intent"
        );

        // Step 1: Get best quote from router
        let route_quote = match self
            .router
            .best_quote_exact_in(&input_mint, &output_mint, amount_in)
            .await?
        {
            Some(rq) => rq,
            None => {
                warn!("no liquidity found for trade intent");
                crate::metrics::STRATEGY_EXECUTION_FAILURES_TOTAL
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return Err(anyhow::anyhow!(
                    "No liquidity available for {} -> {}",
                    input_mint,
                    output_mint
                ));
            }
        };

        let dex = &self.router.dexs()[route_quote.dex_index];
        debug!(
            dex_index = route_quote.dex_index,
            expected_out = route_quote.quote.amount_out,
            price_impact_bps = route_quote.quote.price_impact_bps,
            "selected best route"
        );

        // Step 2: Build swap plan with compute budget
        let compute_unit_limit = Some(400_000u32); // Default CU limit
        let compute_unit_price = Some(1_000u64); // 1000 micro lamports per CU

        // Calculate min_out with slippage
        let min_out = self.apply_slippage(route_quote.quote.amount_out, intent.max_slippage_bps);

        // Step 3: Build swap instructions
        let mut instructions = Vec::new();

        // Add compute budget instructions using helper
        if let Some(limit) = compute_unit_limit {
            instructions.push(crate::solana::compute_budget_helper::set_compute_unit_limit(limit));
        }
        if let Some(price) = compute_unit_price {
            instructions.push(crate::solana::compute_budget_helper::set_compute_unit_price(price));
        }

        // Add swap instructions from DEX
        let swap_ixs = dex.build_swap_ix(&input_mint, &output_mint, amount_in, min_out)?;
        instructions.extend(swap_ixs);

        // Step 4: Build and sign transaction
        let recent_blockhash = self.ctx.rpc.get_latest_blockhash_retry().await?;
        let mut transaction =
            Transaction::new_with_payer(&instructions, Some(&self.ctx.treasury.pubkey()));
        transaction.sign(&[self.ctx.treasury.signer_ref()], recent_blockhash);

        // Step 5: Send transaction with retry logic
        let signature = self.send_transaction_with_retry(&transaction).await?;

        // Step 6: Update metrics and CSV logs
        let execution_time = start_time.elapsed();
        self.update_execution_metrics(&intent, &route_quote.quote, execution_time, true)
            .await;
        self.log_trade_to_csv(&intent, &route_quote.quote, &signature, true)
            .await?;

        info!(
            signature = %signature,
            execution_time_ms = execution_time.as_millis(),
            expected_out = route_quote.quote.amount_out,
            min_out = min_out,
            "trade intent executed successfully"
        );

        Ok(())
    }

    fn convert_ui_amount_to_raw(
        &self,
        _mint: &str,
        ui_amount: rust_decimal::Decimal,
        decimals: u8,
    ) -> Result<u64> {
        let multiplier = 10u64.pow(decimals as u32);
        let raw_amount = ui_amount * rust_decimal::Decimal::from(multiplier);
        Ok(raw_amount
            .to_u64()
            .ok_or_else(|| anyhow::anyhow!("Amount overflow"))?)
    }

    fn apply_slippage(&self, amount_out: u64, slippage_bps: u32) -> u64 {
        let slippage_factor = 10000u64.saturating_sub(slippage_bps as u64);
        (amount_out as u128 * slippage_factor as u128 / 10000u128) as u64
    }

    async fn send_transaction_with_retry(
        &self,
        transaction: &Transaction,
    ) -> Result<solana_sdk::signature::Signature> {
        const MAX_RETRIES: u32 = 3;
        const RETRY_DELAY_MS: u64 = 1000;

        for attempt in 1..=MAX_RETRIES {
            match self
                .ctx
                .rpc
                .rpc
                .send_and_confirm_transaction(transaction)
                .await
            {
                Ok(signature) => {
                    debug!(signature = %signature, attempt = attempt, "transaction sent successfully");
                    return Ok(signature);
                }
                Err(e) => {
                    warn!(
                        error = %e,
                        attempt = attempt,
                        max_retries = MAX_RETRIES,
                        "transaction send failed"
                    );

                    if attempt < MAX_RETRIES {
                        tokio::time::sleep(Duration::from_millis(RETRY_DELAY_MS * attempt as u64))
                            .await;
                    } else {
                        crate::metrics::STRATEGY_EXECUTION_FAILURES_TOTAL
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        return Err(anyhow::anyhow!(
                            "Transaction failed after {} retries: {}",
                            MAX_RETRIES,
                            e
                        ));
                    }
                }
            }
        }

        unreachable!()
    }

    async fn update_execution_metrics(
        &self,
        intent: &TradeIntent,
        quote: &crate::solana::dex::Quote,
        execution_time: Duration,
        success: bool,
    ) {
        // Record execution latency
        crate::metrics::record_swap_latency_duration(execution_time);

        // Update execution counters
        if success {
            crate::metrics::STRATEGY_EXECUTION_SUCCESSES_TOTAL
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        } else {
            crate::metrics::STRATEGY_EXECUTION_FAILURES_TOTAL
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        // Record price impact and slippage
        crate::metrics::record_price_impact(quote.price_impact_bps as f64);
        crate::metrics::record_slippage(intent.max_slippage_bps as f64);
    }

    async fn log_trade_to_csv(
        &self,
        intent: &TradeIntent,
        quote: &crate::solana::dex::Quote,
        signature: &solana_sdk::signature::Signature,
        success: bool,
    ) -> Result<()> {
        let timestamp = chrono::Utc::now()
            .format("%Y-%m-%d %H:%M:%S%.3f")
            .to_string();
        let side_str = match intent.side {
            Side::Buy => "BUY",
            Side::Sell => "SELL",
        };

        let line = format!(
            "{},{},{},{},{},{},{},{},{},{},{},{}",
            timestamp,
            intent.market,
            format!("{}/{}", intent.base.symbol, intent.quote.symbol),
            side_str,
            intent.amount.ui,
            quote.amount_out,
            intent.max_slippage_bps,
            quote.price_impact_bps,
            quote.fee_bps,
            signature,
            if success { "SUCCESS" } else { "FAILED" },
            quote.route.join("|")
        );

        self.append_trade_record(&line, false).await;
        Ok(())
    }

    async fn append_trade_record(&self, line: &str, include_header: bool) {
        // Use the same CSV logging approach as sniper
        static TRADE_LOG_LOCK: once_cell::sync::Lazy<std::sync::Mutex<()>> =
            once_cell::sync::Lazy::new(|| std::sync::Mutex::new(()));
        let _g = TRADE_LOG_LOCK.lock().unwrap();
        let log_dir =
            std::env::var("IRONCRAB_TRADE_LOG_DIR").unwrap_or_else(|_| "trade_logs".to_string());

        if let Err(e) = std::fs::create_dir_all(&log_dir) {
            error!("Failed to create trade log directory: {}", e);
            return;
        }

        let date = chrono::Utc::now().format("%Y%m%d").to_string();
        let dir = std::path::Path::new(&log_dir);
        let file_path = dir.join(format!("trades-{}.csv", date));

        let mut options = std::fs::OpenOptions::new();
        options.create(true).append(true);

        if let Ok(mut file) = options.open(&file_path) {
            if include_header || !file_path.exists() {
                let header = "timestamp,market,pair,side,amount_ui,amount_out,slippage_bps,price_impact_bps,fee_bps,signature,status,route\n";
                let _ = file.write_all(header.as_bytes());
            }

            let _ = writeln!(file, "{}", line);
        } else {
            error!("Failed to open trade log file: {}", file_path.display());
        }
    }
}

struct DummyRustStrategy {
    name: String,
}
#[async_trait::async_trait]
impl Strategy for DummyRustStrategy {
    fn name(&self) -> &str {
        &self.name
    }
    async fn on_tick(&self, _ctx: Arc<EngineContext>) -> anyhow::Result<Vec<TradeIntent>> {
        Ok(vec![]) // keine Trades – nur Vorlage
    }
}
