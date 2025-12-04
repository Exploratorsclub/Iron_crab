use anyhow::Result;
use rust_decimal::prelude::ToPrimitive;
use std::{io::Write, sync::Arc, time::Duration};
use tokio::time::{interval, Duration as TokioDuration};
use tracing::{debug, error, info, warn, Instrument as _};

use crate::config::ArbPairCfg;
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
    #[allow(dead_code)]
    orca: Arc<Orca>, // Direct reference for prefetching (used in background tasks)
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

        // Initialize Orca with optional persistent cache
        let orca_cache_path = if cfg.orca.enable_reserve_cache {
            cfg.orca
                .cache_path
                .clone()
                .or_else(|| Some("orca_reserves.db".to_string()))
        } else {
            None
        };
        let orca = Arc::new(Orca::new_with_cache(rpc.clone(), orca_cache_path));

        let router = Router::new(vec![raydium, orca.clone()]);

        let engine = Self {
            ctx,
            allocator,
            strategies: vec![],
            router,
            orca: orca.clone(),
        };

        // Prefetch top pools if configured
        if cfg.orca.enable_reserve_cache {
            let prefetch_limit = cfg.orca.prefetch_top_pools.unwrap_or(100);
            tracing::info!(prefetch_limit, "orca prefetching top pools in background");
            let orca_prefetch = orca.clone();
            tokio::spawn(async move {
                if let Err(e) = orca_prefetch.prefetch_top_pools(prefetch_limit).await {
                    tracing::warn!(?e, "orca prefetch failed");
                }
            });
        }

        Ok(engine)
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
                    // Beispiel-Rust-Strategie mit konfigurierbaren Parametern
                    let params: SampleRustStrategyCfg =
                        sdef.params.clone().try_into().unwrap_or_default();
                    let s = Arc::new(SampleRustStrategy::new(
                        format!("{}-{}", m.name, m.strategy),
                        params,
                    ));
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

        // DEX Connectors: Periodische Pool-Refreshs (honor config flags)
        {
            let rpc = self.ctx.rpc.clone();
            let cfg = self.ctx.cfg.clone();
            tokio::spawn(async move {
                let rdx = Raydium::new(rpc.clone());
                let orc = Orca::new(rpc.clone());
                let mut iv = interval(Duration::from_secs(10));
                loop {
                    iv.tick().await;
                    // Read discovery flags if present; default to true when unspecified
                    let (use_ray, use_orc) = if let Some(arb) = &cfg.arbitrage {
                        if let Some(disc) = &arb.discovery {
                            (
                                disc.enable_raydium.unwrap_or(true),
                                disc.enable_orca.unwrap_or(true),
                            )
                        } else {
                            (true, true)
                        }
                    } else {
                        (true, true)
                    };

                    if use_ray {
                        match rdx.refresh_pools().await {
                            Ok(()) => {
                                // Emit an info line similar to Orca for visibility
                                let total = rdx.snapshots().len();
                                tracing::info!(
                                    message = "raydium.refresh_pools() done",
                                    added = total,
                                    total = total,
                                    target = "ironcrab::solana::dex::raydium"
                                );
                            }
                            Err(e) => tracing::warn!(?e, "raydium refresh failed"),
                        }
                    }

                    if use_orc {
                        if let Err(e) = orc.refresh_pools().await {
                            tracing::warn!(?e, "orca refresh failed");
                        } else {
                            let total = orc.pools_snapshot().len();
                            tracing::info!(
                                message = "orca.refresh_pools() done",
                                added = total,
                                total = total,
                                target = "ironcrab::solana::dex::orca"
                            );
                        }
                    }
                }
            });
        }

        // Leichter Arbitrage-Scan-Loop (mit optionaler Auto-Discovery)
        {
            let rpc = self.ctx.rpc.clone();
            let cfg_pairs = self.ctx.cfg.arbitrage.clone();
            // Shared buffer for dynamically discovered pairs
            let discovered: Arc<parking_lot::RwLock<Vec<ArbPairCfg>>> =
                Arc::new(parking_lot::RwLock::new(Vec::new()));

            // Optional discovery loop
            if let Some(ref arb_cfg) = cfg_pairs {
                if let Some(ref disc) = arb_cfg.discovery {
                    if disc.enable {
                        let disc_cfg = disc.clone();
                        let rpc_d = rpc.clone();
                        let discovered_d = discovered.clone();
                        tokio::spawn(async move {
                            let ray = Raydium::new(rpc_d.clone());
                            let orc = Orca::new(rpc_d.clone());
                            let mut iv = interval(std::time::Duration::from_secs(
                                disc_cfg.interval_secs.unwrap_or(30).max(1),
                            ));
                            loop {
                                iv.tick().await;
                                // Refresh pools best-effort
                                let use_ray = disc_cfg.enable_raydium.unwrap_or(true);
                                let use_orc = disc_cfg.enable_orca.unwrap_or(true);
                                if use_ray {
                                    let _ = ray.refresh_pools().await;
                                }
                                if use_orc {
                                    let _ = orc.refresh_pools().await;
                                }
                                // Build candidates
                                let mut pairs: Vec<(String, String, f64, String)> = Vec::new();
                                // Helper to push candidate edges (both directions)
                                let push_pair =
                                    |pairs: &mut Vec<(String, String, f64, String)>,
                                     a: &str,
                                     b: &str,
                                     liq: f64,
                                     dex: &str| {
                                        pairs.push((
                                            a.to_string(),
                                            b.to_string(),
                                            liq,
                                            dex.to_string(),
                                        ));
                                        pairs.push((
                                            b.to_string(),
                                            a.to_string(),
                                            liq,
                                            dex.to_string(),
                                        ));
                                    };
                                // Allowed base tokens filter (empty => allow all)
                                let base_allow: std::collections::HashSet<String> =
                                    disc_cfg.base_tokens.iter().cloned().collect();
                                let sol_mint = "So11111111111111111111111111111111111111112";
                                let usdc = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
                                let usdt = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB";
                                // Raydium
                                if use_ray {
                                    for s in ray.snapshots() {
                                        let a = s.base_mint.to_string();
                                        let b = s.quote_mint.to_string();
                                        // UI reserves
                                        let a_ui = s.reserve_base as f64
                                            / 10f64.powi(if a == usdc || a == usdt {
                                                6
                                            } else {
                                                9
                                            });
                                        let b_ui = s.reserve_quote as f64
                                            / 10f64.powi(if b == usdc || b == usdt {
                                                6
                                            } else {
                                                9
                                            });
                                        // Liquidity gate by side (SOL or USD stable)
                                        let mut ok = true;
                                        if a == sol_mint || b == sol_mint {
                                            if let Some(min_sol) = disc_cfg.min_liquidity_sol {
                                                let sol_side =
                                                    if a == sol_mint { b_ui } else { a_ui };
                                                ok = (a_ui + b_ui) > 0.0
                                                    && (a_ui + b_ui) >= (2.0 * min_sol)
                                                    || sol_side >= min_sol;
                                            }
                                        } else if a == usdc || a == usdt || b == usdc || b == usdt {
                                            if let Some(min_usd) = disc_cfg.min_liquidity_usd {
                                                ok = (a_ui + b_ui) >= (2.0 * min_usd);
                                            }
                                        }
                                        if !ok {
                                            continue;
                                        }
                                        // base token filter
                                        if !(base_allow.is_empty()
                                            || base_allow.contains(&a)
                                            || base_allow.contains(&b))
                                        {
                                            continue;
                                        }
                                        let liq = a_ui + b_ui;
                                        push_pair(&mut pairs, &a, &b, liq, "RAYDIUM");
                                    }
                                }
                                // Orca
                                if use_orc {
                                    for s in orc.pools_snapshot() {
                                        let a = s.base_mint.to_string();
                                        let b = s.quote_mint.to_string();
                                        let a_ui = s.reserve_base as f64
                                            / 10f64.powi(if a == usdc || a == usdt {
                                                6
                                            } else {
                                                9
                                            });
                                        let b_ui = s.reserve_quote as f64
                                            / 10f64.powi(if b == usdc || b == usdt {
                                                6
                                            } else {
                                                9
                                            });
                                        let mut ok = true;
                                        if a == sol_mint || b == sol_mint {
                                            if let Some(min_sol) = disc_cfg.min_liquidity_sol {
                                                let sol_side =
                                                    if a == sol_mint { b_ui } else { a_ui };
                                                ok = (a_ui + b_ui) > 0.0
                                                    && (a_ui + b_ui) >= (2.0 * min_sol)
                                                    || sol_side >= min_sol;
                                            }
                                        } else if a == usdc || a == usdt || b == usdc || b == usdt {
                                            if let Some(min_usd) = disc_cfg.min_liquidity_usd {
                                                ok = (a_ui + b_ui) >= (2.0 * min_usd);
                                            }
                                        }
                                        if !ok {
                                            continue;
                                        }
                                        if !(base_allow.is_empty()
                                            || base_allow.contains(&a)
                                            || base_allow.contains(&b))
                                        {
                                            continue;
                                        }
                                        let liq = a_ui + b_ui;
                                        push_pair(&mut pairs, &a, &b, liq, "ORCA");
                                    }
                                }
                                // Rank per base if requested
                                if let Some(k) = disc_cfg.top_n_per_base {
                                    use std::collections::HashMap;
                                    let mut by_base: HashMap<
                                        String,
                                        Vec<(String, String, f64, String)>,
                                    > = HashMap::new();
                                    for (i, o, liq, dex) in pairs.into_iter() {
                                        by_base
                                            .entry(i.clone())
                                            .or_default()
                                            .push((i, o, liq, dex));
                                    }
                                    pairs = Vec::new();
                                    for (_b, mut v) in by_base.into_iter() {
                                        v.sort_by(|x, y| y.2.partial_cmp(&x.2).unwrap());
                                        pairs.extend(v.into_iter().take(k));
                                    }
                                } else {
                                    // Global sort by liquidity
                                    pairs.sort_by(|x, y| y.2.partial_cmp(&x.2).unwrap());
                                }
                                // Map into config pairs
                                let default_amt =
                                    disc_cfg.default_ui_amount.unwrap_or(0.05).max(0.000001);
                                let mut out: Vec<ArbPairCfg> = Vec::new();
                                for (i, o, _liq, _dex) in pairs.iter() {
                                    out.push(ArbPairCfg {
                                        in_mint: i.clone(),
                                        out_mint: o.clone(),
                                        ui_amount: default_amt,
                                    });
                                }
                                // Swap atomically
                                {
                                    let mut w = discovered_d.write();
                                    *w = out;
                                }
                                // Optional CSV logging in discovery-only mode
                                if disc_cfg.mode.as_deref() == Some("discovery-only") {
                                    for (i, o, liq, dex) in pairs.iter().take(100) {
                                        append_arb_pair_record(i, o, *liq, dex);
                                    }
                                }
                            }
                        });
                    }
                }
            }
            tokio::spawn(async move {
                // Arbitrage scanning task (opportunities detection without execution for now)
                tracing::info!("arbitrage_task: starting arbitrage scanning loop");

                let ray = Arc::new(Raydium::new(rpc.clone()));
                let orc = Arc::new(Orca::new(rpc.clone()));

                // CRITICAL: Warm up pools by forcing initial refresh
                tracing::info!("arbitrage_task: warming up DEX connectors (loading pool data)");
                if let Err(e) = ray.refresh_pools().await {
                    tracing::error!(error = %e, "arbitrage_task: failed to refresh Raydium pools");
                } else {
                    tracing::info!("arbitrage_task: Raydium pools refreshed successfully");
                }
                if let Err(e) = orc.refresh_pools().await {
                    tracing::error!(error = %e, "arbitrage_task: failed to refresh Orca pools");
                } else {
                    tracing::info!("arbitrage_task: Orca pools refreshed successfully");
                }

                let arb = ArbitrageEngine::new(rpc.clone(), vec![ray.clone(), orc.clone()])
                    .with_profit_params(10, 5_000_000); // min 10 bps, est 0.05 SOL tx cost

                let interval_ms = cfg_pairs
                    .as_ref()
                    .and_then(|c| c.interval_ms)
                    .unwrap_or(2000);
                let static_pairs = cfg_pairs
                    .as_ref()
                    .map(|c| c.pairs.clone())
                    .unwrap_or_default();
                let disc_cfg = cfg_pairs.and_then(|c| c.discovery);
                let mut iv = interval(Duration::from_millis(interval_ms));

                let mut loop_count = 0u64;

                loop {
                    iv.tick().await;
                    loop_count += 1;

                    if loop_count % 10 == 0 {
                        tracing::info!(
                            loop_iteration = loop_count,
                            "arbitrage_task: cycle scan iteration"
                        );
                    }

                    // Choose base tokens from discovered/static pairs
                    let use_pairs: Vec<ArbPairCfg> = if let Some(dc) = &disc_cfg {
                        if dc.enable && dc.mode.as_deref() == Some("full-auto") {
                            discovered.read().clone()
                        } else {
                            static_pairs.clone()
                        }
                    } else {
                        static_pairs.clone()
                    };

                    // Extract unique base tokens from pairs
                    let mut base_tokens: std::collections::HashSet<String> =
                        std::collections::HashSet::new();
                    for p in &use_pairs {
                        base_tokens.insert(p.in_mint.clone());
                        base_tokens.insert(p.out_mint.clone());
                    }

                    // Fallback to configured base_tokens if discovery hasn't populated yet
                    if base_tokens.is_empty() {
                        if let Some(dc) = &disc_cfg {
                            for bt in &dc.base_tokens {
                                base_tokens.insert(bt.clone());
                            }
                            tracing::debug!(
                                "arbitrage: using configured base_tokens (discovery not yet ready)"
                            );
                        }
                    }

                    let base_list: Vec<String> = base_tokens.into_iter().take(20).collect();

                    if base_list.is_empty() {
                        tracing::debug!(
                            "arbitrage: no base tokens available from discovered pairs or config"
                        );
                        continue;
                    }

                    tracing::debug!(
                        base_tokens_count = base_list.len(),
                        base_tokens = ?base_list.iter().take(5).collect::<Vec<_>>(),
                        "arbitrage: starting cycle enumeration"
                    );

                    // Quick diagnostic: test router connectivity with a simple quote
                    if loop_count == 1 {
                        tracing::info!("arbitrage_diagnostic: testing router with test quote");
                        let test_result = arb
                            .router
                            .best_quote_exact_in(
                                "So11111111111111111111111111111111111111112",  // SOL
                                "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", // USDC
                                1_000_000_000,                                  // 1 SOL
                            )
                            .await;
                        match test_result {
                            Ok(Some(q)) => tracing::info!(
                                quote_found = true,
                                input_mint = %q.quote.input_mint,
                                output_mint = %q.quote.output_mint,
                                amount_out = q.quote.amount_out,
                                "arbitrage_diagnostic: test quote successful"
                            ),
                            Ok(None) => tracing::warn!(
                                "arbitrage_diagnostic: test quote returned None (no liquidity?)"
                            ),
                            Err(e) => tracing::error!(
                                error = %e,
                                "arbitrage_diagnostic: test quote failed"
                            ),
                        }
                    }

                    // Scan for profitable arbitrage cycles (1 SOL = 1B lamports test amount)
                    match arb
                        .enumerate_triangular_cycles(&base_list, 1_000_000_000)
                        .await
                    {
                        Ok(cycles) => {
                            tracing::debug!(
                                total_cycles_found = cycles.len(),
                                "arbitrage: cycle enumeration completed"
                            );
                            let mut profitable: Vec<_> = cycles
                                .into_iter()
                                .filter(|c| c.net_profit.is_some() && c.net_profit.unwrap() > 0)
                                .collect();

                            tracing::debug!(
                                profitable_cycles = profitable.len(),
                                "arbitrage: filtered for profitability"
                            );

                            // Sort by net profit descending
                            profitable.sort_by(|a, b| {
                                let a_net = a.net_profit.unwrap_or(0);
                                let b_net = b.net_profit.unwrap_or(0);
                                b_net.cmp(&a_net)
                            });

                            // Log top opportunities
                            for cycle in profitable.into_iter().take(5) {
                                let (a, b, c) = &cycle.path;
                                let net_profit_norm = cycle.net_profit.unwrap_or(0);
                                let gross_profit_norm = cycle.gross_profit;
                                let amount_in_norm = cycle.amount_in; // Now normalized to 9 decimals

                                // Simple ROI calculation with all values in normalized 9-decimal space
                                let roi_bps = if amount_in_norm > 0 && net_profit_norm > 0 {
                                    ((net_profit_norm as f64 / amount_in_norm as f64) * 10_000.0)
                                        as u32
                                } else {
                                    0
                                };

                                tracing::info!(
                                    path = %format!("{} -> {} -> {} -> {}", a, b, c, a),
                                    gross_profit_lamports = gross_profit_norm,
                                    net_profit_lamports = net_profit_norm,
                                    amount_in_lamports = amount_in_norm,
                                    roi_bps = roi_bps,
                                    "arbitrage cycle opportunity detected"
                                );
                                crate::metrics::ARB_TRIANGLE_OPPORTUNITIES
                                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            }
                        }
                        Err(e) => {
                            tracing::error!(
                                error = %e,
                                error_debug = ?e,
                                loop_iteration = loop_count,
                                "arbitrage_task: triangular cycle enumeration failed - THIS IS CRITICAL"
                            );
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
            // Read diagnostic flag outside the task to avoid capturing &self
            let log_all_inits_flag = self
                .ctx
                .cfg
                .arbitrage
                .as_ref()
                .and_then(|a| a.discovery.as_ref())
                .map(|d| d.log_all_inits)
                .unwrap_or(false);
            tokio::spawn(async move {
                let mut cfg: SniperCfg = (&sn_cfg).into();
                // propagate diagnostic flag from arbitrage.discovery to sniper config
                cfg.log_all_inits = log_all_inits_flag;
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
        raw_amount
            .to_u64()
            .ok_or_else(|| anyhow::anyhow!("Amount overflow"))
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

        let pair = format!("{}/{}", intent.base.symbol, intent.quote.symbol);
        let line = format!(
            "{},{},{},{},{},{},{},{},{},{},{},{}",
            timestamp,
            intent.market,
            pair,
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

fn append_arb_pair_record(in_mint: &str, out_mint: &str, liquidity_ui: f64, dex: &str) {
    use std::io::Write as _;
    static PAIRS_LOG_LOCK: once_cell::sync::Lazy<std::sync::Mutex<()>> =
        once_cell::sync::Lazy::new(|| std::sync::Mutex::new(()));
    let _g = PAIRS_LOG_LOCK.lock().unwrap();
    let dir_name =
        std::env::var("IRONCRAB_TRADE_LOG_DIR").unwrap_or_else(|_| "trade_logs".to_string());
    let dir = std::path::Path::new(&dir_name);
    if !dir.exists() {
        let _ = std::fs::create_dir_all(dir);
    }
    let date = chrono::Utc::now().format("%Y%m%d");
    let file_path = dir.join(format!("arb_pairs-{}.csv", date));
    let new_file = !file_path.exists();
    if let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&file_path)
    {
        if new_file {
            let _ = writeln!(f, "timestamp_utc,in_mint,out_mint,liquidity_ui,dex");
        }
        let _ = writeln!(
            f,
            "{},{},{},{:.6},{}",
            chrono::Utc::now().to_rfc3339(),
            in_mint,
            out_mint,
            liquidity_ui,
            dex
        );
    }
}

// (Ehemalige DummyRustStrategy entfernt; SampleRustStrategy ersetzt diese Vorlage)

// ---------------- SampleRustStrategy ----------------

#[derive(Debug, Clone, serde::Deserialize, Default)]
struct SampleRustStrategyCfg {
    // Required mints for the pair
    base_mint: String,
    quote_mint: String,
    // Optional symbols (purely cosmetic)
    #[serde(default)]
    base_symbol: Option<String>,
    #[serde(default)]
    quote_symbol: Option<String>,
    // Optional decimals (if omitted, fetched lazily via RPC helper)
    #[serde(default)]
    base_decimals: Option<u8>,
    #[serde(default)]
    quote_decimals: Option<u8>,
    // Side and sizing
    #[serde(default = "SampleRustStrategyCfg::default_side")]
    side: String, // "buy" | "sell"
    #[serde(default = "SampleRustStrategyCfg::default_amount")]
    amount_ui: f64, // small, safe notionals
    #[serde(default = "SampleRustStrategyCfg::default_slippage")]
    max_slippage_bps: u32,
    // Throttle
    #[serde(default = "SampleRustStrategyCfg::default_interval")]
    interval_ms: u64,
    // Enable/disable
    #[serde(default = "SampleRustStrategyCfg::default_enabled")]
    enabled: bool,
}

impl SampleRustStrategyCfg {
    fn default_side() -> String {
        "buy".to_string()
    }
    fn default_amount() -> f64 {
        0.01
    }
    fn default_slippage() -> u32 {
        100
    }
    fn default_interval() -> u64 {
        10_000
    }
    fn default_enabled() -> bool {
        true
    }
}

struct SampleRustStrategy {
    name: String,
    cfg: SampleRustStrategyCfg,
    // Cached decimals; lazily resolved
    decimals: parking_lot::Mutex<(Option<u8>, Option<u8>)>,
    last_emit: parking_lot::Mutex<Option<std::time::Instant>>,
}

impl SampleRustStrategy {
    fn new(name: String, cfg: SampleRustStrategyCfg) -> Self {
        Self {
            name,
            decimals: parking_lot::Mutex::new((cfg.base_decimals, cfg.quote_decimals)),
            cfg,
            last_emit: parking_lot::Mutex::new(None),
        }
    }
}

#[async_trait::async_trait]
impl Strategy for SampleRustStrategy {
    fn name(&self) -> &str {
        &self.name
    }

    async fn on_tick(&self, ctx: Arc<EngineContext>) -> anyhow::Result<Vec<TradeIntent>> {
        if !self.cfg.enabled {
            return Ok(vec![]);
        }
        // Throttle by interval
        let now = std::time::Instant::now();
        let should_emit = {
            let last = self.last_emit.lock();
            if let Some(prev) = *last {
                now.duration_since(prev).as_millis() >= self.cfg.interval_ms as u128
            } else {
                true
            }
        };
        if !should_emit {
            return Ok(vec![]);
        }

        // Ensure decimals
        // Snapshot missing flags without holding the lock across await
        let (need_base, need_quote) = {
            let decs = self.decimals.lock();
            (decs.0.is_none(), decs.1.is_none())
        };
        use crate::solana::token_utils::get_token_decimals_or_default;
        use solana_sdk::pubkey::Pubkey as SdkPubkey;
        let mut fetched_base: Option<u8> = None;
        let mut fetched_quote: Option<u8> = None;
        if need_base {
            fetched_base = if let Ok(pk) = self.cfg.base_mint.parse::<SdkPubkey>() {
                Some(get_token_decimals_or_default(&ctx.rpc, &pk).await)
            } else {
                Some(6)
            };
        }
        if need_quote {
            fetched_quote = if let Ok(pk) = self.cfg.quote_mint.parse::<SdkPubkey>() {
                Some(get_token_decimals_or_default(&ctx.rpc, &pk).await)
            } else {
                Some(6)
            };
        }
        if fetched_base.is_some() || fetched_quote.is_some() {
            let mut decs = self.decimals.lock();
            if decs.0.is_none() {
                decs.0 = fetched_base;
            }
            if decs.1.is_none() {
                decs.1 = fetched_quote;
            }
        }

        // Build TradeIntent
        let (base_dec, quote_dec) = *self.decimals.lock();
        let base_dec = base_dec.unwrap_or(6);
        let quote_dec = quote_dec.unwrap_or(6);

        let side = match self.cfg.side.to_ascii_lowercase().as_str() {
            "sell" => crate::types::Side::Sell,
            _ => crate::types::Side::Buy,
        };
        let base_symbol = self
            .cfg
            .base_symbol
            .clone()
            .unwrap_or_else(|| "BASE".to_string());
        let quote_symbol = self
            .cfg
            .quote_symbol
            .clone()
            .unwrap_or_else(|| "QUOTE".to_string());
        let amount_ui = rust_decimal::Decimal::from_f64_retain(self.cfg.amount_ui)
            .unwrap_or(rust_decimal::Decimal::ZERO);

        let ti = TradeIntent {
            market: self.name.clone(),
            base: crate::types::Token {
                symbol: base_symbol,
                mint: self.cfg.base_mint.clone(),
                decimals: base_dec,
            },
            quote: crate::types::Token {
                symbol: quote_symbol,
                mint: self.cfg.quote_mint.clone(),
                decimals: quote_dec,
            },
            side,
            amount: crate::types::Amount { ui: amount_ui },
            max_slippage_bps: self.cfg.max_slippage_bps,
        };

        {
            let mut last = self.last_emit.lock();
            *last = Some(now);
        }
        Ok(vec![ti])
    }
}

// ---------------- Tests ----------------
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test(flavor = "current_thread")]
    async fn sample_rust_strategy_emits_intent_from_config() {
        // Config with explicit decimals to avoid RPC dependency
        let cfg = SampleRustStrategyCfg {
            base_mint: "So11111111111111111111111111111111111111112".to_string(),
            quote_mint: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".to_string(),
            base_symbol: Some("SOL".to_string()),
            quote_symbol: Some("USDC".to_string()),
            base_decimals: Some(9),
            quote_decimals: Some(6),
            side: "buy".to_string(),
            amount_ui: 0.01,
            max_slippage_bps: 100,
            interval_ms: 0,
            enabled: true,
        };
        let strat = SampleRustStrategy::new("TEST-MKT".to_string(), cfg);

        // Minimal EngineContext; rpc will not be used due to explicit decimals
        let app_cfg = crate::config::AppCfg {
            name: "t".into(),
            log_level: "info".into(),
            autosave_state_secs: 60,
        };
        let sol_cfg = crate::config::SolanaCfg {
            rpc_url: "http://127.0.0.1:8899".into(),
            ws_url: "ws://127.0.0.1:8900".into(),
            keypair_path: "./secrets/dummy.json".into(),
            rpc_min_concurrency: None,
            rpc_max_concurrency: None,
            rpc_initial_concurrency: None,
            rpc_inc_every_successes: None,
            rpc_dec_on_rate_limit: None,
            rpc_timeout_ms: None,
            ws_failover_urls: None,
            ws_connect_timeout_ms: None,
            ws_max_backoff_ms: None,
            ws_headers: None,
        };
        let cfg_all = crate::config::Config {
            app: app_cfg,
            solana: sol_cfg,
            markets: vec![],
            allocator: crate::config::AllocatorCfg {
                mode: "fixed".into(),
                rebalance_secs: 60,
                min_transfer_sol: 0.0,
            },
            strategies: std::collections::HashMap::new(),
            arbitrage: None,
            sniper: None,
            orca: Default::default(),
        };
        let rpc = Arc::new(crate::solana::rpc::SolanaRpc::new("http://127.0.0.1:8899"));
        let signer = Arc::new(solana_sdk::signature::Keypair::new());
        let treasury = crate::wallet::Treasury::from_signer(signer);
        let ctx = Arc::new(EngineContext {
            cfg: Arc::new(cfg_all),
            rpc,
            treasury,
        });

        let intents = strat.on_tick(ctx).await.expect("tick ok");
        assert_eq!(intents.len(), 1);
        let ti = &intents[0];
        assert_eq!(ti.market, "TEST-MKT");
        assert_eq!(ti.base.symbol, "SOL");
        assert_eq!(ti.quote.symbol, "USDC");
        assert_eq!(ti.base.decimals, 9);
        assert_eq!(ti.quote.decimals, 6);
        match ti.side {
            crate::types::Side::Buy => {}
            _ => panic!("expected buy"),
        }
        assert!(ti.amount.ui > rust_decimal::Decimal::ZERO);
        assert_eq!(ti.max_slippage_bps, 100);
    }
}
