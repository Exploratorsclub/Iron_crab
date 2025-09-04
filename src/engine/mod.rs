
use std::{sync::Arc, time::Duration};
use anyhow::Result;
use tokio::time::interval;
use tracing::{info, warn};

use crate::config::{Config, StrategyDef};
use crate::wallet::Treasury;
use crate::types::TradeIntent;
use crate::solana::rpc::SolanaRpc;
use crate::solana::arbitrage::ArbitrageEngine;
use crate::solana::sniper::{run_sniper, SniperCfg};
use crate::solana::dex::{raydium::Raydium, orca::Orca, Dex};
use crate::solana::dex::orca::ORCA_WHIRLPOOL_PROGRAM;
use crate::solana::dex::raydium::RAYDIUM_AMM_V4;

pub mod allocator;
pub mod strategy;
#[cfg(feature = "python")]
pub mod py_strategy;

use allocator::Allocator;
use strategy::{Strategy, run_strategy_tick};

#[derive(Clone)]
pub struct EngineContext {
    pub cfg: Arc<Config>,
    pub rpc: Arc<SolanaRpc>,
    pub treasury: Treasury,
}

pub struct Engine {
    ctx: Arc<EngineContext>,
    allocator: Arc<Allocator>,
    strategies: Vec<Arc<dyn Strategy>>,    // pro Markt konfigurierbar
}

impl Engine {
    pub async fn new(cfg: Arc<Config>, rpc: Arc<SolanaRpc>, treasury: Treasury) -> Result<Self> {
        let allocator = Arc::new(Allocator::new(cfg.allocator.clone()));
        let ctx = Arc::new(EngineContext { cfg: cfg.clone(), rpc, treasury });
        Ok(Self { ctx, allocator, strategies: vec![] })
    }

    pub async fn build_strategies(&mut self) -> Result<()> {
        for m in &self.ctx.cfg.markets {
            let sdef: &StrategyDef = self.ctx.cfg.strategies.get(&m.strategy)
                .ok_or_else(|| anyhow::anyhow!("unknown strategy {}", m.strategy))?;
            match sdef.kind.as_str() {
                "rust" => {
                    // Platzhalter: dummy Strategie pro Markt
                    let s = Arc::new(DummyRustStrategy { name: format!("{}-{}", m.name, m.strategy) });
                    self.strategies.push(s);
                }
                "python" => {
                    #[cfg(feature = "python")]
                    {
                        use crate::engine::py_strategy::py::PyStrategy;
                        let module = sdef.module.clone().ok_or_else(|| anyhow::anyhow!("python strategy needs module"))?;
                        let class  = sdef.class.clone().ok_or_else(|| anyhow::anyhow!("python strategy needs class"))?;
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
                    if let Err(e) = rdx.refresh_pools().await { tracing::debug!(?e, "raydium refresh"); }
                    if let Err(e) = orc.refresh_pools().await { tracing::debug!(?e, "orca refresh"); }
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
                let interval_ms = cfg_pairs.as_ref().and_then(|c| c.interval_ms).unwrap_or(2000);
                let pairs = cfg_pairs.map(|c| c.pairs).unwrap_or_default();
                let mut iv = interval(Duration::from_millis(interval_ms));
                loop {
                    iv.tick().await;
                    for p in &pairs {
                        if let Ok(Some(edge)) = arb.best_edge(&p.in_mint, &p.out_mint, p.ui_amount).await {
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
                if let Err(e) = run_sniper(rpc_clone, cfg, Some(raydium_ref), Some(orca_ref), treasury_arc).await {
                    tracing::warn!(?e, "sniper exited");
                }
            });
        }

        // Strategy Tick Loop
        let mut iv = interval(Duration::from_millis(600));
        loop {
            iv.tick().await;
            crate::metrics::record_activity();
            for s in &self.strategies {
                match run_strategy_tick(s.as_ref(), self.ctx.clone()).await {
                    Ok(intents) => {
                        for ti in intents { self.execute(ti).await?; }
                    }
                    Err(e) => tracing::warn!(?e, name = s.name(), "strategy tick error"),
                }
            }
        }
    }

    async fn execute(&self, intent: TradeIntent) -> Result<()> {
        // TODO: Route zu DEX Connector, Baue und Sende TX
        info!(?intent, "EXECUTE");
        Ok(())
    }
}

struct DummyRustStrategy { name: String }
#[async_trait::async_trait]
impl Strategy for DummyRustStrategy {
    fn name(&self) -> &str { &self.name }
    async fn on_tick(&self, _ctx: Arc<EngineContext>) -> anyhow::Result<Vec<TradeIntent>> {
        Ok(vec![]) // keine Trades – nur Vorlage
    }
}
