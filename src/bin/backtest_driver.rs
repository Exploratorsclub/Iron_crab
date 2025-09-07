use std::sync::Arc;
use clap::Parser;
use ironcrab::{backtest::{market::{CfmAdapter, CpmMModel, ClmmModel}, engine::{BacktestEngine, NoopStrategy}}, solana::{rpc::SolanaRpc, dex::raydium::Raydium}, backtest::types::{Portfolio,SimEvent,SimEventKind}};
use ironcrab::backtest::replay::{load_trace, TraceEvent};
use ironcrab::solana::dex::Dex; // bring trait for refresh_pools()

#[derive(Parser, Debug)]
#[command(name="ironcrab-backtest", version, about="Backtest driver with optional replay/impact models")]
struct Opts {
    /// Use local trace replay instead of live RPC
    #[arg(long)]
    replay_trace: Option<String>,
    /// Slot range (start..end) for replay
    #[arg(long)]
    replay_start: Option<u64>,
    #[arg(long)]
    replay_end: Option<u64>,
    /// Impact model to use for slippage checks (cpmm|clmm|none)
    #[arg(long)]
    impact: Option<String>,
    /// Python script path to use as IPC strategy (feature=python)
    #[arg(long)]
    py_script: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Basic demo: fetch raydium pools (may be slow), ingest into adapter, run noop backtest over dummy events or replay.
    let opts = Opts::parse();
    let url = std::env::var("SOLANA_RPC").unwrap_or_else(|_| "https://api.mainnet-beta.solana.com".into());
    let rpc = Arc::new(SolanaRpc::new(&url));
    let ray = Raydium::new(rpc.clone());
    // Try refresh (ignore errors gracefully so sample runs offline)
    if let Err(e) = ray.refresh_pools().await { eprintln!("refresh_pools failed: {e}"); }
    let snaps = ray.snapshots();
    println!("Loaded {} Raydium pools", snaps.len());
    let mut adapter = CfmAdapter::new();
    adapter.ingest_raydium(&snaps);
    let portfolio = Portfolio::new();
    // events: from replay file if provided, otherwise simple slot advance
    let events = if let Some(trace_path) = opts.replay_trace.as_ref() {
        let trace = load_trace(trace_path)?;
        // If start/end provided, filter
        let mut v = Vec::new();
        for ev in trace.into_iter() {
            match ev {
                TraceEvent::Slot { slot } => {
                    if let (Some(s), Some(e)) = (opts.replay_start, opts.replay_end) {
                        if slot < s || slot > e { continue; }
                    }
                    v.push(SimEvent { ts_ms: slot, kind: SimEventKind::SlotAdvance { slot } });
                }
                TraceEvent::Log { slot, msg } => {
                    v.push(SimEvent { ts_ms: slot, kind: SimEventKind::Log(format!("replay: {msg}")) });
                }
                TraceEvent::Account { .. } => { /* ignore for now */ }
            }
        }
        if v.is_empty() { vec![SimEvent { ts_ms: 0, kind: SimEventKind::SlotAdvance { slot: 0 } }] } else { v }
    } else {
        vec![SimEvent { ts_ms: 0, kind: SimEventKind::SlotAdvance { slot: 0 } }]
    };
    #[allow(unused_mut)]
    let mut engine = {
        #[cfg(feature = "python")]
        {
            if let Some(script) = opts.py_script.as_ref() {
                use ironcrab::backtest::engine::py_strategy_adapter::PyProcStrategy;
                let strategy = PyProcStrategy::from_script(script.clone());
                BacktestEngine::new(strategy, adapter, portfolio, events)
            } else {
                let strategy = NoopStrategy;
                BacktestEngine::new(strategy, adapter, portfolio, events)
            }
        }
        #[cfg(not(feature = "python"))]
        {
            let strategy = NoopStrategy;
            BacktestEngine::new(strategy, adapter, portfolio, events)
        }
    };
    // Optional impact model selection
    if let Some(model) = opts.impact.as_deref() {
        match model.to_ascii_lowercase().as_str() {
            "cpmm" => engine.set_impact_model(Box::new(CpmMModel)),
            "clmm" => engine.set_impact_model(Box::new(ClmmModel)),
            "none" => { /* leave unset */ }
            other => eprintln!("Unknown impact model: {other} (use cpmm|clmm|none)"),
        }
    }
    engine.run()?;
    println!("Decisions: {}", engine.decisions.len());
    Ok(())
}
