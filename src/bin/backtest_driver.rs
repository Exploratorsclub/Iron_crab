use clap::Parser;
use ironcrab::audit;
use ironcrab::backtest::replay::{build_events_from_trace, CfmPoolJson, ReplayConfig};
use ironcrab::backtest::replay_rpc::ReplayRpc;
use ironcrab::solana::dex::Dex; // bring trait for refresh_pools()
use ironcrab::{
    backtest::types::{Portfolio, SimEvent, SimEventKind},
    backtest::{
        engine::{BacktestEngine, NoopStrategy},
        impact::ImpactSettings,
        market::{CfmAdapter, ClmmModel, CpmMModel},
    },
    solana::{
        dex::{orca::Orca, raydium::Raydium},
        rpc::SolanaRpc,
    },
};
use std::sync::Arc;

#[derive(Parser, Debug)]
#[command(
    name = "ironcrab-backtest",
    version,
    about = "Backtest driver with optional replay/impact models"
)]
struct Opts {
    /// Use local trace replay instead of live RPC
    #[arg(long)]
    replay_trace: Option<String>,
    /// Slot range (start..end) for replay
    #[arg(long)]
    replay_start: Option<u64>,
    #[arg(long)]
    replay_end: Option<u64>,
    /// Deterministic slot duration in ms (default 400)
    #[arg(long)]
    replay_slot_ms: Option<u64>,
    /// Seed for any deterministic RNG usage (reserved for impact/noise models)
    #[arg(long)]
    replay_seed: Option<u64>,
    /// Impact model to use for slippage checks (cpmm|clmm|none)
    #[arg(long)]
    impact: Option<String>,
    /// Impact noise mean bps (adds to max_slippage for min_out calc)
    #[arg(long, default_value_t = 0.0)]
    impact_noise_mean_bps: f32,
    /// Impact noise std bps (truncated at 0)
    #[arg(long, default_value_t = 0.0)]
    impact_noise_std_bps: f32,
    /// Extra protocol/referral fee bps to subtract from outputs
    #[arg(long, default_value_t = 0)]
    impact_extra_fee_bps: u32,
    /// Python script path to use as IPC strategy (feature=python)
    #[arg(long)]
    py_script: Option<String>,
    /// Emulated latency in ms (contributes to additional slippage bps per slot)
    #[arg(long)]
    emulate_latency_ms: Option<u64>,
    /// Scenario sweep (comma lists): sizes, slippages_bps, latencies_ms (if provided, run a grid)
    #[arg(long)]
    sweep_sizes: Option<String>,
    #[arg(long)]
    sweep_slippages_bps: Option<String>,
    #[arg(long)]
    sweep_latencies_ms: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Basic demo: fetch raydium pools (may be slow), ingest into adapter, run noop backtest over dummy events or replay.
    let opts = Opts::parse();
    // Init redacting logging for this binary as well
    audit::init_redacting_logging(&std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()))?;
    let url = std::env::var("SOLANA_RPC")
        .unwrap_or_else(|_| "https://api.mainnet-beta.solana.com".into());
    let rpc = Arc::new(SolanaRpc::new(&url));
    let mut adapter = CfmAdapter::new();
    // If not replaying, optionally pre-load live Raydium pools to have data
    let using_replay = opts.replay_trace.is_some();
    if !using_replay {
        let ray = Raydium::new(rpc.clone());
        if let Err(e) = ray.refresh_pools().await {
            eprintln!("refresh_pools failed: {e}");
        }
        let snaps = ray.snapshots();
        adapter.ingest_raydium(&snaps);
        println!("Loaded {} Raydium pools", snaps.len());
    }
    let portfolio = Portfolio::new();
    // events: from replay file if provided, otherwise simple slot advance
    let events = if let Some(trace_path) = opts.replay_trace.as_ref() {
        let cfg = ReplayConfig {
            start_slot: opts.replay_start.unwrap_or(0),
            end_slot: opts
                .replay_end
                .unwrap_or_else(|| opts.replay_start.unwrap_or(0)),
            speedup: None,
            trace_path: Some(trace_path.clone()),
            slot_ms: opts.replay_slot_ms,
            seed: opts.replay_seed,
        };
        let (store, sim_events) = build_events_from_trace(&cfg)?;
        let replay_rpc = ReplayRpc::new(std::sync::Arc::new(store.clone()));
        // Populate Raydium snapshots from replay
        let ray = Raydium::new(rpc.clone());
        if let Err(e) = ray.refresh_pools_replay(&replay_rpc) {
            eprintln!("raydium refresh_pools_replay failed: {e}");
        }
        let ray_snaps = ray.snapshots();
        adapter.ingest_raydium(&ray_snaps);
        // Populate Orca snapshots from replay
        let orca = Orca::new(rpc.clone());
        if let Err(e) = orca.refresh_pools_replay(&replay_rpc) {
            eprintln!("orca refresh_pools_replay failed: {e}");
        }
        let orca_snaps = orca.pools_snapshot();
        adapter.ingest_orca(&orca_snaps);
        // Pre-ingest any account JSONs that match CfmPoolJson for deterministic pool state
        let mut added = 0usize;
        for (_k, updates) in store.accounts.iter() {
            for bytes in updates {
                if let Ok(s) = std::str::from_utf8(bytes) {
                    if let Ok(pool) = serde_json::from_str::<CfmPoolJson>(s) {
                        use ironcrab::backtest::market::CfmPool;
                        // Preserve tick_spacing if this pool was already seen via Orca replay refresh
                        let existing_spacing = adapter
                            .pools
                            .iter()
                            .find(|p| p.pool == pool.pool)
                            .and_then(|p| p.tick_spacing);
                        adapter.upsert_pool(CfmPool {
                            pool: pool.pool,
                            base_mint: pool.base_mint,
                            quote_mint: pool.quote_mint,
                            base_reserve: pool.base_reserve,
                            quote_reserve: pool.quote_reserve,
                            fee_bps: pool.fee_bps,
                            tick_spacing: existing_spacing,
                        });
                        added += 1;
                    }
                }
            }
        }
        println!(
            "Replay mode: preloaded {added} pools from trace accounts; ray={} orca={}",
            ray_snaps.len(),
            orca_snaps.len()
        );
        if sim_events.is_empty() {
            vec![SimEvent {
                ts_ms: 0,
                kind: SimEventKind::SlotAdvance { slot: 0 },
            }]
        } else {
            sim_events
        }
    } else {
        vec![SimEvent {
            ts_ms: 0,
            kind: SimEventKind::SlotAdvance { slot: 0 },
        }]
    };
    #[cfg(feature = "python_ipc")]
    if let Some(script) = opts.py_script.as_ref() {
        use ironcrab::backtest::engine::py_strategy_adapter::PyProcStrategy;
        let strategy = PyProcStrategy::from_script(script.clone());
        let mut engine = BacktestEngine::new(strategy, adapter, portfolio, events);
        // Optional impact model selection
        if let Some(model) = opts.impact.as_deref() {
            match model.to_ascii_lowercase().as_str() {
                "cpmm" => engine.set_impact_model(Box::new(CpmMModel)),
                "clmm" => engine.set_impact_model(Box::new(ClmmModel)),
                "none" => { /* leave unset */ }
                other => eprintln!("Unknown impact model: {other} (use cpmm|clmm|none)"),
            }
        }
        // Impact settings
        engine.set_impact_settings(ImpactSettings {
            seed: opts.replay_seed,
            noise_bps_mean: opts.impact_noise_mean_bps,
            noise_bps_std: opts.impact_noise_std_bps,
            emulate_latency_ms: opts.emulate_latency_ms,
            extra_fee_bps: opts.impact_extra_fee_bps,
            slot_ms: opts.replay_slot_ms,
        });
        engine.run()?;
        println!("Decisions: {}", engine.decisions.len());
        return Ok(());
    }

    let strategy = NoopStrategy;
    // If scenario sweep flags are provided, expand into a grid; else run once.
    let mut engine = BacktestEngine::new(strategy, adapter, portfolio, events);
    // Optional impact model selection
    if let Some(model) = opts.impact.as_deref() {
        match model.to_ascii_lowercase().as_str() {
            "cpmm" => engine.set_impact_model(Box::new(CpmMModel)),
            "clmm" => engine.set_impact_model(Box::new(ClmmModel)),
            "none" => { /* leave unset */ }
            other => eprintln!("Unknown impact model: {other} (use cpmm|clmm|none)"),
        }
    }
    engine.set_impact_settings(ImpactSettings {
        seed: opts.replay_seed,
        noise_bps_mean: opts.impact_noise_mean_bps,
        noise_bps_std: opts.impact_noise_std_bps,
        emulate_latency_ms: opts.emulate_latency_ms,
        extra_fee_bps: opts.impact_extra_fee_bps,
        slot_ms: opts.replay_slot_ms,
    });

    if opts.sweep_sizes.is_some()
        || opts.sweep_slippages_bps.is_some()
        || opts.sweep_latencies_ms.is_some()
    {
        let parse_list_u64 = |s: &Option<String>| -> Vec<u64> {
            s.as_ref()
                .map(|x| {
                    x.split(',')
                        .filter_map(|t| t.trim().parse::<u64>().ok())
                        .collect()
                })
                .unwrap_or_default()
        };
        let parse_list_u32 = |s: &Option<String>| -> Vec<u32> {
            s.as_ref()
                .map(|x| {
                    x.split(',')
                        .filter_map(|t| t.trim().parse::<u32>().ok())
                        .collect()
                })
                .unwrap_or_default()
        };
        let sizes = parse_list_u64(&opts.sweep_sizes);
        let slippages = parse_list_u32(&opts.sweep_slippages_bps);
        let latencies = parse_list_u64(&opts.sweep_latencies_ms);
        let sizes = if sizes.is_empty() { vec![0] } else { sizes };
        let slippages = if slippages.is_empty() {
            vec![0]
        } else {
            slippages
        };
        let latencies = if latencies.is_empty() {
            vec![opts.emulate_latency_ms.unwrap_or(0)]
        } else {
            latencies
        };
        let mut runs = 0usize;
        for sz in sizes.iter().cloned() {
            for sl in slippages.iter().cloned() {
                for lt in latencies.iter().cloned() {
                    // Announce scenario meta event to strategy at t0
                    if let Some(first) = engine.events.first().cloned() {
                        let mut meta = first.clone();
                        meta.kind = SimEventKind::ScenarioMeta {
                            name: format!("size={sz}_slip={sl}_lat={lt}"),
                            size: sz,
                            slippage_bps: sl,
                            latency_ms: lt,
                        };
                        engine.events.insert(0, meta);
                    }
                    engine.set_slippage_override_bps(Some(sl));
                    // Override latency for this run
                    let mut iset = engine.impact_settings.clone();
                    iset.emulate_latency_ms = Some(lt);
                    engine.set_impact_settings(iset);
                    engine.run()?;
                    runs += 1;
                }
            }
        }
        println!("Scenario runs completed: {}", runs);
    } else {
        engine.run()?;
        println!("Decisions: {}", engine.decisions.len());
    }
    Ok(())
}
