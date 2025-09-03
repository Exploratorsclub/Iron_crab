use std::sync::Arc;
use ironcrab::{backtest::{market::CfmAdapter, engine::{BacktestEngine, NoopStrategy}}, solana::{rpc::SolanaRpc, dex::raydium::Raydium}, backtest::types::{Portfolio,SimEvent,SimEventKind}};
use ironcrab::solana::dex::Dex; // bring trait for refresh_pools()

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Basic demo: fetch raydium pools (may be slow), ingest into adapter, run noop backtest over dummy events.
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
    let events = vec![SimEvent { ts_ms: 0, kind: SimEventKind::SlotAdvance { slot: 0 } }];
    let strategy = NoopStrategy;
    let mut engine = BacktestEngine::new(strategy, adapter, portfolio, events);
    engine.run()?;
    println!("Decisions: {}", engine.decisions.len());
    Ok(())
}
