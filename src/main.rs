use clap::Parser;
use std::{path::PathBuf, sync::Arc};

use ironcrab::config::Config;
use ironcrab::engine::Engine;
use ironcrab::log_manager::LogManager;
use ironcrab::metrics::serve_metrics;
use ironcrab::solana::rpc::SolanaRpc;
use ironcrab::wallet::Treasury;
use std::net::SocketAddr;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// Pfad zur TOML‑Konfiguration
    #[arg(short, long, default_value = "config.example.toml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let cfg = std::sync::Arc::new(Config::load(&args.config)?);

    // Logging
    ironcrab::audit::init_redacting_logging(&cfg.app.log_level)?;

    tracing::info!(app = %cfg.app.name, "starting ironcrab");

    // Start metrics exporter (Prometheus text format) on 0.0.0.0:9898
    tokio::spawn(async move {
        let addr: SocketAddr = "0.0.0.0:9898".parse().unwrap();
        if let Err(e) = serve_metrics(addr).await {
            tracing::warn!(?e, "metrics server exited");
        }
    });

    // Start log cleanup task if sniper is configured
    if let Some(sniper_cfg) = &cfg.sniper {
        let log_manager = LogManager::from_sniper_config(sniper_cfg);
        tokio::spawn(async move {
            if let Err(e) = log_manager.start_cleanup_task().await {
                tracing::warn!(?e, "log cleanup task exited");
            }
        });
    }

    // Solana RPC & Treasury
    let rpc = Arc::new(SolanaRpc::from_cfg(&cfg.solana));
    // Prefer ENV-based keypair loaders; fall back to path from config
    let treasury =
        Treasury::load_from_env().or_else(|_| Treasury::load(&cfg.solana.keypair_path))?;

    // Engine
    let mut engine = Engine::new(cfg.clone(), rpc, treasury).await?;
    engine.build_strategies().await?;
    engine.run().await?;
    Ok(())
}
