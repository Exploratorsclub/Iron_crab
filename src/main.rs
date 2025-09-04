
use std::{path::PathBuf, sync::Arc};
use clap::Parser;
use tracing_subscriber::EnvFilter;

use ironcrab::config::Config;
use ironcrab::wallet::Treasury;
use ironcrab::engine::Engine;
use ironcrab::solana::rpc::SolanaRpc;
use ironcrab::metrics::serve_metrics;
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
    let filter = EnvFilter::try_new(std::env::var("RUST_LOG").unwrap_or(cfg.app.log_level.clone()))?;
    tracing_subscriber::fmt().with_env_filter(filter).compact().init();

    tracing::info!(app = %cfg.app.name, "starting ironcrab");

    // Start metrics exporter (Prometheus text format) on 0.0.0.0:9898
    tokio::spawn(async move {
        let addr: SocketAddr = "0.0.0.0:9898".parse().unwrap();
        if let Err(e) = serve_metrics(addr).await {
            tracing::warn!(?e, "metrics server exited");
        }
    });

    // Solana RPC & Treasury
    let rpc = Arc::new(SolanaRpc::new(&cfg.solana.rpc_url));
    let treasury = Treasury::load(&cfg.solana.keypair_path)?;

    // Engine
    let mut engine = Engine::new(cfg.clone(), rpc, treasury).await?;
    engine.build_strategies().await?;
    engine.run().await?;
    Ok(())
}
