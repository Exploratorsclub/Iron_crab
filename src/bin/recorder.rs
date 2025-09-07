use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::Engine;
use clap::Parser;
use flate2::write::GzEncoder;
use flate2::Compression;
use futures::{SinkExt, StreamExt};
use serde_json::json;
use std::str::FromStr;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

use ironcrab::backtest::replay::TraceEvent;
use ironcrab::solana::dex::orca::ORCA_WHIRLPOOL_PROGRAM;
use ironcrab::solana::dex::raydium::RAYDIUM_AMM_V4;
use ironcrab::solana::rpc::SolanaRpc;
use solana_client::rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
use solana_sdk::pubkey::Pubkey;

#[derive(Parser, Debug)]
#[command(
    name = "ironcrab-recorder",
    about = "Record slots, logs, and accounts to a compressed JSONL trace for deterministic replays"
)]
struct Opts {
    /// Output file path (.jsonl.gz)
    #[arg(long, default_value = "trace.jsonl.gz")]
    out: String,
    /// RPC HTTP URL (defaults to env SOLANA_RPC or mainnet-beta)
    #[arg(long)]
    rpc_url: Option<String>,
    /// Include Raydium AMM v4 accounts
    #[arg(long, default_value_t = true)]
    raydium: bool,
    /// Include Orca Whirlpool accounts
    #[arg(long, default_value_t = true)]
    orca: bool,
    /// Poll interval for program accounts (seconds)
    #[arg(long, default_value_t = 30)]
    poll_secs: u64,
    /// Optional max duration to run (seconds); 0 = run until Ctrl+C
    #[arg(long, default_value_t = 0)]
    max_secs: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Minimal logging (redacting formatter); respects RUST_LOG if set
    ironcrab::audit::init_redacting_logging("info")?;

    let opts = Opts::parse();
    // Output writer task (single-threaded gzip JSONL)
    let file = File::create(&opts.out)?;
    let buf = BufWriter::new(file);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    tokio::spawn(async move {
        // Blocking gzip writer in an async task is acceptable for small throughput
        let mut gz = GzEncoder::new(buf, Compression::default());
        while let Some(line) = rx.recv().await {
            let _ = writeln!(gz, "{}", line);
        }
        let _ = gz.finish();
    });

    let rpc_url = opts
        .rpc_url
        .clone()
        .or_else(|| std::env::var("SOLANA_RPC").ok())
        .unwrap_or_else(|| "https://api.mainnet-beta.solana.com".to_string());
    let rpc = Arc::new(SolanaRpc::new(&rpc_url));
    let ws_url = if rpc_url.starts_with("https://") {
        rpc_url.replacen("https://", "wss://", 1)
    } else {
        rpc_url.replacen("http://", "ws://", 1)
    };

    // Subscribe to logs mentioning Raydium/Orca programs
    let mut subs: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    for pid_str in [RAYDIUM_AMM_V4, ORCA_WHIRLPOOL_PROGRAM] {
        let ws_url = ws_url.clone();
        let pid = pid_str.to_string();
        let tx_logs = tx.clone();
        let handle = tokio::spawn(async move {
            let req = json!({"jsonrpc":"2.0","id":1,"method":"logsSubscribe","params":[{"mentions":[pid]},{"commitment":"processed"}]});
            if let Ok((mut ws, _)) = connect_async(&ws_url).await {
                let _ = ws.send(Message::text(req.to_string())).await;
                while let Some(msg) = ws.next().await {
                    if let Ok(Message::Text(txt)) = msg {
                        if txt.contains("logsNotification") {
                            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&txt) {
                                if let Some(arr) = v
                                    .pointer("/params/result/value/logs")
                                    .and_then(|x| x.as_array())
                                {
                                    if let Some(slot) = v
                                        .pointer("/params/result/context/slot")
                                        .and_then(|x| x.as_u64())
                                    {
                                        // write Slot event (idempotent in downstream)
                                        let slot_ev = TraceEvent::Slot { slot };
                                        let _ =
                                            tx_logs.send(serde_json::to_string(&slot_ev).unwrap());
                                        for e in arr {
                                            if let Some(line) = e.as_str() {
                                                let log_ev = TraceEvent::Log {
                                                    slot,
                                                    msg: line.to_string(),
                                                };
                                                let _ = tx_logs
                                                    .send(serde_json::to_string(&log_ev).unwrap());
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });
        subs.push(handle);
    }

    // Periodically poll program accounts for Raydium/Orca and store bytes as base64 Account events
    let start = Instant::now();
    loop {
        // stop if max_secs reached
        if opts.max_secs > 0 && start.elapsed() >= Duration::from_secs(opts.max_secs) {
            break;
        }
        let mut tasks = Vec::new();
        if opts.raydium {
            let rpc_c = rpc.clone();
            let tx_c = tx.clone();
            tasks.push(tokio::spawn(async move {
                if let Err(e) = dump_program_accounts(&rpc_c, RAYDIUM_AMM_V4, tx_c).await {
                    eprintln!("raydium dump error: {e}");
                }
            }));
        }
        if opts.orca {
            let rpc_c = rpc.clone();
            let tx_c = tx.clone();
            tasks.push(tokio::spawn(async move {
                if let Err(e) = dump_program_accounts(&rpc_c, ORCA_WHIRLPOOL_PROGRAM, tx_c).await {
                    eprintln!("orca dump error: {e}");
                }
            }));
        }
        for t in tasks {
            let _ = t.await;
        }
        tokio::time::sleep(Duration::from_secs(opts.poll_secs)).await;
    }

    // Shutdown WS tasks and close writer channel
    for h in subs {
        let _ = h.abort();
    }
    drop(tx);
    Ok(())
}

async fn dump_program_accounts(
    rpc: &Arc<SolanaRpc>,
    program_id_str: &str,
    tx: tokio::sync::mpsc::UnboundedSender<String>,
) -> anyhow::Result<()> {
    let program_id = Pubkey::from_str(program_id_str)?;
    let cfg = RpcProgramAccountsConfig {
        filters: None,
        account_config: RpcAccountInfoConfig {
            encoding: None,
            data_slice: None,
            commitment: None,
            min_context_slot: None,
        },
        with_context: None,
        sort_results: None,
    };
    let pairs = rpc
        .rpc
        .get_program_accounts_with_config(&program_id, cfg)
        .await?;
    for (addr, acc) in pairs {
        let data_b64 = base64::engine::general_purpose::STANDARD.encode(&acc.data);
        let ev = TraceEvent::Account {
            pubkey: addr.to_string(),
            data_b64,
        };
        let _ = tx.send(serde_json::to_string(&ev)?);
    }
    Ok(())
}
