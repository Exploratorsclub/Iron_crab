#!/usr/bin/env cargo
//! Manual token swap tool - builds and signs swap transaction
//!
//! Usage:
//!   cargo run --bin manual-swap -- \
//!     --keypair ~/.config/solana/id.json \
//!     --input-mint <TOKEN_MINT> \
//!     --output-mint So11111111111111111111111111111111111111112 \
//!     --amount <AMOUNT_RAW> \
//!     --pool <POOL_ADDRESS> \
//!     --dex meteora_dlmm
//!
//! This tool:
//! 1. Builds swap instructions using existing DEX crates
//! 2. Creates transaction with compute budget
//! 3. Signs with provided keypair
//! 4. Sends to RPC and confirms
//!
//! No external APIs - uses repo's existing DEX integrations

use anyhow::{Context, Result};
use clap::Parser;
use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    transaction::Transaction,
};
use std::path::PathBuf;
use std::str::FromStr;

#[derive(Parser, Debug)]
#[command(name = "manual-swap")]
#[command(about = "Manual token swap using existing DEX integrations")]
struct Args {
    /// Path to keypair file
    #[arg(long)]
    keypair: PathBuf,

    /// Input token mint (token to sell)
    #[arg(long)]
    input_mint: String,

    /// Output token mint (token to receive, usually WSOL)
    #[arg(long)]
    output_mint: String,

    /// Amount to swap (raw, no decimals)
    #[arg(long)]
    amount: u64,

    /// Pool address
    #[arg(long)]
    pool: String,

    /// DEX name (meteora_dlmm, raydium, orca, pumpfun)
    #[arg(long)]
    dex: String,

    /// Minimum output amount (slippage protection)
    #[arg(long, default_value = "1")]
    min_out: u64,

    /// RPC URL
    #[arg(long, default_value = "http://127.0.0.1:8899")]
    rpc_url: String,

    /// Dry run (build TX but don't send)
    #[arg(long)]
    dry_run: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();

    println!("🔧 Manual Swap Tool");
    println!("   Input:  {} ({} raw units)", args.input_mint, args.amount);
    println!("   Output: {}", args.output_mint);
    println!("   Pool:   {} ({})", args.pool, args.dex);
    println!("   Min out: {} lamports", args.min_out);
    println!();

    // Load keypair
    let keypair_bytes = std::fs::read(&args.keypair)
        .with_context(|| format!("Failed to read keypair from {:?}", args.keypair))?;
    
    let keypair: Keypair = if keypair_bytes.len() == 64 {
        // Raw bytes format
        Keypair::from_bytes(&keypair_bytes)?
    } else {
        // JSON format
        let secret: Vec<u8> = serde_json::from_slice(&keypair_bytes)?;
        Keypair::from_bytes(&secret)?
    };

    println!("✅ Loaded wallet: {}", keypair.pubkey());

    // Parse addresses
    let input_mint = Pubkey::from_str(&args.input_mint)?;
    let output_mint = Pubkey::from_str(&args.output_mint)?;
    let pool = Pubkey::from_str(&args.pool)?;

    // Connect to RPC
    let rpc_client = RpcClient::new_with_commitment(
        args.rpc_url.clone(),
        CommitmentConfig::confirmed(),
    );

    println!("🔗 Connected to RPC: {}", args.rpc_url);

    // Get recent blockhash
    let recent_blockhash = rpc_client.get_latest_blockhash()?;
    println!("📦 Recent blockhash: {}", recent_blockhash);

    // TODO: Build swap instructions based on DEX type
    // For now, just show what would be done
    println!();
    println!("⚠️  IMPLEMENTATION NEEDED:");
    println!("   This tool needs DEX-specific swap instruction builders.");
    println!("   Currently supported in main repo:");
    println!("   - Meteora DLMM: src/solana/dex/meteora_dlmm.rs");
    println!("   - Raydium: src/solana/dex/raydium.rs");
    println!("   - Orca: src/solana/dex/orca.rs");
    println!();
    println!("   For IMMEDIATE manual swap, use Solana wallet UI or:");
    println!("   1. Export private key");
    println!("   2. Import to Phantom/Solflare");
    println!("   3. Swap via Jupiter aggregator UI");
    println!();
    println!("   Or use the SPL token swap CLI directly.");

    Ok(())
}
