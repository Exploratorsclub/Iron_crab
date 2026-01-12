//! Setup tool for creating an Address Lookup Table (ALT).
//!
//! This is a one-time setup tool that creates an ALT with common accounts
//! used in trading transactions. The ALT address should be saved to config
//! and used by execution-engine.
//!
//! Usage:
//!   cargo run --bin setup-alt -- --keypair <path> --rpc-url <url>
//!
//! The tool will:
//! 1. Create a new ALT (or extend existing one)
//! 2. Add common program IDs and well-known accounts
//! 3. Print the ALT address for use in config

use anyhow::{Context, Result};
use clap::Parser;
use ironcrab::solana::address_lookup_table::{
    create_alt_instructions, extend_alt_instruction, get_common_accounts, load_alt,
};
use ironcrab::wallet::Treasury;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_commitment_config::CommitmentConfig;
use solana_sdk::{
    pubkey::Pubkey,
    transaction::Transaction,
};
use std::str::FromStr;
use tracing::{info, warn};

#[derive(Parser, Debug)]
#[command(name = "setup-alt")]
#[command(about = "Create or extend an Address Lookup Table for IronCrab")]
struct Args {
    /// Path to keypair file (JSON array or base58)
    #[arg(long, env = "IRONCRAB_KEYPAIR_PATH")]
    keypair: Option<String>,

    /// Base58 keypair (alternative to file)
    #[arg(long, env = "IRONCRAB_KEYPAIR_BASE58")]
    keypair_base58: Option<String>,

    /// RPC URL
    #[arg(long, default_value = "https://api.mainnet-beta.solana.com")]
    rpc_url: String,

    /// Existing ALT address to extend (optional - creates new if not provided)
    #[arg(long)]
    alt_address: Option<String>,

    /// Additional addresses to add (comma-separated pubkeys)
    #[arg(long)]
    extra_addresses: Option<String>,

    /// Dry run - don't actually send transactions
    #[arg(long)]
    dry_run: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    // Load treasury (keypair)
    let treasury = if let Some(path) = &args.keypair {
        Treasury::load(path)?
    } else if let Some(_b58) = &args.keypair_base58 {
        // Base58 loading should use load_from_env with IRONCRAB_KEYPAIR_BASE58
        // For simplicity, recommend using --keypair or env vars
        anyhow::bail!("--keypair-base58 not yet supported directly. Use IRONCRAB_KEYPAIR_JSON env var instead, or provide --keypair <path>");
    } else {
        // Try to load from env
        Treasury::load_from_env().context("No keypair provided and env vars not set")?
    };

    let authority = treasury.pubkey();
    info!(authority = %authority, "Loaded keypair");

    let rpc = RpcClient::new_with_commitment(args.rpc_url.clone(), CommitmentConfig::confirmed());

    // Check balance
    let balance = rpc.get_balance(&authority).await?;
    info!(balance_sol = balance as f64 / 1e9, "Account balance");
    if balance < 10_000_000 {
        // 0.01 SOL minimum
        warn!("Low balance - ALT creation requires ~0.003 SOL rent");
    }

    // Collect addresses to add
    let mut addresses_to_add = get_common_accounts();

    // Add any extra addresses
    if let Some(extra) = &args.extra_addresses {
        for addr_str in extra.split(',') {
            let addr_str = addr_str.trim();
            if !addr_str.is_empty() {
                match Pubkey::from_str(addr_str) {
                    Ok(pk) => {
                        if !addresses_to_add.contains(&pk) {
                            addresses_to_add.push(pk);
                        }
                    }
                    Err(e) => {
                        warn!(address = addr_str, error = %e, "Invalid extra address, skipping");
                    }
                }
            }
        }
    }

    info!(
        count = addresses_to_add.len(),
        "Addresses to add to ALT"
    );

    let alt_address = if let Some(alt_str) = &args.alt_address {
        // Extend existing ALT
        let alt_pubkey = Pubkey::from_str(alt_str).context("invalid ALT address")?;

        // Load existing ALT to check what's already there
        let existing = load_alt(&rpc, &alt_pubkey).await?;
        info!(
            existing_count = existing.accounts.len(),
            "Loaded existing ALT"
        );

        // Filter out addresses already in ALT
        let new_addresses: Vec<Pubkey> = addresses_to_add
            .into_iter()
            .filter(|pk| !existing.contains(pk))
            .collect();

        if new_addresses.is_empty() {
            info!("All addresses already in ALT, nothing to do");
            return Ok(());
        }

        info!(
            new_count = new_addresses.len(),
            "New addresses to add"
        );

        if !args.dry_run {
            // Extend in batches (max 30 addresses per extend instruction due to TX size)
            for chunk in new_addresses.chunks(30) {
                let extend_ix =
                    extend_alt_instruction(&alt_pubkey, &authority, &authority, chunk.to_vec());

                let blockhash = rpc.get_latest_blockhash().await?;
                let tx = Transaction::new_signed_with_payer(
                    &[extend_ix],
                    Some(&authority),
                    &[treasury.signer_ref()],
                    blockhash,
                );

                let sig = rpc.send_and_confirm_transaction(&tx).await?;
                info!(signature = %sig, added = chunk.len(), "Extended ALT");
            }
        } else {
            info!("DRY RUN - would extend ALT with {} addresses", new_addresses.len());
        }

        alt_pubkey
    } else {
        // Create new ALT
        let slot = rpc.get_slot().await?;
        let (create_ix, alt_pubkey) = create_alt_instructions(&authority, slot)?;

        info!(alt_address = %alt_pubkey, "Creating new ALT");

        if !args.dry_run {
            // Create ALT
            let blockhash = rpc.get_latest_blockhash().await?;
            let tx = Transaction::new_signed_with_payer(
                &[create_ix],
                Some(&authority),
                &[treasury.signer_ref()],
                blockhash,
            );

            let sig = rpc.send_and_confirm_transaction(&tx).await?;
            info!(signature = %sig, "Created ALT");

            // Wait a bit for the ALT to be created
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

            // Extend with addresses in batches
            for chunk in addresses_to_add.chunks(30) {
                let extend_ix =
                    extend_alt_instruction(&alt_pubkey, &authority, &authority, chunk.to_vec());

                let blockhash = rpc.get_latest_blockhash().await?;
                let tx = Transaction::new_signed_with_payer(
                    &[extend_ix],
                    Some(&authority),
                    &[treasury.signer_ref()],
                    blockhash,
                );

                let sig = rpc.send_and_confirm_transaction(&tx).await?;
                info!(signature = %sig, added = chunk.len(), "Extended ALT");
            }
        } else {
            info!("DRY RUN - would create ALT at {} with {} addresses", alt_pubkey, addresses_to_add.len());
        }

        alt_pubkey
    };

    // Final summary
    println!("\n========================================");
    println!("ADDRESS LOOKUP TABLE SETUP COMPLETE");
    println!("========================================");
    println!("ALT Address: {}", alt_address);
    println!("\nAdd to your config.toml:");
    println!("  address_lookup_table = \"{}\"", alt_address);
    println!("========================================\n");

    Ok(())
}
