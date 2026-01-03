use clap::Parser;
use ironcrab::config::Config;
use ironcrab::ipc::{ControlRequest, ControlRequestKind};
use ironcrab::nats::{NatsClient, NatsConfig, TOPIC_CONTROL_REQUESTS};
use ironcrab::solana::rpc::SolanaRpc;
use solana_sdk::pubkey::Pubkey;
use spl_token::solana_program::pubkey::Pubkey as SplProgPubkey;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use tracing::{error, info};
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(author, version, about = "Manual burn tool (KEYLESS): publishes BurnTokenAccounts ControlRequests to execution-engine")]
struct Args {
    #[arg(short, long, default_value = "my_config.server.toml")]
    config: PathBuf,

    /// Override RPC URL (e.g. https://api.mainnet-beta.solana.com)
    #[arg(long)]
    rpc_url: Option<String>,

    /// Wallet owner pubkey (must match execution-engine configured wallet)
    #[arg(long)]
    owner_pubkey: String,

    /// Token mint(s) to burn (derives ATA via RPC; repeatable)
    #[arg(long)]
    mint: Vec<String>,

    /// Token account(s) to burn (repeatable). If provided, no derivation is done.
    #[arg(long = "token-account")]
    token_account: Vec<String>,

    /// Close token accounts after burn to recover rent (default: true)
    #[arg(long, default_value_t = true)]
    close_accounts: bool,

    /// NATS URL (default: $NATS_URL or nats://localhost:4222)
    #[arg(long, default_value = "")]
    nats_url: String,

    /// Optional operator reason (stored in burn JSONL)
    #[arg(long)]
    reason: Option<String>,
}

fn ensure_keyless_or_exit() {
    let key_vars = [
        "IRONCRAB_KEYPAIR_JSON",
        "IRONCRAB_KEYPAIR_B64",
        "IRONCRAB_KEYPAIR_PATH",
        "IRONCRAB_KEYPAIR_BASE58",
    ];

    if key_vars.iter().any(|v| std::env::var(v).is_ok()) {
        error!("ERROR: Wallet key environment variables detected! burn tool must be KEYLESS.");
        error!("Only execution-engine should have access to wallet keys.");
        std::process::exit(1);
    }
}

#[inline]
fn sdk_to_spl(pk: &Pubkey) -> SplProgPubkey {
    SplProgPubkey::new_from_array(pk.to_bytes())
}

#[inline]
fn spl_to_sdk(pk: &SplProgPubkey) -> Pubkey {
    Pubkey::new_from_array(pk.to_bytes())
}

async fn token_program_for_mint(rpc: &SolanaRpc, mint: &Pubkey) -> anyhow::Result<Pubkey> {
    let acct = rpc.rpc.get_account(mint).await?;
    let owner = acct.owner;

    let spl = Pubkey::new_from_array(spl_token::id().to_bytes());
    let spl22 = Pubkey::new_from_array(spl_token_2022::id().to_bytes());

    if owner == spl {
        Ok(spl)
    } else if owner == spl22 {
        Ok(spl22)
    } else {
        anyhow::bail!("Mint owner is neither spl-token nor spl-token-2022: {}", owner);
    }
}

fn ata_for_owner_mint(owner: &Pubkey, mint: &Pubkey, token_program: &Pubkey) -> Pubkey {
    let owner_spl = sdk_to_spl(owner);
    let mint_spl = sdk_to_spl(mint);
    let token_prog_spl = sdk_to_spl(token_program);
    let ata_spl = spl_associated_token_account::get_associated_token_address_with_program_id(
        &owner_spl,
        &mint_spl,
        &token_prog_spl,
    );
    spl_to_sdk(&ata_spl)
}

fn prompt_yes_or_exit() {
    use std::io::{self, Write};

    print!("Type 'y' to proceed (anything else cancels): ");
    io::stdout().flush().ok();

    let mut input = String::new();
    if io::stdin().read_line(&mut input).is_err() {
        eprintln!("Failed to read input; cancelling");
        std::process::exit(1);
    }

    let v = input.trim().to_ascii_lowercase();
    if v != "y" {
        println!("Cancelled.");
        std::process::exit(0);
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("burn_manual_keyless=info".parse()?)
                .add_directive("ironcrab=info".parse()?),
        )
        .init();

    let args = Args::parse();
    ensure_keyless_or_exit();

    if args.mint.is_empty() && args.token_account.is_empty() {
        anyhow::bail!("Provide at least one --mint or --token-account");
    }

    let mut cfg = Config::load(&args.config)?;

    // RPC (only needed for --mint derivation)
    if let Some(url) = args.rpc_url.clone() {
        cfg.rpc_url = url;
    }
    let rpc = Arc::new(SolanaRpc::new(&cfg.rpc_url));

    let owner = Pubkey::from_str(&args.owner_pubkey)
        .map_err(|e| anyhow::anyhow!("invalid owner_pubkey: {e}"))?;

    let mut token_accounts: Vec<Pubkey> = Vec::new();

    for ta in &args.token_account {
        let p = Pubkey::from_str(ta)
            .map_err(|e| anyhow::anyhow!("invalid token-account {ta}: {e}"))?;
        token_accounts.push(p);
    }

    for mint_str in &args.mint {
        let mint = Pubkey::from_str(mint_str)
            .map_err(|e| anyhow::anyhow!("invalid mint {mint_str}: {e}"))?;
        let token_program = token_program_for_mint(&rpc, &mint).await?;
        let ata = ata_for_owner_mint(&owner, &mint, &token_program);
        token_accounts.push(ata);
    }

    token_accounts.sort();
    token_accounts.dedup();

    let build = env!("CARGO_PKG_VERSION");
    let run_id = Uuid::new_v4().to_string();
    let request_id = Uuid::new_v4().to_string();

    info!(
        request_id = %request_id,
        owner = %owner,
        close_accounts = args.close_accounts,
        count = token_accounts.len(),
        reason = ?args.reason,
        "Preparing manual burn ControlRequest"
    );

    for (i, ta) in token_accounts.iter().enumerate() {
        info!(index = i, token_account = %ta, "burn target");
    }

    println!("\nDANGER: This will BURN tokens (irreversible) and optionally close accounts.");
    println!("This is NOT used by UI liquidation and is operator-only.");
    prompt_yes_or_exit();

    let kind = ControlRequestKind::BurnTokenAccounts {
        owner_pubkey: owner.to_string(),
        token_accounts: token_accounts.into_iter().map(|p| p.to_string()).collect(),
        close_accounts: args.close_accounts,
        reason: args.reason.clone(),
    };

    let req = ControlRequest::new(
        "burn-manual-keyless",
        build,
        &run_id,
        request_id.clone(),
        "execution-engine",
        kind,
    );

    let nats_url = if args.nats_url.trim().is_empty() {
        NatsConfig::default().url
    } else {
        args.nats_url.clone()
    };

    let mut nats = NatsClient::new(NatsConfig::new(&nats_url, "burn-manual-keyless"));
    nats.connect().await?;

    let ok = nats.publish(TOPIC_CONTROL_REQUESTS, &req).await?;
    if !ok {
        anyhow::bail!("Failed to publish ControlRequest (NATS publish returned false)");
    }

    info!(request_id = %request_id, topic = TOPIC_CONTROL_REQUESTS, "ControlRequest published");
    Ok(())
}
