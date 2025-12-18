use clap::Parser;
use futures::stream::{self, StreamExt};
use ironcrab::config::Config;
use ironcrab::solana::dex::raydium::Raydium;
use ironcrab::solana::rpc::SolanaRpc;
use ironcrab::wallet::Treasury;
use solana_client::rpc_request::TokenAccountsFilter;
use solana_sdk::bs58;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::transaction::Transaction;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use tracing::{info, warn};

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    #[arg(short, long, default_value = "my_config.server.toml")]
    config: PathBuf,

    /// Override RPC URL (e.g. https://api.mainnet-beta.solana.com)
    #[arg(long)]
    rpc_url: Option<String>,
}

struct SellTask {
    mint: Pubkey,
    amount: u64,
    ta_pubkey: Pubkey,
}

async fn sell_token(
    rpc: Arc<SolanaRpc>,
    raydium: Arc<Raydium>,
    treasury: Arc<Treasury>,
    task: SellTask,
    wsol_ata: Pubkey,
) -> anyhow::Result<()> {
    let mint = task.mint;
    let amount = task.amount;
    let sol_mint = Pubkey::from_str("So11111111111111111111111111111111111111112").unwrap();

    // Slippage 99% for panic sell (force execution)
    let slippage_bps = 9900;

    // Try to fetch pool specifically for this pair if not in cache
    // Use Raydium V3 API via curl to avoid RPC scanning issues
    // Fetch multiple pools (pageSize=10) and filter for V4 (AMM)
    let url = format!(
        "https://api-v3.raydium.io/pools/info/mint?mint1={}&mint2={}&poolType=all&poolSortField=liquidity&sortType=desc&pageSize=10&page=1",
        mint, sol_mint
    );

    info!("Fetching pool for {} from Raydium API...", mint);

    let output = std::process::Command::new("curl")
        .arg("-s")
        .arg(&url)
        .output();

    let pool_id = match output {
        Ok(out) if out.status.success() => {
            let json_str = String::from_utf8(out.stdout).unwrap_or_default();
            let v: serde_json::Value =
                serde_json::from_str(&json_str).unwrap_or(serde_json::Value::Null);

            if let Some(arr) = v["data"]["data"].as_array() {
                // Find first pool with Raydium V4 Program ID
                let v4_prog = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
                let found = arr
                    .iter()
                    .find(|p| p["programId"].as_str().unwrap_or("") == v4_prog);

                if let Some(pool) = found {
                    if let Some(id_str) = pool["id"].as_str() {
                        Pubkey::from_str(id_str).ok()
                    } else {
                        None
                    }
                } else {
                    warn!("No Raydium V4 pool found in API response for {}", mint);
                    None
                }
            } else {
                None
            }
        }
        _ => None,
    };

    if let Some(pid) = pool_id {
        if pid != Pubkey::default() {
            info!("Found pool {} via API. Loading...", pid);
            if let Err(e) = raydium.load_pool_from_geyser(&pid).await {
                warn!("Failed to load pool {}: {}", pid, e);
            }
        }
    } else {
        warn!("No pool found for {} via API", mint);
    }

    match raydium
        .build_swap_plan_auto(
            &mint.to_string(),
            &sol_mint.to_string(),
            amount,
            slippage_bps,
        )
        .await
    {
        Ok(Some(plan)) => {
            info!(
                "Selling {} {} -> SOL (Expected: {})",
                amount, mint, plan.expected_out
            );

            let mut ixs = plan.ixs;

            // Patch Raydium instructions with actual user accounts
            let raydium_prog =
                Pubkey::from_str("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8").unwrap();
            let source_pubkey = task.ta_pubkey;

            for ix in ixs.iter_mut() {
                if ix.program_id == raydium_prog {
                    // Raydium V4 Swap Instruction has 18 accounts.
                    // Index 15: User Source
                    // Index 16: User Destination
                    // Index 17: User Authority
                    if ix.accounts.len() >= 18 {
                        // Patch User Source
                        if ix.accounts[15].pubkey == Pubkey::default() {
                            ix.accounts[15].pubkey = source_pubkey;
                        }
                        // Patch User Destination
                        if ix.accounts[16].pubkey == Pubkey::default() {
                            ix.accounts[16].pubkey = wsol_ata;
                        }
                        // Patch User Authority
                        if ix.accounts[17].pubkey == Pubkey::default() {
                            ix.accounts[17].pubkey = treasury.pubkey();
                        }
                    }
                }
            }

            let latest_blockhash = rpc.get_latest_blockhash_retry().await?;
            let mut tx = Transaction::new_with_payer(&ixs, Some(&treasury.pubkey()));

            if let Err(e) = tx.try_sign(&[treasury.signer_ref()], latest_blockhash) {
                warn!("Failed to sign transaction for {}: {:?}", mint, e);
                return Err(anyhow::anyhow!("Failed to sign"));
            }

            match rpc.rpc.send_and_confirm_transaction(&tx).await {
                Ok(sig) => {
                    info!("Sold {}! Sig: {}", mint, sig);
                }
                Err(e) => warn!("Failed to sell {}: {:?}", mint, e),
            }
        }
        Ok(None) => warn!("No route for {}", mint),
        Err(e) => warn!("Error planning swap for {}: {:?}", mint, e),
    }

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let mut cfg = Config::load(&args.config)?;

    if let Some(url) = args.rpc_url {
        info!("Overriding RPC URL: {}", url);
        cfg.solana.rpc_url = url;
    }

    info!("Loading wallet and RPC...");
    let rpc = Arc::new(SolanaRpc::from_cfg(&cfg.solana));
    let treasury =
        Treasury::load_from_env().or_else(|_| Treasury::load(&cfg.solana.keypair_path))?;

    info!("Wallet: {}", treasury.pubkey());

    let raydium = Arc::new(Raydium::new(rpc.clone()));

    // Fetch all token accounts (Token Program AND Token-2022)
    let token_program_id = Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").unwrap();
    let token_2022_program_id =
        Pubkey::from_str("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb").unwrap();

    let mut token_accounts = rpc
        .rpc
        .get_token_accounts_by_owner(
            &treasury.pubkey(),
            TokenAccountsFilter::ProgramId(token_program_id),
        )
        .await?;

    // Also fetch Token-2022 accounts
    if let Ok(mut accounts_2022) = rpc
        .rpc
        .get_token_accounts_by_owner(
            &treasury.pubkey(),
            TokenAccountsFilter::ProgramId(token_2022_program_id),
        )
        .await
    {
        token_accounts.append(&mut accounts_2022);
    }

    info!("Found {} token accounts total.", token_accounts.len());

    let sol_mint = Pubkey::from_str("So11111111111111111111111111111111111111112").unwrap();
    let mut tasks = Vec::new();

    for ta in token_accounts {
        let data = ta.account.data;
        let ta_pubkey = Pubkey::from_str(&ta.pubkey).unwrap();

        let result = match data {
            solana_account_decoder::UiAccountData::Binary(b, _) => {
                let bytes = bs58::decode(b).into_vec().unwrap_or_default();
                if bytes.len() < 72 {
                    None
                } else {
                    // Check frozen
                    if bytes.len() >= 109 && bytes[108] == 2 {
                        None
                    } else {
                        let mint_bytes: [u8; 32] = bytes[0..32].try_into().unwrap();
                        let mint = Pubkey::new_from_array(mint_bytes);
                        let amount_bytes: [u8; 8] = bytes[64..72].try_into().unwrap();
                        let amount = u64::from_le_bytes(amount_bytes);
                        Some((mint, amount))
                    }
                }
            }
            solana_account_decoder::UiAccountData::LegacyBinary(b) => {
                let bytes = bs58::decode(b).into_vec().unwrap_or_default();
                if bytes.len() < 72 {
                    None
                } else {
                    // Check frozen
                    if bytes.len() >= 109 && bytes[108] == 2 {
                        None
                    } else {
                        let mint_bytes: [u8; 32] = bytes[0..32].try_into().unwrap();
                        let mint = Pubkey::new_from_array(mint_bytes);
                        let amount_bytes: [u8; 8] = bytes[64..72].try_into().unwrap();
                        let amount = u64::from_le_bytes(amount_bytes);
                        Some((mint, amount))
                    }
                }
            }
            solana_account_decoder::UiAccountData::Json(parsed) => {
                // Handle JSON parsed accounts
                if let serde_json::Value::Object(info) = parsed.parsed {
                    if let Some(info_obj) = info.get("info") {
                        // Check frozen state
                        let is_frozen = info_obj
                            .get("state")
                            .and_then(|s| s.as_str())
                            .map(|s| s.eq_ignore_ascii_case("frozen"))
                            .unwrap_or(false);

                        if is_frozen {
                            None
                        } else {
                            let mint_str =
                                info_obj.get("mint").and_then(|v| v.as_str()).unwrap_or("");
                            let amount_str = info_obj
                                .get("tokenAmount")
                                .and_then(|v| v.get("amount"))
                                .and_then(|v| v.as_str())
                                .unwrap_or("0");

                            if let Ok(mint) = Pubkey::from_str(mint_str) {
                                let amount = u64::from_str(amount_str).unwrap_or(0);
                                Some((mint, amount))
                            } else {
                                None
                            }
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
        };

        let (mint, amount) = match result {
            Some(r) => r,
            None => continue,
        };

        if mint == sol_mint {
            continue;
        }
        if amount == 0 {
            continue;
        }

        info!("Queuing sell for {} of mint {}", amount, mint);
        tasks.push(SellTask {
            mint,
            amount,
            ta_pubkey,
        });
    }

    if tasks.is_empty() {
        info!("No sellable tokens found.");
        return Ok(());
    }

    // Ensure WSOL ATA exists
    info!("Ensuring WSOL ATA exists...");
    let (wsol_ata, create_ix) = treasury
        .build_ata_ix(&rpc, &treasury.pubkey(), &sol_mint)
        .await?;
    if let Some(ix) = create_ix {
        info!("Creating WSOL ATA...");
        let recent_blockhash = rpc.get_latest_blockhash_retry().await?;
        let mut tx = Transaction::new_with_payer(&[ix], Some(&treasury.pubkey()));
        tx.sign(&[treasury.signer_ref()], recent_blockhash);
        rpc.rpc.send_and_confirm_transaction(&tx).await?;
    }

    info!("Starting parallel sell of {} tokens...", tasks.len());

    let concurrency = 5;
    let treasury = Arc::new(treasury); // Wrap in Arc for sharing

    stream::iter(tasks)
        .map(|task| {
            let rpc = rpc.clone();
            let raydium = raydium.clone();
            let treasury = treasury.clone();
            async move { sell_token(rpc, raydium, treasury, task, wsol_ata).await }
        })
        .buffer_unordered(concurrency)
        .collect::<Vec<_>>()
        .await;

    info!("All sells completed. Unwrapping WSOL...");
    let _ = treasury.unwrap_wsol(&rpc, None).await;

    Ok(())
}
