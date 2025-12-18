use clap::Parser;
use ironcrab::config::Config;
use ironcrab::solana::dex::raydium::Raydium;
use ironcrab::solana::dex::Dex;
use ironcrab::solana::rpc::SolanaRpc;
use ironcrab::wallet::Treasury;
use solana_client::rpc_request::TokenAccountsFilter;
use solana_sdk::bs58;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signer;
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
    // Skip full refresh to avoid hanging
    // info!("Refreshing Raydium pools (this may take a moment)...");
    // raydium.refresh_pools().await?;
    // info!("Pools loaded.");

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
    let mut sold_count = 0;

    for ta in token_accounts {
        let data = ta.account.data;

        let bytes = match data {
            solana_account_decoder::UiAccountData::Binary(b, _) => {
                bs58::decode(b).into_vec().unwrap_or_default()
            }
            solana_account_decoder::UiAccountData::LegacyBinary(b) => {
                bs58::decode(b).into_vec().unwrap_or_default()
            }
            _ => continue,
        };

        // Manual parse to avoid spl_token version mismatch
        // Layout is compatible for basic fields between Token and Token-2022
        if bytes.len() < 72 {
            tracing::debug!("Skipping account with len < 72: {}", bytes.len());
            continue;
        }
        let mint_bytes: [u8; 32] = bytes[0..32].try_into().unwrap();
        let mint = Pubkey::new_from_array(mint_bytes);
        let amount_bytes: [u8; 8] = bytes[64..72].try_into().unwrap();
        let amount = u64::from_le_bytes(amount_bytes);

        tracing::debug!(
            "Checking account: Mint={}, Amount={}, Len={}",
            mint,
            amount,
            bytes.len()
        );

        if mint == sol_mint {
            tracing::debug!("Skipping SOL mint");
            continue;
        }
        if amount == 0 {
            tracing::debug!("Skipping empty account for mint {}", mint);
            continue;
        }

        // Check if account is frozen or closed (state byte at offset 108 for standard token, but let's check offset 64+8+32+1 = 105? No.)
        // SPL Token Layout:
        // mint (32)
        // owner (32)
        // amount (8)
        // delegate (36 option) -> 4 + 32
        // state (1) -> offset 32+32+8+36 = 108.
        // State: 0 = Uninitialized, 1 = Initialized, 2 = Frozen.
        // If state is Frozen (2), we can't sell.
        if bytes.len() >= 109 {
            let state = bytes[108];
            if state == 2 {
                info!("Skipping frozen account for mint {}", mint);
                continue;
            }
        }

        info!("Found {} of mint {}", amount, mint);
        sold_count += 1;

        // Slippage 50% for panic sell (was 5%)
        let slippage_bps = 5000;

        // Try to fetch pool specifically for this pair if not in cache
        // We need to find a pool for Mint <-> SOL
        // Since we skipped refresh_pools, we must discover it now.
        // Raydium doesn't have a public "find_pool" method exposed easily without refresh.
        // But we can try to use `refresh_pools` but maybe we can hack it?
        // Actually, let's just try to refresh pools but ONLY for the mints we have?
        // Raydium::refresh_pools fetches ALL.
        // Let's try to use a targeted fetch if possible, or just accept the wait.
        // Since the user said it hangs, we must avoid full refresh.
        // Let's try to fetch the pool account directly if we can guess the address? No.
        // We can use `get_program_accounts` with a filter for the mints.

        // For now, let's try to just call build_swap_plan_auto.
        // If it fails because of missing pool, we might need to implement a targeted fetch.
        // But wait, build_swap_plan_auto calls `fetch_and_update_reserves` if pool is known.
        // If pool is NOT known, it returns None.

        // Try to fetch pool specifically for this pair if not in cache
        // Use Raydium V3 API via curl to avoid RPC scanning issues (getProgramAccounts with memcmp is often blocked)
        let url = format!(
            "https://api-v3.raydium.io/pools/info/mint?mint1={}&mint2={}&poolType=standard&poolSortField=liquidity&sortType=desc&pageSize=1&page=1",
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
                    if let Some(first) = arr.first() {
                        if let Some(id_str) = first["id"].as_str() {
                            Pubkey::from_str(id_str).ok()
                        } else {
                            None
                        }
                    } else {
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
                    // Don't continue, maybe we can still swap if it was already cached?
                }
            }
        } else {
            warn!("No pool found for {} via API", mint);
            // continue; // Don't continue, maybe it's already in cache?
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

                let wsol_mint_sdk =
                    Pubkey::from_str("So11111111111111111111111111111111111111112").unwrap();
                let (_, create_ix) = treasury
                    .build_ata_ix(&rpc, &treasury.pubkey(), &wsol_mint_sdk)
                    .await?;
                if let Some(ix) = create_ix {
                    ixs.insert(0, ix);
                }

                let latest_blockhash = rpc.get_latest_blockhash_retry().await?;
                let mut tx = Transaction::new_with_payer(&ixs, Some(&treasury.pubkey()));
                tx.try_sign(&[treasury.signer_ref()], latest_blockhash)?;

                match rpc.rpc.send_and_confirm_transaction(&tx).await {
                    Ok(sig) => {
                        info!("Sold! Sig: {}", sig);
                        let _ = treasury.unwrap_wsol(&rpc, None).await;
                    }
                    Err(e) => warn!("Failed to sell {}: {:?}", mint, e),
                }
            }
            Ok(None) => warn!("No route for {}", mint),
            Err(e) => warn!("Error planning swap for {}: {:?}", mint, e),
        }
    }

    if sold_count == 0 {
        info!("No sellable tokens found (checked Token Program and Token-2022).");
    }

    Ok(())
}
