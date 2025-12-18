use clap::Parser;
use ironcrab::config::Config;
use ironcrab::solana::dex::raydium::Raydium;
use ironcrab::solana::rpc::SolanaRpc;
use ironcrab::wallet::Treasury;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signer;
use solana_sdk::transaction::Transaction;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use tracing::{info, warn};
use solana_client::rpc_request::TokenAccountsFilter;
use solana_program::program_pack::Pack;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    #[arg(short, long, default_value = "my_config.server.toml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let cfg = Config::load(&args.config)?;
    
    info!("Loading wallet and RPC...");
    let rpc = Arc::new(SolanaRpc::from_cfg(&cfg.solana));
    let treasury = Treasury::load_from_env().or_else(|_| Treasury::load(&cfg.solana.keypair_path))?;
    
    info!("Wallet: {}", treasury.pubkey());
    
    let raydium = Arc::new(Raydium::new(rpc.clone()));
    info!("Refreshing Raydium pools (this may take a moment)...");
    raydium.refresh_pools().await?;
    info!("Pools loaded.");

    // Fetch all token accounts
    let token_accounts = rpc.rpc.get_token_accounts_by_owner(
        &treasury.pubkey(),
        TokenAccountsFilter::ProgramId(spl_token::id()),
    ).await?;
    
    let sol_mint = Pubkey::from_str("So11111111111111111111111111111111111111112").unwrap();

    for ta in token_accounts {
        // Decode account data
        let data = ta.account.data;
        // Handle UiAccountData if needed, but usually get_token_accounts_by_owner returns parsed or base64.
        // By default it might return base64 if configured in client, but here we use default client.
        // Actually RpcClient default is usually base64 or binary.
        // Let's assume it's binary compatible or we need to handle UiAccountData.
        // Wait, RpcKeyedAccount.account.data is UiAccountData.
        
        let bytes = match data {
            solana_account_decoder::UiAccountData::Binary(b, _) => {
                bs58::decode(b).into_vec().unwrap_or_default() // Wait, Binary is usually base58 or base64 string?
                // UiAccountData::Binary(String, UiAccountEncoding)
            },
            solana_account_decoder::UiAccountData::LegacyBinary(b) => {
                bs58::decode(b).into_vec().unwrap_or_default()
            },
            _ => continue, // Skip parsed
        };
        
        // Actually, let's just use get_token_accounts_by_owner_with_commitment and specify encoding if needed.
        // But simpler: use spl_token::state::Account::unpack on the bytes.
        
        // Re-fetch with explicit encoding to be safe?
        // Or just try to parse.
        
        // Let's try to parse bytes.
        let token_account = match spl_token::state::Account::unpack(&bytes) {
            Ok(a) => a,
            Err(_) => {
                // Try base64 decode if it was base64 string in binary
                // Actually, let's rely on the fact that we can just ask for parsed accounts?
                // No, parsed accounts are easier.
                continue;
            }
        };

        let mint = token_account.mint;
        let amount = token_account.amount;

        if mint == sol_mint { continue; }
        if amount == 0 { continue; }

        info!("Found {} of mint {}", amount, mint);

        // Slippage 5% for panic sell
        let slippage_bps = 500; 
        
        match raydium.build_swap_plan_auto(
            &mint.to_string(),
            &sol_mint.to_string(),
            amount,
            slippage_bps
        ).await {
            Ok(Some(plan)) => {
                info!("Selling {} {} -> SOL (Expected: {})", amount, mint, plan.expected_out);
                
                let mut ixs = plan.ixs;
                
                // Ensure WSOL ATA exists
                let wsol_mint_sdk = solana_sdk::pubkey::Pubkey::from_str("So11111111111111111111111111111111111111112").unwrap();
                let (_, create_ix) = treasury.build_ata_ix(&rpc, &treasury.pubkey(), &wsol_mint_sdk).await?;
                if let Some(ix) = create_ix {
                    ixs.insert(0, ix);
                }
                
                let latest_blockhash = rpc.get_latest_blockhash_retry().await?;
                let mut tx = Transaction::new_with_payer(&ixs, Some(&treasury.pubkey()));
                tx.try_sign(&[treasury.signer_ref()], latest_blockhash)?;
                
                match rpc.send_and_confirm_transaction(&tx).await {
                    Ok(sig) => {
                        info!("Sold! Sig: {}", sig);
                        let _ = treasury.unwrap_wsol(&rpc, None).await;
                    },
                    Err(e) => warn!("Failed to sell {}: {:?}", mint, e),
                }
            },
            Ok(None) => warn!("No route for {}", mint),
            Err(e) => warn!("Error planning swap for {}: {:?}", mint, e),
        }
    }
    
    Ok(())
}
