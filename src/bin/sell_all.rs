use clap::Parser;
use ironcrab::config::Config;
use ironcrab::solana::dex::raydium::Raydium;
use ironcrab::solana::dex::Dex;
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
use solana_sdk::bs58;

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
    // Use string for Token Program ID to avoid version mismatch types
    let token_program_id = Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").unwrap();
    let token_accounts = rpc.rpc.get_token_accounts_by_owner(
        &treasury.pubkey(),
        TokenAccountsFilter::ProgramId(token_program_id),
    ).await?;
    
    let sol_mint = Pubkey::from_str("So11111111111111111111111111111111111111112").unwrap();

    for ta in token_accounts {
        let data = ta.account.data;
        
        let bytes = match data {
            solana_account_decoder::UiAccountData::Binary(b, _) => {
                bs58::decode(b).into_vec().unwrap_or_default()
            },
            solana_account_decoder::UiAccountData::LegacyBinary(b) => {
                bs58::decode(b).into_vec().unwrap_or_default()
            },
            _ => continue,
        };
        
        // Manual parse to avoid spl_token version mismatch
        if bytes.len() < 72 { continue; }
        let mint_bytes: [u8; 32] = bytes[0..32].try_into().unwrap();
        let mint = Pubkey::new_from_array(mint_bytes);
        let amount_bytes: [u8; 8] = bytes[64..72].try_into().unwrap();
        let amount = u64::from_le_bytes(amount_bytes);

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
                
                let wsol_mint_sdk = Pubkey::from_str("So11111111111111111111111111111111111111112").unwrap();
                let (_, create_ix) = treasury.build_ata_ix(&rpc, &treasury.pubkey(), &wsol_mint_sdk).await?;
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
