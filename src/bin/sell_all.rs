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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let cfg = Config::load(&args.config)?;

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

    // Fetch all token accounts
    // Use string for Token Program ID to avoid version mismatch types
    let token_program_id = Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").unwrap();
    let token_accounts = rpc
        .rpc
        .get_token_accounts_by_owner(
            &treasury.pubkey(),
            TokenAccountsFilter::ProgramId(token_program_id),
        )
        .await?;

    let sol_mint = Pubkey::from_str("So11111111111111111111111111111111111111112").unwrap();

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
        if bytes.len() < 72 {
            continue;
        }
        let mint_bytes: [u8; 32] = bytes[0..32].try_into().unwrap();
        let mint = Pubkey::new_from_array(mint_bytes);
        let amount_bytes: [u8; 8] = bytes[64..72].try_into().unwrap();
        let amount = u64::from_le_bytes(amount_bytes);

        if mint == sol_mint {
            continue;
        }
        if amount == 0 {
            continue;
        }

        info!("Found {} of mint {}", amount, mint);

        // Slippage 5% for panic sell
        let slippage_bps = 500;

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

        // We need to populate the cache.
        // Let's implement a targeted pool fetch here using RPC.
        // Raydium V4 Program ID
        let raydium_prog =
            Pubkey::from_str("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8").unwrap();

        // Find pool for Mint/SOL
        // We can search for accounts owned by Raydium with size 752 (AMM) and offsets for mints.
        // Base Mint offset: 400, Quote Mint offset: 432 (approx, need to verify layout)
        // Actually, let's just use the `raydium.refresh_pools()` but maybe we can filter?
        // No, the method doesn't take filters.

        // Alternative: Use the `raydium` instance to fetch specific pools.
        // We can add a helper to `Raydium` struct or just do it here.
        // Let's do it here using raw RPC to avoid modifying `Raydium` struct too much if not needed.

        // Layout:
        // status: u64 (0)
        // nonce: u64 (8)
        // max_order: u64 (16)
        // depth: u64 (24)
        // base_decimal: u64 (32)
        // quote_decimal: u64 (40)
        // state: u64 (48)
        // reset_flag: u64 (56)
        // min_size: u64 (64)
        // vol_max_cut_ratio: u64 (72)
        // amount_wave: u64 (80)
        // base_lot_size: u64 (88)
        // quote_lot_size: u64 (96)
        // min_price_multiplier: u64 (104)
        // max_price_multiplier: u64 (112)
        // system_decimal: u64 (120)
        // min_separate_numerator: u64 (128)
        // min_separate_denominator: u64 (136)
        // trade_fee_numerator: u64 (144)
        // trade_fee_denominator: u64 (152)
        // pnl_numerator: u64 (160)
        // pnl_denominator: u64 (168)
        // swap_fee_numerator: u64 (176)
        // swap_fee_denominator: u64 (184)
        // base_need_take_pnl: u64 (192)
        // quote_need_take_pnl: u64 (200)
        // quote_total_pnl: u64 (208)
        // base_total_pnl: u64 (216)
        // pool_open_time: u64 (224)
        // punish_pc_amount: u64 (232)
        // punish_coin_amount: u64 (240)
        // orderbook_to_init_time: u64 (248)
        // swap_base_in_amount: u128 (256)
        // swap_quote_out_amount: u128 (272)
        // swap_base_2_quote_fee: u64 (288)
        // swap_quote_in_amount: u128 (296)
        // swap_base_out_amount: u128 (312)
        // swap_quote_2_base_fee: u64 (328)
        // base_vault: Pubkey (336)
        // quote_vault: Pubkey (368)
        // base_mint: Pubkey (400)
        // quote_mint: Pubkey (432)
        // lp_mint: Pubkey (464)

        // We search for accounts with size 752, owner Raydium, and (base_mint = mint AND quote_mint = SOL) OR (base_mint = SOL AND quote_mint = mint)

        let filters_base = vec![
            solana_client::rpc_filter::RpcFilterType::DataSize(752),
            solana_client::rpc_filter::RpcFilterType::Memcmp(
                solana_client::rpc_filter::Memcmp::new_base58_encoded(400, &mint.to_bytes()),
            ),
            solana_client::rpc_filter::RpcFilterType::Memcmp(
                solana_client::rpc_filter::Memcmp::new_base58_encoded(432, &sol_mint.to_bytes()),
            ),
        ];

        let filters_quote = vec![
            solana_client::rpc_filter::RpcFilterType::DataSize(752),
            solana_client::rpc_filter::RpcFilterType::Memcmp(
                solana_client::rpc_filter::Memcmp::new_base58_encoded(400, &sol_mint.to_bytes()),
            ),
            solana_client::rpc_filter::RpcFilterType::Memcmp(
                solana_client::rpc_filter::Memcmp::new_base58_encoded(432, &mint.to_bytes()),
            ),
        ];

        let mut pools = rpc
            .rpc
            .get_program_accounts_with_config(
                &raydium_prog,
                solana_client::rpc_config::RpcProgramAccountsConfig {
                    filters: Some(filters_base),
                    account_config: solana_client::rpc_config::RpcAccountInfoConfig {
                        encoding: Some(solana_account_decoder::UiAccountEncoding::Base64),
                        ..Default::default()
                    },
                    with_context: None,
                    sort_results: None,
                },
            )
            .await
            .unwrap_or_default();

        if pools.is_empty() {
            pools = rpc
                .rpc
                .get_program_accounts_with_config(
                    &raydium_prog,
                    solana_client::rpc_config::RpcProgramAccountsConfig {
                        filters: Some(filters_quote),
                        account_config: solana_client::rpc_config::RpcAccountInfoConfig {
                            encoding: Some(solana_account_decoder::UiAccountEncoding::Base64),
                            ..Default::default()
                        },
                        with_context: None,
                        sort_results: None,
                    },
                )
                .await
                .unwrap_or_default();
        }

        if let Some((pubkey, _)) = pools.first() {
            info!("Found pool for {}: {}", mint, pubkey);
            // Use the public method to load the pool into cache
            if let Err(e) = raydium.load_pool_from_geyser(pubkey).await {
                warn!("Failed to load pool {}: {:?}", pubkey, e);
            }
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

    Ok(())
}
